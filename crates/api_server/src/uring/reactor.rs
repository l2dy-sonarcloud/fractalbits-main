use super::ring::PerCoreRing;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use crossbeam_channel::{Receiver, Sender, select, unbounded};
use libc;
use metrics::gauge;
use rpc_client_common::transport::RpcTransport;
use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

const RECV_BUFFER_SIZE: usize = 64 * 1024;

#[derive(Debug)]
pub enum RpcTask {
    Noop,
    ZeroCopySend(ZeroCopySendTask),
    Recv(RecvTask),
}

#[derive(Debug)]
pub struct ZeroCopySendTask {
    pub fd: RawFd,
    pub header: Bytes,
    pub body: Bytes,
    pub completion: oneshot::Sender<io::Result<usize>>,
}

#[derive(Debug)]
pub struct RecvTask {
    pub fd: RawFd,
    pub len: usize,
    pub completion: oneshot::Sender<io::Result<Bytes>>,
}

#[derive(Debug, Default)]
struct ReactorMetrics {
    queue_depth: usize,
}

impl ReactorMetrics {
    fn update_queue_depth(&mut self, depth: usize, worker_index: usize) {
        self.queue_depth = depth;
        gauge!(
            "rpc_reactor_command_queue",
            "worker_index" => worker_index.to_string()
        )
        .set(depth as f64);
    }
}

pub struct RpcReactorHandle {
    worker_index: usize,
    sender: Sender<RpcCommand>,
    closed: AtomicBool,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    io: Arc<ReactorIo>,
}

impl RpcReactorHandle {
    fn new(worker_index: usize, sender: Sender<RpcCommand>, io: Arc<ReactorIo>) -> Self {
        Self {
            worker_index,
            sender,
            closed: AtomicBool::new(false),
            join_handle: Mutex::new(None),
            io,
        }
    }

    pub fn worker_index(&self) -> usize {
        self.worker_index
    }

    pub fn command_sender(&self) -> Sender<RpcCommand> {
        self.sender.clone()
    }

    pub fn initiate_shutdown(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Err(err) = self.sender.send(RpcCommand::Shutdown) {
            warn!(worker_index = self.worker_index, error = %err, "failed to send shutdown to rpc reactor");
        }
    }

    fn attach_join_handle(&self, join: JoinHandle<()>) {
        *self
            .join_handle
            .lock()
            .expect("reactor join handle poisoned") = Some(join);
    }

    pub fn submit_zero_copy_send(
        &self,
        fd: RawFd,
        header: Bytes,
        body: Bytes,
    ) -> oneshot::Receiver<io::Result<usize>> {
        let (tx, mut rx) = oneshot::channel();
        let task = RpcTask::ZeroCopySend(ZeroCopySendTask {
            fd,
            header,
            body,
            completion: tx,
        });
        if let Err(err) = self.sender.send(RpcCommand::Task(task)) {
            let _ = rx.close();
            warn!(
                worker_index = self.worker_index,
                error = %err,
                "failed to enqueue zero-copy send task"
            );
        }
        rx
    }

    pub fn submit_recv(&self, fd: RawFd, len: usize) -> oneshot::Receiver<io::Result<Bytes>> {
        let (tx, mut rx) = oneshot::channel();
        let task = RpcTask::Recv(RecvTask {
            fd,
            len,
            completion: tx,
        });
        if let Err(err) = self.sender.send(RpcCommand::Task(task)) {
            let _ = rx.close();
            warn!(
                worker_index = self.worker_index,
                error = %err,
                "failed to enqueue recv task"
            );
        }
        rx
    }
}

impl Drop for RpcReactorHandle {
    fn drop(&mut self) {
        self.initiate_shutdown();
        if let Some(handle) = self
            .join_handle
            .lock()
            .expect("reactor join handle poisoned")
            .take()
        {
            if let Err(err) = handle.join() {
                warn!(
                    worker_index = self.worker_index,
                    "failed to join rpc reactor thread: {err:?}"
                );
            }
        }
    }
}

#[derive(Debug)]
pub enum RpcCommand {
    Shutdown,
    Task(RpcTask),
}

pub fn spawn_rpc_reactor(worker_index: usize, ring: Arc<PerCoreRing>) -> Arc<RpcReactorHandle> {
    let (tx, rx) = unbounded::<RpcCommand>();
    let io = Arc::new(ReactorIo::new(ring));
    let handle = Arc::new(RpcReactorHandle::new(worker_index, tx, io));
    let thread_handle = Arc::clone(&handle);
    let join = thread::Builder::new()
        .name(format!("rpc-reactor-{worker_index}"))
        .spawn(move || reactor_thread(thread_handle, rx))
        .expect("failed to spawn rpc reactor thread");
    handle.attach_join_handle(join);
    handle
}

fn reactor_thread(handle: Arc<RpcReactorHandle>, rx: Receiver<RpcCommand>) {
    info!(
        worker_index = handle.worker_index,
        "rpc reactor thread started"
    );

    let mut running = true;
    let mut shutdown_seen = false;
    let mut metrics = ReactorMetrics::default();

    while running {
        while let Ok(cmd) = rx.try_recv() {
            if !process_command(&handle, cmd) {
                running = false;
                break;
            }
        }

        metrics.update_queue_depth(rx.len(), handle.worker_index);

        if !running {
            break;
        }

        select! {
            recv(rx) -> msg => match msg {
                Ok(cmd) => {
                    if !process_command(&handle, cmd) {
                        running = false;
                    }
                }
                Err(_) => {
                    debug!(worker_index = handle.worker_index, "rpc reactor command channel closed");
                    running = false;
                }
            },
            default(Duration::from_millis(50)) => {
                if shutdown_seen {
                    running = false;
                }
            }
        }

        shutdown_seen = handle.closed.load(Ordering::Acquire);
    }

    handle.closed.store(true, Ordering::Release);
    info!(
        worker_index = handle.worker_index,
        "rpc reactor thread exiting"
    );
}

fn process_command(handle: &Arc<RpcReactorHandle>, cmd: RpcCommand) -> bool {
    match cmd {
        RpcCommand::Shutdown => {
            debug!(
                worker_index = handle.worker_index,
                "rpc reactor received shutdown"
            );
            false
        }
        RpcCommand::Task(task) => {
            match task {
                RpcTask::Noop => {
                    debug!(
                        worker_index = handle.worker_index,
                        "rpc reactor handled noop task"
                    );
                }
                RpcTask::ZeroCopySend(task) => handle_zero_copy_send(handle, task),
                RpcTask::Recv(task) => handle_recv(handle, task),
            }
            true
        }
    }
}

fn handle_zero_copy_send(handle: &Arc<RpcReactorHandle>, task: ZeroCopySendTask) {
    let ZeroCopySendTask {
        fd,
        header,
        body,
        completion,
    } = task;

    let mut total_written = 0usize;
    let result = handle
        .io
        .send_slice(fd, header.as_ref())
        .and_then(|written| {
            total_written += written;
            if body.is_empty() {
                Ok(0)
            } else {
                handle.io.send_slice(fd, body.as_ref()).map(|w| w as i64)
            }
        })
        .map(|additional| {
            if additional > 0 {
                total_written += additional as usize;
            }
            total_written
        });

    if completion.send(result).is_err() {
        debug!(
            worker_index = handle.worker_index,
            fd, "zero-copy send completion receiver dropped"
        );
    }
}

fn handle_recv(handle: &Arc<RpcReactorHandle>, task: RecvTask) {
    let RecvTask {
        fd,
        len,
        completion,
    } = task;
    let mut buffer = BytesMut::with_capacity(len.max(RECV_BUFFER_SIZE));
    match handle.io.recv_chunk(fd, buffer.capacity()) {
        Ok(bytes) => {
            buffer.extend_from_slice(&bytes);
            let bytes = buffer.freeze();
            let _ = completion.send(Ok(bytes));
        }
        Err(err) => {
            let _ = completion.send(Err(err));
        }
    }
}

struct ReactorIo {
    ring: Arc<PerCoreRing>,
    zerocopy_configured: AtomicBool,
}

impl ReactorIo {
    fn new(ring: Arc<PerCoreRing>) -> Self {
        Self {
            ring,
            zerocopy_configured: AtomicBool::new(false),
        }
    }

    fn send_slice(&self, fd: RawFd, mut buf: &[u8]) -> io::Result<usize> {
        let mut total = 0usize;
        while !buf.is_empty() {
            let bytes_written = if buf.len() >= 16 * 1024 {
                self.send_zero_copy(fd, buf)?
            } else {
                self.send_write(fd, buf)?
            };

            if bytes_written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "io_uring send returned zero bytes",
                ));
            }

            total += bytes_written;
            buf = &buf[bytes_written..];
        }
        Ok(total)
    }

    fn send_write(&self, fd: RawFd, buf: &[u8]) -> io::Result<usize> {
        self.ring.with_lock(|ring| {
            let entry = io_uring::opcode::Write::new(
                io_uring::types::Fd(fd),
                buf.as_ptr(),
                buf.len() as u32,
            )
            .build();
            unsafe {
                ring.submission()
                    .push(&entry)
                    .map_err(|_| io::Error::other("submission queue full"))?;
            }
            ring.submit_and_wait(1)?;
            let mut cq = ring.completion();
            let cqe = cq
                .next()
                .ok_or_else(|| io::Error::other("missing completion"))?;
            let res = cqe.result();
            if res < 0 {
                Err(io::Error::from_raw_os_error(-res))
            } else {
                Ok(res as usize)
            }
        })
    }

    fn send_zero_copy(&self, fd: RawFd, buf: &[u8]) -> io::Result<usize> {
        self.ensure_zero_copy(fd)?;
        self.ring.with_lock(|ring| {
            let entry = io_uring::opcode::SendZc::new(
                io_uring::types::Fd(fd),
                buf.as_ptr(),
                buf.len() as u32,
            )
            .flags(libc::MSG_ZEROCOPY | libc::MSG_NOSIGNAL)
            .build();
            unsafe {
                ring.submission()
                    .push(&entry)
                    .map_err(|_| io::Error::other("submission queue full"))?;
            }
            ring.submit_and_wait(1)?;

            let mut cq = ring.completion();
            let mut result: Option<usize> = None;
            while let Some(cqe) = cq.next() {
                if io_uring::cqueue::notif(cqe.flags()) {
                    continue;
                }
                if result.is_none() {
                    let res = cqe.result();
                    if res < 0 {
                        return Err(io::Error::from_raw_os_error(-res));
                    }
                    result = Some(res as usize);
                }
                if !io_uring::cqueue::more(cqe.flags()) {
                    break;
                }
            }

            result.ok_or_else(|| io::Error::other("missing zero-copy completion"))
        })
    }

    fn recv_chunk(&self, fd: RawFd, len: usize) -> io::Result<Vec<u8>> {
        let mut buffer = vec![0u8; len];
        let read = self.ring.with_lock(|ring| {
            let entry = io_uring::opcode::Recv::new(
                io_uring::types::Fd(fd),
                buffer.as_mut_ptr(),
                len as u32,
            )
            .flags(libc::MSG_NOSIGNAL)
            .build();
            unsafe {
                ring.submission()
                    .push(&entry)
                    .map_err(|_| io::Error::other("submission queue full"))?;
            }
            ring.submit_and_wait(1)?;
            let mut cq = ring.completion();
            let cqe = cq
                .next()
                .ok_or_else(|| io::Error::other("missing completion"))?;
            Ok::<_, io::Error>(cqe.result())
        })?;

        if read < 0 {
            Err(io::Error::from_raw_os_error(-read))
        } else {
            let read = read as usize;
            buffer.truncate(read);
            Ok(buffer)
        }
    }

    fn ensure_zero_copy(&self, fd: RawFd) -> io::Result<()> {
        if self
            .zerocopy_configured
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            let enable: libc::c_int = 1;
            let ret = unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_ZEROCOPY,
                    &enable as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&enable) as libc::socklen_t,
                )
            };
            if ret == -1 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ReactorTransport {
    handle: Arc<RpcReactorHandle>,
}

impl ReactorTransport {
    pub fn new(handle: Arc<RpcReactorHandle>) -> Self {
        Self { handle }
    }
}

#[async_trait]
impl RpcTransport for ReactorTransport {
    async fn send(&self, fd: RawFd, header: Bytes, body: Bytes) -> io::Result<usize> {
        let rx = self.handle.submit_zero_copy_send(fd, header, body);
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "rpc reactor send cancelled",
            )),
        }
    }

    async fn recv(&self, fd: RawFd, len: usize) -> io::Result<Bytes> {
        let rx = self.handle.submit_recv(fd, len);
        match rx.await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "rpc reactor recv cancelled",
            )),
        }
    }

    fn name(&self) -> &'static str {
        "reactor_io_uring"
    }
}
