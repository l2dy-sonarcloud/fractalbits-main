use bytes::Bytes;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::{
    self,
    io::WriteHalf,
    net::TcpStream,
    sync::{oneshot, RwLock},
};

use crate::connection::ReceiveConnection;

#[derive(Error, Debug)]
#[error(transparent)]
pub enum RpcError {
    IoError(io::Error),
    OneshotRecvError(oneshot::error::RecvError),
    EncodeError(prost::EncodeError),
    DecodeError(prost::DecodeError),
    #[error("internal request sending error: {0}")]
    InternalRequestError(String),
    #[error("internal response sending error: {0}")]
    InternalResponseError(String),
}

impl From<io::Error> for RpcError {
    fn from(err: io::Error) -> Self {
        RpcError::IoError(err)
    }
}

pub struct RpcClient {
    requests: Arc<RwLock<HashMap<u32, oneshot::Sender<Bytes>>>>,
    sender: tokio::sync::mpsc::Sender<Bytes>,
    next_id: AtomicU32,
}

impl RpcClient {
    pub async fn new(url: &str) -> Result<Self, RpcError> {
        let stream = TcpStream::connect(url).await?;
        stream.set_nodelay(true)?;
        let (receiver, sender) = tokio::io::split(stream);

        // Start message receiver task, for rpc responses
        let requests = Arc::new(RwLock::new(HashMap::new()));
        {
            let requests_clone = requests.clone();
            tokio::spawn(async move {
                if let Err(e) =
                    Self::receive_message_task(ReceiveConnection::new(receiver), requests_clone)
                        .await
                {
                    tracing::error!("FATAL: receive message task error: {e:?}");
                }
            });
        }

        // Start message sender task, to send rpc requests. We are launching a dedicated task here
        // to reduce lock contention on the sender socket itself.
        let (tx, rx) = tokio::sync::mpsc::channel(1024 * 1024 * 1024);
        {
            tokio::spawn(async move {
                if let Err(e) = Self::send_message_task(sender, rx).await {
                    tracing::error!("FATAL: receive message task error: {e:?}");
                }
            });
        }

        Ok(Self {
            requests,
            sender: tx,
            next_id: AtomicU32::new(1),
        })
    }

    async fn receive_message_task(
        mut receiver: ReceiveConnection,
        requests: Arc<RwLock<HashMap<u32, oneshot::Sender<Bytes>>>>,
    ) -> Result<(), RpcError> {
        loop {
            let res = receiver.read_frame().await;
            let maybe_frame = match res {
                Ok(maybe_frame) => maybe_frame,
                Err(e) => {
                    tracing::warn!("read_frame error: {e}");
                    continue;
                }
            };

            // If `None` is returned from `read_frame()` then the peer closed the socket.
            // There is no further work to do and the task can be terminated.
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => {
                    tracing::warn!("connection closed, receive_message_task quit");
                    return Ok(());
                }
            };

            tracing::debug!("sending response for {}", frame.header.id);
            let tx: oneshot::Sender<Bytes> = match requests.write().await.remove(&frame.header.id) {
                Some(tx) => tx,
                None => continue, // we may have received the response already
            };
            let _ = tx.send(frame.body);
        }
    }

    async fn send_message_task(
        mut sender: WriteHalf<TcpStream>,
        mut input: tokio::sync::mpsc::Receiver<Bytes>,
    ) -> Result<(), RpcError> {
        while let Some(message) = input.recv().await {
            sender.write(message.as_ref()).await.unwrap();
        }
        Ok(())
    }

    pub async fn send_request(&self, id: u32, msg: Bytes) -> Result<Bytes, RpcError> {
        self.sender
            .send(msg.clone())
            .await
            .map_err(|e| RpcError::InternalRequestError(e.to_string()))?;
        tracing::debug!("request sent from handler: request_id={id}");

        let (tx, rx) = oneshot::channel();
        self.requests.write().await.insert(id, tx);
        let res = rx.await.map_err(RpcError::OneshotRecvError);
        if res.is_err() {
            tracing::error!("oneshot error for id={id}");
        }
        res
    }

    pub fn gen_request_id(&self) -> u32 {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}
