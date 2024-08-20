use crate::utils;
use std::collections::HashMap;
use std::io::Result;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::BufReader;
use tokio::{
    net::TcpStream,
    sync::{oneshot, RwLock},
};
use web_socket::*;

pub struct RpcClient {
    requests: Arc<RwLock<HashMap<u32, oneshot::Sender<Box<[u8]>>>>>,
    ws: Arc<RwLock<WebSocket<BufReader<TcpStream>>>>,
    next_id: AtomicU32,
}

impl RpcClient {
    pub async fn new(url: &str, port: u32) -> Result<Self> {
        let ws = Arc::new(RwLock::new(
            utils::connect(&format!("{url}:{port}"), "/").await?,
        ));
        let requests = Arc::new(RwLock::new(HashMap::new()));

        {
            let ws_clone = ws.clone();
            let requests_clone = requests.clone();
            tokio::spawn(async move {
                loop {
                    let mut ws = ws_clone.write().await;
                    match tokio::time::timeout(Duration::from_millis(10), ws.recv()).await {
                        Ok(Ok(Event::Data { ty, data })) => {
                            assert!(matches!(ty, DataType::Complete(MessageType::Text)));
                            let tx: oneshot::Sender<Box<[u8]>> =
                                requests_clone.write().await.remove(&0).unwrap();
                            _ = tx.send(data);
                        }
                        _ => {
                            // ignore for now
                        }
                    }
                }
            });
        }

        Ok(Self {
            requests,
            ws,
            next_id: AtomicU32::new(1),
        })
    }

    pub async fn send_request(&self, _id: u32, msg: &[u8]) -> Result<String> {
        let rx = {
            self.ws.write().await.send(msg).await?;
            let (tx, rx) = oneshot::channel();
            // FIXME: track inflight request with actual id
            self.requests.write().await.insert(0, tx);
            rx
        };

        let response = rx.await.unwrap();
        Ok(std::str::from_utf8(&*response).unwrap().to_string())
    }

    pub fn gen_request_id(&self) -> u32 {
        let request_id = self.next_id.load(std::sync::atomic::Ordering::SeqCst);
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        request_id
    }
}
