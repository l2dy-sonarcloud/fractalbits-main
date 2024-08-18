use crate::utils;
use std::collections::HashMap;
use std::io::Result;
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::{
    net::TcpStream,
    sync::{oneshot, RwLock},
};
use web_socket::*;

pub struct RpcClient {
    requests: Arc<RwLock<HashMap<u64, oneshot::Sender<Box<[u8]>>>>>,
    ws: Arc<RwLock<WebSocket<BufReader<TcpStream>>>>,
    next_id: u64,
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
                    match ws_clone.write().await.recv().await {
                        Ok(Event::Data { ty, data }) => {
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
            next_id: 0,
        })
    }

    pub async fn send_request(&mut self, msg: &[u8]) -> Result<String> {
        let request_id = self.next_id;
        self.next_id += 1;

        self.ws.write().await.send(msg).await?;

        let (tx, rx) = oneshot::channel();

        let mut requests = self.requests.write().await;
        requests.insert(request_id, tx);
        drop(requests);

        let response = rx.await.unwrap();
        Ok(std::str::from_utf8(&*response).unwrap().to_string())
    }
}
