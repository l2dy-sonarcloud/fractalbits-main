use crate::io_err;
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
    requests: Arc<RwLock<HashMap<u64, oneshot::Sender<Event>>>>,
    ws: WebSocket<BufReader<TcpStream>>,
    next_id: u64,
}

impl RpcClient {
    pub async fn new(url: &str, port: u32) -> Result<Self> {
        let ws = utils::connect(&format!("{url}:{port}"), "/").await?;
        {
            // TODO: spawn tokio task for event loop, send results to oneshot channel
            // tokio::spawn(async move {
            //     while let Some(message_result) = ws_rx.next().await {
            //         match message_result {
            //             Ok(message) => {
            //                 if let Err(e) =
            //                     Self::handle_websocket_message(&streams, &requests, message).await
            //                 {
            //                     log::error!("{}", e);
            //                 }
            //             }
            //             Err(e) => {
            //                 log::error!("{}", e);
            //             }
            //         }
            //     }
            // });
        }

        Ok(Self {
            requests: Arc::new(RwLock::new(HashMap::new())),
            ws,
            next_id: 0,
        })
    }

    pub async fn send_request(&mut self, msg: &str) -> Result<String> {
        let request_id = self.next_id;
        self.next_id += 1;

        self.ws.send(msg).await?;

        let (tx, rx) = oneshot::channel();

        let mut requests = self.requests.write().await;
        requests.insert(request_id, tx);
        drop(requests);

        let response = rx.await.unwrap();
        if let Event::Data { ty, data } = response {
            assert!(matches!(ty, DataType::Complete(MessageType::Text)));
            return Ok(String::from_utf8(data.to_vec()).unwrap());
        }
        io_err!(InvalidData, "invalid response")
    }

    pub async fn close(self) -> Result<()> {
        self.ws.close(()).await
    }
}
