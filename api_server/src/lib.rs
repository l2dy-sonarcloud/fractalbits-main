mod utils;
mod ws_client;

#[macro_export]
macro_rules! io_err {
    [$kind: ident, $msg: expr] => {
        return Err(std::io::Error::new(std::io::ErrorKind::$kind, $msg))
    };
}

pub use ws_client::RpcClient;

pub mod nss_ops {
    include!(concat!(env!("OUT_DIR"), "/nss_ops.rs"));
}
use nss_ops::*;

pub async fn nss_put_inode(key: String, value: String) -> PutInodeResponse {
    let mut request = PutInodeRequest::default();
    request.key = key;
    request.value = value;

    let mut rpc_client = RpcClient::new("127.0.0.1", 9224).await.unwrap();
    let response_str = rpc_client.send_request(&request.key).await.unwrap();
    assert_eq!(request.key, response_str);
    rpc_client.close().await.unwrap();

    PutInodeResponse {
        msg_id: 1,
        result: Some(put_inode_response::Result::Ok(())),
    }
}

pub async fn nss_get_inode(key: String) -> GetInodeResponse {
    let mut request = GetInodeRequest::default();
    request.key = key;

    let mut rpc_client = RpcClient::new("127.0.0.1", 9224).await.unwrap();
    let response_str = rpc_client.send_request(&request.key).await.unwrap();
    assert_eq!(request.key, response_str);
    rpc_client.close().await.unwrap();

    GetInodeResponse {
        msg_id: 1,
        result: Some(get_inode_response::Result::Value("success".to_owned())),
    }
}
