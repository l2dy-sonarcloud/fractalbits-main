mod utils;
mod ws_client;

pub use ws_client::rpc_to_nss;

pub mod nss_ops {
    include!(concat!(env!("OUT_DIR"), "/nss_ops.rs"));
}
use nss_ops::*;

pub fn nss_put_inode(key: String, value: String) -> PutInodeResponse {
    let mut _request = PutInodeRequest::default();
    _request.key = key;
    _request.value = value;

    let mut response = PutInodeResponse::default();
    response.result = Some(put_inode_response::Result::Ok(()));
    response
}

pub fn nss_get_inode(key: String) -> GetInodeResponse {
    let mut _request = GetInodeRequest::default();
    _request.key = key;

    let mut response = GetInodeResponse::default();
    response.result = Some(get_inode_response::Result::Value("success".to_owned()));
    response
}
