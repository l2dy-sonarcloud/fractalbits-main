use axum::{extract::Request, response};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;

pub async fn abort_multipart_upload(
    _request: Request,
    _key: String,
    _upload_id: String,
    _rpc_client_nss: &RpcClientNss,
    _rpc_client_bss: &RpcClientBss,
) -> response::Result<()> {
    Ok(())
}
