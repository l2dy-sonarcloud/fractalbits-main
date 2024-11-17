use axum::{
    extract::{Query, Request},
    http::StatusCode,
    response::{self, IntoResponse},
    RequestExt,
};
use rpc_client_nss::RpcClientNss;
use serde::Deserialize;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct HeadObjectOptions {
    #[serde(rename(deserialize = "partNumber"))]
    part_number: Option<u64>,
    #[serde(rename(deserialize = "versionId"))]
    version_id: Option<String>,
    response_cache_control: Option<String>,
    response_content_disposition: Option<String>,
    response_content_encoding: Option<String>,
    response_content_language: Option<String>,
    response_content_type: Option<String>,
    response_expires: Option<String>,
}

pub async fn head_object(
    mut request: Request,
    key: String,
    rpc_client_nss: &RpcClientNss,
) -> response::Result<()> {
    let Query(_opts): Query<HeadObjectOptions> = request.extract_parts().await?;
    let _resp = rpc_client_nss
        .get_inode(key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;

    Ok(())
}
