use crate::response_xml::Xml;
use axum::{
    extract::Request,
    response::{self, IntoResponse, Response},
};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use serde::Serialize;

#[allow(dead_code)]
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct ResponseHeaders {
    x_amz_abort_date: String,
    x_amz_abort_rule_id: String,
    x_amz_server_side_encryption: String,
    x_amz_server_side_encryption_customer_algorithm: String,
    #[serde(rename = "x-amz-server-side-encryption-customer-key-MD5")]
    x_amz_server_side_encryption_customer_key_md5: String,
    x_amz_server_side_encryption_aws_kms_key_id: String,
    x_amz_server_side_encryption_context: String,
    x_amz_server_side_encryption_bucket_key_enabled: String,
    x_amz_request_charged: String,
    x_amz_checksum_algorithm: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct InitiateMultipartUploadResult {
    bucket: String,
    key: String,
    upload_id: String,
}

pub async fn create_multipart_upload(
    _request: Request,
    _key: String,
    _rpc_client_nss: &RpcClientNss,
    _rpc_client_bss: &RpcClientBss,
) -> response::Result<Response> {
    Ok(Xml(InitiateMultipartUploadResult::default()).into_response())
}
