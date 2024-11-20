use axum::{extract::Request, response};
use rpc_client_bss::RpcClientBss;
use rpc_client_nss::RpcClientNss;
use serde::Serialize;

#[allow(dead_code)]
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
struct ResponseHeaders {
    x_amz_server_side_encryption: String,
    #[serde(rename = "ETag")]
    etag: String,
    x_amz_checksum_crc32: String,
    x_amz_checksum_crc32c: String,
    x_amz_checksum_sha1: String,
    x_amz_checksum_sha256: String,
    x_amz_server_side_encryption_customer_algorithm: String,
    #[serde(rename = "x-amz-server-side-encryption-customer-key-MD5")]
    x_amz_server_side_encryption_customer_key_md5: String,
    x_amz_server_side_encryption_aws_kms_key_id: String,
    x_amz_server_side_encryption_bucket_key_enabled: String,
    x_amz_request_charged: String,
}

#[allow(dead_code)]
#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct InitiateMultipartUploadResult {
    bucket: String,
    key: String,
    upload_id: String,
}

pub async fn upload_part(
    _request: Request,
    _key: String,
    _part_number: u64,
    _upload_id: String,
    _rpc_client_nss: &RpcClientNss,
    _rpc_client_bss: &RpcClientBss,
) -> response::Result<()> {
    Ok(())
}
