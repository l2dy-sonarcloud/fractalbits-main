use crate::response_xml::Xml;
use axum::{
    extract::Request,
    response::{self, IntoResponse, Response},
};
use bytes::Buf;
use http_body_util::BodyExt;
use rpc_client_nss::RpcClientNss;
use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CompleteMultipartUpload {
    part: Vec<Part>,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Part {
    checksum_crc32: String,
    checksum_crc32c: String,
    checksum_sha1: String,
    checksum_sha256: String,
    etag: String,
    part_number: usize,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CompleteMultipartUploadResult {
    location: String,
    bucket: String,
    key: String,
    #[serde(rename = "ETag")]
    etag: String,
    checksum_crc32: String,
    checksum_crc32c: String,
    checksum_sha1: String,
    checksum_sha256: String,
}

pub async fn complete_multipart_upload(
    request: Request,
    _key: String,
    _upload_id: String,
    _rpc_client_nss: &RpcClientNss,
) -> response::Result<Response> {
    let body = request.into_body().collect().await.unwrap().to_bytes();
    let _req_body: CompleteMultipartUpload = quick_xml::de::from_reader(body.reader()).unwrap();
    Ok(Xml(CompleteMultipartUploadResult::default()).into_response())
}
