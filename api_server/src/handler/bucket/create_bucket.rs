use axum::{extract::Request, response};
use bytes::Buf;
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CreateBucketConfiguration {
    location_constraint: String,
    location: Location,
    bucket: Bucket,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Location {
    name: String,
    #[serde(rename = "Type")]
    location_type: String,
}

#[derive(Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Bucket {
    data_redundancy: String,
    #[serde(rename = "Type")]
    bucket_type: String,
}

pub async fn create_bucket(_bucket_name: String, request: Request) -> response::Result<()> {
    let body = request.into_body().collect().await.unwrap().to_bytes();
    let _req_body: CreateBucketConfiguration = quick_xml::de::from_reader(body.reader()).unwrap();
    Ok(())
}
