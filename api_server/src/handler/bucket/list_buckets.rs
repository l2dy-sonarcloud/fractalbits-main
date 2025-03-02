use axum::{
    extract::{Query, Request},
    response::{IntoResponse, Response},
    RequestExt,
};
use bucket_tables::{bucket_table::BucketTable, table::Table};
use rpc_client_rss::ArcRpcClientRss;
use serde::{Deserialize, Serialize};

use crate::handler::common::{response::xml::Xml, s3_error::S3Error};

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListBucketsOptions {
    bucket_region: Option<String>,
    continuation_token: Option<String>,
    max_buckets: Option<u32>,
    prefix: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListAllMyBucketsResult {
    buckets: Vec<Bucket>,
    owner: Owner,
    continuation_token: String,
    prefix: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Bucket {
    bucket_region: String,
    creation_date: String, // timestamp
    name: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Owner {
    display_name: String,
    id: String,
}

pub async fn list_buckets(
    mut request: Request,
    rpc_client_rss: ArcRpcClientRss,
) -> Result<Response, S3Error> {
    let Query(_opts): Query<ListBucketsOptions> = request.extract_parts().await?;
    let _bucket_table: Table<ArcRpcClientRss, BucketTable> = Table::new(rpc_client_rss.clone());
    Ok(Xml(ListAllMyBucketsResult::default()).into_response())
}
