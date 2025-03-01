use std::sync::Arc;

use crate::handler::common::{response::xml::Xml, s3_error::S3Error};
use axum::{
    extract::{Query, Request},
    response::{IntoResponse, Response},
    RequestExt,
};
use bucket_tables::bucket_table::Bucket;
use rkyv::{self, rancor::Error};
use rpc_client_nss::{rpc::list_inodes_response, RpcClientNss};
use serde::{Deserialize, Serialize};

use crate::object_layout::ObjectLayout;

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ListObjectsV2Options {
    list_type: Option<String>,
    continuation_token: Option<String>,
    delimiter: Option<String>,
    encoding_type: Option<String>,
    fetch_owner: Option<bool>,
    max_keys: Option<u32>,
    prefix: Option<String>,
    start_after: Option<String>,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    is_truncated: bool,
    contents: Vec<Contents>,
    name: String,
    prefix: String,
    delimiter: String,
    max_keys: u32,
    common_prefixes: Vec<CommonPrefixes>,
    encoding_type: String,
    key_count: usize,
    continuation_token: String,
    next_continuation_token: String,
    start_after: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Contents {
    checksum_algorithm: String,
    etag: String,
    key: String,
    last_modified: String, // timestamp
    owner: Owner,
    restore_status: RestoreStatus,
    size: usize,
    storage_class: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct Owner {
    display_name: String,
    id: String,
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct RestoreStatus {
    is_restore_in_progress: bool,
    restore_expiry_date: String, // timestamp
}

#[derive(Default, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
struct CommonPrefixes {
    prefix: String,
}

pub async fn list_objects_v2(
    mut request: Request,
    bucket: Arc<Bucket>,
    rpc_client_nss: &RpcClientNss,
) -> Result<Response, S3Error> {
    let Query(opts): Query<ListObjectsV2Options> = request.extract_parts().await?;
    tracing::debug!("list_objects_v2 {opts:?}");

    // Sanity checks
    if opts.list_type != Some("2".into()) {
        tracing::warn!(
            "expecting list_type as \"2\" only, got {:?}",
            opts.list_type
        );
        return Err(S3Error::InvalidArgument1);
    }
    if let Some(encoding_type) = opts.encoding_type {
        if encoding_type != "url" {
            tracing::warn!(
                "expecting content_type as \"url\" only, got {}",
                encoding_type
            );
            return Err(S3Error::InvalidArgument1);
        }
    }

    let max_keys = opts.max_keys.unwrap_or(1000);
    let prefix = opts.prefix.unwrap_or("/".into());
    let start_after = opts.start_after.unwrap_or_default();
    let resp = rpc_client_nss
        .list_inodes(
            bucket.root_blob_name.clone(),
            max_keys,
            prefix,
            start_after,
            true,
        )
        .await?;

    // Process results
    let inodes = match resp.result.unwrap() {
        list_inodes_response::Result::Ok(res) => res.inodes,
        list_inodes_response::Result::Err(e) => {
            tracing::error!(e);
            return Err(S3Error::InternalError);
        }
    };

    for inode in inodes {
        let _object = rkyv::from_bytes::<ObjectLayout, Error>(&inode.inode)?;
        // dbg!(&inode.key);
        // dbg!(object.timestamp);
        // dbg!(object.size);
    }

    Ok(Xml(ListBucketResult::default()).into_response())
}
