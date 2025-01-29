#![allow(dead_code)]
use super::table::{Entry, TableSchema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

type BucketKeyPerm = bool; // TODO: real bucket pey permissions

#[derive(Serialize, Deserialize)]
pub struct Bucket {
    pub bucket_name: String,
    pub creation_date: u64,
    pub authorized_keys: HashMap<String /* ApiKey id */, BucketKeyPerm>,
    pub root_blob_id: String,
}

impl Entry for Bucket {
    fn key(&self) -> String {
        self.bucket_name.clone()
    }
}

pub struct BucketTable;

impl TableSchema for BucketTable {
    const TABLE_NAME: &'static str = "buckets";

    type E = Bucket;
}
