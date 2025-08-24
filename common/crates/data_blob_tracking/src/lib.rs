use bytes::Bytes;
use rpc_client_common::rpc_retry;
use rpc_client_nss::{RpcClientNss, RpcErrorNss};
use rpc_client_rss::{RpcClientRss, RpcErrorRss};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataBlobTrackingError {
    #[error("RSS error: {0}")]
    Rss(#[from] RpcErrorRss),
    #[error("NSS error: {0}")]
    Nss(#[from] RpcErrorNss),
    #[error("Blob not found")]
    NotFound,
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Simple wrapper to provide the RSS client through the rpc_retry pattern
struct RssProvider {
    rss_client: Arc<RpcClientRss>,
}

impl RssProvider {
    fn new(rss_client: Arc<RpcClientRss>) -> Self {
        Self { rss_client }
    }

    async fn checkout_rpc_client_rss(&self) -> Result<Arc<RpcClientRss>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(self.rss_client.clone())
    }
}

/// Helper struct for managing data blob tracking operations
pub struct DataBlobTracker {
    /// Cache for root blob names to avoid repeated RSS lookups
    root_blob_cache: tokio::sync::RwLock<HashMap<String, String>>,
}

impl DataBlobTracker {
    pub fn new() -> Self {
        Self {
            root_blob_cache: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Get or create root blob name for data blob tracking tree
    async fn get_or_create_data_blob_tree_root(
        &self,
        rss_client: &Arc<RpcClientRss>,
        nss_client: &RpcClientNss,
        bucket_name: &str,
    ) -> Result<String, DataBlobTrackingError> {
        let key = format!("data_blob_resync/{bucket_name}");

        // Check cache first
        {
            let cache = self.root_blob_cache.read().await;
            if let Some(root_blob_name) = cache.get(&key) {
                return Ok(root_blob_name.clone());
            }
        }

        // Try to get from RSS with retry logic
        let provider = RssProvider::new(rss_client.clone());
        match rpc_retry!(provider, checkout_rpc_client_rss(), get(&key, None)).await {
            Ok((_version, value)) => {
                // Update cache
                {
                    let mut cache = self.root_blob_cache.write().await;
                    cache.insert(key, value.clone());
                }
                Ok(value)
            }
            Err(RpcErrorRss::NotFound) => {
                // Create new tree and store root blob name
                let response = nss_client.create_root_inode(&format!("data_blob_resync_{bucket_name}"), None).await?;
                let root_blob_name = match response.result {
                    Some(resp_result) => match resp_result {
                        rpc_client_nss::rpc::create_root_inode_response::Result::Ok(name) => name,
                        _ => {
                            return Err(DataBlobTrackingError::Internal(
                                "Failed to create root inode".into(),
                            ))
                        }
                    },
                    None => {
                        return Err(DataBlobTrackingError::Internal(
                            "No result in create root inode response".into(),
                        ))
                    }
                };
                rpc_retry!(provider, checkout_rpc_client_rss(), put(0, &key, &root_blob_name, None)).await?;
                // Update cache
                {
                    let mut cache = self.root_blob_cache.write().await;
                    cache.insert(key, root_blob_name.clone());
                }
                Ok(root_blob_name)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Record a blob that exists only in local AZ
    pub async fn put_single_copy_data_blob(
        &self,
        rss_client: &Arc<RpcClientRss>,
        nss_client: &RpcClientNss,
        bucket_name: &str,
        blob_key: &str,
        metadata: &[u8],
    ) -> Result<(), DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(
                rss_client,
                nss_client,
                bucket_name,
            )
            .await?;
        let key = format!("single/{blob_key}");
        nss_client
            .put_inode(
                &root_blob_name,
                &key,
                Bytes::copy_from_slice(metadata),
                None,
            )
            .await?;
        Ok(())
    }

    /// Check if a blob is single-copy
    pub async fn get_single_copy_data_blob(
        &self,
        rss_client: &Arc<RpcClientRss>,
        nss_client: &RpcClientNss,
        bucket_name: &str,
        blob_key: &str,
    ) -> Result<Option<Vec<u8>>, DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(
                rss_client,
                nss_client,
                bucket_name,
            )
            .await?;
        let key = format!("single/{blob_key}");
        match nss_client.get_inode(&root_blob_name, &key, None).await {
            Ok(response) => {
                // Extract bytes from response
                match response.result {
                    Some(rpc_client_nss::rpc::get_inode_response::Result::Ok(bytes)) => {
                        Ok(Some(bytes.to_vec()))
                    }
                    Some(rpc_client_nss::rpc::get_inode_response::Result::ErrNotFound(_)) => {
                        Ok(None)
                    }
                    _ => Err(DataBlobTrackingError::Internal(
                        "Unexpected NSS response".into(),
                    )),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Remove blob from single-copy tracking
    pub async fn delete_single_copy_data_blob(
        &self,
        rss_client: &Arc<RpcClientRss>,
        nss_client: &RpcClientNss,
        bucket_name: &str,
        blob_key: &str,
    ) -> Result<(), DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(
                rss_client,
                nss_client,
                bucket_name,
            )
            .await?;
        let key = format!("single/{blob_key}");
        match nss_client
            .delete_inode(&root_blob_name, &key, None)
            .await
        {
            Ok(_) => Ok(()),
            Err(RpcErrorNss::NotFound) => Ok(()), // Already deleted, that's fine
            Err(e) => Err(e.into()),
        }
    }

    /// Remove blob from single-copy tracking by blob key (with null terminator handling)
    pub async fn delete_single_copy_data_blob_by_key(
        &self,
        rss_client: &Arc<RpcClientRss>,
        nss_client: &RpcClientNss,
        bucket_name: &str,
        blob_key: &str,
    ) -> Result<(), DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(
                rss_client,
                nss_client,
                bucket_name,
            )
            .await?;

        // Use the blob key with single prefix, trimming null terminators
        let trimmed_key = blob_key.trim_end_matches('\0');
        let key = format!("single/{trimmed_key}");
        match nss_client.delete_inode(&root_blob_name, &key, None).await {
            Ok(_) => Ok(()),
            Err(RpcErrorNss::NotFound) => Ok(()), // Already deleted, that's fine
            Err(e) => Err(e.into()),
        }
    }

    /// Record a deleted blob to skip during resync
    pub async fn put_deleted_data_blob(
        &self,
        rss_client: &Arc<RpcClientRss>,
        nss_client: &RpcClientNss,
        bucket_name: &str,
        blob_key: &str,
        timestamp: &[u8],
    ) -> Result<(), DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(
                rss_client,
                nss_client,
                bucket_name,
            )
            .await?;
        let key = format!("deleted/{blob_key}");
        nss_client
            .put_inode(
                &root_blob_name,
                &key,
                Bytes::copy_from_slice(timestamp),
                None,
            )
            .await?;
        Ok(())
    }

    /// Check if a blob is marked as deleted
    pub async fn get_deleted_data_blob(
        &self,
        rss_client: &Arc<RpcClientRss>,
        nss_client: &RpcClientNss,
        bucket_name: &str,
        blob_key: &str,
    ) -> Result<Option<Vec<u8>>, DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(
                rss_client,
                nss_client,
                bucket_name,
            )
            .await?;
        let key = format!("deleted/{blob_key}");
        match nss_client.get_inode(&root_blob_name, &key, None).await {
            Ok(response) => {
                // Extract bytes from response
                match response.result {
                    Some(rpc_client_nss::rpc::get_inode_response::Result::Ok(bytes)) => {
                        Ok(Some(bytes.to_vec()))
                    }
                    Some(rpc_client_nss::rpc::get_inode_response::Result::ErrNotFound(_)) => {
                        Ok(None)
                    }
                    _ => Err(DataBlobTrackingError::Internal(
                        "Unexpected NSS response".into(),
                    )),
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    /// Get deleted data blob tracking entry by blob key
    pub async fn get_deleted_data_blob_by_key(
        &self,
        rss_client: &Arc<RpcClientRss>,
        nss_client: &RpcClientNss,
        bucket_name: &str,
        blob_key: &str,
    ) -> Result<Option<Vec<u8>>, DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(
                rss_client,
                nss_client,
                bucket_name,
            )
            .await?;

        // Use the blob key with deleted prefix, trimming null terminators
        let trimmed_key = blob_key.trim_end_matches('\0');
        let key = format!("deleted/{trimmed_key}");
        match nss_client.get_inode(&root_blob_name, &key, None).await {
            Ok(response) => match response.result {
                Some(rpc_client_nss::rpc::get_inode_response::Result::Ok(bytes)) => {
                    Ok(Some(bytes.to_vec()))
                }
                Some(rpc_client_nss::rpc::get_inode_response::Result::ErrNotFound(_)) => Ok(None),
                _ => Err(DataBlobTrackingError::Internal(
                    "Unexpected NSS response".into(),
                )),
            },
            Err(e) => Err(e.into()),
        }
    }

    /// Remove blob from deleted tracking (used during sanitize)
    pub async fn delete_deleted_data_blob(
        &self,
        rss_client: &Arc<RpcClientRss>,
        nss_client: &RpcClientNss,
        bucket_name: &str,
        blob_key: &str,
    ) -> Result<(), DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(
                rss_client,
                nss_client,
                bucket_name,
            )
            .await?;
        let key = format!("deleted/{blob_key}");
        match nss_client
            .delete_inode(&root_blob_name, &key, None)
            .await
        {
            Ok(_) => Ok(()),
            Err(RpcErrorNss::NotFound) => Ok(()), // Already deleted, that's fine
            Err(e) => Err(e.into()),
        }
    }

    /// List single-copy data blobs for resync
    pub async fn list_single_copy_data_blobs(
        &self,
        rss_client: &Arc<RpcClientRss>,
        nss_client: &RpcClientNss,
        bucket_name: &str,
        prefix: &str,
        start_after: &str,
        max_keys: u32,
    ) -> Result<Vec<(String, String)>, DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(
                rss_client,
                nss_client,
                bucket_name,
            )
            .await?;

        // Add single/ prefix to the search parameters
        let search_prefix = if prefix.is_empty() {
            "single/".to_string()
        } else {
            format!("single/{prefix}")
        };
        let search_start_after = if start_after.is_empty() {
            "".to_string() // Let NSS handle empty start_after with the prefix
        } else {
            format!("single/{start_after}")
        };

        let response = nss_client
            .list_inodes(
                &root_blob_name,
                max_keys,
                &search_prefix,
                "",
                &search_start_after,
                false,
                None,
            )
            .await?;

        // Extract the list from the response
        match response.result {
            Some(rpc_client_nss::rpc::list_inodes_response::Result::Ok(inodes)) => {
                let result = inodes
                    .inodes
                    .into_iter()
                    .filter_map(|inode| {
                        // Strip the "single/" prefix from the key
                        if let Some(blob_key) = inode.key.strip_prefix("single/") {
                            let missing_az = String::from_utf8_lossy(&inode.inode).to_string();
                            Some((blob_key.to_string(), missing_az))
                        } else {
                            None
                        }
                    })
                    .collect();
                Ok(result)
            }
            _ => Err(DataBlobTrackingError::Internal(
                "Unexpected NSS list response".into(),
            )),
        }
    }

    /// List deleted data blobs for sanitize
    pub async fn list_deleted_data_blobs(
        &self,
        rss_client: &Arc<RpcClientRss>,
        nss_client: &RpcClientNss,
        bucket_name: &str,
        prefix: &str,
        start_after: &str,
        max_keys: u32,
    ) -> Result<Vec<(String, Vec<u8>)>, DataBlobTrackingError> {
        let root_blob_name = self
            .get_or_create_data_blob_tree_root(
                rss_client,
                nss_client,
                bucket_name,
            )
            .await?;

        // Add deleted/ prefix to the search parameters
        let search_prefix = if prefix.is_empty() {
            "deleted/".to_string()
        } else {
            format!("deleted/{prefix}")
        };
        let search_start_after = if start_after.is_empty() {
            "".to_string() // Let NSS handle empty start_after with the prefix
        } else {
            format!("deleted/{start_after}")
        };

        let response = nss_client
            .list_inodes(
                &root_blob_name,
                max_keys,
                &search_prefix,
                "",
                &search_start_after,
                false,
                None,
            )
            .await?;

        // Extract the list from the response
        match response.result {
            Some(rpc_client_nss::rpc::list_inodes_response::Result::Ok(inodes)) => {
                let result = inodes
                    .inodes
                    .into_iter()
                    .filter_map(|inode| {
                        // Strip the "deleted/" prefix from the key
                        if let Some(blob_key) = inode.key.strip_prefix("deleted/") {
                            Some((blob_key.to_string(), inode.inode.to_vec()))
                        } else {
                            None
                        }
                    })
                    .collect();
                Ok(result)
            }
            _ => Err(DataBlobTrackingError::Internal(
                "Unexpected NSS list response".into(),
            )),
        }
    }

    /// List all buckets that have data blob tracking data
    pub async fn list_tracked_buckets(
        &self,
        rss_client: &Arc<RpcClientRss>,
    ) -> Result<Vec<String>, DataBlobTrackingError> {
        // List all keys with the data_blob_resync prefix
        let prefix = "data_blob_resync/";
        let provider = RssProvider::new(rss_client.clone());
        let keys = rpc_retry!(provider, checkout_rpc_client_rss(), list(prefix, None)).await?;

        // Extract bucket names from keys like "data_blob_resync/bucket-name"
        let bucket_names: Vec<String> = keys
            .into_iter()
            .filter_map(|key| {
                key.strip_prefix("data_blob_resync/")
                    .map(|bucket_name| bucket_name.to_string())
            })
            .collect();

        Ok(bucket_names)
    }

    /// Get current timestamp as bytes for deleted blob tracking
    pub fn current_timestamp_bytes() -> Vec<u8> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        timestamp.to_le_bytes().to_vec()
    }
}

impl Default for DataBlobTracker {
    fn default() -> Self {
        Self::new()
    }
}
