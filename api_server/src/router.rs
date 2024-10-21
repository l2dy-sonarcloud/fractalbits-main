use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use strum::EnumString;

use super::extract::BucketName;
use super::AppState;
use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::StatusCode,
};

#[derive(Debug, EnumString)]
#[strum(serialize_all = "kebab-case")]
enum ApiCommand {
    Accelerate,
    Acl,
    Analytics,
    Cors,
    Delete,
    Encryption,
    IntelligentTiering,
    Inventory,
    LegalHold,
    Lifecycle,
    Location,
    Logging,
    Metrics,
    Notification,
    ObjectLock,
    OwnershipControls,
    Policy,
    PolicyStatus,
    PublicAccessBlock,
    Replication,
    RequestPayment,
    Restore,
    Retention,
    Select,
    Tagging,
    Torrent,
    Uploads,
    Versioning,
    Versions,
    Website,
}

pub const MAX_NSS_CONNECTION: usize = 8;

pub async fn get_handler(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    BucketName(_bucket): BucketName,
    Query(_query_map): Query<HashMap<String, String>>,
    Path(key): Path<String>,
) -> Result<String, (StatusCode, String)> {
    let hash = calculate_hash(&addr) % MAX_NSS_CONNECTION;
    let mut key = format!("/{key}");
    key.push('\0');
    let resp = nss_rpc_client::nss_get_inode(&state.rpc_clients[hash], key)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        .unwrap();
    match serde_json::to_string_pretty(&resp.result) {
        Ok(resp) => Ok(resp),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("internal server error: {e}"),
        )),
    }
}

pub async fn put_handler(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    BucketName(_bucket): BucketName,
    Query(_query_map): Query<HashMap<String, String>>,
    Path(key): Path<String>,
    value: String,
) -> Result<String, (StatusCode, String)> {
    let hash = calculate_hash(&addr) % MAX_NSS_CONNECTION;
    let mut key = format!("/{key}");
    key.push('\0');
    let resp = nss_rpc_client::nss_put_inode(&state.rpc_clients[hash], key, value)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        .unwrap();
    match serde_json::to_string_pretty(&resp.result) {
        Ok(resp) => Ok(resp),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("internal server error: {e}"),
        )),
    }
}

fn calculate_hash<T: Hash>(t: &T) -> usize {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() as usize
}
