use nss_rpc_client::rpc_client::RpcClient;
use std::collections::HashMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use strum::EnumString;

use super::extract::BucketName;
use super::AppState;
use axum::{
    extract::{ConnectInfo, Path, Query, State},
    http::StatusCode,
};
pub const MAX_NSS_CONNECTION: usize = 8;

#[derive(Debug, EnumString, Copy, Clone, strum::Display)]
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

pub async fn get_handler(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    BucketName(_bucket): BucketName,
    Query(query_map): Query<HashMap<String, String>>,
    Path(key): Path<String>,
) -> Result<String, (StatusCode, String)> {
    let api_command = get_api_command(&query_map);
    let key = key_for_nss(key);
    let rpc_client = get_rpc_client(&state, addr);

    match api_command {
        Some(api_command) => panic!("TODO: {api_command:?}"),
        None => {
            let resp = nss_rpc_client::nss_get_inode(rpc_client, key)
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
    }
}

pub async fn put_handler(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    BucketName(_bucket): BucketName,
    Query(query_map): Query<HashMap<String, String>>,
    Path(key): Path<String>,
    value: String,
) -> Result<String, (StatusCode, String)> {
    let api_command = get_api_command(&query_map);
    let key = key_for_nss(key);
    let rpc_client = get_rpc_client(&state, addr);

    match api_command {
        Some(api_command) => panic!("TODO: {api_command:?}"),
        None => {
            let resp = nss_rpc_client::nss_put_inode(rpc_client, key, value)
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
    }
}

fn get_rpc_client(app_state: &AppState, addr: SocketAddr) -> &RpcClient {
    fn calculate_hash<T: Hash>(t: &T) -> usize {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish() as usize
    }
    let hash = calculate_hash(&addr) % MAX_NSS_CONNECTION;
    &app_state.rpc_clients[hash]
}

fn get_api_command(query_params: &HashMap<String, String>) -> Option<ApiCommand> {
    let api_commands: Vec<ApiCommand> = query_params
        .iter()
        .filter_map(|(k, v)| v.is_empty().then_some(k))
        .filter_map(|cmd| ApiCommand::from_str(cmd).ok())
        .collect();
    if api_commands.is_empty() {
        None
    } else {
        if api_commands.len() > 1 {
            tracing::debug!("Multiple api command found: {api_commands:?}, pick up the first one");
        }
        Some(api_commands[0])
    }
}

fn key_for_nss(key: String) -> String {
    if key.is_empty() {
        return key;
    }
    let mut key = format!("/{key}");
    key.push('\0');
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request, routing::get, Router};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    fn app() -> Router {
        Router::new().route("/*key", get(handler))
    }

    async fn handler(Query(query_map): Query<HashMap<String, String>>) -> String {
        get_api_command(&query_map)
            .map(|cmd| cmd.to_string())
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn test_extract_api_command_ok() {
        let api_cmd = "acl";
        assert_eq!(send_request_get_body(api_cmd).await, api_cmd);
    }

    #[tokio::test]
    async fn test_extract_api_command_null() {
        let api_cmd = "";
        assert_eq!(send_request_get_body(api_cmd).await, api_cmd);
    }

    async fn send_request_get_body(api_cmd: &str) -> String {
        let api_cmd = if api_cmd.is_empty() {
            ""
        } else {
            &format!("?{api_cmd}")
        };
        let body = app()
            .oneshot(
                Request::builder()
                    .uri(format!("http://my-bucket.localhost/obj1{api_cmd}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
            .into_body();
        let bytes = body.collect().await.unwrap().to_bytes();
        String::from_utf8(bytes.to_vec()).unwrap()
    }
}
