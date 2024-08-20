use api_server::{nss_get_inode, nss_put_inode};
use axum::{extract::Path, routing::get, Router};

async fn get_obj(Path(key): Path<String>) -> String {
    let resp = nss_get_inode(key).await;
    serde_json::to_string_pretty(&resp).unwrap()
}

async fn put_obj(Path(key): Path<String>, value: String) -> String {
    let resp = nss_put_inode(key, value).await;
    serde_json::to_string_pretty(&resp).unwrap()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let app = Router::new().route("/:key", get(get_obj).post(put_obj));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
