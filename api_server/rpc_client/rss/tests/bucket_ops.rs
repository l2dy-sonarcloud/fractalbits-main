use rpc_client_rss::*;
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
async fn test_basic_bucket_ops() {
    let url = "127.0.0.1:8888";
    tracing::debug!(%url);
    if let Ok(rpc_client) = RpcClientRss::new(url).await {
        let _resp = rpc_client.put("foo".into(), "bar".into()).await.unwrap();
    }
}
