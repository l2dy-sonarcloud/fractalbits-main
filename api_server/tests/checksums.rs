// convert from aws's s3 rust sdk tests
mod common;
use aws_sdk_s3::{
    operation::get_object::GetObjectOutput, types::ChecksumAlgorithm, types::ChecksumMode,
};
use common::Context;

// The test structure is identical for all supported checksum algorithms
async fn test_checksum_on_streaming<'a>(
    body: &'static [u8],
    checksum_algorithm: ChecksumAlgorithm,
) -> GetObjectOutput {
    let ctx = common::context();
    let bucket = ctx.create_bucket("test-bucket").await;
    let key = "test.txt";

    // ByteStreams created from a file are streaming and have a known size
    let mut file = tempfile::NamedTempFile::new().unwrap();
    use std::io::Write;
    file.write_all(body).unwrap();

    let stream_body = aws_sdk_s3::primitives::ByteStream::read_from()
        .path(file.path())
        .buffer_size(1024)
        .build()
        .await
        .unwrap();

    // The response from the fake connection won't return the expected XML but we don't care about
    // that error in this test
    let _ = ctx
        .client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(stream_body)
        .checksum_algorithm(checksum_algorithm)
        .send()
        .await
        .unwrap();

    ctx.client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap()
}

#[ignore = "todo"]
#[tokio::test]
async fn test_crc32_checksum_on_streaming_request() {
    let ctx = common::context();
    let bucket = ctx.create_bucket("test-bucket").await;
    let key = "test.txt";

    let res = test_checksum_on_streaming(b"Hello world", ChecksumAlgorithm::Crc32).await;
    // Header checksums are base64 encoded
    assert_eq!(res.checksum_crc32(), Some("i9aeUg=="));
    assert_bytes_eq!(res.body, b"Hello world");

    cleanup(&ctx, &bucket, key).await;
}

// This test isn't a duplicate. It tests CRC32C (note the C) checksum request validation
#[ignore = "todo"]
#[tokio::test]
async fn test_crc32c_checksum_on_streaming_request() {
    let ctx = common::context();
    let bucket = ctx.create_bucket("test-bucket").await;
    let key = "test.txt";

    let res = test_checksum_on_streaming(b"Hello world", ChecksumAlgorithm::Crc32C).await;
    // Header checksums are base64 encoded
    assert_eq!(res.checksum_crc32_c(), Some("crUfeA=="));
    assert_bytes_eq!(res.body, b"Hello world");

    cleanup(&ctx, &bucket, key).await;
}

#[ignore = "todo"]
#[tokio::test]
async fn test_sha1_checksum_on_streaming_request() {
    let ctx = common::context();
    let bucket = ctx.create_bucket("test-bucket").await;
    let key = "test.txt";

    let res = test_checksum_on_streaming(b"Hello world", ChecksumAlgorithm::Sha1).await;
    // Header checksums are base64 encoded
    assert_eq!(res.checksum_sha1(), Some("e1AsOh9IyGCa4hLN+2Od7jlnP14="));
    assert_bytes_eq!(res.body, b"Hello world");

    cleanup(&ctx, &bucket, key).await;
}

#[ignore = "todo"]
#[tokio::test]
async fn test_sha256_checksum_on_streaming_request() {
    let ctx = common::context();
    let bucket = ctx.create_bucket("test-bucket").await;
    let key = "test.txt";

    let res = test_checksum_on_streaming(b"Hello world", ChecksumAlgorithm::Sha256).await;
    // Header checksums are base64 encoded
    assert_eq!(
        res.checksum_sha256(),
        Some("ZOyIygCyaOW6GjVnihtTFtIS9PNmskdyMlNKiuyjfzw=")
    );
    assert_bytes_eq!(res.body, b"Hello world");

    cleanup(&ctx, &bucket, key).await;
}

async fn cleanup(ctx: &Context, bucket: &str, key: &str) {
    ctx.client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    ctx.delete_bucket(bucket).await;
}
