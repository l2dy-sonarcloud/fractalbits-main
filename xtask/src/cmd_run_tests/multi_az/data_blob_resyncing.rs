use crate::cmd_service::{start_services, stop_service, wait_for_service_ready};
use crate::{BuildMode, CmdResult, DataBlobStorage, ServiceName};
use aws_sdk_s3::primitives::ByteStream;
use cmd_lib::*;
use colored::*;
use std::time::Duration;
use test_common::*;
use tokio::time::sleep;

pub async fn run_data_blob_resyncing_tests() -> CmdResult {
    info!("Running data blob resyncing tests...");

    // Initialize and start all required services
    info!("Initializing and starting services for resyncing tests...");
    crate::cmd_service::stop_service(ServiceName::All)?;
    crate::cmd_build::build_rust_servers(BuildMode::Debug)?;
    crate::cmd_service::init_service(ServiceName::All, BuildMode::Debug)?;
    crate::cmd_service::start_services(
        ServiceName::All,
        BuildMode::Debug,
        false,
        DataBlobStorage::S3ExpressMultiAz,
    )?;

    info!("All services started successfully");

    // Run all test scenarios
    println!("\n{}", "=== Test 1: Basic Resync Functionality ===".bold());
    if let Err(e) = test_basic_functionality().await {
        eprintln!("{}: {}", "Test 1 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test 2: Resync with Deleted Blobs ===".bold());
    if let Err(e) = test_with_deleted_blobs().await {
        eprintln!("{}: {}", "Test 2 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test 3: Dry Run Mode ===".bold());
    if let Err(e) = test_dry_run_mode().await {
        eprintln!("{}: {}", "Test 3 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("\n{}", "=== Test 4: Resume Functionality ===".bold());
    if let Err(e) = test_resume_functionality().await {
        eprintln!("{}: {}", "Test 4 FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "\n{}",
        "=== All Data Blob Resyncing Tests PASSED ==="
            .green()
            .bold()
    );
    Ok(())
}

async fn test_basic_functionality() -> CmdResult {
    let ctx = context();
    let bucket_name = ctx.create_bucket("test-resync-basic").await;

    println!("Testing data_blob_resync_server basic functionality...");

    // Step 1: Upload initial objects with both AZs online
    println!("  Step 1: Uploading objects with both AZs online");
    let initial_objects = vec![
        ("resync-test-1", b"Initial data object 1"),
        ("resync-test-2", b"Initial data object 2"),
    ];

    for (key, data) in &initial_objects {
        ctx.client
            .put_object()
            .bucket(&bucket_name)
            .key(*key)
            .body(ByteStream::from_static(*data))
            .send()
            .await
            .expect("Failed to upload initial object");
    }

    // Step 2: Simulate remote AZ going down
    println!("  Step 2: Simulating remote AZ outage");
    stop_service(ServiceName::MinioAz2)?;
    sleep(Duration::from_secs(2)).await;

    // Step 3: Upload objects during outage (these should be tracked for resync)
    println!("  Step 3: Uploading objects during outage");
    let outage_objects = vec![
        ("resync-outage-1", b"Data uploaded during outage 1"),
        ("resync-outage-2", b"Data uploaded during outage 2"),
        ("resync-outage-3", b"Data uploaded during outage 3"),
        ("resync-outage-4", b"Data uploaded during outage 4"),
        ("resync-outage-5", b"Data uploaded during outage 5"),
    ];

    for (key, data) in &outage_objects {
        println!("    Uploading {key} during outage");
        ctx.client
            .put_object()
            .bucket(&bucket_name)
            .key(*key)
            .body(ByteStream::from_static(*data))
            .send()
            .await
            .expect("Failed to upload object during outage");

        // Verify immediate access
        let response = ctx
            .client
            .get_object()
            .bucket(&bucket_name)
            .key(*key)
            .send()
            .await
            .expect("Failed to get object during outage");

        let body = response.body.collect().await.expect("Failed to read body");
        assert_eq!(body.into_bytes().as_ref(), *data);
    }

    // Step 4: Check resync server status while remote AZ is down
    println!("  Step 4: Checking resync server status during outage");
    run_resync_server_command("status", false, None)?;
    println!("    OK: Resync server status check completed");

    // Step 5: Bring remote AZ back online
    println!("  Step 5: Bringing remote AZ back online");
    start_services(
        ServiceName::MinioAz2,
        BuildMode::Debug,
        false,
        DataBlobStorage::default(),
    )?;
    wait_for_service_ready(ServiceName::MinioAz2, 30)?;

    // Step 6: Run resync operation to copy single-copy blobs
    println!("  Step 6: Running data blob resync operation");
    run_resync_server_command("resync", false, None)?;
    println!("    OK: Resync operation completed successfully");

    // Step 7: Verify all objects are still accessible after resync
    println!("  Step 7: Verifying data integrity after resync");

    // Check initial objects
    for (key, expected_data) in &initial_objects {
        let response = ctx
            .client
            .get_object()
            .bucket(&bucket_name)
            .key(*key)
            .send()
            .await
            .expect("Failed to get initial object after resync");

        let body = response.body.collect().await.expect("Failed to read body");
        assert_eq!(body.into_bytes().as_ref(), *expected_data);
        println!("    OK: Initial object {key} verified");
    }

    // Check outage objects
    for (key, expected_data) in &outage_objects {
        let response = ctx
            .client
            .get_object()
            .bucket(&bucket_name)
            .key(*key)
            .send()
            .await
            .expect("Failed to get outage object after resync");

        let body = response.body.collect().await.expect("Failed to read body");
        assert_eq!(body.into_bytes().as_ref(), *expected_data);
        println!("    OK: Outage object {key} verified");
    }

    // Step 8: Verify resync server status shows no pending blobs
    println!("  Step 8: Verifying no pending blobs remain");
    run_resync_server_command("status", false, None)?;
    println!("    OK: Final status check completed");

    println!("SUCCESS: Data blob resync server basic functionality test completed!");
    Ok(())
}

async fn test_with_deleted_blobs() -> CmdResult {
    let ctx = context();
    let bucket_name = ctx.create_bucket("test-resync-with-deletes").await;

    println!("Testing resync server with blob deletions during outage...");

    // Step 1: Upload initial objects
    println!("  Step 1: Uploading initial objects");
    let initial_keys = vec!["delete-test-1", "delete-test-2", "keep-test-1"];
    for key in &initial_keys {
        ctx.client
            .put_object()
            .bucket(&bucket_name)
            .key(*key)
            .body(ByteStream::from(
                format!("Initial data for {key}").into_bytes(),
            ))
            .send()
            .await
            .expect("Failed to upload initial object");
    }

    // Step 2: Stop remote AZ
    println!("  Step 2: Stopping remote AZ");
    stop_service(ServiceName::MinioAz2)?;
    sleep(Duration::from_secs(2)).await;

    // Step 3: Upload new objects during outage
    println!("  Step 3: Uploading objects during outage");
    let outage_keys = vec!["outage-1", "outage-2", "outage-delete-me"];
    for key in &outage_keys {
        ctx.client
            .put_object()
            .bucket(&bucket_name)
            .key(*key)
            .body(ByteStream::from(
                format!("Outage data for {key}").into_bytes(),
            ))
            .send()
            .await
            .expect("Failed to upload object during outage");
    }

    // Step 4: Delete some objects during outage (these should be tracked)
    println!("  Step 4: Deleting objects during outage");
    let to_delete = vec!["delete-test-1", "outage-delete-me"];
    for key in &to_delete {
        println!("    Deleting {key} during outage");
        ctx.client
            .delete_object()
            .bucket(&bucket_name)
            .key(*key)
            .send()
            .await
            .expect("Failed to delete object during outage");
    }

    // Step 5: Restart remote AZ
    println!("  Step 5: Restarting remote AZ");
    start_services(
        ServiceName::MinioAz2,
        BuildMode::Debug,
        false,
        DataBlobStorage::default(),
    )?;
    wait_for_service_ready(ServiceName::MinioAz2, 30)?;

    // Step 6: Run resync operation (should handle deleted blobs correctly)
    println!("  Step 6: Running resync operation");
    run_resync_server_command("resync", false, None)?;

    // Step 7: Run sanitize operation to clean up deleted blob tracking
    println!("  Step 7: Running sanitize operation");
    run_resync_server_command("sanitize", false, None)?;

    // Step 8: Verify surviving objects are accessible
    println!("  Step 8: Verifying surviving objects");
    let surviving_keys = vec!["delete-test-2", "keep-test-1", "outage-1", "outage-2"];
    for key in &surviving_keys {
        let response = ctx
            .client
            .get_object()
            .bucket(&bucket_name)
            .key(*key)
            .send()
            .await
            .expect("Failed to get surviving object");

        let body = response.body.collect().await.expect("Failed to read body");
        assert!(!body.into_bytes().is_empty());
        println!("    OK: Surviving object {key} verified");
    }

    // Step 9: Verify deleted objects are gone
    println!("  Step 9: Verifying deleted objects are gone");
    for key in &to_delete {
        let result = ctx
            .client
            .get_object()
            .bucket(&bucket_name)
            .key(*key)
            .send()
            .await;

        assert!(result.is_err(), "Deleted object {key} should not exist");
        println!("    OK: Deleted object {key} confirmed absent");
    }

    println!("SUCCESS: Resync server with deleted blobs test completed!");
    Ok(())
}

async fn test_dry_run_mode() -> CmdResult {
    let ctx = context();
    let bucket_name = ctx.create_bucket("test-resync-dry-run").await;

    println!(" Testing resync server dry run mode...");

    // Step 1: Create outage scenario with data
    println!("  Step 1: Creating outage scenario");
    stop_service(ServiceName::MinioAz2)?;
    sleep(Duration::from_secs(2)).await;

    // Upload objects during outage
    let outage_objects = vec![
        ("dry-run-test-1", b"Dry run test data 1"),
        ("dry-run-test-2", b"Dry run test data 2"),
        ("dry-run-test-3", b"Dry run test data 3"),
    ];

    for (key, data) in &outage_objects {
        ctx.client
            .put_object()
            .bucket(&bucket_name)
            .key(*key)
            .body(ByteStream::from_static(*data))
            .send()
            .await
            .expect("Failed to upload object during outage");
    }

    // Step 2: Restart remote AZ
    println!("  Step 2: Restarting remote AZ");
    start_services(
        ServiceName::MinioAz2,
        BuildMode::Debug,
        false,
        DataBlobStorage::default(),
    )?;
    wait_for_service_ready(ServiceName::MinioAz2, 30)?;

    // Step 3: Run dry-run resync (should not actually copy blobs)
    println!("  Step 3: Running dry-run resync operation");
    run_resync_server_command("resync", true, None)?;

    // Step 4: Check status - should still show pending blobs since dry-run didn't actually copy
    println!("  Step 4: Checking status after dry-run");
    run_resync_server_command("status", false, None)?;

    // Step 5: Run actual resync operation
    println!("  Step 5: Running actual resync operation");
    run_resync_server_command("resync", false, None)?;

    // Step 6: Verify all objects are accessible
    println!("  Step 6: Verifying objects after actual resync");
    for (key, expected_data) in &outage_objects {
        let response = ctx
            .client
            .get_object()
            .bucket(&bucket_name)
            .key(*key)
            .send()
            .await
            .expect("Failed to get object after resync");

        let body = response.body.collect().await.expect("Failed to read body");
        assert_eq!(body.into_bytes().as_ref(), *expected_data);
        println!("    OK: Object {key} verified after actual resync");
    }

    println!("SUCCESS: Dry run mode test completed!");
    Ok(())
}

async fn test_resume_functionality() -> CmdResult {
    let ctx = context();
    let bucket_name = ctx.create_bucket("test-resync-resume").await;

    println!(" Testing resync server resume functionality...");

    // Step 1: Create large outage scenario
    println!("  Step 1: Creating large dataset during outage");
    stop_service(ServiceName::MinioAz2)?;
    sleep(Duration::from_secs(2)).await;

    // Upload many objects with predictable ordering
    for i in 1..=20 {
        let key = format!("resume-test-{i:03}");
        let data = format!("Resume test data item {i} with content");

        ctx.client
            .put_object()
            .bucket(&bucket_name)
            .key(&key)
            .body(ByteStream::from(data.into_bytes()))
            .send()
            .await
            .expect("Failed to upload object during outage");

        if i % 5 == 0 {
            println!("    Uploaded {i} objects...");
        }
    }

    // Step 2: Restart remote AZ
    println!("  Step 2: Restarting remote AZ");
    start_services(
        ServiceName::MinioAz2,
        BuildMode::Debug,
        false,
        DataBlobStorage::default(),
    )?;
    wait_for_service_ready(ServiceName::MinioAz2, 30)?;

    // Step 3: Run resync starting from a specific key (simulating resume)
    println!("  Step 3: Running resync with start_after parameter");
    let resume_key = "resume-test-010";
    run_resync_server_command("resync", false, Some(resume_key))?;

    // Step 4: Run full resync to handle any remaining blobs
    println!("  Step 4: Running full resync to catch any remaining");
    run_resync_server_command("resync", false, None)?;

    // Step 5: Verify all objects are accessible
    println!("  Step 5: Verifying all objects after resume resync");
    for i in 1..=20 {
        let key = format!("resume-test-{i:03}");
        let expected_data = format!("Resume test data item {i} with content");

        let response = ctx
            .client
            .get_object()
            .bucket(&bucket_name)
            .key(&key)
            .send()
            .await
            .expect("Failed to get object after resume resync");

        let body = response.body.collect().await.expect("Failed to read body");
        assert_eq!(body.into_bytes().as_ref(), expected_data.as_bytes());

        if i % 5 == 0 {
            println!("    OK: Verified {i} objects...");
        }
    }

    println!("SUCCESS: Resume functionality test completed!");
    Ok(())
}

// Helper function to run data_blob_resync_server commands
fn run_resync_server_command(command: &str, dry_run: bool, start_after: Option<&str>) -> CmdResult {
    let dry_run_opt = if dry_run { "--dry-run" } else { "" };
    let start_after_opt = if let Some(start_after) = start_after {
        ["--start-after", start_after]
    } else {
        ["", ""]
    };
    run_cmd! {
        info "Running: ./target/debug/data_blob_resync_server ${command}";
        ./target/debug/data_blob_resync_server $command $dry_run_opt $[start_after_opt]
    }
}
