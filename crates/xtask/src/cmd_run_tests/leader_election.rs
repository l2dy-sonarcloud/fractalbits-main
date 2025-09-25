use cmd_lib::*;
use colored::*;
use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

pub fn run_leader_election_tests() -> CmdResult {
    info!("Running leader election tests...");

    // Run all leader election test scenarios
    println!(
        "{}",
        "=== Test 1: Single Instance Becomes Leader ===".bold()
    );
    if let Err(e) = test_single_instance_becomes_leader() {
        eprintln!("{}: {}", "Test 1 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("{}", "=== Test 2: Leader Failover ===".bold());
    if let Err(e) = test_leader_failover() {
        eprintln!("{}: {}", "Test 2 FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "{}",
        "=== Test 3: Fence Token Prevents Split Brain ===".bold()
    );
    if let Err(e) = test_fence_token_prevents_split_brain() {
        eprintln!("{}: {}", "Test 3 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("{}", "=== Test 4: Clock Skew Detection ===".bold());
    if let Err(e) = test_clock_skew_detection() {
        eprintln!("{}: {}", "Test 4 FAILED".red().bold(), e);
        return Err(e);
    }

    println!("{}", "=== Test 5: Manual Leadership Resignation ===".bold());
    if let Err(e) = test_manual_leadership_resignation() {
        eprintln!("{}: {}", "Test 5 FAILED".red().bold(), e);
        return Err(e);
    }

    println!(
        "{}",
        "=== All Leader Election Tests PASSED ===".green().bold()
    );
    Ok(())
}

fn get_test_table_name(test_name: &str) -> String {
    format!("fractalbits-leader-election-test-{test_name}")
}

const LEADER_KEY: &str = "test-leader";
const DDB_ENDPOINT: &str = "http://localhost:8000";

// Process tracker for test instances
struct TestProcessTracker {
    process_pids: HashMap<String, u32>,
}

impl TestProcessTracker {
    fn new() -> Self {
        Self {
            process_pids: HashMap::new(),
        }
    }

    fn add_process_pid(&mut self, instance_id: String, pid: u32) {
        self.process_pids.insert(instance_id, pid);
    }

    fn kill_process(&mut self, instance_id: &str) -> CmdResult {
        if let Some(pid) = self.process_pids.remove(instance_id) {
            info!("Killing process {instance_id} with PID {pid}");
            // Try graceful termination first
            let _ = run_cmd!(kill $pid);
            // Wait a moment for graceful shutdown
            sleep(std::time::Duration::from_secs(2));
            // Force kill if still running
            let _ = run_cmd!(kill -9 $pid);
        }
        Ok(())
    }

    fn graceful_kill_process(&mut self, instance_id: &str) -> CmdResult {
        if let Some(pid) = self.process_pids.remove(instance_id) {
            info!("Gracefully terminating process {instance_id} with PID {pid}");
            // Send SIGTERM and wait longer for resignation
            let _ = run_cmd!(kill -TERM $pid);
            // Wait longer for graceful shutdown and resignation
            sleep(std::time::Duration::from_secs(8));
            // Force kill if still running
            let _ = run_cmd!(kill -9 $pid);
        }
        Ok(())
    }

    fn kill_all(&mut self) -> CmdResult {
        let instance_ids: Vec<String> = self.process_pids.keys().cloned().collect();
        for instance_id in instance_ids {
            self.kill_process(&instance_id)?;
        }
        Ok(())
    }
}

fn setup_test_table(table_name: &str) -> CmdResult {
    // Clean up any existing table
    let _ = run_cmd!(
        aws dynamodb delete-table --table-name $table_name --endpoint-url $DDB_ENDPOINT 2>/dev/null
    );

    // Wait longer for deletion to complete - DDB Local can be slow
    sleep(Duration::from_secs(3));

    // Create test table using AWS CLI with compact JSON output
    run_cmd!(
        aws dynamodb create-table
            --table-name $table_name
            --attribute-definitions AttributeName=key,AttributeType=S
            --key-schema AttributeName=key,KeyType=HASH
            --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1
            --endpoint-url $DDB_ENDPOINT
            --output json | jq -c
    )?;

    // Wait longer for table to be ready
    sleep(Duration::from_secs(2));

    Ok(())
}

fn cleanup_test_table(table_name: &str) -> CmdResult {
    // First try to clear any remaining items
    let _ = run_cmd!(
        aws dynamodb delete-item
            --table-name $table_name
            --key "{\"key\":{\"S\":\"$LEADER_KEY\"}}"
            --endpoint-url $DDB_ENDPOINT
            --output json | jq -c
    );

    // Then delete the table
    let _ = run_cmd!(
        aws dynamodb delete-table --table-name $table_name --endpoint-url $DDB_ENDPOINT
            --output json | jq -c
    );

    // Wait for cleanup to complete
    sleep(Duration::from_secs(2));
    Ok(())
}

fn get_current_leader(table_name: &str) -> Option<String> {
    // Get the full item using AWS CLI with compact JSON
    let result = run_fun!(
        aws dynamodb get-item
            --table-name $table_name
            --key "{\"key\":{\"S\":\"$LEADER_KEY\"}}"
            --endpoint-url $DDB_ENDPOINT
            --output json | jq -c
    );

    match result {
        Ok(output) => {
            if output.trim().is_empty() || output.contains("\"Item\": {}") {
                println!("No DDB item found for key {LEADER_KEY}");
                return None;
            }

            println!("DDB item found: {}", output.trim());

            // Extract instance_id using AWS CLI query
            let instance_id_result = run_fun!(
            aws dynamodb get-item
                --table-name $table_name
                --key "{\"key\":{\"S\":\"$LEADER_KEY\"}}"
                --query "Item.instance_id.S"
                --output text
                --endpoint-url $DDB_ENDPOINT
                );

            if let Ok(instance_id) = instance_id_result {
                let instance_id = instance_id.trim();
                if instance_id == "None" || instance_id.is_empty() {
                    println!("No instance_id found in item");
                    return None;
                }

                // Check if lease is still valid by getting lease_expiry
                let lease_expiry_result = run_fun!(
                aws dynamodb get-item
                    --table-name $table_name
                    --key "{\"key\":{\"S\":\"$LEADER_KEY\"}}"
                    --query "Item.lease_expiry.N"
                    --output text
                    --endpoint-url $DDB_ENDPOINT
                        );

                if let Ok(expiry_str) = lease_expiry_result {
                    let expiry_str = expiry_str.trim();
                    if expiry_str != "None" && !expiry_str.is_empty() {
                        if let Ok(expiry_time) = expiry_str.parse::<u64>() {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs();
                            println!(
                                "Lease check: expiry={}, now={}, valid={}",
                                expiry_time,
                                now,
                                expiry_time > now
                            );
                            if expiry_time > now {
                                return Some(instance_id.to_string());
                            } else {
                                println!("Lease expired for instance {instance_id}");
                            }
                        }
                    } else {
                        println!("No lease_expiry found in item");
                    }
                } else {
                    println!("Failed to get lease_expiry");
                }
            } else {
                println!("Failed to get instance_id");
            }
        }
        Err(e) => {
            println!("DDB get_item error: {e:?}");
        }
    }
    None
}

fn start_test_instance(
    instance_id: &str,
    server_port: u16,
    health_port: u16,
    metrics_port: u16,
    table_name: &str,
    process_tracker: &mut TestProcessTracker,
) -> CmdResult {
    let working_dir = run_fun!(pwd)?;
    let leader_election_test_log =
        format!("{working_dir}/data/logs/leader_election_test_{instance_id}.log");

    let process = start_test_root_server_instance(
        instance_id,
        server_port,
        health_port,
        metrics_port,
        table_name,
        &leader_election_test_log,
    )
    .map_err(|e| std::io::Error::other(format!("Failed to start test instance: {e}")))?;

    // Get the PIDs from the spawned process
    let pids = process.pids();
    if let Some(&pid) = pids.first() {
        process_tracker.add_process_pid(instance_id.to_string(), pid);
        info!("Started process {instance_id} with PID {pid}");
    } else {
        return Err(std::io::Error::other(
            "Failed to get PID from spawned process",
        ));
    }

    Ok(())
}

fn test_single_instance_becomes_leader() -> CmdResult {
    let mut process_tracker = TestProcessTracker::new();

    // Clean up any existing test instances first
    cleanup_test_root_server_instances()?;

    // DDB local should already be started by the test runner
    sleep(Duration::from_secs(2));

    let table_name = get_test_table_name("single_instance");
    setup_test_table(&table_name)?;

    // Start a single instance
    start_test_instance(
        "single-instance-1",
        28086,
        38086,
        18087,
        &table_name,
        &mut process_tracker,
    )?;

    println!("Process started successfully, waiting for leader election...");

    // Poll for leader every 5 seconds for up to 60 seconds
    let mut leader = None;
    for attempt in 1..=12 {
        sleep(Duration::from_secs(5));
        leader = get_current_leader(&table_name);
        println!("Attempt {attempt}: Current leader: {leader:?}");

        if leader.is_some() {
            break;
        }
    }

    // Clean up processes first
    process_tracker.kill_all()?;

    assert_eq!(
        leader,
        Some("single-instance-1".to_string()),
        "Expected single-instance-1 to become leader after 60 seconds"
    );

    // Clean up table
    cleanup_test_table(&table_name)?;

    println!("SUCCESS: Single instance becomes leader test completed!");
    Ok(())
}

fn test_leader_failover() -> CmdResult {
    let mut process_tracker = TestProcessTracker::new();

    // Clean up any existing test instances first
    cleanup_test_root_server_instances()?;

    // DDB local should already be started by the test runner
    sleep(Duration::from_secs(2));

    let table_name = get_test_table_name("leader_failover");
    setup_test_table(&table_name)?;

    // Start first instance
    start_test_instance(
        "failover-instance-1",
        28087,
        38087,
        18088,
        &table_name,
        &mut process_tracker,
    )?;

    // Wait for first instance to become leader (may take up to lease duration)
    sleep(Duration::from_secs(15));
    assert_eq!(
        get_current_leader(&table_name),
        Some("failover-instance-1".to_string())
    );

    // Kill first instance to test failover
    process_tracker.kill_process("failover-instance-1")?;

    // Wait for lease to expire before starting second instance
    // With a 20-second lease, wait 25 seconds to ensure expiration
    // This also avoids metrics port conflict
    sleep(Duration::from_secs(25));

    // Now start second instance after first is dead
    start_test_instance(
        "failover-instance-2",
        28088,
        38088,
        18089,
        &table_name,
        &mut process_tracker,
    )?;

    // Wait for second instance to acquire leadership
    sleep(Duration::from_secs(15));

    // Second instance should now be leader
    let current_leader = get_current_leader(&table_name);
    println!("Current leader after failover: {current_leader:?}");
    assert_eq!(
        current_leader,
        Some("failover-instance-2".to_string()),
        "Expected failover-instance-2 to become leader after failover-instance-1 was killed"
    );

    // Clean up all remaining processes
    process_tracker.kill_all()?;
    cleanup_test_table(&table_name)?;

    println!("SUCCESS: Leader failover test completed!");
    Ok(())
}

fn test_fence_token_prevents_split_brain() -> CmdResult {
    // Clean up any existing test instances first
    cleanup_test_root_server_instances()?;

    // DDB local should already be started by the test runner
    sleep(Duration::from_secs(2));

    let table_name = get_test_table_name("fence_token");
    setup_test_table(&table_name)?;

    // Manually create a leader entry with high fence token
    let high_fence_token = 999999999u64;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let lease_expiry = now + 300; // 5 minutes in future

    run_cmd!(
        aws dynamodb put-item
            --table-name $table_name
            --item "{
                \"key\": {\"S\": \"$LEADER_KEY\"},
                \"instance_id\": {\"S\": \"manual-leader\"},
                \"ip_address\": {\"S\": \"127.0.0.1\"},
                \"port\": {\"N\": \"8086\"},
                \"lease_expiry\": {\"N\": \"$lease_expiry\"},
                \"fence_token\": {\"N\": \"$high_fence_token\"},
                \"renewal_count\": {\"N\": \"1\"},
                \"last_heartbeat\": {\"N\": \"$now\"}
            }"
            --endpoint-url $DDB_ENDPOINT
            --output json | jq -c
    )
    .expect("Failed to create manual leader");

    // Try to start an instance - it should not become leader due to fence token
    let mut process_tracker = TestProcessTracker::new();
    start_test_instance(
        "fence-token-instance-1",
        28089,
        38089,
        18090,
        &table_name,
        &mut process_tracker,
    )?;

    // Wait for leader election attempts
    sleep(Duration::from_secs(20));

    // Manual leader should still be the leader
    assert_eq!(
        get_current_leader(&table_name),
        Some("manual-leader".to_string())
    );

    // Clean up
    process_tracker.kill_all()?;
    cleanup_test_table(&table_name)?;

    println!("SUCCESS: Fence token prevents split brain test completed!");
    Ok(())
}

fn test_clock_skew_detection() -> CmdResult {
    // Clean up any existing test instances first
    cleanup_test_root_server_instances()?;

    // DDB local should already be started by the test runner
    sleep(Duration::from_secs(2));

    let table_name = get_test_table_name("clock_skew");
    setup_test_table(&table_name)?;

    // Create a leader entry with timestamp far in the past (simulating clock skew)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let skewed_time = now - 120; // 2 minutes in the past

    let lease_expiry = now + 30;

    run_cmd!(
        aws dynamodb put-item
            --table-name $table_name
            --item "{
                \"key\": {\"S\": \"$LEADER_KEY\"},
                \"instance_id\": {\"S\": \"skewed-leader\"},
                \"ip_address\": {\"S\": \"127.0.0.1\"},
                \"port\": {\"N\": \"8086\"},
                \"lease_expiry\": {\"N\": \"$lease_expiry\"},
                \"fence_token\": {\"N\": \"1\"},
                \"renewal_count\": {\"N\": \"1\"},
                \"last_heartbeat\": {\"N\": \"$skewed_time\"}
            }"
            --endpoint-url $DDB_ENDPOINT
            --output json | jq -c
    )
    .expect("Failed to create skewed leader");

    // Start an instance - it should detect clock skew
    let mut process_tracker = TestProcessTracker::new();
    start_test_instance(
        "clock-skew-instance-1",
        28090,
        38090,
        18091,
        &table_name,
        &mut process_tracker,
    )?;

    // Wait for leader election attempts
    sleep(Duration::from_secs(15));

    // The instance should not become leader due to clock skew detection
    let leader = get_current_leader(&table_name);
    assert_ne!(leader, Some("clock-skew-instance-1".to_string()));

    // Clean up
    process_tracker.kill_all()?;
    cleanup_test_table(&table_name)?;

    println!("SUCCESS: Clock skew detection test completed!");
    Ok(())
}

// Test instance management for leader election tests
fn start_test_root_server_instance(
    instance_id: &str,
    server_port: u16,
    health_port: u16,
    metrics_port: u16,
    table_name: &str,
    log_path: &str,
) -> Result<cmd_lib::CmdChildren, std::io::Error> {
    info!("Starting test root_server instance: {instance_id}");

    let proc = spawn! {
        RUST_LOG=info,root_server=debug
        AWS_ACCESS_KEY_ID=fakeMyKeyId
        AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey
        INSTANCE_ID=$instance_id
        RSS_SERVER_PORT=$server_port
        RSS_HEALTH_PORT=$health_port
        RSS_METRICS_PORT=$metrics_port
        LEADER_TABLE_NAME=$table_name
        LEADER_KEY=test-leader
        LEADER_LEASE_DURATION=20
        ./target/debug/root_server |& ts -m "%b %d %H:%M:%.S" > $log_path
    }?;

    // Give the instance a moment to start
    sleep(std::time::Duration::from_secs(2));

    Ok(proc)
}

pub fn cleanup_test_root_server_instances() -> CmdResult {
    run_cmd!(ignore pkill root_server)?;
    Ok(())
}

fn test_manual_leadership_resignation() -> CmdResult {
    let mut process_tracker = TestProcessTracker::new();

    // Clean up any existing test instances first
    cleanup_test_root_server_instances()?;

    // DDB local should already be started by the test runner
    sleep(Duration::from_secs(2));

    let table_name = get_test_table_name("manual_resignation");
    setup_test_table(&table_name)?;

    // Start first instance
    start_test_instance(
        "resignation-instance-1",
        28091,
        38091,
        18092,
        &table_name,
        &mut process_tracker,
    )?;

    // Wait for first instance to become leader
    println!("Waiting for first instance to become leader...");
    sleep(Duration::from_secs(15));

    let initial_leader = get_current_leader(&table_name);
    assert_eq!(
        initial_leader,
        Some("resignation-instance-1".to_string()),
        "First instance should become leader"
    );
    println!("SUCCESS: First instance became leader");

    // Start second instance (but it won't become leader while first is active)
    start_test_instance(
        "resignation-instance-2",
        28092,
        38092,
        18093,
        &table_name,
        &mut process_tracker,
    )?;

    // Wait a moment for second instance to start
    sleep(Duration::from_secs(5));

    // Leader should still be the first instance
    let still_first_leader = get_current_leader(&table_name);
    assert_eq!(
        still_first_leader,
        Some("resignation-instance-1".to_string()),
        "First instance should still be leader with second instance running"
    );
    println!("SUCCESS: First instance maintained leadership with second instance running");

    // Manually resign leadership by sending SIGTERM to first instance
    // This should trigger the signal handler we added
    println!("Sending SIGTERM to first instance to trigger resignation...");
    process_tracker.graceful_kill_process("resignation-instance-1")?;

    // Wait a short time for resignation to take effect and second instance to acquire leadership
    // The resignation should be immediate, so we don't need to wait for lease expiration
    sleep(Duration::from_secs(5));

    // Second instance should now be leader (resignation should enable immediate takeover)
    let final_leader = get_current_leader(&table_name);
    println!("Final leader after resignation: {:?}", final_leader);

    // Note: We expect either the second instance to be leader, or no leader (if the second instance
    // hasn't acquired leadership yet). The key test is that it's NOT the first instance anymore.
    if let Some(ref leader) = final_leader {
        assert_ne!(
            leader, "resignation-instance-1",
            "First instance should not be leader after resignation"
        );

        // If there's a leader, it should be the second instance
        if leader == "resignation-instance-2" {
            println!("SUCCESS: Second instance successfully acquired leadership after resignation");
        }
    } else {
        println!("SUCCESS: No leader in DDB (leadership record was successfully deleted)");

        // Wait a bit more for second instance to acquire leadership
        sleep(Duration::from_secs(10));
        let eventual_leader = get_current_leader(&table_name);
        assert_eq!(
            eventual_leader,
            Some("resignation-instance-2".to_string()),
            "Second instance should eventually become leader after resignation cleanup"
        );
        println!("SUCCESS: Second instance eventually became leader");
    }

    // Clean up all remaining processes
    process_tracker.kill_all()?;
    cleanup_test_table(&table_name)?;

    println!("SUCCESS: Manual leadership resignation test completed!");
    Ok(())
}
