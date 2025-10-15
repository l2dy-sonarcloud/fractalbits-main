use crate::*;

pub fn bootstrap(
    bucket: Option<&str>,
    nss_endpoint: &str,
    rss_endpoint: &str,
    remote_az: Option<&str>,
    for_bench: bool,
) -> CmdResult {
    download_binaries(&["api_server"])?;

    let is_multi_az = remote_az.is_some();
    for (role, endpoint) in [("rss", rss_endpoint), ("nss", nss_endpoint)] {
        info!("Waiting for {role} node {endpoint} to be ready");
        while run_cmd!(nc -z $endpoint 8088 &>/dev/null).is_err() {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        info!("{role} node can be reached (`nc -z {endpoint} 8088` is ok)");
    }

    // For S3 Express multi-az setup, only wait for RSS and NSS
    if is_multi_az {
        for (role, ip) in [("rss", rss_endpoint), ("nss", nss_endpoint)] {
            info!("Waiting for {role} node {ip} to be ready");
            while run_cmd!(nc -z $ip 8088 &>/dev/null).is_err() {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            info!("{role} node can be reached (`nc -z {ip} 8088` is ok)");
        }
    }

    create_config(bucket, nss_endpoint, rss_endpoint, remote_az)?;

    if for_bench {
        // Try to download tools for micro-benchmarking
        download_binaries(&["rewrk_rpc", "test_art"])?;
    }

    create_ena_irq_affinity_service()?;

    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("api_server", true)?;
    create_ddb_register_and_deregister_service("api-server")?;

    Ok(())
}

pub fn create_config(
    bucket: Option<&str>,
    nss_endpoint: &str,
    rss_endpoint: &str,
    remote_az: Option<&str>,
) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let num_cores = run_fun!(nproc)?;
    let config_content = if let Some(remote_az) = remote_az {
        // S3 Express Multi-AZ configuration
        let local_az = get_current_aws_az_id()?;
        let local_bucket = get_s3_express_bucket_name(&local_az)?;
        let remote_bucket = get_s3_express_bucket_name(remote_az)?;

        format!(
            r##"nss_addr = "{nss_endpoint}:8088"
rss_addr = "{rss_endpoint}:8088"
nss_conn_num = {num_cores}
rss_conn_num = 1
bss_conn_num = {num_cores}
region = "{aws_region}"
port = 80
mgmt_port = 18088
root_domain = ".localhost"
with_metrics = true
http_request_timeout_seconds = 5
rpc_timeout_seconds = 4
allow_missing_or_bad_signature = false

[https]
enabled = false
port = 443
cert_file = "/opt/fractalbits/etc/cert.pem"
key_file = "/opt/fractalbits/etc/key.pem"
force_http1_only = false

[blob_storage]
backend = "s3_express_multi_az"

[blob_storage.s3_express_multi_az]
local_az_host = "http://s3.{aws_region}.amazonaws.com"
local_az_port = 80
remote_az_host = "http://s3.{aws_region}.amazonaws.com"
remote_az_port = 80
s3_region = "{aws_region}"
local_az_bucket = "{local_bucket}"
remote_az_bucket = "{remote_bucket}"
local_az = "{local_az}"
remote_az = "{remote_az}"

[blob_storage.s3_express_multi_az.ratelimit]
enabled = false
put_qps = 7000
get_qps = 10000
delete_qps = 5000

[blob_storage.s3_express_multi_az.retry_config]
enabled = false
max_attempts = 15
initial_backoff_us = 50
max_backoff_us = 500
backoff_multiplier = 1.0
"##
        )
    } else {
        // Hybrid single az configuration
        let bucket_name =
            bucket.ok_or_else(|| std::io::Error::other("Bucket name required for hybrid mode"))?;
        format!(
            r##"nss_addr = "{nss_endpoint}:8088"
rss_addr = "{rss_endpoint}:8088"
nss_conn_num = {num_cores}
rss_conn_num = 1
bss_conn_num = {num_cores}
region = "{aws_region}"
port = 80
mgmt_port = 18088
root_domain = ".localhost"
with_metrics = true
http_request_timeout_seconds = 5
rpc_timeout_seconds = 4
allow_missing_or_bad_signature = false

[https]
enabled = false
port = 443
cert_file = "/opt/fractalbits/etc/cert.pem"
key_file = "/opt/fractalbits/etc/key.pem"
force_http1_only = false

[blob_storage]
backend = "s3_hybrid_single_az"

[blob_storage.s3_hybrid_single_az]
s3_host = "http://s3.{aws_region}.amazonaws.com"
s3_port = 80
s3_region = "{aws_region}"
s3_bucket = "{bucket_name}"

[blob_storage.s3_hybrid_single_az.ratelimit]
enabled = false
put_qps = 7000
get_qps = 10000
delete_qps = 5000

[blob_storage.s3_hybrid_single_az.retry_config]
enabled = true
max_attempts = 8
initial_backoff_us = 15000
max_backoff_us = 2000000
backoff_multiplier = 1.8
"##
        )
    };

    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$API_SERVER_CONFIG
    }?;
    Ok(())
}

fn create_ena_irq_affinity_service() -> CmdResult {
    let script_path = format!("{BIN_PATH}configure-ena-irq-affinity.sh");
    let systemd_unit_content = format!(
        r##"[Unit]
Description=ENA IRQ Affinity Configuration
After=network-online.target
Before=api_server.service

[Service]
Type=oneshot
ExecStart={script_path}

[Install]
WantedBy=multi-user.target
"##
    );

    let script_content = r##"#!/bin/bash
set -e

echo "Configuring ENA interrupt affinity" >&2

echo "Disabling irqbalance" >&2
systemctl disable --now irqbalance 2>/dev/null || true

iface=$(grep -o "ens[0-9]*-Tx-Rx" /proc/interrupts | head -1 | sed "s/-Tx-Rx//")
if [ -z "$iface" ]; then
    echo "ERROR: Could not detect ENA interface" >&2
    exit 1
fi
echo "Detected ENA interface: $iface" >&2

num_queues=$(grep "$iface-Tx-Rx-" /proc/interrupts | wc -l)
if [ "$num_queues" -eq 0 ]; then
    echo "ERROR: Could not detect ENA queues" >&2
    exit 1
fi
echo "Found $num_queues queues" >&2

num_cpus=$(nproc)
echo "System has $num_cpus CPUs" >&2

echo "Attempting to configure RSS" >&2
if ethtool -X $iface equal $num_cpus 2>&1; then
    echo "RSS configured successfully" >&2
else
    echo "RSS configuration failed or not supported (using hardware default)" >&2
fi

cpus_per_queue=$((num_cpus / num_queues))
echo "Spreading $num_queues queues across $num_cpus CPUs ($cpus_per_queue CPUs per queue)" >&2

for queue in $(seq 0 $((num_queues - 1))); do
    irq=$(grep "$iface-Tx-Rx-$queue" /proc/interrupts | awk -F: '{print $1}' | tr -d ' ')

    if [ -n "$irq" ]; then
        start_cpu=$((queue * cpus_per_queue))
        end_cpu=$(((queue + 1) * cpus_per_queue))
        if [ $end_cpu -gt $num_cpus ]; then
            end_cpu=$num_cpus
        fi

        mask_low=0
        mask_high=0
        for cpu in $(seq $start_cpu $((end_cpu - 1))); do
            if [ $cpu -lt 32 ]; then
                mask_low=$((mask_low | (1 << cpu)))
            else
                mask_high=$((mask_high | (1 << (cpu - 32))))
            fi
        done

        if [ $mask_high -gt 0 ]; then
            mask_str=$(printf "%08x,%08x" $mask_high $mask_low)
        else
            mask_str=$(printf "%x" $mask_low)
        fi

        echo $mask_str > /proc/irq/$irq/smp_affinity
        echo "IRQ $irq ($iface-Tx-Rx-$queue) -> CPUs $start_cpu-$end_cpu (mask: $mask_str)" >&2

        xps_path="/sys/class/net/$iface/queues/tx-$queue/xps_cpus"
        echo $mask_str > $xps_path 2>/dev/null || true
    fi
done

echo 32768 > /proc/sys/net/core/rps_sock_flow_entries || true
for queue in $(seq 0 $((num_queues - 1))); do
    rps_path="/sys/class/net/$iface/queues/rx-$queue/rps_flow_cnt"
    echo 4096 > $rps_path 2>/dev/null || true
done

mgmt_irq=$(grep -E "ena-mgmnt|$iface-mgmnt" /proc/interrupts | awk -F: '{print $1}' | tr -d ' ' | head -1)
if [ -n "$mgmt_irq" ]; then
    echo 1 > /proc/irq/$mgmt_irq/smp_affinity
    echo "Management IRQ $mgmt_irq -> CPU 0" >&2
fi

echo "Done! Current ENA IRQ affinity:" >&2
for queue in $(seq 0 $((num_queues - 1))); do
    irq=$(grep "$iface-Tx-Rx-$queue" /proc/interrupts | awk -F: '{print $1}' | tr -d ' ')
    if [ -n "$irq" ]; then
        affinity=$(cat /proc/irq/$irq/smp_affinity)
        start_cpu=$((queue * cpus_per_queue))
        end_cpu=$(((queue + 1) * cpus_per_queue))
        if [ $end_cpu -gt $num_cpus ]; then
            end_cpu=$num_cpus
        fi
        echo "Queue $queue (IRQ $irq): CPUs $start_cpu-$end_cpu, mask = 0x$affinity" >&2
    fi
done

echo "RFS configured: 32768 global flow entries, 4096 per queue" >&2
echo "XPS configured for TX steering" >&2
"##;

    run_cmd! {
        echo $script_content > $script_path;
        chmod +x $script_path;

        echo $systemd_unit_content > ${ETC_PATH}ena-irq-affinity.service;
        systemctl enable --now ${ETC_PATH}ena-irq-affinity.service;
    }?;
    Ok(())
}
