mod yaml_get;
mod yaml_mixed;
mod yaml_put;

use super::common::*;
use cmd_lib::*;
use {yaml_get::*, yaml_mixed::*, yaml_put::*};

pub fn bootstrap(
    api_server_service_endpoint: String,
    bench_client_service_id: String,
    bench_client_num: usize,
) -> CmdResult {
    download_binaries(&["warp"])?;

    info!("Waiting for {bench_client_num} bench client service");
    let client_ips: Vec<String> = loop {
        let res = run_fun! {
             aws servicediscovery list-instances
                 --service-id $bench_client_service_id
                 --query "Instances[].Attributes.AWS_INSTANCE_IPV4"
                 --output text
        };
        if let Ok(output) = res {
            let ips: Vec<String> = output.split_whitespace().map(String::from).collect();
            if ips.len() == bench_client_num {
                info!("Found a list of bench clients: {ips:?}");
                break ips;
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
    };

    let region = get_current_aws_region()?;
    let mut warp_client_ips = String::new();
    for ip in client_ips.iter() {
        warp_client_ips.push_str(&format!("  - {ip}:7761\n"));
    }
    let duration: &str = "1m";
    create_put_workload_config(
        &warp_client_ips,
        &region,
        &api_server_service_endpoint,
        duration,
    )?;
    create_get_workload_config(
        &warp_client_ips,
        &region,
        &api_server_service_endpoint,
        duration,
    )?;
    create_mixed_workload_config(
        &warp_client_ips,
        &region,
        &api_server_service_endpoint,
        duration,
    )?;

    create_bench_start_script(&region, &api_server_service_endpoint)?;
    Ok(())
}

fn create_bench_start_script(region: &str, api_server_service_endpoint: &str) -> CmdResult {
    let script_content = format!(
        r##"#!/bin/bash

export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret

set -ex
export AWS_DEFAULT_REGION={region}
export AWS_ENDPOINT_URL_S3=http://{api_server_service_endpoint}
bench_bucket=warp-benchmark-bucket

if ! aws s3api head-bucket --bucket $bench_bucket &>/dev/null; then
  aws s3api create-bucket --bucket $bench_bucket
fi

/opt/fractalbits/bin/warp run /opt/fractalbits/etc/bench_${{WORKLOAD:-put}}.yml
"##
    );
    run_cmd! {
        mkdir -p $BIN_PATH;
        echo $script_content > $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
        chmod +x $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
    }?;
    Ok(())
}
