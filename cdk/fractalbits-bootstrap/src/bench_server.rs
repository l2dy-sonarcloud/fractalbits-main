use super::common::*;
use cmd_lib::*;

pub fn bootstrap(mut service_endpoint: String, client_ips: Vec<String>) -> CmdResult {
    download_binaries(&["warp"])?;
    create_workload_config(&service_endpoint, &client_ips)?;
    if service_endpoint == "localhost" {
        // We are running bench tool within api_server, so try to contact the first
        // api_server to create benchmark bucket
        service_endpoint = client_ips[0].clone();
    }
    create_bench_start_script(&service_endpoint)?;
    Ok(())
}

fn create_workload_config(service_endpoint: &str, client_ips: &Vec<String>) -> CmdResult {
    let mut warp_clients_str = String::new();
    for ip in client_ips {
        warp_clients_str.push_str(&format!("  - {}:7761\n", ip));
    }

    let region = get_current_aws_region()?;
    let config_content = format!(
        r##"warp:
  api: v1
  benchmark: mixed
  warp-client:
{warp_clients_str}
  remote:
    region: {region}
    access-key: test_api_key
    secret-key: test_api_secret
    host: {service_endpoint}
    tls: false
    bucket: warp-benchmark-bucket
  params:
    duration: 1m
    concurrent: 50
    distribution:
      get: 45.0
      stat: 30.0
      put: 15.0
      delete: 10.0 # Must be same or lower than 'put'.
    obj:
      size: 4KiB
      rand-size: false
    autoterm:
      enabled: false
      dur: 10s
      pct: 7.5
    no-clear: true
    keep-data: true
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$BENCH_SERVER_WORKLOAD_CONFIG;
        echo $config_content > $ETC_PATH/$BENCH_SERVER_WORKLOAD_CONFIG.orig;
    }?;
    Ok(())
}

fn create_bench_start_script(service_endpoint: &str) -> CmdResult {
    let script_content = format!(
        r##"#!/bin/bash

set -ex

CONF=/opt/fractalbits/etc/bench_workload.yaml
WARP=/opt/fractalbits/bin/warp
region=$(cat $CONF | grep region: | awk '{{print $2}}')
bucket=$(cat $CONF | grep bucket: | awk '{{print $2}}')
export AWS_DEFAULT_REGION=$region
export AWS_ENDPOINT_URL_S3=http://{service_endpoint}
export AWS_ACCESS_KEY_ID=test_api_key
export AWS_SECRET_ACCESS_KEY=test_api_secret


if ! aws s3api head-bucket --bucket $bucket &>/dev/null; then
  aws s3api create-bucket --bucket $bucket
fi
# clean up before benchmarking
aws s3 rm s3://$bucket --recursive --quiet
$WARP run $CONF
# clean up after benchmarking
aws s3 rm s3://$bucket --recursive --quiet
"##
    );
    run_cmd! {
        mkdir -p $BIN_PATH;
        echo $script_content > $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
        chmod +x $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
    }?;
    Ok(())
}
