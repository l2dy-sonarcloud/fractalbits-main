use super::common::*;
use cmd_lib::*;

pub fn bootstrap(service_endpoint: &str, clients_ips: Vec<String>) -> CmdResult {
    download_binaries(&["warp"])?;
    create_workload_config(service_endpoint, clients_ips)?;
    create_bench_start_script()?;
    Ok(())
}

fn create_workload_config(service_endpoint: &str, clients_ips: Vec<String>) -> CmdResult {
    let mut warp_clients_str = String::new();
    for ip in clients_ips {
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
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$BENCH_SERVER_WORKLOAD_CONFIG;
        echo $config_content > $ETC_PATH/$BENCH_SERVER_WORKLOAD_CONFIG.orig;
    }?;
    Ok(())
}

fn create_bench_start_script() -> CmdResult {
    let script_content = include_str!("bench_start.sh");
    run_cmd! {
        mkdir -p $BIN_PATH;
        echo $script_content > $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
        chmod +x $BIN_PATH/$BENCH_SERVER_BENCH_START_SCRIPT;
    }?;
    Ok(())
}
