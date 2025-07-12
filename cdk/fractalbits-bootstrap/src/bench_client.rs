use super::common::*;
use cmd_lib::*;

pub fn bootstrap(api_server_pair_ip: Option<String>) -> CmdResult {
    download_binaries(&["warp"])?;
    create_systemd_unit_file("bench_client", true)?;

    if let Some(api_server_ip) = api_server_pair_ip {
        run_cmd!(echo "$api_server_ip   local-service-endpoint" >>/etc/hosts)?;
    }
    Ok(())
}
