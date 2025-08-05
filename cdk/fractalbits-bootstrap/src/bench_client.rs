use super::common::*;
use cmd_lib::*;

pub fn bootstrap(service_id: &str) -> CmdResult {
    download_binaries(&["warp"])?;
    create_systemd_unit_file("bench_client", true)?;
    create_ddb_register_and_deregister_service(service_id)?;
    Ok(())
}
