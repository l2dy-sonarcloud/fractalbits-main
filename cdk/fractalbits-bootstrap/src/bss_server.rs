use super::common::*;
use cmd_lib::*;

pub fn bootstrap() -> CmdResult {
    download_binaries(&["bss_server"])?;
    create_systemd_unit_file("bss_server", true)?;
    Ok(())
}
