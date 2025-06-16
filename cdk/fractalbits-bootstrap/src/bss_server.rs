use super::common::*;
use cmd_lib::*;

pub fn bootstrap(num_nvme_disks: usize, bench: bool) -> CmdResult {
    assert_ne!(num_nvme_disks, 0);
    install_rpms(&["nvme-cli", "mdadm", "perf", "lldb"])?;
    format_local_nvme_disks(num_nvme_disks)?;

    if bench {
        download_binaries(&["bss_server", "xtask", "rewrk_rpc"])?;
    } else {
        download_binaries(&["bss_server"])?;
    }
    create_coredump_config()?;
    create_systemd_unit_file("bss_server", true)?;
    Ok(())
}
