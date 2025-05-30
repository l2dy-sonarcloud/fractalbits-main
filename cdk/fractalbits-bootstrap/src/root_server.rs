use super::common::*;
use cmd_lib::*;

pub fn bootstrap() -> CmdResult {
    info!("Bootstrapping root_server ...");

    // root_server requires etcd service running
    download_binary("etcd")?;
    start_etcd_service()?;

    download_binary("rss_admin")?;
    run_cmd!($BIN_PATH/rss_admin api-key init-test)?;

    let service = super::Service::RootServer;
    download_binary(service.as_ref())?;
    create_systemd_unit_file(service)?;
    run_cmd! {
        info "Starting root_server.service";
        systemctl start root_server.service;
    }?;
    Ok(())
}

fn start_etcd_service() -> CmdResult {
    let service_file = format!("{ETC_PATH}etcd.service");
    let service_file_content = format!(
        r##"[Unit]
Description=etcd for root_server

[Install]
WantedBy=default.target

[Service]
Type=simple
ExecStart="{BIN_PATH}etcd"
Restart=always
WorkingDirectory=/var/data
"##
    );

    run_cmd! {
        mkdir -p /var/data;
        mkdir -p $ETC_PATH;
        echo $service_file_content > $service_file;
        info "Linking $service_file into /etc/systemd/system";
        systemctl link $service_file --force --quiet;
        info "Starting etcd.service";
        systemctl start etcd.service;
    }?;

    Ok(())
}
