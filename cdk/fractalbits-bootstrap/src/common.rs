use super::Service;
use cmd_lib::*;

pub const BUILDS_BUCKET: &str = "s3://fractalbits-builds";
pub const BIN_PATH: &str = "/opt/fractalbits/bin/";
pub const ETC_PATH: &str = "/opt/fractalbits/etc/";
pub const NSS_SERVER_CONFIG: &str = "nss_server_cloud_config.toml";
pub const API_SERVER_CONFIG: &str = "api_server_cloud_config.toml";

pub fn download_binary(file_name: &str) -> CmdResult {
    run_cmd! {
        info "Downloading $file_name from $BUILDS_BUCKET to $BIN_PATH ...";
        aws s3 cp --no-progress $BUILDS_BUCKET/$file_name $BIN_PATH;
        chmod +x $BIN_PATH/$file_name
    }?;
    Ok(())
}

pub fn create_systemd_unit_file(service: Service) -> CmdResult {
    let service_name = service.as_ref();
    let (requires, exec_start) = match service {
        Service::ApiServer => (
            "",
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{API_SERVER_CONFIG}"),
        ),
        Service::NssServer => (
            "",
            format!("{BIN_PATH}{service_name} -c {ETC_PATH}{NSS_SERVER_CONFIG}"),
        ),
        Service::BssServer => ("", format!("{BIN_PATH}{service_name}")),
        Service::RootServer => ("etcd.service", format!("{BIN_PATH}{service_name}")),
    };
    let systemd_unit_content = format!(
        r##"[Unit]
Description={service_name} Service
Requires={requires}
After={requires}

[Service]
LimitNOFILE=1000000
LimitCORE=infinity
WorkingDirectory=/var/data
ExecStart={exec_start}

[Install]
WantedBy=multi-user.target
"##
    );
    let service_file = format!("{service_name}.service");

    run_cmd! {
        mkdir -p /var/data;
        mkdir -p $ETC_PATH;
        echo $systemd_unit_content > ${ETC_PATH}${service_file};
        info "Linking ${ETC_PATH}${service_file} into /etc/systemd/system";
        systemctl link ${ETC_PATH}${service_file} --force --quiet;
    }?;
    Ok(())
}
