use super::common::*;
use cmd_lib::*;

pub fn bootstrap(bucket_name: &str, volume_id: &str, secondary: bool) -> CmdResult {
    download_binary("mkfs")?;

    // Sanitize: convert vol-07451bc901d5e1e09 → vol07451bc901d5e1e09
    let volume_id = &volume_id.replace("-", "");
    let ebs = format!("nvme-Amazon_Elastic_Block_Store_{volume_id}");
    let ebs_dev = format! {"/dev/disk/by-id/{ebs}"};
    const MNT: &str = "/data";
    if secondary {
        run_cmd! {
            info "Creating mount point $MNT";
            mkdir -p $MNT;
        }?;
    } else {
        loop {
            if let Ok(true) = std::fs::exists(&ebs_dev) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        info!("Checking filesystem on {ebs}");
        // TODO: check and set dynamodb ebs state as being formatted
        if run_cmd!(file -s $ebs_dev | grep -q filesystem).is_err() {
            run_cmd! {
                info "Formatting $ebs with XFS";
                mkfs -t xfs $ebs_dev;
            }?;
        }

        run_cmd! {
            info "Mounting $ebs to $MNT";
            mkdir -p $MNT;
            mount $ebs_dev $MNT;
        }?;

        run_cmd! {
            info "Formatting for nss_server";
            cd $MNT;
            $BIN_PATH/mkfs;
        }?;
    }

    let service_name = "nss_server";
    download_binary(service_name)?;
    create_config(bucket_name)?;
    create_ebs_mount_unit(volume_id)?;
    create_udev_rule(volume_id)?;
    create_systemd_unit_file(service_name)?;
    if !secondary {
        run_cmd! {
            info "Starting nss_server.service";
            systemctl start nss_server.service;
        }?;
    }
    Ok(())
}

fn create_config(bucket_name: &str) -> CmdResult {
    let aws_region = get_current_aws_region()?;
    let config_content = format!(
        r##"[s3_cache]
s3_host = "s3.{aws_region}.amazonaws.com"
s3_port = 80
s3_region = "{aws_region}"
s3_bucket = "{bucket_name}"
"##
    );
    run_cmd! {
        mkdir -p $ETC_PATH;
        echo $config_content > $ETC_PATH/$NSS_SERVER_CONFIG
    }?;
    Ok(())
}

fn create_ebs_mount_unit(volume_id: &str) -> CmdResult {
    let content = format!(
        r##"[Unit]
Description=Mount EBS Volume at /data

[Mount]
What=/dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_{volume_id}
Where=/data
Type=xfs
Options=defaults,nofail

[Install]
WantedBy=multi-user.target
"##
    );
    run_cmd! {
        echo $content > /etc/systemd/system/data.mount;
    }?;

    Ok(())
}

fn create_udev_rule(volume_id: &str) -> CmdResult {
    let content = format!(
        r##"KERNEL=="nvme*n*", SUBSYSTEM=="block", ENV{{ID_SERIAL}}=="Amazon_Elastic_Block_Store_{volume_id}_1", TAG+="systemd", ENV{{SYSTEMD_WANTS}}="nss_server.service""##
    );
    run_cmd! {
        echo $content > /etc/udev/rules.d/99-ebs.rules;
    }?;

    Ok(())
}
