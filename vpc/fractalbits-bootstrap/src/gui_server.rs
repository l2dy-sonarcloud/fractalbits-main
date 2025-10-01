use crate::*;

pub fn bootstrap(
    bucket: Option<&str>,
    nss_endpoint: &str,
    rss_endpoint: &str,
    remote_az: Option<&str>,
) -> CmdResult {
    download_binaries(&["api_server"])?;
    let builds_bucket = get_builds_bucket()?;
    run_cmd!(aws s3 cp --no-progress $builds_bucket/ui $GUI_WEB_ROOT --recursive)?;

    // Check if we're using S3 Express by checking if remote_az is provided
    let is_s3_express = remote_az.is_some();

    for (role, endpoint) in [("rss", rss_endpoint), ("nss", nss_endpoint)] {
        info!("Waiting for {role} node with endpoint {endpoint} to be ready");
        while run_cmd!(nc -z $endpoint 8088 &>/dev/null).is_err() {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        info!("{role} node can be reached (`nc -z {endpoint} 8088` is ok)");
    }

    // For S3 Express, only wait for RSS and NSS
    if is_s3_express {
        for (role, ip) in [("rss", rss_endpoint), ("nss", nss_endpoint)] {
            info!("Waiting for {role} node {ip} to be ready");
            while run_cmd!(nc -z $ip 8088 &>/dev/null).is_err() {
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
            info!("{role} node can be reached (`nc -z {ip} 8088` is ok)");
        }
    }

    api_server::create_config(bucket, nss_endpoint, rss_endpoint, remote_az)?;
    // setup_cloudwatch_agent()?;
    create_systemd_unit_file("gui_server", true)?;

    Ok(())
}
