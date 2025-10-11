use cmd_lib::*;
use rayon::prelude::*;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

// Support GenUuids only for now
pub fn gen_uuids(num: usize, file: &str) -> CmdResult {
    info!("Generating {num} uuids into file {file}");
    let num_threads = num_cpus::get();
    let num_uuids = num / num_threads;
    let last_num_uuids = num - num_uuids * (num_threads - 1);

    let uuids = Arc::new(Mutex::new(Vec::new()));
    (0..num_threads).into_par_iter().for_each(|i| {
        let mut uuids_str = String::new();
        let n = if i == num_threads - 1 {
            last_num_uuids
        } else {
            num_uuids
        };
        for _ in 0..n {
            uuids_str += &Uuid::now_v7().to_string();
            uuids_str += "\n";
        }
        uuids.lock().unwrap().push(uuids_str);
    });

    let dir = run_fun!(dirname $file)?;
    run_cmd! {
        mkdir -p $dir;
        echo -n > $file;
    }?;
    for uuid in uuids.lock().unwrap().iter() {
        run_cmd!(echo -n $uuid >> $file)?;
    }
    info!("File {file} is ready");
    Ok(())
}

pub fn dump_vg_config(localdev: bool) -> CmdResult {
    // AWS cli environment variables based on localdev flag
    let env_vars: &[&str] = if localdev {
        &[
            "AWS_DEFAULT_REGION=fakeRegion",
            "AWS_ACCESS_KEY_ID=fakeMyKeyId",
            "AWS_SECRET_ACCESS_KEY=fakeSecretAccessKey",
            "AWS_ENDPOINT_URL_DYNAMODB=http://localhost:8000",
        ]
    } else {
        &[]
    };

    // Query BSS data volume group configuration
    let data_vg_result = run_fun! {
        $[env_vars]
        aws dynamodb get-item
            --table-name "fractalbits-service-discovery"
            --key "{\"service_id\": {\"S\": \"bss-data-vg-config\"}}"
            --query "Item.value.S"
            --output text
    };

    // Query BSS metadata volume group configuration
    let metadata_vg_result = run_fun! {
        $[env_vars]
        aws dynamodb get-item
            --table-name "fractalbits-service-discovery"
            --key "{\"service_id\": {\"S\": \"bss-metadata-vg-config\"}}"
            --query "Item.value.S"
            --output text
    };

    // JSON output - output raw JSON strings that can be used as environment variables
    let mut output = serde_json::Map::new();

    // Add data VG config if available
    if let Ok(json_str) = data_vg_result
        && !json_str.trim().is_empty()
        && json_str.trim() != "None"
    {
        match serde_json::from_str::<serde_json::Value>(&json_str) {
            Ok(json_value) => {
                output.insert("data_vg_config".to_string(), json_value);
            }
            Err(e) => {
                error!("Failed to parse data VG config JSON: {}", e);
            }
        }
    }

    // Add metadata VG config if available
    if let Ok(json_str) = metadata_vg_result
        && !json_str.trim().is_empty()
        && json_str.trim() != "None"
    {
        match serde_json::from_str::<serde_json::Value>(&json_str) {
            Ok(json_value) => {
                output.insert("metadata_vg_config".to_string(), json_value);
            }
            Err(e) => {
                error!("Failed to parse metadata VG config JSON: {}", e);
            }
        }
    }

    // Output the combined JSON
    let combined_json = serde_json::Value::Object(output);
    match serde_json::to_string(&combined_json) {
        Ok(json_string) => println!("{}", json_string),
        Err(e) => error!("Failed to serialize combined JSON: {}", e),
    }

    Ok(())
}
