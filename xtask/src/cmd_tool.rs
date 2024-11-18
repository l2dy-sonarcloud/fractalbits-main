use cmd_lib::*;
use rayon::prelude::*;
use std::io::prelude::*;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

use crate::ToolKind;

// Support GenUuids only for now
pub fn run_cmd_tool(ToolKind::GenUuids { num, file }: ToolKind) -> CmdResult {
    info!("Generating {num} uuids into file {file}");
    let num_threads = num_cpus::get();
    let num_uuids = (num as usize) / num_threads;
    let last_num_uuids = num - (num_uuids as u32) * (num_threads as u32 - 1);

    let uuids = Arc::new(Mutex::new(Vec::new()));
    (0..num_threads).into_par_iter().for_each(|i| {
        let mut uuids_str = String::new();
        let n = if i == num_threads - 1 {
            last_num_uuids as usize
        } else {
            num_uuids
        };
        for _ in 0..n {
            uuids_str += &Uuid::now_v7().to_string();
            uuids_str += "\n";
        }
        uuids.lock().unwrap().push(uuids_str);
    });

    std::fs::File::options()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&file)
        .expect(&format!("Truncate {file} failed"));
    let mut f = std::fs::File::options()
        .append(true)
        .open(&file)
        .expect(&format!("Open {file} for writing failed"));
    for uuid in uuids.lock().unwrap().iter() {
        f.write_all(uuid.as_bytes())?;
    }
    info!("File {file} is ready");
    Ok(())
}
