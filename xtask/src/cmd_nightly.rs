use cmd_lib::*;

pub fn run_cmd_nightly() -> CmdResult {
    run_cmd! {
        info "Building ...";
        zig build 2>&1;
    }?;

    let rand_log = "test_art_nightly.log";
    run_cmd! {
        info "Running art tests (random) with log $rand_log ...";
        ./zig-out/bin/mkfs;
        ./zig-out/bin/test_art --tests random --size 1000000 --ops 80000000 -d 100 &> $rand_log;
    }
    .map_err(|e| {
        run_cmd!(tail $rand_log).unwrap();
        e
    })?;
    Ok(())
}
