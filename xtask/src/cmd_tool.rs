use crate::ToolKind;
use cmd_lib::*;

// Support GenUuids only for now
pub fn run_cmd_tool(ToolKind::GenUuids { num, file }: ToolKind) -> CmdResult {
    xtask_tools::gen_uuids(num, &file)?;
    Ok(())
}
