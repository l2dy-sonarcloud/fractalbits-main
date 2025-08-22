pub mod leader_election;
pub mod multi_az;

use crate::{CmdResult, TestType};

pub async fn run_tests(test_type: TestType) -> CmdResult {
    match test_type {
        TestType::MultiAz => multi_az::run_multi_az_tests().await,
        TestType::LeaderElection => leader_election::run_leader_election_tests().await,
    }
}
