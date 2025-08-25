pub mod data_blob_resyncing;
pub mod data_blob_tracking;

use crate::{CmdResult, MultiAzTestType};

pub async fn run_multi_az_tests(test_type: MultiAzTestType) -> CmdResult {
    match test_type {
        MultiAzTestType::DataBlobTracking => data_blob_tracking::run_multi_az_tests().await,
        MultiAzTestType::DataBlobResyncing => {
            data_blob_resyncing::run_data_blob_resyncing_tests().await
        }
        MultiAzTestType::All => {
            data_blob_tracking::run_multi_az_tests().await?;
            data_blob_resyncing::run_data_blob_resyncing_tests().await
        }
    }
}
