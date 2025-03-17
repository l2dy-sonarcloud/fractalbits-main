mod block_data_stream;
mod put_object;
mod upload_part;

pub use put_object::put_object;
pub use upload_part::upload_part;

use super::common::authorization::Authorization;

pub enum PutEndpoint {
    PutObject,
    UploadPart(u64, String),
}

impl PutEndpoint {
    pub fn authorization_type(&self) -> Authorization {
        match self {
            PutEndpoint::PutObject => Authorization::Write,
            PutEndpoint::UploadPart(..) => Authorization::Write,
        }
    }
}
