mod abort_multipart_upload;
mod delete_object;

pub use abort_multipart_upload::abort_multipart_upload;
pub use delete_object::delete_object;

use super::common::authorization::Authorization;

pub enum DeleteEndpoint {
    AbortMultipartUpload(String),
    DeleteObject,
}

impl DeleteEndpoint {
    pub fn authorization_type(&self) -> Authorization {
        match self {
            DeleteEndpoint::AbortMultipartUpload(_) => Authorization::Write,
            DeleteEndpoint::DeleteObject => Authorization::Write,
        }
    }
}
