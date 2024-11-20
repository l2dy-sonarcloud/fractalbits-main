use rkyv::{Archive, Deserialize, Serialize};
use uuid::Uuid;

#[derive(Archive, Deserialize, Serialize)]
pub struct ObjectLayout {
    pub timestamp: u64,
    pub size: u64,
    pub blob_id: Uuid,
    // TODO: pub state: mpu related states, tombstone state, etc
}

impl ObjectLayout {
    const _SIZE_OK: () = assert!(size_of::<Self>() == 32);
    const _ALIGNMENT_OK: () = assert!(align_of::<Self>() == 8);
    pub const ALIGNMENT: usize = align_of::<Self>();
}
