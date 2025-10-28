use bytemuck::{Pod, Zeroable};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use xxhash_rust::xxh32::xxh32;

use crate::MessageHeaderTrait;

/// Generic protobuf-based message header implementation
///
/// This provides a common implementation for protobuf-based RPC protocols.
/// The Command type must be a protobuf enum (i32) that implements Pod and Zeroable.
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct ProtobufMessageHeader<Command>
where
    Command: Pod + Zeroable + Default + Clone + Copy + Send + Sync + 'static,
{
    /// A checksum covering only the remainder of this header.
    /// This allows the header to be trusted without having to fetch the associated body.
    /// Using xxhash32
    pub checksum: u32,

    /// A checksum covering only the associated body after this header.
    /// Using xxhash32
    pub checksum_body: u32,

    /// Trace ID for distributed tracing
    pub trace_id: u64,

    /// The size of the Header structure (always), plus any associated body.
    pub size: u32,

    /// Every request would be sent with a unique id, so the client can get the right response
    pub id: u32,

    /// The protocol command (method) for this message.
    /// i32 size, defined as protobuf enum type
    pub command: Command,

    /// Number of retry attempts for this request (0 = first attempt)
    pub retry_count: u8,

    /// Reserved for future use
    reserved: [u8; 3],
}

// Safety: ProtobufMessageHeader has the same layout requirements as its fields.
// When Command implements Pod (meaning it's valid for any bit pattern), and all other fields
// are primitive types that implement Pod, the whole struct is Pod.
unsafe impl<Command> Pod for ProtobufMessageHeader<Command> where
    Command: Pod + Zeroable + Default + Clone + Copy + Send + Sync + 'static
{
}

// Safety: When Command implements Zeroable (meaning all zeros is a valid value),
// and all other fields are primitive types that implement Zeroable, the whole struct is Zeroable.
unsafe impl<Command> Zeroable for ProtobufMessageHeader<Command> where
    Command: Pod + Zeroable + Default + Clone + Copy + Send + Sync + 'static
{
}

impl<Command> ProtobufMessageHeader<Command>
where
    Command: Pod + Zeroable + Default + Clone + Copy + Send + Sync + 'static,
{
    const _SIZE_OK: () = assert!(size_of::<Self>() == 32);
    pub const SIZE: usize = size_of::<Self>();

    pub fn encode(&self, dst: &mut BytesMut) {
        let bytes: &[u8] = bytemuck::bytes_of(self);
        dst.put(bytes);
    }

    pub fn decode_bytes(src: &Bytes) -> Self {
        let header_bytes = &src.chunk()[0..Self::SIZE];
        bytemuck::pod_read_unaligned::<Self>(header_bytes)
    }

    pub fn get_size_bytes(src: &mut BytesMut) -> usize {
        let offset = std::mem::offset_of!(Self, size);
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&src[offset..offset + 4]);
        u32::from_le_bytes(bytes) as usize
    }

    /// Calculate and set the checksum field for this header.
    /// The checksum covers all header fields after the checksum field itself.
    /// Using xxh32
    pub fn set_checksum(&mut self) {
        let checksum_offset = std::mem::offset_of!(Self, checksum);
        let bytes: &[u8] = bytemuck::bytes_of(self);
        let bytes_to_hash = &bytes[checksum_offset + size_of::<u32>()..Self::SIZE];
        let hash = xxh32(bytes_to_hash, 0);
        self.checksum = hash;
    }

    /// Verify that the checksum field matches the calculated checksum.
    /// Returns true if valid, false if invalid.
    pub fn verify_checksum(&self) -> bool {
        let checksum_offset = std::mem::offset_of!(Self, checksum);
        let bytes: &[u8] = bytemuck::bytes_of(self);
        let bytes_to_hash = &bytes[checksum_offset + size_of::<u32>()..Self::SIZE];
        let hash = xxh32(bytes_to_hash, 0);
        self.checksum == hash
    }
}

impl<Command> MessageHeaderTrait for ProtobufMessageHeader<Command>
where
    Command: Pod + Zeroable + Default + Clone + Copy + Send + Sync + 'static,
{
    const SIZE: usize = 32;

    fn encode(&self, dst: &mut BytesMut) {
        self.encode(dst)
    }

    fn decode(src: &[u8]) -> Self {
        bytemuck::pod_read_unaligned::<Self>(&src[..Self::SIZE])
    }

    fn get_size(src: &[u8]) -> usize {
        let offset = std::mem::offset_of!(Self, size);
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&src[offset..offset + 4]);
        u32::from_le_bytes(bytes) as usize
    }

    fn set_size(&mut self, size: u32) {
        self.size = size;
    }

    fn get_id(&self) -> u32 {
        self.id
    }

    fn set_id(&mut self, id: u32) {
        self.id = id;
    }

    fn get_body_size(&self) -> usize {
        (self.size as usize).saturating_sub(Self::SIZE)
    }

    fn get_retry_count(&self) -> u32 {
        self.retry_count.into()
    }

    fn set_retry_count(&mut self, retry_count: u32) {
        self.retry_count = retry_count as u8;
    }

    fn get_trace_id(&self) -> u64 {
        self.trace_id
    }

    fn set_trace_id(&mut self, trace_id: u64) {
        self.trace_id = trace_id;
    }

    fn set_checksum(&mut self) {
        self.set_checksum()
    }

    fn verify_checksum(&self) -> bool {
        self.verify_checksum()
    }
}
