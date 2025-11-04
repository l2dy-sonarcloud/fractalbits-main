use bytes::{Bytes, BytesMut};

pub mod protobuf_header;
pub use protobuf_header::{EMPTY_BODY_CHECKSUM, ProtobufMessageHeader, verify_header_checksum_raw};

pub trait MessageHeaderTrait: Sized + Clone + Copy + Send + Sync + 'static {
    const SIZE: usize;

    fn encode(&self, dst: &mut BytesMut);
    fn decode(src: &[u8]) -> Self;
    fn get_size(src: &[u8]) -> usize;
    fn set_size(&mut self, size: u32);
    fn get_id(&self) -> u32;
    fn set_id(&mut self, id: u32);
    fn get_body_size(&self) -> usize;
    fn get_retry_count(&self) -> u32;
    fn set_retry_count(&mut self, retry_count: u32);
    fn get_trace_id(&self) -> u64;
    fn set_trace_id(&mut self, trace_id: u64);
    fn set_checksum(&mut self);
    fn set_body_checksum(&mut self, body: &[u8]);
    fn verify_body_checksum(&self, body: &[u8]) -> bool;
    fn set_body_checksum_vectored(&mut self, chunks: &[impl AsRef<[u8]>]);
}

pub struct MessageFrame<H: MessageHeaderTrait, B = Bytes> {
    pub header: H,
    pub body: B,
}

impl<H: MessageHeaderTrait, B> MessageFrame<H, B> {
    pub fn new(header: H, body: B) -> Self {
        Self { header, body }
    }
}

impl<H: MessageHeaderTrait> MessageFrame<H, Bytes> {
    pub fn from_bytes(header: H, body: Bytes) -> Self {
        Self { header, body }
    }
}

impl<'a, H: MessageHeaderTrait> MessageFrame<H, &'a [u8]> {
    pub fn from_slice(header: H, body: &'a [u8]) -> Self {
        Self { header, body }
    }
}

#[derive(Default, Clone)]
pub struct MessageCodec<H: MessageHeaderTrait> {
    _phantom: std::marker::PhantomData<H>,
}

#[macro_export]
macro_rules! impl_protobuf_message_header {
    ($header_type:ident, $command_type:ty) => {
        // Safety: Command is defined as protobuf enum type (i32), and 0 as Invalid. There is also no padding
        // as verified from the zig side. With header checksum validation, we can also be sure no invalid
        // enum value being interpreted.
        unsafe impl bytemuck::Pod for $command_type {}
        unsafe impl bytemuck::Zeroable for $command_type {}

        impl $header_type {
            pub const SIZE: usize = $crate::ProtobufMessageHeader::<$command_type>::SIZE;

            pub fn encode(&self, dst: &mut bytes::BytesMut) {
                self.0.encode(dst)
            }

            pub fn decode_bytes(src: &bytes::Bytes) -> Self {
                Self($crate::ProtobufMessageHeader::decode_bytes(src))
            }

            pub fn get_size_bytes(src: &mut bytes::BytesMut) -> usize {
                $crate::ProtobufMessageHeader::<$command_type>::get_size_bytes(src)
            }

            pub fn set_checksum(&mut self) {
                self.0.set_checksum()
            }

            pub fn set_body_checksum(&mut self, body: &[u8]) {
                self.0.set_body_checksum(body)
            }

            pub fn verify_body_checksum(&self, body: &[u8]) -> bool {
                self.0.verify_body_checksum(body)
            }

            pub fn set_body_checksum_vectored(&mut self, chunks: &[impl AsRef<[u8]>]) {
                self.0.set_body_checksum_vectored(chunks)
            }
        }

        impl std::ops::Deref for $header_type {
            type Target = $crate::ProtobufMessageHeader<$command_type>;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl std::ops::DerefMut for $header_type {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl $crate::MessageHeaderTrait for $header_type {
            const SIZE: usize = $crate::ProtobufMessageHeader::<$command_type>::SIZE;

            fn encode(&self, dst: &mut bytes::BytesMut) {
                self.0.encode(dst)
            }

            fn decode(src: &[u8]) -> Self {
                Self($crate::ProtobufMessageHeader::decode(src))
            }

            fn get_size(src: &[u8]) -> usize {
                $crate::ProtobufMessageHeader::<$command_type>::get_size(src)
            }

            fn set_size(&mut self, size: u32) {
                self.0.set_size(size)
            }

            fn get_id(&self) -> u32 {
                self.0.get_id()
            }

            fn set_id(&mut self, id: u32) {
                self.0.set_id(id)
            }

            fn get_body_size(&self) -> usize {
                self.0.get_body_size()
            }

            fn get_retry_count(&self) -> u32 {
                self.0.get_retry_count()
            }

            fn set_retry_count(&mut self, retry_count: u32) {
                self.0.set_retry_count(retry_count)
            }

            fn get_trace_id(&self) -> u64 {
                self.0.get_trace_id()
            }

            fn set_trace_id(&mut self, trace_id: u64) {
                self.0.set_trace_id(trace_id)
            }

            fn set_checksum(&mut self) {
                self.0.set_checksum()
            }

            fn set_body_checksum(&mut self, body: &[u8]) {
                self.0.set_body_checksum(body)
            }

            fn verify_body_checksum(&self, body: &[u8]) -> bool {
                self.0.verify_body_checksum(body)
            }

            fn set_body_checksum_vectored(&mut self, chunks: &[impl AsRef<[u8]>]) {
                self.0.set_body_checksum_vectored(chunks)
            }
        }
    };
}
