use std::time::Duration;

use crate::client::RpcClient;
use bss_codec::{Command, MessageHeader};
use bytes::Bytes;
use rpc_client_common::{ErrorRetryable, InflightRpcGuard, RpcError};
use rpc_codec_common::MessageFrame;
use tracing::error;
use uuid::Uuid;

impl RpcClient {
    pub async fn put_data_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        volume_id: u8,
        body: Bytes,
        timeout: Option<Duration>,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "put_data_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_id.into_bytes();
        header.block_number = block_number;
        header.volume_id = volume_id;
        header.command = Command::PutDataBlob;
        header.size = (MessageHeader::SIZE + body.len()) as u32;

        let msg_frame = MessageFrame::new(header, body);
        self
            .send_request(request_id, msg_frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"put_data_blob", %request_id, %blob_id, %block_number, %volume_id, error=?e, "bss rpc failed");
                }
                e
            })?;
        Ok(())
    }

    pub async fn get_data_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        volume_id: u8,
        body: &mut Bytes,
        timeout: Option<Duration>,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "get_data_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_id.into_bytes();
        header.block_number = block_number;
        header.volume_id = volume_id;
        header.command = Command::GetDataBlob;
        header.size = MessageHeader::SIZE as u32;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        let resp_frame = self
            .send_request(header.id, msg_frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"get_data_blob", %request_id, %blob_id, %block_number, %volume_id, error=?e, "bss rpc failed");
                }
                e
            })?;
        *body = resp_frame.body;
        Ok(())
    }

    pub async fn delete_data_blob(
        &self,
        blob_id: Uuid,
        block_number: u32,
        volume_id: u8,
        timeout: Option<Duration>,
    ) -> Result<(), RpcError> {
        let _guard = InflightRpcGuard::new("bss", "delete_data_blob");
        let mut header = MessageHeader::default();
        let request_id = self.gen_request_id();
        header.id = request_id;
        header.blob_id = blob_id.into_bytes();
        header.block_number = block_number;
        header.volume_id = volume_id;
        header.command = Command::DeleteDataBlob;
        header.size = MessageHeader::SIZE as u32;

        let msg_frame = MessageFrame::new(header, Bytes::new());
        self
            .send_request(header.id, msg_frame, timeout)
            .await
            .map_err(|e| {
                if !e.retryable() {
                    error!(rpc=%"delete_data_blob", %request_id, %blob_id, %block_number, %volume_id, error=?e, "bss rpc failed");
                }
                e
            })?;
        Ok(())
    }
}
