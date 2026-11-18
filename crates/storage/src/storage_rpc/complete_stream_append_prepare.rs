// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate read-only append preparation, NOT installation or acknowledgement
//! authority. The receiver must validate durable session/window state and the
//! sender must bind the returned complete context to its retained physical
//! custody. Framing never decodes a command or mints a submission.
use super::*;
use super::{complete_applied as command, complete_staging_ack as ack};

const MAGIC: &[u8; 4] = b"SAPP";
const REPLY: &[u8; 4] = b"SAPR";
const VERSION: u16 = 1;
pub(crate) const MAX_REQUEST_BYTES: usize = 10 + ack::MAX_REQUEST_BYTES;
pub(crate) const MAX_RESPONSE_BYTES: usize = 38 + command::MAX_REQUEST_BYTES;
const _: () = assert!(MAX_RESPONSE_BYTES + 6 <= STORAGE_RPC_MAX_PAYLOAD_LEN);

pub(crate) struct EncodedRequest {
    bytes: Vec<u8>,
    digest: [u8; 32],
    primary: complete_settled_prefix::Request,
}

fn invalid() -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope("acknowledged stream append preparation")
}

fn bounded(len: usize, limit: usize) -> Result<(), StorageRpcPayloadError> {
    if len > limit {
        Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
    } else {
        Ok(())
    }
}

fn validate(request: &ack::Request<'_>) -> Result<(), StorageRpcPayloadError> {
    let ack::Provenance::Stream { inspection } = request.provenance else {
        return Err(invalid());
    };
    let files = complete_stream_file_primary::decode_request(inspection)?;
    let allocation = complete_stream_allocation::decode_request(files.allocation)?;
    let original = allocation.window.route;
    let segment = &files.segment;
    let expected = &allocation.preparation.request;
    // Unlike file/ack requests, preparation is addressed to the original
    // OBJECT-PG primary, not a data-PG participant. No cutoff renewal is legal.
    if request.receiver.node != original.node
        || request.receiver.pg != original.pg
        || request.receiver.epoch != original.epoch
        || request.receiver.topology != original.topology
        || request.receiver.deadline.portable_wall_valid_until_ms == 0
        || request.receiver.deadline.portable_wall_valid_until_ms
            > original.deadline.portable_wall_valid_until_ms
        || segment.session_id != expected.session_id
        || segment.segment_index != expected.segment_index
        || segment.size != expected.size
        || segment.segment_crc64 != expected.segment_crc64
        || segment.payload_crc64 != expected.payload_crc64
        || segment.placement_cluster_epoch > original.epoch
        || segment.ec_k == 0
        || request.receipts.len() != usize::from(segment.ec_k) + usize::from(segment.ec_m)
    {
        return Err(invalid());
    }
    let session = match allocation.preparation.expected_append_state.as_ref() {
        Some(crate::ExpectedStreamAppendState::PutObject { session, .. })
        | Some(crate::ExpectedStreamAppendState::UploadPart { session, .. }) => session,
        None => return Err(invalid()),
    };
    let stored_size = session
        .encryption
        .payload_codec()
        .stored_size(segment.size)
        .ok_or_else(invalid)?;
    let shard_size = stored_size.div_ceil(u64::from(segment.ec_k));
    if session.session_id != segment.session_id
        || session.bucket != allocation.preparation.object.bucket
        || session.key != allocation.preparation.object.key
        || request
            .receipts
            .iter()
            .any(|r| r.ack.stored_size != shard_size)
    {
        return Err(invalid());
    }
    // Descriptor/placement provenance, allocation cursor, receipt authenticity
    // and durable acknowledgement ownership remain admitted-owner checks.
    Ok(())
}

pub(crate) fn encode_request(
    request: &ack::Request<'_>,
) -> Result<EncodedRequest, StorageRpcPayloadError> {
    validate(request)?;
    let nested = ack::encode_request(request)?;
    let mut bytes = MAGIC.to_vec();
    put_u16(&mut bytes, VERSION);
    put_bytes(&mut bytes, nested.bytes());
    Ok(EncodedRequest {
        digest: complete_stream_allocation::digest(&bytes),
        bytes,
        primary: request.receiver,
    })
}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<ack::Request<'_>, StorageRpcPayloadError> {
    bounded(bytes.len(), MAX_REQUEST_BYTES)?;
    let mut input = StorageRpcDecoder::new(bytes);
    if input.read_exact(4)? != MAGIC || input.read_u16()? != VERSION {
        return Err(invalid());
    }
    let len = input.read_u32()? as usize;
    bounded(len, ack::MAX_REQUEST_BYTES)?;
    let nested = input.read_exact(len)?;
    input.finish()?;
    let request = ack::decode_request(nested)?;
    validate(&request)?;
    Ok(request)
}

fn require_reply_subject(
    primary: complete_settled_prefix::Request,
    reply: &command::BorrowedRequest<'_>,
) -> Result<(), StorageRpcPayloadError> {
    if reply.node_id != primary.node || reply.epoch != primary.epoch || reply.pg_id != primary.pg {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn encode_response(
    request: &[u8],
    prepared: command::BorrowedRequest<'_>,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let original = decode_request(request)?;
    require_reply_subject(original.receiver, &prepared)?;
    let body = command::encode(prepared)?;
    let mut bytes = REPLY.to_vec();
    put_u16(&mut bytes, VERSION);
    bytes.extend_from_slice(&complete_stream_allocation::digest(request));
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

impl EncodedRequest {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) fn decode_response<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<command::BorrowedRequest<'a>, StorageRpcPayloadError> {
        bounded(bytes.len(), MAX_RESPONSE_BYTES)?;
        let mut input = StorageRpcDecoder::new(bytes);
        if input.read_exact(4)? != REPLY
            || input.read_u16()? != VERSION
            || input.read_exact(32)? != self.digest
        {
            return Err(invalid());
        }
        let reply = command::decode(&bytes[38..])?;
        require_reply_subject(self.primary, &reply)?;
        Ok(reply)
    }
}

#[cfg(test)]
#[path = "complete_stream_append_prepare_tests.rs"]
mod tests;
