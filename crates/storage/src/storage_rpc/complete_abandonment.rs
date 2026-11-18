// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate unstarted primary abandonment V1. Untrusted framing, NOT proof of
//! replica abandonment, cleanup or terminal ownership transfer. Live receiver dispatch remains unselected.

use super::*;
use crate::metadata_command::MetadataCommandId;

const REQUEST_MAGIC: &[u8; 4] = b"ABND";
const RESPONSE_MAGIC: &[u8; 4] = b"ABOK";
const VERSION: u16 = 1;
pub(crate) const MAX_REQUEST_BYTES: usize = 4 + 2 + 8 + complete_applied::MAX_REQUEST_BYTES;
pub(crate) const RESPONSE_BYTES: usize = 4 + 2 + 4 + 8 + 4 + 8 + 32;

pub(crate) struct BorrowedRequest<'a> {
    pub(crate) command: complete_applied::BorrowedRequest<'a>,
    pub(crate) deadline: StorageRpcOperationDeadline,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Confirmation {
    pub(crate) node_id: NodeId,
    pub(crate) id: MetadataCommandId,
    pub(crate) command_sha256: [u8; 32],
}

fn invalid(reason: &'static str) -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope(reason)
}

fn header(
    decoder: &mut StorageRpcDecoder<'_>,
    magic: &[u8; 4],
) -> Result<(), StorageRpcPayloadError> {
    if decoder.read_exact(4)? != magic || decoder.read_u16()? != VERSION {
        return Err(invalid("complete abandonment purpose/version"));
    }
    Ok(())
}

pub(crate) fn encode_request(
    request: BorrowedRequest<'_>,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let body = complete_applied::encode(request.command)?;
    let mut bytes = Vec::with_capacity(14 + body.len());
    bytes.extend_from_slice(REQUEST_MAGIC);
    put_u16(&mut bytes, VERSION);
    put_u64(&mut bytes, request.deadline.portable_wall_valid_until_ms);
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<BorrowedRequest<'_>, StorageRpcPayloadError> {
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: bytes.len(),
            limit: MAX_REQUEST_BYTES,
        });
    }
    let mut decoder = StorageRpcDecoder::new(bytes);
    header(&mut decoder, REQUEST_MAGIC)?;
    let deadline = StorageRpcOperationDeadline {
        portable_wall_valid_until_ms: decoder.read_u64()?,
    };
    Ok(BorrowedRequest {
        command: complete_applied::decode(&bytes[14..])?,
        deadline,
    })
}

pub(crate) fn encode_confirmation(confirmation: Confirmation) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(RESPONSE_BYTES);
    bytes.extend_from_slice(RESPONSE_MAGIC);
    put_u16(&mut bytes, VERSION);
    put_u32(&mut bytes, confirmation.node_id.as_u32());
    put_u64(&mut bytes, confirmation.id.cluster_epoch().get());
    put_u32(&mut bytes, confirmation.id.pg_id().get());
    put_u64(&mut bytes, confirmation.id.log_index().get());
    bytes.extend_from_slice(&confirmation.command_sha256);
    bytes
}

pub(crate) fn decode_confirmation_for(
    bytes: &[u8],
    node_id: NodeId,
    id: MetadataCommandId,
    sha256: [u8; 32],
) -> Result<(), StorageRpcPayloadError> {
    if bytes.len() != RESPONSE_BYTES {
        return Err(invalid("complete abandonment confirmation size"));
    }
    let mut decoder = StorageRpcDecoder::new(bytes);
    header(&mut decoder, RESPONSE_MAGIC)?;
    if decoder.read_u32()? != node_id.as_u32()
        || decoder.read_cluster_epoch()? != id.cluster_epoch()
        || decoder.read_u32()? != id.pg_id().get()
        || decoder.read_u64()? != id.log_index().get()
        || decoder.read_exact(32)? != sha256
    {
        return Err(invalid("complete abandonment confirmation subject"));
    }
    decoder.finish()
}

#[cfg(test)]
#[path = "complete_abandonment_tests.rs"]
mod tests;
