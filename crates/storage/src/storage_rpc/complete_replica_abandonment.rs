// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate replica abandonment V1. Separate purpose from primary abandonment:
//! the authenticated sender owns primary confirmation; this is not a transferable
//! tombstone certificate. Receiver admission independently checks the exact route.

use super::*;
use crate::metadata_command::MetadataCommandId;

const REQUEST_MAGIC: &[u8; 4] = b"RABD";
const RESPONSE_MAGIC: &[u8; 4] = b"RAOK";
const VERSION: u16 = 1;
pub(crate) const MAX_REQUEST_BYTES: usize = 18 + complete_applied::MAX_REQUEST_BYTES;
const RESPONSE_BYTES: usize = 66;

pub(crate) struct BorrowedRequest<'a> {
    pub(crate) primary: NodeId,
    pub(crate) command: complete_applied::BorrowedRequest<'a>,
    pub(crate) deadline: StorageRpcOperationDeadline,
}

fn invalid(reason: &'static str) -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope(reason)
}

fn header(
    decoder: &mut StorageRpcDecoder<'_>,
    magic: &[u8; 4],
) -> Result<(), StorageRpcPayloadError> {
    if decoder.read_exact(4)? != magic || decoder.read_u16()? != VERSION {
        return Err(invalid("replica abandonment purpose/version"));
    }
    Ok(())
}

pub(crate) fn is_request(bytes: &[u8]) -> bool {
    bytes.starts_with(REQUEST_MAGIC)
}

pub(crate) fn encode_request(
    request: BorrowedRequest<'_>,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.primary == request.command.node_id {
        return Err(invalid("replica abandonment targets primary"));
    }
    let body = complete_applied::encode(request.command)?;
    let mut bytes = Vec::with_capacity(18 + body.len());
    bytes.extend_from_slice(REQUEST_MAGIC);
    put_u16(&mut bytes, VERSION);
    put_u64(&mut bytes, request.deadline.portable_wall_valid_until_ms);
    put_u32(&mut bytes, request.primary.as_u32());
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
    let primary = NodeId::new(decoder.read_u32()?);
    let command = complete_applied::decode(&bytes[18..])?;
    if primary == command.node_id {
        return Err(invalid("replica abandonment targets primary"));
    }
    Ok(BorrowedRequest {
        primary,
        command,
        deadline,
    })
}

pub(crate) fn encode_confirmation(
    primary: NodeId,
    confirmation: complete_abandonment::Confirmation,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(RESPONSE_BYTES);
    bytes.extend_from_slice(RESPONSE_MAGIC);
    put_u16(&mut bytes, VERSION);
    put_u32(&mut bytes, primary.as_u32());
    put_u32(&mut bytes, confirmation.node_id.as_u32());
    put_u64(&mut bytes, confirmation.id.cluster_epoch().get());
    put_u32(&mut bytes, confirmation.id.pg_id().get());
    put_u64(&mut bytes, confirmation.id.log_index().get());
    bytes.extend_from_slice(&confirmation.command_sha256);
    bytes
}

pub(crate) fn decode_confirmation_for(
    bytes: &[u8],
    primary: NodeId,
    node: NodeId,
    id: MetadataCommandId,
    sha256: [u8; 32],
) -> Result<(), StorageRpcPayloadError> {
    if bytes.len() != RESPONSE_BYTES || primary == node {
        return Err(invalid("replica abandonment confirmation size/role"));
    }
    let mut decoder = StorageRpcDecoder::new(bytes);
    header(&mut decoder, RESPONSE_MAGIC)?;
    if decoder.read_u32()? != primary.as_u32()
        || decoder.read_u32()? != node.as_u32()
        || decoder.read_cluster_epoch()? != id.cluster_epoch()
        || decoder.read_u32()? != id.pg_id().get()
        || decoder.read_u64()? != id.log_index().get()
        || decoder.read_exact(32)? != sha256
    {
        return Err(invalid("replica abandonment confirmation subject"));
    }
    decoder.finish()
}

#[cfg(test)]
#[path = "complete_replica_abandonment_tests.rs"]
mod tests;
