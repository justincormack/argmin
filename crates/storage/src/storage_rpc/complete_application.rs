// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate complete application V1. Framing is untrusted input, not authority.
//! Separate purpose tags prevent publication/probe payloads from substituting.

use super::*;
use crate::metadata_command::{MetadataCommandId, MetadataCommandReplicaState};

const REQUEST_MAGIC: &[u8; 4] = b"APLY";
const RESPONSE_MAGIC: &[u8; 4] = b"APLD";
const VERSION: u16 = 1;
pub(crate) const MAX_REQUEST_BYTES: usize = 4 + 2 + 8 + complete_applied::MAX_REQUEST_BYTES;
pub(crate) const RESPONSE_BYTES: usize = 4 + 2 + 4 + 8 + 4 + 8 + 32 + 34;

pub(crate) struct BorrowedRequest<'a> {
    pub(crate) command: complete_applied::BorrowedRequest<'a>,
    pub(crate) deadline: StorageRpcOperationDeadline,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Confirmation {
    pub(crate) node_id: NodeId,
    pub(crate) id: MetadataCommandId,
    pub(crate) command_sha256: [u8; 32],
    pub(crate) state: MetadataCommandReplicaState,
}

fn invalid(reason: &'static str) -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope(reason)
}

fn header(
    decoder: &mut StorageRpcDecoder<'_>,
    magic: &[u8; 4],
) -> Result<(), StorageRpcPayloadError> {
    if decoder.read_exact(4)? != magic || decoder.read_u16()? != VERSION {
        return Err(invalid("complete application purpose/version"));
    }
    Ok(())
}

pub(crate) fn encode_request(
    request: BorrowedRequest<'_>,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let body = complete_applied::encode(request.command)?;
    let mut out = Vec::with_capacity(14 + body.len());
    out.extend_from_slice(REQUEST_MAGIC);
    put_u16(&mut out, VERSION);
    put_u64(&mut out, request.deadline.portable_wall_valid_until_ms);
    out.extend_from_slice(&body);
    Ok(out)
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
    let mut out = Vec::with_capacity(RESPONSE_BYTES);
    out.extend_from_slice(RESPONSE_MAGIC);
    put_u16(&mut out, VERSION);
    put_u32(&mut out, confirmation.node_id.as_u32());
    put_u64(&mut out, confirmation.id.cluster_epoch().get());
    put_u32(&mut out, confirmation.id.pg_id().get());
    put_u64(&mut out, confirmation.id.log_index().get());
    out.extend_from_slice(&confirmation.command_sha256);
    out.extend_from_slice(&encode_metadata_command_state_response(
        &StorageRpcMetadataCommandStateResponse {
            state: confirmation.state,
        },
    ));
    out
}

pub(crate) fn decode_confirmation(bytes: &[u8]) -> Result<Confirmation, StorageRpcPayloadError> {
    if bytes.len() != RESPONSE_BYTES {
        return Err(invalid("complete application confirmation size"));
    }
    let mut decoder = StorageRpcDecoder::new(bytes);
    header(&mut decoder, RESPONSE_MAGIC)?;
    let node_id = NodeId::new(decoder.read_u32()?);
    let epoch = decoder.read_cluster_epoch()?;
    let pg = PgId::new(decoder.read_u32()?);
    let index = MetadataCommandLogIndex::new(decoder.read_u64()?)
        .ok_or_else(|| invalid("complete application confirmation index"))?;
    let command_sha256 = decoder
        .read_exact(32)?
        .try_into()
        .expect("exact digest width");
    let state = decode_metadata_command_state_response(decoder.read_exact(34)?)?.state;
    decoder.finish()?;
    if state.cluster_epoch != epoch || state.applied_log_index < index.get() {
        return Err(invalid("complete application confirmation prefix"));
    }
    Ok(Confirmation {
        node_id,
        id: MetadataCommandId::new(epoch, pg, index),
        command_sha256,
        state,
    })
}

/// Request matching only, not publication/cleanup authority or sender history.
pub(crate) fn decode_confirmation_for(
    bytes: &[u8],
    node_id: NodeId,
    id: MetadataCommandId,
    command_sha256: [u8; 32],
) -> Result<MetadataCommandReplicaState, StorageRpcPayloadError> {
    let confirmation = decode_confirmation(bytes)?;
    if confirmation.node_id != node_id
        || confirmation.id != id
        || confirmation.command_sha256 != command_sha256
    {
        return Err(invalid("complete application confirmation subject"));
    }
    Ok(confirmation.state)
}

#[cfg(test)]
#[path = "complete_application_tests.rs"]
mod tests;
