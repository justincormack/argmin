// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate one-slot preparation V1. No command decoder or physical grant.
use super::complete_window_primary as window;
use super::*;

const MAGIC: &[u8; 4] = b"SALL";
const REPLY: &[u8; 4] = b"SALR";
const VERSION: u16 = 1;
pub(crate) const MAX_REQUEST_BYTES: usize = 6
    + 4
    + window::MAX_REQUEST_BYTES
    + 34
    + 4
    + STORAGE_RPC_MAX_STREAM_SEGMENT_APPEND_PREPARE_REQUEST_PAYLOAD_LEN;

pub(crate) struct Request<'a> {
    pub(crate) window: window::Request<'a>,
    pub(crate) prefix: MetadataCommandReplicaState,
    pub(crate) preparation: StorageRpcStreamSegmentAppendPrepareRequest,
}

fn invalid() -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope("confirmed stream allocation request/response")
}

fn require_subject(request: &Request<'_>) -> Result<(), StorageRpcPayloadError> {
    let route = request.window.route;
    let object = &request.preparation.object;
    if object.node_id != route.node
        || object.pg_id != route.pg
        || object.cluster_epoch != route.epoch
        || request.prefix.cluster_epoch != route.epoch
        || request.preparation.expected_append_state.is_none()
        || request.preparation.effect_deadline.is_some()
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn encode_request(request: &Request<'_>) -> Result<Vec<u8>, StorageRpcPayloadError> {
    require_subject(request)?;
    let window = window::encode_request(request.window)?;
    let preparation = encode_stream_segment_append_prepare_request(&request.preparation);
    if preparation.len() > STORAGE_RPC_MAX_STREAM_SEGMENT_APPEND_PREPARE_REQUEST_PAYLOAD_LEN {
        return Err(invalid());
    }
    let mut out = MAGIC.to_vec();
    put_u16(&mut out, VERSION);
    put_bytes(&mut out, &window);
    out.extend_from_slice(&encode_metadata_command_state_response(
        &StorageRpcMetadataCommandStateResponse {
            state: request.prefix,
        },
    ));
    put_bytes(&mut out, &preparation);
    Ok(out)
}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<Request<'_>, StorageRpcPayloadError> {
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(invalid());
    }
    let mut input = StorageRpcDecoder::new(bytes);
    if input.read_exact(4)? != MAGIC || input.read_u16()? != VERSION {
        return Err(invalid());
    }
    let len = input.read_u32()? as usize;
    let window = window::decode_request(input.read_exact(len)?)?;
    let prefix = decode_metadata_command_state_response(input.read_exact(34)?)?.state;
    let len = input.read_u32()? as usize;
    if len > STORAGE_RPC_MAX_STREAM_SEGMENT_APPEND_PREPARE_REQUEST_PAYLOAD_LEN {
        return Err(invalid());
    }
    let preparation = decode_stream_segment_append_prepare_request(input.read_exact(len)?)?;
    input.finish()?;
    let request = Request {
        window,
        prefix,
        preparation,
    };
    require_subject(&request)?;
    Ok(request)
}

pub(crate) fn digest(bytes: &[u8]) -> [u8; 32] {
    checksum::compute_checksum(checksum::ChecksumAlgorithm::Sha256, bytes)
        .bytes()
        .try_into()
        .expect("SHA-256 length")
}

pub(crate) fn encode_response(
    request_digest: [u8; 32],
    segment: &StreamUploadSegmentRecord,
) -> Vec<u8> {
    let mut out = REPLY.to_vec();
    put_u16(&mut out, VERSION);
    out.extend_from_slice(&request_digest);
    put_stream_upload_segment_record(&mut out, segment);
    out
}

pub(crate) fn decode_response(
    bytes: &[u8],
    request_digest: [u8; 32],
) -> Result<StreamUploadSegmentRecord, StorageRpcPayloadError> {
    let mut input = StorageRpcDecoder::new(bytes);
    if input.read_exact(4)? != REPLY
        || input.read_u16()? != VERSION
        || input.read_exact(32)? != request_digest
    {
        return Err(invalid());
    }
    let segment = input.read_stream_upload_segment_record()?;
    input.finish()?;
    Ok(segment)
}

#[cfg(test)]
#[path = "complete_stream_allocation_tests.rs"]
mod tests;
