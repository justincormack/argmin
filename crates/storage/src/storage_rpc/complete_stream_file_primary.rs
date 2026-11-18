// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only post-allocation inspection. Neither this description nor its reply
//! grants physical authority. Allocation and inspection have distinct purposes.
use super::complete_stream_allocation as allocation;
use super::*;

pub(crate) const MAGIC: &[u8; 4] = b"SFPI";
const REPLY: &[u8; 4] = b"SFOK";
const VERSION: u16 = 1;
// A fresh allocation has no shard-integrity witness; its bounded session ID
// and scalar geometry fit comfortably within this independent framing limit.
const MAX_SEGMENT_BYTES: usize = 1024;
pub(crate) const MAX_REQUEST_BYTES: usize =
    6 + 4 + allocation::MAX_REQUEST_BYTES + MAX_SEGMENT_BYTES;

pub(crate) struct Request<'a> {
    pub(crate) allocation: &'a [u8],
    pub(crate) segment: StreamUploadSegmentRecord,
}

fn invalid() -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope("stream file inspection request/response")
}

pub(crate) fn encode_request(
    allocation: &[u8],
    segment: &StreamUploadSegmentRecord,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    allocation::decode_request(allocation)?;
    if segment.shard_integrity.is_some() {
        return Err(invalid());
    }
    let mut encoded = Vec::new();
    put_stream_upload_segment_record(&mut encoded, segment);
    if encoded.len() > MAX_SEGMENT_BYTES {
        return Err(invalid());
    }
    let mut bytes = MAGIC.to_vec();
    put_u16(&mut bytes, VERSION);
    put_bytes(&mut bytes, allocation);
    bytes.extend_from_slice(&encoded);
    Ok(bytes)
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
    if len > allocation::MAX_REQUEST_BYTES
        || bytes.len().saturating_sub(10 + len) > MAX_SEGMENT_BYTES
    {
        return Err(invalid());
    }
    let allocation = input.read_exact(len)?;
    allocation::decode_request(allocation)?;
    let segment = input.read_stream_upload_segment_record()?;
    input.finish()?;
    if segment.shard_integrity.is_some() {
        return Err(invalid());
    }
    Ok(Request {
        allocation,
        segment,
    })
}

pub(crate) fn encode_response(request: &[u8]) -> Vec<u8> {
    let mut bytes = REPLY.to_vec();
    put_u16(&mut bytes, VERSION);
    bytes.extend_from_slice(&allocation::digest(request));
    bytes
}

pub(crate) fn decode_response(bytes: &[u8], request: &[u8]) -> Result<(), StorageRpcPayloadError> {
    let mut input = StorageRpcDecoder::new(bytes);
    if input.read_exact(4)? != REPLY
        || input.read_u16()? != VERSION
        || input.read_exact(32)? != allocation::digest(request)
    {
        return Err(invalid());
    }
    input.finish()
}
