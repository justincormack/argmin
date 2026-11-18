// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate file-effect transport, not authority. No live dispatch selects it.
//! The receiver must independently confirm provenance before physical exclusion.
//! Decoding borrows the payload; neither side receives a caller-selected shard key.

use super::*;
use super::{complete_settled_prefix as route, complete_stream_file_primary as stream};

const MAGIC: &[u8; 4] = b"SWRT";
const REPLY: &[u8; 4] = b"SWOK";
const VERSION: u16 = 1;
// Includes encrypted segment overhead with room above the current 8 MiB unit.
// Geometry validation belongs to the retained window, not this framing limit.
pub(crate) const MAX_SHARD_BYTES: usize = 16 * 1024 * 1024;
const MAX_DIRECT_DESCRIPTION_BYTES: usize = 8 + 4 + complete_window_primary::MAX_DESCRIPTOR_BYTES;
const MAX_STREAM_DESCRIPTION_BYTES: usize = 4 + stream::MAX_REQUEST_BYTES;
pub(super) const MAX_DESCRIPTION_BYTES: usize =
    if MAX_DIRECT_DESCRIPTION_BYTES > MAX_STREAM_DESCRIPTION_BYTES {
        MAX_DIRECT_DESCRIPTION_BYTES
    } else {
        MAX_STREAM_DESCRIPTION_BYTES
    };
pub(crate) const MAX_REQUEST_BYTES: usize =
    6 + route::REQUEST_BYTES + 1 + MAX_DESCRIPTION_BYTES + 1 + 4 + MAX_SHARD_BYTES;

#[derive(Clone, Copy)]
pub(crate) enum Provenance<'a> {
    Direct {
        descriptor: &'a [u8],
        logical_size: u64,
    },
    Stream {
        inspection: &'a [u8],
    },
}

#[derive(Clone, Copy)]
pub(crate) struct Request<'a> {
    /// Receiver node, object PG, placement epoch, topology and absolute cutoff.
    /// Data PG and physical key must be derived from the retained description.
    pub(crate) receiver: route::Request,
    pub(crate) provenance: Provenance<'a>,
    pub(crate) shard_index: u8,
    pub(crate) bytes: &'a [u8],
}

/// Request-bound reply checking only, never installation or write authority.
/// Compute the outgoing shard CRC once, not on each attempted response decode.
pub(crate) struct EncodedRequest {
    bytes: Vec<u8>,
    digest: [u8; 32],
    expected: WriteAck,
}

fn invalid() -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope("staging file request/response")
}

fn bounded(len: usize, limit: usize) -> Result<(), StorageRpcPayloadError> {
    if len > limit {
        Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
    } else {
        Ok(())
    }
}

fn validate(request: Request<'_>) -> Result<(), StorageRpcPayloadError> {
    bounded(request.bytes.len(), MAX_SHARD_BYTES)?;
    if request.bytes.is_empty() {
        return Err(invalid());
    }
    validate_provenance(request.receiver, request.provenance)
}

pub(super) fn validate_provenance(
    receiver: route::Request,
    provenance: Provenance<'_>,
) -> Result<(), StorageRpcPayloadError> {
    if receiver.deadline.portable_wall_valid_until_ms == 0 {
        return Err(invalid());
    }
    match provenance {
        Provenance::Direct { descriptor, .. } => {
            bounded(
                descriptor.len(),
                complete_window_primary::MAX_DESCRIPTOR_BYTES,
            )?;
            if descriptor.is_empty() {
                return Err(invalid());
            }
        }
        Provenance::Stream { inspection } => {
            bounded(inspection.len(), stream::MAX_REQUEST_BYTES)?;
            let files = stream::decode_request(inspection)?;
            let allocation = complete_stream_allocation::decode_request(files.allocation)?;
            let original = allocation.window.route;
            if original.pg != receiver.pg
                || original.epoch != receiver.epoch
                || original.topology != receiver.topology
                || receiver.deadline.portable_wall_valid_until_ms
                    > original.deadline.portable_wall_valid_until_ms
            {
                return Err(invalid());
            }
            // The receiver need not be the primary named by the allocation.
            // Exact placement/session/cursor validation remains receiver-owned.
        }
    }
    Ok(())
}

pub(crate) fn encode_request(
    request: Request<'_>,
) -> Result<EncodedRequest, StorageRpcPayloadError> {
    validate(request)?;
    let mut bytes = MAGIC.to_vec();
    put_u16(&mut bytes, VERSION);
    bytes.extend_from_slice(&route::encode_request(request.receiver));
    encode_provenance(&mut bytes, request.provenance);
    put_u8(&mut bytes, request.shard_index);
    put_bytes(&mut bytes, request.bytes);
    Ok(EncodedRequest {
        digest: complete_stream_allocation::digest(&bytes),
        expected: WriteAck {
            stored_size: request.bytes.len() as u64,
            crc64: checksum::crc64::checksum(request.bytes),
        },
        bytes,
    })
}

pub(super) fn encode_provenance(bytes: &mut Vec<u8>, provenance: Provenance<'_>) {
    match provenance {
        Provenance::Direct {
            descriptor,
            logical_size,
        } => {
            put_u8(bytes, 0);
            put_u64(bytes, logical_size);
            put_bytes(bytes, descriptor);
        }
        Provenance::Stream { inspection } => {
            put_u8(bytes, 1);
            put_bytes(bytes, inspection);
        }
    }
}

fn read_bytes<'a>(
    input: &mut StorageRpcDecoder<'a>,
    limit: usize,
) -> Result<&'a [u8], StorageRpcPayloadError> {
    let len = input.read_u32()? as usize;
    bounded(len, limit)?;
    input.read_exact(len)
}

pub(super) fn decode_provenance<'a>(
    input: &mut StorageRpcDecoder<'a>,
) -> Result<Provenance<'a>, StorageRpcPayloadError> {
    Ok(match input.read_u8()? {
        0 => Provenance::Direct {
            logical_size: input.read_u64()?,
            descriptor: read_bytes(input, complete_window_primary::MAX_DESCRIPTOR_BYTES)?,
        },
        1 => Provenance::Stream {
            inspection: read_bytes(input, stream::MAX_REQUEST_BYTES)?,
        },
        _ => return Err(invalid()),
    })
}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<Request<'_>, StorageRpcPayloadError> {
    bounded(bytes.len(), MAX_REQUEST_BYTES)?;
    let mut input = StorageRpcDecoder::new(bytes);
    if input.read_exact(4)? != MAGIC || input.read_u16()? != VERSION {
        return Err(invalid());
    }
    let receiver = route::decode_request(input.read_exact(route::REQUEST_BYTES)?)?;
    let provenance = decode_provenance(&mut input)?;
    let shard_index = input.read_u8()?;
    let payload = read_bytes(&mut input, MAX_SHARD_BYTES)?;
    input.finish()?;
    let request = Request {
        receiver,
        provenance,
        shard_index,
        bytes: payload,
    };
    validate(request)?;
    Ok(request)
}

pub(crate) fn encode_response(
    request: &[u8],
    ack: WriteAck,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let decoded = decode_request(request)?;
    if ack.stored_size != decoded.bytes.len() as u64 {
        return Err(invalid());
    }
    let mut bytes = REPLY.to_vec();
    put_u16(&mut bytes, VERSION);
    bytes.extend_from_slice(&complete_stream_allocation::digest(request));
    put_u64(&mut bytes, ack.stored_size);
    put_u64(&mut bytes, ack.crc64);
    Ok(bytes)
}

impl EncodedRequest {
    pub(crate) fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn decode_response(&self, bytes: &[u8]) -> Result<WriteAck, StorageRpcPayloadError> {
        let mut input = StorageRpcDecoder::new(bytes);
        if input.read_exact(4)? != REPLY
            || input.read_u16()? != VERSION
            || input.read_exact(32)? != self.digest
        {
            return Err(invalid());
        }
        let ack = WriteAck {
            stored_size: input.read_u64()?,
            crc64: input.read_u64()?,
        };
        input.finish()?;
        if ack != self.expected {
            return Err(invalid());
        }
        Ok(ack)
    }
}

#[cfg(test)]
#[path = "complete_staging_file_tests.rs"]
mod tests;
