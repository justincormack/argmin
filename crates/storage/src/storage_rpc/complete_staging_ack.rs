// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate acknowledgement transport. Decoding neither confirms the
//! retained window nor authorizes SQL. Physical keys and the acknowledgement PG
//! are derived by the admitted receiver, never supplied by this carrier.
use super::*;
use super::{complete_settled_prefix as route, complete_staging_file as file};
pub(crate) use file::Provenance;

const MAGIC: &[u8; 4] = b"SART";
const REPLY: &[u8; 4] = b"SAOK";
const VERSION: u16 = 1;
// Frozen grammar bound, independently pinned by tests. Not the selected EC count.
pub(crate) const MAX_RECEIPTS: usize = 32;
const RECEIPT_BYTES: usize = 1 + 8 + 8;
pub(crate) const MAX_REQUEST_BYTES: usize =
    6 + route::REQUEST_BYTES + 1 + file::MAX_DESCRIPTION_BYTES + 2 + MAX_RECEIPTS * RECEIPT_BYTES;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Receipt {
    pub(crate) shard_index: u8,
    pub(crate) ack: WriteAck,
}

pub(crate) struct Request<'a> {
    /// Receiver node and OBJECT PG, epoch, topology and absolute cutoff.
    pub(crate) receiver: route::Request,
    pub(crate) provenance: Provenance<'a>,
    pub(crate) receipts: Vec<Receipt>,
}

pub(crate) struct EncodedRequest {
    bytes: Vec<u8>,
    digest: [u8; 32],
}

fn invalid() -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope("staging acknowledgement request/response")
}

fn bounded(len: usize, limit: usize) -> Result<(), StorageRpcPayloadError> {
    if len > limit {
        Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
    } else {
        Ok(())
    }
}

fn validate(request: &Request<'_>) -> Result<(), StorageRpcPayloadError> {
    file::validate_provenance(request.receiver, request.provenance)?;
    bounded(request.receipts.len(), MAX_RECEIPTS)?;
    let Some(first) = request.receipts.first() else {
        return Err(invalid());
    };
    for (index, receipt) in request.receipts.iter().enumerate() {
        if usize::from(receipt.shard_index) != index
            || receipt.ack.stored_size == 0
            || receipt.ack.stored_size > file::MAX_SHARD_BYTES as u64
            || receipt.ack.stored_size != first.ack.stored_size
        {
            return Err(invalid());
        }
    }
    // This establishes canonical framing, not completeness for the retained EC
    // shape or proof that the frontend actually owns these file receipts.
    Ok(())
}

pub(crate) fn encode_request(
    request: &Request<'_>,
) -> Result<EncodedRequest, StorageRpcPayloadError> {
    validate(request)?;
    let mut bytes = MAGIC.to_vec();
    put_u16(&mut bytes, VERSION);
    bytes.extend_from_slice(&route::encode_request(request.receiver));
    file::encode_provenance(&mut bytes, request.provenance);
    put_u16(&mut bytes, request.receipts.len() as u16);
    for receipt in &request.receipts {
        put_u8(&mut bytes, receipt.shard_index);
        put_u64(&mut bytes, receipt.ack.stored_size);
        put_u64(&mut bytes, receipt.ack.crc64);
    }
    Ok(EncodedRequest {
        digest: complete_stream_allocation::digest(&bytes),
        bytes,
    })
}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<Request<'_>, StorageRpcPayloadError> {
    bounded(bytes.len(), MAX_REQUEST_BYTES)?;
    let mut input = StorageRpcDecoder::new(bytes);
    if input.read_exact(4)? != MAGIC || input.read_u16()? != VERSION {
        return Err(invalid());
    }
    let receiver = route::decode_request(input.read_exact(route::REQUEST_BYTES)?)?;
    let provenance = file::decode_provenance(&mut input)?;
    let count = usize::from(input.read_u16()?);
    bounded(count, MAX_RECEIPTS)?;
    // Validate available framing before allocating even the bounded receipt set.
    let receipt_bytes = input.read_exact(count * RECEIPT_BYTES)?;
    input.finish()?;
    let mut input = StorageRpcDecoder::new(receipt_bytes);
    let mut receipts = Vec::with_capacity(count);
    for _ in 0..count {
        receipts.push(Receipt {
            shard_index: input.read_u8()?,
            ack: WriteAck {
                stored_size: input.read_u64()?,
                crc64: input.read_u64()?,
            },
        });
    }
    let request = Request {
        receiver,
        provenance,
        receipts,
    };
    validate(&request)?;
    Ok(request)
}

pub(crate) fn encode_response(request: &[u8]) -> Result<Vec<u8>, StorageRpcPayloadError> {
    decode_request(request)?;
    let mut bytes = REPLY.to_vec();
    put_u16(&mut bytes, VERSION);
    bytes.extend_from_slice(&complete_stream_allocation::digest(request));
    Ok(bytes)
}

impl EncodedRequest {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) fn decode_response(&self, bytes: &[u8]) -> Result<(), StorageRpcPayloadError> {
        let mut input = StorageRpcDecoder::new(bytes);
        if input.read_exact(4)? != REPLY
            || input.read_u16()? != VERSION
            || input.read_exact(32)? != self.digest
        {
            return Err(invalid());
        }
        input.finish()
    }
}

#[cfg(test)]
#[path = "complete_staging_ack_tests.rs"]
mod tests;
