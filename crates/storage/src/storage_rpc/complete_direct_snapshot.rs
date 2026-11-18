// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only Direct PUT post-write snapshot framing. Neither a decoded request
//! nor a request-bound reply is authority to prepare/install a terminal command.
//! The admitted owner must check the descriptor, original held session, durable
//! acknowledgements and snapshot subject before retaining the snapshot in custody.
use super::complete_staging_ack as ack;
use super::*;

const MAGIC: &[u8; 4] = b"DSNP";
const REPLY: &[u8; 4] = b"DSNR";
const VERSION: u16 = 1;
// Outer framing + SART header/route + Direct tag/size/descriptor + receipt set.
// Do not admit the much larger stream-provenance alternative of the shared codec.
pub(crate) const MAX_REQUEST_BYTES: usize = 10
    + 6
    + complete_settled_prefix::REQUEST_BYTES
    + 1
    + 8
    + 4
    + complete_window_primary::MAX_DESCRIPTOR_BYTES
    + 2
    + ack::MAX_RECEIPTS * (1 + 8 + 8);
const RESPONSE_HEADER_BYTES: usize = 4 + 2 + 32 + 4;
// Preserve the ordinary snapshot's frame-sized transport budget, including the
// displaced multipart manifest. This is a framing bound, not a new PG-size cap.
const MAX_SNAPSHOT_BYTES: usize = STORAGE_RPC_MAX_PAYLOAD_LEN - 6 - RESPONSE_HEADER_BYTES;
pub(crate) const MAX_RESPONSE_BYTES: usize = RESPONSE_HEADER_BYTES + MAX_SNAPSHOT_BYTES;

pub(crate) struct EncodedRequest {
    bytes: Vec<u8>,
    digest: [u8; 32],
}

fn invalid() -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope("acknowledged direct payload snapshot")
}

fn bounded(len: usize, limit: usize) -> Result<(), StorageRpcPayloadError> {
    if len > limit {
        Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
    } else {
        Ok(())
    }
}

fn validate(request: &ack::Request<'_>) -> Result<(), StorageRpcPayloadError> {
    // Receipt ordering, framing and the nonzero absolute cutoff are shared with
    // acknowledgement transport. Completeness/geometry and original-primary
    // identity require the storage-owned descriptor and are not codec authority.
    match request.provenance {
        ack::Provenance::Direct { .. } => Ok(()),
        ack::Provenance::Stream { .. } => Err(invalid()),
    }
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

/// `snapshot` is the existing direct-commit snapshot grammar. Keep the carrier
/// borrowed/untrusted: the owning receiver/sender, not this framing layer,
/// is responsible for encoding/validating its storage facts under admission.
pub(crate) fn encode_response(
    request: &[u8],
    snapshot: &[u8],
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    decode_request(request)?;
    bounded(snapshot.len(), MAX_SNAPSHOT_BYTES)?;
    if snapshot.is_empty() {
        return Err(invalid());
    }
    let mut bytes = REPLY.to_vec();
    put_u16(&mut bytes, VERSION);
    bytes.extend_from_slice(&complete_stream_allocation::digest(request));
    put_bytes(&mut bytes, snapshot);
    Ok(bytes)
}

impl EncodedRequest {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn decode_response<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<&'a [u8], StorageRpcPayloadError> {
        bounded(bytes.len(), MAX_RESPONSE_BYTES)?;
        let mut input = StorageRpcDecoder::new(bytes);
        if input.read_exact(4)? != REPLY
            || input.read_u16()? != VERSION
            || input.read_exact(32)? != self.digest
        {
            return Err(invalid());
        }
        let len = input.read_u32()? as usize;
        bounded(len, MAX_SNAPSHOT_BYTES)?;
        if len == 0 {
            return Err(invalid());
        }
        let snapshot = input.read_exact(len)?;
        input.finish()?;
        Ok(snapshot)
    }
}

#[cfg(test)]
#[path = "complete_direct_snapshot_tests.rs"]
mod tests;
