// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate framing only, never installation authority or absence evidence.
//! Preserve the original preparation (including its immutable cutoff) beside
//! the exact complete command. Confirmation authenticates both as one request.
use super::*;
use super::{complete_applied as command, complete_direct_prepare as preparation};

const MAGIC: &[u8; 4] = b"DTIQ";
const REPLY: &[u8; 4] = b"DTIC";
const VERSION: u16 = 1;
pub(crate) const MAX_REQUEST_BYTES: usize =
    10 + preparation::MAX_REQUEST_BYTES + command::MAX_REQUEST_BYTES;
pub(crate) const CONFIRMATION_BYTES: usize = 6 + 32;
const _: () = assert!(MAX_REQUEST_BYTES <= STORAGE_RPC_MAX_PAYLOAD_LEN);

pub(crate) struct Request<'a> {
    pub(crate) preparation: &'a [u8],
    pub(crate) command: command::BorrowedRequest<'a>,
}

fn invalid() -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope("acknowledged direct terminal installation")
}

fn validate(request: &Request<'_>) -> Result<(), StorageRpcPayloadError> {
    let prepared = preparation::decode_request(request.preparation)?;
    let route = complete_direct_snapshot::decode_request(prepared.snapshot_request)?.receiver;
    if route.node != request.command.node_id
        || route.pg != request.command.pg_id
        || route.epoch != request.command.epoch
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn encode_request(request: Request<'_>) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate(&request)?;
    let command = command::encode(request.command)?;
    let mut bytes = MAGIC.to_vec();
    put_u16(&mut bytes, VERSION);
    put_bytes(&mut bytes, request.preparation);
    bytes.extend_from_slice(&command);
    Ok(bytes)
}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<Request<'_>, StorageRpcPayloadError> {
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: bytes.len(),
            limit: MAX_REQUEST_BYTES,
        });
    }
    let mut input = StorageRpcDecoder::new(bytes);
    if input.read_exact(4)? != MAGIC || input.read_u16()? != VERSION {
        return Err(invalid());
    }
    let len = input.read_u32()? as usize;
    if len > preparation::MAX_REQUEST_BYTES {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len,
            limit: preparation::MAX_REQUEST_BYTES,
        });
    }
    let preparation = input.read_exact(len)?;
    let request = Request {
        preparation,
        command: command::decode(&bytes[10 + len..])?,
    };
    validate(&request)?;
    Ok(request)
}

pub(crate) fn confirmation(request: &[u8]) -> Result<Vec<u8>, StorageRpcPayloadError> {
    decode_request(request)?;
    let mut bytes = REPLY.to_vec();
    put_u16(&mut bytes, VERSION);
    bytes.extend_from_slice(&complete_stream_allocation::digest(request));
    Ok(bytes)
}

pub(crate) fn require_confirmation(
    request: &[u8],
    reply: &[u8],
) -> Result<(), StorageRpcPayloadError> {
    if reply.len() != CONFIRMATION_BYTES || reply != confirmation(request)? {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
#[path = "complete_direct_install_tests.rs"]
mod tests;
