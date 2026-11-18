// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Descriptive continuation of the exact prepared exchange. Only the held
//! receiver section can authorize installation; these bytes cannot recreate it.
use super::complete_direct_introduction as preparation;
use super::*;

const MAGIC: &[u8; 4] = b"DIIQ";
const REPLY: &[u8; 4] = b"DIIC";
const VERSION: u16 = 1;
pub(crate) const MAX_REQUEST_BYTES: usize =
    14 + preparation::MAX_REQUEST_BYTES + preparation::MAX_RESPONSE_BYTES;
pub(crate) const CONFIRMATION_BYTES: usize = 38;
const _: () = assert!(MAX_REQUEST_BYTES <= STORAGE_RPC_MAX_PAYLOAD_LEN);

pub(crate) struct Request<'a> {
    pub(crate) preparation: &'a [u8],
    pub(crate) response: &'a [u8],
}

fn invalid() -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope("direct introduction installation")
}

fn bounded(len: usize, limit: usize) -> Result<(), StorageRpcPayloadError> {
    if len > limit {
        Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
    } else {
        Ok(())
    }
}

fn validate(request: &Request<'_>) -> Result<(), StorageRpcPayloadError> {
    let logical = preparation::decode_request(request.preparation)?;
    let encoded = preparation::encode_request(&logical)?;
    if encoded.bytes() != request.preparation {
        return Err(invalid());
    }
    encoded.decode_response(request.response)?;
    Ok(())
}

pub(crate) fn encode_request(request: Request<'_>) -> Result<Vec<u8>, StorageRpcPayloadError> {
    validate(&request)?;
    let mut bytes = MAGIC.to_vec();
    put_u16(&mut bytes, VERSION);
    put_bytes(&mut bytes, request.preparation);
    put_bytes(&mut bytes, request.response);
    Ok(bytes)
}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<Request<'_>, StorageRpcPayloadError> {
    bounded(bytes.len(), MAX_REQUEST_BYTES)?;
    let mut input = StorageRpcDecoder::new(bytes);
    if input.read_exact(4)? != MAGIC || input.read_u16()? != VERSION {
        return Err(invalid());
    }
    let len = input.read_u32()? as usize;
    bounded(len, preparation::MAX_REQUEST_BYTES)?;
    let preparation = input.read_exact(len)?;
    let len = input.read_u32()? as usize;
    bounded(len, preparation::MAX_RESPONSE_BYTES)?;
    let response = input.read_exact(len)?;
    input.finish()?;
    let request = Request {
        preparation,
        response,
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
#[path = "complete_direct_introduction_install_tests.rs"]
mod tests;
