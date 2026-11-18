// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bounded primary window inspection, not a grant or a complete-command decoder.
//! Shares the state-inspection opcode, but never the witness/raw-state grammar.

use super::complete_settled_prefix as witness;
use super::*;

pub(crate) const MAGIC: &[u8; 4] = b"WPRI";
const REPLY: &[u8; 4] = b"WPOK";
pub(crate) const DIRECT_FILE_MAGIC: &[u8; 4] = b"DPRI";

/// Both replies are read-only observations. Direct-file inspection additionally
/// checks the live issuer and fixed generation reservation; it is not a grant.
#[derive(Clone, Copy)]
pub(crate) enum Purpose {
    RetainedWindow,
    DirectFileBinding,
}

impl Purpose {
    fn request_magic(self) -> &'static [u8; 4] {
        match self {
            Self::RetainedWindow => MAGIC,
            Self::DirectFileBinding => DIRECT_FILE_MAGIC,
        }
    }
    fn response_magic(self) -> &'static [u8; 4] {
        match self {
            Self::RetainedWindow => REPLY,
            Self::DirectFileBinding => b"DPOK",
        }
    }
}
pub(crate) const MAX_DESCRIPTOR_BYTES: usize = 16 * 1024;
pub(crate) const MAX_REQUEST_BYTES: usize = 4 + witness::REQUEST_BYTES + 4 + MAX_DESCRIPTOR_BYTES;
const RESPONSE_BYTES: usize = 4 + witness::RESPONSE_BYTES + 32 + 1 + 4;

#[derive(Clone, Copy)]
pub(crate) struct Request<'a> {
    pub(crate) route: witness::Request,
    pub(crate) descriptor: &'a [u8],
}

fn invalid(reason: &'static str) -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope(reason)
}

pub(crate) fn encode_request(request: Request<'_>) -> Result<Vec<u8>, StorageRpcPayloadError> {
    encode_request_for(Purpose::RetainedWindow, request)
}

pub(crate) fn encode_request_for(
    purpose: Purpose,
    request: Request<'_>,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if request.descriptor.is_empty() || request.descriptor.len() > MAX_DESCRIPTOR_BYTES {
        return Err(invalid("primary window descriptor size"));
    }
    let mut bytes = purpose.request_magic().to_vec();
    bytes.extend_from_slice(&witness::encode_request(request.route));
    put_bytes(&mut bytes, request.descriptor);
    Ok(bytes)
}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<Request<'_>, StorageRpcPayloadError> {
    decode_request_for(Purpose::RetainedWindow, bytes)
}

pub(crate) fn decode_request_for(
    purpose: Purpose,
    bytes: &[u8],
) -> Result<Request<'_>, StorageRpcPayloadError> {
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: bytes.len(),
            limit: MAX_REQUEST_BYTES,
        });
    }
    let mut input = StorageRpcDecoder::new(bytes);
    if input.read_exact(4)? != purpose.request_magic() {
        return Err(invalid("primary window purpose"));
    }
    let route = witness::decode_request(input.read_exact(witness::REQUEST_BYTES)?)?;
    let len = input.read_u32()? as usize;
    if len == 0 || len > MAX_DESCRIPTOR_BYTES {
        return Err(invalid("primary window descriptor size"));
    }
    let descriptor = input.read_exact(len)?;
    input.finish()?;
    Ok(Request { route, descriptor })
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    checksum::compute_checksum(checksum::ChecksumAlgorithm::Sha256, bytes)
        .bytes()
        .try_into()
        .expect("SHA-256 length")
}

pub(crate) fn encode_response(
    request: Request<'_>,
    state: MetadataCommandReplicaState,
    peer: Option<NodeId>,
) -> Vec<u8> {
    encode_response_for(Purpose::RetainedWindow, request, state, peer)
}

pub(crate) fn encode_response_for(
    purpose: Purpose,
    request: Request<'_>,
    state: MetadataCommandReplicaState,
    peer: Option<NodeId>,
) -> Vec<u8> {
    let mut bytes = purpose.response_magic().to_vec();
    bytes.extend_from_slice(&witness::encode_response(request.route, state));
    bytes.extend_from_slice(&digest(request.descriptor));
    put_u8(&mut bytes, u8::from(peer.is_some()));
    put_u32(&mut bytes, peer.map_or(0, NodeId::as_u32));
    bytes
}

pub(crate) fn decode_response(
    bytes: &[u8],
    request: Request<'_>,
    peer: Option<NodeId>,
) -> Result<MetadataCommandReplicaState, StorageRpcPayloadError> {
    decode_response_for(Purpose::RetainedWindow, bytes, request, peer)
}

pub(crate) fn decode_response_for(
    purpose: Purpose,
    bytes: &[u8],
    request: Request<'_>,
    peer: Option<NodeId>,
) -> Result<MetadataCommandReplicaState, StorageRpcPayloadError> {
    if bytes.len() != RESPONSE_BYTES {
        return Err(invalid("primary window response size"));
    }
    let mut input = StorageRpcDecoder::new(bytes);
    if input.read_exact(4)? != purpose.response_magic() {
        return Err(invalid("primary window response purpose"));
    }
    let state =
        witness::decode_response(input.read_exact(witness::RESPONSE_BYTES)?, request.route)?;
    if input.read_exact(32)? != digest(request.descriptor)
        || input.read_u8()? != u8::from(peer.is_some())
        || input.read_u32()? != peer.map_or(0, NodeId::as_u32)
    {
        return Err(invalid("primary window response descriptor/witness"));
    }
    input.finish()?;
    Ok(state)
}

#[cfg(test)]
#[path = "complete_window_primary_tests.rs"]
mod tests;
