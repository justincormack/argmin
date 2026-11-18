// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate terminal preparation framing, not an installation capability.
//! The receiver must rederive the snapshot under serialization and compare its
//! canonical digest before command construction. Provisional/committed material
//! is descriptive here; only the consuming sender can attest its custody.
use super::*;
use super::{complete_applied as command, complete_direct_snapshot as snapshot};

const MAGIC: &[u8; 4] = b"DTPQ";
const REPLY: &[u8; 4] = b"DTPR";
const VERSION: u16 = 1;
const MAX_INPUT_BYTES: usize = crate::metadata_command::MAX_METADATA_COMMAND_BYTES;
// Fixed framing, complete snapshot request, digest/CRC, and two independently
// bounded inputs. Request-only provenance must not consume the command budget.
pub(crate) const MAX_REQUEST_BYTES: usize =
    6 + 4 + snapshot::MAX_REQUEST_BYTES + 32 + 8 + 4 + MAX_INPUT_BYTES + 4 + MAX_INPUT_BYTES;
pub(crate) const MAX_RESPONSE_BYTES: usize = 38 + command::MAX_REQUEST_BYTES;
const _: () = assert!(MAX_REQUEST_BYTES < STORAGE_RPC_MAX_PAYLOAD_LEN);
const _: () = assert!(MAX_RESPONSE_BYTES + 6 <= STORAGE_RPC_MAX_PAYLOAD_LEN);

pub(crate) struct Request<'a> {
    pub(crate) snapshot_request: &'a [u8],
    pub(crate) snapshot_sha256: [u8; 32],
    pub(crate) segment_crc64: u64,
    pub(crate) provisional: ObjectEncryption,
    pub(crate) commit: StreamPutCommitInput,
}

pub(crate) struct EncodedRequest {
    bytes: Vec<u8>,
    digest: [u8; 32],
    primary: complete_settled_prefix::Request,
}

fn invalid() -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope("acknowledged direct terminal preparation")
}

fn bounded(len: usize, limit: usize) -> Result<(), StorageRpcPayloadError> {
    if len > limit {
        Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
    } else {
        Ok(())
    }
}

fn validate(
    request: &Request<'_>,
) -> Result<complete_settled_prefix::Request, StorageRpcPayloadError> {
    let original = snapshot::decode_request(request.snapshot_request)?;
    if request.provisional.key_binding() != crate::types::ObjectEncryptionKeyBinding::ProvisionalV1
        || request.commit.encryption.key_binding()
            != crate::types::ObjectEncryptionKeyBinding::ObjectSubjectV2
        || request.provisional.payload_codec() != request.commit.encryption.payload_codec()
        || ((request.commit.versioning == BucketVersioningState::Enabled)
            == request.commit.version_id.is_null())
    {
        return Err(invalid());
    }
    // Subject/material correspondence, witness/CRC equality and durable current
    // facts remain admitted-owner checks, not properties of decoded framing.
    Ok(original.receiver)
}

pub(crate) fn encode_request(
    request: &Request<'_>,
) -> Result<EncodedRequest, StorageRpcPayloadError> {
    let primary = validate(request)?;
    let mut provisional = Vec::new();
    put_object_encryption(&mut provisional, &request.provisional);
    bounded(provisional.len(), MAX_INPUT_BYTES)?;
    let mut commit = Vec::new();
    put_stream_put_commit_input(&mut commit, &request.commit);
    bounded(commit.len(), MAX_INPUT_BYTES)?;
    let mut bytes = MAGIC.to_vec();
    put_u16(&mut bytes, VERSION);
    put_bytes(&mut bytes, request.snapshot_request);
    bytes.extend_from_slice(&request.snapshot_sha256);
    put_u64(&mut bytes, request.segment_crc64);
    put_bytes(&mut bytes, &provisional);
    put_bytes(&mut bytes, &commit);
    bounded(bytes.len(), MAX_REQUEST_BYTES)?;
    Ok(EncodedRequest {
        digest: complete_stream_allocation::digest(&bytes),
        bytes,
        primary,
    })
}

fn read_input<'a>(
    input: &mut StorageRpcDecoder<'a>,
    limit: usize,
) -> Result<&'a [u8], StorageRpcPayloadError> {
    let len = input.read_u32()? as usize;
    bounded(len, limit)?;
    input.read_exact(len)
}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<Request<'_>, StorageRpcPayloadError> {
    bounded(bytes.len(), MAX_REQUEST_BYTES)?;
    let mut input = StorageRpcDecoder::new(bytes);
    if input.read_exact(4)? != MAGIC || input.read_u16()? != VERSION {
        return Err(invalid());
    }
    let snapshot_request = read_input(&mut input, snapshot::MAX_REQUEST_BYTES)?;
    let snapshot_sha256 = input.read_exact(32)?.try_into().map_err(|_| invalid())?;
    let segment_crc64 = input.read_u64()?;
    let provisional = read_input(&mut input, MAX_INPUT_BYTES)?;
    let commit = read_input(&mut input, MAX_INPUT_BYTES)?;
    input.finish()?;
    let mut input = StorageRpcDecoder::new(provisional);
    let provisional = input.read_object_encryption()?;
    input.finish()?;
    let mut input = StorageRpcDecoder::new(commit);
    let commit = input.read_stream_put_commit_input()?;
    input.finish()?;
    let request = Request {
        snapshot_request,
        snapshot_sha256,
        segment_crc64,
        provisional,
        commit,
    };
    validate(&request)?;
    Ok(request)
}

fn require_reply(
    primary: complete_settled_prefix::Request,
    reply: &command::BorrowedRequest<'_>,
) -> Result<(), StorageRpcPayloadError> {
    if primary.node != reply.node_id || primary.pg != reply.pg_id || primary.epoch != reply.epoch {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn encode_response(
    request: &[u8],
    prepared: command::BorrowedRequest<'_>,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    let decoded = decode_request(request)?;
    require_reply(validate(&decoded)?, &prepared)?;
    let body = command::encode(prepared)?;
    let mut bytes = REPLY.to_vec();
    put_u16(&mut bytes, VERSION);
    bytes.extend_from_slice(&complete_stream_allocation::digest(request));
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

impl EncodedRequest {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) fn decode_response<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<command::BorrowedRequest<'a>, StorageRpcPayloadError> {
        bounded(bytes.len(), MAX_RESPONSE_BYTES)?;
        let mut input = StorageRpcDecoder::new(bytes);
        if input.read_exact(4)? != REPLY
            || input.read_u16()? != VERSION
            || input.read_exact(32)? != self.digest
        {
            return Err(invalid());
        }
        let reply = command::decode(&bytes[38..])?;
        require_reply(self.primary, &reply)?;
        Ok(reply)
    }
}

#[cfg(test)]
#[path = "complete_direct_prepare_tests.rs"]
mod tests;
