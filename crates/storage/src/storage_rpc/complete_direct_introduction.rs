// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate logical introduction framing. Decoded replies are descriptive bytes,
//! not installation authority. The receiver retains their exact identity in the
//! held primary section; the receiver-local owning installer checks that identity.
//! The test-selected preparation RPC retains it before replying; the distinct
//! installation RPC can only continue that retained state. Live selection is gated.
use super::*;
use super::{complete_applied as command, complete_settled_prefix as route};

const MAGIC: &[u8; 4] = b"DINQ";
const REPLY: &[u8; 4] = b"DINR";
const VERSION: u16 = 1;
pub(crate) const MAX_REQUEST_BYTES: usize = 6
    + route::REQUEST_BYTES
    + STORAGE_RPC_MAX_BUCKET_NAME_FIELD_LEN
    + 4
    + STORAGE_RPC_MAX_OBJECT_KEY_LEN
    + 4
    + SESSION_ID_LEN
    + STORAGE_RPC_BUCKET_WRITE_RECORD_MAX_LEN;
const RESPONSE_HEADER_BYTES: usize = 6 + 32 + 32 + 8;
pub(crate) const MAX_RESPONSE_BYTES: usize = RESPONSE_HEADER_BYTES + command::MAX_REQUEST_BYTES;
const _: () = assert!(MAX_RESPONSE_BYTES + 6 <= STORAGE_RPC_MAX_PAYLOAD_LEN);

pub(crate) struct Request {
    pub(crate) route: route::Request,
    pub(crate) bucket: BucketName,
    pub(crate) key: ObjectKey,
    pub(crate) attempt: SessionId,
    pub(crate) proof: BucketWriteReservationProof,
}

pub(crate) struct EncodedRequest {
    bytes: Vec<u8>,
    route: route::Request,
    digest: [u8; 32],
}

pub(crate) struct Response<'a> {
    pub(crate) command: command::BorrowedRequest<'a>,
    pub(crate) command_sha256: [u8; 32],
    pub(crate) cutoff: StorageRpcOperationDeadline,
}

fn invalid() -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope("direct introduction preparation")
}

fn bounded(len: usize, limit: usize) -> Result<(), StorageRpcPayloadError> {
    if len > limit {
        Err(StorageRpcPayloadError::PayloadTooLarge { len, limit })
    } else {
        Ok(())
    }
}

fn validate(request: &Request) -> Result<(), StorageRpcPayloadError> {
    let proof = &request.proof;
    // Source reservation schema limits, not the larger generic RPC string caps.
    if !proof.matches_exact_mutation_subject(
        request.route.epoch,
        &request.bucket,
        crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
        Some(request.key.as_str()),
    ) || !(1..=256).contains(&proof.reservation_id.len())
        || !(1..=256).contains(&proof.owner_token.len())
        || proof.reservation_id.starts_with('\0')
        || proof.owner_token.starts_with('\0')
        || proof.bucket_incarnation_generation == 0
        || proof.bucket_execution_generation < proof.bucket_incarnation_generation
        || proof.bucket_execution_generation > i64::MAX as u64
        || proof.cluster_epoch.get() > i64::MAX as u64
        || proof.lease_deadline <= proof.created_at
        || proof.lease_deadline > i64::MAX as u64
        || request.route.deadline.portable_wall_valid_until_ms == 0
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn encode_request(request: &Request) -> Result<EncodedRequest, StorageRpcPayloadError> {
    validate(request)?;
    let mut bytes = MAGIC.to_vec();
    put_u16(&mut bytes, VERSION);
    bytes.extend_from_slice(&route::encode_request(request.route));
    put_string(&mut bytes, request.bucket.as_str());
    put_string(&mut bytes, request.key.as_str());
    put_string(&mut bytes, request.attempt.as_str());
    put_bucket_write_reservation_proof(&mut bytes, &request.proof);
    bounded(bytes.len(), MAX_REQUEST_BYTES)?;
    Ok(EncodedRequest {
        digest: complete_stream_allocation::digest(&bytes),
        route: request.route,
        bytes,
    })
}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<Request, StorageRpcPayloadError> {
    bounded(bytes.len(), MAX_REQUEST_BYTES)?;
    let mut input = StorageRpcDecoder::new(bytes);
    if input.read_exact(4)? != MAGIC || input.read_u16()? != VERSION {
        return Err(invalid());
    }
    let request = Request {
        route: route::decode_request(input.read_exact(route::REQUEST_BYTES)?)?,
        bucket: input.read_bucket_name()?,
        key: input.read_object_key()?,
        attempt: input.read_session_id()?,
        proof: input.read_bucket_write_reservation_proof()?,
    };
    input.finish()?;
    validate(&request)?;
    Ok(request)
}

fn require_reply(
    primary: route::Request,
    reply: &command::BorrowedRequest<'_>,
    cutoff: StorageRpcOperationDeadline,
) -> Result<(), StorageRpcPayloadError> {
    if primary.node != reply.node_id
        || primary.pg != reply.pg_id
        || primary.epoch != reply.epoch
        || cutoff.portable_wall_valid_until_ms == 0
        || cutoff.portable_wall_valid_until_ms > primary.deadline.portable_wall_valid_until_ms
    {
        return Err(invalid());
    }
    Ok(())
}

pub(crate) fn encode_response(
    request: &EncodedRequest,
    prepared: command::BorrowedRequest<'_>,
    cutoff: StorageRpcOperationDeadline,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    require_reply(request.route, &prepared, cutoff)?;
    let digest = complete_stream_allocation::digest(prepared.command);
    let body = command::encode(prepared)?;
    let mut bytes = REPLY.to_vec();
    put_u16(&mut bytes, VERSION);
    bytes.extend_from_slice(&request.digest);
    bytes.extend_from_slice(&digest);
    put_u64(&mut bytes, cutoff.portable_wall_valid_until_ms);
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

impl EncodedRequest {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) fn digest(&self) -> [u8; 32] {
        self.digest
    }
    pub(crate) fn route(&self) -> route::Request {
        self.route
    }

    pub(crate) fn decode_response<'a>(
        &self,
        bytes: &'a [u8],
    ) -> Result<Response<'a>, StorageRpcPayloadError> {
        bounded(bytes.len(), MAX_RESPONSE_BYTES)?;
        let mut input = StorageRpcDecoder::new(bytes);
        if input.read_exact(4)? != REPLY
            || input.read_u16()? != VERSION
            || input.read_exact(32)? != self.digest
        {
            return Err(invalid());
        }
        let command_sha256 = input.read_exact(32)?.try_into().map_err(|_| invalid())?;
        let cutoff = StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: input.read_u64()?,
        };
        let command = command::decode(&bytes[RESPONSE_HEADER_BYTES..])?;
        require_reply(self.route, &command, cutoff)?;
        if command_sha256 != complete_stream_allocation::digest(command.command) {
            return Err(invalid());
        }
        Ok(Response {
            command,
            command_sha256,
            cutoff,
        })
    }
}

#[cfg(test)]
#[path = "complete_direct_introduction_tests.rs"]
mod tests;
