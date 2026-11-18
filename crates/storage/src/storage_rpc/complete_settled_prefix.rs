// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate witness inspection V1. The live replica-state grammar is unchanged;
//! only complete-command owner-test servers select this bounded read protocol.
//! These scalars are observations, never publication or physical authority.

use super::*;

const VERSION: u16 = 1;
pub(crate) const REQUEST_BYTES: usize = 2 + 8 + 4 + 8 + 4 + 32;
pub(crate) const RESPONSE_BYTES: usize = REQUEST_BYTES + 34;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Request {
    pub(crate) node: NodeId,
    pub(crate) epoch: ClusterEpoch,
    pub(crate) pg: PgId,
    pub(crate) deadline: StorageRpcOperationDeadline,
    pub(crate) topology: [u8; 32],
}

pub(crate) fn topology_digest(topology: &crate::PgTopology) -> [u8; 32] {
    let mut hash = checksum::ChecksumHasher::new(checksum::ChecksumAlgorithm::Sha256);
    hash.update(b"argmin.settled-witness-topology.v1");
    topology
        .for_each_pg::<std::convert::Infallible>(|pg| {
            hash.update(&pg.to_le_bytes());
            Ok(())
        })
        .expect("infallible topology iteration");
    hash.finalize().bytes().try_into().expect("SHA-256 length")
}

pub(crate) fn encode_request(request: Request) -> Vec<u8> {
    let mut out = Vec::with_capacity(REQUEST_BYTES);
    put_u16(&mut out, VERSION);
    put_u64(&mut out, request.deadline.portable_wall_valid_until_ms);
    put_u32(&mut out, request.node.as_u32());
    put_u64(&mut out, request.epoch.get());
    put_u32(&mut out, request.pg.get());
    out.extend_from_slice(&request.topology);
    out
}

pub(crate) fn decode_request(bytes: &[u8]) -> Result<Request, StorageRpcPayloadError> {
    if bytes.len() != REQUEST_BYTES {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "settled witness request size",
        ));
    }
    let mut decoder = StorageRpcDecoder::new(bytes);
    if decoder.read_u16()? != VERSION {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "settled witness request version",
        ));
    }
    let deadline = StorageRpcOperationDeadline {
        portable_wall_valid_until_ms: decoder.read_u64()?,
    };
    let node = NodeId::new(decoder.read_u32()?);
    let epoch = decoder.read_cluster_epoch()?;
    let pg = PgId::new(decoder.read_u32()?);
    let topology = decoder
        .read_exact(32)?
        .try_into()
        .expect("fixed digest length");
    decoder.finish()?;
    Ok(Request {
        node,
        epoch,
        pg,
        deadline,
        topology,
    })
}

pub(crate) fn encode_response(request: Request, state: MetadataCommandReplicaState) -> Vec<u8> {
    let mut out = encode_request(request);
    out.extend_from_slice(&encode_metadata_command_state_response(
        &StorageRpcMetadataCommandStateResponse { state },
    ));
    out
}

pub(crate) fn decode_response(
    bytes: &[u8],
    request: Request,
) -> Result<MetadataCommandReplicaState, StorageRpcPayloadError> {
    if bytes.len() != RESPONSE_BYTES || decode_request(&bytes[..REQUEST_BYTES])? != request {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "settled witness response subject/cutoff",
        ));
    }
    let state = decode_metadata_command_state_response(&bytes[REQUEST_BYTES..])?.state;
    if state.cluster_epoch != request.epoch {
        return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
            "settled witness response prefix epoch",
        ));
    }
    Ok(state)
}

#[cfg(test)]
#[path = "complete_settled_prefix_tests.rs"]
mod tests;
