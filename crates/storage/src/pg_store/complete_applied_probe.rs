// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact applied-record observations. Client probes borrow their pending owner;
//! decoded server queries cannot escape into submissions or recovery authority.

use super::*;
use crate::metadata_command::metadata_command_log_hash;
use crate::node_runtime::pg_store::payload_staging::EncodedContextualCommand;
use crate::storage_rpc::{self as rpc, complete_applied as framing};
use placement::NodeId;
pub(in crate::node_runtime) use publication::{
    CompleteAbandonmentQuery, CompleteAbandonmentRequest,
};
pub(in crate::node_runtime) use publication::{
    CompleteReplicaAbandonmentQuery, CompleteReplicaAbandonmentRequest,
};

#[path = "complete_abandoned_probe.rs"]
mod abandoned;
pub(in crate::node_runtime) use abandoned::{CompleteAbandonedEvidence, CompleteAbandonedQuery};

#[path = "complete_acceptance_probe.rs"]
mod acceptance;
#[path = "complete_publication_start.rs"]
mod publication;
#[cfg(test)]
pub(in crate::node_runtime) use acceptance::CompleteAcceptanceDisposition;
pub(in crate::node_runtime) use acceptance::{
    CompleteAcceptanceEvidence, CompleteAcceptanceKind, CompleteAcceptanceQuery,
};
pub(in crate::node_runtime) use publication::CompletePublicationError;
pub(in crate::node_runtime) use publication::CompletePublicationQuery;
pub(in crate::node_runtime) use publication::CompletePublicationRequest;
pub(in crate::node_runtime) use publication::CompletePublicationTransport;
pub(in crate::node_runtime) use publication::{CompleteRetirementQuery, CompleteRetirementRequest};

pub(in crate::node_runtime) struct CompleteAppliedProbe<'a> {
    source: &'a CompletePendingEvidence,
    command: EncodedContextualCommand<'a>,
    node_id: NodeId,
}

/// Absence means only that this peer has no retained exact applied evidence.
/// It never licenses abandonment, release, slot removal or physical deletion.
pub(in crate::node_runtime) struct CompleteAppliedEvidence<'a> {
    source: &'a CompletePendingEvidence,
    node_id: NodeId,
    hashes: Option<(u64, u64)>,
}

impl CompleteAppliedEvidence<'_> {
    pub(in crate::node_runtime) fn hashes(&self) -> Option<(u64, u64)> {
        self.hashes
    }
}

impl CompletePendingEvidence {
    pub(in crate::node_runtime) fn applied_probe(
        &self,
        node_id: NodeId,
        topology: &PgTopology,
    ) -> Result<CompleteAppliedProbe<'_>, StorageRpcPayloadError> {
        if topology != &self.topology {
            return Err(invalid("complete applied probe topology"));
        }
        let (context, _) = self
            .pending
            .as_ref()
            .ok_or_else(|| invalid("absent pending probe"))?;
        let command = context
            .encode_command()
            .map_err(|_| invalid("encode complete applied probe"))?;
        Ok(CompleteAppliedProbe {
            source: self,
            command,
            node_id,
        })
    }
}

fn validate_hashes(command: &EncodedContextualCommand<'_>, hashes: Option<(u64, u64)>) -> bool {
    hashes.is_none_or(|(previous, hash)| {
        let id = command.id();
        metadata_command_log_hash(
            id.cluster_epoch(),
            id.pg_id(),
            id.log_index(),
            previous,
            command.checksum_crc64(),
        )
        .value()
            == hash
    })
}

fn read(
    pg: &PgStore,
    node_id: NodeId,
    command: &EncodedContextualCommand<'_>,
) -> Result<Option<(u64, u64)>, CompletePendingInspectionError> {
    if command.id().pg_id().get() != pg.pg_id {
        return Err(StoreError::RouteCapabilitySubjectMismatch {
            operation: "complete applied probe PG",
        }
        .into());
    }
    let snapshot = pg.command_build_snapshot()?;
    let identity = TerminalCommandIdentity::complete(command);
    // One retained entry or compacted receipt, not a scan of the whole prefix.
    let hashes = pg.applied_metadata_command_identity_hashes(node_id.as_u32(), &identity)?;
    if !validate_hashes(command, hashes) {
        return Err(identity.conflict(node_id.as_u32(), pg.pg_id).into());
    }
    snapshot.finish()?;
    Ok(hashes)
}

impl<'a> CompleteAppliedProbe<'a> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.command.id()
    }

    pub(in crate::node_runtime) fn encode_request(
        &self,
    ) -> Result<Vec<u8>, StorageRpcPayloadError> {
        framing::encode(framing::BorrowedRequest {
            node_id: self.node_id,
            epoch: self.id().cluster_epoch(),
            pg_id: self.id().pg_id(),
            checksum: self.command.checksum_crc64(),
            command: self.command.bytes(),
        })
    }

    pub(in crate::node_runtime) fn read(
        &self,
        pg: &PgStore,
    ) -> Result<CompleteAppliedEvidence<'a>, CompletePendingInspectionError> {
        Ok(self.evidence(read(pg, self.node_id, &self.command)?))
    }

    pub(in crate::node_runtime) fn decode_response(
        &self,
        bytes: &[u8],
    ) -> Result<CompleteAppliedEvidence<'a>, StoreError> {
        let malformed = |reason: String| StoreError::StorageRpc {
            node_id: self.node_id.as_u32(),
            operation: "decode complete applied probe response",
            failure: rpc::StorageRpcErrorCode::PayloadDecode,
            detail: crate::StorageNodeFailureDetail::new(reason),
        };
        let response = rpc::decode_metadata_command_applied_hashes_response(bytes)
            .map_err(|e| malformed(e.to_string()))?;
        match response.outcome {
            rpc::StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(hashes) => {
                if !validate_hashes(&self.command, hashes) {
                    return Err(malformed("complete applied probe hash linkage".into()));
                }
                Ok(self.evidence(hashes))
            }
            rpc::StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            } => {
                let id = self.id();
                if node_id != self.node_id.as_u32()
                    || pg_id != id.pg_id().get()
                    || cluster_epoch != id.cluster_epoch()
                    || log_index != id.log_index().get()
                {
                    return Err(malformed("complete applied probe conflict subject".into()));
                }
                Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                })
            }
        }
    }

    fn evidence(&self, hashes: Option<(u64, u64)>) -> CompleteAppliedEvidence<'a> {
        CompleteAppliedEvidence {
            source: self.source,
            node_id: self.node_id,
            hashes,
        }
    }
}

pub(in crate::node_runtime) struct CompleteAppliedQuery {
    node_id: NodeId,
    context: ContextualCommand,
}

impl CompleteAppliedQuery {
    pub(in crate::node_runtime) fn decode(
        bytes: &[u8],
        topology: &PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        let request = framing::decode(bytes)?;
        let context =
            ContextualCommand::decode_command(request.command, request.checksum, topology)
                .map_err(|_| invalid("decode complete applied probe context"))?;
        if context.id().pg_id() != request.pg_id || context.id().cluster_epoch() != request.epoch {
            return Err(invalid("complete applied probe request subject"));
        }
        Ok(Self {
            node_id: request.node_id,
            context,
        })
    }
    pub(in crate::node_runtime) fn node_id(&self) -> NodeId {
        self.node_id
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.context.id()
    }

    pub(in crate::node_runtime) fn read_response(
        &self,
        pg: &PgStore,
    ) -> Result<Vec<u8>, CompletePendingInspectionError> {
        let encoded = self
            .context
            .encode_command()
            .map_err(|_| invalid("encode complete applied probe context"))?;
        let outcome = match read(pg, self.node_id, &encoded) {
            Ok(hashes) => rpc::StorageRpcMetadataCommandAppliedHashesOutcome::Hashes(hashes),
            Err(CompletePendingInspectionError::Store(
                StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                },
            )) => rpc::StorageRpcMetadataCommandAppliedHashesOutcome::LogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            },
            Err(error) => return Err(error),
        };
        Ok(rpc::encode_metadata_command_applied_hashes_response(
            &rpc::StorageRpcMetadataCommandAppliedHashesResponse { outcome },
        ))
    }
}
