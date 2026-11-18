// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Replica-position observations, not authorization to apply or abandon. The
//! source context and question remain attached to every observation. Durable
//! mutation must independently revalidate under admitted serialization.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::node_runtime) enum CompleteAcceptanceKind {
    Apply,
    Abandon,
}

impl CompleteAcceptanceKind {
    pub(in crate::node_runtime) fn message_kind(self) -> rpc::StorageRpcMessageKind {
        match self {
            Self::Apply => rpc::StorageRpcMessageKind::MetadataCommandAcceptance,
            Self::Abandon => rpc::StorageRpcMessageKind::MetadataCommandAbandonAcceptance,
        }
    }

    fn abandoned(self) -> bool {
        matches!(self, Self::Abandon)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::node_runtime) enum CompleteAcceptanceDisposition {
    NextPosition,
    ExactTerminal,
}

pub(in crate::node_runtime) struct CompleteAcceptanceProbe<'a> {
    identity: CompleteAppliedProbe<'a>,
    kind: CompleteAcceptanceKind,
}

pub(in crate::node_runtime) struct CompleteAcceptanceEvidence<'a> {
    source: &'a CompletePendingEvidence,
    node_id: NodeId,
    kind: CompleteAcceptanceKind,
    disposition: CompleteAcceptanceDisposition,
}

impl CompleteAcceptanceEvidence<'_> {
    pub(in crate::node_runtime) fn disposition(&self) -> CompleteAcceptanceDisposition {
        self.disposition
    }
    pub(in crate::node_runtime) fn kind(&self) -> CompleteAcceptanceKind {
        self.kind
    }
}

impl CompletePendingEvidence {
    pub(in crate::node_runtime) fn acceptance_probe(
        &self,
        node_id: NodeId,
        topology: &PgTopology,
        kind: CompleteAcceptanceKind,
    ) -> Result<CompleteAcceptanceProbe<'_>, StorageRpcPayloadError> {
        Ok(CompleteAcceptanceProbe {
            identity: self.applied_probe(node_id, topology)?,
            kind,
        })
    }
}

fn read(
    pg: &PgStore,
    node_id: NodeId,
    command: &EncodedContextualCommand<'_>,
    kind: CompleteAcceptanceKind,
) -> Result<MetadataCommandAcceptance, CompletePendingInspectionError> {
    if command.id().pg_id().get() != pg.pg_id {
        return Err(StoreError::RouteCapabilitySubjectMismatch {
            operation: "complete acceptance probe PG",
        }
        .into());
    }
    let snapshot = pg.command_build_snapshot()?;
    let identity = TerminalCommandIdentity::complete(command);
    let result = pg.metadata_command_identity_terminal_acceptance(
        node_id.as_u32(),
        &identity,
        kind.abandoned(),
    )?;
    if result == MetadataCommandAcceptance::AlreadyApplied {
        // Acceptance alone historically checks bytes/receipts. The complete
        // observation also requires its exact terminal hash link, but does not
        // claim to authenticate the whole historical prefix.
        let Some((abandoned, previous, hash)) =
            pg.exact_terminal_metadata_command(node_id.as_u32(), &identity)?
        else {
            return Err(identity.conflict(node_id.as_u32(), pg.pg_id).into());
        };
        let (checksum, _) = identity.record(abandoned);
        let id = command.id();
        if abandoned != kind.abandoned()
            || metadata_command_log_hash(
                id.cluster_epoch(),
                id.pg_id(),
                id.log_index(),
                previous,
                checksum,
            )
            .value()
                != hash
        {
            return Err(identity.conflict(node_id.as_u32(), pg.pg_id).into());
        }
    }
    snapshot.finish()?;
    Ok(result)
}

impl<'a> CompleteAcceptanceProbe<'a> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.identity.id()
    }
    pub(in crate::node_runtime) fn encode_request(
        &self,
    ) -> Result<Vec<u8>, StorageRpcPayloadError> {
        self.identity.encode_request()
    }
    pub(in crate::node_runtime) fn read(
        &self,
        pg: &PgStore,
    ) -> Result<CompleteAcceptanceEvidence<'a>, CompletePendingInspectionError> {
        Ok(self.evidence(read(
            pg,
            self.identity.node_id,
            &self.identity.command,
            self.kind,
        )?))
    }
    pub(in crate::node_runtime) fn decode_response(
        &self,
        bytes: &[u8],
    ) -> Result<CompleteAcceptanceEvidence<'a>, StoreError> {
        let malformed = |reason: String| StoreError::StorageRpc {
            node_id: self.identity.node_id.as_u32(),
            operation: "decode complete acceptance response",
            failure: rpc::StorageRpcErrorCode::PayloadDecode,
            detail: crate::StorageNodeFailureDetail::new(reason),
        };
        let response = rpc::decode_metadata_command_acceptance_response(bytes)
            .map_err(|error| malformed(error.to_string()))?;
        match response.outcome {
            rpc::StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(value) => {
                Ok(self.evidence(value))
            }
            rpc::StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            } => {
                let id = self.id();
                if node_id != self.identity.node_id.as_u32()
                    || pg_id != id.pg_id().get()
                    || cluster_epoch != id.cluster_epoch()
                    || log_index != id.log_index().get()
                {
                    return Err(malformed("complete acceptance conflict subject".into()));
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
    fn evidence(&self, value: MetadataCommandAcceptance) -> CompleteAcceptanceEvidence<'a> {
        CompleteAcceptanceEvidence {
            source: self.identity.source,
            node_id: self.identity.node_id,
            kind: self.kind,
            disposition: match value {
                MetadataCommandAcceptance::Apply => CompleteAcceptanceDisposition::NextPosition,
                MetadataCommandAcceptance::AlreadyApplied => {
                    CompleteAcceptanceDisposition::ExactTerminal
                }
            },
        }
    }
}

pub(in crate::node_runtime) struct CompleteAcceptanceQuery {
    identity: CompleteAppliedQuery,
    kind: CompleteAcceptanceKind,
}

impl CompleteAcceptanceQuery {
    pub(in crate::node_runtime) fn decode(
        bytes: &[u8],
        topology: &PgTopology,
        kind: CompleteAcceptanceKind,
    ) -> Result<Self, StorageRpcPayloadError> {
        Ok(Self {
            identity: CompleteAppliedQuery::decode(bytes, topology)?,
            kind,
        })
    }
    pub(in crate::node_runtime) fn node_id(&self) -> NodeId {
        self.identity.node_id()
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.identity.id()
    }
    pub(in crate::node_runtime) fn read_response(
        &self,
        pg: &PgStore,
    ) -> Result<Vec<u8>, CompletePendingInspectionError> {
        let encoded = self
            .identity
            .context
            .encode_command()
            .map_err(|_| invalid("encode complete acceptance context"))?;
        let outcome = match read(pg, self.node_id(), &encoded, self.kind) {
            Ok(value) => rpc::StorageRpcMetadataCommandAcceptanceOutcome::Acceptance(value),
            Err(CompletePendingInspectionError::Store(
                StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                },
            )) => rpc::StorageRpcMetadataCommandAcceptanceOutcome::LogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            },
            Err(error) => return Err(error),
        };
        Ok(rpc::encode_metadata_command_acceptance_response(
            &rpc::StorageRpcMetadataCommandAcceptanceResponse { outcome },
        ))
    }
}
