// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact tombstone observations, not abandonment acceptance or permission.
//! Reuse the private pending-bound identity and request decoder; no context or
//! installable command can be extracted from either probe or server query.

use super::*;

pub(in crate::node_runtime) struct CompleteAbandonedProbe<'a>(CompleteAppliedProbe<'a>);

pub(in crate::node_runtime) struct CompleteAbandonedEvidence<'a> {
    source: &'a CompletePendingEvidence,
    node_id: NodeId,
    abandoned: bool,
}

impl CompleteAbandonedEvidence<'_> {
    /// False is only lack of exact abandoned evidence at this peer. Neither
    /// value establishes global convergence or licenses a new durable effect.
    pub(in crate::node_runtime) fn is_abandoned(&self) -> bool {
        self.abandoned
    }
}

impl CompletePendingEvidence {
    pub(in crate::node_runtime) fn abandoned_probe(
        &self,
        node_id: NodeId,
        topology: &PgTopology,
    ) -> Result<CompleteAbandonedProbe<'_>, StorageRpcPayloadError> {
        self.applied_probe(node_id, topology)
            .map(CompleteAbandonedProbe)
    }
}

fn read(
    pg: &PgStore,
    node_id: NodeId,
    command: &EncodedContextualCommand<'_>,
) -> Result<bool, CompletePendingInspectionError> {
    if command.id().pg_id().get() != pg.pg_id {
        return Err(StoreError::RouteCapabilitySubjectMismatch {
            operation: "complete abandoned probe PG",
        }
        .into());
    }
    let snapshot = pg.command_build_snapshot()?;
    let identity = TerminalCommandIdentity::complete(command);
    let result = match pg.exact_terminal_metadata_command(node_id.as_u32(), &identity)? {
        None => false,
        Some((abandoned, previous, hash)) => {
            // A tombstone hashes its complete source digest, not the original
            // command's CRC alone. An exact applied record returns false only
            // after its complete identity and hash link have also been checked.
            let (checksum, _) = identity.record(abandoned);
            let id = command.id();
            if metadata_command_log_hash(
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
            abandoned
        }
    };
    snapshot.finish()?;
    Ok(result)
}

impl<'a> CompleteAbandonedProbe<'a> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.0.id()
    }

    pub(in crate::node_runtime) fn encode_request(
        &self,
    ) -> Result<Vec<u8>, StorageRpcPayloadError> {
        self.0.encode_request()
    }

    pub(in crate::node_runtime) fn read(
        &self,
        pg: &PgStore,
    ) -> Result<CompleteAbandonedEvidence<'a>, CompletePendingInspectionError> {
        Ok(self.evidence(read(pg, self.0.node_id, &self.0.command)?))
    }

    pub(in crate::node_runtime) fn decode_response(
        &self,
        bytes: &[u8],
    ) -> Result<CompleteAbandonedEvidence<'a>, StoreError> {
        let malformed = |reason: String| StoreError::StorageRpc {
            node_id: self.0.node_id.as_u32(),
            operation: "decode complete abandoned probe response",
            failure: rpc::StorageRpcErrorCode::PayloadDecode,
            detail: crate::StorageNodeFailureDetail::new(reason),
        };
        let response = rpc::decode_metadata_command_bool_outcome_response(bytes)
            .map_err(|error| malformed(error.to_string()))?;
        match response.outcome {
            rpc::StorageRpcMetadataCommandBoolOutcome::Value(value) => Ok(self.evidence(value)),
            rpc::StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            } => {
                let id = self.id();
                if node_id != self.0.node_id.as_u32()
                    || pg_id != id.pg_id().get()
                    || cluster_epoch != id.cluster_epoch()
                    || log_index != id.log_index().get()
                {
                    return Err(malformed(
                        "complete abandoned probe conflict subject".into(),
                    ));
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

    fn evidence(&self, abandoned: bool) -> CompleteAbandonedEvidence<'a> {
        CompleteAbandonedEvidence {
            source: self.0.source,
            node_id: self.0.node_id,
            abandoned,
        }
    }
}

pub(in crate::node_runtime) struct CompleteAbandonedQuery(CompleteAppliedQuery);

impl CompleteAbandonedQuery {
    pub(in crate::node_runtime) fn decode(
        bytes: &[u8],
        topology: &PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        CompleteAppliedQuery::decode(bytes, topology).map(Self)
    }
    pub(in crate::node_runtime) fn node_id(&self) -> NodeId {
        self.0.node_id()
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.0.id()
    }

    pub(in crate::node_runtime) fn read_response(
        &self,
        pg: &PgStore,
    ) -> Result<Vec<u8>, CompletePendingInspectionError> {
        let encoded = self
            .0
            .context
            .encode_command()
            .map_err(|_| invalid("encode complete abandoned probe context"))?;
        let outcome = match read(pg, self.node_id(), &encoded) {
            Ok(value) => rpc::StorageRpcMetadataCommandBoolOutcome::Value(value),
            Err(CompletePendingInspectionError::Store(
                StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id,
                    cluster_epoch,
                    log_index,
                },
            )) => rpc::StorageRpcMetadataCommandBoolOutcome::LogConflict {
                node_id,
                pg_id,
                cluster_epoch,
                log_index,
            },
            Err(error) => return Err(error),
        };
        Ok(rpc::encode_metadata_command_bool_outcome_response(
            &rpc::StorageRpcMetadataCommandBoolOutcomeResponse { outcome },
        ))
    }
}
