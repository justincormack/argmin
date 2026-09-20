// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[cfg(test)]
use crate::control_plane::CanonicalStateDigest;
use crate::control_plane::{MetadataCommandLogHash, PgMetadataProof};
use crate::error::{BucketSnapshotLoadError, PgMetadataTransferError, StoreError};
use crate::metadata_command::{
    abandoned_command_log_bytes, metadata_command_log_hash, MetadataCommandEnvelope,
    MetadataCommandId, MetadataCommandLogHashRangeEntry, MetadataCommandLogIndex,
    MetadataCommandLogRangeEntry, MetadataCommandLogRangeEntryKind, MetadataCommandReplicaState,
    MetadataTransferCommand,
};
use crate::pg_store::MetadataCommandCheckpoint;
use crate::types::{ClusterEpoch, PgId, PgState};
use placement::NodeId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgPeeringReplicaReconstructionInput {
    pub(crate) node_id: NodeId,
    pub(crate) state: MetadataCommandReplicaState,
    pub(crate) has_pending_metadata_command: bool,
    pub(crate) retained_log_hashes: Vec<MetadataCommandLogHashRangeEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgPeeringReplicaCatchUp {
    pub(crate) node_id: NodeId,
    pub(crate) from_log_index: u64,
    pub(crate) from_log_hash: MetadataCommandLogHash,
    pub(crate) to_log_index: u64,
    pub(crate) to_log_hash: MetadataCommandLogHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgPeeringReplicaReplayPlan {
    pub(crate) node_id: NodeId,
    pub(crate) commands: Vec<MetadataCommandEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgMetadataTransferArtifact {
    pub(crate) pg_id: PgId,
    pub(crate) source_node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) base_kind: PgMetadataTransferBaseKind,
    pub(crate) base_proof: PgMetadataProof,
    pub(crate) checkpoint_base: Option<MetadataCommandCheckpoint>,
    pub(crate) proof: PgMetadataProof,
    pub(crate) retained_log_entries: Vec<MetadataCommandLogRangeEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PgMetadataTransferBaseKind {
    Empty,
    RetainedLogPrefix,
    Checkpoint,
}

impl PgMetadataTransferArtifact {
    #[must_use]
    #[cfg(test)]
    pub(crate) fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn source_node_id(&self) -> NodeId {
        self.source_node_id
    }

    #[must_use]
    pub(crate) fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub(crate) fn source_metadata_proof(&self) -> PgMetadataProof {
        self.proof
    }

    #[must_use]
    pub(crate) fn source_base_metadata_proof(&self) -> PgMetadataProof {
        self.base_proof
    }

    #[must_use]
    #[cfg(test)]
    pub(crate) fn source_base_kind(&self) -> PgMetadataTransferBaseKind {
        self.base_kind
    }

    #[must_use]
    pub(crate) fn checkpoint_base(&self) -> Option<&MetadataCommandCheckpoint> {
        self.checkpoint_base.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PgPeeringReconstructionDecision {
    AlreadyConverged {
        proof: PgMetadataProof,
    },
    CatchUpRequired {
        proof: PgMetadataProof,
        replicas: Vec<PgPeeringReplicaCatchUp>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PgPeeringReconstructionError {
    #[error("PG peering selected primary {primary:?} is missing from reconstruction input")]
    PrimaryMissing { primary: NodeId },
    #[error(
        "PG metadata transfer source {source_node:?} is not the current primary {primary:?} for PG {pg_id} in cluster epoch {cluster_epoch}"
    )]
    TransferSourceNotPrimary {
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        source_node: NodeId,
        primary: NodeId,
    },
    #[error(
        "PG metadata transfer source route for PG {pg_id} in cluster epoch {cluster_epoch} is {state}, expected Peering"
    )]
    TransferSourceNotQuiesced {
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },
    #[error(
        "PG metadata transfer source {node_id:?} retained command log entry {log_index} for PG {pg_id} is missing its post-state digest proof"
    )]
    MissingRetainedCommandStateProof {
        node_id: NodeId,
        pg_id: PgId,
        log_index: u64,
    },
    #[error(
        "PG metadata transfer source {node_id:?} retained command log entry {log_index} for PG {pg_id} has state digest fork: expected pre-state digest {expected_pre_state_digest:#018X}, actual pre-state digest {actual_pre_state_digest:#018X}, post-state digest {post_state_digest:#018X}"
    )]
    RetainedCommandStateDigestFork {
        node_id: NodeId,
        pg_id: PgId,
        log_index: u64,
        expected_pre_state_digest: u64,
        actual_pre_state_digest: u64,
        post_state_digest: u64,
    },
    #[error(
        "PG metadata transfer artifact for PG {pg_id} from source {node_id:?} is missing checkpoint base payload for proof {proof:?}"
    )]
    MissingMetadataTransferCheckpointBase {
        node_id: NodeId,
        pg_id: PgId,
        proof: PgMetadataProof,
    },
    #[error(
        "PG metadata transfer artifact for PG {pg_id} from source {node_id:?} has checkpoint proof {checkpoint:?}, expected {expected:?}"
    )]
    MetadataTransferCheckpointProofMismatch {
        node_id: NodeId,
        pg_id: PgId,
        checkpoint: PgMetadataProof,
        expected: PgMetadataProof,
    },
    #[error(
        "PG metadata transfer artifact for PG {pg_id} from source {node_id:?} carries a checkpoint payload with non-checkpoint base kind {base_kind:?}"
    )]
    UnexpectedMetadataTransferCheckpointBase {
        node_id: NodeId,
        pg_id: PgId,
        base_kind: PgMetadataTransferBaseKind,
    },
    #[error(
        "PG metadata transfer destination node {node_id:?} for PG {pg_id} in cluster epoch {cluster_epoch} is not empty and does not contain the expected imported proof {expected:?}: found log index {applied_log_index}, log hash {applied_log_hash:#018X}, digest {state_digest:#018X}"
    )]
    DirtyMetadataTransferDestination {
        node_id: NodeId,
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        applied_log_index: u64,
        applied_log_hash: u64,
        state_digest: u64,
        expected: PgMetadataProof,
    },
    #[error("PG peering replay target {node_id:?} is missing from acting-set clients")]
    ReplayTargetMissing { node_id: NodeId },
    #[error(
        "PG peering node {node_id:?} reported epoch {replica_epoch}, expected {cluster_epoch}"
    )]
    StaleReplicaEpoch {
        node_id: NodeId,
        replica_epoch: ClusterEpoch,
        cluster_epoch: ClusterEpoch,
    },
    #[error("PG peering node {node_id:?} still has a pending metadata command")]
    PendingMetadataCommand { node_id: NodeId },
    #[error(
        "PG peering node {node_id:?} is ahead of primary: replica log index {replica_log_index}, primary log index {primary_log_index}"
    )]
    ReplicaAheadOfPrimary {
        node_id: NodeId,
        replica_log_index: u64,
        primary_log_index: u64,
    },
    #[error(
        "PG peering metadata fork on node {node_id:?}; reference node {reference_node_id:?} has {reference:?}, replica has {replica:?}"
    )]
    MetadataFork {
        node_id: NodeId,
        reference_node_id: NodeId,
        replica: PgMetadataProof,
        reference: PgMetadataProof,
    },
    #[error("PG peering node {node_id:?} is missing retained command-log entry {log_index}")]
    MissingRetainedCommandLogEntry { node_id: NodeId, log_index: u64 },
    #[error(
        "PG peering node {node_id:?} retained command-log entry {log_index} forks: expected previous hash {expected_previous_log_hash:#018X}, actual {actual_previous_log_hash:#018X}"
    )]
    RetainedCommandLogFork {
        node_id: NodeId,
        log_index: u64,
        expected_previous_log_hash: u64,
        actual_previous_log_hash: u64,
    },
    #[error("PG peering node {node_id:?} retained command-log entry {log_index} for PG {pg_id} is invalid: {reason}")]
    InvalidRetainedCommandLogEntry {
        node_id: NodeId,
        pg_id: PgId,
        log_index: u64,
        reason: &'static str,
    },
    #[error("PG peering node {node_id:?} retained command-log entry {log_index} has log hash {actual_log_hash:#018X}, expected {expected_log_hash:#018X}")]
    RetainedCommandLogHashMismatch {
        node_id: NodeId,
        log_index: u64,
        expected_log_hash: u64,
        actual_log_hash: u64,
    },
    #[error("PG peering node {node_id:?} retained command-log chain ends at {actual:?}, expected {expected:?}")]
    RetainedCommandLogTipMismatch {
        node_id: NodeId,
        expected: PgMetadataProof,
        actual: PgMetadataProof,
    },
    #[error("PG peering node {node_id:?} retained command-log entry {log_index} is abandoned and cannot be replayed from retained payloads")]
    UnreplayableAbandonedCommandLogEntry { node_id: NodeId, log_index: u64 },
}

pub(crate) struct PgMetadataRetainedChainVerifier {
    node_id: NodeId,
    pg_id: PgId,
    log_epoch: ClusterEpoch,
    proof: PgMetadataProof,
}

impl PgMetadataRetainedChainVerifier {
    pub(crate) fn new(
        node_id: NodeId,
        pg_id: PgId,
        log_epoch: ClusterEpoch,
        certified_floor: PgMetadataProof,
    ) -> Self {
        Self {
            node_id,
            pg_id,
            log_epoch,
            proof: certified_floor,
        }
    }

    pub(crate) fn append_page(
        &mut self,
        entries: &[MetadataCommandLogRangeEntry],
    ) -> Result<(), PgPeeringReconstructionError> {
        let mut proof = self.proof;
        for entry in entries {
            let expected_index = proof.applied_log_index().checked_add(1).ok_or(
                PgPeeringReconstructionError::InvalidRetainedCommandLogEntry {
                    node_id: self.node_id,
                    pg_id: self.pg_id,
                    log_index: entry.log_index,
                    reason: "log index overflow",
                },
            )?;
            if entry.log_index != expected_index {
                return Err(
                    PgPeeringReconstructionError::MissingRetainedCommandLogEntry {
                        node_id: self.node_id,
                        log_index: expected_index,
                    },
                );
            }
            let log_index = MetadataCommandLogIndex::new(entry.log_index)
                .expect("successor of a metadata proof index is nonzero");
            let command_id = MetadataCommandId::new(self.log_epoch, self.pg_id, log_index);
            let invalid = |reason| PgPeeringReconstructionError::InvalidRetainedCommandLogEntry {
                node_id: self.node_id,
                pg_id: self.pg_id,
                log_index: entry.log_index,
                reason,
            };
            if entry.previous_log_hash != proof.applied_log_hash() {
                return Err(PgPeeringReconstructionError::RetainedCommandLogFork {
                    node_id: self.node_id,
                    log_index: entry.log_index,
                    expected_previous_log_hash: proof.applied_log_hash().value(),
                    actual_previous_log_hash: entry.previous_log_hash.value(),
                });
            }
            let pre_state_digest = entry.pre_state_digest.ok_or(
                PgPeeringReconstructionError::MissingRetainedCommandStateProof {
                    node_id: self.node_id,
                    pg_id: self.pg_id,
                    log_index: entry.log_index,
                },
            )?;
            let post_state_digest = entry.post_state_digest.ok_or(
                PgPeeringReconstructionError::MissingRetainedCommandStateProof {
                    node_id: self.node_id,
                    pg_id: self.pg_id,
                    log_index: entry.log_index,
                },
            )?;
            if pre_state_digest != proof.state_digest() {
                return Err(
                    PgPeeringReconstructionError::RetainedCommandStateDigestFork {
                        node_id: self.node_id,
                        pg_id: self.pg_id,
                        log_index: entry.log_index,
                        expected_pre_state_digest: proof.state_digest().value(),
                        actual_pre_state_digest: pre_state_digest.value(),
                        post_state_digest: post_state_digest.value(),
                    },
                );
            }
            let checksum = match &entry.kind {
                MetadataCommandLogRangeEntryKind::Applied(command) => {
                    if command.id() != command_id {
                        return Err(invalid("command identity does not match log row"));
                    }
                    let checksum = checksum::crc64::checksum(&command.command_bytes());
                    if checksum != command.checksum_crc64() {
                        return Err(invalid("command checksum does not match canonical bytes"));
                    }
                    checksum
                }
                MetadataCommandLogRangeEntryKind::Abandoned {
                    original_command_checksum,
                } => {
                    if post_state_digest != pre_state_digest {
                        return Err(invalid("abandonment changed the metadata digest"));
                    }
                    checksum::crc64::checksum(&abandoned_command_log_bytes(
                        command_id,
                        *original_command_checksum,
                    ))
                }
            };
            let expected_log_hash = metadata_command_log_hash(
                self.log_epoch,
                self.pg_id,
                log_index,
                entry.previous_log_hash.value(),
                checksum,
            );
            if entry.log_hash != expected_log_hash {
                return Err(
                    PgPeeringReconstructionError::RetainedCommandLogHashMismatch {
                        node_id: self.node_id,
                        log_index: entry.log_index,
                        expected_log_hash: expected_log_hash.value(),
                        actual_log_hash: entry.log_hash.value(),
                    },
                );
            }
            proof =
                PgMetadataProof::from_carriers(entry.log_index, entry.log_hash, post_state_digest);
        }
        self.proof = proof;
        Ok(())
    }

    pub(crate) fn finish(
        self,
        expected_tip: PgMetadataProof,
    ) -> Result<(), PgPeeringReconstructionError> {
        if self.proof.applied_log_index() == expected_tip.applied_log_index() {
            if self.proof.applied_log_hash() != expected_tip.applied_log_hash() {
                return Err(PgPeeringReconstructionError::RetainedCommandLogFork {
                    node_id: self.node_id,
                    log_index: expected_tip.applied_log_index(),
                    expected_previous_log_hash: self.proof.applied_log_hash().value(),
                    actual_previous_log_hash: expected_tip.applied_log_hash().value(),
                });
            }
            if self.proof.state_digest() != expected_tip.state_digest() {
                return Err(
                    PgPeeringReconstructionError::RetainedCommandStateDigestFork {
                        node_id: self.node_id,
                        pg_id: self.pg_id,
                        log_index: expected_tip.applied_log_index(),
                        expected_pre_state_digest: expected_tip.state_digest().value(),
                        actual_pre_state_digest: self.proof.state_digest().value(),
                        post_state_digest: self.proof.state_digest().value(),
                    },
                );
            }
        }
        if self.proof != expected_tip {
            return Err(
                PgPeeringReconstructionError::RetainedCommandLogTipMismatch {
                    node_id: self.node_id,
                    expected: expected_tip,
                    actual: self.proof,
                },
            );
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PgPeeringReconstructionFailure {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Apply(#[from] BucketSnapshotLoadError),
    #[error(transparent)]
    Reconstruction(#[from] PgPeeringReconstructionError),
}

impl From<PgPeeringReconstructionFailure> for PgMetadataTransferError {
    fn from(error: PgPeeringReconstructionFailure) -> Self {
        match error {
            PgPeeringReconstructionFailure::Store(error) => Self::Store(error),
            PgPeeringReconstructionFailure::Apply(error) => Self::Apply(error),
            PgPeeringReconstructionFailure::Reconstruction(error) => Self::reconstruction(error),
        }
    }
}

pub(crate) fn reconstruct_pg_peering_from_primary_retained_log(
    cluster_epoch: ClusterEpoch,
    _pg_id: PgId,
    primary: NodeId,
    replicas: &[PgPeeringReplicaReconstructionInput],
) -> Result<PgPeeringReconstructionDecision, PgPeeringReconstructionError> {
    let primary_replica = replicas
        .iter()
        .find(|replica| replica.node_id == primary)
        .ok_or(PgPeeringReconstructionError::PrimaryMissing { primary })?;
    validate_epoch_and_pending(primary_replica, cluster_epoch)?;

    let primary_proof = proof_from_replica_state(primary_replica.state);
    let mut catchups = Vec::new();
    for replica in replicas {
        validate_epoch_and_pending(replica, cluster_epoch)?;
        if replica.node_id == primary {
            continue;
        }
        match replica
            .state
            .applied_log_index
            .cmp(&primary_replica.state.applied_log_index)
        {
            std::cmp::Ordering::Equal => {
                let replica_proof = proof_from_replica_state(replica.state);
                if replica_proof != primary_proof {
                    return Err(PgPeeringReconstructionError::MetadataFork {
                        node_id: replica.node_id,
                        reference_node_id: primary,
                        replica: replica_proof,
                        reference: primary_proof,
                    });
                }
            }
            std::cmp::Ordering::Greater => {
                return Err(PgPeeringReconstructionError::ReplicaAheadOfPrimary {
                    node_id: replica.node_id,
                    replica_log_index: replica.state.applied_log_index,
                    primary_log_index: primary_replica.state.applied_log_index,
                });
            }
            std::cmp::Ordering::Less => {
                validate_retained_suffix_for_catchup(
                    primary_replica,
                    replica.node_id,
                    replica.state.applied_log_index,
                    replica.state.applied_log_hash,
                )?;
                catchups.push(PgPeeringReplicaCatchUp {
                    node_id: replica.node_id,
                    from_log_index: replica.state.applied_log_index,
                    from_log_hash: replica.state.applied_log_hash,
                    to_log_index: primary_replica.state.applied_log_index,
                    to_log_hash: primary_replica.state.applied_log_hash,
                });
            }
        }
    }

    if catchups.is_empty() {
        Ok(PgPeeringReconstructionDecision::AlreadyConverged {
            proof: primary_proof,
        })
    } else {
        Ok(PgPeeringReconstructionDecision::CatchUpRequired {
            proof: primary_proof,
            replicas: catchups,
        })
    }
}

fn validate_epoch_and_pending(
    replica: &PgPeeringReplicaReconstructionInput,
    cluster_epoch: ClusterEpoch,
) -> Result<(), PgPeeringReconstructionError> {
    if replica.state.cluster_epoch != cluster_epoch {
        return Err(PgPeeringReconstructionError::StaleReplicaEpoch {
            node_id: replica.node_id,
            replica_epoch: replica.state.cluster_epoch,
            cluster_epoch,
        });
    }
    if replica.has_pending_metadata_command {
        return Err(PgPeeringReconstructionError::PendingMetadataCommand {
            node_id: replica.node_id,
        });
    }
    Ok(())
}

fn validate_retained_suffix_for_catchup(
    primary: &PgPeeringReplicaReconstructionInput,
    lagging_node_id: NodeId,
    from_log_index: u64,
    from_log_hash: MetadataCommandLogHash,
) -> Result<(), PgPeeringReconstructionError> {
    let mut expected_previous_log_hash = from_log_hash;
    for log_index in (from_log_index + 1)..=primary.state.applied_log_index {
        let retained = primary
            .retained_log_hashes
            .iter()
            .find(|entry| entry.log_index == log_index)
            .ok_or(
                PgPeeringReconstructionError::MissingRetainedCommandLogEntry {
                    node_id: lagging_node_id,
                    log_index,
                },
            )?;
        if retained.previous_log_hash != expected_previous_log_hash {
            return Err(PgPeeringReconstructionError::RetainedCommandLogFork {
                node_id: lagging_node_id,
                log_index,
                expected_previous_log_hash: expected_previous_log_hash.value(),
                actual_previous_log_hash: retained.previous_log_hash.value(),
            });
        }
        expected_previous_log_hash = retained.log_hash;
    }
    if expected_previous_log_hash != primary.state.applied_log_hash {
        return Err(PgPeeringReconstructionError::RetainedCommandLogFork {
            node_id: lagging_node_id,
            log_index: primary.state.applied_log_index,
            expected_previous_log_hash: expected_previous_log_hash.value(),
            actual_previous_log_hash: primary.state.applied_log_hash.value(),
        });
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn build_pg_peering_replay_plan_from_retained_log_entries(
    catchups: &[PgPeeringReplicaCatchUp],
    retained_log_entries: &[MetadataCommandLogRangeEntry],
) -> Result<Vec<PgPeeringReplicaReplayPlan>, PgPeeringReconstructionError> {
    let mut plans = Vec::with_capacity(catchups.len());
    for catchup in catchups {
        let mut expected_previous_log_hash = catchup.from_log_hash;
        let mut commands = Vec::new();
        for log_index in (catchup.from_log_index + 1)..=catchup.to_log_index {
            let retained = retained_log_entries
                .iter()
                .find(|entry| entry.log_index == log_index)
                .ok_or(
                    PgPeeringReconstructionError::MissingRetainedCommandLogEntry {
                        node_id: catchup.node_id,
                        log_index,
                    },
                )?;
            if retained.previous_log_hash != expected_previous_log_hash {
                return Err(PgPeeringReconstructionError::RetainedCommandLogFork {
                    node_id: catchup.node_id,
                    log_index,
                    expected_previous_log_hash: expected_previous_log_hash.value(),
                    actual_previous_log_hash: retained.previous_log_hash.value(),
                });
            }
            match &retained.kind {
                MetadataCommandLogRangeEntryKind::Applied(command) => {
                    commands.push((**command).clone());
                }
                MetadataCommandLogRangeEntryKind::Abandoned { .. } => {
                    return Err(
                        PgPeeringReconstructionError::UnreplayableAbandonedCommandLogEntry {
                            node_id: catchup.node_id,
                            log_index,
                        },
                    );
                }
            }
            expected_previous_log_hash = retained.log_hash;
        }
        if expected_previous_log_hash != catchup.to_log_hash {
            return Err(PgPeeringReconstructionError::RetainedCommandLogFork {
                node_id: catchup.node_id,
                log_index: catchup.to_log_index,
                expected_previous_log_hash: expected_previous_log_hash.value(),
                actual_previous_log_hash: catchup.to_log_hash.value(),
            });
        }
        plans.push(PgPeeringReplicaReplayPlan {
            node_id: catchup.node_id,
            commands,
        });
    }
    Ok(plans)
}

#[allow(dead_code)]
pub(crate) fn build_pg_metadata_transfer_artifact_from_retained_log_entries(
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    source_node_id: NodeId,
    state: MetadataCommandReplicaState,
    has_pending_metadata_command: bool,
    retained_log_entries: Vec<MetadataCommandLogRangeEntry>,
) -> Result<PgMetadataTransferArtifact, PgPeeringReconstructionError> {
    let source = PgPeeringReplicaReconstructionInput {
        node_id: source_node_id,
        state,
        has_pending_metadata_command,
        retained_log_hashes: Vec::new(),
    };
    validate_epoch_and_pending(&source, cluster_epoch)?;

    let Some(first_retained_log_index) = retained_log_entries
        .iter()
        .map(|entry| entry.log_index)
        .min()
    else {
        if state.applied_log_index == 0 {
            return Ok(PgMetadataTransferArtifact {
                pg_id,
                source_node_id,
                cluster_epoch,
                base_kind: PgMetadataTransferBaseKind::Empty,
                base_proof: proof_from_replica_state(state),
                checkpoint_base: None,
                proof: proof_from_replica_state(state),
                retained_log_entries,
            });
        }
        return Err(
            PgPeeringReconstructionError::MissingRetainedCommandLogEntry {
                node_id: source_node_id,
                log_index: state.applied_log_index,
            },
        );
    };
    if first_retained_log_index == 0 || first_retained_log_index > state.applied_log_index {
        return Err(
            PgPeeringReconstructionError::MissingRetainedCommandLogEntry {
                node_id: source_node_id,
                log_index: state.applied_log_index,
            },
        );
    }

    let first_retained = retained_log_entries
        .iter()
        .find(|entry| entry.log_index == first_retained_log_index)
        .expect("minimum retained log index should select an entry");
    let Some(base_state_digest) = first_retained.pre_state_digest else {
        return Err(
            PgPeeringReconstructionError::MissingRetainedCommandStateProof {
                node_id: source_node_id,
                pg_id,
                log_index: first_retained_log_index,
            },
        );
    };
    let base_proof = PgMetadataProof::from_carriers(
        first_retained_log_index - 1,
        first_retained.previous_log_hash,
        base_state_digest,
    );
    let base_kind = if base_proof.applied_log_index == 0 && base_proof.applied_log_hash.value() == 0
    {
        PgMetadataTransferBaseKind::Empty
    } else {
        PgMetadataTransferBaseKind::RetainedLogPrefix
    };
    if let Some(abandoned) = retained_log_entries.iter().find(|entry| {
        matches!(
            &entry.kind,
            MetadataCommandLogRangeEntryKind::Abandoned { .. }
        )
    }) {
        return Err(
            PgPeeringReconstructionError::UnreplayableAbandonedCommandLogEntry {
                node_id: source_node_id,
                log_index: abandoned.log_index,
            },
        );
    }
    let mut verifier =
        PgMetadataRetainedChainVerifier::new(source_node_id, pg_id, cluster_epoch, base_proof);
    verifier.append_page(&retained_log_entries)?;
    verifier.finish(proof_from_replica_state(state))?;

    Ok(PgMetadataTransferArtifact {
        pg_id,
        source_node_id,
        cluster_epoch,
        base_kind,
        base_proof,
        checkpoint_base: None,
        proof: proof_from_replica_state(state),
        retained_log_entries,
    })
}

#[allow(dead_code)]
pub(crate) fn rebase_pg_metadata_transfer_artifact_commands(
    artifact: &PgMetadataTransferArtifact,
    destination_cluster_epoch: ClusterEpoch,
) -> Result<Vec<MetadataTransferCommand>, PgPeeringReconstructionError> {
    if artifact.base_kind == PgMetadataTransferBaseKind::Checkpoint {
        let Some(checkpoint) = artifact.checkpoint_base() else {
            return Err(
                PgPeeringReconstructionError::MissingMetadataTransferCheckpointBase {
                    node_id: artifact.source_node_id,
                    pg_id: artifact.pg_id,
                    proof: artifact.base_proof,
                },
            );
        };
        checkpoint.verify().map_err(|_| {
            PgPeeringReconstructionError::MetadataTransferCheckpointProofMismatch {
                node_id: artifact.source_node_id,
                pg_id: artifact.pg_id,
                checkpoint: PgMetadataProof::from_carriers(
                    checkpoint.applied_log_index,
                    checkpoint.applied_log_hash,
                    checkpoint.state_digest,
                ),
                expected: artifact.base_proof,
            }
        })?;
        let checkpoint_proof = PgMetadataProof::from_carriers(
            checkpoint.applied_log_index,
            checkpoint.applied_log_hash,
            checkpoint.state_digest,
        );
        if checkpoint.cluster_epoch != artifact.cluster_epoch
            || checkpoint.pg_id != artifact.pg_id
            || checkpoint_proof != artifact.base_proof
        {
            return Err(
                PgPeeringReconstructionError::MetadataTransferCheckpointProofMismatch {
                    node_id: artifact.source_node_id,
                    pg_id: artifact.pg_id,
                    checkpoint: checkpoint_proof,
                    expected: artifact.base_proof,
                },
            );
        }
        if artifact.retained_log_entries.is_empty() {
            if artifact.base_proof != artifact.proof {
                return Err(
                    PgPeeringReconstructionError::MetadataTransferCheckpointProofMismatch {
                        node_id: artifact.source_node_id,
                        pg_id: artifact.pg_id,
                        checkpoint: checkpoint_proof,
                        expected: artifact.proof,
                    },
                );
            }
            return Ok(Vec::new());
        }
        let source_state = MetadataCommandReplicaState {
            cluster_epoch: artifact.cluster_epoch,
            applied_log_index: artifact.proof.applied_log_index,
            applied_log_hash: artifact.proof.applied_log_hash,
            state_digest: artifact.proof.state_digest,
        };
        let validated_artifact = build_pg_metadata_transfer_artifact_from_retained_log_entries(
            artifact.cluster_epoch,
            artifact.pg_id,
            artifact.source_node_id,
            source_state,
            false,
            artifact.retained_log_entries.clone(),
        )?;
        if validated_artifact.base_proof != artifact.base_proof
            || validated_artifact.proof != artifact.proof
        {
            return Err(
                PgPeeringReconstructionError::RetainedCommandStateDigestFork {
                    node_id: artifact.source_node_id,
                    pg_id: artifact.pg_id,
                    log_index: artifact.base_proof.applied_log_index + 1,
                    expected_pre_state_digest: artifact.base_proof.state_digest.value(),
                    actual_pre_state_digest: validated_artifact.base_proof.state_digest.value(),
                    post_state_digest: artifact.proof.state_digest.value(),
                },
            );
        }
    } else {
        if artifact.checkpoint_base().is_some() {
            return Err(
                PgPeeringReconstructionError::UnexpectedMetadataTransferCheckpointBase {
                    node_id: artifact.source_node_id,
                    pg_id: artifact.pg_id,
                    base_kind: artifact.base_kind,
                },
            );
        }

        let source_state = MetadataCommandReplicaState {
            cluster_epoch: artifact.cluster_epoch,
            applied_log_index: artifact.proof.applied_log_index,
            applied_log_hash: artifact.proof.applied_log_hash,
            state_digest: artifact.proof.state_digest,
        };
        let validated_artifact = build_pg_metadata_transfer_artifact_from_retained_log_entries(
            artifact.cluster_epoch,
            artifact.pg_id,
            artifact.source_node_id,
            source_state,
            false,
            artifact.retained_log_entries.clone(),
        )?;
        if validated_artifact.base_proof != artifact.base_proof {
            return Err(
                PgPeeringReconstructionError::RetainedCommandStateDigestFork {
                    node_id: artifact.source_node_id,
                    pg_id: artifact.pg_id,
                    log_index: artifact.base_proof.applied_log_index + 1,
                    expected_pre_state_digest: artifact.base_proof.state_digest.value(),
                    actual_pre_state_digest: validated_artifact.base_proof.state_digest.value(),
                    post_state_digest: validated_artifact.base_proof.state_digest.value(),
                },
            );
        }
        if validated_artifact.base_kind != artifact.base_kind {
            return Err(
                PgPeeringReconstructionError::RetainedCommandStateDigestFork {
                    node_id: artifact.source_node_id,
                    pg_id: artifact.pg_id,
                    log_index: artifact.base_proof.applied_log_index + 1,
                    expected_pre_state_digest: validated_artifact.base_proof.state_digest.value(),
                    actual_pre_state_digest: artifact.base_proof.state_digest.value(),
                    post_state_digest: artifact.base_proof.state_digest.value(),
                },
            );
        }
    }

    let mut commands = Vec::new();
    for log_index in (artifact.base_proof.applied_log_index + 1)..=artifact.proof.applied_log_index
    {
        let retained = artifact
            .retained_log_entries
            .iter()
            .find(|entry| entry.log_index == log_index)
            .ok_or(
                PgPeeringReconstructionError::MissingRetainedCommandLogEntry {
                    node_id: artifact.source_node_id,
                    log_index,
                },
            )?;
        let MetadataCommandLogRangeEntryKind::Applied(command) = &retained.kind else {
            return Err(
                PgPeeringReconstructionError::UnreplayableAbandonedCommandLogEntry {
                    node_id: artifact.source_node_id,
                    log_index,
                },
            );
        };
        let Some(pre_state_digest) = retained.pre_state_digest else {
            return Err(
                PgPeeringReconstructionError::MissingRetainedCommandStateProof {
                    node_id: artifact.source_node_id,
                    pg_id: artifact.pg_id,
                    log_index,
                },
            );
        };
        let Some(post_state_digest) = retained.post_state_digest else {
            return Err(
                PgPeeringReconstructionError::MissingRetainedCommandStateProof {
                    node_id: artifact.source_node_id,
                    pg_id: artifact.pg_id,
                    log_index,
                },
            );
        };
        let destination_log_index = log_index
            .checked_sub(artifact.base_proof.applied_log_index)
            .expect("retained metadata transfer suffix starts after the base proof");
        let log_index = MetadataCommandLogIndex::new(destination_log_index)
            .expect("metadata transfer import log index is non-zero");
        commands.push(MetadataTransferCommand {
            command: MetadataCommandEnvelope::new(
                MetadataCommandId::new(destination_cluster_epoch, artifact.pg_id, log_index),
                command.payload().clone(),
            ),
            pre_state_digest,
            post_state_digest,
        });
    }
    Ok(commands)
}

fn proof_from_replica_state(state: MetadataCommandReplicaState) -> PgMetadataProof {
    PgMetadataProof::from_carriers(
        state.applied_log_index,
        state.applied_log_hash,
        state.state_digest,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_command::{
        CreateBucketCommand, MetadataCommandId, MetadataCommandLogIndex, MetadataCommandPayload,
    };
    use proptest::prelude::*;

    fn state(log_index: u64, log_hash: u64, state_digest: u64) -> MetadataCommandReplicaState {
        MetadataCommandReplicaState {
            cluster_epoch: ClusterEpoch::INITIAL,
            applied_log_index: log_index,
            applied_log_hash: MetadataCommandLogHash::for_test(log_hash),
            state_digest: CanonicalStateDigest::for_test(state_digest),
        }
    }

    fn state_at_epoch(
        cluster_epoch: ClusterEpoch,
        log_index: u64,
        log_hash: u64,
        state_digest: u64,
    ) -> MetadataCommandReplicaState {
        MetadataCommandReplicaState {
            cluster_epoch,
            applied_log_index: log_index,
            applied_log_hash: MetadataCommandLogHash::for_test(log_hash),
            state_digest: CanonicalStateDigest::for_test(state_digest),
        }
    }

    fn replica(
        node_id: u32,
        state: MetadataCommandReplicaState,
        retained_log_hashes: Vec<MetadataCommandLogHashRangeEntry>,
    ) -> PgPeeringReplicaReconstructionInput {
        PgPeeringReplicaReconstructionInput {
            node_id: NodeId::new(node_id),
            state,
            has_pending_metadata_command: false,
            retained_log_hashes,
        }
    }

    fn retained(
        log_index: u64,
        previous_log_hash: u64,
        log_hash: u64,
    ) -> MetadataCommandLogHashRangeEntry {
        MetadataCommandLogHashRangeEntry {
            log_index,
            previous_log_hash: MetadataCommandLogHash::for_test(previous_log_hash),
            log_hash: MetadataCommandLogHash::for_test(log_hash),
        }
    }

    fn command(log_index: u64) -> MetadataCommandEnvelope {
        let owner = crate::types::OwnerIdentity::from_principal("owner");
        let bucket =
            crate::types::BucketName::try_from(format!("peering-replay-{log_index}")).unwrap();
        let config = crate::types::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &s3_types::AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: s3_types::BucketVersioningState::Disabled,
            object_lock: s3_types::BucketObjectLockConfig::default(),
            ownership_controls: crate::types::BucketOwnershipControls {
                object_ownership: crate::types::BucketObjectOwnership::ObjectWriter,
            },
        };
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(7),
                MetadataCommandLogIndex::new(log_index).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config_with_fixed_upload_id_key_for_test(
                    &config, 1, log_index,
                )
                .unwrap(),
            ),
        )
    }

    fn retained_entry(
        log_index: u64,
        previous_log_hash: u64,
        log_hash: u64,
        kind: MetadataCommandLogRangeEntryKind,
    ) -> MetadataCommandLogRangeEntry {
        MetadataCommandLogRangeEntry {
            log_index,
            previous_log_hash: MetadataCommandLogHash::for_test(previous_log_hash),
            log_hash: MetadataCommandLogHash::for_test(log_hash),
            pre_state_digest: None,
            post_state_digest: None,
            kind,
        }
    }

    fn retained_transfer_entry(
        log_index: u64,
        previous_log_hash: u64,
        log_hash: u64,
        pre_state_digest: u64,
        post_state_digest: u64,
    ) -> MetadataCommandLogRangeEntry {
        MetadataCommandLogRangeEntry {
            log_index,
            previous_log_hash: MetadataCommandLogHash::for_test(previous_log_hash),
            log_hash: MetadataCommandLogHash::for_test(log_hash),
            pre_state_digest: Some(CanonicalStateDigest::for_test(pre_state_digest)),
            post_state_digest: Some(CanonicalStateDigest::for_test(post_state_digest)),
            kind: MetadataCommandLogRangeEntryKind::Applied(Box::new(command(log_index))),
        }
    }

    fn derived_transfer_value(seed: u64, ordinal: u64, salt: u64) -> u64 {
        seed.wrapping_add(ordinal.wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .rotate_left((ordinal as u32 % 31) + 1)
            ^ salt
    }

    fn different_transfer_value(value: u64) -> u64 {
        value ^ 0xA5A5_5A5A_D3C3_B4B4
    }

    fn retained_transfer_chain(
        first_log_index: u64,
        len: usize,
        base_log_hash: u64,
        base_state_digest: u64,
        seed: u64,
    ) -> (
        MetadataCommandReplicaState,
        Vec<MetadataCommandLogRangeEntry>,
    ) {
        let mut previous_log_hash = base_log_hash;
        let mut pre_state_digest = base_state_digest;
        let mut retained_entries = Vec::with_capacity(len);

        for offset in 0..len {
            let log_index = first_log_index + offset as u64;
            let log_hash = metadata_command_log_hash(
                ClusterEpoch::INITIAL,
                PgId::new(7),
                MetadataCommandLogIndex::new(log_index).unwrap(),
                previous_log_hash,
                command(log_index).checksum_crc64(),
            )
            .value();
            let post_state_digest = derived_transfer_value(seed, log_index, 0x0053_5441_5445);
            retained_entries.push(retained_transfer_entry(
                log_index,
                previous_log_hash,
                log_hash,
                pre_state_digest,
                post_state_digest,
            ));
            previous_log_hash = log_hash;
            pre_state_digest = post_state_digest;
        }

        (
            state_at_epoch(
                ClusterEpoch::INITIAL,
                first_log_index + len as u64 - 1,
                previous_log_hash,
                pre_state_digest,
            ),
            retained_entries,
        )
    }

    fn outage_chain_with_abandonment() -> (
        PgMetadataProof,
        Vec<MetadataCommandLogRangeEntry>,
        PgMetadataProof,
        PgMetadataProof,
    ) {
        let log_epoch = ClusterEpoch::new(16).unwrap();
        let pg_id = PgId::new(12);
        let floor = PgMetadataProof::from_carriers(
            10,
            MetadataCommandLogHash::for_test(3439484954540676867),
            CanonicalStateDigest::for_test(13956199312537336264),
        );
        let mut proof = floor;
        let mut rows = Vec::new();
        for index in 11..=14 {
            let id = MetadataCommandId::new(
                log_epoch,
                pg_id,
                MetadataCommandLogIndex::new(index).unwrap(),
            );
            let command = MetadataCommandEnvelope::new(id, command(index).payload().clone());
            let post_digest = CanonicalStateDigest::for_test(proof.state_digest().value() + index);
            let log_hash = metadata_command_log_hash(
                log_epoch,
                pg_id,
                id.log_index(),
                proof.applied_log_hash().value(),
                command.checksum_crc64(),
            );
            rows.push(MetadataCommandLogRangeEntry {
                log_index: index,
                previous_log_hash: proof.applied_log_hash(),
                log_hash,
                pre_state_digest: Some(proof.state_digest()),
                post_state_digest: Some(post_digest),
                kind: MetadataCommandLogRangeEntryKind::Applied(Box::new(command)),
            });
            proof = PgMetadataProof::from_carriers(index, log_hash, post_digest);
        }
        let pre_terminal = proof;
        let terminal_id =
            MetadataCommandId::new(log_epoch, pg_id, MetadataCommandLogIndex::new(15).unwrap());
        let original_checksum = 7650087655367533823;
        let tombstone_checksum =
            checksum::crc64::checksum(&abandoned_command_log_bytes(terminal_id, original_checksum));
        let terminal_hash = metadata_command_log_hash(
            log_epoch,
            pg_id,
            terminal_id.log_index(),
            proof.applied_log_hash().value(),
            tombstone_checksum,
        );
        rows.push(MetadataCommandLogRangeEntry {
            log_index: 15,
            previous_log_hash: proof.applied_log_hash(),
            log_hash: terminal_hash,
            pre_state_digest: Some(proof.state_digest()),
            post_state_digest: Some(proof.state_digest()),
            kind: MetadataCommandLogRangeEntryKind::Abandoned {
                original_command_checksum: original_checksum,
            },
        });
        (
            floor,
            rows,
            pre_terminal,
            PgMetadataProof::from_carriers(15, terminal_hash, proof.state_digest()),
        )
    }

    #[test]
    fn outage_chain_verifies_floor_gap_and_terminal_tombstone_across_pages() {
        let (floor, rows, pre_terminal, terminal) = outage_chain_with_abandonment();
        for node_id in [NodeId::new(1), NodeId::new(3)] {
            let mut verifier = PgMetadataRetainedChainVerifier::new(
                node_id,
                PgId::new(12),
                ClusterEpoch::new(16).unwrap(),
                floor,
            );
            verifier.append_page(&rows[..2]).unwrap();
            verifier.append_page(&rows[2..4]).unwrap();
            verifier.append_page(&rows[4..]).unwrap();
            verifier.finish(terminal).unwrap();
        }
        let mut verifier = PgMetadataRetainedChainVerifier::new(
            NodeId::new(3),
            PgId::new(12),
            ClusterEpoch::new(16).unwrap(),
            floor,
        );
        verifier.append_page(&rows[..4]).unwrap();
        verifier.finish(pre_terminal).unwrap();
    }

    #[test]
    fn outage_chain_rejects_missing_forked_and_misbound_evidence() {
        let (floor, rows, _, terminal) = outage_chain_with_abandonment();
        let verify = |entries: &[MetadataCommandLogRangeEntry]| {
            let mut verifier = PgMetadataRetainedChainVerifier::new(
                NodeId::new(3),
                PgId::new(12),
                ClusterEpoch::new(16).unwrap(),
                floor,
            );
            verifier.append_page(entries)?;
            verifier.finish(terminal)
        };
        assert!(matches!(
            verify(&rows[1..]),
            Err(PgPeeringReconstructionError::MissingRetainedCommandLogEntry { log_index: 11, .. })
        ));
        let mut forked = rows.clone();
        forked[2].previous_log_hash = floor.applied_log_hash();
        assert!(matches!(
            verify(&forked),
            Err(PgPeeringReconstructionError::RetainedCommandLogFork { log_index: 13, .. })
        ));
        let mut wrong_hash = rows.clone();
        wrong_hash[0].log_hash = floor.applied_log_hash();
        assert!(matches!(
            verify(&wrong_hash),
            Err(PgPeeringReconstructionError::RetainedCommandLogHashMismatch { log_index: 11, .. })
        ));
        let mut wrong_epoch = rows.clone();
        let MetadataCommandLogRangeEntryKind::Applied(command) = &rows[0].kind else {
            unreachable!()
        };
        wrong_epoch[0].kind =
            MetadataCommandLogRangeEntryKind::Applied(Box::new(MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::new(17).unwrap(),
                    PgId::new(12),
                    MetadataCommandLogIndex::new(11).unwrap(),
                ),
                command.payload().clone(),
            )));
        assert!(matches!(
            verify(&wrong_epoch),
            Err(PgPeeringReconstructionError::InvalidRetainedCommandLogEntry { log_index: 11, .. })
        ));
        let mut changed_tombstone = rows.clone();
        changed_tombstone[4].post_state_digest = Some(CanonicalStateDigest::for_test(1));
        assert!(matches!(
            verify(&changed_tombstone),
            Err(PgPeeringReconstructionError::InvalidRetainedCommandLogEntry { log_index: 15, .. })
        ));
        let mut wrong_tombstone_checksum = rows.clone();
        wrong_tombstone_checksum[4].kind = MetadataCommandLogRangeEntryKind::Abandoned {
            original_command_checksum: 1,
        };
        assert!(matches!(
            verify(&wrong_tombstone_checksum),
            Err(PgPeeringReconstructionError::RetainedCommandLogHashMismatch { log_index: 15, .. })
        ));
        assert!(matches!(
            verify(&rows[..4]),
            Err(PgPeeringReconstructionError::RetainedCommandLogTipMismatch { .. })
        ));
    }

    #[test]
    fn rejected_evidence_page_does_not_advance_the_verified_floor() {
        let (floor, rows, _, terminal) = outage_chain_with_abandonment();
        let mut verifier = PgMetadataRetainedChainVerifier::new(
            NodeId::new(3),
            PgId::new(12),
            ClusterEpoch::new(16).unwrap(),
            floor,
        );
        let mut corrupt_page = rows[..2].to_vec();
        corrupt_page[1].log_hash = floor.applied_log_hash();
        assert!(matches!(
            verifier.append_page(&corrupt_page),
            Err(PgPeeringReconstructionError::RetainedCommandLogHashMismatch { log_index: 12, .. })
        ));
        verifier.append_page(&rows[..2]).unwrap();
        verifier.append_page(&rows[2..]).unwrap();
        verifier.finish(terminal).unwrap();
    }

    #[derive(Clone, Copy, Debug)]
    enum RetainedTransferCorruption {
        MissingPreStateDigest,
        MissingPostStateDigest,
        PreStateDigestForkAfterBase,
        PreviousLogHashForkAfterBase,
        FinalStateDigestFork,
        FinalLogHashFork,
        MissingRetainedEntry,
        AbandonedRetainedEntry,
    }

    proptest! {
        #[test]
        fn prop_metadata_transfer_retained_log_artifact_preserves_valid_chain(
            first_log_index in 1_u64..6,
            len in 1_usize..8,
            base_log_hash in any::<u64>(),
            base_state_digest in any::<u64>(),
            seed in any::<u64>(),
        ) {
            let pg_id = PgId::new(7);
            let source_node_id = NodeId::new(1);
            let destination_epoch = ClusterEpoch::new(5).unwrap();
            let (source_state, retained_entries) = retained_transfer_chain(
                first_log_index,
                len,
                base_log_hash,
                base_state_digest,
                seed,
            );

            let artifact = build_pg_metadata_transfer_artifact_from_retained_log_entries(
                ClusterEpoch::INITIAL,
                pg_id,
                source_node_id,
                source_state,
                false,
                retained_entries.clone(),
            )
            .unwrap();

            prop_assert_eq!(artifact.pg_id, pg_id);
            prop_assert_eq!(artifact.source_node_id, source_node_id);
            prop_assert_eq!(artifact.cluster_epoch, ClusterEpoch::INITIAL);
            prop_assert_eq!(
                artifact.base_proof,
                PgMetadataProof::current(first_log_index - 1, retained_entries[0].previous_log_hash, retained_entries[0].pre_state_digest.unwrap())
            );
            prop_assert_eq!(artifact.proof, proof_from_replica_state(source_state));
            prop_assert_eq!(&artifact.retained_log_entries, &retained_entries);
            prop_assert_eq!(
                artifact.base_kind,
                if first_log_index == 1 && retained_entries[0].previous_log_hash == 0 {
                    PgMetadataTransferBaseKind::Empty
                } else {
                    PgMetadataTransferBaseKind::RetainedLogPrefix
                }
            );

            let rebased =
                rebase_pg_metadata_transfer_artifact_commands(&artifact, destination_epoch)
                    .unwrap();
            prop_assert_eq!(rebased.len(), len);
            for (rebased, retained) in rebased.iter().zip(&artifact.retained_log_entries) {
                prop_assert_eq!(rebased.command.id().cluster_epoch(), destination_epoch);
                prop_assert_eq!(rebased.command.id().pg_id(), pg_id);
                prop_assert_eq!(
                    rebased.command.id().log_index().get(),
                    retained.log_index - artifact.base_proof.applied_log_index
                );
                prop_assert_eq!(rebased.pre_state_digest, retained.pre_state_digest.unwrap());
                prop_assert_eq!(rebased.post_state_digest, retained.post_state_digest.unwrap());
            }
        }

        #[test]
        fn prop_metadata_transfer_retained_log_artifact_rejects_corrupt_chain(
            first_log_index in 1_u64..6,
            len in 1_usize..8,
            base_log_hash in any::<u64>(),
            base_state_digest in any::<u64>(),
            seed in any::<u64>(),
            target_offset in any::<usize>(),
            corruption in prop_oneof![
                Just(RetainedTransferCorruption::MissingPreStateDigest),
                Just(RetainedTransferCorruption::MissingPostStateDigest),
                Just(RetainedTransferCorruption::PreStateDigestForkAfterBase),
                Just(RetainedTransferCorruption::PreviousLogHashForkAfterBase),
                Just(RetainedTransferCorruption::FinalStateDigestFork),
                Just(RetainedTransferCorruption::FinalLogHashFork),
                Just(RetainedTransferCorruption::MissingRetainedEntry),
                Just(RetainedTransferCorruption::AbandonedRetainedEntry),
            ],
        ) {
            let pg_id = PgId::new(7);
            let source_node_id = NodeId::new(1);
            let (mut source_state, mut retained_entries) = retained_transfer_chain(
                first_log_index,
                len,
                base_log_hash,
                base_state_digest,
                seed,
            );
            let target = target_offset % retained_entries.len();

            match corruption {
                RetainedTransferCorruption::MissingPreStateDigest => {
                    retained_entries[target].pre_state_digest = None;
                }
                RetainedTransferCorruption::MissingPostStateDigest => {
                    retained_entries[target].post_state_digest = None;
                }
                RetainedTransferCorruption::PreStateDigestForkAfterBase => {
                    if retained_entries.len() == 1 {
                        source_state.state_digest = CanonicalStateDigest::for_test(
                            different_transfer_value(source_state.state_digest.value()),
                        );
                    } else {
                        let target = 1 + target_offset % (retained_entries.len() - 1);
                        let current = retained_entries[target].pre_state_digest.unwrap();
                        retained_entries[target].pre_state_digest = Some(
                            CanonicalStateDigest::for_test(different_transfer_value(
                                current.value(),
                            )),
                        );
                    }
                }
                RetainedTransferCorruption::PreviousLogHashForkAfterBase => {
                    if retained_entries.len() == 1 {
                        source_state.applied_log_hash = MetadataCommandLogHash::for_test(
                            different_transfer_value(source_state.applied_log_hash.value()),
                        );
                    } else {
                        let target = 1 + target_offset % (retained_entries.len() - 1);
                        retained_entries[target].previous_log_hash =
                            MetadataCommandLogHash::for_test(different_transfer_value(
                                retained_entries[target].previous_log_hash.value(),
                            ));
                    }
                }
                RetainedTransferCorruption::FinalStateDigestFork => {
                    source_state.state_digest = CanonicalStateDigest::for_test(
                        different_transfer_value(source_state.state_digest.value()),
                    );
                }
                RetainedTransferCorruption::FinalLogHashFork => {
                    source_state.applied_log_hash = MetadataCommandLogHash::for_test(
                        different_transfer_value(source_state.applied_log_hash.value()),
                    );
                }
                RetainedTransferCorruption::MissingRetainedEntry => {
                    if retained_entries.len() == 1 {
                        retained_entries.remove(target);
                    } else {
                        let target = 1 + target_offset % (retained_entries.len() - 1);
                        retained_entries.remove(target);
                    }
                }
                RetainedTransferCorruption::AbandonedRetainedEntry => {
                    retained_entries[target].kind =
                        MetadataCommandLogRangeEntryKind::Abandoned {
                            original_command_checksum: 0x1234,
                        };
                }
            }

            let result = build_pg_metadata_transfer_artifact_from_retained_log_entries(
                ClusterEpoch::INITIAL,
                pg_id,
                source_node_id,
                source_state,
                false,
                retained_entries,
            );
            prop_assert!(result.is_err());
        }
    }

    #[test]
    fn peering_reconstruction_accepts_already_converged_replicas() {
        let proof = PgMetadataProof::current(2, 20, 200);
        let decision = reconstruct_pg_peering_from_primary_retained_log(
            ClusterEpoch::INITIAL,
            PgId::new(7),
            NodeId::new(1),
            &[
                replica(1, state(2, 20, 200), Vec::new()),
                replica(2, state(2, 20, 200), Vec::new()),
            ],
        )
        .unwrap();
        assert_eq!(
            decision,
            PgPeeringReconstructionDecision::AlreadyConverged { proof }
        );
    }

    #[test]
    fn peering_reconstruction_requests_catchup_for_lagging_replica_with_retained_suffix() {
        let proof = PgMetadataProof::current(3, 30, 300);
        let decision = reconstruct_pg_peering_from_primary_retained_log(
            ClusterEpoch::INITIAL,
            PgId::new(7),
            NodeId::new(1),
            &[
                replica(
                    1,
                    state(3, 30, 300),
                    vec![retained(2, 10, 20), retained(3, 20, 30)],
                ),
                replica(2, state(1, 10, 100), Vec::new()),
            ],
        )
        .unwrap();
        assert_eq!(
            decision,
            PgPeeringReconstructionDecision::CatchUpRequired {
                proof,
                replicas: vec![PgPeeringReplicaCatchUp {
                    node_id: NodeId::new(2),
                    from_log_index: 1,
                    from_log_hash: MetadataCommandLogHash::for_test(10),
                    to_log_index: 3,
                    to_log_hash: MetadataCommandLogHash::for_test(30),
                }],
            }
        );
    }

    #[test]
    fn peering_reconstruction_fails_closed_when_retained_suffix_is_missing() {
        let err = reconstruct_pg_peering_from_primary_retained_log(
            ClusterEpoch::INITIAL,
            PgId::new(7),
            NodeId::new(1),
            &[
                replica(1, state(3, 30, 300), vec![retained(2, 10, 20)]),
                replica(2, state(1, 10, 100), Vec::new()),
            ],
        )
        .unwrap_err();
        assert_eq!(
            err,
            PgPeeringReconstructionError::MissingRetainedCommandLogEntry {
                node_id: NodeId::new(2),
                log_index: 3,
            }
        );
    }

    #[test]
    fn peering_reconstruction_fails_closed_on_retained_suffix_fork() {
        let err = reconstruct_pg_peering_from_primary_retained_log(
            ClusterEpoch::INITIAL,
            PgId::new(7),
            NodeId::new(1),
            &[
                replica(
                    1,
                    state(3, 30, 300),
                    vec![retained(2, 10, 20), retained(3, 99, 30)],
                ),
                replica(2, state(1, 10, 100), Vec::new()),
            ],
        )
        .unwrap_err();
        assert_eq!(
            err,
            PgPeeringReconstructionError::RetainedCommandLogFork {
                node_id: NodeId::new(2),
                log_index: 3,
                expected_previous_log_hash: 20,
                actual_previous_log_hash: 99,
            }
        );
    }

    #[test]
    fn peering_replay_plan_returns_applied_commands_for_lagging_replica() {
        let first = command(2);
        let second = command(3);
        let plans = build_pg_peering_replay_plan_from_retained_log_entries(
            &[PgPeeringReplicaCatchUp {
                node_id: NodeId::new(2),
                from_log_index: 1,
                from_log_hash: MetadataCommandLogHash::for_test(10),
                to_log_index: 3,
                to_log_hash: MetadataCommandLogHash::for_test(30),
            }],
            &[
                retained_entry(
                    2,
                    10,
                    20,
                    MetadataCommandLogRangeEntryKind::Applied(Box::new(first.clone())),
                ),
                retained_entry(
                    3,
                    20,
                    30,
                    MetadataCommandLogRangeEntryKind::Applied(Box::new(second.clone())),
                ),
            ],
        )
        .unwrap();

        assert_eq!(
            plans,
            vec![PgPeeringReplicaReplayPlan {
                node_id: NodeId::new(2),
                commands: vec![first, second],
            }]
        );
    }

    #[test]
    fn peering_replay_plan_fails_closed_on_abandoned_tombstone() {
        let err = build_pg_peering_replay_plan_from_retained_log_entries(
            &[PgPeeringReplicaCatchUp {
                node_id: NodeId::new(2),
                from_log_index: 1,
                from_log_hash: MetadataCommandLogHash::for_test(10),
                to_log_index: 2,
                to_log_hash: MetadataCommandLogHash::for_test(20),
            }],
            &[retained_entry(
                2,
                10,
                20,
                MetadataCommandLogRangeEntryKind::Abandoned {
                    original_command_checksum: 0x1234,
                },
            )],
        )
        .unwrap_err();

        assert_eq!(
            err,
            PgPeeringReconstructionError::UnreplayableAbandonedCommandLogEntry {
                node_id: NodeId::new(2),
                log_index: 2,
            }
        );
    }

    #[test]
    fn peering_reconstruction_fails_closed_on_same_index_hash_fork() {
        let err = reconstruct_pg_peering_from_primary_retained_log(
            ClusterEpoch::INITIAL,
            PgId::new(7),
            NodeId::new(1),
            &[
                replica(1, state(2, 20, 200), Vec::new()),
                replica(2, state(2, 21, 200), Vec::new()),
            ],
        )
        .unwrap_err();
        assert_eq!(
            err,
            PgPeeringReconstructionError::MetadataFork {
                node_id: NodeId::new(2),
                reference_node_id: NodeId::new(1),
                replica: PgMetadataProof::current(2, 21, 200),
                reference: PgMetadataProof::current(2, 20, 200),
            }
        );
    }

    #[test]
    fn peering_reconstruction_fails_closed_on_same_index_state_digest_fork() {
        let err = reconstruct_pg_peering_from_primary_retained_log(
            ClusterEpoch::INITIAL,
            PgId::new(7),
            NodeId::new(1),
            &[
                replica(1, state(2, 20, 200), Vec::new()),
                replica(2, state(2, 20, 201), Vec::new()),
            ],
        )
        .unwrap_err();
        assert_eq!(
            err,
            PgPeeringReconstructionError::MetadataFork {
                node_id: NodeId::new(2),
                reference_node_id: NodeId::new(1),
                replica: PgMetadataProof::current(2, 20, 201),
                reference: PgMetadataProof::current(2, 20, 200),
            }
        );
    }

    #[test]
    fn peering_reconstruction_fails_closed_when_replica_is_ahead_of_primary() {
        let err = reconstruct_pg_peering_from_primary_retained_log(
            ClusterEpoch::INITIAL,
            PgId::new(7),
            NodeId::new(1),
            &[
                replica(1, state(2, 20, 200), Vec::new()),
                replica(2, state(3, 30, 300), Vec::new()),
            ],
        )
        .unwrap_err();
        assert_eq!(
            err,
            PgPeeringReconstructionError::ReplicaAheadOfPrimary {
                node_id: NodeId::new(2),
                replica_log_index: 3,
                primary_log_index: 2,
            }
        );
    }

    #[test]
    fn peering_reconstruction_fails_closed_on_stale_replica_epoch() {
        let current_epoch = ClusterEpoch::new(2).unwrap();
        let err = reconstruct_pg_peering_from_primary_retained_log(
            current_epoch,
            PgId::new(7),
            NodeId::new(1),
            &[
                replica(1, state_at_epoch(current_epoch, 2, 20, 200), Vec::new()),
                replica(
                    2,
                    state_at_epoch(ClusterEpoch::INITIAL, 2, 20, 200),
                    Vec::new(),
                ),
            ],
        )
        .unwrap_err();
        assert_eq!(
            err,
            PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id: NodeId::new(2),
                replica_epoch: ClusterEpoch::INITIAL,
                cluster_epoch: current_epoch,
            }
        );
    }

    #[test]
    fn peering_reconstruction_fails_closed_on_pending_metadata_command() {
        let mut pending = replica(2, state(2, 20, 200), Vec::new());
        pending.has_pending_metadata_command = true;
        let err = reconstruct_pg_peering_from_primary_retained_log(
            ClusterEpoch::INITIAL,
            PgId::new(7),
            NodeId::new(1),
            &[replica(1, state(2, 20, 200), Vec::new()), pending],
        )
        .unwrap_err();
        assert_eq!(
            err,
            PgPeeringReconstructionError::PendingMetadataCommand {
                node_id: NodeId::new(2),
            }
        );
    }
}
