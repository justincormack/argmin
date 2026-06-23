use crate::control_plane::PgMetadataProof;
use crate::error::{BucketSnapshotLoadError, PgMetadataTransferError, StoreError};
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogHashRangeEntry,
    MetadataCommandLogIndex, MetadataCommandLogRangeEntry, MetadataCommandLogRangeEntryKind,
    MetadataCommandReplicaState, MetadataTransferCommand,
};
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
    pub(crate) from_log_hash: u64,
    pub(crate) to_log_index: u64,
    pub(crate) to_log_hash: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgPeeringReplicaReplayPlan {
    pub(crate) node_id: NodeId,
    pub(crate) commands: Vec<MetadataCommandEnvelope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgMetadataTransferArtifact {
    pub(crate) pg_id: PgId,
    pub(crate) source_node_id: NodeId,
    pub(crate) cluster_epoch: ClusterEpoch,
    pub(crate) proof: PgMetadataProof,
    pub(crate) retained_log_entries: Vec<MetadataCommandLogRangeEntry>,
}

impl PgMetadataTransferArtifact {
    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn source_node_id(&self) -> NodeId {
        self.source_node_id
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn source_metadata_proof(&self) -> PgMetadataProof {
        self.proof
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
    #[error("PG peering node {node_id:?} retained command-log entry {log_index} is abandoned and cannot be replayed from retained payloads")]
    UnreplayableAbandonedCommandLogEntry { node_id: NodeId, log_index: u64 },
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
            PgPeeringReconstructionFailure::Reconstruction(error) => Self::Reconstruction {
                message: error.to_string(),
            },
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
    from_log_hash: u64,
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
                expected_previous_log_hash,
                actual_previous_log_hash: retained.previous_log_hash,
            });
        }
        expected_previous_log_hash = retained.log_hash;
    }
    if expected_previous_log_hash != primary.state.applied_log_hash {
        return Err(PgPeeringReconstructionError::RetainedCommandLogFork {
            node_id: lagging_node_id,
            log_index: primary.state.applied_log_index,
            expected_previous_log_hash,
            actual_previous_log_hash: primary.state.applied_log_hash,
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
                    expected_previous_log_hash,
                    actual_previous_log_hash: retained.previous_log_hash,
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
                expected_previous_log_hash,
                actual_previous_log_hash: catchup.to_log_hash,
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

    let mut expected_previous_log_hash = 0;
    let mut expected_pre_state_digest = None;
    for log_index in 1..=state.applied_log_index {
        let retained = retained_log_entries
            .iter()
            .find(|entry| entry.log_index == log_index)
            .ok_or(
                PgPeeringReconstructionError::MissingRetainedCommandLogEntry {
                    node_id: source_node_id,
                    log_index,
                },
            )?;
        if retained.previous_log_hash != expected_previous_log_hash {
            return Err(PgPeeringReconstructionError::RetainedCommandLogFork {
                node_id: source_node_id,
                log_index,
                expected_previous_log_hash,
                actual_previous_log_hash: retained.previous_log_hash,
            });
        }
        if matches!(
            &retained.kind,
            MetadataCommandLogRangeEntryKind::Abandoned { .. }
        ) {
            return Err(
                PgPeeringReconstructionError::UnreplayableAbandonedCommandLogEntry {
                    node_id: source_node_id,
                    log_index,
                },
            );
        }
        let Some(pre_state_digest) = retained.pre_state_digest else {
            return Err(
                PgPeeringReconstructionError::MissingRetainedCommandStateProof {
                    node_id: source_node_id,
                    pg_id,
                    log_index,
                },
            );
        };
        let Some(post_state_digest) = retained.post_state_digest else {
            return Err(
                PgPeeringReconstructionError::MissingRetainedCommandStateProof {
                    node_id: source_node_id,
                    pg_id,
                    log_index,
                },
            );
        };
        let expected = expected_pre_state_digest.unwrap_or(pre_state_digest);
        if pre_state_digest != expected {
            return Err(
                PgPeeringReconstructionError::RetainedCommandStateDigestFork {
                    node_id: source_node_id,
                    pg_id,
                    log_index,
                    expected_pre_state_digest: expected,
                    actual_pre_state_digest: pre_state_digest,
                    post_state_digest,
                },
            );
        }
        expected_pre_state_digest = Some(post_state_digest);
        expected_previous_log_hash = retained.log_hash;
    }
    if expected_previous_log_hash != state.applied_log_hash {
        return Err(PgPeeringReconstructionError::RetainedCommandLogFork {
            node_id: source_node_id,
            log_index: state.applied_log_index,
            expected_previous_log_hash,
            actual_previous_log_hash: state.applied_log_hash,
        });
    }
    if let Some(final_state_digest) = expected_pre_state_digest {
        if final_state_digest != state.state_digest {
            return Err(
                PgPeeringReconstructionError::RetainedCommandStateDigestFork {
                    node_id: source_node_id,
                    pg_id,
                    log_index: state.applied_log_index,
                    expected_pre_state_digest: state.state_digest,
                    actual_pre_state_digest: final_state_digest,
                    post_state_digest: final_state_digest,
                },
            );
        }
    }

    Ok(PgMetadataTransferArtifact {
        pg_id,
        source_node_id,
        cluster_epoch,
        proof: proof_from_replica_state(state),
        retained_log_entries,
    })
}

#[allow(dead_code)]
pub(crate) fn rebase_pg_metadata_transfer_artifact_commands(
    artifact: &PgMetadataTransferArtifact,
    destination_cluster_epoch: ClusterEpoch,
) -> Result<Vec<MetadataTransferCommand>, PgPeeringReconstructionError> {
    let source_state = MetadataCommandReplicaState {
        cluster_epoch: artifact.cluster_epoch,
        applied_log_index: artifact.proof.applied_log_index,
        applied_log_hash: artifact.proof.applied_log_hash,
        state_digest: artifact.proof.state_digest,
    };
    build_pg_metadata_transfer_artifact_from_retained_log_entries(
        artifact.cluster_epoch,
        artifact.pg_id,
        artifact.source_node_id,
        source_state,
        false,
        artifact.retained_log_entries.clone(),
    )?;

    let mut commands = Vec::new();
    for log_index in 1..=artifact.proof.applied_log_index {
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
        let log_index = MetadataCommandLogIndex::new(log_index)
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
    PgMetadataProof::new(
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

    fn state(log_index: u64, log_hash: u64, state_digest: u64) -> MetadataCommandReplicaState {
        MetadataCommandReplicaState {
            cluster_epoch: ClusterEpoch::INITIAL,
            applied_log_index: log_index,
            applied_log_hash: log_hash,
            state_digest,
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
            applied_log_hash: log_hash,
            state_digest,
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
            previous_log_hash,
            log_hash,
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
                CreateBucketCommand::from_config(&config, 1, log_index).unwrap(),
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
            previous_log_hash,
            log_hash,
            pre_state_digest: None,
            post_state_digest: None,
            kind,
        }
    }

    #[test]
    fn peering_reconstruction_accepts_already_converged_replicas() {
        let proof = PgMetadataProof::new(2, 20, 200);
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
        let proof = PgMetadataProof::new(3, 30, 300);
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
                    from_log_hash: 10,
                    to_log_index: 3,
                    to_log_hash: 30,
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
                from_log_hash: 10,
                to_log_index: 3,
                to_log_hash: 30,
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
                from_log_hash: 10,
                to_log_index: 2,
                to_log_hash: 20,
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
                replica: PgMetadataProof::new(2, 21, 200),
                reference: PgMetadataProof::new(2, 20, 200),
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
                replica: PgMetadataProof::new(2, 20, 201),
                reference: PgMetadataProof::new(2, 20, 200),
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
