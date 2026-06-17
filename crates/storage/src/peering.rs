use crate::control_plane::PgMetadataProof;
use crate::metadata_command::MetadataCommandReplicaState;
use crate::types::{ClusterEpoch, PgId};
use placement::NodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetainedMetadataCommandLogHash {
    pub(crate) log_index: u64,
    pub(crate) previous_log_hash: u64,
    pub(crate) log_hash: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgPeeringReplicaReconstructionInput {
    pub(crate) node_id: NodeId,
    pub(crate) state: MetadataCommandReplicaState,
    pub(crate) has_pending_metadata_command: bool,
    pub(crate) retained_log_hashes: Vec<RetainedMetadataCommandLogHash>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgPeeringReplicaCatchUp {
    pub(crate) node_id: NodeId,
    pub(crate) from_log_index: u64,
    pub(crate) to_log_index: u64,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PgPeeringReconstructionError {
    PrimaryMissing {
        primary: NodeId,
    },
    StaleReplicaEpoch {
        node_id: NodeId,
        replica_epoch: ClusterEpoch,
        cluster_epoch: ClusterEpoch,
    },
    PendingMetadataCommand {
        node_id: NodeId,
    },
    ReplicaAheadOfPrimary {
        node_id: NodeId,
        replica_log_index: u64,
        primary_log_index: u64,
    },
    MetadataFork {
        node_id: NodeId,
        reference_node_id: NodeId,
        replica: PgMetadataProof,
        reference: PgMetadataProof,
    },
    MissingRetainedCommandLogEntry {
        node_id: NodeId,
        log_index: u64,
    },
    RetainedCommandLogFork {
        node_id: NodeId,
        log_index: u64,
        expected_previous_log_hash: u64,
        actual_previous_log_hash: u64,
    },
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
                    to_log_index: primary_replica.state.applied_log_index,
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

    fn state(log_index: u64, log_hash: u64, state_digest: u64) -> MetadataCommandReplicaState {
        MetadataCommandReplicaState {
            cluster_epoch: ClusterEpoch::INITIAL,
            applied_log_index: log_index,
            applied_log_hash: log_hash,
            state_digest,
        }
    }

    fn replica(
        node_id: u32,
        state: MetadataCommandReplicaState,
        retained_log_hashes: Vec<RetainedMetadataCommandLogHash>,
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
    ) -> RetainedMetadataCommandLogHash {
        RetainedMetadataCommandLogHash {
            log_index,
            previous_log_hash,
            log_hash,
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
                    to_log_index: 3,
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
