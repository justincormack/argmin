fn segment_payload_placement_key(segment_okh: &[u8; 16], segment_vid: GenerationId) -> [u8; 24] {
    let mut key = [0u8; 24];
    key[..16].copy_from_slice(segment_okh);
    key[16..].copy_from_slice(&segment_vid.get().to_be_bytes());
    key
}

fn validate_placed_segment_repair_ec_shape(ec: EcShape) -> Result<EcConfig, StoreError> {
    EcConfig::new(ec.k, ec.m).map_err(|error| StoreError::ErasureCoding {
        context: "inspect placed segment repair targets EC shape",
        reason: error.to_string(),
    })
}

fn build_placed_segment_shard_backfill_plan(
    source_health: PlacedSegmentShardSetHealth,
    desired_health: PlacedSegmentShardSetHealth,
) -> Result<PlacedSegmentShardBackfillPlan, StoreError> {
    if source_health.total_shards != desired_health.total_shards
        || source_health.required_shards != desired_health.required_shards
    {
        return Err(StoreError::PayloadShardSetMismatch {
            reason: format!(
                "source shard set {}/{} does not match desired shard set {}/{}",
                source_health.required_shards,
                source_health.total_shards,
                desired_health.required_shards,
                desired_health.total_shards
            ),
        });
    }

    let mut already_present = Vec::new();
    let mut copy_targets = Vec::new();
    let mut reconstruction_targets = Vec::new();
    let mut unrecoverable_targets = Vec::new();
    let source_recoverable =
        !matches!(source_health.risk, PlacedSegmentShardSetRisk::Unrecoverable);

    for desired in &desired_health.shards {
        if desired.validation.is_valid() {
            already_present.push(desired.shard_index);
            continue;
        }
        let source = source_health
            .shards
            .iter()
            .find(|source| source.shard_index == desired.shard_index)
            .ok_or_else(|| StoreError::PayloadShardSetMismatch {
                reason: format!(
                    "desired shard index {} has no source shard",
                    desired.shard_index.get()
                ),
            })?;
        if source.validation.is_valid() {
            copy_targets.push(PlacedSegmentShardBackfillCopyTarget {
                shard_index: desired.shard_index,
                shard_key: desired.shard_key.clone(),
                source: source.location,
                destination: desired.location,
            });
        } else if source_recoverable {
            reconstruction_targets.push(desired.shard_index);
        } else {
            unrecoverable_targets.push(desired.shard_index);
        }
    }

    Ok(PlacedSegmentShardBackfillPlan {
        source_health,
        desired_health,
        already_present,
        copy_targets,
        reconstruction_targets,
        unrecoverable_targets,
    })
}

fn note_shard_backfill_candidate_error(
    summary: &mut PlacedSegmentShardBackfillCandidateEnqueueSummary,
    error: &StoreError,
) {
    if shard_backfill_candidate_error_is_deferred(error) {
        summary.deferred += 1;
    } else {
        summary.failed += 1;
    }
}

fn note_metadata_command_checkpoint_record_error(
    pg_id: PgId,
    outcome: &'static str,
    error: &StoreError,
) {
    let error_kind = metadata_command_checkpoint_record_error_kind(error);
    let _ = observability::emit_metadata_command_checkpoint_record_error(
        TRACE_TARGET,
        observability::MetadataCommandCheckpointRecordErrorSummary {
            pg_id: pg_id.get(),
            outcome,
            error_kind,
        },
    );
}

fn compact_metadata_command_log_for_checkpoint_record(
    metadata_client: &dyn MetadataCommandNodeClient,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
    summary: &mut MetadataCommandCheckpointRecordSummary,
) {
    match metadata_client.compact_metadata_command_log(pg_id, cluster_epoch) {
        Ok(MetadataCommandLogCompactionStatus::NoCheckpoint { .. }) => {
            summary.compaction_no_checkpoint += 1;
        }
        Ok(MetadataCommandLogCompactionStatus::PendingCommand { .. }) => {
            summary.compaction_pending += 1;
        }
        Ok(MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries: 0, ..
        }) => {
            summary.compaction_noop += 1;
        }
        Ok(MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries, ..
        }) => {
            summary.compacted += 1;
            summary.compaction_deleted_entries = summary
                .compaction_deleted_entries
                .saturating_add(deleted_entries);
        }
        Err(error) => {
            if metadata_command_checkpoint_record_error_is_stale(&error) {
                summary.skipped_stale_epoch += 1;
                note_metadata_command_checkpoint_record_error(pg_id, "skipped_stale", &error);
            } else {
                note_metadata_command_checkpoint_record_error(pg_id, "compaction_failed", &error);
                summary.compaction_failed += 1;
            }
        }
    }
}

fn metadata_command_checkpoint_record_error_kind(error: &StoreError) -> &'static str {
    error.diagnostic_kind()
}

fn metadata_command_checkpoint_record_error_is_stale(error: &StoreError) -> bool {
    match error {
        StoreError::StalePayloadOperation { .. }
        | StoreError::StaleMetadataCommand { .. }
        | StoreError::StaleMetadataPrimaryBridge { .. }
        | StoreError::StaleMetadataOperation { .. }
        | StoreError::StaleMetadataRoute { .. }
        | StoreError::StaleMetadataReadProof { .. }
        | StoreError::RouteMapExpired { .. }
        | StoreError::StaleShardOperation { .. }
        | StoreError::StaleShardLocation { .. }
        | StoreError::PgNotActive { .. }
        | StoreError::MetadataCommandContention { .. } => true,
        StoreError::Io {
            context: "connect storage-node RPC socket",
            ..
        } => true,
        StoreError::StorageRpc { failure: code, .. } => {
            storage_rpc_code_is_retryable_pg_route_error(*code)
                // Background checkpoint scans can observe intermediate route-map states
                // while PG metadata transfer is moving between Peering and Active routes.
                // A later scan will retry from a refreshed map.
                || matches!(
                    *code,
                    StorageRpcErrorCode::UnknownPg
                        | StorageRpcErrorCode::MetadataTransferHistoricalRouteActive
                )
        }
        _ => false,
    }
}

#[cfg(test)]
mod metadata_command_checkpoint_record_error_tests {
    use super::*;

    #[test]
    fn metadata_checkpoint_record_treats_restart_and_contention_as_transient() {
        assert!(metadata_command_checkpoint_record_error_is_stale(
            &StoreError::Io {
                context: "connect storage-node RPC socket",
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "node socket missing"),
            },
        ));
        assert!(metadata_command_checkpoint_record_error_is_stale(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "metadata command checkpoint record current",
                failure: StorageRpcErrorCode::MetadataCommandContention,
                detail: crate::StorageNodeFailureDetail::new(
                    "metadata command contention during export metadata command checkpoint",
                ),
            },
        ));
        assert!(metadata_command_checkpoint_record_error_is_stale(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "metadata command checkpoint record current",
                failure: StorageRpcErrorCode::UnknownPg,
                detail: crate::StorageNodeFailureDetail::new(
                    "PG 7 is not configured on this storage node",
                ),
            },
        ));
        assert!(metadata_command_checkpoint_record_error_is_stale(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "metadata command replica state",
                failure: StorageRpcErrorCode::MetadataTransferHistoricalRouteActive,
                detail: crate::StorageNodeFailureDetail::new("historical peering inspection for PG 7 at epoch 42 requires Peering route, got active"),
            },
        ));
        assert!(metadata_command_checkpoint_record_error_is_stale(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "metadata command checkpoint record current",
                failure: StorageRpcErrorCode::TransportClosed,
                detail: crate::StorageNodeFailureDetail::new(
                    "storage RPC stream I/O error: early eof",
                ),
            },
        ));
        assert!(!metadata_command_checkpoint_record_error_is_stale(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "metadata command checkpoint record current",
                failure: StorageRpcErrorCode::Internal,
                detail: crate::StorageNodeFailureDetail::new("metadata state digest mismatch"),
            },
        ));
    }

    #[test]
    fn metadata_checkpoint_record_error_kind_labels_checkpoint_integrity_failures() {
        let epoch = ClusterEpoch::new(3).unwrap();
        assert_eq!(
            metadata_command_checkpoint_record_error_kind(
                &StoreError::MetadataCommandLogHashMismatch {
                    node_id: 0,
                    pg_id: 7,
                    cluster_epoch: epoch,
                    log_index: 9,
                    expected_previous_log_hash: 11,
                    actual_previous_log_hash: 12,
                    expected_log_hash: 13,
                    actual_log_hash: 14,
                }
            ),
            "metadata_command_log_hash_mismatch"
        );
        assert_eq!(
            metadata_command_checkpoint_record_error_kind(
                &StoreError::MetadataCommandReplicaStateDiverged {
                    node_id: 0,
                    reference_node_id: 1,
                    pg_id: 7,
                    cluster_epoch: epoch,
                    reference_cluster_epoch: epoch,
                    applied_log_index: 9,
                    reference_applied_log_index: 8,
                    applied_log_hash: 10,
                    reference_applied_log_hash: 11,
                    state_digest: 12,
                    reference_state_digest: 13,
                }
            ),
            "metadata_command_replica_state_diverged"
        );
        assert_eq!(
            metadata_command_checkpoint_record_error_kind(
                &StoreError::MetadataStateDigestMismatch {
                    node_id: 0,
                    pg_id: 7,
                    cluster_epoch: epoch,
                    expected_digest: 12,
                    actual_digest: 13,
                }
            ),
            "metadata_state_digest_mismatch"
        );
        assert_eq!(
            metadata_command_checkpoint_record_error_kind(&StoreError::MetadataCheckpointInvalid {
                node_id: 0,
                pg_id: 7,
                cluster_epoch: epoch,
                reason: "frame hash mismatch".to_string(),
            }),
            "metadata_checkpoint_invalid"
        );
        assert_eq!(
            metadata_command_checkpoint_record_error_kind(
                &StoreError::MetadataTransferUnsupportedProof {
                    node_id: 0,
                    pg_id: 7,
                    cluster_epoch: epoch,
                    applied_log_index: 9,
                    applied_log_hash: 10,
                }
            ),
            "metadata_transfer_unsupported_proof"
        );
    }

    #[test]
    fn shard_backfill_candidate_treats_restart_connect_failure_as_deferred() {
        assert!(shard_backfill_candidate_error_is_deferred(
            &StoreError::Io {
                context: "connect storage-node RPC socket",
                source: std::io::Error::new(std::io::ErrorKind::NotFound, "node socket missing"),
            },
        ));
        assert!(shard_backfill_candidate_error_is_deferred(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "list shard scavenger payload references",
                failure: StorageRpcErrorCode::TransportClosed,
                detail: crate::StorageNodeFailureDetail::new(
                    "storage RPC stream I/O error: early eof",
                ),
            },
        ));
        assert!(!shard_backfill_candidate_error_is_deferred(
            &StoreError::StorageRpc {
                node_id: 0,
                operation: "list shard scavenger payload references",
                failure: StorageRpcErrorCode::PayloadDecode,
                detail: crate::StorageNodeFailureDetail::new("storage RPC frame checksum mismatch",),
            },
        ));
    }
}

fn metadata_command_checkpoint_record_decision(
    state: &MetadataCommandReplicaState,
    latest_checkpoint: Option<&MetadataCommandCheckpoint>,
    min_log_distance: u64,
    frame_risk_bytes: usize,
) -> Result<MetadataCommandCheckpointRecordDecision, StoreError> {
    let Some(checkpoint) = latest_checkpoint else {
        return Ok(MetadataCommandCheckpointRecordDecision::Record);
    };
    if checkpoint.cluster_epoch == state.cluster_epoch
        && checkpoint.applied_log_index == state.applied_log_index
        && checkpoint.applied_log_hash == state.applied_log_hash
        && checkpoint.state_digest == state.state_digest
    {
        return Ok(MetadataCommandCheckpointRecordDecision::AlreadyCurrent);
    }

    let log_distance = state
        .applied_log_index
        .saturating_sub(checkpoint.applied_log_index);
    if log_distance >= min_log_distance {
        return Ok(MetadataCommandCheckpointRecordDecision::Record);
    }

    let checkpoint_bytes = crate::storage_rpc::encode_metadata_command_checkpoint_payload(
        checkpoint,
    )
    .map_err(|error| StoreError::MetadataCheckpointInvalid {
        node_id: 0,
        pg_id: checkpoint.pg_id.get(),
        cluster_epoch: checkpoint.cluster_epoch,
        reason: error.to_string(),
    })?;
    if checkpoint_bytes.len() >= frame_risk_bytes {
        return Ok(MetadataCommandCheckpointRecordDecision::Record);
    }

    Ok(MetadataCommandCheckpointRecordDecision::SkipCadence)
}

fn shard_backfill_candidate_error_is_deferred(error: &StoreError) -> bool {
    match error {
        StoreError::ShardStore { source, .. } => shard_backfill_candidate_error_is_deferred(source),
        StoreError::PgNotActive { .. }
        | StoreError::ShardPgNotActive { .. }
        | StoreError::StalePayloadOperation { .. }
        | StoreError::StaleMetadataPrimaryBridge { .. }
        | StoreError::StaleMetadataOperation { .. }
        | StoreError::StaleMetadataRoute { .. }
        | StoreError::StaleMetadataReadProof { .. }
        | StoreError::RouteMapExpired { .. }
        | StoreError::StaleShardOperation { .. }
        | StoreError::StaleShardLocation { .. }
        | StoreError::StorageRpcResourceExhausted { .. } => true,
        StoreError::Io {
            context: "connect storage-node RPC socket",
            ..
        } => true,
        StoreError::StorageRpc { failure: code, .. } => {
            storage_rpc_code_is_retryable_pg_route_error(*code)
        }
        _ => false,
    }
}

fn storage_rpc_code_is_retryable_pg_route_error(code: StorageRpcErrorCode) -> bool {
    matches!(
        code,
        StorageRpcErrorCode::StaleShardLocation
            | StorageRpcErrorCode::InactivePgRoute
            | StorageRpcErrorCode::NonActingSetAccess
            | StorageRpcErrorCode::WrongClusterEpoch
            | StorageRpcErrorCode::MetadataCommandContention
            | StorageRpcErrorCode::TransportTimeout
            | StorageRpcErrorCode::TransportClosed
    )
}

fn erasure_codec_for_shape(ec: EcShape, context: &'static str) -> Result<ErasureCodec, StoreError> {
    let config = EcConfig::new(ec.k, ec.m).map_err(|error| StoreError::ErasureCoding {
        context,
        reason: error.to_string(),
    })?;
    ErasureCodec::new(config).map_err(|error| StoreError::ErasureCoding {
        context,
        reason: error.to_string(),
    })
}

fn cluster_build_error_to_store(error: ClusterBuildError) -> StoreError {
    match error {
        ClusterBuildError::PgNotFound {
            pg_id,
            cluster_epoch,
        } => StoreError::ClusterPgNotFound {
            pg_id,
            cluster_epoch,
        },
        ClusterBuildError::PgNotActive {
            pg_id,
            cluster_epoch,
            state,
        } => StoreError::PgNotActive {
            pg_id,
            cluster_epoch,
            state,
        },
        ClusterBuildError::StalePayloadPlacement {
            pg_id,
            operation_epoch,
            current_epoch,
        } => StoreError::StalePayloadOperation {
            pg_id,
            operation_epoch,
            current_epoch,
        },
        ClusterBuildError::RouteMapExpired {
            pg_id: _,
            cluster_epoch,
            valid_until_ms,
            now_ms,
        } => StoreError::RouteMapExpired {
            cluster_epoch,
            valid_until_ms,
            now_ms,
        },
        other => StoreError::Io {
            context: "place payload shards",
            source: std::io::Error::other(other.to_string()),
        },
    }
}

fn shard_io_error_to_store(error: ShardIoError) -> StoreError {
    match error {
        ShardIoError::Store {
            node_id,
            pg_id,
            cluster_epoch,
            source,
        } => StoreError::ShardStore {
            node_id,
            pg_id,
            cluster_epoch,
            source: Box::new(source),
        },
        ShardIoError::StaleOperationEpoch {
            node_id,
            pg_id,
            operation_epoch,
            current_epoch,
        } => StoreError::StaleShardOperation {
            node_id,
            pg_id,
            operation_epoch,
            current_epoch,
        },
        ShardIoError::StaleLocation {
            node_id,
            pg_id,
            location_epoch,
            current_epoch,
        } => StoreError::StaleShardLocation {
            node_id,
            pg_id,
            location_epoch,
            current_epoch,
        },
        ShardIoError::RouteMapExpired {
            node_id: _,
            pg_id: _,
            cluster_epoch,
            valid_until_ms,
            now_ms,
        } => StoreError::RouteMapExpired {
            cluster_epoch,
            valid_until_ms,
            now_ms,
        },
        ShardIoError::NodeNotFound {
            node_id,
            pg_id,
            cluster_epoch,
        } => StoreError::NodeNotFound {
            node_id,
            pg_id,
            cluster_epoch,
        },
        ShardIoError::PgNotFound {
            node_id,
            pg_id,
            cluster_epoch,
        } => StoreError::ShardPgNotFound {
            node_id,
            pg_id,
            cluster_epoch,
        },
        ShardIoError::PgNotActive {
            node_id,
            pg_id,
            cluster_epoch,
            state,
        } => StoreError::ShardPgNotActive {
            node_id,
            pg_id,
            cluster_epoch,
            state,
        },
        ShardIoError::NodeNotInActingSet {
            node_id,
            pg_id,
            cluster_epoch,
        } => StoreError::NodeNotInActingSet {
            node_id,
            pg_id,
            cluster_epoch,
        },
        ShardIoError::ShardIndexMismatch {
            node_id,
            pg_id,
            cluster_epoch,
            location_shard_index,
            key_shard_index,
        } => StoreError::ShardIndexMismatch {
            node_id,
            pg_id,
            cluster_epoch,
            location_shard_index,
            key_shard_index,
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoverableShardReadFailure {
    RepairRequired,
    TemporarilyUnavailable,
}

fn placed_segment_recoverable_shard_error(
    error: ShardIoError,
) -> Result<RecoverableShardReadFailure, StoreError> {
    match error {
        ShardIoError::Store {
            source: StoreError::NotFound,
            ..
        }
        | ShardIoError::Store {
            source: StoreError::IntegrityError { .. },
            ..
        }
        | ShardIoError::Store {
            source:
                StoreError::StorageRpcShardDeleteInProgress {
                    operation: "read handles acquire",
                    ..
                },
            ..
        } => Ok(RecoverableShardReadFailure::RepairRequired),
        ShardIoError::Store { ref source, .. }
            if source.storage_node_failure_class()
                == Some(crate::error::StorageNodeFailureClass::TransportInterrupted) =>
        {
            Ok(RecoverableShardReadFailure::TemporarilyUnavailable)
        }
        ShardIoError::Store {
            source:
                StoreError::Io {
                    context:
                        "connect storage-node RPC endpoint"
                        | "connect storage-node RPC socket"
                        | "connect storage-node read-handle RPC endpoint",
                    ..
                },
            ..
        } => Ok(RecoverableShardReadFailure::TemporarilyUnavailable),
        ShardIoError::Store {
            source:
                StoreError::StorageRpc {
                    operation,
                    failure: code,
                    ..
                },
            ..
        } if is_recoverable_remote_shard_read_error(operation, code) => {
            Ok(RecoverableShardReadFailure::RepairRequired)
        }
        ShardIoError::Store {
            source: StoreError::Io { context, source },
            ..
        } if is_recoverable_physical_shard_io_error(context, source.kind()) => {
            Ok(RecoverableShardReadFailure::RepairRequired)
        }
        other => Err(shard_io_error_to_store(other)),
    }
}

fn is_recoverable_remote_shard_read_error(
    operation: &'static str,
    code: StorageRpcErrorCode,
) -> bool {
    matches!(
        operation,
        "shard read" | "shard read range" | "shard historical read"
    ) && matches!(
        code,
        StorageRpcErrorCode::NotFound | StorageRpcErrorCode::ShardIntegrity
    )
}

fn is_recoverable_physical_shard_io_error(context: &'static str, kind: std::io::ErrorKind) -> bool {
    matches!(
        (context, kind),
        (
            "read payload shard size mismatch",
            std::io::ErrorKind::InvalidData
        ) | (
            "read shard file length mismatch",
            std::io::ErrorKind::InvalidData
        ) | ("read shard file", std::io::ErrorKind::UnexpectedEof)
    )
}

#[cfg(test)]
mod backfill_plan_tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashSet;

    fn backfill_plan_test_health(
        risk: PlacedSegmentShardSetRisk,
        valid_indexes: &[u8],
    ) -> PlacedSegmentShardSetHealth {
        let valid_indexes: HashSet<u8> = valid_indexes.iter().copied().collect();
        let shards = (0..6)
            .map(|shard_index| {
                let shard_index = ShardIndex::new(shard_index);
                PlacedSegmentShardHealth {
                    shard_index,
                    shard_key: ShardKey::new(&[9; 16], 1, shard_index.get()),
                    location: ShardLocation::new(
                        ClusterEpoch::INITIAL,
                        DataPgId::new_for_test(PgId::new(0)),
                        shard_index,
                        NodeId::new(u32::from(shard_index.get())),
                    ),
                    validation: if valid_indexes.contains(&shard_index.get()) {
                        PlacedSegmentShardValidation::Valid
                    } else {
                        PlacedSegmentShardValidation::MissingAck
                    },
                }
            })
            .collect();
        PlacedSegmentShardSetHealth {
            total_shards: 6,
            required_shards: 4,
            valid_shards: valid_indexes.len(),
            risk,
            shards,
        }
    }

    fn generated_backfill_plan_test_health(
        required_shards: usize,
        total_shards: usize,
        valid_mask: u16,
        data_pg_id: DataPgId,
        node_offset: u32,
    ) -> PlacedSegmentShardSetHealth {
        let valid_shards = (0..total_shards)
            .filter(|index| (valid_mask & (1_u16 << index)) != 0)
            .count();
        let risk = match valid_shards {
            valid if valid == total_shards => PlacedSegmentShardSetRisk::Healthy,
            valid if valid >= required_shards => PlacedSegmentShardSetRisk::Degraded {
                tolerance_remaining: valid - required_shards,
            },
            _ => PlacedSegmentShardSetRisk::Unrecoverable,
        };
        let shards = (0..total_shards)
            .map(|index| {
                let shard_index = ShardIndex::new(u8::try_from(index).unwrap());
                PlacedSegmentShardHealth {
                    shard_index,
                    shard_key: ShardKey::new(&[7; 16], 1, shard_index.get()),
                    location: ShardLocation::new(
                        ClusterEpoch::INITIAL,
                        data_pg_id,
                        shard_index,
                        NodeId::new(node_offset + u32::from(shard_index.get())),
                    ),
                    validation: if (valid_mask & (1_u16 << index)) != 0 {
                        PlacedSegmentShardValidation::Valid
                    } else {
                        PlacedSegmentShardValidation::MissingAck
                    },
                }
            })
            .collect();
        PlacedSegmentShardSetHealth {
            total_shards,
            required_shards,
            valid_shards,
            risk,
            shards,
        }
    }

    #[test]
    fn backfill_plan_marks_reconstruction_targets_for_recoverable_source_gaps() {
        let source_health = backfill_plan_test_health(
            PlacedSegmentShardSetRisk::Degraded {
                tolerance_remaining: 0,
            },
            &[0, 1, 2, 3],
        );
        let desired_health = backfill_plan_test_health(
            PlacedSegmentShardSetRisk::Degraded {
                tolerance_remaining: 1,
            },
            &[0, 1, 2, 3, 4],
        );

        let plan = build_placed_segment_shard_backfill_plan(source_health, desired_health).unwrap();

        assert_eq!(
            plan.already_present,
            vec![
                ShardIndex::new(0),
                ShardIndex::new(1),
                ShardIndex::new(2),
                ShardIndex::new(3),
                ShardIndex::new(4)
            ]
        );
        assert_eq!(plan.copy_targets, Vec::new());
        assert_eq!(plan.reconstruction_targets, vec![ShardIndex::new(5)]);
        assert_eq!(plan.unrecoverable_targets, Vec::new());
    }

    #[test]
    fn backfill_plan_marks_unrecoverable_targets_for_unrecoverable_source_gaps() {
        let source_health =
            backfill_plan_test_health(PlacedSegmentShardSetRisk::Unrecoverable, &[0, 1, 2]);
        let desired_health = backfill_plan_test_health(
            PlacedSegmentShardSetRisk::Degraded {
                tolerance_remaining: 1,
            },
            &[0, 1, 2, 3, 4],
        );

        let plan = build_placed_segment_shard_backfill_plan(source_health, desired_health).unwrap();

        assert_eq!(
            plan.already_present,
            vec![
                ShardIndex::new(0),
                ShardIndex::new(1),
                ShardIndex::new(2),
                ShardIndex::new(3),
                ShardIndex::new(4)
            ]
        );
        assert_eq!(plan.copy_targets, Vec::new());
        assert_eq!(plan.reconstruction_targets, Vec::new());
        assert_eq!(plan.unrecoverable_targets, vec![ShardIndex::new(5)]);
    }

    proptest! {
        #[test]
        fn prop_backfill_plan_classifies_targets_and_priority(
            required_shards in 1_usize..=6,
            parity_shards in 0_usize..=4,
            source_mask in any::<u16>(),
            desired_mask in any::<u16>(),
        ) {
            let total_shards = required_shards + parity_shards;
            prop_assume!(total_shards <= 10);
            let shard_mask = (1_u16 << total_shards) - 1;
            let source_mask = source_mask & shard_mask;
            let desired_mask = desired_mask & shard_mask;
            let source_valid_count = usize::try_from(source_mask.count_ones()).unwrap();
            let source_recoverable = source_valid_count >= required_shards;

            let source_health = generated_backfill_plan_test_health(
                required_shards,
                total_shards,
                source_mask,
                DataPgId::new_for_test(PgId::new(0)),
                10,
            );
            let desired_health = generated_backfill_plan_test_health(
                required_shards,
                total_shards,
                desired_mask,
                DataPgId::new_for_test(PgId::new(1)),
                100,
            );

            let plan = build_placed_segment_shard_backfill_plan(
                source_health.clone(),
                desired_health.clone(),
            )
            .unwrap();

            let expected_tolerance = source_valid_count.saturating_sub(required_shards);
            prop_assert_eq!(
                usize::from(plan.source_remaining_tolerance()),
                expected_tolerance
            );
            prop_assert_eq!(plan.source_health.valid_shards, source_valid_count);
            prop_assert_eq!(
                plan.source_health.risk,
                if source_valid_count == total_shards {
                    PlacedSegmentShardSetRisk::Healthy
                } else if source_recoverable {
                    PlacedSegmentShardSetRisk::Degraded {
                        tolerance_remaining: expected_tolerance,
                    }
                } else {
                    PlacedSegmentShardSetRisk::Unrecoverable
                }
            );

            for index in 0..total_shards {
                let shard_index = ShardIndex::new(u8::try_from(index).unwrap());
                let desired_valid = (desired_mask & (1_u16 << index)) != 0;
                let source_valid = (source_mask & (1_u16 << index)) != 0;
                let is_already_present = plan.already_present.contains(&shard_index);
                let copy_target = plan
                    .copy_targets
                    .iter()
                    .find(|target| target.shard_index == shard_index);
                let is_reconstruction_target =
                    plan.reconstruction_targets.contains(&shard_index);
                let is_unrecoverable_target =
                    plan.unrecoverable_targets.contains(&shard_index);
                let target_count = usize::from(is_already_present)
                    + usize::from(copy_target.is_some())
                    + usize::from(is_reconstruction_target)
                    + usize::from(is_unrecoverable_target);

                prop_assert_eq!(
                    target_count,
                    1,
                    "shard {} must be classified exactly once",
                    index
                );

                if desired_valid {
                    prop_assert!(is_already_present);
                    prop_assert!(copy_target.is_none());
                    prop_assert!(!is_reconstruction_target);
                    prop_assert!(!is_unrecoverable_target);
                } else if source_valid {
                    let target = copy_target.expect("valid source shard must produce a copy target");
                    prop_assert_eq!(target.source.shard_index(), shard_index);
                    prop_assert_eq!(target.destination.shard_index(), shard_index);
                    prop_assert!(!is_already_present);
                    prop_assert!(!is_reconstruction_target);
                    prop_assert!(!is_unrecoverable_target);
                } else if source_recoverable {
                    prop_assert!(is_reconstruction_target);
                    prop_assert!(!is_already_present);
                    prop_assert!(copy_target.is_none());
                    prop_assert!(!is_unrecoverable_target);
                } else {
                    prop_assert!(is_unrecoverable_target);
                    prop_assert!(!is_already_present);
                    prop_assert!(copy_target.is_none());
                    prop_assert!(!is_reconstruction_target);
                }
            }
        }
    }
}

#[cfg(test)]
mod reissue_decision_tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn metadata_contention_backoff_cap_grows_and_clamps() {
        assert_eq!(
            metadata_contention_backoff_cap(1),
            METADATA_CONTENTION_BACKOFF_INITIAL
        );
        assert_eq!(
            metadata_contention_backoff_cap(2),
            METADATA_CONTENTION_BACKOFF_INITIAL * 2
        );
        assert_eq!(
            metadata_contention_backoff_cap(3),
            METADATA_CONTENTION_BACKOFF_INITIAL * 4
        );
        assert_eq!(
            metadata_contention_backoff_cap(64),
            METADATA_CONTENTION_BACKOFF_MAX
        );
    }

    #[test]
    fn placed_segment_direct_read_recovers_when_read_handle_acquire_hits_delete_fence() {
        let error = ShardIoError::Store {
            node_id: 5,
            pg_id: 13,
            cluster_epoch: ClusterEpoch::INITIAL,
            source: StoreError::StorageRpcShardDeleteInProgress {
                node_id: 5,
                operation: "read handles acquire",
                detail: crate::StorageNodeFailureDetail::new("shard is being deleted"),
            },
        };

        assert_eq!(
            placed_segment_recoverable_shard_error(error).unwrap(),
            RecoverableShardReadFailure::RepairRequired
        );
    }

    #[test]
    fn placed_segment_direct_read_does_not_recover_unrelated_delete_fence_errors() {
        let error = ShardIoError::Store {
            node_id: 5,
            pg_id: 13,
            cluster_epoch: ClusterEpoch::INITIAL,
            source: StoreError::StorageRpcShardDeleteInProgress {
                node_id: 5,
                operation: "shard delete",
                detail: crate::StorageNodeFailureDetail::new("shard is being deleted"),
            },
        };

        assert!(matches!(
            placed_segment_recoverable_shard_error(error),
            Err(StoreError::ShardStore { .. })
        ));
    }

    #[test]
    fn placed_segment_read_recovers_remote_shard_read_damage_errors() {
        for operation in ["shard read", "shard read range", "shard historical read"] {
            for (code, message) in [
                (StorageRpcErrorCode::NotFound, "not found".to_string()),
                (
                    StorageRpcErrorCode::ShardIntegrity,
                    "remote shard integrity failure".to_string(),
                ),
            ] {
                let error = ShardIoError::Store {
                    node_id: 5,
                    pg_id: 13,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    source: StoreError::StorageRpc {
                        node_id: 5,
                        operation,
                        failure: code,
                        detail: crate::StorageNodeFailureDetail::new(message),
                    },
                };

                assert_eq!(
                    placed_segment_recoverable_shard_error(error).unwrap(),
                    RecoverableShardReadFailure::RepairRequired
                );
            }
        }
    }

    #[test]
    fn placed_segment_read_treats_transport_interruption_as_temporary_unavailability() {
        for source in [
            StoreError::Io {
                context: "connect storage-node read-handle RPC endpoint",
                source: std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "storage node is unavailable",
                ),
            },
            StoreError::StorageRpc {
                node_id: 5,
                operation: "shard read",
                failure: StorageRpcErrorCode::TransportClosed,
                detail: crate::StorageNodeFailureDetail::new("storage node connection closed"),
            },
        ] {
            let error = ShardIoError::Store {
                node_id: 5,
                pg_id: 13,
                cluster_epoch: ClusterEpoch::INITIAL,
                source,
            };

            assert_eq!(
                placed_segment_recoverable_shard_error(error).unwrap(),
                RecoverableShardReadFailure::TemporarilyUnavailable
            );
        }
    }

    #[test]
    fn placed_segment_read_does_not_recover_unrelated_remote_rpc_errors() {
        for (operation, code, message) in [
            (
                "shard read",
                StorageRpcErrorCode::Internal,
                "shard not found",
            ),
            (
                "shard read",
                StorageRpcErrorCode::Internal,
                "shard 00000000000000000000000000000000000000000000000000 ack mismatch: expected size 4 CRC 0x0000000000000001, got size 4 CRC 0x0000000000000002",
            ),
            (
                "shard delete",
                StorageRpcErrorCode::ShardIntegrity,
                "remote shard integrity failure",
            ),
        ] {
            let error = ShardIoError::Store {
                node_id: 5,
                pg_id: 13,
                cluster_epoch: ClusterEpoch::INITIAL,
                source: StoreError::StorageRpc {
                    node_id: 5,
                    operation,
                    failure: code,
                    detail: crate::StorageNodeFailureDetail::new(message),
                },
            };

            assert!(matches!(
                placed_segment_recoverable_shard_error(error),
                Err(StoreError::ShardStore { .. })
            ));
        }
    }

    fn replica_match(code: u8) -> ReissuedPendingCommandReplicaMatch {
        match code % 3 {
            0 => ReissuedPendingCommandReplicaMatch::BelowReplacement,
            1 => ReissuedPendingCommandReplicaMatch::MatchesHashChain,
            _ => ReissuedPendingCommandReplicaMatch::MissingOrMismatched,
        }
    }

    proptest! {
        #[test]
        fn prop_reissued_pending_command_decision_is_fail_closed(
            payload_matches in any::<bool>(),
            primary_max_log_index in 0_u64..64,
            primary_applied_log_index in 0_u64..64,
            primary_applied_log_hash in any::<u64>(),
            acting_set_max_log_index in 0_u64..66,
            current_log_index in 0_u64..66,
            replica_inputs in proptest::collection::vec(
                (0_u32..6, 0_u64..66, 0_u64..66, any::<u64>(), 0_u8..3),
                0..8,
            ),
        ) {
            let primary_node_id = NodeId::new(1);
            let replicas = replica_inputs
                .into_iter()
                .map(|(node_id, max_log_index, applied_log_index, applied_log_hash, match_code)| {
                    ReissuedPendingCommandReplicaSummary {
                        node_id: NodeId::new(node_id),
                        max_log_index,
                        applied_log_index,
                        applied_log_hash,
                        replacement_match: replica_match(match_code),
                    }
                })
                .collect::<Vec<_>>();

            let decision = decide_reissued_pending_command(
                ReissuedPendingCommandPrimarySummary {
                    node_id: primary_node_id,
                    max_log_index: primary_max_log_index,
                    applied_log_index: primary_applied_log_index,
                    applied_log_hash: primary_applied_log_hash,
                },
                acting_set_max_log_index,
                current_log_index,
                payload_matches,
                &replicas,
            );

            if !payload_matches {
                prop_assert_eq!(decision, ReissuedPendingCommandDecision::StaleCommandDisplaced);
                return Ok(());
            }
            if primary_applied_log_index != primary_max_log_index {
                prop_assert_eq!(
                    decision,
                    ReissuedPendingCommandDecision::Conflict {
                        node_id: primary_node_id,
                        log_index: primary_max_log_index,
                    }
                );
                return Ok(());
            }
            let expected_log_index = primary_max_log_index + 1;
            if current_log_index != expected_log_index
                || acting_set_max_log_index > current_log_index
            {
                prop_assert_eq!(
                    decision,
                    ReissuedPendingCommandDecision::Conflict {
                        node_id: primary_node_id,
                        log_index: acting_set_max_log_index.max(current_log_index),
                    }
                );
                return Ok(());
            }
            for replica in &replicas {
                if replica.max_log_index < current_log_index {
                    if replica.max_log_index != primary_applied_log_index
                        || replica.applied_log_index != primary_applied_log_index
                        || replica.applied_log_hash != primary_applied_log_hash
                    {
                        prop_assert_eq!(
                            decision,
                            ReissuedPendingCommandDecision::Conflict {
                                node_id: replica.node_id,
                                log_index: primary_applied_log_index.max(replica.max_log_index),
                            }
                        );
                        return Ok(());
                    }
                    continue;
                }
                if replica.replacement_match != ReissuedPendingCommandReplicaMatch::MatchesHashChain
                {
                    prop_assert_eq!(
                        decision,
                        ReissuedPendingCommandDecision::Conflict {
                            node_id: replica.node_id,
                            log_index: current_log_index,
                        }
                    );
                    return Ok(());
                }
            }
            prop_assert_eq!(decision, ReissuedPendingCommandDecision::ReloadCurrent);
        }
    }

    #[test]
    fn reissued_pending_command_decision_allows_primary_last_window() {
        let decision = decide_reissued_pending_command(
            ReissuedPendingCommandPrimarySummary {
                node_id: NodeId::new(1),
                max_log_index: 1,
                applied_log_index: 1,
                applied_log_hash: 100,
            },
            2,
            2,
            true,
            &[
                ReissuedPendingCommandReplicaSummary {
                    node_id: NodeId::new(1),
                    max_log_index: 1,
                    applied_log_index: 1,
                    applied_log_hash: 100,
                    replacement_match: ReissuedPendingCommandReplicaMatch::BelowReplacement,
                },
                ReissuedPendingCommandReplicaSummary {
                    node_id: NodeId::new(0),
                    max_log_index: 2,
                    applied_log_index: 2,
                    applied_log_hash: 200,
                    replacement_match: ReissuedPendingCommandReplicaMatch::MatchesHashChain,
                },
            ],
        );
        assert_eq!(decision, ReissuedPendingCommandDecision::ReloadCurrent);
    }

    #[test]
    fn reissued_pending_command_decision_rejects_divergent_prefix() {
        let decision = decide_reissued_pending_command(
            ReissuedPendingCommandPrimarySummary {
                node_id: NodeId::new(1),
                max_log_index: 1,
                applied_log_index: 1,
                applied_log_hash: 100,
            },
            2,
            2,
            true,
            &[ReissuedPendingCommandReplicaSummary {
                node_id: NodeId::new(0),
                max_log_index: 2,
                applied_log_index: 2,
                applied_log_hash: 200,
                replacement_match: ReissuedPendingCommandReplicaMatch::MissingOrMismatched,
            }],
        );
        assert_eq!(
            decision,
            ReissuedPendingCommandDecision::Conflict {
                node_id: NodeId::new(0),
                log_index: 2,
            }
        );
    }

    #[test]
    fn reissued_pending_command_decision_rejects_below_replacement_divergent_prefix() {
        let decision = decide_reissued_pending_command(
            ReissuedPendingCommandPrimarySummary {
                node_id: NodeId::new(1),
                max_log_index: 1,
                applied_log_index: 1,
                applied_log_hash: 100,
            },
            1,
            2,
            true,
            &[ReissuedPendingCommandReplicaSummary {
                node_id: NodeId::new(0),
                max_log_index: 1,
                applied_log_index: 1,
                applied_log_hash: 999,
                replacement_match: ReissuedPendingCommandReplicaMatch::BelowReplacement,
            }],
        );
        assert_eq!(
            decision,
            ReissuedPendingCommandDecision::Conflict {
                node_id: NodeId::new(0),
                log_index: 1,
            }
        );
    }
}
