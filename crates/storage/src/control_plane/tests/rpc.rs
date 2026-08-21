// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

const CONTROL_PLANE_RPC_CATALOGUE_PG_STATES: [PgState; 5] = [
    PgState::Active,
    PgState::Peering,
    PgState::Degraded,
    PgState::Backfilling,
    PgState::Inconsistent,
];
const CONTROL_PLANE_RPC_CATALOGUE_HISTORY_KINDS: [PgClusterMapHistoryRouteReferenceKind; 5] = [
    PgClusterMapHistoryRouteReferenceKind::LivePlacement,
    PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
    PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
    PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
    PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim,
];
const CONTROL_PLANE_RPC_CATALOGUE_METRIC_KINDS: [observability::ControlPlaneRpcMetricKind; 17] = {
    use observability::ControlPlaneRpcMetricKind as Kind;
    [
        Kind::RuntimeMapSnapshot,
        Kind::RefreshNodeHeartbeat,
        Kind::SetPgActingSet,
        Kind::SetPgActingSetWithMetadataTransfer,
        Kind::SetPgActingSetWithMetadataTransferRuntimeMap,
        Kind::FencePgForMetadataTransferRuntimeMap,
        Kind::TransferRaftLeadership,
        Kind::PgRuntimeMapSnapshot,
        Kind::TriggerRaftSnapshotAndPurge,
        Kind::TriggerRaftElection,
        Kind::RuntimeMapStatus,
        Kind::PendingMetadataCommandRecoveries,
        Kind::AuthorityClockStatus,
        Kind::ReestablishAuthorityClock,
        Kind::RuntimeMapDiagnostics,
        Kind::Unknown,
        Kind::ServingPgRuntimeMapSnapshot,
    ]
};
const CONTROL_PLANE_RPC_CATALOGUE_RECOVERY_FAILURE_KINDS:
    [PendingMetadataCommandRecoveryDiscoveryFailureKind; 3] = [
    PendingMetadataCommandRecoveryDiscoveryFailureKind::HistoricalRouteInvalid,
    PendingMetadataCommandRecoveryDiscoveryFailureKind::ReporterNotHistoricalPrimary,
    PendingMetadataCommandRecoveryDiscoveryFailureKind::ConflictingIdentity,
];
const CONTROL_PLANE_RPC_CATALOGUE_OPENRAFT_ERROR_KINDS: [ControlPlaneRaftOperationErrorKind; 4] = [
    ControlPlaneRaftOperationErrorKind::ForwardToLeader,
    ControlPlaneRaftOperationErrorKind::QuorumNotEnough,
    ControlPlaneRaftOperationErrorKind::Fatal,
    ControlPlaneRaftOperationErrorKind::Rejected,
];
const CONTROL_PLANE_RPC_CATALOGUE_BLOCKED_REASONS: [Option<
    ControlPlaneAuthorityClockBlockedReason,
>; 8] = [
    None,
    Some(ControlPlaneAuthorityClockBlockedReason::InitialTimestampDiscontinuity),
    Some(ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged),
    Some(ControlPlaneAuthorityClockBlockedReason::ClockSourceUnavailable),
    Some(ControlPlaneAuthorityClockBlockedReason::ClockHealthRegression),
    Some(ControlPlaneAuthorityClockBlockedReason::WallClockRegression),
    Some(ControlPlaneAuthorityClockBlockedReason::WallClockForwardJump),
    Some(ControlPlaneAuthorityClockBlockedReason::CheckpointPersistenceFailure),
];

fn control_plane_rpc_catalogue_snapshot() -> ClusterRuntimeMapSnapshot {
    runtime_map_test_snapshot_with_active_route()
}

fn control_plane_rpc_catalogue_freshness_kind(
    proof: &RuntimeMapFreshnessProof,
) -> ControlPlaneRpcRuntimeMapFreshnessProofKind {
    match proof {
        RuntimeMapFreshnessProof::SingleAuthority { .. } => {
            ControlPlaneRpcRuntimeMapFreshnessProofKind::SingleAuthority
        }
        RuntimeMapFreshnessProof::Reconstructed { .. } => {
            ControlPlaneRpcRuntimeMapFreshnessProofKind::Reconstructed
        }
        RuntimeMapFreshnessProof::ReadIndex { .. } => {
            ControlPlaneRpcRuntimeMapFreshnessProofKind::ReadIndex
        }
    }
}

fn control_plane_rpc_catalogue_transfer_snapshot() -> ClusterRuntimeMapSnapshot {
    let mut snapshot = runtime_map_test_snapshot_with_transfer_route(true);
    snapshot.freshness_proof = RuntimeMapFreshnessProof::ReadIndex {
        authority_incarnation: AuthorityIncarnation::new(2).unwrap(),
        read_index: ControlPlaneLogId::new(3, 4).unwrap(),
        issued_at_ms: 5,
    };
    snapshot.nodes[0].cluster_map_history_floor_epoch = Some(ClusterEpoch::INITIAL);
    snapshot.pg_routes[0].metadata_read_route = Some(PgMetadataReadRoute::new(
        NodeId::new(1),
        PgMetadataProof::current(6, 7, 8),
    ));
    snapshot
}

fn control_plane_rpc_catalogue_recovery_snapshot() -> ClusterRuntimeMapSnapshot {
    let mut snapshot = runtime_map_test_snapshot_with_transfer_route(true);
    snapshot.freshness_proof = RuntimeMapFreshnessProof::Reconstructed {
        authority_incarnation: AuthorityIncarnation::new(9).unwrap(),
    };
    let route = &mut snapshot.pg_routes[0];
    route.peering_metadata_transfer = None;
    route.peering_metadata_transfer_destination_epoch = None;
    route.peering_metadata_transfer_source_route_epoch = None;
    route.peering_metadata_transfer_source_node_id = None;
    route.pending_metadata_command_recovery = Some(PendingMetadataCommandRecovery::new(
        NodeId::new(1),
        PendingMetadataCommandObservation::new(
            ClusterEpoch::INITIAL,
            NonZeroU64::new(10).unwrap(),
            11,
        ),
    ));
    snapshot
}

fn control_plane_rpc_catalogue_empty_snapshot() -> ClusterRuntimeMapSnapshot {
    runtime_map_test_snapshot(RuntimeMapFreshnessProof::Reconstructed {
        authority_incarnation: AuthorityIncarnation::new(12).unwrap(),
    })
}

fn assert_control_plane_rpc_catalogue_runtime_map_branches(
    snapshots: &[ClusterRuntimeMapSnapshot],
) {
    let nodes = snapshots
        .iter()
        .flat_map(|snapshot| snapshot.nodes())
        .collect::<Vec<_>>();
    assert!(nodes
        .iter()
        .any(|node| node.cluster_map_history_floor_epoch().is_none()));
    assert!(nodes
        .iter()
        .any(|node| node.cluster_map_history_floor_epoch().is_some()));

    let routes = snapshots
        .iter()
        .flat_map(|snapshot| {
            snapshot
                .pg_routes()
                .iter()
                .chain(snapshot.historical_pg_routes())
        })
        .collect::<Vec<_>>();
    assert!(routes
        .iter()
        .any(|route| route.active_metadata_proof().is_none()));
    assert!(routes
        .iter()
        .any(|route| route.active_metadata_proof().is_some()));
    assert!(routes
        .iter()
        .any(|route| route.metadata_read_route().is_none()));
    assert!(routes
        .iter()
        .any(|route| route.metadata_read_route().is_some()));
    assert!(routes
        .iter()
        .any(|route| route.primary_lease_deadline_ms().is_none()));
    assert!(routes
        .iter()
        .any(|route| route.primary_lease_deadline_ms().is_some()));
    assert!(routes
        .iter()
        .any(|route| route.peering_metadata_transfer().is_none()));
    assert!(routes
        .iter()
        .any(|route| route.peering_metadata_transfer().is_some()));
    assert!(routes.iter().any(|route| {
        route
            .peering_metadata_transfer_destination_epoch()
            .is_none()
    }));
    assert!(routes.iter().any(|route| {
        route
            .peering_metadata_transfer_destination_epoch()
            .is_some()
    }));
    assert!(routes.iter().any(|route| {
        route
            .peering_metadata_transfer_source_route_epoch()
            .is_none()
    }));
    assert!(routes.iter().any(|route| {
        route
            .peering_metadata_transfer_source_route_epoch()
            .is_some()
    }));
    assert!(routes
        .iter()
        .any(|route| route.peering_metadata_transfer_source_node_id().is_none()));
    assert!(routes
        .iter()
        .any(|route| route.peering_metadata_transfer_source_node_id().is_some()));
    assert!(routes
        .iter()
        .any(|route| route.pending_metadata_command_recovery().is_none()));
    assert!(routes
        .iter()
        .any(|route| route.pending_metadata_command_recovery().is_some()));

    assert!(snapshots.iter().any(|snapshot| snapshot.nodes().is_empty()));
    assert!(snapshots
        .iter()
        .any(|snapshot| !snapshot.nodes().is_empty()));
    assert!(snapshots
        .iter()
        .any(|snapshot| snapshot.pg_routes().is_empty()));
    assert!(snapshots
        .iter()
        .any(|snapshot| !snapshot.pg_routes().is_empty()));
    assert!(snapshots
        .iter()
        .any(|snapshot| snapshot.historical_pg_routes().is_empty()));
    assert!(snapshots
        .iter()
        .any(|snapshot| !snapshot.historical_pg_routes().is_empty()));
    assert!(snapshots
        .iter()
        .any(|snapshot| snapshot.historical_cluster_epochs().is_empty()));
    assert!(snapshots
        .iter()
        .any(|snapshot| !snapshot.historical_cluster_epochs().is_empty()));
}

fn control_plane_rpc_catalogue_transfer() -> PgMetadataTransferProof {
    PgMetadataTransferProof::new_with_imported_metadata_proof(
        ClusterEpoch::new(9).unwrap(),
        PgMetadataProof::current(10, 11, 12),
        PgMetadataProof::current(13, 14, 15),
    )
}

fn control_plane_rpc_catalogue_heartbeat() -> NodeHeartbeat {
    NodeHeartbeat {
        node_id: NodeId::new(3),
        node_incarnation: 4,
        endpoint: "unix:///catalogue-node-3".to_owned(),
        observed_epoch: ClusterEpoch::new(5).unwrap(),
        requested_lease_duration_ms: 6_000,
        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
        cluster_map_history_route_references: PgClusterMapHistoryRouteReferences::try_from_iter(
            CONTROL_PLANE_RPC_CATALOGUE_HISTORY_KINDS
                .into_iter()
                .enumerate()
                .map(|(index, kind)| {
                    PgClusterMapHistoryRouteReference::new(
                        kind,
                        ClusterEpoch::new(u64::try_from(index).unwrap() + 1).unwrap(),
                        PgId::new(u32::try_from(index).unwrap() + 20),
                    )
                }),
        )
        .unwrap(),
        pg_observations: CONTROL_PLANE_RPC_CATALOGUE_PG_STATES
            .into_iter()
            .enumerate()
            .map(|(index, state)| NodePgHeartbeatObservation {
                pg_id: PgId::new(u32::try_from(index).unwrap() + 7),
                state,
                metadata_proof: PgMetadataProof::current(
                    u64::try_from(index).unwrap() + 8,
                    u64::try_from(index).unwrap() + 20,
                    u64::try_from(index).unwrap() + 30,
                ),
                pending_metadata_command: (index % 2 == 0).then(|| {
                    PendingMetadataCommandObservation::new(
                        ClusterEpoch::new(u64::try_from(index).unwrap() + 11).unwrap(),
                        NonZeroU64::new(u64::try_from(index).unwrap() + 12).unwrap(),
                        u64::try_from(index).unwrap() + 13,
                    )
                }),
            })
            .collect(),
    }
}

fn control_plane_rpc_catalogue_request(
    kind: ControlPlaneRpcKind,
) -> Result<Vec<u8>, ControlPlaneError> {
    let mut payload = Vec::new();
    match kind {
        ControlPlaneRpcKind::RuntimeMapSnapshot
        | ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge
        | ControlPlaneRpcKind::TriggerRaftElection
        | ControlPlaneRpcKind::RuntimeMapStatus
        | ControlPlaneRpcKind::PendingMetadataCommandRecoveries
        | ControlPlaneRpcKind::AuthorityClockStatus
        | ControlPlaneRpcKind::ReestablishAuthorityClock
        | ControlPlaneRpcKind::RuntimeMapDiagnostics => {}
        ControlPlaneRpcKind::RefreshNodeHeartbeat => {
            write_node_heartbeat(&mut payload, &control_plane_rpc_catalogue_heartbeat())?;
        }
        ControlPlaneRpcKind::SetPgActingSet => {
            write_pg_acting_set_request(
                &mut payload,
                PgId::new(7),
                &[NodeId::new(1), NodeId::new(3)],
            )?;
        }
        ControlPlaneRpcKind::SetPgActingSetWithMetadataTransfer
        | ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap => {
            write_pg_acting_set_with_metadata_transfer_request(
                &mut payload,
                PgId::new(7),
                &[NodeId::new(1), NodeId::new(3)],
                control_plane_rpc_catalogue_transfer(),
                ClusterEpoch::new(16).unwrap(),
            )?;
        }
        ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap
        | ControlPlaneRpcKind::PgRuntimeMapSnapshot
        | ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot => {
            write_pg_id_request(&mut payload, PgId::new(7));
        }
        ControlPlaneRpcKind::TransferRaftLeadership => write_u64(&mut payload, 17),
    }
    Ok(payload)
}

fn validate_control_plane_rpc_catalogue_request(
    kind: ControlPlaneRpcKind,
    payload: &[u8],
) -> Result<(), ControlPlaneError> {
    let mut reader = PayloadReader::new(payload);
    match kind {
        ControlPlaneRpcKind::RuntimeMapSnapshot
        | ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge
        | ControlPlaneRpcKind::TriggerRaftElection
        | ControlPlaneRpcKind::RuntimeMapStatus
        | ControlPlaneRpcKind::PendingMetadataCommandRecoveries
        | ControlPlaneRpcKind::AuthorityClockStatus
        | ControlPlaneRpcKind::ReestablishAuthorityClock
        | ControlPlaneRpcKind::RuntimeMapDiagnostics => {}
        ControlPlaneRpcKind::RefreshNodeHeartbeat => {
            assert_eq!(
                read_node_heartbeat(&mut reader)?,
                control_plane_rpc_catalogue_heartbeat()
            );
        }
        ControlPlaneRpcKind::SetPgActingSet => {
            assert_eq!(
                read_pg_acting_set_request(&mut reader)?,
                (PgId::new(7), vec![NodeId::new(1), NodeId::new(3)])
            );
        }
        ControlPlaneRpcKind::SetPgActingSetWithMetadataTransfer
        | ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap => {
            assert_eq!(
                read_pg_acting_set_with_metadata_transfer_request(&mut reader)?,
                (
                    PgId::new(7),
                    vec![NodeId::new(1), NodeId::new(3)],
                    control_plane_rpc_catalogue_transfer(),
                    ClusterEpoch::new(16).unwrap(),
                )
            );
        }
        ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap
        | ControlPlaneRpcKind::PgRuntimeMapSnapshot
        | ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot => {
            assert_eq!(read_pg_id_request(&mut reader)?, PgId::new(7));
        }
        ControlPlaneRpcKind::TransferRaftLeadership => assert_eq!(reader.read_u64()?, 17),
    }
    reader.finish()
}

fn control_plane_rpc_catalogue_diagnostics() -> ControlPlaneRuntimeMapDiagnostics {
    let rpc_metrics = CONTROL_PLANE_RPC_CATALOGUE_METRIC_KINDS
        .into_iter()
        .enumerate()
        .map(|(index, kind)| {
            let base = u64::try_from(index).unwrap() * 20;
            observability::ControlPlaneRpcMetricSample {
                kind,
                total: base + 1,
                lock_wait_us_total: base + 2,
                lock_wait_us_max: base + 3,
                operation_us_total: base + 4,
                operation_us_max: base + 5,
                response_write_us_total: base + 6,
                response_write_us_max: base + 7,
                response_write_error_total: base + 8,
                response_write_broken_pipe_total: base + 9,
                response_write_connection_reset_total: base + 10,
                response_write_timeout_total: base + 11,
                response_write_other_error_total: base + 12,
            }
        })
        .collect();
    ControlPlaneRuntimeMapDiagnostics {
        runtime_map: control_plane_rpc_catalogue_snapshot(),
        rpc_metrics,
        snapshot_metrics: observability::ControlPlaneSnapshotMetricSnapshot {
            serialize_total: 401,
            serialize_us_total: 402,
            serialize_us_max: 403,
            save_total: 404,
            save_error_total: 405,
            save_us_total: 406,
            save_us_max: 407,
            sync_total: 408,
            sync_us_total: 409,
            sync_us_max: 410,
            bytes_total: 411,
            bytes_last: 412,
            bytes_max: 413,
        },
        journal_metrics: observability::ControlPlaneJournalMetricSnapshot {
            append_total: 421,
            append_error_total: 422,
            append_us_total: 423,
            append_us_max: 424,
            lock_wait_us_total: 425,
            lock_wait_us_max: 426,
            frame_bytes_total: 427,
            frame_bytes_last: 428,
            frame_bytes_max: 429,
            file_sync_total: 430,
            file_sync_us_total: 431,
            file_sync_us_max: 432,
            directory_sync_total: 433,
            directory_sync_us_total: 434,
            directory_sync_us_max: 435,
            compaction_total: 436,
            compaction_error_total: 437,
            compaction_us_total: 438,
            compaction_us_max: 439,
            compaction_lock_wait_us_total: 440,
            compaction_lock_wait_us_max: 441,
            compaction_bytes_total: 442,
            compaction_bytes_last: 443,
            compaction_bytes_max: 444,
            compaction_file_sync_total: 445,
            compaction_file_sync_us_total: 446,
            compaction_file_sync_us_max: 447,
            compaction_directory_sync_total: 448,
            compaction_directory_sync_us_total: 449,
            compaction_directory_sync_us_max: 450,
        },
        raft_checkpoint_metrics: observability::ControlPlaneRaftCheckpointMetricSnapshot {
            encode_total: 461,
            encode_us_total: 462,
            encode_us_max: 463,
            store_total: 464,
            store_error_total: 465,
            store_us_total: 466,
            store_us_max: 467,
            file_sync_total: 468,
            file_sync_us_total: 469,
            file_sync_us_max: 470,
            directory_sync_total: 471,
            directory_sync_us_total: 472,
            directory_sync_us_max: 473,
            bytes_total: 474,
            bytes_last: 475,
            bytes_max: 476,
            compaction_total: 477,
            compaction_error_total: 478,
            compaction_us_total: 479,
            compaction_us_max: 480,
        },
        raft_wal_metrics: observability::ControlPlaneRaftWalMetricSnapshot {
            append_total: 491,
            append_error_total: 492,
            append_us_total: 493,
            append_us_max: 494,
            lock_wait_us_total: 495,
            lock_wait_us_max: 496,
            frame_bytes_total: 497,
            frame_bytes_last: 498,
            frame_bytes_max: 499,
            file_sync_total: 500,
            file_sync_us_total: 501,
            file_sync_us_max: 502,
            directory_sync_total: 503,
            directory_sync_us_total: 504,
            directory_sync_us_max: 505,
            durability_queue_depth: 506,
            durability_queue_depth_max: 507,
            durability_queue_wait_us_total: 508,
            durability_queue_wait_us_max: 509,
            append_accept_us_total: 510,
            append_accept_us_max: 511,
            durability_operation_us_total: 512,
            durability_operation_us_max: 513,
        },
        raft_command_metrics: observability::ControlPlaneRaftCommandMetricSnapshot {
            submit_total: 521,
            submit_error_total: 522,
            queue_wait_us_total: 523,
            queue_wait_us_max: 524,
            operation_us_total: 525,
            operation_us_max: 526,
        },
        history_reference_samples: vec![observability::ControlPlaneHistoryReferenceSample {
            node_id: 1,
            observed_epoch: 1,
            validation_epoch: 1,
            observed_at_ms: 531,
            oldest_live_placement_epoch: Some(1),
            oldest_durable_backfill_epoch: None,
            oldest_metadata_command_resource_epoch: Some(1),
            oldest_object_payload_reclaim_claim_epoch: None,
        }],
        node_leases: vec![ControlPlaneRuntimeMapNodeLeaseDiagnostic::new(
            NodeId::new(1),
            Some(541),
        )],
    }
}

fn control_plane_rpc_catalogue_diagnostics_alternate_options() -> ControlPlaneRuntimeMapDiagnostics
{
    let mut diagnostics = control_plane_rpc_catalogue_diagnostics();
    diagnostics.history_reference_samples[0] = observability::ControlPlaneHistoryReferenceSample {
        node_id: 1,
        observed_epoch: 1,
        validation_epoch: 1,
        observed_at_ms: 551,
        oldest_live_placement_epoch: None,
        oldest_durable_backfill_epoch: Some(1),
        oldest_metadata_command_resource_epoch: None,
        oldest_object_payload_reclaim_claim_epoch: Some(1),
    };
    diagnostics.node_leases[0] =
        ControlPlaneRuntimeMapNodeLeaseDiagnostic::new(NodeId::new(1), None);
    diagnostics
}

fn control_plane_rpc_catalogue_recoveries() -> PendingMetadataCommandRecoveryListing {
    PendingMetadataCommandRecoveryListing::new(
        vec![PendingMetadataCommandRecoveryTask::new(
            PgId::new(7),
            PendingMetadataCommandRecovery::new(
                NodeId::new(3),
                PendingMetadataCommandObservation::new(
                    ClusterEpoch::new(9).unwrap(),
                    NonZeroU64::new(11).unwrap(),
                    13,
                ),
            ),
        )],
        CONTROL_PLANE_RPC_CATALOGUE_RECOVERY_FAILURE_KINDS
            .into_iter()
            .enumerate()
            .map(|(index, kind)| {
                PendingMetadataCommandRecoveryDiscoveryFailure::new(
                    PgId::new(u32::try_from(index).unwrap() + 20),
                    kind,
                    format!("catalogue failure {index}"),
                )
            })
            .collect(),
    )
}

fn control_plane_rpc_catalogue_clock_status() -> ControlPlaneAuthorityClockStatus {
    ControlPlaneAuthorityClockStatus {
        generation: 31,
        established: false,
        blocked_reason: Some(ControlPlaneAuthorityClockBlockedReason::CheckpointPersistenceFailure),
        committed_timestamp_high_water_ms: Some(32),
        bound_raft_leadership_term: None,
        current_raft_leadership_term: Some(33),
        local_raft_authority_leader: true,
        local_raft_authority_serving: false,
    }
}

fn control_plane_rpc_catalogue_success(
    kind: ControlPlaneRpcKind,
) -> Result<Vec<u8>, ControlPlaneError> {
    let mut payload = Vec::new();
    match kind {
        ControlPlaneRpcKind::RuntimeMapSnapshot
        | ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap
        | ControlPlaneRpcKind::PgRuntimeMapSnapshot
        | ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot => {
            write_runtime_map_snapshot(&mut payload, &control_plane_rpc_catalogue_snapshot())?;
        }
        ControlPlaneRpcKind::RuntimeMapDiagnostics => {
            write_control_plane_runtime_map_diagnostics_value(
                &mut payload,
                &control_plane_rpc_catalogue_diagnostics(),
            )?;
        }
        ControlPlaneRpcKind::RuntimeMapStatus => {
            write_runtime_map_status(
                &mut payload,
                ControlPlaneRuntimeMapStatus::from_runtime_map(
                    &control_plane_rpc_catalogue_snapshot(),
                ),
            )?;
        }
        ControlPlaneRpcKind::PendingMetadataCommandRecoveries => {
            write_pending_metadata_command_recovery_listing(
                &mut payload,
                &control_plane_rpc_catalogue_recoveries(),
            )?;
        }
        ControlPlaneRpcKind::RefreshNodeHeartbeat => {
            write_heartbeat_lease_summary(
                &mut payload,
                &HeartbeatLease {
                    authority_incarnation: AuthorityIncarnation::new(21).unwrap(),
                    cluster_epoch: ClusterEpoch::new(22).unwrap(),
                    node_id: NodeId::new(3),
                    lease_deadline_ms: 23,
                    serving: true,
                    snapshot: ClusterControlSnapshot::empty(),
                },
            );
            write_runtime_map_snapshot(&mut payload, &control_plane_rpc_catalogue_snapshot())?;
        }
        ControlPlaneRpcKind::SetPgActingSet
        | ControlPlaneRpcKind::SetPgActingSetWithMetadataTransfer => {
            write_u64(&mut payload, 24);
        }
        ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap => {
            write_runtime_map_snapshot(&mut payload, &control_plane_rpc_catalogue_snapshot())?;
            write_option_u64(&mut payload, Some(25));
        }
        ControlPlaneRpcKind::TransferRaftLeadership | ControlPlaneRpcKind::TriggerRaftElection => {}
        ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge => {
            write_option_u64(&mut payload, Some(26));
        }
        ControlPlaneRpcKind::AuthorityClockStatus
        | ControlPlaneRpcKind::ReestablishAuthorityClock => {
            write_authority_clock_status(&mut payload, control_plane_rpc_catalogue_clock_status());
        }
    }
    Ok(payload)
}

fn validate_control_plane_rpc_catalogue_success(
    kind: ControlPlaneRpcKind,
    payload: &[u8],
) -> Result<(), ControlPlaneError> {
    let mut reader = PayloadReader::new(payload);
    match kind {
        ControlPlaneRpcKind::RuntimeMapSnapshot
        | ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap
        | ControlPlaneRpcKind::PgRuntimeMapSnapshot
        | ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot => {
            assert_eq!(
                read_runtime_map_snapshot(&mut reader)?,
                control_plane_rpc_catalogue_snapshot()
            );
        }
        ControlPlaneRpcKind::RuntimeMapDiagnostics => {
            assert_eq!(
                read_control_plane_runtime_map_diagnostics(&mut reader)?,
                control_plane_rpc_catalogue_diagnostics()
            );
        }
        ControlPlaneRpcKind::RuntimeMapStatus => {
            assert_eq!(
                read_runtime_map_status(&mut reader)?,
                ControlPlaneRuntimeMapStatus::from_runtime_map(
                    &control_plane_rpc_catalogue_snapshot()
                )
            );
        }
        ControlPlaneRpcKind::PendingMetadataCommandRecoveries => {
            assert_eq!(
                read_pending_metadata_command_recovery_listing(&mut reader)?,
                control_plane_rpc_catalogue_recoveries()
            );
        }
        ControlPlaneRpcKind::RefreshNodeHeartbeat => {
            let lease = read_heartbeat_lease_summary(&mut reader)?;
            assert_eq!(
                lease.authority_incarnation(),
                AuthorityIncarnation::new(21).unwrap()
            );
            assert_eq!(lease.cluster_epoch(), ClusterEpoch::new(22).unwrap());
            assert_eq!(lease.node_id(), NodeId::new(3));
            assert_eq!(lease.lease_deadline_ms(), 23);
            assert!(lease.serving());
            assert_eq!(
                read_runtime_map_snapshot(&mut reader)?,
                control_plane_rpc_catalogue_snapshot()
            );
        }
        ControlPlaneRpcKind::SetPgActingSet
        | ControlPlaneRpcKind::SetPgActingSetWithMetadataTransfer => {
            assert_eq!(reader.read_u64()?, 24);
        }
        ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap => {
            assert_eq!(
                read_runtime_map_snapshot(&mut reader)?,
                control_plane_rpc_catalogue_snapshot()
            );
            assert_eq!(reader.read_option_u64()?, Some(25));
        }
        ControlPlaneRpcKind::TransferRaftLeadership | ControlPlaneRpcKind::TriggerRaftElection => {}
        ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge => {
            assert_eq!(reader.read_option_u64()?, Some(26));
        }
        ControlPlaneRpcKind::AuthorityClockStatus
        | ControlPlaneRpcKind::ReestablishAuthorityClock => {
            assert_eq!(
                read_authority_clock_status(&mut reader)?,
                control_plane_rpc_catalogue_clock_status()
            );
        }
    }
    reader.finish()
}

fn control_plane_rpc_catalogue_errors() -> Vec<ControlPlaneError> {
    let mut errors = vec![
        ControlPlaneError::rpc_protocol("catalogue generic rejection".to_owned()),
        ControlPlaneError::PgPeeringPendingMetadataCommand {
            pg_id: 1,
            node_id: 2,
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pending: PendingMetadataCommandObservation::new(
                ClusterEpoch::new(4).unwrap(),
                NonZeroU64::new(5).unwrap(),
                6,
            ),
        },
        ControlPlaneError::PgMetadataMigrationSourceNotReady {
            pg_id: 7,
            cluster_epoch: ClusterEpoch::new(8).unwrap(),
        },
        ControlPlaneError::PgHasNoServingPrimary {
            pg_id: 9,
            cluster_epoch: ClusterEpoch::new(10).unwrap(),
        },
        ControlPlaneError::PgPrimaryMissingActiveObservation {
            pg_id: 11,
            node_id: 12,
            cluster_epoch: ClusterEpoch::new(13).unwrap(),
        },
        ControlPlaneError::PgPrimaryObservationNotActive {
            pg_id: 14,
            node_id: 15,
            cluster_epoch: ClusterEpoch::new(16).unwrap(),
            state: PgState::Degraded,
        },
        ControlPlaneError::PgActingSetChangeNotReady {
            pg_id: 17,
            cluster_epoch: ClusterEpoch::new(18).unwrap(),
            state: PgState::Inconsistent,
        },
        ControlPlaneError::UnknownPg { pg_id: 19 },
        ControlPlaneError::AuthorityNotServing,
        ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority,
        ControlPlaneError::UnknownNode { node_id: 20 },
        ControlPlaneError::UnknownActingSetNode {
            pg_id: 21,
            node_id: 22,
        },
        ControlPlaneError::AuthorityClockLeadershipChanged {
            established_term: None,
            current_term: 23,
        },
        ControlPlaneError::AuthorityClockLeadershipChanged {
            established_term: Some(24),
            current_term: 25,
        },
        ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
            pg_id: 26,
            expected_destination_epoch: ClusterEpoch::new(27).unwrap(),
            actual_destination_epoch: ClusterEpoch::new(28).unwrap(),
        },
    ];
    errors.extend(
        CONTROL_PLANE_RPC_CATALOGUE_OPENRAFT_ERROR_KINDS
            .into_iter()
            .enumerate()
            .map(|(index, kind)| ControlPlaneError::OpenRaftOperation {
                kind,
                message: format!("catalogue OpenRaft rejection {index}"),
            }),
    );
    errors
}

#[derive(Debug, PartialEq, Eq)]
enum ControlPlaneRpcCatalogueRejection {
    Remote(String),
    PendingMetadataCommand {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        pending: PendingMetadataCommandObservation,
    },
    MetadataMigrationSourceNotReady {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },
    NoServingPrimary {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },
    PrimaryMissingActiveObservation {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    },
    PrimaryObservationNotActive {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },
    ActingSetChangeNotReady {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },
    UnknownPg(u32),
    OpenRaft {
        kind: ControlPlaneRaftOperationErrorKind,
        message: String,
    },
    AuthorityNotServing,
    AuthorityClockNotLocalServingRaftAuthority,
    UnknownNode(u32),
    UnknownActingSetNode {
        pg_id: u32,
        node_id: u32,
    },
    AuthorityClockLeadershipChanged {
        established_term: Option<u64>,
        current_term: u64,
    },
    MetadataTransferDestinationEpochMismatch {
        pg_id: u32,
        expected_destination_epoch: ClusterEpoch,
        actual_destination_epoch: ClusterEpoch,
    },
}

#[derive(Clone, Copy)]
enum ControlPlaneRpcCatalogueRejectionSide {
    Expected,
    Decoded,
}

fn control_plane_rpc_catalogue_rejection(
    side: ControlPlaneRpcCatalogueRejectionSide,
    error: &ControlPlaneError,
) -> ControlPlaneRpcCatalogueRejection {
    match (side, error) {
        (
            ControlPlaneRpcCatalogueRejectionSide::Expected,
            ControlPlaneError::RpcProtocol { .. },
        ) => ControlPlaneRpcCatalogueRejection::Remote(error.retained_diagnostic_message()),
        (
            ControlPlaneRpcCatalogueRejectionSide::Decoded,
            ControlPlaneError::RpcRemote { diagnostic },
        ) => ControlPlaneRpcCatalogueRejection::Remote(diagnostic.as_str().to_owned()),
        (
            _,
            ControlPlaneError::PgPeeringPendingMetadataCommand {
                pg_id,
                node_id,
                cluster_epoch,
                pending,
            },
        ) => ControlPlaneRpcCatalogueRejection::PendingMetadataCommand {
            pg_id: *pg_id,
            node_id: *node_id,
            cluster_epoch: *cluster_epoch,
            pending: *pending,
        },
        (
            _,
            ControlPlaneError::PgMetadataMigrationSourceNotReady {
                pg_id,
                cluster_epoch,
            },
        ) => ControlPlaneRpcCatalogueRejection::MetadataMigrationSourceNotReady {
            pg_id: *pg_id,
            cluster_epoch: *cluster_epoch,
        },
        (
            _,
            ControlPlaneError::PgHasNoServingPrimary {
                pg_id,
                cluster_epoch,
            },
        ) => ControlPlaneRpcCatalogueRejection::NoServingPrimary {
            pg_id: *pg_id,
            cluster_epoch: *cluster_epoch,
        },
        (
            _,
            ControlPlaneError::PgPrimaryMissingActiveObservation {
                pg_id,
                node_id,
                cluster_epoch,
            },
        ) => ControlPlaneRpcCatalogueRejection::PrimaryMissingActiveObservation {
            pg_id: *pg_id,
            node_id: *node_id,
            cluster_epoch: *cluster_epoch,
        },
        (
            _,
            ControlPlaneError::PgPrimaryObservationNotActive {
                pg_id,
                node_id,
                cluster_epoch,
                state,
            },
        ) => ControlPlaneRpcCatalogueRejection::PrimaryObservationNotActive {
            pg_id: *pg_id,
            node_id: *node_id,
            cluster_epoch: *cluster_epoch,
            state: *state,
        },
        (
            _,
            ControlPlaneError::PgActingSetChangeNotReady {
                pg_id,
                cluster_epoch,
                state,
            },
        ) => ControlPlaneRpcCatalogueRejection::ActingSetChangeNotReady {
            pg_id: *pg_id,
            cluster_epoch: *cluster_epoch,
            state: *state,
        },
        (_, ControlPlaneError::UnknownPg { pg_id }) => {
            ControlPlaneRpcCatalogueRejection::UnknownPg(*pg_id)
        }
        (_, ControlPlaneError::OpenRaftOperation { kind, message }) => {
            ControlPlaneRpcCatalogueRejection::OpenRaft {
                kind: *kind,
                message: message.clone(),
            }
        }
        (_, ControlPlaneError::AuthorityNotServing) => {
            ControlPlaneRpcCatalogueRejection::AuthorityNotServing
        }
        (_, ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority) => {
            ControlPlaneRpcCatalogueRejection::AuthorityClockNotLocalServingRaftAuthority
        }
        (_, ControlPlaneError::UnknownNode { node_id }) => {
            ControlPlaneRpcCatalogueRejection::UnknownNode(*node_id)
        }
        (_, ControlPlaneError::UnknownActingSetNode { pg_id, node_id }) => {
            ControlPlaneRpcCatalogueRejection::UnknownActingSetNode {
                pg_id: *pg_id,
                node_id: *node_id,
            }
        }
        (
            _,
            ControlPlaneError::AuthorityClockLeadershipChanged {
                established_term,
                current_term,
            },
        ) => ControlPlaneRpcCatalogueRejection::AuthorityClockLeadershipChanged {
            established_term: *established_term,
            current_term: *current_term,
        },
        (
            _,
            ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
                pg_id,
                expected_destination_epoch,
                actual_destination_epoch,
            },
        ) => ControlPlaneRpcCatalogueRejection::MetadataTransferDestinationEpochMismatch {
            pg_id: *pg_id,
            expected_destination_epoch: *expected_destination_epoch,
            actual_destination_epoch: *actual_destination_epoch,
        },
        (side, error) => panic!(
            "unexpected {side} control-plane RPC catalogue rejection: {error:?}",
            side = match side {
                ControlPlaneRpcCatalogueRejectionSide::Expected => "expected",
                ControlPlaneRpcCatalogueRejectionSide::Decoded => "decoded",
            }
        ),
    }
}

fn assert_control_plane_rpc_catalogue_registries_are_complete() {
    let response_statuses = (0..=u8::MAX)
        .filter_map(|tag| ControlPlaneRpcResponseStatus::from_u8(tag).ok())
        .collect::<Vec<_>>();
    assert_eq!(response_statuses, ControlPlaneRpcResponseStatus::ALL);

    let freshness_proof_kinds = (0..=u8::MAX)
        .filter_map(|tag| ControlPlaneRpcRuntimeMapFreshnessProofKind::from_u8(tag).ok())
        .collect::<Vec<_>>();
    assert_eq!(
        freshness_proof_kinds,
        ControlPlaneRpcRuntimeMapFreshnessProofKind::ALL
    );

    let pg_states = (0..=u8::MAX)
        .filter_map(|tag| {
            let bytes = [tag];
            let mut reader = PayloadReader::new(&bytes);
            read_pg_state(&mut reader).ok()
        })
        .collect::<Vec<_>>();
    assert_eq!(pg_states, CONTROL_PLANE_RPC_CATALOGUE_PG_STATES);

    let history_kinds = (0..=u8::MAX)
        .filter_map(|tag| {
            let bytes = [tag];
            let mut reader = PayloadReader::new(&bytes);
            read_cluster_map_history_route_reference_kind(&mut reader).ok()
        })
        .collect::<Vec<_>>();
    assert_eq!(history_kinds, CONTROL_PLANE_RPC_CATALOGUE_HISTORY_KINDS);

    let metric_kinds = (0..=u8::MAX)
        .filter_map(|tag| read_control_plane_rpc_metric_kind(tag).ok())
        .collect::<Vec<_>>();
    assert_eq!(metric_kinds, CONTROL_PLANE_RPC_CATALOGUE_METRIC_KINDS);

    let recovery_failure_kinds = (0..=u8::MAX)
        .filter_map(|tag| PendingMetadataCommandRecoveryDiscoveryFailureKind::from_u8(tag).ok())
        .collect::<Vec<_>>();
    assert_eq!(
        recovery_failure_kinds,
        CONTROL_PLANE_RPC_CATALOGUE_RECOVERY_FAILURE_KINDS
    );

    let openraft_error_kinds = (0..=u8::MAX)
        .filter_map(|tag| ControlPlaneRaftOperationErrorKind::from_wire_tag(tag).ok())
        .collect::<Vec<_>>();
    assert_eq!(
        openraft_error_kinds,
        CONTROL_PLANE_RPC_CATALOGUE_OPENRAFT_ERROR_KINDS
    );

    let blocked_reasons = (0..=u8::MAX)
        .filter_map(|tag| {
            let bytes = [tag];
            let mut reader = PayloadReader::new(&bytes);
            read_authority_clock_blocked_reason(&mut reader).ok()
        })
        .collect::<Vec<_>>();
    assert_eq!(blocked_reasons, CONTROL_PLANE_RPC_CATALOGUE_BLOCKED_REASONS);
}

fn append_control_plane_rpc_catalogue_frame(
    aggregate: &mut Vec<u8>,
    section: u8,
    kind: ControlPlaneRpcKind,
    payload: &[u8],
) {
    let frame = encode_control_plane_rpc_frame(kind, payload).unwrap();
    write_u8(aggregate, section);
    write_u32(aggregate, u32::try_from(frame.len()).unwrap());
    aggregate.extend_from_slice(&frame);
}

fn append_control_plane_rpc_catalogue_success(
    aggregate: &mut Vec<u8>,
    kind: ControlPlaneRpcKind,
    success: Vec<u8>,
) {
    let response = encode_control_plane_rpc_response(Ok(success.clone())).unwrap();
    assert_eq!(
        decode_control_plane_rpc_response(response.clone()).unwrap(),
        success
    );
    append_control_plane_rpc_catalogue_frame(aggregate, 2, kind, &response);
}

fn append_control_plane_rpc_catalogue_invalid_runtime_map(
    aggregate: &mut Vec<u8>,
    snapshot: &ClusterRuntimeMapSnapshot,
    expected_diagnostic: &str,
) {
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, snapshot).unwrap();
    let mut reader = PayloadReader::new(&payload);
    let error = read_runtime_map_snapshot(&mut reader).unwrap_err();
    assert!(
        error
            .retained_diagnostic_message()
            .contains(expected_diagnostic),
        "unexpected invalid runtime-map diagnostic: {error:?}"
    );
    let response = encode_control_plane_rpc_response(Ok(payload)).unwrap();
    append_control_plane_rpc_catalogue_frame(
        aggregate,
        4,
        ControlPlaneRpcKind::RuntimeMapSnapshot,
        &response,
    );
}

#[test]
fn control_plane_rpc_v14_operation_catalogue_remains_rejected_evidence() {
    const AGGREGATE: &[u8] = include_bytes!("../testdata/rpc_v14_operation.aggregate");
    assert_eq!(
        (
            AGGREGATE.len(),
            hex_encode(&checksum::sha256::digest(AGGREGATE))
        ),
        (
            12_786,
            "f100f7b2f763b0c40cc70ad8d52bf54e853bb4fb67fb20f98f86302f6cca07cc".to_owned()
        )
    );
    let mut remaining = AGGREGATE;
    let mut count = 0usize;
    while !remaining.is_empty() {
        let (_section, tail) = remaining.split_first().unwrap();
        let (raw_len, tail) = tail.split_at(4);
        let len = usize::try_from(u32::from_be_bytes(raw_len.try_into().unwrap())).unwrap();
        let (frame, tail) = tail.split_at(len);
        let error = read_control_plane_rpc_frame(&mut std::io::Cursor::new(frame)).unwrap_err();
        assert!(matches!(
            error,
            ControlPlaneError::RpcProtocol { diagnostic }
                if diagnostic.as_str() == "unsupported control-plane RPC version 14"
        ));
        remaining = tail;
        count += 1;
    }
    assert!(count > ControlPlaneRpcKind::ALL.len());
}

#[test]
fn control_plane_rpc_v15_operation_catalogue_is_exact() {
    assert_control_plane_rpc_catalogue_registries_are_complete();
    let decoded_kinds = (0..=u16::MAX)
        .filter_map(|raw| ControlPlaneRpcKind::from_u16(raw).ok())
        .collect::<Vec<_>>();
    assert_eq!(decoded_kinds, ControlPlaneRpcKind::ALL);
    let runtime_map_snapshots = [
        control_plane_rpc_catalogue_snapshot(),
        control_plane_rpc_catalogue_transfer_snapshot(),
        control_plane_rpc_catalogue_recovery_snapshot(),
        control_plane_rpc_catalogue_empty_snapshot(),
    ];
    assert_control_plane_rpc_catalogue_runtime_map_branches(&runtime_map_snapshots);
    assert_eq!(
        runtime_map_snapshots
            .iter()
            .map(|snapshot| control_plane_rpc_catalogue_freshness_kind(snapshot.freshness_proof()))
            .collect::<BTreeSet<_>>(),
        ControlPlaneRpcRuntimeMapFreshnessProofKind::ALL
            .into_iter()
            .collect::<BTreeSet<_>>()
    );

    let mut aggregate = Vec::new();
    let mut response_statuses = BTreeSet::new();
    for kind in ControlPlaneRpcKind::ALL {
        let request = control_plane_rpc_catalogue_request(kind).unwrap();
        validate_control_plane_rpc_catalogue_request(kind, &request).unwrap();
        append_control_plane_rpc_catalogue_frame(&mut aggregate, 1, kind, &request);

        let success = control_plane_rpc_catalogue_success(kind).unwrap();
        validate_control_plane_rpc_catalogue_success(kind, &success).unwrap();
        response_statuses.insert(ControlPlaneRpcResponseStatus::Success.as_u8());
        append_control_plane_rpc_catalogue_success(&mut aggregate, kind, success);
    }

    for snapshot in &runtime_map_snapshots[1..] {
        let mut payload = Vec::new();
        write_runtime_map_snapshot(&mut payload, snapshot).unwrap();
        let mut reader = PayloadReader::new(&payload);
        assert_eq!(read_runtime_map_snapshot(&mut reader).unwrap(), *snapshot);
        reader.finish().unwrap();
        append_control_plane_rpc_catalogue_success(
            &mut aggregate,
            ControlPlaneRpcKind::RuntimeMapSnapshot,
            payload,
        );
    }

    let mut unbounded = control_plane_rpc_catalogue_snapshot();
    unbounded.validity = RouteMapValidity::Forever;
    append_control_plane_rpc_catalogue_invalid_runtime_map(
        &mut aggregate,
        &unbounded,
        "validity must be bounded",
    );

    for (missing_field, expected_diagnostic) in [
        (0, "missing its destination epoch"),
        (1, "incomplete metadata transfer source route"),
        (2, "incomplete metadata transfer source route"),
    ] {
        let mut incomplete_transfer = control_plane_rpc_catalogue_transfer_snapshot();
        match missing_field {
            0 => {
                incomplete_transfer.pg_routes[0].peering_metadata_transfer_destination_epoch = None;
            }
            1 => {
                incomplete_transfer.pg_routes[0].peering_metadata_transfer_source_route_epoch =
                    None;
            }
            2 => {
                incomplete_transfer.pg_routes[0].peering_metadata_transfer_source_node_id = None;
            }
            _ => unreachable!(),
        }
        append_control_plane_rpc_catalogue_invalid_runtime_map(
            &mut aggregate,
            &incomplete_transfer,
            expected_diagnostic,
        );
    }

    let alternate_diagnostics = control_plane_rpc_catalogue_diagnostics_alternate_options();
    let mut alternate_diagnostics_payload = Vec::new();
    write_control_plane_runtime_map_diagnostics_value(
        &mut alternate_diagnostics_payload,
        &alternate_diagnostics,
    )
    .unwrap();
    let mut reader = PayloadReader::new(&alternate_diagnostics_payload);
    assert_eq!(
        read_control_plane_runtime_map_diagnostics(&mut reader).unwrap(),
        alternate_diagnostics
    );
    reader.finish().unwrap();
    append_control_plane_rpc_catalogue_success(
        &mut aggregate,
        ControlPlaneRpcKind::RuntimeMapDiagnostics,
        alternate_diagnostics_payload,
    );

    let mut no_snapshot_index = Vec::new();
    write_option_u64(&mut no_snapshot_index, None);
    let mut reader = PayloadReader::new(&no_snapshot_index);
    assert_eq!(reader.read_option_u64().unwrap(), None);
    reader.finish().unwrap();
    append_control_plane_rpc_catalogue_success(
        &mut aggregate,
        ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge,
        no_snapshot_index,
    );

    let mut no_source_lease = Vec::new();
    write_runtime_map_snapshot(
        &mut no_source_lease,
        &control_plane_rpc_catalogue_snapshot(),
    )
    .unwrap();
    write_option_u64(&mut no_source_lease, None);
    let mut reader = PayloadReader::new(&no_source_lease);
    assert_eq!(
        read_runtime_map_snapshot(&mut reader).unwrap(),
        control_plane_rpc_catalogue_snapshot()
    );
    assert_eq!(reader.read_option_u64().unwrap(), None);
    reader.finish().unwrap();
    append_control_plane_rpc_catalogue_success(
        &mut aggregate,
        ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap,
        no_source_lease,
    );

    let no_lease_renewal =
        ControlPlaneRuntimeMapStatus::new(ClusterEpoch::new(61).unwrap(), 62, 63);
    let mut status_payload = Vec::new();
    write_runtime_map_status(&mut status_payload, no_lease_renewal).unwrap();
    let mut reader = PayloadReader::new(&status_payload);
    assert_eq!(
        read_runtime_map_status(&mut reader).unwrap(),
        no_lease_renewal
    );
    reader.finish().unwrap();
    append_control_plane_rpc_catalogue_success(
        &mut aggregate,
        ControlPlaneRpcKind::RuntimeMapStatus,
        status_payload,
    );

    for (index, blocked_reason) in CONTROL_PLANE_RPC_CATALOGUE_BLOCKED_REASONS
        .into_iter()
        .enumerate()
    {
        let index = u64::try_from(index).unwrap();
        let status = ControlPlaneAuthorityClockStatus {
            generation: 70 + index,
            established: blocked_reason.is_none(),
            blocked_reason,
            committed_timestamp_high_water_ms: (index % 2 == 0).then_some(80 + index),
            bound_raft_leadership_term: (index % 2 != 0).then_some(90 + index),
            current_raft_leadership_term: (index % 3 == 0).then_some(100 + index),
            local_raft_authority_leader: index % 2 == 0,
            local_raft_authority_serving: index % 2 != 0,
        };
        let mut payload = Vec::new();
        write_authority_clock_status(&mut payload, status);
        let mut reader = PayloadReader::new(&payload);
        assert_eq!(read_authority_clock_status(&mut reader).unwrap(), status);
        reader.finish().unwrap();
        append_control_plane_rpc_catalogue_success(
            &mut aggregate,
            ControlPlaneRpcKind::AuthorityClockStatus,
            payload,
        );
    }

    for error in control_plane_rpc_catalogue_errors() {
        let expected = control_plane_rpc_catalogue_rejection(
            ControlPlaneRpcCatalogueRejectionSide::Expected,
            &error,
        );
        let response = encode_control_plane_rpc_response(Err(error)).unwrap();
        response_statuses.insert(response[0]);
        let decoded = decode_control_plane_rpc_response(response.clone()).unwrap_err();
        assert_eq!(
            control_plane_rpc_catalogue_rejection(
                ControlPlaneRpcCatalogueRejectionSide::Decoded,
                &decoded,
            ),
            expected
        );
        append_control_plane_rpc_catalogue_frame(
            &mut aggregate,
            3,
            ControlPlaneRpcKind::RuntimeMapStatus,
            &response,
        );
    }
    assert_eq!(
        response_statuses.into_iter().collect::<Vec<_>>(),
        ControlPlaneRpcResponseStatus::ALL
            .into_iter()
            .map(ControlPlaneRpcResponseStatus::as_u8)
            .collect::<Vec<_>>()
    );

    assert_eq!(
        (
            aggregate.len(),
            hex_encode(&checksum::sha256::digest(&aggregate))
        ),
        (
            12_794,
            "662eda24308716d84b41f5e1904103affccc76d2344d1a37ec715db256d22958".to_owned()
        )
    );
}

#[test]
fn authenticated_control_plane_rpc_v14_and_v15_auth_v1_payload_bindings_are_exact() {
    assert_eq!(CONTROL_PLANE_RPC_VERSION, 15);
    let kind = ControlPlaneRpcKind::RuntimeMapStatus;
    let credential = frontend_auth_credential("auth-cluster", "frontend-1");
    let verifier = frontend_auth_verifier("auth-cluster", "frontend-1");
    let request = signed_frontend_runtime_map_request(
        kind,
        &credential,
        Vec::new(),
        Some(1_000),
        Some(6_000),
    );
    assert!(request.payload.starts_with(b"ARGCPAUT\x00\x01"));
    let request_envelope =
        ControlPlaneAuthEnvelope::decode_frame(&request.payload, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)
            .unwrap();
    assert_eq!(request_envelope.payload(), &[0x00, 0x0c]);
    let request_frame = encode_control_plane_rpc_frame(kind, &request.payload).unwrap();
    assert!(request_frame.starts_with(b"argmin-control-plane-rpc\x00\x0f"));
    let previous_request_frame =
        encode_control_plane_rpc_frame_with_version(kind, &request.payload, 14).unwrap();
    assert_eq!(
        (
            previous_request_frame.len(),
            hex_encode(&checksum::sha256::digest(&previous_request_frame))
        ),
        (
            182,
            "8e6ced7925437cef8aae609508634cdcd86e76088d8e4b5816654f77652f4419".to_owned()
        )
    );
    let verified = verify_control_plane_unix_request(request, Some(&verifier), 1_000).unwrap();
    assert_eq!(verified.kind, kind);
    assert!(verified.payload.is_empty());
    assert!(verified.response_auth.is_some());

    let logical_response = vec![0xa5, 0x5a];
    let encoded_response = encode_control_plane_rpc_response(Ok(logical_response.clone())).unwrap();
    let response =
        signed_runtime_map_response_payload(kind, &credential, logical_response.clone(), 1_001);
    let response_envelope =
        ControlPlaneAuthEnvelope::decode_frame(&response, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)
            .unwrap();
    assert_eq!(
        response_envelope.payload(),
        &[0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x02, 0xa5, 0x5a]
    );
    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new("unused-test-socket"),
        credential,
    );
    assert_eq!(
        client
            .verify_runtime_map_response(kind, 1_001, &response)
            .unwrap(),
        encoded_response
    );
    let response_frame = encode_control_plane_rpc_frame(kind, &response).unwrap();
    let previous_response_frame =
        encode_control_plane_rpc_frame_with_version(kind, &response, 14).unwrap();
    assert_eq!(
        (
            previous_response_frame.len(),
            hex_encode(&checksum::sha256::digest(&previous_response_frame))
        ),
        (
            190,
            "72f5e379703b62bcd438281d63ac0dcc9c937400004d8c75e4014bb43b1128b7".to_owned()
        )
    );

    assert_eq!(
        (
            request_frame.len(),
            hex_encode(&checksum::sha256::digest(&request_frame))
        ),
        (
            182,
            "37645109ed5bf0542da0101873aebdfffd5f2eae42bd51d2b4a7956cb31584da".to_owned()
        )
    );
    assert_eq!(
        (
            response_frame.len(),
            hex_encode(&checksum::sha256::digest(&response_frame))
        ),
        (
            190,
            "ae755056e5f6f4308f6e28c788cdfcd204f74e8d383bf0188cbb08b7873bce9d".to_owned()
        )
    );
}

#[test]
fn unix_control_plane_client_fetches_runtime_map() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_until_serving_with_endpoint(
        &mut authority,
        1,
        1_000,
        "/tmp/argmin-node-1.sock".to_owned(),
    );
    let expected_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream(&mut authority, &mut stream, 1_001).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let runtime_map = client.runtime_map_snapshot(0).unwrap();

    server.join().unwrap();
    assert_eq!(runtime_map.cluster_epoch(), expected_epoch);
    assert_eq!(runtime_map.nodes().len(), 1);
    assert_eq!(runtime_map.nodes()[0].node_id(), NodeId::new(1));
    assert_eq!(runtime_map.nodes()[0].endpoint(), "/tmp/argmin-node-1.sock");
    assert_eq!(runtime_map.pg_routes().len(), 1);
    assert_eq!(runtime_map.pg_routes()[0].pg_id(), PgId::new(7));
    assert_eq!(runtime_map.pg_routes()[0].state(), PgState::Peering);
    assert_eq!(
        runtime_map.freshness_proof(),
        &RuntimeMapFreshnessProof::SingleAuthority {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            issued_at_ms: 1_001,
        }
    );
}

#[test]
fn unix_control_plane_client_routes_around_dead_and_follower_endpoints() {
    let tmp = test_util::tempdir();
    let dead_socket_path = tmp.path().join("dead.sock");
    let follower_socket_path = tmp.path().join("follower.sock");
    let leader_socket_path = tmp.path().join("leader.sock");
    let follower_listener = std::os::unix::net::UnixListener::bind(&follower_socket_path).unwrap();
    let leader_listener = std::os::unix::net::UnixListener::bind(&leader_socket_path).unwrap();
    let mut leader_authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
        tmp.path().join("leader.state"),
    ))
    .unwrap();
    leader_authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let follower = std::thread::spawn(move || {
        let (mut stream, _addr) = follower_listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        let response = ControlPlaneRpcResponse {
            kind: request.kind,
            payload: encode_control_plane_rpc_response(Err(ControlPlaneError::AuthorityNotServing))
                .unwrap(),
        };
        write_control_plane_unix_response(&mut stream, response).unwrap();
    });
    let leader = std::thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _addr) = leader_listener.accept().unwrap();
            handle_control_plane_unix_stream(&mut leader_authority, &mut stream, 2_000).unwrap();
        }
    });

    let client = UnixControlPlaneClient::with_socket_paths([
        dead_socket_path,
        follower_socket_path,
        leader_socket_path,
    ])
    .unwrap();
    let first = client.runtime_map_snapshot(0).unwrap();
    let second = client.runtime_map_snapshot(0).unwrap();

    follower.join().unwrap();
    leader.join().unwrap();
    assert_eq!(first.cluster_epoch(), ClusterEpoch::new(2).unwrap());
    assert_eq!(second.cluster_epoch(), ClusterEpoch::new(2).unwrap());
    assert_eq!(client.preferred_endpoint_index(), 2);
}

#[test]
fn authenticated_unix_control_plane_client_routes_after_verified_follower_rejection() {
    let tmp = test_util::tempdir();
    let follower_1_socket_path = tmp.path().join("follower-1.sock");
    let follower_2_socket_path = tmp.path().join("follower-2.sock");
    let leader_socket_path = tmp.path().join("leader.sock");
    let follower_1_listener =
        std::os::unix::net::UnixListener::bind(&follower_1_socket_path).unwrap();
    let follower_2_listener =
        std::os::unix::net::UnixListener::bind(&follower_2_socket_path).unwrap();
    let leader_listener = std::os::unix::net::UnixListener::bind(&leader_socket_path).unwrap();
    let credential = frontend_auth_credential("auth-cluster", "frontend-1");
    let mut leader_authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
        tmp.path().join("leader.state"),
    ))
    .unwrap();
    leader_authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let spawn_follower = |listener: std::os::unix::net::UnixListener,
                          signer: ControlPlaneScopedCredential| {
        std::thread::spawn(move || {
            let (mut stream, _addr) = listener.accept().unwrap();
            let (kind, _payload) = read_control_plane_rpc_frame(&mut stream).unwrap();
            let response =
                encode_control_plane_rpc_response(Err(ControlPlaneError::AuthorityNotServing))
                    .unwrap();
            let response = sign_control_plane_response_payload(
                kind,
                &signer
                    .runtime_map_response_credential_for_frontend()
                    .unwrap(),
                signer.principal().clone(),
                ControlPlaneAuthOperation::RuntimeMapResponse,
                2_000,
                response,
            )
            .unwrap();
            write_control_plane_rpc_frame(&mut stream, kind, &response).unwrap();
        })
    };
    let follower_1 = spawn_follower(follower_1_listener, credential.clone());
    let follower_2 = spawn_follower(follower_2_listener, credential.clone());
    let leader = std::thread::spawn(move || {
        let verifier = frontend_auth_verifier("auth-cluster", "frontend-1");
        let (mut stream, _addr) = leader_listener.accept().unwrap();
        handle_control_plane_unix_stream_with_auth(
            &mut leader_authority,
            &mut stream,
            2_000,
            &verifier,
        )
        .unwrap();
    });

    let inner = UnixControlPlaneClient::with_socket_paths([
        follower_1_socket_path,
        follower_2_socket_path,
        leader_socket_path,
    ])
    .unwrap();
    let client = AuthenticatedUnixControlPlaneClient::new(inner, credential);
    let payload = client
        .send_signed_read_only_request_with_read_timeout_and_clock(
            ControlPlaneRpcKind::RuntimeMapSnapshot,
            Vec::new(),
            CONTROL_PLANE_RPC_IO_TIMEOUT,
            || Ok(2_000),
        )
        .unwrap();
    let mut reader = PayloadReader::new(&payload);
    let runtime_map = read_runtime_map_snapshot(&mut reader).unwrap();
    reader.finish().unwrap();

    follower_1.join().unwrap();
    follower_2.join().unwrap();
    leader.join().unwrap();
    assert_eq!(runtime_map.cluster_epoch(), ClusterEpoch::new(2).unwrap());
    assert_eq!(client.inner().preferred_endpoint_index(), 2);
}

#[test]
fn authenticated_unix_control_plane_client_rejects_unverified_follower_routing_error() {
    let tmp = test_util::tempdir();
    let follower_socket_path = tmp.path().join("follower.sock");
    let follower_listener = std::os::unix::net::UnixListener::bind(&follower_socket_path).unwrap();
    let credential = frontend_auth_credential("auth-cluster", "frontend-1");
    let follower = std::thread::spawn(move || {
        let (mut stream, _addr) = follower_listener.accept().unwrap();
        let (kind, _payload) = read_control_plane_rpc_frame(&mut stream).unwrap();
        let response =
            encode_control_plane_rpc_response(Err(ControlPlaneError::AuthorityNotServing)).unwrap();
        write_control_plane_rpc_frame(&mut stream, kind, &response).unwrap();
    });

    let inner = UnixControlPlaneClient::new(follower_socket_path);
    let client = AuthenticatedUnixControlPlaneClient::new(inner, credential);
    let error = client
        .send_signed_read_only_request_with_read_timeout_and_clock(
            ControlPlaneRpcKind::RuntimeMapSnapshot,
            Vec::new(),
            CONTROL_PLANE_RPC_IO_TIMEOUT,
            || Ok(2_000),
        )
        .unwrap_err();

    follower.join().unwrap();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("authentication envelope")
                || message.contains("truncated")
                || message.contains("magic")
    ));
}

#[test]
fn control_plane_rpc_preserves_pending_metadata_command_error_identity() {
    let pending = PendingMetadataCommandObservation::new(
        ClusterEpoch::new(41).unwrap(),
        NonZeroU64::new(17).unwrap(),
        0xfeed_beef,
    );
    let encoded = encode_control_plane_rpc_response(Err(
        ControlPlaneError::PgPeeringPendingMetadataCommand {
            pg_id: 7,
            node_id: 3,
            cluster_epoch: ClusterEpoch::new(44).unwrap(),
            pending,
        },
    ))
    .unwrap();

    assert!(matches!(
        decode_control_plane_rpc_response(encoded),
        Err(ControlPlaneError::PgPeeringPendingMetadataCommand {
            pg_id: 7,
            node_id: 3,
            cluster_epoch,
            pending: decoded,
        }) if cluster_epoch == ClusterEpoch::new(44).unwrap() && decoded == pending
    ));
}

#[test]
fn control_plane_rpc_preserves_runtime_map_serving_gap_error_identities() {
    let cluster_epoch = ClusterEpoch::new(44).unwrap();

    let encoded =
        encode_control_plane_rpc_response(Err(ControlPlaneError::PgHasNoServingPrimary {
            pg_id: 7,
            cluster_epoch,
        }))
        .unwrap();
    assert!(matches!(
        decode_control_plane_rpc_response(encoded),
        Err(ControlPlaneError::PgHasNoServingPrimary {
            pg_id: 7,
            cluster_epoch: decoded_epoch,
        }) if decoded_epoch == cluster_epoch
    ));

    let encoded = encode_control_plane_rpc_response(Err(
        ControlPlaneError::PgPrimaryMissingActiveObservation {
            pg_id: 7,
            node_id: 3,
            cluster_epoch,
        },
    ))
    .unwrap();
    assert!(matches!(
        decode_control_plane_rpc_response(encoded),
        Err(ControlPlaneError::PgPrimaryMissingActiveObservation {
            pg_id: 7,
            node_id: 3,
            cluster_epoch: decoded_epoch,
        }) if decoded_epoch == cluster_epoch
    ));

    let encoded =
        encode_control_plane_rpc_response(Err(ControlPlaneError::PgPrimaryObservationNotActive {
            pg_id: 7,
            node_id: 3,
            cluster_epoch,
            state: PgState::Peering,
        }))
        .unwrap();
    assert!(matches!(
        decode_control_plane_rpc_response(encoded),
        Err(ControlPlaneError::PgPrimaryObservationNotActive {
            pg_id: 7,
            node_id: 3,
            cluster_epoch: decoded_epoch,
            state: PgState::Peering,
        }) if decoded_epoch == cluster_epoch
    ));
}

#[test]
fn control_plane_rpc_preserves_authority_not_serving_identity() {
    let encoded =
        encode_control_plane_rpc_response(Err(ControlPlaneError::AuthorityNotServing)).unwrap();

    let error = decode_control_plane_rpc_response(encoded).unwrap_err();
    assert!(matches!(&error, ControlPlaneError::AuthorityNotServing));
    assert!(error.is_control_plane_leader_routing_rejection());

    let encoded = encode_control_plane_rpc_response(Err(
        ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority,
    ))
    .unwrap();
    let error = decode_control_plane_rpc_response(encoded).unwrap_err();
    assert!(matches!(
        &error,
        ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority
    ));
    assert!(error.is_control_plane_leader_routing_rejection());
}

#[test]
fn control_plane_rpc_preserves_authority_clock_leadership_change_identity() {
    for established_term in [None, Some(41)] {
        let encoded = encode_control_plane_rpc_response(Err(
            ControlPlaneError::AuthorityClockLeadershipChanged {
                established_term,
                current_term: 42,
            },
        ))
        .unwrap();

        assert!(matches!(
            decode_control_plane_rpc_response(encoded),
            Err(ControlPlaneError::AuthorityClockLeadershipChanged {
                established_term: decoded_established_term,
                current_term: 42,
            }) if decoded_established_term == established_term
        ));
    }
}

#[test]
fn runtime_map_observation_retry_classification_uses_semantic_errors() {
    let cluster_epoch = ClusterEpoch::new(44).unwrap();
    for error in [
        ControlPlaneError::PgHasNoServingPrimary {
            pg_id: 7,
            cluster_epoch,
        },
        ControlPlaneError::PgPrimaryMissingActiveObservation {
            pg_id: 7,
            node_id: 3,
            cluster_epoch,
        },
        ControlPlaneError::PgPrimaryObservationNotActive {
            pg_id: 7,
            node_id: 3,
            cluster_epoch,
            state: PgState::Peering,
        },
        ControlPlaneError::PgPeeringPendingMetadataCommand {
            pg_id: 7,
            node_id: 3,
            cluster_epoch,
            pending: PendingMetadataCommandObservation::new(
                ClusterEpoch::new(41).unwrap(),
                NonZeroU64::new(17).unwrap(),
                0xfeed_beef,
            ),
        },
        ControlPlaneError::AuthorityNotServing,
        ControlPlaneError::io(
            "read control-plane RPC magic",
            std::io::Error::from(ErrorKind::WouldBlock),
        ),
    ] {
        assert!(
            error.is_retryable_runtime_map_observation_error(),
            "expected semantic retry classification for {error:?}"
        );
    }

    assert!(!ControlPlaneError::rpc_remote(
        "PG 7 has no serving primary in cluster epoch 44".to_owned()
    )
    .is_retryable_runtime_map_observation_error());
    assert!(
        !ControlPlaneError::rpc_protocol("invalid runtime-map response".to_owned())
            .is_retryable_runtime_map_observation_error()
    );
    assert!(!ControlPlaneError::UnknownPg { pg_id: 7 }.is_retryable_runtime_map_observation_error());
    let unconfirmed = ControlPlaneError::RpcUnconfirmed {
        message: "heartbeat retry budget expired".to_owned(),
    };
    assert!(unconfirmed.is_retryable_heartbeat_startup_error());
    assert!(!unconfirmed.is_retryable_runtime_map_observation_error());
}

#[test]
fn higher_layer_failure_diagnostics_are_opaque() {
    const SECRET_DIAGNOSTIC: &str = "implementation detail that must remain storage-owned";

    for error in [
        ControlPlaneError::durability_failure(SECRET_DIAGNOSTIC),
        ControlPlaneError::invariant_failure(SECRET_DIAGNOSTIC),
        ControlPlaneError::startup_timeout(SECRET_DIAGNOSTIC),
        ControlPlaneError::static_topology_failure(SECRET_DIAGNOSTIC),
    ] {
        assert!(!error.to_string().contains(SECRET_DIAGNOSTIC));
        assert!(!format!("{error:?}").contains(SECRET_DIAGNOSTIC));
    }
}

#[test]
fn durability_reclassification_retains_an_opaque_cause() {
    const CLASSIFICATION_CONTEXT: &str = "restart-checkpoint classification context";
    const SOURCE_CONTEXT: &str = "restart-checkpoint source context";
    const SOURCE_DETAIL: &str = "restart-checkpoint source detail";

    let error = ControlPlaneError::io(SOURCE_CONTEXT, std::io::Error::other(SOURCE_DETAIL))
        .into_durability_failure(CLASSIFICATION_CONTEXT);

    assert_eq!(error.to_string(), "control-plane durability is unavailable");
    assert!(!format!("{error:?}").contains(CLASSIFICATION_CONTEXT));
    assert!(!format!("{error:?}").contains(SOURCE_CONTEXT));
    assert!(!format!("{error:?}").contains(SOURCE_DETAIL));
    assert!(std::error::Error::source(&error).is_none());

    let ControlPlaneError::DurabilityFailure { diagnostic } = &error else {
        unreachable!("the error was classified as a durability failure")
    };
    let ControlPlaneFailureDiagnosticKind::ClassifiedCause { context, source } = &diagnostic.0
    else {
        unreachable!("the classified error retains its cause")
    };
    assert_eq!(*context, CLASSIFICATION_CONTEXT);
    let ControlPlaneError::Io { diagnostic } = source.as_ref() else {
        unreachable!("the original I/O failure is retained")
    };
    assert_eq!(diagnostic.context(), SOURCE_CONTEXT);
    assert_eq!(diagnostic.source.to_string(), SOURCE_DETAIL);
}

#[test]
fn durability_reclassification_retains_opaque_diagnostics_across_rpc() {
    const CLASSIFICATION_CONTEXT: &str = "restart-checkpoint classification context";
    const SOURCE_CONTEXT: &str = "restart-checkpoint source context";
    const SOURCE_DETAIL: &str = "restart-checkpoint source detail";

    let encoded = encode_control_plane_rpc_response(Err(ControlPlaneError::io(
        SOURCE_CONTEXT,
        std::io::Error::other(SOURCE_DETAIL),
    )
    .into_durability_failure(CLASSIFICATION_CONTEXT)))
    .unwrap();
    let error = decode_control_plane_rpc_response(encoded).unwrap_err();

    assert_eq!(error.to_string(), "control-plane RPC remote failure");
    assert!(!format!("{error:?}").contains(CLASSIFICATION_CONTEXT));
    assert!(!format!("{error:?}").contains(SOURCE_CONTEXT));
    assert!(!format!("{error:?}").contains(SOURCE_DETAIL));
    assert!(error.retained_diagnostic_contains(CLASSIFICATION_CONTEXT));
    assert!(error.retained_diagnostic_contains(SOURCE_CONTEXT));
    assert!(error.retained_diagnostic_contains(SOURCE_DETAIL));
}

#[test]
fn raw_control_plane_diagnostics_are_opaque() {
    const SECRET_CONTEXT: &str = "secret control-plane transport context";
    const SECRET_SOURCE: &str = "secret control-plane operating-system error";
    const SECRET_RPC: &str = "secret control-plane RPC diagnostic";

    let io_error = ControlPlaneError::io(SECRET_CONTEXT, std::io::Error::other(SECRET_SOURCE));
    let owner_local_diagnostic =
        FileControlPlaneStore::durability_failure_log_message("journal_append", &io_error);
    assert!(owner_local_diagnostic.contains("stage=journal_append"));
    assert!(owner_local_diagnostic.contains(SECRET_CONTEXT));
    assert!(owner_local_diagnostic.contains(SECRET_SOURCE));
    assert!(!io_error.to_string().contains(SECRET_CONTEXT));
    assert!(!io_error.to_string().contains(SECRET_SOURCE));
    assert!(!format!("{io_error:?}").contains(SECRET_CONTEXT));
    assert!(!format!("{io_error:?}").contains(SECRET_SOURCE));
    assert!(std::error::Error::source(&io_error).is_none());
    let ControlPlaneError::Io { diagnostic } = io_error else {
        unreachable!("constructed an I/O error")
    };
    assert!(!diagnostic.to_string().contains(SECRET_CONTEXT));
    assert!(!diagnostic.to_string().contains(SECRET_SOURCE));
    assert!(!format!("{diagnostic:?}").contains(SECRET_CONTEXT));
    assert!(!format!("{diagnostic:?}").contains(SECRET_SOURCE));

    for rpc_error in [
        ControlPlaneError::rpc_protocol(SECRET_RPC),
        ControlPlaneError::rpc_remote(SECRET_RPC),
    ] {
        assert!(!rpc_error.to_string().contains(SECRET_RPC));
        assert!(!format!("{rpc_error:?}").contains(SECRET_RPC));
        assert!(std::error::Error::source(&rpc_error).is_none());
        let diagnostic = match rpc_error {
            ControlPlaneError::RpcProtocol { diagnostic }
            | ControlPlaneError::RpcRemote { diagnostic } => diagnostic,
            _ => unreachable!("constructed an RPC diagnostic error"),
        };
        assert!(!diagnostic.to_string().contains(SECRET_RPC));
        assert!(!format!("{diagnostic:?}").contains(SECRET_RPC));
    }
}

#[test]
fn failed_owner_local_durability_log_observes_published_poison() {
    struct FailedOutput {
        durability: Arc<Mutex<FileControlPlaneStoreDurability>>,
        observed_poison: bool,
    }

    impl std::io::Write for FailedOutput {
        fn write(&mut self, _bytes: &[u8]) -> std::io::Result<usize> {
            let durability = self.durability.try_lock().map_err(|_| {
                std::io::Error::other("durability lock remained held during diagnostic write")
            })?;
            self.observed_poison = durability.poisoned.is_some();
            Err(std::io::Error::new(
                ErrorKind::BrokenPipe,
                "injected diagnostic output failure",
            ))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let error = ControlPlaneError::io(
        "sync single-authority control-plane journal",
        std::io::Error::from(ErrorKind::StorageFull),
    );
    let durability = Arc::new(Mutex::new(FileControlPlaneStoreDurability::default()));
    {
        let mut durability = durability.lock().unwrap();
        FileControlPlaneStore::latch_durability_failure(&mut durability, &error);
    }
    let mut output = FailedOutput {
        durability: Arc::clone(&durability),
        observed_poison: false,
    };
    let write_error =
        FileControlPlaneStore::write_durability_failure("journal_append", &error, &mut output)
            .unwrap_err();

    assert_eq!(write_error.kind(), ErrorKind::BrokenPipe);
    assert!(output.observed_poison);
    assert_eq!(
        durability.lock().unwrap().poisoned.as_deref(),
        Some("control-plane transport I/O failure")
    );
}

#[test]
fn control_plane_rpc_preserves_metadata_migration_source_not_ready_identity() {
    let cluster_epoch = ClusterEpoch::new(44).unwrap();
    let encoded = encode_control_plane_rpc_response(Err(
        ControlPlaneError::PgMetadataMigrationSourceNotReady {
            pg_id: 7,
            cluster_epoch,
        },
    ))
    .unwrap();

    assert!(matches!(
        decode_control_plane_rpc_response(encoded),
        Err(ControlPlaneError::PgMetadataMigrationSourceNotReady {
            pg_id: 7,
            cluster_epoch: decoded_epoch,
        }) if decoded_epoch == cluster_epoch
    ));
}

#[test]
fn control_plane_rpc_preserves_metadata_transfer_destination_epoch_mismatch_identity() {
    let expected_destination_epoch = ClusterEpoch::new(44).unwrap();
    let actual_destination_epoch = ClusterEpoch::new(45).unwrap();
    let encoded = encode_control_plane_rpc_response(Err(
        ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
            pg_id: 7,
            expected_destination_epoch,
            actual_destination_epoch,
        },
    ))
    .unwrap();

    assert!(matches!(
        decode_control_plane_rpc_response(encoded),
        Err(ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
            pg_id: 7,
            expected_destination_epoch: decoded_expected,
            actual_destination_epoch: decoded_actual,
        }) if decoded_expected == expected_destination_epoch
            && decoded_actual == actual_destination_epoch
    ));
}

#[test]
fn control_plane_rpc_preserves_acting_set_change_not_ready_identity() {
    let cluster_epoch = ClusterEpoch::new(44).unwrap();
    let encoded =
        encode_control_plane_rpc_response(Err(ControlPlaneError::PgActingSetChangeNotReady {
            pg_id: 7,
            cluster_epoch,
            state: PgState::Backfilling,
        }))
        .unwrap();

    assert!(matches!(
        decode_control_plane_rpc_response(encoded),
        Err(ControlPlaneError::PgActingSetChangeNotReady {
            pg_id: 7,
            cluster_epoch: decoded_epoch,
            state: PgState::Backfilling,
        }) if decoded_epoch == cluster_epoch
    ));
}

#[test]
fn control_plane_rpc_preserves_unknown_pg_identity() {
    let encoded =
        encode_control_plane_rpc_response(Err(ControlPlaneError::UnknownPg { pg_id: 7 })).unwrap();

    assert!(matches!(
        decode_control_plane_rpc_response(encoded),
        Err(ControlPlaneError::UnknownPg { pg_id: 7 })
    ));

    let encoded =
        encode_control_plane_rpc_response(Err(ControlPlaneError::UnknownNode { node_id: 9 }))
            .unwrap();
    assert!(matches!(
        decode_control_plane_rpc_response(encoded),
        Err(ControlPlaneError::UnknownNode { node_id: 9 })
    ));

    let encoded = encode_control_plane_rpc_response(Err(ControlPlaneError::UnknownActingSetNode {
        pg_id: 7,
        node_id: 9,
    }))
    .unwrap();
    assert!(matches!(
        decode_control_plane_rpc_response(encoded),
        Err(ControlPlaneError::UnknownActingSetNode {
            pg_id: 7,
            node_id: 9,
        })
    ));
}

#[test]
fn unix_control_plane_client_fetches_pg_runtime_map_with_bounded_non_serving_validity() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();
    let expected_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream(&mut authority, &mut stream, 1_234).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let runtime_map = client.pg_runtime_map_snapshot(PgId::new(7), 0).unwrap();

    server.join().unwrap();
    assert_eq!(runtime_map.cluster_epoch(), expected_epoch);
    assert_eq!(
        runtime_map.valid_until_ms(),
        Some(1_234 + MAX_HEARTBEAT_LEASE_MS)
    );
    assert_eq!(runtime_map.pg_routes().len(), 1);
    assert_eq!(runtime_map.pg_routes()[0].state(), PgState::Peering);
    assert_eq!(runtime_map.pg_routes()[0].primary_lease_deadline_ms(), None);
}

#[test]
fn unix_control_plane_client_retries_read_only_runtime_map_after_lost_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_until_serving_with_endpoint(
        &mut authority,
        1,
        1_000,
        "/tmp/argmin-node-1.sock".to_owned(),
    );
    let expected_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::RuntimeMapSnapshot);
        drop(stream);

        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream(&mut authority, &mut stream, 1_001).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let runtime_map = client.runtime_map_snapshot(0).unwrap();

    server.join().unwrap();
    assert_eq!(runtime_map.cluster_epoch(), expected_epoch);
    assert_eq!(runtime_map.pg_routes().len(), 1);
    assert_eq!(runtime_map.pg_routes()[0].pg_id(), PgId::new(7));
}

#[test]
fn unix_control_plane_client_refreshes_heartbeat_and_runtime_map_together() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let heartbeat_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream(&mut authority, &mut stream, 2_000).unwrap();
    });

    let mut client = UnixControlPlaneClient::new(&socket_path);
    let refresh = client
        .refresh_node_heartbeat(
            NodeHeartbeat {
                node_id: NodeId::new(1),
                node_incarnation: 42,
                endpoint: "/tmp/argmin-node-1.sock".to_owned(),
                observed_epoch: heartbeat_epoch,
                requested_lease_duration_ms: 100,
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                cluster_map_history_route_references: Default::default(),
                pg_observations: Vec::new(),
            },
            0,
        )
        .unwrap();

    server.join().unwrap();
    assert_eq!(refresh.lease().node_id(), NodeId::new(1));
    assert_eq!(refresh.lease().lease_deadline_ms(), 2_100);
    assert!(!refresh.lease().serving());
    assert_eq!(
        refresh.lease().cluster_epoch(),
        refresh.runtime_map().cluster_epoch()
    );
    assert_eq!(refresh.runtime_map().nodes().len(), 1);
    assert_eq!(
        refresh.runtime_map().nodes()[0].endpoint(),
        "/tmp/argmin-node-1.sock"
    );
    assert_eq!(
        refresh.runtime_map().nodes()[0].cluster_map_history_floor_epoch(),
        None
    );
}

#[test]
fn authenticated_unix_control_plane_client_reads_runtime_map_as_frontend() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let verifier = frontend_auth_verifier("auth-cluster", "frontend-1");
    let verifier_for_assert = verifier.clone();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream_with_auth(&mut authority, &mut stream, 2_000, &verifier)
            .unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        frontend_auth_credential("auth-cluster", "frontend-1"),
    );
    let payload = client
        .send_signed_read_only_request_with_read_timeout_and_clock(
            ControlPlaneRpcKind::RuntimeMapSnapshot,
            Vec::new(),
            CONTROL_PLANE_RPC_CHECK_APPLIED_IO_TIMEOUT,
            || Ok(2_000),
        )
        .unwrap();
    let mut reader = PayloadReader::new(&payload);
    let runtime_map = read_runtime_map_snapshot(&mut reader).unwrap();
    reader.finish().unwrap();

    server.join().unwrap();
    assert!(runtime_map.cluster_epoch().get() >= 1);
    let metrics = verifier_for_assert.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 1);
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::FrontendRuntimeMapRead),
        1
    );
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn authenticated_frontend_read_accepts_overlapping_credentials() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let old_credential =
        frontend_auth_config_credential_with("frontend-1", "frontend", 1, "old-secret");
    let new_credential =
        frontend_auth_config_credential_with("frontend-1", "frontend", 2, "new-secret");
    let old_signer = old_credential
        .scoped_for_cluster("auth-cluster")
        .expect("old frontend credential should scope");
    let verifier = ControlPlaneUnixAuthVerifier::new_empty("auth-cluster")
        .unwrap()
        .with_frontend_credentials(vec![old_credential, new_credential])
        .unwrap();
    let verifier_for_assert = verifier.clone();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream_with_auth(&mut authority, &mut stream, 2_000, &verifier)
            .unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        old_signer,
    );
    let runtime_map = client.runtime_map_snapshot(2_000).unwrap();

    server.join().unwrap();
    assert!(runtime_map.cluster_epoch().get() >= 1);
    let status = verifier_for_assert.status_snapshot();
    assert_eq!(status.frontend_credentials().len(), 2);
    let metrics = verifier_for_assert.metrics_snapshot();
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::FrontendRuntimeMapRead),
        1
    );
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn authenticated_unix_control_plane_client_resigns_read_only_retry_attempts() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let (issued_tx, issued_rx) = std::sync::mpsc::channel();
    let credential = frontend_auth_credential("auth-cluster", "frontend-1");
    let response_signer = credential.clone();
    let server = std::thread::spawn(move || {
        for attempt in 0..2 {
            let (mut stream, _addr) = listener.accept().unwrap();
            let (kind, payload) = read_control_plane_rpc_frame(&mut stream).unwrap();
            assert_eq!(kind, ControlPlaneRpcKind::RuntimeMapStatus);
            let envelope =
                ControlPlaneAuthEnvelope::decode_frame(&payload, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)
                    .unwrap();
            issued_tx
                .send(envelope.header().issued_at_ms().unwrap())
                .unwrap();
            if attempt == 0 {
                continue;
            }
            let response_payload =
                signed_runtime_map_response_payload(kind, &response_signer, Vec::new(), 6_500);
            write_control_plane_rpc_frame(&mut stream, kind, &response_payload).unwrap();
        }
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        credential,
    );
    let mut issued_at_ms = [1_000, 6_500].into_iter();
    let response = client
        .send_signed_read_only_request_with_read_timeout_and_clock(
            ControlPlaneRpcKind::RuntimeMapStatus,
            Vec::new(),
            CONTROL_PLANE_RPC_IO_TIMEOUT,
            || Ok(issued_at_ms.next().unwrap_or(6_500)),
        )
        .unwrap();

    server.join().unwrap();
    assert!(response.is_empty());
    assert_eq!(issued_rx.recv().unwrap(), 1_000);
    assert_eq!(issued_rx.recv().unwrap(), 6_500);
}

#[test]
fn authenticated_endpoint_failover_rejects_exhausted_aggregate_budget_before_transport() {
    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new("unused-test-socket"),
        frontend_auth_credential("auth-cluster", "frontend-1"),
    );

    let error = client
        .send_verified_request_with_endpoint_failover_until(
            ControlPlaneRpcKind::RuntimeMapStatus,
            Instant::now() - Duration::from_millis(1),
            || Ok(Vec::new()),
            |_| Ok(Vec::new()),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic }
            if diagnostic.context() == "control-plane RPC endpoint failover deadline"
                && diagnostic.kind() == ErrorKind::TimedOut
    ));
}

#[test]
fn unix_control_plane_connect_rejects_expired_aggregate_budget_before_transport() {
    let error = connect_unix_stream_until(
        Path::new("unused-test-socket"),
        Instant::now() - Duration::from_millis(1),
    )
    .unwrap_err();

    assert_eq!(error.kind(), ErrorKind::TimedOut);
}

#[test]
fn unix_control_plane_frame_io_rechecks_expired_aggregate_budget() {
    let (mut write_stream, mut write_peer) = UnixStream::pair().unwrap();
    let mut write_stream = DeadlineUnixStream::new(
        &mut write_stream,
        Instant::now() - Duration::from_millis(1),
        CONTROL_PLANE_RPC_DEADLINE_EXPIRED,
    )
    .unwrap();
    let write_error = write_control_plane_rpc_frame(
        &mut write_stream,
        ControlPlaneRpcKind::RuntimeMapSnapshot,
        &[],
    )
    .unwrap_err();
    assert!(matches!(
        write_error,
        ControlPlaneError::Io { diagnostic }
            if diagnostic.context() == "write control-plane RPC magic"
                && diagnostic.kind() == ErrorKind::TimedOut
    ));
    write_peer.set_nonblocking(true).unwrap();
    let mut unexpected_byte = [0_u8; 1];
    assert!(matches!(
        write_peer.read(&mut unexpected_byte),
        Err(error) if error.kind() == ErrorKind::WouldBlock
    ));

    let (mut read_stream, _read_peer) = UnixStream::pair().unwrap();
    let mut read_stream = DeadlineUnixStream::new(
        &mut read_stream,
        Instant::now() - Duration::from_millis(1),
        CONTROL_PLANE_RPC_DEADLINE_EXPIRED,
    )
    .unwrap();
    let read_error = read_control_plane_rpc_frame(&mut read_stream).unwrap_err();
    assert!(matches!(
        read_error,
        ControlPlaneError::Io { diagnostic }
            if diagnostic.context() == "read control-plane RPC magic"
                && diagnostic.kind() == ErrorKind::TimedOut
    ));
}

#[test]
fn authenticated_admission_error_preserves_explicit_routing_rejection() {
    let credential = frontend_auth_credential("auth-cluster", "frontend-1");
    let verifier = frontend_auth_verifier("auth-cluster", "frontend-1");
    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new("unused-test-socket"),
        credential.clone(),
    );
    let kind = ControlPlaneRpcKind::RuntimeMapStatus;
    let request = signed_frontend_runtime_map_request(
        kind,
        &credential,
        Vec::new(),
        Some(2_000),
        Some(7_000),
    );
    let request = verify_control_plane_unix_request(request, Some(&verifier), 2_000).unwrap();
    let response = build_control_plane_unix_admission_error_response(
        request,
        ControlPlaneError::AuthorityNotServing,
        2_000,
    )
    .unwrap();
    let payload = client
        .verify_runtime_map_response(kind, 2_000, &response.payload)
        .unwrap();
    let error = decode_control_plane_rpc_response(payload).unwrap_err();
    let metrics = verifier.metrics_snapshot();

    assert!(matches!(error, ControlPlaneError::AuthorityNotServing));
    assert_eq!(metrics.accepted_total(), 1);
}

#[test]
fn unix_control_plane_client_reads_pending_recoveries_without_runtime_map() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let expected = PendingMetadataCommandRecoveryListing::new(
        vec![PendingMetadataCommandRecoveryTask::new(
            PgId::new(7),
            PendingMetadataCommandRecovery::new(
                NodeId::new(3),
                PendingMetadataCommandObservation::new(
                    ClusterEpoch::new(9).unwrap(),
                    NonZeroU64::new(11).unwrap(),
                    0x1234,
                ),
            ),
        )],
        vec![PendingMetadataCommandRecoveryDiscoveryFailure::new(
            PgId::new(8),
            PendingMetadataCommandRecoveryDiscoveryFailureKind::ConflictingIdentity,
            "conflicting observations".to_owned(),
        )],
    );
    let server_expected = expected.clone();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let (kind, payload) = read_control_plane_rpc_frame(&mut stream).unwrap();
        assert_eq!(kind, ControlPlaneRpcKind::PendingMetadataCommandRecoveries);
        assert!(payload.is_empty());
        let mut response = Vec::new();
        write_pending_metadata_command_recovery_listing(&mut response, &server_expected).unwrap();
        let response = encode_control_plane_rpc_response(Ok(response)).unwrap();
        write_control_plane_rpc_frame(&mut stream, kind, &response).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    assert_eq!(
        client.pending_metadata_command_recoveries().unwrap(),
        expected
    );
    server.join().unwrap();
}

#[test]
fn authenticated_unix_control_plane_client_rejects_unsigned_runtime_map_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let (kind, payload) = read_control_plane_rpc_frame(&mut stream).unwrap();
        assert_eq!(kind, ControlPlaneRpcKind::RuntimeMapStatus);
        ControlPlaneAuthEnvelope::decode_frame(&payload, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)
            .unwrap();
        let response = encode_control_plane_rpc_response(Ok(Vec::new())).unwrap();
        write_control_plane_rpc_frame(&mut stream, kind, &response).unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        frontend_auth_credential("auth-cluster", "frontend-1"),
    );
    let error = client
        .runtime_map_status(1_000)
        .expect_err("unsigned runtime-map response should be rejected");

    server.join().unwrap();
    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
        if message.contains("control-plane auth envelope")),
        "unexpected error: {error}"
    );
}

#[test]
fn authenticated_unix_control_plane_client_rejects_wrong_target_runtime_map_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let response_signer = frontend_auth_credential("auth-cluster", "frontend-1");
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let (kind, payload) = read_control_plane_rpc_frame(&mut stream).unwrap();
        assert_eq!(kind, ControlPlaneRpcKind::RuntimeMapStatus);
        ControlPlaneAuthEnvelope::decode_frame(&payload, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)
            .unwrap();
        let response_payload = sign_control_plane_response_payload(
            kind,
            &response_signer
                .runtime_map_response_credential_for_frontend()
                .expect("test frontend credential should derive runtime-map response credential"),
            ControlPlaneAuthPrincipal::Frontend {
                instance_id: "frontend-2".to_owned(),
            },
            ControlPlaneAuthOperation::RuntimeMapResponse,
            1_000,
            encode_control_plane_rpc_response(Ok(Vec::new())).unwrap(),
        )
        .unwrap();
        write_control_plane_rpc_frame(&mut stream, kind, &response_payload).unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        frontend_auth_credential("auth-cluster", "frontend-1"),
    );
    let error = client
        .runtime_map_status(1_000)
        .expect_err("wrong-target runtime-map response should be rejected");

    server.join().unwrap();
    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
        if message.contains("WrongTarget")),
        "unexpected error: {error}"
    );
}

#[test]
fn authenticated_unix_control_plane_client_verifies_runtime_map_error_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let response_signer = frontend_auth_credential("auth-cluster", "frontend-1");
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let (kind, payload) = read_control_plane_rpc_frame(&mut stream).unwrap();
        assert_eq!(kind, ControlPlaneRpcKind::RuntimeMapStatus);
        ControlPlaneAuthEnvelope::decode_frame(&payload, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)
            .unwrap();
        let response_payload = sign_control_plane_response_payload(
            kind,
            &response_signer
                .runtime_map_response_credential_for_frontend()
                .expect("test frontend credential should derive runtime-map response credential"),
            response_signer.principal().clone(),
            ControlPlaneAuthOperation::RuntimeMapResponse,
            2_000,
            encode_control_plane_rpc_response(Err(ControlPlaneError::rpc_remote(
                "synthetic signed runtime-map error".to_owned(),
            )))
            .unwrap(),
        )
        .unwrap();
        write_control_plane_rpc_frame(&mut stream, kind, &response_payload).unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        frontend_auth_credential("auth-cluster", "frontend-1"),
    );
    let error = client
        .runtime_map_status(2_000)
        .expect_err("signed runtime-map error should decode after auth verification");

    server.join().unwrap();
    assert!(
        matches!(error, ControlPlaneError::RpcRemote { diagnostic: ref message }
        if message.contains("synthetic signed runtime-map error")),
        "unexpected error: {error}"
    );
}

#[test]
fn runtime_map_response_credential_requires_frontend_credential() {
    let admin = admin_auth_credential("auth-cluster", "admin-1");

    let error = admin
        .runtime_map_response_credential_for_frontend()
        .expect_err("admin credentials must not derive runtime-map response credentials");

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
        if message.contains("requires a frontend scoped credential")),
        "unexpected error: {error}"
    );
}

#[test]
fn runtime_map_response_credential_accepts_storage_node_credential() {
    let storage_node = storage_node_auth_credential("auth-cluster", 1, 42);

    let response_credential = storage_node
        .runtime_map_response_credential_for_storage_node()
        .expect("storage-node credentials should derive runtime-map response credentials");

    assert_eq!(response_credential.cluster_id(), "auth-cluster");
    assert_eq!(response_credential.credential_id(), "storage-node-1");
    assert_eq!(response_credential.credential_version(), 1);
    assert_eq!(
        response_credential.principal(),
        &ControlPlaneAuthPrincipal::Service {
            service: ControlPlaneAuthService::RuntimeMap
        }
    );
}

#[test]
fn admin_control_plane_response_credential_requires_admin_credential() {
    let frontend = frontend_auth_credential("auth-cluster", "frontend-1");
    let admin = admin_auth_credential("auth-cluster", "admin-1");

    let error = frontend
        .admin_control_plane_response_credential_for_admin()
        .expect_err("frontend credentials must not derive admin response credentials");
    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
        if message.contains("requires an admin scoped credential")),
        "unexpected error: {error}"
    );

    let response_credential = admin
        .admin_control_plane_response_credential_for_admin()
        .expect("admin credentials should derive admin response credentials");
    assert_eq!(response_credential.cluster_id(), "auth-cluster");
    assert_eq!(response_credential.credential_id(), "admin-1-credential");
    assert_eq!(response_credential.credential_version(), 1);
    assert_eq!(
        response_credential.principal(),
        &ControlPlaneAuthPrincipal::Service {
            service: ControlPlaneAuthService::Admin
        }
    );
}

#[test]
fn authenticated_control_plane_rejects_missing_frontend_runtime_map_auth() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let before = authority.snapshot().clone();
    let verifier = frontend_auth_verifier("auth-cluster", "frontend-1");
    let request = ControlPlaneRpcRequest {
        kind: ControlPlaneRpcKind::RuntimeMapSnapshot,
        payload: Vec::new(),
    };

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { .. }),
        "unexpected error: {error}"
    );
    assert_eq!(authority.snapshot(), &before);
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::FrontendRuntimeMapRead),
        1
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::Missing),
        1
    );
}

#[test]
fn mandatory_control_plane_auth_rejects_unsigned_read_with_admin_only_verifier() {
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let request = || ControlPlaneRpcRequest {
        kind: ControlPlaneRpcKind::RuntimeMapStatus,
        payload: Vec::new(),
    };

    let compatible = verify_control_plane_unix_request(request(), Some(&verifier), 2_000)
        .expect("Unix compatibility mode should retain configured-role auth policy");
    assert!(compatible.response_auth.is_none());

    let error = verify_control_plane_authenticated_request(request(), Some(&verifier), 2_000)
        .err()
        .expect("mandatory-auth endpoints must reject unsigned runtime-map reads");
    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
            if message.contains("requires authenticated requests")),
        "unexpected error: {error}"
    );
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::Missing),
        1
    );
}

#[test]
fn mandatory_control_plane_auth_accepts_admin_signed_runtime_map_read() {
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let signer = admin_auth_credential("auth-cluster", "admin-1");
    let request = signed_admin_control_plane_request(
        ControlPlaneRpcKind::RuntimeMapStatus,
        &signer,
        Vec::new(),
        Some(1_999),
        Some(2_999),
    );

    let verified = verify_control_plane_authenticated_request(request, Some(&verifier), 2_000)
        .expect("admin-signed runtime-map read should authenticate");

    assert!(verified.response_auth.is_some());
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 1);
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        1
    );
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn unix_auth_verifier_allows_rotation_but_rejects_duplicate_credential_identity() {
    let verifier = ControlPlaneUnixAuthVerifier::new(
        "auth-cluster",
        vec![
            storage_node_auth_node_credential_with(1, "storage-node", 1, "old-secret"),
            storage_node_auth_node_credential_with(1, "storage-node", 2, "new-secret"),
        ],
    )
    .unwrap();
    assert_eq!(
        verifier.status_snapshot().storage_node_credentials().len(),
        2
    );

    let error = ControlPlaneUnixAuthVerifier::new(
        "auth-cluster",
        vec![
            storage_node_auth_node_credential_with(1, "storage-node", 1, "old-secret"),
            storage_node_auth_node_credential_with(1, "storage-node", 1, "other-secret"),
        ],
    )
    .expect_err("duplicate storage-node credential identity should reject");
    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
        if message.contains("repeats credential identity")),
        "unexpected error: {error}"
    );

    let verifier = ControlPlaneUnixAuthVerifier::new_empty("auth-cluster")
        .unwrap()
        .with_frontend_credentials(vec![
            frontend_auth_config_credential_with("frontend-1", "frontend", 1, "old-secret"),
            frontend_auth_config_credential_with("frontend-1", "frontend", 2, "new-secret"),
        ])
        .unwrap()
        .with_admin_credentials(vec![
            admin_auth_config_credential_with("admin-1", "admin", 1, "old-secret"),
            admin_auth_config_credential_with("admin-1", "admin", 2, "new-secret"),
        ])
        .unwrap();
    let status = verifier.status_snapshot();
    assert_eq!(status.frontend_credentials().len(), 2);
    assert_eq!(status.admin_credentials().len(), 2);

    let error = ControlPlaneUnixAuthVerifier::new_empty("auth-cluster")
        .unwrap()
        .with_frontend_credentials(vec![
            frontend_auth_config_credential_with("frontend-1", "frontend", 1, "old-secret"),
            frontend_auth_config_credential_with("frontend-1", "frontend", 1, "other-secret"),
        ])
        .expect_err("duplicate frontend credential identity should reject");
    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
        if message.contains("repeats credential identity")),
        "unexpected error: {error}"
    );

    let error = ControlPlaneUnixAuthVerifier::new_empty("auth-cluster")
        .unwrap()
        .with_admin_credentials(vec![
            admin_auth_config_credential_with("admin-1", "admin", 1, "old-secret"),
            admin_auth_config_credential_with("admin-1", "admin", 1, "other-secret"),
        ])
        .expect_err("duplicate admin credential identity should reject");
    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
        if message.contains("repeats credential identity")),
        "unexpected error: {error}"
    );
}

#[test]
fn authenticated_control_plane_rejects_storage_credential_for_frontend_runtime_map_read() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let before = authority.snapshot().clone();
    let verifier = frontend_auth_verifier("auth-cluster", "frontend-1");
    let signer = storage_node_auth_credential("auth-cluster", 1, 42);
    let request = signed_frontend_runtime_map_request(
        ControlPlaneRpcKind::RuntimeMapSnapshot,
        &signer,
        Vec::new(),
        Some(1_999),
        Some(2_999),
    );

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message } if message.contains("source is not a frontend")),
        "unexpected error: {error}"
    );
    assert_eq!(authority.snapshot(), &before);
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::FrontendRuntimeMapRead),
        1
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::WrongRole),
        1
    );
}

#[test]
fn authenticated_control_plane_rejects_frontend_runtime_map_kind_replay() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let before = authority.snapshot().clone();
    let verifier = frontend_auth_verifier("auth-cluster", "frontend-1");
    let signer = frontend_auth_credential("auth-cluster", "frontend-1");
    let mut request = signed_frontend_runtime_map_request(
        ControlPlaneRpcKind::RuntimeMapSnapshot,
        &signer,
        Vec::new(),
        Some(1_999),
        Some(2_999),
    );
    request.kind = ControlPlaneRpcKind::RuntimeMapStatus;

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message } if message.contains("did not match outer kind")),
        "unexpected error: {error}"
    );
    assert_eq!(authority.snapshot(), &before);
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::FrontendRuntimeMapRead),
        1
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::WrongRole),
        1
    );
}

#[test]
fn authenticated_control_plane_rejects_resigned_admin_inner_kind_before_dispatch() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let before = authority.snapshot().clone();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let signer = admin_auth_credential("auth-cluster", "admin-1");
    let mut payload = Vec::new();
    write_pg_acting_set_request(&mut payload, PgId::new(7), &[NodeId::new(1)]).unwrap();
    let request = signed_admin_control_plane_request_with_embedded_kind(
        ControlPlaneRpcKind::SetPgActingSet,
        ControlPlaneRpcKind::RuntimeMapStatus,
        &signer,
        payload,
        Some(1_999),
        Some(2_999),
    );

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
            if message.contains("authenticated RPC kind RuntimeMapStatus did not match outer kind SetPgActingSet")),
        "unexpected error: {error}"
    );
    assert_eq!(authority.snapshot(), &before);
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        1
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::WrongRole),
        1
    );
}

#[test]
fn authenticated_control_plane_rejects_resigned_auth_versions_before_dispatch() {
    for auth_version in [0_u16, 2] {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        let before = authority.snapshot().clone();
        let verifier = admin_auth_verifier("auth-cluster", "admin-1");
        let signer = admin_auth_credential("auth-cluster", "admin-1");
        let mut payload = Vec::new();
        write_pg_acting_set_request(&mut payload, PgId::new(7), &[NodeId::new(1)]).unwrap();
        let request = signed_admin_control_plane_request_with_auth_version(
            ControlPlaneRpcKind::SetPgActingSet,
            &signer,
            payload,
            Some(1_999),
            Some(2_999),
            auth_version,
        );

        let error = build_control_plane_unix_response_with_auth(
            &mut authority,
            request,
            2_000,
            Some(&verifier),
        )
        .unwrap_err();

        assert!(
            matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
                if message.as_str() == format!("control-plane auth envelope: unsupported control-plane auth version {auth_version}")),
            "unexpected auth v{auth_version} error: {error}"
        );
        assert_eq!(authority.snapshot(), &before);
        let metrics = verifier.metrics_snapshot();
        assert_eq!(metrics.accepted_total(), 0);
        assert_eq!(metrics.rejected_total(), 1);
        assert_eq!(
            metrics.rejected_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
            1
        );
        assert_eq!(
            metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::UnsupportedVersion),
            1
        );
    }
}

#[test]
fn authenticated_control_plane_rejects_resigned_response_inner_kind_before_acceptance() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let outer_kind = ControlPlaneRpcKind::RuntimeMapStatus;
    let embedded_kind = ControlPlaneRpcKind::RuntimeMapSnapshot;
    let frontend = frontend_auth_credential("auth-cluster", "frontend-1");
    let response_signer = frontend.clone();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let (kind, request) = read_control_plane_rpc_frame(&mut stream).unwrap();
        assert_eq!(kind, outer_kind);
        ControlPlaneAuthEnvelope::decode_frame(&request, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)
            .unwrap();
        let payload = write_authenticated_control_plane_rpc_payload(embedded_kind, &[]);
        let response_credential = response_signer
            .runtime_map_response_credential_for_frontend()
            .unwrap();
        let envelope = response_credential
            .sign_envelope(crate::control_plane_auth::ControlPlaneAuthSignInput {
                target: ControlPlaneAuthTarget::Principal(response_signer.principal().clone()),
                operation: ControlPlaneAuthOperation::RuntimeMapResponse,
                issued_at_ms: Some(2_000),
                expires_at_ms: Some(7_000),
                sequence: None,
                nonce: Vec::new(),
                payload,
            })
            .unwrap();
        write_control_plane_rpc_frame(&mut stream, outer_kind, &envelope.encode_frame().unwrap())
            .unwrap();
    });
    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        frontend,
    );

    let error = client.runtime_map_status(2_000).unwrap_err();

    server.join().unwrap();
    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
            if message.contains("authenticated RPC kind RuntimeMapSnapshot did not match outer kind RuntimeMapStatus")),
        "unexpected error: {error}"
    );
}

#[test]
fn authenticated_control_plane_rejects_missing_admin_command_auth() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let before = authority.snapshot().clone();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let mut payload = Vec::new();
    write_pg_acting_set_request(&mut payload, PgId::new(7), &[NodeId::new(1)]).unwrap();
    let request = ControlPlaneRpcRequest {
        kind: ControlPlaneRpcKind::SetPgActingSet,
        payload,
    };

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { .. }),
        "unexpected error: {error}"
    );
    assert_eq!(authority.snapshot(), &before);
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        1
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::Missing),
        1
    );
}

#[test]
fn authenticated_control_plane_rejects_malformed_admin_pg_runtime_map_read() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let before = authority.snapshot().clone();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let signer = admin_auth_credential("auth-cluster", "admin-1");
    let mut payload = Vec::new();
    write_pg_id_request(&mut payload, PgId::new(7));
    let mut request = signed_admin_control_plane_request(
        ControlPlaneRpcKind::PgRuntimeMapSnapshot,
        &signer,
        payload,
        Some(1_999),
        Some(2_999),
    );
    request
        .payload
        .pop()
        .expect("signed auth frame should not be empty");

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { .. }),
        "unexpected error: {error}"
    );
    assert_eq!(authority.snapshot(), &before);
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        1
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::Malformed),
        1
    );
}

#[test]
fn authenticated_control_plane_accepts_admin_command_auth() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let signer = admin_auth_credential("auth-cluster", "admin-1");
    let mut payload = Vec::new();
    write_pg_acting_set_request(&mut payload, PgId::new(7), &[NodeId::new(1)]).unwrap();
    let request = signed_admin_control_plane_request(
        ControlPlaneRpcKind::SetPgActingSet,
        &signer,
        payload,
        Some(1_999),
        Some(2_999),
    );

    let response = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap();
    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(tmp.path().join("unused.sock")),
        signer,
    );
    let response_payload = client
        .verify_admin_control_plane_response(
            ControlPlaneRpcKind::SetPgActingSet,
            2_000,
            &response.payload,
        )
        .unwrap();
    let response_payload = decode_control_plane_rpc_response(response_payload).unwrap();
    let mut reader = PayloadReader::new(&response_payload);
    let cluster_epoch = ClusterEpoch::new(reader.read_u64().unwrap()).unwrap();
    reader.finish().unwrap();

    assert_eq!(cluster_epoch, authority.snapshot().cluster_epoch());
    let pg = authority.snapshot().pg(PgId::new(7)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(1)]);
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 1);
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        1
    );
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn authenticated_authority_clock_admin_reestablishes_once_and_rejects_stale_replay() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let high_water = authority.snapshot().max_committed_timestamp_ms();
    let wall_ms = high_water.unwrap() + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS + 1;
    let mut clock = ControlPlaneAuthorityClock::new(high_water, wall_ms, Some(50)).unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let signer = admin_auth_credential("auth-cluster", "admin-1");
    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(tmp.path().join("unused.sock")),
        signer.clone(),
    );

    let status_request = signed_admin_control_plane_request(
        ControlPlaneRpcKind::AuthorityClockStatus,
        &signer,
        Vec::new(),
        Some(wall_ms),
        Some(wall_ms + 1_000),
    );
    let status_response = build_control_plane_authority_clock_admin_response(
        &authority,
        &mut clock,
        status_request,
        Some(&verifier),
        ControlPlaneAuthorityClockAdminSample::new(wall_ms, wall_ms, Some(50)),
        |_, _| Ok(()),
        || Ok(wall_ms),
    )
    .unwrap();
    let status_payload = client
        .verify_admin_control_plane_response(
            ControlPlaneRpcKind::AuthorityClockStatus,
            wall_ms,
            &status_response.payload,
        )
        .and_then(decode_control_plane_rpc_response)
        .unwrap();
    let mut reader = PayloadReader::new(&status_payload);
    let blocked = read_authority_clock_status(&mut reader).unwrap();
    reader.finish().unwrap();
    assert!(!blocked.established());

    let mut recovery_payload = Vec::new();
    write_u64(&mut recovery_payload, blocked.generation());
    write_option_u64(
        &mut recovery_payload,
        blocked.committed_timestamp_high_water_ms(),
    );
    write_option_u64(
        &mut recovery_payload,
        blocked.current_raft_leadership_term(),
    );
    let recovery_request = signed_admin_control_plane_request(
        ControlPlaneRpcKind::ReestablishAuthorityClock,
        &signer,
        recovery_payload,
        Some(wall_ms),
        Some(wall_ms + 1_000),
    );
    let checkpoint_persisted = std::cell::Cell::new(false);
    let recovery_response = build_control_plane_authority_clock_admin_response(
        &authority,
        &mut clock,
        recovery_request,
        Some(&verifier),
        ControlPlaneAuthorityClockAdminSample::new(wall_ms, wall_ms, Some(50)),
        |_, _| {
            checkpoint_persisted.set(true);
            Ok(())
        },
        || {
            assert!(
                checkpoint_persisted.get(),
                "recovery checkpoint must persist before response-time sampling"
            );
            Ok(wall_ms)
        },
    )
    .unwrap();
    let recovery_payload = client
        .verify_admin_control_plane_response(
            ControlPlaneRpcKind::ReestablishAuthorityClock,
            wall_ms,
            &recovery_response.payload,
        )
        .and_then(decode_control_plane_rpc_response)
        .unwrap();
    let mut reader = PayloadReader::new(&recovery_payload);
    let established = read_authority_clock_status(&mut reader).unwrap();
    reader.finish().unwrap();
    assert!(established.established());
    assert_eq!(established.generation(), blocked.generation() + 1);

    assert!(clock.effective_now_ms(wall_ms + 1, None).is_err());
    let mut stale_payload = Vec::new();
    write_u64(&mut stale_payload, blocked.generation());
    write_option_u64(
        &mut stale_payload,
        blocked.committed_timestamp_high_water_ms(),
    );
    write_option_u64(&mut stale_payload, blocked.current_raft_leadership_term());
    let stale_request = signed_admin_control_plane_request(
        ControlPlaneRpcKind::ReestablishAuthorityClock,
        &signer,
        stale_payload,
        Some(wall_ms),
        Some(wall_ms + 1_000),
    );
    let stale_response = build_control_plane_authority_clock_admin_response(
        &authority,
        &mut clock,
        stale_request,
        Some(&verifier),
        ControlPlaneAuthorityClockAdminSample::new(wall_ms, wall_ms + 1, Some(51)),
        |_, _| Ok(()),
        || Ok(wall_ms + 1),
    )
    .unwrap();
    let error = client
        .verify_admin_control_plane_response(
            ControlPlaneRpcKind::ReestablishAuthorityClock,
            wall_ms + 1,
            &stale_response.payload,
        )
        .and_then(decode_control_plane_rpc_response)
        .unwrap_err();
    assert!(
        matches!(error, ControlPlaneError::RpcRemote { diagnostic: ref message }
        if message.contains("generation changed"))
    );
    assert!(!clock
        .status(authority.authority_clock_context().unwrap())
        .established());
}

#[test]
fn authenticated_authority_clock_status_observes_missing_health_sample() {
    let checkpoint_callback_invoked = std::cell::Cell::new(false);
    let tmp = test_util::tempdir();
    let authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
        tmp.path().join("control-plane.state"),
    ))
    .unwrap();
    let mut clock = ControlPlaneAuthorityClock::new(None, 1_000, Some(50)).unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let signer = admin_auth_credential("auth-cluster", "admin-1");
    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(tmp.path().join("unused.sock")),
        signer.clone(),
    );
    let request = signed_admin_control_plane_request(
        ControlPlaneRpcKind::AuthorityClockStatus,
        &signer,
        Vec::new(),
        Some(1_001),
        Some(2_001),
    );

    let response = build_control_plane_authority_clock_admin_response(
        &authority,
        &mut clock,
        request,
        Some(&verifier),
        ControlPlaneAuthorityClockAdminSample::new(1_001, 1_001, None),
        |_, _| {
            checkpoint_callback_invoked.set(true);
            Err(ControlPlaneError::io(
                "unexpected status checkpoint callback",
                std::io::Error::other("status must not persist a checkpoint"),
            ))
        },
        || {
            assert!(
                !checkpoint_callback_invoked.get(),
                "status must sample response time without checkpoint persistence"
            );
            Ok(1_001)
        },
    )
    .unwrap();
    let payload = client
        .verify_admin_control_plane_response(
            ControlPlaneRpcKind::AuthorityClockStatus,
            1_001,
            &response.payload,
        )
        .and_then(decode_control_plane_rpc_response)
        .unwrap();
    let mut reader = PayloadReader::new(&payload);
    let status = read_authority_clock_status(&mut reader).unwrap();
    reader.finish().unwrap();

    assert!(!status.established());
    assert_eq!(
        status.blocked_reason(),
        Some(ControlPlaneAuthorityClockBlockedReason::ClockSourceUnavailable)
    );
    assert!(!checkpoint_callback_invoked.get());
}

#[test]
fn authenticated_authority_clock_healthy_status_does_not_persist_checkpoint() {
    let tmp = test_util::tempdir();
    let authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
        tmp.path().join("control-plane.state"),
    ))
    .unwrap();
    let mut clock = ControlPlaneAuthorityClock::new(None, 1_000, Some(50)).unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let signer = admin_auth_credential("auth-cluster", "admin-1");
    let request = signed_admin_control_plane_request(
        ControlPlaneRpcKind::AuthorityClockStatus,
        &signer,
        Vec::new(),
        Some(1_001),
        Some(2_001),
    );
    let checkpoint_callback_invoked = std::cell::Cell::new(false);

    let response = build_control_plane_authority_clock_admin_response(
        &authority,
        &mut clock,
        request,
        Some(&verifier),
        ControlPlaneAuthorityClockAdminSample::new(1_001, 1_001, Some(51)),
        |_, _| {
            checkpoint_callback_invoked.set(true);
            Err(ControlPlaneError::io(
                "unexpected status checkpoint callback",
                std::io::Error::other("status must not persist a checkpoint"),
            ))
        },
        || Ok(1_001),
    )
    .unwrap();
    assert!(!checkpoint_callback_invoked.get());
    assert!(clock
        .status(authority.authority_clock_context().unwrap())
        .established());

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(tmp.path().join("unused.sock")),
        signer,
    );
    let payload = client
        .verify_admin_control_plane_response(
            ControlPlaneRpcKind::AuthorityClockStatus,
            1_001,
            &response.payload,
        )
        .and_then(decode_control_plane_rpc_response)
        .unwrap();
    let mut reader = PayloadReader::new(&payload);
    assert!(read_authority_clock_status(&mut reader)
        .unwrap()
        .established());
    reader.finish().unwrap();
}

#[test]
fn authority_clock_recovery_rejects_clock_step_before_checkpoint_persistence() {
    let tmp = test_util::tempdir();
    let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
        tmp.path().join("control-plane.state"),
    ))
    .unwrap();
    authority.expire_heartbeat_leases(1_000).unwrap();
    let context = authority.authority_clock_context().unwrap();
    let mut clock = ControlPlaneAuthorityClock::new(Some(1_000), 2_500, Some(1_000)).unwrap();
    let blocked = clock.status(context);
    assert!(!blocked.established());
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let signer = admin_auth_credential("auth-cluster", "admin-1");
    let mut recovery_payload = Vec::new();
    write_u64(&mut recovery_payload, blocked.generation());
    write_option_u64(
        &mut recovery_payload,
        blocked.committed_timestamp_high_water_ms(),
    );
    write_option_u64(
        &mut recovery_payload,
        blocked.current_raft_leadership_term(),
    );
    let request = signed_admin_control_plane_request(
        ControlPlaneRpcKind::ReestablishAuthorityClock,
        &signer,
        recovery_payload,
        Some(2_500),
        Some(3_500),
    );
    let response_clock_sampled = std::cell::Cell::new(false);
    let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft("test-cluster", 1);

    let error = build_control_plane_authority_clock_admin_response(
        &authority,
        &mut clock,
        request,
        Some(&verifier),
        ControlPlaneAuthorityClockAdminSample::new(2_500, 2_500, Some(1_000)),
        |_, clock| {
            validated_authority_clock_restart_checkpoint(
                binding,
                Some(1_000),
                clock,
                4_501,
                Some(1_000),
            )
            .map(drop)
        },
        || {
            response_clock_sampled.set(true);
            Ok(4_501)
        },
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::CommittedTimestampTooFarAhead { .. }
    ));
    assert!(!response_clock_sampled.get());
    assert_eq!(
        clock.status(context).blocked_reason(),
        Some(ControlPlaneAuthorityClockBlockedReason::WallClockForwardJump)
    );

    let restarted = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        4_501,
        Some(1_000),
        None,
    )
    .unwrap();
    assert!(!restarted.status(context).established());
}

#[test]
fn authority_clock_admin_requires_configured_admin_authentication() {
    let tmp = test_util::tempdir();
    let authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
        tmp.path().join("control-plane.state"),
    ))
    .unwrap();
    let mut clock = ControlPlaneAuthorityClock::new(None, 1_000, Some(50)).unwrap();
    let request = ControlPlaneRpcRequest {
        kind: ControlPlaneRpcKind::AuthorityClockStatus,
        payload: Vec::new(),
    };
    assert!(matches!(
        build_control_plane_authority_clock_admin_response(
            &authority,
            &mut clock,
            request,
            None,
            ControlPlaneAuthorityClockAdminSample::new(1_000, 1_000, Some(50)),
            |_, _| Ok(()),
            || Ok(1_000),
        ),
        Err(ControlPlaneError::RpcProtocol { diagnostic: ref message })
            if message.contains("requires configured admin authentication")
    ));
}

#[test]
fn authenticated_authority_clock_recovery_confirms_after_lost_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
        tmp.path().join("control-plane.state"),
    ))
    .unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let high_water = authority.snapshot().max_committed_timestamp_ms();
    let wall_ms = high_water.unwrap() + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS + 1;
    let mut clock = ControlPlaneAuthorityClock::new(high_water, wall_ms, Some(50)).unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        for request_number in 0..3 {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let now_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            let elapsed_ms = now_ms.saturating_sub(wall_ms);
            let response = build_control_plane_authority_clock_admin_response(
                &authority,
                &mut clock,
                request,
                Some(&verifier),
                ControlPlaneAuthorityClockAdminSample::new(
                    now_ms,
                    now_ms,
                    Some(50u64.saturating_add(elapsed_ms)),
                ),
                |_, _| Ok(()),
                || Ok(now_ms),
            )
            .unwrap();
            if request_number == 1 {
                drop(response);
                drop(stream);
            } else {
                write_control_plane_unix_response(&mut stream, response).unwrap();
            }
        }
        clock.status(authority.authority_clock_context().unwrap())
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let status =
        crate::clock::with_time_override(wall_ms, || client.reestablish_authority_clock(wall_ms))
            .unwrap();
    assert!(status.established());
    let server_status = server.join().unwrap();
    assert_eq!(status, server_status);
}

#[test]
fn authenticated_authority_clock_invalid_success_payload_is_unconfirmed() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let expected = ControlPlaneAuthorityClockStatus {
        generation: 7,
        established: false,
        blocked_reason: Some(ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged),
        committed_timestamp_high_water_ms: Some(1_000),
        bound_raft_leadership_term: None,
        current_raft_leadership_term: Some(3),
        local_raft_authority_leader: true,
        local_raft_authority_serving: true,
    };
    let server = std::thread::spawn(move || {
        let verifier = admin_auth_verifier("auth-cluster", "admin-1");
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        let request = verify_control_plane_unix_request(request, Some(&verifier), 2_000)
            .expect("authority-clock mutation request should authenticate");
        assert_eq!(request.kind, ControlPlaneRpcKind::ReestablishAuthorityClock);
        let response = build_control_plane_verified_response(
            request.kind,
            Ok(vec![0xff]),
            request.response_auth,
            2_000,
        )
        .unwrap();
        write_control_plane_unix_response(&mut stream, response).unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let error = crate::clock::with_time_override(2_000, || {
        client.reestablish_authority_clock_from_status_with_attempt_timeout(
            expected,
            2_000,
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(100),
        )
    })
    .unwrap_err();

    server.join().unwrap();
    assert!(matches!(
        error,
        ControlPlaneError::RpcUnconfirmed { ref message }
            if message.contains("authority-clock re-establishment")
                && message.contains("may have applied")
                && message.contains("confirmation predicate")
    ));
}

#[test]
fn authenticated_authority_clock_recovery_status_moves_past_response_loss() {
    let tmp = test_util::tempdir();
    let failing_socket = tmp.path().join("failing.sock");
    let healthy_socket = tmp.path().join("healthy.sock");
    let failing_listener = std::os::unix::net::UnixListener::bind(&failing_socket).unwrap();
    let healthy_listener = std::os::unix::net::UnixListener::bind(&healthy_socket).unwrap();
    let wall_ms = 2_000;
    let expected = ControlPlaneAuthorityClockStatus {
        generation: 7,
        established: false,
        blocked_reason: Some(ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged),
        committed_timestamp_high_water_ms: Some(1_000),
        bound_raft_leadership_term: None,
        current_raft_leadership_term: Some(3),
        local_raft_authority_leader: true,
        local_raft_authority_serving: true,
    };
    let failing_server = std::thread::spawn(move || {
        let (mut stream, _addr) = failing_listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::AuthorityClockStatus);
    });
    let healthy_server = std::thread::spawn(move || {
        let verifier = admin_auth_verifier("auth-cluster", "admin-1");
        let (mut stream, _addr) = healthy_listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        let request_now_ms = ControlPlaneAuthEnvelope::decode_frame(
            &request.payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        )
        .unwrap()
        .header()
        .issued_at_ms()
        .unwrap();
        let response = scripted_authenticated_authority_clock_response(
            request,
            &verifier,
            request_now_ms,
            Ok(expected),
        );
        write_control_plane_unix_response(&mut stream, response).unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::with_socket_paths([failing_socket, healthy_socket]).unwrap(),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let observed = crate::clock::with_time_override(wall_ms, || {
        client.retry_authority_clock_status_until(
            wall_ms,
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(100),
        )
    })
    .unwrap();

    failing_server.join().unwrap();
    healthy_server.join().unwrap();
    assert_eq!(observed, expected);
    assert_eq!(client.inner().preferred_endpoint_index(), 1);
}

#[test]
fn authenticated_authority_clock_recovery_retries_when_lost_request_did_not_apply() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let wall_ms = 2_000;
    let blocked = ControlPlaneAuthorityClockStatus {
        generation: 7,
        established: false,
        blocked_reason: Some(ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged),
        committed_timestamp_high_water_ms: Some(1_000),
        bound_raft_leadership_term: None,
        current_raft_leadership_term: Some(3),
        local_raft_authority_leader: true,
        local_raft_authority_serving: true,
    };
    let established = ControlPlaneAuthorityClockStatus {
        generation: 8,
        established: true,
        blocked_reason: None,
        bound_raft_leadership_term: Some(3),
        ..blocked
    };
    let server = std::thread::spawn(move || {
        let verifier = admin_auth_verifier("auth-cluster", "admin-1");
        for request_number in 0..5 {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let request_now_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            let expected_kind = if matches!(request_number, 1 | 4) {
                ControlPlaneRpcKind::ReestablishAuthorityClock
            } else {
                ControlPlaneRpcKind::AuthorityClockStatus
            };
            assert_eq!(request.kind, expected_kind);
            if request_number == 1 {
                drop(stream);
                continue;
            }
            let response = scripted_authenticated_authority_clock_response(
                request,
                &verifier,
                request_now_ms,
                Ok(if request_number == 4 {
                    established
                } else {
                    blocked
                }),
            );
            write_control_plane_unix_response(&mut stream, response).unwrap();
        }
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let status = crate::clock::with_time_override(wall_ms, || {
        client.reestablish_authority_clock_with_attempt_timeout(wall_ms, Duration::from_millis(100))
    })
    .unwrap();

    assert_eq!(status, established);
    server.join().unwrap();
}

#[test]
fn authenticated_authority_clock_recovery_confirms_while_success_response_is_delayed() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
        tmp.path().join("control-plane.state"),
    ))
    .unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let high_water = authority.snapshot().max_committed_timestamp_ms();
    let wall_ms = crate::clock::current_time_millis();
    let mut clock = ControlPlaneAuthorityClock::new(high_water, wall_ms, Some(50)).unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let mut delayed_response = None;
        let mut release_delayed_response = None;
        let mut confirmation_written = false;
        for _ in 0..64 {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let request_kind = request.kind;
            let now_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            let elapsed_ms = now_ms.saturating_sub(wall_ms);
            let response = build_control_plane_authority_clock_admin_response(
                &authority,
                &mut clock,
                request,
                Some(&verifier),
                ControlPlaneAuthorityClockAdminSample::new(
                    now_ms,
                    now_ms,
                    Some(50u64.saturating_add(elapsed_ms)),
                ),
                |_, _| Ok(()),
                || Ok(now_ms),
            )
            .unwrap();
            let established = clock
                .status(authority.authority_clock_context().unwrap())
                .established();
            if request_kind == ControlPlaneRpcKind::ReestablishAuthorityClock
                && delayed_response.is_none()
            {
                let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
                release_delayed_response = Some(release_tx);
                delayed_response = Some(std::thread::spawn(move || {
                    release_rx.recv().unwrap();
                    let _ = write_control_plane_unix_response(&mut stream, response);
                }));
            } else {
                if request_kind == ControlPlaneRpcKind::AuthorityClockStatus && established {
                    if let Some(release) = release_delayed_response.take() {
                        release.send(()).unwrap();
                    }
                }
                if write_control_plane_unix_response(&mut stream, response).is_ok()
                    && request_kind == ControlPlaneRpcKind::AuthorityClockStatus
                    && established
                {
                    confirmation_written = true;
                    break;
                }
            }
        }
        assert!(
            confirmation_written,
            "client should confirm the applied clock recovery"
        );
        if let Some(delayed_response) = delayed_response {
            delayed_response.join().unwrap();
        }
        clock.status(authority.authority_clock_context().unwrap())
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let status = client
        .reestablish_authority_clock_with_attempt_timeout(
            wall_ms,
            CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ATTEMPT_TIMEOUT,
        )
        .unwrap();
    assert!(status.established());
    assert_eq!(status, server.join().unwrap());
}

#[test]
fn authenticated_authority_clock_recovery_follows_new_leader_after_lost_response() {
    let tmp = test_util::tempdir();
    let old_socket = tmp.path().join("old-leader.sock");
    let new_socket = tmp.path().join("new-leader.sock");
    let old_listener = std::os::unix::net::UnixListener::bind(&old_socket).unwrap();
    let new_listener = std::os::unix::net::UnixListener::bind(&new_socket).unwrap();
    let wall_ms = 2_000;
    let old_blocked = ControlPlaneAuthorityClockStatus {
        generation: 2,
        established: false,
        blocked_reason: Some(ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged),
        committed_timestamp_high_water_ms: Some(1_000),
        bound_raft_leadership_term: None,
        current_raft_leadership_term: Some(2),
        local_raft_authority_leader: true,
        local_raft_authority_serving: true,
    };
    let new_blocked = ControlPlaneAuthorityClockStatus {
        generation: 7,
        current_raft_leadership_term: Some(3),
        ..old_blocked
    };
    let new_established = ControlPlaneAuthorityClockStatus {
        generation: 8,
        established: true,
        blocked_reason: None,
        bound_raft_leadership_term: Some(3),
        ..new_blocked
    };

    let old_server = std::thread::spawn(move || {
        let verifier = admin_auth_verifier("auth-cluster", "admin-1");
        for request_number in 0..3 {
            let (mut stream, _addr) = old_listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let request_now_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            let response = match request_number {
                0 => scripted_authenticated_authority_clock_response(
                    request,
                    &verifier,
                    request_now_ms,
                    Ok(old_blocked),
                ),
                1 => {
                    assert_eq!(request.kind, ControlPlaneRpcKind::ReestablishAuthorityClock);
                    drop(stream);
                    continue;
                }
                2 => {
                    let response = scripted_authenticated_authority_clock_response(
                        request,
                        &verifier,
                        request_now_ms,
                        Err(ControlPlaneError::AuthorityNotServing),
                    );
                    std::thread::sleep(Duration::from_millis(200));
                    let _ = write_control_plane_unix_response(&mut stream, response);
                    continue;
                }
                _ => unreachable!(),
            };
            write_control_plane_unix_response(&mut stream, response).unwrap();
        }
    });
    let new_server = std::thread::spawn(move || {
        let verifier = admin_auth_verifier("auth-cluster", "admin-1");
        for request_number in 0..3 {
            let (mut stream, _addr) = new_listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let request_now_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            let response = scripted_authenticated_authority_clock_response(
                request,
                &verifier,
                request_now_ms,
                Ok(if request_number == 2 {
                    new_established
                } else {
                    new_blocked
                }),
            );
            write_control_plane_unix_response(&mut stream, response).unwrap();
        }
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::with_socket_paths([old_socket, new_socket]).unwrap(),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let status = crate::clock::with_time_override(wall_ms, || {
        client.reestablish_authority_clock_with_attempt_timeout(wall_ms, Duration::from_millis(100))
    })
    .unwrap();

    assert_eq!(status, new_established);
    old_server.join().unwrap();
    new_server.join().unwrap();
}

#[test]
fn authenticated_authority_clock_recovery_checks_deadline_before_each_rpc() {
    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new("unused-test-socket"),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let expired = Instant::now() - Duration::from_millis(1);
    let expected = ControlPlaneAuthorityClockStatus {
        generation: 7,
        established: false,
        blocked_reason: Some(ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged),
        committed_timestamp_high_water_ms: Some(1_000),
        bound_raft_leadership_term: None,
        current_raft_leadership_term: Some(3),
        local_raft_authority_leader: true,
        local_raft_authority_serving: true,
    };

    let status_error = client
        .authority_clock_status_until(2_000, expired)
        .unwrap_err();
    let mutation_error = client
        .reestablish_authority_clock_from_status_with_attempt_timeout(
            expected,
            2_000,
            expired,
            CONTROL_PLANE_RPC_AUTHORITY_CLOCK_ATTEMPT_TIMEOUT,
        )
        .unwrap_err();

    assert!(matches!(
        status_error,
        ControlPlaneError::RpcRemote { diagnostic: ref message }
            if message.contains("authority-clock admin operation deadline expired")
    ));
    assert!(matches!(
        mutation_error,
        ControlPlaneError::RpcRemote { diagnostic: ref message }
            if message.contains("authority-clock admin operation deadline expired")
    ));
}

#[test]
fn authenticated_authority_clock_recovery_retries_transient_raft_admission_rejection() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let mut authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(
        tmp.path().join("control-plane.state"),
    ))
    .unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let high_water = authority.snapshot().max_committed_timestamp_ms();
    let wall_ms = high_water.unwrap() + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS + 1;
    let mut clock = ControlPlaneAuthorityClock::new(high_water, wall_ms, Some(50)).unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        for request_number in 0..4 {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let now_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            let response = if request_number == 1 {
                assert_eq!(request.kind, ControlPlaneRpcKind::ReestablishAuthorityClock);
                let request =
                    verify_control_plane_unix_request(request, Some(&verifier), now_ms).unwrap();
                build_control_plane_unix_admission_error_response(
                    request,
                    ControlPlaneError::AuthorityNotServing,
                    now_ms,
                )
                .unwrap()
            } else {
                let expected_kind = if request_number == 3 {
                    ControlPlaneRpcKind::ReestablishAuthorityClock
                } else {
                    ControlPlaneRpcKind::AuthorityClockStatus
                };
                assert_eq!(request.kind, expected_kind);
                let elapsed_ms = now_ms.saturating_sub(wall_ms);
                build_control_plane_authority_clock_admin_response(
                    &authority,
                    &mut clock,
                    request,
                    Some(&verifier),
                    ControlPlaneAuthorityClockAdminSample::new(
                        now_ms,
                        now_ms,
                        Some(50u64.saturating_add(elapsed_ms)),
                    ),
                    |_, _| Ok(()),
                    || Ok(now_ms),
                )
                .unwrap()
            };
            write_control_plane_unix_response(&mut stream, response).unwrap();
        }
        clock.status(authority.authority_clock_context().unwrap())
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let status =
        crate::clock::with_time_override(wall_ms, || client.reestablish_authority_clock(wall_ms))
            .unwrap();

    assert!(status.established());
    assert_eq!(status, server.join().unwrap());
}

#[test]
fn authenticated_control_plane_signs_admin_response_with_response_time() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let signer = admin_auth_credential("auth-cluster", "admin-1");
    let mut payload = Vec::new();
    write_pg_acting_set_request(&mut payload, PgId::new(7), &[NodeId::new(1)]).unwrap();
    let request = signed_admin_control_plane_request(
        ControlPlaneRpcKind::SetPgActingSet,
        &signer,
        payload,
        Some(2_000),
        Some(7_000),
    );

    let response = build_control_plane_unix_response_with_auth_and_response_clock(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
        || Ok(7_000),
    )
    .unwrap();
    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(tmp.path().join("unused.sock")),
        signer,
    );
    let response_payload = client
        .verify_admin_control_plane_response(
            ControlPlaneRpcKind::SetPgActingSet,
            7_000,
            &response.payload,
        )
        .unwrap();
    let response_payload = decode_control_plane_rpc_response(response_payload).unwrap();
    let mut reader = PayloadReader::new(&response_payload);
    assert!(ClusterEpoch::new(reader.read_u64().unwrap()).is_some());
    reader.finish().unwrap();
}

#[test]
fn authenticated_pg_admin_facade_signs_admin_pg_update() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let verifier_for_assert = verifier.clone();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            let response = build_control_plane_unix_response_with_auth_and_response_clock(
                &mut authority,
                request,
                issued_at_ms,
                Some(&verifier),
                || Ok(issued_at_ms),
            )
            .unwrap();
            write_control_plane_unix_response(&mut stream, response).unwrap();
        }
        assert_eq!(
            authority.snapshot().pg(PgId::new(7)).unwrap().acting_set(),
            &[NodeId::new(1)]
        );
    });

    let client = crate::ControlPlanePgAdminClient::new(
        UnixControlPlaneClient::new(&socket_path),
        Some(admin_auth_credential("auth-cluster", "admin-1")),
    );
    let cluster_epoch =
        crate::clock::with_time_override(2_000, || client.set_acting_set(7, vec![1])).unwrap();

    server.join().unwrap();
    assert!(cluster_epoch >= 2);
    let metrics = verifier_for_assert.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 2);
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        2
    );
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn authenticated_admin_pg_update_resamples_after_slow_preflight() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let (retry_clock, elapsed_ms) = AuthenticatedAdminRetryClock::with_elapsed_source(2_000);
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
            &request.payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        )
        .unwrap()
        .header()
        .issued_at_ms()
        .unwrap();
        assert_eq!(issued_at_ms, 2_000);
        elapsed_ms.store(6_000, std::sync::atomic::Ordering::SeqCst);
        let response = build_control_plane_unix_response_with_auth_and_response_clock(
            &mut authority,
            request,
            issued_at_ms,
            Some(&verifier),
            || Ok(8_000),
        )
        .unwrap();
        write_control_plane_unix_response(&mut stream, response).unwrap();

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::SetPgActingSet);
        let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
            &request.payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        )
        .unwrap()
        .header()
        .issued_at_ms()
        .unwrap();
        assert_eq!(issued_at_ms, 8_000);
        let response = build_control_plane_unix_response_with_auth_and_response_clock(
            &mut authority,
            request,
            issued_at_ms,
            Some(&verifier),
            || Ok(issued_at_ms),
        )
        .unwrap();
        write_control_plane_unix_response(&mut stream, response).unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let cluster_epoch = crate::clock::with_time_override(9_000, || {
        client.set_pg_acting_set_checked_with_retry_clock(
            PgId::new(7),
            vec![NodeId::new(1)],
            retry_clock,
        )
    })
    .unwrap();

    server.join().unwrap();
    assert!(cluster_epoch.get() >= 2);
}

#[test]
fn authenticated_admin_pg_update_observes_route_before_mutation_after_transient_preflight() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(7);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    let before = authority.snapshot().clone();

    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        for request_number in 0..3 {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let expected_kind = if request_number < 2 {
                ControlPlaneRpcKind::PgRuntimeMapSnapshot
            } else {
                ControlPlaneRpcKind::SetPgActingSet
            };
            assert_eq!(request.kind, expected_kind);
            let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            let response = if request_number == 0 {
                let verified =
                    verify_control_plane_unix_request(request, Some(&verifier), issued_at_ms)
                        .unwrap();
                build_control_plane_verified_response(
                    verified.kind,
                    Err(ControlPlaneError::PgHasNoServingPrimary {
                        pg_id: pg_id.get(),
                        cluster_epoch: authority.snapshot().cluster_epoch(),
                    }),
                    verified.response_auth,
                    issued_at_ms,
                )
                .unwrap()
            } else {
                build_control_plane_unix_response_with_auth_and_response_clock(
                    &mut authority,
                    request,
                    issued_at_ms,
                    Some(&verifier),
                    || Ok(issued_at_ms),
                )
                .unwrap()
            };
            write_control_plane_unix_response(&mut stream, response).unwrap();
            if request_number < 2 {
                assert_eq!(
                    authority.snapshot(),
                    &before,
                    "preflight retries must not submit the acting-set mutation"
                );
            }
        }
        authority.snapshot().clone()
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let cluster_epoch = crate::clock::with_time_override(3_000, || {
        client.set_pg_acting_set_checked(pg_id, vec![NodeId::new(1), NodeId::new(2)], 3_000)
    })
    .unwrap();

    let snapshot = server.join().unwrap();
    assert_eq!(snapshot.cluster_epoch(), cluster_epoch);
    assert_eq!(
        snapshot.pg(pg_id).unwrap().acting_set(),
        &[NodeId::new(1), NodeId::new(2)]
    );
}

#[test]
fn authenticated_admin_pg_update_retries_absent_pg_when_lost_request_did_not_apply() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(7);
    let previous_epoch = authority.snapshot().cluster_epoch();
    let expected_epoch = ClusterEpoch::new(previous_epoch.get() + 1).unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        for request_number in 0..4 {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let expected_kind = if matches!(request_number, 0 | 2) {
                ControlPlaneRpcKind::PgRuntimeMapSnapshot
            } else {
                ControlPlaneRpcKind::SetPgActingSet
            };
            assert_eq!(request.kind, expected_kind);

            if request_number == 1 {
                drop(request);
                drop(stream);
                assert_eq!(
                    authority.snapshot().cluster_epoch(),
                    previous_epoch,
                    "first request is lost before apply"
                );
                continue;
            }

            let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            let response = build_control_plane_unix_response_with_auth_and_response_clock(
                &mut authority,
                request,
                issued_at_ms,
                Some(&verifier),
                || Ok(issued_at_ms),
            )
            .unwrap();
            write_control_plane_unix_response(&mut stream, response).unwrap();

            if request_number == 2 {
                assert_eq!(
                    authority.snapshot().cluster_epoch(),
                    previous_epoch,
                    "typed UnknownPg confirms the lost mutation did not apply"
                );
            }
        }
        authority.snapshot().clone()
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let cluster_epoch = crate::clock::with_time_override(3_000, || {
        client.set_pg_acting_set_checked(pg_id, vec![NodeId::new(1), NodeId::new(2)], 3_000)
    })
    .unwrap();

    let snapshot = server.join().unwrap();
    assert_eq!(cluster_epoch, expected_epoch);
    assert_eq!(snapshot.cluster_epoch(), expected_epoch);
    assert_eq!(
        snapshot.pg(pg_id).unwrap().acting_set(),
        &[NodeId::new(1), NodeId::new(2)]
    );
}

#[test]
fn authenticated_admin_pg_update_rejects_successful_confirmation_omitting_target_pg() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(7);
    let previous_epoch = authority.snapshot().cluster_epoch();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        for request_number in 0..3 {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let expected_kind = if request_number == 1 {
                ControlPlaneRpcKind::SetPgActingSet
            } else {
                ControlPlaneRpcKind::PgRuntimeMapSnapshot
            };
            assert_eq!(request.kind, expected_kind);

            if request_number == 1 {
                drop(request);
                drop(stream);
                continue;
            }

            let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            let response = if request_number == 0 {
                build_control_plane_unix_response_with_auth_and_response_clock(
                    &mut authority,
                    request,
                    issued_at_ms,
                    Some(&verifier),
                    || Ok(issued_at_ms),
                )
                .unwrap()
            } else {
                let verified =
                    verify_control_plane_unix_request(request, Some(&verifier), issued_at_ms)
                        .unwrap();
                let mut malformed_payload = Vec::new();
                write_runtime_map_snapshot(
                    &mut malformed_payload,
                    &authority.runtime_map_snapshot(issued_at_ms).unwrap(),
                )
                .unwrap();
                build_control_plane_verified_response(
                    verified.kind,
                    Ok(malformed_payload),
                    verified.response_auth,
                    issued_at_ms,
                )
                .unwrap()
            };
            write_control_plane_unix_response(&mut stream, response).unwrap();
        }
        authority.snapshot().clone()
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let error = crate::clock::with_time_override(3_000, || {
        client.set_pg_acting_set_checked(pg_id, vec![NodeId::new(1), NodeId::new(2)], 3_000)
    })
    .unwrap_err();

    let snapshot = server.join().unwrap();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.as_str() == "PG-specific runtime map response omitted requested PG 7"
    ));
    assert_eq!(snapshot.cluster_epoch(), previous_epoch);
    assert!(snapshot.pg(pg_id).is_none());
}

#[test]
fn authenticated_admin_pg_update_retries_after_source_acknowledges_epoch() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let target_pg_id = PgId::new(7);
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(target_pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        target_pg_id.get(),
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            target_pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        target_pg_id.get(),
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    let preflight_epoch = authority.snapshot().cluster_epoch();

    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        for request_number in 0..4 {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let expected_kind = match request_number {
                0 | 2 => ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                1 | 3 => ControlPlaneRpcKind::SetPgActingSet,
                _ => unreachable!(),
            };
            assert_eq!(request.kind, expected_kind);
            let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            let response = build_control_plane_unix_response_with_auth_and_response_clock(
                &mut authority,
                request,
                issued_at_ms,
                Some(&verifier),
                || Ok(issued_at_ms),
            )
            .unwrap();
            write_control_plane_unix_response(&mut stream, response).unwrap();

            if request_number == 0 {
                authority
                    .set_pg_acting_set(PgId::new(8), vec![NodeId::new(1)])
                    .unwrap();
                assert!(authority.snapshot().cluster_epoch() > preflight_epoch);
            }
            if request_number == 1 {
                assert!(authority.snapshot().cluster_epoch() > preflight_epoch);
                heartbeat_with_pg_proof(
                    &mut authority,
                    1,
                    target_pg_id.get(),
                    PgState::Active,
                    active_proof,
                    false,
                    2_003,
                );
            }
        }
        authority.snapshot().clone()
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let cluster_epoch = crate::clock::with_time_override(3_000, || {
        client.set_pg_acting_set_checked(target_pg_id, vec![NodeId::new(1), NodeId::new(2)], 3_000)
    })
    .unwrap();

    let snapshot = server.join().unwrap();
    assert!(cluster_epoch.get() > preflight_epoch.get() + 1);
    assert_eq!(snapshot.cluster_epoch(), cluster_epoch);
    assert_eq!(
        snapshot.pg(target_pg_id).unwrap().acting_set(),
        &[NodeId::new(1), NodeId::new(2)]
    );
}

#[test]
fn authenticated_admin_pg_update_waits_across_target_pg_lifecycle_progress() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let target_pg_id = PgId::new(7);
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(target_pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        target_pg_id.get(),
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    let preflight_epoch = authority.snapshot().cluster_epoch();

    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        for request_number in 0..5 {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let expected_kind = match request_number {
                0 | 2 | 3 => ControlPlaneRpcKind::PgRuntimeMapSnapshot,
                1 | 4 => ControlPlaneRpcKind::SetPgActingSet,
                _ => unreachable!(),
            };
            assert_eq!(request.kind, expected_kind);

            if request_number == 1 {
                drop(request);
                drop(stream);
                authority
                    .complete_pg_peering(
                        target_pg_id,
                        NodeId::new(1),
                        node_incarnation(&authority, 1),
                        2_001,
                    )
                    .unwrap();
                heartbeat_with_pg_proof(
                    &mut authority,
                    1,
                    target_pg_id.get(),
                    PgState::Active,
                    active_proof,
                    false,
                    2_002,
                );
                assert_eq!(
                    authority.snapshot().pg(target_pg_id).unwrap().state(),
                    PgState::Active
                );
                assert!(authority.snapshot().cluster_epoch() > preflight_epoch);
                authority.snapshot.pgs.get_mut(&target_pg_id).unwrap().state = PgState::Degraded;
                continue;
            }

            let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            let response = build_control_plane_unix_response_with_auth_and_response_clock(
                &mut authority,
                request,
                issued_at_ms,
                Some(&verifier),
                || Ok(issued_at_ms),
            )
            .unwrap();
            write_control_plane_unix_response(&mut stream, response).unwrap();
            if request_number == 2 {
                authority.snapshot.pgs.get_mut(&target_pg_id).unwrap().state = PgState::Active;
            }
        }
        authority.snapshot().clone()
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let cluster_epoch = crate::clock::with_time_override(3_000, || {
        client.set_pg_acting_set_checked(target_pg_id, vec![NodeId::new(1), NodeId::new(2)], 3_000)
    })
    .unwrap();

    let snapshot = server.join().unwrap();
    assert_eq!(snapshot.cluster_epoch(), cluster_epoch);
    assert_eq!(
        snapshot.pg(target_pg_id).unwrap().acting_set(),
        &[NodeId::new(1), NodeId::new(2)]
    );
}

#[test]
fn pg_acting_set_retry_waits_for_pending_and_unsafe_routes_before_resubmission() {
    let pg_id = PgId::new(7);
    let reporting_node_id = NodeId::new(1);
    let retained_node_id = NodeId::new(2);
    let acting_set = vec![reporting_node_id, retained_node_id];
    let before = PgRouteSnapshot::reconstructed(
        ClusterEpoch::new(11).unwrap(),
        pg_id,
        reporting_node_id,
        acting_set,
        PgState::Active,
    );
    let pending = PendingMetadataCommandObservation::new(
        before.cluster_epoch(),
        NonZeroU64::new(3).unwrap(),
        0xfeed_beef,
    );
    let mut recovery = before.clone();
    recovery.cluster_epoch = ClusterEpoch::new(12).unwrap();
    recovery.state = PgState::Peering;
    recovery.pending_metadata_command_recovery = Some(PendingMetadataCommandRecovery::new(
        reporting_node_id,
        pending,
    ));

    let requested_acting_set = [retained_node_id];
    assert!(!requested_acting_set.contains(&reporting_node_id));
    assert_eq!(
        pg_acting_set_retry_route_disposition(&before, &recovery),
        PgActingSetRetryRouteDisposition::Wait
    );

    let mut recovered = recovery.clone();
    recovered.state = PgState::Active;
    recovered.pending_metadata_command_recovery = None;
    assert_eq!(
        pg_acting_set_retry_route_disposition(&before, &recovered),
        PgActingSetRetryRouteDisposition::RetryReady
    );

    for unsafe_state in [
        PgState::Degraded,
        PgState::Backfilling,
        PgState::Inconsistent,
    ] {
        let mut changed = before.clone();
        changed.state = unsafe_state;
        assert_eq!(
            pg_acting_set_retry_route_disposition(&before, &changed),
            PgActingSetRetryRouteDisposition::Wait
        );
    }

    let mut conflicting = before.clone();
    conflicting.acting_set = vec![retained_node_id];
    assert_eq!(
        pg_acting_set_retry_route_disposition(&before, &conflicting),
        PgActingSetRetryRouteDisposition::Conflict
    );
}

#[test]
fn pg_acting_set_command_rejects_removing_pending_reporter_before_mutation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(7);
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            active_proof,
            false,
            2_000,
        );
    }
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    let pending = test_pending_metadata_command(active_epoch);
    let mut pending_heartbeat = heartbeat_from_record(&authority, 1, active_epoch, 2_002);
    pending_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id,
        state: PgState::Active,
        metadata_proof: active_proof,
        pending_metadata_command: Some(pending),
    }];
    authority
        .refresh_node_heartbeat(pending_heartbeat, 2_002)
        .unwrap();
    assert_eq!(
        authority
            .snapshot()
            .pending_metadata_command_recoveries()
            .tasks(),
        &[PendingMetadataCommandRecoveryTask::new(
            pg_id,
            PendingMetadataCommandRecovery::new(NodeId::new(1), pending),
        )]
    );

    let before = authority.snapshot().clone();
    let error = authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(2)])
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::PgPeeringPendingMetadataCommand {
            pg_id: 7,
            node_id: 1,
            pending: actual,
            ..
        } if actual == pending
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn pg_acting_set_command_rejects_unsafe_lifecycle_state_before_mutation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(7);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    let baseline = authority.snapshot().clone();

    for state in [
        PgState::Degraded,
        PgState::Backfilling,
        PgState::Inconsistent,
    ] {
        let mut snapshot = baseline.clone();
        snapshot.pgs.get_mut(&pg_id).unwrap().state = state;
        let before = snapshot.clone();
        let error = snapshot
            .apply_control_plane_command(ControlPlaneCommand::SetPgActingSet {
                pg_id,
                acting_set: vec![NodeId::new(1), NodeId::new(2)],
            })
            .unwrap_err();

        assert!(matches!(
            error,
            ControlPlaneError::PgActingSetChangeNotReady {
                pg_id: 7,
                state: actual,
                ..
            } if actual == state
        ));
        assert_eq!(snapshot, before);
    }
}

#[test]
fn authenticated_admin_command_accepts_overlapping_credentials() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let old_credential = admin_auth_config_credential_with("admin-1", "admin", 1, "old-secret");
    let new_credential = admin_auth_config_credential_with("admin-1", "admin", 2, "new-secret");
    let old_signer = old_credential
        .scoped_for_cluster("auth-cluster")
        .expect("old admin credential should scope");
    let verifier = ControlPlaneUnixAuthVerifier::new_empty("auth-cluster")
        .unwrap()
        .with_admin_credentials(vec![old_credential, new_credential])
        .unwrap();
    let verifier_for_assert = verifier.clone();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let (server_ready_tx, server_ready_rx) = std::sync::mpsc::sync_channel(0);
    let server = std::thread::spawn(move || {
        server_ready_tx
            .send(())
            .expect("admin auth test client should wait for server readiness");
        for authority_now_ms in [2_000, 2_000] {
            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream_with_auth(
                &mut authority,
                &mut stream,
                authority_now_ms,
                &verifier,
            )
            .unwrap();
        }
        assert_eq!(
            authority.snapshot().pg(PgId::new(7)).unwrap().acting_set(),
            &[NodeId::new(1)]
        );
    });
    server_ready_rx
        .recv()
        .expect("admin auth test server should become ready");

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        old_signer,
    );
    let mut preflight_payload = Vec::new();
    write_pg_id_request(&mut preflight_payload, PgId::new(7));
    let preflight_error = client
        .send_admin_request_with_read_timeout_and_clocks(
            ControlPlaneRpcKind::PgRuntimeMapSnapshot,
            preflight_payload,
            CONTROL_PLANE_RPC_IO_TIMEOUT,
            || Ok(2_000),
            || Ok(2_000),
        )
        .unwrap_err();
    assert!(matches!(
        preflight_error,
        ControlPlaneError::UnknownPg { pg_id: 7 }
    ));
    let mut update_payload = Vec::new();
    write_pg_acting_set_request(&mut update_payload, PgId::new(7), &[NodeId::new(1)]).unwrap();
    let response = client
        .send_admin_request_with_read_timeout_and_clocks(
            ControlPlaneRpcKind::SetPgActingSet,
            update_payload,
            CONTROL_PLANE_RPC_IO_TIMEOUT,
            || Ok(2_000),
            || Ok(2_000),
        )
        .unwrap();
    let mut reader = PayloadReader::new(&response);
    let cluster_epoch = ClusterEpoch::new(reader.read_u64().unwrap()).unwrap();
    reader.finish().unwrap();

    server.join().unwrap();
    assert!(cluster_epoch.get() >= 2);
    let status = verifier_for_assert.status_snapshot();
    assert_eq!(status.admin_credentials().len(), 2);
    let metrics = verifier_for_assert.metrics_snapshot();
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        2
    );
    assert_eq!(metrics.rejected_total(), 0);
}

fn runtime_map_serving_gap_test_errors() -> [ControlPlaneError; 4] {
    [
        ControlPlaneError::PgHasNoServingPrimary {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
        },
        ControlPlaneError::PgPrimaryMissingActiveObservation {
            pg_id: 0,
            node_id: 1,
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
        },
        ControlPlaneError::PgPrimaryObservationNotActive {
            pg_id: 0,
            node_id: 1,
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            state: PgState::Peering,
        },
        ControlPlaneError::PgPeeringPendingMetadataCommand {
            pg_id: 0,
            node_id: 1,
            cluster_epoch: ClusterEpoch::new(3).unwrap(),
            pending: PendingMetadataCommandObservation::new(
                ClusterEpoch::new(2).unwrap(),
                NonZeroU64::new(1).unwrap(),
                0xfeed_beef,
            ),
        },
    ]
}

#[test]
fn authenticated_admin_pg_update_confirms_after_all_typed_serving_gaps() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let verifier_for_assert = verifier.clone();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
            &request.payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        )
        .unwrap()
        .header()
        .issued_at_ms()
        .unwrap();
        let response = build_control_plane_unix_response_with_auth_and_response_clock(
            &mut authority,
            request,
            issued_at_ms,
            Some(&verifier),
            || Ok(issued_at_ms),
        )
        .unwrap();
        write_control_plane_unix_response(&mut stream, response).unwrap();

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::SetPgActingSet);
        let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
            &request.payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        )
        .unwrap()
        .header()
        .issued_at_ms()
        .unwrap();
        build_control_plane_unix_response_with_auth_and_response_clock(
            &mut authority,
            request,
            issued_at_ms,
            Some(&verifier),
            || Ok(issued_at_ms),
        )
        .expect("dropped authenticated admin mutation should still apply");
        drop(stream);

        for error in runtime_map_serving_gap_test_errors() {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            let request =
                verify_control_plane_unix_request(request, Some(&verifier), issued_at_ms).unwrap();
            let response =
                build_control_plane_unix_admission_error_response(request, error, issued_at_ms)
                    .unwrap();
            write_control_plane_unix_response(&mut stream, response).unwrap();
        }

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
            &request.payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        )
        .unwrap()
        .header()
        .issued_at_ms()
        .unwrap();
        let response = build_control_plane_unix_response_with_auth_and_response_clock(
            &mut authority,
            request,
            issued_at_ms,
            Some(&verifier),
            || Ok(issued_at_ms),
        )
        .unwrap();
        write_control_plane_unix_response(&mut stream, response).unwrap();
        assert_eq!(
            authority.snapshot().pg(PgId::new(7)).unwrap().acting_set(),
            &[NodeId::new(1)]
        );
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let cluster_epoch = client
        .set_pg_acting_set_checked(PgId::new(7), vec![NodeId::new(1)], 2_000)
        .unwrap();

    server.join().unwrap();
    assert!(cluster_epoch.get() >= 2);
    let metrics = verifier_for_assert.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 7);
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        7
    );
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn authenticated_unix_control_plane_client_treats_unsigned_admin_response_as_unconfirmed() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let (kind, payload) = read_control_plane_rpc_frame(&mut stream).unwrap();
        assert_eq!(kind, ControlPlaneRpcKind::SetPgActingSet);
        ControlPlaneAuthEnvelope::decode_frame(&payload, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)
            .expect("admin request should be auth-wrapped");
        let response = encode_control_plane_rpc_response(Ok(Vec::new())).unwrap();
        write_control_plane_rpc_frame(&mut stream, kind, &response).unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let error = client
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)], 2_000)
        .expect_err("unsigned admin response should be rejected");

    server.join().unwrap();
    assert!(matches!(
        error,
        ControlPlaneError::RpcUnconfirmed { ref message }
            if message.contains("PG acting-set update")
                && message.contains("may have applied")
                && message.contains("confirmation predicate")
    ));
}

#[test]
fn authenticated_unix_control_plane_client_verifies_admin_response_at_receive_time() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream_with_auth(&mut authority, &mut stream, 2_001, &verifier)
            .unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let mut payload = Vec::new();
    write_pg_acting_set_request(&mut payload, PgId::new(7), &[NodeId::new(1)]).unwrap();
    let mut timestamps = [2_000, 2_001].into_iter();
    let response = client
        .send_admin_request_with_read_timeout_and_clock(
            ControlPlaneRpcKind::SetPgActingSet,
            payload,
            CONTROL_PLANE_RPC_IO_TIMEOUT,
            || {
                timestamps.next().ok_or_else(|| {
                    ControlPlaneError::rpc_protocol(
                        "test admin response clock exhausted".to_owned(),
                    )
                })
            },
        )
        .unwrap();

    server.join().unwrap();
    let mut reader = PayloadReader::new(&response);
    assert!(ClusterEpoch::new(reader.read_u64().unwrap()).is_some());
    reader.finish().unwrap();
}

#[test]
fn authenticated_unix_control_plane_client_accepts_admin_response_with_supported_clock_skew() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream_with_auth(
            &mut authority,
            &mut stream,
            2_000 + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
            &verifier,
        )
        .unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let mut payload = Vec::new();
    write_pg_acting_set_request(&mut payload, PgId::new(7), &[NodeId::new(1)]).unwrap();
    let response = client
        .send_admin_request_with_read_timeout_and_clock(
            ControlPlaneRpcKind::SetPgActingSet,
            payload,
            CONTROL_PLANE_RPC_IO_TIMEOUT,
            || Ok(2_000),
        )
        .unwrap();

    server.join().unwrap();
    let mut reader = PayloadReader::new(&response);
    assert!(ClusterEpoch::new(reader.read_u64().unwrap()).is_some());
    reader.finish().unwrap();
}

#[test]
fn authenticated_unix_control_plane_client_signs_admin_request_at_caller_time() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream_with_auth(&mut authority, &mut stream, 2_000, &verifier)
            .unwrap();
        authority
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let cluster_epoch = crate::clock::with_time_override(2_000, || {
        client.set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)], 2_000)
    })
    .unwrap();

    let authority = server.join().unwrap();
    assert!(cluster_epoch.get() >= 2);
    assert_eq!(
        authority.snapshot().pg(PgId::new(7)).unwrap().acting_set(),
        &[NodeId::new(1)]
    );
}

#[test]
fn authenticated_unix_control_plane_client_resamples_wall_clock_for_admin_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream_with_auth(&mut authority, &mut stream, 4_000, &verifier)
            .unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let cluster_epoch = crate::clock::with_time_override(4_000, || {
        client.set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)], 2_000)
    })
    .unwrap();

    server.join().unwrap();
    assert!(cluster_epoch.get() >= 2);
}

#[test]
fn authenticated_unix_control_plane_client_allows_small_admin_response_issue_skew() {
    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new("/tmp/unused-control-plane.sock"),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let response_credential = client
        .credential()
        .admin_control_plane_response_credential_for_admin()
        .unwrap();
    let payload = sign_control_plane_response_payload(
        ControlPlaneRpcKind::SetPgActingSet,
        &response_credential,
        client.credential().principal().clone(),
        ControlPlaneAuthOperation::AdminControlPlaneResponse,
        2_001,
        encode_control_plane_rpc_response(Ok(Vec::new())).unwrap(),
    )
    .unwrap();

    let response = client
        .verify_admin_control_plane_response(ControlPlaneRpcKind::SetPgActingSet, 2_000, &payload)
        .unwrap();
    let response = decode_control_plane_rpc_response(response).unwrap();

    let reader = PayloadReader::new(&response);
    reader.finish().unwrap();
}

#[test]
fn authenticated_unix_control_plane_client_verifies_admin_error_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream_with_auth(&mut authority, &mut stream, 2_000, &verifier)
            .unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let error = crate::clock::with_time_override(2_000, || {
        client.set_pg_acting_set(PgId::new(7), vec![NodeId::new(99)], 2_000)
    })
    .expect_err("signed admin error response should decode after auth verification");

    server.join().unwrap();
    assert!(
        matches!(
            error,
            ControlPlaneError::UnknownActingSetNode {
                pg_id: 7,
                node_id: 99,
            }
        ),
        "unexpected error: {error}"
    );
}

#[derive(Default)]
struct RecordingRaftAdminAuthority {
    transferred_to: Vec<u64>,
    snapshot_purge_triggers: usize,
    election_triggers: usize,
}

impl RecordingRaftAdminAuthority {
    fn unsupported_snapshot_command() -> Result<ClusterControlSnapshot, ControlPlaneError> {
        Err(ControlPlaneError::rpc_remote(
            "recording Raft admin authority only supports Raft admin triggers".to_owned(),
        ))
    }
}

impl ControlPlaneAdmin for RecordingRaftAdminAuthority {
    fn set_pg_acting_set(
        &mut self,
        _pg_id: PgId,
        _acting_set: Vec<NodeId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        Self::unsupported_snapshot_command()
    }

    fn fence_pg_for_metadata_transfer(
        &mut self,
        _pg_id: PgId,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        Self::unsupported_snapshot_command()
    }

    fn fence_pg_for_metadata_transfer_with_source_lease(
        &mut self,
        _pg_id: PgId,
    ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError> {
        Err(ControlPlaneError::rpc_remote(
            "recording Raft admin authority only supports Raft admin triggers".to_owned(),
        ))
    }

    fn set_pg_acting_set_with_metadata_transfer(
        &mut self,
        _pg_id: PgId,
        _acting_set: Vec<NodeId>,
        _transfer: PgMetadataTransferProof,
        _expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        Self::unsupported_snapshot_command()
    }

    fn transfer_raft_leadership_to(&mut self, node_id: u64) -> Result<(), ControlPlaneError> {
        self.transferred_to.push(node_id);
        Ok(())
    }

    fn trigger_raft_snapshot_and_purge(&mut self) -> Result<Option<u64>, ControlPlaneError> {
        self.snapshot_purge_triggers += 1;
        Ok(Some(42))
    }

    fn trigger_raft_election(&mut self) -> Result<(), ControlPlaneError> {
        self.election_triggers += 1;
        Ok(())
    }
}

impl ControlPlaneHeartbeatRuntimeMapSource for RecordingRaftAdminAuthority {
    fn refresh_node_heartbeat(
        &mut self,
        _heartbeat: NodeHeartbeat,
        _authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        Err(ControlPlaneError::rpc_remote(
            "recording Raft admin authority does not support heartbeats".to_owned(),
        ))
    }
}

impl ControlPlaneRuntimeMapSource for RecordingRaftAdminAuthority {
    fn runtime_map_snapshot(
        &self,
        _authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        Err(ControlPlaneError::rpc_remote(
            "recording Raft admin authority does not support runtime maps".to_owned(),
        ))
    }

    fn runtime_map_status(
        &self,
        _authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        Ok(ControlPlaneRuntimeMapStatus::new(
            ClusterEpoch::INITIAL,
            0,
            0,
        ))
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        _pg_id: PgId,
        _authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        Err(ControlPlaneError::rpc_remote(
            "recording Raft admin authority does not support serving PG runtime maps".to_owned(),
        ))
    }
}

fn assert_raft_admin_trigger_unconfirmed(error: ControlPlaneError, expected_operation: &str) {
    assert!(
        matches!(error, ControlPlaneError::RpcUnconfirmed { ref message }
        if message.contains(expected_operation)
            && message.contains("may have applied")
            && message.contains("confirmation predicate")),
        "unexpected error: {error}"
    );
}

#[test]
fn authenticated_unix_control_plane_client_signs_raft_admin_request_at_caller_time() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let mut authority = RecordingRaftAdminAuthority::default();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream_with_auth(&mut authority, &mut stream, 2_000, &verifier)
            .unwrap();
        authority
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    crate::clock::with_time_override(2_000, || client.transfer_raft_leadership_to(102, 2_000))
        .unwrap();

    let authority = server.join().unwrap();
    assert_eq!(authority.transferred_to, vec![102]);
}

#[test]
fn authenticated_raft_admin_triggers_fail_unconfirmed_after_lost_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let verifier_for_assert = verifier.clone();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let mut authority = RecordingRaftAdminAuthority::default();
        for expected_kind in [
            ControlPlaneRpcKind::TransferRaftLeadership,
            ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge,
            ControlPlaneRpcKind::TriggerRaftElection,
        ] {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            assert_eq!(request.kind, expected_kind);
            build_control_plane_unix_response_with_auth(
                &mut authority,
                request,
                2_000,
                Some(&verifier),
            )
            .expect("recording Raft admin trigger should apply before response loss");
            drop(stream);
        }
        authority
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    assert_raft_admin_trigger_unconfirmed(
        client.transfer_raft_leadership_to(102, 2_000).unwrap_err(),
        "leadership transfer",
    );
    assert_raft_admin_trigger_unconfirmed(
        client.trigger_raft_snapshot_and_purge(2_000).unwrap_err(),
        "snapshot/purge trigger",
    );
    assert_raft_admin_trigger_unconfirmed(
        client.trigger_raft_election(2_000).unwrap_err(),
        "election trigger",
    );

    let authority = server.join().unwrap();
    assert_eq!(authority.transferred_to, vec![102]);
    assert_eq!(authority.snapshot_purge_triggers, 1);
    assert_eq!(authority.election_triggers, 1);
    let metrics = verifier_for_assert.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 3);
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        3
    );
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn operator_raft_admin_facade_preserves_unconfirmed_outcomes_after_application() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let mut authority = RecordingRaftAdminAuthority::default();
        for expected_kind in [
            ControlPlaneRpcKind::TransferRaftLeadership,
            ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge,
            ControlPlaneRpcKind::TriggerRaftElection,
        ] {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            assert_eq!(request.kind, expected_kind);
            build_control_plane_unix_response_with_auth(
                &mut authority,
                request,
                2_000,
                Some(&verifier),
            )
            .expect("recording Raft admin trigger should apply before response loss");
            drop(stream);
        }
        authority
    });

    let client = crate::control_plane_operator_admin::ControlPlaneRaftAdminClient::new(
        UnixControlPlaneClient::new(&socket_path),
        Some(admin_auth_credential("auth-cluster", "admin-1")),
    );
    let errors = crate::clock::with_time_override(2_000, || {
        [
            client.transfer_leadership_to(102).unwrap_err(),
            client.trigger_snapshot_and_purge().unwrap_err(),
            client.trigger_election().unwrap_err(),
        ]
    });

    for (error, operation) in errors.into_iter().zip([
        "Raft leadership transfer",
        "Raft snapshot/purge trigger",
        "Raft election trigger",
    ]) {
        assert_eq!(
            error.to_string(),
            format!(
                "control-plane {operation} may have applied but could not be confirmed; do not retry without an operation-specific confirmation check"
            )
        );
        assert!(std::error::Error::source(&error).is_none());
        assert_eq!(
            format!("{error:?}"),
            format!(
                "ControlPlaneOperatorAdminError {{ operation: \"{operation}\", diagnostic: \"<redacted>\" }}"
            )
        );
    }

    let authority = server.join().unwrap();
    assert_eq!(authority.transferred_to, vec![102]);
    assert_eq!(authority.snapshot_purge_triggers, 1);
    assert_eq!(authority.election_triggers, 1);
}

#[derive(Clone, Copy)]
enum AppliedRaftAdminResponseFault {
    WrongOuterKind,
    MalformedAuthenticatedEnvelope,
    InvalidAuthentication,
    InvalidOperationPayload,
}

#[test]
fn operator_raft_admin_facade_treats_every_invalid_post_application_response_as_unconfirmed() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let mut authority = RecordingRaftAdminAuthority::default();
        for fault in [
            AppliedRaftAdminResponseFault::WrongOuterKind,
            AppliedRaftAdminResponseFault::MalformedAuthenticatedEnvelope,
            AppliedRaftAdminResponseFault::InvalidAuthentication,
            AppliedRaftAdminResponseFault::InvalidOperationPayload,
        ] {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            let request =
                verify_control_plane_unix_request(request, Some(&verifier), 2_000).unwrap();
            let VerifiedControlPlaneRpcRequest {
                kind,
                payload,
                response_auth,
            } = request;
            assert_eq!(kind, ControlPlaneRpcKind::TransferRaftLeadership);
            let mut reader = PayloadReader::new(&payload);
            let node_id = reader.read_u64().unwrap();
            reader.finish().unwrap();
            authority.transfer_raft_leadership_to(node_id).unwrap();

            let mut response = match fault {
                AppliedRaftAdminResponseFault::MalformedAuthenticatedEnvelope => {
                    ControlPlaneRpcResponse {
                        kind,
                        payload: vec![0xff],
                    }
                }
                AppliedRaftAdminResponseFault::InvalidOperationPayload => {
                    build_control_plane_verified_response(
                        kind,
                        Ok(vec![0xff]),
                        response_auth,
                        2_000,
                    )
                    .unwrap()
                }
                AppliedRaftAdminResponseFault::WrongOuterKind
                | AppliedRaftAdminResponseFault::InvalidAuthentication => {
                    build_control_plane_verified_response(
                        kind,
                        Ok(Vec::new()),
                        response_auth,
                        2_000,
                    )
                    .unwrap()
                }
            };
            match fault {
                AppliedRaftAdminResponseFault::WrongOuterKind => {
                    response.kind = ControlPlaneRpcKind::TriggerRaftElection;
                }
                AppliedRaftAdminResponseFault::InvalidAuthentication => {
                    let last = response
                        .payload
                        .last_mut()
                        .expect("authenticated response should not be empty");
                    *last ^= 0xff;
                }
                AppliedRaftAdminResponseFault::MalformedAuthenticatedEnvelope
                | AppliedRaftAdminResponseFault::InvalidOperationPayload => {}
            }
            write_control_plane_unix_response(&mut stream, response).unwrap();
        }

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        let request = verify_control_plane_unix_request(request, Some(&verifier), 2_000).unwrap();
        let response = build_control_plane_unix_admission_error_response(
            request,
            ControlPlaneError::UnknownNode { node_id: 999 },
            2_000,
        )
        .unwrap();
        write_control_plane_unix_response(&mut stream, response).unwrap();
        authority
    });

    let client = crate::control_plane_operator_admin::ControlPlaneRaftAdminClient::new(
        UnixControlPlaneClient::new(&socket_path),
        Some(admin_auth_credential("auth-cluster", "admin-1")),
    );
    crate::clock::with_time_override(2_000, || {
        for node_id in 201..=204 {
            let error = client.transfer_leadership_to(node_id).unwrap_err();
            assert_eq!(
                error.to_string(),
                "control-plane Raft leadership transfer may have applied but could not be confirmed; do not retry without an operation-specific confirmation check"
            );
            assert!(std::error::Error::source(&error).is_none());
        }

        let rejected = client.transfer_leadership_to(999).unwrap_err();
        assert_eq!(
            rejected.to_string(),
            "control-plane Raft leadership transfer failed"
        );
    });

    let authority = server.join().unwrap();
    assert_eq!(authority.transferred_to, vec![201, 202, 203, 204]);
}

#[test]
fn operator_plain_raft_admin_facade_preserves_post_application_ambiguity() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let mut authority = RecordingRaftAdminAuthority::default();
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        let mut response =
            build_control_plane_unix_response(&mut authority, request, 2_000).unwrap();
        response.kind = ControlPlaneRpcKind::TriggerRaftElection;
        write_control_plane_unix_response(&mut stream, response).unwrap();
        authority
    });

    let client = crate::control_plane_operator_admin::ControlPlaneRaftAdminClient::new(
        UnixControlPlaneClient::new(&socket_path),
        None,
    );
    let error = client.transfer_leadership_to(205).unwrap_err();
    assert_eq!(
        error.to_string(),
        "control-plane Raft leadership transfer may have applied but could not be confirmed; do not retry without an operation-specific confirmation check"
    );
    assert!(std::error::Error::source(&error).is_none());

    let authority = server.join().unwrap();
    assert_eq!(authority.transferred_to, vec![205]);
}

#[test]
fn authenticated_unix_control_plane_client_signs_metadata_transfer_admin_commands() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(43), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(43),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    let active_epoch = authority.snapshot().cluster_epoch();
    let imported_proof = PgMetadataProof::current(9, 12, 11);
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        active_proof,
        imported_proof,
    );
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let verifier_for_assert = verifier.clone();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        for authority_now_ms in [2_003, 2_004] {
            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream_with_auth(
                &mut authority,
                &mut stream,
                authority_now_ms,
                &verifier,
            )
            .unwrap();
        }
        let pg = authority.snapshot().pg(PgId::new(43)).unwrap();
        assert_eq!(pg.state(), PgState::Peering);
        assert_eq!(pg.acting_set(), &[NodeId::new(2)]);
        assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let clock = crate::clock::test_time_override_guard(2_003);
    let fenced = client
        .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(PgId::new(43), 2_003)
        .unwrap();
    let fence_epoch = fenced.runtime_map().cluster_epoch();
    let expected_transfer_epoch = ClusterEpoch::new(fence_epoch.get() + 1).unwrap();
    clock.set(2_004);
    let runtime_map = client
        .set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
            PgId::new(43),
            vec![NodeId::new(2)],
            transfer,
            expected_transfer_epoch,
            2_004,
        )
        .unwrap();

    server.join().unwrap();
    assert_eq!(runtime_map.cluster_epoch(), expected_transfer_epoch);
    let route = runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(43))
        .unwrap();
    assert_eq!(route.acting_set(), &[NodeId::new(2)]);
    assert_eq!(route.peering_metadata_transfer(), Some(transfer));
    let metrics = verifier_for_assert.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 2);
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        2
    );
    assert_eq!(metrics.rejected_total(), 0);
}

fn assert_authenticated_metadata_transfer_fence_retries_before_and_after_apply<S>(
    client: UnixControlPlaneClient,
    mut accept: impl FnMut() -> S + Send + 'static,
) where
    S: std::io::Read + std::io::Write + Send + 'static,
{
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(43), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(43),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    let active_epoch = authority.snapshot().cluster_epoch();
    let expected_fence_epoch = ClusterEpoch::new(active_epoch.get() + 1).unwrap();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let verifier_for_assert = verifier.clone();
    let server = std::thread::spawn(move || {
        for request_number in 0..3 {
            let mut stream = accept();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            assert_eq!(
                request.kind,
                ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap
            );
            let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
                &request.payload,
                CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
            )
            .unwrap()
            .header()
            .issued_at_ms()
            .unwrap();
            if request_number == 0 {
                verify_control_plane_unix_request(request, Some(&verifier), issued_at_ms)
                    .expect("first authenticated fence request should verify");
                drop(stream);
                assert_eq!(authority.snapshot().cluster_epoch(), active_epoch);
                continue;
            }

            let response = build_control_plane_unix_response_with_auth_and_response_clock(
                &mut authority,
                request,
                issued_at_ms,
                Some(&verifier),
                || Ok(issued_at_ms),
            )
            .expect("retried authenticated fence should apply or confirm");
            if request_number == 1 {
                drop(response);
                drop(stream);
            } else {
                write_control_plane_unix_response(&mut stream, response).unwrap();
                stream.flush().unwrap();
            }
            assert_eq!(authority.snapshot().cluster_epoch(), expected_fence_epoch);
        }
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        client,
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let fenced = client
        .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(
            PgId::new(43),
            crate::clock::current_time_millis(),
        )
        .unwrap();

    server.join().unwrap();
    assert_eq!(fenced.runtime_map().cluster_epoch(), expected_fence_epoch);
    assert_eq!(fenced.source_primary_lease_deadline_ms(), Some(2_102));
    let metrics = verifier_for_assert.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 3);
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn authenticated_unix_metadata_transfer_fence_retries_before_and_after_apply() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    assert_authenticated_metadata_transfer_fence_retries_before_and_after_apply(
        UnixControlPlaneClient::new(socket_path),
        move || listener.accept().unwrap().0,
    );
}

#[test]
fn authenticated_tls_metadata_transfer_fence_retries_before_and_after_apply() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = control_plane_test_tls_endpoint(listener.local_addr().unwrap());
    let server_config = control_plane_test_tls_server_config();
    assert_authenticated_metadata_transfer_fence_retries_before_and_after_apply(
        UnixControlPlaneClient::with_endpoints([endpoint]).unwrap(),
        move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let connection = rustls::ServerConnection::new(Arc::clone(&server_config)).unwrap();
            rustls::StreamOwned::new(connection, stream)
        },
    );
}

#[test]
fn authenticated_admin_metadata_transfer_confirms_after_lost_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(43), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(43),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    let active_epoch = authority.snapshot().cluster_epoch();
    let expected_transfer_epoch = ClusterEpoch::new(active_epoch.get() + 1).unwrap();
    let imported_proof = PgMetadataProof::current(9, 12, 11);
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        active_proof,
        imported_proof,
    );
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let verifier_for_assert = verifier.clone();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(
            request.kind,
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap
        );
        let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
            &request.payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        )
        .unwrap()
        .header()
        .issued_at_ms()
        .unwrap();
        let response = build_control_plane_unix_response_with_auth_and_response_clock(
            &mut authority,
            request,
            issued_at_ms,
            Some(&verifier),
            || Ok(issued_at_ms),
        )
        .expect("authenticated metadata-transfer install should apply before response loss");
        assert_eq!(
            response.kind,
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap
        );
        assert_eq!(
            authority.snapshot().cluster_epoch(),
            expected_transfer_epoch
        );
        drop(response);
        drop(stream);
        authority
            .set_pg_acting_set(PgId::new(44), vec![NodeId::new(1)])
            .expect("unrelated PG epoch advance should succeed");

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        let issued_at_ms = ControlPlaneAuthEnvelope::decode_frame(
            &request.payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        )
        .unwrap()
        .header()
        .issued_at_ms()
        .unwrap();
        let runtime_map = authority
            .pg_runtime_map_snapshot(PgId::new(43), issued_at_ms)
            .unwrap();
        assert!(
            metadata_transfer_install_applied(
                &runtime_map,
                PgId::new(43),
                &[NodeId::new(2)],
                transfer,
                expected_transfer_epoch,
            ),
            "server-side confirmation route should be observable before response"
        );
        let response = build_control_plane_unix_response_with_auth_and_response_clock(
            &mut authority,
            request,
            issued_at_ms,
            Some(&verifier),
            || Ok(issued_at_ms),
        )
        .unwrap();
        write_control_plane_unix_response(&mut stream, response).unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        admin_auth_credential("auth-cluster", "admin-1"),
    );
    let runtime_map = client.set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
        PgId::new(43),
        vec![NodeId::new(2)],
        transfer,
        expected_transfer_epoch,
        2_003,
    );

    server.join().unwrap();
    let runtime_map = runtime_map.unwrap();
    assert!(runtime_map.cluster_epoch() > expected_transfer_epoch);
    let route = runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(43))
        .unwrap();
    assert_eq!(route.cluster_epoch(), runtime_map.cluster_epoch());
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(route.acting_set(), &[NodeId::new(2)]);
    assert_eq!(route.peering_metadata_transfer(), Some(transfer));
    let metrics = verifier_for_assert.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 2);
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        2
    );
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn authenticated_control_plane_rejects_frontend_credential_for_admin_command() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let before = authority.snapshot().clone();
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let signer = frontend_auth_credential("auth-cluster", "frontend-1");
    let mut payload = Vec::new();
    write_pg_acting_set_request(&mut payload, PgId::new(7), &[NodeId::new(1)]).unwrap();
    let request = signed_admin_control_plane_request(
        ControlPlaneRpcKind::SetPgActingSet,
        &signer,
        payload,
        Some(1_999),
        Some(2_999),
    );

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message } if message.contains("source is not an admin")),
        "unexpected error: {error}"
    );
    assert_eq!(authority.snapshot(), &before);
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        1
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::WrongRole),
        1
    );
}

#[test]
fn authenticated_unix_control_plane_client_refreshes_storage_node_heartbeat() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let heartbeat_epoch = authority.snapshot().cluster_epoch();
    let credential = storage_node_auth_credential("auth-cluster", 1, 42);
    let verifier =
        storage_node_auth_verifier("auth-cluster", vec![storage_node_auth_node_credential(1)]);
    let verifier_for_assert = verifier.clone();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream_with_auth(&mut authority, &mut stream, 2_000, &verifier)
            .unwrap();
    });

    let mut client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        credential,
    );
    let mut clock = [2_000, 2_050].into_iter();
    let refresh = client
        .refresh_node_heartbeat_with_clock(
            NodeHeartbeat {
                node_id: NodeId::new(1),
                node_incarnation: 42,
                endpoint: "/tmp/argmin-node-1.sock".to_owned(),
                observed_epoch: heartbeat_epoch,
                requested_lease_duration_ms: 100,
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                cluster_map_history_route_references: Default::default(),
                pg_observations: Vec::new(),
            },
            || {
                clock.next().ok_or_else(|| {
                    ControlPlaneError::rpc_protocol(
                        "test heartbeat auth clock exhausted".to_owned(),
                    )
                })
            },
        )
        .unwrap();

    server.join().unwrap();
    assert_eq!(refresh.lease().node_id(), NodeId::new(1));
    assert_eq!(refresh.lease().lease_deadline_ms(), 2_100);
    assert_eq!(
        refresh.runtime_map().nodes()[0].endpoint(),
        "/tmp/argmin-node-1.sock"
    );
    let metrics = verifier_for_assert.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 1);
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::StorageRuntimeMapRefresh),
        1
    );
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn authenticated_unix_control_plane_client_resigns_heartbeat_transport_retry() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let heartbeat_epoch = authority.snapshot().cluster_epoch();
    let verifier =
        storage_node_auth_verifier("auth-cluster", vec![storage_node_auth_node_credential(1)]);
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        let first_envelope = ControlPlaneAuthEnvelope::decode_frame(
            &request.payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        )
        .unwrap();
        assert_eq!(first_envelope.header().issued_at_ms(), Some(2_000));
        drop(stream);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        let retry_envelope = ControlPlaneAuthEnvelope::decode_frame(
            &request.payload,
            CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN,
        )
        .unwrap();
        assert_eq!(
            retry_envelope.header().issued_at_ms(),
            Some(1_900),
            "transport retry must be re-signed after a wall-clock correction"
        );
        let response = build_control_plane_unix_response_with_auth(
            &mut authority,
            request,
            1_900,
            Some(&verifier),
        )
        .unwrap();
        write_control_plane_unix_response(&mut stream, response).unwrap();
    });

    let refresh = crate::clock::with_time_override(1_900, || {
        let mut client = AuthenticatedUnixControlPlaneClient::new(
            UnixControlPlaneClient::new(&socket_path),
            storage_node_auth_credential("auth-cluster", 1, 42),
        );
        client.refresh_node_heartbeat(
            NodeHeartbeat {
                node_id: NodeId::new(1),
                node_incarnation: 42,
                endpoint: "/tmp/argmin-node-1.sock".to_owned(),
                observed_epoch: heartbeat_epoch,
                requested_lease_duration_ms: 1_000,
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                cluster_map_history_route_references: Default::default(),
                pg_observations: Vec::new(),
            },
            2_000,
        )
    })
    .unwrap();

    server.join().unwrap();
    assert_eq!(refresh.lease().lease_deadline_ms(), 2_900);
}

#[test]
fn authenticated_unix_control_plane_client_does_not_recreate_heartbeat_deadline() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let server = std::thread::spawn(move || {
        let accept_deadline = Instant::now() + Duration::from_millis(100);
        loop {
            match listener.accept() {
                Ok((_stream, _addr)) => return true,
                Err(error)
                    if error.kind() == ErrorKind::WouldBlock
                        && Instant::now() < accept_deadline =>
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => return false,
                Err(error) => panic!("accept authenticated heartbeat: {error}"),
            }
        }
    });

    let mut client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        storage_node_auth_credential("auth-cluster", 1, 42),
    );
    let error = client
        .refresh_node_heartbeat_with_clock_and_before_dispatch(
            NodeHeartbeat {
                node_id: NodeId::new(1),
                node_incarnation: 42,
                endpoint: "/tmp/argmin-node-1.sock".to_owned(),
                observed_epoch: ClusterEpoch::new(1).unwrap(),
                requested_lease_duration_ms: 10,
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                cluster_map_history_route_references: Default::default(),
                pg_observations: Vec::new(),
            },
            || Ok(2_000),
            || std::thread::sleep(Duration::from_millis(30)),
        )
        .expect_err("expired authenticated heartbeat must not start transport");

    assert!(!server.join().unwrap());
    assert!(
        matches!(error, ControlPlaneError::RpcUnconfirmed { ref message }
            if message.contains("expired before the first request")),
        "unexpected error: {error}"
    );
}

#[test]
fn authenticated_storage_node_heartbeat_accepts_overlapping_credentials() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let heartbeat_epoch = authority.snapshot().cluster_epoch();
    let old_credential = storage_node_auth_node_credential_with(1, "storage-node", 1, "old-secret");
    let new_credential = storage_node_auth_node_credential_with(1, "storage-node", 2, "new-secret");
    let old_signer = old_credential
        .scoped_for_cluster_and_incarnation("auth-cluster", 42)
        .expect("old storage-node credential should scope");
    let verifier = storage_node_auth_verifier("auth-cluster", vec![old_credential, new_credential]);
    let verifier_for_assert = verifier.clone();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream_with_auth(&mut authority, &mut stream, 2_000, &verifier)
            .unwrap();
    });

    let mut client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        old_signer,
    );
    let refresh = crate::clock::with_time_override(2_000, || {
        client.refresh_node_heartbeat(
            NodeHeartbeat {
                node_id: NodeId::new(1),
                node_incarnation: 42,
                endpoint: "/tmp/argmin-node-1.sock".to_owned(),
                observed_epoch: heartbeat_epoch,
                requested_lease_duration_ms: 100,
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                cluster_map_history_route_references: Default::default(),
                pg_observations: Vec::new(),
            },
            2_000,
        )
    })
    .unwrap();

    server.join().unwrap();
    assert_eq!(refresh.lease().node_id(), NodeId::new(1));
    assert_eq!(refresh.lease().lease_deadline_ms(), 2_100);
    let status = verifier_for_assert.status_snapshot();
    assert_eq!(status.storage_node_credentials().len(), 2);
    let metrics = verifier_for_assert.metrics_snapshot();
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::StorageRuntimeMapRefresh),
        1
    );
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn authenticated_unix_control_plane_client_verifies_heartbeat_response_at_receive_time() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let heartbeat_epoch = authority.snapshot().cluster_epoch();
    let verifier =
        storage_node_auth_verifier("auth-cluster", vec![storage_node_auth_node_credential(1)]);
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream_with_auth(&mut authority, &mut stream, 2_001, &verifier)
            .unwrap();
    });

    let mut client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        storage_node_auth_credential("auth-cluster", 1, 42),
    );
    let mut timestamps = [2_000, 2_000].into_iter();
    let refresh = client
        .refresh_node_heartbeat_with_clock(
            NodeHeartbeat {
                node_id: NodeId::new(1),
                node_incarnation: 42,
                endpoint: "/tmp/argmin-node-1.sock".to_owned(),
                observed_epoch: heartbeat_epoch,
                requested_lease_duration_ms: 100,
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                cluster_map_history_route_references: Default::default(),
                pg_observations: Vec::new(),
            },
            || {
                timestamps.next().ok_or_else(|| {
                    ControlPlaneError::rpc_protocol("test heartbeat clock exhausted".to_owned())
                })
            },
        )
        .unwrap();

    server.join().unwrap();
    assert_eq!(refresh.lease().node_id(), NodeId::new(1));
    assert_eq!(refresh.lease().lease_deadline_ms(), 2_101);
}

#[test]
fn authenticated_unix_control_plane_client_rejects_unsigned_heartbeat_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let (kind, payload) = read_control_plane_rpc_frame(&mut stream).unwrap();
        assert_eq!(kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
        ControlPlaneAuthEnvelope::decode_frame(&payload, CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN)
            .expect("heartbeat request should be auth-wrapped");
        let response = encode_control_plane_rpc_response(Ok(Vec::new())).unwrap();
        write_control_plane_rpc_frame(&mut stream, kind, &response).unwrap();
    });

    let mut client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        storage_node_auth_credential("auth-cluster", 1, 42),
    );
    let error = client
        .refresh_node_heartbeat(
            NodeHeartbeat {
                node_id: NodeId::new(1),
                node_incarnation: 42,
                endpoint: "/tmp/argmin-node-1.sock".to_owned(),
                observed_epoch: ClusterEpoch::new(1).unwrap(),
                requested_lease_duration_ms: 100,
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                cluster_map_history_route_references: Default::default(),
                pg_observations: Vec::new(),
            },
            1_999,
        )
        .expect_err("unsigned heartbeat response should be rejected");

    server.join().unwrap();
    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
        if message.contains("control-plane auth envelope")),
        "unexpected error: {error}"
    );
}

#[test]
fn authenticated_control_plane_rejects_missing_storage_node_heartbeat_auth() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let verifier =
        storage_node_auth_verifier("auth-cluster", vec![storage_node_auth_node_credential(1)]);
    let heartbeat = NodeHeartbeat {
        node_id: NodeId::new(1),
        node_incarnation: 42,
        endpoint: "/tmp/argmin-node-1.sock".to_owned(),
        observed_epoch: authority.snapshot().cluster_epoch(),
        requested_lease_duration_ms: 100,
        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
        cluster_map_history_route_references: Default::default(),
        pg_observations: Vec::new(),
    };
    let request = ControlPlaneRpcRequest {
        kind: ControlPlaneRpcKind::RefreshNodeHeartbeat,
        payload: write_node_heartbeat_payload(&heartbeat).unwrap(),
    };

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message } if message.contains("auth magic")),
        "unexpected error: {error}"
    );
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::StorageRuntimeMapRefresh),
        1
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::Missing),
        1
    );
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        None
    );
}

#[test]
fn frontend_auth_verifier_does_not_require_storage_node_heartbeat_auth() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let verifier = ControlPlaneUnixAuthVerifier::new_empty("auth-cluster")
        .unwrap()
        .with_frontend_credentials(vec![frontend_auth_config_credential("frontend-1")])
        .unwrap();
    assert!(verifier.requires_frontend_runtime_map_auth());
    assert!(!verifier.requires_storage_node_heartbeat_auth());
    let heartbeat = NodeHeartbeat {
        node_id: NodeId::new(1),
        node_incarnation: 42,
        endpoint: "/tmp/argmin-node-1.sock".to_owned(),
        observed_epoch: authority.snapshot().cluster_epoch(),
        requested_lease_duration_ms: 100,
        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
        cluster_map_history_route_references: Default::default(),
        pg_observations: Vec::new(),
    };
    let request = ControlPlaneRpcRequest {
        kind: ControlPlaneRpcKind::RefreshNodeHeartbeat,
        payload: write_node_heartbeat_payload(&heartbeat).unwrap(),
    };

    let response = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap();

    assert_eq!(response.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
    let response_payload = decode_control_plane_rpc_response(response.payload).unwrap();
    let mut reader = PayloadReader::new(&response_payload);
    let lease = read_heartbeat_lease_summary(&mut reader).unwrap();
    let runtime_map = read_runtime_map_snapshot(&mut reader).unwrap();
    reader.finish().unwrap();
    assert_eq!(lease.node_id(), NodeId::new(1));
    assert_eq!(lease.lease_deadline_ms(), 2_100);
    assert_eq!(
        runtime_map.cluster_epoch(),
        authority.snapshot().cluster_epoch()
    );
    let node = authority.snapshot().node(NodeId::new(1)).unwrap();
    assert_eq!(node.node_incarnation(), 42);
    assert_eq!(node.endpoint(), "/tmp/argmin-node-1.sock");
    assert_eq!(node.lease_deadline_ms(), Some(2_100));
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn authenticated_control_plane_counts_malformed_storage_node_heartbeat_auth() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let verifier =
        storage_node_auth_verifier("auth-cluster", vec![storage_node_auth_node_credential(1)]);
    let request = ControlPlaneRpcRequest {
        kind: ControlPlaneRpcKind::RefreshNodeHeartbeat,
        payload: b"ARGCPAUT".to_vec(),
    };

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { .. }),
        "unexpected error: {error}"
    );
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::StorageRuntimeMapRefresh),
        1
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::Malformed),
        1
    );
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        None
    );
}

#[test]
fn authenticated_control_plane_rejects_storage_node_heartbeat_wrong_embedded_kind() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let signer = storage_node_auth_credential("auth-cluster", 1, 42);
    let verifier =
        storage_node_auth_verifier("auth-cluster", vec![storage_node_auth_node_credential(1)]);
    let heartbeat = NodeHeartbeat {
        node_id: NodeId::new(1),
        node_incarnation: 42,
        endpoint: "/tmp/argmin-node-1.sock".to_owned(),
        observed_epoch: authority.snapshot().cluster_epoch(),
        requested_lease_duration_ms: 100,
        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
        cluster_map_history_route_references: Default::default(),
        pg_observations: Vec::new(),
    };
    let request = signed_storage_node_heartbeat_request_with_embedded_kind(
        &signer,
        &heartbeat,
        ControlPlaneRpcKind::RuntimeMapStatus,
        Some(1_999),
        Some(2_099),
    );

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message } if message.contains("authenticated RPC kind")),
        "unexpected error: {error}"
    );
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::StorageRuntimeMapRefresh),
        1
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::WrongRole),
        1
    );
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        None
    );
}

#[test]
fn authenticated_control_plane_rejects_storage_node_incarnation_mismatch() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let signer = storage_node_auth_credential("auth-cluster", 1, 41);
    let verifier =
        storage_node_auth_verifier("auth-cluster", vec![storage_node_auth_node_credential(1)]);
    let heartbeat = NodeHeartbeat {
        node_id: NodeId::new(1),
        node_incarnation: 42,
        endpoint: "/tmp/argmin-node-1.sock".to_owned(),
        observed_epoch: authority.snapshot().cluster_epoch(),
        requested_lease_duration_ms: 100,
        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
        cluster_map_history_route_references: Default::default(),
        pg_observations: Vec::new(),
    };
    let request =
        signed_storage_node_heartbeat_request(&signer, &heartbeat, Some(1_999), Some(2_099));

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message } if message.contains("WrongSource")),
        "unexpected error: {error}"
    );
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::StorageRuntimeMapRefresh),
        1
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::WrongSource),
        1
    );
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        None
    );
}

#[test]
fn authenticated_control_plane_rejects_storage_node_heartbeat_without_freshness() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let signer = storage_node_auth_credential("auth-cluster", 1, 42);
    let verifier =
        storage_node_auth_verifier("auth-cluster", vec![storage_node_auth_node_credential(1)]);
    let heartbeat = NodeHeartbeat {
        node_id: NodeId::new(1),
        node_incarnation: 42,
        endpoint: "/tmp/argmin-node-1.sock".to_owned(),
        observed_epoch: authority.snapshot().cluster_epoch(),
        requested_lease_duration_ms: 100,
        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
        cluster_map_history_route_references: Default::default(),
        pg_observations: Vec::new(),
    };
    let request = signed_storage_node_heartbeat_request(&signer, &heartbeat, None, None);

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
            if message.contains("ReplayFreshnessFailure")
                && message.contains("issued_at_ms=None")
                && message.contains("expires_at_ms=None")
                && message.contains("authority_now_ms=2000")),
        "unexpected error: {error}"
    );
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::StorageRuntimeMapRefresh),
        1
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::ReplayFreshnessFailure),
        1
    );
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        None
    );
}

#[test]
fn authenticated_control_plane_accepts_storage_node_heartbeat_at_future_skew_boundary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let signer = storage_node_auth_credential("auth-cluster", 1, 42);
    let verifier =
        storage_node_auth_verifier("auth-cluster", vec![storage_node_auth_node_credential(1)]);
    let authority_now_ms = 2_000;
    let signer_now_ms = authority_now_ms + CONTROL_PLANE_RPC_AUTH_FUTURE_SKEW_MS;
    let heartbeat = NodeHeartbeat {
        node_id: NodeId::new(1),
        node_incarnation: 42,
        endpoint: "/tmp/argmin-node-1.sock".to_owned(),
        observed_epoch: authority.snapshot().cluster_epoch(),
        requested_lease_duration_ms: 100,
        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
        cluster_map_history_route_references: Default::default(),
        pg_observations: Vec::new(),
    };
    let request = signed_storage_node_heartbeat_request(
        &signer,
        &heartbeat,
        Some(signer_now_ms),
        Some(signer_now_ms + heartbeat.requested_lease_duration_ms),
    );

    let response = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        authority_now_ms,
        Some(&verifier),
    )
    .unwrap();

    assert_eq!(response.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        Some(authority_now_ms + heartbeat.requested_lease_duration_ms)
    );
    let metrics = verifier.metrics_snapshot();
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::StorageRuntimeMapRefresh),
        1
    );
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn authenticated_control_plane_rejects_storage_node_heartbeat_beyond_future_skew_budget() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let signer = storage_node_auth_credential("auth-cluster", 1, 42);
    let verifier =
        storage_node_auth_verifier("auth-cluster", vec![storage_node_auth_node_credential(1)]);
    let authority_now_ms = 2_000;
    let signer_now_ms = authority_now_ms + CONTROL_PLANE_RPC_AUTH_FUTURE_SKEW_MS + 1;
    let heartbeat = NodeHeartbeat {
        node_id: NodeId::new(1),
        node_incarnation: 42,
        endpoint: "/tmp/argmin-node-1.sock".to_owned(),
        observed_epoch: authority.snapshot().cluster_epoch(),
        requested_lease_duration_ms: 100,
        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
        cluster_map_history_route_references: Default::default(),
        pg_observations: Vec::new(),
    };
    let request = signed_storage_node_heartbeat_request(
        &signer,
        &heartbeat,
        Some(signer_now_ms),
        Some(signer_now_ms + heartbeat.requested_lease_duration_ms),
    );

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        authority_now_ms,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
            if message.contains("ReplayFreshnessFailure")
                && message.contains("issued_delta_ms=Some(1001)")),
        "unexpected error: {error}"
    );
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        None
    );
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::ReplayFreshnessFailure),
        1
    );
}

#[test]
fn authenticated_control_plane_accepts_admin_request_at_future_skew_boundary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let signer = admin_auth_credential("auth-cluster", "admin-1");
    let verifier = admin_auth_verifier("auth-cluster", "admin-1");
    let authority_now_ms = 2_000;
    let signer_now_ms = authority_now_ms + CONTROL_PLANE_RPC_AUTH_FUTURE_SKEW_MS;
    let mut payload = Vec::new();
    write_pg_acting_set_request(&mut payload, PgId::new(7), &[NodeId::new(1)]).unwrap();
    let request = signed_admin_control_plane_request(
        ControlPlaneRpcKind::SetPgActingSet,
        &signer,
        payload,
        Some(signer_now_ms),
        Some(signer_now_ms + CONTROL_PLANE_RPC_READ_AUTH_REPLAY_WINDOW_MS),
    );

    let response = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        authority_now_ms,
        Some(&verifier),
    )
    .unwrap();

    assert_eq!(response.kind, ControlPlaneRpcKind::SetPgActingSet);
    assert_eq!(
        authority.snapshot().pg(PgId::new(7)).unwrap().acting_set(),
        &[NodeId::new(1)]
    );
    let metrics = verifier.metrics_snapshot();
    assert_eq!(
        metrics.accepted_for_operation(ControlPlaneAuthOperation::AdminControlPlaneCommand),
        1
    );
    assert_eq!(metrics.rejected_total(), 0);
}

#[test]
fn authenticated_control_plane_rejects_storage_node_heartbeat_long_replay_window() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let signer = storage_node_auth_credential("auth-cluster", 1, 42);
    let verifier =
        storage_node_auth_verifier("auth-cluster", vec![storage_node_auth_node_credential(1)]);
    let heartbeat = NodeHeartbeat {
        node_id: NodeId::new(1),
        node_incarnation: 42,
        endpoint: "/tmp/argmin-node-1.sock".to_owned(),
        observed_epoch: authority.snapshot().cluster_epoch(),
        requested_lease_duration_ms: 100,
        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
        cluster_map_history_route_references: Default::default(),
        pg_observations: Vec::new(),
    };
    let request =
        signed_storage_node_heartbeat_request(&signer, &heartbeat, Some(1_999), Some(12_000));

    let error = build_control_plane_unix_response_with_auth(
        &mut authority,
        request,
        2_000,
        Some(&verifier),
    )
    .unwrap_err();

    assert!(
        matches!(error, ControlPlaneError::RpcProtocol { diagnostic: ref message }
            if message.contains("ReplayFreshnessFailure")
                && message.contains("issued_at_ms=Some(1999)")
                && message.contains("expires_at_ms=Some(12000)")
                && message.contains("issued_delta_ms=Some(-1)")),
        "unexpected error: {error}"
    );
    let metrics = verifier.metrics_snapshot();
    assert_eq!(metrics.accepted_total(), 0);
    assert_eq!(metrics.rejected_total(), 1);
    assert_eq!(
        metrics.rejected_for_operation(ControlPlaneAuthOperation::StorageRuntimeMapRefresh),
        1
    );
    assert_eq!(
        metrics.rejected_for_reason(ControlPlaneAuthRejectionReason::ReplayFreshnessFailure),
        1
    );
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        None
    );
}

#[test]
fn unix_control_plane_client_retries_heartbeat_after_lost_response() {
    const TEST_HEARTBEAT_LEASE_MS: u64 = 3_000;
    const TEST_RETRY_SERVER_DELAY: Duration = Duration::from_millis(200);

    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let heartbeat_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
        let response = build_control_plane_unix_response(&mut authority, request, 2_000)
            .expect("heartbeat refresh should apply before response loss");
        assert_eq!(response.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
        assert_eq!(
            authority
                .snapshot()
                .node(NodeId::new(1))
                .unwrap()
                .lease_deadline_ms(),
            Some(5_000)
        );
        drop(response);
        drop(stream);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
        std::thread::sleep(TEST_RETRY_SERVER_DELAY);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_050).unwrap();
        assert_eq!(
            authority
                .snapshot()
                .node(NodeId::new(1))
                .unwrap()
                .lease_deadline_ms(),
            Some(5_050)
        );
    });

    let mut client = UnixControlPlaneClient::new(&socket_path);
    let refresh = client
        .refresh_node_heartbeat(
            NodeHeartbeat {
                node_id: NodeId::new(1),
                node_incarnation: 42,
                endpoint: "/tmp/argmin-node-1.sock".to_owned(),
                observed_epoch: heartbeat_epoch,
                requested_lease_duration_ms: TEST_HEARTBEAT_LEASE_MS,
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                cluster_map_history_route_references: Default::default(),
                pg_observations: Vec::new(),
            },
            0,
        )
        .unwrap();

    server.join().unwrap();
    assert_eq!(refresh.lease().lease_deadline_ms(), 5_050);
    assert_eq!(
        refresh.runtime_map().nodes()[0].endpoint(),
        "/tmp/argmin-node-1.sock"
    );
}

#[test]
fn unix_control_plane_client_rejects_zero_heartbeat_lease_without_panicking() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let mut client = UnixControlPlaneClient::new(&socket_path);
    let mut heartbeat = heartbeat(1, ClusterEpoch::new(1).unwrap(), 0);
    heartbeat.requested_lease_duration_ms = 0;

    let error = client.refresh_node_heartbeat(heartbeat, 0).unwrap_err();

    assert!(matches!(error, ControlPlaneError::InvalidLeaseDuration));
}

#[test]
fn unix_control_plane_client_reports_exhausted_liveness_budget_without_panicking() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let client = UnixControlPlaneClient::new(&socket_path);

    let error = client
        .send_liveness_request(
            ControlPlaneRpcKind::RefreshNodeHeartbeat,
            &[],
            Duration::ZERO,
        )
        .unwrap_err();

    assert!(matches!(error, ControlPlaneError::RpcUnconfirmed { .. }));
}

#[test]
fn unix_control_plane_client_waits_for_slow_heartbeat_response_within_lease() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let heartbeat_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
        std::thread::sleep(CONTROL_PLANE_RPC_IO_TIMEOUT + Duration::from_millis(200));
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_000).unwrap();

        listener.set_nonblocking(true).unwrap();
        let retry_probe_deadline = Instant::now() + Duration::from_millis(100);
        loop {
            match listener.accept() {
                Ok((_stream, _addr)) => panic!("heartbeat client retried before slow response"),
                Err(error)
                    if error.kind() == ErrorKind::WouldBlock
                        && Instant::now() < retry_probe_deadline =>
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => break,
                Err(error) => panic!("accept heartbeat retry probe: {error}"),
            }
        }
    });

    let mut client = UnixControlPlaneClient::new(&socket_path);
    let refresh = client
        .refresh_node_heartbeat(
            NodeHeartbeat {
                node_id: NodeId::new(1),
                node_incarnation: 42,
                endpoint: "/tmp/argmin-node-1.sock".to_owned(),
                observed_epoch: heartbeat_epoch,
                requested_lease_duration_ms: 3_000,
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                cluster_map_history_route_references: Default::default(),
                pg_observations: Vec::new(),
            },
            0,
        )
        .unwrap();

    server.join().unwrap();
    assert_eq!(refresh.lease().lease_deadline_ms(), 5_000);
    assert_eq!(
        refresh.runtime_map().nodes()[0].endpoint(),
        "/tmp/argmin-node-1.sock"
    );
}

#[test]
fn unix_control_plane_heartbeat_retry_observes_completed_peering_after_lost_response() {
    const TEST_HEARTBEAT_LEASE_MS: u64 = 3_000;
    const TEST_RETRY_SERVER_DELAY: Duration = Duration::from_millis(200);

    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(22), vec![NodeId::new(1)])
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let expected_active_epoch = ClusterEpoch::new(peering_epoch.get() + 1).unwrap();
    let mut peering_heartbeat = heartbeat_from_record(&authority, 1, peering_epoch, 2_000);
    peering_heartbeat.requested_lease_duration_ms = TEST_HEARTBEAT_LEASE_MS;
    peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(22),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
        let response = build_control_plane_unix_response(&mut authority, request, 2_000)
            .expect("heartbeat refresh should complete peering before response loss");
        assert_eq!(response.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
        assert_eq!(authority.snapshot().cluster_epoch(), expected_active_epoch);
        let pg = authority.snapshot().pg(PgId::new(22)).unwrap();
        assert_eq!(pg.state(), PgState::Active);
        assert_eq!(pg.active_primary(), Some(NodeId::new(1)));
        drop(response);
        drop(stream);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
        std::thread::sleep(TEST_RETRY_SERVER_DELAY);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_050).unwrap();
        assert_eq!(
            authority.snapshot().cluster_epoch(),
            expected_active_epoch,
            "retrying the stale heartbeat must not complete peering a second time"
        );
    });

    let mut client = UnixControlPlaneClient::new(&socket_path);
    let refresh = client.refresh_node_heartbeat(peering_heartbeat, 0).unwrap();

    server.join().unwrap();
    assert_eq!(refresh.lease().lease_deadline_ms(), 5_050);
    assert_eq!(refresh.runtime_map().cluster_epoch(), expected_active_epoch);
    let route = refresh
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(22))
        .unwrap();
    assert_eq!(route.state(), PgState::Active);
    assert_eq!(route.primary_node_id(), NodeId::new(1));
}

#[test]
fn unix_control_plane_client_stops_heartbeat_retry_before_lease_window_expires() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let heartbeat_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
        let response = build_control_plane_unix_response(&mut authority, request, 2_000)
            .expect("heartbeat refresh should apply before response loss");
        assert_eq!(response.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
        assert_eq!(
            authority
                .snapshot()
                .node(NodeId::new(1))
                .unwrap()
                .lease_deadline_ms(),
            Some(2_001)
        );
        drop(response);
        drop(stream);
    });

    let mut client = UnixControlPlaneClient::new(&socket_path);
    let error = client
        .refresh_node_heartbeat(
            NodeHeartbeat {
                node_id: NodeId::new(1),
                node_incarnation: 42,
                endpoint: "/tmp/argmin-node-1.sock".to_owned(),
                observed_epoch: heartbeat_epoch,
                requested_lease_duration_ms: 1,
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                cluster_map_history_route_references: Default::default(),
                pg_observations: Vec::new(),
            },
            0,
        )
        .unwrap_err();

    server.join().unwrap();
    assert!(
        error.is_retryable_control_plane_rpc_transport_error(),
        "heartbeat retry should return its terminal retryable transport error: {error}"
    );
}

#[test]
fn unix_control_plane_client_does_not_send_heartbeat_retry_at_lease_deadline() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let server_socket_path = socket_path.clone();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let heartbeat_epoch = authority.snapshot().cluster_epoch();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
        drop(stream);
        drop(listener);
        std::fs::remove_file(&server_socket_path).unwrap();

        std::thread::sleep(Duration::from_millis(75));
        let listener = std::os::unix::net::UnixListener::bind(server_socket_path).unwrap();
        listener.set_nonblocking(true).unwrap();
        let accept_deadline = Instant::now() + Duration::from_millis(75);
        loop {
            match listener.accept() {
                Ok((_stream, _addr)) => return true,
                Err(error)
                    if error.kind() == ErrorKind::WouldBlock
                        && Instant::now() < accept_deadline =>
                {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(error) if error.kind() == ErrorKind::WouldBlock => return false,
                Err(error) => panic!("accept heartbeat retry: {error}"),
            }
        }
    });

    let mut client = UnixControlPlaneClient::new(&socket_path);
    let refresh = client.refresh_node_heartbeat(
        NodeHeartbeat {
            node_id: NodeId::new(1),
            node_incarnation: 42,
            endpoint: "/tmp/argmin-node-1.sock".to_owned(),
            observed_epoch: heartbeat_epoch,
            requested_lease_duration_ms: 50,
            cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
            cluster_map_history_route_references: Default::default(),
            pg_observations: Vec::new(),
        },
        0,
    );
    let accepted_retry_after_deadline = server.join().unwrap();

    assert!(!accepted_retry_after_deadline);
    let error = refresh.unwrap_err();
    assert!(
        error.is_retryable_control_plane_rpc_transport_error(),
        "heartbeat retry should return its terminal retryable transport error: {error}"
    );
}

#[test]
fn plain_pg_admin_facade_sets_pg_acting_set_live() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    let previous_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream(&mut authority, &mut stream, 2_000).unwrap();
        }
    });

    let client =
        crate::ControlPlanePgAdminClient::new(UnixControlPlaneClient::new(&socket_path), None);
    let cluster_epoch = client.set_acting_set(7, vec![1, 2]).unwrap();

    server.join().unwrap();
    assert!(cluster_epoch > previous_epoch.get());
    let authority =
        SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&state_path)).unwrap();
    assert_eq!(
        authority.snapshot().pg(PgId::new(7)).unwrap().acting_set(),
        &[NodeId::new(1), NodeId::new(2)]
    );
}

#[test]
fn unix_control_plane_client_observes_pg_acting_set_after_all_typed_serving_gaps() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let previous_epoch = authority.snapshot().cluster_epoch();
    let expected_epoch = ClusterEpoch::new(previous_epoch.get() + 1).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 1_999).unwrap();
        assert_eq!(authority.snapshot().cluster_epoch(), previous_epoch);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::SetPgActingSet);
        let response = build_control_plane_unix_response(&mut authority, request, 2_000)
            .expect("acting-set update should apply before response loss");
        assert_eq!(response.kind, ControlPlaneRpcKind::SetPgActingSet);
        assert_eq!(authority.snapshot().cluster_epoch(), expected_epoch);
        drop(response);
        drop(stream);

        for error in runtime_map_serving_gap_test_errors() {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
            let response = ControlPlaneRpcResponse {
                kind: request.kind,
                payload: encode_control_plane_rpc_response(Err(error)).unwrap(),
            };
            write_control_plane_unix_response(&mut stream, response).unwrap();
        }

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_001).unwrap();
        assert_eq!(authority.snapshot().cluster_epoch(), expected_epoch);
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let cluster_epoch = client
        .set_pg_acting_set_checked(PgId::new(7), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    server.join().unwrap();
    assert_eq!(cluster_epoch, expected_epoch);
}

#[test]
fn unix_control_plane_client_retries_pg_acting_set_when_lost_request_did_not_apply() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let previous_epoch = authority.snapshot().cluster_epoch();
    let expected_epoch = ClusterEpoch::new(previous_epoch.get() + 1).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 1_999).unwrap();
        assert_eq!(authority.snapshot().cluster_epoch(), previous_epoch);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::SetPgActingSet);
        drop(request);
        drop(stream);
        assert_eq!(
            authority.snapshot().cluster_epoch(),
            previous_epoch,
            "first request is lost before apply"
        );

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_001).unwrap();
        assert_eq!(
            authority.snapshot().cluster_epoch(),
            previous_epoch,
            "observation should still see the pre-command route"
        );

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::SetPgActingSet);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_002).unwrap();
        assert_eq!(authority.snapshot().cluster_epoch(), expected_epoch);
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let cluster_epoch = client
        .set_pg_acting_set_checked(PgId::new(7), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    server.join().unwrap();
    assert_eq!(cluster_epoch, expected_epoch);
}

#[test]
fn unix_control_plane_client_retries_absent_pg_when_lost_request_did_not_apply() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(7);
    let previous_epoch = authority.snapshot().cluster_epoch();
    let expected_epoch = ClusterEpoch::new(previous_epoch.get() + 1).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 1_999).unwrap();
        assert_eq!(authority.snapshot().cluster_epoch(), previous_epoch);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::SetPgActingSet);
        drop(request);
        drop(stream);
        assert_eq!(
            authority.snapshot().cluster_epoch(),
            previous_epoch,
            "first request is lost before apply"
        );

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_001).unwrap();
        assert_eq!(
            authority.snapshot().cluster_epoch(),
            previous_epoch,
            "typed UnknownPg confirms the lost mutation did not apply"
        );

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::SetPgActingSet);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_002).unwrap();
        assert_eq!(authority.snapshot().cluster_epoch(), expected_epoch);
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let cluster_epoch = client
        .set_pg_acting_set_checked(pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    server.join().unwrap();
    assert_eq!(cluster_epoch, expected_epoch);
}

#[test]
fn unix_control_plane_client_rejects_successful_confirmation_omitting_target_pg() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let pg_id = PgId::new(7);
    let previous_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 1_999).unwrap();

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::SetPgActingSet);
        drop(request);
        drop(stream);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        let mut malformed_payload = Vec::new();
        write_runtime_map_snapshot(
            &mut malformed_payload,
            &authority.runtime_map_snapshot(2_001).unwrap(),
        )
        .unwrap();
        let response = ControlPlaneRpcResponse {
            kind: request.kind,
            payload: encode_control_plane_rpc_response(Ok(malformed_payload)).unwrap(),
        };
        write_control_plane_unix_response(&mut stream, response).unwrap();
        authority.snapshot().clone()
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let error = client
        .set_pg_acting_set_checked(pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap_err();

    let snapshot = server.join().unwrap();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.as_str() == "PG-specific runtime map response omitted requested PG 7"
    ));
    assert_eq!(snapshot.cluster_epoch(), previous_epoch);
    assert!(snapshot.pg(pg_id).is_none());
}

#[derive(Clone, Copy, Debug)]
enum AppliedPlainMutationResponseFault {
    InvalidMagic,
    UnsupportedVersion,
    BadChecksum,
    WrongOuterKind,
    InvalidSuccessPayload,
}

#[test]
fn plain_checked_mutation_confirms_every_invalid_post_publication_response() {
    for fault in [
        AppliedPlainMutationResponseFault::InvalidMagic,
        AppliedPlainMutationResponseFault::UnsupportedVersion,
        AppliedPlainMutationResponseFault::BadChecksum,
        AppliedPlainMutationResponseFault::WrongOuterKind,
        AppliedPlainMutationResponseFault::InvalidSuccessPayload,
    ] {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
        }
        authority
            .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
            .unwrap();
        let previous_epoch = authority.snapshot().cluster_epoch();
        let expected_epoch = ClusterEpoch::new(previous_epoch.get() + 1).unwrap();
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
            respond_control_plane_unix_request(&mut authority, &mut stream, request, 1_999)
                .unwrap();

            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            assert_eq!(request.kind, ControlPlaneRpcKind::SetPgActingSet);
            let response = build_control_plane_unix_response(&mut authority, request, 2_000)
                .expect("acting-set update should apply before the invalid response");
            assert_eq!(authority.snapshot().cluster_epoch(), expected_epoch);
            let mut frame = match fault {
                AppliedPlainMutationResponseFault::UnsupportedVersion => {
                    encode_control_plane_rpc_frame_with_version(
                        response.kind,
                        &response.payload,
                        CONTROL_PLANE_RPC_VERSION - 1,
                    )
                    .unwrap()
                }
                AppliedPlainMutationResponseFault::WrongOuterKind => {
                    encode_control_plane_rpc_frame(
                        ControlPlaneRpcKind::TriggerRaftElection,
                        &response.payload,
                    )
                    .unwrap()
                }
                AppliedPlainMutationResponseFault::InvalidSuccessPayload => {
                    let payload = encode_control_plane_rpc_response(Ok(vec![0xff])).unwrap();
                    encode_control_plane_rpc_frame(response.kind, &payload).unwrap()
                }
                AppliedPlainMutationResponseFault::InvalidMagic
                | AppliedPlainMutationResponseFault::BadChecksum => {
                    encode_control_plane_rpc_frame(response.kind, &response.payload).unwrap()
                }
            };
            match fault {
                AppliedPlainMutationResponseFault::InvalidMagic => frame[0] ^= 1,
                AppliedPlainMutationResponseFault::BadChecksum => {
                    let checksum_offset = CONTROL_PLANE_RPC_MAGIC.len()
                        + std::mem::size_of::<u16>()
                        + std::mem::size_of::<u16>()
                        + std::mem::size_of::<u32>();
                    frame[checksum_offset] ^= 1;
                }
                AppliedPlainMutationResponseFault::UnsupportedVersion
                | AppliedPlainMutationResponseFault::WrongOuterKind
                | AppliedPlainMutationResponseFault::InvalidSuccessPayload => {}
            }
            stream.write_all(&frame).unwrap();

            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
            respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_001)
                .unwrap();
            authority.snapshot().clone()
        });

        let client = UnixControlPlaneClient::new(&socket_path);
        let cluster_epoch = client
            .set_pg_acting_set_checked(PgId::new(7), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap_or_else(|error| panic!("{fault:?}: {error}"));

        let snapshot = server.join().unwrap();
        assert_eq!(cluster_epoch, expected_epoch, "{fault:?}");
        assert_eq!(snapshot.cluster_epoch(), expected_epoch, "{fault:?}");
        assert_eq!(
            snapshot.pg(PgId::new(7)).unwrap().acting_set(),
            &[NodeId::new(1), NodeId::new(2)],
            "{fault:?}"
        );
    }
}

#[test]
fn unix_control_plane_client_uses_check_applied_timeout_for_pg_acting_set_observation() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let previous_epoch = authority.snapshot().cluster_epoch();
    let expected_epoch = ClusterEpoch::new(previous_epoch.get() + 1).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 1_999).unwrap();
        assert_eq!(authority.snapshot().cluster_epoch(), previous_epoch);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::SetPgActingSet);
        let response = build_control_plane_unix_response(&mut authority, request, 2_000)
            .expect("acting-set update should apply before response loss");
        assert_eq!(response.kind, ControlPlaneRpcKind::SetPgActingSet);
        assert_eq!(authority.snapshot().cluster_epoch(), expected_epoch);
        drop(response);
        drop(stream);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        std::thread::sleep(CONTROL_PLANE_RPC_IO_TIMEOUT + Duration::from_millis(250));
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_001).unwrap();
        assert_eq!(authority.snapshot().cluster_epoch(), expected_epoch);
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let cluster_epoch = client
        .set_pg_acting_set_checked(PgId::new(7), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    server.join().unwrap();
    assert_eq!(cluster_epoch, expected_epoch);
}

#[test]
fn unix_control_plane_client_confirms_pg_acting_set_with_unrelated_non_serving_pg() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(8), vec![NodeId::new(1)])
        .unwrap();
    let unrelated_proof = PgMetadataProof::current(3, 4, 5);
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        8,
        PgState::Peering,
        unrelated_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(8),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        8,
        PgState::Active,
        unrelated_proof,
        false,
        2_002,
    );
    let previous_epoch = authority.snapshot().cluster_epoch();
    let expected_epoch = ClusterEpoch::new(previous_epoch.get() + 1).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_003).unwrap();
        assert_eq!(authority.snapshot().cluster_epoch(), previous_epoch);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::SetPgActingSet);
        let response = build_control_plane_unix_response(&mut authority, request, 2_004)
            .expect("acting-set update should apply before response loss");
        assert_eq!(response.kind, ControlPlaneRpcKind::SetPgActingSet);
        assert_eq!(authority.snapshot().cluster_epoch(), expected_epoch);
        assert!(matches!(
            authority.snapshot().runtime_map(2_005),
            Err(ControlPlaneError::PgHasNoServingPrimary { pg_id: 8, .. })
        ));
        drop(response);
        drop(stream);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_005).unwrap();
        assert_eq!(authority.snapshot().cluster_epoch(), expected_epoch);
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let cluster_epoch = client
        .set_pg_acting_set_checked(PgId::new(7), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    server.join().unwrap();
    assert_eq!(cluster_epoch, expected_epoch);
}

#[test]
fn unix_control_plane_client_does_not_clobber_newer_acting_set_after_lost_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let original_epoch = authority.snapshot().cluster_epoch();
    let first_update_epoch = ClusterEpoch::new(original_epoch.get() + 1).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 1_999).unwrap();
        assert_eq!(authority.snapshot().cluster_epoch(), original_epoch);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::SetPgActingSet);
        let response = build_control_plane_unix_response(&mut authority, request, 2_000)
            .expect("acting-set update should apply before response loss");
        assert_eq!(response.kind, ControlPlaneRpcKind::SetPgActingSet);
        assert_eq!(authority.snapshot().cluster_epoch(), first_update_epoch);
        drop(response);
        drop(stream);

        authority
            .set_pg_acting_set(PgId::new(7), vec![NodeId::new(2)])
            .expect("newer acting-set update should apply");
        let newer_epoch = authority.snapshot().cluster_epoch();
        assert!(newer_epoch > first_update_epoch);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_001).unwrap();
        assert_eq!(authority.snapshot().cluster_epoch(), newer_epoch);
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let error = client
        .set_pg_acting_set_checked(PgId::new(7), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap_err();

    server.join().unwrap();
    assert!(
        matches!(error, ControlPlaneError::RpcUnconfirmed { message }
        if message.contains("current route")
            && message.contains("expected [NodeId(1), NodeId(2)]"))
    );
    let authority =
        SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&state_path)).unwrap();
    let pg = authority.snapshot().pg(PgId::new(7)).unwrap();
    assert!(authority.snapshot().cluster_epoch() > first_update_epoch);
    assert_eq!(pg.acting_set(), &[NodeId::new(2)]);
}

#[test]
fn unix_control_plane_client_fences_pg_for_metadata_transfer_live() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(44), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(44),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    let active_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream(&mut authority, &mut stream, 2_003).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let fenced = client
        .fence_pg_for_metadata_transfer_runtime_map_with_source_lease(PgId::new(44))
        .unwrap();
    let source_primary_lease_deadline_ms = fenced.source_primary_lease_deadline_ms();
    let runtime_map = fenced.runtime_map().clone();
    let peering_epoch = runtime_map.cluster_epoch();

    server.join().unwrap();
    assert!(peering_epoch > active_epoch);
    assert_eq!(source_primary_lease_deadline_ms, Some(2_102));
    assert_eq!(
        runtime_map.valid_until_ms(),
        Some(2_003 + MAX_HEARTBEAT_LEASE_MS)
    );
    let route = runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(44))
        .unwrap();
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(route.primary_lease_deadline_ms(), None);
    let mut authority =
        SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&state_path)).unwrap();
    let pg = authority.snapshot().pg(PgId::new(44)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(1)]);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(active_proof));
    assert_eq!(pg.peering_metadata_transfer(), None);
    assert!(pg.metadata_transfer_fenced());
    assert_eq!(
        pg.metadata_transfer_fence_source_lease_deadline_ms(),
        Some(2_102)
    );

    let reopened_epoch = authority.snapshot().cluster_epoch();
    let retry_fence = authority
        .fence_pg_for_metadata_transfer_with_source_lease(PgId::new(44))
        .unwrap();
    let (retry_snapshot, retry_source_lease_deadline_ms) = retry_fence.into_parts();
    let same_epoch = retry_snapshot.cluster_epoch();
    assert_eq!(same_epoch, reopened_epoch);
    assert_eq!(retry_source_lease_deadline_ms, Some(2_102));
}

#[test]
fn unix_control_plane_client_retries_convergent_fence_before_and_after_apply() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(44), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(44),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    let active_epoch = authority.snapshot().cluster_epoch();
    let expected_fence_epoch = ClusterEpoch::new(active_epoch.get() + 1).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        for request_number in 0..3 {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            assert_eq!(
                request.kind,
                ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap
            );
            if request_number == 0 {
                drop(request);
                drop(stream);
                assert_eq!(authority.snapshot().cluster_epoch(), active_epoch);
            } else if request_number == 1 {
                let response = build_control_plane_unix_response(&mut authority, request, 2_003)
                    .expect("metadata-transfer fence should apply before response loss");
                assert_eq!(
                    response.kind,
                    ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap
                );
                assert_eq!(authority.snapshot().cluster_epoch(), expected_fence_epoch);
                drop(response);
                drop(stream);
            } else {
                respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_004)
                    .unwrap();
            }
        }
        assert_eq!(authority.snapshot().cluster_epoch(), expected_fence_epoch);
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let fenced = client
        .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(PgId::new(44))
        .unwrap();

    server.join().unwrap();
    assert_eq!(fenced.runtime_map().cluster_epoch(), expected_fence_epoch);
    assert_eq!(fenced.source_primary_lease_deadline_ms(), Some(2_102));
    let route = fenced
        .runtime_map()
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(44))
        .unwrap();
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(route.acting_set(), &[NodeId::new(1)]);
    assert_eq!(route.primary_lease_deadline_ms(), None);
}

#[test]
fn unix_control_plane_client_uses_longer_timeout_for_fence_confirmation() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(44), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(44),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    let active_epoch = authority.snapshot().cluster_epoch();
    let expected_fence_epoch = ClusterEpoch::new(active_epoch.get() + 1).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        for request_number in 0..2 {
            let (mut stream, _addr) = listener.accept().unwrap();
            let request = read_control_plane_unix_request(&mut stream).unwrap();
            assert_eq!(
                request.kind,
                ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap
            );
            if request_number == 0 {
                let response = build_control_plane_unix_response(&mut authority, request, 2_003)
                    .expect("metadata-transfer fence should apply before response loss");
                assert_eq!(authority.snapshot().cluster_epoch(), expected_fence_epoch);
                drop(response);
                drop(stream);
            } else {
                std::thread::sleep(CONTROL_PLANE_RPC_IO_TIMEOUT + Duration::from_millis(200));
                respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_004)
                    .unwrap();
            }
        }
        assert_eq!(authority.snapshot().cluster_epoch(), expected_fence_epoch);
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let fenced = client
        .fence_pg_for_metadata_transfer_runtime_map_with_source_lease_checked(PgId::new(44))
        .unwrap();

    server.join().unwrap();
    assert_eq!(fenced.runtime_map().cluster_epoch(), expected_fence_epoch);
    assert_eq!(fenced.source_primary_lease_deadline_ms(), Some(2_102));
}

#[test]
fn unix_control_plane_client_sets_pg_acting_set_with_metadata_transfer_live() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(43), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Peering,
        active_proof,
        false,
        10_003,
    );
    authority
        .complete_pg_peering(
            PgId::new(43),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            10_004,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Active,
        active_proof,
        false,
        10_005,
    );
    let active_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new(active_epoch, PgMetadataProof::current(10, 12, 13));
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream(&mut authority, &mut stream, 10_006).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let expected_destination_epoch = next_epoch(active_epoch).unwrap();
    let cluster_epoch = client
        .set_pg_acting_set_with_metadata_transfer(
            PgId::new(43),
            vec![NodeId::new(2)],
            transfer,
            expected_destination_epoch,
        )
        .unwrap();

    server.join().unwrap();
    assert!(cluster_epoch > active_epoch);
    let authority =
        SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&state_path)).unwrap();
    let pg = authority.snapshot().pg(PgId::new(43)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(2)]);
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(transfer.metadata_proof())
    );
}

#[test]
fn unix_control_plane_client_observes_metadata_transfer_epoch_after_lost_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(43), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(43),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    let active_epoch = authority.snapshot().cluster_epoch();
    let expected_transfer_epoch = ClusterEpoch::new(active_epoch.get() + 1).unwrap();
    let imported_proof = PgMetadataProof::current(9, 12, 11);
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        active_proof,
        imported_proof,
    );
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(
            request.kind,
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransfer
        );
        let response = build_control_plane_unix_response(&mut authority, request, 2_003)
            .expect("metadata-transfer install should apply before response loss");
        assert_eq!(
            response.kind,
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransfer
        );
        assert_eq!(
            authority.snapshot().cluster_epoch(),
            expected_transfer_epoch
        );
        drop(response);
        drop(stream);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        let mut reader = PayloadReader::new(&request.payload);
        assert_eq!(read_pg_id_request(&mut reader).unwrap(), PgId::new(43));
        reader.finish().unwrap();
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_004).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let cluster_epoch = client
        .set_pg_acting_set_with_metadata_transfer_checked(
            PgId::new(43),
            vec![NodeId::new(2)],
            transfer,
            expected_transfer_epoch,
        )
        .unwrap();

    server.join().unwrap();
    assert_eq!(cluster_epoch, expected_transfer_epoch);
}

#[test]
fn unix_control_plane_client_sets_transfer_and_returns_exact_runtime_map() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(43), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(43),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    let active_epoch = authority.snapshot().cluster_epoch();
    let imported_proof = PgMetadataProof::current(9, 12, 11);
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        active_proof,
        imported_proof,
    );
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream(&mut authority, &mut stream, 2_003).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let expected_destination_epoch = next_epoch(active_epoch).unwrap();
    let runtime_map = client
        .set_pg_acting_set_with_metadata_transfer_runtime_map(
            PgId::new(43),
            vec![NodeId::new(2)],
            transfer,
            expected_destination_epoch,
        )
        .unwrap();

    server.join().unwrap();
    assert!(runtime_map.cluster_epoch() > active_epoch);
    assert_eq!(
        runtime_map.valid_until_ms(),
        Some(2_003 + MAX_HEARTBEAT_LEASE_MS)
    );
    assert_eq!(runtime_map.pg_routes().len(), 1);
    let route = runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(43))
        .unwrap();
    assert_eq!(route.cluster_epoch(), runtime_map.cluster_epoch());
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(route.acting_set(), &[NodeId::new(2)]);
    assert!(matches!(
        runtime_map.freshness_proof(),
        RuntimeMapFreshnessProof::Reconstructed {
            authority_incarnation: AuthorityIncarnation::INITIAL,
        }
    ));

    let authority =
        SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&state_path)).unwrap();
    let pg = authority.snapshot().pg(PgId::new(43)).unwrap();
    assert_eq!(pg.peering_metadata_proof_floor(), Some(imported_proof));
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
}

#[test]
fn unix_control_plane_client_observes_metadata_transfer_install_after_lost_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(43), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(43),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        43,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    let unrelated_proof = PgMetadataProof::current(17, 18, 19);
    authority
        .set_pg_acting_set(PgId::new(44), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Peering,
        unrelated_proof,
        false,
        2_010,
    );
    authority
        .complete_pg_peering(
            PgId::new(44),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_011,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Active,
        unrelated_proof,
        false,
        2_012,
    );
    let active_epoch = authority.snapshot().cluster_epoch();
    let expected_transfer_epoch = ClusterEpoch::new(active_epoch.get() + 1).unwrap();
    let imported_proof = PgMetadataProof::current(9, 12, 11);
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        active_proof,
        imported_proof,
    );
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(
            request.kind,
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap
        );
        let response = build_control_plane_unix_response(&mut authority, request, 2_003)
            .expect("metadata-transfer install should apply before response loss");
        assert_eq!(
            response.kind,
            ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap
        );
        drop(response);
        drop(stream);

        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        let mut reader = PayloadReader::new(&request.payload);
        assert_eq!(read_pg_id_request(&mut reader).unwrap(), PgId::new(43));
        reader.finish().unwrap();
        assert!(matches!(
            authority.runtime_map_snapshot(2_200),
            Err(ControlPlaneError::PgHasNoServingPrimary { pg_id: 44, .. })
        ));
        std::thread::sleep(CONTROL_PLANE_RPC_IO_TIMEOUT + Duration::from_millis(200));
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_200).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let runtime_map = client
        .set_pg_acting_set_with_metadata_transfer_runtime_map_checked(
            PgId::new(43),
            vec![NodeId::new(2)],
            transfer,
            expected_transfer_epoch,
        )
        .unwrap();

    server.join().unwrap();
    assert_eq!(runtime_map.cluster_epoch(), expected_transfer_epoch);
    assert_eq!(runtime_map.pg_routes().len(), 1);
    let route = runtime_map
        .pg_routes()
        .iter()
        .find(|route| route.pg_id() == PgId::new(43))
        .unwrap();
    assert_eq!(route.cluster_epoch(), expected_transfer_epoch);
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(route.acting_set(), &[NodeId::new(2)]);
    assert_eq!(route.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        route.peering_metadata_transfer_source_route_epoch(),
        Some(active_epoch)
    );
    assert_eq!(
        route.peering_metadata_transfer_source_node_id(),
        Some(NodeId::new(1))
    );
}

#[test]
fn unix_control_plane_client_receives_framed_authority_error() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        handle_control_plane_unix_stream(&mut authority, &mut stream, 2_000).unwrap();
    });

    let mut client = UnixControlPlaneClient::new(&socket_path);
    let error = client
        .refresh_node_heartbeat(
            NodeHeartbeat {
                node_id: NodeId::new(99),
                node_incarnation: 1,
                endpoint: "/tmp/argmin-node-99.sock".to_owned(),
                observed_epoch: ClusterEpoch::INITIAL,
                requested_lease_duration_ms: 100,
                cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
                cluster_map_history_route_references: Default::default(),
                pg_observations: Vec::new(),
            },
            0,
        )
        .unwrap_err();

    server.join().unwrap();
    assert!(matches!(
        error,
        ControlPlaneError::UnknownNode { node_id: 99 }
    ));
}

#[test]
fn control_plane_rpc_kinds_have_explicit_auth_operations() {
    let frontend_read_kinds = [
        ControlPlaneRpcKind::RuntimeMapSnapshot,
        ControlPlaneRpcKind::RuntimeMapDiagnostics,
        ControlPlaneRpcKind::PgRuntimeMapSnapshot,
        ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot,
        ControlPlaneRpcKind::RuntimeMapStatus,
        ControlPlaneRpcKind::PendingMetadataCommandRecoveries,
    ];
    for kind in frontend_read_kinds {
        assert_eq!(
            kind.auth_operation(),
            ControlPlaneAuthOperation::FrontendRuntimeMapRead,
            "{kind:?}"
        );
    }

    assert_eq!(
        ControlPlaneRpcKind::RefreshNodeHeartbeat.auth_operation(),
        ControlPlaneAuthOperation::StorageRuntimeMapRefresh
    );

    let admin_kinds = [
        ControlPlaneRpcKind::SetPgActingSet,
        ControlPlaneRpcKind::SetPgActingSetWithMetadataTransfer,
        ControlPlaneRpcKind::SetPgActingSetWithMetadataTransferRuntimeMap,
        ControlPlaneRpcKind::FencePgForMetadataTransferRuntimeMap,
        ControlPlaneRpcKind::TransferRaftLeadership,
        ControlPlaneRpcKind::TriggerRaftSnapshotAndPurge,
        ControlPlaneRpcKind::TriggerRaftElection,
        ControlPlaneRpcKind::AuthorityClockStatus,
        ControlPlaneRpcKind::ReestablishAuthorityClock,
    ];
    for kind in admin_kinds {
        assert_eq!(
            kind.auth_operation(),
            ControlPlaneAuthOperation::AdminControlPlaneCommand,
            "{kind:?}"
        );
    }

    let mut classified_kinds = frontend_read_kinds
        .into_iter()
        .chain([ControlPlaneRpcKind::RefreshNodeHeartbeat])
        .chain(admin_kinds)
        .collect::<Vec<_>>();
    classified_kinds.sort_by_key(|kind| kind.as_u16());
    let mut all_kinds = ControlPlaneRpcKind::ALL.to_vec();
    all_kinds.sort_by_key(|kind| kind.as_u16());
    assert_eq!(classified_kinds, all_kinds);
}

#[test]
fn control_plane_rpc_raft_admission_classifies_verified_requests() {
    let request = |kind| VerifiedControlPlaneRpcRequest {
        kind,
        payload: Vec::new(),
        response_auth: None,
    };

    let status = request(ControlPlaneRpcKind::AuthorityClockStatus);
    assert!(status.is_authority_clock_admin());
    assert!(!status.requires_raft_authority_confirmation());

    let reestablish = request(ControlPlaneRpcKind::ReestablishAuthorityClock);
    assert!(reestablish.is_authority_clock_admin());
    assert!(reestablish.requires_raft_authority_confirmation());

    let election = request(ControlPlaneRpcKind::TriggerRaftElection);
    assert!(!election.is_authority_clock_admin());
    assert!(!election.requires_raft_authority_confirmation());

    let heartbeat = request(ControlPlaneRpcKind::RefreshNodeHeartbeat);
    assert!(!heartbeat.is_authority_clock_admin());
    assert!(heartbeat.requires_raft_authority_confirmation());
}

#[test]
fn unix_control_plane_client_runtime_map_status_uses_compact_rpc() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(&mut authority, 1, 7, PgState::Peering, proof, false, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(7),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(&mut authority, 1, 7, PgState::Active, proof, false, 2_002);
    let expected_epoch = authority.snapshot().cluster_epoch();
    let expected_runtime_map = authority.runtime_map_snapshot(2_003).unwrap();
    let expected_digest = expected_runtime_map.content_digest();
    let expected_validity = expected_runtime_map.validity();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::RuntimeMapStatus);
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_003).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let status = client.runtime_map_status(2_003).unwrap();

    server.join().unwrap();
    assert_eq!(status.cluster_epoch(), expected_epoch);
    assert_eq!(status.pg_routes(), 1);
    assert_eq!(status.active_serving_pg_routes(), 1);
    let renewal = status
        .lease_renewal()
        .expect("serving runtime-map status should include a renewal certificate");
    assert_eq!(renewal.content_digest(), expected_digest);
    assert_eq!(renewal.validity(), expected_validity);
}

#[test]
fn unix_runtime_map_source_status_uses_check_applied_timeout_for_slow_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(&mut authority, 1, 7, PgState::Peering, proof, false, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(7),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(&mut authority, 1, 7, PgState::Active, proof, false, 2_002);
    let expected_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::RuntimeMapStatus);
        std::thread::sleep(CONTROL_PLANE_RPC_IO_TIMEOUT + Duration::from_millis(250));
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_003).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let status = ControlPlaneRuntimeMapSource::runtime_map_status(&client, 2_003).unwrap();

    server.join().unwrap();
    assert_eq!(status.cluster_epoch(), expected_epoch);
    assert_eq!(status.pg_routes(), 1);
    assert_eq!(status.active_serving_pg_routes(), 1);
}

#[test]
fn unix_pg_runtime_map_uses_check_applied_timeout_for_slow_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let expected_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::PgRuntimeMapSnapshot);
        std::thread::sleep(CONTROL_PLANE_RPC_IO_TIMEOUT + Duration::from_millis(250));
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_000).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let runtime_map =
        ControlPlaneRuntimeMapSource::pg_runtime_map_snapshot(&client, PgId::new(7), 2_000)
            .unwrap();

    server.join().unwrap();
    assert_eq!(runtime_map.cluster_epoch(), expected_epoch);
    assert_eq!(runtime_map.pg_routes().len(), 1);
    assert_eq!(runtime_map.pg_routes()[0].pg_id(), PgId::new(7));
    assert_eq!(runtime_map.pg_routes()[0].state(), PgState::Peering);
}

#[test]
fn authenticated_pg_runtime_map_uses_check_applied_timeout_for_slow_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let expected_epoch = authority.snapshot().cluster_epoch();
    let verifier = frontend_auth_verifier("auth-cluster", "frontend-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        std::thread::sleep(CONTROL_PLANE_RPC_IO_TIMEOUT + Duration::from_millis(250));
        handle_control_plane_unix_stream_with_auth(&mut authority, &mut stream, 2_000, &verifier)
            .unwrap();
    });

    let client = AuthenticatedUnixControlPlaneClient::new(
        UnixControlPlaneClient::new(&socket_path),
        frontend_auth_credential("auth-cluster", "frontend-1"),
    );
    let runtime_map =
        ControlPlaneRuntimeMapSource::pg_runtime_map_snapshot(&client, PgId::new(7), 2_000)
            .unwrap();

    server.join().unwrap();
    assert_eq!(runtime_map.cluster_epoch(), expected_epoch);
    assert_eq!(runtime_map.pg_routes().len(), 1);
    assert_eq!(runtime_map.pg_routes()[0].pg_id(), PgId::new(7));
    assert_eq!(runtime_map.pg_routes()[0].state(), PgState::Peering);
}

#[test]
fn unix_serving_pg_runtime_map_uses_check_applied_timeout_for_slow_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    let expected_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(
            request.kind,
            ControlPlaneRpcKind::ServingPgRuntimeMapSnapshot
        );
        std::thread::sleep(CONTROL_PLANE_RPC_IO_TIMEOUT + Duration::from_millis(250));
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_000).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let runtime_map =
        ControlPlaneRuntimeMapSource::serving_pg_runtime_map_snapshot(&client, PgId::new(7), 2_000)
            .unwrap();

    server.join().unwrap();
    assert_eq!(runtime_map.cluster_epoch(), expected_epoch);
    assert_eq!(runtime_map.pg_routes().len(), 1);
    assert_eq!(runtime_map.pg_routes()[0].pg_id(), PgId::new(7));
    assert!(runtime_map.freshness_proof().is_serving_authority_read());
}

#[test]
fn authenticated_pg_status_facade_uses_check_applied_timeout_for_slow_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(&mut authority, 1, 7, PgState::Peering, proof, false, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(7),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(&mut authority, 1, 7, PgState::Active, proof, false, 2_002);
    let expected_epoch = authority.snapshot().cluster_epoch();
    let expected_route_epoch =
        authority.runtime_map_snapshot(2_003).unwrap().pg_routes()[0].cluster_epoch();
    let verifier = frontend_auth_verifier("auth-cluster", "frontend-1");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        std::thread::sleep(CONTROL_PLANE_RPC_IO_TIMEOUT + Duration::from_millis(250));
        handle_control_plane_unix_stream_with_auth(&mut authority, &mut stream, 2_003, &verifier)
            .unwrap();
    });

    let client = crate::ControlPlanePgStatusClient::new(
        UnixControlPlaneClient::new(&socket_path),
        Some(frontend_auth_credential("auth-cluster", "frontend-1")),
    );
    let (runtime_epoch, route_epoch) =
        crate::clock::with_time_override(2_003, || client.serving_epochs(7, &[1])).unwrap();

    server.join().unwrap();
    assert_eq!(runtime_epoch, expected_epoch.get());
    assert_eq!(route_epoch, expected_route_epoch.get());
}

#[test]
fn unix_control_plane_client_runtime_map_snapshot_allows_slow_full_response() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(&mut authority, 1, 7, PgState::Peering, proof, false, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(7),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(&mut authority, 1, 7, PgState::Active, proof, false, 2_002);
    let expected_epoch = authority.snapshot().cluster_epoch();
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let request = read_control_plane_unix_request(&mut stream).unwrap();
        assert_eq!(request.kind, ControlPlaneRpcKind::RuntimeMapSnapshot);
        std::thread::sleep(CONTROL_PLANE_RPC_IO_TIMEOUT + Duration::from_millis(250));
        respond_control_plane_unix_request(&mut authority, &mut stream, request, 2_003).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let runtime_map = client.runtime_map_snapshot(2_003).unwrap();

    server.join().unwrap();
    assert_eq!(runtime_map.cluster_epoch(), expected_epoch);
    assert_eq!(runtime_map.pg_routes().len(), 1);
    assert_eq!(runtime_map.pg_routes()[0].state(), PgState::Active);
}

#[test]
fn refresh_node_heartbeat_rpc_response_omits_control_snapshot_body() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    heartbeat_until_serving_with_endpoint(&mut authority, 1, 1_000, "node-1.sock".to_owned());
    for node_id in 100..300 {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    let formatted_snapshot_len = format_snapshot(authority.snapshot()).len();
    assert!(
        formatted_snapshot_len > 10_000,
        "test setup should build a nontrivial snapshot"
    );
    let heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    let mut payload = Vec::new();
    write_node_heartbeat(&mut payload, &heartbeat).unwrap();
    let request = ControlPlaneRpcRequest {
        kind: ControlPlaneRpcKind::RefreshNodeHeartbeat,
        payload,
    };

    let response = build_control_plane_unix_response(&mut authority, request, 2_000).unwrap();

    assert_eq!(response.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
    assert!(
        response.payload.len() < formatted_snapshot_len / 4,
        "heartbeat response should carry compact lease metadata, not the full control-plane snapshot"
    );
    let response_payload = decode_control_plane_rpc_response(response.payload).unwrap();
    let mut reader = PayloadReader::new(&response_payload);
    let lease = read_heartbeat_lease_summary(&mut reader).unwrap();
    let runtime_map = read_runtime_map_snapshot(&mut reader).unwrap();
    reader.finish().unwrap();
    assert_eq!(lease.node_id(), NodeId::new(1));
    assert_eq!(lease.cluster_epoch(), runtime_map.cluster_epoch());
}

#[test]
fn runtime_map_diagnostics_reports_rpc_and_snapshot_metrics() {
    const DIAGNOSTIC_NODE_ID: u32 = 4_000_000_001;

    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(DIAGNOSTIC_NODE_ID), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(DIAGNOSTIC_NODE_ID)])
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, DIAGNOSTIC_NODE_ID, 1_000).serving());
    let history_epoch = authority.snapshot().cluster_epoch();
    let mut heartbeat = heartbeat_from_record(&authority, DIAGNOSTIC_NODE_ID, history_epoch, 2_000);
    heartbeat.cluster_map_history_route_references = history_route_references([
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            history_epoch,
            PgId::new(1),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
            history_epoch,
            PgId::new(1),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim,
            history_epoch,
            PgId::new(1),
        ),
    ]);
    let mut heartbeat_payload = Vec::new();
    write_node_heartbeat(&mut heartbeat_payload, &heartbeat).unwrap();
    let prepared = prepare_control_plane_heartbeat_response(
        &mut authority,
        ControlPlaneRpcRequest {
            kind: ControlPlaneRpcKind::RefreshNodeHeartbeat,
            payload: heartbeat_payload,
        },
        2_000,
        None,
    )
    .unwrap();
    let heartbeat_response =
        finish_control_plane_heartbeat_response(prepared, || Ok(2_001)).unwrap();
    decode_control_plane_rpc_response(heartbeat_response.payload).unwrap();
    let expected_sample = observability::ControlPlaneHistoryReferenceSample {
        node_id: DIAGNOSTIC_NODE_ID,
        observed_epoch: history_epoch.get(),
        validation_epoch: history_epoch.get(),
        observed_at_ms: 2_000,
        oldest_live_placement_epoch: Some(history_epoch.get()),
        oldest_durable_backfill_epoch: None,
        oldest_metadata_command_resource_epoch: Some(history_epoch.get()),
        oldest_object_payload_reclaim_claim_epoch: Some(history_epoch.get()),
    };
    assert_eq!(
        observability::control_plane_history_reference_samples()
            .into_iter()
            .find(|sample| sample.node_id == DIAGNOSTIC_NODE_ID),
        Some(expected_sample)
    );
    let rejected_observed_epoch = authority.snapshot().cluster_epoch();
    let mut rejected_heartbeat = heartbeat_from_record(
        &authority,
        DIAGNOSTIC_NODE_ID,
        rejected_observed_epoch,
        2_002,
    );
    rejected_heartbeat.cluster_map_history_route_references = history_route_references([
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            history_epoch,
            PgId::new(1),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
            ClusterEpoch::new(rejected_observed_epoch.get() + 1).unwrap(),
            PgId::new(1),
        ),
    ]);
    let mut rejected_payload = Vec::new();
    write_node_heartbeat(&mut rejected_payload, &rejected_heartbeat).unwrap();
    let rejected = prepare_control_plane_heartbeat_response(
        &mut authority,
        ControlPlaneRpcRequest {
            kind: ControlPlaneRpcKind::RefreshNodeHeartbeat,
            payload: rejected_payload,
        },
        2_002,
        None,
    )
    .unwrap();
    let rejected_response =
        finish_control_plane_heartbeat_response(rejected, || Ok(2_003)).unwrap();
    assert!(decode_control_plane_rpc_response(rejected_response.payload).is_err());
    assert_eq!(
        observability::control_plane_history_reference_samples()
            .into_iter()
            .find(|sample| sample.node_id == DIAGNOSTIC_NODE_ID),
        Some(expected_sample),
        "a rejected heartbeat must not replace the last accepted history report"
    );
    authority
        .set_node_membership(
            NodeId::new(DIAGNOSTIC_NODE_ID),
            NodeMembershipState::Draining,
        )
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    assert!(current_epoch > history_epoch);
    let mut stale_heartbeat =
        heartbeat_from_record(&authority, DIAGNOSTIC_NODE_ID, history_epoch, 2_004);
    stale_heartbeat.cluster_map_history_route_references = history_route_references([
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            history_epoch,
            PgId::new(1),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
            current_epoch,
            PgId::new(1),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
            history_epoch,
            PgId::new(1),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim,
            history_epoch,
            PgId::new(1),
        ),
    ]);
    let mut stale_payload = Vec::new();
    write_node_heartbeat(&mut stale_payload, &stale_heartbeat).unwrap();
    let stale_prepared = prepare_control_plane_heartbeat_response(
        &mut authority,
        ControlPlaneRpcRequest {
            kind: ControlPlaneRpcKind::RefreshNodeHeartbeat,
            payload: stale_payload,
        },
        2_004,
        None,
    )
    .unwrap();
    let stale_response =
        finish_control_plane_heartbeat_response(stale_prepared, || Ok(2_005)).unwrap();
    decode_control_plane_rpc_response(stale_response.payload).unwrap();
    let expected_stale_sample = observability::ControlPlaneHistoryReferenceSample {
        validation_epoch: current_epoch.get(),
        observed_at_ms: 2_004,
        oldest_durable_backfill_epoch: Some(current_epoch.get()),
        ..expected_sample
    };
    assert_eq!(
        observability::control_plane_history_reference_samples()
            .into_iter()
            .find(|sample| sample.node_id == DIAGNOSTIC_NODE_ID),
        Some(expected_stale_sample),
        "an accepted stale report should replace the diagnostic sample"
    );

    let mut inconsistent_stale_heartbeat =
        heartbeat_from_record(&authority, DIAGNOSTIC_NODE_ID, history_epoch, 2_006);
    inconsistent_stale_heartbeat.cluster_map_history_route_references = history_route_references([
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            history_epoch,
            PgId::new(1),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
            ClusterEpoch::new(current_epoch.get() + 1).unwrap(),
            PgId::new(1),
        ),
    ]);
    let mut inconsistent_stale_payload = Vec::new();
    write_node_heartbeat(
        &mut inconsistent_stale_payload,
        &inconsistent_stale_heartbeat,
    )
    .unwrap();
    let inconsistent_stale = prepare_control_plane_heartbeat_response(
        &mut authority,
        ControlPlaneRpcRequest {
            kind: ControlPlaneRpcKind::RefreshNodeHeartbeat,
            payload: inconsistent_stale_payload,
        },
        2_006,
        None,
    )
    .unwrap();
    let inconsistent_stale_response =
        finish_control_plane_heartbeat_response(inconsistent_stale, || Ok(2_007)).unwrap();
    assert!(decode_control_plane_rpc_response(inconsistent_stale_response.payload).is_err());
    assert_eq!(
        observability::control_plane_history_reference_samples()
            .into_iter()
            .find(|sample| sample.node_id == DIAGNOSTIC_NODE_ID),
        Some(expected_stale_sample),
        "an inconsistent stale report must not replace the last accepted sample"
    );
    assert!(authority
        .snapshot()
        .runtime_map(2_008)
        .unwrap()
        .nodes()
        .iter()
        .any(|node| node.node_id() == NodeId::new(DIAGNOSTIC_NODE_ID)));
    let request = ControlPlaneRpcRequest {
        kind: ControlPlaneRpcKind::RuntimeMapDiagnostics,
        payload: Vec::new(),
    };
    let expected_lease_deadline_ms = authority
        .snapshot()
        .node(NodeId::new(DIAGNOSTIC_NODE_ID))
        .and_then(NodeControlRecord::lease_deadline_ms);

    let response = build_control_plane_unix_response(&mut authority, request, 2_008).unwrap();

    assert_eq!(response.kind, ControlPlaneRpcKind::RuntimeMapDiagnostics);
    let response_payload = decode_control_plane_rpc_response(response.payload).unwrap();
    let mut reader = PayloadReader::new(&response_payload);
    let diagnostics = read_control_plane_runtime_map_diagnostics(&mut reader).unwrap();
    reader.finish().unwrap();
    assert!(diagnostics.runtime_map().cluster_epoch() > ClusterEpoch::INITIAL);
    assert_eq!(
        diagnostics.rpc_metrics().len(),
        observability::ControlPlaneRpcMetricKind::COUNT
    );
    assert!(
        diagnostics.snapshot_metrics().save_total >= 1,
        "the preceding durable mutation should be measured"
    );
    assert!(diagnostics.snapshot_metrics().bytes_last > 0);
    assert!(
        diagnostics.journal_metrics().append_total >= 1,
        "durable commands after initial creation should be journaled"
    );
    assert!(diagnostics.journal_metrics().frame_bytes_last > 0);
    assert_eq!(
        diagnostics.history_reference_samples(),
        &[expected_stale_sample]
    );
    assert_eq!(
        diagnostics.node_leases(),
        &[ControlPlaneRuntimeMapNodeLeaseDiagnostic::new(
            NodeId::new(DIAGNOSTIC_NODE_ID),
            expected_lease_deadline_ms,
        )]
    );

    observability::record_control_plane_history_reference_sample(
        observability::ControlPlaneHistoryReferenceSample {
            oldest_durable_backfill_epoch: Some(current_epoch.get() + 1),
            ..expected_stale_sample
        },
    );
    let malformed_response = build_control_plane_unix_response(
        &mut authority,
        ControlPlaneRpcRequest {
            kind: ControlPlaneRpcKind::RuntimeMapDiagnostics,
            payload: Vec::new(),
        },
        2_008,
    )
    .unwrap();
    let malformed_payload = decode_control_plane_rpc_response(malformed_response.payload).unwrap();
    let malformed_error =
        read_control_plane_runtime_map_diagnostics(&mut PayloadReader::new(&malformed_payload))
            .unwrap_err();
    assert!(
        matches!(
            &malformed_error,
            ControlPlaneError::RpcProtocol { diagnostic: message }
                if message.contains("future durable backfill epoch")
        ),
        "unexpected malformed diagnostics error: {malformed_error}"
    );
}

#[test]
fn prepared_heartbeat_response_can_be_finished_after_authority_changes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let heartbeat =
        heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
    let mut payload = Vec::new();
    write_node_heartbeat(&mut payload, &heartbeat).unwrap();
    let request = ControlPlaneRpcRequest {
        kind: ControlPlaneRpcKind::RefreshNodeHeartbeat,
        payload,
    };

    let prepared =
        prepare_control_plane_heartbeat_response(&mut authority, request, 2_000, None).unwrap();
    let prepared_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    assert!(
        authority.snapshot().cluster_epoch() > prepared_epoch,
        "test setup should mutate authority after heartbeat preparation"
    );

    let response = finish_control_plane_heartbeat_response(prepared, || Ok(2_001)).unwrap();

    assert_eq!(response.kind, ControlPlaneRpcKind::RefreshNodeHeartbeat);
    let response_payload = decode_control_plane_rpc_response(response.payload).unwrap();
    let mut reader = PayloadReader::new(&response_payload);
    let lease = read_heartbeat_lease_summary(&mut reader).unwrap();
    let runtime_map = read_runtime_map_snapshot(&mut reader).unwrap();
    reader.finish().unwrap();
    assert_eq!(lease.cluster_epoch(), prepared_epoch);
    assert_eq!(runtime_map.cluster_epoch(), prepared_epoch);
}

#[test]
fn control_plane_rpc_rejects_corrupted_payload_checksum() {
    let (mut writer, mut reader) = UnixStream::pair().unwrap();
    let payload = b"not a valid request";
    writer.write_all(CONTROL_PLANE_RPC_MAGIC).unwrap();
    write_u16_to_stream(&mut writer, CONTROL_PLANE_RPC_VERSION);
    write_u16_to_stream(
        &mut writer,
        ControlPlaneRpcKind::RefreshNodeHeartbeat as u16,
    );
    write_u32_to_stream(&mut writer, payload.len() as u32);
    write_u64_to_stream(&mut writer, 0);
    writer.write_all(payload).unwrap();

    let error = read_control_plane_unix_request(&mut reader).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("checksum mismatch")
    ));
}

#[test]
fn control_plane_rpc_rejects_previous_and_future_version_fixtures() {
    for version in [
        13,
        CONTROL_PLANE_RPC_VERSION - 1,
        CONTROL_PLANE_RPC_VERSION + 1,
    ] {
        let frame = encode_control_plane_rpc_frame_with_version(
            ControlPlaneRpcKind::RuntimeMapStatus,
            &[],
            version,
        )
        .unwrap();
        let reservation_calls = Cell::new(0);
        let error =
            read_control_plane_rpc_frame_with_reservation(&mut std::io::Cursor::new(frame), |_| {
                reservation_calls.set(reservation_calls.get() + 1);
                Ok(())
            })
            .unwrap_err();

        assert!(matches!(
            error,
            ControlPlaneError::RpcProtocol { diagnostic: message }
                if message.as_str()
                    == format!("unsupported control-plane RPC version {version}")
        ));
        assert_eq!(reservation_calls.get(), 0);
    }
}

#[test]
fn control_plane_rpc_reader_preserves_typed_marker_precedence() {
    let mut unknown_magic = CONTROL_PLANE_RPC_MAGIC.to_vec();
    unknown_magic[0] ^= 1;
    let error = read_control_plane_rpc_frame(&mut std::io::Cursor::new(unknown_magic)).unwrap_err();
    assert!(!error.is_retryable_control_plane_rpc_transport_error());
    assert!(!error.is_maybe_applied_control_plane_rpc_response_loss());
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic }
            if diagnostic.as_str() == "invalid control-plane RPC magic"
    ));

    for truncated in [
        Vec::new(),
        CONTROL_PLANE_RPC_MAGIC[..CONTROL_PLANE_RPC_MAGIC.len() - 1].to_vec(),
        CONTROL_PLANE_RPC_MAGIC.to_vec(),
    ] {
        let error = read_control_plane_rpc_frame(&mut std::io::Cursor::new(truncated)).unwrap_err();
        assert!(error.is_retryable_control_plane_rpc_transport_error());
        assert!(!error.is_maybe_applied_control_plane_rpc_response_loss());
        assert!(matches!(
            error,
            ControlPlaneError::RpcProtocol { diagnostic }
                if diagnostic.as_str() == "truncated control-plane RPC frame marker"
        ));
    }
}

#[test]
fn control_plane_rpc_rejects_corrupted_header_checksum() {
    let (mut writer, mut reader) = UnixStream::pair().unwrap();
    let payload = b"";
    let checksum = control_plane_rpc_frame_checksum(
        CONTROL_PLANE_RPC_VERSION,
        ControlPlaneRpcKind::RuntimeMapSnapshot as u16,
        payload.len() as u32,
        payload,
    );
    writer.write_all(CONTROL_PLANE_RPC_MAGIC).unwrap();
    write_u16_to_stream(&mut writer, CONTROL_PLANE_RPC_VERSION);
    write_u16_to_stream(
        &mut writer,
        ControlPlaneRpcKind::RefreshNodeHeartbeat as u16,
    );
    write_u32_to_stream(&mut writer, payload.len() as u32);
    write_u64_to_stream(&mut writer, checksum);

    let error = read_control_plane_unix_request(&mut reader).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("checksum mismatch")
    ));
}

#[test]
fn unix_control_plane_client_rejects_corrupted_response_checksum() {
    let tmp = test_util::tempdir();
    let socket_path = tmp.path().join("control-plane.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _addr) = listener.accept().unwrap();
        let _request = read_control_plane_unix_request(&mut stream).unwrap();
        let payload = [0u8];
        stream.write_all(CONTROL_PLANE_RPC_MAGIC).unwrap();
        write_u16_to_stream(&mut stream, CONTROL_PLANE_RPC_VERSION);
        write_u16_to_stream(&mut stream, ControlPlaneRpcKind::RuntimeMapSnapshot as u16);
        write_u32_to_stream(&mut stream, payload.len() as u32);
        write_u64_to_stream(&mut stream, 0);
        stream.write_all(&payload).unwrap();
    });

    let client = UnixControlPlaneClient::new(&socket_path);
    let error = client.runtime_map_snapshot(0).unwrap_err();

    server.join().unwrap();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("checksum mismatch")
    ));
}

#[test]
fn control_plane_rpc_rejects_oversized_heartbeat_observation_count_before_allocation() {
    let mut payload = Vec::new();
    write_u32(&mut payload, 1);
    write_u64(&mut payload, 1);
    write_string(&mut payload, "/tmp/argmin-node-1.sock").unwrap();
    write_u64(&mut payload, ClusterEpoch::INITIAL.get());
    write_u64(&mut payload, 100);
    write_u64(&mut payload, 1);
    write_u32(&mut payload, 0);
    write_u32(&mut payload, u32::MAX);

    let mut reader = PayloadReader::new(&payload);
    let error = read_node_heartbeat(&mut reader).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("PG observations count")
    ));
}

#[test]
fn control_plane_rpc_heartbeat_round_trips_exact_history_route_references() {
    let references = PgClusterMapHistoryRouteReferences::try_from_iter([
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            ClusterEpoch::new(3).unwrap(),
            PgId::new(7),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
            ClusterEpoch::new(2).unwrap(),
            PgId::new(8),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim,
            ClusterEpoch::new(1).unwrap(),
            PgId::new(7),
        ),
    ])
    .unwrap();
    let heartbeat = NodeHeartbeat {
        node_id: NodeId::new(1),
        node_incarnation: 2,
        endpoint: "/tmp/argmin-node-1.sock".to_owned(),
        observed_epoch: ClusterEpoch::new(3).unwrap(),
        requested_lease_duration_ms: 100,
        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1).unwrap(),
        cluster_map_history_route_references: references,
        pg_observations: Vec::new(),
    };

    let encoded = write_node_heartbeat_payload(&heartbeat).unwrap();
    assert_eq!(read_node_heartbeat_payload(&encoded).unwrap(), heartbeat);
}

#[test]
fn control_plane_rpc_rejects_oversized_history_route_count_before_allocation() {
    let mut payload = Vec::new();
    write_u32(&mut payload, 1);
    write_u64(&mut payload, 1);
    write_string(&mut payload, "/tmp/argmin-node-1.sock").unwrap();
    write_u64(&mut payload, ClusterEpoch::INITIAL.get());
    write_u64(&mut payload, 100);
    write_u64(&mut payload, 1);
    write_u32(&mut payload, u32::MAX);

    let mut reader = PayloadReader::new(&payload);
    let error = read_node_heartbeat(&mut reader).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("cluster-map history route references count")
    ));
}

#[test]
fn control_plane_rpc_rejects_malformed_heartbeat_pending_command_presence() {
    let mut payload = Vec::new();
    write_u32(&mut payload, 1);
    write_u64(&mut payload, 1);
    write_string(&mut payload, "/tmp/argmin-node-1.sock").unwrap();
    write_u64(&mut payload, ClusterEpoch::INITIAL.get());
    write_u64(&mut payload, 100);
    write_u64(&mut payload, 1);
    write_u32(&mut payload, 0);
    write_u32(&mut payload, 1);
    write_u32(&mut payload, 7);
    write_pg_state(&mut payload, PgState::Peering);
    write_pg_metadata_proof(&mut payload, PgMetadataProof::current(10, 11, 12));
    write_u8(&mut payload, 2);

    let mut reader = PayloadReader::new(&payload);
    let error = read_node_heartbeat(&mut reader).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("invalid pending metadata command presence code 2")
    ));
}

#[test]
fn control_plane_rpc_rejects_oversized_runtime_node_count_before_allocation() {
    let mut payload = Vec::new();
    write_u64(&mut payload, ClusterEpoch::INITIAL.get());
    write_option_u64(&mut payload, None);
    write_runtime_map_test_single_authority_proof(&mut payload);
    write_u32(&mut payload, u32::MAX);

    let mut reader = PayloadReader::new(&payload);
    let error = read_runtime_map_snapshot(&mut reader).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("runtime nodes count")
    ));
}

#[test]
fn control_plane_rpc_rejects_oversized_runtime_route_count_before_allocation() {
    let mut payload = Vec::new();
    write_u64(&mut payload, ClusterEpoch::INITIAL.get());
    write_option_u64(&mut payload, None);
    write_runtime_map_test_single_authority_proof(&mut payload);
    write_u32(&mut payload, 0);
    write_u32(&mut payload, u32::MAX);

    let mut reader = PayloadReader::new(&payload);
    let error = read_runtime_map_snapshot(&mut reader).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("PG routes count")
    ));
}

#[test]
fn control_plane_rpc_rejects_oversized_runtime_acting_set_count_before_allocation() {
    let mut payload = Vec::new();
    write_u64(&mut payload, ClusterEpoch::INITIAL.get());
    write_option_u64(&mut payload, None);
    write_runtime_map_test_single_authority_proof(&mut payload);
    write_u32(&mut payload, 0);
    write_u32(&mut payload, 1);
    write_u64(&mut payload, ClusterEpoch::INITIAL.get());
    write_u32(&mut payload, 7);
    write_u32(&mut payload, 1);
    write_pg_state(&mut payload, PgState::Active);
    write_u8(&mut payload, 0);
    write_u8(&mut payload, 0);
    write_option_u64(&mut payload, None);
    write_u8(&mut payload, 0);
    write_u8(&mut payload, 0);
    write_u32(&mut payload, u32::MAX);

    let mut reader = PayloadReader::new(&payload);
    let error = read_runtime_map_snapshot(&mut reader).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("PG route acting set count")
    ));
}

#[test]
fn control_plane_rpc_rejects_invalid_runtime_map_freshness_proof() {
    let mut payload = Vec::new();
    write_u64(&mut payload, ClusterEpoch::INITIAL.get());
    write_option_u64(&mut payload, None);
    write_u8(&mut payload, 99);

    let mut reader = PayloadReader::new(&payload);
    let error = read_runtime_map_snapshot(&mut reader).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("invalid runtime map freshness proof tag 99")
    ));
}

#[test]
fn control_plane_rpc_rejects_zero_runtime_map_freshness_proof_incarnation() {
    let mut payload = Vec::new();
    write_u64(&mut payload, ClusterEpoch::INITIAL.get());
    write_option_u64(&mut payload, None);
    write_u8(
        &mut payload,
        CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_SINGLE_AUTHORITY,
    );
    write_u64(&mut payload, 0);

    let mut reader = PayloadReader::new(&payload);
    let error = read_runtime_map_snapshot(&mut reader).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("runtime map freshness proof authority incarnation must be nonzero")
    ));
}

#[test]
fn control_plane_rpc_rejects_zero_runtime_map_read_index_proof_index() {
    let mut payload = Vec::new();
    write_u64(&mut payload, ClusterEpoch::INITIAL.get());
    write_option_u64(&mut payload, None);
    write_u8(&mut payload, CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_READ_INDEX);
    write_u64(&mut payload, AuthorityIncarnation::INITIAL.get());
    write_u64(&mut payload, 7);
    write_u64(&mut payload, 0);

    let mut reader = PayloadReader::new(&payload);
    let error = read_runtime_map_snapshot(&mut reader).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("runtime map freshness proof read-index index must be nonzero")
    ));
}

#[test]
fn control_plane_rpc_rejects_zero_runtime_map_read_index_proof_term() {
    let mut payload = Vec::new();
    write_u64(&mut payload, ClusterEpoch::INITIAL.get());
    write_option_u64(&mut payload, None);
    write_u8(&mut payload, CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_READ_INDEX);
    write_u64(&mut payload, AuthorityIncarnation::INITIAL.get());
    write_u64(&mut payload, 0);
    write_u64(&mut payload, 42);

    let mut reader = PayloadReader::new(&payload);
    let error = read_runtime_map_snapshot(&mut reader).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("runtime map freshness proof read-index term must be nonzero")
    ));
}

#[test]
fn control_plane_rpc_rejects_duplicate_runtime_map_nodes() {
    let mut snapshot = runtime_map_test_snapshot_with_active_route();
    snapshot.nodes.push(snapshot.nodes[0].clone());
    let error = decode_runtime_map_test_snapshot(snapshot).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("runtime map contains duplicate node 1")
    ));
}

#[test]
fn control_plane_rpc_rejects_runtime_map_route_unknown_node() {
    let mut snapshot = runtime_map_test_snapshot_with_active_route();
    snapshot.pg_routes[0].acting_set.push(NodeId::new(99));
    let error = decode_runtime_map_test_snapshot(snapshot).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("runtime map route for PG 7 references unknown acting-set node 99")
    ));
}

#[test]
fn control_plane_rpc_rejects_duplicate_runtime_map_pg_routes() {
    let mut snapshot = runtime_map_test_snapshot_with_active_route();
    snapshot.pg_routes.push(snapshot.pg_routes[0].clone());
    let error = decode_runtime_map_test_snapshot(snapshot).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("runtime map contains duplicate route for PG 7")
    ));
}

#[test]
fn control_plane_rpc_round_trips_pending_command_recovery_authorization() {
    let mut snapshot = runtime_map_test_snapshot_with_active_route();
    let historical = snapshot.pg_routes[0].without_serving_authority();
    snapshot.cluster_epoch = ClusterEpoch::new(2).unwrap();
    snapshot.pg_routes[0].cluster_epoch = snapshot.cluster_epoch;
    snapshot.pg_routes[0].state = PgState::Peering;
    snapshot.pg_routes[0].active_metadata_proof = None;
    snapshot.pg_routes[0].primary_lease_deadline_ms = None;
    snapshot.pg_routes[0].pending_metadata_command_recovery =
        Some(PendingMetadataCommandRecovery::new(
            NodeId::new(1),
            PendingMetadataCommandObservation::new(ClusterEpoch::INITIAL, NonZeroU64::MIN, 0x1234),
        ));
    snapshot
        .historical_cluster_epochs
        .push(ClusterEpoch::INITIAL);
    snapshot.historical_pg_routes.push(historical);

    let decoded = decode_runtime_map_test_snapshot(snapshot.clone()).unwrap();
    assert_eq!(decoded, snapshot);
}

#[test]
fn control_plane_rpc_rejects_runtime_map_transfer_without_source_route() {
    let mut snapshot = runtime_map_test_snapshot_with_active_route();
    snapshot.cluster_epoch = ClusterEpoch::new(2).unwrap();
    let route = &mut snapshot.pg_routes[0];
    route.cluster_epoch = snapshot.cluster_epoch;
    route.state = PgState::Peering;
    route.active_metadata_proof = None;
    route.primary_lease_deadline_ms = None;
    route.peering_metadata_transfer = Some(PgMetadataTransferProof::new(
        ClusterEpoch::INITIAL,
        PgMetadataProof::current(1, 2, 3),
    ));
    route.peering_metadata_transfer_destination_epoch = Some(snapshot.cluster_epoch);
    let error = decode_runtime_map_test_snapshot(snapshot).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("runtime map route for PG 7 has incomplete metadata transfer source route")
    ));
}

#[test]
fn control_plane_rpc_rejects_historical_runtime_route_at_current_epoch() {
    let mut snapshot = runtime_map_test_snapshot_with_active_route();
    snapshot
        .historical_pg_routes
        .push(snapshot.pg_routes[0].without_serving_authority());
    let error = decode_runtime_map_test_snapshot(snapshot).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("runtime map historical route for PG 7 has epoch")
                && message.contains("expected an epoch older than")
    ));
}

#[test]
fn control_plane_rpc_rejects_historical_runtime_route_from_future_epoch() {
    let mut snapshot = runtime_map_test_snapshot_with_active_route();
    let mut future_route = snapshot.pg_routes[0].without_serving_authority();
    future_route.cluster_epoch = ClusterEpoch::new(snapshot.cluster_epoch().get() + 1).unwrap();
    snapshot.historical_pg_routes.push(future_route);
    let error = decode_runtime_map_test_snapshot(snapshot).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("runtime map historical route for PG 7 has epoch")
                && message.contains("expected an epoch older than")
    ));
}

#[test]
fn control_plane_rpc_rejects_runtime_map_transfer_unknown_source_node() {
    let mut snapshot = runtime_map_test_snapshot_with_active_route();
    snapshot.cluster_epoch = ClusterEpoch::new(2).unwrap();
    let route = &mut snapshot.pg_routes[0];
    route.cluster_epoch = snapshot.cluster_epoch;
    route.state = PgState::Peering;
    route.active_metadata_proof = None;
    route.primary_lease_deadline_ms = None;
    route.peering_metadata_transfer = Some(PgMetadataTransferProof::new(
        ClusterEpoch::INITIAL,
        PgMetadataProof::current(1, 2, 3),
    ));
    route.peering_metadata_transfer_destination_epoch = Some(snapshot.cluster_epoch);
    route.peering_metadata_transfer_source_route_epoch = Some(ClusterEpoch::INITIAL);
    route.peering_metadata_transfer_source_node_id = Some(NodeId::new(99));
    let error = decode_runtime_map_test_snapshot(snapshot).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("runtime map route for PG 7 references unknown metadata transfer source node 99")
    ));
}

#[test]
fn control_plane_rpc_rejects_runtime_map_transfer_missing_source_route_epoch() {
    let snapshot = runtime_map_test_snapshot_with_transfer_route(false);
    let error = decode_runtime_map_test_snapshot(snapshot).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("runtime map route for PG 7 references missing metadata transfer source route epoch")
    ));
}

#[test]
fn control_plane_rpc_rejects_runtime_map_transfer_source_primary_mismatch() {
    let mut snapshot = runtime_map_test_snapshot_with_transfer_route(true);
    snapshot.nodes.push(NodeRouteSnapshot {
        node_id: NodeId::new(2),
        node_incarnation: 12,
        endpoint: "/tmp/argmin-node-2.sock".to_owned(),
        cluster_map_history_floor_epoch: None,
    });
    let historical = &mut snapshot.historical_pg_routes[0];
    historical.primary_node_id = NodeId::new(2);
    historical.acting_set = vec![NodeId::new(2)];
    let error = decode_runtime_map_test_snapshot(snapshot).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("metadata transfer source node 1 does not match source route primary 2")
    ));
}

#[test]
fn control_plane_rpc_rejects_runtime_map_transfer_source_route_self_reference() {
    let mut snapshot = runtime_map_test_snapshot_with_transfer_route(true);
    let destination_epoch = snapshot.cluster_epoch;
    snapshot.historical_pg_routes.clear();
    let route = &mut snapshot.pg_routes[0];
    route.peering_metadata_transfer_source_route_epoch = Some(destination_epoch);
    let error = decode_runtime_map_test_snapshot(snapshot).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("metadata transfer source route epoch")
                && message.contains("must be older than transfer route epoch")
    ));
}

#[test]
fn control_plane_rpc_rejects_historical_runtime_map_transfer_source_route_forward_reference() {
    let mut snapshot = runtime_map_test_snapshot_with_transfer_route(true);
    let mut transfer_route = snapshot.pg_routes[0].clone();
    let transfer_epoch = transfer_route.cluster_epoch();
    let current_epoch = next_epoch(transfer_epoch).unwrap();
    snapshot.cluster_epoch = current_epoch;
    snapshot.pg_routes[0].cluster_epoch = current_epoch;
    transfer_route.peering_metadata_transfer_source_route_epoch = Some(current_epoch);
    transfer_route.peering_metadata_transfer_source_node_id = Some(NodeId::new(1));
    snapshot.pg_routes[0].peering_metadata_transfer = None;
    snapshot.pg_routes[0].peering_metadata_transfer_destination_epoch = None;
    snapshot.pg_routes[0].peering_metadata_transfer_source_route_epoch = None;
    snapshot.pg_routes[0].peering_metadata_transfer_source_node_id = None;
    snapshot.historical_pg_routes.clear();
    snapshot.historical_pg_routes.push(transfer_route);
    snapshot.historical_cluster_epochs.push(transfer_epoch);
    let error = decode_runtime_map_test_snapshot(snapshot).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic: message }
            if message.contains("metadata transfer source route epoch")
                && message.contains("must be older than transfer route epoch")
    ));
}

#[test]
fn control_plane_rpc_round_trips_runtime_map_transfer_source_route_reference() {
    let snapshot = runtime_map_test_snapshot_with_transfer_route(true);
    assert_eq!(
        decode_runtime_map_test_snapshot(snapshot.clone()).unwrap(),
        snapshot
    );
}

#[test]
fn control_plane_rpc_rejects_noncanonical_historical_epoch_order() {
    let mut snapshot = runtime_map_test_snapshot(RuntimeMapFreshnessProof::Reconstructed {
        authority_incarnation: AuthorityIncarnation::INITIAL,
    });
    snapshot.cluster_epoch = ClusterEpoch::new(3).unwrap();
    snapshot.historical_cluster_epochs =
        vec![ClusterEpoch::new(2).unwrap(), ClusterEpoch::new(1).unwrap()];

    assert!(matches!(
        decode_runtime_map_test_snapshot(snapshot),
        Err(ControlPlaneError::RpcProtocol { diagnostic: message })
            if message.as_str() == "runtime map historical epochs are not strictly increasing"
    ));
}

#[test]
fn control_plane_rpc_round_trips_single_authority_runtime_map_freshness_proof() {
    let snapshot = runtime_map_test_snapshot(RuntimeMapFreshnessProof::SingleAuthority {
        authority_incarnation: AuthorityIncarnation::INITIAL,
        issued_at_ms: 12_345,
    });
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &snapshot).unwrap();

    let mut reader = PayloadReader::new(&payload);
    let decoded = read_runtime_map_snapshot(&mut reader).unwrap();
    reader.finish().unwrap();

    assert_eq!(decoded, snapshot);
    assert_eq!(
        decoded.freshness_proof(),
        &RuntimeMapFreshnessProof::SingleAuthority {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            issued_at_ms: 12_345,
        }
    );
}

#[test]
fn control_plane_rpc_round_trips_read_index_runtime_map_freshness_proof() {
    let read_index = ControlPlaneLogId::new(7, 42).unwrap();
    let snapshot = runtime_map_test_snapshot(RuntimeMapFreshnessProof::ReadIndex {
        authority_incarnation: AuthorityIncarnation::INITIAL,
        read_index,
        issued_at_ms: 12_345,
    });
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &snapshot).unwrap();

    let mut reader = PayloadReader::new(&payload);
    let decoded = read_runtime_map_snapshot(&mut reader).unwrap();
    reader.finish().unwrap();

    assert_eq!(decoded, snapshot);
    assert_eq!(
        decoded.freshness_proof(),
        &RuntimeMapFreshnessProof::ReadIndex {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            read_index,
            issued_at_ms: 12_345,
        }
    );
    assert_eq!(decoded.freshness_proof().read_index(), Some(read_index));
    assert!(decoded.freshness_proof().is_serving_authority_read());
}

#[test]
fn control_plane_rpc_round_trips_reconstructed_runtime_map_freshness_proof() {
    let snapshot = runtime_map_test_snapshot(RuntimeMapFreshnessProof::Reconstructed {
        authority_incarnation: AuthorityIncarnation::INITIAL,
    });
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &snapshot).unwrap();

    let mut reader = PayloadReader::new(&payload);
    let decoded = read_runtime_map_snapshot(&mut reader).unwrap();
    reader.finish().unwrap();

    assert_eq!(decoded, snapshot);
    assert_eq!(
        decoded.freshness_proof(),
        &RuntimeMapFreshnessProof::Reconstructed {
            authority_incarnation: AuthorityIncarnation::INITIAL,
        }
    );
}

fn write_u16_to_stream(stream: &mut UnixStream, value: u16) {
    stream.write_all(&value.to_be_bytes()).unwrap();
}

fn write_u32_to_stream(stream: &mut UnixStream, value: u32) {
    stream.write_all(&value.to_be_bytes()).unwrap();
}

fn write_u64_to_stream(stream: &mut UnixStream, value: u64) {
    stream.write_all(&value.to_be_bytes()).unwrap();
}

fn write_runtime_map_test_single_authority_proof(out: &mut Vec<u8>) {
    write_u8(out, CONTROL_PLANE_RPC_RUNTIME_MAP_PROOF_SINGLE_AUTHORITY);
    write_u64(out, AuthorityIncarnation::INITIAL.get());
    write_u64(out, 1_000);
}

fn decode_runtime_map_test_snapshot(
    snapshot: ClusterRuntimeMapSnapshot,
) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &snapshot).unwrap();
    let mut reader = PayloadReader::new(&payload);
    let decoded = read_runtime_map_snapshot(&mut reader)?;
    reader.finish()?;
    Ok(decoded)
}

fn runtime_map_test_snapshot(
    freshness_proof: RuntimeMapFreshnessProof,
) -> ClusterRuntimeMapSnapshot {
    ClusterRuntimeMapSnapshot {
        cluster_epoch: ClusterEpoch::INITIAL,
        validity: RouteMapValidity::until_ms(12_345).unwrap(),
        freshness_proof,
        nodes: Vec::new(),
        pg_routes: Vec::new(),
        historical_pg_routes: Vec::new(),
        historical_cluster_epochs: Vec::new(),
    }
}

pub(super) fn runtime_map_test_snapshot_with_active_route() -> ClusterRuntimeMapSnapshot {
    ClusterRuntimeMapSnapshot {
        cluster_epoch: ClusterEpoch::INITIAL,
        validity: RouteMapValidity::until_ms(12_345).unwrap(),
        freshness_proof: RuntimeMapFreshnessProof::SingleAuthority {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            issued_at_ms: 12_000,
        },
        nodes: vec![NodeRouteSnapshot {
            node_id: NodeId::new(1),
            node_incarnation: 11,
            endpoint: "/tmp/argmin-node-1.sock".to_owned(),
            cluster_map_history_floor_epoch: None,
        }],
        pg_routes: vec![PgRouteSnapshot {
            cluster_epoch: ClusterEpoch::INITIAL,
            pg_id: PgId::new(7),
            primary_node_id: NodeId::new(1),
            acting_set: vec![NodeId::new(1)],
            state: PgState::Active,
            active_metadata_proof: Some(PgMetadataProof::current(2, 3, 4)),
            primary_lease_deadline_ms: Some(12_345),
            peering_metadata_transfer: None,
            peering_metadata_transfer_destination_epoch: None,
            peering_metadata_transfer_source_route_epoch: None,
            peering_metadata_transfer_source_node_id: None,
            pending_metadata_command_recovery: None,
            metadata_read_route: None,
        }],
        historical_pg_routes: Vec::new(),
        historical_cluster_epochs: Vec::new(),
    }
}

#[test]
fn control_plane_rpc_round_trips_active_route_metadata_proof() {
    let snapshot = runtime_map_test_snapshot_with_active_route();
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &snapshot).unwrap();

    let mut reader = PayloadReader::new(&payload);
    let decoded = read_runtime_map_snapshot(&mut reader).unwrap();
    reader.finish().unwrap();

    assert_eq!(decoded, snapshot);
    assert_eq!(
        decoded.pg_routes()[0].active_metadata_proof(),
        Some(PgMetadataProof::current(2, 3, 4))
    );
}

#[test]
fn runtime_map_content_digest_excludes_lease_freshness_but_binds_route_content() {
    let snapshot = runtime_map_test_snapshot_with_active_route();
    let expected = snapshot.content_digest();
    let mut renewed = snapshot.clone();
    renewed.validity = RouteMapValidity::until_ms(22_345).unwrap();
    renewed.freshness_proof = RuntimeMapFreshnessProof::ReadIndex {
        authority_incarnation: AuthorityIncarnation::INITIAL,
        read_index: ControlPlaneLogId::new(7, 9).unwrap(),
        issued_at_ms: 22_000,
    };
    renewed.pg_routes[0].primary_lease_deadline_ms = Some(22_345);

    assert_eq!(renewed.content_digest(), expected);

    renewed.pg_routes[0].active_metadata_proof = Some(PgMetadataProof::current(3, 4, 5));
    assert_ne!(renewed.content_digest(), expected);
    renewed.pg_routes[0].active_metadata_proof = snapshot.pg_routes[0].active_metadata_proof;

    renewed.nodes[0].endpoint = "/tmp/argmin-node-1-replaced.sock".to_owned();
    assert_ne!(renewed.content_digest(), expected);
}

#[test]
fn runtime_map_status_lease_renewal_codec_round_trips_and_fails_closed() {
    let snapshot = runtime_map_test_snapshot_with_active_route();
    let status = ControlPlaneRuntimeMapStatus::from_runtime_map(&snapshot);
    let mut payload = Vec::new();
    write_runtime_map_status(&mut payload, status).unwrap();
    let mut reader = PayloadReader::new(&payload);
    assert_eq!(read_runtime_map_status(&mut reader).unwrap(), status);
    reader.finish().unwrap();

    let mut reconstructed_snapshot = snapshot.clone();
    reconstructed_snapshot.freshness_proof = RuntimeMapFreshnessProof::Reconstructed {
        authority_incarnation: AuthorityIncarnation::INITIAL,
    };
    assert!(
        ControlPlaneRuntimeMapStatus::from_runtime_map(&reconstructed_snapshot)
            .lease_renewal()
            .is_none()
    );

    let mut unbounded = Vec::new();
    write_u64(&mut unbounded, ClusterEpoch::INITIAL.get());
    write_u32(&mut unbounded, 1);
    write_u32(&mut unbounded, 1);
    write_u8(&mut unbounded, 1);
    unbounded.extend_from_slice(&[0; RUNTIME_MAP_CONTENT_DIGEST_LEN]);
    write_u64(&mut unbounded, u64::MAX);
    let mut reader = PayloadReader::new(&unbounded);
    assert!(matches!(
        read_runtime_map_status(&mut reader),
        Err(ControlPlaneError::RpcProtocol { diagnostic: message })
            if message.contains("reserved unbounded sentinel")
    ));

    let mut reconstructed = Vec::new();
    write_u64(&mut reconstructed, ClusterEpoch::INITIAL.get());
    write_u32(&mut reconstructed, 1);
    write_u32(&mut reconstructed, 1);
    write_u8(&mut reconstructed, 1);
    reconstructed.extend_from_slice(&[0; RUNTIME_MAP_CONTENT_DIGEST_LEN]);
    write_u64(&mut reconstructed, 12_345);
    write_runtime_map_freshness_proof(
        &mut reconstructed,
        &RuntimeMapFreshnessProof::Reconstructed {
            authority_incarnation: AuthorityIncarnation::INITIAL,
        },
    );
    let mut reader = PayloadReader::new(&reconstructed);
    assert!(matches!(
        read_runtime_map_status(&mut reader),
        Err(ControlPlaneError::RpcProtocol { diagnostic: message })
            if message.contains("serving-authority freshness proof")
    ));
}

struct CountingRuntimeMapSource {
    status_map: ClusterRuntimeMapSnapshot,
    full_map: ClusterRuntimeMapSnapshot,
    full_map_calls: Cell<usize>,
    status_authority_now_ms: Cell<Option<u64>>,
    full_map_authority_now_ms: Cell<Option<u64>>,
}

impl ControlPlaneRuntimeMapSource for CountingRuntimeMapSource {
    fn runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.full_map_calls.set(self.full_map_calls.get() + 1);
        self.full_map_authority_now_ms.set(Some(authority_now_ms));
        Ok(self.full_map.clone())
    }

    fn runtime_map_status(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        self.status_authority_now_ms.set(Some(authority_now_ms));
        Ok(ControlPlaneRuntimeMapStatus::from_runtime_map(
            &self.status_map,
        ))
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let runtime_map = self.runtime_map_snapshot(authority_now_ms)?;
        runtime_map
            .pg_routes()
            .iter()
            .any(|route| route.pg_id() == pg_id)
            .then_some(runtime_map)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })
    }
}

fn current_runtime_map_test_snapshot() -> ClusterRuntimeMapSnapshot {
    let now_ms = crate::clock::current_time_millis();
    let mut snapshot = runtime_map_test_snapshot_with_active_route();
    snapshot.validity = RouteMapValidity::until_ms(now_ms.saturating_add(10_000)).unwrap();
    snapshot.freshness_proof = RuntimeMapFreshnessProof::SingleAuthority {
        authority_incarnation: AuthorityIncarnation::INITIAL,
        issued_at_ms: now_ms,
    };
    snapshot.pg_routes[0].primary_lease_deadline_ms = Some(now_ms.saturating_add(10_000));
    snapshot
}

#[test]
fn runtime_map_handle_renews_matching_content_without_fetching_full_snapshot() {
    let initial_map = current_runtime_map_test_snapshot();
    let initial = crate::cluster::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &initial_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle =
        crate::cluster::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&initial));
    let mut renewed_map = initial_map.clone();
    let renewed_deadline = initial_map.valid_until_ms().unwrap().saturating_add(5_000);
    renewed_map.validity = RouteMapValidity::until_ms(renewed_deadline).unwrap();
    renewed_map.freshness_proof = RuntimeMapFreshnessProof::SingleAuthority {
        authority_incarnation: AuthorityIncarnation::INITIAL,
        issued_at_ms: crate::clock::current_time_millis(),
    };
    renewed_map.pg_routes[0].primary_lease_deadline_ms = Some(renewed_deadline);
    let source = CountingRuntimeMapSource {
        status_map: renewed_map.clone(),
        full_map: renewed_map,
        full_map_calls: Cell::new(0),
        status_authority_now_ms: Cell::new(None),
        full_map_authority_now_ms: Cell::new(None),
    };

    let refreshed = handle
        .refresh_from_control_plane_runtime_map(&source, crate::clock::current_time_millis())
        .unwrap();

    assert!(Arc::ptr_eq(&refreshed, &initial));
    assert_eq!(source.full_map_calls.get(), 0);
    assert_eq!(initial.route_map_valid_until_ms(), Some(renewed_deadline));
}

#[test]
fn runtime_map_handle_fetches_full_snapshot_for_same_epoch_content_change() {
    let initial_map = current_runtime_map_test_snapshot();
    let initial = crate::cluster::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &initial_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let handle =
        crate::cluster::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&initial));
    let mut changed_map = initial_map;
    changed_map.nodes[0].endpoint = "/tmp/argmin-node-1-replaced.sock".to_owned();
    let mut source = CountingRuntimeMapSource {
        status_map: changed_map.clone(),
        full_map: changed_map,
        full_map_calls: Cell::new(0),
        status_authority_now_ms: Cell::new(None),
        full_map_authority_now_ms: Cell::new(None),
    };

    let mut retry_clock = [2_000, 8_000].into_iter();
    let refreshed = handle
        .refresh_from_control_plane_runtime_map_with_authority_clock(&source, || {
            retry_clock
                .next()
                .expect("status and fallback clock samples")
        })
        .unwrap();
    assert!(!Arc::ptr_eq(&refreshed, &initial));
    assert_eq!(source.full_map_calls.get(), 1);
    assert_eq!(source.status_authority_now_ms.get(), Some(2_000));
    assert_eq!(source.full_map_authority_now_ms.get(), Some(8_000));

    let initial_deadline = initial.route_map_valid_until_ms().unwrap();
    let renewed_deadline = initial_deadline.saturating_add(5_000);
    source.status_map.validity = RouteMapValidity::until_ms(renewed_deadline).unwrap();
    source.status_map.freshness_proof = RuntimeMapFreshnessProof::SingleAuthority {
        authority_incarnation: AuthorityIncarnation::INITIAL,
        issued_at_ms: crate::clock::current_time_millis(),
    };
    source.status_map.pg_routes[0].primary_lease_deadline_ms = Some(renewed_deadline);
    let renewed = handle
        .refresh_from_control_plane_runtime_map(&source, crate::clock::current_time_millis())
        .unwrap();
    assert!(Arc::ptr_eq(&renewed, &refreshed));
    assert_eq!(source.full_map_calls.get(), 1);
    assert_eq!(refreshed.route_map_valid_until_ms(), Some(renewed_deadline));
    assert_eq!(initial.route_map_valid_until_ms(), Some(initial_deadline));
}

#[test]
fn route_map_validity_rejects_reserved_unbounded_deadline() {
    assert!(RouteMapValidity::until_ms(u64::MAX).is_none());
    assert!(RouteMapValidity::from_valid_until_ms(Some(u64::MAX)).is_none());

    let mut payload = Vec::new();
    write_u64(&mut payload, ClusterEpoch::INITIAL.get());
    write_option_u64(&mut payload, Some(u64::MAX));
    write_runtime_map_test_single_authority_proof(&mut payload);
    write_u32(&mut payload, 0);
    write_u32(&mut payload, 0);
    write_u32(&mut payload, 0);
    write_u32(&mut payload, 0);

    let mut reader = PayloadReader::new(&payload);
    assert!(matches!(
        read_runtime_map_snapshot(&mut reader),
        Err(ControlPlaneError::RpcProtocol { diagnostic: message })
            if message.contains("reserved unbounded sentinel")
    ));
}

#[test]
fn control_plane_rpc_rejects_unbounded_runtime_map_validity() {
    let mut payload = Vec::new();
    write_u64(&mut payload, ClusterEpoch::INITIAL.get());
    write_option_u64(&mut payload, None);
    write_runtime_map_test_single_authority_proof(&mut payload);
    write_u32(&mut payload, 0);
    write_u32(&mut payload, 0);
    write_u32(&mut payload, 0);
    write_u32(&mut payload, 0);

    let mut reader = PayloadReader::new(&payload);
    assert!(matches!(
        read_runtime_map_snapshot(&mut reader),
        Err(ControlPlaneError::RpcProtocol { diagnostic: message })
            if message.contains("validity must be bounded")
    ));
}

fn runtime_map_test_snapshot_with_transfer_route(
    include_source_route: bool,
) -> ClusterRuntimeMapSnapshot {
    let mut snapshot = runtime_map_test_snapshot_with_active_route();
    let source_epoch = snapshot.cluster_epoch;
    let destination_epoch = ClusterEpoch::new(source_epoch.get() + 1).unwrap();
    let source_route = snapshot.pg_routes[0].without_serving_authority();
    snapshot.cluster_epoch = destination_epoch;
    snapshot.validity = RouteMapValidity::until_ms(12_346).unwrap();
    snapshot.freshness_proof = RuntimeMapFreshnessProof::SingleAuthority {
        authority_incarnation: AuthorityIncarnation::INITIAL,
        issued_at_ms: 12_001,
    };
    {
        let route = &mut snapshot.pg_routes[0];
        route.cluster_epoch = destination_epoch;
        route.state = PgState::Peering;
        route.active_metadata_proof = None;
        route.primary_lease_deadline_ms = None;
        route.peering_metadata_transfer = Some(PgMetadataTransferProof::new(
            source_epoch,
            PgMetadataProof::current(1, 2, 3),
        ));
        route.peering_metadata_transfer_destination_epoch = Some(destination_epoch);
        route.peering_metadata_transfer_source_route_epoch = Some(source_epoch);
        route.peering_metadata_transfer_source_node_id = Some(NodeId::new(1));
    }
    if include_source_route {
        snapshot.historical_pg_routes.push(source_route);
        snapshot.historical_cluster_epochs.push(source_epoch);
    }
    snapshot
}

#[test]
fn metadata_transfer_source_runtime_map_preserves_only_exact_fresh_authorization() {
    let mut snapshot = runtime_map_test_snapshot_with_transfer_route(true);
    let source_epoch = snapshot.historical_pg_routes[0].cluster_epoch();
    snapshot.historical_pg_routes[0].state = PgState::Peering;
    snapshot.historical_pg_routes[0].active_metadata_proof = None;
    let expected_current_route = snapshot.pg_routes[0].clone();
    let now_ms = crate::clock::current_time_millis();
    snapshot.validity = RouteMapValidity::until_ms(now_ms + 10_000).unwrap();
    snapshot.freshness_proof = RuntimeMapFreshnessProof::SingleAuthority {
        authority_incarnation: AuthorityIncarnation::INITIAL,
        issued_at_ms: now_ms,
    };
    let expected_proof = *snapshot.freshness_proof();

    let source = snapshot
        .metadata_transfer_source_runtime_map(&expected_current_route, source_epoch, NodeId::new(1))
        .unwrap();

    assert_eq!(source.cluster_epoch(), source_epoch);
    assert_eq!(source.pg_routes().len(), 1);
    assert_eq!(source.pg_routes()[0].state(), PgState::Peering);
    assert_eq!(source.pg_routes()[0].primary_node_id(), NodeId::new(1));
    assert_eq!(*source.freshness_proof(), expected_proof);
    assert!(source
        .bind_process_local_lease_at(now_ms, 50_000)
        .unwrap()
        .is_some_and(|lease| lease.is_valid_at_monotonic(50_001)));

    assert!(
        snapshot
            .metadata_transfer_source_runtime_map(
                &expected_current_route,
                source_epoch,
                NodeId::new(2),
            )
            .is_err()
    );
    snapshot.freshness_proof = RuntimeMapFreshnessProof::Reconstructed {
        authority_incarnation: AuthorityIncarnation::INITIAL,
    };
    assert!(
        snapshot
            .metadata_transfer_source_runtime_map(
                &expected_current_route,
                source_epoch,
                NodeId::new(1),
            )
            .is_err()
    );
}

#[test]
fn metadata_transfer_source_runtime_map_accepts_fresh_current_fence() {
    let mut snapshot = runtime_map_test_snapshot_with_active_route();
    snapshot.pg_routes[0].state = PgState::Peering;
    snapshot.pg_routes[0].active_metadata_proof = None;
    snapshot.pg_routes[0].primary_lease_deadline_ms = None;
    let expected_current_route = snapshot.pg_routes[0].clone();
    snapshot.pg_routes[0].metadata_read_route = Some(PgMetadataReadRoute::new(
        NodeId::new(1),
        PgMetadataProof::current(1, 2, 3),
    ));

    let source = snapshot
        .metadata_transfer_source_runtime_map(
            &expected_current_route,
            ClusterEpoch::INITIAL,
            NodeId::new(1),
        )
        .unwrap();

    assert_eq!(source.pg_routes(), snapshot.pg_routes());
    assert_eq!(source.freshness_proof(), snapshot.freshness_proof());
}

#[test]
fn metadata_transfer_destination_runtime_map_preserves_fresh_exact_authorization() {
    let mut snapshot = runtime_map_test_snapshot_with_transfer_route(true);
    let transfer = snapshot.pg_routes[0].peering_metadata_transfer().unwrap();
    let acting_set = snapshot.pg_routes[0].acting_set().to_vec();
    let destination_epoch = snapshot.pg_routes[0]
        .peering_metadata_transfer_destination_epoch()
        .unwrap();
    let unrelated_epoch = next_epoch(snapshot.cluster_epoch()).unwrap();
    snapshot.cluster_epoch = unrelated_epoch;
    snapshot.pg_routes[0].cluster_epoch = unrelated_epoch;
    let now_ms = crate::clock::current_time_millis();
    snapshot.validity = RouteMapValidity::until_ms(now_ms + 10_000).unwrap();
    snapshot.freshness_proof = RuntimeMapFreshnessProof::SingleAuthority {
        authority_incarnation: AuthorityIncarnation::INITIAL,
        issued_at_ms: now_ms,
    };

    let destination = snapshot
        .metadata_transfer_destination_runtime_map(PgId::new(7), &acting_set, transfer)
        .unwrap();

    assert_eq!(destination.cluster_epoch(), destination_epoch);
    assert_eq!(destination.pg_routes().len(), 1);
    assert_eq!(
        destination.pg_routes()[0],
        snapshot.pg_routes()[0].with_cluster_epoch(destination_epoch)
    );
    assert!(destination
        .bind_process_local_lease_at(now_ms, 60_000)
        .unwrap()
        .is_some_and(|lease| lease.is_valid_at_monotonic(60_001)));

    snapshot.freshness_proof = RuntimeMapFreshnessProof::Reconstructed {
        authority_incarnation: AuthorityIncarnation::INITIAL,
    };
    assert!(snapshot
        .metadata_transfer_destination_runtime_map(PgId::new(7), &acting_set, transfer)
        .is_err());
}
