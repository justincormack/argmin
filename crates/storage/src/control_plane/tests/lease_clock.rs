// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn lease_grant_horizon_command_replays_and_fences_authority_rebinding() {
    let initial_authority = LeaseHorizonAuthorityBinding::new(7, Some(11));
    let replacement_authority = LeaseHorizonAuthorityBinding::new(8, Some(12));
    let initial = ClusterControlSnapshot::empty()
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: initial_authority,
            authority_now_ms: 10_000,
            horizon_duration_ms: 30_000,
        })
        .unwrap();
    assert!(initial.changed());
    assert_eq!(
        initial.response(),
        &ControlPlaneCommandResponse::EstablishLeaseGrantHorizon
    );
    let initial = initial.into_snapshot();
    assert_eq!(initial.max_committed_timestamp_ms(), Some(10_000));
    let horizon = initial.lease_grant_horizon().unwrap();
    assert_eq!(horizon.authority(), initial_authority);
    assert_eq!(horizon.grant_not_after_ms(), 40_000);

    let replay = initial
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: initial_authority,
            authority_now_ms: 10_000,
            horizon_duration_ms: 30_000,
        })
        .unwrap();
    assert!(!replay.changed());
    assert_eq!(replay.snapshot(), &initial);

    let error = initial
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: replacement_authority,
            authority_now_ms: 40_999,
            horizon_duration_ms: 30_000,
        })
        .unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::PreviousLeaseGrantHorizonStillActive {
            authority_now_ms: 40_999,
            fenced_until_ms: 41_000,
        }
    ));

    let replacement = initial
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: replacement_authority,
            authority_now_ms: 41_000,
            horizon_duration_ms: 30_000,
        })
        .unwrap()
        .into_snapshot();
    let horizon = replacement.lease_grant_horizon().unwrap();
    assert_eq!(horizon.authority(), replacement_authority);
    assert_eq!(horizon.grant_not_after_ms(), 71_000);
    assert_eq!(replacement.max_committed_timestamp_ms(), Some(41_000));
}

#[test]
fn single_authority_heartbeat_establishes_reuses_and_restores_lease_horizon() {
    let tmp = test_util::tempdir();
    let store_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&store_path);
    let mut control_plane = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    control_plane
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let authority = LeaseHorizonAuthorityBinding::new(7, None);
    let observed_epoch = control_plane.snapshot().cluster_epoch();

    let first = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, observed_epoch, 1_000),
            1_000,
            authority,
        )
        .unwrap();
    let first_horizon = control_plane.snapshot().lease_grant_horizon().unwrap();
    assert_eq!(
        first_horizon.grant_not_after_ms(),
        1_000 + CONTROL_PLANE_LEASE_GRANT_HORIZON_DURATION_MS
    );
    assert!(control_plane
        .snapshot()
        .lease_grant_horizon_covers(authority, first.lease().lease_deadline_ms()));

    let current_epoch = control_plane.snapshot().cluster_epoch();
    let acknowledged = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, current_epoch, 12_000),
            12_000,
            authority,
        )
        .unwrap();
    assert_eq!(acknowledged.lease().lease_deadline_ms(), 12_100);
    assert_eq!(
        control_plane.snapshot().lease_grant_horizon(),
        Some(first_horizon),
        "a covered heartbeat must not extend the durable horizon"
    );
    let durable_after_epoch_acknowledgement = std::fs::read(&store_path).unwrap();
    let persisted_after_epoch_acknowledgement = store.load().unwrap().unwrap();
    let journal_offset_before_volatile_renewal = store.journal.clean_len().unwrap();

    let renewed = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, current_epoch, 12_001),
            12_001,
            authority,
        )
        .unwrap();
    assert_eq!(
        std::fs::read(&store_path).unwrap(),
        durable_after_epoch_acknowledgement,
        "an unchanged heartbeat covered by the durable horizon must not rewrite state"
    );
    assert_eq!(
        store.journal.clean_len().unwrap(),
        journal_offset_before_volatile_renewal,
        "an unchanged heartbeat covered by the durable horizon must not append a journal record"
    );
    assert_eq!(
        control_plane
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        Some(renewed.lease().lease_deadline_ms()),
        "the volatile lease renewal must still be visible to live serving checks"
    );
    assert_eq!(
        store.load().unwrap().unwrap().lease_grant_horizon(),
        Some(first_horizon),
        "the heartbeat and horizon must have identical restart state"
    );
    assert_eq!(
        store
            .load()
            .unwrap()
            .unwrap()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        persisted_after_epoch_acknowledgement
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        "volatile renewal must not alter the restart lease"
    );

    let before_replacement = control_plane.snapshot().clone();
    let replacement_authority = LeaseHorizonAuthorityBinding::new(8, None);
    let error = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, current_epoch, 12_002),
            12_002,
            replacement_authority,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::PreviousLeaseGrantHorizonStillActive { .. }
    ));
    assert_eq!(control_plane.snapshot(), &before_replacement);
}

#[test]
fn single_authority_promotes_volatile_lease_before_unrelated_durable_command() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut control_plane = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    control_plane
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let authority = LeaseHorizonAuthorityBinding::new(7, None);
    let initial_epoch = control_plane.snapshot().cluster_epoch();
    control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, initial_epoch, 1_000),
            1_000,
            authority,
        )
        .unwrap();
    let current_epoch = control_plane.snapshot().cluster_epoch();
    control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, current_epoch, 12_000),
            12_000,
            authority,
        )
        .unwrap();
    let renewed = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, current_epoch, 12_001),
            12_001,
            authority,
        )
        .unwrap();
    let acknowledged_deadline = renewed.lease().lease_deadline_ms();

    control_plane
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();

    assert_eq!(
        store
            .load()
            .unwrap()
            .unwrap()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        Some(acknowledged_deadline),
        "the unrelated command must first promote the acknowledged volatile lease"
    );
    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        Some(acknowledged_deadline)
    );
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(2))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
}

#[test]
fn single_authority_promotes_volatile_lease_before_semantic_heartbeat() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut control_plane = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    control_plane
        .bootstrap_initial_cluster_map(
            vec![(NodeId::new(1), "/tmp/node-1.sock".to_owned())],
            vec![PgId::new(7)],
        )
        .unwrap();
    let authority = LeaseHorizonAuthorityBinding::new(7, None);
    let initial_epoch = control_plane.snapshot().cluster_epoch();
    control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, initial_epoch, 1_000),
            1_000,
            authority,
        )
        .unwrap();
    let current_epoch = control_plane.snapshot().cluster_epoch();
    let volatile = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, current_epoch, 12_000),
            12_000,
            authority,
        )
        .unwrap();
    let acknowledged_deadline = volatile.lease().lease_deadline_ms();
    let mut semantic = heartbeat(1, current_epoch, 12_001);
    semantic.requested_lease_duration_ms = 1;
    semantic.endpoint = "/tmp/node-1-moved.sock".to_owned();

    let refreshed = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(semantic, 12_001, authority)
        .unwrap();

    assert_eq!(refreshed.lease().lease_deadline_ms(), acknowledged_deadline);
    let durable = store.load().unwrap().unwrap();
    assert_eq!(
        durable.node(NodeId::new(1)).unwrap().lease_deadline_ms(),
        Some(acknowledged_deadline)
    );
    assert_eq!(
        durable.node(NodeId::new(1)).unwrap().endpoint(),
        "/tmp/node-1-moved.sock"
    );
}

#[test]
fn single_authority_exact_heartbeat_retransmission_does_not_append_journal() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut control_plane = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    control_plane
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    heartbeat_until_serving(&mut control_plane, 1, 1_000);
    let heartbeat_at_ms = 2_000;
    let heartbeat = heartbeat_from_record(
        &control_plane,
        1,
        control_plane.snapshot().cluster_epoch(),
        heartbeat_at_ms,
    );
    let first = control_plane
        .heartbeat(heartbeat.clone(), heartbeat_at_ms)
        .unwrap();
    let snapshot_after_first = control_plane.snapshot().clone();
    let journal_offset_after_first = store.journal.clean_len().unwrap();

    let retry = control_plane.heartbeat(heartbeat, heartbeat_at_ms).unwrap();

    assert_eq!(retry.lease_deadline_ms(), first.lease_deadline_ms());
    assert_eq!(control_plane.snapshot(), &snapshot_after_first);
    assert_eq!(
        store.journal.clean_len().unwrap(),
        journal_offset_after_first,
        "an exact heartbeat retry must not append a non-mutating journal record"
    );
}

#[test]
fn rejected_horizon_enabled_heartbeat_leaves_durable_state_unchanged() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut control_plane = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    control_plane
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let before = control_plane.snapshot().clone();
    let future_epoch = ClusterEpoch::new(before.cluster_epoch().get() + 1).unwrap();

    let error = control_plane
        .refresh_node_heartbeat_with_lease_horizon_authority(
            heartbeat(1, future_epoch, 1_000),
            1_000,
            LeaseHorizonAuthorityBinding::new(7, None),
        )
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::FutureNodeObservedEpoch { .. }
    ));
    assert_eq!(control_plane.snapshot(), &before);
    assert_eq!(store.load().unwrap(), Some(before));
}

#[test]
fn lease_grant_horizon_command_rejects_invalid_duration_and_timestamp() {
    let authority = LeaseHorizonAuthorityBinding::new(1, None);
    let baseline = ClusterControlSnapshot::empty();
    for duration_ms in [0, MAX_LEASE_GRANT_HORIZON_MS + 1] {
        assert!(matches!(
            baseline.apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
                authority,
                authority_now_ms: 1_000,
                horizon_duration_ms: duration_ms,
            }),
            Err(ControlPlaneError::InvalidLeaseGrantHorizonDuration { .. })
        ));
    }
    assert!(matches!(
        baseline.apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority,
            authority_now_ms: u64::MAX,
            horizon_duration_ms: 1,
        }),
        Err(ControlPlaneError::LeaseGrantHorizonTimestampOverflow { .. })
    ));

    let established = baseline
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority,
            authority_now_ms: 1_000,
            horizon_duration_ms: 10_000,
        })
        .unwrap()
        .into_snapshot();
    assert!(matches!(
        established.apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority,
            authority_now_ms: 999,
            horizon_duration_ms: 10_000,
        }),
        Err(ControlPlaneError::CommittedTimestampRegression { .. })
    ));
}

#[test]
fn lease_grant_horizon_round_trips_canonical_snapshot_state() {
    let snapshot = ClusterControlSnapshot::empty()
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: LeaseHorizonAuthorityBinding::new(9, Some(13)),
            authority_now_ms: 5_000,
            horizon_duration_ms: 30_000,
        })
        .unwrap()
        .into_snapshot();
    let encoded = format_snapshot(&snapshot);
    assert!(encoded.contains("lease_grant_horizon=9,13,35000\n"));
    assert_eq!(parse_snapshot(&encoded).unwrap(), snapshot);

    for invalid in [
        encoded.replace("9,13,35000", "0,13,35000"),
        encoded.replace("9,13,35000", "9,0,35000"),
        encoded.replace("9,13,35000", "9,13,0"),
        encoded.replace(
            "max_committed_timestamp_ms=5000",
            "max_committed_timestamp_ms=-",
        ),
        encoded.replace("9,13,35000", "9,13,65001"),
    ] {
        assert!(parse_snapshot(&invalid).is_err());
    }
}

#[test]
fn replicated_snapshot_install_rejects_impossible_lease_grant_horizon() {
    for invalid_snapshot in [
        ClusterControlSnapshot::test_invalid_lease_grant_horizon(None, 30_000),
        ClusterControlSnapshot::test_invalid_lease_grant_horizon(
            Some(5_000),
            5_000 + MAX_LEASE_GRANT_HORIZON_MS + 1,
        ),
    ] {
        let payload =
            crate::control_plane_command::encode_control_plane_snapshot(&invalid_snapshot).unwrap();
        let mut state_machine =
            crate::control_plane_command::ReplicatedControlPlaneStateMachine::empty();
        let before = state_machine.clone();

        assert!(state_machine
            .install_snapshot_artifact(
                crate::control_plane_command::ControlPlaneSnapshotArtifact::new(None, payload)
            )
            .is_err());
        assert_eq!(state_machine, before);
    }
}

#[test]
fn single_authority_linearized_command_sink_persists_submitted_command() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(state_path.clone());
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();

    let applied = ControlPlaneLinearizedCommandSink::submit_control_plane_command(
        &mut authority,
        ControlPlaneCommand::SetNodeMembership {
            node_id: NodeId::new(1),
            membership: NodeMembershipState::Active,
        },
    )
    .unwrap();

    assert!(applied.changed());
    assert_eq!(
        applied.response(),
        &ControlPlaneCommandResponse::SetNodeMembership
    );
    assert_eq!(applied.snapshot(), authority.snapshot());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
    assert_eq!(store.load().unwrap().unwrap(), *authority.snapshot());
    assert!(std::fs::metadata(state_path).unwrap().is_file());
}

#[test]
fn single_authority_linearized_runtime_map_read_carries_freshness_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let authority = SingleAuthorityControlPlane::open(store).unwrap();

    let runtime_map =
        ControlPlaneLinearizedRuntimeMapSource::linearized_runtime_map_snapshot(&authority, 12_345)
            .unwrap();

    assert_eq!(
        runtime_map.freshness_proof(),
        &RuntimeMapFreshnessProof::SingleAuthority {
            authority_incarnation: authority.snapshot().authority_incarnation(),
            issued_at_ms: 12_345,
        }
    );
}

#[test]
fn record_node_heartbeat_command_records_current_epoch_pg_observation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
        .unwrap();

    let observed_epoch = authority.snapshot().cluster_epoch();
    let metadata_proof = PgMetadataProof {
        applied_log_index: 7,
        applied_log_hash: 8,
        state_digest: 9,
    };
    let mut heartbeat = heartbeat_from_record(&authority, 1, observed_epoch, 2_000);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(7),
        state: PgState::Peering,
        metadata_proof,
        pending_metadata_command: None,
    }];
    let before = authority.snapshot().clone();
    let applied = before
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: 2_000,
            lease_deadline_ms: 2_100,
            lease_horizon_authority: None,
        })
        .unwrap();

    assert!(applied.changed());
    assert_eq!(
        applied.response(),
        &ControlPlaneCommandResponse::RecordNodeHeartbeat
    );
    let snapshot = applied.snapshot();
    assert_eq!(snapshot.cluster_epoch(), before.cluster_epoch());
    assert_eq!(snapshot.max_committed_timestamp_ms(), Some(2_000));
    let record = snapshot.node(NodeId::new(1)).unwrap();
    assert_eq!(record.last_observed_epoch(), Some(observed_epoch));
    assert_eq!(record.last_heartbeat_ms(), Some(2_000));
    assert_eq!(record.lease_deadline_ms(), Some(2_100));
    let observation = record.pg_observation(PgId::new(7)).unwrap();
    assert_eq!(observation.state(), PgState::Peering);
    assert_eq!(observation.observed_epoch(), observed_epoch);
    assert_eq!(observation.observed_at_ms(), 2_000);
    assert_eq!(observation.metadata_proof(), metadata_proof);
    assert!(!observation.has_pending_metadata_command());
}

#[test]
fn record_node_heartbeat_command_stale_epoch_clears_pg_observations() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(8), vec![NodeId::new(1)])
        .unwrap();

    let current_epoch = authority.snapshot().cluster_epoch();
    let stale_epoch = ClusterEpoch::new(current_epoch.get() - 1).unwrap();
    let mut heartbeat = heartbeat_from_record(&authority, 1, stale_epoch, 2_000);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(8),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: None,
    }];
    let before = authority.snapshot().clone();
    let applied = before
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: 2_000,
            lease_deadline_ms: 2_100,
            lease_horizon_authority: None,
        })
        .unwrap();

    let snapshot = applied.snapshot();
    assert!(applied.changed());
    assert_eq!(snapshot.cluster_epoch(), before.cluster_epoch());
    assert_eq!(snapshot.max_committed_timestamp_ms(), Some(2_000));
    let record = snapshot.node(NodeId::new(1)).unwrap();
    assert_eq!(record.last_observed_epoch(), Some(stale_epoch));
    assert_eq!(record.last_heartbeat_ms(), Some(2_000));
    assert_eq!(record.lease_deadline_ms(), Some(2_100));
    assert!(record.pg_observation(PgId::new(8)).is_none());
}

#[test]
fn record_node_heartbeat_command_rejects_future_history_route_without_mutation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let current_epoch = authority.snapshot().cluster_epoch();
    let future_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut heartbeat = heartbeat_from_record(&authority, 1, current_epoch, 2_000);
    heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            future_epoch,
            PgId::new(1),
        )]);
    let before = authority.snapshot().clone();
    let error = before
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: 2_000,
            lease_deadline_ms: 2_100,
            lease_horizon_authority: None,
        })
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::StorageClusterMapHistoryRouteInFuture {
            node_id: 1,
            route_epoch,
            validation_epoch,
            ..
        } if route_epoch == future_epoch && validation_epoch == current_epoch
    ));
    assert_eq!(
        before
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        None
    );
    assert_eq!(before.cluster_epoch(), current_epoch);
}

#[test]
fn record_node_heartbeat_command_validates_committed_lease_deadline() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let current_epoch = authority.snapshot().cluster_epoch();
    let before = authority.snapshot().clone();
    let mut zero_duration = heartbeat_from_record(&authority, 1, current_epoch, 2_000);
    zero_duration.requested_lease_duration_ms = 0;
    assert!(matches!(
        before.apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat: zero_duration,
            heartbeat_at_ms: 2_000,
            lease_deadline_ms: 2_000,
            lease_horizon_authority: None,
        },),
        Err(ControlPlaneError::InvalidLeaseDuration)
    ));

    let mut overlong_duration = heartbeat_from_record(&authority, 1, current_epoch, 2_001);
    overlong_duration.requested_lease_duration_ms = MAX_HEARTBEAT_LEASE_MS + 1;
    assert!(matches!(
        before.apply_control_plane_command(
            ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: overlong_duration,
                heartbeat_at_ms: 2_001,
                lease_deadline_ms: 2_001 + MAX_HEARTBEAT_LEASE_MS + 1,
                lease_horizon_authority: None,
            },
        ),
        Err(ControlPlaneError::LeaseDurationTooLong {
            requested_ms,
            max_ms,
        }) if requested_ms == MAX_HEARTBEAT_LEASE_MS + 1
            && max_ms == MAX_HEARTBEAT_LEASE_MS
    ));

    let heartbeat = heartbeat_from_record(&authority, 1, current_epoch, 2_001);
    assert!(matches!(
        before.apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: 2_001,
            lease_deadline_ms: 2_200,
            lease_horizon_authority: None,
        },),
        Err(ControlPlaneError::LeaseDeadlineMismatch {
            node_id: 1,
            heartbeat_at_ms: 2_001,
            requested_ms: 100,
            expected_deadline_ms: 2_101,
            actual_deadline_ms: 2_200,
        })
    ));
}

#[test]
fn record_node_heartbeat_command_rejects_committed_timestamp_regression() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let before = authority.snapshot().clone();
    let heartbeat = heartbeat_from_record(&authority, 1, before.cluster_epoch(), 999);
    let error = before
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: 999,
            lease_deadline_ms: 1_099,
            lease_horizon_authority: None,
        })
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommittedTimestampRegression {
            timestamp_ms: 999,
            max_committed_timestamp_ms: 1_001,
        }
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn record_node_heartbeat_command_accepts_elapsed_forward_progress() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let before = authority.snapshot().clone();
    let heartbeat_at_ms = 1_001 + CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 1;
    let heartbeat = heartbeat_from_record(&authority, 1, before.cluster_epoch(), heartbeat_at_ms);
    let applied = before
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms,
            lease_deadline_ms: heartbeat_at_ms + 100,
            lease_horizon_authority: None,
        })
        .unwrap();

    assert_eq!(
        applied.snapshot().max_committed_timestamp_ms(),
        Some(heartbeat_at_ms)
    );
    assert_eq!(
        applied
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        Some(heartbeat_at_ms + 100)
    );
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn record_node_heartbeat_command_rejects_lease_deadline_regression() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let before = authority.snapshot().clone();
    let mut heartbeat = heartbeat_from_record(&authority, 1, before.cluster_epoch(), 1_001);
    heartbeat.requested_lease_duration_ms = 50;
    let error = before
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: 1_001,
            lease_deadline_ms: 1_051,
            lease_horizon_authority: None,
        })
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::NodeLeaseDeadlineRegression {
            node_id: 1,
            current_lease_deadline_ms: 1_101,
            requested_lease_deadline_ms: 1_051,
        }
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn expire_heartbeat_leases_command_replays_with_committed_expiry_time() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    assert!(heartbeat_until_serving(&mut authority, 2, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(9), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_observation(
            &mut authority,
            node_id,
            9,
            PgState::Peering,
            1_010 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(9),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            1_020,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 9, PgState::Active, 1_030);

    let before = authority.snapshot().clone();
    let not_yet_expired = before
        .apply_control_plane_command(ControlPlaneCommand::ExpireHeartbeatLeases {
            expire_at_ms: 1_099,
        })
        .unwrap();
    assert!(not_yet_expired.changed());
    assert_eq!(
        not_yet_expired.response(),
        &ControlPlaneCommandResponse::ExpireHeartbeatLeases {
            expired_nodes: Vec::new(),
            peering_pgs: Vec::new(),
        }
    );
    let mut expected_not_yet_expired = before.clone();
    expected_not_yet_expired.record_committed_timestamp(1_099);
    assert_eq!(not_yet_expired.snapshot(), &expected_not_yet_expired);

    let applied = before
        .apply_control_plane_command(ControlPlaneCommand::ExpireHeartbeatLeases {
            expire_at_ms: 1_130,
        })
        .unwrap();
    assert!(applied.changed());
    assert_eq!(
        applied.response(),
        &ControlPlaneCommandResponse::ExpireHeartbeatLeases {
            expired_nodes: vec![NodeId::new(1), NodeId::new(2)],
            peering_pgs: vec![PgId::new(9)],
        }
    );
    assert_eq!(
        applied
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .availability(),
        NodeAvailabilityState::Unavailable
    );
    assert_eq!(
        applied
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        None
    );
    assert_eq!(
        applied.snapshot().pg(PgId::new(9)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        applied.snapshot().cluster_epoch(),
        ClusterEpoch::new(before.cluster_epoch().get() + 1).unwrap()
    );
    assert_eq!(applied.snapshot().max_committed_timestamp_ms(), Some(1_130));
}

#[test]
fn targeted_heartbeat_expiry_does_not_expire_unlisted_volatile_renewals() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    assert!(heartbeat_until_serving(&mut authority, 2, 1_000).serving());

    let horizon_authority = LeaseHorizonAuthorityBinding::new(7, Some(2));
    let authority_now_ms = authority.snapshot().max_committed_timestamp_ms().unwrap();
    let with_horizon = authority
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: horizon_authority,
            authority_now_ms,
            horizon_duration_ms: CONTROL_PLANE_LEASE_GRANT_HORIZON_DURATION_MS,
        })
        .unwrap()
        .into_snapshot();
    let node_1_deadline = with_horizon
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    let node_2_deadline = with_horizon
        .node(NodeId::new(2))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    let expire_at_ms = node_1_deadline.max(node_2_deadline);

    let applied = with_horizon
        .apply_control_plane_command(ControlPlaneCommand::ExpireNodeHeartbeatLeases {
            authority: horizon_authority,
            expire_at_ms,
            expired: vec![ExpiredNodeHeartbeatLease {
                node_id: NodeId::new(1),
                lease_deadline_ms: node_1_deadline,
            }],
        })
        .unwrap();

    assert_eq!(
        applied.response(),
        &ControlPlaneCommandResponse::ExpireHeartbeatLeases {
            expired_nodes: vec![NodeId::new(1)],
            peering_pgs: Vec::new(),
        }
    );
    assert_eq!(
        applied
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .observed_availability(),
        NodeAvailabilityState::Unavailable
    );
    let unlisted = applied.snapshot().node(NodeId::new(2)).unwrap();
    assert_eq!(
        unlisted.observed_availability(),
        NodeAvailabilityState::Healthy,
        "a durable deadline that looks expired must not override a newer volatile grant"
    );
    assert_eq!(unlisted.lease_deadline_ms(), Some(node_2_deadline));
}

#[test]
fn targeted_heartbeat_expiry_transitions_horizon_after_successor_fence() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let previous_authority = LeaseHorizonAuthorityBinding::new(7, Some(1));
    let successor_authority = LeaseHorizonAuthorityBinding::new(8, Some(2));
    let authority_now_ms = authority.snapshot().max_committed_timestamp_ms().unwrap();
    let baseline = authority
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority: previous_authority,
            authority_now_ms,
            horizon_duration_ms: CONTROL_PLANE_LEASE_GRANT_HORIZON_DURATION_MS,
        })
        .unwrap()
        .into_snapshot();
    let lease_deadline_ms = baseline
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    let successor_fence_ms = baseline.lease_grant_horizon().unwrap().grant_not_after_ms()
        + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS;
    let command = |expire_at_ms| ControlPlaneCommand::ExpireNodeHeartbeatLeases {
        authority: successor_authority,
        expire_at_ms,
        expired: vec![ExpiredNodeHeartbeatLease {
            node_id: NodeId::new(1),
            lease_deadline_ms,
        }],
    };

    assert!(matches!(
        baseline.apply_control_plane_command(command(successor_fence_ms - 1)),
        Err(ControlPlaneError::PreviousLeaseGrantHorizonStillActive {
            authority_now_ms,
            fenced_until_ms,
        }) if authority_now_ms == successor_fence_ms - 1
            && fenced_until_ms == successor_fence_ms
    ));

    let applied = baseline
        .apply_control_plane_command(command(successor_fence_ms))
        .unwrap();
    assert_eq!(
        applied.snapshot().lease_grant_horizon_authority(),
        Some(successor_authority)
    );
    assert_eq!(
        applied
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .observed_availability(),
        NodeAvailabilityState::Unavailable
    );
}

#[test]
fn expire_heartbeat_leases_command_rejects_committed_timestamp_regression() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let before = authority.snapshot().clone();
    let error = before
        .apply_control_plane_command(ControlPlaneCommand::ExpireHeartbeatLeases {
            expire_at_ms: 999,
        })
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommittedTimestampRegression {
            timestamp_ms: 999,
            max_committed_timestamp_ms: 1_001,
        }
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn expire_heartbeat_leases_command_accepts_elapsed_forward_progress() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority.expire_heartbeat_leases(1_101).unwrap();

    let before = authority.snapshot().clone();
    let expire_at_ms = 1_101 + CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 1;
    let applied = before
        .apply_control_plane_command(ControlPlaneCommand::ExpireHeartbeatLeases { expire_at_ms })
        .unwrap();

    assert_eq!(
        applied.snapshot().max_committed_timestamp_ms(),
        Some(expire_at_ms)
    );
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn authority_clock_rejects_restart_discontinuity_before_expiry() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    drop(authority);
    let store = FileControlPlaneStore::new(&state_path);
    let authority = SingleAuthorityControlPlane::open(store).unwrap();
    let before = authority.snapshot().clone();
    let far_future_now_ms = 1_001 + CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 123;
    let mut clock = ControlPlaneAuthorityClock::new(
        before.max_committed_timestamp_ms(),
        far_future_now_ms,
        Some(20),
    )
    .unwrap();
    assert!(matches!(
        clock.effective_now_ms(far_future_now_ms, Some(20)),
        Err(ControlPlaneError::AuthorityClockNotEstablished {
            blocked_reason: Some(
                ControlPlaneAuthorityClockBlockedReason::InitialTimestampDiscontinuity
            ),
        })
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn authority_clock_accepts_long_restart_when_wall_and_health_elapsed_match() {
    let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft("test-cluster", 1);
    let checkpoint =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 1, Some(1_000), 1_000, 50);
    let mut clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        3_601_000,
        Some(3_600_050),
        Some(checkpoint),
    )
    .unwrap();

    assert_eq!(
        clock.effective_now_ms(3_601_001, Some(3_600_051)).unwrap(),
        3_601_001
    );
}

#[test]
fn authority_clock_restart_checkpoint_rejects_elapsed_clock_divergence() {
    let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft("test-cluster", 1);
    let checkpoint =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 1, Some(1_000), 1_000, 50);
    let mut clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        3_601_000 + CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 1,
        Some(3_600_050),
        Some(checkpoint),
    )
    .unwrap();

    assert!(matches!(
        clock.effective_now_ms(
            3_601_000 + CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 1,
            Some(3_600_050),
        ),
        Err(ControlPlaneError::AuthorityClockNotEstablished {
            blocked_reason: Some(
                ControlPlaneAuthorityClockBlockedReason::InitialTimestampDiscontinuity
            ),
        })
    ));
}

#[test]
fn authority_clock_restart_checkpoint_rejects_health_regression_and_wrong_high_water() {
    let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft("test-cluster", 1);
    let checkpoint =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 1, Some(1_000), 1_000, 500);
    let health_regressed = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        1_100,
        Some(499),
        Some(checkpoint),
    )
    .unwrap();
    assert!(!health_regressed
        .status(ControlPlaneAuthorityClockContext::new(
            Some(1_000),
            None,
            true,
            true,
        ))
        .established());

    let checkpoint_ahead =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 1, Some(1_002), 1_002, 500);
    let wrong_high_water = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_001),
        1_102,
        Some(600),
        Some(checkpoint_ahead),
    )
    .unwrap();
    assert!(!wrong_high_water
        .status(ControlPlaneAuthorityClockContext::new(
            Some(1_001),
            None,
            true,
            true,
        ))
        .established());
}

#[test]
fn durable_authority_clock_requires_checkpoint_for_restored_timestamp_state() {
    let clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        1_000,
        Some(500),
        None,
    )
    .unwrap();

    assert!(!clock
        .status(ControlPlaneAuthorityClockContext::new(
            Some(1_000),
            None,
            true,
            true,
        ))
        .established());
}

#[test]
fn restarted_authority_clock_cannot_reuse_restored_lease_horizon_generation() {
    let previous_authority = LeaseHorizonAuthorityBinding::new(7, Some(11));
    let mut clock = ControlPlaneAuthorityClock::new(None, 1_000, Some(50)).unwrap();

    clock
        .advance_generation_past_lease_horizon(previous_authority)
        .unwrap();
    clock.bind_initial_raft_leadership_term(Some(11));

    let restarted_authority = clock.lease_horizon_authority_binding(Some(11)).unwrap();
    assert_eq!(restarted_authority.clock_generation(), 8);
    assert_ne!(restarted_authority, previous_authority);
}

#[test]
fn checkpoint_proven_single_authority_restart_resumes_lease_horizon_generation() {
    let binding = ControlPlaneAuthorityClockCheckpointBinding([0x41; 32]);
    let checkpoint =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 7, Some(1_000), 1_000, 50);
    let previous_authority = LeaseHorizonAuthorityBinding::new(7, None);
    let mut clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        1_001,
        Some(51),
        Some(checkpoint),
    )
    .unwrap();

    assert!(clock.resume_single_authority_lease_horizon_generation(previous_authority));
    assert_eq!(
        clock.lease_horizon_authority_binding(None).unwrap(),
        previous_authority
    );
    assert!(!clock.resume_single_authority_lease_horizon_generation(previous_authority));
}

#[test]
fn single_authority_horizon_resume_requires_checkpoint_and_no_raft_term() {
    let previous_authority = LeaseHorizonAuthorityBinding::new(7, None);
    let mut unproven =
        ControlPlaneAuthorityClock::new_with_restart_checkpoint(Some(1_000), 1_001, Some(51), None)
            .unwrap();
    assert!(!unproven.resume_single_authority_lease_horizon_generation(previous_authority));

    let binding = ControlPlaneAuthorityClockCheckpointBinding([0x42; 32]);
    let checkpoint =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 1, Some(1_000), 1_000, 50);
    let mut raft_clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        1_001,
        Some(51),
        Some(checkpoint),
    )
    .unwrap();
    assert!(
        !raft_clock.resume_single_authority_lease_horizon_generation(
            LeaseHorizonAuthorityBinding::new(7, Some(11))
        )
    );
}

#[test]
fn recovered_clock_checkpoint_cannot_resume_an_older_horizon_generation() {
    let binding = ControlPlaneAuthorityClockCheckpointBinding([0x43; 32]);
    let previous_authority = LeaseHorizonAuthorityBinding::new(7, None);
    let context = ControlPlaneAuthorityClockContext::new(Some(1_000), None, true, true);
    let mut recovered =
        ControlPlaneAuthorityClock::new_with_restart_checkpoint(Some(1_000), 1_000, Some(50), None)
            .unwrap();
    recovered
        .advance_generation_past_lease_horizon(previous_authority)
        .unwrap();
    assert_eq!(recovered.status(context).generation(), 8);
    recovered
        .reestablish(8, Some(1_000), None, context, 1_000, Some(50))
        .unwrap();
    let checkpoint = validated_authority_clock_restart_checkpoint(
        binding,
        Some(1_000),
        &mut recovered,
        1_001,
        Some(51),
    )
    .unwrap();
    assert_eq!(checkpoint.authority_generation(), 9);

    let mut restarted = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
        Some(1_000),
        1_002,
        Some(52),
        Some(checkpoint),
    )
    .unwrap();
    assert_eq!(restarted.status(context).generation(), 9);
    assert!(!restarted.resume_single_authority_lease_horizon_generation(previous_authority));
    restarted
        .advance_generation_past_lease_horizon(previous_authority)
        .unwrap();
    assert_eq!(
        restarted
            .lease_horizon_authority_binding(None)
            .unwrap()
            .clock_generation(),
        9
    );
}

#[test]
fn file_backed_authority_does_not_replace_blocked_restart_checkpoint() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    crate::clock::with_time_override(1_000, || {
        let store = FileControlPlaneStore::new(&state_path);
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    });
    let binding = FileControlPlaneStore::new(&state_path)
        .load_or_create_authority_clock_checkpoint_binding()
        .unwrap();
    let invalid_checkpoint =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 1, Some(1_001), 1_000, 2_000);
    std::fs::write(
        authority_clock_restart_checkpoint_path(&state_path),
        invalid_checkpoint.encode(),
    )
    .unwrap();

    for _ in 0..2 {
        crate::clock::with_time_override(5_000, || {
            let store = FileControlPlaneStore::new(&state_path);
            let authority = SingleAuthorityControlPlane::open(store).unwrap();
            assert_eq!(
                load_authority_clock_restart_checkpoint(&state_path, binding).unwrap(),
                Some(invalid_checkpoint)
            );
            let clock = ControlPlaneAuthorityClock::new_from_process_clock_with_restart_checkpoint(
                authority.snapshot().max_committed_timestamp_ms(),
                Some(invalid_checkpoint),
            )
            .unwrap();
            assert!(!clock
                .status(ControlPlaneAuthorityClockContext::new(
                    authority.snapshot().max_committed_timestamp_ms(),
                    None,
                    true,
                    true,
                ))
                .established());
        });
    }
}

#[test]
fn authority_clock_restart_checkpoint_file_round_trips_and_rejects_corruption() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let binding = FileControlPlaneStore::new(&state_path)
        .load_or_create_authority_clock_checkpoint_binding()
        .unwrap();
    let stored = crate::clock::with_time_override(5_000, || {
        store_authority_clock_restart_checkpoint(&state_path, binding, 9, Some(4_999)).unwrap()
    });
    assert_eq!(stored.authority_generation(), 9);
    assert_eq!(
        load_authority_clock_restart_checkpoint(&state_path, binding).unwrap(),
        Some(stored)
    );

    let checkpoint_path = authority_clock_restart_checkpoint_path(&state_path);
    for version in [
        CONTROL_PLANE_CLOCK_CHECKPOINT_VERSION - 1,
        CONTROL_PLANE_CLOCK_CHECKPOINT_VERSION + 1,
    ] {
        let mut bytes = stored.encode();
        let version_offset = CONTROL_PLANE_CLOCK_CHECKPOINT_MAGIC.len();
        bytes[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut bytes);
        std::fs::write(&checkpoint_path, bytes).unwrap();
        assert!(matches!(
            load_authority_clock_restart_checkpoint(&state_path, binding),
            Err(ControlPlaneError::AuthorityClockCheckpoint { message })
                if message == format!("unsupported checkpoint version {version}")
        ));
    }

    let zero_generation =
        ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 0, Some(4_999), 5_000, 5_000);
    std::fs::write(&checkpoint_path, zero_generation.encode()).unwrap();
    assert!(matches!(
        load_authority_clock_restart_checkpoint(&state_path, binding),
        Err(ControlPlaneError::AuthorityClockCheckpoint { ref message })
            if message.contains("generation must be nonzero")
    ));

    let mut bytes = stored.encode();
    bytes[CONTROL_PLANE_CLOCK_CHECKPOINT_MAGIC.len() + 2] ^= 1;
    std::fs::write(checkpoint_path, bytes).unwrap();
    assert!(matches!(
        load_authority_clock_restart_checkpoint(&state_path, binding),
        Err(ControlPlaneError::AuthorityClockCheckpoint { ref message })
            if message.contains("checksum mismatch")
    ));
}

#[test]
fn authority_clock_restart_checkpoint_rejects_wrong_raft_cluster_and_node() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft("cluster-a", 1);
    crate::clock::with_time_override(5_000, || {
        store_authority_clock_restart_checkpoint(&state_path, binding, 1, Some(4_999)).unwrap();
    });

    for wrong_binding in [
        ControlPlaneAuthorityClockCheckpointBinding::for_raft("cluster-b", 1),
        ControlPlaneAuthorityClockCheckpointBinding::for_raft("cluster-a", 2),
    ] {
        assert!(matches!(
            load_authority_clock_restart_checkpoint(&state_path, wrong_binding),
            Err(ControlPlaneError::AuthorityClockCheckpoint { ref message })
                if message.contains("identity does not match")
        ));
    }
}

#[test]
fn authority_clock_restart_checkpoint_rejects_wrong_single_authority_identity() {
    let tmp = test_util::tempdir();
    let first_path = tmp.path().join("first.state");
    let second_path = tmp.path().join("second.state");
    let first_binding = FileControlPlaneStore::new(&first_path)
        .load_or_create_authority_clock_checkpoint_binding()
        .unwrap();
    let second_binding = FileControlPlaneStore::new(&second_path)
        .load_or_create_authority_clock_checkpoint_binding()
        .unwrap();
    assert_ne!(first_binding, second_binding);
    crate::clock::with_time_override(5_000, || {
        store_authority_clock_restart_checkpoint(&first_path, first_binding, 1, Some(4_999))
            .unwrap();
    });

    assert!(matches!(
        load_authority_clock_restart_checkpoint(&first_path, second_binding),
        Err(ControlPlaneError::AuthorityClockCheckpoint { ref message })
            if message.contains("identity does not match")
    ));
}

#[test]
fn authority_clock_restart_checkpoint_rejects_oversized_sparse_file_before_reading() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft("cluster-a", 1);
    crate::clock::with_time_override(5_000, || {
        store_authority_clock_restart_checkpoint(&state_path, binding, 1, Some(4_999)).unwrap();
    });
    std::fs::OpenOptions::new()
        .write(true)
        .open(authority_clock_restart_checkpoint_path(&state_path))
        .unwrap()
        .set_len(1 << 30)
        .unwrap();

    assert!(matches!(
        load_authority_clock_restart_checkpoint(&state_path, binding),
        Err(ControlPlaneError::AuthorityClockCheckpoint { ref message })
            if message.contains("does not match required fixed length")
    ));
}

#[test]
fn file_backed_authority_restarts_after_long_elapsed_downtime_without_recovery() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    crate::clock::with_time_override(1_001, || {
        let store = FileControlPlaneStore::new(&state_path);
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    });

    let store = FileControlPlaneStore::new(&state_path);
    let binding = store
        .load_or_create_authority_clock_checkpoint_binding()
        .unwrap();
    let checkpoint = store
        .load_authority_clock_restart_checkpoint(binding)
        .unwrap()
        .expect("file-backed state should have a clock checkpoint");
    crate::clock::with_time_override(3_601_001, || {
        let authority = SingleAuthorityControlPlane::open(store).unwrap();
        let clock = ControlPlaneAuthorityClock::new_from_process_clock_with_restart_checkpoint(
            authority.snapshot().max_committed_timestamp_ms(),
            Some(checkpoint),
        )
        .unwrap();
        assert!(clock
            .status(ControlPlaneAuthorityClockContext::new(
                authority.snapshot().max_committed_timestamp_ms(),
                None,
                true,
                true,
            ))
            .established());
    });
}

#[test]
fn authority_clock_accepts_healthy_elapsed_time_after_idle() {
    let mut clock = ControlPlaneAuthorityClock::new(Some(1_000), 1_000, Some(50)).unwrap();
    assert_eq!(
        clock.effective_now_ms(11_000, Some(10_050)).unwrap(),
        11_000
    );
}

#[test]
fn authority_clock_rejects_missing_initial_health_sample() {
    assert!(matches!(
        ControlPlaneAuthorityClock::new(Some(1_000), 1_000, None),
        Err(ControlPlaneError::AuthorityClockSourceUnavailable)
    ));
}

#[test]
fn authority_clock_latches_later_health_source_failure() {
    let mut clock = ControlPlaneAuthorityClock::new(Some(1_000), 1_000, Some(50)).unwrap();
    assert!(matches!(
        clock.effective_now_ms(1_100, None),
        Err(ControlPlaneError::AuthorityClockSourceUnavailable)
    ));
    assert!(clock.effective_now_ms(1_101, Some(151)).is_err());
}

#[test]
fn authority_clock_status_observes_new_raft_term_before_serving_request() {
    let mut clock = ControlPlaneAuthorityClock::new(Some(1_000), 1_000, Some(50)).unwrap();
    clock.bind_initial_raft_leadership_term(Some(7));
    let context = ControlPlaneAuthorityClockContext::new(Some(1_000), Some(8), true, true);

    let status = clock.observe_status(context, 1_100, Some(150)).unwrap();

    assert!(!status.established());
    assert_eq!(
        status.blocked_reason(),
        Some(ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged)
    );
    assert_eq!(status.bound_raft_leadership_term(), Some(7));
    assert_eq!(status.current_raft_leadership_term(), Some(8));
    assert!(status.local_raft_authority_leader());
    assert!(status.local_raft_authority_serving());
}

#[test]
fn authority_clock_status_codec_preserves_blocked_local_raft_leader() {
    let clock = ControlPlaneAuthorityClock::new(Some(1_000), 1_000, Some(50)).unwrap();
    let status = clock.status(ControlPlaneAuthorityClockContext::new(
        Some(1_000),
        Some(8),
        true,
        false,
    ));
    let mut encoded = Vec::new();
    write_authority_clock_status(&mut encoded, status);
    let mut reader = PayloadReader::new(&encoded);
    let decoded = read_authority_clock_status(&mut reader).unwrap();
    reader.finish().unwrap();

    assert!(decoded.local_raft_authority_leader());
    assert!(!decoded.local_raft_authority_serving());
    assert_eq!(decoded.current_raft_leadership_term(), Some(8));
}

#[test]
fn authority_clock_invalidates_new_local_raft_leadership_term() {
    let mut clock = ControlPlaneAuthorityClock::new(Some(1_000), 1_000, Some(50)).unwrap();
    clock.bind_initial_raft_leadership_term(Some(7));
    clock.validate_raft_leadership_term(7).unwrap();
    assert_eq!(clock.effective_now_ms(1_100, Some(150)).unwrap(), 1_100);

    assert!(matches!(
        clock.validate_raft_leadership_term(8),
        Err(ControlPlaneError::AuthorityClockLeadershipChanged {
            established_term: Some(7),
            current_term: 8,
        })
    ));
    assert!(clock.effective_now_ms(1_101, Some(151)).is_err());
}

#[test]
fn authority_clock_reestablishment_is_fenced_by_generation_timestamp_and_term() {
    let mut clock = ControlPlaneAuthorityClock::new(Some(1_000), 1_000, Some(50)).unwrap();
    clock.bind_initial_raft_leadership_term(Some(7));
    assert!(clock.validate_raft_leadership_term(8).is_err());
    let context = ControlPlaneAuthorityClockContext::new(Some(1_000), Some(8), true, true);
    let blocked = clock.status(context);
    assert!(!blocked.established());
    assert_eq!(
        blocked.blocked_reason(),
        Some(ControlPlaneAuthorityClockBlockedReason::RaftLeadershipChanged)
    );

    assert!(matches!(
        clock.reestablish(
            blocked.generation(),
            Some(999),
            Some(8),
            context,
            1_100,
            Some(150),
        ),
        Err(ControlPlaneError::AuthorityClockCommittedTimestampMismatch { .. })
    ));
    assert!(matches!(
        clock.reestablish(
            blocked.generation(),
            Some(1_000),
            Some(9),
            context,
            1_100,
            Some(150),
        ),
        Err(ControlPlaneError::AuthorityClockRaftTermMismatch { .. })
    ));

    let established = clock
        .reestablish(
            blocked.generation(),
            Some(1_000),
            Some(8),
            context,
            1_100,
            Some(150),
        )
        .unwrap();
    assert!(established.established());
    assert_eq!(established.bound_raft_leadership_term(), Some(8));
    assert_eq!(established.generation(), blocked.generation() + 1);

    assert!(clock.effective_now_ms(1_101, None).is_err());
    assert!(matches!(
        clock.reestablish(
            blocked.generation(),
            Some(1_000),
            Some(8),
            context,
            1_102,
            Some(152),
        ),
        Err(ControlPlaneError::AuthorityClockGenerationMismatch { .. })
    ));
}

#[test]
fn authority_clock_reestablishment_rejects_follower_and_wall_behind_high_water() {
    let mut clock = ControlPlaneAuthorityClock::new(Some(2_000), 4_000, Some(50)).unwrap();
    clock.bind_initial_raft_leadership_term(Some(7));
    assert!(clock.validate_raft_leadership_term(8).is_err());
    let follower_context =
        ControlPlaneAuthorityClockContext::new(Some(2_000), Some(8), false, false);
    let generation = clock.status(follower_context).generation();
    assert!(matches!(
        clock.reestablish(
            generation,
            Some(2_000),
            Some(8),
            follower_context,
            4_000,
            Some(50),
        ),
        Err(ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority)
    ));

    let leader_context = ControlPlaneAuthorityClockContext::new(Some(2_000), Some(8), true, true);
    assert!(matches!(
        clock.reestablish(
            generation,
            Some(2_000),
            Some(8),
            leader_context,
            1_999,
            Some(50),
        ),
        Err(ControlPlaneError::AuthorityClockWallBehindCommittedTimestamp { .. })
    ));
}

#[test]
fn restored_follower_clock_cannot_establish_first_local_leadership_term() {
    let mut clock = ControlPlaneAuthorityClock::new(None, 1_000, Some(50)).unwrap();
    clock.bind_initial_raft_leadership_term(None);
    clock.observe_committed_timestamp_high_water(Some(1_000));

    assert!(matches!(
        clock.validate_raft_leadership_term(8),
        Err(ControlPlaneError::AuthorityClockLeadershipChanged {
            established_term: None,
            current_term: 8,
        })
    ));
}

#[test]
fn follower_role_without_timestamp_high_water_still_requires_reestablishment() {
    let mut clock = ControlPlaneAuthorityClock::new(None, 1_000, Some(50)).unwrap();
    clock.bind_initial_raft_leadership_term(None);

    assert!(matches!(
        clock.validate_raft_leadership_term(1),
        Err(ControlPlaneError::AuthorityClockLeadershipChanged {
            established_term: None,
            current_term: 1,
        })
    ));
}

#[test]
fn fresh_unbound_clock_can_bind_first_local_raft_leadership_term() {
    let mut clock = ControlPlaneAuthorityClock::new(None, 1_000, Some(50)).unwrap();

    clock.validate_raft_leadership_term(1).unwrap();
    assert_eq!(clock.effective_now_ms(1_001, Some(51)).unwrap(), 1_001);
}

#[test]
fn authority_clock_latches_forward_step_without_timestamp_ratchet() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority.expire_heartbeat_leases(1_101).unwrap();

    let before = authority.snapshot().clone();
    let mut clock = ControlPlaneAuthorityClock::new(Some(1_101), 1_101, Some(10)).unwrap();
    let far_future_now_ms = 1_101 + CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 123;
    assert!(matches!(
        clock.effective_now_ms(far_future_now_ms, Some(11)),
        Err(ControlPlaneError::CommittedTimestampTooFarAhead { .. })
    ));
    for _ in 0..2 {
        assert!(matches!(
            clock.effective_now_ms(far_future_now_ms, Some(11)),
            Err(ControlPlaneError::AuthorityClockNotEstablished {
                blocked_reason: Some(ControlPlaneAuthorityClockBlockedReason::WallClockForwardJump),
            })
        ));
    }
    assert_eq!(authority.snapshot(), &before);
}
