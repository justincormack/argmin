// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn file_backed_authority_restarts_with_never_reused_epoch_and_incarnation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert_eq!(
        authority.snapshot().authority_incarnation(),
        AuthorityIncarnation::INITIAL
    );
    assert_eq!(authority.snapshot().cluster_epoch(), ClusterEpoch::INITIAL);

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let first_lease = authority
        .heartbeat(
            heartbeat(1, authority.snapshot().cluster_epoch(), 1_000),
            1_000,
        )
        .unwrap();
    assert_eq!(
        store.load().unwrap().unwrap().max_committed_timestamp_ms(),
        Some(1_000)
    );

    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(restarted.snapshot().authority_incarnation() > first_lease.authority_incarnation());
    assert!(restarted.snapshot().cluster_epoch() > first_lease.cluster_epoch());
    assert_eq!(
        restarted.snapshot().max_committed_timestamp_ms(),
        Some(1_000)
    );
}

#[test]
fn file_backed_authority_restarts_empty_state_with_new_epoch_and_incarnation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert_eq!(
        authority.snapshot().authority_incarnation(),
        AuthorityIncarnation::INITIAL
    );
    assert_eq!(authority.snapshot().cluster_epoch(), ClusterEpoch::INITIAL);

    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(restarted.snapshot().authority_incarnation() > AuthorityIncarnation::INITIAL);
    assert!(restarted.snapshot().cluster_epoch() > ClusterEpoch::INITIAL);
    assert_eq!(restarted.snapshot().nodes().count(), 0);
}

#[test]
fn file_backed_authority_rejects_pre_v7_state() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        "version=2\nauthority_incarnation=1\ncluster_epoch=1\nnode=1,active,1,healthy,11,1,100,200,6e6f64652d312e736f636b\n",
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "missing or unsupported control-plane state version"
    ));
}

#[test]
fn file_backed_authority_rejects_version_twenty_six_state() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        "version=26\nauthority_incarnation=1\ncluster_epoch=1\ninitial_topology=-\n",
    )
    .unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(path).load(),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "missing or unsupported control-plane state version"
    ));
}

#[test]
fn file_backed_authority_rejects_current_state_missing_timestamp_high_water() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        "version=27\nauthority_incarnation=1\ncluster_epoch=1\ninitial_topology=-\n",
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "missing max committed timestamp"
    ));
}

#[test]
fn initial_216_pg_placement_uses_sparse_history_deltas() {
    const PG_COUNT: u32 = 216;

    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    for pg_id in 0..PG_COUNT {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
    }

    let snapshot = authority.snapshot();
    assert_eq!(snapshot.pgs().count(), PG_COUNT as usize);
    assert_eq!(
        snapshot
            .cluster_map_history()
            .iter()
            .map(|record| record.pgs().len())
            .sum::<usize>(),
        0,
        "introducing PGs must not copy every previously configured route"
    );
    assert_eq!(
        snapshot
            .cluster_map_history()
            .iter()
            .map(|record| record.absent_pgs.len())
            .sum::<usize>(),
        PG_COUNT as usize
    );
    let runtime_map = snapshot.runtime_map(1_001).unwrap();
    assert!(
        runtime_map.historical_pg_routes().len()
            <= snapshot.cluster_map_history().len() + PG_COUNT as usize
    );
    let persisted = std::fs::read(store.path()).unwrap();
    assert!(
        persisted.len() < 128 * 1_024,
        "216-PG initial placement state unexpectedly grew to {} bytes",
        persisted.len()
    );
    assert_eq!(store.load().unwrap().as_ref(), Some(snapshot));
}

#[test]
fn sparse_runtime_map_round_trip_preserves_pg_introduction_boundary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    let before_introduction = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(2), vec![NodeId::new(1)])
        .unwrap();
    let after_introduction = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(3), NodeMembershipState::Active)
        .unwrap();

    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &runtime_map).unwrap();
    let decoded = read_runtime_map_snapshot(&mut PayloadReader::new(&payload)).unwrap();

    assert!(matches!(
        decoded.reconstructed_pg_route_at_epoch(PgId::new(2), before_introduction),
        Err(ControlPlaneError::UnknownPg { pg_id: 2 })
    ));
    assert_eq!(
        decoded
            .reconstructed_pg_route_at_epoch(PgId::new(2), after_introduction)
            .unwrap()
            .acting_set(),
        &[NodeId::new(1)]
    );
}

#[test]
fn sparse_runtime_map_round_trip_preserves_epoch_before_first_pg() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    let no_pg_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    authority
        .set_node_membership(NodeId::new(3), NodeMembershipState::Active)
        .unwrap();

    assert!(matches!(
        authority
            .snapshot()
            .reconstructed_pg_route_at_epoch(PgId::new(1), no_pg_epoch),
        Err(ControlPlaneError::UnknownPg { pg_id: 1 })
    ));
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    assert!(runtime_map
        .historical_cluster_epochs()
        .contains(&no_pg_epoch));
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &runtime_map).unwrap();
    let decoded = read_runtime_map_snapshot(&mut PayloadReader::new(&payload)).unwrap();
    assert!(matches!(
        decoded.reconstructed_pg_route_at_epoch(PgId::new(1), no_pg_epoch),
        Err(ControlPlaneError::UnknownPg { pg_id: 1 })
    ));
}

#[test]
fn cluster_map_history_is_persisted_across_epoch_changes_and_pruned() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let initial_epoch = authority.snapshot().cluster_epoch();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();

    let persisted = store.load().unwrap().unwrap();
    let initial_history = persisted.cluster_map_at_epoch(initial_epoch).unwrap();
    assert_eq!(
        initial_history.authority_incarnation(),
        AuthorityIncarnation::INITIAL
    );
    assert_eq!(initial_history.nodes().len(), 0);
    assert_eq!(initial_history.pgs().len(), 0);

    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(3), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let pg_epoch = authority.snapshot().cluster_epoch();
    let restarted = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let before_restart = restarted
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(3), pg_epoch)
        .unwrap();
    assert_eq!(
        before_restart.acting_set(),
        &[NodeId::new(1), NodeId::new(2)]
    );

    let mut authority = restarted;
    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    let history = authority.snapshot().cluster_map_history();
    assert_eq!(history.len(), CLUSTER_MAP_HISTORY_LIMIT);
    assert!(history.first().unwrap().cluster_epoch() > initial_epoch);
    assert!(history.last().unwrap().cluster_epoch() < authority.snapshot().cluster_epoch());

    let persisted = store.load().unwrap().unwrap();
    assert_eq!(
        persisted.cluster_map_history().len(),
        CLUSTER_MAP_HISTORY_LIMIT
    );
    assert!(persisted.cluster_map_at_epoch(initial_epoch).is_none());
}

#[test]
fn cluster_map_history_pruning_preserves_metadata_transfer_route_epochs() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let active_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(43), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        11_002,
    );
    let initial_epoch = ClusterEpoch::INITIAL;
    let source_epoch = authority.snapshot().cluster_epoch();
    let imported_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 12,
        state_digest: 11,
    };
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        active_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    let destination_epoch = authority.snapshot().cluster_epoch();

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    let history = authority.snapshot().cluster_map_history();
    assert_eq!(history.len(), CLUSTER_MAP_HISTORY_LIMIT + 2);
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(source_epoch)
        .is_some());
    let protected_source = authority
        .snapshot()
        .cluster_map_at_epoch(source_epoch)
        .unwrap();
    assert!(protected_source.pg(PgId::new(42)).is_some());
    assert!(protected_source.pg(PgId::new(43)).is_none());
    assert_eq!(protected_source.pgs().len(), 1);
    let protected_destination = authority
        .snapshot()
        .cluster_map_at_epoch(destination_epoch)
        .unwrap();
    assert!(protected_destination.pg(PgId::new(43)).is_none());
    let destination_route = authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(42), destination_epoch)
        .unwrap();
    assert_eq!(
        destination_route.peering_metadata_transfer(),
        Some(transfer)
    );
    assert_eq!(
        destination_route.peering_metadata_transfer_destination_epoch(),
        Some(destination_epoch)
    );
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(initial_epoch)
        .is_none());

    let persisted = store.load().unwrap().unwrap();
    assert!(persisted.cluster_map_at_epoch(source_epoch).is_some());
    assert!(persisted.cluster_map_at_epoch(destination_epoch).is_some());
    assert_eq!(
        persisted
            .pg(PgId::new(42))
            .unwrap()
            .peering_metadata_transfer_source_route_epoch(),
        Some(source_epoch)
    );

    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        imported_proof,
        false,
        13_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            13_001,
        )
        .unwrap();
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(destination_epoch)
        .is_none());
    let persisted_after_completion = store.load().unwrap().unwrap();
    assert!(persisted_after_completion
        .cluster_map_at_epoch(destination_epoch)
        .is_none());
}

#[test]
fn exact_old_transfer_route_preserves_and_clears_older_source_dependency_atomically() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(&mut authority, 1, 42, PgState::Active, proof, false, 11_002);
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        proof,
        PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 12,
            state_digest: 11,
        },
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    let transfer_epoch = authority.snapshot().cluster_epoch();
    let source_record = authority
        .snapshot()
        .cluster_map_at_epoch(source_epoch)
        .unwrap()
        .clone();
    let transfer_record = ClusterMapHistoryRecord::from_snapshot(authority.snapshot());
    assert_eq!(
        transfer_record
            .pg(PgId::new(42))
            .unwrap()
            .peering_metadata_transfer_source_route_epoch,
        Some(source_epoch)
    );

    let current_epoch =
        ClusterEpoch::new(transfer_epoch.get() + CLUSTER_MAP_HISTORY_LIMIT as u64 + 2).unwrap();
    let mut history = vec![source_record, transfer_record];
    for raw_epoch in (transfer_epoch.get() + 1)..current_epoch.get() {
        history.push(ClusterMapHistoryRecord {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            cluster_epoch: ClusterEpoch::new(raw_epoch).unwrap(),
            nodes: Vec::new(),
            pgs: Vec::new(),
            absent_pgs: Vec::new(),
        });
    }
    let protection = ClusterMapHistoryProtection {
        exact_routes: [(transfer_epoch, PgId::new(42))].into_iter().collect(),
    };

    prune_cluster_map_history(&mut history, &protection, current_epoch);

    assert!(history.iter().any(|record| {
        record.cluster_epoch() == transfer_epoch && record.pg(PgId::new(42)).is_some()
    }));
    assert!(history.iter().any(|record| {
        record.cluster_epoch() == source_epoch && record.pg(PgId::new(42)).is_some()
    }));
    validate_metadata_transfer_route_references(
        &history,
        current_epoch,
        std::iter::empty::<(PgId, Option<ClusterEpoch>, Option<NodeId>)>(),
    )
    .unwrap();

    prune_cluster_map_history(
        &mut history,
        &ClusterMapHistoryProtection {
            exact_routes: BTreeSet::new(),
        },
        current_epoch,
    );

    assert!(!history.iter().any(|record| {
        record.cluster_epoch() == transfer_epoch && record.pg(PgId::new(42)).is_some()
    }));
    assert!(!history.iter().any(|record| {
        record.cluster_epoch() == source_epoch && record.pg(PgId::new(42)).is_some()
    }));
}

#[test]
fn exact_old_routes_do_not_displace_recent_reverse_deltas() {
    let current_epoch = ClusterEpoch::new(1_000).unwrap();
    let ordinary_floor =
        ClusterEpoch::new(current_epoch.get() - CLUSTER_MAP_HISTORY_LIMIT as u64).unwrap();
    let old_epoch = ClusterEpoch::new(100).unwrap();
    let pg_id = PgId::new(42);
    let old_pg_id = PgId::new(7);
    let route = HistoricalPgRouteRecord {
        pg_id,
        state: PgState::Active,
        acting_set: vec![NodeId::new(1), NodeId::new(2)],
        active_primary: Some(NodeId::new(1)),
        peering_metadata_transfer: None,
        peering_metadata_transfer_source_route_epoch: None,
        peering_metadata_transfer_source_node_id: None,
    };
    let old_route = HistoricalPgRouteRecord {
        pg_id: old_pg_id,
        ..route.clone()
    };
    let mut history = vec![ClusterMapHistoryRecord {
        authority_incarnation: AuthorityIncarnation::INITIAL,
        cluster_epoch: old_epoch,
        nodes: vec![NodeId::new(1), NodeId::new(2)],
        pgs: vec![old_route],
        absent_pgs: Vec::new(),
    }];
    for raw_epoch in ordinary_floor.get()..current_epoch.get() {
        history.push(ClusterMapHistoryRecord {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            cluster_epoch: ClusterEpoch::new(raw_epoch).unwrap(),
            nodes: vec![NodeId::new(1), NodeId::new(2)],
            pgs: (raw_epoch == ordinary_floor.get())
                .then(|| route.clone())
                .into_iter()
                .collect(),
            absent_pgs: Vec::new(),
        });
    }
    let protection = ClusterMapHistoryProtection {
        exact_routes: [(old_epoch, old_pg_id)].into_iter().collect(),
    };

    prune_cluster_map_history(&mut history, &protection, current_epoch);

    assert_eq!(history.len(), CLUSTER_MAP_HISTORY_LIMIT + 1);
    assert!(history
        .iter()
        .any(|record| { record.cluster_epoch() == ordinary_floor && record.pg(pg_id).is_some() }));
}

#[test]
fn cluster_map_history_pruning_preserves_only_exact_storage_node_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    for pg_id in [1, 2] {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1)])
            .unwrap();
    }
    assert!(heartbeat_until_serving(&mut authority, 1, 10_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 10_100);
    floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            protected_epoch,
            PgId::new(1),
        )]);
    assert!(authority
        .heartbeat(floor_heartbeat, 10_100)
        .unwrap()
        .serving());

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());
    let protected_record = authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .unwrap();
    assert!(
        protected_record.pgs().is_empty(),
        "unchanged routes should not be copied into an exact epoch marker"
    );
    assert!(authority
        .snapshot()
        .reconstructed_pg_route_at_epoch(PgId::new(1), protected_epoch)
        .is_ok());
    let current_epoch = authority.snapshot().cluster_epoch();
    let advanced_floor = ClusterEpoch::new(current_epoch.get() - 10).unwrap();
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(advanced_floor)
        .is_some());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(protected_epoch)
    );
    let retained_before_floor_advance = authority.snapshot().cluster_map_history().len();
    assert_eq!(retained_before_floor_advance, CLUSTER_MAP_HISTORY_LIMIT + 1);
    let runtime_map_before_floor_advance = authority.snapshot().runtime_map(10_999).unwrap();
    assert_eq!(
        runtime_map_before_floor_advance.historical_cluster_epochs(),
        authority
            .snapshot()
            .cluster_map_history()
            .iter()
            .map(ClusterMapHistoryRecord::cluster_epoch)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        runtime_map_before_floor_advance
            .historical_pg_routes()
            .len(),
        2,
        "one exact route and one reconstruction baseline are sufficient"
    );
    let mut advanced_floor_heartbeat = heartbeat_from_record(&authority, 1, current_epoch, 11_000);
    advanced_floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            advanced_floor,
            PgId::new(1),
        )]);
    assert!(authority
        .heartbeat(advanced_floor_heartbeat, 11_000)
        .unwrap()
        .serving());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(advanced_floor)
    );
    assert_eq!(
        authority.snapshot().cluster_map_history().len(),
        CLUSTER_MAP_HISTORY_LIMIT
    );
    let runtime_map_after_floor_advance = authority.snapshot().runtime_map(11_000).unwrap();
    assert_eq!(
        runtime_map_after_floor_advance.historical_cluster_epochs(),
        authority
            .snapshot()
            .cluster_map_history()
            .iter()
            .map(ClusterMapHistoryRecord::cluster_epoch)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        runtime_map_after_floor_advance.historical_pg_routes().len(),
        2,
        "advancing the exact route must not restore per-epoch route markers"
    );
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_none());
    let persisted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(persisted
        .snapshot()
        .cluster_map_at_epoch(advanced_floor)
        .is_some());
    assert_eq!(
        persisted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(advanced_floor)
    );
    assert_eq!(
        persisted.snapshot().runtime_map(11_000).unwrap().nodes()[0]
            .cluster_map_history_floor_epoch(),
        Some(advanced_floor)
    );
}

#[test]
fn exact_old_route_retains_later_pg_introduction_boundary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut exact_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 1_001);
    exact_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            protected_epoch,
            PgId::new(1),
        )]);
    authority.heartbeat(exact_heartbeat, 1_001).unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    let introduction_boundary = authority.snapshot().cluster_epoch();
    authority
        .set_pg_acting_set(PgId::new(2), vec![NodeId::new(1)])
        .unwrap();

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(introduction_boundary)
        .is_some_and(|record| record.absent_pgs.contains(&PgId::new(2))));
    assert!(matches!(
        authority
            .snapshot()
            .reconstructed_pg_route_at_epoch(PgId::new(2), protected_epoch),
        Err(ControlPlaneError::UnknownPg { pg_id: 2 })
    ));
    let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
    let mut payload = Vec::new();
    write_runtime_map_snapshot(&mut payload, &runtime_map).unwrap();
    let decoded = read_runtime_map_snapshot(&mut PayloadReader::new(&payload)).unwrap();
    assert!(matches!(
        decoded.reconstructed_pg_route_at_epoch(PgId::new(2), protected_epoch),
        Err(ControlPlaneError::UnknownPg { pg_id: 2 })
    ));
}

#[test]
fn heartbeat_persists_exact_cluster_map_history_route_references() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    for pg_id in [1, 2] {
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1)])
            .unwrap();
    }
    let current_epoch = authority.snapshot().cluster_epoch();
    let references = PgClusterMapHistoryRouteReferences::try_from_iter([
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            current_epoch,
            PgId::new(1),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
            current_epoch,
            PgId::new(2),
        ),
        PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim,
            current_epoch,
            PgId::new(1),
        ),
    ])
    .unwrap();
    let mut heartbeat = heartbeat_from_record(&authority, 1, current_epoch, 10_000);
    heartbeat.cluster_map_history_route_references = references.clone();
    authority.heartbeat(heartbeat, 10_000).unwrap();

    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_route_references(),
        &references
    );
    let persisted_text = format_snapshot(&store.load().unwrap().unwrap());
    assert!(persisted_text.contains("node_history_route=1,live,"));
    assert!(persisted_text.contains("node_history_route=1,backfill-desired,"));
    let invalid_kind = persisted_text.replace(
        "node_history_route=1,live,",
        "node_history_route=1,unknown,",
    );
    assert!(matches!(
        parse_snapshot(&invalid_kind),
        Err(ControlPlaneError::Parse { message, .. })
            if message.contains("invalid node history route reference kind")
    ));
    let live_line = persisted_text
        .lines()
        .find(|line| line.starts_with("node_history_route=1,live,"))
        .unwrap();
    let duplicate = persisted_text.replace(live_line, &format!("{live_line}\n{live_line}"));
    assert!(matches!(
        parse_snapshot(&duplicate),
        Err(ControlPlaneError::Parse { message, .. })
            if message.contains("duplicate node history route reference")
    ));
    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_route_references(),
        &references
    );
}

#[test]
fn heartbeat_rejects_future_or_missing_exact_history_route_before_persisting() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let before = authority.snapshot().clone();

    let mut future = heartbeat_from_record(&authority, 1, current_epoch, 10_000);
    future.cluster_map_history_route_references =
        PgClusterMapHistoryRouteReferences::try_from_iter([
            PgClusterMapHistoryRouteReference::new(
                PgClusterMapHistoryRouteReferenceKind::LivePlacement,
                ClusterEpoch::new(current_epoch.get() + 1).unwrap(),
                PgId::new(1),
            ),
        ])
        .unwrap();
    assert!(matches!(
        authority.heartbeat(future, 10_000),
        Err(ControlPlaneError::StorageClusterMapHistoryRouteInFuture {
            route_epoch,
            pg_id: 1,
            validation_epoch,
            ..
        }) if route_epoch.get() == current_epoch.get() + 1
            && validation_epoch == current_epoch
    ));
    assert_eq!(authority.snapshot(), &before);

    let mut missing = heartbeat_from_record(&authority, 1, current_epoch, 10_001);
    missing.cluster_map_history_route_references =
        PgClusterMapHistoryRouteReferences::try_from_iter([
            PgClusterMapHistoryRouteReference::new(
                PgClusterMapHistoryRouteReferenceKind::LivePlacement,
                ClusterEpoch::INITIAL,
                PgId::new(99),
            ),
        ])
        .unwrap();
    assert!(matches!(
        authority.heartbeat(missing, 10_001),
        Err(
            ControlPlaneError::StorageClusterMapHistoryRouteNotRetained {
                route_epoch: ClusterEpoch::INITIAL,
                pg_id: 99,
                ..
            }
        )
    ));
    assert_eq!(authority.snapshot(), &before);
}

#[test]
fn cluster_map_history_pruning_preserves_reported_pending_command_epoch_exactly() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 10_000).serving());
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        1,
        PgState::Peering,
        PgMetadataProof::empty(),
        false,
        10_020,
    );
    authority.complete_ready_pg_peerings(10_030).unwrap();
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 10_100);
    heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand,
            protected_epoch,
            PgId::new(1),
        )]);
    heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(1),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        pending_metadata_command: Some(PendingMetadataCommandObservation::new(
            protected_epoch,
            NonZeroU64::MIN,
            0x1234,
        )),
    }];
    authority.heartbeat(heartbeat, 10_100).unwrap();
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .pg_observation(PgId::new(1))
            .unwrap()
            .pending_metadata_command(),
        Some(PendingMetadataCommandObservation::new(
            protected_epoch,
            NonZeroU64::MIN,
            0x1234,
        ))
    );

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        let now_ms = 20_000 + u64::from(node_id);
        let current_epoch = authority.snapshot().cluster_epoch();
        let mut heartbeat = heartbeat_from_record(&authority, 1, current_epoch, now_ms);
        heartbeat.cluster_map_history_route_references =
            history_route_references([PgClusterMapHistoryRouteReference::new(
                PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand,
                protected_epoch,
                PgId::new(1),
            )]);
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(1),
            state: PgState::Peering,
            metadata_proof: PgMetadataProof::empty(),
            pending_metadata_command: Some(PendingMetadataCommandObservation::new(
                protected_epoch,
                NonZeroU64::MIN,
                0x1234,
            )),
        }];
        authority.heartbeat(heartbeat, now_ms).unwrap();
    }

    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());
    let persisted = store.load().unwrap().unwrap();
    assert!(persisted.cluster_map_at_epoch(protected_epoch).is_some());
}

#[test]
fn cluster_map_history_pruning_preserves_exact_durable_backfill_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 10_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 10_100);
    floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
            protected_epoch,
            PgId::new(1),
        )]);
    assert!(authority
        .heartbeat(floor_heartbeat, 10_100)
        .unwrap()
        .serving());

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(protected_epoch)
    );
    let persisted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(persisted
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());
    assert_eq!(
        persisted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(protected_epoch)
    );
}

#[test]
fn cluster_map_history_pruning_releases_cleared_exact_storage_node_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(1), vec![NodeId::new(1)])
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 10_000).serving());
    let protected_epoch = authority.snapshot().cluster_epoch();
    let mut floor_heartbeat = heartbeat_from_record(&authority, 1, protected_epoch, 10_100);
    floor_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            protected_epoch,
            PgId::new(1),
        )]);
    assert!(authority
        .heartbeat(floor_heartbeat, 10_100)
        .unwrap()
        .serving());

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_some());

    let current_epoch = authority.snapshot().cluster_epoch();
    let clear_floor_heartbeat = heartbeat_from_record(&authority, 1, current_epoch, 11_000);
    assert!(authority
        .heartbeat(clear_floor_heartbeat, 11_000)
        .unwrap()
        .serving());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        None
    );

    for node_id in 100..(100 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_none());
    let persisted = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(persisted
        .snapshot()
        .cluster_map_at_epoch(protected_epoch)
        .is_none());
    assert_eq!(
        persisted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        None
    );
}

#[test]
fn heartbeat_accepts_exact_route_without_unretained_intermediate_epoch() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let active_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        11_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        active_proof,
        PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 12,
            state_digest: 11,
        },
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();

    for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }

    let destination_epoch = next_epoch(source_epoch).unwrap();
    let unretained_intermediate_epoch = next_epoch(destination_epoch).unwrap();
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(source_epoch)
        .is_some());
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(destination_epoch)
        .is_some());
    assert!(authority
        .snapshot()
        .cluster_map_at_epoch(unretained_intermediate_epoch)
        .is_none());
    let current_epoch = authority.snapshot().cluster_epoch();
    let heartbeat_at_ms = authority
        .snapshot()
        .max_committed_timestamp_ms()
        .unwrap_or(20_000);
    let mut exact_heartbeat = heartbeat_from_record(&authority, 1, current_epoch, heartbeat_at_ms);
    exact_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            source_epoch,
            PgId::new(42),
        )]);
    authority
        .heartbeat(exact_heartbeat, heartbeat_at_ms)
        .unwrap();
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(source_epoch)
    );
    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .cluster_map_history_floor_epoch(),
        Some(source_epoch)
    );
}

#[test]
fn peering_acting_set_update_preserves_metadata_transfer_source_route_fields() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let active_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        11_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        active_proof,
        PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 12,
            state_digest: 11,
        },
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        transfer.metadata_proof(),
        false,
        11_003,
    );

    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(2), NodeId::new(3)])
        .unwrap();

    let persisted = SingleAuthorityControlPlane::open(store).unwrap();
    let pg = persisted.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        pg.peering_metadata_transfer_source_route_epoch(),
        Some(source_epoch)
    );
    assert_eq!(
        pg.peering_metadata_transfer_source_node_id(),
        Some(NodeId::new(1))
    );
}

#[test]
fn control_plane_reload_rejects_transfer_marker_without_source_route_history() {
    let tmp = test_util::tempdir();
    let state_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&state_path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 10_000).serving());
    }
    let active_proof = PgMetadataProof {
        applied_log_index: 9,
        applied_log_hash: 10,
        state_digest: 11,
    };
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        11_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            11_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        11_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        active_proof,
        PgMetadataProof {
            applied_log_index: 9,
            applied_log_hash: 12,
            state_digest: 11,
        },
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    assert!(store
        .load()
        .unwrap()
        .unwrap()
        .cluster_map_at_epoch(source_epoch)
        .is_some());
    store
        .checkpoint(Some(authority.snapshot()), authority.snapshot())
        .unwrap();

    let source_history_prefixes = [
        format!("history={},", source_epoch.get()),
        format!("history_node={},", source_epoch.get()),
        format!("history_pg={},", source_epoch.get()),
    ];
    let state = std::fs::read_to_string(&state_path).unwrap();
    let filtered = state
        .lines()
        .filter(|line| {
            !source_history_prefixes
                .iter()
                .any(|prefix| line.starts_with(prefix))
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(&state_path, filtered).unwrap();

    let err = store.load().unwrap_err();
    assert!(matches!(
        err,
        ControlPlaneError::Parse { message, .. }
            if message.contains(
                "references missing metadata transfer source route epoch"
            )
    ));
}

#[test]
fn snapshot_reconstructs_pg_route_at_epoch_without_serving_authority() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(3), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let route_epoch = authority.snapshot().cluster_epoch();

    authority
        .set_node_membership(NodeId::new(4), NodeMembershipState::Active)
        .unwrap();
    let snapshot = authority.snapshot();
    let route = snapshot
        .reconstructed_pg_route_at_epoch(PgId::new(3), route_epoch)
        .unwrap();
    assert_eq!(route.cluster_epoch(), route_epoch);
    assert_eq!(route.pg_id(), PgId::new(3));
    assert_eq!(route.primary_node_id(), NodeId::new(1));
    assert_eq!(route.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
    assert_eq!(route.state(), PgState::Peering);
    assert_eq!(route.primary_lease_deadline_ms(), None);
    let missing_epoch = ClusterEpoch::new(snapshot.cluster_epoch().get() + 1).unwrap();
    assert!(matches!(
        snapshot.reconstructed_pg_route_at_epoch(PgId::new(3), missing_epoch),
        Err(ControlPlaneError::UnknownClusterMapEpoch { cluster_epoch })
            if cluster_epoch == missing_epoch
    ));
}

#[test]
fn file_backed_authority_rejects_duplicate_pg_acting_set_nodes() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=1\n",
            "pg=7,peering,1:1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "PG acting set contains duplicate node"
    ));
}

#[test]
fn file_backed_authority_rejects_current_pg_nodes_absent_from_current_map() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "pg=7,active,1:99,1,1,2,3,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,2,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "PG acting set references unknown node"
    ));
}

#[test]
fn file_backed_authority_rejects_current_pg_future_metadata_transfer_epoch() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "pg=7,peering,1,-,-,-,-,9,10,11,3,9,10,11,-,-,-,2,1,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message
                == "metadata transfer source epoch must not be newer than PG record epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_active_imported_provenance_without_epoch() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "pg=7,active,1,1,9,10,11,-,-,-,-,-,-,-,-,-,-,-,-,0,1,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);

    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message
                == "active metadata transfer imported provenance requires an active metadata proof epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_history_for_unsupported_version() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        "version=5\nauthority_incarnation=1\ncluster_epoch=2\nhistory=1,1\n",
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "missing or unsupported control-plane state version"
    ));
}

#[test]
fn file_backed_authority_rejects_current_or_future_history_epochs() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "history=2,1\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "history epoch must be older than current cluster epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_history_pg_nodes_absent_from_history_map() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=3\n",
            "history=2,1\n",
            "history_node=2,1\n",
            "history_pg=2,7,peering,1:2,-,-,-,-,-,-,-,-,-,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "PG 7 acting set references unknown node 2"
    ));
}

#[test]
fn file_backed_authority_rejects_reconstructible_observations_in_history() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=3\n",
            "history=2,1\n",
            "history_node=2,1\n",
            "history_node_pg=2,1,7,peering,2,100,0,0,0,-,-,-\n",
            "history_pg=2,7,peering,1,-,-,-,-,-,-,-,-,-,-\n",
            "node=1,active,1,healthy,11,3,100,200,6e6f64652d312e736f636b\n",
            "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();

    let error = FileControlPlaneStore::new(path).load().unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Parse { message, .. }
            if message == "unknown control-plane state line"
    ));
}

#[test]
fn file_backed_authority_rejects_history_pg_future_metadata_transfer_epoch() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=4\n",
            "history=2,1\n",
            "history_node=2,1\n",
            "history_pg=2,7,peering,1,-,3,9,10,11,9,10,11,2,1\n",
            "node=1,active,1,healthy,11,4,100,200,6e6f64652d312e736f636b\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message
                == "PG 7 metadata transfer source epoch is newer than route epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_incomplete_compact_history_routes() {
    let cases = [
        (
            "history_pg=2,7,active,1,-,-,-,-,-,-,-,-,-,-\n",
            "active PG 7 has no primary",
        ),
        (
            "history_pg=2,7,peering,1,-,2,9,10,11,9,10,11,-,-\n",
            "PG 7 has incomplete metadata transfer route state",
        ),
    ];
    for (index, (history_pg, expected)) in cases.into_iter().enumerate() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{index}.state"));
        let contents = format!(
            "version=27\nmax_committed_timestamp_ms=-\nlease_grant_horizon=-\nauthority_incarnation=1\ncluster_epoch=3\ninitial_topology=-\nhistory=2,1\nhistory_node=2,1\n{history_pg}"
        );
        std::fs::write(&path, contents).unwrap();

        let error = FileControlPlaneStore::new(path).load().unwrap_err();

        assert!(
            matches!(
                error,
                ControlPlaneError::Parse { ref message, .. } if message == expected
            ),
            "unexpected error: {error:?}"
        );
    }
}

#[test]
fn file_backed_authority_rejects_broken_compact_history_transfer_chains() {
    let cases = [
        (
            "self-reference",
            concat!(
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg=2,7,peering,1,-,1,9,10,11,9,10,11,2,1\n",
            ),
            "PG 7 metadata transfer source route epoch is not older than route epoch",
        ),
        (
            "missing-source-epoch",
            concat!(
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg=2,7,peering,1,-,1,9,10,11,9,10,11,1,1\n",
            ),
            "PG 7 references missing metadata transfer source route epoch 1",
        ),
        (
            "missing-source-pg",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_pg_absent=1,7\n",
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg=2,7,peering,1,-,1,9,10,11,9,10,11,1,1\n",
            ),
            "PG 7 references missing metadata transfer source PG at epoch 1",
        ),
        (
            "mismatched-source-primary",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_node=1,2\n",
                "history_pg=1,7,active,2,2,-,-,-,-,-,-,-,-,-\n",
                "history=2,1\n",
                "history_node=2,1\n",
                "history_node=2,2\n",
                "history_pg=2,7,peering,1,-,1,9,10,11,9,10,11,1,1\n",
            ),
            "PG 7 metadata transfer source node 1 does not match source route primary 2 at epoch 1",
        ),
    ];
    for (name, history, expected) in cases {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{name}.state"));
        let contents = format!(
            "version=27\nmax_committed_timestamp_ms=-\nlease_grant_horizon=-\nauthority_incarnation=1\ncluster_epoch=3\ninitial_topology=-\n{history}"
        );
        std::fs::write(&path, contents).unwrap();

        let error = FileControlPlaneStore::new(path).load().unwrap_err();

        assert!(
            matches!(
                error,
                ControlPlaneError::Parse { ref message, .. } if message == expected
            ),
            "unexpected error: {error:?}"
        );
    }
}

#[test]
fn file_backed_authority_rejects_invalid_pg_introduction_history() {
    let current_node = "node=1,active,1,suspect,11,-,-,-,2f746d702f6e6f64652d312e736f636b\n";
    let current_pg = "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n";
    let cases = [
        (
            "duplicate",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_pg_absent=1,7\n",
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg_absent=2,7\n",
            ),
            "history repeats a PG introduction boundary",
        ),
        (
            "route-before-introduction",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_pg=1,7,peering,1,-,-,-,-,-,-,-,-,-,-\n",
                "history=2,1\n",
                "history_node=2,1\n",
                "history_pg_absent=2,7\n",
            ),
            "history PG route precedes its introduction boundary",
        ),
        (
            "missing-current-pg",
            concat!(
                "history=1,1\n",
                "history_node=1,1\n",
                "history_pg_absent=1,7\n",
                "history=2,1\n",
                "history_node=2,1\n",
            ),
            "history absent PG is missing from current state",
        ),
    ];
    for (name, history, expected) in cases {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{name}.state"));
        let current_pg = if name == "missing-current-pg" {
            ""
        } else {
            current_pg
        };
        std::fs::write(
            &path,
            format!(
                "version=27\nmax_committed_timestamp_ms=-\nlease_grant_horizon=-\nauthority_incarnation=1\ncluster_epoch=3\ninitial_topology=-\n{history}{current_node}{current_pg}"
            ),
        )
        .unwrap();

        let error = FileControlPlaneStore::new(path).load().unwrap_err();

        assert!(
            matches!(
                error,
                ControlPlaneError::Parse { ref message, .. } if message == expected
            ),
            "unexpected error for {name}: {error:?}"
        );
    }
}

#[test]
fn file_backed_authority_rejects_noncanonical_absent_pg_order() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "history=1,1\n",
            "history_pg_absent=1,8\n",
            "history_pg_absent=1,7\n",
            "node=1,active,1,suspect,11,-,-,-,2f746d702f6e6f64652d312e736f636b\n",
            "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
            "pg=8,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(path).load(),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "history absent PG records must be strictly increasing"
    ));
}

#[test]
fn file_backed_authority_rejects_pg_observations_for_unsupported_version() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=5\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "node_pg=1,7,peering,2,100,0,0,0,0\n",
            "pg=7,peering,1,-,-,-,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "missing or unsupported control-plane state version"
    ));
}

#[test]
fn file_backed_authority_rejects_current_pg_observation_outside_acting_set() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "node=2,active,1,healthy,12,2,100,200,6e6f64652d322e736f636b\n",
            "node_pg=2,7,peering,2,100,0,0,0,-,-,-\n",
            "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "node PG observation references PG outside node acting set"
    ));
}

#[test]
fn file_backed_authority_rejects_current_pg_observation_wrong_epoch() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=3\n",
            "node=1,active,1,healthy,11,3,100,200,6e6f64652d312e736f636b\n",
            "node_pg=1,7,peering,2,100,0,0,0,-,-,-\n",
            "pg=7,peering,1,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,-,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "node PG observation epoch must match current cluster epoch"
    ));
}

#[test]
fn file_backed_authority_rejects_active_pg_observation_with_mismatched_proof() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "node_pg=1,7,active,2,100,9,10,12,-,-,-\n",
            "pg=7,active,1,1,9,10,11,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,2,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message
                == "active node PG observation metadata proof is behind or diverges from PG active proof"
    ));
}

#[test]
fn file_backed_authority_accepts_active_pg_observation_after_metadata_progress() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let initial = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let snapshot = parse_snapshot(concat!(
        "version=27\n",
        "authority_incarnation=1\n",
        "cluster_epoch=2\n",
        "initial_topology=-\n",
        "max_committed_timestamp_ms=100\nlease_grant_horizon=-\n",
        "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
        "node_pg=1,7,active,2,100,10,20,30,-,-,-\n",
        "pg=7,active,1,1,9,10,11,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,2,-,0,-,-,-,-,0,-\n",
    ))
    .unwrap();
    store
        .checkpoint(Some(initial.snapshot()), &snapshot)
        .unwrap();
    drop(initial);
    let authority = SingleAuthorityControlPlane::open(store).unwrap();
    let history = authority
        .snapshot()
        .cluster_map_at_epoch(ClusterEpoch::new(2).unwrap())
        .unwrap();
    assert!(history.nodes().contains(&NodeId::new(1)));
    let historical_pg = history.pg(PgId::new(7)).unwrap();
    assert_eq!(historical_pg.state(), PgState::Active);
    assert_eq!(historical_pg.active_primary, Some(NodeId::new(1)));
}

#[test]
fn file_backed_authority_replays_journal_without_per_command_checkpoint() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let checkpoint_before = std::fs::read(store.path()).unwrap();
    let digest_computations_before = single_authority_snapshot_digest_computations();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();

    assert_eq!(
        single_authority_snapshot_digest_computations(),
        digest_computations_before,
        "journal suffix commands must not format and digest the full snapshot"
    );
    assert_eq!(
        std::fs::read(store.path()).unwrap(),
        checkpoint_before,
        "ordinary durable commands must not rewrite the full checkpoint"
    );
    let offsets = store.journal.status_offsets().unwrap();
    assert!(offsets.clean_len > offsets.base_offset);
    let replayed = store.load().unwrap().unwrap();
    assert_eq!(
        replayed.node(NodeId::new(1)).unwrap().membership(),
        NodeMembershipState::Active
    );

    let restarted = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
    let compacted = store.journal.status_offsets().unwrap();
    let retained = store
        .journal
        .read_frames_from(compacted.base_offset)
        .unwrap();
    assert_eq!(retained.frames.len(), 1);
    assert!(
        SingleAuthorityJournalRecord::decode(&retained.frames[0])
            .unwrap()
            .command
            .is_none(),
        "checkpoint compaction must retain exactly one checkpoint anchor"
    );
}

#[test]
fn file_backed_authority_recovers_identity_only_initialization() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let initializing_store = FileControlPlaneStore::new(&path);
    initializing_store
        .load_or_create_authority_clock_checkpoint_binding()
        .unwrap();

    let authority = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(authority.snapshot().nodes().count(), 0);
    assert!(single_authority_initialized_path(&path).exists());
}

#[test]
fn single_authority_durable_formats_reject_unsupported_versions() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let binding = ControlPlaneAuthorityClockCheckpointBinding([0x42; 32]);

    for version in [0, CONTROL_PLANE_STATE_IDENTITY_VERSION + 1] {
        store_single_authority_clock_checkpoint_binding(&path, binding).unwrap();
        let identity_path = single_authority_identity_path(&path);
        let mut bytes = std::fs::read(&identity_path).unwrap();
        bytes[CONTROL_PLANE_STATE_IDENTITY_MAGIC.len()
            ..CONTROL_PLANE_STATE_IDENTITY_MAGIC.len() + 2]
            .copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut bytes);
        std::fs::write(&identity_path, bytes).unwrap();
        assert!(matches!(
            load_single_authority_clock_checkpoint_binding(&path),
            Err(ControlPlaneError::AuthorityClockCheckpoint { message })
                if message == format!(
                    "unsupported single-authority durable identity version {version}"
                )
        ));
    }

    for version in [0, SINGLE_AUTHORITY_INITIALIZED_VERSION + 1] {
        store_single_authority_initialized_binding(&path, binding).unwrap();
        let initialized_path = single_authority_initialized_path(&path);
        let mut bytes = std::fs::read(&initialized_path).unwrap();
        bytes[SINGLE_AUTHORITY_INITIALIZED_MAGIC.len()
            ..SINGLE_AUTHORITY_INITIALIZED_MAGIC.len() + 2]
            .copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut bytes);
        std::fs::write(&initialized_path, bytes).unwrap();
        assert!(matches!(
            load_single_authority_initialized_binding(&path),
            Err(ControlPlaneError::CommandDecode { message })
                if message == format!(
                    "unsupported single-authority initialization marker version {version}"
                )
        ));
    }

    let store = FileControlPlaneStore::new(&path);
    for version in [
        SINGLE_AUTHORITY_JOURNAL_FILE_VERSION - 1,
        SINGLE_AUTHORITY_JOURNAL_FILE_VERSION + 1,
    ] {
        let mut header = store.journal.encode_file_header(0);
        let version_offset = SINGLE_AUTHORITY_JOURNAL_FILE_MAGIC.len();
        header[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut header);
        assert!(matches!(
            store.journal.decode_file_header(&header),
            Err(ControlPlaneError::CommandDecode { message })
                if message == format!(
                    "unsupported single-authority control-plane journal file header version {version}"
                )
        ));
    }

    for version in [
        SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION - 1,
        SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION + 1,
    ] {
        let mut record = SingleAuthorityJournalRecord {
            binding,
            previous_chain_digest: 7,
            resulting_chain_digest: 7,
            command: None,
        }
        .encode()
        .unwrap();
        let version_offset = SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC.len();
        record[version_offset..version_offset + 2].copy_from_slice(&version.to_be_bytes());
        reseal_crc64_suffix(&mut record);
        assert!(matches!(
            SingleAuthorityJournalRecord::decode(&record),
            Err(ControlPlaneError::CommandDecode { message })
                if message == format!(
                    "unsupported single-authority control-plane journal record version {version}"
                )
        ));
    }
}

#[test]
fn bare_control_plane_state_path_uses_current_directory_for_durability() {
    assert_eq!(
        state_parent(Path::new("control-plane.state")),
        Path::new(".")
    );
    assert_eq!(
        state_parent(Path::new("./control-plane.state")),
        Path::new(".")
    );
}

#[test]
fn control_plane_state_directory_creation_syncs_each_new_component_parent() {
    let tmp = test_util::tempdir();
    let first = tmp.path().join("first");
    let second = first.join("second");
    let mut synced_parents = Vec::new();

    create_control_plane_directory_all_durable_with(&second, |parent| {
        synced_parents.push(parent.to_path_buf());
        Ok(())
    })
    .unwrap();

    assert_eq!(
        synced_parents,
        vec![
            state_parent(tmp.path()).to_path_buf(),
            tmp.path().to_path_buf(),
            first
        ]
    );
    assert!(second.is_dir());
}

#[test]
fn control_plane_state_directory_creation_reconfirms_failed_sync_on_retry() {
    let tmp = test_util::tempdir();
    let first = tmp.path().join("first");
    let second = first.join("second");
    let mut sync_attempts = 0;

    let error = create_control_plane_directory_all_durable_with(&second, |_| {
        sync_attempts += 1;
        if sync_attempts == 2 {
            Err(ControlPlaneError::io(
                "injected control-plane state parent sync",
                std::io::Error::other("injected parent sync failure"),
            ))
        } else {
            Ok(())
        }
    })
    .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "injected control-plane state parent sync"
    ));
    assert!(first.is_dir());
    assert!(
        !second.exists(),
        "the next directory component must not be created before its parent link is durable"
    );

    let mut retry_synced_parents = Vec::new();
    create_control_plane_directory_all_durable_with(&second, |parent| {
        retry_synced_parents.push(parent.to_path_buf());
        Ok(())
    })
    .unwrap();

    assert_eq!(retry_synced_parents, vec![tmp.path().to_path_buf(), first]);
    assert!(second.is_dir());
}

#[test]
fn file_backed_authority_checkpoints_at_command_and_byte_bounds() {
    for (name, command_limit, byte_limit, commands_before_checkpoint) in
        [("commands", 2, u64::MAX, 2), ("bytes", u64::MAX, 1, 1)]
    {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::with_checkpoint_limits(
            tmp.path().join(format!("control-plane-{name}.state")),
            command_limit,
            byte_limit,
        );
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        let initial_checkpoint = std::fs::read(store.path()).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        if commands_before_checkpoint == 2 {
            assert_eq!(std::fs::read(store.path()).unwrap(), initial_checkpoint);
            authority
                .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
                .unwrap();
        }
        authority
            .capture_durable_checkpoint_if_due(Instant::now())
            .unwrap()
            .expect("checkpoint threshold should be due")
            .persist()
            .unwrap();

        assert_ne!(
            std::fs::read(store.path()).unwrap(),
            initial_checkpoint,
            "{name} threshold must publish a compacted checkpoint"
        );
        let offsets = store.journal.status_offsets().unwrap();
        let retained = store.journal.read_frames_from(offsets.base_offset).unwrap();
        assert_eq!(retained.frames.len(), 1);
        assert!(
            SingleAuthorityJournalRecord::decode(&retained.frames[0])
                .unwrap()
                .command
                .is_none(),
            "{name} threshold compaction must retain one checkpoint anchor"
        );
        let restarted = SingleAuthorityControlPlane::open(store).unwrap();
        assert_eq!(
            restarted
                .snapshot()
                .node(NodeId::new(1))
                .unwrap()
                .membership(),
            NodeMembershipState::Active
        );
    }
}

#[test]
fn file_backed_authority_checkpoint_is_due_at_time_bound() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_policy(
        tmp.path().join("control-plane.state"),
        u64::MAX,
        u64::MAX,
        Duration::ZERO,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let checkpoint_before = std::fs::read(store.path()).unwrap();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("time threshold should be due")
        .persist()
        .unwrap();

    assert_ne!(std::fs::read(store.path()).unwrap(), checkpoint_before);
}

#[test]
fn captured_checkpoint_preparation_failure_latches_poison() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::with_checkpoint_limits(path.clone(), 1, u64::MAX);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due");
    let prepared_path = single_authority_snapshot_tmp_path(&path);
    std::fs::create_dir(&prepared_path).unwrap();

    assert!(matches!(
        checkpoint.persist(),
        Err(ControlPlaneError::Io { diagnostic })
            if diagnostic.context() == "create control-plane state"
    ));
    let error = authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("durability is poisoned")
    ));
}

#[test]
fn captured_checkpoint_rebases_commands_appended_during_persistence() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::with_checkpoint_limits(path.clone(), 1, u64::MAX);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due");

    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    checkpoint.persist().unwrap();

    let offsets = store.journal.status_offsets().unwrap();
    let retained = store.journal.read_frames_from(offsets.base_offset).unwrap();
    assert_eq!(retained.frames.len(), 2);
    assert!(SingleAuthorityJournalRecord::decode(&retained.frames[0])
        .unwrap()
        .command
        .is_none());
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(path)).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
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
fn checkpoint_failure_latches_poison_before_concurrent_command_can_append() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::with_checkpoint_limits(path.clone(), 1, u64::MAX);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .unwrap();
    let previous_snapshot = authority.durable_snapshot.clone();
    let command = ControlPlaneCommand::SetNodeMembership {
        node_id: NodeId::new(2),
        membership: NodeMembershipState::Active,
    };
    let mut next_snapshot = previous_snapshot
        .apply_control_plane_command(command.clone())
        .unwrap()
        .into_snapshot();
    next_snapshot.record_history_from(&previous_snapshot);
    drop(authority);

    store.pause_next_checkpoint_after_journal_replacement();
    store.fail_next_checkpoint_after_anchor();
    let checkpoint_worker = std::thread::spawn(move || checkpoint.persist());
    let replacement_reached = store.wait_for_checkpoint_journal_replacement(Duration::from_secs(2));

    store.arm_commit_before_durability_lock_signal();
    let concurrent_store = store.clone();
    let command_worker = std::thread::spawn(move || {
        concurrent_store.commit_command(&previous_snapshot, &command, &next_snapshot)
    });
    let command_reached_durability_lock =
        store.wait_for_commit_before_durability_lock(Duration::from_secs(2));
    store.release_checkpoint_after_journal_replacement();

    let checkpoint_error = checkpoint_worker.join().unwrap().unwrap_err();
    let command_error = command_worker.join().unwrap().unwrap_err();
    assert!(
        replacement_reached,
        "checkpoint should pause after durable journal replacement"
    );
    assert!(
        command_reached_durability_lock,
        "concurrent command should reach the durability lock while checkpoint publication is paused"
    );
    assert!(matches!(
        checkpoint_error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "publish prepared single-authority control-plane checkpoint"
    ));
    assert!(matches!(
        command_error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("durability is poisoned")
    ));

    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(path)).unwrap();
    assert!(restarted.snapshot().node(NodeId::new(1)).is_some());
    assert!(
        restarted.snapshot().node(NodeId::new(2)).is_none(),
        "the waiting command must not append after replacement failure"
    );
}

#[test]
fn captured_checkpoint_preserves_conservative_suffix_age() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let captured_at = Instant::now();
    let checkpoint = authority
        .capture_durable_checkpoint_if_due(captured_at)
        .unwrap()
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();

    checkpoint.persist().unwrap();

    let durability = store.lock_durability().unwrap();
    assert_eq!(durability.commands_since_checkpoint, 1);
    assert_eq!(durability.first_uncheckpointed_at, Some(captured_at));
}

#[test]
fn checkpoint_capture_uses_tracked_offset_without_scanning_journal() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let expected_offset = store.lock_durability().unwrap().journal_clean_offset;
    std::fs::write(store.journal_path(), b"not a valid journal").unwrap();

    let checkpoint = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .expect("capture must not read the journal")
        .expect("command threshold should be due");

    assert_eq!(checkpoint.capture.journal_offset, expected_offset);
}

#[test]
fn stale_captured_checkpoint_is_rejected_before_publication() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let stale = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .unwrap();
    let current = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .unwrap();
    current.persist().unwrap();
    let checkpoint_before = std::fs::read(store.path()).unwrap();
    let journal_before = std::fs::read(store.journal_path()).unwrap();

    let error = stale.persist().unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("checkpoint capture is stale")
    ));
    assert_eq!(std::fs::read(store.path()).unwrap(), checkpoint_before);
    assert_eq!(std::fs::read(store.journal_path()).unwrap(), journal_before);
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .expect("stale checkpoint rejection must not poison the store");
}

#[test]
fn captured_checkpoint_is_bound_to_its_store_instance() {
    let tmp = test_util::tempdir();
    let first_store =
        FileControlPlaneStore::with_checkpoint_limits(tmp.path().join("first.state"), 1, u64::MAX);
    let mut first = SingleAuthorityControlPlane::open(first_store).unwrap();
    first
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint = first
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .unwrap();
    let SingleAuthorityDurableCheckpoint {
        capture, snapshot, ..
    } = checkpoint;
    let second_store = FileControlPlaneStore::new(tmp.path().join("second.state"));
    SingleAuthorityControlPlane::open(second_store.clone()).unwrap();
    let checkpoint_before = std::fs::read(second_store.path()).unwrap();
    let journal_before = std::fs::read(second_store.journal_path()).unwrap();

    let error = second_store
        .persist_captured_checkpoint(capture, &snapshot)
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("belongs to another store instance")
    ));
    assert_eq!(
        std::fs::read(second_store.path()).unwrap(),
        checkpoint_before
    );
    assert_eq!(
        std::fs::read(second_store.journal_path()).unwrap(),
        journal_before
    );
    second_store.ensure_healthy().unwrap();
}

#[test]
fn captured_checkpoint_persists_without_authority_mutex() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let authority = Arc::new(Mutex::new(
        SingleAuthorityControlPlane::open(store).unwrap(),
    ));
    let checkpoint = {
        let mut authority = authority.lock().unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        authority
            .capture_durable_checkpoint_if_due(Instant::now())
            .unwrap()
            .unwrap()
    };
    let authority_guard = authority.lock().unwrap();
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        completed_tx.send(checkpoint.persist()).unwrap();
    });

    completed_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("checkpoint persistence must not wait for the authority mutex")
        .unwrap();
    drop(authority_guard);
    worker.join().unwrap();
}

#[test]
fn file_backed_authority_checkpoint_compaction_reports_physical_io() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    let before = observability::control_plane_journal_metrics_snapshot();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due")
        .persist()
        .unwrap();

    let after = observability::control_plane_journal_metrics_snapshot();
    assert!(after.compaction_total > before.compaction_total);
    assert!(after.compaction_us_total >= before.compaction_us_total);
    assert!(after.compaction_lock_wait_us_total >= before.compaction_lock_wait_us_total);
    assert!(after.compaction_bytes_total > before.compaction_bytes_total);
    assert!(after.compaction_bytes_last > 0);
    assert!(after.compaction_file_sync_total > before.compaction_file_sync_total);
    assert!(after.compaction_directory_sync_total > before.compaction_directory_sync_total);
}

#[test]
fn file_backed_authority_recovers_checkpoint_anchor_before_snapshot_publication() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::with_checkpoint_limits(path.clone(), 1, u64::MAX);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    store.fail_next_checkpoint_after_anchor();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let error = authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due")
        .persist()
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "publish prepared single-authority control-plane checkpoint"
    ));
    assert!(single_authority_snapshot_tmp_path(&path).exists());
    drop(authority);
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
    assert!(!single_authority_snapshot_tmp_path(&path).exists());
}

#[test]
fn file_backed_authority_recovers_anchor_file_sync_before_directory_sync() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let previous = authority.snapshot().clone();
    let applied = previous
        .clone()
        .apply_control_plane_command(ControlPlaneCommand::SetNodeMembership {
            node_id: NodeId::new(1),
            membership: NodeMembershipState::Active,
        })
        .unwrap();
    let mut next = applied.into_snapshot();
    next.record_history_from(&previous);
    store.fail_next_journal_directory_sync();

    let error = store.checkpoint(Some(&previous), &next).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "sync single-authority control-plane journal directory"
    ));
    assert!(single_authority_snapshot_tmp_path(&path).exists());
    drop(authority);
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
}

#[test]
fn file_backed_authority_recovers_initial_identity_creation_interruption() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    store.fail_next_initial_checkpoint_after_identity();

    let error = SingleAuthorityControlPlane::open(store.clone()).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "initialize single-authority control-plane checkpoint"
    ));
    assert!(single_authority_identity_path(&path).exists());
    assert!(!path.exists());
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(restarted.snapshot().nodes().count(), 0);
    assert!(single_authority_initialized_path(&path).exists());
}

#[test]
fn file_backed_authority_recovers_initial_prepared_snapshot_interruption() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    store.fail_next_checkpoint_after_prepared_snapshot_sync();

    let error = SingleAuthorityControlPlane::open(store.clone()).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "anchor prepared single-authority control-plane checkpoint"
    ));
    assert!(single_authority_snapshot_tmp_path(&path).exists());
    assert!(!store.journal_path().exists());
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(restarted.snapshot().nodes().count(), 0);
    assert!(single_authority_initialized_path(&path).exists());
}

#[test]
fn file_backed_authority_recovers_torn_first_journal_creation() {
    for shape in ["empty", "truncated-header", "torn-first-frame"] {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{shape}.state"));
        let store = FileControlPlaneStore::new(&path);
        store.fail_next_checkpoint_after_prepared_snapshot_sync();
        SingleAuthorityControlPlane::open(store.clone()).unwrap_err();
        let prepared = std::fs::read_to_string(single_authority_snapshot_tmp_path(&path)).unwrap();
        let snapshot_digest = checksum::crc64::checksum(prepared.as_bytes());
        let binding = load_single_authority_clock_checkpoint_binding(&path)
            .unwrap()
            .unwrap();
        let anchor = SingleAuthorityJournalRecord {
            binding,
            previous_chain_digest: snapshot_digest,
            resulting_chain_digest: snapshot_digest,
            command: None,
        }
        .encode()
        .unwrap();
        store.journal.append_frame(&anchor).unwrap();
        let offsets = store.journal.status_offsets().unwrap();
        let physical_len = std::fs::metadata(store.journal_path()).unwrap().len();
        let header_len = physical_len - offsets.clean_len;
        let truncated_len = match shape {
            "empty" => 0,
            "truncated-header" => header_len - 1,
            "torn-first-frame" => physical_len - 1,
            _ => unreachable!(),
        };
        std::fs::OpenOptions::new()
            .write(true)
            .open(store.journal_path())
            .unwrap()
            .set_len(truncated_len)
            .unwrap();

        let restarted =
            SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
        assert_eq!(restarted.snapshot().nodes().count(), 0);
        assert!(single_authority_initialized_path(&path).exists());
    }
}

#[test]
fn file_backed_authority_rejects_torn_established_first_journal_record() {
    for shape in ["empty", "truncated-header", "torn-first-frame"] {
        let tmp = test_util::tempdir();
        let path = tmp.path().join(format!("control-plane-{shape}.state"));
        let store = FileControlPlaneStore::new(&path);
        SingleAuthorityControlPlane::open(store.clone()).unwrap();
        let offsets = store.journal.status_offsets().unwrap();
        let physical_len = std::fs::metadata(store.journal_path()).unwrap().len();
        let header_len = physical_len - offsets.clean_len;
        let truncated_len = match shape {
            "empty" => 0,
            "truncated-header" => header_len - 1,
            "torn-first-frame" => physical_len - 1,
            _ => unreachable!(),
        };
        std::fs::OpenOptions::new()
            .write(true)
            .open(store.journal_path())
            .unwrap()
            .set_len(truncated_len)
            .unwrap();

        assert!(
            FileControlPlaneStore::new(&path).load().is_err(),
            "established {shape} journal must fail closed"
        );
    }
}

#[test]
fn file_backed_authority_recovers_initial_anchor_before_snapshot_publication() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    store.fail_next_checkpoint_after_anchor();

    let error = SingleAuthorityControlPlane::open(store).unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "publish prepared single-authority control-plane checkpoint"
    ));
    assert!(!path.exists());
    assert!(single_authority_snapshot_tmp_path(&path).exists());
    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(restarted.snapshot().nodes().count(), 0);
    assert!(!single_authority_snapshot_tmp_path(&path).exists());
}

#[test]
fn file_backed_authority_recovers_restart_bump_anchor_before_snapshot_publication() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let incarnation_before = authority.snapshot().authority_incarnation();
    drop(authority);

    let failing_store = FileControlPlaneStore::new(&path);
    failing_store.fail_next_checkpoint_after_anchor();
    let error = SingleAuthorityControlPlane::open(failing_store).unwrap_err();
    assert!(matches!(
        error,
        ControlPlaneError::Io { diagnostic } if diagnostic.context() == "publish prepared single-authority control-plane checkpoint"
    ));

    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert!(
        restarted.snapshot().authority_incarnation() > incarnation_before,
        "restart must recover and advance beyond the prepared incarnation"
    );
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
}

#[test]
fn file_backed_authority_truncates_torn_journal_tail_after_replay() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let clean_len = store.journal.clean_len().unwrap();
    let physical_len_before = std::fs::metadata(store.journal_path()).unwrap().len();
    std::fs::OpenOptions::new()
        .append(true)
        .open(store.journal_path())
        .unwrap()
        .write_all(&[0, 0])
        .unwrap();

    let replayed = store.load().unwrap().unwrap();

    assert_eq!(
        replayed.node(NodeId::new(1)).unwrap().membership(),
        NodeMembershipState::Active
    );
    assert_eq!(store.journal.clean_len().unwrap(), clean_len);
    assert_eq!(
        std::fs::metadata(store.journal_path()).unwrap().len(),
        physical_len_before
    );
}

#[test]
fn file_backed_authority_rejects_missing_or_empty_journal_after_acknowledged_command() {
    for missing in [true, false] {
        let tmp = test_util::tempdir();
        let store =
            FileControlPlaneStore::new(tmp.path().join(format!("control-plane-{missing}.state")));
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        if missing {
            std::fs::remove_file(store.journal_path()).unwrap();
        } else {
            std::fs::File::create(store.journal_path()).unwrap();
        }

        let error = FileControlPlaneStore::new(store.path()).load().unwrap_err();

        assert!(
            matches!(
                error,
                ControlPlaneError::CommandDecode { ref message }
                    if message.contains("has no identity-bound checkpoint anchor")
            ),
            "unexpected recovery error: {error:?}"
        );
    }
}

#[test]
fn file_backed_authority_rejects_missing_established_checkpoint_and_journal() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    SingleAuthorityControlPlane::open(store.clone()).unwrap();
    assert!(single_authority_initialized_path(&path).exists());
    std::fs::remove_file(&path).unwrap();
    std::fs::remove_file(store.journal_path()).unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(&path).load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("without its control-plane checkpoint")
    ));
}

#[test]
fn file_backed_authority_rejects_foreign_identity_journal() {
    let tmp = test_util::tempdir();
    let first = FileControlPlaneStore::new(tmp.path().join("first.state"));
    let second = FileControlPlaneStore::new(tmp.path().join("second.state"));
    let mut first_authority = SingleAuthorityControlPlane::open(first.clone()).unwrap();
    SingleAuthorityControlPlane::open(second.clone()).unwrap();
    first_authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    std::fs::copy(first.journal_path(), second.journal_path()).unwrap();

    assert!(matches!(
        second.load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("journal identity does not match durable state")
    ));
}

#[test]
fn file_backed_authority_rejects_checksum_valid_discontinuous_command_chain() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let binding = load_single_authority_clock_checkpoint_binding(store.path())
        .unwrap()
        .unwrap();
    let command = ControlPlaneCommand::SetNodeMembership {
        node_id: NodeId::new(2),
        membership: NodeMembershipState::Active,
    };
    let encoded_command = encode_control_plane_command(&command).unwrap();
    let published_chain_digest = store
        .lock_durability()
        .unwrap()
        .published_chain_digest
        .unwrap();
    let wrong_previous_chain_digest = published_chain_digest ^ 1;
    let record = SingleAuthorityJournalRecord {
        binding,
        previous_chain_digest: wrong_previous_chain_digest,
        resulting_chain_digest: single_authority_command_chain_digest(
            wrong_previous_chain_digest,
            &encoded_command,
        ),
        command: Some(command),
    };
    store
        .journal
        .append_frame(&record.encode().unwrap())
        .unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(store.path()).load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("command chain is discontinuous")
    ));
}

#[test]
fn file_backed_authority_rejects_complete_interior_command_omission() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    let offsets = store.journal.status_offsets().unwrap();
    let frames = store
        .journal
        .read_frames_from(offsets.base_offset)
        .unwrap()
        .frames;
    assert_eq!(frames.len(), 3);
    std::fs::remove_file(store.journal_path()).unwrap();
    store.journal.append_frame(&frames[0]).unwrap();
    store.journal.append_frame(&frames[2]).unwrap();

    assert!(matches!(
        FileControlPlaneStore::new(&path).load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("command chain is discontinuous")
    ));
}

#[test]
fn file_backed_authority_rejects_checkpoint_off_retained_journal_chain() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::with_checkpoint_limits(
        tmp.path().join("control-plane.state"),
        1,
        u64::MAX,
    );
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let initial_checkpoint = std::fs::read(store.path()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    authority
        .capture_durable_checkpoint_if_due(Instant::now())
        .unwrap()
        .expect("command threshold should be due")
        .persist()
        .unwrap();
    std::fs::write(store.path(), initial_checkpoint).unwrap();

    assert!(matches!(
        store.load(),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("has no identity-bound checkpoint anchor for the durable snapshot")
    ));
}

#[test]
fn file_backed_authority_rejects_stale_checkpoint_before_mutation() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let stale_snapshot = authority.snapshot().clone();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    let checkpoint_before = std::fs::read(store.path()).unwrap();
    let journal_before = std::fs::read(store.journal_path()).unwrap();

    let error = store
        .checkpoint(Some(&stale_snapshot), &stale_snapshot)
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("checkpoint base does not match")
    ));
    assert_eq!(std::fs::read(store.path()).unwrap(), checkpoint_before);
    assert_eq!(std::fs::read(store.journal_path()).unwrap(), journal_before);
}

#[test]
fn file_backed_authority_poisoned_by_ambiguous_journal_append_stops_serving() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    store.fail_next_journal_file_sync();

    let error = authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap_err();

    assert!(matches!(
        error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("durability poisoned after ambiguous journal append")
    ));
    assert!(authority.snapshot().node(NodeId::new(1)).is_none());
    assert!(matches!(
        authority.runtime_map_snapshot(1),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("durability is poisoned")
    ));

    let restarted = SingleAuthorityControlPlane::open(FileControlPlaneStore::new(&path)).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .membership(),
        NodeMembershipState::Active
    );
}

#[test]
fn file_backed_authority_rejects_active_pg_observation_with_pending_metadata_command() {
    let tmp = test_util::tempdir();
    let path = tmp.path().join("control-plane.state");
    std::fs::write(
        &path,
        concat!(
            "version=27\ninitial_topology=-\n",
            "max_committed_timestamp_ms=-\nlease_grant_horizon=-\n",
            "authority_incarnation=1\n",
            "cluster_epoch=2\n",
            "node=1,active,1,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
            "node_pg=1,7,active,2,100,9,10,11,2,1,1\n",
            "pg=7,active,1,1,9,10,11,-,-,-,-,-,-,-,-,-,-,-,-,0,0,-,0,2,-,0,-,-,-,-,0,-\n",
        ),
    )
    .unwrap();
    let store = FileControlPlaneStore::new(path);
    assert!(matches!(
        SingleAuthorityControlPlane::open(store),
        Err(ControlPlaneError::Parse { message, .. })
            if message == "active node PG observation must not have pending metadata command"
    ));
}
