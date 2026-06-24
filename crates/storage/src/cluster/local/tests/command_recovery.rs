use super::*;

#[test]
fn metadata_command_recovery_single_flight_waits_for_matching_command() {
    let runtime_state = Arc::new(LocalClusterRuntimeState::new());
    let pg_id = PgId::new(1);
    let bucket = BucketName::new("single-flight-pending-command").unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    let recovery = runtime_state.join_metadata_command_recovery(pg_id, &command);
    let MetadataCommandRecoveryAdmission::Leader(leader_guard) = recovery else {
        panic!("first recovery caller should lead the single-flight");
    };

    let waiter_state = Arc::clone(&runtime_state);
    let waiter_command = command.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    let waiter = thread::spawn(move || {
        let admission = waiter_state.join_metadata_command_recovery(pg_id, &waiter_command);
        tx.send(matches!(
            admission,
            MetadataCommandRecoveryAdmission::Waited { .. }
        ))
        .unwrap();
    });

    assert!(
        rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "second recovery caller should wait while the leader is active"
    );
    drop(leader_guard);
    assert!(
        rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        "second recovery caller should return as a waiter after the leader finishes"
    );
    waiter.join().unwrap();
}

#[test]
fn metadata_command_recovery_single_flight_wait_is_bounded() {
    let runtime_state = Arc::new(LocalClusterRuntimeState::new());
    let pg_id = PgId::new(1);
    let bucket = BucketName::new("single-flight-timeout-pending-command").unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    let recovery = runtime_state.join_metadata_command_recovery(pg_id, &command);
    let MetadataCommandRecoveryAdmission::Leader(_leader_guard) = recovery else {
        panic!("first recovery caller should lead the single-flight");
    };

    let timed_out = runtime_state.join_metadata_command_recovery(pg_id, &command);
    assert!(
        matches!(timed_out, MetadataCommandRecoveryAdmission::TimedOut { .. }),
        "waiter should return a bounded timeout while the leader remains active"
    );
}

#[test]
fn stale_duplicate_metadata_command_index_is_reissued_before_apply() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "duplicate-index-first-");
    let second_bucket = bucket_for_pg(topology, 1, "duplicate-index-second-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let applied = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &applied)
        .unwrap();

    let stale_duplicate = create_bucket_metadata_command(pg_id, 1, second_bucket.clone());
    force_insert_pending_metadata_command_for_test(&map, pg_id, &second_bucket, &stale_duplicate);

    create_test_bucket(&cluster, &second_bucket);

    assert!(pending_metadata_command_for_test(&map, pg_id, &second_bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        assert_eq!(
            pg.max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap(),
            2
        );
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn pending_slot_drain_records_diagnostic_action() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "pending-drain-diagnostic-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _trace = observability::AttachedTrace::new(observability::TraceContext::from_ids(
        "trace-pending-slot-drain".to_string(),
        "request-pending-slot-drain".to_string(),
    ));

    cluster
        .drain_pending_object_metadata_commands_for_bucket_collect(pg_id, &bucket)
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
    }
    let records = observability::flight_recorder_snapshot();
    let matching: Vec<_> = records
        .iter()
        .filter(|record| {
            record.request_id == "request-pending-slot-drain"
                && record.event == "metadata_command_pending_slot_action"
        })
        .collect();
    assert_eq!(matching.len(), 1);
    let record = matching[0];
    assert!(record.detail.contains("node_id=1"));
    assert!(record.detail.contains("pg_id=1"));
    assert!(record.detail.contains("log_index=1"));
    assert!(record.detail.contains("action=drain_attempt"));
    assert!(record.detail.contains("command_kind=CreateBucket"));
    assert!(!record.detail.contains(bucket.as_str()));
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn fresh_bucket_pg_command_finish_does_not_record_drain_attempt() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "fresh-pending-finish-diagnostic-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _trace = observability::AttachedTrace::new(observability::TraceContext::from_ids(
        "trace-fresh-pending-finish".to_string(),
        "request-fresh-pending-finish".to_string(),
    ));

    let outcome = cluster
        .finish_pending_metadata_command_to_acting_set(pg_id, &bucket, &command, false)
        .unwrap();
    assert_eq!(outcome, PendingMetadataCommandOutcome::Applied);

    let records = observability::flight_recorder_snapshot();
    assert!(
        !records.iter().any(|record| {
            record.request_id == "request-fresh-pending-finish"
                && record.event == "metadata_command_pending_slot_action"
        }),
        "freshly installed bucket commands must not be labelled as drains"
    );
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn direct_bucket_pg_pending_finish_records_diagnostic_action() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-pending-finish-diagnostic-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _trace = observability::AttachedTrace::new(observability::TraceContext::from_ids(
        "trace-direct-pending-finish".to_string(),
        "request-direct-pending-finish".to_string(),
    ));

    let outcome = cluster
        .drain_bucket_pg_pending_metadata_command(pg_id, &bucket, &command, false)
        .unwrap();
    assert_eq!(outcome, PendingMetadataCommandOutcome::Applied);

    let records = observability::flight_recorder_snapshot();
    let matching: Vec<_> = records
        .iter()
        .filter(|record| {
            record.request_id == "request-direct-pending-finish"
                && record.event == "metadata_command_pending_slot_action"
        })
        .collect();
    assert_eq!(matching.len(), 1);
    let record = matching[0];
    assert!(record.detail.contains("node_id=1"));
    assert!(record.detail.contains("pg_id=1"));
    assert!(record.detail.contains("log_index=1"));
    assert!(record.detail.contains("action=drain_attempt"));
    assert!(record.detail.contains("command_kind=CreateBucket"));
    assert!(!record.detail.contains(bucket.as_str()));
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn bucket_pg_pending_slot_helper_records_diagnostic_action() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "helper-pending-drain-diagnostic-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let _trace = observability::AttachedTrace::new(observability::TraceContext::from_ids(
        "trace-helper-pending-drain".to_string(),
        "request-helper-pending-drain".to_string(),
    ));

    cluster
        .drain_pending_metadata_command_pg_slot(pg_id, &bucket, &command)
        .unwrap();

    let records = observability::flight_recorder_snapshot();
    let matching: Vec<_> = records
        .iter()
        .filter(|record| {
            record.request_id == "request-helper-pending-drain"
                && record.event == "metadata_command_pending_slot_action"
        })
        .collect();
    assert_eq!(matching.len(), 1);
    let record = matching[0];
    assert!(record.detail.contains("node_id=1"));
    assert!(record.detail.contains("pg_id=1"));
    assert!(record.detail.contains("log_index=1"));
    assert!(record.detail.contains("action=drain_attempt"));
    assert!(record.detail.contains("command_kind=CreateBucket"));
    assert!(!record.detail.contains(bucket.as_str()));
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn recovery_waiter_preserves_exact_applied_object_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "collect-waiter-stream-create-");
    let key = key_for_object_pg(topology, &bucket, 1, "stream-key-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key,
            crate::VersionId::from_u64(1),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.record_metadata_command_applied(node_id.as_u32(), &command)
            .unwrap();
    }
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap();
    assert!(primary_pg
        .remove_pending_metadata_command_slot(NodeId::new(1).as_u32(), &command)
        .unwrap());
    drop(primary_pg);

    let outcome = cluster
        .pending_command_recovery_waiter_outcome(pg_id, &command)
        .unwrap();
    assert_eq!(
        outcome.pending_outcome(),
        Some(PendingMetadataCommandOutcome::Applied)
    );
    assert!(
        crate::StorageCluster::metadata_command_recovery_applied_collectable_object_command(
            &command,
            outcome.pending_outcome().unwrap()
        ),
        "collect drains must preserve exact applied object commands for idempotent recovery"
    );
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn recovery_waiter_distinguishes_missing_and_replaced_unapplied_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "collect-waiter-missing-");
    let key = key_for_object_pg(topology, &bucket, 1, "stream-key-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let missing_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            crate::VersionId::from_u64(1),
        )),
    );

    let missing_outcome = cluster
        .pending_command_recovery_waiter_outcome(pg_id, &missing_command)
        .unwrap();
    assert_eq!(
        missing_outcome,
        MetadataCommandRecoveryWaiterOutcome::MissingNotApplied
    );
    assert_eq!(
        missing_outcome.pending_outcome(),
        Some(PendingMetadataCommandOutcome::RetryPartialExactConflict)
    );

    let replacement_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key,
            crate::VersionId::from_u64(2),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &replacement_command);

    let replaced_outcome = cluster
        .pending_command_recovery_waiter_outcome(pg_id, &missing_command)
        .unwrap();
    assert_eq!(
        replaced_outcome,
        MetadataCommandRecoveryWaiterOutcome::ReplacedNotApplied
    );
    assert_eq!(
        replaced_outcome.pending_outcome(),
        Some(PendingMetadataCommandOutcome::RetryPartialExactConflict)
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.record_metadata_command_applied(node_id.as_u32(), &replacement_command)
            .unwrap();
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(pg_id.get()).unwrap();
        assert!(pg
            .remove_pending_metadata_command_slot(NodeId::new(1).as_u32(), &replacement_command,)
            .unwrap());
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn recovery_waiter_drain_treats_missing_unapplied_command_as_abandoned() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "drain-waiter-missing-");
    let key = key_for_object_pg(topology, &bucket, 1, "stream-key-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key,
            crate::SessionId::try_from("71".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    let MetadataCommandRecoveryAdmission::Leader(leader_guard) = map
        .runtime_state()
        .join_metadata_command_recovery(pg_id, &command)
    else {
        panic!("first recovery caller should lead the single-flight");
    };

    let waiter_cluster = cluster.clone();
    let waiter_command = command.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (outcome_tx, outcome_rx) = std::sync::mpsc::channel();
    let waiter = thread::spawn(move || {
        started_tx.send(()).unwrap();
        outcome_tx
            .send(
                waiter_cluster
                    .drain_pending_metadata_command_with_recovery_gate(pg_id, &waiter_command)
                    .unwrap(),
            )
            .unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        outcome_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "waiter should block while another request owns command recovery"
    );

    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    pg.connection()
        .execute(
            "DELETE FROM metadata_command_pending_slot WHERE singleton = 0",
            [],
        )
        .unwrap();
    drop(pg);
    drop(leader_guard);

    let outcome = outcome_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(outcome, PendingMetadataCommandOutcome::Abandoned);
    waiter.join().unwrap();
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn reissue_accepts_terminal_pending_command_after_stale_primary_max_snapshot() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let topology = primary.storage_node().pg_topology();
    let bucket = bucket_for_pg(topology, pg_id.get(), "terminal-pending-reissue-");
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(primary.node_id(), &command)
        .unwrap();
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let reloaded = cluster
        .matching_reissued_pending_command_if_safe(
            pg_id,
            primary.node_id(),
            primary.metadata_command_client().as_ref(),
            0,
            command.id().log_index().get(),
            &command,
            command.clone(),
        )
        .unwrap();

    assert_eq!(reloaded.as_ref(), Some(&command));
}

#[test]
fn terminal_pending_reissue_rejects_different_stale_payload() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let topology = primary.storage_node().pg_topology();
    let current_bucket = bucket_for_pg(topology, pg_id.get(), "terminal-current-");
    let stale_bucket = bucket_for_pg(topology, pg_id.get(), "terminal-stale-");
    let current = create_bucket_metadata_command(pg_id, 1, current_bucket.clone());
    let stale = create_bucket_metadata_command(pg_id, 1, stale_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(primary.node_id(), &current)
        .unwrap();
    insert_pending_metadata_command_for_test(&map, pg_id, &current_bucket, &current);

    let reloaded = cluster
        .matching_reissued_pending_command_if_safe(
            pg_id,
            primary.node_id(),
            primary.metadata_command_client().as_ref(),
            0,
            current.id().log_index().get(),
            &stale,
            current,
        )
        .unwrap();

    assert_eq!(reloaded, None);
}

#[test]
fn terminal_pending_reissue_rejects_divergent_replica_prefix() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let current_bucket = bucket_for_pg(topology, 1, "terminal-divergent-current-");
    let divergent_bucket = bucket_for_pg(topology, 1, "terminal-divergent-other-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let current = create_bucket_metadata_command(pg_id, 1, current_bucket.clone());
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    primary_pg
        .apply_metadata_command_and_record(NodeId::new(1).as_u32(), &current)
        .unwrap();
    drop(primary_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &current_bucket, &current);

    let divergent = create_bucket_metadata_command(pg_id, 1, divergent_bucket);
    let non_primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    non_primary_pg
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &divergent)
        .unwrap();
    drop(non_primary_pg);

    let err = cluster
        .matching_reissued_pending_command_if_safe(
            pg_id,
            NodeId::new(1),
            map.node(NodeId::new(1))
                .unwrap()
                .metadata_command_client()
                .as_ref(),
            0,
            current.id().log_index().get(),
            &current,
            current.clone(),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 1,
            ..
        }
    ));
}

#[test]
fn terminal_pending_reissue_rejects_live_replica_ahead_of_stale_max() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let current_bucket = bucket_for_pg(topology, 1, "terminal-ahead-current-");
    let tail_bucket = bucket_for_pg(topology, 1, "terminal-ahead-tail-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let current = create_bucket_metadata_command(pg_id, 1, current_bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &current)
        .unwrap();
    insert_pending_metadata_command_for_test(&map, pg_id, &current_bucket, &current);

    let tail = create_bucket_metadata_command(pg_id, 2, tail_bucket);
    let non_primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    non_primary_pg
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &tail)
        .unwrap();
    drop(non_primary_pg);

    let err = cluster
        .matching_reissued_pending_command_if_safe(
            pg_id,
            NodeId::new(1),
            map.node(NodeId::new(1))
                .unwrap()
                .metadata_command_client()
                .as_ref(),
            0,
            current.id().log_index().get(),
            &current,
            current.clone(),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 2,
            ..
        }
    ));
}

#[test]
fn stale_duplicate_metadata_command_index_on_non_primary_fails_closed_without_dropping_slot() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "duplicate-nonprimary-first-");
    let second_bucket = bucket_for_pg(topology, 1, "duplicate-nonprimary-second-");
    let occupant_bucket = bucket_for_pg(topology, 1, "duplicate-nonprimary-occupant-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let applied = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &applied)
        .unwrap();

    let stale_duplicate = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    cluster
        .try_set_pending_metadata_command_for_bucket(pg_id, &second_bucket, &stale_duplicate)
        .unwrap()
        .unwrap();
    let occupant = create_bucket_metadata_command(pg_id, 2, occupant_bucket.clone());
    let non_primary = map.node(NodeId::new(0)).unwrap().storage_node();
    non_primary
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &occupant)
        .unwrap();

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let err = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: second_bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 2,
            ..
        })
    ));

    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &second_bucket)
            .as_ref()
            .map(MetadataCommandEnvelope::id),
        Some(stale_duplicate.id())
    );
    {
        let primary_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let slot = primary_pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .unwrap();
        assert_eq!(slot.id, stale_duplicate.id());
    }
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        if node_id == NodeId::new(1) {
            crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        } else {
            assert!(crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).is_err());
        }
        if node_id == NodeId::new(0) {
            crate::PgMetadataStore::head_bucket(&*pg, &occupant_bucket).unwrap();
        } else {
            assert!(crate::PgMetadataStore::head_bucket(&*pg, &occupant_bucket).is_err());
        }
        assert_eq!(
            pg.max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap(),
            if node_id == NodeId::new(0) || node_id == NodeId::new(1) {
                2
            } else {
                1
            }
        );
    }
}

#[test]
fn stale_duplicate_reissue_reloads_replaced_slot_during_primary_last_fanout() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "duplicate-race-first-");
    let second_bucket = bucket_for_pg(topology, 1, "duplicate-race-second-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let applied = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &applied)
        .unwrap();

    let stale = create_bucket_metadata_command(pg_id, 1, second_bucket.clone());
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        stale.payload().clone(),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &second_bucket, &replacement);
    {
        let non_primary = map.node(NodeId::new(0)).unwrap().storage_node();
        non_primary
            .get_pg(1)
            .unwrap()
            .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &replacement)
            .unwrap();
    }

    let reissued = cluster
        .test_reissue_pending_metadata_command(pg_id, &stale)
        .unwrap()
        .expect("reissue should reload the replacement pending slot");
    assert_eq!(reissued, replacement);

    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &reissued)
        .unwrap();
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(1).unwrap();
        assert!(pg
            .remove_pending_metadata_command_slot(NodeId::new(1).as_u32(), &reissued)
            .unwrap());
    }

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        assert_eq!(
            pg.max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap(),
            2
        );
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn pending_slot_reissue_records_diagnostic_action() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "pending-reissue-first-");
    let second_bucket = bucket_for_pg(topology, 1, "pending-reissue-second-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let applied = create_bucket_metadata_command(pg_id, 1, first_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &applied)
        .unwrap();

    let stale = create_bucket_metadata_command(pg_id, 1, second_bucket.clone());
    force_insert_pending_metadata_command_for_test(&map, pg_id, &second_bucket, &stale);
    let _trace = observability::AttachedTrace::new(observability::TraceContext::from_ids(
        "trace-pending-slot-reissue".to_string(),
        "request-pending-slot-reissue".to_string(),
    ));

    let reissued = cluster
        .test_reissue_pending_metadata_command(pg_id, &stale)
        .unwrap()
        .expect("stale duplicate pending command should be reissued");
    assert_eq!(reissued.id().log_index().get(), 2);

    let records = observability::flight_recorder_snapshot();
    let matching: Vec<_> = records
        .iter()
        .filter(|record| {
            record.request_id == "request-pending-slot-reissue"
                && record.event == "metadata_command_pending_slot_action"
        })
        .collect();
    assert_eq!(matching.len(), 1);
    let record = matching[0];
    assert!(record.detail.contains("node_id=1"));
    assert!(record.detail.contains("pg_id=1"));
    assert!(record.detail.contains("log_index=1"));
    assert!(record.detail.contains("action=reissue_attempt"));
    assert!(record.detail.contains("command_kind=CreateBucket"));
    assert!(!record.detail.contains(second_bucket.as_str()));
}

#[test]
fn stale_duplicate_reissue_rejects_same_payload_replacement_over_divergent_gap() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "duplicate-gap-first-");
    let second_bucket = bucket_for_pg(topology, 1, "duplicate-gap-second-");
    let occupant_bucket = bucket_for_pg(topology, 1, "duplicate-gap-occupant-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let applied = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &applied)
        .unwrap();

    let stale = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(3).unwrap(),
        ),
        stale.payload().clone(),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &second_bucket, &replacement);
    let divergent = create_bucket_metadata_command(pg_id, 2, occupant_bucket.clone());
    let non_primary = map.node(NodeId::new(0)).unwrap().storage_node();
    non_primary
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &divergent)
        .unwrap();

    let err = cluster
        .test_reissue_pending_metadata_command(pg_id, &stale)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 3,
            ..
        })
    ));

    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &second_bucket)
            .as_ref()
            .map(MetadataCommandEnvelope::id),
        Some(replacement.id())
    );
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        assert!(crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).is_err());
        if node_id == NodeId::new(0) {
            crate::PgMetadataStore::head_bucket(&*pg, &occupant_bucket).unwrap();
        } else {
            assert!(crate::PgMetadataStore::head_bucket(&*pg, &occupant_bucket).is_err());
        }
    }
}

#[test]
fn stale_duplicate_reissue_rejects_same_payload_replacement_on_divergent_prefix() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let primary_bucket = bucket_for_pg(topology, 1, "duplicate-prefix-primary-");
    let replacement_bucket = bucket_for_pg(topology, 1, "duplicate-prefix-replacement-");
    let divergent_bucket = bucket_for_pg(topology, 1, "duplicate-prefix-divergent-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);

    let primary_prefix = create_bucket_metadata_command(pg_id, 1, primary_bucket.clone());
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    primary_pg
        .apply_metadata_command_and_record(NodeId::new(1).as_u32(), &primary_prefix)
        .unwrap();
    drop(primary_pg);

    let stale = create_bucket_metadata_command(pg_id, 1, replacement_bucket.clone());
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        stale.payload().clone(),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &replacement_bucket, &replacement);

    let divergent_prefix = create_bucket_metadata_command(pg_id, 1, divergent_bucket.clone());
    let non_primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    non_primary_pg
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &divergent_prefix)
        .unwrap();
    non_primary_pg
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &replacement)
        .unwrap();
    drop(non_primary_pg);

    let err = cluster
        .test_reissue_pending_metadata_command(pg_id, &stale)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 2,
            ..
        })
    ));

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    crate::PgMetadataStore::head_bucket(&*primary_pg, &primary_bucket).unwrap();
    assert!(crate::PgMetadataStore::head_bucket(&*primary_pg, &replacement_bucket).is_err());
    let non_primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    crate::PgMetadataStore::head_bucket(&*non_primary_pg, &divergent_bucket).unwrap();
    crate::PgMetadataStore::head_bucket(&*non_primary_pg, &replacement_bucket).unwrap();
}

#[test]
fn stale_duplicate_reissue_rejects_below_replacement_divergent_prefix() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let primary_bucket = bucket_for_pg(topology, 1, "duplicate-below-primary-");
    let replacement_bucket = bucket_for_pg(topology, 1, "duplicate-below-replacement-");
    let divergent_bucket = bucket_for_pg(topology, 1, "duplicate-below-divergent-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);

    let primary_prefix = create_bucket_metadata_command(pg_id, 1, primary_bucket.clone());
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    primary_pg
        .apply_metadata_command_and_record(NodeId::new(1).as_u32(), &primary_prefix)
        .unwrap();
    drop(primary_pg);

    let stale = create_bucket_metadata_command(pg_id, 1, replacement_bucket.clone());
    let replacement = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        stale.payload().clone(),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &replacement_bucket, &replacement);

    let divergent_prefix = create_bucket_metadata_command(pg_id, 1, divergent_bucket.clone());
    let non_primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    non_primary_pg
        .apply_metadata_command_and_record(NodeId::new(0).as_u32(), &divergent_prefix)
        .unwrap();
    drop(non_primary_pg);

    let err = cluster
        .test_reissue_pending_metadata_command(pg_id, &stale)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 1,
            ..
        })
    ));

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    crate::PgMetadataStore::head_bucket(&*primary_pg, &primary_bucket).unwrap();
    assert!(crate::PgMetadataStore::head_bucket(&*primary_pg, &replacement_bucket).is_err());
    let non_primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    crate::PgMetadataStore::head_bucket(&*non_primary_pg, &divergent_bucket).unwrap();
    assert!(crate::PgMetadataStore::head_bucket(&*non_primary_pg, &replacement_bucket).is_err());
}

#[test]
fn stale_duplicate_direct_put_commit_index_is_reissued_before_apply() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, object_pg, data_pg) = bucket_key_with_distinct_object_and_data_pg(topology);
    let occupant_bucket = bucket_for_pg(topology, object_pg, "duplicate-direct-occupant-");
    set_route_primary(&mut map, object_pg, NodeId::new(0));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation_id = crate::SessionId::try_from("64".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "stale-duplicate-direct-put-test",
            Some(key.as_str()),
        )
        .unwrap();
    let bucket_write_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);
    let payload = b"direct put duplicate index reissue";
    let segment_okh = [0x64; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
        bucket_write_proof.clone(),
    );
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(object_pg))
        .unwrap();
    let object_pg_store = primary.storage_node().get_pg(object_pg).unwrap();
    let stale_command = cluster
        .prepare_commit_direct_put_object_command(
            PgId::new(object_pg),
            &object_pg_store,
            &commit_req,
            crate::VersionId::Null,
            bucket_write_proof,
        )
        .unwrap();
    drop(object_pg_store);
    let duplicate_index = stale_command.id().log_index().get();
    let occupant =
        create_bucket_metadata_command(PgId::new(object_pg), duplicate_index, occupant_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &occupant)
        .unwrap();
    force_insert_pending_metadata_command_for_test(
        &map,
        PgId::new(object_pg),
        &bucket,
        &stale_command,
    );

    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_direct_put_metadata_on_acting_nodes(&map, &node_ids, object_pg, &commit_req, &outcome);
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert_eq!(
            pg.max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap(),
            duplicate_index + 1
        );
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn stale_duplicate_stream_append_index_is_reissued_before_apply() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let (bucket, key, object_pg, data_pg) = bucket_key_with_distinct_object_and_data_pg(topology);
    let occupant_bucket = bucket_for_pg(topology, object_pg, "duplicate-stream-occupant-");
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("65".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream append duplicate index reissue";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh: [0x65; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch: Vec<(&crate::ShardKey, crate::WriteAck)> = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    cluster
        .register_payload_shard_acks(segment.data_pg_id, &shard_batch)
        .unwrap();
    let stale_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(PgId::new(object_pg))
            .unwrap(),
        MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
            bucket: bucket.clone(),
            key: key.clone(),
            segment: segment.clone(),
        })),
    );
    let duplicate_index = stale_command.id().log_index().get();
    let occupant =
        create_bucket_metadata_command(PgId::new(object_pg), duplicate_index, occupant_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &occupant)
        .unwrap();
    force_insert_pending_metadata_command_for_test(
        &map,
        PgId::new(object_pg),
        &bucket,
        &stale_command,
    );

    cluster
        .apply_new_stream_append_command(
            PgId::new(object_pg),
            &bucket,
            &stale_command,
            &segment,
            &shard_batch,
        )
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
            vec![segment.clone()]
        );
        assert_eq!(
            pg.max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap(),
            duplicate_index + 1
        );
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn zero_apply_command_failure_records_tombstone_for_later_hash_chain_convergence() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "zero-apply-first-");
    let second_bucket = bucket_for_pg(topology, 1, "zero-apply-second-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let fail_once_hook = Arc::clone(&fail_once);
    let first_bucket_for_hook = first_bucket.clone();
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateBucket(create)
                    if create.bucket.name == first_bucket_for_hook
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected zero-apply create bucket failure",
                        source: std::io::Error::other("injected zero-apply create bucket failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let err = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: first_bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected zero-apply create bucket failure",
                ..
            })
        ),
        "expected injected zero-apply failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &first_bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 1);
        assert!(matches!(
            crate::PgMetadataStore::head_bucket(&*pg, &first_bucket),
            Err(crate::MetadataError::BucketNotFound { .. })
        ));
    }

    create_test_bucket(&cluster, &second_bucket);
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 2);
        let second = crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        assert_eq!(second.name, second_bucket);
    }

    create_test_bucket(&cluster, &first_bucket);
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &first_bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 3);
        let first = crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        assert_eq!(first.name, first_bucket);
        let second = crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        assert_eq!(second.name, second_bucket);
    }
}

#[test]
fn peering_reconstruction_gather_accepts_converged_acting_set() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "peering-converged-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &command)
        .unwrap();

    let state = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    let proof = crate::control_plane::PgMetadataProof::new(
        state.applied_log_index,
        state.applied_log_hash,
        state.state_digest,
    );

    let decision = cluster
        .reconstruct_pg_peering_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );
}

#[test]
fn peering_reconstruction_gather_allows_peering_route_state() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "peering-route-gather-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &command)
            .unwrap();
    }
    let state = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    let proof = crate::control_plane::PgMetadataProof::new(
        state.applied_log_index,
        state.applied_log_hash,
        state.state_digest,
    );

    let decision = cluster
        .reconstruct_pg_peering_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );
}

#[test]
fn peering_reconstruction_gather_uses_primary_retained_suffix_for_lagging_replica() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "peering-lagging-first-");
    let second_bucket = bucket_for_pg(topology, 1, "peering-lagging-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    let lagging_state = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    let primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    primary_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let primary_state = primary_pg.metadata_command_replica_state().unwrap();
    let proof = crate::control_plane::PgMetadataProof::new(
        primary_state.applied_log_index,
        primary_state.applied_log_hash,
        primary_state.state_digest,
    );
    drop(primary_pg);

    let decision = cluster
        .reconstruct_pg_peering_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::CatchUpRequired {
            proof,
            replicas: vec![
                crate::peering::PgPeeringReplicaCatchUp {
                    node_id: NodeId::new(1),
                    from_log_index: 1,
                    from_log_hash: lagging_state.applied_log_hash,
                    to_log_index: 2,
                    to_log_hash: primary_state.applied_log_hash,
                },
                crate::peering::PgPeeringReplicaCatchUp {
                    node_id: NodeId::new(2),
                    from_log_index: 1,
                    from_log_hash: lagging_state.applied_log_hash,
                    to_log_index: 2,
                    to_log_hash: primary_state.applied_log_hash,
                },
            ],
        }
    );
}

#[test]
fn peering_reconstruction_gather_fails_closed_when_replica_is_ahead_of_primary() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "peering-ahead-first-");
    let second_bucket = bucket_for_pg(topology, 1, "peering-ahead-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    map.node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(1, &second)
        .unwrap();

    let err = cluster
        .reconstruct_pg_peering_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::ReplicaAheadOfPrimary {
                node_id,
                replica_log_index: 2,
                primary_log_index: 1,
            }
        ) if node_id == NodeId::new(1)
    ));
}

#[test]
fn peering_reconstruction_gather_fails_closed_on_stale_replica_epoch() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "peering-stale-epoch-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &command)
            .unwrap();
    }

    let current_epoch = ClusterEpoch::new(2).unwrap();
    map.epoch = current_epoch;
    for route in map.pg_routes.values_mut() {
        route.cluster_epoch = current_epoch;
    }
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .reconstruct_pg_peering_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id,
                replica_epoch: ClusterEpoch::INITIAL,
                cluster_epoch,
            }
        ) if node_id == NodeId::new(0) && cluster_epoch == current_epoch
    ));
}

#[test]
fn peering_replay_catches_up_lagging_replicas_from_primary_retained_entries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "peering-replay-first-");
    let second_bucket = bucket_for_pg(topology, 1, "peering-replay-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    let primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    primary_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let primary_state = primary_pg.metadata_command_replica_state().unwrap();
    let proof = crate::control_plane::PgMetadataProof::new(
        primary_state.applied_log_index,
        primary_state.applied_log_hash,
        primary_state.state_digest,
    );
    drop(primary_pg);

    let decision = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );
    for node_id in node_ids {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state, primary_state);
    }
}

#[test]
fn peering_replay_catches_up_replicas_with_different_lag_distances() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let pg_id = PgId::new(1);
    let commands: Vec<_> = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (1..=4)
            .map(|log_index| {
                create_bucket_metadata_command(
                    pg_id,
                    log_index,
                    bucket_for_pg(
                        topology,
                        1,
                        &format!("peering-replay-mixed-lag-{log_index}-"),
                    ),
                )
            })
            .collect()
    };
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &commands[0])
            .unwrap();
    }
    for command in &commands[1..] {
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .apply_metadata_command_and_record(0, command)
            .unwrap();
    }
    for command in &commands[1..3] {
        map.node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .apply_metadata_command_and_record(1, command)
            .unwrap();
    }
    let primary_state = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    let proof = crate::control_plane::PgMetadataProof::new(
        primary_state.applied_log_index,
        primary_state.applied_log_hash,
        primary_state.state_digest,
    );

    let decision = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );
    for node_id in node_ids {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state, primary_state);
    }
}

#[test]
fn metadata_transfer_export_packages_authoritative_retained_log() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let source_state = source_pg.metadata_command_replica_state().unwrap();
    drop(source_pg);

    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(artifact.pg_id, pg_id);
    assert_eq!(artifact.source_node_id, NodeId::new(0));
    assert_eq!(artifact.cluster_epoch, ClusterEpoch::INITIAL);
    assert_eq!(
        artifact.proof,
        crate::control_plane::PgMetadataProof::new(
            source_state.applied_log_index,
            source_state.applied_log_hash,
            source_state.state_digest,
        )
    );
    assert_eq!(artifact.retained_log_entries.len(), 2);
    assert_eq!(artifact.retained_log_entries[0].log_index, 1);
    assert_eq!(artifact.retained_log_entries[0].previous_log_hash, 0);
    assert_eq!(
        artifact.retained_log_entries[1].previous_log_hash,
        artifact.retained_log_entries[0].log_hash
    );
}

#[test]
fn metadata_transfer_export_preserves_source_log_epoch_under_fenced_route() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-fenced-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-fenced-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    let checkpoint = source_pg
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let source_state = source_pg.metadata_command_replica_state().unwrap();
    drop(source_pg);

    let fenced_epoch = ClusterEpoch::new(2).unwrap();
    map.epoch = fenced_epoch;
    for route in map.pg_routes.values_mut() {
        route.cluster_epoch = fenced_epoch;
    }
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(cluster.operation_epoch(), fenced_epoch);
    assert_eq!(source_state.cluster_epoch, ClusterEpoch::INITIAL);
    assert_eq!(artifact.cluster_epoch, source_state.cluster_epoch);
    assert_eq!(
        artifact.proof.applied_log_index,
        source_state.applied_log_index
    );
    assert_eq!(artifact.proof.state_digest, source_state.state_digest);
    assert_eq!(
        artifact.proof.applied_log_hash,
        source_state.applied_log_hash
    );
    assert_eq!(artifact.retained_log_entries.len(), 2);
    assert!(artifact.retained_log_entries.iter().all(|entry| matches!(
        &entry.kind,
        crate::metadata_command::MetadataCommandLogRangeEntryKind::Applied(command)
            if command.id().cluster_epoch() == source_state.cluster_epoch
    )));

    let checkpoint_suffix_artifact = cluster
        .export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
            pg_id,
            NodeId::new(0),
            checkpoint,
        )
        .unwrap();
    assert_eq!(
        checkpoint_suffix_artifact.cluster_epoch,
        source_state.cluster_epoch
    );
    assert_eq!(checkpoint_suffix_artifact.retained_log_entries.len(), 1);
    assert!(checkpoint_suffix_artifact
        .retained_log_entries
        .iter()
        .all(|entry| matches!(
            &entry.kind,
            crate::metadata_command::MetadataCommandLogRangeEntryKind::Applied(command)
                if command.id().cluster_epoch() == source_state.cluster_epoch
        )));

    let checkpoint_artifact = cluster
        .export_pg_metadata_transfer_from_checkpoint(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        checkpoint_artifact.cluster_epoch,
        source_state.cluster_epoch
    );
    assert!(checkpoint_artifact.retained_log_entries.is_empty());
    assert_eq!(checkpoint_artifact.source_metadata_proof(), artifact.proof);
}

#[test]
fn metadata_transfer_export_rejects_active_route_without_fence() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-active-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &command)
        .unwrap();

    let err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::TransferSourceNotQuiesced {
                pg_id: err_pg_id,
                cluster_epoch,
                state: PgState::Active,
            }
        ) if err_pg_id == pg_id && cluster_epoch == ClusterEpoch::INITIAL
    ));
}

#[test]
fn metadata_transfer_export_rejects_non_primary_source() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-non-primary-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-non-primary-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    for node_id in node_ids {
        map.node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &second)
        .unwrap();

    let err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(1))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::TransferSourceNotPrimary {
                pg_id: err_pg_id,
                cluster_epoch,
                source_node,
                primary,
            }
        ) if err_pg_id == pg_id
            && cluster_epoch == ClusterEpoch::INITIAL
            && source_node == NodeId::new(1)
            && primary == NodeId::new(0)
    ));
}

#[test]
fn metadata_transfer_export_packages_retained_suffix_with_base_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-missing-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-missing-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    source_pg
        .connection()
        .execute(
            "DELETE FROM metadata_command_log WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            rusqlite::params![ClusterEpoch::INITIAL.get() as i64, pg_id.get() as i64, 1_i64],
        )
        .unwrap();
    drop(source_pg);

    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::RetainedLogPrefix
    );
    assert_eq!(artifact.base_proof.applied_log_index, 1);
    assert_eq!(
        artifact.base_proof.applied_log_hash,
        artifact.retained_log_entries[0].previous_log_hash
    );
    assert_eq!(
        Some(artifact.base_proof.state_digest),
        artifact.retained_log_entries[0].pre_state_digest
    );
    assert_eq!(artifact.retained_log_entries.len(), 1);
    assert_eq!(artifact.retained_log_entries[0].log_index, 2);
}

#[test]
fn metadata_transfer_import_rejects_suffix_into_empty_destination_without_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-empty-suffix-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-empty-suffix-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    source_pg
        .connection()
        .execute(
            "DELETE FROM metadata_command_log WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            rusqlite::params![ClusterEpoch::INITIAL.get() as i64, pg_id.get() as i64, 1_i64],
        )
        .unwrap();
    drop(source_pg);

    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::RetainedLogPrefix
    );
    assert_eq!(artifact.base_proof.applied_log_index, 1);
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::peering::PgPeeringReconstructionFailure::Reconstruction(
                crate::peering::PgPeeringReconstructionError::DirtyMetadataTransferDestination { .. }
            )
        ),
        "unexpected error: {err:?}"
    );

    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, ClusterEpoch::INITIAL);
        assert_eq!(state.applied_log_index, 0);
        assert_eq!(state.applied_log_hash, 0);
        assert!(crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).is_err());
    }
}

#[test]
fn metadata_transfer_export_fails_closed_when_retained_state_digest_chain_is_broken() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-digest-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-digest-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    source_pg
        .connection()
        .execute(
            "UPDATE metadata_command_log SET pre_state_digest = pre_state_digest + 1 WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            rusqlite::params![ClusterEpoch::INITIAL.get() as i64, pg_id.get() as i64, 2_i64],
        )
        .unwrap();
    drop(source_pg);

    let err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::RetainedCommandStateDigestFork {
                node_id,
                pg_id: err_pg_id,
                log_index: 2,
                ..
            }
        ) if node_id == NodeId::new(0) && err_pg_id == pg_id
    ));
}

#[test]
fn metadata_transfer_export_fails_closed_on_abandoned_retained_entry() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-abandoned-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .record_metadata_command_abandoned(0, &command)
        .unwrap();

    let err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::UnreplayableAbandonedCommandLogEntry {
                node_id,
                log_index: 1,
            }
        ) if node_id == NodeId::new(0)
    ));
}

#[test]
fn metadata_transfer_import_replays_rebased_artifact_to_peering_acting_set() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-import-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-import-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    drop(source_pg);
    let artifact = cluster
        .export_pg_metadata_transfer_artifact_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(artifact.pg_id(), pg_id);
    assert_eq!(artifact.source_node_id(), NodeId::new(0));
    assert_eq!(artifact.cluster_epoch(), cluster.operation_epoch());
    assert_eq!(artifact.source_metadata_proof(), artifact.proof);
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_artifact_from_retained_log(&artifact)
        .unwrap();
    let retried_proof = cluster
        .import_pg_metadata_transfer_artifact_from_retained_log(&artifact)
        .unwrap();

    assert_eq!(proof, expected_proof);
    assert_eq!(retried_proof, proof);
    assert_eq!(proof.applied_log_index, artifact.proof.applied_log_index);
    assert_eq!(proof.state_digest, artifact.proof.state_digest);
    assert_ne!(
        proof.applied_log_hash, artifact.proof.applied_log_hash,
        "rebased destination epoch must produce a distinct metadata log hash"
    );
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, proof.applied_log_index);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_retries_after_partial_destination_pending_failure() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-partial-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-partial-second-");
    let pending_bucket = bucket_for_pg(topology, 1, "metadata-transfer-partial-pending-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    drop(source_pg);
    let artifact = cluster
        .export_pg_metadata_transfer_artifact_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let pending = create_bucket_metadata_command_at_epoch(
        destination_epoch,
        pg_id,
        99,
        pending_bucket.clone(),
    );
    force_insert_pending_metadata_command_for_node_for_test(
        &map,
        NodeId::new(2),
        pg_id,
        &pending_bucket,
        &pending,
    );
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();

    let err = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::PendingMetadataCommand { node_id }
        ) if node_id == NodeId::new(2)
    ));

    let imported_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let imported_state = imported_pg.metadata_command_replica_state().unwrap();
    assert_eq!(imported_state.cluster_epoch, destination_epoch);
    assert_eq!(
        imported_state.applied_log_index,
        expected_proof.applied_log_index
    );
    assert_eq!(
        imported_state.applied_log_hash,
        expected_proof.applied_log_hash
    );
    assert_eq!(imported_state.state_digest, expected_proof.state_digest);
    crate::PgMetadataStore::head_bucket(&*imported_pg, &first_bucket).unwrap();
    crate::PgMetadataStore::head_bucket(&*imported_pg, &second_bucket).unwrap();
    drop(imported_pg);

    let blocked_pg = map
        .node(NodeId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let blocked_state = blocked_pg.metadata_command_replica_state().unwrap();
    assert_eq!(blocked_state.applied_log_index, 0);
    assert!(blocked_pg
        .pending_metadata_command_envelope(2, destination_epoch)
        .unwrap()
        .is_some());
    drop(blocked_pg);

    clear_pending_metadata_command_for_node_for_test(&map, NodeId::new(2), pg_id);
    let proof = cluster
        .import_pg_metadata_transfer_artifact_from_retained_log(&artifact)
        .unwrap();
    assert_eq!(proof, expected_proof);

    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, proof.applied_log_index);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_rejects_artifact_with_mismatched_final_state_digest() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-final-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-final-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    drop(source_pg);
    let mut artifact = cluster
        .export_pg_metadata_transfer_artifact_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    artifact
        .retained_log_entries
        .last_mut()
        .expect("test artifact should contain retained commands")
        .post_state_digest = Some(artifact.proof.state_digest.wrapping_add(1));

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .import_pg_metadata_transfer_artifact_from_retained_log(&artifact)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::PgMetadataTransferError::Reconstruction { .. }
        ),
        "unexpected error: {err:?}"
    );
    assert!(
        err.to_string().contains("state digest fork"),
        "unexpected error: {err}"
    );
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state.applied_log_index, 0);
        assert_eq!(state.applied_log_hash, 0);
    }
}

#[test]
fn metadata_transfer_import_rejects_unproven_prefix_zero_base_digest() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let source_first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-base-first-");
    let source_second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-base-second-");
    let unrelated_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-base-unrelated-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let source_first = create_bucket_metadata_command(pg_id, 1, source_first_bucket.clone());
    let source_second = create_bucket_metadata_command(pg_id, 2, source_second_bucket.clone());
    let unrelated = create_bucket_metadata_command(pg_id, 1, unrelated_bucket.clone());
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &source_first)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &source_second)
        .unwrap();
    drop(source_pg);
    let mut artifact = cluster
        .export_pg_metadata_transfer_artifact_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    let unrelated_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    unrelated_pg
        .apply_metadata_command_and_record(1, &unrelated)
        .unwrap();
    let unrelated_state = unrelated_pg.metadata_command_replica_state().unwrap();
    drop(unrelated_pg);
    drop(cluster);

    artifact
        .retained_log_entries
        .first_mut()
        .expect("test artifact should contain retained commands")
        .pre_state_digest = Some(unrelated_state.state_digest);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .import_pg_metadata_transfer_artifact_from_retained_log(&artifact)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::PgMetadataTransferError::Reconstruction { .. }
        ),
        "unexpected error: {err:?}"
    );
    assert!(
        err.to_string().contains("state digest fork"),
        "unexpected error: {err}"
    );
    let destination_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let destination_state = destination_pg.metadata_command_replica_state().unwrap();
    assert_eq!(destination_state, unrelated_state);
    assert!(crate::PgMetadataStore::head_bucket(&*destination_pg, &unrelated_bucket).is_ok());
    assert!(crate::PgMetadataStore::head_bucket(&*destination_pg, &source_first_bucket).is_err());
    assert!(crate::PgMetadataStore::head_bucket(&*destination_pg, &source_second_bucket).is_err());
}

#[test]
fn metadata_transfer_import_adopts_matching_existing_destination_state() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-adopt-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-adopt-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &second)
            .unwrap();
    }
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Empty
    );
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();

    assert_eq!(proof.applied_log_index, artifact.proof.applied_log_index);
    assert_eq!(proof.state_digest, artifact.proof.state_digest);
    assert_ne!(proof.applied_log_hash, artifact.proof.applied_log_hash);
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, proof.applied_log_index);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_replays_suffix_over_proven_older_prefix_state() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-prefix-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-prefix-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();
    assert_eq!(proof, expected_proof);

    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, 2);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_replays_retained_suffix_over_exact_source_base_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-suffix-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-suffix-second-");
    let third_bucket = bucket_for_pg(topology, 1, "metadata-transfer-suffix-third-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    let third = create_bucket_metadata_command(pg_id, 3, third_bucket.clone());

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &second)
            .unwrap();
    }
    let base_state = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &third)
        .unwrap();
    let mut artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    artifact.base_kind = crate::peering::PgMetadataTransferBaseKind::RetainedLogPrefix;
    artifact.base_proof = crate::control_plane::PgMetadataProof::new(
        base_state.applied_log_index,
        base_state.applied_log_hash,
        base_state.state_digest,
    );
    artifact
        .retained_log_entries
        .retain(|entry| entry.log_index > base_state.applied_log_index);
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();
    assert_eq!(proof, expected_proof);
    assert_eq!(proof.applied_log_index, 1);
    assert_ne!(proof.applied_log_hash, artifact.proof.applied_log_hash);

    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, proof.applied_log_index);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &third_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_rejects_older_prefix_with_unproven_log_hash() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-prefix-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-bad-prefix-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.connection()
            .execute(
                "UPDATE metadata_command_replica_state SET applied_log_hash = applied_log_hash + 1 WHERE singleton = 0",
                [],
            )
            .unwrap();
    }
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::peering::PgPeeringReconstructionFailure::Reconstruction(
                crate::peering::PgPeeringReconstructionError::DirtyMetadataTransferDestination { .. }
            )
        ),
        "unexpected error: {err:?}"
    );

    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, ClusterEpoch::INITIAL);
        assert_eq!(state.applied_log_index, 1);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        assert!(crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).is_err());
    }
}

#[test]
fn metadata_transfer_import_installs_checkpoint_base() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-base-");
    let pending_bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-base-pending-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket.clone());
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_checkpoint(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    assert!(artifact.checkpoint_base().is_some());
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();
    assert_eq!(proof, expected_proof);

    let pending = create_bucket_metadata_command_at_epoch(
        destination_epoch,
        pg_id,
        99,
        pending_bucket.clone(),
    );
    force_insert_pending_metadata_command_for_node_for_test(
        &map,
        NodeId::new(2),
        pg_id,
        &pending_bucket,
        &pending,
    );
    let err = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::PendingMetadataCommand { node_id }
        ) if node_id == NodeId::new(2)
    ));

    clear_pending_metadata_command_for_node_for_test(&map, NodeId::new(2), pg_id);
    let retry_proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    assert_eq!(retry_proof, expected_proof);

    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, 0);
        assert_eq!(state.applied_log_hash, 0);
        assert_eq!(state.state_digest, expected_proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_replays_retained_suffix_over_checkpoint_base() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-suffix-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-suffix-second-");
    let third_bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-suffix-third-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket.clone());
    let third = create_bucket_metadata_command(pg_id, 3, third_bucket.clone());
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    let checkpoint = source_pg
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    let checkpoint_proof = crate::control_plane::PgMetadataProof::new(
        checkpoint.applied_log_index,
        checkpoint.applied_log_hash,
        checkpoint.state_digest,
    );
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &third)
        .unwrap();
    drop(source_pg);
    let artifact = cluster
        .export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
            pg_id,
            NodeId::new(0),
            checkpoint.clone(),
        )
        .unwrap();
    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    assert_eq!(artifact.source_base_metadata_proof(), checkpoint_proof);
    assert_eq!(artifact.retained_log_entries.len(), 2);
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();
    let rebased_commands =
        crate::peering::rebase_pg_metadata_transfer_artifact_commands(&artifact, destination_epoch)
            .unwrap();
    let partial_client = map
        .node(NodeId::new(1))
        .unwrap()
        .metadata_command_client()
        .clone();
    partial_client
        .install_metadata_transfer_checkpoint_base(pg_id, destination_epoch, &checkpoint)
        .unwrap();
    partial_client
        .replay_metadata_command_for_peering(pg_id, &rebased_commands[0].command)
        .unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    let retry_proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();

    assert_eq!(proof, expected_proof);
    assert_eq!(retry_proof, expected_proof);
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, expected_proof.applied_log_index);
        assert_eq!(state.applied_log_hash, expected_proof.applied_log_hash);
        assert_eq!(state.state_digest, expected_proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &third_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_checkpoint_suffix_export_rejects_wrong_pg_checkpoint() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1, 2], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let pg_id = PgId::new(1);
    let checkpoint_pg_id = PgId::new(2);
    let bucket = bucket_for_pg(topology, 2, "metadata-transfer-wrong-checkpoint-pg-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    set_route_primary(&mut map, 2, NodeId::new(0));
    set_route_state(&mut map, 2, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let command = create_bucket_metadata_command(checkpoint_pg_id, 1, bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let checkpoint = source_pg
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();

    let err = cluster
        .export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
            pg_id,
            NodeId::new(0),
            checkpoint,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::MetadataTransferCheckpointProofMismatch {
                pg_id: err_pg_id,
                ..
            }
        ) if err_pg_id == pg_id
    ));
}

#[test]
fn metadata_transfer_checkpoint_suffix_export_rejects_checkpoint_ahead_of_source() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-source-prefix-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-ahead-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    drop(source_pg);
    let ahead_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    ahead_pg
        .apply_metadata_command_and_record(1, &first)
        .unwrap();
    ahead_pg
        .apply_metadata_command_and_record(1, &second)
        .unwrap();
    let checkpoint = ahead_pg
        .metadata_command_checkpoint(1, ClusterEpoch::INITIAL)
        .unwrap();

    let err = cluster
        .export_pg_metadata_transfer_from_checkpoint_and_retained_suffix(
            pg_id,
            NodeId::new(0),
            checkpoint,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::MetadataTransferCheckpointProofMismatch {
                pg_id: err_pg_id,
                ..
            }
        ) if err_pg_id == pg_id
    ));
}

#[test]
fn metadata_transfer_checkpoint_import_rejects_stale_nonempty_destination() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let source_bucket = bucket_for_pg(topology, 1, "metadata-transfer-stale-checkpoint-source-");
    let stale_bucket = bucket_for_pg(topology, 1, "metadata-transfer-stale-checkpoint-dirty-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let source_command = create_bucket_metadata_command(pg_id, 1, source_bucket);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &source_command)
        .unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_checkpoint(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let stale_command = create_bucket_metadata_command(pg_id, 1, stale_bucket);
    let stale_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    stale_pg
        .apply_metadata_command_and_record(1, &stale_command)
        .unwrap();
    let stale_state = stale_pg.metadata_command_replica_state().unwrap();
    drop(stale_pg);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap_err();

    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                node_id,
                pg_id: err_pg_id,
                applied_log_index,
                applied_log_hash,
                state_digest,
                ..
            }
        ) if node_id == NodeId::new(1)
            && err_pg_id == pg_id
            && applied_log_index == stale_state.applied_log_index
            && applied_log_hash == stale_state.applied_log_hash
            && state_digest == stale_state.state_digest
    ));
}

#[test]
fn metadata_transfer_live_export_falls_back_to_checkpoint_without_retained_state_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-fallback-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    source_pg
        .connection()
        .execute(
            "UPDATE metadata_command_log SET post_state_digest = NULL WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            rusqlite::params![ClusterEpoch::INITIAL.get() as i64, pg_id.get() as i64, 1_i64],
        )
        .unwrap();
    drop(source_pg);

    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    let checkpoint = artifact.checkpoint_base().unwrap();
    assert_eq!(checkpoint.pg_id, pg_id);
    assert_eq!(checkpoint.applied_log_index, 1);
    assert_eq!(
        artifact.source_metadata_proof(),
        crate::control_plane::PgMetadataProof::new(
            checkpoint.applied_log_index,
            checkpoint.applied_log_hash,
            checkpoint.state_digest
        )
    );
}

#[test]
fn metadata_transfer_live_checkpoint_fallback_preserves_source_epoch_under_fenced_route() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-fenced-checkpoint-fallback-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    source_pg
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let source_state = source_pg.metadata_command_replica_state().unwrap();
    source_pg
        .connection()
        .execute(
            "UPDATE metadata_command_log SET post_state_digest = NULL WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            rusqlite::params![ClusterEpoch::INITIAL.get() as i64, pg_id.get() as i64, 1_i64],
        )
        .unwrap();
    drop(source_pg);

    let fenced_epoch = ClusterEpoch::new(2).unwrap();
    map.epoch = fenced_epoch;
    for route in map.pg_routes.values_mut() {
        route.cluster_epoch = fenced_epoch;
    }
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let retained_log_err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        retained_log_err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::MissingRetainedCommandStateProof {
                node_id,
                log_index: 1,
                ..
            }
        ) if node_id == NodeId::new(0)
    ));

    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    assert_eq!(artifact.cluster_epoch, source_state.cluster_epoch);
    assert_eq!(
        artifact.source_metadata_proof(),
        crate::control_plane::PgMetadataProof::new(
            source_state.applied_log_index,
            source_state.applied_log_hash,
            source_state.state_digest,
        )
    );
}

#[test]
fn metadata_transfer_live_export_prefers_checkpoint_suffix_candidate() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-candidate-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-candidate-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let pg_id = PgId::new(1);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    let checkpoint = source_pg
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let mut corrupt_newest_checkpoint = source_pg
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    corrupt_newest_checkpoint.checkpoint_crc64 ^= 1;
    let source_state = source_pg.metadata_command_replica_state().unwrap();
    source_pg
        .connection()
        .execute(
            "UPDATE metadata_command_log SET post_state_digest = NULL WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            rusqlite::params![ClusterEpoch::INITIAL.get() as i64, pg_id.get() as i64, 1_i64],
        )
        .unwrap();
    drop(source_pg);

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let retained_log_err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        retained_log_err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::MissingRetainedCommandStateProof {
                node_id,
                log_index: 1,
                ..
            }
        ) if node_id == NodeId::new(0)
    ));

    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer_with_checkpoints(
            pg_id,
            NodeId::new(0),
            [corrupt_newest_checkpoint, checkpoint.clone()],
        )
        .unwrap();

    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    assert_eq!(artifact.cluster_epoch, checkpoint.cluster_epoch);
    assert_eq!(
        artifact.source_base_metadata_proof(),
        crate::control_plane::PgMetadataProof::new(
            checkpoint.applied_log_index,
            checkpoint.applied_log_hash,
            checkpoint.state_digest,
        )
    );
    assert_eq!(artifact.retained_log_entries.len(), 1);
    assert_eq!(artifact.retained_log_entries[0].log_index, 2);
    assert_eq!(
        artifact.source_metadata_proof(),
        crate::control_plane::PgMetadataProof::new(
            source_state.applied_log_index,
            source_state.applied_log_hash,
            source_state.state_digest,
        )
    );
}

#[test]
fn metadata_transfer_live_export_uses_durable_checkpoint_candidate() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-durable-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-durable-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let pg_id = PgId::new(1);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    source_pg
        .apply_metadata_command_and_record(0, &first)
        .unwrap();
    let checkpoint = source_pg
        .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    source_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let source_state = source_pg.metadata_command_replica_state().unwrap();
    source_pg
        .connection()
        .execute(
            "UPDATE metadata_command_log SET post_state_digest = NULL WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            rusqlite::params![ClusterEpoch::INITIAL.get() as i64, pg_id.get() as i64, 1_i64],
        )
        .unwrap();
    drop(source_pg);

    let cluster = crate::StorageCluster::from_local_map(Arc::new(map)).unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    assert_eq!(
        artifact.source_base_metadata_proof(),
        crate::control_plane::PgMetadataProof::new(
            checkpoint.applied_log_index,
            checkpoint.applied_log_hash,
            checkpoint.state_digest,
        )
    );
    assert_eq!(artifact.retained_log_entries.len(), 1);
    assert_eq!(artifact.retained_log_entries[0].log_index, 2);
    assert_eq!(
        artifact.source_metadata_proof(),
        crate::control_plane::PgMetadataProof::new(
            source_state.applied_log_index,
            source_state.applied_log_hash,
            source_state.state_digest,
        )
    );
}

#[test]
fn routine_metadata_checkpoint_records_current_primary_candidate_once() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "routine-metadata-checkpoint-");
    let second_bucket = bucket_for_pg(topology, 1, "routine-metadata-checkpoint-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let pg_id = PgId::new(1);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    source_pg
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let state = source_pg.metadata_command_replica_state().unwrap();
    drop(source_pg);

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let summary = cluster
        .record_routine_metadata_command_checkpoints()
        .unwrap();

    assert_eq!(summary.scanned, 1);
    assert_eq!(summary.recorded, 1);
    assert_eq!(summary.already_current, 0);
    assert_eq!(summary.compacted, 1);
    assert_eq!(summary.failed, 0);

    let primary_pg = cluster
        .local_pg_route(pg_id)
        .and_then(|route| map.node(route.primary_node_id()))
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap();
    let candidates = primary_pg
        .metadata_command_checkpoint_candidates(ClusterEpoch::INITIAL, u64::MAX, 4)
        .unwrap();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].applied_log_index, state.applied_log_index);
    assert_eq!(candidates[0].applied_log_hash, state.applied_log_hash);
    assert_eq!(candidates[0].state_digest, state.state_digest);
    let stats = primary_pg
        .metadata_command_log_stats(ClusterEpoch::INITIAL)
        .unwrap();
    assert_eq!(stats.retained_entries, 0);
    assert_eq!(
        primary_pg
            .max_metadata_command_log_index(ClusterEpoch::INITIAL)
            .unwrap(),
        state.applied_log_index
    );
    drop(primary_pg);

    let summary = cluster
        .record_routine_metadata_command_checkpoints()
        .unwrap();

    assert_eq!(summary.scanned, 1);
    assert_eq!(summary.recorded, 0);
    assert_eq!(summary.already_current, 1);
    assert_eq!(summary.compaction_noop, 1);
    assert_eq!(summary.failed, 0);

    let primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap();
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);
    primary_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let next_state = primary_pg.metadata_command_replica_state().unwrap();
    assert_eq!(next_state.applied_log_index, state.applied_log_index + 1);
    assert_eq!(
        crate::cluster::metadata_command_checkpoint_record_decision(
            &next_state,
            candidates.first(),
            1,
            usize::MAX,
        )
        .unwrap(),
        crate::cluster::MetadataCommandCheckpointRecordDecision::Record
    );
    assert_eq!(
        crate::cluster::metadata_command_checkpoint_record_decision(
            &next_state,
            candidates.first(),
            crate::cluster::METADATA_COMMAND_CHECKPOINT_MIN_LOG_DISTANCE,
            1,
        )
        .unwrap(),
        crate::cluster::MetadataCommandCheckpointRecordDecision::Record
    );
    drop(primary_pg);

    let summary = cluster
        .record_routine_metadata_command_checkpoints()
        .unwrap();

    assert_eq!(summary.scanned, 1);
    assert_eq!(summary.recorded, 0);
    assert_eq!(summary.already_current, 0);
    assert_eq!(summary.skipped_cadence, 1);
    assert_eq!(summary.compaction_noop, 1);
    assert_eq!(summary.failed, 0);
}

#[test]
fn metadata_command_checkpoint_catalogue_retains_newest_candidates_per_epoch() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    set_route_primary(&mut map, 1, NodeId::new(0));
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let pg_id = PgId::new(1);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();

    for log_index in 1..=10 {
        let bucket = bucket_for_pg(
            topology,
            1,
            &format!("metadata-checkpoint-retention-{log_index}-"),
        );
        let command = create_bucket_metadata_command(pg_id, log_index, bucket);
        source_pg
            .apply_metadata_command_and_record(0, &command)
            .unwrap();
        source_pg
            .record_current_metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
    }

    let candidates = source_pg
        .metadata_command_checkpoint_candidates(ClusterEpoch::INITIAL, u64::MAX, 16)
        .unwrap();
    assert_eq!(candidates.len(), 8);
    assert_eq!(candidates.first().unwrap().applied_log_index, 10);
    assert_eq!(candidates.last().unwrap().applied_log_index, 3);
}

#[test]
fn metadata_transfer_live_export_checkpoint_candidates_do_not_mask_hard_source_error() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-candidate-hard-error-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let pg_id = PgId::new(1);
    let source_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    source_pg
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let checkpoint = source_pg
        .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
        .unwrap();
    drop(source_pg);

    let cluster = crate::StorageCluster::from_local_map(Arc::new(map)).unwrap();
    let err = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer_with_checkpoints(
            pg_id,
            NodeId::new(0),
            [checkpoint],
        )
        .unwrap_err();

    assert!(matches!(
        err,
        crate::error::PgMetadataTransferError::Reconstruction { ref message }
            if message.contains("expected Peering")
    ));
}

#[test]
fn metadata_transfer_live_export_falls_back_to_checkpoint_for_abandoned_retained_entry() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-checkpoint-abandoned-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .record_metadata_command_abandoned(0, &command)
        .unwrap();

    let retained_log_err = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        retained_log_err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::UnreplayableAbandonedCommandLogEntry {
                node_id,
                log_index: 1,
            }
        ) if node_id == NodeId::new(0)
    ));

    let artifact = cluster
        .export_pg_metadata_transfer_artifact_for_live_transfer(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        artifact.source_base_kind(),
        crate::peering::PgMetadataTransferBaseKind::Checkpoint
    );
    let checkpoint = artifact.checkpoint_base().unwrap();
    assert_eq!(checkpoint.pg_id, pg_id);
    assert_eq!(
        artifact.source_metadata_proof(),
        crate::control_plane::PgMetadataProof::new(
            checkpoint.applied_log_index,
            checkpoint.applied_log_hash,
            checkpoint.state_digest
        )
    );
}

#[test]
fn metadata_transfer_import_rejects_checkpoint_base_without_payload() {
    let artifact = crate::peering::PgMetadataTransferArtifact {
        pg_id: PgId::new(1),
        source_node_id: NodeId::new(0),
        cluster_epoch: ClusterEpoch::INITIAL,
        base_kind: crate::peering::PgMetadataTransferBaseKind::Checkpoint,
        base_proof: crate::control_plane::PgMetadataProof::new(1, 2, 3),
        checkpoint_base: None,
        proof: crate::control_plane::PgMetadataProof::new(1, 2, 3),
        retained_log_entries: Vec::new(),
    };

    let err = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        ClusterEpoch::new(2).unwrap(),
    )
    .unwrap_err();
    assert!(
        matches!(
            err,
            crate::error::PgMetadataTransferError::Reconstruction { ref message }
                if message.contains("missing checkpoint base payload")
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn metadata_transfer_import_replays_suffix_over_round_trip_base_state() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
    ];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-base-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-base-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    map.pg_routes.get_mut(&PgId::new(1)).unwrap().acting_set =
        Arc::from([NodeId::new(0), NodeId::new(1)]);
    let mut map = Arc::new(map);
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let first_artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let first_destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = first_destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = first_destination_epoch;
    route.primary_node_id = NodeId::new(2);
    route.acting_set = Arc::from([NodeId::new(2), NodeId::new(3)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let imported_base_proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&first_artifact)
        .unwrap();
    drop(cluster);

    let second = create_bucket_metadata_command_at_epoch(
        first_destination_epoch,
        pg_id,
        2,
        second_bucket.clone(),
    );
    for node_id in [NodeId::new(2), NodeId::new(3)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let pre_state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(pre_state.state_digest, imported_base_proof.state_digest);
        pg.apply_metadata_command_and_record(node_id.as_u32(), &second)
            .unwrap();
    }
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let second_artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(2))
        .unwrap();
    drop(cluster);

    let return_epoch = ClusterEpoch::new(4).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = return_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = return_epoch;
    route.primary_node_id = NodeId::new(0);
    route.acting_set = Arc::from([NodeId::new(0), NodeId::new(1)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&second_artifact)
        .unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &second_artifact,
        return_epoch,
    )
    .unwrap();
    assert_eq!(proof, expected_proof);

    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, return_epoch);
        assert_eq!(state.applied_log_index, 2);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_replays_suffix_across_repeated_reshuffles() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
    ];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-repeat-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-repeat-second-");
    let third_bucket = bucket_for_pg(topology, 1, "metadata-transfer-repeat-third-");
    let pg_id = PgId::new(1);

    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    map.pg_routes.get_mut(&pg_id).unwrap().acting_set = Arc::from([NodeId::new(0), NodeId::new(1)]);
    let mut map = Arc::new(map);

    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let first_artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let first_destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = first_destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = first_destination_epoch;
    route.primary_node_id = NodeId::new(2);
    route.acting_set = Arc::from([NodeId::new(2), NodeId::new(3)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    cluster
        .import_pg_metadata_transfer_from_retained_log(&first_artifact)
        .unwrap();
    drop(cluster);

    let second = create_bucket_metadata_command_at_epoch(
        first_destination_epoch,
        pg_id,
        2,
        second_bucket.clone(),
    );
    for node_id in [NodeId::new(2), NodeId::new(3)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &second)
            .unwrap();
    }
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let second_artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(2))
        .unwrap();
    drop(cluster);

    let return_epoch = ClusterEpoch::new(3).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = return_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = return_epoch;
    route.primary_node_id = NodeId::new(0);
    route.acting_set = Arc::from([NodeId::new(0), NodeId::new(1)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    cluster
        .import_pg_metadata_transfer_from_retained_log(&second_artifact)
        .unwrap();
    drop(cluster);

    let third =
        create_bucket_metadata_command_at_epoch(return_epoch, pg_id, 3, third_bucket.clone());
    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &third)
            .unwrap();
    }
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let third_artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let second_destination_epoch = ClusterEpoch::new(4).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = second_destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = second_destination_epoch;
    route.primary_node_id = NodeId::new(2);
    route.acting_set = Arc::from([NodeId::new(2), NodeId::new(3)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&third_artifact)
        .unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &third_artifact,
        second_destination_epoch,
    )
    .unwrap();
    assert_eq!(proof, expected_proof);
    assert_eq!(proof.applied_log_index, 3);

    for node_id in [NodeId::new(2), NodeId::new(3)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, second_destination_epoch);
        assert_eq!(state.applied_log_index, proof.applied_log_index);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &third_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_bootstraps_matching_old_base_before_suffix_replay() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
    ];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "metadata-transfer-return-base-first-");
    let second_bucket = bucket_for_pg(topology, 1, "metadata-transfer-return-base-second-");
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket.clone());
    let source_epoch = ClusterEpoch::new(2).unwrap();
    let second =
        create_bucket_metadata_command_at_epoch(source_epoch, pg_id, 1, second_bucket.clone());

    set_route_primary(&mut map, 1, NodeId::new(2));
    set_route_state(&mut map, 1, PgState::Peering);
    map.epoch = source_epoch;
    let route = map.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = source_epoch;
    route.acting_set = Arc::from([NodeId::new(2), NodeId::new(3)]);
    let mut map = Arc::new(map);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    for node_id in [NodeId::new(2), NodeId::new(3)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let base = pg.metadata_command_replica_state().unwrap();
        pg.initialize_metadata_transfer_matching_state(
            node_id.as_u32(),
            source_epoch,
            0,
            0,
            base.state_digest,
        )
        .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &second)
            .unwrap();
    }

    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(2))
        .unwrap();
    drop(cluster);

    let return_epoch = ClusterEpoch::new(3).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = return_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = return_epoch;
    route.primary_node_id = NodeId::new(0);
    route.acting_set = Arc::from([NodeId::new(0), NodeId::new(1)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    let expected_proof =
        crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(&artifact, return_epoch)
            .unwrap();
    assert_eq!(proof, expected_proof);

    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, return_epoch);
        assert_eq!(state.applied_log_index, 1);
        assert_eq!(state.applied_log_hash, proof.applied_log_hash);
        assert_eq!(state.state_digest, proof.state_digest);
        crate::PgMetadataStore::head_bucket(&*pg, &first_bucket).unwrap();
        crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).unwrap();
    }
}

#[test]
fn metadata_transfer_import_initializes_empty_destination_state() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(artifact.source_metadata_proof().applied_log_index, 0);
    assert_eq!(artifact.source_metadata_proof().applied_log_hash, 0);
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let proof = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap();
    let expected_proof = crate::StorageCluster::metadata_transfer_imported_proof_at_epoch(
        &artifact,
        destination_epoch,
    )
    .unwrap();
    assert_eq!(proof, expected_proof);
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state.cluster_epoch, destination_epoch);
        assert_eq!(state.applied_log_index, 0);
        assert_eq!(state.applied_log_hash, 0);
        assert_eq!(state.state_digest, expected_proof.state_digest);
    }
}

fn create_bucket_metadata_command_at_epoch(
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    log_index: u64,
    bucket: crate::BucketName,
) -> MetadataCommandEnvelope {
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let config = crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: "owner",
        owner_canonical_id: &owner,
        acl_grants: &acl_grants,
        public_read: false,
        public_write: false,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            cluster_epoch,
            pg_id,
            MetadataCommandLogIndex::new(log_index).unwrap(),
        ),
        MetadataCommandPayload::CreateBucket(
            CreateBucketCommand::from_config(&config, 1_234, log_index).unwrap(),
        ),
    )
}

#[test]
fn metadata_transfer_import_rejects_dirty_destination_replicas() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let source_bucket = bucket_for_pg(topology, 1, "metadata-transfer-import-source-");
    let stale_bucket = bucket_for_pg(topology, 1, "metadata-transfer-import-stale-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let source = create_bucket_metadata_command(pg_id, 1, source_bucket);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &source)
        .unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Peering;
    let stale = create_bucket_metadata_command_at_epoch(destination_epoch, pg_id, 1, stale_bucket);
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        map.node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .apply_metadata_command_and_record(node_id.as_u32(), &stale)
            .unwrap();
    }
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                node_id,
                pg_id: dirty_pg_id,
                cluster_epoch,
                ..
            }
        ) if node_id == NodeId::new(1)
            && dirty_pg_id == pg_id
            && cluster_epoch == destination_epoch
    ));
}

#[test]
fn metadata_transfer_import_rejects_active_destination_route() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "metadata-transfer-import-active-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let mut map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let command = create_bucket_metadata_command(pg_id, 1, bucket);
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &command)
        .unwrap();
    let artifact = cluster
        .export_pg_metadata_transfer_from_retained_log(pg_id, NodeId::new(0))
        .unwrap();
    drop(cluster);

    let destination_epoch = ClusterEpoch::new(2).unwrap();
    let map_mut = Arc::get_mut(&mut map).unwrap();
    map_mut.epoch = destination_epoch;
    let route = map_mut.pg_routes.get_mut(&pg_id).unwrap();
    route.cluster_epoch = destination_epoch;
    route.primary_node_id = NodeId::new(1);
    route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2)]);
    route.state = PgState::Active;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let err = cluster
        .import_pg_metadata_transfer_from_retained_log(&artifact)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Store(StoreError::PgNotActive {
            pg_id: 1,
            cluster_epoch,
            state: PgState::Active,
        }) if cluster_epoch == destination_epoch
    ));
}

#[test]
fn peering_replay_retries_after_partial_replica_catchup() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let pg_id = PgId::new(1);
    let commands: Vec<_> = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (1..=3)
            .map(|log_index| {
                create_bucket_metadata_command(
                    pg_id,
                    log_index,
                    bucket_for_pg(
                        topology,
                        1,
                        &format!("peering-replay-partial-retry-{log_index}-"),
                    ),
                )
            })
            .collect()
    };
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &commands[0])
            .unwrap();
    }
    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        for command in &commands[1..] {
            pg.apply_metadata_command_and_record(node_id.as_u32(), command)
                .unwrap();
        }
    }
    let primary_state = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    let proof = crate::control_plane::PgMetadataProof::new(
        primary_state.applied_log_index,
        primary_state.applied_log_hash,
        primary_state.state_digest,
    );

    let decision = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );
    for node_id in node_ids {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state, primary_state);
    }
}

#[test]
fn peering_replay_fetches_primary_retained_entries_across_batches() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let pg_id = PgId::new(1);
    let command_count =
        crate::storage_rpc::STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES + 2;
    let commands: Vec<_> = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (1..=command_count)
            .map(|log_index| {
                create_bucket_metadata_command(
                    pg_id,
                    log_index,
                    bucket_for_pg(topology, 1, &format!("peering-replay-batch-{log_index}-")),
                )
            })
            .collect()
    };
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &commands[0])
            .unwrap();
    }
    let primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    for command in &commands[1..] {
        primary_pg
            .apply_metadata_command_and_record(0, command)
            .unwrap();
    }
    let primary_state = primary_pg.metadata_command_replica_state().unwrap();
    let proof = crate::control_plane::PgMetadataProof::new(
        primary_state.applied_log_index,
        primary_state.applied_log_hash,
        primary_state.state_digest,
    );
    drop(primary_pg);

    let decision = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();

    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );
    for node_id in node_ids {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state, primary_state);
    }
}

#[test]
fn peering_replay_rejects_active_route_without_mutation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "active-replay-first-");
    let second_bucket = bucket_for_pg(topology, 1, "active-replay-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .apply_metadata_command_and_record(0, &second)
        .unwrap();

    let replica_states_before = [NodeId::new(1), NodeId::new(2)].map(|node_id| {
        map.node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap()
    });

    let err = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap_err();

    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Store(StoreError::PgNotActive {
            pg_id: 1,
            cluster_epoch,
            state: PgState::Active,
        }) if cluster_epoch == cluster.operation_epoch()
    ));
    for (node_id, expected_state) in [NodeId::new(1), NodeId::new(2)]
        .into_iter()
        .zip(replica_states_before)
    {
        let state = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap()
            .metadata_command_replica_state()
            .unwrap();
        assert_eq!(state, expected_state);
    }
}

#[test]
fn peering_replay_fails_closed_on_primary_abandoned_tombstone() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "peering-tombstone-first-");
    let abandoned_bucket = bucket_for_pg(topology, 1, "peering-tombstone-abandoned-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let abandoned = create_bucket_metadata_command(pg_id, 2, abandoned_bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    map.node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap()
        .record_metadata_command_abandoned(0, &abandoned)
        .unwrap();

    let err = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap_err();

    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::UnreplayableAbandonedCommandLogEntry {
                node_id,
                log_index: 2,
            }
        ) if node_id == NodeId::new(1) || node_id == NodeId::new(2)
    ));
}

#[test]
fn peering_replay_then_fresh_heartbeats_complete_authority_activation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map =
        LocalClusterMap::open(&tmp.path().join("nodes"), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "authority-replay-first-");
    let second_bucket = bucket_for_pg(topology, 1, "authority-replay-second-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    set_route_state(&mut map, 1, PgState::Peering);
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    let second = create_bucket_metadata_command(pg_id, 2, second_bucket);

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &first)
            .unwrap();
    }
    let primary_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    primary_pg
        .apply_metadata_command_and_record(0, &second)
        .unwrap();
    let primary_state = primary_pg.metadata_command_replica_state().unwrap();
    let proof = crate::control_plane::PgMetadataProof::new(
        primary_state.applied_log_index,
        primary_state.applied_log_hash,
        primary_state.state_digest,
    );
    drop(primary_pg);

    let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
        crate::control_plane::FileControlPlaneStore::new(tmp.path().join("control-plane.state")),
    )
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            vec![
                (NodeId::new(0), "node-0.sock".to_owned()),
                (NodeId::new(1), "node-1.sock".to_owned()),
                (NodeId::new(2), "node-2.sock".to_owned()),
            ],
            vec![pg_id],
        )
        .unwrap();

    for (node_id, now_ms) in node_ids.into_iter().zip([990, 991, 992]) {
        heartbeat_authority_node(&mut authority, node_id, now_ms);
    }
    heartbeat_authority_with_local_pg_proof(&mut authority, &map, NodeId::new(0), pg_id, 1_000);
    heartbeat_authority_with_local_pg_proof(&mut authority, &map, NodeId::new(1), pg_id, 1_001);
    heartbeat_authority_with_local_pg_proof(&mut authority, &map, NodeId::new(2), pg_id, 1_002);
    let pre_replay_completion = authority.complete_pg_peering(pg_id, NodeId::new(0), 0, 1_003);
    assert!(
        matches!(
            pre_replay_completion,
            Err(crate::control_plane::ControlPlaneError::PgPeeringMetadataProofMismatch { .. })
        ),
        "unexpected pre-replay completion result: {pre_replay_completion:?}"
    );

    let decision = cluster
        .replay_pg_peering_catchup_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap();
    assert_eq!(
        decision,
        crate::peering::PgPeeringReconstructionDecision::AlreadyConverged { proof }
    );

    heartbeat_authority_with_local_pg_proof(&mut authority, &map, NodeId::new(0), pg_id, 1_010);
    heartbeat_authority_with_local_pg_proof(&mut authority, &map, NodeId::new(1), pg_id, 1_011);
    heartbeat_authority_with_local_pg_proof(&mut authority, &map, NodeId::new(2), pg_id, 1_012);

    let activated = authority
        .complete_pg_peering(pg_id, NodeId::new(0), 0, 1_013)
        .unwrap();
    let pg = activated.pg(pg_id).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_primary(), Some(NodeId::new(0)));
    assert_eq!(pg.active_metadata_proof(), Some(proof));
}

fn heartbeat_authority_node<S: crate::control_plane::ControlPlaneStore>(
    authority: &mut crate::control_plane::SingleAuthorityControlPlane<S>,
    node_id: NodeId,
    now_ms: u64,
) {
    let record = authority.snapshot().node(node_id).unwrap();
    let heartbeat = crate::control_plane::NodeHeartbeat {
        node_id,
        node_incarnation: record.node_incarnation(),
        endpoint: record.endpoint().to_owned(),
        observed_epoch: authority.snapshot().cluster_epoch(),
        requested_lease_duration_ms: 100,
        pg_observations: Vec::new(),
    };
    authority.heartbeat(heartbeat, now_ms).unwrap();
}

fn heartbeat_authority_with_local_pg_proof<S: crate::control_plane::ControlPlaneStore>(
    authority: &mut crate::control_plane::SingleAuthorityControlPlane<S>,
    map: &Arc<LocalClusterMap>,
    node_id: NodeId,
    pg_id: PgId,
    now_ms: u64,
) {
    let state = map
        .node(node_id)
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap()
        .metadata_command_replica_state()
        .unwrap();
    let record = authority.snapshot().node(node_id).unwrap();
    let heartbeat = crate::control_plane::NodeHeartbeat {
        node_id,
        node_incarnation: record.node_incarnation(),
        endpoint: record.endpoint().to_owned(),
        observed_epoch: authority.snapshot().cluster_epoch(),
        requested_lease_duration_ms: 100,
        pg_observations: vec![crate::control_plane::NodePgHeartbeatObservation {
            pg_id,
            state: PgState::Peering,
            metadata_proof: crate::control_plane::PgMetadataProof::new(
                state.applied_log_index,
                state.applied_log_hash,
                state.state_digest,
            ),
            has_pending_metadata_command: false,
        }],
    };
    authority.heartbeat(heartbeat, now_ms).unwrap();
}

#[test]
fn peering_reconstruction_gather_fails_closed_on_pending_metadata_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "peering-pending-first-");
    let pending_bucket = bucket_for_pg(topology, 1, "peering-pending-next-");
    set_route_primary(&mut map, 1, NodeId::new(0));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let first = create_bucket_metadata_command(pg_id, 1, first_bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(0), &first)
        .unwrap();
    let pending = create_bucket_metadata_command(pg_id, 2, pending_bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &pending_bucket, &pending);

    let err = cluster
        .reconstruct_pg_peering_from_retained_metadata_log(pg_id, NodeId::new(0))
        .unwrap_err();
    assert!(matches!(
        err,
        crate::peering::PgPeeringReconstructionFailure::Reconstruction(
            crate::peering::PgPeeringReconstructionError::PendingMetadataCommand {
                node_id
            }
        ) if node_id == NodeId::new(0)
    ));
}

#[test]
fn partial_tombstone_recording_retries_as_idempotent_abandon() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "partial-tombstone-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let command = create_bucket_metadata_command(PgId::new(1), 1, bucket.clone());

    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        node_zero_pg
            .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
            .unwrap();
    }

    cluster
        .record_abandoned_metadata_command_to_acting_set(&command)
        .unwrap();
    cluster
        .record_abandoned_metadata_command_to_acting_set(&command)
        .unwrap();

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 1);
        assert!(matches!(
            crate::PgMetadataStore::head_bucket(&*pg, &bucket),
            Err(crate::MetadataError::BucketNotFound { .. })
        ));
    }
}

#[test]
fn partial_abandoned_create_bucket_retry_rebuilds_command_before_reporting_created() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "partial-create-tombstone-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let abandoned_index = map.test_next_metadata_command_log_index(pg_id).get();
    let command = create_bucket_metadata_command(pg_id, abandoned_index, bucket.clone());
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        node_zero_pg
            .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
            .unwrap();
    }

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let outcome = cluster
        .create_bucket_with_config_and_load_info(&crate::CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: "owner",
            owner_canonical_id: &owner,
            acl_grants: &acl_grants,
            public_read: false,
            public_write: false,
            versioning: crate::BucketVersioningState::Disabled,
            object_lock: crate::BucketObjectLockConfig::default(),
            ownership_controls: crate::BucketOwnershipControls {
                object_ownership: crate::BucketObjectOwnership::ObjectWriter,
            },
        })
        .unwrap();
    let crate::BucketCreateAttemptOutcome::Created(info) = outcome else {
        panic!("abandoned create retry must create a fresh bucket, got {outcome:?}");
    };
    assert_eq!(info.name, bucket);
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, abandoned_index + 1);
        let stored = crate::PgMetadataStore::head_bucket(&*pg, &bucket).unwrap();
        assert_eq!(stored.name, bucket);
    }
}

#[test]
fn partial_abandoned_reservation_retry_does_not_report_skipped_command_success() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let reservation_id = crate::SessionId::try_from("58".repeat(16)).unwrap();
    let skipped_generation_id = crate::GenerationId::MIN;
    let command = MetadataCommandEnvelope::new(
        crate::metadata_command::MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(
            crate::metadata_command::ReserveObjectGenerationCommand::new(
                bucket.clone(),
                key.clone(),
                reservation_id.clone(),
                skipped_generation_id,
                123,
            ),
        ),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        node_zero_pg
            .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
            .unwrap();
    }

    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    assert_eq!(generation_id, skipped_generation_id);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id
            )
            .unwrap(),
            generation_id
        );
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, 2);
    }
}

#[test]
fn abandoned_put_object_stream_create_releases_reserved_generation_on_drain() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let session_id = crate::SessionId::try_from("59".repeat(16)).unwrap();
    cluster
        .reserve_put_object_generation(&bucket, &key, &session_id)
        .unwrap();
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-put-object-stream-create-abandoned",
        Some(key.as_str()),
    );
    let command = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                cluster.operation_epoch(),
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    crate::CreateStreamUploadReq {
                        session_id: session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: crate::StreamUploadTarget::PutObject,
                        encryption: crate::ObjectEncryption::None,
                    },
                    123,
                    proof,
                ),
            )),
        );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        node_zero_pg
            .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
            .unwrap();
    }

    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &session_id
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn abandoned_put_object_stream_create_release_failure_retries_to_terminal_cleanup() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let session_id = crate::SessionId::try_from("5a".repeat(16)).unwrap();
    let _generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &session_id)
        .unwrap();
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-put-object-stream-create-release-failure",
        Some(key.as_str()),
    );
    let command = MetadataCommandEnvelope::new(
            crate::metadata_command::MetadataCommandId::new(
                cluster.operation_epoch(),
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    crate::CreateStreamUploadReq {
                        session_id: session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: crate::StreamUploadTarget::PutObject,
                        encryption: crate::ObjectEncryption::None,
                    },
                    123,
                    proof,
                ),
            )),
        );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        node_zero_pg
            .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
            .unwrap();
    }

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_session = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::ReleaseObjectGeneration(release)
                    if release.matches_request(&hook_bucket, &hook_key, &hook_session)
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected abandoned stream create release failure",
                        source: std::io::Error::other(
                            "injected abandoned stream create release failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected abandoned stream create release failure",
                ..
            })
        ),
        "expected injected abandoned release failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &session_id
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn zero_apply_generation_reservation_records_tombstone_and_later_reserves() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let before_index = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap()
        .applied_log_index;

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::ReserveObjectGeneration(reservation)
                    if reservation.bucket == hook_bucket
                        && reservation.key == hook_key
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected zero-apply reservation failure",
                        source: std::io::Error::other("injected zero-apply reservation failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let reservation_id = crate::SessionId::try_from("52".repeat(16)).unwrap();
    let err = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected zero-apply reservation failure",
                ..
            })
        ),
        "expected injected zero-apply reservation failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, before_index + 1);
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }

    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    assert!(generation_id.get() >= crate::GenerationId::MIN.get());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, before_index + 2);
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id
            )
            .unwrap(),
            generation_id
        );
    }
}

#[test]
fn zero_apply_direct_put_commit_records_tombstone_and_cleans_new_payload() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation_id = crate::SessionId::try_from("53".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let before_index = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .metadata_command_replica_state()
        .unwrap()
        .applied_log_index;
    let payload = b"direct put zero apply tombstone";
    let segment_okh = [93; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: reservation_id.clone(),
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected zero-apply direct put commit failure",
                        source: std::io::Error::other(
                            "injected zero-apply direct put commit failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected zero-apply direct put commit failure",
                ..
            })
        ),
        "expected injected zero-apply direct PUT commit failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.applied_log_index, before_index + 2);
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn abandoned_matching_direct_put_commit_cleans_pending_and_current_payload() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id = crate::SessionId::try_from("54".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "abandoned-matching-direct-put-test",
            Some(key.as_str()),
        )
        .unwrap();
    let bucket_write_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);

    let abandoned_payload = b"abandoned direct put payload";
    let abandoned_okh = [94; 16];
    let abandoned_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &abandoned_okh,
            abandoned_payload,
        )
        .unwrap();
    let abandoned_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: reservation_id.clone(),
            generation_id,
            payload: abandoned_payload,
            segment_okh: abandoned_okh,
            written: &abandoned_written,
        },
    );
    let mut abandoned_req = abandoned_req;
    abandoned_req.bucket_write_reservation = bucket_write_proof.clone();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(object_pg))
        .unwrap();
    let object_pg_store = primary.storage_node().get_pg(object_pg).unwrap();
    let command = cluster
        .prepare_commit_direct_put_object_command(
            PgId::new(object_pg),
            &object_pg_store,
            &abandoned_req,
            crate::VersionId::Null,
            bucket_write_proof.clone(),
        )
        .unwrap();
    drop(object_pg_store);
    let abandoned_shard_batch: Vec<(&ShardKey, crate::WriteAck)> = abandoned_written
        .written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    cluster
        .register_payload_shard_acks(data_pg, &abandoned_shard_batch)
        .unwrap();

    insert_pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket, &command);
    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        node_zero_pg
            .record_metadata_command_abandoned(NodeId::new(0).as_u32(), &command)
            .unwrap();
    }

    let current_payload = b"current direct put retry payload";
    let current_okh = [95; 16];
    let current_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &current_okh,
            current_payload,
        )
        .unwrap();
    let current_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: reservation_id.clone(),
            generation_id,
            payload: current_payload,
            segment_okh: current_okh,
            written: &current_written,
        },
    );
    let mut current_req = current_req;
    current_req.bucket_write_reservation = bucket_write_proof;
    let current_shard_batch: Vec<(&ShardKey, crate::WriteAck)> = current_written
        .written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    cluster
        .register_payload_shard_acks(data_pg, &current_shard_batch)
        .unwrap();

    let data_primary = map.node(NodeId::new(2)).unwrap().storage_node();
    for written in abandoned_written
        .written_shards
        .iter()
        .chain(current_written.written_shards.iter())
    {
        assert!(data_primary
            .test_shard_exists(data_pg, &written.key)
            .unwrap());
    }

    let err = cluster
        .commit_direct_put_object_from_payload_shards(
            &current_req,
            &current_written.written_shards,
            |_| -> Result<(), ()> {
                panic!("matching abandoned pending direct PUT must not build a new command")
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
                context: "abandoned pending command for direct put commit",
            })
        ),
        "expected abandoned direct PUT conflict, got {err:?}"
    );

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }

    for shard_index in 0..abandoned_written.ec.k + abandoned_written.ec.m {
        assert!(!cluster
            .test_payload_shard_file_exists(
                abandoned_written.data_pg_id,
                abandoned_written.ec,
                &abandoned_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
        assert!(!cluster
            .test_payload_shard_file_exists(
                current_written.data_pg_id,
                current_written.ec,
                &current_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
    }
    for written in abandoned_written
        .written_shards
        .iter()
        .chain(current_written.written_shards.iter())
    {
        assert!(!data_primary
            .test_shard_exists(data_pg, &written.key)
            .unwrap());
    }
}

#[test]
fn direct_put_commit_drains_unrelated_pending_command_before_publish() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pending_key = key_for_object_pg(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology(),
        &bucket,
        object_pg,
        "pending-tags-",
    );
    write_committed_direct_segment_for(
        &cluster,
        &bucket,
        &pending_key,
        b"unrelated pending object",
    );

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = pending_key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutObjectMetadata(update)
                    if update.object.bucket == hook_bucket
                        && update.object.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected unrelated metadata command apply failure",
                        source: std::io::Error::other(
                            "injected unrelated metadata command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let tags =
        "<Tagging><TagSet><Tag><Key>phase</Key><Value>pending</Value></Tag></TagSet></Tagging>";
    let err = cluster
        .put_object_tags_if(&bucket, &pending_key, None, tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected unrelated metadata command apply failure",
                ..
            })
        ),
        "expected injected unrelated pending command failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "unrelated object metadata command must remain pending"
    );

    let reservation_id = crate::SessionId::try_from("54".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put drains unrelated pending command";
    let segment_okh = [54; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let mut commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    commit_req.versioning = crate::BucketVersioningState::Enabled;
    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<_, ()>(())
        })
        .unwrap()
        .unwrap();
    assert_eq!(outcome.live_size, payload.len() as u64);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_direct_put_metadata_on_acting_nodes(&map, &node_ids, object_pg, &commit_req, &outcome);
    assert_object_version_counter_on_acting_nodes(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        outcome.version_id.to_u64() + 1,
    );

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &pending_key).unwrap();
        assert_eq!(stored.as_live().unwrap().tags.as_deref(), Some(tags));
    }
}

#[test]
fn direct_put_commit_returns_contention_after_unrelated_partial_exact_pending_conflict() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pending_key = key_for_object_pg(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology(),
        &bucket,
        object_pg,
        "pending-partial-tags-",
    );
    write_committed_direct_segment_for(
        &cluster,
        &bucket,
        &pending_key,
        b"unrelated partial pending object",
    );

    let reservation_id = crate::SessionId::try_from("55".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put drains unrelated partial exact conflict";
    let segment_okh = [55; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let mut commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    commit_req.versioning = crate::BucketVersioningState::Enabled;

    let tags =
        "<Tagging><TagSet><Tag><Key>phase</Key><Value>partial</Value></Tag></TagSet></Tagging>";
    let primary = map.node(NodeId::new(1)).unwrap().storage_node();
    let pg = primary.get_pg(object_pg).unwrap();
    let live = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &pending_key)
        .unwrap()
        .into_live()
        .unwrap();
    drop(pg);
    let pg_id = PgId::new(object_pg);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-put-object-metadata",
        Some(pending_key.as_str()),
    );
    let pending_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::PutObjectMetadata(Box::new(
            PutObjectMetadataCommand::from_live_object_and_mutation(
                live,
                PutObjectMetadataMutation::PutTags(tags.to_string()),
                proof,
            ),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending_command);
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_some(),
        "unrelated object metadata command must start pending"
    );

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = pending_key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutObjectMetadata(update)
                    if update.object.bucket == hook_bucket
                        && update.object.key == hook_key
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(NodeId::new(2)).unwrap().storage_node();
                    let pg = node.get_pg(command.id().pg_id().get())?;
                    pg.apply_metadata_command_and_record(NodeId::new(2).as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual object metadata command apply failed: {error}")
                            }
                        })?;
                    return Err(StoreError::MetadataCommandLogConflict {
                        node_id: node_id.as_u32(),
                        pg_id: command.id().pg_id().get(),
                        cluster_epoch: command.id().cluster_epoch(),
                        log_index: command.id().log_index().get(),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<_, ()>(())
        })
        .unwrap()
        .unwrap();
    drop(hook_guard);
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &pending_key).unwrap();
        assert_eq!(stored.as_live().unwrap().tags.as_deref(), Some(tags));
        assert!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key)
                .unwrap()
                .as_live()
                .is_some(),
            "direct PUT should publish after recovering a now-complete unrelated pending command"
        );
    }
    assert_eq!(outcome.version_id.to_u64(), 1);
}

#[test]
fn direct_put_commit_retries_partial_exact_command_conflict() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id = crate::SessionId::try_from("56".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put exact partial retry";
    let segment_okh = [56; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );

    let _serial = lock_metadata_command_apply_hook_test();
    let applied_by_hook = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let applied_by_hook_guard = Arc::clone(&applied_by_hook);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(2)
                        && !applied_by_hook_guard.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(NodeId::new(2)).unwrap().storage_node();
                    let pg = node.get_pg(command.id().pg_id().get())?;
                    pg.apply_metadata_command_and_record(NodeId::new(2).as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual direct put command apply failed: {error}")
                            }
                        })?;
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<_, ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(applied_by_hook.load(Ordering::SeqCst));
    assert_eq!(outcome.live_size, payload.len() as u64);
    assert_direct_put_metadata_on_acting_nodes(&map, &node_ids, object_pg, &commit_req, &outcome);
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn reserve_object_version_retry_reuses_pending_partial_replica_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(object_pg);

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::ReserveObjectVersion(reservation)
                    if reservation.bucket == hook_bucket
                        && reservation.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected reserve object version apply failure",
                        source: std::io::Error::other(
                            "injected reserve object version apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected reserve object version apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);

    let pending = pending_metadata_command_for_test(&map, pg_id, &bucket)
        .expect("partial version reservation must remain pending");
    let MetadataCommandPayload::ReserveObjectVersion(reservation) = pending.payload() else {
        panic!("expected pending ReserveObjectVersion, got {pending:?}");
    };
    assert_eq!(reservation.version_id, crate::VersionId::from_u64(1));
    assert_object_version_counter_on_acting_nodes(
        &map,
        &[NodeId::new(1)],
        object_pg,
        &bucket,
        &key,
        2,
    );
    for node_id in [NodeId::new(0), NodeId::new(2)] {
        assert_object_version_counter_on_acting_nodes(
            &map,
            &[node_id],
            object_pg,
            &bucket,
            &key,
            0,
        );
    }

    let reserved = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap();
    assert_eq!(reserved, crate::VersionId::from_u64(1));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 2);
}

#[test]
fn reserve_object_version_abandons_stale_pending_reservation_and_retries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(object_pg);

    let first = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            crate::VersionId::from_u64(1),
        )),
    );
    cluster
        .apply_metadata_command_to_acting_set(&first)
        .unwrap();
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 2);

    let stale_pending = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            crate::VersionId::from_u64(1),
        )),
    );
    force_insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &stale_pending);

    let reserved = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap();
    assert_eq!(reserved, crate::VersionId::from_u64(2));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 3);
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn reserve_object_version_clears_fully_applied_pending_reservation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            bucket.clone(),
            key.clone(),
            crate::VersionId::from_u64(1),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();

    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_some(),
        "test setup must leave the fully applied command pending"
    );
    let version = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap();
    assert_eq!(version, crate::VersionId::from_u64(1));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 2);
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn reserve_object_version_partial_apply_after_reopen_converges() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(0));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(object_pg);

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::ReserveObjectVersion(reservation)
                    if reservation.bucket == hook_bucket
                        && reservation.key == hook_key
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected lost reserve object version replica apply failure",
                        source: std::io::Error::other(
                            "injected lost reserve object version replica apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected lost reserve object version replica apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);

    assert_object_version_counter_on_acting_nodes(
        &map,
        &[NodeId::new(0), NodeId::new(1)],
        object_pg,
        &bucket,
        &key,
        2,
    );
    assert_object_version_counter_on_acting_nodes(
        &map,
        &[NodeId::new(2)],
        object_pg,
        &bucket,
        &key,
        0,
    );

    drop(cluster);
    drop(map);

    let mut reopened_map =
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    set_route_primary(&mut reopened_map, object_pg, NodeId::new(0));
    let reopened_map = Arc::new(reopened_map);
    let reopened_cluster =
        crate::StorageCluster::from_local_map(Arc::clone(&reopened_map)).unwrap();
    assert!(
        pending_metadata_command_for_test(&reopened_map, pg_id, &bucket).is_none(),
        "open-time recovery should converge and clear the partial version reservation slot"
    );
    assert_object_version_counter_on_acting_nodes(
        &reopened_map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        2,
    );
    let reserved = reopened_cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap();
    assert_eq!(reserved, crate::VersionId::from_u64(2));
    assert!(pending_metadata_command_for_test(&reopened_map, pg_id, &bucket).is_none());
    assert_object_version_counter_on_acting_nodes(
        &reopened_map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        3,
    );
    assert_clean_metadata_command_stream(&reopened_map, &[object_pg]);
}

#[test]
fn direct_put_action_failure_does_not_reserve_object_version() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    let reservation_id = crate::SessionId::try_from("75".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"conditional direct put should not reserve a version";
    let segment_okh = [75; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let mut commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id,
            generation_id,
            payload,
            segment_okh,
            written: &written,
        },
    );
    commit_req.versioning = crate::BucketVersioningState::Enabled;

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Err::<(), _>("conditional write rejected")
        })
        .unwrap()
        .unwrap_err();
    assert_eq!(err, "conditional write rejected");
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 0);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
}
