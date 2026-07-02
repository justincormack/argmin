use super::*;

#[test]
fn finalized_bucket_delete_clears_pending_versioning_command_for_recreate() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "partial-versioning-delete-recreate-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.bucket.name == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected metadata command apply failure",
                        source: std::io::Error::other("injected metadata command apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_some(),
        "failed versioning command should remain pending before delete"
    );
    let old_partial_generation = {
        let applied_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = applied_replica.get_pg(1).unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
            .unwrap()
            .bucket_execution_generation
    };

    cluster.begin_bucket_delete(&bucket).unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
        "finalized delete must clear stale pending commands for the old bucket incarnation"
    );

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let recreated = cluster
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
    let recreated_generation = match recreated {
        crate::BucketCreateAttemptOutcome::Created(info) => info.bucket_execution_generation,
        other => panic!("expected recreated bucket, got {other:?}"),
    };
    assert!(recreated_generation > old_partial_generation);

    let updated = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(updated.versioning, crate::BucketVersioningState::Enabled);
    assert!(updated.bucket_execution_generation > recreated_generation);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
        assert_eq!(
            info.bucket_execution_generation,
            updated.bucket_execution_generation
        );
    }
}

#[test]
fn finalized_bucket_delete_removes_replicated_create_rows() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-recreate-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let created_generation = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .test_head_bucket_raw(&bucket)
        .unwrap()
        .bucket_execution_generation;
    cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    let pre_delete_generation = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .test_head_bucket_raw(&bucket)
        .unwrap()
        .bucket_execution_generation;
    assert!(pre_delete_generation > created_generation);

    cluster.begin_bucket_delete(&bucket).unwrap();
    let deleting_generation = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .test_head_bucket_raw(&bucket)
        .unwrap()
        .bucket_execution_generation;
    assert!(deleting_generation > pre_delete_generation);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
        assert_eq!(info.bucket_execution_generation, deleting_generation);
    }
    assert_bucket_execution_counter_on_acting_nodes(&map, &node_ids, 1, deleting_generation);

    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert!(crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).is_err());
    }

    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let recreated = cluster
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
    assert!(matches!(
        recreated,
        crate::BucketCreateAttemptOutcome::Created(info)
            if info.name == bucket
                && info.bucket_execution_generation > pre_delete_generation
    ));

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        assert_eq!(
            crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .name,
            bucket
        );
    }
}

#[test]
fn finalized_bucket_delete_after_reopen_does_not_need_begin_waiter() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-finalize-reopen-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster.begin_bucket_delete(&bucket).unwrap();
    assert_clean_metadata_command_stream(&map, &[1]);
    drop(cluster);
    drop(map);

    let mut reopened = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    set_route_primary(&mut reopened, 1, NodeId::new(1));
    let reopened = Arc::new(reopened);
    let reopened_cluster = crate::StorageCluster::from_local_map(Arc::clone(&reopened)).unwrap();

    assert_eq!(
        reopened_cluster
            .try_finalize_bucket_delete(&bucket)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized,
        "finalization must not require the process that began DeleteBucket"
    );
    assert_eq!(
        reopened_cluster
            .try_finalize_bucket_delete(&bucket)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::NotFound,
        "finalized delete should be idempotent after row removal"
    );
    assert_clean_metadata_command_stream(&reopened, &[1]);
}

#[test]
fn durable_bucket_finalize_scan_recovers_lost_local_queue_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let bucket = {
        let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
        let bucket = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_for_pg(topology, 1, "delete-finalize-scan-")
        };
        set_route_primary(&mut map, 1, NodeId::new(1));
        let map = Arc::new(map);
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &bucket);
        cluster.begin_bucket_delete(&bucket).unwrap();
        bucket
    };

    let mut reopened = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    set_route_primary(&mut reopened, 1, NodeId::new(1));
    let reopened = Arc::new(reopened);
    let reopened_cluster = crate::StorageCluster::from_local_map(Arc::clone(&reopened)).unwrap();

    let scan = reopened_cluster.enqueue_durable_bucket_delete_finalize_roots();
    assert_eq!(scan.errors, 0);
    assert_eq!(
        scan.queued, 1,
        "startup scan should rediscover the deleting bucket without an in-memory hint"
    );
    assert!(matches!(
        reopened_cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_bucket))
            if queued_bucket == bucket
    ));
    assert_eq!(
        reopened_cluster
            .try_finalize_bucket_delete(&bucket)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert_clean_metadata_command_stream(&reopened, &[1]);
}

#[test]
fn durable_bucket_finalize_scan_prioritizes_expired_claimed_bucket() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket_a, bucket_b) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "delete-finalize-a-"),
            bucket_for_pg(topology, 1, "delete-finalize-b-"),
        )
    };
    assert!(
        bucket_a < bucket_b,
        "test bucket names should exercise an earlier unclaimed bucket"
    );
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket_a);
    create_test_bucket(&cluster, &bucket_b);
    cluster.begin_bucket_delete(&bucket_b).unwrap();

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let deleting_b =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket_b).unwrap();
    crate::PgMetadataStore::acquire_bucket_delete_finalize_claim(
        &*primary_pg,
        &bucket_b,
        deleting_b.bucket_incarnation_generation,
        "held-finalizer-claim-b",
        "external-worker",
        ClusterEpoch::INITIAL,
        10,
        Some(20),
        10,
    )
    .unwrap()
    .expect("later bucket should be claimable");
    drop(primary_pg);

    cluster.begin_bucket_delete(&bucket_a).unwrap();

    let scan = crate::clock::with_time_override(21, || {
        cluster.enqueue_durable_bucket_delete_finalize_roots()
    });
    assert_eq!(scan.errors, 0);
    assert_eq!(
        scan.queued, 2,
        "scan should enqueue the expired claimed bucket and the earlier deleting bucket"
    );
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_bucket))
            if queued_bucket == bucket_b
    ));
    assert_eq!(
        crate::clock::with_time_override(21, || { cluster.try_finalize_bucket_delete(&bucket_b) })
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized,
        "expired stale claim work should be recoverable from the durable scan"
    );
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_bucket))
            if queued_bucket == bucket_a
    ));
    assert_eq!(
        crate::clock::with_time_override(22, || { cluster.try_finalize_bucket_delete(&bucket_a) })
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn durable_bucket_finalize_scan_continues_after_unavailable_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-finalize-scan-later-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster.begin_bucket_delete(&bucket).unwrap();
    drop(cluster);

    let mut map = Arc::try_unwrap(map).expect("test should hold the only map reference");
    map.pg_routes.get_mut(&PgId::new(0)).unwrap().state = PgState::Peering;
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let scan = cluster.enqueue_durable_bucket_delete_finalize_roots();
    assert_eq!(
        scan.errors, 1,
        "unavailable PG should be reported in scan stats"
    );
    assert_eq!(
        scan.queued, 1,
        "scan should continue and enqueue the later healthy deleting bucket"
    );
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDelete(queued_bucket))
            if queued_bucket == bucket
    ));
}

#[test]
fn bucket_finalize_durable_claim_blocks_second_worker_until_released() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-finalize-claim-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster.begin_bucket_delete(&bucket).unwrap();

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let deleting = crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket).unwrap();
    let claimed_at = crate::clock::current_time_millis();
    let held = crate::PgMetadataStore::acquire_bucket_delete_finalize_claim(
        &*primary_pg,
        &bucket,
        deleting.bucket_incarnation_generation,
        "held-finalizer-claim",
        "external-worker",
        ClusterEpoch::INITIAL,
        claimed_at,
        claimed_at.checked_add(60_000),
        claimed_at,
    )
    .unwrap()
    .expect("test should be able to hold the finalizer claim");
    drop(primary_pg);

    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Pending,
        "a non-expired durable finalizer claim should block a second worker"
    );
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Deleting
    );

    crate::PgMetadataStore::release_bucket_delete_finalize_claim(
        &*primary_pg,
        &bucket,
        deleting.bucket_incarnation_generation,
        &held.claim_id,
        &held.owner_token,
        held.cluster_epoch,
    )
    .unwrap();
    drop(primary_pg);
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
}

#[test]
fn finalized_bucket_delete_releases_finalizer_claim_for_next_same_pg_bucket() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket_a, bucket_b) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "delete-finalize-release-a-"),
            bucket_for_pg(topology, 1, "delete-finalize-release-b-"),
        )
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket_a);
    create_test_bucket(&cluster, &bucket_b);

    cluster.begin_bucket_delete(&bucket_a).unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket_a).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );

    cluster.begin_bucket_delete(&bucket_b).unwrap();
    assert_eq!(
            cluster.try_finalize_bucket_delete(&bucket_b).unwrap(),
            crate::BucketDeleteFinalizeOutcome::Finalized,
            "a terminal bucket finalizer must release its singleton PG claim before unrelated same-PG work"
        );
}

#[test]
fn finalized_bucket_delete_waits_for_reclaim_then_finalizes_after_worker_progress() {
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
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg, NodeId::new(1));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"finalize reclaim");
    let lease = cluster
        .acquire_object_payload_lease(&bucket, &key, committed.generation_id)
        .unwrap();

    let delete_outcome = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(matches!(
        delete_outcome.deleted,
        crate::DeletedCurrentObject::Live {
            generation_id,
            ..
        } if generation_id == committed.generation_id
    ));
    assert!(
        cluster
            .payload_reclaim_exists(&bucket, &key, committed.generation_id)
            .unwrap(),
        "object delete should leave payload reclaim metadata"
    );

    cluster.begin_bucket_delete(&bucket).unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Pending,
        "finalization must wait while reclaim metadata remains"
    );

    let released = lease.release();
    assert_eq!(released.remaining(), 0);
    assert!(
        cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "worker progress should clear the reclaim root after the read lease releases"
    );
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    assert_clean_metadata_command_stream(&map, &[1, object_pg]);
}

#[test]
fn finalized_bucket_delete_ignores_volatile_read_lease_without_reclaim_root() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-phantom-lease-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let phantom_key = crate::ObjectKey::try_from("phantom-read".to_string()).unwrap();
    let phantom_lease = cluster
        .acquire_object_payload_lease(&bucket, &phantom_key, crate::GenerationId::MIN)
        .unwrap();
    assert_eq!(cluster.bucket_object_payload_lease_count(&bucket), 1);

    cluster.begin_bucket_delete(&bucket).unwrap();
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized,
        "volatile read handles without durable reclaim roots must not wedge bucket finalization"
    );
    drop(phantom_lease);
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn finalized_bucket_delete_preserves_unrelated_same_pg_pending_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (deleting_bucket, pending_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "delete-pending-target-"),
            bucket_for_pg(topology, 1, "delete-pending-survivor-"),
        )
    };
    assert_ne!(deleting_bucket, pending_bucket);
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &deleting_bucket);
    create_test_bucket(&cluster, &pending_bucket);
    cluster.begin_bucket_delete(&deleting_bucket).unwrap();

    let pg_id = PgId::new(1);
    let pending_log_index = map.test_next_metadata_command_log_index(pg_id);
    let pending_command = {
        let primary_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let current =
            crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &pending_bucket).unwrap();
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, pending_log_index),
            MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
                current.with_execution_generation(
                    primary_pg
                        .next_bucket_execution_generation_candidate()
                        .unwrap(),
                ),
                crate::BucketVersioningState::Enabled,
            )),
        )
    };
    insert_pending_metadata_command_for_test(&map, pg_id, &pending_bucket, &pending_command);

    assert_eq!(
        cluster
            .try_finalize_bucket_delete(&deleting_bucket)
            .unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );

    assert!(
        pending_metadata_command_for_test(&map, pg_id, &pending_bucket).is_none(),
        "bucket finalization should drain same-PG pending work rather than dropping it"
    );
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(
            &*map
                .node(NodeId::new(1))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap(),
            &pending_bucket,
        )
        .unwrap()
        .versioning,
        crate::BucketVersioningState::Enabled
    );
}

#[test]
fn begin_bucket_delete_retries_when_pending_slot_wins_before_command_id() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-command-id-race-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let initial_bucket = cluster.test_head_bucket_raw(&bucket).unwrap();
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard =
        cluster.test_install_before_bucket_delete_command_id_hook(Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let log_index = hook_map.test_next_metadata_command_log_index(pg_id);
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let current =
                crate::PgMetadataStore::head_bucket_record_raw(&*pg, &hook_bucket).unwrap();
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(ClusterEpoch::INITIAL, pg_id, log_index),
                MetadataCommandPayload::PutBucketVersioning(
                    PutBucketVersioningCommand::from_bucket(
                        current.with_execution_generation(
                            pg.next_bucket_execution_generation_candidate().unwrap(),
                        ),
                        crate::BucketVersioningState::Enabled,
                    ),
                ),
            );
            drop(pg);
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
        }));

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
            matches!(
                err,
                crate::BucketWriteDrainError::Store(StoreError::MetadataCommandContention { .. })
            ),
            "bucket delete owner should return retryable contention after a winning pending slot advances the bucket generation, got {err:?}"
        );
    {
        let pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*pg, &bucket)
            .unwrap()
            .expect("retryable DeleteBucket begin should record an attempt outcome");
        assert_eq!(
            outcome.outcome,
            crate::BucketDeleteAttemptOutcomeKind::Retryable
        );
        assert_eq!(
            outcome.phase,
            crate::BucketDeleteAttemptPhase::StreamCleanup
        );
    }
    assert_eq!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDeleteBegin(
            crate::BucketDeleteBeginRoot {
                bucket: bucket.clone(),
                bucket_execution_generation: initial_bucket.bucket_execution_generation,
                bucket_incarnation_generation: initial_bucket.bucket_incarnation_generation,
            }
        )),
        "retryable preserved DeleteBucket begin should queue background resume work"
    );
    cluster.begin_bucket_delete(&bucket).unwrap();
    {
        let pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*pg, &bucket)
            .unwrap()
            .expect("successful DeleteBucket begin should record an attempt outcome");
        assert_eq!(
            outcome.outcome,
            crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
        );
        assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    }

    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should install a contender before MarkBucketDeleting id allocation"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "bucket delete should drain the winning pending slot before retrying"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
        assert_eq!(info.versioning, crate::BucketVersioningState::Enabled);
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_bucket_delete_retries_after_partial_mark_deleting_conflict() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-partial-conflict-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if node_id != NodeId::new(0) || hook_ran_for_closure.load(Ordering::SeqCst) {
                return Ok(());
            }
            match command.payload() {
                MetadataCommandPayload::MarkBucketDeleting(mark)
                    if mark.bucket_name() == &hook_bucket =>
                {
                    hook_ran_for_closure.store(true, Ordering::SeqCst);
                    let node = hook_map.node(node_id).unwrap().storage_node();
                    let pg = node.get_pg(command.id().pg_id().get())?;
                    pg.apply_metadata_command_and_record(node_id.as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual mark deleting command apply failed: {error}")
                            }
                        })?;
                    Err(StoreError::MetadataCommandLogConflict {
                        node_id: node_id.as_u32(),
                        pg_id: command.id().pg_id().get(),
                        cluster_epoch: command.id().cluster_epoch(),
                        log_index: command.id().log_index().get(),
                    })
                }
                _ => Ok(()),
            }
        },
    ));

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::MetadataCommandContention { .. })
        ),
        "partial exact mark deleting conflict should ask the caller to retry, got {err:?}"
    );
    cluster.begin_bucket_delete(&bucket).unwrap();

    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should inject a command-log conflict after non-primary replicas apply"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "bucket delete should finish and clear the pending slot after retrying"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_bucket_delete_reissues_stale_duplicate_mark_deleting_index() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let (occupant_bucket, delete_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, pg_id.get(), "delete-stale-occupant-"),
            bucket_for_pg(topology, pg_id.get(), "delete-stale-mark-"),
        )
    };
    create_test_bucket(&cluster, &occupant_bucket);
    create_test_bucket(&cluster, &delete_bucket);

    let stale_command_id = cluster.next_bucket_metadata_command_id(pg_id).unwrap();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let occupant_current =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &occupant_bucket).unwrap();
    let occupant_command = MetadataCommandEnvelope::new(
        stale_command_id,
        MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
            occupant_current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
            crate::BucketVersioningState::Enabled,
        )),
    );
    let delete_current =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &delete_bucket).unwrap();
    let stale_delete_command = MetadataCommandEnvelope::new(
        stale_command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            delete_current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
        )),
    );
    drop(primary_pg);

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &occupant_command)
            .unwrap();
    }
    force_insert_pending_metadata_command_for_test(
        &map,
        pg_id,
        &delete_bucket,
        &stale_delete_command,
    );

    cluster.begin_bucket_delete(&delete_bucket).unwrap();

    assert!(
        pending_metadata_command_for_test(&map, pg_id, &delete_bucket).is_none(),
        "stale duplicate-index mark command should be reissued and cleared"
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let occupant = crate::PgMetadataStore::head_bucket_raw(&*pg, &occupant_bucket).unwrap();
        assert_eq!(occupant.versioning, crate::BucketVersioningState::Enabled);
        let deleted = crate::PgMetadataStore::head_bucket_raw(&*pg, &delete_bucket).unwrap();
        assert_eq!(deleted.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_bucket_delete_reissue_waits_for_primary_last_apply_window() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let pg_id = PgId::new(1);
    let (occupant_bucket, delete_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, pg_id.get(), "delete-window-occupant-"),
            bucket_for_pg(topology, pg_id.get(), "delete-window-mark-"),
        )
    };
    create_test_bucket(&cluster, &occupant_bucket);
    create_test_bucket(&cluster, &delete_bucket);

    let stale_command_id = cluster.next_bucket_metadata_command_id(pg_id).unwrap();
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let occupant_current =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &occupant_bucket).unwrap();
    let occupant_command = MetadataCommandEnvelope::new(
        stale_command_id,
        MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
            occupant_current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
            crate::BucketVersioningState::Enabled,
        )),
    );
    let delete_current =
        crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &delete_bucket).unwrap();
    let stale_delete_command = MetadataCommandEnvelope::new(
        stale_command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            delete_current.with_execution_generation(
                primary_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
        )),
    );
    drop(primary_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &delete_bucket, &stale_delete_command);

    let primary_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release_primary = Arc::new((Mutex::new(false), Condvar::new()));
    let occupant_map = Arc::clone(&map);
    let occupant_command_for_thread = occupant_command.clone();
    let primary_gate_for_thread = Arc::clone(&primary_gate);
    let release_primary_for_thread = Arc::clone(&release_primary);
    let occupant_thread = std::thread::spawn(move || {
        let pg_lock = occupant_map.runtime_state().metadata_command_pg_lock(pg_id);
        let _pg_guard = pg_lock.lock().unwrap();
        for node_id in [NodeId::new(1), NodeId::new(2)] {
            let pg = occupant_map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(pg_id.get())
                .unwrap();
            pg.apply_metadata_command_and_record(node_id.as_u32(), &occupant_command_for_thread)
                .unwrap();
        }
        {
            let (lock, cv) = &*primary_gate_for_thread;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }
        {
            let (lock, cv) = &*release_primary_for_thread;
            let _guard = cv
                .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |released| {
                    !*released
                })
                .unwrap();
        }
        let pg = occupant_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.apply_metadata_command_and_record(0, &occupant_command_for_thread)
            .unwrap();
    });

    {
        let (lock, cv) = &*primary_gate;
        let guard = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |at_primary| {
                !*at_primary
            })
            .unwrap()
            .0;
        assert!(
            *guard,
            "occupant command should pause in the primary-last apply window"
        );
    }

    let reissue_cluster = cluster.clone();
    let stale_for_thread = stale_delete_command.clone();
    let reissue_thread = std::thread::spawn(move || {
        reissue_cluster
            .test_reissue_pending_metadata_command(pg_id, &stale_for_thread)
            .unwrap()
    });

    {
        let (lock, cv) = &*release_primary;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
    occupant_thread.join().unwrap();
    let replacement = reissue_thread
        .join()
        .unwrap()
        .expect("stale delete command should be reissued after in-flight apply finishes");
    assert_eq!(
        replacement.id().log_index().get(),
        stale_command_id.log_index().get() + 1
    );
    assert_eq!(replacement.payload(), stale_delete_command.payload());

    cluster.begin_bucket_delete(&delete_bucket).unwrap();

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let occupant = crate::PgMetadataStore::head_bucket_raw(&*pg, &occupant_bucket).unwrap();
        assert_eq!(occupant.versioning, crate::BucketVersioningState::Enabled);
        let deleted = crate::PgMetadataStore::head_bucket_raw(&*pg, &delete_bucket).unwrap();
        assert_eq!(deleted.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_bucket_delete_retries_after_partial_object_pg_drain_conflict() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, key) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-object-pg-conflict-");
        let key = key_for_object_pg(topology, &bucket, 2, "key-");
        (bucket, key)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let object_pg_id = PgId::new(2);
    let reservation_id = crate::SessionId::try_from("44".repeat(16)).unwrap();
    let command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(object_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key.clone(),
            reservation_id.clone(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, object_pg_id, &bucket, &command);

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if node_id != NodeId::new(0) || hook_ran_for_closure.load(Ordering::SeqCst) {
                return Ok(());
            }
            match command.payload() {
                MetadataCommandPayload::ReserveObjectGeneration(reservation)
                    if reservation.bucket == hook_bucket && reservation.key == hook_key =>
                {
                    hook_ran_for_closure.store(true, Ordering::SeqCst);
                    let node = hook_map.node(node_id).unwrap().storage_node();
                    let pg = node.get_pg(command.id().pg_id().get())?;
                    pg.apply_metadata_command_and_record(node_id.as_u32(), command)
                        .map_err(|error| match error {
                            crate::BucketSnapshotLoadError::Store(error) => error,
                            crate::BucketSnapshotLoadError::Metadata(error) => {
                                panic!("manual object PG command apply failed: {error}")
                            }
                        })?;
                    Err(StoreError::MetadataCommandLogConflict {
                        node_id: node_id.as_u32(),
                        pg_id: command.id().pg_id().get(),
                        cluster_epoch: command.id().cluster_epoch(),
                        log_index: command.id().log_index().get(),
                    })
                }
                _ => Ok(()),
            }
        },
    ));

    cluster.begin_bucket_delete(&bucket).unwrap();

    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should inject a command-log conflict after non-primary object replicas apply"
    );
    assert!(
        pending_metadata_command_for_test(&map, object_pg_id, &bucket).is_none(),
        "bucket delete should finish object-PG drain instead of surfacing a retryable conflict"
    );
    for node_id in node_ids {
        let bucket_pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);

        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id.get())
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*object_pg,
                &bucket,
                &key,
                &reservation_id,
            )
            .unwrap(),
            crate::GenerationId::MIN
        );
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id.get()]);
}

#[test]
fn begin_bucket_delete_skips_unrelated_all_pg_drain_slot() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, pending_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "delete-unrelated-bucket-drain-"),
            bucket_for_pg(topology, 2, "delete-pending-bucket-drain-"),
        )
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    create_test_bucket(&cluster, &pending_bucket);

    let pending_pg_id = PgId::new(2);
    let command_id = cluster
        .next_bucket_metadata_command_id(pending_pg_id)
        .unwrap();
    let pending_primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pending_pg_id)
        .unwrap();
    let pending_pg = pending_primary
        .storage_node()
        .get_pg(pending_pg_id.get())
        .unwrap();
    let current =
        crate::PgMetadataStore::head_bucket_record_raw(&*pending_pg, &pending_bucket).unwrap();
    let pending_command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            current.with_execution_generation(
                pending_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            ),
        )),
    );
    drop(pending_pg);
    insert_pending_metadata_command_for_test(
        &map,
        pending_pg_id,
        &pending_bucket,
        &pending_command,
    );

    cluster.begin_bucket_delete(&bucket).unwrap();

    assert!(
        pending_metadata_command_for_test(&map, pending_pg_id, &pending_bucket).is_some(),
        "bucket delete should not drain unrelated bucket-PG work found during all-PG scan"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let bucket_pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);

        let pending_pg = node.get_pg(pending_pg_id.get()).unwrap();
        let pending_info =
            crate::PgMetadataStore::head_bucket_raw(&*pending_pg, &pending_bucket).unwrap();
        assert_eq!(pending_info.state, crate::BucketState::Active);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn begin_bucket_delete_fails_closed_on_divergent_same_index_after_partial_apply() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-divergent-conflict-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let injected = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let injected_for_closure = Arc::clone(&injected);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::MarkBucketDeleting(mark)
                    if mark.bucket_name() == &hook_bucket
                        && node_id == NodeId::new(0)
                        && !injected_for_closure.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(node_id).unwrap().storage_node();
                    let node_pg = node.get_pg(pg_id.get()).unwrap();
                    let current =
                        crate::PgMetadataStore::head_bucket_record_raw(&*node_pg, &hook_bucket)
                            .unwrap();
                    let divergent = MetadataCommandEnvelope::new(
                        command.id(),
                        MetadataCommandPayload::PutBucketVersioning(
                            PutBucketVersioningCommand::from_bucket(
                                current.with_execution_generation(
                                    node_pg
                                        .next_bucket_execution_generation_candidate()
                                        .unwrap(),
                                ),
                                crate::BucketVersioningState::Enabled,
                            ),
                        ),
                    );
                    node_pg
                        .apply_metadata_command_and_record(node_id.as_u32(), &divergent)
                        .unwrap();
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();

    assert!(
        injected.load(Ordering::SeqCst),
        "test hook should inject a divergent same-index command on a replica"
    );
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::MetadataCommandLogConflict { .. })
        ),
        "divergent same-index command log state must fail closed, got {err:?}"
    );
}

#[test]
fn begin_bucket_delete_partial_mark_deleting_reopens_and_converges() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-mark-reopen-")
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let command_id = cluster.next_bucket_metadata_command_id(pg_id).unwrap();
    let command = {
        let primary_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let now = crate::clock::current_time_millis();
        crate::PgMetadataStore::begin_durable_bucket_write_drain(
            &*primary_pg,
            &bucket,
            "delete-mark-reopen-drain",
            "delete-mark-reopen-owner",
            crate::ClusterEpoch::INITIAL,
            now,
            None,
        )
        .unwrap();
        let current =
            crate::PgMetadataStore::head_bucket_record_raw(&*primary_pg, &bucket).unwrap();
        let command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
                current.with_execution_generation(
                    primary_pg
                        .next_bucket_execution_generation_candidate()
                        .unwrap(),
                ),
            )),
        );
        primary_pg
            .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
            .unwrap();
        command
    };
    for node_id in [NodeId::new(1), NodeId::new(2)] {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &command)
            .unwrap();
    }
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_some(),
        "partial MarkBucketDeleting must leave the primary pending slot durable"
    );
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
    }
    assert!(
        pending_metadata_command_for_test(&reopened, pg_id, &bucket).is_none(),
        "open-time convergence should clear terminal MarkBucketDeleting pending slot"
    );
    let primary_pg = reopened
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*primary_pg, &bucket)
            .unwrap()
            .is_some(),
        "terminal delete drain should remain durable after reopen convergence"
    );
    drop(primary_pg);
    assert_clean_metadata_command_stream(&reopened, &[1]);
}

#[test]
fn bucket_update_fails_closed_on_divergent_same_index_after_partial_apply() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "bucket-update-divergent-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let injected = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let injected_for_closure = Arc::clone(&injected);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::PutBucketVersioning(versioning)
                    if versioning.bucket_name() == &hook_bucket
                        && node_id == NodeId::new(0)
                        && !injected_for_closure.swap(true, Ordering::SeqCst) =>
                {
                    let node = hook_map.node(node_id).unwrap().storage_node();
                    let node_pg = node.get_pg(pg_id.get()).unwrap();
                    let current =
                        crate::PgMetadataStore::head_bucket_record_raw(&*node_pg, &hook_bucket)
                            .unwrap();
                    let divergent = MetadataCommandEnvelope::new(
                        command.id(),
                        MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                            current.with_execution_generation(
                                node_pg
                                    .next_bucket_execution_generation_candidate()
                                    .unwrap(),
                            ),
                            crate::AclGrants::default(),
                            false,
                            false,
                        )),
                    );
                    node_pg
                        .apply_metadata_command_and_record(node_id.as_u32(), &divergent)
                        .unwrap();
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap_err();

    assert!(
        injected.load(Ordering::SeqCst),
        "test hook should inject a divergent same-index command on a replica"
    );
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict { .. })
        ),
        "ordinary bucket-PG finish conflicts must fail closed, got {err:?}"
    );
}

#[test]
fn finalized_bucket_delete_fails_closed_on_active_replica() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-diverged-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster.begin_bucket_delete(&bucket).unwrap();

    let divergent_node = map.node(NodeId::new(0)).unwrap().storage_node();
    let divergent_pg = divergent_node.get_pg(1).unwrap();
    crate::PgMetadataStore::delete_finalized_bucket(&*divergent_pg, &bucket).unwrap();
    divergent_pg
        .refresh_metadata_command_state_digest()
        .unwrap();
    crate::PgMetadataStore::create_bucket(
        &*divergent_pg,
        &bucket,
        "owner",
        &crate::CanonicalUserId::from_principal("owner"),
        &crate::AclGrants::default(),
        false,
        false,
    )
    .unwrap();
    divergent_pg
        .refresh_metadata_command_state_digest()
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*divergent_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Active
    );
    drop(divergent_pg);

    let err = cluster.try_finalize_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(
                crate::MetadataError::BucketNotFinalizedForDelete {
                    state: crate::BucketState::Active
                }
            )
        ),
        "expected active replica to fail finalized delete, got {err:?}"
    );

    let divergent_pg = divergent_node.get_pg(1).unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*divergent_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Active
    );
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Deleting
    );
}

#[test]
fn bucket_snapshot_pair_routes_to_bucket_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (source_bucket, destination_bucket) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "snapshot-source-"),
            bucket_for_pg(topology, 2, "snapshot-destination-"),
        )
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &source_bucket);
    create_test_bucket(&cluster, &destination_bucket);
    cluster
        .put_bucket_subresource_and_load_info(
            &source_bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Tagging,
                body:
                    "<Tagging><TagSet><Tag><Key>src</Key><Value>1</Value></Tag></TagSet></Tagging>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    cluster
        .put_bucket_subresource_and_load_info(
            &destination_bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Cors,
                body: "<CORSConfiguration/>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();

    let pair = cluster
        .load_bucket_snapshot_pair(
            (
                &source_bucket,
                crate::BucketSnapshotRequest {
                    tags: crate::BucketSnapshotTagsRequest::Always,
                    ..Default::default()
                },
            ),
            (
                &destination_bucket,
                crate::BucketSnapshotRequest {
                    cors: true,
                    ..Default::default()
                },
            ),
        )
        .unwrap();
    assert_eq!(pair.source().bucket.name, source_bucket);
    assert_eq!(pair.destination().bucket.name, destination_bucket);
    assert_eq!(
        pair.source().tags,
        crate::LoadedBucketSubresource::Loaded(
            "<Tagging><TagSet><Tag><Key>src</Key><Value>1</Value></Tag></TagSet></Tagging>"
                .to_string()
        )
    );
    assert_eq!(
        pair.destination().cors,
        crate::LoadedBucketSubresource::Loaded("<CORSConfiguration/>".to_string())
    );
}

#[test]
fn bucket_snapshot_fails_closed_while_bucket_pg_is_peering() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "snapshot-peering-")
    };
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    drop(cluster);

    Arc::get_mut(&mut map)
        .unwrap()
        .pg_routes
        .get_mut(&PgId::new(1))
        .unwrap()
        .state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(map).unwrap();

    let err = cluster
        .load_bucket_snapshot(&bucket, crate::BucketSnapshotRequest::default())
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::PgNotActive {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Peering,
        })
    ));
}

#[test]
fn bucket_snapshot_pair_fails_closed_while_either_bucket_pg_is_peering() {
    let assert_pair_fails = |peering_pg: u32| {
        let tmp = test_util::tempdir();
        let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
        let ec_shape = EcShape { k: 2, m: 1 };
        let mut map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap());
        let (source_bucket, destination_bucket) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            (
                bucket_for_pg(topology, 1, "snapshot-pair-source-peering-"),
                bucket_for_pg(topology, 2, "snapshot-pair-destination-peering-"),
            )
        };
        let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
        create_test_bucket(&cluster, &source_bucket);
        create_test_bucket(&cluster, &destination_bucket);
        drop(cluster);

        Arc::get_mut(&mut map)
            .unwrap()
            .pg_routes
            .get_mut(&PgId::new(peering_pg))
            .unwrap()
            .state = PgState::Peering;
        let cluster = crate::StorageCluster::from_local_map(map).unwrap();

        let err = cluster
            .load_bucket_snapshot_pair(
                (&source_bucket, crate::BucketSnapshotRequest::default()),
                (&destination_bucket, crate::BucketSnapshotRequest::default()),
            )
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketSnapshotLoadError::Store(StoreError::PgNotActive {
                    pg_id,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    state: PgState::Peering,
                }) if pg_id == peering_pg
            ),
            "unexpected snapshot-pair error for Peering PG {peering_pg}: {err:?}"
        );
    };

    assert_pair_fails(1);
    assert_pair_fails(2);
}

#[test]
fn composite_multipart_and_lifecycle_scans_fan_out_to_routed_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let topology = map
        .nodes
        .get(&NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let upload_bucket = crate::BucketName::try_from("multipart-fanout-bucket".to_string()).unwrap();
    let lifecycle_bucket = bucket_for_pg(topology, 1, "lifecycle-bucket-");
    let aborting_bucket = bucket_for_pg(topology, 1, "aborting-bucket-");
    let key_a = key_for_object_pg(topology, &upload_bucket, 1, "uploads/a-");
    let key_b = key_for_object_pg(topology, &upload_bucket, 2, "uploads/b-");
    let aborting_key = key_for_object_pg(topology, &aborting_bucket, 2, "abort-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &lifecycle_bucket);
    create_test_bucket(&cluster, &aborting_bucket);
    cluster
        .put_bucket_subresource_and_load_info(
            &lifecycle_bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();

    let upload_a = upload_id_from_label("routedUploadA");
    let upload_b = upload_id_from_label("routedUploadB");
    let aborting_upload = upload_id_from_label("abortingUpload");
    seed_multipart_upload_record(
        &map,
        NodeId::new(1),
        1,
        &upload_bucket,
        &key_a,
        &upload_a,
        crate::UploadState::InProgress,
    );
    seed_multipart_upload_record(
        &map,
        NodeId::new(2),
        2,
        &upload_bucket,
        &key_b,
        &upload_b,
        crate::UploadState::InProgress,
    );
    seed_multipart_upload_record(
        &map,
        NodeId::new(2),
        2,
        &aborting_bucket,
        &aborting_key,
        &aborting_upload,
        crate::UploadState::Aborting,
    );

    let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
    assert!(bridge_node
        .test_list_multipart_uploads_for_bucket(&upload_bucket)
        .unwrap()
        .is_empty());
    let all_uploads = cluster
        .list_all_multipart_uploads_for_bucket(&upload_bucket)
        .unwrap();
    assert_eq!(
        all_uploads
            .iter()
            .map(|upload| (&upload.key, &upload.upload_id))
            .collect::<Vec<_>>(),
        vec![(&key_a, &upload_a), (&key_b, &upload_b)]
    );

    let mut listed_uploads = cluster
        .list_multipart_uploads_for_bucket(&upload_bucket, None, None, None, 100, 100)
        .unwrap()
        .uploads;
    listed_uploads.sort_by(|left, right| left.key.cmp(&right.key));
    assert_eq!(
        listed_uploads
            .iter()
            .map(|upload| (&upload.key, &upload.upload_id))
            .collect::<Vec<_>>(),
        vec![(&key_a, &upload_a), (&key_b, &upload_b)]
    );

    let sweep = cluster.list_lifecycle_sweep_buckets().unwrap();
    assert_eq!(
        sweep
            .lifecycle_buckets
            .iter()
            .map(|bucket| &bucket.name)
            .collect::<Vec<_>>(),
        vec![&lifecycle_bucket]
    );
    assert_eq!(sweep.aborting_buckets, vec![aborting_bucket.clone()]);

    let roots = cluster.list_lifecycle_sweep_roots(0).unwrap();
    assert_eq!(
        roots
            .iter()
            .map(|root| (&root.bucket, root.source))
            .collect::<Vec<_>>(),
        vec![
            (
                &lifecycle_bucket,
                crate::LifecycleSweepRootSource::LifecycleConfig
            ),
            (
                &aborting_bucket,
                crate::LifecycleSweepRootSource::AbortingMultipartUpload,
            ),
        ]
    );
}

#[test]
fn completed_multipart_prune_fans_out_to_routed_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = crate::BucketName::try_from("completed-prune-bucket".to_string()).unwrap();
    let topology = map
        .nodes
        .get(&NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let older_key = key_for_object_pg(topology, &bucket, 1, "older-");
    let newer_key = key_for_object_pg(topology, &bucket, 2, "newer-");
    let post_prune_bucket = bucket_for_pg(topology, 1, "post-prune-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let older_upload = upload_id_from_label("olderCompleted");
    let newer_upload = upload_id_from_label("newerCompleted");
    seed_completed_multipart_upload_record(
        &map,
        NodeId::new(1),
        1,
        &bucket,
        &older_key,
        &older_upload,
        1,
    );
    seed_completed_multipart_upload_record(
        &map,
        NodeId::new(2),
        2,
        &bucket,
        &newer_key,
        &newer_upload,
        2,
    );

    let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
    assert!(bridge_node
        .get_pg(1)
        .unwrap()
        .list_completed_multipart_uploads_for_bucket(bucket.as_str())
        .unwrap()
        .is_empty());
    assert!(bridge_node
        .get_pg(2)
        .unwrap()
        .list_completed_multipart_uploads_for_bucket(bucket.as_str())
        .unwrap()
        .is_empty());

    cluster
        .prune_completed_multipart_uploads_for_bucket_with_limit(&bucket, 1)
        .unwrap();

    let node_one_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::get_completed_multipart_upload(&*node_one_pg, &older_upload)
            .unwrap()
            .is_none(),
        "older routed completed-upload tombstone should be pruned"
    );
    let node_two_pg = map
        .node(NodeId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    assert!(
        crate::PgMetadataStore::get_completed_multipart_upload(&*node_two_pg, &newer_upload)
            .unwrap()
            .is_some(),
        "newer routed completed-upload tombstone should be retained"
    );
    drop(node_one_pg);
    drop(node_two_pg);

    create_test_bucket(&cluster, &post_prune_bucket);
}

#[test]
fn completed_multipart_prune_partial_command_retries_and_preserves_digest() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = crate::BucketName::try_from("completed-prune-retry-bucket".to_string()).unwrap();
    let topology = map
        .nodes
        .get(&NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let key = key_for_object_pg(topology, &bucket, 1, "retry-");
    let post_prune_bucket = bucket_for_pg(topology, 1, "post-prune-retry-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();

    let upload_id = upload_id_from_label("retryCompleted");
    for node_id in node_ids {
        seed_completed_multipart_upload_record(&map, node_id, 1, &bucket, &key, &upload_id, 1);
    }

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::DeleteCompletedMultipartUpload(_)
            ) && node_id == NodeId::new(2)
                && fail_once_hook.swap(false, Ordering::SeqCst)
            {
                return Err(StoreError::Io {
                    context: "injected completed multipart prune failure",
                    source: std::io::Error::other("injected completed multipart prune failure"),
                });
            }
            Ok(())
        },
    ));

    let err = cluster
        .prune_completed_multipart_uploads_for_bucket_with_limit(&bucket, 0)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io { .. })
        ),
        "expected injected partial prune failure, got {err:?}"
    );
    assert!(
        crate::PgMetadataStore::get_completed_multipart_upload(
            &*map
                .node(NodeId::new(0))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap(),
            &upload_id,
        )
        .unwrap()
        .is_none(),
        "first applied replica should have deleted the tombstone"
    );
    assert!(
        crate::PgMetadataStore::get_completed_multipart_upload(
            &*map
                .node(NodeId::new(2))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap(),
            &upload_id,
        )
        .unwrap()
        .is_some(),
        "failed replica should still have the tombstone before retry"
    );
    drop(hook_guard);

    cluster
        .prune_completed_multipart_uploads_for_bucket_with_limit(&bucket, 0)
        .unwrap();
    for node_id in node_ids {
        assert!(
            crate::PgMetadataStore::get_completed_multipart_upload(
                &*map.node(node_id).unwrap().storage_node().get_pg(1).unwrap(),
                &upload_id,
            )
            .unwrap()
            .is_none(),
            "retry should delete tombstone on node {node_id:?}"
        );
    }
    create_test_bucket(&cluster, &post_prune_bucket);
}

#[test]
fn bucket_delete_and_finalize_fan_out_to_routed_pg_primaries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, live_key, tombstone_key) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-bucket-");
        let live_key = key_for_object_pg(topology, &bucket, 2, "live-");
        let tombstone_key = key_for_object_pg(topology, &bucket, 2, "tombstone-");
        (bucket, live_key, tombstone_key)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    write_committed_direct_segment_for_with_okh(&cluster, &bucket, &live_key, [61; 16], b"live");

    let bridge_node = map.node(NodeId::new(0)).unwrap().storage_node();
    assert!(bridge_node.test_get_object_meta(&bucket, &live_key).is_ok());

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "routed non-empty bucket should reject delete, got {err:?}"
    );
    {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
                .unwrap()
                .is_none(),
            "non-empty DeleteBucket must roll back the temporary durable drain"
        );
        let _ = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket)
            .expect("non-empty delete should leave the bucket active");
    }

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(2).unwrap();
        crate::PgMetadataStore::delete_object_meta(&*pg, &bucket, &live_key).unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    let node_two = map.node(NodeId::new(2)).unwrap().storage_node();
    let completed_upload = upload_id_from_label("deleteCompleted");
    seed_completed_multipart_upload_record(
        &map,
        NodeId::new(2),
        2,
        &bucket,
        &tombstone_key,
        &completed_upload,
        1,
    );
    let node_two_pg = node_two.get_pg(2).unwrap();
    crate::PgMetadataStore::delete_object_meta(&*node_two_pg, &bucket, &tombstone_key).unwrap();
    node_two_pg.refresh_metadata_command_state_digest().unwrap();
    assert!(crate::PgMetadataStore::get_completed_multipart_upload(
        &*node_two_pg,
        &completed_upload
    )
    .unwrap()
    .is_some());
    drop(node_two_pg);

    cluster.begin_bucket_delete(&bucket).unwrap();
    {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
                .unwrap()
                .is_some(),
            "successful DeleteBucket begin should leave a terminal durable drain until finalize"
        );
    }
    assert!(
        matches!(
            cluster.begin_durable_bucket_delete_drain(&bucket).unwrap(),
            crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting
        ),
        "durable delete-drain conflict must observe terminal Deleting as idempotent success"
    );
    cluster
        .begin_bucket_delete(&bucket)
        .expect("retrying DeleteBucket after MarkBucketDeleting should be idempotent");
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );

    assert!(map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .test_head_bucket_raw(&bucket)
        .is_err());
    let node_two_pg = node_two.get_pg(2).unwrap();
    assert!(
        crate::PgMetadataStore::get_completed_multipart_upload(&*node_two_pg, &completed_upload)
            .unwrap()
            .is_none(),
        "finalization should prune routed completed-upload tombstones"
    );
}

#[test]
fn bucket_control_plane_pending_install_waits_behind_durable_delete_drain() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "control-plane-drain-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(1);
    let primary = map.node(NodeId::new(1)).unwrap().storage_node();
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };

    let versioning_command_id = MetadataCommandId::new(
        crate::ClusterEpoch::INITIAL,
        pg_id,
        MetadataCommandLogIndex::new(2).unwrap(),
    );
    let versioning_command = {
        let bucket_pg = primary.get_pg(pg_id.get()).unwrap();
        let current = crate::PgMetadataStore::head_bucket_record_raw(&*bucket_pg, &bucket)
            .unwrap()
            .with_execution_generation(
                bucket_pg
                    .next_bucket_execution_generation_candidate()
                    .unwrap(),
            );
        MetadataCommandEnvelope::new(
            versioning_command_id,
            MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
                current,
                crate::BucketVersioningState::Enabled,
            )),
        )
    };
    assert!(
        !cluster
            .try_set_bucket_control_pending_command_or_retry(pg_id, &bucket, &versioning_command)
            .unwrap(),
        "versioning command must not install while a durable delete drain is active"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "blocked bucket control-plane command must not leave a pending slot"
    );

    let lifecycle_command_id = MetadataCommandId::new(
        crate::ClusterEpoch::INITIAL,
        pg_id,
        MetadataCommandLogIndex::new(2).unwrap(),
    );
    let lifecycle_command = {
        let bucket_pg = primary.get_pg(pg_id.get()).unwrap();
        let generation = bucket_pg
            .next_bucket_execution_generation_candidate()
            .unwrap();
        MetadataCommandEnvelope::new(
            lifecycle_command_id,
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                bucket.clone(),
                BucketSubresourceMutation::Put {
                    kind: crate::BucketSubresourceKind::Lifecycle,
                    body: "<LifecycleConfiguration/>".to_string(),
                    aux: crate::BucketSubresourceAux::None,
                },
                generation,
            )),
        )
    };
    assert!(
        !cluster
            .try_set_bucket_control_pending_command_or_retry(pg_id, &bucket, &lifecycle_command)
            .unwrap(),
        "lifecycle command must not install while a durable delete drain is active"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "blocked lifecycle command must not leave a pending slot"
    );

    let cors_command_id = MetadataCommandId::new(
        crate::ClusterEpoch::INITIAL,
        pg_id,
        MetadataCommandLogIndex::new(2).unwrap(),
    );
    let cors_command = {
        let bucket_pg = primary.get_pg(pg_id.get()).unwrap();
        let generation = bucket_pg
            .next_bucket_execution_generation_candidate()
            .unwrap();
        MetadataCommandEnvelope::new(
            cors_command_id,
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                bucket.clone(),
                BucketSubresourceMutation::Put {
                    kind: crate::BucketSubresourceKind::Cors,
                    body: "<CORSConfiguration/>".to_string(),
                    aux: crate::BucketSubresourceAux::None,
                },
                generation,
            )),
        )
    };
    assert!(
        !cluster
            .try_set_bucket_control_pending_command_or_retry(pg_id, &bucket, &cors_command)
            .unwrap(),
        "CORS command must not install while a durable delete drain is active"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "blocked CORS command must not leave a pending slot"
    );

    cluster.clear_durable_bucket_delete_drain(&drain).unwrap();
    let versioned = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert_eq!(versioned.versioning, crate::BucketVersioningState::Enabled);
    let lifecycle = cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    assert!(lifecycle.bucket_lifecycle_present);
    cluster
        .put_bucket_subresource_and_load_info(
            &bucket,
            crate::PutBucketSubresource {
                kind: crate::BucketSubresourceKind::Cors,
                body: "<CORSConfiguration/>",
                aux: crate::BucketSubresourceAux::None,
            },
        )
        .unwrap();
    let bucket_pg = primary.get_pg(pg_id.get()).unwrap();
    let cors = crate::PgMetadataStore::get_bucket_subresource(
        &*bucket_pg,
        &bucket,
        crate::BucketSubresourceKind::Cors,
    )
    .unwrap()
    .expect("CORS subresource should be installed after the drain clears");
    assert_eq!(cors.body, "<CORSConfiguration/>");
}

#[test]
fn begin_bucket_delete_waits_for_durable_reservation_and_post_drains_visible_write() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, key) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-durable-reservation-");
        let key = key_for_object_pg(topology, &bucket, 2, "key-");
        (bucket, key)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(&bucket, "test-held-write", Some(key.as_str()))
        .unwrap();
    let bucket_write_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);
    let payload = b"visible";
    let generation_reservation_id = crate::SessionId::try_from("72".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &generation_reservation_id)
        .unwrap();
    let segment_okh = [71; 16];
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
    let commit_req = crate::CommitDirectPutObjectReq {
        bucket: bucket.clone(),
        key: key.clone(),
        generation_reservation_id,
        versioning: crate::BucketVersioningState::Disabled,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        generation_id,
        size: payload.len() as u64,
        etag_crc64: checksum::crc64::checksum(payload),
        ec: written.ec,
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        segment_index: 0,
        segment_crc64: checksum::crc64::checksum(payload),
        segment_okh,
        segment_vid: generation_id,
        data_pg_id: written.data_pg_id,
        bucket_write_reservation: bucket_write_proof,
    };

    let delete_started = Arc::new((Mutex::new(false), Condvar::new()));
    let delete_waiting = Arc::new((Mutex::new(false), Condvar::new()));
    let hook_bucket = bucket.clone();
    let delete_waiting_for_hook = Arc::clone(&delete_waiting);
    let _hook_guard =
        crate::node::install_bucket_scoped_test_hooks(crate::node::BucketScopedTestHooks {
            target: Some(hook_bucket),
            before_bucket_write_drain_wait: Some(Arc::new(move || {
                let (lock, cv) = &*delete_waiting_for_hook;
                *lock.lock().unwrap() = true;
                cv.notify_all();
            })),
            ..crate::node::BucketScopedTestHooks::default()
        });

    let delete_cluster = Arc::clone(&cluster);
    let delete_bucket = bucket.clone();
    let delete_started_for_thread = Arc::clone(&delete_started);
    let delete_thread = std::thread::spawn(move || {
        {
            let (lock, cv) = &*delete_started_for_thread;
            *lock.lock().unwrap() = true;
            cv.notify_all();
        }
        delete_cluster.begin_bucket_delete(&delete_bucket)
    });

    {
        let (lock, cv) = &*delete_started;
        let guard = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |started| {
                !*started
            })
            .unwrap()
            .0;
        assert!(*guard, "delete thread should start");
    }
    {
        let (lock, cv) = &*delete_waiting;
        let guard = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |waiting| {
                !*waiting
            })
            .unwrap()
            .0;
        assert!(
            *guard,
            "DeleteBucket should wait for the durable writer reservation before emptiness"
        );
    }

    {
        let pg_id = PgId::new(2);
        let shard_batch: Vec<(&ShardKey, WriteAck)> = written
            .written_shards
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect();
        cluster
            .register_payload_shard_acks(written.data_pg_id, &shard_batch)
            .unwrap();
        let primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
            .unwrap();
        let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
        let command = cluster
            .prepare_commit_direct_put_object_command(
                pg_id,
                &pg,
                &commit_req,
                crate::VersionId::Null,
                commit_req.bucket_write_reservation.clone(),
            )
            .unwrap();
        pg.try_insert_pending_metadata_command_slot(
            primary.node_id().as_u32(),
            &command,
            Some(&bucket),
        )
        .unwrap();
    }

    let err = delete_thread.join().unwrap().unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "post-reservation drain/check should see the committed object, got {err:?}"
    );
    assert_bucket_write_reservations_released(&map, &bucket);
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .is_none(),
        "failed DeleteBucket should clear its temporary durable drain"
    );
}

#[test]
fn begin_bucket_delete_bounds_orphaned_durable_reservation_wait() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-orphan-reservation-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "test-orphaned-write",
            Some("orphaned-key"),
        )
        .unwrap();

    let started = std::time::Instant::now();
    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "DeleteBucket should not wait indefinitely for an orphaned durable reservation"
    );
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "orphaned durable reservation should make DeleteBucket retryable, got {err:?}"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .is_none(),
        "failed DeleteBucket should clear its temporary durable drain"
    );
    assert_eq!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .len(),
        1,
        "DeleteBucket must not silently drop another operation's durable reservation"
    );
    drop(bucket_pg);

    cluster
        .release_durable_bucket_write_reservation(reservation)
        .unwrap();
    cluster.begin_bucket_delete(&bucket).unwrap();
}

#[test]
fn begin_bucket_delete_bounds_active_delete_drain_wait() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-active-drain-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let drain = crate::PgMetadataStore::begin_durable_bucket_write_drain(
        &*bucket_pg,
        &bucket,
        "held-delete-drain",
        "other-delete-owner",
        crate::ClusterEpoch::INITIAL,
        crate::clock::current_time_millis(),
        None,
    )
    .unwrap();
    drop(bucket_pg);

    let started = std::time::Instant::now();
    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "DeleteBucket should not wait indefinitely behind another active delete drain"
    );
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::MetadataCommandContention { .. })
        ),
        "active delete drain should make DeleteBucket return retryable contention, got {err:?}"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .as_ref()
            .map(|record| record.drain_id.as_str()),
        Some(drain.drain_id.as_str()),
        "DeleteBucket must not clear another caller's active delete drain"
    );
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Active,
        "timed out DeleteBucket begin must leave the bucket active"
    );
}

#[test]
fn begin_bucket_delete_adopts_active_delete_drain_after_reopen() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-active-drain-reopen-")
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let clock = Arc::new(crate::clock::test_time_override_guard(1_000));
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let original_deadline = drain
        .record
        .lease_deadline
        .expect("delete drains should carry a recovery lease deadline");
    clock.set(12_000);
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let reopened_cluster = crate::StorageCluster::from_local_map(Arc::clone(&reopened)).unwrap();
    let advanced_during_proof = Arc::new(AtomicBool::new(false));
    let advanced_during_proof_for_hook = Arc::clone(&advanced_during_proof);
    let clock_for_hook = Arc::clone(&clock);
    let _progress_hook_guard = reopened_cluster
        .test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |_next_object_pg_id| {
                clock_for_hook.set(17_000);
                advanced_during_proof_for_hook.store(true, Ordering::SeqCst);
                Ok(())
            },
        ));
    reopened_cluster
        .begin_bucket_delete(&bucket)
        .expect("DeleteBucket should adopt and complete an active pre-mark drain after reopen");
    assert!(
        advanced_during_proof.load(Ordering::SeqCst),
        "test must advance logical time during the adopted DeleteBucket proof"
    );

    let bucket_pg = reopened
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .as_ref()
            .map(|record| record.drain_id.as_str()),
        Some(drain.record.drain_id.as_str()),
        "DeleteBucket adoption should preserve the original drain identity"
    );
    let renewed_deadline = crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
        .unwrap()
        .and_then(|record| record.lease_deadline)
        .expect("adopted delete drain should remain leased until finalization");
    assert!(
        renewed_deadline > original_deadline,
        "adopted delete drain should renew before long proof phases; original={original_deadline} renewed={renewed_deadline}"
    );
    assert!(
        renewed_deadline > crate::clock::current_time_millis(),
        "adopted delete drain should still be live after the proof reaches MarkBucketDeleting"
    );
    assert_eq!(
        crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket)
            .unwrap()
            .state,
        crate::BucketState::Deleting,
        "adopted pre-mark delete drain should reach terminal deleting state"
    );
}

#[test]
fn durable_scan_skips_live_and_queues_expired_delete_begin_drain_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-active-drain-scan-")
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_info = cluster.test_head_bucket_raw(&bucket).unwrap();
    let live_drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let reopened_cluster = crate::StorageCluster::from_local_map(Arc::clone(&reopened)).unwrap();
    let expected_root = crate::BucketDeleteBeginRoot {
        bucket: bucket.clone(),
        bucket_execution_generation: bucket_info.bucket_execution_generation,
        bucket_incarnation_generation: bucket_info.bucket_incarnation_generation,
    };

    let scan =
        reopened_cluster.enqueue_durable_bucket_delete_begin_roots_excluding(&HashSet::new());
    assert_eq!(scan.errors, 0);
    assert_eq!(scan.queued, 0);
    assert_eq!(reopened_cluster.try_take_reclaim_work(), None);
    reopened_cluster
        .clear_durable_bucket_delete_drain(&live_drain)
        .unwrap();

    let expired_drain = {
        let bucket_pg = reopened
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let now = crate::clock::current_time_millis();
        crate::PgMetadataStore::begin_durable_bucket_write_drain(
            &*bucket_pg,
            &bucket,
            "expired-delete-begin-scan-drain",
            "expired-delete-begin-scan-owner",
            crate::ClusterEpoch::INITIAL,
            now.saturating_sub(10),
            Some(now.saturating_sub(1)),
        )
        .unwrap()
    };

    let scan =
        reopened_cluster.enqueue_durable_bucket_delete_begin_roots_excluding(&HashSet::new());
    assert_eq!(scan.errors, 0);
    assert_eq!(scan.queued, 1);
    assert_eq!(
        reopened_cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDeleteBegin(
            expected_root.clone()
        ))
    );

    let excluded = HashSet::from([expected_root]);
    let scan = reopened_cluster.enqueue_durable_bucket_delete_begin_roots_excluding(&excluded);
    assert_eq!(scan.errors, 0);
    assert_eq!(scan.queued, 0);
    assert_eq!(reopened_cluster.try_take_reclaim_work(), None);

    reopened_cluster
        .clear_durable_bucket_delete_drain(&crate::cluster::DurableBucketWriteDrain {
            pg_id: 1,
            record: expired_drain,
        })
        .unwrap();
}

#[test]
fn durable_scan_paginates_past_excluded_delete_begin_drains() {
    const PAGE_LIMIT: usize = 16;

    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let buckets = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let mut buckets = Vec::new();
        for index in 0..=PAGE_LIMIT {
            buckets.push(bucket_for_pg(
                topology,
                1,
                &format!("delete-begin-page-{index:02}-"),
            ));
        }
        buckets.sort();
        buckets
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let mut drains = Vec::new();
    let mut roots = Vec::new();
    let now = crate::clock::current_time_millis();
    for (index, bucket) in buckets.iter().enumerate() {
        create_test_bucket(&cluster, bucket);
        let bucket_info = cluster.test_head_bucket_raw(bucket).unwrap();
        let bucket_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let drain = crate::PgMetadataStore::begin_durable_bucket_write_drain(
            &*bucket_pg,
            bucket,
            &format!("expired-delete-begin-page-drain-{index}"),
            "expired-delete-begin-page-owner",
            crate::ClusterEpoch::INITIAL,
            now.saturating_sub(10),
            Some(now.saturating_sub(1)),
        )
        .unwrap();
        drains.push(drain);
        roots.push(crate::BucketDeleteBeginRoot {
            bucket: bucket.clone(),
            bucket_execution_generation: bucket_info.bucket_execution_generation,
            bucket_incarnation_generation: bucket_info.bucket_incarnation_generation,
        });
    }
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let reopened_cluster = crate::StorageCluster::from_local_map(Arc::clone(&reopened)).unwrap();
    let excluded = roots
        .iter()
        .take(PAGE_LIMIT)
        .cloned()
        .collect::<HashSet<_>>();

    let scan = reopened_cluster.enqueue_durable_bucket_delete_begin_roots_excluding(&excluded);
    assert_eq!(scan.errors, 0);
    assert_eq!(scan.queued, 1);
    assert_eq!(
        reopened_cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::BucketDeleteBegin(
            roots[PAGE_LIMIT].clone()
        ))
    );
    assert_eq!(reopened_cluster.try_take_reclaim_work(), None);

    for drain in drains {
        reopened_cluster
            .clear_durable_bucket_delete_drain(&crate::cluster::DurableBucketWriteDrain {
                pg_id: 1,
                record: drain,
            })
            .unwrap();
    }
}

#[test]
fn begin_bucket_delete_records_final_visibility_phase_before_mark_command() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-final-visibility-phase-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let pg_id = PgId::new(1);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard =
        cluster.test_install_before_bucket_delete_command_id_hook(Arc::new(move || {
            hook_ran_for_closure.store(true, Ordering::SeqCst);
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*pg, &hook_bucket)
                .unwrap()
                .expect("DeleteBucket should record final visibility progress before mark command");
            assert_eq!(
                outcome.outcome,
                crate::BucketDeleteAttemptOutcomeKind::Retryable
            );
            assert_eq!(
                outcome.phase,
                crate::BucketDeleteAttemptPhase::FinalVisibilityProven
            );
            assert_eq!(
                outcome.post_reservation_next_object_pg_id,
                Some(0),
                "final visibility progress should preserve the pre-cleanup frontier reset"
            );
        }));

    cluster.begin_bucket_delete(&bucket).unwrap();
    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should observe the attempt before MarkBucketDeleting id allocation"
    );

    let pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(pg_id.get())
        .unwrap();
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*pg, &bucket)
        .unwrap()
        .expect("successful DeleteBucket begin should record terminal outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
}

#[test]
fn begin_bucket_delete_adopts_final_visibility_phase_without_repeating_post_reservation_scan() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-final-visibility-adopt-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let bucket_pg_id = PgId::new(drain.pg_id);
    let bucket_pg_primary = map
        .metadata_pg_primary_node(crate::ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_pg_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    crate::PgMetadataStore::record_bucket_delete_attempt_outcome(
        &*bucket_pg,
        &crate::BucketDeleteAttemptOutcomeRecord {
            bucket: bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: crate::BucketDeleteAttemptOutcomeKind::Retryable,
            phase: crate::BucketDeleteAttemptPhase::FinalVisibilityCheck,
            detail: "resume from final visibility".to_string(),
            post_reservation_next_object_pg_id: Some(0),
            updated_at: crate::clock::current_time_millis(),
        },
    )
    .unwrap();
    drop(bucket_pg);

    let post_reservation_scan_ran = Arc::new(AtomicBool::new(false));
    let post_reservation_scan_ran_for_hook = Arc::clone(&post_reservation_scan_ran);
    let _progress_hook_guard = cluster
        .test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |next_object_pg_id| {
                post_reservation_scan_ran_for_hook.store(true, Ordering::SeqCst);
                Err(StoreError::Io {
                    context: "unexpected post-reservation scan during final-visibility adoption",
                    source: std::io::Error::other(format!(
                        "unexpected next_object_pg_id={next_object_pg_id}"
                    )),
                })
            },
        ));

    cluster.begin_bucket_delete(&bucket).unwrap();
    assert!(
        !post_reservation_scan_ran.load(Ordering::SeqCst),
        "final-visibility adoption should not repeat the post-reservation scan"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted DeleteBucket begin should record terminal outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    assert_eq!(outcome.drain_id, drain.record.drain_id);
}

#[test]
fn begin_bucket_delete_adopts_final_visibility_proven_without_repeating_visibility_check() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-final-visibility-proven-adopt-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let bucket_pg_id = PgId::new(drain.pg_id);
    let bucket_pg_primary = map
        .metadata_pg_primary_node(crate::ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_pg_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    crate::PgMetadataStore::record_bucket_delete_attempt_outcome(
        &*bucket_pg,
        &crate::BucketDeleteAttemptOutcomeRecord {
            bucket: bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: crate::BucketDeleteAttemptOutcomeKind::Retryable,
            phase: crate::BucketDeleteAttemptPhase::FinalVisibilityProven,
            detail: "resume after final visibility proof".to_string(),
            post_reservation_next_object_pg_id: Some(0),
            updated_at: crate::clock::current_time_millis(),
        },
    )
    .unwrap();
    drop(bucket_pg);

    let visibility_check_ran = Arc::new(AtomicBool::new(false));
    let visibility_check_ran_for_hook = Arc::clone(&visibility_check_ran);
    let _visibility_hook_guard =
        cluster.test_install_before_bucket_delete_final_visibility_hook(Arc::new(move || {
            visibility_check_ran_for_hook.store(true, Ordering::SeqCst);
            Err(StoreError::Io {
                context: "unexpected final visibility scan during proven adoption",
                source: std::io::Error::other("final visibility should already be proven"),
            })
        }));

    cluster.begin_bucket_delete(&bucket).unwrap();
    assert!(
        !visibility_check_ran.load(Ordering::SeqCst),
        "final-visibility-proven adoption should not repeat the visibility scan"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted DeleteBucket begin should record terminal outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    assert_eq!(outcome.drain_id, drain.record.drain_id);
}

#[test]
fn begin_bucket_delete_renews_drain_after_final_visibility_proof_before_retryable_exit() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-final-visibility-renew-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let clock = Arc::new(crate::clock::test_time_override_guard(1_000));
    let advanced_during_visibility = Arc::new(AtomicBool::new(false));
    let advanced_during_visibility_for_hook = Arc::clone(&advanced_during_visibility);
    let clock_for_visibility_hook = Arc::clone(&clock);
    let visibility_hook_guard =
        cluster.test_install_before_bucket_delete_final_visibility_hook(Arc::new(move || {
            clock_for_visibility_hook.set(12_000);
            advanced_during_visibility_for_hook.store(true, Ordering::SeqCst);
            Ok(())
        }));
    let proven_hook_guard =
        cluster.test_install_after_bucket_delete_final_visibility_proven_hook(Arc::new(|| {
            Err(StoreError::MetadataCommandContention {
                context: "injected retryable error after final visibility proof",
            })
        }));

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::MetadataCommandContention {
                context: "injected retryable error after final visibility proof"
            })
        ),
        "expected injected retryable error, got {err:?}"
    );
    assert!(
        advanced_during_visibility.load(Ordering::SeqCst),
        "test must advance logical time during final visibility"
    );
    drop(proven_hook_guard);
    drop(visibility_hook_guard);

    let bucket_pg_id = PgId::new(cluster.bucket_metadata_pg_id(&bucket));
    let bucket_pg_primary = map
        .metadata_pg_primary_node(crate::ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_pg_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let preserved_drain = crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
        .unwrap()
        .expect("retryable final visibility proof should preserve the delete drain");
    let preserved_deadline = preserved_drain
        .lease_deadline
        .expect("preserved delete drain should remain leased");
    assert!(
        preserved_deadline > 20_000,
        "final visibility proof should renew the drain for a later retry; deadline={preserved_deadline}"
    );
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("retryable final visibility proof should record an attempt outcome");
    assert_eq!(
        outcome.phase,
        crate::BucketDeleteAttemptPhase::FinalVisibilityProven
    );
    assert_eq!(outcome.drain_id, preserved_drain.drain_id);
    drop(bucket_pg);

    clock.set(20_000);
    let visibility_reran = Arc::new(AtomicBool::new(false));
    let visibility_reran_for_hook = Arc::clone(&visibility_reran);
    let _visibility_retry_hook_guard = cluster
        .test_install_before_bucket_delete_final_visibility_hook(Arc::new(move || {
            visibility_reran_for_hook.store(true, Ordering::SeqCst);
            Err(StoreError::Io {
                context: "unexpected final visibility rerun after preserved proof",
                source: std::io::Error::other("final visibility should already be proven"),
            })
        }));

    cluster
        .begin_bucket_delete(&bucket)
        .expect("retry should adopt the preserved final-visibility proof");
    assert!(
        !visibility_reran.load(Ordering::SeqCst),
        "retry should not rerun final visibility while the preserved drain is live"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let final_outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted DeleteBucket begin should record terminal outcome");
    assert_eq!(
        final_outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(final_outcome.drain_id, preserved_drain.drain_id);
}

#[test]
fn begin_bucket_delete_committed_response_loss_retry_observes_deleting() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-committed-response-loss-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls_for_hook = Arc::clone(&hook_calls);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::MarkBucketDeleting(mark)
                    if mark.bucket_name() == &hook_bucket
                        && node_id == NodeId::new(0)
                        && hook_calls_for_hook.fetch_add(1, Ordering::SeqCst) == 0 =>
                {
                    for node_id in [NodeId::new(0), NodeId::new(2)] {
                        let node = hook_map.node(node_id).unwrap().storage_node();
                        let pg = node.get_pg(command.id().pg_id().get())?;
                        pg.apply_metadata_command_and_record(node_id.as_u32(), command)
                            .map_err(|error| match error {
                                crate::BucketSnapshotLoadError::Store(error) => error,
                                crate::BucketSnapshotLoadError::Metadata(error) => {
                                    panic!("manual mark deleting command apply failed: {error}")
                                }
                            })?;
                    }
                    return Err(StoreError::RouteMapExpired {
                        cluster_epoch: ClusterEpoch::INITIAL,
                        valid_until_ms: 0,
                        now_ms: 1,
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let first_err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            first_err,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "expected injected post-commit route error, got {first_err:?}"
    );
    assert_eq!(
        hook_calls.load(Ordering::SeqCst),
        1,
        "first attempt should inject exactly once after committing MarkBucketDeleting"
    );
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_some(),
        "response loss after committed MarkBucketDeleting may leave a terminal pending command for retry cleanup"
    );

    cluster
        .begin_bucket_delete(&bucket)
        .expect("retry should observe the committed bucket delete");
    assert_eq!(
        hook_calls.load(Ordering::SeqCst),
        1,
        "committed retry should not rerun MarkBucketDeleting apply"
    );
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none(),
        "committed retry must clear the terminal MarkBucketDeleting pending command"
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(1).unwrap();
        let info = crate::PgMetadataStore::head_bucket_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn begin_bucket_delete_treats_retryable_error_after_concurrent_mark_deleting_as_success() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-concurrent-mark-success-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let concurrent_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let hook_bucket = bucket.clone();
    let _proven_hook_guard = cluster.test_install_after_bucket_delete_final_visibility_proven_hook(
        Arc::new(move || {
            if hook_ran_for_hook.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            concurrent_cluster
                .begin_bucket_delete(&hook_bucket)
                .expect("concurrent begin should mark the same bucket incarnation deleting");
            Err(StoreError::MetadataCommandContention {
                context: "injected retryable error after concurrent mark deleting",
            })
        }),
    );

    cluster
        .begin_bucket_delete(&bucket)
        .expect("retryable error after same-incarnation MarkBucketDeleting should be success");
    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should simulate a concurrent MarkBucketDeleting before retryable exit"
    );
    assert_eq!(
        cluster.try_take_reclaim_work(),
        None,
        "stale retryable exit should not enqueue begin work after observing Deleting"
    );

    let bucket_pg_id = PgId::new(cluster.bucket_metadata_pg_id(&bucket));
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful concurrent mark should record terminal outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
}

#[test]
fn begin_bucket_delete_does_not_suppress_route_error_after_concurrent_mark_deleting() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-route-error-preserved-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let concurrent_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let hook_bucket = bucket.clone();
    let _proven_hook_guard = cluster.test_install_after_bucket_delete_final_visibility_proven_hook(
        Arc::new(move || {
            if hook_ran_for_hook.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            concurrent_cluster
                .begin_bucket_delete(&hook_bucket)
                .expect("concurrent begin should mark the same bucket incarnation deleting");
            Err(StoreError::RouteMapExpired {
                cluster_epoch: ClusterEpoch::INITIAL,
                valid_until_ms: 1,
                now_ms: 2,
            })
        }),
    );

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "route errors from the pinned map must not be suppressed by a same-client deleting recheck; got {err:?}"
    );
    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should simulate a concurrent MarkBucketDeleting before route error"
    );

    let bucket_pg_id = PgId::new(cluster.bucket_metadata_pg_id(&bucket));
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
}

#[test]
fn begin_bucket_delete_adopts_stream_cleanup_phase_and_revalidates_after_reservations() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-stream-cleanup-adopt-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let bucket_pg_id = PgId::new(drain.pg_id);
    let bucket_pg_primary = map
        .metadata_pg_primary_node(crate::ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_pg_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    crate::PgMetadataStore::record_bucket_delete_attempt_outcome(
        &*bucket_pg,
        &crate::BucketDeleteAttemptOutcomeRecord {
            bucket: bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: crate::BucketDeleteAttemptOutcomeKind::Retryable,
            phase: crate::BucketDeleteAttemptPhase::StreamCleanup,
            detail: "resume from stream cleanup".to_string(),
            post_reservation_next_object_pg_id: None,
            updated_at: crate::clock::current_time_millis(),
        },
    )
    .unwrap();
    drop(bucket_pg);

    let post_reservation_scan_ran = Arc::new(AtomicBool::new(false));
    let post_reservation_scan_ran_for_hook = Arc::clone(&post_reservation_scan_ran);
    let _progress_hook_guard = cluster
        .test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |_next_object_pg_id| {
                post_reservation_scan_ran_for_hook.store(true, Ordering::SeqCst);
                Ok(())
            },
        ));

    cluster.begin_bucket_delete(&bucket).unwrap();
    assert!(
        post_reservation_scan_ran.load(Ordering::SeqCst),
        "stream-cleanup adoption must still revalidate the post-reservation object-PG drain"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted DeleteBucket begin should record terminal outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    assert_eq!(outcome.drain_id, drain.record.drain_id);
}

#[test]
fn begin_bucket_delete_records_reservation_wait_phase_after_stream_cleanup() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-reservation-wait-record-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let _hook_guard =
        cluster.test_install_after_bucket_delete_reservation_wait_ready_hook(Arc::new(move || {
            hook_ran_for_hook.store(true, Ordering::SeqCst);
            let bucket_pg = hook_map
                .node(NodeId::new(1))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            let outcome =
                crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &hook_bucket)
                    .unwrap()
                    .expect("DeleteBucket should record reservation-wait progress");
            assert_eq!(
                outcome.outcome,
                crate::BucketDeleteAttemptOutcomeKind::Retryable
            );
            assert_eq!(
                outcome.phase,
                crate::BucketDeleteAttemptPhase::ReservationWait
            );
            Err(StoreError::RouteMapExpired {
                cluster_epoch: ClusterEpoch::INITIAL,
                valid_until_ms: 0,
                now_ms: 1,
            })
        }));

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "hook should fail after recording reservation-wait cursor, got {err:?}"
    );
    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test hook should observe the durable reservation-wait cursor"
    );
}

#[test]
fn begin_bucket_delete_adopts_reservation_wait_phase_without_repeating_initial_scan() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "delete-reservation-wait-adopt-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let bucket_pg_id = PgId::new(drain.pg_id);
    let bucket_pg_primary = map
        .metadata_pg_primary_node(crate::ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_pg_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    crate::PgMetadataStore::record_bucket_delete_attempt_outcome(
        &*bucket_pg,
        &crate::BucketDeleteAttemptOutcomeRecord {
            bucket: bucket.clone(),
            drain_id: drain.record.drain_id.clone(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: crate::BucketDeleteAttemptOutcomeKind::Retryable,
            phase: crate::BucketDeleteAttemptPhase::ReservationWait,
            detail: "resume from reservation wait".to_string(),
            post_reservation_next_object_pg_id: None,
            updated_at: crate::clock::current_time_millis(),
        },
    )
    .unwrap();
    drop(bucket_pg);

    let initial_scan_ran = Arc::new(AtomicBool::new(false));
    let initial_scan_ran_for_hook = Arc::clone(&initial_scan_ran);
    let _exact_drain_hook_guard = cluster.test_install_before_bucket_delete_exact_drain_hook(
        Arc::new(move |has_progress, next_object_pg_id| {
            if !has_progress {
                initial_scan_ran_for_hook.store(true, Ordering::SeqCst);
                return Err(StoreError::Io {
                    context:
                        "unexpected initial exact-bucket drain during reservation-wait adoption",
                    source: std::io::Error::other(format!("next_object_pg_id={next_object_pg_id}")),
                });
            }
            Ok(())
        }),
    );
    let post_reservation_scan_ran = Arc::new(AtomicBool::new(false));
    let post_reservation_scan_ran_for_hook = Arc::clone(&post_reservation_scan_ran);
    let _progress_hook_guard = cluster
        .test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |_next_object_pg_id| {
                post_reservation_scan_ran_for_hook.store(true, Ordering::SeqCst);
                Ok(())
            },
        ));

    cluster.begin_bucket_delete(&bucket).unwrap();
    assert!(
        !initial_scan_ran.load(Ordering::SeqCst),
        "reservation-wait adoption must skip the initial pre-cleanup exact-bucket drain"
    );
    assert!(
        post_reservation_scan_ran.load(Ordering::SeqCst),
        "reservation-wait adoption must still validate object PGs after reservations drain"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted DeleteBucket begin should record terminal outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    assert_eq!(outcome.drain_id, drain.record.drain_id);
}

#[test]
fn post_reservation_exact_bucket_frontier_is_identity_fenced_and_resettable() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, pg_count) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        (
            bucket_for_pg(topology, 1, "delete-progress-frontier-"),
            topology.pg_count(),
        )
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };
    let bucket_pg_id = PgId::new(drain.pg_id);
    let bucket_pg_primary = map
        .metadata_pg_primary_node(crate::ClusterEpoch::INITIAL, bucket_pg_id)
        .unwrap();
    let bucket_pg = bucket_pg_primary
        .storage_node()
        .get_pg(bucket_pg_id.get())
        .unwrap();

    crate::PgMetadataStore::record_bucket_delete_attempt_outcome(
        &*bucket_pg,
        &crate::BucketDeleteAttemptOutcomeRecord {
            bucket: bucket.clone(),
            drain_id: "stale-delete-drain".to_string(),
            cluster_epoch: drain.record.cluster_epoch,
            bucket_execution_generation: drain.record.bucket_execution_generation,
            outcome: crate::BucketDeleteAttemptOutcomeKind::Retryable,
            phase: crate::BucketDeleteAttemptPhase::PostReservationObjectDrain,
            detail: "stale progress must not be trusted".to_string(),
            post_reservation_next_object_pg_id: Some(pg_count),
            updated_at: crate::clock::current_time_millis(),
        },
    )
    .unwrap();
    drop(bucket_pg);
    assert_eq!(
        cluster
            .test_bucket_delete_post_reservation_next_object_pg_id(&drain)
            .unwrap(),
        None,
        "frontier from a different drain identity must not be trusted"
    );

    cluster
        .test_record_bucket_delete_post_reservation_next_object_pg_id(&drain, pg_count)
        .unwrap();
    assert_eq!(
        cluster
            .test_bucket_delete_post_reservation_next_object_pg_id(&drain)
            .unwrap(),
        Some(pg_count),
        "empty post-reservation scan should advance the durable frontier"
    );

    cluster
        .test_record_bucket_delete_post_reservation_next_object_pg_id(&drain, 0)
        .unwrap();
    assert_eq!(
        cluster
            .test_bucket_delete_post_reservation_next_object_pg_id(&drain)
            .unwrap(),
        Some(0),
        "pre-cleanup reset must be persisted before cleanup can proceed"
    );

    cluster.clear_durable_bucket_delete_drain(&drain).unwrap();
}

#[test]
fn post_reservation_exact_bucket_frontier_resumes_after_recorded_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, lower_key, later_key, pg_count) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-progress-resume-");
        let lower_key = key_for_object_pg(topology, &bucket, 0, "lower-");
        let later_key = key_for_object_pg(topology, &bucket, 2, "later-");
        (bucket, lower_key, later_key, topology.pg_count())
    };
    assert!(
        pg_count > 2,
        "test requires at least three metadata PGs to prove frontier resume"
    );

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };

    let lower_pg_id = PgId::new(0);
    let lower_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(lower_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            lower_key,
            crate::SessionId::try_from("81".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, lower_pg_id, &bucket, &lower_command);

    let later_pg_id = PgId::new(2);
    let later_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(later_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            later_key,
            crate::SessionId::try_from("82".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, later_pg_id, &bucket, &later_command);

    cluster
        .test_record_bucket_delete_post_reservation_next_object_pg_id(&drain, 2)
        .unwrap();
    cluster
        .test_drain_pending_object_metadata_commands_for_exact_bucket_after_reservation(
            &bucket, &drain,
        )
        .unwrap();

    assert!(
        pending_metadata_command_for_test(&map, lower_pg_id, &bucket).is_some(),
        "post-reservation resume must not revisit object PGs below the stored frontier"
    );
    assert!(
        pending_metadata_command_for_test(&map, later_pg_id, &bucket).is_none(),
        "post-reservation resume must drain object PGs at or above the stored frontier"
    );
    assert_eq!(
        cluster
            .test_bucket_delete_post_reservation_next_object_pg_id(&drain)
            .unwrap(),
        Some(pg_count),
        "completed resumed scan should advance the durable frontier to the end"
    );

    cluster.clear_durable_bucket_delete_drain(&drain).unwrap();
}

#[test]
fn begin_bucket_delete_adopts_preserved_post_reservation_frontier() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids: Vec<u32> = (0..32).collect();
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, lower_key, later_key, pg_count) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-progress-adopt-");
        let lower_key = key_for_object_pg(topology, &bucket, 0, "lower-");
        let later_pg = topology.pg_count() - 1;
        let later_key = key_for_object_pg(topology, &bucket, later_pg, "later-");
        (bucket, lower_key, later_key, topology.pg_count())
    };
    assert!(
        pg_count >= 32,
        "test requires several exact-bucket drain chunks"
    );
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let initial_bucket = {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap()
    };
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "test-held-write",
            Some(lower_key.as_str()),
        )
        .unwrap();

    let delete_waiting = Arc::new((Mutex::new(false), Condvar::new()));
    let release_delete_wait = Arc::new((Mutex::new(false), Condvar::new()));
    let hook_bucket = bucket.clone();
    let delete_waiting_for_hook = Arc::clone(&delete_waiting);
    let release_delete_wait_for_hook = Arc::clone(&release_delete_wait);
    let _bucket_hook_guard =
        crate::node::install_bucket_scoped_test_hooks(crate::node::BucketScopedTestHooks {
            target: Some(hook_bucket),
            before_bucket_write_drain_wait: Some(Arc::new(move || {
                let (lock, cv) = &*delete_waiting_for_hook;
                *lock.lock().unwrap() = true;
                cv.notify_all();

                let (release_lock, release_cv) = &*release_delete_wait_for_hook;
                let _release_guard = release_cv
                    .wait_while(release_lock.lock().unwrap(), |released| !*released)
                    .unwrap();
            })),
            ..crate::node::BucketScopedTestHooks::default()
        });

    let fail_after_first_frontier = Arc::new(AtomicBool::new(true));
    let fail_after_first_frontier_for_hook = Arc::clone(&fail_after_first_frontier);
    let _progress_hook_guard = cluster
        .test_install_after_bucket_delete_post_reservation_progress_hook(Arc::new(
            move |next_object_pg_id| {
                if next_object_pg_id < pg_count
                    && fail_after_first_frontier_for_hook.swap(false, Ordering::SeqCst)
                {
                    return Err(StoreError::RouteMapExpired {
                        cluster_epoch: ClusterEpoch::INITIAL,
                        valid_until_ms: 0,
                        now_ms: 1,
                    });
                }
                Ok(())
            },
        ));

    let delete_cluster = Arc::clone(&cluster);
    let delete_bucket = bucket.clone();
    let delete_thread =
        std::thread::spawn(move || delete_cluster.begin_bucket_delete(&delete_bucket));

    {
        let (lock, cv) = &*delete_waiting;
        let guard = cv
            .wait_timeout_while(lock.lock().unwrap(), Duration::from_secs(5), |waiting| {
                !*waiting
            })
            .unwrap()
            .0;
        assert!(
            *guard,
            "DeleteBucket should reach reservation wait before test installs pending work"
        );
    }

    let lower_pg_id = PgId::new(0);
    let lower_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(lower_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            lower_key,
            crate::SessionId::try_from("83".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, lower_pg_id, &bucket, &lower_command);

    let later_pg_id = PgId::new(pg_count - 1);
    let later_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(later_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            later_key,
            crate::SessionId::try_from("84".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, later_pg_id, &bucket, &later_command);

    cluster
        .release_durable_bucket_write_reservation(reservation)
        .unwrap();
    {
        let (lock, cv) = &*release_delete_wait;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }

    let err = delete_thread.join().unwrap().unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "first DeleteBucket should preserve the attempt on injected route expiry, got {err:?}"
    );
    assert!(
        !fail_after_first_frontier.load(Ordering::SeqCst),
        "test hook should fail after the first persisted post-reservation frontier"
    );
    assert!(
        pending_metadata_command_for_test(&map, lower_pg_id, &bucket).is_none(),
        "first DeleteBucket attempt should drain object PGs below the persisted frontier"
    );
    assert!(
        pending_metadata_command_for_test(&map, later_pg_id, &bucket).is_some(),
        "first DeleteBucket attempt should leave later object PGs for adoption/resume"
    );
    let preserved_drain = {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .expect("retryable failure should preserve the active delete drain")
    };
    assert_eq!(
        preserved_drain.bucket_execution_generation,
        initial_bucket.bucket_execution_generation
    );
    let preserved_frontier = cluster
        .test_bucket_delete_post_reservation_next_object_pg_id(
            &crate::cluster::DurableBucketWriteDrain {
                pg_id: 1,
                record: preserved_drain.clone(),
            },
        )
        .unwrap()
        .expect("retryable failure should persist post-reservation progress");
    assert!(
        preserved_frontier > 0 && preserved_frontier < pg_count,
        "frontier should identify a partial post-reservation scan, got {preserved_frontier}"
    );
    let resume_root = match cluster.try_take_reclaim_work() {
        Some(crate::ReclaimWorkItem::BucketDeleteBegin(root)) => root,
        other => panic!(
            "retryable preserved DeleteBucket begin should queue background resume work, got {other:?}"
        ),
    };
    assert_eq!(resume_root.bucket, bucket);
    assert_eq!(
        resume_root.bucket_execution_generation,
        initial_bucket.bucket_execution_generation
    );
    assert_eq!(
        resume_root.bucket_incarnation_generation,
        initial_bucket.bucket_incarnation_generation
    );

    cluster
        .begin_bucket_delete_if_current(
            &resume_root.bucket,
            resume_root.bucket_execution_generation,
            resume_root.bucket_incarnation_generation,
        )
        .unwrap();

    assert!(
        pending_metadata_command_for_test(&map, later_pg_id, &bucket).is_none(),
        "adopted DeleteBucket attempt should resume and drain later object PGs"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted attempt should record final outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::MarkDeleting);
    assert_eq!(outcome.drain_id, preserved_drain.drain_id);
}

#[test]
fn begin_bucket_delete_adopts_preserved_initial_frontier_then_resets_before_stream_cleanup() {
    let _serial = lock_bucket_scoped_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids: Vec<u32> = (0..32).collect();
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, lower_key, later_key, pg_count) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-initial-progress-adopt-");
        let lower_key = key_for_object_pg(topology, &bucket, 0, "lower-");
        let later_pg = topology.pg_count() - 1;
        let later_key = key_for_object_pg(topology, &bucket, later_pg, "later-");
        (bucket, lower_key, later_key, topology.pg_count())
    };
    assert!(
        pg_count >= 32,
        "test requires several exact-bucket drain chunks"
    );
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let initial_bucket = {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap()
    };

    let hook_stage = Arc::new(AtomicUsize::new(0));
    let first_frontier = Arc::new(AtomicUsize::new(0));
    let reset_seen = Arc::new(AtomicBool::new(false));
    let hook_stage_for_hook = Arc::clone(&hook_stage);
    let first_frontier_for_hook = Arc::clone(&first_frontier);
    let reset_seen_for_hook = Arc::clone(&reset_seen);
    let _progress_hook_guard = cluster.test_install_after_bucket_delete_exact_drain_progress_hook(
        Arc::new(move |phase, next_object_pg_id| {
            match (hook_stage_for_hook.load(Ordering::SeqCst), phase) {
                (0, crate::BucketDeleteAttemptPhase::Initial)
                    if next_object_pg_id > 0 && next_object_pg_id < pg_count =>
                {
                    first_frontier_for_hook.store(next_object_pg_id as usize, Ordering::SeqCst);
                    hook_stage_for_hook.store(1, Ordering::SeqCst);
                    return Err(StoreError::RouteMapExpired {
                        cluster_epoch: ClusterEpoch::INITIAL,
                        valid_until_ms: 0,
                        now_ms: 1,
                    });
                }
                (1, crate::BucketDeleteAttemptPhase::Initial) if next_object_pg_id == pg_count => {
                    hook_stage_for_hook.store(2, Ordering::SeqCst);
                    return Err(StoreError::RouteMapExpired {
                        cluster_epoch: ClusterEpoch::INITIAL,
                        valid_until_ms: 0,
                        now_ms: 1,
                    });
                }
                (_, crate::BucketDeleteAttemptPhase::StreamCleanup) if next_object_pg_id == 0 => {
                    reset_seen_for_hook.store(true, Ordering::SeqCst);
                }
                _ => {}
            }
            Ok(())
        }),
    );

    let first_err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            first_err,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "first DeleteBucket should preserve the attempt on injected route expiry, got {first_err:?}"
    );
    let preserved_drain = {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .expect("retryable failure should preserve the active delete drain")
    };
    let first_frontier = first_frontier.load(Ordering::SeqCst) as u32;
    assert!(
        first_frontier > 0 && first_frontier < pg_count,
        "first attempt should persist a partial initial frontier, got {first_frontier}"
    );
    let initial_outcome = cluster
        .test_bucket_delete_post_reservation_next_object_pg_id(
            &crate::cluster::DurableBucketWriteDrain {
                pg_id: 1,
                record: preserved_drain.clone(),
            },
        )
        .unwrap();
    assert_eq!(
        initial_outcome,
        Some(first_frontier),
        "first retryable failure should persist the initial exact-bucket frontier"
    );

    let lower_pg_id = PgId::new(0);
    let lower_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(lower_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            lower_key,
            crate::SessionId::try_from("87".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, lower_pg_id, &bucket, &lower_command);

    let later_pg_id = PgId::new(pg_count - 1);
    let later_command = MetadataCommandEnvelope::new(
        cluster
            .next_object_metadata_command_id(later_pg_id)
            .unwrap(),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            later_key,
            crate::SessionId::try_from("88".repeat(16)).unwrap(),
            crate::GenerationId::MIN,
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, later_pg_id, &bucket, &later_command);

    let second_err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            second_err,
            crate::BucketWriteDrainError::Store(StoreError::RouteMapExpired { .. })
        ),
        "second DeleteBucket should fail after completing the resumed initial scan, got {second_err:?}"
    );
    assert_eq!(
        hook_stage.load(Ordering::SeqCst),
        2,
        "second attempt should reach the completed initial frontier"
    );
    assert!(
        pending_metadata_command_for_test(&map, lower_pg_id, &bucket).is_some(),
        "resumed initial scan must not revisit object PGs below the stored frontier"
    );
    assert!(
        pending_metadata_command_for_test(&map, later_pg_id, &bucket).is_none(),
        "resumed initial scan must drain object PGs at or above the stored frontier"
    );

    cluster
        .begin_bucket_delete_if_current(
            &bucket,
            initial_bucket.bucket_execution_generation,
            initial_bucket.bucket_incarnation_generation,
        )
        .unwrap();
    assert!(
        reset_seen.load(Ordering::SeqCst),
        "successful retry should reset the exact-bucket frontier before stream cleanup"
    );
    assert!(
        pending_metadata_command_for_test(&map, lower_pg_id, &bucket).is_none(),
        "post-reset post-reservation scan should drain lower object PG work"
    );
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Deleting);
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("successful adopted attempt should record final outcome");
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::MarkDeleting
    );
    assert_eq!(
        outcome.post_reservation_next_object_pg_id,
        Some(0),
        "stream cleanup reset should be preserved in the final outcome"
    );
    assert_eq!(outcome.drain_id, preserved_drain.drain_id);
}

#[test]
fn begin_bucket_delete_drains_pending_delete_marker_before_emptiness_decision() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let (bucket, key) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-marker-drain-");
        let key = key_for_object_pg(topology, &bucket, 2, "marker-");
        (bucket, key)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();

    let pg_id = PgId::new(2);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let marker_version = cluster
        .reserve_next_object_version(pg_id, &bucket, &key)
        .unwrap();
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let object_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: acquire_test_bucket_write_proof(
                &cluster,
                &bucket,
                "delete-marker-drain-test",
                Some(key.as_str()),
            ),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: marker_version,
            owner: crate::OwnerIdentity::from_principal("owner"),
            write_sequence: object_pg
                .next_object_write_sequence(bucket.as_str(), key.as_str())
                .unwrap(),
            last_modified_millis: crate::clock::current_time_millis(),
            stale_payload: None,
        }),
    );
    drop(object_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "DeleteBucket should see the drained delete marker as bucket data, got {err:?}"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_version(
                    &*object_pg,
                    &bucket,
                    &key,
                    marker_version,
                ),
                Ok(crate::StoredObject::DeleteMarker(_))
            ),
            "DeleteBucket should converge the pending delete marker on node {node_id:?}"
        );
        let bucket_pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let bucket_info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket)
            .expect("BucketNotEmpty should leave the bucket active");
        assert_eq!(bucket_info.state, crate::BucketState::Active);
    }
    let bucket_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*bucket_pg, &bucket)
            .unwrap()
            .is_none(),
        "BucketNotEmpty rollback should clear the durable delete drain"
    );
    drop(bucket_pg);
    assert_clean_metadata_command_stream(&map, &[1, 2]);
}

#[test]
fn begin_bucket_delete_drains_pending_specific_version_delete_that_empties_bucket() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg_id, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "specific-delete-drain-");
        let key = key_for_object_pg(topology, &bucket, 2, "version-");
        (bucket, key, 2, 3)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg_id, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    let committed = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [91; 16],
        [92; 16],
        b"delete the only version",
    );

    let pg_id = PgId::new(object_pg_id);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let object_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let stored = crate::PgMetadataStore::get_object_version(
        &*object_pg,
        &bucket,
        &key,
        committed.version_id,
    )
    .unwrap();
    let live = stored.as_live().unwrap();
    let payload = crate::StorageCluster::snapshot_live_object_payload_reclaim_command(
        &object_pg,
        &bucket,
        &key,
        live,
        crate::clock::current_time_millis(),
    )
    .unwrap();
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
            bucket_write_reservation: acquire_test_bucket_write_proof(
                &cluster,
                &bucket,
                "specific-delete-drain-test",
                Some(key.as_str()),
            ),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: committed.version_id,
            target: DeleteObjectVersionTarget::Live {
                generation_id: live.generation_id,
                layout: live.layout,
                payload,
            },
        })),
    );
    drop(object_pg);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);

    cluster.begin_bucket_delete(&bucket).unwrap();

    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id)
            .unwrap();
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_version(
                    &*object_pg,
                    &bucket,
                    &key,
                    committed.version_id,
                ),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "DeleteBucket should converge the pending specific-version delete on node {node_id:?}"
        );
        let bucket_pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let bucket_info = crate::PgMetadataStore::head_bucket_raw(&*bucket_pg, &bucket).unwrap();
        assert_eq!(bucket_info.state, crate::BucketState::Deleting);
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id]);
}

#[test]
fn begin_bucket_delete_drains_pending_lifecycle_current_expiry_marker() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg_id, data_pg_id) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-lifecycle-current-");
        let key = key_for_object_pg(topology, &bucket, 2, "current-");
        (bucket, key, 2, 3)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg_id, NodeId::new(2));
    set_route_primary(&mut map, data_pg_id, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    put_test_lifecycle(&cluster, &bucket);
    let committed = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0x93; 16],
        [0x94; 16],
        b"lifecycle current before delete",
    );

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::InsertDeleteMarker(marker)
                    if marker.bucket == hook_bucket
                        && marker.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected lifecycle current expiry apply failure",
                        source: std::io::Error::other(
                            "injected lifecycle current expiry apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .expire_current_object_if_due(
            &bucket,
            &key,
            committed.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |_, _| Ok::<_, ()>(true),
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected lifecycle current expiry apply failure",
                ..
            })
        ),
        "expected injected lifecycle current expiry failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_some(),
        "partial lifecycle current expiry command should remain pending"
    );

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "DeleteBucket should see the lifecycle delete marker as visible data, got {err:?}"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id)
            .unwrap();
        let current = crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key)
            .expect("DeleteBucket should leave the lifecycle marker visible");
        assert!(
            matches!(current, crate::StoredObject::DeleteMarker(_)),
            "expected current delete marker on node {node_id:?}, got {current:?}"
        );
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id]);
}

#[test]
fn begin_bucket_delete_drains_pending_lifecycle_noncurrent_expiry() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg_id, data_pg_id) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-lifecycle-noncurrent-");
        let key = key_for_object_pg(topology, &bucket, 2, "noncurrent-");
        (bucket, key, 2, 3)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg_id, NodeId::new(2));
    set_route_primary(&mut map, data_pg_id, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    put_test_lifecycle(&cluster, &bucket);
    let older = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xa1; 16],
        [0xa2; 16],
        b"older lifecycle version",
    );
    let current = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xa3; 16],
        [0xa4; 16],
        b"current lifecycle version",
    );

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let older_version = older.version_id;
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && delete.version_id == older_version
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected lifecycle noncurrent expiry apply failure",
                        source: std::io::Error::other(
                            "injected lifecycle noncurrent expiry apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .delete_noncurrent_live_versions_if_due(
            &bucket,
            &key,
            current_bucket_incarnation(&cluster, &bucket),
            |_, _| Ok::<_, ()>(HashSet::from([older.version_id])),
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected lifecycle noncurrent expiry apply failure",
                ..
            })
        ),
        "expected injected lifecycle noncurrent expiry failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_some(),
        "partial lifecycle noncurrent expiry command should remain pending"
    );

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
            matches!(
                err,
                crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
            ),
            "DeleteBucket should still see the current version after draining noncurrent expiry, got {err:?}"
        );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(
                &*object_pg,
                &bucket,
                &key,
                older.version_id,
            ),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        let visible = crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key)
            .expect("current version should remain visible");
        assert_eq!(visible.version_id(), current.version_id);
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id]);
}

#[test]
fn begin_bucket_delete_drains_pending_lifecycle_expired_delete_marker_cleanup() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg_id, data_pg_id) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "delete-lifecycle-marker-");
        let key = key_for_object_pg(topology, &bucket, 2, "marker-");
        (bucket, key, 2, 3)
    };
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, object_pg_id, NodeId::new(2));
    set_route_primary(&mut map, data_pg_id, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket_with_versioning(&cluster, &bucket, crate::BucketVersioningState::Enabled);
    put_test_lifecycle(&cluster, &bucket);
    let live = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Enabled,
        [0xb1; 16],
        [0xb2; 16],
        b"live behind marker",
    );
    let marker = cluster
        .insert_current_delete_marker_if(
            &bucket,
            &key,
            crate::OwnerIdentity::from_principal("owner"),
            |_| Ok::<_, ()>(()),
        )
        .unwrap()
        .unwrap();

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let marker_version = marker.version_id;
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectVersion(delete)
                    if delete.bucket == hook_bucket
                        && delete.key == hook_key
                        && delete.version_id == marker_version
                        && matches!(
                            delete.target,
                            DeleteObjectVersionTarget::DeleteMarker { .. }
                        )
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected lifecycle delete-marker cleanup apply failure",
                        source: std::io::Error::other(
                            "injected lifecycle delete-marker cleanup apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .delete_expired_delete_marker_if_due(
            &bucket,
            &key,
            marker.version_id,
            current_bucket_incarnation(&cluster, &bucket),
            |_, _| Ok::<_, ()>(true),
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected lifecycle delete-marker cleanup apply failure",
                ..
            })
        ),
        "expected injected lifecycle delete-marker cleanup failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_some(),
        "partial lifecycle delete-marker cleanup command should remain pending"
    );

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
            matches!(
                err,
                crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
            ),
            "DeleteBucket should still see the revealed live version after draining marker cleanup, got {err:?}"
        );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg_id), &bucket).is_none());
    assert_bucket_write_reservations_released(&map, &bucket);
    for node_id in node_ids {
        let object_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg_id)
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(
                &*object_pg,
                &bucket,
                &key,
                marker.version_id,
            ),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        let visible = crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key)
            .expect("live version should be revealed after marker cleanup");
        assert_eq!(visible.version_id(), live.version_id);
    }
    assert_clean_metadata_command_stream(&map, &[1, object_pg_id]);
}

#[test]
fn stale_delete_drain_identity_cannot_clear_recreated_bucket_drain() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "stale-delete-drain-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    cluster.begin_bucket_delete(&bucket).unwrap();

    let old_drain = {
        let node = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg_id = 1;
        let pg = node.get_pg(pg_id).unwrap();
        let record = crate::PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
            .unwrap()
            .expect("DeleteBucket begin should leave a terminal durable drain");
        crate::cluster::DurableBucketWriteDrain { pg_id, record }
    };
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
    create_test_bucket(&cluster, &bucket);

    let new_drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("recreated active bucket should acquire a fresh delete drain")
        }
    };
    assert_ne!(
        old_drain.record.bucket_execution_generation, new_drain.record.bucket_execution_generation,
        "delete/recreate must produce a distinct bucket incarnation"
    );

    let err = cluster
        .clear_durable_bucket_delete_drain(&old_drain)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(
                crate::MetadataError::BucketWriteDrainNotFound { .. }
            )
        ),
        "stale drain cleanup should not match the recreated bucket, got {err:?}"
    );
    {
        let pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let current = crate::PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
            .unwrap()
            .expect("fresh drain should remain installed");
        assert_eq!(current.drain_id, new_drain.record.drain_id);
        assert_eq!(
            current.bucket_execution_generation,
            new_drain.record.bucket_execution_generation
        );
    }

    cluster
        .clear_durable_bucket_delete_drain(&new_drain)
        .unwrap();
    let info = cluster.head_bucket_info(&bucket).unwrap();
    assert_eq!(info.state, crate::BucketState::Active);
}

#[test]
fn begin_bucket_delete_recovers_expired_durable_drain_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let bucket = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_for_pg(topology, 1, "expired-delete-drain-")
    };
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let old_drain_id = "expired-delete-drain-before-reopen";
    {
        let pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let now = crate::clock::current_time_millis();
        crate::PgMetadataStore::begin_durable_bucket_write_drain(
            &*pg,
            &bucket,
            old_drain_id,
            "dead-delete-owner",
            crate::ClusterEpoch::INITIAL,
            now.saturating_sub(10),
            Some(now.saturating_sub(1)),
        )
        .unwrap();
    }
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap());
    let reopened_cluster = crate::StorageCluster::from_local_map(Arc::clone(&reopened)).unwrap();
    reopened_cluster.begin_bucket_delete(&bucket).unwrap();

    {
        let pg = reopened
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let current = crate::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert_eq!(current.state, crate::BucketState::Deleting);
        let drain = crate::PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
            .unwrap()
            .expect(
                "DeleteBucket should leave a terminal drain after recovering the expired drain",
            );
        assert_ne!(
            drain.drain_id, old_drain_id,
            "expired pre-reopen drain must be rolled back by exact identity"
        );
    }
    assert!(
        matches!(
            reopened_cluster
                .begin_durable_bucket_delete_drain(&bucket)
                .unwrap(),
            crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting
        ),
        "recovered terminal delete should be idempotent after reopen"
    );
    assert_clean_metadata_command_stream(&reopened, &[1]);
}
