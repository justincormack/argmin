use super::*;
use crate::ObjectPayloadReclaimKind;

#[test]
fn object_payload_reclaim_capacity_counts_dequeued_work_until_finished() {
    let runtime_state = LocalClusterRuntimeState::new();
    let bucket = BucketName::try_from("bucket").unwrap();
    let key_a = ObjectKey::try_from("a".to_string()).unwrap();
    let key_b = ObjectKey::try_from("b".to_string()).unwrap();
    let key_c = ObjectKey::try_from("c".to_string()).unwrap();
    let generation_a = GenerationId::new(1).unwrap();
    let generation_b = GenerationId::new(2).unwrap();
    let generation_c = GenerationId::new(3).unwrap();

    assert_eq!(
        runtime_state.enqueue_object_payload_reclaim(&bucket, &key_a, generation_a, 0),
        ReclaimQueueInsert::Queued
    );
    assert_eq!(
        runtime_state.enqueue_object_payload_reclaim(&bucket, &key_b, generation_b, 0),
        ReclaimQueueInsert::Queued
    );
    assert_eq!(
        runtime_state.enqueue_object_payload_reclaim(&bucket, &key_c, generation_c, 0),
        ReclaimQueueInsert::PgCapacityDeferred
    );

    assert_eq!(
        runtime_state.try_take_reclaim_work(),
        Some(ReclaimWorkItem::ObjectPayload((
            bucket.clone(),
            key_a.clone(),
            generation_a
        )))
    );
    assert_eq!(
        runtime_state.try_take_reclaim_work(),
        Some(ReclaimWorkItem::ObjectPayload((
            bucket.clone(),
            key_b.clone(),
            generation_b
        )))
    );
    assert_eq!(
        runtime_state.enqueue_object_payload_reclaim(&bucket, &key_c, generation_c, 0),
        ReclaimQueueInsert::PgCapacityDeferred
    );

    runtime_state.finish_object_payload_reclaim_work(&bucket, &key_a, generation_a);
    assert_eq!(
        runtime_state.enqueue_object_payload_reclaim(&bucket, &key_c, generation_c, 0),
        ReclaimQueueInsert::Queued
    );
}

#[test]
fn object_scoped_reclaim_count_sees_later_same_pg_root() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let bucket = BucketName::try_from("bucket").unwrap();
    let first_key = ObjectKey::try_from("a-first".to_string()).unwrap();
    let target_key = ObjectKey::try_from("z-target".to_string()).unwrap();

    cluster
        .test_seed_segmented_payload_reclaim(&bucket, &first_key, GenerationId::new(1).unwrap(), 1)
        .unwrap();
    cluster
        .test_seed_segmented_payload_reclaim(&bucket, &target_key, GenerationId::new(1).unwrap(), 1)
        .unwrap();

    assert_eq!(
        crate::test_support::object_payload_reclaim_root_count_for(&cluster, &bucket, &target_key,)
            .unwrap(),
        1,
        "an earlier root on the same PG must not hide the target object's root"
    );
}

#[test]
fn opaque_segmented_reclaim_subject_skips_live_generation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let bucket = BucketName::try_from("bucket").unwrap();
    let key = ObjectKey::try_from("live-key".to_string()).unwrap();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"live payload");

    let candidate = cluster
        .test_next_unreferenced_object_generation(&bucket, &key)
        .unwrap();
    assert!(candidate.get() > committed.generation_id.get());
    let subject =
        crate::test_support::seed_segmented_object_payload_reclaim(&cluster, &bucket, &key, 1)
            .unwrap();
    assert!(crate::test_support::object_payload_has_reclaim_root(&cluster, &subject,).unwrap());
    assert!(crate::test_support::reclaim_object_payload_if_unleased(&cluster, &subject,).unwrap());

    for shard_index in 0..committed.written.ec.k + committed.written.ec.m {
        assert!(
            cluster
                .test_payload_shard_file_exists(
                    committed.written.data_pg_id,
                    committed.written.ec,
                    &committed.segment_okh,
                    committed.generation_id,
                    shard_index,
                )
                .unwrap(),
            "synthetic reclaim must not delete live generation shard {shard_index}"
        );
    }
}

#[test]
fn bucket_payload_reclaim_root_validation_rejects_wrong_object_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 0, "payload-root-validation-");
    let key_pg0 = key_for_object_pg(topology, &bucket, 0, "object-pg0-");
    let key_pg1 = key_for_object_pg(topology, &bucket, 1, "object-pg1-");
    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();

    cluster
        .validate_bucket_payload_reclaim_root_for_pg(
            PgId::new(0),
            &crate::PayloadReclaimRoot {
                bucket: bucket.clone(),
                key: key_pg0,
                generation_id: crate::GenerationId::MIN,
            },
            NodeId::new(7),
        )
        .unwrap();
    let err = cluster
        .validate_bucket_payload_reclaim_root_for_pg(
            PgId::new(0),
            &crate::PayloadReclaimRoot {
                bucket,
                key: key_pg1,
                generation_id: crate::GenerationId::MIN,
            },
            NodeId::new(7),
        )
        .unwrap_err();

    assert!(matches!(
        err,
        crate::BucketWriteDrainError::Store(StoreError::StorageRpc {
            node_id: 7,
            operation: "object bucket payload reclaim root",
            ..
        })
    ));
}
#[test]
fn stream_append_registers_payload_acks_on_routed_data_pg_primary() {
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
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("03".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let payload = b"stream append payload with routed data acks";
    let segment_okh = crate::stream_segment_key_hash(&session_id, 0);
    let (_, segment_record) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                payload_crc64: checksum::crc64::checksum(payload),
                segment_okh,
            },
        )
        .unwrap();
    assert_eq!(segment_record.data_pg_id, data_pg);

    let written = cluster
        .write_stream_segment_payload_shards(&segment_record, payload)
        .unwrap();
    let shard_batch: Vec<(&ShardKey, crate::WriteAck)> = written
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    cluster
        .commit_stream_segment_append(&bucket, &key, &session_id, 0, &segment_record, &shard_batch)
        .unwrap();

    let first_shard_key = &written[0].key;
    assert!(map
        .nodes
        .get(&NodeId::new(2))
        .unwrap()
        .storage_node()
        .test_shard_exists(data_pg, first_shard_key)
        .unwrap());
    assert!(!map
        .nodes
        .get(&NodeId::new(1))
        .unwrap()
        .storage_node()
        .test_shard_exists(data_pg, first_shard_key)
        .unwrap());

    let mut readback = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: data_pg,
                segment_okh: segment_record.segment_okh,
                segment_vid: segment_record.segment_vid,
                stored_size: payload.len(),
                segment_crc64: checksum::crc64::checksum(payload),
                ec: EcShape {
                    k: segment_record.ec_k,
                    m: segment_record.ec_m,
                },
            },
            &mut readback,
        )
        .unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn lease_release_requeues_routed_reclaim_after_worker_defers_for_active_lease() {
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

    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"leased payload");
    assert_eq!(committed.written.data_pg_id, data_pg);

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
        "delete should create reclaim metadata on the routed object PG primary"
    );

    cluster.enqueue_object_payload_reclaim(&bucket, &key, committed.generation_id);
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == committed.generation_id
    ));
    assert!(
        !cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "active lease should make the worker defer reclaim"
    );
    assert!(cluster.try_take_reclaim_work().is_none());

    let released = lease.release();
    assert_eq!(released.remaining(), 0);
    assert!(
        released.payload_reclaim_exists().unwrap(),
        "released lease must find reclaim metadata through the routed object PG primary"
    );
    let object_payload_reclaim_event_count = |event: &'static str| {
        observability::object_payload_reclaim_event_dimension_snapshot()
            .iter()
            .find(|sample| sample.pg_id == object_pg && sample.event == event)
            .map_or(0, |sample| sample.count)
    };
    let deduplicated_events_before_release_requeue =
        object_payload_reclaim_event_count("deduplicated");
    released.enqueue_object_payload_reclaim();
    assert!(
        object_payload_reclaim_event_count("deduplicated")
            > deduplicated_events_before_release_requeue,
        "lease-release requeue should deduplicate against the worker's deferred reclaim root"
    );
    assert!(
        cluster.try_take_reclaim_work().is_none(),
        "the deferred root remains owned by the worker until terminal completion"
    );

    assert!(
        cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "lease release should make the worker's deferred reclaim retryable"
    );
    cluster.finish_object_payload_reclaim_work(&bucket, &key, committed.generation_id);
    assert!(!cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
    for shard_index in 0..committed.written.ec.k + committed.written.ec.m {
        assert!(
            !cluster
                .test_payload_shard_file_exists(
                    committed.written.data_pg_id,
                    committed.written.ec,
                    &committed.segment_okh,
                    committed.generation_id,
                    shard_index,
                )
                .unwrap(),
            "retried reclaim should delete placed shard {shard_index}"
        );
    }
}

#[test]
fn object_payload_reclaim_defers_behind_unrelated_pending_object_command() {
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
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let old = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [31; 16],
        [32; 16],
        b"old payload",
    );
    let _new = write_committed_direct_segment_for_with_versioning(
        &cluster,
        &bucket,
        &key,
        crate::BucketVersioningState::Disabled,
        [33; 16],
        [34; 16],
        b"new payload",
    );
    assert!(
        cluster
            .payload_reclaim_exists(&bucket, &key, old.generation_id)
            .unwrap(),
        "overwrite should create stale-payload reclaim metadata"
    );

    let pg_id = PgId::new(object_pg);
    let command_id = cluster.next_object_metadata_command_id(pg_id).unwrap();
    let pending = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key.clone(),
            crate::SessionId::try_from("ab".repeat(16)).unwrap(),
            GenerationId::new(100).unwrap(),
            crate::clock::current_time_millis(),
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending);

    assert!(
        !cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, old.generation_id)
            .unwrap(),
        "background reclaim should defer instead of draining foreground object metadata"
    );
    assert_eq!(
        pending_metadata_command_for_test(&map, pg_id, &bucket),
        Some(pending),
        "deferred reclaim must leave the existing pending command for foreground drain"
    );
    assert_eq!(
        object_payload_reclaim_claim_count_for_test(&map, pg_id),
        0,
        "deferred reclaim must not acquire a durable claim before it owns cleanup"
    );
    assert!(
        cluster
            .payload_reclaim_exists(&bucket, &key, old.generation_id)
            .unwrap(),
        "deferred reclaim root must remain retryable"
    );
}

#[test]
fn object_payload_reclaim_acquires_and_releases_durable_claim() {
    let _serial = lock_metadata_command_apply_hook_test();
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
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"claim payload");
    cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    let saw_claim = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let saw_claim_hook = Arc::clone(&saw_claim);
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            assert_eq!(
                object_payload_reclaim_claim_count_for_test(&hook_map, PgId::new(object_pg)),
                1,
                "reclaim worker must hold a durable claim before publishing terminal cleanup"
            );
            saw_claim_hook.store(true, Ordering::SeqCst);
        }));

    assert!(
        cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "unleased reclaim should complete"
    );
    assert!(saw_claim.load(Ordering::SeqCst));
    assert_eq!(
        object_payload_reclaim_claim_count_for_test(&map, PgId::new(object_pg)),
        0,
        "terminal reclaim command should release the durable claim"
    );
    assert!(!cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
}

#[test]
fn object_payload_reclaim_expiry_at_claim_insertion_preserves_retryable_root() {
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
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap());
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"expired reclaim claim");
    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap()
        .unwrap();
    assert!(cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());

    let pg_id = PgId::new(object_pg);
    let command_stream_before = collect_metadata_replay_snapshot(&map, &node_ids, &[object_pg]);
    let clock = Arc::new(crate::clock::test_time_override_guard(1_000));
    cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));

    let hook_cluster = Arc::clone(&cluster);
    let hook_clock = Arc::clone(&clock);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    map.node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap()
        .test_install_before_object_payload_reclaim_claim_effect_check_hook(move || {
            hook_cluster.test_store_route_map_lease(
                RouteMapValidity::until_ms(10_000).unwrap(),
                Some(9_000),
            );
            hook_clock.set(4_500);
            hook_ran_for_hook.store(true, Ordering::SeqCst);
        });

    let error = cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap_err();
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::RouteMapExpired { .. })
    ));
    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test must renew the raw route and expire the captured fence inside the claim transaction"
    );
    assert_eq!(
        object_payload_reclaim_claim_count_for_test(&map, pg_id),
        0,
        "expired authority must not insert a durable reclaim claim"
    );
    assert_eq!(
        collect_metadata_replay_snapshot(&map, &node_ids, &[object_pg]),
        command_stream_before,
        "rejected claim insertion must not advance any replica command log"
    );
    assert!(
        cluster
            .payload_reclaim_exists(&bucket, &key, committed.generation_id)
            .unwrap(),
        "rejected claim insertion must preserve durable reclaim state for retry"
    );
}

#[test]
fn object_payload_reclaim_expiry_between_build_and_pending_install_preserves_root() {
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
    let cluster = Arc::new(crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap());
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"expired reclaim install");
    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap()
        .unwrap();
    assert!(cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());

    let pg_id = PgId::new(object_pg);
    let command_stream_before = collect_metadata_replay_snapshot(&map, &node_ids, &[object_pg]);
    let clock = Arc::new(crate::clock::test_time_override_guard(1_000));
    cluster.test_store_route_map_lease(RouteMapValidity::until_ms(5_000).unwrap(), Some(4_000));

    let hook_cluster = Arc::clone(&cluster);
    let hook_clock = Arc::clone(&clock);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_hook = Arc::clone(&hook_ran);
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            hook_cluster.test_store_route_map_lease(
                RouteMapValidity::until_ms(10_000).unwrap(),
                Some(9_000),
            );
            hook_clock.set(4_500);
            hook_ran_for_hook.store(true, Ordering::SeqCst);
        }));

    let error = cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap_err();
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(StoreError::RouteMapExpired { .. })
    ));
    assert!(
        hook_ran.load(Ordering::SeqCst),
        "test must expire the captured fence after command construction"
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "expired authority must not install a pending reclaim command"
    );
    assert_eq!(
        collect_metadata_replay_snapshot(&map, &node_ids, &[object_pg]),
        command_stream_before,
        "expired pending-slot insertion must not advance any replica log"
    );
    assert_eq!(
        object_payload_reclaim_claim_count_for_test(&map, pg_id),
        0,
        "failed insertion must release the frontend-owned reclaim claim"
    );
    assert!(
        cluster
            .payload_reclaim_exists(&bucket, &key, committed.generation_id)
            .unwrap(),
        "failed insertion must preserve durable reclaim state for retry"
    );
}

#[test]
fn object_payload_reclaim_retry_releases_surviving_terminal_claim() {
    let _serial = lock_metadata_command_apply_hook_test();
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
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"retry claim payload");
    let generation_id = committed.generation_id;
    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap()
        .unwrap();

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::DeleteObjectPayloadReclaim(reclaim)
                    if reclaim.matches_request(&hook_bucket, &hook_key, generation_id)
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected reclaim apply failure",
                        source: std::io::Error::other("injected reclaim apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, generation_id)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::Io {
            context: "injected reclaim apply failure",
            ..
        })
    ));
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "failed terminal cleanup must keep the pending command"
    );
    assert_eq!(
        object_payload_reclaim_claim_count_for_test(&map, PgId::new(object_pg)),
        0,
        "primary-first terminal cleanup releases the durable claim before replica failure"
    );

    assert!(
        cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, generation_id)
            .unwrap(),
        "retry should finish the exact pending reclaim command"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_eq!(
        object_payload_reclaim_claim_count_for_test(&map, PgId::new(object_pg)),
        0,
        "retrying the terminal command must release the surviving durable claim"
    );
    assert!(!cluster
        .payload_reclaim_exists(&bucket, &key, generation_id)
        .unwrap());
}

#[test]
fn durable_reclaim_scan_recovers_lost_local_queue_after_reopen() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let pg_ids = [0, 1, 2, 3];

    let (bucket, key, generation_id) = {
        let map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap());
        let (bucket, key, _, _) = {
            let topology = map
                .nodes
                .get(&NodeId::new(0))
                .unwrap()
                .storage_node()
                .pg_topology();
            bucket_key_with_distinct_object_and_data_pg(topology)
        };
        let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
        let committed =
            write_committed_direct_segment_for(&cluster, &bucket, &key, b"lost hint payload");
        cluster
            .delete_current_object_if(&bucket, &key, |stored| {
                assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
                Ok::<(), ()>(())
            })
            .unwrap()
            .unwrap();
        assert!(
            cluster
                .payload_reclaim_exists(&bucket, &key, committed.generation_id)
                .unwrap(),
            "delete should leave a durable reclaim root"
        );
        (bucket, key, committed.generation_id)
    };

    let reopened_map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap());
    let reopened_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&reopened_map)).unwrap();
    assert_eq!(
        reopened_cluster
            .enqueue_durable_object_payload_reclaim_roots_excluding(&HashSet::new())
            .queued,
        1,
        "startup scan should rediscover the durable root without an in-memory hint"
    );
    assert!(matches!(
        reopened_cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == generation_id
    ));
    assert!(
        reopened_cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, generation_id)
            .unwrap(),
        "reopened worker should complete durable reclaim"
    );
    assert!(!reopened_cluster
        .payload_reclaim_exists(&bucket, &key, generation_id)
        .unwrap());
}

#[test]
fn durable_reclaim_scan_retries_same_pg_until_all_reopened_roots_are_discovered() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let pg_ids = [0, 1, 2, 3];
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();

    let expected_roots = {
        let map =
            Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap());
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let key_a = key_for_object_pg(topology, &bucket, 1, "reopened-root-a-");
        let key_b = key_for_object_pg(topology, &bucket, 1, "reopened-root-b-");
        let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
        let committed_a = write_committed_direct_segment_for_with_okh(
            &cluster,
            &bucket,
            &key_a,
            [41; 16],
            b"first reopened root",
        );
        let committed_b = write_committed_direct_segment_for_with_okh(
            &cluster,
            &bucket,
            &key_b,
            [42; 16],
            b"second reopened root",
        );
        for key in [&key_a, &key_b] {
            cluster
                .delete_current_object_if(&bucket, key, |_| Ok::<(), ()>(()))
                .unwrap()
                .unwrap();
        }
        [
            (key_a, committed_a.generation_id),
            (key_b, committed_b.generation_id),
        ]
    };

    let reopened_map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap());
    let reopened_cluster = crate::StorageCluster::from_static_local_map(reopened_map).unwrap();
    let mut discovered = Vec::new();
    for expected_remaining in [2, 1] {
        let batch = reopened_cluster.enqueue_durable_reclaim_work_batch_excluding(
            None,
            pg_ids.len(),
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
        );
        assert_eq!(batch.outcome, crate::DurableReclaimScanOutcome::Complete);
        assert_eq!(batch.next_pg_id, None);
        assert!(
            batch.retry_pass_required,
            "a returned single-row root cannot prove the PG is exhausted"
        );
        let (queued_bucket, key, generation_id) = loop {
            match reopened_cluster.try_take_reclaim_work() {
                Some(crate::ReclaimWorkItem::ObjectPayload(root)) => break root,
                Some(crate::ReclaimWorkItem::BucketDelete(root)) => {
                    reopened_cluster.finish_bucket_delete_finalize_work(&root);
                }
                Some(other) => panic!("unexpected startup reclaim work: {other:?}"),
                None => panic!("startup pass should queue one reopened durable root"),
            }
        };
        assert_eq!(queued_bucket, bucket);
        assert!(
            expected_roots.contains(&(key.clone(), generation_id)),
            "startup pass queued an unexpected durable root"
        );
        assert!(
            reopened_cluster
                .reclaim_object_payload_if_unleased(&bucket, &key, generation_id)
                .unwrap(),
            "discovered root should be reclaimable"
        );
        discovered.push((key, generation_id));
        assert_eq!(
            expected_roots
                .iter()
                .filter(|(key, generation_id)| reopened_cluster
                    .payload_reclaim_exists(&bucket, key, *generation_id)
                    .unwrap())
                .count(),
            expected_remaining - 1
        );
    }
    assert_ne!(discovered[0], discovered[1]);
    while let Some(work) = reopened_cluster.try_take_reclaim_work() {
        match work {
            crate::ReclaimWorkItem::BucketDelete(root) => {
                reopened_cluster.finish_bucket_delete_finalize_work(&root);
            }
            other => panic!("unexpected trailing reclaim work: {other:?}"),
        }
    }

    let clean = reopened_cluster.enqueue_durable_reclaim_work_batch_excluding(
        None,
        pg_ids.len(),
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
    );
    assert_eq!(clean.outcome, crate::DurableReclaimScanOutcome::Complete);
    assert!(!clean.retry_pass_required);
    assert!(reopened_cluster.try_take_reclaim_work().is_none());
}

#[test]
fn durable_reclaim_scan_continues_after_unavailable_pg() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let pg_ids = [0, 1, 2, 3];
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        key_for_object_pg(topology, &bucket, 1, "scan-key-")
    };
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"later pg reclaim");
    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<(), ()>(()))
        .unwrap()
        .unwrap();
    drop(cluster);

    let mut map = Arc::try_unwrap(map).expect("test should hold the only map reference");
    map.pg_routes.get_mut(&PgId::new(0)).unwrap().state = PgState::Peering;
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let scan = cluster.enqueue_durable_object_payload_reclaim_roots_excluding(&HashSet::new());
    assert_eq!(
        scan.errors, 1,
        "unavailable PG should be reported in scan stats"
    );
    assert_eq!(
        scan.queued, 0,
        "healthy PG root was already outstanding from the original delete enqueue"
    );
    assert!(
        scan.retry_required,
        "an unavailable PG must require a short complete-pass retry"
    );
    assert!(matches!(
        cluster.try_take_reclaim_work(),
        Some(crate::ReclaimWorkItem::ObjectPayload((
            queued_bucket,
            queued_key,
            queued_generation_id
        ))) if queued_bucket == bucket
            && queued_key == key
            && queued_generation_id == committed.generation_id
    ));
}

#[test]
fn durable_reclaim_scan_defers_before_pg_walk_when_route_map_expired() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_static_local_map(map).unwrap();
    cluster.test_store_route_map_validity(RouteMapValidity::until_ms(0).unwrap());

    let batch = cluster.enqueue_durable_reclaim_work_batch_excluding(
        Some(2),
        1,
        &HashSet::new(),
        &HashSet::new(),
        &HashSet::new(),
    );
    assert_eq!(
        batch.outcome,
        crate::DurableReclaimScanOutcome::RouteRefreshRequired
    );
    assert_eq!(batch.next_pg_id, Some(2));
    assert_eq!(batch.scanned_pgs, 0);
    assert!(!batch.retry_pass_required);
}

#[test]
fn payload_lease_blocks_reclaim_across_cluster_handles() {
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
    let reader_cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let reclaim_cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&reader_cluster, &bucket, &key, b"shared lease");

    let lease = reader_cluster
        .acquire_object_payload_lease(&bucket, &key, committed.generation_id)
        .unwrap();
    assert_eq!(
        reclaim_cluster.object_payload_lease_count(&bucket, &key, committed.generation_id),
        1,
        "lease count must be visible through another cluster handle"
    );
    reclaim_cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(
        !reclaim_cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "storage-node-owned lease should block reclaim from another cluster handle"
    );

    drop(lease);
    assert_eq!(
        reader_cluster.object_payload_lease_count(&bucket, &key, committed.generation_id),
        0
    );
    assert!(
        reclaim_cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "reclaim should proceed after the cross-handle lease is released"
    );
}

#[test]
fn payload_lease_for_shard_locations_only_acquires_selected_storage_nodes() {
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
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"selected shard lease");
    let selected = committed.locations[0];

    let lease = cluster
        .acquire_object_payload_lease_for_shard_locations(
            &bucket,
            &key,
            committed.generation_id,
            &[selected],
        )
        .unwrap();
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let expected = usize::from(node_id == selected.node_id());
        assert_eq!(
            node.object_payload_lease_count(&bucket, &key, committed.generation_id),
            expected,
            "unexpected selected-shard lease count on node {node_id:?}"
        );
    }

    cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(
        !cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
            .unwrap(),
        "a lease on one selected shard owner must block whole-generation reclaim"
    );
    drop(lease);
    assert!(cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap());
}

#[test]
fn payload_lease_for_historical_shard_location_uses_retained_route() {
    let tmp = test_util::tempdir();
    let node_ids = [
        NodeId::new(0),
        NodeId::new(1),
        NodeId::new(2),
        NodeId::new(3),
    ];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::new(2).unwrap();
    let configs = node_ids.map(|node_id| {
        LocalNodeStoreConfig::new(
            node_id,
            tmp.path().join(format!("node-{}", node_id.as_u32())),
        )
    });
    let mut map = LocalClusterMap::open_with_configs_and_epoch(
        NodeId::new(0),
        configs,
        &[0],
        ec_shape,
        current_epoch,
    )
    .unwrap();
    let current_route = map.pg_routes.get_mut(&PgId::new(0)).unwrap();
    current_route.primary_node_id = NodeId::new(1);
    current_route.acting_set = Arc::from([NodeId::new(1), NodeId::new(2), NodeId::new(3)]);
    map.test_install_historical_pg_routes([PgRouteSnapshot::reconstructed(
        ClusterEpoch::INITIAL,
        PgId::new(0),
        NodeId::new(0),
        vec![NodeId::new(0), NodeId::new(1), NodeId::new(2)],
        PgState::Active,
    )]);

    let bucket = BucketName::try_from("bucket".to_string()).unwrap();
    let key = ObjectKey::try_from("key".to_string()).unwrap();
    let generation_id = GenerationId::MIN;
    let historical = ShardLocation::new(
        ClusterEpoch::INITIAL,
        DataPgId::new_for_test(PgId::new(0)),
        ShardIndex::new(0),
        NodeId::new(0),
    );

    let leases = map
        .try_acquire_object_payload_lease_on_locations(&bucket, &key, generation_id, &[historical])
        .unwrap();
    assert_eq!(leases.len(), 1);
    for node_id in node_ids {
        let expected = usize::from(node_id == historical.node_id());
        assert_eq!(
            map.node(node_id)
                .unwrap()
                .storage_node()
                .object_payload_lease_count(&bucket, &key, generation_id),
            expected,
            "historical payload lease must bind only its recorded shard owner"
        );
    }
    drop(leases);
    assert_eq!(
        map.node(historical.node_id())
            .unwrap()
            .storage_node()
            .object_payload_lease_count(&bucket, &key, generation_id),
        0
    );
}

#[test]
fn payload_lease_for_shard_locations_releases_partial_acquire_on_fence() {
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
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"partial acquire");
    let first = committed.locations[0];
    let fenced = committed
        .locations
        .iter()
        .copied()
        .find(|location| location.node_id() != first.node_id())
        .expect("EC placement should use at least two storage nodes");
    let fenced_node = map.node(fenced.node_id()).unwrap().storage_node();
    let reclaim_authority = ObjectPayloadReclaimClaimProof {
        bucket_incarnation_generation: 1,
        reclaim_kind: ObjectPayloadReclaimKind::ObjectSegments,
        claim_id: "partial-acquire-claim".to_string(),
        owner_token: "partial-acquire-owner".to_string(),
        cluster_epoch: map.epoch,
    };
    assert!(fenced_node.try_begin_object_payload_reclaim(
        &bucket,
        &key,
        committed.generation_id,
        &reclaim_authority,
    ));

    match cluster.acquire_object_payload_lease_for_shard_locations(
        &bucket,
        &key,
        committed.generation_id,
        &[first, fenced],
    ) {
        Ok(_) => panic!("fenced shard location unexpectedly acquired a payload lease"),
        Err(error) => assert!(matches!(error, crate::StoreError::NotFound)),
    }
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        assert_eq!(
            node.object_payload_lease_count(&bucket, &key, committed.generation_id),
            0,
            "failed all-or-release acquisition leaked a lease on node {node_id:?}"
        );
    }
    assert!(fenced_node.finish_object_payload_reclaim(
        &bucket,
        &key,
        committed.generation_id,
        &reclaim_authority,
        false,
    ));
}

#[test]
fn payload_lease_for_shard_locations_prevalidates_nodes_before_acquire() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0], ec_shape).unwrap();
    let route = map.pg_routes.get_mut(&PgId::new(0)).unwrap();
    route.primary_node_id = NodeId::new(0);
    route.acting_set = Arc::from([NodeId::new(0), NodeId::new(99)]);

    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let generation_id = crate::GenerationId::MIN;
    let locations = [
        ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new_for_test(PgId::new(0)),
            ShardIndex::new(0),
            NodeId::new(0),
        ),
        ShardLocation::new(
            ClusterEpoch::INITIAL,
            DataPgId::new_for_test(PgId::new(0)),
            ShardIndex::new(1),
            NodeId::new(99),
        ),
    ];

    let err = match map.try_acquire_object_payload_lease_on_locations(
        &bucket,
        &key,
        generation_id,
        &locations,
    ) {
        Ok(_) => panic!("missing selected node unexpectedly acquired read handles"),
        Err(error) => error,
    };
    assert!(matches!(
        err,
        StoreError::NodeNotFound {
            node_id: 99,
            pg_id: 0,
            cluster_epoch: ClusterEpoch::INITIAL,
        }
    ));
    assert_eq!(
        map.node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .object_payload_lease_count(&bucket, &key, generation_id),
        0,
        "missing later selected node must not leak an earlier acquired read handle"
    );
}

#[test]
fn payload_reclaim_in_progress_blocks_new_payload_leases() {
    let _serial = lock_payload_cleanup_hook_test();
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

    let cluster = crate::StorageCluster::from_static_local_map(Arc::new(map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"reclaim race payload");
    cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let hook_gate = Arc::clone(&gate);
    let _hook_guard =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |_| {
            let (lock, cv) = &*hook_gate;
            let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
            if !state.0 {
                state.0 = true;
                cv.notify_all();
                while !state.1 {
                    state = cv.wait(state).unwrap_or_else(|e| e.into_inner());
                }
            }
            Ok(())
        }));

    let reclaim_cluster = Arc::clone(&cluster);
    let reclaim_bucket = bucket.clone();
    let reclaim_key = key.clone();
    let reclaim_generation_id = committed.generation_id;
    let reclaim_thread = std::thread::spawn(move || {
        reclaim_cluster.reclaim_object_payload_if_unleased(
            &reclaim_bucket,
            &reclaim_key,
            reclaim_generation_id,
        )
    });

    let (lock, cv) = &*gate;
    let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
    while !state.0 {
        state = cv.wait(state).unwrap_or_else(|e| e.into_inner());
    }
    let err = match cluster.acquire_object_payload_lease(&bucket, &key, committed.generation_id) {
        Ok(_) => panic!("new lease acquired after payload reclaim started"),
        Err(error) => error,
    };
    assert!(
        matches!(err, crate::StoreError::NotFound),
        "new leases must be rejected once reclaim starts deleting payload, got {err:?}"
    );
    state.1 = true;
    cv.notify_all();
    drop(state);

    assert!(reclaim_thread.join().unwrap().unwrap());
    assert!(!cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
}

#[test]
fn reclaim_placed_payload_delete_failure_preserves_payload_until_retry() {
    let _serial = lock_payload_cleanup_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap());
    let (bucket, key, _, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"retryable payload");
    cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    let failing_key = ShardKey::new(&committed.segment_okh, committed.generation_id.get(), 0);
    let hook_guard =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |shard_key| {
            if shard_key == &failing_key {
                return Err(crate::StoreError::Io {
                    context: "injected reclaim placed delete failure",
                    source: std::io::Error::other("injected reclaim placed delete failure"),
                });
            }
            Ok(())
        }));
    let error = cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap_err();
    assert!(matches!(
        error,
        crate::ObjectPgActionError::Store(crate::StoreError::Io {
            context: "injected reclaim placed delete failure",
            ..
        })
    ));
    assert!(cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
    for shard_index in 0..committed.written.ec.k + committed.written.ec.m {
        let shard_key = ShardKey::new(
            &committed.segment_okh,
            committed.generation_id.get(),
            shard_index,
        );
        assert!(map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .test_shard_exists(committed.written.data_pg_id, &shard_key)
            .unwrap());
        assert!(cluster
            .test_payload_shard_file_exists(
                committed.written.data_pg_id,
                committed.written.ec,
                &committed.segment_okh,
                committed.generation_id,
                shard_index,
            )
            .unwrap());
    }

    drop(hook_guard);
    assert!(cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap());
    assert!(!cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
    for shard_index in 0..committed.written.ec.k + committed.written.ec.m {
        let shard_key = ShardKey::new(
            &committed.segment_okh,
            committed.generation_id.get(),
            shard_index,
        );
        assert!(!map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .test_shard_exists(committed.written.data_pg_id, &shard_key)
            .unwrap());
        assert!(!cluster
            .test_payload_shard_file_exists(
                committed.written.data_pg_id,
                committed.written.ec,
                &committed.segment_okh,
                committed.generation_id,
                shard_index,
            )
            .unwrap());
    }
}

#[test]
fn reclaim_payload_cleanup_failure_keeps_payload_lease_fence_until_retry() {
    let _serial = lock_payload_cleanup_hook_test();
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
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"cleanup fence payload");
    cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    let failed_ack_delete = Arc::new(AtomicBool::new(false));
    let failed_ack_delete_hook = Arc::clone(&failed_ack_delete);
    let hook_guard = cluster.test_install_before_metadata_primary_payload_ack_delete_hook(
        Arc::new(move |_shard_key| {
            if !failed_ack_delete_hook.swap(true, Ordering::SeqCst) {
                return Err(crate::StoreError::Io {
                    context: "injected reclaim ack delete failure",
                    source: std::io::Error::other("injected reclaim ack delete failure"),
                });
            }
            Ok(())
        }),
    );
    let err = cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(crate::StoreError::Io {
            context: "injected reclaim ack delete failure",
            ..
        })
    ));
    assert!(failed_ack_delete.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none(),
        "ack cleanup failure happens before the reclaim metadata delete command is installed"
    );
    assert!(cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
    for shard_index in 0..committed.written.ec.k + committed.written.ec.m {
        assert!(
            !cluster
                .test_payload_shard_file_exists(
                    committed.written.data_pg_id,
                    committed.written.ec,
                    &committed.segment_okh,
                    committed.generation_id,
                    shard_index,
                )
                .unwrap(),
            "placed shard {shard_index} should already be deleted before ack cleanup fails"
        );
    }

    let err = match cluster.acquire_object_payload_lease(&bucket, &key, committed.generation_id) {
        Ok(_) => panic!("new lease acquired after payload cleanup failed mid-reclaim"),
        Err(error) => error,
    };
    assert!(
        matches!(err, crate::StoreError::NotFound),
        "failed mid-reclaim cleanup must keep leases fenced until retry converges, got {err:?}"
    );
    drop(hook_guard);

    assert!(cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap());
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert!(!cluster
        .payload_reclaim_exists(&bucket, &key, committed.generation_id)
        .unwrap());
}

#[test]
fn reclaim_payload_metadata_delete_applies_to_object_pg_acting_set() {
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
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"reclaim payload");
    cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(
            crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id,
            )
            .unwrap(),
            "delete should publish reclaim metadata on node {node_id:?}"
        );
    }

    assert!(cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap());
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(
            !crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id,
            )
            .unwrap(),
            "reclaim command should delete metadata on node {node_id:?}"
        );
    }
    for shard_index in 0..committed.written.ec.k + committed.written.ec.m {
        assert!(!cluster
            .test_payload_shard_file_exists(
                committed.written.data_pg_id,
                committed.written.ec,
                &committed.segment_okh,
                committed.generation_id,
                shard_index,
            )
            .unwrap());
    }
}

#[test]
fn reclaim_payload_metadata_delete_retry_reuses_pending_partial_command() {
    let _serial = lock_metadata_command_apply_hook_test();
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
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();

    let committed = write_committed_direct_segment_for(&cluster, &bucket, &key, b"retry payload");
    cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    let hook_guard =
        cluster.test_install_before_metadata_command_apply_hook(Arc::new(|node_id, command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::DeleteObjectPayloadReclaim(_)
            ) && node_id == NodeId::new(2)
            {
                return Err(crate::StoreError::Io {
                    context: "injected reclaim metadata command apply failure",
                    source: std::io::Error::other(
                        "injected reclaim metadata command apply failure",
                    ),
                });
            }
            Ok(())
        }));
    let err = cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(crate::StoreError::Io {
            context: "injected reclaim metadata command apply failure",
            ..
        })
    ));
    let pending = pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket)
        .expect("partial reclaim metadata delete must keep pending command");
    assert!(matches!(
        pending.payload(),
        MetadataCommandPayload::DeleteObjectPayloadReclaim(delete)
            if delete.matches_request(&bucket, &key, committed.generation_id)
    ));
    let err = match cluster.acquire_object_payload_lease(&bucket, &key, committed.generation_id) {
        Ok(_) => panic!("new lease acquired while reclaim metadata delete was pending"),
        Err(error) => error,
    };
    assert!(
            matches!(err, crate::StoreError::NotFound),
            "pending reclaim metadata delete must fence new leases after payload deletion starts, got {err:?}"
        );
    for (node_id, expected_exists) in [
        (NodeId::new(0), false),
        (NodeId::new(1), false),
        (NodeId::new(2), true),
    ] {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::payload_reclaim_exists(
                &*pg,
                &bucket,
                &key,
                committed.generation_id,
            )
            .unwrap(),
            expected_exists,
            "partial apply state mismatch on node {node_id:?}"
        );
    }
    drop(hook_guard);

    assert!(cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, committed.generation_id)
        .unwrap());
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(!crate::PgMetadataStore::payload_reclaim_exists(
            &*pg,
            &bucket,
            &key,
            committed.generation_id,
        )
        .unwrap());
    }
    let lease = cluster
        .acquire_object_payload_lease(&bucket, &key, committed.generation_id)
        .expect("converged reclaim metadata delete must clear the in-memory lease fence");
    drop(lease);
}
