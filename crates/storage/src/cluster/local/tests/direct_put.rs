use super::*;

#[test]
fn direct_put_pending_install_race_reruns_precondition_action() {
    let _guard = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut first_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-pending-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster = crate::StorageCluster::from_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);

    let loser_payload = b"loser direct put";
    let loser_reservation_id =
        crate::SessionId::try_from("11111111111111111111111111111111".to_string()).unwrap();
    let loser_generation_id = first_cluster
        .reserve_put_object_generation(&bucket, &key, &loser_reservation_id)
        .unwrap();
    let loser_written = first_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            loser_generation_id,
            0,
            &[0x91; 16],
            loser_payload,
        )
        .unwrap();
    let loser_req = direct_put_commit_req(
        &first_cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: loser_reservation_id,
            generation_id: loser_generation_id,
            payload: loser_payload,
            segment_okh: [0x91; 16],
            written: &loser_written,
        },
    );

    let winner_payload = b"winner direct put";
    let winner_reservation_id =
        crate::SessionId::try_from("22222222222222222222222222222222".to_string()).unwrap();
    let winner_generation_id = second_cluster
        .reserve_put_object_generation(&bucket, &key, &winner_reservation_id)
        .unwrap();
    let winner_written = second_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            winner_generation_id,
            0,
            &[0x92; 16],
            winner_payload,
        )
        .unwrap();
    let winner_req = direct_put_commit_req(
        &first_cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: winner_reservation_id,
            generation_id: winner_generation_id,
            payload: winner_payload,
            segment_okh: [0x92; 16],
            written: &winner_written,
        },
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_cluster = Arc::clone(&second_cluster);
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_req = winner_req.clone();
    let hook_written_shards = winner_written.written_shards.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = first_cluster.test_install_before_metadata_command_pending_install_hook(
        Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(2);
            let shard_batch: Vec<(&ShardKey, WriteAck)> = hook_written_shards
                .iter()
                .map(|written| (&written.key, written.ack))
                .collect();
            hook_cluster
                .register_payload_shard_acks(hook_req.data_pg_id, &shard_batch)
                .unwrap();
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let command = hook_cluster
                .prepare_commit_direct_put_object_command(
                    pg_id,
                    &pg,
                    &hook_req,
                    crate::VersionId::Null,
                    hook_req.bucket_write_reservation.clone(),
                )
                .unwrap();
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
        }),
    );

    let calls_for_action = Arc::clone(&action_calls);
    let result = first_cluster
        .commit_direct_put_object_from_payload_shards(
            &loser_req,
            &loser_written.written_shards,
            move |snapshot| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                if snapshot.existing_etag.is_some() {
                    Err("object already exists")
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
    assert!(matches!(result, Err("object already exists")));
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        2,
        "direct PUT precondition must be rerun after slot contention changes object state"
    );
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().expect("winner object is live");
        assert_eq!(live.generation_id, winner_generation_id);
        assert_eq!(live.size, winner_payload.len() as u64);
    }
}

#[test]
fn direct_put_pending_install_race_keeps_bucket_write_proof_for_retry() {
    let _guard = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut first_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-proof-race-");
    let loser_key = key_for_object_pg(topology, &bucket, 2, "loser-");
    let winner_key = key_for_object_pg(topology, &bucket, 2, "winner-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster = crate::StorageCluster::from_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);

    let loser_payload = b"loser direct put with command proof";
    let loser_reservation_id =
        crate::SessionId::try_from("51515151515151515151515151515151".to_string()).unwrap();
    let loser_generation_id = first_cluster
        .reserve_put_object_generation(&bucket, &loser_key, &loser_reservation_id)
        .unwrap();
    let loser_written = first_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &loser_key,
            loser_generation_id,
            0,
            &[0xb1; 16],
            loser_payload,
        )
        .unwrap();
    let command_reservation = first_cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "direct-put-proof-race",
            Some(loser_key.as_str()),
        )
        .unwrap();
    let loser_req = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &loser_key,
            reservation_id: loser_reservation_id,
            generation_id: loser_generation_id,
            payload: loser_payload,
            segment_okh: [0xb1; 16],
            written: &loser_written,
        },
        crate::metadata_command::BucketWriteReservationProof::from(&command_reservation.record),
    );

    let winner_payload = b"winner unrelated direct put";
    let winner_reservation_id =
        crate::SessionId::try_from("52525252525252525252525252525252".to_string()).unwrap();
    let winner_generation_id = second_cluster
        .reserve_put_object_generation(&bucket, &winner_key, &winner_reservation_id)
        .unwrap();
    let winner_written = second_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &winner_key,
            winner_generation_id,
            0,
            &[0xb2; 16],
            winner_payload,
        )
        .unwrap();
    let winner_req = direct_put_commit_req(
        &first_cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &winner_key,
            reservation_id: winner_reservation_id,
            generation_id: winner_generation_id,
            payload: winner_payload,
            segment_okh: [0xb2; 16],
            written: &winner_written,
        },
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_cluster = Arc::clone(&second_cluster);
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_req = winner_req.clone();
    let hook_written_shards = winner_written.written_shards.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = first_cluster.test_install_before_metadata_command_pending_install_hook(
        Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(2);
            let shard_batch: Vec<(&ShardKey, WriteAck)> = hook_written_shards
                .iter()
                .map(|written| (&written.key, written.ack))
                .collect();
            hook_cluster
                .register_payload_shard_acks(hook_req.data_pg_id, &shard_batch)
                .unwrap();
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let command = hook_cluster
                .prepare_commit_direct_put_object_command(
                    pg_id,
                    &pg,
                    &hook_req,
                    crate::VersionId::Null,
                    hook_req.bucket_write_reservation.clone(),
                )
                .unwrap();
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
        }),
    );

    let calls_for_action = Arc::clone(&action_calls);
    first_cluster
        .commit_direct_put_object_from_payload_shards(
            &loser_req,
            &loser_written.written_shards,
            move |snapshot| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                assert!(snapshot.existing_etag.is_none());
                Ok::<(), ()>(())
            },
        )
        .unwrap()
        .unwrap();
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        2,
        "direct PUT must rerun after install contention while keeping its write proof"
    );
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());
    let object_pg = first_map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    let stored = crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &loser_key).unwrap();
    assert_eq!(stored.as_live().unwrap().generation_id, loser_generation_id);
    drop(object_pg);
    let bucket_pg = first_map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .is_empty()
    );
    drop(bucket_pg);
    assert_clean_metadata_command_stream(&first_map, &[2]);
}

#[test]
fn direct_put_committed_response_loss_retry_returns_existing_commit() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-response-loss-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("53535353535353535353535353535353".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put committed response loss retry";
    let segment_okh = [0xb3; 16];
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
    let hook_guard = cluster.test_install_after_direct_put_metadata_publish_hook(Arc::new(|| {
        Err(crate::ObjectPgActionError::InvalidRequest {
            reason: "injected direct PUT response loss".to_string(),
        })
    }));

    let first_err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            first_err,
            crate::ObjectPgActionError::InvalidRequest { ref reason }
                if reason == "injected direct PUT response loss"
        ),
        "expected injected post-commit direct PUT response-loss error, got {first_err:?}"
    );
    drop(hook_guard);

    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            |_| -> Result<(), ()> { panic!("committed direct PUT retry must not rerun action") },
        )
        .unwrap()
        .unwrap();

    assert_eq!(outcome.version_id, crate::VersionId::Null);
    assert_eq!(outcome.live_size, payload.len() as u64);
    assert!(pending_metadata_command_for_test(&map, PgId::new(2), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[2]);
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().expect("direct PUT object should be live");
        assert_eq!(live.generation_id, generation_id);
        assert_eq!(live.size, payload.len() as u64);
    }
}

#[test]
fn direct_put_overwrite_committed_response_loss_retry_preserves_reclaim_generation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-overwrite-response-loss-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let first_reservation_id =
        crate::SessionId::try_from("54545454545454545454545454545454".to_string()).unwrap();
    let first_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &first_reservation_id)
        .unwrap();
    let first_payload = b"original direct put object";
    let first_segment_okh = [0xc4; 16];
    let first_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            first_generation_id,
            0,
            &first_segment_okh,
            first_payload,
        )
        .unwrap();
    let first_commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: first_reservation_id,
            generation_id: first_generation_id,
            payload: first_payload,
            segment_okh: first_segment_okh,
            written: &first_written,
        },
    );
    let first_outcome = cluster
        .commit_direct_put_object_from_payload_shards(
            &first_commit_req,
            &first_written.written_shards,
            |_| Ok::<(), ()>(()),
        )
        .unwrap()
        .unwrap();
    assert_eq!(first_outcome.stale_generation_id, None);

    let overwrite_reservation_id =
        crate::SessionId::try_from("55555555555555555555555555555555".to_string()).unwrap();
    let overwrite_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &overwrite_reservation_id)
        .unwrap();
    let overwrite_payload = b"replacement direct put object after response loss";
    let overwrite_segment_okh = [0xc5; 16];
    let overwrite_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            overwrite_generation_id,
            0,
            &overwrite_segment_okh,
            overwrite_payload,
        )
        .unwrap();
    let overwrite_commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: overwrite_reservation_id,
            generation_id: overwrite_generation_id,
            payload: overwrite_payload,
            segment_okh: overwrite_segment_okh,
            written: &overwrite_written,
        },
    );

    let _serial = lock_metadata_command_apply_hook_test();
    let hook_guard = cluster.test_install_after_direct_put_metadata_publish_hook(Arc::new(|| {
        Err(crate::ObjectPgActionError::InvalidRequest {
            reason: "injected direct PUT overwrite response loss".to_string(),
        })
    }));

    let first_err = cluster
        .commit_direct_put_object_from_payload_shards(
            &overwrite_commit_req,
            &overwrite_written.written_shards,
            |_| Ok::<(), ()>(()),
        )
        .unwrap_err();
    assert!(
        matches!(
            first_err,
            crate::ObjectPgActionError::InvalidRequest { ref reason }
                if reason == "injected direct PUT overwrite response loss"
        ),
        "expected injected post-commit direct PUT response-loss error, got {first_err:?}"
    );
    drop(hook_guard);

    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(
            &overwrite_commit_req,
            &overwrite_written.written_shards,
            |_| -> Result<(), ()> {
                panic!("committed direct PUT overwrite retry must not rerun action")
            },
        )
        .unwrap()
        .unwrap();

    assert_eq!(outcome.version_id, crate::VersionId::Null);
    assert_eq!(outcome.live_size, overwrite_payload.len() as u64);
    assert_eq!(outcome.stale_generation_id, Some(first_generation_id));
    assert!(cluster
        .payload_reclaim_exists(&bucket, &key, first_generation_id)
        .unwrap());
    assert!(pending_metadata_command_for_test(&map, PgId::new(2), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[2]);
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored
            .as_live()
            .expect("direct PUT overwrite object should be live");
        assert_eq!(live.generation_id, overwrite_generation_id);
        assert_eq!(live.size, overwrite_payload.len() as u64);
    }
}

#[test]
fn copy_object_destination_committed_response_loss_retry_returns_existing_commit() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "copy-object-response-loss-");
    let source_key = key_for_object_pg(topology, &bucket, 2, "source-");
    let dst_key = key_for_object_pg(topology, &bucket, 2, "dest-");
    for pg_id in pg_ids {
        set_route_primary(&mut map, pg_id, NodeId::new(1));
    }

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let source_payload = b"copied object payload after response loss";
    let source_segment = write_committed_direct_segment_for_with_okh(
        &cluster,
        &bucket,
        &source_key,
        [0xcb; 16],
        source_payload,
    );

    let reservation_id =
        crate::SessionId::try_from("56565656565656565656565656565656".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &dst_key, &reservation_id)
        .unwrap();
    let dst_segment_okh = [0xcc; 16];
    let written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &dst_key,
            generation_id,
            0,
            &dst_segment_okh,
            &source_segment.payload,
        )
        .unwrap();
    let commit_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &dst_key,
            reservation_id,
            generation_id,
            payload: &source_segment.payload,
            segment_okh: dst_segment_okh,
            written: &written,
        },
    );

    let _serial = lock_metadata_command_apply_hook_test();
    let hook_guard = cluster.test_install_after_direct_put_metadata_publish_hook(Arc::new(|| {
        Err(crate::ObjectPgActionError::InvalidRequest {
            reason: "injected CopyObject destination response loss".to_string(),
        })
    }));

    let first_err = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            |snapshot| {
                assert!(
                    snapshot.existing_etag.is_none(),
                    "copy destination should not exist before first publish"
                );
                Ok::<(), ()>(())
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            first_err,
            crate::ObjectPgActionError::InvalidRequest { ref reason }
                if reason == "injected CopyObject destination response loss"
        ),
        "expected injected post-commit CopyObject response-loss error, got {first_err:?}"
    );
    drop(hook_guard);

    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            |_| -> Result<(), ()> {
                panic!("committed CopyObject destination retry must not rerun action")
            },
        )
        .unwrap()
        .unwrap();

    assert_eq!(outcome.version_id, crate::VersionId::Null);
    assert_eq!(outcome.live_size, source_segment.payload.len() as u64);
    assert_eq!(outcome.stale_generation_id, None);
    let dst_object_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .object_pg_for(&bucket, &dst_key);
    assert_direct_put_metadata_on_acting_nodes(
        &map,
        &node_ids,
        dst_object_pg,
        &commit_req,
        &outcome,
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(dst_object_pg), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[dst_object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
    let source_object_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .object_pg_for(&bucket, &source_key);
    for node_id in node_ids {
        let source_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(source_object_pg)
            .unwrap();
        let stored =
            crate::PgMetadataStore::get_object_meta(&*source_pg, &bucket, &source_key).unwrap();
        assert_eq!(
            stored.as_live().unwrap().generation_id,
            source_segment.generation_id,
            "committed CopyObject retry must preserve source object on node {node_id:?}"
        );
    }
}

#[test]
fn stale_direct_put_reservation_cannot_resurrect_deleted_null_version() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stale-direct-put-delete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let stale_payload = b"stale direct put";
    let stale_reservation_id =
        crate::SessionId::try_from("61616161616161616161616161616161".to_string()).unwrap();
    let stale_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &stale_reservation_id)
        .unwrap();
    let stale_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            stale_generation_id,
            0,
            &[0xa1; 16],
            stale_payload,
        )
        .unwrap();
    let stale_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: stale_reservation_id,
            generation_id: stale_generation_id,
            payload: stale_payload,
            segment_okh: [0xa1; 16],
            written: &stale_written,
        },
    );
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap();
    let object_pg_store = primary.storage_node().get_pg(2).unwrap();
    let stale_command = cluster
        .prepare_commit_direct_put_object_command(
            PgId::new(2),
            &object_pg_store,
            &stale_req,
            crate::VersionId::Null,
            stale_req.bucket_write_reservation.clone(),
        )
        .unwrap();
    drop(object_pg_store);

    for (label, payload, segment_byte) in [
        ("newer-a", b"newer direct put a".as_slice(), 0xa2),
        ("newer-b", b"newer direct put b".as_slice(), 0xa3),
    ] {
        let reservation_id =
            crate::SessionId::try_from(format!("{segment_byte:02x}").repeat(16)).unwrap();
        let generation_id = cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap();
        let written = cluster
            .write_direct_put_segment_payload_shards(
                &bucket,
                &key,
                generation_id,
                0,
                &[segment_byte; 16],
                payload,
            )
            .unwrap();
        let req = direct_put_commit_req(
            &cluster,
            DirectPutCommitReqFixture {
                bucket: &bucket,
                key: &key,
                reservation_id,
                generation_id,
                payload,
                segment_okh: [segment_byte; 16],
                written: &written,
            },
        );
        let outcome = cluster
            .commit_direct_put_object_from_payload_shards(&req, &written.written_shards, |_| {
                Ok::<_, ()>(())
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            outcome.live_size,
            payload.len() as u64,
            "{label} commit should be live before cleanup"
        );
    }

    cluster
        .delete_current_object_if(&bucket, &key, |_| Ok::<_, ()>(()))
        .unwrap()
        .unwrap();

    let stale_late_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(2),
            map.test_next_metadata_command_log_index(PgId::new(2)),
        ),
        stale_command.payload().clone(),
    );
    let stale_result = cluster.test_apply_metadata_command_to_acting_set_from_origin(
        primary.node_id(),
        &stale_late_command,
    );
    assert!(
        matches!(
            stale_result,
            Err(crate::BucketSnapshotLoadError::Metadata(
                crate::MetadataError::StaleObjectWriteCommand {
                    ref bucket,
                    ref key,
                    write_sequence: 1,
                    generation_id: Some(1),
                }
            )) if bucket == &stale_req.bucket && key == &stale_req.key
        ),
        "expected stale object write command after newer writes and delete, got {stale_result:?}"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(2), &bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "stale direct PUT must not resurrect object on node {node_id:?}"
        );
    }
}

#[test]
fn direct_put_pre_command_route_error_releases_bucket_write_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = local_map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut local_map, object_pg, NodeId::new(1));
    set_route_primary(&mut local_map, data_pg, NodeId::new(2));

    let mut map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let bucket_pg_id = cluster.bucket_metadata_pg_id(&bucket);
    let reservation_id =
        crate::SessionId::try_from("53535353535353535353535353535353".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put route failure";
    let segment_okh = [0xb3; 16];
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
    let command_reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "direct-put-route-failure",
            Some(key.as_str()),
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
        crate::metadata_command::BucketWriteReservationProof::from(&command_reservation.record),
    );
    drop(cluster);

    Arc::get_mut(&mut map)
        .unwrap()
        .pg_routes
        .get_mut(&PgId::new(object_pg))
        .unwrap()
        .state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::PgNotActive {
            pg_id,
            state: PgState::Peering,
            ..
        }) if pg_id == object_pg
    ));

    let bucket_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg_id))
        .unwrap()
        .storage_node()
        .get_pg(bucket_pg_id)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .is_empty(),
        "pre-command storage errors must release caller-owned bucket write proof"
    );
}

#[test]
fn non_current_epoch_direct_put_commit_fails_closed_and_cleans_unowned_state() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let current_epoch = ClusterEpoch::INITIAL;
    let stale_epoch = ClusterEpoch::new(current_epoch.get() + 1).unwrap();
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let (bucket, key, object_pg, data_pg) = {
        let topology = local_map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut local_map, object_pg, NodeId::new(1));
    set_route_primary(&mut local_map, data_pg, NodeId::new(2));

    let map = Arc::new(local_map);
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&current_cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("54545454545454545454545454545454".to_string()).unwrap();
    let generation_id = current_cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let before_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let payload = b"stale epoch direct put";
    let segment_okh = [0xb4; 16];
    let written = current_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    let bucket_write_reservation = current_cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "stale-epoch-direct-put",
            Some(key.as_str()),
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
        crate::metadata_command::BucketWriteReservationProof::from(
            &bucket_write_reservation.record,
        ),
    );
    let stale_cluster =
        crate::StorageCluster::test_from_local_map_with_epoch(Arc::clone(&map), stale_epoch)
            .unwrap();

    let err = stale_cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == stale_epoch
                && observed_current_epoch == current_epoch
        ),
        "stale direct PUT commit should fail closed at the metadata-primary boundary, got {err:?}"
    );

    let after_object_pg_proof = current_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    assert_eq!(
        after_object_pg_proof, before_object_pg_proof,
        "stale direct PUT commit must not append an object-PG command"
    );
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "stale direct PUT must not publish object metadata on node {node_id:?}"
        );
    }
    assert_bucket_write_reservations_released(&map, &bucket);
    for shard_index in 0..written.ec.k + written.ec.m {
        assert!(!current_cluster
            .test_payload_shard_file_exists(
                written.data_pg_id,
                written.ec,
                &segment_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
    }
}

#[test]
fn control_plane_peering_direct_put_old_primary_fails_closed_and_cleans_unowned_state() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
        crate::control_plane::FileControlPlaneStore::new(tmp.path().join("control-plane.state")),
    )
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            node_ids
                .iter()
                .map(|node_id| {
                    (
                        *node_id,
                        tmp.path()
                            .join("sockets")
                            .join(format!("node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();
    let configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("storage")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, key, object_pg, _data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_cluster = crate::StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    create_test_bucket(&source_cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("67676767676767676767676767676767".to_string()).unwrap();
    let generation_id = source_cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let before_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &key)
        .unwrap();
    let payload = b"control-plane peering stale direct put";
    let segment_okh = [0xc7; 16];
    let written = source_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            generation_id,
            0,
            &segment_okh,
            payload,
        )
        .unwrap();
    source_cluster
        .test_register_payload_shard_acks(written.data_pg_id, &written.written_shards)
        .unwrap();
    {
        let source_data_pg_primary = source_map
            .node(
                source_map
                    .pg_route(PgId::new(written.data_pg_id))
                    .unwrap()
                    .primary_node_id(),
            )
            .unwrap()
            .storage_node()
            .get_pg(written.data_pg_id)
            .unwrap();
        for written_shard in &written.written_shards {
            source_data_pg_primary
                .validate_written_shard_ack(&written_shard.key, written_shard.ack)
                .unwrap();
        }
    }
    let bucket_write_reservation = source_cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "control-plane-peering-stale-direct-put",
            Some(key.as_str()),
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
        crate::metadata_command::BucketWriteReservationProof::from(
            &bucket_write_reservation.record,
        ),
    );
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(PgId::new(object_pg), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes);
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map.pg_route(PgId::new(object_pg)).unwrap().state(),
        PgState::Peering,
        "control-plane acting-set change should put the object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the current acting set"
    );
    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();

    let err = old_primary_cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary direct PUT commit should fail closed after control-plane Peering transition, got {err:?}"
    );

    for node_id in node_ids {
        let pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        assert_eq!(
            proof, before_object_pg_proof,
            "old-primary direct PUT must not append an object-PG command on node {node_id:?}"
        );
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "old-primary direct PUT must not publish object metadata on node {node_id:?}"
        );
        assert!(
            pg.pending_metadata_command_envelope(node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary direct PUT must not leave a source-epoch pending command on node {node_id:?}"
        );
        assert!(
            pg.pending_metadata_command_envelope(node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary direct PUT must not leave a current-epoch pending command on node {node_id:?}"
        );
    }
    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for node_id in node_ids {
        let pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(bucket_pg)
            .unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*pg, &bucket)
                .unwrap()
                .is_empty(),
            "old-primary direct PUT must release bucket write reservations on node {node_id:?}"
        );
    }
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&current_map)).unwrap();
    for shard_index in 0..written.ec.k + written.ec.m {
        assert!(!current_cluster
            .test_payload_shard_file_exists(
                written.data_pg_id,
                written.ec,
                &segment_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
    }
    let source_data_pg_route = current_map
        .reconstructed_pg_route_at_epoch(PgId::new(written.data_pg_id), source_epoch)
        .unwrap();
    let source_data_pg_primary = current_map
        .node(source_data_pg_route.primary_node_id())
        .unwrap()
        .storage_node()
        .get_pg(written.data_pg_id)
        .unwrap();
    for written_shard in &written.written_shards {
        assert!(
            matches!(
                source_data_pg_primary
                    .validate_written_shard_ack(&written_shard.key, written_shard.ack),
                Err(StoreError::NotFound)
            ),
            "old-primary direct PUT must delete retained data-PG ack row for shard {}",
            written_shard.key
        );
    }
}

#[test]
fn control_plane_peering_copy_object_destination_old_primary_fails_closed_and_cleans_staging() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut authority = crate::control_plane::SingleAuthorityControlPlane::open(
        crate::control_plane::FileControlPlaneStore::new(tmp.path().join("control-plane.state")),
    )
    .unwrap();
    authority
        .bootstrap_initial_cluster_map(
            node_ids
                .iter()
                .map(|node_id| {
                    (
                        *node_id,
                        tmp.path()
                            .join("sockets")
                            .join(format!("copy-node-{}.sock", node_id.as_u32()))
                            .to_string_lossy()
                            .into_owned(),
                    )
                })
                .collect(),
            pg_ids.iter().copied().map(PgId::new).collect(),
        )
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    let source_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), source_epoch)
                .unwrap();
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                PgState::Active,
            )
        })
        .collect::<Vec<_>>();
    let configs = node_ids
        .iter()
        .map(|node_id| {
            LocalNodeStoreConfig::new(
                *node_id,
                tmp.path()
                    .join("copy-storage")
                    .join(format!("node-{:04}", node_id.as_u32())),
            )
        })
        .collect::<Vec<_>>();
    let source_map = Arc::new(
        LocalClusterMap::open_frontend_with_configs_and_pg_routes(
            NodeId::new(0),
            configs.clone(),
            &pg_ids,
            ec_shape,
            source_epoch,
            source_routes.iter().map(LocalPgRoute::from),
        )
        .unwrap(),
    );
    let (bucket, dst_key, dst_object_pg, _dst_data_pg) = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let source_key = {
        let topology = source_map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let source_pg = pg_ids
            .iter()
            .copied()
            .find(|pg_id| *pg_id != dst_object_pg)
            .unwrap();
        key_for_object_pg(topology, &bucket, source_pg, "copy-source-")
    };
    let source_cluster = crate::StorageCluster::from_local_map(Arc::clone(&source_map)).unwrap();
    create_test_bucket(&source_cluster, &bucket);
    let source_payload = b"control-plane peering stale copy source payload";
    let source_segment = write_committed_direct_segment_for_with_okh(
        &source_cluster,
        &bucket,
        &source_key,
        [0xc9; 16],
        source_payload,
    );

    let reservation_id =
        crate::SessionId::try_from("69696969696969696969696969696969".to_string()).unwrap();
    let generation_id = source_cluster
        .reserve_put_object_generation(&bucket, &dst_key, &reservation_id)
        .unwrap();
    let before_dst_object_pg_proof = source_cluster
        .test_object_pg_metadata_proof(&bucket, &dst_key)
        .unwrap();
    let dst_segment_okh = [0xca; 16];
    let written = source_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &dst_key,
            generation_id,
            0,
            &dst_segment_okh,
            &source_segment.payload,
        )
        .unwrap();
    source_cluster
        .test_register_payload_shard_acks(written.data_pg_id, &written.written_shards)
        .unwrap();
    let bucket_write_reservation = source_cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "control-plane-peering-stale-copy-object",
            Some(dst_key.as_str()),
        )
        .unwrap();
    let commit_req = direct_put_commit_req_with_bucket_write_proof(
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &dst_key,
            reservation_id,
            generation_id,
            payload: &source_segment.payload,
            segment_okh: dst_segment_okh,
            written: &written,
        },
        crate::metadata_command::BucketWriteReservationProof::from(
            &bucket_write_reservation.record,
        ),
    );
    drop(source_cluster);
    drop(source_map);

    authority
        .set_pg_acting_set(
            PgId::new(dst_object_pg),
            vec![NodeId::new(1), NodeId::new(2)],
        )
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    let current_routes = pg_ids
        .iter()
        .map(|pg_id| {
            let route = authority
                .snapshot()
                .reconstructed_pg_route_at_epoch(PgId::new(*pg_id), current_epoch)
                .unwrap();
            let state = if *pg_id == dst_object_pg {
                PgState::Peering
            } else {
                PgState::Active
            };
            crate::control_plane::PgRouteSnapshot::reconstructed(
                route.cluster_epoch(),
                route.pg_id(),
                route.primary_node_id(),
                route.acting_set().to_vec(),
                state,
            )
        })
        .collect::<Vec<_>>();
    let mut current_map = LocalClusterMap::open_frontend_with_configs_and_pg_routes(
        NodeId::new(0),
        configs,
        &pg_ids,
        ec_shape,
        current_epoch,
        current_routes.iter().map(LocalPgRoute::from),
    )
    .unwrap();
    current_map.test_install_historical_pg_routes(source_routes);
    let current_map = Arc::new(current_map);
    assert_eq!(
        current_map
            .pg_route(PgId::new(dst_object_pg))
            .unwrap()
            .state(),
        PgState::Peering,
        "control-plane acting-set change should put the destination object PG into Peering"
    );
    assert!(
        !current_map
            .pg_route(PgId::new(dst_object_pg))
            .unwrap()
            .acting_set()
            .contains(&NodeId::new(0)),
        "the old source primary should no longer be in the destination acting set"
    );
    let old_primary_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
        Arc::clone(&current_map),
        source_epoch,
    )
    .unwrap();

    let err = old_primary_cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::StaleMetadataOperation {
                pg_id,
                operation_epoch,
                current_epoch: observed_current_epoch,
            }) if pg_id == dst_object_pg
                && operation_epoch == source_epoch
                && observed_current_epoch == current_epoch
        ),
        "old-primary CopyObject destination commit should fail closed after control-plane Peering transition, got {err:?}"
    );

    for node_id in node_ids {
        let dst_pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(dst_object_pg)
            .unwrap();
        let state = dst_pg.metadata_command_replica_state().unwrap();
        let proof = crate::control_plane::PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        assert_eq!(
            proof, before_dst_object_pg_proof,
            "old-primary CopyObject destination must not append an object-PG command on node {node_id:?}"
        );
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_meta(&*dst_pg, &bucket, &dst_key),
                Err(crate::MetadataError::ObjectNotFound)
            ),
            "old-primary CopyObject destination must not publish destination metadata on node {node_id:?}"
        );
        assert!(
            dst_pg
                .pending_metadata_command_envelope(node_id.as_u32(), source_epoch)
                .unwrap()
                .is_none(),
            "old-primary CopyObject destination must not leave a source-epoch pending command on node {node_id:?}"
        );
        assert!(
            dst_pg
                .pending_metadata_command_envelope(node_id.as_u32(), current_epoch)
                .unwrap()
                .is_none(),
            "old-primary CopyObject destination must not leave a current-epoch pending command on node {node_id:?}"
        );
    }
    let source_object_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .object_pg_for(&bucket, &source_key);
    for node_id in node_ids {
        let source_pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(source_object_pg)
            .unwrap();
        let stored =
            crate::PgMetadataStore::get_object_meta(&*source_pg, &bucket, &source_key).unwrap();
        let live = stored.as_live().unwrap();
        assert_eq!(
            live.generation_id, source_segment.generation_id,
            "failed CopyObject destination commit must preserve source object on node {node_id:?}"
        );
    }
    let bucket_pg = current_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .bucket_pg_for(&bucket);
    for node_id in node_ids {
        let pg = current_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(bucket_pg)
            .unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*pg, &bucket)
                .unwrap()
                .is_empty(),
            "old-primary CopyObject destination must release bucket write reservations on node {node_id:?}"
        );
    }
    let current_cluster = crate::StorageCluster::from_local_map(Arc::clone(&current_map)).unwrap();
    for shard_index in 0..written.ec.k + written.ec.m {
        assert!(!current_cluster
            .test_payload_shard_file_exists(
                written.data_pg_id,
                written.ec,
                &dst_segment_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
    }
    let source_data_pg_route = current_map
        .reconstructed_pg_route_at_epoch(PgId::new(written.data_pg_id), source_epoch)
        .unwrap();
    let source_data_pg_primary = current_map
        .node(source_data_pg_route.primary_node_id())
        .unwrap()
        .storage_node()
        .get_pg(written.data_pg_id)
        .unwrap();
    for written_shard in &written.written_shards {
        assert!(
            matches!(
                source_data_pg_primary
                    .validate_written_shard_ack(&written_shard.key, written_shard.ack),
                Err(StoreError::NotFound)
            ),
            "old-primary CopyObject destination must delete retained data-PG ack row for shard {}",
            written_shard.key
        );
    }
}

#[test]
fn direct_put_publish_validation_fails_closed_when_acknowledged_shard_file_is_missing() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-publish-validation-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut local_map, 1, NodeId::new(1));
    set_route_primary(&mut local_map, 2, NodeId::new(1));
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("64646464646464646464646464646464".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put publish validation";
    let segment_okh = [0xd1; 16];
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
    let data_pg_id = DataPgId::new_for_test(PgId::new(written.data_pg_id));
    let placement_key =
        super::super::super::segment_payload_placement_key(&segment_okh, generation_id);
    let locations = cluster
        .place_payload_shards(data_pg_id, written.ec, &placement_key)
        .unwrap();
    let missing_shard = written.written_shards[0].key.clone();
    let missing_location = locations[usize::from(missing_shard.shard_index().get())];
    let hook_map = Arc::clone(&map);
    let hook_missing_shard = missing_shard.clone();
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = cluster.test_install_before_direct_put_command_id_hook(Arc::new(move || {
        if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
            return;
        }
        hook_map
            .node(missing_location.node_id())
            .unwrap()
            .storage_node()
            .delete_shard_file(missing_location.data_pg_id().get(), &hook_missing_shard)
            .unwrap();
    }));

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
    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::ShardStore {
                ref source,
                ..
            }) if matches!(**source, StoreError::NotFound)
        ),
        "missing acknowledged shard file should fail closed before metadata publish, got {err:?}"
    );
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_bucket_write_reservations_released(&map, &bucket);
    assert_clean_metadata_command_stream(&map, &[2]);
    let object_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key),
        Err(crate::MetadataError::ObjectNotFound)
    ));
}

#[test]
fn direct_put_publish_validation_rejects_truncated_shard_batch() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-truncated-shards-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut local_map, 1, NodeId::new(1));
    set_route_primary(&mut local_map, 2, NodeId::new(1));
    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("65656565656565656565656565656565".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put truncated shard batch";
    let segment_okh = [0xd3; 16];
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
    let truncated = written.written_shards[..written.written_shards.len() - 1].to_vec();
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
    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &truncated, |_| Ok::<(), ()>(()))
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::PayloadShardSetMismatch { .. })
        ),
        "truncated shard batch should fail closed before metadata publish, got {err:?}"
    );
    assert_bucket_write_reservations_released(&map, &bucket);
    assert_clean_metadata_command_stream(&map, &[2]);
    let object_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_object_meta(&*object_pg, &bucket, &key),
        Err(crate::MetadataError::ObjectNotFound)
    ));
}

#[test]
fn direct_put_command_id_race_drains_winner_and_reruns_precondition_action() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut first_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-command-id-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster = crate::StorageCluster::from_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);

    let loser_payload = b"loser direct put command id";
    let loser_reservation_id =
        crate::SessionId::try_from("31313131313131313131313131313131".to_string()).unwrap();
    let loser_generation_id = first_cluster
        .reserve_put_object_generation(&bucket, &key, &loser_reservation_id)
        .unwrap();
    let loser_written = first_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            loser_generation_id,
            0,
            &[0xa1; 16],
            loser_payload,
        )
        .unwrap();
    let loser_req = direct_put_commit_req(
        &first_cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: loser_reservation_id,
            generation_id: loser_generation_id,
            payload: loser_payload,
            segment_okh: [0xa1; 16],
            written: &loser_written,
        },
    );

    let winner_payload = b"winner direct put command id";
    let winner_reservation_id =
        crate::SessionId::try_from("32323232323232323232323232323232".to_string()).unwrap();
    let winner_generation_id = second_cluster
        .reserve_put_object_generation(&bucket, &key, &winner_reservation_id)
        .unwrap();
    let winner_written = second_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            winner_generation_id,
            0,
            &[0xa2; 16],
            winner_payload,
        )
        .unwrap();
    let winner_req = direct_put_commit_req(
        &first_cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: winner_reservation_id,
            generation_id: winner_generation_id,
            payload: winner_payload,
            segment_okh: [0xa2; 16],
            written: &winner_written,
        },
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_cluster = Arc::clone(&second_cluster);
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_req = winner_req.clone();
    let hook_written_shards = winner_written.written_shards.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard =
        first_cluster.test_install_before_direct_put_command_id_hook(Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(2);
            let shard_batch: Vec<(&ShardKey, WriteAck)> = hook_written_shards
                .iter()
                .map(|written| (&written.key, written.ack))
                .collect();
            hook_cluster
                .register_payload_shard_acks(hook_req.data_pg_id, &shard_batch)
                .unwrap();
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let command = hook_cluster
                .prepare_commit_direct_put_object_command(
                    pg_id,
                    &pg,
                    &hook_req,
                    crate::VersionId::Null,
                    hook_req.bucket_write_reservation.clone(),
                )
                .unwrap();
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
        }));

    let calls_for_action = Arc::clone(&action_calls);
    let result = first_cluster
        .commit_direct_put_object_from_payload_shards(
            &loser_req,
            &loser_written.written_shards,
            move |snapshot| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                if snapshot.existing_etag.is_some() {
                    Err("object already exists")
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
    assert!(matches!(result, Err("object already exists")));
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        2,
        "direct PUT precondition must be rerun after command-id contention changes object state"
    );
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().expect("winner object is live");
        assert_eq!(live.generation_id, winner_generation_id);
        assert_eq!(live.size, winner_payload.len() as u64);
    }
}

#[test]
fn direct_put_log_conflict_pending_visibility_error_cleans_new_payload() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut local_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = local_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-pending-read-error-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    let wrong_scope_bucket = bucket_for_pg(topology, 1, "wrong-pending-scope-");
    set_route_primary(&mut local_map, 1, NodeId::new(1));
    set_route_primary(&mut local_map, 2, NodeId::new(1));

    let map = Arc::new(local_map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation_id =
        crate::SessionId::try_from("36363636363636363636363636363636".to_string()).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put pending visibility error";
    let segment_okh = [0xc6; 16];
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

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_wrong_scope_bucket = wrong_scope_bucket.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard = cluster.test_install_before_direct_put_command_id_hook(Arc::new(move || {
        if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
            return;
        }
        let pg_id = PgId::new(2);
        let command = create_bucket_metadata_command(pg_id, 2, hook_wrong_scope_bucket.clone());
        force_insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
    }));

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandPendingConflict { .. })
        ),
        "expected malformed pending slot visibility error, got {err:?}"
    );

    assert_bucket_write_reservations_released(&map, &bucket);
    // The unreadable pending slot still blocks command-log-preserving
    // generation-reservation release. This regression pins the cleanup that
    // must not be bypassed by the pending-visibility error: the caller-owned
    // bucket write proof and unowned direct PUT payload shards.
    for shard_index in 0..written.ec.k + written.ec.m {
        assert!(!cluster
            .test_payload_shard_file_exists(
                written.data_pg_id,
                written.ec,
                &segment_okh,
                generation_id,
                shard_index,
            )
            .unwrap());
    }
}

#[test]
fn direct_put_stale_commit_snapshot_reruns_precondition_action() {
    const STALE_SNAPSHOTS: usize = 17;

    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut first_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "direct-put-stale-snapshot-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);

    let loser_payload = b"loser direct put stale snapshot";
    let loser_reservation_id =
        crate::SessionId::try_from("41414141414141414141414141414141".to_string()).unwrap();
    let loser_generation_id = first_cluster
        .reserve_put_object_generation(&bucket, &key, &loser_reservation_id)
        .unwrap();
    let loser_written = first_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            loser_generation_id,
            0,
            &[0xb1; 16],
            loser_payload,
        )
        .unwrap();
    let loser_req = direct_put_commit_req(
        &first_cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: loser_reservation_id,
            generation_id: loser_generation_id,
            payload: loser_payload,
            segment_okh: [0xb1; 16],
            written: &loser_written,
        },
    );

    let hook_calls = Arc::new(AtomicUsize::new(0));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let action_saw_existing = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&first_map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_calls_for_closure = Arc::clone(&hook_calls);
    let _hook_guard =
        first_cluster.test_install_before_direct_put_command_id_hook(Arc::new(move || {
            let call = hook_calls_for_closure.fetch_add(1, Ordering::SeqCst);
            if call >= STALE_SNAPSHOTS {
                return;
            }
            let pg_id = PgId::new(2);
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let generation_id = crate::GenerationId::new(10_000 + call as u64).unwrap();
            let size = 100 + call as u64;
            crate::PgMetadataStore::put_object_meta(
                &*pg,
                &crate::PutObjectReq::Live(crate::PutLiveObjectReq {
                    bucket: hook_bucket.clone(),
                    key: hook_key.clone(),
                    version_id: crate::VersionId::Null,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    generation_id,
                    size,
                    etag: crate::ObjectEtag::single_part(10_000 + call as u64),
                    ec: EcShape { k: 1, m: 0 },
                    layout: crate::ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: Some(crate::SerializedMetadataBlob::default()),
                    system_metadata_blob: Some(crate::SerializedSystemMetadataBlob::default()),
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                }),
            )
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
        }));

    let calls_for_action = Arc::clone(&action_calls);
    let saw_existing_for_action = Arc::clone(&action_saw_existing);
    let hook_calls_for_action = Arc::clone(&hook_calls);
    let result = first_cluster
        .commit_direct_put_object_from_payload_shards(
            &loser_req,
            &loser_written.written_shards,
            move |snapshot| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                if snapshot.existing_etag.is_some() {
                    saw_existing_for_action.store(true, Ordering::SeqCst);
                }
                if hook_calls_for_action.load(Ordering::SeqCst) >= STALE_SNAPSHOTS {
                    Err("object changed repeatedly")
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
    assert!(matches!(result, Err("object changed repeatedly")));
    assert_eq!(hook_calls.load(Ordering::SeqCst), STALE_SNAPSHOTS);
    assert_eq!(action_calls.load(Ordering::SeqCst), STALE_SNAPSHOTS + 1);
    assert!(action_saw_existing.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());

    assert_bucket_write_reservations_released(&first_map, &bucket);
}
