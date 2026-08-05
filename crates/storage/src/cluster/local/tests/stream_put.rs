use super::*;
use crate::test_support::{
    StorageClusterLifecycleTestSupport as _, StorageClusterTopologyTestSupport as _,
};

#[test]
fn stream_reservation_existence_observation_binds_bucket_key_and_session() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], EcShape { k: 2, m: 1 })
            .unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(map).unwrap();
    let bucket = crate::tests::bucket_name("stream-reservation-observation");
    let first_key = crate::tests::object_key("first");
    let second_key = crate::tests::object_key("second");
    let first_session = crate::tests::stream_session_id("reservation-one");
    let second_session = crate::tests::stream_session_id("reservation-two");
    let first_object_pg = cluster.test_object_pg_id_for(&bucket, &first_key);
    let crossed_bucket = (0..10_000)
        .map(|suffix| crate::tests::bucket_name(format!("reservation-crossed-{suffix:04}")))
        .find(|candidate| cluster.test_object_pg_id_for(candidate, &first_key) == first_object_pg)
        .expect("four object PGs must yield a distinct same-PG bucket candidate");
    assert_ne!(crossed_bucket, bucket);
    assert_eq!(
        cluster.test_object_pg_id_for(&crossed_bucket, &first_key),
        first_object_pg,
        "crossed-bucket canary must isolate bucket binding from object-PG placement"
    );
    create_test_bucket(&cluster, &bucket);
    create_test_bucket(&cluster, &crossed_bucket);
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &first_key,
            &first_session,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &second_key,
            &second_session,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    assert!(cluster
        .test_stream_upload_reservation_exists(&bucket, &first_key, &first_session)
        .unwrap());
    assert!(cluster
        .test_stream_upload_reservation_exists(&bucket, &second_key, &second_session)
        .unwrap());
    assert!(!cluster
        .test_stream_upload_reservation_exists(&bucket, &first_key, &second_session)
        .unwrap());
    assert!(!cluster
        .test_stream_upload_reservation_exists(&bucket, &second_key, &first_session)
        .unwrap());
    assert!(!cluster
        .test_stream_upload_reservation_exists(&crossed_bucket, &first_key, &first_session)
        .unwrap());
}

#[test]
fn topology_test_support_binds_stream_placement_to_exact_put_session() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, EcShape { k: 2, m: 1 }).unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(map).unwrap();
    let bucket = crate::tests::bucket_name("opaque-stream-topology-bucket");
    create_test_bucket(&cluster, &bucket);

    let mut selected = None;
    for suffix in 0..10_000 {
        let key = crate::tests::object_key(format!("opaque-stream-{suffix:04}"));
        let session_id = crate::tests::stream_session_id(format!("opaque-{suffix:04}"));
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
        let crosses = cluster
            .test_stream_put_session_crosses_metadata_and_data_pgs(&bucket, &key, &session_id)
            .unwrap();
        if selected.as_ref().is_some_and(
            |(_, _, selected_crosses): &(crate::ObjectKey, crate::SessionId, bool)| {
                *selected_crosses != crosses
            },
        ) {
            let (other_key, other_session_id, _) = selected.unwrap();
            let error = cluster
                .test_stream_put_session_crosses_metadata_and_data_pgs(
                    &bucket,
                    &other_key,
                    &session_id,
                )
                .unwrap_err();
            assert!(matches!(
                error,
                crate::ObjectPgActionError::InvalidRequest { ref reason }
                    if reason == "topology observation requires the exact PutObject stream session"
            ));
            let error = cluster
                .test_stream_put_session_crosses_metadata_and_data_pgs(
                    &bucket,
                    &key,
                    &other_session_id,
                )
                .unwrap_err();
            assert!(matches!(
                error,
                crate::ObjectPgActionError::InvalidRequest { ref reason }
                    if reason == "topology observation requires the exact PutObject stream session"
            ));
            return;
        }
        selected.get_or_insert((key, session_id, crosses));
    }
    panic!("failed to find both same-PG and cross-PG PutObject stream sessions");
}

#[test]
fn stream_put_finalize_pending_install_race_reruns_precondition_action() {
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
    let bucket = bucket_for_pg(topology, 1, "stream-put-pending-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket_with_versioning(
        &first_cluster,
        &bucket,
        crate::BucketVersioningState::Enabled,
    );

    let session_id =
        crate::SessionId::try_from("33333333333333333333333333333333".to_string()).unwrap();
    first_cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let loser_payload = b"loser stream put";
    let loser_crc64 = checksum::crc64::checksum(loser_payload);
    let (_target, segment) = first_cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: loser_payload.len() as u64,
                segment_crc64: loser_crc64,
                payload_crc64: loser_crc64,
                segment_okh: [0x93; 16],
            },
        )
        .unwrap();
    let written_shards = first_cluster
        .write_stream_segment_payload_shards(&segment, loser_payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    first_cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let winner_payload = b"winner before stream finalize";
    let winner_reservation_id =
        crate::SessionId::try_from("44444444444444444444444444444444".to_string()).unwrap();
    let winner_generation_id = second_cluster
        .reserve_put_object_generation(&bucket, &key, &winner_reservation_id)
        .unwrap();
    let winner_written = second_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            winner_generation_id,
            0,
            &[0x94; 16],
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
            segment_okh: [0x94; 16],
            written: &winner_written,
        },
    );
    let mut winner_req = winner_req;
    winner_req.versioning = crate::BucketVersioningState::Enabled;

    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_cluster = Arc::clone(&second_cluster);
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
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
            let version_id = hook_cluster
                .reserve_next_object_version(pg_id, &hook_bucket, &hook_key)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let command = hook_cluster
                .prepare_commit_direct_put_object_command(
                    pg_id,
                    &pg,
                    &hook_req,
                    version_id,
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
        .finalize_put_object_stream(
            &bucket,
            &key,
            &session_id,
            loser_payload.len() as u64,
            move |snapshot| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                if snapshot.existing_etag.is_some() {
                    Err("object already exists")
                } else {
                    Ok(crate::PreparedStreamPutCommit {
                        value: (),
                        versioning: crate::BucketVersioningState::Enabled,
                        owner: crate::OwnerIdentity::from_principal("owner"),
                        acl_grants: crate::AclGrants::default(),
                        public_read: false,
                        etag_crc64: loser_crc64,
                        tags: None,
                        metadata_blob: crate::SerializedMetadataBlob::default(),
                        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                        object_lock: crate::ObjectLockState::default(),
                        encryption: crate::ObjectEncryption::None,
                    })
                }
            },
        )
        .unwrap();
    assert!(matches!(result, Err("object already exists")));
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
            action_calls.load(Ordering::SeqCst),
            2,
            "stream PUT finalization precondition must be rerun after slot contention changes object state"
        );
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());
    let losing_reserved_version = crate::VersionId::from_u64(1);
    let winning_version = crate::VersionId::from_u64(2);
    assert_object_version_counter_on_acting_nodes(&first_map, &node_ids, 2, &bucket, &key, 3);

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().expect("winner object is live");
        assert_eq!(live.version_id, winning_version);
        assert_eq!(live.generation_id, winner_generation_id);
        assert_eq!(live.size, winner_payload.len() as u64);
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(
                &*pg,
                &bucket,
                &key,
                losing_reserved_version,
            ),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
}

#[test]
fn stream_put_create_pending_install_race_reruns_authorization_action() {
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
    let bucket = bucket_for_pg(topology, 1, "stream-create-pending-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);

    let winner_payload = b"winner before stream create";
    let winner_reservation_id =
        crate::SessionId::try_from("55555555555555555555555555555555".to_string()).unwrap();
    let winner_generation_id = second_cluster
        .reserve_put_object_generation(&bucket, &key, &winner_reservation_id)
        .unwrap();
    let winner_written = second_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            winner_generation_id,
            0,
            &[0x95; 16],
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
            segment_okh: [0x95; 16],
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

    let session_id =
        crate::SessionId::try_from("66666666666666666666666666666666".to_string()).unwrap();
    let calls_for_action = Arc::clone(&action_calls);
    let result = first_cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_, existing_object| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                if existing_object.is_some() {
                    Err("object already exists")
                } else {
                    Ok((
                        (),
                        crate::CreateStreamUploadReq {
                            session_id: session_id.clone(),
                            bucket: bucket.clone(),
                            key: key.clone(),
                            target: crate::StreamUploadTarget::PutObject,
                            encryption: crate::ObjectEncryption::None,
                        },
                    ))
                }
            },
        )
        .unwrap();
    assert!(matches!(result, Err("object already exists")));
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        2,
        "stream PUT create authorization must be rerun after slot contention changes object state"
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
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(crate::PgMetadataStore::get_object_generation_reservation(
            &*pg,
            &bucket,
            &key,
            &session_id,
        )
        .is_err());
    }
}

#[test]
fn stream_put_create_command_id_race_releases_reservation_and_retries() {
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
    let bucket = bucket_for_pg(topology, 1, "stream-create-id-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);

    let winner_payload = b"winner before stream create command id";
    let winner_reservation_id =
        crate::SessionId::try_from("56565656565656565656565656565656".to_string()).unwrap();
    let winner_generation_id = second_cluster
        .reserve_put_object_generation(&bucket, &key, &winner_reservation_id)
        .unwrap();
    let winner_written = second_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            winner_generation_id,
            0,
            &[0x96; 16],
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
            segment_okh: [0x96; 16],
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
        first_cluster.test_install_before_stream_put_create_command_id_hook(Arc::new(move || {
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

    let session_id =
        crate::SessionId::try_from("57575757575757575757575757575757".to_string()).unwrap();
    let calls_for_action = Arc::clone(&action_calls);
    let result = first_cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_, existing_object| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                if existing_object.is_some() {
                    Err("object already exists")
                } else {
                    Ok((
                        (),
                        crate::CreateStreamUploadReq {
                            session_id: session_id.clone(),
                            bucket: bucket.clone(),
                            key: key.clone(),
                            target: crate::StreamUploadTarget::PutObject,
                            encryption: crate::ObjectEncryption::None,
                        },
                    ))
                }
            },
        )
        .unwrap();
    assert!(matches!(result, Err("object already exists")));
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
            action_calls.load(Ordering::SeqCst),
            2,
            "stream PUT create must release its reservation and rerun after post-reservation command-id contention"
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
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &session_id,
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn stream_put_finalize_command_id_race_drains_winner_and_retries() {
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
    let bucket = bucket_for_pg(topology, 1, "stream-finalize-id-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&second_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);

    let session_id =
        crate::SessionId::try_from("5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a".to_string()).unwrap();
    first_cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let stream_generation_id = {
        let primary = first_map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
            .unwrap();
        let pg = primary.storage_node().get_pg(2).unwrap();
        crate::PgMetadataStore::get_object_generation_reservation(&*pg, &bucket, &key, &session_id)
            .unwrap()
    };
    let stream_payload = b"stream finalize retries after command-id contention";
    let stream_crc64 = checksum::crc64::checksum(stream_payload);
    let (_target, stream_segment) = first_cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: stream_payload.len() as u64,
                segment_crc64: stream_crc64,
                payload_crc64: stream_crc64,
                segment_okh: [0x5a; 16],
            },
        )
        .unwrap();
    let stream_written = first_cluster
        .write_stream_segment_payload_shards(&stream_segment, stream_payload)
        .unwrap();
    let stream_shard_batch = stream_written
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    first_cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            stream_segment.segment_index,
            &stream_segment,
            &stream_shard_batch,
        )
        .unwrap();

    let winner_payload = b"winner before stream finalize command id";
    let winner_reservation_id =
        crate::SessionId::try_from("5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b".to_string()).unwrap();
    let winner_generation_id = second_cluster
        .reserve_put_object_generation(&bucket, &key, &winner_reservation_id)
        .unwrap();
    let winner_written = second_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            winner_generation_id,
            0,
            &[0x5b; 16],
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
            segment_okh: [0x5b; 16],
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
    let _hook_guard = first_cluster.test_install_before_stream_put_finalize_command_id_hook(
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
        .finalize_put_object_stream(
            &bucket,
            &key,
            &session_id,
            stream_payload.len() as u64,
            move |snapshot| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(crate::PreparedStreamPutCommit {
                    value: snapshot.existing_etag,
                    versioning: crate::BucketVersioningState::Disabled,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    etag_crc64: stream_crc64,
                    tags: None,
                    metadata_blob: crate::SerializedMetadataBlob::default(),
                    system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                })
            },
        )
        .unwrap()
        .unwrap();
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        2,
        "stream PUT finalization must rerun after command-id contention"
    );
    assert!(
        result.value.is_some(),
        "retry should observe the drained winner as the stale payload"
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
        let live = stored
            .as_live()
            .expect("stream finalization object is live");
        assert_eq!(live.generation_id, stream_generation_id);
        assert_eq!(live.size, stream_payload.len() as u64);
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &session_id,
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn successful_streamed_overwrites_do_not_block_bucket_delete_after_object_cleanup() {
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
    let bucket = bucket_for_pg(topology, 1, "stream-overwrite-delete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let first_session =
        crate::SessionId::try_from("a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1".to_string()).unwrap();
    let second_session =
        crate::SessionId::try_from("a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2".to_string()).unwrap();

    for (session_id, payload, segment_okh) in [
        (
            &first_session,
            b"first streamed object".as_slice(),
            [0xa1; 16],
        ),
        (
            &second_session,
            b"second streamed object".as_slice(),
            [0xa2; 16],
        ),
    ] {
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
        let crc64 = checksum::crc64::checksum(payload);
        let (_target, segment) = cluster
            .prepare_stream_segment_append(
                &bucket,
                &key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: session_id.clone(),
                    segment_index: 0,
                    size: payload.len() as u64,
                    segment_crc64: crc64,
                    payload_crc64: crc64,
                    segment_okh,
                },
            )
            .unwrap();
        let written = cluster
            .write_stream_segment_payload_shards(&segment, payload)
            .unwrap();
        let shard_batch = written
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        cluster
            .commit_stream_segment_append(
                &bucket,
                &key,
                session_id,
                segment.segment_index,
                &segment,
                &shard_batch,
            )
            .unwrap();
        cluster
            .finalize_put_object_stream(&bucket, &key, session_id, payload.len() as u64, |_| {
                Ok::<_, ()>(crate::PreparedStreamPutCommit {
                    value: (),
                    versioning: crate::BucketVersioningState::Disabled,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    etag_crc64: crc64,
                    tags: None,
                    metadata_blob: crate::SerializedMetadataBlob::default(),
                    system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                })
            })
            .unwrap()
            .unwrap();
    }

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        let stream_uploads = crate::PgMetadataStore::list_all_stream_uploads(&*pg).unwrap();
        assert!(
                stream_uploads.is_empty(),
                "successful streamed overwrites must not leave stream_uploads rows on node {node_id:?}: {stream_uploads:?}"
            );
    }

    let outcome = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            assert!(matches!(stored, Some(crate::StoredObject::Live(_))));
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(matches!(
        outcome.deleted,
        crate::DeletedCurrentObject::Live { .. }
    ));
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
}

#[test]
fn abandoned_put_object_stream_upload_does_not_block_bucket_delete() {
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
    let bucket = bucket_for_pg(topology, 1, "abandoned-stream-delete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let session_id =
        crate::SessionId::try_from("a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3a3".to_string()).unwrap();
    let create = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &session_id)
        .unwrap();
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        crate::PgMetadataStore::create_stream_upload(&*pg, &create).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &session_id
            )
            .unwrap(),
            generation_id
        );
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect("abandoned direct PUT stream staging must not make bucket non-empty");

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        assert!(
            matches!(
                crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ),
            "DeleteBucket should abort abandoned direct PUT stream staging on node {node_id:?}"
        );
        assert!(
                matches!(
                    crate::PgMetadataStore::get_object_generation_reservation(
                        &*pg,
                        &bucket,
                        &key,
                        &session_id
                    ),
                    Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
                ),
                "DeleteBucket should release abandoned direct PUT generation reservation on node {node_id:?}"
            );
    }
    assert_eq!(
        cluster.try_finalize_bucket_delete(&bucket).unwrap(),
        crate::BucketDeleteFinalizeOutcome::Finalized
    );
}

#[test]
fn active_put_object_stream_upload_blocks_bucket_delete() {
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
    let bucket = bucket_for_pg(topology, 1, "active-stream-delete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let session_id =
        crate::SessionId::try_from("a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6a6".to_string()).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        crate::cluster::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        crate::cluster::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("fresh active bucket should acquire delete drain")
        }
    };

    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "connected direct PUT stream must make DeleteBucket return BucketNotEmpty: {err:?}"
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
        "terminal BucketNotEmpty should clear adopted delete drain {}",
        drain.record.drain_id
    );
    let outcome = crate::PgMetadataStore::bucket_delete_attempt_outcome(&*bucket_pg, &bucket)
        .unwrap()
        .expect("terminal BucketNotEmpty should record the delete attempt outcome");
    assert_eq!(outcome.drain_id, drain.record.drain_id);
    assert_eq!(
        outcome.outcome,
        crate::BucketDeleteAttemptOutcomeKind::NotEmpty
    );
    assert_eq!(
        outcome.phase,
        crate::BucketDeleteAttemptPhase::StreamCleanup
    );
    drop(bucket_pg);

    cluster
        .abort_stream_upload_session(&bucket, &key, &session_id)
        .unwrap();
    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
}

#[test]
fn old_empty_put_object_stream_with_live_proof_blocks_bucket_delete() {
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
    let bucket = bucket_for_pg(topology, 1, "stale-empty-stream-delete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let session_id =
        crate::SessionId::try_from("a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9a9".to_string()).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    cluster
        .test_mark_stream_upload_stale(&bucket, &key, &session_id)
        .unwrap();

    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
        ),
        "valid direct PUT stream proof must block DeleteBucket regardless of stream age: {err:?}"
    );

    for node_id in node_ids {
        let object_pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        crate::PgMetadataStore::get_stream_upload(&*object_pg, &session_id)
            .expect("live stream row must remain after blocked DeleteBucket");
        crate::PgMetadataStore::get_object_generation_reservation(
            &*object_pg,
            &bucket,
            &key,
            &session_id,
        )
        .expect("live stream generation reservation must remain after blocked DeleteBucket");
    }
    let bucket_pg_primary = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
            !crate::PgMetadataStore::durable_bucket_write_reservations(
                &*bucket_pg_primary,
                &bucket
            )
            .unwrap()
            .is_empty(),
            "valid stream-create bucket reservation must remain on the bucket PG primary after blocked DeleteBucket"
        );

    // Do not terminalize this synthetic stream after forcing created_at:
    // terminal commands intentionally validate the exact create image.
}

#[test]
fn stream_put_heartbeat_then_finalize_converges_across_stale_replica_deadlines() {
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
    let bucket = bucket_for_pg(topology, 1, "stream-heartbeat-proof-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id =
        crate::SessionId::try_from("acacacacacacacacacacacacacacacac".to_string()).unwrap();
    crate::clock::with_time_override(1_000, || {
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
    });

    let object_pg = || {
        map.metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap()
    };
    let initial_proof = crate::PgMetadataStore::get_stream_upload(&*object_pg(), &session_id)
        .unwrap()
        .bucket_write_reservation
        .expect("stream session should carry initial bucket write proof");
    let original_command = crate::metadata_command::CreateStreamUploadCommand {
        session: crate::StreamUploadCommandRecord {
            session_id: session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: crate::StreamUploadTarget::PutObject,
            state: crate::StreamUploadState::InProgress,
            created_at: 1_000,
            encryption: crate::ObjectEncryption::None,
        },
        initial_next_segment_vid: crate::GenerationId::MIN,
        cleanup_after: None,
        bucket_write_reservation: initial_proof.clone(),
    };

    crate::clock::with_time_override(5_000, || {
        cluster
            .heartbeat_put_object_stream_session(&bucket, &key, &session_id)
            .unwrap();
    });
    let first_renewed = crate::PgMetadataStore::get_stream_upload(&*object_pg(), &session_id)
        .unwrap()
        .bucket_write_reservation
        .expect("heartbeat should preserve bucket write proof");
    assert!(
        first_renewed.lease_deadline > initial_proof.lease_deadline,
        "stream row proof deadline should advance with durable reservation heartbeat"
    );

    crate::clock::with_time_override(6_000, || {
        cluster
            .heartbeat_put_object_stream_session(&bucket, &key, &session_id)
            .unwrap();
    });
    let second_renewed = crate::PgMetadataStore::get_stream_upload(&*object_pg(), &session_id)
        .unwrap()
        .bucket_write_reservation
        .expect("second heartbeat should preserve bucket write proof");
    assert!(
        second_renewed.lease_deadline > first_renewed.lease_deadline,
        "subsequent heartbeat should use the renewed stream row proof"
    );
    let create = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };
    let mutation_client = cluster
        .object_mutation_metadata_primary_client(&bucket, &key)
        .unwrap();
    let stream_creation_route = mutation_client
        .open_stream_upload_creation_metadata_route(
            cluster.operation_epoch(),
            cluster.object_metadata_pg(&bucket, &key),
            &bucket,
            &key,
        )
        .unwrap();
    assert!(
        stream_creation_route
            .matching_stream_upload_exists(&create, Some(&original_command))
            .unwrap(),
        "stream create idempotency should ignore the mutable proof lease deadline"
    );

    let mut observed_stale_replica_deadline = false;
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        let replica_proof = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id)
            .unwrap()
            .bucket_write_reservation
            .expect("every object-PG replica must retain the stream proof");
        assert!(
            replica_proof.has_same_stable_identity(&second_renewed),
            "heartbeat must not change stable reservation identity on node {node_id:?}"
        );
        if replica_proof.lease_deadline != second_renewed.lease_deadline {
            observed_stale_replica_deadline = true;
            assert_eq!(replica_proof.lease_deadline, initial_proof.lease_deadline);
        }
    }
    assert!(
        observed_stale_replica_deadline,
        "the regression must finalize while at least one replica retains the pre-heartbeat deadline"
    );

    crate::clock::with_time_override(7_000, || {
        cluster
            .finalize_put_object_stream(&bucket, &key, &session_id, 0, |_| {
                Ok::<_, ()>(crate::PreparedStreamPutCommit {
                    value: (),
                    versioning: crate::BucketVersioningState::Disabled,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    etag_crc64: checksum::crc64::checksum(&[]),
                    tags: None,
                    metadata_blob: crate::SerializedMetadataBlob::default(),
                    system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                })
            })
            .unwrap()
            .unwrap();
    });

    let mut converged_state = None;
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_object_version(&*pg, &bucket, &key, crate::VersionId::Null,),
            Ok(crate::StoredObject::Live(_))
        ));
        let state = pg.metadata_command_replica_state().unwrap();
        if let Some(expected) = &converged_state {
            assert_eq!(&state, expected);
        } else {
            converged_state = Some(state);
        }

        let bucket_pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
                .unwrap()
                .is_empty(),
            "finalization must release the renewed reservation on node {node_id:?}"
        );
    }
}

#[test]
fn bucket_delete_refreshes_stale_stream_proof_for_live_heartbeated_reservation() {
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
    let bucket = bucket_for_pg(topology, 1, "stale-live-stream-delete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id =
        crate::SessionId::try_from("adadadadadadadadadadadadadadadad".to_string()).unwrap();
    crate::clock::with_time_override(1_000, || {
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
    });

    let object_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    let stale_proof = crate::PgMetadataStore::get_stream_upload(&*object_pg, &session_id)
        .unwrap()
        .bucket_write_reservation
        .expect("stream session should carry initial bucket write proof");
    drop(object_pg);

    let bucket_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let renewed = crate::PgMetadataStore::heartbeat_durable_bucket_write_reservation(
        &*bucket_pg,
        crate::traits::DurableBucketWriteReservationHeartbeat {
            name: &bucket,
            reservation_id: &stale_proof.reservation_id,
            owner_token: &stale_proof.owner_token,
            cluster_epoch: stale_proof.cluster_epoch,
            bucket_execution_generation: stale_proof.bucket_execution_generation,
            bucket_incarnation_generation: stale_proof.bucket_incarnation_generation,
            current_lease_deadline: stale_proof.lease_deadline,
            lease_deadline: 30_000,
            now: 2_000,
        },
    )
    .unwrap();
    drop(bucket_pg);

    crate::clock::with_time_override(5_000, || {
        let err = cluster
            .test_begin_bucket_delete_if_current(&bucket)
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
            ),
            "DeleteBucket must treat stale stream proof with live renewed reservation as a blocker: {err:?}"
        );
    });

    let object_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    let refreshed = crate::PgMetadataStore::get_stream_upload(&*object_pg, &session_id)
        .unwrap()
        .bucket_write_reservation
        .expect("DeleteBucket live check should refresh stream proof");
    assert_eq!(refreshed.lease_deadline, renewed.lease_deadline);
}

#[test]
fn expired_put_object_stream_proof_allows_bucket_delete_cleanup() {
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
    let bucket = bucket_for_pg(topology, 1, "expired-stream-delete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let session_id =
        crate::SessionId::try_from("abababababababababababababababab".to_string()).unwrap();
    crate::clock::with_time_override(1_000, || {
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                crate::ObjectEncryption::None,
            )
            .unwrap();
    });

    crate::clock::with_time_override(62_000, || {
        cluster
            .test_begin_bucket_delete_if_current(&bucket)
            .expect("expired direct PUT stream proof should be abandoned cleanup");
    });

    for node_id in node_ids {
        let object_pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        assert!(
            matches!(
                crate::PgMetadataStore::get_stream_upload(&*object_pg, &session_id),
                Err(crate::MetadataError::StreamSessionNotFound { .. })
            ),
            "expired stream row should be aborted during DeleteBucket cleanup on node {node_id:?}"
        );
        assert!(
            matches!(
                crate::PgMetadataStore::get_object_generation_reservation(
                    &*object_pg,
                    &bucket,
                    &key,
                    &session_id
                ),
                Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
            ),
            "expired stream generation reservation should be released on node {node_id:?}"
        );
    }
    let bucket_pg_primary = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg_primary, &bucket)
            .unwrap()
            .is_empty(),
        "expired stream-create bucket reservation should be released"
    );
}

#[test]
fn stream_session_scavenger_aborts_only_expired_put_object_proofs() {
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
    let live_bucket = bucket_for_pg(topology, 1, "live-scavenge-stream-");
    let live_key = key_for_object_pg(topology, &live_bucket, 2, "object-");
    let expired_bucket = bucket_for_pg(topology, 1, "expired-scavenge-stream-");
    let expired_key = key_for_object_pg(topology, &expired_bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &live_bucket);
    create_test_bucket(&cluster, &expired_bucket);

    let live_session =
        crate::SessionId::try_from("acacacacacacacacacacacacacacacac".to_string()).unwrap();
    let expired_session =
        crate::SessionId::try_from("adadadadadadadadadadadadadadadad".to_string()).unwrap();
    crate::clock::with_time_override(1_000, || {
        cluster
            .create_put_object_stream_session_record(
                &expired_bucket,
                &expired_key,
                &expired_session,
                crate::ObjectEncryption::None,
            )
            .unwrap();
    });
    crate::clock::with_time_override(61_500, || {
        cluster
            .create_put_object_stream_session_record(
                &live_bucket,
                &live_key,
                &live_session,
                crate::ObjectEncryption::None,
            )
            .unwrap();
        assert_eq!(
            cluster.scavenge_abandoned_stream_sessions(0).cleaned,
            1,
            "scavenger should abort only the expired proof"
        );
    });

    let object_pg = map
        .node(NodeId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    crate::PgMetadataStore::get_stream_upload(&*object_pg, &live_session)
        .expect("live stream proof must survive scavenger");
    assert!(matches!(
        crate::PgMetadataStore::get_stream_upload(&*object_pg, &expired_session),
        Err(crate::MetadataError::StreamSessionNotFound { .. })
    ));
}

#[test]
fn stream_session_scavenger_does_not_age_abort_upload_part_sessions() {
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
    let bucket = bucket_for_pg(topology, 1, "upload-part-scavenge-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("scavengeuploadpart");
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>((
                    (),
                    crate::CreateMultipartUploadReq {
                        upload_id: upload_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        tags: None,
                        metadata_blob: crate::SerializedMetadataBlob::default(),
                        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                        initiator: crate::OwnerIdentity::from_principal("initiator"),
                        owner: crate::OwnerIdentity::from_principal("owner"),
                        acl_grants: crate::AclGrants::default(),
                        public_read: false,
                        object_lock: crate::ObjectLockState::default(),
                        checksum: None,
                        encryption: crate::ObjectEncryption::None,
                    },
                ))
            },
        )
        .unwrap()
        .unwrap();

    let session_id =
        crate::SessionId::try_from("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_string()).unwrap();
    crate::clock::with_time_override(1_000, || {
        let reservation = cluster
            .acquire_durable_bucket_write_reservation(
                &bucket,
                crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
                Some(key.as_str()),
            )
            .unwrap();
        let proof = crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);
        cluster
            .begin_upload_part_stream_session(
                crate::BeginUploadPartStreamSessionReq {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    upload_id: upload_id.clone(),
                    part_number: 1,
                    session_id: session_id.clone(),
                    bucket_write_reservation: proof,
                },
                |upload| {
                    Ok::<_, ()>((
                        crate::AuthorizedMultipartUploadRecord::assume_authorized(upload.clone()),
                        (),
                    ))
                },
            )
            .unwrap()
            .unwrap();
    });

    crate::clock::with_time_override(121_000, || {
        assert_eq!(
            cluster.scavenge_abandoned_stream_sessions(60_000).cleaned,
            0,
            "periodic scavenger must not age-abort UploadPart sessions without durable liveness"
        );
    });

    let object_pg = map
        .node(NodeId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    let upload = crate::PgMetadataStore::get_stream_upload(&*object_pg, &session_id)
        .expect("active UploadPart stream session must survive periodic scavenger");
    assert_eq!(
        upload.target,
        crate::StreamUploadTarget::UploadPart {
            upload_id,
            part_number: 1,
        }
    );
}

#[test]
fn active_put_object_stream_upload_blocks_bucket_delete_from_independent_frontend() {
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
    let bucket = bucket_for_pg(topology, 1, "active-stream-remote-delete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let writer_frontend = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let deleting_frontend = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&writer_frontend, &bucket);

    let session_id =
        crate::SessionId::try_from("a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7".to_string()).unwrap();
    writer_frontend
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let err = deleting_frontend
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
            matches!(
                err,
                crate::BucketWriteDrainError::Metadata(crate::MetadataError::BucketNotEmpty)
            ),
            "a connected stream created by another frontend must make DeleteBucket return BucketNotEmpty: {err:?}"
        );
    {
        let object_pg = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        crate::PgMetadataStore::get_stream_upload(&*object_pg, &session_id)
            .expect("DeleteBucket must not abort a live stream session owned by another frontend");
    }

    writer_frontend
        .abort_stream_upload_session(&bucket, &key, &session_id)
        .unwrap();
    deleting_frontend
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap();
}

#[test]
fn bucket_delete_treats_conflicting_stream_reservation_proof_as_abandoned() {
    let _serial = lock_bucket_scoped_hook_test();
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
    let bucket = bucket_for_pg(topology, 1, "conflicting-stream-proof-delete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let session_id =
        crate::SessionId::try_from("a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8".to_string()).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let object_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    let stream_session = crate::PgMetadataStore::get_stream_upload(&*object_pg, &session_id)
        .expect("stream session should exist");
    let proof = stream_session
        .bucket_write_reservation
        .clone()
        .expect("direct PutObject stream row should carry its create reservation proof");
    drop(object_pg);

    let bucket_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let proof_record = crate::PgMetadataStore::durable_bucket_write_reservation(
        &*bucket_pg,
        &bucket,
        &proof.reservation_id,
    )
    .unwrap()
    .expect("stream proof should still have a durable bucket write reservation");
    crate::PgMetadataStore::release_durable_bucket_write_reservation(&*bucket_pg, &proof_record)
        .unwrap();
    let mismatched_record = crate::PgMetadataStore::acquire_durable_bucket_write_reservation(
        &*bucket_pg,
        crate::traits::DurableBucketWriteReservationAcquire {
            name: &bucket,
            reservation_id: &proof.reservation_id,
            owner_token: "different-stream-owner",
            cluster_epoch: proof.cluster_epoch,
            operation_kind: &proof.operation_kind,
            created_at: proof.created_at,
            lease_deadline: proof.lease_deadline,
            target_context: proof.target_context.as_deref(),
        },
    )
    .unwrap();
    drop(bucket_pg);

    let released_mismatch = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_record = mismatched_record.clone();
    let released_mismatch_for_hook = Arc::clone(&released_mismatch);
    let _hook_guard =
        crate::node::install_bucket_scoped_test_hooks(crate::node::BucketScopedTestHooks {
            target: Some(hook_bucket.clone()),
            before_bucket_write_drain_wait: Some(Arc::new(move || {
                if released_mismatch_for_hook.swap(true, Ordering::SeqCst) {
                    return;
                }
                let bucket_pg = hook_map
                    .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
                    .unwrap()
                    .storage_node()
                    .get_pg(1)
                    .unwrap();
                crate::PgMetadataStore::release_durable_bucket_write_reservation(
                    &*bucket_pg,
                    &hook_record,
                )
                .unwrap();
            })),
            ..crate::node::BucketScopedTestHooks::default()
        });

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect("stale/conflicting stream proof must not keep bucket non-empty");
    assert!(
        released_mismatch.load(Ordering::SeqCst),
        "DeleteBucket should wait for and release the unrelated mismatched reservation"
    );
    let object_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    assert!(matches!(
        crate::PgMetadataStore::get_stream_upload(&*object_pg, &session_id),
        Err(crate::MetadataError::StreamSessionNotFound { .. })
    ));
    assert!(matches!(
        crate::PgMetadataStore::get_object_generation_reservation(
            &*object_pg,
            &bucket,
            &key,
            &session_id
        ),
        Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
    ));
}

#[test]
fn bucket_delete_stream_cleanup_tolerates_concurrent_missing_session() {
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
    let bucket = bucket_for_pg(topology, 1, "missing-stream-delete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let session_id =
        crate::SessionId::try_from("a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4".to_string()).unwrap();
    cluster
        .reserve_put_object_generation(&bucket, &key, &session_id)
        .unwrap();
    let create = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        crate::PgMetadataStore::create_stream_upload(&*pg, &create).unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    let removed = Arc::new(AtomicBool::new(false));
    let removed_for_hook = Arc::clone(&removed);
    let map_for_hook = Arc::clone(&map);
    let session_for_hook = session_id.clone();
    let _hook = cluster.test_install_before_stream_abort_storage_hook(Arc::new(move || {
        if removed_for_hook.swap(true, Ordering::SeqCst) {
            return;
        }
        for node_id in node_ids {
            let pg = map_for_hook
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(2)
                .unwrap();
            crate::PgMetadataStore::delete_stream_upload(&*pg, &session_for_hook).unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
        }
    }));

    cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .expect("DeleteBucket cleanup should tolerate a concurrently aborted stream session");
    assert!(removed.load(Ordering::SeqCst));

    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
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
fn bucket_delete_stream_cleanup_rejects_wrong_pg_stream_row() {
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
    let bucket = bucket_for_pg(topology, 1, "wrong-pg-stream-delete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    assert_eq!(cluster.object_metadata_pg_id(&bucket, &key), 2);

    let session_id =
        crate::SessionId::try_from("a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5".to_string()).unwrap();
    let create = crate::CreateStreamUploadReq {
        session_id,
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(0).unwrap();
        crate::PgMetadataStore::create_stream_upload(&*pg, &create).unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    let err = cluster
        .test_begin_bucket_delete_if_current(&bucket)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::RouteCapabilitySubjectMismatch {
                operation: "list bucket stream uploads",
            })
        ),
        "wrong-PG stream row should fail closed, got {err:?}"
    );
}

#[test]
fn stream_put_record_pending_install_race_releases_reservation_and_retries() {
    let _guard = lock_metadata_command_apply_hook_test();
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
    let bucket = bucket_for_pg(topology, 1, "stream-record-install-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    let contender_key = key_for_object_pg(topology, &bucket, 2, "contender-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let session_id =
        crate::SessionId::try_from("58585858585858585858585858585858".to_string()).unwrap();
    let contender_session_id =
        crate::SessionId::try_from("59595959595959595959595959595959".to_string()).unwrap();
    let pg_id = PgId::new(2);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_contender_key = contender_key.clone();
    let hook_contender_session = contender_session_id.clone();
    let target_session = session_id.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            if crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &hook_bucket,
                &hook_key,
                &target_session,
            )
            .is_err()
                || hook_ran_for_closure.swap(true, Ordering::SeqCst)
            {
                return;
            }
            drop(pg);
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    hook_map.test_next_metadata_command_log_index(pg_id),
                ),
                MetadataCommandPayload::ReserveObjectGeneration(
                    ReserveObjectGenerationCommand::new(
                        hook_bucket.clone(),
                        hook_contender_key.clone(),
                        hook_contender_session.clone(),
                        GenerationId::MIN,
                        1_234,
                    ),
                ),
            );
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
        }));

    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());

    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let primary_pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
    let applied_log_index = primary_pg
        .metadata_command_replica_state()
        .unwrap()
        .applied_log_index;
    let entries = primary_pg
        .retained_metadata_command_log_entries(
            primary.node_id().as_u32(),
            ClusterEpoch::INITIAL,
            MetadataCommandLogIndex::new(1).unwrap(),
            MetadataCommandLogIndex::new(applied_log_index).unwrap(),
        )
        .unwrap()
        .into_iter()
        .filter_map(|entry| match entry.kind {
            crate::metadata_command::MetadataCommandLogRangeEntryKind::Applied(command) => {
                Some(command)
            }
            crate::metadata_command::MetadataCommandLogRangeEntryKind::Abandoned { .. } => None,
        })
        .collect::<Vec<_>>();
    assert!(
        entries.iter().any(|command| {
            matches!(
                command.payload(),
                MetadataCommandPayload::ReleaseObjectGeneration(release)
                    if release.matches_request(&bucket, &key, &session_id)
            )
        }),
        "pending-install contention must release the stream create reservation before retrying"
    );
    assert!(
        entries.iter().any(|command| {
            matches!(
                command.payload(),
                MetadataCommandPayload::CreateStreamUpload(create)
                    if create.session.session_id == session_id
            )
        }),
        "retry must publish the requested stream session after cleanup"
    );
    drop(primary_pg);
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
        assert_eq!(session.bucket, bucket);
        assert_eq!(session.key, key);
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &contender_key,
                &contender_session_id,
            )
            .unwrap(),
            GenerationId::MIN
        );
    }
}
