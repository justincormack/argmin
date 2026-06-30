use super::*;

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
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster = crate::StorageCluster::from_local_map(Arc::clone(&second_map)).unwrap();
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
            acquire_test_bucket_write_proof(
                &first_cluster,
                &bucket,
                "stream-put-finalize-test",
                Some(key.as_str()),
            ),
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
                        size: loser_payload.len() as u64,
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
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster = crate::StorageCluster::from_local_map(Arc::clone(&second_map)).unwrap();
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
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster = crate::StorageCluster::from_local_map(Arc::clone(&second_map)).unwrap();
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
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    let second_cluster = crate::StorageCluster::from_local_map(Arc::clone(&second_map)).unwrap();
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
            acquire_test_bucket_write_proof(
                &first_cluster,
                &bucket,
                "stream-put-finalize-test",
                Some(key.as_str()),
            ),
            move |snapshot| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(crate::PreparedStreamPutCommit {
                    value: snapshot.existing_etag,
                    versioning: crate::BucketVersioningState::Disabled,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    size: stream_payload.len() as u64,
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
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
            .finalize_put_object_stream(
                &bucket,
                &key,
                session_id,
                payload.len() as u64,
                acquire_test_bucket_write_proof(
                    &cluster,
                    &bucket,
                    "streamed-overwrite-cleanup-test",
                    Some(key.as_str()),
                ),
                |_| {
                    Ok::<_, ()>(crate::PreparedStreamPutCommit {
                        value: (),
                        versioning: crate::BucketVersioningState::Disabled,
                        owner: crate::OwnerIdentity::from_principal("owner"),
                        acl_grants: crate::AclGrants::default(),
                        public_read: false,
                        size: payload.len() as u64,
                        etag_crc64: crc64,
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
    cluster.begin_bucket_delete(&bucket).unwrap();
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
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
        .begin_bucket_delete(&bucket)
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
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

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
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
    assert_eq!(outcome.phase, crate::BucketDeleteAttemptPhase::Initial);
    drop(bucket_pg);

    cluster
        .abort_stream_upload_session(&bucket, &key, &session_id)
        .unwrap();
    cluster.begin_bucket_delete(&bucket).unwrap();
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
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
        .test_force_stream_upload_created_at(&bucket, &key, &session_id, 0)
        .unwrap();

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
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
            .begin_bucket_delete(&bucket)
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
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
            cluster.scavenge_abandoned_stream_sessions(0),
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
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
                        initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
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
                "begin-upload-part-scavenge-test",
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
            cluster.scavenge_abandoned_stream_sessions(60_000),
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
    let writer_frontend = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let deleting_frontend = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
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

    let err = deleting_frontend.begin_bucket_delete(&bucket).unwrap_err();
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
    deleting_frontend.begin_bucket_delete(&bucket).unwrap();
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
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
    crate::PgMetadataStore::release_durable_bucket_write_reservation(
        &*bucket_pg,
        &bucket,
        &proof.reservation_id,
        &proof.owner_token,
        proof.cluster_epoch,
        proof.bucket_execution_generation,
        proof.bucket_incarnation_generation,
    )
    .unwrap();
    let mismatched_record = crate::PgMetadataStore::acquire_durable_bucket_write_reservation(
        &*bucket_pg,
        &bucket,
        &proof.reservation_id,
        "different-stream-owner",
        proof.cluster_epoch,
        &proof.operation_kind,
        proof.created_at,
        proof.lease_deadline,
        proof.target_context.as_deref(),
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
                    &hook_bucket,
                    &hook_record.reservation_id,
                    &hook_record.owner_token,
                    hook_record.cluster_epoch,
                    hook_record.bucket_execution_generation,
                    hook_record.bucket_incarnation_generation,
                )
                .unwrap();
            })),
            ..crate::node::BucketScopedTestHooks::default()
        });

    cluster
        .begin_bucket_delete(&bucket)
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
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
        .begin_bucket_delete(&bucket)
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
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

    let err = cluster.begin_bucket_delete(&bucket).unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketWriteDrainError::Store(StoreError::Io { .. })
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
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
    let command_bytes = primary_pg
        .connection()
        .prepare("SELECT command_bytes FROM metadata_command_log ORDER BY log_index")
        .unwrap()
        .query_map([], |row| row.get::<_, Vec<u8>>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        command_bytes.iter().any(|bytes| {
            matches!(
                crate::metadata_command::decode_metadata_command_envelope(bytes)
                    .unwrap()
                    .payload(),
                MetadataCommandPayload::ReleaseObjectGeneration(release)
                    if release.matches_request(&bucket, &key, &session_id)
            )
        }),
        "pending-install contention must release the stream create reservation before retrying"
    );
    assert!(
        command_bytes.iter().any(|bytes| {
            matches!(
                crate::metadata_command::decode_metadata_command_envelope(bytes)
                    .unwrap()
                    .payload(),
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
