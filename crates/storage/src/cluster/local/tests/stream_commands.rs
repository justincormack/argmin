use super::*;

#[test]
fn stream_put_create_partial_apply_retry_reuses_existing_session() {
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
    let session_id = crate::SessionId::try_from("34".repeat(16)).unwrap();
    let create = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateStreamUpload(create)
                    if create.session.session_id == hook_session_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected stream create metadata command replica apply failure",
                        source: std::io::Error::other(
                            "injected stream create metadata command replica apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::Io {
                context: "injected stream create metadata command replica apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);

    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial stream create command must remain pending"
    );
    let primary_created_at = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        crate::PgMetadataStore::get_stream_upload(&*pg, &session_id)
            .unwrap()
            .created_at
    };
    {
        let failed_replica = map.node(NodeId::new(2)).unwrap().storage_node();
        let pg = failed_replica.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }

    let retry_value = cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>((7_u8, create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(retry_value, 7);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
        assert_eq!(session.bucket, bucket);
        assert_eq!(session.key, key);
        assert_eq!(session.created_at, primary_created_at);
        assert!(matches!(
            session.target,
            crate::StreamUploadTarget::PutObject
        ));
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &session_id,
            )
            .unwrap(),
            crate::GenerationId::MIN
        );
    }
}

#[test]
fn stream_put_create_retry_rejects_same_request_with_mismatched_created_at() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("7a".repeat(16)).unwrap();
    let create = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };

    cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    {
        let primary = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
        pg.connection()
            .execute(
                "UPDATE stream_uploads SET created_at = ?1 WHERE session_id = ?2",
                rusqlite::params![
                    session.created_at.saturating_add(1) as i64,
                    session_id.as_str()
                ],
            )
            .unwrap();
    }

    let err = cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::Db {
                context: "create stream upload existing session mismatch",
                ..
            })
        ),
        "expected exact stream session row mismatch, got {err:?}"
    );
}

#[test]
fn stream_put_create_retry_rejects_same_request_with_mismatched_allocator_floor() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("7b".repeat(16)).unwrap();
    let create = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };

    cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    {
        let primary = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        pg.connection()
            .execute(
                "UPDATE stream_uploads SET next_segment_vid = ?1 WHERE session_id = ?2",
                rusqlite::params![2_i64, session_id.as_str()],
            )
            .unwrap();
    }

    let err = cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::Db {
                context: "create stream upload existing session mismatch",
                ..
            })
        ),
        "expected explicit initial allocator floor mismatch, got {err:?}"
    );
}

#[test]
fn stream_put_create_drains_unrelated_pending_create_before_new_session() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let first_session_id = crate::SessionId::try_from("37".repeat(16)).unwrap();
    let second_session_id = crate::SessionId::try_from("38".repeat(16)).unwrap();
    let first_create = crate::CreateStreamUploadReq {
        session_id: first_session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };
    let second_create = crate::CreateStreamUploadReq {
        session_id: second_session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = first_session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateStreamUpload(create)
                    if create.session.session_id == hook_session_id
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected stream create metadata command apply failure",
                        source: std::io::Error::other(
                            "injected stream create metadata command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), first_create.clone()))
            },
        )
        .unwrap_err();
    drop(hook_guard);

    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial first stream create command must remain pending"
    );

    let value = cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>((9_u8, second_create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(value, 9);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        crate::PgMetadataStore::get_stream_upload(&*pg, &first_session_id).unwrap();
        crate::PgMetadataStore::get_stream_upload(&*pg, &second_session_id).unwrap();
    }
}

#[test]
fn stream_put_create_retries_after_pending_install_conflict() {
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
    let session_id = crate::SessionId::try_from("39".repeat(16)).unwrap();
    let unrelated_session_id = crate::SessionId::try_from("3a".repeat(16)).unwrap();
    let create = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };

    let pg_id = PgId::new(object_pg);
    let _serial = lock_metadata_command_apply_hook_test();
    let injected = Arc::new(AtomicBool::new(false));
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let injected_for_hook = Arc::clone(&injected);
    let map_for_hook = Arc::clone(&map);
    let bucket_for_hook = bucket.clone();
    let key_for_hook = key.clone();
    let session_for_hook = unrelated_session_id.clone();
    let proof_for_hook = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-stream-put-create-unrelated",
        Some(key.as_str()),
    );
    let command_epoch = cluster.operation_epoch();
    let _hook_guard = cluster.test_install_before_stream_put_create_pending_install_hook(
            Arc::new(move || {
                if injected_for_hook.swap(true, Ordering::SeqCst) {
                    return;
                }
                let command = MetadataCommandEnvelope::new(
                    crate::metadata_command::MetadataCommandId::new(
                        command_epoch,
                        pg_id,
                        map_for_hook.test_next_metadata_command_log_index(pg_id),
                    ),
                    MetadataCommandPayload::CreateStreamUpload(Box::new(
                        crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                            crate::CreateStreamUploadReq {
                                session_id: session_for_hook.clone(),
                                bucket: bucket_for_hook.clone(),
                                key: key_for_hook.clone(),
                                target: crate::StreamUploadTarget::PutObject,
                                encryption: crate::ObjectEncryption::None,
                            },
                            123,
                            proof_for_hook.clone(),
                        ),
                    )),
                );
                insert_pending_metadata_command_for_test(
                    &map_for_hook,
                    pg_id,
                    &bucket_for_hook,
                    &command,
                );
            }),
        );

    let attempts_for_action = Arc::clone(&attempts);
    let value = cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                attempts_for_action.fetch_add(1, Ordering::SeqCst);
                assert!(existing_object.is_none());
                Ok::<_, ()>((13_u8, create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(value, 13);
    assert!(injected.load(Ordering::SeqCst));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "stream creation reruns request action after pending-slot contention"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
        assert_eq!(session.bucket, bucket);
        assert_eq!(session.key, key);
        let unrelated =
            crate::PgMetadataStore::get_stream_upload(&*pg, &unrelated_session_id).unwrap();
        assert_eq!(unrelated.bucket, bucket);
        assert_eq!(unrelated.key, key);
    }
}

#[test]
fn stream_abort_missing_session_does_not_succeed_after_unrelated_pending_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let missing_session_id = crate::SessionId::try_from("35".repeat(16)).unwrap();
    let unrelated_session_id = crate::SessionId::try_from("36".repeat(16)).unwrap();
    let pg_id = PgId::new(object_pg);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-stream-abort-unrelated",
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
                        session_id: unrelated_session_id.clone(),
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

    let error = cluster
        .abort_stream_upload_session(&bucket, &key, &missing_session_id)
        .unwrap_err();
    assert!(
        matches!(
            error,
            crate::ObjectPgActionError::Metadata(
                crate::MetadataError::StreamSessionNotFound { .. }
            )
        ),
        "unrelated pending command must not make missing stream abort idempotent: {error:?}"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let session =
            crate::PgMetadataStore::get_stream_upload(&*pg, &unrelated_session_id).unwrap();
        assert_eq!(session.bucket, bucket);
        assert_eq!(session.key, key);
    }
}

#[test]
fn stream_put_append_partial_apply_keeps_payload_for_pending_retry() {
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
    let session_id = crate::SessionId::try_from("33".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream append partial apply";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                segment_okh: [89; 16],
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

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::AppendStreamSegment(append)
                    if append.segment.session_id == hook_session_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected stream append metadata command replica apply failure",
                        source: std::io::Error::other(
                            "injected stream append metadata command replica apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected stream append metadata command replica apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial stream append command must remain pending"
    );
    {
        let failed_replica = map.node(NodeId::new(2)).unwrap().storage_node();
        let pg = failed_replica.get_pg(object_pg).unwrap();
        assert!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id)
                .unwrap()
                .is_empty()
        );
    }
    for node_id in [NodeId::new(0), NodeId::new(1)] {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
            vec![segment.clone()]
        );
    }

    let mut readback = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size: payload.len(),
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                ec: EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            },
            &mut readback,
        )
        .unwrap();
    assert_eq!(readback, payload);

    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
            vec![segment.clone()]
        );
    }
}

#[test]
fn stream_append_publish_validation_fails_closed_when_acknowledged_shard_file_is_missing() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("37".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream append publish validation";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                segment_okh: [0xd4; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let data_pg_id = DataPgId::new(PgId::new(segment.data_pg_id));
    let placement_key = super::super::super::segment_payload_placement_key(
        &segment.segment_okh,
        segment.segment_vid,
    );
    let locations = cluster
        .place_payload_shards(
            data_pg_id,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
            &placement_key,
        )
        .unwrap();
    let missing_shard = written_shards[0].key.clone();
    let missing_location = locations[usize::from(missing_shard.shard_index().get())];
    map.node(missing_location.node_id())
        .unwrap()
        .storage_node()
        .delete_shard_file(missing_location.data_pg_id().get(), &missing_shard)
        .unwrap();

    let shard_batch: Vec<(&crate::ShardKey, crate::WriteAck)> = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    let err = cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap_err();
    assert!(
            matches!(
                err,
                crate::ObjectPgActionError::Store(StoreError::ShardStore {
                    ref source,
                    ..
                }) if matches!(**source, StoreError::NotFound)
            ),
            "missing acknowledged stream shard file should fail closed before segment publish, got {err:?}"
        );
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    let object_pg_store = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(object_pg))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap();
    assert!(
        crate::PgMetadataStore::list_stream_segments(&*object_pg_store, &session_id)
            .unwrap()
            .is_empty(),
        "failed stream append publish validation must not publish segment metadata"
    );
}

#[test]
fn stream_put_append_command_id_race_drains_winner_before_ack_publish() {
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
    let contender = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("34".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream append command id race";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                segment_okh: [98; 16],
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
    let hook_shard_batch: Vec<(crate::ShardKey, crate::WriteAck)> = written_shards
        .iter()
        .map(|written| (written.key.clone(), written.ack))
        .collect();

    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_segment = segment.clone();
    let hook_once = Arc::new(AtomicBool::new(true));
    let hook_once_for_closure = Arc::clone(&hook_once);
    let _guard = cluster.test_install_before_stream_append_command_id_hook(Arc::new(move || {
        if !hook_once_for_closure.swap(false, Ordering::SeqCst) {
            return;
        }
        let pg_id = PgId::new(object_pg);
        let command_id = contender.next_object_metadata_command_id(pg_id).unwrap();
        let hook_shard_refs: Vec<(&crate::ShardKey, crate::WriteAck)> = hook_shard_batch
            .iter()
            .map(|(key, ack)| (key, *ack))
            .collect();
        contender
            .register_payload_shard_acks(hook_segment.data_pg_id, &hook_shard_refs)
            .unwrap();
        let command = MetadataCommandEnvelope::new(
            command_id,
            MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
                bucket: hook_bucket.clone(),
                key: hook_key.clone(),
                segment: hook_segment.clone(),
            })),
        );
        insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
    }));

    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    assert!(!hook_once.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
            vec![segment.clone()]
        );
    }
}

#[test]
fn stream_abort_pending_drain_cleans_terminal_stream_session() {
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
    let session_id = crate::SessionId::try_from("45".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream abort pending drain allocator cleanup";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                segment_okh: [0x45; 16],
            },
        )
        .unwrap();
    assert_stream_next_segment_vid(&map, NodeId::new(1), object_pg, &session_id, 2);
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::AbortStreamUpload(abort)
                    if abort.session_id == hook_session_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected stream abort metadata command replica apply failure",
                        source: std::io::Error::other(
                            "injected stream abort metadata command replica apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .abort_stream_upload_session(&bucket, &key, &session_id)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected stream abort metadata command replica apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial abort command must remain pending"
    );
    assert_stream_next_segment_vid(&map, NodeId::new(2), object_pg, &session_id, 2);

    let next_reservation_id = crate::SessionId::try_from("46".repeat(16)).unwrap();
    cluster
        .reserve_put_object_generation(&bucket, &key, &next_reservation_id)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
}

#[test]
fn stream_abort_pending_install_race_rebuilds_staged_segments() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("4b".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let first_payload = b"first staged stream abort segment";
    let (_target, first_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: first_payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(first_payload)),
                segment_okh: [0x4b; 16],
            },
        )
        .unwrap();
    let first_shards = cluster
        .write_stream_segment_payload_shards(&first_segment, first_payload)
        .unwrap();
    let first_shard_batch = first_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            first_segment.segment_index,
            &first_segment,
            &first_shard_batch,
        )
        .unwrap();

    let second_payload = b"raced stream append before abort install";
    let (_target, second_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 1,
                size: second_payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(second_payload)),
                segment_okh: [0x4c; 16],
            },
        )
        .unwrap();
    let second_shards = cluster
        .write_stream_segment_payload_shards(&second_segment, second_payload)
        .unwrap();

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let hook_cluster = Arc::clone(&cluster);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_segment = second_segment.clone();
    let hook_shards = second_shards.clone();
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(object_pg);
            let shard_batch = hook_shards
                .iter()
                .map(|written| (&written.key, written.ack))
                .collect::<Vec<_>>();
            hook_cluster
                .register_payload_shard_acks(hook_segment.data_pg_id, &shard_batch)
                .unwrap();
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let log_index = pg
                .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap()
                + 1;
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    MetadataCommandLogIndex::new(log_index).unwrap(),
                ),
                MetadataCommandPayload::AppendStreamSegment(Box::new(AppendStreamSegmentCommand {
                    bucket: hook_bucket.clone(),
                    key: hook_key.clone(),
                    segment: hook_segment.clone(),
                })),
            );
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
        }));

    cluster
        .abort_stream_upload_session(&bucket, &key, &session_id)
        .unwrap();
    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id)
                .unwrap()
                .is_empty()
        );
    }

    for (segment, payload) in [
        (&first_segment, first_payload.as_slice()),
        (&second_segment, second_payload.as_slice()),
    ] {
        let mut readback = Vec::new();
        let error = cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: segment.data_pg_id,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    stored_size: payload.len(),
                    segment_crc64: Some(checksum::crc64::checksum(payload)),
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                },
                &mut readback,
            )
            .unwrap_err();
        assert!(
            matches!(error, StoreError::NotFound),
            "abort must clean the staged payload after rebuilding from the raced append: {error:?}"
        );
    }
}

#[test]
fn stream_abort_matching_pending_install_race_returns_success() {
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
    let session_id = crate::SessionId::try_from("48".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let pg_id = PgId::new(object_pg);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::AbortStreamUpload(Box::new(AbortStreamUploadCommand {
            bucket: bucket.clone(),
            key: key.clone(),
            session_id: session_id.clone(),
            staged_segments: Vec::new(),
            stream_create_bucket_write_reservation: None,
        })),
    );
    let inserted = Arc::new(AtomicBool::new(false));
    let inserted_for_hook = Arc::clone(&inserted);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_command = command.clone();
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if inserted_for_hook.swap(true, Ordering::SeqCst) {
                return;
            }
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &hook_command);
        }));

    cluster
        .abort_stream_upload_session(&bucket, &key, &session_id)
        .unwrap();

    assert!(inserted.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn stream_put_finalize_pending_drain_cleans_terminal_stream_session() {
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
    let bucket_pg = cluster.bucket_metadata_pg_id(&bucket);
    let session_id = crate::SessionId::try_from("49".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream put finalize pending drain allocator cleanup";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(payload_crc64),
                segment_okh: [0x49; 16],
            },
        )
        .unwrap();
    assert_stream_next_segment_vid(&map, NodeId::new(1), object_pg, &session_id, 2);
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.generation_reservation_id == hook_session_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context:
                            "injected stream put finalize metadata command replica apply failure",
                        source: std::io::Error::other(
                            "injected stream put finalize metadata command replica apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let command_reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "stream-put-finalize-test",
            Some(key.as_str()),
        )
        .unwrap();
    let command_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&command_reservation.record);
    let err = cluster
        .finalize_put_object_stream(
            &bucket,
            &key,
            &session_id,
            payload.len() as u64,
            command_proof,
            |_| {
                Ok::<_, ()>(crate::PreparedStreamPutCommit {
                    value: (),
                    versioning: crate::BucketVersioningState::Disabled,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    size: payload.len() as u64,
                    etag_crc64: payload_crc64,
                    tags: None,
                    metadata_blob: crate::SerializedMetadataBlob::default(),
                    system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                    object_lock: crate::ObjectLockState::default(),
                    encryption: crate::ObjectEncryption::None,
                })
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected stream put finalize metadata command replica apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial stream PUT finalize command must remain pending"
    );
    {
        let bucket_primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg))
            .unwrap();
        let bucket_pg_store = bucket_primary.storage_node().get_pg(bucket_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg_store, &bucket,)
                .unwrap()
                .len(),
            2
        );
    }
    assert_stream_next_segment_vid(&map, NodeId::new(2), object_pg, &session_id, 2);

    let next_reservation_id = crate::SessionId::try_from("4a".repeat(16)).unwrap();
    cluster
        .reserve_put_object_generation(&bucket, &key, &next_reservation_id)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().unwrap();
        assert_eq!(live.size, payload.len() as u64);
        assert_eq!(live.generation_id, crate::GenerationId::MIN);
    }
    {
        let bucket_primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg))
            .unwrap();
        let bucket_pg_store = bucket_primary.storage_node().get_pg(bucket_pg).unwrap();
        assert!(crate::PgMetadataStore::durable_bucket_write_reservations(
            &*bucket_pg_store,
            &bucket,
        )
        .unwrap()
        .is_empty());
    }
}

#[test]
fn stream_put_finalize_missing_session_same_pg_releases_bucket_write_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-finalize-missing-");
    let key = key_for_object_pg(topology, &bucket, 1, "object-");

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    assert_eq!(cluster.bucket_metadata_pg_id(&bucket), 1);
    assert_eq!(cluster.object_metadata_pg_id(&bucket, &key), 1);

    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "stream-put-finalize-missing-session-test",
            Some(key.as_str()),
        )
        .unwrap();
    let proof = crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);
    let session_id = crate::SessionId::try_from("7d".repeat(16)).unwrap();
    let action_called = Arc::new(AtomicBool::new(false));
    let action_called_for_closure = Arc::clone(&action_called);

    let err = cluster
        .finalize_put_object_stream(&bucket, &key, &session_id, 0, proof, move |_| {
            action_called_for_closure.store(true, Ordering::SeqCst);
            Ok::<_, ()>(crate::PreparedStreamPutCommit {
                value: (),
                versioning: crate::BucketVersioningState::Disabled,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                size: 0,
                etag_crc64: checksum::crc64::checksum(&[]),
                tags: None,
                metadata_blob: crate::SerializedMetadataBlob::default(),
                system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
                object_lock: crate::ObjectLockState::default(),
                encryption: crate::ObjectEncryption::None,
            })
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Metadata(
                crate::MetadataError::StreamSessionNotFound { .. }
            )
        ),
        "expected StreamSessionNotFound, got {err:?}"
    );
    assert!(!action_called.load(Ordering::SeqCst));

    let bucket_primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap();
    let bucket_pg = bucket_primary.storage_node().get_pg(1).unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn stream_put_finalize_action_failure_same_pg_releases_bucket_write_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-finalize-action-");
    let key = key_for_object_pg(topology, &bucket, 1, "object-");

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    assert_eq!(cluster.bucket_metadata_pg_id(&bucket), 1);
    assert_eq!(cluster.object_metadata_pg_id(&bucket, &key), 1);

    let session_id = crate::SessionId::try_from("7e".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "stream-put-finalize-action-failure-test",
            Some(key.as_str()),
        )
        .unwrap();
    let proof = crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);

    let result: Result<crate::FinalizeStreamPutOutcome<()>, &str> = cluster
        .finalize_put_object_stream(&bucket, &key, &session_id, 0, proof, |_| {
            Err("condition failed")
        })
        .unwrap();
    assert!(matches!(result, Err("condition failed")));

    let bucket_primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap();
    let bucket_pg = bucket_primary.storage_node().get_pg(1).unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket).unwrap();
    assert_eq!(reservations.len(), 1);
    let upload = crate::PgMetadataStore::get_stream_upload(&*bucket_pg, &session_id).unwrap();
    assert_eq!(
        upload.bucket_write_reservation.as_ref(),
        Some(&crate::metadata_command::BucketWriteReservationProof::from(
            &reservations[0]
        )),
        "failed finalize should release its caller proof but keep the live stream-create proof"
    );
}

#[test]
fn stream_put_finalize_matching_pending_install_race_returns_success() {
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
    let session_id = crate::SessionId::try_from("4c".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"stream put same pending install race";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(payload_crc64),
                segment_okh: [0x4c; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let pg_id = PgId::new(object_pg);
    let (generation_id, write_sequence) = {
        let primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
            .unwrap();
        let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
        let generation_id = crate::traits::PgMetadataStore::get_object_generation_reservation(
            &*pg,
            &bucket,
            &key,
            &session_id,
        )
        .unwrap();
        let write_sequence = pg
            .next_object_write_sequence(bucket.as_str(), key.as_str())
            .unwrap();
        (generation_id, write_sequence)
    };
    let object = crate::PutLiveObjectReq {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: crate::VersionId::Null,
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        generation_id,
        size: payload.len() as u64,
        etag: crate::ObjectEtag::single_part(payload_crc64),
        ec: EcShape {
            k: segment.ec_k,
            m: segment.ec_m,
        },
        layout: crate::ObjectLayout::Standard,
        tags: None,
        metadata_blob: Some(crate::SerializedMetadataBlob::default()),
        system_metadata_blob: Some(crate::SerializedSystemMetadataBlob::default()),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
    };
    let segments = vec![crate::ObjectSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: crate::VersionId::Null,
        segment_index: segment.segment_index,
        size: segment.size,
        segment_crc64: segment.segment_crc64,
        segment_okh: segment.segment_okh,
        segment_vid: segment.segment_vid,
        data_pg_id: segment.data_pg_id,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
    }];
    let command_reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "stream-put-finalize-test",
            Some(key.as_str()),
        )
        .unwrap();
    let command_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&command_reservation.record);
    let request_proof = command_proof.clone();
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
            object,
            segments,
            generation_reservation_id: session_id.clone(),
            write_sequence,
            last_modified_millis: 123_460,
            stale_payload: None,
            bucket_write_reservation: command_proof,
            stream_create_bucket_write_reservation: None,
        })),
    );
    let inserted = Arc::new(AtomicBool::new(false));
    let inserted_for_hook = Arc::clone(&inserted);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_command = command.clone();
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if inserted_for_hook.swap(true, Ordering::SeqCst) {
                return;
            }
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &hook_command);
        }));

    let outcome = cluster
        .finalize_put_object_stream(
            &bucket,
            &key,
            &session_id,
            payload.len() as u64,
            request_proof,
            |_| {
                Ok::<_, ()>(crate::PreparedStreamPutCommit {
                    value: "ok",
                    versioning: crate::BucketVersioningState::Disabled,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    size: payload.len() as u64,
                    etag_crc64: payload_crc64,
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

    assert!(inserted.load(Ordering::SeqCst));
    assert_eq!(outcome.value, "ok");
    assert_eq!(outcome.version_id, crate::VersionId::Null);
    assert_eq!(outcome.live_size, payload.len() as u64);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().size, payload.len() as u64);
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn versioned_stream_put_finalize_reserves_object_version_through_command_stream() {
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
    let session_id = crate::SessionId::try_from("76".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let payload = b"versioned stream put finalization";
    let payload_crc64 = checksum::crc64::checksum(payload);
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(payload_crc64),
                segment_okh: [0x76; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let _serial = lock_metadata_command_apply_hook_test();
    let reserve_apply_count = Arc::new(AtomicUsize::new(0));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let reserve_apply_count_hook = Arc::clone(&reserve_apply_count);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, command| {
            if let MetadataCommandPayload::ReserveObjectVersion(reservation) = command.payload() {
                if reservation.bucket == hook_bucket && reservation.key == hook_key {
                    reserve_apply_count_hook.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(())
        },
    ));

    let outcome = cluster
        .finalize_put_object_stream(
            &bucket,
            &key,
            &session_id,
            payload.len() as u64,
            acquire_test_bucket_write_proof(
                &cluster,
                &bucket,
                "stream-put-finalize-test",
                Some(key.as_str()),
            ),
            |_| {
                Ok::<_, ()>(crate::PreparedStreamPutCommit {
                    value: (),
                    versioning: crate::BucketVersioningState::Enabled,
                    owner: crate::OwnerIdentity::from_principal("owner"),
                    acl_grants: crate::AclGrants::default(),
                    public_read: false,
                    size: payload.len() as u64,
                    etag_crc64: payload_crc64,
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
    drop(hook_guard);
    assert_eq!(outcome.version_id, crate::VersionId::from_u64(1));
    assert_eq!(
        reserve_apply_count.load(Ordering::SeqCst),
        node_ids.len(),
        "versioned stream PUT finalization must reserve through the metadata command stream"
    );

    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 2);
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().unwrap();
        assert_eq!(live.version_id, outcome.version_id);
        assert_eq!(live.size, payload.len() as u64);
        assert_eq!(
            pg.object_write_sequence(bucket.as_str(), key.as_str(), outcome.version_id,)
                .unwrap(),
            Some(1),
        );
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
}

#[test]
fn stream_part_finalize_pending_drain_cleans_terminal_stream_session() {
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
    let upload_id = upload_id_from_label("partfinalizedrain");
    let create = crate::CreateMultipartUploadReq {
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
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let session_id = crate::SessionId::try_from("47".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap();
    let payload = b"stream part finalize pending drain allocator cleanup";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                segment_okh: [0x47; 16],
            },
        )
        .unwrap();
    assert_stream_next_segment_vid(&map, NodeId::new(1), object_pg, &session_id, 2);
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitStreamPart(commit)
                    if commit.session_id == hook_session_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected stream part metadata command replica apply failure",
                        source: std::io::Error::other(
                            "injected stream part metadata command replica apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let expected_part = crate::MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: payload.len() as u64,
        etag: vec![0x47; 8],
        etag_kind: crate::EtagKind::Crc64,
        part_okh: [0u8; 16],
        part_vid: crate::GenerationId::MIN,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
        last_modified: 123_456,
        checksum: None,
    };
    let expected_segments = vec![crate::MultipartPartSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number: 1,
        segment_index: segment.segment_index,
        size: segment.size,
        segment_crc64: segment.segment_crc64,
        segment_okh: segment.segment_okh,
        segment_vid: segment.segment_vid,
        data_pg_id: segment.data_pg_id,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
    }];
    let err = cluster
        .finalize_upload_part_stream(&bucket, &key, &upload_id, &session_id, 1, |_| {
            Ok::<_, ()>(crate::PreparedStreamPartCommit {
                value: (),
                part: expected_part.clone(),
                segments: expected_segments.clone(),
            })
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected stream part metadata command replica apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial stream part command must remain pending"
    );
    assert_stream_next_segment_vid(&map, NodeId::new(2), object_pg, &session_id, 2);

    cluster
        .drain_pending_object_metadata_commands_for_bucket(PgId::new(object_pg), &bucket)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap(),
            expected_part
        );
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part_segments_for_upload_part(
                &*pg, &bucket, &key, &upload_id, 1
            )
            .unwrap(),
            expected_segments
        );
    }
}

#[test]
fn stream_part_finalize_matching_pending_install_race_returns_success() {
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
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("partfinalizematch");
    let create = crate::CreateMultipartUploadReq {
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
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let session_id = crate::SessionId::try_from("4b".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload.clone()),
            1,
            &session_id,
        )
        .unwrap();
    assert_bucket_write_reservations_released(&map, &bucket);
    let payload = b"stream part same pending install race";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                segment_okh: [0x4b; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let expected_part = crate::MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: payload.len() as u64,
        etag: vec![0x4b; 8],
        etag_kind: crate::EtagKind::Crc64,
        part_okh: [0u8; 16],
        part_vid: crate::GenerationId::MIN,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
        last_modified: 123_459,
        checksum: None,
    };
    let expected_segments = vec![crate::MultipartPartSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number: 1,
        segment_index: segment.segment_index,
        size: segment.size,
        segment_crc64: segment.segment_crc64,
        segment_okh: segment.segment_okh,
        segment_vid: segment.segment_vid,
        data_pg_id: segment.data_pg_id,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
    }];

    let inserted = Arc::new(AtomicBool::new(false));
    let inserted_for_hook = Arc::clone(&inserted);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_upload = upload.clone();
    let hook_session_id = session_id.clone();
    let hook_part = expected_part.clone();
    let hook_segments = expected_segments.clone();
    let hook_proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-stream-part-terminal-race",
        Some(key.as_str()),
    );
    let pg_id = PgId::new(object_pg);
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if inserted_for_hook.swap(true, Ordering::SeqCst) {
                return;
            }
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    hook_map.test_next_metadata_command_log_index(pg_id),
                ),
                MetadataCommandPayload::CommitStreamPart(Box::new(CommitStreamPartCommand {
                    bucket: hook_bucket.clone(),
                    key: hook_key.clone(),
                    session_id: hook_session_id.clone(),
                    upload: hook_upload.clone(),
                    part: hook_part.clone(),
                    segments: hook_segments.clone(),
                    existing_part: None,
                    displaced_segments: Vec::new(),
                    bucket_write_reservation: hook_proof.clone(),
                })),
            );
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
        }));

    let outcome = cluster
        .finalize_upload_part_stream(&bucket, &key, &upload_id, &session_id, 1, |_| {
            Ok::<_, ()>(crate::PreparedStreamPartCommit {
                value: "ok",
                part: expected_part.clone(),
                segments: expected_segments.clone(),
            })
        })
        .unwrap()
        .unwrap();

    assert!(inserted.load(Ordering::SeqCst));
    assert_eq!(outcome.value, "ok");
    assert_eq!(outcome.last_modified, expected_part.last_modified);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap(),
            expected_part
        );
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn upload_part_stream_finalize_partial_apply_reopens_and_converges() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("open local map");
    let (bucket, key, object_pg, data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(0));
    set_route_primary(&mut map, data_pg, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("partfinalizereopen");
    let create = crate::CreateMultipartUploadReq {
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
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let session_id = crate::SessionId::try_from("4e".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap();
    let payload = b"stream part finalize partial apply survives reopen";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                segment_okh: [0x4e; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let expected_part = crate::MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: payload.len() as u64,
        etag: vec![0x4e; 8],
        etag_kind: crate::EtagKind::Crc64,
        part_okh: [0u8; 16],
        part_vid: crate::GenerationId::MIN,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
        last_modified: 123_459,
        checksum: None,
    };
    let expected_segments = vec![crate::MultipartPartSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number: 1,
        segment_index: segment.segment_index,
        size: segment.size,
        segment_crc64: segment.segment_crc64,
        segment_okh: segment.segment_okh,
        segment_vid: segment.segment_vid,
        data_pg_id: segment.data_pg_id,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
    }];

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_session_id = session_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitStreamPart(commit)
                    if commit.session_id == hook_session_id
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected stream part reopen apply failure",
                        source: std::io::Error::other("injected stream part reopen apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));
    let err = cluster
        .finalize_upload_part_stream(&bucket, &key, &upload_id, &session_id, 1, |_| {
            Ok::<_, ()>(crate::PreparedStreamPartCommit {
                value: (),
                part: expected_part.clone(),
                segments: expected_segments.clone(),
            })
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected stream part reopen apply failure",
                ..
            })
        ),
        "expected injected primary failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial stream part command must remain durable before reopen"
    );
    drop(cluster);
    drop(map);

    let mut reopened_map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("reopen local map");
    set_route_primary(&mut reopened_map, object_pg, NodeId::new(0));
    set_route_primary(&mut reopened_map, data_pg, NodeId::new(2));
    let reopened_map = Arc::new(reopened_map);
    assert!(
        pending_metadata_command_for_test(&reopened_map, PgId::new(object_pg), &bucket).is_none(),
        "open-time recovery should converge and clear the partial stream part command"
    );

    for node_id in node_ids {
        let node = reopened_map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap(),
            expected_part
        );
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part_segments_for_upload_part(
                &*pg, &bucket, &key, &upload_id, 1
            )
            .unwrap(),
            expected_segments
        );
    }
    assert_clean_metadata_command_stream(&reopened_map, &[object_pg]);
}

#[test]
fn upload_part_stream_finalize_finishes_terminal_pending_slot() {
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
    let upload_id = upload_id_from_label("terminalslot");
    let create = crate::CreateMultipartUploadReq {
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
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let session_id = crate::SessionId::try_from("4d".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload.clone()),
            1,
            &session_id,
        )
        .unwrap();
    let payload = b"stream part terminal pending slot";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                segment_okh: [0x4d; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            segment.segment_index,
            &segment,
            &shard_batch,
        )
        .unwrap();

    let part = crate::MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: payload.len() as u64,
        etag: vec![0x4d; 8],
        etag_kind: crate::EtagKind::Crc64,
        part_okh: [0u8; 16],
        part_vid: crate::GenerationId::MIN,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
        last_modified: 123_458,
        checksum: None,
    };
    let segments = vec![crate::MultipartPartSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number: 1,
        segment_index: segment.segment_index,
        size: segment.size,
        segment_crc64: segment.segment_crc64,
        segment_okh: segment.segment_okh,
        segment_vid: segment.segment_vid,
        data_pg_id: segment.data_pg_id,
        ec_k: segment.ec_k,
        ec_m: segment.ec_m,
    }];
    let pg_id = PgId::new(object_pg);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-stream-part-open-converge",
        Some(key.as_str()),
    );
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::CommitStreamPart(Box::new(CommitStreamPartCommand {
            bucket: bucket.clone(),
            key: key.clone(),
            session_id: session_id.clone(),
            upload,
            part: part.clone(),
            segments: segments.clone(),
            existing_part: None,
            displaced_segments: Vec::new(),
            bucket_write_reservation: proof,
        })),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();
    assert!(
        matches!(
            pending_metadata_command_for_test(&map, pg_id, &bucket)
                .as_ref()
                .map(MetadataCommandEnvelope::payload),
            Some(MetadataCommandPayload::CommitStreamPart(commit))
                if commit.matches_request(&bucket, &key, &upload_id, &session_id, 1)
        ),
        "terminal pending slot must survive before retry"
    );

    let err = cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            &upload_id,
            &session_id,
            1,
            |_| -> Result<crate::PreparedStreamPartCommit<()>, ()> {
                panic!("terminal pending slot should finish before rerunning action")
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Metadata(
                crate::MetadataError::StreamSessionNotFound { .. }
            )
        ),
        "expected retry to finish terminal slot then report missing stream session, got {err:?}"
    );
    let leftover = pending_metadata_command_for_test(&map, pg_id, &bucket);
    assert!(leftover.is_none(), "leftover pending command: {leftover:?}");
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap(),
            part
        );
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part_segments_for_upload_part(
                &*pg, &bucket, &key, &upload_id, 1
            )
            .unwrap(),
            segments
        );
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn upload_part_stream_finalize_pending_install_race_reloads_after_abort() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut first_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
        .expect("open first local map");
    let topology = first_map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "upload-part-finalize-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
        .expect("open second local map");
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster = crate::StorageCluster::from_local_map(Arc::clone(&first_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);
    let upload_id = upload_id_from_label("partfinalizeabort");
    let create = crate::CreateMultipartUploadReq {
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
    };
    first_cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();

    let session_id = crate::SessionId::try_from("59".repeat(16)).unwrap();
    let upload = first_cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    first_cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap();

    let payload = b"stream part finalize loses the pending slot to abort";
    let (_target, segment) = first_cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                segment_okh: [0x59; 16],
            },
        )
        .unwrap();
    let written_shards = first_cluster
        .write_stream_segment_payload_shards(&segment, payload)
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

    let pg_id = PgId::new(2);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_upload_id = upload_id.clone();
    let hook_bucket_write_reservation = acquire_test_bucket_write_proof(
        &first_cluster,
        &bucket,
        "abort-multipart-upload",
        Some(key.as_str()),
    );
    let _hook_guard = first_cluster.test_install_before_metadata_command_pending_install_hook(
        Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let cleanup = pg
                .prepare_abort_multipart_upload_cleanup(&hook_bucket, &hook_key, &hook_upload_id)
                .unwrap()
                .expect("upload is still in progress");
            let log_index = pg
                .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap()
                + 1;
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    MetadataCommandLogIndex::new(log_index).unwrap(),
                ),
                MetadataCommandPayload::AbortMultipartUpload(Box::new(
                    AbortMultipartUploadCommand {
                        bucket: hook_bucket.clone(),
                        key: hook_key.clone(),
                        upload_id: hook_upload_id.clone(),
                        cleanup,
                        bucket_write_reservation: hook_bucket_write_reservation.clone(),
                    },
                )),
            );
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
        }),
    );

    let calls_for_action = Arc::clone(&action_calls);
    let err = first_cluster
        .finalize_upload_part_stream(&bucket, &key, &upload_id, &session_id, 1, |snapshot| {
            calls_for_action.fetch_add(1, Ordering::SeqCst);
            let part = crate::MultipartPartRecord {
                upload_id: upload_id.clone(),
                part_number: 1,
                generation: snapshot
                    .existing_part_generation
                    .map_or(0, |generation| generation + 1),
                size: payload.len() as u64,
                etag: vec![0x59; 8],
                etag_kind: crate::EtagKind::Crc64,
                part_okh: [0u8; 16],
                part_vid: crate::GenerationId::MIN,
                ec_k: segment.ec_k,
                ec_m: segment.ec_m,
                last_modified: 123_456,
                checksum: None,
            };
            let segments = snapshot
                .staging_segments
                .iter()
                .map(|staged| crate::MultipartPartSegmentRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    upload_id: upload_id.clone(),
                    version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
                    part_number: 1,
                    segment_index: staged.segment_index,
                    size: staged.size,
                    segment_crc64: staged.segment_crc64,
                    segment_okh: staged.segment_okh,
                    segment_vid: staged.segment_vid,
                    data_pg_id: staged.data_pg_id,
                    ec_k: staged.ec_k,
                    ec_m: staged.ec_m,
                })
                .collect::<Vec<_>>();
            Ok::<_, ()>(crate::PreparedStreamPartCommit {
                value: (),
                part,
                segments,
            })
        })
        .unwrap_err();

    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Metadata(
                crate::MetadataError::StreamSessionNotFound { .. }
            )
        ),
        "expected finalize to reload after abort removed the session, got {err:?}"
    );
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(action_calls.load(Ordering::SeqCst), 1);
    assert!(pending_metadata_command_for_test(&first_map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let node = first_map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1),
            Err(crate::MetadataError::PartNotFound { .. })
        ));
    }

    let mut readback = Vec::new();
    let error = first_cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size: payload.len(),
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                ec: EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            },
            &mut readback,
        )
        .unwrap_err();
    assert!(
        matches!(error, StoreError::NotFound),
        "abort winner must clean staged payload after finalize contention: {error:?}"
    );
    assert_clean_metadata_command_stream(&first_map, &[pg_id.get()]);
    assert_bucket_write_reservations_released(&first_map, &bucket);
}

#[test]
fn upload_part_copy_staged_segments_are_cleaned_when_complete_wins_finalize_slot() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("open local map");
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "upload-part-finalize-complete-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req, mut expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "completewinsfinalize");
    let pg_id = PgId::new(2);
    let completion_order = cluster
        .test_reserve_completed_multipart_upload_order(&bucket)
        .unwrap();

    let session_id = crate::SessionId::try_from("5a".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &req.upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            2,
            &session_id,
        )
        .unwrap();
    // UploadPartCopy stores copied source bytes as ordinary UploadPart stream
    // segments. Use two staged segments so terminal MPU cleanup proves it
    // removes every copied segment payload when completion wins the slot.
    let first_payload = b"copied source segment one";
    let (_target, first_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: first_payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(first_payload)),
                segment_okh: [0x5a; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&first_segment, first_payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            first_segment.segment_index,
            &first_segment,
            &shard_batch,
        )
        .unwrap();
    let second_payload = b"copied source segment two";
    let (_target, second_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 1,
                size: second_payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(second_payload)),
                segment_okh: [0x5d; 16],
            },
        )
        .unwrap();
    let written_shards = cluster
        .write_stream_segment_payload_shards(&second_segment, second_payload)
        .unwrap();
    let shard_batch = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            second_segment.segment_index,
            &second_segment,
            &shard_batch,
        )
        .unwrap();

    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_req = req.clone();
    let hook_session_id = session_id.clone();
    let hook_proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-complete-multipart-race",
        Some(key.as_str()),
    );
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, &hook_req.upload_id)
                .expect("seeded upload is still in progress");
            let parts_count =
                std::num::NonZeroU32::new(u32::try_from(hook_req.part_records.len()).unwrap())
                    .unwrap();
            let object_parts = crate::node_client::complete_multipart_expected_object_parts(
                &hook_req,
                crate::VersionId::Null,
                primary.storage_node().pg_topology(),
            );
            let mut selected_streaming_segments =
                crate::PgMetadataStore::get_all_multipart_part_segments_for_upload(
                    &*pg,
                    &hook_req.upload_id,
                )
                .unwrap();
            for selected in &mut selected_streaming_segments {
                selected.version_id = crate::VersionId::Null.to_u64();
            }
            let active_session = crate::PgMetadataStore::get_stream_upload(&*pg, &hook_session_id)
                .expect("active UploadPart stream session");
            let stream_upload_segments =
                crate::PgMetadataStore::list_stream_segments(&*pg, &hook_session_id)
                    .expect("active UploadPart stream segments");
            let write_sequence = pg
                .next_object_write_sequence(hook_bucket.as_str(), hook_key.as_str())
                .unwrap();
            let log_index = pg
                .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                .unwrap()
                + 1;
            let last_modified_millis = 987_655;
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    MetadataCommandLogIndex::new(log_index).unwrap(),
                ),
                MetadataCommandPayload::CommitMultipartObject(Box::new(
                    CommitMultipartObjectCommand {
                        upload_id: hook_req.upload_id.clone(),
                        bucket_write_reservation: hook_proof.clone(),
                        object: crate::PutLiveObjectReq {
                            bucket: hook_bucket.clone(),
                            key: hook_key.clone(),
                            version_id: crate::VersionId::Null,
                            owner: hook_req.owner.clone(),
                            acl_grants: hook_req.acl_grants.clone(),
                            public_read: hook_req.public_read,
                            generation_id: hook_req.generation_id,
                            size: hook_req.size,
                            etag: crate::ObjectEtag::MultipartComposite {
                                crc64: hook_req.etag_crc64,
                                parts: parts_count,
                            },
                            ec: EcShape { k: 0, m: 0 },
                            layout: crate::ObjectLayout::MultipartManifest { parts_count },
                            tags: hook_req.tags.clone(),
                            metadata_blob: hook_req.metadata_blob.clone(),
                            system_metadata_blob: hook_req.system_metadata_blob.clone(),
                            object_lock: hook_req.object_lock,
                            encryption: hook_req.encryption.clone(),
                        },
                        parts: object_parts,
                        selected_streaming_segments,
                        omitted_parts: Vec::new(),
                        omitted_streaming_segments: Vec::new(),
                        stream_uploads: vec![crate::TerminalStreamCleanupRecord::from(
                            &active_session,
                        )],
                        stream_upload_segments,
                        write_sequence,
                        completion_order,
                        completed_at_millis: last_modified_millis,
                        initiator: upload.initiator.clone(),
                        last_modified_millis,
                        stale_payload: None,
                    },
                )),
            );
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&hook_bucket),
            )
            .unwrap();
        }));

    let calls_for_action = Arc::clone(&action_calls);
    let err = cluster
        .finalize_upload_part_stream(&bucket, &key, &req.upload_id, &session_id, 2, |snapshot| {
            calls_for_action.fetch_add(1, Ordering::SeqCst);
            let part = crate::MultipartPartRecord {
                upload_id: req.upload_id.clone(),
                part_number: 2,
                generation: snapshot
                    .existing_part_generation
                    .map_or(0, |generation| generation + 1),
                size: snapshot
                    .staging_segments
                    .iter()
                    .map(|segment| segment.size)
                    .sum(),
                etag: vec![0x5a; 8],
                etag_kind: crate::EtagKind::Crc64,
                part_okh: [0u8; 16],
                part_vid: crate::GenerationId::MIN,
                ec_k: first_segment.ec_k,
                ec_m: first_segment.ec_m,
                last_modified: 123_457,
                checksum: None,
            };
            let segments = snapshot
                .staging_segments
                .iter()
                .map(|staged| crate::MultipartPartSegmentRecord {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    upload_id: req.upload_id.clone(),
                    version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
                    part_number: 2,
                    segment_index: staged.segment_index,
                    size: staged.size,
                    segment_crc64: staged.segment_crc64,
                    segment_okh: staged.segment_okh,
                    segment_vid: staged.segment_vid,
                    data_pg_id: staged.data_pg_id,
                    ec_k: staged.ec_k,
                    ec_m: staged.ec_m,
                })
                .collect::<Vec<_>>();
            Ok::<_, ()>(crate::PreparedStreamPartCommit {
                value: (),
                part,
                segments,
            })
        })
        .unwrap_err();

    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Metadata(
                crate::MetadataError::StreamSessionNotFound { .. }
            )
        ),
        "expected finalize to reload after complete removed the session, got {err:?}"
    );
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(action_calls.load(Ordering::SeqCst), 1);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());

    expected_segment.version_id = crate::VersionId::Null.to_u64();
    let outcome = crate::CompleteMultipartCommitOutcome {
        version_id: crate::VersionId::Null,
        stale_payload: None,
        live_tags: req.tags.clone(),
        live_size: req.size,
        live_last_modified: 987_655,
    };
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &map,
        &node_ids,
        pg_id.get(),
        &req,
        &expected_segment,
        &outcome,
        1,
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &req.upload_id, 2),
            Err(crate::MetadataError::PartNotFound { .. })
        ));
    }

    for (segment, payload) in [
        (&first_segment, first_payload.as_slice()),
        (&second_segment, second_payload.as_slice()),
    ] {
        let mut readback = Vec::new();
        let error = cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: segment.data_pg_id,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    stored_size: payload.len(),
                    segment_crc64: Some(checksum::crc64::checksum(payload)),
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                },
                &mut readback,
            )
            .unwrap_err();
        assert!(
            matches!(error, StoreError::NotFound),
            "complete winner must clean copied staged payload after finalize contention: {error:?}"
        );
    }
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        pg_id.get(),
        &bucket,
        &key,
        &req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn upload_part_stream_finalize_replaces_same_part_with_displaced_cleanup() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("open local map");
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "upload-part-replace-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("partreplace");
    let create = crate::CreateMultipartUploadReq {
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
    };
    cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>(((), create.clone()))
            },
        )
        .unwrap()
        .unwrap();

    let first_payload = b"first streamed multipart part";
    let (_first_shards, first_part, first_segment) = upload_streamed_test_multipart_part(
        &cluster,
        &bucket,
        &key,
        &upload_id,
        1,
        [0x5b; 16],
        first_payload,
    );
    assert_eq!(first_part.generation, 0);

    let second_payload = b"replacement streamed multipart part";
    let (_second_shards, second_part, second_segment) = upload_streamed_test_multipart_part(
        &cluster,
        &bucket,
        &key,
        &upload_id,
        1,
        [0x5c; 16],
        second_payload,
    );
    assert_eq!(second_part.generation, 1);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(2).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1).unwrap(),
            second_part
        );
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part_segments_for_upload_part(
                &*pg, &bucket, &key, &upload_id, 1
            )
            .unwrap(),
            vec![second_segment.clone()]
        );
    }

    let mut first_readback = Vec::new();
    let first_error = cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: first_segment.data_pg_id,
                segment_okh: first_segment.segment_okh,
                segment_vid: first_segment.segment_vid,
                stored_size: first_payload.len(),
                segment_crc64: Some(checksum::crc64::checksum(first_payload)),
                ec: EcShape {
                    k: first_segment.ec_k,
                    m: first_segment.ec_m,
                },
            },
            &mut first_readback,
        )
        .unwrap_err();
    assert!(
        matches!(first_error, StoreError::NotFound),
        "replacement finalize must clean displaced part payload: {first_error:?}"
    );

    let mut second_readback = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: second_segment.data_pg_id,
                segment_okh: second_segment.segment_okh,
                segment_vid: second_segment.segment_vid,
                stored_size: second_payload.len(),
                segment_crc64: Some(checksum::crc64::checksum(second_payload)),
                ec: EcShape {
                    k: second_segment.ec_k,
                    m: second_segment.ec_m,
                },
            },
            &mut second_readback,
        )
        .unwrap();
    assert_eq!(second_readback, second_payload);

    assert_clean_metadata_command_stream(&map, &[2]);
}

#[test]
fn stream_segment_prepare_uses_durable_session_vid_allocator() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, _object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("7b".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let req = crate::PrepareStreamUploadSegmentAppendReq {
        session_id: session_id.clone(),
        segment_index: 0,
        size: 16,
        segment_crc64: Some(1),
        segment_okh: [42; 16],
    };

    let (_target, first) = cluster
        .prepare_stream_segment_append(&bucket, &key, &req)
        .unwrap();
    let (_target, second) = cluster
        .prepare_stream_segment_append(&bucket, &key, &req)
        .unwrap();

    assert_eq!(first.segment_vid, crate::GenerationId::MIN);
    assert_eq!(second.segment_vid, crate::GenerationId::new(2).unwrap());
    assert_eq!(first.segment_okh, second.segment_okh);
    assert_eq!(first.segment_index, second.segment_index);
}

#[test]
fn stream_segment_prepare_allocates_vid_after_validation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, _object_pg, _data_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let session_id = crate::SessionId::try_from("7c".repeat(16)).unwrap();
    let req = crate::PrepareStreamUploadSegmentAppendReq {
        session_id: session_id.clone(),
        segment_index: 0,
        size: 16,
        segment_crc64: Some(1),
        segment_okh: [42; 16],
    };

    let err = cluster
        .prepare_stream_segment_append(&bucket, &key, &req)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Metadata(crate::MetadataError::StreamSessionNotFound { .. })
    ));

    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();
    let (_target, segment) = cluster
        .prepare_stream_segment_append(&bucket, &key, &req)
        .unwrap();

    assert_eq!(segment.segment_vid, crate::GenerationId::MIN);
}
