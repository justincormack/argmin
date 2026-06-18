use super::*;

#[test]
fn metadata_command_log_checksum_mismatch_prevents_ack() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "checksum-mismatch-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();

    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        node_zero_pg
            .connection()
            .execute(
                "UPDATE metadata_command_log SET command_checksum = ?1 WHERE log_index = 1",
                rusqlite::params![command.checksum_crc64().wrapping_add(1) as i64],
            )
            .unwrap();
    }

    let err = cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogChecksumMismatch {
            node_id: 0,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 1,
            ..
        })
    ));
}

#[test]
fn metadata_command_log_bytes_mismatch_prevents_ack() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "bytes-mismatch-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let command = create_bucket_metadata_command(PgId::new(1), 1, bucket);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();

    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        node_zero_pg
            .connection()
            .execute(
                "UPDATE metadata_command_log SET command_bytes = ?1 WHERE log_index = 1",
                rusqlite::params![b"corrupt-command-bytes".as_slice()],
            )
            .unwrap();
    }

    let err = cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogChecksumMismatch {
            node_id: 0,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            log_index: 1,
            ..
        })
    ));
}

#[test]
fn metadata_state_digest_mismatch_prevents_ack() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let first_bucket = bucket_for_pg(topology, 1, "digest-source-");
    let second_bucket = bucket_for_pg(topology, 1, "digest-next-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let first_command = create_bucket_metadata_command(PgId::new(1), 1, first_bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &first_command)
        .unwrap();

    let expected_digest = {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let expected_digest = node_zero_pg
            .metadata_command_replica_state()
            .unwrap()
            .state_digest;
        node_zero_pg
            .connection()
            .execute(
                "UPDATE buckets SET public_read = 1 WHERE name = ?1",
                rusqlite::params![&first_bucket],
            )
            .unwrap();
        expected_digest
    };

    let second_command = create_bucket_metadata_command(PgId::new(1), 2, second_bucket.clone());
    let err = cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &second_command)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::MetadataStateDigestMismatch {
            node_id: 0,
            pg_id: 1,
            cluster_epoch: ClusterEpoch::INITIAL,
            expected_digest: digest,
            actual_digest: _,
        }) if digest == expected_digest
    ));
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        if node_id == NodeId::new(1) {
            assert!(crate::PgMetadataStore::head_bucket(&*pg, &second_bucket).is_ok());
        } else {
            assert!(matches!(
                crate::PgMetadataStore::head_bucket(&*pg, &second_bucket),
                Err(crate::MetadataError::BucketNotFound { .. })
            ));
        }
    }
}

#[test]
fn cluster_bucket_write_snapshot_uses_durable_reservation_rows() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "durable-write-snapshot-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let second_cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    cluster
        .with_bucket_write_snapshot(&bucket, Default::default(), |snapshot| {
            assert_eq!(snapshot.bucket.name, bucket);
            let primary_pg = map
                .node(NodeId::new(1))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            let reservations =
                crate::PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket)
                    .unwrap();
            assert_eq!(reservations.len(), 1);
            assert_eq!(reservations[0].bucket, bucket);
            assert_eq!(reservations[0].cluster_epoch, crate::ClusterEpoch::INITIAL);
            assert_eq!(reservations[0].operation_kind, "bucket-write-snapshot");
            let _ = crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket).unwrap();
            drop(primary_pg);

            second_cluster
                .with_bucket_write_snapshot(&bucket, Default::default(), |second_snapshot| {
                    assert_eq!(second_snapshot.bucket.name, bucket);
                    let primary_pg = map
                        .node(NodeId::new(1))
                        .unwrap()
                        .storage_node()
                        .get_pg(1)
                        .unwrap();
                    let reservations = crate::PgMetadataStore::durable_bucket_write_reservations(
                        &*primary_pg,
                        &bucket,
                    )
                    .unwrap();
                    assert_eq!(reservations.len(), 2);
                    assert_ne!(
                        reservations[0].reservation_id,
                        reservations[1].reservation_id
                    );
                    let _ = crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket).unwrap();
                    Ok::<(), ()>(())
                })
                .unwrap()
                .unwrap();

            let primary_pg = map
                .node(NodeId::new(1))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            let reservations =
                crate::PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket)
                    .unwrap();
            assert_eq!(reservations.len(), 1);
            let _ = crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket).unwrap();
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket)
            .unwrap()
            .is_empty()
    );
    let _ = crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket).unwrap();
}

#[test]
fn cluster_bucket_write_snapshot_clears_expired_active_delete_drain() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "expired-active-delete-drain-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    {
        let primary_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let now = crate::clock::current_time_millis();
        crate::PgMetadataStore::begin_durable_bucket_write_drain(
            &*primary_pg,
            &bucket,
            "expired-active-delete-drain",
            "abandoned-delete-owner",
            crate::ClusterEpoch::INITIAL,
            now.saturating_sub(10),
            Some(now.saturating_sub(1)),
        )
        .unwrap();
    }

    cluster
        .with_bucket_write_snapshot(&bucket, Default::default(), |snapshot| {
            assert_eq!(snapshot.bucket.name, bucket);
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_drain(&*primary_pg, &bucket)
            .unwrap()
            .is_none(),
        "expired active delete drain should not keep later write snapshots blocked"
    );
}

#[test]
fn low_level_put_object_stream_create_uses_durable_bucket_reservation() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "durable-stream-create-");
    let key = key_for_object_pg(topology, &bucket, 1, "key-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let _hook_guard =
        cluster.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind != crate::cluster::MetadataCommandApplyTestKind::CreateStreamUpload
                || context.bucket.as_ref() != Some(&hook_bucket)
                || context.key.as_ref() != Some(&hook_key)
            {
                return Ok(());
            }
            let primary_pg = hook_map
                .node(NodeId::new(1))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            let reservations = crate::PgMetadataStore::durable_bucket_write_reservations(
                &*primary_pg,
                &hook_bucket,
            )
            .unwrap();
            assert_eq!(reservations.len(), 1);
            assert_eq!(reservations[0].operation_kind, "put-object-stream-create");
            let _ = crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &hook_bucket).unwrap();
            Ok(())
        }));

    let session_id = crate::SessionId::try_from("be".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket).unwrap();
    assert_eq!(reservations.len(), 1);
    let upload = crate::PgMetadataStore::get_stream_upload(&*primary_pg, &session_id).unwrap();
    assert_eq!(
        upload.bucket_write_reservation.as_ref(),
        Some(&crate::metadata_command::BucketWriteReservationProof::from(
            &reservations[0]
        ))
    );
    let _ = crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket).unwrap();
}

#[test]
fn low_level_put_object_stream_create_keeps_reservation_until_pending_converges() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "durable-stream-pending-");
    let key = key_for_object_pg(topology, &bucket, 1, "key-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let failed = Arc::new(AtomicBool::new(false));
    let hook_failed = Arc::clone(&failed);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard =
        cluster.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == crate::cluster::MetadataCommandApplyTestKind::CreateStreamUpload
                && context.node_id == NodeId::new(2)
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && !hook_failed.swap(true, Ordering::SeqCst)
            {
                return Err(StoreError::Io {
                    context: "injected stream-create replica apply failure",
                    source: std::io::Error::other("injected stream-create replica apply failure"),
                });
            }
            Ok(())
        }));

    let session_id = crate::SessionId::try_from("c0".repeat(16)).unwrap();
    let err = cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::Io { .. })
    ));

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket).unwrap();
    assert_eq!(reservations.len(), 1);
    let _ = crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket).unwrap();
    drop(primary_pg);

    drop(hook_guard);
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket).unwrap();
    assert_eq!(reservations.len(), 1);
    let upload = crate::PgMetadataStore::get_stream_upload(&*primary_pg, &session_id).unwrap();
    assert_eq!(
        upload.bucket_write_reservation.as_ref(),
        Some(&crate::metadata_command::BucketWriteReservationProof::from(
            &reservations[0]
        ))
    );
    let _ = crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket).unwrap();
}

#[test]
fn stream_put_create_keeps_reservation_until_pending_converges() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "durable-stream-request-pending-");
    let key = key_for_object_pg(topology, &bucket, 1, "key-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let failed = Arc::new(AtomicBool::new(false));
    let hook_failed = Arc::clone(&failed);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard =
        cluster.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == crate::cluster::MetadataCommandApplyTestKind::CreateStreamUpload
                && context.node_id == NodeId::new(2)
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && !hook_failed.swap(true, Ordering::SeqCst)
            {
                return Err(StoreError::Io {
                    context: "injected request stream-create replica apply failure",
                    source: std::io::Error::other(
                        "injected request stream-create replica apply failure",
                    ),
                });
            }
            Ok(())
        }));

    let session_id = crate::SessionId::try_from("d0".repeat(16)).unwrap();
    let err = cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_, _| {
                Ok::<_, ()>((
                    (),
                    crate::CreateStreamUploadReq {
                        session_id: session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: crate::StreamUploadTarget::PutObject,
                        encryption: crate::ObjectEncryption::None,
                    },
                ))
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::Io { .. })
    ));

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket).unwrap();
    assert_eq!(reservations.len(), 1);
    assert_eq!(reservations[0].operation_kind, "put-object-stream-create");
    let _ = crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket).unwrap();
    drop(primary_pg);

    drop(hook_guard);
    cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_, _| {
                Ok::<_, ()>((
                    (),
                    crate::CreateStreamUploadReq {
                        session_id: session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: crate::StreamUploadTarget::PutObject,
                        encryption: crate::ObjectEncryption::None,
                    },
                ))
            },
        )
        .unwrap()
        .unwrap();

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket).unwrap();
    assert_eq!(reservations.len(), 1);
    let upload = crate::PgMetadataStore::get_stream_upload(&*primary_pg, &session_id).unwrap();
    assert_eq!(
        upload.bucket_write_reservation.as_ref(),
        Some(&crate::metadata_command::BucketWriteReservationProof::from(
            &reservations[0]
        ))
    );
    let _ = crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket).unwrap();
}

#[test]
fn stream_put_create_preserves_reservation_when_pending_owner_check_fails() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-owner-check-fail-");
    let key = key_for_object_pg(topology, &bucket, 1, "key-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let failed = Arc::new(AtomicBool::new(false));
    let hook_failed = Arc::clone(&failed);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let _hook_guard =
        cluster.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind != crate::cluster::MetadataCommandApplyTestKind::CreateStreamUpload
                || context.bucket.as_ref() != Some(&hook_bucket)
                || context.key.as_ref() != Some(&hook_key)
                || hook_failed.swap(true, Ordering::SeqCst)
            {
                return Ok(());
            }
            let primary_pg = hook_map
                .node(NodeId::new(1))
                .unwrap()
                .storage_node()
                .get_pg(1)
                .unwrap();
            primary_pg
                .connection()
                .execute(
                    "UPDATE metadata_command_pending_slot SET command_bytes = ?1",
                    rusqlite::params![vec![0_u8]],
                )
                .unwrap();
            Err(StoreError::Io {
                context: "injected request stream-create apply failure after corrupt pending",
                source: std::io::Error::other(
                    "injected request stream-create apply failure after corrupt pending",
                ),
            })
        }));

    let session_id = crate::SessionId::try_from("d1".repeat(16)).unwrap();
    let err = cluster
        .create_put_object_stream_session(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_, _| {
                Ok::<_, ()>((
                    (),
                    crate::CreateStreamUploadReq {
                        session_id: session_id.clone(),
                        bucket: bucket.clone(),
                        key: key.clone(),
                        target: crate::StreamUploadTarget::PutObject,
                        encryption: crate::ObjectEncryption::None,
                    },
                ))
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(
            StoreError::MetadataCommandLogChecksumMismatch { .. }
        )
    ));

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert_eq!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket)
            .unwrap()
            .len(),
        1,
        "ownership-check failures must preserve reservation proof for recovery"
    );
}

#[test]
fn live_put_object_stream_create_keeps_reservation_and_clears_pending_slot() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "live-stream-release-fail-");
    let key = key_for_object_pg(topology, &bucket, 1, "key-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "put-object-stream-create",
            Some(key.as_str()),
        )
        .unwrap();
    let proof = crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);
    let session_id = crate::SessionId::try_from("c3".repeat(16)).unwrap();
    let request = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };
    let pg_id = PgId::new(1);
    let command = MetadataCommandEnvelope::new(
            cluster.next_object_metadata_command_id(pg_id).unwrap(),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    request,
                    crate::clock::current_time_millis(),
                    proof,
                ),
            )),
        );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();

    assert_eq!(
        cluster
            .finish_exact_pending_object_metadata_command(
                pg_id,
                crate::cluster::ExactPendingObjectMetadataCommand::for_checked_request(&command),
            )
            .unwrap(),
        crate::cluster::PendingMetadataCommandOutcome::Applied
    );
    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_none(),
        "applied live stream create should clear its pending slot"
    );
    {
        let primary_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        assert_eq!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket)
                .unwrap()
                .len(),
            1,
            "live direct PUT stream create must keep its bucket-write proof until abort/finalize"
        );
    }
    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let session = crate::PgMetadataStore::get_stream_upload(&*primary_pg, &session_id).unwrap();
    assert_eq!(session.bucket, bucket);
    assert_eq!(session.key, key);
    assert_eq!(
        session.bucket_write_reservation.as_ref(),
        Some(&crate::metadata_command::BucketWriteReservationProof::from(
            &reservation.record
        ))
    );
}

#[test]
fn put_object_stream_create_open_time_convergence_preserves_bucket_write_reservation() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-open-release-");
    let key = key_for_object_pg(topology, &bucket, 1, "key-");

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let failed = Arc::new(AtomicBool::new(false));
    let hook_failed = Arc::clone(&failed);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard =
        cluster.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
            if context.kind == crate::cluster::MetadataCommandApplyTestKind::CreateStreamUpload
                && context.node_id == NodeId::new(2)
                && context.bucket.as_ref() == Some(&hook_bucket)
                && context.key.as_ref() == Some(&hook_key)
                && !hook_failed.swap(true, Ordering::SeqCst)
            {
                return Err(StoreError::Io {
                    context: "injected stream-create reopen failure",
                    source: std::io::Error::other("injected stream-create reopen failure"),
                });
            }
            Ok(())
        }));

    let session_id = crate::SessionId::try_from("c2".repeat(16)).unwrap();
    let err = cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::Io { .. })
    ));
    drop(hook_guard);

    let primary_pg = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let reservation =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket)
            .unwrap()
            .pop()
            .expect("partial stream-create command should keep reservation live");
    drop(primary_pg);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_some(),
        "partial stream-create command should remain pending"
    );

    drop(cluster);
    drop(map);

    let reopened = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
        .expect("reopen local map with in-flight stream create");
    let reopened = Arc::new(reopened);
    assert!(
        pending_metadata_command_for_test(&reopened, PgId::new(1), &bucket).is_none(),
        "open-time recovery should converge and clear the stream-create pending slot"
    );
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
        assert_eq!(session.bucket, bucket);
        assert_eq!(session.key, key);
    }
    let primary_pg = reopened
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket).unwrap();
    assert_eq!(
        reservations,
        vec![reservation],
        "open-time direct stream-create recovery must preserve the live writer proof"
    );
    drop(primary_pg);
    assert_clean_metadata_command_stream(&reopened, &[1]);
}

#[test]
fn stream_create_command_rejects_missing_bucket_write_reservation_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-proof-missing-");
    let key = key_for_object_pg(topology, &bucket, 2, "key-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "put-object-stream-create",
            Some(key.as_str()),
        )
        .unwrap();
    let proof = crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);
    cluster
        .release_durable_bucket_write_reservation(reservation)
        .unwrap();

    let session_id = crate::SessionId::try_from("c1".repeat(16)).unwrap();
    let request = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };
    let command = MetadataCommandEnvelope::new(
            cluster
                .next_object_metadata_command_id(PgId::new(2))
                .unwrap(),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    request,
                    crate::clock::current_time_millis(),
                    proof,
                ),
            )),
        );

    let err = cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Metadata(
            crate::MetadataError::BucketWriteReservationNotFound { .. }
        )
    ));
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
}

#[test]
fn stream_create_command_rejects_stale_bucket_incarnation_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-proof-stale-");
    let key = key_for_object_pg(topology, &bucket, 2, "key-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "put-object-stream-create",
            Some(key.as_str()),
        )
        .unwrap();
    let proof = crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);
    {
        let bucket_pg = map
            .node(NodeId::new(1))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        bucket_pg
            .connection()
            .execute(
                "UPDATE buckets \
                     SET bucket_incarnation_generation = ?1 \
                     WHERE name = ?2",
                rusqlite::params![
                    (proof.bucket_incarnation_generation + 1) as i64,
                    bucket.as_str(),
                ],
            )
            .unwrap();
    }

    let session_id = crate::SessionId::try_from("c4".repeat(16)).unwrap();
    let request = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };
    let command = MetadataCommandEnvelope::new(
            cluster
                .next_object_metadata_command_id(PgId::new(2))
                .unwrap(),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    request,
                    crate::clock::current_time_millis(),
                    proof,
                ),
            )),
        );

    let err = cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Metadata(
            crate::MetadataError::BucketWriteReservationConflict { .. }
        )
    ));
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
}

#[test]
fn stream_create_command_allows_bucket_metadata_generation_change() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-proof-metadata-gen-");
    let key = key_for_object_pg(topology, &bucket, 2, "key-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "put-object-stream-create",
            Some(key.as_str()),
        )
        .unwrap();
    let proof = crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);
    let updated = cluster
        .put_bucket_versioning_and_load_info(&bucket, crate::BucketVersioningState::Enabled)
        .unwrap();
    assert!(
        updated.bucket_execution_generation > proof.bucket_execution_generation,
        "bucket control-plane updates should advance metadata generation"
    );
    assert_eq!(
        updated.bucket_incarnation_generation, proof.bucket_incarnation_generation,
        "bucket control-plane updates must not change bucket incarnation"
    );

    let session_id = crate::SessionId::try_from("c5".repeat(16)).unwrap();
    let request = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::PutObject,
        encryption: crate::ObjectEncryption::None,
    };
    let command = MetadataCommandEnvelope::new(
            cluster
                .next_object_metadata_command_id(PgId::new(2))
                .unwrap(),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    request,
                    crate::clock::current_time_millis(),
                    proof,
                ),
            )),
        );

    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(2).unwrap();
        let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
        assert_eq!(session.bucket, bucket);
        assert_eq!(session.key, key);
    }
}

#[test]
fn low_level_put_object_stream_create_retries_durable_delete_drain() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-create-drain-");
    let key = key_for_object_pg(topology, &bucket, 1, "key-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let primary_node = Arc::clone(map.node(NodeId::new(1)).unwrap().storage_node());
    let drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        super::super::super::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        super::super::super::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("active test bucket should acquire a temporary durable delete drain")
        }
    };
    let retry_cluster = Arc::clone(&cluster);
    let retry_bucket = bucket.clone();
    let retry_key = key.clone();
    let session_id = crate::SessionId::try_from("bf".repeat(16)).unwrap();
    let retry_session_id = session_id.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let handle = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = retry_cluster.create_put_object_stream_session_record(
            &retry_bucket,
            &retry_key,
            &retry_session_id,
            crate::ObjectEncryption::None,
        );
        result_tx.send(result).unwrap();
    });
    started_rx.recv().unwrap();
    assert!(result_rx
        .recv_timeout(std::time::Duration::from_millis(50))
        .is_err());

    cluster.clear_durable_bucket_delete_drain(&drain).unwrap();
    let result = result_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("temporary durable drain should be retried after release");
    result.unwrap();
    handle.join().unwrap();

    let primary_pg = primary_node.get_pg(1).unwrap();
    let upload = crate::PgMetadataStore::get_stream_upload(&*primary_pg, &session_id).unwrap();
    assert_eq!(upload.bucket, bucket);
    let reservations =
        crate::PgMetadataStore::durable_bucket_write_reservations(&*primary_pg, &bucket).unwrap();
    assert_eq!(reservations.len(), 1);
    assert_eq!(
        upload.bucket_write_reservation.as_ref(),
        Some(&crate::metadata_command::BucketWriteReservationProof::from(
            &reservations[0]
        ))
    );
    let _ = crate::PgMetadataStore::head_bucket_raw(&*primary_pg, &bucket).unwrap();
}

#[test]
fn low_level_put_object_stream_create_bounds_durable_delete_drain_wait() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "stream-create-held-drain-");
    let key = key_for_object_pg(topology, &bucket, 1, "key-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let _drain = match cluster.begin_durable_bucket_delete_drain(&bucket).unwrap() {
        super::super::super::DurableBucketDeleteDrainBegin::Acquired(drain) => drain,
        super::super::super::DurableBucketDeleteDrainBegin::AlreadyDeleting => {
            panic!("active test bucket should acquire a temporary durable delete drain")
        }
    };

    let session_id = crate::SessionId::try_from("c0".repeat(16)).unwrap();
    let started = std::time::Instant::now();
    let err = cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap_err();
    assert!(
        started.elapsed() < Duration::from_secs(12),
        "held durable drain should return within the stream-create retry budget"
    );
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
                context: "put object stream create retry budget exhausted",
            })
        ),
        "held durable drain should return retryable contention, got {err:?}"
    );

    let primary_pg = map
        .node(NodeId::new(1))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::get_stream_upload(&*primary_pg, &session_id).is_err(),
        "timed out stream create must not publish a session"
    );
}

#[test]
fn metadata_state_digest_covers_completed_multipart_order_sequence() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let allocated_bucket = bucket_for_pg(topology, 1, "digest-mpu-order-");
    let next_bucket = bucket_for_pg(topology, 1, "digest-after-mpu-order-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let first_command = create_bucket_metadata_command(PgId::new(1), 1, allocated_bucket.clone());
    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &first_command)
        .unwrap();

    {
        let node_zero_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(1)
            .unwrap();
        node_zero_pg
            .advance_completed_multipart_upload_sequence_for_bucket(&allocated_bucket, 7)
            .unwrap();
        assert_eq!(
            node_zero_pg
                .completed_multipart_upload_sequence_for_bucket(&allocated_bucket)
                .unwrap(),
            7
        );
    }

    let second_command = create_bucket_metadata_command(PgId::new(1), 2, next_bucket.clone());
    let err = cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &second_command)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Store(StoreError::MetadataStateDigestMismatch { .. })
        ),
        "expected direct completed-MPU sequence mutation to trip digest mismatch, got {err:?}"
    );
}

#[test]
fn local_cluster_reopen_rejects_large_command_stream_materialized_tamper() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap();
    set_route_primary(&mut map, 1, NodeId::new(1));
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology()
        .clone();
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let mut first_bucket = None;

    for log_index in 1..=129 {
        let bucket = bucket_for_pg(&topology, 1, &format!("digest-verified-{log_index}-"));
        if first_bucket.is_none() {
            first_bucket = Some(bucket.clone());
        }
        let command = create_bucket_metadata_command(PgId::new(1), log_index, bucket.clone());
        cluster
            .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
            .unwrap();
    }

    let node_zero_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    let state = node_zero_pg.metadata_command_replica_state().unwrap();
    assert_eq!(state.applied_log_index, 129);
    assert_ne!(state.state_digest, 0);
    node_zero_pg
        .connection()
        .execute(
            "UPDATE buckets SET public_read = 1 WHERE name = ?1",
            rusqlite::params![first_bucket.unwrap().as_str()],
        )
        .unwrap();

    drop(node_zero_pg);
    drop(cluster);
    drop(map);

    let err = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1], ec_shape).unwrap_err();
    assert!(
        matches!(
            err,
            ClusterBuildError::OpenLocalNode {
                node_id: 0,
                source: StoreError::MetadataStateDigestMismatch { pg_id: 1, .. }
            }
        ),
        "unexpected reopen error: {err:?}"
    );
}

#[test]
fn direct_put_registers_payload_acks_on_routed_data_pg_primary() {
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
    let reservation_id = crate::SessionId::try_from("02".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    assert_eq!(generation_id, crate::GenerationId::MIN);

    let payload = b"direct put payload with routed data acks";
    let segment_okh = [62; 16];
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
    assert_eq!(written.data_pg_id, data_pg);

    let commit_req = crate::CommitDirectPutObjectReq {
        bucket: bucket.clone(),
        key: key.clone(),
        generation_reservation_id: reservation_id,
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
        bucket_write_reservation: acquire_test_bucket_write_proof(
            &cluster,
            &bucket,
            "direct-put-commit-test",
            Some(key.as_str()),
        ),
    };
    cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

    let first_shard_key = &written.written_shards[0].key;
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
                segment_okh,
                segment_vid: generation_id,
                stored_size: payload.len(),
                segment_crc64: checksum::crc64::checksum(payload),
                ec: written.ec,
            },
            &mut readback,
        )
        .unwrap();
    assert_eq!(readback, payload);
}

#[test]
fn object_generation_reservation_commands_apply_to_all_acting_object_pg_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "object-reservation-");
        let object_pg = 2;
        let key = key_for_object_pg(topology, &bucket, object_pg, "key-");
        (bucket, key, object_pg)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let reservation_id = crate::SessionId::try_from("12".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    assert_eq!(generation_id, crate::GenerationId::MIN);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id,
            )
            .unwrap(),
            generation_id
        );
    }

    cluster
        .release_object_generation_reservation(&bucket, &key, &reservation_id)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id,
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn object_generation_reservation_drains_other_bucket_pending_reserve_without_stealing_it() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (first_bucket, first_key, second_bucket, second_key, object_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let first_bucket = bucket_for_pg(topology, 1, "pending-reserve-first-");
        let second_bucket = bucket_for_pg(topology, 1, "pending-reserve-second-");
        let object_pg = 2;
        let first_key = key_for_object_pg(topology, &first_bucket, object_pg, "key-");
        let second_key = key_for_object_pg(topology, &second_bucket, object_pg, "key-");
        (
            first_bucket,
            first_key,
            second_bucket,
            second_key,
            object_pg,
        )
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &first_bucket);
    create_test_bucket(&cluster, &second_bucket);

    let pg_id = PgId::new(object_pg);
    let first_reservation_id = crate::SessionId::try_from("65".repeat(16)).unwrap();
    let second_reservation_id = crate::SessionId::try_from("66".repeat(16)).unwrap();
    let pending = MetadataCommandEnvelope::new(
        crate::metadata_command::MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(
            crate::metadata_command::ReserveObjectGenerationCommand::new(
                first_bucket.clone(),
                first_key.clone(),
                first_reservation_id.clone(),
                crate::GenerationId::MIN,
                123,
            ),
        ),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &first_bucket, &pending);

    let second_generation = cluster
        .reserve_put_object_generation(&second_bucket, &second_key, &second_reservation_id)
        .unwrap();
    assert_eq!(second_generation, crate::GenerationId::MIN);
    assert!(pending_metadata_command_for_test(&map, pg_id, &second_bucket).is_none());
    let retried_first_generation = cluster
        .reserve_put_object_generation(&first_bucket, &first_key, &first_reservation_id)
        .unwrap();
    assert_eq!(
            retried_first_generation,
            crate::GenerationId::MIN,
            "the original request must recover its applied reservation after another request drained its pending slot"
        );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &first_bucket,
                &first_key,
                &first_reservation_id,
            )
            .unwrap(),
            crate::GenerationId::MIN
        );
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &second_bucket,
                &second_key,
                &second_reservation_id,
            )
            .unwrap(),
            second_generation
        );
    }
}

#[test]
fn object_generation_exact_pending_reissue_conflict_returns_contention() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, other_bucket, other_key, object_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "pending-gen-reissue-main-");
        let other_bucket = bucket_for_pg(topology, 1, "pending-gen-reissue-other-");
        let object_pg = 2;
        let key = key_for_object_pg(topology, &bucket, object_pg, "key-");
        let other_key = key_for_object_pg(topology, &other_bucket, object_pg, "key-");
        (bucket, key, other_bucket, other_key, object_pg)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    create_test_bucket(&cluster, &other_bucket);

    let pg_id = PgId::new(object_pg);
    let reservation_id = crate::SessionId::try_from("67".repeat(16)).unwrap();
    let other_reservation_id = crate::SessionId::try_from("68".repeat(16)).unwrap();
    let later_reservation_id = crate::SessionId::try_from("69".repeat(16)).unwrap();
    let pending = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket.clone(),
            key.clone(),
            reservation_id.clone(),
            crate::GenerationId::MIN,
            123,
        )),
    );
    let other_first = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            other_bucket.clone(),
            other_key.clone(),
            other_reservation_id,
            crate::GenerationId::MIN,
            123,
        )),
    );
    let other_second = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            other_bucket.clone(),
            other_key.clone(),
            later_reservation_id.clone(),
            crate::GenerationId::new(2).unwrap(),
            124,
        )),
    );

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        pg.apply_metadata_command_and_record(node_id.as_u32(), &other_first)
            .unwrap();
        if node_id == NodeId::new(0) {
            pg.apply_metadata_command_and_record(node_id.as_u32(), &other_second)
                .unwrap();
        }
    }
    force_insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending);

    let err = cluster
        .reserve_put_object_generation(&other_bucket, &other_key, &later_reservation_id)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
                context: "retryable partial pending object metadata drain",
            })
        ),
        "expected retryable drain contention, got {err:?}"
    );

    let err = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::MetadataCommandContention {
                context: "retryable partial pending generation reservation command",
            })
        ),
        "expected retryable generation reservation contention, got {err:?}"
    );
}

#[test]
fn object_version_reservation_drains_stale_other_bucket_pending_reserve() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (first_bucket, first_key, second_bucket, second_key, object_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let first_bucket = bucket_for_pg(topology, 1, "pending-version-first-");
        let second_bucket = bucket_for_pg(topology, 1, "pending-version-second-");
        let object_pg = 2;
        let first_key = key_for_object_pg(topology, &first_bucket, object_pg, "key-");
        let second_key = key_for_object_pg(topology, &second_bucket, object_pg, "key-");
        (
            first_bucket,
            first_key,
            second_bucket,
            second_key,
            object_pg,
        )
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &first_bucket);
    create_test_bucket(&cluster, &second_bucket);

    let pg_id = PgId::new(object_pg);
    let first_version = cluster
        .reserve_next_object_version(pg_id, &first_bucket, &first_key)
        .unwrap();
    assert_eq!(first_version, crate::VersionId::from_u64(1));

    let stale = MetadataCommandEnvelope::new(
        crate::metadata_command::MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
            first_bucket.clone(),
            first_key.clone(),
            first_version,
        )),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &first_bucket, &stale);

    let second_version = cluster
        .reserve_next_object_version(pg_id, &second_bucket, &second_key)
        .unwrap();
    assert_eq!(second_version, crate::VersionId::from_u64(1));
    assert!(pending_metadata_command_for_test(&map, pg_id, &second_bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::next_version_id(&*pg, &first_bucket, &first_key).unwrap(),
            crate::VersionId::from_u64(first_version.to_u64() + 1)
        );
        assert_eq!(
            crate::PgMetadataStore::next_version_id(&*pg, &second_bucket, &second_key).unwrap(),
            crate::VersionId::from_u64(second_version.to_u64() + 1)
        );
    }
}

#[test]
fn release_object_generation_reservation_drains_intervening_pg_slot_without_stealing_it() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (first_bucket, first_key, second_bucket, second_key, object_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let first_bucket = bucket_for_pg(topology, 1, "release-drain-first-");
        let second_bucket = bucket_for_pg(topology, 1, "release-drain-second-");
        let object_pg = 2;
        let first_key = key_for_object_pg(topology, &first_bucket, object_pg, "key-");
        let second_key = key_for_object_pg(topology, &second_bucket, object_pg, "key-");
        (
            first_bucket,
            first_key,
            second_bucket,
            second_key,
            object_pg,
        )
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &first_bucket);
    create_test_bucket(&cluster, &second_bucket);

    let pg_id = PgId::new(object_pg);
    let first_reservation_id = crate::SessionId::try_from("67".repeat(16)).unwrap();
    let second_reservation_id = crate::SessionId::try_from("68".repeat(16)).unwrap();
    assert_eq!(
        cluster
            .reserve_put_object_generation(&first_bucket, &first_key, &first_reservation_id)
            .unwrap(),
        crate::GenerationId::MIN
    );
    let pending = MetadataCommandEnvelope::new(
        crate::metadata_command::MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(
            crate::metadata_command::ReserveObjectGenerationCommand::new(
                second_bucket.clone(),
                second_key.clone(),
                second_reservation_id.clone(),
                crate::GenerationId::MIN,
                123,
            ),
        ),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &second_bucket, &pending);

    cluster
        .release_object_generation_reservation(&first_bucket, &first_key, &first_reservation_id)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, pg_id, &first_bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &first_bucket,
                &first_key,
                &first_reservation_id,
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &second_bucket,
                &second_key,
                &second_reservation_id,
            )
            .unwrap(),
            crate::GenerationId::MIN
        );
    }
}

#[test]
fn required_reservation_release_drains_intervening_pg_slot_without_stealing_it() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (first_bucket, first_key, second_bucket, second_key, object_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let first_bucket = bucket_for_pg(topology, 1, "required-release-first-");
        let second_bucket = bucket_for_pg(topology, 1, "required-release-second-");
        let object_pg = 2;
        let first_key = key_for_object_pg(topology, &first_bucket, object_pg, "key-");
        let second_key = key_for_object_pg(topology, &second_bucket, object_pg, "key-");
        (
            first_bucket,
            first_key,
            second_bucket,
            second_key,
            object_pg,
        )
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &first_bucket);
    create_test_bucket(&cluster, &second_bucket);

    let pg_id = PgId::new(object_pg);
    let first_reservation_id = crate::SessionId::try_from("69".repeat(16)).unwrap();
    let second_reservation_id = crate::SessionId::try_from("6a".repeat(16)).unwrap();
    assert_eq!(
        cluster
            .reserve_put_object_generation(&first_bucket, &first_key, &first_reservation_id)
            .unwrap(),
        crate::GenerationId::MIN
    );
    let pending = MetadataCommandEnvelope::new(
        crate::metadata_command::MetadataCommandId::new(
            cluster.operation_epoch(),
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::ReserveObjectGeneration(
            crate::metadata_command::ReserveObjectGenerationCommand::new(
                second_bucket.clone(),
                second_key.clone(),
                second_reservation_id.clone(),
                crate::GenerationId::MIN,
                123,
            ),
        ),
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &second_bucket, &pending);

    cluster
        .release_object_generation_reservation_command_required(
            pg_id,
            &first_bucket,
            &first_key,
            &first_reservation_id,
        )
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, pg_id, &first_bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &first_bucket,
                &first_key,
                &first_reservation_id,
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &second_bucket,
                &second_key,
                &second_reservation_id,
            )
            .unwrap(),
            crate::GenerationId::MIN
        );
    }
}

#[test]
fn required_reservation_release_keeps_durable_slot_until_partial_apply_retry() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "required-release-partial-");
        let object_pg = 2;
        let key = key_for_object_pg(topology, &bucket, object_pg, "key-");
        (bucket, key, object_pg)
    };
    set_route_primary(&mut map, object_pg, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let pg_id = PgId::new(object_pg);
    let reservation_id = crate::SessionId::try_from("6b".repeat(16)).unwrap();
    assert_eq!(
        cluster
            .reserve_put_object_generation(&bucket, &key, &reservation_id)
            .unwrap(),
        crate::GenerationId::MIN
    );

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_reservation_id = reservation_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let _hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            if matches!(
                command.payload(),
                MetadataCommandPayload::ReleaseObjectGeneration(release)
                    if release.matches_request(
                        &hook_bucket,
                        &hook_key,
                        &hook_reservation_id
                    ) && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst)
            ) {
                return Err(StoreError::Io {
                    context: "injected required release apply failure",
                    source: std::io::Error::other("injected required release apply failure"),
                });
            }
            Ok(())
        },
    ));

    let err = cluster
        .release_object_generation_reservation_command_required(
            pg_id,
            &bucket,
            &key,
            &reservation_id,
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected required release apply failure",
                ..
            })
        ),
        "expected injected release failure, got {err:?}"
    );
    assert!(!fail_once.load(Ordering::SeqCst));
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let slot = pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .expect("partial required release should leave durable primary pending slot");
        assert_eq!(slot.scope_bucket.as_ref(), Some(&bucket));
    }

    cluster
        .release_object_generation_reservation_command_required(
            pg_id,
            &bucket,
            &key,
            &reservation_id,
        )
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        assert!(pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &reservation_id,
            ),
            Err(crate::MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}

#[test]
fn direct_put_metadata_command_applies_to_all_acting_object_pg_nodes() {
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
    let reservation_id = crate::SessionId::try_from("13".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put metadata command replication";
    let segment_okh = [63; 16];
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
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();

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
}

#[test]
fn already_recorded_direct_put_fanout_cleans_terminal_stream_uploads() {
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
    let reservation_id = crate::SessionId::try_from("68".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put already-recorded fanout cleanup";
    let segment_okh = [68; 16];
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
    let primary = map.node(NodeId::new(1)).unwrap().storage_node();
    let primary_pg = primary.get_pg(object_pg).unwrap();
    let command = cluster
        .prepare_commit_direct_put_object_command(
            PgId::new(object_pg),
            &primary_pg,
            &commit_req,
            crate::VersionId::Null,
            commit_req.bucket_write_reservation.clone(),
        )
        .unwrap();
    drop(primary_pg);

    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        crate::PgMetadataStore::create_stream_upload(
            &*pg,
            &crate::CreateStreamUploadReq {
                session_id: reservation_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: crate::StreamUploadTarget::PutObject,
                encryption: crate::ObjectEncryption::None,
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &reservation_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn stream_put_staging_commands_apply_to_all_acting_object_pg_nodes() {
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
    let session_id = crate::SessionId::try_from("32".repeat(16)).unwrap();
    cluster
        .create_put_object_stream_session_record(
            &bucket,
            &key,
            &session_id,
            crate::ObjectEncryption::None,
        )
        .unwrap();

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
        assert_eq!(session.bucket, bucket);
        assert_eq!(session.key, key);
        assert!(matches!(
            session.target,
            crate::StreamUploadTarget::PutObject
        ));
    }

    let payload = b"stream staging command replication";
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(payload),
                segment_okh: [88; 16],
            },
        )
        .unwrap();
    assert_eq!(segment.data_pg_id, data_pg);
    let written_shards = cluster
        .write_stream_segment_payload_shards(&segment, payload)
        .unwrap();
    let shard_batch: Vec<(&crate::ShardKey, crate::WriteAck)> = written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
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

    cluster
        .abort_stream_upload_session(&bucket, &key, &session_id)
        .unwrap();
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

    let mut readback = Vec::new();
    assert!(cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: segment.data_pg_id,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                stored_size: payload.len(),
                segment_crc64: checksum::crc64::checksum(payload),
                ec: EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            },
            &mut readback,
        )
        .is_err());
}
