use super::*;

#[test]
fn multipart_upload_lookup_fails_closed_while_metadata_pg_is_peering() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap());
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::BucketName::try_from("bucket".to_string()).unwrap();
    let key = crate::ObjectKey::try_from("key".to_string()).unwrap();
    let object_pg = cluster.object_metadata_pg_id(&bucket, &key);
    let upload_id = upload_id_from_label("peeringlookuplock");
    create_test_bucket(&cluster, &bucket);
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
    drop(cluster);

    Arc::get_mut(&mut map)
        .unwrap()
        .pg_routes
        .get_mut(&PgId::new(object_pg))
        .unwrap()
        .state = PgState::Peering;
    let cluster = crate::StorageCluster::from_local_map(map).unwrap();

    let err = cluster
        .load_multipart_upload(&bucket, &key, &upload_id)
        .unwrap_err();
    assert!(matches!(
        err,
        crate::BucketSnapshotLoadError::Store(StoreError::PgNotActive {
            pg_id,
            cluster_epoch: ClusterEpoch::INITIAL,
            state: PgState::Peering,
        }) if pg_id == object_pg
    ));
}

#[test]
fn multipart_create_pending_install_race_reruns_authorization_action() {
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
    let bucket = bucket_for_pg(topology, 1, "mpu-create-pending-race-");
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

    let winner_payload = b"winner before multipart create";
    let winner_reservation_id =
        crate::SessionId::try_from("77777777777777777777777777777777".to_string()).unwrap();
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

    let upload_id = upload_id_from_label("mpucreatependingrace");
    let calls_for_action = Arc::clone(&action_calls);
    let result = first_cluster
        .create_multipart_upload(
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
                }
            },
        )
        .unwrap();
    assert!(matches!(result, Err("object already exists")));
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        2,
        "multipart create authorization must be rerun after slot contention changes object state"
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
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
    }
}

#[test]
fn multipart_create_command_id_race_drains_winner_and_reruns_authorization_action() {
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
    let bucket = bucket_for_pg(topology, 1, "mpu-create-id-race-");
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

    let winner_payload = b"winner before multipart create command id";
    let winner_reservation_id =
        crate::SessionId::try_from("78787878787878787878787878787878".to_string()).unwrap();
    let winner_generation_id = second_cluster
        .reserve_put_object_generation(&bucket, &key, &winner_reservation_id)
        .unwrap();
    let winner_written = second_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            winner_generation_id,
            0,
            &[0x97; 16],
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
            segment_okh: [0x97; 16],
            written: &winner_written,
        },
    );
    let winner_shard_batch: Vec<(&ShardKey, WriteAck)> = winner_written
        .written_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect();
    second_cluster
        .register_payload_shard_acks(winner_req.data_pg_id, &winner_shard_batch)
        .unwrap();

    let slot_installed = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let install_map = Arc::clone(&second_map);
    let install_cluster = Arc::clone(&second_cluster);
    let install_bucket = bucket.clone();
    let install_req = winner_req.clone();
    let install_once = Arc::clone(&slot_installed);
    let calls_for_action = Arc::clone(&action_calls);
    let upload_id = upload_id_from_label("mpucreateidrace");
    let result = first_cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_, existing_object| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                if existing_object.is_some() {
                    return Err("object already exists");
                }
                if !install_once.swap(true, Ordering::SeqCst) {
                    let pg_id = PgId::new(2);
                    let primary = install_map
                        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                        .unwrap();
                    let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
                    let command = install_cluster
                        .prepare_commit_direct_put_object_command(
                            pg_id,
                            &pg,
                            &install_req,
                            crate::VersionId::Null,
                            install_req.bucket_write_reservation.clone(),
                        )
                        .unwrap();
                    pg.try_insert_pending_metadata_command_slot(
                        primary.node_id().as_u32(),
                        &command,
                        Some(&install_bucket),
                    )
                    .unwrap();
                }
                Ok((
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
        .unwrap();
    assert!(matches!(result, Err("object already exists")));
    assert!(slot_installed.load(Ordering::SeqCst));
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        2,
        "multipart create authorization must be rerun after command-id contention"
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
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
    }
    assert_clean_metadata_command_stream(&first_map, &[2]);
}
#[test]
fn multipart_create_command_applies_to_all_acting_object_pg_nodes() {
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
    let upload_id = upload_id_from_label("mpucreatecommand");
    let create = crate::CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: Some(crate::SerializedTagSet::new("<Tagging/>".to_string())),
        metadata_blob: crate::SerializedMetadataBlob::new(vec![1, 2, 3]),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::new(vec![4, 5, 6]),
        initiator: Some(crate::OwnerIdentity::from_principal("initiator")),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: true,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };

    let outcome = cluster
        .create_multipart_upload(
            &bucket,
            &key,
            crate::BucketSnapshotRequest::default(),
            |_snapshot, existing_object| {
                assert!(existing_object.is_none());
                Ok::<_, ()>((11_u8, create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(outcome.value, 11);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    let mut generation_id = None;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap();
        assert_eq!(upload.bucket, bucket);
        assert_eq!(upload.key, key);
        assert_eq!(upload.initiated_at, outcome.initiated_at);
        assert_eq!(upload.tags, create.tags);
        assert_eq!(upload.metadata_blob, create.metadata_blob);
        assert_eq!(upload.system_metadata_blob, create.system_metadata_blob);
        assert_eq!(upload.initiator, create.initiator);
        assert_eq!(upload.owner, create.owner);
        assert_eq!(upload.acl_grants, create.acl_grants);
        assert_eq!(upload.public_read, create.public_read);
        assert_eq!(upload.object_lock, create.object_lock);
        assert_eq!(upload.checksum, create.checksum);
        assert_eq!(upload.encryption, create.encryption);
        if let Some(generation_id) = generation_id {
            assert_eq!(upload.object_generation_id, generation_id);
        } else {
            generation_id = Some(upload.object_generation_id);
        }
    }
}

#[test]
fn multipart_create_partial_apply_retry_reuses_pending_command() {
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
    let upload_id = upload_id_from_label("mpucreateretry");
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

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_upload_id = upload_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateMultipartUpload(create)
                    if create.upload.upload_id == hook_upload_id
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected multipart create metadata command apply failure",
                        source: std::io::Error::other(
                            "injected multipart create metadata command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .create_multipart_upload(
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
                context: "injected multipart create metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);

    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial multipart create command must remain pending"
    );
    let primary_upload = {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap()
    };
    {
        let failed_replica = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = failed_replica.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
    }

    let retry = cluster
        .create_multipart_upload(
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
    assert_eq!(retry.value, 7);
    assert_eq!(retry.initiated_at, primary_upload.initiated_at);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap();
        assert_eq!(upload.bucket, bucket);
        assert_eq!(upload.key, key);
        assert_eq!(upload.initiated_at, primary_upload.initiated_at);
        assert_eq!(
            upload.object_generation_id,
            primary_upload.object_generation_id
        );
    }
}

#[test]
fn multipart_create_partial_apply_reopens_and_converges() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("open local map");
    let (bucket, key, object_pg, _data_pg, bucket_pg) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let (bucket, key, object_pg, data_pg) =
            bucket_key_with_distinct_object_and_data_pg(topology);
        let bucket_pg = topology.bucket_pg_for(&bucket);
        (bucket, key, object_pg, data_pg, bucket_pg)
    };
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("mpucreatereopen");
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

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_upload_id = upload_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CreateMultipartUpload(create)
                    if create.upload.upload_id == hook_upload_id
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected multipart create reopen failure",
                        source: std::io::Error::other("injected multipart create reopen failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .create_multipart_upload(
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
                context: "injected multipart create reopen failure",
                ..
            })
        ),
        "expected injected partial create failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    let pending = pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket);
    assert!(
        pending.is_some(),
        "partial multipart create command must remain durable before reopen"
    );
    let pending = pending.unwrap();
    assert!(
        matches!(
            pending.payload(),
            MetadataCommandPayload::CreateMultipartUpload(create)
                if create.bucket_write_reservation.operation_kind == "create-multipart-upload"
        ),
        "partial multipart create command must carry the bucket write reservation proof"
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
            1
        );
    }
    let primary_upload = {
        let primary = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap()
    };
    drop(cluster);
    drop(map);

    let reopened = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
        .expect("reopen local map with in-flight multipart create");
    let reopened = Arc::new(reopened);
    assert!(
        pending_metadata_command_for_test(&reopened, PgId::new(object_pg), &bucket).is_none(),
        "open-time recovery should converge and clear the partial create command"
    );

    for node_id in node_ids {
        let node = reopened.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap();
        assert_eq!(upload.bucket, bucket);
        assert_eq!(upload.key, key);
        assert_eq!(upload.initiated_at, primary_upload.initiated_at);
        assert_eq!(
            upload.object_generation_id,
            primary_upload.object_generation_id
        );
    }
    {
        let bucket_primary = reopened
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
    assert_clean_metadata_command_stream(&reopened, &[bucket_pg, object_pg]);
}

#[test]
fn multipart_create_retry_rejects_same_request_with_mismatched_generation() {
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
    let upload_id = upload_id_from_label("mpurowmismatch");
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
    {
        let primary = map.node(NodeId::new(0)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap();
        let mismatched_generation =
            crate::GenerationId::new(upload.object_generation_id.get() + 1).unwrap();
        pg.connection()
            .execute(
                "UPDATE multipart_uploads SET object_generation_id = ?1 WHERE upload_id = ?2",
                rusqlite::params![mismatched_generation.get() as i64, upload_id.as_str()],
            )
            .unwrap();
    }

    let err = cluster
        .create_multipart_upload(
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
                context: "create multipart upload existing upload mismatch",
                ..
            })
        ),
        "expected exact multipart upload row mismatch, got {err:?}"
    );
}

#[test]
fn multipart_abort_command_removes_upload_from_all_acting_object_pg_nodes() {
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

    let first_upload_id = upload_id_from_label("mpuabortonallnodes");
    let first_create = crate::CreateMultipartUploadReq {
        upload_id: first_upload_id.clone(),
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
                Ok::<_, ()>(((), first_create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    assert!(cluster
        .abort_multipart_upload(&bucket, &key, &first_upload_id)
        .unwrap());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &first_upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
    }
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &first_upload_id,
        TerminalMultipartOutcome::Aborted,
    );

    let second_upload_id = upload_id_from_label("mpuabortedfresh");
    let second_create = crate::CreateMultipartUploadReq {
        upload_id: second_upload_id.clone(),
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
                Ok::<_, ()>(((), second_create.clone()))
            },
        )
        .unwrap()
        .unwrap();
    let mut second_generation = None;
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, &second_upload_id).unwrap();
        assert_eq!(upload.bucket, bucket);
        assert_eq!(upload.key, key);
        if let Some(second_generation) = second_generation {
            assert_eq!(upload.object_generation_id, second_generation);
        } else {
            second_generation = Some(upload.object_generation_id);
        }
    }
}

#[test]
fn multipart_abort_partial_apply_retry_cleans_uploaded_part_payload() {
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
    let upload_id = upload_id_from_label("mpuabandretryclean");
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

    let part_number = 1;
    let (shard_keys, _uploaded_part, uploaded_segment) = upload_streamed_test_multipart_part(
        &cluster,
        &bucket,
        &key,
        &upload_id,
        part_number,
        [0xAB; 16],
        b"uploaded part payload",
    );
    let data_pg_id = uploaded_segment.data_pg_id;
    let part_okh = uploaded_segment.segment_okh;
    let part_vid = uploaded_segment.segment_vid;

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_upload_id = upload_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::AbortMultipartUpload(abort)
                    if abort.upload_id == hook_upload_id
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected multipart abort metadata command apply failure",
                        source: std::io::Error::other(
                            "injected multipart abort metadata command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .abort_multipart_upload(&bucket, &key, &upload_id)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected multipart abort metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);

    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial multipart abort command must remain pending with cleanup refs"
    );
    {
        let replica_pg = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let upload =
            crate::PgMetadataStore::get_multipart_upload(&*replica_pg, &upload_id).unwrap();
        assert_eq!(upload.state, crate::UploadState::InProgress);
        assert!(
            crate::PgMetadataStore::get_multipart_part(&*replica_pg, &upload_id, part_number)
                .is_ok()
        );
    }
    for shard_index in 0..ec_shape.k + ec_shape.m {
        assert!(cluster
            .test_payload_shard_file_exists(data_pg_id, ec_shape, &part_okh, part_vid, shard_index)
            .unwrap());
    }

    assert!(cluster
        .abort_multipart_upload(&bucket, &key, &upload_id)
        .unwrap());
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
    }
    for (shard_index, key) in shard_keys.iter().enumerate() {
        assert!(
            !cluster
                .test_payload_shard_file_exists(
                    data_pg_id,
                    ec_shape,
                    &part_okh,
                    part_vid,
                    shard_index as u8
                )
                .unwrap(),
            "retrying pending abort should delete placed shard {key:?}"
        );
    }
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &upload_id,
        TerminalMultipartOutcome::Aborted,
    );
}

#[test]
fn multipart_abort_partial_apply_reopens_and_converges() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("open local map");
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
    let upload_id = upload_id_from_label("mpuabortreopen");
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

    let (_shard_keys, _uploaded_part, _uploaded_segment) = upload_streamed_test_multipart_part(
        &cluster,
        &bucket,
        &key,
        &upload_id,
        1,
        [0xAC; 16],
        b"uploaded part payload before abort reopen",
    );

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_upload_id = upload_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::AbortMultipartUpload(abort)
                    if abort.upload_id == hook_upload_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected multipart abort reopen failure",
                        source: std::io::Error::other("injected multipart abort reopen failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .abort_multipart_upload(&bucket, &key, &upload_id)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected multipart abort reopen failure",
                ..
            })
        ),
        "expected injected partial abort failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial abort must leave a durable primary pending slot before reopen"
    );
    drop(cluster);
    drop(map);

    let reopened = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
        .expect("reopen local map with in-flight multipart abort");
    let reopened = Arc::new(reopened);
    let reopened_cluster = crate::StorageCluster::from_local_map(Arc::clone(&reopened)).unwrap();
    assert!(
        !reopened_cluster
            .abort_multipart_upload(&bucket, &key, &upload_id)
            .unwrap(),
        "open-time recovery should have already converged the in-flight abort command"
    );
    assert!(pending_metadata_command_for_test(&reopened, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = reopened.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_part(&*pg, &upload_id, 1),
            Err(crate::MetadataError::PartNotFound { .. })
        ));
        assert!(
            crate::PgMetadataStore::get_all_multipart_part_segments_for_upload(&*pg, &upload_id,)
                .unwrap()
                .is_empty()
        );
    }
    assert_terminal_multipart_upload_invariants(
        &reopened,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &upload_id,
        TerminalMultipartOutcome::Aborted,
    );
    assert_clean_metadata_command_stream(&reopened, &[object_pg]);
}

#[test]
fn multipart_abort_retries_after_pending_install_conflict() {
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
    let upload_id = upload_id_from_label("mpuabortinstall");
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

    let pg_id = PgId::new(object_pg);
    let unrelated_session_id = crate::SessionId::try_from("37".repeat(16)).unwrap();
    let injected = Arc::new(AtomicBool::new(false));
    let injected_for_hook = Arc::clone(&injected);
    let map_for_hook = Arc::clone(&map);
    let bucket_for_hook = bucket.clone();
    let key_for_hook = key.clone();
    let session_for_hook = unrelated_session_id.clone();
    let proof_for_hook = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-abort-multipart-unrelated-stream-create",
        Some(key.as_str()),
    );
    let command_epoch = cluster.operation_epoch();
    let _hook_guard =
            cluster.test_install_before_abort_multipart_pending_install_hook(Arc::new(move || {
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
            }));

    assert!(cluster
        .abort_multipart_upload(&bucket, &key, &upload_id)
        .unwrap());
    assert!(
        injected.load(Ordering::SeqCst),
        "test hook must exercise the abort pending-install conflict window"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
        let session =
            crate::PgMetadataStore::get_stream_upload(&*pg, &unrelated_session_id).unwrap();
        assert_eq!(session.bucket, bucket);
        assert_eq!(session.key, key);
        assert_eq!(session.target, crate::StreamUploadTarget::PutObject);
    }
}

fn assert_multipart_abort_matching_pending_install_race_returns_success(authorized: bool) {
    let _serial = lock_metadata_command_apply_hook_test();
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
    let upload_id = upload_id_from_label(if authorized {
        "mpuabortsameauth"
    } else {
        "mpuabortsame"
    });
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
    let authorized_upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();

    let pg_id = PgId::new(object_pg);
    let cleanup = {
        let primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
            .unwrap();
        let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
        pg.prepare_abort_multipart_upload_cleanup(&bucket, &key, &upload_id)
            .unwrap()
            .expect("upload is still abortable")
    };
    let bucket_write_reservation = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "abort-multipart-upload",
        Some(key.as_str()),
    );
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id,
            map.test_next_metadata_command_log_index(pg_id),
        ),
        MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            cleanup,
            bucket_write_reservation,
        })),
    );
    let inserted = Arc::new(AtomicBool::new(false));
    let inserted_for_hook = Arc::clone(&inserted);
    let map_for_hook = Arc::clone(&map);
    let bucket_for_hook = bucket.clone();
    let command_for_hook = command.clone();
    let _hook_guard =
        cluster.test_install_before_abort_multipart_pending_install_hook(Arc::new(move || {
            if inserted_for_hook.swap(true, Ordering::SeqCst) {
                return;
            }
            insert_pending_metadata_command_for_test(
                &map_for_hook,
                pg_id,
                &bucket_for_hook,
                &command_for_hook,
            );
        }));

    let aborted = if authorized {
        cluster
            .abort_authorized_multipart_upload(
                &crate::AuthorizedMultipartUploadRecord::assume_authorized(authorized_upload),
            )
            .unwrap()
    } else {
        cluster
            .abort_multipart_upload(&bucket, &key, &upload_id)
            .unwrap()
    };

    assert!(aborted);
    assert!(inserted.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &upload_id,
        TerminalMultipartOutcome::Aborted,
    );
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn multipart_abort_matching_pending_install_race_returns_success() {
    assert_multipart_abort_matching_pending_install_race_returns_success(false);
}

#[test]
fn authorized_multipart_abort_matching_pending_install_race_returns_success() {
    assert_multipart_abort_matching_pending_install_race_returns_success(true);
}

#[test]
fn authorized_multipart_abort_rejects_stale_upload_row() {
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
    let upload_id = upload_id_from_label("authabortstale");
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

    let authorized = crate::AuthorizedMultipartUploadRecord::assume_authorized(
        cluster
            .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
            .unwrap(),
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        crate::PgMetadataStore::set_upload_state(&*pg, &upload_id, crate::UploadState::Completing)
            .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    assert!(
        !cluster
            .abort_authorized_multipart_upload(&authorized)
            .unwrap(),
        "authorized abort must not apply a stale authorization snapshot"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id)
                .unwrap()
                .state,
            crate::UploadState::Completing
        );
    }
}

#[test]
fn multipart_abort_drains_pending_completion_before_aborting() {
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
    let (req, mut expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "completewinsabort");
    let pg_id = PgId::new(object_pg);
    let last_modified_millis = 987_656;
    let (pending_completion, write_sequence) = pending_multipart_completion_command_for_test(
        &map,
        &cluster,
        pg_id,
        &req,
        last_modified_millis,
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending_completion);

    assert!(
        !cluster
            .abort_multipart_upload(&bucket, &key, &req.upload_id)
            .unwrap(),
        "abort should observe that the pending completion won the upload lifecycle"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());

    expected_segment.version_id = crate::VersionId::Null.to_u64();
    let outcome = crate::CompleteMultipartCommitOutcome {
        version_id: crate::VersionId::Null,
        stale_payload: None,
        live_tags: req.tags.clone(),
        live_size: req.size,
        live_last_modified: last_modified_millis,
    };
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &map,
        &node_ids,
        object_pg,
        &req,
        &expected_segment,
        &outcome,
        write_sequence,
    );
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn multipart_abort_pending_install_conflict_cleans_upload_part_stream_session_and_segments() {
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
    let upload_id = upload_id_from_label("mpuabortstream");
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

    let pg_id = PgId::new(object_pg);
    let session_id = crate::SessionId::try_from("38".repeat(16)).unwrap();
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
    let first_payload = b"copied segment before abort pending conflict";
    let (_target, first_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: first_payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(first_payload),
                payload_crc64: checksum::crc64::checksum(first_payload),
                segment_okh: [0x38; 16],
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
    let second_payload = b"second copied segment before abort pending conflict";
    let (_target, second_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 1,
                size: second_payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(second_payload),
                payload_crc64: checksum::crc64::checksum(second_payload),
                segment_okh: [0x39; 16],
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

    let raced_session_id = crate::SessionId::try_from("3a".repeat(16)).unwrap();
    let injected = Arc::new(AtomicBool::new(false));
    let injected_for_hook = Arc::clone(&injected);
    let map_for_hook = Arc::clone(&map);
    let bucket_for_hook = bucket.clone();
    let key_for_hook = key.clone();
    let upload_for_hook = upload_id.clone();
    let session_for_hook = raced_session_id.clone();
    let proof_for_hook = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-abort-multipart-raced-stream-create",
        Some(key.as_str()),
    );
    let command_epoch = cluster.operation_epoch();
    let _hook_guard =
            cluster.test_install_before_abort_multipart_pending_install_hook(Arc::new(move || {
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
                                target: crate::StreamUploadTarget::UploadPart {
                                    upload_id: upload_for_hook.clone(),
                                    part_number: 1,
                                },
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
            }));

    assert!(cluster
        .abort_multipart_upload(&bucket, &key, &upload_id)
        .unwrap());
    assert!(
        injected.load(Ordering::SeqCst),
        "test hook must exercise the UploadPart stream creation conflict window"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &raced_session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
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
                    segment_crc64: checksum::crc64::checksum(payload),
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
            "abort must clean copied UploadPart staged payload after contention: {error:?}"
        );
    }
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &upload_id,
        TerminalMultipartOutcome::Aborted,
    );
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn multipart_abort_pending_install_conflict_cleans_committed_stream_part() {
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
    let upload_id = upload_id_from_label("mpuabortfinpart");
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

    let session_id = crate::SessionId::try_from("3b".repeat(16)).unwrap();
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
    let payload = b"streamed part finalized while abort waits for pending slot";
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
                segment_okh: [0x3b; 16],
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
    let injected = Arc::new(AtomicBool::new(false));
    let injected_for_hook = Arc::clone(&injected);
    let map_for_hook = Arc::clone(&map);
    let bucket_for_hook = bucket.clone();
    let key_for_hook = key.clone();
    let upload_for_hook = upload_id.clone();
    let session_for_hook = session_id.clone();
    let proof_for_hook = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-stream-part-commit-race",
        Some(key.as_str()),
    );
    let command_epoch = cluster.operation_epoch();
    let _hook_guard =
        cluster.test_install_before_abort_multipart_pending_install_hook(Arc::new(move || {
            if injected_for_hook.swap(true, Ordering::SeqCst) {
                return;
            }
            let primary = map_for_hook
                .metadata_pg_primary_node(command_epoch, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let upload =
                crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_for_hook).unwrap();
            let staging_segments =
                crate::PgMetadataStore::list_stream_segments(&*pg, &session_for_hook).unwrap();
            let payload_crc64 =
                staging_segments
                    .iter()
                    .fold(checksum::crc64::checksum(&[]), |crc64, segment| {
                        checksum::crc64::combine(crc64, segment.payload_crc64, segment.size)
                    });
            let part = crate::MultipartPartRecord {
                upload_id: upload_for_hook.clone(),
                part_number: 1,
                generation: 0,
                size: staging_segments.iter().map(|segment| segment.size).sum(),
                payload_crc64,
                etag: vec![0x3b; 8],
                etag_kind: crate::EtagKind::Crc64,
                part_okh: [0u8; 16],
                part_vid: crate::GenerationId::MIN,
                ec_k: staging_segments[0].ec_k,
                ec_m: staging_segments[0].ec_m,
                last_modified: 123_456,
                checksum: None,
            };
            let segments = staging_segments
                .iter()
                .map(|staged| crate::MultipartPartSegmentRecord {
                    bucket: bucket_for_hook.clone(),
                    key: key_for_hook.clone(),
                    upload_id: upload_for_hook.clone(),
                    version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
                    part_number: 1,
                    segment_index: staged.segment_index,
                    size: staged.size,
                    segment_crc64: staged.segment_crc64,
                    segment_okh: staged.segment_okh,
                    segment_vid: staged.segment_vid,
                    data_pg_id: staged.data_pg_id,
                    placement_cluster_epoch: staged.placement_cluster_epoch,
                    ec_k: staged.ec_k,
                    ec_m: staged.ec_m,
                })
                .collect::<Vec<_>>();
            let log_index = pg.max_metadata_command_log_index(command_epoch).unwrap() + 1;
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    command_epoch,
                    pg_id,
                    MetadataCommandLogIndex::new(log_index).unwrap(),
                ),
                MetadataCommandPayload::CommitStreamPart(Box::new(CommitStreamPartCommand {
                    bucket: bucket_for_hook.clone(),
                    key: key_for_hook.clone(),
                    session_id: session_for_hook.clone(),
                    upload,
                    part,
                    segments,
                    existing_part: None,
                    displaced_segments: Vec::new(),
                    bucket_write_reservation: proof_for_hook.clone(),
                })),
            );
            pg.try_insert_pending_metadata_command_slot(
                primary.node_id().as_u32(),
                &command,
                Some(&bucket_for_hook),
            )
            .unwrap();
        }));

    assert!(cluster
        .abort_multipart_upload(&bucket, &key, &upload_id)
        .unwrap());
    assert!(
        injected.load(Ordering::SeqCst),
        "test hook must exercise the stream-part finalize conflict window"
    );
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
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
        assert!(
            crate::PgMetadataStore::get_all_multipart_part_segments_for_upload(&*pg, &upload_id,)
                .unwrap()
                .is_empty()
        );
    }
    let mut readback = Vec::new();
    let error = cluster
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
        .unwrap_err();
    assert!(
        matches!(error, StoreError::NotFound),
        "abort must clean stream part payload after finalize wins the slot: {error:?}"
    );
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &upload_id,
        TerminalMultipartOutcome::Aborted,
    );
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn upload_part_stream_create_pending_install_race_reloads_after_abort() {
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
    let bucket = bucket_for_pg(topology, 1, "upload-part-create-race-");
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
    let upload_id = upload_id_from_label("partcreateabort");
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
    let upload = first_cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();

    let pg_id = PgId::new(2);
    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_upload = upload.clone();
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
            let command = MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::INITIAL,
                    pg_id,
                    hook_map.test_next_metadata_command_log_index(pg_id),
                ),
                MetadataCommandPayload::AbortMultipartUpload(Box::new(
                    AbortMultipartUploadCommand {
                        bucket: hook_bucket.clone(),
                        key: hook_key.clone(),
                        upload_id: hook_upload_id.clone(),
                        cleanup: crate::AbortMultipartUploadCleanup {
                            upload: hook_upload.clone(),
                            parts: Vec::new(),
                            streaming_segments: Vec::new(),
                            stream_uploads: Vec::new(),
                            stream_upload_segments: Vec::new(),
                        },
                        bucket_write_reservation: hook_bucket_write_reservation.clone(),
                    },
                )),
            );
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
        }),
    );

    let session_id = crate::SessionId::try_from("52".repeat(16)).unwrap();
    let err = first_cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap_err();

    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Metadata(crate::MetadataError::NoSuchUpload { .. })
        ),
        "expected upload-part session create to reload after abort, got {err:?}"
    );
    assert!(hook_ran.load(Ordering::SeqCst));
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
    }
    assert_clean_metadata_command_stream(&first_map, &[pg_id.get()]);
}

#[test]
fn begin_upload_part_stream_pending_install_race_reruns_action() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("open local map");
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "begin-upload-part-race-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("beginpartrace");
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

    let pg_id = PgId::new(2);
    let requested_session_id = crate::SessionId::try_from("54".repeat(16)).unwrap();
    let winner_session_id = crate::SessionId::try_from("55".repeat(16)).unwrap();
    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_upload_id = upload_id.clone();
    let hook_winner_session_id = winner_session_id.clone();
    let hook_proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "test-upload-part-stream-create-winner",
        Some(key.as_str()),
    );
    let _hook_guard = cluster.test_install_before_metadata_command_pending_install_hook(
            Arc::new(move || {
                if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                    return;
                }
                let command = MetadataCommandEnvelope::new(
                    MetadataCommandId::new(
                        ClusterEpoch::INITIAL,
                        pg_id,
                        hook_map.test_next_metadata_command_log_index(pg_id),
                    ),
                    MetadataCommandPayload::CreateStreamUpload(Box::new(
                        crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                            crate::CreateStreamUploadReq {
                                session_id: hook_winner_session_id.clone(),
                                bucket: hook_bucket.clone(),
                                key: hook_key.clone(),
                                target: crate::StreamUploadTarget::UploadPart {
                                    upload_id: hook_upload_id.clone(),
                                    part_number: 2,
                                },
                                encryption: crate::ObjectEncryption::None,
                            },
                            789,
                            hook_proof.clone(),
                        ),
                    )),
                );
                insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
            }),
        );

    let calls_for_action = Arc::clone(&action_calls);
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "begin-upload-part-test",
            Some(key.as_str()),
        )
        .unwrap();
    let proof = crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);
    let value = cluster
        .begin_upload_part_stream_session(
            crate::BeginUploadPartStreamSessionReq {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: upload_id.clone(),
                part_number: 1,
                session_id: requested_session_id.clone(),
                bucket_write_reservation: proof,
            },
            move |upload| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>((
                    crate::AuthorizedMultipartUploadRecord::assume_authorized(upload.clone()),
                    21_u8,
                ))
            },
        )
        .unwrap()
        .unwrap();

    assert_eq!(value, 21);
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(action_calls.load(Ordering::SeqCst), 2);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        let requested =
            crate::PgMetadataStore::get_stream_upload(&*pg, &requested_session_id).unwrap();
        assert_eq!(
            requested.target,
            crate::StreamUploadTarget::UploadPart {
                upload_id: upload_id.clone(),
                part_number: 1,
            }
        );
        let winner = crate::PgMetadataStore::get_stream_upload(&*pg, &winner_session_id).unwrap();
        assert_eq!(
            winner.target,
            crate::StreamUploadTarget::UploadPart {
                upload_id: upload_id.clone(),
                part_number: 2,
            }
        );
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
    let bucket_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .is_empty(),
        "UploadPart stream-create command convergence must release the durable reservation"
    );
}

#[test]
fn begin_upload_part_stream_drains_pending_completion_before_create() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("open local map");
    let (bucket, key) = {
        let topology = map
            .node(NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 1, "begin-upload-part-complete-");
        let key = key_for_object_pg(topology, &bucket, 2, "object-");
        (bucket, key)
    };

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req, mut expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "completewinscreate");
    let pg_id = PgId::new(2);
    let last_modified_millis = 987_654;
    let (pending_completion, write_sequence) = pending_multipart_completion_command_for_test(
        &map,
        &cluster,
        pg_id,
        &req,
        last_modified_millis,
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending_completion);

    let action_calls = Arc::new(AtomicUsize::new(0));
    let calls_for_action = Arc::clone(&action_calls);
    let session_id = crate::SessionId::try_from("56".repeat(16)).unwrap();
    let reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            "begin-upload-part-complete-test",
            Some(key.as_str()),
        )
        .unwrap();
    let proof = crate::metadata_command::BucketWriteReservationProof::from(&reservation.record);
    let err = cluster
        .begin_upload_part_stream_session(
            crate::BeginUploadPartStreamSessionReq {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id: req.upload_id.clone(),
                part_number: 2,
                session_id: session_id.clone(),
                bucket_write_reservation: proof,
            },
            move |fresh_upload| {
                calls_for_action.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>((
                    crate::AuthorizedMultipartUploadRecord::assume_authorized(fresh_upload.clone()),
                    (),
                ))
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::BucketSnapshotLoadError::Metadata(crate::MetadataError::NoSuchUpload { .. })
        ),
        "expected pending complete to win before session create, got {err:?}"
    );
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        0,
        "session-create action must not run after completion wins the slot"
    );
    let bucket_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .is_empty(),
        "failed UploadPart stream-create must release the caller's durable reservation"
    );
    drop(bucket_pg);
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    expected_segment.version_id = crate::VersionId::Null.to_u64();
    let outcome = crate::CompleteMultipartCommitOutcome {
        version_id: crate::VersionId::Null,
        stale_payload: None,
        live_tags: req.tags.clone(),
        live_size: req.size,
        live_last_modified: last_modified_millis,
    };
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &map,
        &node_ids,
        pg_id.get(),
        &req,
        &expected_segment,
        &outcome,
        write_sequence,
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
    assert_clean_metadata_command_stream(&map, &[pg_id.get()]);
}

#[test]
fn begin_upload_part_stream_existing_session_mismatch_releases_bucket_write_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "begin-upload-part-mismatch-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("beginmismatch");
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

    let session_id = crate::SessionId::try_from("57".repeat(16)).unwrap();
    let object_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    crate::PgMetadataStore::create_stream_upload(
        &*object_pg,
        &crate::CreateStreamUploadReq {
            session_id: session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: crate::StreamUploadTarget::UploadPart {
                upload_id: upload_id.clone(),
                part_number: 9,
            },
            encryption: crate::ObjectEncryption::None,
        },
    )
    .unwrap();
    drop(object_pg);

    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "begin-upload-part-mismatch-test",
        Some(key.as_str()),
    );
    let err = cluster
        .begin_upload_part_stream_session(
            crate::BeginUploadPartStreamSessionReq {
                bucket: bucket.clone(),
                key: key.clone(),
                upload_id,
                part_number: 1,
                session_id,
                bucket_write_reservation: proof,
            },
            |upload| {
                Ok::<_, ()>((
                    crate::AuthorizedMultipartUploadRecord::assume_authorized(upload.clone()),
                    (),
                ))
            },
        )
        .unwrap_err();
    assert!(
        matches!(err, crate::BucketSnapshotLoadError::Metadata(_)),
        "expected existing-session mismatch to fail before command ownership, got {err:?}"
    );

    let bucket_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn create_upload_part_stream_existing_session_mismatch_releases_bucket_write_proof() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "create-upload-part-mismatch-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("createmismatch");
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

    let session_id = crate::SessionId::try_from("58".repeat(16)).unwrap();
    let object_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(2)
        .unwrap();
    crate::PgMetadataStore::create_stream_upload(
        &*object_pg,
        &crate::CreateStreamUploadReq {
            session_id: session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: crate::StreamUploadTarget::UploadPart {
                upload_id: upload_id.clone(),
                part_number: 9,
            },
            encryption: crate::ObjectEncryption::None,
        },
    )
    .unwrap();
    let upload = crate::PgMetadataStore::get_multipart_upload(&*object_pg, &upload_id).unwrap();
    drop(object_pg);

    let err = cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap_err();
    assert!(
        matches!(err, crate::ObjectPgActionError::Metadata(_)),
        "expected existing-session mismatch to fail before command ownership, got {err:?}"
    );

    let bucket_pg = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn upload_part_stream_create_zero_apply_reopens_and_converges() {
    let _serial = lock_metadata_command_apply_hook_test();
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map =
        LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).expect("open local map");
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "upload-part-create-reopen-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let upload_id = upload_id_from_label("partcreatereopen");
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

    let session_id = crate::SessionId::try_from("53".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    let pg_id = PgId::new(2);
    let create = crate::CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: crate::StreamUploadTarget::UploadPart {
            upload_id: upload_id.clone(),
            part_number: 1,
        },
        encryption: upload.encryption.clone(),
    };
    let pending_command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                crate::metadata_command::CreateStreamUploadCommand::from_request_with_bucket_write_reservation(
                    create,
                    456,
                    crate::metadata_command::BucketWriteReservationProof::from(
                        &cluster
                            .acquire_durable_bucket_write_reservation(
                                &bucket,
                                "upload-part-stream-create-reopen",
                                Some(key.as_str()),
                            )
                            .unwrap()
                            .record,
                    ),
                ),
            )),
        );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending_command);

    assert!(
        pending_metadata_command_for_test(&map, pg_id, &bucket).is_some(),
        "crash-shape UploadPart stream create should remain pending before reopen"
    );
    let expected_created_at = {
        let MetadataCommandPayload::CreateStreamUpload(create) = pending_command.payload() else {
            unreachable!("constructed command changed payload kind");
        };
        create.session.created_at
    };
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }

    let reopened = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
        .expect("reopen local map with pending UploadPart stream create");
    let reopened = Arc::new(reopened);
    let reopened_cluster = crate::StorageCluster::from_local_map(Arc::clone(&reopened)).unwrap();
    let reopened_upload = reopened_cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    let retried_session = reopened_cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(reopened_upload),
            1,
            &session_id,
        )
        .unwrap();
    assert_eq!(retried_session, session_id);
    assert!(pending_metadata_command_for_test(&reopened, pg_id, &bucket).is_none());

    for node_id in node_ids {
        let node = reopened.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(pg_id.get()).unwrap();
        let session = crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).unwrap();
        assert_eq!(session.bucket, bucket);
        assert_eq!(session.key, key);
        assert_eq!(session.created_at, expected_created_at);
        assert_eq!(
            session.target,
            crate::StreamUploadTarget::UploadPart {
                upload_id: upload_id.clone(),
                part_number: 1,
            }
        );
    }
    assert_clean_metadata_command_stream(&reopened, &[pg_id.get()]);
    let bucket_pg = reopened
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(1)
        .unwrap();
    assert!(
        crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
            .unwrap()
            .is_empty(),
        "UploadPart stream-create reopen convergence must release command-owned reservations"
    );
}

#[test]
fn multipart_abort_zero_apply_leaves_upload_in_progress_before_retry() {
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
    let upload_id = upload_id_from_label("mpuabortingblocks");
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

    let session_id = crate::SessionId::try_from("0123456789abcdef0123456789abcdef").unwrap();
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
    let staged_payload = b"staged upload part segment";
    let staged_okh = [0xCD; 16];
    let (_target, staged_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: staged_payload.len() as u64,
                segment_crc64: checksum::crc64::checksum(staged_payload),
                payload_crc64: checksum::crc64::checksum(staged_payload),
                segment_okh: staged_okh,
            },
        )
        .unwrap();
    let staged_shards = cluster
        .write_stream_segment_payload_shards(&staged_segment, staged_payload)
        .unwrap();
    let staged_shard_batch = staged_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &session_id,
            staged_segment.segment_index,
            &staged_segment,
            &staged_shard_batch,
        )
        .unwrap();

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_upload_id = upload_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::AbortMultipartUpload(abort)
                    if abort.upload_id == hook_upload_id
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected zero-apply multipart abort failure",
                        source: std::io::Error::other(
                            "injected zero-apply multipart abort failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .abort_multipart_upload(&bucket, &key, &upload_id)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected zero-apply multipart abort failure",
                ..
            })
        ),
        "expected injected zero-apply failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let upload = crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap();
        assert_eq!(upload.state, crate::UploadState::InProgress);
        assert!(crate::PgMetadataStore::get_stream_upload(&*pg, &session_id).is_ok());
        assert_eq!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id).unwrap(),
            vec![staged_segment.clone()]
        );
    }
    for shard_index in 0..ec_shape.k + ec_shape.m {
        assert!(cluster
            .test_payload_shard_file_exists(
                staged_segment.data_pg_id,
                ec_shape,
                &staged_segment.segment_okh,
                staged_segment.segment_vid,
                shard_index
            )
            .unwrap());
    }

    assert!(cluster
        .abort_multipart_upload(&bucket, &key, &upload_id)
        .unwrap());
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
    for shard_index in 0..ec_shape.k + ec_shape.m {
        assert!(
            !cluster
                .test_payload_shard_file_exists(
                    staged_segment.data_pg_id,
                    ec_shape,
                    &staged_segment.segment_okh,
                    staged_segment.segment_vid,
                    shard_index
                )
                .unwrap(),
            "retrying abort should delete staged UploadPart stream shard {shard_index}"
        );
    }
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &upload_id,
        TerminalMultipartOutcome::Aborted,
    );
}

#[test]
fn lifecycle_multipart_abort_uses_command_and_cleans_uploaded_part_payload() {
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
    put_test_lifecycle(&cluster, &bucket);

    let upload_id = upload_id_from_label("mpulifecycleabort");
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

    let part_number = 1;
    let (shard_keys, _uploaded_part, uploaded_segment) = upload_streamed_test_multipart_part(
        &cluster,
        &bucket,
        &key,
        &upload_id,
        part_number,
        [0xBC; 16],
        b"lifecycle uploaded part payload",
    );
    let data_pg_id = uploaded_segment.data_pg_id;
    let part_okh = uploaded_segment.segment_okh;
    let part_vid = uploaded_segment.segment_vid;

    let aborted = cluster
        .abort_multipart_upload_if_due(
            &bucket,
            &key,
            &upload_id,
            current_bucket_incarnation(&cluster, &bucket),
            |raw_lifecycle, upload| {
                assert_eq!(raw_lifecycle, Some("<LifecycleConfiguration/>"));
                assert_eq!(upload.state, crate::UploadState::InProgress);
                Ok::<bool, ()>(true)
            },
        )
        .unwrap()
        .unwrap();
    assert!(aborted);
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
    }
    for (shard_index, key) in shard_keys.iter().enumerate() {
        assert!(
            !cluster
                .test_payload_shard_file_exists(
                    data_pg_id,
                    ec_shape,
                    &part_okh,
                    part_vid,
                    shard_index as u8
                )
                .unwrap(),
            "lifecycle abort should delete placed shard {key:?}"
        );
    }
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &upload_id,
        TerminalMultipartOutcome::Aborted,
    );
}
