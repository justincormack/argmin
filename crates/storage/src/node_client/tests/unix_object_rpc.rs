use super::*;

#[test]
fn unix_object_payload_reclaim_claim_release_survives_expired_route() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("expired-reclaim-claim-route-bucket");
    let key = crate::tests::object_key("expired-reclaim-claim-route-key");
    let generation_id = GenerationId::new(81).unwrap();
    let claim = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::put_object_segments_reclaim(
            &*pg,
            &ObjectSegmentsReclaimRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                generation_id,
                created_at: 10,
                segments: Vec::new(),
            },
        )
        .unwrap();
        let claim = PgMetadataStore::acquire_object_payload_reclaim_claim(
            &*pg,
            &bucket,
            1,
            &key,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "expired-reclaim-route-claim",
            "expired-reclaim-route-owner",
            config.cluster_epoch,
            10,
            Some(20),
            10,
        )
        .unwrap()
        .expect("seeded reclaim root must be claimable");
        pg.refresh_metadata_command_state_digest().unwrap();
        claim
    };
    config.route_map_validity =
        RouteMapValidity::until_ms(crate::clock::current_time_millis().saturating_sub(1)).unwrap();
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    ObjectMutationMetadataNodeClient::release_object_payload_reclaim_claim(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &claim,
    )
    .expect("retained reclaim claim release must survive active route expiry");
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(
        PgMetadataStore::object_payload_reclaim_claim(&*pg)
            .unwrap()
            .is_none(),
        "retained Unix reclaim cleanup must release the exact durable claim"
    );
}

#[test]
fn unix_retained_stream_abort_cleans_expired_route_session() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    config.pg_routes = vec![
        StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: config.cluster_epoch,
            state: crate::types::PgState::Active,
            primary_node_id: config.node_id,
            acting_set: vec![config.node_id],
        },
        StorageNodePgRoute {
            pg_id: 1,
            cluster_epoch: config.cluster_epoch,
            state: crate::types::PgState::Active,
            primary_node_id: config.node_id,
            acting_set: vec![config.node_id],
        },
    ];
    config.route_map_validity =
        RouteMapValidity::until_ms(crate::clock::current_time_millis().saturating_sub(1)).unwrap();
    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let (bucket, key, correct_pg_id) = (0..100)
        .map(|index| {
            let bucket = crate::tests::bucket_name(format!("retained-stream-abort-bucket-{index}"));
            let key = crate::tests::object_key(format!("retained-stream-abort-key-{index}"));
            let pg_id = node.pg_topology().object_pg_for(&bucket, &key);
            (bucket, key, pg_id)
        })
        .find(|(_, _, pg_id)| *pg_id < 2)
        .expect("two-PG topology must place a test object");
    let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
    let session_id = crate::tests::stream_session_id("retained-abort");
    let segment = StreamUploadSegmentRecord {
        session_id: session_id.clone(),
        segment_index: 0,
        size: 17,
        segment_crc64: 41,
        payload_crc64: 41,
        segment_okh: [0x61; 16],
        segment_vid: GenerationId::new(2).unwrap(),
        data_pg_id: 0,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 1,
        ec_m: 0,
    };
    for pg_id in [correct_pg_id, wrong_pg_id] {
        let pg = node.get_pg(pg_id).unwrap();
        PgMetadataStore::reserve_object_generation(&*pg, &bucket, &key, &session_id).unwrap();
        PgMetadataStore::create_stream_upload(
            &*pg,
            &CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: StreamUploadTarget::PutObject,
                encryption: ObjectEncryption::None,
            },
        )
        .unwrap();
        PgMetadataStore::append_stream_segment(&*pg, &segment).unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    let wrong_command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            config.cluster_epoch,
            PgId::new(wrong_pg_id),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::AbortStreamUpload(Box::new(
            crate::metadata_command::AbortStreamUploadCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                session_id: session_id.clone(),
                staged_segments: vec![segment.clone()],
                stream_create_bucket_write_reservation: None,
            },
        )),
    );
    node.get_pg(wrong_pg_id)
        .unwrap()
        .try_insert_pending_metadata_command_slot(
            config.node_id.as_u32(),
            &wrong_command,
            Some(&bucket),
        )
        .unwrap();
    let part_upload_id = crate::tests::multipart_upload_id("retained-abort-upload");
    let part_session_id = crate::tests::stream_session_id("retained-part");
    let part_segment = StreamUploadSegmentRecord {
        session_id: part_session_id.clone(),
        segment_index: 0,
        size: 23,
        segment_crc64: 43,
        payload_crc64: 43,
        segment_okh: [0x62; 16],
        segment_vid: GenerationId::new(3).unwrap(),
        data_pg_id: 0,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 1,
        ec_m: 0,
    };
    {
        let pg = node.get_pg(correct_pg_id).unwrap();
        PgMetadataStore::create_multipart_upload(
            &*pg,
            &CreateMultipartUploadReq {
                upload_id: part_upload_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                tags: None,
                metadata_blob: SerializedMetadataBlob::default(),
                system_metadata_blob: SerializedSystemMetadataBlob::default(),
                initiator: OwnerIdentity::from_principal("owner"),
                owner: OwnerIdentity::from_principal("owner"),
                acl_grants: AclGrants::default(),
                public_read: false,
                object_lock: ObjectLockState::default(),
                checksum: None,
                encryption: ObjectEncryption::None,
            },
        )
        .unwrap();
        PgMetadataStore::create_stream_upload(
            &*pg,
            &CreateStreamUploadReq {
                session_id: part_session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: StreamUploadTarget::UploadPart {
                    upload_id: part_upload_id.clone(),
                    part_number: 1,
                },
                encryption: ObjectEncryption::None,
            },
        )
        .unwrap();
        PgMetadataStore::append_stream_segment(&*pg, &part_segment).unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    drop(node);

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..10)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );
    let correct_pg = ObjectMetadataPgId::new_for_test(PgId::new(correct_pg_id));
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(wrong_pg_id));

    let active_error = ObjectMutationMetadataNodeClient::load_stream_upload_session(
        &client,
        correct_pg,
        &bucket,
        &key,
        &session_id,
    )
    .unwrap_err();
    assert!(matches!(
        active_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::StaleShardLocation,
            ..
        })
    ));

    assert!(matches!(
        ObjectMutationMetadataNodeClient::prepare_retained_stream_upload_abort(
            &client,
            wrong_pg,
            config.cluster_epoch,
            &bucket,
            &key,
            &session_id,
        ),
        Err(ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        }))
    ));
    assert!(matches!(
        MetadataCommandNodeClient::apply_retained_stream_upload_abort(
            &client,
            PgId::new(wrong_pg_id),
            &wrong_command,
        ),
        Err(BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        }))
    ));
    assert!(matches!(
        MetadataCommandNodeClient::finish_retained_stream_upload_abort(
            &client,
            PgId::new(wrong_pg_id),
            &wrong_command,
        ),
        Err(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let command = ObjectMutationMetadataNodeClient::prepare_retained_stream_upload_abort(
        &client,
        correct_pg,
        config.cluster_epoch,
        &bucket,
        &key,
        &session_id,
    )
    .unwrap()
    .expect("expired-route retained cleanup must prepare the exact abort");
    assert!(matches!(
        command.payload(),
        MetadataCommandPayload::AbortStreamUpload(abort)
            if abort.bucket == bucket
                && abort.key == key
                && abort.session_id == session_id
                && abort.staged_segments == vec![segment.clone()]
    ));
    MetadataCommandNodeClient::apply_retained_stream_upload_abort(
        &client,
        PgId::new(correct_pg_id),
        &command,
    )
    .unwrap();
    assert!(
        MetadataCommandNodeClient::finish_retained_stream_upload_abort(
            &client,
            PgId::new(correct_pg_id),
            &command,
        )
        .unwrap()
    );

    let part_command = ObjectMutationMetadataNodeClient::prepare_retained_stream_upload_abort(
        &client,
        correct_pg,
        config.cluster_epoch,
        &bucket,
        &key,
        &part_session_id,
    )
    .unwrap()
    .expect("expired-route retained cleanup must prepare the UploadPart abort");
    assert!(matches!(
        part_command.payload(),
        MetadataCommandPayload::AbortStreamUpload(abort)
            if abort.bucket == bucket
                && abort.key == key
                && abort.session_id == part_session_id
                && abort.staged_segments == vec![part_segment.clone()]
                && abort.stream_create_bucket_write_reservation.is_none()
    ));
    MetadataCommandNodeClient::apply_retained_stream_upload_abort(
        &client,
        PgId::new(correct_pg_id),
        &part_command,
    )
    .unwrap();
    assert!(
        MetadataCommandNodeClient::finish_retained_stream_upload_abort(
            &client,
            PgId::new(correct_pg_id),
            &part_command,
        )
        .unwrap()
    );

    for thread in server_threads {
        thread.join().unwrap();
    }
    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(correct_pg_id).unwrap();
    assert!(matches!(
        PgMetadataStore::get_stream_upload(&*pg, &session_id),
        Err(MetadataError::StreamSessionNotFound { .. })
    ));
    assert!(matches!(
        PgMetadataStore::get_object_generation_reservation(&*pg, &bucket, &key, &session_id),
        Err(MetadataError::ObjectGenerationReservationNotFound { .. })
    ));
    assert!(PgMetadataStore::list_stream_segments(&*pg, &session_id)
        .unwrap()
        .is_empty());
    assert!(pg
        .pending_metadata_command_slot(config.node_id.as_u32(), config.cluster_epoch)
        .unwrap()
        .is_none());
    assert!(matches!(
        PgMetadataStore::get_stream_upload(&*pg, &part_session_id),
        Err(MetadataError::StreamSessionNotFound { .. })
    ));
    assert!(
        PgMetadataStore::list_stream_segments(&*pg, &part_session_id)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        PgMetadataStore::get_multipart_upload(&*pg, &part_upload_id)
            .unwrap()
            .state,
        UploadState::InProgress
    );

    let wrong_pg = node.get_pg(wrong_pg_id).unwrap();
    assert_eq!(
        PgMetadataStore::get_stream_upload(&*wrong_pg, &session_id)
            .unwrap()
            .state,
        StreamUploadState::InProgress
    );
    assert_eq!(
        PgMetadataStore::list_stream_segments(&*wrong_pg, &session_id).unwrap(),
        vec![segment]
    );
    assert_eq!(
        wrong_pg
            .pending_metadata_command_envelope(config.node_id.as_u32(), config.cluster_epoch)
            .unwrap(),
        Some(wrong_command)
    );
}

#[test]
fn unix_retained_stream_abort_prepare_response_binds_reservation_subject() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::INITIAL,
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("retained-abort-response-bucket");
    let key = crate::tests::object_key("retained-abort-response-key");
    let session_id = crate::tests::stream_session_id("abort-response");
    let pg_id = ObjectMetadataPgId::new_for_test(PgId::new(0));
    let mut proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    proof.operation_kind =
        crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            pg_id.pg_id(),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::AbortStreamUpload(Box::new(
            crate::metadata_command::AbortStreamUploadCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                session_id: session_id.clone(),
                staged_segments: Vec::new(),
                stream_create_bucket_write_reservation: Some(proof.clone()),
            },
        )),
    );
    client
        .validate_retained_stream_upload_abort_prepare_command(
            &command,
            pg_id,
            ClusterEpoch::INITIAL,
            &bucket,
            &key,
            &session_id,
        )
        .unwrap();

    proof.operation_kind =
        crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();
    let upload_part_command = MetadataCommandEnvelope::new(
        command.id(),
        MetadataCommandPayload::AbortStreamUpload(Box::new(
            crate::metadata_command::AbortStreamUploadCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                session_id: session_id.clone(),
                staged_segments: Vec::new(),
                stream_create_bucket_write_reservation: Some(proof.clone()),
            },
        )),
    );
    client
        .validate_retained_stream_upload_abort_prepare_command(
            &upload_part_command,
            pg_id,
            ClusterEpoch::INITIAL,
            &bucket,
            &key,
            &session_id,
        )
        .unwrap();

    proof.operation_kind = "put-object".to_string();
    let malformed = MetadataCommandEnvelope::new(
        command.id(),
        MetadataCommandPayload::AbortStreamUpload(Box::new(
            crate::metadata_command::AbortStreamUploadCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                session_id: session_id.clone(),
                staged_segments: Vec::new(),
                stream_create_bucket_write_reservation: Some(proof),
            },
        )),
    );
    assert!(matches!(
        client.validate_retained_stream_upload_abort_prepare_command(
            &malformed,
            pg_id,
            ClusterEpoch::INITIAL,
            &bucket,
            &key,
            &session_id,
        ),
        Err(ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate retained stream upload abort prepare response",
            ..
        }))
    ));
}

#[test]
fn unix_object_metadata_scans_accept_installed_scan_pg_and_reject_unknown_pg() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    config.pg_routes.push(StorageNodePgRoute {
        pg_id: 1,
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        state: crate::types::PgState::Active,
        primary_node_id: NodeId::new(7),
        acting_set: vec![NodeId::new(7)],
    });
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..18)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );
    let bucket = crate::tests::bucket_name("object-listing-scan-pg-bucket");
    let installed_scan_pg = ObjectMetadataScanPgId::new_for_test(PgId::new(1));
    let unknown_scan_pg = ObjectMetadataScanPgId::new_for_test(PgId::new(2));

    let objects = ObjectListingMetadataNodeClient::list_objects_page(
        &client,
        installed_scan_pg,
        &ListObjectsReq {
            bucket: bucket.clone(),
            prefix: None,
            start_after: None,
            start_at: None,
            max_keys: 10,
        },
    )
    .unwrap();
    assert!(objects.objects.is_empty());
    assert!(!objects.is_truncated);
    assert!(objects.next_start_after.is_none());

    let versions = ObjectListingMetadataNodeClient::list_object_versions_page(
        &client,
        installed_scan_pg,
        &ListObjectVersionsReq {
            bucket: bucket.clone(),
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            start_at: None,
            max_keys: 10,
        },
    )
    .unwrap();
    assert!(versions.versions.is_empty());
    assert!(!versions.is_truncated);
    assert!(versions.next_key_marker.is_none());
    assert!(versions.next_version_id_marker.is_none());

    let uploads = ObjectListingMetadataNodeClient::list_multipart_uploads_page(
        &client,
        installed_scan_pg,
        &ListMultipartUploadsReq {
            bucket: bucket.clone(),
            prefix: None,
            page_start: None,
            max_uploads: 10,
        },
    )
    .unwrap();
    assert!(uploads.uploads.is_empty());
    assert!(!uploads.is_truncated);
    assert!(uploads.next_key_marker.is_none());
    assert!(uploads.next_upload_id_marker.is_none());

    let Err(object_error) = ObjectListingMetadataNodeClient::list_objects_page(
        &client,
        unknown_scan_pg,
        &ListObjectsReq {
            bucket: bucket.clone(),
            prefix: None,
            start_after: None,
            start_at: None,
            max_keys: 10,
        },
    ) else {
        panic!("unknown scan PG must not reach object listing");
    };
    assert!(matches!(
        object_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::UnknownPg,
            ..
        })
    ));

    let Err(version_error) = ObjectListingMetadataNodeClient::list_object_versions_page(
        &client,
        unknown_scan_pg,
        &ListObjectVersionsReq {
            bucket: bucket.clone(),
            prefix: None,
            key_marker: None,
            version_id_marker: None,
            start_at: None,
            max_keys: 10,
        },
    ) else {
        panic!("unknown scan PG must not reach object-version listing");
    };
    assert!(matches!(
        version_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::UnknownPg,
            ..
        })
    ));

    let Err(upload_error) = ObjectListingMetadataNodeClient::list_multipart_uploads_page(
        &client,
        unknown_scan_pg,
        &ListMultipartUploadsReq {
            bucket: bucket.clone(),
            prefix: None,
            page_start: None,
            max_uploads: 10,
        },
    ) else {
        panic!("unknown scan PG must not reach multipart-upload listing");
    };
    assert!(matches!(
        upload_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::UnknownPg,
            ..
        })
    ));

    let bucket_streams = ObjectMutationMetadataNodeClient::list_stream_uploads_for_bucket_page(
        &client,
        installed_scan_pg,
        &bucket,
        None,
        10,
    )
    .unwrap();
    assert!(bucket_streams.uploads.is_empty());
    assert!(bucket_streams.next_session_id_marker.is_none());

    let all_streams = ObjectMutationMetadataNodeClient::list_all_stream_uploads_page(
        &client,
        installed_scan_pg,
        None,
        10,
    )
    .unwrap();
    assert!(all_streams.uploads.is_empty());
    assert!(all_streams.next_session_id_marker.is_none());

    let bucket_stream_error =
        ObjectMutationMetadataNodeClient::list_stream_uploads_for_bucket_page(
            &client,
            unknown_scan_pg,
            &bucket,
            None,
            10,
        )
        .unwrap_err();
    assert!(matches!(
        bucket_stream_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::UnknownPg,
            ..
        })
    ));

    let all_stream_error = ObjectMutationMetadataNodeClient::list_all_stream_uploads_page(
        &client,
        unknown_scan_pg,
        None,
        10,
    )
    .unwrap_err();
    assert!(matches!(
        all_stream_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::UnknownPg,
            ..
        })
    ));

    assert!(
        ObjectMutationMetadataNodeClient::get_bucket_payload_reclaim_root(
            &client,
            installed_scan_pg,
            &bucket,
        )
        .unwrap()
        .is_none()
    );
    assert!(
        ObjectMutationMetadataNodeClient::get_payload_reclaim_root(&client, installed_scan_pg)
            .unwrap()
            .is_none()
    );
    assert!(
        ObjectMutationMetadataNodeClient::object_payload_reclaim_claim(&client, installed_scan_pg,)
            .unwrap()
            .is_none()
    );

    for error in [
        ObjectMutationMetadataNodeClient::get_bucket_payload_reclaim_root(
            &client,
            unknown_scan_pg,
            &bucket,
        )
        .unwrap_err(),
        ObjectMutationMetadataNodeClient::get_payload_reclaim_root(&client, unknown_scan_pg)
            .unwrap_err(),
        ObjectMutationMetadataNodeClient::object_payload_reclaim_claim(&client, unknown_scan_pg)
            .unwrap_err(),
    ] {
        assert!(matches!(
            error,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                failure: StorageRpcErrorCode::UnknownPg,
                ..
            })
        ));
    }
    assert!(
        ShardScavengerNodeClient::list_shard_scavenger_payload_references(
            &client,
            installed_scan_pg,
        )
        .unwrap()
        .is_empty()
    );
    assert!(matches!(
        ShardScavengerNodeClient::list_shard_scavenger_payload_references(
            &client,
            unknown_scan_pg,
        )
        .unwrap_err(),
        StoreError::StorageRpc {
            failure: StorageRpcErrorCode::UnknownPg,
            ..
        }
    ));

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_object_generation_metadata_client_routes_generation_reads() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("object-generation-rpc-bucket");
    let key = crate::tests::object_key("object-generation-rpc-key");
    let reservation_id = crate::tests::stream_session_id("obj-gen-rpc");
    let reserved_generation = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        let generation =
            PgMetadataStore::reserve_object_generation(&*pg, &bucket, &key, &reservation_id)
                .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        generation
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..2)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    assert_eq!(
        client
            .object_generation_reservation(
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
                &reservation_id,
            )
            .unwrap(),
        reserved_generation
    );
    assert_eq!(
        client
            .next_object_generation_id(
                ObjectMetadataPgId::new_for_test(PgId::new(0)),
                &bucket,
                &key,
            )
            .unwrap(),
        GenerationId::new(reserved_generation.get() + 1).unwrap()
    );
    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_object_version_metadata_client_routes_version_reads() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );
    let bucket = crate::tests::bucket_name("object-version-rpc-bucket");
    let key = crate::tests::object_key("object-version-rpc-key");

    assert_eq!(
        ObjectVersionMetadataNodeClient::next_object_version_id(
            &client,
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &bucket,
            &key
        )
        .unwrap(),
        VersionId::from_u64(1)
    );
    server_thread.join().unwrap();
}

#[test]
fn unix_object_metadata_clients_reject_wrong_object_pg_before_node_access() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    config.pg_routes = vec![
        StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: crate::types::PgState::Active,
            primary_node_id: NodeId::new(7),
            acting_set: vec![NodeId::new(7)],
        },
        StorageNodePgRoute {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: crate::types::PgState::Active,
            primary_node_id: NodeId::new(7),
            acting_set: vec![NodeId::new(7)],
        },
    ];
    let reservation_id = crate::tests::stream_session_id("obj-rpc-wrong-pg");
    let (bucket, key, correct_pg_id, wrong_pg_id, reserved_generation) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let (bucket, key, correct_pg_id) = (0..100)
            .map(|index| {
                let bucket =
                    crate::tests::bucket_name(format!("object-rpc-wrong-pg-bucket-{index}"));
                let key = crate::tests::object_key(format!("object-rpc-wrong-pg-key-{index}"));
                let pg_id = node.pg_topology().object_pg_for(&bucket, &key);
                (bucket, key, pg_id)
            })
            .find(|(_, _, pg_id)| *pg_id < 2)
            .expect("two-PG topology must place a test object");
        let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
        let pg = node.get_pg(correct_pg_id).unwrap();
        let reserved_generation =
            PgMetadataStore::reserve_object_generation(&*pg, &bucket, &key, &reservation_id)
                .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        let wrong_pg = node.get_pg(wrong_pg_id).unwrap();
        let wrong_pg_reserved_generation =
            PgMetadataStore::reserve_object_generation(&*wrong_pg, &bucket, &key, &reservation_id)
                .unwrap();
        wrong_pg.refresh_metadata_command_state_digest().unwrap();
        assert_eq!(
            wrong_pg_reserved_generation, reserved_generation,
            "equivalent wrong-PG state must make an unguarded direct-PUT lookup succeed"
        );
        {
            let _time = crate::clock::test_time_override_guard(1_000);
            for object_pg in [&pg, &wrong_pg] {
                PgMetadataStore::put_object_with_segments(
                    &**object_pg,
                    &PutLiveObjectReq {
                        bucket: bucket.clone(),
                        key: key.clone(),
                        version_id: VersionId::Null,
                        owner: OwnerIdentity::from_principal("owner"),
                        acl_grants: AclGrants::default(),
                        public_read: false,
                        generation_id: GenerationId::new(9).unwrap(),
                        size: 0,
                        etag: ObjectEtag::single_part(99),
                        ec: EcShape { k: 4, m: 2 },
                        layout: ObjectLayout::Standard,
                        tags: Some(SerializedTagSet::new(
                            "<Tagging><TagSet><Tag><Key>route</Key><Value>canary</Value></Tag></TagSet></Tagging>"
                                .to_string(),
                        )),
                        metadata_blob: None,
                        system_metadata_blob: None,
                        object_lock: ObjectLockState::default(),
                        encryption: ObjectEncryption::None,
                    },
                    &[],
                )
                .unwrap();
                object_pg.refresh_metadata_command_state_digest().unwrap();
            }
        }
        let correct_subject = SharedStorageNode::load_object_read_auth_subject_from_object_pg(
            &pg, &bucket, &key, None,
        )
        .unwrap();
        let wrong_subject = SharedStorageNode::load_object_read_auth_subject_from_object_pg(
            &wrong_pg, &bucket, &key, None,
        )
        .unwrap();
        assert_eq!(
            wrong_subject.identity, correct_subject.identity,
            "equivalent wrong-PG state must make unguarded subject-bound reads succeed"
        );
        (bucket, key, correct_pg_id, wrong_pg_id, reserved_generation)
    };

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..36)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );
    let wrong_object_pg = ObjectMetadataPgId::new_for_test(PgId::new(wrong_pg_id));
    let correct_object_pg = ObjectMetadataPgId::new_for_test(PgId::new(correct_pg_id));

    let reservation_error = ObjectGenerationMetadataNodeClient::object_generation_reservation(
        &client,
        wrong_object_pg,
        &bucket,
        &key,
        &reservation_id,
    )
    .unwrap_err();
    assert!(matches!(
        reservation_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let version_error = ObjectVersionMetadataNodeClient::next_object_version_id(
        &client,
        wrong_object_pg,
        &bucket,
        &key,
    )
    .unwrap_err();
    assert!(matches!(
        version_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    assert_eq!(
        ObjectGenerationMetadataNodeClient::object_generation_reservation(
            &client,
            correct_object_pg,
            &bucket,
            &key,
            &reservation_id,
        )
        .unwrap(),
        reserved_generation
    );

    let read_subject = ObjectReadMetadataNodeClient::load_object_read_auth_subject(
        &client,
        correct_object_pg,
        &bucket,
        &key,
        None,
    )
    .unwrap();
    let read_subject_error = ObjectReadMetadataNodeClient::load_object_read_auth_subject(
        &client,
        wrong_object_pg,
        &bucket,
        &key,
        None,
    )
    .unwrap_err();
    assert!(matches!(
        read_subject_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let read_snapshot_error = ObjectReadMetadataNodeClient::load_object_read_snapshot_for_subject(
        &client,
        wrong_object_pg,
        &bucket,
        &key,
        None,
        &read_subject.identity,
        ObjectReadSnapshotMode::StandardSegments,
    )
    .unwrap_err();
    assert!(matches!(
        read_snapshot_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let read_tags_error = ObjectReadMetadataNodeClient::get_object_tags_for_subject(
        &client,
        wrong_object_pg,
        &bucket,
        &key,
        None,
        &read_subject.identity,
        VersionId::Null,
    )
    .unwrap_err();
    assert!(matches!(
        read_tags_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let metadata_stored = ObjectMutationMetadataNodeClient::load_put_object_metadata_snapshot(
        &client,
        correct_object_pg,
        &bucket,
        &key,
        None,
    )
    .unwrap();
    let metadata_snapshot_error =
        ObjectMutationMetadataNodeClient::load_put_object_metadata_snapshot(
            &client,
            wrong_object_pg,
            &bucket,
            &key,
            None,
        )
        .unwrap_err();
    assert!(matches!(
        metadata_snapshot_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    let metadata_proof = BucketWriteReservationProof {
        bucket: bucket.clone(),
        reservation_id: "wrong-object-pg-metadata-proof".to_string(),
        owner_token: "wrong-object-pg-metadata-owner".to_string(),
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        operation_kind: "put-object-metadata".to_string(),
        created_at: 10,
        lease_deadline: 20,
        target_context: Some(key.as_str().to_string()),
    };
    let mut delete_current_proof = metadata_proof.clone();
    delete_current_proof.operation_kind = "delete-current-object".to_string();
    let mut delete_specific_proof = metadata_proof.clone();
    delete_specific_proof.operation_kind = "delete-object-version".to_string();
    let mut marker_proof = metadata_proof.clone();
    marker_proof.operation_kind = "insert-delete-marker".to_string();
    let mut stream_proof = metadata_proof.clone();
    stream_proof.operation_kind =
        crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();
    let current_delete_snapshot =
        ObjectMutationMetadataNodeClient::load_current_object_delete_snapshot(
            &client,
            correct_object_pg,
            &bucket,
            &key,
        )
        .unwrap();
    assert_eq!(
        current_delete_snapshot.stored.as_ref(),
        Some(&metadata_stored)
    );
    let current_delete_error =
        ObjectMutationMetadataNodeClient::load_current_object_delete_snapshot(
            &client,
            wrong_object_pg,
            &bucket,
            &key,
        )
        .unwrap_err();
    assert!(matches!(
        current_delete_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let specific_delete_snapshot =
        ObjectMutationMetadataNodeClient::load_specific_object_delete_snapshot(
            &client,
            correct_object_pg,
            &bucket,
            &key,
            VersionId::Null,
        )
        .unwrap();
    assert_eq!(
        specific_delete_snapshot.stored,
        current_delete_snapshot.stored
    );
    assert!(matches!(
        (
            specific_delete_snapshot.target.as_ref(),
            current_delete_snapshot.target.as_ref(),
        ),
        (
            Some(DeleteObjectVersionTarget::Live {
                generation_id: specific_generation,
                layout: specific_layout,
                ..
            }),
            Some(DeleteObjectVersionTarget::Live {
                generation_id: current_generation,
                layout: current_layout,
                ..
            }),
        ) if specific_generation == current_generation && specific_layout == current_layout
    ));
    let specific_delete_error =
        ObjectMutationMetadataNodeClient::load_specific_object_delete_snapshot(
            &client,
            wrong_object_pg,
            &bucket,
            &key,
            VersionId::Null,
        )
        .unwrap_err();
    assert!(matches!(
        specific_delete_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let lifecycle_versions = ObjectMutationMetadataNodeClient::list_object_versions_for_lifecycle(
        &client,
        correct_object_pg,
        &bucket,
        &key,
    )
    .unwrap();
    assert_eq!(lifecycle_versions, vec![metadata_stored.clone()]);
    let lifecycle_list_error =
        ObjectMutationMetadataNodeClient::list_object_versions_for_lifecycle(
            &client,
            wrong_object_pg,
            &bucket,
            &key,
        )
        .unwrap_err();
    assert!(matches!(
        lifecycle_list_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let stream_request = CreateStreamUploadReq {
        session_id: crate::tests::stream_session_id("wrong-pg-stream"),
        bucket: bucket.clone(),
        key: key.clone(),
        target: StreamUploadTarget::PutObject,
        encryption: ObjectEncryption::None,
    };
    assert!(
        !ObjectMutationMetadataNodeClient::matching_stream_upload_exists(
            &client,
            correct_object_pg,
            &stream_request,
            None,
        )
        .unwrap(),
        "correctly routed absent stream session must be a positive request canary"
    );
    let stream_match_error = ObjectMutationMetadataNodeClient::matching_stream_upload_exists(
        &client,
        wrong_object_pg,
        &stream_request,
        None,
    )
    .unwrap_err();
    assert!(matches!(
        stream_match_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    let stream_command_error =
        ObjectMutationMetadataNodeClient::build_create_stream_upload_command(
            &client,
            BuildCreateStreamUploadCommandReq {
                pg_id: wrong_object_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &stream_request,
                cleanup_after: None,
                precondition: CreateStreamUploadPrecondition::PutObject {
                    expected_current: current_delete_snapshot.stored.as_ref(),
                    require_generation_reservation: false,
                },
                bucket_write_reservation: &stream_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        stream_command_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let multipart_request = CreateMultipartUploadReq {
        upload_id: crate::tests::multipart_upload_id("wrong-object-pg-multipart"),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: SerializedMetadataBlob::default(),
        system_metadata_blob: SerializedSystemMetadataBlob::default(),
        initiator: OwnerIdentity::from_principal("owner"),
        owner: OwnerIdentity::from_principal("owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        object_lock: ObjectLockState::default(),
        checksum: None,
        encryption: ObjectEncryption::None,
    };
    assert_eq!(
        ObjectMutationMetadataNodeClient::matching_multipart_upload_initiated_at(
            &client,
            correct_object_pg,
            &multipart_request,
            None,
        )
        .unwrap(),
        None,
        "correctly routed absent multipart upload must be a positive request canary"
    );
    let multipart_match_error =
        ObjectMutationMetadataNodeClient::matching_multipart_upload_initiated_at(
            &client,
            wrong_object_pg,
            &multipart_request,
            None,
        )
        .unwrap_err();
    assert!(matches!(
        multipart_match_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    let mut create_multipart_proof = metadata_proof.clone();
    create_multipart_proof.operation_kind =
        crate::metadata_command::CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND.to_string();
    let multipart_command_error =
        ObjectMutationMetadataNodeClient::build_create_multipart_upload_command(
            &client,
            BuildCreateMultipartUploadCommandReq {
                pg_id: wrong_object_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &multipart_request,
                expected_current: current_delete_snapshot.stored.as_ref(),
                bucket_write_reservation: &create_multipart_proof,
            },
        )
        .unwrap_err();
    match multipart_command_error {
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            detail,
            ..
        }) => {
            assert!(detail
                .as_str()
                .contains("multipart upload command build PG"));
            assert!(detail.as_str().contains("does not match object"));
        }
        other => panic!("wrong-PG multipart command build must fail placement: {other:?}"),
    }

    let delete_current_command_error =
        ObjectMutationMetadataNodeClient::build_delete_current_object_command(
            &client,
            BuildDeleteCurrentObjectCommandReq {
                pg_id: wrong_object_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                expected_current: current_delete_snapshot.stored.as_ref(),
                expected_target: current_delete_snapshot.target.as_ref(),
                bucket_write_reservation: &delete_current_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        delete_current_command_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let delete_specific_command_error =
        ObjectMutationMetadataNodeClient::build_delete_specific_object_version_command(
            &client,
            BuildDeleteSpecificObjectVersionCommandReq {
                pg_id: wrong_object_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                version_id: VersionId::Null,
                expected_stored: specific_delete_snapshot.stored.as_ref(),
                expected_target: specific_delete_snapshot.target.as_ref(),
                expected_version_list: None,
                bucket_write_reservation: &delete_specific_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        delete_specific_command_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let owner = OwnerIdentity::from_principal("owner");
    let insert_delete_marker_command_error =
        ObjectMutationMetadataNodeClient::build_insert_delete_marker_command(
            &client,
            BuildInsertDeleteMarkerCommandReq {
                pg_id: wrong_object_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                expected_current: current_delete_snapshot.stored.as_ref(),
                version_id: VersionId::from_u64(2),
                owner: &owner,
                stale_payload: InsertDeleteMarkerStalePayload::Explicit(None),
                expected_stale_payload_source: None,
                bucket_write_reservation: &marker_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        insert_delete_marker_command_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let mismatched_stale_payload_error =
        ObjectMutationMetadataNodeClient::build_insert_delete_marker_command(
            &client,
            BuildInsertDeleteMarkerCommandReq {
                pg_id: correct_object_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                expected_current: current_delete_snapshot.stored.as_ref(),
                version_id: VersionId::from_u64(2),
                owner: &owner,
                stale_payload: InsertDeleteMarkerStalePayload::Explicit(Some(
                    ObjectPayloadReclaimCommand::Segments(ObjectSegmentsReclaimRecord {
                        bucket: crate::tests::bucket_name("wrong-stale-payload-bucket"),
                        key: key.clone(),
                        generation_id: GenerationId::new(9).unwrap(),
                        created_at: 1_000,
                        segments: Vec::new(),
                    }),
                )),
                expected_stale_payload_source: None,
                bucket_write_reservation: &marker_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        mismatched_stale_payload_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let null_marker_without_snapshot_error =
        ObjectMutationMetadataNodeClient::build_insert_delete_marker_command(
            &client,
            BuildInsertDeleteMarkerCommandReq {
                pg_id: correct_object_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                expected_current: current_delete_snapshot.stored.as_ref(),
                version_id: VersionId::Null,
                owner: &owner,
                stale_payload: InsertDeleteMarkerStalePayload::Explicit(None),
                expected_stale_payload_source: None,
                bucket_write_reservation: &marker_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        null_marker_without_snapshot_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let numbered_marker_with_snapshot_error =
        ObjectMutationMetadataNodeClient::build_insert_delete_marker_command(
            &client,
            BuildInsertDeleteMarkerCommandReq {
                pg_id: correct_object_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                expected_current: current_delete_snapshot.stored.as_ref(),
                version_id: VersionId::from_u64(2),
                owner: &owner,
                stale_payload: InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                    created_at: 10,
                },
                expected_stale_payload_source: current_delete_snapshot.stored.as_ref(),
                bucket_write_reservation: &marker_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        numbered_marker_with_snapshot_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let numbered_marker_with_source_error =
        ObjectMutationMetadataNodeClient::build_insert_delete_marker_command(
            &client,
            BuildInsertDeleteMarkerCommandReq {
                pg_id: correct_object_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                expected_current: current_delete_snapshot.stored.as_ref(),
                version_id: VersionId::from_u64(2),
                owner: &owner,
                stale_payload: InsertDeleteMarkerStalePayload::Explicit(None),
                expected_stale_payload_source: current_delete_snapshot.stored.as_ref(),
                bucket_write_reservation: &marker_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        numbered_marker_with_source_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let mut versioned_stale_source = metadata_stored.clone();
    let StoredObject::Live(versioned_stale_record) = &mut versioned_stale_source else {
        panic!("seeded metadata object must be live");
    };
    versioned_stale_record.version_id = VersionId::from_u64(3);
    let null_marker_with_versioned_source_error =
        ObjectMutationMetadataNodeClient::build_insert_delete_marker_command(
            &client,
            BuildInsertDeleteMarkerCommandReq {
                pg_id: correct_object_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                expected_current: current_delete_snapshot.stored.as_ref(),
                version_id: VersionId::Null,
                owner: &owner,
                stale_payload: InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                    created_at: 10,
                },
                expected_stale_payload_source: Some(&versioned_stale_source),
                bucket_write_reservation: &marker_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        null_marker_with_versioned_source_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let mut wrong_operation_proof = metadata_proof.clone();
    wrong_operation_proof.operation_kind = "delete-object-version".to_string();
    let mut wrong_target_proof = metadata_proof.clone();
    wrong_target_proof.target_context = Some("same-bucket-other-key".to_string());
    let mut wrong_epoch_proof = metadata_proof.clone();
    wrong_epoch_proof.cluster_epoch = ClusterEpoch::new(2).unwrap();
    for (mismatch, proof) in [
        ("operation", wrong_operation_proof),
        ("target", wrong_target_proof),
        ("epoch", wrong_epoch_proof),
    ] {
        let error = ObjectMutationMetadataNodeClient::build_put_object_metadata_command(
            &client,
            BuildPutObjectMetadataCommandReq {
                pg_id: correct_object_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                requested_version_id: None,
                expected_stored: &metadata_stored,
                version_id: VersionId::Null,
                mutation: PutObjectMetadataMutation::PutTags(SerializedTagSet::default()),
                bucket_write_reservation: &proof,
            },
        )
        .unwrap_err();
        assert!(
            matches!(
                error,
                ObjectPgActionError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                })
            ),
            "same-bucket proof with mismatched {mismatch} must fail as PayloadDecode: {error:?}"
        );
    }

    let metadata_command_error =
        ObjectMutationMetadataNodeClient::build_put_object_metadata_command(
            &client,
            BuildPutObjectMetadataCommandReq {
                pg_id: wrong_object_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                requested_version_id: None,
                expected_stored: &metadata_stored,
                version_id: VersionId::Null,
                mutation: PutObjectMetadataMutation::PutTags(SerializedTagSet::default()),
                bucket_write_reservation: &metadata_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        metadata_command_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let snapshot_error = DirectPutMetadataNodeClient::load_direct_put_commit_snapshot(
        &client,
        wrong_object_pg,
        &bucket,
        &key,
        &reservation_id,
        reserved_generation,
    )
    .unwrap_err();
    assert!(matches!(
        snapshot_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let snapshot = DirectPutMetadataNodeClient::load_direct_put_commit_snapshot(
        &client,
        correct_object_pg,
        &bucket,
        &key,
        &reservation_id,
        reserved_generation,
    )
    .unwrap();
    let bucket_write_reservation = BucketWriteReservationProof {
        bucket: bucket.clone(),
        reservation_id: "wrong-object-pg-proof".to_string(),
        owner_token: "wrong-object-pg-owner".to_string(),
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        operation_kind: "direct-put".to_string(),
        created_at: 10,
        lease_deadline: 20,
        target_context: Some(key.as_str().to_string()),
    };
    let direct_put_request = CommitDirectPutObjectReq {
        bucket: bucket.clone(),
        key: key.clone(),
        generation_reservation_id: reservation_id.clone(),
        versioning: BucketVersioningState::Suspended,
        owner: OwnerIdentity {
            principal: "owner".to_string(),
            canonical_id: crate::CanonicalUserId::from_principal("owner"),
        },
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        generation_id: reserved_generation,
        size: 12,
        etag_crc64: 99,
        ec: EcShape { k: 4, m: 2 },
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        segment_index: 0,
        segment_crc64: 99,
        segment_okh: [7; 16],
        segment_vid: GenerationId::new(10).unwrap(),
        data_pg_id: 0,
        bucket_write_reservation: bucket_write_reservation.clone(),
    };
    let command_error = DirectPutMetadataNodeClient::build_direct_put_commit_command(
        &client,
        BuildDirectPutCommitCommandReq {
            pg_id: wrong_object_pg,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            request: &direct_put_request,
            version_id: VersionId::Null,
            expected_snapshot: &snapshot,
            bucket_write_reservation: &bucket_write_reservation,
        },
    )
    .unwrap_err();
    assert!(matches!(
        command_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_stream_metadata_rejects_wrong_object_pg_before_node_access() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    config.pg_routes = vec![
        StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: crate::types::PgState::Active,
            primary_node_id: NodeId::new(7),
            acting_set: vec![NodeId::new(7)],
        },
        StorageNodePgRoute {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: crate::types::PgState::Active,
            primary_node_id: NodeId::new(7),
            acting_set: vec![NodeId::new(7)],
        },
    ];
    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let (bucket, key, correct_pg_id) = (0..100)
        .map(|index| {
            let bucket = crate::tests::bucket_name(format!("stream-rpc-wrong-pg-bucket-{index}"));
            let key = crate::tests::object_key(format!("stream-rpc-wrong-pg-key-{index}"));
            let pg_id = node.pg_topology().object_pg_for(&bucket, &key);
            (bucket, key, pg_id)
        })
        .find(|(_, _, pg_id)| *pg_id < 2)
        .expect("two-PG topology must place a test object");
    let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
    let session_id = crate::tests::stream_session_id("wrong-pg-stream");
    let create = CreateStreamUploadReq {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: StreamUploadTarget::PutObject,
        encryption: ObjectEncryption::None,
    };
    let mut current_proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    current_proof.operation_kind =
        crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();
    let mut renewed_proof = current_proof.clone();
    renewed_proof.lease_deadline += 1;
    let mut substituted_put_proof = renewed_proof.clone();
    substituted_put_proof.reservation_id = "wrong-pg-stream-substitute".to_string();
    let mut part_finalize_proof = current_proof.clone();
    part_finalize_proof.operation_kind =
        crate::metadata_command::UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND
            .to_string();
    let upload_id = crate::tests::multipart_upload_id("wrong-pg-stream-part-upload");
    let part_session_id = crate::tests::stream_session_id("wrong-pg-part");
    let multipart_create = CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: SerializedMetadataBlob::default(),
        system_metadata_blob: SerializedSystemMetadataBlob::default(),
        initiator: OwnerIdentity::from_principal("owner"),
        owner: OwnerIdentity::from_principal("owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        object_lock: ObjectLockState::default(),
        checksum: None,
        encryption: ObjectEncryption::None,
    };
    let part_create = CreateStreamUploadReq {
        session_id: part_session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: StreamUploadTarget::UploadPart {
            upload_id: upload_id.clone(),
            part_number: 1,
        },
        encryption: ObjectEncryption::None,
    };
    let mut reserved_generation = None;
    let _time = crate::clock::test_time_override_guard(1_000);
    for pg_id in [correct_pg_id, wrong_pg_id] {
        let pg = node.get_pg(pg_id).unwrap();
        let generation =
            PgMetadataStore::reserve_object_generation(&*pg, &bucket, &key, &session_id).unwrap();
        let stream_create_command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(pg_id),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateStreamUpload(Box::new(
                CreateStreamUploadCommand::from_request_with_bucket_write_reservation_and_cleanup_deadline(
                    create.clone(),
                    1_000,
                    Some(9_000),
                    current_proof.clone(),
                ),
            )),
        );
        pg.apply_metadata_command(&stream_create_command).unwrap();
        PgMetadataStore::create_multipart_upload(&*pg, &multipart_create).unwrap();
        PgMetadataStore::create_stream_upload(&*pg, &part_create).unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        if let Some(expected) = reserved_generation {
            assert_eq!(
                generation, expected,
                "equivalent wrong-PG state must make unguarded stream finalization succeed"
            );
        } else {
            reserved_generation = Some(generation);
        }
    }
    let multipart_upload =
        PgMetadataStore::get_multipart_upload(&*node.get_pg(correct_pg_id).unwrap(), &upload_id)
            .unwrap();
    drop(node);

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..24)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );
    let correct_pg = ObjectMetadataPgId::new_for_test(PgId::new(correct_pg_id));
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(wrong_pg_id));
    let new_part_create = CreateStreamUploadReq {
        session_id: crate::tests::stream_session_id("new-part"),
        bucket: bucket.clone(),
        key: key.clone(),
        target: StreamUploadTarget::UploadPart {
            upload_id: upload_id.clone(),
            part_number: 2,
        },
        encryption: ObjectEncryption::None,
    };
    let mut part_create_proof = current_proof.clone();
    part_create_proof.operation_kind =
        crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();
    let part_create_command = ObjectMutationMetadataNodeClient::build_create_stream_upload_command(
        &client,
        BuildCreateStreamUploadCommandReq {
            pg_id: correct_pg,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            request: &new_part_create,
            cleanup_after: None,
            precondition: CreateStreamUploadPrecondition::UploadPart {
                expected_upload: &multipart_upload,
            },
            bucket_write_reservation: &part_create_proof,
        },
    )
    .unwrap();
    assert!(matches!(
        part_create_command.payload(),
        MetadataCommandPayload::CreateStreamUpload(command)
            if command.session.session_id == new_part_create.session_id
                && command.bucket_write_reservation == part_create_proof
    ));
    let wrong_part_create_error =
        ObjectMutationMetadataNodeClient::build_create_stream_upload_command(
            &client,
            BuildCreateStreamUploadCommandReq {
                pg_id: wrong_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &new_part_create,
                cleanup_after: None,
                precondition: CreateStreamUploadPrecondition::UploadPart {
                    expected_upload: &multipart_upload,
                },
                bucket_write_reservation: &part_create_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        wrong_part_create_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    let crossed_part_create_error =
        ObjectMutationMetadataNodeClient::build_create_stream_upload_command(
            &client,
            BuildCreateStreamUploadCommandReq {
                pg_id: correct_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &new_part_create,
                cleanup_after: None,
                precondition: CreateStreamUploadPrecondition::UploadPart {
                    expected_upload: &multipart_upload,
                },
                bucket_write_reservation: &current_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        crossed_part_create_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let session = ObjectMutationMetadataNodeClient::load_stream_upload_session(
        &client,
        correct_pg,
        &bucket,
        &key,
        &session_id,
    )
    .unwrap();
    assert_eq!(session.session_id, session_id);
    assert_eq!(session.cleanup_after, Some(9_000));
    let wrong_session_error = ObjectMutationMetadataNodeClient::load_stream_upload_session(
        &client,
        wrong_pg,
        &bucket,
        &key,
        &session_id,
    )
    .unwrap_err();
    assert!(matches!(
        wrong_session_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    assert!(
        ObjectMutationMetadataNodeClient::load_stream_upload_segments(
            &client,
            correct_pg,
            &bucket,
            &key,
            &session_id,
        )
        .unwrap()
        .is_empty()
    );
    let wrong_segments_error = ObjectMutationMetadataNodeClient::load_stream_upload_segments(
        &client,
        wrong_pg,
        &bucket,
        &key,
        &session_id,
    )
    .unwrap_err();
    assert!(matches!(
        wrong_segments_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let snapshot = ObjectMutationMetadataNodeClient::load_stream_put_finalize_snapshot(
        &client,
        correct_pg,
        &bucket,
        &key,
        &session_id,
    )
    .unwrap();
    assert_eq!(snapshot.generation_id, reserved_generation.unwrap());
    let wrong_snapshot_error = ObjectMutationMetadataNodeClient::load_stream_put_finalize_snapshot(
        &client,
        wrong_pg,
        &bucket,
        &key,
        &session_id,
    )
    .unwrap_err();
    assert!(matches!(
        wrong_snapshot_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let effect_fence = crate::types::AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL);
    ObjectMutationMetadataNodeClient::update_stream_upload_bucket_write_reservation_with_effect_fence(
        &client,
        UpdateStreamUploadBucketWriteReservationReq {
            pg_id: correct_pg,
            bucket: &bucket,
            key: &key,
            session_id: &session_id,
            current: &current_proof,
            renewed: &renewed_proof,
            effect_fence,
        },
    )
    .unwrap();
    let wrong_update_error =
        ObjectMutationMetadataNodeClient::update_stream_upload_bucket_write_reservation_with_effect_fence(
            &client,
            UpdateStreamUploadBucketWriteReservationReq {
                pg_id: wrong_pg,
                bucket: &bucket,
                key: &key,
                session_id: &session_id,
                current: &current_proof,
                renewed: &renewed_proof,
                effect_fence,
            },
        )
        .unwrap_err();
    assert!(matches!(
        wrong_update_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    let snapshot = ObjectMutationMetadataNodeClient::load_stream_put_finalize_snapshot(
        &client,
        correct_pg,
        &bucket,
        &key,
        &session_id,
    )
    .unwrap();

    let commit = StreamPutCommitInput {
        versioning: BucketVersioningState::Suspended,
        version_id: VersionId::Null,
        owner: OwnerIdentity::from_principal("owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        size: 0,
        etag_crc64: 0,
        tags: None,
        metadata_blob: SerializedMetadataBlob::default(),
        system_metadata_blob: SerializedSystemMetadataBlob::default(),
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    };
    let correct_command = ObjectMutationMetadataNodeClient::build_stream_put_commit_command(
        &client,
        BuildStreamPutCommitCommandReq {
            pg_id: correct_pg,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket: &bucket,
            key: &key,
            session_id: &session_id,
            total_size: 0,
            expected_snapshot: &snapshot,
            commit: &commit,
            bucket_write_reservation: &renewed_proof,
        },
    )
    .unwrap();
    assert!(matches!(
        correct_command.payload(),
        MetadataCommandPayload::CommitDirectPutObject(command)
            if command.matches_request(&bucket, &key, &session_id, snapshot.generation_id)
                && command.bucket_write_reservation == renewed_proof
    ));
    let wrong_command_error = ObjectMutationMetadataNodeClient::build_stream_put_commit_command(
        &client,
        BuildStreamPutCommitCommandReq {
            pg_id: wrong_pg,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket: &bucket,
            key: &key,
            session_id: &session_id,
            total_size: 0,
            expected_snapshot: &snapshot,
            commit: &commit,
            bucket_write_reservation: &renewed_proof,
        },
    )
    .unwrap_err();
    assert!(matches!(
        wrong_command_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    let crossed_put_proof_error =
        ObjectMutationMetadataNodeClient::build_stream_put_commit_command(
            &client,
            BuildStreamPutCommitCommandReq {
                pg_id: correct_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                session_id: &session_id,
                total_size: 0,
                expected_snapshot: &snapshot,
                commit: &commit,
                bucket_write_reservation: &part_finalize_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        crossed_put_proof_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    let substituted_put_proof_error =
        ObjectMutationMetadataNodeClient::build_stream_put_commit_command(
            &client,
            BuildStreamPutCommitCommandReq {
                pg_id: correct_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                session_id: &session_id,
                total_size: 0,
                expected_snapshot: &snapshot,
                commit: &commit,
                bucket_write_reservation: &substituted_put_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        substituted_put_proof_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let append = PrepareStreamUploadSegmentAppendReq {
        session_id: session_id.clone(),
        segment_index: 0,
        size: 12,
        segment_crc64: 99,
        payload_crc64: 99,
        segment_okh: [7; 16],
    };
    let (target, _) = ObjectMutationMetadataNodeClient::prepare_stream_segment_append(
        &client, correct_pg, &bucket, &key, &append,
    )
    .unwrap();
    assert_eq!(target, StreamUploadTarget::PutObject);
    let append_rpc_request = crate::storage_rpc::StorageRpcStreamSegmentAppendPrepareRequest {
        object: client.object_request(wrong_pg.pg_id(), &bucket, &key),
        request: append,
        effect_deadline: None,
    };
    let wrong_append_error = client
        .rpc_request(
            crate::storage_rpc::StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare,
            crate::storage_rpc::encode_stream_segment_append_prepare_request(&append_rpc_request),
        )
        .unwrap_err();
    assert!(matches!(
        wrong_append_error,
        StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        }
    ));

    let part_snapshot = ObjectMutationMetadataNodeClient::load_stream_part_finalize_snapshot(
        &client,
        correct_pg,
        &bucket,
        &key,
        &upload_id,
        &part_session_id,
        1,
    )
    .unwrap();
    assert_eq!(part_snapshot.auth_snapshot.upload.upload_id, upload_id);
    assert_eq!(
        part_snapshot.auth_snapshot.session.session_id,
        part_session_id
    );
    let wrong_part_snapshot_error =
        ObjectMutationMetadataNodeClient::load_stream_part_finalize_snapshot(
            &client,
            wrong_pg,
            &bucket,
            &key,
            &upload_id,
            &part_session_id,
            1,
        )
        .unwrap_err();
    assert!(matches!(
        wrong_part_snapshot_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    let part = MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: 0,
        payload_crc64: 0,
        etag: Vec::new(),
        etag_kind: EtagKind::Crc64,
        part_vid: GenerationId::MIN,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
        last_modified: 1,
        checksum: None,
    };
    let correct_part_command = ObjectMutationMetadataNodeClient::build_stream_part_commit_command(
        &client,
        BuildStreamPartCommitCommandReq {
            pg_id: correct_pg,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket: &bucket,
            key: &key,
            upload_id: &upload_id,
            session_id: &part_session_id,
            part_number: 1,
            expected_snapshot: &part_snapshot,
            part: &part,
            segments: &[],
            bucket_write_reservation: &part_finalize_proof,
        },
    )
    .unwrap();
    assert!(matches!(
        correct_part_command.payload(),
        MetadataCommandPayload::CommitStreamPart(commit)
            if commit.upload.upload_id == upload_id && commit.session_id == part_session_id
    ));
    let wrong_part_command_error =
        ObjectMutationMetadataNodeClient::build_stream_part_commit_command(
            &client,
            BuildStreamPartCommitCommandReq {
                pg_id: wrong_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                upload_id: &upload_id,
                session_id: &part_session_id,
                part_number: 1,
                expected_snapshot: &part_snapshot,
                part: &part,
                segments: &[],
                bucket_write_reservation: &part_finalize_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        wrong_part_command_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    let crossed_part_proof_error =
        ObjectMutationMetadataNodeClient::build_stream_part_commit_command(
            &client,
            BuildStreamPartCommitCommandReq {
                pg_id: correct_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                upload_id: &upload_id,
                session_id: &part_session_id,
                part_number: 1,
                expected_snapshot: &part_snapshot,
                part: &part,
                segments: &[],
                bucket_write_reservation: &current_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        crossed_part_proof_error,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_multipart_metadata_rejects_wrong_object_pg_before_node_access() {
    macro_rules! assert_object_payload_decode {
        ($result:expr) => {
            assert!(matches!(
                $result.unwrap_err(),
                ObjectPgActionError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                })
            ));
        };
    }
    macro_rules! assert_bucket_payload_decode {
        ($result:expr) => {
            assert!(matches!(
                $result.unwrap_err(),
                BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                })
            ));
        };
    }

    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    config.pg_routes = vec![
        StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: crate::types::PgState::Active,
            primary_node_id: NodeId::new(7),
            acting_set: vec![NodeId::new(7)],
        },
        StorageNodePgRoute {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: crate::types::PgState::Active,
            primary_node_id: NodeId::new(7),
            acting_set: vec![NodeId::new(7)],
        },
    ];
    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let (bucket, key, correct_pg_id) = (0..100)
        .map(|index| {
            let bucket =
                crate::tests::bucket_name(format!("multipart-rpc-wrong-pg-bucket-{index}"));
            let key = crate::tests::object_key(format!("multipart-rpc-wrong-pg-key-{index}"));
            let pg_id = node.pg_topology().object_pg_for(&bucket, &key);
            (bucket, key, pg_id)
        })
        .find(|(_, _, pg_id)| *pg_id < 2)
        .expect("two-PG topology must place a test object");
    let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
    let upload_id = crate::tests::multipart_upload_id("wrong-pg-multipart");
    let owner = OwnerIdentity::from_principal("owner");
    let create = CreateMultipartUploadReq {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: SerializedMetadataBlob::default(),
        system_metadata_blob: SerializedSystemMetadataBlob::default(),
        initiator: owner.clone(),
        owner: owner.clone(),
        acl_grants: AclGrants::default(),
        public_read: false,
        object_lock: ObjectLockState::default(),
        checksum: None,
        encryption: ObjectEncryption::None,
    };
    let part = test_multipart_part_record(upload_id.clone(), 1);
    let _time = crate::clock::test_time_override_guard(1_000);
    let mut expected_upload = None;
    for pg_id in [correct_pg_id, wrong_pg_id] {
        let pg = node.get_pg(pg_id).unwrap();
        PgMetadataStore::create_multipart_upload(&*pg, &create).unwrap();
        PgMetadataStore::upsert_multipart_part(&*pg, &part).unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        let upload = PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap();
        if let Some(expected) = &expected_upload {
            assert_eq!(
                &upload, expected,
                "equivalent wrong-PG state must make unguarded multipart operations succeed"
            );
        } else {
            expected_upload = Some(upload);
        }
    }
    drop(_time);
    let topology = node.pg_topology().clone();
    drop(node);

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..27)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );
    let correct_pg = ObjectMetadataPgId::new_for_test(PgId::new(correct_pg_id));
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(wrong_pg_id));

    let upload = ObjectMutationMetadataNodeClient::load_multipart_upload(
        &client, correct_pg, &bucket, &key, &upload_id,
    )
    .unwrap();
    assert_eq!(upload, expected_upload.unwrap());
    assert_bucket_payload_decode!(ObjectMutationMetadataNodeClient::load_multipart_upload(
        &client, wrong_pg, &bucket, &key, &upload_id,
    ));

    assert_eq!(
        ObjectMutationMetadataNodeClient::load_in_progress_multipart_upload(
            &client, correct_pg, &bucket, &key, &upload_id,
        )
        .unwrap(),
        upload
    );
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::load_in_progress_multipart_upload(
            &client, wrong_pg, &bucket, &key, &upload_id,
        )
    );

    assert_eq!(
        ObjectMutationMetadataNodeClient::load_in_progress_multipart_upload_for_listing(
            &client, correct_pg, &bucket, &key, &upload_id,
        )
        .unwrap(),
        upload
    );
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::load_in_progress_multipart_upload_for_listing(
            &client, wrong_pg, &bucket, &key, &upload_id,
        )
    );

    let authorized_upload = AuthorizedMultipartUploadRecord::assume_authorized(upload.clone());
    let snapshot = ObjectMutationMetadataNodeClient::load_multipart_completion_snapshot(
        &client,
        correct_pg,
        &authorized_upload,
        &[1],
    )
    .unwrap();
    assert_eq!(snapshot.part_records, vec![part.clone()]);
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::load_multipart_completion_snapshot(
            &client,
            wrong_pg,
            &authorized_upload,
            &[1],
        )
    );

    ObjectMutationMetadataNodeClient::load_multipart_completion_preflight(
        &client,
        correct_pg,
        &authorized_upload,
    )
    .unwrap();
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::load_multipart_completion_preflight(
            &client,
            wrong_pg,
            &authorized_upload,
        )
    );

    let listed = ObjectMutationMetadataNodeClient::list_multipart_parts_for_authorized_upload(
        &client,
        correct_pg,
        &authorized_upload,
        None,
        10,
    )
    .unwrap();
    assert_eq!(listed.response.parts, vec![part.clone()]);
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::list_multipart_parts_for_authorized_upload(
            &client,
            wrong_pg,
            &authorized_upload,
            None,
            10,
        )
    );

    assert!(matches!(
        ObjectMutationMetadataNodeClient::lookup_multipart_upload_management(
            &client,
            correct_pg,
            &bucket,
            &key,
            &upload_id,
        )
        .unwrap(),
        MultipartUploadManagementLookup::InProgress(current) if *current == upload
    ));
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::lookup_multipart_upload_management(
            &client, wrong_pg, &bucket, &key, &upload_id,
        )
    );

    assert!(
        ObjectMutationMetadataNodeClient::load_multipart_completion_stale_payload_source(
            &client, correct_pg, &bucket, &key,
        )
        .unwrap()
        .is_none()
    );
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::load_multipart_completion_stale_payload_source(
            &client, wrong_pg, &bucket, &key,
        )
    );

    let cleanup = ObjectMutationMetadataNodeClient::load_abort_multipart_upload_cleanup(
        &client, correct_pg, &bucket, &key, &upload_id,
    )
    .unwrap()
    .expect("in-progress upload has abort cleanup");
    assert_eq!(cleanup.upload, upload);
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::load_abort_multipart_upload_cleanup(
            &client, wrong_pg, &bucket, &key, &upload_id,
        )
    );

    let complete_request = CompleteMultipartCommitRequest {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        completion_fingerprint: crate::MultipartCompletionFingerprint::from_bytes([0x61; 32]),
        versioning: BucketVersioningState::Disabled,
        owner,
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: upload.object_generation_id,
        size: part.size,
        etag_crc64: [9; 8],
        tags: None,
        metadata_blob: Some(SerializedMetadataBlob::default()),
        system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
        expected_stale_payload_source: snapshot.stale_payload_source.clone(),
        expected_current_object_identity: snapshot.current_object_identity,
        conditional_completion: false,
        part_records: snapshot.part_records,
        selected_streaming_segments: snapshot.selected_streaming_segments,
        expected_cleanup: snapshot.cleanup,
    };
    let expected_object_parts = crate::node_client::complete_multipart_expected_object_parts(
        &complete_request,
        VersionId::Null,
        &topology,
    );
    let mut complete_proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    complete_proof.operation_kind =
        COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND.to_string();
    let mut abort_proof = complete_proof.clone();
    abort_proof.operation_kind = ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND.to_string();
    let complete_command =
        ObjectMutationMetadataNodeClient::build_complete_multipart_object_command(
            &client,
            BuildCompleteMultipartObjectCommandReq {
                pg_id: correct_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &complete_request,
                version_id: VersionId::Null,
                expected_object_parts: &expected_object_parts,
                bucket_write_reservation: &complete_proof,
            },
        )
        .unwrap();
    assert!(matches!(
        complete_command.payload(),
        MetadataCommandPayload::CommitMultipartObject(commit)
            if commit.upload_id == upload_id
    ));
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::build_complete_multipart_object_command(
            &client,
            BuildCompleteMultipartObjectCommandReq {
                pg_id: wrong_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &complete_request,
                version_id: VersionId::Null,
                expected_object_parts: &expected_object_parts,
                bucket_write_reservation: &complete_proof,
            },
        )
    );
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::build_complete_multipart_object_command(
            &client,
            BuildCompleteMultipartObjectCommandReq {
                pg_id: correct_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &complete_request,
                version_id: VersionId::Null,
                expected_object_parts: &expected_object_parts,
                bucket_write_reservation: &abort_proof,
            },
        )
    );

    let abort_command = ObjectMutationMetadataNodeClient::build_abort_multipart_upload_command(
        &client,
        BuildAbortMultipartUploadCommandReq {
            pg_id: correct_pg,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket: &bucket,
            key: &key,
            upload_id: &upload_id,
            expected_cleanup: Some(&cleanup),
            bucket_write_reservation: abort_proof.clone(),
        },
    )
    .unwrap()
    .expect("in-progress upload produces abort command");
    assert!(matches!(
        abort_command.payload(),
        MetadataCommandPayload::AbortMultipartUpload(abort)
            if abort.upload_id == upload_id
    ));
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::build_abort_multipart_upload_command(
            &client,
            BuildAbortMultipartUploadCommandReq {
                pg_id: wrong_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                upload_id: &upload_id,
                expected_cleanup: Some(&cleanup),
                bucket_write_reservation: abort_proof.clone(),
            },
        )
    );
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::build_abort_multipart_upload_command(
            &client,
            BuildAbortMultipartUploadCommandReq {
                pg_id: correct_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                upload_id: &upload_id,
                expected_cleanup: Some(&cleanup),
                bucket_write_reservation: complete_proof.clone(),
            },
        )
    );

    ObjectMutationMetadataNodeClient::build_authorized_abort_multipart_upload_command(
        &client,
        BuildAuthorizedAbortMultipartUploadCommandReq {
            pg_id: correct_pg,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            authorized_upload: &authorized_upload,
            expected_cleanup: Some(&cleanup),
            bucket_write_reservation: abort_proof.clone(),
        },
    )
    .unwrap()
    .expect("authorized in-progress upload produces abort command");
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::build_authorized_abort_multipart_upload_command(
            &client,
            BuildAuthorizedAbortMultipartUploadCommandReq {
                pg_id: wrong_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                authorized_upload: &authorized_upload,
                expected_cleanup: Some(&cleanup),
                bucket_write_reservation: abort_proof,
            },
        )
    );
    assert_object_payload_decode!(
        ObjectMutationMetadataNodeClient::build_authorized_abort_multipart_upload_command(
            &client,
            BuildAuthorizedAbortMultipartUploadCommandReq {
                pg_id: correct_pg,
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                authorized_upload: &authorized_upload,
                expected_cleanup: Some(&cleanup),
                bucket_write_reservation: complete_proof,
            },
        )
    );

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_object_read_metadata_client_loads_subject_and_snapshot() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("object-read-rpc-bucket");
    let key = crate::tests::object_key("object-read-rpc-key");
    let generation_id = GenerationId::new(9).unwrap();
    let segment_vid = GenerationId::new(10).unwrap();
    let segment = ObjectSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: VersionId::Null,
        segment_index: 0,
        size: 12,
        segment_crc64: 99,
        segment_okh: [3; 16],
        segment_vid,
        data_pg_id: 0,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
    };
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &crate::CanonicalUserId::from_principal("owner"),
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::put_object_with_segments(
            &*pg,
            &PutLiveObjectReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                owner: OwnerIdentity::from_principal("owner"),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id,
                size: 12,
                etag: ObjectEtag::single_part(99),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: Some(SerializedTagSet::new(
                    "<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>"
                        .to_string(),
                )),
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            std::slice::from_ref(&segment),
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..3)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let subject = ObjectReadMetadataNodeClient::load_object_read_auth_subject(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &bucket,
        &key,
        None,
    )
    .unwrap();
    assert_eq!(subject.stored.bucket(), &bucket);
    assert_eq!(subject.stored.key(), &key);

    let snapshot = ObjectReadMetadataNodeClient::load_object_read_snapshot_for_subject(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &bucket,
        &key,
        None,
        &subject.identity,
        ObjectReadSnapshotMode::StandardSegments,
    )
    .unwrap();
    assert_eq!(snapshot.stored, subject.stored);
    assert_eq!(snapshot.object_segments, vec![segment]);
    assert!(snapshot.multipart_parts.is_empty());
    assert!(snapshot.multipart_part_segments.is_empty());

    let tags = ObjectReadMetadataNodeClient::get_object_tags_for_subject(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &bucket,
        &key,
        None,
        &subject.identity,
        VersionId::Null,
    )
    .unwrap();
    let expected_tags = crate::tests::object_tags(
        "<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>",
    );
    assert_eq!(
        tags.as_ref().map(crate::SerializedTagSet::tag_set),
        Some(expected_tags.tag_set())
    );

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_object_mutation_metadata_client_loads_snapshots_and_builds_commands() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("object-mutation-rpc-bucket");
    let key = crate::tests::object_key("object-mutation-rpc-key");
    let listed_stream_request = CreateStreamUploadReq {
        session_id: crate::tests::stream_session_id("mut-rpc-listed"),
        bucket: bucket.clone(),
        key: crate::tests::object_key("object-mutation-listed-stream-key"),
        target: StreamUploadTarget::PutObject,
        encryption: ObjectEncryption::None,
    };
    let generation_id = GenerationId::new(19).unwrap();
    let reclaim_generation_id = GenerationId::new(21).unwrap();
    let segment = ObjectSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: VersionId::Null,
        segment_index: 0,
        size: 12,
        segment_crc64: 100,
        segment_okh: [4; 16],
        segment_vid: GenerationId::new(20).unwrap(),
        data_pg_id: 0,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
    };
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &crate::CanonicalUserId::from_principal("owner"),
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::put_object_with_segments(
            &*pg,
            &PutLiveObjectReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                owner: OwnerIdentity::from_principal("owner"),
                acl_grants: AclGrants::default(),
                public_read: false,
                generation_id,
                size: 12,
                etag: ObjectEtag::single_part(100),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: None,
                metadata_blob: None,
                system_metadata_blob: None,
                object_lock: ObjectLockState::default(),
                encryption: ObjectEncryption::None,
            },
            std::slice::from_ref(&segment),
        )
        .unwrap();
        PgMetadataStore::create_stream_upload(&*pg, &listed_stream_request).unwrap();
        pg.put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id: reclaim_generation_id,
            created_at: 12,
            segments: vec![ObjectSegmentsReclaimSegmentRecord {
                segment_index: segment.segment_index,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                data_pg_id: segment.data_pg_id,
                ec: EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            }],
        })
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..18)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );
    let proof = BucketWriteReservationProof {
        bucket: bucket.clone(),
        reservation_id: "reservation-id".to_string(),
        owner_token: "owner-token".to_string(),
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        operation_kind: "object-mutation-test".to_string(),
        created_at: 1,
        lease_deadline: 20,
        target_context: Some(key.as_str().to_string()),
    };
    let mut metadata_proof = proof.clone();
    metadata_proof.operation_kind = "put-object-metadata".to_string();
    let mut delete_current_proof = proof.clone();
    delete_current_proof.operation_kind = "delete-current-object".to_string();
    let mut stream_proof = proof.clone();
    stream_proof.operation_kind =
        crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();
    let mut upload_part_stream_proof = proof.clone();
    upload_part_stream_proof.operation_kind =
        crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();

    let stored = ObjectMutationMetadataNodeClient::load_put_object_metadata_snapshot(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &bucket,
        &key,
        None,
    )
    .unwrap();
    assert_eq!(stored.bucket(), &bucket);
    assert_eq!(stored.key(), &key);

    let put_command = ObjectMutationMetadataNodeClient::build_put_object_metadata_command(
        &client,
        BuildPutObjectMetadataCommandReq {
            pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket: &bucket,
            key: &key,
            requested_version_id: None,
            expected_stored: &stored,
            version_id: VersionId::Null,
            mutation: PutObjectMetadataMutation::PutTags(SerializedTagSet::default()),
            bucket_write_reservation: &metadata_proof,
        },
    )
    .unwrap();
    assert!(matches!(
        put_command.payload(),
        MetadataCommandPayload::PutObjectMetadata(update)
            if update.object.bucket == bucket && update.object.key == key
    ));

    let current = ObjectMutationMetadataNodeClient::load_current_object_delete_snapshot(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &bucket,
        &key,
    )
    .unwrap();
    assert_eq!(current.stored.as_ref(), Some(&stored));
    let stream_uploads = ObjectMutationMetadataNodeClient::list_stream_uploads_for_bucket_page(
        &client,
        ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
        &bucket,
        None,
        10,
    )
    .unwrap();
    assert!(stream_uploads.uploads.iter().any(|upload| upload.session_id
        == listed_stream_request.session_id
        && upload.bucket == listed_stream_request.bucket
        && upload.key == listed_stream_request.key));
    let reclaim_root = ObjectMutationMetadataNodeClient::get_bucket_payload_reclaim_root(
        &client,
        ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
        &bucket,
    )
    .unwrap()
    .expect("seeded reclaim root should exist");
    assert_eq!(reclaim_root.bucket, bucket);
    assert_eq!(reclaim_root.key, key);
    assert_eq!(reclaim_root.generation_id, reclaim_generation_id);
    let pg_reclaim_root = ObjectMutationMetadataNodeClient::get_payload_reclaim_root(
        &client,
        ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
    )
    .unwrap()
    .expect("seeded PG reclaim root should exist");
    assert_eq!(pg_reclaim_root, reclaim_root);
    let object_reclaim = ObjectMutationMetadataNodeClient::get_object_payload_reclaim(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &bucket,
        &key,
        reclaim_generation_id,
    )
    .unwrap()
    .expect("seeded object reclaim should exist");
    assert!(matches!(
        &object_reclaim,
        ObjectPayloadReclaimCommand::Segments(reclaim)
            if reclaim.bucket == bucket
                && reclaim.key == key
                && reclaim.generation_id == reclaim_generation_id
    ));
    let claim = ObjectMutationMetadataNodeClient::acquire_object_payload_reclaim_claim(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &bucket,
        1,
        &key,
        reclaim_generation_id,
        object_reclaim.kind(),
        "object-reclaim-claim",
        "object-reclaim-owner",
        ClusterEpoch::new(1).unwrap(),
        100,
        Some(1_000),
        100,
    )
    .unwrap()
    .expect("seeded object reclaim claim should be acquired");
    assert_eq!(claim.bucket, bucket);
    assert_eq!(claim.key, key);
    assert_eq!(claim.generation_id, reclaim_generation_id);
    let loaded_claim = ObjectMutationMetadataNodeClient::object_payload_reclaim_claim(
        &client,
        ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
    )
    .unwrap()
    .expect("seeded object reclaim claim should load");
    assert_eq!(loaded_claim, claim);
    ObjectMutationMetadataNodeClient::release_object_payload_reclaim_claim(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &claim,
    )
    .unwrap();
    client
        .validate_bucket_payload_reclaim_root_response(
            &StorageRpcPayloadReclaimRootResponse {
                root: Some(PayloadReclaimRoot {
                    bucket: crate::tests::bucket_name("wrong-reclaim-root-bucket"),
                    key: key.clone(),
                    generation_id: reclaim_generation_id,
                }),
            },
            &bucket,
        )
        .unwrap_err();
    assert!(ObjectMutationMetadataNodeClient::payload_reclaim_exists(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &bucket,
        &key,
        reclaim_generation_id,
    )
    .unwrap());
    assert!(!ObjectMutationMetadataNodeClient::payload_reclaim_exists(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &bucket,
        &key,
        GenerationId::new(22).unwrap(),
    )
    .unwrap());
    let delete_command = ObjectMutationMetadataNodeClient::build_delete_current_object_command(
        &client,
        BuildDeleteCurrentObjectCommandReq {
            pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket: &bucket,
            key: &key,
            expected_current: current.stored.as_ref(),
            expected_target: current.target.as_ref(),
            bucket_write_reservation: &delete_current_proof,
        },
    )
    .unwrap()
    .expect("live current object should build delete command");
    assert!(matches!(
        delete_command.payload(),
        MetadataCommandPayload::DeleteObjectVersion(delete)
            if delete.bucket == bucket && delete.key == key
    ));

    let stream_request = CreateStreamUploadReq {
        session_id: crate::tests::stream_session_id("mut-stream-rpc"),
        bucket: bucket.clone(),
        key: key.clone(),
        target: StreamUploadTarget::PutObject,
        encryption: ObjectEncryption::None,
    };
    assert!(
        !ObjectMutationMetadataNodeClient::matching_stream_upload_exists(
            &client,
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &stream_request,
            None,
        )
        .unwrap()
    );
    let stream_command = ObjectMutationMetadataNodeClient::build_create_stream_upload_command(
        &client,
        BuildCreateStreamUploadCommandReq {
            pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            request: &stream_request,
            cleanup_after: Some(12_345),
            precondition: CreateStreamUploadPrecondition::PutObject {
                expected_current: Some(&stored),
                require_generation_reservation: false,
            },
            bucket_write_reservation: &stream_proof,
        },
    )
    .unwrap();
    let MetadataCommandPayload::CreateStreamUpload(stream_create) = stream_command.payload() else {
        panic!("expected create stream upload command");
    };
    assert_eq!(stream_create.session.bucket, bucket);
    assert_eq!(stream_create.session.key, key);
    assert_eq!(stream_create.cleanup_after, Some(12_345));
    assert_eq!(stream_create.bucket_write_reservation, stream_proof);
    client
        .validate_stream_upload_match_response(true, Some(stream_create.as_ref()))
        .unwrap();
    let err = client
        .validate_stream_upload_match_response(true, None)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate stream upload match response",
            ..
        })
    ));

    let mut bad_stream_payload = stream_command.payload().clone();
    let MetadataCommandPayload::CreateStreamUpload(bad_stream_create) = &mut bad_stream_payload
    else {
        panic!("expected create stream upload command");
    };
    bad_stream_create.session.key = crate::tests::object_key("wrong-stream-key");
    let bad_stream_command = MetadataCommandEnvelope::new(stream_command.id(), bad_stream_payload);
    let err = client
        .validate_create_stream_upload_command_response(
            &bad_stream_command,
            &BuildCreateStreamUploadCommandReq {
                pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &stream_request,
                cleanup_after: Some(12_345),
                precondition: CreateStreamUploadPrecondition::PutObject {
                    expected_current: Some(&stored),
                    require_generation_reservation: false,
                },
                bucket_write_reservation: &stream_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate stream upload command build response",
            ..
        })
    ));

    let mut bad_cleanup_payload = stream_command.payload().clone();
    let MetadataCommandPayload::CreateStreamUpload(bad_cleanup_create) = &mut bad_cleanup_payload
    else {
        panic!("expected create stream upload command");
    };
    bad_cleanup_create.cleanup_after = Some(12_346);
    let bad_cleanup_command =
        MetadataCommandEnvelope::new(stream_command.id(), bad_cleanup_payload);
    let err = client
        .validate_create_stream_upload_command_response(
            &bad_cleanup_command,
            &BuildCreateStreamUploadCommandReq {
                pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &stream_request,
                cleanup_after: Some(12_345),
                precondition: CreateStreamUploadPrecondition::PutObject {
                    expected_current: Some(&stored),
                    require_generation_reservation: false,
                },
                bucket_write_reservation: &stream_proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate stream upload command build response",
            ..
        })
    ));

    let missing_upload_id = crate::tests::multipart_upload_id("mut-stream-rpc-missing-upload");
    let missing_upload = test_multipart_upload_record(
        bucket.clone(),
        key.clone(),
        missing_upload_id.clone(),
        UploadState::InProgress,
    );
    let missing_upload_part_stream_request = CreateStreamUploadReq {
        session_id: crate::tests::stream_session_id("mut-rpc-miss"),
        bucket: bucket.clone(),
        key: key.clone(),
        target: StreamUploadTarget::UploadPart {
            upload_id: missing_upload_id.clone(),
            part_number: 1,
        },
        encryption: ObjectEncryption::None,
    };
    let err = ObjectMutationMetadataNodeClient::build_create_stream_upload_command(
        &client,
        BuildCreateStreamUploadCommandReq {
            pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            request: &missing_upload_part_stream_request,
            cleanup_after: None,
            precondition: CreateStreamUploadPrecondition::UploadPart {
                expected_upload: &missing_upload,
            },
            bucket_write_reservation: &upload_part_stream_proof,
        },
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { upload_id })
            if upload_id == missing_upload_id.as_str()
    ));

    let multipart_request = CreateMultipartUploadReq {
        upload_id: crate::tests::multipart_upload_id("object-mutation-mpu-rpc"),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: OwnerIdentity::from_principal("owner"),
        owner: OwnerIdentity::from_principal("owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        object_lock: ObjectLockState::default(),
        checksum: None,
        encryption: ObjectEncryption::None,
    };
    let mut multipart_proof = proof.clone();
    multipart_proof.operation_kind =
        crate::metadata_command::CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND.to_string();
    assert_eq!(
        ObjectMutationMetadataNodeClient::matching_multipart_upload_initiated_at(
            &client,
            ObjectMetadataPgId::new_for_test(PgId::new(0)),
            &multipart_request,
            None,
        )
        .unwrap(),
        None
    );
    let multipart_command =
        ObjectMutationMetadataNodeClient::build_create_multipart_upload_command(
            &client,
            BuildCreateMultipartUploadCommandReq {
                pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &multipart_request,
                expected_current: Some(&stored),
                bucket_write_reservation: &multipart_proof,
            },
        )
        .unwrap();
    let MetadataCommandPayload::CreateMultipartUpload(multipart_create) =
        multipart_command.payload()
    else {
        panic!("expected create multipart upload command");
    };
    assert_eq!(multipart_create.upload.bucket, bucket);
    assert_eq!(multipart_create.upload.key, key);
    assert_eq!(multipart_create.bucket_write_reservation, multipart_proof);
    client
        .validate_multipart_upload_match_response(
            Some(multipart_create.upload.initiated_at),
            Some(multipart_create.as_ref()),
        )
        .unwrap();
    let err = client
        .validate_multipart_upload_match_response(Some(multipart_create.upload.initiated_at), None)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate multipart upload match response",
            ..
        })
    ));
    let err = client
        .validate_multipart_upload_match_response(
            Some(multipart_create.upload.initiated_at + 1),
            Some(multipart_create.as_ref()),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate multipart upload match response",
            ..
        })
    ));

    let mut bad_multipart_payload = multipart_command.payload().clone();
    let MetadataCommandPayload::CreateMultipartUpload(bad_multipart_create) =
        &mut bad_multipart_payload
    else {
        panic!("expected create multipart upload command");
    };
    bad_multipart_create.upload.owner = OwnerIdentity::from_principal("other-owner");
    let bad_multipart_command =
        MetadataCommandEnvelope::new(multipart_command.id(), bad_multipart_payload);
    let err = client
        .validate_create_multipart_upload_command_response(
            &bad_multipart_command,
            &BuildCreateMultipartUploadCommandReq {
                pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &multipart_request,
                expected_current: Some(&stored),
                bucket_write_reservation: &proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate multipart upload command build response",
            ..
        })
    ));

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_object_mutation_client_rejects_stale_upload_part_stream_command_epoch() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let stale_epoch = ClusterEpoch::new(config.cluster_epoch.get() + 1).unwrap();
    let client = UnixStorageNodeClient::new(NodeId::new(7), stale_epoch, config.socket_path);
    let bucket = crate::tests::bucket_name("stale-upload-part-rpc-bucket");
    let key = crate::tests::object_key("stale-upload-part-rpc-key");
    let upload_id = crate::tests::multipart_upload_id("stale-upload-part-rpc-upload");
    let upload = test_multipart_upload_record(
        bucket.clone(),
        key.clone(),
        upload_id.clone(),
        UploadState::InProgress,
    );
    let request = CreateStreamUploadReq {
        session_id: crate::tests::stream_session_id("stale-part-rpc"),
        bucket: bucket.clone(),
        key: key.clone(),
        target: StreamUploadTarget::UploadPart {
            upload_id,
            part_number: 1,
        },
        encryption: ObjectEncryption::None,
    };
    let mut proof = test_bucket_write_reservation_proof(bucket, &key);
    proof.operation_kind =
        crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND.to_string();

    let err = ObjectMutationMetadataNodeClient::build_create_stream_upload_command(
        &client,
        BuildCreateStreamUploadCommandReq {
            pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
            cluster_epoch: stale_epoch,
            request: &request,
            cleanup_after: None,
            precondition: CreateStreamUploadPrecondition::UploadPart {
                expected_upload: &upload,
            },
            bucket_write_reservation: &proof,
        },
    )
    .unwrap_err();

    assert!(
        matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "object stream upload command build",
                failure: StorageRpcErrorCode::StaleShardLocation,
                ref detail,
                ..
            }) if detail.as_str().contains(&format!("request route epoch {stale_epoch}"))
                && detail.as_str().contains(&format!("storage-node epoch {}", config.cluster_epoch))
        ),
        "stale UploadPart stream command-build RPC should fail route validation, got {err:?}"
    );
    server_thread.join().unwrap();
}

#[test]
fn unix_stream_uploads_list_requires_pg_primary() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_routes[0].primary_node_id = NodeId::new(8);
    config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..2)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let bucket = crate::tests::bucket_name("stream-upload-list-primary");
    let err = ObjectMutationMetadataNodeClient::list_stream_uploads_for_bucket_page(
        &client,
        ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
        &bucket,
        None,
        1,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "object stream uploads list",
            ..
        })
    ));
    let err = ObjectMutationMetadataNodeClient::list_all_stream_uploads_page(
        &client,
        ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
        None,
        1,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "object stream uploads PG list",
            ..
        })
    ));
    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_stream_uploads_list_rejects_wrong_pg_rows() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    config.pg_routes = vec![
        StorageNodePgRoute {
            pg_id: 0,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: crate::types::PgState::Active,
            primary_node_id: NodeId::new(7),
            acting_set: vec![NodeId::new(7)],
        },
        StorageNodePgRoute {
            pg_id: 1,
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            state: crate::types::PgState::Active,
            primary_node_id: NodeId::new(7),
            acting_set: vec![NodeId::new(7)],
        },
    ];
    let (bucket, wrong_pg_id) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let topology = node.pg_topology();
        let bucket = crate::tests::bucket_name("stream-upload-list-wrong-pg");
        let key = (0..100)
            .map(|index| crate::tests::object_key(format!("key-{index}")))
            .find(|key| topology.object_pg_for(&bucket, key) == 1)
            .expect("two-PG topology must place a test object on PG 1");
        let wrong_pg_id = 0;
        let session_id = crate::SessionId::try_from("ef".repeat(16)).unwrap();
        let wrong_pg = node.get_pg(wrong_pg_id).unwrap();
        crate::PgMetadataStore::create_stream_upload(
            &*wrong_pg,
            &crate::CreateStreamUploadReq {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: crate::StreamUploadTarget::PutObject,
                encryption: crate::ObjectEncryption::None,
            },
        )
        .unwrap();
        wrong_pg.refresh_metadata_command_state_digest().unwrap();
        (bucket, wrong_pg_id)
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..2)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let err = ObjectMutationMetadataNodeClient::list_stream_uploads_for_bucket_page(
        &client,
        ObjectMetadataScanPgId::new_for_test(PgId::new(wrong_pg_id)),
        &bucket,
        None,
        10,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "object stream uploads list",
            ..
        })
    ));
    let err = ObjectMutationMetadataNodeClient::list_all_stream_uploads_page(
        &client,
        ObjectMetadataScanPgId::new_for_test(PgId::new(wrong_pg_id)),
        None,
        10,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "object stream uploads PG list",
            ..
        })
    ));
    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_payload_reclaim_exists_requires_pg_primary() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_routes[0].primary_node_id = NodeId::new(8);
    config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );
    let bucket = crate::tests::bucket_name("reclaim-primary-rpc-bucket");
    let key = crate::tests::object_key("reclaim-primary-rpc-key");

    let err = ObjectMutationMetadataNodeClient::payload_reclaim_exists(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &bucket,
        &key,
        GenerationId::new(1).unwrap(),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "object payload reclaim exists",
            ..
        })
    ));
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_payload_reclaim_root_requires_pg_primary() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_routes[0].primary_node_id = NodeId::new(8);
    config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );
    let bucket = crate::tests::bucket_name("bucket-reclaim-primary-rpc-bucket");

    let err = ObjectMutationMetadataNodeClient::get_bucket_payload_reclaim_root(
        &client,
        ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
        &bucket,
    )
    .unwrap_err();

    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "object bucket payload reclaim root",
            ..
        })
    ));
    server_thread.join().unwrap();
}

#[test]
fn unix_object_payload_reclaim_root_requires_pg_primary() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_routes[0].primary_node_id = NodeId::new(8);
    config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let err = ObjectMutationMetadataNodeClient::get_payload_reclaim_root(
        &client,
        ObjectMetadataScanPgId::new_for_test(PgId::new(0)),
    )
    .unwrap_err();

    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "object payload reclaim root",
            ..
        })
    ));
    server_thread.join().unwrap();
}

#[test]
fn unix_object_payload_reclaim_roles_reject_equivalent_wrong_pg_state() {
    macro_rules! assert_object_payload_decode {
        ($result:expr) => {
            assert!(matches!(
                $result.unwrap_err(),
                ObjectPgActionError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                })
            ));
        };
    }
    macro_rules! assert_bucket_payload_decode {
        ($result:expr) => {
            assert!(matches!(
                $result.unwrap_err(),
                BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                })
            ));
        };
    }

    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.pg_ids = vec![0, 1];
    config.pg_routes.push(StorageNodePgRoute {
        pg_id: 1,
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        state: crate::types::PgState::Active,
        primary_node_id: NodeId::new(7),
        acting_set: vec![NodeId::new(7)],
    });
    let bucket = crate::tests::bucket_name("object-reclaim-role-rpc-bucket");
    let generation_id = GenerationId::new(91).unwrap();
    let (key, correct_raw_pg, wrong_raw_pg, correct_claim, wrong_claim) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let key = (0..100)
            .map(|index| crate::tests::object_key(format!("reclaim-key-{index}")))
            .find(|key| node.pg_topology().object_pg_for(&bucket, key) == 1)
            .expect("two-PG topology must place a test object on PG 1");
        let correct_raw_pg = 1;
        let wrong_raw_pg = 0;
        let reclaim = ObjectSegmentsReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
            created_at: 12,
            segments: Vec::new(),
        };
        let mut claims = Vec::new();
        for raw_pg_id in [correct_raw_pg, wrong_raw_pg] {
            let pg = node.get_pg(raw_pg_id).unwrap();
            PgMetadataStore::put_object_segments_reclaim(&*pg, &reclaim).unwrap();
            claims.push(
                PgMetadataStore::acquire_object_payload_reclaim_claim(
                    &*pg,
                    &bucket,
                    1,
                    &key,
                    generation_id,
                    ObjectPayloadReclaimKind::ObjectSegments,
                    "object-reclaim-role-claim",
                    "object-reclaim-role-owner",
                    ClusterEpoch::new(1).unwrap(),
                    100,
                    Some(1_000),
                    100,
                )
                .unwrap()
                .expect("seeded reclaim must be claimable on either PG"),
            );
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        (
            key,
            correct_raw_pg,
            wrong_raw_pg,
            claims.remove(0),
            claims.remove(0),
        )
    };
    let correct_pg = ObjectMetadataPgId::new_for_test(PgId::new(correct_raw_pg));
    let wrong_pg = ObjectMetadataPgId::new_for_test(PgId::new(wrong_raw_pg));
    let correct_scan_pg = ObjectMetadataScanPgId::new_for_test(PgId::new(correct_raw_pg));
    let wrong_scan_pg = ObjectMetadataScanPgId::new_for_test(PgId::new(wrong_raw_pg));

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..16)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    assert!(ObjectMutationMetadataNodeClient::payload_reclaim_exists(
        &client,
        correct_pg,
        &bucket,
        &key,
        generation_id,
    )
    .unwrap());
    assert_object_payload_decode!(ObjectMutationMetadataNodeClient::payload_reclaim_exists(
        &client,
        wrong_pg,
        &bucket,
        &key,
        generation_id,
    ));

    let reclaim = ObjectMutationMetadataNodeClient::get_object_payload_reclaim(
        &client,
        correct_pg,
        &bucket,
        &key,
        generation_id,
    )
    .unwrap()
    .expect("correctly routed reclaim must load");
    assert_eq!(reclaim.kind(), ObjectPayloadReclaimKind::ObjectSegments);
    assert_bucket_payload_decode!(
        ObjectMutationMetadataNodeClient::get_object_payload_reclaim(
            &client,
            wrong_pg,
            &bucket,
            &key,
            generation_id,
        )
    );

    let acquired = ObjectMutationMetadataNodeClient::acquire_object_payload_reclaim_claim(
        &client,
        correct_pg,
        &bucket,
        1,
        &key,
        generation_id,
        ObjectPayloadReclaimKind::ObjectSegments,
        "object-reclaim-role-claim",
        "object-reclaim-role-owner",
        ClusterEpoch::new(1).unwrap(),
        100,
        Some(1_000),
        100,
    )
    .unwrap()
    .expect("correctly routed reclaim claim must load idempotently");
    assert_eq!(acquired, correct_claim);
    assert_bucket_payload_decode!(
        ObjectMutationMetadataNodeClient::acquire_object_payload_reclaim_claim(
            &client,
            wrong_pg,
            &bucket,
            1,
            &key,
            generation_id,
            ObjectPayloadReclaimKind::ObjectSegments,
            "object-reclaim-role-claim",
            "object-reclaim-role-owner",
            ClusterEpoch::new(1).unwrap(),
            100,
            Some(1_000),
            100,
        )
    );

    let bucket_root = ObjectMutationMetadataNodeClient::get_bucket_payload_reclaim_root(
        &client,
        correct_scan_pg,
        &bucket,
    )
    .unwrap()
    .expect("correct scan PG must return its reclaim root");
    assert_eq!(bucket_root.key, key);
    assert_bucket_payload_decode!(
        ObjectMutationMetadataNodeClient::get_bucket_payload_reclaim_root(
            &client,
            wrong_scan_pg,
            &bucket,
        )
    );

    let pg_root =
        ObjectMutationMetadataNodeClient::get_payload_reclaim_root(&client, correct_scan_pg)
            .unwrap()
            .expect("correct scan PG must return its reclaim root");
    assert_eq!(pg_root.key, key);
    assert_bucket_payload_decode!(ObjectMutationMetadataNodeClient::get_payload_reclaim_root(
        &client,
        wrong_scan_pg,
    ));

    let loaded_claim =
        ObjectMutationMetadataNodeClient::object_payload_reclaim_claim(&client, correct_scan_pg)
            .unwrap()
            .expect("correct scan PG must return its reclaim claim");
    assert_eq!(loaded_claim, correct_claim);
    assert_bucket_payload_decode!(
        ObjectMutationMetadataNodeClient::object_payload_reclaim_claim(&client, wrong_scan_pg)
    );

    assert_bucket_payload_decode!(
        ObjectMutationMetadataNodeClient::release_object_payload_reclaim_claim(
            &client,
            wrong_pg,
            &wrong_claim,
        )
    );
    assert_bucket_payload_decode!(
        ObjectMutationMetadataNodeClient::object_payload_reclaim_claim(&client, wrong_scan_pg)
    );

    ObjectMutationMetadataNodeClient::release_object_payload_reclaim_claim(
        &client,
        correct_pg,
        &correct_claim,
    )
    .unwrap();
    assert!(
        ObjectMutationMetadataNodeClient::object_payload_reclaim_claim(&client, correct_scan_pg)
            .unwrap()
            .is_none()
    );

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_object_mutation_client_rejects_malformed_multipart_read_responses() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("multipart-read-rpc-bucket");
    let key = crate::tests::object_key("multipart-read-rpc-key");
    let upload_id = crate::tests::multipart_upload_id("multipart-read-rpc-upload");
    let upload = test_multipart_upload_record(
        bucket.clone(),
        key.clone(),
        upload_id.clone(),
        UploadState::InProgress,
    );
    let authorized_upload = AuthorizedMultipartUploadRecord::assume_authorized(upload.clone());
    let part = test_multipart_part_record(upload_id.clone(), 1);

    client
        .validate_multipart_upload_response(
            &upload,
            &bucket,
            &key,
            &upload_id,
            Some(UploadState::InProgress),
            "validate in-progress multipart upload load response",
        )
        .unwrap();
    let mut wrong_upload = upload.clone();
    wrong_upload.key = crate::tests::object_key("wrong-multipart-read-key");
    let err = client
        .validate_multipart_upload_response(
            &wrong_upload,
            &bucket,
            &key,
            &upload_id,
            Some(UploadState::InProgress),
            "validate in-progress multipart upload load response",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate in-progress multipart upload load response",
            ..
        })
    ));

    let snapshot = MultipartCompletionSnapshot {
        existing_etag: None,
        current_object_identity: Some(crate::MultipartObjectIdentity::DeleteMarker {
            version_id: VersionId::from_u64(3),
            write_sequence: 11,
        }),
        stale_payload_source: None,
        part_records: vec![part.clone()],
        selected_streaming_segments: Vec::new(),
        cleanup: CompleteMultipartCommitCleanup::default(),
    };
    client
        .validate_multipart_completion_snapshot_response(&snapshot, &authorized_upload, &[1])
        .unwrap();
    let mut bad_snapshot = snapshot.clone();
    bad_snapshot.cleanup.omitted_parts.push(part.clone());
    let err = client
        .validate_multipart_completion_snapshot_response(&bad_snapshot, &authorized_upload, &[1])
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate multipart completion snapshot response",
            ..
        })
    ));

    let listed = ListedMultipartParts {
        upload: upload.clone(),
        response: crate::types::ListPartsResp {
            parts: vec![part.clone()],
            is_truncated: false,
            next_part_number_marker: Some(1),
        },
    };
    client
        .validate_listed_multipart_parts_response(&listed, &authorized_upload, None, 1)
        .unwrap();
    let mut bad_listed = listed.clone();
    bad_listed.response.parts[0].upload_id =
        crate::tests::multipart_upload_id("wrong-listed-upload");
    let err = client
        .validate_listed_multipart_parts_response(&bad_listed, &authorized_upload, None, 1)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate multipart parts list response",
            ..
        })
    ));
    let mut bad_listed = listed.clone();
    bad_listed.response.parts = vec![
        test_multipart_part_record(upload_id.clone(), 2),
        test_multipart_part_record(upload_id.clone(), 1),
    ];
    let err = client
        .validate_listed_multipart_parts_response(&bad_listed, &authorized_upload, None, 2)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate multipart parts list response",
            ..
        })
    ));
    let err = client
        .validate_listed_multipart_parts_response(&listed, &authorized_upload, Some(1), 1)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate multipart parts list response",
            ..
        })
    ));
    let mut bad_listed = listed.clone();
    bad_listed.response.next_part_number_marker = Some(2);
    let err = client
        .validate_listed_multipart_parts_response(&bad_listed, &authorized_upload, None, 1)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate multipart parts list response",
            ..
        })
    ));
    let mut bad_listed = listed.clone();
    bad_listed.response.is_truncated = true;
    bad_listed.response.next_part_number_marker = Some(2);
    let err = client
        .validate_listed_multipart_parts_response(&bad_listed, &authorized_upload, None, 1)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate multipart parts list response",
            ..
        })
    ));
    let mut truncated_listed = listed.clone();
    truncated_listed.response.is_truncated = true;
    client
        .validate_listed_multipart_parts_response(&truncated_listed, &authorized_upload, None, 1)
        .unwrap();
    let mut missing_marker_listed = listed.clone();
    missing_marker_listed.response.next_part_number_marker = None;
    let err = client
        .validate_listed_multipart_parts_response(
            &missing_marker_listed,
            &authorized_upload,
            None,
            1,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate multipart parts list response",
            ..
        })
    ));
    let zero_page_listed = ListedMultipartParts {
        upload: upload.clone(),
        response: crate::types::ListPartsResp {
            parts: Vec::new(),
            is_truncated: false,
            next_part_number_marker: Some(0),
        },
    };
    client
        .validate_listed_multipart_parts_response(&zero_page_listed, &authorized_upload, None, 0)
        .unwrap();
    client
        .validate_listed_multipart_parts_response(&zero_page_listed, &authorized_upload, Some(7), 1)
        .unwrap();
    client
        .validate_listed_multipart_parts_response(&zero_page_listed, &authorized_upload, Some(7), 0)
        .unwrap();
    let mut bad_zero_page_listed = zero_page_listed.clone();
    bad_zero_page_listed.response.is_truncated = true;
    let err = client
        .validate_listed_multipart_parts_response(
            &bad_zero_page_listed,
            &authorized_upload,
            None,
            0,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate multipart parts list response",
            ..
        })
    ));
    let mut bad_zero_page_listed = zero_page_listed.clone();
    bad_zero_page_listed.response.next_part_number_marker = Some(1);
    let err = client
        .validate_listed_multipart_parts_response(
            &bad_zero_page_listed,
            &authorized_upload,
            None,
            0,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate multipart parts list response",
            ..
        })
    ));

    client
        .validate_multipart_management_lookup_response(
            &MultipartUploadManagementLookup::InProgress(Box::new(upload.clone())),
            &bucket,
            &key,
            &upload_id,
        )
        .unwrap();
    let err = client
        .validate_multipart_management_lookup_response(
            &MultipartUploadManagementLookup::Replay(Box::new(crate::MultipartCompletionReplay {
                upload_id,
                bucket,
                key: crate::tests::object_key("wrong-completed-key"),
                fingerprint: crate::MultipartCompletionFingerprint::from_bytes([0x66; 32]),
                version_id: VersionId::Null,
                etag: ObjectEtag::MultipartComposite {
                    crc64: [0; 8],
                    parts: std::num::NonZeroU32::new(1).unwrap(),
                },
                size: 1,
                last_modified: 2,
                tags: None,
                system_metadata_blob: None,
                encryption: ObjectEncryption::None,
            })),
            &upload.bucket,
            &upload.key,
            &upload.upload_id,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate multipart management lookup response",
            ..
        })
    ));
}

#[test]
fn unix_object_mutation_client_loads_multipart_upload_over_rpc() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("multipart-upload-load-rpc-bucket");
    let key = crate::tests::object_key("multipart-upload-load-rpc-key");
    let upload_id = crate::tests::multipart_upload_id("multipart-upload-load-rpc-upload");
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &crate::CanonicalUserId::from_principal("owner"),
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        let create = CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            tags: None,
            metadata_blob: SerializedMetadataBlob::default(),
            system_metadata_blob: SerializedSystemMetadataBlob::default(),
            initiator: OwnerIdentity::from_principal("owner"),
            owner: OwnerIdentity::from_principal("owner"),
            acl_grants: AclGrants::default(),
            public_read: false,
            object_lock: ObjectLockState::default(),
            checksum: None,
            encryption: ObjectEncryption::None,
        };
        let proof = BucketWriteReservationProof {
            bucket: bucket.clone(),
            reservation_id: "reservation-1".to_string(),
            owner_token: "owner-token".to_string(),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            bucket_execution_generation: 1,
            bucket_incarnation_generation: 1,
            operation_kind: "create-multipart-upload".to_string(),
            created_at: 123,
            lease_deadline: 200,
            target_context: Some(key.as_str().to_string()),
        };
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(1).unwrap(),
                PgId::new(0),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::CreateMultipartUpload(Box::new(
                CreateMultipartUploadCommand::from_request_with_bucket_write_reservation(
                    create,
                    GenerationId::new(1).unwrap(),
                    None,
                    123,
                    proof,
                ),
            )),
        );
        pg.apply_metadata_command_and_record(7, &command).unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let upload = ObjectMutationMetadataNodeClient::load_in_progress_multipart_upload(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &bucket,
        &key,
        &upload_id,
    )
    .unwrap();
    assert_eq!(upload.bucket, bucket);
    assert_eq!(upload.key, key);
    assert_eq!(upload.upload_id, upload_id);
    assert_eq!(upload.state, UploadState::InProgress);
    server_thread.join().unwrap();
}

#[test]
fn unix_direct_put_metadata_client_loads_commit_snapshot() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("direct-put-snapshot-rpc-bucket");
    let key = crate::tests::object_key("direct-put-snapshot-rpc-key");
    let reservation_id = crate::tests::stream_session_id("dp-snap-rpc");
    let reserved_generation = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        let generation =
            PgMetadataStore::reserve_object_generation(&*pg, &bucket, &key, &reservation_id)
                .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        generation
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let snapshot = DirectPutMetadataNodeClient::load_direct_put_commit_snapshot(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &bucket,
        &key,
        &reservation_id,
        reserved_generation,
    )
    .unwrap();
    assert_eq!(snapshot.auth_snapshot.existing_etag, None);
    assert_eq!(snapshot.current, None);
    server_thread.join().unwrap();
}

#[test]
fn unix_direct_put_metadata_client_builds_commit_command() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("direct-put-build-rpc-bucket");
    let key = crate::tests::object_key("direct-put-build-rpc-key");
    let reservation_id = crate::tests::stream_session_id("dp-build-rpc");
    let reserved_generation = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        let generation =
            PgMetadataStore::reserve_object_generation(&*pg, &bucket, &key, &reservation_id)
                .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        generation
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let _stderr_guard = server.suppress_metadata_command_lock_wait_stderr();
    let server_thread = {
        let server = Arc::clone(&server);
        thread::spawn(move || server.accept_one().unwrap())
    };
    let build_server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );
    let proof = BucketWriteReservationProof {
        bucket: bucket.clone(),
        reservation_id: "reservation-1".to_string(),
        owner_token: "owner-token".to_string(),
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        operation_kind: "direct-put".to_string(),
        created_at: 123,
        lease_deadline: 200,
        target_context: Some(key.as_str().to_string()),
    };
    let request = CommitDirectPutObjectReq {
        bucket: bucket.clone(),
        key: key.clone(),
        generation_reservation_id: reservation_id.clone(),
        versioning: BucketVersioningState::Suspended,
        owner: OwnerIdentity {
            principal: "owner".to_string(),
            canonical_id: crate::CanonicalUserId::from_principal("owner"),
        },
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        generation_id: reserved_generation,
        size: 12,
        etag_crc64: 99,
        ec: EcShape { k: 4, m: 2 },
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        object_lock: crate::ObjectLockState::default(),
        encryption: crate::ObjectEncryption::None,
        segment_index: 0,
        segment_crc64: 99,
        segment_okh: [7; 16],
        segment_vid: GenerationId::new(10).unwrap(),
        data_pg_id: 0,
        bucket_write_reservation: proof.clone(),
    };
    let snapshot = DirectPutMetadataNodeClient::load_direct_put_commit_snapshot(
        &client,
        ObjectMetadataPgId::new_for_test(PgId::new(0)),
        &bucket,
        &key,
        &reservation_id,
        reserved_generation,
    )
    .unwrap();

    let command = DirectPutMetadataNodeClient::build_direct_put_commit_command(
        &client,
        BuildDirectPutCommitCommandReq {
            pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            request: &request,
            version_id: VersionId::Null,
            expected_snapshot: &snapshot,
            bucket_write_reservation: &proof,
        },
    )
    .unwrap();

    assert_eq!(command.id().pg_id(), PgId::new(0));
    let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
        panic!("expected direct PUT commit command");
    };
    assert!(commit.matches_request(&bucket, &key, &reservation_id, reserved_generation));
    assert_eq!(commit.bucket_write_reservation, proof);

    let mut bad_payload = command.payload().clone();
    let MetadataCommandPayload::CommitDirectPutObject(bad_commit) = &mut bad_payload else {
        panic!("expected direct PUT commit command");
    };
    bad_commit.stale_payload = Some(ObjectPayloadReclaimCommand::Segments(
        ObjectSegmentsReclaimRecord {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id: reserved_generation,
            created_at: 0,
            segments: vec![ObjectSegmentsReclaimSegmentRecord {
                segment_index: 99,
                segment_okh: [9; 16],
                segment_vid: GenerationId::new(11).unwrap(),
                data_pg_id: 0,
                ec: EcShape { k: 4, m: 2 },
            }],
        },
    ));
    let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
    let err = client
        .validate_direct_put_command_build_response(
            &bad_command,
            &BuildDirectPutCommitCommandReq {
                pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                request: &request,
                version_id: VersionId::Null,
                expected_snapshot: &snapshot,
                bucket_write_reservation: &proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate direct PUT commit command build response",
            ..
        })
    ));
    server_thread.join().unwrap();
    build_server_thread.join().unwrap();
}

#[test]
fn unix_object_mutation_client_rejects_malformed_stream_append_read_responses() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("stream-append-rpc-bucket");
    let key = crate::tests::object_key("stream-append-rpc-key");
    let session_id = crate::tests::stream_session_id("append-rpc");
    let session = StreamUploadRecord {
        session_id: session_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        target: StreamUploadTarget::PutObject,
        state: StreamUploadState::InProgress,
        created_at: 1,
        cleanup_after: None,
        encryption: ObjectEncryption::None,
        next_segment_vid: GenerationId::new(2).unwrap(),
        bucket_write_reservation: None,
    };
    client
        .validate_stream_upload_session_response(
            &session,
            &bucket,
            &key,
            &session_id,
            "validate stream append read response",
        )
        .unwrap();
    let mut bad_session = session.clone();
    bad_session.session_id = crate::tests::stream_session_id("wrong-rpc");
    let err = client
        .validate_stream_upload_session_response(
            &bad_session,
            &bucket,
            &key,
            &session_id,
            "validate stream append read response",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate stream append read response",
            ..
        })
    ));

    let request = PrepareStreamUploadSegmentAppendReq {
        session_id: session_id.clone(),
        segment_index: 0,
        size: 16,
        segment_crc64: 44,
        payload_crc64: 44,
        segment_okh: [3; 16],
    };
    let segment = StreamUploadSegmentRecord {
        session_id: session_id.clone(),
        segment_index: 0,
        size: request.size,
        segment_crc64: request.segment_crc64,
        payload_crc64: request.segment_crc64,
        segment_okh: request.segment_okh,
        segment_vid: GenerationId::new(1).unwrap(),
        data_pg_id: 0,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 1,
        ec_m: 0,
    };
    client
        .validate_stream_segment_append_prepare_response(
            &segment,
            &StreamUploadTarget::PutObject,
            &StreamUploadTarget::PutObject,
            &request,
            "validate stream append read response",
        )
        .unwrap();
    let mut bad_segment = segment.clone();
    bad_segment.size += 1;
    let err = client
        .validate_stream_segment_append_prepare_response(
            &bad_segment,
            &StreamUploadTarget::PutObject,
            &StreamUploadTarget::PutObject,
            &request,
            "validate stream append read response",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate stream append read response",
            ..
        })
    ));
    let mut bad_payload_crc_segment = segment.clone();
    bad_payload_crc_segment.payload_crc64 += 1;
    let err = client
        .validate_stream_segment_append_prepare_response(
            &bad_payload_crc_segment,
            &StreamUploadTarget::PutObject,
            &StreamUploadTarget::PutObject,
            &request,
            "validate stream append read response",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate stream append read response",
            ..
        })
    ));
    let upload_part_target = StreamUploadTarget::UploadPart {
        upload_id: crate::tests::multipart_upload_id("append-rpc-upload"),
        part_number: 1,
    };
    let mut bad_upload_part_segment = segment.clone();
    bad_upload_part_segment.segment_okh = [9; 16];
    let err = client
        .validate_stream_segment_append_prepare_response(
            &bad_upload_part_segment,
            &StreamUploadTarget::PutObject,
            &upload_part_target,
            &request,
            "validate stream append read response",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate stream append read response",
            ..
        })
    ));
    let err = client
        .validate_stream_segment_append_prepare_response(
            &bad_upload_part_segment,
            &upload_part_target,
            &upload_part_target,
            &request,
            "validate stream append read response",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate stream append read response",
            ..
        })
    ));
    client
        .validate_stream_upload_segments_response(
            &[
                segment.clone(),
                StreamUploadSegmentRecord {
                    segment_index: 1,
                    segment_vid: GenerationId::new(2).unwrap(),
                    ..segment.clone()
                },
            ],
            &session_id,
            "validate stream append read response",
        )
        .unwrap();
    let err = client
        .validate_stream_upload_segments_response(
            &[
                StreamUploadSegmentRecord {
                    segment_index: 1,
                    segment_vid: GenerationId::new(2).unwrap(),
                    ..segment.clone()
                },
                segment,
            ],
            &session_id,
            "validate stream append read response",
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate stream append read response",
            ..
        })
    ));
}

#[test]
fn unix_object_mutation_client_rejects_malformed_stream_put_commit_response() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("stream-put-rpc-bucket");
    let key = crate::tests::object_key("stream-put-rpc-key");
    let session_id = crate::tests::stream_session_id("stream-put-rpc");
    let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    let segment = StreamUploadSegmentRecord {
        session_id: session_id.clone(),
        segment_index: 0,
        size: 12,
        segment_crc64: 99,
        payload_crc64: 99,
        segment_okh: [4; 16],
        segment_vid: GenerationId::new(10).unwrap(),
        data_pg_id: 0,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
    };
    let snapshot = StreamPutFinalizeStorageSnapshot {
        session: StreamUploadRecord {
            session_id: session_id.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            target: StreamUploadTarget::PutObject,
            state: StreamUploadState::InProgress,
            created_at: 1,
            cleanup_after: None,
            encryption: ObjectEncryption::None,
            next_segment_vid: GenerationId::new(11).unwrap(),
            bucket_write_reservation: Some(proof.clone()),
        },
        existing_etag: None,
        generation_id: GenerationId::new(20).unwrap(),
        stale_payload_source: None,
        stale_payload: None,
        staging_segments: vec![segment.clone()],
    };
    let commit_input = StreamPutCommitInput {
        versioning: BucketVersioningState::Suspended,
        version_id: VersionId::Null,
        owner: OwnerIdentity::from_principal("owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        size: 12,
        etag_crc64: 99,
        tags: None,
        metadata_blob: SerializedMetadataBlob::default(),
        system_metadata_blob: SerializedSystemMetadataBlob::default(),
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    };
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(CommitDirectPutObjectCommand {
            object: PutLiveObjectReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                owner: commit_input.owner.clone(),
                acl_grants: commit_input.acl_grants.clone(),
                public_read: commit_input.public_read,
                generation_id: snapshot.generation_id,
                size: commit_input.size,
                etag: ObjectEtag::single_part(commit_input.etag_crc64),
                ec: EcShape { k: 4, m: 2 },
                layout: ObjectLayout::Standard,
                tags: commit_input.tags.clone(),
                metadata_blob: Some(commit_input.metadata_blob.clone()),
                system_metadata_blob: Some(commit_input.system_metadata_blob.clone()),
                object_lock: commit_input.object_lock,
                encryption: commit_input.encryption.clone(),
            },
            segments: vec![ObjectSegmentRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: VersionId::Null,
                segment_index: segment.segment_index,
                size: segment.size,
                segment_crc64: segment.segment_crc64,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                data_pg_id: segment.data_pg_id,
                placement_cluster_epoch: segment.placement_cluster_epoch,
                ec_k: segment.ec_k,
                ec_m: segment.ec_m,
            }],
            generation_reservation_id: session_id.clone(),
            write_sequence: 1,
            last_modified_millis: 1,
            stale_payload: None,
            bucket_write_reservation: proof.clone(),
        })),
    );
    let request = BuildStreamPutCommitCommandReq {
        pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        bucket: &bucket,
        key: &key,
        session_id: &session_id,
        total_size: 12,
        expected_snapshot: &snapshot,
        commit: &commit_input,
        bucket_write_reservation: &proof,
    };
    client
        .validate_stream_put_commit_command_response(&command, &request)
        .unwrap();

    let mut substituted_proof_payload = command.payload().clone();
    let MetadataCommandPayload::CommitDirectPutObject(substituted_proof) =
        &mut substituted_proof_payload
    else {
        panic!("expected stream PUT commit command");
    };
    substituted_proof.bucket_write_reservation.reservation_id =
        "substituted-stream-proof".to_string();
    let substituted_proof_command =
        MetadataCommandEnvelope::new(command.id(), substituted_proof_payload);
    let err = client
        .validate_stream_put_commit_command_response(&substituted_proof_command, &request)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate stream PUT commit command build response",
            ..
        })
    ));

    let mut bad_payload = command.payload().clone();
    let MetadataCommandPayload::CommitDirectPutObject(bad_commit) = &mut bad_payload else {
        panic!("expected stream PUT commit command");
    };
    bad_commit.object.generation_id = GenerationId::new(21).unwrap();
    let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
    let err = client
        .validate_stream_put_commit_command_response(&bad_command, &request)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate stream PUT commit command build response",
            ..
        })
    ));

    let stale_generation = GenerationId::new(50).unwrap();
    let mut snapshot_with_stale = snapshot.clone();
    snapshot_with_stale.stale_payload_source = Some(test_live_stored_object(
        bucket.clone(),
        key.clone(),
        stale_generation,
        ObjectLayout::Standard,
    ));
    snapshot_with_stale.stale_payload = Some(test_segments_reclaim(
        bucket.clone(),
        key.clone(),
        stale_generation,
    ));
    let mut stale_payload = command.payload().clone();
    let MetadataCommandPayload::CommitDirectPutObject(stale_commit) = &mut stale_payload else {
        panic!("expected stream PUT commit command");
    };
    stale_commit.stale_payload = snapshot_with_stale.stale_payload.clone();
    let stale_command = MetadataCommandEnvelope::new(command.id(), stale_payload);
    let stale_request = BuildStreamPutCommitCommandReq {
        pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        bucket: &bucket,
        key: &key,
        session_id: &session_id,
        total_size: 12,
        expected_snapshot: &snapshot_with_stale,
        commit: &commit_input,
        bucket_write_reservation: &proof,
    };
    client
        .validate_stream_put_commit_command_response(&stale_command, &stale_request)
        .unwrap();

    let mut bad_payload = command.payload().clone();
    let MetadataCommandPayload::CommitDirectPutObject(bad_commit) = &mut bad_payload else {
        panic!("expected stream PUT commit command");
    };
    bad_commit.stale_payload = Some(test_segments_reclaim(
        bucket.clone(),
        key.clone(),
        GenerationId::new(51).unwrap(),
    ));
    let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
    let err = client
        .validate_stream_put_commit_command_response(&bad_command, &stale_request)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate stream PUT commit command build response",
            ..
        })
    ));
}

#[test]
fn unix_object_mutation_client_rejects_malformed_stream_part_commit_response() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("stream-part-rpc-bucket");
    let key = crate::tests::object_key("stream-part-rpc-key");
    let upload_id = crate::tests::multipart_upload_id("stream-part-rpc-upload");
    let session_id = crate::tests::stream_session_id("stream-part-rpc");
    let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    let upload = MultipartUploadRecord {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        initiated_at: 1,
        state: UploadState::InProgress,
        tags: None,
        metadata_blob: SerializedMetadataBlob::default(),
        system_metadata_blob: SerializedSystemMetadataBlob::default(),
        initiator: OwnerIdentity::from_principal("owner"),
        owner: OwnerIdentity::from_principal("owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        object_generation_id: GenerationId::new(30).unwrap(),
        initiated_object_identity: Some(crate::MultipartObjectIdentity::Live {
            version_id: VersionId::from_u64(1),
            generation_id: GenerationId::new(29).unwrap(),
        }),
        object_lock: ObjectLockState::default(),
        checksum: None,
        encryption: ObjectEncryption::None,
    };
    let snapshot = StreamUploadPartStorageSnapshot {
        auth_snapshot: StreamUploadPartSnapshot {
            session: StreamUploadRecord {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: StreamUploadTarget::UploadPart {
                    upload_id: upload_id.clone(),
                    part_number: 1,
                },
                state: StreamUploadState::InProgress,
                created_at: 1,
                cleanup_after: None,
                encryption: ObjectEncryption::None,
                next_segment_vid: GenerationId::new(31).unwrap(),
                bucket_write_reservation: None,
            },
            upload: upload.clone(),
            existing_part_generation: None,
            staging_segments: vec![StreamUploadSegmentRecord {
                session_id: session_id.clone(),
                segment_index: 0,
                size: 12,
                segment_crc64: 100,
                payload_crc64: 100,
                segment_okh: [5; 16],
                segment_vid: GenerationId::new(32).unwrap(),
                data_pg_id: 0,
                placement_cluster_epoch: ClusterEpoch::INITIAL,
                ec_k: 4,
                ec_m: 2,
            }],
        },
        existing_part: None,
        displaced_segments: Vec::new(),
    };
    let part = MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 1,
        size: 12,
        payload_crc64: 0,
        etag: vec![1; 8],
        etag_kind: EtagKind::Crc64,
        part_vid: GenerationId::new(40).unwrap(),
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
        last_modified: 10,
        checksum: None,
    };
    let segments = vec![MultipartPartSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        version_id: 1,
        part_number: 1,
        segment_index: 0,
        size: 12,
        segment_crc64: 100,
        segment_okh: [5; 16],
        segment_vid: GenerationId::new(32).unwrap(),
        data_pg_id: 0,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
    }];
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
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
            bucket_write_reservation: proof.clone(),
        })),
    );
    let request = BuildStreamPartCommitCommandReq {
        pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        bucket: &bucket,
        key: &key,
        upload_id: &upload_id,
        session_id: &session_id,
        part_number: 1,
        expected_snapshot: &snapshot,
        part: &part,
        segments: &segments,
        bucket_write_reservation: &proof,
    };
    client
        .validate_stream_part_commit_command_response(&command, &request)
        .unwrap();

    let mut bad_payload = command.payload().clone();
    let MetadataCommandPayload::CommitStreamPart(bad_commit) = &mut bad_payload else {
        panic!("expected stream part commit command");
    };
    bad_commit.segments[0].key = crate::tests::object_key("wrong-stream-part-key");
    let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
    let err = client
        .validate_stream_part_commit_command_response(&bad_command, &request)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate stream part commit command build response",
            ..
        })
    ));
}

#[test]
fn unix_object_mutation_client_rejects_stale_stream_part_commit_command_epoch() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let stale_epoch = ClusterEpoch::new(config.cluster_epoch.get() + 1).unwrap();
    let client = UnixStorageNodeClient::new(NodeId::new(7), stale_epoch, config.socket_path);
    let bucket = crate::tests::bucket_name("stale-stream-part-rpc-bucket");
    let key = crate::tests::object_key("stale-stream-part-rpc-key");
    let mut proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    proof.operation_kind =
        crate::metadata_command::UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND
            .to_string();
    let upload_id = crate::tests::multipart_upload_id("stale-stream-part-rpc-upload");
    let session_id = crate::tests::stream_session_id("staleprtcmit");
    let upload = test_multipart_upload_record(
        bucket.clone(),
        key.clone(),
        upload_id.clone(),
        UploadState::InProgress,
    );
    let snapshot = StreamUploadPartStorageSnapshot {
        auth_snapshot: StreamUploadPartSnapshot {
            session: StreamUploadRecord {
                session_id: session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: StreamUploadTarget::UploadPart {
                    upload_id: upload_id.clone(),
                    part_number: 1,
                },
                state: StreamUploadState::InProgress,
                created_at: 1,
                cleanup_after: None,
                encryption: ObjectEncryption::None,
                next_segment_vid: GenerationId::new(31).unwrap(),
                bucket_write_reservation: None,
            },
            upload: upload.clone(),
            existing_part_generation: None,
            staging_segments: Vec::new(),
        },
        existing_part: None,
        displaced_segments: Vec::new(),
    };
    let part = MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 0,
        size: 12,
        payload_crc64: 100,
        etag: vec![0x51; 8],
        etag_kind: EtagKind::Crc64,
        part_vid: GenerationId::new(40).unwrap(),
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
        last_modified: 10,
        checksum: None,
    };
    let segments = vec![MultipartPartSegmentRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        version_id: crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64(),
        part_number: 1,
        segment_index: 0,
        size: 12,
        segment_crc64: 100,
        segment_okh: [0x52; 16],
        segment_vid: GenerationId::new(41).unwrap(),
        data_pg_id: 0,
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
    }];

    let err = ObjectMutationMetadataNodeClient::build_stream_part_commit_command(
        &client,
        BuildStreamPartCommitCommandReq {
            pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
            cluster_epoch: stale_epoch,
            bucket: &bucket,
            key: &key,
            upload_id: &upload_id,
            session_id: &session_id,
            part_number: 1,
            expected_snapshot: &snapshot,
            part: &part,
            segments: &segments,
            bucket_write_reservation: &proof,
        },
    )
    .unwrap_err();

    assert!(
        matches!(
            err,
            ObjectPgActionError::Store(StoreError::StorageRpc {
                operation: "object stream part commit command build",
                failure: StorageRpcErrorCode::StaleShardLocation,
                ref detail,
                ..
            }) if detail.as_str().contains(&format!("request route epoch {stale_epoch}"))
                && detail.as_str().contains(&format!("storage-node epoch {}", config.cluster_epoch))
        ),
        "stale stream-part commit command-build RPC should fail route validation, got {err:?}"
    );
    server_thread.join().unwrap();
}

#[test]
fn unix_complete_multipart_uses_durable_initiation_identity() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("complete-mpu-identity-rpc-bucket");
    let key = crate::tests::object_key("complete-mpu-identity-rpc-key");
    let upload_id = crate::tests::multipart_upload_id("complete-mpu-identity-rpc-upload");
    let owner = OwnerIdentity::from_principal("owner");
    let put_live = |generation_id| PutLiveObjectReq {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id: VersionId::Null,
        owner: owner.clone(),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::new(generation_id).unwrap(),
        size: 0,
        etag: ObjectEtag::single_part(generation_id),
        ec: EcShape { k: 4, m: 2 },
        layout: ObjectLayout::Standard,
        tags: None,
        metadata_blob: Some(SerializedMetadataBlob::default()),
        system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
    };
    let upload = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner.canonical_id,
            &AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::put_object_meta(&*pg, &crate::PutObjectReq::Live(put_live(1))).unwrap();
        PgMetadataStore::create_multipart_upload(
            &*pg,
            &CreateMultipartUploadReq {
                upload_id: upload_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                tags: None,
                metadata_blob: SerializedMetadataBlob::default(),
                system_metadata_blob: SerializedSystemMetadataBlob::default(),
                initiator: owner.clone(),
                owner: owner.clone(),
                acl_grants: AclGrants::default(),
                public_read: false,
                object_lock: ObjectLockState::default(),
                checksum: None,
                encryption: ObjectEncryption::None,
            },
        )
        .unwrap();
        pg.test_force_multipart_upload_initiated_object_identity(
            &upload_id,
            crate::MultipartObjectIdentity::Live {
                version_id: VersionId::Null,
                generation_id: GenerationId::MIN,
            },
        )
        .unwrap();
        PgMetadataStore::put_object_meta(&*pg, &crate::PutObjectReq::Live(put_live(3))).unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        PgMetadataStore::get_multipart_upload(&*pg, &upload_id).unwrap()
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path,
    );
    let current_identity = crate::MultipartObjectIdentity::Live {
        version_id: VersionId::Null,
        generation_id: GenerationId::new(3).unwrap(),
    };
    let part = test_multipart_part_record(upload_id.clone(), 1);
    let request = CompleteMultipartCommitRequest {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        completion_fingerprint: crate::MultipartCompletionFingerprint::from_bytes([0x77; 32]),
        versioning: BucketVersioningState::Disabled,
        owner,
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: upload.object_generation_id,
        size: part.size,
        etag_crc64: [8; 8],
        tags: None,
        metadata_blob: Some(SerializedMetadataBlob::default()),
        system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
        expected_stale_payload_source: None,
        expected_current_object_identity: Some(current_identity),
        conditional_completion: true,
        part_records: vec![part],
        selected_streaming_segments: Vec::new(),
        expected_cleanup: CompleteMultipartCommitCleanup::default(),
    };
    let mut proof = test_bucket_write_reservation_proof(bucket, &key);
    proof.operation_kind = COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND.to_string();
    let error = ObjectMutationMetadataNodeClient::build_complete_multipart_object_command(
        &client,
        BuildCompleteMultipartObjectCommandReq {
            pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            request: &request,
            version_id: VersionId::Null,
            expected_object_parts: &[],
            bucket_write_reservation: &proof,
        },
    )
    .unwrap_err();
    assert!(matches!(
        error,
        ObjectPgActionError::MultipartConditionalRequestConflict
    ));
    server_thread.join().unwrap();
}

#[test]
fn unix_object_mutation_client_rejects_malformed_complete_multipart_response() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("complete-mpu-rpc-bucket");
    let key = crate::tests::object_key("complete-mpu-rpc-key");
    let upload_id = crate::tests::multipart_upload_id("complete-mpu-rpc-upload");
    let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    let part = MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 1,
        size: 12,
        payload_crc64: 0,
        etag: vec![1; 8],
        etag_kind: EtagKind::Crc64,
        part_vid: GenerationId::new(40).unwrap(),
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
        last_modified: 10,
        checksum: None,
    };
    let request = CompleteMultipartCommitRequest {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        completion_fingerprint: crate::MultipartCompletionFingerprint::from_bytes([0x55; 32]),
        versioning: BucketVersioningState::Enabled,
        owner: OwnerIdentity::from_principal("owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        generation_id: GenerationId::new(30).unwrap(),
        size: 12,
        etag_crc64: [8; 8],
        tags: None,
        metadata_blob: Some(SerializedMetadataBlob::default()),
        system_metadata_blob: Some(SerializedSystemMetadataBlob::default()),
        object_lock: ObjectLockState::default(),
        encryption: ObjectEncryption::None,
        expected_stale_payload_source: None,
        expected_current_object_identity: Some(crate::MultipartObjectIdentity::Live {
            version_id: VersionId::from_u64(2),
            generation_id: GenerationId::new(31).unwrap(),
        }),
        conditional_completion: true,
        part_records: vec![part.clone()],
        selected_streaming_segments: Vec::new(),
        expected_cleanup: CompleteMultipartCommitCleanup::default(),
    };
    let version_id = VersionId::from_u64(9);
    let parts_count = std::num::NonZeroU32::new(1).unwrap();
    let expected_object_parts = vec![ObjectPartRecord {
        bucket: bucket.clone(),
        key: key.clone(),
        version_id,
        part_number: part.part_number,
        size: part.size,
        payload_crc64: 0,
        etag: part.etag.clone(),
        etag_kind: part.etag_kind,
        part_vid: part.part_vid,
        placement_cluster_epoch: part.placement_cluster_epoch,
        ec_k: part.ec_k,
        ec_m: part.ec_m,
        data_pg_id: 0,
        checksum: part.checksum.clone(),
    }];
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::CommitMultipartObject(Box::new(CommitMultipartObjectCommand {
            upload_id: upload_id.clone(),
            completion_fingerprint: request.completion_fingerprint,
            bucket_write_reservation: proof.clone(),
            object: PutLiveObjectReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                owner: request.owner.clone(),
                acl_grants: request.acl_grants.clone(),
                public_read: request.public_read,
                generation_id: request.generation_id,
                size: request.size,
                etag: ObjectEtag::MultipartComposite {
                    crc64: request.etag_crc64,
                    parts: parts_count,
                },
                ec: EcShape { k: 0, m: 0 },
                layout: ObjectLayout::MultipartManifest { parts_count },
                tags: request.tags.clone(),
                metadata_blob: request.metadata_blob.clone(),
                system_metadata_blob: request.system_metadata_blob.clone(),
                object_lock: request.object_lock,
                encryption: request.encryption.clone(),
            },
            parts: expected_object_parts.clone(),
            selected_streaming_segments: Vec::new(),
            omitted_parts: Vec::new(),
            omitted_streaming_segments: Vec::new(),
            stream_uploads: Vec::new(),
            stream_upload_segments: Vec::new(),
            write_sequence: 1,
            last_modified_millis: 3,
            stale_payload: None,
        })),
    );
    let build = BuildCompleteMultipartObjectCommandReq {
        pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        request: &request,
        version_id,
        expected_object_parts: &expected_object_parts,
        bucket_write_reservation: &proof,
    };
    client
        .validate_complete_multipart_command_response(&command, &build)
        .unwrap();

    let mut bad_payload = command.payload().clone();
    let MetadataCommandPayload::CommitMultipartObject(bad_commit) = &mut bad_payload else {
        panic!("expected complete multipart command");
    };
    bad_commit.parts[0].key = crate::tests::object_key("wrong-complete-mpu-key");
    let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
    let err = client
        .validate_complete_multipart_command_response(&bad_command, &build)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate complete multipart command build response",
            ..
        })
    ));

    let mut bad_payload = command.payload().clone();
    let MetadataCommandPayload::CommitMultipartObject(bad_commit) = &mut bad_payload else {
        panic!("expected complete multipart command");
    };
    bad_commit.parts[0].data_pg_id = 424_242;
    let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
    let err = client
        .validate_complete_multipart_command_response(&bad_command, &build)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate complete multipart command build response",
            ..
        })
    ));

    let mut missing_cleanup_request = request.clone();
    missing_cleanup_request.expected_cleanup.omitted_parts = vec![MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 2,
        generation: 1,
        size: 9,
        payload_crc64: 0,
        etag: vec![2; 8],
        etag_kind: EtagKind::Crc64,
        part_vid: GenerationId::new(41).unwrap(),
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
        last_modified: 11,
        checksum: None,
    }];
    let missing_cleanup_build = BuildCompleteMultipartObjectCommandReq {
        pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        request: &missing_cleanup_request,
        version_id,
        expected_object_parts: &expected_object_parts,
        bucket_write_reservation: &proof,
    };
    let err = client
        .validate_complete_multipart_command_response(&command, &missing_cleanup_build)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate complete multipart command build response",
            ..
        })
    ));

    let mut bad_cleanup_payload = command.payload().clone();
    let MetadataCommandPayload::CommitMultipartObject(bad_cleanup) = &mut bad_cleanup_payload
    else {
        panic!("expected complete multipart command");
    };
    bad_cleanup.omitted_parts.push(part.clone());
    let bad_cleanup_command = MetadataCommandEnvelope::new(command.id(), bad_cleanup_payload);
    let err = client
        .validate_complete_multipart_command_response(&bad_cleanup_command, &build)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate complete multipart command build response",
            ..
        })
    ));

    let stale_source_generation = GenerationId::new(50).unwrap();
    let mut null_request = request.clone();
    null_request.versioning = BucketVersioningState::Suspended;
    null_request.expected_stale_payload_source = Some(test_live_stored_object(
        bucket.clone(),
        key.clone(),
        stale_source_generation,
        ObjectLayout::Standard,
    ));
    let mut null_expected_object_parts = expected_object_parts.clone();
    for part in &mut null_expected_object_parts {
        part.version_id = VersionId::Null;
    }
    let null_build = BuildCompleteMultipartObjectCommandReq {
        pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
        cluster_epoch: ClusterEpoch::new(1).unwrap(),
        request: &null_request,
        version_id: VersionId::Null,
        expected_object_parts: &null_expected_object_parts,
        bucket_write_reservation: &proof,
    };
    let mut stale_payload = command.payload().clone();
    let MetadataCommandPayload::CommitMultipartObject(stale_commit) = &mut stale_payload else {
        panic!("expected complete multipart command");
    };
    stale_commit.object.version_id = VersionId::Null;
    for part in &mut stale_commit.parts {
        part.version_id = VersionId::Null;
    }
    stale_commit.stale_payload = Some(test_segments_reclaim(
        bucket.clone(),
        key.clone(),
        GenerationId::new(51).unwrap(),
    ));
    let stale_command = MetadataCommandEnvelope::new(command.id(), stale_payload);
    let err = client
        .validate_complete_multipart_command_response(&stale_command, &null_build)
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate complete multipart command build response",
            ..
        })
    ));
}

#[test]
fn unix_object_mutation_client_rejects_malformed_abort_multipart_response() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("abort-mpu-rpc-bucket");
    let key = crate::tests::object_key("abort-mpu-rpc-key");
    let upload_id = crate::tests::multipart_upload_id("abort-mpu-rpc-upload");
    let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    let upload = MultipartUploadRecord {
        upload_id: upload_id.clone(),
        bucket: bucket.clone(),
        key: key.clone(),
        initiated_at: 1,
        state: UploadState::InProgress,
        tags: None,
        metadata_blob: SerializedMetadataBlob::default(),
        system_metadata_blob: SerializedSystemMetadataBlob::default(),
        initiator: OwnerIdentity::from_principal("owner"),
        owner: OwnerIdentity::from_principal("owner"),
        acl_grants: AclGrants::default(),
        public_read: false,
        object_generation_id: GenerationId::new(30).unwrap(),
        initiated_object_identity: None,
        object_lock: ObjectLockState::default(),
        checksum: None,
        encryption: ObjectEncryption::None,
    };
    let cleanup = crate::types::AbortMultipartUploadCleanup {
        upload,
        parts: Vec::new(),
        streaming_segments: Vec::new(),
        stream_uploads: Vec::new(),
        stream_upload_segments: Vec::new(),
    };
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            cleanup: cleanup.clone(),
            bucket_write_reservation: proof.clone(),
        })),
    );
    client
        .validate_abort_multipart_command_response(
            &command,
            &AbortMultipartCommandValidation {
                pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                upload_id: &upload_id,
                expected_cleanup: Some(&cleanup),
                bucket_write_reservation: &proof,
            },
        )
        .unwrap();

    let mut bad_payload = command.payload().clone();
    let MetadataCommandPayload::AbortMultipartUpload(bad_abort) = &mut bad_payload else {
        panic!("expected abort multipart command");
    };
    bad_abort.upload_id = crate::tests::multipart_upload_id("abort-mpu-rpc-wrong");
    let bad_command = MetadataCommandEnvelope::new(command.id(), bad_payload);
    let err = client
        .validate_abort_multipart_command_response(
            &bad_command,
            &AbortMultipartCommandValidation {
                pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                upload_id: &upload_id,
                expected_cleanup: Some(&cleanup),
                bucket_write_reservation: &proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate abort multipart command build response",
            ..
        })
    ));

    let mut bad_cleanup_payload = command.payload().clone();
    let MetadataCommandPayload::AbortMultipartUpload(bad_cleanup) = &mut bad_cleanup_payload else {
        panic!("expected abort multipart command");
    };
    bad_cleanup.cleanup.parts.push(MultipartPartRecord {
        upload_id: crate::tests::multipart_upload_id("abort-mpu-rpc-other"),
        part_number: 1,
        generation: 1,
        size: 12,
        payload_crc64: 0,
        etag: vec![1; 8],
        etag_kind: EtagKind::Crc64,
        part_vid: GenerationId::new(40).unwrap(),
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
        last_modified: 10,
        checksum: None,
    });
    let bad_cleanup_command = MetadataCommandEnvelope::new(command.id(), bad_cleanup_payload);
    let err = client
        .validate_abort_multipart_command_response(
            &bad_cleanup_command,
            &AbortMultipartCommandValidation {
                pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                upload_id: &upload_id,
                expected_cleanup: Some(&cleanup),
                bucket_write_reservation: &proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate abort multipart command build response",
            ..
        })
    ));

    let mut expected_missing_cleanup = cleanup.clone();
    expected_missing_cleanup.parts.push(MultipartPartRecord {
        upload_id: upload_id.clone(),
        part_number: 1,
        generation: 1,
        size: 12,
        payload_crc64: 0,
        etag: vec![1; 8],
        etag_kind: EtagKind::Crc64,
        part_vid: GenerationId::new(40).unwrap(),
        placement_cluster_epoch: ClusterEpoch::INITIAL,
        ec_k: 4,
        ec_m: 2,
        last_modified: 10,
        checksum: None,
    });
    let err = client
        .validate_abort_multipart_command_response(
            &command,
            &AbortMultipartCommandValidation {
                pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                upload_id: &upload_id,
                expected_cleanup: Some(&expected_missing_cleanup),
                bucket_write_reservation: &proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate abort multipart command build response",
            ..
        })
    ));
}
