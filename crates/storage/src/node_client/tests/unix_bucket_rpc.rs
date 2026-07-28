use super::*;
use crate::control_plane::{PendingMetadataCommandObservation, PendingMetadataCommandRecovery};

#[derive(Clone, Copy)]
enum HistoricalReplicaHeadAuthorization {
    Matching,
    Missing,
    WrongEpoch,
}

fn historical_bucket_delete_replica_head(
    authorization: HistoricalReplicaHeadAuthorization,
) -> Result<BucketInfo, BucketSnapshotLoadError> {
    let tmp = test_util::tempdir();
    let source_epoch = ClusterEpoch::new(1).unwrap();
    let other_epoch = ClusterEpoch::new(2).unwrap();
    let current_epoch = ClusterEpoch::new(3).unwrap();
    let mut config = test_config(&tmp);
    config.node_id = NodeId::new(8);
    config.cluster_epoch = current_epoch;
    config.route_map_validity =
        RouteMapValidity::until_ms(crate::clock::current_time_millis().saturating_add(60_000))
            .unwrap();
    config.pg_routes[0] = StorageNodePgRoute {
        pg_id: 0,
        cluster_epoch: current_epoch,
        state: crate::types::PgState::Peering,
        primary_node_id: NodeId::new(7),
        acting_set: vec![NodeId::new(7), NodeId::new(8)],
    };
    let retained_route = |cluster_epoch| StorageNodePgRoute {
        pg_id: 0,
        cluster_epoch,
        state: crate::types::PgState::Active,
        primary_node_id: NodeId::new(7),
        acting_set: vec![NodeId::new(7), NodeId::new(8)],
    };
    config
        .historical_pg_routes
        .push(retained_route(source_epoch));
    let authorized_epoch = match authorization {
        HistoricalReplicaHeadAuthorization::Matching => Some(source_epoch),
        HistoricalReplicaHeadAuthorization::Missing => None,
        HistoricalReplicaHeadAuthorization::WrongEpoch => {
            config
                .historical_pg_routes
                .push(retained_route(other_epoch));
            Some(other_epoch)
        }
    };
    if let Some(authorized_epoch) = authorized_epoch {
        config.pending_metadata_command_recoveries.push((
            PgId::new(0),
            PendingMetadataCommandRecovery::new(
                NodeId::new(7),
                PendingMetadataCommandObservation::new(
                    authorized_epoch,
                    std::num::NonZeroU64::MIN,
                    0x1234,
                ),
            ),
        ));
    }
    let bucket = crate::tests::bucket_name("historical-delete-replica-head-bucket");
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
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client =
        UnixStorageNodeClient::new(config.node_id, source_epoch, config.socket_path.clone());
    let result = BucketMetadataNodeClient::head_bucket_replica_for_delete(
        &client,
        bucket_pg_id_for_test(0),
        &bucket,
    );
    server_thread.join().unwrap();
    result
}

fn bucket_pg_id_for_test(pg_id: u32) -> BucketPgId {
    BucketPgId::new_for_test(PgId::new(pg_id))
}

#[test]
fn unix_object_list_response_requires_truncated_marker_identity() {
    let client = test_unix_storage_node_client();
    let bucket = crate::tests::bucket_name("list-marker-bucket");
    let key = crate::tests::object_key("list-marker-key");
    let object = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        GenerationId::new(10).unwrap(),
        ObjectLayout::Standard,
    );
    let req = ListObjectsReq {
        bucket,
        prefix: None,
        start_after: None,
        start_at: None,
        max_keys: 1,
    };
    let mut response = ListObjectsResp {
        objects: vec![object],
        is_truncated: true,
        next_start_after: None,
    };
    assert!(validate_list_objects_response(&client, &response, &req).is_err());

    response.next_start_after = Some(crate::tests::object_key("wrong-marker"));
    assert!(validate_list_objects_response(&client, &response, &req).is_err());

    response.next_start_after = Some(key);
    validate_list_objects_response(&client, &response, &req).unwrap();
}

#[test]
fn unix_object_version_list_response_requires_truncated_marker_identity() {
    let client = test_unix_storage_node_client();
    let bucket = crate::tests::bucket_name("version-list-marker-bucket");
    let key = crate::tests::object_key("version-list-marker-key");
    let version_id = VersionId::from_u64(44);
    let mut object = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        GenerationId::new(10).unwrap(),
        ObjectLayout::Standard,
    );
    let StoredObject::Live(record) = &mut object else {
        unreachable!("test helper always builds a live object");
    };
    record.version_id = version_id;
    let req = ListObjectVersionsReq {
        bucket,
        prefix: None,
        key_marker: None,
        version_id_marker: None,
        start_at: None,
        max_keys: 1,
    };
    let mut response = ListObjectVersionsResp {
        versions: vec![object],
        is_truncated: true,
        next_key_marker: Some(key.clone()),
        next_version_id_marker: None,
    };
    assert!(validate_list_object_versions_response(&client, &response, &req).is_err());

    response.next_version_id_marker = Some(VersionId::from_u64(45));
    assert!(validate_list_object_versions_response(&client, &response, &req).is_err());

    response.next_version_id_marker = Some(version_id);
    validate_list_object_versions_response(&client, &response, &req).unwrap();
}

#[test]
fn unix_multipart_upload_list_response_requires_final_upload_marker_identity() {
    let client = test_unix_storage_node_client();
    let bucket = crate::tests::bucket_name("mpu-list-marker-bucket");
    let key = crate::tests::object_key("mpu-list-marker-key");
    let upload_id = UploadId::try_from("u".repeat(crate::UPLOAD_ID_LEN)).unwrap();
    let upload = test_multipart_upload_record(
        bucket.clone(),
        key.clone(),
        upload_id.clone(),
        UploadState::InProgress,
    );
    let req = ListMultipartUploadsReq {
        bucket,
        prefix: None,
        page_start: None,
        max_uploads: 1,
    };
    let mut response = ListMultipartUploadsResp {
        uploads: vec![upload],
        is_truncated: false,
        next_key_marker: Some(key.clone()),
        next_upload_id_marker: None,
    };
    assert!(validate_list_multipart_uploads_response(&client, &response, &req).is_err());

    response.next_upload_id_marker =
        Some(UploadId::try_from("v".repeat(crate::UPLOAD_ID_LEN)).unwrap());
    assert!(validate_list_multipart_uploads_response(&client, &response, &req).is_err());

    response.next_upload_id_marker = Some(upload_id.clone());
    validate_list_multipart_uploads_response(&client, &response, &req).unwrap();

    response.is_truncated = true;
    validate_list_multipart_uploads_response(&client, &response, &req).unwrap();

    let mut empty = ListMultipartUploadsResp {
        uploads: Vec::new(),
        is_truncated: false,
        next_key_marker: None,
        next_upload_id_marker: None,
    };
    validate_list_multipart_uploads_response(&client, &empty, &req).unwrap();

    empty.next_key_marker = Some(key);
    empty.next_upload_id_marker = Some(upload_id);
    assert!(validate_list_multipart_uploads_response(&client, &empty, &req).is_err());

    empty.next_key_marker = None;
    empty.next_upload_id_marker = None;
    empty.is_truncated = true;
    assert!(validate_list_multipart_uploads_response(&client, &empty, &req).is_err());
}

fn test_bucket_info(
    name: BucketName,
    owner: &crate::CanonicalUserId,
    acl_grants: &crate::AclGrants,
) -> BucketInfo {
    BucketInfo {
        name,
        owner_principal: "owner".to_string(),
        owner_canonical_id: owner.clone(),
        created_at: 123,
        region: 0,
        state: BucketState::Active,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        acl_grants: acl_grants.clone(),
        public_read: false,
        public_write: false,
        public_access_block: None,
        ownership_controls: None,
        bucket_policy_present: false,
        bucket_policy_public: false,
        bucket_policy_generation: 0,
        bucket_lifecycle_present: false,
        bucket_lifecycle_generation: 0,
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        multipart_upload_id_key: crate::types::MultipartUploadIdKey::from_bytes([1; 32]),
        bucket_abac_enabled: false,
        encryption: crate::types::EffectiveBucketEncryptionConfig::default(),
    }
}

#[test]
fn unix_create_bucket_build_response_rejects_mismatched_identity() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("create-bucket-rpc-expected");
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
    let command_id = MetadataCommandId::new(
        ClusterEpoch::new(1).unwrap(),
        PgId::new(0),
        MetadataCommandLogIndex::new(1).unwrap(),
    );

    let wrong_bucket = crate::tests::bucket_name("create-bucket-rpc-wrong");
    let err = client
        .validate_create_bucket_command_build_outcome(
            StorageRpcCreateBucketCommandBuildOutcome::Exists(test_bucket_info(
                wrong_bucket,
                &owner,
                &acl_grants,
            )),
            &bucket,
            command_id,
            &config,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate create-bucket command build response",
            ..
        })
    ));

    let other_owner = crate::CanonicalUserId::from_principal("other-owner");
    let bad_config = crate::CreateBucketConfig {
        name: bucket.as_str(),
        owner_principal: "other-owner",
        owner_canonical_id: &other_owner,
        acl_grants: &acl_grants,
        public_read: false,
        public_write: false,
        versioning: crate::BucketVersioningState::Disabled,
        object_lock: crate::BucketObjectLockConfig::default(),
        ownership_controls: crate::BucketOwnershipControls {
            object_ownership: crate::BucketObjectOwnership::ObjectWriter,
        },
    };
    let bad_command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::CreateBucket(
            CreateBucketCommand::from_config(&bad_config, 123, 1).unwrap(),
        ),
    );
    let err = client
        .validate_create_bucket_command_build_outcome(
            StorageRpcCreateBucketCommandBuildOutcome::Command(Box::new(bad_command)),
            &bucket,
            command_id,
            &config,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate create-bucket command build response",
            ..
        })
    ));
}

#[test]
fn unix_multipart_completion_barrier_build_response_rejects_mismatched_identity() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("completed-order-rpc-expected");
    let wrong_bucket = crate::tests::bucket_name("completed-order-rpc-wrong");
    let command_id = MetadataCommandId::new(
        ClusterEpoch::new(1).unwrap(),
        PgId::new(0),
        MetadataCommandLogIndex::new(1).unwrap(),
    );
    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
            AdvanceMultipartCompletionBarrierCommand {
                bucket: wrong_bucket,
                barrier_sequence: 3,
            },
        ),
    );

    let err = client
        .validate_multipart_completion_barrier_command_build_response(
            3, command, &bucket, command_id,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate multipart completion barrier command build response",
            ..
        })
    ));

    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
            AdvanceMultipartCompletionBarrierCommand {
                bucket: bucket.clone(),
                barrier_sequence: 0,
            },
        ),
    );
    let err = client
        .validate_multipart_completion_barrier_command_build_response(
            0, command, &bucket, command_id,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate multipart completion barrier command build response",
            ..
        })
    ));

    let command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(
            AdvanceMultipartCompletionBarrierCommand {
                bucket: bucket.clone(),
                barrier_sequence: 4,
            },
        ),
    );
    let err = client
        .validate_multipart_completion_barrier_command_build_response(
            3, command, &bucket, command_id,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate multipart completion barrier command build response",
            ..
        })
    ));
}

#[test]
fn unix_mark_bucket_deleting_build_response_rejects_mismatched_identity() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("mark-deleting-rpc-expected");
    let wrong_bucket = crate::tests::bucket_name("mark-deleting-rpc-wrong");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let command_id = MetadataCommandId::new(
        ClusterEpoch::new(1).unwrap(),
        PgId::new(0),
        MetadataCommandLogIndex::new(1).unwrap(),
    );
    let wrong_bucket_record = BucketRecord::from_create_config(
        &crate::CreateBucketConfig {
            name: wrong_bucket.as_str(),
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
        },
        123,
        1,
    )
    .unwrap();
    let wrong_bucket_command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand::from_bucket(
            wrong_bucket_record,
        )),
    );

    let err = client
        .validate_mark_bucket_deleting_command_build_response(
            wrong_bucket_command,
            &bucket,
            command_id,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate bucket mark-deleting command build response",
            ..
        })
    ));

    let active_bucket_record = BucketRecord::from_create_config(
        &crate::CreateBucketConfig {
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
        },
        123,
        1,
    )
    .unwrap();
    let active_state_command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::MarkBucketDeleting(MarkBucketDeletingCommand {
            bucket: active_bucket_record,
        }),
    );
    let err = client
        .validate_mark_bucket_deleting_command_build_response(
            active_state_command,
            &bucket,
            command_id,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate bucket mark-deleting command build response",
            ..
        })
    ));

    let create_bucket_config = crate::CreateBucketConfig {
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
    let wrong_payload_command = MetadataCommandEnvelope::new(
        command_id,
        MetadataCommandPayload::CreateBucket(
            CreateBucketCommand::from_config(&create_bucket_config, 123, 1).unwrap(),
        ),
    );

    let err = client
        .validate_mark_bucket_deleting_command_build_response(
            wrong_payload_command,
            &bucket,
            command_id,
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate bucket mark-deleting command build response",
            ..
        })
    ));

    let mut active_info = test_bucket_info(bucket.clone(), &owner, &acl_grants);
    active_info.state = BucketState::Active;
    let err = client
        .validate_mark_bucket_deleting_already_deleting_response(&active_info, &bucket)
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate bucket mark-deleting command build response",
            ..
        })
    ));

    let mut deleting_info = test_bucket_info(wrong_bucket, &owner, &acl_grants);
    deleting_info.state = BucketState::Deleting;
    let err = client
        .validate_mark_bucket_deleting_already_deleting_response(&deleting_info, &bucket)
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "validate bucket mark-deleting command build response",
            ..
        })
    ));
}

#[test]
fn unix_object_read_snapshot_response_rejects_missing_multipart_parts() {
    let bucket = crate::tests::bucket_name("object-read-missing-part-rpc");
    let key = crate::tests::object_key("object-read-missing-part-rpc-key");
    let stored = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        GenerationId::new(10).unwrap(),
        ObjectLayout::MultipartManifest {
            parts_count: std::num::NonZeroU32::new(2).unwrap(),
        },
    );
    let snapshot = ObjectReadSnapshot {
        stored,
        object_segments: Vec::new(),
        multipart_parts: vec![test_object_read_multipart_part(&bucket, &key, 1)],
        multipart_part_segments: Vec::new(),
    };

    assert_object_read_snapshot_rejected(&snapshot, ObjectReadSnapshotMode::MultipartParts);
}

#[test]
fn unix_object_read_snapshot_response_rejects_orphan_multipart_segments() {
    let bucket = crate::tests::bucket_name("object-read-orphan-seg-rpc");
    let key = crate::tests::object_key("object-read-orphan-seg-rpc-key");
    let stored = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        GenerationId::new(10).unwrap(),
        test_multipart_layout(),
    );
    let snapshot = ObjectReadSnapshot {
        stored,
        object_segments: Vec::new(),
        multipart_parts: vec![test_object_read_multipart_part(&bucket, &key, 1)],
        multipart_part_segments: vec![test_object_read_multipart_segment(&bucket, &key, 2)],
    };

    assert_object_read_snapshot_rejected(&snapshot, ObjectReadSnapshotMode::FullPayloadLayout);
}

#[test]
fn unix_object_read_snapshot_response_accepts_zero_byte_multipart_part_without_segments() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("object-read-zero-part-rpc");
    let key = crate::tests::object_key("object-read-zero-part-rpc-key");
    let stored = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        GenerationId::new(10).unwrap(),
        ObjectLayout::MultipartManifest {
            parts_count: std::num::NonZeroU32::new(2).unwrap(),
        },
    );
    let mut zero_part = test_object_read_multipart_part(&bucket, &key, 2);
    zero_part.size = 0;
    let snapshot = ObjectReadSnapshot {
        stored,
        object_segments: Vec::new(),
        multipart_parts: vec![test_object_read_multipart_part(&bucket, &key, 1), zero_part],
        multipart_part_segments: vec![test_object_read_multipart_segment(&bucket, &key, 1)],
    };
    let identity = ObjectReadAuthSubjectIdentity::for_stored(&snapshot.stored);

    client
        .validate_object_read_snapshot_response(
            &snapshot,
            &bucket,
            &key,
            Some(VersionId::Null),
            &identity,
            ObjectReadSnapshotMode::FullPayloadLayout,
        )
        .unwrap();
}

#[test]
fn unix_direct_put_snapshot_response_rejects_mismatched_auth_etag() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-validate");
    let key = crate::tests::object_key("direct-put-snapshot-validate-key");
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: Some("unexpected-etag".to_string()),
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: None,
        stale_payload: None,
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_mismatched_stale_payload_identity() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-stale");
    let key = crate::tests::object_key("direct-put-snapshot-stale-key");
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: None,
        stale_payload: Some(ObjectPayloadReclaimCommand::Segments(
            ObjectSegmentsReclaimRecord {
                bucket: crate::tests::bucket_name("wrong-direct-put-snapshot-stale"),
                key: key.clone(),
                generation_id: GenerationId::new(9).unwrap(),
                created_at: 0,
                segments: Vec::new(),
            },
        )),
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_stale_payload_without_source() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-stale-shape");
    let key = crate::tests::object_key("direct-put-snapshot-stale-shape-key");
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: None,
        stale_payload: Some(ObjectPayloadReclaimCommand::Segments(
            ObjectSegmentsReclaimRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                generation_id: GenerationId::new(9).unwrap(),
                created_at: 0,
                segments: Vec::new(),
            },
        )),
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_stale_source_without_payload() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-source-only");
    let key = crate::tests::object_key("direct-put-snapshot-source-only-key");
    let generation_id = GenerationId::new(9).unwrap();
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: Some(test_live_stored_object(
            bucket.clone(),
            key.clone(),
            generation_id,
            ObjectLayout::Standard,
        )),
        stale_payload: None,
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_stale_payload_delete_marker_source() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-delete-marker-source");
    let key = crate::tests::object_key("direct-put-snapshot-delete-marker-source-key");
    let generation_id = GenerationId::new(9).unwrap();
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: Some(test_delete_marker_stored_object(
            bucket.clone(),
            key.clone(),
        )),
        stale_payload: Some(test_segments_reclaim(
            bucket.clone(),
            key.clone(),
            generation_id,
        )),
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_stale_payload_generation_mismatch() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-generation");
    let key = crate::tests::object_key("direct-put-snapshot-generation-key");
    let source_generation = GenerationId::new(9).unwrap();
    let reclaim_generation = GenerationId::new(10).unwrap();
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: Some(test_live_stored_object(
            bucket.clone(),
            key.clone(),
            source_generation,
            ObjectLayout::Standard,
        )),
        stale_payload: Some(test_segments_reclaim(
            bucket.clone(),
            key.clone(),
            reclaim_generation,
        )),
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_numbered_stale_source() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-numbered-source");
    let key = crate::tests::object_key("direct-put-snapshot-numbered-source-key");
    let generation_id = GenerationId::new(9).unwrap();
    let mut source = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        generation_id,
        ObjectLayout::Standard,
    );
    let StoredObject::Live(source_record) = &mut source else {
        unreachable!("test helper always builds a live object");
    };
    source_record.version_id = VersionId::from_u64(2);
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: Some(source),
        stale_payload: Some(test_segments_reclaim(
            bucket.clone(),
            key.clone(),
            generation_id,
        )),
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_rejects_stale_payload_layout_mismatch() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-layout");
    let key = crate::tests::object_key("direct-put-snapshot-layout-key");
    let generation_id = GenerationId::new(9).unwrap();
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: None,
        },
        current: None,
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: Some(test_live_stored_object(
            bucket.clone(),
            key.clone(),
            generation_id,
            test_multipart_layout(),
        )),
        stale_payload: Some(test_segments_reclaim(
            bucket.clone(),
            key.clone(),
            generation_id,
        )),
    };

    assert_direct_put_snapshot_rejected(&snapshot, &bucket, &key);
}

#[test]
fn unix_direct_put_snapshot_response_accepts_stale_null_source_under_numbered_current() {
    let bucket = crate::tests::bucket_name("direct-put-snapshot-null-source");
    let key = crate::tests::object_key("direct-put-snapshot-null-source-key");
    let current_generation = GenerationId::new(10).unwrap();
    let stale_generation = GenerationId::new(9).unwrap();
    let mut current = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        current_generation,
        ObjectLayout::Standard,
    );
    let StoredObject::Live(current_record) = &mut current else {
        unreachable!("test helper always builds a live object");
    };
    current_record.version_id = VersionId::from_u64(2);
    let expected_etag = current_record.etag.format();
    let snapshot = DirectPutCommitStorageSnapshot {
        auth_snapshot: crate::DirectPutCommitSnapshot {
            existing_etag: Some(expected_etag),
        },
        current: Some(current),
        committed_segments: None,
        committed_stale_generation_id: None,
        stale_payload_source: Some(test_live_stored_object(
            bucket.clone(),
            key.clone(),
            stale_generation,
            ObjectLayout::Standard,
        )),
        stale_payload: Some(test_segments_reclaim(
            bucket.clone(),
            key.clone(),
            stale_generation,
        )),
    };

    assert_direct_put_snapshot_accepted(&snapshot, &bucket, &key);
}

#[test]
fn unix_delete_specific_command_response_rejects_wrong_reclaim_target() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("delete-specific-target-rpc");
    let key = crate::tests::object_key("delete-specific-target-rpc-key");
    let generation_id = GenerationId::new(9).unwrap();
    let stored = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        generation_id,
        ObjectLayout::Standard,
    );
    let expected_target = DeleteObjectVersionTarget::Live {
        generation_id,
        layout: ObjectLayout::Standard,
        payload: test_segments_reclaim(bucket.clone(), key.clone(), generation_id),
    };
    let bad_generation_id = GenerationId::new(10).unwrap();
    let bad_target = DeleteObjectVersionTarget::Live {
        generation_id: bad_generation_id,
        layout: ObjectLayout::Standard,
        payload: test_segments_reclaim(bucket.clone(), key.clone(), bad_generation_id),
    };
    let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
            bucket_write_reservation: proof.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            target: bad_target,
        })),
    );

    let err = client
        .validate_delete_specific_object_command_response(
            &command,
            &BuildDeleteSpecificObjectVersionCommandReq {
                pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                version_id: VersionId::Null,
                expected_stored: Some(&stored),
                expected_target: Some(&expected_target),
                expected_version_list: None,
                bucket_write_reservation: &proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate delete-specific object command build response",
            ..
        })
    ));
}

#[test]
fn unix_insert_delete_marker_response_rejects_wrong_snapshot_stale_payload() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );
    let bucket = crate::tests::bucket_name("insert-marker-stale-rpc");
    let key = crate::tests::object_key("insert-marker-stale-rpc-key");
    let generation_id = GenerationId::new(9).unwrap();
    let stored = test_live_stored_object(
        bucket.clone(),
        key.clone(),
        generation_id,
        ObjectLayout::Standard,
    );
    let proof = test_bucket_write_reservation_proof(bucket.clone(), &key);
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
            bucket_write_reservation: proof.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            version_id: VersionId::Null,
            owner: OwnerIdentity::from_principal("owner"),
            write_sequence: 1,
            last_modified_millis: 1,
            stale_payload: Some(test_segments_reclaim(
                crate::tests::bucket_name("wrong-insert-marker-stale-rpc"),
                key.clone(),
                generation_id,
            )),
        }),
    );
    let owner = OwnerIdentity::from_principal("owner");

    let err = client
        .validate_insert_delete_marker_command_response(
            &command,
            &BuildInsertDeleteMarkerCommandReq {
                pg_id: ObjectMetadataPgId::new_for_test(PgId::new(0)),
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                bucket: &bucket,
                key: &key,
                expected_current: Some(&stored),
                version_id: VersionId::Null,
                owner: &owner,
                stale_payload: InsertDeleteMarkerStalePayload::SnapshotCurrentNullLive {
                    created_at: 1,
                },
                expected_stale_payload_source: Some(&stored),
                bucket_write_reservation: &proof,
            },
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ObjectPgActionError::Store(StoreError::StorageRpc {
            operation: "validate insert-delete-marker command build response",
            ..
        })
    ));
}

#[test]
fn unix_proof_release_response_rejects_non_empty_payload() {
    let tmp = test_util::tempdir();
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        tmp.path().join("unused.sock"),
    );

    client.validate_proof_release_response(&[]).unwrap();
    let err = client
        .validate_proof_release_response(b"unexpected")
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "decode proof release response",
            ..
        })
    ));
}

#[test]
fn unix_bucket_write_reservation_client_acquires_validates_and_releases() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-write-reservation-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
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
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..5)
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

    let lease_deadline = crate::clock::current_time_millis().saturating_add(60_000);
    let record = BucketWriteReservationNodeClient::acquire_durable_bucket_write_reservation(
        &client,
        bucket_pg_id_for_test(0),
        crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
            name: &bucket,
            reservation_id: "reservation-remote-1",
            owner_token: "owner-token-remote-1",
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            operation_kind: "put-object",
            created_at: 10,
            lease_deadline,
            target_context: Some("key=a"),
        },
    )
    .unwrap();
    assert_eq!(record.bucket, bucket);
    assert_eq!(record.reservation_id, "reservation-remote-1");

    BucketWriteReservationNodeClient::validate_bucket_write_reservation_proof(
        &client,
        bucket_pg_id_for_test(0),
        &BucketWriteReservationProof::from(&record),
    )
    .unwrap();
    let mut conflicting_proof = BucketWriteReservationProof::from(&record);
    conflicting_proof.owner_token = "wrong-owner-token".to_string();
    let conflict = BucketWriteReservationNodeClient::validate_bucket_write_reservation_proof(
        &client,
        bucket_pg_id_for_test(0),
        &conflicting_proof,
    )
    .unwrap_err();
    assert!(matches!(
        conflict,
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteReservationConflict {
            reservation_id
        }) if reservation_id == record.reservation_id
    ));
    let renewed_deadline = lease_deadline.saturating_add(60_000);
    let renewed = BucketWriteReservationNodeClient::heartbeat_durable_bucket_write_reservation_with_effect_fence(
        &client,
        bucket_pg_id_for_test(0),
        &BucketWriteReservationProof::from(&record),
        renewed_deadline,
        crate::types::AdmittedRouteEffectFence::bounded(
            ClusterEpoch::new(1).unwrap(),
            crate::clock::current_time_millis().saturating_add(120_000),
            crate::clock::monotonic_time_millis().saturating_add(119_000),
        ),
    )
    .unwrap();
    assert_eq!(renewed.lease_deadline, renewed_deadline);
    BucketWriteReservationNodeClient::release_durable_bucket_write_reservation(
        &client,
        bucket_pg_id_for_test(0),
        &renewed,
    )
    .unwrap();
    for thread in server_threads {
        thread.join().unwrap();
    }

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(PgMetadataStore::durable_bucket_write_reservation(
        &*pg,
        &bucket,
        "reservation-remote-1",
    )
    .unwrap()
    .is_none());
}

#[test]
fn unix_bucket_write_reservation_identity_uses_current_route_after_epoch_change() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    let acquire_epoch = config.cluster_epoch;
    let route_epoch = ClusterEpoch::new(acquire_epoch.get() + 1).unwrap();
    config.cluster_epoch = route_epoch;
    config.pg_routes[0].cluster_epoch = route_epoch;
    let bucket = crate::tests::bucket_name("bucket-write-reservation-old-epoch-release-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let lease_deadline = crate::clock::current_time_millis().saturating_add(60_000);
    let record = {
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
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        let record = PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-old-epoch-release",
                owner_token: "owner-token-old-epoch-release",
                cluster_epoch: acquire_epoch,
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline,
                target_context: Some("key=a"),
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        record
    };

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..3)
        .map(|_| {
            let server = Arc::clone(&server);
            thread::spawn(move || server.accept_one().unwrap())
        })
        .collect();
    let client =
        UnixStorageNodeClient::new(config.node_id, route_epoch, config.socket_path.clone());

    let proof = BucketWriteReservationProof::from(&record);
    BucketWriteReservationNodeClient::validate_bucket_write_reservation_proof(
        &client,
        bucket_pg_id_for_test(0),
        &proof,
    )
    .unwrap();
    let renewed = BucketWriteReservationNodeClient::heartbeat_durable_bucket_write_reservation_with_effect_fence(
        &client,
        bucket_pg_id_for_test(0),
        &proof,
        lease_deadline.saturating_add(60_000),
        crate::types::AdmittedRouteEffectFence::bounded(
            route_epoch,
            crate::clock::current_time_millis().saturating_add(120_000),
            crate::clock::monotonic_time_millis().saturating_add(119_000),
        ),
    )
    .unwrap();
    BucketWriteReservationNodeClient::release_durable_bucket_write_reservation(
        &client,
        bucket_pg_id_for_test(0),
        &renewed,
    )
    .unwrap();
    for thread in server_threads {
        thread.join().unwrap();
    }

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(PgMetadataStore::durable_bucket_write_reservation(
        &*pg,
        &bucket,
        &record.reservation_id,
    )
    .unwrap()
    .is_none());
}

#[test]
fn unix_bucket_write_reservation_client_preserves_draining_signal() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-write-reservation-draining-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
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
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::begin_durable_bucket_write_drain(
            &*pg,
            &bucket,
            "drain-remote-1",
            "drain-owner-remote-1",
            ClusterEpoch::new(1).unwrap(),
            10,
            20,
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let err = BucketWriteReservationNodeClient::acquire_durable_bucket_write_reservation(
        &client,
        bucket_pg_id_for_test(0),
        crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
            name: &bucket,
            reservation_id: "reservation-remote-1",
            owner_token: "owner-token-remote-1",
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            operation_kind: "put-object",
            created_at: 30,
            lease_deadline: 40,
            target_context: Some("key=a"),
        },
    )
    .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteDraining)
    ));
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_write_reservation_client_preserves_bucket_not_found() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-write-reservation-missing-rpc");
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let err = BucketWriteReservationNodeClient::acquire_durable_bucket_write_reservation(
        &client,
        bucket_pg_id_for_test(0),
        crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
            name: &bucket,
            reservation_id: "reservation-remote-1",
            owner_token: "owner-token-remote-1",
            cluster_epoch: ClusterEpoch::new(1).unwrap(),
            operation_kind: "put-object",
            created_at: 30,
            lease_deadline: 40,
            target_context: Some("key=a"),
        },
    )
    .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name })
            if name == bucket
    ));
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_write_reservation_client_routes_drain_and_finalize_coordination() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-drain-finalize-rpc");
    let finalize_bucket = crate::tests::bucket_name("bucket-finalize-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let finalize_bucket_incarnation_generation = {
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
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-for-drain-list",
                owner_token: "reservation-owner-for-drain-list",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline: 20,
                target_context: Some("key=a"),
            },
        )
        .unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &finalize_bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::mark_bucket_deleting(&*pg, &finalize_bucket).unwrap();
        let generation = PgMetadataStore::head_bucket_raw(&*pg, &finalize_bucket)
            .unwrap()
            .bucket_incarnation_generation;
        pg.refresh_metadata_command_state_digest().unwrap();
        generation
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..22)
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

    let reservations = BucketWriteReservationNodeClient::durable_bucket_write_reservations(
        &client,
        bucket_pg_id_for_test(0),
        &bucket,
    )
    .unwrap();
    assert_eq!(reservations.len(), 1);
    assert_eq!(reservations[0].reservation_id, "reservation-for-drain-list");

    let drain = BucketWriteReservationNodeClient::begin_durable_bucket_write_drain(
        &client,
        bucket_pg_id_for_test(0),
        &bucket,
        "drain-rpc-1",
        "drain-owner-rpc-1",
        ClusterEpoch::new(1).unwrap(),
        30,
        40,
    )
    .unwrap();
    assert_eq!(drain.bucket, bucket);
    assert_eq!(drain.drain_id, "drain-rpc-1");
    assert!(
        BucketWriteReservationNodeClient::durable_bucket_write_drain_exists(
            &client,
            bucket_pg_id_for_test(0),
            &bucket,
        )
        .unwrap()
    );
    assert_eq!(
        BucketWriteReservationNodeClient::durable_bucket_write_drain(
            &client,
            bucket_pg_id_for_test(0),
            &bucket,
        )
        .unwrap()
        .as_ref()
        .map(|record| record.drain_id.as_str()),
        Some("drain-rpc-1")
    );
    BucketWriteReservationNodeClient::clear_durable_bucket_write_drain(
        &client,
        bucket_pg_id_for_test(0),
        &drain,
    )
    .unwrap();
    assert!(
        !BucketWriteReservationNodeClient::durable_bucket_write_drain_exists(
            &client,
            bucket_pg_id_for_test(0),
            &bucket,
        )
        .unwrap()
    );
    assert!(
        BucketWriteReservationNodeClient::durable_bucket_write_drain(
            &client,
            bucket_pg_id_for_test(0),
            &bucket,
        )
        .unwrap()
        .is_none()
    );

    BucketWriteReservationNodeClient::begin_durable_bucket_write_drain(
        &client,
        bucket_pg_id_for_test(0),
        &bucket,
        "expired-drain-rpc-1",
        "expired-drain-owner-rpc-1",
        ClusterEpoch::new(1).unwrap(),
        50,
        55,
    )
    .unwrap();
    let expired = BucketWriteReservationNodeClient::clear_expired_durable_bucket_write_drain(
        &client,
        bucket_pg_id_for_test(0),
        &bucket,
        60,
    )
    .unwrap()
    .expect("expired drain should clear");
    assert_eq!(expired.drain_id, "expired-drain-rpc-1");

    let live_begin_drain = BucketWriteReservationNodeClient::begin_durable_bucket_write_drain(
        &client,
        bucket_pg_id_for_test(0),
        &bucket,
        "active-begin-drain-rpc-1",
        "active-begin-drain-owner-rpc-1",
        ClusterEpoch::new(1).unwrap(),
        61,
        120,
    )
    .unwrap();
    let begin_roots = BucketWriteReservationNodeClient::get_bucket_delete_begin_roots(
        &client,
        bucket_pg_id_for_test(0),
        90,
        None,
        16,
    )
    .unwrap();
    assert!(begin_roots.is_empty());
    BucketWriteReservationNodeClient::clear_durable_bucket_write_drain(
        &client,
        bucket_pg_id_for_test(0),
        &live_begin_drain,
    )
    .unwrap();

    let expired_begin_drain = BucketWriteReservationNodeClient::begin_durable_bucket_write_drain(
        &client,
        bucket_pg_id_for_test(0),
        &bucket,
        "expired-begin-drain-rpc-1",
        "expired-begin-drain-owner-rpc-1",
        ClusterEpoch::new(1).unwrap(),
        61,
        80,
    )
    .unwrap();
    let begin_roots = BucketWriteReservationNodeClient::get_bucket_delete_begin_roots(
        &client,
        bucket_pg_id_for_test(0),
        90,
        None,
        16,
    )
    .unwrap();
    assert_eq!(begin_roots.len(), 1);
    assert_eq!(begin_roots[0].bucket, bucket);
    BucketWriteReservationNodeClient::clear_durable_bucket_write_drain(
        &client,
        bucket_pg_id_for_test(0),
        &expired_begin_drain,
    )
    .unwrap();

    let claim = BucketWriteReservationNodeClient::acquire_bucket_delete_finalize_claim(
        &client,
        bucket_pg_id_for_test(0),
        &finalize_bucket,
        finalize_bucket_incarnation_generation,
        "finalize-claim-rpc-1",
        "finalize-claim-owner-rpc-1",
        ClusterEpoch::new(1).unwrap(),
        70,
        Some(80),
        70,
    )
    .unwrap()
    .expect("finalize claim should acquire");
    assert_eq!(claim.bucket, finalize_bucket);
    assert_eq!(claim.claim_id, "finalize-claim-rpc-1");
    let replacement_claim = BucketWriteReservationNodeClient::acquire_bucket_delete_finalize_claim(
        &client,
        bucket_pg_id_for_test(0),
        &finalize_bucket,
        finalize_bucket_incarnation_generation,
        "finalize-claim-rpc-2",
        "finalize-claim-owner-rpc-2",
        ClusterEpoch::new(1).unwrap(),
        81,
        Some(100),
        81,
    )
    .unwrap()
    .expect("expired finalizer claim should be stealable");
    let observed_claim = BucketWriteReservationNodeClient::bucket_delete_finalize_claim(
        &client,
        bucket_pg_id_for_test(0),
        &finalize_bucket,
    )
    .unwrap()
    .expect("finalize claim read should return current claim");
    assert_eq!(observed_claim.bucket, finalize_bucket);
    assert_eq!(observed_claim.claim_id, replacement_claim.claim_id);
    assert_eq!(
        observed_claim.bucket_incarnation_generation,
        finalize_bucket_incarnation_generation
    );
    let stale_release = BucketWriteReservationNodeClient::release_bucket_delete_finalize_claim(
        &client,
        bucket_pg_id_for_test(0),
        &claim,
    )
    .unwrap_err();
    assert!(matches!(
        stale_release,
        BucketSnapshotLoadError::Metadata(MetadataError::ReclaimClaimConflict { .. })
    ));
    BucketWriteReservationNodeClient::release_bucket_delete_finalize_claim(
        &client,
        bucket_pg_id_for_test(0),
        &replacement_claim,
    )
    .unwrap();
    assert!(
        BucketWriteReservationNodeClient::bucket_delete_finalize_claim(
            &client,
            bucket_pg_id_for_test(0),
            &finalize_bucket,
        )
        .unwrap()
        .is_none()
    );

    let roots = BucketWriteReservationNodeClient::get_bucket_delete_finalize_roots(
        &client,
        bucket_pg_id_for_test(0),
        90,
        16,
    )
    .unwrap();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].bucket, finalize_bucket);

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_bucket_write_reservation_client_routes_lifecycle_sweep_coordination() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-lifecycle-sweep-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let bucket_incarnation_generation = {
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
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::put_bucket_subresource(
            &*pg,
            &bucket,
            crate::types::PutBucketSubresource {
                kind: BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: crate::types::BucketSubresourceAux::None,
            },
        )
        .unwrap();
        let generation = PgMetadataStore::head_bucket_raw(&*pg, &bucket)
            .unwrap()
            .bucket_incarnation_generation;
        pg.refresh_metadata_command_state_digest().unwrap();
        generation
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    // One connection for each lifecycle RPC below: list, roots, acquire,
    // heartbeat, record-error, and release.
    let server_threads: Vec<_> = (0..6)
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

    let buckets = BucketWriteReservationNodeClient::list_lifecycle_sweep_buckets(
        &client,
        bucket_pg_id_for_test(0),
    )
    .unwrap();
    assert_eq!(buckets.lifecycle_buckets.len(), 1);
    assert_eq!(buckets.lifecycle_buckets[0].name, bucket);
    assert!(buckets.aborting_buckets.is_empty());

    let roots = BucketWriteReservationNodeClient::get_lifecycle_sweep_roots(
        &client,
        bucket_pg_id_for_test(0),
        10,
        16,
    )
    .unwrap();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0].bucket, bucket);
    assert_eq!(
        roots[0].source,
        crate::types::LifecycleSweepRootSource::LifecycleConfig
    );

    let claim = BucketWriteReservationNodeClient::acquire_lifecycle_sweep_claim(
        &client,
        bucket_pg_id_for_test(0),
        &bucket,
        bucket_incarnation_generation,
        "lifecycle-claim-rpc-1",
        "lifecycle-owner-rpc-1",
        ClusterEpoch::new(1).unwrap(),
        20,
        Some(40),
        20,
    )
    .unwrap()
    .expect("lifecycle claim should acquire");
    assert_eq!(claim.bucket, bucket);
    assert_eq!(claim.claim_id, "lifecycle-claim-rpc-1");

    let heartbeat = BucketWriteReservationNodeClient::heartbeat_lifecycle_sweep_claim(
        &client,
        bucket_pg_id_for_test(0),
        &claim,
        30,
        Some(50),
    )
    .unwrap();
    assert_eq!(heartbeat.heartbeat_at, 30);
    assert_eq!(heartbeat.lease_deadline, Some(50));

    let error_record = BucketWriteReservationNodeClient::record_lifecycle_sweep_claim_error(
        &client,
        bucket_pg_id_for_test(0),
        &heartbeat,
        "transient lifecycle error",
    )
    .unwrap();
    assert_eq!(
        error_record.last_error.as_deref(),
        Some("transient lifecycle error")
    );

    BucketWriteReservationNodeClient::release_lifecycle_sweep_claim(
        &client,
        bucket_pg_id_for_test(0),
        &error_record,
    )
    .unwrap();

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_bucket_metadata_client_loads_bucket_snapshot() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-snapshot-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
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
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::put_bucket_subresource(
            &*pg,
            &bucket,
            crate::types::PutBucketSubresource {
                kind: BucketSubresourceKind::Policy,
                body: "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
                aux: crate::types::BucketSubresourceAux::policy(false),
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let request = BucketSnapshotRequest {
        policy: true,
        tags: BucketSnapshotTagsRequest::Always,
        lifecycle: false,
        cors: true,
    };
    let snapshot = BucketMetadataNodeClient::load_bucket_snapshot(
        &client,
        BucketPgId::new_for_test(PgId::new(0)),
        &bucket,
        request,
    )
    .unwrap();
    assert_eq!(snapshot.bucket.name, bucket);
    assert_eq!(snapshot.request, request);
    assert_eq!(
        snapshot.policy,
        crate::types::LoadedBucketSubresource::Loaded(
            "{\"Version\":\"2012-10-17\",\"Statement\":[]}".to_string()
        )
    );
    assert_eq!(
        snapshot.tags,
        crate::types::LoadedBucketSubresource::Missing
    );
    assert_eq!(
        snapshot.lifecycle,
        crate::types::LoadedBucketSubresource::NotRequested
    );
    assert_eq!(
        snapshot.cors,
        crate::types::LoadedBucketSubresource::Missing
    );
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_metadata_client_loads_bucket_snapshot_pair() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let source_bucket = crate::tests::bucket_name("bucket-snapshot-pair-source-rpc");
    let destination_bucket = crate::tests::bucket_name("bucket-snapshot-pair-dest-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        for bucket in [&source_bucket, &destination_bucket] {
            PgMetadataStore::create_bucket(
                &*pg,
                bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
        }
        PgMetadataStore::put_bucket_subresource(
            &*pg,
            &source_bucket,
            crate::types::PutBucketSubresource {
                kind: BucketSubresourceKind::Tagging,
                body: "<Tagging/>",
                aux: crate::types::BucketSubresourceAux::None,
            },
        )
        .unwrap();
        PgMetadataStore::put_bucket_subresource(
            &*pg,
            &destination_bucket,
            crate::types::PutBucketSubresource {
                kind: BucketSubresourceKind::Cors,
                body: "<CORSConfiguration/>",
                aux: crate::types::BucketSubresourceAux::None,
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let pair = BucketMetadataNodeClient::load_bucket_snapshot_pair(
        &client,
        BucketPgId::new_for_test(PgId::new(0)),
        (
            &source_bucket,
            BucketSnapshotRequest {
                tags: BucketSnapshotTagsRequest::Always,
                ..Default::default()
            },
        ),
        BucketPgId::new_for_test(PgId::new(0)),
        (
            &destination_bucket,
            BucketSnapshotRequest {
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
        crate::types::LoadedBucketSubresource::Loaded("<Tagging/>".to_string())
    );
    assert_eq!(
        pair.destination().cors,
        crate::types::LoadedBucketSubresource::Loaded("<CORSConfiguration/>".to_string())
    );
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_metadata_client_builds_multipart_completion_barrier_command() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("completed-order-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let bucket_write_reservation;
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
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        let reservation = PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "completed-order-reservation",
                owner_token: "completed-order-owner",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                created_at: 1_000,
                lease_deadline: crate::clock::current_time_millis() + 60_000,
                target_context: Some("object-key"),
            },
        )
        .unwrap();
        bucket_write_reservation = BucketWriteReservationProof::from(&reservation);
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );
    let command_id = MetadataCommandId::new(
        ClusterEpoch::new(1).unwrap(),
        PgId::new(0),
        MetadataCommandLogIndex::new(1).unwrap(),
    );

    let (barrier_sequence, command) =
        BucketMetadataNodeClient::build_advance_multipart_completion_barrier_command(
            &client,
            BucketPgId::new_for_test(PgId::new(0)),
            &bucket,
            command_id,
            "object-key",
            &bucket_write_reservation,
        )
        .unwrap();

    assert_eq!(barrier_sequence, 1);
    assert_eq!(command.id(), command_id);
    match command.payload() {
        MetadataCommandPayload::AdvanceMultipartCompletionBarrier(advance) => {
            assert_eq!(advance.bucket, bucket);
            assert_eq!(advance.barrier_sequence, barrier_sequence);
        }
        other => panic!("unexpected command payload: {other:?}"),
    }
    server_thread.join().unwrap();
}

#[test]
fn unix_bucket_metadata_client_routes_bucket_control_operations() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("bucket-control-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
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
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::put_bucket_subresource(
            &*pg,
            &bucket,
            crate::types::PutBucketSubresource {
                kind: BucketSubresourceKind::Policy,
                body: "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
                aux: crate::types::BucketSubresourceAux::policy(false),
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
    }
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..6)
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
    let command_id = |log_index| {
        MetadataCommandId::new(
            ClusterEpoch::new(1).unwrap(),
            PgId::new(0),
            MetadataCommandLogIndex::new(log_index).unwrap(),
        )
    };

    let versioning = BucketMetadataNodeClient::build_put_bucket_versioning_command(
        &client,
        BucketPgId::new_for_test(PgId::new(0)),
        &bucket,
        command_id(1),
        BucketVersioningState::Enabled,
    )
    .unwrap();
    let MetadataCommandPayload::PutBucketVersioning(versioning_command) = versioning.payload()
    else {
        panic!("unexpected versioning command payload");
    };
    assert!(
        BucketMetadataNodeClient::pending_put_bucket_versioning_command_matches_current(
            &client,
            BucketPgId::new_for_test(PgId::new(0)),
            &bucket,
            versioning_command,
            BucketVersioningState::Enabled,
        )
        .unwrap()
    );

    let acl = BucketMetadataNodeClient::build_put_bucket_acl_command(
        &client,
        BucketPgId::new_for_test(PgId::new(0)),
        &bucket,
        command_id(2),
        &crate::AclGrants::default(),
        crate::BucketAclSummary {
            public_read: true,
            public_write: false,
        },
    )
    .unwrap();
    match acl.payload() {
        MetadataCommandPayload::PutBucketAcl(command) => {
            assert_eq!(command.bucket.name, bucket);
            assert!(command.bucket.public_read);
            assert!(!command.bucket.public_write);
        }
        other => panic!("unexpected ACL command payload: {other:?}"),
    }

    let property = BucketMetadataNodeClient::build_put_bucket_property_command(
        &client,
        BucketPgId::new_for_test(PgId::new(0)),
        &bucket,
        command_id(3),
        &BucketPropertyMutation::AbacEnabled(true),
    )
    .unwrap();
    match property.payload() {
        MetadataCommandPayload::PutBucketProperty(command) => {
            assert_eq!(command.bucket.name, bucket);
            assert!(command.bucket.bucket_abac_enabled);
        }
        other => panic!("unexpected property command payload: {other:?}"),
    }

    let subresource = BucketSubresourceMutation::Put {
        kind: BucketSubresourceKind::Lifecycle,
        body: "<LifecycleConfiguration/>".to_string(),
        aux: crate::types::BucketSubresourceAux::None,
    };
    let subresource_command = BucketMetadataNodeClient::build_put_bucket_subresource_command(
        &client,
        BucketPgId::new_for_test(PgId::new(0)),
        &bucket,
        command_id(4),
        &subresource,
    )
    .unwrap();
    match subresource_command.payload() {
        MetadataCommandPayload::PutBucketSubresource(command) => {
            assert!(command.matches_mutation(&bucket, &subresource));
        }
        other => panic!("unexpected subresource command payload: {other:?}"),
    }

    let policy = BucketMetadataNodeClient::get_bucket_subresource(
        &client,
        BucketPgId::new_for_test(PgId::new(0)),
        &bucket,
        BucketSubresourceKind::Policy,
    )
    .unwrap();
    assert_eq!(
        policy.as_deref(),
        Some("{\"Version\":\"2012-10-17\",\"Statement\":[]}")
    );

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_bucket_metadata_client_releases_bucket_write_proof_after_route_expiry() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("proof-release-rpc-bucket");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let reservation = {
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
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        let reservation = PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-1",
                owner_token: "owner-token-1",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline: 20,
                target_context: Some("key=a"),
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        reservation
    };
    config.route_map_validity = crate::RouteMapValidity::until_ms(1).unwrap();
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    client
        .release_metadata_command_bucket_write_reservation(
            bucket_pg_id_for_test(0),
            &BucketWriteReservationProof::from(&reservation),
        )
        .unwrap();
    server_thread.join().unwrap();

    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    client
        .release_metadata_command_bucket_write_reservation(
            bucket_pg_id_for_test(0),
            &BucketWriteReservationProof::from(&reservation),
        )
        .unwrap();
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(
        PgMetadataStore::durable_bucket_write_reservation(&*pg, &bucket, "reservation-1")
            .unwrap()
            .is_none()
    );
}

#[test]
fn unix_bucket_write_reservation_client_clears_exact_drain_after_route_expiry() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("drain-clear-expired-route-rpc-bucket");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let drain = {
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
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        let drain = PgMetadataStore::begin_durable_bucket_write_drain(
            &*pg,
            &bucket,
            "expired-route-drain",
            "expired-route-drain-owner",
            config.cluster_epoch,
            10,
            20,
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        drain
    };
    config.route_map_validity = crate::RouteMapValidity::until_ms(1).unwrap();
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    BucketWriteReservationNodeClient::clear_durable_bucket_write_drain(
        &client,
        bucket_pg_id_for_test(0),
        &drain,
    )
    .unwrap();
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(
        PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket)
            .unwrap()
            .is_none(),
        "retained drain clear must remove the exact drain after active route expiry"
    );
}

#[test]
fn unix_bucket_write_reservation_client_releases_finalizer_claim_after_route_expiry() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("finalizer-claim-release-expired-route-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let claim = {
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
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::mark_bucket_deleting(&*pg, &bucket).unwrap();
        let generation = PgMetadataStore::head_bucket_raw(&*pg, &bucket)
            .unwrap()
            .bucket_incarnation_generation;
        let claim = PgMetadataStore::acquire_bucket_delete_finalize_claim(
            &*pg,
            &bucket,
            generation,
            "expired-route-finalizer-claim",
            "expired-route-finalizer-owner",
            config.cluster_epoch,
            10,
            Some(20),
            10,
        )
        .unwrap()
        .expect("finalizer claim should be acquired");
        pg.refresh_metadata_command_state_digest().unwrap();
        claim
    };
    config.route_map_validity = crate::RouteMapValidity::until_ms(1).unwrap();
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    BucketWriteReservationNodeClient::release_bucket_delete_finalize_claim(
        &client,
        bucket_pg_id_for_test(0),
        &claim,
    )
    .unwrap();
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(
        PgMetadataStore::bucket_delete_finalize_claim(&*pg)
            .unwrap()
            .is_none(),
        "retained finalizer-claim release must work after active route expiry"
    );
}

#[test]
fn unix_bucket_write_reservation_client_releases_lifecycle_claim_after_route_expiry() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("lifecycle-claim-release-expired-route-rpc");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let (bucket_incarnation_generation, claim) = {
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
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::put_bucket_subresource(
            &*pg,
            &bucket,
            crate::types::PutBucketSubresource {
                kind: BucketSubresourceKind::Lifecycle,
                body: "<LifecycleConfiguration/>",
                aux: crate::types::BucketSubresourceAux::None,
            },
        )
        .unwrap();
        let generation = PgMetadataStore::head_bucket_raw(&*pg, &bucket)
            .unwrap()
            .bucket_incarnation_generation;
        let claim = PgMetadataStore::acquire_lifecycle_sweep_claim(
            &*pg,
            &bucket,
            generation,
            "expired-route-lifecycle-claim",
            "expired-route-lifecycle-owner",
            config.cluster_epoch,
            10,
            None,
            10,
        )
        .unwrap()
        .expect("lifecycle claim should be acquired");
        pg.refresh_metadata_command_state_digest().unwrap();
        (generation, claim)
    };
    config.route_map_validity = crate::RouteMapValidity::until_ms(1).unwrap();
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        config.node_id,
        config.cluster_epoch,
        config.socket_path.clone(),
    );

    BucketWriteReservationNodeClient::release_lifecycle_sweep_claim(
        &client,
        bucket_pg_id_for_test(0),
        &claim,
    )
    .unwrap();
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    let replacement = PgMetadataStore::acquire_lifecycle_sweep_claim(
        &*pg,
        &bucket,
        bucket_incarnation_generation,
        "post-release-lifecycle-claim",
        "post-release-lifecycle-owner",
        config.cluster_epoch,
        20,
        None,
        20,
    )
    .unwrap()
    .expect("retained lifecycle-claim release must work after active route expiry");
    PgMetadataStore::release_lifecycle_sweep_claim(
        &*pg,
        &replacement.bucket,
        replacement.bucket_incarnation_generation,
        &replacement.claim_id,
        &replacement.owner_token,
        replacement.cluster_epoch,
    )
    .unwrap();
}

#[test]
fn unix_bucket_metadata_client_preserves_proof_release_conflict() {
    let tmp = test_util::tempdir();
    let config = test_config(&tmp);
    let bucket = crate::tests::bucket_name("proof-release-conflict-bucket");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let reservation = {
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
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        let reservation = PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-1",
                owner_token: "owner-token-1",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline: 20,
                target_context: Some("key=a"),
            },
        )
        .unwrap();
        pg.refresh_metadata_command_state_digest().unwrap();
        reservation
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let mut proof = BucketWriteReservationProof::from(&reservation);
    proof.owner_token = "wrong-owner-token".to_string();
    let err = client
        .release_metadata_command_bucket_write_reservation(bucket_pg_id_for_test(0), &proof)
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Metadata(MetadataError::BucketWriteReservationConflict {
            reservation_id
        }) if reservation_id == "reservation-1"
    ));
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(
        PgMetadataStore::durable_bucket_write_reservation(&*pg, &bucket, "reservation-1")
            .unwrap()
            .is_some()
    );
}

#[test]
fn unix_bucket_clients_reject_wrong_bucket_pg_before_bucket_access() {
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
    let owner = crate::CanonicalUserId::from_principal("owner");
    let acl_grants = crate::AclGrants::default();
    let (bucket, correct_pg_id, wrong_pg_id, wrong_pg_release_record) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let (bucket, correct_pg_id) = (0..100)
            .map(|index| crate::tests::bucket_name(format!("bucket-rpc-wrong-pg-{index}")))
            .map(|bucket| {
                let pg_id = node.pg_topology().bucket_pg_for(&bucket);
                (bucket, pg_id)
            })
            .find(|(_, pg_id)| *pg_id < 2)
            .expect("two-PG topology must place a test bucket");
        for pg_id in [correct_pg_id, if correct_pg_id == 0 { 1 } else { 0 }] {
            let pg = node.get_pg(pg_id).unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &acl_grants,
                false,
                false,
            )
            .unwrap();
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
        let wrong_pg = node.get_pg(wrong_pg_id).unwrap();
        let wrong_pg_release_record = PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*wrong_pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "wrong-pg-release-reservation",
                owner_token: "wrong-pg-release-owner",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline: 20,
                target_context: Some("key"),
            },
        )
        .unwrap();
        wrong_pg.refresh_metadata_command_state_digest().unwrap();
        (bucket, correct_pg_id, wrong_pg_id, wrong_pg_release_record)
    };

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    // One connection for each RPC below: head, correct snapshot, wrong
    // snapshot, two pair orderings, create-command, reservation acquire, and
    // reservation release.
    let server_threads: Vec<_> = (0..8)
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
    let wrong_bucket_pg = BucketPgId::new_for_test(PgId::new(wrong_pg_id));

    let head_error =
        BucketMetadataNodeClient::head_bucket_raw(&client, wrong_bucket_pg, &bucket).unwrap_err();
    assert!(matches!(
        head_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let correct_bucket_pg = BucketPgId::new_for_test(PgId::new(correct_pg_id));
    let correct_snapshot = BucketMetadataNodeClient::load_bucket_snapshot(
        &client,
        correct_bucket_pg,
        &bucket,
        crate::BucketSnapshotRequest::default(),
    )
    .unwrap();
    assert_eq!(correct_snapshot.bucket.name, bucket);

    let snapshot_error = BucketMetadataNodeClient::load_bucket_snapshot(
        &client,
        wrong_bucket_pg,
        &bucket,
        crate::BucketSnapshotRequest::default(),
    )
    .unwrap_err();
    assert!(matches!(
        snapshot_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let pair_error = BucketMetadataNodeClient::load_bucket_snapshot_pair(
        &client,
        wrong_bucket_pg,
        (&bucket, crate::BucketSnapshotRequest::default()),
        correct_bucket_pg,
        (&bucket, crate::BucketSnapshotRequest::default()),
    )
    .unwrap_err();
    assert!(matches!(
        pair_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    let destination_pair_error = BucketMetadataNodeClient::load_bucket_snapshot_pair(
        &client,
        correct_bucket_pg,
        (&bucket, crate::BucketSnapshotRequest::default()),
        wrong_bucket_pg,
        (&bucket, crate::BucketSnapshotRequest::default()),
    )
    .unwrap_err();
    assert!(matches!(
        destination_pair_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let create_config = crate::CreateBucketConfig {
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
    let command_id = MetadataCommandId::new(
        ClusterEpoch::new(1).unwrap(),
        PgId::new(wrong_pg_id),
        MetadataCommandLogIndex::new(1).unwrap(),
    );
    let create_error = BucketMetadataNodeClient::build_create_bucket_command(
        &client,
        wrong_bucket_pg,
        &bucket,
        command_id,
        &create_config,
    )
    .unwrap_err();
    assert!(matches!(
        create_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));

    let reservation_error =
        BucketWriteReservationNodeClient::acquire_durable_bucket_write_reservation(
            &client,
            wrong_bucket_pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "wrong-pg-reservation",
                owner_token: "wrong-pg-owner",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline: 20,
                target_context: Some("key"),
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            reservation_error,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            })
        ),
        "unexpected wrong-PG reservation error: {reservation_error:?}"
    );

    let release_error = BucketWriteReservationNodeClient::release_durable_bucket_write_reservation(
        &client,
        wrong_bucket_pg,
        &wrong_pg_release_record,
    )
    .unwrap_err();
    assert!(
        matches!(
            release_error,
            BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                failure: StorageRpcErrorCode::PayloadDecode,
                ..
            })
        ),
        "unexpected wrong-PG reservation release error: {release_error:?}"
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
    for pg_id in [correct_pg_id, wrong_pg_id] {
        let pg = node.get_pg(pg_id).unwrap();
        assert!(
            PgMetadataStore::durable_bucket_write_reservation(
                &*pg,
                &bucket,
                "wrong-pg-reservation",
            )
            .unwrap()
            .is_none(),
            "wrong-PG reservation must not mutate PG {pg_id}"
        );
    }
    let wrong_pg = node.get_pg(wrong_pg_id).unwrap();
    assert_eq!(
        PgMetadataStore::durable_bucket_write_reservation(
            &*wrong_pg,
            &bucket,
            &wrong_pg_release_record.reservation_id,
        )
        .unwrap(),
        Some(wrong_pg_release_record),
        "wrong-PG retained release must not mutate the equivalent durable subject"
    );
}

#[test]
fn unix_bucket_metadata_client_rejects_proof_release_wrong_bucket_pg() {
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
    let owner = crate::CanonicalUserId::from_principal("owner");
    let (bucket, correct_pg_id, wrong_pg_id, reservation) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let (bucket, correct_pg_id, wrong_pg_id) = (0..100)
            .map(|index| crate::tests::bucket_name(format!("proof-release-wrong-pg-{index}")))
            .find_map(|bucket| {
                let correct_pg_id = node.pg_topology().bucket_pg_for(&bucket);
                (correct_pg_id < 2).then(|| {
                    let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
                    (bucket, correct_pg_id, wrong_pg_id)
                })
            })
            .expect("two-PG topology must place a test bucket");
        let mut reservations = Vec::new();
        for pg_id in [correct_pg_id, wrong_pg_id] {
            let pg = node.get_pg(pg_id).unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            reservations.push(
                PgMetadataStore::acquire_durable_bucket_write_reservation(
                    &*pg,
                    crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                        name: &bucket,
                        reservation_id: "reservation-1",
                        owner_token: "owner-token-1",
                        cluster_epoch: ClusterEpoch::new(1).unwrap(),
                        operation_kind: "put-object",
                        created_at: 10,
                        lease_deadline: 20,
                        target_context: Some("key=a"),
                    },
                )
                .unwrap(),
            );
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        assert_eq!(
            reservations[0], reservations[1],
            "wrong-PG proof-release canary must have equivalent durable state"
        );
        let reservation = reservations.remove(0);
        (bucket, correct_pg_id, wrong_pg_id, reservation)
    };
    private_socket_dir(config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(7),
        ClusterEpoch::new(1).unwrap(),
        config.socket_path.clone(),
    );

    let err = client
        .release_metadata_command_bucket_write_reservation(
            bucket_pg_id_for_test(wrong_pg_id),
            &BucketWriteReservationProof::from(&reservation),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "proof release",
            failure: StorageRpcErrorCode::PayloadDecode,
            ..
        })
    ));
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &config.data_dir,
        &config.pg_ids,
        config.default_ec_shape,
    )
    .unwrap();
    for pg_id in [correct_pg_id, wrong_pg_id] {
        let pg = node.get_pg(pg_id).unwrap();
        assert_eq!(
            PgMetadataStore::durable_bucket_write_reservation(&*pg, &bucket, "reservation-1")
                .unwrap(),
            Some(reservation.clone()),
            "wrong-PG proof release must reject before mutating either durable canary"
        );
    }
}

#[test]
fn unix_bucket_write_drain_operations_reject_wrong_bucket_pg_before_access() {
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
    let owner = crate::CanonicalUserId::from_principal("owner");
    let (bucket, correct_pg_id, wrong_pg_id, drain) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let (bucket, correct_pg_id) = (0..100)
            .map(|index| crate::tests::bucket_name(format!("drain-rpc-wrong-pg-{index}")))
            .map(|bucket| {
                let pg_id = node.pg_topology().bucket_pg_for(&bucket);
                (bucket, pg_id)
            })
            .find(|(_, pg_id)| *pg_id < 2)
            .expect("two-PG topology must place a test bucket");
        let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
        let mut drains = Vec::new();
        for pg_id in [correct_pg_id, wrong_pg_id] {
            let pg = node.get_pg(pg_id).unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            drains.push(
                PgMetadataStore::begin_durable_bucket_write_drain(
                    &*pg,
                    &bucket,
                    "wrong-pg-drain",
                    "wrong-pg-drain-owner",
                    config.cluster_epoch,
                    10,
                    80,
                )
                .unwrap(),
            );
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        assert_eq!(
            drains[0], drains[1],
            "wrong-PG drain canary must have equivalent durable state"
        );
        (bucket, correct_pg_id, wrong_pg_id, drains.remove(0))
    };

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..6)
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
    let wrong_pg = bucket_pg_id_for_test(wrong_pg_id);
    let assert_payload_decode = |error: BucketSnapshotLoadError, operation: &str| {
        assert!(
            matches!(
                &error,
                BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                })
            ),
            "wrong-PG {operation} must fail with PayloadDecode, got {error:?}"
        );
    };
    let assert_drains_unchanged = || {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        for pg_id in [correct_pg_id, wrong_pg_id] {
            let pg = node.get_pg(pg_id).unwrap();
            assert_eq!(
                PgMetadataStore::durable_bucket_write_drain(&*pg, &bucket).unwrap(),
                Some(drain.clone()),
                "wrong-PG drain operation must not mutate PG {pg_id}"
            );
        }
    };

    let error = BucketWriteReservationNodeClient::durable_bucket_write_drain_exists(
        &client, wrong_pg, &bucket,
    )
    .unwrap_err();
    assert_payload_decode(error, "drain exists");
    assert_drains_unchanged();

    let error =
        BucketWriteReservationNodeClient::durable_bucket_write_drain(&client, wrong_pg, &bucket)
            .unwrap_err();
    assert_payload_decode(error, "drain get");
    assert_drains_unchanged();

    let error = BucketWriteReservationNodeClient::begin_durable_bucket_write_drain(
        &client,
        wrong_pg,
        &bucket,
        "other-wrong-pg-drain",
        "other-wrong-pg-drain-owner",
        config.cluster_epoch,
        20,
        90,
    )
    .unwrap_err();
    assert_payload_decode(error, "drain begin");
    assert_drains_unchanged();

    let error = BucketWriteReservationNodeClient::heartbeat_durable_bucket_write_drain(
        &client, wrong_pg, &drain, 90,
    )
    .unwrap_err();
    assert_payload_decode(error, "drain heartbeat");
    assert_drains_unchanged();

    let error = BucketWriteReservationNodeClient::clear_expired_durable_bucket_write_drain(
        &client, wrong_pg, &bucket, 100,
    )
    .unwrap_err();
    assert_payload_decode(error, "expired drain clear");
    assert_drains_unchanged();

    let error = BucketWriteReservationNodeClient::clear_durable_bucket_write_drain(
        &client, wrong_pg, &drain,
    )
    .unwrap_err();
    assert_payload_decode(error, "exact drain clear");
    assert_drains_unchanged();

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_bucket_delete_finalize_claim_operations_reject_wrong_bucket_pg_before_access() {
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
    let owner = crate::CanonicalUserId::from_principal("owner");
    let (bucket, correct_pg_id, wrong_pg_id, generation, correct_claim, wrong_claim) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let (bucket, correct_pg_id) = (0..100)
            .map(|index| crate::tests::bucket_name(format!("finalize-claim-wrong-pg-{index}")))
            .map(|bucket| {
                let pg_id = node.pg_topology().bucket_pg_for(&bucket);
                (bucket, pg_id)
            })
            .find(|(_, pg_id)| *pg_id < 2)
            .expect("two-PG topology must place a test bucket");
        let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
        let mut generations = Vec::new();
        let mut claims = Vec::new();
        for pg_id in [correct_pg_id, wrong_pg_id] {
            let pg = node.get_pg(pg_id).unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            PgMetadataStore::mark_bucket_deleting(&*pg, &bucket).unwrap();
            let generation = PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .bucket_incarnation_generation;
            generations.push(generation);
            claims.push(
                PgMetadataStore::acquire_bucket_delete_finalize_claim(
                    &*pg,
                    &bucket,
                    generation,
                    "wrong-pg-finalize-claim",
                    "wrong-pg-finalize-owner",
                    config.cluster_epoch,
                    10,
                    Some(80),
                    10,
                )
                .unwrap()
                .expect("finalizer claim should be acquired"),
            );
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        assert_eq!(
            generations[0], generations[1],
            "wrong-PG finalizer canaries must use the same bucket generation"
        );
        (
            bucket,
            correct_pg_id,
            wrong_pg_id,
            generations[0],
            claims.remove(0),
            claims.remove(0),
        )
    };

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..3)
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
    let wrong_pg = bucket_pg_id_for_test(wrong_pg_id);
    let assert_payload_decode = |error: BucketSnapshotLoadError, operation: &str| {
        assert!(
            matches!(
                &error,
                BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                })
            ),
            "wrong-PG {operation} must fail with PayloadDecode, got {error:?}"
        );
    };
    let assert_claims_unchanged = || {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        for (pg_id, expected) in [(correct_pg_id, &correct_claim), (wrong_pg_id, &wrong_claim)] {
            let pg = node.get_pg(pg_id).unwrap();
            assert_eq!(
                PgMetadataStore::bucket_delete_finalize_claim(&*pg).unwrap(),
                Some(expected.clone()),
                "wrong-PG finalizer-claim operation must not mutate PG {pg_id}"
            );
        }
    };

    let error = BucketWriteReservationNodeClient::acquire_bucket_delete_finalize_claim(
        &client,
        wrong_pg,
        &bucket,
        generation,
        &wrong_claim.claim_id,
        &wrong_claim.owner_token,
        config.cluster_epoch,
        wrong_claim.claimed_at,
        wrong_claim.lease_deadline,
        wrong_claim.claimed_at,
    )
    .unwrap_err();
    assert_payload_decode(error, "finalizer claim acquire");
    assert_claims_unchanged();

    let error =
        BucketWriteReservationNodeClient::bucket_delete_finalize_claim(&client, wrong_pg, &bucket)
            .unwrap_err();
    assert_payload_decode(error, "finalizer claim get");
    assert_claims_unchanged();

    let error = BucketWriteReservationNodeClient::release_bucket_delete_finalize_claim(
        &client,
        wrong_pg,
        &wrong_claim,
    )
    .unwrap_err();
    assert_payload_decode(error, "finalizer claim release");
    assert_claims_unchanged();

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_lifecycle_sweep_claim_operations_reject_wrong_bucket_pg_before_access() {
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
    let owner = crate::CanonicalUserId::from_principal("owner");
    let (bucket, correct_pg_id, wrong_pg_id, generation, correct_claim, wrong_claim) = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        let (bucket, correct_pg_id) = (0..100)
            .map(|index| crate::tests::bucket_name(format!("lifecycle-claim-wrong-pg-{index}")))
            .map(|bucket| {
                let pg_id = node.pg_topology().bucket_pg_for(&bucket);
                (bucket, pg_id)
            })
            .find(|(_, pg_id)| *pg_id < 2)
            .expect("two-PG topology must place a test bucket");
        let wrong_pg_id = if correct_pg_id == 0 { 1 } else { 0 };
        let mut generations = Vec::new();
        let mut claims = Vec::new();
        for pg_id in [correct_pg_id, wrong_pg_id] {
            let pg = node.get_pg(pg_id).unwrap();
            PgMetadataStore::create_bucket(
                &*pg,
                &bucket,
                "owner",
                &owner,
                &crate::AclGrants::default(),
                false,
                false,
            )
            .unwrap();
            PgMetadataStore::put_bucket_subresource(
                &*pg,
                &bucket,
                crate::types::PutBucketSubresource {
                    kind: BucketSubresourceKind::Lifecycle,
                    body: "<LifecycleConfiguration/>",
                    aux: crate::types::BucketSubresourceAux::None,
                },
            )
            .unwrap();
            let generation = PgMetadataStore::head_bucket_raw(&*pg, &bucket)
                .unwrap()
                .bucket_incarnation_generation;
            generations.push(generation);
            claims.push(
                PgMetadataStore::acquire_lifecycle_sweep_claim(
                    &*pg,
                    &bucket,
                    generation,
                    "wrong-pg-lifecycle-claim",
                    "wrong-pg-lifecycle-owner",
                    config.cluster_epoch,
                    10,
                    None,
                    10,
                )
                .unwrap()
                .expect("lifecycle claim should be acquired"),
            );
            pg.refresh_metadata_command_state_digest().unwrap();
        }
        assert_eq!(
            generations[0], generations[1],
            "wrong-PG lifecycle canaries must use the same bucket generation"
        );
        (
            bucket,
            correct_pg_id,
            wrong_pg_id,
            generations[0],
            claims.remove(0),
            claims.remove(0),
        )
    };

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..4)
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
    let wrong_pg = bucket_pg_id_for_test(wrong_pg_id);
    let assert_payload_decode = |error: BucketSnapshotLoadError, operation: &str| {
        assert!(
            matches!(
                &error,
                BucketSnapshotLoadError::Store(StoreError::StorageRpc {
                    failure: StorageRpcErrorCode::PayloadDecode,
                    ..
                })
            ),
            "wrong-PG {operation} must fail with PayloadDecode, got {error:?}"
        );
    };
    let assert_claims_unchanged = || {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &config.data_dir,
            &config.pg_ids,
            config.default_ec_shape,
        )
        .unwrap();
        for (pg_id, expected) in [(correct_pg_id, &correct_claim), (wrong_pg_id, &wrong_claim)] {
            let pg = node.get_pg(pg_id).unwrap();
            assert_eq!(
                PgMetadataStore::acquire_lifecycle_sweep_claim(
                    &*pg,
                    &bucket,
                    generation,
                    &expected.claim_id,
                    &expected.owner_token,
                    expected.cluster_epoch,
                    expected.claimed_at,
                    expected.lease_deadline,
                    20,
                )
                .unwrap(),
                Some(expected.clone()),
                "wrong-PG lifecycle-claim operation must not mutate PG {pg_id}"
            );
        }
    };

    let error = BucketWriteReservationNodeClient::acquire_lifecycle_sweep_claim(
        &client,
        wrong_pg,
        &bucket,
        generation,
        &wrong_claim.claim_id,
        &wrong_claim.owner_token,
        config.cluster_epoch,
        wrong_claim.claimed_at,
        wrong_claim.lease_deadline,
        20,
    )
    .unwrap_err();
    assert_payload_decode(error, "lifecycle claim acquire");
    assert_claims_unchanged();

    let error = BucketWriteReservationNodeClient::heartbeat_lifecycle_sweep_claim(
        &client,
        wrong_pg,
        &wrong_claim,
        20,
        Some(80),
    )
    .unwrap_err();
    assert_payload_decode(error, "lifecycle claim heartbeat");
    assert_claims_unchanged();

    let error = BucketWriteReservationNodeClient::record_lifecycle_sweep_claim_error(
        &client,
        wrong_pg,
        &wrong_claim,
        "must not be recorded",
    )
    .unwrap_err();
    assert_payload_decode(error, "lifecycle claim error");
    assert_claims_unchanged();

    let error = BucketWriteReservationNodeClient::release_lifecycle_sweep_claim(
        &client,
        wrong_pg,
        &wrong_claim,
    )
    .unwrap_err();
    assert_payload_decode(error, "lifecycle claim release");
    assert_claims_unchanged();

    for thread in server_threads {
        thread.join().unwrap();
    }
}

#[test]
fn unix_bucket_metadata_client_rejects_proof_release_on_non_primary() {
    let tmp = test_util::tempdir();
    let mut primary_config = test_config(&tmp);
    primary_config.node_id = NodeId::new(7);
    primary_config.data_dir = tmp.path().join("primary-node");
    primary_config.pg_routes[0].primary_node_id = NodeId::new(7);
    primary_config.pg_routes[0].acting_set = vec![NodeId::new(8), NodeId::new(7)];
    let mut replica_config = primary_config.clone();
    replica_config.node_id = NodeId::new(8);
    replica_config.data_dir = tmp.path().join("replica-node");
    replica_config.socket_path = tmp.path().join("sock").join("replica-storage.sock");
    let bucket = crate::tests::bucket_name("proof-release-replica-bucket");
    let owner = crate::CanonicalUserId::from_principal("owner");
    let reservation = {
        let node = SharedStorageNode::open_with_default_ec_shape(
            &primary_config.data_dir,
            &primary_config.pg_ids,
            primary_config.default_ec_shape,
        )
        .unwrap();
        let pg = node.get_pg(0).unwrap();
        PgMetadataStore::create_bucket(
            &*pg,
            &bucket,
            "owner",
            &owner,
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
        PgMetadataStore::acquire_durable_bucket_write_reservation(
            &*pg,
            crate::node_runtime::traits::DurableBucketWriteReservationAcquire {
                name: &bucket,
                reservation_id: "reservation-1",
                owner_token: "owner-token-1",
                cluster_epoch: ClusterEpoch::new(1).unwrap(),
                operation_kind: "put-object",
                created_at: 10,
                lease_deadline: 20,
                target_context: Some("key=a"),
            },
        )
        .unwrap()
    };
    private_socket_dir(replica_config.socket_path.parent().unwrap());
    let server = StorageNodeServer::bind(replica_config.clone()).unwrap();
    let server_thread = thread::spawn(move || server.accept_one().unwrap());
    let client = UnixStorageNodeClient::new(
        NodeId::new(8),
        ClusterEpoch::new(1).unwrap(),
        replica_config.socket_path.clone(),
    );

    let err = client
        .release_metadata_command_bucket_write_reservation(
            bucket_pg_id_for_test(0),
            &BucketWriteReservationProof::from(&reservation),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            operation: "proof release",
            ..
        })
    ));
    server_thread.join().unwrap();

    let node = SharedStorageNode::open_with_default_ec_shape(
        &primary_config.data_dir,
        &primary_config.pg_ids,
        primary_config.default_ec_shape,
    )
    .unwrap();
    let pg = node.get_pg(0).unwrap();
    assert!(
        PgMetadataStore::durable_bucket_write_reservation(&*pg, &bucket, "reservation-1")
            .unwrap()
            .is_some()
    );
}

#[test]
fn unix_bucket_delete_replica_head_reads_non_primary_acting_replica() {
    let tmp = test_util::tempdir();
    let mut config = test_config(&tmp);
    config.node_id = NodeId::new(8);
    config.pg_routes[0].primary_node_id = NodeId::new(7);
    config.pg_routes[0].acting_set = vec![NodeId::new(7), NodeId::new(8)];
    let bucket = crate::tests::bucket_name("delete-replica-head-bucket");
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
        pg.refresh_metadata_command_state_digest().unwrap();
    }

    private_socket_dir(config.socket_path.parent().unwrap());
    let server = Arc::new(StorageNodeServer::bind(config.clone()).unwrap());
    let server_threads: Vec<_> = (0..2)
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
    let pg_id = bucket_pg_id_for_test(0);

    let ordinary_error = BucketMetadataNodeClient::head_bucket_raw(&client, pg_id, &bucket)
        .expect_err("ordinary bucket reads must remain primary-only");
    assert!(matches!(
        ordinary_error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::NonActingSetAccess,
            ..
        })
    ));

    let replica =
        BucketMetadataNodeClient::head_bucket_replica_for_delete(&client, pg_id, &bucket).unwrap();
    assert_eq!(replica.name, bucket);

    for server_thread in server_threads {
        server_thread.join().unwrap();
    }
}

#[test]
fn unix_bucket_delete_replica_head_reads_authorized_historical_active_replica() {
    let info = historical_bucket_delete_replica_head(HistoricalReplicaHeadAuthorization::Matching)
        .unwrap();
    assert_eq!(info.name.as_str(), "historical-delete-replica-head-bucket");
}

#[test]
fn unix_bucket_delete_replica_head_rejects_missing_historical_authorization() {
    let error = historical_bucket_delete_replica_head(HistoricalReplicaHeadAuthorization::Missing)
        .unwrap_err();
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::StaleShardLocation,
            ..
        })
    ));
}

#[test]
fn unix_bucket_delete_replica_head_rejects_wrong_historical_authorization() {
    let error =
        historical_bucket_delete_replica_head(HistoricalReplicaHeadAuthorization::WrongEpoch)
            .unwrap_err();
    assert!(matches!(
        error,
        BucketSnapshotLoadError::Store(StoreError::StorageRpc {
            failure: StorageRpcErrorCode::StaleShardLocation,
            ..
        })
    ));
}
