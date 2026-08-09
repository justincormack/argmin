// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn put_object_metadata_fanout_rejects_live_crossed_reservation_subjects() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0], EcShape { k: 2, m: 1 }).unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::tests::bucket_name("metadata-crossed-proof-bucket");
    let key = crate::tests::object_key("metadata-crossed-proof-key");
    create_test_bucket(&cluster, &bucket);
    write_committed_direct_segment_for(&cluster, &bucket, &key, b"metadata subject");

    let operation_reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let operation_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&operation_reservation.record);
    let target_reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
            Some("metadata-crossed-proof-other-key"),
        )
        .unwrap();
    let target_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&target_reservation.record);

    let pg_id = PgId::new(0);
    let primary = map
        .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
        .unwrap();
    let live = crate::PgMetadataStore::get_object_meta(
        &*primary.storage_node().get_pg(pg_id.get()).unwrap(),
        &bucket,
        &key,
    )
    .unwrap()
    .into_live()
    .unwrap();
    let command_with_proof = |proof| {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::PutObjectMetadata(Box::new(
                PutObjectMetadataCommand::from_live_object_and_mutation(
                    live.clone(),
                    PutObjectMetadataMutation::PutTags(crate::tests::object_tags(
                        "<Tagging><TagSet><Tag><Key>crossed</Key><Value>proof</Value></Tag></TagSet></Tagging>",
                    )),
                    proof,
                ),
            )),
        )
    };

    for (case, proof) in [
        ("operation", operation_proof.clone()),
        ("target", target_proof),
    ] {
        let error = cluster
            .validate_metadata_command_bucket_write_reservation(&command_with_proof(proof))
            .unwrap_err();
        assert!(
            matches!(
                error,
                crate::BucketSnapshotLoadError::Metadata(
                    crate::MetadataError::BucketWriteReservationConflict { .. }
                )
            ),
            "crossed PUT object metadata proof {case} must fail central validation: {error:?}"
        );
    }

    let command = command_with_proof(operation_proof);
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &command);
    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let stored = crate::PgMetadataStore::get_object_meta(
            &*map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(pg_id.get())
                .unwrap(),
            &bucket,
            &key,
        )
        .unwrap();
        assert_eq!(stored.as_live().unwrap().tags, None);
    }
}

#[test]
fn object_delete_central_validation_rejects_live_crossed_reservation_subjects() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0], EcShape { k: 2, m: 1 }).unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::tests::bucket_name("delete-crossed-proof-bucket");
    let key = crate::tests::object_key("delete-crossed-proof-key");
    let version_id = crate::VersionId::Null;
    let owner = crate::CanonicalUserId::from_principal("owner");
    for node_id in node_ids {
        seed_bucket_record(&map, node_id, 0, &bucket, &owner);
        let pg = map.node(node_id).unwrap().storage_node().get_pg(0).unwrap();
        crate::PgMetadataStore::put_object_meta(
            &*pg,
            &crate::PutObjectReq::Live(crate::PutLiveObjectReq {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                owner: crate::OwnerIdentity::from_principal("owner"),
                acl_grants: crate::AclGrants::default(),
                public_read: false,
                generation_id: crate::GenerationId::MIN,
                size: 0,
                etag: crate::ObjectEtag::single_part(0),
                ec: EcShape { k: 2, m: 1 },
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
    }

    let operation_reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let operation_proof =
        crate::metadata_command::BucketWriteReservationProof::from(&operation_reservation.record);
    let delete_target_reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
            Some("delete-crossed-proof-other-key"),
        )
        .unwrap();
    let delete_target_proof = crate::metadata_command::BucketWriteReservationProof::from(
        &delete_target_reservation.record,
    );
    let marker_target_reservation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::INSERT_DELETE_MARKER_BUCKET_WRITE_OPERATION_KIND,
            Some("marker-crossed-proof-other-key"),
        )
        .unwrap();
    let marker_target_proof = crate::metadata_command::BucketWriteReservationProof::from(
        &marker_target_reservation.record,
    );

    let pg_id = PgId::new(0);
    let delete_command_with_proof = |proof, mode| {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket_write_reservation: proof,
                bucket: bucket.clone(),
                key: key.clone(),
                version_id,
                mode,
                target: DeleteObjectVersionTarget::DeleteMarker { write_sequence: 1 },
            })),
        )
    };
    let marker_command_with_proof = |proof| {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::InsertDeleteMarker(InsertDeleteMarkerCommand {
                bucket_write_reservation: proof,
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: crate::VersionId::from_u64(2),
                owner: crate::OwnerIdentity::from_principal("owner"),
                write_sequence: 2,
                last_modified_millis: 2,
                stale_payload: None,
            }),
        )
    };

    for (case, command) in [
        (
            "delete operation",
            delete_command_with_proof(
                operation_proof.clone(),
                crate::metadata_command::DeleteObjectVersionMode::Specific,
            ),
        ),
        (
            "delete target",
            delete_command_with_proof(
                delete_target_proof,
                crate::metadata_command::DeleteObjectVersionMode::Specific,
            ),
        ),
        (
            "marker operation",
            marker_command_with_proof(operation_proof.clone()),
        ),
        (
            "marker target",
            marker_command_with_proof(marker_target_proof),
        ),
    ] {
        let error = cluster
            .validate_metadata_command_bucket_write_reservation(&command)
            .unwrap_err();
        assert!(
            matches!(
                error,
                crate::BucketSnapshotLoadError::Metadata(
                    crate::MetadataError::BucketWriteReservationConflict { .. }
                )
            ),
            "crossed object-delete proof {case} must fail central validation: {error:?}"
        );
    }

    for node_id in node_ids {
        let stored = crate::PgMetadataStore::get_object_meta(
            &*map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(pg_id.get())
                .unwrap(),
            &bucket,
            &key,
        )
        .unwrap();
        assert_eq!(stored.version_id(), version_id);
        assert!(stored.as_live().is_some());
    }
}

#[test]
fn object_delete_modes_reject_crossed_live_reservation_operations() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0], EcShape { k: 2, m: 1 }).unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::tests::bucket_name("delete-mode-crossed-proof-bucket");
    let key = crate::tests::object_key("delete-mode-crossed-proof-key");
    create_test_bucket(&cluster, &bucket);
    let live = write_committed_direct_segment_for(&cluster, &bucket, &key, b"delete subject");
    let current = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::DELETE_CURRENT_OBJECT_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let specific = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::DELETE_OBJECT_VERSION_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let pg_id = PgId::new(0);
    let command = |proof, mode| {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::DeleteObjectVersion(Box::new(DeleteObjectVersionCommand {
                bucket_write_reservation: proof,
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: live.version_id,
                mode,
                target: DeleteObjectVersionTarget::DeleteMarker { write_sequence: 1 },
            })),
        )
    };
    for (case, command) in [
        (
            "current proof for specific deletion",
            command(
                crate::metadata_command::BucketWriteReservationProof::from(&current.record),
                crate::metadata_command::DeleteObjectVersionMode::Specific,
            ),
        ),
        (
            "specific proof for current deletion",
            command(
                crate::metadata_command::BucketWriteReservationProof::from(&specific.record),
                crate::metadata_command::DeleteObjectVersionMode::Current,
            ),
        ),
    ] {
        let error = cluster
            .validate_metadata_command_bucket_write_reservation(&command)
            .unwrap_err();
        assert!(
            matches!(
                error,
                crate::BucketSnapshotLoadError::Metadata(
                    crate::MetadataError::BucketWriteReservationConflict { .. }
                )
            ),
            "crossed delete mode {case} must fail central validation: {error:?}"
        );
    }
}

#[test]
fn multipart_creation_fanout_rejects_live_crossed_reservation_subjects() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let map = Arc::new(
        LocalClusterMap::open(tmp.path(), &node_ids, &[0], EcShape { k: 2, m: 1 }).unwrap(),
    );
    let cluster = crate::StorageCluster::from_static_local_map(Arc::clone(&map)).unwrap();
    let bucket = crate::tests::bucket_name("multipart-create-crossed-proof-bucket");
    let key = crate::tests::object_key("multipart-create-crossed-proof-key");
    create_test_bucket(&cluster, &bucket);

    let correct = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let operation = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
            Some(key.as_str()),
        )
        .unwrap();
    let target = cluster
        .acquire_durable_bucket_write_reservation(
            &bucket,
            crate::metadata_command::CREATE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
            Some("multipart-create-crossed-proof-other-key"),
        )
        .unwrap();
    let request = crate::CreateMultipartUploadReq {
        upload_id: crate::tests::multipart_upload_id("multipart-create-crossed-proof"),
        bucket: bucket.clone(),
        key: key.clone(),
        tags: None,
        metadata_blob: crate::SerializedMetadataBlob::default(),
        system_metadata_blob: crate::SerializedSystemMetadataBlob::default(),
        initiator: crate::OwnerIdentity::from_principal("owner"),
        owner: crate::OwnerIdentity::from_principal("owner"),
        acl_grants: crate::AclGrants::default(),
        public_read: false,
        object_lock: crate::ObjectLockState::default(),
        checksum: None,
        encryption: crate::ObjectEncryption::None,
    };
    let pg_id = PgId::new(0);
    let command = |proof| {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                pg_id,
                map.test_next_metadata_command_log_index(pg_id),
            ),
            MetadataCommandPayload::CreateMultipartUpload(Box::new(
                crate::metadata_command::CreateMultipartUploadCommand::from_request_with_bucket_write_reservation_for_test(
                    request.clone(),
                    crate::GenerationId::MIN,
                    None,
                    1,
                    proof,
                ),
            )),
        )
    };

    cluster
        .validate_metadata_command_bucket_write_reservation(&command(
            crate::metadata_command::BucketWriteReservationProof::from(&correct.record),
        ))
        .unwrap();
    for (case, proof) in [
        (
            "operation",
            crate::metadata_command::BucketWriteReservationProof::from(&operation.record),
        ),
        (
            "target",
            crate::metadata_command::BucketWriteReservationProof::from(&target.record),
        ),
    ] {
        let error = cluster
            .validate_metadata_command_bucket_write_reservation(&command(proof))
            .unwrap_err();
        assert!(
            matches!(
                error,
                crate::BucketSnapshotLoadError::Metadata(
                    crate::MetadataError::BucketWriteReservationConflict { .. }
                )
            ),
            "crossed multipart creation proof {case} must fail central validation: {error:?}"
        );
    }

    let malformed = command(crate::metadata_command::BucketWriteReservationProof::from(
        &operation.record,
    ));
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &malformed);
    cluster
        .drain_pending_object_metadata_commands_for_bucket(pg_id, &bucket)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(pg_id.get())
            .unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &request.upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
    }
}

#[test]
fn object_metadata_pending_install_race_drains_winner_and_retries() {
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
    let bucket = bucket_for_pg(topology, 1, "pending-install-race-");
    let first_key = key_for_object_pg(topology, &bucket, 2, "first-object-");
    let second_key = key_for_object_pg(topology, &bucket, 2, "second-object-");
    set_route_primary(&mut first_map, 1, NodeId::new(1));
    set_route_primary(&mut first_map, 2, NodeId::new(1));

    let mut second_map = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap();
    set_route_primary(&mut second_map, 1, NodeId::new(1));
    set_route_primary(&mut second_map, 2, NodeId::new(1));

    let first_map = Arc::new(first_map);
    let second_map = Arc::new(second_map);
    let first_cluster =
        crate::StorageCluster::from_static_local_map(Arc::clone(&first_map)).unwrap();
    create_test_bucket(&first_cluster, &bucket);
    let first_committed = write_committed_direct_segment_for_with_okh(
        &first_cluster,
        &bucket,
        &first_key,
        [0x81; 16],
        b"first",
    );
    write_committed_direct_segment_for_with_okh(
        &first_cluster,
        &bucket,
        &second_key,
        [0x82; 16],
        b"second",
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_key = second_key.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let second_tags =
        "<Tagging><TagSet><Tag><Key>slot</Key><Value>winner</Value></Tag></TagSet></Tagging>"
            .to_string();
    let hook_second_tags = second_tags.clone();
    let hook_proof = acquire_test_bucket_write_proof(
        &first_cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
        Some(second_key.as_str()),
    );
    let _hook_guard = first_cluster.test_install_before_metadata_command_pending_install_hook(
        Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(2);
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let stored =
                crate::PgMetadataStore::get_object_meta(&*pg, &hook_bucket, &hook_key).unwrap();
            let live = stored.as_live().expect("test object is live").clone();
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
                MetadataCommandPayload::PutObjectMetadata(Box::new(
                    PutObjectMetadataCommand::from_live_object_and_mutation(
                        live,
                        PutObjectMetadataMutation::PutTags(crate::tests::object_tags(
                            &hook_second_tags,
                        )),
                        hook_proof.clone(),
                    ),
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

    let first_tags =
        "<Tagging><TagSet><Tag><Key>slot</Key><Value>retry</Value></Tag></TagSet></Tagging>";
    let tagged_version = first_cluster
        .put_object_tags_if(&bucket, &first_key, None, first_tags, |stored| {
            Ok::<_, ()>(stored.version_id())
        })
        .unwrap()
        .unwrap();
    assert_eq!(tagged_version, first_committed.version_id);
    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());

    let first_tags = crate::tests::object_tags(first_tags);
    let second_tags = crate::tests::object_tags(&second_tags);
    for (key, tags) in [(&first_key, &first_tags), (&second_key, &second_tags)] {
        for node_id in node_ids {
            let pg = first_map
                .node(node_id)
                .unwrap()
                .storage_node()
                .get_pg(2)
                .unwrap();
            let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, key).unwrap();
            let live = stored.as_live().expect("test object is live");
            assert_eq!(
                live.tags.as_ref().map(crate::SerializedTagSet::tag_set),
                Some(tags.tag_set())
            );
        }
    }
}

#[test]
fn object_metadata_pending_install_race_reruns_precondition_action() {
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
    let bucket = bucket_for_pg(topology, 1, "pending-install-precondition-");
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
    create_test_bucket(&first_cluster, &bucket);
    write_committed_direct_segment_for_with_okh(
        &first_cluster,
        &bucket,
        &key,
        [0x83; 16],
        b"object",
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let action_calls = Arc::new(AtomicUsize::new(0));
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let winner_tags =
        "<Tagging><TagSet><Tag><Key>slot</Key><Value>winner</Value></Tag></TagSet></Tagging>"
            .to_string();
    let hook_winner_tags = winner_tags.clone();
    let hook_proof = acquire_test_bucket_write_proof(
        &first_cluster,
        &bucket,
        crate::metadata_command::PUT_OBJECT_METADATA_BUCKET_WRITE_OPERATION_KIND,
        Some(key.as_str()),
    );
    let _hook_guard = first_cluster.test_install_before_metadata_command_pending_install_hook(
        Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(2);
            let primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                .unwrap();
            let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
            let stored =
                crate::PgMetadataStore::get_object_meta(&*pg, &hook_bucket, &hook_key).unwrap();
            let live = stored.as_live().expect("test object is live").clone();
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
                MetadataCommandPayload::PutObjectMetadata(Box::new(
                    PutObjectMetadataCommand::from_live_object_and_mutation(
                        live,
                        PutObjectMetadataMutation::PutTags(crate::tests::object_tags(
                            &hook_winner_tags,
                        )),
                        hook_proof.clone(),
                    ),
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

    let loser_tags =
        "<Tagging><TagSet><Tag><Key>slot</Key><Value>loser</Value></Tag></TagSet></Tagging>";
    let calls_for_action = Arc::clone(&action_calls);
    let result = first_cluster
        .put_object_tags_if(&bucket, &key, None, loser_tags, move |stored| {
            calls_for_action.fetch_add(1, Ordering::SeqCst);
            if stored
                .as_live()
                .expect("test object is live")
                .tags
                .is_some()
            {
                Err("tags already present")
            } else {
                Ok(stored.version_id())
            }
        })
        .unwrap();
    assert_eq!(result, Err("tags already present"));
    assert!(hook_ran.load(Ordering::SeqCst));
    assert_eq!(
        action_calls.load(Ordering::SeqCst),
        2,
        "request action must be rerun after slot contention changes object state"
    );
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());
    let winner_tags = crate::tests::object_tags(&winner_tags);

    for node_id in node_ids {
        let pg = first_map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(2)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        let live = stored.as_live().expect("test object is live");
        assert_eq!(
            live.tags.as_ref().map(crate::SerializedTagSet::tag_set),
            Some(winner_tags.tag_set())
        );
    }
    assert_bucket_write_reservations_released(&first_map, &bucket);
}
