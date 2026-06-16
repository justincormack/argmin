use super::*;

#[test]
fn direct_put_metadata_command_retry_reuses_pending_partial_replica_command() {
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
    let bucket_pg_id = cluster.bucket_metadata_pg_id(&bucket);
    let reservation_id = crate::SessionId::try_from("14".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put metadata command partial apply retry";
    let segment_okh = [64; 16];
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
    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected direct put metadata command apply failure",
                        source: std::io::Error::other(
                            "injected direct put metadata command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected direct put metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial direct PUT metadata command must remain pending"
    );
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let slot = pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .expect("partial direct PUT should leave durable primary pending slot");
        assert_eq!(slot.scope_bucket.as_ref(), Some(&bucket));
    }
    for node_id in [NodeId::new(0), NodeId::new(2)] {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }
    {
        let bucket_primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg_id))
            .unwrap()
            .storage_node();
        let bucket_pg = bucket_primary.get_pg(bucket_pg_id).unwrap();
        assert_eq!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
                .unwrap()
                .len(),
            1,
            "partial direct PUT command must keep its command-owned bucket write proof live"
        );
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().generation_id, generation_id);
    }

    let outcome = cluster
        .commit_direct_put_object_from_payload_shards(
            &commit_req,
            &written.written_shards,
            |_| -> Result<(), ()> { panic!("retry must reuse the pending direct PUT command") },
        )
        .unwrap()
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    {
        let bucket_primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg_id))
            .unwrap()
            .storage_node();
        let bucket_pg = bucket_primary.get_pg(bucket_pg_id).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
                .unwrap()
                .is_empty(),
            "direct PUT retry convergence must release the command-owned bucket write proof"
        );
    }
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        assert!(pg
            .pending_metadata_command_slot(NodeId::new(1).as_u32(), ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
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
fn direct_put_open_time_convergence_releases_bucket_write_reservation() {
    let _serial = lock_metadata_command_apply_hook_test();
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
    let bucket_pg_id = cluster.bucket_metadata_pg_id(&bucket);
    let reservation_id = crate::SessionId::try_from("51".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put metadata command open-time convergence";
    let segment_okh = [81; 16];
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
    let failed = Arc::new(AtomicBool::new(false));
    let hook_failed = Arc::clone(&failed);
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(1)
                        && !hook_failed.swap(true, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected direct put reopen apply failure",
                        source: std::io::Error::other("injected direct put reopen apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(matches!(
        err,
        crate::ObjectPgActionError::Store(StoreError::Io { .. })
    ));
    drop(hook_guard);
    assert!(failed.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial direct PUT should leave a pending command for open-time convergence"
    );
    {
        let bucket_primary = map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg_id))
            .unwrap()
            .storage_node();
        let bucket_pg = bucket_primary.get_pg(bucket_pg_id).unwrap();
        assert_eq!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
                .unwrap()
                .len(),
            1
        );
    }
    drop(cluster);
    drop(map);

    let reopened =
        Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap());
    assert!(
        pending_metadata_command_for_test(&reopened, PgId::new(object_pg), &bucket).is_none(),
        "open-time convergence should clear the direct PUT pending command"
    );
    for node_id in node_ids {
        let pg = reopened
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().generation_id, generation_id);
    }
    {
        let bucket_primary = reopened
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(bucket_pg_id))
            .unwrap()
            .storage_node();
        let bucket_pg = bucket_primary.get_pg(bucket_pg_id).unwrap();
        assert!(
            crate::PgMetadataStore::durable_bucket_write_reservations(&*bucket_pg, &bucket)
                .unwrap()
                .is_empty(),
            "open-time convergence must release the command-owned bucket write proof"
        );
    }
    assert_clean_metadata_command_stream(&reopened, &[object_pg]);
}

#[test]
fn object_generation_reservation_entry_drains_pending_direct_put_commit() {
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
    let reservation_id = crate::SessionId::try_from("22".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let payload = b"direct put metadata command production retry";
    let segment_okh = [72; 16];
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
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected direct put metadata command apply failure",
                        source: std::io::Error::other(
                            "injected direct put metadata command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected direct put metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial direct PUT metadata command must remain pending"
    );

    let next_reservation_id = crate::SessionId::try_from("23".repeat(16)).unwrap();
    let next_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &next_reservation_id)
        .unwrap();
    assert!(next_generation_id.get() > generation_id.get());
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().generation_id, generation_id);
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &next_reservation_id,
            )
            .unwrap(),
            next_generation_id
        );
    }
}

#[test]
fn object_delete_drains_pending_direct_put_commit_before_delete() {
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
    let old_committed =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"old direct object");

    let reservation_id = crate::SessionId::try_from("22".repeat(16)).unwrap();
    let generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    assert!(generation_id.get() > old_committed.generation_id.get());
    let payload = b"new direct object with pending command";
    let segment_okh = [73; 16];
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
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(0)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected direct put metadata command apply failure",
                        source: std::io::Error::other(
                            "injected direct put metadata command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .commit_direct_put_object_from_payload_shards(&commit_req, &written.written_shards, |_| {
            Ok::<(), ()>(())
        })
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected direct put metadata command apply failure",
                ..
            })
        ),
        "expected injected replica failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial direct PUT metadata command must remain pending"
    );
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let stored = crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key).unwrap();
        assert_eq!(stored.as_live().unwrap().generation_id, generation_id);
    }

    let outcome = cluster
        .delete_current_object_if(&bucket, &key, |stored| {
            let stored = stored.expect("pending direct PUT should be applied before delete");
            let live = stored
                .as_live()
                .expect("pending direct PUT should publish a live object");
            assert_eq!(live.generation_id, generation_id);
            Ok::<(), ()>(())
        })
        .unwrap()
        .unwrap();
    assert!(matches!(
        outcome.deleted,
        crate::DeletedCurrentObject::Live {
            generation_id: deleted_generation_id,
            ..
        } if deleted_generation_id == generation_id
    ));
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }

    let next_reservation_id = crate::SessionId::try_from("23".repeat(16)).unwrap();
    let next_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &next_reservation_id)
        .unwrap();
    assert!(next_generation_id.get() > generation_id.get());
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(
                matches!(
                    crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
                    Err(crate::MetadataError::ObjectNotFound)
                ),
                "later reservation must not resurrect the deleted pending direct PUT on node {node_id:?}"
            );
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &next_reservation_id,
            )
            .unwrap(),
            next_generation_id
        );
    }
}

#[test]
fn multipart_completion_command_publishes_streamed_part_segments_to_all_acting_nodes() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
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
    let (mut req, mut expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "streamedcomplete");
    req.versioning = crate::BucketVersioningState::Enabled;

    let replacement_session_id = crate::SessionId::try_from("52".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &req.upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            2,
            &replacement_session_id,
        )
        .unwrap();
    let replacement_payload = b"replacement stream session";
    let replacement_okh = [0x52; 16];
    let (_target, replacement_segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: replacement_session_id.clone(),
                segment_index: 0,
                size: replacement_payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(replacement_payload)),
                segment_okh: replacement_okh,
            },
        )
        .unwrap();
    let replacement_shards = cluster
        .write_stream_segment_payload_shards(&replacement_segment, replacement_payload)
        .unwrap();
    let replacement_shard_batch = replacement_shards
        .iter()
        .map(|written| (&written.key, written.ack))
        .collect::<Vec<_>>();
    cluster
        .commit_stream_segment_append(
            &bucket,
            &key,
            &replacement_session_id,
            replacement_segment.segment_index,
            &replacement_segment,
            &replacement_shard_batch,
        )
        .unwrap();
    for shard_index in 0..ec_shape.k + ec_shape.m {
        assert!(cluster
            .test_payload_shard_file_exists(
                replacement_segment.data_pg_id,
                ec_shape,
                &replacement_segment.segment_okh,
                replacement_segment.segment_vid,
                shard_index
            )
            .unwrap());
    }
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &req.upload_id)
        .unwrap();
    let requested_parts = req
        .part_records
        .iter()
        .map(|part| part.part_number)
        .collect::<Vec<_>>();
    let completion_snapshot = cluster
        .load_multipart_completion_snapshot(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            &requested_parts,
        )
        .unwrap();
    req.expected_stale_payload_source = completion_snapshot.stale_payload_source;
    req.selected_streaming_segments = completion_snapshot.selected_streaming_segments;
    req.expected_cleanup = completion_snapshot.cleanup;

    let outcome = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();
    expected_segment.version_id = outcome.version_id.to_u64();

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_streamed_multipart_completion_on_acting_nodes(
        &map,
        &node_ids,
        object_pg,
        &req,
        &expected_segment,
        &outcome,
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &replacement_session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &replacement_session_id)
                .unwrap()
                .is_empty(),
            "completion must delete staged replacement stream segments on node {node_id:?}"
        );
    }
    for shard_index in 0..ec_shape.k + ec_shape.m {
        assert!(
            !cluster
                .test_payload_shard_file_exists(
                    replacement_segment.data_pg_id,
                    ec_shape,
                    &replacement_segment.segment_okh,
                    replacement_segment.segment_vid,
                    shard_index
                )
                .unwrap(),
            "completion must delete staged replacement stream shard {shard_index}"
        );
    }
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
fn already_recorded_multipart_completion_fanout_cleans_terminal_stream_uploads() {
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
    let (req, _) = seed_streamed_multipart_completion(&cluster, &bucket, &key, "mpufanout");
    let stale_session_id = crate::SessionId::try_from("69".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &req.upload_id)
        .unwrap();
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            2,
            &stale_session_id,
        )
        .unwrap();
    let (mut command, _) = pending_multipart_completion_command_for_test(
        &map,
        &cluster,
        PgId::new(object_pg),
        &req,
        1234,
    );
    {
        let primary = map.node(NodeId::new(1)).unwrap().storage_node();
        let pg = primary.get_pg(object_pg).unwrap();
        let active_session = crate::PgMetadataStore::get_stream_upload(&*pg, &stale_session_id)
            .expect("active UploadPart stream session");
        let stream_upload_segments =
            crate::PgMetadataStore::list_stream_segments(&*pg, &stale_session_id)
                .expect("active UploadPart stream segments");
        let mut payload = command.payload().clone();
        let MetadataCommandPayload::CommitMultipartObject(commit) = &mut payload else {
            unreachable!("test helper must build a multipart completion command");
        };
        commit.stream_uploads = vec![crate::TerminalStreamCleanupRecord::from(&active_session)];
        commit.stream_upload_segments = stream_upload_segments;
        command = MetadataCommandEnvelope::new(command.id(), payload);
    }

    cluster
        .test_apply_metadata_command_to_acting_set_from_origin(NodeId::new(1), &command)
        .unwrap();

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        crate::PgMetadataStore::create_stream_upload(
            &*pg,
            &crate::CreateStreamUploadReq {
                session_id: stale_session_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                target: crate::StreamUploadTarget::UploadPart {
                    upload_id: req.upload_id.clone(),
                    part_number: 2,
                },
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
            crate::PgMetadataStore::get_stream_upload(&*pg, &stale_session_id),
            Err(crate::MetadataError::StreamSessionNotFound { .. })
        ));
    }
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}

#[test]
fn multipart_completion_over_standard_object_reopens_with_valid_digest() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let pg_ids = [0, 1, 2, 3];
    let ec_shape = EcShape { k: 2, m: 1 };
    let map = Arc::new(LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape).unwrap());
    let (bucket, key, object_pg, _) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        bucket_key_with_distinct_object_and_data_pg(topology)
    };

    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let old =
        write_committed_direct_segment_for(&cluster, &bucket, &key, b"old standard object payload");
    let (req, expected_segment) = seed_streamed_multipart_completion_with_existing(
        &cluster,
        &bucket,
        &key,
        "stdoverwrite",
        true,
    );

    let outcome = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();
    assert!(
        matches!(
            outcome.stale_payload,
            Some(crate::CompletedMultipartStalePayload::Segments { generation_id, .. })
                if generation_id == old.generation_id
        ),
        "multipart overwrite should record stale standard payload"
    );
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &map,
        &node_ids,
        object_pg,
        &req,
        &expected_segment,
        &outcome,
        2,
    );
    for node_id in &node_ids {
        let pg = map
            .nodes
            .get(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        assert_eq!(
            pg.test_metadata_digest_table_mismatches().unwrap(),
            Vec::<(String, u64, u64)>::new(),
            "node {node_id:?} should keep digest cache in sync before reopen"
        );
    }

    drop(cluster);
    drop(map);
    LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
        .expect("multipart overwrite should leave restart digest valid");
}

#[test]
fn multipart_completion_rejects_stale_selected_part_row() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
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
    let (req, _) = seed_streamed_multipart_completion(&cluster, &bucket, &key, "stalepart");

    let (_replacement_shards, replacement_part, _replacement_segment) =
        upload_streamed_test_multipart_part(
            &cluster,
            &bucket,
            &key,
            &req.upload_id,
            1,
            [0x5A; 16],
            b"replacement part payload",
        );
    assert_ne!(
        replacement_part, req.part_records[0],
        "test must replace the selected part row after completion snapshot"
    );

    let err = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::StaleMultipartCompletionSnapshot
        ),
        "stale selected part row must fail closed, got {err:?}"
    );
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none(),
        "stale multipart completion must not publish a pending command"
    );
}

#[test]
fn versioned_direct_put_and_multipart_completion_allocate_versions_via_command_stream() {
    #[derive(Default)]
    struct VersionRaceState {
        direct_at_apply: bool,
        multipart_at_apply: bool,
        release_direct: bool,
    }

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
    let (mut multipart_req, mut expected_multipart_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "versionraceupload");
    multipart_req.versioning = crate::BucketVersioningState::Enabled;

    let reservation_id = crate::SessionId::try_from("73".repeat(16)).unwrap();
    let direct_generation_id = cluster
        .reserve_put_object_generation(&bucket, &key, &reservation_id)
        .unwrap();
    let direct_payload = b"versioned direct put races multipart completion";
    let direct_okh = [73; 16];
    let direct_written = cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            direct_generation_id,
            0,
            &direct_okh,
            direct_payload,
        )
        .unwrap();
    let mut direct_req = direct_put_commit_req(
        &cluster,
        DirectPutCommitReqFixture {
            bucket: &bucket,
            key: &key,
            reservation_id: reservation_id.clone(),
            generation_id: direct_generation_id,
            payload: direct_payload,
            segment_okh: direct_okh,
            written: &direct_written,
        },
    );
    direct_req.versioning = crate::BucketVersioningState::Enabled;

    let _serial = lock_metadata_command_apply_hook_test();
    let race_state = Arc::new((Mutex::new(VersionRaceState::default()), Condvar::new()));
    let direct_seen = Arc::new(AtomicBool::new(false));
    let multipart_seen = Arc::new(AtomicBool::new(false));
    let hook_key = key.clone();
    let hook_reservation_id = reservation_id.clone();
    let hook_upload_id = multipart_req.upload_id.clone();
    let expected_multipart_req = multipart_req.clone();
    let hook_race_state = Arc::clone(&race_state);
    let hook_direct_seen = Arc::clone(&direct_seen);
    let hook_multipart_seen = Arc::clone(&multipart_seen);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, command| {
            let (lock, cvar) = &*hook_race_state;
            match command.payload() {
                MetadataCommandPayload::CommitDirectPutObject(commit)
                    if commit.object.key == hook_key
                        && commit.generation_reservation_id == hook_reservation_id
                        && !hook_direct_seen.swap(true, Ordering::SeqCst) =>
                {
                    let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                    state.direct_at_apply = true;
                    cvar.notify_all();
                    while !state.release_direct {
                        state = cvar.wait(state).unwrap_or_else(|e| e.into_inner());
                    }
                }
                MetadataCommandPayload::CommitMultipartObject(commit)
                    if commit.upload_id == hook_upload_id
                        && !hook_multipart_seen.swap(true, Ordering::SeqCst) =>
                {
                    let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                    state.multipart_at_apply = true;
                    cvar.notify_all();
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let direct_cluster = Arc::clone(&cluster);
    let direct_written_shards = direct_written.written_shards.clone();
    let direct_thread = std::thread::spawn(move || {
        direct_cluster.commit_direct_put_object_from_payload_shards(
            &direct_req,
            &direct_written_shards,
            |_| Ok::<_, ()>(()),
        )
    });

    {
        let (lock, cvar) = &*race_state;
        let state = lock.lock().unwrap_or_else(|e| e.into_inner());
        let (state, _) = cvar
            .wait_timeout_while(state, Duration::from_secs(5), |state| {
                !state.direct_at_apply
            })
            .unwrap();
        assert!(
            state.direct_at_apply,
            "direct PUT did not reach command apply"
        );
    }

    let multipart_cluster = Arc::clone(&cluster);
    let multipart_thread = std::thread::spawn(move || {
        multipart_cluster.complete_multipart_upload_commit_serialized(multipart_req, 16)
    });

    {
        let (lock, cvar) = &*race_state;
        let state = lock.lock().unwrap_or_else(|e| e.into_inner());
        let (mut state, _) = cvar
            .wait_timeout_while(state, Duration::from_millis(100), |state| {
                !state.multipart_at_apply
            })
            .unwrap();
        assert!(
            !state.multipart_at_apply,
            "multipart completion applied while direct PUT held the bucket command stream"
        );
        state.release_direct = true;
        cvar.notify_all();
    }

    let direct_outcome = direct_thread.join().unwrap().unwrap().unwrap();
    let multipart_outcome = multipart_thread.join().unwrap().unwrap();
    drop(hook_guard);

    assert_eq!(direct_outcome.version_id, crate::VersionId::from_u64(1));
    assert_eq!(multipart_outcome.version_id, crate::VersionId::from_u64(2));
    expected_multipart_segment.version_id = multipart_outcome.version_id.to_u64();

    for node_id in node_ids {
        let pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(object_pg)
            .unwrap();
        let direct_version = crate::PgMetadataStore::get_object_version(
            &*pg,
            &bucket,
            &key,
            direct_outcome.version_id,
        )
        .unwrap();
        let direct_live = direct_version.as_live().unwrap();
        assert_eq!(direct_live.generation_id, direct_generation_id);
        assert!(direct_live.became_noncurrent_at.is_some());
    }
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &map,
        &node_ids,
        object_pg,
        &expected_multipart_req,
        &expected_multipart_segment,
        &multipart_outcome,
        2,
    );
    assert_object_version_counter_on_acting_nodes(&map, &node_ids, object_pg, &bucket, &key, 3);
}

#[test]
fn stream_upload_part_staging_and_finalize_use_object_metadata_commands() {
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
    let upload_id = upload_id_from_label("streampartcmd");
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

    let session_id = crate::SessionId::try_from("43".repeat(16)).unwrap();
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

    let payload = b"streamed multipart command part";
    let segment_okh = [0x43; 16];
    let (_target, segment) = cluster
        .prepare_stream_segment_append(
            &bucket,
            &key,
            &crate::PrepareStreamUploadSegmentAppendReq {
                session_id: session_id.clone(),
                segment_index: 0,
                size: payload.len() as u64,
                segment_crc64: Some(checksum::crc64::checksum(payload)),
                segment_okh,
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

    let expected_part = cluster
        .finalize_upload_part_stream(&bucket, &key, &upload_id, &session_id, 1, |snapshot| {
            let generation = snapshot
                .existing_part_generation
                .map_or(0, |generation| generation + 1);
            let part = crate::MultipartPartRecord {
                upload_id: upload_id.clone(),
                part_number: 1,
                generation,
                size: payload.len() as u64,
                etag: vec![0x55; 8],
                etag_kind: crate::EtagKind::Crc64,
                part_okh: [0u8; 16],
                part_vid: crate::GenerationId::new(u64::from(generation) + 1).unwrap(),
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
                value: part.clone(),
                part,
                segments,
            })
        })
        .unwrap()
        .unwrap()
        .value;

    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
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

fn stream_upload_part_create_rejects_raced_upload_state(target_state: crate::UploadState) {
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
    let upload_id = upload_id_from_label("streamraced");
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

    let _serial = lock_metadata_command_apply_hook_test();
    let did_flip = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_upload_id = upload_id.clone();
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let hook_node_ids = node_ids;
    let hook_did_flip = Arc::clone(&did_flip);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, command| {
            if let MetadataCommandPayload::CreateStreamUpload(create) = command.payload() {
                let is_target_upload = matches!(
                    &create.session.target,
                    crate::StreamUploadTarget::UploadPart { upload_id, .. }
                        if upload_id == &hook_upload_id
                );
                if create.session.bucket == hook_bucket
                    && create.session.key == hook_key
                    && is_target_upload
                    && !hook_did_flip.swap(true, Ordering::SeqCst)
                {
                    for node_id in hook_node_ids {
                        let node = hook_map.node(node_id).unwrap().storage_node();
                        let pg = node.get_pg(object_pg).unwrap();
                        crate::PgMetadataStore::set_upload_state(
                            &*pg,
                            &hook_upload_id,
                            target_state,
                        )
                        .unwrap();
                        pg.refresh_metadata_command_state_digest().unwrap();
                    }
                }
            }
            Ok(())
        },
    ));

    let session_id = crate::SessionId::try_from("44".repeat(16)).unwrap();
    let upload = cluster
        .load_in_progress_multipart_upload(&bucket, &key, &upload_id)
        .unwrap();
    let err = cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            1,
            &session_id,
        )
        .unwrap_err();
    drop(hook_guard);

    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Metadata(crate::MetadataError::NoSuchUpload { .. })
        ),
        "expected raced upload state to reject stream session creation, got {err:?}"
    );
    assert!(did_flip.load(Ordering::SeqCst));
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
fn stream_upload_part_create_rejects_raced_multipart_abort() {
    stream_upload_part_create_rejects_raced_upload_state(crate::UploadState::Aborting);
}

#[test]
fn stream_upload_part_create_rejects_raced_multipart_completion() {
    stream_upload_part_create_rejects_raced_upload_state(crate::UploadState::Completing);
}

#[test]
fn multipart_completion_order_is_bucket_primary_serialized_across_object_pgs() {
    #[derive(Default)]
    struct CompletionRaceState {
        first_at_apply: bool,
        second_at_apply: bool,
        release_first: bool,
    }

    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key_a, key_b, object_pg_a, object_pg_b) = {
        let topology = map
            .nodes
            .get(&NodeId::new(0))
            .unwrap()
            .storage_node()
            .pg_topology();
        let bucket = bucket_for_pg(topology, 0, "mpu-order-bucket-");
        let key_a = key_for_object_pg(topology, &bucket, 1, "mpu-order-a-");
        let key_b = key_for_object_pg(topology, &bucket, 2, "mpu-order-b-");
        (bucket, key_a, key_b, 1, 2)
    };
    set_route_primary(&mut map, object_pg_a, NodeId::new(1));
    set_route_primary(&mut map, object_pg_b, NodeId::new(2));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req_a, _) = seed_streamed_multipart_completion(&cluster, &bucket, &key_a, "bucketordera");
    let (req_b, _) = seed_streamed_multipart_completion(&cluster, &bucket, &key_b, "bucketorderb");

    let _serial = lock_metadata_command_apply_hook_test();
    let race_state = Arc::new((Mutex::new(CompletionRaceState::default()), Condvar::new()));
    let first_seen = Arc::new(AtomicBool::new(false));
    let second_seen = Arc::new(AtomicBool::new(false));
    let hook_key_a = key_a.clone();
    let hook_key_b = key_b.clone();
    let hook_race_state = Arc::clone(&race_state);
    let hook_first_seen = Arc::clone(&first_seen);
    let hook_second_seen = Arc::clone(&second_seen);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, command| {
            let MetadataCommandPayload::CommitMultipartObject(commit) = command.payload() else {
                return Ok(());
            };
            let (lock, cvar) = &*hook_race_state;
            if commit.object.key == hook_key_a && !hook_first_seen.swap(true, Ordering::SeqCst) {
                let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                state.first_at_apply = true;
                cvar.notify_all();
                while !state.release_first {
                    state = cvar.wait(state).unwrap_or_else(|e| e.into_inner());
                }
            } else if commit.object.key == hook_key_b
                && !hook_second_seen.swap(true, Ordering::SeqCst)
            {
                let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                state.second_at_apply = true;
                cvar.notify_all();
            }
            Ok(())
        },
    ));

    let cluster_a = Arc::clone(&cluster);
    let first = std::thread::spawn(move || {
        cluster_a.complete_multipart_upload_commit_serialized(req_a, 16)
    });

    {
        let (lock, cvar) = &*race_state;
        let state = lock.lock().unwrap_or_else(|e| e.into_inner());
        let (state, _) = cvar
            .wait_timeout_while(state, Duration::from_secs(5), |state| !state.first_at_apply)
            .unwrap();
        assert!(
            state.first_at_apply,
            "first completion did not reach command apply"
        );
    }

    let cluster_b = Arc::clone(&cluster);
    let second_upload_id = req_b.upload_id.clone();
    let second = std::thread::spawn(move || {
        cluster_b.complete_multipart_upload_commit_serialized(req_b, 16)
    });

    {
        let (lock, cvar) = &*race_state;
        let state = lock.lock().unwrap_or_else(|e| e.into_inner());
        let (mut state, _) = cvar
            .wait_timeout_while(state, Duration::from_millis(100), |state| {
                !state.second_at_apply
            })
            .unwrap();
        state.release_first = true;
        cvar.notify_all();
    }

    let first_outcome = first.join().unwrap().unwrap();
    let second_outcome = second.join().unwrap().unwrap();
    drop(hook_guard);

    assert_eq!(first_outcome.version_id, crate::VersionId::Null);
    assert_eq!(second_outcome.version_id, crate::VersionId::Null);
    let mut orders = vec![
        completed_multipart_order_on_node(
            &map,
            NodeId::new(0),
            object_pg_a,
            &bucket,
            &upload_id_from_label("bucketordera"),
        ),
        completed_multipart_order_on_node(
            &map,
            NodeId::new(0),
            object_pg_b,
            &bucket,
            &second_upload_id,
        ),
    ];
    orders.sort_unstable();
    assert_eq!(orders, vec![1, 2]);

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let bucket_pg = node
            .get_pg(node.pg_topology().bucket_pg_for(&bucket))
            .unwrap();
        assert_eq!(
            bucket_pg
                .completed_multipart_upload_sequence_for_bucket(&bucket)
                .unwrap(),
            2,
            "node {node_id:?} did not catch up bucket completed MPU order"
        );
    }
}

#[test]
fn multipart_completion_zero_apply_failure_retains_pending_command_for_retry() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let (bucket, key, object_pg, _) = {
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
    let (req, expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "zerofailcomplete");

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let hook_key = key.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitMultipartObject(commit)
                    if commit.object.bucket == hook_bucket
                        && commit.object.key == hook_key
                        && node_id == NodeId::new(1)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected multipart completion command apply failure",
                        source: std::io::Error::other(
                            "injected multipart completion command apply failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected multipart completion command apply failure",
                ..
            })
        ),
        "expected injected zero-apply failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "zero-apply multipart completion failure must keep its pending command"
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
    }

    let outcome = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());
    assert_streamed_multipart_completion_on_acting_nodes(
        &map,
        &node_ids,
        object_pg,
        &req,
        &expected_segment,
        &outcome,
    );
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn multipart_completion_partial_apply_reopens_and_converges() {
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
    let (req, mut expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "completereopen");

    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_upload_id = req.upload_id.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::CommitMultipartObject(commit)
                    if commit.upload_id == hook_upload_id
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected multipart completion reopen failure",
                        source: std::io::Error::other(
                            "injected multipart completion reopen failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected multipart completion reopen failure",
                ..
            })
        ),
        "expected injected partial completion failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial multipart completion command must remain durable before reopen"
    );
    drop(cluster);
    drop(map);

    let reopened = LocalClusterMap::open(tmp.path(), &node_ids, &pg_ids, ec_shape)
        .expect("reopen local map with in-flight multipart completion");
    let reopened = Arc::new(reopened);
    assert!(
        pending_metadata_command_for_test(&reopened, PgId::new(object_pg), &bucket).is_none(),
        "open-time recovery should converge and clear the partial completion command"
    );

    let first_pg = reopened
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(object_pg)
        .unwrap();
    let stored = crate::PgMetadataStore::get_object_meta(&*first_pg, &bucket, &key).unwrap();
    let live = stored.as_live().unwrap();
    let outcome = crate::CompleteMultipartCommitOutcome {
        version_id: live.version_id,
        stale_payload: None,
        live_tags: live.tags.clone(),
        live_size: live.size,
        live_last_modified: live.last_modified,
    };
    expected_segment.version_id = outcome.version_id.to_u64();
    drop(first_pg);

    assert_streamed_multipart_completion_on_acting_nodes(
        &reopened,
        &node_ids,
        object_pg,
        &req,
        &expected_segment,
        &outcome,
    );
    assert_terminal_multipart_upload_invariants(
        &reopened,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    assert_clean_metadata_command_stream(&reopened, &[object_pg]);
    assert_bucket_write_reservations_released(&reopened, &bucket);
}

#[test]
fn multipart_completion_command_id_race_drains_winner_and_resnapshots_stale_payload() {
    let _serial = lock_metadata_command_apply_hook_test();
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
    let bucket = bucket_for_pg(topology, 1, "mpu-complete-id-race-");
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
    let (req, expected_segment) =
        seed_streamed_multipart_completion(&first_cluster, &bucket, &key, "completeidrace");

    let winner_payload = b"winner before multipart completion command id";
    let winner_reservation_id =
        crate::SessionId::try_from("8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a".to_string()).unwrap();
    let winner_generation_id = second_cluster
        .reserve_put_object_generation(&bucket, &key, &winner_reservation_id)
        .unwrap();
    let winner_written = second_cluster
        .write_direct_put_segment_payload_shards(
            &bucket,
            &key,
            winner_generation_id,
            0,
            &[0x8a; 16],
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
            segment_okh: [0x8a; 16],
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
    let winner_command = {
        let primary = second_map
            .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(2))
            .unwrap();
        let pg = primary.storage_node().get_pg(2).unwrap();
        second_cluster
            .prepare_commit_direct_put_object_command(
                PgId::new(2),
                &pg,
                &winner_req,
                crate::VersionId::Null,
                winner_req.bucket_write_reservation.clone(),
            )
            .unwrap()
    };

    let slot_installed = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&second_map);
    let hook_bucket = bucket.clone();
    let hook_command = winner_command.clone();
    let hook_slot_installed = Arc::clone(&slot_installed);
    let hook_guard = first_cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |_node_id, command| {
            let MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(advance) =
                command.payload()
            else {
                return Ok(());
            };
            if advance.bucket == hook_bucket && !hook_slot_installed.swap(true, Ordering::SeqCst) {
                let pg_id = PgId::new(2);
                let primary = hook_map
                    .metadata_pg_primary_node(ClusterEpoch::INITIAL, pg_id)
                    .unwrap();
                let pg = primary.storage_node().get_pg(pg_id.get()).unwrap();
                pg.try_insert_pending_metadata_command_slot(
                    primary.node_id().as_u32(),
                    &hook_command,
                    Some(&hook_bucket),
                )
                .unwrap();
            }
            Ok(())
        },
    ));

    let outcome = first_cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();
    drop(hook_guard);

    assert!(slot_installed.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&first_map, PgId::new(2), &bucket).is_none());
    assert!(
        matches!(
            outcome.stale_payload,
            Some(crate::CompletedMultipartStalePayload::Segments { generation_id, .. })
                if generation_id == winner_generation_id
        ),
        "completion must resnapshot stale payload after draining the winning object command"
    );
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &first_map,
        &node_ids,
        2,
        &req,
        &expected_segment,
        &outcome,
        2,
    );
    assert_clean_metadata_command_stream(&first_map, &[1, 2]);
}

#[test]
fn multipart_completion_retries_partial_bucket_order_command() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "mpu-order-retry-");
    let key = key_for_object_pg(topology, &bucket, 2, "object-");
    set_route_primary(&mut map, 1, NodeId::new(1));
    set_route_primary(&mut map, 2, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);
    let (req, expected_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "orderretrycomplete");

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_bucket = bucket.clone();
    let fail_once_hook = Arc::clone(&fail_once);
    let hook_guard = cluster.test_install_before_metadata_command_apply_hook(Arc::new(
        move |node_id, command| {
            match command.payload() {
                MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(advance)
                    if advance.bucket == hook_bucket
                        && node_id == NodeId::new(2)
                        && fail_once_hook.swap(false, Ordering::SeqCst) =>
                {
                    return Err(StoreError::Io {
                        context: "injected completed MPU order apply failure",
                        source: std::io::Error::other("injected completed MPU order apply failure"),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected completed MPU order apply failure",
                ..
            })
        ),
        "expected injected bucket-PG order failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_some(),
        "partial bucket-PG order command must remain pending"
    );
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(2), &bucket).is_none(),
        "object-PG completion must not publish before order command converges"
    );

    let outcome = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());
    assert!(pending_metadata_command_for_test(&map, PgId::new(2), &bucket).is_none());
    assert_eq!(
        completed_multipart_order_on_node(&map, NodeId::new(0), 2, &bucket, &req.upload_id),
        1
    );
    assert_streamed_multipart_completion_on_acting_nodes(
        &map,
        &node_ids,
        2,
        &req,
        &expected_segment,
        &outcome,
    );
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.completed_multipart_upload_sequence, 1);
    }
    assert_clean_metadata_command_stream(&map, &[1, 2]);
}

#[test]
fn completed_multipart_order_command_id_race_drains_winner_and_retries() {
    let tmp = test_util::tempdir();
    let node_ids = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec_shape = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(tmp.path(), &node_ids, &[0, 1, 2, 3], ec_shape).unwrap();
    let topology = map
        .node(NodeId::new(0))
        .unwrap()
        .storage_node()
        .pg_topology();
    let bucket = bucket_for_pg(topology, 1, "mpu-order-command-id-race-");
    set_route_primary(&mut map, 1, NodeId::new(1));

    let map = Arc::new(map);
    let cluster = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    let contender = crate::StorageCluster::from_local_map(Arc::clone(&map)).unwrap();
    create_test_bucket(&cluster, &bucket);

    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_once = Arc::new(AtomicBool::new(true));
    let hook_once_for_closure = Arc::clone(&hook_once);
    let _hook_guard = cluster.test_install_before_completed_multipart_order_command_id_hook(
        Arc::new(move || {
            if !hook_once_for_closure.swap(false, Ordering::SeqCst) {
                return;
            }
            let pg_id = PgId::new(1);
            let command = MetadataCommandEnvelope::new(
                contender.next_metadata_command_id(pg_id).unwrap(),
                MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(
                    AdvanceCompletedMultipartUploadSequenceCommand {
                        bucket: hook_bucket.clone(),
                        completion_order: 1,
                    },
                ),
            );
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &command);
        }),
    );

    let completion_order = cluster
        .test_reserve_completed_multipart_upload_order(&bucket)
        .unwrap();

    assert!(!hook_once.load(Ordering::SeqCst));
    assert_eq!(completion_order, 2);
    assert!(pending_metadata_command_for_test(&map, PgId::new(1), &bucket).is_none());
    for node_id in node_ids {
        let pg = map.node(node_id).unwrap().storage_node().get_pg(1).unwrap();
        let info = crate::traits::PgMetadataStore::head_bucket_record_raw(&*pg, &bucket).unwrap();
        assert_eq!(info.completed_multipart_upload_sequence, 2);
    }
    assert_clean_metadata_command_stream(&map, &[1]);
}

#[test]
fn multipart_completion_drains_matching_pending_completion() {
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
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "samecomplete");
    let pg_id = PgId::new(object_pg);
    let bucket_pg_id = cluster.bucket_metadata_pg_id(&bucket);
    let last_modified_millis = 987_657;
    let (pending_completion, write_sequence) = pending_multipart_completion_command_for_test(
        &map,
        &cluster,
        pg_id,
        &req,
        last_modified_millis,
    );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending_completion);

    let outcome = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_eq!(outcome.version_id, crate::VersionId::Null);
    assert_eq!(outcome.live_tags, req.tags);
    assert_eq!(outcome.live_size, req.size);
    assert_eq!(outcome.live_last_modified, last_modified_millis);
    assert!(outcome.stale_payload.is_none());
    expected_segment.version_id = crate::VersionId::Null.to_u64();
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &map,
        &node_ids,
        object_pg,
        &req,
        &expected_segment,
        &outcome,
        write_sequence,
    );
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    for node_id in node_ids {
        let bucket_pg = map
            .node(node_id)
            .unwrap()
            .storage_node()
            .get_pg(bucket_pg_id)
            .unwrap();
        let info =
            crate::traits::PgMetadataStore::head_bucket_record_raw(&*bucket_pg, &bucket).unwrap();
        assert_eq!(
            info.completed_multipart_upload_sequence, 1,
            "same-upload completion retry must not allocate a second order on node {node_id:?}"
        );
    }
    assert_clean_metadata_command_stream(&map, &[bucket_pg_id, object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn multipart_completion_pending_install_conflict_with_matching_completion_returns_success() {
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
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "racecomplete");
    let pg_id = PgId::new(object_pg);
    let bucket_pg_id = cluster.bucket_metadata_pg_id(&bucket);
    let last_modified_millis = 987_659;
    let (pending_completion, write_sequence) = pending_multipart_completion_command_for_test(
        &map,
        &cluster,
        pg_id,
        &req,
        last_modified_millis,
    );

    let hook_ran = Arc::new(AtomicBool::new(false));
    let hook_map = Arc::clone(&map);
    let hook_bucket = bucket.clone();
    let hook_command_template = pending_completion.clone();
    let hook_bucket_pg_id = bucket_pg_id;
    let hook_ran_for_closure = Arc::clone(&hook_ran);
    let _hook_guard =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if hook_ran_for_closure.swap(true, Ordering::SeqCst) {
                return;
            }
            let bucket_primary = hook_map
                .metadata_pg_primary_node(ClusterEpoch::INITIAL, PgId::new(hook_bucket_pg_id))
                .unwrap();
            let bucket_pg = bucket_primary
                .storage_node()
                .get_pg(hook_bucket_pg_id)
                .unwrap();
            let completion_order = bucket_pg
                .completed_multipart_upload_sequence_for_bucket(&hook_bucket)
                .unwrap();
            drop(bucket_pg);
            let mut payload = hook_command_template.payload().clone();
            let MetadataCommandPayload::CommitMultipartObject(commit) = &mut payload else {
                panic!("test command must be a multipart completion");
            };
            commit.completion_order = completion_order;
            let hook_command = MetadataCommandEnvelope::new(hook_command_template.id(), payload);
            insert_pending_metadata_command_for_test(&hook_map, pg_id, &hook_bucket, &hook_command);
        }));

    let outcome = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap();

    assert!(hook_ran.load(Ordering::SeqCst));
    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert_eq!(outcome.version_id, crate::VersionId::Null);
    assert_eq!(outcome.live_tags, req.tags);
    assert_eq!(outcome.live_size, req.size);
    assert_eq!(outcome.live_last_modified, last_modified_millis);
    assert!(outcome.stale_payload.is_none());
    expected_segment.version_id = crate::VersionId::Null.to_u64();
    assert_streamed_multipart_completion_on_acting_nodes_with_write_sequence(
        &map,
        &node_ids,
        object_pg,
        &req,
        &expected_segment,
        &outcome,
        write_sequence,
    );
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    assert_clean_metadata_command_stream(&map, &[bucket_pg_id, object_pg]);
    assert_bucket_write_reservations_released(&map, &bucket);
}

#[test]
fn multipart_completion_drains_other_upload_same_key_and_resnapshots_stale_payload() {
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
    let (first_req, first_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "firstsamekey");
    let (second_req, mut second_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "secondsamekey");
    assert_ne!(first_req.upload_id, second_req.upload_id);
    assert_ne!(first_req.generation_id, second_req.generation_id);
    let pg_id = PgId::new(object_pg);
    let bucket_pg_id = cluster.bucket_metadata_pg_id(&bucket);
    let first_last_modified_millis = 987_658;
    let (pending_first_completion, _first_write_sequence) =
        pending_multipart_completion_command_for_test(
            &map,
            &cluster,
            pg_id,
            &first_req,
            first_last_modified_millis,
        );
    insert_pending_metadata_command_for_test(&map, pg_id, &bucket, &pending_first_completion);

    let second_outcome = cluster
        .complete_multipart_upload_commit_serialized(second_req.clone(), 16)
        .unwrap();

    assert!(pending_metadata_command_for_test(&map, pg_id, &bucket).is_none());
    assert!(
        matches!(
            second_outcome.stale_payload,
            Some(crate::CompletedMultipartStalePayload::Multipart { generation_id, .. })
                if generation_id == first_req.generation_id
        ),
        "second completion should reclaim the first completed upload payload, got {:?}",
        second_outcome.stale_payload
    );
    second_segment.version_id = crate::VersionId::Null.to_u64();
    assert_eq!(
        completed_multipart_order_on_node(
            &map,
            NodeId::new(0),
            object_pg,
            &bucket,
            &first_req.upload_id
        ),
        1
    );
    assert_eq!(
        completed_multipart_order_on_node(
            &map,
            NodeId::new(0),
            object_pg,
            &bucket,
            &second_req.upload_id
        ),
        2
    );
    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let object_pg_store = node.get_pg(object_pg).unwrap();
        let stored =
            crate::PgMetadataStore::get_object_meta(&*object_pg_store, &bucket, &key).unwrap();
        let live = stored.as_live().unwrap();
        assert_eq!(live.generation_id, second_req.generation_id);
        assert_eq!(live.version_id, second_outcome.version_id);
        assert_eq!(live.size, second_req.size);
        assert_eq!(live.last_modified, second_outcome.live_last_modified);
        assert_eq!(
            crate::PgMetadataStore::get_object_parts(
                &*object_pg_store,
                &bucket,
                &key,
                second_outcome.version_id
            )
            .unwrap(),
            vec![crate::ObjectPartRecord {
                bucket: bucket.clone(),
                key: key.clone(),
                version_id: second_outcome.version_id,
                part_number: second_req.part_records[0].part_number,
                size: second_req.part_records[0].size,
                etag: second_req.part_records[0].etag.clone(),
                etag_kind: second_req.part_records[0].etag_kind,
                part_okh: second_req.part_records[0].part_okh,
                part_vid: second_req.part_records[0].part_vid,
                ec_k: second_req.part_records[0].ec_k,
                ec_m: second_req.part_records[0].ec_m,
                data_pg_id: node
                    .pg_topology()
                    .object_generation_multipart_part_data_pg(
                        &bucket,
                        &key,
                        second_req.generation_id,
                        second_req.part_records[0].part_number,
                    )
                    .get(),
                checksum: second_req.part_records[0].checksum.clone(),
            }]
        );
        assert_eq!(
            crate::PgMetadataStore::get_multipart_part_segments(
                &*object_pg_store,
                &bucket,
                &key,
                second_outcome.version_id,
                second_segment.part_number,
            )
            .unwrap(),
            vec![second_segment.clone()]
        );
        assert!(crate::PgMetadataStore::payload_reclaim_exists(
            &*object_pg_store,
            &bucket,
            &key,
            first_req.generation_id
        )
        .unwrap());
        assert_eq!(
            crate::PgMetadataStore::get_all_multipart_part_segments_for_upload(
                &*object_pg_store,
                &first_req.upload_id
            )
            .unwrap(),
            Vec::<crate::MultipartPartSegmentRecord>::new(),
            "first completed upload staging rows should be removed on node {node_id:?}"
        );
        let bucket_pg_store = node.get_pg(bucket_pg_id).unwrap();
        let info =
            crate::traits::PgMetadataStore::head_bucket_record_raw(&*bucket_pg_store, &bucket)
                .unwrap();
        assert_eq!(info.completed_multipart_upload_sequence, 2);
    }
    let mut first_readback = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(
            crate::SegmentStoredBytesRequest {
                data_pg_id: first_segment.data_pg_id,
                segment_okh: first_segment.segment_okh,
                segment_vid: first_segment.segment_vid,
                stored_size: first_segment.size as usize,
                segment_crc64: first_segment.segment_crc64,
                ec: EcShape {
                    k: first_segment.ec_k,
                    m: first_segment.ec_m,
                },
            },
            &mut first_readback,
        )
        .unwrap();
    assert_eq!(first_readback, b"streamed completion");
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &first_req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &second_req.upload_id,
        TerminalMultipartOutcome::Completed,
    );
    assert_clean_metadata_command_stream(&map, &[bucket_pg_id, object_pg]);
}

#[test]
fn multipart_completion_drains_pending_abort_before_completing() {
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
    let (req, uploaded_segment) =
        seed_streamed_multipart_completion(&cluster, &bucket, &key, "abortwinscomplete");

    for shard_index in 0..ec_shape.k + ec_shape.m {
        assert!(cluster
            .test_payload_shard_file_exists(
                uploaded_segment.data_pg_id,
                ec_shape,
                &uploaded_segment.segment_okh,
                uploaded_segment.segment_vid,
                shard_index,
            )
            .unwrap());
    }

    let _serial = lock_metadata_command_apply_hook_test();
    let fail_once = Arc::new(AtomicBool::new(true));
    let hook_upload_id = req.upload_id.clone();
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
                        context: "injected multipart abort before completion failure",
                        source: std::io::Error::other(
                            "injected multipart abort before completion failure",
                        ),
                    });
                }
                _ => {}
            }
            Ok(())
        },
    ));

    let err = cluster
        .abort_multipart_upload(&bucket, &key, &req.upload_id)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "injected multipart abort before completion failure",
                ..
            })
        ),
        "expected injected abort failure, got {err:?}"
    );
    drop(hook_guard);
    assert!(!fail_once.load(Ordering::SeqCst));
    assert!(
        pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_some(),
        "partial abort must remain pending for completion to drain"
    );

    let err = cluster
        .complete_multipart_upload_commit_serialized(req.clone(), 16)
        .unwrap_err();
    assert!(
        matches!(
            err,
            crate::ObjectPgActionError::Metadata(crate::MetadataError::NoSuchUpload { .. })
        ),
        "completion should observe the pending abort as the terminal upload outcome, got {err:?}"
    );
    assert!(pending_metadata_command_for_test(&map, PgId::new(object_pg), &bucket).is_none());

    for node_id in node_ids {
        let node = map.node(node_id).unwrap().storage_node();
        let pg = node.get_pg(object_pg).unwrap();
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &req.upload_id),
            Err(crate::MetadataError::NoSuchUpload { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_object_meta(&*pg, &bucket, &key),
            Err(crate::MetadataError::ObjectNotFound)
        ));
        assert!(pg
            .list_completed_multipart_uploads_for_bucket(bucket.as_str())
            .unwrap()
            .is_empty());
    }
    for shard_index in 0..ec_shape.k + ec_shape.m {
        assert!(
            !cluster
                .test_payload_shard_file_exists(
                    uploaded_segment.data_pg_id,
                    ec_shape,
                    &uploaded_segment.segment_okh,
                    uploaded_segment.segment_vid,
                    shard_index,
                )
                .unwrap(),
            "completion draining abort should delete uploaded part shard {shard_index}"
        );
    }
    assert_terminal_multipart_upload_invariants(
        &map,
        &node_ids,
        object_pg,
        &bucket,
        &key,
        &req.upload_id,
        TerminalMultipartOutcome::Aborted,
    );
    assert_clean_metadata_command_stream(&map, &[object_pg]);
}
