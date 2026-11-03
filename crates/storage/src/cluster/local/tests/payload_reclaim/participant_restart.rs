// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

type ReclaimRecords = (
    Option<crate::types::ObjectSegmentsReclaimRecord>,
    Option<crate::types::MultipartReclaimRecord>,
);

fn assert_reclaim_records(
    map: &LocalClusterMap,
    bucket: &BucketName,
    key: &ObjectKey,
    generation: GenerationId,
    expected: &ReclaimRecords,
) {
    for node in map.nodes.values() {
        let pg = node.storage_node().get_pg(0).unwrap();
        assert_eq!(
            (
                PgMetadataStore::get_object_segments_reclaim(&*pg, bucket, key, generation)
                    .unwrap(),
                PgMetadataStore::get_multipart_reclaim(&*pg, bucket, key, generation).unwrap(),
            ),
            *expected,
            "reclaim owner differs on {:?}",
            node.node_id(),
        );
        assert!(PgMetadataStore::object_payload_reclaim_claim(&*pg)
            .unwrap()
            .is_none());
    }
    assert!(pending_metadata_command_for_test(map, PgId::new(0), bucket).is_none());
}

fn assert_present_participants(
    cluster: &StorageCluster,
    map: &LocalClusterMap,
    reads: &[crate::SegmentStoredBytesRequest],
) {
    for read in reads {
        let primary = map
            .metadata_pg_primary_node(cluster.operation_epoch(), PgId::new(read.data_pg_id))
            .unwrap();
        for index in 0..read.ec.k + read.ec.m {
            let key = ShardKey::new(&read.segment_okh, read.segment_vid.get(), index);
            assert!(
                cluster
                    .test_payload_shard_file_exists(
                        read.data_pg_id,
                        read.ec,
                        &read.segment_okh,
                        read.segment_vid,
                        index,
                    )
                    .unwrap(),
                "missing file {key:?}"
            );
            assert!(
                primary
                    .storage_node()
                    .test_shard_exists(read.data_pg_id, &key)
                    .unwrap(),
                "missing acknowledgement {key:?}"
            );
        }
    }
}

fn canary_read_request(canary: &CommittedDirectSegment) -> crate::SegmentStoredBytesRequest {
    crate::SegmentStoredBytesRequest {
        data_pg_id: canary.written.data_pg_id,
        segment_okh: canary.segment_okh,
        segment_vid: canary.generation_id,
        stored_size: canary.encrypted.storage_bytes.len(),
        segment_crc64: canary.encrypted.storage_crc64,
        ec: canary.written.ec,
    }
}

fn assert_participants(
    cluster: &StorageCluster,
    map: &LocalClusterMap,
    reads: &[crate::SegmentStoredBytesRequest],
    expected_file: Option<&ShardKey>,
    expected_ack: Option<&ShardKey>,
) {
    for read in reads {
        let primary = map
            .metadata_pg_primary_node(cluster.operation_epoch(), PgId::new(read.data_pg_id))
            .unwrap();
        for index in 0..read.ec.k + read.ec.m {
            let key = ShardKey::new(&read.segment_okh, read.segment_vid.get(), index);
            assert_eq!(
                cluster
                    .test_payload_shard_file_exists(
                        read.data_pg_id,
                        read.ec,
                        &read.segment_okh,
                        read.segment_vid,
                        index,
                    )
                    .unwrap(),
                expected_file == Some(&key),
                "unexpected file state for {key:?}",
            );
            assert_eq!(
                primary
                    .storage_node()
                    .test_shard_exists(read.data_pg_id, &key)
                    .unwrap(),
                expected_ack == Some(&key),
                "unexpected acknowledgement state for {key:?}",
            );
        }
    }
}

fn check_reclaim_restart(multipart: bool, fail_file: bool, fail_ack: bool, after_effect: bool) {
    let tmp = test_util::tempdir();
    let nodes = [NodeId::new(0), NodeId::new(1), NodeId::new(2)];
    let ec = EcShape { k: 2, m: 1 };
    let bucket = crate::tests::bucket_name("reclaim-restart");
    let key = crate::tests::object_key("target");
    let canary_key = crate::tests::object_key("canary");
    let (generation, reads, original, index, canary, canary_meta, old_map) = {
        let mut map = LocalClusterMap::open(tmp.path(), &nodes, &[0], ec).unwrap();
        set_route_primary(&mut map, 0, NodeId::new(2));
        let map = Arc::new(map);
        let cluster = current_cluster(&map);
        create_test_bucket(&cluster, &bucket);
        // These helpers verify real ciphertext before namespace removal, not
        // synthetic SSE metadata or reads with plaintext geometry.
        let (generation, reads) = publish_segments(&cluster, &bucket, &key, multipart);
        let canary = write_committed_direct_segment_for_with_versioning(
            &cluster,
            &bucket,
            &canary_key,
            crate::BucketVersioningState::Disabled,
            [0xD1; 16],
            [0xD2; 16],
            b"live same-PG canary across cleanup restart",
        );
        let canary_meta = cluster.test_get_object_meta(&bucket, &canary_key).unwrap();
        assert_present_participants(&cluster, &map, &reads);
        assert_present_participants(&cluster, &map, &[canary_read_request(&canary)]);
        cluster
            .delete_current_object_if(&bucket, &key, |_| Ok::<_, ()>(()))
            .unwrap()
            .unwrap();
        let original = {
            let pg = map
                .node(NodeId::new(2))
                .unwrap()
                .storage_node()
                .get_pg(0)
                .unwrap();
            (
                PgMetadataStore::get_object_segments_reclaim(&*pg, &bucket, &key, generation)
                    .unwrap(),
                PgMetadataStore::get_multipart_reclaim(&*pg, &bucket, &key, generation).unwrap(),
            )
        };
        assert_eq!(original.0.is_some(), !multipart);
        assert_eq!(original.1.is_some(), multipart);
        let index = map.test_next_metadata_command_log_index(PgId::new(0));
        let failed = ShardKey::new(&reads[0].segment_okh, reads[0].segment_vid.get(), 0);

        let file_map = Arc::clone(&map);
        let file_key = failed.clone();
        let first = reads[0];
        let file_hook =
            cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |key| {
                if fail_file && *key == file_key {
                    if after_effect {
                        // Complete the real scoped deletion, then withhold its
                        // successful completion at the orchestration boundary.
                        file_map
                            .open_placed_segment_shard_deleter(
                                ClusterEpoch::INITIAL,
                                file_map.data_pg(PgId::new(first.data_pg_id)).unwrap(),
                                first.ec,
                                &first.segment_okh,
                                first.segment_vid,
                            )
                            .unwrap()
                            .delete_matching_key(key)
                            .unwrap();
                    }
                    return Err(StoreError::Io {
                        context: "injected reclaim restart file completion failure",
                        source: std::io::Error::other("injected"),
                    });
                }
                Ok(())
            }));
        let ack_cluster = Arc::clone(&cluster);
        let ack_key = failed.clone();
        let ack_hook = cluster.test_install_before_metadata_primary_payload_ack_delete_hook(
            Arc::new(move |key| {
                if fail_ack && *key == ack_key {
                    if after_effect {
                        ack_cluster
                            .metadata_pg_primary_shard_ack_route(
                                ack_cluster
                                    .validated_data_pg(PgId::new(first.data_pg_id))
                                    .unwrap(),
                            )
                            .unwrap()
                            .delete_shard_ack(key)
                            .unwrap();
                    }
                    return Err(StoreError::Io {
                        context: "injected reclaim restart acknowledgement completion failure",
                        source: std::io::Error::other("injected"),
                    });
                }
                Ok(())
            }),
        );
        let error = cluster
            .reclaim_object_payload_if_unleased(&bucket, &key, generation)
            .unwrap_err();
        assert!(
            matches!(error, ObjectPgActionError::Store(StoreError::Io { context, .. })
            if context == if fail_file { "injected reclaim restart file completion failure" }
                else { "injected reclaim restart acknowledgement completion failure" })
        );
        assert_reclaim_records(&map, &bucket, &key, generation, &original);
        assert_eq!(
            map.test_next_metadata_command_log_index(PgId::new(0)),
            index
        );
        assert_participants(
            &cluster,
            &map,
            &reads,
            (fail_file && !after_effect).then_some(&failed),
            (fail_ack && !after_effect).then_some(&failed),
        );
        assert_present_participants(&cluster, &map, &[canary_read_request(&canary)]);
        drop(file_hook);
        drop(ack_hook);
        (
            generation,
            reads,
            original,
            index,
            canary,
            canary_meta,
            Arc::downgrade(&map),
        )
    };
    // Do not accidentally test a second route view over a still-live runtime.
    assert!(
        old_map.upgrade().is_none(),
        "original runtime must be closed before reopening"
    );
    let mut map = LocalClusterMap::open(tmp.path(), &nodes, &[0], ec).unwrap();
    set_route_primary(&mut map, 0, NodeId::new(2));
    let map = Arc::new(map);
    let cluster = current_cluster(&map);
    assert_reclaim_records(&map, &bucket, &key, generation, &original);
    assert_eq!(
        map.test_next_metadata_command_log_index(PgId::new(0)),
        index
    );
    let failed = ShardKey::new(&reads[0].segment_okh, reads[0].segment_vid.get(), 0);
    assert_participants(
        &cluster,
        &map,
        &reads,
        (fail_file && !after_effect).then_some(&failed),
        (fail_ack && !after_effect).then_some(&failed),
    );

    assert!(
        cluster.try_take_reclaim_work().is_none(),
        "restart must lose the process-local hint"
    );
    assert_eq!(
        cluster
            .enqueue_durable_object_payload_reclaim_roots_excluding(&HashSet::new())
            .queued,
        1
    );
    let Some(crate::ReclaimWorkItem::ObjectPayload((queued_bucket, queued_key, queued_generation))) =
        cluster.try_take_reclaim_work()
    else {
        panic!("durable scan must rediscover the exact owner")
    };
    assert_eq!(
        (&queued_bucket, &queued_key, queued_generation),
        (&bucket, &key, generation)
    );

    let files = Arc::new(Mutex::new(Vec::new()));
    let acks = Arc::new(Mutex::new(Vec::new()));
    let file_calls = Arc::clone(&files);
    let _file_hook =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |key| {
            file_calls.lock().unwrap().push(key.clone());
            Ok(())
        }));
    let ack_calls = Arc::clone(&acks);
    let _ack_hook = cluster.test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(
        move |key| {
            ack_calls.lock().unwrap().push(key.clone());
            Ok(())
        },
    ));
    assert!(cluster
        .reclaim_object_payload_if_unleased(&queued_bucket, &queued_key, queued_generation)
        .unwrap());
    let expected: Vec<_> = reads
        .iter()
        .flat_map(|read| {
            (0..ec.k + ec.m)
                .map(move |index| ShardKey::new(&read.segment_okh, read.segment_vid.get(), index))
        })
        .collect();
    assert_eq!(expected.len(), 6);
    assert_eq!(
        *files.lock().unwrap(),
        expected,
        "restart must reconfirm every file, even when all names are absent"
    );
    assert_eq!(
        *acks.lock().unwrap(),
        expected,
        "restart must reconfirm every acknowledgement"
    );
    assert_reclaim_records(&map, &bucket, &key, generation, &(None, None));
    assert_participants(&cluster, &map, &reads, None, None);
    assert_present_participants(&cluster, &map, &[canary_read_request(&canary)]);
    assert_eq!(
        cluster.test_get_object_meta(&bucket, &canary_key).unwrap(),
        canary_meta
    );
    let mut bytes = Vec::new();
    cluster
        .read_segment_payload_stored_bytes_into(canary_read_request(&canary), &mut bytes)
        .unwrap();
    assert_eq!(bytes, canary.encrypted.storage_bytes);
    let completed_index = map.test_next_metadata_command_log_index(PgId::new(0));
    assert!(!cluster
        .reclaim_object_payload_if_unleased(&bucket, &key, generation)
        .unwrap());
    assert_eq!(
        map.test_next_metadata_command_log_index(PgId::new(0)),
        completed_index
    );
    assert_clean_metadata_command_stream(&map, &[0]);
}

#[test]
fn segmented_reclaim_restart_reconfirms_partial_and_unconfirmed_cleanup() {
    let _serial = lock_payload_cleanup_hook_test();
    for after_effect in [false, true] {
        for (file, ack) in [(true, false), (false, true), (true, true)] {
            check_reclaim_restart(false, file, ack, after_effect);
        }
    }
}

#[test]
fn multipart_reclaim_restart_reconfirms_partial_and_unconfirmed_cleanup() {
    let _serial = lock_payload_cleanup_hook_test();
    for after_effect in [false, true] {
        for (file, ack) in [(true, false), (false, true), (true, true)] {
            check_reclaim_restart(true, file, ack, after_effect);
        }
    }
}
