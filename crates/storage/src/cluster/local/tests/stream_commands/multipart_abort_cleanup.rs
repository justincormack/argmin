// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[derive(Clone, Copy)]
enum AbortPath {
    Foreground,
    Recovery,
    Finisher,
}

#[test]
fn multipart_abort_retains_applied_command_until_part_and_session_cleanup_succeeds() {
    let _serial = lock_payload_cleanup_hook_test();
    for path in [
        AbortPath::Foreground,
        AbortPath::Recovery,
        AbortPath::Finisher,
    ] {
        for failed_participant in ["file", "ack"] {
            for fail_active_session in [false, true] {
                check_abort_cleanup(path, failed_participant, fail_active_session);
            }
        }
    }
}

fn check_abort_cleanup(
    path: AbortPath,
    failed_participant: &'static str,
    fail_active_session: bool,
) {
    let tmp = test_util::tempdir();
    let ec = EcShape { k: 2, m: 1 };
    let map = Arc::new(
        LocalClusterMap::open(
            tmp.path(),
            &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
            &[0],
            ec,
        )
        .unwrap(),
    );
    let cluster = current_cluster(&map);
    let bucket = crate::tests::bucket_name("multipart-abort-cleanup");
    let key = crate::tests::object_key("object");
    let upload_id = upload_id_from_label("abortcleanup");
    let session = crate::tests::stream_session_id("abort-cleanup");
    create_test_bucket(&cluster, &bucket);
    let encrypted = crate::tests::sse_s3_multipart_segment_test_fixture(b"active part", 2, 0);
    let upload = create_encrypted_test_multipart_upload(
        &cluster,
        &bucket,
        &key,
        &upload_id,
        encrypted.encryption.clone(),
    );
    let (_, _, committed, _) = upload_streamed_test_multipart_part(
        &cluster,
        &bucket,
        &key,
        &upload_id,
        1,
        [31; 16],
        b"committed part",
    );
    cluster
        .create_upload_part_stream_session(
            &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
            2,
            &session,
        )
        .unwrap();
    let active = append_encrypted_test_multipart_stream_segment(
        &cluster, &bucket, &key, &session, 0, &encrypted,
    );
    assert_encrypted_test_stream_segment_stored(&cluster, &active, &encrypted);
    let subjects = [
        (
            committed.data_pg_id,
            committed.segment_okh,
            committed.segment_vid,
        ),
        (active.data_pg_id, active.segment_okh, active.segment_vid),
    ];
    let keys = subjects
        .iter()
        .flat_map(|(_, okh, vid)| (0..ec.k + ec.m).map(move |i| ShardKey::new(okh, vid.get(), i)))
        .collect::<Vec<_>>();
    let failed_key = keys[if fail_active_session { 3 } else { 0 }].clone();
    for (pg, okh, vid) in &subjects {
        for i in 0..ec.k + ec.m {
            assert!(cluster
                .test_payload_shard_file_exists(*pg, ec, okh, *vid, i)
                .unwrap());
            assert!(cluster
                .test_shard_exists(*pg, &ShardKey::new(okh, vid.get(), i))
                .unwrap());
        }
    }
    let cleanup = map
        .metadata_pg_primary_node(map.epoch, PgId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(0)
        .unwrap()
        .prepare_abort_multipart_upload_cleanup(&bucket, &key, &upload_id)
        .unwrap()
        .unwrap();
    assert_eq!(cleanup.streaming_segments, vec![committed]);
    assert_eq!(cleanup.stream_upload_segments, vec![active]);
    let proof = acquire_test_bucket_write_proof(
        &cluster,
        &bucket,
        "abort-multipart-upload",
        Some(key.as_str()),
    );
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            map.epoch,
            PgId::new(0),
            map.test_next_metadata_command_log_index(PgId::new(0)),
        ),
        MetadataCommandPayload::AbortMultipartUpload(Box::new(AbortMultipartUploadCommand {
            bucket: bucket.clone(),
            key: key.clone(),
            upload_id: upload_id.clone(),
            cleanup,
            bucket_write_reservation: proof,
        })),
    );
    insert_pending_metadata_command_for_test(&map, PgId::new(0), &bucket, &command);
    let files = Arc::new(Mutex::new(Vec::new()));
    let acks = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&files);
    let failed = failed_key.clone();
    let file_hook =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |key| {
            observed.lock().unwrap().push(key.clone());
            if failed_participant == "file" && *key == failed {
                return Err(StoreError::Io {
                    context: "multipart abort cleanup canary",
                    source: std::io::Error::other("injected file failure"),
                });
            }
            Ok(())
        }));
    let observed = Arc::clone(&acks);
    let failed = failed_key.clone();
    let ack_hook = cluster.test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(
        move |key| {
            observed.lock().unwrap().push(key.clone());
            if failed_participant == "ack" && *key == failed {
                return Err(StoreError::Io {
                    context: "multipart abort cleanup canary",
                    source: std::io::Error::other("injected ack failure"),
                });
            }
            Ok(())
        },
    ));
    let finish = || match path {
        AbortPath::Foreground => cluster
            .abort_multipart_upload(&bucket, &key, &upload_id)
            .map(|_| PendingMetadataCommandOutcome::Applied),
        AbortPath::Recovery => cluster
            .drain_pending_metadata_command_with_static_recovery_route(PgId::new(0), &command),
        AbortPath::Finisher => cluster
            .finish_pending_metadata_command_to_acting_set(PgId::new(0), &bucket, &command, false)
            .map_err(crate::cluster::bucket_snapshot_error_to_object_pg_action_error),
    };
    let error = finish().expect_err("unconfirmed multipart cleanup must retain the applied abort");
    assert!(
        matches!(
            error,
            crate::ObjectPgActionError::Store(StoreError::Io {
                context: "multipart abort cleanup canary",
                ..
            })
        ),
        "{error:?}"
    );
    assert_eq!(
        pending_metadata_command_for_test(&map, PgId::new(0), &bucket),
        Some(command.clone())
    );
    assert_eq!(*files.lock().unwrap(), keys);
    assert_eq!(*acks.lock().unwrap(), keys);
    for (pg, okh, vid) in &subjects {
        for i in 0..ec.k + ec.m {
            let key = ShardKey::new(okh, vid.get(), i);
            assert_eq!(
                cluster
                    .test_payload_shard_file_exists(*pg, ec, okh, *vid, i)
                    .unwrap(),
                failed_participant == "file" && key == failed_key
            );
            assert_eq!(
                cluster.test_shard_exists(*pg, &key).unwrap(),
                failed_participant == "ack" && key == failed_key
            );
        }
    }
    let applied = map
        .nodes
        .values()
        .map(|node| {
            let state = node
                .storage_node()
                .get_pg(0)
                .unwrap()
                .metadata_command_replica_state()
                .unwrap();
            (state.applied_log_index, state.applied_log_hash)
        })
        .collect::<Vec<_>>();
    drop(file_hook);
    drop(ack_hook);
    assert_eq!(finish().unwrap(), PendingMetadataCommandOutcome::Applied);
    assert!(pending_metadata_command_for_test(&map, PgId::new(0), &bucket).is_none());
    for (node, expected) in map.nodes.values().zip(applied) {
        let pg = node.storage_node().get_pg(0).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!((state.applied_log_index, state.applied_log_hash), expected);
        assert!(matches!(
            crate::PgMetadataStore::get_multipart_upload(&*pg, &upload_id),
            Err(MetadataError::NoSuchUpload { .. })
        ));
    }
    for (pg, okh, vid) in &subjects {
        for i in 0..ec.k + ec.m {
            assert!(!cluster
                .test_payload_shard_file_exists(*pg, ec, okh, *vid, i)
                .unwrap());
            assert!(!cluster
                .test_shard_exists(*pg, &ShardKey::new(okh, vid.get(), i))
                .unwrap());
        }
    }
}
