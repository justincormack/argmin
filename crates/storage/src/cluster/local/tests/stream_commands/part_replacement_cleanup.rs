// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::PgMetadataStore;

#[test]
fn stream_part_replacement_retains_cleanup_until_all_displaced_participants_confirm() {
    let _serial = lock_payload_cleanup_hook_test();
    for recovery in [false, true] {
        for failed_participant in ["file", "ack"] {
            check_replacement_cleanup(recovery, failed_participant);
        }
    }
}

fn check_replacement_cleanup(recovery: bool, failed_participant: &'static str) {
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
    let bucket = crate::tests::bucket_name("replacement-cleanup");
    let key = crate::tests::object_key("object");
    let upload_id = upload_id_from_label("replacementcleanup");
    create_test_bucket(&cluster, &bucket);
    let old_encrypted = [
        crate::tests::sse_s3_multipart_segment_test_fixture(b"first old segment", 1, 0),
        crate::tests::sse_s3_multipart_segment_test_fixture(b"second old segment", 1, 1),
    ];
    let upload = crate::AuthorizedMultipartUploadRecord::assume_authorized(
        create_encrypted_test_multipart_upload(
            &cluster,
            &bucket,
            &key,
            &upload_id,
            old_encrypted[0].encryption.clone(),
        ),
    );
    let old_session = crate::tests::stream_session_id("old-part");
    cluster
        .create_upload_part_stream_session(&upload, 1, &old_session)
        .unwrap();
    let old_segments = old_encrypted
        .iter()
        .enumerate()
        .map(|(index, encrypted)| {
            let segment = append_encrypted_test_multipart_stream_segment(
                &cluster,
                &bucket,
                &key,
                &old_session,
                u32::try_from(index).unwrap(),
                encrypted,
            );
            assert_encrypted_test_stream_segment_stored(&cluster, &segment, encrypted);
            segment
        })
        .collect::<Vec<_>>();
    let old_size = old_encrypted.iter().map(|e| e.logical_size).sum();
    let old_crc = checksum::crc64::combine(
        old_encrypted[0].payload_crc64,
        old_encrypted[1].payload_crc64,
        old_encrypted[1].logical_size,
    );
    cluster
        .finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(&upload_id, &old_session, 1, old_size, old_crc),
            |_| {
                Ok::<_, ()>(crate::PreparedStreamPartCommit {
                    value: (),
                    last_modified: 123,
                    checksum: None,
                })
            },
        )
        .unwrap()
        .unwrap();
    let old_part = map
        .metadata_pg_primary_node(map.epoch, PgId::new(0))
        .unwrap()
        .storage_node()
        .get_pg(0)
        .unwrap()
        .get_multipart_part(&upload_id, 1)
        .unwrap();
    // A different completed part must remain available for eventual completion.
    let (_, canary_part, canary_segment, canary_encrypted) = upload_streamed_test_multipart_part(
        &cluster,
        &bucket,
        &key,
        &upload_id,
        2,
        [41; 16],
        b"unrelated completed part",
    );
    let replacement_session = crate::tests::stream_session_id("replacement-part");
    let replacement_encrypted =
        crate::tests::sse_s3_multipart_segment_test_fixture(b"replacement segment", 1, 0);
    cluster
        .create_upload_part_stream_session(&upload, 1, &replacement_session)
        .unwrap();
    let replacement = append_encrypted_test_multipart_stream_segment(
        &cluster,
        &bucket,
        &key,
        &replacement_session,
        0,
        &replacement_encrypted,
    );
    let keys = old_segments
        .iter()
        .flat_map(|segment| {
            (0..ec.k + ec.m)
                .map(move |i| ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i))
        })
        .collect::<Vec<_>>();
    for segment in old_segments.iter().chain(std::iter::once(&replacement)) {
        for i in 0..ec.k + ec.m {
            assert!(cluster
                .test_payload_shard_file_exists(
                    segment.data_pg_id,
                    ec,
                    &segment.segment_okh,
                    segment.segment_vid,
                    i
                )
                .unwrap());
            assert!(cluster
                .test_shard_exists(
                    segment.data_pg_id,
                    &ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i)
                )
                .unwrap());
        }
    }
    let failed_key = keys[0].clone();
    let files = Arc::new(Mutex::new(Vec::new()));
    let acks = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&files);
    let failed = failed_key.clone();
    let file_hook =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |key| {
            observed.lock().unwrap().push(key.clone());
            if failed_participant == "file" && *key == failed {
                return Err(StoreError::Io {
                    context: "part replacement cleanup canary",
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
                    context: "part replacement cleanup canary",
                    source: std::io::Error::other("injected ack failure"),
                });
            }
            Ok(())
        },
    ));
    let foreground = || {
        cluster.finalize_upload_part_stream(
            &bucket,
            &key,
            stream_part_finalize_input(
                &upload_id,
                &replacement_session,
                1,
                replacement_encrypted.logical_size,
                replacement_encrypted.payload_crc64,
            ),
            |_| {
                Ok::<_, ()>(crate::PreparedStreamPartCommit {
                    value: (),
                    last_modified: 124,
                    checksum: None,
                })
            },
        )
    };
    let assert_error = |error: crate::ObjectPgActionError| {
        assert!(
            matches!(
                error,
                crate::ObjectPgActionError::Store(StoreError::Io {
                    context: "part replacement cleanup canary",
                    ..
                })
            ),
            "{error:?}"
        );
    };
    assert_error(
        foreground().expect_err("failed displaced cleanup must retain publication custody"),
    );
    let pending = pending_metadata_command_for_test(&map, PgId::new(0), &bucket).unwrap();
    let MetadataCommandPayload::CommitStreamPart(commit) = pending.payload() else {
        panic!("expected stream part commit")
    };
    assert_eq!(commit.existing_part.as_ref(), Some(&old_part));
    assert_eq!(commit.displaced_segments.len(), old_segments.len());
    assert_eq!(commit.part.generation, old_part.generation + 1);
    assert_eq!(*files.lock().unwrap(), keys);
    assert_eq!(*acks.lock().unwrap(), keys);
    let check_payloads = |failed: bool| {
        for segment in &old_segments {
            for i in 0..ec.k + ec.m {
                let key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i);
                assert_eq!(
                    cluster
                        .test_payload_shard_file_exists(
                            segment.data_pg_id,
                            ec,
                            &segment.segment_okh,
                            segment.segment_vid,
                            i
                        )
                        .unwrap(),
                    failed && failed_participant == "file" && key == failed_key
                );
                assert_eq!(
                    cluster.test_shard_exists(segment.data_pg_id, &key).unwrap(),
                    failed && failed_participant == "ack" && key == failed_key
                );
            }
        }
        assert_encrypted_test_stream_segment_stored(&cluster, &replacement, &replacement_encrypted);
        for (pg, okh, vid) in [
            (
                replacement.data_pg_id,
                replacement.segment_okh,
                replacement.segment_vid,
            ),
            (
                canary_segment.data_pg_id,
                canary_segment.segment_okh,
                canary_segment.segment_vid,
            ),
        ] {
            for i in 0..ec.k + ec.m {
                assert!(cluster
                    .test_payload_shard_file_exists(pg, ec, &okh, vid, i)
                    .unwrap());
                assert!(cluster
                    .test_shard_exists(pg, &ShardKey::new(&okh, vid.get(), i))
                    .unwrap());
            }
        }
        let mut bytes = Vec::new();
        cluster
            .read_segment_payload_stored_bytes_into(
                crate::SegmentStoredBytesRequest {
                    data_pg_id: canary_segment.data_pg_id,
                    segment_okh: canary_segment.segment_okh,
                    segment_vid: canary_segment.segment_vid,
                    stored_size: canary_encrypted.storage_bytes.len(),
                    segment_crc64: canary_segment.segment_crc64,
                    ec,
                },
                &mut bytes,
            )
            .unwrap();
        assert_eq!(bytes, canary_encrypted.storage_bytes);
    };
    check_payloads(true);
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
    let finish = || {
        if recovery {
            cluster
                .drain_pending_metadata_command_with_static_recovery_route(PgId::new(0), &pending)
        } else {
            cluster
                .finish_pending_metadata_command_to_acting_set(
                    PgId::new(0),
                    &bucket,
                    &pending,
                    false,
                )
                .map_err(crate::cluster::bucket_snapshot_error_to_object_pg_action_error)
        }
    };
    files.lock().unwrap().clear();
    acks.lock().unwrap().clear();
    assert_error(
        finish().expect_err("recovery must retain the exact applied command on cleanup failure"),
    );
    assert_eq!(
        pending_metadata_command_for_test(&map, PgId::new(0), &bucket),
        Some(pending.clone())
    );
    assert_eq!(*files.lock().unwrap(), keys);
    assert_eq!(*acks.lock().unwrap(), keys);
    drop(file_hook);
    drop(ack_hook);
    assert_eq!(finish().unwrap(), PendingMetadataCommandOutcome::Applied);
    assert!(pending_metadata_command_for_test(&map, PgId::new(0), &bucket).is_none());
    check_payloads(false);
    for (node, expected) in map.nodes.values().zip(applied) {
        let pg = node.storage_node().get_pg(0).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!((state.applied_log_index, state.applied_log_hash), expected);
        assert_eq!(pg.get_multipart_part(&upload_id, 1).unwrap(), commit.part);
        assert_eq!(pg.get_multipart_part(&upload_id, 2).unwrap(), canary_part);
        assert_eq!(
            pg.get_multipart_part_segments_for_upload_part(&bucket, &key, &upload_id, 1)
                .unwrap(),
            commit.segments
        );
        assert!(matches!(
            pg.get_stream_upload(&replacement_session),
            Err(MetadataError::StreamSessionNotFound { .. })
        ));
    }
}
