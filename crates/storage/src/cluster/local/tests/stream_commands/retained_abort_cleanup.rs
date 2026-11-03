// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn retained_stream_abort_attempts_all_participants_before_retry_for_both_targets() {
    let _serial = lock_payload_cleanup_hook_test();
    for multipart in [false, true] {
        for failed_participant in [None, Some("file"), Some("ack")] {
            check_retained_abort(multipart, failed_participant);
        }
    }
}

fn check_retained_abort(multipart: bool, failed_participant: Option<&'static str>) {
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
    let bucket = crate::tests::bucket_name("retained-cleanup");
    let key = crate::tests::object_key("object");
    let session = crate::tests::stream_session_id("retained-cleanup");
    create_test_bucket(&cluster, &bucket);
    let handle = crate::StorageClusterRouteHandle::from_authorized_cluster(Arc::clone(&cluster));
    let admission = handle.admit_current_route().unwrap();
    let cleanup = admission
        .retained_stream_upload_cleanup(&bucket, &key)
        .unwrap();
    let encrypted = [
        b"first encrypted segment".as_slice(),
        b"second encrypted segment".as_slice(),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, bytes)| {
        if multipart {
            crate::tests::sse_s3_multipart_segment_test_fixture(
                bytes,
                1,
                u32::try_from(index).unwrap(),
            )
        } else {
            crate::tests::sse_s3_segment_test_fixture(bytes, u32::try_from(index).unwrap())
        }
    })
    .collect::<Vec<_>>();
    if multipart {
        let upload = create_encrypted_test_multipart_upload(
            &cluster,
            &bucket,
            &key,
            &upload_id_from_label("retainedcleanup"),
            encrypted[0].encryption.clone(),
        );
        cluster
            .create_upload_part_stream_session(
                &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
                1,
                &session,
            )
            .unwrap();
    } else {
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session,
                encrypted[0].encryption.clone(),
            )
            .unwrap();
    }
    let segments = encrypted
        .iter()
        .enumerate()
        .map(|(index, encrypted)| {
            // This helper uses the shared production prepare/write/append path for
            // either stream target; each fixture uses its target's encryption scope.
            let segment = append_encrypted_test_multipart_stream_segment(
                &cluster,
                &bucket,
                &key,
                &session,
                u32::try_from(index).unwrap(),
                encrypted,
            );
            assert_encrypted_test_stream_segment_stored(&cluster, &segment, encrypted);
            segment
        })
        .collect::<Vec<_>>();
    drop(admission);
    let expected_keys = segments
        .iter()
        .flat_map(|segment| {
            (0..ec.k + ec.m).map(move |index| {
                ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), index)
            })
        })
        .collect::<Vec<_>>();
    let failed_key = expected_keys[0].clone();
    let files = Arc::new(Mutex::new(Vec::new()));
    let acks = Arc::new(Mutex::new(Vec::new()));
    let observed_files = Arc::clone(&files);
    let observed_acks = Arc::clone(&acks);
    let file_key = failed_key.clone();
    let ack_key = failed_key.clone();
    let _trace = observability::AttachedTrace::new(observability::TraceContext::new_request());
    let reports = Arc::new(AtomicUsize::new(0));
    let observed_reports = Arc::clone(&reports);
    let _observer = cluster.test_install_best_effort_payload_cleanup_error_hook(Arc::new(
        move |operation, error| {
            let (expected_operation, expected_context) = match failed_participant {
                Some("file") => (
                    "delete placed payload shard",
                    "retained abort file cleanup canary",
                ),
                Some("ack") => ("delete payload ack", "retained abort ack cleanup canary"),
                _ => panic!("unexpected cleanup diagnostic: {operation}: {error:?}"),
            };
            assert_eq!(operation, expected_operation);
            assert!(
                matches!(error, StoreError::Io { context, .. } if *context == expected_context)
            );
            observed_reports.fetch_add(1, Ordering::SeqCst);
        },
    ));
    let file_hook =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |key| {
            observed_files.lock().unwrap().push(key.clone());
            if failed_participant == Some("file") && *key == file_key {
                return Err(StoreError::Io {
                    context: "retained abort file cleanup canary",
                    source: std::io::Error::other("injected"),
                });
            }
            Ok(())
        }));
    let ack_hook = cluster.test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(
        move |key| {
            observed_acks.lock().unwrap().push(key.clone());
            if failed_participant == Some("ack") && *key == ack_key {
                return Err(StoreError::Io {
                    context: "retained abort ack cleanup canary",
                    source: std::io::Error::other("injected"),
                });
            }
            Ok(())
        },
    ));
    let result = cleanup.abort(&session);
    if failed_participant.is_some() {
        let error = result.unwrap_err();
        assert_eq!(error.diagnostic_cause_label(), "store_io_failure");
        let pending = pending_metadata_command_for_test(&map, PgId::new(0), &bucket).unwrap();
        let MetadataCommandPayload::AbortStreamUpload(abort) = pending.payload() else {
            panic!("expected retained abort")
        };
        assert_eq!(abort.staged_segments, segments);
        assert_eq!(abort.session_id, session);
    } else {
        result.unwrap();
        assert!(pending_metadata_command_for_test(&map, PgId::new(0), &bucket).is_none());
    }
    assert_eq!(
        *files.lock().unwrap(),
        expected_keys,
        "one file pass includes both segments"
    );
    assert_eq!(
        *acks.lock().unwrap(),
        expected_keys,
        "one acknowledgement pass includes both segments"
    );
    assert_eq!(
        reports.load(Ordering::SeqCst),
        usize::from(failed_participant.is_some())
    );
    for segment in &segments {
        for index in 0..ec.k + ec.m {
            let key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), index);
            assert_eq!(
                cluster
                    .test_payload_shard_file_exists(
                        segment.data_pg_id,
                        ec,
                        &segment.segment_okh,
                        segment.segment_vid,
                        index
                    )
                    .unwrap(),
                failed_participant == Some("file") && key == failed_key
            );
            assert_eq!(
                cluster.test_shard_exists(segment.data_pg_id, &key).unwrap(),
                failed_participant == Some("ack") && key == failed_key
            );
        }
    }
    drop(file_hook);
    drop(ack_hook);
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
    // The same capability reuses the exact pending abort after the transient
    // participant failure. No new client session or reservation is created.
    cleanup.abort(&session).unwrap();
    cleanup.abort(&session).unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(0), &bucket).is_none());
    for segment in &segments {
        for index in 0..ec.k + ec.m {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), index);
            assert!(!cluster
                .test_payload_shard_file_exists(
                    segment.data_pg_id,
                    ec,
                    &segment.segment_okh,
                    segment.segment_vid,
                    index
                )
                .unwrap());
            assert!(!cluster
                .test_shard_exists(segment.data_pg_id, &shard_key)
                .unwrap());
        }
    }
    for (node, expected_applied) in map.nodes.values().zip(applied) {
        let pg = node.storage_node().get_pg(0).unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(
            (state.applied_log_index, state.applied_log_hash),
            expected_applied,
            "retry must reuse the already-applied abort"
        );
        assert!(matches!(
            crate::PgMetadataStore::get_stream_upload(&*pg, &session),
            Err(MetadataError::StreamSessionNotFound { .. })
        ));
        assert!(matches!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg, &bucket, &key, &session
            ),
            Err(MetadataError::ObjectGenerationReservationNotFound { .. })
        ));
    }
}
