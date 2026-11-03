// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::types::AdmittedRouteEffectFence;
use crate::ObjectPgActionError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailurePoint {
    LeaseAfterWrite,
    BeforeAckRegistration,
    BeforePendingInstall,
}

#[test]
fn reference_checked_stream_append_cleanup_reports_each_failure_once() {
    let _serial = lock_payload_cleanup_hook_test();
    for fail_file in [true, false] {
        let fixture = stream_payload_cleanup_fixture("b1", b"observed cleanup failure");
        let cluster = &fixture.cluster;
        let (target, segment) = cluster
            .prepare_stream_segment_append(
                &fixture.bucket,
                &fixture.key,
                &crate::PrepareStreamUploadSegmentAppendReq {
                    session_id: fixture.session_id.clone(),
                    segment_index: 0,
                    size: fixture.encrypted.logical_size,
                    segment_crc64: fixture.encrypted.storage_crc64,
                    payload_crc64: fixture.encrypted.payload_crc64,
                },
            )
            .unwrap();
        let written = cluster
            .write_stream_segment_payload_shards(&segment, &fixture.encrypted.storage_bytes)
            .unwrap();
        let shard_batch = written
            .iter()
            .map(|written| (&written.key, written.ack))
            .collect::<Vec<_>>();
        let stop = Arc::new(AtomicBool::new(false));
        let hook_stop = Arc::clone(&stop);
        let _install_boundary = cluster.test_install_before_metadata_command_pending_install_hook(
            Arc::new(move || {
                hook_stop.store(true, Ordering::SeqCst);
            }),
        );
        let _trace = observability::AttachedTrace::new(observability::TraceContext::new_request());
        // These curated scenarios validate the exact operation label as well as
        // counting events. A second wrapper event must not be accepted.
        let failure = if fail_file {
            cluster.test_fail_placed_payload_shard_cleanup()
        } else {
            cluster.test_fail_payload_ack_cleanup()
        };
        let error = cluster
            .commit_stream_segment_append_with_route_validation(
                crate::cluster::PutObjectMutationEffectRoute {
                    bucket_pg_id: cluster.bucket_metadata_pg(&fixture.bucket),
                    object_pg_id: cluster.object_metadata_pg(&fixture.bucket, &fixture.key),
                    bucket: &fixture.bucket,
                    key: &fixture.key,
                    effect_fence: AdmittedRouteEffectFence::unbounded(cluster.operation_epoch()),
                },
                StreamAppendCommitRequest {
                    bucket: &fixture.bucket,
                    key: &fixture.key,
                    session_id: &fixture.session_id,
                    segment_index: 0,
                    segment_record: &segment,
                    stored_segment_size: crate::cluster::StreamAppendStoredSegmentSize::Exact(
                        fixture.encrypted.storage_bytes.len() as u64,
                    ),
                    shard_batch: &shard_batch,
                },
                &target,
                || {
                    if stop.load(Ordering::SeqCst) {
                        Err(StoreError::NotFound)
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
        assert!(
            stop.load(Ordering::SeqCst),
            "must reach reference-checked cleanup"
        );
        let context = if fail_file {
            "injected placed cleanup delete failure"
        } else {
            "injected ack cleanup delete failure"
        };
        assert!(
            matches!(error, ObjectPgActionError::Store(StoreError::Io { context: actual, .. }) if actual == context),
            "{error:?}"
        );
        assert_eq!(
            failure.invocation_count(),
            usize::from(fixture.ec.k + fixture.ec.m)
        );
        assert_stream_payload_shard_state(&fixture, !fail_file, fail_file);
    }
}

#[test]
fn stream_append_cleanup_errors_preserve_session_ownership_for_both_targets() {
    let _serial = lock_payload_cleanup_hook_test();
    for multipart in [false, true] {
        for point in [
            FailurePoint::LeaseAfterWrite,
            FailurePoint::BeforeAckRegistration,
            FailurePoint::BeforePendingInstall,
        ] {
            for failed_participant in [None, Some("file"), Some("ack")] {
                check_cleanup(multipart, point, failed_participant);
            }
        }
    }
}

fn check_cleanup(multipart: bool, point: FailurePoint, failed_participant: Option<&'static str>) {
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
    let bucket = crate::tests::bucket_name("append-cleanup");
    let key = crate::tests::object_key("object");
    let session_id = crate::tests::stream_session_id("append-cleanup");
    create_test_bucket(&cluster, &bucket);
    let plaintext = b"encrypted append needing foreground cleanup";
    let encrypted = if multipart {
        crate::tests::sse_s3_multipart_segment_test_fixture(plaintext, 1, 0)
    } else {
        crate::tests::sse_s3_segment_test_fixture(plaintext, 0)
    };
    if multipart {
        let upload = create_encrypted_test_multipart_upload(
            &cluster,
            &bucket,
            &key,
            &upload_id_from_label("appendcleanupupload"),
            encrypted.encryption.clone(),
        );
        cluster
            .create_upload_part_stream_session(
                &crate::AuthorizedMultipartUploadRecord::assume_authorized(upload),
                1,
                &session_id,
            )
            .unwrap();
    } else {
        cluster
            .create_put_object_stream_session_record(
                &bucket,
                &key,
                &session_id,
                encrypted.encryption.clone(),
            )
            .unwrap();
    }
    let session_before = cluster
        .load_stream_upload_session(&bucket, &key, &session_id)
        .unwrap();
    let generation = cluster
        .test_object_generation_reservation_for(&bucket, &key, &session_id)
        .unwrap();
    let expected = match &session_before.target {
        crate::StreamUploadTarget::PutObject => crate::ExpectedStreamAppendState::PutObject {
            generation_id: generation,
            session: session_before.clone(),
        },
        crate::StreamUploadTarget::UploadPart { upload_id, .. } => {
            crate::ExpectedStreamAppendState::UploadPart {
                session: session_before.clone(),
                upload: Box::new(
                    cluster
                        .load_in_progress_multipart_upload(&bucket, &key, upload_id)
                        .unwrap(),
                ),
                part_generation_id: generation,
            }
        }
    };
    let written = Arc::new(AtomicUsize::new(0));
    let observed_writes = Arc::clone(&written);
    let _write_hook =
        cluster.test_install_before_placed_payload_shard_write_hook(Arc::new(move |_, _| {
            observed_writes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));
    let stop = Arc::new(AtomicBool::new(false));
    let stop_before_acks = Arc::clone(&stop);
    let _ack_boundary =
        cluster.test_install_before_stream_append_command_id_hook(Arc::new(move || {
            if point == FailurePoint::BeforeAckRegistration {
                stop_before_acks.store(true, Ordering::SeqCst);
            }
        }));
    let stop_before_install = Arc::clone(&stop);
    let _install_boundary =
        cluster.test_install_before_metadata_command_pending_install_hook(Arc::new(move || {
            if point == FailurePoint::BeforePendingInstall {
                stop_before_install.store(true, Ordering::SeqCst);
            }
        }));
    let files = Arc::new(Mutex::new(Vec::new()));
    let acks = Arc::new(Mutex::new(Vec::new()));
    let observed_files = Arc::clone(&files);
    let observed_acks = Arc::clone(&acks);
    let _file_hook =
        cluster.test_install_before_placed_payload_shard_delete_hook(Arc::new(move |key| {
            observed_files.lock().unwrap().push(key.clone());
            if failed_participant == Some("file") && key.shard_index().get() == 0 {
                return Err(StoreError::Io {
                    context: "append file cleanup canary",
                    source: std::io::Error::other("injected"),
                });
            }
            Ok(())
        }));
    let _ack_hook = cluster.test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(
        move |key| {
            observed_acks.lock().unwrap().push(key.clone());
            if failed_participant == Some("ack") && key.shard_index().get() == 0 {
                return Err(StoreError::Io {
                    context: "append ack cleanup canary",
                    source: std::io::Error::other("injected"),
                });
            }
            Ok(())
        },
    ));
    let segment_okh = crate::stream_segment_key_hash(&session_id, 0);
    let route = crate::cluster::PutObjectMutationEffectRoute {
        bucket_pg_id: cluster.bucket_metadata_pg(&bucket),
        object_pg_id: cluster.object_metadata_pg(&bucket, &key),
        bucket: &bucket,
        key: &key,
        effect_fence: AdmittedRouteEffectFence::unbounded(cluster.operation_epoch()),
    };
    let error = cluster
        .append_stream_segment_with_route_validation(
            route,
            crate::StreamSegmentAppendInput {
                session_id: &session_id,
                segment_index: 0,
                payload_crc64: encrypted.payload_crc64,
                storage_bytes: &encrypted.storage_bytes,
            },
            Some(expected),
            crate::cluster::StreamAppendCallbacks {
                validate_session: |_: &crate::StreamUploadRecord| Ok(()),
                before_prepare: || {},
                require_valid_route: || {
                    if stop.load(Ordering::SeqCst) {
                        Err(StoreError::NotFound)
                    } else {
                        Ok(())
                    }
                },
                after_prepare: || {},
                maintain_lease: || -> Result<(), ObjectPgActionError> {
                    if written.load(Ordering::SeqCst) == usize::from(ec.k + ec.m) {
                        // Positive canary before cleanup: these are real placed files,
                        // not an early validation failure masquerading as rollback.
                        for index in 0..ec.k + ec.m {
                            assert!(cluster
                                .test_payload_shard_file_exists(
                                    0,
                                    ec,
                                    &segment_okh,
                                    GenerationId::MIN,
                                    index
                                )
                                .unwrap());
                        }
                        if point == FailurePoint::LeaseAfterWrite {
                            return Err(ObjectPgActionError::InvalidRequest {
                                reason: "append lease maintenance canary".into(),
                            });
                        }
                    }
                    Ok(())
                },
            },
        )
        .unwrap_err();
    match failed_participant {
        Some("file") => assert!(
            matches!(
                error,
                ObjectPgActionError::Store(StoreError::Io {
                    context: "append file cleanup canary",
                    ..
                })
            ),
            "{error:?}"
        ),
        Some("ack") => assert!(
            matches!(
                error,
                ObjectPgActionError::Store(StoreError::Io {
                    context: "append ack cleanup canary",
                    ..
                })
            ),
            "{error:?}"
        ),
        None if point == FailurePoint::LeaseAfterWrite => assert!(
            matches!(error, ObjectPgActionError::InvalidRequest { ref reason } if reason == "append lease maintenance canary"),
            "{error:?}"
        ),
        None => assert!(
            matches!(error, ObjectPgActionError::Store(StoreError::NotFound)),
            "{error:?}"
        ),
        Some(_) => unreachable!(),
    }
    let expected_keys = (0..ec.k + ec.m)
        .map(|index| ShardKey::new(&segment_okh, GenerationId::MIN.get(), index))
        .collect::<Vec<_>>();
    assert_eq!(
        *files.lock().unwrap(),
        expected_keys,
        "one complete file pass"
    );
    assert_eq!(
        *acks.lock().unwrap(),
        expected_keys,
        "one complete acknowledgement pass"
    );
    for (index, key) in expected_keys.iter().enumerate() {
        assert_eq!(
            cluster.test_shard_exists(0, key).unwrap(),
            point == FailurePoint::BeforePendingInstall
                && failed_participant == Some("ack")
                && index == 0
        );
        assert_eq!(
            cluster
                .test_payload_shard_file_exists(
                    0,
                    ec,
                    &segment_okh,
                    GenerationId::MIN,
                    u8::try_from(index).unwrap()
                )
                .unwrap(),
            failed_participant == Some("file") && index == 0
        );
    }
    assert!(pending_metadata_command_for_test(&map, PgId::new(0), &bucket).is_none());
    let mut expected_session = session_before;
    expected_session.next_segment_vid =
        GenerationId::new(expected_session.next_segment_vid.get() + 1).unwrap();
    assert_eq!(
        cluster
            .load_stream_upload_session(&bucket, &key, &session_id)
            .unwrap(),
        expected_session
    );
    for node in map.nodes.values() {
        let pg = node.storage_node().get_pg(0).unwrap();
        assert_eq!(
            crate::PgMetadataStore::get_object_generation_reservation(
                &*pg,
                &bucket,
                &key,
                &session_id
            )
            .unwrap(),
            generation
        );
        assert!(
            crate::PgMetadataStore::list_stream_segments(&*pg, &session_id)
                .unwrap()
                .is_empty()
        );
    }
}
