// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::cluster::bucket_snapshot_error_to_object_pg_action_error;
use crate::node_client::{
    RetainedBucketWriteReservationNodeClient, RetainedBucketWriteReservationRoute,
};
use crate::ObjectPgActionError;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReservationFailure {
    BeforeEffect,
    LostReply,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AbortRetry {
    Retained,
    GenericRecovery,
    GenericFinisher,
}

impl AbortRetry {
    fn finish(
        self,
        cluster: &StorageCluster,
        command: &MetadataCommandEnvelope,
    ) -> Result<PendingMetadataCommandOutcome, ObjectPgActionError> {
        match self {
            Self::GenericRecovery => cluster
                .drain_pending_metadata_command_with_static_recovery_route(PgId::new(0), command),
            Self::GenericFinisher => cluster
                .finish_pending_metadata_command_to_acting_set(
                    PgId::new(0),
                    command.bucket_name(),
                    command,
                    false,
                )
                .map_err(bucket_snapshot_error_to_object_pg_action_error),
            Self::Retained => unreachable!("retained retry uses its cleanup capability"),
        }
    }
}

// Exercise the actual retained route and SQL commit, then lose only its result.
// This is a client-boundary fault model, not a socket/disconnect simulation.
struct LostReleaseReplyClient {
    inner: Arc<dyn RetainedBucketWriteReservationNodeClient>,
    armed: Arc<AtomicBool>,
}

struct LostReleaseReplyRoute<'a> {
    inner: Box<dyn RetainedBucketWriteReservationRoute + 'a>,
    armed: &'a AtomicBool,
}

impl RetainedBucketWriteReservationNodeClient for LostReleaseReplyClient {
    fn open_retained_bucket_write_reservation_route(
        &self,
        pg_id: crate::BucketPgId,
        bucket: &BucketName,
    ) -> Result<Box<dyn RetainedBucketWriteReservationRoute + '_>, BucketSnapshotLoadError> {
        Ok(Box::new(LostReleaseReplyRoute {
            inner: self
                .inner
                .open_retained_bucket_write_reservation_route(pg_id, bucket)?,
            armed: &self.armed,
        }))
    }
}

impl RetainedBucketWriteReservationRoute for LostReleaseReplyRoute<'_> {
    fn release_metadata_command_bucket_write_reservation(
        &self,
        proof: &crate::BucketWriteReservationProof,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.inner
            .release_metadata_command_bucket_write_reservation(proof)?;
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(StoreError::Io {
                context: "lost retained bucket reservation release reply",
                source: std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "injected after commit",
                ),
            }
            .into());
        }
        Ok(())
    }

    fn release_durable_bucket_write_reservation(
        &self,
        record: &crate::BucketWriteReservationRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.inner.release_durable_bucket_write_reservation(record)
    }

    fn clear_durable_bucket_write_drain(
        &self,
        record: &crate::BucketWriteDrainRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.inner.clear_durable_bucket_write_drain(record)
    }

    fn release_bucket_delete_finalize_claim(
        &self,
        claim: &crate::BucketDeleteFinalizeClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.inner.release_bucket_delete_finalize_claim(claim)
    }

    fn release_lifecycle_sweep_claim(
        &self,
        claim: &crate::LifecycleSweepClaimRecord,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.inner.release_lifecycle_sweep_claim(claim)
    }
}

#[test]
fn retained_stream_abort_attempts_all_participants_before_retry_for_both_targets() {
    let _serial = lock_payload_cleanup_hook_test();
    for multipart in [false, true] {
        for failed_participant in [None, Some("file"), Some("ack")] {
            check_retained_abort(multipart, failed_participant, None, AbortRetry::Retained);
        }
    }
}

#[test]
fn retained_stream_abort_reservation_failure_does_not_starve_physical_cleanup() {
    let _serial = lock_payload_cleanup_hook_test();
    // UploadPart releases its creation proof when creation completes; only PUT
    // retains that proof in the session and releases it during retained abort.
    for failed_participant in [None, Some("file"), Some("ack")] {
        check_retained_abort(
            false,
            failed_participant,
            Some(ReservationFailure::BeforeEffect),
            AbortRetry::Retained,
        );
    }
}

#[test]
fn retained_stream_abort_retries_committed_reservation_release_after_lost_reply() {
    let _serial = lock_payload_cleanup_hook_test();
    for failed_participant in [None, Some("file"), Some("ack")] {
        check_retained_abort(
            false,
            failed_participant,
            Some(ReservationFailure::LostReply),
            AbortRetry::Retained,
        );
    }
}

#[test]
fn generic_recovery_retains_applied_stream_abort_until_all_cleanup_succeeds() {
    let _serial = lock_payload_cleanup_hook_test();
    for retry in [AbortRetry::GenericRecovery, AbortRetry::GenericFinisher] {
        for multipart in [false, true] {
            for participant in [Some("file"), Some("ack")] {
                check_retained_abort(multipart, participant, None, retry);
            }
        }
    }
}

#[test]
fn generic_stream_abort_recovery_attempts_payload_cleanup_after_release_failure() {
    let _serial = lock_payload_cleanup_hook_test();
    for retry in [AbortRetry::GenericRecovery, AbortRetry::GenericFinisher] {
        for participant in [None, Some("file"), Some("ack")] {
            check_retained_abort(
                false,
                participant,
                Some(ReservationFailure::BeforeEffect),
                retry,
            );
        }
    }
}

fn check_retained_abort(
    multipart: bool,
    failed_participant: Option<&'static str>,
    reservation_failure: Option<ReservationFailure>,
    retry: AbortRetry,
) {
    let tmp = test_util::tempdir();
    let ec = EcShape { k: 2, m: 1 };
    let mut map = LocalClusterMap::open(
        tmp.path(),
        &[NodeId::new(0), NodeId::new(1), NodeId::new(2)],
        &[0],
        ec,
    )
    .unwrap();
    let lost_release_reply = Arc::new(AtomicBool::new(false));
    if reservation_failure == Some(ReservationFailure::LostReply) {
        for node in map.nodes.values_mut() {
            node.retained_bucket_write_reservation_client = Arc::new(LostReleaseReplyClient {
                inner: Arc::clone(&node.retained_bucket_write_reservation_client),
                armed: Arc::clone(&lost_release_reply),
            });
        }
    }
    let map = Arc::new(map);
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
    let bucket_primary = map
        .metadata_pg_primary_node(map.epoch, PgId::new(0))
        .unwrap();
    let reservations = || {
        let pg = bucket_primary.storage_node().get_pg(0).unwrap();
        crate::PgMetadataStore::durable_bucket_write_reservations(&*pg, &bucket).unwrap()
    };
    let reservation_fault = reservation_failure.map(|failure| {
        let proof = {
            let pg = bucket_primary.storage_node().get_pg(0).unwrap();
            crate::PgMetadataStore::get_stream_upload(&*pg, &session)
                .unwrap()
                .bucket_write_reservation
                .expect("PUT must retain its creation proof")
        };
        assert!(reservations().iter().any(|row| proof.matches_record(row)));
        let canary = acquire_test_bucket_write_proof(
            &cluster,
            &bucket,
            crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            Some("unrelated-key"),
        );
        // The owning PgStore injects a real SQL deletion failure on its own
        // connection. No extra runtime/connection or production hook is needed.
        match failure {
            ReservationFailure::BeforeEffect => bucket_primary
                .storage_node()
                .get_pg(0)
                .unwrap()
                .test_set_bucket_reservation_release_failure(true)
                .unwrap(),
            ReservationFailure::LostReply => lost_release_reply.store(true, Ordering::SeqCst),
        }
        (proof, canary)
    });
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
    if failed_participant.is_some() || reservation_failure.is_some() {
        let error = result.unwrap_err();
        assert_eq!(
            error.diagnostic_cause_label(),
            if reservation_failure == Some(ReservationFailure::BeforeEffect) {
                "metadata_db_error"
            } else {
                "store_io_failure"
            },
            "retain the first failure even when physical cleanup also fails"
        );
        let pending = pending_metadata_command_for_test(&map, PgId::new(0), &bucket).unwrap();
        let MetadataCommandPayload::AbortStreamUpload(abort) = pending.payload() else {
            panic!("expected retained abort")
        };
        assert_eq!(abort.staged_segments, segments);
        assert_eq!(abort.session_id, session);
        if let Some((proof, canary)) = &reservation_fault {
            assert_eq!(
                abort.stream_create_bucket_write_reservation.as_ref(),
                Some(proof)
            );
            let rows = reservations();
            let release_failed_before_effect =
                reservation_failure == Some(ReservationFailure::BeforeEffect);
            assert_eq!(rows.len(), 1 + usize::from(release_failed_before_effect));
            assert_eq!(
                rows.iter().any(|row| proof.matches_record(row)),
                release_failed_before_effect
            );
            assert!(rows.iter().any(|row| canary.matches_record(row)));
            assert!(
                !lost_release_reply.load(Ordering::SeqCst),
                "lost reply must follow the actual release"
            );
        }
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
    if retry != AbortRetry::Retained {
        files.lock().unwrap().clear();
        acks.lock().unwrap().clear();
        reports.store(0, Ordering::SeqCst);
        let pending = pending_metadata_command_for_test(&map, PgId::new(0), &bucket).unwrap();
        let error = retry
            .finish(&cluster, &pending)
            .expect_err("failed cleanup must not retire the applied abort");
        if reservation_failure == Some(ReservationFailure::BeforeEffect) {
            assert!(
                matches!(
                    error,
                    ObjectPgActionError::Metadata(crate::MetadataError::Db {
                        context: "release metadata command durable bucket write reservation",
                        ..
                    })
                ),
                "preserve the reservation failure before any physical failure: {error:?}"
            );
        } else {
            let expected_context = if failed_participant == Some("file") {
                "retained abort file cleanup canary"
            } else {
                "retained abort ack cleanup canary"
            };
            assert!(
                matches!(
                    error,
                    ObjectPgActionError::Store(StoreError::Io { context, .. })
                        if context == expected_context
                ),
                "preserve the exact physical failure: {error:?}"
            );
        }
        assert_eq!(
            pending_metadata_command_for_test(&map, PgId::new(0), &bucket),
            Some(pending)
        );
        assert_eq!(
            *files.lock().unwrap(),
            expected_keys,
            "recovery must retry every file"
        );
        assert_eq!(
            *acks.lock().unwrap(),
            expected_keys,
            "recovery must retry every acknowledgement"
        );
        assert_eq!(
            reports.load(Ordering::SeqCst),
            usize::from(failed_participant.is_some())
        );
    }
    drop(file_hook);
    drop(ack_hook);
    if reservation_failure == Some(ReservationFailure::BeforeEffect) {
        bucket_primary
            .storage_node()
            .get_pg(0)
            .unwrap()
            .test_set_bucket_reservation_release_failure(false)
            .unwrap();
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
    // Discard the frontend capability after an uncertain release. A fresh one
    // must rediscover the exact pending abort even though the reservation is gone.
    let cleanup = if reservation_failure == Some(ReservationFailure::LostReply) {
        drop(cleanup);
        let admission = handle.admit_current_route().unwrap();
        admission
            .retained_stream_upload_cleanup(&bucket, &key)
            .unwrap()
    } else {
        cleanup
    };
    // No new client session or reservation is created.
    if retry != AbortRetry::Retained {
        let pending = pending_metadata_command_for_test(&map, PgId::new(0), &bucket).unwrap();
        assert_eq!(
            retry.finish(&cluster, &pending).unwrap(),
            PendingMetadataCommandOutcome::Applied
        );
        assert!(pending_metadata_command_for_test(&map, PgId::new(0), &bucket).is_none());
    }
    cleanup.abort(&session).unwrap();
    cleanup.abort(&session).unwrap();
    assert!(pending_metadata_command_for_test(&map, PgId::new(0), &bucket).is_none());
    if let Some((proof, canary)) = &reservation_fault {
        let rows = reservations();
        assert_eq!(rows.len(), 1);
        assert!(!rows.iter().any(|row| proof.matches_record(row)));
        assert!(
            canary.matches_record(&rows[0]),
            "retry must not release an unrelated proof"
        );
    }
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
