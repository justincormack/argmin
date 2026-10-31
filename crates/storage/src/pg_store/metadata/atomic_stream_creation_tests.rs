// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::metadata_command::{
    CreateStreamUploadCommand, MetadataCommandEnvelope, MetadataCommandPayload,
};

#[path = "stream_creation_evidence_tests.rs"]
mod evidence;

#[path = "allocation_lifetime_tests.rs"]
mod allocation_lifetime;

fn fixture(store: &PgStore, part: bool) -> CreateStreamUploadCommand {
    let bucket = trusted_bucket_name("atomic-stream");
    let key = trusted_object_key("object");
    create_probe_bucket_direct(store, &bucket);
    let encryption = metadata_only_test_encryption();
    let target = if part {
        let upload_id = crate::tests::multipart_upload_id("atomic-stream");
        store
            .create_multipart_upload(&CreateMultipartUploadReq {
                upload_id: upload_id.clone(),
                bucket: bucket.clone(),
                key: key.clone(),
                tags: None,
                metadata_blob: SerializedMetadataBlob::default(),
                system_metadata_blob: SerializedSystemMetadataBlob::default(),
                initiator: test_owner(),
                owner: test_owner(),
                acl_grants: AclGrants::default(),
                public_read: false,
                object_lock: ObjectLockState::default(),
                checksum: None,
                encryption: encryption.clone(),
            })
            .unwrap();
        StreamUploadTarget::UploadPart {
            upload_id,
            part_number: 1,
        }
    } else {
        StreamUploadTarget::PutObject
    };
    let proof = BucketWriteReservationProof {
        bucket: bucket.clone(),
        reservation_id: "stream-create-proof".into(),
        owner_token: "owner".into(),
        cluster_epoch: ClusterEpoch::INITIAL,
        bucket_execution_generation: 1,
        bucket_incarnation_generation: 1,
        operation_kind: if part {
            crate::metadata_command::UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
        } else {
            crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
        }
        .into(),
        created_at: 10,
        lease_deadline: 100,
        target_context: Some(key.as_str().into()),
    };
    let generation = store.next_generation_id(&bucket, &key).unwrap();
    store.refresh_metadata_command_state_digest().unwrap();
    CreateStreamUploadCommand::from_request_with_bucket_write_reservation_and_cleanup_deadline(
        CreateStreamUploadReq {
            session_id: crate::tests::stream_session_id("atomic-create"),
            bucket,
            key,
            target,
            encryption,
        },
        generation,
        10,
        Some(100),
        proof,
    )
}

fn envelope(create: &CreateStreamUploadCommand, index: u64) -> MetadataCommandEnvelope {
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(index).unwrap(),
        ),
        MetadataCommandPayload::CreateStreamUpload(Box::new(create.clone())),
    )
}

fn assert_absent(store: &PgStore, create: &CreateStreamUploadCommand) {
    assert!(matches!(
        store.get_stream_upload(&create.session.session_id),
        Err(MetadataError::StreamSessionNotFound { .. })
    ));
    assert!(matches!(
        store.get_object_generation_reservation(
            &create.session.bucket,
            &create.session.key,
            &create.session.session_id
        ),
        Err(MetadataError::ObjectGenerationReservationNotFound { .. })
    ));
    assert_eq!(
        store
            .next_generation_id(&create.session.bucket, &create.session.key)
            .unwrap(),
        create.payload_generation,
        "failed creation advanced the generation high-water"
    );
    assert_eq!(
        store
            .max_metadata_command_log_index(ClusterEpoch::INITIAL)
            .unwrap(),
        0,
        "failed creation advanced the command log"
    );
}

fn assert_present(store: &PgStore, create: &CreateStreamUploadCommand) {
    let session = store.get_stream_upload(&create.session.session_id).unwrap();
    assert_eq!(StreamUploadCommandRecord::from(&session), create.session);
    assert_eq!(session.next_segment_vid, create.initial_next_segment_vid);
    assert_eq!(session.cleanup_after, create.cleanup_after);
    assert_eq!(
        store
            .get_object_generation_reservation(
                &create.session.bucket,
                &create.session.key,
                &create.session.session_id
            )
            .unwrap(),
        create.payload_generation
    );
    assert_eq!(
        store
            .next_generation_id(&create.session.bucket, &create.session.key)
            .unwrap()
            .get(),
        create.payload_generation.get() + 1
    );
    assert_eq!(
        store
            .max_metadata_command_log_index(ClusterEpoch::INITIAL)
            .unwrap(),
        1
    );
}

#[test]
fn decoded_stream_creation_rejects_forged_initial_cursor_before_mutation() {
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    for part in [false, true] {
        let dir = test_util::tempdir();
        let mut store = PgStore::open(dir.path(), 1).unwrap();
        let create = fixture(&store, part);
        let before = store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        let writes = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&writes);
        store
            .conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        let sql = sql.trim_start();
                        if (sql.starts_with("INSERT")
                            || sql.starts_with("UPDATE")
                            || sql.starts_with("DELETE"))
                            && [
                                "object_generation_reservations",
                                "object_write_counters",
                                "stream_uploads",
                            ]
                            .iter()
                            .any(|table| sql.contains(table))
                        {
                            observed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }),
            )
            .unwrap();
        for cursor in [2, 641, i64::MAX as u64, u64::MAX] {
            let mut invalid = create.clone();
            invalid.initial_next_segment_vid = GenerationId::new(cursor).unwrap();
            let encoded = envelope(&invalid, 1);
            let decoded = decode_metadata_command_envelope(
                &encoded.command_bytes(),
                &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
            )
            .unwrap();
            assert_eq!(decoded, encoded);
            let error = store
                .apply_metadata_command_and_record(0, &decoded)
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    BucketSnapshotLoadError::Metadata(MetadataError::InvariantViolation {
                        context: "create stream upload command initial allocator mismatch",
                        ..
                    })
                ),
                "{error:?}"
            );
            assert_eq!(
                writes.load(Ordering::Relaxed),
                0,
                "rollback must not mask attempted mutation"
            );
            assert_absent(&store, &create);
            assert_eq!(
                store
                    .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                    .unwrap(),
                before
            );
        }
        // Pin the observer and the valid origin independently of rejection.
        store
            .apply_metadata_command_and_record(0, &envelope(&create, 1))
            .unwrap();
        assert_present(&store, &create);
        assert!(writes.load(Ordering::Relaxed) >= 3);
    }
}

#[test]
fn recovered_stream_creation_cannot_install_a_forged_initial_cursor() {
    for part in [false, true] {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 1).unwrap();
        let create = fixture(&store, part);
        let mut invalid = create.clone();
        invalid.initial_next_segment_vid = GenerationId::new(2).unwrap();
        let command = envelope(&invalid, 1);
        store
            .try_insert_pending_metadata_command_slot(0, &command, Some(command.bucket_name()))
            .unwrap();
        let slot = store.pending_metadata_command_slot_any_epoch(0).unwrap();
        // Checkpoint export deliberately rejects a pending slot. Observe the
        // materialized state and applied prefix without bypassing that guard.
        let state_digest = store.metadata_state_digest().unwrap();
        let replica_state = store.metadata_command_replica_state().unwrap();
        drop(store);
        for _ in 0..2 {
            let store = PgStore::open(dir.path(), 1).unwrap();
            let recovered = store
                .pending_metadata_command_envelope_any_epoch(0)
                .unwrap()
                .unwrap();
            assert_eq!(recovered, command);
            let error = store
                .apply_metadata_command_and_record(0, &recovered)
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    BucketSnapshotLoadError::Metadata(MetadataError::InvariantViolation {
                        context: "create stream upload command initial allocator mismatch",
                        ..
                    })
                ),
                "{error:?}"
            );
            assert_absent(&store, &create);
            assert_eq!(store.metadata_state_digest().unwrap(), state_digest);
            assert_eq!(
                store.metadata_command_replica_state().unwrap(),
                replica_state
            );
            assert_eq!(
                store.pending_metadata_command_slot_any_epoch(0).unwrap(),
                slot
            );
        }
    }
}

#[test]
fn stream_creation_reservation_session_and_log_commit_or_rollback_together() {
    for part in [false, true] {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let create = fixture(&store, part);
        let command = envelope(&create, 1);
        assert_absent(&store, &create);

        // Fail after the reservation/high-water writes, at the session insertion.
        store.conn.execute_batch("CREATE TEMP TRIGGER fail_stream_insert BEFORE INSERT ON stream_uploads BEGIN SELECT RAISE(ABORT, 'injected stream insertion failure'); END").unwrap();
        let error = store
            .apply_metadata_command_and_record(0, &command)
            .unwrap_err();
        assert!(
            matches!(
                error,
                BucketSnapshotLoadError::Metadata(MetadataError::Db {
                    context: "create stream upload explicit",
                    ..
                })
            ),
            "{error:?}"
        );
        assert_absent(&store, &create);
        store
            .conn
            .execute_batch("DROP TRIGGER fail_stream_insert")
            .unwrap();

        // Also fail the encompassing commit after all three rows were prepared.
        store.fail_next_metadata_txn_commit();
        assert!(store
            .apply_metadata_command_and_record(0, &command)
            .is_err());
        assert_absent(&store, &create);
        store.refresh_metadata_command_state_digest().unwrap();
        store
            .apply_metadata_command_and_record(0, &command)
            .unwrap();
        assert_present(&store, &create);
        let before = store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        store
            .apply_metadata_command_and_record(0, &command)
            .unwrap();
        // Exercise mutation-level idempotence too, rather than only the log's
        // already-applied fast path.
        store.apply_metadata_command(&command).unwrap();
        assert_eq!(
            store
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            before
        );

        // A different command cannot attach another generation to the same session.
        let mut crossed = create.clone();
        crossed.payload_generation =
            GenerationId::new(create.payload_generation.get() + 1).unwrap();
        let error = store
            .apply_metadata_command_and_record(0, &envelope(&crossed, 2))
            .unwrap_err();
        assert!(
            matches!(
                error,
                BucketSnapshotLoadError::Metadata(MetadataError::InvariantViolation {
                    context: "create stream upload command generation mismatch",
                    ..
                })
            ),
            "{error:?}"
        );
        assert_present(&store, &create);
        assert_eq!(
            store
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            before
        );
        drop(store);
        let reopened = PgStore::open(tmp.path(), 1).unwrap();
        assert_present(&reopened, &create);
        reopened
            .apply_metadata_command_and_record(0, &command)
            .unwrap();
        assert_present(&reopened, &create);
    }
}

#[test]
fn stream_creation_replicas_apply_the_exact_selected_generation_without_reserve_command() {
    for part in [false, true] {
        let dirs = [
            test_util::tempdir(),
            test_util::tempdir(),
            test_util::tempdir(),
        ];
        let stores = dirs
            .iter()
            .map(|dir| PgStore::open(dir.path(), 1).unwrap())
            .collect::<Vec<_>>();
        let creates = stores
            .iter()
            .map(|store| fixture(store, part))
            .collect::<Vec<_>>();
        let command = envelope(&creates[0], 1);
        for (node, store) in stores.iter().enumerate() {
            // No replica has a standalone generation reservation to depend on.
            assert_absent(store, &creates[0]);
            let decoded = decode_metadata_command_envelope(
                &command.command_bytes(),
                &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
            )
            .unwrap();
            store
                .apply_metadata_command_and_record(node as u32, &decoded)
                .unwrap();
            assert_present(store, &creates[0]);
        }
    }
}

#[test]
fn decoded_stream_creation_cannot_adopt_an_independent_generation_reservation() {
    for part in [false, true] {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let create = fixture(&store, part);
        // A different operation owns precisely the reservation that creation
        // would otherwise adopt. Keep bucket, key, ID and generation identical
        // so a subject mismatch cannot mask the ownership check.
        let reservation = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectGeneration(
                crate::metadata_command::ReserveObjectGenerationCommand::new(
                    create.session.bucket.clone(),
                    create.session.key.clone(),
                    create.session.session_id.clone(),
                    create.payload_generation,
                    3,
                ),
            ),
        );
        store
            .apply_metadata_command_and_record(0, &reservation)
            .unwrap();
        let before = store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        let before_state = store.metadata_command_replica_state().unwrap();
        let before_reservation = store
            .get_object_generation_reservation_by_id(&create.session.session_id)
            .unwrap();
        let before_next_generation = store
            .next_generation_id(&create.session.bucket, &create.session.key)
            .unwrap();
        let bytes = envelope(&create, 2).command_bytes();

        let assert_rejected_without_mutation = |store: &PgStore| {
            let decoded = decode_metadata_command_envelope(
                &bytes,
                &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
            )
            .unwrap();
            assert!(decoded.verify_checksum());
            let error = store
                .apply_metadata_command_and_record(0, &decoded)
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    BucketSnapshotLoadError::Metadata(MetadataError::InvariantViolation {
                        context: "create stream upload command pre-existing generation reservation",
                        ..
                    })
                ),
                "{error:?}"
            );
            assert!(matches!(
                store.get_stream_upload(&create.session.session_id),
                Err(MetadataError::StreamSessionNotFound { .. })
            ));
            assert_eq!(
                store
                    .get_object_generation_reservation_by_id(&create.session.session_id)
                    .unwrap(),
                before_reservation
            );
            assert_eq!(
                store
                    .next_generation_id(&create.session.bucket, &create.session.key)
                    .unwrap(),
                before_next_generation
            );
            assert_eq!(
                store
                    .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                    .unwrap(),
                before
            );
            assert_eq!(
                store.metadata_command_replica_state().unwrap(),
                before_state
            );
            assert_eq!(
                store
                    .max_metadata_command_log_index(ClusterEpoch::INITIAL)
                    .unwrap(),
                1
            );
        };
        assert_rejected_without_mutation(&store);
        drop(store);
        let reopened = PgStore::open(tmp.path(), 1).unwrap();
        assert_rejected_without_mutation(&reopened);
    }
}

#[test]
fn decoded_direct_put_reservation_cannot_adopt_a_stream_generation() {
    for part in [false, true] {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 1).unwrap();
        let create = fixture(&store, part);
        store
            .apply_metadata_command_and_record(0, &envelope(&create, 1))
            .unwrap();
        let before = store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        let session = store.get_stream_upload(&create.session.session_id).unwrap();
        let reservation = store
            .get_object_generation_reservation_by_id(&create.session.session_id)
            .unwrap();
        // Every reservation field matches, including creation time. Only its
        // durable stream ownership distinguishes this from Direct PUT reissue.
        let crossed = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectGeneration(
                crate::metadata_command::ReserveObjectGenerationCommand::new(
                    create.session.bucket.clone(),
                    create.session.key.clone(),
                    create.session.session_id.clone(),
                    create.payload_generation,
                    create.session.created_at,
                ),
            ),
        );
        let decoded = decode_metadata_command_envelope(
            &crossed.command_bytes(),
            &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
        )
        .unwrap();
        assert_eq!(decoded, crossed);
        let assert_rejected = |store: &PgStore| {
            let changes = store.conn.total_changes();
            let error = store
                .apply_metadata_command_and_record(0, &decoded)
                .unwrap_err();
            assert!(
                matches!(
                    error,
                    BucketSnapshotLoadError::Metadata(MetadataError::InvariantViolation {
                        context: "reserve object generation command stream-owned reservation",
                        ..
                    })
                ),
                "part={part}: {error:?}"
            );
            assert_eq!(store.conn.total_changes(), changes);
            assert_eq!(
                store.get_stream_upload(&create.session.session_id).unwrap(),
                session
            );
            assert_eq!(
                store
                    .get_object_generation_reservation_by_id(&create.session.session_id)
                    .unwrap(),
                reservation
            );
            assert_present(store, &create);
            assert_eq!(
                store
                    .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                    .unwrap(),
                before
            );
        };
        assert_rejected(&store);
        drop(store);
        let reopened = PgStore::open(dir.path(), 1).unwrap();
        assert_rejected(&reopened);
    }
}

#[test]
fn atomic_stream_creation_does_not_authorize_a_generation_release_derivative() {
    for part in [false, true] {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 1).unwrap();
        let create = fixture(&store, part);
        let source = envelope(&create, 1);
        let release = MetadataCommandPayload::ReleaseObjectGeneration(
            crate::metadata_command::ReleaseObjectGenerationCommand::new(
                create.session.bucket.clone(),
                create.session.key.clone(),
                create.session.session_id.clone(),
            ),
        );
        assert!(source.payload().abandoned_recovery_follow_up().is_none());
        assert!(!release.is_authorized_recovery_derivative_of(source.payload()));
        assert!(
            source
                .payload()
                .is_authorized_recovery_derivative_of(source.payload()),
            "equivalent source reissue must remain authorized"
        );
    }
}
