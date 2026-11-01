// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn command(store: &PgStore, bucket_control: bool, index: u64) -> MetadataCommandEnvelope {
    let bucket = crate::tests::bucket_name("pending-effect");
    let payload = if bucket_control {
        MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
            store.head_bucket_record_raw(&bucket).unwrap(),
            s3_types::BucketVersioningState::Enabled,
        ))
    } else {
        MetadataCommandPayload::ReserveObjectGeneration(ReserveObjectGenerationCommand::new(
            bucket,
            crate::tests::object_key("key"),
            crate::tests::stream_session_id("pending-effect"),
            GenerationId::new(1).unwrap(),
            1_000,
        ))
    };
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::INITIAL,
            PgId::new(0),
            MetadataCommandLogIndex::new(index).unwrap(),
        ),
        payload,
    )
}

fn seed(store: &PgStore) {
    store
        .create_bucket(
            &crate::tests::bucket_name("pending-effect"),
            "owner",
            &crate::CanonicalUserId::from_principal("owner"),
            &crate::AclGrants::default(),
            false,
            false,
        )
        .unwrap();
    store.refresh_metadata_command_state_digest().unwrap();
}

fn insert(
    store: &PgStore,
    command: &MetadataCommandEnvelope,
    fence: AdmittedRouteEffectFence,
) -> Result<bool, PendingMetadataCommandSlotInsertError> {
    if command.payload().is_bucket_control_pending_slot_payload() {
        store
            .try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence(
                0,
                command,
                command.bucket_name(),
                fence,
            )
            .map_err(PendingMetadataCommandSlotInsertError::definitive)
    } else {
        store
            .try_insert_pending_metadata_command_slot_with_effect_fence_classified(
                0,
                command,
                Some(command.bucket_name()),
                fence,
            )
            .map(|()| true)
    }
}

#[test]
fn pending_slot_effect_expiry_is_definitive_before_any_sql_change() {
    for bucket_control in [false, true] {
        for occupied in [false, true] {
            let time = crate::clock::test_time_override_guard(1_000);
            let dir = test_util::tempdir();
            let store = PgStore::open(dir.path(), 0).unwrap();
            seed(&store);
            let target = command(&store, bucket_control, 2);
            if occupied {
                assert!(insert(
                    &store,
                    &command(&store, false, 1),
                    AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL)
                )
                .unwrap());
            }
            let observe = || {
                (
                    store.metadata_checkpoint_table_blocks().unwrap(),
                    store.metadata_command_replica_state().unwrap(),
                    store
                        .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
                        .unwrap(),
                )
            };
            let before = observe();
            let changes = store.conn.total_changes();
            let control = time.control();
            let error = PgStore::test_with_before_pending_slot_effect_hook(
                move |in_transaction| {
                    assert!(in_transaction);
                    control.set(4_500);
                },
                || {
                    insert(
                        &store,
                        &target,
                        AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 5_000, 4_000),
                    )
                },
            )
            .unwrap_err();
            assert!(
                matches!(
                    error,
                    PendingMetadataCommandSlotInsertError::Definitive(
                        StoreError::RouteMapExpired {
                            valid_until_ms: 5_000,
                            now_ms: 4_500,
                            ..
                        }
                    )
                ),
                "{error:?}"
            );
            assert!(store.conn.is_autocommit());
            assert_eq!(
                store.conn.total_changes(),
                changes,
                "expiry must precede slot/sidecar mutation, not just roll it back"
            );
            assert_eq!(observe(), before);
            if occupied {
                // Leave the unrelated unapplied command intact. Only its own
                // publication/recovery protocol may remove it.
                continue;
            }
            let fresh = AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 20_000, 19_000);
            assert!(insert(&store, &target, fresh).unwrap());
            let installed = observe();
            assert_eq!(insert(&store, &target, fresh).unwrap(), !bucket_control);
            assert_eq!(observe(), installed);
        }
    }
}

#[test]
fn pending_slot_effect_fence_joins_outer_transaction() {
    for bucket_control in [false, true] {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        seed(&store);
        let target = command(&store, bucket_control, 1);
        store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(insert(
            &store,
            &target,
            AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL)
        )
        .unwrap());
        assert!(!store.conn.is_autocommit());
        assert!(store
            .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
            .unwrap()
            .is_some());
        store.conn.execute_batch("ROLLBACK").unwrap();
        assert!(store
            .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
    }
}

#[test]
fn pending_slot_effect_accepted_before_cutoff_can_finish_after_cutoff() {
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    for bucket_control in [false, true] {
        let time = crate::clock::test_time_override_guard(1_000);
        let dir = test_util::tempdir();
        let mut store = PgStore::open(dir.path(), 0).unwrap();
        seed(&store);
        let target = command(&store, bucket_control, 1);
        let ran = Arc::new(AtomicBool::new(false));
        let captured = Arc::clone(&ran);
        let control = time.control();
        // SQLite traces execution after the adjacent effect check. This is an
        // accepted insertion, still protected by the transaction; passing the
        // cutoff now must not make its committed slot look like a rejected send.
        store
            .conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        if sql.starts_with("INSERT INTO metadata_command_pending_slot ") {
                            control.set(4_500);
                            captured.store(true, Ordering::SeqCst);
                        }
                    }
                }),
            )
            .unwrap();
        assert!(insert(
            &store,
            &target,
            AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 5_000, 4_000)
        )
        .unwrap());
        assert!(ran.load(Ordering::SeqCst));
        assert!(store.conn.is_autocommit());
        let slot = store
            .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
            .unwrap()
            .unwrap();
        assert_eq!(slot.id, target.id());
        assert_eq!(slot.command_bytes, target.command_bytes());
    }
}

#[test]
fn pending_slot_effect_fence_preserves_failure_classification_and_rollback() {
    for fail_commit in [false, true] {
        let dir = test_util::tempdir();
        let store = PgStore::open(dir.path(), 0).unwrap();
        seed(&store);
        let target = command(&store, false, 1);
        let before = store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();
        if fail_commit {
            // The INSERT and sidecars succeed; a deferred FK violation fails
            // COMMIT. Production must still report MayHaveApplied, since a
            // genuine commit I/O failure cannot establish rollback from its error.
            store
                .conn
                .execute_batch(
                    "CREATE TEMP TABLE fail_pending_commit (
                    id INTEGER PRIMARY KEY,
                    parent INTEGER REFERENCES fail_pending_commit(id) DEFERRABLE INITIALLY DEFERRED
                );
                CREATE TEMP TRIGGER fail_pending AFTER INSERT ON main.metadata_command_pending_slot
                BEGIN INSERT INTO fail_pending_commit VALUES (1, 2); END;",
                )
                .unwrap();
        } else {
            store.conn.execute_batch(
                "CREATE TEMP TRIGGER fail_pending BEFORE INSERT ON metadata_command_pending_slot
                BEGIN SELECT RAISE(ABORT, 'injected pending insert failure'); END;"
            ).unwrap();
        }
        let fence = AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL);
        let error = insert(&store, &target, fence).unwrap_err();
        assert_eq!(
            matches!(
                &error,
                PendingMetadataCommandSlotInsertError::MayHaveApplied(_)
            ),
            fail_commit,
            "{error:?}"
        );
        assert!(matches!(error.into_source(), StoreError::Db { context, .. }
            if context == if fail_commit { "commit pending metadata command slot transaction" }
            else { "insert metadata command pending slot" }));
        assert!(store.conn.is_autocommit());
        assert!(store
            .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
            .unwrap()
            .is_none());
        assert_eq!(
            store
                .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
                .unwrap(),
            before
        );
        store
            .conn
            .execute_batch("DROP TRIGGER fail_pending")
            .unwrap();
        assert!(insert(&store, &target, fence).unwrap());
    }
}

#[test]
fn pending_slot_effect_fence_has_one_successful_write_transaction() {
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    for bucket_control in [false, true] {
        let dir = test_util::tempdir();
        let mut store = PgStore::open(dir.path(), 0).unwrap();
        seed(&store);
        let target = command(&store, bucket_control, 1);
        let statements = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let captured = std::sync::Arc::clone(&statements);
        store
            .conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        captured.lock().unwrap().push(sql.to_owned());
                    }
                }),
            )
            .unwrap();
        assert!(insert(
            &store,
            &target,
            AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL)
        )
        .unwrap());
        let observed = statements.lock().unwrap();
        let boundaries = observed
            .iter()
            .filter(|sql| {
                sql.starts_with("BEGIN") || sql.starts_with("COMMIT") || sql.starts_with("ROLLBACK")
            })
            .map(String::as_str)
            .collect::<Vec<_>>();
        assert_eq!(boundaries, ["BEGIN IMMEDIATE", "COMMIT"], "{observed:?}");
    }
}
