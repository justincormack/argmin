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

#[derive(Clone, Copy, Debug)]
enum EmbeddedInsertApi {
    Classified,
    Generic,
    BucketControl,
}

impl EmbeddedInsertApi {
    fn insert(
        self,
        client: &dyn crate::node_client::MetadataCommandNodeClient,
        command: &MetadataCommandEnvelope,
        fence: Option<AdmittedRouteEffectFence>,
        deadline: Instant,
    ) -> Result<(), StoreError> {
        let pg = command.id().pg_id();
        let bucket = command.bucket_name();
        match (self, fence) {
            (Self::Classified, fence) => {
                let result = match fence {
                    Some(fence) => client
                        .try_insert_pending_metadata_command_slot_with_effect_fence_classified_until(
                            pg, command, Some(bucket), fence, deadline,
                        ),
                    None => client.try_insert_pending_metadata_command_slot_classified_until(
                        pg, command, Some(bucket), deadline,
                    ),
                };
                result.map_err(|error| {
                    assert_eq!(
                        error.kind(),
                        crate::node_client::MetadataCommandApplyErrorKind::Definitive,
                        "SQLite rejected before effects, not an uncertain dispatch: {error:?}",
                    );
                    error.into_source()
                })
            }
            (Self::Generic, Some(fence)) => client
                .try_insert_pending_metadata_command_slot_with_effect_fence_until(
                    pg,
                    command,
                    Some(bucket),
                    fence,
                    deadline,
                ),
            (Self::Generic, None) => client.try_insert_pending_metadata_command_slot_until(
                pg,
                command,
                Some(bucket),
                deadline,
            ),
            (Self::BucketControl, Some(fence)) => client
                .try_insert_bucket_control_pending_metadata_command_slot_with_effect_fence_until(
                    pg, command, bucket, fence, deadline,
                )
                .map(|_| ()),
            (Self::BucketControl, None) => client
                .try_insert_bucket_control_pending_metadata_command_slot_until(
                    pg, command, bucket, deadline,
                )
                .map(|_| ()),
        }
    }
}

fn embedded_work_deadline_matrix(api: EmbeddedInsertApi) {
    use crate::node::SharedStorageNode;
    use crate::node_runtime::client_facade::LocalStorageNodeClient;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    for fenced in [false, true] {
        for occupied in [false, true] {
            // Keep admission live throughout. Only the process-local work
            // deadline expires, so the route fence cannot mask its omission.
            let _time = crate::clock::test_time_override_guard(1_000);
            let dir = test_util::tempdir();
            let node = Arc::new(SharedStorageNode::open(dir.path(), &[0]).unwrap());
            let client = LocalStorageNodeClient::new(NodeId::new(0), Arc::clone(&node));
            let target = {
                let pg = node.get_pg(0).unwrap();
                seed(&pg);
                if occupied {
                    insert(
                        &pg,
                        &command(&pg, false, 1),
                        AdmittedRouteEffectFence::unbounded(ClusterEpoch::INITIAL),
                    )
                    .unwrap();
                }
                command(&pg, matches!(api, EmbeddedInsertApi::BucketControl), 2)
            };
            let observe = || {
                let pg = node.get_pg(0).unwrap();
                (
                    pg.conn.total_changes(),
                    pg.metadata_checkpoint_table_blocks().unwrap(),
                    pg.metadata_command_replica_state().unwrap(),
                    pg.pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
                        .unwrap(),
                )
            };
            let before = observe();
            let fence = fenced
                .then(|| AdmittedRouteEffectFence::bounded(ClusterEpoch::INITIAL, 20_000, 19_000));
            let reached = Arc::new(AtomicBool::new(false));
            let hook_reached = Arc::clone(&reached);
            let deadline = Instant::now() + Duration::from_secs(1);
            let error = PgStore::test_with_before_pending_slot_effect_hook(
                move |in_transaction| {
                    assert!(in_transaction, "must hold SQLite write serialization");
                    hook_reached.store(true, Ordering::SeqCst);
                    // Wait for the captured Instant itself, not an approximate
                    // sleep or the unrelated route clock override.
                    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
                        std::thread::sleep(remaining);
                    }
                },
                || api.insert(&client, &target, fence, deadline),
            )
            .expect_err("expired work must not reach INSERT even while admission is live");
            assert!(
                reached.load(Ordering::SeqCst),
                "{api:?} must reach final effect hook"
            );
            assert!(
                matches!(error, StoreError::OperationDeadlineExceeded { context }
                if context == if matches!(api, EmbeddedInsertApi::BucketControl) {
                    "insert bucket control pending metadata command slot"
                } else {
                    "insert metadata command pending slot"
                }),
                "{api:?} fenced={fenced} occupied={occupied}: {error:?}",
            );
            assert_eq!(
                observe(),
                before,
                "expiry must precede every slot/sidecar SQL mutation, not only roll back",
            );
            assert!(node.get_pg(0).unwrap().conn.is_autocommit());
            if !occupied {
                api.insert(
                    &client,
                    &target,
                    fence,
                    Instant::now() + Duration::from_secs(30),
                )
                .unwrap();
                let installed = observe();
                assert!(
                    installed.0 > before.0,
                    "positive insertion canary must mutate SQL"
                );
                assert_eq!(
                    installed.3.as_ref().unwrap().command_bytes,
                    target.command_bytes()
                );
                api.insert(
                    &client,
                    &target,
                    fence,
                    Instant::now() + Duration::from_secs(30),
                )
                .unwrap();
                assert_eq!(observe(), installed, "exact retry must remain idempotent");
            }
        }
    }
}

#[test]
fn embedded_classified_pending_slot_checks_work_deadline_at_sql_effect() {
    embedded_work_deadline_matrix(EmbeddedInsertApi::Classified);
}

#[test]
fn embedded_generic_pending_slot_checks_work_deadline_at_sql_effect() {
    embedded_work_deadline_matrix(EmbeddedInsertApi::Generic);
}

#[test]
fn embedded_bucket_control_pending_slot_checks_work_deadline_at_sql_effect() {
    embedded_work_deadline_matrix(EmbeddedInsertApi::BucketControl);
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
