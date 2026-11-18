// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Real complete-carrier slot transactions, not live RPC/custody activation.
//! Metadata-only fixtures deliberately do not create physical payloads.

use super::super::abandonment::source::PartFinalizationSource;
use super::super::command_context::ContextualCommand;
use super::super::submission::{
    ContextualSubmission, InstallationObservation as Observed, SubmissionError,
};
use super::*;
use crate::clock::test_time_override_guard;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ReserveObjectVersionCommand,
};
use crate::node_runtime::pg_store::PgStore;
use crate::types::AdmittedRouteEffectFence;
use crate::{StoreError, VersionId};
use std::time::{Duration, Instant};

pub(super) fn setup(case: usize) -> (test_util::TempDir, PgStore, ContextualCommand) {
    if case < 5 {
        let (target, append) = candidate::cases()[case].clone();
        let (dir, pg, owner, command) = candidate::setup(target, append);
        let context =
            ContextualCommand::from_described(candidate::capture(&pg, &owner, command, append));
        return (dir, pg, context);
    }
    let owner = fixture(multipart_target());
    let dir = test_util::tempdir();
    let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
    let context = if case == 5 {
        let command = MetadataCommandEnvelope::new(
            owner.subject.id.introduction,
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                owner.subject.bucket,
                owner.subject.key,
                VersionId::from_u64(17),
            )),
        );
        ContextualCommand::decode_field(command, &[1, 0, 0], &topology()).unwrap()
    } else {
        let owner = part::initialize(&pg, 2);
        let source = part::command(&pg, &owner);
        let cleanup = abandonment::cleanup(&pg, &owner, &source);
        let context = ContextualCommand::abandoned_part(
            PartFinalizationSource::capture_for_test(&source)
                .unwrap()
                .bind_cleanup(cleanup, &topology())
                .unwrap(),
            &topology(),
        )
        .unwrap();
        abandonment::terminal(&pg, &source, true);
        pg.conn.execute_batch("COMMIT").unwrap();
        context
    };
    (dir, pg, context)
}

pub(super) fn install(
    holder: &mut ContextualSubmission,
    pg: &PgStore,
) -> Result<(), SubmissionError> {
    holder.install(
        pg,
        0,
        &topology(),
        AdmittedRouteEffectFence::unbounded(holder.observation().command_id.cluster_epoch()),
        None,
    )
}

fn reject_expired(holder: &mut ContextualSubmission, pg: &PgStore) {
    let before = observe(pg);
    let writes = pg.conn.total_changes();
    let error = holder
        .install(
            pg,
            0,
            &topology(),
            AdmittedRouteEffectFence::unbounded(holder.observation().command_id.cluster_epoch()),
            Some(Instant::now()),
        )
        .unwrap_err();
    assert!(
        matches!(
            error,
            SubmissionError::Rejected(StoreError::OperationDeadlineExceeded { .. })
        ),
        "{error:?}"
    );
    assert_eq!(observe(pg), before);
    assert_eq!(pg.conn.total_changes(), writes);
}

pub(super) fn observe(pg: &PgStore) -> Vec<Vec<String>> {
    // The live v13 decoder must reject complete v14 slots until the coordinated
    // switch. Owner-local SQL observes exact bytes without weakening that gate.
    [
        "SELECT printf('%lld:%lld:%lld:%lld:%lld:%lld:%lld:%s', cluster_epoch, pg_id, log_index, command_checksum, publication_started, placed_segment_reference_count, reclaim_reference_count, scope_bucket) || ':' || hex(command_bytes) FROM metadata_command_pending_slot",
        "SELECT printf('%lld:%lld:%lld', command_checksum, dependency_count, dependency_checksum) || ':' || hex(encoded_dependencies) FROM metadata_command_pending_route_dependencies",
        "SELECT printf('%lld:%lld', command_checksum, protection_checksum) || ':' || hex(encoded_protection) FROM metadata_command_pending_staging_protection",
        "SELECT printf('%lld:%lld', page_index, reference_count) || ':' || hex(encoded_references) FROM metadata_command_pending_placed_reference_pages ORDER BY page_index",
        "SELECT printf('%lld:%lld', page_index, reference_count) || ':' || hex(encoded_references) FROM metadata_command_pending_reclaim_reference_pages ORDER BY page_index",
    ].into_iter().map(|sql| pg.conn.prepare(sql).unwrap().query_map([], |row| row.get(0)).unwrap().collect::<Result<Vec<_>, _>>().unwrap()).collect()
}

#[test]
fn staging_complete_pending_installs_exact_carrier_and_all_indexes_then_reopens() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..7 {
        let (dir, pg, context) = setup(case);
        let encoded = context.encode_command().unwrap();
        let bytes = encoded.bytes().to_vec();
        let checksum = encoded.checksum_crc64();
        let id = encoded.id();
        let snapshot = pg.command_build_snapshot().unwrap();
        let inventory = context.pending_inventory(&pg, &topology()).unwrap();
        let routes = inventory.retention().routes().to_vec();
        let window = inventory.retention().future_window().cloned();
        let pages = inventory.encode_reference_pages().unwrap();
        snapshot.finish().unwrap();
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let mut holder = context.into_submission();
        install(&mut holder, &pg).unwrap();
        assert_eq!(holder.observation().installation, Observed::Installed);
        let stored: (Vec<u8>, i64) = pg
            .conn
            .query_row(
                "SELECT command_bytes, command_checksum FROM metadata_command_pending_slot",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(stored, (bytes.clone(), checksum as i64));
        let decoded = ContextualCommand::decode_command(&bytes, checksum, &topology()).unwrap();
        holder.assert_context_for_test(&decoded, &topology());
        // Pin separation: installing v14 does not relax the current reader.
        assert!(pg.pending_metadata_command_slot_any_epoch(0).is_err());
        let indexed = pg
            .pending_metadata_command_route_references(0)
            .unwrap()
            .unwrap();
        assert_eq!(indexed.id, id);
        assert_eq!(indexed.command_checksum, checksum);
        assert_eq!(indexed.dependencies, routes);
        let staged = pg.pending_staging_protection(&topology()).unwrap().unwrap();
        assert_eq!(staged.command_checksum, checksum);
        assert_eq!(staged.window.map(|w| w.protection), window);
        for (table, expected) in [
            (
                "metadata_command_pending_placed_reference_pages",
                pages.placed_pages,
            ),
            (
                "metadata_command_pending_reclaim_reference_pages",
                pages.reclaim_pages,
            ),
        ] {
            let stored = pg
                .conn
                .prepare(&format!(
                    "SELECT encoded_references FROM {table} ORDER BY page_index"
                ))
                .unwrap()
                .query_map([], |row| row.get::<_, Vec<u8>>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(stored, expected);
        }
        let pending = observe(&pg);
        let writes = pg.conn.total_changes();
        install(&mut holder, &pg).unwrap();
        assert_eq!(
            pg.conn.total_changes(),
            writes,
            "exact retry rewrote evidence"
        );
        assert_eq!(observe(&pg), pending);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        drop(pg);
        let pg = PgStore::open(dir.path(), id.pg_id().get()).unwrap();
        assert_eq!(observe(&pg), pending);
        install(&mut holder, &pg).unwrap();
        reject_expired(&mut holder, &pg);
        assert_eq!(holder.observation().installation, Observed::Installed);
        let writes = pg.conn.total_changes();
        drop(holder);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(observe(&pg), pending);
    }
}

#[test]
fn staging_complete_pending_rolls_back_every_index_and_retains_uncertain_history() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..7 {
        for fault in 0..4 {
            let (dir, pg, context) = setup(case);
            let bytes = context.encode_command().unwrap().bytes().to_vec();
            let pg_id = pg.pg_id;
            let before = introduction::state(&pg);
            let mut holder = context.into_submission();
            let sql = match fault {
                0 => "CREATE TEMP TRIGGER fail_pending BEFORE INSERT ON metadata_command_pending_route_dependencies BEGIN SELECT RAISE(ABORT, 'complete route failure'); END;",
                1 | 3 => "CREATE TEMP TRIGGER fail_pending BEFORE INSERT ON metadata_command_pending_staging_protection BEGIN SELECT RAISE(ABORT, 'complete staging failure'); END;",
                2 => "CREATE TEMP TABLE deferred_failure (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES deferred_failure(id) DEFERRABLE INITIALLY DEFERRED); CREATE TEMP TRIGGER fail_pending AFTER INSERT ON metadata_command_pending_staging_protection BEGIN INSERT INTO deferred_failure VALUES (1, 2); END;",
                _ => unreachable!(),
            };
            pg.conn.execute_batch(sql).unwrap();
            if fault == 3 {
                pg.test_fail_metadata_command_rollbacks(1);
            }
            let writes = pg.conn.total_changes();
            let error = install(&mut holder, &pg).unwrap_err();
            let observed = if fault < 2 {
                assert!(matches!(error, SubmissionError::Rejected(_)), "{error:?}");
                Observed::NoAcceptanceObserved
            } else {
                assert!(
                    matches!(error, SubmissionError::Unconfirmed(_)),
                    "{error:?}"
                );
                assert!(error.to_string().contains(if fault == 2 {
                    "commit pending metadata command slot transaction"
                } else {
                    "injected metadata-command rollback failure"
                }));
                Observed::Unconfirmed
            };
            assert!(
                pg.conn.total_changes() > writes,
                "fault did not follow slot writes"
            );
            assert_eq!(holder.observation().installation, observed);
            let pg = if fault == 3 {
                assert!(!pg.conn.is_autocommit());
                assert!(pg.require_usable_connection().is_err());
                assert!(matches!(
                    install(&mut holder, &pg),
                    Err(SubmissionError::Unconfirmed(_))
                ));
                assert_eq!(holder.observation().installation, Observed::Unconfirmed);
                drop(pg);
                PgStore::open(dir.path(), pg_id).unwrap()
            } else {
                pg.conn.execute_batch("DROP TRIGGER fail_pending").unwrap();
                pg
            };
            pending_install::assert_empty(&pg);
            assert!(pg.conn.is_autocommit());
            assert_eq!(introduction::state(&pg), before);
            reject_expired(&mut holder, &pg);
            assert_eq!(holder.observation().installation, observed);
            install(&mut holder, &pg).unwrap();
            assert_eq!(holder.observation().installation, Observed::Installed);
            holder.assert_context_for_test(
                &ContextualCommand::decode_command(
                    &bytes,
                    checksum::crc64::checksum(&bytes),
                    &topology(),
                )
                .unwrap(),
                &topology(),
            );
        }
    }
}

#[test]
fn staging_complete_pending_exact_retry_rejects_damaged_indexes_without_repair() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..7 {
        for damage in [
            "UPDATE metadata_command_pending_slot SET scope_bucket = NULL",
            "UPDATE metadata_command_pending_slot SET scope_bucket = 'crossed-complete-install-bucket'",
            "DELETE FROM metadata_command_pending_route_dependencies",
            "UPDATE metadata_command_pending_route_dependencies SET command_sha256 = zeroblob(32)",
            "UPDATE metadata_command_pending_route_dependencies SET dependency_checksum = dependency_checksum + 1",
            "DELETE FROM metadata_command_pending_staging_protection",
            "UPDATE metadata_command_pending_staging_protection SET protection_checksum = protection_checksum + 1",
            "UPDATE metadata_command_pending_slot SET placed_segment_reference_count = placed_segment_reference_count + 1",
        ] {
            let (_dir, pg, context) = setup(case);
            let expected_scope = context.encode_command().unwrap().scope_bucket().as_str().to_owned();
            let mut holder = context.into_submission();
            install(&mut holder, &pg).unwrap();
            let scope = || pg.conn.query_row("SELECT scope_bucket FROM metadata_command_pending_slot", [], |row| row.get::<_, Option<String>>(0)).unwrap();
            assert_eq!(scope().as_deref(), Some(expected_scope.as_str()));
            pg.conn.execute_batch(damage).unwrap();
            if damage.contains("scope_bucket") {
                assert_ne!(scope().as_deref(), Some(expected_scope.as_str()));
            }
            let before = observe(&pg);
            let writes = pg.conn.total_changes();
            let error = install(&mut holder, &pg).unwrap_err();
            assert!(matches!(error, SubmissionError::Unconfirmed(_)), "{error:?}");
            if damage.contains("scope_bucket") {
                assert!(matches!(error, SubmissionError::Unconfirmed(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "pending-staging-install-existing-scope",
                })), "{error:?}");
            }
            assert_eq!(holder.observation().installation, Observed::Unconfirmed);
            assert_eq!(observe(&pg), before);
            assert_eq!(pg.conn.total_changes(), writes);
            reject_expired(&mut holder, &pg);
            assert_eq!(holder.observation().installation, Observed::Unconfirmed);
        }
    }
}

#[test]
fn staging_complete_pending_expiry_unwind_topology_and_outer_transaction_fail_closed() {
    let clock = test_time_override_guard(2_000);
    for case in 0..7 {
        let (_dir, pg, context) = setup(case);
        let mut holder = context.into_submission();
        let id = holder.observation().command_id;
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(matches!(
            install(&mut holder, &pg),
            Err(SubmissionError::OpenTransaction)
        ));
        assert!(!pg.conn.is_autocommit());
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert!(matches!(
            holder.install(
                &pg,
                0,
                &PgTopology::new(&[id.pg_id().get()]).unwrap(),
                AdmittedRouteEffectFence::unbounded(id.cluster_epoch()),
                None
            ),
            Err(SubmissionError::Rejected(_))
        ));
        for work_deadline in [false, true] {
            let deadline = Instant::now() + Duration::from_millis(10);
            let control = clock.control();
            let error = PgStore::test_with_before_pending_slot_effect_hook(
                move |in_transaction| {
                    assert!(in_transaction);
                    if work_deadline {
                        std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
                    } else {
                        control.set(4_500);
                    }
                },
                || {
                    holder.install(
                        &pg,
                        0,
                        &topology(),
                        AdmittedRouteEffectFence::bounded(id.cluster_epoch(), 5_000, 4_000),
                        work_deadline.then_some(deadline),
                    )
                },
            )
            .unwrap_err();
            assert!(matches!(error, SubmissionError::Rejected(_)), "{error:?}");
            if work_deadline {
                assert!(matches!(
                    error,
                    SubmissionError::Rejected(StoreError::OperationDeadlineExceeded { .. })
                ));
            }
            assert_eq!(
                holder.observation().installation,
                Observed::NoAcceptanceObserved
            );
            clock.set(2_000);
        }
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            PgStore::test_with_before_pending_slot_effect_hook(
                |in_transaction| {
                    assert!(in_transaction);
                    panic!("complete installation interrupted");
                },
                || install(&mut holder, &pg),
            )
        }));
        assert!(panic.is_err());
        assert_eq!(holder.observation().installation, Observed::Unconfirmed);
        assert!(pg.conn.is_autocommit());
        pg.require_usable_connection().unwrap();
        reject_expired(&mut holder, &pg);
        assert_eq!(holder.observation().installation, Observed::Unconfirmed);
        assert_eq!(pg.conn.total_changes(), writes);
        pending_install::assert_empty(&pg);
        assert_eq!(introduction::state(&pg), before);
        install(&mut holder, &pg).unwrap();
    }
}

#[test]
fn staging_complete_pending_unwind_after_all_indexes_preserves_holder_until_reopen() {
    let _clock = test_time_override_guard(2_000);
    for case in 0..7 {
        for failed_rollback in [false, true] {
            let (dir, pg, context) = setup(case);
            let pg_id = pg.pg_id;
            let bytes = context.encode_command().unwrap().bytes().to_vec();
            let before = introduction::state(&pg);
            let writes = pg.conn.total_changes();
            let mut holder = context.into_submission();
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                PgStore::test_with_before_pending_slot_finish_hook(
                    move |pg| {
                        assert!(pg.conn.total_changes() > writes);
                        assert!(pg
                            .pending_staging_protection(&topology())
                            .unwrap()
                            .is_some());
                        if failed_rollback {
                            pg.test_fail_metadata_command_rollbacks(1);
                        }
                        panic!("interrupted after complete pending indexes");
                    },
                    || install(&mut holder, &pg),
                )
            }));
            assert!(panic.is_err());
            assert_eq!(holder.observation().installation, Observed::Unconfirmed);
            holder.assert_context_for_test(
                &ContextualCommand::decode_command(
                    &bytes,
                    checksum::crc64::checksum(&bytes),
                    &topology(),
                )
                .unwrap(),
                &topology(),
            );
            if failed_rollback {
                assert!(!pg.conn.is_autocommit());
                assert!(pg.require_usable_connection().is_err());
                assert!(matches!(
                    install(&mut holder, &pg),
                    Err(SubmissionError::Unconfirmed(_))
                ));
                assert!(!pg.conn.is_autocommit(), "hidden teardown retry");
            } else {
                assert!(pg.conn.is_autocommit());
                pg.require_usable_connection().unwrap();
                pending_install::assert_empty(&pg);
            }
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            assert_eq!(introduction::state(&pg), before);
            pending_install::assert_empty(&pg);
            reject_expired(&mut holder, &pg);
            assert_eq!(holder.observation().installation, Observed::Unconfirmed);
            install(&mut holder, &pg).unwrap();
            assert_eq!(holder.observation().installation, Observed::Installed);
        }
    }
}

#[test]
fn staging_complete_pending_source_is_revalidated_inside_install_transaction() {
    let _clock = test_time_override_guard(2_000);
    let (_dir, pg, context) = setup(6);
    // A successful earlier preparation cannot authorize installation after the
    // linked source evidence disappears. Keep the complete context and history.
    let holder = context.into_submission();
    let prepared = holder
        .prepare_abandoned_part_pending(&pg, 0, &topology())
        .unwrap();
    let mut holder = prepared.finish().unwrap();
    pg.conn.execute_batch("DELETE FROM metadata_command_log; DELETE FROM metadata_command_terminal_receipts; DELETE FROM metadata_command_checkpoints").unwrap();
    let writes = pg.conn.total_changes();
    let error = install(&mut holder, &pg).unwrap_err();
    assert!(matches!(error, SubmissionError::Rejected(_)), "{error:?}");
    assert_eq!(
        holder.observation().installation,
        Observed::NoAcceptanceObserved
    );
    assert_eq!(pg.conn.total_changes(), writes);
    pending_install::assert_empty(&pg);
}

#[test]
fn staging_complete_pending_same_draft_cannot_replace_a_different_complete_context() {
    let _clock = test_time_override_guard(2_000);
    for install_source_first in [false, true] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, 2);
        let source = part::command(&pg, &owner);
        let cleanup = abandonment::cleanup(&pg, &owner, &source);
        let ordinary =
            ContextualCommand::decode_field(cleanup.clone(), &[1, 0, 0], &topology()).unwrap();
        let with_source = ContextualCommand::abandoned_part(
            PartFinalizationSource::capture_for_test(&source)
                .unwrap()
                .bind_cleanup(cleanup, &topology())
                .unwrap(),
            &topology(),
        )
        .unwrap();
        abandonment::terminal(&pg, &source, true);
        pg.conn.execute_batch("COMMIT").unwrap();
        let (first, crossed) = if install_source_first {
            (with_source, ordinary)
        } else {
            (ordinary, with_source)
        };
        assert_eq!(first.id(), crossed.id());
        assert_ne!(
            first.encode_command().unwrap().bytes(),
            crossed.encode_command().unwrap().bytes()
        );
        let mut first = first.into_submission();
        let mut crossed = crossed.into_submission();
        install(&mut first, &pg).unwrap();
        let before = observe(&pg);
        let writes = pg.conn.total_changes();
        let error = install(&mut crossed, &pg).unwrap_err();
        assert!(
            matches!(
                error,
                SubmissionError::Rejected(StoreError::MetadataCommandPendingConflict { .. })
            ),
            "{error:?}"
        );
        assert_eq!(
            crossed.observation().installation,
            Observed::NoAcceptanceObserved
        );
        assert_eq!(observe(&pg), before);
        assert_eq!(pg.conn.total_changes(), writes);
        install(&mut first, &pg).unwrap();
    }
}

#[test]
fn staging_complete_pending_uses_one_existing_transaction_for_slot_and_indexes() {
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    use std::sync::{Arc, Mutex};
    let _clock = test_time_override_guard(2_000);
    for case in 0..7 {
        let (_dir, mut pg, context) = setup(case);
        let statements = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&statements);
        pg.conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        captured.lock().unwrap().push(sql.to_owned());
                    }
                }),
            )
            .unwrap();
        install(&mut context.into_submission(), &pg).unwrap();
        let statements = statements.lock().unwrap();
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.starts_with("BEGIN") || sql.starts_with("SAVEPOINT"))
                .count(),
            1
        );
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.as_str() == "BEGIN IMMEDIATE")
                .count(),
            1
        );
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.as_str() == "COMMIT")
                .count(),
            1
        );
        let commit = statements.iter().position(|sql| sql == "COMMIT").unwrap();
        for table in [
            "metadata_command_pending_slot",
            "metadata_command_pending_route_dependencies",
            "metadata_command_pending_staging_protection",
        ] {
            assert!(
                statements
                    .iter()
                    .position(|sql| sql.starts_with(&format!("INSERT INTO {table} ")))
                    .unwrap()
                    < commit
            );
        }
    }
}
