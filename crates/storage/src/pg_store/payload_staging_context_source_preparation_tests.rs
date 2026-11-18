// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only terminal-source inspection; never an installation/deletion grant.

use super::super::super::abandonment::AbandonmentError;
use super::super::super::command_context::{ApplicationError, ContextError};
use super::*;
use crate::control_plane::MetadataCommandLogHash;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::sync::{atomic::AtomicUsize, Arc, Mutex};

struct Fixture {
    dir: test_util::TempDir,
    pg: PgStore,
    owner: StagingOwnerRecord,
    source: MetadataCommandEnvelope,
    cleanup: MetadataCommandEnvelope,
}

fn setup(count: usize) -> Fixture {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, count);
    let source = part::command(&pg, &owner);
    let cleanup = abandonment::cleanup(&pg, &owner, &source);
    Fixture {
        dir,
        pg,
        owner,
        source,
        cleanup,
    }
}

fn context(
    source: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
) -> ContextualCommand {
    ContextualCommand::abandoned_part(
        PartFinalizationSource::capture_for_test(source)
            .unwrap()
            .bind_cleanup(cleanup.clone(), &topology())
            .unwrap(),
        &topology(),
    )
    .unwrap()
}

fn stop_trace(pg: &mut PgStore) {
    pg.conn
        .trace_v2(TraceEventCodes::empty(), None::<fn(TraceEvent<'_>)>)
        .unwrap();
}

fn restored(pg: &PgStore, read_only: bool) {
    assert!(pg.conn.is_autocommit());
    assert_eq!(
        pg.conn
            .pragma_query_value(None, "query_only", |row| row.get::<_, bool>(0))
            .unwrap(),
        read_only
    );
}

#[test]
fn staging_context_source_preparation_preserves_exact_context_after_compaction_restart_and_epoch_reset(
) {
    for count in [0, 2] {
        for compact in [false, true] {
            for reset_epoch in [false, true] {
                for read_only in [false, true] {
                    let Fixture {
                        dir,
                        pg,
                        owner,
                        source,
                        mut cleanup,
                    } = setup(count);
                    abandonment::terminal(&pg, &source, true);
                    pg.conn.execute_batch("COMMIT").unwrap();
                    if compact {
                        pg.record_current_metadata_command_checkpoint(
                            0,
                            source.id().cluster_epoch(),
                        )
                        .unwrap();
                        pg.compact_metadata_command_log(source.id().cluster_epoch())
                            .unwrap();
                        assert_eq!(
                            pg.metadata_command_log_stats(source.id().cluster_epoch())
                                .unwrap()
                                .retained_entries,
                            0
                        );
                    }
                    if reset_epoch {
                        let epoch =
                            ClusterEpoch::new(source.id().cluster_epoch().get() + 1).unwrap();
                        let digest = pg.metadata_command_replica_state().unwrap().state_digest;
                        pg.initialize_metadata_transfer_matching_state(
                            0,
                            epoch,
                            0,
                            MetadataCommandLogHash::genesis(),
                            digest,
                        )
                        .unwrap();
                        cleanup = MetadataCommandEnvelope::new(
                            MetadataCommandId::new(
                                epoch,
                                source.id().pg_id(),
                                MetadataCommandLogIndex::new(1).unwrap(),
                            ),
                            cleanup.payload().clone(),
                        );
                    }
                    let holder = context(&source, &cleanup).into_submission();
                    let expected = context(&source, &cleanup);
                    drop(pg);
                    let mut pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
                    let before = introduction::state(&pg);
                    let prefix = pg.metadata_command_replica_state().unwrap();
                    let changes = pg.conn.total_changes();
                    pg.conn
                        .pragma_update(None, "query_only", read_only)
                        .unwrap();
                    let transactions = Arc::new(Mutex::new(Vec::new()));
                    let captured = Arc::clone(&transactions);
                    pg.conn
                        .trace_v2(
                            TraceEventCodes::SQLITE_TRACE_STMT,
                            Some(move |event: TraceEvent<'_>| {
                                if let TraceEvent::Stmt(_, sql) = event {
                                    if ["BEGIN", "ROLLBACK", "COMMIT", "SAVEPOINT", "RELEASE"]
                                        .iter()
                                        .any(|prefix| sql.starts_with(prefix))
                                    {
                                        captured.lock().unwrap().push(sql.to_owned());
                                    }
                                }
                            }),
                        )
                        .unwrap();
                    let prepared = crate::clock::with_time_and_monotonic_override(
                        9_000_000,
                        90_000_000,
                        || holder.prepare_abandoned_part_pending(&pg, 0, &topology()),
                    )
                    .unwrap();
                    assert_eq!(prepared.prefix(), &prefix);
                    assert_eq!(prepared.observation().command_id, cleanup.id());
                    assert_eq!(
                        prepared.observation().installation,
                        Observed::NoAcceptanceObserved
                    );
                    assert_eq!(prepared.inventory().payload_reference_count, count);
                    assert_eq!(
                        prepared.inventory().route_dependency_count,
                        if reset_epoch { 3 } else { 2 }
                    );
                    assert!(!prepared.inventory().has_future_window);
                    let holder = prepared.finish().unwrap();
                    assert_eq!(
                        *transactions.lock().unwrap(),
                        ["BEGIN DEFERRED", "ROLLBACK"]
                    );
                    stop_trace(&mut pg);
                    restored(&pg, read_only);
                    holder.assert_context_for_test(&expected, &topology());
                    drop(holder);
                    assert_eq!(pg.conn.total_changes(), changes);
                    assert_eq!(introduction::state(&pg), before);
                    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                    pending_install::assert_empty(&pg);
                }
            }
        }
    }
}

#[test]
fn staging_context_source_preparation_rejects_missing_applied_unlinked_started_and_substituted_evidence(
) {
    for count in [0, 2] {
        for case in [
            "absent",
            "pending",
            "unlinked",
            "applied",
            "started",
            "reservation",
            "owner",
        ] {
            let Fixture {
                dir: _dir,
                pg,
                source,
                cleanup,
                ..
            } = setup(count);
            let mut inspected_source = source.clone();
            match case {
                "absent" => (),
                "pending" => pg
                    .test_replace_pending_metadata_command_slot(&source, None)
                    .unwrap(),
                "unlinked" => pg
                    .test_insert_abandoned_metadata_command_log_entry(&source)
                    .unwrap(),
                "applied" => {
                    abandonment::terminal(&pg, &source, false);
                }
                "started" => {
                    abandonment::terminal(&pg, &source, true);
                    // Deliberately contradictory durable rows, not a legal
                    // publication/abandonment schedule.
                    pg.test_replace_pending_metadata_command_slot(&source, None)
                        .unwrap();
                    pg.test_mark_pending_metadata_command_publication_started(&source)
                        .unwrap();
                }
                "reservation" | "owner" => {
                    abandonment::terminal(&pg, &source, true);
                    inspected_source =
                        abandonment::source_carrier::crossed_proof_with_equal_crc(&source, case);
                    assert_eq!(inspected_source.checksum_crc64(), source.checksum_crc64());
                    assert_ne!(inspected_source.command_sha256(), source.command_sha256());
                }
                _ => unreachable!(),
            }
            pg.conn.execute_batch("COMMIT").unwrap();
            let expected = context(&inspected_source, &cleanup);
            let before = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let pending = pending_install::observe(&pg);
            let pages = pending_payload_transaction::pages(&pg);
            let writes = pg.conn.total_changes();
            let failure = context(&inspected_source, &cleanup)
                .into_submission()
                .prepare_abandoned_part_pending(&pg, 0, &topology())
                .err()
                .expect(case);
            // The exact complete finalizer is now applied, not abandoned. Its
            // identity authenticates successfully but cannot authorize cleanup.
            if ["absent", "pending", "started", "applied"].contains(&case) {
                assert!(
                    matches!(
                        failure.error(),
                        ContextError::Abandonment(AbandonmentError::NotAbandoned)
                    ),
                    "{case}: {:?}",
                    failure.error()
                );
            } else {
                assert!(
                    matches!(
                        failure.error(),
                        ContextError::Abandonment(AbandonmentError::Store(
                            crate::StoreError::MetadataCommandLogConflict { .. }
                        ))
                    ),
                    "{case}: {:?}",
                    failure.error()
                );
            }
            let holder = failure.into_submission();
            holder.assert_context_for_test(&expected, &topology());
            assert_eq!(
                holder.observation().installation,
                Observed::NoAcceptanceObserved
            );
            drop(holder);
            restored(&pg, false);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            assert_eq!(pending_install::observe(&pg), pending);
            assert_eq!(pending_payload_transaction::pages(&pg), pages);
        }
    }
}

#[test]
fn staging_context_source_preparation_rejects_other_variants_without_resetting_history() {
    let _clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        for history in [
            Observed::NoAcceptanceObserved,
            Observed::Unconfirmed,
            Observed::Installed,
        ] {
            let (_dir, mut pg, owner, command) = candidate::setup(target.clone(), append);
            let candidate = candidate::capture(&pg, &owner, command.clone(), append);
            let expected = described_context(&command, &candidate.descriptor_bytes(), append);
            let mut holder = candidate.into_submission();
            match history {
                Observed::NoAcceptanceObserved => (),
                Observed::Unconfirmed => interrupt(&mut holder, &pg),
                Observed::Installed => submission::install(&mut holder, &pg).unwrap(),
            }
            let before = introduction::state(&pg);
            let pending = pending_install::observe(&pg);
            let writes = pg.conn.total_changes();
            let statements = Arc::new(AtomicUsize::new(0));
            let captured = Arc::clone(&statements);
            pg.conn
                .trace_v2(
                    TraceEventCodes::SQLITE_TRACE_STMT,
                    Some(move |event: TraceEvent<'_>| {
                        if matches!(event, TraceEvent::Stmt(_, _)) {
                            captured.fetch_add(1, Ordering::SeqCst);
                        }
                    }),
                )
                .unwrap();
            let failure = holder
                .into_contextual()
                .prepare_abandoned_part_pending(&pg, 0, &topology())
                .err()
                .unwrap();
            assert!(matches!(failure.error(), ContextError::CommandKind));
            assert_eq!(statements.load(Ordering::SeqCst), 0);
            stop_trace(&mut pg);
            let holder = failure.into_submission();
            holder.assert_context_for_test(&expected, &topology());
            assert_eq!(holder.observation().installation, history);
            drop(holder);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pending_install::observe(&pg), pending);
            restored(&pg, false);
        }
    }
    // Same abort payload, but an ordinary context is not source-bearing.
    let Fixture {
        dir: _dir,
        pg,
        cleanup,
        ..
    } = setup(2);
    pg.conn.execute_batch("COMMIT").unwrap();
    let ordinary = ContextualCommand::decode_field(cleanup, &[1, 0, 0], &topology()).unwrap();
    let failure = ordinary
        .into_submission()
        .prepare_abandoned_part_pending(&pg, 0, &topology())
        .err()
        .unwrap();
    assert!(matches!(failure.error(), ContextError::CommandKind));
    restored(&pg, false);
}

#[test]
fn staging_context_source_preparation_errors_restore_snapshot_and_retain_source() {
    for count in [0, 2] {
        for read_only in [false, true] {
            let Fixture {
                dir: _dir,
                mut pg,
                source,
                cleanup,
                ..
            } = setup(count);
            abandonment::terminal(&pg, &source, true);
            pg.conn.execute_batch("COMMIT").unwrap();
            let expected = context(&source, &cleanup);
            let holder = context(&source, &cleanup).into_submission();
            let before = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let writes = pg.conn.total_changes();
            pg.conn
                .pragma_update(None, "query_only", read_only)
                .unwrap();
            let fired = Arc::new(AtomicUsize::new(0));
            let captured = Arc::clone(&fired);
            let interrupt = pg.conn.get_interrupt_handle();
            pg.conn
                .trace_v2(
                    TraceEventCodes::SQLITE_TRACE_STMT,
                    Some(move |event: TraceEvent<'_>| {
                        if let TraceEvent::Stmt(_, sql) = event {
                            if sql.contains("FROM metadata_command_log ")
                                && captured.fetch_add(1, Ordering::SeqCst) == 0
                            {
                                interrupt.interrupt();
                            }
                        }
                    }),
                )
                .unwrap();
            let failure = holder
                .prepare_abandoned_part_pending(&pg, 0, &topology())
                .err()
                .expect("terminal read interruption");
            assert_eq!(fired.load(Ordering::SeqCst), 1);
            stop_trace(&mut pg);
            assert!(failure.error().to_string().contains("interrupt"));
            assert!(matches!(
                failure.error(),
                ContextError::Abandonment(AbandonmentError::Store(crate::StoreError::Db {
                    context: "load historical terminal metadata command for startup validation",
                    ..
                }))
            ));
            let holder = failure.into_submission();
            holder.assert_context_for_test(&expected, &topology());
            restored(&pg, read_only);
            pg.conn.execute_batch("BEGIN").unwrap();
            let failure = holder
                .prepare_abandoned_part_pending(&pg, 0, &topology())
                .err()
                .unwrap();
            assert!(matches!(
                failure.error(),
                ContextError::Snapshot(crate::MetadataError::InvariantViolation { .. })
            ));
            assert!(!pg.conn.is_autocommit());
            pg.conn.execute_batch("ROLLBACK").unwrap();
            let holder = failure.into_submission();
            let prepared = holder
                .prepare_abandoned_part_pending(&pg, 0, &topology())
                .unwrap();
            pg.conn.execute_batch("ROLLBACK").unwrap();
            let failure = prepared.finish().err().expect("snapshot already ended");
            assert!(matches!(
                failure.error(),
                ContextError::Snapshot(crate::MetadataError::Db { .. })
            ));
            let holder = failure.into_submission();
            holder.assert_context_for_test(&expected, &topology());
            restored(&pg, read_only);
            let prepared = holder
                .prepare_abandoned_part_pending(&pg, 0, &topology())
                .unwrap();
            drop(prepared);
            restored(&pg, read_only);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            pending_install::assert_empty(&pg);
        }
    }
}

#[test]
fn staging_context_source_preparation_snapshot_does_not_authorize_later_apply() {
    for count in [0, 2] {
        let Fixture {
            dir,
            mut pg,
            source,
            cleanup,
            ..
        } = setup(count);
        abandonment::terminal(&pg, &source, true);
        pg.conn.execute_batch("COMMIT").unwrap();
        let expected = context(&source, &cleanup);
        let holder = context(&source, &cleanup).into_submission();
        let writer = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
        let changed = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&changed);
        let index = source.id().log_index().get();
        pg.conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        if sql.contains("FROM metadata_command_log ") {
                            let mut change = captured.lock().unwrap();
                            if change.is_none() {
                                *change = Some(writer.execute(
                                    "DELETE FROM metadata_command_log WHERE log_index = ?1",
                                    [index],
                                ));
                            }
                        }
                    }
                }),
            )
            .unwrap();
        let prepared = holder
            .prepare_abandoned_part_pending(&pg, 0, &topology())
            .unwrap();
        assert_eq!(
            changed.lock().unwrap().as_ref().unwrap().as_ref().unwrap(),
            &1
        );
        assert_eq!(prepared.inventory().payload_reference_count, count);
        let holder = prepared.finish().unwrap();
        stop_trace(&mut pg);
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        let failure = holder
            .prepare_abandoned_part_pending(&pg, 0, &topology())
            .err()
            .expect("new snapshot must reject removed source evidence");
        assert!(matches!(
            failure.error(),
            ContextError::Abandonment(AbandonmentError::NotAbandoned)
        ));
        let holder = failure.into_submission();
        holder.assert_context_for_test(&expected, &topology());
        assert_eq!(
            holder.observation().installation,
            Observed::NoAcceptanceObserved
        );
        // Independently retained exact command, as a receiver would hold. The
        // successful prior inspection is not a certificate accepted by apply.
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(matches!(
            expected
                .application(&topology())
                .unwrap()
                .apply_in_open_transaction(&pg, 0),
            Err(ApplicationError::Abandonment(
                AbandonmentError::NotAbandoned
            ))
        ));
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        pending_install::assert_empty(&pg);
        restored(&pg, false);
    }
}
