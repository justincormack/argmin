// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Three-command ownership under one read view; no installation or deletion.

use super::*;
use crate::node_runtime::pg_store::payload_staging::submission::PreparedPartAbandonmentReissue;

fn propose(
    pg: &PgStore,
    source: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
    new_epoch: bool,
) -> (ContextualPartAbandonmentReissue, MetadataCommandEnvelope) {
    let proposal = source_context(source)
        .into_submission()
        .prepare_part_abandonment(pg, 0, &topology(), cleanup.id())
        .unwrap();
    let successor = at(
        cleanup,
        if new_epoch {
            next_epoch(cleanup.id())
        } else {
            later(cleanup.id())
        },
    );
    let reissue = if new_epoch {
        proposal.into_epoch_reissue(successor.id())
    } else {
        proposal.into_same_epoch_reissue(successor.id())
    }
    .unwrap();
    (reissue, successor)
}

fn observed(
    prepared: &PreparedPartAbandonmentReissue<'_>,
    source: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
    successor: &MetadataCommandEnvelope,
    count: usize,
    terminal: bool,
) {
    assert_eq!(prepared.source().command_id, source.id());
    assert_eq!(prepared.previous_cleanup().command_id, cleanup.id());
    assert_eq!(prepared.proposed_cleanup().command_id, successor.id());
    for observation in [
        prepared.source(),
        prepared.previous_cleanup(),
        prepared.proposed_cleanup(),
    ] {
        assert_eq!(observation.installation, Observed::NoAcceptanceObserved);
    }
    assert_eq!(
        prepared.source_inventory(),
        PendingInventoryObservation {
            payload_reference_count: count,
            // Ordinary finalizer has its bucket-proof route. A source-bearing
            // cleanup additionally pins the original object-PG source route.
            route_dependency_count: 1,
            has_future_window: false,
        }
    );
    assert_eq!(
        prepared.previous_cleanup_inventory(),
        PendingInventoryObservation {
            payload_reference_count: if terminal { 0 } else { count },
            route_dependency_count: 2,
            has_future_window: false,
        }
    );
    assert_eq!(
        prepared.proposed_cleanup_inventory(),
        PendingInventoryObservation {
            payload_reference_count: if terminal { 0 } else { count },
            route_dependency_count: if successor.id().cluster_epoch()
                == cleanup.id().cluster_epoch()
            {
                2
            } else {
                3
            },
            has_future_window: false,
        }
    );
}

#[test]
fn staging_part_reissue_preparation_owns_three_commands_in_one_readonly_snapshot() {
    for count in [0, 2] {
        for state in ["active", "closed", "published", "completed"] {
            for new_epoch in [false, true] {
                for exit in ["finish", "drop", "unwind"] {
                    let Fixture {
                        dir,
                        pg,
                        owner,
                        source,
                        cleanup,
                    } = setup(count, true);
                    let cleanup = terminal_state(&pg, &owner, &source, cleanup, state);
                    let (reissue, successor) = propose(&pg, &source, &cleanup, new_epoch);
                    let original_prefix = *reissue.captured_prefix();
                    if state == "active" {
                        // Terminal-owner fixtures exercise standalone apply
                        // components, not the still-unconnected command carrier.
                        // Only the fully logged active fixture may checkpoint
                        // and reset its epoch; never forge a matching prefix.
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
                        if new_epoch {
                            let digest = pg.metadata_command_replica_state().unwrap().state_digest;
                            pg.initialize_metadata_transfer_matching_state(
                                0,
                                successor.id().cluster_epoch(),
                                0,
                                MetadataCommandLogHash::genesis(),
                                digest,
                            )
                            .unwrap();
                        }
                    }
                    drop(pg);
                    let mut pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
                    let before = introduction::state(&pg);
                    let prefix = pg.metadata_command_replica_state().unwrap();
                    let writes = pg.conn.total_changes();
                    let read_only = exit == "drop";
                    pg.conn
                        .pragma_update(None, "query_only", read_only)
                        .unwrap();
                    let trace = Arc::new(Mutex::new(Vec::new()));
                    let captured = Arc::clone(&trace);
                    pg.conn
                        .trace_v2(
                            TraceEventCodes::SQLITE_TRACE_STMT,
                            Some(move |event: TraceEvent<'_>| {
                                if let TraceEvent::Stmt(_, sql) = event {
                                    if ["BEGIN", "ROLLBACK", "COMMIT", "SAVEPOINT", "RELEASE"]
                                        .iter()
                                        .any(|p| sql.starts_with(p))
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
                        || reissue.prepare_pending(&pg, 0, &topology()),
                    )
                    .unwrap();
                    observed(
                        &prepared,
                        &source,
                        &cleanup,
                        &successor,
                        count,
                        state != "active",
                    );
                    assert_eq!(prepared.prefix(), &prefix);
                    assert!(!pg.conn.is_autocommit());
                    assert!(
                        pg.conn
                            .execute("DELETE FROM payload_staging_owners", [])
                            .is_err(),
                        "scope must remain read-only"
                    );
                    if exit == "finish" {
                        let reissue = prepared.finish().unwrap();
                        expected_reissue(&reissue, &source, &cleanup, &successor);
                        assert_eq!(reissue.captured_prefix(), &original_prefix);
                        if new_epoch && state == "active" {
                            assert_ne!(reissue.captured_prefix(), &prefix);
                        }
                    } else {
                        let outcome =
                            std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                                let _prepared = prepared;
                                assert_ne!(exit, "unwind", "unwind prepared reissue");
                            }));
                        assert_eq!(outcome.is_err(), exit == "unwind");
                    }
                    assert_eq!(*trace.lock().unwrap(), ["BEGIN DEFERRED", "ROLLBACK"]);
                    stop_trace(&mut pg);
                    restored(&pg, read_only);
                    assert_eq!(pg.conn.total_changes(), writes);
                    assert_eq!(introduction::state(&pg), before);
                    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                    pending_install::assert_empty(&pg);
                }
            }
        }
    }
}

#[test]
fn staging_part_reissue_preparation_rejects_crossed_scope_and_returns_all_commands_on_finish_failure(
) {
    for new_epoch in [false, true] {
        let Fixture {
            dir: _dir,
            pg,
            owner,
            source,
            cleanup,
        } = setup(2, true);
        let (reissue, successor) = propose(&pg, &source, &cleanup, new_epoch);
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let prefix = pg.metadata_command_replica_state().unwrap();
        let other_dir = test_util::tempdir();
        let other = PgStore::open(other_dir.path(), (pg.pg_id() + 1) % 8).unwrap();
        let other_before = introduction::state(&other);
        let failure = reissue
            .prepare_pending(&other, 0, &topology())
            .err()
            .expect("wrong PG");
        assert!(matches!(
            failure.error(),
            ContextError::CommandPg | ContextError::Abandonment(AbandonmentError::Subject)
        ));
        assert_eq!(introduction::state(&other), other_before);
        let reissue = failure.into_reissue();
        expected_reissue(&reissue, &source, &cleanup, &successor);
        let foreign = PgTopology::new(&[owner.subject.object_pg.get()]).unwrap();
        let failure = reissue
            .prepare_pending(&pg, 0, &foreign)
            .err()
            .expect("crossed topology");
        assert!(matches!(
            failure.error(),
            ContextError::Candidate(CandidateBindingError::Topology)
        ));
        let reissue = failure.into_reissue();
        expected_reissue(&reissue, &source, &cleanup, &successor);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let failure = reissue
            .prepare_pending(&pg, 0, &topology())
            .err()
            .expect("outer transaction");
        assert!(matches!(
            failure.error(),
            ContextError::Snapshot(crate::MetadataError::InvariantViolation { .. })
        ));
        assert!(!pg.conn.is_autocommit());
        let reissue = failure.into_reissue();
        expected_reissue(&reissue, &source, &cleanup, &successor);
        pg.conn.execute_batch("ROLLBACK").unwrap();
        let prepared = reissue.prepare_pending(&pg, 0, &topology()).unwrap();
        // Deterministic owner-local failure, not simulated disk durability.
        pg.conn.execute_batch("ROLLBACK").unwrap();
        let failure = prepared.finish().err().expect("snapshot already ended");
        assert!(matches!(
            failure.error(),
            ContextError::Snapshot(crate::MetadataError::Db {
                context: "primary metadata command build snapshot",
                ..
            })
        ));
        assert!(failure
            .error()
            .to_string()
            .contains("no transaction is active"));
        let reissue = failure.into_reissue();
        expected_reissue(&reissue, &source, &cleanup, &successor);
        let reissue = reissue
            .prepare_pending(&pg, 0, &topology())
            .unwrap()
            .finish()
            .unwrap();
        expected_reissue(&reissue, &source, &cleanup, &successor);
        restored(&pg, false);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        pending_install::assert_empty(&pg);
    }
}

// Exactly once per candidate, AFTER validating its original terminal chain.
// Counting log-row reads would depend on the length of that chain.
const TERMINAL_CHECK: &str = "FROM metadata_command_pending_slot WHERE singleton = 0";

#[test]
fn staging_part_reissue_preparation_authenticates_both_cleanups_in_one_view_and_rechecks_next_time()
{
    for new_epoch in [false, true] {
        let Fixture {
            dir,
            mut pg,
            source,
            cleanup,
            ..
        } = setup(2, true);
        let (reissue, successor) = propose(&pg, &source, &cleanup, new_epoch);
        let writer = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
        crate::node_runtime::pg_store::command_log::register_metadata_digest_sql_functions(&writer)
            .unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let captured = Arc::clone(&reads);
        let changed = Arc::new(AtomicUsize::new(0));
        let mutations = Arc::clone(&changed);
        let id = source.id();
        pg.conn.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(move |event: TraceEvent<'_>| {
            if let TraceEvent::Stmt(_, sql) = event {
                if sql.contains(TERMINAL_CHECK) && captured.fetch_add(1, Ordering::SeqCst) == 0 {
                    let removed = writer.execute("DELETE FROM metadata_command_log WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3", rusqlite::params![id.cluster_epoch().get(), id.pg_id().get(), id.log_index().get()]).unwrap();
                    assert_eq!(removed, 1);
                    mutations.fetch_add(1, Ordering::SeqCst);
                }
            }
        })).unwrap();
        let prepared = reissue.prepare_pending(&pg, 0, &topology()).unwrap();
        assert_eq!(
            reads.load(Ordering::SeqCst),
            2,
            "both candidates must authenticate their source"
        );
        assert_eq!(changed.load(Ordering::SeqCst), 1);
        observed(&prepared, &source, &cleanup, &successor, 2, false);
        let reissue = prepared.finish().unwrap();
        stop_trace(&mut pg);
        expected_reissue(&reissue, &source, &cleanup, &successor);
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let failure = reissue
            .prepare_pending(&pg, 0, &topology())
            .err()
            .expect("new snapshot must reject missing exact source evidence");
        assert!(
            matches!(failure.error(), ContextError::Abandonment(_)),
            "{}",
            failure.error()
        );
        expected_reissue(&failure.into_reissue(), &source, &cleanup, &successor);
        restored(&pg, false);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        pending_install::assert_empty(&pg);
    }
}

#[test]
fn staging_part_reissue_preparation_read_interruption_preserves_three_complete_contexts() {
    for new_epoch in [false, true] {
        for interrupted_read in [0, 1] {
            let Fixture {
                dir: _dir,
                mut pg,
                source,
                cleanup,
                ..
            } = setup(2, true);
            let (reissue, successor) = propose(&pg, &source, &cleanup, new_epoch);
            let before = introduction::state(&pg);
            let writes = pg.conn.total_changes();
            let interrupt = pg.conn.get_interrupt_handle();
            let reads = Arc::new(AtomicUsize::new(0));
            let captured = Arc::clone(&reads);
            pg.conn
                .trace_v2(
                    TraceEventCodes::SQLITE_TRACE_STMT,
                    Some(move |event: TraceEvent<'_>| {
                        if let TraceEvent::Stmt(_, sql) = event {
                            if sql.contains(TERMINAL_CHECK)
                                && captured.fetch_add(1, Ordering::SeqCst) == interrupted_read
                            {
                                interrupt.interrupt();
                            }
                        }
                    }),
                )
                .unwrap();
            let failure = reissue
                .prepare_pending(&pg, 0, &topology())
                .err()
                .expect("interrupted source authentication");
            assert_eq!(reads.load(Ordering::SeqCst), interrupted_read + 1);
            assert!(
                failure.error().to_string().contains("interrupt"),
                "{}",
                failure.error()
            );
            stop_trace(&mut pg);
            let reissue = failure.into_reissue();
            expected_reissue(&reissue, &source, &cleanup, &successor);
            let reissue = reissue
                .prepare_pending(&pg, 0, &topology())
                .unwrap()
                .finish()
                .unwrap();
            expected_reissue(&reissue, &source, &cleanup, &successor);
            restored(&pg, false);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            pending_install::assert_empty(&pg);
        }
    }
}

#[test]
fn staging_part_reissue_preparation_does_not_replace_apply_time_owner_validation() {
    for new_epoch in [false, true] {
        let Fixture {
            dir: _dir,
            pg,
            owner,
            source,
            cleanup,
        } = setup(2, true);
        let (reissue, successor) = propose(&pg, &source, &cleanup, new_epoch);
        let reissue = reissue
            .prepare_pending(&pg, 0, &topology())
            .unwrap()
            .finish()
            .unwrap();
        terminal_state(&pg, &owner, &source, cleanup.clone(), "closed");
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        // Protection inspection retains the original payload even after owner
        // closure. It does not recapture a proof-only abort or validate apply.
        let prepared = reissue.prepare_pending(&pg, 0, &topology()).unwrap();
        observed(&prepared, &source, &cleanup, &successor, 2, false);
        let reissue = prepared.finish().unwrap();
        expected_reissue(&reissue, &source, &cleanup, &successor);
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert!(matches!(
            cleanup_context(&source, &successor)
                .application(&topology())
                .unwrap()
                .apply_in_open_transaction(&pg, 0),
            Err(ApplicationError::Abandonment(
                AbandonmentError::TerminalResponsibility
            ))
        ));
        pg.conn.execute_batch("ROLLBACK").unwrap();
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        restored(&pg, false);
        pending_install::assert_empty(&pg);
    }
}
