// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! A reissued cleanup retains the ORIGINAL finalizer, not its immediate
//! predecessor cleanup. Evidence and both inventories share one read view.

use super::*;
use crate::control_plane::MetadataCommandLogHash;
use crate::node_runtime::pg_store::payload_staging::abandonment::AbandonmentError;
use crate::node_runtime::pg_store::payload_staging::command_context::ApplicationError;
use std::sync::atomic::AtomicBool;

const FRESH: (Observed, Observed) = (
    Observed::NoAcceptanceObserved,
    Observed::NoAcceptanceObserved,
);

struct SourceFixture {
    dir: test_util::TempDir,
    pg: PgStore,
    finalizer: MetadataCommandEnvelope,
    cleanup: MetadataCommandEnvelope,
}

fn setup(count: usize) -> SourceFixture {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, count);
    let finalizer = part::command(&pg, &owner);
    let cleanup = abandonment::cleanup(&pg, &owner, &finalizer);
    SourceFixture {
        dir,
        pg,
        finalizer,
        cleanup,
    }
}

fn context(
    finalizer: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
) -> ContextualCommand {
    ContextualCommand::abandoned_part(
        PartFinalizationSource::capture(finalizer)
            .unwrap()
            .bind_cleanup(cleanup.clone(), &topology())
            .unwrap(),
        &topology(),
    )
    .unwrap()
}

fn pair(
    finalizer: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
) -> (ContextualReissue, ContextualCommand, ContextualCommand) {
    let successor = MetadataCommandEnvelope::new(next(cleanup.id()), cleanup.payload().clone());
    let pair = context(finalizer, cleanup)
        .into_submission()
        .into_same_epoch_reissue(successor.id())
        .unwrap();
    (
        pair,
        context(finalizer, cleanup),
        context(finalizer, &successor),
    )
}

#[test]
fn staging_context_source_reissue_preparation_retains_original_source_across_reopen_and_epoch_reset(
) {
    for count in [0, 2] {
        for compact in [false, true] {
            for reset_epoch in [false, true] {
                for original_read_only in [false, true] {
                    let SourceFixture {
                        dir,
                        pg,
                        finalizer,
                        mut cleanup,
                    } = setup(count);
                    abandonment::terminal(&pg, &finalizer, true);
                    pg.conn.execute_batch("COMMIT").unwrap();
                    if compact {
                        pg.record_current_metadata_command_checkpoint(
                            0,
                            finalizer.id().cluster_epoch(),
                        )
                        .unwrap();
                        pg.compact_metadata_command_log(finalizer.id().cluster_epoch())
                            .unwrap();
                        assert_eq!(
                            pg.metadata_command_log_stats(finalizer.id().cluster_epoch())
                                .unwrap()
                                .retained_entries,
                            0
                        );
                    }
                    if reset_epoch {
                        let epoch =
                            ClusterEpoch::new(finalizer.id().cluster_epoch().get() + 1).unwrap();
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
                                cleanup.id().pg_id(),
                                MetadataCommandLogIndex::new(1).unwrap(),
                            ),
                            cleanup.payload().clone(),
                        );
                    }
                    let (pair, source, successor) = pair(&finalizer, &cleanup);
                    drop(pg);
                    let mut pg = PgStore::open(dir.path(), cleanup.id().pg_id().get()).unwrap();
                    let state = introduction::state(&pg);
                    let prefix = pg.metadata_command_replica_state().unwrap();
                    let writes = pg.conn.total_changes();
                    pg.conn
                        .pragma_update(None, "query_only", original_read_only)
                        .unwrap();
                    let trace = Arc::new(Mutex::new(Vec::new()));
                    let captured = Arc::clone(&trace);
                    pg.conn
                        .trace_v2(
                            TraceEventCodes::SQLITE_TRACE_STMT,
                            Some(move |event: TraceEvent<'_>| {
                                if let TraceEvent::Stmt(_, sql) = event {
                                    if ["BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE"]
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
                        || pair.prepare_abandoned_part_pending(&pg, 0, &topology()),
                    )
                    .unwrap();
                    assert_eq!(prepared.prefix(), &prefix);
                    assert_eq!(prepared.source().command_id, source.id());
                    assert_eq!(prepared.successor().command_id, successor.id());
                    assert_eq!(prepared.source().installation, FRESH.0);
                    assert_eq!(prepared.successor().installation, FRESH.1);
                    let expected = PendingInventoryObservation {
                        payload_reference_count: count,
                        route_dependency_count: if reset_epoch { 3 } else { 2 },
                        has_future_window: false,
                    };
                    assert_eq!(prepared.source_inventory(), expected);
                    assert_eq!(prepared.successor_inventory(), expected);
                    let pair = prepared.finish().unwrap();
                    assert_eq!(*trace.lock().unwrap(), ["BEGIN DEFERRED", "ROLLBACK"]);
                    stop_trace(&mut pg);
                    assert_pair(&pair, &source, &successor, FRESH);
                    assert!(
                        pair.into_observed_successor().is_err(),
                        "inspection cannot authorize source retirement"
                    );
                    restored(&pg, original_read_only);
                    assert_eq!(pg.conn.total_changes(), writes);
                    assert_eq!(introduction::state(&pg), state);
                    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                    pending_install::assert_empty(&pg);
                }
            }
        }
    }
}

#[test]
fn staging_context_source_reissue_preparation_rejects_invalid_terminal_evidence_without_losing_pair(
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
            let SourceFixture {
                dir: _dir,
                pg,
                finalizer,
                cleanup,
            } = setup(count);
            let mut inspected = finalizer.clone();
            match case {
                "absent" => (),
                "pending" => pg
                    .test_replace_pending_metadata_command_slot(&finalizer, None)
                    .unwrap(),
                "unlinked" => pg
                    .test_insert_abandoned_metadata_command_log_entry(&finalizer)
                    .unwrap(),
                "applied" => {
                    part::apply(&pg, &finalizer).unwrap();
                    abandonment::terminal(&pg, &finalizer, false);
                }
                "started" => {
                    abandonment::terminal(&pg, &finalizer, true);
                    // Corrupt contradictory evidence, not a legal schedule.
                    pg.test_replace_pending_metadata_command_slot(&finalizer, None)
                        .unwrap();
                    pg.test_mark_pending_metadata_command_publication_started(&finalizer)
                        .unwrap();
                }
                "reservation" | "owner" => {
                    abandonment::terminal(&pg, &finalizer, true);
                    inspected =
                        abandonment::source_carrier::crossed_proof_with_equal_crc(&finalizer, case);
                    assert_eq!(inspected.checksum_crc64(), finalizer.checksum_crc64());
                    assert_ne!(inspected.command_sha256(), finalizer.command_sha256());
                }
                _ => unreachable!(),
            }
            pg.conn.execute_batch("COMMIT").unwrap();
            let (pair, source, successor) = pair(&inspected, &cleanup);
            let state = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let slot = pending_install::observe(&pg);
            let pages = pending_payload_transaction::pages(&pg);
            let writes = pg.conn.total_changes();
            let failure = pair
                .prepare_abandoned_part_pending(&pg, 0, &topology())
                .err()
                .expect(case);
            if ["absent", "pending", "applied", "started"].contains(&case) {
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
                            StoreError::MetadataCommandLogConflict { .. }
                        ))
                    ),
                    "{case}: {:?}",
                    failure.error()
                );
            }
            let pair = failure.into_reissue();
            assert_pair(&pair, &source, &successor, FRESH);
            assert!(pair.into_observed_successor().is_err());
            restored(&pg, false);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), state);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            assert_eq!(pending_install::observe(&pg), slot);
            assert_eq!(pending_payload_transaction::pages(&pg), pages);
        }
    }
}

#[test]
fn staging_context_source_reissue_preparation_rejects_other_kinds_preserving_independent_histories()
{
    let _clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        for source_observed in HISTORIES {
            for successor_observed in HISTORIES {
                let Fixture {
                    dir: _dir,
                    pg,
                    source,
                    successor,
                    pair,
                    ..
                } = setup_pair(target.clone(), append, source_observed, successor_observed);
                let state = introduction::state(&pg);
                let prefix = pg.metadata_command_replica_state().unwrap();
                let slot = pending_install::observe(&pg);
                let pages = pending_payload_transaction::pages(&pg);
                let writes = pg.conn.total_changes();
                let failure = pair
                    .prepare_abandoned_part_pending(&pg, 0, &topology())
                    .err()
                    .expect("described command is not a cleanup follow-up");
                assert!(matches!(failure.error(), ContextError::CommandKind));
                let pair = failure.into_reissue();
                assert_pair(
                    &pair,
                    &source,
                    &successor,
                    (source_observed, successor_observed),
                );
                // A rejected readiness inspection does not poison generic
                // protection inspection or reset either real SQL observation.
                let pair = pair
                    .prepare_pending(&pg, &topology())
                    .unwrap()
                    .finish()
                    .unwrap();
                assert_pair(
                    &pair,
                    &source,
                    &successor,
                    (source_observed, successor_observed),
                );
                drop(pair);
                restored(&pg, false);
                assert_eq!(pg.conn.total_changes(), writes);
                assert_eq!(introduction::state(&pg), state);
                assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                assert_eq!(pending_install::observe(&pg), slot);
                assert_eq!(pending_payload_transaction::pages(&pg), pages);
            }
        }
    }
    for count in [0, 2] {
        let SourceFixture {
            dir: _dir,
            pg,
            finalizer,
            cleanup,
        } = setup(count);
        abandonment::terminal(&pg, &finalizer, true);
        pg.conn.execute_batch("COMMIT").unwrap();
        let ordinary =
            ContextualCommand::decode_field(cleanup.clone(), &[1, 0, 0], &topology()).unwrap();
        let successor = ContextualCommand::decode_field(
            MetadataCommandEnvelope::new(next(cleanup.id()), cleanup.payload().clone()),
            &[1, 0, 0],
            &topology(),
        )
        .unwrap();
        let pair = ContextualCommand::decode_field(cleanup.clone(), &[1, 0, 0], &topology())
            .unwrap()
            .into_submission()
            .into_same_epoch_reissue(successor.id())
            .unwrap();
        let writes = pg.conn.total_changes();
        let state = introduction::state(&pg);
        let failure = pair
            .prepare_abandoned_part_pending(&pg, 0, &topology())
            .err()
            .expect("same abort without retained source");
        assert!(matches!(failure.error(), ContextError::CommandKind));
        assert_pair(&failure.into_reissue(), &ordinary, &successor, FRESH);
        restored(&pg, false);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), state);
    }
}

#[test]
fn staging_context_source_reissue_preparation_interrupts_either_authentication_and_restores_scope()
{
    for count in [0, 2] {
        for successor_read in [false, true] {
            for original_read_only in [false, true] {
                let SourceFixture {
                    dir: _dir,
                    mut pg,
                    finalizer,
                    cleanup,
                } = setup(count);
                abandonment::terminal(&pg, &finalizer, true);
                pg.conn.execute_batch("COMMIT").unwrap();
                let (pair, source, successor) = pair(&finalizer, &cleanup);
                let state = introduction::state(&pg);
                let prefix = pg.metadata_command_replica_state().unwrap();
                let writes = pg.conn.total_changes();
                pg.conn
                    .pragma_update(None, "query_only", original_read_only)
                    .unwrap();
                let source_validated = Arc::new(AtomicBool::new(false));
                let validated = Arc::clone(&source_validated);
                let fired = Arc::new(AtomicUsize::new(0));
                let captured = Arc::clone(&fired);
                let interrupt = pg.conn.get_interrupt_handle();
                pg.conn
                    .trace_v2(
                        TraceEventCodes::SQLITE_TRACE_STMT,
                        Some(move |event: TraceEvent<'_>| {
                            if let TraceEvent::Stmt(_, sql) = event {
                                // Publication-start inspection follows the source's
                                // complete chain check; do not confuse its chain-link
                                // reads with the successor's first source read.
                                if sql.contains("FROM metadata_command_pending_slot ") {
                                    validated.store(true, Ordering::SeqCst);
                                }
                                if sql.contains("FROM metadata_command_log ")
                                    && (!successor_read || validated.load(Ordering::SeqCst))
                                    && captured.fetch_add(1, Ordering::SeqCst) == 0
                                {
                                    interrupt.interrupt();
                                }
                            }
                        }),
                    )
                    .unwrap();
                let failure = pair
                    .prepare_abandoned_part_pending(&pg, 0, &topology())
                    .err()
                    .expect("selected authentication read interrupted");
                assert_eq!(fired.load(Ordering::SeqCst), 1);
                assert_eq!(source_validated.load(Ordering::SeqCst), successor_read);
                stop_trace(&mut pg);
                assert!(matches!(
                    failure.error(),
                    ContextError::Abandonment(AbandonmentError::Store(StoreError::Db {
                        context: "load historical terminal metadata command for startup validation",
                        ..
                    }))
                ));
                assert!(failure.error().to_string().contains("interrupt"));
                let pair = failure.into_reissue();
                assert_pair(&pair, &source, &successor, FRESH);
                restored(&pg, original_read_only);
                pg.conn.execute_batch("BEGIN").unwrap();
                let failure = pair
                    .prepare_abandoned_part_pending(&pg, 0, &topology())
                    .err()
                    .expect("caller transaction");
                assert!(matches!(
                    failure.error(),
                    ContextError::Snapshot(MetadataError::InvariantViolation { .. })
                ));
                assert!(!pg.conn.is_autocommit());
                pg.conn.execute_batch("ROLLBACK").unwrap();
                let prepared = failure
                    .into_reissue()
                    .prepare_abandoned_part_pending(&pg, 0, &topology())
                    .unwrap();
                pg.conn.execute_batch("ROLLBACK").unwrap();
                let failure = prepared.finish().err().expect("snapshot already ended");
                assert!(matches!(
                    failure.error(),
                    ContextError::Snapshot(MetadataError::Db { .. })
                ));
                let pair = failure.into_reissue();
                assert_pair(&pair, &source, &successor, FRESH);
                drop(
                    pair.prepare_abandoned_part_pending(&pg, 0, &topology())
                        .unwrap(),
                );
                restored(&pg, original_read_only);
                assert_eq!(pg.conn.total_changes(), writes);
                assert_eq!(introduction::state(&pg), state);
                assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                pending_install::assert_empty(&pg);
            }
        }
    }
}

#[test]
fn staging_context_source_reissue_preparation_holds_one_view_between_authentication_checks() {
    for count in [0, 2] {
        let SourceFixture {
            dir,
            mut pg,
            finalizer,
            cleanup,
        } = setup(count);
        abandonment::terminal(&pg, &finalizer, true);
        pg.conn.execute_batch("COMMIT").unwrap();
        let (pair, source, successor) = pair(&finalizer, &cleanup);
        let writer = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
        let changed = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&changed);
        let inspections = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&inspections);
        let id = finalizer.id();
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        pg.conn.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(move |event: TraceEvent<'_>| {
            if let TraceEvent::Stmt(_, sql) = event {
                if sql.contains("FROM metadata_command_pending_slot ") && observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    // The first candidate's exact-source validation completed.
                    // Fault only its evidence, using a raw connection rather
                    // than opening a second runtime over these stores.
                    *captured.lock().unwrap() = Some(writer.execute(
                        "DELETE FROM metadata_command_log WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
                        rusqlite::params![id.cluster_epoch().get(), id.pg_id().get(), id.log_index().get()],
                    ));
                }
            }
        })).unwrap();
        let prepared = pair
            .prepare_abandoned_part_pending(&pg, 0, &topology())
            .unwrap();
        assert_eq!(
            changed.lock().unwrap().as_ref().unwrap().as_ref().unwrap(),
            &1
        );
        assert_eq!(
            inspections.load(Ordering::SeqCst),
            2,
            "both exact-source validations must complete in the held view"
        );
        assert_eq!(prepared.prefix(), &prefix);
        assert_eq!(prepared.source_inventory().payload_reference_count, count);
        assert_eq!(
            prepared.successor_inventory().payload_reference_count,
            count
        );
        let pair = prepared.finish().unwrap();
        stop_trace(&mut pg);
        assert_pair(&pair, &source, &successor, FRESH);
        let state = introduction::state(&pg);
        let failure = pair
            .prepare_abandoned_part_pending(&pg, 0, &topology())
            .err()
            .expect("new view must reject removed evidence");
        assert!(matches!(
            failure.error(),
            ContextError::Abandonment(AbandonmentError::NotAbandoned)
        ));
        let pair = failure.into_reissue();
        assert_pair(&pair, &source, &successor, FRESH);
        assert!(pair.into_observed_successor().is_err());
        // Neither successful inspection authorizes subsequent application.
        for context in [&source, &successor] {
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert!(matches!(
                context
                    .application(&topology())
                    .unwrap()
                    .apply_in_open_transaction(&pg, 0),
                Err(ApplicationError::Abandonment(
                    AbandonmentError::NotAbandoned
                ))
            ));
            pg.conn.execute_batch("ROLLBACK").unwrap();
        }
        restored(&pg, false);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), state);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        pending_install::assert_empty(&pg);
    }
}
