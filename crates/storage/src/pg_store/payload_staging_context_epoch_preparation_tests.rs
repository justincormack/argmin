// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Both epoch candidates and their history remain owned by one read-only scope.

use super::*;
use crate::node_runtime::pg_store::payload_staging::abandonment::AbandonmentError;
use crate::node_runtime::pg_store::payload_staging::candidate::CandidateBindingError;
use crate::node_runtime::pg_store::payload_staging::command_context::{
    ApplicationError, PendingInventoryObservation,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::{MetadataError, StoreError};
use std::sync::Mutex;

const ENCRYPTION_READ: &str = "SELECT encryption_type, encryption_state FROM stream_uploads";

struct Fixture {
    dir: test_util::TempDir,
    pg: PgStore,
    owner: StagingOwnerRecord,
    source: ContextualCommand,
    successor: ContextualCommand,
    pair: ContextualEpochReissue,
}

fn setup(target: StagingTarget, append: bool, observed: Observed) -> Fixture {
    let (dir, pg, owner, command) = candidate::setup(target, append);
    let captured = candidate::capture(&pg, &owner, command.clone(), append);
    let source = described_context(&command, &captured.descriptor_bytes(), append);
    let successor = expected_successor(&command, &source, new_epoch(command.id(), 1, 1));
    pending_payload_transaction::install(&pg, &command, Some(&captured)).unwrap();
    let mut holder = captured.into_submission();
    seed_history(&mut holder, &pg, observed);
    let pair = holder
        .into_contextual()
        .into_epoch_reissue(successor.id())
        .unwrap();
    Fixture {
        dir,
        pg,
        owner,
        source,
        successor,
        pair,
    }
}

fn restored(pg: &PgStore, original_read_only: bool) {
    assert!(pg.conn.is_autocommit());
    assert_eq!(
        pg.conn
            .pragma_query_value(None, "query_only", |row| row.get::<_, bool>(0))
            .unwrap(),
        original_read_only
    );
}

fn stop_trace(pg: &mut PgStore) {
    pg.conn
        .trace_v2(TraceEventCodes::empty(), None::<fn(TraceEvent<'_>)>)
        .unwrap();
}

#[test]
fn staging_context_epoch_preparation_preserves_histories_and_both_inventories_in_one_view() {
    let clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        for observed in [
            Observed::NoAcceptanceObserved,
            Observed::Unconfirmed,
            Observed::Installed,
        ] {
            for original_read_only in [false, true] {
                clock.set(2_000);
                let Fixture {
                    dir,
                    pg,
                    owner,
                    source,
                    successor,
                    pair,
                } = setup(target.clone(), append, observed);
                let state = introduction::state(&pg);
                let slot = pending_install::observe(&pg);
                let pages = pending_payload_transaction::pages(&pg);
                let prefix = pg.metadata_command_replica_state().unwrap();
                drop(pg);
                let mut pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
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
                clock.set(9_000_000);
                let prepared = pair.prepare_pending(&pg, &topology()).unwrap();
                assert_eq!(prepared.source().installation, observed);
                assert_eq!(prepared.source().command_id, source.id());
                assert_eq!(
                    prepared.successor().installation,
                    Observed::NoAcceptanceObserved
                );
                assert_eq!(prepared.successor().command_id, successor.id());
                assert_eq!(prepared.prefix(), &prefix);
                // Inventory does not assert target-epoch authorization: this
                // fixture deliberately still has an old-epoch pending command.
                let mut pgs = vec![
                    owner.subject.object_pg.get(),
                    owner.subject.data_pg(&topology(), u32::from(append)),
                ];
                if !append {
                    pgs.push(topology().bucket_pg_for(&owner.subject.bucket));
                }
                pgs.sort_unstable();
                pgs.dedup();
                let expected = PendingInventoryObservation {
                    payload_reference_count: usize::from(append),
                    route_dependency_count: pgs.len()
                        + usize::from(!append && target != StagingTarget::DirectPut),
                    has_future_window: true,
                };
                assert_eq!(prepared.source_inventory(), expected);
                assert_eq!(
                    prepared.successor_inventory(),
                    PendingInventoryObservation {
                        route_dependency_count: expected.route_dependency_count + 1,
                        ..expected
                    }
                );
                let pair = prepared.finish().unwrap();
                assert_pair(&pair, &source, &successor, observed);
                assert_eq!(*trace.lock().unwrap(), ["BEGIN DEFERRED", "ROLLBACK"]);
                stop_trace(&mut pg);
                drop(pair);
                restored(&pg, original_read_only);
                assert_eq!(pg.conn.total_changes(), writes);
                assert_eq!(introduction::state(&pg), state);
                assert_eq!(pending_install::observe(&pg), slot);
                assert_eq!(pending_payload_transaction::pages(&pg), pages);
                assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            }
        }
    }
}

#[test]
fn staging_context_epoch_preparation_interrupts_either_inventory_without_resetting_history() {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for observed in [Observed::Unconfirmed, Observed::Installed] {
            for fail_read in [1, 2] {
                let Fixture {
                    dir: _dir,
                    mut pg,
                    source,
                    successor,
                    pair,
                    ..
                } = setup(target.clone(), true, observed);
                let state = introduction::state(&pg);
                let slot = pending_install::observe(&pg);
                let pages = pending_payload_transaction::pages(&pg);
                let prefix = pg.metadata_command_replica_state().unwrap();
                let writes = pg.conn.total_changes();
                let reads = Arc::new(AtomicUsize::new(0));
                let captured = Arc::clone(&reads);
                let interrupt = pg.conn.get_interrupt_handle();
                pg.conn
                    .trace_v2(
                        TraceEventCodes::SQLITE_TRACE_STMT,
                        Some(move |event: TraceEvent<'_>| {
                            if let TraceEvent::Stmt(_, sql) = event {
                                if sql.starts_with(ENCRYPTION_READ)
                                    && captured.fetch_add(1, Ordering::SeqCst) + 1 == fail_read
                                {
                                    interrupt.interrupt();
                                }
                            }
                        }),
                    )
                    .unwrap();
                let failure = pair
                    .prepare_pending(&pg, &topology())
                    .err()
                    .expect("selected inventory interrupted");
                assert_eq!(reads.load(Ordering::SeqCst), fail_read);
                stop_trace(&mut pg);
                assert!(matches!(
                    failure.error(),
                    ContextError::Retention(StoreError::Db {
                        context: "load exact stream encryption for pending payload inventory",
                        ..
                    })
                ));
                assert!(failure.error().to_string().contains("interrupt"));
                let pair = failure.into_reissue();
                assert_pair(&pair, &source, &successor, observed);
                let pair = pair
                    .prepare_pending(&pg, &topology())
                    .unwrap()
                    .finish()
                    .unwrap();
                assert_pair(&pair, &source, &successor, observed);
                drop(pair);
                restored(&pg, false);
                assert_eq!(pg.conn.total_changes(), writes);
                assert_eq!(introduction::state(&pg), state);
                assert_eq!(pending_install::observe(&pg), slot);
                assert_eq!(pending_payload_transaction::pages(&pg), pages);
                assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            }
        }
    }
}

#[test]
fn staging_context_epoch_preparation_does_not_mix_session_views_between_candidates() {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let Fixture {
            dir,
            mut pg,
            owner,
            source,
            successor,
            pair,
        } = setup(target, true, Observed::Unconfirmed);
        let slot = pending_install::observe(&pg);
        let pages = pending_payload_transaction::pages(&pg);
        let original = pg.get_stream_upload(&owner.subject.id.attempt).unwrap();
        let writer = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
        crate::node_runtime::pg_store::command_log::register_metadata_digest_sql_functions(&writer)
            .unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let captured = Arc::clone(&reads);
        let change = Arc::new(Mutex::new(None));
        let changed = Arc::clone(&change);
        let session = owner.subject.id.attempt.clone();
        pg.conn.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(move |event: TraceEvent<'_>| {
            if let TraceEvent::Stmt(_, sql) = event {
                if sql.starts_with(ENCRYPTION_READ) && captured.fetch_add(1, Ordering::SeqCst) == 1 {
                    *changed.lock().unwrap() = Some(writer.execute("UPDATE stream_uploads SET key = 'crossed-key' WHERE session_id = ?1", [session.as_str()]));
                }
            }
        })).unwrap();
        let prepared = pair.prepare_pending(&pg, &topology()).unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        assert_eq!(
            change.lock().unwrap().as_ref().unwrap().as_ref().unwrap(),
            &1
        );
        assert_eq!(
            pg.get_stream_upload(&owner.subject.id.attempt).unwrap(),
            original
        );
        assert_eq!(prepared.source_inventory().payload_reference_count, 1);
        assert_eq!(prepared.successor_inventory().payload_reference_count, 1);
        let pair = prepared.finish().unwrap();
        stop_trace(&mut pg);
        let state = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        let failure = pair
            .prepare_pending(&pg, &topology())
            .err()
            .expect("new snapshot must reject crossed session");
        assert!(
            matches!(failure.error(), ContextError::Retention(StoreError::ShardScavengerScanIncomplete { context: "derive pending command payload inventory", errors }) if errors.contains("missing exact stream encryption"))
        );
        assert_pair(
            &failure.into_reissue(),
            &source,
            &successor,
            Observed::Unconfirmed,
        );
        restored(&pg, false);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), state);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(pending_install::observe(&pg), slot);
        assert_eq!(pending_payload_transaction::pages(&pg), pages);
    }
}

#[test]
fn staging_context_epoch_preparation_rejections_finish_errors_and_unwind_keep_custody() {
    let _clock = test_time_override_guard(2_000);
    for observed in [Observed::Unconfirmed, Observed::Installed] {
        for original_read_only in [false, true] {
            for unwind in [false, true] {
                let Fixture {
                    dir: _dir,
                    pg,
                    owner,
                    source,
                    successor,
                    pair,
                } = setup(multipart_target(), true, observed);
                let state = introduction::state(&pg);
                let slot = pending_install::observe(&pg);
                let pages = pending_payload_transaction::pages(&pg);
                let prefix = pg.metadata_command_replica_state().unwrap();
                let writes = pg.conn.total_changes();
                pg.conn
                    .pragma_update(None, "query_only", original_read_only)
                    .unwrap();
                let other_dir = test_util::tempdir();
                let other =
                    PgStore::open(other_dir.path(), (owner.subject.object_pg.get() + 1) % 8)
                        .unwrap();
                let other_writes = other.conn.total_changes();
                let failure = pair
                    .prepare_pending(&other, &topology())
                    .err()
                    .expect("crossed PG");
                assert!(matches!(failure.error(), ContextError::CommandPg));
                assert_eq!(other.conn.total_changes(), other_writes);
                restored(&other, false);
                let pair = failure.into_reissue();
                let foreign = PgTopology::new(&[owner.subject.object_pg.get()]).unwrap();
                let failure = pair
                    .prepare_pending(&pg, &foreign)
                    .err()
                    .expect("crossed topology");
                assert!(matches!(
                    failure.error(),
                    ContextError::Candidate(CandidateBindingError::Topology)
                ));
                let pair = failure.into_reissue();
                assert_pair(&pair, &source, &successor, observed);
                pg.conn.execute_batch("BEGIN").unwrap();
                let failure = pair
                    .prepare_pending(&pg, &topology())
                    .err()
                    .expect("caller transaction");
                assert!(matches!(
                    failure.error(),
                    ContextError::Snapshot(MetadataError::InvariantViolation { .. })
                ));
                assert!(!pg.conn.is_autocommit());
                pg.conn.execute_batch("ROLLBACK").unwrap();
                let pair = failure.into_reissue();
                // Described commands cannot masquerade as abandoned follow-ups.
                let failure = pair
                    .prepare_abandoned_part_pending(&pg, 0, &topology())
                    .err()
                    .expect("wrong kind");
                assert!(matches!(failure.error(), ContextError::CommandKind));
                let prepared = failure
                    .into_reissue()
                    .prepare_pending(&pg, &topology())
                    .unwrap();
                pg.conn.execute_batch("ROLLBACK").unwrap();
                let failure = prepared.finish().err().expect("snapshot already ended");
                assert!(matches!(
                    failure.error(),
                    ContextError::Snapshot(MetadataError::Db { .. })
                ));
                let pair = failure.into_reissue();
                assert_pair(&pair, &source, &successor, observed);
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _prepared = pair.prepare_pending(&pg, &topology()).unwrap();
                    assert!(!unwind, "unwind epoch inspection");
                }));
                assert_eq!(result.is_err(), unwind);
                restored(&pg, original_read_only);
                assert_eq!(pg.conn.total_changes(), writes);
                assert_eq!(introduction::state(&pg), state);
                assert_eq!(pending_install::observe(&pg), slot);
                assert_eq!(pending_payload_transaction::pages(&pg), pages);
                assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            }
        }
    }
}

#[test]
fn staging_context_epoch_preparation_authenticates_original_finalizer_after_compaction_and_reopen()
{
    for count in [0, 2] {
        for compact in [false, true] {
            let dir = test_util::tempdir();
            let pg_id = fixture(multipart_target()).subject.object_pg.get();
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            let owner = part::initialize(&pg, count);
            let finalizer = part::command(&pg, &owner);
            let cleanup = abandonment::cleanup(&pg, &owner, &finalizer);
            abandonment::terminal(&pg, &finalizer, true);
            pg.conn.execute_batch("COMMIT").unwrap();
            if compact {
                pg.record_current_metadata_command_checkpoint(0, finalizer.id().cluster_epoch())
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
            let id = new_epoch(cleanup.id(), 1, 1);
            let digest = pg.metadata_command_replica_state().unwrap().state_digest;
            pg.initialize_metadata_transfer_matching_state(
                0,
                id.cluster_epoch(),
                0,
                MetadataCommandLogHash::genesis(),
                digest,
            )
            .unwrap();
            let source = abandoned_context(&finalizer, &cleanup);
            let successor = expected_successor(&cleanup, &source, id);
            let pair = abandoned_context(&finalizer, &cleanup)
                .into_submission()
                .into_epoch_reissue(id)
                .unwrap();
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            let state = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let writes = pg.conn.total_changes();
            let prepared =
                crate::clock::with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                    pair.prepare_abandoned_part_pending(&pg, 0, &topology())
                })
                .unwrap();
            assert_eq!(prepared.prefix(), &prefix);
            let expected = PendingInventoryObservation {
                payload_reference_count: count,
                route_dependency_count: 2,
                has_future_window: false,
            };
            assert_eq!(prepared.source_inventory(), expected);
            assert_eq!(
                prepared.successor_inventory(),
                PendingInventoryObservation {
                    route_dependency_count: 3,
                    ..expected
                }
            );
            let pair = prepared.finish().unwrap();
            assert_pair(&pair, &source, &successor, Observed::NoAcceptanceObserved);
            drop(pair);
            restored(&pg, false);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), state);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            pending_install::assert_empty(&pg);
        }
    }
}

#[test]
fn staging_context_epoch_preparation_rechecks_source_evidence_after_snapshot_ends() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg.get();
        let mut pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = part::initialize(&pg, count);
        let finalizer = part::command(&pg, &owner);
        let cleanup = abandonment::cleanup(&pg, &owner, &finalizer);
        abandonment::terminal(&pg, &finalizer, true);
        pg.conn.execute_batch("COMMIT").unwrap();
        let source = abandoned_context(&finalizer, &cleanup);
        let successor = expected_successor(&cleanup, &source, new_epoch(cleanup.id(), 1, 1));
        let pair = abandoned_context(&finalizer, &cleanup)
            .into_submission()
            .into_epoch_reissue(successor.id())
            .unwrap();
        let writer = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
        let changed = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&changed);
        let inspections = Arc::new(AtomicUsize::new(0));
        let checked = Arc::clone(&inspections);
        let id = finalizer.id();
        pg.conn.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(move |event: TraceEvent<'_>| {
            if let TraceEvent::Stmt(_, sql) = event {
                if sql.contains("FROM metadata_command_pending_slot ") && checked.fetch_add(1, Ordering::SeqCst) == 0 {
                    *captured.lock().unwrap() = Some(writer.execute("DELETE FROM metadata_command_log WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3", rusqlite::params![id.cluster_epoch().get(), id.pg_id().get(), id.log_index().get()]));
                }
            }
        })).unwrap();
        let prepared = pair
            .prepare_abandoned_part_pending(&pg, 0, &topology())
            .unwrap();
        assert_eq!(inspections.load(Ordering::SeqCst), 2);
        assert_eq!(
            changed.lock().unwrap().as_ref().unwrap().as_ref().unwrap(),
            &1
        );
        assert_eq!(prepared.source_inventory().payload_reference_count, count);
        assert_eq!(
            prepared.successor_inventory().payload_reference_count,
            count
        );
        let pair = prepared.finish().unwrap();
        stop_trace(&mut pg);
        let state = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        let failure = pair
            .prepare_abandoned_part_pending(&pg, 0, &topology())
            .err()
            .expect("fresh snapshot must reject removed source");
        assert!(matches!(
            failure.error(),
            ContextError::Abandonment(AbandonmentError::NotAbandoned)
        ));
        let pair = failure.into_reissue();
        assert_pair(&pair, &source, &successor, Observed::NoAcceptanceObserved);
        // Protection inventory still describes the retained contexts; failed
        // readiness is not evidence that their payload can be deleted.
        let pair = pair
            .prepare_pending(&pg, &topology())
            .unwrap()
            .finish()
            .unwrap();
        assert_pair(&pair, &source, &successor, Observed::NoAcceptanceObserved);
        drop(pair);
        for candidate in [&source, &successor] {
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert!(matches!(
                candidate
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
