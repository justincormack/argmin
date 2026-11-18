// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One read-only snapshot for both possible commands; real SQL seeds each
//! history independently. No authenticated carrier or physical-custody claims.

use super::*;
use crate::node_runtime::pg_store::payload_staging::candidate::CandidateBindingError;
use crate::node_runtime::pg_store::payload_staging::command_context::{
    ContextError, PendingInventoryObservation,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::{MetadataError, StoreError};
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::sync::{atomic::AtomicUsize, Arc, Mutex};

#[path = "payload_staging_context_source_reissue_preparation_tests.rs"]
mod source;

const HISTORIES: [Observed; 3] = [
    Observed::NoAcceptanceObserved,
    Observed::Unconfirmed,
    Observed::Installed,
];
const ENCRYPTION_READ: &str = "SELECT encryption_type, encryption_state FROM stream_uploads";

struct Fixture {
    dir: test_util::TempDir,
    pg: PgStore,
    owner: StagingOwnerRecord,
    source: ContextualCommand,
    successor: ContextualCommand,
    pair: ContextualReissue,
}

fn next(id: MetadataCommandId) -> MetadataCommandId {
    MetadataCommandId::new(
        id.cluster_epoch(),
        id.pg_id(),
        MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
    )
}

fn setup_pair(
    target: StagingTarget,
    append: bool,
    source_observed: Observed,
    successor_observed: Observed,
) -> Fixture {
    let (dir, pg, owner, command) = candidate::setup(target, append);
    let captured = candidate::capture(&pg, &owner, command.clone(), append);
    let bytes = captured.descriptor_bytes();
    let source = described_context(&command, &bytes, append);
    let successor_command =
        MetadataCommandEnvelope::new(next(command.id()), command.payload().clone());
    let successor = described_context(&successor_command, &bytes, append);
    pending_payload_transaction::install(&pg, &command, Some(&captured)).unwrap();
    let mut holder = captured.into_submission();
    match source_observed {
        Observed::NoAcceptanceObserved => {}
        Observed::Unconfirmed => interrupt(&mut holder, &pg),
        Observed::Installed => submission::install(&mut holder, &pg).unwrap(),
    }
    reject(&mut holder, &pg);
    let mut pair = holder
        .into_same_epoch_reissue(successor_command.id())
        .unwrap();
    match successor_observed {
        Observed::NoAcceptanceObserved => {}
        Observed::Unconfirmed => {
            pg.fail_next_pending_slot_replace_after_commit
                .store(true, Ordering::Relaxed);
            assert!(matches!(
                pair.replace(&pg, 0, &topology()),
                Err(SubmissionError::Unconfirmed(_))
            ));
            pg.fail_next_pending_slot_inspection
                .store(true, Ordering::Relaxed);
            assert!(matches!(
                pair.replace(&pg, 0, &topology()),
                Err(SubmissionError::Rejected(_))
            ));
        }
        Observed::Installed => assert!(pair.replace(&pg, 0, &topology()).unwrap()),
    }
    assert_eq!(pair.source().observation(), source_observed);
    assert_eq!(pair.observation(), successor_observed);
    Fixture {
        dir,
        pg,
        owner,
        source,
        successor,
        pair: pair.into_contextual(),
    }
}

fn read_only(pg: &PgStore) -> bool {
    pg.conn
        .pragma_query_value(None, "query_only", |row| row.get(0))
        .unwrap()
}

fn restored(pg: &PgStore, original_read_only: bool) {
    assert!(pg.conn.is_autocommit());
    assert_eq!(read_only(pg), original_read_only);
}

fn assert_pair(
    pair: &ContextualReissue,
    source: &ContextualCommand,
    successor: &ContextualCommand,
    histories: (Observed, Observed),
) {
    assert_eq!(
        pair.source(),
        ContextSubmissionObservation {
            command_id: source.id(),
            installation: histories.0
        }
    );
    assert_eq!(
        pair.successor(),
        ContextSubmissionObservation {
            command_id: successor.id(),
            installation: histories.1
        }
    );
    pair.assert_contexts_for_test(source, successor, &topology());
}

fn stop_trace(pg: &mut PgStore) {
    pg.conn
        .trace_v2(TraceEventCodes::empty(), None::<fn(TraceEvent<'_>)>)
        .unwrap();
}

#[test]
fn staging_context_reissue_preparation_preserves_both_histories_in_one_snapshot() {
    let clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        for source_observed in HISTORIES {
            for successor_observed in HISTORIES {
                clock.set(2_000);
                let Fixture {
                    dir,
                    pg,
                    owner,
                    source,
                    successor,
                    pair,
                } = setup_pair(target.clone(), append, source_observed, successor_observed);
                let state = introduction::state(&pg);
                let slot = pending_install::observe(&pg);
                let pages = pending_payload_transaction::pages(&pg);
                let prefix = pg.metadata_command_replica_state().unwrap();
                drop(pg);
                let mut pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
                let writes = pg.conn.total_changes();
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
                assert_eq!(
                    prepared.source(),
                    ContextSubmissionObservation {
                        command_id: source.id(),
                        installation: source_observed
                    }
                );
                assert_eq!(
                    prepared.successor(),
                    ContextSubmissionObservation {
                        command_id: successor.id(),
                        installation: successor_observed
                    }
                );
                assert_eq!(prepared.prefix(), &prefix);
                assert!(!pg.conn.is_autocommit());
                assert!(read_only(&pg));
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
                assert_eq!(prepared.successor_inventory(), expected);
                let pair = prepared.finish().unwrap();
                restored(&pg, false);
                assert_eq!(*trace.lock().unwrap(), ["BEGIN DEFERRED", "ROLLBACK"]);
                stop_trace(&mut pg);
                assert_pair(
                    &pair,
                    &source,
                    &successor,
                    (source_observed, successor_observed),
                );
                match pair.into_observed_successor() {
                    Ok(holder) => {
                        assert_eq!(successor_observed, Observed::Installed);
                        assert_eq!(holder.observation().installation, successor_observed);
                        holder.assert_context_for_test(&successor, &topology());
                    }
                    Err(pair) => {
                        assert_ne!(successor_observed, Observed::Installed);
                        assert_pair(
                            &pair,
                            &source,
                            &successor,
                            (source_observed, successor_observed),
                        );
                    }
                }
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
fn staging_context_reissue_preparation_interrupts_either_inventory_without_losing_the_pair() {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for histories in [
            (Observed::Unconfirmed, Observed::Installed),
            (Observed::Installed, Observed::Unconfirmed),
        ] {
            for fail_read in [1, 2] {
                let Fixture {
                    dir: _dir,
                    mut pg,
                    source,
                    successor,
                    pair,
                    ..
                } = setup_pair(target.clone(), true, histories.0, histories.1);
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
                    .expect("selected inventory read must fail");
                assert_eq!(reads.load(Ordering::SeqCst), fail_read);
                stop_trace(&mut pg);
                assert!(
                    matches!(
                        failure.error(),
                        ContextError::Retention(StoreError::Db {
                            context: "load exact stream encryption for pending payload inventory",
                            ..
                        })
                    ),
                    "{:?}",
                    failure.error()
                );
                assert!(failure.error().to_string().contains("interrupt"));
                restored(&pg, false);
                let pair = failure.into_reissue();
                assert_pair(&pair, &source, &successor, histories);
                let pair = pair
                    .prepare_pending(&pg, &topology())
                    .unwrap()
                    .finish()
                    .unwrap();
                assert_pair(&pair, &source, &successor, histories);
                drop(pair);
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
fn staging_context_reissue_preparation_keeps_one_view_between_inventory_reads() {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let histories = (Observed::Installed, Observed::Unconfirmed);
        let Fixture {
            dir,
            mut pg,
            owner,
            source,
            successor,
            pair,
        } = setup_pair(target, true, histories.0, histories.1);
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
                    // Change durable geometry after the source SELECT finished,
                    // immediately before the successor SELECT. One WAL snapshot
                    // must prevent mixing these two committed session views.
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
            .expect("new inspection must reload crossed session");
        assert!(
            matches!(failure.error(), ContextError::Retention(StoreError::ShardScavengerScanIncomplete { context: "derive pending command payload inventory", errors }) if errors.contains("missing exact stream encryption"))
        );
        let pair = failure.into_reissue();
        assert_pair(&pair, &source, &successor, histories);
        restored(&pg, false);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), state);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(pending_install::observe(&pg), slot);
        assert_eq!(pending_payload_transaction::pages(&pg), pages);
        let writer = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
        crate::node_runtime::pg_store::command_log::register_metadata_digest_sql_functions(&writer)
            .unwrap();
        writer
            .execute(
                "UPDATE stream_uploads SET key = ?1 WHERE session_id = ?2",
                rusqlite::params![original.key.as_str(), owner.subject.id.attempt.as_str()],
            )
            .unwrap();
        let state = introduction::state(&pg);
        let pair = pair
            .prepare_pending(&pg, &topology())
            .unwrap()
            .finish()
            .unwrap();
        assert_pair(&pair, &source, &successor, histories);
        drop(pair);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), state);
    }
}

#[test]
fn staging_context_reissue_preparation_rejections_and_finish_failure_keep_both_histories() {
    let _clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        let histories = (Observed::Unconfirmed, Observed::Installed);
        let Fixture {
            dir: _dir,
            pg,
            owner,
            source,
            successor,
            pair,
            ..
        } = setup_pair(target, append, histories.0, histories.1);
        let state = introduction::state(&pg);
        let slot = pending_install::observe(&pg);
        let pages = pending_payload_transaction::pages(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let failure = pair
            .prepare_pending(&pg, &topology())
            .err()
            .expect("caller transaction");
        assert!(matches!(
            failure.error(),
            ContextError::Snapshot(MetadataError::InvariantViolation {
                context: "primary metadata command build snapshot",
                ..
            })
        ));
        assert!(!pg.conn.is_autocommit());
        assert!(!read_only(&pg));
        let pair = failure.into_reissue();
        assert_pair(&pair, &source, &successor, histories);
        pg.conn.execute_batch("ROLLBACK").unwrap();
        let other_dir = test_util::tempdir();
        let other =
            PgStore::open(other_dir.path(), (owner.subject.object_pg.get() + 1) % 8).unwrap();
        let other_writes = other.conn.total_changes();
        let failure = pair
            .prepare_pending(&other, &topology())
            .err()
            .expect("wrong PG");
        assert!(matches!(failure.error(), ContextError::CommandPg));
        let pair = failure.into_reissue();
        assert_pair(&pair, &source, &successor, histories);
        restored(&other, false);
        assert_eq!(other.conn.total_changes(), other_writes);
        let foreign_topology = PgTopology::new(&[owner.subject.object_pg.get()]).unwrap();
        let failure = pair
            .prepare_pending(&pg, &foreign_topology)
            .err()
            .expect("crossed topology");
        assert!(matches!(
            failure.error(),
            ContextError::Candidate(CandidateBindingError::Topology)
        ));
        let pair = failure.into_reissue();
        assert_pair(&pair, &source, &successor, histories);
        restored(&pg, false);
        let prepared = pair.prepare_pending(&pg, &topology()).unwrap();
        // Owner-local read-snapshot failure, not an I/O durability claim.
        pg.conn.execute_batch("ROLLBACK").unwrap();
        let failure = prepared.finish().err().expect("snapshot already ended");
        assert!(matches!(
            failure.error(),
            ContextError::Snapshot(MetadataError::Db {
                context: "primary metadata command build snapshot",
                ..
            })
        ));
        assert!(failure
            .error()
            .to_string()
            .contains("no transaction is active"));
        let pair = failure.into_reissue();
        assert_pair(&pair, &source, &successor, histories);
        restored(&pg, false);
        let pair = pair
            .prepare_pending(&pg, &topology())
            .unwrap()
            .finish()
            .unwrap();
        assert_pair(&pair, &source, &successor, histories);
        drop(pair);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), state);
        assert_eq!(pending_install::observe(&pg), slot);
        assert_eq!(pending_payload_transaction::pages(&pg), pages);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    }
}

#[test]
fn staging_context_reissue_preparation_preserves_ordinary_and_abandonment_pairs() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let cleanup = abandonment::cleanup(&pg, &owner, &source);
        let abandoned = ContextualCommand::abandoned_part(
            PartFinalizationSource::capture_for_test(&source)
                .unwrap()
                .bind_cleanup(cleanup.clone(), &topology())
                .unwrap(),
            &topology(),
        )
        .unwrap();
        let ordinary =
            ContextualCommand::decode_field(cleanup.clone(), &[1, 0, 0], &topology()).unwrap();
        let reserve = MetadataCommandEnvelope::new(
            owner.subject.id.introduction,
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                owner.subject.bucket.clone(),
                owner.subject.key.clone(),
                crate::VersionId::from_u64(17),
            )),
        );
        let nonterminal =
            ContextualCommand::decode_field(reserve.clone(), &[1, 0, 0], &topology()).unwrap();
        abandonment::terminal(&pg, &source, true);
        pg.conn.execute_batch("COMMIT").unwrap();
        pg.record_current_metadata_command_checkpoint(0, source.id().cluster_epoch())
            .unwrap();
        pg.compact_metadata_command_log(source.id().cluster_epoch())
            .unwrap();
        assert_eq!(
            pg.metadata_command_log_stats(source.id().cluster_epoch())
                .unwrap()
                .retained_entries,
            0
        );
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let state = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        for (command, context, payloads, routes) in [
            (&cleanup, abandoned, count, 2),
            (&cleanup, ordinary, count, 0),
            (&reserve, nonterminal, 0, 0),
        ] {
            let field = context.encode_field().unwrap();
            let source =
                ContextualCommand::decode_field(command.clone(), &field, &topology()).unwrap();
            let successor = ContextualCommand::decode_field(
                MetadataCommandEnvelope::new(next(command.id()), command.payload().clone()),
                &field,
                &topology(),
            )
            .unwrap();
            let pair = context
                .into_submission()
                .into_same_epoch_reissue(successor.id())
                .unwrap();
            let prepared = pair.prepare_pending(&pg, &topology()).unwrap();
            let expected = PendingInventoryObservation {
                payload_reference_count: payloads,
                route_dependency_count: routes,
                has_future_window: false,
            };
            assert_eq!(prepared.source_inventory(), expected);
            assert_eq!(prepared.successor_inventory(), expected);
            let pair = prepared.finish().unwrap();
            assert_pair(
                &pair,
                &source,
                &successor,
                (
                    Observed::NoAcceptanceObserved,
                    Observed::NoAcceptanceObserved,
                ),
            );
            assert!(pair.into_observed_successor().is_err());
        }
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), state);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        pending_install::assert_empty(&pg);
    }
}

#[test]
fn staging_context_reissue_preparation_drop_and_unwind_preserve_pending_state() {
    let _clock = test_time_override_guard(2_000);
    for histories in [
        (Observed::Installed, Observed::Unconfirmed),
        (Observed::Unconfirmed, Observed::Installed),
    ] {
        for original_read_only in [false, true] {
            for exit in ["drop", "unwind", "failed finish unwind"] {
                let Fixture {
                    dir: _dir,
                    pg,
                    source,
                    successor,
                    pair,
                    ..
                } = setup_pair(StagingTarget::StreamPut, true, histories.0, histories.1);
                let state = introduction::state(&pg);
                let slot = pending_install::observe(&pg);
                let pages = pending_payload_transaction::pages(&pg);
                let prefix = pg.metadata_command_replica_state().unwrap();
                let writes = pg.conn.total_changes();
                pg.conn
                    .pragma_update(None, "query_only", original_read_only)
                    .unwrap();
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let prepared = pair.prepare_pending(&pg, &topology()).unwrap();
                    assert_eq!(
                        prepared.source(),
                        ContextSubmissionObservation {
                            command_id: source.id(),
                            installation: histories.0
                        }
                    );
                    assert_eq!(
                        prepared.successor(),
                        ContextSubmissionObservation {
                            command_id: successor.id(),
                            installation: histories.1
                        }
                    );
                    assert_eq!(prepared.source_inventory().payload_reference_count, 1);
                    assert_eq!(prepared.successor_inventory().payload_reference_count, 1);
                    match exit {
                        "drop" => drop(prepared),
                        "unwind" => panic!("unwind reissue inspection"),
                        "failed finish unwind" => {
                            pg.conn.execute_batch("ROLLBACK").unwrap();
                            let _failure = prepared.finish().err().expect("snapshot already ended");
                            panic!("unwind reissue inspection failure");
                        }
                        _ => unreachable!(),
                    }
                }));
                assert_eq!(outcome.is_err(), exit != "drop");
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
