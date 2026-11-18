// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only inspections of owning submissions, with histories seeded through
//! real pending SQL outcomes. No carrier, custody or physical-effects activation.

use super::*;
use crate::node_runtime::pg_store::payload_staging::candidate::CandidateBindingError;
use crate::node_runtime::pg_store::payload_staging::command_context::{
    ContextError, PendingInventoryObservation,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::{MetadataError, StoreError};
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::sync::{Arc, Mutex};

const HISTORIES: [Observed; 3] = [
    Observed::NoAcceptanceObserved,
    Observed::Unconfirmed,
    Observed::Installed,
];

fn setup(
    target: StagingTarget,
    append: bool,
    observed: Observed,
) -> (
    test_util::TempDir,
    PgStore,
    StagingOwnerRecord,
    ContextualCommand,
    ContextualSubmission,
) {
    let (dir, pg, owner, command) = candidate::setup(target, append);
    let captured = candidate::capture(&pg, &owner, command.clone(), append);
    let expected = described_context(&command, &captured.descriptor_bytes(), append);
    // Even NoAcceptanceObserved has an actual slot: the history is deliberately
    // not a global-absence assertion. A successful inspection cannot reset it.
    pending_payload_transaction::install(&pg, &command, Some(&captured)).unwrap();
    let mut holder = captured.into_submission();
    match observed {
        Observed::NoAcceptanceObserved => {}
        Observed::Unconfirmed => interrupt(&mut holder, &pg),
        Observed::Installed => submission::install(&mut holder, &pg).unwrap(),
    }
    reject(&mut holder, &pg);
    assert_eq!(holder.observation(), observed);
    (dir, pg, owner, expected, holder.into_contextual())
}

fn read_only(pg: &PgStore) -> bool {
    pg.conn
        .pragma_query_value(None, "query_only", |row| row.get(0))
        .unwrap()
}

fn assert_restored(pg: &PgStore, original_read_only: bool) {
    assert!(pg.conn.is_autocommit());
    assert_eq!(read_only(pg), original_read_only);
}

fn assert_holder(holder: &ContextualSubmission, expected: &ContextualCommand, observed: Observed) {
    assert_eq!(
        holder.observation(),
        ContextSubmissionObservation {
            command_id: expected.id(),
            installation: observed,
        }
    );
    holder.assert_context_for_test(expected, &topology());
}

#[test]
fn staging_context_submission_preparation_keeps_history_and_one_read_only_view() {
    let clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        for observed in HISTORIES {
            clock.set(2_000);
            let (dir, mut pg, owner, expected, holder) = setup(target.clone(), append, observed);
            pg.conn.execute_batch("CREATE TABLE owned_preparation_canary(value INTEGER NOT NULL); INSERT INTO owned_preparation_canary VALUES(1)").unwrap();
            let mut exact_pgs = vec![
                owner.subject.object_pg.get(),
                owner.subject.data_pg(&topology(), u32::from(append)),
            ];
            if !append {
                exact_pgs.push(topology().bucket_pg_for(&owner.subject.bucket));
            }
            exact_pgs.sort_unstable();
            exact_pgs.dedup();
            let routes =
                exact_pgs.len() + usize::from(!append && target != StagingTarget::DirectPut);
            let before = introduction::state(&pg);
            let slot = pending_install::observe(&pg);
            let pages = pending_payload_transaction::pages(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
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
            let prepared = holder.prepare_pending(&pg, &topology()).unwrap();
            assert_eq!(
                prepared.observation(),
                ContextSubmissionObservation {
                    command_id: expected.id(),
                    installation: observed
                }
            );
            assert_eq!(prepared.prefix(), &prefix);
            assert!(!pg.conn.is_autocommit());
            assert!(read_only(&pg));
            let error = pg
                .conn
                .execute("UPDATE owned_preparation_canary SET value = 3", [])
                .unwrap_err();
            assert_eq!(
                error.sqlite_error_code(),
                Some(rusqlite::ErrorCode::ReadOnly)
            );
            // Raw fault/canary connection, not a second PG runtime. Even an
            // empty introduction must already have pinned one WAL snapshot.
            let writer = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
            writer
                .execute("UPDATE owned_preparation_canary SET value = 2", [])
                .unwrap();
            assert_eq!(
                pg.conn
                    .query_row("SELECT value FROM owned_preparation_canary", [], |row| row
                        .get::<_, i64>(
                        0
                    ))
                    .unwrap(),
                1
            );
            let observation = prepared.inventory();
            let PendingInventoryObservation {
                payload_reference_count,
                route_dependency_count,
                has_future_window,
            } = observation;
            assert_eq!(
                (
                    payload_reference_count,
                    route_dependency_count,
                    has_future_window
                ),
                (usize::from(append), routes, true)
            );
            let holder = prepared.finish().unwrap();
            assert_holder(&holder, &expected, observed);
            assert_restored(&pg, false);
            assert_eq!(
                pg.conn
                    .query_row("SELECT value FROM owned_preparation_canary", [], |row| row
                        .get::<_, i64>(
                        0
                    ))
                    .unwrap(),
                2
            );
            assert_eq!(*trace.lock().unwrap(), ["BEGIN DEFERRED", "ROLLBACK"]);
            let id = MetadataCommandId::new(
                expected.id().cluster_epoch(),
                expected.id().pg_id(),
                MetadataCommandLogIndex::new(expected.id().log_index().get() + 1).unwrap(),
            );
            let pair = holder.into_same_epoch_reissue(id).unwrap();
            assert_eq!(pair.source().installation, observed);
            assert_eq!(pair.successor().command_id, id);
            assert_eq!(
                pair.successor().installation,
                Observed::NoAcceptanceObserved
            );
            let pair = match pair.into_observed_successor() {
                Ok(_) => panic!("inspection certified an unsent successor"),
                Err(pair) => pair,
            };
            assert_eq!(pair.source().installation, observed);
            drop(pair);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pending_install::observe(&pg), slot);
            assert_eq!(pending_payload_transaction::pages(&pg), pages);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        }
    }
}

#[test]
fn staging_context_submission_preparation_rejection_restores_the_owning_holder() {
    let _clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        for observed in HISTORIES {
            let (_dir, pg, owner, expected, holder) = setup(target.clone(), append, observed);
            let before = introduction::state(&pg);
            let slot = pending_install::observe(&pg);
            let pages = pending_payload_transaction::pages(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let writes = pg.conn.total_changes();
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            let failure = holder
                .prepare_pending(&pg, &topology())
                .err()
                .expect("must not adopt the caller transaction");
            assert!(matches!(
                failure.error(),
                ContextError::Snapshot(MetadataError::InvariantViolation {
                    context: "primary metadata command build snapshot",
                    ..
                })
            ));
            let holder = failure.into_submission();
            assert_holder(&holder, &expected, observed);
            assert!(!pg.conn.is_autocommit());
            assert!(!read_only(&pg));
            pg.conn.execute_batch("ROLLBACK").unwrap();
            let foreign_dir = test_util::tempdir();
            let foreign =
                PgStore::open(foreign_dir.path(), (owner.subject.object_pg.get() + 1) % 8).unwrap();
            let foreign_state = introduction::state(&foreign);
            let foreign_writes = foreign.conn.total_changes();
            let failure = holder
                .prepare_pending(&foreign, &topology())
                .err()
                .expect("wrong PG");
            assert!(matches!(failure.error(), ContextError::CommandPg));
            let holder = failure.into_submission();
            assert_holder(&holder, &expected, observed);
            assert_restored(&foreign, false);
            assert_eq!(foreign.conn.total_changes(), foreign_writes);
            assert_eq!(introduction::state(&foreign), foreign_state);
            let foreign_topology = PgTopology::new(&[owner.subject.object_pg.get()]).unwrap();
            let failure = holder
                .prepare_pending(&pg, &foreign_topology)
                .err()
                .expect("same PG, different topology");
            assert!(matches!(
                failure.error(),
                ContextError::Candidate(CandidateBindingError::Topology)
            ));
            let holder = failure.into_submission();
            assert_holder(&holder, &expected, observed);
            assert_restored(&pg, false);
            let holder = holder
                .prepare_pending(&pg, &topology())
                .unwrap()
                .finish()
                .unwrap();
            assert_holder(&holder, &expected, observed);
            drop(holder);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pending_install::observe(&pg), slot);
            assert_eq!(pending_payload_transaction::pages(&pg), pages);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        }
    }
}

#[test]
fn staging_context_submission_preparation_reloads_without_resetting_uncertainty() {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for observed in HISTORIES {
            let (dir, pg, owner, expected, holder) = setup(target.clone(), true, observed);
            let slot = pending_install::observe(&pg);
            let pages = pending_payload_transaction::pages(&pg);
            let prepared = holder.prepare_pending(&pg, &topology()).unwrap();
            let original = pg.get_stream_upload(&owner.subject.id.attempt).unwrap();
            let writer = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
            crate::node_runtime::pg_store::command_log::register_metadata_digest_sql_functions(
                &writer,
            )
            .unwrap();
            assert_eq!(
                writer
                    .execute(
                        "UPDATE stream_uploads SET key = 'crossed-key' WHERE session_id = ?1",
                        [&owner.subject.id.attempt]
                    )
                    .unwrap(),
                1
            );
            assert_eq!(
                pg.get_stream_upload(&owner.subject.id.attempt).unwrap(),
                original
            );
            assert_eq!(prepared.inventory().payload_reference_count, 1);
            let holder = prepared.finish().unwrap();
            let writes = pg.conn.total_changes();
            let before = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let failure = holder
                .prepare_pending(&pg, &topology())
                .err()
                .expect("must reload crossed session");
            assert!(
                matches!(failure.error(), ContextError::Retention(StoreError::ShardScavengerScanIncomplete { context: "derive pending command payload inventory", errors }) if errors.contains("missing exact stream encryption"))
            );
            let holder = failure.into_submission();
            assert_holder(&holder, &expected, observed);
            assert_restored(&pg, false);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            assert_eq!(pending_install::observe(&pg), slot);
            assert_eq!(pending_payload_transaction::pages(&pg), pages);
            writer
                .execute(
                    "UPDATE stream_uploads SET key = ?1 WHERE session_id = ?2",
                    rusqlite::params![original.key.as_str(), &owner.subject.id.attempt],
                )
                .unwrap();
            let before = introduction::state(&pg);
            let holder = holder
                .prepare_pending(&pg, &topology())
                .unwrap()
                .finish()
                .unwrap();
            assert_holder(&holder, &expected, observed);
            drop(holder);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pending_install::observe(&pg), slot);
            assert_eq!(pending_payload_transaction::pages(&pg), pages);
        }
    }
}

#[test]
fn staging_context_submission_preparation_finish_failure_keeps_history() {
    let _clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        for observed in HISTORIES {
            let (_dir, pg, _owner, expected, holder) = setup(target.clone(), append, observed);
            let before = introduction::state(&pg);
            let slot = pending_install::observe(&pg);
            let pages = pending_payload_transaction::pages(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let writes = pg.conn.total_changes();
            let prepared = holder.prepare_pending(&pg, &topology()).unwrap();
            // Owner-local fault: end the read snapshot underneath finish so its
            // ROLLBACK reports a real SQLite error. This is not a simulated
            // durable-write failure, and inspection must not alter history.
            pg.conn.execute_batch("ROLLBACK").unwrap();
            assert!(pg.conn.is_autocommit());
            assert!(read_only(&pg));
            let failure = prepared.finish().err().expect("missing snapshot must fail");
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
            let holder = failure.into_submission();
            assert_holder(&holder, &expected, observed);
            assert_restored(&pg, false);
            let holder = holder
                .prepare_pending(&pg, &topology())
                .unwrap()
                .finish()
                .unwrap();
            assert_holder(&holder, &expected, observed);
            drop(holder);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pending_install::observe(&pg), slot);
            assert_eq!(pending_payload_transaction::pages(&pg), pages);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        }
    }
}

#[test]
fn staging_context_submission_preparation_keeps_ordinary_and_abandonment_contexts() {
    for count in [0, 2] {
        for source_bound in [false, true] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(multipart_target()).subject.object_pg.get(),
            )
            .unwrap();
            let owner = part::initialize(&pg, count);
            let source = part::command(&pg, &owner);
            let command = abandonment::cleanup(&pg, &owner, &source);
            let context = if source_bound {
                ContextualCommand::abandoned_part(
                    PartFinalizationSource::capture_for_test(&source)
                        .unwrap()
                        .bind_cleanup(command.clone(), &topology())
                        .unwrap(),
                    &topology(),
                )
                .unwrap()
            } else {
                ContextualCommand::decode_field(command.clone(), &[1, 0, 0], &topology()).unwrap()
            };
            let expected = ContextualCommand::decode_field(
                command,
                &context.encode_field().unwrap(),
                &topology(),
            )
            .unwrap();
            pg.conn.execute_batch("COMMIT").unwrap();
            let before = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let writes = pg.conn.total_changes();
            let prepared = context
                .into_submission()
                .prepare_pending(&pg, &topology())
                .unwrap();
            assert_eq!(
                prepared.observation().installation,
                Observed::NoAcceptanceObserved
            );
            assert_eq!(
                prepared.inventory(),
                PendingInventoryObservation {
                    payload_reference_count: count,
                    route_dependency_count: if source_bound { 2 } else { 0 },
                    has_future_window: false
                }
            );
            let holder = prepared.finish().unwrap();
            assert_holder(&holder, &expected, Observed::NoAcceptanceObserved);
            assert_restored(&pg, false);
            drop(holder);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            pending_install::assert_empty(&pg);
        }
    }
}

#[test]
fn staging_context_submission_preparation_drop_and_unwind_preserve_pending_state() {
    let _clock = test_time_override_guard(2_000);
    for observed in HISTORIES {
        for original_read_only in [false, true] {
            for exit in ["drop", "unwind", "failed finish unwind"] {
                let (_dir, pg, _owner, expected, holder) =
                    setup(StagingTarget::StreamPut, true, observed);
                let before = introduction::state(&pg);
                let slot = pending_install::observe(&pg);
                let pages = pending_payload_transaction::pages(&pg);
                let prefix = pg.metadata_command_replica_state().unwrap();
                let writes = pg.conn.total_changes();
                pg.conn
                    .pragma_update(None, "query_only", original_read_only)
                    .unwrap();
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let prepared = holder.prepare_pending(&pg, &topology()).unwrap();
                    assert_eq!(
                        prepared.observation(),
                        ContextSubmissionObservation {
                            command_id: expected.id(),
                            installation: observed
                        }
                    );
                    assert_eq!(prepared.inventory().payload_reference_count, 1);
                    match exit {
                        "drop" => drop(prepared),
                        "unwind" => panic!("unwind owned preparation"),
                        "failed finish unwind" => {
                            pg.conn.execute_batch("ROLLBACK").unwrap();
                            let _failure = prepared.finish().err().expect("missing snapshot");
                            panic!("unwind preparation failure");
                        }
                        _ => unreachable!(),
                    }
                }));
                assert_eq!(outcome.is_err(), exit != "drop");
                assert_restored(&pg, original_read_only);
                assert_eq!(pg.conn.total_changes(), writes);
                assert_eq!(introduction::state(&pg), before);
                assert_eq!(pending_install::observe(&pg), slot);
                assert_eq!(pending_payload_transaction::pages(&pg), pages);
                assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            }
        }
    }
}
