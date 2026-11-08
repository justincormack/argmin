// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh primary -> complete-context holder, with no installation or payload IO.

use super::super::super::capture::{StagingCapture, StagingCaptureError};
use super::super::super::command_context::ContextError;
use super::super::super::submission::FreshContextSubmissionError;
use super::*;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::traits::PgMetadataStore;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::sync::{atomic::AtomicUsize, Arc, Mutex};

fn build(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    command: MetadataCommandEnvelope,
    append: bool,
    fence: AdmittedRouteEffectFence,
) -> Result<ContextualSubmission, FreshContextSubmissionError> {
    let config = capture::config(owner);
    let topology = topology();
    let capture = StagingCapture {
        topology: &topology,
        placement: PayloadPlacementInput::from_server_config(&config),
        effect_fence: fence,
    };
    if append {
        capture.append_submission(pg, config.node_id, command)
    } else {
        capture.introduction_submission(
            pg,
            config.node_id,
            command,
            &proof(&owner.subject, owner.subject.creation_operation()),
        )
    }
}

fn unbounded(owner: &StagingOwnerRecord) -> AdmittedRouteEffectFence {
    AdmittedRouteEffectFence::unbounded(owner.subject.originating_epoch)
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
fn staging_context_fresh_submission_captures_all_targets_in_one_read_only_view() {
    let clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        for read_only in [false, true] {
            clock.set(2_000);
            let (_dir, mut pg, owner, command) = candidate::setup(target.clone(), append);
            // Independent component capture pins the exact descriptor, not only
            // ID/count equality. The new path must retain it through handoff.
            let expected = ContextualCommand::described(
                candidate::capture(&pg, &owner, command.clone(), append),
                &topology(),
            )
            .unwrap();
            let state = introduction::state(&pg);
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
            let holder = build(&pg, &owner, command.clone(), append, unbounded(&owner)).unwrap();
            assert_eq!(
                *transactions.lock().unwrap(),
                ["BEGIN DEFERRED", "ROLLBACK"]
            );
            stop_trace(&mut pg);
            restored(&pg, read_only);
            assert_eq!(
                holder.observation(),
                ContextSubmissionObservation {
                    command_id: command.id(),
                    installation: Observed::NoAcceptanceObserved,
                }
            );
            holder.assert_context_for_test(&expected, &topology());
            // An expired captured descriptor is retained, not recaptured when
            // a later read-only inspection or unsent reissue is prepared.
            clock.set(9_000_000);
            let holder = holder
                .prepare_pending(&pg, &topology())
                .unwrap()
                .finish()
                .unwrap();
            holder.assert_context_for_test(&expected, &topology());
            let id = command.id();
            let pair = holder
                .into_same_epoch_reissue(MetadataCommandId::new(
                    id.cluster_epoch(),
                    id.pg_id(),
                    MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
                ))
                .unwrap();
            assert_eq!(pair.source().installation, Observed::NoAcceptanceObserved);
            assert_eq!(
                pair.successor().installation,
                Observed::NoAcceptanceObserved
            );
            assert!(pair.into_observed_successor().is_err());
            restored(&pg, read_only);
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), state);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            pending_install::assert_empty(&pg);
        }
    }
}

#[test]
fn staging_context_fresh_submission_rejects_enclosing_transaction_and_occupied_command() {
    let _clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, append);
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let error = build(&pg, &owner, command.clone(), append, unbounded(&owner))
            .err()
            .unwrap();
        assert!(matches!(
            error,
            FreshContextSubmissionError::Capture(StagingCaptureError::Metadata(
                crate::MetadataError::InvariantViolation {
                    context: "primary metadata command build snapshot",
                    ..
                }
            ))
        ));
        assert!(
            !pg.conn.is_autocommit(),
            "must not adopt caller transaction"
        );
        pg.conn.execute_batch("ROLLBACK").unwrap();
        let id = command.id();
        let crossed = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                id.cluster_epoch(),
                id.pg_id(),
                MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
            ),
            command.payload().clone(),
        );
        assert!(matches!(
            build(&pg, &owner, crossed, append, unbounded(&owner)),
            Err(FreshContextSubmissionError::Capture(
                StagingCaptureError::Command
            ))
        ));
        // Independently exercise wrong operation selection, not just position.
        assert!(matches!(
            build(&pg, &owner, command.clone(), !append, unbounded(&owner)),
            Err(FreshContextSubmissionError::Capture(
                StagingCaptureError::Command
            ))
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        restored(&pg, false);
        let described = candidate::capture(&pg, &owner, command.clone(), append);
        pending_payload_transaction::install(&pg, &command, Some(&described)).unwrap();
        let slot = pending_install::observe(&pg);
        let pages = pending_payload_transaction::pages(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(
            build(&pg, &owner, command, append, unbounded(&owner)),
            Err(FreshContextSubmissionError::Capture(
                StagingCaptureError::Command
            ))
        ));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pending_install::observe(&pg), slot);
        assert_eq!(pending_payload_transaction::pages(&pg), pages);
        restored(&pg, false);
    }
}

#[test]
fn staging_context_fresh_submission_checks_deadline_after_read_snapshot_release() {
    let clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        clock.set(2_000);
        let (_dir, mut pg, owner, command) = candidate::setup(target, append);
        let state = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let changes = pg.conn.total_changes();
        let fired = Arc::new(AtomicUsize::new(0));
        let capture = Arc::clone(&fired);
        let control = clock.control();
        pg.conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        if sql == "ROLLBACK" {
                            capture.fetch_add(1, Ordering::SeqCst);
                            // Beyond the effective monotonic cutoff, BEFORE the raw
                            // authority wall bound. No renewed-route recapture allowed.
                            control.set(4_500);
                        }
                    }
                }),
            )
            .unwrap();
        let result = build(
            &pg,
            &owner,
            command.clone(),
            append,
            AdmittedRouteEffectFence::bounded(owner.subject.originating_epoch, 5_000, 4_000),
        );
        assert_eq!(fired.load(Ordering::SeqCst), 1);
        stop_trace(&mut pg);
        assert!(matches!(
            result,
            Err(FreshContextSubmissionError::Capture(
                StagingCaptureError::Store(crate::StoreError::RouteMapExpired { .. })
            ))
        ));
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
        let result = build(
            &pg,
            &owner,
            command,
            append,
            AdmittedRouteEffectFence::bounded(owner.subject.originating_epoch, 5_000, 4_000),
        );
        assert!(matches!(
            result,
            Err(FreshContextSubmissionError::Capture(
                StagingCaptureError::Store(crate::StoreError::RouteMapExpired { .. })
            ))
        ));
        assert_eq!(
            statements.load(Ordering::SeqCst),
            0,
            "expired capture must not begin SQL inspection"
        );
        stop_trace(&mut pg);
        restored(&pg, false);
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), state);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        pending_install::assert_empty(&pg);
    }
}

#[test]
fn staging_context_fresh_submission_interruptions_do_not_export_partial_capture() {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        for inventory in [false, true] {
            for read_only in [false, true] {
                let (_dir, mut pg, owner, command) = candidate::setup(target.clone(), true);
                let state = introduction::state(&pg);
                let prefix = pg.metadata_command_replica_state().unwrap();
                let changes = pg.conn.total_changes();
                pg.conn
                    .pragma_update(None, "query_only", read_only)
                    .unwrap();
                let fired = Arc::new(AtomicUsize::new(0));
                let captured = Arc::clone(&fired);
                let interrupt = pg.conn.get_interrupt_handle();
                pg.conn.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        let selected = if inventory {
                            sql.starts_with("SELECT encryption_type, encryption_state FROM stream_uploads")
                        } else {
                            sql.contains("FROM metadata_command_replica_state WHERE singleton = 0")
                        };
                        if selected && captured.fetch_add(1, Ordering::SeqCst) == 0 {
                            interrupt.interrupt();
                        }
                    }
                })).unwrap();
                let error = build(&pg, &owner, command.clone(), true, unbounded(&owner))
                    .err()
                    .expect("selected SQL read must be reached and fail");
                assert_eq!(fired.load(Ordering::SeqCst), 1);
                stop_trace(&mut pg);
                assert!(error.to_string().contains("interrupt"), "{error:?}");
                if inventory {
                    assert!(matches!(
                        error,
                        FreshContextSubmissionError::Context(ContextError::Retention(
                            crate::StoreError::Db {
                                context:
                                    "load exact stream encryption for pending payload inventory",
                                ..
                            }
                        ))
                    ));
                } else {
                    assert!(matches!(
                        error,
                        FreshContextSubmissionError::Capture(StagingCaptureError::Store(
                            crate::StoreError::Db {
                                context: "load metadata command replica state",
                                ..
                            }
                        ))
                    ));
                }
                restored(&pg, read_only);
                let holder = build(&pg, &owner, command, true, unbounded(&owner)).unwrap();
                assert_eq!(
                    holder.observation().installation,
                    Observed::NoAcceptanceObserved
                );
                drop(holder);
                restored(&pg, read_only);
                assert_eq!(pg.conn.total_changes(), changes);
                assert_eq!(introduction::state(&pg), state);
                assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                pending_install::assert_empty(&pg);
            }
        }
    }
}

#[test]
fn staging_context_fresh_submission_capture_and_inventory_share_the_same_snapshot() {
    let _clock = test_time_override_guard(2_000);
    for target in [StagingTarget::StreamPut, multipart_target()] {
        let (dir, mut pg, owner, command) = candidate::setup(target, true);
        let expected = ContextualCommand::described(
            candidate::capture(&pg, &owner, command.clone(), true),
            &topology(),
        )
        .unwrap();
        let original = pg.get_stream_upload(&owner.subject.id.attempt).unwrap();
        let writer = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
        crate::node_runtime::pg_store::command_log::register_metadata_digest_sql_functions(&writer)
            .unwrap();
        let changed = Arc::new(Mutex::new(None));
        let captured = Arc::clone(&changed);
        let session = owner.subject.id.attempt.clone();
        pg.conn.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(move |event: TraceEvent<'_>| {
            if let TraceEvent::Stmt(_, sql) = event {
                if sql.starts_with("SELECT encryption_type, encryption_state FROM stream_uploads") {
                    *captured.lock().unwrap() = Some(writer.execute("UPDATE stream_uploads SET key = 'crossed-key' WHERE session_id = ?1", [session.as_str()]));
                }
            }
        })).unwrap();
        let writes = pg.conn.total_changes();
        let holder = build(&pg, &owner, command, true, unbounded(&owner)).unwrap();
        assert_eq!(
            changed.lock().unwrap().as_ref().unwrap().as_ref().unwrap(),
            &1
        );
        stop_trace(&mut pg);
        assert_eq!(
            pg.get_stream_upload(&owner.subject.id.attempt)
                .unwrap()
                .key
                .as_str(),
            "crossed-key"
        );
        holder.assert_context_for_test(&expected, &topology());
        let state = introduction::state(&pg);
        // Fresh capture is NOT an installation certificate: a subsequent view
        // must reload and reject the externally changed session, keeping history.
        let failure = holder.prepare_pending(&pg, &topology()).err().unwrap();
        assert!(failure
            .error()
            .to_string()
            .contains("missing exact stream encryption"));
        let holder = failure.into_submission();
        holder.assert_context_for_test(&expected, &topology());
        assert_eq!(
            holder.observation().installation,
            Observed::NoAcceptanceObserved
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), state);
        restored(&pg, false);
        pending_install::assert_empty(&pg);
        // Restore the owner-local injected row, then reuse the same holder.
        let writer = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
        crate::node_runtime::pg_store::command_log::register_metadata_digest_sql_functions(&writer)
            .unwrap();
        writer
            .execute(
                "UPDATE stream_uploads SET key = ?1 WHERE session_id = ?2",
                rusqlite::params![original.key.as_str(), owner.subject.id.attempt.as_str()],
            )
            .unwrap();
        let holder = holder
            .prepare_pending(&pg, &topology())
            .unwrap()
            .finish()
            .unwrap();
        holder.assert_context_for_test(&expected, &topology());
        drop(holder);
    }
}
