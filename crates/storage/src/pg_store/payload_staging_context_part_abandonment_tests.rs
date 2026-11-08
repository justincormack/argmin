// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only primary preparation. No new carrier or physical authority.

use super::*;
use crate::node_runtime::pg_store::payload_staging::{
    abandonment::AbandonmentError,
    candidate::CandidateBindingError,
    command_context::{ApplicationError, ContextError},
    submission::ContextualPartAbandonment,
};
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::sync::{atomic::AtomicUsize, Arc, Mutex};

#[path = "payload_staging_context_part_abandonment_reissue_tests.rs"]
mod reissue;

struct Fixture {
    dir: test_util::TempDir,
    pg: PgStore,
    owner: StagingOwnerRecord,
    source: MetadataCommandEnvelope,
    cleanup: MetadataCommandEnvelope,
}

fn setup(count: usize, terminal: bool) -> Fixture {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, count);
    let source = part::command(&pg, &owner);
    let cleanup = abandonment::cleanup(&pg, &owner, &source);
    if terminal {
        abandonment::terminal(&pg, &source, true);
    }
    pg.conn.execute_batch("COMMIT").unwrap();
    Fixture {
        dir,
        pg,
        owner,
        source,
        cleanup,
    }
}

fn source_context(source: &MetadataCommandEnvelope) -> ContextualCommand {
    application::ordinary_context(source, &topology()).unwrap()
}

fn cleanup_context(
    source: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
) -> ContextualCommand {
    ContextualCommand::abandoned_part(
        PartFinalizationSource::capture(source)
            .unwrap()
            .bind_cleanup(cleanup.clone(), &topology())
            .unwrap(),
        &topology(),
    )
    .unwrap()
}

fn check_pair(
    pair: &ContextualPartAbandonment,
    source: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
) {
    assert_eq!(
        pair.source(),
        ContextSubmissionObservation {
            command_id: source.id(),
            installation: Observed::NoAcceptanceObserved
        }
    );
    assert_eq!(
        pair.cleanup(),
        ContextSubmissionObservation {
            command_id: cleanup.id(),
            installation: Observed::NoAcceptanceObserved
        }
    );
    pair.assert_contexts_for_test(
        &source_context(source),
        &cleanup_context(source, cleanup),
        &topology(),
    );
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

fn stop_trace(pg: &mut PgStore) {
    pg.conn
        .trace_v2(TraceEventCodes::empty(), None::<fn(TraceEvent<'_>)>)
        .unwrap();
}

fn next_epoch(id: MetadataCommandId) -> MetadataCommandId {
    MetadataCommandId::new(
        ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap(),
        id.pg_id(),
        MetadataCommandLogIndex::new(1).unwrap(),
    )
}

#[test]
fn staging_context_part_abandonment_captures_exact_active_prefix_after_restart_and_compaction() {
    for count in [0, 2] {
        for compact in [false, true] {
            for future_epoch in [false, true] {
                for read_only in [false, true] {
                    let Fixture {
                        dir,
                        pg,
                        owner,
                        source,
                        mut cleanup,
                    } = setup(count, true);
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
                    if future_epoch {
                        let id = next_epoch(cleanup.id());
                        let digest = pg.metadata_command_replica_state().unwrap().state_digest;
                        pg.initialize_metadata_transfer_matching_state(
                            0,
                            id.cluster_epoch(),
                            0,
                            crate::control_plane::MetadataCommandLogHash::genesis(),
                            digest,
                        )
                        .unwrap();
                        cleanup = MetadataCommandEnvelope::new(id, cleanup.payload().clone());
                    }
                    let holder = source_context(&source).into_submission();
                    drop(pg);
                    let mut pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
                    let before = introduction::state(&pg);
                    let prefix = pg.metadata_command_replica_state().unwrap();
                    let writes = pg.conn.total_changes();
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
                    let pair = crate::clock::with_time_and_monotonic_override(
                        9_000_000,
                        90_000_000,
                        || holder.prepare_part_abandonment(&pg, 0, &topology(), cleanup.id()),
                    )
                    .unwrap();
                    check_pair(&pair, &source, &cleanup);
                    assert_eq!(pair.prefix(), &prefix);
                    assert_eq!(pair.source_inventory().payload_reference_count, count);
                    assert_eq!(pair.cleanup_inventory().payload_reference_count, count);
                    assert!(!pair.cleanup_inventory().has_future_window);
                    assert_eq!(*trace.lock().unwrap(), ["BEGIN DEFERRED", "ROLLBACK"]);
                    stop_trace(&mut pg);
                    // Unwinding the retained proposal cannot clean up either input.
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        let _pair = pair;
                        assert!(!future_epoch, "unwind proposed cleanup");
                    }));
                    assert_eq!(result.is_err(), future_epoch);
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
fn staging_context_part_abandonment_terminal_owners_propose_only_proof_custody() {
    for count in [0, 2] {
        for state in ["closed", "published", "completed", "already-retained"] {
            let Fixture {
                dir: _dir,
                pg,
                owner,
                source,
                cleanup,
            } = setup(count, true);
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            if state == "closed" || state == "already-retained" {
                super::super::super::close::apply_in_open_transaction(&pg, &topology(), &cleanup)
                    .unwrap();
            } else {
                let MetadataCommandPayload::CommitStreamPart(mut winner) = source.payload().clone()
                else {
                    unreachable!()
                };
                winner
                    .bucket_write_reservation
                    .reservation_id
                    .push_str("-winner");
                winner
                    .bucket_write_reservation
                    .owner_token
                    .push_str("-winner");
                part::apply(
                    &pg,
                    &MetadataCommandEnvelope::new(
                        cleanup.id(),
                        MetadataCommandPayload::CommitStreamPart(winner),
                    ),
                )
                .unwrap();
                if state == "completed" {
                    completion::apply(
                        &pg,
                        &completion::command(&pg, &owner, crate::VersionId::Null),
                    )
                    .unwrap();
                }
            }
            let MetadataCommandPayload::AbortStreamUpload(mut abort) = cleanup.payload().clone()
            else {
                unreachable!()
            };
            abort.staged_segments.clear();
            let cleanup = MetadataCommandEnvelope::new(
                cleanup.id(),
                MetadataCommandPayload::AbortStreamUpload(abort),
            );
            if state == "already-retained" {
                cleanup_context(&source, &cleanup)
                    .application(&topology())
                    .unwrap()
                    .apply_in_open_transaction(&pg, 0)
                    .unwrap();
            }
            pg.conn.execute_batch("COMMIT").unwrap();
            let before = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let writes = pg.conn.total_changes();
            let pair = source_context(&source)
                .into_submission()
                .prepare_part_abandonment(&pg, 0, &topology(), cleanup.id())
                .unwrap();
            check_pair(&pair, &source, &cleanup);
            assert_eq!(pair.cleanup_inventory().payload_reference_count, 0);
            assert_eq!(pair.source_inventory().payload_reference_count, count);
            assert_eq!(pair.prefix(), &prefix);
            drop(pair);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            restored(&pg, false);
        }
    }
}

#[test]
fn staging_context_part_abandonment_rejects_missing_or_substituted_source_and_corrupt_live_state() {
    for case in [
        "absent",
        "substituted-proof",
        "missing-owner",
        "missing-generation",
        "missing-high-water",
        "missing-segment",
        "crossed-session",
    ] {
        let Fixture {
            dir: _dir,
            pg,
            owner,
            mut source,
            cleanup,
        } = setup(2, case != "absent");
        match case {
            "absent" => (),
            "substituted-proof" => {
                let MetadataCommandPayload::CommitStreamPart(mut part) = source.payload().clone()
                else {
                    unreachable!()
                };
                part.bucket_write_reservation
                    .owner_token
                    .push_str("-substituted");
                source = MetadataCommandEnvelope::new(
                    source.id(),
                    MetadataCommandPayload::CommitStreamPart(part),
                );
            }
            "missing-owner" => {
                pg.conn
                    .execute(
                        "DELETE FROM payload_staging_owners WHERE attempt = ?1",
                        [&owner.subject.id.attempt],
                    )
                    .unwrap();
            }
            "missing-generation" => {
                pg.conn
                    .execute(
                        "DELETE FROM object_generation_reservations WHERE reservation_id = ?1",
                        [&owner.subject.id.attempt],
                    )
                    .unwrap();
            }
            "missing-high-water" => {
                pg.conn
                    .execute(
                        "DELETE FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
                        rusqlite::params![owner.subject.bucket, owner.subject.key],
                    )
                    .unwrap();
            }
            "missing-segment" => {
                pg.conn.execute("DELETE FROM stream_upload_segments WHERE session_id = ?1 AND segment_index = 1", [&owner.subject.id.attempt]).unwrap();
            }
            "crossed-session" => {
                pg.conn
                    .execute(
                        "UPDATE stream_uploads SET key = 'crossed-key' WHERE session_id = ?1",
                        [&owner.subject.id.attempt],
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        let failure = source_context(&source)
            .into_submission()
            .prepare_part_abandonment(&pg, 0, &topology(), cleanup.id())
            .err()
            .expect(case);
        if case == "absent" {
            assert!(
                matches!(
                    failure.error(),
                    ContextError::Abandonment(AbandonmentError::NotAbandoned)
                ),
                "{}",
                failure.error()
            );
        }
        if case == "substituted-proof" {
            assert!(
                matches!(
                    failure.error(),
                    ContextError::Abandonment(AbandonmentError::Store(
                        crate::StoreError::MetadataCommandLogConflict { .. }
                    ))
                ),
                "{}",
                failure.error()
            );
        }
        let original = failure.into_submission();
        original.assert_context_for_test(&source_context(&source), &topology());
        assert_eq!(
            original.observation().installation,
            Observed::NoAcceptanceObserved
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        restored(&pg, false);
    }
}

#[test]
fn staging_context_part_abandonment_wrong_kind_keeps_real_uncertain_and_installed_history() {
    let _clock = test_time_override_guard(2_000);
    for observed in [Observed::Unconfirmed, Observed::Installed] {
        for (target, append) in candidate::cases() {
            let (_dir, pg, owner, command) = candidate::setup(target, append);
            let captured = candidate::capture(&pg, &owner, command.clone(), append);
            let expected = described_context(&command, &captured.descriptor_bytes(), append);
            let mut holder = captured.into_submission();
            match observed {
                Observed::Unconfirmed => interrupt(&mut holder, &pg),
                Observed::Installed => submission::install(&mut holder, &pg).unwrap(),
                Observed::NoAcceptanceObserved => unreachable!(),
            }
            reject(&mut holder, &pg);
            let state = introduction::state(&pg);
            let slot = pending_install::observe(&pg);
            let writes = pg.conn.total_changes();
            let failure = holder
                .into_contextual()
                .prepare_part_abandonment(&pg, 0, &topology(), next_epoch(command.id()))
                .err()
                .expect("not finalizer");
            assert!(matches!(failure.error(), ContextError::CommandKind));
            let holder = failure.into_submission();
            assert_eq!(holder.observation().installation, observed);
            holder.assert_context_for_test(&expected, &topology());
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), state);
            assert_eq!(pending_install::observe(&pg), slot);
        }
    }
}

#[test]
fn staging_context_part_abandonment_rejects_crossed_position_topology_and_outer_transaction() {
    let Fixture {
        dir: _dir,
        pg,
        owner,
        source,
        cleanup,
    } = setup(2, true);
    let state = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    let mut holder = source_context(&source).into_submission();
    for id in [
        source.id(),
        MetadataCommandId::new(
            source.id().cluster_epoch(),
            PgId::new((pg.pg_id() + 1) % 8),
            cleanup.id().log_index(),
        ),
        MetadataCommandId::new(
            ClusterEpoch::new(source.id().cluster_epoch().get() - 1).unwrap(),
            source.id().pg_id(),
            cleanup.id().log_index(),
        ),
    ] {
        let failure = holder
            .prepare_part_abandonment(&pg, 0, &topology(), id)
            .err()
            .expect("bad position");
        assert!(matches!(
            failure.error(),
            ContextError::Abandonment(AbandonmentError::Subject)
        ));
        holder = failure.into_submission();
    }
    let foreign = PgTopology::new(&[owner.subject.object_pg.get()]).unwrap();
    let failure = holder
        .prepare_part_abandonment(&pg, 0, &foreign, cleanup.id())
        .err()
        .expect("topology");
    assert!(matches!(
        failure.error(),
        ContextError::Candidate(CandidateBindingError::Topology)
    ));
    pg.conn.execute_batch("BEGIN").unwrap();
    let failure = failure
        .into_submission()
        .prepare_part_abandonment(&pg, 0, &topology(), cleanup.id())
        .err()
        .expect("caller transaction");
    assert!(matches!(
        failure.error(),
        ContextError::Snapshot(crate::MetadataError::InvariantViolation { .. })
    ));
    assert!(!pg.conn.is_autocommit());
    pg.conn.execute_batch("ROLLBACK").unwrap();
    let pair = failure
        .into_submission()
        .prepare_part_abandonment(&pg, 0, &topology(), cleanup.id())
        .unwrap();
    check_pair(&pair, &source, &cleanup);
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), state);
}

#[test]
fn staging_context_part_abandonment_reads_one_snapshot_and_apply_rechecks_changed_session() {
    let Fixture {
        dir,
        mut pg,
        owner,
        source,
        cleanup,
    } = setup(2, true);
    let writer = rusqlite::Connection::open(dir.path().join("metadata.db")).unwrap();
    crate::node_runtime::pg_store::command_log::register_metadata_digest_sql_functions(&writer)
        .unwrap();
    let mutations = Arc::new(AtomicUsize::new(0));
    let captured = Arc::clone(&mutations);
    let session = owner.subject.id.attempt.clone();
    pg.conn.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(move |event: TraceEvent<'_>| {
        if let TraceEvent::Stmt(_, sql) = event {
            // After prefix pinning/source validation, before owner/session capture.
            if sql.contains("FROM payload_staging_owners") && captured.load(Ordering::SeqCst) == 0 {
                let changed = writer.execute("UPDATE stream_uploads SET key = 'crossed-key' WHERE session_id = ?1", [session.as_str()]).unwrap();
                assert_eq!(changed, 1);
                captured.fetch_add(1, Ordering::SeqCst);
            }
        }
    })).unwrap();
    let pair = source_context(&source)
        .into_submission()
        .prepare_part_abandonment(&pg, 0, &topology(), cleanup.id())
        .unwrap();
    assert_eq!(mutations.load(Ordering::SeqCst), 1);
    check_pair(&pair, &source, &cleanup);
    stop_trace(&mut pg);
    let state = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    let failure = source_context(&source)
        .into_submission()
        .prepare_part_abandonment(&pg, 0, &topology(), cleanup.id())
        .err()
        .expect("new snapshot sees changed session");
    assert!(matches!(
        failure.error(),
        ContextError::Abandonment(AbandonmentError::Closure(_))
    ));
    failure
        .into_submission()
        .assert_context_for_test(&source_context(&source), &topology());
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert!(matches!(
        cleanup_context(&source, &cleanup)
            .application(&topology())
            .unwrap()
            .apply_in_open_transaction(&pg, 0),
        Err(ApplicationError::Abandonment(AbandonmentError::Closure(_)))
    ));
    pg.conn.execute_batch("ROLLBACK").unwrap();
    drop(pair);
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), state);
    restored(&pg, false);
}

#[test]
fn staging_context_part_abandonment_interrupted_owner_read_returns_original_holder() {
    for read_only in [false, true] {
        let Fixture {
            dir: _dir,
            mut pg,
            source,
            cleanup,
            ..
        } = setup(2, true);
        let state = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        pg.conn
            .pragma_update(None, "query_only", read_only)
            .unwrap();
        let interrupt = pg.conn.get_interrupt_handle();
        let count = Arc::new(AtomicUsize::new(0));
        let captured = Arc::clone(&count);
        pg.conn
            .trace_v2(
                TraceEventCodes::SQLITE_TRACE_STMT,
                Some(move |event: TraceEvent<'_>| {
                    if let TraceEvent::Stmt(_, sql) = event {
                        if sql.contains("FROM payload_staging_owners") {
                            captured.fetch_add(1, Ordering::SeqCst);
                            interrupt.interrupt();
                        }
                    }
                }),
            )
            .unwrap();
        let failure = source_context(&source)
            .into_submission()
            .prepare_part_abandonment(&pg, 0, &topology(), cleanup.id())
            .err()
            .expect("owner read interrupted");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(
            failure.error().to_string().contains("interrupt"),
            "{}",
            failure.error()
        );
        stop_trace(&mut pg);
        let holder = failure.into_submission();
        holder.assert_context_for_test(&source_context(&source), &topology());
        let pair = holder
            .prepare_part_abandonment(&pg, 0, &topology(), cleanup.id())
            .unwrap();
        check_pair(&pair, &source, &cleanup);
        restored(&pg, read_only);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), state);
    }
}
