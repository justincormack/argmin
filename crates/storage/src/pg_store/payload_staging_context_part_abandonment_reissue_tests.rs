// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owning descriptive transitions only, not installed replacement/recovery.

use super::*;
use crate::control_plane::MetadataCommandLogHash;
use crate::node_runtime::pg_store::payload_staging::submission::ContextualPartAbandonmentReissue;

#[path = "payload_staging_context_part_abandonment_reissue_preparation_tests.rs"]
mod preparation;

fn later(id: MetadataCommandId) -> MetadataCommandId {
    MetadataCommandId::new(
        id.cluster_epoch(),
        id.pg_id(),
        MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
    )
}

fn at(command: &MetadataCommandEnvelope, id: MetadataCommandId) -> MetadataCommandEnvelope {
    MetadataCommandEnvelope::new(id, command.payload().clone())
}

fn expected_reissue(
    pair: &ContextualPartAbandonmentReissue,
    source: &MetadataCommandEnvelope,
    previous: &MetadataCommandEnvelope,
    successor: &MetadataCommandEnvelope,
) {
    assert_eq!(pair.source().command_id, source.id());
    assert_eq!(pair.previous_cleanup().command_id, previous.id());
    assert_eq!(pair.proposed_cleanup().command_id, successor.id());
    // The complete-context installer is not active yet; do not invent accepted
    // histories. Every original context, including its exact source, is pinned.
    assert_eq!(pair.source().installation, Observed::NoAcceptanceObserved);
    assert_eq!(
        pair.previous_cleanup().installation,
        Observed::NoAcceptanceObserved
    );
    assert_eq!(
        pair.proposed_cleanup().installation,
        Observed::NoAcceptanceObserved
    );
    pair.assert_contexts_for_test(
        &source_context(source),
        &cleanup_context(source, previous),
        &cleanup_context(source, successor),
        &topology(),
    );
}

fn terminal_state(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    source: &MetadataCommandEnvelope,
    cleanup: MetadataCommandEnvelope,
    state: &str,
) -> MetadataCommandEnvelope {
    if state == "active" {
        return cleanup;
    }
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    if state == "closed" {
        super::super::super::super::close::apply_in_open_transaction(pg, &topology(), &cleanup)
            .unwrap();
    } else {
        let MetadataCommandPayload::CommitStreamPart(mut winner) = source.payload().clone() else {
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
            pg,
            &MetadataCommandEnvelope::new(
                cleanup.id(),
                MetadataCommandPayload::CommitStreamPart(winner),
            ),
        )
        .unwrap();
        if state == "completed" {
            completion::apply(pg, &completion::command(pg, owner, crate::VersionId::Null)).unwrap();
        }
    }
    let MetadataCommandPayload::AbortStreamUpload(mut abort) = cleanup.payload().clone() else {
        unreachable!()
    };
    abort.staged_segments.clear();
    pg.conn.execute_batch("COMMIT").unwrap();
    MetadataCommandEnvelope::new(
        cleanup.id(),
        MetadataCommandPayload::AbortStreamUpload(abort),
    )
}

#[test]
fn staging_part_abandonment_reissue_retains_original_source_and_both_cleanup_candidates_without_sql(
) {
    for count in [0, 2] {
        for state in ["active", "closed", "published", "completed"] {
            for new_epoch in [false, true] {
                for unwind in [false, true] {
                    let Fixture {
                        dir,
                        pg,
                        owner,
                        source,
                        cleanup,
                    } = setup(count, true);
                    let cleanup = terminal_state(&pg, &owner, &source, cleanup, state);
                    let pair = source_context(&source)
                        .into_submission()
                        .prepare_part_abandonment(&pg, 0, &topology(), cleanup.id())
                        .unwrap();
                    let captured_prefix = *pair.prefix();
                    let successor = at(
                        &cleanup,
                        if new_epoch {
                            next_epoch(cleanup.id())
                        } else {
                            later(cleanup.id())
                        },
                    );
                    let before = introduction::state(&pg);
                    let prefix = pg.metadata_command_replica_state().unwrap();
                    let pending = pending_install::observe(&pg);
                    let pages = pending_payload_transaction::pages(&pg);
                    drop(pg);
                    let mut pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
                    let writes = pg.conn.total_changes();
                    let statements = Arc::new(AtomicUsize::new(0));
                    let captured = Arc::clone(&statements);
                    pg.conn
                        .trace_v2(
                            TraceEventCodes::SQLITE_TRACE_STMT,
                            Some(move |event: TraceEvent<'_>| {
                                if matches!(event, TraceEvent::Stmt(..)) {
                                    captured.fetch_add(1, Ordering::SeqCst);
                                }
                            }),
                        )
                        .unwrap();
                    let reissue = crate::clock::with_time_and_monotonic_override(
                        9_000_000,
                        90_000_000,
                        || {
                            if new_epoch {
                                pair.into_epoch_reissue(successor.id())
                            } else {
                                pair.into_same_epoch_reissue(successor.id())
                            }
                        },
                    )
                    .unwrap();
                    expected_reissue(&reissue, &source, &cleanup, &successor);
                    assert_eq!(reissue.captured_prefix(), &captured_prefix);
                    let outcome =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                            let _reissue = reissue;
                            assert!(!unwind, "unwind cleanup reissue");
                        }));
                    assert_eq!(outcome.is_err(), unwind);
                    assert_eq!(
                        statements.load(Ordering::SeqCst),
                        0,
                        "derivation/Drop must not access storage"
                    );
                    stop_trace(&mut pg);
                    assert_eq!(pg.conn.total_changes(), writes);
                    assert_eq!(introduction::state(&pg), before);
                    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                    assert_eq!(pending_install::observe(&pg), pending);
                    assert_eq!(pending_payload_transaction::pages(&pg), pages);
                    restored(&pg, false);
                }
            }
        }
    }
}

#[test]
fn staging_part_abandonment_reissue_rejection_returns_the_exact_original_proposal() {
    for count in [0, 2] {
        for new_epoch in [false, true] {
            let Fixture {
                dir: _dir,
                mut pg,
                source,
                cleanup,
                ..
            } = setup(count, true);
            let mut proposal = source_context(&source)
                .into_submission()
                .prepare_part_abandonment(&pg, 0, &topology(), cleanup.id())
                .unwrap();
            let captured_prefix = *proposal.prefix();
            let source_inventory = proposal.source_inventory();
            let cleanup_inventory = proposal.cleanup_inventory();
            let before = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let pending = pending_install::observe(&pg);
            let writes = pg.conn.total_changes();
            let statements = Arc::new(AtomicUsize::new(0));
            let captured = Arc::clone(&statements);
            pg.conn
                .trace_v2(
                    TraceEventCodes::SQLITE_TRACE_STMT,
                    Some(move |event: TraceEvent<'_>| {
                        if matches!(event, TraceEvent::Stmt(..)) {
                            captured.fetch_add(1, Ordering::SeqCst);
                        }
                    }),
                )
                .unwrap();
            let valid = if new_epoch {
                next_epoch(cleanup.id())
            } else {
                later(cleanup.id())
            };
            for invalid in [
                cleanup.id(),
                source.id(),
                MetadataCommandId::new(
                    ClusterEpoch::new(cleanup.id().cluster_epoch().get() - 1).unwrap(),
                    cleanup.id().pg_id(),
                    valid.log_index(),
                ),
                MetadataCommandId::new(
                    valid.cluster_epoch(),
                    PgId::new((cleanup.id().pg_id().get() + 1) % 8),
                    valid.log_index(),
                ),
                if new_epoch {
                    later(cleanup.id())
                } else {
                    next_epoch(cleanup.id())
                },
            ] {
                let failure = if new_epoch {
                    proposal.into_epoch_reissue(invalid)
                } else {
                    proposal.into_same_epoch_reissue(invalid)
                }
                .err()
                .expect("invalid position");
                assert!(matches!(failure.error(), ContextError::ReissuePosition));
                proposal = failure.into_proposal();
                check_pair(&proposal, &source, &cleanup);
                assert_eq!(proposal.prefix(), &captured_prefix);
                assert_eq!(proposal.source_inventory(), source_inventory);
                assert_eq!(proposal.cleanup_inventory(), cleanup_inventory);
            }
            let successor = at(&cleanup, valid);
            let reissue = if new_epoch {
                proposal.into_epoch_reissue(valid)
            } else {
                proposal.into_same_epoch_reissue(valid)
            }
            .unwrap();
            expected_reissue(&reissue, &source, &cleanup, &successor);
            drop(reissue);
            assert_eq!(statements.load(Ordering::SeqCst), 0);
            stop_trace(&mut pg);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            assert_eq!(pending_install::observe(&pg), pending);
        }
    }
}

#[test]
fn staging_part_abandonment_reissue_preserves_original_capture_across_epoch_reset_and_compaction() {
    for count in [0, 2] {
        let Fixture {
            dir,
            pg,
            owner,
            source,
            cleanup,
        } = setup(count, true);
        let proposal = source_context(&source)
            .into_submission()
            .prepare_part_abandonment(&pg, 0, &topology(), cleanup.id())
            .unwrap();
        let captured_prefix = *proposal.prefix();
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
        let id = next_epoch(cleanup.id());
        pg.initialize_metadata_transfer_matching_state(
            0,
            id.cluster_epoch(),
            0,
            MetadataCommandLogHash::genesis(),
            captured_prefix.state_digest,
        )
        .unwrap();
        drop(pg);
        let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
        let current_prefix = pg.metadata_command_replica_state().unwrap();
        assert_ne!(current_prefix, captured_prefix);
        let writes = pg.conn.total_changes();
        let before = introduction::state(&pg);
        let reissue = proposal.into_epoch_reissue(id).unwrap();
        expected_reissue(&reissue, &source, &cleanup, &at(&cleanup, id));
        assert_eq!(
            reissue.captured_prefix(),
            &captured_prefix,
            "diagnostic cannot pretend to be refreshed authority"
        );
        drop(reissue);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), current_prefix);
        pending_install::assert_empty(&pg);
    }
}

#[test]
fn staging_part_abandonment_reissue_does_not_recapture_changed_cleanup_or_grant_apply_authority() {
    for new_epoch in [false, true] {
        let Fixture {
            dir: _dir,
            pg,
            source,
            cleanup,
            ..
        } = setup(2, true);
        let proposal = source_context(&source)
            .into_submission()
            .prepare_part_abandonment(&pg, 0, &topology(), cleanup.id())
            .unwrap();
        assert_eq!(proposal.cleanup_inventory().payload_reference_count, 2);
        // A different worker closes the owner after capture. A fresh proposal
        // needs no segment inputs, but reissue must not silently change payload.
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        super::super::super::super::close::apply_in_open_transaction(&pg, &topology(), &cleanup)
            .unwrap();
        pg.conn.execute_batch("COMMIT").unwrap();
        let id = if new_epoch {
            next_epoch(cleanup.id())
        } else {
            later(cleanup.id())
        };
        let fresh = source_context(&source)
            .into_submission()
            .prepare_part_abandonment(&pg, 0, &topology(), id)
            .unwrap();
        assert_eq!(fresh.cleanup_inventory().payload_reference_count, 0);
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        let reissue = if new_epoch {
            proposal.into_epoch_reissue(id)
        } else {
            proposal.into_same_epoch_reissue(id)
        }
        .unwrap();
        let successor = at(&cleanup, id);
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
        drop((reissue, fresh));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        restored(&pg, false);
    }
}
