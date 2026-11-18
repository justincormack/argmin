// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Cross-epoch proposals retain history, not authority to recover. Real pending
//! SQL outcomes seed observations; these tests never install a future carrier.

use super::*;
use crate::control_plane::MetadataCommandLogHash;
use crate::node_runtime::pg_store::payload_staging::command_context::ContextError;
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use std::sync::{atomic::AtomicUsize, Arc};

#[path = "payload_staging_context_epoch_preparation_tests.rs"]
mod preparation;

fn new_epoch(id: MetadataCommandId, delta: u64, index: u64) -> MetadataCommandId {
    MetadataCommandId::new(
        ClusterEpoch::new(id.cluster_epoch().get() + delta).unwrap(),
        id.pg_id(),
        MetadataCommandLogIndex::new(index).unwrap(),
    )
}

fn expected_successor(
    command: &MetadataCommandEnvelope,
    context: &ContextualCommand,
    id: MetadataCommandId,
) -> ContextualCommand {
    ContextualCommand::decode_field(
        MetadataCommandEnvelope::new(id, command.payload().clone()),
        &context.encode_field().unwrap(),
        &topology(),
    )
    .unwrap()
}

fn assert_pair(
    pair: &ContextualEpochReissue,
    source: &ContextualCommand,
    successor: &ContextualCommand,
    original: Observed,
) {
    assert_eq!(
        pair.source(),
        ContextSubmissionObservation {
            command_id: source.id(),
            installation: original,
        }
    );
    assert_eq!(
        pair.successor(),
        ContextSubmissionObservation {
            command_id: successor.id(),
            installation: Observed::NoAcceptanceObserved,
        }
    );
    pair.assert_contexts_for_test(source, successor, &topology());
}

fn seed_history(holder: &mut StagingSubmission, pg: &PgStore, observed: Observed) {
    match observed {
        Observed::NoAcceptanceObserved => (),
        Observed::Unconfirmed => interrupt(holder, pg),
        Observed::Installed => submission::install(holder, pg).unwrap(),
    }
    reject(holder, pg);
    assert_eq!(holder.observation(), observed);
}

#[test]
fn staging_context_epoch_reissue_retains_all_descriptors_and_histories_after_reopen() {
    let clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        for observed in [
            Observed::NoAcceptanceObserved,
            Observed::Unconfirmed,
            Observed::Installed,
        ] {
            for (delta, index) in [(1, 1), (1, 17), (3, 1)] {
                for unwind in [false, true] {
                    clock.set(2_000);
                    let (dir, pg, owner, command) = candidate::setup(target.clone(), append);
                    let captured = candidate::capture(&pg, &owner, command.clone(), append);
                    let source = described_context(&command, &captured.descriptor_bytes(), append);
                    let successor = expected_successor(
                        &command,
                        &source,
                        new_epoch(command.id(), delta, index),
                    );
                    pending_payload_transaction::install(&pg, &command, Some(&captured)).unwrap();
                    let mut holder = captured.into_submission();
                    seed_history(&mut holder, &pg, observed);
                    let holder = holder.into_contextual();
                    let state = introduction::state(&pg);
                    let slot = pending_install::observe(&pg);
                    let pages = pending_payload_transaction::pages(&pg);
                    let prefix = pg.metadata_command_replica_state().unwrap();
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
                    clock.set(9_000_000);
                    let pair = holder.into_epoch_reissue(successor.id()).unwrap();
                    assert_pair(&pair, &source, &successor, observed);
                    let outcome =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                            let _pair = pair;
                            assert!(!unwind, "unwind epoch-reissue ownership");
                        }));
                    assert_eq!(outcome.is_err(), unwind);
                    assert_eq!(
                        statements.load(Ordering::SeqCst),
                        0,
                        "proposal/Drop must not inspect or mutate storage"
                    );
                    pg.conn
                        .trace_v2(TraceEventCodes::empty(), None::<fn(TraceEvent<'_>)>)
                        .unwrap();
                    assert_eq!(pg.conn.total_changes(), writes);
                    assert_eq!(introduction::state(&pg), state);
                    assert_eq!(pending_install::observe(&pg), slot);
                    assert_eq!(pending_payload_transaction::pages(&pg), pages);
                    assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
                }
            }
        }
    }
}

#[test]
fn staging_context_epoch_reissue_rejects_crossed_pg_and_nonadvancing_epoch_without_resetting_source(
) {
    let clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        for observed in [
            Observed::NoAcceptanceObserved,
            Observed::Unconfirmed,
            Observed::Installed,
        ] {
            clock.set(2_000);
            let (_dir, pg, owner, command) = candidate::setup(target.clone(), append);
            let captured = candidate::capture(&pg, &owner, command.clone(), append);
            let source = described_context(&command, &captured.descriptor_bytes(), append);
            pending_payload_transaction::install(&pg, &command, Some(&captured)).unwrap();
            let mut holder = captured.into_submission();
            seed_history(&mut holder, &pg, observed);
            let mut holder = holder.into_contextual();
            let before = holder.observation();
            let id = command.id();
            assert!(id.cluster_epoch().get() > 1);
            let foreign_pg = PgId::new((id.pg_id().get() + 1) % 8);
            let invalid = [
                id,
                MetadataCommandId::new(
                    id.cluster_epoch(),
                    id.pg_id(),
                    MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
                ),
                MetadataCommandId::new(
                    ClusterEpoch::new(id.cluster_epoch().get() - 1).unwrap(),
                    id.pg_id(),
                    MetadataCommandLogIndex::new(99).unwrap(),
                ),
                MetadataCommandId::new(
                    ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap(),
                    foreign_pg,
                    MetadataCommandLogIndex::new(1).unwrap(),
                ),
            ];
            let state = introduction::state(&pg);
            let slot = pending_install::observe(&pg);
            let pages = pending_payload_transaction::pages(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let writes = pg.conn.total_changes();
            clock.set(9_000_000);
            for invalid in invalid {
                let failure = holder
                    .into_epoch_reissue(invalid)
                    .err()
                    .expect("invalid epoch proposal");
                assert!(matches!(failure.error(), ContextError::ReissuePosition));
                holder = failure.into_submission();
                assert_eq!(holder.observation(), before);
                holder.assert_context_for_test(&source, &topology());
            }
            // The rejected transition does not prevent a valid proposal; it
            // must retain the original descriptor and uncertainty to do so.
            let successor = expected_successor(&command, &source, new_epoch(id, 1, 1));
            let pair = holder.into_epoch_reissue(successor.id()).unwrap();
            assert_pair(&pair, &source, &successor, observed);
            drop(pair);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), state);
            assert_eq!(pending_install::observe(&pg), slot);
            assert_eq!(pending_payload_transaction::pages(&pg), pages);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        }
    }
}

#[test]
fn staging_context_epoch_reissue_keeps_the_observed_same_epoch_successors_original_descriptor() {
    let clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        clock.set(2_000);
        let (_dir, pg, owner, command) = candidate::setup(target, append);
        let captured = candidate::capture(&pg, &owner, command.clone(), append);
        let descriptor = captured.descriptor_bytes();
        let mut source = captured.into_submission();
        submission::install(&mut source, &pg).unwrap();
        let id = command.id();
        let replaced = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                id.cluster_epoch(),
                id.pg_id(),
                MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
            ),
            command.payload().clone(),
        );
        let mut pair = source.into_same_epoch_reissue(replaced.id()).unwrap();
        assert!(pair.replace(&pg, 0, &topology()).unwrap());
        let source = match pair.into_contextual().into_observed_successor() {
            Ok(source) => source,
            Err(_) => panic!("observed replacement missing"),
        };
        let expected = described_context(&replaced, &descriptor, append);
        let successor = expected_successor(&replaced, &expected, new_epoch(replaced.id(), 1, 1));
        let state = introduction::state(&pg);
        let slot = pending_install::observe(&pg);
        let pages = pending_payload_transaction::pages(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        clock.set(9_000_000);
        let pair = source.into_epoch_reissue(successor.id()).unwrap();
        assert_pair(&pair, &expected, &successor, Observed::Installed);
        drop(pair);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), state);
        assert_eq!(pending_install::observe(&pg), slot);
        assert_eq!(pending_payload_transaction::pages(&pg), pages);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    }
}

fn abandoned_context(
    finalizer: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
) -> ContextualCommand {
    ContextualCommand::abandoned_part(
        PartFinalizationSource::capture_for_test(finalizer)
            .unwrap()
            .bind_cleanup(cleanup.clone(), &topology())
            .unwrap(),
        &topology(),
    )
    .unwrap()
}

#[test]
fn staging_context_epoch_reissue_retains_ordinary_and_original_abandoned_source_after_compaction() {
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
            drop(pg);
            let pg = PgStore::open(dir.path(), pg_id).unwrap();
            let state = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let writes = pg.conn.total_changes();
            let ordinary =
                ContextualCommand::decode_field(cleanup.clone(), &[1, 0, 0], &topology()).unwrap();
            let abandoned = abandoned_context(&finalizer, &cleanup);
            assert_ne!(
                ordinary.encode_field().unwrap(),
                abandoned.encode_field().unwrap()
            );
            for context in [ordinary, abandoned] {
                let expected = ContextualCommand::decode_field(
                    cleanup.clone(),
                    &context.encode_field().unwrap(),
                    &topology(),
                )
                .unwrap();
                let successor = expected_successor(&cleanup, &context, id);
                let pair =
                    crate::clock::with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                        context.into_submission().into_epoch_reissue(id)
                    })
                    .unwrap();
                assert_pair(&pair, &expected, &successor, Observed::NoAcceptanceObserved);
                drop(pair);
            }
            // Also retain the first-class ordinary, non-cleanup variant.
            let reserve = MetadataCommandEnvelope::new(
                cleanup.id(),
                MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                    owner.subject.bucket.clone(),
                    owner.subject.key.clone(),
                    crate::VersionId::from_u64(17),
                )),
            );
            let context =
                ContextualCommand::decode_field(reserve.clone(), &[1, 0, 0], &topology()).unwrap();
            let expected =
                ContextualCommand::decode_field(reserve.clone(), &[1, 0, 0], &topology()).unwrap();
            let successor = expected_successor(&reserve, &context, id);
            let pair = context.into_submission().into_epoch_reissue(id).unwrap();
            assert_pair(&pair, &expected, &successor, Observed::NoAcceptanceObserved);
            drop(pair);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), state);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            pending_install::assert_empty(&pg);
        }
    }
}
