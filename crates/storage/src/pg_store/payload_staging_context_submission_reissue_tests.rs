// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming retry preparation, not an authenticated carrier or pending send.
//! Source histories come from real SQL outcomes, never a test-only setter.

use super::*;
use crate::node_runtime::pg_store::payload_staging::command_context::ContextError;

fn next(id: MetadataCommandId) -> MetadataCommandId {
    MetadataCommandId::new(
        id.cluster_epoch(),
        id.pg_id(),
        MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
    )
}

fn expected_successor(
    command: &MetadataCommandEnvelope,
    context: &ContextualCommand,
) -> ContextualCommand {
    ContextualCommand::decode_field(
        MetadataCommandEnvelope::new(next(command.id()), command.payload().clone()),
        &context.encode_field().unwrap(),
        &topology(),
    )
    .unwrap()
}

fn assert_rejected_positions_preserve(
    mut holder: ContextualSubmission,
    expected: &ContextualCommand,
) -> ContextualSubmission {
    let before = holder.observation();
    let id = before.command_id;
    let mut invalid = vec![
        id,
        MetadataCommandId::new(
            id.cluster_epoch(),
            PgId::new((id.pg_id().get() + 1) % 8),
            next(id).log_index(),
        ),
        // An epoch-reset position is valid for descriptive recovery reissue,
        // but must not enter ordinary same-epoch pending replacement.
        MetadataCommandId::new(
            ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap(),
            id.pg_id(),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
    ];
    if id.log_index().get() > 1 {
        invalid.push(MetadataCommandId::new(
            id.cluster_epoch(),
            id.pg_id(),
            MetadataCommandLogIndex::new(id.log_index().get() - 1).unwrap(),
        ));
    }
    assert!(id.cluster_epoch().get() > 1);
    invalid.push(MetadataCommandId::new(
        ClusterEpoch::new(id.cluster_epoch().get() - 1).unwrap(),
        id.pg_id(),
        next(id).log_index(),
    ));
    for invalid_id in invalid {
        let failure = match holder.into_same_epoch_reissue(invalid_id) {
            Ok(_) => panic!("invalid reissue accepted: {invalid_id:?}"),
            Err(failure) => failure,
        };
        assert!(matches!(failure.error(), ContextError::ReissuePosition));
        holder = failure.into_submission();
        assert_eq!(holder.observation(), before);
        holder.assert_context_for_test(expected, &topology());
    }
    holder
}

fn assert_prepared(
    holder: ContextualSubmission,
    expected: &ContextualCommand,
    successor: &ContextualCommand,
) -> ContextualReissue {
    let before = holder.observation();
    assert_eq!(before.command_id, expected.id());
    assert_eq!(successor.id(), next(expected.id()));
    let pair = holder.into_same_epoch_reissue(successor.id()).unwrap();
    assert_eq!(pair.source(), before);
    assert_eq!(
        pair.successor(),
        ContextSubmissionObservation {
            command_id: successor.id(),
            installation: Observed::NoAcceptanceObserved,
        }
    );
    pair.assert_contexts_for_test(expected, successor, &topology());
    let pair = match pair.into_observed_successor() {
        Ok(_) => panic!("unsent successor discarded its possibly installed source"),
        Err(pair) => *pair,
    };
    assert_eq!(pair.source(), before);
    assert_eq!(
        pair.successor().installation,
        Observed::NoAcceptanceObserved
    );
    pair.assert_contexts_for_test(expected, successor, &topology());
    pair
}

#[test]
fn staging_context_submission_reissue_keeps_source_history_after_expiry_and_reopen() {
    let clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        for observed in [
            Observed::NoAcceptanceObserved,
            Observed::Unconfirmed,
            Observed::Installed,
        ] {
            clock.set(2_000);
            let (dir, pg, owner, command) = candidate::setup(target.clone(), append);
            let captured = candidate::capture(&pg, &owner, command.clone(), append);
            let expected = described_context(&command, &captured.descriptor_bytes(), append);
            let successor = expected_successor(&command, &expected);
            // Every case has an installed slot. A local observation is not a
            // statement of global absence; even fresh history must retain it.
            pending_payload_transaction::install(&pg, &command, Some(&captured)).unwrap();
            let mut holder = captured.into_submission();
            match observed {
                Observed::NoAcceptanceObserved => {}
                Observed::Unconfirmed => interrupt(&mut holder, &pg),
                Observed::Installed => submission::install(&mut holder, &pg).unwrap(),
            }
            reject(&mut holder, &pg);
            assert_eq!(holder.observation(), observed);
            let holder = holder.into_contextual();
            let before = introduction::state(&pg);
            let slot = pending_install::observe(&pg);
            let pages = pending_payload_transaction::pages(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            drop(pg);
            let pg = PgStore::open(dir.path(), owner.subject.object_pg.get()).unwrap();
            let writes = pg.conn.total_changes();
            clock.set(9_000_000);
            let holder = assert_rejected_positions_preserve(holder, &expected);
            let pair = assert_prepared(holder, &expected, &successor);
            assert_eq!(pair.source().installation, observed);
            // Unwinding cannot discard durable custody or release resources.
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    let _pair = pair;
                    panic!("unwind complete-context reissue");
                }))
                .is_err()
            );
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(pending_install::observe(&pg), slot);
            assert_eq!(pending_payload_transaction::pages(&pg), pages);
            assert_eq!(introduction::state(&pg), before);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        }
    }
}

#[test]
fn staging_context_submission_reissue_retains_ordinary_and_original_abandoned_source() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let cleanup = abandonment::cleanup(&pg, &owner, &source);
        let abandoned = ContextualCommand::abandoned_part(
            PartFinalizationSource::capture(&source)
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
        // The source is authenticated and compacted, but these holders still
        // have not observed installation of either proposed cleanup command.
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
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        for (command, context) in [
            (&cleanup, abandoned),
            (&cleanup, ordinary),
            (&reserve, nonterminal),
        ] {
            let expected = ContextualCommand::decode_field(
                command.clone(),
                &context.encode_field().unwrap(),
                &topology(),
            )
            .unwrap();
            let successor = expected_successor(command, &context);
            let holder = assert_rejected_positions_preserve(context.into_submission(), &expected);
            drop(assert_prepared(holder, &expected, &successor));
        }
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        pending_install::assert_empty(&pg);
    }
}

#[test]
fn staging_context_submission_reissue_after_observed_replacement_keeps_original_descriptor() {
    let clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        clock.set(2_000);
        let (_dir, pg, owner, command) = candidate::setup(target, append);
        let captured = candidate::capture(&pg, &owner, command.clone(), append);
        let descriptor = captured.descriptor_bytes();
        let mut source = captured.into_submission();
        submission::install(&mut source, &pg).unwrap();
        let first = MetadataCommandEnvelope::new(next(command.id()), command.payload().clone());
        let mut pair = source.into_same_epoch_reissue(first.id()).unwrap();
        assert!(pair.replace(&pg, 0, &topology()).unwrap());
        let source = match pair.into_contextual().into_observed_successor() {
            Ok(source) => source,
            Err(_) => panic!("observed successor was lost"),
        };
        let expected = described_context(&first, &descriptor, append);
        let successor = expected_successor(&first, &expected);
        assert_eq!(source.observation().installation, Observed::Installed);
        let before = introduction::state(&pg);
        let slot = pending_install::observe(&pg);
        let pages = pending_payload_transaction::pages(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        clock.set(9_000_000);
        let holder = assert_rejected_positions_preserve(source, &expected);
        let pair = assert_prepared(holder, &expected, &successor);
        assert_eq!(pair.source().installation, Observed::Installed);
        drop(pair);
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(pending_install::observe(&pg), slot);
        assert_eq!(pending_payload_transaction::pages(&pg), pages);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    }
}

#[test]
fn staging_context_submission_failed_reissue_drop_has_no_cleanup_effects() {
    let _clock = test_time_override_guard(2_000);
    for (target, append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, append);
        let mut holder = candidate::capture(&pg, &owner, command.clone(), append).into_submission();
        submission::install(&mut holder, &pg).unwrap();
        interrupt(&mut holder, &pg);
        let before = introduction::state(&pg);
        let slot = pending_install::observe(&pg);
        let pages = pending_payload_transaction::pages(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        let writes = pg.conn.total_changes();
        let failure = match holder
            .into_contextual()
            .into_same_epoch_reissue(command.id())
        {
            Ok(_) => panic!("same-position reissue accepted"),
            Err(failure) => failure,
        };
        assert!(matches!(failure.error(), ContextError::ReissuePosition));
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let _failure = failure;
                panic!("drop rejected contextual reissue");
            }))
            .is_err()
        );
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(pending_install::observe(&pg), slot);
        assert_eq!(pending_payload_transaction::pages(&pg), pages);
        assert_eq!(introduction::state(&pg), before);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
    }
}
