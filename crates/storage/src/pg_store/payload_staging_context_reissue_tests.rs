// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Descriptive reissue through the complete owned context, not installation or
//! recovery authorization. Current v13 commands do not authenticate this field.
//! These owner-local fixtures write metadata, never physical shard payloads.

use super::super::abandonment::{
    source::PartFinalizationSource, AbandonmentError, AbandonmentOutcome,
};
use super::super::candidate::{CandidateApplied, CandidateBindingError};
use super::super::command_context::{
    ApplicationError, Applied, ContextError, ContextReissueFailure, ContextualCommand,
};
use super::*;
use crate::clock::{test_time_override_guard, with_time_and_monotonic_override};
use crate::control_plane::MetadataCommandLogHash;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ReserveObjectVersionCommand,
};
use crate::pg_store::PgStore;
use crate::VersionId;

fn positions(id: MetadataCommandId) -> [MetadataCommandId; 3] {
    let next_epoch = ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap();
    [
        MetadataCommandId::new(
            id.cluster_epoch(),
            id.pg_id(),
            MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
        ),
        MetadataCommandId::new(
            next_epoch,
            id.pg_id(),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        MetadataCommandId::new(
            next_epoch,
            id.pg_id(),
            MetadataCommandLogIndex::new(2).unwrap(),
        ),
    ]
}

fn assert_identity(context: &ContextualCommand, original: &MetadataCommandEnvelope, field: &[u8]) {
    context.assert_command_for_test(&MetadataCommandEnvelope::new(
        context.id(),
        original.payload().clone(),
    ));
    assert_eq!(context.encode_field().unwrap(), field);
}

#[test]
fn staging_context_reissue_preserves_every_captured_descriptor_after_expiry() {
    // Pin a consuming API: a borrowed transformation would let callers detach
    // a successor from a context later owned by an unresolved submission.
    let _: fn(
        ContextualCommand,
        MetadataCommandId,
    ) -> Result<ContextualCommand, ContextReissueFailure> = ContextualCommand::into_reissue;
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let captured = candidate::capture(&pg, &owner, command.clone(), is_append);
        let topology = topology();
        let mut context = ContextualCommand::described(captured, &topology).unwrap();
        let field = context.encode_field().unwrap();
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        for id in positions(command.id()) {
            let writes = pg.conn.total_changes();
            context = with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                context.into_reissue(id).unwrap()
            });
            assert_eq!(context.id(), id);
            assert_identity(&context, &command, &field);
            assert_eq!(pg.conn.total_changes(), writes);
            assert_eq!(introduction::state(&pg), before);
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert_eq!(
                context
                    .application(&topology)
                    .unwrap()
                    .apply_in_open_transaction(&pg, 0)
                    .unwrap(),
                Applied::Described(if is_append {
                    CandidateApplied::Append(super::super::append::AppendApplied::Appended)
                } else {
                    CandidateApplied::Introduction(
                        super::super::introduction::IntroductionApplied::Created,
                    )
                })
            );
            let retained = append::load(&pg, &owner);
            assert_eq!(retained.subject, owner.subject);
            assert_eq!(retained.windows.len(), 1 + usize::from(is_append));
            assert_eq!(retained.references.len(), usize::from(is_append));
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            pg.conn.execute_batch("ROLLBACK").unwrap();
            assert_eq!(introduction::state(&pg), before);
        }
    }
}

fn abandoned_context(
    source: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
) -> ContextualCommand {
    let topology = topology();
    let candidate = PartFinalizationSource::capture(source)
        .unwrap()
        .bind_cleanup(cleanup.clone(), &topology)
        .unwrap();
    let context = ContextualCommand::abandoned_part(candidate, &topology).unwrap();
    // Exercise the decoded variant, not only locally captured construction.
    ContextualCommand::decode_field(cleanup.clone(), &context.encode_field().unwrap(), &topology)
        .unwrap()
}

#[test]
fn staging_context_reissue_retains_original_abandonment_source_after_compaction_and_reopen() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let cleanup = abandonment::cleanup(&pg, &owner, &source);
        let mut context = abandoned_context(&source, &cleanup);
        let field = context.encode_field().unwrap();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let id = positions(cleanup.id())[0];
        context = context.into_reissue(id).unwrap();
        assert_eq!(context.id(), id);
        assert_identity(&context, &cleanup, &field);
        assert!(matches!(
            context
                .application(&topology())
                .unwrap()
                .apply_in_open_transaction(&pg, 0),
            Err(ApplicationError::Abandonment(
                AbandonmentError::NotAbandoned
            ))
        ));
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);

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
        // The store is reopened, not a v13 context carrier. Each later cleanup
        // still authenticates the original finalizer against durable evidence.
        for epoch in [
            source.id().cluster_epoch().get() + 1,
            source.id().cluster_epoch().get() + 2,
        ] {
            let epoch = ClusterEpoch::new(epoch).unwrap();
            let digest = pg.metadata_command_replica_state().unwrap().state_digest;
            pg.initialize_metadata_transfer_matching_state(
                0,
                epoch,
                0,
                MetadataCommandLogHash::genesis(),
                digest,
            )
            .unwrap();
            let before = introduction::state(&pg);
            let prefix = pg.metadata_command_replica_state().unwrap();
            let writes = pg.conn.total_changes();
            let id = MetadataCommandId::new(
                epoch,
                PgId::new(pg_id),
                MetadataCommandLogIndex::new(1).unwrap(),
            );
            context = with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                context.into_reissue(id).unwrap()
            });
            assert_eq!(context.id(), id);
            assert_identity(&context, &cleanup, &field);
            assert_eq!(pg.conn.total_changes(), writes);
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            assert_eq!(
                context
                    .application(&topology())
                    .unwrap()
                    .apply_in_open_transaction(&pg, 0)
                    .unwrap(),
                Applied::Abandonment(AbandonmentOutcome::ClosedAndRetained)
            );
            abandonment::assert_closed(&pg, &owner, &source);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            pg.conn.execute_batch("ROLLBACK").unwrap();
            assert_eq!(introduction::state(&pg), before);
        }
    }
}

fn assert_rejected_positions_restore_context(
    pg: &PgStore,
    mut context: ContextualCommand,
    original: &MetadataCommandEnvelope,
) {
    let field = context.encode_field().unwrap();
    let id = context.id();
    assert!(id.cluster_epoch().get() > 1);
    let mut invalid = vec![
        id,
        MetadataCommandId::new(
            ClusterEpoch::new(id.cluster_epoch().get() - 1).unwrap(),
            id.pg_id(),
            MetadataCommandLogIndex::new(id.log_index().get() + 100).unwrap(),
        ),
        MetadataCommandId::new(
            id.cluster_epoch(),
            PgId::new((id.pg_id().get() + 1) % 8),
            MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
        ),
    ];
    if id.log_index().get() > 1 {
        invalid.push(MetadataCommandId::new(
            id.cluster_epoch(),
            id.pg_id(),
            MetadataCommandLogIndex::new(id.log_index().get() - 1).unwrap(),
        ));
    }
    let before = introduction::state(pg);
    let writes = pg.conn.total_changes();
    for invalid_id in invalid {
        let failure = match context.into_reissue(invalid_id) {
            Ok(_) => panic!("accepted invalid reissue position {invalid_id:?}"),
            Err(failure) => failure,
        };
        assert!(matches!(failure.error(), ContextError::ReissuePosition));
        context = failure.into_context();
        assert_eq!(context.id(), id);
        assert_identity(&context, original, &field);
    }
    let successor_id = positions(id)[1];
    context = context.into_reissue(successor_id).unwrap();
    assert_eq!(context.id(), successor_id);
    assert_identity(&context, original, &field);
    // A different topology containing the same object PG must still fail.
    let foreign = PgTopology::new(&[id.pg_id().get()]).unwrap();
    assert!(matches!(
        context.application(&foreign),
        Err(ApplicationError::Candidate(CandidateBindingError::Topology))
    ));
    assert!(context.application(&topology()).is_ok());
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(pg), before);
}

#[test]
fn staging_context_reissue_rejection_returns_complete_original_for_every_variant() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let captured = candidate::capture(&pg, &owner, command.clone(), is_append);
        let context = ContextualCommand::described(captured, &topology()).unwrap();
        assert_rejected_positions_restore_context(&pg, context, &command);
    }
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let cleanup = abandonment::cleanup(&pg, &owner, &source);
        assert_rejected_positions_restore_context(
            &pg,
            abandoned_context(&source, &cleanup),
            &cleanup,
        );
        assert_rejected_positions_restore_context(
            &pg,
            application::ordinary_context(&cleanup, &topology()).unwrap(),
            &cleanup,
        );
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_context_reissue_keeps_ordinary_dispatch_and_exact_terminal_effects() {
    for ordinary_abort in [false, true] {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg;
        let pg = PgStore::open(dir.path(), pg_id.get()).unwrap();
        let command = if ordinary_abort {
            let owner = part::initialize(&pg, 2);
            close::command(&pg, &owner)
        } else {
            pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            MetadataCommandEnvelope::new(
                MetadataCommandId::new(
                    ClusterEpoch::new(5).unwrap(),
                    pg_id,
                    MetadataCommandLogIndex::new(20).unwrap(),
                ),
                MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                    BucketName::new("bucket").unwrap(),
                    ObjectKey::new("key").unwrap(),
                    VersionId::from_u64(7),
                )),
            )
        };
        let before = introduction::state(&pg);
        let prefix = pg.metadata_command_replica_state().unwrap();
        pg.conn.execute_batch("SAVEPOINT expected_effect").unwrap();
        if ordinary_abort {
            super::super::close::apply_in_open_transaction(&pg, &topology(), &command).unwrap();
        } else {
            pg.apply_metadata_command(&command).unwrap();
        }
        let expected = introduction::state(&pg);
        assert_ne!(expected, before);
        pg.conn
            .execute_batch("ROLLBACK TO expected_effect; RELEASE expected_effect")
            .unwrap();
        let mut context = application::ordinary_context(&command, &topology()).unwrap();
        for id in positions(command.id()) {
            context = context.into_reissue(id).unwrap();
            assert_eq!(context.id(), id);
            assert_identity(&context, &command, &[1, 0, 0]);
            pg.conn.execute_batch("SAVEPOINT actual_effect").unwrap();
            assert_eq!(
                context
                    .application(&topology())
                    .unwrap()
                    .apply_in_open_transaction(&pg, 0)
                    .unwrap(),
                Applied::Existing
            );
            assert_eq!(introduction::state(&pg), expected);
            assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
            pg.conn
                .execute_batch("ROLLBACK TO actual_effect; RELEASE actual_effect")
                .unwrap();
            assert_eq!(introduction::state(&pg), before);
        }
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}
