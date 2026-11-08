// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only pending comparison, not authenticated installation or recovery.
//! Metadata-only fixtures; no physical payload or cleanup acceptance claims.

use super::super::abandonment::source::PartFinalizationSource;
use super::super::candidate::{CandidateBindingError, DescribedStagingCommand};
use super::super::command_context::{ContextError, ContextualCommand, PendingContextMatch};
use super::super::descriptor::{AppendDescriptor, IntroductionDescriptor};
use super::*;
use crate::clock::{test_time_override_guard, with_time_and_monotonic_override};
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ReserveObjectVersionCommand,
};
use crate::pg_store::PgStore;
use crate::VersionId;
use PendingContextMatch::{Conflict, ExactRetry, SameEpochReissue};

#[path = "payload_staging_context_epoch_match_tests.rs"]
mod epoch;

fn next(id: MetadataCommandId) -> MetadataCommandId {
    MetadataCommandId::new(
        id.cluster_epoch(),
        id.pg_id(),
        MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
    )
}

fn received(
    original: &MetadataCommandEnvelope,
    context: &ContextualCommand,
    id: MetadataCommandId,
) -> ContextualCommand {
    ContextualCommand::decode_field(
        MetadataCommandEnvelope::new(id, original.payload().clone()),
        &context.encode_field().unwrap(),
        &topology(),
    )
    .unwrap()
}

fn assert_conflict_both(left: &ContextualCommand, right: &ContextualCommand) {
    assert_eq!(left.pending_match(right, &topology()).unwrap(), Conflict);
    assert_eq!(right.pending_match(left, &topology()).unwrap(), Conflict);
}

#[test]
fn staging_context_match_all_descriptors_requires_same_epoch_and_exact_captured_context() {
    let _clock = test_time_override_guard(2_000);
    for (target, is_append) in candidate::cases() {
        let (_dir, pg, owner, command) = candidate::setup(target, is_append);
        let source = ContextualCommand::described(
            candidate::capture(&pg, &owner, command.clone(), is_append),
            &topology(),
        )
        .unwrap();
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        let prefix = pg.metadata_command_replica_state().unwrap();
        let retry = received(&command, &source, command.id());
        assert_eq!(
            retry.pending_match(&source, &topology()).unwrap(),
            ExactRetry
        );
        assert_eq!(
            source.pending_match(&retry, &topology()).unwrap(),
            ExactRetry
        );
        let later = received(&command, &source, next(command.id()));
        assert_eq!(later.id(), next(command.id()));
        assert_eq!(
            with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                later.pending_match(&source, &topology())
            })
            .unwrap(),
            SameEpochReissue
        );
        assert_eq!(source.pending_match(&later, &topology()).unwrap(), Conflict);
        let newer_epoch = MetadataCommandId::new(
            ClusterEpoch::new(command.id().cluster_epoch().get() + 1).unwrap(),
            command.id().pg_id(),
            MetadataCommandLogIndex::new(1).unwrap(),
        );
        let recovery = received(&command, &source, newer_epoch);
        assert_conflict_both(&source, &recovery);
        let mut changed = command.payload().clone();
        match &mut changed {
            MetadataCommandPayload::ReserveObjectGeneration(reserve) => {
                reserve.created_at_millis += 1;
            }
            MetadataCommandPayload::CreateStreamUpload(create) => {
                create.session.created_at += 1;
            }
            MetadataCommandPayload::AppendStreamSegment(append) => {
                append.segment.payload_crc64 ^= 1;
            }
            _ => unreachable!("five introduction/append cases"),
        }
        for id in [command.id(), next(command.id())] {
            let changed = ContextualCommand::decode_field(
                MetadataCommandEnvelope::new(id, changed.clone()),
                &source.encode_field().unwrap(),
                &topology(),
            )
            .unwrap();
            assert_eq!(
                changed.encode_field().unwrap(),
                source.encode_field().unwrap()
            );
            assert_conflict_both(&source, &changed);
        }
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), prefix);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_context_match_rejects_changed_cutoff_with_identical_bare_command() {
    for (target, is_append) in candidate::cases() {
        let owner = fixture(target);
        let command = if is_append {
            append::command(&owner)
        } else {
            introduction::command(&owner)
        };
        let topology = topology();
        let description = |shorten: bool| {
            let bytes = if is_append {
                let mut lookahead = window(&owner.subject, 1, owner.reserved_end);
                lookahead.cutoff_ms -= u64::from(shorten);
                AppendDescriptor::capture(&owner.validate(&topology).unwrap(), lookahead)
                    .unwrap()
                    .encode()
                    .unwrap()
            } else {
                let mut changed = owner.clone();
                changed.windows[0].cutoff_ms -= u64::from(shorten);
                changed.maximum_cutoff_ms = changed.windows[0].cutoff_ms;
                IntroductionDescriptor::capture(&changed.validate(&topology).unwrap())
                    .unwrap()
                    .encode()
                    .unwrap()
            };
            ContextualCommand::described(
                DescribedStagingCommand::decode_descriptor_field(
                    command.clone(),
                    &bytes,
                    &topology,
                )
                .unwrap(),
                &topology,
            )
            .unwrap()
        };
        let source = description(false);
        let changed = description(true);
        source.assert_command_for_test(&command);
        changed.assert_command_for_test(&command);
        assert_ne!(
            source.encode_field().unwrap(),
            changed.encode_field().unwrap()
        );
        assert_conflict_both(&source, &changed);
        let later = received(&command, &changed, next(command.id()));
        assert_eq!(
            later.pending_match(&changed, &topology).unwrap(),
            SameEpochReissue
        );
        assert_conflict_both(&source, &later);
    }
}

fn abandoned(
    source: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
) -> ContextualCommand {
    let candidate = PartFinalizationSource::capture(source)
        .unwrap()
        .bind_cleanup(cleanup.clone(), &topology())
        .unwrap();
    let context = ContextualCommand::abandoned_part(candidate, &topology()).unwrap();
    received(cleanup, &context, cleanup.id())
}

#[test]
fn staging_context_match_never_downgrades_source_bearing_cleanup_to_ordinary_abort() {
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
        let contextual = abandoned(&source, &cleanup);
        let ordinary = application::ordinary_context(&cleanup, &topology()).unwrap();
        contextual.assert_command_for_test(&cleanup);
        ordinary.assert_command_for_test(&cleanup);
        let before = introduction::state(&pg);
        let writes = pg.conn.total_changes();
        for id in [cleanup.id(), next(cleanup.id())] {
            let repeated = received(&cleanup, &contextual, id);
            assert_eq!(
                repeated.pending_match(&contextual, &topology()).unwrap(),
                if id == cleanup.id() {
                    ExactRetry
                } else {
                    SameEpochReissue
                }
            );
            assert_conflict_both(&ordinary, &repeated);
            let ordinary_retry = received(&cleanup, &ordinary, id);
            assert_conflict_both(&contextual, &ordinary_retry);
            if count > 0 {
                let MetadataCommandPayload::AbortStreamUpload(mut changed) =
                    cleanup.payload().clone()
                else {
                    unreachable!()
                };
                assert!(!changed.staged_segments.is_empty());
                changed.staged_segments.clear();
                let changed = abandoned(
                    &source,
                    &MetadataCommandEnvelope::new(
                        id,
                        MetadataCommandPayload::AbortStreamUpload(changed),
                    ),
                );
                assert_eq!(
                    changed.encode_field().unwrap(),
                    contextual.encode_field().unwrap()
                );
                assert_conflict_both(&contextual, &changed);
            }
        }
        assert_eq!(pg.conn.total_changes(), writes);
        assert_eq!(introduction::state(&pg), before);
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}

#[test]
fn staging_context_match_requires_the_exact_original_finalizer_not_only_its_subject() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owner = part::initialize(&pg, 2);
    let source = part::command(&pg, &owner);
    let cleanup = abandonment::cleanup(&pg, &owner, &source);
    let expected = abandoned(&source, &cleanup);
    let before = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    for change in 0..4 {
        let MetadataCommandPayload::CommitStreamPart(mut part) = source.payload().clone() else {
            unreachable!()
        };
        match change {
            0 => part
                .bucket_write_reservation
                .reservation_id
                .push_str("-other"),
            1 => part.bucket_write_reservation.owner_token.push_str("-other"),
            2 => part.bucket_write_reservation.lease_deadline += 1,
            3 => part.part.size += 1,
            _ => unreachable!(),
        }
        let changed_source = MetadataCommandEnvelope::new(
            source.id(),
            MetadataCommandPayload::CommitStreamPart(part),
        );
        let changed = abandoned(&changed_source, &cleanup);
        changed.assert_command_for_test(&cleanup);
        assert_ne!(
            changed.encode_field().unwrap(),
            expected.encode_field().unwrap()
        );
        for id in [cleanup.id(), next(cleanup.id())] {
            let changed = received(&cleanup, &changed, id);
            assert_conflict_both(&expected, &changed);
        }
    }
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
    pg.conn.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn staging_context_match_preserves_only_ordinary_multipart_reservation_takeover() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(multipart_target()).subject.object_pg.get(),
    )
    .unwrap();
    let owners = completion::setup(&pg, 2);
    let command = completion::command(&pg, &owners[0], VersionId::Null);
    let source = application::ordinary_context(&command, &topology()).unwrap();
    let before = introduction::state(&pg);
    let writes = pg.conn.total_changes();
    let MetadataCommandPayload::CommitMultipartObject(mut takeover) = command.payload().clone()
    else {
        unreachable!()
    };
    takeover
        .bucket_write_reservation
        .reservation_id
        .push_str("-takeover");
    takeover
        .bucket_write_reservation
        .owner_token
        .push_str("-takeover");
    takeover.bucket_write_reservation.created_at += 1;
    takeover.bucket_write_reservation.lease_deadline += 1;
    let context = |id, part| {
        application::ordinary_context(
            &MetadataCommandEnvelope::new(id, MetadataCommandPayload::CommitMultipartObject(part)),
            &topology(),
        )
        .unwrap()
    };
    assert_conflict_both(&source, &context(command.id(), takeover.clone()));
    let later = context(next(command.id()), takeover.clone());
    assert_eq!(
        later.pending_match(&source, &topology()).unwrap(),
        SameEpochReissue
    );
    assert_eq!(source.pending_match(&later, &topology()).unwrap(), Conflict);
    for change in 0..4 {
        let mut invalid = takeover.clone();
        match change {
            0 => invalid
                .bucket_write_reservation
                .operation_kind
                .push_str("-other"),
            1 => invalid.bucket_write_reservation.target_context = Some("other-key".into()),
            2 => invalid.parts[0].size += 1,
            3 => invalid.object.size += 1,
            _ => unreachable!(),
        }
        assert_conflict_both(&source, &context(next(command.id()), invalid));
    }
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
    pg.conn.execute_batch("ROLLBACK").unwrap();
}

fn ordinary(id: MetadataCommandId, topology: &PgTopology) -> ContextualCommand {
    application::ordinary_context(
        &MetadataCommandEnvelope::new(
            id,
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                BucketName::new("staging-bucket").unwrap(),
                ObjectKey::new("payload").unwrap(),
                VersionId::from_u64(7),
            )),
        ),
        topology,
    )
    .unwrap()
}

#[test]
fn staging_context_match_checks_both_topologies_and_command_positions() {
    let owner = fixture(StagingTarget::DirectPut);
    let id = owner.subject.id.introduction;
    let topology = topology();
    let foreign = (1..256_u32)
        .find_map(|mask| {
            let ids = (0..8)
                .filter(|id| mask & (1 << id) != 0)
                .collect::<Vec<_>>();
            let candidate = PgTopology::new(&ids).unwrap();
            (candidate.object_pg_for(&owner.subject.bucket, &owner.subject.key) == id.pg_id().get()
                && candidate.bucket_pg_for(&owner.subject.bucket)
                    != topology.bucket_pg_for(&owner.subject.bucket))
            .then_some(candidate)
        })
        .unwrap();
    assert_eq!(
        foreign.object_pg_for(&owner.subject.bucket, &owner.subject.key),
        id.pg_id().get()
    );
    assert_ne!(
        foreign.bucket_pg_for(&owner.subject.bucket),
        topology.bucket_pg_for(&owner.subject.bucket)
    );
    let source = ordinary(id, &topology);
    let crossed = ordinary(id, &foreign);
    for configured in [&topology, &foreign] {
        for (candidate, previous) in [(&source, &crossed), (&crossed, &source)] {
            assert!(matches!(
                candidate.pending_match(previous, configured),
                Err(ContextError::Candidate(CandidateBindingError::Topology))
            ));
        }
    }
    assert_eq!(
        source
            .pending_match(&ordinary(id, &topology), &topology)
            .unwrap(),
        ExactRetry
    );
    for (epoch, pg, index) in [
        (
            id.cluster_epoch().get(),
            id.pg_id(),
            id.log_index().get() - 1,
        ),
        (
            id.cluster_epoch().get(),
            PgId::new((id.pg_id().get() + 1) % 8),
            id.log_index().get() + 1,
        ),
        (
            id.cluster_epoch().get() - 1,
            id.pg_id(),
            id.log_index().get() + 1,
        ),
        (
            id.cluster_epoch().get() + 1,
            id.pg_id(),
            id.log_index().get() + 1,
        ),
    ] {
        let candidate = ordinary(
            MetadataCommandId::new(
                ClusterEpoch::new(epoch).unwrap(),
                pg,
                MetadataCommandLogIndex::new(index).unwrap(),
            ),
            &topology,
        );
        assert_eq!(
            candidate.pending_match(&source, &topology).unwrap(),
            Conflict
        );
    }
    let invalid = ordinary(
        MetadataCommandId::new(id.cluster_epoch(), PgId::new(99), id.log_index()),
        &topology,
    );
    for (candidate, previous) in [
        (&source, &invalid),
        (&invalid, &source),
        (&invalid, &invalid),
    ] {
        assert!(matches!(
            candidate.pending_match(previous, &topology),
            Err(ContextError::CommandPg)
        ));
    }
}
