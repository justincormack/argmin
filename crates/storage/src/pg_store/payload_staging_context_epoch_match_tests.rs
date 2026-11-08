// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn future(id: MetadataCommandId) -> MetadataCommandId {
    MetadataCommandId::new(
        ClusterEpoch::new(id.cluster_epoch().get() + 1).unwrap(),
        id.pg_id(),
        MetadataCommandLogIndex::new(1).unwrap(),
    )
}

fn rejects(candidate: &ContextualCommand, source: &ContextualCommand) {
    assert!(matches!(
        candidate.require_epoch_reissue_of(source, &topology()),
        Err(ContextError::EpochReissueContext)
    ));
}

#[test]
fn staging_context_epoch_match_binds_every_descriptor_and_exact_payload() {
    for (target, is_append) in candidate::cases() {
        let owner = fixture(target);
        let command = if is_append {
            append::command(&owner)
        } else {
            introduction::command(&owner)
        };
        let make = |shorten: bool| {
            let bytes = if is_append {
                let mut lookahead = window(&owner.subject, 1, owner.reserved_end);
                lookahead.cutoff_ms -= u64::from(shorten);
                AppendDescriptor::capture(&owner.validate(&topology()).unwrap(), lookahead)
                    .unwrap()
                    .encode()
                    .unwrap()
            } else {
                let mut changed = owner.clone();
                changed.windows[0].cutoff_ms -= u64::from(shorten);
                changed.maximum_cutoff_ms = changed.windows[0].cutoff_ms;
                IntroductionDescriptor::capture(&changed.validate(&topology()).unwrap())
                    .unwrap()
                    .encode()
                    .unwrap()
            };
            ContextualCommand::described(
                DescribedStagingCommand::decode_descriptor_field(
                    command.clone(),
                    &bytes,
                    &topology(),
                )
                .unwrap(),
                &topology(),
            )
            .unwrap()
        };
        let source = make(false);
        let successor = received(&command, &source, future(command.id()));
        successor
            .require_epoch_reissue_of(&source, &topology())
            .unwrap();
        // The new relationship never widens ordinary pending replacement.
        assert_eq!(
            successor.pending_match(&source, &topology()).unwrap(),
            Conflict
        );
        rejects(&source, &source);
        rejects(&source, &successor);
        rejects(&received(&command, &source, next(command.id())), &source);
        let changed = received(&command, &make(true), future(command.id()));
        changed.assert_command_for_test(&MetadataCommandEnvelope::new(
            future(command.id()),
            command.payload().clone(),
        ));
        assert_ne!(
            changed.encode_field().unwrap(),
            source.encode_field().unwrap()
        );
        rejects(&changed, &source);
        let mut payload = command.payload().clone();
        match &mut payload {
            MetadataCommandPayload::ReserveObjectGeneration(reserve) => {
                reserve.created_at_millis += 1
            }
            MetadataCommandPayload::CreateStreamUpload(create) => create.session.created_at += 1,
            MetadataCommandPayload::AppendStreamSegment(append) => {
                append.segment.payload_crc64 ^= 1
            }
            _ => unreachable!(),
        }
        let changed = ContextualCommand::decode_field(
            MetadataCommandEnvelope::new(future(command.id()), payload),
            &source.encode_field().unwrap(),
            &topology(),
        )
        .unwrap();
        assert_eq!(
            changed.encode_field().unwrap(),
            source.encode_field().unwrap()
        );
        rejects(&changed, &source);
    }
}

#[test]
fn staging_context_epoch_match_rejects_mpu_takeover_and_crossed_positions_or_topologies() {
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
    received(&command, &source, future(command.id()))
        .require_epoch_reissue_of(&source, &topology())
        .unwrap();
    let MetadataCommandPayload::CommitMultipartObject(mut changed) = command.payload().clone()
    else {
        unreachable!()
    };
    changed
        .bucket_write_reservation
        .reservation_id
        .push_str("-takeover");
    changed
        .bucket_write_reservation
        .owner_token
        .push_str("-takeover");
    changed.bucket_write_reservation.created_at += 1;
    changed.bucket_write_reservation.lease_deadline += 1;
    let takeover = MetadataCommandPayload::CommitMultipartObject(changed);
    let same_epoch = application::ordinary_context(
        &MetadataCommandEnvelope::new(next(command.id()), takeover.clone()),
        &topology(),
    )
    .unwrap();
    assert_eq!(
        same_epoch.pending_match(&source, &topology()).unwrap(),
        SameEpochReissue,
        "positive ordinary-takeover canary"
    );
    let crossed = application::ordinary_context(
        &MetadataCommandEnvelope::new(future(command.id()), takeover),
        &topology(),
    )
    .unwrap();
    rejects(&crossed, &source);
    assert_eq!(pg.conn.total_changes(), writes);
    assert_eq!(introduction::state(&pg), before);
    pg.conn.execute_batch("ROLLBACK").unwrap();

    let id = fixture(StagingTarget::DirectPut).subject.id.introduction;
    let source = ordinary(id, &topology());
    let same_pg_foreign = PgTopology::new(&[id.pg_id().get()]).unwrap();
    for (candidate_topology, source_topology) in [
        (&same_pg_foreign, &topology()),
        (&topology(), &same_pg_foreign),
    ] {
        assert!(matches!(
            ordinary(future(id), candidate_topology)
                .require_epoch_reissue_of(&ordinary(id, source_topology), &topology()),
            Err(ContextError::Candidate(CandidateBindingError::Topology))
        ));
    }
    for (epoch, pg_id, index) in [
        (
            id.cluster_epoch().get(),
            id.pg_id().get(),
            id.log_index().get() + 1,
        ),
        (id.cluster_epoch().get() - 1, id.pg_id().get(), 99),
        (id.cluster_epoch().get() + 1, (id.pg_id().get() + 1) % 8, 1),
    ] {
        rejects(
            &ordinary(
                MetadataCommandId::new(
                    ClusterEpoch::new(epoch).unwrap(),
                    PgId::new(pg_id),
                    MetadataCommandLogIndex::new(index).unwrap(),
                ),
                &topology(),
            ),
            &source,
        );
    }
    let outside = ordinary(
        MetadataCommandId::new(
            future(id).cluster_epoch(),
            PgId::new(99),
            MetadataCommandLogIndex::new(1).unwrap(),
        ),
        &topology(),
    );
    assert!(matches!(
        outside.require_epoch_reissue_of(&source, &topology()),
        Err(ContextError::CommandPg)
    ));
}

#[test]
fn staging_context_epoch_match_binds_original_finalizer_and_cleanup_mode() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, count);
        let original = part::command(&pg, &owner);
        let cleanup = abandonment::cleanup(&pg, &owner, &original);
        let source = abandoned(&original, &cleanup);
        let successor = received(&cleanup, &source, future(cleanup.id()));
        successor
            .require_epoch_reissue_of(&source, &topology())
            .unwrap();
        let ordinary = application::ordinary_context(&cleanup, &topology()).unwrap();
        rejects(
            &received(&cleanup, &ordinary, future(cleanup.id())),
            &source,
        );
        rejects(&successor, &ordinary);
        for change in 0..4 {
            let MetadataCommandPayload::CommitStreamPart(mut part) = original.payload().clone()
            else {
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
                original.id(),
                MetadataCommandPayload::CommitStreamPart(part),
            );
            let changed = received(
                &cleanup,
                &abandoned(&changed_source, &cleanup),
                future(cleanup.id()),
            );
            rejects(&changed, &source);
        }
        if count > 0 {
            let MetadataCommandPayload::AbortStreamUpload(mut payload) = cleanup.payload().clone()
            else {
                unreachable!()
            };
            payload.staged_segments.clear();
            let changed = abandoned(
                &original,
                &MetadataCommandEnvelope::new(
                    future(cleanup.id()),
                    MetadataCommandPayload::AbortStreamUpload(payload),
                ),
            );
            assert_eq!(
                changed.encode_field().unwrap(),
                source.encode_field().unwrap()
            );
            rejects(&changed, &source);
        }
        pg.conn.execute_batch("ROLLBACK").unwrap();
    }
}
