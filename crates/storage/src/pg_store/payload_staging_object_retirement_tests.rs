// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::super::super::object::require_direct_retirement;
use super::*;
use crate::metadata_command::{
    DeleteObjectPayloadReclaimCommand, DeleteObjectVersionMode, DeleteObjectVersionTarget,
    ObjectPayloadReclaimClaimProof,
};

#[test]
fn staging_direct_retirement_classifies_overwrites_but_keeps_stream_work_gated() {
    use super::super::super::command_context::{ContextualCommand, TerminalSlotWork};
    for target in [StagingTarget::DirectPut, StagingTarget::StreamPut] {
        let dir = test_util::tempdir();
        let pg =
            PgStore::open(dir.path(), fixture(target.clone()).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, target.clone(), 1);
        let original = command(&pg, &owner, VersionId::Null);
        let mut overwrite = payload(&original).clone();
        overwrite.stale_payload = Some(
            crate::metadata_command::ObjectPayloadReclaimCommand::Segments(
                object_overwrite::reclaim(&original),
            ),
        );
        for (envelope, work) in [
            (
                original.clone(),
                if target == StagingTarget::DirectPut {
                    TerminalSlotWork::DirectPutPublication
                } else {
                    TerminalSlotWork::Gated
                },
            ),
            (
                MetadataCommandEnvelope::new(
                    original.id(),
                    MetadataCommandPayload::CommitDirectPutObject(Box::new(overwrite)),
                ),
                if target == StagingTarget::DirectPut {
                    TerminalSlotWork::DirectPutPublication
                } else {
                    TerminalSlotWork::Gated
                },
            ),
        ] {
            let context =
                ContextualCommand::decode_field(envelope, &[1, 0, 0], &topology()).unwrap();
            assert_eq!(context.terminal_slot_work(), work);
        }
    }
}

#[test]
fn staging_direct_retirement_custody_follows_retained_reference_transitions() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::DirectPut).subject.object_pg.get(),
    )
    .unwrap();
    let owner = initialize(&pg, StagingTarget::DirectPut, 1);
    let original = command(&pg, &owner, VersionId::Null);
    apply(&pg, &original).unwrap();
    require_direct_retirement(&pg, &topology(), &original).unwrap();
    pg.conn
        .execute_batch("UPDATE payload_staging_owners SET lifecycle = 3")
        .unwrap();
    require_direct_retirement(&pg, &topology(), &original).unwrap();
    let deletion = delete::command(
        &pg,
        &owner,
        VersionId::Null,
        DeleteObjectVersionMode::Specific,
    );
    delete::apply(&pg, &deletion).unwrap();
    require_direct_retirement(&pg, &topology(), &original).unwrap();
    let MetadataCommandPayload::DeleteObjectVersion(deleted) = deletion.payload() else {
        unreachable!()
    };
    let DeleteObjectVersionTarget::Live {
        generation_id,
        payload,
        ..
    } = &deleted.target
    else {
        unreachable!()
    };
    let completion = MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(9).unwrap(),
            original.id().pg_id(),
            MetadataCommandLogIndex::new(7000).unwrap(),
        ),
        MetadataCommandPayload::DeleteObjectPayloadReclaim(Box::new(
            DeleteObjectPayloadReclaimCommand::new(
                deleted.bucket.clone(),
                deleted.key.clone(),
                *generation_id,
                payload.clone(),
                ObjectPayloadReclaimClaimProof {
                    bucket_incarnation_generation: owner.subject.bucket_incarnation,
                    reclaim_kind: payload.kind(),
                    claim_id: "claim".into(),
                    owner_token: "owner".into(),
                    cluster_epoch: ClusterEpoch::new(9).unwrap(),
                },
            ),
        )),
    );
    super::super::super::reclaim::apply_in_open_transaction(&pg, &topology(), &completion).unwrap();
    let retained = append::load(&pg, &owner);
    assert_eq!(
        retained.lifecycle,
        StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced)
    );
    assert!(retained.references.is_empty());
    let before = introduction::state(&pg);
    let changes = pg.conn.total_changes();
    require_direct_retirement(&pg, &topology(), &original).unwrap();
    assert_eq!(pg.conn.total_changes(), changes);
    assert_eq!(introduction::state(&pg), before);
    pg.conn
        .execute_batch("UPDATE payload_staging_windows SET placement_epoch = placement_epoch + 1")
        .unwrap();
    // Unlike an invalid subject-derived hash, this remains structurally valid.
    // Only binding to the authenticated original manifest can reject it.
    append::load(&pg, &owner).validate(&topology()).unwrap();
    let error = require_direct_retirement(&pg, &topology(), &original).unwrap_err();
    assert!(matches!(error, ObjectError::SelectedPrefix), "{error}");
}

#[test]
fn staging_direct_retirement_custody_rejects_independent_corruption_without_mutation() {
    for mutation in [
        "DELETE FROM payload_staging_owners",
        "DELETE FROM payload_staging_references",
        "DELETE FROM payload_staging_resources",
        "UPDATE payload_staging_resources SET owner_token = owner_token || '-crossed'",
        "UPDATE payload_staging_resources SET state = 1",
        "UPDATE payload_staging_owners SET generation_state = 1",
        "DELETE FROM object_write_counters",
        "UPDATE object_write_counters SET max_committed_generation = 1",
        "UPDATE payload_staging_windows SET segment_hash = zeroblob(16); UPDATE payload_staging_references SET segment_hash = zeroblob(16)",
        "UPDATE payload_staging_windows SET placement_epoch = placement_epoch + 1",
        "UPDATE payload_staging_references SET logical_size = logical_size + 1, stored_size = stored_size + 1",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(dir.path(), fixture(StagingTarget::DirectPut).subject.object_pg.get()).unwrap();
        let owner = initialize(&pg, StagingTarget::DirectPut, 1);
        let command = command(&pg, &owner, VersionId::Null);
        apply(&pg, &command).unwrap();
        require_direct_retirement(&pg, &topology(), &command).unwrap();
        pg.conn.execute_batch(mutation).unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(require_direct_retirement(&pg, &topology(), &command).is_err(), "{mutation}");
        assert_eq!(pg.conn.total_changes(), changes, "{mutation}");
        assert_eq!(introduction::state(&pg), before, "{mutation}");
    }
}

#[test]
fn staging_direct_retirement_custody_preserves_exact_command_binding_after_reopen() {
    for version in [VersionId::Null, VersionId::from_u64(3)] {
        let dir = test_util::tempdir();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = initialize(&pg, StagingTarget::DirectPut, 1);
        let command = command(&pg, &owner, version);
        assert!(require_direct_retirement(&pg, &topology(), &command).is_err());
        apply(&pg, &command).unwrap();
        pg.conn.execute_batch("COMMIT").unwrap();
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        assert!(matches!(
            require_direct_retirement(&pg, &topology(), &command),
            Err(ObjectError::TransactionRequired)
        ));
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        require_direct_retirement(&pg, &topology(), &command).unwrap();
        for case in 0..9 {
            let mut bad = payload(&command).clone();
            match case {
                0 => bad.bucket_write_reservation.operation_kind =
                    crate::metadata_command::PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
                        .into(),
                1 => bad
                    .bucket_write_reservation
                    .owner_token
                    .push_str("-crossed"),
                2 => {
                    bad.object.generation_id =
                        GenerationId::new(bad.object.generation_id.get() + 1).unwrap()
                }
                3 => bad.object.ec.m += 1,
                4 => bad.object.size += 1,
                5 => bad.segments[0].segment_okh[0] ^= 1,
                6 => bad.segments[0].placement_cluster_epoch = ClusterEpoch::new(99).unwrap(),
                7 => bad.segments.clear(),
                8 => bad.bucket_write_reservation.bucket_incarnation_generation += 1,
                _ => unreachable!(),
            }
            let bad = MetadataCommandEnvelope::new(
                command.id(),
                MetadataCommandPayload::CommitDirectPutObject(Box::new(bad)),
            );
            assert!(
                require_direct_retirement(&pg, &topology(), &bad).is_err(),
                "{case}"
            );
        }
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}
