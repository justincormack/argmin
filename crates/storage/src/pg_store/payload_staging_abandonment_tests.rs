// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only component/certificate tests. The failed source is still a
//! separate required input until coordinated command encoding is integrated.

use super::super::abandonment::{
    source::PartFinalizationSource, AbandonmentError, AbandonmentOutcome,
};
use super::*;
use crate::control_plane::MetadataCommandLogHash;
use crate::metadata_command::{
    CommitStreamPartCommand, MetadataCommandAcceptance, MetadataCommandEnvelope,
    MetadataCommandPayload, ReserveObjectVersionCommand,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::{MetadataError, VersionId};

#[path = "payload_staging_abandonment_terminal_tests.rs"]
mod terminal_owner;

#[path = "payload_staging_abandonment_source_tests.rs"]
mod source_carrier;

#[path = "payload_staging_obligations_tests.rs"]
mod obligations;

fn payload(source: &MetadataCommandEnvelope) -> &CommitStreamPartCommand {
    let MetadataCommandPayload::CommitStreamPart(part) = source.payload() else {
        unreachable!()
    };
    part
}

fn cleanup(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    source: &MetadataCommandEnvelope,
) -> MetadataCommandEnvelope {
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            source.id().cluster_epoch(),
            source.id().pg_id(),
            MetadataCommandLogIndex::new(source.id().log_index().get() + 1).unwrap(),
        ),
        close::command(pg, owner).payload().clone(),
    )
}

fn terminal(pg: &PgStore, source: &MetadataCommandEnvelope, abandoned: bool) {
    // Component setup materializes the initial owner/session state. Anchor that
    // fixture as the starting digest, then use actual linked terminal recording
    // (not raw tombstone insertion) for the source certificate.
    pg.refresh_metadata_command_state_digest().unwrap();
    pg.conn.execute_batch("COMMIT").unwrap();
    for index in 1..source.id().log_index().get() {
        let unrelated = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                source.id().cluster_epoch(),
                source.id().pg_id(),
                MetadataCommandLogIndex::new(index).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                payload(source).bucket.clone(),
                payload(source).key.clone(),
                VersionId::from_u64(100 + index),
            )),
        );
        pg.record_metadata_command_abandoned(0, &unrelated).unwrap();
    }
    let before = pg.metadata_state_digest().unwrap();
    if abandoned {
        pg.record_metadata_command_abandoned(0, source).unwrap();
        assert_eq!(
            pg.metadata_state_digest().unwrap(),
            before,
            "tombstones must not change materialized state"
        );
        assert_eq!(
            pg.metadata_command_replica_state()
                .unwrap()
                .state_digest
                .value(),
            before
        );
    } else {
        pg.apply_metadata_command_and_record(0, source).unwrap();
    }
    pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
}

fn apply(
    pg: &PgStore,
    source: &MetadataCommandEnvelope,
    follow_up: &MetadataCommandEnvelope,
) -> Result<AbandonmentOutcome, AbandonmentError> {
    let decode = |command: &MetadataCommandEnvelope| {
        crate::metadata_command::decode_metadata_command_envelope(
            &command.command_bytes(),
            &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
        )
        .unwrap()
    };
    let source = PartFinalizationSource::capture(source).unwrap();
    let source = PartFinalizationSource::decode(&source.encode()).unwrap();
    source.apply_closure_in_open_transaction(pg, &topology(), 0, &decode(follow_up))
}

fn expected(owner: &StagingOwnerRecord, source: &MetadataCommandEnvelope) -> StagingOwnerRecord {
    let mut result = owner.clone();
    result.lifecycle = StagingLifecycle::Retiring(StagingRetirement::Unpublished);
    result.references.clear();
    release_due(&mut result.resources.generation);
    release_due(&mut result.resources.creation);
    result.resources.finalization =
        ResourceResponsibility::ReleaseDue(StagingBucketResource::capture(
            &payload(source).bucket_write_reservation,
            PgId::new(topology().bucket_pg_for(&owner.subject.bucket)),
        ));
    result
}

fn assert_closed(pg: &PgStore, owner: &StagingOwnerRecord, source: &MetadataCommandEnvelope) {
    assert_eq!(append::load(pg, owner), expected(owner, source));
    assert!(matches!(
        pg.get_stream_upload(&owner.subject.id.attempt),
        Err(MetadataError::StreamSessionNotFound { .. })
    ));
    assert!(pg
        .list_stream_segments(&owner.subject.id.attempt)
        .unwrap()
        .is_empty());
    assert!(matches!(
        pg.get_object_generation_reservation(
            &owner.subject.bucket,
            &owner.subject.key,
            &owner.subject.id.attempt
        ),
        Err(MetadataError::ObjectGenerationReservationNotFound { .. })
    ));
    let routes = pg
        .cluster_map_history_route_references(&topology())
        .unwrap();
    for resource in [
        &expected(owner, source).resources.creation,
        &expected(owner, source).resources.finalization,
    ] {
        let ResourceResponsibility::ReleaseDue(r) = resource else {
            unreachable!()
        };
        assert!(routes.iter().any(|route| route.pg_id() == r.release_pg
            && route.cluster_epoch() == r.identity.cluster_epoch));
    }
}

#[test]
fn staging_abandonment_part_follow_up_retains_both_proofs_across_restart() {
    for count in [0, 2] {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = part::initialize(&pg, count);
        let source = part::command(&pg, &owner);
        let follow_up = cleanup(&pg, &owner, &source);
        terminal(&pg, &source, true);
        assert_eq!(
            append::load(&pg, &owner),
            owner,
            "abandonment alone retains the active owner"
        );
        pg.conn.execute_batch("COMMIT").unwrap();
        if count != 0 {
            pg.record_current_metadata_command_checkpoint(0, source.id().cluster_epoch())
                .unwrap();
            pg.compact_metadata_command_log(source.id().cluster_epoch())
                .unwrap();
            assert_eq!(
                pg.metadata_command_log_stats(source.id().cluster_epoch())
                    .unwrap()
                    .retained_entries,
                0,
                "the source must be authenticated through its terminal receipt"
            );
        }
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        assert_ne!(
            pg.stream_allocation_issuer(owner.subject.issuer.node_id)
                .unwrap(),
            owner.subject.issuer
        );
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        crate::clock::with_time_and_monotonic_override(9_000_000, 90_000_000, || {
            apply(&pg, &source, &follow_up)
        })
        .unwrap();
        assert_closed(&pg, &owner, &source);
        pg.record_metadata_command_applied_inner(0, &follow_up)
            .unwrap();
        pg.conn.execute_batch("COMMIT").unwrap();
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert_closed(&pg, &owner, &source);
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        if count == 0 {
            assert_eq!(
                apply(&pg, &source, &follow_up).unwrap(),
                AbandonmentOutcome::AlreadyRetained
            );
        } else {
            assert!(
                apply(&pg, &source, &follow_up).is_err(),
                "terminal proof custody cannot authorize an old payload cleanup"
            );
        }
        assert_eq!(
            pg.metadata_command_acceptance(0, &follow_up).unwrap(),
            MetadataCommandAcceptance::AlreadyApplied,
            "command replay is certified by acceptance, not terminal owner state"
        );
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_abandonment_historical_source_closes_at_new_epoch_first_position() {
    for (count, compact, source_index) in
        [(0, false, 1), (0, true, 11), (2, false, 11), (2, true, 1)]
    {
        let dir = test_util::tempdir();
        let pg_id = fixture(multipart_target()).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let owner = part::initialize(&pg, count);
        let original = part::command(&pg, &owner);
        let source = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                original.id().cluster_epoch(),
                original.id().pg_id(),
                MetadataCommandLogIndex::new(source_index).unwrap(),
            ),
            original.payload().clone(),
        );
        let old_cleanup = cleanup(&pg, &owner, &source);
        terminal(&pg, &source, true);
        pg.conn.execute_batch("COMMIT").unwrap();
        if compact {
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
        }
        let epoch = ClusterEpoch::new(source.id().cluster_epoch().get() + 1).unwrap();
        let digest = pg.metadata_command_replica_state().unwrap().state_digest;
        // Use the production matching-state transfer boundary to advance the
        // replica epoch, reset its log position and replace allocation lifetime.
        let state = pg
            .initialize_metadata_transfer_matching_state(
                0,
                epoch,
                0,
                MetadataCommandLogHash::genesis(),
                digest,
            )
            .unwrap();
        assert_eq!(state.cluster_epoch, epoch);
        assert_eq!(state.applied_log_index, 0);
        assert_eq!(state.state_digest, digest);
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        assert_ne!(
            pg.stream_allocation_issuer(owner.subject.issuer.node_id)
                .unwrap(),
            owner.subject.issuer
        );
        assert!(matches!(
            pg.metadata_command_acceptance(0, &old_cleanup),
            Err(crate::StoreError::StaleMetadataCommand { command_epoch, current_epoch, .. })
                if command_epoch == source.id().cluster_epoch() && current_epoch == epoch
        ));
        let follow_up = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                epoch,
                source.id().pg_id(),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            old_cleanup.payload().clone(),
        );
        assert!(follow_up.id().log_index() <= owner.subject.id.introduction.log_index());
        assert!(follow_up.id().log_index() <= source.id().log_index());
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert_eq!(
            pg.metadata_command_acceptance(0, &follow_up).unwrap(),
            MetadataCommandAcceptance::Apply
        );
        assert_eq!(append::load(&pg, &owner), owner);

        // A newer cleanup epoch does not excuse absent or substituted source
        // evidence; both fail before closing the owner or retaining a proof.
        let missing = MetadataCommandEnvelope::new(old_cleanup.id(), source.payload().clone());
        let mut changed = payload(&source).clone();
        changed.bucket_write_reservation.owner_token.push('x');
        let substituted = MetadataCommandEnvelope::new(
            source.id(),
            MetadataCommandPayload::CommitStreamPart(Box::new(changed)),
        );
        for bad in [missing, substituted] {
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(apply(&pg, &bad, &follow_up).is_err());
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
        apply(&pg, &source, &follow_up).unwrap();
        assert_closed(&pg, &owner, &source);
        pg.record_metadata_command_applied_inner(0, &follow_up)
            .unwrap();
        pg.conn.execute_batch("COMMIT").unwrap();
        let state = pg.metadata_command_replica_state().unwrap();
        assert_eq!(state.cluster_epoch, epoch);
        assert_eq!(state.applied_log_index, 1);
        assert_eq!(
            state.state_digest.value(),
            pg.metadata_state_digest().unwrap()
        );
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert_closed(&pg, &owner, &source);
        assert_eq!(pg.metadata_command_replica_state().unwrap(), state);
        assert_eq!(
            pg.metadata_command_acceptance(0, &follow_up).unwrap(),
            MetadataCommandAcceptance::AlreadyApplied
        );
    }
}

#[test]
fn staging_abandonment_requires_linked_exact_source_not_absence_or_started_publication() {
    for case in [
        "absent",
        "unlinked",
        "applied",
        "started",
        "substituted",
        "transaction",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, 1);
        let mut source = part::command(&pg, &owner);
        let follow_up = cleanup(&pg, &owner, &source);
        match case {
            "absent" => (),
            "unlinked" => pg
                .test_insert_abandoned_metadata_command_log_entry(&source)
                .unwrap(),
            "applied" => {
                part::apply(&pg, &source).unwrap();
                terminal(&pg, &source, false);
            }
            "started" => {
                terminal(&pg, &source, true);
                // Deliberately contradictory durable state must fail closed,
                // even with a linked tombstone. Do not claim this is a legal
                // publication/abandonment schedule.
                pg.test_replace_pending_metadata_command_slot(&source, None)
                    .unwrap();
                pg.test_mark_pending_metadata_command_publication_started(&source)
                    .unwrap();
            }
            "substituted" => {
                terminal(&pg, &source, true);
                let mut changed = payload(&source).clone();
                changed.bucket_write_reservation.owner_token.push('x');
                source = MetadataCommandEnvelope::new(
                    source.id(),
                    MetadataCommandPayload::CommitStreamPart(Box::new(changed)),
                );
            }
            "transaction" => {
                terminal(&pg, &source, true);
                pg.conn.execute_batch("COMMIT").unwrap();
            }
            _ => unreachable!(),
        }
        let changes = pg.conn.total_changes();
        let before = introduction::state(&pg);
        assert!(apply(&pg, &source, &follow_up).is_err(), "{case}");
        assert_eq!(pg.conn.total_changes(), changes, "{case}");
        assert_eq!(introduction::state(&pg), before, "{case}");
    }
}

#[test]
fn staging_abandonment_linked_source_still_requires_exact_owner_and_proof_subject() {
    for field in [
        "bucket",
        "key",
        "session",
        "upload-id",
        "upload-generation",
        "encryption",
        "part-generation",
        "part-number",
        "proof-operation",
        "proof-target",
        "proof-incarnation",
        "proof-epoch",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, 2);
        let source = part::command(&pg, &owner);
        let follow_up = cleanup(&pg, &owner, &source);
        let mut bad = payload(&source).clone();
        match field {
            "bucket" => bad.bucket = crate::tests::bucket_name("crossed"),
            "key" => bad.key = crate::tests::object_key("crossed"),
            "session" => bad.session_id = crate::tests::stream_session_id("crossed"),
            "upload-id" => {
                bad.upload.upload_id = crate::tests::multipart_upload_id("crossed");
                bad.part.upload_id = bad.upload.upload_id.clone();
                for segment in &mut bad.segments {
                    segment.upload_id = bad.upload.upload_id.clone();
                }
            }
            "upload-generation" => {
                bad.upload.object_generation_id = GenerationId::new(100).unwrap()
            }
            "encryption" => {
                // Same codec, mode, instance and nonce; cross only key material.
                bad.upload.encryption =
                    crate::ObjectEncryption::SseS3(crate::SseS3ObjectState::new(
                        2,
                        [2; crate::SSE_S3_WRAP_NONCE_LEN],
                        [3; crate::SSE_S3_WRAPPED_DEK_LEN],
                        [4; crate::SSE_S3_SEGMENT_NONCE_PREFIX_LEN],
                    ));
            }
            "part-generation" => bad.part.part_vid = GenerationId::new(100).unwrap(),
            "part-number" => {
                bad.part.part_number += 1;
                for segment in &mut bad.segments {
                    segment.part_number += 1;
                }
            }
            "proof-operation" => {
                bad.bucket_write_reservation.operation_kind =
                    PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND.into()
            }
            "proof-target" => bad.bucket_write_reservation.target_context = Some("crossed".into()),
            "proof-incarnation" => bad.bucket_write_reservation.bucket_incarnation_generation += 1,
            "proof-epoch" => {
                bad.bucket_write_reservation.cluster_epoch = ClusterEpoch::new(7).unwrap()
            }
            _ => unreachable!(),
        }
        let bad = MetadataCommandEnvelope::new(
            source.id(),
            MetadataCommandPayload::CommitStreamPart(Box::new(bad)),
        );
        terminal(&pg, &bad, true);
        let changes = pg.conn.total_changes();
        let before = introduction::state(&pg);
        assert!(apply(&pg, &bad, &follow_up).is_err(), "{field}");
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_abandonment_follow_up_checks_command_order_and_actual_staged_prefix() {
    for field in ["index", "epoch", "pg", "session", "prefix", "vid"] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, 2);
        let source = part::command(&pg, &owner);
        let follow_up = cleanup(&pg, &owner, &source);
        terminal(&pg, &source, true);
        let mut id = follow_up.id();
        let MetadataCommandPayload::AbortStreamUpload(mut bad) = follow_up.payload().clone() else {
            unreachable!()
        };
        match field {
            "index" => id = source.id(),
            "epoch" => {
                id = MetadataCommandId::new(
                    ClusterEpoch::new(source.id().cluster_epoch().get() - 1).unwrap(),
                    id.pg_id(),
                    id.log_index(),
                )
            }
            "pg" => {
                id = MetadataCommandId::new(
                    id.cluster_epoch(),
                    PgId::new((pg.pg_id + 1) % 8),
                    id.log_index(),
                )
            }
            "session" => bad.session_id = crate::tests::stream_session_id("crossed"),
            "prefix" => {
                bad.staged_segments.pop();
            }
            "vid" => bad.staged_segments[0].segment_vid = GenerationId::new(100).unwrap(),
            _ => unreachable!(),
        }
        let bad = MetadataCommandEnvelope::new(id, MetadataCommandPayload::AbortStreamUpload(bad));
        let changes = pg.conn.total_changes();
        let before = introduction::state(&pg);
        assert!(apply(&pg, &source, &bad).is_err(), "{field}");
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        apply(&pg, &source, &follow_up).unwrap();
        assert_closed(&pg, &owner, &source);
    }
}

#[test]
fn staging_abandonment_closure_preserves_committed_part_including_later_replacement() {
    for replace_part in [false, true] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let old = part::initialize(&pg, 1);
        let committed = part::command(&pg, &old);
        part::apply(&pg, &committed).unwrap();
        let old = append::load(&pg, &old);
        let owner = part::next_attempt(&pg, &old, 2);
        let source = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::new(6).unwrap(),
                owner.subject.object_pg,
                MetadataCommandLogIndex::new(100).unwrap(),
            ),
            part::command(&pg, &owner).payload().clone(),
        );
        let follow_up = cleanup(&pg, &owner, &source);
        let mut retained = vec![old.clone()];
        if replace_part {
            let replacement = completion::new_attempt(&pg, &owner, 1, 3, 1);
            part::apply(&pg, &part::command(&pg, &replacement)).unwrap();
            retained.push(replacement);
        }
        let retained = retained
            .iter()
            .map(|old| append::load(&pg, old))
            .collect::<Vec<_>>();
        terminal(&pg, &source, true);
        let before_parts = pg
            .get_multipart_part_segments_for_upload_part(
                &owner.subject.bucket,
                &owner.subject.key,
                &payload(&source).upload.upload_id,
                payload(&source).part.part_number,
            )
            .unwrap();
        let before_part = pg
            .get_multipart_part(
                &payload(&source).upload.upload_id,
                payload(&source).part.part_number,
            )
            .unwrap();
        assert_eq!(
            before_part != payload(&source).existing_part.clone().unwrap(),
            replace_part,
            "the second case must invalidate the source's captured part"
        );
        apply(&pg, &source, &follow_up).unwrap();
        assert_closed(&pg, &owner, &source);
        for retained in retained {
            assert_eq!(append::load(&pg, &retained), retained);
        }
        assert_eq!(
            pg.get_multipart_part_segments_for_upload_part(
                &owner.subject.bucket,
                &owner.subject.key,
                &payload(&source).upload.upload_id,
                payload(&source).part.part_number
            )
            .unwrap(),
            before_parts
        );
        assert_eq!(
            pg.get_multipart_part(
                &payload(&source).upload.upload_id,
                payload(&source).part.part_number
            )
            .unwrap(),
            before_part
        );
    }
}

#[test]
fn staging_abandonment_finalization_retention_failure_rolls_back_session_closure() {
    for event in [
        "BEFORE DELETE ON stream_uploads",
        "BEFORE DELETE ON object_generation_reservations",
        "BEFORE UPDATE OF lifecycle ON payload_staging_owners",
        "BEFORE INSERT ON payload_staging_resources WHEN NEW.role = 1",
    ] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(multipart_target()).subject.object_pg.get(),
        )
        .unwrap();
        let owner = part::initialize(&pg, 2);
        let source = part::command(&pg, &owner);
        let follow_up = cleanup(&pg, &owner, &source);
        terminal(&pg, &source, true);
        pg.conn
            .execute_batch(
                "CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42)",
            )
            .unwrap();
        pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_abandonment {event} BEGIN SELECT RAISE(ABORT, 'abandonment closure failure'); END")).unwrap();
        let before = introduction::state(&pg);
        let error = apply(&pg, &source, &follow_up).unwrap_err();
        assert!(
            error.to_string().contains("abandonment closure failure"),
            "{event}: {error}"
        );
        assert_eq!(introduction::state(&pg), before);
        assert!(!pg.conn.is_autocommit());
        assert_eq!(
            pg.conn
                .query_row("SELECT value FROM outer_work", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            42
        );
        pg.conn
            .execute_batch("DROP TRIGGER fail_abandonment")
            .unwrap();
        apply(&pg, &source, &follow_up).unwrap();
        assert_closed(&pg, &owner, &source);
    }
}
