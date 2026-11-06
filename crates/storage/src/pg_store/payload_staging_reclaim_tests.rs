// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only completion of existing reclaim roots. These tests do not
//! manufacture physical-deletion receipts or claim worker/exclusion coverage.

use super::super::reclaim::{apply_in_open_transaction, ReclaimError};
use super::*;
use crate::metadata_command::{
    DeleteObjectPayloadReclaimCommand, DeleteObjectVersionMode, DeleteObjectVersionTarget,
    MetadataCommandEnvelope, MetadataCommandPayload, ObjectPayloadReclaimClaimProof,
    ObjectPayloadReclaimCommand,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::pg_store::PgStore;
use crate::types::AdmittedRouteEffectFence;
use crate::VersionId;

fn payload(envelope: &MetadataCommandEnvelope) -> &DeleteObjectPayloadReclaimCommand {
    let MetadataCommandPayload::DeleteObjectPayloadReclaim(command) = envelope.payload() else {
        unreachable!()
    };
    command
}

fn completion_from_delete(envelope: &MetadataCommandEnvelope) -> MetadataCommandEnvelope {
    let MetadataCommandPayload::DeleteObjectVersion(delete) = envelope.payload() else {
        unreachable!()
    };
    let DeleteObjectVersionTarget::Live {
        generation_id,
        payload,
        ..
    } = &delete.target
    else {
        unreachable!()
    };
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(9).unwrap(),
            envelope.id().pg_id(),
            MetadataCommandLogIndex::new(7000).unwrap(),
        ),
        MetadataCommandPayload::DeleteObjectPayloadReclaim(Box::new(
            DeleteObjectPayloadReclaimCommand::new(
                delete.bucket.clone(),
                delete.key.clone(),
                *generation_id,
                payload.clone(),
                ObjectPayloadReclaimClaimProof {
                    bucket_incarnation_generation: delete
                        .bucket_write_reservation
                        .bucket_incarnation_generation,
                    reclaim_kind: payload.kind(),
                    claim_id: "reclaim-claim".into(),
                    owner_token: "reclaim-owner".into(),
                    cluster_epoch: ClusterEpoch::new(9).unwrap(),
                },
            ),
        )),
    )
}

fn setup(
    pg: &PgStore,
    kind: u8,
    version: VersionId,
) -> (Vec<StagingOwnerRecord>, MetadataCommandEnvelope) {
    let owners = delete::source(pg, kind, version);
    let delete = delete::command(pg, &owners[0], version, DeleteObjectVersionMode::Specific);
    let command = completion_from_delete(&delete);
    delete::apply(pg, &delete).unwrap();
    (
        owners.iter().map(|owner| append::load(pg, owner)).collect(),
        command,
    )
}

fn claim(pg: &PgStore, envelope: &MetadataCommandEnvelope) {
    let c = payload(envelope);
    let proof = &c.reclaim_claim;
    let claimed = pg
        .acquire_object_payload_reclaim_claim(
            &c.bucket,
            proof.bucket_incarnation_generation,
            &c.key,
            c.generation_id,
            proof.reclaim_kind,
            &proof.claim_id,
            &proof.owner_token,
            proof.cluster_epoch,
            AdmittedRouteEffectFence::unbounded(proof.cluster_epoch),
            5_000,
            Some(6_000),
            5_000,
        )
        .unwrap()
        .unwrap();
    assert_eq!(ObjectPayloadReclaimClaimProof::from(&claimed), *proof);
}

fn apply(pg: &PgStore, envelope: &MetadataCommandEnvelope) -> Result<(), ReclaimError> {
    let decoded = crate::metadata_command::decode_metadata_command_envelope(
        &envelope.command_bytes(),
        &crate::node_runtime::MetadataCommandDecodeAuthority::new_for_test(),
    )
    .unwrap();
    apply_in_open_transaction(pg, &topology(), &decoded)
}

fn root(
    pg: &PgStore,
    command: &DeleteObjectPayloadReclaimCommand,
) -> Option<ObjectPayloadReclaimCommand> {
    match &command.payload {
        ObjectPayloadReclaimCommand::Segments(_) => pg
            .get_object_segments_reclaim(&command.bucket, &command.key, command.generation_id)
            .unwrap()
            .map(ObjectPayloadReclaimCommand::Segments),
        ObjectPayloadReclaimCommand::Multipart(_) => pg
            .get_multipart_reclaim(&command.bucket, &command.key, command.generation_id)
            .unwrap()
            .map(ObjectPayloadReclaimCommand::Multipart),
    }
}

fn store_root(pg: &PgStore, root: &ObjectPayloadReclaimCommand) {
    assert!(!pg.conn.is_autocommit());
    // Owner-local corruption fixture: preserve the caller's transaction and
    // replace the complete child set, including malformed physical identities.
    match root {
        ObjectPayloadReclaimCommand::Segments(root) => {
            pg.conn.execute("DELETE FROM object_segments_reclaims WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                rusqlite::params![root.bucket, root.key, root.generation_id.get()]).unwrap();
            pg.conn.execute("INSERT INTO object_segments_reclaims (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![root.bucket, root.key, root.generation_id.get(), root.created_at]).unwrap();
            for s in &root.segments {
                pg.conn.execute("INSERT INTO object_segment_reclaim_segments (bucket, key, generation_id, segment_index, segment_okh, segment_vid, data_pg_id, ec_k, ec_m) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    rusqlite::params![root.bucket, root.key, root.generation_id.get(), s.segment_index, s.segment_okh.as_slice(), s.segment_vid.get(), s.data_pg_id, s.ec.k, s.ec.m]).unwrap();
            }
        }
        ObjectPayloadReclaimCommand::Multipart(root) => {
            pg.conn.execute("DELETE FROM multipart_reclaims WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                rusqlite::params![root.bucket, root.key, root.generation_id.get()]).unwrap();
            pg.conn.execute("INSERT INTO multipart_reclaims (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![root.bucket, root.key, root.generation_id.get(), root.created_at]).unwrap();
            for part in &root.parts {
                pg.conn.execute("INSERT INTO multipart_reclaim_parts (bucket, key, generation_id, part_number) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![root.bucket, root.key, root.generation_id.get(), part.part_number]).unwrap();
                for s in &part.segments {
                    pg.conn.execute("INSERT INTO multipart_reclaim_part_segments (bucket, key, generation_id, part_number, segment_index, segment_okh, segment_vid, data_pg_id, ec_k, ec_m) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                        rusqlite::params![root.bucket, root.key, root.generation_id.get(), s.part_number, s.segment_index, s.segment_okh.as_slice(), s.segment_vid.get(), s.data_pg_id, s.ec.k, s.ec.m]).unwrap();
                }
            }
        }
    }
}

fn assert_completed(pg: &PgStore, envelope: &MetadataCommandEnvelope, old: &[StagingOwnerRecord]) {
    assert!(root(pg, payload(envelope)).is_none());
    assert!(pg.object_payload_reclaim_claim().unwrap().is_none());
    let routes = pg
        .cluster_map_history_route_references(&topology())
        .unwrap();
    for before in old {
        let mut expected = before.clone();
        if before.lifecycle == StagingLifecycle::Retiring(StagingRetirement::PublishedRetained) {
            expected.references.clear();
            expected.lifecycle =
                StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced);
        }
        assert_eq!(append::load(pg, before), expected);
        // Original file/ack and release routes survive even when no reference
        // rows, live version, reclaim root or worker claim remains.
        let mut required = vec![(before.subject.object_pg, before.subject.originating_epoch)];
        required.extend(
            before
                .windows
                .iter()
                .map(|w| (w.participants.data_pg, w.participants.placement_epoch)),
        );
        for resource in [&before.resources.creation, &before.resources.finalization] {
            if let ResourceResponsibility::Live(r) | ResourceResponsibility::ReleaseDue(r) =
                resource
            {
                required.push((r.release_pg, r.identity.cluster_epoch));
            }
        }
        for (pg_id, epoch) in required {
            assert!(routes
                .iter()
                .any(|r| r.pg_id() == pg_id && r.cluster_epoch() == epoch));
        }
    }
}

#[test]
fn staging_epoch_reset_reclaim_preserves_original_claim_and_retained_routes() {
    for kind in 0..5 {
        for primary_claim in [false, true] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(StagingTarget::DirectPut).subject.object_pg.get(),
            )
            .unwrap();
            let (owners, original) = setup(&pg, kind, VersionId::Null);
            if primary_claim {
                claim(&pg, &original);
            }
            let command =
                epoch_order::at_epoch_start(&pg, &original, ClusterEpoch::new(10).unwrap());
            assert!(payload(&command).reclaim_claim.cluster_epoch < command.id().cluster_epoch());
            apply(&pg, &command).unwrap();
            assert_completed(&pg, &command, &owners);
            let pg = epoch_order::record_and_reopen(pg, dir.path(), &command);
            assert_completed(&pg, &command, &owners);
        }
    }
}

#[test]
fn staging_reclaim_completion_retains_exact_obligations_and_routes_through_restart() {
    for kind in 0..5 {
        for version in [VersionId::Null, VersionId::from_u64(3)] {
            for primary_claim in [false, true] {
                let dir = test_util::tempdir();
                let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg.get();
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                let (owners, command) = setup(&pg, kind, version);
                if primary_claim {
                    claim(&pg, &command);
                }
                pg.conn.execute_batch("COMMIT").unwrap();
                drop(pg);
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                assert_ne!(
                    pg.stream_allocation_issuer(owners[0].subject.issuer.node_id)
                        .unwrap(),
                    owners[0].subject.issuer
                );
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                // Accepted command application is clock-independent, including
                // on a replica with no primary-local claim and on reissue.
                let reissued = MetadataCommandEnvelope::new(
                    MetadataCommandId::new(
                        ClusterEpoch::new(10).unwrap(),
                        command.id().pg_id(),
                        MetadataCommandLogIndex::new(7001).unwrap(),
                    ),
                    command.payload().clone(),
                );
                crate::clock::with_time_and_monotonic_override(9_000_000, 90_000_000, || {
                    apply(&pg, &reissued)
                })
                .unwrap();
                assert_completed(&pg, &command, &owners);
                let before = introduction::state(&pg);
                let changes = pg.conn.total_changes();
                assert!(matches!(apply(&pg, &command), Err(ReclaimError::Target)));
                assert_eq!(pg.conn.total_changes(), changes);
                assert_eq!(introduction::state(&pg), before);
                pg.conn.execute_batch("COMMIT").unwrap();
                drop(pg);
                let pg = PgStore::open(dir.path(), pg_id).unwrap();
                pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
                assert_completed(&pg, &command, &owners);
            }
        }
    }
}

#[test]
fn staging_reclaim_completion_crossed_command_and_claim_reject_before_mutation() {
    for kind in [0, 2, 4] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let (owners, good) = setup(&pg, kind, VersionId::Null);
        claim(&pg, &good);
        for field in [
            "bucket",
            "key",
            "generation",
            "incarnation",
            "claim-id",
            "claim-owner",
            "claim-epoch",
            "kind",
            "pg",
            "payload",
            "created-at",
        ] {
            let mut bad = payload(&good).clone();
            let mut id = good.id();
            match field {
                "bucket" => bad.bucket = crate::tests::bucket_name("crossed"),
                "key" => bad.key = crate::tests::object_key("crossed"),
                "generation" => bad.generation_id = GenerationId::new(100).unwrap(),
                "incarnation" => bad.reclaim_claim.bucket_incarnation_generation += 1,
                "claim-id" => bad.reclaim_claim.claim_id.push('x'),
                "claim-owner" => bad.reclaim_claim.owner_token.push('x'),
                "claim-epoch" => bad.reclaim_claim.cluster_epoch = ClusterEpoch::new(8).unwrap(),
                "kind" => {
                    bad.reclaim_claim.reclaim_kind = match bad.reclaim_claim.reclaim_kind {
                        crate::ObjectPayloadReclaimKind::ObjectSegments => {
                            crate::ObjectPayloadReclaimKind::Multipart
                        }
                        crate::ObjectPayloadReclaimKind::Multipart => {
                            crate::ObjectPayloadReclaimKind::ObjectSegments
                        }
                    }
                }
                "pg" => {
                    id = MetadataCommandId::new(
                        id.cluster_epoch(),
                        PgId::new((pg.pg_id + 1) % 8),
                        id.log_index(),
                    )
                }
                "payload" => match &mut bad.payload {
                    ObjectPayloadReclaimCommand::Segments(r) => r.segments.clear(),
                    ObjectPayloadReclaimCommand::Multipart(r) => r.parts.clear(),
                },
                "created-at" => match &mut bad.payload {
                    ObjectPayloadReclaimCommand::Segments(r) => r.created_at += 1,
                    ObjectPayloadReclaimCommand::Multipart(r) => r.created_at += 1,
                },
                _ => unreachable!(),
            }
            let bad = MetadataCommandEnvelope::new(
                id,
                MetadataCommandPayload::DeleteObjectPayloadReclaim(Box::new(bad)),
            );
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(apply(&pg, &bad).is_err(), "{field}");
            assert_eq!(pg.conn.total_changes(), changes, "{field}");
            assert_eq!(introduction::state(&pg), before, "{field}");
        }
        apply(&pg, &good).unwrap();
        assert_completed(&pg, &good, &owners);
    }
}

#[test]
fn staging_reclaim_completion_rebuilt_root_must_match_independent_owners() {
    for kind in [0, 2, 3, 4] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let (owners, good) = setup(&pg, kind, VersionId::Null);
        for field in [
            "omit",
            "duplicate",
            "hash",
            "vid",
            "data-pg",
            "ec",
            "index",
            "part",
        ] {
            if kind == 3 && !matches!(field, "omit" | "duplicate" | "part") {
                continue;
            }
            let mut bad = payload(&good).clone();
            match &mut bad.payload {
                ObjectPayloadReclaimCommand::Segments(r) => {
                    let s = &mut r.segments[0];
                    match field {
                        "omit" => {
                            r.segments.pop();
                        }
                        "duplicate" => {
                            let mut duplicate = s.clone();
                            duplicate.segment_index = r.segments.last().unwrap().segment_index + 1;
                            r.segments.push(duplicate);
                        }
                        "hash" => s.segment_okh[0] ^= 1,
                        "vid" => {
                            s.segment_vid = GenerationId::new(s.segment_vid.get() + 1).unwrap()
                        }
                        "data-pg" => s.data_pg_id = (s.data_pg_id + 1) % 8,
                        "ec" => s.ec = EcShape { k: 1, m: 0 },
                        "index" => s.segment_index += 10,
                        "part" => continue,
                        _ => unreachable!(),
                    }
                }
                ObjectPayloadReclaimCommand::Multipart(r) => match field {
                    "omit" => {
                        r.parts.pop();
                    }
                    "duplicate" => {
                        let mut duplicate = r.parts[0].clone();
                        duplicate.part_number = r.parts.last().unwrap().part_number + 1;
                        for segment in &mut duplicate.segments {
                            segment.part_number = duplicate.part_number;
                        }
                        r.parts.push(duplicate);
                    }
                    "part" => {
                        r.parts[0].part_number += 1;
                        for s in &mut r.parts[0].segments {
                            s.part_number += 1;
                        }
                    }
                    _ => {
                        let s = &mut r.parts[0].segments[0];
                        match field {
                            "hash" => s.segment_okh[0] ^= 1,
                            "vid" => {
                                s.segment_vid = GenerationId::new(s.segment_vid.get() + 1).unwrap()
                            }
                            "data-pg" => s.data_pg_id = (s.data_pg_id + 1) % 8,
                            "ec" => s.ec = EcShape { k: 1, m: 0 },
                            "index" => s.segment_index += 10,
                            _ => unreachable!(),
                        }
                    }
                },
            }
            // The command agrees with the corrupted durable root. Matching
            // those two alone must not drop retained ownership.
            match &mut bad.payload {
                ObjectPayloadReclaimCommand::Segments(r) => {
                    r.segments.sort_by_key(|s| s.segment_index)
                }
                ObjectPayloadReclaimCommand::Multipart(r) => {
                    for part in &mut r.parts {
                        part.segments.sort_by_key(|s| s.segment_index);
                    }
                }
            }
            store_root(&pg, &bad.payload);
            assert_eq!(root(&pg, &bad), Some(bad.payload.clone()));
            let bad = MetadataCommandEnvelope::new(
                good.id(),
                MetadataCommandPayload::DeleteObjectPayloadReclaim(Box::new(bad)),
            );
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(apply(&pg, &bad).is_err(), "{kind}/{field}");
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
            store_root(&pg, &payload(&good).payload);
        }
        apply(&pg, &good).unwrap();
        assert_completed(&pg, &good, &owners);
    }
}

#[test]
fn staging_reclaim_completion_requires_empty_owners_high_water_and_reclaim_destination() {
    for kind in 0..5 {
        for field in [
            "missing-owner",
            "missing-counter",
            "lower-counter",
            "live-destination",
            "active-lifecycle",
            "incarnation",
        ] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(StagingTarget::DirectPut).subject.object_pg.get(),
            )
            .unwrap();
            let (owners, good) = setup(&pg, kind, VersionId::Null);
            let owner = &owners[0];
            match field {
                "missing-owner" => {
                    pg.conn
                        .execute(
                            "DELETE FROM payload_staging_owners WHERE attempt = ?1",
                            [&owner.subject.id.attempt],
                        )
                        .unwrap();
                }
                "missing-counter" => {
                    pg.conn
                        .execute(
                            "DELETE FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
                            rusqlite::params![owner.subject.bucket, owner.subject.key],
                        )
                        .unwrap();
                }
                "lower-counter" => {
                    pg.conn
                        .execute(
                            "UPDATE object_write_counters SET max_committed_generation = 1",
                            [],
                        )
                        .unwrap();
                }
                "live-destination" => {
                    if owner.references.is_empty() {
                        continue;
                    }
                    pg.conn.execute("UPDATE payload_staging_references SET destination = 2 WHERE attempt = ?1", [&owner.subject.id.attempt]).unwrap();
                }
                "active-lifecycle" => {
                    pg.conn
                        .execute(
                            "UPDATE payload_staging_owners SET lifecycle = 1 WHERE attempt = ?1",
                            [&owner.subject.id.attempt],
                        )
                        .unwrap();
                }
                "incarnation" => {
                    // Keep the owner and all proofs coherent: the claim alone
                    // must still bind its original bucket incarnation.
                    pg.conn.execute_batch("UPDATE payload_staging_owners SET incarnation = incarnation + 1; UPDATE payload_staging_resources SET incarnation = incarnation + 1").unwrap();
                }
                _ => unreachable!(),
            }
            let before = introduction::state(&pg);
            let changes = pg.conn.total_changes();
            assert!(apply(&pg, &good).is_err(), "{kind}/{field}");
            assert_eq!(pg.conn.total_changes(), changes);
            assert_eq!(introduction::state(&pg), before);
        }
    }
}

#[test]
fn staging_reclaim_completion_preserves_other_generation_live_or_reclaim_state() {
    for kind in [0, 4] {
        for reclaim_other in [false, true] {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(StagingTarget::DirectPut).subject.object_pg.get(),
            )
            .unwrap();
            let (owners, command) = setup(&pg, kind, VersionId::Null);
            let mut template = owners
                .iter()
                .max_by_key(|o| o.subject.payload_generation.get())
                .unwrap()
                .clone();
            template.subject.originating_epoch = ClusterEpoch::new(8).unwrap();
            let new = object::next_attempt(&pg, &template, StagingTarget::DirectPut, 1);
            object::apply(&pg, &object::command(&pg, &new, VersionId::from_u64(4))).unwrap();
            let other = if reclaim_other {
                let delete = delete::command(
                    &pg,
                    &new,
                    VersionId::from_u64(4),
                    DeleteObjectVersionMode::Specific,
                );
                let other = completion_from_delete(&delete);
                delete::apply(&pg, &delete).unwrap();
                Some(other)
            } else {
                None
            };
            let new = append::load(&pg, &new);
            let live = pg
                .get_object_version(
                    &new.subject.bucket,
                    &new.subject.key,
                    VersionId::from_u64(4),
                )
                .ok();
            let segments = pg
                .get_object_segments(
                    &new.subject.bucket,
                    &new.subject.key,
                    VersionId::from_u64(4),
                )
                .unwrap();
            apply(&pg, &command).unwrap();
            assert_completed(&pg, &command, &owners);
            assert_eq!(append::load(&pg, &new), new);
            assert_eq!(
                pg.get_object_version(
                    &new.subject.bucket,
                    &new.subject.key,
                    VersionId::from_u64(4)
                )
                .ok(),
                live
            );
            assert_eq!(
                pg.get_object_segments(
                    &new.subject.bucket,
                    &new.subject.key,
                    VersionId::from_u64(4)
                )
                .unwrap(),
                segments
            );
            if let Some(other) = other {
                assert_eq!(
                    root(&pg, payload(&other)),
                    Some(payload(&other).payload.clone())
                );
            }
        }
    }
}

#[test]
fn staging_reclaim_completion_live_generation_and_transaction_are_required() {
    for kind in [0, 3, 4] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let owners = delete::source(&pg, kind, VersionId::Null);
        let delete = delete::command(
            &pg,
            &owners[0],
            VersionId::Null,
            DeleteObjectVersionMode::Specific,
        );
        let command = completion_from_delete(&delete);
        store_root(&pg, &payload(&command).payload);
        pg.conn.execute_batch("UPDATE payload_staging_references SET destination = 3 WHERE destination = 2; UPDATE payload_staging_owners SET lifecycle = 3 WHERE lifecycle = 1").unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        assert!(matches!(apply(&pg, &command), Err(ReclaimError::Target)));
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        pg.conn.execute_batch("COMMIT").unwrap();
        assert!(matches!(
            apply(&pg, &command),
            Err(ReclaimError::TransactionRequired)
        ));
    }
}

#[test]
fn staging_reclaim_completion_late_failures_roll_back_root_claim_and_all_owners() {
    for kind in [0, 2, 4] {
        let mut events = vec!["BEFORE DELETE ON object_payload_reclaim_claims"];
        if kind == 4 {
            events.extend(["BEFORE DELETE ON multipart_reclaims", "BEFORE DELETE ON payload_staging_references WHEN OLD.part_number = 11",
                "BEFORE UPDATE OF lifecycle ON payload_staging_owners WHEN NEW.lifecycle = 4 AND OLD.part_number = 11"]);
        } else {
            events.extend([
                "BEFORE DELETE ON object_segments_reclaims",
                "BEFORE DELETE ON payload_staging_references",
                "BEFORE UPDATE OF lifecycle ON payload_staging_owners WHEN NEW.lifecycle = 4",
            ]);
        }
        for event in events {
            let dir = test_util::tempdir();
            let pg = PgStore::open(
                dir.path(),
                fixture(StagingTarget::DirectPut).subject.object_pg.get(),
            )
            .unwrap();
            let (owners, command) = setup(&pg, kind, VersionId::Null);
            claim(&pg, &command);
            pg.conn.execute_batch("CREATE TEMP TABLE outer_work(value INTEGER); INSERT INTO outer_work VALUES (42)").unwrap();
            pg.conn.execute_batch(&format!("CREATE TEMP TRIGGER fail_reclaim {event} BEGIN SELECT RAISE(ABORT, 'staging reclaim failure'); END")).unwrap();
            let before = introduction::state(&pg);
            let error = apply(&pg, &command).unwrap_err();
            assert!(
                error.to_string().contains("staging reclaim failure"),
                "{event}: {error}"
            );
            assert_eq!(introduction::state(&pg), before, "{event}");
            assert!(!pg.conn.is_autocommit());
            assert_eq!(
                pg.conn
                    .query_row("SELECT value FROM outer_work", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                42
            );
            pg.conn.execute_batch("DROP TRIGGER fail_reclaim").unwrap();
            apply(&pg, &command).unwrap();
            assert_completed(&pg, &command, &owners);
        }
    }
}
