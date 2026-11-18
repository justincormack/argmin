// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Metadata-only exact handoff fixtures, using actual publication/displacement
//! and reclaim-completion transactions. No physical-deletion evidence is minted.
use super::super::object::require_direct_retirement;
use super::*;
use crate::metadata_command::{
    DeleteObjectPayloadReclaimCommand, MetadataCommandEnvelope, MetadataCommandPayload,
    ObjectPayloadReclaimClaimProof, ObjectPayloadReclaimCommand,
};
use crate::node_runtime::pg_store::PgStore;
use crate::VersionId;

pub(super) fn setup(pg: &PgStore, kind: u8, count: usize) -> MetadataCommandEnvelope {
    if kind == 2 {
        let old = displacement::source(pg, count);
        return displacement::incoming(pg, &old, 0).0;
    }
    let old = object::initialize(
        pg,
        if kind == 0 {
            StagingTarget::DirectPut
        } else {
            StagingTarget::StreamPut
        },
        count,
    );
    let first = object::command(pg, &old, VersionId::Null);
    object::apply(pg, &first).unwrap();
    let incoming = object::next_attempt(pg, &old, StagingTarget::DirectPut, 1);
    let base = object::command(pg, &incoming, VersionId::Null);
    let mut command = object::payload(&base).clone();
    command.stale_payload = Some(ObjectPayloadReclaimCommand::Segments(
        object_overwrite::reclaim(&first),
    ));
    MetadataCommandEnvelope::new(
        base.id(),
        MetadataCommandPayload::CommitDirectPutObject(Box::new(command)),
    )
}

pub(super) fn reclaim_completion(command: &MetadataCommandEnvelope) -> MetadataCommandEnvelope {
    let command = object::payload(command);
    let payload = command.stale_payload.clone().unwrap();
    let generation = match &payload {
        ObjectPayloadReclaimCommand::Segments(root) => root.generation_id,
        ObjectPayloadReclaimCommand::Multipart(root) => root.generation_id,
    };
    MetadataCommandEnvelope::new(
        MetadataCommandId::new(
            ClusterEpoch::new(9).unwrap(),
            PgId::new(topology().object_pg_for(&command.object.bucket, &command.object.key)),
            MetadataCommandLogIndex::new(7000).unwrap(),
        ),
        MetadataCommandPayload::DeleteObjectPayloadReclaim(Box::new(
            DeleteObjectPayloadReclaimCommand::new(
                command.object.bucket.clone(),
                command.object.key.clone(),
                generation,
                payload.clone(),
                ObjectPayloadReclaimClaimProof {
                    bucket_incarnation_generation: command
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

fn complete(pg: &PgStore, command: &MetadataCommandEnvelope) {
    super::super::reclaim::apply_in_open_transaction(pg, &topology(), &reclaim_completion(command))
        .unwrap();
}

#[test]
fn staging_displaced_retirement_consumed_empty_part_with_an_older_empty_attempt() {
    let dir = test_util::tempdir();
    let pg = PgStore::open(
        dir.path(),
        fixture(StagingTarget::DirectPut).subject.object_pg.get(),
    )
    .unwrap();
    let mut owners = completion::setup(&pg, 0);
    let replacement = completion::new_attempt(&pg, &owners[0], 6, 3, 0);
    part::apply(&pg, &part::command(&pg, &replacement)).unwrap();
    owners.push(replacement);
    completion::apply(&pg, &completion::command(&pg, &owners[0], VersionId::Null)).unwrap();
    let command = displacement::incoming(&pg, &owners, 0).0;
    object::apply(&pg, &command).unwrap();
    require_direct_retirement(&pg, &topology(), &command).unwrap();
    complete(&pg, &command);
    // Both completed empty attempts retain independent release obligations;
    // neither is discarded just because their physical manifests are empty.
    for old in [&owners[0], owners.last().unwrap()] {
        let retained = append::load(&pg, old);
        assert_eq!(
            retained.lifecycle,
            StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced)
        );
        assert!(retained.references.is_empty());
        assert!(matches!(
            retained.resources.creation,
            ResourceResponsibility::ReleaseDue(_)
        ));
    }
    require_direct_retirement(&pg, &topology(), &command).unwrap();
}

#[test]
fn staging_displaced_retirement_binds_consumed_root_full_manifest() {
    for kind in [0, 1, 2] {
        let dir = test_util::tempdir();
        let pg = PgStore::open(
            dir.path(),
            fixture(StagingTarget::DirectPut).subject.object_pg.get(),
        )
        .unwrap();
        let command = setup(&pg, kind, 2);
        object::apply(&pg, &command).unwrap();
        complete(&pg, &command);
        require_direct_retirement(&pg, &topology(), &command).unwrap();
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        for field in 0..8 {
            let mut crossed = object::payload(&command).clone();
            match crossed.stale_payload.as_mut().unwrap() {
                ObjectPayloadReclaimCommand::Segments(root) => match field {
                    0 => root.bucket = crate::tests::bucket_name("crossed"),
                    1 => root.key = crate::tests::object_key("crossed"),
                    2 => root.segments.clear(),
                    3 => root.segments[0].segment_okh[0] ^= 1,
                    4 => root.segments[0].segment_vid = GenerationId::new(999).unwrap(),
                    5 => root.segments[0].data_pg_id ^= 1,
                    6 => root.segments[0].ec.m += 1,
                    7 => root.segments.push(root.segments[0].clone()),
                    _ => unreachable!(),
                },
                ObjectPayloadReclaimCommand::Multipart(root) => match field {
                    0 => root.bucket = crate::tests::bucket_name("crossed"),
                    1 => root.key = crate::tests::object_key("crossed"),
                    2 => root.parts[0].segments.clear(),
                    3 => root.parts[0].segments[0].segment_okh[0] ^= 1,
                    4 => root.parts[0].segments[0].segment_vid = GenerationId::new(999).unwrap(),
                    5 => root.parts[0].segments[0].data_pg_id ^= 1,
                    6 => root.parts[0].segments[0].ec.m += 1,
                    7 => root.parts.push(root.parts[0].clone()),
                    _ => unreachable!(),
                },
            }
            let crossed = MetadataCommandEnvelope::new(
                command.id(),
                MetadataCommandPayload::CommitDirectPutObject(Box::new(crossed)),
            );
            assert!(
                require_direct_retirement(&pg, &topology(), &crossed).is_err(),
                "kind={kind} field={field}"
            );
        }
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
    }
}

#[test]
fn staging_displaced_retirement_requires_root_or_exact_completed_owners_after_restart() {
    for (kind, count) in [(0, 1), (1, 0), (1, 2), (2, 0), (2, 2)] {
        let dir = test_util::tempdir();
        let pg_id = fixture(StagingTarget::DirectPut).subject.object_pg.get();
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        let command = setup(&pg, kind, count);
        object::apply(&pg, &command).unwrap();
        require_direct_retirement(&pg, &topology(), &command).unwrap();
        pg.conn.execute_batch("COMMIT").unwrap();
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        require_direct_retirement(&pg, &topology(), &command).unwrap();
        complete(&pg, &command);
        let before = introduction::state(&pg);
        let changes = pg.conn.total_changes();
        require_direct_retirement(&pg, &topology(), &command).unwrap();
        assert_eq!(pg.conn.total_changes(), changes);
        assert_eq!(introduction::state(&pg), before);
        pg.conn.execute_batch("COMMIT").unwrap();
        drop(pg);
        let pg = PgStore::open(dir.path(), pg_id).unwrap();
        pg.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        require_direct_retirement(&pg, &topology(), &command).unwrap();
    }
}

#[test]
fn staging_displaced_retirement_rejects_missing_root_and_coherently_changed_old_windows() {
    for kind in [0, 1, 2] {
        for completed in [false, true] {
            for fault in [
                "root",
                "owner",
                "placement",
                "reference",
                "resource",
                "incarnation",
            ] {
                let dir = test_util::tempdir();
                let pg = PgStore::open(
                    dir.path(),
                    fixture(StagingTarget::DirectPut).subject.object_pg.get(),
                )
                .unwrap();
                let command = setup(&pg, kind, 2);
                object::apply(&pg, &command).unwrap();
                if completed {
                    complete(&pg, &command);
                }
                let incoming = &object::payload(&command).generation_reservation_id;
                match fault {
                    "root" if completed => continue,
                    "root" => {
                        pg.conn.execute_batch("DELETE FROM object_segments_reclaims; DELETE FROM multipart_reclaims").unwrap();
                    }
                    "owner" => {
                        pg.conn
                            .execute(
                                "DELETE FROM payload_staging_owners WHERE attempt != ?1",
                                [incoming],
                            )
                            .unwrap();
                    }
                    "placement" => {
                        pg.conn.execute("UPDATE payload_staging_windows SET data_pg = (data_pg + 1) % 4 WHERE attempt != ?1 AND window_index = 0", [incoming]).unwrap();
                    }
                    "reference" if completed => continue,
                    "reference" => {
                        pg.conn
                            .execute(
                                "DELETE FROM payload_staging_references WHERE attempt != ?1",
                                [incoming],
                            )
                            .unwrap();
                    }
                    "resource" => {
                        pg.conn.execute("DELETE FROM payload_staging_resources WHERE attempt != ?1 AND role = 0", [incoming]).unwrap();
                    }
                    "incarnation" => {
                        pg.conn.execute("UPDATE payload_staging_owners SET incarnation = incarnation + 1 WHERE attempt != ?1", [incoming]).unwrap();
                        pg.conn.execute("UPDATE payload_staging_resources SET incarnation = incarnation + 1, execution_generation = execution_generation + 1 WHERE attempt != ?1", [incoming]).unwrap();
                    }
                    _ => unreachable!(),
                }
                let before = introduction::state(&pg);
                let changes = pg.conn.total_changes();
                assert!(
                    require_direct_retirement(&pg, &topology(), &command).is_err(),
                    "kind={kind} completed={completed} fault={fault}"
                );
                assert_eq!(pg.conn.total_changes(), changes);
                assert_eq!(introduction::state(&pg), before);
            }
        }
    }
}
