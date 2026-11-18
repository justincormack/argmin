// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Inspect an applied overwrite's independent reclaim handoff. No cleanup or
//! owner mutation: even a consumed root must leave exact retained windows and
//! release obligations. The caller owns the slot-removal transaction.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

type Segment = (u32, [u8; 16], GenerationId, u32, EcShape);

pub(super) fn require_displacement(
    pg: &PgStore,
    topology: &PgTopology,
    envelope: &MetadataCommandEnvelope,
    command: &CommitDirectPutObjectCommand,
    expected: &ObjectPayloadReclaimCommand,
) -> Result<(), ObjectError> {
    let mut manifests = BTreeMap::<Option<u32>, Vec<Segment>>::new();
    let (bucket, key, generation) = match expected {
        ObjectPayloadReclaimCommand::Segments(root) => {
            manifests.insert(
                None,
                root.segments
                    .iter()
                    .map(|s| {
                        (
                            s.segment_index,
                            s.segment_okh,
                            s.segment_vid,
                            s.data_pg_id,
                            s.ec,
                        )
                    })
                    .collect(),
            );
            (&root.bucket, &root.key, root.generation_id)
        }
        ObjectPayloadReclaimCommand::Multipart(root) => {
            if root.parts.is_empty()
                || root
                    .parts
                    .windows(2)
                    .any(|p| p[0].part_number >= p[1].part_number)
            {
                return Err(ObjectError::DisplacedOwner);
            }
            for part in &root.parts {
                if part
                    .segments
                    .iter()
                    .any(|s| s.part_number != part.part_number)
                {
                    return Err(ObjectError::DisplacedOwner);
                }
                manifests.insert(
                    Some(part.part_number),
                    part.segments
                        .iter()
                        .map(|s| {
                            (
                                s.segment_index,
                                s.segment_okh,
                                s.segment_vid,
                                s.data_pg_id,
                                s.ec,
                            )
                        })
                        .collect(),
                );
            }
            (&root.bucket, &root.key, root.generation_id)
        }
    };
    if bucket != &command.object.bucket
        || key != &command.object.key
        || generation == command.object.generation_id
        || !command.object.version_id.is_null()
    {
        return Err(ObjectError::DisplacedOwner);
    }
    let standard = pg.get_object_segments_reclaim(bucket, key, generation)?;
    let multipart = pg.get_multipart_reclaim(bucket, key, generation)?;
    let present = match (expected, &standard, &multipart) {
        (ObjectPayloadReclaimCommand::Segments(expected), Some(actual), None)
            if expected == actual =>
        {
            true
        }
        (ObjectPayloadReclaimCommand::Multipart(expected), None, Some(actual))
            if expected == actual =>
        {
            true
        }
        (_, None, None) => false,
        _ => return Err(ObjectError::DisplacedOwner),
    };
    if pg.conn.query_row("SELECT EXISTS(SELECT 1 FROM objects WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3 AND status = 0)",
        rusqlite::params![bucket, key, generation.get()], |r| r.get::<_, bool>(0))?
    {
        return Err(ObjectError::DisplacedOwner);
    }
    let owners =
        sql::owners_for_object_generation(&pg.conn, PgId::new(pg.pg_id), bucket, key, generation)?;
    if owners.is_empty()
        || (matches!(expected, ObjectPayloadReclaimCommand::Segments(_)) && owners.len() != 1)
    {
        return Err(ObjectError::DisplacedOwner);
    }
    let high_water = pg.conn.query_row(
        "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
        rusqlite::params![bucket, key], |r| r.get::<_, Option<u64>>(0),
    ).optional()?.flatten();
    let mut covered = BTreeSet::new();
    let mut upload = None;
    for owner in &owners {
        owner.validate(topology)?;
        owner.subject.require_generation_high_water(high_water)?;
        if owner.subject.bucket_incarnation
            != command
                .bucket_write_reservation
                .bucket_incarnation_generation
            || !command_follows(owner.subject.id.introduction, envelope.id())
        {
            return Err(ObjectError::DisplacedOwner);
        }
        let (part, selected_count) = match (&owner.subject.target, expected) {
            (StagingTarget::DirectPut, ObjectPayloadReclaimCommand::Segments(_)) => (None, 1),
            (StagingTarget::StreamPut, ObjectPayloadReclaimCommand::Segments(_)) => {
                (None, owner.windows.len() - 1)
            }
            (
                StagingTarget::UploadPart {
                    upload_id,
                    part_number,
                    ..
                },
                ObjectPayloadReclaimCommand::Multipart(_),
            ) => {
                if upload
                    .replace(upload_id)
                    .is_some_and(|previous| previous != upload_id)
                {
                    return Err(ObjectError::DisplacedOwner);
                }
                (Some(*part_number), owner.windows.len() - 1)
            }
            _ => return Err(ObjectError::DisplacedOwner),
        };
        match owner.lifecycle {
            StagingLifecycle::Retiring(StagingRetirement::PublishedRetained) if present => {
                if owner.references.iter().any(|r| {
                    r.destination
                        != (StagingDestination::Reclaim {
                            generation,
                            part_number: part,
                        })
                }) {
                    return Err(ObjectError::DisplacedOwner);
                }
            }
            StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced) => {
                // Earlier replaced/omitted parts also remain durable. They
                // cannot stand in for a still-present root's selected owners.
                if present {
                    continue;
                }
            }
            StagingLifecycle::Retiring(StagingRetirement::Unpublished) if part.is_some() => {
                continue
            }
            _ => return Err(ObjectError::DisplacedOwner),
        }
        let actual: Vec<Segment> = owner.windows[..selected_count]
            .iter()
            .map(|w| {
                (
                    w.index,
                    w.segment_hash,
                    w.start,
                    w.participants.data_pg.get(),
                    w.geometry.ec,
                )
            })
            .collect();
        if manifests.get(&part) == Some(&actual) {
            // An empty part has no physical keys. After root consumption, two
            // old empty attempts may be indistinguishable in the root grammar;
            // both still retain their independently validated obligations.
            if !covered.insert(part) && (present || !actual.is_empty()) {
                return Err(ObjectError::DisplacedOwner);
            }
        } else if present || part.is_none() {
            return Err(ObjectError::DisplacedOwner);
        }
    }
    if covered.len() != manifests.len() {
        return Err(ObjectError::DisplacedOwner);
    }
    Ok(())
}
