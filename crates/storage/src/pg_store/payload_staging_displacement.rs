// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Completed multipart source validation shared by PUT and MPU overwrite.
//! The caller has validated the full reclaim descriptor against durable rows.
//! Those rows must additionally match the original, independently retained
//! owners; rebuilding a descriptor from corrupted rows is not authorization.

use super::object::ObjectError;
use super::*;
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::PgMetadataStore;
use crate::types::{validate_segment_shard_integrity_for_profile, MultipartPartSegmentRecord};
use crate::LiveObjectRecord;
use rusqlite::OptionalExtension;

/// Match the complete scope deleted by terminal apply, not the part manifest
/// or upload identity. Segment rows have no FK to object_parts, and a crossed
/// upload ID must not hide a row that version-scoped deletion would consume.
fn version_segments(
    pg: &PgStore,
    object: &LiveObjectRecord,
) -> Result<std::collections::BTreeMap<u32, Vec<MultipartPartSegmentRecord>>, ObjectError> {
    let mut statement = pg.conn.prepare_cached(
        "SELECT bucket, key, upload_id, version_id, part_number, segment_index, \
         size, segment_crc64, segment_okh, segment_vid, data_pg_id, placement_cluster_epoch, \
         ec_k, ec_m, shard_integrity FROM multipart_part_segments \
         WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 ORDER BY part_number, segment_index",
    )?;
    let rows = statement.query_map(
        rusqlite::params![object.bucket, object.key, object.version_id.to_u64() as i64],
        PgStore::row_to_multipart_part_segment,
    )?;
    let mut by_part = std::collections::BTreeMap::<_, Vec<_>>::new();
    for row in rows {
        let segment = row?;
        by_part
            .entry(segment.part_number)
            .or_default()
            .push(segment);
    }
    Ok(by_part)
}

pub(super) fn multipart(
    pg: &PgStore,
    topology: &PgTopology,
    object: &LiveObjectRecord,
    incarnation: u64,
) -> Result<Vec<(StagingOwnerRecord, StagingOwnerRecord)>, ObjectError> {
    let (upload, _) = pg
        .multipart_completion_identity(&object.bucket, &object.key, object.version_id)?
        .ok_or(ObjectError::DisplacedOwner)?;
    let parts = pg.get_object_parts(&object.bucket, &object.key, object.version_id)?;
    let mut segments_by_part = version_segments(pg, object)?;
    if parts.is_empty()
        || object.layout.parts_count() != Some(parts.len() as u32)
        || object.ec != (EcShape { k: 0, m: 0 })
        || parts
            .windows(2)
            .any(|pair| pair[0].part_number >= pair[1].part_number)
        || parts
            .iter()
            .try_fold(0_u64, |sum, part| sum.checked_add(part.size))
            != Some(object.size)
    {
        return Err(ObjectError::DisplacedOwner);
    }
    let owners = sql::owners_for_upload(&pg.conn, PgId::new(pg.pg_id), &upload)?;
    let high_water = pg.conn.query_row(
        "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
        rusqlite::params![object.bucket, object.key], |row| row.get::<_, Option<u64>>(0),
    ).optional()?.flatten();
    let mut selected = BTreeSet::new();
    let mut transfers = Vec::new();
    let mut by_part = std::collections::BTreeMap::<_, Vec<_>>::new();
    for owner in &owners {
        let StagingTarget::UploadPart { part_number, .. } = owner.subject.target else {
            return Err(ObjectError::DisplacedOwner);
        };
        by_part
            .entry((part_number, owner.subject.payload_generation.get()))
            .or_default()
            .push(owner);
    }
    for part in &parts {
        let matches = by_part.get(&(part.part_number, part.part_vid.get()));
        let Some([old]) = matches.map(Vec::as_slice) else {
            return Err(ObjectError::DisplacedOwner);
        };
        old.validate(topology)?;
        old.subject.require_generation_high_water(high_water)?;
        if old.subject.bucket != object.bucket
            || old.subject.key != object.key
            || old.subject.bucket_incarnation != incarnation
            || !matches!(&old.subject.target, StagingTarget::UploadPart { upload_id, upload_generation, .. }
                if *upload_id == upload && *upload_generation == object.generation_id)
            || part.bucket != object.bucket
            || part.key != object.key
            || part.version_id != object.version_id
            || part.placement_cluster_epoch != old.part_placement_epoch()?
            || part.data_pg_id
                != topology
                    .object_generation_multipart_part_data_pg(
                        &object.bucket,
                        &object.key,
                        object.generation_id,
                        part.part_number,
                    )
                    .get()
            || old.windows.iter().any(|window| {
                window.geometry.codec != object.encryption.payload_codec()
                    || window.geometry.ec
                        != (EcShape {
                            k: part.ec_k,
                            m: part.ec_m,
                        })
            })
            || old
                .references
                .iter()
                .try_fold(0_u64, |sum, r| sum.checked_add(r.logical_size))
                != Some(part.size)
        {
            return Err(ObjectError::DisplacedOwner);
        }
        // Also validates lifecycle and every exact Object destination, retaining
        // the enclosing object generation rather than the part generation.
        let retired = old.reclaimed_object()?;
        retired.validate(topology)?;
        let segments = segments_by_part
            .remove(&part.part_number)
            .unwrap_or_default();
        if segments.len() != old.references.len() {
            return Err(ObjectError::DisplacedOwner);
        }
        for (segment, reference) in segments.iter().zip(&old.references) {
            let window = &old.windows[reference.window_index as usize];
            if segment.bucket != object.bucket
                || segment.key != object.key
                || segment.upload_id != upload
                || segment.version_id != object.version_id.to_u64()
                || segment.part_number != part.part_number
                || segment.segment_index != reference.window_index
                || segment.segment_okh != reference.segment_hash
                || segment.segment_vid != reference.physical_vid
                || segment.size != reference.logical_size
                || segment.data_pg_id != window.participants.data_pg.get()
                || segment.placement_cluster_epoch != window.participants.placement_epoch
                || segment.ec_k != window.geometry.ec.k
                || segment.ec_m != window.geometry.ec.m
                || validate_segment_shard_integrity_for_profile(
                    object.encryption.payload_profile(),
                    segment.shard_integrity.as_ref(),
                    window.geometry.ec,
                    reference.stored_size,
                    segment.segment_crc64,
                )
                .is_err()
            {
                return Err(ObjectError::DisplacedOwner);
            }
        }
        selected.insert(old.subject.id.attempt.clone());
        transfers.push(((*old).clone(), retired));
    }
    // Every row that apply would delete must have passed exact owner/window
    // validation above. A valid reclaim for just the listed parts is not enough.
    if !segments_by_part.is_empty() {
        return Err(ObjectError::DisplacedOwner);
    }
    // A forged manifest must not silently omit a completed owner's part, even
    // if the command's reclaim list was rebuilt to agree with that manifest.
    // Omitted/aborted attempts remain independent obligations and are untouched.
    for owner in &owners {
        if matches!(
            owner.lifecycle,
            StagingLifecycle::Active
                | StagingLifecycle::Published
                | StagingLifecycle::Retiring(StagingRetirement::PublishedRetained)
        ) && !selected.contains(&owner.subject.id.attempt)
        {
            return Err(ObjectError::DisplacedOwner);
        }
    }
    Ok(transfers)
}
