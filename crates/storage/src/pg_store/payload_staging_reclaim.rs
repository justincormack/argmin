// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh reclaim-root completion component, not production dispatch or physical
//! deletion authority. Removing a root drops its exact selected bindings only;
//! windows and resource obligations survive for later confirmed reinspection.

use super::*;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandPayload, ObjectPayloadReclaimClaimProof,
    ObjectPayloadReclaimCommand,
};
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::PgMetadataStore;
use crate::types::{
    MultipartReclaimPartRecord, MultipartReclaimPartSegmentRecord, MultipartReclaimRecord,
    ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord,
};
use crate::MetadataError;
use rusqlite::OptionalExtension;

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum ReclaimError {
    #[error("staging reclaim completion requires the enclosing command transaction")]
    TransactionRequired,
    #[error("staging reclaim completion does not bind the exact command and claim")]
    CommandBinding,
    #[error("staging reclaim completion does not match its durable root and owners")]
    Target,
    #[error(transparent)]
    Invariant(#[from] StagingInvariant),
    #[error(transparent)]
    Persistence(#[from] sql::PersistenceError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error(transparent)]
    Database(#[from] rusqlite::Error),
}

impl StagingOwnerRecord {
    pub(super) fn completed_reclaim(&self) -> Result<Self, StagingInvariant> {
        if self.lifecycle != StagingLifecycle::Retiring(StagingRetirement::PublishedRetained) {
            return Err(StagingInvariant::Lifecycle);
        }
        let part_number = match self.subject.target {
            StagingTarget::DirectPut | StagingTarget::StreamPut => None,
            StagingTarget::UploadPart { part_number, .. } => Some(part_number),
        };
        let destination = StagingDestination::Reclaim {
            generation: self.subject.published_generation(),
            part_number,
        };
        if self
            .references
            .iter()
            .any(|reference| reference.destination != destination)
        {
            return Err(StagingInvariant::Reference);
        }
        let mut after = self.clone();
        after.references.clear();
        after.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced);
        Ok(after)
    }
}

fn segments(owner: &StagingOwnerRecord) -> Vec<ObjectSegmentsReclaimSegmentRecord> {
    owner
        .references
        .iter()
        .map(|reference| {
            let window = &owner.windows[reference.window_index as usize];
            ObjectSegmentsReclaimSegmentRecord {
                segment_index: reference.window_index,
                segment_okh: reference.segment_hash,
                segment_vid: reference.physical_vid,
                data_pg_id: window.participants.data_pg.get(),
                ec: window.geometry.ec,
            }
        })
        .collect()
}

pub(in super::super) fn apply_in_open_transaction(
    pg: &PgStore,
    topology: &PgTopology,
    envelope: &MetadataCommandEnvelope,
) -> Result<(), ReclaimError> {
    if pg.conn.is_autocommit() {
        return Err(ReclaimError::TransactionRequired);
    }
    let MetadataCommandPayload::DeleteObjectPayloadReclaim(command) = envelope.payload() else {
        return Err(ReclaimError::CommandBinding);
    };
    let proof = &command.reclaim_claim;
    if !envelope.verify_checksum()
        || envelope.id().pg_id().get() != pg.pg_id
        || topology.object_pg_for(&command.bucket, &command.key) != pg.pg_id
        || proof.bucket_incarnation_generation == 0
        || proof.cluster_epoch > envelope.id().cluster_epoch()
        || proof.reclaim_kind != command.payload.kind()
        || proof.claim_id.is_empty()
        || proof.owner_token.is_empty()
    {
        return Err(ReclaimError::CommandBinding);
    }
    // Claims are primary-local; absence on a replica is expected. A conflicting
    // claim is never released. Renewal deadlines are not replica clock decisions.
    if let Some(claim) = pg.object_payload_reclaim_claim()? {
        if claim.bucket != command.bucket
            || claim.key != command.key
            || claim.generation_id != command.generation_id
            || claim.pg_id != pg.pg_id
            || ObjectPayloadReclaimClaimProof::from(&claim) != *proof
        {
            return Err(ReclaimError::CommandBinding);
        }
    }
    let standard =
        pg.get_object_segments_reclaim(&command.bucket, &command.key, command.generation_id)?;
    let multipart =
        pg.get_multipart_reclaim(&command.bucket, &command.key, command.generation_id)?;
    match (&command.payload, &standard, &multipart) {
        (ObjectPayloadReclaimCommand::Segments(expected), Some(root), None) if expected == root => {
        }
        (ObjectPayloadReclaimCommand::Multipart(expected), None, Some(root))
            if expected == root => {}
        _ => return Err(ReclaimError::Target),
    }
    // A live version cannot be reconciled by dropping its reclaim protection.
    if pg.conn.query_row("SELECT EXISTS(SELECT 1 FROM objects WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3 AND status = 0)",
        rusqlite::params![command.bucket, command.key, command.generation_id.get()], |row| row.get::<_, bool>(0))?
    { return Err(ReclaimError::Target); }
    let owners = sql::owners_for_object_generation(
        &pg.conn,
        PgId::new(pg.pg_id),
        &command.bucket,
        &command.key,
        command.generation_id,
    )?;
    let high_water = pg.conn.query_row("SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
        rusqlite::params![command.bucket, command.key], |row| row.get::<_, Option<u64>>(0)).optional()?.flatten();
    let mut transfers = Vec::new();
    let mut parts = std::collections::BTreeMap::new();
    let mut upload = None;
    for owner in &owners {
        owner.validate(topology)?;
        owner.subject.require_generation_high_water(high_water)?;
        if owner.subject.bucket_incarnation != proof.bucket_incarnation_generation
            || owner.subject.originating_epoch > proof.cluster_epoch
            || !command_follows(owner.subject.id.introduction, envelope.id())
        {
            return Err(ReclaimError::CommandBinding);
        }
        match (&command.payload, &owner.subject.target) {
            (
                ObjectPayloadReclaimCommand::Segments(root),
                StagingTarget::DirectPut | StagingTarget::StreamPut,
            ) => {
                if owners.len() != 1
                    || *root
                        != (ObjectSegmentsReclaimRecord {
                            bucket: command.bucket.clone(),
                            key: command.key.clone(),
                            generation_id: command.generation_id,
                            created_at: root.created_at,
                            segments: segments(owner),
                        })
                {
                    return Err(ReclaimError::Target);
                }
            }
            (
                ObjectPayloadReclaimCommand::Multipart(_),
                StagingTarget::UploadPart {
                    upload_id,
                    part_number,
                    ..
                },
            ) => {
                if upload.replace(upload_id).is_some_and(|id| id != upload_id) {
                    return Err(ReclaimError::Target);
                }
                match owner.lifecycle {
                    StagingLifecycle::Retiring(
                        StagingRetirement::Unpublished | StagingRetirement::PublishedUnreferenced,
                    ) => continue,
                    StagingLifecycle::Retiring(StagingRetirement::PublishedRetained) => (),
                    _ => return Err(ReclaimError::Target),
                }
                let part = MultipartReclaimPartRecord {
                    part_number: *part_number,
                    segments: segments(owner)
                        .into_iter()
                        .map(|segment| MultipartReclaimPartSegmentRecord {
                            part_number: *part_number,
                            segment_index: segment.segment_index,
                            segment_okh: segment.segment_okh,
                            segment_vid: segment.segment_vid,
                            data_pg_id: segment.data_pg_id,
                            ec: segment.ec,
                        })
                        .collect(),
                };
                if parts.insert(*part_number, part).is_some() {
                    return Err(ReclaimError::Target);
                }
            }
            _ => return Err(ReclaimError::Target),
        }
        transfers.push((owner, owner.completed_reclaim()?));
    }
    if transfers.is_empty() {
        return Err(ReclaimError::Target);
    }
    if let ObjectPayloadReclaimCommand::Multipart(root) = &command.payload {
        if *root
            != (MultipartReclaimRecord {
                bucket: command.bucket.clone(),
                key: command.key.clone(),
                generation_id: command.generation_id,
                created_at: root.created_at,
                parts: parts.into_values().collect(),
            })
        {
            return Err(ReclaimError::Target);
        }
    }
    let checked = transfers
        .iter()
        .map(|(before, after)| Ok((before.validate(topology)?, after.validate(topology)?)))
        .collect::<Result<Vec<_>, ReclaimError>>()?;
    let savepoint = sql::CommandApplySavepoint::begin(&pg.conn)?;
    pg.apply_delete_object_payload_reclaim_command(command)?;
    for (before, after) in &checked {
        sql::complete_reclaim(&pg.conn, PgId::new(pg.pg_id), topology, before, after)?;
    }
    savepoint.commit()?;
    Ok(())
}
