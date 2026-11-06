// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! UploadPart publication/replacement transaction component. Production dispatch
//! remains inactive until mandatory descriptors, custody and all terminal
//! transitions are integrated. The enclosing command acceptance boundary owns
//! replay/reissue authorization and applied-prefix recording; these transitions
//! do not infer replay from a surviving part or terminal owner.

use super::*;
use crate::metadata_command::{
    CommitStreamPartCommand, MetadataCommandEnvelope, MetadataCommandPayload,
};
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::PgMetadataStore;
use crate::types::{
    validate_segment_shard_integrity_for_profile, MultipartPartRecord, MultipartPartSegmentRecord,
};
use crate::{
    MetadataError, StreamUploadState, StreamUploadTarget, MULTIPART_PART_SEGMENT_STAGING_VERSION_ID,
};
use rusqlite::OptionalExtension;

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum PartError {
    #[error("staging part publication requires the enclosing command transaction")]
    TransactionRequired,
    #[error("staging part publication does not bind the exact owner and command")]
    CommandBinding,
    #[error("staging part publication has missing or changed live responsibility")]
    LiveResponsibility,
    #[error("staging part publication disagrees with the retained selected prefix")]
    SelectedPrefix,
    #[error("displaced part does not match its exact retained owner")]
    DisplacedOwner,
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
    /// Empty parts retain their finalization epoch; nonempty parts retain the
    /// first selected placement, independently of later completion/overwrite.
    pub(super) fn part_placement_epoch(&self) -> Result<ClusterEpoch, StagingInvariant> {
        if let Some(reference) = self.references.first() {
            return self
                .windows
                .get(reference.window_index as usize)
                .map(|window| window.participants.placement_epoch)
                .ok_or(StagingInvariant::Reference);
        }
        let ResourceResponsibility::ReleaseDue(finalization) = &self.resources.finalization else {
            return Err(StagingInvariant::Lifecycle);
        };
        Ok(finalization.identity.cluster_epoch)
    }

    fn part_destination(&self) -> Result<StagingDestination, StagingInvariant> {
        let StagingTarget::UploadPart {
            upload_id,
            part_number,
            ..
        } = &self.subject.target
        else {
            return Err(StagingInvariant::Target);
        };
        Ok(StagingDestination::Part {
            upload_id: upload_id.clone(),
            part_number: *part_number,
            payload_generation: self.subject.payload_generation,
        })
    }

    pub(super) fn published_part(
        &self,
        finalization: &StagingBucketResource,
    ) -> Result<Self, StagingInvariant> {
        if self.lifecycle != StagingLifecycle::Active
            || self.resources.finalization != ResourceResponsibility::NoResource
        {
            return Err(StagingInvariant::Lifecycle);
        }
        let destination = self.part_destination()?;
        finalization.validate(
            &self.subject,
            UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
        )?;
        let mut after = self.clone();
        after.lifecycle = StagingLifecycle::Published;
        if let ResourceResponsibility::Live(resource) = &self.resources.generation {
            after.resources.generation = ResourceResponsibility::ReleaseDue(resource.clone());
        }
        after.resources.finalization = ResourceResponsibility::ReleaseDue(finalization.clone());
        for reference in &mut after.references {
            reference.destination = destination.clone();
        }
        Ok(after)
    }

    pub(super) fn require_published_part(&self) -> Result<(), StagingInvariant> {
        if !matches!(
            self.lifecycle,
            StagingLifecycle::Published
                | StagingLifecycle::Retiring(StagingRetirement::PublishedRetained)
        ) {
            return Err(StagingInvariant::Lifecycle);
        }
        let destination = self.part_destination()?;
        if self
            .references
            .iter()
            .any(|reference| reference.destination != destination)
        {
            return Err(StagingInvariant::Reference);
        }
        Ok(())
    }

    pub(super) fn unreferenced_part(&self) -> Result<Self, StagingInvariant> {
        self.require_published_part()?;
        let mut after = self.clone();
        after.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedUnreferenced);
        after.references.clear();
        Ok(after)
    }

    pub(super) fn completed_part(&self) -> Result<Self, StagingInvariant> {
        // Check the exact Part destination and published lifecycle even for an
        // empty part; do not permit a second Object/Reclaim transfer as replay.
        self.require_published_part()?;
        let StagingTarget::UploadPart {
            upload_generation,
            part_number,
            ..
        } = self.subject.target
        else {
            return Err(StagingInvariant::Target);
        };
        let mut after = self.clone();
        for reference in &mut after.references {
            reference.destination = StagingDestination::Object {
                generation: upload_generation,
                part_number: Some(part_number),
            };
        }
        Ok(after)
    }
}

fn validate_generation_fence(pg: &PgStore, owner: &StagingOwnerRecord) -> Result<(), PartError> {
    let high_water = pg.conn.query_row(
        "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
        rusqlite::params![owner.subject.bucket, owner.subject.key], |row| row.get::<_, Option<u64>>(0),
    ).optional()?.flatten();
    owner.subject.require_generation_high_water(high_water)?;
    Ok(())
}

fn validate_live(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    command: &CommitStreamPartCommand,
) -> Result<(), PartError> {
    let subject = &owner.subject;
    let StagingTarget::UploadPart {
        upload_id,
        upload_generation,
        part_number,
    } = &subject.target
    else {
        return Err(PartError::CommandBinding);
    };
    if subject.bucket != command.bucket
        || subject.key != command.key
        || subject.id.attempt != command.session_id
        || *upload_id != command.upload.upload_id
        || *upload_generation != command.upload.object_generation_id
        || *part_number != command.part.part_number
        || subject.payload_generation != command.part.part_vid
        || owner.lifecycle != StagingLifecycle::Active
        || (command.existing_part.is_none() && !command.displaced_segments.is_empty())
    {
        return Err(PartError::CommandBinding);
    }
    validate_generation_fence(pg, owner)?;
    let session = pg.get_stream_upload(&subject.id.attempt)?;
    let lookahead = owner.windows.last().ok_or(PartError::LiveResponsibility)?;
    if session.bucket != subject.bucket
        || session.key != subject.key
        || session.bucket_incarnation_generation != subject.bucket_incarnation
        || session.target
            != (StreamUploadTarget::UploadPart {
                upload_id: upload_id.clone(),
                part_number: *part_number,
            })
        || session.state != StreamUploadState::InProgress
        || session.encryption != command.upload.encryption
        || session.bucket_write_reservation.is_some()
        || pg.load_stream_allocation_issuer(&subject.id.attempt)? != subject.issuer
        || (session.next_segment_vid != lookahead.start
            && session.next_segment_vid != lookahead.end)
        || owner
            .windows
            .iter()
            .any(|window| window.geometry.codec != session.encryption.payload_codec())
    {
        return Err(PartError::LiveResponsibility);
    }
    // Reuse current command/part/upload/size/CRC/integrity and exact-displaced
    // preflight. Do not allow its already-applied shortcut to bypass ownership.
    pg.validate_commit_stream_part_command(command)?;
    let segments = pg.list_stream_segments(&subject.id.attempt)?;
    if segments.len() != owner.references.len() {
        return Err(PartError::SelectedPrefix);
    }
    for ((segment, reference), committed) in segments
        .iter()
        .zip(&owner.references)
        .zip(&command.segments)
    {
        if owner.selected_stream_reference(segment)? != *reference
            || segment.shard_integrity != committed.shard_integrity
        {
            return Err(PartError::SelectedPrefix);
        }
    }
    Ok(())
}

fn validate_displaced(
    pg: &PgStore,
    current: &StagingOwnerRecord,
    old: &StagingOwnerRecord,
    part: &MultipartPartRecord,
    segments: &[MultipartPartSegmentRecord],
    command: &CommitStreamPartCommand,
) -> Result<(), PartError> {
    if old.subject.id == current.subject.id
        || old.subject.payload_generation == current.subject.payload_generation
        || old.subject.bucket != current.subject.bucket
        || old.subject.key != current.subject.key
        || old.subject.bucket_incarnation != current.subject.bucket_incarnation
        || old.subject.target != current.subject.target
        || old.subject.payload_generation != part.part_vid
        || old.references.len() != segments.len()
        || old
            .references
            .iter()
            .map(|reference| reference.logical_size)
            .sum::<u64>()
            != part.size
    {
        return Err(PartError::DisplacedOwner);
    }
    old.unreferenced_part()?;
    validate_generation_fence(pg, old)?;
    for (segment, reference) in segments.iter().zip(&old.references) {
        let window = &old.windows[reference.window_index as usize];
        if segment.bucket != old.subject.bucket
            || segment.key != old.subject.key
            || segment.upload_id != part.upload_id
            || segment.part_number != part.part_number
            || segment.version_id != MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64()
            || segment.segment_index != reference.window_index
            || segment.segment_okh != reference.segment_hash
            || segment.segment_vid != reference.physical_vid
            || segment.size != reference.logical_size
            || segment.data_pg_id != window.participants.data_pg.get()
            || segment.placement_cluster_epoch != window.participants.placement_epoch
            || segment.ec_k != window.geometry.ec.k
            || segment.ec_m != window.geometry.ec.m
            || window.geometry.codec != command.upload.encryption.payload_codec()
            || validate_segment_shard_integrity_for_profile(
                command.upload.encryption.payload_profile(),
                segment.shard_integrity.as_ref(),
                window.geometry.ec,
                reference.stored_size,
                segment.segment_crc64,
            )
            .is_err()
        {
            return Err(PartError::DisplacedOwner);
        }
    }
    Ok(())
}

pub(in super::super) fn apply_in_open_transaction(
    pg: &PgStore,
    configured_topology: &PgTopology,
    envelope: &MetadataCommandEnvelope,
) -> Result<(), PartError> {
    if pg.conn.is_autocommit() {
        return Err(PartError::TransactionRequired);
    }
    let MetadataCommandPayload::CommitStreamPart(command) = envelope.payload() else {
        return Err(PartError::CommandBinding);
    };
    let owner = sql::load(&pg.conn, PgId::new(pg.pg_id), &command.session_id)?
        .ok_or(PartError::LiveResponsibility)?;
    let checked = owner.validate(configured_topology)?;
    if !envelope.verify_checksum()
        || envelope.id().pg_id() != owner.subject.object_pg
        || !command_follows(owner.subject.id.introduction, envelope.id())
        || command.bucket_write_reservation.cluster_epoch != envelope.id().cluster_epoch()
    {
        return Err(PartError::CommandBinding);
    }
    validate_live(pg, &owner, command)?;
    let finalization = StagingBucketResource::capture(
        &command.bucket_write_reservation,
        PgId::new(configured_topology.bucket_pg_for(&owner.subject.bucket)),
    );
    let after = owner.published_part(&finalization)?;
    let checked_after = after.validate(configured_topology)?;
    let displaced = if let Some(part) = &command.existing_part {
        let old = sql::owner_for_part(
            &pg.conn,
            owner.subject.object_pg,
            &command.bucket,
            &command.key,
            part,
        )?;
        old.validate(configured_topology)?;
        validate_displaced(pg, &owner, &old, part, &command.displaced_segments, command)?;
        let retired = old.unreferenced_part()?;
        Some((old, retired))
    } else {
        None
    };
    let checked_displaced = displaced
        .as_ref()
        .map(|(old, retired)| -> Result<_, StagingInvariant> {
            Ok((
                old.validate(configured_topology)?,
                retired.validate(configured_topology)?,
            ))
        })
        .transpose()?;
    let savepoint = sql::CommandApplySavepoint::begin(&pg.conn)?;
    pg.apply_commit_stream_part_command(command)?;
    sql::publish_part(
        &pg.conn,
        owner.subject.object_pg,
        configured_topology,
        &checked,
        &checked_after,
        checked_displaced
            .as_ref()
            .map(|(old, retired)| (old, retired)),
    )?;
    savepoint.commit()?;
    Ok(())
}
