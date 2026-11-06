// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Standard-object publication component, not production activation. Replacing
//! a null version transfers its exact retained owners to reclaim in the same
//! transaction. MPU completion shares this displacement validation.
//! Exact replay/reissue authorization belongs to the enclosing command boundary,
//! not inference from a surviving object or a missing session/reservation.

use super::*;
use crate::metadata_command::{
    CommitDirectPutObjectCommand, MetadataCommandEnvelope, MetadataCommandPayload,
    ObjectPayloadReclaimCommand,
};
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::PgMetadataStore;
use crate::{
    LiveObjectRecord, MetadataError, ObjectLayout, ObjectSegmentRecord, PutLiveObjectReq,
    StoredObject, StreamUploadState, StreamUploadTarget,
};
use rusqlite::OptionalExtension;

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum ObjectError {
    #[error("staging object publication requires the enclosing command transaction")]
    TransactionRequired,
    #[error("staging object publication does not bind the exact owner and command")]
    CommandBinding,
    #[error("staging object publication has missing or changed live responsibility")]
    LiveResponsibility,
    #[error("staging object publication disagrees with the retained selected prefix")]
    SelectedPrefix,
    #[error("staging object publication requires a displaced-payload ownership transfer")]
    DisplacedPayload,
    #[error("displaced object does not match its exact retained owner")]
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
    pub(super) fn reclaimed_object(&self) -> Result<Self, StagingInvariant> {
        let (generation, part_number) = match self.subject.target {
            StagingTarget::DirectPut | StagingTarget::StreamPut => {
                (self.subject.payload_generation, None)
            }
            StagingTarget::UploadPart {
                upload_generation,
                part_number,
                ..
            } => (upload_generation, Some(part_number)),
        };
        if !matches!(
            self.lifecycle,
            StagingLifecycle::Published
                | StagingLifecycle::Retiring(StagingRetirement::PublishedRetained)
        ) {
            return Err(StagingInvariant::Lifecycle);
        }
        let destination = StagingDestination::Object {
            generation,
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
        after.lifecycle = StagingLifecycle::Retiring(StagingRetirement::PublishedRetained);
        for reference in &mut after.references {
            reference.destination = StagingDestination::Reclaim {
                generation,
                part_number,
            };
        }
        Ok(after)
    }

    pub(super) fn published_standard(
        &self,
        references: &[StagingReference],
    ) -> Result<Self, StagingInvariant> {
        let destination = StagingDestination::Object {
            generation: self.subject.payload_generation,
            part_number: None,
        };
        match self.subject.target {
            StagingTarget::DirectPut => {}
            StagingTarget::StreamPut => {
                let mut expected = self.references.clone();
                for reference in &mut expected {
                    reference.destination = destination.clone();
                }
                if references != expected {
                    return Err(StagingInvariant::Reference);
                }
            }
            StagingTarget::UploadPart { .. } => return Err(StagingInvariant::Target),
        }
        if references.iter().any(|r| r.destination != destination) {
            return Err(StagingInvariant::Reference);
        }
        let mut after = self.closed_unpublished()?;
        after.lifecycle = StagingLifecycle::Published;
        after.references = references.to_vec();
        Ok(after)
    }

    fn selected_object_reference(
        &self,
        command: &CommitDirectPutObjectCommand,
        index: usize,
        segment: &ObjectSegmentRecord,
    ) -> Result<StagingReference, ObjectError> {
        let window = self.windows.get(index).ok_or(ObjectError::SelectedPrefix)?;
        if segment.segment_index as usize != index
            || segment.bucket != self.subject.bucket
            || segment.key != self.subject.key
            || segment.version_id != command.object.version_id
            || segment.segment_okh != window.segment_hash
            || segment.segment_vid != window.start
            || segment.data_pg_id != window.participants.data_pg.get()
            || segment.placement_cluster_epoch != window.participants.placement_epoch
            || segment.ec_k != window.geometry.ec.k
            || segment.ec_m != window.geometry.ec.m
            || segment.size > window.geometry.logical_bound
            || command.object.encryption.payload_codec() != window.geometry.codec
        {
            return Err(ObjectError::SelectedPrefix);
        }
        Ok(StagingReference {
            window_index: segment.segment_index,
            segment_hash: segment.segment_okh,
            physical_vid: segment.segment_vid,
            logical_size: segment.size,
            stored_size: window
                .geometry
                .codec
                .stored_size(segment.size)
                .ok_or(ObjectError::SelectedPrefix)?,
            destination: StagingDestination::Object {
                generation: self.subject.payload_generation,
                part_number: None,
            },
        })
    }
}

fn validate_live(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    command: &CommitDirectPutObjectCommand,
) -> Result<(), ObjectError> {
    let subject = &owner.subject;
    let ResourceResponsibility::Live(creation) = &owner.resources.creation else {
        return Err(ObjectError::LiveResponsibility);
    };
    if subject.bucket != command.object.bucket
        || subject.key != command.object.key
        || subject.id.attempt != command.generation_reservation_id
        || subject.payload_generation != command.object.generation_id
        || command.object.layout != ObjectLayout::Standard
        // Object-level geometry must come from the same retained allocation
        // authority as its segments, including the initial window of an empty
        // stream. Checking only per-segment geometry leaves this DTO forgeable.
        || owner.windows.iter().any(|window| command.object.ec != window.geometry.ec)
        || StagingBucketResource::capture(&command.bucket_write_reservation, creation.release_pg)
            .identity
            != creation.identity
    {
        return Err(ObjectError::CommandBinding);
    }
    let generation =
        pg.get_object_generation_reservation(&subject.bucket, &subject.key, &subject.id.attempt)?;
    if generation != subject.payload_generation {
        return Err(ObjectError::LiveResponsibility);
    }
    let high_water = pg.conn.query_row(
        "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
        rusqlite::params![subject.bucket, subject.key], |row| row.get::<_, Option<u64>>(0),
    ).optional()?.flatten();
    subject.require_generation_high_water(high_water)?;
    match subject.target {
        StagingTarget::DirectPut => {
            match pg.get_stream_upload(&subject.id.attempt) {
                Err(MetadataError::StreamSessionNotFound { .. }) => {}
                Err(error) => return Err(error.into()),
                Ok(_) => return Err(ObjectError::LiveResponsibility),
            }
            if command.segments.len() != 1 {
                return Err(ObjectError::SelectedPrefix);
            }
        }
        StagingTarget::StreamPut => {
            let session = pg.get_stream_upload(&subject.id.attempt)?;
            let current = owner
                .windows
                .last()
                .ok_or(ObjectError::LiveResponsibility)?;
            if session.bucket != subject.bucket
                || session.key != subject.key
                || session.bucket_incarnation_generation != subject.bucket_incarnation
                || session.target != StreamUploadTarget::PutObject
                || session.state != StreamUploadState::InProgress
                || pg.load_stream_allocation_issuer(&subject.id.attempt)? != subject.issuer
                || (session.next_segment_vid != current.start
                    && session.next_segment_vid != current.end)
                || owner
                    .windows
                    .iter()
                    .any(|window| window.geometry.codec != session.encryption.payload_codec())
            {
                return Err(ObjectError::LiveResponsibility);
            }
            // Existing apply also checks the stable session proof and committed
            // encryption subject before writing. Bind the exact staged bytes'
            // metadata here, including the witness, not just their placement.
            let staged = pg.list_stream_segments(&subject.id.attempt)?;
            if staged.len() != command.segments.len() || staged.len() != owner.references.len() {
                return Err(ObjectError::SelectedPrefix);
            }
            for ((staged, committed), reference) in
                staged.iter().zip(&command.segments).zip(&owner.references)
            {
                if owner.selected_stream_reference(staged)? != *reference
                    || staged.size != committed.size
                    || staged.segment_crc64 != committed.segment_crc64
                    || staged.shard_integrity != committed.shard_integrity
                {
                    return Err(ObjectError::SelectedPrefix);
                }
            }
        }
        StagingTarget::UploadPart { .. } => return Err(ObjectError::CommandBinding),
    }
    let total = command
        .segments
        .iter()
        .try_fold(0_u64, |sum, segment| sum.checked_add(segment.size));
    if total != Some(command.object.size) {
        return Err(ObjectError::SelectedPrefix);
    }
    Ok(())
}

/// Resolve and validate displacement before either the live manifest or its
/// owner changes. Empty streams still require one exact logical owner; hashes
/// and the presence of a reclaim root are not ownership/replay evidence.
pub(super) fn displaced_object(
    pg: &PgStore,
    topology: &PgTopology,
    replacement: &PutLiveObjectReq,
    proof: &BucketWriteReservationProof,
    record: &LiveObjectRecord,
    reclaim: &ObjectPayloadReclaimCommand,
) -> Result<Vec<(StagingOwnerRecord, StagingOwnerRecord)>, ObjectError> {
    if !record.version_id.is_null() || record.generation_id == replacement.generation_id {
        return Err(ObjectError::DisplacedPayload);
    }
    if record.bucket != replacement.bucket
        || record.key != replacement.key
        || record.version_id != replacement.version_id
        || proof.bucket != replacement.bucket
        || proof.target_context.as_deref() != Some(replacement.key.as_str())
    {
        return Err(ObjectError::DisplacedOwner);
    }
    retained_for_reclaim(
        pg,
        topology,
        record,
        proof.bucket_incarnation_generation,
        reclaim,
    )
}

/// Shared ownership preflight for overwrite and deletion. Unlike an overwrite,
/// an explicit delete may retire a numbered version, without synthesizing a
/// replacement object or changing the original physical/reference identity.
pub(super) fn retained_for_reclaim(
    pg: &PgStore,
    topology: &PgTopology,
    record: &LiveObjectRecord,
    incarnation: u64,
    reclaim: &ObjectPayloadReclaimCommand,
) -> Result<Vec<(StagingOwnerRecord, StagingOwnerRecord)>, ObjectError> {
    pg.validate_object_payload_reclaim_matches_durable_object(
        &record.bucket,
        &record.key,
        record,
        reclaim,
        "staging displaced object reclaim binding",
    )?;
    if pg
        .get_object_segments_reclaim(&record.bucket, &record.key, record.generation_id)?
        .is_some()
        || pg
            .get_multipart_reclaim(&record.bucket, &record.key, record.generation_id)?
            .is_some()
    {
        return Err(ObjectError::DisplacedOwner);
    }
    if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
        return displacement::multipart(pg, topology, record, incarnation);
    }
    let old = sql::owner_for_standard(
        &pg.conn,
        PgId::new(pg.pg_id),
        &record.bucket,
        &record.key,
        record.generation_id,
    )?;
    old.validate(topology)?;
    let segments = pg.get_object_segments(&record.bucket, &record.key, record.version_id)?;
    if old.subject.bucket_incarnation != incarnation
        || old.references.len() != segments.len()
        || old
            .references
            .iter()
            .map(|reference| reference.logical_size)
            .sum::<u64>()
            != record.size
        || old.windows.iter().any(|window| {
            window.geometry.ec != record.ec
                || window.geometry.codec != record.encryption.payload_codec()
        })
    {
        return Err(ObjectError::DisplacedOwner);
    }
    let high_water = pg.conn.query_row(
        "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
        rusqlite::params![record.bucket, record.key], |row| row.get::<_, Option<u64>>(0),
    ).optional()?.flatten();
    old.subject.require_generation_high_water(high_water)?;
    for (segment, reference) in segments.iter().zip(&old.references) {
        let window = &old.windows[reference.window_index as usize];
        if segment.bucket != old.subject.bucket
            || segment.key != old.subject.key
            || segment.version_id != record.version_id
            || segment.segment_index != reference.window_index
            || segment.segment_okh != reference.segment_hash
            || segment.segment_vid != reference.physical_vid
            || segment.size != reference.logical_size
            || segment.data_pg_id != window.participants.data_pg.get()
            || segment.placement_cluster_epoch != window.participants.placement_epoch
            || segment.ec_k != window.geometry.ec.k
            || segment.ec_m != window.geometry.ec.m
            || crate::types::validate_segment_shard_integrity_for_profile(
                record.encryption.payload_profile(),
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
    let retired = old.reclaimed_object()?;
    Ok(vec![(old, retired)])
}

pub(in super::super) fn apply_in_open_transaction(
    pg: &PgStore,
    configured_topology: &PgTopology,
    envelope: &MetadataCommandEnvelope,
) -> Result<(), ObjectError> {
    if pg.conn.is_autocommit() {
        return Err(ObjectError::TransactionRequired);
    }
    let MetadataCommandPayload::CommitDirectPutObject(command) = envelope.payload() else {
        return Err(ObjectError::CommandBinding);
    };
    let owner = sql::load(
        &pg.conn,
        PgId::new(pg.pg_id),
        &command.generation_reservation_id,
    )?
    .ok_or(ObjectError::LiveResponsibility)?;
    let checked = owner.validate(configured_topology)?;
    if !envelope.verify_checksum()
        || envelope.id().pg_id() != owner.subject.object_pg
        || !command_follows(owner.subject.id.introduction, envelope.id())
        || owner.lifecycle != StagingLifecycle::Active
    {
        return Err(ObjectError::CommandBinding);
    }
    validate_live(pg, &owner, command)?;
    // No already-applied shortcut: the enclosing command acceptance layer owns
    // replay. A fresh command must transfer the exact displaced null payload.
    let displaced = match pg.get_object_version(
        &command.object.bucket,
        &command.object.key,
        command.object.version_id,
    ) {
        Err(MetadataError::ObjectNotFound) if command.stale_payload.is_none() => Vec::new(),
        Ok(StoredObject::DeleteMarker(_))
            if command.object.version_id.is_null() && command.stale_payload.is_none() =>
        {
            Vec::new()
        }
        Ok(StoredObject::Live(record)) => displaced_object(
            pg,
            configured_topology,
            &command.object,
            &command.bucket_write_reservation,
            &record,
            command
                .stale_payload
                .as_ref()
                .ok_or(ObjectError::DisplacedPayload)?,
        )?,
        Err(MetadataError::ObjectNotFound) => return Err(ObjectError::DisplacedPayload),
        Ok(_) => return Err(ObjectError::DisplacedPayload),
        Err(error) => return Err(error.into()),
    };
    let checked_displaced = displaced
        .iter()
        .map(|(old, retired)| -> Result<_, StagingInvariant> {
            Ok((
                old.validate(configured_topology)?,
                retired.validate(configured_topology)?,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let references = command
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| owner.selected_object_reference(command, index, segment))
        .collect::<Result<Vec<_>, _>>()?;
    let after = owner.published_standard(&references)?;
    let checked_after = after.validate(configured_topology)?;
    let savepoint = sql::CommandApplySavepoint::begin(&pg.conn)?;
    pg.apply_commit_direct_put_object_command(command)?;
    sql::publish_standard(
        &pg.conn,
        owner.subject.object_pg,
        configured_topology,
        &checked,
        &checked_after,
    )?;
    for (old, retired) in &checked_displaced {
        sql::retain_object_reclaim(
            &pg.conn,
            owner.subject.object_pg,
            configured_topology,
            old,
            retired,
        )?;
    }
    savepoint.commit()?;
    Ok(())
}
