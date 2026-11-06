// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh MPU completion/abort ownership components, not production dispatch.
//! Validate the complete original owner inventory before live metadata removal.
//! Null-version displacement joins the same transaction. Accepted-command replay
//! and physical cleanup remain separate.

use super::*;
use crate::metadata_command::{
    CommitMultipartObjectCommand, MetadataCommandEnvelope, MetadataCommandPayload,
    ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
};
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::PgMetadataStore;
use crate::types::{
    validate_segment_shard_integrity_for_profile, ListPartsReq, MultipartPartRecord,
    MultipartPartSegmentRecord, MultipartUploadRecord, ObjectPartRecord, StreamUploadRecord,
    StreamUploadSegmentRecord, TerminalStreamCleanupRecord,
};
use crate::{MetadataError, ObjectLayout, StoredObject, StreamUploadTarget, UploadState};
use rusqlite::OptionalExtension;

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum MultipartTerminalError {
    #[error("staging multipart terminal operation requires the enclosing command transaction")]
    TransactionRequired,
    #[error("staging multipart terminal operation does not bind the exact upload and command")]
    CommandBinding,
    #[error(
        "staging multipart terminal operation does not partition the complete durable payload"
    )]
    Manifest,
    #[error("staging multipart terminal operation has missing, crossed or unaccounted ownership")]
    Owner,
    #[error(
        "staging multipart terminal operation requires a displaced-payload ownership transfer"
    )]
    DisplacedPayload,
    #[error(transparent)]
    Displacement(#[from] object::ObjectError),
    #[error(transparent)]
    Invariant(#[from] StagingInvariant),
    #[error(transparent)]
    Persistence(#[from] sql::PersistenceError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error(transparent)]
    Database(#[from] rusqlite::Error),
}

#[derive(Clone, Copy)]
pub(super) enum Disposition {
    Selected,
    Omitted,
    Active,
}

impl Disposition {
    pub(super) fn transition(
        self,
        owner: &StagingOwnerRecord,
    ) -> Result<StagingOwnerRecord, StagingInvariant> {
        match self {
            Self::Selected => owner.completed_part(),
            Self::Omitted => owner.unreferenced_part(),
            Self::Active => owner.closed_unpublished(),
        }
    }
}

fn object_part(
    topology: &PgTopology,
    command: &CommitMultipartObjectCommand,
    part: &MultipartPartRecord,
) -> ObjectPartRecord {
    ObjectPartRecord {
        bucket: command.object.bucket.clone(),
        key: command.object.key.clone(),
        version_id: command.object.version_id,
        part_number: part.part_number,
        size: part.size,
        payload_crc64: part.payload_crc64,
        etag: part.etag.clone(),
        etag_kind: part.etag_kind,
        part_vid: part.part_vid,
        placement_cluster_epoch: part.placement_cluster_epoch,
        ec_k: part.ec_k,
        ec_m: part.ec_m,
        data_pg_id: topology
            .object_generation_multipart_part_data_pg(
                &command.object.bucket,
                &command.object.key,
                command.object.generation_id,
                part.part_number,
            )
            .get(),
        checksum: part.checksum.clone(),
    }
}

fn validate_owner(
    owner: &StagingOwnerRecord,
    upload: &MultipartUploadRecord,
    id: MetadataCommandId,
    incarnation: u64,
    topology: &PgTopology,
    high_water: Option<u64>,
) -> Result<(), MultipartTerminalError> {
    owner.validate(topology)?;
    let subject = &owner.subject;
    if subject.bucket != upload.bucket
        || subject.key != upload.key
        || subject.bucket_incarnation != incarnation
        || subject.object_pg != id.pg_id()
        || !command_follows(subject.id.introduction, id)
        || !matches!(&subject.target, StagingTarget::UploadPart { upload_id, upload_generation, .. }
            if *upload_id == upload.upload_id && *upload_generation == upload.object_generation_id)
        || owner
            .windows
            .iter()
            .any(|window| window.geometry.codec != upload.encryption.payload_codec())
    {
        return Err(MultipartTerminalError::Owner);
    }
    subject.require_generation_high_water(high_water)?;
    Ok(())
}

fn validate_part(
    owner: &StagingOwnerRecord,
    part: &MultipartPartRecord,
    segments: &[&MultipartPartSegmentRecord],
    upload: &MultipartUploadRecord,
) -> Result<(), MultipartTerminalError> {
    owner.require_published_part()?; // exact Part destination, not an Object/reclaim
    let placement_epoch = owner.part_placement_epoch()?;
    if owner.subject.payload_generation != part.part_vid
        || part.placement_cluster_epoch != placement_epoch
        || !matches!(owner.subject.target, StagingTarget::UploadPart { part_number, .. } if part_number == part.part_number)
        || owner.references.len() != segments.len()
        || owner.references.iter().map(|r| r.logical_size).sum::<u64>() != part.size
        || owner.windows.iter().any(|w| {
            w.geometry.ec
                != (EcShape {
                    k: part.ec_k,
                    m: part.ec_m,
                })
        })
    {
        return Err(MultipartTerminalError::Owner);
    }
    for (segment, reference) in segments.iter().zip(&owner.references) {
        let window = &owner.windows[reference.window_index as usize];
        if segment.bucket != upload.bucket
            || segment.key != upload.key
            || segment.upload_id != upload.upload_id
            || segment.version_id != crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64()
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
                upload.encryption.payload_profile(),
                segment.shard_integrity.as_ref(),
                window.geometry.ec,
                reference.stored_size,
                segment.segment_crc64,
            )
            .is_err()
        {
            return Err(MultipartTerminalError::Owner);
        }
    }
    Ok(())
}

struct UploadSnapshot {
    upload: MultipartUploadRecord,
    parts: Vec<MultipartPartRecord>,
    segments: Vec<MultipartPartSegmentRecord>,
    sessions: Vec<StreamUploadRecord>,
}

impl UploadSnapshot {
    fn load(pg: &PgStore, upload: &UploadId) -> Result<Self, MultipartTerminalError> {
        Ok(Self {
            upload: pg.get_multipart_upload(upload)?,
            parts: pg
                .list_multipart_parts(&ListPartsReq {
                    upload_id: upload.clone(),
                    part_number_marker: None,
                    max_parts: u32::MAX,
                })?
                .parts,
            segments: pg.get_all_multipart_part_segments_for_upload(upload)?,
            sessions: pg.list_stream_uploads_for_multipart_upload(upload)?,
        })
    }

    fn matches_sessions(
        &self,
        pg: &PgStore,
        sessions: &[TerminalStreamCleanupRecord],
        segments: &[StreamUploadSegmentRecord],
    ) -> Result<bool, MultipartTerminalError> {
        Ok(
            PgStore::stream_upload_cleanup_records_match(&self.sessions, sessions)
                && pg.list_stream_segments_for_sessions(&self.sessions)? == segments,
        )
    }

    fn prepare_transfers(
        &self,
        pg: &PgStore,
        topology: &PgTopology,
        id: MetadataCommandId,
        incarnation: u64,
        selected: &BTreeSet<u32>,
    ) -> Result<Vec<OwnerTransfer>, MultipartTerminalError> {
        let upload = &self.upload;
        let pg_id = PgId::new(pg.pg_id);
        let high_water = pg.conn.query_row(
            "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
            rusqlite::params![upload.bucket, upload.key], |row| row.get::<_, Option<u64>>(0),
        ).optional()?.flatten();
        if high_water.is_none_or(|value| value < upload.object_generation_id.get()) {
            return Err(MultipartTerminalError::Owner);
        }
        let mut by_part = std::collections::BTreeMap::<_, Vec<_>>::new();
        for segment in &self.segments {
            by_part
                .entry(segment.part_number)
                .or_default()
                .push(segment);
        }
        let durable_numbers: BTreeSet<_> = self.parts.iter().map(|part| part.part_number).collect();
        if by_part
            .keys()
            .any(|number| !durable_numbers.contains(number))
            || !selected.is_subset(&durable_numbers)
        {
            return Err(MultipartTerminalError::Manifest);
        }
        let mut transfers = Vec::new();
        let mut attempts = BTreeSet::new();
        for part in &self.parts {
            let owner = sql::owner_for_part(&pg.conn, pg_id, &upload.bucket, &upload.key, part)?;
            validate_owner(&owner, upload, id, incarnation, topology, high_water)?;
            validate_part(
                &owner,
                part,
                by_part.get(&part.part_number).map_or(&[], Vec::as_slice),
                upload,
            )?;
            let disposition = if selected.contains(&part.part_number) {
                Disposition::Selected
            } else {
                Disposition::Omitted
            };
            let after = disposition.transition(&owner)?;
            if !attempts.insert(owner.subject.id.attempt.clone()) {
                return Err(MultipartTerminalError::Owner);
            }
            transfers.push((disposition, owner, after));
        }
        for session in &self.sessions {
            let owner = sql::load(&pg.conn, pg_id, &session.session_id)?
                .ok_or(MultipartTerminalError::Owner)?;
            validate_owner(&owner, upload, id, incarnation, topology, high_water)?;
            let StagingTarget::UploadPart { part_number, .. } = owner.subject.target else {
                return Err(MultipartTerminalError::Owner);
            };
            let last = owner.windows.last().ok_or(MultipartTerminalError::Owner)?;
            if owner.lifecycle != StagingLifecycle::Active
                || session.bucket != upload.bucket
                || session.key != upload.key
                || session.bucket_incarnation_generation != owner.subject.bucket_incarnation
                || session.target
                    != (StreamUploadTarget::UploadPart {
                        upload_id: upload.upload_id.clone(),
                        part_number,
                    })
                || session.bucket_write_reservation.is_some()
                || session.encryption != upload.encryption
                || pg.load_stream_allocation_issuer(&session.session_id)? != owner.subject.issuer
                || (session.next_segment_vid != last.start && session.next_segment_vid != last.end)
                || pg.get_object_generation_reservation(
                    &upload.bucket,
                    &upload.key,
                    &session.session_id,
                )? != owner.subject.payload_generation
            {
                return Err(MultipartTerminalError::Owner);
            }
            let segments = pg.list_stream_segments(&session.session_id)?;
            if segments.len() != owner.references.len() {
                return Err(MultipartTerminalError::Owner);
            }
            for (segment, reference) in segments.iter().zip(&owner.references) {
                if owner.selected_stream_reference(segment)? != *reference {
                    return Err(MultipartTerminalError::Owner);
                }
            }
            let after = Disposition::Active.transition(&owner)?;
            if !attempts.insert(owner.subject.id.attempt.clone()) {
                return Err(MultipartTerminalError::Owner);
            }
            transfers.push((Disposition::Active, owner, after));
        }
        // A missing manifest/session must not hide an active or still-selected
        // owner. Previously closed/replaced attempts remain untouched obligations.
        for owner in sql::owners_for_upload(&pg.conn, pg_id, &upload.upload_id)? {
            validate_owner(&owner, upload, id, incarnation, topology, high_water)?;
            let needs_transfer = matches!(
                owner.lifecycle,
                StagingLifecycle::Active
                    | StagingLifecycle::Published
                    | StagingLifecycle::Retiring(StagingRetirement::PublishedRetained)
            );
            if needs_transfer != attempts.contains(&owner.subject.id.attempt) {
                return Err(MultipartTerminalError::Owner);
            }
        }

        Ok(transfers)
    }
}

type OwnerTransfer = (Disposition, StagingOwnerRecord, StagingOwnerRecord);

fn apply_transfers(
    pg: &PgStore,
    topology: &PgTopology,
    transfers: &[OwnerTransfer],
    displaced: &[(StagingOwnerRecord, StagingOwnerRecord)],
    apply_live: impl FnOnce() -> Result<(), MetadataError>,
) -> Result<(), MultipartTerminalError> {
    let checked = transfers
        .iter()
        .map(
            |(disposition, before, after)| -> Result<_, StagingInvariant> {
                Ok((
                    *disposition,
                    before.validate(topology)?,
                    after.validate(topology)?,
                ))
            },
        )
        .collect::<Result<Vec<_>, _>>()?;
    let checked_displaced = displaced
        .iter()
        .map(|(old, retired)| -> Result<_, StagingInvariant> {
            Ok((old.validate(topology)?, retired.validate(topology)?))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let savepoint = sql::CommandApplySavepoint::begin(&pg.conn)?;
    apply_live()?;
    sql::transfer_multipart_ownership(&pg.conn, PgId::new(pg.pg_id), topology, &checked)?;
    for (old, retired) in &checked_displaced {
        sql::retain_object_reclaim(&pg.conn, PgId::new(pg.pg_id), topology, old, retired)?;
    }
    savepoint.commit()?;
    Ok(())
}

fn complete(
    pg: &PgStore,
    topology: &PgTopology,
    envelope: &MetadataCommandEnvelope,
) -> Result<(), MultipartTerminalError> {
    let MetadataCommandPayload::CommitMultipartObject(command) = envelope.payload() else {
        return Err(MultipartTerminalError::CommandBinding);
    };
    let proof = &command.bucket_write_reservation;
    let pg_id = PgId::new(pg.pg_id);
    if !envelope.verify_checksum()
        || envelope.id().pg_id() != pg_id
        || topology.object_pg_for(&command.object.bucket, &command.object.key) != pg_id.get()
        || proof.cluster_epoch != envelope.id().cluster_epoch()
        || proof.operation_kind != COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND
        || proof.bucket != command.object.bucket
        || proof.target_context.as_deref() != Some(command.object.key.as_str())
        || proof.bucket_incarnation_generation == 0
    {
        return Err(MultipartTerminalError::CommandBinding);
    }
    let snapshot = UploadSnapshot::load(pg, &command.upload_id)?;
    let upload = &snapshot.upload;
    if upload.bucket != command.object.bucket
        || upload.key != command.object.key
        || upload.object_generation_id != command.object.generation_id
        || upload.state != UploadState::InProgress
        || command.parts.is_empty()
        || command.parts.len() > 10_000
        || command
            .parts
            .windows(2)
            .any(|pair| pair[0].part_number >= pair[1].part_number)
        || command.object.layout.parts_count() != Some(command.parts.len() as u32)
        || !matches!(
            command.object.layout,
            ObjectLayout::MultipartManifest { .. }
        )
        || command.object.ec != (EcShape { k: 0, m: 0 })
    {
        return Err(MultipartTerminalError::CommandBinding);
    }
    let displaced =
        match pg.get_object_version(&upload.bucket, &upload.key, command.object.version_id) {
            Err(MetadataError::ObjectNotFound) if command.stale_payload.is_none() => Vec::new(),
            Ok(StoredObject::DeleteMarker(_))
                if command.object.version_id.is_null() && command.stale_payload.is_none() =>
            {
                Vec::new()
            }
            Ok(StoredObject::Live(record)) => object::displaced_object(
                pg,
                topology,
                &command.object,
                proof,
                &record,
                command
                    .stale_payload
                    .as_ref()
                    .ok_or(MultipartTerminalError::DisplacedPayload)?,
            )?,
            Err(MetadataError::ObjectNotFound) => {
                return Err(MultipartTerminalError::DisplacedPayload)
            }
            Ok(_) => return Err(MultipartTerminalError::DisplacedPayload),
            Err(error) => return Err(error.into()),
        };
    let all_parts = &snapshot.parts;
    let all_segments = &snapshot.segments;
    let selected: BTreeSet<_> = command.parts.iter().map(|part| part.part_number).collect();
    let selected_parts: Vec<_> = all_parts
        .iter()
        .filter(|part| selected.contains(&part.part_number))
        .map(|part| object_part(topology, command, part))
        .collect();
    let omitted_parts: Vec<_> = all_parts
        .iter()
        .filter(|part| !selected.contains(&part.part_number))
        .cloned()
        .collect();
    let selected_segments: Vec<_> = all_segments
        .iter()
        .filter(|segment| selected.contains(&segment.part_number))
        .cloned()
        .map(|mut segment| {
            segment.version_id = command.object.version_id.to_u64();
            segment
        })
        .collect();
    let omitted_segments: Vec<_> = all_segments
        .iter()
        .filter(|segment| !selected.contains(&segment.part_number))
        .cloned()
        .collect();
    let total = command
        .parts
        .iter()
        .try_fold(0_u64, |sum, part| sum.checked_add(part.size));
    if command.parts != selected_parts
        || command.omitted_parts != omitted_parts
        || command.selected_streaming_segments != selected_segments
        || command.omitted_streaming_segments != omitted_segments
        || total != Some(command.object.size)
    {
        return Err(MultipartTerminalError::Manifest);
    }

    if !snapshot.matches_sessions(pg, &command.stream_uploads, &command.stream_upload_segments)? {
        return Err(MultipartTerminalError::Manifest);
    }
    let transfers = snapshot.prepare_transfers(
        pg,
        topology,
        envelope.id(),
        proof.bucket_incarnation_generation,
        &selected,
    )?;
    apply_transfers(pg, topology, &transfers, &displaced, || {
        pg.apply_commit_multipart_object_command(command)
    })
}

fn abort(
    pg: &PgStore,
    topology: &PgTopology,
    envelope: &MetadataCommandEnvelope,
) -> Result<(), MultipartTerminalError> {
    let MetadataCommandPayload::AbortMultipartUpload(command) = envelope.payload() else {
        return Err(MultipartTerminalError::CommandBinding);
    };
    let proof = &command.bucket_write_reservation;
    if !envelope.verify_checksum()
        || envelope.id().pg_id() != PgId::new(pg.pg_id)
        || topology.object_pg_for(&command.bucket, &command.key) != pg.pg_id
        || proof.cluster_epoch != envelope.id().cluster_epoch()
        || proof.operation_kind != ABORT_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND
        || proof.bucket != command.bucket
        || proof.target_context.as_deref() != Some(command.key.as_str())
        || proof.bucket_incarnation_generation == 0
        || !command.has_consistent_subject()
    {
        return Err(MultipartTerminalError::CommandBinding);
    }
    let snapshot = UploadSnapshot::load(pg, &command.upload_id)?;
    if snapshot.upload != command.cleanup.upload
        || !matches!(
            snapshot.upload.state,
            UploadState::InProgress | UploadState::Aborting
        )
    {
        return Err(MultipartTerminalError::CommandBinding);
    }
    if snapshot.parts != command.cleanup.parts
        || snapshot.segments != command.cleanup.streaming_segments
        || !snapshot.matches_sessions(
            pg,
            &command.cleanup.stream_uploads,
            &command.cleanup.stream_upload_segments,
        )?
    {
        return Err(MultipartTerminalError::Manifest);
    }
    let transfers = snapshot.prepare_transfers(
        pg,
        topology,
        envelope.id(),
        proof.bucket_incarnation_generation,
        &BTreeSet::new(),
    )?;
    apply_transfers(pg, topology, &transfers, &[], || {
        pg.apply_abort_multipart_upload_command(command)
    })
}

pub(in super::super) fn apply_in_open_transaction(
    pg: &PgStore,
    topology: &PgTopology,
    envelope: &MetadataCommandEnvelope,
) -> Result<(), MultipartTerminalError> {
    if pg.conn.is_autocommit() {
        return Err(MultipartTerminalError::TransactionRequired);
    }
    match envelope.payload() {
        MetadataCommandPayload::CommitMultipartObject(_) => complete(pg, topology, envelope),
        MetadataCommandPayload::AbortMultipartUpload(_) => abort(pg, topology, envelope),
        _ => Err(MultipartTerminalError::CommandBinding),
    }
}
