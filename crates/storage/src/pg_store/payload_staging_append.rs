// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Atomic append/reference/lookahead transaction component. Production dispatch
//! is deliberately not activated until descriptors are mandatory command fields
//! and custody/terminal transfers land with them. The enclosing command boundary
//! supplies recovery authorization and records its applied prefix. This module
//! never resamples clocks, local issuer lifetimes, placement or remote storage.

use super::*;
use crate::metadata_command::{AppendStreamSegmentCommand, MetadataCommandPayload};
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::PgMetadataStore;
use crate::{MetadataError, StreamUploadState, StreamUploadTarget};
use rusqlite::OptionalExtension;

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum AppendError {
    #[error("staging append requires the enclosing command transaction")]
    TransactionRequired,
    #[error("staging append does not bind the exact owner, command and next window")]
    CommandBinding,
    #[error("staging append has missing or changed live responsibility")]
    LiveResponsibility,
    #[error("staging append disagrees with the retained selected prefix")]
    SelectedPrefix,
    #[error(transparent)]
    Invariant(#[from] StagingInvariant),
    #[error(transparent)]
    Persistence(#[from] sql::PersistenceError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error(transparent)]
    Database(#[from] rusqlite::Error),
}

#[derive(Debug, PartialEq, Eq)]
pub(in super::super) enum AppendApplied {
    Appended,
    ExactExisting,
}

fn validate_live(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    command: &AppendStreamSegmentCommand,
    next: &StagingWindow,
) -> Result<(), AppendError> {
    let subject = &owner.subject;
    let session = pg.get_stream_upload(&subject.id.attempt)?;
    let target_matches = match (&subject.target, &session.target) {
        (StagingTarget::StreamPut, StreamUploadTarget::PutObject) => true,
        (
            StagingTarget::UploadPart {
                upload_id,
                upload_generation,
                part_number,
            },
            StreamUploadTarget::UploadPart {
                upload_id: session_upload,
                part_number: session_part,
            },
        ) => {
            let upload = pg.get_multipart_upload(upload_id)?;
            upload_id == session_upload
                && part_number == session_part
                && upload.bucket == subject.bucket
                && upload.key == subject.key
                && upload.object_generation_id == *upload_generation
                && upload.state == crate::UploadState::InProgress
                && upload.encryption == session.encryption
        }
        _ => false,
    };
    let proof_matches = match (
        &subject.target,
        &session.bucket_write_reservation,
        &owner.resources.creation,
    ) {
        (StagingTarget::StreamPut, Some(proof), ResourceResponsibility::Live(resource)) => {
            StagingBucketResource::capture(proof, resource.release_pg).identity == resource.identity
        }
        (StagingTarget::UploadPart { .. }, None, ResourceResponsibility::ReleaseDue(_)) => true,
        _ => false,
    };
    let current = owner
        .windows
        .last()
        .ok_or(AppendError::LiveResponsibility)?;
    if !target_matches
        || !proof_matches
        || session.bucket != subject.bucket
        || session.key != subject.key
        || session.bucket_incarnation_generation != subject.bucket_incarnation
        || session.target != command.target
        || session.state != StreamUploadState::InProgress
        || pg.load_stream_allocation_issuer(&subject.id.attempt)? != subject.issuer
        || (session.next_segment_vid != current.start && session.next_segment_vid != current.end)
        || owner
            .windows
            .iter()
            .any(|window| window.geometry.codec != session.encryption.payload_codec())
        || next.geometry.codec != session.encryption.payload_codec()
    {
        return Err(AppendError::LiveResponsibility);
    }
    let reservation_matches: bool = pg.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM object_generation_reservations WHERE reservation_id = ?1 \
         AND bucket = ?2 AND key = ?3 AND generation_id = ?4 AND created_at = ?5)",
        rusqlite::params![
            subject.id.attempt,
            subject.bucket,
            subject.key,
            subject.payload_generation.get(),
            session.created_at
        ],
        |row| row.get(0),
    )?;
    if !reservation_matches {
        return Err(AppendError::LiveResponsibility);
    }
    let high_water = pg.conn.query_row(
        "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
        rusqlite::params![subject.bucket, subject.key], |row| row.get::<_, Option<u64>>(0),
    ).optional()?.flatten();
    subject.require_generation_high_water(high_water)?;
    // Exact checksum/witness and idempotent-segment validation remains shared
    // with ordinary replica apply. It performs no mutation or remote lookup.
    pg.validate_append_stream_segment_command(command)?;
    let mut segments = pg.list_stream_segments(&subject.id.attempt)?;
    segments.sort_by_key(|segment| segment.segment_index);
    if segments.len() != owner.references.len() {
        return Err(AppendError::SelectedPrefix);
    }
    for (segment, reference) in segments.iter().zip(&owner.references) {
        if owner
            .selected_stream_reference(segment)
            .map_err(|_| AppendError::SelectedPrefix)?
            != *reference
        {
            return Err(AppendError::SelectedPrefix);
        }
    }
    Ok(())
}

// The descriptor's private binding is mandatory even for direct sibling calls.
// Live ownership is still reloaded here, not certified by the earlier binding.
pub(super) fn apply_in_open_transaction(
    pg: &PgStore,
    descriptor: descriptor::BoundAppendDescriptor<'_>,
) -> Result<AppendApplied, AppendError> {
    if pg.conn.is_autocommit() {
        return Err(AppendError::TransactionRequired);
    }
    let (configured_topology, envelope, expected_subject, next) = descriptor.inputs();
    let MetadataCommandPayload::AppendStreamSegment(command) = envelope.payload() else {
        return Err(AppendError::CommandBinding);
    };
    let owner = sql::load(&pg.conn, PgId::new(pg.pg_id), &command.segment.session_id)?
        .ok_or(AppendError::LiveResponsibility)?;
    if owner.subject != *expected_subject {
        return Err(AppendError::CommandBinding);
    }
    let checked = owner.validate(configured_topology)?;
    let subject = &owner.subject;
    if !envelope.verify_checksum()
        || envelope.id().pg_id() != subject.object_pg
        || !command_follows(subject.id.introduction, envelope.id())
        || owner.lifecycle != StagingLifecycle::Active
        || matches!(subject.target, StagingTarget::DirectPut)
        || command.bucket != subject.bucket
        || command.key != subject.key
        || next.owner != subject.id
        || next.issuer != subject.issuer
        || command.segment.segment_index.checked_add(1) != Some(next.index)
        || next.participants.placement_epoch > envelope.id().cluster_epoch()
        || command.segment.placement_cluster_epoch > envelope.id().cluster_epoch()
    {
        return Err(AppendError::CommandBinding);
    }
    next.validate(subject)?;
    validate_live(pg, &owner, command, next)?;
    let reference = owner
        .selected_stream_reference(&command.segment)
        .map_err(|_| AppendError::SelectedPrefix)?;
    let index = command.segment.segment_index as usize;
    if index < owner.references.len() {
        if owner.references[index] != reference || owner.windows.get(index + 1) != Some(next) {
            return Err(AppendError::SelectedPrefix);
        }
        // Preserve later appends and all original cutoffs. Exact replay does
        // not advance the local cursor or manufacture a replacement window.
        return Ok(AppendApplied::ExactExisting);
    }
    if index != owner.references.len() {
        return Err(AppendError::SelectedPrefix);
    }
    let mut after = owner.clone();
    after.references.push(reference);
    after.reserved_end = next.end;
    after.maximum_cutoff_ms = after.maximum_cutoff_ms.max(next.cutoff_ms);
    after.windows.push(next.clone());
    let checked_after = after.validate(configured_topology)?;
    let savepoint = sql::CommandApplySavepoint::begin(&pg.conn)?;
    pg.apply_append_stream_segment_command(command)?;
    sql::append_selection(
        &pg.conn,
        subject.object_pg,
        configured_topology,
        &checked,
        &checked_after,
    )?;
    savepoint.commit()?;
    Ok(AppendApplied::Appended)
}
