// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Ordinary unpublished release/abort transaction component. Not yet called by
//! production dispatch: owner introduction, mandatory command descriptors and
//! the remaining terminal/custody transitions must activate together.
//!
//! This closes allocation and retains cleanup responsibility, not deletion
//! authority or a cleanup receipt. The enclosing acceptance boundary handles
//! exact already-applied commands; an absent session/reservation or terminal
//! owner alone cannot certify replay of an arbitrary command. Finalization
//! abandonment (with its separate proof) is a different transition.

use super::*;
use crate::metadata_command::{
    AbortStreamUploadCommand, MetadataCommandEnvelope, MetadataCommandPayload,
    ReleaseObjectGenerationCommand,
};
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::PgMetadataStore;
use crate::{MetadataError, StreamUploadState, StreamUploadTarget};
use rusqlite::OptionalExtension;

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum CloseError {
    #[error("staging closure requires the enclosing command transaction")]
    TransactionRequired,
    #[error("staging closure does not bind the exact unpublished owner and command")]
    CommandBinding,
    #[error("staging closure has missing or changed live responsibility")]
    LiveResponsibility,
    #[error("staging closure disagrees with the retained selected prefix")]
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

enum Closure<'a> {
    Direct(&'a ReleaseObjectGenerationCommand),
    Stream(&'a AbortStreamUploadCommand),
}

impl<'a> Closure<'a> {
    fn from_envelope(envelope: &'a MetadataCommandEnvelope) -> Result<Self, CloseError> {
        match envelope.payload() {
            MetadataCommandPayload::ReleaseObjectGeneration(command) => Ok(Self::Direct(command)),
            MetadataCommandPayload::AbortStreamUpload(command) => Ok(Self::Stream(command)),
            _ => Err(CloseError::CommandBinding),
        }
    }

    fn subject(&self) -> (&BucketName, &ObjectKey, &SessionId) {
        match self {
            Self::Direct(command) => (&command.bucket, &command.key, &command.reservation_id),
            Self::Stream(command) => (&command.bucket, &command.key, &command.session_id),
        }
    }

    fn validate_live(&self, pg: &PgStore, owner: &StagingOwnerRecord) -> Result<(), CloseError> {
        let subject = &owner.subject;
        if self.subject() != (&subject.bucket, &subject.key, &subject.id.attempt) {
            return Err(CloseError::CommandBinding);
        }
        let generation = pg.get_object_generation_reservation(
            &subject.bucket,
            &subject.key,
            &subject.id.attempt,
        )?;
        if generation != subject.payload_generation {
            return Err(CloseError::LiveResponsibility);
        }
        let high_water = pg.conn.query_row(
            "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
            rusqlite::params![subject.bucket, subject.key], |row| row.get::<_, Option<u64>>(0),
        ).optional()?.flatten();
        subject.require_generation_high_water(high_water)?;
        match (self, &subject.target) {
            (Self::Direct(_), StagingTarget::DirectPut) => {
                match pg.get_stream_upload(&subject.id.attempt) {
                    Err(MetadataError::StreamSessionNotFound { .. }) => Ok(()),
                    Err(error) => Err(error.into()),
                    Ok(_) => Err(CloseError::LiveResponsibility),
                }
            }
            (
                Self::Stream(command),
                StagingTarget::StreamPut | StagingTarget::UploadPart { .. },
            ) => Self::validate_stream(pg, owner, command),
            _ => Err(CloseError::CommandBinding),
        }
    }

    fn validate_stream(
        pg: &PgStore,
        owner: &StagingOwnerRecord,
        command: &AbortStreamUploadCommand,
    ) -> Result<(), CloseError> {
        let subject = &owner.subject;
        let session = pg.get_stream_upload(&subject.id.attempt)?;
        let target_matches = match (&subject.target, &session.target) {
            (StagingTarget::StreamPut, StreamUploadTarget::PutObject) => true,
            (
                StagingTarget::UploadPart {
                    upload_id,
                    part_number,
                    ..
                },
                StreamUploadTarget::UploadPart {
                    upload_id: session_upload,
                    part_number: session_part,
                },
            ) => upload_id == session_upload && part_number == session_part,
            _ => false,
        };
        // Parent MPU existence/availability is not a cleanup prerequisite. This
        // closes only the exact session/part generation; it cannot remove a
        // previously committed part or adopt another upload's responsibility.
        let proof_matches = match (
            &subject.target,
            &session.bucket_write_reservation,
            &owner.resources.creation,
        ) {
            (StagingTarget::StreamPut, Some(proof), ResourceResponsibility::Live(resource)) => {
                StagingBucketResource::capture(proof, resource.release_pg).identity
                    == resource.identity
                    && command
                        .stream_create_bucket_write_reservation
                        .as_ref()
                        .is_some_and(|captured| captured.has_same_stable_identity(proof))
            }
            (StagingTarget::UploadPart { .. }, None, ResourceResponsibility::ReleaseDue(_)) => {
                command.stream_create_bucket_write_reservation.is_none()
            }
            _ => false,
        };
        let current = owner.windows.last().ok_or(CloseError::LiveResponsibility)?;
        if !target_matches
            || !proof_matches
            || session.bucket != subject.bucket
            || session.key != subject.key
            || session.bucket_incarnation_generation != subject.bucket_incarnation
            || session.state != StreamUploadState::InProgress
            || pg.load_stream_allocation_issuer(&subject.id.attempt)? != subject.issuer
            || (session.next_segment_vid != current.start
                && session.next_segment_vid != current.end)
            || owner
                .windows
                .iter()
                .any(|window| window.geometry.codec != session.encryption.payload_codec())
        {
            return Err(CloseError::LiveResponsibility);
        }
        let mut segments = pg.list_stream_segments(&subject.id.attempt)?;
        segments.sort_by_key(|segment| segment.segment_index);
        if segments != command.staged_segments || segments.len() != owner.references.len() {
            return Err(CloseError::SelectedPrefix);
        }
        for (segment, reference) in segments.iter().zip(&owner.references) {
            if owner.selected_stream_reference(segment)? != *reference {
                return Err(CloseError::SelectedPrefix);
            }
        }
        Ok(())
    }

    fn apply_live(&self, pg: &PgStore) -> Result<(), MetadataError> {
        match self {
            Self::Direct(command) => pg.apply_release_object_generation_command(command),
            Self::Stream(command) => pg.apply_abort_stream_upload_command(command),
        }
    }
}

pub(in super::super) fn apply_in_open_transaction(
    pg: &PgStore,
    configured_topology: &PgTopology,
    envelope: &MetadataCommandEnvelope,
) -> Result<(), CloseError> {
    if pg.conn.is_autocommit() {
        return Err(CloseError::TransactionRequired);
    }
    let bound = Closure::from_envelope(envelope)?;
    let owner = sql::load(&pg.conn, PgId::new(pg.pg_id), bound.subject().2)?
        .ok_or(CloseError::LiveResponsibility)?;
    let checked = owner.validate(configured_topology)?;
    if !envelope.verify_checksum()
        || envelope.id().pg_id() != owner.subject.object_pg
        || !command_follows(owner.subject.id.introduction, envelope.id())
        || owner.lifecycle != StagingLifecycle::Active
    {
        return Err(CloseError::CommandBinding);
    }
    bound.validate_live(pg, &owner)?;
    let closed = owner.closed_unpublished()?;
    let checked_closed = closed.validate(configured_topology)?;
    let savepoint = sql::CommandApplySavepoint::begin(&pg.conn)?;
    bound.apply_live(pg)?;
    sql::close_unpublished(
        &pg.conn,
        owner.subject.object_pg,
        configured_topology,
        &checked,
        &checked_closed,
    )?;
    savepoint.commit()?;
    Ok(())
}
