// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Transaction component for reserve/create command integration. The descriptor
//! will be a mandatory command field; current command dispatch does NOT call
//! this component until encoding, custody and terminal transfers land together.
//!
//! This is deterministic replica application, not recovery authorization. The
//! enclosing command acceptance/recovery boundary must authorize the envelope
//! (including any reissue) and record its applied prefix in the same transaction.
//! No local issuer, clock, renewable route or cross-PG availability is sampled.

use super::*;
use crate::metadata_command::{
    CreateStreamUploadCommand, MetadataCommandEnvelope, MetadataCommandPayload,
    ReserveObjectGenerationCommand,
};
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::PgMetadataStore;
use crate::types::StreamUploadCommandRecord;
use crate::{MetadataError, StreamUploadState, StreamUploadTarget};
use rusqlite::OptionalExtension;

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum IntroductionError {
    #[error("staging introduction requires the enclosing command transaction")]
    TransactionRequired,
    #[error("staging introduction does not bind the exact reserve/create command")]
    CommandBinding,
    #[error("staging introduction cannot adopt an existing reservation or session")]
    IndependentlyOwned,
    #[error("staging introduction conflicts with the retained owner")]
    OwnerConflict,
    #[error("staging introduction replay has missing or changed live responsibility")]
    LiveResponsibility,
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
pub(in super::super) enum IntroductionApplied {
    Created,
    ExactExisting,
}

/// Keeps command binding separate from validation of a standalone owner record.
/// Neither this private borrowed view nor an exact owner authorizes replay after
/// its live generation/session responsibility has been removed.
enum BoundIntroduction<'a> {
    Direct(&'a ReserveObjectGenerationCommand),
    Stream(&'a CreateStreamUploadCommand),
}

impl<'a> BoundIntroduction<'a> {
    fn bind(
        command: &'a MetadataCommandEnvelope,
        owner: &StagingOwnerRecord,
    ) -> Result<Self, IntroductionError> {
        let subject = &owner.subject;
        let origin = subject.id.introduction;
        // Reissues preserve the original revision. Ordering is only a necessary
        // check here; authorization is the enclosing recovery boundary's job.
        if !command.verify_checksum()
            || command.id().pg_id() != subject.object_pg
            || (command.id() != origin && !command_follows(origin, command.id()))
            || owner.lifecycle != StagingLifecycle::Active
            || owner.windows.len() != 1
            || !owner.references.is_empty()
        {
            return Err(IntroductionError::CommandBinding);
        }
        match (command.payload(), &subject.target) {
            (
                MetadataCommandPayload::ReserveObjectGeneration(reserve),
                StagingTarget::DirectPut,
            ) if reserve.bucket == subject.bucket
                && reserve.key == subject.key
                && reserve.reservation_id == subject.id.attempt
                && reserve.generation_id == subject.payload_generation =>
            {
                Ok(Self::Direct(reserve))
            }
            (MetadataCommandPayload::CreateStreamUpload(create), target) => {
                let target_matches = match (target, &create.session.target) {
                    (StagingTarget::StreamPut, StreamUploadTarget::PutObject) => true,
                    (
                        StagingTarget::UploadPart {
                            upload_id,
                            part_number,
                            ..
                        },
                        StreamUploadTarget::UploadPart {
                            upload_id: command_upload,
                            part_number: command_part,
                        },
                    ) => upload_id == command_upload && part_number == command_part,
                    _ => false,
                };
                let creation = match &owner.resources.creation {
                    ResourceResponsibility::Live(proof)
                    | ResourceResponsibility::ReleaseDue(proof) => proof,
                    ResourceResponsibility::NoResource => {
                        return Err(IntroductionError::CommandBinding);
                    }
                };
                if !target_matches
                    || create.session.bucket != subject.bucket
                    || create.session.key != subject.key
                    || create.session.session_id != subject.id.attempt
                    || create.session.bucket_incarnation_generation != subject.bucket_incarnation
                    || create.session.state != StreamUploadState::InProgress
                    || create.payload_generation != subject.payload_generation
                    || create.initial_next_segment_vid != owner.windows[0].start
                    || create.allocation_issuer != subject.issuer
                    || create.session.encryption.payload_codec() != owner.windows[0].geometry.codec
                    || *creation
                        != StagingBucketResource::capture(
                            &create.bucket_write_reservation,
                            creation.release_pg,
                        )
                {
                    return Err(IntroductionError::CommandBinding);
                }
                Ok(Self::Stream(create))
            }
            _ => Err(IntroductionError::CommandBinding),
        }
    }

    fn created_at(&self) -> u64 {
        match self {
            Self::Direct(command) => command.created_at_millis,
            Self::Stream(command) => command.session.created_at,
        }
    }

    fn apply_live(&self, pg: &PgStore) -> Result<(), MetadataError> {
        // Invoke only the payload validated by this borrowed view, not generic
        // dispatch (which will eventually delegate introduction back here).
        match self {
            Self::Direct(command) => pg.apply_reserve_object_generation_command(command),
            Self::Stream(command) => pg.apply_create_stream_upload_command(command),
        }
    }

    fn validate_upload(
        &self,
        pg: &PgStore,
        owner: &StagingOwnerRecord,
    ) -> Result<(), IntroductionError> {
        if let StagingTarget::UploadPart {
            upload_id,
            upload_generation,
            ..
        } = &owner.subject.target
        {
            let upload = pg.get_multipart_upload(upload_id)?;
            let Self::Stream(command) = self else {
                return Err(IntroductionError::CommandBinding);
            };
            if upload.bucket != owner.subject.bucket
                || upload.key != owner.subject.key
                || upload.object_generation_id != *upload_generation
                || upload.state != crate::UploadState::InProgress
                || upload.encryption != command.session.encryption
            {
                return Err(IntroductionError::CommandBinding);
            }
        }
        Ok(())
    }

    fn validate_existing(
        &self,
        pg: &PgStore,
        owner: &StagingOwnerRecord,
    ) -> Result<(), IntroductionError> {
        let subject = &owner.subject;
        let high_water = pg.conn.query_row(
            "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
            rusqlite::params![subject.bucket, subject.key],
            |row| row.get::<_, Option<u64>>(0),
        ).optional()?.flatten();
        subject.require_generation_high_water(high_water)?;
        let generation_matches: bool = pg.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM object_generation_reservations \
             WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3 \
             AND generation_id = ?4 AND created_at = ?5)",
            rusqlite::params![
                subject.id.attempt,
                subject.bucket,
                subject.key,
                subject.payload_generation.get(),
                self.created_at()
            ],
            |row| row.get(0),
        )?;
        if !generation_matches {
            return Err(IntroductionError::LiveResponsibility);
        }
        let session = match pg.get_stream_upload(&subject.id.attempt) {
            Ok(session) => Some(session),
            Err(MetadataError::StreamSessionNotFound { .. }) => None,
            Err(error) => return Err(error.into()),
        };
        match (self, session) {
            (Self::Direct(_), None) => Ok(()),
            (Self::Stream(command), Some(session)) => {
                let proof_matches =
                    match (&command.session.target, &session.bucket_write_reservation) {
                        (StreamUploadTarget::PutObject, Some(proof)) => {
                            proof.has_same_stable_identity(&command.bucket_write_reservation)
                        }
                        (StreamUploadTarget::UploadPart { .. }, None) => true,
                        _ => false,
                    };
                if StreamUploadCommandRecord::from(&session) != command.session
                    || session.next_segment_vid != command.initial_next_segment_vid
                    || session.cleanup_after != command.cleanup_after
                    || pg.load_stream_allocation_issuer(&subject.id.attempt)?
                        != command.allocation_issuer
                    || !proof_matches
                {
                    return Err(IntroductionError::LiveResponsibility);
                }
                Ok(())
            }
            _ => Err(IntroductionError::LiveResponsibility),
        }
    }
}

/// Pure correspondence validation shared by pending-descriptor projection and
/// application. It is not command acceptance or a durable replay certificate.
pub(super) fn validate_command(
    command: &MetadataCommandEnvelope,
    owner: &StagingOwnerRecord,
) -> Result<(), IntroductionError> {
    BoundIntroduction::bind(command, owner).map(|_| ())
}

// The descriptor's private binding is mandatory even for direct sibling calls.
// No independently replaceable command, owner or topology reaches application.
pub(super) fn apply_in_open_transaction(
    pg: &PgStore,
    descriptor: descriptor::BoundIntroductionDescriptor<'_>,
) -> Result<IntroductionApplied, IntroductionError> {
    if pg.conn.is_autocommit() {
        return Err(IntroductionError::TransactionRequired);
    }
    let (configured_topology, command, owner) = descriptor.inputs();
    let checked = owner.validate(configured_topology)?;
    if owner.subject.object_pg.get() != pg.pg_id {
        return Err(IntroductionError::CommandBinding);
    }
    let bound = BoundIntroduction::bind(command, owner)?;
    bound.validate_upload(pg, owner)?;
    match sql::load(&pg.conn, PgId::new(pg.pg_id), &owner.subject.id.attempt)? {
        Some(existing) if existing == *owner => {
            bound.validate_existing(pg, owner)?;
            return Ok(IntroductionApplied::ExactExisting);
        }
        Some(_) => return Err(IntroductionError::OwnerConflict),
        None => {}
    }
    let independently_owned: bool = pg.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM object_generation_reservations WHERE reservation_id = ?1) \
         OR EXISTS(SELECT 1 FROM stream_uploads WHERE session_id = ?1)",
        [&owner.subject.id.attempt],
        |row| row.get(0),
    )?;
    if independently_owned {
        return Err(IntroductionError::IndependentlyOwned);
    }

    // No commit/fsync: this nests inside the command's existing transaction.
    // Roll back the generation high-water, reservation and session too if any
    // owner child fails; callers cannot accidentally commit a partial apply.
    let savepoint = sql::CommandApplySavepoint::begin(&pg.conn)?;
    bound.apply_live(pg)?;
    sql::insert(&pg.conn, PgId::new(pg.pg_id), configured_topology, &checked)?;
    savepoint.commit()?;
    Ok(IntroductionApplied::Created)
}
