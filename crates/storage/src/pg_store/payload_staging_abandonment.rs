// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! UploadPart finalization-abandonment cleanup component. Tombstones remain
//! materialized-state no-ops. This transition belongs to a separate, certified
//! cleanup follow-up, before pending source/proof custody can be discarded.
//! The source is a mandatory component input, NOT an optional wire adapter:
//! command encoding, derivative authorization and dispatch must activate together.

use super::*;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::node_runtime::pg_store::{MetadataCommandStartupDisposition, PgStore};
use crate::node_runtime::traits::PgMetadataStore;
use crate::{MetadataError, StoreError};
use rusqlite::OptionalExtension;

#[path = "payload_staging_abandonment_source.rs"]
pub(super) mod source;

/// Custody only, never a physical-cleanup or already-applied-command receipt.
/// The enclosing acceptance boundary must still record this exact follow-up.
#[derive(Debug, PartialEq, Eq)]
pub(in super::super) enum AbandonmentOutcome {
    ClosedAndRetained,
    RetainedOnClosedOwner,
    AlreadyRetained,
}

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum AbandonmentError {
    #[error("staging abandonment cleanup requires the enclosing command transaction")]
    TransactionRequired,
    #[error("staging abandonment cleanup does not bind its source, owner and follow-up")]
    Subject,
    #[error("staging abandonment cleanup requires exact durable terminal abandonment")]
    NotAbandoned,
    #[error("staging abandonment cleanup found conflicting terminal responsibility")]
    TerminalResponsibility,
    #[error(transparent)]
    Closure(#[from] close::CloseError),
    #[error(transparent)]
    Invariant(#[from] StagingInvariant),
    #[error(transparent)]
    Persistence(#[from] sql::PersistenceError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Database(#[from] rusqlite::Error),
}

impl StagingOwnerRecord {
    pub(super) fn with_abandoned_part_finalization(
        &self,
        finalization: &StagingBucketResource,
    ) -> Result<Self, StagingInvariant> {
        if self.lifecycle == StagingLifecycle::Active
            || !matches!(self.subject.target, StagingTarget::UploadPart { .. })
        {
            return Err(StagingInvariant::Lifecycle);
        }
        finalization.validate(
            &self.subject,
            UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
        )?;
        let mut retained = self.clone();
        match &self.resources.finalization {
            ResourceResponsibility::NoResource
                if self.lifecycle == StagingLifecycle::Retiring(StagingRetirement::Unpublished) =>
            {
                retained.resources.finalization =
                    ResourceResponsibility::ReleaseDue(finalization.clone());
            }
            ResourceResponsibility::ReleaseDue(existing)
                if existing.identity != finalization.identity
                    && !self
                        .resources
                        .abandoned_finalizations
                        .iter()
                        .any(|proof| proof.identity == finalization.identity) =>
            {
                retained
                    .resources
                    .abandoned_finalizations
                    .push(finalization.clone());
            }
            _ => return Err(StagingInvariant::Resource),
        }
        Ok(retained)
    }
}

fn retain_on_terminal_owner(
    pg: &PgStore,
    topology: &PgTopology,
    owner: &StagingOwnerRecord,
    finalization: &StagingBucketResource,
) -> Result<AbandonmentOutcome, AbandonmentError> {
    // Absence is only a consistency check on an already validated terminal
    // owner. It never certifies abandonment, replay, or physical deletion.
    let live: bool = pg.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM stream_uploads WHERE session_id = ?1)
         OR EXISTS(SELECT 1 FROM stream_upload_segments WHERE session_id = ?1)
         OR EXISTS(SELECT 1 FROM object_generation_reservations WHERE reservation_id = ?1)",
        [&owner.subject.id.attempt],
        |row| row.get(0),
    )?;
    let high_water = pg.conn.query_row(
        "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
        rusqlite::params![owner.subject.bucket, owner.subject.key],
        |row| row.get::<_, Option<u64>>(0),
    ).optional()?.flatten();
    owner.subject.require_generation_high_water(high_water)?;
    if live {
        return Err(AbandonmentError::TerminalResponsibility);
    }
    finalization.validate(
        &owner.subject,
        UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
    )?;
    let primary = match &owner.resources.finalization {
        ResourceResponsibility::ReleaseDue(proof) => Some(proof),
        _ => None,
    };
    if primary
        .into_iter()
        .chain(&owner.resources.abandoned_finalizations)
        .any(|existing| {
            existing.identity == finalization.identity
                && existing.release_pg == finalization.release_pg
        })
    {
        // Preserve the original renewable evidence. This is custody, not an
        // AlreadyApplied certificate for this follow-up's command ID.
        return Ok(AbandonmentOutcome::AlreadyRetained);
    }
    let retained = owner.with_abandoned_part_finalization(finalization)?;
    sql::retain_abandoned_part_finalization(
        &pg.conn,
        owner.subject.object_pg,
        topology,
        &owner.validate(topology)?,
        &retained.validate(topology)?,
        finalization,
    )?;
    Ok(AbandonmentOutcome::RetainedOnClosedOwner)
}

fn apply_part_closure_in_open_transaction(
    pg: &PgStore,
    topology: &PgTopology,
    node_id: u32,
    abandoned: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
) -> Result<AbandonmentOutcome, AbandonmentError> {
    if pg.conn.is_autocommit() {
        return Err(AbandonmentError::TransactionRequired);
    }
    let (
        MetadataCommandPayload::CommitStreamPart(source),
        MetadataCommandPayload::AbortStreamUpload(abort),
    ) = (abandoned.payload(), cleanup.payload())
    else {
        return Err(AbandonmentError::Subject);
    };
    if !abandoned.verify_checksum()
        || !cleanup.verify_checksum()
        || abandoned.id().pg_id().get() != pg.pg_id
        || cleanup.id().pg_id() != abandoned.id().pg_id()
        // A historical tombstone authenticates the source, not the epoch of
        // future cleanup. Log positions restart at one in each new epoch.
        || !command_follows(abandoned.id(), cleanup.id())
        || !source.has_consistent_subject()
        || abort.bucket != source.bucket
        || abort.key != source.key
        || abort.session_id != source.session_id
        || abort.stream_create_bucket_write_reservation.is_some()
    {
        return Err(AbandonmentError::Subject);
    }

    // A raw tombstone row, pending slot, expired lease or missing command is
    // insufficient. Reuse the historical terminal-chain/receipt validator.
    if !matches!(
        pg.metadata_command_startup_disposition(node_id, abandoned)?,
        MetadataCommandStartupDisposition::Abandoned { .. }
    ) {
        return Err(AbandonmentError::NotAbandoned);
    }
    if pg
        .pending_metadata_command_slot_any_epoch(node_id)?
        .is_some_and(|slot| slot.id == abandoned.id() && slot.publication_started)
    {
        return Err(AbandonmentError::NotAbandoned);
    }

    let owner = sql::load(&pg.conn, PgId::new(pg.pg_id), &source.session_id)?
        .ok_or(AbandonmentError::Subject)?;
    owner.validate(topology)?;
    if owner.subject.bucket != source.bucket
        || owner.subject.key != source.key
        || !matches!(&owner.subject.target, StagingTarget::UploadPart { upload_id, upload_generation, part_number }
            if *upload_id == source.upload.upload_id && *upload_generation == source.upload.object_generation_id && *part_number == source.part.part_number)
        || owner.subject.payload_generation != source.part.part_vid
        || !command_follows(owner.subject.id.introduction, abandoned.id())
        || !source
            .bucket_write_reservation
            .matches_exact_mutation_subject(
                abandoned.id().cluster_epoch(),
                &source.bucket,
                UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
                Some(source.key.as_str()),
            )
    {
        return Err(AbandonmentError::Subject);
    }
    let finalization = StagingBucketResource::capture(
        &source.bucket_write_reservation,
        PgId::new(topology.bucket_pg_for(&source.bucket)),
    );
    if owner.lifecycle != StagingLifecycle::Active {
        // A terminal follow-up transfers only the authenticated source's proof.
        // It must not carry payload-deletion inputs or replay an old abort against
        // a published owner. No source encryption/segment metadata is applied.
        if !abort.staged_segments.is_empty() {
            return Err(AbandonmentError::TerminalResponsibility);
        }
        return retain_on_terminal_owner(pg, topology, &owner, &finalization);
    }
    if pg.get_stream_upload(&source.session_id)?.encryption != source.upload.encryption {
        return Err(AbandonmentError::Subject);
    }
    // Do not require the MPU or its previously committed part to remain equal
    // to the failed publication snapshot: that change may have caused abandonment.
    // Ordinary closure below validates the actual session, full staged prefix,
    // immutable windows, issuer, generation fence and creation-proof custody.
    let closed = owner.closed_unpublished()?;
    let retained = closed.with_abandoned_part_finalization(&finalization)?;
    let checked_closed = closed.validate(topology)?;
    let checked_retained = retained.validate(topology)?;
    let savepoint = sql::CommandApplySavepoint::begin(&pg.conn)?;
    close::apply_in_open_transaction(pg, topology, cleanup)?;
    sql::retain_abandoned_part_finalization(
        &pg.conn,
        PgId::new(pg.pg_id),
        topology,
        &checked_closed,
        &checked_retained,
        &finalization,
    )?;
    savepoint.commit()?;
    Ok(AbandonmentOutcome::ClosedAndRetained)
}
