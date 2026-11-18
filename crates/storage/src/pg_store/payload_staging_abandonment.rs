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

pub(super) use super::command_context::finalization_source as source;

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
    #[error("staging abandonment candidate topology differs from the configured topology")]
    Topology,
    #[error("staging abandonment reissue must advance the same PG")]
    ReissuePosition,
    #[error(transparent)]
    Source(#[from] source::SourceError),
    #[error("staging abandonment cleanup requires the enclosing command transaction")]
    TransactionRequired,
    #[error("staging abandonment cleanup does not bind its source, owner and follow-up")]
    Subject,
    #[error("staging abandonment cleanup requires exact durable terminal abandonment")]
    NotAbandoned,
    #[error("staging abandonment cleanup found conflicting terminal responsibility")]
    TerminalResponsibility,
    #[error(
        "staging abandonment published custody differs from the authenticated source manifest"
    )]
    PublishedManifest,
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

fn terminal_retention(
    pg: &PgStore,
    topology: &PgTopology,
    owner: &StagingOwnerRecord,
    finalization: &StagingBucketResource,
) -> Result<Option<StagingOwnerRecord>, AbandonmentError> {
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
        return Ok(None);
    }
    let retained = owner.with_abandoned_part_finalization(finalization)?;
    retained.validate(topology)?;
    Ok(Some(retained))
}

fn retain_on_terminal_owner(
    pg: &PgStore,
    topology: &PgTopology,
    owner: &StagingOwnerRecord,
    finalization: &StagingBucketResource,
) -> Result<AbandonmentOutcome, AbandonmentError> {
    let Some(retained) = terminal_retention(pg, topology, owner, finalization)? else {
        return Ok(AbandonmentOutcome::AlreadyRetained);
    };
    sql::retain_abandoned_part_finalization(
        pg,
        owner.subject.object_pg,
        topology,
        &owner.validate(topology)?,
        &retained.validate(topology)?,
        finalization,
    )?;
    Ok(AbandonmentOutcome::RetainedOnClosedOwner)
}

pub(super) fn validate_part_closure_subject(
    topology: &PgTopology,
    abandoned: &MetadataCommandEnvelope,
    cleanup: &MetadataCommandEnvelope,
) -> Result<(), AbandonmentError> {
    let (
        MetadataCommandPayload::CommitStreamPart(source),
        MetadataCommandPayload::AbortStreamUpload(abort),
    ) = (abandoned.payload(), cleanup.payload())
    else {
        return Err(AbandonmentError::Subject);
    };
    if !abandoned.verify_checksum()
        || !cleanup.verify_checksum()
        || topology.object_pg_for(&source.bucket, &source.key) != abandoned.id().pg_id().get()
        || cleanup.id().pg_id() != abandoned.id().pg_id()
        // A historical tombstone authenticates the source, not the epoch of
        // future cleanup. Log positions restart at one in each new epoch.
        || !command_follows(abandoned.id(), cleanup.id())
        || !source.has_consistent_subject()
        || abort.bucket != source.bucket
        || abort.key != source.key
        || abort.session_id != source.session_id
        || !abort.has_consistent_segment_subjects()
        || abort.stream_create_bucket_write_reservation.is_some()
        || !source.bucket_write_reservation.matches_exact_mutation_subject(
            abandoned.id().cluster_epoch(),
            &source.bucket,
            UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
            Some(source.key.as_str()),
        )
    {
        return Err(AbandonmentError::Subject);
    }
    Ok(())
}

// Shared by read-only primary inspection and replica apply. The bound input
// prevents independent substitution of the source, follow-up or topology.
// Success is an observation in THIS transaction, never cached apply authority.
pub(super) fn require_abandoned_source_in_open_transaction(
    pg: &PgStore,
    node_id: u32,
    bound: &source::BoundPartCleanup<'_>,
) -> Result<(), AbandonmentError> {
    pg.require_usable_connection()?;
    if pg.conn.is_autocommit() {
        return Err(AbandonmentError::TransactionRequired);
    }
    let (_, abandoned, _) = bound.inputs();
    if abandoned.id().pg_id().get() != pg.pg_id {
        return Err(AbandonmentError::Subject);
    }
    // A raw tombstone row, pending slot, expired lease or missing command is
    // insufficient. Reuse the historical terminal-chain/receipt validator.
    if !matches!(
        bound.inspect_source_terminal(pg, node_id)?,
        MetadataCommandStartupDisposition::Abandoned { .. }
    ) {
        return Err(AbandonmentError::NotAbandoned);
    }
    if bound.source_has_started_publication(pg)? {
        return Err(AbandonmentError::NotAbandoned);
    }
    Ok(())
}

// The deepest mutation boundary accepts only the private, source-bound view.
// A caller cannot substitute another source, cleanup command or topology here.
pub(super) fn apply_part_closure_in_open_transaction(
    pg: &PgStore,
    node_id: u32,
    bound: source::BoundPartCleanup<'_>,
) -> Result<AbandonmentOutcome, AbandonmentError> {
    require_abandoned_source_in_open_transaction(pg, node_id, &bound)?;
    let (topology, abandoned, cleanup) = bound.inputs();
    let (
        MetadataCommandPayload::CommitStreamPart(source),
        MetadataCommandPayload::AbortStreamUpload(abort),
    ) = (abandoned.payload(), cleanup.payload())
    else {
        unreachable!("private bound cleanup validates both command kinds");
    };

    let owner = load_source_owner(pg, topology, abandoned)?;
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
    let savepoint = sql::CommandApplySavepoint::begin(pg)?;
    let result = (|| {
        close::apply_in_open_transaction(pg, topology, cleanup)?;
        sql::retain_abandoned_part_finalization(
            pg,
            PgId::new(pg.pg_id),
            topology,
            &checked_closed,
            &checked_retained,
            &finalization,
        )?;
        Ok(AbandonmentOutcome::ClosedAndRetained)
    })();
    savepoint.finish(result)
}

fn load_source_owner(
    pg: &PgStore,
    topology: &PgTopology,
    abandoned: &MetadataCommandEnvelope,
) -> Result<StagingOwnerRecord, AbandonmentError> {
    let MetadataCommandPayload::CommitStreamPart(source) = abandoned.payload() else {
        return Err(AbandonmentError::Subject);
    };
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
    Ok(owner)
}

/// Validate retained custody after exact cleanup application, without producing
/// physical exclusion, release confirmation, or permission to retire a slot.
/// The caller separately authenticates the complete cleanup's Applied receipt.
pub(super) fn inspect_terminal_custody_in_open_transaction(
    pg: &PgStore,
    node_id: u32,
    bound: source::BoundPartCleanup<'_>,
) -> Result<(usize, usize, usize), AbandonmentError> {
    require_abandoned_source_in_open_transaction(pg, node_id, &bound)?;
    let (topology, source, cleanup) = bound.inputs();
    let owner = load_source_owner(pg, topology, source)?;
    let MetadataCommandPayload::CommitStreamPart(source) = source.payload() else {
        unreachable!("bound source kind");
    };
    let MetadataCommandPayload::AbortStreamUpload(cleanup) = cleanup.payload() else {
        unreachable!("bound cleanup kind");
    };
    if owner.lifecycle == StagingLifecycle::Active {
        return Err(AbandonmentError::TerminalResponsibility);
    }
    let proof = StagingBucketResource::capture(
        &source.bucket_write_reservation,
        PgId::new(topology.bucket_pg_for(&source.bucket)),
    );
    // None means exact stable proof custody already exists. Some would describe
    // a new transfer: inspection must reject, never silently perform or infer it.
    if terminal_retention(pg, topology, &owner, &proof)?.is_some() {
        return Err(AbandonmentError::TerminalResponsibility);
    }
    if matches!(
        owner.lifecycle,
        StagingLifecycle::Published
            | StagingLifecycle::Retiring(
                StagingRetirement::PublishedRetained | StagingRetirement::PublishedUnreferenced
            )
    ) {
        require_published_source_manifest(&owner, source)?;
    }
    if owner.lifecycle == StagingLifecycle::Retiring(StagingRetirement::Unpublished) {
        // Even an empty unpublished manifest owns exactly one unused lookahead
        // window. Coherent allocator summaries cannot authorize extra windows.
        if owner.windows.len() != cleanup.staged_segments.len() + 1 {
            return Err(AbandonmentError::TerminalResponsibility);
        }
        for (index, segment) in cleanup.staged_segments.iter().enumerate() {
            if segment.segment_index as usize != index {
                return Err(AbandonmentError::Subject);
            }
            // Closure removed stream references, but retained every physical
            // window, including the final unused lookahead window.
            owner.selected_stream_reference(segment)?;
        }
    } else if !cleanup.staged_segments.is_empty() {
        return Err(AbandonmentError::TerminalResponsibility);
    }
    Ok((
        owner.windows.len(),
        owner.references.len(),
        owner.resources.abandoned_finalizations.len(),
    ))
}

/// A published cleanup has no abort manifest. Bind the retained projection to
/// the authenticated finalizer instead, even after publication transferred its
/// destinations or removed references. Structural window/reference agreement
/// alone cannot detect a coherent substitution of size or placement.
fn require_published_source_manifest(
    owner: &StagingOwnerRecord,
    source: &crate::metadata_command::CommitStreamPartCommand,
) -> Result<(), AbandonmentError> {
    if owner.windows.len() != source.segments.len() + 1 {
        return Err(AbandonmentError::PublishedManifest);
    }
    for (index, segment) in source.segments.iter().enumerate() {
        let window = &owner.windows[index];
        if segment.version_id != crate::MULTIPART_PART_SEGMENT_STAGING_VERSION_ID.to_u64()
            || segment.segment_index as usize != index
            || segment.segment_okh != window.segment_hash
            || segment.segment_vid != window.start
            || segment.size > window.geometry.logical_bound
            || segment.data_pg_id != window.participants.data_pg.get()
            || segment.placement_cluster_epoch != window.participants.placement_epoch
            || segment.ec_k != window.geometry.ec.k
            || segment.ec_m != window.geometry.ec.m
            || window.geometry.codec != source.upload.encryption.payload_codec()
        {
            return Err(AbandonmentError::PublishedManifest);
        }
        // Owner validation has already pinned cardinality, ordering and the
        // destination identity appropriate to the current lifecycle. A later
        // Part -> Object -> Reclaim transition must not change payload identity.
        if let Some(reference) = owner.references.get(index) {
            if reference.window_index != segment.segment_index
                || reference.segment_hash != segment.segment_okh
                || reference.physical_vid != segment.segment_vid
                || reference.logical_size != segment.size
                || Some(reference.stored_size) != window.geometry.codec.stored_size(segment.size)
            {
                return Err(AbandonmentError::PublishedManifest);
            }
        }
    }
    Ok(())
}

/// Build from the exact durable owner in the caller's pinned read snapshot.
/// This neither records abandonment nor grants recovery or deletion authority.
pub(super) fn capture_part_cleanup_in_open_transaction(
    pg: &PgStore,
    node_id: u32,
    topology: &PgTopology,
    source: source::PartFinalizationSource,
    id: MetadataCommandId,
) -> Result<source::AbandonedPartCleanupCommand, AbandonmentError> {
    use crate::metadata_command::AbortStreamUploadCommand;
    let abandoned = source.draft();
    let MetadataCommandPayload::CommitStreamPart(part) = abandoned.payload() else {
        return Err(AbandonmentError::Subject);
    };
    let mut abort = AbortStreamUploadCommand {
        bucket: part.bucket.clone(),
        key: part.key.clone(),
        session_id: part.session_id.clone(),
        staged_segments: Vec::new(),
        stream_create_bucket_write_reservation: None,
    };
    let envelope = |abort| {
        MetadataCommandEnvelope::new(
            id,
            MetadataCommandPayload::AbortStreamUpload(Box::new(abort)),
        )
    };
    let candidate = source
        .clone()
        .bind_cleanup(envelope(abort.clone()), topology)?;
    candidate
        .bind(topology)?
        .require_abandoned_source_in_open_transaction(pg, node_id)?;
    let owner = load_source_owner(pg, topology, abandoned)?;
    let finalization = StagingBucketResource::capture(
        &part.bucket_write_reservation,
        PgId::new(topology.bucket_pg_for(&part.bucket)),
    );
    if owner.lifecycle == StagingLifecycle::Active {
        if pg.get_stream_upload(&part.session_id)?.encryption != part.upload.encryption {
            return Err(AbandonmentError::Subject);
        }
        abort.staged_segments = pg.list_stream_segments(&part.session_id)?;
        abort
            .staged_segments
            .sort_by_key(|segment| segment.segment_index);
        let command = envelope(abort);
        close::validate_in_open_transaction(pg, topology, &command)?;
        owner
            .closed_unpublished()?
            .with_abandoned_part_finalization(&finalization)?
            .validate(topology)?;
        Ok(source.bind_cleanup(command, topology)?)
    } else {
        terminal_retention(pg, topology, &owner, &finalization)?;
        Ok(candidate)
    }
}
