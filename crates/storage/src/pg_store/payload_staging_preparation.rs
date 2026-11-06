// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Primary-local consumption of an already replicated, one-slot stream window.
//! This component replaces the existing allocator UPDATE inside its write
//! transaction when mandatory command descriptors are activated. It is not yet
//! called by production preparation. No fallback to unretained allocation is
//! permitted here, and its returned record is NOT physical-write authority.

use super::*;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::traits::PgMetadataStore;
use crate::types::{AdmittedRouteEffectFence, ExpectedStreamAppendState};
use crate::{
    MetadataError, PrepareStreamUploadSegmentAppendReq, StoreError, StreamUploadRecord,
    StreamUploadSegmentRecord, StreamUploadState, StreamUploadTarget,
};
use rusqlite::OptionalExtension;

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum PreparationError {
    #[error("retained stream preparation requires an acquired SQLite write transaction")]
    TransactionRequired,
    #[error("retained stream preparation does not match its captured session authority")]
    Authority,
    #[error("retained stream preparation has missing or changed owner responsibility")]
    Responsibility,
    #[error("retained stream preparation requires its exact unconsumed window")]
    Window,
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

fn validate_session(
    pg: &PgStore,
    owner: &StagingOwnerRecord,
    expected: &ExpectedStreamAppendState,
) -> Result<StreamUploadRecord, PreparationError> {
    let subject = &owner.subject;
    let session = pg.get_stream_upload(&subject.id.attempt)?;
    let (captured, generation) = match (expected, &subject.target, &session.target) {
        (
            ExpectedStreamAppendState::PutObject {
                session,
                generation_id,
            },
            StagingTarget::StreamPut,
            StreamUploadTarget::PutObject,
        ) => (session, *generation_id),
        (
            ExpectedStreamAppendState::UploadPart {
                session,
                upload: expected_upload,
                part_generation_id,
            },
            StagingTarget::UploadPart {
                upload_id,
                upload_generation,
                part_number,
            },
            StreamUploadTarget::UploadPart {
                upload_id: current_upload,
                part_number: current_part,
            },
        ) if upload_id == current_upload && part_number == current_part => {
            let upload = pg.get_multipart_upload(upload_id)?;
            if &upload != expected_upload.as_ref()
                || upload.bucket != subject.bucket
                || upload.key != subject.key
                || upload.object_generation_id != *upload_generation
                || upload.state != crate::UploadState::InProgress
                || upload.encryption != session.encryption
            {
                return Err(PreparationError::Authority);
            }
            (session, *part_generation_id)
        }
        _ => return Err(PreparationError::Authority),
    };
    if !captured.has_same_append_authority(&session) || generation != subject.payload_generation {
        return Err(PreparationError::Authority);
    }
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
    if session.bucket != subject.bucket
        || session.key != subject.key
        || session.bucket_incarnation_generation != subject.bucket_incarnation
        || session.state != StreamUploadState::InProgress
        || !proof_matches
        || pg.load_stream_allocation_issuer(&subject.id.attempt)? != subject.issuer
        || owner
            .windows
            .iter()
            .any(|window| window.geometry.codec != session.encryption.payload_codec())
    {
        return Err(PreparationError::Responsibility);
    }
    let reservation_matches: bool = pg.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM object_generation_reservations WHERE reservation_id = ?1
         AND bucket = ?2 AND key = ?3 AND generation_id = ?4 AND created_at = ?5)",
        rusqlite::params![
            subject.id.attempt,
            subject.bucket,
            subject.key,
            generation.get(),
            session.created_at
        ],
        |row| row.get(0),
    )?;
    if !reservation_matches {
        return Err(PreparationError::Responsibility);
    }
    let high_water = pg.conn.query_row(
        "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
        rusqlite::params![subject.bucket, subject.key], |row| row.get::<_, Option<u64>>(0),
    ).optional()?.flatten();
    subject.require_generation_high_water(high_water)?;
    let mut segments = pg.list_stream_segments(&subject.id.attempt)?;
    segments.sort_by_key(|segment| segment.segment_index);
    if segments.len() != owner.references.len() {
        return Err(PreparationError::Responsibility);
    }
    for (segment, reference) in segments.iter().zip(&owner.references) {
        if owner.selected_stream_reference(segment)? != *reference {
            return Err(PreparationError::Responsibility);
        }
    }
    Ok(session)
}

/// The caller holds object-PG serialization and its already acquired SQLite
/// write transaction. Do not expose the result until that transaction commits.
/// This consumes only the local cursor, never extends the replicated reservation
/// or cutoff, and does not allocate another slot after an ambiguous/lost reply.
pub(in super::super) fn prepare_in_open_transaction(
    pg: &PgStore,
    topology: &PgTopology,
    node_id: NodeId,
    placement: PayloadPlacementInput<'_>,
    effect_fence: AdmittedRouteEffectFence,
    request: &PrepareStreamUploadSegmentAppendReq,
    expected: &ExpectedStreamAppendState,
) -> Result<StreamUploadSegmentRecord, PreparationError> {
    // A merely open deferred/read transaction could wait for the SQLite writer
    // AFTER the last fence check. Require writer admission before any reads.
    if pg.conn.transaction_state(Some(rusqlite::MAIN_DB))? != rusqlite::TransactionState::Write {
        return Err(PreparationError::TransactionRequired);
    }
    let owner = sql::load(&pg.conn, PgId::new(pg.pg_id), &request.session_id)?
        .ok_or(PreparationError::Responsibility)?;
    owner.validate(topology)?;
    if owner.lifecycle != StagingLifecycle::Active
        || matches!(owner.subject.target, StagingTarget::DirectPut)
    {
        return Err(PreparationError::Responsibility);
    }
    let window = owner.windows.last().ok_or(PreparationError::Window)?;
    let epoch = window.participants.placement_epoch;
    effect_fence.require_valid_for(epoch)?;
    let fence = effect_fence.intersect(AdmittedRouteEffectFence::bind_portable(
        epoch,
        window.cutoff_ms,
        window.cutoff_ms,
    ))?;
    fence.require_valid_for(epoch)?;
    let session = validate_session(pg, &owner, expected)?;
    if request.segment_index != window.index
        || !crate::payload_limits::stream_segment_extent_is_valid(
            request.segment_index,
            request.size,
        )
        || request.size > window.geometry.logical_bound
        || session.next_segment_vid != window.start
    {
        return Err(PreparationError::Window);
    }
    if placement.require_topology(topology)? != window.geometry.ec {
        return Err(StagingInvariant::Geometry.into());
    }
    placement.require_staging_primary(epoch, owner.subject.object_pg, node_id)?;
    let (ack_primary, locations) = placement.capture_segment_participants(
        epoch,
        window.participants.data_pg.get(),
        window.geometry.ec,
        &window.segment_hash,
        window.start,
    )?;
    if ack_primary != window.participants.acknowledgement_primary
        || !locations
            .iter()
            .map(|location| location.node_id())
            .eq(window.participants.files.iter().copied())
    {
        return Err(StagingInvariant::Placement.into());
    }
    let record = StreamUploadSegmentRecord {
        session_id: request.session_id.clone(),
        segment_index: window.index,
        size: request.size,
        segment_crc64: request.segment_crc64,
        payload_crc64: request.payload_crc64,
        segment_okh: window.segment_hash,
        segment_vid: window.start,
        data_pg_id: window.participants.data_pg.get(),
        placement_cluster_epoch: epoch,
        ec_k: window.geometry.ec.k,
        ec_m: window.geometry.ec.m,
        shard_integrity: None,
    };
    let mut statement = pg.conn.prepare_cached(
        "UPDATE stream_uploads SET next_segment_vid = ?3
         WHERE session_id = ?1 AND next_segment_vid = ?2 AND state = 0",
    )?;
    #[cfg(test)]
    if let Some(hook) = BEFORE_EFFECT.with(|slot| slot.borrow_mut().take()) {
        hook();
    }
    if pg.stream_allocation_issuer(node_id)? != owner.subject.issuer {
        return Err(StoreError::PgAllocationLifetimeUnavailable { pg_id: pg.pg_id }.into());
    }
    // No authority/metadata lookup or writer admission after this check.
    fence.require_valid_for(epoch)?;
    if statement.execute(rusqlite::params![
        request.session_id,
        window.start.get(),
        window.end.get()
    ])? != 1
    {
        return Err(PreparationError::Window);
    }
    Ok(record)
}

#[cfg(test)]
thread_local! {
    static BEFORE_EFFECT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn with_before_effect<T>(
    hook: impl FnOnce() + 'static,
    action: impl FnOnce() -> T,
) -> T {
    struct Reset(Option<Box<dyn FnOnce()>>);
    impl Drop for Reset {
        fn drop(&mut self) {
            BEFORE_EFFECT.with(|slot| *slot.borrow_mut() = self.0.take());
        }
    }
    let _reset = Reset(BEFORE_EFFECT.with(|slot| slot.replace(Some(Box::new(hook)))));
    action()
}
