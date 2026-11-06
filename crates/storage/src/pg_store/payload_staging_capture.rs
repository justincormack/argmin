// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Primary-side descriptor construction for the pending command integration.
//! These are descriptive records, NOT physical-write or cleanup capabilities.
//! The enclosing builder must load/validate the reservation, target and selected
//! append under PG serialization before calling this code, and install the
//! resulting descriptor under the same captured effect fence. Replica apply and
//! exact replay validate/preserve the descriptor; they never recapture it.

use super::*;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::types::AdmittedRouteEffectFence;
use crate::StoreError;

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum StagingCaptureError {
    #[error(transparent)]
    Invariant(#[from] StagingInvariant),
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Logical inputs already checked by the enclosing primary command builder.
/// Placement, incarnation, issuer, geometry and cutoff are not supplied here.
pub(in super::super) struct StagingIntroduction {
    pub(in super::super) id: StagingOwnerId,
    pub(in super::super) bucket: BucketName,
    pub(in super::super) key: ObjectKey,
    pub(in super::super) payload_generation: GenerationId,
    pub(in super::super) target: StagingTarget,
}

/// Borrows one captured catalogue and the actual node's configured topology.
/// It does not sample a renewable runtime-map handle. This is internal builder
/// input, not a replacement for admitted node/PG authority or its lock.
pub(in super::super) struct StagingCapture<'a> {
    pub(in super::super) topology: &'a PgTopology,
    pub(in super::super) placement: PayloadPlacementInput<'a>,
    pub(in super::super) effect_fence: AdmittedRouteEffectFence,
}

impl StagingCapture<'_> {
    pub(in super::super) fn introduction(
        &self,
        pg: &super::super::PgStore,
        node_id: NodeId,
        input: StagingIntroduction,
        proof: &BucketWriteReservationProof,
    ) -> Result<StagingOwnerRecord, StagingCaptureError> {
        let epoch = self.effect_fence.cluster_epoch();
        let object_pg = PgId::new(pg.pg_id);
        let ec = self.placement.require_topology(self.topology)?;
        self.placement
            .require_staging_primary(epoch, object_pg, node_id)?;
        let subject = StagingSubject {
            id: input.id,
            bucket: input.bucket,
            bucket_incarnation: proof.bucket_incarnation_generation,
            key: input.key,
            object_pg,
            originating_epoch: epoch,
            payload_generation: input.payload_generation,
            target: input.target,
            issuer: pg.stream_allocation_issuer(node_id)?,
        };
        subject.validate()?;
        if self.topology.object_pg_for(&subject.bucket, &subject.key) != object_pg.get() {
            return Err(StagingInvariant::Subject.into());
        }
        let creation = StagingBucketResource::capture(
            proof,
            PgId::new(self.topology.bucket_pg_for(&subject.bucket)),
        );
        creation.validate(&subject, subject.creation_operation())?;
        let first = self.window(&subject, 0, subject.first_vid(), ec)?;
        let resources = StagingResources {
            generation: ResourceResponsibility::Live(StagingGenerationResource {
                reservation_id: subject.id.attempt.clone(),
                generation: subject.payload_generation,
            }),
            creation: if matches!(subject.target, StagingTarget::UploadPart { .. }) {
                ResourceResponsibility::ReleaseDue(creation)
            } else {
                ResourceResponsibility::Live(creation)
            },
            finalization: ResourceResponsibility::NoResource,
            abandoned_finalizations: vec![],
        };
        let owner = StagingOwnerRecord {
            subject,
            lifecycle: StagingLifecycle::Active,
            reserved_end: first.end,
            maximum_cutoff_ms: first.cutoff_ms,
            windows: vec![first],
            references: vec![],
            resources,
        };
        owner.validate(self.topology)?;
        Ok(owner)
    }

    /// Construct bounded lookahead only; selecting the consumed current window
    /// and atomically appending both manifest and lookahead belongs to apply.
    pub(in super::super) fn next_window(
        &self,
        pg: &super::super::PgStore,
        node_id: NodeId,
        owner: &StagingOwnerRecord,
    ) -> Result<StagingWindow, StagingCaptureError> {
        owner.validate(self.topology)?;
        if owner.lifecycle != StagingLifecycle::Active
            || matches!(owner.subject.target, StagingTarget::DirectPut)
        {
            return Err(StagingInvariant::Lifecycle.into());
        }
        if pg.pg_id != owner.subject.object_pg.get() {
            return Err(StagingInvariant::Subject.into());
        }
        let ec = self.placement.require_topology(self.topology)?;
        self.placement.require_staging_primary(
            self.effect_fence.cluster_epoch(),
            owner.subject.object_pg,
            node_id,
        )?;
        if pg.stream_allocation_issuer(node_id)? != owner.subject.issuer {
            return Err(StagingInvariant::WindowOwner.into());
        }
        let index = u32::try_from(owner.windows.len()).map_err(|_| StagingInvariant::Windows)?;
        if index > MAX_STREAM_SEGMENTS {
            return Err(StagingInvariant::Windows.into());
        }
        self.window(&owner.subject, index, owner.reserved_end, ec)
    }

    fn window(
        &self,
        subject: &StagingSubject,
        index: u32,
        start: GenerationId,
        ec: EcShape,
    ) -> Result<StagingWindow, StagingCaptureError> {
        let epoch = self.effect_fence.cluster_epoch();
        self.effect_fence.require_valid_for(epoch)?;
        let issued_at_ms = crate::clock::current_time_millis();
        let cap = issued_at_ms
            .checked_add(subject.cap_ms())
            .ok_or(StagingInvariant::Cutoff)?;
        // Absolute conservative wall cutoff only. Never persist a remaining
        // duration or a process-local monotonic origin. Even static/unbounded
        // route admission receives a finite staging cap.
        let cutoff_ms = self.effect_fence.deadline().map_or(cap, |deadline| {
            cap.min(deadline.portable_wall_valid_until_ms())
        });
        let end = start
            .get()
            .checked_add(1)
            .and_then(GenerationId::new)
            .ok_or(StagingInvariant::Interval)?;
        let segment_hash = subject.segment_hash(index);
        let geometry = StagingGeometry::new(
            SegmentPayloadCodec::Cobblestone256SegmentV1,
            ec,
            INTERNAL_SEGMENT_SIZE as u64,
        )?;
        let data_pg = PgId::new(subject.data_pg(self.topology, index));
        let (acknowledgement_primary, locations) = self.placement.capture_segment_participants(
            epoch,
            data_pg.get(),
            geometry.ec,
            &segment_hash,
            start,
        )?;
        let window = StagingWindow {
            owner: subject.id.clone(),
            issuer: subject.issuer.clone(),
            index,
            segment_hash,
            start,
            end,
            issued_at_ms,
            cutoff_ms,
            geometry,
            participants: StagingParticipants {
                data_pg,
                placement_epoch: epoch,
                files: locations
                    .iter()
                    .map(|location| location.node_id())
                    .collect(),
                acknowledgement_primary,
            },
        };
        window.validate(subject)?;
        self.effect_fence.require_valid_for(epoch)?;
        Ok(window)
    }
}
