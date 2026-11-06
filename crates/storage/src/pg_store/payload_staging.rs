// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owner-local representation for the coordinated staging-ownership integration.
//!
//! These values describe retained responsibility, NOT permission to write or
//! delete. Validation is deterministic: it performs no clock, route-map, file,
//! reservation-RPC or local-allocation-lifetime lookup. The primary builder and
//! the physical-effect boundary must establish those separate authorities.
//!
//! Integration status: persistence/checkpoints include these records, but they
//! are not yet produced by command application. Keep frontend custody unchanged
//! until reserve/create/append, terminal transfers and recovery consume this
//! representation together.

use std::collections::BTreeSet;

use crate::metadata_command::{
    BucketWriteReservationProof, MetadataCommandId, StreamAllocationIssuer,
    PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
    UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
};
use crate::payload_limits::{INTERNAL_SEGMENT_SIZE, MAX_STREAM_SEGMENTS};
use crate::pg_topology::PgTopology;
use crate::{
    BucketName, ClusterEpoch, EcShape, GenerationId, NodeId, ObjectKey, PgId, SegmentPayloadCodec,
    SessionId, UploadId,
};

#[path = "payload_staging_sql.rs"]
mod sql;

#[path = "payload_staging_capture.rs"]
mod capture;

#[path = "payload_staging_descriptor.rs"]
mod descriptor;

#[path = "payload_staging_introduction.rs"]
mod introduction;

#[path = "payload_staging_append.rs"]
mod append;

#[path = "payload_staging_preparation.rs"]
mod preparation;

#[path = "payload_staging_close.rs"]
mod close;

#[path = "payload_staging_part.rs"]
mod part;

#[path = "payload_staging_object.rs"]
mod object;

#[path = "payload_staging_multipart.rs"]
mod multipart;

#[path = "payload_staging_displacement.rs"]
mod displacement;

#[path = "payload_staging_delete.rs"]
mod delete;

#[path = "payload_staging_marker.rs"]
mod marker;

#[path = "payload_staging_reclaim.rs"]
mod reclaim;

#[path = "payload_staging_abandonment.rs"]
mod abandonment;

#[path = "payload_staging_inventory.rs"]
mod inventory;

pub(super) use sql::{ROUTE_REFERENCES, SCHEMA, TABLES};

pub(super) fn extend_scavenger_protection_page(
    conn: &rusqlite::Connection,
    pg_id: PgId,
    topology: &PgTopology,
    after: Option<&crate::types::ShardScavengerReferenceCursor>,
    limit: std::num::NonZeroU16,
    items: &mut Vec<crate::types::ShardScavengerReferencePageItem>,
) -> Result<bool, crate::error::StoreError> {
    inventory::extend_scavenger_page(conn, pg_id, topology, after, limit, items).map_err(|error| {
        crate::error::StoreError::ShardScavengerScanIncomplete {
            context: "load retained staging protection page",
            errors: error.to_string(),
        }
    })
}

pub(super) fn extend_scavenger_protection_references(
    conn: &rusqlite::Connection,
    pg_id: PgId,
    topology: &PgTopology,
    references: &mut Vec<crate::types::ShardScavengerPayloadReference>,
) -> Result<(), crate::error::StoreError> {
    inventory::extend_protection_references(conn, pg_id, topology, references).map_err(|error| {
        crate::error::StoreError::ShardScavengerScanIncomplete {
            context: "load retained staging protection references",
            errors: error.to_string(),
        }
    })
}

pub(super) fn checkpoint_owners(
    pg_id: PgId,
    blocks: &[super::command_log::MetadataCheckpointTableBlock],
) -> Result<Vec<StagingOwnerRecord>, String> {
    let owner_blocks = &blocks[blocks.len().saturating_sub(TABLES.len())..];
    let owners =
        sql::decode_checkpoint_blocks(pg_id, owner_blocks).map_err(|error| error.to_string())?;
    sql::validate_checkpoint_generation_fences(&owners, blocks)
        .map_err(|error| error.to_string())?;
    Ok(owners)
}

pub(super) const DIRECT_PUT_WINDOW_CAP_MS: u64 = 10_000;
pub(super) const STREAM_WINDOW_CAP_MS: u64 = 300_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(super) enum StagingInvariant {
    #[error("staging validation topology differs from the configured insertion topology")]
    Topology,
    #[error("staging subject does not match its object PG or introduction")]
    Subject,
    #[error("staging value does not fit its durable integer domain")]
    IntegerDomain,
    #[error("staging target or payload generation is inconsistent")]
    Target,
    #[error("staging windows are missing, duplicated, reordered or overlapping")]
    Windows,
    #[error("staging window differs from its original owner or issuer")]
    WindowOwner,
    #[error("staging window does not reserve exactly one representable physical VID")]
    Interval,
    #[error("staging window has an invalid finite immutable cutoff")]
    Cutoff,
    #[error("staging window codec or geometry is inconsistent")]
    Geometry,
    #[error("staging window placement or participant ordering is inconsistent")]
    Placement,
    #[error("staging selected reference does not match its exact window or destination")]
    Reference,
    #[error("staging lifecycle and selected references disagree")]
    Lifecycle,
    #[error("staging resource responsibility does not match its owner or role")]
    Resource,
    #[error("staging owner lacks its durable object generation high-water fence")]
    GenerationFence,
}

fn sql_positive(value: u64) -> Result<(), StagingInvariant> {
    if value == 0 || value > i64::MAX as u64 {
        Err(StagingInvariant::IntegerDomain)
    } else {
        Ok(())
    }
}

fn sql_nonnegative(value: u64) -> Result<(), StagingInvariant> {
    if value > i64::MAX as u64 {
        Err(StagingInvariant::IntegerDomain)
    } else {
        Ok(())
    }
}

/// The first introducing command supplies an immutable revision. A reissue
/// keeps this identity even when its own envelope has a later index/epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StagingOwnerId {
    pub(super) attempt: SessionId,
    pub(super) introduction: MetadataCommandId,
}

/// Necessary command ordering, not permission to apply or reissue. Positions
/// are comparable only within one PG; each epoch restarts its index at one.
/// The enclosing command boundary still enforces acceptance/recovery authority.
fn command_follows(earlier: MetadataCommandId, later: MetadataCommandId) -> bool {
    earlier.pg_id() == later.pg_id()
        && (earlier.cluster_epoch(), earlier.log_index())
            < (later.cluster_epoch(), later.log_index())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StagingTarget {
    DirectPut,
    StreamPut,
    UploadPart {
        upload_id: UploadId,
        upload_generation: GenerationId,
        part_number: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StagingSubject {
    pub(super) id: StagingOwnerId,
    pub(super) bucket: BucketName,
    pub(super) bucket_incarnation: u64,
    pub(super) key: ObjectKey,
    pub(super) object_pg: PgId,
    pub(super) originating_epoch: ClusterEpoch,
    pub(super) payload_generation: GenerationId,
    pub(super) target: StagingTarget,
    pub(super) issuer: StreamAllocationIssuer,
}

impl StagingSubject {
    /// Retained owners outlive reservations, sessions and live manifests. Those
    /// transient references cannot substitute for the non-recycling allocator
    /// fence, including after publication or while awaiting cleanup.
    fn require_generation_high_water(
        &self,
        high_water: Option<u64>,
    ) -> Result<(), StagingInvariant> {
        if high_water.is_some_and(|high_water| high_water >= self.payload_generation.get()) {
            Ok(())
        } else {
            Err(StagingInvariant::GenerationFence)
        }
    }

    fn validate(&self) -> Result<(), StagingInvariant> {
        sql_positive(self.bucket_incarnation)?;
        sql_positive(self.payload_generation.get())?;
        sql_positive(self.originating_epoch.get())?;
        sql_positive(self.id.introduction.log_index().get())?;
        if self.id.introduction.pg_id() != self.object_pg
            || self.id.introduction.cluster_epoch() != self.originating_epoch
        {
            return Err(StagingInvariant::Subject);
        }
        if let StagingTarget::UploadPart {
            upload_generation,
            part_number,
            ..
        } = self.target
        {
            sql_positive(upload_generation.get())?;
            if !(1..=10_000).contains(&part_number) || upload_generation == self.payload_generation
            {
                return Err(StagingInvariant::Target);
            }
        }
        Ok(())
    }

    fn first_vid(&self) -> GenerationId {
        match self.target {
            StagingTarget::DirectPut => self.payload_generation,
            StagingTarget::StreamPut | StagingTarget::UploadPart { .. } => GenerationId::MIN,
        }
    }

    fn segment_hash(&self, index: u32) -> [u8; 16] {
        match self.target {
            StagingTarget::DirectPut => crate::direct_put_segment_key_hash(&self.id.attempt, index),
            StagingTarget::StreamPut | StagingTarget::UploadPart { .. } => {
                crate::stream_segment_key_hash(&self.id.attempt, index)
            }
        }
    }

    fn data_pg(&self, topology: &PgTopology, index: u32) -> u32 {
        match self.target {
            StagingTarget::DirectPut | StagingTarget::StreamPut => topology
                .object_generation_segment_data_pg(
                    &self.bucket,
                    &self.key,
                    self.payload_generation,
                    index,
                )
                .get(),
            StagingTarget::UploadPart {
                upload_generation,
                part_number,
                ..
            } => topology
                .object_generation_multipart_part_segment_data_pg(
                    &self.bucket,
                    &self.key,
                    upload_generation,
                    part_number,
                    index,
                )
                .get(),
        }
    }

    fn cap_ms(&self) -> u64 {
        match self.target {
            StagingTarget::DirectPut => DIRECT_PUT_WINDOW_CAP_MS,
            StagingTarget::StreamPut | StagingTarget::UploadPart { .. } => STREAM_WINDOW_CAP_MS,
        }
    }

    fn creation_operation(&self) -> &'static str {
        match self.target {
            StagingTarget::DirectPut => PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            StagingTarget::StreamPut => PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
            StagingTarget::UploadPart { .. } => {
                UPLOAD_PART_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            }
        }
    }

    fn published_generation(&self) -> GenerationId {
        match self.target {
            StagingTarget::DirectPut | StagingTarget::StreamPut => self.payload_generation,
            StagingTarget::UploadPart {
                upload_generation, ..
            } => upload_generation,
        }
    }
}

/// There is one file participant per shard index, in index order. The independent
/// acknowledgement primary is not inferred from the first file participant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StagingParticipants {
    pub(super) data_pg: PgId,
    pub(super) placement_epoch: ClusterEpoch,
    pub(super) files: Vec<NodeId>,
    pub(super) acknowledgement_primary: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StagingGeometry {
    pub(super) codec: SegmentPayloadCodec,
    pub(super) ec: EcShape,
    pub(super) logical_bound: u64,
    pub(super) stored_bound: u64,
    pub(super) shard_bound: u64,
}

impl StagingGeometry {
    pub(super) fn new(
        codec: SegmentPayloadCodec,
        ec: EcShape,
        logical_bound: u64,
    ) -> Result<Self, StagingInvariant> {
        if ec::EcConfig::new(ec.k, ec.m).is_err() || logical_bound > INTERNAL_SEGMENT_SIZE as u64 {
            return Err(StagingInvariant::Geometry);
        }
        let stored_bound = codec
            .stored_size(logical_bound)
            .ok_or(StagingInvariant::Geometry)?;
        let shard_bound = stored_bound.div_ceil(u64::from(ec.k));
        sql_positive(stored_bound)?;
        sql_positive(shard_bound)?;
        Ok(Self {
            codec,
            ec,
            logical_bound,
            stored_bound,
            shard_bound,
        })
    }

    fn validate(&self) -> Result<(), StagingInvariant> {
        if *self != Self::new(self.codec, self.ec, self.logical_bound)? {
            return Err(StagingInvariant::Geometry);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StagingWindow {
    pub(super) owner: StagingOwnerId,
    pub(super) issuer: StreamAllocationIssuer,
    pub(super) index: u32,
    pub(super) segment_hash: [u8; 16],
    pub(super) start: GenerationId,
    pub(super) end: GenerationId,
    pub(super) issued_at_ms: u64,
    pub(super) cutoff_ms: u64,
    pub(super) geometry: StagingGeometry,
    pub(super) participants: StagingParticipants,
}

impl StagingWindow {
    fn validate(&self, owner: &StagingSubject) -> Result<(), StagingInvariant> {
        if self.owner != owner.id || self.issuer != owner.issuer {
            return Err(StagingInvariant::WindowOwner);
        }
        sql_positive(self.start.get())?;
        sql_positive(self.end.get())?;
        if self.start.get().checked_add(1) != Some(self.end.get()) {
            return Err(StagingInvariant::Interval);
        }
        if self.segment_hash != owner.segment_hash(self.index) {
            return Err(StagingInvariant::WindowOwner);
        }
        sql_nonnegative(self.issued_at_ms)?;
        sql_positive(self.cutoff_ms)?;
        if self.cutoff_ms <= self.issued_at_ms
            || self.cutoff_ms - self.issued_at_ms > owner.cap_ms()
        {
            return Err(StagingInvariant::Cutoff);
        }
        self.geometry.validate()?;
        sql_positive(self.participants.placement_epoch.get())?;
        if self.participants.placement_epoch < owner.originating_epoch
            || self.participants.files.len()
                != usize::from(self.geometry.ec.k) + usize::from(self.geometry.ec.m)
            || self
                .participants
                .files
                .iter()
                .collect::<BTreeSet<_>>()
                .len()
                != self.participants.files.len()
        {
            return Err(StagingInvariant::Placement);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StagingLifecycle {
    Active,
    Published,
    Retiring(StagingRetirement),
}

/// Closure forbids allocation; it does not by itself remove a reference or
/// prove a resource was released. Preserve publication provenance even after
/// all manifest/reclaim bindings have gone, including for an empty payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StagingRetirement {
    Unpublished,
    PublishedRetained,
    PublishedUnreferenced,
}

/// The bucket/key/incarnation are inherited from the owner, so a destination
/// cannot silently redirect a reference to a different object subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum StagingDestination {
    Stream,
    Part {
        upload_id: UploadId,
        part_number: u32,
        payload_generation: GenerationId,
    },
    Object {
        generation: GenerationId,
        part_number: Option<u32>,
    },
    Reclaim {
        generation: GenerationId,
        part_number: Option<u32>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StagingReference {
    pub(super) window_index: u32,
    pub(super) segment_hash: [u8; 16],
    pub(super) physical_vid: GenerationId,
    pub(super) logical_size: u64,
    pub(super) stored_size: u64,
    pub(super) destination: StagingDestination,
}

/// A role has exactly one of these meanings; `None` must not conflate an absent
/// resource with an unresolved release. Renewal changes lease evidence, never
/// the stable reservation identity or its captured historical release route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ResourceResponsibility<T> {
    NoResource,
    Live(T),
    ReleaseDue(T),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct StableBucketReservation {
    bucket: BucketName,
    reservation_id: String,
    owner_token: String,
    cluster_epoch: ClusterEpoch,
    execution_generation: u64,
    incarnation: u64,
    operation_kind: String,
    created_at: u64,
    target_context: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StagingBucketResource {
    pub(super) identity: StableBucketReservation,
    pub(super) observed_lease_deadline: u64,
    pub(super) release_pg: PgId,
}

// Reservation identities must fit the authoritative bucket reservation's
// 256-character fields AND the release transport. Bound UTF-8 bytes here (not
// just character count); the descriptor's overall framing cap is not a valid
// bound for an individual durable release identity. Operation and target are
// separately bound to the canonical operation and validated object key below.
const STAGING_BUCKET_RESERVATION_IDENTITY_MAX_BYTES: usize = 256;

impl StagingBucketResource {
    pub(super) fn capture(proof: &BucketWriteReservationProof, release_pg: PgId) -> Self {
        Self {
            identity: StableBucketReservation {
                bucket: proof.bucket.clone(),
                reservation_id: proof.reservation_id.clone(),
                owner_token: proof.owner_token.clone(),
                cluster_epoch: proof.cluster_epoch,
                execution_generation: proof.bucket_execution_generation,
                incarnation: proof.bucket_incarnation_generation,
                operation_kind: proof.operation_kind.clone(),
                created_at: proof.created_at,
                target_context: proof.target_context.clone(),
            },
            observed_lease_deadline: proof.lease_deadline,
            release_pg,
        }
    }

    fn validate(&self, owner: &StagingSubject, operation: &str) -> Result<(), StagingInvariant> {
        let proof = &self.identity;
        sql_positive(proof.cluster_epoch.get())?;
        sql_positive(proof.execution_generation)?;
        sql_positive(proof.incarnation)?;
        sql_nonnegative(proof.created_at)?;
        sql_positive(self.observed_lease_deadline)?;
        if proof.bucket != owner.bucket
            || proof.incarnation != owner.bucket_incarnation
            || proof.execution_generation < proof.incarnation
            || !(1..=STAGING_BUCKET_RESERVATION_IDENTITY_MAX_BYTES)
                .contains(&proof.reservation_id.len())
            || !(1..=STAGING_BUCKET_RESERVATION_IDENTITY_MAX_BYTES)
                .contains(&proof.owner_token.len())
            // SQLite length(TEXT) stops at the first NUL: these otherwise
            // nonempty byte strings violate the source table's lower bound.
            || proof.reservation_id.starts_with('\0')
            || proof.owner_token.starts_with('\0')
            || proof.operation_kind != operation
            || (operation == owner.creation_operation()
                && proof.cluster_epoch != owner.originating_epoch)
            || (operation == UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND
                && proof.cluster_epoch < owner.originating_epoch)
            || proof.target_context.as_deref() != Some(owner.key.as_str())
            || self.observed_lease_deadline <= proof.created_at
        {
            return Err(StagingInvariant::Resource);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StagingGenerationResource {
    pub(super) reservation_id: SessionId,
    pub(super) generation: GenerationId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StagingResources {
    pub(super) generation: ResourceResponsibility<StagingGenerationResource>,
    pub(super) creation: ResourceResponsibility<StagingBucketResource>,
    pub(super) finalization: ResourceResponsibility<StagingBucketResource>,
    /// Additional, independently authenticated abandoned finalizations. Keep
    /// the original terminal proof separate: these never replace publication
    /// custody or change the owner's selected references. Append order is the
    /// replicated follow-up order; stable identity, not lease evidence, dedups.
    pub(super) abandoned_finalizations: Vec<StagingBucketResource>,
}

/// Untrusted row/command projection. A checked view is borrowed, so validation
/// cannot be detached from the exact values it checked and followed by mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StagingOwnerRecord {
    pub(super) subject: StagingSubject,
    pub(super) lifecycle: StagingLifecycle,
    pub(super) reserved_end: GenerationId,
    pub(super) maximum_cutoff_ms: u64,
    pub(super) windows: Vec<StagingWindow>,
    pub(super) references: Vec<StagingReference>,
    pub(super) resources: StagingResources,
}

/// Both the exact record and its canonical validation topology remain borrowed
/// and immutable. Equal object PGs alone do not establish equal data/release PGs.
#[derive(Debug)]
pub(super) struct CheckedStagingOwner<'a> {
    record: &'a StagingOwnerRecord,
    topology: &'a PgTopology,
}

impl CheckedStagingOwner<'_> {
    pub(super) fn record_for_topology(
        &self,
        configured_topology: &PgTopology,
    ) -> Result<&StagingOwnerRecord, StagingInvariant> {
        if self.topology != configured_topology {
            return Err(StagingInvariant::Topology);
        }
        Ok(self.record)
    }
}

impl StagingOwnerRecord {
    /// Bind a live staging row to its retained physical window. This is a
    /// correspondence check, not permission to read, write or delete shards.
    fn selected_stream_reference(
        &self,
        segment: &crate::StreamUploadSegmentRecord,
    ) -> Result<StagingReference, StagingInvariant> {
        let window = self
            .windows
            .get(segment.segment_index as usize)
            .ok_or(StagingInvariant::Reference)?;
        if segment.session_id != self.subject.id.attempt
            || segment.segment_okh != window.segment_hash
            || segment.segment_vid != window.start
            || segment.data_pg_id != window.participants.data_pg.get()
            || segment.placement_cluster_epoch != window.participants.placement_epoch
            || segment.ec_k != window.geometry.ec.k
            || segment.ec_m != window.geometry.ec.m
            || segment.size > window.geometry.logical_bound
        {
            return Err(StagingInvariant::Reference);
        }
        let stored_size = window
            .geometry
            .codec
            .stored_size(segment.size)
            .ok_or(StagingInvariant::Reference)?;
        Ok(StagingReference {
            window_index: segment.segment_index,
            segment_hash: segment.segment_okh,
            physical_vid: segment.segment_vid,
            logical_size: segment.size,
            stored_size,
            destination: StagingDestination::Stream,
        })
    }

    /// Ordinary release/abort has acquired no finalization proof. Authoritative
    /// finalization abandonment needs its own proof-bearing transition.
    fn closed_unpublished(&self) -> Result<Self, StagingInvariant> {
        if self.lifecycle != StagingLifecycle::Active
            || self.resources.finalization != ResourceResponsibility::NoResource
        {
            return Err(StagingInvariant::Lifecycle);
        }
        let mut closed = self.clone();
        closed.lifecycle = StagingLifecycle::Retiring(StagingRetirement::Unpublished);
        closed.references.clear();
        if let ResourceResponsibility::Live(resource) = &self.resources.generation {
            closed.resources.generation = ResourceResponsibility::ReleaseDue(resource.clone());
        }
        if let ResourceResponsibility::Live(resource) = &self.resources.creation {
            closed.resources.creation = ResourceResponsibility::ReleaseDue(resource.clone());
        }
        Ok(closed)
    }

    pub(super) fn validate<'a>(
        &'a self,
        topology: &'a PgTopology,
    ) -> Result<CheckedStagingOwner<'a>, StagingInvariant> {
        self.validate_structure()?;
        if topology.object_pg_for(&self.subject.bucket, &self.subject.key)
            != self.subject.object_pg.get()
        {
            return Err(StagingInvariant::Subject);
        }
        for window in &self.windows {
            if window.participants.data_pg.get() != self.subject.data_pg(topology, window.index) {
                return Err(StagingInvariant::Placement);
            }
        }
        for resource in [&self.resources.creation, &self.resources.finalization] {
            if let ResourceResponsibility::Live(proof) | ResourceResponsibility::ReleaseDue(proof) =
                resource
            {
                if proof.release_pg.get() != topology.bucket_pg_for(&self.subject.bucket) {
                    return Err(StagingInvariant::Resource);
                }
            }
        }
        for resource in &self.resources.abandoned_finalizations {
            if resource.release_pg.get() != topology.bucket_pg_for(&self.subject.bucket) {
                return Err(StagingInvariant::Resource);
            }
        }
        Ok(CheckedStagingOwner {
            record: self,
            topology,
        })
    }

    /// Topology-independent row/checkpoint validation. This does not mint the
    /// checked insertion view: only validation against the caller's topology
    /// can do that. No wall clock or route-map lookup belongs in replica apply.
    fn validate_structure(&self) -> Result<(), StagingInvariant> {
        self.subject.validate()?;
        let limit = match self.subject.target {
            StagingTarget::DirectPut => 1,
            // The final descriptor is bounded lookahead, never a selectable
            // payload slot beyond the single-write size limit.
            StagingTarget::StreamPut | StagingTarget::UploadPart { .. } => {
                MAX_STREAM_SEGMENTS as usize + 1
            }
        };
        if self.windows.is_empty() || self.windows.len() > limit {
            return Err(StagingInvariant::Windows);
        }
        let mut next_vid = self.subject.first_vid();
        let mut maximum_cutoff = 0;
        for (index, window) in self.windows.iter().enumerate() {
            if window.index as usize != index || window.start != next_vid {
                return Err(StagingInvariant::Windows);
            }
            window.validate(&self.subject)?;
            next_vid = window.end;
            maximum_cutoff = maximum_cutoff.max(window.cutoff_ms);
        }
        if self.reserved_end != next_vid || self.maximum_cutoff_ms != maximum_cutoff {
            return Err(StagingInvariant::Windows);
        }
        self.validate_references()?;
        self.validate_resources()?;
        Ok(())
    }

    fn validate_references(&self) -> Result<(), StagingInvariant> {
        let streamed_count = self.windows.len() - 1;
        let expected_count = match (self.lifecycle, &self.subject.target) {
            (StagingLifecycle::Active, StagingTarget::DirectPut)
            | (
                StagingLifecycle::Retiring(
                    StagingRetirement::Unpublished | StagingRetirement::PublishedUnreferenced,
                ),
                _,
            ) => 0,
            (
                StagingLifecycle::Published
                | StagingLifecycle::Retiring(StagingRetirement::PublishedRetained),
                StagingTarget::DirectPut,
            ) => 1,
            (
                StagingLifecycle::Active
                | StagingLifecycle::Published
                | StagingLifecycle::Retiring(StagingRetirement::PublishedRetained),
                StagingTarget::StreamPut | StagingTarget::UploadPart { .. },
            ) => streamed_count,
        };
        if self.references.len() != expected_count {
            return Err(StagingInvariant::Reference);
        }
        // One atomic terminal transfer moves the entire selected prefix to one
        // exact destination. Per-row validity alone permits split Part/Object
        // or Object/Reclaim ownership and cannot detect a missing prefix row.
        let destination = self
            .references
            .first()
            .map(|reference| &reference.destination);
        for (index, reference) in self.references.iter().enumerate() {
            if self.lifecycle == StagingLifecycle::Active
                && reference.destination != StagingDestination::Stream
            {
                return Err(StagingInvariant::Reference);
            }
            if reference.window_index as usize != index
                || reference.window_index >= MAX_STREAM_SEGMENTS
                || Some(&reference.destination) != destination
            {
                return Err(StagingInvariant::Reference);
            }
            let window = self
                .windows
                .get(reference.window_index as usize)
                .ok_or(StagingInvariant::Reference)?;
            if reference.physical_vid != window.start
                || reference.segment_hash != window.segment_hash
                || reference.logical_size > window.geometry.logical_bound
                || window.geometry.codec.stored_size(reference.logical_size)
                    != Some(reference.stored_size)
            {
                return Err(StagingInvariant::Reference);
            }
            match &reference.destination {
                StagingDestination::Stream => {
                    if self.lifecycle != StagingLifecycle::Active
                        || self.subject.target == StagingTarget::DirectPut
                    {
                        return Err(StagingInvariant::Lifecycle);
                    }
                }
                StagingDestination::Part {
                    upload_id,
                    part_number,
                    payload_generation,
                } => {
                    let StagingTarget::UploadPart {
                        upload_id: expected_upload,
                        part_number: expected_part,
                        ..
                    } = &self.subject.target
                    else {
                        return Err(StagingInvariant::Reference);
                    };
                    if upload_id != expected_upload
                        || part_number != expected_part
                        || *payload_generation != self.subject.payload_generation
                    {
                        return Err(StagingInvariant::Reference);
                    }
                    if self.lifecycle == StagingLifecycle::Active {
                        return Err(StagingInvariant::Lifecycle);
                    }
                }
                StagingDestination::Object {
                    generation,
                    part_number,
                }
                | StagingDestination::Reclaim {
                    generation,
                    part_number,
                } => {
                    let expected_part = match self.subject.target {
                        StagingTarget::DirectPut | StagingTarget::StreamPut => None,
                        StagingTarget::UploadPart { part_number, .. } => Some(part_number),
                    };
                    if *generation != self.subject.published_generation()
                        || *part_number != expected_part
                    {
                        return Err(StagingInvariant::Reference);
                    }
                    if self.lifecycle == StagingLifecycle::Active {
                        return Err(StagingInvariant::Lifecycle);
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_resources(&self) -> Result<(), StagingInvariant> {
        let active = self.lifecycle == StagingLifecycle::Active;
        match &self.resources.generation {
            // Every owner was introduced with this obligation. A successful
            // release does not erase evidence without a confirmed transition.
            ResourceResponsibility::NoResource => return Err(StagingInvariant::Resource),
            ResourceResponsibility::Live(resource)
            | ResourceResponsibility::ReleaseDue(resource) => {
                if resource.reservation_id != self.subject.id.attempt
                    || resource.generation != self.subject.payload_generation
                    || active
                        != matches!(self.resources.generation, ResourceResponsibility::Live(_))
                {
                    return Err(StagingInvariant::Resource);
                }
            }
        }
        let multipart = matches!(self.subject.target, StagingTarget::UploadPart { .. });
        match &self.resources.creation {
            ResourceResponsibility::NoResource => return Err(StagingInvariant::Resource),
            ResourceResponsibility::Live(proof) | ResourceResponsibility::ReleaseDue(proof) => {
                if (active && !multipart)
                    != matches!(self.resources.creation, ResourceResponsibility::Live(_))
                {
                    return Err(StagingInvariant::Resource);
                }
                proof.validate(&self.subject, self.subject.creation_operation())?;
            }
        }
        match (
            &self.subject.target,
            self.lifecycle,
            &self.resources.finalization,
        ) {
            // A pending finalization owns its own proof until apply transfers
            // it to a published owner; it cannot appear on an active owner.
            (
                StagingTarget::DirectPut | StagingTarget::StreamPut,
                _,
                ResourceResponsibility::NoResource,
            )
            | (
                StagingTarget::UploadPart { .. },
                StagingLifecycle::Active,
                ResourceResponsibility::NoResource,
            )
            | (
                StagingTarget::UploadPart { .. },
                StagingLifecycle::Retiring(StagingRetirement::Unpublished),
                ResourceResponsibility::NoResource,
            ) => Ok(()),
            (
                StagingTarget::UploadPart { .. },
                StagingLifecycle::Published | StagingLifecycle::Retiring(_),
                ResourceResponsibility::ReleaseDue(proof),
            ) => proof.validate(
                &self.subject,
                UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
            ),
            _ => Err(StagingInvariant::Resource),
        }?;
        if !self.resources.abandoned_finalizations.is_empty() {
            let ResourceResponsibility::ReleaseDue(primary) = &self.resources.finalization else {
                return Err(StagingInvariant::Resource);
            };
            if active
                || !multipart
                || self.resources.abandoned_finalizations.len() > (u32::MAX - 1) as usize
            {
                return Err(StagingInvariant::Resource);
            }
            let mut identities = BTreeSet::from([&primary.identity]);
            for proof in &self.resources.abandoned_finalizations {
                proof.validate(
                    &self.subject,
                    UPLOAD_PART_STREAM_FINALIZE_BUCKET_WRITE_OPERATION_KIND,
                )?;
                if !identities.insert(&proof.identity) {
                    return Err(StagingInvariant::Resource);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "payload_staging_tests.rs"]
mod tests;
