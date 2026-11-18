// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only confirmation input derived from a bound immutable descriptor. This
//! is not a new submission, allocator, physical grant, or detached pending index.

use super::*;
use crate::metadata_command::MetadataCommandReplicaState;
use crate::node_runtime::pg_store::command_log::{
    CompleteSettledPrefixError, CompleteSettledPrefixRead,
};
use crate::types::AdmittedRouteEffectFence;
use crate::StoreError;
use std::time::{Duration, Instant};

#[path = "payload_staging_direct_terminal_command.rs"]
mod direct_terminal;

impl StagingWindowConfirmationRequest<'_> {
    pub(in crate::node_runtime::pg_store) fn require_direct_snapshot_response(
        &self,
        client: &crate::node_client::UnixStorageNodeClient,
        snapshot: &crate::DirectPutCommitStorageSnapshot,
    ) -> Result<(), StagingWindowConfirmationError> {
        let invalid = |why: &str| {
            StagingWindowConfirmationError::Window(format!("remote direct snapshot: {why}"))
        };
        if self.subject.target != StagingTarget::DirectPut {
            return Err(invalid("target"));
        }
        client
            .validate_direct_put_commit_snapshot_response(
                snapshot,
                &self.subject.bucket,
                &self.subject.key,
            )
            .map_err(|error| match error {
                crate::error::ObjectPgActionError::Store(error) => {
                    StagingWindowConfirmationError::Store(error)
                }
                error => invalid(&error.to_string()),
            })?;
        // Active introduction custody cannot already be a Published generation.
        if snapshot.committed_segments.is_some()
            || snapshot.committed_stale_generation_id.is_some()
            || snapshot
                .current
                .as_ref()
                .and_then(crate::StoredObject::as_live)
                .is_some_and(|current| current.generation_id == self.subject.payload_generation)
        {
            return Err(invalid(
                "active owner returned an already-committed generation",
            ));
        }
        Ok(())
    }
    pub(in crate::node_runtime::pg_store) fn direct_encryption_subject(
        &self,
        provisional: &crate::ObjectEncryption,
    ) -> Result<crate::ObjectEncryptionSubject, StagingWindowConfirmationError> {
        if self.subject.target != StagingTarget::DirectPut
            || provisional.key_binding() != crate::types::ObjectEncryptionKeyBinding::ProvisionalV1
            || provisional.payload_codec() != self.window.geometry.codec
        {
            return Err(StagingWindowConfirmationError::Window(
                "direct provisional encryption binding".into(),
            ));
        }
        Ok(crate::ObjectEncryptionSubject::standard_object(
            self.subject.bucket_incarnation,
            &self.subject.bucket,
            &self.subject.key,
            self.subject.payload_generation,
            provisional,
        ))
    }
}

const _: () = assert!(
    MAX_DESCRIPTOR_BYTES == crate::storage_rpc::complete_window_primary::MAX_DESCRIPTOR_BYTES
);

impl DirectStagingManifest {
    pub(in crate::node_runtime::pg_store) fn crc64(&self) -> u64 {
        self.crc64
    }
}

#[derive(Debug, thiserror::Error)]
pub(in crate::node_runtime) enum StagingWindowConfirmationError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Prefix(#[from] CompleteSettledPrefixError),
    #[error("staging window differs from its durable active frontier: {0}")]
    Window(String),
    #[error("staging window primary and designated witness prefixes differ")]
    PrefixDisagreement,
}

/// Scalar evidence only; no descriptor extraction or physical-effect consumer.
/// It must not be upgraded to a transferable grant without the later consuming
/// custody/physical-admission boundary. The admitted owner constructs it.
#[derive(Debug)]
pub(in crate::node_runtime) struct StagingWindowConfirmationObservation {
    pub(in crate::node_runtime::pg_store) prefix: MetadataCommandReplicaState,
    pub(in crate::node_runtime::pg_store) witness: Option<NodeId>,
}

/// Private borrowed inputs; possession is descriptive, not route authority.
pub(in crate::node_runtime) struct StagingWindowConfirmationRequest<'a> {
    subject: &'a StagingSubject,
    window: &'a StagingWindow,
    topology: &'a PgTopology,
    creation: Option<&'a StagingBucketResource>,
}

/// Payload evidence, never independent write/publication authority. The owning
/// frontend handoff retains this alongside its snapshot and physical custody.
pub(in crate::node_runtime::pg_store) struct DirectStagingManifest {
    pub(in crate::node_runtime::pg_store::payload_staging) bucket: BucketName,
    pub(in crate::node_runtime::pg_store::payload_staging) key: ObjectKey,
    pub(in crate::node_runtime::pg_store::payload_staging) generation: GenerationId,
    pub(in crate::node_runtime::pg_store::payload_staging) segment_hash: [u8; 16],
    pub(in crate::node_runtime::pg_store::payload_staging) data_pg: PgId,
    pub(in crate::node_runtime::pg_store::payload_staging) placement_epoch: ClusterEpoch,
    pub(in crate::node_runtime::pg_store::payload_staging) ec: EcShape,
    pub(in crate::node_runtime::pg_store::payload_staging) codec: SegmentPayloadCodec,
    pub(in crate::node_runtime::pg_store::payload_staging) logical_size: u64,
    pub(in crate::node_runtime::pg_store::payload_staging) stored_size: u64,
    pub(in crate::node_runtime::pg_store::payload_staging) crc64: u64,
    pub(in crate::node_runtime::pg_store::payload_staging) integrity:
        crate::types::SegmentShardIntegrityWitness,
}

/// Read-only receiver description. It cannot bind a command, install an index,
/// transfer custody or mint an effect grant. Exact durable equality is mandatory.
pub(in crate::node_runtime) struct StagingWindowInspectionDescription {
    descriptor: InspectionDescriptor,
}

enum InspectionDescriptor {
    Introduction(Box<IntroductionDescriptor>),
    Append(Box<AppendDescriptor>),
}

impl StagingWindowInspectionDescription {
    pub(in crate::node_runtime) fn decode(
        bytes: &[u8],
    ) -> Result<Self, StagingWindowConfirmationError> {
        let descriptor = match bytes.get(10) {
            Some(0) => IntroductionDescriptor::decode(bytes)
                .map(Box::new)
                .map(InspectionDescriptor::Introduction),
            Some(1) => AppendDescriptor::decode(bytes)
                .map(Box::new)
                .map(InspectionDescriptor::Append),
            _ => {
                return Err(StagingWindowConfirmationError::Window(
                    "not an introduction/lookahead descriptor".into(),
                ))
            }
        }
        .map_err(|error| StagingWindowConfirmationError::Window(error.to_string()))?;
        Ok(Self { descriptor })
    }

    pub(in crate::node_runtime) fn request<'a>(
        &'a self,
        topology: &'a PgTopology,
    ) -> Result<StagingWindowConfirmationRequest<'a>, StagingWindowConfirmationError> {
        let request = match &self.descriptor {
            InspectionDescriptor::Introduction(descriptor) => {
                descriptor
                    .0
                    .validate(topology)
                    .map_err(|error| StagingWindowConfirmationError::Window(error.to_string()))?;
                let creation = match &descriptor.0.resources.creation {
                    ResourceResponsibility::Live(proof)
                    | ResourceResponsibility::ReleaseDue(proof) => proof,
                    ResourceResponsibility::NoResource => {
                        unreachable!("decoded introduction resource")
                    }
                };
                StagingWindowConfirmationRequest {
                    subject: &descriptor.0.subject,
                    window: &descriptor.0.windows[0],
                    topology,
                    creation: Some(creation),
                }
            }
            InspectionDescriptor::Append(descriptor) => StagingWindowConfirmationRequest {
                subject: &descriptor.subject,
                window: &descriptor.window,
                topology,
                creation: None,
            },
        };
        if topology.object_pg_for(&request.subject.bucket, &request.subject.key)
            != request.subject.object_pg.get()
            || request.subject.data_pg(topology, request.window.index)
                != request.window.participants.data_pg.get()
        {
            return Err(StagingWindowConfirmationError::Window(
                "inspection descriptor topology".into(),
            ));
        }
        Ok(request)
    }
}

impl<'a> PendingStagingProtection<'a> {
    pub(in crate::node_runtime::pg_store) fn confirmation_request(
        &self,
    ) -> StagingWindowConfirmationRequest<'a> {
        StagingWindowConfirmationRequest {
            subject: self.subject,
            window: self.window,
            topology: self.topology,
            creation: self.creation_release,
        }
    }
}

impl StagingWindowConfirmationRequest<'_> {
    pub(in crate::node_runtime::pg_store) fn require_append_successor(
        &self,
        next: &Self,
        placement: crate::node_runtime::payload_placement::PayloadPlacementInput<'_>,
        epoch: ClusterEpoch,
        cutoff: u64,
    ) -> Result<(), StagingWindowConfirmationError> {
        let invalid =
            || StagingWindowConfirmationError::Window("remote append successor window".into());
        if self.subject != next.subject
            || self.topology != next.topology
            || next.creation.is_some()
            || self.window.index.checked_add(1) != Some(next.window.index)
            || next.window.start != self.window.end
            || next.window.geometry != self.window.geometry
            || next.window.participants.placement_epoch != epoch
            || next.window.cutoff_ms > cutoff
            || next.window.cutoff_ms > self.window.cutoff_ms
            || next.window.cutoff_ms <= crate::clock::current_time_millis()
        {
            return Err(invalid());
        }
        placement.require_topology(self.topology)?;
        let w = next.window;
        let (primary, locations) = placement.capture_segment_participants(
            epoch,
            w.participants.data_pg.get(),
            w.geometry.ec,
            &w.segment_hash,
            w.start,
        )?;
        if primary != w.participants.acknowledgement_primary
            || !locations
                .iter()
                .map(|l| l.node_id())
                .eq(w.participants.files.iter().copied())
        {
            return Err(invalid());
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn acknowledgement_batch(
        &self,
        logical_size: u64,
        acks: &std::collections::BTreeMap<u8, crate::WriteAck>,
    ) -> Result<StagingAcknowledgementRecords, StagingWindowConfirmationError> {
        let geometry = &self.window.geometry;
        let count = usize::from(geometry.ec.k) + usize::from(geometry.ec.m);
        let expected_size = geometry
            .codec
            .stored_size(logical_size)
            .map(|bytes| bytes.div_ceil(u64::from(geometry.ec.k)));
        if logical_size > geometry.logical_bound || acks.len() != count {
            return Err(StagingWindowConfirmationError::Window(
                "incomplete staging acknowledgement set".into(),
            ));
        }
        let mut rows = Vec::with_capacity(count);
        for index in 0..count {
            let index = index as u8;
            let ack = acks.get(&index).ok_or_else(|| {
                StagingWindowConfirmationError::Window(
                    "missing staging acknowledgement index".into(),
                )
            })?;
            if Some(ack.stored_size) != expected_size || ack.stored_size > geometry.shard_bound {
                return Err(StagingWindowConfirmationError::Window(
                    "staging acknowledgement geometry".into(),
                ));
            }
            rows.push((
                crate::ShardKey::new(&self.window.segment_hash, self.window.start.get(), index),
                *ack,
            ));
        }
        let participants = &self.window.participants;
        Ok(StagingAcknowledgementRecords {
            pg: participants.data_pg,
            primary: participants.acknowledgement_primary,
            epoch: participants.placement_epoch,
            rows,
        })
    }

    pub(in crate::node_runtime::pg_store) fn direct_manifest(
        &self,
        logical_size: u64,
        crc64: u64,
        acks: &std::collections::BTreeMap<u8, crate::WriteAck>,
    ) -> Result<DirectStagingManifest, StagingWindowConfirmationError> {
        if self.subject.target != StagingTarget::DirectPut
            || self.window.index != 0
            || self.window.start != self.subject.payload_generation
        {
            return Err(StagingWindowConfirmationError::Window(
                "direct payload manifest target".into(),
            ));
        }
        let batch = self.acknowledgement_batch(logical_size, acks)?;
        let window = self.window;
        let stored_size = window
            .geometry
            .codec
            .stored_size(logical_size)
            .ok_or_else(|| {
                StagingWindowConfirmationError::Window("direct payload manifest geometry".into())
            })?;
        let integrity = crate::types::SegmentShardIntegrityWitness::from_ordered_shard_acks(
            window.participants.data_pg.get(),
            window.geometry.ec,
            &window.segment_hash,
            window.start,
            stored_size,
            crc64,
            &batch
                .rows
                .iter()
                .map(|(key, ack)| (key, *ack))
                .collect::<Vec<_>>(),
        )
        .map_err(|e| {
            StagingWindowConfirmationError::Window(format!("direct payload integrity: {e}"))
        })?;
        Ok(DirectStagingManifest {
            bucket: self.subject.bucket.clone(),
            key: self.subject.key.clone(),
            generation: window.start,
            segment_hash: window.segment_hash,
            data_pg: window.participants.data_pg,
            placement_epoch: window.participants.placement_epoch,
            ec: window.geometry.ec,
            codec: window.geometry.codec,
            logical_size,
            stored_size,
            crc64,
            integrity,
        })
    }

    pub(in crate::node_runtime) fn require_direct_file_reservation(
        &self,
        pg: &PgStore,
        node: NodeId,
    ) -> Result<(), StagingWindowConfirmationError> {
        use crate::node_runtime::traits::PgMetadataStore;
        if !matches!(self.subject.target, StagingTarget::DirectPut)
            || self.window.start != self.subject.payload_generation
            || self.window.index != 0
            || pg.stream_allocation_issuer(node)? != self.subject.issuer
        {
            return Err(StagingWindowConfirmationError::Window(
                "direct file reservation target/issuer".into(),
            ));
        }
        sql::require_current_window(
            &pg.conn,
            self.subject,
            self.window,
            self.creation,
            self.topology,
        )
        .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))?;
        let generation = pg
            .get_object_generation_reservation(
                &self.subject.bucket,
                &self.subject.key,
                &self.subject.id.attempt,
            )
            .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))?;
        if generation != self.subject.payload_generation {
            return Err(StagingWindowConfirmationError::Window(
                "direct file reservation generation".into(),
            ));
        }
        Ok(())
    }

    pub(in crate::node_runtime::pg_store) fn direct_commit_snapshot(
        &self,
        pg: &PgStore,
        node: NodeId,
    ) -> Result<crate::DirectPutCommitStorageSnapshot, StagingWindowConfirmationError> {
        self.require_direct_file_reservation(pg, node)?;
        crate::node_runtime::client_facade::load_direct_put_commit_snapshot_from_pg(
            pg,
            node,
            &self.subject.bucket,
            &self.subject.key,
            &self.subject.id.attempt,
            self.subject.payload_generation,
        )
        .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))
    }

    /// Pure descriptor/placement validation. This returns no write authority;
    /// the owning file writer must retain custody and fence the guarded effect.
    pub(in crate::node_runtime) fn direct_file_location(
        &self,
        placement: crate::node_runtime::payload_placement::PayloadPlacementInput<'_>,
        shard_index: u8,
        logical_size: u64,
        bytes: usize,
    ) -> Result<(crate::cluster::ShardLocation, crate::ShardKey), StagingWindowConfirmationError>
    {
        if !matches!(self.subject.target, StagingTarget::DirectPut) {
            return Err(StagingWindowConfirmationError::Window(
                "direct file target".into(),
            ));
        }
        self.file_location(placement, shard_index, logical_size, bytes)
    }

    pub(in crate::node_runtime::pg_store) fn direct_ciphertext_size(
        &self,
        logical_size: u64,
    ) -> Result<u64, StagingWindowConfirmationError> {
        if !matches!(self.subject.target, StagingTarget::DirectPut)
            || logical_size > self.window.geometry.logical_bound
        {
            return Err(StagingWindowConfirmationError::Window(
                "direct encryption logical size exceeds captured window".into(),
            ));
        }
        self.window
            .geometry
            .codec
            .stored_size(logical_size)
            .ok_or_else(|| {
                StagingWindowConfirmationError::Window("direct encryption stored geometry".into())
            })
    }

    pub(in crate::node_runtime::pg_store) fn direct_encoding_geometry(
        &self,
        logical_size: u64,
    ) -> Result<([u8; 16], crate::GenerationId, crate::EcShape), StagingWindowConfirmationError>
    {
        self.direct_ciphertext_size(logical_size)?;
        Ok((
            self.window.segment_hash,
            self.window.start,
            self.window.geometry.ec,
        ))
    }

    pub(in crate::node_runtime::pg_store) fn require_stream_files(
        &self,
        pg: &PgStore,
        node: NodeId,
        segment: &crate::StreamUploadSegmentRecord,
        expected: &crate::types::ExpectedStreamAppendState,
    ) -> Result<(), StagingWindowConfirmationError> {
        let request = self.file_segment_request(segment)?;
        self.require_stream_inputs(&request, expected)?;
        self.require_stream_preparation(pg, &request)?;
        if pg.stream_allocation_issuer(node)? != self.subject.issuer {
            return Err(StagingWindowConfirmationError::Window(
                "stream file issuer".into(),
            ));
        }
        let owner = sql::load(&pg.conn, self.subject.object_pg, &self.subject.id.attempt)
            .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))?
            .ok_or_else(|| StagingWindowConfirmationError::Window("stream file owner".into()))?;
        owner
            .validate(self.topology)
            .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))?;
        let session = preparation::validate_session(pg, &owner, expected)
            .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))?;
        if session.next_segment_vid != self.window.end {
            return Err(StagingWindowConfirmationError::Window(
                "stream file consumed cursor".into(),
            ));
        }
        Ok(())
    }

    fn file_segment_request(
        &self,
        segment: &crate::StreamUploadSegmentRecord,
    ) -> Result<crate::PrepareStreamUploadSegmentAppendReq, StagingWindowConfirmationError> {
        let request = crate::PrepareStreamUploadSegmentAppendReq {
            session_id: segment.session_id.clone(),
            segment_index: segment.segment_index,
            size: segment.size,
            segment_crc64: segment.segment_crc64,
            payload_crc64: segment.payload_crc64,
        };
        self.require_allocated_segment(&request, segment)?;
        Ok(request)
    }

    pub(in crate::node_runtime) fn stream_file_location(
        &self,
        placement: crate::node_runtime::payload_placement::PayloadPlacementInput<'_>,
        segment: &crate::StreamUploadSegmentRecord,
        shard_index: u8,
        bytes: usize,
    ) -> Result<(crate::cluster::ShardLocation, crate::ShardKey), StagingWindowConfirmationError>
    {
        self.file_segment_request(segment)?;
        self.file_location(placement, shard_index, segment.size, bytes)
    }

    fn file_location(
        &self,
        placement: crate::node_runtime::payload_placement::PayloadPlacementInput<'_>,
        shard_index: u8,
        logical_size: u64,
        bytes: usize,
    ) -> Result<(crate::cluster::ShardLocation, crate::ShardKey), StagingWindowConfirmationError>
    {
        let window = self.window;
        let geometry = &window.geometry;
        if logical_size > geometry.logical_bound
            || geometry
                .codec
                .stored_size(logical_size)
                .map(|size| size.div_ceil(u64::from(geometry.ec.k)))
                != Some(bytes as u64)
            || bytes as u64 > geometry.shard_bound
        {
            return Err(StagingWindowConfirmationError::Window(
                "staging file geometry".into(),
            ));
        }
        placement.require_topology(self.topology)?;
        let (primary, locations) = placement.capture_segment_participants(
            window.participants.placement_epoch,
            window.participants.data_pg.get(),
            geometry.ec,
            &window.segment_hash,
            window.start,
        )?;
        if primary != window.participants.acknowledgement_primary
            || locations
                .iter()
                .map(|location| location.node_id())
                .collect::<Vec<_>>()
                != window.participants.files
        {
            return Err(StagingWindowConfirmationError::Window(
                "staging file participants".into(),
            ));
        }
        let location = locations.get(usize::from(shard_index)).ok_or_else(|| {
            StagingWindowConfirmationError::Window("staging file shard index".into())
        })?;
        Ok((
            *location,
            crate::ShardKey::new(&window.segment_hash, window.start.get(), shard_index),
        ))
    }

    pub(in crate::node_runtime::pg_store) fn require_stream_inputs(
        &self,
        request: &crate::PrepareStreamUploadSegmentAppendReq,
        expected: &crate::types::ExpectedStreamAppendState,
    ) -> Result<(), StagingWindowConfirmationError> {
        let (session, generation) = match (expected, &self.subject.target) {
            (
                crate::types::ExpectedStreamAppendState::PutObject {
                    session,
                    generation_id,
                },
                StagingTarget::StreamPut,
            ) if matches!(session.target, crate::StreamUploadTarget::PutObject) => {
                (session, *generation_id)
            }
            (
                crate::types::ExpectedStreamAppendState::UploadPart {
                    session,
                    upload,
                    part_generation_id,
                },
                StagingTarget::UploadPart {
                    upload_id,
                    upload_generation,
                    part_number,
                },
            ) if upload.upload_id == *upload_id
                && upload.object_generation_id == *upload_generation
                && upload.bucket == self.subject.bucket
                && upload.key == self.subject.key
                && matches!(&session.target, crate::StreamUploadTarget::UploadPart { upload_id: id, part_number: part } if id == upload_id && part == part_number) =>
            {
                (session, *part_generation_id)
            }
            _ => {
                return Err(StagingWindowConfirmationError::Window(
                    "allocation input target".into(),
                ))
            }
        };
        if session.session_id != self.subject.id.attempt
            || request.session_id != session.session_id
            || session.bucket != self.subject.bucket
            || session.key != self.subject.key
            || session.bucket_incarnation_generation != self.subject.bucket_incarnation
            || generation != self.subject.payload_generation
            || request.segment_index != self.window.index
        {
            return Err(StagingWindowConfirmationError::Window(
                "allocation input subject".into(),
            ));
        }
        Ok(())
    }

    pub(in crate::node_runtime::pg_store) fn require_allocated_segment(
        &self,
        request: &crate::PrepareStreamUploadSegmentAppendReq,
        segment: &crate::StreamUploadSegmentRecord,
    ) -> Result<(), StagingWindowConfirmationError> {
        if matches!(self.subject.target, StagingTarget::DirectPut)
            || segment.session_id != self.subject.id.attempt
            || segment.session_id != request.session_id
            || segment.segment_index != self.window.index
            || segment.segment_index != request.segment_index
            || segment.size != request.size
            || segment.size > self.window.geometry.logical_bound
            || segment.segment_crc64 != request.segment_crc64
            || segment.payload_crc64 != request.payload_crc64
            || segment.segment_okh != self.window.segment_hash
            || segment.segment_vid != self.window.start
            || segment.data_pg_id != self.window.participants.data_pg.get()
            || segment.placement_cluster_epoch != self.window.participants.placement_epoch
            || segment.ec_k != self.window.geometry.ec.k
            || segment.ec_m != self.window.geometry.ec.m
            || segment.shard_integrity.is_some()
        {
            return Err(StagingWindowConfirmationError::Window(
                "allocation response segment".into(),
            ));
        }
        Ok(())
    }
    pub(in crate::node_runtime::pg_store) fn require_stream_preparation(
        &self,
        pg: &PgStore,
        request: &crate::PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(), StagingWindowConfirmationError> {
        if matches!(self.subject.target, StagingTarget::DirectPut)
            || request.session_id != self.subject.id.attempt
            || request.segment_index != self.window.index
        {
            return Err(StagingWindowConfirmationError::Window(
                "stream allocation request subject".into(),
            ));
        }
        sql::require_current_window(
            &pg.conn,
            self.subject,
            self.window,
            self.creation,
            self.topology,
        )
        .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))
    }
    pub(in crate::node_runtime) fn encode_descriptor(
        &self,
    ) -> Result<Vec<u8>, StagingWindowConfirmationError> {
        let encode = || -> Result<Vec<u8>, DescriptorError> {
            let mut writer = Writer::new(u8::from(self.creation.is_none()));
            writer.subject(self.subject)?;
            writer.window(self.window)?;
            if let Some(creation) = self.creation {
                writer.resource(creation)?;
            }
            writer.finish()
        };
        encode().map_err(|error| StagingWindowConfirmationError::Window(error.to_string()))
    }

    pub(in crate::node_runtime) fn bind_deadline(
        &self,
        pg_id: PgId,
        topology: &PgTopology,
        epoch: ClusterEpoch,
        deadline: Instant,
    ) -> Result<(Instant, AdmittedRouteEffectFence), StoreError> {
        if self.subject.object_pg != pg_id
            || self.topology != topology
            || self.window.participants.placement_epoch > epoch
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "window confirmation admitted subject/topology",
            });
        }
        let started = Instant::now();
        let now = crate::clock::current_time_millis();
        let cutoff = self.window.cutoff_ms;
        let fence = AdmittedRouteEffectFence::bind_portable(epoch, cutoff, cutoff);
        fence.require_valid_for(epoch)?;
        let bound = started
            .checked_add(Duration::from_millis(cutoff.saturating_sub(now)))
            .unwrap_or(deadline);
        Ok((deadline.min(bound), fence))
    }

    /// Reuse the bounded read-only validator after a contender or ambiguous
    /// effect invalidated process-local prefix validation. This never replays,
    /// repairs durable state, or widens the window's original authority.
    pub(in crate::node_runtime) fn validate_reinspection_prefix(
        &self,
        pg: &PgStore,
        node: NodeId,
        epoch: ClusterEpoch,
        deadline: Instant,
        fence: AdmittedRouteEffectFence,
    ) -> Result<(), StagingWindowConfirmationError> {
        let (deadline, window_fence) =
            self.bind_deadline(PgId::new(pg.pg_id()), self.topology, epoch, deadline)?;
        pg.validate_complete_direct_reinspection_prefix(
            node,
            epoch,
            self.topology,
            deadline,
            fence.intersect(window_fence)?,
        )?;
        Ok(())
    }

    pub(in crate::node_runtime) fn inspect_primary<'a>(
        &self,
        pg: &'a PgStore,
        node: NodeId,
        epoch: ClusterEpoch,
    ) -> Result<CompleteSettledPrefixRead<'a>, StagingWindowConfirmationError> {
        let read = pg.inspect_complete_settled_prefix(node.as_u32(), epoch, self.topology)?;
        sql::require_current_window(
            &pg.conn,
            self.subject,
            self.window,
            self.creation,
            self.topology,
        )
        .map_err(|error| StagingWindowConfirmationError::Window(error.to_string()))?;
        Ok(read)
    }
}

impl StagingWindowConfirmationObservation {
    pub(in crate::node_runtime) fn settled(
        prefix: MetadataCommandReplicaState,
        witness: Option<NodeId>,
    ) -> Self {
        // Observational only: this constructor does not mint effect authority.
        Self { prefix, witness }
    }
}
