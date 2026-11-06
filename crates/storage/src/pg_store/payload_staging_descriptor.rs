// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bounded, canonical command-field descriptors. These are descriptions, never
//! write/cleanup authority or command-acceptance certificates. Their bytes must
//! become mandatory, checksummed fields of reserve/create/append commands at
//! coordinated activation; no current envelope or RPC accepts them yet.
//!
//! Only an introduction or one lookahead window is encodable. In particular, a
//! peer cannot supply a Published owner, selected references or terminal proof
//! custody through this grammar. Replica interpretation uses no clock or route.

use super::*;
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandLogIndex};
use crate::node_runtime::pg_store::PgStore;

#[path = "payload_staging_descriptor_binding.rs"]
mod binding;

#[cfg(test)]
pub(super) use binding::{apply_append_for_test, apply_introduction_for_test};
pub(super) use binding::{BoundAppendDescriptor, BoundIntroductionDescriptor};

const MAGIC: &[u8; 8] = b"staging\0";
const VERSION: u16 = 1;
// Secondary framing/allocation bound only: subject and proof fields must also
// satisfy their individual durable/transport limits in central validation.
// File participants have the independent EC maximum of 32. This limit is also
// an input to the eventual enclosing command/RPC size admission.
const MAX_DESCRIPTOR_BYTES: usize = 16 * 1024;

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum DescriptorError {
    #[error("invalid staging descriptor: {0}")]
    Encoding(&'static str),
    #[error(transparent)]
    Invariant(#[from] StagingInvariant),
}

/// The complete introduction is reconstructed from its minimal immutable
/// inputs. Resource roles, lifecycle, prefix/end and horizon are not wire tags
/// that a caller can independently choose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in super::super) struct IntroductionDescriptor(StagingOwnerRecord);

/// Includes the original immutable subject, not just a reusable session name.
/// Application compares it to the retained owner before consulting live state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in super::super) struct AppendDescriptor {
    subject: StagingSubject,
    window: StagingWindow,
}

impl IntroductionDescriptor {
    pub(super) fn capture(owner: &CheckedStagingOwner<'_>) -> Result<Self, DescriptorError> {
        let owner = owner.record;
        if owner.lifecycle != StagingLifecycle::Active
            || owner.windows.len() != 1
            || !owner.references.is_empty()
        {
            return Err(DescriptorError::Encoding("not an introduction"));
        }
        let descriptor = Self(owner.clone());
        descriptor.encode()?;
        Ok(descriptor)
    }

    pub(in super::super) fn encode(&self) -> Result<Vec<u8>, DescriptorError> {
        let mut out = Writer::new(0);
        out.subject(&self.0.subject)?;
        out.window(&self.0.windows[0])?;
        let creation = match &self.0.resources.creation {
            ResourceResponsibility::Live(proof) | ResourceResponsibility::ReleaseDue(proof) => {
                proof
            }
            ResourceResponsibility::NoResource => unreachable!("checked introduction"),
        };
        out.resource(creation)?;
        out.finish()
    }

    pub(in super::super) fn decode(bytes: &[u8]) -> Result<Self, DescriptorError> {
        let mut input = Reader::new(bytes, 0)?;
        let subject = input.subject()?;
        let window = input.window(&subject)?;
        let creation = input.resource()?;
        input.finish()?;
        let generation = StagingGenerationResource {
            reservation_id: subject.id.attempt.clone(),
            generation: subject.payload_generation,
        };
        let multipart = matches!(subject.target, StagingTarget::UploadPart { .. });
        let owner = StagingOwnerRecord {
            subject,
            lifecycle: StagingLifecycle::Active,
            reserved_end: window.end,
            maximum_cutoff_ms: window.cutoff_ms,
            windows: vec![window],
            references: vec![],
            resources: StagingResources {
                generation: ResourceResponsibility::Live(generation),
                creation: if multipart {
                    ResourceResponsibility::ReleaseDue(creation)
                } else {
                    ResourceResponsibility::Live(creation)
                },
                finalization: ResourceResponsibility::NoResource,
                abandoned_finalizations: vec![],
            },
        };
        owner.validate_structure()?;
        Ok(Self(owner))
    }
}

impl AppendDescriptor {
    pub(super) fn capture(
        owner: &CheckedStagingOwner<'_>,
        window: StagingWindow,
    ) -> Result<Self, DescriptorError> {
        if window.participants.data_pg.get()
            != owner.record.subject.data_pg(owner.topology, window.index)
        {
            return Err(StagingInvariant::Placement.into());
        }
        let owner = owner.record;
        if owner.lifecycle != StagingLifecycle::Active
            || matches!(owner.subject.target, StagingTarget::DirectPut)
            || window.index as usize != owner.windows.len()
            || window.start != owner.reserved_end
        {
            return Err(DescriptorError::Encoding("not the next append window"));
        }
        let descriptor = Self {
            subject: owner.subject.clone(),
            window,
        };
        descriptor.validate()?;
        descriptor.encode()?;
        Ok(descriptor)
    }

    fn validate(&self) -> Result<(), DescriptorError> {
        self.subject.validate()?;
        self.window.validate(&self.subject)?;
        if matches!(self.subject.target, StagingTarget::DirectPut)
            || self.window.index == 0
            || self.window.index > MAX_STREAM_SEGMENTS
            || self
                .subject
                .first_vid()
                .get()
                .checked_add(u64::from(self.window.index))
                != Some(self.window.start.get())
        {
            return Err(DescriptorError::Encoding("invalid append position"));
        }
        Ok(())
    }

    pub(in super::super) fn encode(&self) -> Result<Vec<u8>, DescriptorError> {
        let mut out = Writer::new(1);
        out.subject(&self.subject)?;
        out.window(&self.window)?;
        out.finish()
    }

    pub(in super::super) fn decode(bytes: &[u8]) -> Result<Self, DescriptorError> {
        let mut input = Reader::new(bytes, 1)?;
        let subject = input.subject()?;
        let window = input.window(&subject)?;
        input.finish()?;
        let descriptor = Self { subject, window };
        descriptor.validate()?;
        Ok(descriptor)
    }
}

struct Writer(Vec<u8>);

impl Writer {
    fn new(kind: u8) -> Self {
        let mut out = Self(MAGIC.to_vec());
        out.u16(VERSION);
        out.u8(kind);
        out
    }
    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }
    fn u16(&mut self, value: u16) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    fn u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }
    fn string(&mut self, value: &str) -> Result<(), DescriptorError> {
        if value.len() > MAX_DESCRIPTOR_BYTES {
            return Err(DescriptorError::Encoding("string exceeds descriptor bound"));
        }
        self.u32(value.len() as u32);
        self.0.extend_from_slice(value.as_bytes());
        self.check_size()
    }
    fn check_size(&self) -> Result<(), DescriptorError> {
        if self.0.len() > MAX_DESCRIPTOR_BYTES {
            Err(DescriptorError::Encoding("descriptor exceeds bound"))
        } else {
            Ok(())
        }
    }
    fn finish(self) -> Result<Vec<u8>, DescriptorError> {
        self.check_size()?;
        Ok(self.0)
    }

    fn subject(&mut self, subject: &StagingSubject) -> Result<(), DescriptorError> {
        self.string(subject.id.attempt.as_str())?;
        self.u64(subject.id.introduction.cluster_epoch().get());
        self.u32(subject.id.introduction.pg_id().get());
        self.u64(subject.id.introduction.log_index().get());
        self.string(subject.bucket.as_str())?;
        self.u64(subject.bucket_incarnation);
        self.string(subject.key.as_str())?;
        // Object PG and originating epoch equal the introduction by invariant.
        self.u64(subject.payload_generation.get());
        match &subject.target {
            StagingTarget::DirectPut => self.u8(0),
            StagingTarget::StreamPut => self.u8(1),
            StagingTarget::UploadPart {
                upload_id,
                upload_generation,
                part_number,
            } => {
                self.u8(2);
                self.string(upload_id.as_str())?;
                self.u64(upload_generation.get());
                self.u32(*part_number);
            }
        }
        self.u32(subject.issuer.node_id.as_u32());
        self.0.extend_from_slice(&subject.issuer.lifetime);
        Ok(())
    }

    fn window(&mut self, window: &StagingWindow) -> Result<(), DescriptorError> {
        // Owner/issuer inherit the subject. Explicit physical hash, bounds and
        // end are retained and validated, not recomputed from a newer profile.
        self.u32(window.index);
        self.0.extend_from_slice(&window.segment_hash);
        self.u64(window.start.get());
        self.u64(window.end.get());
        self.u64(window.issued_at_ms);
        self.u64(window.cutoff_ms);
        self.u8(window.geometry.codec.tag());
        self.u8(window.geometry.ec.k);
        self.u8(window.geometry.ec.m);
        self.u64(window.geometry.logical_bound);
        self.u64(window.geometry.stored_bound);
        self.u64(window.geometry.shard_bound);
        self.u32(window.participants.data_pg.get());
        self.u64(window.participants.placement_epoch.get());
        self.u8(u8::try_from(window.participants.files.len())
            .map_err(|_| DescriptorError::Encoding("file participant count"))?);
        for node in &window.participants.files {
            self.u32(node.as_u32());
        }
        self.u32(window.participants.acknowledgement_primary.as_u32());
        Ok(())
    }

    fn resource(&mut self, resource: &StagingBucketResource) -> Result<(), DescriptorError> {
        let proof = &resource.identity;
        self.string(proof.bucket.as_str())?;
        self.string(&proof.reservation_id)?;
        self.string(&proof.owner_token)?;
        self.u64(proof.cluster_epoch.get());
        self.u64(proof.execution_generation);
        self.u64(proof.incarnation);
        self.string(&proof.operation_kind)?;
        self.u64(proof.created_at);
        match &proof.target_context {
            None => self.u8(0),
            Some(target) => {
                self.u8(1);
                self.string(target)?;
            }
        }
        self.u64(resource.observed_lease_deadline);
        self.u32(resource.release_pg.get());
        Ok(())
    }
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8], kind: u8) -> Result<Self, DescriptorError> {
        if bytes.len() > MAX_DESCRIPTOR_BYTES {
            return Err(DescriptorError::Encoding("descriptor exceeds bound"));
        }
        let mut input = Self(bytes);
        if input.take(MAGIC.len())? != MAGIC || input.u16()? != VERSION || input.u8()? != kind {
            return Err(DescriptorError::Encoding("descriptor magic/version/kind"));
        }
        Ok(input)
    }
    fn take(&mut self, len: usize) -> Result<&'a [u8], DescriptorError> {
        let (value, rest) = self
            .0
            .split_at_checked(len)
            .ok_or(DescriptorError::Encoding("truncated descriptor"))?;
        self.0 = rest;
        Ok(value)
    }
    fn bytes<const N: usize>(&mut self) -> Result<[u8; N], DescriptorError> {
        Ok(self.take(N)?.try_into().expect("exact length"))
    }
    fn u8(&mut self) -> Result<u8, DescriptorError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, DescriptorError> {
        Ok(u16::from_le_bytes(self.bytes()?))
    }
    fn u32(&mut self) -> Result<u32, DescriptorError> {
        Ok(u32::from_le_bytes(self.bytes()?))
    }
    fn u64(&mut self) -> Result<u64, DescriptorError> {
        Ok(u64::from_le_bytes(self.bytes()?))
    }
    fn string(&mut self) -> Result<String, DescriptorError> {
        let len = self.u32()? as usize;
        std::str::from_utf8(self.take(len)?)
            .map(str::to_owned)
            .map_err(|_| DescriptorError::Encoding("non-UTF8 string"))
    }
    fn epoch(&mut self) -> Result<ClusterEpoch, DescriptorError> {
        ClusterEpoch::new(self.u64()?).ok_or(DescriptorError::Encoding("zero epoch"))
    }
    fn generation(&mut self) -> Result<GenerationId, DescriptorError> {
        GenerationId::new(self.u64()?).ok_or(DescriptorError::Encoding("zero generation"))
    }
    fn finish(self) -> Result<(), DescriptorError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(DescriptorError::Encoding("trailing descriptor bytes"))
        }
    }

    fn subject(&mut self) -> Result<StagingSubject, DescriptorError> {
        let attempt = SessionId::try_from(self.string()?)
            .map_err(|_| DescriptorError::Encoding("session id"))?;
        let originating_epoch = self.epoch()?;
        let object_pg = PgId::new(self.u32()?);
        let index = MetadataCommandLogIndex::new(self.u64()?)
            .ok_or(DescriptorError::Encoding("zero command index"))?;
        let bucket = BucketName::try_from(self.string()?)
            .map_err(|_| DescriptorError::Encoding("bucket"))?;
        let bucket_incarnation = self.u64()?;
        let key =
            ObjectKey::try_from(self.string()?).map_err(|_| DescriptorError::Encoding("key"))?;
        let payload_generation = self.generation()?;
        let target = match self.u8()? {
            0 => StagingTarget::DirectPut,
            1 => StagingTarget::StreamPut,
            2 => StagingTarget::UploadPart {
                upload_id: UploadId::try_from(self.string()?)
                    .map_err(|_| DescriptorError::Encoding("upload id"))?,
                upload_generation: self.generation()?,
                part_number: self.u32()?,
            },
            _ => return Err(DescriptorError::Encoding("target tag")),
        };
        Ok(StagingSubject {
            id: StagingOwnerId {
                attempt,
                introduction: MetadataCommandId::new(originating_epoch, object_pg, index),
            },
            bucket,
            bucket_incarnation,
            key,
            object_pg,
            originating_epoch,
            payload_generation,
            target,
            issuer: StreamAllocationIssuer {
                node_id: NodeId::new(self.u32()?),
                lifetime: self.bytes()?,
            },
        })
    }

    fn window(&mut self, subject: &StagingSubject) -> Result<StagingWindow, DescriptorError> {
        let index = self.u32()?;
        let segment_hash = self.bytes()?;
        let start = self.generation()?;
        let end = self.generation()?;
        let issued_at_ms = self.u64()?;
        let cutoff_ms = self.u64()?;
        let codec = SegmentPayloadCodec::from_u8(self.u8()?)
            .ok_or(DescriptorError::Encoding("codec tag"))?;
        let ec = EcShape {
            k: self.u8()?,
            m: self.u8()?,
        };
        ec::EcConfig::new(ec.k, ec.m).map_err(|_| StagingInvariant::Geometry)?;
        let geometry = StagingGeometry {
            codec,
            ec,
            logical_bound: self.u64()?,
            stored_bound: self.u64()?,
            shard_bound: self.u64()?,
        };
        let data_pg = PgId::new(self.u32()?);
        let placement_epoch = self.epoch()?;
        let count = self.u8()?;
        if usize::from(count) != usize::from(ec.k) + usize::from(ec.m) {
            return Err(DescriptorError::Encoding("file participant count"));
        }
        let files = (0..count)
            .map(|_| self.u32().map(NodeId::new))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(StagingWindow {
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
                placement_epoch,
                files,
                acknowledgement_primary: NodeId::new(self.u32()?),
            },
        })
    }

    fn resource(&mut self) -> Result<StagingBucketResource, DescriptorError> {
        let bucket = BucketName::try_from(self.string()?)
            .map_err(|_| DescriptorError::Encoding("proof bucket"))?;
        let reservation_id = self.string()?;
        let owner_token = self.string()?;
        let cluster_epoch = self.epoch()?;
        let execution_generation = self.u64()?;
        let incarnation = self.u64()?;
        let operation_kind = self.string()?;
        let created_at = self.u64()?;
        let target_context = match self.u8()? {
            0 => None,
            1 => Some(self.string()?),
            _ => return Err(DescriptorError::Encoding("optional target tag")),
        };
        Ok(StagingBucketResource {
            identity: StableBucketReservation {
                bucket,
                reservation_id,
                owner_token,
                cluster_epoch,
                execution_generation,
                incarnation,
                operation_kind,
                created_at,
                target_context,
            },
            observed_lease_deadline: self.u64()?,
            release_pg: PgId::new(self.u32()?),
        })
    }
}
