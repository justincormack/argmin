// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owned command description, from primary capture or checked field decoding.
//! Keep the exact draft command, required descriptor and validated topology
//! together through pending projection/application/reissue. This is descriptive
//! data, not acceptance, recovery authorization or physical-write authority.
//! The production command carrier/custody switch has not been activated.

use super::capture::{StagingCapture, StagingCaptureError};
use super::descriptor::{
    AppendDescriptor, BoundAppendDescriptor, BoundIntroductionDescriptor, DescriptorError,
    IntroductionDescriptor, PendingStagingProtection,
};
use super::{append, introduction};
use crate::metadata_command::{
    BucketWriteReservationProof, MetadataCommandEnvelope, MetadataCommandId, MetadataCommandPayload,
};
use crate::node_runtime::pg_store::PgStore;
use crate::{NodeId, PgTopology};
use std::sync::Arc;

#[derive(Clone, PartialEq, Eq)]
enum CommandDescriptor {
    Introduction(Arc<IntroductionDescriptor>),
    Append(Arc<AppendDescriptor>),
}

/// No public fields, unchecked pair constructor, mutable command access or split.
/// The command alone remains v13 and does NOT authenticate these descriptors.
/// Future serialization must include them in the mandatory checksummed carrier.
pub(in super::super) struct DescribedStagingCommand {
    command: MetadataCommandEnvelope,
    descriptor: CommandDescriptor,
    topology: PgTopology,
}

/// Borrowed inspection, not another complete candidate. In production this
/// contains only the envelope: no descriptor, bind/reissue operation, conversion
/// to a submission or Deref back to the owned description. Holders must not lend
/// their DescribedStagingCommand, whose reissue operation can mint a new holder.
pub(in super::super) struct StagingCommandInspection<'a> {
    command: &'a MetadataCommandEnvelope,
    #[cfg(test)]
    descriptor: &'a CommandDescriptor,
}

impl<'a> StagingCommandInspection<'a> {
    pub(in super::super) fn command(&self) -> &'a MetadataCommandEnvelope {
        self.command
    }

    // Owner-local evidence only; production inspection cannot recover the
    // descriptor bytes and use the decoder to detach/reset submission history.
    #[cfg(test)]
    pub(super) fn descriptor_bytes(&self) -> Vec<u8> {
        match self.descriptor {
            CommandDescriptor::Introduction(descriptor) => descriptor.encode().unwrap(),
            CommandDescriptor::Append(descriptor) => descriptor.encode().unwrap(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum CandidateBindingError {
    #[error("command kind does not carry a staging descriptor")]
    CommandKind,
    #[error(transparent)]
    Descriptor(#[from] DescriptorError),
    #[error(transparent)]
    Introduction(#[from] introduction::IntroductionError),
    #[error(transparent)]
    Append(#[from] append::AppendError),
    #[error("staging candidate reissue must advance the same PG")]
    ReissuePosition,
    #[error("staging candidate topology differs from the configured application topology")]
    Topology,
}

enum BoundDescriptor<'a> {
    Introduction(BoundIntroductionDescriptor<'a>),
    Append(BoundAppendDescriptor<'a>),
}

pub(in super::super) struct BoundStagingCandidate<'a>(BoundDescriptor<'a>);

#[derive(Debug, PartialEq, Eq)]
pub(in super::super) enum CandidateApplied {
    Introduction(introduction::IntroductionApplied),
    Append(append::AppendApplied),
}

impl StagingCapture<'_> {
    pub(in super::super) fn introduction_candidate(
        &self,
        pg: &PgStore,
        node_id: NodeId,
        command: MetadataCommandEnvelope,
        proof: &BucketWriteReservationProof,
    ) -> Result<DescribedStagingCommand, StagingCaptureError> {
        let descriptor = self.introduction_for_command(pg, node_id, &command, proof)?;
        Ok(DescribedStagingCommand {
            command,
            descriptor: CommandDescriptor::Introduction(Arc::new(descriptor)),
            topology: self.topology.clone(),
        })
    }

    pub(in super::super) fn append_candidate(
        &self,
        pg: &PgStore,
        node_id: NodeId,
        command: MetadataCommandEnvelope,
    ) -> Result<DescribedStagingCommand, StagingCaptureError> {
        let descriptor = self.append_for_command(pg, node_id, &command)?;
        Ok(DescribedStagingCommand {
            command,
            descriptor: CommandDescriptor::Append(Arc::new(descriptor)),
            topology: self.topology.clone(),
        })
    }
}

impl DescribedStagingCommand {
    /// Immutable construction context, not a new admission or route lookup.
    /// Submission inspection deliberately does not expose this owning type.
    pub(super) fn captured_topology(&self) -> &PgTopology {
        &self.topology
    }

    /// Decode the required field for this exact command kind, validating its
    /// command correspondence and the receiver's configured topology before
    /// returning an owned description. No primary recapture, SQL, clock or
    /// route lookup occurs; old issuers/cutoffs remain unchanged on replicas.
    ///
    /// This is NOT an authenticated carrier decoder. The future enclosing
    /// command codec must checksum/authenticate this mandatory field together
    /// with the command; the current v13 envelope cannot establish that bond.
    pub(in super::super) fn decode_descriptor_field(
        command: MetadataCommandEnvelope,
        bytes: &[u8],
        configured_topology: &PgTopology,
    ) -> Result<Self, CandidateBindingError> {
        let descriptor = match command.payload() {
            MetadataCommandPayload::ReserveObjectGeneration(_)
            | MetadataCommandPayload::CreateStreamUpload(_) => {
                CommandDescriptor::Introduction(Arc::new(IntroductionDescriptor::decode(bytes)?))
            }
            MetadataCommandPayload::AppendStreamSegment(_) => {
                CommandDescriptor::Append(Arc::new(AppendDescriptor::decode(bytes)?))
            }
            _ => return Err(CandidateBindingError::CommandKind),
        };
        let candidate = Self {
            command,
            descriptor,
            topology: configured_topology.clone(),
        };
        candidate.bind(configured_topology)?;
        Ok(candidate)
    }

    pub(in super::super) fn command(&self) -> &MetadataCommandEnvelope {
        &self.command
    }

    /// Compare retained inputs without lending either candidate or allocating
    /// another serialized description. Command position/payload match separately.
    pub(super) fn has_same_context(&self, other: &Self) -> bool {
        self.topology == other.topology && self.descriptor == other.descriptor
    }

    // Serialization belongs to the prospective enclosing command, not to an
    // inspection view borrowed from an unresolved submission holder.
    pub(super) fn encode_descriptor_field(&self) -> Result<Vec<u8>, DescriptorError> {
        match &self.descriptor {
            CommandDescriptor::Introduction(descriptor) => descriptor.encode(),
            CommandDescriptor::Append(descriptor) => descriptor.encode(),
        }
    }

    pub(in super::super) fn inspect(&self) -> StagingCommandInspection<'_> {
        StagingCommandInspection {
            command: &self.command,
            #[cfg(test)]
            descriptor: &self.descriptor,
        }
    }

    pub(in super::super) fn bind<'a>(
        &'a self,
        configured_topology: &'a PgTopology,
    ) -> Result<BoundStagingCandidate<'a>, CandidateBindingError> {
        if configured_topology != &self.topology {
            return Err(CandidateBindingError::Topology);
        }
        let bound = match &self.descriptor {
            CommandDescriptor::Introduction(descriptor) => {
                BoundDescriptor::Introduction(descriptor.bind(configured_topology, &self.command)?)
            }
            CommandDescriptor::Append(descriptor) => {
                BoundDescriptor::Append(descriptor.bind(configured_topology, &self.command)?)
            }
        };
        Ok(BoundStagingCandidate(bound))
    }

    /// Necessary ordering only, not permission to reissue or replace a slot.
    /// The enclosing publication/recovery boundary must establish that authority.
    /// Keep the original description byte-for-byte; never sample time or routes.
    pub(in super::super) fn reissued_at(
        &self,
        id: MetadataCommandId,
    ) -> Result<Self, CandidateBindingError> {
        if !super::command_follows(self.command.id(), id) {
            return Err(CandidateBindingError::ReissuePosition);
        }
        let candidate = Self {
            command: MetadataCommandEnvelope::new(id, self.command.payload().clone()),
            descriptor: self.descriptor.clone(),
            topology: self.topology.clone(),
        };
        candidate.bind(&self.topology)?;
        Ok(candidate)
    }
}

impl BoundStagingCandidate<'_> {
    pub(in super::super) fn pending_protection(&self) -> PendingStagingProtection<'_> {
        match &self.0 {
            BoundDescriptor::Introduction(bound) => bound.pending_protection(),
            BoundDescriptor::Append(bound) => bound.pending_protection(),
        }
    }

    pub(in super::super) fn apply_in_open_transaction(
        self,
        pg: &PgStore,
    ) -> Result<CandidateApplied, CandidateBindingError> {
        match self.0 {
            BoundDescriptor::Introduction(bound) => bound
                .apply_in_open_transaction(pg)
                .map(CandidateApplied::Introduction)
                .map_err(CandidateBindingError::Introduction),
            BoundDescriptor::Append(bound) => bound
                .apply_in_open_transaction(pg)
                .map(CandidateApplied::Append)
                .map_err(CandidateBindingError::Append),
        }
    }
}

#[cfg(test)]
impl DescribedStagingCommand {
    pub(super) fn descriptor_bytes(&self) -> Vec<u8> {
        match &self.descriptor {
            CommandDescriptor::Introduction(descriptor) => descriptor.encode().unwrap(),
            CommandDescriptor::Append(descriptor) => descriptor.encode().unwrap(),
        }
    }
}
