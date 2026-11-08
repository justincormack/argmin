// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Required context-field codec for the coordinated command-carrier switch.
//! V1 is u16(version), u8(kind), then, for nonordinary kinds, u32(length) and
//! the existing independently versioned descriptor/source bytes. Ordinary has
//! an explicit tag, never an absent-field/default interpretation.
//!
//! This is NOT an authenticated command codec. No current command/RPC embeds
//! this field. Activation must put the complete field under the enclosing
//! command's checksum/hash, advance that grammar, and account for the combined
//! size at admission. In particular a v13 abort cannot authenticate the choice
//! between ordinary closure and an abandoned-part follow-up. Do not dispatch
//! this based on owner existence, caller flags or a separately supplied field.

use super::abandonment::{
    source::{AbandonedPartCleanupCommand, MAX_SOURCE_BYTES},
    AbandonmentError,
};
use super::candidate::{CandidateBindingError, DescribedStagingCommand};
use super::descriptor::{DescriptorError, MAX_DESCRIPTOR_BYTES};
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandId, MetadataCommandPayload};
use crate::PgTopology;

#[path = "payload_staging_application.rs"]
mod application;

#[path = "payload_staging_context_reissue.rs"]
mod reissue;

#[path = "payload_staging_context_match.rs"]
mod pending_match;

#[path = "payload_staging_context_epoch_match.rs"]
mod epoch_match;

#[path = "payload_staging_context_inventory.rs"]
mod inventory;

#[path = "payload_staging_context_preparation.rs"]
mod preparation;

#[cfg(test)]
pub(in super::super) use application::Applied;
pub(in super::super) use application::{ApplicationError, CommandApplication};
#[cfg(test)]
pub(in super::super) use inventory::ContextPendingRetention;
pub(in super::super) use inventory::{ContextPendingInventory, PendingInventoryObservation};
pub(in super::super) use pending_match::PendingContextMatch;
pub(in super::super) use preparation::{ContextPreparationFailure, PendingContextPreparation};
pub(in super::super) use reissue::ContextReissueFailure;

const VERSION: u16 = 1;
const HEADER_BYTES: usize = 3;
const LENGTH_BYTES: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum Kind {
    Ordinary = 0,
    Introduction = 1,
    Append = 2,
    AbandonedPart = 3,
}

impl Kind {
    fn decode(tag: u8) -> Result<Self, ContextError> {
        match tag {
            0 => Ok(Self::Ordinary),
            1 => Ok(Self::Introduction),
            2 => Ok(Self::Append),
            3 => Ok(Self::AbandonedPart),
            _ => Err(ContextError::Encoding("unknown context kind")),
        }
    }

    fn maximum_bytes(self) -> usize {
        match self {
            Self::Ordinary => 0,
            Self::Introduction | Self::Append => MAX_DESCRIPTOR_BYTES,
            Self::AbandonedPart => MAX_SOURCE_BYTES,
        }
    }

    fn encode(self, body: &[u8]) -> Result<Vec<u8>, ContextError> {
        if body.len() > self.maximum_bytes() {
            return Err(ContextError::Encoding("context exceeds kind bound"));
        }
        let mut output = Vec::with_capacity(HEADER_BYTES + LENGTH_BYTES + body.len());
        output.extend_from_slice(&VERSION.to_le_bytes());
        output.push(self as u8);
        if self != Self::Ordinary {
            output.extend_from_slice(
                &u32::try_from(body.len())
                    .expect("bounded field")
                    .to_le_bytes(),
            );
            output.extend_from_slice(body);
        }
        Ok(output)
    }

    /// Exhaustive over the production command enum, including explicitly
    /// ordinary commands. Adding a command cannot silently select ordinary.
    fn require_command(self, command: &MetadataCommandEnvelope) -> Result<(), ContextError> {
        use MetadataCommandPayload as P;
        let allowed = match command.payload() {
            P::ReserveObjectGeneration(_) | P::CreateStreamUpload(_) => self == Self::Introduction,
            P::AppendStreamSegment(_) => self == Self::Append,
            P::AbortStreamUpload(_) => matches!(self, Self::Ordinary | Self::AbandonedPart),
            P::CreateBucket(_)
            | P::PutBucketVersioning(_)
            | P::PutBucketAcl(_)
            | P::PutBucketProperty(_)
            | P::PutBucketSubresource(_)
            | P::MarkBucketDeleting(_)
            | P::DeleteFinalizedBucket(_)
            | P::ReleaseObjectGeneration(_)
            | P::ReserveObjectVersion(_)
            | P::CommitDirectPutObject(_)
            | P::CommitMultipartObject(_)
            | P::DeleteObjectVersion(_)
            | P::InsertDeleteMarker(_)
            | P::PutObjectMetadata(_)
            | P::CommitStreamPart(_)
            | P::CreateMultipartUpload(_)
            | P::AbortMultipartUpload(_)
            | P::DeleteObjectPayloadReclaim(_)
            | P::AdvanceMultipartCompletionBarrier(_) => self == Self::Ordinary,
        };
        if allowed {
            Ok(())
        } else {
            Err(ContextError::CommandKind)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum ContextError {
    #[error("invalid staging command context: {0}")]
    Encoding(&'static str),
    #[error("staging command context kind does not match the command")]
    CommandKind,
    #[error("staging command reissue must advance within the same PG")]
    ReissuePosition,
    #[error("pending reissue contexts do not describe an exact same-epoch replacement")]
    ReissueContext,
    #[error("recovery proposal is not an exact context-preserving reissue in a newer epoch")]
    EpochReissueContext,
    #[error("staging command context PG is outside the configured topology")]
    CommandPg,
    #[error("pending context inventory requires the owning PG transaction")]
    InventoryTransaction,
    #[error(transparent)]
    Snapshot(#[from] crate::MetadataError),
    #[error(transparent)]
    Candidate(#[from] CandidateBindingError),
    #[error(transparent)]
    Descriptor(#[from] DescriptorError),
    #[error(transparent)]
    Abandonment(#[from] AbandonmentError),
    #[error(transparent)]
    Retention(#[from] crate::StoreError),
}

enum Input {
    Ordinary(MetadataCommandEnvelope),
    Described(DescribedStagingCommand),
    AbandonedPart(AbandonedPartCleanupCommand),
}

/// Pre-submission description, not installation/custody/recovery authority.
/// No Clone, split, raw envelope projection or borrowed full candidate. Once
/// decoded, dispatch derives solely from the retained variant, not another
/// caller-selected handler. Application construction lives in a private child
/// module and accepts this complete input, never an independently supplied
/// envelope and handler. Raw field decoding is an ingress seam, not authority;
/// it must be enclosed by the future authenticated command decoder.
pub(in super::super) struct ContextualCommand {
    input: Input,
    topology: PgTopology,
}

impl ContextualCommand {
    pub(in super::super) fn described(
        candidate: DescribedStagingCommand,
        topology: &PgTopology,
    ) -> Result<Self, ContextError> {
        candidate.bind(topology)?;
        Ok(Self::from_described(candidate))
    }

    /// Consuming handoff of an existing description, not receiver validation
    /// against a new topology. Preserve its original captured topology; there
    /// is no separately supplied field/command or new authority here.
    pub(super) fn from_described(candidate: DescribedStagingCommand) -> Self {
        let topology = candidate.captured_topology().clone();
        Self {
            input: Input::Described(candidate),
            topology,
        }
    }

    pub(in super::super) fn abandoned_part(
        candidate: AbandonedPartCleanupCommand,
        topology: &PgTopology,
    ) -> Result<Self, ContextError> {
        candidate.bind(topology)?;
        Ok(Self {
            input: Input::AbandonedPart(candidate),
            topology: topology.clone(),
        })
    }

    pub(in super::super) fn id(&self) -> MetadataCommandId {
        match &self.input {
            Input::Ordinary(command) => command.id(),
            Input::Described(candidate) => candidate.command().id(),
            Input::AbandonedPart(candidate) => candidate.command().id(),
        }
    }

    /// Consuming pre-submission transformation, never recovery or installation
    /// authority. Preserve the exact context and captured topology, or return
    /// this complete input on rejection. Unresolved submission holders must
    /// retain their separate history/pair; they must not lend this owning value.
    pub(in super::super) fn into_reissue(
        self,
        id: MetadataCommandId,
    ) -> Result<Self, ContextReissueFailure> {
        reissue::derive(self, id)
    }

    /// Internal consuming description handoff. The owning submission transition
    /// keeps both candidates and its original observation; it never lends this
    /// input or exposes either result separately to its callers.
    pub(super) fn into_same_epoch_reissue_pair(
        self,
        id: MetadataCommandId,
    ) -> Result<(Self, Self), ContextReissueFailure> {
        reissue::derive_same_epoch_pair(self, id)
    }

    /// Descriptive epoch transition only. The owning recovery holder must keep
    /// both inputs and all prior installation observations; this neither proves
    /// source abandonment nor admits recovery under the proposed target epoch.
    pub(super) fn into_epoch_reissue_pair(
        self,
        id: MetadataCommandId,
    ) -> Result<(Self, Self), ContextReissueFailure> {
        reissue::derive_epoch_pair(self, id)
    }

    /// Descriptive exact retry / ordinary same-epoch replacement comparison.
    /// Not slot, recovery or custody authority; callers still need the atomic
    /// pending engine, authenticated carrier and retained submission history.
    pub(in super::super) fn pending_match(
        &self,
        source: &Self,
        configured_topology: &PgTopology,
    ) -> Result<PendingContextMatch, ContextError> {
        pending_match::classify(self, source, configured_topology)
    }

    /// Relationship inspection only, never recovery or pending-replacement
    /// permission. In particular ordinary MPU takeover matching is not allowed.
    pub(super) fn require_epoch_reissue_of(
        &self,
        source: &Self,
        configured_topology: &PgTopology,
    ) -> Result<(), ContextError> {
        epoch_match::require(self, source, configured_topology)
    }

    /// Only the retained ordinary finalizer can supply the original source.
    /// This is a read-only proposal, not a certificate of recovery authority.
    /// Submission holders never lend this input to their callers.
    pub(super) fn capture_part_abandonment_in_open_transaction(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        node_id: u32,
        topology: &PgTopology,
        id: MetadataCommandId,
    ) -> Result<Self, ContextError> {
        if topology != &self.topology {
            return Err(CandidateBindingError::Topology.into());
        }
        let Input::Ordinary(source) = &self.input else {
            return Err(ContextError::CommandKind);
        };
        if !matches!(
            source.payload(),
            MetadataCommandPayload::CommitStreamPart(_)
        ) {
            return Err(ContextError::CommandKind);
        }
        let candidate = super::abandonment::capture_part_cleanup_in_open_transaction(
            pg, node_id, topology, source, id,
        )?;
        Self::abandoned_part(candidate, topology)
    }

    pub(in super::super) fn encode_field(&self) -> Result<Vec<u8>, ContextError> {
        let (kind, body) = match &self.input {
            Input::Ordinary(_) => (Kind::Ordinary, Vec::new()),
            Input::Described(candidate) => (
                match candidate.command().payload() {
                    MetadataCommandPayload::ReserveObjectGeneration(_)
                    | MetadataCommandPayload::CreateStreamUpload(_) => Kind::Introduction,
                    MetadataCommandPayload::AppendStreamSegment(_) => Kind::Append,
                    _ => unreachable!("private described candidate"),
                },
                candidate.encode_descriptor_field()?,
            ),
            Input::AbandonedPart(candidate) => {
                (Kind::AbandonedPart, candidate.encode_source_field())
            }
        };
        kind.encode(&body)
    }

    /// Decodes only a bounded field, not a whole authenticated command. The
    /// future envelope decoder must invoke this on checksum/hash-covered bytes.
    /// Unknown kinds/versions, missing required data and trailing bytes reject;
    /// there is no fallback to an ordinary command on any decoding failure.
    pub(in super::super) fn decode_field(
        command: MetadataCommandEnvelope,
        field: &[u8],
        topology: &PgTopology,
    ) -> Result<Self, ContextError> {
        let (header, rest) = field
            .split_at_checked(HEADER_BYTES)
            .ok_or(ContextError::Encoding("missing context header"))?;
        if u16::from_le_bytes([header[0], header[1]]) != VERSION {
            return Err(ContextError::Encoding("unsupported context version"));
        }
        let kind = Kind::decode(header[2])?;
        kind.require_command(&command)?;
        if kind == Kind::Ordinary {
            if !rest.is_empty() {
                return Err(ContextError::Encoding(
                    "ordinary context has trailing bytes",
                ));
            }
            return Ok(Self {
                input: Input::Ordinary(command),
                topology: topology.clone(),
            });
        }
        let (length, body) = rest
            .split_at_checked(LENGTH_BYTES)
            .ok_or(ContextError::Encoding("missing context length"))?;
        let len = u32::from_le_bytes(length.try_into().expect("fixed length")) as usize;
        if len > kind.maximum_bytes() {
            return Err(ContextError::Encoding("context exceeds kind bound"));
        }
        if len != body.len() {
            return Err(ContextError::Encoding("context length/trailing bytes"));
        }
        match kind {
            Kind::Ordinary => unreachable!("handled above"),
            Kind::Introduction | Kind::Append => Self::described(
                DescribedStagingCommand::decode_descriptor_field(command, body, topology)?,
                topology,
            ),
            Kind::AbandonedPart => Self::abandoned_part(
                AbandonedPartCleanupCommand::decode_source_field(command, body, topology)?,
                topology,
            ),
        }
    }

    pub(in super::super) fn application<'a>(
        &'a self,
        topology: &'a PgTopology,
    ) -> Result<CommandApplication<'a>, ApplicationError> {
        CommandApplication::from_context(self, topology)
    }

    /// Owner-local exact-retention evidence for component tests. Production
    /// derivation is private to inventory and exposes only scalar observations.
    #[cfg(test)]
    pub(in super::super) fn pending_retention(
        &self,
        topology: &PgTopology,
    ) -> Result<ContextPendingRetention, ContextError> {
        inventory::derive_retention_for_test(self, topology)
    }

    /// Own this complete context and a read-only PG snapshot while inspecting
    /// its pending requirements. Not installation or acceptance authority.
    pub(in super::super) fn prepare_pending<'pg>(
        self,
        pg: &'pg crate::node_runtime::pg_store::PgStore,
        topology: &PgTopology,
    ) -> Result<PendingContextPreparation<'pg>, ContextPreparationFailure> {
        preparation::prepare(self, pg, topology)
    }

    /// Inspect the exact durable source of a source-bearing follow-up in the
    /// same snapshot as its inventory. Ordinary aborts cannot select this path.
    /// Only the owning submission layer may recover/retain submission history.
    pub(super) fn prepare_abandoned_part_pending<'pg>(
        self,
        pg: &'pg crate::node_runtime::pg_store::PgStore,
        node_id: u32,
        topology: &PgTopology,
    ) -> Result<PendingContextPreparation<'pg>, ContextPreparationFailure> {
        preparation::prepare_abandoned_part(self, pg, node_id, topology)
    }

    /// Scalar-only inspection for an owning multi-command scope. The caller
    /// keeps the complete contexts/history and one PG snapshot; no exact
    /// retention projection or installable pages escape this operation.
    pub(super) fn observe_pending_inventory_in_open_transaction(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        topology: &PgTopology,
    ) -> Result<PendingInventoryObservation, ContextError> {
        Ok(inventory::derive(self, pg, topology)?.observation())
    }

    /// Source-readiness inspection for an owning reissue scope. No bound source,
    /// raw context or reusable validation certificate escapes. Both candidates
    /// must be inspected in the caller's single pinned view; apply still reruns
    /// source authentication in its own mutation transaction.
    pub(super) fn observe_abandoned_part_inventory_in_open_transaction(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        node_id: u32,
        topology: &PgTopology,
    ) -> Result<PendingInventoryObservation, ContextError> {
        let Input::AbandonedPart(candidate) = &self.input else {
            return Err(ContextError::CommandKind);
        };
        candidate
            .bind(topology)?
            .require_abandoned_source_in_open_transaction(pg, node_id)?;
        self.observe_pending_inventory_in_open_transaction(pg, topology)
    }

    /// Owner-local component seam for tests already inside an apply transaction.
    /// Production inspection uses the owning, read-only preparation scope.
    #[cfg(test)]
    pub(in super::super) fn pending_inventory(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        topology: &PgTopology,
    ) -> Result<ContextPendingInventory, ContextError> {
        inventory::derive(self, pg, topology)
    }

    // Owner-local full-identity canary without reintroducing a raw projection.
    // Only test compilation provides this assertion; it returns no command or
    // candidate that could be reconstructed as another context.
    #[cfg(test)]
    pub(in super::super) fn assert_command_for_test(&self, expected: &MetadataCommandEnvelope) {
        let command = match &self.input {
            Input::Ordinary(command) => command,
            Input::Described(candidate) => candidate.command(),
            Input::AbandonedPart(candidate) => candidate.command(),
        };
        assert_eq!(command, expected);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staging_command_context_v1_source_v1_command_v13_frozen_composite() {
        // Exact source bytes remain the existing immutable specimen, including
        // its nested command version, length and CRC. Do not regenerate it.
        let source = crate::tests::frozen_hex_fixture(include_str!(
            "testdata/staging_abandonment_source_v1_command_v13.hex"
        ));
        assert_eq!(source.len(), 1728);
        let expected = [&[1, 0, 3, 192, 6, 0, 0][..], &source].concat();
        assert_eq!(Kind::AbandonedPart.encode(&source).unwrap(), expected);
    }
}
