// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! The first coordinated integration boundary: a mandatory retained context
//! participates in the complete command identity, not a v13 side attachment.
//! The complete local installer uses this candidate grammar, but no live
//! builder/RPC selects it yet. The current v13 reader still rejects it; a local
//! installation observation is not frontend custody or physical authority.

use super::{ContextError, ContextualCommand, Input};
use crate::metadata_command::{context_carrier, MetadataCommandFormatError, MetadataCommandId};
use crate::PgTopology;

#[path = "payload_staging_complete_pending.rs"]
mod pending;
pub(in super::super::super) use pending::{CompletePendingIndexes, CompletePendingProjection};

/// Only this private child can mint the fragment-codec permit. The ordinary
/// node-runtime decode authority must not expose the draft separately from its
/// required context. Reexports name the type, never its constructor or fields.
pub(crate) struct CommandContextCodecAuthority(());

impl CommandContextCodecAuthority {
    const fn new() -> Self {
        Self(())
    }

    #[cfg(test)]
    pub(in crate::node_runtime) const fn new_for_test() -> Self {
        Self::new()
    }
}

// Keep complete framing admission tied to the independently owned nested
// format bounds; changing a source/descriptor cap cannot silently truncate it.
const _: () = assert!(
    context_carrier::MAX_CONTEXT_BYTES
        == super::MAX_SOURCE_BYTES + super::HEADER_BYTES + super::LENGTH_BYTES
);
const _: () = assert!(
    super::MAX_DESCRIPTOR_BYTES + super::HEADER_BYTES + super::LENGTH_BYTES
        <= context_carrier::MAX_CONTEXT_BYTES
);

#[derive(Debug, thiserror::Error)]
pub(in super::super::super) enum CarrierError {
    #[error(transparent)]
    Format(#[from] MetadataCommandFormatError),
    #[error(transparent)]
    Context(#[from] ContextError),
}

/// Borrows the complete pre-submission input, never a detachable draft
/// envelope. The byte/hash accessors all describe the SAME complete encoding.
/// Encoding confers no authority, acceptance observation or cleanup right.
pub(in super::super::super) struct EncodedContextualCommand<'a> {
    context: &'a ContextualCommand,
    bytes: Vec<u8>,
    checksum: u64,
    sha256: [u8; 32],
}

impl EncodedContextualCommand<'_> {
    pub(in super::super::super) fn require_direct_terminal_installation(
        &self,
        transaction: &crate::node_runtime::pg_store::command_log::PendingInstallationTransaction<
            '_,
        >,
        node: u32,
        topology: &PgTopology,
        proof: &super::DirectInstallationCheck<'_, '_>,
    ) -> Result<(), crate::StoreError> {
        let invalid = |_| crate::StoreError::RouteCapabilitySubjectMismatch {
            operation: "direct terminal installation retained snapshot",
        };
        if topology != self.topology() {
            return Err(crate::StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct terminal installation topology",
            });
        }
        let proof = match proof {
            super::DirectInstallationCheck::Remote(remote) => {
                return remote.require_context(transaction, node, topology, self.context);
            }
            super::DirectInstallationCheck::Embedded(proof) => proof,
        };
        proof
            .require_identity(self.id(), self.command_sha256())
            .map_err(invalid)?;
        proof
            .require_snapshot(transaction.store(), crate::NodeId::new(node), topology)
            .map_err(invalid)?;
        self.context
            .require_direct_terminal_installation(transaction.store())
    }
    pub(in super::super::super) fn require_slot_retirement_custody(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
    ) -> Result<(), ContextError> {
        self.context.require_slot_retirement_custody(pg)
    }

    /// Closed admitted preparation only; the source-owning permit cannot be
    /// minted by a store caller or detached from its all-member handoff.
    pub(in super::super::super) fn capture_admitted_part_cleanup(
        &self,
        permit: &crate::node_runtime::pg_store::AbandonedPartPreparation<'_, '_>,
        pg: &crate::node_runtime::pg_store::PgStore,
        node: u32,
        id: MetadataCommandId,
    ) -> Result<ContextualCommand, super::ContextError> {
        permit.require_context(self)?;
        self.context
            .capture_part_abandonment_in_open_transaction(pg, node, self.topology(), id)
    }

    pub(in super::super::super) fn application<'a>(
        &'a self,
        topology: &'a PgTopology,
    ) -> Result<super::CommandApplication<'a>, super::ApplicationError> {
        self.context.application(topology)
    }

    pub(in super::super::super) fn topology(&self) -> &PgTopology {
        &self.context.topology
    }
    pub(in super::super::super) fn validate_terminal_generation_owner(
        &self,
    ) -> Result<(), crate::StoreError> {
        match &self.context.input {
            Input::Ordinary(command) => command.payload().validate_terminal_generation_owner(),
            Input::Described(candidate) => candidate
                .command()
                .payload()
                .validate_terminal_generation_owner(),
            Input::AbandonedPart(candidate) => candidate
                .command()
                .payload()
                .validate_terminal_generation_owner(),
        }
    }

    /// An observation in the caller's pinned SQLite view, not a recovery or
    /// deletion capability. Always compare the COMPLETE identity; using the
    /// payload draft here would authenticate a different original command.
    pub(in super::super::super) fn inspect_terminal_in_open_transaction(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        node_id: u32,
    ) -> Result<
        crate::node_runtime::pg_store::command_log::MetadataCommandStartupDisposition,
        crate::StoreError,
    > {
        pg.complete_command_startup_disposition(node_id, self)
    }

    pub(in super::super::super) fn id(&self) -> MetadataCommandId {
        self.context.id()
    }

    pub(in super::super::super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(in super::super::super) fn checksum_crc64(&self) -> u64 {
        self.checksum
    }

    pub(in super::super::super) fn command_sha256(&self) -> [u8; 32] {
        self.sha256
    }
}

impl ContextualCommand {
    pub(in super::super::super) fn encode_command(
        &self,
    ) -> Result<EncodedContextualCommand<'_>, CarrierError> {
        if !self.topology.contains_pg(self.id().pg_id()) {
            return Err(ContextError::CommandPg.into());
        }
        let command = match &self.input {
            Input::Ordinary(command) => command,
            Input::Described(candidate) => candidate.command(),
            Input::AbandonedPart(candidate) => candidate.command(),
        };
        let bytes = context_carrier::encode(
            command,
            &self.encode_field()?,
            &CommandContextCodecAuthority::new(),
        )?;
        Ok(EncodedContextualCommand {
            context: self,
            checksum: checksum::crc64::checksum(&bytes),
            sha256: checksum::sha256::digest(&bytes),
            bytes,
        })
    }

    /// Decode only the complete current candidate grammar. The envelope and
    /// variant cannot be supplied independently or recovered via a raw view.
    /// Kind/subject/topology/source validation is mandatory after integrity,
    /// and still does not establish durable abandonment or recovery admission.
    pub(in super::super::super) fn decode_command(
        bytes: &[u8],
        checksum: u64,
        topology: &PgTopology,
    ) -> Result<Self, CarrierError> {
        Self::decode_complete(bytes, checksum, topology, false)
    }

    /// Restrict the nested source BEFORE decoding context. In particular an
    /// abort cannot recursively nest source-bearing aborts through this path.
    pub(super) fn decode_finalizer_source(
        bytes: &[u8],
        checksum: u64,
        topology: &PgTopology,
    ) -> Result<Self, CarrierError> {
        Self::decode_complete(bytes, checksum, topology, true)
    }

    fn decode_complete(
        bytes: &[u8],
        checksum: u64,
        topology: &PgTopology,
        finalizer_only: bool,
    ) -> Result<Self, CarrierError> {
        let (command, field) =
            context_carrier::decode(bytes, checksum, &CommandContextCodecAuthority::new())?;
        if finalizer_only
            && (!matches!(
                command.payload(),
                crate::metadata_command::MetadataCommandPayload::CommitStreamPart(_)
            ) || field != [1, 0, 0])
        {
            return Err(ContextError::CommandKind.into());
        }
        if !topology.contains_pg(command.id().pg_id()) {
            return Err(ContextError::CommandPg.into());
        }
        let context = Self::decode_field(command, field, topology)?;
        // Inner decoders reject malformed fields; this also pins canonicality
        // of their composition rather than trusting round-trip payload alone.
        if context.encode_command()?.bytes() != bytes {
            return Err(MetadataCommandFormatError::NonCanonical.into());
        }
        Ok(context)
    }
}
