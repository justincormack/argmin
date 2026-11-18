// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Mandatory source carrier for the pending abandonment-follow-up integration.
//! This preserves the complete original command, not just its proof or a
//! reconstructed payload. Decoding is NOT a certificate of abandonment: every
//! application validates the durable terminal chain in the caller's transaction.
//! Tombstone V2 persists SHA-256 of the original command bytes. The CRC in this
//! carrier is only an integrity check; it cannot establish source identity.
//! Source V2 embeds the complete V14 finalizer with mandatory ordinary context
//! V1. No current production command/RPC embeds this candidate carrier yet.

use super::super::abandonment::{self, AbandonmentError, AbandonmentOutcome};
use super::{CarrierError, ContextualCommand, Input};
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandFormatError, MetadataCommandId, MetadataCommandPayload,
    MAX_METADATA_COMMAND_BYTES,
};
use crate::node_runtime::pg_store::{MetadataCommandStartupDisposition, PgStore};
use crate::pg_topology::PgTopology;
use std::sync::Arc;

// The release implementation can inspect the immutable source here without
// exporting a raw source/proof accessor to the surrounding store or caller.
#[path = "payload_staging_terminal_release.rs"]
mod terminal_release;
pub(crate) use terminal_release::CompleteTerminalReservationRelease;

const MAGIC: &[u8; 8] = b"stg-abnd";
const VERSION: u16 = 2;
const HEADER_BYTES: usize = MAGIC.len() + 2 + 8 + 4;
// A finalizer always carries exactly the three-byte ordinary context. Its
// payload keeps the supported 2 MiB limit; source framing is additional.
const MAX_FINALIZER_BYTES: usize = MAX_METADATA_COMMAND_BYTES + 3;
pub(in super::super) const MAX_SOURCE_BYTES: usize = HEADER_BYTES + MAX_FINALIZER_BYTES;

#[derive(Debug, thiserror::Error)]
pub(in super::super::super) enum SourceError {
    #[error("invalid staging abandonment source: {0}")]
    Encoding(&'static str),
    #[error(transparent)]
    Command(#[from] MetadataCommandFormatError),
}

/// Descriptive, cloneable evidence, not authority. The source cannot be swapped
/// independently at the apply boundary, and has no caller-visible raw envelope.
/// A source may outlive its log entry, but only authenticated durable terminal
/// evidence (including compacted receipts) permits the custody transition.
#[derive(Clone)]
pub(in super::super::super) struct PartFinalizationSource(Arc<ContextualCommand>);

impl std::fmt::Debug for PartFinalizationSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PartFinalizationSource")
            .field("id", &self.0.id())
            .finish_non_exhaustive()
    }
}

impl PartialEq for PartFinalizationSource {
    fn eq(&self, other: &Self) -> bool {
        // Both inputs are necessarily ordinary finalizers in the same exact
        // grammar. Comparing their immutable input binds every encoded field.
        self.0.topology == other.0.topology && self.draft() == other.draft()
    }
}

impl Eq for PartFinalizationSource {}

/// Owned description of one cleanup and its indispensable original source.
/// Private fields prevent independently replacing/dropping source context at
/// application or reissue. This is NOT an abandonment/recovery certificate;
/// durable source authentication remains inside the application transaction.
pub(in super::super::super) struct AbandonedPartCleanupCommand {
    command: MetadataCommandEnvelope,
    source: PartFinalizationSource,
    topology: PgTopology,
}

pub(in super::super::super) struct BoundPartCleanup<'a>(&'a AbandonedPartCleanupCommand);

impl PartFinalizationSource {
    pub(in super::super::super) fn capture(
        source: &ContextualCommand,
    ) -> Result<Self, SourceError> {
        let Input::Ordinary(command) = &source.input else {
            return Err(SourceError::Encoding(
                "source is not an ordinary CommitStreamPart",
            ));
        };
        if !matches!(
            command.payload(),
            MetadataCommandPayload::CommitStreamPart(_)
        ) {
            return Err(SourceError::Encoding("source is not CommitStreamPart"));
        }
        let encoded = source.encode_command().map_err(Self::carrier_error)?;
        if encoded.bytes().len() > MAX_FINALIZER_BYTES {
            return Err(SourceError::Encoding("source command exceeds bound"));
        }
        // Copy descriptive evidence only. No submission holder lends its
        // ContextualCommand; its installation history remains in that holder.
        Ok(Self(Arc::new(ContextualCommand {
            input: Input::Ordinary(command.clone()),
            topology: source.topology.clone(),
        })))
    }

    fn carrier_error(error: CarrierError) -> SourceError {
        match error {
            CarrierError::Format(error) => SourceError::Command(error),
            CarrierError::Context(_) => SourceError::Encoding("invalid source context"),
        }
    }

    // Semantic input for the private abandonment component. Never offered as
    // a pre-submission context or as the original command's durable identity.
    pub(in super::super) fn draft(&self) -> &MetadataCommandEnvelope {
        let Input::Ordinary(command) = &self.0.input else {
            unreachable!("source captures only an ordinary finalizer");
        };
        command
    }

    #[cfg(test)]
    pub(in super::super::super) fn capture_for_test(
        draft: &MetadataCommandEnvelope,
    ) -> Result<Self, SourceError> {
        let input = ContextualCommand {
            input: Input::Ordinary(draft.clone()),
            topology: super::super::tests::topology(),
        };
        Self::capture(&input)
    }

    #[cfg(test)]
    pub(in super::super::super) fn decode_for_test(bytes: &[u8]) -> Result<Self, SourceError> {
        Self::decode(bytes, &super::super::tests::topology())
    }

    pub(in super::super::super) fn encode(&self) -> Vec<u8> {
        let encoded = self.0.encode_command().expect("captured bounded finalizer");
        let bytes = encoded.bytes();
        let mut output = Vec::with_capacity(HEADER_BYTES + bytes.len());
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(&VERSION.to_le_bytes());
        output.extend_from_slice(&encoded.checksum_crc64().to_le_bytes());
        output.extend_from_slice(
            &u32::try_from(bytes.len())
                .expect("bounded source")
                .to_le_bytes(),
        );
        output.extend_from_slice(bytes);
        output
    }

    pub(in super::super::super) fn decode(
        bytes: &[u8],
        topology: &PgTopology,
    ) -> Result<Self, SourceError> {
        // Bound the complete carrier and its nested allocation BEFORE decoding
        // any command vectors. No duration, clock or route is interpreted here.
        if bytes.len() > MAX_SOURCE_BYTES {
            return Err(SourceError::Encoding("source carrier exceeds bound"));
        }
        let (header, command) = bytes
            .split_at_checked(HEADER_BYTES)
            .ok_or(SourceError::Encoding("truncated source header"))?;
        if &header[..8] != MAGIC
            || u16::from_le_bytes(header[8..10].try_into().expect("fixed header")) != VERSION
        {
            return Err(SourceError::Encoding("source magic/version"));
        }
        let checksum = u64::from_le_bytes(header[10..18].try_into().expect("fixed header"));
        let len = u32::from_le_bytes(header[18..22].try_into().expect("fixed header")) as usize;
        if len > MAX_FINALIZER_BYTES {
            return Err(SourceError::Encoding("source command exceeds bound"));
        }
        if len != command.len() {
            return Err(SourceError::Encoding("source length/trailing bytes"));
        }
        if checksum::crc64::checksum(command) != checksum {
            return Err(SourceError::Encoding("source checksum"));
        }
        let source = ContextualCommand::decode_finalizer_source(command, checksum, topology)
            .map_err(Self::carrier_error)?;
        Self::capture(&source)
    }

    pub(in super::super::super) fn bind_cleanup(
        self,
        command: MetadataCommandEnvelope,
        topology: &PgTopology,
    ) -> Result<AbandonedPartCleanupCommand, AbandonmentError> {
        if topology != &self.0.topology {
            return Err(AbandonmentError::Topology);
        }
        abandonment::validate_part_closure_subject(topology, self.draft(), &command)?;
        Ok(AbandonedPartCleanupCommand {
            command,
            source: self,
            topology: topology.clone(),
        })
    }
}

impl AbandonedPartCleanupCommand {
    /// Interpret the required source field of the future cleanup carrier. This
    /// does not authenticate either this field or the enclosing command: current
    /// command-v13 bytes cannot carry it. No current RPC dispatch uses this API.
    pub(in super::super::super) fn decode_source_field(
        command: MetadataCommandEnvelope,
        bytes: &[u8],
        topology: &PgTopology,
    ) -> Result<Self, AbandonmentError> {
        PartFinalizationSource::decode(bytes, topology)?.bind_cleanup(command, topology)
    }

    pub(in super::super::super) fn command(&self) -> &MetadataCommandEnvelope {
        &self.command
    }

    /// The original finalizer is part of identity, even when two cleanup
    /// envelopes are byte-identical. This does not authenticate its tombstone.
    pub(in super::super::super) fn has_same_context(&self, other: &Self) -> bool {
        self.topology == other.topology && self.source == other.source
    }

    pub(in super::super::super) fn bind(
        &self,
        configured_topology: &PgTopology,
    ) -> Result<BoundPartCleanup<'_>, AbandonmentError> {
        if configured_topology != &self.topology {
            return Err(AbandonmentError::Topology);
        }
        Ok(BoundPartCleanup(self))
    }

    /// Ordering/correspondence only, not recovery authorization. Preserve the
    /// *original finalizer*, not the immediately preceding cleanup command.
    pub(in super::super::super) fn reissued_at(
        &self,
        id: MetadataCommandId,
    ) -> Result<Self, AbandonmentError> {
        if !super::super::command_follows(self.command.id(), id) {
            return Err(AbandonmentError::ReissuePosition);
        }
        let command = MetadataCommandEnvelope::new(id, self.command.payload().clone());
        abandonment::validate_part_closure_subject(&self.topology, self.source.draft(), &command)?;
        Ok(Self {
            command,
            source: self.source.clone(),
            topology: self.topology.clone(),
        })
    }

    pub(in super::super) fn encode_source_field(&self) -> Vec<u8> {
        self.source.encode()
    }

    #[cfg(test)]
    pub(in super::super::super) fn source_bytes(&self) -> Vec<u8> {
        self.encode_source_field()
    }
}

impl BoundPartCleanup<'_> {
    pub(in super::super) fn source_has_started_publication(
        &self,
        pg: &PgStore,
    ) -> Result<bool, AbandonmentError> {
        let encoded = self
            .0
            .source
            .0
            .encode_command()
            .map_err(PartFinalizationSource::carrier_error)?;
        Ok(pg.complete_source_has_started_publication(&encoded)?)
    }

    pub(in super::super) fn inspect_source_terminal(
        &self,
        pg: &PgStore,
        node_id: u32,
    ) -> Result<MetadataCommandStartupDisposition, AbandonmentError> {
        let encoded = self
            .0
            .source
            .0
            .encode_command()
            .map_err(PartFinalizationSource::carrier_error)?;
        Ok(encoded.inspect_terminal_in_open_transaction(pg, node_id)?)
    }

    /// Read-only inspection in the caller's PG snapshot. Replica application
    /// must re-run this validation in its mutation transaction; this returns no
    /// certificate, raw source or permission to release/delete anything.
    pub(in super::super::super) fn require_abandoned_source_in_open_transaction(
        &self,
        pg: &PgStore,
        node_id: u32,
    ) -> Result<(), AbandonmentError> {
        abandonment::require_abandoned_source_in_open_transaction(pg, node_id, self)
    }

    /// Project cleanup payload using the immutable original source, not a live
    /// session that may already have been removed. This is protection inventory,
    /// not authorization to apply the cleanup or to delete any listed shard.
    pub(in super::super::super) fn pending_payload_references(
        &self,
    ) -> Result<Vec<crate::types::ShardScavengerPayloadReference>, crate::StoreError> {
        let MetadataCommandPayload::CommitStreamPart(source) = self.0.source.draft().payload()
        else {
            unreachable!("private source carrier requires CommitStreamPart");
        };
        let mut references = Vec::new();
        PgStore::extend_command_payload_references_with_stream_encryption(
            &mut references,
            self.0.command.payload(),
            |bucket, key, session| {
                if *bucket != source.bucket || *key != source.key || *session != source.session_id {
                    return Err(crate::StoreError::RouteCapabilitySubjectMismatch {
                        operation: "pending-abandonment-payload-subject",
                    });
                }
                Ok(Some(source.upload.encryption.clone()))
            },
        )?;
        Ok(references)
    }

    /// Required route retention for the future source-bearing pending carrier.
    /// The abort envelope alone has no UploadPart-finalization proof. Retain its
    /// original source route and exact bucket release route independently of the
    /// follow-up epoch; reissue must not reinterpret either at its newer epoch.
    /// This read-only projection is not recovery/installation authority. It must
    /// be installed atomically with authenticated source-bearing command bytes,
    /// never patched onto an ordinary v13 abort slot after installation.
    pub(in super::super::super) fn pending_route_dependencies(
        &self,
    ) -> Vec<crate::node_runtime::pg_store::command_log::PendingMetadataCommandRouteDependency>
    {
        use crate::node_runtime::pg_store::command_log::{
            PendingMetadataCommandRouteDependency, PendingRouteTarget,
        };

        let source = self.0.source.draft();
        let MetadataCommandPayload::CommitStreamPart(part) = source.payload() else {
            unreachable!("private source carrier requires CommitStreamPart");
        };
        let proof = &part.bucket_write_reservation;
        let exact = |cluster_epoch, pg_id| PendingMetadataCommandRouteDependency {
            cluster_epoch,
            target: PendingRouteTarget::ExactPg(pg_id),
        };
        let mut routes = vec![
            exact(
                self.0.command.id().cluster_epoch(),
                self.0.command.id().pg_id(),
            ),
            exact(source.id().cluster_epoch(), source.id().pg_id()),
            exact(
                proof.cluster_epoch,
                crate::PgId::new(self.0.topology.bucket_pg_for(&proof.bucket)),
            ),
        ];
        routes.sort_unstable();
        routes.dedup();
        routes
    }

    pub(in super::super) fn inputs(
        &self,
    ) -> (
        &PgTopology,
        &MetadataCommandEnvelope,
        &MetadataCommandEnvelope,
    ) {
        (&self.0.topology, self.0.source.draft(), &self.0.command)
    }

    pub(in super::super::super) fn apply_in_open_transaction(
        self,
        pg: &PgStore,
        node_id: u32,
    ) -> Result<AbandonmentOutcome, AbandonmentError> {
        // Neither canonical bytes nor a valid CRC proves that this command was
        // abandoned. The durable V2 tombstone (or its compacted receipt) binds
        // the original command's SHA-256, not just its CRC64. Do not cache the
        // disposition: validate the exact source and
        // follow-up under the same transaction that transfers custody.
        abandonment::apply_part_closure_in_open_transaction(pg, node_id, self)
    }
}
