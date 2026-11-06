// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Mandatory source carrier for the pending abandonment-follow-up integration.
//! This preserves the complete original command, not just its proof or a
//! reconstructed payload. Decoding is NOT a certificate of abandonment: every
//! application validates the durable terminal chain in the caller's transaction.
//! Tombstone V2 persists SHA-256 of the original command bytes. The CRC in this
//! carrier is only an integrity check; it cannot establish source identity.
//! No current command/RPC embeds this carrier yet.

use super::{AbandonmentError, AbandonmentOutcome};
use crate::metadata_command::{
    decode_metadata_command_envelope, MetadataCommandEnvelope, MetadataCommandFormatError,
    MetadataCommandPayload, MAX_METADATA_COMMAND_BYTES,
};
use crate::node_runtime::{pg_store::PgStore, MetadataCommandDecodeAuthority};
use crate::pg_topology::PgTopology;

const MAGIC: &[u8; 8] = b"stg-abnd";
const VERSION: u16 = 1;
const HEADER_BYTES: usize = MAGIC.len() + 2 + 8 + 4;
const MAX_SOURCE_BYTES: usize = HEADER_BYTES + MAX_METADATA_COMMAND_BYTES;

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in super::super::super) struct PartFinalizationSource(MetadataCommandEnvelope);

impl PartFinalizationSource {
    pub(in super::super::super) fn capture(
        source: &MetadataCommandEnvelope,
    ) -> Result<Self, SourceError> {
        Self::require_kind(source)?;
        let bytes = source.command_bytes();
        if bytes.len() > MAX_METADATA_COMMAND_BYTES {
            return Err(SourceError::Encoding("source command exceeds bound"));
        }
        if checksum::crc64::checksum(&bytes) != source.checksum_crc64() {
            return Err(SourceError::Encoding("source checksum"));
        }
        Ok(Self(source.clone()))
    }

    fn require_kind(source: &MetadataCommandEnvelope) -> Result<(), SourceError> {
        if !matches!(
            source.payload(),
            MetadataCommandPayload::CommitStreamPart(_)
        ) {
            return Err(SourceError::Encoding("source is not CommitStreamPart"));
        }
        Ok(())
    }

    pub(in super::super::super) fn encode(&self) -> Vec<u8> {
        let bytes = self.0.command_bytes();
        let mut output = Vec::with_capacity(HEADER_BYTES + bytes.len());
        output.extend_from_slice(MAGIC);
        output.extend_from_slice(&VERSION.to_le_bytes());
        output.extend_from_slice(&self.0.checksum_crc64().to_le_bytes());
        output.extend_from_slice(
            &u32::try_from(bytes.len())
                .expect("bounded source")
                .to_le_bytes(),
        );
        output.extend_from_slice(&bytes);
        output
    }

    pub(in super::super::super) fn decode(bytes: &[u8]) -> Result<Self, SourceError> {
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
        if len > MAX_METADATA_COMMAND_BYTES {
            return Err(SourceError::Encoding("source command exceeds bound"));
        }
        if len != command.len() {
            return Err(SourceError::Encoding("source length/trailing bytes"));
        }
        if checksum::crc64::checksum(command) != checksum {
            return Err(SourceError::Encoding("source checksum"));
        }
        let source =
            decode_metadata_command_envelope(command, &MetadataCommandDecodeAuthority::new())?;
        Self::require_kind(&source)?;
        Ok(Self(source))
    }

    pub(in super::super::super) fn apply_closure_in_open_transaction(
        &self,
        pg: &PgStore,
        topology: &PgTopology,
        node_id: u32,
        cleanup: &MetadataCommandEnvelope,
    ) -> Result<AbandonmentOutcome, AbandonmentError> {
        // Neither canonical bytes nor a valid CRC proves that this command was
        // abandoned. The durable V2 tombstone (or its compacted receipt) binds
        // the original command's SHA-256, not just its CRC64. Do not cache the
        // disposition: validate the exact source and
        // follow-up under the same transaction that transfers custody.
        super::apply_part_closure_in_open_transaction(pg, topology, node_id, &self.0, cleanup)
    }
}
