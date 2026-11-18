// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate complete-command grammar for the coordinated ownership switch.
//!
//! This is not a second supported production format. No live RPC/log decoder
//! calls this module. Its node-runtime-only consumer must retain and validate
//! the mandatory context; the returned payload envelope is a draft, NOT the
//! identity/checksum of the complete command. At activation this replaces the
//! v13 writer/reader, rather than becoming a fallback alongside it.

use super::{
    canonical_command_bytes_with_version, MetadataCommandEnvelope, MetadataCommandFormatError,
    MetadataCommandLogEntryDecoder, MetadataCommandRecordFormat, MAX_METADATA_COMMAND_BYTES,
    METADATA_COMMAND_MAGIC,
};
use crate::node_runtime::CommandContextCodecAuthority;

const VERSION: u16 = 14;
// Preserve the existing payload capacity. An abandonment context may retain a
// complete maximal finalizer (payload plus ordinary context), its V2 header
// (22), and the enclosing source-bearing context framing (7).
// This is candidate-format capacity. Selected read-only RPC frame caps admit it
// before activation; their live command decoder still accepts only v13.
pub(crate) const MAX_CONTEXT_BYTES: usize = MAX_METADATA_COMMAND_BYTES + 3 + 22 + 7;
pub(crate) const MAX_COMPLETE_COMMAND_BYTES: usize = MAX_METADATA_COMMAND_BYTES + MAX_CONTEXT_BYTES;

fn invalid(reason: &str) -> MetadataCommandFormatError {
    MetadataCommandFormatError::InvalidPayload(reason.to_owned())
}

/// The context owner validates the field's mandatory kind and canonical inner
/// encoding. The format owner only combines the two without adding an optional
/// field, a compatibility tag, or a separately checksummed attachment.
pub(crate) fn encode(
    command: &MetadataCommandEnvelope,
    context: &[u8],
    _authority: &CommandContextCodecAuthority,
) -> Result<Vec<u8>, MetadataCommandFormatError> {
    if context.is_empty() {
        return Err(invalid("missing required command context"));
    }
    if context.len() > MAX_CONTEXT_BYTES {
        return Err(invalid("metadata command context exceeds size limit"));
    }
    let mut bytes = canonical_command_bytes_with_version(command.id(), command.payload(), VERSION);
    if bytes.len() > MAX_METADATA_COMMAND_BYTES {
        return Err(invalid("metadata command payload exceeds size limit"));
    }
    // The payload decoder knows its end; the independently versioned context
    // consumes the remaining bytes and rejects trailing data itself.
    bytes.extend_from_slice(context);
    Ok(bytes)
}

/// Only the owning context decoder may turn these parts into a usable input.
/// Integrity covers ALL bytes before any nested payload allocation/decoding.
/// This checksum is corruption detection, not peer/recovery authentication.
pub(crate) fn decode<'a>(
    bytes: &'a [u8],
    checksum: u64,
    _authority: &CommandContextCodecAuthority,
) -> Result<(MetadataCommandEnvelope, &'a [u8]), MetadataCommandFormatError> {
    if bytes.len() > MAX_COMPLETE_COMMAND_BYTES {
        return Err(invalid("complete metadata command exceeds size limit"));
    }
    let mut decoder = MetadataCommandLogEntryDecoder::new(bytes);
    if decoder.read_bytes()? != METADATA_COMMAND_MAGIC {
        return Err(MetadataCommandFormatError::UnknownMagic);
    }
    let version = decoder.read_u16()?;
    if version != VERSION {
        return Err(MetadataCommandFormatError::UnsupportedVersion {
            format: MetadataCommandRecordFormat::Applied,
            version,
        });
    }
    if checksum::crc64::checksum(bytes) != checksum {
        return Err(invalid("complete metadata command checksum mismatch"));
    }
    let id = decoder.read_command_id()?;
    let kind = decoder.read_u16()?;
    let payload = decoder.read_metadata_command_payload(kind)?;
    if decoder.offset > MAX_METADATA_COMMAND_BYTES {
        return Err(invalid("metadata command payload exceeds size limit"));
    }
    let context = &bytes[decoder.offset..];
    if context.len() > MAX_CONTEXT_BYTES {
        return Err(invalid("metadata command context exceeds size limit"));
    }
    if context.is_empty() {
        return Err(invalid("missing required command context"));
    }
    let command = MetadataCommandEnvelope::new(id, payload);
    if canonical_command_bytes_with_version(id, command.payload(), VERSION)
        != bytes[..decoder.offset]
    {
        return Err(MetadataCommandFormatError::NonCanonical);
    }
    Ok((command, context))
}
