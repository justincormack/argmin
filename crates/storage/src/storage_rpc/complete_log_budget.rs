// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate retained-log response outcome, governed by RPC frame v37.
//! Ordinary pages keep their count-prefixed grammar. The impossible count
//! u32::MAX denotes a request-bound validation-budget hint, NOT a successful
//! read or a resume cursor. No page data or recovery authority accompanies it.

use super::*;

const BUDGET_HINT: u32 = u32::MAX;

fn invalid() -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope("complete log validation budget hint")
}

fn require_prefix(
    request: &StorageRpcMetadataCommandLogHashRangeRequest,
    covered_end: u64,
) -> Result<MetadataCommandLogIndex, StorageRpcPayloadError> {
    if covered_end < request.first_log_index.get() || covered_end >= request.last_log_index.get() {
        return Err(invalid());
    }
    MetadataCommandLogIndex::new(covered_end).ok_or_else(invalid)
}

fn require_kind(kind: StorageRpcMessageKind) -> Result<(), StorageRpcPayloadError> {
    match kind {
        StorageRpcMessageKind::MetadataCommandRetainedLogEntries
        | StorageRpcMessageKind::MetadataCommandRetainedLogHashes => Ok(()),
        _ => Err(invalid()),
    }
}

pub(crate) fn encode_response(
    kind: StorageRpcMessageKind,
    request: &StorageRpcMetadataCommandLogHashRangeRequest,
    first_unread: u64,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    require_kind(kind)?;
    let covered_end = require_prefix(request, first_unread.checked_sub(1).ok_or_else(invalid)?)?;
    let mut bytes = Vec::new();
    put_u32(&mut bytes, BUDGET_HINT);
    put_u16(&mut bytes, kind as u16);
    bytes.extend_from_slice(&encode_metadata_command_log_hash_range_request(request));
    put_u64(&mut bytes, covered_end.get());
    Ok(encode_storage_rpc_success_response(&bytes))
}

/// Authentication of the enclosing response is checked by the transport first.
/// Bind every subject/range field as well as the operation before permitting a
/// strict prefix retry. Non-hint bytes still require the owner's page decoder.
pub(crate) fn decode_hint(
    kind: StorageRpcMessageKind,
    request: &StorageRpcMetadataCommandLogHashRangeRequest,
    bytes: &[u8],
) -> Result<Option<MetadataCommandLogIndex>, StorageRpcPayloadError> {
    let mut decoder = StorageRpcDecoder::new(bytes);
    if decoder.read_u32()? != BUDGET_HINT {
        return Ok(None);
    }
    require_kind(kind)?;
    if decoder.read_u16()? != kind as u16 {
        return Err(invalid());
    }
    let expected = encode_metadata_command_log_hash_range_request(request);
    if decoder.read_exact(expected.len())? != expected {
        return Err(invalid());
    }
    let end = require_prefix(request, decoder.read_u64()?)?;
    decoder.finish()?;
    Ok(Some(end))
}

#[cfg(test)]
#[path = "complete_log_budget_tests.rs"]
mod tests;
