// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Existing pending-envelope outer grammar with candidate complete-command
//! capacity. Borrowed, untrusted framing only; no command decoder or authority.

use super::*;

pub(crate) const MAX_RESPONSE_BYTES: usize =
    1 + 8 + 4 + crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES + 1;
const _: () = assert!(MAX_RESPONSE_BYTES + 6 <= STORAGE_RPC_MAX_PAYLOAD_LEN);

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BorrowedPendingResponse<'a> {
    pub(crate) command: Option<(u64, &'a [u8])>,
    pub(crate) publication_started: bool,
}

fn invalid(reason: &'static str) -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope(reason)
}

fn validate_len(len: usize) -> Result<(), StorageRpcPayloadError> {
    if len == 0 || len > crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES {
        return Err(invalid("complete pending command size"));
    }
    Ok(())
}

pub(crate) fn encode(
    command: Option<(u64, &[u8])>,
    publication_started: bool,
) -> Result<Vec<u8>, StorageRpcPayloadError> {
    if command.is_none() && publication_started {
        return Err(invalid("absent complete pending publication"));
    }
    let mut out = Vec::new();
    match command {
        None => put_u8(&mut out, 0),
        Some((checksum, bytes)) => {
            validate_len(bytes.len())?;
            put_u8(&mut out, 1);
            put_u64(&mut out, checksum);
            put_bytes(&mut out, bytes);
        }
    }
    put_u8(&mut out, u8::from(publication_started));
    Ok(out)
}

pub(crate) fn decode(bytes: &[u8]) -> Result<BorrowedPendingResponse<'_>, StorageRpcPayloadError> {
    if bytes.len() > MAX_RESPONSE_BYTES {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: bytes.len(),
            limit: MAX_RESPONSE_BYTES,
        });
    }
    let mut decoder = StorageRpcDecoder::new(bytes);
    let command = match decoder.read_u8()? {
        0 => None,
        1 => {
            let checksum = decoder.read_u64()?;
            let len = decoder.read_u32()? as usize;
            validate_len(len)?;
            Some((checksum, decoder.read_exact(len)?))
        }
        _ => return Err(invalid("complete pending presence tag")),
    };
    let publication_started = match decoder.read_u8()? {
        0 => false,
        1 if command.is_some() => true,
        _ => return Err(invalid("complete pending publication tag")),
    };
    decoder.finish()?;
    Ok(BorrowedPendingResponse {
        command,
        publication_started,
    })
}

#[cfg(test)]
#[path = "complete_pending_tests.rs"]
mod tests;
