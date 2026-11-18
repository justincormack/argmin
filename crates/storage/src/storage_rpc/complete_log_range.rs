// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Same retained-entry outer grammar, with an explicitly selected complete
//! command capacity. No command decoder or recovery authority lives here.
//! Live RPC dispatch does not use this candidate integration path yet.

use super::*;

pub(crate) const MAX_COMMAND_BYTES: usize = 16 * 1024 * 1024;
const MAX_ENTRIES: usize = STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES as usize;
// Count plus 31 maximal fixed headers; command bodies have a separate budget.
const MAX_RESPONSE_BYTES: usize =
    4 + MAX_ENTRIES * (8 + 2 * 9 + 2 * 10 + 1 + 40) + MAX_COMMAND_BYTES;
const _: () = assert!(MAX_RESPONSE_BYTES + 6 <= STORAGE_RPC_MAX_PAYLOAD_LEN);

#[derive(Debug)]
pub(crate) enum WireKind<'a> {
    Applied { checksum: u64, bytes: &'a [u8] },
    Abandoned { checksum: u64, sha256: [u8; 32] },
}

#[derive(Debug)]
pub(crate) struct WireEntry<'a> {
    pub(crate) log_index: u64,
    pub(crate) previous_log_hash: MetadataCommandLogHash,
    pub(crate) log_hash: MetadataCommandLogHash,
    pub(crate) pre_state_digest: Option<CanonicalStateDigest>,
    pub(crate) post_state_digest: Option<CanonicalStateDigest>,
    pub(crate) kind: WireKind<'a>,
}

fn invalid(reason: &'static str) -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope(reason)
}

fn charge(used: &mut usize, bytes: usize) -> Result<(), StorageRpcPayloadError> {
    if bytes == 0 || bytes > crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES {
        return Err(invalid("complete log command size"));
    }
    if bytes > MAX_COMMAND_BYTES - *used {
        return Err(invalid("complete log response command byte budget"));
    }
    *used += bytes;
    Ok(())
}

pub(crate) struct Encoder {
    bytes: Vec<u8>,
    count: usize,
    command_bytes: usize,
}

impl Encoder {
    pub(crate) fn new() -> Self {
        Self {
            bytes: vec![0; 4],
            count: 0,
            command_bytes: 0,
        }
    }

    // Consuming on error prevents finishing a prefix after a failed push.
    pub(crate) fn push(mut self, entry: WireEntry<'_>) -> Result<Self, StorageRpcPayloadError> {
        if self.count == MAX_ENTRIES || entry.log_index == 0 {
            return Err(invalid("complete log response count/index"));
        }
        if let WireKind::Applied { bytes, .. } = &entry.kind {
            charge(&mut self.command_bytes, bytes.len())?;
        }
        put_u64(&mut self.bytes, entry.log_index);
        put_metadata_command_log_hash(&mut self.bytes, entry.previous_log_hash);
        put_metadata_command_log_hash(&mut self.bytes, entry.log_hash);
        for digest in [entry.pre_state_digest, entry.post_state_digest] {
            match digest {
                Some(value) => {
                    put_u8(&mut self.bytes, 1);
                    put_canonical_state_digest(&mut self.bytes, value);
                }
                None => put_u8(&mut self.bytes, 0),
            }
        }
        match entry.kind {
            WireKind::Applied { checksum, bytes } => {
                put_u8(&mut self.bytes, 0);
                put_u64(&mut self.bytes, checksum);
                put_bytes(&mut self.bytes, bytes);
            }
            WireKind::Abandoned { checksum, sha256 } => {
                put_u8(&mut self.bytes, 1);
                put_u64(&mut self.bytes, checksum);
                self.bytes.extend_from_slice(&sha256);
            }
        }
        self.count += 1;
        Ok(self)
    }

    pub(crate) fn finish(mut self) -> Vec<u8> {
        self.bytes[..4].copy_from_slice(&(self.count as u32).to_le_bytes());
        self.bytes
    }
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Vec<WireEntry<'_>>, StorageRpcPayloadError> {
    if bytes.len() > MAX_RESPONSE_BYTES {
        return Err(StorageRpcPayloadError::PayloadTooLarge {
            len: bytes.len(),
            limit: MAX_RESPONSE_BYTES,
        });
    }
    let mut decoder = StorageRpcDecoder::new(bytes);
    let count = decoder.read_u32()? as usize;
    if count > MAX_ENTRIES {
        return Err(invalid("complete log response count/index"));
    }
    let mut entries = Vec::with_capacity(count);
    let mut command_bytes = 0;
    for _ in 0..count {
        let log_index = decoder.read_u64()?;
        if log_index == 0 {
            return Err(invalid("complete log response count/index"));
        }
        let previous_log_hash = read_metadata_command_log_hash(&mut decoder)?;
        let log_hash = read_metadata_command_log_hash(&mut decoder)?;
        let mut digest = || match decoder.read_u8()? {
            0 => Ok(None),
            1 => read_canonical_state_digest(&mut decoder).map(Some),
            _ => Err(invalid("complete log response state digest flag")),
        };
        let pre_state_digest = digest()?;
        let post_state_digest = digest()?;
        let kind = match decoder.read_u8()? {
            0 => {
                let checksum = decoder.read_u64()?;
                let len = decoder.read_u32()? as usize;
                charge(&mut command_bytes, len)?;
                WireKind::Applied {
                    checksum,
                    bytes: decoder.read_exact(len)?,
                }
            }
            1 => WireKind::Abandoned {
                checksum: decoder.read_u64()?,
                sha256: decoder.read_exact(32)?.try_into().expect("32 bytes"),
            },
            _ => return Err(invalid("complete log response entry kind")),
        };
        entries.push(WireEntry {
            log_index,
            previous_log_hash,
            log_hash,
            pre_state_digest,
            post_state_digest,
            kind,
        });
    }
    decoder.finish()?;
    Ok(entries)
}

#[cfg(test)]
#[path = "complete_log_range_tests.rs"]
mod tests;
