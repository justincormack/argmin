// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Request-bound interpretation of complete retained-log responses. The wire
//! owner frames borrowed bytes; only this context owner decodes commands.
//! Observations never grant recovery admission or publication/cleanup custody.

use super::*;
use crate::metadata_command::abandoned_command_log_bytes;
use crate::storage_rpc::{complete_log_range as wire, StorageRpcPayloadError};

pub(in crate::node_runtime) struct CompleteLogRangeRequest<'a> {
    pg_id: PgId,
    epoch: ClusterEpoch,
    first: MetadataCommandLogIndex,
    last: MetadataCommandLogIndex,
    topology: &'a PgTopology,
}

/// Validated read observation. Complete contexts stay private to the durable
/// owner; this is neither recovery admission nor an installable envelope.
/// The covered interval is retained even for empty/sparse pages, so a caller
/// cannot mistake a budget-reduced read for coverage of its original request.
#[derive(Debug)]
pub(in crate::node_runtime) struct CompleteLogPage {
    pg_id: PgId,
    epoch: ClusterEpoch,
    topology: PgTopology,
    first: MetadataCommandLogIndex,
    last: MetadataCommandLogIndex,
    entries: Vec<CompleteCommandLogEntry>,
}

impl CompleteLogPage {
    pub(in crate::node_runtime) fn covered_range(
        &self,
    ) -> (MetadataCommandLogIndex, MetadataCommandLogIndex) {
        (self.first, self.last)
    }

    pub(in crate::node_runtime) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn entries(&self) -> &[CompleteCommandLogEntry] {
        &self.entries
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn into_entries(self) -> Vec<CompleteCommandLogEntry> {
        self.entries
    }
}

fn invalid(reason: &'static str) -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope(reason)
}

fn charge(total: &mut usize, size: usize) -> Result<(), StorageRpcPayloadError> {
    if size > MAX_VALIDATED_COMMAND_BYTES - *total {
        return Err(invalid("complete log response validation byte budget"));
    }
    *total += size;
    Ok(())
}

impl<'a> CompleteLogRangeRequest<'a> {
    pub(in crate::node_runtime) fn new(
        pg_id: PgId,
        epoch: ClusterEpoch,
        first: MetadataCommandLogIndex,
        last: MetadataCommandLogIndex,
        topology: &'a PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        if !topology.contains_pg(pg_id)
            || last.get() < first.get()
            || last.get() - first.get()
                >= crate::storage_rpc::STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES
            || i64::try_from(last.get()).is_err()
            || i64::try_from(epoch.get()).is_err()
        {
            return Err(invalid("complete log response request scope"));
        }
        Ok(Self {
            pg_id,
            epoch,
            first,
            last,
            topology,
        })
    }

    pub(in crate::node_runtime) fn read_response(
        &self,
        pg: &PgStore,
        node_id: u32,
    ) -> Result<Vec<u8>, CompleteLogReadError> {
        if pg.pg_id != self.pg_id.get() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "read complete log response PG",
            }
            .into());
        }
        let entries = pg.retained_complete_metadata_command_log_entries(
            node_id,
            self.epoch,
            self.first,
            self.last,
            self.topology,
        )?;
        Ok(self.encode_response(&entries)?)
    }

    pub(in crate::node_runtime) fn decode_page(
        &self,
        bytes: &[u8],
    ) -> Result<CompleteLogPage, StorageRpcPayloadError> {
        Ok(CompleteLogPage {
            pg_id: self.pg_id,
            epoch: self.epoch,
            topology: self.topology.clone(),
            first: self.first,
            last: self.last,
            entries: self.decode_response(bytes)?,
        })
    }

    fn validate(
        &self,
        entry: &CompleteCommandLogEntry,
        checksum: u64,
        previous: Option<&CompleteCommandLogEntry>,
    ) -> Result<(), StorageRpcPayloadError> {
        let id = entry.id;
        if id.pg_id() != self.pg_id
            || id.cluster_epoch() != self.epoch
            || id.log_index() < self.first
            || id.log_index() > self.last
        {
            return Err(invalid("complete log response entry subject/range"));
        }
        if let CompleteCommandLogEntryKind::Applied(context) = &entry.kind {
            if context.id() != id
                || entry.pre_state_digest.is_none()
                || entry.post_state_digest.is_none()
            {
                return Err(invalid("complete log response applied identity/digests"));
            }
        }
        if metadata_command_log_hash(
            self.epoch,
            self.pg_id,
            id.log_index(),
            entry.previous_log_hash.value(),
            checksum,
        ) != entry.log_hash
            || (id.log_index().get() == 1 && entry.previous_log_hash.value() != 0)
        {
            return Err(invalid("complete log response row hash"));
        }
        if let Some(previous) = previous {
            if previous.id.log_index() >= id.log_index()
                || (previous.id.log_index().get().checked_add(1) == Some(id.log_index().get())
                    && previous.log_hash != entry.previous_log_hash)
            {
                return Err(invalid("complete log response ordering/hash link"));
            }
        }
        Ok(())
    }

    pub(in super::super::super) fn encode_response(
        &self,
        entries: &[CompleteCommandLogEntry],
    ) -> Result<Vec<u8>, StorageRpcPayloadError> {
        if entries.len() as u64 > self.last.get() - self.first.get() + 1 {
            return Err(invalid("complete log response count exceeds request"));
        }
        let mut encoder = wire::Encoder::new();
        let mut previous = None;
        let mut total = 0;
        for entry in entries {
            let encoded;
            let kind = match &entry.kind {
                CompleteCommandLogEntryKind::Applied(context) => {
                    encoded = context
                        .encode_command()
                        .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)?;
                    if encoded.topology() != self.topology {
                        return Err(invalid("complete log response encoder topology"));
                    }
                    charge(&mut total, encoded.bytes().len())?;
                    self.validate(entry, encoded.checksum_crc64(), previous)?;
                    wire::WireKind::Applied {
                        checksum: encoded.checksum_crc64(),
                        bytes: encoded.bytes(),
                    }
                }
                CompleteCommandLogEntryKind::Abandoned {
                    original_command_checksum,
                    original_command_sha256,
                } => {
                    let bytes = abandoned_command_log_bytes(
                        entry.id,
                        *original_command_checksum,
                        *original_command_sha256,
                    );
                    charge(&mut total, bytes.len())?;
                    self.validate(entry, checksum::crc64::checksum(&bytes), previous)?;
                    wire::WireKind::Abandoned {
                        checksum: *original_command_checksum,
                        sha256: *original_command_sha256,
                    }
                }
            };
            encoder = encoder.push(wire::WireEntry {
                log_index: entry.id.log_index().get(),
                previous_log_hash: entry.previous_log_hash,
                log_hash: entry.log_hash,
                pre_state_digest: entry.pre_state_digest,
                post_state_digest: entry.post_state_digest,
                kind,
            })?;
            previous = Some(entry);
        }
        Ok(encoder.finish())
    }

    pub(in super::super::super) fn decode_response(
        &self,
        bytes: &[u8],
    ) -> Result<Vec<CompleteCommandLogEntry>, StorageRpcPayloadError> {
        let rows = wire::decode(bytes)?;
        if rows.len() as u64 > self.last.get() - self.first.get() + 1 {
            return Err(invalid("complete log response count exceeds request"));
        }
        let mut entries = Vec::with_capacity(rows.len());
        let mut total = 0;
        for row in rows {
            let id = MetadataCommandId::new(
                self.epoch,
                self.pg_id,
                MetadataCommandLogIndex::new(row.log_index).expect("wire rejects zero"),
            );
            if id.log_index() < self.first || id.log_index() > self.last {
                return Err(invalid("complete log response entry subject/range"));
            }
            let (kind, checksum) = match row.kind {
                wire::WireKind::Applied { checksum, bytes } => {
                    charge(&mut total, bytes.len())?;
                    let context = ContextualCommand::decode_command(bytes, checksum, self.topology)
                        .map_err(|_| StorageRpcPayloadError::InvalidMetadataCommandEnvelope)?;
                    (
                        CompleteCommandLogEntryKind::Applied(Box::new(context)),
                        checksum,
                    )
                }
                wire::WireKind::Abandoned { checksum, sha256 } => {
                    let bytes = abandoned_command_log_bytes(id, checksum, sha256);
                    charge(&mut total, bytes.len())?;
                    (
                        CompleteCommandLogEntryKind::Abandoned {
                            original_command_checksum: checksum,
                            original_command_sha256: sha256,
                        },
                        checksum::crc64::checksum(&bytes),
                    )
                }
            };
            let entry = CompleteCommandLogEntry {
                id,
                previous_log_hash: row.previous_log_hash,
                log_hash: row.log_hash,
                pre_state_digest: row.pre_state_digest,
                post_state_digest: row.post_state_digest,
                kind,
            };
            self.validate(&entry, checksum, entries.last())?;
            entries.push(entry);
        }
        Ok(entries)
    }
}
