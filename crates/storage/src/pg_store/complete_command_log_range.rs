// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete-context retained-log inspection for coordinated integration.
//! This is not selected by live RPC/recovery dispatch. A returned row is an
//! observation, not recovery admission, terminal convergence or cleanup custody.

use super::*;
use crate::PgTopology;
use payload_staging::ContextualCommand;

// Position limits bound sparse scans/response cardinality, not decoding work.
// Keep this independent of the wire frame cap and of maximum command growth.
const MAX_VALIDATED_COMMAND_BYTES: usize =
    crate::storage_rpc::complete_log_range::MAX_COMMAND_BYTES;

#[path = "complete_command_log_hash_transport.rs"]
mod hash_transport;
#[path = "complete_command_log_transport.rs"]
mod transport;
// Live RPC selects this owner-scoped adapter only at coordinated activation.
pub(in crate::node_runtime) use hash_transport::{
    CompleteLogHashPage, CompleteLogHashRangeRequest,
};
pub(in crate::node_runtime) use transport::{CompleteLogPage, CompleteLogRangeRequest};

#[derive(Debug, thiserror::Error)]
pub(in crate::node_runtime) enum CompleteLogReadError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Snapshot(#[from] MetadataError),
    #[error(transparent)]
    Response(#[from] crate::storage_rpc::StorageRpcPayloadError),
    // No partial page is returned. Transport integration must split/reduce the
    // requested range, not skip next_log_index or treat the range as absent.
    #[error("complete log validation exceeds {limit_bytes} encoded bytes before index {next_log_index} (already admitted {admitted_bytes})")]
    ValidationBudgetExceeded {
        next_log_index: u64,
        admitted_bytes: usize,
        limit_bytes: usize,
    },
}

pub(in super::super) enum CompleteCommandLogEntryKind {
    Applied(Box<ContextualCommand>),
    Abandoned {
        original_command_checksum: u64,
        original_command_sha256: [u8; 32],
    },
}

/// The applied variant retains its mandatory context. There is no raw envelope
/// projection or independently supplied context tag, including for aborts.
pub(in super::super) struct CompleteCommandLogEntry {
    id: MetadataCommandId,
    previous_log_hash: MetadataCommandLogHash,
    log_hash: MetadataCommandLogHash,
    pre_state_digest: Option<CanonicalStateDigest>,
    post_state_digest: Option<CanonicalStateDigest>,
    kind: CompleteCommandLogEntryKind,
}

impl std::fmt::Debug for CompleteCommandLogEntry {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompleteCommandLogEntry")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

impl CompleteCommandLogEntry {
    pub(in super::super) fn id(&self) -> MetadataCommandId {
        self.id
    }
    pub(in super::super) fn previous_log_hash(&self) -> MetadataCommandLogHash {
        self.previous_log_hash
    }
    pub(in super::super) fn log_hash(&self) -> MetadataCommandLogHash {
        self.log_hash
    }
    pub(in super::super) fn pre_state_digest(&self) -> Option<CanonicalStateDigest> {
        self.pre_state_digest
    }
    pub(in super::super) fn post_state_digest(&self) -> Option<CanonicalStateDigest> {
        self.post_state_digest
    }
    pub(in super::super) fn into_kind(self) -> CompleteCommandLogEntryKind {
        self.kind
    }
}

struct CompleteLogRange<'a> {
    node_id: u32,
    cluster_epoch: ClusterEpoch,
    first_log_index: MetadataCommandLogIndex,
    last_log_index: MetadataCommandLogIndex,
    topology: &'a PgTopology,
}

impl PgStore {
    /// Inspect one bounded, possibly sparse range in a single read-only SQLite
    /// view. Missing/compacted rows stay missing; receipts cannot recreate their
    /// payloads. Discard the whole page on error, never return a validated prefix.
    /// This owns its snapshot and must not run inside a caller transaction.
    pub(in super::super) fn retained_complete_metadata_command_log_entries(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
        topology: &PgTopology,
    ) -> Result<Vec<CompleteCommandLogEntry>, CompleteLogReadError> {
        // RPC activation must also account for complete carrier framing; the
        // shared reader independently limits cumulative validation bytes.
        self.read_complete_log_range(
            CompleteLogRange {
                node_id,
                cluster_epoch,
                first_log_index,
                last_log_index,
                topology,
            },
            crate::storage_rpc::STORAGE_RPC_MAX_METADATA_COMMAND_LOG_ENTRY_RANGE_ENTRIES,
            std::convert::identity,
        )
    }

    /// Scalar retained-row observations, not a proof of a complete prefix or
    /// recovery admission. Validate the same complete grammar as entry reads,
    /// dropping each decoded payload before loading the next row. Receipts and
    /// checkpoint hashes never synthesize entries for compacted/missing rows.
    /// The shared byte budget bounds work independently of this scalar output.
    pub(in super::super) fn retained_complete_metadata_command_log_hashes(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
        topology: &PgTopology,
    ) -> Result<Vec<MetadataCommandLogHashRangeEntry>, CompleteLogReadError> {
        self.read_complete_log_range(
            CompleteLogRange {
                node_id,
                cluster_epoch,
                first_log_index,
                last_log_index,
                topology,
            },
            crate::storage_rpc::STORAGE_RPC_MAX_METADATA_COMMAND_LOG_HASH_RANGE_ENTRIES,
            |entry| MetadataCommandLogHashRangeEntry {
                log_index: entry.id.log_index().get(),
                previous_log_hash: entry.previous_log_hash,
                log_hash: entry.log_hash,
            },
        )
    }

    // Both projections remain inside this module. A caller cannot inject an
    // action into the snapshot or request an unbounded allocation/scan.
    fn read_complete_log_range<T>(
        &self,
        range: CompleteLogRange<'_>,
        limit: u64,
        project: impl Fn(CompleteCommandLogEntry) -> T,
    ) -> Result<Vec<T>, CompleteLogReadError> {
        let CompleteLogRange {
            node_id,
            cluster_epoch,
            first_log_index,
            last_log_index,
            topology,
        } = range;
        self.require_usable_connection()?;
        if last_log_index.get() < first_log_index.get()
            || last_log_index.get() - first_log_index.get() >= limit
            || i64::try_from(last_log_index.get()).is_err()
            || i64::try_from(cluster_epoch.get()).is_err()
            || !topology.contains_pg(PgId::new(self.pg_id))
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "read complete command log range bounds/topology",
            }
            .into());
        }
        let snapshot = self.command_build_snapshot()?;
        let mut entries = Vec::new();
        let mut admitted_bytes = 0;
        let mut previous: Option<MetadataCommandLogHashRangeEntry> = None;
        for raw_log_index in first_log_index.get()..=last_log_index.get() {
            let log_index =
                MetadataCommandLogIndex::new(raw_log_index).expect("bounded range starts nonzero");
            let id = MetadataCommandId::new(cluster_epoch, PgId::new(self.pg_id), log_index);
            let conflict = || StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                log_index: raw_log_index,
            };
            // For a BLOB, SQLite obtains length from record metadata without
            // materializing its contents. Reject other storage classes without
            // evaluating length(TEXT), which would scan its entire contents.
            // This and the payload read share the owned snapshot: another
            // connection cannot enlarge the row after its budget admission.
            let encoded_len: Option<i64> = self.query_row_cached_optional(
                "SELECT CASE WHEN typeof(command_bytes) = 'blob' THEN length(command_bytes) ELSE -1 END \
                 FROM metadata_command_log WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
                params![cluster_epoch.get() as i64, self.pg_id as i64, raw_log_index as i64],
                "admit retained complete command log bytes",
                |row| row.get(0),
            )?;
            let Some(encoded_len) = encoded_len else {
                continue;
            };
            let encoded_len = usize::try_from(encoded_len)
                .ok()
                .filter(|len| {
                    *len > 0
                        && *len
                            <= crate::metadata_command::context_carrier::MAX_COMPLETE_COMMAND_BYTES
                })
                .ok_or_else(conflict)?;
            if encoded_len > MAX_VALIDATED_COMMAND_BYTES - admitted_bytes {
                return Err(CompleteLogReadError::ValidationBudgetExceeded {
                    next_log_index: raw_log_index,
                    admitted_bytes,
                    limit_bytes: MAX_VALIDATED_COMMAND_BYTES,
                });
            }
            admitted_bytes += encoded_len;
            let entry = self
                .load_metadata_command_log_entry(
                    "load retained complete command log range",
                    cluster_epoch,
                    id.pg_id(),
                    log_index,
                )?
                .ok_or_else(conflict)?;
            if entry.command_bytes.len() != encoded_len {
                return Err(conflict().into());
            }
            let kind = if entry.abandoned {
                // Independent V2 tombstones are shared by the two explicitly
                // selected applied grammars; this is not decoder fallback.
                self.verify_metadata_command_log_entry(
                    node_id,
                    cluster_epoch,
                    id.pg_id(),
                    log_index,
                    &entry,
                )?;
                let header = decode_metadata_command_log_entry_header(&entry.command_bytes)
                    .map_err(|_| conflict())?;
                let MetadataCommandLogEntryKind::Abandoned {
                    original_command_checksum,
                    original_command_sha256,
                } = header.kind()
                else {
                    return Err(conflict().into());
                };
                CompleteCommandLogEntryKind::Abandoned {
                    original_command_checksum,
                    original_command_sha256,
                }
            } else {
                let context = TerminalLogGrammar::decode_complete_applied(
                    self, node_id, id, &entry, topology,
                )?;
                if entry.pre_state_digest.is_none() || entry.post_state_digest.is_none() {
                    return Err(conflict().into());
                }
                CompleteCommandLogEntryKind::Applied(Box::new(context))
            };
            let (Some(previous_hash), Some(hash)) = (entry.previous_log_hash, entry.log_hash)
            else {
                return Err(conflict().into());
            };
            let expected = metadata_command_log_hash(
                cluster_epoch,
                id.pg_id(),
                log_index,
                previous_hash,
                entry.command_checksum,
            );
            if expected.value() != hash || (raw_log_index == 1 && previous_hash != 0) {
                return Err(conflict().into());
            }
            if let Some(previous) = previous {
                if previous.log_index.checked_add(1) == Some(raw_log_index)
                    && previous.log_hash.value() != previous_hash
                {
                    return Err(conflict().into());
                }
            }
            let entry = CompleteCommandLogEntry {
                id,
                previous_log_hash: MetadataCommandLogHash::from_storage(
                    previous_hash,
                    super::MetadataProofStorageIssuer::new(),
                ),
                log_hash: expected,
                pre_state_digest: entry.pre_state_digest.map(|value| {
                    CanonicalStateDigest::from_storage(
                        value,
                        super::MetadataProofStorageIssuer::new(),
                    )
                }),
                post_state_digest: entry.post_state_digest.map(|value| {
                    CanonicalStateDigest::from_storage(
                        value,
                        super::MetadataProofStorageIssuer::new(),
                    )
                }),
                kind,
            };
            previous = Some(MetadataCommandLogHashRangeEntry {
                log_index: raw_log_index,
                previous_log_hash: entry.previous_log_hash,
                log_hash: entry.log_hash,
            });
            entries.push(project(entry));
        }
        snapshot.finish()?;
        Ok(entries)
    }
}
