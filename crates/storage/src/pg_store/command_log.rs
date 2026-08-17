// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::control_plane::{
    CanonicalStateDigest, MetadataCommandLogHash, METADATA_CANONICAL_STATE_ENCODING_VERSION,
};
use crate::metadata_command::MetadataTransferCommand;
use crate::storage_rpc::{
    decode_metadata_command_checkpoint_payload, encode_metadata_command_checkpoint_payload,
};

const METADATA_CANONICAL_PG_STATE_DOMAIN: &[u8] = b"argmin.metadata.pg-state";
const METADATA_COMMAND_CHECKPOINT_DOMAIN: &[u8] = b"argmin.metadata.command-checkpoint";
const METADATA_COMMAND_CHECKPOINT_RETAIN_PER_EPOCH: usize = 8;
const METADATA_COMMAND_CHECKPOINT_CAPTURE_LIMIT: usize =
    METADATA_COMMAND_CHECKPOINT_RETAIN_PER_EPOCH + 1;
const METADATA_COMMAND_CHECKPOINT_RETAIN_EPOCHS: usize = 4;
const METADATA_COMMAND_TERMINAL_RECEIPT_RETAIN_PER_EPOCH: u64 = 4096;

#[derive(Debug, Clone)]
pub(crate) struct MetadataCommandCheckpointCandidateRow {
    applied_log_index: i64,
    applied_log_hash: i64,
    state_digest: i64,
    checkpoint_crc64: i64,
    checkpoint_bytes: Vec<u8>,
}

pub(crate) fn decode_metadata_command_checkpoint_candidate_rows(
    rows: Vec<MetadataCommandCheckpointCandidateRow>,
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    limit: usize,
) -> Vec<MetadataCommandCheckpoint> {
    let mut checkpoints = Vec::new();
    for row in rows {
        let Ok(applied_log_index) = decode_nonnegative_u64(
            "decode metadata command checkpoint candidate log index",
            row.applied_log_index,
        ) else {
            continue;
        };
        let checkpoint = match decode_metadata_command_checkpoint_payload(&row.checkpoint_bytes) {
            Ok(checkpoint) => checkpoint,
            Err(_) => continue,
        };
        if checkpoint.cluster_epoch != cluster_epoch
            || checkpoint.pg_id != pg_id
            || checkpoint.applied_log_index != applied_log_index
            || checkpoint.applied_log_hash.value() != row.applied_log_hash as u64
            || checkpoint.state_digest.value() != row.state_digest as u64
            || checkpoint.checkpoint_crc64 != row.checkpoint_crc64 as u64
            || checkpoint.verify().is_err()
        {
            continue;
        }
        checkpoints.push(checkpoint);
        if checkpoints.len() == limit {
            break;
        }
    }
    checkpoints
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MetadataDigestFilter {
    AllRows,
}

/// Command-log retention summary for one PG replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataCommandLogStats {
    /// Cluster epoch whose command-log rows were counted.
    pub cluster_epoch: ClusterEpoch,
    /// Placement group stored by this PgStore.
    pub pg_id: PgId,
    /// Lowest retained log index for this epoch, if any rows are retained.
    pub min_log_index: Option<u64>,
    /// Highest retained log index for this epoch, if any rows are retained.
    pub max_log_index: Option<u64>,
    /// Highest contiguous log index accepted by this replica.
    pub applied_log_index: u64,
    /// Number of retained command-log rows for this epoch.
    pub retained_entries: u64,
    /// Number of retained abandoned/tombstone command-log rows.
    pub abandoned_entries: u64,
    /// Retained rows beyond the current applied prefix.
    pub pending_tail_entries: u64,
    /// Missing rows inside the current applied prefix.
    pub missing_applied_prefix_entries: u64,
    /// Exclusive log-index bound for rows covered by a verified checkpoint.
    pub compactable_before: Option<u64>,
}

/// Result of attempting metadata command-log compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataCommandLogCompactionStatus {
    /// No verified checkpoint currently covers this command-log prefix.
    NoCheckpoint { retained_entries: u64 },
    /// A pending or tail command makes this replica unsafe to compact now.
    PendingCommand { retained_entries: u64 },
    /// Rows below `compacted_before` were removed.
    Compacted {
        deleted_entries: u64,
        compacted_before: u64,
    },
}

#[derive(Debug, Clone, Copy)]
struct MetadataCommandLogValidationBase {
    applied_log_index: u64,
    applied_log_hash: u64,
    compactable_before: Option<u64>,
}

/// Per-table materialized-state digest covered by a metadata checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCheckpointTableDigest {
    pub table_name: String,
    pub row_count: u64,
    pub row_hash_xor: u64,
    pub row_hash_sum: u64,
    pub table_digest: u64,
}

/// Owned SQLite value in the canonical metadata checkpoint encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataCheckpointValue {
    Null,
    Integer(i64),
    RealBits(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

/// One ordered materialized metadata row in a checkpoint table block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCheckpointRow {
    pub values: Vec<MetadataCheckpointValue>,
    pub row_digest: u64,
}

/// Materialized rows for one metadata table, in canonical checkpoint order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCheckpointTableBlock {
    pub table_name: String,
    pub columns: Vec<String>,
    pub order_columns: Vec<String>,
    pub filter: String,
    pub rows: Vec<MetadataCheckpointRow>,
    pub row_count: u64,
    pub row_hash_xor: u64,
    pub row_hash_sum: u64,
    pub table_digest: u64,
}

/// Checked summary of a PG metadata replica state.
///
/// This is not yet an installable checkpoint. It carries the materialized row
/// payload and proof envelope, but import still needs storage-node install
/// validation before checkpoint-base metadata transfer can trust it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataCommandCheckpoint {
    pub cluster_epoch: ClusterEpoch,
    pub pg_id: PgId,
    pub applied_log_index: u64,
    pub(crate) applied_log_hash: MetadataCommandLogHash,
    pub(crate) state_digest: CanonicalStateDigest,
    pub table_digests: Vec<MetadataCheckpointTableDigest>,
    pub table_blocks: Vec<MetadataCheckpointTableBlock>,
    pub checkpoint_crc64: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataCommandCheckpointValidationError {
    UnsupportedCheckpointEncoding {
        actual: u8,
    },
    UnsupportedStateEncoding {
        actual: u8,
    },
    TableSummaryCountMismatch {
        expected: usize,
        actual: usize,
    },
    TableBlockCountMismatch {
        expected: usize,
        actual: usize,
    },
    TableNameMismatch {
        expected: String,
        actual: String,
    },
    TableColumnsMismatch {
        table_name: String,
    },
    TableOrderColumnsMismatch {
        table_name: String,
    },
    TableFilterMismatch {
        table_name: String,
    },
    RowDigestMismatch {
        table_name: String,
        row_index: usize,
        expected_digest: u64,
        actual_digest: u64,
    },
    InvalidBucketTagRow {
        row_index: usize,
    },
    InvalidAclGrantsRow {
        table_name: String,
        row_index: usize,
    },
    TableDigestMismatch {
        table_name: String,
        expected_digest: u64,
        actual_digest: u64,
    },
    StateDigestMismatch {
        expected_digest: u64,
        actual_digest: u64,
    },
    CheckpointCrcMismatch {
        expected_crc64: u64,
        actual_crc64: u64,
    },
}

impl MetadataCommandCheckpoint {
    pub fn verify(&self) -> Result<(), MetadataCommandCheckpointValidationError> {
        if self.table_digests.len() != METADATA_DIGEST_TABLES.len() {
            return Err(
                MetadataCommandCheckpointValidationError::TableSummaryCountMismatch {
                    expected: METADATA_DIGEST_TABLES.len(),
                    actual: self.table_digests.len(),
                },
            );
        }
        if self.table_blocks.len() != METADATA_DIGEST_TABLES.len() {
            return Err(
                MetadataCommandCheckpointValidationError::TableBlockCountMismatch {
                    expected: METADATA_DIGEST_TABLES.len(),
                    actual: self.table_blocks.len(),
                },
            );
        }

        let mut state_hasher = checksum::crc64::Hasher::new();
        PgStore::digest_canonical_pg_state_header(&mut state_hasher);
        for ((table, summary), block) in METADATA_DIGEST_TABLES
            .iter()
            .zip(&self.table_digests)
            .zip(&self.table_blocks)
        {
            Self::verify_table_identity(table, &summary.table_name)?;
            Self::verify_table_identity(table, &block.table_name)?;
            if block
                .columns
                .iter()
                .map(String::as_str)
                .ne(table.columns.iter().copied())
            {
                return Err(
                    MetadataCommandCheckpointValidationError::TableColumnsMismatch {
                        table_name: table.name.to_owned(),
                    },
                );
            }
            if block
                .order_columns
                .iter()
                .map(String::as_str)
                .ne(table.order_columns.iter().copied())
            {
                return Err(
                    MetadataCommandCheckpointValidationError::TableOrderColumnsMismatch {
                        table_name: table.name.to_owned(),
                    },
                );
            }
            if block.filter != table.filter.canonical_name() {
                return Err(
                    MetadataCommandCheckpointValidationError::TableFilterMismatch {
                        table_name: table.name.to_owned(),
                    },
                );
            }

            let mut stats = MetadataTableDigestStats {
                row_count: 0,
                row_hash_xor: 0,
                row_hash_sum: 0,
            };
            for (row_index, row) in block.rows.iter().enumerate() {
                let row_digest = PgStore::metadata_checkpoint_row_digest(table, &row.values);
                if row_digest != row.row_digest {
                    return Err(
                        MetadataCommandCheckpointValidationError::RowDigestMismatch {
                            table_name: table.name.to_owned(),
                            row_index,
                            expected_digest: row.row_digest,
                            actual_digest: row_digest,
                        },
                    );
                }
                Self::verify_row_semantics(table, row_index, &row.values)?;
                stats.row_count += 1;
                stats.row_hash_xor ^= row_digest;
                stats.row_hash_sum = stats.row_hash_sum.wrapping_add(row_digest);
            }
            let table_digest = metadata_table_digest_from_stats(table, stats);
            if table_digest != summary.table_digest
                || stats.row_count != summary.row_count
                || stats.row_hash_xor != summary.row_hash_xor
                || stats.row_hash_sum != summary.row_hash_sum
                || table_digest != block.table_digest
                || stats.row_count != block.row_count
                || stats.row_hash_xor != block.row_hash_xor
                || stats.row_hash_sum != block.row_hash_sum
            {
                return Err(
                    MetadataCommandCheckpointValidationError::TableDigestMismatch {
                        table_name: table.name.to_owned(),
                        expected_digest: summary.table_digest,
                        actual_digest: table_digest,
                    },
                );
            }
            PgStore::digest_metadata_table_digest_entry(&mut state_hasher, table, table_digest);
        }

        let state_digest = state_hasher.finalize();
        if state_digest != self.state_digest.value() {
            return Err(
                MetadataCommandCheckpointValidationError::StateDigestMismatch {
                    expected_digest: self.state_digest.value(),
                    actual_digest: state_digest,
                },
            );
        }

        let expected_crc64 = PgStore::metadata_command_checkpoint_crc64(self);
        if expected_crc64 != self.checkpoint_crc64 {
            return Err(
                MetadataCommandCheckpointValidationError::CheckpointCrcMismatch {
                    expected_crc64,
                    actual_crc64: self.checkpoint_crc64,
                },
            );
        }
        Ok(())
    }

    fn verify_table_identity(
        table: &MetadataDigestTable,
        actual: &str,
    ) -> Result<(), MetadataCommandCheckpointValidationError> {
        if actual == table.name {
            Ok(())
        } else {
            Err(
                MetadataCommandCheckpointValidationError::TableNameMismatch {
                    expected: table.name.to_owned(),
                    actual: actual.to_owned(),
                },
            )
        }
    }

    fn verify_row_semantics(
        table: &MetadataDigestTable,
        row_index: usize,
        values: &[MetadataCheckpointValue],
    ) -> Result<(), MetadataCommandCheckpointValidationError> {
        Self::verify_bucket_tag_row(table, row_index, values)?;
        Self::verify_acl_grants_row(table, row_index, values)
    }

    fn verify_bucket_tag_row(
        table: &MetadataDigestTable,
        row_index: usize,
        values: &[MetadataCheckpointValue],
    ) -> Result<(), MetadataCommandCheckpointValidationError> {
        if table.name != "bucket_subresources"
            || !matches!(
                values.get(1),
                Some(MetadataCheckpointValue::Integer(kind))
                    if *kind == BucketSubresourceKind::Tagging as u8 as i64
            )
        {
            return Ok(());
        }

        let body = match values.get(2) {
            Some(MetadataCheckpointValue::Null) => None,
            Some(MetadataCheckpointValue::Text(xml)) => Some(
                String::from_utf8(xml.clone())
                    .map_err(|_| Self::invalid_bucket_tag_row(row_index))?,
            ),
            _ => return Err(Self::invalid_bucket_tag_row(row_index)),
        };
        let aux_int_1 = match values.get(4) {
            Some(MetadataCheckpointValue::Null) => None,
            Some(MetadataCheckpointValue::Integer(value)) => Some(*value),
            _ => return Err(Self::invalid_bucket_tag_row(row_index)),
        };
        let valid = PgStore::decode_bucket_tag_subresource_row(body, aux_int_1).is_ok();
        if valid {
            Ok(())
        } else {
            Err(Self::invalid_bucket_tag_row(row_index))
        }
    }

    fn verify_acl_grants_row(
        table: &MetadataDigestTable,
        row_index: usize,
        values: &[MetadataCheckpointValue],
    ) -> Result<(), MetadataCommandCheckpointValidationError> {
        let Some(acl_column) = table
            .columns
            .iter()
            .position(|column| *column == "acl_grants")
        else {
            return Ok(());
        };
        let valid = matches!(
            values.get(acl_column),
            Some(MetadataCheckpointValue::Text(raw))
                if std::str::from_utf8(raw)
                    .ok()
                    .and_then(|raw| {
                        s3_types::StoredAclGrants::parse_current(raw.to_owned()).ok()
                    })
                    .is_some()
        );
        if valid {
            Ok(())
        } else {
            Err(
                MetadataCommandCheckpointValidationError::InvalidAclGrantsRow {
                    table_name: table.name.to_owned(),
                    row_index,
                },
            )
        }
    }

    fn invalid_bucket_tag_row(row_index: usize) -> MetadataCommandCheckpointValidationError {
        MetadataCommandCheckpointValidationError::InvalidBucketTagRow { row_index }
    }
}

fn decode_nonnegative_u64(context: &'static str, raw: i64) -> Result<u64, StoreError> {
    raw.try_into().map_err(|_| StoreError::Db {
        context,
        source: crate::error::DatabaseError::from_sql_conversion_failure(
            0,
            rusqlite::types::Type::Integer,
            Box::from("negative integer where non-negative value was expected"),
        ),
    })
}

fn decode_metadata_command_log_hash(
    pg_id: u32,
    raw_version: i64,
    raw_value: i64,
) -> Result<MetadataCommandLogHash, StoreError> {
    let version = decode_nonnegative_u64("decode metadata-command log-hash version", raw_version)?;
    let Ok(version_u8) = u8::try_from(version) else {
        return Err(StoreError::MetadataCommandReplicaStateEncodingVersion {
            pg_id,
            carrier: "log-hash",
            actual: version,
        });
    };
    MetadataCommandLogHash::from_encoded_parts(version_u8, raw_value as u64).map_err(|_| {
        StoreError::MetadataCommandReplicaStateEncodingVersion {
            pg_id,
            carrier: "log-hash",
            actual: version,
        }
    })
}

fn decode_canonical_state_digest(
    pg_id: u32,
    raw_version: i64,
    raw_value: i64,
) -> Result<CanonicalStateDigest, StoreError> {
    let version = decode_nonnegative_u64("decode canonical-state digest version", raw_version)?;
    let Ok(version_u8) = u8::try_from(version) else {
        return Err(StoreError::MetadataCommandReplicaStateEncodingVersion {
            pg_id,
            carrier: "state-digest",
            actual: version,
        });
    };
    CanonicalStateDigest::from_encoded_parts(version_u8, raw_value as u64).map_err(|_| {
        StoreError::MetadataCommandReplicaStateEncodingVersion {
            pg_id,
            carrier: "state-digest",
            actual: version,
        }
    })
}

#[derive(Debug, Clone, Copy)]
pub(super) struct MetadataDigestTable {
    pub(super) name: &'static str,
    pub(super) columns: &'static [&'static str],
    pub(super) order_columns: &'static [&'static str],
    pub(super) filter: MetadataDigestFilter,
}

impl MetadataDigestFilter {
    fn canonical_name(self) -> &'static str {
        match self {
            MetadataDigestFilter::AllRows => "all-rows",
        }
    }
}

pub(super) const METADATA_DIGEST_TABLES: &[MetadataDigestTable] = &[
    MetadataDigestTable {
        name: "bucket_subresources",
        columns: &["bucket_name", "kind", "body", "generation", "aux_int_1"],
        order_columns: &["bucket_name", "kind"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "buckets",
        columns: &[
            "name",
            "owner_principal",
            "owner_canonical_id",
            "created_at",
            "region",
            "state",
            "versioning",
            "acl_grants",
            "public_read",
            "public_write",
            "public_access_block_present",
            "public_access_block_block_public_acls",
            "public_access_block_ignore_public_acls",
            "public_access_block_block_public_policy",
            "public_access_block_restrict_public_buckets",
            "ownership_controls_mode",
            "bucket_policy_public",
            "bucket_policy_generation",
            "bucket_lifecycle_generation",
            "bucket_execution_generation",
            "bucket_incarnation_generation",
            "multipart_upload_id_key",
            "multipart_completion_barrier_sequence",
            "bucket_abac_enabled",
            "default_encryption_type",
            "sse_c_blocked",
            "object_lock_enabled",
            "object_lock_default_mode",
            "object_lock_default_days",
            "object_lock_default_years",
        ],
        order_columns: &["name"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "multipart_part_segments",
        columns: &[
            "bucket",
            "key",
            "upload_id",
            "version_id",
            "part_number",
            "segment_index",
            "size",
            "segment_crc64",
            "segment_okh",
            "segment_vid",
            "data_pg_id",
            "placement_cluster_epoch",
            "ec_k",
            "ec_m",
        ],
        order_columns: &["bucket", "key", "upload_id", "part_number", "segment_index"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "multipart_parts",
        columns: &[
            "upload_id",
            "part_number",
            "generation",
            "size",
            "payload_crc64",
            "etag",
            "etag_kind",
            "part_vid",
            "placement_cluster_epoch",
            "ec_k",
            "ec_m",
            "last_modified",
            "checksum",
        ],
        order_columns: &["upload_id", "part_number"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "multipart_reclaim_part_segments",
        columns: &[
            "bucket",
            "key",
            "generation_id",
            "part_number",
            "segment_index",
            "segment_okh",
            "segment_vid",
            "data_pg_id",
            "ec_k",
            "ec_m",
        ],
        order_columns: &[
            "bucket",
            "key",
            "generation_id",
            "part_number",
            "segment_index",
        ],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "multipart_reclaim_parts",
        columns: &["bucket", "key", "generation_id", "part_number"],
        order_columns: &["bucket", "key", "generation_id", "part_number"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "multipart_reclaims",
        columns: &["bucket", "key", "generation_id", "created_at"],
        order_columns: &["bucket", "key", "generation_id"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "multipart_uploads",
        columns: &[
            "upload_id",
            "bucket",
            "key",
            "initiated_at",
            "state",
            "tags",
            "metadata_blob",
            "system_metadata_blob",
            "owner_principal",
            "owner_canonical_id",
            "initiator_principal",
            "initiator_canonical_id",
            "checksum_algorithm",
            "checksum_type",
            "encryption_type",
            "encryption_state",
            "acl_grants",
            "public_read",
            "object_generation_id",
            "initiated_object_kind",
            "initiated_object_version_id",
            "initiated_object_generation_or_write_sequence",
            "object_lock_retention_mode",
            "object_lock_retain_until",
            "object_lock_legal_hold",
        ],
        order_columns: &["upload_id"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "object_generation_reservations",
        columns: &[
            "reservation_id",
            "bucket",
            "key",
            "generation_id",
            "created_at",
        ],
        order_columns: &["reservation_id"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "object_version_counters",
        columns: &["bucket", "key", "next_version_id"],
        order_columns: &["bucket", "key"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "object_write_counters",
        columns: &[
            "bucket",
            "key",
            "next_write_sequence",
            "max_committed_generation",
        ],
        order_columns: &["bucket", "key"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "object_parts",
        columns: &[
            "bucket",
            "key",
            "version_id",
            "part_number",
            "object_offset_start",
            "size",
            "payload_crc64",
            "etag",
            "etag_kind",
            "part_vid",
            "placement_cluster_epoch",
            "ec_k",
            "ec_m",
            "data_pg_id",
            "checksum",
        ],
        order_columns: &["bucket", "key", "version_id", "part_number"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "object_segment_reclaim_segments",
        columns: &[
            "bucket",
            "key",
            "generation_id",
            "segment_index",
            "segment_okh",
            "segment_vid",
            "data_pg_id",
            "ec_k",
            "ec_m",
        ],
        order_columns: &["bucket", "key", "generation_id", "segment_index"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "object_segments",
        columns: &[
            "bucket",
            "key",
            "version_id",
            "segment_index",
            "size",
            "segment_crc64",
            "segment_okh",
            "segment_vid",
            "data_pg_id",
            "placement_cluster_epoch",
            "ec_k",
            "ec_m",
        ],
        order_columns: &["bucket", "key", "version_id", "segment_index"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "object_segments_reclaims",
        columns: &["bucket", "key", "generation_id", "created_at"],
        order_columns: &["bucket", "key", "generation_id"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "objects",
        columns: &[
            "bucket",
            "key",
            "version_id",
            "write_sequence",
            "generation_id",
            "size",
            "etag",
            "etag_kind",
            "last_modified",
            "storage_class",
            "ec_k",
            "ec_m",
            "status",
            "tags",
            "data_layout",
            "parts_count",
            "metadata_blob",
            "system_metadata_blob",
            "encryption_type",
            "encryption_state",
            "owner_principal",
            "owner_canonical_id",
            "acl_grants",
            "public_read",
            "object_lock_retention_mode",
            "object_lock_retain_until",
            "object_lock_legal_hold",
            "multipart_completion_upload_id",
            "multipart_completion_fingerprint",
            "became_noncurrent_at",
        ],
        order_columns: &["bucket", "key", "version_id"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "pg_counters",
        columns: &["singleton", "next_bucket_execution_generation"],
        order_columns: &["singleton"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "stream_upload_segments",
        columns: &[
            "session_id",
            "segment_index",
            "size",
            "segment_crc64",
            "payload_crc64",
            "segment_okh",
            "segment_vid",
            "data_pg_id",
            "placement_cluster_epoch",
            "ec_k",
            "ec_m",
        ],
        order_columns: &["session_id", "segment_index"],
        filter: MetadataDigestFilter::AllRows,
    },
    MetadataDigestTable {
        name: "stream_uploads",
        columns: &[
            "session_id",
            "bucket",
            "key",
            "op_kind",
            "upload_id",
            "part_number",
            "state",
            "created_at",
            "encryption_type",
            "encryption_state",
        ],
        order_columns: &["session_id"],
        filter: MetadataDigestFilter::AllRows,
    },
];

/// Feed a variable-length byte field into a canonical digest.
///
/// The length prefix is part of the canonical state encoding. Without it,
/// adjacent text/blob fields could be split differently while producing the
/// same byte stream.
pub(super) fn digest_len_prefixed_bytes(hasher: &mut checksum::crc64::Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn digest_len_prefixed_bytes_crc64(hasher: &mut checksum::crc64::Hasher, bytes: &[u8]) {
    let mut value_hasher = checksum::crc64::Hasher::new();
    value_hasher.update(bytes);
    digest_u64(hasher, bytes.len() as u64);
    digest_u64(hasher, value_hasher.finalize());
}

fn digest_u8(hasher: &mut checksum::crc64::Hasher, value: u8) {
    hasher.update(&[value]);
}

fn digest_i64(hasher: &mut checksum::crc64::Hasher, value: i64) {
    hasher.update(&value.to_be_bytes());
}

fn digest_u64(hasher: &mut checksum::crc64::Hasher, value: u64) {
    hasher.update(&value.to_be_bytes());
}

fn quote_sql_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sqlite_user_function_error(message: impl Into<String>) -> SqlError {
    SqlError::UserFunctionError(Box::new(std::io::Error::other(message.into())))
}

pub(super) fn register_metadata_digest_sql_functions(conn: &Connection) -> rusqlite::Result<()> {
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    conn.create_scalar_function("argmin_metadata_row_digest", -1, flags, |ctx| {
        metadata_row_digest_sql(ctx).map(|digest| digest as i64)
    })?;
    conn.create_scalar_function("argmin_metadata_table_digest", 4, flags, |ctx| {
        let table_name: String = ctx.get(0)?;
        let row_count = ctx.get::<i64>(1)? as u64;
        let row_hash_xor = ctx.get::<i64>(2)? as u64;
        let row_hash_sum = ctx.get::<i64>(3)? as u64;
        let table = METADATA_DIGEST_TABLES
            .iter()
            .find(|table| table.name == table_name)
            .ok_or_else(|| {
                sqlite_user_function_error(format!("unknown digest table {table_name}"))
            })?;
        Ok(metadata_table_digest_from_stats(
            table,
            MetadataTableDigestStats {
                row_count,
                row_hash_xor,
                row_hash_sum,
            },
        ) as i64)
    })?;
    conn.create_scalar_function("argmin_crc64_xor", 2, flags, |ctx| {
        let left = ctx.get::<i64>(0)? as u64;
        let right = ctx.get::<i64>(1)? as u64;
        Ok((left ^ right) as i64)
    })?;
    conn.create_scalar_function("argmin_crc64_add", 2, flags, |ctx| {
        let left = ctx.get::<i64>(0)? as u64;
        let right = ctx.get::<i64>(1)? as u64;
        Ok(left.wrapping_add(right) as i64)
    })?;
    conn.create_scalar_function("argmin_crc64_sub", 2, flags, |ctx| {
        let left = ctx.get::<i64>(0)? as u64;
        let right = ctx.get::<i64>(1)? as u64;
        Ok(left.wrapping_sub(right) as i64)
    })?;
    Ok(())
}

fn metadata_row_digest_sql(ctx: &Context<'_>) -> rusqlite::Result<u64> {
    if ctx.is_empty() {
        return Err(sqlite_user_function_error(
            "metadata row digest requires table name",
        ));
    }
    let table_name: String = ctx.get(0)?;
    let mut hasher = checksum::crc64::Hasher::new();
    digest_u8(&mut hasher, 0x20);
    digest_len_prefixed_bytes(&mut hasher, table_name.as_bytes());
    digest_u64(&mut hasher, (ctx.len() - 1) as u64);
    for index in 1..ctx.len() {
        PgStore::digest_canonical_sql_value(&mut hasher, ctx.get_raw(index));
    }
    Ok(hasher.finalize())
}

fn metadata_table_digest_from_stats(
    table: &MetadataDigestTable,
    stats: MetadataTableDigestStats,
) -> u64 {
    let mut hasher = checksum::crc64::Hasher::new();
    digest_u8(&mut hasher, 0x10);
    digest_len_prefixed_bytes(&mut hasher, table.name.as_bytes());
    digest_len_prefixed_bytes(&mut hasher, table.filter.canonical_name().as_bytes());
    digest_u64(&mut hasher, table.columns.len() as u64);
    for column in table.columns {
        digest_len_prefixed_bytes(&mut hasher, column.as_bytes());
    }
    digest_u64(&mut hasher, table.order_columns.len() as u64);
    for column in table.order_columns {
        digest_len_prefixed_bytes(&mut hasher, column.as_bytes());
    }
    digest_u64(&mut hasher, stats.row_count);
    digest_u64(&mut hasher, stats.row_hash_xor);
    digest_u64(&mut hasher, stats.row_hash_sum);
    digest_u8(&mut hasher, 0x11);
    hasher.finalize()
}

#[derive(Debug)]
struct MetadataCommandLogEntry {
    command_checksum: u64,
    command_bytes: Vec<u8>,
    abandoned: bool,
    previous_log_hash: Option<u64>,
    log_hash: Option<u64>,
    pre_state_digest: Option<u64>,
    post_state_digest: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MetadataCommandTerminalReceipt {
    command_checksum: u64,
    command_sha256: [u8; 32],
    abandoned: bool,
    previous_log_hash: u64,
    log_hash: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingMetadataCommandSlot {
    pub(crate) id: MetadataCommandId,
    pub(crate) command_checksum: u64,
    pub(crate) command_bytes: Vec<u8>,
    pub(crate) publication_started: bool,
    pub(crate) scope_bucket: Option<BucketName>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingMetadataCommandSlotAction {
    Unresolved,
    CleanTerminal,
    AdvanceAbandonedThenClean,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingMetadataCommandSlotCleanup {
    CleanTerminal,
    PreserveTerminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ObjectGenerationReservationConstraint {
    ReservationId,
    Generation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ObjectGenerationReservationIdentity {
    pub(super) bucket: BucketName,
    pub(super) key: ObjectKey,
    pub(super) generation_id: GenerationId,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct MetadataCommandRecordResult {
    pub(super) state: MetadataCommandReplicaState,
    pub(super) digest_revision: u64,
}

#[derive(Debug, Clone, Copy)]
struct MetadataTableDigestStats {
    row_count: u64,
    row_hash_xor: u64,
    row_hash_sum: u64,
}

impl PgStore {
    fn with_metadata_command_checkpoint_transaction<T>(
        &self,
        operation: impl FnOnce() -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        if !self.conn.is_autocommit() {
            return operation();
        }
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| StoreError::Db {
                context: "begin metadata command checkpoint transaction",
                source: source.into(),
            })?;
        match operation() {
            Ok(value) => self
                .conn
                .execute_batch("COMMIT")
                .map(|()| value)
                .map_err(|source| {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    StoreError::Db {
                        context: "commit metadata command checkpoint transaction",
                        source: source.into(),
                    }
                }),
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    fn with_pending_slot_transaction<T>(
        &self,
        operation: impl FnOnce() -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        if !self.conn.is_autocommit() {
            return operation();
        }
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| StoreError::Db {
                context: "begin pending metadata command slot transaction",
                source: source.into(),
            })?;
        match operation() {
            Ok(value) => self
                .conn
                .execute_batch("COMMIT")
                .map(|()| value)
                .map_err(|source| {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    StoreError::Db {
                        context: "commit pending metadata command slot transaction",
                        source: source.into(),
                    }
                }),
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    fn with_pending_slot_insert_transaction<T>(
        &self,
        operation: impl FnOnce() -> Result<T, StoreError>,
    ) -> Result<T, PendingMetadataCommandSlotInsertError> {
        if !self.conn.is_autocommit() {
            return operation().map_err(PendingMetadataCommandSlotInsertError::definitive);
        }
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| StoreError::Db {
                context: "begin pending metadata command slot transaction",
                source: source.into(),
            })
            .map_err(PendingMetadataCommandSlotInsertError::definitive)?;
        match operation() {
            Ok(value) => self
                .conn
                .execute_batch("COMMIT")
                .map(|()| value)
                .map_err(|source| {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    PendingMetadataCommandSlotInsertError::may_have_applied(StoreError::Db {
                        context: "commit pending metadata command slot transaction",
                        source: source.into(),
                    })
                }),
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(PendingMetadataCommandSlotInsertError::definitive(error))
            }
        }
    }

    fn replace_pending_placed_reference_pages(
        &self,
        reference_count: u32,
        pages: &[Vec<u8>],
    ) -> Result<(), StoreError> {
        self.execute_cached(
            "DELETE FROM metadata_command_pending_placed_reference_pages WHERE singleton = 0",
            [],
            "clear pending command placed reference pages",
        )?;
        let references_per_page = usize::from(PLACED_SEGMENT_BACKFILL_REFERENCE_PAGE_LIMIT);
        let reference_count = reference_count as usize;
        for (page_index, encoded) in pages.iter().enumerate() {
            let page_start = page_index * references_per_page;
            let page_reference_count = (reference_count - page_start).min(references_per_page);
            self.execute_cached(
                "INSERT INTO metadata_command_pending_placed_reference_pages \
                 (singleton, page_index, reference_count, encoded_references) \
                 VALUES (0, ?1, ?2, ?3)",
                params![page_index as i64, page_reference_count as i64, encoded,],
                "insert pending command placed reference page",
            )?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn test_require_one_changed(changed: usize) -> Result<(), StoreError> {
        if changed == 1 {
            Ok(())
        } else {
            Err(StoreError::IntegrityError {
                expected: 1,
                actual: changed as u64,
            })
        }
    }

    #[cfg(test)]
    pub(crate) fn test_delete_metadata_command_log_entry(
        &self,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "DELETE FROM metadata_command_log WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            params![cluster_epoch.get() as i64, self.pg_id as i64, log_index as i64],
            "delete metadata command log entry for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_set_metadata_command_log_checksum(
        &self,
        log_index: u64,
        checksum: u64,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_command_log SET command_checksum = ?1 WHERE log_index = ?2",
            params![checksum as i64, log_index as i64],
            "replace metadata command checksum for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_set_metadata_command_log_bytes(
        &self,
        log_index: u64,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_command_log SET command_bytes = ?1 WHERE log_index = ?2",
            params![bytes, log_index as i64],
            "replace metadata command bytes for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_replace_metadata_command_log_command(
        &self,
        log_index: u64,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_command_log SET command_checksum = ?1, command_bytes = ?2 WHERE log_index = ?3",
            params![
                command.checksum_crc64() as i64,
                command.command_bytes(),
                log_index as i64
            ],
            "replace metadata command for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_replace_metadata_command_log_command_and_hashes(
        &self,
        log_index: u64,
        command: &MetadataCommandEnvelope,
        previous_log_hash: u64,
        log_hash: u64,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_command_log SET command_checksum = ?1, command_bytes = ?2, previous_log_hash = ?3, log_hash = ?4 WHERE log_index = ?5",
            params![
                command.checksum_crc64() as i64,
                command.command_bytes(),
                previous_log_hash as i64,
                log_hash as i64,
                log_index as i64
            ],
            "replace metadata command and hashes for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_set_metadata_command_log_previous_hash(
        &self,
        log_index: u64,
        previous_log_hash: u64,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_command_log SET previous_log_hash = ?1 WHERE log_index = ?2",
            params![previous_log_hash as i64, log_index as i64],
            "replace metadata command previous hash for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_increment_metadata_command_log_pre_state_digest(
        &self,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_command_log SET pre_state_digest = pre_state_digest + 1 WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            params![cluster_epoch.get() as i64, self.pg_id as i64, log_index as i64],
            "increment metadata command pre-state digest for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_clear_metadata_command_log_post_state_digest(
        &self,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_command_log SET post_state_digest = NULL WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            params![cluster_epoch.get() as i64, self.pg_id as i64, log_index as i64],
            "clear metadata command post-state digest for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_insert_abandoned_metadata_command_log_entry(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "INSERT INTO metadata_command_log (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL, NULL)",
            params![
                command.id().cluster_epoch().get() as i64,
                self.pg_id as i64,
                command.id().log_index().get() as i64,
                command.abandoned_log_checksum_crc64() as i64,
                command.abandoned_log_bytes(),
            ],
            "insert abandoned metadata command log entry for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_increment_metadata_command_replica_state_digest(
        &self,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_command_replica_state SET state_digest = state_digest + 1 WHERE singleton = 0",
            [],
            "increment metadata command replica state digest for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_increment_metadata_table_digest(
        &self,
        table_name: &str,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_table_digests SET table_digest = table_digest + 1 WHERE table_name = ?1",
            params![table_name],
            "increment cached metadata table digest for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_increment_metadata_command_replica_applied_log_hash(
        &self,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_command_replica_state SET applied_log_hash = applied_log_hash + 1 WHERE singleton = 0",
            [],
            "increment metadata command replica applied log hash for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_set_metadata_command_replica_applied_log_hash(
        &self,
        applied_log_hash: u64,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_command_replica_state SET applied_log_hash = ?1 WHERE singleton = 0",
            params![applied_log_hash as i64],
            "replace metadata command replica applied log hash for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_replace_metadata_command_replica_state(
        &self,
        state: MetadataCommandReplicaState,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_command_replica_state SET cluster_epoch = ?1, applied_log_index = ?2, \
             applied_log_hash_encoding_version = ?3, applied_log_hash = ?4, \
             state_digest_encoding_version = ?5, state_digest = ?6 WHERE singleton = 0",
            params![
                state.cluster_epoch.get() as i64,
                state.applied_log_index as i64,
                state.applied_log_hash.encoding_version() as i64,
                state.applied_log_hash.value() as i64,
                state.state_digest.encoding_version() as i64,
                state.state_digest.value() as i64,
            ],
            "replace metadata command replica state for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_delete_metadata_command_replica_state(&self) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "DELETE FROM metadata_command_replica_state WHERE singleton = 0",
            [],
            "delete metadata command replica state for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_replace_pending_metadata_command_slot(
        &self,
        command: &MetadataCommandEnvelope,
        scope_bucket: Option<&BucketName>,
    ) -> Result<(), StoreError> {
        let (reference_count, pages) =
            self.encode_pending_placed_segment_reference_pages(command)?;
        self.with_pending_slot_transaction(|| {
            self.execute_cached(
                "INSERT INTO metadata_command_pending_slot (singleton, cluster_epoch, pg_id, log_index, command_checksum, command_bytes, placed_segment_reference_count, scope_bucket) VALUES (0, ?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(singleton) DO UPDATE SET cluster_epoch = excluded.cluster_epoch, pg_id = excluded.pg_id, log_index = excluded.log_index, command_checksum = excluded.command_checksum, command_bytes = excluded.command_bytes, publication_started = 0, placed_segment_reference_count = excluded.placed_segment_reference_count, scope_bucket = excluded.scope_bucket",
                params![
                    command.id().cluster_epoch().get() as i64,
                    command.id().pg_id().get() as i64,
                    command.id().log_index().get() as i64,
                    command.checksum_crc64() as i64,
                    command.command_bytes(),
                    reference_count as i64,
                    scope_bucket,
                ],
                "replace pending metadata command slot for test",
            )?;
            self.replace_pending_placed_reference_pages(reference_count, &pages)
        })
    }

    #[cfg(test)]
    pub(crate) fn test_insert_raw_pending_metadata_command_slot(
        &self,
        id: MetadataCommandId,
        checksum: u64,
        bytes: &[u8],
        scope_bucket: Option<&BucketName>,
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "INSERT INTO metadata_command_pending_slot (singleton, cluster_epoch, pg_id, log_index, command_checksum, command_bytes, placed_segment_reference_count, scope_bucket) VALUES (0, ?1, ?2, ?3, ?4, ?5, 0, ?6)",
            params![
                id.cluster_epoch().get() as i64,
                id.pg_id().get() as i64,
                id.log_index().get() as i64,
                checksum as i64,
                bytes,
                scope_bucket,
            ],
            "insert raw pending metadata command slot for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_clear_pending_metadata_command_slot(&self) -> Result<bool, StoreError> {
        self.execute_cached(
            "DELETE FROM metadata_command_pending_slot WHERE singleton = 0",
            [],
            "clear pending metadata command slot for test",
        )
        .map(|changed| changed == 1)
    }

    #[cfg(test)]
    pub(crate) fn test_set_pending_metadata_command_bytes(
        &self,
        bytes: &[u8],
    ) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_command_pending_slot SET command_bytes = ?1 WHERE singleton = 0",
            params![bytes],
            "replace pending metadata command bytes for test",
        )?;
        Self::test_require_one_changed(changed)
    }

    #[cfg(test)]
    pub(crate) fn test_metadata_command_log_row_count(&self) -> Result<u64, StoreError> {
        let count = self.query_row_cached(
            "SELECT count(*) FROM metadata_command_log",
            [],
            "inspect metadata command log row count for test",
            |row| row.get::<_, i64>(0),
        )?;
        decode_nonnegative_u64("decode metadata command log row count for test", count)
    }

    pub(super) fn ensure_metadata_digest_bootstrap(&self) -> Result<(), StoreError> {
        if self.metadata_digest_bootstrap_complete()? {
            self.validate_metadata_digest_bootstrap()?;
            return Ok(());
        }

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| StoreError::Db {
                context: "begin metadata digest bootstrap",
                source: e.into(),
            })?;
        let result = (|| {
            if self.metadata_digest_bootstrap_complete()? {
                return self.validate_metadata_digest_bootstrap();
            }
            self.install_metadata_digest_triggers()?;
            self.refresh_all_metadata_table_digests()?;
            self.mark_metadata_digest_bootstrap_complete()
        })();
        match result {
            Ok(()) => self.conn.execute_batch("COMMIT").map_err(|e| {
                let _ = self.conn.execute_batch("ROLLBACK");
                StoreError::Db {
                    context: "commit metadata digest bootstrap",
                    source: e.into(),
                }
            }),
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn validate_metadata_digest_bootstrap(&self) -> Result<(), StoreError> {
        for table in METADATA_DIGEST_TABLES {
            if !self.metadata_digest_row_exists(table)? {
                return Err(StoreError::MetadataDigestBootstrapInvalid {
                    reason: format!("missing digest row for table {}", table.name),
                });
            }
            if !self.metadata_digest_triggers_complete(table)? {
                return Err(StoreError::MetadataDigestBootstrapInvalid {
                    reason: format!("missing digest trigger for table {}", table.name),
                });
            }
        }
        Ok(())
    }

    pub(super) fn metadata_digest_bootstrap_complete(&self) -> Result<bool, StoreError> {
        self.query_row_cached_optional(
            "SELECT completed FROM metadata_digest_bootstrap_state WHERE singleton = 0",
            [],
            "check metadata digest bootstrap state",
            |row| row.get::<_, i64>(0),
        )
        .and_then(|completed| match completed {
            Some(0) => Ok(false),
            Some(1) => Ok(true),
            Some(value) => Err(StoreError::MetadataDigestBootstrapInvalid {
                reason: format!("invalid completion value {value}"),
            }),
            None => Err(StoreError::MetadataDigestBootstrapInvalid {
                reason: "missing singleton marker".to_string(),
            }),
        })
    }

    fn mark_metadata_digest_bootstrap_complete(&self) -> Result<(), StoreError> {
        let changed = self.execute_cached(
            "UPDATE metadata_digest_bootstrap_state SET completed = 1 \
             WHERE singleton = 0 AND completed = 0",
            [],
            "mark metadata digest bootstrap complete",
        )?;
        if changed != 1 {
            return Err(StoreError::MetadataDigestBootstrapInvalid {
                reason: "bootstrap marker was not incomplete".to_string(),
            });
        }
        Ok(())
    }

    fn install_metadata_digest_triggers(&self) -> Result<(), StoreError> {
        for table in METADATA_DIGEST_TABLES {
            self.execute_cached(
                "INSERT INTO metadata_table_digests \
                 (table_name, table_digest, row_count, row_hash_xor, row_hash_sum) \
                 VALUES (?1, ?2, 0, 0, 0)",
                params![
                    table.name,
                    metadata_table_digest_from_stats(
                        table,
                        MetadataTableDigestStats {
                            row_count: 0,
                            row_hash_xor: 0,
                            row_hash_sum: 0,
                        },
                    ) as i64,
                ],
                "initialize metadata digest table row",
            )?;
            self.conn
                .execute_batch(&self.metadata_digest_trigger_sql(table))
                .map_err(|e| StoreError::Db {
                    context: "install metadata digest triggers",
                    source: e.into(),
                })?;
        }
        Ok(())
    }

    fn metadata_digest_row_exists(&self, table: &MetadataDigestTable) -> Result<bool, StoreError> {
        self.query_row_cached_optional(
            "SELECT 1 FROM metadata_table_digests WHERE table_name = ?1",
            params![table.name],
            "check metadata digest table row",
            |_| Ok(()),
        )
        .map(|exists| exists.is_some())
    }

    fn metadata_digest_triggers_complete(
        &self,
        table: &MetadataDigestTable,
    ) -> Result<bool, StoreError> {
        for suffix in ["ai", "ad", "au"] {
            let exists = self
                .query_row_cached_optional(
                    "SELECT 1 FROM sqlite_master WHERE type = 'trigger' AND name = ?1",
                    params![format!("metadata_digest_{}_{}", table.name, suffix)],
                    "check metadata digest trigger",
                    |_| Ok(()),
                )?
                .is_some();
            if !exists {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn metadata_digest_trigger_sql(&self, table: &MetadataDigestTable) -> String {
        let table_name = quote_sql_identifier(table.name);
        let table_literal = quote_sql_string(table.name);
        let new_digest = Self::metadata_row_digest_sql_expr(table, "NEW");
        let old_digest = Self::metadata_row_digest_sql_expr(table, "OLD");
        format!(
			"CREATE TRIGGER metadata_digest_{name}_ai \
			 AFTER INSERT ON {table_name} BEGIN \
			   UPDATE metadata_table_digests \
			      SET (row_count, row_hash_xor, row_hash_sum, table_digest) = ( \
			          SELECT next_count, next_xor, next_sum, \
			                 argmin_metadata_table_digest(table_name, next_count, next_xor, next_sum) \
			            FROM ( \
			              SELECT row_count + 1 AS next_count, \
			                     argmin_crc64_xor(row_hash_xor, row_digest) AS next_xor, \
			                     argmin_crc64_add(row_hash_sum, row_digest) AS next_sum \
			                FROM (SELECT {new_digest} AS row_digest) \
			            ) \
				      ) \
				    WHERE table_name = {table_literal}; \
				   UPDATE metadata_digest_revision \
				      SET revision = revision + 1 \
				    WHERE singleton = 0; \
				 END; \
				 CREATE TRIGGER metadata_digest_{name}_ad \
				 AFTER DELETE ON {table_name} BEGIN \
			   UPDATE metadata_table_digests \
			      SET (row_count, row_hash_xor, row_hash_sum, table_digest) = ( \
			          SELECT next_count, next_xor, next_sum, \
			                 argmin_metadata_table_digest(table_name, next_count, next_xor, next_sum) \
			            FROM ( \
			              SELECT row_count - 1 AS next_count, \
			                     argmin_crc64_xor(row_hash_xor, row_digest) AS next_xor, \
			                     argmin_crc64_sub(row_hash_sum, row_digest) AS next_sum \
			                FROM (SELECT {old_digest} AS row_digest) \
			            ) \
				      ) \
				    WHERE table_name = {table_literal}; \
				   UPDATE metadata_digest_revision \
				      SET revision = revision + 1 \
				    WHERE singleton = 0; \
				 END; \
				 CREATE TRIGGER metadata_digest_{name}_au \
				 AFTER UPDATE ON {table_name} BEGIN \
			   UPDATE metadata_table_digests \
			      SET (row_hash_xor, row_hash_sum, table_digest) = ( \
			          SELECT next_xor, next_sum, \
			                 argmin_metadata_table_digest(table_name, row_count, next_xor, next_sum) \
			            FROM ( \
			              SELECT argmin_crc64_xor(argmin_crc64_xor(row_hash_xor, old_row_digest), new_row_digest) AS next_xor, \
			                     argmin_crc64_add(argmin_crc64_sub(row_hash_sum, old_row_digest), new_row_digest) AS next_sum \
			                FROM ( \
			                  SELECT {old_digest} AS old_row_digest, \
			                         {new_digest} AS new_row_digest \
			                ) \
			            ) \
				      ) \
				    WHERE table_name = {table_literal}; \
				   UPDATE metadata_digest_revision \
				      SET revision = revision + 1 \
				    WHERE singleton = 0; \
				 END;",
			name = table.name,
		)
    }

    fn metadata_row_digest_sql_expr(table: &MetadataDigestTable, qualifier: &str) -> String {
        let mut args = Vec::with_capacity(table.columns.len() + 1);
        args.push(quote_sql_string(table.name));
        args.extend(
            table
                .columns
                .iter()
                .map(|column| format!("{qualifier}.{}", quote_sql_identifier(column))),
        );
        format!("argmin_metadata_row_digest({})", args.join(", "))
    }

    pub(super) fn ensure_metadata_command_replica_state(
        &self,
        initial_cluster_epoch: ClusterEpoch,
    ) -> Result<(), StoreError> {
        let exists = self.query_row_cached_optional(
            "SELECT 1 FROM metadata_command_replica_state WHERE singleton = 0",
            [],
            "load metadata command replica state",
            |_| Ok(()),
        )?;
        if exists.is_some() {
            return Ok(());
        }
        if !self.metadata_command_replica_state_can_initialize()? {
            return Err(StoreError::MetadataCommandReplicaStateMissing { pg_id: self.pg_id });
        }

        let state_digest = self.metadata_state_digest()?;
        self.execute_cached(
            "INSERT INTO metadata_command_replica_state \
             (singleton, cluster_epoch, applied_log_index, applied_log_hash_encoding_version, \
              applied_log_hash, state_digest_encoding_version, state_digest) \
             VALUES (0, ?1, 0, ?2, 0, ?3, ?4)",
            params![
                initial_cluster_epoch.get() as i64,
                MetadataCommandLogHash::from_storage(0, super::MetadataProofStorageIssuer::new(),)
                    .encoding_version() as i64,
                CanonicalStateDigest::from_storage(
                    state_digest,
                    super::MetadataProofStorageIssuer::new(),
                )
                .encoding_version() as i64,
                state_digest as i64,
            ],
            "initialize metadata command replica state",
        )?;
        Ok(())
    }

    pub(crate) fn metadata_command_replica_state_can_initialize(&self) -> Result<bool, StoreError> {
        if self
            .query_row_cached_optional(
                "SELECT 1 FROM metadata_command_log LIMIT 1",
                [],
                "check metadata command log emptiness",
                |_| Ok(()),
            )?
            .is_some()
        {
            return Ok(false);
        }
        if self
            .query_row_cached_optional(
                "SELECT 1 FROM metadata_command_terminal_receipts LIMIT 1",
                [],
                "check compacted metadata command receipt emptiness",
                |_| Ok(()),
            )?
            .is_some()
        {
            return Ok(false);
        }

        for table in METADATA_DIGEST_TABLES {
            if table.name == "pg_counters" {
                let has_default_counter = self
                    .query_row_cached_optional(
                        "SELECT 1 FROM pg_counters \
                         WHERE singleton = 0 AND next_bucket_execution_generation = 0",
                        [],
                        "check canonical metadata counter baseline",
                        |_| Ok(()),
                    )?
                    .is_some();
                let has_unexpected_counter = self
                    .query_row_cached_optional(
                        "SELECT 1 FROM pg_counters \
                         WHERE singleton != 0 OR next_bucket_execution_generation != 0 \
                         LIMIT 1",
                        [],
                        "check canonical metadata counter baseline",
                        |_| Ok(()),
                    )?
                    .is_some();
                if !has_default_counter || has_unexpected_counter {
                    return Ok(false);
                }
                continue;
            }
            let table_sql = quote_sql_identifier(table.name);
            let where_clause = Self::metadata_digest_where_clause(table.filter);
            let sql = format!("SELECT 1 FROM {table_sql}{where_clause} LIMIT 1");
            if self
                .conn
                .query_row(&sql, [], |_| Ok(()))
                .optional()
                .map_err(|e| StoreError::Db {
                    context: "check canonical metadata state emptiness",
                    source: e.into(),
                })?
                .is_some()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn initialize_metadata_transfer_empty_state(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        expected_state_digest: CanonicalStateDigest,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| StoreError::Db {
                context: "begin metadata transfer empty state initialization",
                source: e.into(),
            })?;

        let result = (|| {
            if self
                .query_row_cached_optional(
                    "SELECT 1 FROM metadata_command_pending_slot WHERE singleton = 0",
                    [],
                    "check metadata transfer empty destination pending slot",
                    |_| Ok(()),
                )?
                .is_some()
            {
                return Err(StoreError::MetadataCommandContention {
                    context: "initialize metadata transfer empty state with pending command",
                });
            }

            if !self.metadata_command_replica_state_can_initialize()? {
                return Err(StoreError::MetadataCommandReplicaStateMissing { pg_id: self.pg_id });
            }

            let actual_digest = self.metadata_state_digest()?;
            if actual_digest != expected_state_digest.value() {
                return Err(StoreError::MetadataStateDigestMismatch {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    expected_digest: expected_state_digest.value(),
                    actual_digest,
                });
            }

            self.update_metadata_command_replica_state_preserving_digest(
                cluster_epoch,
                0,
                0,
                expected_state_digest,
            )
            .map(|record| record.state)
        })();

        match result {
            Ok(state) => {
                self.conn.execute_batch("COMMIT").map_err(|e| {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    StoreError::Db {
                        context: "commit metadata transfer empty state initialization",
                        source: e.into(),
                    }
                })?;
                self.mark_metadata_state_digest_clean()?;
                Ok(state)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    pub(crate) fn initialize_metadata_transfer_matching_state(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        applied_log_index: u64,
        applied_log_hash: MetadataCommandLogHash,
        expected_state_digest: CanonicalStateDigest,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if applied_log_index != 0 || applied_log_hash.value() != 0 {
            return Err(StoreError::MetadataTransferUnsupportedProof {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                applied_log_index,
                applied_log_hash: applied_log_hash.value(),
            });
        }

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| StoreError::Db {
                context: "begin metadata transfer matching state initialization",
                source: e.into(),
            })?;

        let result = (|| {
            if self
                .query_row_cached_optional(
                    "SELECT 1 FROM metadata_command_pending_slot WHERE singleton = 0",
                    [],
                    "check metadata transfer matching destination pending slot",
                    |_| Ok(()),
                )?
                .is_some()
            {
                return Err(StoreError::MetadataCommandContention {
                    context: "initialize metadata transfer matching state with pending command",
                });
            }

            let actual_digest = self.metadata_state_digest()?;
            if actual_digest != expected_state_digest.value() {
                return Err(StoreError::MetadataStateDigestMismatch {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    expected_digest: expected_state_digest.value(),
                    actual_digest,
                });
            }

            self.update_metadata_command_replica_state_preserving_digest(
                cluster_epoch,
                applied_log_index,
                applied_log_hash.value(),
                expected_state_digest,
            )
            .map(|record| record.state)
        })();

        match result {
            Ok(state) => {
                self.conn.execute_batch("COMMIT").map_err(|e| {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    StoreError::Db {
                        context: "commit metadata transfer matching state initialization",
                        source: e.into(),
                    }
                })?;
                self.mark_metadata_state_digest_clean()?;
                Ok(state)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn install_metadata_transfer_checkpoint_base(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        checkpoint: &MetadataCommandCheckpoint,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if checkpoint.pg_id != PgId::new(self.pg_id) {
            return Err(StoreError::MetadataCheckpointInvalid {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                reason: format!(
                    "checkpoint PG {} does not match destination PG {}",
                    checkpoint.pg_id.get(),
                    self.pg_id
                ),
            });
        }
        checkpoint
            .verify()
            .map_err(|error| StoreError::MetadataCheckpointInvalid {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                reason: format!("{error:?}"),
            })?;

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| StoreError::Db {
                context: "begin metadata transfer checkpoint install",
                source: e.into(),
            })?;
        self.conn
            .execute_batch("PRAGMA defer_foreign_keys = ON")
            .map_err(|e| {
                let _ = self.conn.execute_batch("ROLLBACK");
                StoreError::Db {
                    context: "defer metadata transfer checkpoint foreign keys",
                    source: e.into(),
                }
            })?;

        let result = (|| {
            if self
                .query_row_cached_optional(
                    "SELECT 1 FROM metadata_command_pending_slot WHERE singleton = 0",
                    [],
                    "check metadata transfer checkpoint destination pending slot",
                    |_| Ok(()),
                )?
                .is_some()
            {
                return Err(StoreError::MetadataCommandContention {
                    context: "install metadata transfer checkpoint with pending command",
                });
            }

            if !self.metadata_command_replica_state_can_initialize()? {
                let current = self.metadata_command_replica_state()?;
                if current.cluster_epoch >= cluster_epoch {
                    return Err(StoreError::MetadataCommandReplicaStateMissing {
                        pg_id: self.pg_id,
                    });
                }
                self.clear_metadata_transfer_checkpoint_destination_state()?;
            }

            self.clear_metadata_checkpoint_tables()?;
            self.insert_metadata_checkpoint_table_blocks(&checkpoint.table_blocks)?;
            self.refresh_all_metadata_table_digests()?;
            let actual_digest = self.cached_metadata_state_digest()?;
            if actual_digest != checkpoint.state_digest.value() {
                return Err(StoreError::MetadataStateDigestMismatch {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    expected_digest: checkpoint.state_digest.value(),
                    actual_digest,
                });
            }

            self.update_metadata_command_replica_state_preserving_digest(
                cluster_epoch,
                0,
                0,
                checkpoint.state_digest,
            )
            .map(|record| record.state)
        })();

        match result {
            Ok(state) => {
                self.conn.execute_batch("COMMIT").map_err(|e| {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    StoreError::Db {
                        context: "commit metadata transfer checkpoint install",
                        source: e.into(),
                    }
                })?;
                self.mark_metadata_state_digest_clean()?;
                Ok(state)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    pub(crate) fn adopt_metadata_transfer_state_from_rebased_commands(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        commands: &[MetadataTransferCommand],
        expected_state_digest: CanonicalStateDigest,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        if commands.is_empty() {
            return Err(StoreError::MetadataTransferEmpty {
                pg_id: self.pg_id,
                cluster_epoch,
            });
        }

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| StoreError::Db {
                context: "begin metadata transfer state adoption",
                source: e.into(),
            })?;

        let result = (|| {
            if self
                .query_row_cached_optional(
                    "SELECT 1 FROM metadata_command_pending_slot WHERE singleton = 0",
                    [],
                    "check metadata transfer destination pending slot",
                    |_| Ok(()),
                )?
                .is_some()
            {
                return Err(StoreError::MetadataCommandContention {
                    context: "adopt metadata transfer state with pending command",
                });
            }

            let actual_digest = self.metadata_state_digest()?;
            if actual_digest != expected_state_digest.value() {
                return Err(StoreError::MetadataStateDigestMismatch {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    expected_digest: expected_state_digest.value(),
                    actual_digest,
                });
            }

            let pg_id = PgId::new(self.pg_id);
            let mut previous_log_hash = 0;
            let mut applied_log_index: u64 = 0;
            for transfer_command in commands {
                let command = &transfer_command.command;
                if command.id().cluster_epoch() != cluster_epoch
                    || command.id().pg_id() != pg_id
                    || command.id().log_index().get()
                        != applied_log_index
                            .checked_add(1)
                            .expect("metadata transfer log index overflow")
                {
                    return Err(StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id: self.pg_id,
                        cluster_epoch,
                        log_index: command.id().log_index().get(),
                    });
                }

                let command_checksum = command.checksum_crc64();
                let log_hash = metadata_command_log_hash(
                    cluster_epoch,
                    pg_id,
                    command.id().log_index(),
                    previous_log_hash,
                    command_checksum,
                );
                let inserted = self.execute_cached(
                    "INSERT INTO metadata_command_log \
                     (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash, pre_state_digest, post_state_digest) \
                     VALUES (?1, ?2, ?3, ?4, ?5, 0, ?6, ?7, ?8, ?9) \
                     ON CONFLICT(cluster_epoch, pg_id, log_index) DO NOTHING",
                    params![
                        cluster_epoch.get() as i64,
                        self.pg_id as i64,
                        command.id().log_index().get() as i64,
                        command_checksum as i64,
                        command.command_bytes(),
                        previous_log_hash as i64,
                        log_hash.value() as i64,
                        transfer_command.pre_state_digest.value() as i64,
                        transfer_command.post_state_digest.value() as i64,
                    ],
                    "install metadata transfer command log entry",
                )?;
                if inserted == 0 {
                    let entry = self
                        .load_metadata_command_log_entry(
                            "load existing metadata transfer command log entry",
                            cluster_epoch,
                            pg_id,
                            command.id().log_index(),
                        )?
                        .expect("metadata command log conflict must leave an entry");
                    if !self.metadata_command_log_entry_matches(node_id, command, &entry, false)?
                        || entry.previous_log_hash != Some(previous_log_hash)
                        || entry.log_hash != Some(log_hash.value())
                        || entry.pre_state_digest != Some(transfer_command.pre_state_digest.value())
                        || entry.post_state_digest
                            != Some(transfer_command.post_state_digest.value())
                    {
                        return Err(StoreError::MetadataCommandLogConflict {
                            node_id,
                            pg_id: self.pg_id,
                            cluster_epoch,
                            log_index: command.id().log_index().get(),
                        });
                    }
                }
                applied_log_index = command.id().log_index().get();
                previous_log_hash = log_hash.value();
            }

            self.update_metadata_command_replica_state_preserving_digest(
                cluster_epoch,
                applied_log_index,
                previous_log_hash,
                expected_state_digest,
            )
            .map(|record| record.state)
        })();

        match result {
            Ok(state) => {
                self.conn.execute_batch("COMMIT").map_err(|e| {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    StoreError::Db {
                        context: "commit metadata transfer state adoption",
                        source: e.into(),
                    }
                })?;
                self.mark_metadata_state_digest_clean()?;
                Ok(state)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    pub(crate) fn metadata_command_replica_state(
        &self,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let (
            cluster_epoch,
            applied_log_index,
            applied_log_hash_version,
            applied_log_hash,
            state_digest_version,
            state_digest,
        ): (i64, i64, i64, i64, i64, i64) = self.query_row_cached(
            "SELECT cluster_epoch, applied_log_index, applied_log_hash_encoding_version, \
                    applied_log_hash, state_digest_encoding_version, state_digest \
             FROM metadata_command_replica_state WHERE singleton = 0",
            [],
            "load metadata command replica state",
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )?;
        Ok(MetadataCommandReplicaState {
            cluster_epoch: ClusterEpoch::new(cluster_epoch as u64)
                .expect("metadata command replica state stores non-zero epoch"),
            applied_log_index: applied_log_index as u64,
            applied_log_hash: decode_metadata_command_log_hash(
                self.pg_id,
                applied_log_hash_version,
                applied_log_hash,
            )?,
            state_digest: decode_canonical_state_digest(
                self.pg_id,
                state_digest_version,
                state_digest,
            )?,
        })
    }

    fn metadata_command_replica_state_with_digest_revision(
        &self,
    ) -> Result<(MetadataCommandReplicaState, u64), StoreError> {
        let (
            cluster_epoch,
            applied_log_index,
            applied_log_hash_version,
            applied_log_hash,
            state_digest_version,
            state_digest,
            revision,
        ): (
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
        ) = self.query_row_cached(
            "SELECT s.cluster_epoch, s.applied_log_index, s.applied_log_hash_encoding_version, \
                    s.applied_log_hash, s.state_digest_encoding_version, s.state_digest, r.revision \
             FROM metadata_command_replica_state s, metadata_digest_revision r \
             WHERE s.singleton = 0 AND r.singleton = 0",
            [],
            "load metadata command replica state and digest revision",
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )?;
        let state = MetadataCommandReplicaState {
            cluster_epoch: ClusterEpoch::new(cluster_epoch as u64)
                .expect("metadata command replica state stores non-zero epoch"),
            applied_log_index: applied_log_index as u64,
            applied_log_hash: decode_metadata_command_log_hash(
                self.pg_id,
                applied_log_hash_version,
                applied_log_hash,
            )?,
            state_digest: decode_canonical_state_digest(
                self.pg_id,
                state_digest_version,
                state_digest,
            )?,
        };
        let revision = decode_nonnegative_u64("decode metadata digest revision", revision)?;
        Ok((state, revision))
    }

    pub(crate) fn max_metadata_command_log_index(
        &self,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError> {
        let state = self.metadata_command_replica_state()?;
        let raw = self.query_row_cached(
            "SELECT max(log_index) FROM metadata_command_log \
             WHERE cluster_epoch = ?1 AND pg_id = ?2",
            params![cluster_epoch.get() as i64, self.pg_id as i64],
            "load max metadata command log index",
            |row| row.get::<_, Option<i64>>(0),
        )?;
        let max_retained: u64 = raw
            .unwrap_or_default()
            .try_into()
            .map_err(|_| StoreError::Db {
                context: "decode max metadata command log index",
                source: crate::error::DatabaseError::from_sql_conversion_failure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from("negative metadata command log index"),
                ),
            })?;
        if state.cluster_epoch == cluster_epoch {
            Ok(max_retained.max(state.applied_log_index))
        } else {
            Ok(max_retained)
        }
    }

    pub(crate) fn has_matching_applied_metadata_command_log_entry(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
        expected_previous_log_hash: u64,
    ) -> Result<bool, StoreError> {
        let Some(entry) = self.load_metadata_command_log_entry(
            "load metadata command log entry for acting-set validation",
            command.id().cluster_epoch(),
            command.id().pg_id(),
            command.id().log_index(),
        )?
        else {
            return Ok(self
                .metadata_command_terminal_receipt_matches(node_id, command, false)?
                .is_some_and(|(previous_log_hash, _)| {
                    previous_log_hash == expected_previous_log_hash
                }));
        };
        if !self.metadata_command_log_entry_matches(node_id, command, &entry, false)? {
            return Ok(false);
        }
        let expected_log_hash = metadata_command_log_hash(
            command.id().cluster_epoch(),
            command.id().pg_id(),
            command.id().log_index(),
            expected_previous_log_hash,
            command.checksum_crc64(),
        );
        Ok(entry.previous_log_hash == Some(expected_previous_log_hash)
            && entry.log_hash == Some(expected_log_hash.value()))
    }

    pub(crate) fn applied_metadata_command_log_entry_hashes(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        let Some(entry) = self.load_metadata_command_log_entry(
            "load metadata command log entry for partial apply retry validation",
            command.id().cluster_epoch(),
            command.id().pg_id(),
            command.id().log_index(),
        )?
        else {
            if let Some(hashes) =
                self.metadata_command_terminal_receipt_matches(node_id, command, false)?
            {
                return Ok(Some(hashes));
            }
            if self
                .load_metadata_command_terminal_receipt(
                    "load divergent compacted metadata command terminal receipt",
                    command.id().cluster_epoch(),
                    command.id().pg_id(),
                    command.id().log_index(),
                )?
                .is_some()
            {
                return Err(self.metadata_command_log_conflict(node_id, command));
            }
            return Ok(None);
        };
        if !self.metadata_command_log_entry_matches(node_id, command, &entry, false)? {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
                log_index: command.id().log_index().get(),
            });
        }
        let Some(previous_log_hash) = entry.previous_log_hash else {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
                log_index: command.id().log_index().get(),
            });
        };
        let Some(log_hash) = entry.log_hash else {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
                log_index: command.id().log_index().get(),
            });
        };
        Ok(Some((previous_log_hash, log_hash)))
    }

    pub(crate) fn retained_metadata_command_log_hashes(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogHashRangeEntry>, StoreError> {
        let mut entries = Vec::new();
        for raw_log_index in first_log_index.get()..=last_log_index.get() {
            let log_index = MetadataCommandLogIndex::new(raw_log_index)
                .expect("metadata command log index range starts non-zero");
            let Some(entry) = self.load_metadata_command_log_entry(
                "load retained metadata command log entry hash range",
                cluster_epoch,
                PgId::new(self.pg_id),
                log_index,
            )?
            else {
                continue;
            };
            self.verify_metadata_command_log_entry(
                node_id,
                cluster_epoch,
                PgId::new(self.pg_id),
                log_index,
                &entry,
            )?;
            let Some(previous_log_hash) = entry.previous_log_hash else {
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    log_index: raw_log_index,
                });
            };
            let Some(log_hash) = entry.log_hash else {
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    log_index: raw_log_index,
                });
            };
            entries.push(MetadataCommandLogHashRangeEntry {
                log_index: raw_log_index,
                previous_log_hash: MetadataCommandLogHash::from_storage(
                    previous_log_hash,
                    super::MetadataProofStorageIssuer::new(),
                ),
                log_hash: MetadataCommandLogHash::from_storage(
                    log_hash,
                    super::MetadataProofStorageIssuer::new(),
                ),
            });
        }
        Ok(entries)
    }

    pub(crate) fn retained_metadata_command_log_entries(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        first_log_index: MetadataCommandLogIndex,
        last_log_index: MetadataCommandLogIndex,
    ) -> Result<Vec<MetadataCommandLogRangeEntry>, StoreError> {
        let mut entries = Vec::new();
        for raw_log_index in first_log_index.get()..=last_log_index.get() {
            let log_index = MetadataCommandLogIndex::new(raw_log_index)
                .expect("metadata command log index range starts non-zero");
            let Some(entry) = self.load_metadata_command_log_entry(
                "load retained metadata command log entry range",
                cluster_epoch,
                PgId::new(self.pg_id),
                log_index,
            )?
            else {
                continue;
            };
            self.verify_metadata_command_log_entry(
                node_id,
                cluster_epoch,
                PgId::new(self.pg_id),
                log_index,
                &entry,
            )?;
            let Some(previous_log_hash) = entry.previous_log_hash else {
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    log_index: raw_log_index,
                });
            };
            let Some(log_hash) = entry.log_hash else {
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    log_index: raw_log_index,
                });
            };
            let header =
                decode_metadata_command_log_entry_header(&entry.command_bytes).map_err(|_| {
                    StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id: self.pg_id,
                        cluster_epoch,
                        log_index: raw_log_index,
                    }
                })?;
            if header.id()
                != MetadataCommandId::new(cluster_epoch, PgId::new(self.pg_id), log_index)
            {
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    log_index: raw_log_index,
                });
            }
            let kind = match header.kind() {
                MetadataCommandLogEntryKind::Applied => {
                    if entry.abandoned {
                        return Err(StoreError::MetadataCommandLogConflict {
                            node_id,
                            pg_id: self.pg_id,
                            cluster_epoch,
                            log_index: raw_log_index,
                        });
                    }
                    let command = decode_metadata_command_envelope(
                        &entry.command_bytes,
                        &MetadataCommandDecodeAuthority::new(),
                    )
                    .map_err(|_| StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id: self.pg_id,
                        cluster_epoch,
                        log_index: raw_log_index,
                    })?;
                    MetadataCommandLogRangeEntryKind::Applied(Box::new(command))
                }
                MetadataCommandLogEntryKind::Abandoned {
                    original_command_checksum,
                } => {
                    if !entry.abandoned {
                        return Err(StoreError::MetadataCommandLogConflict {
                            node_id,
                            pg_id: self.pg_id,
                            cluster_epoch,
                            log_index: raw_log_index,
                        });
                    }
                    MetadataCommandLogRangeEntryKind::Abandoned {
                        original_command_checksum,
                    }
                }
            };
            entries.push(MetadataCommandLogRangeEntry {
                log_index: raw_log_index,
                previous_log_hash: MetadataCommandLogHash::from_storage(
                    previous_log_hash,
                    super::MetadataProofStorageIssuer::new(),
                ),
                log_hash: MetadataCommandLogHash::from_storage(
                    log_hash,
                    super::MetadataProofStorageIssuer::new(),
                ),
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
            });
        }
        Ok(entries)
    }

    pub(crate) fn metadata_command_log_entry_command_kind_name(
        &self,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    ) -> Result<Option<&'static str>, StoreError> {
        let Some(log_index) = MetadataCommandLogIndex::new(log_index) else {
            return Ok(None);
        };
        let Some(entry) = self.load_metadata_command_log_entry(
            "load metadata command log entry for conflict diagnostics",
            cluster_epoch,
            PgId::new(self.pg_id),
            log_index,
        )?
        else {
            return Ok(None);
        };
        let Ok(header) = decode_metadata_command_log_entry_header(&entry.command_bytes) else {
            return Ok(None);
        };
        Ok(header.command_kind_name())
    }

    pub(crate) fn pending_metadata_command_slot(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<PendingMetadataCommandSlot>, StoreError> {
        let Some(slot) = self.pending_metadata_command_slot_any_epoch(node_id)? else {
            return Ok(None);
        };
        if slot.id.cluster_epoch() != cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: self.pg_id,
                operation_epoch: cluster_epoch,
                current_epoch: slot.id.cluster_epoch(),
            });
        }
        Ok(Some(slot))
    }

    pub(crate) fn pending_metadata_command_slot_any_epoch(
        &self,
        node_id: u32,
    ) -> Result<Option<PendingMetadataCommandSlot>, StoreError> {
        let raw = self.query_row_cached_optional(
            "SELECT cluster_epoch, pg_id, log_index, command_checksum, command_bytes, \
                    publication_started, scope_bucket \
             FROM metadata_command_pending_slot WHERE singleton = 0",
            [],
            "load metadata command pending slot",
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Vec<u8>>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            },
        )?;
        let Some((
            raw_cluster_epoch,
            raw_pg_id,
            raw_log_index,
            raw_command_checksum,
            command_bytes,
            raw_publication_started,
            raw_scope_bucket,
        )) = raw
        else {
            return Ok(None);
        };
        let stored_epoch = ClusterEpoch::new(decode_nonnegative_u64(
            "decode pending slot cluster epoch",
            raw_cluster_epoch,
        )?)
        .expect("pending slot stores non-zero cluster epoch");
        let stored_pg_id: u32 = raw_pg_id.try_into().map_err(|_| StoreError::Db {
            context: "decode pending slot PG id",
            source: crate::error::DatabaseError::from_sql_conversion_failure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("negative metadata command pending slot PG id"),
            ),
        })?;
        if stored_pg_id != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id,
                command_pg_id: stored_pg_id,
                target_pg_id: self.pg_id,
                cluster_epoch: stored_epoch,
            });
        }
        let log_index = MetadataCommandLogIndex::new(decode_nonnegative_u64(
            "decode pending slot log index",
            raw_log_index,
        )?)
        .expect("pending slot stores non-zero log index");
        let computed_checksum = checksum::crc64::checksum(&command_bytes);
        let command_checksum = raw_command_checksum as u64;
        if computed_checksum != command_checksum {
            return Err(StoreError::MetadataCommandLogChecksumMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: stored_epoch,
                log_index: log_index.get(),
                stored_checksum: command_checksum,
                computed_checksum,
            });
        }
        let header = decode_metadata_command_log_entry_header(&command_bytes).map_err(|_| {
            StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: stored_epoch,
                log_index: log_index.get(),
            }
        })?;
        let id = MetadataCommandId::new(stored_epoch, PgId::new(self.pg_id), log_index);
        if header.id() != id || header.kind() != MetadataCommandLogEntryKind::Applied {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: stored_epoch,
                log_index: log_index.get(),
            });
        }
        let scope_bucket = raw_scope_bucket
            .map(BucketName::try_from)
            .transpose()
            .map_err(|reason| StoreError::Db {
                context: "decode pending slot scope bucket",
                source: crate::error::DatabaseError::from_sql_conversion_failure(
                    6,
                    rusqlite::types::Type::Text,
                    Box::from(reason.to_string()),
                ),
            })?;
        Ok(Some(PendingMetadataCommandSlot {
            id,
            command_checksum,
            command_bytes,
            publication_started: match raw_publication_started {
                0 => false,
                1 => true,
                _ => {
                    return Err(StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id: self.pg_id,
                        cluster_epoch: stored_epoch,
                        log_index: log_index.get(),
                    });
                }
            },
            scope_bucket,
        }))
    }

    pub(crate) fn pending_metadata_command_publication_started(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        if command.id().pg_id().get() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id,
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
            });
        }
        let Some(slot) =
            self.pending_metadata_command_slot(node_id, command.id().cluster_epoch())?
        else {
            return Ok(false);
        };
        if slot.id != command.id()
            || slot.command_checksum != command.checksum_crc64()
            || slot.command_bytes != command.command_bytes()
        {
            return Ok(false);
        }
        Ok(slot.publication_started)
    }

    pub(crate) fn mark_pending_metadata_command_publication_started(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), BucketSnapshotLoadError> {
        if command.id().pg_id().get() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id,
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
            }
            .into());
        }
        self.conn
            .execute_batch("BEGIN IMMEDIATE; SAVEPOINT validate_publication_start")
            .map_err(|source| {
                BucketSnapshotLoadError::Metadata(MetadataError::Db {
                    context: "validate metadata command publication start (begin txn)",
                    source: source.into(),
                })
            })?;
        let result = (|| {
            match self
                .metadata_command_acceptance(node_id, command)
                .map_err(BucketSnapshotLoadError::Store)?
            {
                MetadataCommandAcceptance::Apply => self
                    .apply_metadata_command(command)
                    .map_err(BucketSnapshotLoadError::Metadata)?,
                MetadataCommandAcceptance::AlreadyApplied => {}
            }
            self.conn
                .execute_batch(
                    "ROLLBACK TO validate_publication_start; RELEASE validate_publication_start",
                )
                .map_err(|source| {
                    BucketSnapshotLoadError::Metadata(MetadataError::Db {
                        context: "validate metadata command publication start (rollback probe)",
                        source: source.into(),
                    })
                })?;
            self.invalidate_clean_metadata_digest_revision();

            let changed = self
                .execute_cached(
                    "UPDATE metadata_command_pending_slot SET publication_started = 1 \
                     WHERE singleton = 0 AND cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3 \
                       AND command_checksum = ?4 AND command_bytes = ?5",
                    params![
                        command.id().cluster_epoch().get() as i64,
                        command.id().pg_id().get() as i64,
                        command.id().log_index().get() as i64,
                        command.checksum_crc64() as i64,
                        command.command_bytes(),
                    ],
                    "mark pending metadata command publication started",
                )
                .map_err(BucketSnapshotLoadError::Store)?;
            if changed == 1
                || self
                    .pending_metadata_command_publication_started(node_id, command)
                    .map_err(BucketSnapshotLoadError::Store)?
            {
                Ok(())
            } else {
                Err(BucketSnapshotLoadError::Store(
                    StoreError::MetadataCommandPendingConflict {
                        pg_id: self.pg_id,
                        cluster_epoch: command.id().cluster_epoch(),
                        existing_log_index: 0,
                        candidate_log_index: command.id().log_index().get(),
                    },
                ))
            }
        })();

        match result {
            Ok(()) => self
                .commit_immediate_txn("mark metadata command publication started (commit txn)")
                .map_err(BucketSnapshotLoadError::Metadata),
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                self.invalidate_clean_metadata_digest_revision();
                Err(error)
            }
        }
    }

    /// Remove an epoch-mismatched pending slot during pre-serving recovery.
    ///
    /// This must not be called from heartbeat or other serving-time observation
    /// paths. A normal command can install a future-epoch pending slot before
    /// apply/record advances the durable replica state to that epoch, and no
    /// terminal log entry exists during that in-flight window. Only slots from
    /// older epochs are locally cleanable; future-epoch slots need cluster-level
    /// acting-set evidence before any recovery path can classify them as
    /// abandoned.
    pub(crate) fn clean_epoch_mismatched_orphan_pending_metadata_command_slot(
        &self,
        ctx: super::PgStoreRecoveryContext,
    ) -> Result<bool, StoreError> {
        let node_id = ctx.node_id().as_u32();
        let current_epoch = self.metadata_command_replica_state()?.cluster_epoch;
        let Some(slot) = self.pending_metadata_command_slot_any_epoch(node_id)? else {
            return Ok(false);
        };
        if slot.id.cluster_epoch() == current_epoch {
            return Ok(false);
        }
        if slot.id.cluster_epoch() > current_epoch {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: slot.id.cluster_epoch(),
                log_index: slot.id.log_index().get(),
            });
        }
        if self
            .load_metadata_command_log_entry(
                "load epoch-mismatched metadata command pending slot terminal entry",
                slot.id.cluster_epoch(),
                slot.id.pg_id(),
                slot.id.log_index(),
            )?
            .is_some()
            || self
                .load_metadata_command_terminal_receipt(
                    "check compacted pending metadata command terminal receipt",
                    slot.id.cluster_epoch(),
                    slot.id.pg_id(),
                    slot.id.log_index(),
                )?
                .is_some()
        {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: slot.id.cluster_epoch(),
                log_index: slot.id.log_index().get(),
            });
        }
        self.remove_pending_metadata_command_slot_exact(node_id, &slot)?;
        Ok(true)
    }

    pub(crate) fn pending_metadata_command_envelope(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let Some(slot) = self.pending_metadata_command_slot(node_id, cluster_epoch)? else {
            return Ok(None);
        };
        let computed_checksum = checksum::crc64::checksum(&slot.command_bytes);
        if computed_checksum != slot.command_checksum {
            return Err(StoreError::MetadataCommandLogChecksumMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                log_index: slot.id.log_index().get(),
                stored_checksum: slot.command_checksum,
                computed_checksum,
            });
        }
        let command = decode_metadata_command_envelope(
            &slot.command_bytes,
            &MetadataCommandDecodeAuthority::new(),
        )
        .map_err(|_| StoreError::MetadataCommandLogConflict {
            node_id,
            pg_id: self.pg_id,
            cluster_epoch,
            log_index: slot.id.log_index().get(),
        })?;
        if command.id() != slot.id || command.checksum_crc64() != slot.command_checksum {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                log_index: slot.id.log_index().get(),
            });
        }
        if let Some(scope_bucket) = slot.scope_bucket {
            if !Self::pending_slot_scope_matches_command(&scope_bucket, &command) {
                return Err(StoreError::MetadataCommandPendingConflict {
                    pg_id: self.pg_id,
                    cluster_epoch,
                    existing_log_index: slot.id.log_index().get(),
                    candidate_log_index: command.id().log_index().get(),
                });
            }
        }
        Ok(Some(command))
    }

    pub(crate) fn try_insert_pending_metadata_command_slot(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
        scope_bucket: Option<&BucketName>,
    ) -> Result<(), StoreError> {
        self.try_insert_pending_metadata_command_slot_classified(node_id, command, scope_bucket)
            .map_err(PendingMetadataCommandSlotInsertError::into_source)
    }

    pub(crate) fn try_insert_pending_metadata_command_slot_classified(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
        scope_bucket: Option<&BucketName>,
    ) -> Result<(), PendingMetadataCommandSlotInsertError> {
        if command.payload().is_bucket_control_pending_slot_payload() {
            return Err(PendingMetadataCommandSlotInsertError::definitive(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "insert generic pending metadata command",
                },
            ));
        }
        if command.id().pg_id().get() != self.pg_id {
            return Err(PendingMetadataCommandSlotInsertError::definitive(
                StoreError::MetadataCommandWrongPg {
                    node_id,
                    command_pg_id: command.id().pg_id().get(),
                    target_pg_id: self.pg_id,
                    cluster_epoch: command.id().cluster_epoch(),
                },
            ));
        }
        if self
            .load_metadata_command_log_entry(
                "check pending metadata command slot log entry",
                command.id().cluster_epoch(),
                command.id().pg_id(),
                command.id().log_index(),
            )
            .map_err(PendingMetadataCommandSlotInsertError::definitive)?
            .is_some()
            || self
                .load_metadata_command_terminal_receipt(
                    "check compacted pending metadata command terminal receipt",
                    command.id().cluster_epoch(),
                    command.id().pg_id(),
                    command.id().log_index(),
                )
                .map_err(PendingMetadataCommandSlotInsertError::definitive)?
                .is_some()
            || self
                .metadata_command_index_is_checkpoint_covered(command.id())
                .map_err(PendingMetadataCommandSlotInsertError::definitive)?
        {
            return Err(PendingMetadataCommandSlotInsertError::definitive(
                self.metadata_command_log_conflict(node_id, command),
            ));
        }
        let (reference_count, pages) = self
            .encode_pending_placed_segment_reference_pages(command)
            .map_err(PendingMetadataCommandSlotInsertError::definitive)?;
        self.with_pending_slot_insert_transaction(|| {
            // Drain clearing performs the inverse check under the same SQLite
            // write serialization: either the mark slot protects its drain,
            // or a clear that linearized first makes this insert impossible.
            self.require_mark_bucket_deleting_install_authorization(
                command,
                scope_bucket,
                crate::clock::current_time_millis(),
            )?;
            let inserted = self.execute_cached(
                "INSERT INTO metadata_command_pending_slot \
                 (singleton, cluster_epoch, pg_id, log_index, command_checksum, command_bytes, \
                  placed_segment_reference_count, scope_bucket) \
                 VALUES (0, ?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                 ON CONFLICT(singleton) DO NOTHING",
                params![
                    command.id().cluster_epoch().get() as i64,
                    command.id().pg_id().get() as i64,
                    command.id().log_index().get() as i64,
                    command.checksum_crc64() as i64,
                    command.command_bytes(),
                    reference_count as i64,
                    scope_bucket.map(BucketName::as_str),
                ],
                "insert metadata command pending slot",
            )?;
            if inserted == 1 {
                return self.replace_pending_placed_reference_pages(reference_count, &pages);
            }
            let existing = self
                .pending_metadata_command_slot(node_id, command.id().cluster_epoch())?
                .ok_or_else(|| StoreError::MetadataCommandPendingConflict {
                    pg_id: self.pg_id,
                    cluster_epoch: command.id().cluster_epoch(),
                    existing_log_index: 0,
                    candidate_log_index: command.id().log_index().get(),
                })?;
            if existing.command_checksum == command.checksum_crc64()
                && existing.command_bytes == command.command_bytes()
            {
                return Ok(());
            }
            Err(StoreError::MetadataCommandPendingConflict {
                pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
                existing_log_index: existing.id.log_index().get(),
                candidate_log_index: command.id().log_index().get(),
            })
        })
    }

    fn require_mark_bucket_deleting_install_authorization(
        &self,
        command: &MetadataCommandEnvelope,
        scope_bucket: Option<&BucketName>,
        now: u64,
    ) -> Result<(), StoreError> {
        let MetadataCommandPayload::MarkBucketDeleting(mark) = command.payload() else {
            return Ok(());
        };
        let Some(scope_bucket) = scope_bucket.filter(|bucket| *bucket == mark.bucket_name()) else {
            return Err(StoreError::MetadataCommandContention {
                context: "mark bucket deleting pending install requires its bucket scope",
            });
        };
        let now = i64::try_from(now).map_err(|source| StoreError::Db {
            context: "validate mark bucket deleting drain authorization time",
            source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(source)),
        })?;
        let authorized = self
            .conn
            .query_row(
                "SELECT EXISTS( \
                     SELECT 1 \
                     FROM bucket_write_drains AS drain \
                     JOIN bucket_delete_attempt_outcomes AS attempt \
                       ON attempt.bucket_name = drain.bucket_name \
                      AND attempt.drain_id = drain.drain_id \
                      AND attempt.cluster_epoch = drain.cluster_epoch \
                      AND attempt.bucket_execution_generation = \
                          drain.bucket_execution_generation \
                     WHERE drain.bucket_name = ?1 \
                       AND drain.lease_deadline > ?2 \
                       AND ( \
                           (attempt.outcome = ?3 AND attempt.phase = ?4) \
                           OR (attempt.outcome = ?5 AND attempt.phase = ?6) \
                       ) \
                 )",
                params![
                    scope_bucket.as_str(),
                    now,
                    BucketDeleteAttemptOutcomeKind::Retryable as u8,
                    BucketDeleteAttemptPhase::FinalVisibilityProven as u8,
                    BucketDeleteAttemptOutcomeKind::MarkDeleting as u8,
                    BucketDeleteAttemptPhase::MarkDeleting as u8,
                ],
                |row| row.get::<_, bool>(0),
            )
            .map_err(|source| StoreError::Db {
                context: "validate mark bucket deleting drain authorization",
                source: source.into(),
            })?;
        if !authorized {
            return Err(StoreError::MetadataCommandContention {
                context: "mark bucket deleting drain authorization changed before pending install",
            });
        }
        Ok(())
    }

    pub(crate) fn try_insert_bucket_control_pending_metadata_command_slot(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
    ) -> Result<bool, StoreError> {
        if !command.payload().is_bucket_control_pending_slot_payload() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "insert bucket-control pending metadata command",
            });
        }
        if command.id().pg_id().get() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id,
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
            });
        }
        if self
            .load_metadata_command_log_entry(
                "check bucket control pending metadata command slot log entry",
                command.id().cluster_epoch(),
                command.id().pg_id(),
                command.id().log_index(),
            )?
            .is_some()
            || self
                .load_metadata_command_terminal_receipt(
                    "check compacted bucket-control pending metadata command receipt",
                    command.id().cluster_epoch(),
                    command.id().pg_id(),
                    command.id().log_index(),
                )?
                .is_some()
            || self.metadata_command_index_is_checkpoint_covered(command.id())?
        {
            return Err(self.metadata_command_log_conflict(node_id, command));
        }

        let (reference_count, pages) =
            self.encode_pending_placed_segment_reference_pages(command)?;
        self.with_pending_slot_transaction(|| {
            let inserted = self.execute_cached(
                "INSERT INTO metadata_command_pending_slot \
                 (singleton, cluster_epoch, pg_id, log_index, command_checksum, command_bytes, \
                  placed_segment_reference_count, scope_bucket) \
                 SELECT 0, ?1, ?2, ?3, ?4, ?5, ?6, ?7 \
                 WHERE NOT EXISTS ( \
                     SELECT 1 FROM bucket_write_drains WHERE bucket_name = ?8 \
                 ) \
                 ON CONFLICT(singleton) DO NOTHING",
                params![
                    command.id().cluster_epoch().get() as i64,
                    command.id().pg_id().get() as i64,
                    command.id().log_index().get() as i64,
                    command.checksum_crc64() as i64,
                    command.command_bytes(),
                    reference_count as i64,
                    bucket.as_str(),
                    bucket.as_str(),
                ],
                "insert bucket control pending metadata command slot",
            )?;
            if inserted == 1 {
                self.replace_pending_placed_reference_pages(reference_count, &pages)?;
            }
            Ok(inserted == 1)
        })
    }

    pub(crate) fn remove_pending_metadata_command_slot(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        if self.conn.is_autocommit() {
            self.conn
                .execute_batch("BEGIN IMMEDIATE")
                .map_err(|source| StoreError::Db {
                    context: "remove pending metadata command slot (begin txn)",
                    source: source.into(),
                })?;
            let result = self.remove_pending_metadata_command_slot_inner(node_id, command);
            match result {
                Ok(removed) => {
                    if let Err(source) = self.conn.execute_batch("COMMIT") {
                        let _ = self.conn.execute_batch("ROLLBACK");
                        return Err(StoreError::Db {
                            context: "remove pending metadata command slot (commit txn)",
                            source: source.into(),
                        });
                    }
                    Ok(removed)
                }
                Err(error) => {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    Err(error)
                }
            }
        } else {
            self.remove_pending_metadata_command_slot_inner(node_id, command)
        }
    }

    fn remove_pending_metadata_command_slot_inner(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        if command.id().pg_id().get() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id,
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
            });
        }
        let Some(slot) =
            self.pending_metadata_command_slot(node_id, command.id().cluster_epoch())?
        else {
            return Ok(false);
        };
        if slot.id != command.id()
            || slot.command_checksum != command.checksum_crc64()
            || slot.command_bytes != command.command_bytes()
        {
            return Ok(false);
        }
        if let Some(scope_bucket) = &slot.scope_bucket {
            if !Self::pending_slot_scope_matches_command(scope_bucket, command) {
                return Err(StoreError::MetadataCommandPendingConflict {
                    pg_id: self.pg_id,
                    cluster_epoch: command.id().cluster_epoch(),
                    existing_log_index: slot.id.log_index().get(),
                    candidate_log_index: command.id().log_index().get(),
                });
            }
        }
        let Some(entry) = self.load_metadata_command_log_entry(
            "load terminal metadata command log entry before pending slot removal",
            command.id().cluster_epoch(),
            command.id().pg_id(),
            command.id().log_index(),
        )?
        else {
            return Err(StoreError::MetadataCommandTerminalEntryPending {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
                log_index: command.id().log_index().get(),
            });
        };
        if !self.pending_slot_terminal_entry_matches(node_id, &slot, &entry)? {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
                log_index: command.id().log_index().get(),
            });
        }
        self.remove_pending_metadata_command_slot_exact(node_id, &slot)?;
        Ok(true)
    }

    pub(crate) fn replace_pending_metadata_command_slot_for_reissue(
        &self,
        node_id: u32,
        expected: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        scope_bucket: Option<&BucketName>,
    ) -> Result<bool, PendingMetadataCommandSlotReplaceError> {
        if !replacement
            .payload()
            .is_ordinary_pending_slot_reissue_of(expected.payload())
        {
            return Err(PendingMetadataCommandSlotReplaceError::definitive(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "replace ordinary pending metadata command for reissue",
                },
            ));
        }
        self.replace_pending_metadata_command_slot_for_reissue_inner(
            node_id,
            expected,
            replacement,
            scope_bucket,
        )
    }

    pub(crate) fn replace_pending_metadata_command_slot_for_recovery(
        &self,
        node_id: u32,
        authorized_source: &MetadataCommandEnvelope,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        expected: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        scope_bucket: Option<&BucketName>,
    ) -> Result<bool, PendingMetadataCommandSlotReplaceError> {
        let cleanup_chain_matches = match abandoned_source {
            None => true,
            Some(abandoned_source) if expected.payload() == authorized_source.payload() => {
                expected == abandoned_source
            }
            Some(abandoned_source) => {
                expected.payload() == replacement.payload()
                    && expected.id().log_index() > abandoned_source.id().log_index()
            }
        };
        let relationship_is_valid =
            crate::metadata_command::validate_metadata_command_recovery_certificate(
                authorized_source,
                abandoned_source,
                replacement,
            )
            .is_ok()
                && authorized_source.id().cluster_epoch() == expected.id().cluster_epoch()
                && authorized_source.id().pg_id() == expected.id().pg_id()
                && abandoned_source.is_none_or(|abandoned_source| {
                    abandoned_source.id().cluster_epoch() == expected.id().cluster_epoch()
                        && abandoned_source.id().pg_id() == expected.id().pg_id()
                })
                && expected
                    .payload()
                    .is_authorized_recovery_derivative_of(authorized_source.payload())
                && expected.id().log_index() >= authorized_source.id().log_index()
                && replacement.id().log_index() > expected.id().log_index()
                && cleanup_chain_matches;
        if !relationship_is_valid {
            return Err(PendingMetadataCommandSlotReplaceError::definitive(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "replace certified recovery pending metadata command",
                },
            ));
        }
        self.replace_pending_metadata_command_slot_for_reissue_inner(
            node_id,
            expected,
            replacement,
            scope_bucket,
        )
    }

    fn replace_pending_metadata_command_slot_for_reissue_inner(
        &self,
        node_id: u32,
        expected: &MetadataCommandEnvelope,
        replacement: &MetadataCommandEnvelope,
        scope_bucket: Option<&BucketName>,
    ) -> Result<bool, PendingMetadataCommandSlotReplaceError> {
        let Some(scope_bucket) = scope_bucket.filter(|scope| *scope == replacement.bucket_name())
        else {
            return Err(PendingMetadataCommandSlotReplaceError::definitive(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "replace pending metadata command bucket scope",
                },
            ));
        };
        if expected.id().pg_id().get() != self.pg_id {
            return Err(PendingMetadataCommandSlotReplaceError::definitive(
                StoreError::MetadataCommandWrongPg {
                    node_id,
                    command_pg_id: expected.id().pg_id().get(),
                    target_pg_id: self.pg_id,
                    cluster_epoch: expected.id().cluster_epoch(),
                },
            ));
        }
        if replacement.id().pg_id().get() != self.pg_id {
            return Err(PendingMetadataCommandSlotReplaceError::definitive(
                StoreError::MetadataCommandWrongPg {
                    node_id,
                    command_pg_id: replacement.id().pg_id().get(),
                    target_pg_id: self.pg_id,
                    cluster_epoch: replacement.id().cluster_epoch(),
                },
            ));
        }
        let durable_log_tip = self
            .max_metadata_command_log_index(expected.id().cluster_epoch())
            .map_err(PendingMetadataCommandSlotReplaceError::definitive)?;
        let expected_replacement_index = durable_log_tip
            .max(expected.id().log_index().get())
            .checked_add(1)
            .ok_or_else(|| {
                PendingMetadataCommandSlotReplaceError::definitive(
                    StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id: self.pg_id,
                        cluster_epoch: expected.id().cluster_epoch(),
                        log_index: u64::MAX,
                    },
                )
            })?;
        if replacement.id().cluster_epoch() != expected.id().cluster_epoch()
            || replacement.id().log_index().get() != expected_replacement_index
        {
            return Err(PendingMetadataCommandSlotReplaceError::definitive(
                StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch: expected.id().cluster_epoch(),
                    log_index: replacement.id().log_index().get(),
                },
            ));
        }
        let Some(slot) = self
            .pending_metadata_command_slot(node_id, expected.id().cluster_epoch())
            .map_err(PendingMetadataCommandSlotReplaceError::definitive)?
        else {
            return Ok(false);
        };
        if slot.scope_bucket.as_ref() != Some(scope_bucket) {
            return Err(PendingMetadataCommandSlotReplaceError::definitive(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "replace pending metadata command existing bucket scope",
                },
            ));
        }
        if slot.id == replacement.id()
            && slot.command_checksum == replacement.checksum_crc64()
            && slot.command_bytes == replacement.command_bytes()
        {
            return Ok(true);
        }
        if slot.id != expected.id()
            || slot.command_checksum != expected.checksum_crc64()
            || slot.command_bytes != expected.command_bytes()
        {
            return Ok(false);
        }
        if slot.publication_started {
            return Ok(false);
        }
        let expected_bytes = expected.command_bytes();
        let replacement_bytes = replacement.command_bytes();
        let (reference_count, pages) = self
            .encode_pending_placed_segment_reference_pages(replacement)
            .map_err(PendingMetadataCommandSlotReplaceError::definitive)?;
        let replaced = self
            .with_pending_slot_transaction(|| {
                let updated = self.execute_cached(
                    "UPDATE metadata_command_pending_slot \
                 SET cluster_epoch = ?1, pg_id = ?2, log_index = ?3, \
                     command_checksum = ?4, command_bytes = ?5, \
                     publication_started = 0, \
                     placed_segment_reference_count = ?6 \
                 WHERE singleton = 0 \
                   AND cluster_epoch = ?7 \
                   AND pg_id = ?8 \
                   AND log_index = ?9 \
                   AND command_checksum = ?10 \
                   AND command_bytes = ?11 \
                   AND scope_bucket = ?12",
                    params![
                        replacement.id().cluster_epoch().get() as i64,
                        replacement.id().pg_id().get() as i64,
                        replacement.id().log_index().get() as i64,
                        replacement.checksum_crc64() as i64,
                        replacement_bytes,
                        reference_count as i64,
                        expected.id().cluster_epoch().get() as i64,
                        expected.id().pg_id().get() as i64,
                        expected.id().log_index().get() as i64,
                        expected.checksum_crc64() as i64,
                        expected_bytes,
                        scope_bucket.as_str(),
                    ],
                    "replace metadata command pending slot for reissue",
                )?;
                if updated == 1 {
                    self.replace_pending_placed_reference_pages(reference_count, &pages)?;
                }
                Ok(updated == 1)
            })
            .map_err(PendingMetadataCommandSlotReplaceError::may_have_applied)?;
        #[cfg(test)]
        if replaced
            && self
                .fail_next_pending_slot_replace_after_commit
                .swap(false, Ordering::Relaxed)
        {
            return Err(PendingMetadataCommandSlotReplaceError::may_have_applied(
                StoreError::Io {
                    context: "injected pending metadata command slot replacement response failure",
                    source: std::io::Error::other(
                        "injected failure after pending slot replacement commit",
                    ),
                },
            ));
        }
        Ok(replaced)
    }

    pub(crate) fn validate_metadata_command_replay_state(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.validate_metadata_command_replay_state_with_pending_cleanup(
            node_id,
            cluster_epoch,
            PendingMetadataCommandSlotCleanup::CleanTerminal,
        )
    }

    pub(crate) fn validate_metadata_command_replay_state_preserving_pending_slot(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.validate_metadata_command_replay_state_with_pending_cleanup(
            node_id,
            cluster_epoch,
            PendingMetadataCommandSlotCleanup::PreserveTerminal,
        )
    }

    /// Recover this PG store after open before it serves any request.
    ///
    /// This is the recovery boundary documented in `guides/storage-cluster-invariants.md`.
    /// The owning node id comes from the caller (which owns node identity); the
    /// recovery epoch is read from the store's own replica state and must never
    /// be supplied externally, since orphan detection compares the pending slot's
    /// epoch against that stored epoch.
    ///
    /// Ordering is load-bearing: older-epoch orphan cleanup runs before replay
    /// validation, because replay validation reads the pending slot through the
    /// epoch-checked path and would otherwise reject that locally cleanable
    /// orphan before cleanup could run.
    ///
    /// This local recovery boundary deliberately preserves same-epoch terminal
    /// pending slots. A terminal slot proves only that this replica recorded the
    /// command; it does not prove the acting set converged. Callers that have
    /// topology context may use [`validate_metadata_command_replay_state`] after
    /// proving convergence to remove terminal slots.
    pub(crate) fn recover(
        &self,
        ctx: super::PgStoreRecoveryContext,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let node_id = ctx.node_id().as_u32();
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::recover",
            "pg_id={} node_id={}",
            self.pg_id,
            node_id,
        );
        self.recover_clean_orphan_pending_command_slots(ctx)?;
        // The recovery epoch is the store's own replica-state epoch. An external
        // authority/config epoch must not be used: orphan detection compares the
        // slot's epoch against this stored epoch.
        let state = self.metadata_command_replica_state()?;
        let stored_epoch = state.cluster_epoch;
        // Replay validation preserves same-epoch terminal pending slots here:
        // local recovery has no acting-set evidence, so erasing the slot could
        // hide partial fanout from cluster-level recovery.
        let state = self.validate_metadata_command_replay_state_preserving_pending_slot(
            node_id,
            stored_epoch,
        )?;
        // Detect cached per-table digest drift that the state-digest check above
        // cannot see (xor-cancelling drift), and refresh the cached digests from
        // materialised rows when found so a drifted trigger-maintained cache
        // cannot poison the next mutation.
        self.repair_metadata_table_digest_cache_drift(node_id)?;
        Ok(state)
    }

    /// Recovery phase A: clean older-epoch orphan pending command slots.
    ///
    /// This must run before any cluster-wide replay validation in clustered open
    /// paths. Cluster-wide validation reads the pending slot through the
    /// epoch-checked path (`pending_metadata_command_slot`), which returns
    /// `StaleMetadataOperation` when the slot's epoch differs from the store
    /// epoch; an older-epoch orphan would therefore reject the whole open before
    /// full recovery could clean it. This phase only removes slots whose epoch
    /// is older than the store's replica-state epoch. Same-epoch primary
    /// pending slots remain available for convergence, and future-epoch slots
    /// fail closed because they may be in-flight first commands for that future
    /// epoch.
    pub(crate) fn recover_clean_orphan_pending_command_slots(
        &self,
        ctx: super::PgStoreRecoveryContext,
    ) -> Result<(), StoreError> {
        self.clean_epoch_mismatched_orphan_pending_metadata_command_slot(ctx)?;
        Ok(())
    }

    /// Detect per-table cached-vs-materialised digest drift and refresh the
    /// cached digests when drift is found.
    ///
    /// Runs after replay validation has already confirmed the stored state
    /// digest against a full materialised recompute, so any mismatch here is
    /// cache-only drift with provably-correct materialised state. The detection
    /// must run before the refresh: `refresh_all_metadata_table_digests`
    /// overwrites the cached rows from materialised rows and would destroy the
    /// drift evidence if run first.
    fn repair_metadata_table_digest_cache_drift(&self, node_id: u32) -> Result<(), StoreError> {
        let cached = self.cached_metadata_table_digests()?;
        let mut drifted_tables: Vec<&'static str> = Vec::new();
        for table in METADATA_DIGEST_TABLES {
            let materialised = self.metadata_table_digest(table)?;
            let Some(&cached_digest) = cached.get(table.name) else {
                // A missing cached row is itself drift the refresh will repair.
                drifted_tables.push(table.name);
                continue;
            };
            if cached_digest != materialised {
                drifted_tables.push(table.name);
            }
        }
        if drifted_tables.is_empty() {
            return Ok(());
        }
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::recover digest cache drift",
            "pg_id={} node_id={} drifted_tables={:?}",
            self.pg_id,
            node_id,
            drifted_tables,
        );
        self.refresh_all_metadata_table_digests()?;
        self.mark_metadata_state_digest_clean()?;
        Ok(())
    }

    pub(crate) fn metadata_command_replica_state_for_heartbeat(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let state = self.metadata_command_replica_state()?;
        if state.cluster_epoch != cluster_epoch {
            return Err(StoreError::StaleMetadataCommand {
                node_id,
                pg_id: self.pg_id,
                command_epoch: state.cluster_epoch,
                current_epoch: cluster_epoch,
            });
        }

        let actual_digest = self.cached_metadata_state_digest()?;
        if state.state_digest.value() != actual_digest {
            return Err(StoreError::MetadataStateDigestMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: state.cluster_epoch,
                expected_digest: state.state_digest.value(),
                actual_digest,
            });
        }

        Ok(state)
    }

    pub fn metadata_command_checkpoint(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandCheckpoint, StoreError> {
        let state =
            self.validate_metadata_command_checkpoint_state_read_only(node_id, cluster_epoch)?;
        let table_blocks = self.metadata_checkpoint_table_blocks()?;
        let table_digests = table_blocks
            .iter()
            .map(|block| MetadataCheckpointTableDigest {
                table_name: block.table_name.clone(),
                row_count: block.row_count,
                row_hash_xor: block.row_hash_xor,
                row_hash_sum: block.row_hash_sum,
                table_digest: block.table_digest,
            })
            .collect::<Vec<_>>();
        let state_digest = self.metadata_state_digest_from_checkpoint_tables(&table_digests);
        if state_digest != state.state_digest.value() {
            return Err(StoreError::MetadataStateDigestMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                expected_digest: state.state_digest.value(),
                actual_digest: state_digest,
            });
        }

        let mut checkpoint = MetadataCommandCheckpoint {
            cluster_epoch,
            pg_id: PgId::new(self.pg_id),
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: CanonicalStateDigest::from_storage(
                state_digest,
                super::MetadataProofStorageIssuer::new(),
            ),
            table_digests,
            table_blocks,
            checkpoint_crc64: 0,
        };
        checkpoint.checkpoint_crc64 = Self::metadata_command_checkpoint_crc64(&checkpoint);
        checkpoint
            .verify()
            .map_err(|error| StoreError::MetadataCheckpointInvalid {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                reason: format!("{error:?}"),
            })?;
        Ok(checkpoint)
    }

    pub fn record_current_metadata_command_checkpoint(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandCheckpoint, StoreError> {
        let checkpoint = self.metadata_command_checkpoint(node_id, cluster_epoch)?;
        self.record_metadata_command_checkpoint(&checkpoint)?;
        Ok(checkpoint)
    }

    pub fn record_metadata_command_checkpoint(
        &self,
        checkpoint: &MetadataCommandCheckpoint,
    ) -> Result<(), StoreError> {
        if checkpoint.pg_id != PgId::new(self.pg_id) {
            return Err(StoreError::MetadataCheckpointInvalid {
                node_id: 0,
                pg_id: self.pg_id,
                cluster_epoch: checkpoint.cluster_epoch,
                reason: format!(
                    "checkpoint PG {} does not match store PG {}",
                    checkpoint.pg_id.get(),
                    self.pg_id
                ),
            });
        }
        checkpoint
            .verify()
            .map_err(|error| StoreError::MetadataCheckpointInvalid {
                node_id: 0,
                pg_id: self.pg_id,
                cluster_epoch: checkpoint.cluster_epoch,
                reason: format!("{error:?}"),
            })?;
        let checkpoint_bytes =
            encode_metadata_command_checkpoint_payload(checkpoint).map_err(|error| {
                StoreError::MetadataCheckpointInvalid {
                    node_id: 0,
                    pg_id: self.pg_id,
                    cluster_epoch: checkpoint.cluster_epoch,
                    reason: error.to_string(),
                }
            })?;
        self.with_metadata_command_checkpoint_transaction(|| {
            self.conn
                .execute(
                    "INSERT INTO metadata_command_checkpoints \
                     (cluster_epoch, pg_id, applied_log_index, applied_log_hash, state_digest, checkpoint_crc64, checkpoint_bytes) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) \
                     ON CONFLICT(cluster_epoch, pg_id, applied_log_index, applied_log_hash, state_digest) \
                     DO UPDATE SET checkpoint_crc64 = excluded.checkpoint_crc64, checkpoint_bytes = excluded.checkpoint_bytes",
                    rusqlite::params![
                        checkpoint.cluster_epoch.get() as i64,
                        checkpoint.pg_id.get() as i64,
                        checkpoint.applied_log_index as i64,
                        checkpoint.applied_log_hash.value() as i64,
                        checkpoint.state_digest.value() as i64,
                        checkpoint.checkpoint_crc64 as i64,
                        checkpoint_bytes,
                    ],
                )
                .map_err(|source| StoreError::Db {
                    context: "record metadata command checkpoint",
                    source: source.into(),
                })?;
            self.prune_metadata_command_checkpoints(
                checkpoint.cluster_epoch,
                METADATA_COMMAND_CHECKPOINT_RETAIN_PER_EPOCH,
            )?;
            self.prune_metadata_command_checkpoint_epochs(
                METADATA_COMMAND_CHECKPOINT_RETAIN_EPOCHS,
            )
        })
    }

    fn prune_metadata_command_checkpoints(
        &self,
        cluster_epoch: ClusterEpoch,
        retain_per_epoch: usize,
    ) -> Result<(), StoreError> {
        let retain_per_epoch = i64::try_from(retain_per_epoch).unwrap_or(i64::MAX);
        self.conn
            .execute(
                "DELETE FROM metadata_command_checkpoints \
                 WHERE cluster_epoch = ?1 AND pg_id = ?2 \
                   AND rowid NOT IN ( \
                     SELECT rowid FROM metadata_command_checkpoints \
                     WHERE cluster_epoch = ?1 AND pg_id = ?2 \
                     ORDER BY applied_log_index DESC, applied_log_hash DESC, state_digest DESC \
                     LIMIT ?3 \
                   )",
                rusqlite::params![
                    cluster_epoch.get() as i64,
                    self.pg_id as i64,
                    retain_per_epoch,
                ],
            )
            .map_err(|source| StoreError::Db {
                context: "prune metadata command checkpoints",
                source: source.into(),
            })?;
        Ok(())
    }

    fn prune_metadata_command_checkpoint_epochs(
        &self,
        retain_epochs: usize,
    ) -> Result<(), StoreError> {
        let retain_epochs = i64::try_from(retain_epochs).unwrap_or(i64::MAX);
        self.conn
            .execute(
                "DELETE FROM metadata_command_checkpoints \
                 WHERE pg_id = ?1 \
                   AND cluster_epoch NOT IN ( \
                     SELECT cluster_epoch FROM ( \
                       SELECT DISTINCT cluster_epoch \
                       FROM metadata_command_checkpoints \
                       WHERE pg_id = ?1 \
                       ORDER BY cluster_epoch DESC \
                       LIMIT ?2 \
                     ) \
                   )",
                rusqlite::params![self.pg_id as i64, retain_epochs],
            )
            .map_err(|source| StoreError::Db {
                context: "prune metadata command checkpoint epochs",
                source: source.into(),
            })?;
        self.conn
            .execute(
                "DELETE FROM metadata_command_terminal_receipts \
                 WHERE pg_id = ?1 \
                   AND cluster_epoch NOT IN ( \
                     SELECT DISTINCT cluster_epoch \
                     FROM metadata_command_checkpoints \
                     WHERE pg_id = ?1 \
                   )",
                rusqlite::params![self.pg_id as i64],
            )
            .map_err(|source| StoreError::Db {
                context: "prune metadata command terminal receipt epochs",
                source: source.into(),
            })?;
        Ok(())
    }

    pub fn metadata_command_checkpoint_candidates(
        &self,
        cluster_epoch: ClusterEpoch,
        max_applied_log_index: u64,
        limit: usize,
    ) -> Result<Vec<MetadataCommandCheckpoint>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows =
            self.metadata_command_checkpoint_candidate_rows(cluster_epoch, max_applied_log_index)?;
        Ok(decode_metadata_command_checkpoint_candidate_rows(
            rows,
            cluster_epoch,
            PgId::new(self.pg_id),
            limit,
        ))
    }

    pub(crate) fn metadata_command_checkpoint_candidate_rows(
        &self,
        cluster_epoch: ClusterEpoch,
        max_applied_log_index: u64,
    ) -> Result<Vec<MetadataCommandCheckpointCandidateRow>, StoreError> {
        let catalogue_rows: i64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM ( \
                   SELECT 1 FROM metadata_command_checkpoints \
                   WHERE cluster_epoch = ?1 AND pg_id = ?2 \
                   LIMIT ?3 \
                 )",
                rusqlite::params![
                    cluster_epoch.get() as i64,
                    self.pg_id as i64,
                    METADATA_COMMAND_CHECKPOINT_CAPTURE_LIMIT as i64,
                ],
                |row| row.get(0),
            )
            .map_err(|source| StoreError::Db {
                context: "count bounded metadata command checkpoint catalogue",
                source: source.into(),
            })?;
        if catalogue_rows > METADATA_COMMAND_CHECKPOINT_RETAIN_PER_EPOCH as i64 {
            return Err(StoreError::MetadataCheckpointInvalid {
                node_id: 0,
                pg_id: self.pg_id,
                cluster_epoch,
                reason: format!(
                    "checkpoint catalogue exceeds retained-row limit {}",
                    METADATA_COMMAND_CHECKPOINT_RETAIN_PER_EPOCH
                ),
            });
        }

        let max_applied_log_index_sql = i64::try_from(max_applied_log_index).unwrap_or(i64::MAX);
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT applied_log_index, applied_log_hash, state_digest, checkpoint_crc64, checkpoint_bytes \
                 FROM metadata_command_checkpoints \
                 WHERE cluster_epoch = ?1 AND pg_id = ?2 AND applied_log_index <= ?3 \
                 ORDER BY applied_log_index DESC, applied_log_hash DESC, state_digest DESC \
                 LIMIT ?4",
            )
            .map_err(|source| StoreError::Db {
                context: "prepare metadata command checkpoint candidate scan",
                source: source.into(),
            })?;
        let rows = stmt
            .query_map(
                rusqlite::params![
                    cluster_epoch.get() as i64,
                    self.pg_id as i64,
                    max_applied_log_index_sql,
                    METADATA_COMMAND_CHECKPOINT_RETAIN_PER_EPOCH as i64,
                ],
                |row| {
                    Ok(MetadataCommandCheckpointCandidateRow {
                        applied_log_index: row.get(0)?,
                        applied_log_hash: row.get(1)?,
                        state_digest: row.get(2)?,
                        checkpoint_crc64: row.get(3)?,
                        checkpoint_bytes: row.get(4)?,
                    })
                },
            )
            .map_err(|source| StoreError::Db {
                context: "scan metadata command checkpoint candidates",
                source: source.into(),
            })?;
        let rows = rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| StoreError::Db {
                context: "decode metadata command checkpoint candidate row",
                source: source.into(),
            })?;
        Ok(rows)
    }

    fn validate_metadata_command_checkpoint_state_read_only(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let state = self.metadata_command_replica_state()?;
        if state.cluster_epoch != cluster_epoch {
            return Err(StoreError::StaleMetadataCommand {
                node_id,
                pg_id: self.pg_id,
                command_epoch: state.cluster_epoch,
                current_epoch: cluster_epoch,
            });
        }
        if self
            .pending_metadata_command_slot(node_id, cluster_epoch)?
            .is_some()
        {
            return Err(StoreError::MetadataCommandContention {
                context: "export metadata command checkpoint with pending command",
            });
        }

        self.validate_metadata_command_log_suffix_from_checkpoint_base(
            node_id,
            cluster_epoch,
            &state,
            "load metadata command log entry for checkpoint validation",
            false,
        )?;
        if let Some(tail_log_index) =
            self.metadata_command_log_min_tail_after(cluster_epoch, state.applied_log_index)?
        {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                log_index: tail_log_index,
            });
        }
        let actual_digest = self.metadata_state_digest()?;
        if state.state_digest.value() != actual_digest {
            return Err(StoreError::MetadataStateDigestMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: state.cluster_epoch,
                expected_digest: state.state_digest.value(),
                actual_digest,
            });
        }
        Ok(state)
    }

    fn metadata_command_log_validation_base(
        &self,
        cluster_epoch: ClusterEpoch,
        applied_log_index: u64,
    ) -> Result<MetadataCommandLogValidationBase, StoreError> {
        let checkpoint = self
            .metadata_command_checkpoint_candidates(cluster_epoch, applied_log_index, 1)?
            .into_iter()
            .next();
        let Some(checkpoint) = checkpoint else {
            return Ok(MetadataCommandLogValidationBase {
                applied_log_index: 0,
                applied_log_hash: 0,
                compactable_before: None,
            });
        };
        Ok(MetadataCommandLogValidationBase {
            applied_log_index: checkpoint.applied_log_index,
            applied_log_hash: checkpoint.applied_log_hash.value(),
            compactable_before: checkpoint.applied_log_index.checked_add(1),
        })
    }

    fn validate_metadata_command_log_suffix_from_checkpoint_base(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        state: &MetadataCommandReplicaState,
        load_context: &'static str,
        count_replay_validation_entries: bool,
    ) -> Result<(), StoreError> {
        let pg_id = PgId::new(self.pg_id);
        let base =
            self.metadata_command_log_validation_base(cluster_epoch, state.applied_log_index)?;
        let mut applied_log_hash = base.applied_log_hash;
        if base.applied_log_index < state.applied_log_index {
            let mut raw_log_index = base
                .applied_log_index
                .checked_add(1)
                .expect("checkpoint base is lower than applied index");
            while raw_log_index <= state.applied_log_index {
                let log_index = MetadataCommandLogIndex::new(raw_log_index)
                    .expect("applied metadata command log index is non-zero");
                if count_replay_validation_entries {
                    #[cfg(test)]
                    self.metadata_command_log_replay_validation_entries
                        .fetch_add(1, Ordering::Relaxed);
                }
                let Some(entry) = self.load_metadata_command_log_entry(
                    load_context,
                    cluster_epoch,
                    pg_id,
                    log_index,
                )?
                else {
                    return Err(StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id: self.pg_id,
                        cluster_epoch,
                        log_index: raw_log_index,
                    });
                };
                self.verify_metadata_command_log_entry(
                    node_id,
                    cluster_epoch,
                    pg_id,
                    log_index,
                    &entry,
                )?;
                let expected_log_hash = metadata_command_log_hash(
                    cluster_epoch,
                    pg_id,
                    log_index,
                    applied_log_hash,
                    entry.command_checksum,
                );
                match (entry.previous_log_hash, entry.log_hash) {
                    (Some(previous_log_hash), Some(log_hash))
                        if previous_log_hash == applied_log_hash
                            && log_hash == expected_log_hash.value() => {}
                    (previous_log_hash, log_hash) => {
                        return Err(StoreError::MetadataCommandLogHashMismatch {
                            node_id,
                            pg_id: self.pg_id,
                            cluster_epoch,
                            log_index: raw_log_index,
                            expected_previous_log_hash: applied_log_hash,
                            actual_previous_log_hash: previous_log_hash.unwrap_or_default(),
                            expected_log_hash: expected_log_hash.value(),
                            actual_log_hash: log_hash.unwrap_or_default(),
                        });
                    }
                }
                applied_log_hash = expected_log_hash.value();
                if raw_log_index == state.applied_log_index {
                    break;
                }
                raw_log_index = raw_log_index
                    .checked_add(1)
                    .expect("applied metadata command log index can advance");
            }
        }
        if applied_log_hash != state.applied_log_hash.value() {
            return Err(StoreError::MetadataCommandLogHashMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                log_index: state.applied_log_index,
                expected_previous_log_hash: applied_log_hash,
                actual_previous_log_hash: state.applied_log_hash.value(),
                expected_log_hash: applied_log_hash,
                actual_log_hash: state.applied_log_hash.value(),
            });
        }
        Ok(())
    }

    fn validate_metadata_command_replay_state_with_pending_cleanup(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        pending_cleanup: PendingMetadataCommandSlotCleanup,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let mut state = self.metadata_command_replica_state()?;
        if state.cluster_epoch != cluster_epoch {
            return Err(StoreError::StaleMetadataCommand {
                node_id,
                pg_id: self.pg_id,
                command_epoch: state.cluster_epoch,
                current_epoch: cluster_epoch,
            });
        }
        if let Some(slot) = self.pending_metadata_command_slot(node_id, cluster_epoch)? {
            match self.validate_pending_metadata_command_slot_relation(node_id, &state, &slot)? {
                PendingMetadataCommandSlotAction::Unresolved => {}
                PendingMetadataCommandSlotAction::CleanTerminal => {
                    if pending_cleanup == PendingMetadataCommandSlotCleanup::CleanTerminal {
                        self.remove_pending_metadata_command_slot_exact(node_id, &slot)?;
                    }
                }
                PendingMetadataCommandSlotAction::AdvanceAbandonedThenClean => {
                    state = self.advance_abandoned_metadata_command_log_tail(
                        node_id,
                        cluster_epoch,
                        state,
                    )?;
                    self.remove_pending_metadata_command_slot_exact(node_id, &slot)?;
                }
            }
        }
        state = self.advance_abandoned_metadata_command_log_tail(node_id, cluster_epoch, state)?;
        self.validate_metadata_command_log_suffix_from_checkpoint_base(
            node_id,
            cluster_epoch,
            &state,
            "load metadata command log entry for replay validation",
            true,
        )?;
        let actual_digest = self.metadata_state_digest()?;
        if state.state_digest.value() != actual_digest {
            return Err(StoreError::MetadataStateDigestMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: state.cluster_epoch,
                expected_digest: state.state_digest.value(),
                actual_digest,
            });
        }
        self.mark_metadata_state_digest_clean()?;
        Ok(state)
    }

    fn advance_abandoned_metadata_command_log_tail(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        state: MetadataCommandReplicaState,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg_id = PgId::new(self.pg_id);
        let mut applied_log_index = state.applied_log_index;
        let mut applied_log_hash = state.applied_log_hash.value();
        let mut advanced = false;

        while let Some(next_log_index) = applied_log_index.checked_add(1) {
            let log_index = MetadataCommandLogIndex::new(next_log_index)
                .expect("metadata command log index is non-zero");
            let Some(entry) = self.load_metadata_command_log_entry(
                "load next abandoned metadata command log entry during replay validation",
                cluster_epoch,
                pg_id,
                log_index,
            )?
            else {
                break;
            };
            self.verify_metadata_command_log_entry(
                node_id,
                cluster_epoch,
                pg_id,
                log_index,
                &entry,
            )?;
            if !entry.abandoned {
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    log_index: next_log_index,
                });
            }

            let expected_log_hash = metadata_command_log_hash(
                cluster_epoch,
                pg_id,
                log_index,
                applied_log_hash,
                entry.command_checksum,
            );
            match (entry.previous_log_hash, entry.log_hash) {
                (None, None) => {
                    let updated = self.execute_cached(
                        "UPDATE metadata_command_log \
                         SET previous_log_hash = ?1, log_hash = ?2 \
                         WHERE cluster_epoch = ?3 AND pg_id = ?4 AND log_index = ?5 \
                           AND previous_log_hash IS NULL AND log_hash IS NULL",
                        params![
                            applied_log_hash as i64,
                            expected_log_hash.value() as i64,
                            cluster_epoch.get() as i64,
                            self.pg_id as i64,
                            next_log_index as i64,
                        ],
                        "recover abandoned metadata command log hash",
                    )?;
                    if updated != 1 {
                        return Err(StoreError::MetadataCommandLogConflict {
                            node_id,
                            pg_id: self.pg_id,
                            cluster_epoch,
                            log_index: next_log_index,
                        });
                    }
                }
                (Some(previous_log_hash), Some(log_hash))
                    if previous_log_hash == applied_log_hash
                        && log_hash == expected_log_hash.value() => {}
                (previous_log_hash, log_hash) => {
                    return Err(StoreError::MetadataCommandLogHashMismatch {
                        node_id,
                        pg_id: self.pg_id,
                        cluster_epoch,
                        log_index: next_log_index,
                        expected_previous_log_hash: applied_log_hash,
                        actual_previous_log_hash: previous_log_hash.unwrap_or_default(),
                        expected_log_hash: expected_log_hash.value(),
                        actual_log_hash: log_hash.unwrap_or_default(),
                    });
                }
            }

            applied_log_index = next_log_index;
            applied_log_hash = expected_log_hash.value();
            advanced = true;
        }

        if !advanced {
            return Ok(state);
        }
        Ok(self
            .update_metadata_command_replica_state_preserving_digest(
                cluster_epoch,
                applied_log_index,
                applied_log_hash,
                state.state_digest,
            )?
            .state)
    }

    fn load_metadata_command_log_entry(
        &self,
        context: &'static str,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        log_index: MetadataCommandLogIndex,
    ) -> Result<Option<MetadataCommandLogEntry>, StoreError> {
        self.query_row_cached_optional(
            "SELECT command_checksum, command_bytes, abandoned, previous_log_hash, log_hash, pre_state_digest, post_state_digest \
             FROM metadata_command_log \
             WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            params![
                cluster_epoch.get() as i64,
                pg_id.get() as i64,
                log_index.get() as i64,
            ],
            context,
            |row| {
                Ok(MetadataCommandLogEntry {
                    command_checksum: row.get::<_, i64>(0)? as u64,
                    command_bytes: row.get::<_, Vec<u8>>(1)?,
                    abandoned: row.get::<_, i64>(2)? != 0,
                    previous_log_hash: row.get::<_, Option<i64>>(3)?.map(|value| value as u64),
                    log_hash: row.get::<_, Option<i64>>(4)?.map(|value| value as u64),
                    pre_state_digest: row.get::<_, Option<i64>>(5)?.map(|value| value as u64),
                    post_state_digest: row.get::<_, Option<i64>>(6)?.map(|value| value as u64),
                })
            },
        )
    }

    fn load_metadata_command_terminal_receipt(
        &self,
        context: &'static str,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        log_index: MetadataCommandLogIndex,
    ) -> Result<Option<MetadataCommandTerminalReceipt>, StoreError> {
        self.query_row_cached_optional(
            "SELECT command_checksum, command_sha256, abandoned, previous_log_hash, log_hash \
             FROM metadata_command_terminal_receipts \
             WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3",
            params![
                cluster_epoch.get() as i64,
                pg_id.get() as i64,
                log_index.get() as i64,
            ],
            context,
            |row| {
                let command_sha256 = row.get::<_, Vec<u8>>(1)?;
                let command_sha256 = command_sha256.try_into().map_err(|bytes: Vec<u8>| {
                    rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Blob,
                        format!(
                            "metadata command terminal receipt SHA-256 has invalid length {}",
                            bytes.len()
                        )
                        .into(),
                    )
                })?;
                Ok(MetadataCommandTerminalReceipt {
                    command_checksum: row.get::<_, i64>(0)? as u64,
                    command_sha256,
                    abandoned: row.get::<_, i64>(2)? != 0,
                    previous_log_hash: row.get::<_, i64>(3)? as u64,
                    log_hash: row.get::<_, i64>(4)? as u64,
                })
            },
        )
    }

    fn metadata_command_index_is_checkpoint_covered(
        &self,
        command_id: MetadataCommandId,
    ) -> Result<bool, StoreError> {
        self.conn
            .query_row(
                "SELECT EXISTS( \
                   SELECT 1 FROM metadata_command_checkpoints \
                   WHERE cluster_epoch = ?1 AND pg_id = ?2 AND applied_log_index >= ?3 \
                 )",
                params![
                    command_id.cluster_epoch().get() as i64,
                    command_id.pg_id().get() as i64,
                    command_id.log_index().get() as i64,
                ],
                |row| row.get::<_, i64>(0),
            )
            .map(|covered| covered != 0)
            .map_err(|source| StoreError::Db {
                context: "check metadata command checkpoint-covered index",
                source: source.into(),
            })
    }

    fn verify_metadata_command_terminal_receipt(
        &self,
        node_id: u32,
        command_id: MetadataCommandId,
        receipt: &MetadataCommandTerminalReceipt,
    ) -> Result<(), StoreError> {
        let expected_log_hash = metadata_command_log_hash(
            command_id.cluster_epoch(),
            command_id.pg_id(),
            command_id.log_index(),
            receipt.previous_log_hash,
            receipt.command_checksum,
        )
        .value();
        if receipt.log_hash != expected_log_hash {
            return Err(StoreError::MetadataCommandLogHashMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: command_id.cluster_epoch(),
                log_index: command_id.log_index().get(),
                expected_previous_log_hash: receipt.previous_log_hash,
                actual_previous_log_hash: receipt.previous_log_hash,
                expected_log_hash,
                actual_log_hash: receipt.log_hash,
            });
        }
        Ok(())
    }

    fn metadata_command_terminal_receipt_matches(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
        abandoned: bool,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        let Some(receipt) = self.load_metadata_command_terminal_receipt(
            "load compacted metadata command terminal receipt",
            command.id().cluster_epoch(),
            command.id().pg_id(),
            command.id().log_index(),
        )?
        else {
            return Ok(None);
        };
        self.verify_metadata_command_terminal_receipt(node_id, command.id(), &receipt)?;
        let (expected_checksum, expected_bytes) = if abandoned {
            (
                command.abandoned_log_checksum_crc64(),
                command.abandoned_log_bytes(),
            )
        } else {
            (command.checksum_crc64(), command.command_bytes())
        };
        if receipt.abandoned != abandoned
            || receipt.command_checksum != expected_checksum
            || receipt.command_sha256 != checksum::sha256::digest(&expected_bytes)
        {
            return Ok(None);
        }
        Ok(Some((receipt.previous_log_hash, receipt.log_hash)))
    }

    fn verify_metadata_command_log_entry(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        log_index: MetadataCommandLogIndex,
        entry: &MetadataCommandLogEntry,
    ) -> Result<(), StoreError> {
        let computed_checksum = checksum::crc64::checksum(&entry.command_bytes);
        if computed_checksum != entry.command_checksum {
            return Err(StoreError::MetadataCommandLogChecksumMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                log_index: log_index.get(),
                stored_checksum: entry.command_checksum,
                computed_checksum,
            });
        }

        let header =
            decode_metadata_command_log_entry_header(&entry.command_bytes).map_err(|_| {
                StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    log_index: log_index.get(),
                }
            })?;
        let expected_kind = if entry.abandoned {
            matches!(header.kind(), MetadataCommandLogEntryKind::Abandoned { .. })
        } else {
            header.kind() == MetadataCommandLogEntryKind::Applied
        };
        if header.id() == MetadataCommandId::new(cluster_epoch, pg_id, log_index) && expected_kind {
            return Ok(());
        }
        Err(StoreError::MetadataCommandLogConflict {
            node_id,
            pg_id: self.pg_id,
            cluster_epoch,
            log_index: log_index.get(),
        })
    }

    fn metadata_command_log_entry_matches(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
        entry: &MetadataCommandLogEntry,
        abandoned: bool,
    ) -> Result<bool, StoreError> {
        self.verify_metadata_command_log_entry(
            node_id,
            command.id().cluster_epoch(),
            command.id().pg_id(),
            command.id().log_index(),
            entry,
        )?;

        if entry.abandoned != abandoned {
            return Ok(false);
        }
        let (expected_checksum, expected_bytes) = if abandoned {
            (
                command.abandoned_log_checksum_crc64(),
                command.abandoned_log_bytes(),
            )
        } else {
            (command.checksum_crc64(), command.command_bytes())
        };
        Ok(entry.command_checksum == expected_checksum && entry.command_bytes == expected_bytes)
    }

    fn metadata_command_log_conflict(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> StoreError {
        StoreError::MetadataCommandLogConflict {
            node_id,
            pg_id: self.pg_id,
            cluster_epoch: command.id().cluster_epoch(),
            log_index: command.id().log_index().get(),
        }
    }

    fn pending_slot_terminal_entry_matches(
        &self,
        node_id: u32,
        slot: &PendingMetadataCommandSlot,
        entry: &MetadataCommandLogEntry,
    ) -> Result<bool, StoreError> {
        self.verify_metadata_command_log_entry(
            node_id,
            slot.id.cluster_epoch(),
            slot.id.pg_id(),
            slot.id.log_index(),
            entry,
        )?;
        if entry.abandoned {
            let expected_bytes = abandoned_command_log_bytes(slot.id, slot.command_checksum);
            return Ok(
                entry.command_checksum == checksum::crc64::checksum(&expected_bytes)
                    && entry.command_bytes == expected_bytes,
            );
        }
        Ok(entry.command_checksum == slot.command_checksum
            && entry.command_bytes == slot.command_bytes)
    }

    fn pending_slot_terminal_receipt_matches(
        &self,
        node_id: u32,
        slot: &PendingMetadataCommandSlot,
    ) -> Result<bool, StoreError> {
        let Some(receipt) = self.load_metadata_command_terminal_receipt(
            "load compacted terminal receipt for pending slot",
            slot.id.cluster_epoch(),
            slot.id.pg_id(),
            slot.id.log_index(),
        )?
        else {
            return Ok(false);
        };
        self.verify_metadata_command_terminal_receipt(node_id, slot.id, &receipt)?;
        let (expected_checksum, expected_bytes) = if receipt.abandoned {
            let bytes = abandoned_command_log_bytes(slot.id, slot.command_checksum);
            (checksum::crc64::checksum(&bytes), bytes)
        } else {
            (slot.command_checksum, slot.command_bytes.clone())
        };
        Ok(receipt.command_checksum == expected_checksum
            && receipt.command_sha256 == checksum::sha256::digest(&expected_bytes))
    }

    fn validate_pending_metadata_command_slot_relation(
        &self,
        node_id: u32,
        state: &MetadataCommandReplicaState,
        slot: &PendingMetadataCommandSlot,
    ) -> Result<PendingMetadataCommandSlotAction, StoreError> {
        let slot_index = slot.id.log_index().get();
        let next_index = state.applied_log_index.checked_add(1).ok_or(
            StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: state.cluster_epoch,
                log_index: slot_index,
            },
        )?;
        if slot_index == next_index {
            let existing_entry = self.load_metadata_command_log_entry(
                "load pending metadata command slot terminal entry",
                slot.id.cluster_epoch(),
                slot.id.pg_id(),
                slot.id.log_index(),
            )?;
            let Some(entry) = existing_entry else {
                return Ok(PendingMetadataCommandSlotAction::Unresolved);
            };
            if entry.abandoned && self.pending_slot_terminal_entry_matches(node_id, slot, &entry)? {
                self.validate_pending_slot_scope_provenance(node_id, state.cluster_epoch, slot)?;
                return Ok(PendingMetadataCommandSlotAction::AdvanceAbandonedThenClean);
            }
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: state.cluster_epoch,
                log_index: slot_index,
            });
        }
        if slot_index > next_index {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: state.cluster_epoch,
                log_index: slot_index,
            });
        }
        let Some(entry) = self.load_metadata_command_log_entry(
            "load terminal metadata command log entry for pending slot",
            slot.id.cluster_epoch(),
            slot.id.pg_id(),
            slot.id.log_index(),
        )?
        else {
            if self.pending_slot_terminal_receipt_matches(node_id, slot)? {
                self.validate_pending_slot_scope_provenance(node_id, state.cluster_epoch, slot)?;
                return Ok(PendingMetadataCommandSlotAction::CleanTerminal);
            }
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: state.cluster_epoch,
                log_index: slot_index,
            });
        };
        if self.pending_slot_terminal_entry_matches(node_id, slot, &entry)? {
            self.validate_pending_slot_scope_provenance(node_id, state.cluster_epoch, slot)?;
            return Ok(PendingMetadataCommandSlotAction::CleanTerminal);
        }
        Err(StoreError::MetadataCommandLogConflict {
            node_id,
            pg_id: self.pg_id,
            cluster_epoch: state.cluster_epoch,
            log_index: slot_index,
        })
    }

    fn validate_pending_slot_scope_provenance(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        slot: &PendingMetadataCommandSlot,
    ) -> Result<(), StoreError> {
        if let Some(scope_bucket) = &slot.scope_bucket {
            if !Self::pending_slot_scope_matches_slot(scope_bucket, slot) {
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    log_index: slot.id.log_index().get(),
                });
            }
        }
        Ok(())
    }

    fn remove_pending_metadata_command_slot_exact(
        &self,
        node_id: u32,
        slot: &PendingMetadataCommandSlot,
    ) -> Result<(), StoreError> {
        let removed = self.execute_cached(
            "DELETE FROM metadata_command_pending_slot \
             WHERE singleton = 0 \
               AND cluster_epoch = ?1 \
               AND pg_id = ?2 \
               AND log_index = ?3 \
               AND command_checksum = ?4 \
               AND command_bytes = ?5 \
               AND scope_bucket IS ?6",
            params![
                slot.id.cluster_epoch().get() as i64,
                slot.id.pg_id().get() as i64,
                slot.id.log_index().get() as i64,
                slot.command_checksum as i64,
                slot.command_bytes.as_slice(),
                slot.scope_bucket.as_ref().map(BucketName::as_str),
            ],
            "remove exact metadata command pending slot",
        )?;
        if removed == 1 {
            return Ok(());
        }
        Err(StoreError::MetadataCommandLogConflict {
            node_id,
            pg_id: self.pg_id,
            cluster_epoch: slot.id.cluster_epoch(),
            log_index: slot.id.log_index().get(),
        })
    }

    fn pending_slot_scope_matches_command(
        scope_bucket: &BucketName,
        command: &MetadataCommandEnvelope,
    ) -> bool {
        command.bucket_name() == scope_bucket
    }

    fn pending_slot_scope_matches_slot(
        scope_bucket: &BucketName,
        slot: &PendingMetadataCommandSlot,
    ) -> bool {
        let Ok(command) = decode_metadata_command_envelope(
            &slot.command_bytes,
            &MetadataCommandDecodeAuthority::new(),
        ) else {
            return false;
        };
        command.id() == slot.id
            && command.checksum_crc64() == slot.command_checksum
            && command.bucket_name() == scope_bucket
    }

    pub(crate) fn metadata_command_acceptance(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        if command.id().pg_id().get() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id,
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
            });
        }
        if let Some((cluster_epoch, expected_digest, actual_digest)) =
            self.metadata_state_digest_mismatch()?
        {
            return Err(StoreError::MetadataStateDigestMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                expected_digest,
                actual_digest,
            });
        }
        let state = self.metadata_command_replica_state()?;
        self.validate_metadata_command_record_position(node_id, command, &state)?;

        let Some(entry) = self.load_metadata_command_log_entry(
            "load metadata command log entry",
            command.id().cluster_epoch(),
            command.id().pg_id(),
            command.id().log_index(),
        )?
        else {
            if !Self::metadata_command_is_next_record_position(command, &state) {
                if self
                    .metadata_command_terminal_receipt_matches(node_id, command, false)?
                    .is_some()
                {
                    return Ok(MetadataCommandAcceptance::AlreadyApplied);
                }
                return Err(self.metadata_command_log_conflict(node_id, command));
            }
            return Ok(MetadataCommandAcceptance::Apply);
        };
        if self.metadata_command_log_entry_matches(node_id, command, &entry, false)? {
            return Ok(MetadataCommandAcceptance::AlreadyApplied);
        }
        Err(self.metadata_command_log_conflict(node_id, command))
    }

    pub(crate) fn metadata_command_abandon_acceptance(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        if command.id().pg_id().get() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id,
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
            });
        }
        if let Some((cluster_epoch, expected_digest, actual_digest)) =
            self.metadata_state_digest_mismatch()?
        {
            return Err(StoreError::MetadataStateDigestMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                expected_digest,
                actual_digest,
            });
        }
        let state = self.metadata_command_replica_state()?;
        self.validate_metadata_command_record_position(node_id, command, &state)?;
        let Some(entry) = self.load_metadata_command_log_entry(
            "load metadata command abandon log entry",
            command.id().cluster_epoch(),
            command.id().pg_id(),
            command.id().log_index(),
        )?
        else {
            if !Self::metadata_command_is_next_record_position(command, &state) {
                if self
                    .metadata_command_terminal_receipt_matches(node_id, command, true)?
                    .is_some()
                {
                    return Ok(MetadataCommandAcceptance::AlreadyApplied);
                }
                return Err(self.metadata_command_log_conflict(node_id, command));
            }
            return Ok(MetadataCommandAcceptance::Apply);
        };
        if self.metadata_command_log_entry_matches(node_id, command, &entry, true)? {
            return Ok(MetadataCommandAcceptance::AlreadyApplied);
        }
        Err(self.metadata_command_log_conflict(node_id, command))
    }

    pub(crate) fn metadata_command_abandoned(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<bool, StoreError> {
        if command.id().pg_id().get() != self.pg_id {
            return Ok(false);
        }
        let Some(entry) = self.load_metadata_command_log_entry(
            "load metadata command abandoned state",
            command.id().cluster_epoch(),
            command.id().pg_id(),
            command.id().log_index(),
        )?
        else {
            return Ok(self
                .metadata_command_terminal_receipt_matches(node_id, command, true)?
                .is_some());
        };
        self.metadata_command_log_entry_matches(node_id, command, &entry, true)
    }

    fn validate_metadata_command_record_position(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
        state: &MetadataCommandReplicaState,
    ) -> Result<(), StoreError> {
        let command_epoch = command.id().cluster_epoch();
        if command_epoch < state.cluster_epoch {
            return Err(StoreError::StaleMetadataCommand {
                node_id,
                pg_id: self.pg_id,
                command_epoch,
                current_epoch: state.cluster_epoch,
            });
        }

        let log_index = command.id().log_index().get();
        let expected_log_index = if command_epoch == state.cluster_epoch {
            state
                .applied_log_index
                .checked_add(1)
                .expect("metadata command log index overflow")
        } else {
            1
        };
        if log_index > expected_log_index {
            return Err(StoreError::MetadataCommandLogGap {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: command_epoch,
                log_index,
                expected_log_index,
            });
        }
        Ok(())
    }

    fn metadata_command_is_next_record_position(
        command: &MetadataCommandEnvelope,
        state: &MetadataCommandReplicaState,
    ) -> bool {
        let command_epoch = command.id().cluster_epoch();
        let log_index = command.id().log_index().get();
        let expected_log_index = if command_epoch == state.cluster_epoch {
            state
                .applied_log_index
                .checked_add(1)
                .expect("metadata command log index overflow")
        } else {
            1
        };
        log_index == expected_log_index
    }

    #[cfg(test)]
    pub(crate) fn record_metadata_command_applied(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.record_metadata_command_applied_inner(node_id, command)
            .map(|result| result.state)
    }

    pub(super) fn record_metadata_command_applied_inner(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandRecordResult, StoreError> {
        if command.id().pg_id().get() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id,
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
            });
        }
        let state = self.metadata_command_replica_state()?;
        self.validate_metadata_command_record_position(node_id, command, &state)?;
        if command.id().cluster_epoch() == state.cluster_epoch
            && command.id().log_index().get() <= state.applied_log_index
            && self
                .load_metadata_command_log_entry(
                    "load already-applied metadata command log entry",
                    command.id().cluster_epoch(),
                    command.id().pg_id(),
                    command.id().log_index(),
                )?
                .is_none()
        {
            if self
                .metadata_command_terminal_receipt_matches(node_id, command, false)?
                .is_some()
            {
                return Ok(MetadataCommandRecordResult {
                    state,
                    digest_revision: self.metadata_digest_revision()?,
                });
            }
            return Err(self.metadata_command_log_conflict(node_id, command));
        }
        let command_bytes = command.command_bytes();
        let inserted = self.execute_cached(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash, pre_state_digest, post_state_digest) \
             VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL, NULL, NULL, NULL) \
             ON CONFLICT(cluster_epoch, pg_id, log_index) DO NOTHING",
            params![
                command.id().cluster_epoch().get() as i64,
                command.id().pg_id().get() as i64,
                command.id().log_index().get() as i64,
                command.checksum_crc64() as i64,
                command_bytes,
            ],
            "record metadata command log entry",
        )?;

        if inserted == 0 {
            let entry = self
                .load_metadata_command_log_entry(
                    "load recorded metadata command log entry",
                    command.id().cluster_epoch(),
                    command.id().pg_id(),
                    command.id().log_index(),
                )?
                .expect("metadata command log conflict must leave an entry");
            if !self.metadata_command_log_entry_matches(node_id, command, &entry, false)? {
                return Err(self.metadata_command_log_conflict(node_id, command));
            }
        }
        let inserted_entry =
            (inserted > 0).then_some((command.id().log_index(), command.checksum_crc64(), false));
        self.advance_metadata_command_log_state_with_inserted(
            node_id,
            command.id().cluster_epoch(),
            inserted_entry,
        )
    }

    pub(crate) fn record_metadata_command_abandoned(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| StoreError::Db {
                context: "record abandoned metadata command (begin txn)",
                source: source.into(),
            })?;

        let result = self.record_metadata_command_abandoned_inner(node_id, command);
        match result {
            Ok(record) => {
                if let Err(source) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    self.invalidate_clean_metadata_digest_revision();
                    return Err(StoreError::Db {
                        context: "record abandoned metadata command (commit txn)",
                        source: source.into(),
                    });
                }
                self.invalidate_clean_metadata_digest_revision();
                Ok(record.state)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                self.invalidate_clean_metadata_digest_revision();
                Err(error)
            }
        }
    }

    fn record_metadata_command_abandoned_inner(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandRecordResult, StoreError> {
        if command.id().pg_id().get() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id,
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
            });
        }
        let state = self.metadata_command_replica_state()?;
        self.validate_metadata_command_record_position(node_id, command, &state)?;
        if command.id().cluster_epoch() == state.cluster_epoch
            && command.id().log_index().get() <= state.applied_log_index
            && self
                .load_metadata_command_log_entry(
                    "load already-abandoned metadata command log entry",
                    command.id().cluster_epoch(),
                    command.id().pg_id(),
                    command.id().log_index(),
                )?
                .is_none()
        {
            if self
                .metadata_command_terminal_receipt_matches(node_id, command, true)?
                .is_some()
            {
                return Ok(MetadataCommandRecordResult {
                    state,
                    digest_revision: self.metadata_digest_revision()?,
                });
            }
            return Err(self.metadata_command_log_conflict(node_id, command));
        }
        let command_bytes = command.abandoned_log_bytes();
        let command_checksum = command.abandoned_log_checksum_crc64();
        let inserted = self.execute_cached(
                "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash, pre_state_digest, post_state_digest) \
             VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL, NULL, NULL, NULL) \
             ON CONFLICT(cluster_epoch, pg_id, log_index) DO NOTHING",
            params![
                command.id().cluster_epoch().get() as i64,
                command.id().pg_id().get() as i64,
                command.id().log_index().get() as i64,
                command_checksum as i64,
                command_bytes,
            ],
            "record abandoned metadata command log entry",
        )?;

        if inserted == 0 {
            let entry = self
                .load_metadata_command_log_entry(
                    "load abandoned metadata command log entry",
                    command.id().cluster_epoch(),
                    command.id().pg_id(),
                    command.id().log_index(),
                )?
                .expect("abandoned metadata command log conflict must leave an entry");
            if !self.metadata_command_log_entry_matches(node_id, command, &entry, true)? {
                return Err(self.metadata_command_log_conflict(node_id, command));
            }
        }
        let inserted_entry =
            (inserted > 0).then_some((command.id().log_index(), command_checksum, true));
        self.advance_metadata_command_log_state_with_inserted(
            node_id,
            command.id().cluster_epoch(),
            inserted_entry,
        )
    }

    pub fn metadata_command_log_stats(
        &self,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandLogStats, StoreError> {
        let state = self.metadata_command_replica_state()?;
        if state.cluster_epoch != cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: self.pg_id,
                operation_epoch: cluster_epoch,
                current_epoch: state.cluster_epoch,
            });
        }
        let base =
            self.metadata_command_log_validation_base(cluster_epoch, state.applied_log_index)?;
        let (raw_min, raw_max, raw_retained, raw_abandoned, raw_applied_entries, raw_tail_entries) = self
            .query_row_cached(
                "SELECT min(log_index), max(log_index), count(*), \
                    coalesce(sum(abandoned), 0), \
                    coalesce(sum(CASE WHEN log_index > ?3 AND log_index <= ?4 THEN 1 ELSE 0 END), 0), \
                    coalesce(sum(CASE WHEN log_index > ?4 THEN 1 ELSE 0 END), 0) \
                 FROM metadata_command_log \
                 WHERE cluster_epoch = ?1 AND pg_id = ?2",
                params![
                    cluster_epoch.get() as i64,
                    self.pg_id as i64,
                    base.applied_log_index as i64,
                    state.applied_log_index as i64,
                ],
                "load metadata command log stats",
                |row| {
                    Ok((
                        row.get::<_, Option<i64>>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )?;

        let min_log_index = raw_min
            .map(|raw| decode_nonnegative_u64("decode minimum metadata command log index", raw))
            .transpose()?;
        let max_log_index = raw_max
            .map(|raw| decode_nonnegative_u64("decode maximum metadata command log index", raw))
            .transpose()?;
        let retained_entries =
            decode_nonnegative_u64("decode metadata command log retained count", raw_retained)?;
        let abandoned_entries =
            decode_nonnegative_u64("decode metadata command log abandoned count", raw_abandoned)?;
        let applied_entries = decode_nonnegative_u64(
            "decode metadata command log applied-prefix count",
            raw_applied_entries,
        )?;
        let pending_tail_entries =
            decode_nonnegative_u64("decode metadata command log tail count", raw_tail_entries)?;
        let missing_applied_prefix_entries = state
            .applied_log_index
            .saturating_sub(base.applied_log_index)
            .saturating_sub(applied_entries);

        Ok(MetadataCommandLogStats {
            cluster_epoch,
            pg_id: PgId::new(self.pg_id),
            min_log_index,
            max_log_index,
            applied_log_index: state.applied_log_index,
            retained_entries,
            abandoned_entries,
            pending_tail_entries,
            missing_applied_prefix_entries,
            compactable_before: base.compactable_before,
        })
    }

    pub fn compact_metadata_command_log(
        &self,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandLogCompactionStatus, StoreError> {
        self.compact_metadata_command_log_with_receipt_retention(
            cluster_epoch,
            METADATA_COMMAND_TERMINAL_RECEIPT_RETAIN_PER_EPOCH,
        )
    }

    pub(super) fn compact_metadata_command_log_with_receipt_retention(
        &self,
        cluster_epoch: ClusterEpoch,
        receipt_retention: u64,
    ) -> Result<MetadataCommandLogCompactionStatus, StoreError> {
        assert!(receipt_retention > 0, "receipt retention must not be zero");
        let stats = self.metadata_command_log_stats(cluster_epoch)?;
        let Some(compactable_before) = stats.compactable_before else {
            return Ok(MetadataCommandLogCompactionStatus::NoCheckpoint {
                retained_entries: stats.retained_entries,
            });
        };
        if stats.pending_tail_entries > 0
            || self
                .pending_metadata_command_slot(0, cluster_epoch)?
                .is_some()
        {
            return Ok(MetadataCommandLogCompactionStatus::PendingCommand {
                retained_entries: stats.retained_entries,
            });
        }

        let max_compacted_log_index = compactable_before
            .checked_sub(1)
            .expect("compactable bound is exclusive and non-zero");
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| StoreError::Db {
                context: "begin metadata command log compaction",
                source: source.into(),
            })?;
        let result = (|| {
            self.validate_metadata_command_checkpoint_state_read_only(0, cluster_epoch)?;
            let first_retained_receipt = max_compacted_log_index
                .saturating_sub(receipt_retention - 1)
                .max(1);
            let receipts = self.metadata_command_terminal_receipts_in_range(
                cluster_epoch,
                first_retained_receipt,
                max_compacted_log_index,
            )?;
            for (log_index, receipt) in receipts {
                self.insert_metadata_command_terminal_receipt(cluster_epoch, log_index, &receipt)?;
            }
            let deleted = self.execute_cached(
                "DELETE FROM metadata_command_log \
                 WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index <= ?3",
                params![
                    cluster_epoch.get() as i64,
                    self.pg_id as i64,
                    max_compacted_log_index as i64,
                ],
                "compact metadata command log through checkpoint",
            )?;
            self.prune_metadata_command_terminal_receipts(cluster_epoch, first_retained_receipt)?;
            Ok(deleted)
        })();
        let deleted = match result {
            Ok(deleted) => {
                if let Err(source) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(StoreError::Db {
                        context: "commit metadata command log compaction",
                        source: source.into(),
                    });
                }
                deleted
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                return Err(error);
            }
        };
        Ok(MetadataCommandLogCompactionStatus::Compacted {
            deleted_entries: deleted as u64,
            compacted_before: compactable_before,
        })
    }

    fn metadata_command_terminal_receipts_in_range(
        &self,
        cluster_epoch: ClusterEpoch,
        min_log_index: u64,
        max_log_index: u64,
    ) -> Result<Vec<(MetadataCommandLogIndex, MetadataCommandTerminalReceipt)>, StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash \
                 FROM metadata_command_log \
                 WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index <= ?3 \
                 ORDER BY log_index",
            )
            .map_err(|source| StoreError::Db {
                context: "prepare compacted metadata command receipt scan",
                source: source.into(),
            })?;
        let rows = stmt
            .query_map(
                params![
                    cluster_epoch.get() as i64,
                    self.pg_id as i64,
                    max_log_index as i64,
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        MetadataCommandLogEntry {
                            command_checksum: row.get::<_, i64>(1)? as u64,
                            command_bytes: row.get(2)?,
                            abandoned: row.get::<_, i64>(3)? != 0,
                            previous_log_hash: row.get::<_, Option<i64>>(4)?.map(|v| v as u64),
                            log_hash: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                            pre_state_digest: None,
                            post_state_digest: None,
                        },
                    ))
                },
            )
            .map_err(|source| StoreError::Db {
                context: "scan compacted metadata command receipts",
                source: source.into(),
            })?;
        let mut receipts = Vec::new();
        for row in rows {
            let (raw_log_index, entry) = row.map_err(|source| StoreError::Db {
                context: "decode compacted metadata command receipt row",
                source: source.into(),
            })?;
            let log_index = decode_nonnegative_u64(
                "decode compacted metadata command receipt log index",
                raw_log_index,
            )?;
            let log_index = MetadataCommandLogIndex::new(log_index).ok_or({
                StoreError::MetadataCommandLogConflict {
                    node_id: 0,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    log_index,
                }
            })?;
            self.verify_metadata_command_log_entry(
                0,
                cluster_epoch,
                PgId::new(self.pg_id),
                log_index,
                &entry,
            )?;
            let Some(previous_log_hash) = entry.previous_log_hash else {
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id: 0,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    log_index: log_index.get(),
                });
            };
            let Some(log_hash) = entry.log_hash else {
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id: 0,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    log_index: log_index.get(),
                });
            };
            if log_index.get() >= min_log_index {
                receipts.push((
                    log_index,
                    MetadataCommandTerminalReceipt {
                        command_checksum: entry.command_checksum,
                        command_sha256: checksum::sha256::digest(&entry.command_bytes),
                        abandoned: entry.abandoned,
                        previous_log_hash,
                        log_hash,
                    },
                ));
            }
        }
        Ok(receipts)
    }

    fn prune_metadata_command_terminal_receipts(
        &self,
        cluster_epoch: ClusterEpoch,
        first_retained_log_index: u64,
    ) -> Result<(), StoreError> {
        self.execute_cached(
            "DELETE FROM metadata_command_terminal_receipts \
             WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index < ?3",
            params![
                cluster_epoch.get() as i64,
                self.pg_id as i64,
                first_retained_log_index as i64,
            ],
            "prune compacted metadata command terminal receipts",
        )?;
        Ok(())
    }

    fn insert_metadata_command_terminal_receipt(
        &self,
        cluster_epoch: ClusterEpoch,
        log_index: MetadataCommandLogIndex,
        receipt: &MetadataCommandTerminalReceipt,
    ) -> Result<(), StoreError> {
        let inserted = self.execute_cached(
            "INSERT INTO metadata_command_terminal_receipts \
             (cluster_epoch, pg_id, log_index, command_checksum, command_sha256, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
             ON CONFLICT(cluster_epoch, pg_id, log_index) DO NOTHING",
            params![
                cluster_epoch.get() as i64,
                self.pg_id as i64,
                log_index.get() as i64,
                receipt.command_checksum as i64,
                receipt.command_sha256.as_slice(),
                i64::from(receipt.abandoned),
                receipt.previous_log_hash as i64,
                receipt.log_hash as i64,
            ],
            "insert compacted metadata command terminal receipt",
        )?;
        if inserted == 1 {
            return Ok(());
        }
        let existing = self
            .load_metadata_command_terminal_receipt(
                "load existing compacted metadata command terminal receipt",
                cluster_epoch,
                PgId::new(self.pg_id),
                log_index,
            )?
            .ok_or_else(|| StoreError::MetadataCommandLogConflict {
                node_id: 0,
                pg_id: self.pg_id,
                cluster_epoch,
                log_index: log_index.get(),
            })?;
        if existing == *receipt {
            Ok(())
        } else {
            Err(StoreError::MetadataCommandLogConflict {
                node_id: 0,
                pg_id: self.pg_id,
                cluster_epoch,
                log_index: log_index.get(),
            })
        }
    }

    pub(crate) fn refresh_metadata_command_state_digest(&self) -> Result<(), StoreError> {
        self.refresh_all_metadata_table_digests()?;
        let state_digest = self.cached_metadata_state_digest()?;
        self.store_metadata_command_state_digest(state_digest)
    }

    fn store_metadata_command_state_digest(&self, state_digest: u64) -> Result<(), StoreError> {
        let state_digest = CanonicalStateDigest::from_storage(
            state_digest,
            super::MetadataProofStorageIssuer::new(),
        );
        self.execute_cached(
            "UPDATE metadata_command_replica_state \
             SET state_digest_encoding_version = ?1, state_digest = ?2 WHERE singleton = 0",
            params![
                state_digest.encoding_version() as i64,
                state_digest.value() as i64,
            ],
            "refresh metadata command state digest",
        )?;
        self.mark_metadata_state_digest_clean()?;
        Ok(())
    }

    pub(super) fn metadata_digest_revision(&self) -> Result<u64, StoreError> {
        let raw = self.query_row_cached(
            "SELECT revision FROM metadata_digest_revision WHERE singleton = 0",
            [],
            "load metadata digest revision",
            |row| row.get::<_, i64>(0),
        )?;
        decode_nonnegative_u64("decode metadata digest revision", raw)
    }

    fn mark_metadata_state_digest_clean(&self) -> Result<(), StoreError> {
        let revision = self.metadata_digest_revision()?;
        self.clean_metadata_digest_revision
            .store(revision, Ordering::Relaxed);
        Ok(())
    }

    pub(super) fn invalidate_clean_metadata_digest_revision(&self) {
        self.clean_metadata_digest_revision
            .store(UNCLEAN_METADATA_DIGEST_REVISION, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn test_metadata_digest_table_mismatches(
        &self,
    ) -> Result<Vec<(String, u64, u64)>, StoreError> {
        let mut mismatches = Vec::new();
        for table in METADATA_DIGEST_TABLES {
            let cached = self.cached_metadata_table_digest(table)?;
            let materialized = self.metadata_table_digest(table)?;
            if cached != materialized {
                mismatches.push((table.name.to_string(), cached, materialized));
            }
        }
        Ok(mismatches)
    }

    fn metadata_state_digest_mismatch(
        &self,
    ) -> Result<Option<(ClusterEpoch, u64, u64)>, StoreError> {
        let (state, revision) = self.metadata_command_replica_state_with_digest_revision()?;
        if self.clean_metadata_digest_revision.load(Ordering::Relaxed) == revision {
            return Ok(None);
        }
        let actual_digest = self.cached_metadata_state_digest()?;
        if state.state_digest.value() == actual_digest {
            self.clean_metadata_digest_revision
                .store(revision, Ordering::Relaxed);
            return Ok(None);
        }
        Ok(Some((
            state.cluster_epoch,
            state.state_digest.value(),
            actual_digest,
        )))
    }

    fn advance_metadata_command_log_state_with_inserted(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        inserted_entry: Option<(MetadataCommandLogIndex, u64, bool)>,
    ) -> Result<MetadataCommandRecordResult, StoreError> {
        let mut state = self.metadata_command_replica_state()?;
        if state.cluster_epoch != cluster_epoch {
            if cluster_epoch < state.cluster_epoch {
                return Err(StoreError::StaleMetadataCommand {
                    node_id,
                    pg_id: self.pg_id,
                    command_epoch: cluster_epoch,
                    current_epoch: state.cluster_epoch,
                });
            }
            let base_state_digest = state.state_digest;
            self.refresh_all_metadata_table_digests()?;
            state = MetadataCommandReplicaState {
                cluster_epoch,
                applied_log_index: 0,
                applied_log_hash: MetadataCommandLogHash::genesis(),
                state_digest: base_state_digest,
            };
        }

        let pg_id = PgId::new(self.pg_id);
        let mut applied_log_index = state.applied_log_index;
        let mut applied_log_hash = state.applied_log_hash.value();
        let mut advanced = false;
        let mut advanced_materialized_state = false;
        if let Some((inserted_log_index, command_checksum, inserted_abandoned)) = inserted_entry {
            if applied_log_index
                .checked_add(1)
                .is_some_and(|next| inserted_log_index.get() == next)
                && !self.metadata_command_log_has_tail_after(cluster_epoch, inserted_log_index)?
            {
                let expected_log_hash = metadata_command_log_hash(
                    cluster_epoch,
                    pg_id,
                    inserted_log_index,
                    applied_log_hash,
                    command_checksum,
                );
                let updated = self.execute_cached(
                    "UPDATE metadata_command_log \
                     SET previous_log_hash = ?1, log_hash = ?2, pre_state_digest = ?3 \
                     WHERE cluster_epoch = ?4 AND pg_id = ?5 AND log_index = ?6 \
                       AND previous_log_hash IS NULL AND log_hash IS NULL",
                    params![
                        applied_log_hash as i64,
                        expected_log_hash.value() as i64,
                        state.state_digest.value() as i64,
                        cluster_epoch.get() as i64,
                        self.pg_id as i64,
                        inserted_log_index.get() as i64,
                    ],
                    "update inserted metadata command log hash",
                )?;
                if updated != 1 {
                    return Err(StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id: self.pg_id,
                        cluster_epoch,
                        log_index: inserted_log_index.get(),
                    });
                }
                applied_log_index = inserted_log_index.get();
                applied_log_hash = expected_log_hash.value();
                #[cfg(test)]
                self.metadata_command_log_prefix_fast_path_hits
                    .fetch_add(1, Ordering::Relaxed);
                return if inserted_abandoned {
                    self.update_metadata_command_replica_state_preserving_digest(
                        cluster_epoch,
                        applied_log_index,
                        applied_log_hash,
                        state.state_digest,
                    )
                } else {
                    self.update_metadata_command_replica_state(
                        cluster_epoch,
                        applied_log_index,
                        applied_log_hash,
                    )
                };
            }
        }

        while let Some(next_log_index) = applied_log_index.checked_add(1) {
            let log_index = MetadataCommandLogIndex::new(next_log_index)
                .expect("metadata command log index is non-zero");

            let (expected_log_hash, entry_abandoned) =
                if let Some((inserted_log_index, command_checksum, inserted_abandoned)) =
                    inserted_entry
                        .filter(|(inserted_log_index, _, _)| *inserted_log_index == log_index)
                {
                    let expected_log_hash = metadata_command_log_hash(
                        cluster_epoch,
                        pg_id,
                        inserted_log_index,
                        applied_log_hash,
                        command_checksum,
                    );
                    let updated = self.execute_cached(
                        "UPDATE metadata_command_log \
                     SET previous_log_hash = ?1, log_hash = ?2, pre_state_digest = ?3 \
                     WHERE cluster_epoch = ?4 AND pg_id = ?5 AND log_index = ?6 \
                       AND previous_log_hash IS NULL AND log_hash IS NULL",
                        params![
                            applied_log_hash as i64,
                            expected_log_hash.value() as i64,
                            state.state_digest.value() as i64,
                            cluster_epoch.get() as i64,
                            self.pg_id as i64,
                            next_log_index as i64,
                        ],
                        "update inserted metadata command log hash",
                    )?;
                    if updated != 1 {
                        return Err(StoreError::MetadataCommandLogConflict {
                            node_id,
                            pg_id: self.pg_id,
                            cluster_epoch,
                            log_index: next_log_index,
                        });
                    }
                    (expected_log_hash, inserted_abandoned)
                } else {
                    let Some(entry) = self.load_metadata_command_log_entry(
                        "load next metadata command log entry",
                        cluster_epoch,
                        pg_id,
                        log_index,
                    )?
                    else {
                        break;
                    };
                    self.verify_metadata_command_log_entry(
                        node_id,
                        cluster_epoch,
                        pg_id,
                        log_index,
                        &entry,
                    )?;
                    let expected_log_hash = metadata_command_log_hash(
                        cluster_epoch,
                        pg_id,
                        log_index,
                        applied_log_hash,
                        entry.command_checksum,
                    );
                    match (entry.previous_log_hash, entry.log_hash) {
                        (None, None) => {
                            self.execute_cached(
                                "UPDATE metadata_command_log \
                             SET previous_log_hash = ?1, log_hash = ?2 \
                             WHERE cluster_epoch = ?3 AND pg_id = ?4 AND log_index = ?5",
                                params![
                                    applied_log_hash as i64,
                                    expected_log_hash.value() as i64,
                                    cluster_epoch.get() as i64,
                                    self.pg_id as i64,
                                    next_log_index as i64,
                                ],
                                "update metadata command log hash",
                            )?;
                        }
                        (Some(previous_log_hash), Some(log_hash))
                            if previous_log_hash == applied_log_hash
                                && log_hash == expected_log_hash.value() => {}
                        (previous_log_hash, log_hash) => {
                            return Err(StoreError::MetadataCommandLogHashMismatch {
                                node_id,
                                pg_id: self.pg_id,
                                cluster_epoch,
                                log_index: next_log_index,
                                expected_previous_log_hash: applied_log_hash,
                                actual_previous_log_hash: previous_log_hash.unwrap_or_default(),
                                expected_log_hash: expected_log_hash.value(),
                                actual_log_hash: log_hash.unwrap_or_default(),
                            });
                        }
                    }
                    (expected_log_hash, entry.abandoned)
                };
            if !entry_abandoned {
                advanced_materialized_state = true;
            }
            applied_log_index = next_log_index;
            applied_log_hash = expected_log_hash.value();
            advanced = true;
        }

        if !advanced {
            return Ok(MetadataCommandRecordResult {
                state,
                digest_revision: self.metadata_digest_revision()?,
            });
        }
        if advanced_materialized_state {
            self.update_metadata_command_replica_state(
                cluster_epoch,
                applied_log_index,
                applied_log_hash,
            )
        } else {
            self.update_metadata_command_replica_state_preserving_digest(
                cluster_epoch,
                applied_log_index,
                applied_log_hash,
                state.state_digest,
            )
        }
    }

    pub(super) fn update_metadata_command_replica_state(
        &self,
        cluster_epoch: ClusterEpoch,
        applied_log_index: u64,
        applied_log_hash: u64,
    ) -> Result<MetadataCommandRecordResult, StoreError> {
        let (state_digest, digest_revision) = self.cached_metadata_state_digest_with_revision()?;
        let applied_log_hash = MetadataCommandLogHash::from_storage(
            applied_log_hash,
            super::MetadataProofStorageIssuer::new(),
        );
        let state_digest = CanonicalStateDigest::from_storage(
            state_digest,
            super::MetadataProofStorageIssuer::new(),
        );
        self.execute_cached(
            "UPDATE metadata_command_replica_state \
			 SET cluster_epoch = ?1, applied_log_index = ?2, \
                 applied_log_hash_encoding_version = ?3, applied_log_hash = ?4, \
                 state_digest_encoding_version = ?5, state_digest = ?6 \
             WHERE singleton = 0",
            params![
                cluster_epoch.get() as i64,
                applied_log_index as i64,
                applied_log_hash.encoding_version() as i64,
                applied_log_hash.value() as i64,
                state_digest.encoding_version() as i64,
                state_digest.value() as i64,
            ],
            "update metadata command replica state",
        )?;
        if applied_log_index > 0 {
            self.execute_cached(
                "UPDATE metadata_command_log \
                 SET post_state_digest = ?1 \
                 WHERE cluster_epoch = ?2 AND pg_id = ?3 AND log_index = ?4 AND abandoned = 0",
                params![
                    state_digest.value() as i64,
                    cluster_epoch.get() as i64,
                    self.pg_id as i64,
                    applied_log_index as i64,
                ],
                "update metadata command log post-state digest",
            )?;
        }
        Ok(MetadataCommandRecordResult {
            state: MetadataCommandReplicaState {
                cluster_epoch,
                applied_log_index,
                applied_log_hash,
                state_digest,
            },
            digest_revision,
        })
    }

    fn update_metadata_command_replica_state_preserving_digest(
        &self,
        cluster_epoch: ClusterEpoch,
        applied_log_index: u64,
        applied_log_hash: u64,
        state_digest: CanonicalStateDigest,
    ) -> Result<MetadataCommandRecordResult, StoreError> {
        let applied_log_hash = MetadataCommandLogHash::from_storage(
            applied_log_hash,
            super::MetadataProofStorageIssuer::new(),
        );
        self.execute_cached(
            "UPDATE metadata_command_replica_state \
			 SET cluster_epoch = ?1, applied_log_index = ?2, \
                 applied_log_hash_encoding_version = ?3, applied_log_hash = ?4, \
                 state_digest_encoding_version = ?5, state_digest = ?6 \
             WHERE singleton = 0",
            params![
                cluster_epoch.get() as i64,
                applied_log_index as i64,
                applied_log_hash.encoding_version() as i64,
                applied_log_hash.value() as i64,
                state_digest.encoding_version() as i64,
                state_digest.value() as i64,
            ],
            "update metadata command replica state preserving digest",
        )?;
        if applied_log_index > 0 {
            self.execute_cached(
                "UPDATE metadata_command_log \
                 SET post_state_digest = ?1 \
                 WHERE cluster_epoch = ?2 AND pg_id = ?3 AND log_index = ?4 AND abandoned = 0",
                params![
                    state_digest.value() as i64,
                    cluster_epoch.get() as i64,
                    self.pg_id as i64,
                    applied_log_index as i64,
                ],
                "update metadata command log post-state digest preserving digest",
            )?;
        }
        Ok(MetadataCommandRecordResult {
            state: MetadataCommandReplicaState {
                cluster_epoch,
                applied_log_index,
                applied_log_hash,
                state_digest,
            },
            digest_revision: self.metadata_digest_revision()?,
        })
    }

    fn cached_metadata_state_digest_with_revision(&self) -> Result<(u64, u64), StoreError> {
        let (mut table_digests, mut revision) =
            self.cached_metadata_table_digests_with_revision()?;
        if METADATA_DIGEST_TABLES
            .iter()
            .any(|table| !table_digests.contains_key(table.name))
        {
            self.refresh_all_metadata_table_digests()?;
            (table_digests, revision) = self.cached_metadata_table_digests_with_revision()?;
        }

        let mut hasher = checksum::crc64::Hasher::new();
        Self::digest_canonical_pg_state_header(&mut hasher);
        for table in METADATA_DIGEST_TABLES {
            let table_digest =
                table_digests
                    .get(table.name)
                    .copied()
                    .ok_or_else(|| StoreError::Db {
                        context: "load initialized cached metadata table digest",
                        source: rusqlite::Error::QueryReturnedNoRows.into(),
                    })?;
            Self::digest_metadata_table_digest_entry(&mut hasher, table, table_digest);
        }
        Ok((hasher.finalize(), revision))
    }

    pub(super) fn cached_metadata_table_digests_with_revision(
        &self,
    ) -> Result<(HashMap<String, u64>, u64), StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT d.table_name, d.table_digest, r.revision \
                 FROM metadata_table_digests d, metadata_digest_revision r \
                 WHERE r.singleton = 0",
            )
            .map_err(|source| StoreError::Db {
                context: "prepare cached metadata table digests with revision",
                source: source.into(),
            })?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(|source| StoreError::Db {
                context: "load cached metadata table digests with revision",
                source: source.into(),
            })?;
        let mut digests = HashMap::new();
        let mut revision = None;
        for row in rows {
            let (table_name, table_digest, raw_revision) =
                row.map_err(|source| StoreError::Db {
                    context: "decode cached metadata table digest with revision",
                    source: source.into(),
                })?;
            digests.insert(table_name, table_digest as u64);
            revision = Some(decode_nonnegative_u64(
                "decode metadata digest revision",
                raw_revision,
            )?);
        }
        let revision = revision.ok_or_else(|| StoreError::Db {
            context: "load metadata digest revision with cached table digests",
            source: rusqlite::Error::QueryReturnedNoRows.into(),
        })?;
        Ok((digests, revision))
    }

    pub(super) fn mark_metadata_state_digest_clean_at_revision(&self, revision: u64) {
        self.clean_metadata_digest_revision
            .store(revision, Ordering::Relaxed);
    }

    fn metadata_command_log_has_tail_after(
        &self,
        cluster_epoch: ClusterEpoch,
        log_index: MetadataCommandLogIndex,
    ) -> Result<bool, StoreError> {
        self.query_row_cached_optional(
            "SELECT 1 FROM metadata_command_log \
             WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index > ?3 \
             LIMIT 1",
            params![
                cluster_epoch.get() as i64,
                self.pg_id as i64,
                log_index.get() as i64,
            ],
            "check metadata command log tail",
            |_| Ok(()),
        )
        .map(|value| value.is_some())
    }

    fn metadata_command_log_min_tail_after(
        &self,
        cluster_epoch: ClusterEpoch,
        applied_log_index: u64,
    ) -> Result<Option<u64>, StoreError> {
        self.query_row_cached(
            "SELECT min(log_index) FROM metadata_command_log \
             WHERE cluster_epoch = ?1 AND pg_id = ?2 AND log_index > ?3",
            params![
                cluster_epoch.get() as i64,
                self.pg_id as i64,
                applied_log_index as i64,
            ],
            "load first metadata command log tail index",
            |row| row.get::<_, Option<i64>>(0),
        )?
        .map(|raw| decode_nonnegative_u64("decode metadata command log tail index", raw))
        .transpose()
    }

    pub(super) fn metadata_state_digest(&self) -> Result<u64, StoreError> {
        let mut hasher = checksum::crc64::Hasher::new();
        Self::digest_canonical_pg_state_header(&mut hasher);
        for table in METADATA_DIGEST_TABLES {
            let table_digest = self.metadata_table_digest(table)?;
            Self::digest_metadata_table_digest_entry(&mut hasher, table, table_digest);
        }
        Ok(hasher.finalize())
    }

    fn metadata_state_digest_from_checkpoint_tables(
        &self,
        table_digests: &[MetadataCheckpointTableDigest],
    ) -> u64 {
        let mut hasher = checksum::crc64::Hasher::new();
        Self::digest_canonical_pg_state_header(&mut hasher);
        for (table, checkpoint_table) in METADATA_DIGEST_TABLES.iter().zip(table_digests) {
            debug_assert_eq!(table.name, checkpoint_table.table_name);
            Self::digest_metadata_table_digest_entry(
                &mut hasher,
                table,
                checkpoint_table.table_digest,
            );
        }
        hasher.finalize()
    }

    fn clear_metadata_checkpoint_tables(&self) -> Result<(), StoreError> {
        for table in METADATA_DIGEST_TABLES.iter().rev() {
            let table_sql = quote_sql_identifier(table.name);
            self.execute_cached(
                &format!("DELETE FROM {table_sql}"),
                [],
                "clear metadata checkpoint destination table",
            )?;
        }
        Ok(())
    }

    fn clear_metadata_transfer_checkpoint_destination_state(&self) -> Result<(), StoreError> {
        self.clear_metadata_checkpoint_tables()?;
        self.execute_cached(
            "DELETE FROM metadata_command_log",
            [],
            "clear stale metadata transfer command log",
        )?;
        self.execute_cached(
            "DELETE FROM metadata_command_terminal_receipts",
            [],
            "clear stale compacted metadata command receipts",
        )?;
        self.execute_cached(
            "DELETE FROM metadata_command_checkpoints",
            [],
            "clear stale metadata transfer checkpoints",
        )?;
        Ok(())
    }

    fn insert_metadata_checkpoint_table_blocks(
        &self,
        blocks: &[MetadataCheckpointTableBlock],
    ) -> Result<(), StoreError> {
        for (table, block) in METADATA_DIGEST_TABLES.iter().zip(blocks) {
            self.insert_metadata_checkpoint_table_block(table, block)?;
        }
        Ok(())
    }

    fn insert_metadata_checkpoint_table_block(
        &self,
        table: &MetadataDigestTable,
        block: &MetadataCheckpointTableBlock,
    ) -> Result<(), StoreError> {
        if block.rows.is_empty() {
            return Ok(());
        }
        let table_sql = quote_sql_identifier(table.name);
        let columns = table
            .columns
            .iter()
            .map(|column| quote_sql_identifier(column))
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = (1..=table.columns.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("INSERT INTO {table_sql} ({columns}) VALUES ({placeholders})");
        let mut stmt = self.conn.prepare_cached(&sql).map_err(|e| StoreError::Db {
            context: "prepare metadata checkpoint row insert",
            source: e.into(),
        })?;
        for row in &block.rows {
            let values = row
                .values
                .iter()
                .map(Self::metadata_checkpoint_value_to_sql)
                .collect::<Result<Vec<_>, _>>()?;
            stmt.execute(params_from_iter(values))
                .map_err(|e| StoreError::Db {
                    context: "insert metadata checkpoint row",
                    source: e.into(),
                })?;
        }
        Ok(())
    }

    fn metadata_checkpoint_value_to_sql(
        value: &MetadataCheckpointValue,
    ) -> Result<rusqlite::types::Value, StoreError> {
        Ok(match value {
            MetadataCheckpointValue::Null => rusqlite::types::Value::Null,
            MetadataCheckpointValue::Integer(value) => rusqlite::types::Value::Integer(*value),
            MetadataCheckpointValue::RealBits(value) => {
                rusqlite::types::Value::Real(f64::from_bits(*value))
            }
            MetadataCheckpointValue::Text(value) => {
                let text = String::from_utf8(value.clone()).map_err(|error| StoreError::Db {
                    context: "encode metadata checkpoint text value",
                    source: crate::error::DatabaseError::to_sql_conversion_failure(Box::new(error)),
                })?;
                rusqlite::types::Value::Text(text)
            }
            MetadataCheckpointValue::Blob(value) => rusqlite::types::Value::Blob(value.clone()),
        })
    }

    fn metadata_checkpoint_table_blocks(
        &self,
    ) -> Result<Vec<MetadataCheckpointTableBlock>, StoreError> {
        let mut blocks = Vec::with_capacity(METADATA_DIGEST_TABLES.len());
        for table in METADATA_DIGEST_TABLES {
            blocks.push(self.metadata_checkpoint_table_block(table)?);
        }
        Ok(blocks)
    }

    fn metadata_checkpoint_table_block(
        &self,
        table: &MetadataDigestTable,
    ) -> Result<MetadataCheckpointTableBlock, StoreError> {
        let where_clause = Self::metadata_digest_where_clause(table.filter);
        let table_sql = quote_sql_identifier(table.name);
        let quoted_columns: Vec<String> = table
            .columns
            .iter()
            .map(|column| quote_sql_identifier(column))
            .collect();
        let select_values = quoted_columns.join(", ");
        let quoted_order_columns: Vec<String> = table
            .order_columns
            .iter()
            .map(|column| quote_sql_identifier(column))
            .collect();
        let order_by = quoted_order_columns.join(", ");
        let sql =
            format!("SELECT {select_values} FROM {table_sql}{where_clause} ORDER BY {order_by}");
        let mut stmt = self.conn.prepare_cached(&sql).map_err(|e| StoreError::Db {
            context: "prepare canonical metadata checkpoint table block scan",
            source: e.into(),
        })?;
        let mut rows = stmt.query([]).map_err(|e| StoreError::Db {
            context: "scan canonical metadata checkpoint table block rows",
            source: e.into(),
        })?;
        let mut checkpoint_rows = Vec::new();
        let mut stats = MetadataTableDigestStats {
            row_count: 0,
            row_hash_xor: 0,
            row_hash_sum: 0,
        };
        while let Some(row) = rows.next().map_err(|e| StoreError::Db {
            context: "scan canonical metadata checkpoint table block row",
            source: e.into(),
        })? {
            let mut values = Vec::with_capacity(table.columns.len());
            for index in 0..table.columns.len() {
                let value = row.get_ref(index).map_err(|e| StoreError::Db {
                    context: "read canonical metadata checkpoint row value",
                    source: e.into(),
                })?;
                values.push(Self::metadata_checkpoint_value_from_sql(value));
            }
            let row_digest = Self::metadata_checkpoint_row_digest(table, &values);
            stats.row_count += 1;
            stats.row_hash_xor ^= row_digest;
            stats.row_hash_sum = stats.row_hash_sum.wrapping_add(row_digest);
            checkpoint_rows.push(MetadataCheckpointRow { values, row_digest });
        }
        let table_digest = metadata_table_digest_from_stats(table, stats);
        Ok(MetadataCheckpointTableBlock {
            table_name: table.name.to_owned(),
            columns: table
                .columns
                .iter()
                .map(|column| (*column).to_owned())
                .collect(),
            order_columns: table
                .order_columns
                .iter()
                .map(|column| (*column).to_owned())
                .collect(),
            filter: table.filter.canonical_name().to_owned(),
            rows: checkpoint_rows,
            row_count: stats.row_count,
            row_hash_xor: stats.row_hash_xor,
            row_hash_sum: stats.row_hash_sum,
            table_digest,
        })
    }

    fn metadata_command_checkpoint_crc64(checkpoint: &MetadataCommandCheckpoint) -> u64 {
        Self::metadata_command_checkpoint_crc64_for_encoding_version(
            checkpoint,
            METADATA_COMMAND_CHECKPOINT_ENCODING_VERSION,
        )
    }

    fn metadata_command_checkpoint_crc64_for_encoding_version(
        checkpoint: &MetadataCommandCheckpoint,
        encoding_version: u16,
    ) -> u64 {
        let mut hasher = checksum::crc64::Hasher::new();
        digest_len_prefixed_bytes(&mut hasher, METADATA_COMMAND_CHECKPOINT_DOMAIN);
        hasher.update(METADATA_COMMAND_CHECKPOINT_MAGIC);
        hasher.update(&encoding_version.to_be_bytes());
        digest_u64(&mut hasher, checkpoint.cluster_epoch.get());
        digest_u64(&mut hasher, checkpoint.pg_id.get() as u64);
        digest_u64(&mut hasher, checkpoint.applied_log_index);
        digest_u8(&mut hasher, checkpoint.applied_log_hash.encoding_version());
        digest_u64(&mut hasher, checkpoint.applied_log_hash.value());
        digest_u8(&mut hasher, checkpoint.state_digest.encoding_version());
        digest_u64(&mut hasher, checkpoint.state_digest.value());
        digest_u64(&mut hasher, checkpoint.table_digests.len() as u64);
        for table in &checkpoint.table_digests {
            digest_len_prefixed_bytes(&mut hasher, table.table_name.as_bytes());
            digest_u64(&mut hasher, table.row_count);
            digest_u64(&mut hasher, table.row_hash_xor);
            digest_u64(&mut hasher, table.row_hash_sum);
            digest_u64(&mut hasher, table.table_digest);
        }
        digest_u64(&mut hasher, checkpoint.table_blocks.len() as u64);
        for block in &checkpoint.table_blocks {
            digest_len_prefixed_bytes(&mut hasher, block.table_name.as_bytes());
            digest_u64(&mut hasher, block.columns.len() as u64);
            for column in &block.columns {
                digest_len_prefixed_bytes(&mut hasher, column.as_bytes());
            }
            digest_u64(&mut hasher, block.order_columns.len() as u64);
            for column in &block.order_columns {
                digest_len_prefixed_bytes(&mut hasher, column.as_bytes());
            }
            digest_len_prefixed_bytes(&mut hasher, block.filter.as_bytes());
            digest_u64(&mut hasher, block.row_count);
            digest_u64(&mut hasher, block.row_hash_xor);
            digest_u64(&mut hasher, block.row_hash_sum);
            digest_u64(&mut hasher, block.table_digest);
            digest_u64(&mut hasher, block.rows.len() as u64);
            for row in &block.rows {
                digest_u64(&mut hasher, row.row_digest);
                digest_u64(&mut hasher, row.values.len() as u64);
                for value in &row.values {
                    Self::digest_metadata_checkpoint_value(&mut hasher, value);
                }
            }
        }
        hasher.finalize()
    }

    #[cfg(test)]
    pub(super) fn test_reseal_metadata_command_checkpoint(
        checkpoint: &mut MetadataCommandCheckpoint,
    ) {
        for ((table, summary), block) in METADATA_DIGEST_TABLES
            .iter()
            .zip(&mut checkpoint.table_digests)
            .zip(&mut checkpoint.table_blocks)
        {
            let mut stats = MetadataTableDigestStats {
                row_count: 0,
                row_hash_xor: 0,
                row_hash_sum: 0,
            };
            for row in &mut block.rows {
                row.row_digest = Self::metadata_checkpoint_row_digest(table, &row.values);
                stats.row_count += 1;
                stats.row_hash_xor ^= row.row_digest;
                stats.row_hash_sum = stats.row_hash_sum.wrapping_add(row.row_digest);
            }
            let table_digest = metadata_table_digest_from_stats(table, stats);
            summary.row_count = stats.row_count;
            summary.row_hash_xor = stats.row_hash_xor;
            summary.row_hash_sum = stats.row_hash_sum;
            summary.table_digest = table_digest;
            block.row_count = stats.row_count;
            block.row_hash_xor = stats.row_hash_xor;
            block.row_hash_sum = stats.row_hash_sum;
            block.table_digest = table_digest;
        }
        let mut state_hasher = checksum::crc64::Hasher::new();
        Self::digest_canonical_pg_state_header(&mut state_hasher);
        for (table, summary) in METADATA_DIGEST_TABLES.iter().zip(&checkpoint.table_digests) {
            Self::digest_metadata_table_digest_entry(
                &mut state_hasher,
                table,
                summary.table_digest,
            );
        }
        checkpoint.state_digest = CanonicalStateDigest::from_storage(
            state_hasher.finalize(),
            super::MetadataProofStorageIssuer::new(),
        );
        checkpoint.checkpoint_crc64 = Self::metadata_command_checkpoint_crc64(checkpoint);
    }

    #[cfg(test)]
    pub(crate) fn test_reseal_metadata_command_checkpoint_for_encoding_version(
        checkpoint: &mut MetadataCommandCheckpoint,
        encoding_version: u16,
    ) {
        checkpoint.checkpoint_crc64 = Self::metadata_command_checkpoint_crc64_for_encoding_version(
            checkpoint,
            encoding_version,
        );
    }

    fn digest_canonical_pg_state_header(hasher: &mut checksum::crc64::Hasher) {
        digest_len_prefixed_bytes(hasher, METADATA_CANONICAL_PG_STATE_DOMAIN);
        digest_u8(hasher, METADATA_CANONICAL_STATE_ENCODING_VERSION);
        digest_u64(hasher, METADATA_DIGEST_TABLES.len() as u64);
    }

    fn digest_metadata_table_digest_entry(
        hasher: &mut checksum::crc64::Hasher,
        table: &MetadataDigestTable,
        table_digest: u64,
    ) {
        digest_u8(hasher, 0x12);
        digest_len_prefixed_bytes(hasher, table.name.as_bytes());
        digest_u64(hasher, table_digest);
    }

    fn metadata_checkpoint_row_digest(
        table: &MetadataDigestTable,
        values: &[MetadataCheckpointValue],
    ) -> u64 {
        let mut hasher = checksum::crc64::Hasher::new();
        digest_u8(&mut hasher, 0x20);
        digest_len_prefixed_bytes(&mut hasher, table.name.as_bytes());
        digest_u64(&mut hasher, values.len() as u64);
        for value in values {
            Self::digest_metadata_checkpoint_value(&mut hasher, value);
        }
        hasher.finalize()
    }

    fn metadata_checkpoint_value_from_sql(value: ValueRef<'_>) -> MetadataCheckpointValue {
        match value {
            ValueRef::Null => MetadataCheckpointValue::Null,
            ValueRef::Integer(value) => MetadataCheckpointValue::Integer(value),
            ValueRef::Real(value) => MetadataCheckpointValue::RealBits(value.to_bits()),
            ValueRef::Text(value) => MetadataCheckpointValue::Text(value.to_vec()),
            ValueRef::Blob(value) => MetadataCheckpointValue::Blob(value.to_vec()),
        }
    }

    fn digest_metadata_checkpoint_value(
        hasher: &mut checksum::crc64::Hasher,
        value: &MetadataCheckpointValue,
    ) {
        match value {
            MetadataCheckpointValue::Null => {
                digest_u8(hasher, 0x00);
            }
            MetadataCheckpointValue::Integer(value) => {
                digest_u8(hasher, 0x01);
                digest_i64(hasher, *value);
            }
            MetadataCheckpointValue::RealBits(value) => {
                digest_u8(hasher, 0x02);
                digest_u64(hasher, *value);
            }
            MetadataCheckpointValue::Text(value) => {
                digest_u8(hasher, 0x03);
                digest_len_prefixed_bytes(hasher, value);
            }
            MetadataCheckpointValue::Blob(value) => {
                digest_u8(hasher, 0x04);
                digest_len_prefixed_bytes_crc64(hasher, value);
            }
        }
    }

    pub(super) fn metadata_table_digest(
        &self,
        table: &MetadataDigestTable,
    ) -> Result<u64, StoreError> {
        let stats = self.metadata_table_digest_stats(table)?;
        Ok(metadata_table_digest_from_stats(table, stats))
    }

    fn metadata_table_digest_stats(
        &self,
        table: &MetadataDigestTable,
    ) -> Result<MetadataTableDigestStats, StoreError> {
        let where_clause = Self::metadata_digest_where_clause(table.filter);
        let table_sql = quote_sql_identifier(table.name);
        let quoted_columns: Vec<String> = table
            .columns
            .iter()
            .map(|column| quote_sql_identifier(column))
            .collect();
        let select_values = quoted_columns.join(", ");
        let quoted_order_columns: Vec<String> = table
            .order_columns
            .iter()
            .map(|column| quote_sql_identifier(column))
            .collect();
        let order_by = quoted_order_columns.join(", ");
        let sql =
            format!("SELECT {select_values} FROM {table_sql}{where_clause} ORDER BY {order_by}");
        let mut stmt = self.conn.prepare_cached(&sql).map_err(|e| StoreError::Db {
            context: "prepare canonical metadata table range scan",
            source: e.into(),
        })?;
        let mut rows = stmt.query([]).map_err(|e| StoreError::Db {
            context: "scan canonical metadata table range rows",
            source: e.into(),
        })?;
        let mut stats = MetadataTableDigestStats {
            row_count: 0,
            row_hash_xor: 0,
            row_hash_sum: 0,
        };
        while let Some(row) = rows.next().map_err(|e| StoreError::Db {
            context: "scan canonical metadata table range row",
            source: e.into(),
        })? {
            let mut hasher = checksum::crc64::Hasher::new();
            digest_u8(&mut hasher, 0x20);
            digest_len_prefixed_bytes(&mut hasher, table.name.as_bytes());
            digest_u64(&mut hasher, table.columns.len() as u64);
            for index in 0..table.columns.len() {
                let value = row.get_ref(index).map_err(|e| StoreError::Db {
                    context: "read canonical metadata table range value",
                    source: e.into(),
                })?;
                Self::digest_canonical_sql_value(&mut hasher, value);
            }
            let row_digest = hasher.finalize();
            stats.row_count += 1;
            stats.row_hash_xor ^= row_digest;
            stats.row_hash_sum = stats.row_hash_sum.wrapping_add(row_digest);
        }
        Ok(stats)
    }

    fn refresh_all_metadata_table_digests(&self) -> Result<(), StoreError> {
        for table in METADATA_DIGEST_TABLES {
            self.refresh_metadata_table_digest(table)?;
        }
        self.bump_metadata_digest_revision()?;
        Ok(())
    }

    fn bump_metadata_digest_revision(&self) -> Result<(), StoreError> {
        self.execute_cached(
            "UPDATE metadata_digest_revision SET revision = revision + 1 WHERE singleton = 0",
            [],
            "bump metadata digest revision",
        )?;
        Ok(())
    }

    fn refresh_metadata_table_digest(&self, table: &MetadataDigestTable) -> Result<(), StoreError> {
        let stats = self.metadata_table_digest_stats(table)?;
        let table_digest = metadata_table_digest_from_stats(table, stats);
        self.execute_cached(
            "INSERT INTO metadata_table_digests \
             (table_name, table_digest, row_count, row_hash_xor, row_hash_sum) \
             VALUES (?1, ?2, ?3, ?4, ?5) \
             ON CONFLICT(table_name) DO UPDATE SET \
                table_digest = excluded.table_digest, \
                row_count = excluded.row_count, \
                row_hash_xor = excluded.row_hash_xor, \
                row_hash_sum = excluded.row_hash_sum",
            params![
                table.name,
                table_digest as i64,
                stats.row_count as i64,
                stats.row_hash_xor as i64,
                stats.row_hash_sum as i64,
            ],
            "refresh metadata table digest",
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn cached_metadata_table_digest(
        &self,
        table: &MetadataDigestTable,
    ) -> Result<u64, StoreError> {
        let raw = self.query_row_cached_optional(
            "SELECT table_digest FROM metadata_table_digests WHERE table_name = ?1",
            params![table.name],
            "load cached metadata table digest",
            |row| row.get::<_, i64>(0),
        )?;
        match raw {
            Some(value) => Ok(value as u64),
            None => {
                self.refresh_all_metadata_table_digests()?;
                self.query_row_cached(
                    "SELECT table_digest FROM metadata_table_digests WHERE table_name = ?1",
                    params![table.name],
                    "load initialized cached metadata table digest",
                    |row| row.get::<_, i64>(0),
                )
                .map(|value| value as u64)
            }
        }
    }

    pub(super) fn cached_metadata_table_digests(&self) -> Result<HashMap<String, u64>, StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT table_name, table_digest FROM metadata_table_digests")
            .map_err(|e| StoreError::Db {
                context: "prepare cached metadata table digests",
                source: e.into(),
            })?;
        let mut rows = stmt.query([]).map_err(|e| StoreError::Db {
            context: "load cached metadata table digests",
            source: e.into(),
        })?;
        let mut digests = HashMap::with_capacity(METADATA_DIGEST_TABLES.len());
        while let Some(row) = rows.next().map_err(|e| StoreError::Db {
            context: "load cached metadata table digest row",
            source: e.into(),
        })? {
            let table_name = row.get::<_, String>(0).map_err(|e| StoreError::Db {
                context: "decode cached metadata table digest name",
                source: e.into(),
            })?;
            let table_digest = row.get::<_, i64>(1).map_err(|e| StoreError::Db {
                context: "decode cached metadata table digest",
                source: e.into(),
            })? as u64;
            digests.insert(table_name, table_digest);
        }
        Ok(digests)
    }

    pub(super) fn cached_metadata_state_digest(&self) -> Result<u64, StoreError> {
        let mut table_digests = self.cached_metadata_table_digests()?;
        if METADATA_DIGEST_TABLES
            .iter()
            .any(|table| !table_digests.contains_key(table.name))
        {
            self.refresh_all_metadata_table_digests()?;
            table_digests = self.cached_metadata_table_digests()?;
        }

        let mut hasher = checksum::crc64::Hasher::new();
        Self::digest_canonical_pg_state_header(&mut hasher);
        for table in METADATA_DIGEST_TABLES {
            let table_digest =
                table_digests
                    .get(table.name)
                    .copied()
                    .ok_or_else(|| StoreError::Db {
                        context: "load initialized cached metadata table digest",
                        source: rusqlite::Error::QueryReturnedNoRows.into(),
                    })?;
            Self::digest_metadata_table_digest_entry(&mut hasher, table, table_digest);
        }
        Ok(hasher.finalize())
    }

    #[cfg(test)]
    pub(crate) fn test_metadata_command_log_replay_validation_entries(&self) -> u64 {
        self.metadata_command_log_replay_validation_entries
            .load(Ordering::Relaxed)
    }

    pub(super) fn digest_canonical_sql_value(
        hasher: &mut checksum::crc64::Hasher,
        value: ValueRef<'_>,
    ) {
        match value {
            ValueRef::Null => {
                digest_u8(hasher, 0x00);
            }
            ValueRef::Integer(value) => {
                digest_u8(hasher, 0x01);
                digest_i64(hasher, value);
            }
            ValueRef::Real(value) => {
                digest_u8(hasher, 0x02);
                digest_u64(hasher, value.to_bits());
            }
            ValueRef::Text(value) => {
                digest_u8(hasher, 0x03);
                digest_len_prefixed_bytes(hasher, value);
            }
            ValueRef::Blob(value) => {
                digest_u8(hasher, 0x04);
                digest_len_prefixed_bytes_crc64(hasher, value);
            }
        }
    }

    fn metadata_digest_where_clause(filter: MetadataDigestFilter) -> String {
        match filter {
            MetadataDigestFilter::AllRows => String::new(),
        }
    }
}

#[cfg(test)]
mod canonical_format_baseline_tests {
    use super::*;

    fn insert_test_checkpoint_row(store: &PgStore, applied_log_index: usize) {
        store
            .conn
            .execute(
                "INSERT INTO metadata_command_checkpoints \
                 (cluster_epoch, pg_id, applied_log_index, applied_log_hash, state_digest, checkpoint_crc64, checkpoint_bytes) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    ClusterEpoch::INITIAL.get() as i64,
                    store.pg_id as i64,
                    applied_log_index as i64,
                    applied_log_index as i64,
                    applied_log_index as i64,
                    applied_log_index as i64,
                    vec![applied_log_index as u8],
                ],
            )
            .unwrap();
    }

    #[test]
    fn checkpoint_candidate_capture_checks_catalogue_above_requested_cutoff() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        for applied_log_index in 1..=METADATA_COMMAND_CHECKPOINT_CAPTURE_LIMIT {
            insert_test_checkpoint_row(&store, applied_log_index);
        }

        let error = store
            .metadata_command_checkpoint_candidate_rows(
                ClusterEpoch::INITIAL,
                METADATA_COMMAND_CHECKPOINT_RETAIN_PER_EPOCH as u64,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            StoreError::MetadataCheckpointInvalid {
                pg_id: 1,
                cluster_epoch: ClusterEpoch::INITIAL,
                reason,
                ..
            } if reason.contains("exceeds retained-row limit 8")
        ));
    }

    #[test]
    fn checkpoint_insert_rolls_back_when_atomic_pruning_fails() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        for applied_log_index in 1..=METADATA_COMMAND_CHECKPOINT_RETAIN_PER_EPOCH {
            insert_test_checkpoint_row(&store, applied_log_index);
        }
        store
            .conn
            .execute_batch(
                "CREATE TEMP TRIGGER fail_checkpoint_prune \
                 BEFORE DELETE ON metadata_command_checkpoints \
                 BEGIN \
                   SELECT RAISE(ABORT, 'injected checkpoint prune failure'); \
                 END",
            )
            .unwrap();
        let checkpoint = store
            .metadata_command_checkpoint(0, ClusterEpoch::INITIAL)
            .unwrap();

        let error = store
            .record_metadata_command_checkpoint(&checkpoint)
            .unwrap_err();

        assert!(matches!(
            error,
            StoreError::Db {
                context: "prune metadata command checkpoints",
                ..
            }
        ));
        let row_count = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM metadata_command_checkpoints",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(
            row_count,
            METADATA_COMMAND_CHECKPOINT_RETAIN_PER_EPOCH as i64
        );
        let inserted_checkpoint_count = store
            .conn
            .query_row(
                "SELECT COUNT(*) FROM metadata_command_checkpoints \
                 WHERE cluster_epoch = ?1 AND pg_id = ?2 AND applied_log_index = ?3 \
                   AND applied_log_hash = ?4 AND state_digest = ?5",
                rusqlite::params![
                    checkpoint.cluster_epoch.get() as i64,
                    checkpoint.pg_id.get() as i64,
                    checkpoint.applied_log_index as i64,
                    checkpoint.applied_log_hash.value() as i64,
                    checkpoint.state_digest.value() as i64,
                ],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(inserted_checkpoint_count, 0);
    }

    #[test]
    fn canonical_metadata_state_v5_digests_are_stable() {
        let table = &METADATA_DIGEST_TABLES[0];
        assert_eq!(table.name, "bucket_subresources");
        let values = vec![
            MetadataCheckpointValue::Text(b"bucket".to_vec()),
            MetadataCheckpointValue::Integer(7),
            MetadataCheckpointValue::Text(b"<Tagging/>".to_vec()),
            MetadataCheckpointValue::Integer(-2),
            MetadataCheckpointValue::Null,
        ];
        let row_digest = PgStore::metadata_checkpoint_row_digest(table, &values);

        let table_digest = metadata_table_digest_from_stats(
            table,
            MetadataTableDigestStats {
                row_count: 1,
                row_hash_xor: row_digest,
                row_hash_sum: row_digest,
            },
        );

        let mut state_hasher = checksum::crc64::Hasher::new();
        PgStore::digest_canonical_pg_state_header(&mut state_hasher);
        for (index, table) in METADATA_DIGEST_TABLES.iter().enumerate() {
            PgStore::digest_metadata_table_digest_entry(
                &mut state_hasher,
                table,
                0x1000_0000_0000_0000_u64 + index as u64,
            );
        }
        let state_digest = state_hasher.finalize();

        assert_eq!(
            (row_digest, table_digest, state_digest),
            (
                0x8e12_8dbf_237c_be5e,
                0x64a9_e3d9_b3ad_266d,
                0x7b41_179d_44a2_8eb5,
            )
        );
    }
}
