/// PgStore — per-PG shard I/O and object metadata, backed by filesystem + SQLite.
///
/// Owns a single `rusqlite::Connection` to the per-PG `metadata.db` and manages
/// shard files under the PG directory.
///
/// Directory layout:
/// ```text
/// pg-NNNN/
///   metadata.db
///   shards/<hex_prefix>/<shard_key_hex>
///   tmp/
/// ```
use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use rusqlite::types::ValueRef;
use rusqlite::{
    params, params_from_iter, Connection, Error as SqlError, OptionalExtension, Params, Row,
};

use crate::error::{BucketSnapshotLoadError, MetadataError, StoreError};
#[cfg(test)]
use crate::metadata_command::BucketPropertyMutation;
use crate::metadata_command::{
    abandoned_command_log_bytes, decode_metadata_command_envelope,
    decode_metadata_command_log_entry_header, metadata_command_log_hash,
    AbortMultipartUploadCommand, AbortStreamUploadCommand,
    AdvanceCompletedMultipartUploadSequenceCommand, AppendStreamSegmentCommand,
    BucketPropertyEffect, BucketRecord, BucketSubresourceMutation, CommitDirectPutObjectCommand,
    CommitMultipartObjectCommand, CommitStreamPartCommand, CreateBucketCommand,
    CreateMultipartUploadCommand, CreateStreamUploadCommand, DeleteCompletedMultipartUploadCommand,
    DeleteObjectPayloadReclaimCommand, DeleteObjectVersionCommand, DeleteObjectVersionTarget,
    InsertDeleteMarkerCommand, MarkBucketDeletingCommand, MetadataCommandAcceptance,
    MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogEntryKind,
    MetadataCommandLogIndex, MetadataCommandPayload, MetadataCommandReplicaState,
    ObjectPayloadReclaimCommand, PutBucketAclCommand, PutBucketPropertyCommand,
    PutBucketSubresourceCommand, PutBucketVersioningCommand, PutObjectMetadataCommand,
    ReleaseObjectGenerationCommand, ReserveObjectGenerationCommand, ReserveObjectVersionCommand,
};
use crate::schema::init_pg_schema;
use crate::traits::{PgMetadataStore, ShardStore};
use crate::types::*;

const TRACE_TARGET: &str = "storage";

const LIFECYCLE_SUBRESOURCE_KIND_SQL: i64 = BucketSubresourceKind::Lifecycle as u8 as i64;
const BUCKET_INFO_SELECT: &str = "\
SELECT name, owner_principal, owner_canonical_id, created_at, region, state, versioning, acl_grants, public_read, public_write, write_reservations_blocked, active_write_reservations, \
       public_access_block_present, public_access_block_block_public_acls, public_access_block_ignore_public_acls, public_access_block_block_public_policy, public_access_block_restrict_public_buckets, ownership_controls_mode, \
       EXISTS(SELECT 1 FROM bucket_subresources WHERE bucket_name = buckets.name AND kind = 4 AND body IS NOT NULL) AS bucket_policy_present, \
       bucket_policy_public, bucket_policy_generation, \
       EXISTS(SELECT 1 FROM bucket_subresources WHERE bucket_name = buckets.name AND kind = 5 AND body IS NOT NULL) AS bucket_lifecycle_present, \
       bucket_lifecycle_generation, bucket_execution_generation, bucket_abac_enabled, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years \
FROM buckets";
const BUCKET_INFO_BY_NAME_SELECT: &str = "\
SELECT name, owner_principal, owner_canonical_id, created_at, region, state, versioning, acl_grants, public_read, public_write, write_reservations_blocked, active_write_reservations, \
       public_access_block_present, public_access_block_block_public_acls, public_access_block_ignore_public_acls, public_access_block_block_public_policy, public_access_block_restrict_public_buckets, ownership_controls_mode, \
       EXISTS(SELECT 1 FROM bucket_subresources WHERE bucket_name = buckets.name AND kind = 4 AND body IS NOT NULL) AS bucket_policy_present, \
       bucket_policy_public, bucket_policy_generation, \
       EXISTS(SELECT 1 FROM bucket_subresources WHERE bucket_name = buckets.name AND kind = 5 AND body IS NOT NULL) AS bucket_lifecycle_present, \
       bucket_lifecycle_generation, bucket_execution_generation, bucket_abac_enabled, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years \
FROM buckets WHERE name = ?1";

const BUCKET_RECORD_BY_NAME_SELECT: &str = "\
SELECT name, owner_principal, owner_canonical_id, created_at, region, state, versioning, acl_grants, public_read, public_write, write_reservations_blocked, active_write_reservations, \
       public_access_block_present, public_access_block_block_public_acls, public_access_block_ignore_public_acls, public_access_block_block_public_policy, public_access_block_restrict_public_buckets, ownership_controls_mode, \
       bucket_policy_public, bucket_policy_generation, bucket_lifecycle_generation, bucket_execution_generation, completed_multipart_upload_sequence, bucket_abac_enabled, default_encryption_type, sse_c_blocked, \
       object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years \
FROM buckets WHERE name = ?1";

/// Part segment rows use a sentinel version_id during staging (pre-CompleteMultipartUpload).
/// Must differ from any real version_id (0 for unversioned, 1+ for versioned) so that
/// in-progress staging rows are invisible to reads of completed objects.
const PART_SEGMENT_STAGING_VERSION_ID: VersionId = MULTIPART_PART_SEGMENT_STAGING_VERSION_ID;
#[cfg(test)]
type StreamSessionRow = (u8, u8, BucketName, ObjectKey, Option<UploadId>, Option<i64>);

const METADATA_CANONICAL_STATE_ENCODING_VERSION: u8 = 1;
const METADATA_CANONICAL_PG_STATE_DOMAIN: &[u8] = b"argmin.metadata.pg-state";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataDigestFilter {
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
    /// Highest log index that can be compacted before, if compaction is safe.
    pub compactable_before: Option<u64>,
}

/// Result of attempting metadata command-log compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetadataCommandLogCompactionStatus {
    /// Compaction is disabled until checkpoint-backed equivalence exists.
    UnsupportedUntilCheckpoint { retained_entries: u64 },
}

fn decode_nonnegative_u64(context: &'static str, raw: i64) -> Result<u64, StoreError> {
    raw.try_into().map_err(|_| StoreError::Db {
        context,
        source: rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Integer,
            Box::from("negative integer where non-negative value was expected"),
        ),
    })
}

#[derive(Debug, Clone, Copy)]
struct MetadataDigestTable {
    name: &'static str,
    columns: &'static [&'static str],
    order_columns: &'static [&'static str],
    filter: MetadataDigestFilter,
}

impl MetadataDigestFilter {
    fn canonical_name(self) -> &'static str {
        match self {
            MetadataDigestFilter::AllRows => "all-rows",
        }
    }
}

const METADATA_DIGEST_TABLES: &[MetadataDigestTable] = &[
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
            "completed_multipart_upload_sequence",
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
        name: "completed_multipart_uploads",
        columns: &[
            "upload_id",
            "bucket",
            "key",
            "completion_order",
            "completed_at",
            "owner_principal",
            "owner_canonical_id",
            "initiator_principal",
            "initiator_canonical_id",
        ],
        order_columns: &["upload_id"],
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
            "etag",
            "etag_kind",
            "part_okh",
            "part_vid",
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
        columns: &[
            "bucket",
            "key",
            "generation_id",
            "part_number",
            "storage_kind",
            "part_okh",
            "part_vid",
            "data_pg_id",
            "ec_k",
            "ec_m",
        ],
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
        name: "object_parts",
        columns: &[
            "bucket",
            "key",
            "version_id",
            "part_number",
            "object_offset_start",
            "size",
            "etag",
            "etag_kind",
            "part_okh",
            "part_vid",
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
            "segment_okh",
            "segment_vid",
            "data_pg_id",
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

#[derive(Debug, Clone, Copy)]
enum BucketExecutionGeneration {
    #[cfg(test)]
    Allocate,
    Explicit(u64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketRecordUpdateEffect {
    State,
    Versioning,
    Acl,
    Property(BucketPropertyEffect),
}

fn bucket_property_stale_context(effect: BucketPropertyEffect) -> &'static str {
    match effect {
        BucketPropertyEffect::ObjectLock => "apply stale bucket object lock command",
        BucketPropertyEffect::Encryption => "apply stale bucket encryption command",
        BucketPropertyEffect::PublicAccessBlock => "apply stale bucket public access block command",
        BucketPropertyEffect::OwnershipControls => "apply stale bucket ownership controls command",
        BucketPropertyEffect::AbacEnabled => "apply stale bucket abac command",
    }
}

fn bucket_property_conflict_context(effect: BucketPropertyEffect) -> &'static str {
    match effect {
        BucketPropertyEffect::ObjectLock => "apply conflicting bucket object lock command",
        BucketPropertyEffect::Encryption => "apply conflicting bucket encryption command",
        BucketPropertyEffect::PublicAccessBlock => {
            "apply conflicting bucket public access block command"
        }
        BucketPropertyEffect::OwnershipControls => {
            "apply conflicting bucket ownership controls command"
        }
        BucketPropertyEffect::AbacEnabled => "apply conflicting bucket abac command",
    }
}

fn bucket_subresource_stale_context(mutation: &BucketSubresourceMutation) -> &'static str {
    match mutation {
        BucketSubresourceMutation::Put { .. } => "apply stale put bucket subresource command",
        BucketSubresourceMutation::Delete { .. } => "apply stale delete bucket subresource command",
    }
}

fn bucket_subresource_conflict_context(mutation: &BucketSubresourceMutation) -> &'static str {
    match mutation {
        BucketSubresourceMutation::Put { .. } => "apply conflicting put bucket subresource command",
        BucketSubresourceMutation::Delete { .. } => {
            "apply conflicting delete bucket subresource command"
        }
    }
}
type PublicAccessBlockSqlValues = (i64, i64, i64, i64, i64);
type BucketObjectLockSqlValues = (i64, Option<u8>, Option<i64>, Option<i64>);
type ObjectLockSqlValues = (Option<u8>, Option<i64>, u8);

#[cfg(test)]
fn trusted_bucket_name(name: impl Into<String>) -> BucketName {
    BucketName::try_from(name.into())
        .expect("pg_store must only construct BucketName from validated values")
}

fn bucket_not_found(name: &str) -> MetadataError {
    match BucketName::try_from(name) {
        Ok(name) => MetadataError::BucketNotFound { name },
        Err(error) => MetadataError::InvalidBucketName {
            reason: error.to_string(),
        },
    }
}

#[cfg(test)]
fn trusted_object_key(key: impl Into<String>) -> ObjectKey {
    ObjectKey::try_from(key.into())
        .expect("pg_store must only construct ObjectKey from validated values")
}

/// Feed a variable-length byte field into a canonical digest.
///
/// The length prefix is part of the canonical state encoding. Without it,
/// adjacent text/blob fields could be split differently while producing the
/// same byte stream.
fn digest_len_prefixed_bytes(hasher: &mut checksum::crc64::Hasher, bytes: &[u8]) {
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

fn register_metadata_digest_sql_functions(conn: &Connection) -> rusqlite::Result<()> {
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

fn install_sqlite_profile_hook(conn: &Connection) {
    let Ok(raw_threshold) = env::var("ARGMIN_SQLITE_PROFILE_MS") else {
        return;
    };
    let Ok(threshold_ms) = raw_threshold.parse::<u64>() else {
        eprintln!(
            "argmin_sqlite_profile invalid ARGMIN_SQLITE_PROFILE_MS={raw_threshold:?}; expected integer milliseconds"
        );
        return;
    };
    SQLITE_PROFILE_THRESHOLD_NANOS.store(threshold_ms.saturating_mul(1_000_000), Ordering::Relaxed);
    conn.trace_v2(
        TraceEventCodes::SQLITE_TRACE_PROFILE,
        Some(sqlite_profile_callback),
    );
}

fn sqlite_profile_callback(event: TraceEvent<'_>) {
    let TraceEvent::Profile(stmt, duration) = event else {
        return;
    };
    let threshold = SQLITE_PROFILE_THRESHOLD_NANOS.load(Ordering::Relaxed);
    if threshold == SQLITE_PROFILE_DISABLED || duration.as_nanos() < u128::from(threshold) {
        return;
    }
    eprintln!(
        "argmin_sqlite_profile duration_us={} sql=\"{}\"",
        duration.as_micros(),
        compact_sql_for_log(stmt.sql().as_ref())
    );
}

fn compact_sql_for_log(sql: &str) -> String {
    const MAX_LOGGED_SQL_BYTES: usize = 512;

    let mut compact = String::with_capacity(sql.len().min(MAX_LOGGED_SQL_BYTES));
    let mut last_was_space = false;
    for ch in sql.chars() {
        if compact.len() >= MAX_LOGGED_SQL_BYTES {
            compact.push_str("...");
            break;
        }
        if ch.is_whitespace() {
            if !last_was_space {
                compact.push(' ');
                last_was_space = true;
            }
        } else {
            compact.push(ch);
            last_was_space = false;
        }
    }
    compact.trim().to_owned()
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

/// Per-PG store combining shard file I/O with SQLite metadata.
pub struct PgStore {
    pg_id: u32,
    shards_dir: PathBuf,
    tmp_dir: PathBuf,
    conn: Connection,
    clean_metadata_digest_revision: AtomicU64,
    #[cfg(test)]
    metadata_command_log_prefix_fast_path_hits: AtomicU64,
}

const PG_STORE_STATEMENT_CACHE_CAPACITY: usize = 1024;
const SQLITE_PROFILE_DISABLED: u64 = u64::MAX;
const UNCLEAN_METADATA_DIGEST_REVISION: u64 = u64::MAX;
static SQLITE_PROFILE_THRESHOLD_NANOS: AtomicU64 = AtomicU64::new(SQLITE_PROFILE_DISABLED);

#[derive(Debug)]
struct MetadataCommandLogEntry {
    command_checksum: u64,
    command_bytes: Vec<u8>,
    abandoned: bool,
    previous_log_hash: Option<u64>,
    log_hash: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingMetadataCommandSlot {
    pub(crate) id: MetadataCommandId,
    pub(crate) command_checksum: u64,
    pub(crate) command_bytes: Vec<u8>,
    pub(crate) scope_bucket: Option<BucketName>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingMetadataCommandSlotAction {
    Unresolved,
    CleanTerminal,
    AdvanceAbandonedThenClean,
}

#[derive(Debug, Clone, Copy)]
struct MetadataCommandRecordResult {
    state: MetadataCommandReplicaState,
    digest_revision: u64,
}

#[derive(Debug, Clone, Copy)]
struct MetadataTableDigestStats {
    row_count: u64,
    row_hash_xor: u64,
    row_hash_sum: u64,
}

impl PgStore {
    /// Open (or create) a PG store at the given directory.
    ///
    /// Creates `shards/` and `tmp/` subdirectories if they don't exist.
    /// Initializes the SQLite schema (idempotent).
    pub fn open(pg_dir: &Path, pg_id: u32) -> Result<Self, StoreError> {
        let shards_dir = pg_dir.join("shards");
        let tmp_dir = pg_dir.join("tmp");

        fs::create_dir_all(&shards_dir).map_err(|e| StoreError::Io {
            context: "create shards dir",
            source: e,
        })?;
        fs::create_dir_all(&tmp_dir).map_err(|e| StoreError::Io {
            context: "create tmp dir",
            source: e,
        })?;

        let db_path = pg_dir.join("metadata.db");
        let conn = Connection::open(&db_path).map_err(|e| StoreError::Db {
            context: "open pg database",
            source: e,
        })?;
        conn.set_prepared_statement_cache_capacity(PG_STORE_STATEMENT_CACHE_CAPACITY);
        install_sqlite_profile_hook(&conn);

        conn.execute_batch("PRAGMA recursive_triggers = ON")
            .map_err(|e| StoreError::Db {
                context: "enable recursive pg database triggers",
                source: e,
            })?;
        register_metadata_digest_sql_functions(&conn).map_err(|e| StoreError::Db {
            context: "register metadata digest SQL functions",
            source: e,
        })?;
        init_pg_schema(&conn).map_err(|e| StoreError::Db {
            context: "init pg schema",
            source: e,
        })?;
        conn.busy_timeout(std::time::Duration::from_millis(0))
            .map_err(|e| StoreError::Db {
                context: "configure pg database busy timeout",
                source: e,
            })?;

        // Clean up any orphaned temp files from previous crashes.
        if let Ok(entries) = fs::read_dir(&tmp_dir) {
            for entry in entries.flatten() {
                let _ = fs::remove_file(entry.path());
            }
        }

        Ok(Self {
            pg_id,
            shards_dir,
            tmp_dir,
            conn,
            clean_metadata_digest_revision: AtomicU64::new(UNCLEAN_METADATA_DIGEST_REVISION),
            #[cfg(test)]
            metadata_command_log_prefix_fast_path_hits: AtomicU64::new(0),
        })
        .and_then(|store| {
            store.ensure_metadata_digest_bootstrap()?;
            store.ensure_metadata_command_replica_state()?;
            Ok(store)
        })
    }

    /// Return the PG ID.
    pub fn pg_id(&self) -> u32 {
        self.pg_id
    }

    /// Return a reference to the underlying SQLite connection.
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    fn query_row_cached<T, P, F>(
        &self,
        sql: &str,
        params: P,
        context: &'static str,
        f: F,
    ) -> Result<T, StoreError>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.conn
            .prepare_cached(sql)
            .and_then(|mut stmt| stmt.query_row(params, f))
            .map_err(|e| StoreError::Db { context, source: e })
    }

    fn query_row_cached_optional<T, P, F>(
        &self,
        sql: &str,
        params: P,
        context: &'static str,
        f: F,
    ) -> Result<Option<T>, StoreError>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.conn
            .prepare_cached(sql)
            .and_then(|mut stmt| stmt.query_row(params, f).optional())
            .map_err(|e| StoreError::Db { context, source: e })
    }

    fn execute_cached<P>(
        &self,
        sql: &str,
        params: P,
        context: &'static str,
    ) -> Result<usize, StoreError>
    where
        P: Params,
    {
        self.conn
            .prepare_cached(sql)
            .and_then(|mut stmt| stmt.execute(params))
            .map_err(|e| StoreError::Db { context, source: e })
    }

    fn execute_cached_metadata<P>(
        &self,
        sql: &str,
        params: P,
        context: &'static str,
    ) -> Result<usize, MetadataError>
    where
        P: Params,
    {
        self.conn
            .prepare_cached(sql)
            .and_then(|mut stmt| stmt.execute(params))
            .map_err(|e| MetadataError::Db { context, source: e })
    }

    fn query_row_cached_metadata<T, P, F>(
        &self,
        sql: &str,
        params: P,
        context: &'static str,
        f: F,
    ) -> Result<T, MetadataError>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.conn
            .prepare_cached(sql)
            .and_then(|mut stmt| stmt.query_row(params, f))
            .map_err(|e| MetadataError::Db { context, source: e })
    }

    fn query_row_cached_optional_metadata<T, P, F>(
        &self,
        sql: &str,
        params: P,
        context: &'static str,
        f: F,
    ) -> Result<Option<T>, MetadataError>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    {
        self.conn
            .prepare_cached(sql)
            .and_then(|mut stmt| stmt.query_row(params, f).optional())
            .map_err(|e| MetadataError::Db { context, source: e })
    }

    fn ensure_metadata_digest_bootstrap(&self) -> Result<(), StoreError> {
        if !self.metadata_digest_bootstrap_needs_repair()? {
            return Ok(());
        }

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| StoreError::Db {
                context: "begin metadata digest bootstrap",
                source: e,
            })?;
        let result = (|| {
            self.install_metadata_digest_triggers()?;
            self.refresh_all_metadata_table_digests()?;
            self.mark_metadata_digest_bootstrap_complete()
        })();
        match result {
            Ok(()) => self.conn.execute_batch("COMMIT").map_err(|e| {
                let _ = self.conn.execute_batch("ROLLBACK");
                StoreError::Db {
                    context: "commit metadata digest bootstrap",
                    source: e,
                }
            }),
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn metadata_digest_bootstrap_needs_repair(&self) -> Result<bool, StoreError> {
        if !self.metadata_digest_bootstrap_complete()? {
            return Ok(true);
        }
        for table in METADATA_DIGEST_TABLES {
            if !self.metadata_digest_row_exists(table)? {
                return Ok(true);
            }
            if !self.metadata_digest_triggers_complete(table)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn metadata_digest_bootstrap_complete(&self) -> Result<bool, StoreError> {
        self.query_row_cached_optional(
            "SELECT completed FROM metadata_digest_bootstrap_state WHERE singleton = 0",
            [],
            "check metadata digest bootstrap state",
            |row| row.get::<_, i64>(0),
        )
        .map(|completed| completed == Some(1))
    }

    fn mark_metadata_digest_bootstrap_complete(&self) -> Result<(), StoreError> {
        self.execute_cached(
            "INSERT INTO metadata_digest_bootstrap_state (singleton, completed) \
             VALUES (0, 1) \
             ON CONFLICT(singleton) DO UPDATE SET completed = excluded.completed",
            [],
            "mark metadata digest bootstrap complete",
        )
        .map(|_| ())
    }

    fn install_metadata_digest_triggers(&self) -> Result<(), StoreError> {
        for table in METADATA_DIGEST_TABLES {
            if !self.metadata_digest_row_exists(table)? {
                self.execute_cached(
                    "INSERT INTO metadata_table_digests \
                     (table_name, table_digest, row_count, row_hash_xor, row_hash_sum) \
                     VALUES (?1, ?2, 0, 0, 0) \
                     ON CONFLICT(table_name) DO NOTHING",
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
            }

            if !self.metadata_digest_triggers_complete(table)? {
                self.conn
                    .execute_batch(&self.metadata_digest_trigger_sql(table))
                    .map_err(|e| StoreError::Db {
                        context: "install metadata digest triggers",
                        source: e,
                    })?;
            }
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
			"CREATE TRIGGER IF NOT EXISTS metadata_digest_{name}_ai \
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
				 CREATE TRIGGER IF NOT EXISTS metadata_digest_{name}_ad \
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
				 CREATE TRIGGER IF NOT EXISTS metadata_digest_{name}_au \
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

    fn ensure_metadata_command_replica_state(&self) -> Result<(), StoreError> {
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
             (singleton, cluster_epoch, applied_log_index, applied_log_hash, state_digest) \
             VALUES (0, ?1, 0, 0, ?2)",
            params![ClusterEpoch::INITIAL.get() as i64, state_digest as i64],
            "initialize metadata command replica state",
        )?;
        Ok(())
    }

    fn metadata_command_replica_state_can_initialize(&self) -> Result<bool, StoreError> {
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
                    source: e,
                })?
                .is_some()
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn metadata_command_replica_state(
        &self,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let (cluster_epoch, applied_log_index, applied_log_hash, state_digest): (
            i64,
            i64,
            i64,
            i64,
        ) = self.query_row_cached(
            "SELECT cluster_epoch, applied_log_index, applied_log_hash, state_digest \
             FROM metadata_command_replica_state WHERE singleton = 0",
            [],
            "load metadata command replica state",
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;
        Ok(MetadataCommandReplicaState {
            cluster_epoch: ClusterEpoch::new(cluster_epoch as u64)
                .expect("metadata command replica state stores non-zero epoch"),
            applied_log_index: applied_log_index as u64,
            applied_log_hash: applied_log_hash as u64,
            state_digest: state_digest as u64,
        })
    }

    fn metadata_command_replica_state_with_digest_revision(
        &self,
    ) -> Result<(MetadataCommandReplicaState, u64), StoreError> {
        let (cluster_epoch, applied_log_index, applied_log_hash, state_digest, revision): (
            i64,
            i64,
            i64,
            i64,
            i64,
        ) = self.query_row_cached(
            "SELECT s.cluster_epoch, s.applied_log_index, s.applied_log_hash, s.state_digest, r.revision \
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
                ))
            },
        )?;
        let state = MetadataCommandReplicaState {
            cluster_epoch: ClusterEpoch::new(cluster_epoch as u64)
                .expect("metadata command replica state stores non-zero epoch"),
            applied_log_index: applied_log_index as u64,
            applied_log_hash: applied_log_hash as u64,
            state_digest: state_digest as u64,
        };
        let revision = decode_nonnegative_u64("decode metadata digest revision", revision)?;
        Ok((state, revision))
    }

    pub(crate) fn max_metadata_command_log_index(
        &self,
        cluster_epoch: ClusterEpoch,
    ) -> Result<u64, StoreError> {
        let raw = self.query_row_cached(
            "SELECT max(log_index) FROM metadata_command_log \
             WHERE cluster_epoch = ?1 AND pg_id = ?2",
            params![cluster_epoch.get() as i64, self.pg_id as i64],
            "load max metadata command log index",
            |row| row.get::<_, Option<i64>>(0),
        )?;
        raw.unwrap_or_default()
            .try_into()
            .map_err(|_| StoreError::Db {
                context: "decode max metadata command log index",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from("negative metadata command log index"),
                ),
            })
    }

    pub(crate) fn pending_metadata_command_slot(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<PendingMetadataCommandSlot>, StoreError> {
        let raw = self.query_row_cached_optional(
            "SELECT cluster_epoch, pg_id, log_index, command_checksum, command_bytes, scope_bucket \
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
                    row.get::<_, Option<String>>(5)?,
                ))
            },
        )?;
        let Some((
            raw_cluster_epoch,
            raw_pg_id,
            raw_log_index,
            raw_command_checksum,
            command_bytes,
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
        if stored_epoch != cluster_epoch {
            return Err(StoreError::StaleMetadataOperation {
                pg_id: self.pg_id,
                operation_epoch: cluster_epoch,
                current_epoch: stored_epoch,
            });
        }
        let stored_pg_id: u32 = raw_pg_id.try_into().map_err(|_| StoreError::Db {
            context: "decode pending slot PG id",
            source: rusqlite::Error::FromSqlConversionFailure(
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
                cluster_epoch,
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
                cluster_epoch,
                log_index: log_index.get(),
                stored_checksum: command_checksum,
                computed_checksum,
            });
        }
        let header = decode_metadata_command_log_entry_header(&command_bytes).map_err(|_| {
            StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                log_index: log_index.get(),
            }
        })?;
        let id = MetadataCommandId::new(cluster_epoch, PgId::new(self.pg_id), log_index);
        if header.id() != id || header.kind() != MetadataCommandLogEntryKind::Applied {
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                log_index: log_index.get(),
            });
        }
        let scope_bucket = raw_scope_bucket
            .map(BucketName::try_from)
            .transpose()
            .map_err(|reason| StoreError::Db {
                context: "decode pending slot scope bucket",
                source: rusqlite::Error::FromSqlConversionFailure(
                    5,
                    rusqlite::types::Type::Text,
                    Box::from(reason.to_string()),
                ),
            })?;
        Ok(Some(PendingMetadataCommandSlot {
            id,
            command_checksum,
            command_bytes,
            scope_bucket,
        }))
    }

    pub(crate) fn pending_metadata_command_envelope(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    ) -> Result<Option<MetadataCommandEnvelope>, StoreError> {
        let Some(slot) = self.pending_metadata_command_slot(node_id, cluster_epoch)? else {
            return Ok(None);
        };
        let command = decode_metadata_command_envelope(&slot.command_bytes).map_err(|_| {
            StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                log_index: slot.id.log_index().get(),
            }
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
            if command.bucket_name() != &scope_bucket {
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
        if command.id().pg_id().get() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id,
                command_pg_id: command.id().pg_id().get(),
                target_pg_id: self.pg_id,
                cluster_epoch: command.id().cluster_epoch(),
            });
        }
        let command_bytes = command.command_bytes();
        let inserted = self.execute_cached(
            "INSERT INTO metadata_command_pending_slot \
             (singleton, cluster_epoch, pg_id, log_index, command_checksum, command_bytes, scope_bucket) \
             VALUES (0, ?1, ?2, ?3, ?4, ?5, ?6) \
             ON CONFLICT(singleton) DO NOTHING",
            params![
                command.id().cluster_epoch().get() as i64,
                command.id().pg_id().get() as i64,
                command.id().log_index().get() as i64,
                command.checksum_crc64() as i64,
                command_bytes,
                scope_bucket.map(BucketName::as_str),
            ],
            "insert metadata command pending slot",
        )?;
        if inserted == 1 {
            return Ok(());
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
    }

    pub(crate) fn remove_pending_metadata_command_slot(
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
        let Some(entry) = self.load_metadata_command_log_entry(
            "load terminal metadata command log entry before pending slot removal",
            command.id().cluster_epoch(),
            command.id().pg_id(),
            command.id().log_index(),
        )?
        else {
            return Err(StoreError::MetadataCommandLogConflict {
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
    ) -> Result<bool, StoreError> {
        if expected.id().pg_id().get() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id,
                command_pg_id: expected.id().pg_id().get(),
                target_pg_id: self.pg_id,
                cluster_epoch: expected.id().cluster_epoch(),
            });
        }
        if replacement.id().pg_id().get() != self.pg_id {
            return Err(StoreError::MetadataCommandWrongPg {
                node_id,
                command_pg_id: replacement.id().pg_id().get(),
                target_pg_id: self.pg_id,
                cluster_epoch: replacement.id().cluster_epoch(),
            });
        }
        let Some(slot) =
            self.pending_metadata_command_slot(node_id, expected.id().cluster_epoch())?
        else {
            return Ok(false);
        };
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
        let expected_bytes = expected.command_bytes();
        let replacement_bytes = replacement.command_bytes();
        let updated = self.execute_cached(
            "UPDATE metadata_command_pending_slot \
             SET cluster_epoch = ?1, pg_id = ?2, log_index = ?3, \
                 command_checksum = ?4, command_bytes = ?5, scope_bucket = ?6 \
             WHERE singleton = 0 \
               AND cluster_epoch = ?7 \
               AND pg_id = ?8 \
               AND log_index = ?9 \
               AND command_checksum = ?10 \
               AND command_bytes = ?11",
            params![
                replacement.id().cluster_epoch().get() as i64,
                replacement.id().pg_id().get() as i64,
                replacement.id().log_index().get() as i64,
                replacement.checksum_crc64() as i64,
                replacement_bytes,
                scope_bucket.map(BucketName::as_str),
                expected.id().cluster_epoch().get() as i64,
                expected.id().pg_id().get() as i64,
                expected.id().log_index().get() as i64,
                expected.checksum_crc64() as i64,
                expected_bytes,
            ],
            "replace metadata command pending slot for reissue",
        )?;
        Ok(updated == 1)
    }

    pub(crate) fn validate_metadata_command_replay_state(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let mut state = self.metadata_command_replica_state()?;
        let pg_id = PgId::new(self.pg_id);
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
                    self.remove_pending_metadata_command_slot_exact(node_id, &slot)?;
                }
                PendingMetadataCommandSlotAction::AdvanceAbandonedThenClean => {
                    let record = self.advance_metadata_command_log_state_with_inserted(
                        node_id,
                        cluster_epoch,
                        None,
                    )?;
                    self.mark_metadata_state_digest_clean_at_revision(record.digest_revision);
                    state = record.state;
                    self.remove_pending_metadata_command_slot_exact(node_id, &slot)?;
                }
            }
        }
        let mut applied_log_hash = 0_u64;
        for raw_log_index in 1..=state.applied_log_index {
            let log_index = MetadataCommandLogIndex::new(raw_log_index)
                .expect("applied metadata command log index is non-zero");
            let Some(entry) = self.load_metadata_command_log_entry(
                "load metadata command log entry for replay validation",
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
                    if previous_log_hash == applied_log_hash && log_hash == expected_log_hash => {}
                (previous_log_hash, log_hash) => {
                    return Err(StoreError::MetadataCommandLogHashMismatch {
                        node_id,
                        pg_id: self.pg_id,
                        cluster_epoch,
                        log_index: raw_log_index,
                        expected_previous_log_hash: applied_log_hash,
                        actual_previous_log_hash: previous_log_hash.unwrap_or_default(),
                        expected_log_hash,
                        actual_log_hash: log_hash.unwrap_or_default(),
                    });
                }
            }
            applied_log_hash = expected_log_hash;
        }
        if applied_log_hash != state.applied_log_hash {
            return Err(StoreError::MetadataCommandLogHashMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch,
                log_index: state.applied_log_index,
                expected_previous_log_hash: applied_log_hash,
                actual_previous_log_hash: state.applied_log_hash,
                expected_log_hash: applied_log_hash,
                actual_log_hash: state.applied_log_hash,
            });
        }
        let actual_digest = self.metadata_state_digest()?;
        if state.state_digest != actual_digest {
            return Err(StoreError::MetadataStateDigestMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: state.cluster_epoch,
                expected_digest: state.state_digest,
                actual_digest,
            });
        }
        self.mark_metadata_state_digest_clean()?;
        Ok(state)
    }

    fn load_metadata_command_log_entry(
        &self,
        context: &'static str,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
        log_index: MetadataCommandLogIndex,
    ) -> Result<Option<MetadataCommandLogEntry>, StoreError> {
        self.query_row_cached_optional(
            "SELECT command_checksum, command_bytes, abandoned, previous_log_hash, log_hash \
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
                })
            },
        )
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
            return Err(StoreError::MetadataCommandLogConflict {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: state.cluster_epoch,
                log_index: slot_index,
            });
        };
        if self.pending_slot_terminal_entry_matches(node_id, slot, &entry)? {
            return Ok(PendingMetadataCommandSlotAction::CleanTerminal);
        }
        Err(StoreError::MetadataCommandLogConflict {
            node_id,
            pg_id: self.pg_id,
            cluster_epoch: state.cluster_epoch,
            log_index: slot_index,
        })
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
               AND command_bytes = ?5",
            params![
                slot.id.cluster_epoch().get() as i64,
                slot.id.pg_id().get() as i64,
                slot.id.log_index().get() as i64,
                slot.command_checksum as i64,
                slot.command_bytes.as_slice(),
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

        let Some(entry) = self.load_metadata_command_log_entry(
            "load metadata command log entry",
            command.id().cluster_epoch(),
            command.id().pg_id(),
            command.id().log_index(),
        )?
        else {
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
        let Some(entry) = self.load_metadata_command_log_entry(
            "load metadata command abandon log entry",
            command.id().cluster_epoch(),
            command.id().pg_id(),
            command.id().log_index(),
        )?
        else {
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
            return Ok(false);
        };
        self.metadata_command_log_entry_matches(node_id, command, &entry, true)
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

    fn record_metadata_command_applied_inner(
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
        let command_bytes = command.command_bytes();
        let inserted = self.execute_cached(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL, NULL) \
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
            (inserted > 0).then_some((command.id().log_index(), command.checksum_crc64()));
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
                source,
            })?;

        let result = self.record_metadata_command_abandoned_inner(node_id, command);
        match result {
            Ok(record) => {
                if let Err(source) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    self.invalidate_clean_metadata_digest_revision();
                    return Err(StoreError::Db {
                        context: "record abandoned metadata command (commit txn)",
                        source,
                    });
                }
                self.mark_metadata_state_digest_clean_at_revision(record.digest_revision);
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
        let command_bytes = command.abandoned_log_bytes();
        let command_checksum = command.abandoned_log_checksum_crc64();
        let inserted = self.execute_cached(
            "INSERT INTO metadata_command_log \
             (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL, NULL) \
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
        let inserted_entry = (inserted > 0).then_some((command.id().log_index(), command_checksum));
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
        let (raw_min, raw_max, raw_retained, raw_abandoned, raw_applied_entries) = self
            .query_row_cached(
                "SELECT min(log_index), max(log_index), count(*), \
                    coalesce(sum(abandoned), 0), \
                    coalesce(sum(CASE WHEN log_index <= ?3 THEN 1 ELSE 0 END), 0) \
                 FROM metadata_command_log \
                 WHERE cluster_epoch = ?1 AND pg_id = ?2",
                params![
                    cluster_epoch.get() as i64,
                    self.pg_id as i64,
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
        let missing_applied_prefix_entries =
            state.applied_log_index.saturating_sub(applied_entries);
        let pending_tail_entries = retained_entries.saturating_sub(applied_entries);

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
            compactable_before: None,
        })
    }

    pub fn compact_metadata_command_log_without_checkpoint(
        &self,
        cluster_epoch: ClusterEpoch,
    ) -> Result<MetadataCommandLogCompactionStatus, StoreError> {
        let stats = self.metadata_command_log_stats(cluster_epoch)?;
        Ok(
            MetadataCommandLogCompactionStatus::UnsupportedUntilCheckpoint {
                retained_entries: stats.retained_entries,
            },
        )
    }

    pub(crate) fn refresh_metadata_command_state_digest(&self) -> Result<(), StoreError> {
        self.refresh_all_metadata_table_digests()?;
        let state_digest = self.cached_metadata_state_digest()?;
        self.store_metadata_command_state_digest(state_digest)
    }

    fn store_metadata_command_state_digest(&self, state_digest: u64) -> Result<(), StoreError> {
        self.execute_cached(
            "UPDATE metadata_command_replica_state SET state_digest = ?1 WHERE singleton = 0",
            params![state_digest as i64],
            "refresh metadata command state digest",
        )?;
        self.mark_metadata_state_digest_clean()?;
        Ok(())
    }

    fn metadata_digest_revision(&self) -> Result<u64, StoreError> {
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

    fn invalidate_clean_metadata_digest_revision(&self) {
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
        if state.state_digest == actual_digest {
            self.clean_metadata_digest_revision
                .store(revision, Ordering::Relaxed);
            return Ok(None);
        }
        Ok(Some((
            state.cluster_epoch,
            state.state_digest,
            actual_digest,
        )))
    }

    fn advance_metadata_command_log_state_with_inserted(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        inserted_entry: Option<(MetadataCommandLogIndex, u64)>,
    ) -> Result<MetadataCommandRecordResult, StoreError> {
        let mut state = self.metadata_command_replica_state()?;
        if state.cluster_epoch != cluster_epoch {
            self.refresh_all_metadata_table_digests()?;
            state = MetadataCommandReplicaState {
                cluster_epoch,
                applied_log_index: 0,
                applied_log_hash: 0,
                state_digest: self.cached_metadata_state_digest()?,
            };
        }

        let pg_id = PgId::new(self.pg_id);
        let mut applied_log_index = state.applied_log_index;
        let mut applied_log_hash = state.applied_log_hash;
        if let Some((inserted_log_index, command_checksum)) = inserted_entry {
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
                     SET previous_log_hash = ?1, log_hash = ?2 \
                     WHERE cluster_epoch = ?3 AND pg_id = ?4 AND log_index = ?5 \
                       AND previous_log_hash IS NULL AND log_hash IS NULL",
                    params![
                        applied_log_hash as i64,
                        expected_log_hash as i64,
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
                applied_log_hash = expected_log_hash;
                #[cfg(test)]
                self.metadata_command_log_prefix_fast_path_hits
                    .fetch_add(1, Ordering::Relaxed);
                return self.update_metadata_command_replica_state(
                    cluster_epoch,
                    applied_log_index,
                    applied_log_hash,
                );
            }
        }

        while let Some(next_log_index) = applied_log_index.checked_add(1) {
            let log_index = MetadataCommandLogIndex::new(next_log_index)
                .expect("metadata command log index is non-zero");

            let expected_log_hash = if let Some((inserted_log_index, command_checksum)) =
                inserted_entry.filter(|(inserted_log_index, _)| *inserted_log_index == log_index)
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
                     SET previous_log_hash = ?1, log_hash = ?2 \
                     WHERE cluster_epoch = ?3 AND pg_id = ?4 AND log_index = ?5 \
                       AND previous_log_hash IS NULL AND log_hash IS NULL",
                    params![
                        applied_log_hash as i64,
                        expected_log_hash as i64,
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
                expected_log_hash
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
                                expected_log_hash as i64,
                                cluster_epoch.get() as i64,
                                self.pg_id as i64,
                                next_log_index as i64,
                            ],
                            "update metadata command log hash",
                        )?;
                    }
                    (Some(previous_log_hash), Some(log_hash))
                        if previous_log_hash == applied_log_hash
                            && log_hash == expected_log_hash => {}
                    (previous_log_hash, log_hash) => {
                        return Err(StoreError::MetadataCommandLogHashMismatch {
                            node_id,
                            pg_id: self.pg_id,
                            cluster_epoch,
                            log_index: next_log_index,
                            expected_previous_log_hash: applied_log_hash,
                            actual_previous_log_hash: previous_log_hash.unwrap_or_default(),
                            expected_log_hash,
                            actual_log_hash: log_hash.unwrap_or_default(),
                        });
                    }
                }
                expected_log_hash
            };
            applied_log_index = next_log_index;
            applied_log_hash = expected_log_hash;
        }

        self.update_metadata_command_replica_state(
            cluster_epoch,
            applied_log_index,
            applied_log_hash,
        )
    }

    fn update_metadata_command_replica_state(
        &self,
        cluster_epoch: ClusterEpoch,
        applied_log_index: u64,
        applied_log_hash: u64,
    ) -> Result<MetadataCommandRecordResult, StoreError> {
        let (state_digest, digest_revision) = self.cached_metadata_state_digest_with_revision()?;
        self.execute_cached(
            "UPDATE metadata_command_replica_state \
			 SET cluster_epoch = ?1, applied_log_index = ?2, applied_log_hash = ?3, state_digest = ?4 \
             WHERE singleton = 0",
            params![
                cluster_epoch.get() as i64,
                applied_log_index as i64,
                applied_log_hash as i64,
                state_digest as i64,
            ],
            "update metadata command replica state",
        )?;
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
                        source: rusqlite::Error::QueryReturnedNoRows,
                    })?;
            Self::digest_metadata_table_digest_entry(&mut hasher, table, table_digest);
        }
        Ok((hasher.finalize(), revision))
    }

    fn cached_metadata_table_digests_with_revision(
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
                source,
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
                source,
            })?;
        let mut digests = HashMap::new();
        let mut revision = None;
        for row in rows {
            let (table_name, table_digest, raw_revision) =
                row.map_err(|source| StoreError::Db {
                    context: "decode cached metadata table digest with revision",
                    source,
                })?;
            digests.insert(table_name, table_digest as u64);
            revision = Some(decode_nonnegative_u64(
                "decode metadata digest revision",
                raw_revision,
            )?);
        }
        let revision = revision.ok_or_else(|| StoreError::Db {
            context: "load metadata digest revision with cached table digests",
            source: rusqlite::Error::QueryReturnedNoRows,
        })?;
        Ok((digests, revision))
    }

    fn mark_metadata_state_digest_clean_at_revision(&self, revision: u64) {
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

    fn metadata_state_digest(&self) -> Result<u64, StoreError> {
        let mut hasher = checksum::crc64::Hasher::new();
        Self::digest_canonical_pg_state_header(&mut hasher);
        for table in METADATA_DIGEST_TABLES {
            let table_digest = self.metadata_table_digest(table)?;
            Self::digest_metadata_table_digest_entry(&mut hasher, table, table_digest);
        }
        Ok(hasher.finalize())
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

    fn metadata_table_digest(&self, table: &MetadataDigestTable) -> Result<u64, StoreError> {
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
            source: e,
        })?;
        let mut rows = stmt.query([]).map_err(|e| StoreError::Db {
            context: "scan canonical metadata table range rows",
            source: e,
        })?;
        let mut stats = MetadataTableDigestStats {
            row_count: 0,
            row_hash_xor: 0,
            row_hash_sum: 0,
        };
        while let Some(row) = rows.next().map_err(|e| StoreError::Db {
            context: "scan canonical metadata table range row",
            source: e,
        })? {
            let mut hasher = checksum::crc64::Hasher::new();
            digest_u8(&mut hasher, 0x20);
            digest_len_prefixed_bytes(&mut hasher, table.name.as_bytes());
            digest_u64(&mut hasher, table.columns.len() as u64);
            for index in 0..table.columns.len() {
                let value = row.get_ref(index).map_err(|e| StoreError::Db {
                    context: "read canonical metadata table range value",
                    source: e,
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
    fn cached_metadata_table_digest(&self, table: &MetadataDigestTable) -> Result<u64, StoreError> {
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

    fn cached_metadata_table_digests(&self) -> Result<HashMap<String, u64>, StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT table_name, table_digest FROM metadata_table_digests")
            .map_err(|e| StoreError::Db {
                context: "prepare cached metadata table digests",
                source: e,
            })?;
        let mut rows = stmt.query([]).map_err(|e| StoreError::Db {
            context: "load cached metadata table digests",
            source: e,
        })?;
        let mut digests = HashMap::with_capacity(METADATA_DIGEST_TABLES.len());
        while let Some(row) = rows.next().map_err(|e| StoreError::Db {
            context: "load cached metadata table digest row",
            source: e,
        })? {
            let table_name = row.get::<_, String>(0).map_err(|e| StoreError::Db {
                context: "decode cached metadata table digest name",
                source: e,
            })?;
            let table_digest = row.get::<_, i64>(1).map_err(|e| StoreError::Db {
                context: "decode cached metadata table digest",
                source: e,
            })? as u64;
            digests.insert(table_name, table_digest);
        }
        Ok(digests)
    }

    fn cached_metadata_state_digest(&self) -> Result<u64, StoreError> {
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
                        source: rusqlite::Error::QueryReturnedNoRows,
                    })?;
            Self::digest_metadata_table_digest_entry(&mut hasher, table, table_digest);
        }
        Ok(hasher.finalize())
    }

    fn digest_canonical_sql_value(hasher: &mut checksum::crc64::Hasher, value: ValueRef<'_>) {
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

    /// Resolve the file path for a shard key.
    fn shard_path(&self, key: &ShardKey) -> PathBuf {
        Self::shard_path_for_shards_dir(&self.shards_dir, key)
    }

    pub(crate) fn shard_path_for_shards_dir(shards_dir: &Path, key: &ShardKey) -> PathBuf {
        let hex = key.hex_bytes();
        let prefix =
            std::str::from_utf8(&hex[..SHARD_KEY_HEX_PREFIX_LEN]).expect("shard key hex is ASCII");
        let full = std::str::from_utf8(&hex).expect("shard key hex is ASCII");
        let mut path = shards_dir.to_path_buf();
        path.push(prefix);
        path.push(full);
        path
    }

    /// Get the current unix timestamp in seconds.
    fn now_secs() -> u64 {
        crate::clock::current_time_secs()
    }

    /// Get the current unix timestamp in milliseconds.
    #[cfg(any(test, feature = "test-hooks"))]
    fn now_millis() -> u64 {
        crate::clock::current_time_millis()
    }

    pub(crate) fn write_shard_file_durable(
        tmp_dir: &Path,
        shards_dir: &Path,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        let crc = checksum::crc64::checksum(data);
        let stored_size = data.len() as u64;

        // Write to temp file with O_EXCL (unique name via pid + timestamp).
        let tmp_name = format!("shard-{}-{}-{key}", std::process::id(), Self::now_secs());
        let tmp_path = tmp_dir.join(&tmp_name);

        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_path)
            .map_err(|e| StoreError::Io {
                context: "create temp shard file",
                source: e,
            })?;

        file.write_all(data).map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            StoreError::Io {
                context: "write shard data",
                source: e,
            }
        })?;

        // fdatasync the file data (sync_data = fdatasync on Linux).
        file.sync_data().map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            StoreError::Io {
                context: "fdatasync shard",
                source: e,
            }
        })?;

        // Ensure prefix subdirectory exists.
        let shard_path = Self::shard_path_for_shards_dir(shards_dir, key);
        if let Some(parent) = shard_path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                let _ = fs::remove_file(&tmp_path);
                StoreError::Io {
                    context: "create shard prefix dir",
                    source: e,
                }
            })?;
        }

        // Atomic rename.
        fs::rename(&tmp_path, &shard_path).map_err(|e| {
            let _ = fs::remove_file(&tmp_path);
            StoreError::Io {
                context: "rename shard into place",
                source: e,
            }
        })?;

        // fsync parent directory to ensure rename is durable.
        if let Some(parent) = shard_path.parent() {
            fsync_dir(parent).map_err(|e| StoreError::Io {
                context: "fsync shard parent dir",
                source: e,
            })?;
        }

        Ok(WriteAck {
            crc64: crc,
            stored_size,
        })
    }

    pub fn register_written_shard(&self, key: &ShardKey, ack: WriteAck) -> Result<(), StoreError> {
        let now = Self::now_secs();
        self.conn
            .execute(
                "INSERT OR REPLACE INTO shards (shard_key, data_size, crc64_nvme, created_at, status) \
                 VALUES (?1, ?2, ?3, ?4, 0)",
                params![
                    key.as_bytes().as_slice(),
                    ack.stored_size as i64,
                    ack.crc64 as i64,
                    now as i64
                ],
            )
            .map_err(|e| StoreError::Db {
                context: "insert shard record",
                source: e,
            })?;
        Ok(())
    }

    pub(crate) fn delete_shard_record(&self, key: &ShardKey) -> Result<(), StoreError> {
        self.conn
            .execute(
                "DELETE FROM shards WHERE shard_key = ?1",
                params![key.as_bytes().as_slice()],
            )
            .map_err(|e| StoreError::Db {
                context: "delete shard record",
                source: e,
            })?;
        Ok(())
    }

    pub fn register_written_shards_batch(
        &self,
        shards: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::register_written_shards_batch",
            "pg_id={} shards={}",
            self.pg_id,
            shards.len()
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| StoreError::Db {
                context: "register written shards batch (begin txn)",
                source: e,
            })?;

        let now = Self::now_secs() as i64;
        let result: Result<(), StoreError> = (|| {
            let mut stmt = self
                .conn
                .prepare_cached(
                    "INSERT OR REPLACE INTO shards (shard_key, data_size, crc64_nvme, created_at, status) \
                     VALUES (?1, ?2, ?3, ?4, 0)",
                )
                .map_err(|e| StoreError::Db {
                    context: "register written shards batch (prepare)",
                    source: e,
                })?;

            for (key, ack) in shards {
                stmt.execute(params![
                    key.as_bytes().as_slice(),
                    ack.stored_size as i64,
                    ack.crc64 as i64,
                    now,
                ])
                .map_err(|e| StoreError::Db {
                    context: "register written shards batch (insert shard record)",
                    source: e,
                })?;
            }

            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(StoreError::Db {
                        context: "register written shards batch (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn register_written_shards_and_append_stream_segment(
        &self,
        shards: &[(&ShardKey, WriteAck)],
        segment: &StreamUploadSegmentRecord,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::register_written_shards_and_append_stream_segment",
            "pg_id={} session_id={:?} segment_index={} shards={}",
            self.pg_id,
            segment.session_id,
            segment.segment_index,
            shards.len()
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "append stream segment with shard publish (begin txn)",
                source: e,
            })?;

        let now = Self::now_secs() as i64;
        let result: Result<(), MetadataError> = (|| {
            let mut shard_stmt = self
                .conn
                .prepare_cached(
                    "INSERT OR REPLACE INTO shards (shard_key, data_size, crc64_nvme, created_at, status) \
                     VALUES (?1, ?2, ?3, ?4, 0)",
                )
                .map_err(|e| MetadataError::Db {
                    context: "append stream segment with shard publish (prepare shard insert)",
                    source: e,
                })?;

            for (key, ack) in shards {
                shard_stmt
                    .execute(params![
                        key.as_bytes().as_slice(),
                        ack.stored_size as i64,
                        ack.crc64 as i64,
                        now,
                    ])
                    .map_err(|e| MetadataError::Db {
                        context: "append stream segment with shard publish (insert shard record)",
                        source: e,
                    })?;
            }

            self.conn
                .execute(
                    "INSERT INTO stream_upload_segments \
                     (session_id, segment_index, size, segment_crc64, segment_okh, segment_vid, data_pg_id, ec_k, ec_m) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        segment.session_id,
                        segment.segment_index,
                        segment.size as i64,
                        segment.segment_crc64.map(|v| v as i64),
                        segment.segment_okh.as_slice(),
                        segment.segment_vid.get() as i64,
                        segment.data_pg_id,
                        segment.ec_k,
                        segment.ec_m,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "append stream segment with shard publish (insert segment)",
                    source: e,
                })?;

            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "append stream segment with shard publish (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn row_to_object_part(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObjectPartRecord> {
        let part_okh = Self::blob_to_okh(row.get(7)?, 7)?;
        let checksum = row
            .get::<_, Option<Vec<u8>>>(12)?
            .map(|blob| Self::blob_to_checksum(blob, 12))
            .transpose()?;
        Ok(ObjectPartRecord {
            bucket: row.get(0)?,
            key: row.get(1)?,
            version_id: PgStore::parse_version_id(row.get::<_, i64>(2)?, 2)?,
            part_number: row.get::<_, i64>(3)? as u32,
            size: row.get::<_, i64>(4)? as u64,
            etag: row.get(5)?,
            etag_kind: Self::parse_enum(row.get::<_, u8>(6)?, 6, "etag_kind", EtagKind::from_u8)?,
            part_okh,
            part_vid: Self::parse_generation_id(row.get::<_, i64>(8)?, 8, "part_vid")?,
            ec_k: row.get::<_, u8>(9)?,
            ec_m: row.get::<_, u8>(10)?,
            data_pg_id: row.get::<_, i64>(11)? as u32,
            checksum,
        })
    }

    #[cfg(test)]
    fn row_to_object_part_range(
        row: &rusqlite::Row<'_>,
    ) -> rusqlite::Result<ObjectPartRangeRecord> {
        Ok(ObjectPartRangeRecord {
            part: Self::row_to_object_part(row)?,
            object_offset_start: row.get::<_, i64>(13)? as u64,
        })
    }

    /// Convert a BLOB to a 16-byte object key hash, failing on wrong length.
    fn blob_to_okh(blob: Vec<u8>, col: usize) -> Result<[u8; 16], rusqlite::Error> {
        let len = blob.len();
        blob.try_into().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col,
                rusqlite::types::Type::Blob,
                Box::from(format!("invalid part_okh length: {len} (expected 16)")),
            )
        })
    }

    /// Map a row with columns (upload_id, part_number, generation, size, etag,
    /// etag_kind, part_okh, part_vid, ec_k, ec_m, last_modified) to a
    /// MultipartPartRecord.
    fn row_to_multipart_part(
        row: &rusqlite::Row<'_>,
    ) -> Result<MultipartPartRecord, rusqlite::Error> {
        let part_okh = Self::blob_to_okh(row.get(6)?, 6)?;
        let checksum = row
            .get::<_, Option<Vec<u8>>>(11)?
            .map(|blob| Self::blob_to_checksum(blob, 11))
            .transpose()?;
        Ok(MultipartPartRecord {
            upload_id: row.get(0)?,
            part_number: row.get::<_, i64>(1)? as u32,
            generation: row.get::<_, i64>(2)? as u32,
            size: row.get::<_, i64>(3)? as u64,
            etag: row.get(4)?,
            etag_kind: Self::parse_enum(row.get::<_, u8>(5)?, 5, "etag_kind", EtagKind::from_u8)?,
            part_okh,
            part_vid: Self::parse_generation_id(row.get::<_, i64>(7)?, 7, "part_vid")?,
            ec_k: row.get::<_, u8>(8)?,
            ec_m: row.get::<_, u8>(9)?,
            last_modified: row.get::<_, i64>(10)? as u64,
            checksum,
        })
    }

    fn blob_to_checksum(
        blob: Vec<u8>,
        col: usize,
    ) -> Result<checksum::ChecksumBytes, rusqlite::Error> {
        let len = blob.len();
        checksum::ChecksumBytes::new(&blob).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col,
                rusqlite::types::Type::Blob,
                Box::from(format!("invalid checksum length: {len} (expected 1..=32)")),
            )
        })
    }

    /// Parse a 16-byte blob into a fixed-size array, returning a typed DB error
    /// instead of panicking if the length is wrong.
    fn parse_okh_blob(blob: &[u8], col_idx: usize) -> Result<[u8; 16], rusqlite::Error> {
        blob.try_into().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Blob,
                Box::from(format!("expected 16-byte segment_okh, got {}", blob.len())),
            )
        })
    }

    fn parse_generation_id(
        raw: i64,
        col_idx: usize,
        field_name: &'static str,
    ) -> Result<GenerationId, rusqlite::Error> {
        let value = u64::try_from(raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Integer,
                Box::from(format!("negative {field_name}: {raw}")),
            )
        })?;
        GenerationId::new(value).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid zero {field_name}")),
            )
        })
    }

    fn parse_stream_target(
        op_kind_raw: u8,
        upload_id: Option<UploadId>,
        part_number: Option<i64>,
        op_kind_col: usize,
    ) -> Result<StreamUploadTarget, rusqlite::Error> {
        let op_kind = StreamUploadKind::from_u8(op_kind_raw).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                op_kind_col,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid op_kind: {op_kind_raw}")),
            )
        })?;

        match op_kind {
            StreamUploadKind::PutObject => {
                if upload_id.is_some() || part_number.is_some() {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        op_kind_col,
                        rusqlite::types::Type::Integer,
                        Box::from("PutObject stream session must not carry upload_id/part_number"),
                    ));
                }
                Ok(StreamUploadTarget::PutObject)
            }
            StreamUploadKind::UploadPart => {
                let upload_id = upload_id.ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        op_kind_col,
                        rusqlite::types::Type::Integer,
                        Box::from("UploadPart stream session missing upload_id"),
                    )
                })?;
                let pn_i64 = part_number.ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        op_kind_col,
                        rusqlite::types::Type::Integer,
                        Box::from("UploadPart stream session missing part_number"),
                    )
                })?;
                if !(1..=10_000).contains(&pn_i64) {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        op_kind_col,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("invalid UploadPart part_number: {pn_i64}")),
                    ));
                }
                Ok(StreamUploadTarget::UploadPart {
                    upload_id,
                    part_number: pn_i64 as u32,
                })
            }
        }
    }

    fn parse_optional_u32(
        value: Option<i64>,
        col: usize,
        field: &str,
    ) -> Result<Option<u32>, rusqlite::Error> {
        value
            .map(|v| {
                u32::try_from(v).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        col,
                        rusqlite::types::Type::Integer,
                        Box::from(format!(
                            "invalid {field}: {v} (expected integer in 0..={})",
                            u32::MAX
                        )),
                    )
                })
            })
            .transpose()
    }

    fn parse_u32(value: i64, col: usize, field: &str) -> Result<u32, rusqlite::Error> {
        u32::try_from(value).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col,
                rusqlite::types::Type::Integer,
                Box::from(format!(
                    "invalid {field}: {value} (expected integer in 0..={})",
                    u32::MAX
                )),
            )
        })
    }

    fn parse_optional_u64(
        value: Option<i64>,
        col: usize,
        field: &str,
    ) -> Result<Option<u64>, rusqlite::Error> {
        value
            .map(|v| {
                u64::try_from(v).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        col,
                        rusqlite::types::Type::Integer,
                        Box::from(format!(
                            "invalid {field}: {v} (expected integer in 0..={})",
                            u64::MAX
                        )),
                    )
                })
            })
            .transpose()
    }

    fn parse_bucket_object_lock(
        (enabled_raw, default_mode_raw, default_days_raw, default_years_raw): BucketObjectLockSqlValues,
        [enabled_col, mode_col, days_col, years_col]: [usize; 4],
    ) -> Result<BucketObjectLockConfig, rusqlite::Error> {
        let enabled = match enabled_raw {
            0 => false,
            1 => true,
            _ => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    enabled_col,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("invalid object_lock_enabled: {enabled_raw}")),
                ));
            }
        };
        let default_mode = default_mode_raw
            .map(|raw| {
                Self::parse_enum(
                    raw,
                    mode_col,
                    "object_lock_default_mode",
                    ObjectLockMode::from_u8,
                )
            })
            .transpose()?;
        let default_days =
            Self::parse_optional_u32(default_days_raw, days_col, "object_lock_default_days")?;
        let default_years =
            Self::parse_optional_u32(default_years_raw, years_col, "object_lock_default_years")?;
        let default_retention = match (default_mode, default_days, default_years) {
            (None, None, None) => None,
            (Some(mode), Some(days), None) => Some(ObjectLockDefaultRetention {
                mode,
                period: RetentionPeriod::days(days).ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        days_col,
                        rusqlite::types::Type::Integer,
                        Box::from("object_lock_default_days must be > 0"),
                    )
                })?,
            }),
            (Some(mode), None, Some(years)) => Some(ObjectLockDefaultRetention {
                mode,
                period: RetentionPeriod::years(years).ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        years_col,
                        rusqlite::types::Type::Integer,
                        Box::from("object_lock_default_years must be > 0"),
                    )
                })?,
            }),
            _ => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    mode_col,
                    rusqlite::types::Type::Integer,
                    Box::from("invalid bucket object lock default retention state"),
                ));
            }
        };
        Ok(BucketObjectLockConfig {
            enabled,
            default_retention,
        })
    }

    fn parse_public_access_block(
        (
            present_raw,
            block_public_acls_raw,
            ignore_public_acls_raw,
            block_public_policy_raw,
            restrict_public_buckets_raw,
        ): PublicAccessBlockSqlValues,
        [present_col, block_public_acls_col, ignore_public_acls_col, block_public_policy_col, restrict_public_buckets_col]: [usize; 5],
    ) -> Result<Option<PublicAccessBlockConfig>, rusqlite::Error> {
        fn parse_flag(raw: i64, col: usize, field: &'static str) -> Result<bool, rusqlite::Error> {
            match raw {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(rusqlite::Error::FromSqlConversionFailure(
                    col,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("invalid {field}: {raw}")),
                )),
            }
        }

        if !parse_flag(present_raw, present_col, "public_access_block_present")? {
            return Ok(None);
        }

        Ok(Some(PublicAccessBlockConfig {
            block_public_acls: parse_flag(
                block_public_acls_raw,
                block_public_acls_col,
                "public_access_block_block_public_acls",
            )?,
            ignore_public_acls: parse_flag(
                ignore_public_acls_raw,
                ignore_public_acls_col,
                "public_access_block_ignore_public_acls",
            )?,
            block_public_policy: parse_flag(
                block_public_policy_raw,
                block_public_policy_col,
                "public_access_block_block_public_policy",
            )?,
            restrict_public_buckets: parse_flag(
                restrict_public_buckets_raw,
                restrict_public_buckets_col,
                "public_access_block_restrict_public_buckets",
            )?,
        }))
    }

    fn parse_ownership_controls(
        raw: Option<i64>,
        col: usize,
    ) -> Result<Option<BucketOwnershipControls>, rusqlite::Error> {
        let raw = match raw {
            Some(raw) => raw,
            None => return Ok(None),
        };
        let raw = u8::try_from(raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid ownership_controls_mode: {raw}")),
            )
        })?;
        let object_ownership = BucketObjectOwnership::from_u8(raw).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                col,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid ownership_controls_mode: {raw}")),
            )
        })?;
        Ok(Some(BucketOwnershipControls { object_ownership }))
    }

    fn parse_object_lock_state(
        retention_mode_raw: Option<u8>,
        retain_until_raw: Option<i64>,
        legal_hold_raw: u8,
        mode_col: usize,
        retain_until_col: usize,
        legal_hold_col: usize,
    ) -> Result<ObjectLockState, rusqlite::Error> {
        let retention_mode = retention_mode_raw
            .map(|raw| {
                Self::parse_enum(
                    raw,
                    mode_col,
                    "object_lock_retention_mode",
                    ObjectLockMode::from_u8,
                )
            })
            .transpose()?;
        let retain_until = Self::parse_optional_u64(
            retain_until_raw,
            retain_until_col,
            "object_lock_retain_until",
        )?;
        let legal_hold = Self::parse_enum(
            legal_hold_raw,
            legal_hold_col,
            "object_lock_legal_hold",
            StoredLegalHoldStatus::from_u8,
        )?;
        let retention = match (retention_mode, retain_until) {
            (None, None) => None,
            (Some(mode), Some(retain_until_unix_seconds)) if retain_until_unix_seconds > 0 => {
                Some(ObjectRetention {
                    retain_until_unix_seconds,
                    mode,
                })
            }
            (Some(_), Some(_)) => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    retain_until_col,
                    rusqlite::types::Type::Integer,
                    Box::from("object_lock_retain_until must be > 0"),
                ));
            }
            _ => {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    mode_col,
                    rusqlite::types::Type::Integer,
                    Box::from("invalid object lock retention state"),
                ));
            }
        };
        Ok(ObjectLockState {
            retention,
            legal_hold,
        })
    }

    fn bucket_object_lock_sql_values(
        config: BucketObjectLockConfig,
    ) -> Result<BucketObjectLockSqlValues, rusqlite::Error> {
        let enabled = i64::from(config.enabled);
        let (default_mode, default_days, default_years) = match config.default_retention {
            None => (None, None, None),
            Some(default_retention) => {
                let (days, years) = match default_retention.period {
                    RetentionPeriod::Days(days) => (Some(i64::from(days.get())), None),
                    RetentionPeriod::Years(years) => (None, Some(i64::from(years.get()))),
                };
                (Some(default_retention.mode as u8), days, years)
            }
        };
        Ok((enabled, default_mode, default_days, default_years))
    }

    fn object_lock_sql_values(
        object_lock: ObjectLockState,
    ) -> Result<ObjectLockSqlValues, rusqlite::Error> {
        let (retention_mode, retain_until) = match object_lock.retention {
            None => (None, None),
            Some(retention) => {
                let retain_until =
                    i64::try_from(retention.retain_until_unix_seconds).map_err(|_| {
                        rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::other(
                            format!(
                                "object lock retain-until exceeds SQLite INTEGER: {}",
                                retention.retain_until_unix_seconds
                            ),
                        )))
                    })?;
                (Some(retention.mode as u8), Some(retain_until))
            }
        };
        Ok((retention_mode, retain_until, object_lock.legal_hold as u8))
    }

    fn public_access_block_sql_values(
        config: Option<PublicAccessBlockConfig>,
    ) -> PublicAccessBlockSqlValues {
        match config {
            None => (0, 0, 0, 0, 0),
            Some(config) => (
                1,
                i64::from(config.block_public_acls),
                i64::from(config.ignore_public_acls),
                i64::from(config.block_public_policy),
                i64::from(config.restrict_public_buckets),
            ),
        }
    }

    fn ownership_controls_sql_value(config: Option<BucketOwnershipControls>) -> Option<i64> {
        config.map(|config| config.object_ownership as u8 as i64)
    }

    fn row_to_bucket_info(row: &rusqlite::Row<'_>) -> Result<BucketInfo, rusqlite::Error> {
        let owner_canonical_id_raw: String = row.get(2)?;
        let owner_canonical_id =
            Self::parse_canonical_user_id(owner_canonical_id_raw, 2, "owner_canonical_id")?;
        let public_access_block = Self::parse_public_access_block(
            (
                row.get::<_, i64>(12)?,
                row.get::<_, i64>(13)?,
                row.get::<_, i64>(14)?,
                row.get::<_, i64>(15)?,
                row.get::<_, i64>(16)?,
            ),
            [12, 13, 14, 15, 16],
        )?;
        let ownership_controls = Self::parse_ownership_controls(row.get(17)?, 17)?;
        let object_lock = Self::parse_bucket_object_lock(
            (
                row.get::<_, i64>(27)?,
                row.get::<_, Option<u8>>(28)?,
                row.get::<_, Option<i64>>(29)?,
                row.get::<_, Option<i64>>(30)?,
            ),
            [27, 28, 29, 30],
        )?;
        let acl_grants = Self::parse_acl_grants(row.get::<_, String>(7)?, 7, "acl_grants")?;
        Ok(BucketInfo {
            name: row.get(0)?,
            owner_principal: row.get(1)?,
            owner_canonical_id,
            created_at: row.get::<_, i64>(3)? as u64,
            region: row.get::<_, i64>(4)? as u16,
            state: Self::parse_enum(row.get::<_, u8>(5)?, 5, "state", BucketState::from_u8)?,
            versioning: Self::parse_enum(
                row.get::<_, u8>(6)?,
                6,
                "versioning",
                BucketVersioningState::from_u8,
            )?,
            object_lock,
            acl_grants,
            public_read: row.get::<_, i64>(8)? != 0,
            public_write: row.get::<_, i64>(9)? != 0,
            write_reservations_blocked: row.get::<_, i64>(10)? != 0,
            active_write_reservations: Self::parse_u32(
                row.get::<_, i64>(11)?,
                11,
                "active_write_reservations",
            )?,
            public_access_block,
            ownership_controls,
            bucket_policy_present: row.get::<_, i64>(18)? != 0,
            bucket_policy_public: row.get::<_, i64>(19)? != 0,
            bucket_policy_generation: row.get::<_, i64>(20)? as u64,
            bucket_lifecycle_present: row.get::<_, i64>(21)? != 0,
            bucket_lifecycle_generation: row.get::<_, i64>(22)? as u64,
            bucket_execution_generation: row.get::<_, i64>(23)? as u64,
            bucket_abac_enabled: row.get::<_, i64>(24)? != 0,
            encryption: BucketEncryptionConfig {
                default_encryption: row
                    .get::<_, Option<u8>>(25)?
                    .map(|value| {
                        ManagedEncryptionAlgorithm::from_u8(value).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                25,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid default_encryption_type: {value}")),
                            )
                        })
                    })
                    .transpose()?,
                sse_c_blocked: row.get::<_, i64>(26)? != 0,
            }
            .effective(),
        })
    }

    fn row_to_bucket_record(row: &rusqlite::Row<'_>) -> Result<BucketRecord, rusqlite::Error> {
        let owner_canonical_id_raw: String = row.get(2)?;
        let owner_canonical_id =
            Self::parse_canonical_user_id(owner_canonical_id_raw, 2, "owner_canonical_id")?;
        let public_access_block = Self::parse_public_access_block(
            (
                row.get::<_, i64>(12)?,
                row.get::<_, i64>(13)?,
                row.get::<_, i64>(14)?,
                row.get::<_, i64>(15)?,
                row.get::<_, i64>(16)?,
            ),
            [12, 13, 14, 15, 16],
        )?;
        let ownership_controls = Self::parse_ownership_controls(row.get(17)?, 17)?;
        let object_lock = Self::parse_bucket_object_lock(
            (
                row.get::<_, i64>(26)?,
                row.get::<_, Option<u8>>(27)?,
                row.get::<_, Option<i64>>(28)?,
                row.get::<_, Option<i64>>(29)?,
            ),
            [26, 27, 28, 29],
        )?;
        let acl_grants = Self::parse_acl_grants(row.get::<_, String>(7)?, 7, "acl_grants")?;
        Ok(BucketRecord {
            name: row.get(0)?,
            owner_principal: row.get(1)?,
            owner_canonical_id,
            created_at: row.get::<_, i64>(3)? as u64,
            region: row.get::<_, i64>(4)? as u16,
            state: Self::parse_enum(row.get::<_, u8>(5)?, 5, "state", BucketState::from_u8)?,
            versioning: Self::parse_enum(
                row.get::<_, u8>(6)?,
                6,
                "versioning",
                BucketVersioningState::from_u8,
            )?,
            object_lock,
            acl_grants,
            public_read: row.get::<_, i64>(8)? != 0,
            public_write: row.get::<_, i64>(9)? != 0,
            write_reservations_blocked: row.get::<_, i64>(10)? != 0,
            active_write_reservations: Self::parse_u32(
                row.get::<_, i64>(11)?,
                11,
                "active_write_reservations",
            )?,
            public_access_block,
            ownership_controls,
            bucket_policy_public: row.get::<_, i64>(18)? != 0,
            bucket_policy_generation: row.get::<_, i64>(19)? as u64,
            bucket_lifecycle_generation: row.get::<_, i64>(20)? as u64,
            bucket_execution_generation: row.get::<_, i64>(21)? as u64,
            completed_multipart_upload_sequence: row.get::<_, i64>(22)? as u64,
            bucket_abac_enabled: row.get::<_, i64>(23)? != 0,
            encryption: BucketEncryptionConfig {
                default_encryption: row
                    .get::<_, Option<u8>>(24)?
                    .map(|value| {
                        ManagedEncryptionAlgorithm::from_u8(value).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                24,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid default_encryption_type: {value}")),
                            )
                        })
                    })
                    .transpose()?,
                sse_c_blocked: row.get::<_, i64>(25)? != 0,
            },
        })
    }

    fn ensure_bucket_exists(&self, name: &str, context: &'static str) -> Result<(), MetadataError> {
        self.conn
            .query_row(
                "SELECT 1 FROM buckets WHERE name = ?1",
                params![name],
                |_| Ok(()),
            )
            .optional()
            .map_err(|source| MetadataError::Db { context, source })?
            .ok_or_else(|| bucket_not_found(name))
    }

    fn bucket_subresource_invalid_aux(
        kind: BucketSubresourceKind,
        aux: BucketSubresourceAux,
    ) -> MetadataError {
        MetadataError::Db {
            context: "put bucket subresource",
            source: rusqlite::Error::InvalidParameterName(format!(
                "{kind:?} does not support aux {aux:?}"
            )),
        }
    }

    fn bucket_subresource_aux_int_1_to_sql(aux: BucketSubresourceAux) -> Option<i64> {
        match aux {
            BucketSubresourceAux::None => None,
            BucketSubresourceAux::Policy { is_public, .. } => Some(i64::from(is_public)),
        }
    }

    fn bucket_subresource_aux_from_sql(
        kind: BucketSubresourceKind,
        raw_int_1: Option<i64>,
    ) -> Result<BucketSubresourceAux, rusqlite::Error> {
        match kind {
            BucketSubresourceKind::Policy => match raw_int_1 {
                Some(0 | 1) => Ok(BucketSubresourceAux::policy(raw_int_1 == Some(1))),
                Some(value) => Err(rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("invalid policy aux_int_1: {value}")),
                )),
                None => Err(rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Null,
                    Box::from("missing policy aux_int_1"),
                )),
            },
            _ => match raw_int_1 {
                None => Ok(BucketSubresourceAux::None),
                Some(value) => Err(rusqlite::Error::FromSqlConversionFailure(
                    2,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("unexpected aux_int_1 for {kind:?}: {value}")),
                )),
            },
        }
    }

    fn parse_bucket_subresource_generation(
        raw: i64,
        col_idx: usize,
    ) -> Result<u64, rusqlite::Error> {
        if raw < 0 {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Integer,
                Box::from(format!(
                    "invalid negative bucket subresource generation: {raw}"
                )),
            ));
        }
        Ok(raw as u64)
    }

    fn bucket_subresource_matches(
        &self,
        info: &BucketInfo,
        mutation: &BucketSubresourceMutation,
    ) -> Result<bool, MetadataError> {
        match mutation {
            BucketSubresourceMutation::Put { kind, body, aux } => {
                let Some(stored) =
                    self.get_bucket_subresource_internal(info.name.as_str(), *kind)?
                else {
                    return Ok(false);
                };
                if stored.body != *body || stored.aux != *aux {
                    return Ok(false);
                }
                Ok(match *kind {
                    BucketSubresourceKind::Policy => {
                        info.bucket_policy_present
                            && info.bucket_policy_public == aux.policy_is_public().unwrap()
                            && info.bucket_policy_generation == stored.generation.unwrap_or(0)
                    }
                    BucketSubresourceKind::Lifecycle => {
                        info.bucket_lifecycle_present
                            && info.bucket_lifecycle_generation == stored.generation.unwrap_or(0)
                    }
                    BucketSubresourceKind::Cors | BucketSubresourceKind::Tagging => true,
                })
            }
            BucketSubresourceMutation::Delete { kind } => {
                if self
                    .get_bucket_subresource_internal(info.name.as_str(), *kind)?
                    .is_some()
                {
                    return Ok(false);
                }
                Ok(match *kind {
                    BucketSubresourceKind::Policy => {
                        !info.bucket_policy_present && !info.bucket_policy_public
                    }
                    BucketSubresourceKind::Lifecycle => !info.bucket_lifecycle_present,
                    BucketSubresourceKind::Cors | BucketSubresourceKind::Tagging => true,
                })
            }
        }
    }

    fn put_bucket_subresource_inner(
        &self,
        name: &BucketName,
        mutation: &BucketSubresourceMutation,
        generation: BucketExecutionGeneration,
    ) -> Result<(), MetadataError> {
        if let BucketSubresourceMutation::Put { kind, aux, .. } = mutation {
            if !kind.supports_aux(*aux) {
                return Err(Self::bucket_subresource_invalid_aux(*kind, *aux));
            }
        }

        let info = self.head_bucket_raw(name)?;
        match generation {
            BucketExecutionGeneration::Explicit(explicit) => {
                if info.bucket_execution_generation == explicit {
                    if self.bucket_subresource_matches(&info, mutation)? {
                        return Ok(());
                    }
                    return Err(MetadataError::Db {
                        context: bucket_subresource_conflict_context(mutation),
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                if info.bucket_execution_generation > explicit {
                    return Err(MetadataError::Db {
                        context: bucket_subresource_stale_context(mutation),
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
            }
            #[cfg(test)]
            BucketExecutionGeneration::Allocate => {}
        }

        self.with_immediate_txn(
            "put bucket subresource command (begin txn)",
            "put bucket subresource command (commit txn)",
            |store| {
                store.ensure_bucket_exists(
                    name.as_str(),
                    "put bucket subresource command (check bucket exists)",
                )?;

                let (kind, policy_public, subresource_generation) = match mutation {
                    BucketSubresourceMutation::Put { kind, body, aux } => {
                        if !kind.supports_aux(*aux) {
                            return Err(Self::bucket_subresource_invalid_aux(*kind, *aux));
                        }
                        let policy_public = match kind {
                            BucketSubresourceKind::Policy => Some(aux.policy_is_public().unwrap()),
                            BucketSubresourceKind::Lifecycle
                            | BucketSubresourceKind::Cors
                            | BucketSubresourceKind::Tagging => None,
                        };
                        let subresource_generation = store
                            .query_row_cached_metadata(
                                "INSERT INTO bucket_subresources (bucket_name, kind, body, generation, aux_int_1) \
                                 VALUES (?1, ?2, ?3, 1, ?4) \
                                 ON CONFLICT(bucket_name, kind) DO UPDATE SET \
                                     body = excluded.body, \
                                     generation = bucket_subresources.generation + 1, \
                                     aux_int_1 = excluded.aux_int_1 \
                                 RETURNING generation",
                                params![
                                    name.as_str(),
                                    *kind as u8 as i64,
                                    body,
                                    Self::bucket_subresource_aux_int_1_to_sql(*aux),
                                ],
                                "put bucket subresource command (upsert subresource row)",
                                |row| row.get::<_, i64>(0),
                            )
                            .and_then(|raw| {
                                Self::parse_bucket_subresource_generation(raw, 0).map_err(
                                    |source| MetadataError::Db {
                                        context:
                                            "put bucket subresource command (parse generation)",
                                        source,
                                    },
                                )
                            })?;
                        (*kind, policy_public, subresource_generation)
                    }
                    BucketSubresourceMutation::Delete { kind } => {
                        let policy_public = match kind {
                            BucketSubresourceKind::Policy => Some(false),
                            BucketSubresourceKind::Lifecycle
                            | BucketSubresourceKind::Cors
                            | BucketSubresourceKind::Tagging => None,
                        };
                        let subresource_generation = store
                            .query_row_cached_metadata(
                                "INSERT INTO bucket_subresources (bucket_name, kind, body, generation, aux_int_1) \
                                 VALUES (?1, ?2, NULL, 1, NULL) \
                                 ON CONFLICT(bucket_name, kind) DO UPDATE SET \
                                     body = NULL, \
                                     generation = bucket_subresources.generation + 1, \
                                     aux_int_1 = NULL \
                                 RETURNING generation",
                                params![name.as_str(), *kind as u8 as i64],
                                "put bucket subresource command (tombstone subresource row)",
                                |row| row.get::<_, i64>(0),
                            )
                            .and_then(|raw| {
                                Self::parse_bucket_subresource_generation(raw, 0).map_err(
                                    |source| MetadataError::Db {
                                        context:
                                            "put bucket subresource command (parse generation)",
                                        source,
                                    },
                                )
                            })?;
                        (*kind, policy_public, subresource_generation)
                    }
                };

                let execution_generation = match generation {
                    #[cfg(test)]
                    BucketExecutionGeneration::Allocate => store
                        .next_bucket_execution_generation_in_txn(
                            "put bucket subresource command (allocate execution generation)",
                        )?,
                    BucketExecutionGeneration::Explicit(generation) => {
                        store.advance_bucket_execution_generation_in_txn(
                            generation,
                            "put bucket subresource command (advance execution generation)",
                        )?;
                        generation
                    }
                };
                let updated = match kind {
                    BucketSubresourceKind::Policy => store.execute_cached_metadata(
                        "UPDATE buckets \
                         SET bucket_policy_public = ?1, \
                             bucket_policy_generation = ?2, \
                             bucket_execution_generation = ?3 \
                         WHERE name = ?4",
                        params![
                            i32::from(policy_public.expect("policy commands set policy summary")),
                            subresource_generation as i64,
                            execution_generation as i64,
                            name.as_str(),
                        ],
                        "put bucket subresource command (update policy bucket mirrors)",
                    )?,
                    BucketSubresourceKind::Lifecycle => store.execute_cached_metadata(
                        "UPDATE buckets \
                         SET bucket_lifecycle_generation = ?1, \
                             bucket_execution_generation = ?2 \
                         WHERE name = ?3",
                        params![
                            subresource_generation as i64,
                            execution_generation as i64,
                            name.as_str(),
                        ],
                        "put bucket subresource command (update lifecycle bucket mirrors)",
                    )?,
                    BucketSubresourceKind::Cors | BucketSubresourceKind::Tagging => {
                        store.execute_cached_metadata(
                            "UPDATE buckets SET bucket_execution_generation = ?1 WHERE name = ?2",
                            params![execution_generation as i64, name.as_str()],
                            "put bucket subresource command (bump execution generation)",
                        )?
                    }
                };
                if updated == 0 {
                    return Err(bucket_not_found(name.as_str()));
                }

                Ok(())
            },
        )
    }

    #[cfg(test)]
    fn put_bucket_subresource_internal(
        &self,
        name: &BucketName,
        kind: BucketSubresourceKind,
        body: &str,
        aux: BucketSubresourceAux,
    ) -> Result<(), MetadataError> {
        self.put_bucket_subresource_inner(
            name,
            &BucketSubresourceMutation::Put {
                kind,
                body: body.to_owned(),
                aux,
            },
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn delete_bucket_subresource_internal(
        &self,
        name: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<(), MetadataError> {
        self.put_bucket_subresource_inner(
            name,
            &BucketSubresourceMutation::Delete { kind },
            BucketExecutionGeneration::Allocate,
        )
    }

    fn get_bucket_subresource_internal(
        &self,
        name: &str,
        kind: BucketSubresourceKind,
    ) -> Result<Option<StoredBucketSubresource>, MetadataError> {
        self.conn
            .query_row(
                "SELECT s.body, s.generation, s.aux_int_1 \
                 FROM buckets b \
                 LEFT JOIN bucket_subresources s \
                   ON s.bucket_name = b.name AND s.kind = ?2 \
                 WHERE b.name = ?1",
                params![name, kind as u8 as i64],
                |row| {
                    let body = row.get::<_, Option<String>>(0)?;
                    let generation = row.get::<_, Option<i64>>(1)?;
                    let aux_int_1 = row.get::<_, Option<i64>>(2)?;
                    match body {
                        Some(body) => Ok(Some(StoredBucketSubresource {
                            body,
                            generation: Some(Self::parse_bucket_subresource_generation(
                                generation.ok_or_else(|| {
                                    rusqlite::Error::FromSqlConversionFailure(
                                        1,
                                        rusqlite::types::Type::Null,
                                        Box::from("missing bucket subresource generation"),
                                    )
                                })?,
                                1,
                            )?),
                            aux: Self::bucket_subresource_aux_from_sql(kind, aux_int_1)?,
                        })),
                        None => Ok(None),
                    }
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket subresource",
                source: e,
            })?
            .ok_or_else(|| bucket_not_found(name))
    }

    /// Map a row with columns (bucket, key, version_id, generation_id, size,
    /// etag, etag_kind, last_modified, storage_class, ec_k, ec_m, status,
    /// tags, data_layout, parts_count, metadata_blob, system_metadata_blob,
    /// encryption_type, encryption_state, owner_principal, owner_canonical_id,
    /// acl_grants, public_read, object_lock_retention_mode,
    /// object_lock_retain_until, object_lock_legal_hold, became_noncurrent_at)
    /// to a StoredObject.
    fn parse_canonical_user_id(
        raw: String,
        col_idx: usize,
        field_name: &'static str,
    ) -> Result<CanonicalUserId, rusqlite::Error> {
        CanonicalUserId::parse_stored(&raw).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Text,
                Box::from(format!("invalid {field_name}: {raw}")),
            )
        })
    }

    fn parse_owner_identity(
        row: &rusqlite::Row<'_>,
        principal_col: usize,
        canonical_col: usize,
        principal_field: &'static str,
        canonical_field: &'static str,
    ) -> Result<OwnerIdentity, rusqlite::Error> {
        let principal: String = row.get(principal_col)?;
        if principal.is_empty() {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                principal_col,
                rusqlite::types::Type::Text,
                Box::from(format!("invalid empty {principal_field}")),
            ));
        }
        let canonical_raw: String = row.get(canonical_col)?;
        let canonical_id =
            Self::parse_canonical_user_id(canonical_raw, canonical_col, canonical_field)?;
        Ok(OwnerIdentity::new(principal, canonical_id))
    }

    fn parse_optional_owner_identity(
        row: &rusqlite::Row<'_>,
        principal_col: usize,
        canonical_col: usize,
        principal_field: &'static str,
        canonical_field: &'static str,
    ) -> Result<Option<OwnerIdentity>, rusqlite::Error> {
        let principal: Option<String> = row.get(principal_col)?;
        let canonical_raw: Option<String> = row.get(canonical_col)?;
        match (principal, canonical_raw) {
            (None, None) => Ok(None),
            (Some(principal), Some(canonical_raw)) => {
                if principal.is_empty() {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        principal_col,
                        rusqlite::types::Type::Text,
                        Box::from(format!("invalid empty {principal_field}")),
                    ));
                }
                let canonical_id =
                    Self::parse_canonical_user_id(canonical_raw, canonical_col, canonical_field)?;
                Ok(Some(OwnerIdentity::new(principal, canonical_id)))
            }
            _ => Err(rusqlite::Error::FromSqlConversionFailure(
                principal_col,
                rusqlite::types::Type::Text,
                Box::from(format!(
                    "inconsistent optional owner identity for {principal_field}/{canonical_field}"
                )),
            )),
        }
    }

    fn parse_acl_grants(
        raw: String,
        col_idx: usize,
        field_name: &'static str,
    ) -> Result<AclGrants, rusqlite::Error> {
        AclGrants::parse(&raw).map_err(|msg| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Text,
                Box::from(format!("invalid {field_name}: {msg}")),
            )
        })
    }

    /// Parse a u8-backed enum from a row column.
    fn parse_enum<T>(
        raw: u8,
        col_idx: usize,
        name: &str,
        from_u8: fn(u8) -> Option<T>,
    ) -> Result<T, rusqlite::Error> {
        from_u8(raw).ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Integer,
                Box::from(format!("invalid {name}: {raw}")),
            )
        })
    }

    /// Parse a version_id from a signed i64 column with checked conversion.
    fn parse_version_id(raw: i64, col_idx: usize) -> Result<VersionId, rusqlite::Error> {
        let v = u64::try_from(raw).map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                col_idx,
                rusqlite::types::Type::Integer,
                Box::from(format!("negative version_id: {raw}")),
            )
        })?;
        Ok(VersionId::from_u64(v))
    }

    fn parse_object_encryption(
        raw_type: u8,
        raw_state: Option<Vec<u8>>,
        type_col_idx: usize,
        state_col_idx: usize,
    ) -> Result<ObjectEncryption, rusqlite::Error> {
        let encryption_type = Self::parse_enum(
            raw_type,
            type_col_idx,
            "encryption_type",
            ObjectEncryptionType::from_u8,
        )?;
        ObjectEncryption::decode(encryption_type, raw_state).map_err(|msg| {
            rusqlite::Error::FromSqlConversionFailure(
                state_col_idx,
                rusqlite::types::Type::Blob,
                Box::from(msg),
            )
        })
    }

    fn row_to_object_record(row: &rusqlite::Row<'_>) -> Result<StoredObject, rusqlite::Error> {
        let status = Self::parse_enum(row.get::<_, u8>(11)?, 11, "status", ObjectState::from_u8)?;
        let bucket: BucketName = row.get(0)?;
        let key: ObjectKey = row.get(1)?;
        let version_id = Self::parse_version_id(row.get::<_, i64>(2)?, 2)?;
        let last_modified = row.get::<_, i64>(7)? as u64;
        let owner =
            Self::parse_owner_identity(row, 19, 20, "owner_principal", "owner_canonical_id")?;
        let acl_grants = Self::parse_acl_grants(row.get::<_, String>(21)?, 21, "acl_grants")?;
        let public_read = row.get::<_, i64>(22)? != 0;
        let object_lock = Self::parse_object_lock_state(
            row.get::<_, Option<u8>>(23)?,
            row.get::<_, Option<i64>>(24)?,
            row.get::<_, u8>(25)?,
            23,
            24,
            25,
        )?;
        let became_noncurrent_at =
            Self::parse_optional_u64(row.get::<_, Option<i64>>(26)?, 26, "became_noncurrent_at")?;

        match status {
            ObjectState::DeleteMarker => {
                let generation_id: Option<i64> = row.get(3)?;
                let size = row.get::<_, i64>(4)?;
                let etag: Vec<u8> = row.get(5)?;
                let etag_kind = row.get::<_, u8>(6)?;
                let storage_class = row.get::<_, u8>(8)?;
                let ec_k = row.get::<_, u8>(9)?;
                let ec_m = row.get::<_, u8>(10)?;
                let tags: Option<SerializedTagSet> = row
                    .get::<_, Option<String>>(12)?
                    .map(SerializedTagSet::from);
                let metadata_blob: Option<SerializedMetadataBlob> = row
                    .get::<_, Option<Vec<u8>>>(15)?
                    .map(SerializedMetadataBlob::from);
                let system_metadata_blob: Option<SerializedSystemMetadataBlob> = row
                    .get::<_, Option<Vec<u8>>>(16)?
                    .map(SerializedSystemMetadataBlob::from);
                let encryption = Self::parse_object_encryption(
                    row.get::<_, u8>(17)?,
                    row.get::<_, Option<Vec<u8>>>(18)?,
                    17,
                    18,
                )?;
                if size != 0
                    || generation_id.is_some()
                    || !etag.is_empty()
                    || etag_kind != 0
                    || storage_class != 0
                    || ec_k != 0
                    || ec_m != 0
                    || tags.is_some()
                    || metadata_blob.is_some()
                    || system_metadata_blob.is_some()
                    || encryption != ObjectEncryption::None
                    || !acl_grants.is_empty()
                    || public_read
                    || object_lock != ObjectLockState::default()
                {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        11,
                        rusqlite::types::Type::Integer,
                        Box::from("delete marker has non-canonical field values"),
                    ));
                }
                Ok(StoredObject::DeleteMarker(DeleteMarkerRecord {
                    bucket,
                    key,
                    version_id,
                    owner,
                    last_modified,
                }))
            }
            ObjectState::Live => {
                let generation_id =
                    Self::parse_generation_id(row.get::<_, i64>(3)?, 3, "generation_id")?;
                let etag_kind =
                    Self::parse_enum(row.get::<_, u8>(6)?, 6, "etag_kind", EtagKind::from_u8)?;
                let storage_class = Self::parse_enum(
                    row.get::<_, u8>(8)?,
                    8,
                    "storage_class",
                    StorageClass::from_u8,
                )?;
                let data_layout = Self::parse_enum(
                    row.get::<_, u8>(13)?,
                    13,
                    "data_layout",
                    DataLayout::from_u8,
                )?;
                let parts_count =
                    Self::parse_optional_u32(row.get::<_, Option<i64>>(14)?, 14, "parts_count")?;
                let layout = ObjectLayout::from_parts(data_layout, parts_count).map_err(|msg| {
                    rusqlite::Error::FromSqlConversionFailure(
                        14,
                        rusqlite::types::Type::Integer,
                        Box::from(msg),
                    )
                })?;

                let etag_bytes: Vec<u8> = row.get(5)?;
                let etag =
                    ObjectEtag::from_parts(&etag_bytes, etag_kind, parts_count).map_err(|msg| {
                        rusqlite::Error::FromSqlConversionFailure(
                            5,
                            rusqlite::types::Type::Blob,
                            Box::from(msg),
                        )
                    })?;
                let encryption = Self::parse_object_encryption(
                    row.get::<_, u8>(17)?,
                    row.get::<_, Option<Vec<u8>>>(18)?,
                    17,
                    18,
                )?;

                Ok(StoredObject::Live(LiveObjectRecord {
                    bucket,
                    key,
                    version_id,
                    owner,
                    acl_grants,
                    public_read,
                    generation_id,
                    size: row.get::<_, i64>(4)? as u64,
                    etag,
                    last_modified,
                    became_noncurrent_at,
                    storage_class,
                    ec: EcShape {
                        k: row.get::<_, u8>(9)?,
                        m: row.get::<_, u8>(10)?,
                    },
                    layout,
                    tags: row
                        .get::<_, Option<String>>(12)?
                        .map(SerializedTagSet::from),
                    metadata_blob: row
                        .get::<_, Option<Vec<u8>>>(15)?
                        .map(SerializedMetadataBlob::from),
                    system_metadata_blob: row
                        .get::<_, Option<Vec<u8>>>(16)?
                        .map(SerializedSystemMetadataBlob::from),
                    object_lock,
                    encryption,
                }))
            }
        }
    }
}

impl ShardStore for PgStore {
    fn write_shard(&self, key: &ShardKey, data: &[u8]) -> Result<WriteAck, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::write_shard",
            "pg_id={} shard={} bytes={}",
            self.pg_id,
            key,
            data.len()
        );
        let ack = Self::write_shard_file_durable(&self.tmp_dir, &self.shards_dir, key, data)?;
        self.register_written_shard(key, ack)?;
        Ok(ack)
    }

    fn read_shard(&self, key: &ShardKey) -> Result<ShardData, StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::read_shard",
            "pg_id={} shard={}",
            self.pg_id,
            key
        );
        // Look up shard in SQLite.
        let row: Option<(i64, i64, i64)> = self
            .conn
            .query_row(
                "SELECT crc64_nvme, data_size, status FROM shards WHERE shard_key = ?1",
                params![key.as_bytes().as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|e| StoreError::Db {
                context: "lookup shard",
                source: e,
            })?;

        let (expected_crc, _size, status) = row.ok_or(StoreError::NotFound)?;
        let expected_crc = expected_crc as u64;

        // Only serve live shards.
        if status != ShardStatus::Live as i64 {
            return Err(StoreError::NotFound);
        }

        // Read file.
        let shard_path = self.shard_path(key);
        let data = fs::read(&shard_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                return StoreError::NotFound;
            }
            StoreError::Io {
                context: "read shard file",
                source: e,
            }
        })?;

        // Verify CRC.
        let actual_crc = checksum::crc64::checksum(&data);
        if actual_crc != expected_crc {
            // Quarantine the shard.
            let _ = self.conn.execute(
                "UPDATE shards SET status = ?1 WHERE shard_key = ?2",
                params![ShardStatus::Quarantined as u8, key.as_bytes().as_slice()],
            );
            return Err(StoreError::IntegrityError {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        Ok(ShardData {
            data,
            crc64: actual_crc,
        })
    }

    fn delete_shard(&self, key: &ShardKey) -> Result<(), StoreError> {
        // Mark as deleting in SQLite.
        let updated = self
            .conn
            .execute(
                "UPDATE shards SET status = ?1 WHERE shard_key = ?2",
                params![ShardStatus::Deleting as u8, key.as_bytes().as_slice()],
            )
            .map_err(|e| StoreError::Db {
                context: "mark shard deleting",
                source: e,
            })?;

        // Unlink the file (ignore ENOENT for idempotency).
        let shard_path = self.shard_path(key);
        match fs::remove_file(&shard_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(StoreError::Io {
                    context: "unlink shard file",
                    source: e,
                });
            }
        }

        // Remove the SQLite record.
        if updated > 0 {
            self.conn
                .execute(
                    "DELETE FROM shards WHERE shard_key = ?1",
                    params![key.as_bytes().as_slice()],
                )
                .map_err(|e| StoreError::Db {
                    context: "delete shard record",
                    source: e,
                })?;
        }

        Ok(())
    }

    fn stat_shard(&self, key: &ShardKey) -> Result<ShardStat, StoreError> {
        let row: Option<(i64, i64, i64, Option<i64>, i64)> = self
            .conn
            .query_row(
                "SELECT data_size, crc64_nvme, created_at, last_verified, status \
                 FROM shards WHERE shard_key = ?1",
                params![key.as_bytes().as_slice()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| StoreError::Db {
                context: "stat shard",
                source: e,
            })?;

        let (size, crc, created_at, last_verified, status) = row.ok_or(StoreError::NotFound)?;

        if status != ShardStatus::Live as i64 {
            return Err(StoreError::NotFound);
        }

        Ok(ShardStat {
            size: size as u64,
            crc64: crc as u64,
            created_at: created_at as u64,
            last_verified: last_verified.map(|v| v as u64),
        })
    }
}

impl PgStore {
    fn with_immediate_txn<T>(
        &self,
        begin_context: &'static str,
        commit_context: &'static str,
        body: impl FnOnce(&Self) -> Result<T, MetadataError>,
    ) -> Result<T, MetadataError> {
        if !self.conn.is_autocommit() {
            return body(self);
        }

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| MetadataError::Db {
                context: begin_context,
                source,
            })?;
        let result = body(self);
        match result {
            Ok(value) => {
                self.conn
                    .execute_batch("COMMIT")
                    .map_err(|source| MetadataError::Db {
                        context: commit_context,
                        source,
                    })?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    #[cfg(test)]
    fn next_bucket_execution_generation_in_txn(
        &self,
        context: &'static str,
    ) -> Result<u64, MetadataError> {
        self.query_row_cached_metadata(
            "UPDATE pg_counters \
             SET next_bucket_execution_generation = next_bucket_execution_generation + 1 \
             WHERE singleton = 0 \
             RETURNING next_bucket_execution_generation",
            [],
            context,
            |row| row.get::<_, i64>(0),
        )
        .and_then(|raw| {
            raw.try_into().map_err(|_| MetadataError::Db {
                context: "decode next bucket execution generation",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from("negative next_bucket_execution_generation"),
                ),
            })
        })
    }

    pub(crate) fn next_bucket_execution_generation_candidate(&self) -> Result<u64, MetadataError> {
        let current: u64 = self
            .query_row_cached_metadata(
                "SELECT next_bucket_execution_generation FROM pg_counters WHERE singleton = 0",
                [],
                "read next bucket execution generation candidate",
                |row| row.get::<_, i64>(0),
            )
            .and_then(|raw| {
                raw.try_into().map_err(|_| MetadataError::Db {
                    context: "decode next bucket execution generation candidate",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("negative next_bucket_execution_generation"),
                    ),
                })
            })?;
        current.checked_add(1).ok_or_else(|| MetadataError::Db {
            context: "increment next bucket execution generation candidate",
            source: rusqlite::Error::ToSqlConversionFailure(Box::from(
                "bucket execution generation exceeds u64",
            )),
        })
    }

    fn advance_bucket_execution_generation_in_txn(
        &self,
        generation: u64,
        context: &'static str,
    ) -> Result<(), MetadataError> {
        let generation = i64::try_from(generation).map_err(|_| MetadataError::Db {
            context: "encode bucket execution generation",
            source: rusqlite::Error::ToSqlConversionFailure(Box::from(
                "bucket execution generation exceeds i64",
            )),
        })?;
        self.execute_cached_metadata(
            "UPDATE pg_counters \
             SET next_bucket_execution_generation = max(next_bucket_execution_generation, ?1) \
             WHERE singleton = 0",
            params![generation],
            context,
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub fn create_bucket_with_config(
        &self,
        config: &CreateBucketConfig<'_>,
    ) -> Result<(), MetadataError> {
        self.create_bucket_with_config_inner(
            config,
            PgStore::now_millis(),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn create_bucket_with_config_inner(
        &self,
        config: &CreateBucketConfig<'_>,
        created_at_millis: u64,
        generation: BucketExecutionGeneration,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_bucket_with_config",
            "pg_id={} bucket={:?} owner={} versioning={:?} object_lock_enabled={}",
            self.pg_id,
            config.name,
            config.owner_principal,
            config.versioning,
            config.object_lock.enabled
        );
        let created_at_millis =
            i64::try_from(created_at_millis).map_err(|_| MetadataError::Db {
                context: "create bucket (encode created_at)",
                source: rusqlite::Error::ToSqlConversionFailure(Box::from(
                    "bucket created_at exceeds i64",
                )),
            })?;
        let (
            object_lock_enabled,
            object_lock_default_mode,
            object_lock_default_days,
            object_lock_default_years,
        ) = Self::bucket_object_lock_sql_values(config.object_lock).map_err(|e| {
            MetadataError::Db {
                context: "create bucket (encode object lock)",
                source: e,
            }
        })?;
        self.with_immediate_txn(
            "create bucket (begin txn)",
            "create bucket (commit txn)",
            |store| {
                let generation = match generation {
                    #[cfg(test)]
                    BucketExecutionGeneration::Allocate => store
                        .next_bucket_execution_generation_in_txn(
                            "create bucket (allocate execution generation)",
                        )?,
                    BucketExecutionGeneration::Explicit(generation) => {
                        store.advance_bucket_execution_generation_in_txn(
                            generation,
                            "create bucket (advance execution generation)",
                        )?;
                        generation
                    }
                };
                match store.conn.execute(
                    "INSERT INTO buckets \
                     (name, owner_principal, owner_canonical_id, created_at, state, versioning, acl_grants, public_read, public_write, write_reservations_blocked, active_write_reservations, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years, bucket_execution_generation) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, 0, ?10, 1, ?11, ?12, ?13, ?14, ?15)",
                    params![
                        config.name,
                        config.owner_principal,
                        config.owner_canonical_id.as_str(),
                        created_at_millis,
                        BucketState::Active as u8,
                        config.versioning as u8 as i64,
                        config.acl_grants.serialized(),
                        i32::from(config.public_read),
                        i32::from(config.public_write),
                        Option::<u8>::None,
                        object_lock_enabled,
                        object_lock_default_mode,
                        object_lock_default_days,
                        object_lock_default_years,
                        generation as i64,
                    ],
                ) {
                    Ok(_) => Ok(()),
                    Err(rusqlite::Error::SqliteFailure(err, _))
                        if err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY =>
                    {
                        Err(MetadataError::BucketAlreadyExists)
                    }
                    Err(source) => Err(MetadataError::Db {
                        context: "create bucket",
                        source,
                    }),
                }
            },
        )
    }

    fn insert_bucket_record_explicit(&self, bucket: &BucketRecord) -> Result<(), MetadataError> {
        let bucket = bucket.clone().command_metadata_projection();
        let created_at = i64::try_from(bucket.created_at).map_err(|_| MetadataError::Db {
            context: "create bucket record (encode created_at)",
            source: rusqlite::Error::ToSqlConversionFailure(Box::from(
                "bucket created_at exceeds i64",
            )),
        })?;
        let completed_multipart_upload_sequence =
            i64::try_from(bucket.completed_multipart_upload_sequence).map_err(|_| {
                MetadataError::Db {
                    context: "create bucket record (encode completed multipart sequence)",
                    source: rusqlite::Error::ToSqlConversionFailure(Box::from(
                        "bucket completed multipart sequence exceeds i64",
                    )),
                }
            })?;
        let (
            object_lock_enabled,
            object_lock_default_mode,
            object_lock_default_days,
            object_lock_default_years,
        ) = Self::bucket_object_lock_sql_values(bucket.object_lock).map_err(|e| {
            MetadataError::Db {
                context: "create bucket record (encode object lock)",
                source: e,
            }
        })?;
        let (
            public_access_block_present,
            public_access_block_block_public_acls,
            public_access_block_ignore_public_acls,
            public_access_block_block_public_policy,
            public_access_block_restrict_public_buckets,
        ) = Self::public_access_block_sql_values(bucket.public_access_block);

        self.with_immediate_txn(
            "create bucket record (begin txn)",
            "create bucket record (commit txn)",
            |store| {
                store.advance_bucket_execution_generation_in_txn(
                    bucket.bucket_execution_generation,
                    "create bucket record (advance execution generation)",
                )?;
                match store.conn.execute(
                    "INSERT INTO buckets \
                     (name, owner_principal, owner_canonical_id, created_at, region, state, versioning, acl_grants, public_read, public_write, write_reservations_blocked, active_write_reservations, public_access_block_present, public_access_block_block_public_acls, public_access_block_ignore_public_acls, public_access_block_block_public_policy, public_access_block_restrict_public_buckets, ownership_controls_mode, bucket_policy_public, bucket_policy_generation, bucket_lifecycle_generation, bucket_execution_generation, completed_multipart_upload_sequence, bucket_abac_enabled, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30)",
                    params![
                        bucket.name.as_str(),
                        &bucket.owner_principal,
                        bucket.owner_canonical_id.as_str(),
                        created_at,
                        bucket.region as i64,
                        bucket.state as u8 as i64,
                        bucket.versioning as u8 as i64,
                        bucket.acl_grants.serialized(),
                        i32::from(bucket.public_read),
                        i32::from(bucket.public_write),
                        0_i32,
                        0_i64,
                        public_access_block_present,
                        public_access_block_block_public_acls,
                        public_access_block_ignore_public_acls,
                        public_access_block_block_public_policy,
                        public_access_block_restrict_public_buckets,
                        Self::ownership_controls_sql_value(bucket.ownership_controls),
                        i32::from(bucket.bucket_policy_public),
                        bucket.bucket_policy_generation as i64,
                        bucket.bucket_lifecycle_generation as i64,
                        bucket.bucket_execution_generation as i64,
                        completed_multipart_upload_sequence,
                        i32::from(bucket.bucket_abac_enabled),
                        bucket.encryption.default_encryption.map(|value| value as u8),
                        i32::from(bucket.encryption.sse_c_blocked),
                        object_lock_enabled,
                        object_lock_default_mode,
                        object_lock_default_days,
                        object_lock_default_years,
                    ],
                ) {
                    Ok(_) => Ok(()),
                    Err(rusqlite::Error::SqliteFailure(err, _))
                        if err.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY =>
                    {
                        Err(MetadataError::BucketAlreadyExists)
                    }
                    Err(source) => Err(MetadataError::Db {
                        context: "create bucket record",
                        source,
                    }),
                }
            },
        )
    }

    fn put_bucket_record_update_inner(
        &self,
        target: &BucketRecord,
        effect: BucketRecordUpdateEffect,
        stale_context: &'static str,
        conflict_context: &'static str,
    ) -> Result<(), MetadataError> {
        let current = self.head_bucket_record_raw(&target.name)?;
        if current.command_metadata_eq(target) {
            return Ok(());
        }
        if current.bucket_execution_generation == target.bucket_execution_generation {
            return Err(MetadataError::Db {
                context: conflict_context,
                source: rusqlite::Error::InvalidQuery,
            });
        }
        if current.bucket_execution_generation > target.bucket_execution_generation {
            return Err(MetadataError::Db {
                context: stale_context,
                source: rusqlite::Error::InvalidQuery,
            });
        }
        if !Self::bucket_record_preimage_matches_update(&current, target, effect) {
            return Err(MetadataError::Db {
                context: conflict_context,
                source: rusqlite::Error::InvalidQuery,
            });
        }
        if matches!(effect, BucketRecordUpdateEffect::Versioning)
            && target.versioning == BucketVersioningState::Disabled
            && current.versioning != BucketVersioningState::Disabled
        {
            return Err(MetadataError::InvalidVersioningTransition {
                from: current.versioning,
                to: target.versioning,
            });
        }

        self.with_immediate_txn(
            "put bucket record update (begin txn)",
            "put bucket record update (commit txn)",
            |store| {
                store.advance_bucket_execution_generation_in_txn(
                    target.bucket_execution_generation,
                    "put bucket record update (advance execution generation)",
                )?;
                let updated = match effect {
                    BucketRecordUpdateEffect::State => store
                        .conn
                        .execute(
                            "UPDATE buckets \
                             SET state = ?1, \
                                 bucket_execution_generation = ?2 \
                             WHERE name = ?3",
                            params![
                                target.state as u8 as i64,
                                target.bucket_execution_generation as i64,
                                target.name.as_str(),
                            ],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "put bucket state",
                            source,
                        })?,
                    BucketRecordUpdateEffect::Versioning => store
                        .conn
                        .execute(
                            "UPDATE buckets \
                             SET versioning = ?1, \
                                 bucket_execution_generation = ?2 \
                             WHERE name = ?3",
                            params![
                                target.versioning as u8 as i64,
                                target.bucket_execution_generation as i64,
                                target.name.as_str(),
                            ],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "put bucket versioning",
                            source,
                        })?,
                    BucketRecordUpdateEffect::Acl => store
                        .conn
                        .execute(
                            "UPDATE buckets \
                             SET acl_grants = ?1, \
                                 public_read = ?2, \
                                 public_write = ?3, \
                                 bucket_execution_generation = ?4 \
                             WHERE name = ?5",
                            params![
                                target.acl_grants.serialized(),
                                i32::from(target.public_read),
                                i32::from(target.public_write),
                                target.bucket_execution_generation as i64,
                                target.name.as_str(),
                            ],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "put bucket acl",
                            source,
                        })?,
                    BucketRecordUpdateEffect::Property(BucketPropertyEffect::ObjectLock) => {
                        let (enabled, default_mode, default_days, default_years) =
                            Self::bucket_object_lock_sql_values(target.object_lock).map_err(
                                |e| MetadataError::Db {
                                    context: "put bucket object lock (encode)",
                                    source: e,
                                },
                            )?;
                        store
                            .conn
                            .execute(
                                "UPDATE buckets \
                                 SET object_lock_enabled = ?1, \
                                     object_lock_default_mode = ?2, \
                                     object_lock_default_days = ?3, \
                                     object_lock_default_years = ?4, \
                                     bucket_execution_generation = ?5 \
                                 WHERE name = ?6",
                                params![
                                    enabled,
                                    default_mode,
                                    default_days,
                                    default_years,
                                    target.bucket_execution_generation as i64,
                                    target.name.as_str(),
                                ],
                            )
                            .map_err(|source| MetadataError::Db {
                                context: "put bucket object lock",
                                source,
                            })?
                    }
                    BucketRecordUpdateEffect::Property(BucketPropertyEffect::Encryption) => store
                        .conn
                        .execute(
                            "UPDATE buckets \
                             SET default_encryption_type = ?1, \
                                 sse_c_blocked = ?2, \
                                 bucket_execution_generation = ?3 \
                             WHERE name = ?4",
                            params![
                                target
                                    .encryption
                                    .default_encryption
                                    .map(|value| value as u8),
                                i32::from(target.encryption.sse_c_blocked),
                                target.bucket_execution_generation as i64,
                                target.name.as_str(),
                            ],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "put bucket encryption",
                            source,
                        })?,
                    BucketRecordUpdateEffect::Property(BucketPropertyEffect::PublicAccessBlock) => {
                        let (
                            present,
                            block_public_acls,
                            ignore_public_acls,
                            block_public_policy,
                            restrict_public_buckets,
                        ) = Self::public_access_block_sql_values(target.public_access_block);
                        store
                            .conn
                            .execute(
                                "UPDATE buckets SET \
                                     public_access_block_present = ?1, \
                                     public_access_block_block_public_acls = ?2, \
                                     public_access_block_ignore_public_acls = ?3, \
                                     public_access_block_block_public_policy = ?4, \
                                     public_access_block_restrict_public_buckets = ?5, \
                                     bucket_execution_generation = ?6 \
                                 WHERE name = ?7",
                                params![
                                    present,
                                    block_public_acls,
                                    ignore_public_acls,
                                    block_public_policy,
                                    restrict_public_buckets,
                                    target.bucket_execution_generation as i64,
                                    target.name.as_str(),
                                ],
                            )
                            .map_err(|source| MetadataError::Db {
                                context: "put bucket public access block",
                                source,
                            })?
                    }
                    BucketRecordUpdateEffect::Property(BucketPropertyEffect::OwnershipControls) => {
                        store
                            .conn
                            .execute(
                                "UPDATE buckets \
                                 SET ownership_controls_mode = ?1, \
                                     bucket_execution_generation = ?2 \
                                 WHERE name = ?3",
                                params![
                                    Self::ownership_controls_sql_value(target.ownership_controls),
                                    target.bucket_execution_generation as i64,
                                    target.name.as_str(),
                                ],
                            )
                            .map_err(|source| MetadataError::Db {
                                context: "put bucket ownership controls",
                                source,
                            })?
                    }
                    BucketRecordUpdateEffect::Property(BucketPropertyEffect::AbacEnabled) => store
                        .conn
                        .execute(
                            "UPDATE buckets \
                             SET bucket_abac_enabled = ?1, \
                                 bucket_execution_generation = ?2 \
                             WHERE name = ?3",
                            params![
                                i32::from(target.bucket_abac_enabled),
                                target.bucket_execution_generation as i64,
                                target.name.as_str(),
                            ],
                        )
                        .map_err(|source| MetadataError::Db {
                            context: "put bucket abac enabled",
                            source,
                        })?,
                };
                if updated == 0 {
                    return Err(bucket_not_found(target.name.as_str()));
                }
                Ok(())
            },
        )
    }

    fn bucket_record_preimage_matches_update(
        current: &BucketRecord,
        target: &BucketRecord,
        effect: BucketRecordUpdateEffect,
    ) -> bool {
        let mut expected = current.clone();
        expected.bucket_execution_generation = target.bucket_execution_generation;
        match effect {
            BucketRecordUpdateEffect::State => {
                expected.state = target.state;
            }
            BucketRecordUpdateEffect::Versioning => {
                expected.versioning = target.versioning;
            }
            BucketRecordUpdateEffect::Acl => {
                expected.acl_grants = target.acl_grants.clone();
                expected.public_read = target.public_read;
                expected.public_write = target.public_write;
            }
            BucketRecordUpdateEffect::Property(BucketPropertyEffect::ObjectLock) => {
                expected.object_lock = target.object_lock;
            }
            BucketRecordUpdateEffect::Property(BucketPropertyEffect::Encryption) => {
                expected.encryption = target.encryption;
            }
            BucketRecordUpdateEffect::Property(BucketPropertyEffect::PublicAccessBlock) => {
                expected.public_access_block = target.public_access_block;
            }
            BucketRecordUpdateEffect::Property(BucketPropertyEffect::OwnershipControls) => {
                expected.ownership_controls = target.ownership_controls;
            }
            BucketRecordUpdateEffect::Property(BucketPropertyEffect::AbacEnabled) => {
                expected.bucket_abac_enabled = target.bucket_abac_enabled;
            }
        }
        expected.command_metadata_eq(target)
    }

    pub(crate) fn apply_metadata_command(
        &self,
        command: &MetadataCommandEnvelope,
    ) -> Result<(), MetadataError> {
        if command.id().pg_id().get() != self.pg_id {
            return Err(MetadataError::Db {
                context: "apply metadata command PG mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        if !command.verify_checksum() {
            return Err(MetadataError::Db {
                context: "apply metadata command checksum",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        match command.payload() {
            MetadataCommandPayload::CreateBucket(create) => {
                self.apply_create_bucket_command(create)
            }
            MetadataCommandPayload::PutBucketVersioning(versioning) => {
                self.apply_put_bucket_versioning_command(versioning)
            }
            MetadataCommandPayload::PutBucketAcl(acl) => self.apply_put_bucket_acl_command(acl),
            MetadataCommandPayload::PutBucketProperty(property) => {
                self.apply_put_bucket_property_command(property)
            }
            MetadataCommandPayload::PutBucketSubresource(subresource) => {
                self.apply_put_bucket_subresource_command(subresource)
            }
            MetadataCommandPayload::MarkBucketDeleting(mark) => {
                self.apply_mark_bucket_deleting_command(mark)
            }
            MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(command) => {
                self.apply_advance_completed_multipart_upload_sequence_command(command)
            }
            MetadataCommandPayload::ReserveObjectGeneration(reservation) => {
                self.apply_reserve_object_generation_command(reservation)
            }
            MetadataCommandPayload::ReleaseObjectGeneration(reservation) => {
                self.apply_release_object_generation_command(reservation)
            }
            MetadataCommandPayload::ReserveObjectVersion(version) => {
                self.apply_reserve_object_version_command(version)
            }
            MetadataCommandPayload::CommitDirectPutObject(command) => {
                self.apply_commit_direct_put_object_command(command)
            }
            MetadataCommandPayload::CommitMultipartObject(command) => {
                self.apply_commit_multipart_object_command(command)
            }
            MetadataCommandPayload::DeleteObjectVersion(command) => {
                self.apply_delete_object_version_command(command)
            }
            MetadataCommandPayload::InsertDeleteMarker(command) => {
                self.apply_insert_delete_marker_command(command)
            }
            MetadataCommandPayload::PutObjectMetadata(command) => {
                self.apply_put_object_metadata_command(command)
            }
            MetadataCommandPayload::CreateStreamUpload(command) => {
                self.apply_create_stream_upload_command(command)
            }
            MetadataCommandPayload::AppendStreamSegment(command) => {
                self.apply_append_stream_segment_command(command)
            }
            MetadataCommandPayload::AbortStreamUpload(command) => {
                self.apply_abort_stream_upload_command(command)
            }
            MetadataCommandPayload::CommitStreamPart(command) => {
                self.apply_commit_stream_part_command(command)
            }
            MetadataCommandPayload::CreateMultipartUpload(command) => {
                self.apply_create_multipart_upload_command(command)
            }
            MetadataCommandPayload::AbortMultipartUpload(command) => {
                self.apply_abort_multipart_upload_command(command)
            }
            MetadataCommandPayload::DeleteObjectPayloadReclaim(command) => {
                self.apply_delete_object_payload_reclaim_command(command)
            }
            MetadataCommandPayload::DeleteCompletedMultipartUpload(command) => {
                self.apply_delete_completed_multipart_upload_command(command)
            }
        }
    }

    pub(crate) fn apply_metadata_command_and_record(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
    ) -> Result<MetadataCommandReplicaState, BucketSnapshotLoadError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|source| {
                BucketSnapshotLoadError::Metadata(MetadataError::Db {
                    context: "apply metadata command and record (begin txn)",
                    source,
                })
            })?;

        let result = (|| {
            self.apply_metadata_command(command)
                .map_err(BucketSnapshotLoadError::Metadata)?;
            self.record_metadata_command_applied_inner(node_id, command)
                .map_err(BucketSnapshotLoadError::Store)
        })();

        match result {
            Ok(record) => {
                if let Err(source) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    self.invalidate_clean_metadata_digest_revision();
                    return Err(BucketSnapshotLoadError::Metadata(MetadataError::Db {
                        context: "apply metadata command and record (commit txn)",
                        source,
                    }));
                }
                self.mark_metadata_state_digest_clean_at_revision(record.digest_revision);
                Ok(record.state)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                self.invalidate_clean_metadata_digest_revision();
                Err(error)
            }
        }
    }

    fn apply_create_bucket_command(
        &self,
        command: &CreateBucketCommand,
    ) -> Result<(), MetadataError> {
        match self.insert_bucket_record_explicit(&command.bucket) {
            Ok(()) => Ok(()),
            Err(MetadataError::BucketAlreadyExists) => {
                let existing = self.head_bucket_record_raw(&command.bucket.name)?;
                if existing.command_metadata_eq(&command.bucket) {
                    Ok(())
                } else {
                    Err(MetadataError::BucketAlreadyExists)
                }
            }
            Err(other) => Err(other),
        }
    }

    fn apply_put_bucket_versioning_command(
        &self,
        command: &PutBucketVersioningCommand,
    ) -> Result<(), MetadataError> {
        self.put_bucket_record_update_inner(
            &command.bucket,
            BucketRecordUpdateEffect::Versioning,
            "apply stale bucket versioning command",
            "apply conflicting bucket versioning command",
        )
    }

    fn apply_put_bucket_acl_command(
        &self,
        command: &PutBucketAclCommand,
    ) -> Result<(), MetadataError> {
        self.put_bucket_record_update_inner(
            &command.bucket,
            BucketRecordUpdateEffect::Acl,
            "apply stale bucket acl command",
            "apply conflicting bucket acl command",
        )
    }

    fn apply_mark_bucket_deleting_command(
        &self,
        command: &MarkBucketDeletingCommand,
    ) -> Result<(), MetadataError> {
        self.put_bucket_record_update_inner(
            &command.bucket,
            BucketRecordUpdateEffect::State,
            "apply stale mark bucket deleting command",
            "apply conflicting mark bucket deleting command",
        )
    }

    fn apply_put_bucket_property_command(
        &self,
        command: &PutBucketPropertyCommand,
    ) -> Result<(), MetadataError> {
        self.put_bucket_record_update_inner(
            &command.bucket,
            BucketRecordUpdateEffect::Property(command.effect),
            bucket_property_stale_context(command.effect),
            bucket_property_conflict_context(command.effect),
        )
    }

    fn apply_put_bucket_subresource_command(
        &self,
        command: &PutBucketSubresourceCommand,
    ) -> Result<(), MetadataError> {
        self.put_bucket_subresource_inner(
            &command.name,
            &command.mutation,
            BucketExecutionGeneration::Explicit(command.bucket_execution_generation),
        )
    }

    fn apply_advance_completed_multipart_upload_sequence_command(
        &self,
        command: &AdvanceCompletedMultipartUploadSequenceCommand,
    ) -> Result<(), MetadataError> {
        self.advance_completed_multipart_upload_sequence_for_bucket(
            &command.bucket,
            command.completion_order,
        )
    }

    fn apply_reserve_object_generation_command(
        &self,
        command: &ReserveObjectGenerationCommand,
    ) -> Result<(), MetadataError> {
        self.reserve_object_generation_explicit(
            &command.bucket,
            &command.key,
            &command.reservation_id,
            command.generation_id,
            command.created_at_millis,
        )
    }

    fn apply_release_object_generation_command(
        &self,
        command: &ReleaseObjectGenerationCommand,
    ) -> Result<(), MetadataError> {
        self.delete_object_generation_reservation_direct(
            &command.bucket,
            &command.key,
            &command.reservation_id,
        )
    }

    fn apply_reserve_object_version_command(
        &self,
        command: &ReserveObjectVersionCommand,
    ) -> Result<(), MetadataError> {
        self.reserve_object_version_explicit(&command.bucket, &command.key, command.version_id)
    }

    fn apply_commit_direct_put_object_command(
        &self,
        command: &CommitDirectPutObjectCommand,
    ) -> Result<(), MetadataError> {
        if self.direct_put_command_already_applied(command)? {
            return Ok(());
        }

        let reserved_generation = self.get_object_generation_reservation(
            &command.object.bucket,
            &command.object.key,
            &command.generation_reservation_id,
        )?;
        if reserved_generation != command.object.generation_id {
            return Err(MetadataError::Db {
                context: "commit direct put command reservation mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }

        self.with_immediate_txn(
            "commit direct put command (begin txn)",
            "commit direct put command (commit txn)",
            |store| {
                store.put_object_with_segments_explicit_in_open_txn(
                    &command.object,
                    &command.segments,
                    command.write_sequence,
                    command.last_modified_millis,
                )?;
                store.delete_object_generation_reservation_direct(
                    &command.object.bucket,
                    &command.object.key,
                    &command.generation_reservation_id,
                )?;
                if let Some(stale_payload) = &command.stale_payload {
                    store.apply_direct_put_stale_payload_in_open_txn(
                        &command.object.bucket,
                        &command.object.key,
                        command.object.version_id,
                        stale_payload,
                    )?;
                }
                store
                    .conn
                    .execute(
                        "DELETE FROM stream_uploads \
                     WHERE session_id = ?1 AND bucket = ?2 AND key = ?3",
                        params![
                            command.generation_reservation_id.as_str(),
                            &command.object.bucket,
                            &command.object.key
                        ],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "commit standard object command (delete stream staging)",
                        source: e,
                    })?;
                Ok(())
            },
        )
    }

    fn direct_put_command_already_applied(
        &self,
        command: &CommitDirectPutObjectCommand,
    ) -> Result<bool, MetadataError> {
        let stored = match self.get_object_version(
            &command.object.bucket,
            &command.object.key,
            command.object.version_id,
        ) {
            Ok(StoredObject::Live(record)) => record,
            Ok(StoredObject::DeleteMarker(_)) | Err(MetadataError::ObjectNotFound) => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        if stored.generation_id != command.object.generation_id
            || stored.size != command.object.size
            || stored.etag != command.object.etag
            || stored.last_modified != command.last_modified_millis
            || stored.ec != command.object.ec
            || stored.layout != command.object.layout
            || stored.tags != command.object.tags
            || stored.metadata_blob != command.object.metadata_blob
            || stored.system_metadata_blob != command.object.system_metadata_blob
            || stored.object_lock != command.object.object_lock
            || stored.encryption != command.object.encryption
            || stored.owner != command.object.owner
            || stored.acl_grants != command.object.acl_grants
            || stored.public_read != command.object.public_read
        {
            return Ok(false);
        }
        let segments = self.get_object_segments(
            &command.object.bucket,
            &command.object.key,
            command.object.version_id,
        )?;
        Ok(segments == command.segments)
    }

    fn apply_commit_multipart_object_command(
        &self,
        command: &CommitMultipartObjectCommand,
    ) -> Result<(), MetadataError> {
        if self.multipart_object_command_already_applied(command)? {
            return Ok(());
        }

        self.with_immediate_txn(
            "commit multipart object command (begin txn)",
            "commit multipart object command (commit txn)",
            |store| {
                let stream_uploads =
                    store.list_stream_uploads_for_multipart_upload(&command.upload_id)?;
                if stream_uploads != command.stream_uploads {
                    return Err(MetadataError::Db {
                        context: "commit multipart object command (stream uploads mismatch)",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                let stream_upload_segments =
                    store.list_stream_segments_for_sessions(&stream_uploads)?;
                if stream_upload_segments != command.stream_upload_segments {
                    return Err(MetadataError::Db {
                        context:
                            "commit multipart object command (stream upload segments mismatch)",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                if let Some(stale_payload) = &command.stale_payload {
                    store.apply_multipart_overwrite_stale_payload_in_open_txn(
                        &command.object.bucket,
                        &command.object.key,
                        command.object.version_id,
                        stale_payload,
                    )?;
                }
                store.put_multipart_object_explicit_in_open_txn(
                    &command.object,
                    &command.parts,
                    command.write_sequence,
                    command.last_modified_millis,
                )?;
                store.delete_multipart_part_segments_direct(
                    &command.object.bucket,
                    &command.object.key,
                    command.object.version_id,
                )?;
                store.delete_multipart_part_staging_segments_for_upload_in_open_txn(
                    &command.upload_id,
                )?;
                store.insert_multipart_part_segments_in_open_txn(
                    &command.object,
                    &command.selected_streaming_segments,
                )?;
                store.insert_completed_multipart_upload_in_open_txn(command)?;
                store.release_multipart_completion_reservation_in_open_txn(command)?;
                for session in &command.stream_uploads {
                    store.delete_stream_upload_direct(&session.session_id)?;
                }
                store.delete_multipart_upload_if_present_in_open_txn(&command.upload_id)?;
                Ok(())
            },
        )
    }

    fn multipart_object_command_already_applied(
        &self,
        command: &CommitMultipartObjectCommand,
    ) -> Result<bool, MetadataError> {
        let stored = match self.get_object_version(
            &command.object.bucket,
            &command.object.key,
            command.object.version_id,
        ) {
            Ok(StoredObject::Live(record)) => record,
            Ok(StoredObject::DeleteMarker(_)) | Err(MetadataError::ObjectNotFound) => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        if stored.generation_id != command.object.generation_id
            || stored.size != command.object.size
            || stored.etag != command.object.etag
            || stored.last_modified != command.last_modified_millis
            || stored.ec != command.object.ec
            || stored.layout != command.object.layout
            || stored.tags != command.object.tags
            || stored.metadata_blob != command.object.metadata_blob
            || stored.system_metadata_blob != command.object.system_metadata_blob
            || stored.object_lock != command.object.object_lock
            || stored.encryption != command.object.encryption
            || stored.owner != command.object.owner
            || stored.acl_grants != command.object.acl_grants
            || stored.public_read != command.object.public_read
        {
            return Ok(false);
        }
        let parts = self.get_object_parts(
            &command.object.bucket,
            &command.object.key,
            command.object.version_id,
        )?;
        if parts != command.parts {
            return Ok(false);
        }
        let mut streaming_segments = Vec::new();
        for part in &parts {
            if part.part_okh == [0u8; 16] {
                streaming_segments.extend(self.get_multipart_part_segments(
                    &command.object.bucket,
                    &command.object.key,
                    command.object.version_id,
                    part.part_number,
                )?);
            }
        }
        Ok(streaming_segments == command.selected_streaming_segments)
    }

    fn insert_multipart_part_segments_in_open_txn(
        &self,
        object: &PutLiveObjectReq,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "INSERT INTO multipart_part_segments \
                 (bucket, key, upload_id, version_id, part_number, segment_index, size, \
                  segment_crc64, segment_okh, segment_vid, data_pg_id, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            )
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (prepare insert part segments)",
                source: e,
            })?;

        for segment in segments {
            if segment.bucket != object.bucket
                || segment.key != object.key
                || segment.version_id != object.version_id.to_u64()
            {
                return Err(MetadataError::Db {
                    context: "commit multipart object command (segment object mismatch)",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::from("multipart part segment row does not match object identity"),
                    ),
                });
            }
            stmt.execute(params![
                &segment.bucket,
                &segment.key,
                segment.upload_id.as_str(),
                segment.version_id as i64,
                segment.part_number,
                segment.segment_index,
                segment.size as i64,
                segment.segment_crc64.map(|v| v as i64),
                segment.segment_okh.as_slice(),
                segment.segment_vid.get() as i64,
                segment.data_pg_id,
                segment.ec_k,
                segment.ec_m,
            ])
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (insert part segment)",
                source: e,
            })?;
        }

        Ok(())
    }

    fn delete_multipart_part_staging_segments_for_upload_in_open_txn(
        &self,
        upload_id: &UploadId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments \
                 WHERE upload_id = ?1 AND version_id = ?2",
                params![
                    upload_id.as_str(),
                    PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (delete staging segments)",
                source: e,
            })?;
        Ok(())
    }

    fn insert_completed_multipart_upload_in_open_txn(
        &self,
        command: &CommitMultipartObjectCommand,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO completed_multipart_uploads \
                 (upload_id, bucket, key, completion_order, completed_at, owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    command.upload_id.as_str(),
                    &command.object.bucket,
                    &command.object.key,
                    command.completion_order as i64,
                    command.completed_at_millis as i64,
                    &command.object.owner.principal,
                    command.object.owner.canonical_id.as_str(),
                    command
                        .initiator
                        .as_ref()
                        .map(|owner| owner.principal.as_str()),
                    command
                        .initiator
                        .as_ref()
                        .map(|owner| owner.canonical_id.as_str()),
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (insert completed upload)",
                source: e,
            })?;
        Ok(())
    }

    pub(crate) fn advance_completed_multipart_upload_sequence_for_bucket(
        &self,
        bucket: &BucketName,
        completion_order: u64,
    ) -> Result<(), MetadataError> {
        let completion_order = i64::try_from(completion_order).map_err(|_| MetadataError::Db {
            context: "commit multipart object command (completion order overflow)",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("completion_order exceeds SQLite integer range"),
            ),
        })?;
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET completed_multipart_upload_sequence = \
                     CASE \
                         WHEN completed_multipart_upload_sequence < ?2 THEN ?2 \
                         ELSE completed_multipart_upload_sequence \
                     END \
                 WHERE name = ?1",
                params![bucket, completion_order],
            )
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (advance completed upload sequence)",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(bucket.as_str()));
        }
        Ok(())
    }

    fn release_multipart_completion_reservation_in_open_txn(
        &self,
        command: &CommitMultipartObjectCommand,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_generation_reservations \
                 WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3 AND generation_id = ?4",
                params![
                    command.upload_id.as_str(),
                    &command.object.bucket,
                    &command.object.key,
                    command.object.generation_id.get() as i64,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (release generation reservation)",
                source: e,
            })?;
        Ok(())
    }

    fn delete_multipart_upload_if_present_in_open_txn(
        &self,
        upload_id: &UploadId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "commit multipart object command (delete upload)",
                source: e,
            })?;
        Ok(())
    }

    fn apply_direct_put_stale_payload_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        stale_payload: &ObjectPayloadReclaimCommand,
    ) -> Result<(), MetadataError> {
        match stale_payload {
            ObjectPayloadReclaimCommand::Segments(reclaim) => {
                self.put_object_segments_reclaim_in_open_txn(reclaim)
            }
            ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                self.put_multipart_reclaim_in_open_txn(reclaim)?;
                self.delete_multipart_part_segments_direct(bucket, key, version_id)?;
                self.delete_object_parts_direct(bucket, key, version_id)
            }
        }
    }

    fn apply_multipart_overwrite_stale_payload_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        stale_payload: &ObjectPayloadReclaimCommand,
    ) -> Result<(), MetadataError> {
        match stale_payload {
            ObjectPayloadReclaimCommand::Segments(reclaim) => {
                self.put_object_segments_reclaim_in_open_txn(reclaim)?;
                self.delete_object_segments_direct(bucket, key, version_id)
            }
            ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                self.put_multipart_reclaim_in_open_txn(reclaim)?;
                self.delete_multipart_part_segments_direct(bucket, key, version_id)?;
                self.delete_object_parts_direct(bucket, key, version_id)
            }
        }
    }

    fn validate_delete_payload_reclaim_command_root(
        command: &DeleteObjectPayloadReclaimCommand,
    ) -> Result<(), MetadataError> {
        let matches_root = match &command.payload {
            ObjectPayloadReclaimCommand::Segments(reclaim) => {
                reclaim.bucket == command.bucket
                    && reclaim.key == command.key
                    && reclaim.generation_id == command.generation_id
            }
            ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                reclaim.bucket == command.bucket
                    && reclaim.key == command.key
                    && reclaim.generation_id == command.generation_id
            }
        };
        if matches_root {
            Ok(())
        } else {
            Err(MetadataError::Db {
                context: "delete object payload reclaim command root mismatch",
                source: rusqlite::Error::InvalidQuery,
            })
        }
    }

    fn apply_delete_object_payload_reclaim_command(
        &self,
        command: &DeleteObjectPayloadReclaimCommand,
    ) -> Result<(), MetadataError> {
        Self::validate_delete_payload_reclaim_command_root(command)?;
        match &command.payload {
            ObjectPayloadReclaimCommand::Segments(expected) => {
                match self.get_object_segments_reclaim(
                    &command.bucket,
                    &command.key,
                    command.generation_id,
                )? {
                    Some(existing) if existing == *expected => self
                        .delete_object_segments_reclaim_direct(
                            &command.bucket,
                            &command.key,
                            command.generation_id,
                        ),
                    Some(_) => Err(MetadataError::Db {
                        context: "delete object payload reclaim command segment mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    }),
                    None => {
                        if self
                            .get_multipart_reclaim(
                                &command.bucket,
                                &command.key,
                                command.generation_id,
                            )?
                            .is_some()
                        {
                            return Err(MetadataError::Db {
                                context: "delete object payload reclaim command kind mismatch",
                                source: rusqlite::Error::InvalidQuery,
                            });
                        }
                        Ok(())
                    }
                }
            }
            ObjectPayloadReclaimCommand::Multipart(expected) => {
                match self.get_multipart_reclaim(
                    &command.bucket,
                    &command.key,
                    command.generation_id,
                )? {
                    Some(existing) if existing == *expected => self
                        .delete_multipart_reclaim_direct(
                            &command.bucket,
                            &command.key,
                            command.generation_id,
                        ),
                    Some(_) => Err(MetadataError::Db {
                        context: "delete object payload reclaim command multipart mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    }),
                    None => {
                        if self
                            .get_object_segments_reclaim(
                                &command.bucket,
                                &command.key,
                                command.generation_id,
                            )?
                            .is_some()
                        {
                            return Err(MetadataError::Db {
                                context: "delete object payload reclaim command kind mismatch",
                                source: rusqlite::Error::InvalidQuery,
                            });
                        }
                        Ok(())
                    }
                }
            }
        }
    }

    fn apply_delete_completed_multipart_upload_command(
        &self,
        command: &DeleteCompletedMultipartUploadCommand,
    ) -> Result<(), MetadataError> {
        match PgMetadataStore::get_completed_multipart_upload(self, &command.record.upload_id)? {
            Some(existing) if existing == command.record => {
                self.delete_completed_multipart_upload(&command.record.upload_id)
            }
            Some(_) => Err(MetadataError::Db {
                context: "delete completed multipart upload command row mismatch",
                source: rusqlite::Error::InvalidQuery,
            }),
            None => Ok(()),
        }
    }

    fn apply_delete_object_version_command(
        &self,
        command: &DeleteObjectVersionCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "delete object version command (begin txn)",
            "delete object version command (commit txn)",
            |store| {
                let stored = match store.get_object_version(
                    &command.bucket,
                    &command.key,
                    command.version_id,
                ) {
                    Ok(stored) => stored,
                    Err(MetadataError::ObjectNotFound) => return Ok(()),
                    Err(error) => return Err(error),
                };

                match (&command.target, stored) {
                    (DeleteObjectVersionTarget::DeleteMarker, StoredObject::DeleteMarker(_)) => {}
                    (
                        DeleteObjectVersionTarget::Live {
                            generation_id,
                            layout,
                            payload,
                        },
                        StoredObject::Live(record),
                    ) => {
                        if record.generation_id != *generation_id || record.layout != *layout {
                            return Err(MetadataError::Db {
                                context: "delete object version command target mismatch",
                                source: rusqlite::Error::InvalidQuery,
                            });
                        }
                        match payload {
                            ObjectPayloadReclaimCommand::Segments(reclaim) => {
                                store.put_object_segments_reclaim_in_open_txn(reclaim)?;
                                store.delete_object_segments_direct(
                                    &command.bucket,
                                    &command.key,
                                    command.version_id,
                                )?;
                            }
                            ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                                store.put_multipart_reclaim_in_open_txn(reclaim)?;
                                store.delete_multipart_part_segments_direct(
                                    &command.bucket,
                                    &command.key,
                                    command.version_id,
                                )?;
                                store.delete_object_parts_direct(
                                    &command.bucket,
                                    &command.key,
                                    command.version_id,
                                )?;
                            }
                        }
                    }
                    _ => {
                        return Err(MetadataError::Db {
                            context: "delete object version command kind mismatch",
                            source: rusqlite::Error::InvalidQuery,
                        });
                    }
                }

                store.delete_object_version_in_open_txn(
                    &command.bucket,
                    &command.key,
                    command.version_id,
                )
            },
        )
    }

    fn apply_insert_delete_marker_command(
        &self,
        command: &InsertDeleteMarkerCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "insert delete marker command (begin txn)",
            "insert delete marker command (commit txn)",
            |store| {
                match store.get_object_version(&command.bucket, &command.key, command.version_id) {
                    Ok(StoredObject::DeleteMarker(marker))
                        if marker.owner == command.owner
                            && marker.last_modified == command.last_modified_millis =>
                    {
                        return Ok(());
                    }
                    Ok(StoredObject::Live(_)) if command.version_id.is_null() => {}
                    Ok(_) => {
                        return Err(MetadataError::Db {
                            context: "insert delete marker command existing object mismatch",
                            source: rusqlite::Error::InvalidQuery,
                        });
                    }
                    Err(MetadataError::ObjectNotFound) => {}
                    Err(error) => return Err(error),
                }

                if let Some(stale_payload) = &command.stale_payload {
                    store.apply_multipart_overwrite_stale_payload_in_open_txn(
                        &command.bucket,
                        &command.key,
                        command.version_id,
                        stale_payload,
                    )?;
                }
                store.put_delete_marker_explicit_in_open_txn(command)
            },
        )
    }

    fn apply_put_object_metadata_command(
        &self,
        command: &PutObjectMetadataCommand,
    ) -> Result<(), MetadataError> {
        if self.put_object_metadata_command_already_applied(command)? {
            return Ok(());
        }
        self.with_immediate_txn(
            "put object metadata command (begin txn)",
            "put object metadata command (commit txn)",
            |store| store.put_object_metadata_explicit_in_open_txn(&command.object),
        )
    }

    fn put_object_metadata_command_already_applied(
        &self,
        command: &PutObjectMetadataCommand,
    ) -> Result<bool, MetadataError> {
        let stored = match self.get_object_version(
            &command.object.bucket,
            &command.object.key,
            command.object.version_id,
        ) {
            Ok(StoredObject::Live(record)) => record,
            Ok(StoredObject::DeleteMarker(_)) | Err(MetadataError::ObjectNotFound) => {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        Ok(command.matches_object(&stored))
    }

    fn put_object_metadata_explicit_in_open_txn(
        &self,
        object: &LiveObjectRecord,
    ) -> Result<(), MetadataError> {
        let stored = match self.get_object_version(&object.bucket, &object.key, object.version_id) {
            Ok(StoredObject::Live(record)) => record,
            Ok(StoredObject::DeleteMarker(_)) => {
                return Err(MetadataError::MethodNotAllowedOnDeleteMarker);
            }
            Err(error) => return Err(error),
        };
        if stored == *object {
            return Ok(());
        }
        if !Self::put_object_metadata_preimage_matches(&stored, object) {
            return Err(MetadataError::Db {
                context: "put object metadata command preimage mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }

        let tags = object.tags.as_ref().map(SerializedTagSet::as_str);
        let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
            Self::object_lock_sql_values(object.object_lock).map_err(|e| MetadataError::Db {
                context: "put object metadata command (encode object lock)",
                source: e,
            })?;
        let updated = self.execute_cached_metadata(
            "UPDATE objects \
             SET tags = ?1, acl_grants = ?2, public_read = ?3, \
                 object_lock_retention_mode = ?4, object_lock_retain_until = ?5, \
                 object_lock_legal_hold = ?6 \
             WHERE bucket = ?7 AND key = ?8 AND version_id = ?9 AND status = ?10",
            params![
                tags,
                object.acl_grants.serialized(),
                i32::from(object.public_read),
                object_lock_retention_mode,
                object_lock_retain_until,
                object_lock_legal_hold,
                &object.bucket,
                &object.key,
                object.version_id.to_u64() as i64,
                ObjectState::Live as u8,
            ],
            "put object metadata command",
        )?;
        if updated == 0 {
            return Err(MetadataError::ObjectNotFound);
        }
        Ok(())
    }

    fn put_object_metadata_preimage_matches(
        stored: &LiveObjectRecord,
        object: &LiveObjectRecord,
    ) -> bool {
        stored.bucket == object.bucket
            && stored.key == object.key
            && stored.version_id == object.version_id
            && stored.owner == object.owner
            && stored.generation_id == object.generation_id
            && stored.size == object.size
            && stored.etag == object.etag
            && stored.last_modified == object.last_modified
            && stored.became_noncurrent_at == object.became_noncurrent_at
            && stored.storage_class == object.storage_class
            && stored.ec == object.ec
            && stored.layout == object.layout
            && stored.metadata_blob == object.metadata_blob
            && stored.system_metadata_blob == object.system_metadata_blob
            && stored.encryption == object.encryption
    }

    fn create_stream_upload_explicit(
        &self,
        session: &StreamUploadRecord,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_stream_upload_explicit",
            "pg_id={} session_id={:?} bucket={:?} key={:?}",
            self.pg_id,
            session.session_id.as_str(),
            session.bucket.as_str(),
            session.key.as_str()
        );
        let op_kind = session.target.op_kind() as u8;
        let upload_id = session.target.upload_id();
        let part_number = session.target.part_number().map(|n| n as i64);
        let encryption_type = session.encryption.encryption_type() as u8;
        let encryption_state = session.encryption.encode_state();
        self.conn
            .execute(
                "INSERT INTO stream_uploads \
                 (session_id, bucket, key, op_kind, upload_id, part_number, state, created_at, encryption_type, encryption_state) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    session.session_id,
                    session.bucket,
                    session.key,
                    op_kind,
                    upload_id,
                    part_number,
                    session.state as u8,
                    session.created_at as i64,
                    encryption_type,
                    encryption_state,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "create stream upload explicit",
                source: e,
            })?;
        Ok(())
    }

    fn apply_create_stream_upload_command(
        &self,
        command: &CreateStreamUploadCommand,
    ) -> Result<(), MetadataError> {
        self.validate_create_stream_upload_command_target(command)?;
        match self.get_stream_upload(&command.session.session_id) {
            Ok(existing) if existing == command.session => Ok(()),
            Ok(_) => Err(MetadataError::Db {
                context: "create stream upload command existing session mismatch",
                source: rusqlite::Error::InvalidQuery,
            }),
            Err(MetadataError::StreamSessionNotFound { .. }) => {
                self.create_stream_upload_explicit(&command.session)
            }
            Err(error) => Err(error),
        }
    }

    fn validate_create_stream_upload_command_target(
        &self,
        command: &CreateStreamUploadCommand,
    ) -> Result<(), MetadataError> {
        match &command.session.target {
            StreamUploadTarget::PutObject => {
                if command.session.state == StreamUploadState::InProgress {
                    Ok(())
                } else {
                    Err(MetadataError::Db {
                        context: "create stream upload command state mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    })
                }
            }
            StreamUploadTarget::UploadPart { upload_id, .. } => {
                let upload = self.get_multipart_upload(upload_id)?;
                if upload.bucket != command.session.bucket
                    || upload.key != command.session.key
                    || upload.state != UploadState::InProgress
                {
                    return Err(MetadataError::NoSuchUpload {
                        upload_id: upload_id.to_string(),
                    });
                }
                if upload.encryption != command.session.encryption {
                    return Err(MetadataError::Db {
                        context: "create stream upload command upload encryption mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                if command.session.state == StreamUploadState::InProgress {
                    Ok(())
                } else {
                    Err(MetadataError::Db {
                        context: "create stream upload command state mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    })
                }
            }
        }
    }

    fn apply_append_stream_segment_command(
        &self,
        command: &AppendStreamSegmentCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "append stream segment command (begin txn)",
            "append stream segment command (commit txn)",
            |store| {
                let session = store.get_stream_upload(&command.segment.session_id)?;
                if session.bucket != command.bucket || session.key != command.key {
                    return Err(MetadataError::Db {
                        context: "append stream segment command session binding mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                if session.state != StreamUploadState::InProgress {
                    return Err(MetadataError::StreamSessionNotInProgress {
                        state: session.state as u8,
                    });
                }
                if let StreamUploadTarget::UploadPart { upload_id, .. } = &session.target {
                    let upload = store.get_multipart_upload(upload_id)?;
                    if upload.bucket != command.bucket
                        || upload.key != command.key
                        || upload.state != UploadState::InProgress
                    {
                        return Err(MetadataError::NoSuchUpload {
                            upload_id: upload_id.to_string(),
                        });
                    }
                }

                let existing = store
                    .list_stream_segments(&command.segment.session_id)?
                    .into_iter()
                    .find(|segment| segment.segment_index == command.segment.segment_index);
                match existing {
                    Some(existing) if existing == command.segment => Ok(()),
                    Some(_) => Err(MetadataError::Db {
                        context: "append stream segment command existing segment mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    }),
                    None => store.append_stream_segment_direct(&command.segment),
                }
            },
        )
    }

    fn apply_abort_stream_upload_command(
        &self,
        command: &AbortStreamUploadCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "abort stream upload command (begin txn)",
            "abort stream upload command (commit txn)",
            |store| {
                let session = match store.get_stream_upload(&command.session_id) {
                    Ok(session) => session,
                    Err(MetadataError::StreamSessionNotFound { .. }) => return Ok(()),
                    Err(error) => return Err(error),
                };
                if session.bucket != command.bucket || session.key != command.key {
                    return Err(MetadataError::Db {
                        context: "abort stream upload command session binding mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                if session.state != StreamUploadState::InProgress {
                    return Err(MetadataError::StreamSessionNotInProgress {
                        state: session.state as u8,
                    });
                }
                let staged_segments = store.list_stream_segments(&command.session_id)?;
                if staged_segments != command.staged_segments {
                    return Err(MetadataError::Db {
                        context: "abort stream upload command staged segment mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                store.set_stream_upload_state_direct(
                    &command.session_id,
                    StreamUploadState::Aborted,
                )?;
                store.delete_stream_upload_direct(&command.session_id)
            },
        )
    }

    fn apply_commit_stream_part_command(
        &self,
        command: &CommitStreamPartCommand,
    ) -> Result<(), MetadataError> {
        if self.commit_stream_part_command_already_applied(command)? {
            return Ok(());
        }

        self.with_immediate_txn(
            "commit stream part command (begin txn)",
            "commit stream part command (commit txn)",
            |store| {
                store.validate_commit_stream_part_command(command)?;

                store.set_stream_upload_state_direct(
                    &command.session_id,
                    StreamUploadState::Completing,
                )?;
                store.insert_multipart_part_explicit(&command.part)?;
                store.delete_multipart_part_segments_for_upload_part(
                    &command.bucket,
                    &command.key,
                    &command.upload.upload_id,
                    command.part.part_number,
                )?;
                store.insert_multipart_part_segments_explicit(&command.segments)?;
                store.delete_stream_upload_direct(&command.session_id)
            },
        )
    }

    fn commit_stream_part_command_already_applied(
        &self,
        command: &CommitStreamPartCommand,
    ) -> Result<bool, MetadataError> {
        match self.get_stream_upload(&command.session_id) {
            Ok(_) => Ok(false),
            Err(MetadataError::StreamSessionNotFound { .. }) => {
                let part = match self
                    .get_multipart_part(&command.part.upload_id, command.part.part_number)
                {
                    Ok(part) => part,
                    Err(MetadataError::PartNotFound { .. }) => return Ok(false),
                    Err(error) => return Err(error),
                };
                let segments = self.get_multipart_part_segments_for_upload_part(
                    &command.bucket,
                    &command.key,
                    &command.upload.upload_id,
                    command.part.part_number,
                )?;
                if part == command.part && segments == command.segments {
                    Ok(true)
                } else {
                    Err(MetadataError::Db {
                        context: "commit stream part command applied result mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    })
                }
            }
            Err(error) => Err(error),
        }
    }

    fn validate_commit_stream_part_command(
        &self,
        command: &CommitStreamPartCommand,
    ) -> Result<(), MetadataError> {
        if command.upload.bucket != command.bucket
            || command.upload.key != command.key
            || command.upload.state != UploadState::InProgress
            || command.part.upload_id != command.upload.upload_id
        {
            return Err(MetadataError::Db {
                context: "commit stream part command upload binding mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        if command.part.part_okh != [0u8; 16] {
            return Err(MetadataError::Db {
                context: "commit stream part command non-streamed part",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        let expected_generation = match command.existing_part.as_ref() {
            Some(existing) => {
                if existing.upload_id != command.upload.upload_id
                    || existing.part_number != command.part.part_number
                {
                    return Err(MetadataError::Db {
                        context: "commit stream part command existing part binding mismatch",
                        source: rusqlite::Error::InvalidQuery,
                    });
                }
                existing
                    .generation
                    .checked_add(1)
                    .ok_or(MetadataError::Db {
                        context: "commit stream part command generation overflow",
                        source: rusqlite::Error::InvalidQuery,
                    })?
            }
            None => 0,
        };
        if command.part.generation != expected_generation {
            return Err(MetadataError::Db {
                context: "commit stream part command generation mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }

        let session = self.get_stream_upload(&command.session_id)?;
        if session.state != StreamUploadState::InProgress {
            return Err(MetadataError::StreamSessionNotInProgress {
                state: session.state as u8,
            });
        }
        match &session.target {
            StreamUploadTarget::UploadPart {
                upload_id,
                part_number,
            } if upload_id == &command.upload.upload_id
                && *part_number == command.part.part_number => {}
            _ => {
                return Err(MetadataError::StreamSessionNotFound {
                    session_id: command.session_id.as_str().to_owned(),
                })
            }
        }
        if session.bucket != command.bucket || session.key != command.key {
            return Err(MetadataError::Db {
                context: "commit stream part command session binding mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }

        let upload = self.get_multipart_upload(&command.upload.upload_id)?;
        if upload != command.upload {
            return Err(MetadataError::Db {
                context: "commit stream part command upload mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }

        let existing_part =
            match self.get_multipart_part(&command.part.upload_id, command.part.part_number) {
                Ok(part) => Some(part),
                Err(MetadataError::PartNotFound { .. }) => None,
                Err(error) => return Err(error),
            };
        if existing_part != command.existing_part {
            return Err(MetadataError::Db {
                context: "commit stream part command existing part mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }

        let displaced_segments = self.get_multipart_part_segments_for_upload_part(
            &command.bucket,
            &command.key,
            &command.upload.upload_id,
            command.part.part_number,
        )?;
        if displaced_segments != command.displaced_segments {
            return Err(MetadataError::Db {
                context: "commit stream part command displaced segments mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }

        let staged_segments = self.list_stream_segments(&command.session_id)?;
        let expected_staged_segments: Vec<StreamUploadSegmentRecord> = command
            .segments
            .iter()
            .map(|segment| StreamUploadSegmentRecord {
                session_id: command.session_id.clone(),
                segment_index: segment.segment_index,
                size: segment.size,
                segment_crc64: segment.segment_crc64,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                data_pg_id: segment.data_pg_id,
                ec_k: segment.ec_k,
                ec_m: segment.ec_m,
            })
            .collect();
        if staged_segments != expected_staged_segments {
            return Err(MetadataError::Db {
                context: "commit stream part command staged segments mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        for segment in &command.segments {
            if segment.bucket != command.bucket
                || segment.key != command.key
                || segment.upload_id != command.upload.upload_id
                || segment.version_id != PART_SEGMENT_STAGING_VERSION_ID.to_u64()
                || segment.part_number != command.part.part_number
            {
                return Err(MetadataError::Db {
                    context: "commit stream part command segment binding mismatch",
                    source: rusqlite::Error::InvalidQuery,
                });
            }
        }
        Ok(())
    }

    fn insert_multipart_part_explicit(
        &self,
        part: &MultipartPartRecord,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO multipart_parts \
                 (upload_id, part_number, generation, size, etag, etag_kind, \
                  part_okh, part_vid, ec_k, ec_m, last_modified, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    part.upload_id,
                    part.part_number,
                    part.generation,
                    part.size as i64,
                    part.etag,
                    part.etag_kind as u8,
                    part.part_okh.as_slice(),
                    part.part_vid.get() as i64,
                    part.ec_k,
                    part.ec_m,
                    part.last_modified as i64,
                    part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "insert multipart part explicit",
                source: e,
            })?;
        Ok(())
    }

    fn delete_multipart_part_segments_for_upload_part(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 AND part_number = ?4",
                params![bucket, key, upload_id, part_number],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart part segments for upload part",
                source: e,
            })?;
        Ok(())
    }

    fn insert_multipart_part_segments_explicit(
        &self,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "INSERT INTO multipart_part_segments \
                 (bucket, key, upload_id, version_id, part_number, segment_index, size, \
                  segment_crc64, segment_okh, segment_vid, data_pg_id, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare insert multipart part segments explicit",
                source: e,
            })?;
        for segment in segments {
            stmt.execute(params![
                segment.bucket,
                segment.key,
                segment.upload_id,
                segment.version_id as i64,
                segment.part_number,
                segment.segment_index,
                segment.size as i64,
                segment.segment_crc64.map(|v| v as i64),
                segment.segment_okh.as_slice(),
                segment.segment_vid.get() as i64,
                segment.data_pg_id,
                segment.ec_k,
                segment.ec_m,
            ])
            .map_err(|e| MetadataError::Db {
                context: "insert multipart part segment explicit",
                source: e,
            })?;
        }
        Ok(())
    }

    fn apply_create_multipart_upload_command(
        &self,
        command: &CreateMultipartUploadCommand,
    ) -> Result<(), MetadataError> {
        if command.upload.state != UploadState::InProgress {
            return Err(MetadataError::Db {
                context: "create multipart upload command state mismatch",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        self.create_multipart_upload_explicit(&command.upload)
    }

    fn create_multipart_upload_explicit(
        &self,
        upload: &MultipartUploadRecord,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_multipart_upload_explicit",
            "pg_id={} upload_id={:?} bucket={:?} key={:?}",
            self.pg_id,
            upload.upload_id.as_str(),
            upload.bucket.as_str(),
            upload.key.as_str()
        );
        let algo = upload.checksum.map(|c| c.algorithm() as u8);
        let ctype = upload.checksum.map(|c| c.checksum_type() as u8);
        let tags = upload.tags.as_ref().map(SerializedTagSet::as_str);
        let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
            Self::object_lock_sql_values(upload.object_lock).map_err(|e| MetadataError::Db {
                context: "create multipart upload (encode object lock)",
                source: e,
            })?;
        let encryption_type = upload.encryption.encryption_type() as u8;
        let encryption_state = upload.encryption.encode_state();
        let system_metadata_blob = upload.system_metadata_blob.as_slice();
        self.with_immediate_txn(
            "create multipart upload (begin txn)",
            "create multipart upload (commit txn)",
            |store| {
                match store.conn.execute(
                    "INSERT INTO object_generation_reservations \
                     (reservation_id, bucket, key, generation_id, created_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        upload.upload_id.as_str(),
                        upload.bucket,
                        upload.key,
                        upload.object_generation_id.get() as i64,
                        upload.initiated_at as i64,
                    ],
                ) {
                    Ok(_) => {}
                    Err(rusqlite::Error::SqliteFailure(_, _)) => {
                        let existing_generation = store
                            .conn
                            .query_row(
                                "SELECT generation_id FROM object_generation_reservations \
                                 WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3",
                                params![upload.upload_id.as_str(), upload.bucket, upload.key],
                                |row| {
                                    let raw: i64 = row.get(0)?;
                                    Self::parse_generation_id(raw, 0, "generation_id")
                                },
                            )
                            .optional()
                            .map_err(|e| MetadataError::Db {
                                context: "create multipart upload explicit reservation lookup",
                                source: e,
                            })?;
                        if !matches!(existing_generation, Some(existing) if existing == upload.object_generation_id)
                        {
                            return Err(MetadataError::Db {
                                context: "create multipart upload explicit reservation mismatch",
                                source: rusqlite::Error::InvalidQuery,
                            });
                        }
                    }
                    Err(e) => {
                        return Err(MetadataError::Db {
                            context: "create multipart upload (reserve generation)",
                            source: e,
                        });
                    }
                }
                match store.conn.execute(
                    "INSERT INTO multipart_uploads \
                     (upload_id, bucket, key, initiated_at, state, tags, metadata_blob, system_metadata_blob, owner_principal, owner_canonical_id, \
                      initiator_principal, initiator_canonical_id, checksum_algorithm, checksum_type, encryption_type, encryption_state, acl_grants, public_read, object_generation_id, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)",
                    params![
                        upload.upload_id,
                        upload.bucket,
                        upload.key,
                        upload.initiated_at as i64,
                        upload.state as u8,
                        tags,
                        upload.metadata_blob.as_slice(),
                        system_metadata_blob,
                        upload.owner.principal,
                        upload.owner.canonical_id.as_str(),
                        upload.initiator.as_ref().map(|owner| owner.principal.as_str()),
                        upload.initiator
                            .as_ref()
                            .map(|owner| owner.canonical_id.as_str()),
                        algo,
                        ctype,
                        encryption_type,
                        encryption_state,
                        upload.acl_grants.serialized(),
                        i32::from(upload.public_read),
                        upload.object_generation_id.get() as i64,
                        object_lock_retention_mode,
                        object_lock_retain_until,
                        object_lock_legal_hold,
                    ],
                ) {
                    Ok(_) => {}
                    Err(rusqlite::Error::SqliteFailure(_, _)) => {
                        let existing = store.get_multipart_upload(&upload.upload_id)?;
                        if existing != *upload {
                            return Err(MetadataError::Db {
                                context: "create multipart upload explicit existing upload mismatch",
                                source: rusqlite::Error::InvalidQuery,
                            });
                        }
                    }
                    Err(e) => {
                        return Err(MetadataError::Db {
                            context: "create multipart upload",
                            source: e,
                        });
                    }
                }
                Ok(())
            },
        )
    }

    fn list_stream_uploads_for_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<Vec<StreamUploadRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT session_id, bucket, key, op_kind, upload_id, part_number, state, \
                 created_at, encryption_type, encryption_state FROM stream_uploads \
                 WHERE op_kind = ?1 AND upload_id = ?2 ORDER BY session_id ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for multipart upload (prepare)",
                source: e,
            })?;
        let rows = stmt
            .query_map(
                params![StreamUploadKind::UploadPart as u8, upload_id.as_str()],
                |row| {
                    let op_kind_raw: u8 = row.get(3)?;
                    let state_raw: u8 = row.get(6)?;
                    let upload_id: Option<UploadId> = row.get(4)?;
                    let part_number: Option<i64> = row.get(5)?;
                    Ok(StreamUploadRecord {
                        session_id: row.get(0)?,
                        bucket: row.get(1)?,
                        key: row.get(2)?,
                        target: PgStore::parse_stream_target(
                            op_kind_raw,
                            upload_id,
                            part_number,
                            3,
                        )?,
                        state: StreamUploadState::from_u8(state_raw).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                6,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid stream state: {state_raw}")),
                            )
                        })?,
                        created_at: row.get::<_, i64>(7)? as u64,
                        encryption: Self::parse_object_encryption(
                            row.get::<_, u8>(8)?,
                            row.get::<_, Option<Vec<u8>>>(9)?,
                            8,
                            9,
                        )?,
                    })
                },
            )
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for multipart upload (query)",
                source: e,
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "list stream uploads for multipart upload (collect)",
                source: e,
            })
    }

    fn list_stream_segments_for_sessions(
        &self,
        sessions: &[StreamUploadRecord],
    ) -> Result<Vec<StreamUploadSegmentRecord>, MetadataError> {
        let mut segments = Vec::new();
        for session in sessions {
            segments.extend(self.list_stream_segments(&session.session_id)?);
        }
        Ok(segments)
    }

    pub(crate) fn prepare_abort_multipart_upload_cleanup(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<Option<AbortMultipartUploadCleanup>, MetadataError> {
        self.with_immediate_txn(
            "prepare abort multipart upload cleanup (begin txn)",
            "prepare abort multipart upload cleanup (commit txn)",
            |store| {
                let upload = match store.get_multipart_upload(upload_id) {
                    Ok(upload) => {
                        if upload.bucket != *bucket || upload.key != *key {
                            return Err(MetadataError::NoSuchUpload {
                                upload_id: upload_id.to_string(),
                            });
                        }
                        upload
                    }
                    Err(MetadataError::NoSuchUpload { .. }) => return Ok(None),
                    Err(error) => return Err(error),
                };

                match upload.state {
                    UploadState::InProgress => {}
                    UploadState::Aborting => {}
                    UploadState::Completing => return Ok(None),
                }

                let parts = store
                    .list_multipart_parts(&ListPartsReq {
                        upload_id: upload_id.clone(),
                        part_number_marker: None,
                        max_parts: u32::MAX,
                    })?
                    .parts;
                let streaming_segments =
                    store.get_all_multipart_part_segments_for_upload(upload_id)?;
                let stream_uploads = store.list_stream_uploads_for_multipart_upload(upload_id)?;
                let stream_upload_segments =
                    store.list_stream_segments_for_sessions(&stream_uploads)?;

                Ok(Some(AbortMultipartUploadCleanup {
                    upload,
                    parts,
                    streaming_segments,
                    stream_uploads,
                    stream_upload_segments,
                }))
            },
        )
    }

    pub(crate) fn prepare_authorized_abort_multipart_upload_cleanup(
        &self,
        authorized_upload: &AuthorizedMultipartUploadRecord,
    ) -> Result<Option<AbortMultipartUploadCleanup>, MetadataError> {
        self.with_immediate_txn(
            "prepare authorized abort multipart upload cleanup (begin txn)",
            "prepare authorized abort multipart upload cleanup (commit txn)",
            |store| {
                let upload = match store.get_multipart_upload(&authorized_upload.record().upload_id)
                {
                    Ok(upload) => upload,
                    Err(MetadataError::NoSuchUpload { .. }) => return Ok(None),
                    Err(error) => return Err(error),
                };
                if upload != *authorized_upload.record() {
                    return Ok(None);
                }

                match upload.state {
                    UploadState::InProgress => {}
                    UploadState::Aborting => {}
                    UploadState::Completing => return Ok(None),
                }

                let parts = store
                    .list_multipart_parts(&ListPartsReq {
                        upload_id: upload.upload_id.clone(),
                        part_number_marker: None,
                        max_parts: u32::MAX,
                    })?
                    .parts;
                let streaming_segments =
                    store.get_all_multipart_part_segments_for_upload(&upload.upload_id)?;
                let stream_uploads =
                    store.list_stream_uploads_for_multipart_upload(&upload.upload_id)?;
                let stream_upload_segments =
                    store.list_stream_segments_for_sessions(&stream_uploads)?;

                Ok(Some(AbortMultipartUploadCleanup {
                    upload,
                    parts,
                    streaming_segments,
                    stream_uploads,
                    stream_upload_segments,
                }))
            },
        )
    }

    fn apply_abort_multipart_upload_command(
        &self,
        command: &AbortMultipartUploadCommand,
    ) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "abort multipart upload command (begin txn)",
            "abort multipart upload command (commit txn)",
            |store| {
                let upload_present = match store.get_multipart_upload(&command.upload_id) {
                    Ok(upload) => {
                        if upload.bucket != command.bucket || upload.key != command.key {
                            return Err(MetadataError::Db {
                                context: "abort multipart upload command (upload mismatch)",
                                source: rusqlite::Error::InvalidQuery,
                            });
                        }
                        if upload != command.cleanup.upload {
                            return Err(MetadataError::Db {
                                context: "abort multipart upload command (cleanup mismatch)",
                                source: rusqlite::Error::InvalidQuery,
                            });
                        }
                        true
                    }
                    Err(MetadataError::NoSuchUpload { .. }) => false,
                    Err(error) => return Err(error),
                };
                if upload_present {
                    let stream_uploads =
                        store.list_stream_uploads_for_multipart_upload(&command.upload_id)?;
                    if stream_uploads != command.cleanup.stream_uploads {
                        return Err(MetadataError::Db {
                            context: "abort multipart upload command (stream uploads mismatch)",
                            source: rusqlite::Error::InvalidQuery,
                        });
                    }
                    let stream_upload_segments =
                        store.list_stream_segments_for_sessions(&stream_uploads)?;
                    if stream_upload_segments != command.cleanup.stream_upload_segments {
                        return Err(MetadataError::Db {
                            context:
                                "abort multipart upload command (stream upload segments mismatch)",
                            source: rusqlite::Error::InvalidQuery,
                        });
                    }
                    for session in &command.cleanup.stream_uploads {
                        store.delete_stream_upload_direct(&session.session_id)?;
                    }
                }
                store.delete_multipart_part_segments_by_upload_id_direct(&command.upload_id)?;
                store
                    .conn
                    .execute(
                        "DELETE FROM object_generation_reservations WHERE reservation_id = ?1",
                        params![command.upload_id.as_str()],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "abort multipart upload command (delete generation reservation)",
                        source: e,
                    })?;
                store
                    .conn
                    .execute(
                        "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                        params![command.upload_id.as_str()],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "abort multipart upload command (delete upload)",
                        source: e,
                    })?;
                Ok(())
            },
        )
    }

    fn put_delete_marker_explicit_in_open_txn(
        &self,
        command: &InsertDeleteMarkerCommand,
    ) -> Result<(), MetadataError> {
        self.mark_current_live_noncurrent(
            command.bucket.as_str(),
            command.key.as_str(),
            command.version_id,
            command.last_modified_millis,
        )
        .map_err(|e| MetadataError::Db {
            context: "put object meta (mark noncurrent delete marker)",
            source: e,
        })?;
        self.advance_object_version_counter_in_open_txn(
            &command.bucket,
            &command.key,
            command.version_id,
        )?;
        let sql = if command.version_id.is_null() {
            "INSERT OR REPLACE INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read) \
             VALUES (?1, ?2, ?3, ?4, NULL, 0, zeroblob(0), 0, ?5, 0, 0, 0, 1, 0, NULL, NULL, NULL, 0, NULL, ?6, ?7, ?8, 0)"
        } else {
            "INSERT INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read) \
             VALUES (?1, ?2, ?3, ?4, NULL, 0, zeroblob(0), 0, ?5, 0, 0, 0, 1, 0, NULL, NULL, NULL, 0, NULL, ?6, ?7, ?8, 0)"
        };
        self.execute_cached_metadata(
            sql,
            params![
                command.bucket,
                command.key,
                command.version_id.to_u64() as i64,
                command.write_sequence as i64,
                command.last_modified_millis as i64,
                command.owner.principal,
                command.owner.canonical_id.as_str(),
                "",
            ],
            "put object meta (delete marker)",
        )?;
        Ok(())
    }

    fn delete_object_version_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        let deleted_was_current: bool = self.query_row_cached_metadata(
            "SELECT EXISTS( \
                 SELECT 1 FROM objects \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                   AND version_id = ( \
                       SELECT version_id FROM objects \
                       WHERE bucket = ?1 AND key = ?2 \
                       ORDER BY write_sequence DESC LIMIT 1 \
                   ) \
             )",
            params![bucket, key, version_id.to_u64() as i64],
            "delete object version (lookup current)",
            |row| row.get::<_, i64>(0).map(|value| value != 0),
        )?;

        self.execute_cached_metadata(
            "DELETE FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
            params![bucket, key, version_id.to_u64() as i64],
            "delete object version",
        )?;

        if deleted_was_current {
            self.clear_current_live_noncurrent(bucket.as_str(), key.as_str())
                .map_err(|e| MetadataError::Db {
                    context: "delete object version (restore current)",
                    source: e,
                })?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn put_bucket_versioning_inner(
        &self,
        name: &BucketName,
        state: BucketVersioningState,
        generation: BucketExecutionGeneration,
    ) -> Result<(), MetadataError> {
        let (current, current_generation): (BucketVersioningState, u64) = self
            .query_row_cached_optional_metadata(
                "SELECT versioning, bucket_execution_generation FROM buckets WHERE name = ?1",
                params![name.as_str()],
                "get bucket versioning",
                |row| {
                    let raw_versioning = row.get::<_, u8>(0)?;
                    let raw_generation = row.get::<_, i64>(1)?;
                    Ok((raw_versioning, raw_generation))
                },
            )?
            .ok_or_else(|| bucket_not_found(name.as_str()))
            .and_then(|(raw_versioning, raw_generation)| {
                let versioning =
                    BucketVersioningState::from_u8(raw_versioning).ok_or_else(|| {
                        MetadataError::Db {
                            context: "invalid versioning state in database",
                            source: rusqlite::Error::FromSqlConversionFailure(
                                0,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid versioning: {raw_versioning}")),
                            ),
                        }
                    })?;
                let generation = raw_generation.try_into().map_err(|_| MetadataError::Db {
                    context: "decode bucket execution generation",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Integer,
                        Box::from("negative bucket_execution_generation"),
                    ),
                })?;
                Ok((versioning, generation))
            })?;
        if let BucketExecutionGeneration::Explicit(explicit) = generation {
            if current_generation == explicit {
                if current == state {
                    return Ok(());
                }
                return Err(MetadataError::Db {
                    context: "apply conflicting bucket versioning command",
                    source: rusqlite::Error::InvalidQuery,
                });
            }
            if current_generation > explicit {
                return Err(MetadataError::Db {
                    context: "apply stale bucket versioning command",
                    source: rusqlite::Error::InvalidQuery,
                });
            }
        }
        if state == BucketVersioningState::Disabled && current != BucketVersioningState::Disabled {
            return Err(MetadataError::InvalidVersioningTransition {
                from: current,
                to: state,
            });
        }

        self.with_immediate_txn(
            "put bucket versioning (begin txn)",
            "put bucket versioning (commit txn)",
            |store| {
                let generation = match generation {
                    #[cfg(test)]
                    BucketExecutionGeneration::Allocate => store
                        .next_bucket_execution_generation_in_txn(
                            "put bucket versioning (allocate execution generation)",
                        )?,
                    BucketExecutionGeneration::Explicit(generation) => {
                        store.advance_bucket_execution_generation_in_txn(
                            generation,
                            "put bucket versioning (advance execution generation)",
                        )?;
                        generation
                    }
                };
                store.execute_cached_metadata(
                    "UPDATE buckets \
                     SET versioning = ?1, \
                         bucket_execution_generation = ?2 \
                     WHERE name = ?3",
                    params![state as u8 as i64, generation as i64, name.as_str()],
                    "put bucket versioning",
                )?;
                Ok(())
            },
        )
    }

    #[cfg(test)]
    fn put_bucket_acl_inner(
        &self,
        name: &BucketName,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
        generation: BucketExecutionGeneration,
    ) -> Result<(), MetadataError> {
        let (current_acl_grants, current_public_read, current_public_write, current_generation): (
            AclGrants,
            bool,
            bool,
            u64,
        ) = self
            .query_row_cached_optional_metadata(
                "SELECT acl_grants, public_read, public_write, bucket_execution_generation \
                 FROM buckets \
                 WHERE name = ?1",
                params![name.as_str()],
                "get bucket acl",
                |row| {
                    let raw_acl_grants = row.get::<_, String>(0)?;
                    let public_read = row.get::<_, i64>(1)? != 0;
                    let public_write = row.get::<_, i64>(2)? != 0;
                    let raw_generation = row.get::<_, i64>(3)?;
                    Ok((raw_acl_grants, public_read, public_write, raw_generation))
                },
            )?
            .ok_or_else(|| bucket_not_found(name.as_str()))
            .and_then(
                |(raw_acl_grants, public_read, public_write, raw_generation)| {
                    let acl_grants = Self::parse_acl_grants(raw_acl_grants, 0, "bucket acl")
                        .map_err(|source| MetadataError::Db {
                            context: "decode bucket acl",
                            source,
                        })?;
                    let generation = raw_generation.try_into().map_err(|_| MetadataError::Db {
                        context: "decode bucket execution generation",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Integer,
                            Box::from("negative bucket_execution_generation"),
                        ),
                    })?;
                    Ok((acl_grants, public_read, public_write, generation))
                },
            )?;
        if let BucketExecutionGeneration::Explicit(explicit) = generation {
            if current_generation == explicit {
                if current_acl_grants == *acl_grants
                    && current_public_read == public_read
                    && current_public_write == public_write
                {
                    return Ok(());
                }
                return Err(MetadataError::Db {
                    context: "apply conflicting bucket acl command",
                    source: rusqlite::Error::InvalidQuery,
                });
            }
            if current_generation > explicit {
                return Err(MetadataError::Db {
                    context: "apply stale bucket acl command",
                    source: rusqlite::Error::InvalidQuery,
                });
            }
        }

        self.with_immediate_txn(
            "put bucket acl (begin txn)",
            "put bucket acl (commit txn)",
            |store| {
                let generation = match generation {
                    #[cfg(test)]
                    BucketExecutionGeneration::Allocate => store
                        .next_bucket_execution_generation_in_txn(
                            "put bucket acl (allocate execution generation)",
                        )?,
                    BucketExecutionGeneration::Explicit(generation) => {
                        store.advance_bucket_execution_generation_in_txn(
                            generation,
                            "put bucket acl (advance execution generation)",
                        )?;
                        generation
                    }
                };
                let updated = store.execute_cached_metadata(
                    "UPDATE buckets \
                     SET acl_grants = ?1, \
                         public_read = ?2, \
                         public_write = ?3, \
                         bucket_execution_generation = ?4 \
                     WHERE name = ?5",
                    params![
                        acl_grants.serialized(),
                        i32::from(public_read),
                        i32::from(public_write),
                        generation as i64,
                        name.as_str()
                    ],
                    "put bucket acl",
                )?;
                if updated == 0 {
                    return Err(bucket_not_found(name.as_str()));
                }
                Ok(())
            },
        )
    }

    #[cfg(test)]
    fn bucket_property_matches(
        &self,
        info: &BucketInfo,
        mutation: &BucketPropertyMutation,
    ) -> Result<bool, MetadataError> {
        match mutation {
            BucketPropertyMutation::ObjectLock(config) => Ok(info.object_lock == *config),
            BucketPropertyMutation::Encryption(config) => {
                Ok(PgMetadataStore::get_bucket_encryption(self, &info.name)? == *config)
            }
            BucketPropertyMutation::PublicAccessBlock(config) => {
                Ok(info.public_access_block == *config)
            }
            BucketPropertyMutation::OwnershipControls(config) => {
                Ok(info.ownership_controls == *config)
            }
            BucketPropertyMutation::AbacEnabled(enabled) => {
                Ok(info.bucket_abac_enabled == *enabled)
            }
        }
    }

    #[cfg(test)]
    fn put_bucket_property_inner(
        &self,
        name: &BucketName,
        mutation: &BucketPropertyMutation,
        generation: BucketExecutionGeneration,
    ) -> Result<(), MetadataError> {
        let info = self.head_bucket_raw(name)?;
        if let BucketExecutionGeneration::Explicit(explicit) = generation {
            if info.bucket_execution_generation == explicit {
                if self.bucket_property_matches(&info, mutation)? {
                    return Ok(());
                }
                return Err(MetadataError::Db {
                    context: bucket_property_conflict_context(mutation.effect()),
                    source: rusqlite::Error::InvalidQuery,
                });
            }
            if info.bucket_execution_generation > explicit {
                return Err(MetadataError::Db {
                    context: bucket_property_stale_context(mutation.effect()),
                    source: rusqlite::Error::InvalidQuery,
                });
            }
        }

        self.with_immediate_txn(
            "put bucket property (begin txn)",
            "put bucket property (commit txn)",
            |store| {
                let generation = match generation {
                    #[cfg(test)]
                    BucketExecutionGeneration::Allocate => store
                        .next_bucket_execution_generation_in_txn(
                            "put bucket property (allocate execution generation)",
                        )?,
                    BucketExecutionGeneration::Explicit(generation) => {
                        store.advance_bucket_execution_generation_in_txn(
                            generation,
                            "put bucket property (advance execution generation)",
                        )?;
                        generation
                    }
                };
                let updated = match mutation {
                    BucketPropertyMutation::ObjectLock(config) => {
                        let (enabled, default_mode, default_days, default_years) =
                            Self::bucket_object_lock_sql_values(*config).map_err(|e| {
                                MetadataError::Db {
                                    context: "put bucket object lock (encode)",
                                    source: e,
                                }
                            })?;
                        store.execute_cached_metadata(
                            "UPDATE buckets \
                             SET object_lock_enabled = ?1, \
                                 object_lock_default_mode = ?2, \
                                 object_lock_default_days = ?3, \
                                 object_lock_default_years = ?4, \
                                 bucket_execution_generation = ?5 \
                             WHERE name = ?6",
                            params![
                                enabled,
                                default_mode,
                                default_days,
                                default_years,
                                generation as i64,
                                name.as_str()
                            ],
                            "put bucket object lock",
                        )?
                    }
                    BucketPropertyMutation::Encryption(config) => store.execute_cached_metadata(
                        "UPDATE buckets \
                             SET default_encryption_type = ?1, \
                                 sse_c_blocked = ?2, \
                                 bucket_execution_generation = ?3 \
                             WHERE name = ?4",
                        params![
                            config.default_encryption.map(|value| value as u8),
                            i32::from(config.sse_c_blocked),
                            generation as i64,
                            name.as_str()
                        ],
                        "put bucket encryption",
                    )?,
                    BucketPropertyMutation::PublicAccessBlock(config) => {
                        let (
                            present,
                            block_public_acls,
                            ignore_public_acls,
                            block_public_policy,
                            restrict_public_buckets,
                        ) = Self::public_access_block_sql_values(*config);
                        store.execute_cached_metadata(
                            "UPDATE buckets SET \
                                 public_access_block_present = ?1, \
                                 public_access_block_block_public_acls = ?2, \
                                 public_access_block_ignore_public_acls = ?3, \
                                 public_access_block_block_public_policy = ?4, \
                                 public_access_block_restrict_public_buckets = ?5, \
                                 bucket_execution_generation = ?6 \
                             WHERE name = ?7",
                            params![
                                present,
                                block_public_acls,
                                ignore_public_acls,
                                block_public_policy,
                                restrict_public_buckets,
                                generation as i64,
                                name.as_str(),
                            ],
                            "put bucket public access block",
                        )?
                    }
                    BucketPropertyMutation::OwnershipControls(config) => store
                        .execute_cached_metadata(
                            "UPDATE buckets \
                             SET ownership_controls_mode = ?1, \
                                 bucket_execution_generation = ?2 \
                             WHERE name = ?3",
                            params![
                                Self::ownership_controls_sql_value(*config),
                                generation as i64,
                                name.as_str()
                            ],
                            "put bucket ownership controls",
                        )?,
                    BucketPropertyMutation::AbacEnabled(enabled) => store.execute_cached_metadata(
                        "UPDATE buckets \
                             SET bucket_abac_enabled = ?1, \
                                 bucket_execution_generation = ?2 \
                             WHERE name = ?3",
                        params![
                            if *enabled { 1 } else { 0 },
                            generation as i64,
                            name.as_str()
                        ],
                        "put bucket abac enabled",
                    )?,
                };
                if updated == 0 {
                    return Err(bucket_not_found(name.as_str()));
                }
                Ok(())
            },
        )
    }

    pub fn load_bucket_execution_generations(
        &self,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, u64>, MetadataError> {
        if buckets.is_empty() {
            return Ok(HashMap::new());
        }
        let placeholders = std::iter::repeat_n("?", buckets.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT name, bucket_execution_generation \
             FROM buckets \
             WHERE name IN ({placeholders})"
        );
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|source| MetadataError::Db {
                context: "prepare load bucket execution generations",
                source,
            })?;
        let rows = stmt
            .query_map(
                params_from_iter(buckets.iter().map(|bucket| bucket.as_str())),
                |row| Ok((row.get::<_, BucketName>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(|source| MetadataError::Db {
                context: "query load bucket execution generations",
                source,
            })?;
        let mut generations = HashMap::with_capacity(buckets.len());
        for row in rows {
            let (bucket, generation) = row.map_err(|source| MetadataError::Db {
                context: "row load bucket execution generations",
                source,
            })?;
            generations.insert(
                bucket,
                generation.try_into().map_err(|_| MetadataError::Db {
                    context: "decode bucket execution generation",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        1,
                        rusqlite::types::Type::Integer,
                        Box::from("negative bucket_execution_generation"),
                    ),
                })?,
            );
        }
        Ok(generations)
    }

    pub(crate) fn next_object_write_sequence(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<u64, MetadataError> {
        let max: Option<i64> = self
            .query_row_cached_optional_metadata(
                "SELECT MAX(write_sequence) FROM objects WHERE bucket = ?1 AND key = ?2",
                params![bucket, key],
                "next object write sequence",
                |row| row.get(0),
            )?
            .flatten();

        match max {
            None => Ok(1),
            Some(value) => {
                let current = u64::try_from(value).map_err(|_| MetadataError::Db {
                    context: "negative write_sequence in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(write_sequence): {value}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "write_sequence overflow",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("MAX(write_sequence) overflow"),
                    ),
                })
            }
        }
    }

    fn advance_object_version_counter_in_open_txn(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        if version_id.is_null() {
            return Ok(());
        }
        let following = version_id
            .to_u64()
            .checked_add(1)
            .ok_or_else(|| MetadataError::Db {
                context: "advance object version counter",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from("version_id overflow"),
                ),
            })?;
        let following = i64::try_from(following).map_err(|_| MetadataError::Db {
            context: "advance object version counter",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("version_id exceeds SQLite integer range"),
            ),
        })?;
        self.execute_cached_metadata(
            "INSERT INTO object_version_counters (bucket, key, next_version_id) \
             VALUES (?1, ?2, ?3) \
             ON CONFLICT(bucket, key) DO UPDATE SET \
                 next_version_id = max(object_version_counters.next_version_id, excluded.next_version_id)",
            params![bucket, key, following],
            "advance object version counter",
        )?;
        Ok(())
    }

    fn reserve_object_version_explicit(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        if version_id.is_null() {
            return Err(MetadataError::Db {
                context: "reserve object version command null version",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        let expected = self.next_version_id(bucket, key)?;
        if expected.to_u64() > version_id.to_u64() {
            return Err(MetadataError::Db {
                context: "reserve object version command stale version",
                source: rusqlite::Error::InvalidQuery,
            });
        }
        // A command from the active primary may be ahead of this replica if a
        // prior reservation partially applied before restart and the pending
        // in-memory command was lost. Advancing forward is safe for this
        // allocator: version ids are opaque and gaps are preferable to making
        // the key permanently unwritable.
        self.advance_object_version_counter_in_open_txn(bucket, key, version_id)
    }

    pub(crate) fn object_write_sequence(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<Option<u64>, MetadataError> {
        self.conn
            .prepare_cached(
                "SELECT write_sequence FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
            )
            .and_then(|mut stmt| {
                stmt.query_row(
                    params![bucket, key, version_id.to_u64() as i64],
                    |row| row.get::<_, i64>(0),
                )
            })
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object write sequence",
                source: e,
            })?
            .map(|value| {
                u64::try_from(value).map_err(|_| MetadataError::Db {
                    context: "negative write_sequence in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative write_sequence: {value}")),
                    ),
                })
            })
            .transpose()
    }

    fn mark_current_live_noncurrent(
        &self,
        bucket: &str,
        key: &str,
        replacement_version_id: VersionId,
        transition_time: u64,
    ) -> Result<(), rusqlite::Error> {
        self.conn
            .prepare_cached(
                "UPDATE objects SET became_noncurrent_at = ?1 \
                 WHERE bucket = ?2 AND key = ?3 \
                   AND version_id = ( \
                       SELECT version_id FROM objects \
                       WHERE bucket = ?2 AND key = ?3 \
                       ORDER BY write_sequence DESC LIMIT 1 \
                   ) \
                   AND version_id <> ?4 \
                   AND status = ?5 \
                   AND became_noncurrent_at IS NULL",
            )?
            .execute(params![
                transition_time as i64,
                bucket,
                key,
                replacement_version_id.to_u64() as i64,
                ObjectState::Live as u8,
            ])?;
        Ok(())
    }

    fn clear_current_live_noncurrent(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(), rusqlite::Error> {
        self.conn
            .prepare_cached(
                "UPDATE objects SET became_noncurrent_at = NULL \
                 WHERE bucket = ?1 AND key = ?2 \
                   AND version_id = ( \
                       SELECT version_id FROM objects \
                       WHERE bucket = ?1 AND key = ?2 \
                       ORDER BY write_sequence DESC LIMIT 1 \
                   ) \
                   AND status = ?3 \
                   AND became_noncurrent_at IS NOT NULL",
            )?
            .execute(params![bucket, key, ObjectState::Live as u8])?;
        Ok(())
    }

    pub(crate) fn completed_multipart_upload_sequence_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<u64, MetadataError> {
        let bucket = bucket.as_str();
        self.query_row_cached_metadata(
            "SELECT completed_multipart_upload_sequence FROM buckets WHERE name = ?1",
            params![bucket],
            "read completed multipart upload sequence",
            |row| row.get::<_, i64>(0),
        )?
        .try_into()
        .map_err(|_| MetadataError::Db {
            context: "decode completed multipart upload sequence",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("negative completed multipart upload sequence"),
            ),
        })
    }

    pub fn list_completed_multipart_uploads_for_bucket(
        &self,
        bucket: &str,
    ) -> Result<Vec<(UploadId, u64)>, MetadataError> {
        self.list_completed_multipart_upload_records_for_bucket(bucket)
            .map(|records| {
                records
                    .into_iter()
                    .map(|record| (record.upload_id, record.completion_order))
                    .collect()
            })
    }

    pub(crate) fn list_completed_multipart_upload_records_for_bucket(
        &self,
        bucket: &str,
    ) -> Result<Vec<CompletedMultipartUploadRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT upload_id, bucket, key, completion_order, completed_at, \
                        owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id \
                 FROM completed_multipart_uploads \
                 WHERE bucket = ?1",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list completed multipart uploads for bucket",
                source: e,
            })?;
        let rows = stmt
            .query_map(params![bucket], |row| {
                Self::completed_multipart_upload_record_from_row(row)
            })
            .map_err(|e| MetadataError::Db {
                context: "query list completed multipart uploads for bucket",
                source: e,
            })?;
        let mut uploads = Vec::new();
        for row in rows {
            uploads.push(row.map_err(|e| MetadataError::Db {
                context: "row list completed multipart uploads for bucket",
                source: e,
            })?);
        }
        Ok(uploads)
    }

    fn completed_multipart_upload_record_from_row(
        row: &rusqlite::Row<'_>,
    ) -> Result<CompletedMultipartUploadRecord, rusqlite::Error> {
        let completion_order = row.get::<_, i64>(3)?.try_into().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Integer,
                Box::from("negative completion order"),
            )
        })?;
        let completed_at = row.get::<_, i64>(4)?.try_into().map_err(|_| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Integer,
                Box::from("negative completed_at"),
            )
        })?;
        let owner = Self::parse_owner_identity(row, 5, 6, "owner_principal", "owner_canonical_id")?;
        let initiator = Self::parse_optional_owner_identity(
            row,
            7,
            8,
            "initiator_principal",
            "initiator_canonical_id",
        )?;
        Ok(CompletedMultipartUploadRecord {
            upload_id: row.get(0)?,
            bucket: row.get(1)?,
            key: row.get(2)?,
            completion_order,
            completed_at,
            initiator,
            owner,
        })
    }

    fn reserve_object_generation_explicit(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
        generation_id: GenerationId,
        created_at_millis: u64,
    ) -> Result<(), MetadataError> {
        match self.conn.execute(
            "INSERT INTO object_generation_reservations \
             (reservation_id, bucket, key, generation_id, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                reservation_id.as_str(),
                bucket,
                key,
                generation_id.get() as i64,
                created_at_millis as i64,
            ],
        ) {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(_, _)) => {
                let existing = PgMetadataStore::get_object_generation_reservation(
                    self,
                    bucket,
                    key,
                    reservation_id,
                );
                if matches!(existing, Ok(existing_generation) if existing_generation == generation_id)
                {
                    Ok(())
                } else {
                    Err(MetadataError::Db {
                        context: "reserve object generation explicit",
                        source: rusqlite::Error::InvalidQuery,
                    })
                }
            }
            Err(source) => Err(MetadataError::Db {
                context: "reserve object generation explicit",
                source,
            }),
        }
    }

    fn put_object_segments_reclaim_in_open_txn(
        &self,
        reclaim: &ObjectSegmentsReclaimRecord,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO object_segments_reclaims \
                 (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![
                    reclaim.bucket,
                    reclaim.key,
                    reclaim.generation_id.get() as i64,
                    reclaim.created_at as i64,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put object segments reclaim (root)",
                source: e,
            })?;

        for segment in &reclaim.segments {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO object_segment_reclaim_segments \
                     (bucket, key, generation_id, segment_index, segment_okh, segment_vid, \
                      data_pg_id, ec_k, ec_m) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    params![
                        reclaim.bucket,
                        reclaim.key,
                        reclaim.generation_id.get() as i64,
                        segment.segment_index as i64,
                        &segment.segment_okh[..],
                        segment.segment_vid.get() as i64,
                        segment.data_pg_id as i64,
                        segment.ec.k,
                        segment.ec.m,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put object segments reclaim (segment)",
                    source: e,
                })?;
        }
        Ok(())
    }

    fn put_multipart_reclaim_in_open_txn(
        &self,
        reclaim: &MultipartReclaimRecord,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO multipart_reclaims \
                 (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![
                    reclaim.bucket,
                    reclaim.key,
                    reclaim.generation_id.get() as i64,
                    reclaim.created_at as i64,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put multipart reclaim (root)",
                source: e,
            })?;

        for part in &reclaim.parts {
            match part {
                MultipartReclaimPartRecord::ShardSet {
                    part_number,
                    part_okh,
                    part_vid,
                    data_pg_id,
                    ec,
                } => {
                    self.conn
                        .execute(
                            "INSERT OR REPLACE INTO multipart_reclaim_parts \
                             (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                              part_vid, data_pg_id, ec_k, ec_m) \
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                            params![
                                reclaim.bucket,
                                reclaim.key,
                                reclaim.generation_id.get() as i64,
                                *part_number as i64,
                                MultipartReclaimPartKind::ShardSet as u8,
                                &part_okh[..],
                                part_vid.get() as i64,
                                *data_pg_id as i64,
                                ec.k,
                                ec.m,
                            ],
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "put multipart reclaim (part shard set)",
                            source: e,
                        })?;
                }
                MultipartReclaimPartRecord::Segments {
                    part_number,
                    segments,
                } => {
                    self.conn
                        .execute(
                            "INSERT OR REPLACE INTO multipart_reclaim_parts \
                             (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                              part_vid, data_pg_id, ec_k, ec_m) \
                             VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, NULL, NULL, NULL)",
                            params![
                                reclaim.bucket,
                                reclaim.key,
                                reclaim.generation_id.get() as i64,
                                *part_number as i64,
                                MultipartReclaimPartKind::Segments as u8,
                            ],
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "put multipart reclaim (part segments)",
                            source: e,
                        })?;

                    for segment in segments {
                        self.conn
                            .execute(
                                "INSERT OR REPLACE INTO multipart_reclaim_part_segments \
                                 (bucket, key, generation_id, part_number, segment_index, \
                                  segment_okh, segment_vid, data_pg_id, ec_k, ec_m) \
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                                params![
                                    reclaim.bucket,
                                    reclaim.key,
                                    reclaim.generation_id.get() as i64,
                                    segment.part_number as i64,
                                    segment.segment_index as i64,
                                    &segment.segment_okh[..],
                                    segment.segment_vid.get() as i64,
                                    segment.data_pg_id as i64,
                                    segment.ec.k,
                                    segment.ec.m,
                                ],
                            )
                            .map_err(|e| MetadataError::Db {
                                context: "put multipart reclaim (part segment)",
                                source: e,
                            })?;
                    }
                }
            }
        }
        Ok(())
    }

    fn put_object_with_segments_explicit_in_open_txn(
        &self,
        obj: &PutLiveObjectReq,
        segments: &[ObjectSegmentRecord],
        write_sequence: u64,
        last_modified: u64,
    ) -> Result<(), MetadataError> {
        obj.validate().map_err(|msg| MetadataError::Db {
            context: "put explicit segment object (etag/layout mismatch)",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Null,
                Box::from(msg),
            ),
        })?;
        if obj.layout != ObjectLayout::Standard {
            return Err(MetadataError::Db {
                context: "put explicit segment object (non-segment layout)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("put_object_with_segments requires Standard layout"),
                ),
            });
        }

        let data_layout = obj.layout.data_layout() as u8;
        let etag_kind = obj.etag.etag_kind() as u8;
        let status = ObjectState::Live as u8;
        let parts_count = obj.layout.parts_count().map(|n| n as i64);
        let tags = obj.tags.as_ref().map(SerializedTagSet::as_str);
        let metadata_blob = obj
            .metadata_blob
            .as_ref()
            .map(SerializedMetadataBlob::as_slice);
        let system_metadata_blob = obj
            .system_metadata_blob
            .as_ref()
            .map(SerializedSystemMetadataBlob::as_slice);
        let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
            Self::object_lock_sql_values(obj.object_lock).map_err(|e| MetadataError::Db {
                context: "put explicit segment object (encode object lock)",
                source: e,
            })?;
        let encryption_type = obj.encryption.encryption_type() as u8;
        let encryption_state = obj.encryption.encode_state();
        self.mark_current_live_noncurrent(
            obj.bucket.as_str(),
            obj.key.as_str(),
            obj.version_id,
            last_modified,
        )
        .map_err(|e| MetadataError::Db {
            context: "put explicit segment object (mark noncurrent)",
            source: e,
        })?;
        self.advance_object_version_counter_in_open_txn(&obj.bucket, &obj.key, obj.version_id)?;

        let obj_sql = if obj.version_id.is_null() {
            "INSERT OR REPLACE INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
        } else {
            "INSERT INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
        };
        self.execute_cached_metadata(
            obj_sql,
            params![
                obj.bucket,
                obj.key,
                obj.version_id.to_u64() as i64,
                write_sequence as i64,
                obj.generation_id.get() as i64,
                obj.size as i64,
                obj.etag.as_bytes().as_slice(),
                etag_kind,
                last_modified as i64,
                obj.ec.k,
                obj.ec.m,
                status,
                data_layout,
                parts_count,
                tags,
                metadata_blob,
                system_metadata_blob,
                encryption_type,
                encryption_state,
                obj.owner.principal,
                obj.owner.canonical_id.as_str(),
                obj.acl_grants.serialized(),
                i32::from(obj.public_read),
                object_lock_retention_mode,
                object_lock_retain_until,
                object_lock_legal_hold,
            ],
            "put explicit segment object (write object)",
        )?;

        self.execute_cached_metadata(
            "DELETE FROM object_segments \
             WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
            params![obj.bucket, obj.key, obj.version_id.to_u64() as i64],
            "put explicit segment object (delete prior segments)",
        )?;

        let mut stmt = self
            .conn
            .prepare_cached(
                "INSERT INTO object_segments \
                 (bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, segment_vid, \
                  data_pg_id, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            )
            .map_err(|e| MetadataError::Db {
                context: "put explicit segment object (prepare insert segments)",
                source: e,
            })?;
        for segment in segments {
            if segment.bucket != obj.bucket
                || segment.key != obj.key
                || segment.version_id != obj.version_id
            {
                return Err(MetadataError::Db {
                    context: "put explicit segment object (segment object mismatch)",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::from("segment row does not match object identity"),
                    ),
                });
            }
            stmt.execute(params![
                segment.bucket,
                segment.key,
                segment.version_id.to_u64() as i64,
                segment.segment_index,
                segment.size as i64,
                segment.segment_crc64.map(|v| v as i64),
                segment.segment_okh.as_slice(),
                segment.segment_vid.get() as i64,
                segment.data_pg_id,
                segment.ec_k,
                segment.ec_m,
            ])
            .map_err(|e| MetadataError::Db {
                context: "put explicit segment object (insert segment)",
                source: e,
            })?;
        }

        Ok(())
    }

    fn put_multipart_object_explicit_in_open_txn(
        &self,
        obj: &PutLiveObjectReq,
        parts: &[ObjectPartRecord],
        write_sequence: u64,
        last_modified: u64,
    ) -> Result<(), MetadataError> {
        obj.validate().map_err(|msg| MetadataError::Db {
            context: "put explicit multipart object (etag/layout mismatch)",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Null,
                Box::from(msg),
            ),
        })?;
        if !matches!(obj.layout, ObjectLayout::MultipartManifest { .. }) {
            return Err(MetadataError::Db {
                context: "put explicit multipart object (non-multipart layout)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("put multipart object requires MultipartManifest layout"),
                ),
            });
        }
        if parts.is_empty() {
            return Err(MetadataError::Db {
                context: "put explicit multipart object (empty parts)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("multipart commit requires at least one part"),
                ),
            });
        }

        let data_layout = obj.layout.data_layout() as u8;
        let etag_kind = obj.etag.etag_kind() as u8;
        let status = ObjectState::Live as u8;
        let parts_count = obj.layout.parts_count().map(|n| n as i64);
        let tags = obj.tags.as_ref().map(SerializedTagSet::as_str);
        let metadata_blob = obj
            .metadata_blob
            .as_ref()
            .map(SerializedMetadataBlob::as_slice);
        let system_metadata_blob = obj
            .system_metadata_blob
            .as_ref()
            .map(SerializedSystemMetadataBlob::as_slice);
        let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
            Self::object_lock_sql_values(obj.object_lock).map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (encode object lock)",
                source: e,
            })?;
        let encryption_type = obj.encryption.encryption_type() as u8;
        let encryption_state = obj.encryption.encode_state();
        self.mark_current_live_noncurrent(
            obj.bucket.as_str(),
            obj.key.as_str(),
            obj.version_id,
            last_modified,
        )
        .map_err(|e| MetadataError::Db {
            context: "put explicit multipart object (mark noncurrent)",
            source: e,
        })?;
        self.advance_object_version_counter_in_open_txn(&obj.bucket, &obj.key, obj.version_id)?;

        let obj_sql = if obj.version_id.is_null() {
            "INSERT OR REPLACE INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
        } else {
            "INSERT INTO objects \
             (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
              storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
        };
        self.conn
            .execute(
                obj_sql,
                params![
                    &obj.bucket,
                    &obj.key,
                    obj.version_id.to_u64() as i64,
                    write_sequence as i64,
                    obj.generation_id.get() as i64,
                    obj.size as i64,
                    obj.etag.as_bytes(),
                    etag_kind,
                    last_modified as i64,
                    obj.ec.k,
                    obj.ec.m,
                    status,
                    data_layout,
                    parts_count,
                    tags,
                    metadata_blob,
                    system_metadata_blob,
                    encryption_type,
                    encryption_state,
                    &obj.owner.principal,
                    obj.owner.canonical_id.as_str(),
                    obj.acl_grants.serialized(),
                    i32::from(obj.public_read),
                    object_lock_retention_mode,
                    object_lock_retain_until,
                    object_lock_legal_hold,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (write object)",
                source: e,
            })?;

        self.conn
            .execute(
                "DELETE FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![&obj.bucket, &obj.key, obj.version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (delete prior parts)",
                source: e,
            })?;

        let mut stmt = self
            .conn
            .prepare_cached(
                "INSERT INTO object_parts \
                 (bucket, key, version_id, part_number, object_offset_start, size, etag, etag_kind, \
                  part_okh, part_vid, ec_k, ec_m, data_pg_id, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            )
            .map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (prepare insert parts)",
                source: e,
            })?;
        let mut ordered_parts: Vec<&ObjectPartRecord> = parts.iter().collect();
        ordered_parts.sort_by_key(|part| part.part_number);
        let mut object_offset_start = 0u64;
        for part in ordered_parts {
            if part.bucket != obj.bucket || part.key != obj.key || part.version_id != obj.version_id
            {
                return Err(MetadataError::Db {
                    context: "put explicit multipart object (part identity mismatch)",
                    source: rusqlite::Error::InvalidQuery,
                });
            }
            stmt.execute(params![
                &part.bucket,
                &part.key,
                part.version_id.to_u64() as i64,
                part.part_number,
                object_offset_start as i64,
                part.size as i64,
                &part.etag,
                part.etag_kind as u8,
                part.part_okh.as_slice(),
                part.part_vid.get() as i64,
                part.ec_k,
                part.ec_m,
                part.data_pg_id,
                part.checksum.as_ref().map(|checksum| checksum.as_slice()),
            ])
            .map_err(|e| MetadataError::Db {
                context: "put explicit multipart object (insert part)",
                source: e,
            })?;
            object_offset_start += part.size;
        }

        Ok(())
    }

    fn delete_completed_multipart_upload(&self, upload_id: &UploadId) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM completed_multipart_uploads WHERE upload_id = ?1",
                params![upload_id.as_str()],
            )
            .map(|_| ())
            .map_err(|e| MetadataError::Db {
                context: "delete completed multipart upload",
                source: e,
            })
    }
}

impl PgMetadataStore for PgStore {
    #[cfg(test)]
    fn create_bucket(
        &self,
        name: &BucketName,
        owner_principal: &str,
        owner_canonical_id: &CanonicalUserId,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<(), MetadataError> {
        self.create_bucket_with_config(&CreateBucketConfig {
            name: name.as_str(),
            owner_principal,
            owner_canonical_id,
            acl_grants,
            public_read,
            public_write,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
        })
    }

    fn delete_finalized_bucket(&self, name: &BucketName) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::delete_finalized_bucket",
            "pg_id={} bucket={:?}",
            self.pg_id,
            name
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "delete finalized bucket (begin txn)",
                source: e,
            })?;
        let result = (|| -> Result<usize, MetadataError> {
            let state = self
                .conn
                .query_row(
                    "SELECT state FROM buckets WHERE name = ?1",
                    params![name.as_str()],
                    |row| row.get::<_, u8>(0),
                )
                .optional()
                .map_err(|source| MetadataError::Db {
                    context: "delete finalized bucket (load state)",
                    source,
                })?;
            let Some(state) = state else {
                return Ok(0);
            };
            let state = BucketState::from_u8(state).ok_or_else(|| MetadataError::Db {
                context: "delete finalized bucket (invalid bucket state)",
                source: rusqlite::Error::InvalidQuery,
            })?;
            if state != BucketState::Deleting {
                return Err(MetadataError::BucketNotFinalizedForDelete { state });
            }
            let deleted = self
                .conn
                .execute(
                    "DELETE FROM buckets WHERE name = ?1 AND state = ?2",
                    params![name.as_str(), BucketState::Deleting as u8],
                )
                .map_err(|source| MetadataError::Db {
                    context: "delete finalized bucket (delete row)",
                    source,
                })?;
            if deleted != 0 {
                self.conn
                    .execute(
                        "DELETE FROM completed_multipart_uploads WHERE bucket = ?1",
                        params![name.as_str()],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "delete finalized bucket (delete completed MPU records)",
                        source,
                    })?;
                self.conn
                    .execute(
                        "DELETE FROM object_version_counters WHERE bucket = ?1",
                        params![name.as_str()],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "delete finalized bucket (delete version counters)",
                        source,
                    })?;
            }
            Ok(deleted)
        })();
        let deleted = match result {
            Ok(deleted) => {
                self.conn
                    .execute_batch("COMMIT")
                    .map_err(|e| MetadataError::Db {
                        context: "delete finalized bucket (commit txn)",
                        source: e,
                    })?;
                deleted
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                return Err(error);
            }
        };
        if deleted == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn head_bucket(&self, name: &BucketName) -> Result<BucketInfo, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::head_bucket",
            "pg_id={} bucket={:?}",
            self.pg_id,
            name
        );
        let info = self.head_bucket_raw(name)?;
        if info.state != BucketState::Active {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(info)
    }

    fn head_bucket_raw(&self, name: &BucketName) -> Result<BucketInfo, MetadataError> {
        self.query_row_cached_optional_metadata(
            BUCKET_INFO_BY_NAME_SELECT,
            params![name.as_str()],
            "head bucket raw",
            Self::row_to_bucket_info,
        )?
        .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    fn head_bucket_record_raw(&self, name: &BucketName) -> Result<BucketRecord, MetadataError> {
        self.query_row_cached_optional_metadata(
            BUCKET_RECORD_BY_NAME_SELECT,
            params![name.as_str()],
            "head bucket record raw",
            Self::row_to_bucket_record,
        )?
        .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    fn list_buckets(&self, owner_canonical_id: &str) -> Result<Vec<BucketInfo>, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_buckets",
            "pg_id={} owner_canonical_id={}",
            self.pg_id,
            owner_canonical_id
        );
        let mut stmt = self
            .conn
            .prepare_cached(&format!(
                "{BUCKET_INFO_SELECT} WHERE owner_canonical_id = ?1 AND state = ?2 ORDER BY name ASC"
            ))
            .map_err(|e| MetadataError::Db {
                context: "prepare list buckets",
                source: e,
            })?;
        let rows = stmt
            .query_map(
                params![owner_canonical_id, BucketState::Active as u8],
                Self::row_to_bucket_info,
            )
            .map_err(|e| MetadataError::Db {
                context: "list buckets query",
                source: e,
            })?;

        let mut buckets = Vec::new();
        for row in rows {
            buckets.push(row.map_err(|e| MetadataError::Db {
                context: "list buckets row",
                source: e,
            })?);
        }
        Ok(buckets)
    }

    fn list_buckets_with_lifecycle(&self) -> Result<Vec<BucketInfo>, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_buckets_with_lifecycle",
            "pg_id={}",
            self.pg_id
        );
        let mut stmt = self
            .conn
            .prepare_cached(&format!(
                "{BUCKET_INFO_SELECT} \
                 JOIN bucket_subresources AS lifecycle \
                   ON lifecycle.bucket_name = buckets.name \
                  AND lifecycle.kind = {LIFECYCLE_SUBRESOURCE_KIND_SQL} \
                  AND lifecycle.body IS NOT NULL \
                 WHERE state = ?1 ORDER BY name ASC"
            ))
            .map_err(|e| MetadataError::Db {
                context: "prepare list buckets with lifecycle",
                source: e,
            })?;
        let rows = stmt
            .query_map(params![BucketState::Active as u8], Self::row_to_bucket_info)
            .map_err(|e| MetadataError::Db {
                context: "list buckets with lifecycle query",
                source: e,
            })?;

        let mut buckets = Vec::new();
        for row in rows {
            buckets.push(row.map_err(|e| MetadataError::Db {
                context: "list buckets with lifecycle row",
                source: e,
            })?);
        }
        Ok(buckets)
    }

    fn list_buckets_with_aborting_multipart_uploads(
        &self,
    ) -> Result<Vec<BucketName>, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_buckets_with_aborting_multipart_uploads",
            "pg_id={}",
            self.pg_id
        );
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT DISTINCT bucket FROM multipart_uploads \
                 WHERE state = ?1 ORDER BY bucket ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list buckets with aborting multipart uploads",
                source: e,
            })?;
        let rows = stmt
            .query_map(params![UploadState::Aborting as u8], |row| row.get(0))
            .map_err(|e| MetadataError::Db {
                context: "list buckets with aborting multipart uploads query",
                source: e,
            })?;

        let mut buckets = Vec::new();
        for row in rows {
            buckets.push(row.map_err(|e| MetadataError::Db {
                context: "list buckets with aborting multipart uploads row",
                source: e,
            })?);
        }
        Ok(buckets)
    }

    #[cfg(test)]
    fn mark_bucket_deleting(&self, name: &BucketName) -> Result<(), MetadataError> {
        self.with_immediate_txn(
            "mark bucket deleting (begin txn)",
            "mark bucket deleting (commit txn)",
            |store| {
                let generation = store.next_bucket_execution_generation_in_txn(
                    "mark bucket deleting (allocate execution generation)",
                )?;
                let updated = store
                    .conn
                    .execute(
                        "UPDATE buckets \
                         SET state = ?1, \
                             bucket_execution_generation = ?2 \
                         WHERE name = ?3 \
                           AND state = ?4 \
                           AND write_reservations_blocked = 1 \
                           AND active_write_reservations = 0",
                        params![
                            BucketState::Deleting as u8,
                            generation as i64,
                            name.as_str(),
                            BucketState::Active as u8
                        ],
                    )
                    .map_err(|source| MetadataError::Db {
                        context: "mark bucket deleting",
                        source,
                    })?;
                if updated == 0 {
                    return Err(bucket_not_found(name.as_str()));
                }
                Ok(())
            },
        )
    }

    fn acquire_bucket_write_reservation(
        &self,
        name: &BucketName,
    ) -> Result<BucketInfo, MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET active_write_reservations = active_write_reservations + 1 \
                 WHERE name = ?1 AND state = ?2 AND write_reservations_blocked = 0",
                params![name.as_str(), BucketState::Active as u8],
            )
            .map_err(|e| MetadataError::Db {
                context: "acquire bucket write reservation",
                source: e,
            })?;
        if updated == 1 {
            return self.head_bucket_raw(name);
        }
        let info = self.head_bucket_raw(name)?;
        if info.state == BucketState::Active && info.write_reservations_blocked {
            return Err(MetadataError::BucketWriteDraining);
        }
        Err(bucket_not_found(name.as_str()))
    }

    fn release_bucket_write_reservation(&self, name: &BucketName) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET active_write_reservations = active_write_reservations - 1 \
                 WHERE name = ?1 AND active_write_reservations > 0",
                params![name.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "release bucket write reservation",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn begin_bucket_write_drain(&self, name: &BucketName) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET write_reservations_blocked = 1 \
                 WHERE name = ?1 AND state = ?2 AND write_reservations_blocked = 0",
                params![name.as_str(), BucketState::Active as u8],
            )
            .map_err(|e| MetadataError::Db {
                context: "begin bucket write drain",
                source: e,
            })?;
        if updated == 0 {
            let info = self.head_bucket_raw(name)?;
            if info.state == BucketState::Active && info.write_reservations_blocked {
                return Err(MetadataError::BucketWriteDraining);
            }
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    fn end_bucket_write_drain(&self, name: &BucketName) -> Result<(), MetadataError> {
        let updated = self
            .conn
            .execute(
                "UPDATE buckets \
                 SET write_reservations_blocked = 0 \
                 WHERE name = ?1 AND state = ?2 AND write_reservations_blocked = 1",
                params![name.as_str(), BucketState::Active as u8],
            )
            .map_err(|e| MetadataError::Db {
                context: "end bucket write drain",
                source: e,
            })?;
        if updated == 0 {
            return Err(bucket_not_found(name.as_str()));
        }
        Ok(())
    }

    #[cfg(test)]
    fn put_bucket_versioning(
        &self,
        name: &BucketName,
        state: BucketVersioningState,
    ) -> Result<(), MetadataError> {
        self.put_bucket_versioning_inner(name, state, BucketExecutionGeneration::Allocate)
    }

    #[cfg(test)]
    fn put_bucket_object_lock(
        &self,
        name: &BucketName,
        config: BucketObjectLockConfig,
    ) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::ObjectLock(config),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn put_bucket_acl(
        &self,
        name: &BucketName,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<(), MetadataError> {
        self.put_bucket_acl_inner(
            name,
            acl_grants,
            public_read,
            public_write,
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn put_bucket_subresource(
        &self,
        name: &BucketName,
        req: PutBucketSubresource<'_>,
    ) -> Result<(), MetadataError> {
        self.put_bucket_subresource_internal(name, req.kind, req.body, req.aux)
    }

    fn get_bucket_subresource(
        &self,
        name: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<StoredBucketSubresource>, MetadataError> {
        self.get_bucket_subresource_internal(name.as_str(), kind)
    }

    #[cfg(test)]
    fn delete_bucket_subresource(
        &self,
        name: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<(), MetadataError> {
        self.delete_bucket_subresource_internal(name, kind)
    }

    #[cfg(test)]
    fn put_bucket_public_access_block(
        &self,
        name: &BucketName,
        config: PublicAccessBlockConfig,
    ) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::PublicAccessBlock(Some(config)),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn get_bucket_public_access_block(
        &self,
        name: &BucketName,
    ) -> Result<Option<PublicAccessBlockConfig>, MetadataError> {
        self.conn
            .query_row(
                "SELECT \
                     public_access_block_present, \
                     public_access_block_block_public_acls, \
                     public_access_block_ignore_public_acls, \
                     public_access_block_block_public_policy, \
                     public_access_block_restrict_public_buckets \
                 FROM buckets WHERE name = ?1",
                params![name.as_str()],
                |row| {
                    Self::parse_public_access_block(
                        (
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                            row.get::<_, i64>(4)?,
                        ),
                        [0, 1, 2, 3, 4],
                    )
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket public access block",
                source: e,
            })?
            .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    #[cfg(test)]
    fn delete_bucket_public_access_block(&self, name: &BucketName) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::PublicAccessBlock(None),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn put_bucket_ownership_controls(
        &self,
        name: &BucketName,
        config: BucketOwnershipControls,
    ) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::OwnershipControls(Some(config)),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn get_bucket_ownership_controls(
        &self,
        name: &BucketName,
    ) -> Result<Option<BucketOwnershipControls>, MetadataError> {
        self.conn
            .query_row(
                "SELECT ownership_controls_mode FROM buckets WHERE name = ?1",
                params![name.as_str()],
                |row| Self::parse_ownership_controls(row.get(0)?, 0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket ownership controls",
                source: e,
            })?
            .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    #[cfg(test)]
    fn delete_bucket_ownership_controls(&self, name: &BucketName) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::OwnershipControls(None),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn put_bucket_abac_enabled(
        &self,
        name: &BucketName,
        enabled: bool,
    ) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::AbacEnabled(enabled),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn get_bucket_abac_enabled(&self, name: &BucketName) -> Result<bool, MetadataError> {
        self.conn
            .query_row(
                "SELECT bucket_abac_enabled FROM buckets WHERE name = ?1",
                params![name.as_str()],
                |row| Ok(row.get::<_, i64>(0)? != 0),
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => bucket_not_found(name.as_str()),
                source => MetadataError::Db {
                    context: "get bucket abac enabled",
                    source,
                },
            })
    }

    #[cfg(test)]
    fn put_bucket_encryption(
        &self,
        name: &BucketName,
        config: BucketEncryptionConfig,
    ) -> Result<(), MetadataError> {
        self.put_bucket_property_inner(
            name,
            &BucketPropertyMutation::Encryption(config),
            BucketExecutionGeneration::Allocate,
        )
    }

    #[cfg(test)]
    fn get_bucket_encryption(
        &self,
        name: &BucketName,
    ) -> Result<BucketEncryptionConfig, MetadataError> {
        self.conn
            .query_row(
                "SELECT default_encryption_type, sse_c_blocked FROM buckets WHERE name = ?1",
                params![name.as_str()],
                |row| {
                    Ok(BucketEncryptionConfig {
                        default_encryption: row
                            .get::<_, Option<u8>>(0)?
                            .map(|value| {
                                ManagedEncryptionAlgorithm::from_u8(value).ok_or_else(|| {
                                    rusqlite::Error::FromSqlConversionFailure(
                                        0,
                                        rusqlite::types::Type::Integer,
                                        Box::from(format!(
                                            "invalid default_encryption_type: {value}"
                                        )),
                                    )
                                })
                            })
                            .transpose()?,
                        sse_c_blocked: row.get::<_, i64>(1)? != 0,
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get bucket encryption",
                source: e,
            })?
            .ok_or_else(|| bucket_not_found(name.as_str()))
    }

    #[cfg(test)]
    fn put_object_meta(&self, req: &PutObjectReq) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::put_object_meta",
            "pg_id={}",
            self.pg_id
        );
        let now = PgStore::now_millis();
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "put object meta (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| match req {
            PutObjectReq::Live(req) => {
                let write_sequence =
                    self.next_object_write_sequence(req.bucket.as_str(), req.key.as_str())?;
                req.validate().map_err(|msg| MetadataError::Db {
                    context: "put object meta (etag/layout mismatch)",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::from(msg),
                    ),
                })?;
                self.mark_current_live_noncurrent(
                    req.bucket.as_str(),
                    req.key.as_str(),
                    req.version_id,
                    now,
                )
                .map_err(|e| MetadataError::Db {
                    context: "put object meta (mark noncurrent)",
                    source: e,
                })?;
                let data_layout_u8 = req.layout.data_layout() as u8;
                let etag_kind_u8 = req.etag.etag_kind() as u8;
                let status_u8 = ObjectState::Live as u8;
                let parts_count = req.layout.parts_count().map(|n| n as i64);
                let tags = req.tags.as_ref().map(SerializedTagSet::as_str);
                let metadata_blob = req
                    .metadata_blob
                    .as_ref()
                    .map(SerializedMetadataBlob::as_slice);
                let system_metadata_blob = req
                    .system_metadata_blob
                    .as_ref()
                    .map(SerializedSystemMetadataBlob::as_slice);
                let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
                    Self::object_lock_sql_values(req.object_lock).map_err(|e| {
                        MetadataError::Db {
                            context: "put object meta (encode object lock)",
                            source: e,
                        }
                    })?;
                let encryption_type = req.encryption.encryption_type() as u8;
                let encryption_state = req.encryption.encode_state();
                self.advance_object_version_counter_in_open_txn(
                    &req.bucket,
                    &req.key,
                    req.version_id,
                )?;
                let sql = if req.version_id.is_null() {
                    "INSERT OR REPLACE INTO objects \
                     (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
                } else {
                    "INSERT INTO objects \
                     (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                      storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
                };
                self.execute_cached_metadata(
                    sql,
                    params![
                        req.bucket,
                        req.key,
                        req.version_id.to_u64() as i64,
                        write_sequence as i64,
                        req.generation_id.get() as i64,
                        req.size as i64,
                        req.etag.as_bytes().as_slice(),
                        etag_kind_u8,
                        now as i64,
                        req.ec.k,
                        req.ec.m,
                        status_u8,
                        data_layout_u8,
                        parts_count,
                        tags,
                        metadata_blob,
                        system_metadata_blob,
                        encryption_type,
                        encryption_state,
                        req.owner.principal,
                        req.owner.canonical_id.as_str(),
                        req.acl_grants.serialized(),
                        i32::from(req.public_read),
                        object_lock_retention_mode,
                        object_lock_retain_until,
                        object_lock_legal_hold,
                    ],
                    "put object meta",
                )?;
                Ok(())
            }
            PutObjectReq::DeleteMarker(req) => {
                let write_sequence =
                    self.next_object_write_sequence(req.bucket.as_str(), req.key.as_str())?;
                self.put_delete_marker_explicit_in_open_txn(&InsertDeleteMarkerCommand {
                    bucket: req.bucket.clone(),
                    key: req.key.clone(),
                    version_id: req.version_id,
                    owner: req.owner.clone(),
                    write_sequence,
                    last_modified_millis: now,
                    stale_payload: None,
                })
            }
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "put object meta (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn get_object_meta(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<StoredObject, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::get_object_meta",
            "pg_id={} bucket={:?} key={:?}",
            self.pg_id,
            bucket,
            key
        );
        self.query_row_cached_optional_metadata(
            "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
             last_modified, storage_class, ec_k, ec_m, status, tags, \
             data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold \
             , became_noncurrent_at \
             FROM objects WHERE bucket = ?1 AND key = ?2 \
             ORDER BY write_sequence DESC LIMIT 1",
            params![bucket, key],
            "get object meta",
            Self::row_to_object_record,
        )?
            .ok_or(MetadataError::ObjectNotFound)
    }

    fn get_object_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<StoredObject, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::get_object_version",
            "pg_id={} bucket={:?} key={:?} version_id={}",
            self.pg_id,
            bucket,
            key,
            version_id
        );
        self.query_row_cached_optional_metadata(
            "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
             last_modified, storage_class, ec_k, ec_m, status, tags, \
             data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold \
             , became_noncurrent_at \
             FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
            params![bucket, key, version_id.to_u64() as i64],
            "get object version",
            Self::row_to_object_record,
        )?
            .ok_or(MetadataError::ObjectNotFound)
    }

    #[cfg(test)]
    fn put_object_acl(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        acl_grants: &AclGrants,
        public_read: bool,
    ) -> Result<(), MetadataError> {
        let updated = self.execute_cached_metadata(
            "UPDATE objects SET acl_grants = ?1, public_read = ?2 \
             WHERE bucket = ?3 AND key = ?4 AND version_id = ?5 AND status = ?6",
            params![
                acl_grants.serialized(),
                i32::from(public_read),
                bucket,
                key,
                version_id.to_u64() as i64,
                ObjectState::Live as u8
            ],
            "put object acl",
        )?;
        if updated == 0 {
            let status = self.query_row_cached_optional_metadata(
                "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                "put object acl (check status)",
                |row| row.get::<_, u8>(0),
            )?;
            return match status {
                Some(v) if v == ObjectState::DeleteMarker as u8 => {
                    Err(MetadataError::MethodNotAllowedOnDeleteMarker)
                }
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    #[cfg(test)]
    fn put_object_retention(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        retention: ObjectRetention,
    ) -> Result<(), MetadataError> {
        let retain_until =
            i64::try_from(retention.retain_until_unix_seconds).map_err(|_| MetadataError::Db {
                context: "put object retention (encode retain-until)",
                source: rusqlite::Error::ToSqlConversionFailure(Box::from(format!(
                    "object lock retain-until exceeds SQLite INTEGER: {}",
                    retention.retain_until_unix_seconds
                ))),
            })?;
        let updated = self.execute_cached_metadata(
            "UPDATE objects \
             SET object_lock_retention_mode = ?1, object_lock_retain_until = ?2 \
             WHERE bucket = ?3 AND key = ?4 AND version_id = ?5 AND status = ?6",
            params![
                retention.mode as u8,
                retain_until,
                bucket,
                key,
                version_id.to_u64() as i64,
                ObjectState::Live as u8
            ],
            "put object retention",
        )?;
        if updated == 0 {
            let status = self.query_row_cached_optional_metadata(
                "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                "put object retention (check status)",
                |row| row.get::<_, u8>(0),
            )?;
            return match status {
                Some(v) if v == ObjectState::DeleteMarker as u8 => {
                    Err(MetadataError::MethodNotAllowedOnDeleteMarker)
                }
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    #[cfg(test)]
    fn put_object_legal_hold(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        legal_hold: StoredLegalHoldStatus,
    ) -> Result<(), MetadataError> {
        let updated = self.execute_cached_metadata(
            "UPDATE objects SET object_lock_legal_hold = ?1 \
             WHERE bucket = ?2 AND key = ?3 AND version_id = ?4 AND status = ?5",
            params![
                legal_hold as u8,
                bucket,
                key,
                version_id.to_u64() as i64,
                ObjectState::Live as u8
            ],
            "put object legal hold",
        )?;
        if updated == 0 {
            let status = self.query_row_cached_optional_metadata(
                "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                "put object legal hold (check status)",
                |row| row.get::<_, u8>(0),
            )?;
            return match status {
                Some(v) if v == ObjectState::DeleteMarker as u8 => {
                    Err(MetadataError::MethodNotAllowedOnDeleteMarker)
                }
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    #[cfg(test)]
    fn delete_object_meta(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::delete_object_meta",
            "pg_id={} bucket={:?} key={:?}",
            self.pg_id,
            bucket,
            key
        );
        self.conn
            .execute(
                "DELETE FROM objects WHERE bucket = ?1 AND key = ?2",
                params![bucket, key],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object meta",
                source: e,
            })?;
        Ok(())
    }

    #[cfg(test)]
    fn delete_object_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::delete_object_version",
            "pg_id={} bucket={:?} key={:?} version_id={}",
            self.pg_id,
            bucket,
            key,
            version_id
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "delete object version (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> =
            self.delete_object_version_in_open_txn(bucket, key, version_id);

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "delete object version (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn list_objects(&self, req: &ListObjectsReq) -> Result<ListObjectsResp, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_objects",
            "pg_id={} bucket={:?} max_keys={}",
            self.pg_id,
            req.bucket.as_str(),
            req.max_keys
        );
        // Select the current record per key using the durable per-key write
        // sequence. Timestamp equality cannot reliably determine currentness
        // for suspended buckets because a newer null version may share the
        // same last_modified millisecond as an older numbered version.
        let limit = req.max_keys as i64 + 1;

        // Build WHERE clause fragments for key filtering
        let mut where_clauses = vec!["o.bucket = ?1".to_string()];
        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(req.bucket.clone())];
        let mut param_idx = 2;

        if let Some(ref start_after) = req.start_after {
            where_clauses.push(format!("o.key > ?{param_idx}"));
            params_vec.push(Box::new(start_after.clone()));
            param_idx += 1;
        } else if let Some(ref start_at) = req.start_at {
            where_clauses.push(format!("o.key >= ?{param_idx}"));
            params_vec.push(Box::new(start_at.clone()));
            param_idx += 1;
        }

        if let Some(ref prefix) = req.prefix {
            where_clauses.push(format!("o.key >= ?{param_idx}"));
            params_vec.push(Box::new(prefix.clone()));
            param_idx += 1;

            if let Some(end) = object_key_prefix_upper_bound(prefix) {
                where_clauses.push(format!("o.key < ?{param_idx}"));
                params_vec.push(Box::new(end));
                param_idx += 1;
            }
        }

        let where_str = where_clauses.join(" AND ");

        let sql = format!(
            "SELECT o.bucket, o.key, o.version_id, o.generation_id, o.size, o.etag, o.etag_kind, \
                   o.last_modified, o.storage_class, o.ec_k, o.ec_m, o.status, o.tags, \
                   o.data_layout, o.parts_count, o.metadata_blob, o.system_metadata_blob, \
                   o.encryption_type, o.encryption_state, o.owner_principal, o.owner_canonical_id, \
                   o.acl_grants, o.public_read, o.object_lock_retention_mode, \
                   o.object_lock_retain_until, o.object_lock_legal_hold, \
                   o.became_noncurrent_at \
            FROM objects o \
            WHERE {where_str} AND o.status = 0 \
              AND NOT EXISTS ( \
                  SELECT 1 FROM objects newer \
                  WHERE newer.bucket = o.bucket AND newer.key = o.key \
                    AND newer.write_sequence > o.write_sequence \
              ) \
            ORDER BY o.key ASC LIMIT ?{param_idx}"
        );
        params_vec.push(Box::new(limit));

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|e| MetadataError::Db {
                context: "prepare list objects",
                source: e,
            })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), Self::row_to_object_record)
            .map_err(|e| MetadataError::Db {
                context: "list objects query",
                source: e,
            })?;

        let mut objects: Vec<StoredObject> = Vec::new();
        for row in rows {
            objects.push(row.map_err(|e| MetadataError::Db {
                context: "list objects row",
                source: e,
            })?);
        }

        let is_truncated = objects.len() as i64 > req.max_keys as i64;
        if is_truncated {
            objects.truncate(req.max_keys as usize);
        }

        let next_start_after = if is_truncated {
            objects.last().map(|o| o.key().clone())
        } else {
            None
        };

        Ok(ListObjectsResp {
            objects,
            is_truncated,
            next_start_after,
        })
    }

    fn list_object_versions(
        &self,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_object_versions",
            "pg_id={} bucket={:?} max_keys={}",
            self.pg_id,
            req.bucket.as_str(),
            req.max_keys
        );
        let limit = req.max_keys as i64 + 1;

        let mut where_clauses = vec!["bucket = ?1".to_string()];
        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(req.bucket.clone())];
        let mut param_idx = 2;

        if let Some(ref start_at) = req.start_at {
            where_clauses.push(format!("key >= ?{param_idx}"));
            params_vec.push(Box::new(start_at.clone()));
            param_idx += 1;
        } else if let Some(ref key_marker) = req.key_marker {
            if let Some(vid_marker) = req.version_id_marker {
                if let Some(write_sequence) = self.object_write_sequence(
                    req.bucket.as_str(),
                    key_marker.as_str(),
                    vid_marker,
                )? {
                    where_clauses.push(format!(
                        "(key > ?{} OR (key = ?{} AND write_sequence < ?{}))",
                        param_idx,
                        param_idx,
                        param_idx + 1
                    ));
                    params_vec.push(Box::new(key_marker.clone()));
                    params_vec.push(Box::new(write_sequence as i64));
                    param_idx += 2;
                } else {
                    where_clauses.push(format!("key > ?{param_idx}"));
                    params_vec.push(Box::new(key_marker.clone()));
                    param_idx += 1;
                }
            } else {
                where_clauses.push(format!("key > ?{param_idx}"));
                params_vec.push(Box::new(key_marker.clone()));
                param_idx += 1;
            }
        }

        if let Some(ref prefix) = req.prefix {
            where_clauses.push(format!("key >= ?{param_idx}"));
            params_vec.push(Box::new(prefix.clone()));
            param_idx += 1;

            if let Some(end) = object_key_prefix_upper_bound(prefix) {
                where_clauses.push(format!("key < ?{param_idx}"));
                params_vec.push(Box::new(end));
                param_idx += 1;
            }
        }

        let where_str = where_clauses.join(" AND ");

        let sql = format!(
            "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
             last_modified, storage_class, ec_k, ec_m, status, tags, \
             data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold \
             , became_noncurrent_at \
             FROM objects \
             WHERE {where_str} \
             ORDER BY key ASC, write_sequence DESC LIMIT ?{param_idx}"
        );
        params_vec.push(Box::new(limit));

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|e| MetadataError::Db {
                context: "prepare list object versions",
                source: e,
            })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), Self::row_to_object_record)
            .map_err(|e| MetadataError::Db {
                context: "list object versions query",
                source: e,
            })?;

        let mut versions: Vec<StoredObject> = Vec::new();
        for row in rows {
            versions.push(row.map_err(|e| MetadataError::Db {
                context: "list object versions row",
                source: e,
            })?);
        }

        let is_truncated = versions.len() as i64 > req.max_keys as i64;
        if is_truncated {
            versions.truncate(req.max_keys as usize);
        }

        let (next_key_marker, next_version_id_marker) = if is_truncated {
            versions.last().map_or((None, None), |o| {
                (Some(o.key().clone()), Some(o.version_id()))
            })
        } else {
            (None, None)
        };

        Ok(ListObjectVersionsResp {
            versions,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
        })
    }

    fn list_object_versions_for_key(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, version_id, generation_id, size, etag, etag_kind, \
                 last_modified, storage_class, ec_k, ec_m, status, tags, \
                 data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold, became_noncurrent_at \
                 FROM objects \
                 WHERE bucket = ?1 AND key = ?2 \
                 ORDER BY write_sequence DESC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list object versions for key",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![bucket, key], Self::row_to_object_record)
            .map_err(|e| MetadataError::Db {
                context: "list object versions for key query",
                source: e,
            })?;

        let mut versions = Vec::new();
        for row in rows {
            versions.push(row.map_err(|e| MetadataError::Db {
                context: "list object versions for key row",
                source: e,
            })?);
        }
        Ok(versions)
    }

    fn next_version_id(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, MetadataError> {
        let (max_existing, stored_next): (Option<i64>, Option<i64>) = self
            .query_row_cached_metadata(
                "SELECT \
                    (SELECT MAX(version_id) FROM objects WHERE bucket = ?1 AND key = ?2), \
                    (SELECT next_version_id FROM object_version_counters \
                         WHERE bucket = ?1 AND key = ?2)",
                params![bucket, key],
                "next version id",
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
        let next_from_rows = match max_existing {
            None => 1,
            Some(v) => {
                let current = u64::try_from(v).map_err(|_| MetadataError::Db {
                    context: "negative version_id in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(version_id): {v}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "version_id overflow",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("MAX(version_id) overflow"),
                    ),
                })?
            }
        };

        let next_from_counter = match stored_next {
            None => 1,
            Some(v) => u64::try_from(v).map_err(|_| MetadataError::Db {
                context: "negative next_version_id in database",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from(format!("negative next_version_id: {v}")),
                ),
            })?,
        };
        let next = next_from_rows.max(next_from_counter);
        next.checked_add(1).ok_or_else(|| MetadataError::Db {
            context: "version_id overflow",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("next_version_id overflow"),
            ),
        })?;
        Ok(VersionId::from_u64(next))
    }

    fn next_generation_id(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, MetadataError> {
        let max: Option<i64> = self
            .query_row_cached_optional_metadata(
                "SELECT MAX(generation_id) FROM (
                     SELECT generation_id FROM objects WHERE bucket = ?1 AND key = ?2
                     UNION ALL
                     SELECT generation_id FROM object_segments_reclaims WHERE bucket = ?1 AND key = ?2
                     UNION ALL
                     SELECT generation_id FROM multipart_reclaims WHERE bucket = ?1 AND key = ?2
                     UNION ALL
                     SELECT object_generation_id FROM multipart_uploads WHERE bucket = ?1 AND key = ?2
                     UNION ALL
                     SELECT generation_id FROM object_generation_reservations WHERE bucket = ?1 AND key = ?2
                 )",
                params![bucket, key],
                "next generation id",
                |row| row.get(0),
            )?
            .flatten();

        let next = match max {
            None => 1u64,
            Some(v) => {
                let current = u64::try_from(v).map_err(|_| MetadataError::Db {
                    context: "negative generation_id in database",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from(format!("negative MAX(generation_id): {v}")),
                    ),
                })?;
                current.checked_add(1).ok_or_else(|| MetadataError::Db {
                    context: "generation_id overflow",
                    source: rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Integer,
                        Box::from("MAX(generation_id) overflow"),
                    ),
                })?
            }
        };
        GenerationId::new(next).ok_or_else(|| MetadataError::Db {
            context: "invalid next generation id",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::from("next generation id must be nonzero"),
            ),
        })
    }

    #[cfg(test)]
    fn reserve_object_generation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "reserve object generation (begin txn)",
                source: e,
            })?;

        let result: Result<GenerationId, MetadataError> = (|| {
            let generation_id = self.next_generation_id(bucket, key)?;
            self.execute_cached_metadata(
                "INSERT INTO object_generation_reservations \
                 (reservation_id, bucket, key, generation_id, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    reservation_id.as_str(),
                    bucket,
                    key,
                    generation_id.get() as i64,
                    PgStore::now_millis() as i64,
                ],
                "reserve object generation (insert reservation)",
            )?;
            Ok(generation_id)
        })();

        match result {
            Ok(generation_id) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "reserve object generation (commit txn)",
                        source: e,
                    });
                }
                Ok(generation_id)
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn get_object_generation_reservation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, MetadataError> {
        let raw: i64 = self
            .query_row_cached_optional_metadata(
                "SELECT generation_id FROM object_generation_reservations \
                 WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3",
                params![reservation_id.as_str(), bucket, key],
                "get object generation reservation",
                |row| row.get(0),
            )?
            .ok_or_else(|| MetadataError::ObjectGenerationReservationNotFound {
                reservation_id: reservation_id.as_str().to_owned(),
            })?;
        Self::parse_generation_id(raw, 0, "generation_id").map_err(|source| MetadataError::Db {
            context: "parse object generation reservation",
            source,
        })
    }

    #[cfg(test)]
    fn delete_object_generation_reservation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<(), MetadataError> {
        self.delete_object_generation_reservation_direct(bucket, key, reservation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn put_object_segments_reclaim(
        &self,
        reclaim: &ObjectSegmentsReclaimRecord,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "put object segments reclaim (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO object_segments_reclaims \
                     (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![
                        reclaim.bucket,
                        reclaim.key,
                        reclaim.generation_id.get() as i64,
                        reclaim.created_at as i64,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put object segments reclaim (root)",
                    source: e,
                })?;

            for segment in &reclaim.segments {
                self.conn
                    .execute(
                        "INSERT OR REPLACE INTO object_segment_reclaim_segments \
                         (bucket, key, generation_id, segment_index, segment_okh, segment_vid, \
                          data_pg_id, ec_k, ec_m) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                        params![
                            reclaim.bucket,
                            reclaim.key,
                            reclaim.generation_id.get() as i64,
                            segment.segment_index as i64,
                            &segment.segment_okh[..],
                            segment.segment_vid.get() as i64,
                            segment.data_pg_id as i64,
                            segment.ec.k,
                            segment.ec.m,
                        ],
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "put object segments reclaim (segment)",
                        source: e,
                    })?;
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "put object segments reclaim (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn get_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectSegmentsReclaimRecord>, MetadataError> {
        let root = self
            .conn
            .query_row(
                "SELECT bucket, key, generation_id, created_at \
                 FROM object_segments_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
                |row| {
                    Ok((
                        row.get::<_, BucketName>(0)?,
                        row.get::<_, ObjectKey>(1)?,
                        Self::parse_generation_id(row.get::<_, i64>(2)?, 2, "generation_id")?,
                        row.get::<_, i64>(3)? as u64,
                    ))
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object segments reclaim (root)",
                source: e,
            })?;

        let Some((bucket_name, key_name, generation_id, created_at)) = root else {
            return Ok(None);
        };

        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT segment_index, segment_okh, segment_vid, data_pg_id, ec_k, ec_m \
                 FROM object_segment_reclaim_segments \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3 \
                 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "get object segments reclaim (prepare segments)",
                source: e,
            })?;

        let segments = stmt
            .query_map(params![bucket, key, generation_id.get() as i64], |row| {
                Ok(ObjectSegmentsReclaimSegmentRecord {
                    segment_index: row.get::<_, i64>(0)? as u32,
                    segment_okh: row.get_ref(1)?.as_blob()?.try_into().map_err(|_| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Blob,
                            Box::from("segment_okh must be 16 bytes"),
                        )
                    })?,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(2)?,
                        2,
                        "segment_vid",
                    )?,
                    data_pg_id: row.get::<_, i64>(3)? as u32,
                    ec: EcShape {
                        k: row.get(4)?,
                        m: row.get(5)?,
                    },
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "get object segments reclaim (query segments)",
                source: e,
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "get object segments reclaim (collect segments)",
                source: e,
            })?;

        Ok(Some(ObjectSegmentsReclaimRecord {
            bucket: bucket_name,
            key: key_name,
            generation_id,
            created_at,
            segments,
        }))
    }

    #[cfg(test)]
    fn delete_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError> {
        self.delete_object_segments_reclaim_direct(bucket, key, generation_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn put_multipart_reclaim(&self, reclaim: &MultipartReclaimRecord) -> Result<(), MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "put multipart reclaim (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| {
            self.conn
                .execute(
                    "INSERT OR REPLACE INTO multipart_reclaims \
                     (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![
                        reclaim.bucket,
                        reclaim.key,
                        reclaim.generation_id.get() as i64,
                        reclaim.created_at as i64,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put multipart reclaim (root)",
                    source: e,
                })?;

            for part in &reclaim.parts {
                match part {
                    MultipartReclaimPartRecord::ShardSet {
                        part_number,
                        part_okh,
                        part_vid,
                        data_pg_id,
                        ec,
                    } => {
                        self.conn
                            .execute(
                                "INSERT OR REPLACE INTO multipart_reclaim_parts \
                                 (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                                  part_vid, data_pg_id, ec_k, ec_m) \
                                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                                params![
                                    reclaim.bucket,
                                    reclaim.key,
                                    reclaim.generation_id.get() as i64,
                                    *part_number as i64,
                                    MultipartReclaimPartKind::ShardSet as u8,
                                    &part_okh[..],
                                    part_vid.get() as i64,
                                    *data_pg_id as i64,
                                    ec.k,
                                    ec.m,
                                ],
                            )
                            .map_err(|e| MetadataError::Db {
                                context: "put multipart reclaim (part shard set)",
                                source: e,
                            })?;
                    }
                    MultipartReclaimPartRecord::Segments {
                        part_number,
                        segments,
                    } => {
                        self.conn
                            .execute(
                                "INSERT OR REPLACE INTO multipart_reclaim_parts \
                                 (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                                  part_vid, data_pg_id, ec_k, ec_m) \
                                 VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, NULL, NULL, NULL)",
                                params![
                                    reclaim.bucket,
                                    reclaim.key,
                                    reclaim.generation_id.get() as i64,
                                    *part_number as i64,
                                    MultipartReclaimPartKind::Segments as u8,
                                ],
                            )
                            .map_err(|e| MetadataError::Db {
                                context: "put multipart reclaim (part segments)",
                                source: e,
                            })?;

                        for segment in segments {
                            self.conn
                                .execute(
                                    "INSERT OR REPLACE INTO multipart_reclaim_part_segments \
                                     (bucket, key, generation_id, part_number, segment_index, \
                                      segment_okh, segment_vid, data_pg_id, ec_k, ec_m) \
                                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                                    params![
                                        reclaim.bucket,
                                        reclaim.key,
                                        reclaim.generation_id.get() as i64,
                                        segment.part_number as i64,
                                        segment.segment_index as i64,
                                        &segment.segment_okh[..],
                                        segment.segment_vid.get() as i64,
                                        segment.data_pg_id as i64,
                                        segment.ec.k,
                                        segment.ec.m,
                                    ],
                                )
                                .map_err(|e| MetadataError::Db {
                                    context: "put multipart reclaim (part segment)",
                                    source: e,
                                })?;
                        }
                    }
                }
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "put multipart reclaim (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn get_multipart_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<MultipartReclaimRecord>, MetadataError> {
        let root = self
            .conn
            .query_row(
                "SELECT bucket, key, generation_id, created_at \
                 FROM multipart_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
                |row| {
                    Ok((
                        row.get::<_, BucketName>(0)?,
                        row.get::<_, ObjectKey>(1)?,
                        Self::parse_generation_id(row.get::<_, i64>(2)?, 2, "generation_id")?,
                        row.get::<_, i64>(3)? as u64,
                    ))
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get multipart reclaim (root)",
                source: e,
            })?;

        let Some((bucket_name, key_name, generation_id, created_at)) = root else {
            return Ok(None);
        };

        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT part_number, storage_kind, part_okh, part_vid, data_pg_id, ec_k, ec_m \
                 FROM multipart_reclaim_parts \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3 \
                 ORDER BY part_number ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "get multipart reclaim (prepare parts)",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![bucket, key, generation_id.get() as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)? as u32,
                    Self::parse_enum(
                        row.get::<_, u8>(1)?,
                        1,
                        "storage_kind",
                        MultipartReclaimPartKind::from_u8,
                    )?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                    row.get::<_, Option<i64>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, Option<u8>>(5)?,
                    row.get::<_, Option<u8>>(6)?,
                ))
            })
            .map_err(|e| MetadataError::Db {
                context: "get multipart reclaim (query parts)",
                source: e,
            })?;

        let mut parts = Vec::new();
        for row in rows {
            let (part_number, kind, part_okh, part_vid, data_pg_id, ec_k, ec_m) =
                row.map_err(|e| MetadataError::Db {
                    context: "get multipart reclaim (part row)",
                    source: e,
                })?;
            match kind {
                MultipartReclaimPartKind::ShardSet => {
                    let part_okh = part_okh.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing part_okh)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing part_okh"),
                        ),
                    })?;
                    let part_okh =
                        Self::parse_okh_blob(&part_okh, 2).map_err(|e| MetadataError::Db {
                            context: "get multipart reclaim (invalid part_okh)",
                            source: e,
                        })?;
                    let part_vid = part_vid.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing part_vid)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing part_vid"),
                        ),
                    })?;
                    let data_pg_id = data_pg_id.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing data_pg_id)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing data_pg_id"),
                        ),
                    })?;
                    let ec_k = ec_k.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing ec_k)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            5,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing ec_k"),
                        ),
                    })?;
                    let ec_m = ec_m.ok_or_else(|| MetadataError::Db {
                        context: "get multipart reclaim (missing ec_m)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            6,
                            rusqlite::types::Type::Null,
                            Box::from("shard-set reclaim part missing ec_m"),
                        ),
                    })?;
                    parts.push(MultipartReclaimPartRecord::ShardSet {
                        part_number,
                        part_okh,
                        part_vid: Self::parse_generation_id(part_vid, 3, "part_vid").map_err(
                            |e| MetadataError::Db {
                                context: "get multipart reclaim (invalid part_vid)",
                                source: e,
                            },
                        )?,
                        data_pg_id: data_pg_id as u32,
                        ec: EcShape { k: ec_k, m: ec_m },
                    });
                }
                MultipartReclaimPartKind::Segments => {
                    let mut segment_stmt = self
                        .conn
                        .prepare_cached(
                            "SELECT part_number, segment_index, segment_okh, segment_vid, data_pg_id, ec_k, ec_m \
                             FROM multipart_reclaim_part_segments \
                             WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3 AND part_number = ?4 \
                             ORDER BY segment_index ASC",
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "get multipart reclaim (prepare part segments)",
                            source: e,
                        })?;

                    let segments = segment_stmt
                        .query_map(
                            params![bucket, key, generation_id.get() as i64, part_number],
                            |row| {
                                let segment_okh = Self::blob_to_okh(row.get(2)?, 2)?;
                                Ok(MultipartReclaimPartSegmentRecord {
                                    part_number: row.get::<_, i64>(0)? as u32,
                                    segment_index: row.get::<_, i64>(1)? as u32,
                                    segment_okh,
                                    segment_vid: Self::parse_generation_id(
                                        row.get::<_, i64>(3)?,
                                        3,
                                        "segment_vid",
                                    )?,
                                    data_pg_id: row.get::<_, i64>(4)? as u32,
                                    ec: EcShape {
                                        k: row.get(5)?,
                                        m: row.get(6)?,
                                    },
                                })
                            },
                        )
                        .map_err(|e| MetadataError::Db {
                            context: "get multipart reclaim (query part segments)",
                            source: e,
                        })?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| MetadataError::Db {
                            context: "get multipart reclaim (collect part segments)",
                            source: e,
                        })?;

                    parts.push(MultipartReclaimPartRecord::Segments {
                        part_number,
                        segments,
                    });
                }
            }
        }

        Ok(Some(MultipartReclaimRecord {
            bucket: bucket_name,
            key: key_name,
            generation_id,
            created_at,
            parts,
        }))
    }

    #[cfg(test)]
    fn delete_multipart_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError> {
        self.delete_multipart_reclaim_direct(bucket, key, generation_id)
    }

    fn payload_reclaim_exists(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, MetadataError> {
        self.query_row_cached_metadata(
            "SELECT
                EXISTS(
                    SELECT 1 FROM object_segments_reclaims
                    WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3
                )
                OR EXISTS(
                    SELECT 1 FROM multipart_reclaims
                    WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3
                )",
            params![bucket, key, generation_id.get() as i64],
            "payload reclaim exists",
            |row| Ok(row.get::<_, i64>(0)? != 0),
        )
    }

    fn get_bucket_payload_reclaim_root(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, MetadataError> {
        self.query_row_cached_optional_metadata(
            "SELECT bucket, key, generation_id FROM (
                 SELECT bucket, key, generation_id FROM object_segments_reclaims WHERE bucket = ?1
                 UNION ALL
                 SELECT bucket, key, generation_id FROM multipart_reclaims WHERE bucket = ?1
             )
             ORDER BY key ASC, generation_id ASC
             LIMIT 1",
            params![bucket],
            "get bucket payload reclaim root",
            |row| {
                Ok(PayloadReclaimRoot {
                    bucket: row.get(0)?,
                    key: row.get(1)?,
                    generation_id: Self::parse_generation_id(
                        row.get::<_, i64>(2)?,
                        2,
                        "generation_id",
                    )?,
                })
            },
        )
    }

    #[cfg(test)]
    fn put_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        tags: &str,
    ) -> Result<(), MetadataError> {
        let updated = self.execute_cached_metadata(
            "UPDATE objects SET tags = ?1 WHERE bucket = ?2 AND key = ?3 AND version_id = ?4 AND status = 0",
            params![tags, bucket, key, version_id.to_u64() as i64],
            "put object tags",
        )?;
        if updated == 0 {
            let status: Option<u8> = self.query_row_cached_optional_metadata(
                "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                "put object tags (check status)",
                |row| row.get(0),
            )?;
            return match status {
                Some(1) => Err(MetadataError::MethodNotAllowedOnDeleteMarker),
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    fn get_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Option<String>, MetadataError> {
        let result = self
            .query_row_cached_optional_metadata(
                "SELECT tags FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND status = 0",
                params![bucket, key, version_id.to_u64() as i64],
                "get object tags",
                |row| row.get(0),
            )?;
        if let Some(tags) = result {
            Ok(tags)
        } else {
            let status: Option<u8> = self.query_row_cached_optional_metadata(
                "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                "get object tags (check status)",
                |row| row.get(0),
            )?;
            match status {
                Some(1) => Err(MetadataError::MethodNotAllowedOnDeleteMarker),
                _ => Err(MetadataError::ObjectNotFound),
            }
        }
    }

    #[cfg(test)]
    fn delete_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        let updated = self.execute_cached_metadata(
            "UPDATE objects SET tags = NULL WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND status = 0",
            params![bucket, key, version_id.to_u64() as i64],
            "delete object tags",
        )?;
        if updated == 0 {
            let status: Option<u8> = self.query_row_cached_optional_metadata(
                "SELECT status FROM objects WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
                "delete object tags (check status)",
                |row| row.get(0),
            )?;
            return match status {
                Some(1) => Err(MetadataError::MethodNotAllowedOnDeleteMarker),
                _ => Err(MetadataError::ObjectNotFound),
            };
        }
        Ok(())
    }

    // ── Multipart upload methods ──────────────────────────────────

    #[cfg(test)]
    fn create_multipart_upload(&self, req: &CreateMultipartUploadReq) -> Result<(), MetadataError> {
        let object_generation_id = self.next_generation_id(&req.bucket, &req.key)?;
        let command = CreateMultipartUploadCommand::from_request(
            req.clone(),
            object_generation_id,
            PgStore::now_millis(),
        );
        self.create_multipart_upload_explicit(&command.upload)
    }

    fn get_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT upload_id, bucket, key, initiated_at, state, tags, metadata_blob, \
                 system_metadata_blob, owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id, \
                 checksum_algorithm, checksum_type, encryption_type, encryption_state, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold, object_generation_id \
                 FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id.as_str()],
                |row| {
                    let state_raw = row.get::<_, u8>(4)?;
                    let algo_raw: Option<u8> = row.get(12)?;
                    let ctype_raw: Option<u8> = row.get(13)?;
                    let object_lock = Self::parse_object_lock_state(
                        row.get::<_, Option<u8>>(18)?,
                        row.get::<_, Option<i64>>(19)?,
                        row.get::<_, u8>(20)?,
                        18,
                        19,
                        20,
                    )?;
                    let checksum = if let Some(algo_val) = algo_raw {
                        let algo = ChecksumAlgorithm::from_u8(algo_val).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                12,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid checksum algorithm: {algo_val}")),
                            )
                        })?;
                        let ctype = ctype_raw
                            .map(|v| {
                                ChecksumType::from_u8(v).ok_or_else(|| {
                                    rusqlite::Error::FromSqlConversionFailure(
                                        13,
                                        rusqlite::types::Type::Integer,
                                        Box::from(format!("invalid checksum type: {v}")),
                                    )
                                })
                            })
                            .transpose()?;
                        Some(MultipartChecksumConfig::new(algo, ctype).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                12,
                                rusqlite::types::Type::Integer,
                                Box::from(e.reason),
                            )
                        })?)
                    } else {
                        None
                    };
                    let owner = Self::parse_owner_identity(
                        row,
                        8,
                        9,
                        "owner_principal",
                        "owner_canonical_id",
                    )?;
                    let initiator = Self::parse_optional_owner_identity(
                        row,
                        10,
                        11,
                        "initiator_principal",
                        "initiator_canonical_id",
                    )?;
                    Ok(MultipartUploadRecord {
                        upload_id: row.get(0)?,
                        bucket: row.get(1)?,
                        key: row.get(2)?,
                        initiated_at: row.get::<_, i64>(3)? as u64,
                        state: UploadState::from_u8(state_raw).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                4,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid upload state: {state_raw}")),
                            )
                        })?,
                        tags: row.get::<_, Option<String>>(5)?.map(SerializedTagSet::from),
                        metadata_blob: SerializedMetadataBlob::from(row.get::<_, Vec<u8>>(6)?),
                        system_metadata_blob: SerializedSystemMetadataBlob::from(
                            row.get::<_, Vec<u8>>(7)?,
                        ),
                        initiator,
                        owner,
                        acl_grants: Self::parse_acl_grants(
                            row.get::<_, String>(16)?,
                            16,
                            "multipart acl_grants",
                        )?,
                        public_read: row.get::<_, i64>(17)? != 0,
                        object_generation_id: Self::parse_generation_id(
                            row.get::<_, i64>(21)?,
                            21,
                            "object_generation_id",
                        )?,
                        object_lock,
                        checksum,
                        encryption: Self::parse_object_encryption(
                            row.get::<_, u8>(14)?,
                            row.get::<_, Option<Vec<u8>>>(15)?,
                            14,
                            15,
                        )?,
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get multipart upload",
                source: e,
            })?
            .ok_or_else(|| MetadataError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn set_upload_state(
        &self,
        upload_id: &UploadId,
        new_state: UploadState,
    ) -> Result<(), MetadataError> {
        // Only Completing and Aborting are valid transition targets.
        // Check existence first so we return NoSuchUpload accurately.
        if new_state == UploadState::InProgress {
            let current = self
                .conn
                .query_row(
                    "SELECT state FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id.as_str()],
                    |row| row.get::<_, u8>(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "set upload state (exists check)",
                    source: e,
                })?;
            return match current {
                Some(state) => Err(MetadataError::UploadNotInProgress { state }),
                None => Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }),
            };
        }
        let updated = self
            .conn
            .execute(
                "UPDATE multipart_uploads SET state = ?1 \
                 WHERE upload_id = ?2 AND state = 0",
                params![new_state as u8, upload_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "set upload state",
                source: e,
            })?;
        if updated == 0 {
            // Either the upload doesn't exist or it's not InProgress.
            let current = self
                .conn
                .query_row(
                    "SELECT state FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id.as_str()],
                    |row| row.get::<_, u8>(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "set upload state (check)",
                    source: e,
                })?;
            return match current {
                None => Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                }),
                Some(s) => Err(MetadataError::UploadNotInProgress { state: s }),
            };
        }
        Ok(())
    }

    #[cfg(test)]
    fn delete_multipart_upload(&self, upload_id: &UploadId) -> Result<(), MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "delete multipart upload (begin txn)",
                source: e,
            })?;

        let result = (|| -> Result<(), MetadataError> {
            self.conn
                .execute(
                    "DELETE FROM object_generation_reservations \
                     WHERE reservation_id = ?1",
                    params![upload_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "delete multipart upload generation reservation",
                    source: e,
                })?;
            let deleted = self
                .conn
                .execute(
                    "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "delete multipart upload",
                    source: e,
                })?;
            if deleted == 0 {
                return Err(MetadataError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "delete multipart upload (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn get_completed_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<Option<CompletedMultipartUploadRecord>, MetadataError> {
        self.conn
            .query_row(
                "SELECT upload_id, bucket, key, completion_order, completed_at, \
                 owner_principal, owner_canonical_id, \
                 initiator_principal, initiator_canonical_id \
                 FROM completed_multipart_uploads WHERE upload_id = ?1",
                params![upload_id.as_str()],
                Self::completed_multipart_upload_record_from_row,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get completed multipart upload",
                source: e,
            })
    }

    #[cfg(test)]
    fn delete_completed_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM completed_multipart_uploads WHERE bucket = ?1",
                params![bucket],
            )
            .map(|_| ())
            .map_err(|e| MetadataError::Db {
                context: "delete completed multipart uploads for bucket",
                source: e,
            })
    }

    fn list_multipart_uploads(
        &self,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::list_multipart_uploads",
            "pg_id={} bucket={:?} max_uploads={}",
            self.pg_id,
            req.bucket.as_str(),
            req.max_uploads
        );
        let limit = req.max_uploads as i64 + 1;
        let mut where_clauses = vec!["bucket = ?1".to_string()];
        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(req.bucket.clone())];
        let mut param_idx = 2;

        if let Some(ref prefix) = req.prefix {
            where_clauses.push(format!("key >= ?{param_idx}"));
            params_vec.push(Box::new(prefix.clone()));
            param_idx += 1;

            if let Some(end) = object_key_prefix_upper_bound(prefix) {
                where_clauses.push(format!("key < ?{param_idx}"));
                params_vec.push(Box::new(end));
                param_idx += 1;
            }
        }

        if let Some(ref key_marker) = req.key_marker {
            if let Some(uid_marker) = req.upload_id_marker.as_ref().map(UploadId::as_str) {
                // Resume after (key_marker, initiated_at of marker, uid_marker).
                // Use a subquery to resolve the marker's initiated_at so the
                // cursor is consistent with the (key, initiated_at, upload_id)
                // sort order. COALESCE to 0 so a deleted marker row safely
                // returns all remaining uploads for that key (duplicates are
                // preferable to silently dropped entries).
                where_clauses.push(format!(
                    "(key > ?{km} OR (key = ?{km} AND (\
                     initiated_at > COALESCE((SELECT initiated_at FROM multipart_uploads \
                     WHERE upload_id = ?{um} AND bucket = ?{bkt} AND key = ?{km}), 0) \
                     OR (initiated_at = COALESCE((SELECT initiated_at FROM multipart_uploads \
                     WHERE upload_id = ?{um} AND bucket = ?{bkt} AND key = ?{km}), 0) \
                     AND upload_id > ?{um}))))",
                    km = param_idx,
                    um = param_idx + 1,
                    bkt = param_idx + 2
                ));
                params_vec.push(Box::new(key_marker.clone()));
                params_vec.push(Box::new(uid_marker.to_string()));
                params_vec.push(Box::new(req.bucket.clone()));
                param_idx += 3;
            } else {
                where_clauses.push(format!("key > ?{param_idx}"));
                params_vec.push(Box::new(key_marker.clone()));
                param_idx += 1;
            }
        }

        let where_str = where_clauses.join(" AND ");
        let sql = format!(
            "SELECT upload_id, bucket, key, initiated_at, state, tags, metadata_blob, \
             system_metadata_blob, owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id, \
             checksum_algorithm, checksum_type, encryption_type, encryption_state, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold, object_generation_id \
             FROM multipart_uploads \
             WHERE {where_str} \
             ORDER BY key ASC, initiated_at ASC, upload_id ASC \
             LIMIT ?{param_idx}"
        );
        params_vec.push(Box::new(limit));

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|e| MetadataError::Db {
                context: "prepare list multipart uploads",
                source: e,
            })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), |row| {
                let state_raw = row.get::<_, u8>(4)?;
                let algo_raw: Option<u8> = row.get(12)?;
                let ctype_raw: Option<u8> = row.get(13)?;
                let object_lock = Self::parse_object_lock_state(
                    row.get::<_, Option<u8>>(18)?,
                    row.get::<_, Option<i64>>(19)?,
                    row.get::<_, u8>(20)?,
                    18,
                    19,
                    20,
                )?;
                let checksum = if let Some(algo_val) = algo_raw {
                    let algo = ChecksumAlgorithm::from_u8(algo_val).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            12,
                            rusqlite::types::Type::Integer,
                            Box::from(format!("invalid checksum algorithm: {algo_val}")),
                        )
                    })?;
                    let ctype = ctype_raw
                        .map(|v| {
                            ChecksumType::from_u8(v).ok_or_else(|| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    13,
                                    rusqlite::types::Type::Integer,
                                    Box::from(format!("invalid checksum type: {v}")),
                                )
                            })
                        })
                        .transpose()?;
                    Some(MultipartChecksumConfig::new(algo, ctype).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            12,
                            rusqlite::types::Type::Integer,
                            Box::from(e.reason),
                        )
                    })?)
                } else {
                    None
                };
                let owner =
                    Self::parse_owner_identity(row, 8, 9, "owner_principal", "owner_canonical_id")?;
                let initiator = Self::parse_optional_owner_identity(
                    row,
                    10,
                    11,
                    "initiator_principal",
                    "initiator_canonical_id",
                )?;
                Ok(MultipartUploadRecord {
                    upload_id: row.get(0)?,
                    bucket: row.get(1)?,
                    key: row.get(2)?,
                    initiated_at: row.get::<_, i64>(3)? as u64,
                    state: UploadState::from_u8(state_raw).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Integer,
                            Box::from(format!("invalid upload state: {state_raw}")),
                        )
                    })?,
                    tags: row.get::<_, Option<String>>(5)?.map(SerializedTagSet::from),
                    metadata_blob: SerializedMetadataBlob::from(row.get::<_, Vec<u8>>(6)?),
                    system_metadata_blob: SerializedSystemMetadataBlob::from(
                        row.get::<_, Vec<u8>>(7)?,
                    ),
                    initiator,
                    owner,
                    acl_grants: Self::parse_acl_grants(
                        row.get::<_, String>(16)?,
                        16,
                        "multipart acl_grants",
                    )?,
                    public_read: row.get::<_, i64>(17)? != 0,
                    object_generation_id: Self::parse_generation_id(
                        row.get::<_, i64>(21)?,
                        21,
                        "object_generation_id",
                    )?,
                    object_lock,
                    checksum,
                    encryption: Self::parse_object_encryption(
                        row.get::<_, u8>(14)?,
                        row.get::<_, Option<Vec<u8>>>(15)?,
                        14,
                        15,
                    )?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "list multipart uploads query",
                source: e,
            })?;

        let mut uploads: Vec<MultipartUploadRecord> = Vec::new();
        for row in rows {
            uploads.push(row.map_err(|e| MetadataError::Db {
                context: "list multipart uploads row",
                source: e,
            })?);
        }

        let is_truncated = uploads.len() as i64 > req.max_uploads as i64;
        if is_truncated {
            uploads.truncate(req.max_uploads as usize);
        }

        let (next_key_marker, next_upload_id_marker) = if is_truncated {
            uploads.last().map_or((None, None), |u| {
                (Some(u.key.clone()), Some(u.upload_id.clone()))
            })
        } else {
            (None, None)
        };

        Ok(ListMultipartUploadsResp {
            uploads,
            is_truncated,
            next_key_marker,
            next_upload_id_marker,
        })
    }

    #[cfg(test)]
    fn upsert_multipart_part(
        &self,
        part: &MultipartPartRecord,
    ) -> Result<Option<u32>, MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "upsert part (begin txn)",
                source: e,
            })?;

        let result = (|| -> Result<Option<u32>, rusqlite::Error> {
            // Read previous generation before overwrite.
            let prev_gen: Option<u32> = self
                .conn
                .query_row(
                    "SELECT generation FROM multipart_parts \
                     WHERE upload_id = ?1 AND part_number = ?2",
                    params![part.upload_id, part.part_number],
                    |row| row.get::<_, i64>(0).map(|v| v as u32),
                )
                .optional()?;

            self.conn.execute(
                "INSERT OR REPLACE INTO multipart_parts \
                 (upload_id, part_number, generation, size, etag, etag_kind, \
                  part_okh, part_vid, ec_k, ec_m, last_modified, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    part.upload_id,
                    part.part_number,
                    part.generation,
                    part.size as i64,
                    part.etag,
                    part.etag_kind as u8,
                    part.part_okh.as_slice(),
                    part.part_vid.get() as i64,
                    part.ec_k,
                    part.ec_m,
                    part.last_modified as i64,
                    part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                ],
            )?;

            Ok(prev_gen)
        })();

        match result {
            Ok(prev_gen) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "upsert part (commit txn)",
                        source: e,
                    });
                }
                Ok(prev_gen)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                // FK violation means the upload_id doesn't exist.
                if let rusqlite::Error::SqliteFailure(ref err, _) = e {
                    if err.code == rusqlite::ffi::ErrorCode::ConstraintViolation {
                        return Err(MetadataError::NoSuchUpload {
                            upload_id: part.upload_id.to_string(),
                        });
                    }
                }
                Err(MetadataError::Db {
                    context: "upsert multipart part",
                    source: e,
                })
            }
        }
    }

    #[cfg(test)]
    fn upsert_multipart_part_segments(
        &self,
        part: &MultipartPartRecord,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(Option<u32>, Vec<MultipartPartSegmentRecord>), MetadataError> {
        if part.part_okh != [0u8; 16] {
            return Err(MetadataError::Db {
                context: "upsert multipart part segments (non-segment part)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("segmented multipart parts must use zero part_okh sentinel"),
                ),
            });
        }

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "upsert multipart part segments (begin txn)",
                source: e,
            })?;

        let result =
            (|| -> Result<(Option<u32>, Vec<MultipartPartSegmentRecord>), rusqlite::Error> {
                let prev_gen: Option<u32> = self
                    .conn
                    .query_row(
                        "SELECT generation FROM multipart_parts \
                     WHERE upload_id = ?1 AND part_number = ?2",
                        params![part.upload_id, part.part_number],
                        |row| row.get::<_, i64>(0).map(|v| v as u32),
                    )
                    .optional()?;

                let mut prev_stmt = self.conn.prepare_cached(
                    "SELECT bucket, key, upload_id, version_id, part_number, segment_index, size, \
                 segment_crc64, segment_okh, segment_vid, data_pg_id, ec_k, ec_m \
                 FROM multipart_part_segments \
                 WHERE upload_id = ?1 AND version_id = ?2 AND part_number = ?3 \
                 ORDER BY segment_index ASC",
                )?;
                let prev_rows = prev_stmt.query_map(
                    params![
                        part.upload_id,
                        PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64,
                        part.part_number
                    ],
                    |row| {
                        let okh_blob: Vec<u8> = row.get(8)?;
                        let okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                        Ok(MultipartPartSegmentRecord {
                            bucket: row.get(0)?,
                            key: row.get(1)?,
                            upload_id: row.get(2)?,
                            version_id: row.get::<_, i64>(3)? as u64,
                            part_number: row.get(4)?,
                            segment_index: row.get(5)?,
                            size: row.get::<_, i64>(6)? as u64,
                            segment_crc64: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                            segment_okh: okh,
                            segment_vid: Self::parse_generation_id(
                                row.get::<_, i64>(9)?,
                                9,
                                "segment_vid",
                            )?,
                            data_pg_id: row.get(10)?,
                            ec_k: row.get(11)?,
                            ec_m: row.get(12)?,
                        })
                    },
                )?;
                let prev_segments = prev_rows.collect::<Result<Vec<_>, _>>()?;

                self.conn.execute(
                    "INSERT OR REPLACE INTO multipart_parts \
                 (upload_id, part_number, generation, size, etag, etag_kind, \
                  part_okh, part_vid, ec_k, ec_m, last_modified, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    params![
                        part.upload_id,
                        part.part_number,
                        part.generation,
                        part.size as i64,
                        part.etag,
                        part.etag_kind as u8,
                        part.part_okh.as_slice(),
                        part.part_vid.get() as i64,
                        part.ec_k,
                        part.ec_m,
                        part.last_modified as i64,
                        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                    ],
                )?;

                self.conn.execute(
                    "DELETE FROM multipart_part_segments \
                 WHERE upload_id = ?1 AND version_id = ?2 AND part_number = ?3",
                    params![
                        part.upload_id,
                        PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64,
                        part.part_number
                    ],
                )?;

                let mut stmt = self.conn.prepare_cached(
                    "INSERT INTO multipart_part_segments \
                 (bucket, key, upload_id, version_id, part_number, segment_index, size, \
                  segment_crc64, segment_okh, segment_vid, data_pg_id, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                )?;
                for segment in segments {
                    if segment.upload_id != part.upload_id
                        || segment.part_number != part.part_number
                        || segment.version_id != PART_SEGMENT_STAGING_VERSION_ID.to_u64()
                    {
                        return Err(rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Null,
                            Box::from("segment row does not match multipart part identity"),
                        ));
                    }
                    stmt.execute(params![
                        segment.bucket,
                        segment.key,
                        segment.upload_id,
                        segment.version_id as i64,
                        segment.part_number,
                        segment.segment_index,
                        segment.size as i64,
                        segment.segment_crc64.map(|v| v as i64),
                        segment.segment_okh.as_slice(),
                        segment.segment_vid.get() as i64,
                        segment.data_pg_id,
                        segment.ec_k,
                        segment.ec_m,
                    ])?;
                }

                Ok((prev_gen, prev_segments))
            })();

        match result {
            Ok(prev_state) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "upsert multipart part segments (commit txn)",
                        source: e,
                    });
                }
                Ok(prev_state)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                if let rusqlite::Error::SqliteFailure(ref err, _) = e {
                    if err.code == rusqlite::ffi::ErrorCode::ConstraintViolation {
                        return Err(MetadataError::NoSuchUpload {
                            upload_id: part.upload_id.to_string(),
                        });
                    }
                }
                Err(MetadataError::Db {
                    context: "upsert multipart part segments",
                    source: e,
                })
            }
        }
    }

    fn get_multipart_part(
        &self,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<MultipartPartRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT upload_id, part_number, generation, size, etag, etag_kind, \
                 part_okh, part_vid, ec_k, ec_m, last_modified, checksum \
                 FROM multipart_parts WHERE upload_id = ?1 AND part_number = ?2",
                params![upload_id.as_str(), part_number],
                Self::row_to_multipart_part,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get multipart part",
                source: e,
            })?
            .ok_or(MetadataError::PartNotFound {
                upload_id: upload_id.to_string(),
                part_number,
            })
    }

    fn list_multipart_parts(&self, req: &ListPartsReq) -> Result<ListPartsResp, MetadataError> {
        // Verify the upload exists so we return NoSuchUpload, not an empty list.
        let exists = self
            .conn
            .query_row(
                "SELECT 1 FROM multipart_uploads WHERE upload_id = ?1",
                params![req.upload_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "list parts (upload exists check)",
                source: e,
            })?;
        if exists.is_none() {
            return Err(MetadataError::NoSuchUpload {
                upload_id: req.upload_id.to_string(),
            });
        }

        let limit = req.max_parts as i64 + 1;

        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> =
            vec![Box::new(req.upload_id.clone())];
        let sql = if let Some(marker) = req.part_number_marker {
            params_vec.push(Box::new(marker));
            params_vec.push(Box::new(limit));
            "SELECT upload_id, part_number, generation, size, etag, etag_kind, \
             part_okh, part_vid, ec_k, ec_m, last_modified, checksum \
             FROM multipart_parts \
             WHERE upload_id = ?1 AND part_number > ?2 \
             ORDER BY part_number ASC LIMIT ?3"
                .to_string()
        } else {
            params_vec.push(Box::new(limit));
            "SELECT upload_id, part_number, generation, size, etag, etag_kind, \
             part_okh, part_vid, ec_k, ec_m, last_modified, checksum \
             FROM multipart_parts \
             WHERE upload_id = ?1 \
             ORDER BY part_number ASC LIMIT ?2"
                .to_string()
        };

        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self
            .conn
            .prepare_cached(&sql)
            .map_err(|e| MetadataError::Db {
                context: "prepare list multipart parts",
                source: e,
            })?;

        let rows = stmt
            .query_map(params_refs.as_slice(), Self::row_to_multipart_part)
            .map_err(|e| MetadataError::Db {
                context: "list multipart parts query",
                source: e,
            })?;

        let mut parts: Vec<MultipartPartRecord> = Vec::new();
        for row in rows {
            parts.push(row.map_err(|e| MetadataError::Db {
                context: "list multipart parts row",
                source: e,
            })?);
        }

        let is_truncated = parts.len() as i64 > req.max_parts as i64;
        if is_truncated {
            parts.truncate(req.max_parts as usize);
        }

        let next_part_number_marker = if is_truncated {
            parts.last().map(|p| p.part_number)
        } else {
            None
        };

        Ok(ListPartsResp {
            parts,
            is_truncated,
            next_part_number_marker,
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn commit_object_parts(&self, parts: &[ObjectPartRecord]) -> Result<(), MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "commit object parts (begin txn)",
                source: e,
            })?;

        let result = (|| {
            let mut stmt = self.conn.prepare_cached(
                "INSERT INTO object_parts \
                 (bucket, key, version_id, part_number, object_offset_start, size, etag, etag_kind, \
                  part_okh, part_vid, ec_k, ec_m, data_pg_id, checksum) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            )?;

            let mut ordered_parts: Vec<&ObjectPartRecord> = parts.iter().collect();
            ordered_parts.sort_by_key(|part| part.part_number);
            let mut object_offset_start = 0u64;
            for part in ordered_parts {
                stmt.execute(params![
                    part.bucket,
                    part.key,
                    part.version_id.to_u64() as i64,
                    part.part_number,
                    object_offset_start as i64,
                    part.size as i64,
                    part.etag,
                    part.etag_kind as u8,
                    part.part_okh.as_slice(),
                    part.part_vid.get() as i64,
                    part.ec_k,
                    part.ec_m,
                    part.data_pg_id,
                    part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                ])?;
                object_offset_start += part.size;
            }
            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "commit object parts (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(MetadataError::Db {
                    context: "commit object parts",
                    source: e,
                })
            }
        }
    }

    fn get_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectPartRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, version_id, part_number, size, etag, etag_kind, \
                 part_okh, part_vid, ec_k, ec_m, data_pg_id, checksum \
                 FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                 ORDER BY part_number ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get object parts",
                source: e,
            })?;

        let rows = stmt
            .query_map(
                params![bucket, key, version_id.to_u64() as i64],
                Self::row_to_object_part,
            )
            .map_err(|e| MetadataError::Db {
                context: "get object parts query",
                source: e,
            })?;

        let mut parts = Vec::new();
        for row in rows {
            parts.push(row.map_err(|e| MetadataError::Db {
                context: "get object parts row",
                source: e,
            })?);
        }
        Ok(parts)
    }

    #[cfg(test)]
    fn get_object_parts_overlapping_range(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        start: u64,
        end_exclusive: u64,
    ) -> Result<Vec<ObjectPartRangeRecord>, MetadataError> {
        if start >= end_exclusive {
            return Ok(Vec::new());
        }

        let mut first_stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, version_id, part_number, size, etag, etag_kind, \
                 part_okh, part_vid, ec_k, ec_m, data_pg_id, checksum, object_offset_start \
                 FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                   AND object_offset_start <= ?4 \
                   AND object_offset_start + size > ?4 \
                 ORDER BY object_offset_start DESC, part_number ASC \
                 LIMIT 1",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get object parts overlapping range first part",
                source: e,
            })?;

        let first = first_stmt
            .query_row(
                params![bucket, key, version_id.to_u64() as i64, start as i64],
                Self::row_to_object_part_range,
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get object parts overlapping range first part",
                source: e,
            })?;

        let Some(first) = first else {
            return Ok(Vec::new());
        };

        let first_part_number = first.part.part_number;
        let first_part_end = first.object_offset_start + first.part.size;
        if first_part_end >= end_exclusive {
            return Ok(vec![first]);
        }

        let mut parts = vec![first];
        let mut tail_stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, version_id, part_number, size, etag, etag_kind, \
                 part_okh, part_vid, ec_k, ec_m, data_pg_id, checksum, object_offset_start \
                 FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                   AND part_number > ?4 \
                   AND object_offset_start < ?5 \
                 ORDER BY part_number ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get object parts overlapping range tail parts",
                source: e,
            })?;

        let rows = tail_stmt
            .query_map(
                params![
                    bucket,
                    key,
                    version_id.to_u64() as i64,
                    first_part_number,
                    end_exclusive as i64
                ],
                Self::row_to_object_part_range,
            )
            .map_err(|e| MetadataError::Db {
                context: "get object parts overlapping range tail parts",
                source: e,
            })?;

        for row in rows {
            parts.push(row.map_err(|e| MetadataError::Db {
                context: "get object parts overlapping range tail row",
                source: e,
            })?);
        }

        Ok(parts)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn delete_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.delete_object_parts_direct(bucket, key, version_id)
    }

    #[cfg(test)]
    fn complete_multipart_commit(
        &self,
        upload_id: &UploadId,
        completion_order: u64,
        obj: &CommitMultipartReq,
        parts: &[ObjectPartRecord],
    ) -> Result<CompleteMultipartCommitCleanup, MetadataError> {
        if parts.is_empty() {
            return Err(MetadataError::Db {
                context: "complete multipart commit (empty parts)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("multipart commit requires at least one part"),
                ),
            });
        }
        let now = PgStore::now_millis();
        let data_layout = DataLayout::MultipartManifest as u8;
        let parts_count = Some(parts.len() as i64);
        let tags = obj.tags.as_ref().map(SerializedTagSet::as_str);
        let metadata_blob = obj
            .metadata_blob
            .as_ref()
            .map(SerializedMetadataBlob::as_slice);
        let system_metadata_blob = obj
            .system_metadata_blob
            .as_ref()
            .map(SerializedSystemMetadataBlob::as_slice);
        let encryption_type = obj.encryption.encryption_type() as u8;
        let encryption_state = obj.encryption.encode_state();

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "complete multipart commit (begin txn)",
                source: e,
            })?;

        let result = (|| -> Result<CompleteMultipartCommitCleanup, rusqlite::Error> {
            // Validate part identity matches object.
            let mut selected_part_numbers = std::collections::BTreeSet::new();
            for part in parts {
                if part.bucket != obj.bucket
                    || part.key != obj.key
                    || part.version_id != obj.version_id
                {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::from("part does not match object"),
                    ));
                }
                if !selected_part_numbers.insert(part.part_number) {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        0,
                        rusqlite::types::Type::Null,
                        Box::from("duplicate multipart completion part"),
                    ));
                }
            }

            // 1. Transition upload to Completing.
            let updated = self.conn.execute(
                "UPDATE multipart_uploads SET state = ?1 \
                 WHERE upload_id = ?2 AND state = 0",
                params![UploadState::Completing as u8, upload_id.as_str()],
            )?;
            if updated == 0 {
                // Check if it's already Completing (idempotent retry).
                let current: Option<u8> = self
                    .conn
                    .query_row(
                        "SELECT state FROM multipart_uploads WHERE upload_id = ?1",
                        params![upload_id.as_str()],
                        |row| row.get(0),
                    )
                    .optional()?;
                match current {
                    Some(1) => { /* Already Completing — allow idempotent retry */ }
                    _ => {
                        return Err(rusqlite::Error::QueryReturnedNoRows);
                    }
                }
            }
            let upload_generation_id: i64 = self.conn.query_row(
                "SELECT object_generation_id FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id],
                |row| row.get(0),
            )?;
            if upload_generation_id != obj.generation_id.get() as i64 {
                return Err(rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from(format!(
                        "multipart upload generation {} does not match commit generation {}",
                        upload_generation_id,
                        obj.generation_id.get()
                    )),
                ));
            }

            let omitted_parts = {
                let mut stmt = self.conn.prepare_cached(
                    "SELECT upload_id, part_number, generation, size, etag, etag_kind, \
                     part_okh, part_vid, ec_k, ec_m, last_modified, checksum \
                     FROM multipart_parts WHERE upload_id = ?1 ORDER BY part_number ASC",
                )?;
                let rows =
                    stmt.query_map(params![upload_id.as_str()], Self::row_to_multipart_part)?;
                let mut omitted = Vec::new();
                for row in rows {
                    let part = row?;
                    if !selected_part_numbers.contains(&part.part_number) {
                        omitted.push(part);
                    }
                }
                omitted
            };

            let (omitted_streaming_segments, omitted_streaming_part_numbers) = {
                let mut stmt = self.conn.prepare_cached(
                    "SELECT bucket, key, upload_id, version_id, part_number, segment_index, \
                     size, segment_crc64, segment_okh, segment_vid, data_pg_id, ec_k, ec_m \
                     FROM multipart_part_segments \
                     WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 AND version_id = ?4 \
                     ORDER BY part_number, segment_index",
                )?;
                let rows = stmt.query_map(
                    params![
                        obj.bucket,
                        obj.key,
                        upload_id.as_str(),
                        PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64
                    ],
                    |row| {
                        let okh_blob: Vec<u8> = row.get(8)?;
                        let segment_okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                        Ok(MultipartPartSegmentRecord {
                            bucket: row.get(0)?,
                            key: row.get(1)?,
                            upload_id: row.get(2)?,
                            version_id: row.get::<_, i64>(3)? as u64,
                            part_number: row.get(4)?,
                            segment_index: row.get(5)?,
                            size: row.get::<_, i64>(6)? as u64,
                            segment_crc64: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                            segment_okh,
                            segment_vid: Self::parse_generation_id(
                                row.get::<_, i64>(9)?,
                                9,
                                "segment_vid",
                            )?,
                            data_pg_id: row.get(10)?,
                            ec_k: row.get(11)?,
                            ec_m: row.get(12)?,
                        })
                    },
                )?;
                let mut omitted = Vec::new();
                let mut omitted_part_numbers = std::collections::BTreeSet::new();
                for row in rows {
                    let segment = row?;
                    if !selected_part_numbers.contains(&segment.part_number) {
                        omitted_part_numbers.insert(segment.part_number);
                        omitted.push(segment);
                    }
                }
                (omitted, omitted_part_numbers)
            };
            let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
                Self::object_lock_sql_values(obj.object_lock)?;
            let write_sequence = self
                .next_object_write_sequence(obj.bucket.as_str(), obj.key.as_str())
                .map_err(|error| match error {
                    MetadataError::Db { source, .. } => source,
                    other => rusqlite::Error::ToSqlConversionFailure(Box::new(
                        std::io::Error::other(other.to_string()),
                    )),
                })?;
            self.mark_current_live_noncurrent(
                obj.bucket.as_str(),
                obj.key.as_str(),
                obj.version_id,
                now,
            )?;
            self.advance_object_version_counter_in_open_txn(&obj.bucket, &obj.key, obj.version_id)
                .map_err(|error| match error {
                    MetadataError::Db { source, .. } => source,
                    other => rusqlite::Error::ToSqlConversionFailure(Box::new(
                        std::io::Error::other(other.to_string()),
                    )),
                })?;

            // 2. Write/overwrite object metadata row.
            let obj_sql = if obj.version_id.is_null() {
                "INSERT OR REPLACE INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, tags, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            } else {
                "INSERT INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, tags, data_layout, parts_count, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            };
            self.conn.execute(
                obj_sql,
                params![
                    obj.bucket,
                    obj.key,
                    obj.version_id.to_u64() as i64,
                    write_sequence as i64,
                    obj.generation_id.get() as i64,
                    obj.size as i64,
                    obj.etag_crc64.as_slice(),
                    EtagKind::MultipartComposite as u8,
                    now as i64,
                    obj.ec.k,
                    obj.ec.m,
                    ObjectState::Live as u8,
                    tags,
                    data_layout,
                    parts_count,
                    metadata_blob,
                    system_metadata_blob,
                    encryption_type,
                    encryption_state,
                    obj.owner.principal,
                    obj.owner.canonical_id.as_str(),
                    obj.acl_grants.serialized(),
                    i32::from(obj.public_read),
                    object_lock_retention_mode,
                    object_lock_retain_until,
                    object_lock_legal_hold,
                ],
            )?;

            // 3. Delete prior object_parts (null-version overwrite).
            self.conn.execute(
                "DELETE FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![obj.bucket, obj.key, obj.version_id.to_u64() as i64],
            )?;

            // 4. Insert new manifest rows.
            {
                let mut stmt = self.conn.prepare_cached(
                    "INSERT INTO object_parts \
                     (bucket, key, version_id, part_number, object_offset_start, size, etag, etag_kind, \
                      part_okh, part_vid, ec_k, ec_m, data_pg_id, checksum) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                )?;
                let mut ordered_parts: Vec<&ObjectPartRecord> = parts.iter().collect();
                ordered_parts.sort_by_key(|part| part.part_number);
                let mut object_offset_start = 0u64;
                for part in ordered_parts {
                    stmt.execute(params![
                        part.bucket,
                        part.key,
                        part.version_id.to_u64() as i64,
                        part.part_number,
                        object_offset_start as i64,
                        part.size as i64,
                        part.etag,
                        part.etag_kind as u8,
                        part.part_okh.as_slice(),
                        part.part_vid.get() as i64,
                        part.ec_k,
                        part.ec_m,
                        part.data_pg_id,
                        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                    ])?;
                    object_offset_start += part.size;
                }
            }

            // 5. Clean up stale multipart_part_segments from prior uploads to
            //    the same key+version_id (e.g. overwriting in unversioned mode).
            self.conn.execute(
                "DELETE FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND upload_id != ?4",
                params![
                    obj.bucket,
                    obj.key,
                    obj.version_id.to_u64() as i64,
                    upload_id
                ],
            )?;

            // 6. Delete omitted streamed part segment rows. Their shard files
            //    are returned to the caller for post-commit cleanup.
            for part_number in &omitted_streaming_part_numbers {
                self.conn.execute(
                    "DELETE FROM multipart_part_segments \
                     WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 \
                     AND version_id = ?4 AND part_number = ?5",
                    params![
                        obj.bucket,
                        obj.key,
                        upload_id.as_str(),
                        PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64,
                        part_number,
                    ],
                )?;
            }

            // 7. Reparent only selected streamed segments from staging
            //    version_id to the real object version_id so reads can find
            //    them. Omitted parts must not survive as unreachable rows.
            for part_number in &selected_part_numbers {
                self.conn.execute(
                    "UPDATE multipart_part_segments \
                     SET version_id = ?1 \
                     WHERE bucket = ?2 AND key = ?3 AND upload_id = ?4 \
                     AND version_id = ?5 AND part_number = ?6",
                    params![
                        obj.version_id.to_u64() as i64,
                        obj.bucket,
                        obj.key,
                        upload_id.as_str(),
                        PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64,
                        part_number,
                    ],
                )?;
            }

            // 8. Record this upload as completed so AbortMultipartUpload can
            //    remain idempotently successful for the exact completed upload_id.
            let (initiator_principal, initiator_canonical_id): (Option<String>, Option<String>) =
                self.conn.query_row(
                    "SELECT initiator_principal, initiator_canonical_id \
                     FROM multipart_uploads WHERE upload_id = ?1",
                    params![upload_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
            self.conn.execute(
                "INSERT OR REPLACE INTO completed_multipart_uploads \
                 (upload_id, bucket, key, completion_order, completed_at, owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    upload_id,
                    obj.bucket,
                    obj.key,
                    completion_order as i64,
                    now as i64,
                    obj.owner.principal,
                    obj.owner.canonical_id.as_str(),
                    initiator_principal,
                    initiator_canonical_id,
                ],
            )?;

            // 9. Release the durable generation reservation now that the
            //    generation is visible on the committed object row.
            let released = self.conn.execute(
                "DELETE FROM object_generation_reservations \
                 WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3 AND generation_id = ?4",
                params![
                    upload_id,
                    obj.bucket,
                    obj.key,
                    obj.generation_id.get() as i64,
                ],
            )?;
            if released == 0 {
                return Err(rusqlite::Error::QueryReturnedNoRows);
            }

            // 10. Delete in-progress upload + parts (CASCADE).
            self.conn.execute(
                "DELETE FROM multipart_uploads WHERE upload_id = ?1",
                params![upload_id],
            )?;

            Ok(CompleteMultipartCommitCleanup {
                omitted_parts,
                omitted_streaming_segments,
                stream_uploads: Vec::new(),
                stream_upload_segments: Vec::new(),
            })
        })();

        match result {
            Ok(cleanup) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "complete multipart commit (commit txn)",
                        source: e,
                    });
                }
                Ok(cleanup)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(MetadataError::Db {
                    context: "complete multipart commit",
                    source: e,
                })
            }
        }
    }
    // ── Streaming upload session methods ──────────────────────────────

    #[cfg(any(test, feature = "test-hooks"))]
    fn create_stream_upload(&self, req: &CreateStreamUploadReq) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::create_stream_upload",
            "pg_id={} session_id={:?} bucket={:?} key={:?}",
            self.pg_id,
            req.session_id.as_str(),
            req.bucket.as_str(),
            req.key.as_str()
        );
        let command = CreateStreamUploadCommand::from_request(req.clone(), PgStore::now_millis());
        self.create_stream_upload_explicit(&command.session)
    }

    fn get_stream_upload(
        &self,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, MetadataError> {
        self.conn
            .query_row(
                "SELECT session_id, bucket, key, op_kind, upload_id, part_number, state, \
                 created_at, encryption_type, encryption_state FROM stream_uploads WHERE session_id = ?1",
                params![session_id.as_str()],
                |row| {
                    let op_kind_raw: u8 = row.get(3)?;
                    let state_raw: u8 = row.get(6)?;
                    let upload_id: Option<UploadId> = row.get(4)?;
                    let part_number: Option<i64> = row.get(5)?;
                    Ok(StreamUploadRecord {
                        session_id: row.get(0)?,
                        bucket: row.get(1)?,
                        key: row.get(2)?,
                        target: PgStore::parse_stream_target(
                            op_kind_raw,
                            upload_id,
                            part_number,
                            3,
                        )?,
                        state: StreamUploadState::from_u8(state_raw).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                6,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid stream state: {state_raw}")),
                            )
                        })?,
                        created_at: row.get::<_, i64>(7)? as u64,
                        encryption: Self::parse_object_encryption(
                            row.get::<_, u8>(8)?,
                            row.get::<_, Option<Vec<u8>>>(9)?,
                            8,
                            9,
                        )?,
                    })
                },
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get stream upload",
                source: e,
            })?
            .ok_or_else(|| MetadataError::StreamSessionNotFound {
                session_id: session_id.as_str().to_owned(),
            })
    }

    #[cfg(test)]
    fn set_stream_upload_state(
        &self,
        session_id: &SessionId,
        new_state: StreamUploadState,
    ) -> Result<(), MetadataError> {
        self.set_stream_upload_state_direct(session_id, new_state)
    }

    #[cfg(test)]
    fn delete_stream_upload(&self, session_id: &SessionId) -> Result<(), MetadataError> {
        self.delete_stream_upload_direct(session_id)
    }

    fn list_all_stream_uploads(&self) -> Result<Vec<StreamUploadRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT session_id, bucket, key, op_kind, upload_id, part_number, state, \
                 created_at, encryption_type, encryption_state FROM stream_uploads",
            )
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads (prepare)",
                source: e,
            })?;
        let rows = stmt
            .query_map([], |row| {
                let op_kind_raw: u8 = row.get(3)?;
                let state_raw: u8 = row.get(6)?;
                let upload_id: Option<UploadId> = row.get(4)?;
                let part_number: Option<i64> = row.get(5)?;
                Ok(StreamUploadRecord {
                    session_id: row.get(0)?,
                    bucket: row.get(1)?,
                    key: row.get(2)?,
                    target: PgStore::parse_stream_target(op_kind_raw, upload_id, part_number, 3)?,
                    state: StreamUploadState::from_u8(state_raw).ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            6,
                            rusqlite::types::Type::Integer,
                            Box::from(format!("invalid stream state: {state_raw}")),
                        )
                    })?,
                    created_at: row.get::<_, i64>(7)? as u64,
                    encryption: Self::parse_object_encryption(
                        row.get::<_, u8>(8)?,
                        row.get::<_, Option<Vec<u8>>>(9)?,
                        8,
                        9,
                    )?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads (query)",
                source: e,
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "list all stream uploads (collect)",
                source: e,
            })
    }

    #[cfg(test)]
    fn append_stream_segment(
        &self,
        segment: &StreamUploadSegmentRecord,
    ) -> Result<(), MetadataError> {
        self.append_stream_segment_direct(segment)
    }

    fn list_stream_segments(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT session_id, segment_index, size, segment_okh, segment_vid, data_pg_id, \
                 segment_crc64, ec_k, ec_m FROM stream_upload_segments \
                 WHERE session_id = ?1 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare list stream segments",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![session_id.as_str()], |row| {
                let okh_blob: Vec<u8> = row.get(3)?;
                let okh = PgStore::parse_okh_blob(&okh_blob, 3)?;
                Ok(StreamUploadSegmentRecord {
                    session_id: row.get(0)?,
                    segment_index: row.get(1)?,
                    size: row.get::<_, i64>(2)? as u64,
                    segment_crc64: row.get::<_, Option<i64>>(6)?.map(|v| v as u64),
                    segment_okh: okh,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(4)?,
                        4,
                        "segment_vid",
                    )?,
                    data_pg_id: row.get(5)?,
                    ec_k: row.get(7)?,
                    ec_m: row.get(8)?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "list stream segments",
                source: e,
            })?;

        let mut segments = Vec::new();
        for row in rows {
            segments.push(row.map_err(|e| MetadataError::Db {
                context: "list stream segments row",
                source: e,
            })?);
        }
        Ok(segments)
    }

    #[cfg(test)]
    fn commit_stream_put(
        &self,
        session_id: &SessionId,
        obj: &CommitStreamPutReq,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), MetadataError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::commit_stream_put",
            "pg_id={} session_id={:?} bucket={:?} key={:?} segments={}",
            self.pg_id,
            session_id,
            obj.bucket.as_str(),
            obj.key.as_str(),
            segments.len()
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "commit stream put (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| {
            // 1. Verify session exists, is InProgress, is PutObject kind, and matches
            //    the target bucket/key. Then transition to Completing.
            let row: Option<StreamSessionRow> = self
                .conn
                .query_row(
                    "SELECT state, op_kind, bucket, key, upload_id, part_number \
                     FROM stream_uploads \
                     WHERE session_id = ?1",
                    params![session_id.as_str()],
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
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "commit stream put (lookup session)",
                    source: e,
                })?;

            let (current, op_kind_raw, sess_bucket, sess_key, upload_id, part_number) = row
                .ok_or_else(|| MetadataError::StreamSessionNotFound {
                    session_id: session_id.as_str().to_owned(),
                })?;
            let target = PgStore::parse_stream_target(op_kind_raw, upload_id, part_number, 1)
                .map_err(|e| MetadataError::Db {
                    context: "commit stream put (parse session target)",
                    source: e,
                })?;

            if current != StreamUploadState::InProgress as u8 {
                return Err(MetadataError::StreamSessionNotInProgress { state: current });
            }
            if target != StreamUploadTarget::PutObject
                || sess_bucket != obj.bucket
                || sess_key != obj.key
            {
                return Err(MetadataError::StreamSessionNotFound {
                    session_id: session_id.as_str().to_owned(),
                });
            }

            self.conn
                .execute(
                    "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                    params![StreamUploadState::Completing as u8, session_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream put (set completing)",
                    source: e,
                })?;

            // 2. Write/overwrite object metadata row.
            let now = PgStore::now_millis();
            let data_layout = DataLayout::StandardInternal as u8;
            let parts_count: Option<i64> = None;
            let tags = obj.tags.as_ref().map(SerializedTagSet::as_str);
            let encryption_type = obj.encryption.encryption_type() as u8;
            let encryption_state = obj.encryption.encode_state();
            let system_metadata_blob = obj
                .system_metadata_blob
                .as_ref()
                .map(SerializedSystemMetadataBlob::as_slice);
            let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
                Self::object_lock_sql_values(obj.object_lock).map_err(|e| MetadataError::Db {
                    context: "commit stream put (encode object lock)",
                    source: e,
                })?;
            let write_sequence =
                self.next_object_write_sequence(obj.bucket.as_str(), obj.key.as_str())?;
            self.mark_current_live_noncurrent(
                obj.bucket.as_str(),
                obj.key.as_str(),
                obj.version_id,
                now,
            )
            .map_err(|e| MetadataError::Db {
                context: "commit stream put (mark noncurrent)",
                source: e,
            })?;
            self.advance_object_version_counter_in_open_txn(&obj.bucket, &obj.key, obj.version_id)?;

            let obj_sql = if obj.version_id.is_null() {
                "INSERT OR REPLACE INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            } else {
                "INSERT INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            };
            self.conn
                .execute(
                    obj_sql,
                    params![
                        obj.bucket,
                        obj.key,
                        obj.version_id.to_u64() as i64,
                        write_sequence as i64,
                        obj.generation_id.get() as i64,
                        obj.size as i64,
                        obj.etag_crc64.to_be_bytes().as_slice(),
                        EtagKind::Crc64 as u8,
                        now as i64,
                        obj.ec.k,
                        obj.ec.m,
                        ObjectState::Live as u8,
                        data_layout,
                        parts_count,
                        tags,
                        obj.metadata_blob
                            .as_ref()
                            .map(SerializedMetadataBlob::as_slice),
                        system_metadata_blob,
                        encryption_type,
                        encryption_state,
                        obj.owner.principal,
                        obj.owner.canonical_id.as_str(),
                        obj.acl_grants.serialized(),
                        i32::from(obj.public_read),
                        object_lock_retention_mode,
                        object_lock_retain_until,
                        object_lock_legal_hold,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream put (write object)",
                    source: e,
                })?;

            // 3. Delete any prior object_segments for this version.
            self.conn
                .execute(
                    "DELETE FROM object_segments \
                     WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                    params![obj.bucket, obj.key, obj.version_id.to_u64() as i64],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream put (delete prior segments)",
                    source: e,
                })?;

            // 4. Insert committed object segment rows.
            {
                let mut stmt = self
                    .conn
                    .prepare_cached(
                        "INSERT INTO object_segments \
                         (bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, segment_vid, \
                          data_pg_id, ec_k, ec_m) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream put (prepare insert segments)",
                        source: e,
                    })?;
                for segment in segments {
                    if segment.bucket != obj.bucket
                        || segment.key != obj.key
                        || segment.version_id != obj.version_id
                    {
                        return Err(MetadataError::StreamSessionNotFound {
                            session_id: session_id.as_str().to_owned(),
                        });
                    }
                    stmt.execute(params![
                        segment.bucket,
                        segment.key,
                        segment.version_id.to_u64() as i64,
                        segment.segment_index,
                        segment.size as i64,
                        segment.segment_crc64.map(|v| v as i64),
                        segment.segment_okh.as_slice(),
                        segment.segment_vid.get() as i64,
                        segment.data_pg_id,
                        segment.ec_k,
                        segment.ec_m,
                    ])
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream put (insert segment)",
                        source: e,
                    })?;
                }
            }

            // 5. Delete staging rows.
            self.conn
                .execute(
                    "DELETE FROM stream_uploads WHERE session_id = ?1",
                    params![session_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream put (delete staging)",
                    source: e,
                })?;
            self.conn
                .execute(
                    "DELETE FROM object_generation_reservations \
                     WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3",
                    params![session_id.as_str(), obj.bucket, obj.key],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream put (delete generation reservation)",
                    source: e,
                })?;

            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "commit stream put (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn put_object_with_segments(
        &self,
        obj: &PutLiveObjectReq,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), MetadataError> {
        obj.validate().map_err(|msg| MetadataError::Db {
            context: "put segment object (etag/layout mismatch)",
            source: rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Null,
                Box::from(msg),
            ),
        })?;
        if obj.layout != ObjectLayout::Standard {
            return Err(MetadataError::Db {
                context: "put segment object (non-segment layout)",
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Null,
                    Box::from("put_object_with_segments requires Standard layout"),
                ),
            });
        }

        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "put segment object (begin txn)",
                source: e,
            })?;

        let result: Result<(), MetadataError> = (|| {
            let now = PgStore::now_millis();
            let data_layout = obj.layout.data_layout() as u8;
            let etag_kind = obj.etag.etag_kind() as u8;
            let status = ObjectState::Live as u8;
            let parts_count = obj.layout.parts_count().map(|n| n as i64);
            let tags = obj.tags.as_ref().map(SerializedTagSet::as_str);
            let metadata_blob = obj
                .metadata_blob
                .as_ref()
                .map(SerializedMetadataBlob::as_slice);
            let system_metadata_blob = obj
                .system_metadata_blob
                .as_ref()
                .map(SerializedSystemMetadataBlob::as_slice);
            let (object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) =
                Self::object_lock_sql_values(obj.object_lock).map_err(|e| MetadataError::Db {
                    context: "put segment object (encode object lock)",
                    source: e,
                })?;
            let encryption_type = obj.encryption.encryption_type() as u8;
            let encryption_state = obj.encryption.encode_state();
            let write_sequence =
                self.next_object_write_sequence(obj.bucket.as_str(), obj.key.as_str())?;
            self.mark_current_live_noncurrent(
                obj.bucket.as_str(),
                obj.key.as_str(),
                obj.version_id,
                now,
            )
            .map_err(|e| MetadataError::Db {
                context: "put segment object (mark noncurrent)",
                source: e,
            })?;
            self.advance_object_version_counter_in_open_txn(&obj.bucket, &obj.key, obj.version_id)?;

            let obj_sql = if obj.version_id.is_null() {
                "INSERT OR REPLACE INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            } else {
                "INSERT INTO objects \
                 (bucket, key, version_id, write_sequence, generation_id, size, etag, etag_kind, last_modified, \
                  storage_class, ec_k, ec_m, status, data_layout, parts_count, tags, metadata_blob, system_metadata_blob, encryption_type, encryption_state, owner_principal, owner_canonical_id, acl_grants, public_read, object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26)"
            };
            self.conn
                .execute(
                    obj_sql,
                    params![
                        obj.bucket,
                        obj.key,
                        obj.version_id.to_u64() as i64,
                        write_sequence as i64,
                        obj.generation_id.get() as i64,
                        obj.size as i64,
                        obj.etag.as_bytes().as_slice(),
                        etag_kind,
                        now as i64,
                        obj.ec.k,
                        obj.ec.m,
                        status,
                        data_layout,
                        parts_count,
                        tags,
                        metadata_blob,
                        system_metadata_blob,
                        encryption_type,
                        encryption_state,
                        obj.owner.principal,
                        obj.owner.canonical_id.as_str(),
                        obj.acl_grants.serialized(),
                        i32::from(obj.public_read),
                        object_lock_retention_mode,
                        object_lock_retain_until,
                        object_lock_legal_hold,
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put segment object (write object)",
                    source: e,
                })?;

            self.conn
                .execute(
                    "DELETE FROM object_segments \
                     WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                    params![obj.bucket, obj.key, obj.version_id.to_u64() as i64],
                )
                .map_err(|e| MetadataError::Db {
                    context: "put segment object (delete prior segments)",
                    source: e,
                })?;

            let mut stmt = self
                .conn
                .prepare_cached(
                    "INSERT INTO object_segments \
                     (bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, segment_vid, \
                      data_pg_id, ec_k, ec_m) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                )
                .map_err(|e| MetadataError::Db {
                    context: "put segment object (prepare insert segments)",
                    source: e,
                })?;
            for segment in segments {
                if segment.bucket != obj.bucket
                    || segment.key != obj.key
                    || segment.version_id != obj.version_id
                {
                    return Err(MetadataError::Db {
                        context: "put segment object (segment object mismatch)",
                        source: rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Null,
                            Box::from("segment row does not match object identity"),
                        ),
                    });
                }
                stmt.execute(params![
                    segment.bucket,
                    segment.key,
                    segment.version_id.to_u64() as i64,
                    segment.segment_index,
                    segment.size as i64,
                    segment.segment_crc64.map(|v| v as i64),
                    segment.segment_okh.as_slice(),
                    segment.segment_vid.get() as i64,
                    segment.data_pg_id,
                    segment.ec_k,
                    segment.ec_m,
                ])
                .map_err(|e| MetadataError::Db {
                    context: "put segment object (insert segment)",
                    source: e,
                })?;
            }

            Ok(())
        })();

        match result {
            Ok(()) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "put segment object (commit txn)",
                        source: e,
                    });
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    #[cfg(test)]
    fn commit_stream_part(
        &self,
        session_id: &SessionId,
        part: &MultipartPartRecord,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError> {
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| MetadataError::Db {
                context: "commit stream part (begin txn)",
                source: e,
            })?;

        let result: Result<Vec<MultipartPartSegmentRecord>, MetadataError> = (|| {
            // 1. Verify session exists, is InProgress, is UploadPart kind, and matches
            //    the target bucket/key/upload_id/part_number. Then transition to Completing.
            let sess_row = self.get_stream_upload(session_id)?;

            if sess_row.state != StreamUploadState::InProgress {
                return Err(MetadataError::StreamSessionNotInProgress {
                    state: sess_row.state as u8,
                });
            }
            // Validate session binding matches commit target.
            match &sess_row.target {
                StreamUploadTarget::UploadPart {
                    upload_id,
                    part_number,
                } if upload_id == &part.upload_id && *part_number == part.part_number => {}
                _ => {
                    return Err(MetadataError::StreamSessionNotFound {
                        session_id: session_id.as_str().to_owned(),
                    })
                }
            }
            let sess_bucket = sess_row.bucket;
            let sess_key = sess_row.key;

            self.conn
                .execute(
                    "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                    params![StreamUploadState::Completing as u8, session_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (set completing)",
                    source: e,
                })?;

            // 2. Upsert multipart part metadata.
            let prev_gen: Option<u32> = self
                .conn
                .query_row(
                    "SELECT generation FROM multipart_parts \
                     WHERE upload_id = ?1 AND part_number = ?2",
                    params![part.upload_id, part.part_number],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (read prev gen)",
                    source: e,
                })?;

            let new_gen = prev_gen.map_or(0, |g| g + 1);

            self.conn
                .execute(
                    "INSERT OR REPLACE INTO multipart_parts \
                     (upload_id, part_number, generation, size, etag, etag_kind, \
                      part_okh, part_vid, ec_k, ec_m, last_modified, checksum) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                    params![
                        part.upload_id,
                        part.part_number,
                        new_gen,
                        part.size as i64,
                        part.etag,
                        part.etag_kind as u8,
                        part.part_okh.as_slice(),
                        part.part_vid.get() as i64,
                        part.ec_k,
                        part.ec_m,
                        part.last_modified as i64,
                        part.checksum.as_ref().map(|checksum| checksum.as_slice()),
                    ],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (upsert part)",
                    source: e,
                })?;

            // 3. Capture prior part segments for this upload+part before deleting
            //    their metadata rows so the caller can reclaim their shards.
            let displaced_segments = {
                let mut stmt = self
                    .conn
                    .prepare_cached(
                        "SELECT bucket, key, upload_id, version_id, part_number, segment_index, size, segment_crc64, segment_okh, \
                         segment_vid, data_pg_id, ec_k, ec_m FROM multipart_part_segments \
                         WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 AND part_number = ?4 \
                         ORDER BY segment_index ASC",
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream part (prepare displaced segments)",
                        source: e,
                    })?;

                let rows = stmt
                    .query_map(
                        params![sess_bucket, sess_key, part.upload_id, part.part_number],
                        |row| {
                            let okh_blob: Vec<u8> = row.get(8)?;
                            let okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                            Ok(MultipartPartSegmentRecord {
                                bucket: row.get(0)?,
                                key: row.get(1)?,
                                upload_id: row.get(2)?,
                                version_id: row.get::<_, i64>(3)? as u64,
                                part_number: row.get(4)?,
                                segment_index: row.get(5)?,
                                size: row.get::<_, i64>(6)? as u64,
                                segment_crc64: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                                segment_okh: okh,
                                segment_vid: Self::parse_generation_id(
                                    row.get::<_, i64>(9)?,
                                    9,
                                    "segment_vid",
                                )?,
                                data_pg_id: row.get(10)?,
                                ec_k: row.get(11)?,
                                ec_m: row.get(12)?,
                            })
                        },
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream part (query displaced segments)",
                        source: e,
                    })?;

                let mut displaced = Vec::new();
                for row in rows {
                    displaced.push(row.map_err(|e| MetadataError::Db {
                        context: "commit stream part (read displaced segment row)",
                        source: e,
                    })?);
                }
                displaced
            };

            // 4. Delete prior part segments for this upload+part (re-upload support).
            //    Scoped by upload_id to avoid clobbering concurrent uploads for the same key.
            self.conn
                .execute(
                    "DELETE FROM multipart_part_segments \
                     WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 \
                     AND part_number = ?4",
                    params![sess_bucket, sess_key, part.upload_id, part.part_number],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (delete prior segments)",
                    source: e,
                })?;

            // 5. Insert committed multipart part segment rows.
            {
                let mut stmt = self
                    .conn
                    .prepare_cached(
                        "INSERT INTO multipart_part_segments \
                         (bucket, key, upload_id, version_id, part_number, segment_index, size, segment_crc64, segment_okh, \
                          segment_vid, data_pg_id, ec_k, ec_m) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    )
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream part (prepare insert segments)",
                        source: e,
                    })?;
                for segment in segments {
                    if segment.bucket != sess_bucket
                        || segment.key != sess_key
                        || segment.upload_id != part.upload_id
                        || segment.version_id != PART_SEGMENT_STAGING_VERSION_ID.to_u64()
                        || segment.part_number != part.part_number
                    {
                        return Err(MetadataError::StreamSessionNotFound {
                            session_id: session_id.as_str().to_owned(),
                        });
                    }
                    stmt.execute(params![
                        segment.bucket,
                        segment.key,
                        segment.upload_id,
                        segment.version_id as i64,
                        segment.part_number,
                        segment.segment_index,
                        segment.size as i64,
                        segment.segment_crc64.map(|v| v as i64),
                        segment.segment_okh.as_slice(),
                        segment.segment_vid.get() as i64,
                        segment.data_pg_id,
                        segment.ec_k,
                        segment.ec_m,
                    ])
                    .map_err(|e| MetadataError::Db {
                        context: "commit stream part (insert segment)",
                        source: e,
                    })?;
                }
            }

            // 5. Delete staging rows.
            self.conn
                .execute(
                    "DELETE FROM stream_uploads WHERE session_id = ?1",
                    params![session_id.as_str()],
                )
                .map_err(|e| MetadataError::Db {
                    context: "commit stream part (delete staging)",
                    source: e,
                })?;

            Ok(displaced_segments)
        })();

        match result {
            Ok(displaced_segments) => {
                if let Err(e) = self.conn.execute_batch("COMMIT") {
                    let _ = self.conn.execute_batch("ROLLBACK");
                    return Err(MetadataError::Db {
                        context: "commit stream part (commit txn)",
                        source: e,
                    });
                }
                Ok(displaced_segments)
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    fn get_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, version_id, segment_index, size, segment_crc64, segment_okh, segment_vid, \
                 data_pg_id, ec_k, ec_m FROM object_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 \
                 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get stream object segments",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![bucket, key, version_id.to_u64() as i64], |row| {
                let okh_blob: Vec<u8> = row.get(6)?;
                let okh = PgStore::parse_okh_blob(&okh_blob, 6)?;
                Ok(ObjectSegmentRecord {
                    bucket: row.get(0)?,
                    key: row.get(1)?,
                    version_id: PgStore::parse_version_id(row.get::<_, i64>(2)?, 2)?,
                    segment_index: row.get(3)?,
                    size: row.get::<_, i64>(4)? as u64,
                    segment_crc64: row.get::<_, Option<i64>>(5)?.map(|v| v as u64),
                    segment_okh: okh,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(7)?,
                        7,
                        "segment_vid",
                    )?,
                    data_pg_id: row.get(8)?,
                    ec_k: row.get(9)?,
                    ec_m: row.get(10)?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "get stream object segments",
                source: e,
            })?;

        let mut segments = Vec::new();
        for row in rows {
            segments.push(row.map_err(|e| MetadataError::Db {
                context: "get stream object segments row",
                source: e,
            })?);
        }
        Ok(segments)
    }

    #[cfg(test)]
    fn delete_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.delete_object_segments_direct(bucket, key, version_id)
    }

    fn get_multipart_part_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        part_number: u32,
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, upload_id, version_id, part_number, segment_index, size, segment_crc64, segment_okh, \
                 segment_vid, data_pg_id, ec_k, ec_m FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3 AND part_number = ?4 \
                 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get multipart part segments",
                source: e,
            })?;

        let rows = stmt
            .query_map(
                params![bucket, key, version_id.to_u64() as i64, part_number],
                |row| {
                    let okh_blob: Vec<u8> = row.get(8)?;
                    let okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                    Ok(MultipartPartSegmentRecord {
                        bucket: row.get(0)?,
                        key: row.get(1)?,
                        upload_id: row.get(2)?,
                        version_id: row.get::<_, i64>(3)? as u64,
                        part_number: row.get(4)?,
                        segment_index: row.get(5)?,
                        size: row.get::<_, i64>(6)? as u64,
                        segment_crc64: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                        segment_okh: okh,
                        segment_vid: Self::parse_generation_id(
                            row.get::<_, i64>(9)?,
                            9,
                            "segment_vid",
                        )?,
                        data_pg_id: row.get(10)?,
                        ec_k: row.get(11)?,
                        ec_m: row.get(12)?,
                    })
                },
            )
            .map_err(|e| MetadataError::Db {
                context: "get multipart part segments",
                source: e,
            })?;

        let mut segments = Vec::new();
        for row in rows {
            segments.push(row.map_err(|e| MetadataError::Db {
                context: "get multipart part segments row",
                source: e,
            })?);
        }
        Ok(segments)
    }

    fn get_multipart_part_segments_for_upload_part(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, upload_id, version_id, part_number, segment_index, \
                 size, segment_crc64, segment_okh, segment_vid, data_pg_id, ec_k, ec_m \
                 FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND upload_id = ?3 AND part_number = ?4 \
                 ORDER BY segment_index ASC",
            )
            .map_err(|e| MetadataError::Db {
                context: "prepare get multipart part segments for upload part",
                source: e,
            })?;

        let rows = stmt
            .query_map(params![bucket, key, upload_id, part_number], |row| {
                let okh_blob: Vec<u8> = row.get(8)?;
                let segment_okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                Ok(MultipartPartSegmentRecord {
                    bucket: row.get(0)?,
                    key: row.get(1)?,
                    upload_id: row.get(2)?,
                    version_id: row.get::<_, i64>(3)? as u64,
                    part_number: row.get(4)?,
                    segment_index: row.get(5)?,
                    size: row.get::<_, i64>(6)? as u64,
                    segment_crc64: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                    segment_okh,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(9)?,
                        9,
                        "segment_vid",
                    )?,
                    data_pg_id: row.get(10)?,
                    ec_k: row.get(11)?,
                    ec_m: row.get(12)?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "get multipart part segments for upload part",
                source: e,
            })?;

        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "get multipart part segments for upload part row",
                source: e,
            })
    }

    #[cfg(test)]
    fn delete_multipart_part_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.delete_multipart_part_segments_direct(bucket, key, version_id)
    }

    fn get_all_multipart_part_segments_for_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT bucket, key, upload_id, version_id, part_number, segment_index, \
                 size, segment_crc64, segment_okh, segment_vid, data_pg_id, ec_k, ec_m \
                 FROM multipart_part_segments \
                 WHERE upload_id = ?1 \
                 ORDER BY part_number, segment_index",
            )
            .map_err(|e| MetadataError::Db {
                context: "get all multipart part segments for upload (prepare)",
                source: e,
            })?;
        let rows = stmt
            .query_map(params![upload_id.as_str()], |row| {
                let okh_blob: Vec<u8> = row.get(8)?;
                let segment_okh = PgStore::parse_okh_blob(&okh_blob, 8)?;
                Ok(MultipartPartSegmentRecord {
                    bucket: row.get(0)?,
                    key: row.get(1)?,
                    upload_id: row.get(2)?,
                    version_id: row.get::<_, i64>(3)? as u64,
                    part_number: row.get(4)?,
                    segment_index: row.get(5)?,
                    size: row.get::<_, i64>(6)? as u64,
                    segment_crc64: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
                    segment_okh,
                    segment_vid: Self::parse_generation_id(
                        row.get::<_, i64>(9)?,
                        9,
                        "segment_vid",
                    )?,
                    data_pg_id: row.get(10)?,
                    ec_k: row.get(11)?,
                    ec_m: row.get(12)?,
                })
            })
            .map_err(|e| MetadataError::Db {
                context: "get all multipart part segments for upload (query)",
                source: e,
            })?;
        rows.collect::<Result<Vec<_>, _>>()
            .map_err(|e| MetadataError::Db {
                context: "get all multipart part segments for upload (collect)",
                source: e,
            })
    }

    #[cfg(test)]
    fn delete_multipart_part_segments_by_upload_id(
        &self,
        upload_id: &UploadId,
    ) -> Result<(), MetadataError> {
        self.delete_multipart_part_segments_by_upload_id_direct(upload_id)
    }
}

impl PgStore {
    fn delete_object_generation_reservation_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_generation_reservations \
                 WHERE reservation_id = ?1 AND bucket = ?2 AND key = ?3",
                params![reservation_id.as_str(), bucket, key],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object generation reservation",
                source: e,
            })?;
        Ok(())
    }

    fn delete_object_segments_reclaim_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_segments_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object segments reclaim",
                source: e,
            })?;
        Ok(())
    }

    fn delete_multipart_reclaim_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_reclaims \
                 WHERE bucket = ?1 AND key = ?2 AND generation_id = ?3",
                params![bucket, key, generation_id.get() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart reclaim",
                source: e,
            })?;
        Ok(())
    }

    fn delete_object_parts_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_parts \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete object parts",
                source: e,
            })?;
        Ok(())
    }

    fn set_stream_upload_state_direct(
        &self,
        session_id: &SessionId,
        new_state: StreamUploadState,
    ) -> Result<(), MetadataError> {
        let current: u8 = self
            .conn
            .query_row(
                "SELECT state FROM stream_uploads WHERE session_id = ?1",
                params![session_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| MetadataError::Db {
                context: "get stream upload state",
                source: e,
            })?
            .ok_or_else(|| MetadataError::StreamSessionNotFound {
                session_id: session_id.as_str().to_owned(),
            })?;

        if current != StreamUploadState::InProgress as u8 {
            return Err(MetadataError::StreamSessionNotInProgress { state: current });
        }

        self.conn
            .execute(
                "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                params![new_state as u8, session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "set stream upload state",
                source: e,
            })?;
        Ok(())
    }

    fn delete_stream_upload_direct(&self, session_id: &SessionId) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM stream_uploads WHERE session_id = ?1",
                params![session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete stream upload",
                source: e,
            })?;
        self.conn
            .execute(
                "DELETE FROM object_generation_reservations WHERE reservation_id = ?1",
                params![session_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete stream upload generation reservation",
                source: e,
            })?;
        Ok(())
    }

    fn append_stream_segment_direct(
        &self,
        segment: &StreamUploadSegmentRecord,
    ) -> Result<(), MetadataError> {
        self.conn
                .execute(
                    "INSERT INTO stream_upload_segments \
                 (session_id, segment_index, size, segment_crc64, segment_okh, segment_vid, data_pg_id, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    segment.session_id,
                    segment.segment_index,
                    segment.size as i64,
                    segment.segment_crc64.map(|v| v as i64),
                    segment.segment_okh.as_slice(),
                    segment.segment_vid.get() as i64,
                    segment.data_pg_id,
                    segment.ec_k,
                    segment.ec_m,
                ],
            )
            .map_err(|e| MetadataError::Db {
                context: "append stream segment",
                source: e,
            })?;
        Ok(())
    }

    fn delete_object_segments_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM object_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete stream object segments",
                source: e,
            })?;
        Ok(())
    }

    fn delete_multipart_part_segments_direct(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments \
                 WHERE bucket = ?1 AND key = ?2 AND version_id = ?3",
                params![bucket, key, version_id.to_u64() as i64],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart part segments",
                source: e,
            })?;
        Ok(())
    }

    fn delete_multipart_part_segments_by_upload_id_direct(
        &self,
        upload_id: &UploadId,
    ) -> Result<(), MetadataError> {
        self.conn
            .execute(
                "DELETE FROM multipart_part_segments WHERE upload_id = ?1",
                params![upload_id.as_str()],
            )
            .map_err(|e| MetadataError::Db {
                context: "delete multipart part segments by upload_id",
                source: e,
            })?;
        Ok(())
    }
}

/// Compute the exclusive end of a prefix range for efficient SQL queries.
///
/// For prefix "foo", returns Some("fop") — the next string after all strings
/// starting with "foo". Returns None if the prefix is all 0xFF bytes (no upper bound).
/// fsync a directory to ensure renames are durable.
fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    let f = fs::File::open(dir)?;
    f.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_command::{MetadataCommandId, MetadataCommandLogIndex};
    use crate::traits::PgMetadataStore;

    fn test_owner() -> OwnerIdentity {
        OwnerIdentity::from_principal("owner")
    }

    fn create_bucket_probe_command(
        pg_id: u32,
        log_index: u64,
        bucket: BucketName,
        bucket_execution_generation: u64,
    ) -> MetadataCommandEnvelope {
        let owner = test_owner();
        let config = CreateBucketConfig {
            name: bucket.as_str(),
            owner_principal: &owner.principal,
            owner_canonical_id: &owner.canonical_id,
            acl_grants: &AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: BucketVersioningState::Disabled,
            object_lock: BucketObjectLockConfig::default(),
        };
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(pg_id),
                MetadataCommandLogIndex::new(log_index).unwrap(),
            ),
            MetadataCommandPayload::CreateBucket(
                CreateBucketCommand::from_config(&config, 123, bucket_execution_generation)
                    .unwrap(),
            ),
        )
    }

    fn assert_metadata_state_digest_mismatch(err: StoreError) {
        assert!(
            matches!(err, StoreError::MetadataStateDigestMismatch { .. }),
            "expected metadata state digest mismatch, got {err:?}"
        );
    }

    fn assert_metadata_state_digest_covers_mutation(
        setup: impl FnOnce(&PgStore),
        mutate: impl FnOnce(&PgStore),
    ) {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        setup(&store);
        store.refresh_metadata_command_state_digest().unwrap();
        mutate(&store);

        let err = store
            .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
            .unwrap_err();
        assert_metadata_state_digest_mismatch(err);
    }

    fn assert_cached_metadata_digest_matches_materialized(store: &PgStore) {
        let cached = store.cached_metadata_state_digest().unwrap();
        let materialized = store.metadata_state_digest().unwrap();
        assert_eq!(cached, materialized);
    }

    #[test]
    fn metadata_digest_cache_tracks_row_changes_incrementally() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("digest-bucket");
        let key = trusted_object_key("object");

        assert_cached_metadata_digest_matches_materialized(&store);
        store
            .conn
            .execute(
                "INSERT INTO object_version_counters \
                 (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
                params![bucket.as_str(), key.as_str(), 2_i64],
            )
            .unwrap();
        assert_cached_metadata_digest_matches_materialized(&store);

        store
            .conn
            .execute(
                "UPDATE object_version_counters SET next_version_id = ?1 \
                 WHERE bucket = ?2 AND key = ?3",
                params![3_i64, bucket.as_str(), key.as_str()],
            )
            .unwrap();
        assert_cached_metadata_digest_matches_materialized(&store);

        store
            .conn
            .execute(
                "INSERT OR REPLACE INTO object_version_counters \
                 (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
                params![bucket.as_str(), key.as_str(), 4_i64],
            )
            .unwrap();
        assert_cached_metadata_digest_matches_materialized(&store);

        store
            .conn
            .execute(
                "DELETE FROM object_version_counters WHERE bucket = ?1 AND key = ?2",
                params![bucket.as_str(), key.as_str()],
            )
            .unwrap();
        assert_cached_metadata_digest_matches_materialized(&store);
    }

    #[test]
    fn metadata_command_acceptance_detects_dirty_cached_digest() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("digest-bucket");
        let key = trusted_object_key("object");
        store
            .conn
            .execute(
                "INSERT INTO object_version_counters \
				 (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
                params![bucket.as_str(), key.as_str(), 2_i64],
            )
            .unwrap();
        store.refresh_metadata_command_state_digest().unwrap();

        store
            .conn
            .execute(
                "UPDATE object_version_counters SET next_version_id = ?1 \
				 WHERE bucket = ?2 AND key = ?3",
                params![3_i64, bucket.as_str(), key.as_str()],
            )
            .unwrap();

        let command =
            create_bucket_probe_command(store.pg_id, 1, trusted_bucket_name("new-bucket"), 1);
        let err = store.metadata_command_acceptance(0, &command).unwrap_err();
        assert_metadata_state_digest_mismatch(err);
    }

    #[test]
    fn metadata_command_acceptance_detects_cross_connection_dirty_cached_digest() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("digest-bucket");
        let key = trusted_object_key("object");
        store
            .conn
            .execute(
                "INSERT INTO object_version_counters \
				 (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
                params![bucket.as_str(), key.as_str(), 2_i64],
            )
            .unwrap();
        store.refresh_metadata_command_state_digest().unwrap();

        let other_store = PgStore::open(tmp.path(), 1).unwrap();
        other_store
            .conn
            .execute(
                "UPDATE object_version_counters SET next_version_id = ?1 \
				 WHERE bucket = ?2 AND key = ?3",
                params![3_i64, bucket.as_str(), key.as_str()],
            )
            .unwrap();

        let command =
            create_bucket_probe_command(store.pg_id, 1, trusted_bucket_name("new-bucket"), 1);
        let err = store.metadata_command_acceptance(0, &command).unwrap_err();
        assert_metadata_state_digest_mismatch(err);
    }

    #[test]
    fn rolled_back_command_record_does_not_mark_digest_revision_clean() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("digest-bucket");
        let key = trusted_object_key("object");
        store
            .conn
            .execute(
                "INSERT INTO object_version_counters \
                 (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
                params![bucket.as_str(), key.as_str(), 2_i64],
            )
            .unwrap();
        store.refresh_metadata_command_state_digest().unwrap();
        let clean_revision = store.clean_metadata_digest_revision.load(Ordering::Relaxed);

        let command =
            create_bucket_probe_command(store.pg_id, 1, trusted_bucket_name("new-bucket"), 1);
        store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        store.apply_metadata_command(&command).unwrap();
        store.record_metadata_command_applied(0, &command).unwrap();
        let transaction_revision = store.metadata_digest_revision().unwrap();
        assert_ne!(clean_revision, transaction_revision);
        store.conn.execute_batch("ROLLBACK").unwrap();

        assert_eq!(
            clean_revision,
            store.clean_metadata_digest_revision.load(Ordering::Relaxed),
            "recording inside an uncommitted transaction must not advance the clean revision"
        );

        let other_store = PgStore::open(tmp.path(), 1).unwrap();
        other_store
            .conn
            .execute(
                "UPDATE object_version_counters SET next_version_id = ?1 \
                 WHERE bucket = ?2 AND key = ?3",
                params![3_i64, bucket.as_str(), key.as_str()],
            )
            .unwrap();

        let err = store.metadata_command_acceptance(0, &command).unwrap_err();
        assert_metadata_state_digest_mismatch(err);
    }

    #[test]
    fn metadata_digest_trigger_bootstrap_repairs_partial_install() {
        let tmp = test_util::tempdir();
        let bucket = trusted_bucket_name("digest-bucket");
        let key = trusted_object_key("object");
        {
            let store = PgStore::open(tmp.path(), 1).unwrap();
            store
                .conn
                .execute(
                    "INSERT INTO object_version_counters \
                     (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
                    params![bucket.as_str(), key.as_str(), 2_i64],
                )
                .unwrap();
            assert_cached_metadata_digest_matches_materialized(&store);

            store
                .conn
                .execute_batch(
                    "DROP TRIGGER metadata_digest_object_version_counters_ad; \
                     DROP TRIGGER metadata_digest_object_version_counters_au;",
                )
                .unwrap();
            store
                .conn
                .execute(
                    "DELETE FROM object_version_counters WHERE bucket = ?1 AND key = ?2",
                    params![bucket.as_str(), key.as_str()],
                )
                .unwrap();
            assert_ne!(
                store.cached_metadata_state_digest().unwrap(),
                store.metadata_state_digest().unwrap(),
                "simulated partial trigger install should leave stale cached digest before reopen",
            );
        }

        let store = PgStore::open(tmp.path(), 1).unwrap();
        assert_cached_metadata_digest_matches_materialized(&store);
        store
            .conn
            .execute(
                "INSERT INTO object_version_counters \
                 (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
                params![bucket.as_str(), key.as_str(), 2_i64],
            )
            .unwrap();
        store
            .conn
            .execute(
                "UPDATE object_version_counters SET next_version_id = ?1 \
                 WHERE bucket = ?2 AND key = ?3",
                params![3_i64, bucket.as_str(), key.as_str()],
            )
            .unwrap();
        store
            .conn
            .execute(
                "DELETE FROM object_version_counters WHERE bucket = ?1 AND key = ?2",
                params![bucket.as_str(), key.as_str()],
            )
            .unwrap();
        assert_cached_metadata_digest_matches_materialized(&store);
    }

    #[test]
    fn metadata_digest_bootstrap_marker_repairs_stale_cache_with_complete_triggers() {
        let tmp = test_util::tempdir();
        let bucket = trusted_bucket_name("digest-bucket");
        let key = trusted_object_key("object");
        {
            let store = PgStore::open(tmp.path(), 1).unwrap();
            store
                .conn
                .execute(
                    "INSERT INTO object_version_counters \
                     (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
                    params![bucket.as_str(), key.as_str(), 2_i64],
                )
                .unwrap();
            assert_cached_metadata_digest_matches_materialized(&store);

            store
                .conn
                .execute(
                    "UPDATE metadata_table_digests \
                     SET table_digest = 0, row_count = 0, row_hash_xor = 0, row_hash_sum = 0 \
                     WHERE table_name = ?1",
                    params!["object_version_counters"],
                )
                .unwrap();
            store
                .conn
                .execute("DELETE FROM metadata_digest_bootstrap_state", [])
                .unwrap();
            assert_ne!(
                store.cached_metadata_state_digest().unwrap(),
                store.metadata_state_digest().unwrap(),
                "simulated interrupted bootstrap should leave stale cache with all triggers present",
            );
        }

        let store = PgStore::open(tmp.path(), 1).unwrap();
        assert_cached_metadata_digest_matches_materialized(&store);
        assert!(store.metadata_digest_bootstrap_complete().unwrap());
    }

    fn insert_digest_multipart_upload(store: &PgStore, upload_id: &UploadId) {
        let owner = test_owner();
        let bucket = trusted_bucket_name("digest-bucket");
        let key = trusted_object_key("object");
        store
            .conn
            .execute(
                "INSERT INTO multipart_uploads \
                 (upload_id, bucket, key, initiated_at, state, tags, metadata_blob, \
                  system_metadata_blob, owner_principal, owner_canonical_id, \
                  initiator_principal, initiator_canonical_id, checksum_algorithm, checksum_type, \
                  encryption_type, encryption_state, acl_grants, public_read, object_generation_id, \
                  object_lock_retention_mode, object_lock_retain_until, object_lock_legal_hold) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)",
                params![
                    upload_id.as_str(),
                    bucket.as_str(),
                    key.as_str(),
                    101_i64,
                    UploadState::InProgress as u8,
                    Option::<&str>::None,
                    b"meta".as_slice(),
                    b"system".as_slice(),
                    owner.principal.as_str(),
                    owner.canonical_id.as_str(),
                    Option::<&str>::None,
                    Option::<&str>::None,
                    Option::<u8>::None,
                    Option::<u8>::None,
                    ObjectEncryption::None.encryption_type() as u8,
                    Option::<&[u8]>::None,
                    "",
                    0_i64,
                    1_i64,
                    Option::<i64>::None,
                    Option::<i64>::None,
                    StoredLegalHoldStatus::NotSet as u8,
                ],
            )
            .unwrap();
    }

    #[test]
    fn metadata_state_digest_table_inventory_is_explicit() {
        let tables: Vec<_> = METADATA_DIGEST_TABLES
            .iter()
            .map(|table| (table.name, table.filter))
            .collect();
        assert_eq!(
            tables,
            vec![
                ("bucket_subresources", MetadataDigestFilter::AllRows),
                ("buckets", MetadataDigestFilter::AllRows),
                ("completed_multipart_uploads", MetadataDigestFilter::AllRows),
                ("multipart_part_segments", MetadataDigestFilter::AllRows),
                ("multipart_parts", MetadataDigestFilter::AllRows),
                (
                    "multipart_reclaim_part_segments",
                    MetadataDigestFilter::AllRows
                ),
                ("multipart_reclaim_parts", MetadataDigestFilter::AllRows),
                ("multipart_reclaims", MetadataDigestFilter::AllRows),
                ("multipart_uploads", MetadataDigestFilter::AllRows),
                (
                    "object_generation_reservations",
                    MetadataDigestFilter::AllRows
                ),
                ("object_version_counters", MetadataDigestFilter::AllRows),
                ("object_parts", MetadataDigestFilter::AllRows),
                (
                    "object_segment_reclaim_segments",
                    MetadataDigestFilter::AllRows
                ),
                ("object_segments", MetadataDigestFilter::AllRows),
                ("object_segments_reclaims", MetadataDigestFilter::AllRows),
                ("objects", MetadataDigestFilter::AllRows),
                ("pg_counters", MetadataDigestFilter::AllRows),
                ("stream_upload_segments", MetadataDigestFilter::AllRows),
                ("stream_uploads", MetadataDigestFilter::AllRows),
            ]
        );

        for table in METADATA_DIGEST_TABLES {
            assert!(
                !table.columns.is_empty(),
                "{} must have explicit digest columns",
                table.name
            );
        }
    }

    #[test]
    fn canonical_metadata_value_encoding_is_typed() {
        fn digest_for(value: ValueRef<'_>) -> u64 {
            let mut hasher = checksum::crc64::Hasher::new();
            PgStore::digest_canonical_sql_value(&mut hasher, value);
            hasher.finalize()
        }

        assert_ne!(digest_for(ValueRef::Null), digest_for(ValueRef::Text(b"")));
        assert_ne!(
            digest_for(ValueRef::Integer(12)),
            digest_for(ValueRef::Text(b"12"))
        );
        assert_ne!(
            digest_for(ValueRef::Text(b"bytes")),
            digest_for(ValueRef::Blob(b"bytes"))
        );
        assert_ne!(
            digest_for(ValueRef::Integer(-1)),
            digest_for(ValueRef::Integer(1))
        );
    }

    #[test]
    fn canonical_metadata_variable_length_encoding_is_prefix_free() {
        fn digest_values(values: &[ValueRef<'_>]) -> u64 {
            let mut hasher = checksum::crc64::Hasher::new();
            for value in values {
                PgStore::digest_canonical_sql_value(&mut hasher, *value);
            }
            hasher.finalize()
        }

        fn digest_names(names: &[&[u8]]) -> u64 {
            let mut hasher = checksum::crc64::Hasher::new();
            for name in names {
                digest_len_prefixed_bytes(&mut hasher, name);
            }
            hasher.finalize()
        }

        assert_ne!(
            digest_values(&[ValueRef::Blob(b"\x04"), ValueRef::Blob(b"")]),
            digest_values(&[ValueRef::Blob(b""), ValueRef::Blob(b"\x04")])
        );
        assert_ne!(
            digest_values(&[ValueRef::Text(b"a"), ValueRef::Text(b"bc")]),
            digest_values(&[ValueRef::Text(b"ab"), ValueRef::Text(b"c")])
        );
        assert_ne!(
            digest_names(&[b"table", b"_range"]),
            digest_names(&[b"table_", b"range"])
        );
    }

    // ── prefix_end ────────────────────────────────────────────────────

    #[test]
    fn key_prefix_upper_bound_basic() {
        assert_eq!(key_prefix_upper_bound("foo"), Some("fop".to_string()));
    }

    #[test]
    fn key_prefix_upper_bound_empty() {
        assert_eq!(key_prefix_upper_bound(""), None);
    }

    #[test]
    fn key_prefix_upper_bound_del_char() {
        assert_eq!(key_prefix_upper_bound("\x7f"), Some("\u{80}".to_string()));
    }

    #[test]
    fn key_prefix_upper_bound_trailing_del() {
        assert_eq!(
            key_prefix_upper_bound("abc\x7f"),
            Some("abc\u{80}".to_string())
        );
    }

    #[test]
    fn key_prefix_upper_bound_tilde() {
        assert_eq!(key_prefix_upper_bound("~"), Some("\x7f".to_string()));
    }

    // ── pg_id accessor ────────────────────────────────────────────────

    #[test]
    fn pg_id_accessor() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 42).unwrap();
        assert_eq!(store.pg_id(), 42);
    }

    #[test]
    fn metadata_command_apply_and_record_rolls_back_metadata_on_record_conflict() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("rollback-bucket");
        let command = create_bucket_probe_command(1, 1, bucket.clone(), 1);
        let conflicting_command =
            create_bucket_probe_command(1, 1, trusted_bucket_name("conflict-bucket"), 1);
        store
            .conn
            .execute(
                "INSERT INTO metadata_command_log \
                 (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL, NULL)",
                params![
                    ClusterEpoch::INITIAL.get() as i64,
                    1_i64,
                    1_i64,
                    conflicting_command.checksum_crc64() as i64,
                    conflicting_command.command_bytes(),
                ],
            )
            .unwrap();

        let err = store
            .apply_metadata_command_and_record(0, &command)
            .unwrap_err();
        assert!(
            matches!(
                err,
                BucketSnapshotLoadError::Store(StoreError::MetadataCommandLogConflict { .. })
            ),
            "expected log conflict, got {err:?}"
        );
        assert!(
            matches!(
                store.head_bucket_raw(&bucket).unwrap_err(),
                MetadataError::BucketNotFound { .. }
            ),
            "bucket creation must roll back when log recording fails"
        );
    }

    #[test]
    fn metadata_command_log_contiguous_insert_uses_prefix_fast_path() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let command = create_bucket_probe_command(1, 1, trusted_bucket_name("fast-path"), 1);
        let before = store
            .metadata_command_log_prefix_fast_path_hits
            .load(Ordering::Relaxed);

        let state = store.record_metadata_command_applied(0, &command).unwrap();

        assert!(
            store
                .metadata_command_log_prefix_fast_path_hits
                .load(Ordering::Relaxed)
                > before,
            "contiguous insert without a durable tail should take the prefix fast path"
        );
        let expected_log_hash = metadata_command_log_hash(
            ClusterEpoch::INITIAL,
            PgId::new(1),
            MetadataCommandLogIndex::new(1).unwrap(),
            0,
            command.checksum_crc64(),
        );
        assert_eq!(state.applied_log_index, 1);
        assert_eq!(state.applied_log_hash, expected_log_hash);

        let (previous_log_hash, log_hash): (Option<i64>, Option<i64>) = store
            .conn
            .query_row(
                "SELECT previous_log_hash, log_hash \
                 FROM metadata_command_log WHERE log_index = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(previous_log_hash, Some(0));
        assert_eq!(log_hash, Some(expected_log_hash as i64));
    }

    #[test]
    fn pending_metadata_command_slot_is_pg_scoped_and_persistent() {
        let tmp = test_util::tempdir();
        let first_bucket = trusted_bucket_name("pending-slot-one");
        let second_bucket = trusted_bucket_name("pending-slot-two");
        let first_command = create_bucket_probe_command(1, 1, first_bucket.clone(), 1);
        let second_command = create_bucket_probe_command(1, 2, second_bucket.clone(), 2);

        {
            let store = PgStore::open(tmp.path(), 1).unwrap();
            store
                .try_insert_pending_metadata_command_slot(0, &first_command, Some(&first_bucket))
                .unwrap();
            store
                .try_insert_pending_metadata_command_slot(0, &first_command, Some(&first_bucket))
                .unwrap();

            let err = store
                .try_insert_pending_metadata_command_slot(0, &second_command, Some(&second_bucket))
                .unwrap_err();
            assert!(matches!(
                err,
                StoreError::MetadataCommandPendingConflict {
                    pg_id: 1,
                    existing_log_index: 1,
                    candidate_log_index: 2,
                    ..
                }
            ));
        }

        let store = PgStore::open(tmp.path(), 1).unwrap();
        let slot = store
            .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
            .unwrap()
            .expect("pending slot should persist across reopen");
        assert_eq!(slot.id, first_command.id());
        assert_eq!(slot.command_checksum, first_command.checksum_crc64());
        assert_eq!(slot.command_bytes, first_command.command_bytes());
        assert_eq!(slot.scope_bucket.as_ref(), Some(&first_bucket));
        let err = store
            .remove_pending_metadata_command_slot(0, &first_command)
            .unwrap_err();
        assert!(
            matches!(err, StoreError::MetadataCommandLogConflict { .. }),
            "slot removal before a terminal log row must fail, got {err:?}"
        );
        store
            .record_metadata_command_abandoned(0, &first_command)
            .unwrap();

        assert!(
            !store
                .remove_pending_metadata_command_slot(0, &second_command)
                .unwrap(),
            "non-matching command must not clear the durable slot"
        );
        assert!(
            store
                .remove_pending_metadata_command_slot(0, &first_command)
                .unwrap(),
            "matching command should clear the durable slot"
        );
        assert!(
            store
                .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
                .unwrap()
                .is_none(),
            "slot should be empty after exact removal"
        );
    }

    #[test]
    fn pending_metadata_command_slot_validation_rejects_log_gap() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("pending-slot-gap");
        let command = create_bucket_probe_command(1, 2, bucket.clone(), 2);
        store
            .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
            .unwrap();

        let err = store
            .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
            .unwrap_err();
        assert!(
            matches!(err, StoreError::MetadataCommandLogConflict { .. }),
            "pending slot ahead of applied prefix must fail validation, got {err:?}"
        );
    }

    #[test]
    fn terminal_pending_metadata_command_slot_is_cleaned_on_validation() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("terminal-pending-slot");
        let command = create_bucket_probe_command(1, 1, bucket.clone(), 1);
        store
            .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
            .unwrap();
        store.record_metadata_command_applied(0, &command).unwrap();

        let state = store
            .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
            .unwrap();
        assert_eq!(state.applied_log_index, 1);
        assert!(
            store
                .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
                .unwrap()
                .is_none(),
            "validation should clean a pending slot whose terminal record is already durable"
        );
    }

    #[test]
    fn abandoned_pending_slot_with_unadvanced_replica_state_recovers_on_validation() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("abandoned-pending-slot");
        let command = create_bucket_probe_command(1, 1, bucket.clone(), 1);
        store
            .try_insert_pending_metadata_command_slot(0, &command, Some(&bucket))
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO metadata_command_log \
                 (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL, NULL)",
                params![
                    ClusterEpoch::INITIAL.get() as i64,
                    1_i64,
                    1_i64,
                    command.abandoned_log_checksum_crc64() as i64,
                    command.abandoned_log_bytes(),
                ],
            )
            .unwrap();

        let before = store.metadata_command_replica_state().unwrap();
        assert_eq!(before.applied_log_index, 0);

        let recovered = store
            .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
            .unwrap();
        assert_eq!(recovered.applied_log_index, 1);
        assert!(
            store
                .pending_metadata_command_slot(0, ClusterEpoch::INITIAL)
                .unwrap()
                .is_none(),
            "validation should advance matching abandoned row and clean the pending slot"
        );
    }

    #[test]
    fn abandoned_metadata_command_log_rows_match_original_command() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let abandoned_command =
            create_bucket_probe_command(1, 1, trusted_bucket_name("abandoned-bucket"), 1);
        let different_command =
            create_bucket_probe_command(1, 1, trusted_bucket_name("different-bucket"), 1);

        store
            .record_metadata_command_abandoned(0, &abandoned_command)
            .unwrap();

        assert_eq!(
            store
                .metadata_command_abandon_acceptance(0, &abandoned_command)
                .unwrap(),
            MetadataCommandAcceptance::AlreadyApplied
        );
        assert!(
            store
                .metadata_command_abandoned(0, &abandoned_command)
                .unwrap(),
            "abandoned row should match its original command"
        );
        assert!(
            !store
                .metadata_command_abandoned(0, &different_command)
                .unwrap(),
            "abandoned tombstones are tied to the original command checksum"
        );
        assert!(matches!(
            store
                .metadata_command_abandon_acceptance(0, &different_command)
                .unwrap_err(),
            StoreError::MetadataCommandLogConflict { .. }
        ));
    }

    #[test]
    fn metadata_command_log_stats_include_retained_prefix_and_tail() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let applied_command =
            create_bucket_probe_command(1, 1, trusted_bucket_name("stats-applied"), 1);
        let abandoned_command =
            create_bucket_probe_command(1, 2, trusted_bucket_name("stats-abandoned"), 2);
        let tail_command = create_bucket_probe_command(1, 4, trusted_bucket_name("stats-tail"), 4);

        store
            .record_metadata_command_applied(0, &applied_command)
            .unwrap();
        store
            .record_metadata_command_abandoned(0, &abandoned_command)
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO metadata_command_log \
                 (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL, NULL)",
                params![
                    ClusterEpoch::INITIAL.get() as i64,
                    1_i64,
                    4_i64,
                    tail_command.checksum_crc64() as i64,
                    tail_command.command_bytes(),
                ],
            )
            .unwrap();

        let stats = store
            .metadata_command_log_stats(ClusterEpoch::INITIAL)
            .unwrap();
        assert_eq!(stats.cluster_epoch, ClusterEpoch::INITIAL);
        assert_eq!(stats.pg_id, PgId::new(1));
        assert_eq!(stats.min_log_index, Some(1));
        assert_eq!(stats.max_log_index, Some(4));
        assert_eq!(stats.applied_log_index, 2);
        assert_eq!(stats.retained_entries, 3);
        assert_eq!(stats.abandoned_entries, 1);
        assert_eq!(stats.pending_tail_entries, 1);
        assert_eq!(stats.missing_applied_prefix_entries, 0);
        assert_eq!(stats.compactable_before, None);
    }

    #[test]
    fn metadata_command_log_compaction_without_checkpoint_is_explicit_noop() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let first = create_bucket_probe_command(1, 1, trusted_bucket_name("retain-first"), 1);
        let second = create_bucket_probe_command(1, 2, trusted_bucket_name("retain-second"), 2);

        store.record_metadata_command_applied(0, &first).unwrap();
        store.record_metadata_command_abandoned(0, &second).unwrap();

        let before = store
            .metadata_command_log_stats(ClusterEpoch::INITIAL)
            .unwrap();
        assert_eq!(before.retained_entries, 2);

        let status = store
            .compact_metadata_command_log_without_checkpoint(ClusterEpoch::INITIAL)
            .unwrap();
        assert_eq!(
            status,
            MetadataCommandLogCompactionStatus::UnsupportedUntilCheckpoint {
                retained_entries: 2
            }
        );

        let after = store
            .metadata_command_log_stats(ClusterEpoch::INITIAL)
            .unwrap();
        assert_eq!(after, before);
    }

    #[test]
    fn metadata_command_log_stats_reject_stale_epoch() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let stale_epoch = ClusterEpoch::new(2).unwrap();

        let err = store.metadata_command_log_stats(stale_epoch).unwrap_err();
        assert!(matches!(
            err,
            StoreError::StaleMetadataOperation {
                pg_id: 1,
                operation_epoch,
                current_epoch: ClusterEpoch::INITIAL,
            } if operation_epoch == stale_epoch
        ));

        let err = store
            .compact_metadata_command_log_without_checkpoint(stale_epoch)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::StaleMetadataOperation {
                pg_id: 1,
                operation_epoch,
                current_epoch: ClusterEpoch::INITIAL,
            } if operation_epoch == stale_epoch
        ));
    }

    #[test]
    fn metadata_command_log_prefix_rejects_row_key_and_kind_mismatch() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let command_with_wrong_embedded_index =
            create_bucket_probe_command(1, 2, trusted_bucket_name("wrong-index"), 1);
        store
            .conn
            .execute(
                "INSERT INTO metadata_command_log \
                 (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL, NULL)",
                params![
                    ClusterEpoch::INITIAL.get() as i64,
                    1_i64,
                    1_i64,
                    command_with_wrong_embedded_index.checksum_crc64() as i64,
                    command_with_wrong_embedded_index.command_bytes(),
                ],
            )
            .unwrap();
        let next_command = create_bucket_probe_command(1, 2, trusted_bucket_name("next"), 1);
        let err = store
            .record_metadata_command_applied(0, &next_command)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::MetadataCommandLogConflict {
                pg_id: 1,
                log_index: 1,
                ..
            }
        ));

        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let applied_command = create_bucket_probe_command(1, 1, trusted_bucket_name("applied"), 1);
        store
            .conn
            .execute(
                "INSERT INTO metadata_command_log \
                 (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 1, NULL, NULL)",
                params![
                    ClusterEpoch::INITIAL.get() as i64,
                    1_i64,
                    1_i64,
                    applied_command.checksum_crc64() as i64,
                    applied_command.command_bytes(),
                ],
            )
            .unwrap();
        let next_command = create_bucket_probe_command(1, 2, trusted_bucket_name("next"), 1);
        let err = store
            .record_metadata_command_applied(0, &next_command)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::MetadataCommandLogConflict {
                pg_id: 1,
                log_index: 1,
                ..
            }
        ));
    }

    #[test]
    fn metadata_command_log_prefix_rejects_malformed_applied_bytes() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let command = create_bucket_probe_command(1, 1, trusted_bucket_name("malformed"), 1);
        let mut malformed_bytes = command.command_bytes();
        malformed_bytes.push(0);
        let malformed_checksum = checksum::crc64::checksum(&malformed_bytes);

        store
            .conn
            .execute(
                "INSERT INTO metadata_command_log \
                 (cluster_epoch, pg_id, log_index, command_checksum, command_bytes, abandoned, previous_log_hash, log_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5, 0, NULL, NULL)",
                params![
                    ClusterEpoch::INITIAL.get() as i64,
                    1_i64,
                    1_i64,
                    malformed_checksum as i64,
                    malformed_bytes,
                ],
            )
            .unwrap();
        let next_command = create_bucket_probe_command(1, 2, trusted_bucket_name("next"), 1);
        let err = store
            .record_metadata_command_applied(0, &next_command)
            .unwrap_err();
        assert!(matches!(
            err,
            StoreError::MetadataCommandLogConflict {
                pg_id: 1,
                log_index: 1,
                ..
            }
        ));
    }

    #[test]
    fn metadata_state_digest_covers_object_segments() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("digest-bucket");
        let key = trusted_object_key("object");
        let okh = [1_u8; 16];
        store
            .conn
            .execute(
                "INSERT INTO object_segments \
                 (bucket, key, version_id, segment_index, size, segment_crc64, \
                  segment_okh, segment_vid, data_pg_id, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    bucket.as_str(),
                    key.as_str(),
                    1_i64,
                    0_i64,
                    32_i64,
                    99_i64,
                    okh.as_slice(),
                    1_i64,
                    1_i64,
                    4_i64,
                    2_i64,
                ],
            )
            .unwrap();
        store.refresh_metadata_command_state_digest().unwrap();
        store
            .conn
            .execute(
                "UPDATE object_segments SET data_pg_id = ?1 \
                 WHERE bucket = ?2 AND key = ?3 AND version_id = ?4",
                params![2_i64, bucket.as_str(), key.as_str(), 1_i64],
            )
            .unwrap();

        let err = store
            .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
            .unwrap_err();
        assert_metadata_state_digest_mismatch(err);
    }

    #[test]
    fn metadata_state_digest_covers_multipart_part_segments() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("digest-bucket");
        let key = trusted_object_key("object");
        let upload_id = UploadId::new("u".repeat(UPLOAD_ID_LEN)).unwrap();
        let okh = [2_u8; 16];
        store
            .conn
            .execute(
                "INSERT INTO multipart_part_segments \
                 (bucket, key, upload_id, version_id, part_number, segment_index, size, \
                  segment_crc64, segment_okh, segment_vid, data_pg_id, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    bucket.as_str(),
                    key.as_str(),
                    upload_id.as_str(),
                    1_i64,
                    1_i64,
                    0_i64,
                    32_i64,
                    99_i64,
                    okh.as_slice(),
                    1_i64,
                    1_i64,
                    4_i64,
                    2_i64,
                ],
            )
            .unwrap();
        store.refresh_metadata_command_state_digest().unwrap();
        store
            .conn
            .execute(
                "UPDATE multipart_part_segments SET ec_m = ?1 \
                 WHERE bucket = ?2 AND key = ?3 AND upload_id = ?4",
                params![3_i64, bucket.as_str(), key.as_str(), upload_id.as_str()],
            )
            .unwrap();

        let err = store
            .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
            .unwrap_err();
        assert_metadata_state_digest_mismatch(err);
    }

    #[test]
    fn metadata_state_digest_covers_multipart_part_staging_segments() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("digest-bucket");
        let key = trusted_object_key("object");
        let upload_id = UploadId::new("u".repeat(UPLOAD_ID_LEN)).unwrap();
        let okh = [3_u8; 16];
        store
            .conn
            .execute(
                "INSERT INTO multipart_part_segments \
                 (bucket, key, upload_id, version_id, part_number, segment_index, size, \
                  segment_crc64, segment_okh, segment_vid, data_pg_id, ec_k, ec_m) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                params![
                    bucket.as_str(),
                    key.as_str(),
                    upload_id.as_str(),
                    PART_SEGMENT_STAGING_VERSION_ID.to_u64() as i64,
                    1_i64,
                    0_i64,
                    32_i64,
                    99_i64,
                    okh.as_slice(),
                    1_i64,
                    1_i64,
                    4_i64,
                    2_i64,
                ],
            )
            .unwrap();
        store.refresh_metadata_command_state_digest().unwrap();
        store
            .conn
            .execute(
                "UPDATE multipart_part_segments SET ec_m = ?1 \
                 WHERE bucket = ?2 AND key = ?3 AND upload_id = ?4",
                params![3_i64, bucket.as_str(), key.as_str(), upload_id.as_str()],
            )
            .unwrap();

        let err = store
            .validate_metadata_command_replay_state(0, ClusterEpoch::INITIAL)
            .unwrap_err();
        assert_metadata_state_digest_mismatch(err);
    }

    #[test]
    fn metadata_state_digest_covers_multipart_upload_and_part_state() {
        assert_metadata_state_digest_covers_mutation(
            |store| {
                let upload_id = UploadId::new("s".repeat(UPLOAD_ID_LEN)).unwrap();
                insert_digest_multipart_upload(store, &upload_id);
            },
            |store| {
                let upload_id = UploadId::new("s".repeat(UPLOAD_ID_LEN)).unwrap();
                store
                    .conn
                    .execute(
                        "UPDATE multipart_uploads SET state = ?1 WHERE upload_id = ?2",
                        params![UploadState::Aborting as u8, upload_id.as_str()],
                    )
                    .unwrap();
            },
        );

        assert_metadata_state_digest_covers_mutation(
            |store| {
                let upload_id = UploadId::new("u".repeat(UPLOAD_ID_LEN)).unwrap();
                insert_digest_multipart_upload(store, &upload_id);
            },
            |store| {
                let upload_id = UploadId::new("u".repeat(UPLOAD_ID_LEN)).unwrap();
                store
                    .conn
                    .execute(
                        "UPDATE multipart_uploads SET metadata_blob = ?1 WHERE upload_id = ?2",
                        params![b"changed".as_slice(), upload_id.as_str()],
                    )
                    .unwrap();
            },
        );

        assert_metadata_state_digest_covers_mutation(
            |store| {
                let upload_id = UploadId::new("p".repeat(UPLOAD_ID_LEN)).unwrap();
                let okh = [4_u8; 16];
                insert_digest_multipart_upload(store, &upload_id);
                store
                    .conn
                    .execute(
                        "INSERT INTO multipart_parts \
                         (upload_id, part_number, generation, size, etag, etag_kind, part_okh, \
                          part_vid, ec_k, ec_m, last_modified, checksum) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                        params![
                            upload_id.as_str(),
                            1_i64,
                            1_i64,
                            64_i64,
                            b"etag".as_slice(),
                            EtagKind::Crc64 as u8,
                            okh.as_slice(),
                            1_i64,
                            4_i64,
                            2_i64,
                            102_i64,
                            Option::<&[u8]>::None,
                        ],
                    )
                    .unwrap();
            },
            |store| {
                let upload_id = UploadId::new("p".repeat(UPLOAD_ID_LEN)).unwrap();
                store
                    .conn
                    .execute(
                        "UPDATE multipart_parts SET checksum = ?1 WHERE upload_id = ?2",
                        params![b"checksum".as_slice(), upload_id.as_str()],
                    )
                    .unwrap();
            },
        );
    }

    #[test]
    fn metadata_state_digest_covers_completed_multipart_uploads() {
        assert_metadata_state_digest_covers_mutation(
            |store| {
                let owner = test_owner();
                let upload_id = UploadId::new("c".repeat(UPLOAD_ID_LEN)).unwrap();
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                store
                    .conn
                    .execute(
                        "INSERT INTO completed_multipart_uploads \
                         (upload_id, bucket, key, completion_order, completed_at, \
                          owner_principal, owner_canonical_id, initiator_principal, initiator_canonical_id) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                        params![
                            upload_id.as_str(),
                            bucket.as_str(),
                            key.as_str(),
                            1_i64,
                            103_i64,
                            owner.principal.as_str(),
                            owner.canonical_id.as_str(),
                            Option::<&str>::None,
                            Option::<&str>::None,
                        ],
                    )
                    .unwrap();
            },
            |store| {
                let upload_id = UploadId::new("c".repeat(UPLOAD_ID_LEN)).unwrap();
                store
                    .conn
                    .execute(
                        "UPDATE completed_multipart_uploads SET completion_order = ?1 WHERE upload_id = ?2",
                        params![2_i64, upload_id.as_str()],
                    )
                    .unwrap();
            },
        );
    }

    #[test]
    fn metadata_state_digest_covers_stream_upload_state() {
        assert_metadata_state_digest_covers_mutation(
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                store
                    .conn
                    .execute(
                        "INSERT INTO stream_uploads \
                         (session_id, bucket, key, op_kind, upload_id, part_number, state, \
                          created_at, encryption_type, encryption_state) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                        params![
                            "session",
                            bucket.as_str(),
                            key.as_str(),
                            StreamUploadKind::PutObject as u8,
                            Option::<&str>::None,
                            Option::<i64>::None,
                            StreamUploadState::InProgress as u8,
                            104_i64,
                            ObjectEncryption::None.encryption_type() as u8,
                            Option::<&[u8]>::None,
                        ],
                    )
                    .unwrap();
            },
            |store| {
                store
                    .conn
                    .execute(
                        "UPDATE stream_uploads SET state = ?1 WHERE session_id = ?2",
                        params![StreamUploadState::Completing as u8, "session"],
                    )
                    .unwrap();
            },
        );

        assert_metadata_state_digest_covers_mutation(
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                let okh = [5_u8; 16];
                store
                    .conn
                    .execute(
                        "INSERT INTO stream_uploads \
                         (session_id, bucket, key, op_kind, upload_id, part_number, state, \
                          created_at, encryption_type, encryption_state) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                        params![
                            "seg-session",
                            bucket.as_str(),
                            key.as_str(),
                            StreamUploadKind::PutObject as u8,
                            Option::<&str>::None,
                            Option::<i64>::None,
                            StreamUploadState::InProgress as u8,
                            105_i64,
                            ObjectEncryption::None.encryption_type() as u8,
                            Option::<&[u8]>::None,
                        ],
                    )
                    .unwrap();
                store
                    .conn
                    .execute(
                        "INSERT INTO stream_upload_segments \
                         (session_id, segment_index, size, segment_crc64, segment_okh, \
                          segment_vid, data_pg_id, ec_k, ec_m) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                        params![
                            "seg-session",
                            0_i64,
                            64_i64,
                            99_i64,
                            okh.as_slice(),
                            1_i64,
                            1_i64,
                            4_i64,
                            2_i64,
                        ],
                    )
                    .unwrap();
            },
            |store| {
                store
                    .conn
                    .execute(
                        "UPDATE stream_upload_segments SET data_pg_id = ?1 WHERE session_id = ?2",
                        params![2_i64, "seg-session"],
                    )
                    .unwrap();
            },
        );
    }

    #[test]
    fn metadata_state_digest_covers_reclaim_and_reservation_state() {
        assert_metadata_state_digest_covers_mutation(
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                store
                    .conn
                    .execute(
                        "INSERT INTO object_generation_reservations \
                         (reservation_id, bucket, key, generation_id, created_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params!["reservation", bucket.as_str(), key.as_str(), 1_i64, 106_i64],
                    )
                    .unwrap();
            },
            |store| {
                store
                    .conn
                    .execute(
                        "UPDATE object_generation_reservations SET created_at = ?1 WHERE reservation_id = ?2",
                        params![107_i64, "reservation"],
                    )
                    .unwrap();
            },
        );

        assert_metadata_state_digest_covers_mutation(
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                let okh = [6_u8; 16];
                store
                    .conn
                    .execute(
                        "INSERT INTO object_segments_reclaims \
                         (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                        params![bucket.as_str(), key.as_str(), 1_i64, 108_i64],
                    )
                    .unwrap();
                store
                    .conn
                    .execute(
                        "INSERT INTO object_segment_reclaim_segments \
                         (bucket, key, generation_id, segment_index, segment_okh, segment_vid, \
                          data_pg_id, ec_k, ec_m) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                        params![
                            bucket.as_str(),
                            key.as_str(),
                            1_i64,
                            0_i64,
                            okh.as_slice(),
                            1_i64,
                            1_i64,
                            4_i64,
                            2_i64,
                        ],
                    )
                    .unwrap();
            },
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                store
                    .conn
                    .execute(
                        "UPDATE object_segment_reclaim_segments SET ec_m = ?1 \
                         WHERE bucket = ?2 AND key = ?3 AND generation_id = ?4",
                        params![3_i64, bucket.as_str(), key.as_str(), 1_i64],
                    )
                    .unwrap();
            },
        );

        assert_metadata_state_digest_covers_mutation(
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                store
                    .conn
                    .execute(
                        "INSERT INTO object_segments_reclaims \
                         (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                        params![bucket.as_str(), key.as_str(), 2_i64, 110_i64],
                    )
                    .unwrap();
            },
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                store
                    .conn
                    .execute(
                        "UPDATE object_segments_reclaims SET created_at = ?1 \
                         WHERE bucket = ?2 AND key = ?3 AND generation_id = ?4",
                        params![111_i64, bucket.as_str(), key.as_str(), 2_i64],
                    )
                    .unwrap();
            },
        );

        assert_metadata_state_digest_covers_mutation(
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                let okh = [7_u8; 16];
                let segment_okh = [8_u8; 16];
                store
                    .conn
                    .execute(
                        "INSERT INTO multipart_reclaims \
                         (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                        params![bucket.as_str(), key.as_str(), 1_i64, 109_i64],
                    )
                    .unwrap();
                store
                    .conn
                    .execute(
                        "INSERT INTO multipart_reclaim_parts \
                         (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                          part_vid, data_pg_id, ec_k, ec_m) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                        params![
                            bucket.as_str(),
                            key.as_str(),
                            1_i64,
                            1_i64,
                            0_i64,
                            okh.as_slice(),
                            1_i64,
                            1_i64,
                            4_i64,
                            2_i64,
                        ],
                    )
                    .unwrap();
                store
                    .conn
                    .execute(
                        "INSERT INTO multipart_reclaim_part_segments \
                         (bucket, key, generation_id, part_number, segment_index, segment_okh, \
                          segment_vid, data_pg_id, ec_k, ec_m) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                        params![
                            bucket.as_str(),
                            key.as_str(),
                            1_i64,
                            1_i64,
                            0_i64,
                            segment_okh.as_slice(),
                            1_i64,
                            1_i64,
                            4_i64,
                            2_i64,
                        ],
                    )
                    .unwrap();
            },
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                store
                    .conn
                    .execute(
                        "UPDATE multipart_reclaim_part_segments SET data_pg_id = ?1 \
                         WHERE bucket = ?2 AND key = ?3 AND generation_id = ?4",
                        params![2_i64, bucket.as_str(), key.as_str(), 1_i64],
                    )
                    .unwrap();
            },
        );

        assert_metadata_state_digest_covers_mutation(
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                let okh = [9_u8; 16];
                store
                    .conn
                    .execute(
                        "INSERT INTO multipart_reclaims \
                         (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                        params![bucket.as_str(), key.as_str(), 2_i64, 112_i64],
                    )
                    .unwrap();
                store
                    .conn
                    .execute(
                        "INSERT INTO multipart_reclaim_parts \
                         (bucket, key, generation_id, part_number, storage_kind, part_okh, \
                          part_vid, data_pg_id, ec_k, ec_m) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                        params![
                            bucket.as_str(),
                            key.as_str(),
                            2_i64,
                            1_i64,
                            0_i64,
                            okh.as_slice(),
                            1_i64,
                            1_i64,
                            4_i64,
                            2_i64,
                        ],
                    )
                    .unwrap();
            },
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                store
                    .conn
                    .execute(
                        "UPDATE multipart_reclaim_parts SET ec_m = ?1 \
                         WHERE bucket = ?2 AND key = ?3 AND generation_id = ?4",
                        params![3_i64, bucket.as_str(), key.as_str(), 2_i64],
                    )
                    .unwrap();
            },
        );

        assert_metadata_state_digest_covers_mutation(
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                store
                    .conn
                    .execute(
                        "INSERT INTO multipart_reclaims \
                         (bucket, key, generation_id, created_at) VALUES (?1, ?2, ?3, ?4)",
                        params![bucket.as_str(), key.as_str(), 3_i64, 113_i64],
                    )
                    .unwrap();
            },
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                store
                    .conn
                    .execute(
                        "UPDATE multipart_reclaims SET created_at = ?1 \
                         WHERE bucket = ?2 AND key = ?3 AND generation_id = ?4",
                        params![114_i64, bucket.as_str(), key.as_str(), 3_i64],
                    )
                    .unwrap();
            },
        );
    }

    #[test]
    fn metadata_state_digest_covers_object_version_counters() {
        assert_metadata_state_digest_covers_mutation(
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                store
                    .conn
                    .execute(
                        "INSERT INTO object_version_counters \
                         (bucket, key, next_version_id) VALUES (?1, ?2, ?3)",
                        params![bucket.as_str(), key.as_str(), 2_i64],
                    )
                    .unwrap();
            },
            |store| {
                let bucket = trusted_bucket_name("digest-bucket");
                let key = trusted_object_key("object");
                store
                    .conn
                    .execute(
                        "UPDATE object_version_counters SET next_version_id = ?1 \
                         WHERE bucket = ?2 AND key = ?3",
                        params![3_i64, bucket.as_str(), key.as_str()],
                    )
                    .unwrap();
            },
        );
    }

    #[test]
    fn metadata_state_digest_covers_pg_counters() {
        assert_metadata_state_digest_covers_mutation(
            |_| {},
            |store| {
                store
                    .conn
                    .execute(
                        "UPDATE pg_counters SET next_bucket_execution_generation = ?1 \
                         WHERE singleton = 0",
                        params![3_i64],
                    )
                    .unwrap();
            },
        );
    }

    #[test]
    fn bucket_execution_generation_candidate_advances_only_on_command_apply() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("bucket");

        let first = store.next_bucket_execution_generation_candidate().unwrap();
        assert_eq!(first, 1);
        assert_eq!(
            store.next_bucket_execution_generation_candidate().unwrap(),
            first
        );

        let command = create_bucket_probe_command(1, 1, bucket, first);
        store.apply_metadata_command(&command).unwrap();

        assert_eq!(
            store.next_bucket_execution_generation_candidate().unwrap(),
            first + 1
        );
    }

    #[test]
    fn reserve_object_version_command_advances_counter_exactly() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("bucket");
        let key = trusted_object_key("object");

        let first = PgMetadataStore::next_version_id(&store, &bucket, &key).unwrap();
        assert_eq!(first, VersionId::from_u64(1));

        let reserve_first = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                bucket.clone(),
                key.clone(),
                first,
            )),
        );
        store.apply_metadata_command(&reserve_first).unwrap();
        assert_eq!(
            PgMetadataStore::next_version_id(&store, &bucket, &key).unwrap(),
            VersionId::from_u64(2)
        );

        let stale = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                bucket.clone(),
                key.clone(),
                first,
            )),
        );
        let err = store.apply_metadata_command(&stale).unwrap_err();
        assert!(
            matches!(
                err,
                MetadataError::Db {
                    context: "reserve object version command stale version",
                    ..
                }
            ),
            "expected stale version reservation rejection, got {err:?}"
        );

        let second = PgMetadataStore::next_version_id(&store, &bucket, &key).unwrap();
        let reserve_second = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                bucket.clone(),
                key.clone(),
                second,
            )),
        );
        store.apply_metadata_command(&reserve_second).unwrap();
        assert_eq!(
            PgMetadataStore::next_version_id(&store, &bucket, &key).unwrap(),
            VersionId::from_u64(3)
        );

        let null_reservation = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(4).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                bucket,
                key,
                VersionId::Null,
            )),
        );
        let err = store.apply_metadata_command(&null_reservation).unwrap_err();
        assert!(
            matches!(
                err,
                MetadataError::Db {
                    context: "reserve object version command null version",
                    ..
                }
            ),
            "expected null version reservation rejection, got {err:?}"
        );
    }

    #[test]
    fn reserve_object_version_command_advances_lower_counter_forward() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("bucket");
        let key = trusted_object_key("object");

        let forward = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::ReserveObjectVersion(ReserveObjectVersionCommand::new(
                bucket.clone(),
                key.clone(),
                VersionId::from_u64(3),
            )),
        );
        store.apply_metadata_command(&forward).unwrap();
        assert_eq!(
            PgMetadataStore::next_version_id(&store, &bucket, &key).unwrap(),
            VersionId::from_u64(4)
        );
    }

    #[test]
    fn put_bucket_versioning_command_does_not_lower_execution_generation() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("bucket");
        let owner = test_owner();
        let acl_grants = AclGrants::default();
        store
            .create_bucket_with_config(&CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: &owner.principal,
                owner_canonical_id: &owner.canonical_id,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: BucketVersioningState::Disabled,
                object_lock: BucketObjectLockConfig::default(),
            })
            .unwrap();

        let newer = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
                store
                    .head_bucket_record_raw(&bucket)
                    .unwrap()
                    .with_execution_generation(12),
                BucketVersioningState::Enabled,
            )),
        );
        store.apply_metadata_command(&newer).unwrap();

        let stale = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
                store
                    .head_bucket_record_raw(&bucket)
                    .unwrap()
                    .with_execution_generation(11),
                BucketVersioningState::Enabled,
            )),
        );
        let err = store.apply_metadata_command(&stale).unwrap_err();
        assert!(
            matches!(
                err,
                MetadataError::Db {
                    context: "apply stale bucket versioning command",
                    ..
                }
            ),
            "expected stale command rejection, got {err:?}"
        );

        let conflicting = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            MetadataCommandPayload::PutBucketVersioning(PutBucketVersioningCommand::from_bucket(
                store
                    .head_bucket_record_raw(&bucket)
                    .unwrap()
                    .with_execution_generation(12),
                BucketVersioningState::Suspended,
            )),
        );
        let err = store.apply_metadata_command(&conflicting).unwrap_err();
        assert!(
            matches!(
                err,
                MetadataError::Db {
                    context: "apply conflicting bucket versioning command",
                    ..
                }
            ),
            "expected conflicting command rejection, got {err:?}"
        );

        let info = store.head_bucket_raw(&bucket).unwrap();
        assert_eq!(info.versioning, BucketVersioningState::Enabled);
        assert_eq!(info.bucket_execution_generation, 12);
    }

    #[test]
    fn put_bucket_acl_command_does_not_lower_execution_generation() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("bucket");
        let owner = test_owner();
        let acl_grants = AclGrants::default();
        store
            .create_bucket_with_config(&CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: &owner.principal,
                owner_canonical_id: &owner.canonical_id,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: BucketVersioningState::Disabled,
                object_lock: BucketObjectLockConfig::default(),
            })
            .unwrap();

        let newer = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                store
                    .head_bucket_record_raw(&bucket)
                    .unwrap()
                    .with_execution_generation(12),
                acl_grants.clone(),
                true,
                false,
            )),
        );
        store.apply_metadata_command(&newer).unwrap();

        let stale = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                store
                    .head_bucket_record_raw(&bucket)
                    .unwrap()
                    .with_execution_generation(11),
                acl_grants.clone(),
                true,
                false,
            )),
        );
        let err = store.apply_metadata_command(&stale).unwrap_err();
        assert!(
            matches!(
                err,
                MetadataError::Db {
                    context: "apply stale bucket acl command",
                    ..
                }
            ),
            "expected stale command rejection, got {err:?}"
        );

        let conflicting = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            MetadataCommandPayload::PutBucketAcl(PutBucketAclCommand::from_bucket(
                store
                    .head_bucket_record_raw(&bucket)
                    .unwrap()
                    .with_execution_generation(12),
                acl_grants.clone(),
                false,
                true,
            )),
        );
        let err = store.apply_metadata_command(&conflicting).unwrap_err();
        assert!(
            matches!(
                err,
                MetadataError::Db {
                    context: "apply conflicting bucket acl command",
                    ..
                }
            ),
            "expected conflicting command rejection, got {err:?}"
        );

        let info = store.head_bucket_raw(&bucket).unwrap();
        assert_eq!(info.acl_grants, acl_grants);
        assert!(info.public_read);
        assert!(!info.public_write);
        assert_eq!(info.bucket_execution_generation, 12);
    }

    #[test]
    fn put_bucket_property_command_does_not_lower_execution_generation() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("bucket");
        let owner = test_owner();
        let acl_grants = AclGrants::default();
        store
            .create_bucket_with_config(&CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: &owner.principal,
                owner_canonical_id: &owner.canonical_id,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: BucketVersioningState::Disabled,
                object_lock: BucketObjectLockConfig::default(),
            })
            .unwrap();

        let newer_config = BucketEncryptionConfig {
            default_encryption: Some(ManagedEncryptionAlgorithm::Aes256),
            sse_c_blocked: true,
        };
        let newer = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::PutBucketProperty(
                PutBucketPropertyCommand::from_bucket_and_mutation(
                    store
                        .head_bucket_record_raw(&bucket)
                        .unwrap()
                        .with_execution_generation(12),
                    BucketPropertyMutation::Encryption(newer_config),
                ),
            ),
        );
        store.apply_metadata_command(&newer).unwrap();

        let stale = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::PutBucketProperty(
                PutBucketPropertyCommand::from_bucket_and_mutation(
                    store
                        .head_bucket_record_raw(&bucket)
                        .unwrap()
                        .with_execution_generation(11),
                    BucketPropertyMutation::Encryption(newer_config),
                ),
            ),
        );
        let err = store.apply_metadata_command(&stale).unwrap_err();
        assert!(
            matches!(
                err,
                MetadataError::Db {
                    context: "apply stale bucket encryption command",
                    ..
                }
            ),
            "expected stale command rejection, got {err:?}"
        );

        let same_effective_but_different_stored_config = BucketEncryptionConfig {
            default_encryption: None,
            sse_c_blocked: true,
        };
        assert_eq!(
            newer_config.effective(),
            same_effective_but_different_stored_config.effective()
        );
        let conflicting = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            MetadataCommandPayload::PutBucketProperty(
                PutBucketPropertyCommand::from_bucket_and_mutation(
                    store
                        .head_bucket_record_raw(&bucket)
                        .unwrap()
                        .with_execution_generation(12),
                    BucketPropertyMutation::Encryption(same_effective_but_different_stored_config),
                ),
            ),
        );
        let err = store.apply_metadata_command(&conflicting).unwrap_err();
        assert!(
            matches!(
                err,
                MetadataError::Db {
                    context: "apply conflicting bucket encryption command",
                    ..
                }
            ),
            "expected conflicting command rejection, got {err:?}"
        );

        assert_eq!(
            PgMetadataStore::get_bucket_encryption(&store, &bucket).unwrap(),
            newer_config
        );
        let info = store.head_bucket_raw(&bucket).unwrap();
        assert_eq!(info.encryption, newer_config.effective());
        assert_eq!(info.bucket_execution_generation, 12);
    }

    #[test]
    fn put_bucket_subresource_command_does_not_lower_execution_generation() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 1).unwrap();
        let bucket = trusted_bucket_name("bucket");
        let owner = test_owner();
        let acl_grants = AclGrants::default();
        store
            .create_bucket_with_config(&CreateBucketConfig {
                name: bucket.as_str(),
                owner_principal: &owner.principal,
                owner_canonical_id: &owner.canonical_id,
                acl_grants: &acl_grants,
                public_read: false,
                public_write: false,
                versioning: BucketVersioningState::Disabled,
                object_lock: BucketObjectLockConfig::default(),
            })
            .unwrap();

        let policy_body = r#"{"Statement":[]}"#.to_owned();
        let mutation = BucketSubresourceMutation::Put {
            kind: BucketSubresourceKind::Policy,
            body: policy_body.clone(),
            aux: BucketSubresourceAux::policy(true),
        };
        let newer = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(2).unwrap(),
            ),
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                bucket.clone(),
                mutation.clone(),
                12,
            )),
        );
        store.apply_metadata_command(&newer).unwrap();

        let stale = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                bucket.clone(),
                mutation.clone(),
                11,
            )),
        );
        let err = store.apply_metadata_command(&stale).unwrap_err();
        assert!(
            matches!(
                err,
                MetadataError::Db {
                    context: "apply stale put bucket subresource command",
                    ..
                }
            ),
            "expected stale command rejection, got {err:?}"
        );

        let conflicting = MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                ClusterEpoch::INITIAL,
                PgId::new(1),
                MetadataCommandLogIndex::new(3).unwrap(),
            ),
            MetadataCommandPayload::PutBucketSubresource(PutBucketSubresourceCommand::new(
                bucket.clone(),
                BucketSubresourceMutation::Put {
                    kind: BucketSubresourceKind::Policy,
                    body: r#"{"Statement":[{"Effect":"Deny"}]}"#.to_owned(),
                    aux: BucketSubresourceAux::policy(false),
                },
                12,
            )),
        );
        let err = store.apply_metadata_command(&conflicting).unwrap_err();
        assert!(
            matches!(
                err,
                MetadataError::Db {
                    context: "apply conflicting put bucket subresource command",
                    ..
                }
            ),
            "expected conflicting command rejection, got {err:?}"
        );

        let stored =
            PgMetadataStore::get_bucket_subresource(&store, &bucket, BucketSubresourceKind::Policy)
                .unwrap()
                .unwrap();
        assert_eq!(stored.body, policy_body);
        assert_eq!(stored.generation, Some(1));
        assert_eq!(stored.aux, BucketSubresourceAux::policy(true));
        let info = store.head_bucket_raw(&bucket).unwrap();
        assert!(info.bucket_policy_present);
        assert!(info.bucket_policy_public);
        assert_eq!(info.bucket_policy_generation, 1);
        assert_eq!(info.bucket_execution_generation, 12);
    }

    // ── list_objects with prefix + start_after ────────────────────────

    #[test]
    fn list_objects_prefix_and_start_after() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();

        // Insert several objects
        for key in &["photos/a.jpg", "photos/b.jpg", "photos/c.jpg", "docs/x"] {
            store
                .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                    bucket: trusted_bucket_name("bucket"),
                    key: trusted_object_key(*key),
                    version_id: VersionId::Null,
                    owner: test_owner(),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::MIN,
                    size: 10,
                    etag: ObjectEtag::SinglePart([0; 8]),
                    ec: EcShape { k: 4, m: 2 },
                    layout: ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: ObjectLockState::default(),
                    encryption: ObjectEncryption::None,
                }))
                .unwrap();
        }

        // List with prefix=photos/ and start_after=photos/a.jpg
        let resp = store
            .list_objects(&ListObjectsReq {
                bucket: trusted_bucket_name("bucket"),
                prefix: Some(trusted_object_key("photos/")),
                start_after: Some(trusted_object_key("photos/a.jpg")),
                start_at: None,
                max_keys: 10,
            })
            .unwrap();

        assert_eq!(resp.objects.len(), 2);
        assert_eq!(resp.objects[0].key(), "photos/b.jpg");
        assert_eq!(resp.objects[1].key(), "photos/c.jpg");
        assert!(!resp.is_truncated);
    }

    // ── stat_shard on quarantined shard ───────────────────────────────

    #[test]
    fn stat_shard_after_quarantine() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();

        let key = ShardKey::new(&[0xAA; 16], 0, 0);
        store.write_shard(&key, b"hello").unwrap();

        // Corrupt the shard file on disk
        let shard_path = store.shard_path(&key);
        fs::write(&shard_path, b"corrupt data!!").unwrap();

        // Read should fail with integrity error and quarantine the shard
        let err = store.read_shard(&key).unwrap_err();
        assert!(matches!(err, StoreError::IntegrityError { .. }));

        // stat_shard should now return NotFound (quarantined)
        let err = store.stat_shard(&key).unwrap_err();
        assert!(matches!(err, StoreError::NotFound));
    }

    // ── stat_shard on live shard ──────────────────────────────────────

    #[test]
    fn stat_shard_live() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();

        let key = ShardKey::new(&[0xBB; 16], 1, 0);
        store.write_shard(&key, b"data").unwrap();

        let stat = store.stat_shard(&key).unwrap();
        assert_eq!(stat.size, 4);
        assert_eq!(stat.crc64, checksum::crc64::checksum(b"data"));
    }

    // ── connection accessor ───────────────────────────────────────────

    #[test]
    fn connection_accessor() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();
        // Just verify we can call it without panicking
        let _conn = store.connection();
    }

    #[test]
    fn row_to_object_record_rejects_negative_parts_count() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();
        let err = store
            .connection()
            .query_row(
                "SELECT \
                    'bucket' AS bucket, \
                    'k' AS key, \
                    0 AS version_id, \
                    1 AS generation_id, \
                    1 AS size, \
                    zeroblob(8) AS etag, \
                    1 AS etag_kind, \
                    0 AS last_modified, \
                    0 AS storage_class, \
                    4 AS ec_k, \
                    2 AS ec_m, \
                    0 AS status, \
                    NULL AS tags, \
                    1 AS data_layout, \
                    -1 AS parts_count, \
                    NULL AS metadata_blob, \
                    NULL AS system_metadata_blob, \
                    0 AS encryption_type, \
                    NULL AS encryption_state, \
                    'owner' AS owner_principal, \
                    ?1 AS owner_canonical_id, \
                    '' AS acl_grants, \
                    0 AS public_read, \
                    NULL AS object_lock_retention_mode, \
                    NULL AS object_lock_retain_until, \
                    0 AS object_lock_legal_hold, \
                    NULL AS became_noncurrent_at",
                params![CanonicalUserId::from_principal("owner").as_str()],
                PgStore::row_to_object_record,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            rusqlite::Error::FromSqlConversionFailure(14, rusqlite::types::Type::Integer, _)
        ));
    }

    #[test]
    fn row_to_object_record_rejects_pending_delete_status() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();
        let err = store
            .connection()
            .query_row(
                "SELECT \
                    'b' AS bucket, \
                    'k' AS key, \
                    0 AS version_id, \
                    1 AS generation_id, \
                    1 AS size, \
                    zeroblob(8) AS etag, \
                    1 AS etag_kind, \
                    0 AS last_modified, \
                    0 AS storage_class, \
                    4 AS ec_k, \
                    2 AS ec_m, \
                    2 AS status, \
                    NULL AS tags, \
                    0 AS data_layout, \
                    NULL AS parts_count, \
                    NULL AS metadata_blob, \
                    NULL AS system_metadata_blob, \
                    0 AS encryption_type, \
                    NULL AS encryption_state, \
                    'owner' AS owner_principal, \
                    ?1 AS owner_canonical_id, \
                    '' AS acl_grants, \
                    0 AS public_read, \
                    NULL AS object_lock_retention_mode, \
                    NULL AS object_lock_retain_until, \
                    0 AS object_lock_legal_hold, \
                    NULL AS became_noncurrent_at",
                params![CanonicalUserId::from_principal("owner").as_str()],
                PgStore::row_to_object_record,
            )
            .unwrap_err();
        assert!(matches!(
            err,
            rusqlite::Error::FromSqlConversionFailure(11, rusqlite::types::Type::Integer, _)
        ));
    }

    // ── list_objects pagination ──────────────────────────────────────

    #[test]
    fn list_objects_pagination() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();

        for i in 0..5 {
            store
                .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                    bucket: trusted_bucket_name("bucket"),
                    key: trusted_object_key(format!("key-{:02}", i)),
                    version_id: VersionId::Null,
                    owner: test_owner(),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::MIN,
                    size: 0,
                    etag: ObjectEtag::SinglePart([0; 8]),
                    ec: EcShape { k: 4, m: 2 },
                    layout: ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: ObjectLockState::default(),
                    encryption: ObjectEncryption::None,
                }))
                .unwrap();
        }

        // First page
        let resp = store
            .list_objects(&ListObjectsReq {
                bucket: trusted_bucket_name("bucket"),
                prefix: None,
                start_after: None,
                start_at: None,
                max_keys: 2,
            })
            .unwrap();
        assert_eq!(resp.objects.len(), 2);
        assert!(resp.is_truncated);
        assert_eq!(resp.objects[0].key(), "key-00");
        assert_eq!(resp.objects[1].key(), "key-01");

        // Second page
        let resp2 = store
            .list_objects(&ListObjectsReq {
                bucket: trusted_bucket_name("bucket"),
                prefix: None,
                start_after: resp.next_start_after,
                start_at: None,
                max_keys: 2,
            })
            .unwrap();
        assert_eq!(resp2.objects.len(), 2);
        assert!(resp2.is_truncated);
        assert_eq!(resp2.objects[0].key(), "key-02");
    }

    #[test]
    fn list_objects_start_at_is_inclusive() {
        let tmp = test_util::tempdir();
        let store = PgStore::open(tmp.path(), 0).unwrap();

        for key in ["alpha", "beta", "gamma"] {
            store
                .put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
                    bucket: trusted_bucket_name("bucket"),
                    key: trusted_object_key(key),
                    version_id: VersionId::Null,
                    owner: test_owner(),
                    acl_grants: AclGrants::default(),
                    public_read: false,
                    generation_id: GenerationId::MIN,
                    size: 0,
                    etag: ObjectEtag::SinglePart([0; 8]),
                    ec: EcShape { k: 4, m: 2 },
                    layout: ObjectLayout::Standard,
                    tags: None,
                    metadata_blob: None,
                    system_metadata_blob: None,
                    object_lock: ObjectLockState::default(),
                    encryption: ObjectEncryption::None,
                }))
                .unwrap();
        }

        let resp = store
            .list_objects(&ListObjectsReq {
                bucket: trusted_bucket_name("bucket"),
                prefix: None,
                start_after: None,
                start_at: Some(trusted_object_key("beta")),
                max_keys: 10,
            })
            .unwrap();

        assert_eq!(resp.objects.len(), 2);
        assert_eq!(resp.objects[0].key(), "beta");
        assert_eq!(resp.objects[1].key(), "gamma");
    }
}
