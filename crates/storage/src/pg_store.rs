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
use std::collections::{HashMap, HashSet};
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
    BucketPropertyEffect, BucketRecord, BucketSubresourceMutation, BucketWriteReservationProof,
    CommitDirectPutObjectCommand, CommitMultipartObjectCommand, CommitStreamPartCommand,
    CreateBucketCommand, CreateMultipartUploadCommand, CreateStreamUploadCommand,
    DeleteCompletedMultipartUploadCommand, DeleteObjectPayloadReclaimCommand,
    DeleteObjectVersionCommand, DeleteObjectVersionTarget, InsertDeleteMarkerCommand,
    MarkBucketDeletingCommand, MetadataCommandAcceptance, MetadataCommandEnvelope,
    MetadataCommandId, MetadataCommandLogEntryKind, MetadataCommandLogIndex,
    MetadataCommandPayload, MetadataCommandReplicaState, ObjectPayloadReclaimCommand,
    PutBucketAclCommand, PutBucketPropertyCommand, PutBucketSubresourceCommand,
    PutBucketVersioningCommand, PutObjectMetadataCommand, ReleaseObjectGenerationCommand,
    ReserveObjectGenerationCommand, ReserveObjectVersionCommand,
};
use crate::schema::init_pg_schema;
use crate::traits::{DurableBucketWriteReservationHeartbeat, PgMetadataStore, ShardStore};
use crate::types::*;

const TRACE_TARGET: &str = "storage";

mod metadata;
mod shards;

const LIFECYCLE_SUBRESOURCE_KIND_SQL: i64 = BucketSubresourceKind::Lifecycle as u8 as i64;
const BUCKET_INFO_SELECT: &str = "\
SELECT name, owner_principal, owner_canonical_id, created_at, region, state, versioning, acl_grants, public_read, public_write, \
       public_access_block_present, public_access_block_block_public_acls, public_access_block_ignore_public_acls, public_access_block_block_public_policy, public_access_block_restrict_public_buckets, ownership_controls_mode, \
       EXISTS(SELECT 1 FROM bucket_subresources WHERE bucket_name = buckets.name AND kind = 4 AND body IS NOT NULL) AS bucket_policy_present, \
       bucket_policy_public, bucket_policy_generation, \
       EXISTS(SELECT 1 FROM bucket_subresources WHERE bucket_name = buckets.name AND kind = 5 AND body IS NOT NULL) AS bucket_lifecycle_present, \
       bucket_lifecycle_generation, bucket_execution_generation, bucket_incarnation_generation, bucket_abac_enabled, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years \
FROM buckets";
const BUCKET_INFO_BY_NAME_SELECT: &str = "\
SELECT name, owner_principal, owner_canonical_id, created_at, region, state, versioning, acl_grants, public_read, public_write, \
       public_access_block_present, public_access_block_block_public_acls, public_access_block_ignore_public_acls, public_access_block_block_public_policy, public_access_block_restrict_public_buckets, ownership_controls_mode, \
       EXISTS(SELECT 1 FROM bucket_subresources WHERE bucket_name = buckets.name AND kind = 4 AND body IS NOT NULL) AS bucket_policy_present, \
       bucket_policy_public, bucket_policy_generation, \
       EXISTS(SELECT 1 FROM bucket_subresources WHERE bucket_name = buckets.name AND kind = 5 AND body IS NOT NULL) AS bucket_lifecycle_present, \
       bucket_lifecycle_generation, bucket_execution_generation, bucket_incarnation_generation, bucket_abac_enabled, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years \
FROM buckets WHERE name = ?1";

const BUCKET_RECORD_BY_NAME_SELECT: &str = "\
SELECT name, owner_principal, owner_canonical_id, created_at, region, state, versioning, acl_grants, public_read, public_write, \
       public_access_block_present, public_access_block_block_public_acls, public_access_block_ignore_public_acls, public_access_block_block_public_policy, public_access_block_restrict_public_buckets, ownership_controls_mode, \
       bucket_policy_public, bucket_policy_generation, bucket_lifecycle_generation, bucket_execution_generation, bucket_incarnation_generation, completed_multipart_upload_sequence, bucket_abac_enabled, default_encryption_type, sse_c_blocked, \
       object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years \
FROM buckets WHERE name = ?1";

const STREAM_UPLOAD_SELECT: &str = "\
SELECT session_id, bucket, key, op_kind, upload_id, part_number, state, created_at, encryption_type, encryption_state, next_segment_vid, \
       bucket_write_reservation_id, bucket_write_owner_token, bucket_write_cluster_epoch, bucket_write_execution_generation, \
       bucket_write_incarnation_generation, bucket_write_operation_kind, bucket_write_created_at, bucket_write_lease_deadline, bucket_write_target_context \
FROM stream_uploads";

fn parse_stream_upload_record(row: &Row<'_>) -> rusqlite::Result<StreamUploadRecord> {
    let op_kind_raw: u8 = row.get(3)?;
    let state_raw: u8 = row.get(6)?;
    let upload_id: Option<UploadId> = row.get(4)?;
    let part_number: Option<i64> = row.get(5)?;
    let bucket: BucketName = row.get(1)?;
    Ok(StreamUploadRecord {
        session_id: row.get(0)?,
        bucket: bucket.clone(),
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
        encryption: PgStore::parse_object_encryption(
            row.get::<_, u8>(8)?,
            row.get::<_, Option<Vec<u8>>>(9)?,
            8,
            9,
        )?,
        next_segment_vid: PgStore::parse_generation_id(
            row.get::<_, i64>(10)?,
            10,
            "next_segment_vid",
        )?,
        bucket_write_reservation: parse_stream_upload_bucket_write_reservation(bucket, row)?,
    })
}

fn parse_stream_upload_bucket_write_reservation(
    bucket: BucketName,
    row: &Row<'_>,
) -> rusqlite::Result<Option<BucketWriteReservationProof>> {
    let Some(reservation_id) = row.get::<_, Option<String>>(11)? else {
        return Ok(None);
    };
    let cluster_epoch_raw: i64 = row.get(13)?;
    let cluster_epoch = ClusterEpoch::new(u64::try_from(cluster_epoch_raw).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            13,
            rusqlite::types::Type::Integer,
            Box::from("invalid stream bucket write reservation cluster epoch"),
        )
    })?)
    .ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            13,
            rusqlite::types::Type::Integer,
            Box::from("invalid stream bucket write reservation cluster epoch"),
        )
    })?;
    Ok(Some(BucketWriteReservationProof {
        bucket,
        reservation_id,
        owner_token: row.get(12)?,
        cluster_epoch,
        bucket_execution_generation: row.get::<_, i64>(14)? as u64,
        bucket_incarnation_generation: row.get::<_, i64>(15)? as u64,
        operation_kind: row.get(16)?,
        created_at: row.get::<_, i64>(17)? as u64,
        lease_deadline: row.get::<_, Option<i64>>(18)?.map(|value| value as u64),
        target_context: row.get(19)?,
    }))
}

fn stream_upload_bucket_write_reservation_matches_command(
    existing: &StreamUploadRecord,
    command: &CreateStreamUploadCommand,
) -> bool {
    match command.session.target {
        StreamUploadTarget::PutObject => {
            existing.bucket_write_reservation.as_ref() == Some(&command.bucket_write_reservation)
        }
        StreamUploadTarget::UploadPart { .. } => existing.bucket_write_reservation.is_none(),
    }
}

/// Part segment rows use a sentinel version_id during staging (pre-CompleteMultipartUpload).
/// Must differ from any real version_id (0 for unversioned, 1+ for versioned) so that
/// in-progress staging rows are invisible to reads of completed objects.
const PART_SEGMENT_STAGING_VERSION_ID: VersionId = MULTIPART_PART_SEGMENT_STAGING_VERSION_ID;

fn shard_scavenger_observation_reason_name(
    reason: ShardScavengerObservationReason,
) -> &'static str {
    match reason {
        ShardScavengerObservationReason::FileWithoutShardRow => "file_without_shard_row",
        ShardScavengerObservationReason::ShardRowWithoutFile => "shard_row_without_file",
        ShardScavengerObservationReason::UnreferencedShardRowAndFile => {
            "unreferenced_shard_row_and_file"
        }
        ShardScavengerObservationReason::ScanIncomplete => "scan_incomplete",
    }
}

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
            "bucket_incarnation_generation",
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
static SHARD_TMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingMetadataCommandSlotCleanup {
    CleanTerminal,
    PreserveTerminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectGenerationReservationConstraint {
    ReservationId,
    Generation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectGenerationReservationIdentity {
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
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

#[derive(Debug, Clone)]
pub(crate) struct ScavengerShardRow {
    pub(crate) key: ShardKey,
    pub(crate) ack: WriteAck,
}

#[derive(Debug, Clone)]
pub(crate) struct ScavengerShardFile {
    pub(crate) key: ShardKey,
    pub(crate) size: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct ScavengerShardFileScan {
    pub(crate) files: Vec<ScavengerShardFile>,
    pub(crate) errors: Vec<String>,
}

fn is_canonical_shard_prefix(prefix: &str) -> bool {
    prefix.len() == SHARD_KEY_HEX_PREFIX_LEN
        && prefix
            .as_bytes()
            .iter()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
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

    /// Record a non-authoritative shard scavenger audit observation.
    pub fn record_shard_scavenger_observation(
        &self,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError> {
        self.validate_shard_scavenger_observation_record(observation)?;
        let now = Self::now_secs();
        self.conn
            .execute(
                "INSERT INTO shard_scavenger_observations \
                 (node_id, data_pg_id, shard_index, shard_key, first_seen_at, last_seen_at, \
                  observation_count, data_size, crc64_nvme, file_exists, shard_row_exists, \
                  reason, last_error, resolved_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?5, 1, ?6, ?7, ?8, ?9, ?10, ?11, NULL) \
                 ON CONFLICT(node_id, data_pg_id, shard_index, shard_key) DO UPDATE SET \
                  last_seen_at = excluded.last_seen_at, \
                  observation_count = shard_scavenger_observations.observation_count + 1, \
                  data_size = excluded.data_size, \
                  crc64_nvme = excluded.crc64_nvme, \
                  file_exists = excluded.file_exists, \
                  shard_row_exists = excluded.shard_row_exists, \
                  reason = excluded.reason, \
                  last_error = excluded.last_error, \
                  resolved_at = NULL",
                params![
                    observation.key.node_id as i64,
                    observation.key.data_pg_id as i64,
                    observation.key.shard_index.get() as i64,
                    observation.key.shard_key.as_bytes().as_slice(),
                    now as i64,
                    observation.data_size.map(|size| size as i64),
                    observation.crc64.map(|crc| crc as i64),
                    if observation.file_exists {
                        1_i64
                    } else {
                        0_i64
                    },
                    if observation.shard_row_exists {
                        1_i64
                    } else {
                        0_i64
                    },
                    observation.reason as u8 as i64,
                    observation.last_error.as_deref(),
                ],
            )
            .map_err(|source| StoreError::Db {
                context: "record shard scavenger observation",
                source,
            })?;
        let _ = observability::emit_shard_scavenger_observation(
            TRACE_TARGET,
            observability::ShardScavengerObservationSummary {
                node_id: observation.key.node_id,
                data_pg_id: observation.key.data_pg_id,
                shard_index: observation.key.shard_index.get(),
                shard_key_hex: &observation.key.shard_key.hex(),
                reason: shard_scavenger_observation_reason_name(observation.reason),
                file_exists: observation.file_exists,
                shard_row_exists: observation.shard_row_exists,
                last_error: observation.last_error.as_deref(),
            },
        );
        Ok(())
    }

    /// Mark a shard scavenger audit observation as resolved.
    pub fn resolve_shard_scavenger_observation(
        &self,
        key: &ShardScavengerObservationKey,
    ) -> Result<bool, StoreError> {
        self.validate_shard_scavenger_observation_key(key)?;
        let now = Self::now_secs();
        let updated = self
            .conn
            .execute(
                "UPDATE shard_scavenger_observations \
                 SET resolved_at = ?1 \
                 WHERE node_id = ?2 AND data_pg_id = ?3 AND shard_index = ?4 AND shard_key = ?5",
                params![
                    now as i64,
                    key.node_id as i64,
                    key.data_pg_id as i64,
                    key.shard_index.get() as i64,
                    key.shard_key.as_bytes().as_slice(),
                ],
            )
            .map_err(|source| StoreError::Db {
                context: "resolve shard scavenger observation",
                source,
            })?;
        Ok(updated > 0)
    }

    pub fn list_shard_scavenger_observations(
        &self,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT node_id, data_pg_id, shard_index, shard_key, first_seen_at, last_seen_at, \
                        observation_count, data_size, crc64_nvme, file_exists, shard_row_exists, \
                        reason, last_error, resolved_at \
                 FROM shard_scavenger_observations \
                 ORDER BY node_id, data_pg_id, shard_index, shard_key",
            )
            .map_err(|source| StoreError::Db {
                context: "list shard scavenger observations (prepare)",
                source,
            })?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, i64>(9)?,
                    row.get::<_, i64>(10)?,
                    row.get::<_, i64>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, Option<i64>>(13)?,
                ))
            })
            .map_err(|source| StoreError::Db {
                context: "list shard scavenger observations",
                source,
            })?;

        let mut observations = Vec::new();
        for row in rows {
            let (
                node_id,
                data_pg_id,
                shard_index,
                shard_key,
                first_seen_at,
                last_seen_at,
                observation_count,
                data_size,
                crc64,
                file_exists,
                shard_row_exists,
                reason,
                last_error,
                resolved_at,
            ) = row.map_err(|source| StoreError::Db {
                context: "read shard scavenger observation",
                source,
            })?;
            observations.push(ShardScavengerObservation {
                key: ShardScavengerObservationKey {
                    node_id: node_id as u32,
                    data_pg_id: data_pg_id as u32,
                    shard_index: ShardIndex::new(shard_index as u8),
                    shard_key: ShardKey::from_bytes(&shard_key)?,
                },
                first_seen_at: first_seen_at as u64,
                last_seen_at: last_seen_at as u64,
                observation_count: observation_count as u64,
                data_size: data_size.map(|size| size as u64),
                crc64: crc64.map(|crc| crc as u64),
                file_exists: file_exists != 0,
                shard_row_exists: shard_row_exists != 0,
                reason: ShardScavengerObservationReason::from_u8(reason as u8)
                    .expect("schema restricts shard scavenger observation reasons"),
                last_error,
                resolved_at: resolved_at.map(|value| value as u64),
            });
        }
        Ok(observations)
    }

    /// Audit local shard file/index mismatches for this data PG.
    ///
    /// This is intentionally non-destructive. It records only local file/row
    /// inconsistencies and resolves prior observations of those classes once
    /// the mismatch disappears.
    pub fn audit_local_shard_storage_for_scavenger(
        &self,
        node_id: u32,
    ) -> Result<Vec<ShardScavengerObservation>, StoreError> {
        let shard_rows = self.list_scavenger_shard_rows()?;
        let shard_file_scan = self.list_scavenger_shard_files()?;
        let shard_files = shard_file_scan.files;
        let row_keys: HashSet<ShardKey> = shard_rows.iter().map(|row| row.key.clone()).collect();
        let file_keys: HashSet<ShardKey> =
            shard_files.iter().map(|file| file.key.clone()).collect();
        let mut active_mismatch_keys = HashSet::new();

        for row in &shard_rows {
            let observation_key = ShardScavengerObservationKey {
                node_id,
                data_pg_id: self.pg_id,
                shard_index: row.key.shard_index(),
                shard_key: row.key.clone(),
            };
            if !file_keys.contains(&row.key) {
                active_mismatch_keys.insert(observation_key.clone());
                self.record_shard_scavenger_observation(&ShardScavengerObservationRecord {
                    key: observation_key,
                    data_size: Some(row.ack.stored_size),
                    crc64: Some(row.ack.crc64),
                    file_exists: false,
                    shard_row_exists: true,
                    reason: ShardScavengerObservationReason::ShardRowWithoutFile,
                    last_error: None,
                })?;
            }
        }

        for file in &shard_files {
            let observation_key = ShardScavengerObservationKey {
                node_id,
                data_pg_id: self.pg_id,
                shard_index: file.key.shard_index(),
                shard_key: file.key.clone(),
            };
            if !row_keys.contains(&file.key) {
                active_mismatch_keys.insert(observation_key.clone());
                self.record_shard_scavenger_observation(&ShardScavengerObservationRecord {
                    key: observation_key,
                    data_size: Some(file.size),
                    crc64: None,
                    file_exists: true,
                    shard_row_exists: false,
                    reason: ShardScavengerObservationReason::FileWithoutShardRow,
                    last_error: None,
                })?;
            }
        }

        if shard_file_scan.errors.is_empty() {
            for observation in self.list_shard_scavenger_observations()? {
                if observation.key.node_id != node_id || observation.key.data_pg_id != self.pg_id {
                    continue;
                }
                if observation.resolved_at.is_some()
                    || active_mismatch_keys.contains(&observation.key)
                {
                    continue;
                }
                if matches!(
                    observation.reason,
                    ShardScavengerObservationReason::FileWithoutShardRow
                        | ShardScavengerObservationReason::ShardRowWithoutFile
                ) {
                    self.resolve_shard_scavenger_observation(&observation.key)?;
                }
            }
        }

        if !shard_file_scan.errors.is_empty() {
            return Err(StoreError::ShardScavengerScanIncomplete {
                context: "local shard file scan",
                errors: shard_file_scan.errors.join("; "),
            });
        }

        self.list_shard_scavenger_observations()
    }

    pub(crate) fn list_shard_scavenger_payload_references(
        &self,
    ) -> Result<Vec<ShardScavengerPayloadReference>, StoreError> {
        let mut references = Vec::new();
        self.extend_scavenger_placed_references(
            &mut references,
            "SELECT data_pg_id, segment_okh, segment_vid, ec_k, ec_m FROM object_segments",
            "list object segment shard scavenger references",
        )?;
        self.extend_scavenger_placed_references(
            &mut references,
            "SELECT data_pg_id, part_okh, part_vid, ec_k, ec_m \
             FROM object_parts WHERE part_okh != zeroblob(16)",
            "list object part shard scavenger references",
        )?;
        self.extend_scavenger_placed_references(
            &mut references,
            "SELECT data_pg_id, segment_okh, segment_vid, ec_k, ec_m FROM stream_upload_segments",
            "list stream upload segment shard scavenger references",
        )?;
        self.extend_scavenger_placed_references(
            &mut references,
            "SELECT data_pg_id, segment_okh, segment_vid, ec_k, ec_m FROM multipart_part_segments",
            "list multipart part segment shard scavenger references",
        )?;
        self.extend_scavenger_placed_references(
            &mut references,
            "SELECT data_pg_id, segment_okh, segment_vid, ec_k, ec_m \
             FROM object_segment_reclaim_segments",
            "list object segment reclaim shard scavenger references",
        )?;
        self.extend_scavenger_placed_references(
            &mut references,
            "SELECT data_pg_id, part_okh, part_vid, ec_k, ec_m \
             FROM multipart_reclaim_parts WHERE storage_kind = 0",
            "list multipart reclaim part shard scavenger references",
        )?;
        self.extend_scavenger_placed_references(
            &mut references,
            "SELECT data_pg_id, segment_okh, segment_vid, ec_k, ec_m \
             FROM multipart_reclaim_part_segments",
            "list multipart reclaim segment shard scavenger references",
        )?;
        self.extend_scavenger_routed_multipart_part_references(&mut references)?;
        self.extend_scavenger_pending_command_references(&mut references)?;
        Ok(references)
    }

    fn extend_scavenger_pending_command_references(
        &self,
        references: &mut Vec<ShardScavengerPayloadReference>,
    ) -> Result<(), StoreError> {
        let Some(command_bytes) = self.query_row_cached_optional(
            "SELECT command_bytes FROM metadata_command_pending_slot WHERE singleton = 0",
            [],
            "load pending metadata command for shard scavenger references",
            |row| row.get::<_, Vec<u8>>(0),
        )?
        else {
            return Ok(());
        };
        let command = decode_metadata_command_envelope(&command_bytes).map_err(|reason| {
            StoreError::ShardScavengerScanIncomplete {
                context: "decode pending metadata command for shard scavenger references",
                errors: reason,
            }
        })?;
        self.extend_scavenger_command_payload_references(references, command.payload());
        Ok(())
    }

    fn extend_scavenger_command_payload_references(
        &self,
        references: &mut Vec<ShardScavengerPayloadReference>,
        payload: &MetadataCommandPayload,
    ) {
        match payload {
            MetadataCommandPayload::CommitDirectPutObject(command) => {
                Self::extend_object_segment_references(references, &command.segments);
                if let Some(stale_payload) = &command.stale_payload {
                    Self::extend_reclaim_payload_references(references, stale_payload);
                }
            }
            MetadataCommandPayload::CommitMultipartObject(command) => {
                Self::extend_object_part_references(references, &command.parts);
                Self::extend_multipart_part_segment_references(
                    references,
                    &command.selected_streaming_segments,
                );
                Self::extend_routed_multipart_part_references(
                    references,
                    &command.object.bucket,
                    &command.object.key,
                    command.object.generation_id,
                    &command.omitted_parts,
                );
                Self::extend_multipart_part_segment_references(
                    references,
                    &command.omitted_streaming_segments,
                );
                Self::extend_stream_segment_references(references, &command.stream_upload_segments);
                if let Some(stale_payload) = &command.stale_payload {
                    Self::extend_reclaim_payload_references(references, stale_payload);
                }
            }
            MetadataCommandPayload::DeleteObjectVersion(command) => {
                if let DeleteObjectVersionTarget::Live { payload, .. } = &command.target {
                    Self::extend_reclaim_payload_references(references, payload);
                }
            }
            MetadataCommandPayload::InsertDeleteMarker(command) => {
                if let Some(stale_payload) = &command.stale_payload {
                    Self::extend_reclaim_payload_references(references, stale_payload);
                }
            }
            MetadataCommandPayload::AppendStreamSegment(command) => {
                Self::extend_stream_segment_reference(references, &command.segment);
            }
            MetadataCommandPayload::AbortStreamUpload(command) => {
                Self::extend_stream_segment_references(references, &command.staged_segments);
            }
            MetadataCommandPayload::CommitStreamPart(command) => {
                Self::extend_multipart_part_segment_references(references, &command.segments);
                if let Some(existing_part) = &command.existing_part {
                    Self::extend_routed_multipart_part_references(
                        references,
                        &command.upload.bucket,
                        &command.upload.key,
                        command.upload.object_generation_id,
                        std::slice::from_ref(existing_part),
                    );
                }
                Self::extend_multipart_part_segment_references(
                    references,
                    &command.displaced_segments,
                );
            }
            MetadataCommandPayload::AbortMultipartUpload(command) => {
                Self::extend_routed_multipart_part_references(
                    references,
                    &command.cleanup.upload.bucket,
                    &command.cleanup.upload.key,
                    command.cleanup.upload.object_generation_id,
                    &command.cleanup.parts,
                );
                Self::extend_multipart_part_segment_references(
                    references,
                    &command.cleanup.streaming_segments,
                );
                Self::extend_stream_segment_references(
                    references,
                    &command.cleanup.stream_upload_segments,
                );
            }
            MetadataCommandPayload::DeleteObjectPayloadReclaim(command) => {
                Self::extend_reclaim_payload_references(references, &command.payload);
            }
            MetadataCommandPayload::CreateBucket(_)
            | MetadataCommandPayload::PutBucketVersioning(_)
            | MetadataCommandPayload::PutBucketAcl(_)
            | MetadataCommandPayload::PutBucketProperty(_)
            | MetadataCommandPayload::PutBucketSubresource(_)
            | MetadataCommandPayload::MarkBucketDeleting(_)
            | MetadataCommandPayload::ReserveObjectGeneration(_)
            | MetadataCommandPayload::ReleaseObjectGeneration(_)
            | MetadataCommandPayload::ReserveObjectVersion(_)
            | MetadataCommandPayload::PutObjectMetadata(_)
            | MetadataCommandPayload::CreateStreamUpload(_)
            | MetadataCommandPayload::CreateMultipartUpload(_)
            | MetadataCommandPayload::DeleteCompletedMultipartUpload(_)
            | MetadataCommandPayload::AdvanceCompletedMultipartUploadSequence(_) => {}
        }
    }

    fn extend_object_segment_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        segments: &[ObjectSegmentRecord],
    ) {
        for segment in segments {
            Self::push_placed_reference(
                references,
                segment.data_pg_id,
                segment.segment_okh,
                segment.segment_vid,
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            );
        }
    }

    fn extend_object_part_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        parts: &[ObjectPartRecord],
    ) {
        for part in parts {
            if part.part_okh == [0; 16] {
                continue;
            }
            Self::push_placed_reference(
                references,
                part.data_pg_id,
                part.part_okh,
                part.part_vid,
                EcShape {
                    k: part.ec_k,
                    m: part.ec_m,
                },
            );
        }
    }

    fn extend_stream_segment_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        segments: &[StreamUploadSegmentRecord],
    ) {
        for segment in segments {
            Self::extend_stream_segment_reference(references, segment);
        }
    }

    fn extend_stream_segment_reference(
        references: &mut Vec<ShardScavengerPayloadReference>,
        segment: &StreamUploadSegmentRecord,
    ) {
        Self::push_placed_reference(
            references,
            segment.data_pg_id,
            segment.segment_okh,
            segment.segment_vid,
            EcShape {
                k: segment.ec_k,
                m: segment.ec_m,
            },
        );
    }

    fn extend_multipart_part_segment_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        segments: &[MultipartPartSegmentRecord],
    ) {
        for segment in segments {
            Self::push_placed_reference(
                references,
                segment.data_pg_id,
                segment.segment_okh,
                segment.segment_vid,
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            );
        }
    }

    fn extend_routed_multipart_part_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        bucket: &BucketName,
        key: &ObjectKey,
        object_generation_id: GenerationId,
        parts: &[MultipartPartRecord],
    ) {
        for part in parts {
            if part.part_okh == [0; 16] {
                continue;
            }
            references.push(ShardScavengerPayloadReference::RoutedMultipartPart(
                ShardScavengerRoutedMultipartPartReference {
                    bucket: bucket.clone(),
                    key: key.clone(),
                    object_generation_id,
                    part_number: part.part_number,
                    part_okh: part.part_okh,
                    part_vid: part.part_vid,
                    ec: EcShape {
                        k: part.ec_k,
                        m: part.ec_m,
                    },
                },
            ));
        }
    }

    fn extend_reclaim_payload_references(
        references: &mut Vec<ShardScavengerPayloadReference>,
        payload: &ObjectPayloadReclaimCommand,
    ) {
        match payload {
            ObjectPayloadReclaimCommand::Segments(reclaim) => {
                for segment in &reclaim.segments {
                    Self::push_placed_reference(
                        references,
                        segment.data_pg_id,
                        segment.segment_okh,
                        segment.segment_vid,
                        segment.ec,
                    );
                }
            }
            ObjectPayloadReclaimCommand::Multipart(reclaim) => {
                for part in &reclaim.parts {
                    match part {
                        MultipartReclaimPartRecord::ShardSet {
                            part_okh,
                            part_vid,
                            data_pg_id,
                            ec,
                            ..
                        } => Self::push_placed_reference(
                            references,
                            *data_pg_id,
                            *part_okh,
                            *part_vid,
                            *ec,
                        ),
                        MultipartReclaimPartRecord::Segments { segments, .. } => {
                            for segment in segments {
                                Self::push_placed_reference(
                                    references,
                                    segment.data_pg_id,
                                    segment.segment_okh,
                                    segment.segment_vid,
                                    segment.ec,
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    fn push_placed_reference(
        references: &mut Vec<ShardScavengerPayloadReference>,
        data_pg_id: u32,
        okh: [u8; 16],
        generation_id: GenerationId,
        ec: EcShape,
    ) {
        references.push(ShardScavengerPayloadReference::Placed(
            ShardScavengerPlacedShardSetReference {
                data_pg_id,
                okh,
                generation_id,
                ec,
            },
        ));
    }

    pub(crate) fn list_scavenger_shard_rows(&self) -> Result<Vec<ScavengerShardRow>, StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT shard_key, data_size, crc64_nvme \
                 FROM shards \
                 ORDER BY shard_key",
            )
            .map_err(|source| StoreError::Db {
                context: "list scavenger shard rows (prepare)",
                source,
            })?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(|source| StoreError::Db {
                context: "list scavenger shard rows",
                source,
            })?;
        let mut shard_rows = Vec::new();
        for row in rows {
            let (key, stored_size, crc64) = row.map_err(|source| StoreError::Db {
                context: "read scavenger shard row",
                source,
            })?;
            shard_rows.push(ScavengerShardRow {
                key: ShardKey::from_bytes(&key)?,
                ack: WriteAck {
                    stored_size: stored_size as u64,
                    crc64: crc64 as u64,
                },
            });
        }
        Ok(shard_rows)
    }

    fn extend_scavenger_placed_references(
        &self,
        references: &mut Vec<ShardScavengerPayloadReference>,
        sql: &'static str,
        context: &'static str,
    ) -> Result<(), StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(sql)
            .map_err(|source| StoreError::Db { context, source })?;
        let rows = stmt
            .query_map([], |row| {
                let okh_blob: Vec<u8> = row.get(1)?;
                Ok(ShardScavengerPayloadReference::Placed(
                    ShardScavengerPlacedShardSetReference {
                        data_pg_id: row.get(0)?,
                        okh: PgStore::parse_okh_blob(&okh_blob, 1)?,
                        generation_id: PgStore::parse_generation_id(
                            row.get::<_, i64>(2)?,
                            2,
                            "shard scavenger reference generation",
                        )?,
                        ec: EcShape {
                            k: row.get(3)?,
                            m: row.get(4)?,
                        },
                    },
                ))
            })
            .map_err(|source| StoreError::Db { context, source })?;
        for row in rows {
            references.push(row.map_err(|source| StoreError::Db { context, source })?);
        }
        Ok(())
    }

    fn extend_scavenger_routed_multipart_part_references(
        &self,
        references: &mut Vec<ShardScavengerPayloadReference>,
    ) -> Result<(), StoreError> {
        let context = "list routed multipart part shard scavenger references";
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT u.bucket, u.key, u.object_generation_id, p.part_number, \
                 p.part_okh, p.part_vid, p.ec_k, p.ec_m \
                 FROM multipart_parts p \
                 JOIN multipart_uploads u ON u.upload_id = p.upload_id \
                 WHERE p.part_okh != zeroblob(16)",
            )
            .map_err(|source| StoreError::Db { context, source })?;
        let rows = stmt
            .query_map([], |row| {
                let okh_blob: Vec<u8> = row.get(4)?;
                Ok(ShardScavengerPayloadReference::RoutedMultipartPart(
                    ShardScavengerRoutedMultipartPartReference {
                        bucket: row.get(0)?,
                        key: row.get(1)?,
                        object_generation_id: PgStore::parse_generation_id(
                            row.get::<_, i64>(2)?,
                            2,
                            "multipart upload object generation",
                        )?,
                        part_number: row.get(3)?,
                        part_okh: PgStore::parse_okh_blob(&okh_blob, 4)?,
                        part_vid: PgStore::parse_generation_id(
                            row.get::<_, i64>(5)?,
                            5,
                            "multipart part payload generation",
                        )?,
                        ec: EcShape {
                            k: row.get(6)?,
                            m: row.get(7)?,
                        },
                    },
                ))
            })
            .map_err(|source| StoreError::Db { context, source })?;
        for row in rows {
            references.push(row.map_err(|source| StoreError::Db { context, source })?);
        }
        Ok(())
    }

    pub(crate) fn list_scavenger_shard_files(&self) -> Result<ScavengerShardFileScan, StoreError> {
        Self::list_scavenger_shard_files_in_dir(&self.shards_dir)
    }

    pub(crate) fn list_scavenger_shard_files_in_dir(
        shards_dir: &Path,
    ) -> Result<ScavengerShardFileScan, StoreError> {
        let mut files = Vec::new();
        let mut errors = Vec::new();
        for prefix in fs::read_dir(shards_dir).map_err(|source| StoreError::Io {
            context: "scan shard prefix directory",
            source,
        })? {
            let prefix = prefix.map_err(|source| StoreError::Io {
                context: "read shard prefix directory entry",
                source,
            })?;
            let file_type = prefix.file_type().map_err(|source| StoreError::Io {
                context: "read shard prefix directory entry type",
                source,
            })?;
            let prefix_path = prefix.path();
            if !file_type.is_dir() {
                errors.push(format!(
                    "unexpected non-directory entry under shard root {}",
                    prefix_path.display()
                ));
                continue;
            }
            let prefix_name = prefix.file_name();
            let Some(prefix_name) = prefix_name.to_str() else {
                errors.push(format!(
                    "non-UTF8 shard prefix directory {}",
                    prefix_path.display()
                ));
                continue;
            };
            if !is_canonical_shard_prefix(prefix_name) {
                errors.push(format!(
                    "invalid shard prefix directory {}",
                    prefix_path.display()
                ));
                continue;
            }
            for entry in fs::read_dir(prefix.path()).map_err(|source| StoreError::Io {
                context: "scan shard directory",
                source,
            })? {
                let entry = entry.map_err(|source| StoreError::Io {
                    context: "read shard directory entry",
                    source,
                })?;
                let file_type = entry.file_type().map_err(|source| StoreError::Io {
                    context: "read shard directory entry type",
                    source,
                })?;
                let path = entry.path();
                if !file_type.is_file() {
                    errors.push(format!(
                        "unexpected non-file entry under shard prefix {}",
                        path.display()
                    ));
                    continue;
                }
                let file_name = entry.file_name();
                let Some(file_name) = file_name.to_str() else {
                    errors.push(format!("non-UTF8 shard file {}", path.display()));
                    continue;
                };
                let Ok(key) = ShardKey::from_hex(file_name) else {
                    errors.push(format!("invalid shard file name {}", path.display()));
                    continue;
                };
                let canonical_prefix = key.hex_prefix();
                let canonical_file_name = key.to_string();
                if prefix_name != canonical_prefix || file_name != canonical_file_name {
                    errors.push(format!(
                        "non-canonical shard file {} expected shards/{}/{}",
                        path.display(),
                        canonical_prefix,
                        canonical_file_name
                    ));
                    continue;
                };
                let metadata = entry.metadata().map_err(|source| StoreError::Io {
                    context: "stat shard file during scavenger scan",
                    source,
                })?;
                files.push(ScavengerShardFile {
                    key,
                    size: metadata.len(),
                });
            }
        }
        files.sort_by(|left, right| left.key.as_bytes().cmp(right.key.as_bytes()));
        Ok(ScavengerShardFileScan { files, errors })
    }

    fn validate_shard_scavenger_observation_key(
        &self,
        key: &ShardScavengerObservationKey,
    ) -> Result<(), StoreError> {
        if key.data_pg_id != self.pg_id {
            return Err(StoreError::ShardScavengerObservationWrongPg {
                store_pg_id: self.pg_id,
                observation_pg_id: key.data_pg_id,
            });
        }
        let key_shard_index = key.shard_key.shard_index();
        if key.shard_index != key_shard_index {
            return Err(StoreError::ShardScavengerObservationShardIndexMismatch {
                observation_shard_index: key.shard_index.get(),
                key_shard_index: key_shard_index.get(),
            });
        }
        Ok(())
    }

    fn validate_shard_scavenger_observation_record(
        &self,
        observation: &ShardScavengerObservationRecord,
    ) -> Result<(), StoreError> {
        self.validate_shard_scavenger_observation_key(&observation.key)?;
        let state_matches_reason = match observation.reason {
            ShardScavengerObservationReason::FileWithoutShardRow => {
                observation.file_exists && !observation.shard_row_exists
            }
            ShardScavengerObservationReason::ShardRowWithoutFile => {
                !observation.file_exists && observation.shard_row_exists
            }
            ShardScavengerObservationReason::UnreferencedShardRowAndFile => {
                observation.file_exists && observation.shard_row_exists
            }
            ShardScavengerObservationReason::ScanIncomplete => true,
        };
        if !state_matches_reason {
            return Err(StoreError::ShardScavengerObservationInconsistentReason {
                reason: observation.reason,
                file_exists: observation.file_exists,
                shard_row_exists: observation.shard_row_exists,
            });
        }
        Ok(())
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
            return Ok(false);
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
            && entry.log_hash == Some(expected_log_hash))
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
        if self
            .load_metadata_command_log_entry(
                "check pending metadata command slot log entry",
                command.id().cluster_epoch(),
                command.id().pg_id(),
                command.id().log_index(),
            )?
            .is_some()
        {
            return Err(self.metadata_command_log_conflict(node_id, command));
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

    pub(crate) fn try_insert_bucket_control_pending_metadata_command_slot(
        &self,
        node_id: u32,
        command: &MetadataCommandEnvelope,
        bucket: &BucketName,
    ) -> Result<bool, StoreError> {
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
        {
            return Err(self.metadata_command_log_conflict(node_id, command));
        }

        let command_bytes = command.command_bytes();
        let inserted = self.execute_cached(
            "INSERT INTO metadata_command_pending_slot \
             (singleton, cluster_epoch, pg_id, log_index, command_checksum, command_bytes, scope_bucket) \
             SELECT 0, ?1, ?2, ?3, ?4, ?5, ?6 \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM bucket_write_drains WHERE bucket_name = ?7 \
             ) \
             ON CONFLICT(singleton) DO NOTHING",
            params![
                command.id().cluster_epoch().get() as i64,
                command.id().pg_id().get() as i64,
                command.id().log_index().get() as i64,
                command.checksum_crc64() as i64,
                command_bytes,
                bucket.as_str(),
                bucket.as_str(),
            ],
            "insert bucket control pending metadata command slot",
        )?;
        Ok(inserted == 1)
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

    fn validate_metadata_command_replay_state_with_pending_cleanup(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        pending_cleanup: PendingMetadataCommandSlotCleanup,
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

    fn advance_abandoned_metadata_command_log_tail(
        &self,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        state: MetadataCommandReplicaState,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        let pg_id = PgId::new(self.pg_id);
        let mut applied_log_index = state.applied_log_index;
        let mut applied_log_hash = state.applied_log_hash;
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
                            expected_log_hash as i64,
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
                    if previous_log_hash == applied_log_hash && log_hash == expected_log_hash => {}
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

            applied_log_index = next_log_index;
            applied_log_hash = expected_log_hash;
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
        inserted_entry: Option<(MetadataCommandLogIndex, u64, bool)>,
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
                    (expected_log_hash, entry.abandoned)
                };
            if !entry_abandoned {
                advanced_materialized_state = true;
            }
            applied_log_index = next_log_index;
            applied_log_hash = expected_log_hash;
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

    fn update_metadata_command_replica_state_preserving_digest(
        &self,
        cluster_epoch: ClusterEpoch,
        applied_log_index: u64,
        applied_log_hash: u64,
        state_digest: u64,
    ) -> Result<MetadataCommandRecordResult, StoreError> {
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
            "update metadata command replica state preserving digest",
        )?;
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

    pub(crate) fn write_shard_file_durable_if_absent(
        tmp_dir: &Path,
        shards_dir: &Path,
        key: &ShardKey,
        data: &[u8],
    ) -> Result<WriteAck, StoreError> {
        let expected = WriteAck {
            crc64: checksum::crc64::checksum(data),
            stored_size: data.len() as u64,
        };
        let shard_path = Self::shard_path_for_shards_dir(shards_dir, key);

        for _ in 0..2 {
            let sequence = SHARD_TMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let tmp_name = format!("shard-{}-{sequence}-{key}", std::process::id());
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
            file.sync_data().map_err(|e| {
                let _ = fs::remove_file(&tmp_path);
                StoreError::Io {
                    context: "fdatasync shard",
                    source: e,
                }
            })?;
            drop(file);

            if let Some(parent) = shard_path.parent() {
                fs::create_dir_all(parent).map_err(|e| {
                    let _ = fs::remove_file(&tmp_path);
                    StoreError::Io {
                        context: "create shard prefix dir",
                        source: e,
                    }
                })?;
            }

            match fs::hard_link(&tmp_path, &shard_path) {
                Ok(()) => {
                    let _ = fs::remove_file(&tmp_path);
                    if let Some(parent) = shard_path.parent() {
                        fsync_dir(parent).map_err(|e| StoreError::Io {
                            context: "fsync shard parent dir",
                            source: e,
                        })?;
                    }
                    return Ok(expected);
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let _ = fs::remove_file(&tmp_path);
                    match fs::read(&shard_path) {
                        Ok(existing) => {
                            let actual = WriteAck {
                                crc64: checksum::crc64::checksum(&existing),
                                stored_size: existing.len() as u64,
                            };
                            if actual.stored_size == expected.stored_size
                                && actual.crc64 == expected.crc64
                            {
                                return Ok(actual);
                            }
                            return Err(StoreError::ShardAckMismatch {
                                shard: key.clone(),
                                expected_size: expected.stored_size,
                                expected_crc: expected.crc64,
                                actual_size: actual.stored_size,
                                actual_crc: actual.crc64,
                            });
                        }
                        Err(read_error) if read_error.kind() == std::io::ErrorKind::NotFound => {
                            continue;
                        }
                        Err(read_error) => {
                            return Err(StoreError::Io {
                                context: "read existing shard file",
                                source: read_error,
                            });
                        }
                    }
                }
                Err(e) => {
                    let _ = fs::remove_file(&tmp_path);
                    return Err(StoreError::Io {
                        context: "link shard into place",
                        source: e,
                    });
                }
            }
        }

        Err(StoreError::NotFound)
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

    pub fn validate_written_shard_ack(
        &self,
        key: &ShardKey,
        expected: WriteAck,
    ) -> Result<(), StoreError> {
        let row: Option<(i64, i64, i64)> = self
            .conn
            .query_row(
                "SELECT data_size, crc64_nvme, status FROM shards WHERE shard_key = ?1",
                params![key.as_bytes().as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|e| StoreError::Db {
                context: "validate written shard ack",
                source: e,
            })?;
        let Some((actual_size, actual_crc, status)) = row else {
            return Err(StoreError::NotFound);
        };
        if status != ShardStatus::Live as i64 {
            return Err(StoreError::NotFound);
        }
        let actual_size = actual_size as u64;
        let actual_crc = actual_crc as u64;
        if actual_size != expected.stored_size || actual_crc != expected.crc64 {
            return Err(StoreError::ShardAckMismatch {
                shard: key.clone(),
                expected_size: expected.stored_size,
                expected_crc: expected.crc64,
                actual_size,
                actual_crc,
            });
        }
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

    pub fn register_written_shards_batch_exact(
        &self,
        shards: &[(&ShardKey, WriteAck)],
    ) -> Result<(), StoreError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "PgStore::register_written_shards_batch_exact",
            "pg_id={} shards={}",
            self.pg_id,
            shards.len()
        );
        self.conn
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(|e| StoreError::Db {
                context: "register written shards batch exact (begin txn)",
                source: e,
            })?;

        let now = Self::now_secs() as i64;
        let result: Result<(), StoreError> = (|| {
            let mut lookup = self
                .conn
                .prepare_cached(
                    "SELECT data_size, crc64_nvme, status FROM shards WHERE shard_key = ?1",
                )
                .map_err(|e| StoreError::Db {
                    context: "register written shards batch exact (prepare lookup)",
                    source: e,
                })?;
            let mut insert = self
                .conn
                .prepare_cached(
                    "INSERT INTO shards (shard_key, data_size, crc64_nvme, created_at, status) \
                     VALUES (?1, ?2, ?3, ?4, 0)",
                )
                .map_err(|e| StoreError::Db {
                    context: "register written shards batch exact (prepare insert)",
                    source: e,
                })?;

            for (key, ack) in shards {
                let row: Option<(i64, i64, i64)> = lookup
                    .query_row(params![key.as_bytes().as_slice()], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .optional()
                    .map_err(|e| StoreError::Db {
                        context: "register written shards batch exact (lookup shard record)",
                        source: e,
                    })?;

                if let Some((actual_size, actual_crc, status)) = row {
                    let actual_size = actual_size as u64;
                    let actual_crc = actual_crc as u64;
                    if status == ShardStatus::Live as i64
                        && actual_size == ack.stored_size
                        && actual_crc == ack.crc64
                    {
                        continue;
                    }
                    return Err(StoreError::ShardAckMismatch {
                        shard: (*key).clone(),
                        expected_size: ack.stored_size,
                        expected_crc: ack.crc64,
                        actual_size,
                        actual_crc,
                    });
                }

                insert
                    .execute(params![
                        key.as_bytes().as_slice(),
                        ack.stored_size as i64,
                        ack.crc64 as i64,
                        now,
                    ])
                    .map_err(|e| StoreError::Db {
                        context: "register written shards batch exact (insert shard record)",
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
                        context: "register written shards batch exact (commit txn)",
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
                row.get::<_, i64>(10)?,
                row.get::<_, i64>(11)?,
                row.get::<_, i64>(12)?,
                row.get::<_, i64>(13)?,
                row.get::<_, i64>(14)?,
            ),
            [10, 11, 12, 13, 14],
        )?;
        let ownership_controls = Self::parse_ownership_controls(row.get(15)?, 15)?;
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
            public_access_block,
            ownership_controls,
            bucket_policy_present: row.get::<_, i64>(16)? != 0,
            bucket_policy_public: row.get::<_, i64>(17)? != 0,
            bucket_policy_generation: row.get::<_, i64>(18)? as u64,
            bucket_lifecycle_present: row.get::<_, i64>(19)? != 0,
            bucket_lifecycle_generation: row.get::<_, i64>(20)? as u64,
            bucket_execution_generation: row.get::<_, i64>(21)? as u64,
            bucket_incarnation_generation: row.get::<_, i64>(22)? as u64,
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
                row.get::<_, i64>(10)?,
                row.get::<_, i64>(11)?,
                row.get::<_, i64>(12)?,
                row.get::<_, i64>(13)?,
                row.get::<_, i64>(14)?,
            ),
            [10, 11, 12, 13, 14],
        )?;
        let ownership_controls = Self::parse_ownership_controls(row.get(15)?, 15)?;
        let object_lock = Self::parse_bucket_object_lock(
            (
                row.get::<_, i64>(25)?,
                row.get::<_, Option<u8>>(26)?,
                row.get::<_, Option<i64>>(27)?,
                row.get::<_, Option<i64>>(28)?,
            ),
            [25, 26, 27, 28],
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
            public_access_block,
            ownership_controls,
            bucket_policy_public: row.get::<_, i64>(16)? != 0,
            bucket_policy_generation: row.get::<_, i64>(17)? as u64,
            bucket_lifecycle_generation: row.get::<_, i64>(18)? as u64,
            bucket_execution_generation: row.get::<_, i64>(19)? as u64,
            bucket_incarnation_generation: row.get::<_, i64>(20)? as u64,
            completed_multipart_upload_sequence: row.get::<_, i64>(21)? as u64,
            bucket_abac_enabled: row.get::<_, i64>(22)? != 0,
            encryption: BucketEncryptionConfig {
                default_encryption: row
                    .get::<_, Option<u8>>(23)?
                    .map(|value| {
                        ManagedEncryptionAlgorithm::from_u8(value).ok_or_else(|| {
                            rusqlite::Error::FromSqlConversionFailure(
                                23,
                                rusqlite::types::Type::Integer,
                                Box::from(format!("invalid default_encryption_type: {value}")),
                            )
                        })
                    })
                    .transpose()?,
                sse_c_blocked: row.get::<_, i64>(24)? != 0,
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
                    return Err(MetadataError::StaleBucketMetadataCommand {
                        name: name.clone(),
                        bucket_execution_generation: explicit,
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

/// fsync a directory to ensure renames are durable.
fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    let f = fs::File::open(dir)?;
    f.sync_all()?;
    Ok(())
}
