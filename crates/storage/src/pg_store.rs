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
    MetadataCommandId, MetadataCommandLogEntryKind, MetadataCommandLogHashRangeEntry,
    MetadataCommandLogIndex, MetadataCommandLogRangeEntry, MetadataCommandLogRangeEntryKind,
    MetadataCommandPayload, MetadataCommandReplicaState, ObjectPayloadReclaimCommand,
    PutBucketAclCommand, PutBucketPropertyCommand, PutBucketSubresourceCommand,
    PutBucketVersioningCommand, PutObjectMetadataCommand, ReleaseObjectGenerationCommand,
    ReserveObjectGenerationCommand, ReserveObjectVersionCommand,
};
use crate::schema::init_pg_schema;
use crate::traits::{DurableBucketWriteReservationHeartbeat, PgMetadataStore, ShardStore};
use crate::types::*;
use placement::NodeId;

const TRACE_TARGET: &str = "storage";

mod command_log;
mod metadata;
mod rows;
mod scavenger;
mod shards;

#[cfg(test)]
use command_log::{digest_len_prefixed_bytes, MetadataDigestFilter, METADATA_DIGEST_TABLES};
pub use command_log::{
    MetadataCheckpointRow, MetadataCheckpointTableBlock, MetadataCheckpointTableDigest,
    MetadataCheckpointValue, MetadataCommandCheckpoint, MetadataCommandCheckpointValidationError,
    MetadataCommandLogCompactionStatus, MetadataCommandLogStats,
};
use command_log::{ObjectGenerationReservationConstraint, ObjectGenerationReservationIdentity};
pub(crate) use scavenger::{ScavengerShardFile, ScavengerShardFileScan, ScavengerShardRow};

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgClusterMapHistoryReferenceSummary {
    pub oldest_live_placement_epoch: Option<ClusterEpoch>,
    pub oldest_durable_backfill_epoch: Option<ClusterEpoch>,
}

impl PgClusterMapHistoryReferenceSummary {
    #[must_use]
    pub fn oldest_required_epoch(&self) -> Option<ClusterEpoch> {
        [
            self.oldest_live_placement_epoch,
            self.oldest_durable_backfill_epoch,
        ]
        .into_iter()
        .flatten()
        .min()
    }

    pub fn merge(&mut self, other: Self) {
        self.oldest_live_placement_epoch = min_optional_epoch(
            self.oldest_live_placement_epoch,
            other.oldest_live_placement_epoch,
        );
        self.oldest_durable_backfill_epoch = min_optional_epoch(
            self.oldest_durable_backfill_epoch,
            other.oldest_durable_backfill_epoch,
        );
    }
}

fn min_optional_epoch(
    left: Option<ClusterEpoch>,
    right: Option<ClusterEpoch>,
) -> Option<ClusterEpoch> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn parse_optional_cluster_epoch(
    raw_epoch: Option<i64>,
    context: &'static str,
) -> Result<Option<ClusterEpoch>, StoreError> {
    raw_epoch
        .map(|raw| {
            let epoch = u64::try_from(raw).map_err(|_| StoreError::Db {
                context,
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from("negative cluster epoch"),
                ),
            })?;
            ClusterEpoch::new(epoch).ok_or_else(|| StoreError::Db {
                context,
                source: rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from("zero cluster epoch"),
                ),
            })
        })
        .transpose()
}

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
        lease_deadline: row.get::<_, i64>(18)? as u64,
        target_context: row.get(19)?,
    }))
}

fn stream_upload_bucket_write_reservation_matches_command(
    existing: &StreamUploadRecord,
    command: &CreateStreamUploadCommand,
) -> bool {
    match command.session.target {
        StreamUploadTarget::PutObject => {
            existing
                .bucket_write_reservation
                .as_ref()
                .is_some_and(|proof| {
                    bucket_write_reservation_stable_identity_matches(
                        proof,
                        &command.bucket_write_reservation,
                    )
                })
        }
        StreamUploadTarget::UploadPart { .. } => existing.bucket_write_reservation.is_none(),
    }
}

fn bucket_write_reservation_stable_identity_matches(
    left: &BucketWriteReservationProof,
    right: &BucketWriteReservationProof,
) -> bool {
    left.bucket == right.bucket
        && left.reservation_id == right.reservation_id
        && left.owner_token == right.owner_token
        && left.cluster_epoch == right.cluster_epoch
        && left.bucket_execution_generation == right.bucket_execution_generation
        && left.bucket_incarnation_generation == right.bucket_incarnation_generation
        && left.operation_kind == right.operation_kind
        && left.created_at == right.created_at
        && left.target_context == right.target_context
}

/// Part segment rows use a sentinel version_id during staging (pre-CompleteMultipartUpload).
/// Must differ from any real version_id (0 for unversioned, 1+ for versioned) so that
/// in-progress staging rows are invisible to reads of completed objects.
const PART_SEGMENT_STAGING_VERSION_ID: VersionId = MULTIPART_PART_SEGMENT_STAGING_VERSION_ID;

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

/// Per-PG store combining shard file I/O with SQLite metadata.
pub struct PgStore {
    pg_id: u32,
    shards_dir: PathBuf,
    tmp_dir: PathBuf,
    conn: Connection,
    clean_metadata_digest_revision: AtomicU64,
    #[cfg(test)]
    fail_next_metadata_txn_commit: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    metadata_command_log_prefix_fast_path_hits: AtomicU64,
    #[cfg(test)]
    metadata_command_log_replay_validation_entries: AtomicU64,
}

const PG_STORE_STATEMENT_CACHE_CAPACITY: usize = 1024;
const SQLITE_PROFILE_DISABLED: u64 = u64::MAX;
const UNCLEAN_METADATA_DIGEST_REVISION: u64 = u64::MAX;
static SQLITE_PROFILE_THRESHOLD_NANOS: AtomicU64 = AtomicU64::new(SQLITE_PROFILE_DISABLED);
static SHARD_TMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Inputs required to recover a [`PgStore`] after open.
///
/// Recovery reconciles benign crash leftovers that can be proven locally
/// (older-epoch orphan pending command slots and cache-only digest drift) and
/// fails closed on command-log, digest, or future-epoch pending-slot ambiguity
/// before the store serves. The owning node id is the only external input: the
/// recovery epoch is read from the store's own replica state, never supplied by
/// the caller.
///
/// This context is intentionally distinct from heartbeat/observation paths:
/// recovery may mutate PG metadata to repair crash leftovers before serving,
/// while heartbeat must only report the current proof and pending-slot
/// presence. See the "PG Store Recovery Boundary" section of
/// `guides/storage-cluster-invariants.md`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PgStoreRecoveryContext {
    node_id: NodeId,
}

impl PgStoreRecoveryContext {
    pub(crate) fn for_node(node_id: NodeId) -> Self {
        Self { node_id }
    }

    pub(crate) fn node_id(self) -> NodeId {
        self.node_id
    }
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
        command_log::register_metadata_digest_sql_functions(&conn).map_err(|e| StoreError::Db {
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
            fail_next_metadata_txn_commit: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            metadata_command_log_prefix_fast_path_hits: AtomicU64::new(0),
            #[cfg(test)]
            metadata_command_log_replay_validation_entries: AtomicU64::new(0),
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

    pub fn cluster_map_history_reference_summary(
        &self,
    ) -> Result<PgClusterMapHistoryReferenceSummary, StoreError> {
        Ok(PgClusterMapHistoryReferenceSummary {
            oldest_live_placement_epoch: self.oldest_live_payload_placement_epoch()?,
            oldest_durable_backfill_epoch: self.oldest_durable_backfill_epoch()?,
        })
    }

    fn oldest_live_payload_placement_epoch(&self) -> Result<Option<ClusterEpoch>, StoreError> {
        let raw_epoch = self.query_row_cached(
            "SELECT MIN(placement_cluster_epoch) FROM ( \
                 SELECT placement_cluster_epoch FROM object_segments \
                 UNION ALL SELECT placement_cluster_epoch FROM object_parts \
                 UNION ALL SELECT placement_cluster_epoch FROM multipart_parts \
                 UNION ALL SELECT placement_cluster_epoch FROM multipart_part_segments \
                 UNION ALL SELECT placement_cluster_epoch FROM stream_upload_segments \
             )",
            [],
            "compute oldest live payload placement epoch",
            |row| row.get::<_, Option<i64>>(0),
        )?;
        parse_optional_cluster_epoch(raw_epoch, "oldest live payload placement epoch")
    }

    fn oldest_durable_backfill_epoch(&self) -> Result<Option<ClusterEpoch>, StoreError> {
        let raw_epoch = self.query_row_cached(
            "SELECT MIN(cluster_epoch) FROM ( \
                 SELECT source_cluster_epoch AS cluster_epoch FROM placed_segment_shard_backfills \
                 UNION ALL SELECT desired_cluster_epoch FROM placed_segment_shard_backfills \
             )",
            [],
            "compute oldest durable backfill epoch",
            |row| row.get::<_, Option<i64>>(0),
        )?;
        parse_optional_cluster_epoch(raw_epoch, "oldest durable backfill epoch")
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

    /// Get the current unix timestamp in seconds.
    fn now_secs() -> u64 {
        crate::clock::current_time_secs()
    }

    /// Get the current unix timestamp in milliseconds.
    #[cfg(any(test, feature = "test-hooks"))]
    fn now_millis() -> u64 {
        crate::clock::current_time_millis()
    }
}
