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
use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::trace::{TraceEvent, TraceEventCodes};
use rusqlite::types::ValueRef;
use rusqlite::{
    params, params_from_iter, Connection, Error as SqlError, OpenFlags, OptionalExtension, Params,
    Row,
};

use crate::error::{BucketSnapshotLoadError, MetadataError, StoreError};
#[cfg(test)]
use crate::metadata_command::BucketPropertyMutation;
use crate::metadata_command::{
    abandoned_command_log_bytes, decode_metadata_command_envelope,
    decode_metadata_command_log_entry_header, metadata_command_log_hash,
    AbortMultipartUploadCommand, AbortStreamUploadCommand,
    AdvanceMultipartCompletionBarrierCommand, AppendStreamSegmentCommand, BucketPropertyEffect,
    BucketRecord, BucketSubresourceMutation, BucketWriteReservationProof,
    CommitDirectPutObjectCommand, CommitMultipartObjectCommand, CommitStreamPartCommand,
    CreateBucketCommand, CreateMultipartUploadCommand, CreateStreamUploadCommand,
    DeleteObjectPayloadReclaimCommand, DeleteObjectVersionCommand, DeleteObjectVersionTarget,
    InsertDeleteMarkerCommand, MarkBucketDeletingCommand, MetadataCommandAcceptance,
    MetadataCommandEnvelope, MetadataCommandId, MetadataCommandLogEntryKind,
    MetadataCommandLogHashRangeEntry, MetadataCommandLogIndex, MetadataCommandLogRangeEntry,
    MetadataCommandLogRangeEntryKind, MetadataCommandPayload, MetadataCommandReplicaState,
    ObjectPayloadReclaimCommand, PutBucketAclCommand, PutBucketPropertyCommand,
    PutBucketSubresourceCommand, PutBucketVersioningCommand, PutObjectMetadataCommand,
    ReleaseObjectGenerationCommand, ReserveObjectGenerationCommand, ReserveObjectVersionCommand,
};
use crate::node_runtime::traits::{
    DurableBucketWriteReservationAcquire, DurableBucketWriteReservationHeartbeat, PgMetadataStore,
    ShardStore,
};
use crate::types::*;
use placement::NodeId;

const TRACE_TARGET: &str = "storage";

impl From<rusqlite::Error> for crate::error::DatabaseError {
    fn from(source: rusqlite::Error) -> Self {
        Self::new(source.to_string())
    }
}

impl crate::error::DatabaseError {
    fn from_sql_conversion_failure(
        index: usize,
        value_type: rusqlite::types::Type,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    ) -> Self {
        rusqlite::Error::FromSqlConversionFailure(index, value_type, source).into()
    }

    fn to_sql_conversion_failure(
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    ) -> Self {
        rusqlite::Error::ToSqlConversionFailure(source).into()
    }
}

#[path = "pg_store/command_log.rs"]
mod command_log;
#[path = "pg_store/metadata.rs"]
mod metadata;
#[path = "pg_store/rows.rs"]
mod rows;
#[path = "pg_store/scavenger.rs"]
mod scavenger;
#[path = "pg_store/schema.rs"]
mod schema;
#[path = "pg_store/shards.rs"]
mod shards;
#[path = "pg_store/sql_types.rs"]
mod sql_types;

use schema::{init_pg_schema, require_current_pg_schema};

pub(crate) use command_log::METADATA_CANONICAL_STATE_ENCODING_VERSION;
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
       bucket_lifecycle_generation, bucket_execution_generation, bucket_incarnation_generation, multipart_upload_id_key, bucket_abac_enabled, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years \
FROM buckets";
const BUCKET_INFO_BY_NAME_SELECT: &str = "\
SELECT name, owner_principal, owner_canonical_id, created_at, region, state, versioning, acl_grants, public_read, public_write, \
       public_access_block_present, public_access_block_block_public_acls, public_access_block_ignore_public_acls, public_access_block_block_public_policy, public_access_block_restrict_public_buckets, ownership_controls_mode, \
       EXISTS(SELECT 1 FROM bucket_subresources WHERE bucket_name = buckets.name AND kind = 4 AND body IS NOT NULL) AS bucket_policy_present, \
       bucket_policy_public, bucket_policy_generation, \
       EXISTS(SELECT 1 FROM bucket_subresources WHERE bucket_name = buckets.name AND kind = 5 AND body IS NOT NULL) AS bucket_lifecycle_present, \
       bucket_lifecycle_generation, bucket_execution_generation, bucket_incarnation_generation, multipart_upload_id_key, bucket_abac_enabled, default_encryption_type, sse_c_blocked, object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years \
FROM buckets WHERE name = ?1";

const BUCKET_RECORD_BY_NAME_SELECT: &str = "\
SELECT name, owner_principal, owner_canonical_id, created_at, region, state, versioning, acl_grants, public_read, public_write, \
       public_access_block_present, public_access_block_block_public_acls, public_access_block_ignore_public_acls, public_access_block_block_public_policy, public_access_block_restrict_public_buckets, ownership_controls_mode, \
       bucket_policy_public, bucket_policy_generation, bucket_lifecycle_generation, bucket_execution_generation, bucket_incarnation_generation, multipart_upload_id_key, multipart_completion_barrier_sequence, bucket_abac_enabled, default_encryption_type, sse_c_blocked, \
       object_lock_enabled, object_lock_default_mode, object_lock_default_days, object_lock_default_years \
FROM buckets WHERE name = ?1";

const STREAM_UPLOAD_SELECT: &str = "\
SELECT session_id, bucket, key, op_kind, upload_id, part_number, state, created_at, cleanup_after, encryption_type, encryption_state, next_segment_vid, \
       bucket_write_reservation_id, bucket_write_owner_token, bucket_write_cluster_epoch, bucket_write_execution_generation, \
       bucket_write_incarnation_generation, bucket_write_operation_kind, bucket_write_created_at, bucket_write_lease_deadline, bucket_write_target_context \
FROM stream_uploads";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PgClusterMapHistoryReferenceSummary {
    pub oldest_live_placement_epoch: Option<ClusterEpoch>,
    pub oldest_durable_backfill_epoch: Option<ClusterEpoch>,
    pub oldest_pending_metadata_command_epoch: Option<ClusterEpoch>,
    pub oldest_object_payload_reclaim_claim_epoch: Option<ClusterEpoch>,
}

pub const MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES: usize = 4096;
pub const MAX_PG_DURABLE_IDENTITY_BYTES: usize = 1024;
const SQLITE_FILE_MAGIC: &[u8; 16] = b"SQLite format 3\0";

/// Read-only comparison of the durable shard index with the local shard tree.
///
/// Identity validation and inventory completeness are intentionally separate:
/// every deployment mode must reject a foreign/unbound PG database, while a
/// replicated node may keep the PG active and repair an incomplete local
/// payload inventory through EC reconstruction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PgShardInventoryInspection {
    pub shard_row_count: usize,
    pub shard_file_count: usize,
    pub authoritative_missing_file_count: usize,
    pub authoritative_size_mismatch_count: usize,
    pub recoverable_missing_file_count: usize,
    pub recoverable_size_mismatch_count: usize,
    pub recoverable_unindexed_file_count: usize,
}

impl PgShardInventoryInspection {
    /// Whether every authoritative non-deleting shard row has a matching payload.
    pub(crate) fn authoritative_inventory_is_complete(self) -> bool {
        self.authoritative_missing_file_count == 0 && self.authoritative_size_mismatch_count == 0
    }

    /// Whether crash residue remains for the PG recovery/scavenger path.
    pub(crate) fn has_recoverable_residue(self) -> bool {
        self.recoverable_missing_file_count != 0
            || self.recoverable_size_mismatch_count != 0
            || self.recoverable_unindexed_file_count != 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PgClusterMapHistoryRouteReferenceKind {
    LivePlacement,
    DurableBackfillSource,
    DurableBackfillDesired,
    PendingMetadataCommand,
    ObjectPayloadReclaimClaim,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PgClusterMapHistoryRouteReference {
    kind: PgClusterMapHistoryRouteReferenceKind,
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
}

impl PgClusterMapHistoryRouteReference {
    #[must_use]
    pub const fn new(
        kind: PgClusterMapHistoryRouteReferenceKind,
        cluster_epoch: ClusterEpoch,
        pg_id: PgId,
    ) -> Self {
        Self {
            kind,
            cluster_epoch,
            pg_id,
        }
    }

    #[must_use]
    pub const fn kind(self) -> PgClusterMapHistoryRouteReferenceKind {
        self.kind
    }

    #[must_use]
    pub const fn cluster_epoch(self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub const fn pg_id(self) -> PgId {
        self.pg_id
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PgClusterMapHistoryRouteReferences {
    references: BTreeSet<PgClusterMapHistoryRouteReference>,
}

impl PgClusterMapHistoryRouteReferences {
    pub fn try_from_iter(
        references: impl IntoIterator<Item = PgClusterMapHistoryRouteReference>,
    ) -> Result<Self, StoreError> {
        let mut result = Self::default();
        result.extend(references)?;
        Ok(result)
    }

    pub fn insert(
        &mut self,
        reference: PgClusterMapHistoryRouteReference,
    ) -> Result<(), StoreError> {
        if self.references.contains(&reference) {
            return Ok(());
        }
        let count = self.references.len() + 1;
        if count > MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES {
            return Err(StoreError::ClusterMapHistoryReferenceLimitExceeded {
                count,
                max: MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES,
            });
        }
        self.references.insert(reference);
        Ok(())
    }

    pub fn extend(
        &mut self,
        references: impl IntoIterator<Item = PgClusterMapHistoryRouteReference>,
    ) -> Result<(), StoreError> {
        for reference in references {
            self.insert(reference)?;
        }
        Ok(())
    }

    pub fn merge(&mut self, other: Self) -> Result<(), StoreError> {
        self.extend(other.references)
    }

    pub fn iter(&self) -> impl Iterator<Item = PgClusterMapHistoryRouteReference> + '_ {
        self.references.iter().copied()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.references.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.references.is_empty()
    }

    #[must_use]
    pub fn summary(&self) -> PgClusterMapHistoryReferenceSummary {
        let mut summary = PgClusterMapHistoryReferenceSummary::default();
        for reference in self.iter() {
            let target = match reference.kind() {
                PgClusterMapHistoryRouteReferenceKind::LivePlacement => {
                    &mut summary.oldest_live_placement_epoch
                }
                PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource
                | PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired => {
                    &mut summary.oldest_durable_backfill_epoch
                }
                PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand => {
                    &mut summary.oldest_pending_metadata_command_epoch
                }
                PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim => {
                    &mut summary.oldest_object_payload_reclaim_claim_epoch
                }
            };
            *target = min_optional_epoch(*target, Some(reference.cluster_epoch()));
        }
        summary
    }
}

impl PgClusterMapHistoryReferenceSummary {
    #[must_use]
    pub fn oldest_required_epoch(&self) -> Option<ClusterEpoch> {
        [
            self.oldest_live_placement_epoch,
            self.oldest_durable_backfill_epoch,
            self.oldest_pending_metadata_command_epoch,
            self.oldest_object_payload_reclaim_claim_epoch,
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
        self.oldest_pending_metadata_command_epoch = min_optional_epoch(
            self.oldest_pending_metadata_command_epoch,
            other.oldest_pending_metadata_command_epoch,
        );
        self.oldest_object_payload_reclaim_claim_epoch = min_optional_epoch(
            self.oldest_object_payload_reclaim_claim_epoch,
            other.oldest_object_payload_reclaim_claim_epoch,
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
                source: crate::error::DatabaseError::from_sql_conversion_failure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::from("negative cluster epoch"),
                ),
            })?;
            ClusterEpoch::new(epoch).ok_or_else(|| StoreError::Db {
                context,
                source: crate::error::DatabaseError::from_sql_conversion_failure(
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
        cleanup_after: row
            .get::<_, Option<i64>>(8)?
            .map(|value| {
                u64::try_from(value).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        8,
                        rusqlite::types::Type::Integer,
                        Box::new(error),
                    )
                })
            })
            .transpose()?,
        encryption: PgStore::parse_object_encryption(
            row.get::<_, u8>(9)?,
            row.get::<_, Option<Vec<u8>>>(10)?,
            9,
            10,
        )?,
        next_segment_vid: PgStore::parse_generation_id(
            row.get::<_, i64>(11)?,
            11,
            "next_segment_vid",
        )?,
        bucket_write_reservation: parse_stream_upload_bucket_write_reservation(bucket, row)?,
    })
}

fn parse_stream_upload_bucket_write_reservation(
    bucket: BucketName,
    row: &Row<'_>,
) -> rusqlite::Result<Option<BucketWriteReservationProof>> {
    let Some(reservation_id) = row.get::<_, Option<String>>(12)? else {
        return Ok(None);
    };
    let cluster_epoch_raw: i64 = row.get(14)?;
    let cluster_epoch = ClusterEpoch::new(u64::try_from(cluster_epoch_raw).map_err(|_| {
        rusqlite::Error::FromSqlConversionFailure(
            14,
            rusqlite::types::Type::Integer,
            Box::from("invalid stream bucket write reservation cluster epoch"),
        )
    })?)
    .ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            14,
            rusqlite::types::Type::Integer,
            Box::from("invalid stream bucket write reservation cluster epoch"),
        )
    })?;
    Ok(Some(BucketWriteReservationProof {
        bucket,
        reservation_id,
        owner_token: row.get(13)?,
        cluster_epoch,
        bucket_execution_generation: row.get::<_, i64>(15)? as u64,
        bucket_incarnation_generation: row.get::<_, i64>(16)? as u64,
        operation_kind: row.get(17)?,
        created_at: row.get::<_, i64>(18)? as u64,
        lease_deadline: row.get::<_, i64>(19)? as u64,
        target_context: row.get(20)?,
    }))
}

fn stream_upload_bucket_write_reservation_matches_command(
    existing: &StreamUploadRecord,
    command: &CreateStreamUploadCommand,
) -> bool {
    match command.session.target {
        StreamUploadTarget::PutObject => existing
            .bucket_write_reservation
            .as_ref()
            .is_some_and(|proof| proof.has_same_stable_identity(&command.bucket_write_reservation)),
        StreamUploadTarget::UploadPart { .. } => existing.bucket_write_reservation.is_none(),
    }
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

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TestSqliteConfiguration {
    pub(crate) temp_store: i64,
    pub(crate) journal_mode: String,
    pub(crate) synchronous: i64,
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

/// Bind an initialized PG database to its configured deployment identity.
///
/// This operation is idempotent for an exact identity and fails closed if the
/// database was already bound differently. It checkpoints the identity row
/// into the main database before returning so publishing an outer/root
/// identity cannot get ahead of the per-PG binding.
pub(crate) fn initialize_pg_durable_identity(
    pg_dir: &Path,
    pg_id: u32,
    identity_bytes: &[u8],
) -> Result<(), StoreError> {
    validate_pg_durable_identity_bytes(pg_id, identity_bytes)?;
    let store = PgStore::open(pg_dir, pg_id)?;
    let existing = store
        .conn
        .query_row(
            "SELECT pg_id, identity_bytes FROM pg_durable_identity WHERE singleton = 0",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
        .map_err(|source| StoreError::Db {
            context: "load PG durable identity",
            source: source.into(),
        })?;
    match existing {
        Some((stored_pg_id, stored_identity))
            if stored_pg_id == i64::from(pg_id) && stored_identity == identity_bytes => {}
        Some(_) => {
            return Err(StoreError::PgDurableIdentityInvalid {
                pg_id,
                reason: "database is already bound to a different identity".to_string(),
            });
        }
        None => {
            store
                .conn
                .execute(
                    "INSERT INTO pg_durable_identity (singleton, pg_id, identity_bytes) \
                     VALUES (0, ?1, ?2)",
                    params![i64::from(pg_id), identity_bytes],
                )
                .map_err(|source| StoreError::Db {
                    context: "persist PG durable identity",
                    source: source.into(),
                })?;
        }
    }
    store
        .conn
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
        .map_err(|source| StoreError::Db {
            context: "checkpoint PG durable identity",
            source: source.into(),
        })
}

/// Verify a PG database's configured identity without creating or migrating it.
pub(crate) fn verify_pg_durable_identity(
    pg_dir: &Path,
    pg_id: u32,
    expected_identity_bytes: &[u8],
) -> Result<(), StoreError> {
    validate_pg_durable_identity_bytes(pg_id, expected_identity_bytes)?;
    let conn = open_existing_pg_database_read_only(pg_dir, pg_id)?;
    require_current_pg_schema(&conn)?;
    let quick_check = conn
        .query_row("PRAGMA quick_check(1)", [], |row| row.get::<_, String>(0))
        .map_err(|source| StoreError::Db {
            context: "check PG database integrity",
            source: source.into(),
        })?;
    if quick_check != "ok" {
        return Err(StoreError::PgDurableIdentityInvalid {
            pg_id,
            reason: "database integrity check failed".to_string(),
        });
    }
    let stored = conn
        .query_row(
            "SELECT pg_id, identity_bytes FROM pg_durable_identity WHERE singleton = 0",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?)),
        )
        .optional()
        .map_err(|source| StoreError::PgDurableIdentityInvalid {
            pg_id,
            reason: format!("database has no valid durable identity schema: {source}"),
        })?;
    let Some((stored_pg_id, stored_identity)) = stored else {
        return Err(StoreError::PgDurableIdentityInvalid {
            pg_id,
            reason: "database has no durable identity".to_string(),
        });
    };
    if stored_pg_id != i64::from(pg_id) {
        return Err(StoreError::PgDurableIdentityInvalid {
            pg_id,
            reason: "database is bound to a different PG".to_string(),
        });
    }
    if stored_identity != expected_identity_bytes {
        return Err(StoreError::PgDurableIdentityInvalid {
            pg_id,
            reason: "database is bound to a different deployment identity".to_string(),
        });
    }
    Ok(())
}

/// Compare a PG's indexed shard inventory with its canonical on-disk tree.
///
/// The caller decides whether an incomplete result is fatal or recoverable.
/// Structural scan failures remain errors because no trustworthy inventory can
/// be produced from them.
pub(crate) fn inspect_pg_shard_inventory(
    pg_dir: &Path,
    pg_id: u32,
) -> Result<PgShardInventoryInspection, StoreError> {
    let pg_metadata = fs::symlink_metadata(pg_dir).map_err(|source| StoreError::Io {
        context: "inspect PG directory for shard inventory",
        source,
    })?;
    if pg_metadata.file_type().is_symlink() || !pg_metadata.is_dir() {
        return Err(StoreError::ShardScavengerScanIncomplete {
            context: "inspect PG shard inventory",
            errors: "PG path is not a real directory".to_string(),
        });
    }
    let shards_dir = pg_dir.join("shards");
    let shards_metadata = fs::symlink_metadata(&shards_dir).map_err(|source| StoreError::Io {
        context: "inspect shard root for PG inventory",
        source,
    })?;
    if shards_metadata.file_type().is_symlink() || !shards_metadata.is_dir() {
        return Err(StoreError::ShardScavengerScanIncomplete {
            context: "inspect PG shard inventory",
            errors: "shard root is not a real directory".to_string(),
        });
    }
    if pg_metadata.dev() != shards_metadata.dev() {
        return Err(StoreError::ShardScavengerScanIncomplete {
            context: "inspect PG shard inventory",
            errors: "shard root is on a different filesystem from its PG directory".to_string(),
        });
    }
    let conn = open_existing_pg_database_read_only(pg_dir, pg_id)?;
    require_current_pg_schema(&conn)?;
    let store = PgStore {
        pg_id,
        shards_dir,
        tmp_dir: pg_dir.join("tmp"),
        conn,
        clean_metadata_digest_revision: AtomicU64::new(UNCLEAN_METADATA_DIGEST_REVISION),
        #[cfg(test)]
        fail_next_metadata_txn_commit: std::sync::atomic::AtomicBool::new(false),
        #[cfg(test)]
        metadata_command_log_prefix_fast_path_hits: AtomicU64::new(0),
        #[cfg(test)]
        metadata_command_log_replay_validation_entries: AtomicU64::new(0),
    };
    let rows = store.list_shard_inventory_rows()?;
    let scan = store.list_scavenger_shard_files()?;
    if !scan.errors.is_empty() {
        return Err(StoreError::ShardScavengerScanIncomplete {
            context: "inspect PG shard inventory",
            errors: scan.errors.join("; "),
        });
    }

    let mut inspection = PgShardInventoryInspection {
        shard_row_count: rows.len(),
        shard_file_count: scan.files.len(),
        ..PgShardInventoryInspection::default()
    };
    let mut row_index = 0;
    let mut file_index = 0;
    while row_index < rows.len() && file_index < scan.files.len() {
        let row = &rows[row_index];
        let file = &scan.files[file_index];
        match row.key.as_bytes().cmp(file.key.as_bytes()) {
            std::cmp::Ordering::Less => {
                if row.status == ShardStatus::Deleting {
                    inspection.recoverable_missing_file_count += 1;
                } else {
                    inspection.authoritative_missing_file_count += 1;
                }
                row_index += 1;
            }
            std::cmp::Ordering::Greater => {
                inspection.recoverable_unindexed_file_count += 1;
                file_index += 1;
            }
            std::cmp::Ordering::Equal => {
                if row.ack.stored_size != file.size {
                    if row.status == ShardStatus::Deleting {
                        inspection.recoverable_size_mismatch_count += 1;
                    } else {
                        inspection.authoritative_size_mismatch_count += 1;
                    }
                }
                row_index += 1;
                file_index += 1;
            }
        }
    }
    for row in &rows[row_index..] {
        if row.status == ShardStatus::Deleting {
            inspection.recoverable_missing_file_count += 1;
        } else {
            inspection.authoritative_missing_file_count += 1;
        }
    }
    inspection.recoverable_unindexed_file_count += scan.files.len() - file_index;
    Ok(inspection)
}

fn validate_pg_durable_identity_bytes(pg_id: u32, identity_bytes: &[u8]) -> Result<(), StoreError> {
    if identity_bytes.is_empty() || identity_bytes.len() > MAX_PG_DURABLE_IDENTITY_BYTES {
        return Err(StoreError::PgDurableIdentityInvalid {
            pg_id,
            reason: format!(
                "identity length {} is outside 1..={MAX_PG_DURABLE_IDENTITY_BYTES}",
                identity_bytes.len()
            ),
        });
    }
    Ok(())
}

pub(crate) fn sync_initialized_pg_store_layout(
    pg_dir: &Path,
    pg_id: u32,
) -> Result<(), StoreError> {
    open_and_validate_existing_pg_database_file(pg_dir, pg_id)?
        .sync_all()
        .map_err(|source| StoreError::Io {
            context: "sync initialized PG metadata store",
            source,
        })?;
    File::open(pg_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| StoreError::Io {
            context: "sync initialized PG directory",
            source,
        })
}

fn open_existing_pg_database_read_only(
    pg_dir: &Path,
    pg_id: u32,
) -> Result<Connection, StoreError> {
    drop(open_and_validate_existing_pg_database_file(pg_dir, pg_id)?);
    Connection::open_with_flags(
        pg_dir.join("metadata.db"),
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )
    .map_err(|source| StoreError::Db {
        context: "open existing PG database read-only",
        source: source.into(),
    })
}

fn open_and_validate_existing_pg_database_file(
    pg_dir: &Path,
    pg_id: u32,
) -> Result<File, StoreError> {
    let pg_metadata =
        fs::symlink_metadata(pg_dir).map_err(|source| StoreError::PgDurableIdentityInvalid {
            pg_id,
            reason: format!("PG directory is unavailable: {source}"),
        })?;
    if pg_metadata.file_type().is_symlink() || !pg_metadata.is_dir() {
        return Err(StoreError::PgDurableIdentityInvalid {
            pg_id,
            reason: "PG path is not a real directory".to_string(),
        });
    }

    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let mut file = options.open(pg_dir.join("metadata.db")).map_err(|source| {
        StoreError::PgDurableIdentityInvalid {
            pg_id,
            reason: format!("metadata store is unavailable: {source}"),
        }
    })?;
    let metadata = file
        .metadata()
        .map_err(|source| StoreError::PgDurableIdentityInvalid {
            pg_id,
            reason: format!("metadata store cannot be inspected: {source}"),
        })?;
    if !metadata.is_file() {
        return Err(StoreError::PgDurableIdentityInvalid {
            pg_id,
            reason: "metadata store is not a regular file".to_string(),
        });
    }
    let mut magic = [0_u8; SQLITE_FILE_MAGIC.len()];
    file.read_exact(&mut magic)
        .map_err(|source| StoreError::PgDurableIdentityInvalid {
            pg_id,
            reason: format!("metadata store has a truncated identity: {source}"),
        })?;
    if magic != *SQLITE_FILE_MAGIC {
        return Err(StoreError::PgDurableIdentityInvalid {
            pg_id,
            reason: "metadata store has an invalid identity".to_string(),
        });
    }
    Ok(file)
}

impl PgStore {
    /// Open (or create) a PG store at the given directory.
    ///
    /// Creates `shards/` and `tmp/` subdirectories if they don't exist.
    /// Initializes the SQLite schema (idempotent).
    pub fn open(pg_dir: &Path, pg_id: u32) -> Result<Self, StoreError> {
        Self::open_with_initial_cluster_epoch(pg_dir, pg_id, ClusterEpoch::INITIAL)
    }

    pub(crate) fn open_with_initial_cluster_epoch(
        pg_dir: &Path,
        pg_id: u32,
        initial_cluster_epoch: ClusterEpoch,
    ) -> Result<Self, StoreError> {
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
            source: e.into(),
        })?;
        conn.set_prepared_statement_cache_capacity(PG_STORE_STATEMENT_CACHE_CAPACITY);
        install_sqlite_profile_hook(&conn);

        conn.execute_batch("PRAGMA recursive_triggers = ON")
            .map_err(|e| StoreError::Db {
                context: "enable recursive pg database triggers",
                source: e.into(),
            })?;
        command_log::register_metadata_digest_sql_functions(&conn).map_err(|e| StoreError::Db {
            context: "register metadata digest SQL functions",
            source: e.into(),
        })?;
        init_pg_schema(&conn)?;
        conn.busy_timeout(std::time::Duration::from_millis(0))
            .map_err(|e| StoreError::Db {
                context: "configure pg database busy timeout",
                source: e.into(),
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
            store.ensure_metadata_command_replica_state(initial_cluster_epoch)?;
            Ok(store)
        })
    }

    /// Return the PG ID.
    pub fn pg_id(&self) -> u32 {
        self.pg_id
    }

    #[cfg(test)]
    pub(crate) fn test_set_busy_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), StoreError> {
        self.conn
            .busy_timeout(timeout)
            .map_err(|source| StoreError::Db {
                context: "configure pg database busy timeout for test",
                source: source.into(),
            })
    }

    #[cfg(test)]
    pub(crate) fn test_sqlite_configuration(&self) -> Result<TestSqliteConfiguration, StoreError> {
        let temp_store = self
            .conn
            .query_row("PRAGMA temp_store", [], |row| row.get(0))
            .map_err(|source| StoreError::Db {
                context: "inspect SQLite temp store configuration for test",
                source: source.into(),
            })?;
        let journal_mode = self
            .conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .map_err(|source| StoreError::Db {
                context: "inspect SQLite journal mode configuration for test",
                source: source.into(),
            })?;
        let synchronous = self
            .conn
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .map_err(|source| StoreError::Db {
                context: "inspect SQLite synchronous configuration for test",
                source: source.into(),
            })?;
        Ok(TestSqliteConfiguration {
            temp_store,
            journal_mode,
            synchronous,
        })
    }

    pub fn cluster_map_history_reference_summary(
        &self,
    ) -> Result<PgClusterMapHistoryReferenceSummary, StoreError> {
        Ok(PgClusterMapHistoryReferenceSummary {
            oldest_live_placement_epoch: self.oldest_live_payload_placement_epoch()?,
            oldest_durable_backfill_epoch: self.oldest_durable_backfill_epoch()?,
            oldest_pending_metadata_command_epoch: self.oldest_pending_metadata_command_epoch()?,
            oldest_object_payload_reclaim_claim_epoch: self
                .oldest_object_payload_reclaim_claim_epoch()?,
        })
    }

    pub fn cluster_map_history_route_references(
        &self,
        _pg_topology: &crate::pg_topology::PgTopology,
    ) -> Result<PgClusterMapHistoryRouteReferences, StoreError> {
        let mut references = PgClusterMapHistoryRouteReferences::default();
        self.extend_direct_cluster_map_history_route_references(
            &mut references,
            "SELECT DISTINCT data_pg_id, placement_cluster_epoch FROM ( \
                 SELECT data_pg_id, placement_cluster_epoch FROM object_segments \
                 UNION ALL SELECT data_pg_id, placement_cluster_epoch FROM multipart_part_segments \
                 UNION ALL SELECT data_pg_id, placement_cluster_epoch FROM stream_upload_segments \
             )",
            PgClusterMapHistoryRouteReferenceKind::LivePlacement,
            "list live payload cluster-map history route references",
        )?;
        self.extend_direct_cluster_map_history_route_references(
            &mut references,
            "SELECT DISTINCT data_pg_id, source_cluster_epoch \
             FROM placed_segment_shard_backfills",
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
            "list durable backfill source cluster-map history route references",
        )?;
        self.extend_direct_cluster_map_history_route_references(
            &mut references,
            "SELECT DISTINCT data_pg_id, desired_cluster_epoch \
             FROM placed_segment_shard_backfills",
            PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
            "list durable backfill desired cluster-map history route references",
        )?;
        self.extend_direct_cluster_map_history_route_references(
            &mut references,
            "SELECT DISTINCT pg_id, cluster_epoch FROM metadata_command_pending_slot",
            PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand,
            "list pending metadata command cluster-map history route references",
        )?;
        self.extend_direct_cluster_map_history_route_references(
            &mut references,
            "SELECT DISTINCT pg_id, cluster_epoch FROM object_payload_reclaim_claims",
            PgClusterMapHistoryRouteReferenceKind::ObjectPayloadReclaimClaim,
            "list object payload reclaim claim cluster-map history route references",
        )?;
        Ok(references)
    }

    fn extend_direct_cluster_map_history_route_references(
        &self,
        references: &mut PgClusterMapHistoryRouteReferences,
        sql: &str,
        kind: PgClusterMapHistoryRouteReferenceKind,
        context: &'static str,
    ) -> Result<(), StoreError> {
        let mut stmt = self
            .conn
            .prepare_cached(sql)
            .map_err(|source| StoreError::Db {
                context,
                source: source.into(),
            })?;
        let rows = stmt
            .query_map([], |row| Ok((row.get::<_, u32>(0)?, row.get::<_, i64>(1)?)))
            .map_err(|source| StoreError::Db {
                context,
                source: source.into(),
            })?;
        for row in rows {
            let (pg_id, raw_epoch) = row.map_err(|source| StoreError::Db {
                context,
                source: source.into(),
            })?;
            references.insert(PgClusterMapHistoryRouteReference::new(
                kind,
                Self::parse_cluster_epoch(raw_epoch, 1, "cluster-map history route epoch")
                    .map_err(|source| StoreError::Db {
                        context,
                        source: source.into(),
                    })?,
                PgId::new(pg_id),
            ))?;
        }
        Ok(())
    }

    fn oldest_live_payload_placement_epoch(&self) -> Result<Option<ClusterEpoch>, StoreError> {
        let raw_epoch = self.query_row_cached(
            "SELECT MIN(placement_cluster_epoch) FROM ( \
                 SELECT placement_cluster_epoch FROM object_segments \
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

    fn oldest_pending_metadata_command_epoch(&self) -> Result<Option<ClusterEpoch>, StoreError> {
        let raw_epoch = self.query_row_cached(
            "SELECT MIN(cluster_epoch) FROM metadata_command_pending_slot",
            [],
            "compute oldest pending metadata command epoch",
            |row| row.get::<_, Option<i64>>(0),
        )?;
        parse_optional_cluster_epoch(raw_epoch, "oldest pending metadata command epoch")
    }

    fn oldest_object_payload_reclaim_claim_epoch(
        &self,
    ) -> Result<Option<ClusterEpoch>, StoreError> {
        let raw_epoch = self.query_row_cached(
            "SELECT MIN(cluster_epoch) FROM object_payload_reclaim_claims",
            [],
            "compute oldest object payload reclaim claim epoch",
            |row| row.get::<_, Option<i64>>(0),
        )?;
        parse_optional_cluster_epoch(raw_epoch, "oldest object payload reclaim claim epoch")
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
            .map_err(|e| StoreError::Db {
                context,
                source: e.into(),
            })
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
            .map_err(|e| StoreError::Db {
                context,
                source: e.into(),
            })
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
            .map_err(|e| StoreError::Db {
                context,
                source: e.into(),
            })
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
            .map_err(|e| MetadataError::Db {
                context,
                source: e.into(),
            })
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
            .map_err(|e| MetadataError::Db {
                context,
                source: e.into(),
            })
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
            .map_err(|e| MetadataError::Db {
                context,
                source: e.into(),
            })
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

#[cfg(test)]
mod durable_identity_tests {
    use super::*;

    const TEST_IDENTITY: &[u8] = b"test-static-cluster-identity";

    #[test]
    fn pg_durable_identity_rejects_placeholder_metadata_store() {
        let temp = test_util::tempdir();
        fs::write(temp.path().join("metadata.db"), b"").unwrap();

        let error = verify_pg_durable_identity(temp.path(), 3, TEST_IDENTITY).unwrap_err();

        assert!(matches!(
            error,
            StoreError::PgDurableIdentityInvalid { pg_id: 3, reason }
                if reason.contains("truncated identity")
        ));
    }

    #[test]
    fn pg_durable_identity_rejects_symlinked_metadata_store() {
        let temp = test_util::tempdir();
        initialize_pg_durable_identity(temp.path(), 3, TEST_IDENTITY).unwrap();
        let external = temp.path().join("external-metadata");
        fs::rename(temp.path().join("metadata.db"), &external).unwrap();
        std::os::unix::fs::symlink(&external, temp.path().join("metadata.db")).unwrap();

        let error = verify_pg_durable_identity(temp.path(), 3, TEST_IDENTITY).unwrap_err();

        assert!(matches!(
            error,
            StoreError::PgDurableIdentityInvalid { pg_id: 3, reason }
                if reason.contains("metadata store is unavailable")
        ));
    }

    #[test]
    fn pg_durable_identity_rejects_unversioned_sqlite_database() {
        let temp = test_util::tempdir();
        fs::create_dir(temp.path().join("shards")).unwrap();
        let conn = Connection::open(temp.path().join("metadata.db")).unwrap();
        conn.execute_batch("CREATE TABLE unrelated (id INTEGER PRIMARY KEY) STRICT")
            .unwrap();
        drop(conn);

        let error = verify_pg_durable_identity(temp.path(), 3, TEST_IDENTITY).unwrap_err();

        assert!(matches!(error, StoreError::PgSchemaInvalid { .. }));
    }

    #[test]
    fn pg_shard_inventory_reports_database_rows_whose_payload_files_are_missing() {
        let temp = test_util::tempdir();
        initialize_pg_durable_identity(temp.path(), 3, TEST_IDENTITY).unwrap();
        let store = PgStore::open(temp.path(), 3).unwrap();
        let key = ShardKey::new(&[7; 16], 11, 0);
        store.write_shard(&key, b"durable shard payload").unwrap();
        drop(store);

        let complete = inspect_pg_shard_inventory(temp.path(), 3).unwrap();
        assert!(complete.authoritative_inventory_is_complete());
        assert!(!complete.has_recoverable_residue());
        fs::remove_file(PgStore::shard_path_for_shards_dir(
            &temp.path().join("shards"),
            &key,
        ))
        .unwrap();

        let incomplete = inspect_pg_shard_inventory(temp.path(), 3).unwrap();
        assert_eq!(
            incomplete,
            PgShardInventoryInspection {
                shard_row_count: 1,
                shard_file_count: 0,
                authoritative_missing_file_count: 1,
                authoritative_size_mismatch_count: 0,
                recoverable_missing_file_count: 0,
                recoverable_size_mismatch_count: 0,
                recoverable_unindexed_file_count: 0,
            }
        );
    }

    #[test]
    fn storage_node_state_inspection_aggregates_incomplete_pg_payloads() {
        let temp = test_util::tempdir();
        let pg_dir = temp.path().join("pg-0003");
        initialize_pg_durable_identity(&pg_dir, 3, TEST_IDENTITY).unwrap();
        let store = PgStore::open(&pg_dir, 3).unwrap();
        let key = ShardKey::new(&[7; 16], 11, 0);
        store.write_shard(&key, b"durable shard payload").unwrap();
        drop(store);
        fs::remove_file(PgStore::shard_path_for_shards_dir(
            &pg_dir.join("shards"),
            &key,
        ))
        .unwrap();

        let inspection = crate::storage_node_server::inspect_initialized_storage_node_state(
            temp.path(),
            &[3],
            TEST_IDENTITY,
        )
        .unwrap();

        assert_eq!(inspection.incomplete_payload_pg_ids(), &[3]);
    }

    #[test]
    fn pg_shard_inventory_classifies_unindexed_files_as_recoverable_crash_residue() {
        let temp = test_util::tempdir();
        initialize_pg_durable_identity(temp.path(), 3, TEST_IDENTITY).unwrap();
        let key = ShardKey::new(&[8; 16], 12, 0);
        PgStore::write_shard_file_durable(
            &temp.path().join("tmp"),
            &temp.path().join("shards"),
            &key,
            b"durable unregistered shard payload",
        )
        .unwrap();

        let inspection = inspect_pg_shard_inventory(temp.path(), 3).unwrap();

        assert!(inspection.authoritative_inventory_is_complete());
        assert!(inspection.has_recoverable_residue());
        assert_eq!(inspection.recoverable_unindexed_file_count, 1);
    }

    #[test]
    fn pg_shard_inventory_classifies_missing_deleting_file_as_recoverable_crash_residue() {
        let temp = test_util::tempdir();
        initialize_pg_durable_identity(temp.path(), 3, TEST_IDENTITY).unwrap();
        let store = PgStore::open(temp.path(), 3).unwrap();
        let key = ShardKey::new(&[9; 16], 13, 0);
        store.write_shard(&key, b"shard being deleted").unwrap();
        store
            .conn
            .execute(
                "UPDATE shards SET status = ?1 WHERE shard_key = ?2",
                params![ShardStatus::Deleting as u8, key.as_bytes().as_slice()],
            )
            .unwrap();
        fs::remove_file(PgStore::shard_path_for_shards_dir(
            &temp.path().join("shards"),
            &key,
        ))
        .unwrap();
        drop(store);

        let inspection = inspect_pg_shard_inventory(temp.path(), 3).unwrap();

        assert!(inspection.authoritative_inventory_is_complete());
        assert!(inspection.has_recoverable_residue());
        assert_eq!(inspection.recoverable_missing_file_count, 1);
    }

    #[test]
    fn pg_shard_inventory_reports_truncated_authoritative_payload() {
        let temp = test_util::tempdir();
        initialize_pg_durable_identity(temp.path(), 3, TEST_IDENTITY).unwrap();
        let store = PgStore::open(temp.path(), 3).unwrap();
        let key = ShardKey::new(&[10; 16], 14, 0);
        store.write_shard(&key, b"complete shard payload").unwrap();
        fs::write(
            PgStore::shard_path_for_shards_dir(&temp.path().join("shards"), &key),
            b"short",
        )
        .unwrap();
        drop(store);

        let inspection = inspect_pg_shard_inventory(temp.path(), 3).unwrap();

        assert!(!inspection.authoritative_inventory_is_complete());
        assert_eq!(inspection.authoritative_size_mismatch_count, 1);
        assert!(!inspection.has_recoverable_residue());
    }

    #[test]
    fn pg_shard_inventory_rejects_symlinked_shard_root() {
        let temp = test_util::tempdir();
        initialize_pg_durable_identity(temp.path(), 3, TEST_IDENTITY).unwrap();
        let external = test_util::tempdir();
        fs::remove_dir(temp.path().join("shards")).unwrap();
        std::os::unix::fs::symlink(external.path(), temp.path().join("shards")).unwrap();

        let error = inspect_pg_shard_inventory(temp.path(), 3).unwrap_err();

        assert!(matches!(
            error,
            StoreError::ShardScavengerScanIncomplete {
                context: "inspect PG shard inventory",
                ..
            }
        ));
    }
}
