/// Storage layer error types.
use std::path::PathBuf;

use crate::types::{ClusterEpoch, PgState};

/// Shard-level storage errors.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("shard not found")]
    NotFound,

    #[error("integrity error: expected CRC {expected:#018X}, got {actual:#018X}")]
    IntegrityError { expected: u64, actual: u64 },

    #[error(
        "shard {shard} ack mismatch: expected size {expected_size} CRC {expected_crc:#018X}, got size {actual_size} CRC {actual_crc:#018X}"
    )]
    ShardAckMismatch {
        shard: crate::types::ShardKey,
        expected_size: u64,
        expected_crc: u64,
        actual_size: u64,
        actual_crc: u64,
    },

    #[error("payload shard set mismatch: {reason}")]
    PayloadShardSetMismatch { reason: String },

    #[error("PG {pg_id} not found on this node")]
    PgNotFound { pg_id: u32 },

    #[error("PG {pg_id} is not present in cluster epoch {cluster_epoch}")]
    ClusterPgNotFound {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error("PG {pg_id} for local node {node_id} is not present in cluster epoch {cluster_epoch}")]
    ShardPgNotFound {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error("PG {pg_id} is {state} in cluster epoch {cluster_epoch}")]
    PgNotActive {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },

    #[error("PG {pg_id} for local node {node_id} is {state} in cluster epoch {cluster_epoch}")]
    ShardPgNotActive {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },

    #[error(
        "shard store failed on local node {node_id} PG {pg_id} epoch {cluster_epoch}: {source}"
    )]
    ShardStore {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        #[source]
        source: Box<StoreError>,
    },

    #[error("storage RPC {operation} failed on node {node_id}: {message}")]
    StorageRpc {
        node_id: u32,
        operation: &'static str,
        message: String,
    },

    #[error(
        "payload operation for PG {pg_id} has cluster epoch {operation_epoch}, current cluster epoch is {current_epoch}"
    )]
    StalePayloadOperation {
        pg_id: u32,
        operation_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error(
        "metadata-primary bridge for local node {metadata_node_id} has operation epoch {operation_epoch}, current cluster epoch is {current_epoch}"
    )]
    StaleMetadataPrimaryBridge {
        metadata_node_id: u32,
        operation_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error(
        "metadata operation for PG {pg_id} has cluster epoch {operation_epoch}, current cluster epoch is {current_epoch}"
    )]
    StaleMetadataOperation {
        pg_id: u32,
        operation_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error(
        "metadata route for PG {pg_id} has cluster epoch {route_epoch}, current cluster epoch is {current_epoch}"
    )]
    StaleMetadataRoute {
        pg_id: u32,
        route_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error(
        "metadata command for local node {node_id} PG {pg_id} has epoch {command_epoch}, current cluster epoch is {current_epoch}"
    )]
    StaleMetadataCommand {
        node_id: u32,
        pg_id: u32,
        command_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error(
        "metadata command for local node {node_id} targets PG {command_pg_id}, but was delivered to PG {target_pg_id} in epoch {cluster_epoch}"
    )]
    MetadataCommandWrongPg {
        node_id: u32,
        command_pg_id: u32,
        target_pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "metadata command for local node {node_id} PG {pg_id} epoch {cluster_epoch} came from node {origin_node_id}, expected primary node {primary_node_id}"
    )]
    MetadataCommandFromNonPrimary {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        origin_node_id: u32,
        primary_node_id: u32,
    },

    #[error(
        "metadata command for local node {node_id} PG {pg_id} epoch {cluster_epoch} conflicts with already-applied log index {log_index}"
    )]
    MetadataCommandLogConflict {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
    },

    #[error(
        "metadata command pending slot for PG {pg_id} epoch {cluster_epoch} already contains log index {existing_log_index}, cannot install log index {candidate_log_index}"
    )]
    MetadataCommandPendingConflict {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        existing_log_index: u64,
        candidate_log_index: u64,
    },

    #[error(
        "metadata command pending slot for PG {pg_id} epoch {cluster_epoch} exists on local node {node_id}, expected primary node {primary_node_id}"
    )]
    MetadataCommandPendingOnNonPrimary {
        node_id: u32,
        primary_node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "metadata command log checksum mismatch for local node {node_id} PG {pg_id} epoch {cluster_epoch} log index {log_index}: stored checksum {stored_checksum:#018X}, computed checksum {computed_checksum:#018X}"
    )]
    MetadataCommandLogChecksumMismatch {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
        stored_checksum: u64,
        computed_checksum: u64,
    },

    #[error(
        "metadata command log hash mismatch for local node {node_id} PG {pg_id} epoch {cluster_epoch} log index {log_index}: expected previous hash {expected_previous_log_hash:#018X} and log hash {expected_log_hash:#018X}, found previous hash {actual_previous_log_hash:#018X} and log hash {actual_log_hash:#018X}"
    )]
    MetadataCommandLogHashMismatch {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
        expected_previous_log_hash: u64,
        actual_previous_log_hash: u64,
        expected_log_hash: u64,
        actual_log_hash: u64,
    },

    #[error("metadata command replica state is missing for PG {pg_id}")]
    MetadataCommandReplicaStateMissing { pg_id: u32 },

    #[error(
        "metadata command replica state diverged for PG {pg_id}: local node {node_id} has epoch {cluster_epoch}, log index {applied_log_index}, log hash {applied_log_hash:#018X}, digest {state_digest:#018X}; reference local node {reference_node_id} has epoch {reference_cluster_epoch}, log index {reference_applied_log_index}, log hash {reference_applied_log_hash:#018X}, digest {reference_state_digest:#018X}"
    )]
    MetadataCommandReplicaStateDiverged {
        node_id: u32,
        reference_node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        reference_cluster_epoch: ClusterEpoch,
        applied_log_index: u64,
        reference_applied_log_index: u64,
        applied_log_hash: u64,
        reference_applied_log_hash: u64,
        state_digest: u64,
        reference_state_digest: u64,
    },

    #[error(
        "metadata state digest mismatch for local node {node_id} PG {pg_id} epoch {cluster_epoch}: expected {expected_digest:#018X}, got {actual_digest:#018X}"
    )]
    MetadataStateDigestMismatch {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        expected_digest: u64,
        actual_digest: u64,
    },

    #[error(
        "shard operation for local node {node_id} PG {pg_id} has epoch {operation_epoch}, current cluster epoch is {current_epoch}"
    )]
    StaleShardOperation {
        node_id: u32,
        pg_id: u32,
        operation_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error(
        "shard location for local node {node_id} PG {pg_id} has epoch {location_epoch}, current cluster epoch is {current_epoch}"
    )]
    StaleShardLocation {
        node_id: u32,
        pg_id: u32,
        location_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error(
        "shard location references unknown local node {node_id} for PG {pg_id} in epoch {cluster_epoch}"
    )]
    NodeNotFound {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "local node {node_id} is not in the acting set for PG {pg_id} in cluster epoch {cluster_epoch}"
    )]
    NodeNotInActingSet {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "shard location for local node {node_id} PG {pg_id} epoch {cluster_epoch} has index {location_shard_index}, shard key has index {key_shard_index}"
    )]
    ShardIndexMismatch {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        location_shard_index: u8,
        key_shard_index: u8,
    },

    #[error(
        "shard scavenger observation targets data PG {observation_pg_id}, but was recorded on PG {store_pg_id}"
    )]
    ShardScavengerObservationWrongPg {
        store_pg_id: u32,
        observation_pg_id: u32,
    },

    #[error(
        "shard scavenger observation shard index {observation_shard_index} does not match shard key index {key_shard_index}"
    )]
    ShardScavengerObservationShardIndexMismatch {
        observation_shard_index: u8,
        key_shard_index: u8,
    },

    #[error(
        "shard scavenger observation reason {reason:?} is inconsistent with file_exists={file_exists} shard_row_exists={shard_row_exists}"
    )]
    ShardScavengerObservationInconsistentReason {
        reason: crate::types::ShardScavengerObservationReason,
        file_exists: bool,
        shard_row_exists: bool,
    },

    #[error("invalid shard key length: {len} (expected {expected})")]
    InvalidKeyLength { len: usize, expected: usize },

    #[error("invalid shard key hex")]
    InvalidShardKeyHex,

    #[error("shard scavenger scan incomplete during {context}: {errors}")]
    ShardScavengerScanIncomplete {
        context: &'static str,
        errors: String,
    },

    #[error("IO error: {context}")]
    Io {
        context: &'static str,
        #[source]
        source: std::io::Error,
    },

    #[error("database error: {context}: {source}")]
    Db {
        context: &'static str,
        #[source]
        source: rusqlite::Error,
    },

    #[error("erasure coding error: {context}: {reason}")]
    ErasureCoding {
        context: &'static str,
        reason: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ShardIoError {
    #[error(
        "shard operation for local node {node_id} PG {pg_id} has epoch {operation_epoch}, current cluster epoch is {current_epoch}"
    )]
    StaleOperationEpoch {
        node_id: u32,
        pg_id: u32,
        operation_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error(
        "shard location for local node {node_id} PG {pg_id} has epoch {location_epoch}, current cluster epoch is {current_epoch}"
    )]
    StaleLocation {
        node_id: u32,
        pg_id: u32,
        location_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error("shard location references unknown local node {node_id} for PG {pg_id} in epoch {cluster_epoch}")]
    NodeNotFound {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error("PG {pg_id} for local node {node_id} is not present in cluster epoch {cluster_epoch}")]
    PgNotFound {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error("PG {pg_id} for local node {node_id} is {state} in cluster epoch {cluster_epoch}")]
    PgNotActive {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },

    #[error(
        "local node {node_id} is not in the acting set for PG {pg_id} in cluster epoch {cluster_epoch}"
    )]
    NodeNotInActingSet {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "shard location for local node {node_id} PG {pg_id} epoch {cluster_epoch} has index {location_shard_index}, shard key has index {key_shard_index}"
    )]
    ShardIndexMismatch {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        location_shard_index: u8,
        key_shard_index: u8,
    },

    #[error("shard IO failed on local node {node_id} PG {pg_id} epoch {cluster_epoch}: {source}")]
    Store {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        #[source]
        source: StoreError,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ClusterBuildError {
    #[error("cluster map must contain at least one local node")]
    EmptyCluster,

    #[error("duplicate local node id {id}")]
    DuplicateNodeId { id: u32 },

    #[error("duplicate remote shard client node id {id}")]
    DuplicateRemoteShardClientNodeId { id: u32 },

    #[error("remote shard client node {id} is not present in the local cluster map")]
    RemoteShardClientNodeNotFound { id: u32 },

    #[error("remote shard client socket path {path:?} must be absolute")]
    RemoteShardClientSocketPathNotAbsolute { path: PathBuf },

    #[error("metadata primary node {id} is not present in the local cluster map")]
    MetadataPrimaryNotFound { id: u32 },

    #[error("local cluster map must contain at least one PG")]
    EmptyPgSet,

    #[error("duplicate PG id {pg_id}")]
    DuplicatePgId { pg_id: u32 },

    #[error("invalid EC shape k={data_shards} m={parity_shards}: {reason}")]
    InvalidEcShape {
        data_shards: u8,
        parity_shards: u8,
        reason: String,
    },

    #[error(
        "local cluster with {node_count} active nodes cannot place EC shape k={data_shards} m={parity_shards}; at least {required_nodes} distinct nodes are required"
    )]
    UnplaceableEcShape {
        data_shards: u8,
        parity_shards: u8,
        required_nodes: usize,
        node_count: usize,
    },

    #[error("invalid local placement map: {reason}")]
    InvalidLocalPlacement { reason: String },

    #[error("shard index {shard_index} is outside EC shape k={data_shards} m={parity_shards}")]
    InvalidShardIndex {
        data_shards: u8,
        parity_shards: u8,
        shard_index: u8,
    },

    #[error(
        "payload placement for PG {pg_id} has cluster epoch {operation_epoch}, current cluster epoch is {current_epoch}"
    )]
    StalePayloadPlacement {
        pg_id: u32,
        operation_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error("PG {pg_id} is not present in cluster epoch {cluster_epoch}")]
    PgNotFound {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error("PG {pg_id} is {state} in cluster epoch {cluster_epoch}")]
    PgNotActive {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },

    #[error(
        "local node {duplicate_node_id} shares data directory {data_dir:?} with local node {first_node_id}"
    )]
    DuplicateDataDir {
        first_node_id: u32,
        duplicate_node_id: u32,
        data_dir: PathBuf,
    },

    #[error("failed to open local node {node_id}: {source}")]
    OpenLocalNode {
        node_id: u32,
        #[source]
        source: StoreError,
    },
}

/// Metadata-level errors (object records, bucket operations).
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("bucket not found: {name}")]
    BucketNotFound { name: crate::types::BucketName },

    #[error("invalid bucket name: {reason}")]
    InvalidBucketName { reason: String },

    #[error("invalid object key: {reason}")]
    InvalidObjectKey { reason: String },

    #[error("bucket already exists")]
    BucketAlreadyExists,

    #[error("bucket not empty")]
    BucketNotEmpty,

    #[error("bucket is not finalized for delete: state {state:?}")]
    BucketNotFinalizedForDelete { state: crate::types::BucketState },

    #[error("bucket write reservations are draining")]
    BucketWriteDraining,

    #[error("bucket write reservation conflict: {reservation_id}")]
    BucketWriteReservationConflict { reservation_id: String },

    #[error("bucket write reservation not found: {reservation_id}")]
    BucketWriteReservationNotFound { reservation_id: String },

    #[error("bucket write drain conflict: {drain_id}")]
    BucketWriteDrainConflict { drain_id: String },

    #[error("bucket write drain not found: {drain_id}")]
    BucketWriteDrainNotFound { drain_id: String },

    #[error("reclaim claim conflict: {claim_id}")]
    ReclaimClaimConflict { claim_id: String },

    #[error("reclaim claim not found: {claim_id}")]
    ReclaimClaimNotFound { claim_id: String },

    #[error("object not found")]
    ObjectNotFound,

    #[error("method not allowed on delete marker")]
    MethodNotAllowedOnDeleteMarker,

    #[error("invalid versioning transition from {from:?} to {to:?}")]
    InvalidVersioningTransition {
        from: crate::types::BucketVersioningState,
        to: crate::types::BucketVersioningState,
    },

    #[error("multipart upload not found: {upload_id}")]
    NoSuchUpload { upload_id: String },

    #[error("upload not in InProgress state (current: {state})")]
    UploadNotInProgress { state: u8 },

    #[error("multipart part not found: upload={upload_id} part={part_number}")]
    PartNotFound { upload_id: String, part_number: u32 },

    #[error("stream session not found: {session_id}")]
    StreamSessionNotFound { session_id: String },

    #[error("stream session not in InProgress state (current: {state})")]
    StreamSessionNotInProgress { state: u8 },

    #[error("object generation reservation not found: {reservation_id}")]
    ObjectGenerationReservationNotFound { reservation_id: String },

    #[error("not implemented: {context}")]
    NotImplemented { context: &'static str },

    #[error("database error: {context}: {source}")]
    Db {
        context: &'static str,
        #[source]
        source: rusqlite::Error,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum BucketSnapshotLoadError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
}

#[derive(Debug, thiserror::Error)]
pub enum BucketWriteDrainError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectPgActionError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
    #[error("invalid request: {reason}")]
    InvalidRequest { reason: String },
    #[error("object read subject changed before snapshot load")]
    StaleObjectReadSubject,
    #[error("direct PUT commit snapshot changed before command build")]
    StaleDirectPutCommitSnapshot,
    #[error("stream finalize snapshot changed before command build")]
    StaleStreamFinalizeSnapshot,
    #[error("multipart completion snapshot changed before command build")]
    StaleMultipartCompletionSnapshot,
}
