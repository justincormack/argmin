/// Storage layer error types.
use std::path::PathBuf;

use crate::storage_rpc::StorageRpcErrorCode;
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

    #[error("object payload reclaim fence authority does not match the active reclaim")]
    ObjectPayloadReclaimFenceAuthorityMismatch,

    #[error("PG {pg_id} durable identity is invalid: {reason}")]
    PgDurableIdentityInvalid { pg_id: u32, reason: String },

    #[error("cluster-map history route reference count {count} exceeds maximum {max}")]
    ClusterMapHistoryReferenceLimitExceeded { count: usize, max: usize },

    #[error("PG {pg_id} not found on this node")]
    PgNotFound { pg_id: u32 },

    #[error("invalid PG topology: {source}")]
    InvalidPgTopology {
        #[source]
        source: crate::pg_topology::PgTopologyError,
    },

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

    #[error("storage RPC {operation} failed on node {node_id} with {code:?}: {message}")]
    StorageRpc {
        node_id: u32,
        operation: &'static str,
        code: StorageRpcErrorCode,
        message: String,
    },

    #[error("storage RPC {operation} on node {node_id} exhausted resources: {message}")]
    StorageRpcResourceExhausted {
        node_id: u32,
        operation: &'static str,
        message: String,
    },

    #[error(
        "storage RPC {operation} on node {node_id} found shard deletion in progress: {message}"
    )]
    StorageRpcShardDeleteInProgress {
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
        "runtime route map for cluster epoch {cluster_epoch} expired at {valid_until_ms}; current time is {now_ms}"
    )]
    RouteMapExpired {
        cluster_epoch: ClusterEpoch,
        valid_until_ms: u64,
        now_ms: u64,
    },

    #[error(
        "route admission for cluster epoch {admitted_epoch} was used with a different runtime-map generation at epoch {operation_epoch}"
    )]
    RouteAdmissionClusterMismatch {
        admitted_epoch: ClusterEpoch,
        operation_epoch: ClusterEpoch,
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
        "metadata command for local node {node_id} PG {pg_id} epoch {cluster_epoch} has non-contiguous log index {log_index}, expected {expected_log_index}"
    )]
    MetadataCommandLogGap {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        log_index: u64,
        expected_log_index: u64,
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

    #[error("metadata command contention during {context}")]
    MetadataCommandContention { context: &'static str },

    #[error(
        "metadata transfer adoption for PG {pg_id} epoch {cluster_epoch} requires at least one retained command"
    )]
    MetadataTransferEmpty {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
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
        "metadata transfer for local node {node_id} PG {pg_id} epoch {cluster_epoch} supplied unsupported unproven proof tuple log index {applied_log_index}, log hash {applied_log_hash:#018X}"
    )]
    MetadataTransferUnsupportedProof {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        applied_log_index: u64,
        applied_log_hash: u64,
    },

    #[error("metadata checkpoint for local node {node_id} PG {pg_id} epoch {cluster_epoch} is invalid: {reason}")]
    MetadataCheckpointInvalid {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        reason: String,
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

impl StoreError {
    /// Return a bounded diagnostic category suitable for metrics labels.
    #[must_use]
    pub fn diagnostic_kind(&self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::IntegrityError { .. } => "integrity_error",
            Self::ShardAckMismatch { .. } => "shard_ack_mismatch",
            Self::PayloadShardSetMismatch { .. } => "payload_shard_set_mismatch",
            Self::ObjectPayloadReclaimFenceAuthorityMismatch => {
                "object_payload_reclaim_fence_authority_mismatch"
            }
            Self::PgDurableIdentityInvalid { .. } => "pg_durable_identity_invalid",
            Self::ClusterMapHistoryReferenceLimitExceeded { .. } => {
                "cluster_map_history_reference_limit_exceeded"
            }
            Self::PgNotFound { .. } => "pg_not_found",
            Self::InvalidPgTopology { .. } => "invalid_pg_topology",
            Self::ClusterPgNotFound { .. } => "cluster_pg_not_found",
            Self::ShardPgNotFound { .. } => "shard_pg_not_found",
            Self::PgNotActive { .. } => "pg_not_active",
            Self::ShardPgNotActive { .. } => "shard_pg_not_active",
            Self::ShardStore { source, .. } => source.diagnostic_kind(),
            Self::StorageRpc { code, .. } => storage_rpc_error_diagnostic_kind(*code),
            Self::StorageRpcResourceExhausted { .. } => "storage_rpc_resource_exhausted",
            Self::StorageRpcShardDeleteInProgress { .. } => "storage_rpc_shard_delete_in_progress",
            Self::StalePayloadOperation { .. } => "stale_payload_operation",
            Self::StaleMetadataPrimaryBridge { .. } => "stale_metadata_primary_bridge",
            Self::StaleMetadataOperation { .. } => "stale_metadata_operation",
            Self::StaleMetadataRoute { .. } => "stale_metadata_route",
            Self::RouteMapExpired { .. } => "route_map_expired",
            Self::RouteAdmissionClusterMismatch { .. } => "route_admission_cluster_mismatch",
            Self::StaleMetadataCommand { .. } => "stale_metadata_command",
            Self::MetadataCommandWrongPg { .. } => "metadata_command_wrong_pg",
            Self::MetadataCommandFromNonPrimary { .. } => "metadata_command_from_non_primary",
            Self::MetadataCommandLogConflict { .. } => "metadata_command_log_conflict",
            Self::MetadataCommandLogGap { .. } => "metadata_command_log_gap",
            Self::MetadataCommandPendingConflict { .. } => "metadata_command_pending_conflict",
            Self::StaleShardOperation { .. } => "stale_shard_operation",
            Self::StaleShardLocation { .. } => "stale_shard_location",
            Self::MetadataCommandContention { .. } => "metadata_command_contention",
            Self::MetadataTransferEmpty { .. } => "metadata_transfer_empty",
            Self::MetadataCommandPendingOnNonPrimary { .. } => {
                "metadata_command_pending_on_non_primary"
            }
            Self::MetadataCommandLogChecksumMismatch { .. } => {
                "metadata_command_log_checksum_mismatch"
            }
            Self::MetadataCommandLogHashMismatch { .. } => "metadata_command_log_hash_mismatch",
            Self::MetadataCommandReplicaStateMissing { .. } => {
                "metadata_command_replica_state_missing"
            }
            Self::MetadataCommandReplicaStateDiverged { .. } => {
                "metadata_command_replica_state_diverged"
            }
            Self::MetadataStateDigestMismatch { .. } => "metadata_state_digest_mismatch",
            Self::MetadataTransferUnsupportedProof { .. } => "metadata_transfer_unsupported_proof",
            Self::MetadataCheckpointInvalid { .. } => "metadata_checkpoint_invalid",
            Self::NodeNotFound { .. } => "node_not_found",
            Self::NodeNotInActingSet { .. } => "node_not_in_acting_set",
            Self::ShardIndexMismatch { .. } => "shard_index_mismatch",
            Self::ShardScavengerObservationWrongPg { .. } => "shard_scavenger_observation_wrong_pg",
            Self::ShardScavengerObservationShardIndexMismatch { .. } => {
                "shard_scavenger_observation_shard_index_mismatch"
            }
            Self::ShardScavengerObservationInconsistentReason { .. } => {
                "shard_scavenger_observation_inconsistent_reason"
            }
            Self::InvalidKeyLength { .. } => "invalid_key_length",
            Self::InvalidShardKeyHex => "invalid_shard_key_hex",
            Self::ShardScavengerScanIncomplete { .. } => "shard_scavenger_scan_incomplete",
            Self::Io { context, .. }
                if matches!(
                    *context,
                    "connect storage-node RPC socket" | "connect storage-node RPC endpoint"
                ) =>
            {
                "storage_rpc_socket_connect"
            }
            Self::Io { .. } => "io",
            Self::Db { .. } => "db",
            Self::ErasureCoding { .. } => "erasure_coding",
        }
    }
}

fn storage_rpc_error_diagnostic_kind(code: StorageRpcErrorCode) -> &'static str {
    match code {
        StorageRpcErrorCode::FrameDecode => "storage_rpc_frame_decode",
        StorageRpcErrorCode::PayloadDecode => "storage_rpc_payload_decode",
        StorageRpcErrorCode::UnknownNode => "storage_rpc_unknown_node",
        StorageRpcErrorCode::UnknownPg => "storage_rpc_unknown_pg",
        StorageRpcErrorCode::WrongClusterEpoch => "storage_rpc_wrong_cluster_epoch",
        StorageRpcErrorCode::InactivePgRoute => "storage_rpc_inactive_pg_route",
        StorageRpcErrorCode::StaleShardLocation => "storage_rpc_stale_shard_location",
        StorageRpcErrorCode::NonActingSetAccess => "storage_rpc_non_acting_set_access",
        StorageRpcErrorCode::UnsupportedOperation => "storage_rpc_unsupported_operation",
        StorageRpcErrorCode::Internal => "storage_rpc_internal",
        StorageRpcErrorCode::ResourceExhausted => "storage_rpc_resource_exhausted",
        StorageRpcErrorCode::ReclaimClaimNotFound => "storage_rpc_reclaim_claim_not_found",
        StorageRpcErrorCode::ShardDeleteInProgress => "storage_rpc_shard_delete_in_progress",
        StorageRpcErrorCode::BucketWriteDrainConflict => "storage_rpc_bucket_write_drain_conflict",
        StorageRpcErrorCode::BucketWriteDrainNotFound => "storage_rpc_bucket_write_drain_not_found",
        StorageRpcErrorCode::ReclaimClaimConflict => "storage_rpc_reclaim_claim_conflict",
        StorageRpcErrorCode::NotFound => "storage_rpc_not_found",
        StorageRpcErrorCode::BucketWriteReservationConflict => {
            "storage_rpc_bucket_write_reservation_conflict"
        }
        StorageRpcErrorCode::BucketWriteReservationNotFound => {
            "storage_rpc_bucket_write_reservation_not_found"
        }
        StorageRpcErrorCode::MetadataCommandContention => "storage_rpc_metadata_command_contention",
        StorageRpcErrorCode::MetadataTransferHistoricalRouteActive => {
            "storage_rpc_metadata_transfer_historical_route_active"
        }
        StorageRpcErrorCode::TransportTimeout => "storage_rpc_transport_timeout",
        StorageRpcErrorCode::TransportClosed => "storage_rpc_transport_closed",
        StorageRpcErrorCode::ShardIntegrity => "storage_rpc_shard_integrity",
        StorageRpcErrorCode::MultipartConditionalRequestConflict => {
            "storage_rpc_multipart_conditional_request_conflict"
        }
    }
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

    #[error(
        "runtime route map for shard operation on local node {node_id} PG {pg_id} epoch {cluster_epoch} expired at {valid_until_ms}; current time is {now_ms}"
    )]
    RouteMapExpired {
        node_id: u32,
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        valid_until_ms: u64,
        now_ms: u64,
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

    #[error("runtime route-map lease could not bind to the process clock: {message}")]
    RouteMapLeaseBinding { message: String },

    #[error(
        "historical recovery runtime map node set {candidate:?} does not match current local node set {current:?}"
    )]
    HistoricalRecoveryNodeSetMismatch {
        current: Vec<u32>,
        candidate: Vec<u32>,
    },

    #[error("duplicate local node id {id}")]
    DuplicateNodeId { id: u32 },

    #[error("duplicate remote storage-node client node id {id}")]
    DuplicateRemoteStorageNodeClientNodeId { id: u32 },

    #[error("remote storage-node client node {id} is not present in the local cluster map")]
    RemoteStorageNodeClientNodeNotFound { id: u32 },

    #[error("remote storage-node client socket path {path:?} must be absolute")]
    RemoteStorageNodeClientSocketPathNotAbsolute { path: PathBuf },

    #[error("remote storage-node client node {id} RPC admission limit must be > 0")]
    RemoteStorageNodeClientRpcAdmissionLimitZero { id: u32 },

    #[error("remote storage-node client node {id} RPC admission wait timeout must be > 0")]
    RemoteStorageNodeClientRpcAdmissionWaitTimeoutZero { id: u32 },

    #[error("remote storage-node client node {id} RPC control admission wait timeout must be > 0")]
    RemoteStorageNodeClientRpcControlAdmissionWaitTimeoutZero { id: u32 },

    #[error("duplicate remote shard client node id {id}")]
    DuplicateRemoteShardClientNodeId { id: u32 },

    #[error("remote shard client node {id} is not present in the local cluster map")]
    RemoteShardClientNodeNotFound { id: u32 },

    #[error("remote shard client socket path {path:?} must be absolute")]
    RemoteShardClientSocketPathNotAbsolute { path: PathBuf },

    #[error("duplicate remote metadata-command client node id {id}")]
    DuplicateRemoteMetadataCommandClientNodeId { id: u32 },

    #[error("remote metadata-command client node {id} is not present in the local cluster map")]
    RemoteMetadataCommandClientNodeNotFound { id: u32 },

    #[error("remote metadata-command client socket path {path:?} must be absolute")]
    RemoteMetadataCommandClientSocketPathNotAbsolute { path: PathBuf },

    #[error("duplicate remote bucket metadata client node id {id}")]
    DuplicateRemoteBucketMetadataClientNodeId { id: u32 },

    #[error("remote bucket metadata client node {id} is not present in the local cluster map")]
    RemoteBucketMetadataClientNodeNotFound { id: u32 },

    #[error("remote bucket metadata client socket path {path:?} must be absolute")]
    RemoteBucketMetadataClientSocketPathNotAbsolute { path: PathBuf },

    #[error(
        "remote bucket metadata client node {id} socket path {bucket_metadata_socket_path:?} does not match remote bucket write reservation client socket path {bucket_write_reservation_socket_path:?}"
    )]
    RemoteBucketMetadataClientMismatchedBucketWriteReservationClient {
        id: u32,
        bucket_metadata_socket_path: PathBuf,
        bucket_write_reservation_socket_path: PathBuf,
    },

    #[error("duplicate remote bucket write reservation client node id {id}")]
    DuplicateRemoteBucketWriteReservationClientNodeId { id: u32 },

    #[error(
        "remote bucket write reservation client node {id} is not present in the local cluster map"
    )]
    RemoteBucketWriteReservationClientNodeNotFound { id: u32 },

    #[error("remote bucket write reservation client socket path {path:?} must be absolute")]
    RemoteBucketWriteReservationClientSocketPathNotAbsolute { path: PathBuf },

    #[error(
        "remote bucket write reservation client node {id} requires a matching remote bucket metadata client"
    )]
    RemoteBucketWriteReservationClientMissingBucketMetadataClient { id: u32 },

    #[error(
        "remote bucket write reservation client node {id} socket path {bucket_write_reservation_socket_path:?} does not match remote bucket metadata client socket path {bucket_metadata_socket_path:?}"
    )]
    RemoteBucketWriteReservationClientMismatchedBucketMetadataClient {
        id: u32,
        bucket_metadata_socket_path: PathBuf,
        bucket_write_reservation_socket_path: PathBuf,
    },

    #[error("duplicate remote object-generation metadata client node id {id}")]
    DuplicateRemoteObjectGenerationMetadataClientNodeId { id: u32 },

    #[error("remote object-generation metadata client node {id} is not present in the local cluster map")]
    RemoteObjectGenerationMetadataClientNodeNotFound { id: u32 },

    #[error("remote object-generation metadata client socket path {path:?} must be absolute")]
    RemoteObjectGenerationMetadataClientSocketPathNotAbsolute { path: PathBuf },

    #[error("duplicate remote object-version metadata client node id {id}")]
    DuplicateRemoteObjectVersionMetadataClientNodeId { id: u32 },

    #[error(
        "remote object-version metadata client node {id} is not present in the local cluster map"
    )]
    RemoteObjectVersionMetadataClientNodeNotFound { id: u32 },

    #[error("remote object-version metadata client socket path {path:?} must be absolute")]
    RemoteObjectVersionMetadataClientSocketPathNotAbsolute { path: PathBuf },

    #[error("duplicate remote direct PUT metadata client node id {id}")]
    DuplicateRemoteDirectPutMetadataClientNodeId { id: u32 },

    #[error("remote direct PUT metadata client node {id} is not present in the local cluster map")]
    RemoteDirectPutMetadataClientNodeNotFound { id: u32 },

    #[error("remote direct PUT metadata client socket path {path:?} must be absolute")]
    RemoteDirectPutMetadataClientSocketPathNotAbsolute { path: PathBuf },

    #[error("duplicate remote object-listing metadata client node id {id}")]
    DuplicateRemoteObjectListingMetadataClientNodeId { id: u32 },

    #[error(
        "remote object-listing metadata client node {id} is not present in the local cluster map"
    )]
    RemoteObjectListingMetadataClientNodeNotFound { id: u32 },

    #[error("remote object-listing metadata client socket path {path:?} must be absolute")]
    RemoteObjectListingMetadataClientSocketPathNotAbsolute { path: PathBuf },

    #[error("duplicate remote object-mutation metadata client node id {id}")]
    DuplicateRemoteObjectMutationMetadataClientNodeId { id: u32 },

    #[error(
        "remote object-mutation metadata client node {id} is not present in the local cluster map"
    )]
    RemoteObjectMutationMetadataClientNodeNotFound { id: u32 },

    #[error("remote object-mutation metadata client socket path {path:?} must be absolute")]
    RemoteObjectMutationMetadataClientSocketPathNotAbsolute { path: PathBuf },

    #[error("duplicate remote object-read metadata client node id {id}")]
    DuplicateRemoteObjectReadMetadataClientNodeId { id: u32 },

    #[error(
        "remote object-read metadata client node {id} is not present in the local cluster map"
    )]
    RemoteObjectReadMetadataClientNodeNotFound { id: u32 },

    #[error("remote object-read metadata client socket path {path:?} must be absolute")]
    RemoteObjectReadMetadataClientSocketPathNotAbsolute { path: PathBuf },

    #[error("metadata primary node {id} is not present in the local cluster map")]
    MetadataPrimaryNotFound { id: u32 },

    #[error("local cluster map must contain at least one PG")]
    EmptyPgSet,

    #[error("duplicate PG id {pg_id}")]
    DuplicatePgId { pg_id: u32 },

    #[error("duplicate route for PG {pg_id}")]
    DuplicatePgRoute { pg_id: u32 },

    #[error("route for PG {pg_id} is not present in the configured PG set")]
    RoutePgNotConfigured { pg_id: u32 },

    #[error("configured PG {pg_id} has no route")]
    MissingPgRoute { pg_id: u32 },

    #[error(
        "route for PG {pg_id} has cluster epoch {route_epoch}, expected current cluster epoch {cluster_epoch}"
    )]
    RouteClusterEpochMismatch {
        pg_id: u32,
        route_epoch: ClusterEpoch,
        cluster_epoch: ClusterEpoch,
    },

    #[error("route for PG {pg_id} primary node {primary_node_id} is not in its acting set")]
    RoutePrimaryNotInActingSet { pg_id: u32, primary_node_id: u32 },

    #[error("route for PG {pg_id} acting set references unknown local node {node_id}")]
    RouteActingSetNodeNotFound { pg_id: u32, node_id: u32 },

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

    #[error(
        "runtime route map for payload placement PG {pg_id} epoch {cluster_epoch} expired at {valid_until_ms}; current time is {now_ms}"
    )]
    RouteMapExpired {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        valid_until_ms: u64,
        now_ms: u64,
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

    #[error("stream segment already exists at index {segment_index}")]
    StreamSegmentConflict { segment_index: u32 },

    #[error("object generation reservation not found: {reservation_id}")]
    ObjectGenerationReservationNotFound { reservation_id: String },

    #[error(
        "object generation reservation conflict: reservation {reservation_id} generation {generation_id}"
    )]
    ObjectGenerationReservationConflict {
        reservation_id: String,
        generation_id: u64,
    },

    #[error("object version reservation conflict: version {version_id}")]
    ObjectVersionReservationConflict { version_id: crate::types::VersionId },

    #[error(
        "stale bucket metadata command for {name} at execution generation {bucket_execution_generation}"
    )]
    StaleBucketMetadataCommand {
        name: crate::types::BucketName,
        bucket_execution_generation: u64,
    },

    #[error(
        "stale object write metadata command for {bucket}/{key} at write sequence {write_sequence}"
    )]
    StaleObjectWriteCommand {
        bucket: crate::types::BucketName,
        key: crate::types::ObjectKey,
        write_sequence: u64,
        generation_id: Option<u64>,
    },

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
pub enum PgMetadataTransferError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Apply(#[from] BucketSnapshotLoadError),
    #[error("PG metadata transfer reconstruction failed: {message}")]
    Reconstruction { message: String },
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
    #[error("conditional multipart completion conflicts with an object write after initiation")]
    MultipartConditionalRequestConflict,
}
