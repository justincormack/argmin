/// Storage layer error types.
use std::path::PathBuf;

#[cfg(test)]
use crate::storage_rpc::StorageRpcErrorCode;
use crate::storage_rpc::{StorageNodeFailure, StorageRpcWireErrorCode};
use crate::types::{BucketName, ClusterEpoch, PgState, RouteMapValidity};

/// Opaque diagnostic for a failure inside the metadata database implementation.
///
/// The concrete database driver and its error types are private to `PgStore`.
/// Callers may preserve and report this diagnostic, but cannot construct or
/// classify it by backend-specific details.
pub struct DatabaseError {
    detail: Box<str>,
}

impl DatabaseError {
    pub(crate) fn new(detail: impl Into<Box<str>>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl std::fmt::Debug for DatabaseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("DatabaseError")
            .field(&self.detail)
            .finish()
    }
}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for DatabaseError {}

/// Semantic classification of a transient failure reported by a storage node.
///
/// This is an owner-internal intermediate classification. Public callers receive
/// the operation-level [`StoreOperationFailureClass`] instead of reconstructing
/// policy from storage-node behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageNodeFailureClass {
    ShardLocationStale,
    PgRouteUnavailable,
    MetadataCommandContention,
    MetadataTransferHistoricalRouteActive,
    TransportInterrupted,
}

/// Exhaustive semantic classification of a failed storage operation.
///
/// This is the only storage failure policy exposed to higher layers. It is
/// deliberately independent of PG, shard, database, route, command-log, and RPC
/// representations. Adding a `StoreError` variant requires an explicit decision
/// in [`StoreError::operation_failure_class`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOperationFailureClass {
    ResourceExhausted,
    MetadataCommandContention,
    RetryableConvergence,
    Other,
}

/// Bounded operator diagnostic retained when a concrete storage error crosses
/// its ownership boundary.
///
/// This is deliberately separate from [`StoreOperationFailureClass`]: request
/// policy must not depend on operational diagnosis, while operators still need
/// to distinguish broad failure domains. The category contains no resource
/// names, physical identifiers, paths, database text, or remote messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreFailureDiagnosticCategory {
    NotFound,
    Integrity,
    Topology,
    MetadataContention,
    MetadataConsistency,
    ResourceExhausted,
    RpcTransport,
    RpcProtocol,
    Schema,
    Io,
    Database,
    Codec,
    InternalInvariant,
}

impl StoreFailureDiagnosticCategory {
    const fn cause_label(self) -> &'static str {
        match self {
            Self::NotFound => "store_not_found",
            Self::Integrity => "store_integrity_failure",
            Self::Topology => "store_topology_failure",
            Self::MetadataContention => "store_metadata_command_contention",
            Self::MetadataConsistency => "store_metadata_consistency_failure",
            Self::ResourceExhausted => "store_resource_exhausted",
            Self::RpcTransport => "store_rpc_transport_failure",
            Self::RpcProtocol => "store_rpc_protocol_failure",
            Self::Schema => "store_schema_failure",
            Self::Io => "store_io_failure",
            Self::Database => "store_database_failure",
            Self::Codec => "store_codec_failure",
            Self::InternalInvariant => "store_internal_failure",
        }
    }
}

/// A storage failure summarized for diagnosis without exposing its
/// implementation representation to another crate.
///
/// Public formatting and the error chain are intentionally redacted. Callers may
/// use [`StoreFailure::class`] to translate the semantic outcome into their own
/// protocol response, but cannot inspect the original PG, shard, database,
/// command-log, route, or RPC error. Operator diagnostics retain only a bounded
/// storage-owned category.
pub struct StoreFailure {
    class: StoreOperationFailureClass,
    diagnostic_category: StoreFailureDiagnosticCategory,
}

impl StoreFailure {
    #[must_use]
    pub const fn class(&self) -> StoreOperationFailureClass {
        self.class
    }

    #[must_use]
    pub const fn diagnostic_cause_label(&self) -> &'static str {
        self.diagnostic_category.cause_label()
    }
}

impl From<StoreError> for StoreFailure {
    fn from(error: StoreError) -> Self {
        Self {
            class: error.operation_failure_class(),
            diagnostic_category: error.failure_diagnostic_category(),
        }
    }
}

impl std::fmt::Debug for StoreFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoreFailure")
            .field("class", &self.class)
            .field(
                "diagnostic_category",
                &self.diagnostic_category.cause_label(),
            )
            .finish()
    }
}

impl std::fmt::Display for StoreFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("storage operation failed")
    }
}

impl std::error::Error for StoreFailure {}

/// Opaque diagnostic detail reported by a storage node.
///
/// The remote text is retained for storage-owned diagnosis, but its public
/// formatting is deliberately redacted so it cannot become an API or leak
/// through an outer error renderer.
pub struct StorageNodeFailureDetail(Box<str>);

impl StorageNodeFailureDetail {
    pub(crate) fn new(detail: impl Into<Box<str>>) -> Self {
        Self(detail.into())
    }

    #[cfg(test)]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for StorageNodeFailureDetail {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(_retained_detail) = self;
        formatter.write_str("StorageNodeFailureDetail(<redacted>)")
    }
}

impl std::fmt::Display for StorageNodeFailureDetail {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self(_retained_detail) = self;
        formatter.write_str("storage-node diagnostic redacted")
    }
}

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

    #[error("placed segment backfill source payload is unavailable")]
    PlacedSegmentBackfillSourceUnavailable,

    #[error("PG {pg_id} route for cluster epoch {cluster_epoch} is not retained")]
    HistoricalPgRouteNotRetained {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

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

    #[error("storage-node {operation} failed on node {node_id}: {detail}")]
    StorageRpc {
        node_id: u32,
        operation: &'static str,
        failure: StorageNodeFailure,
        detail: StorageNodeFailureDetail,
    },

    #[error("storage-node {operation} on node {node_id} exhausted resources: {detail}")]
    StorageRpcResourceExhausted {
        node_id: u32,
        operation: &'static str,
        detail: StorageNodeFailureDetail,
    },

    #[error(
        "storage-node {operation} on node {node_id} found shard deletion in progress: {detail}"
    )]
    StorageRpcShardDeleteInProgress {
        node_id: u32,
        operation: &'static str,
        detail: StorageNodeFailureDetail,
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
        "local node {node_id} no longer matches the certified metadata read proof for PG {pg_id}"
    )]
    StaleMetadataReadProof { node_id: u32, pg_id: u32 },

    #[error(
        "runtime route map for cluster epoch {cluster_epoch} expired at {valid_until_ms}; current time is {now_ms}"
    )]
    RouteMapExpired {
        cluster_epoch: ClusterEpoch,
        valid_until_ms: u64,
        now_ms: u64,
    },

    #[error(
        "route admission for cluster epoch {admitted_epoch} was used with a different publication domain or runtime-map generation at epoch {operation_epoch}"
    )]
    RouteAdmissionClusterMismatch {
        admitted_epoch: ClusterEpoch,
        operation_epoch: ClusterEpoch,
    },

    #[error("{operation} subject does not match its admitted route capability")]
    RouteCapabilitySubjectMismatch { operation: &'static str },

    #[error("failed to issue a multipart upload ID")]
    MultipartUploadIdIssuanceFailed,

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

    #[error("PG database schema is invalid: {reason}")]
    PgSchemaInvalid { reason: String },

    #[error("metadata digest bootstrap state is invalid: {reason}")]
    MetadataDigestBootstrapInvalid { reason: String },

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
        source: DatabaseError,
    },

    #[error("erasure coding error: {context}: {reason}")]
    ErasureCoding {
        context: &'static str,
        reason: String,
    },
}

impl StoreError {
    /// Classify this failure for a higher-layer request operation.
    ///
    /// The match is intentionally exhaustive. Storage owns recursive adapter and
    /// RPC interpretation; callers make protocol-specific decisions from this
    /// bounded semantic result only.
    #[must_use]
    pub fn operation_failure_class(&self) -> StoreOperationFailureClass {
        match self {
            Self::ClusterMapHistoryReferenceLimitExceeded { .. }
            | Self::StorageRpcResourceExhausted { .. } => {
                StoreOperationFailureClass::ResourceExhausted
            }
            Self::MetadataCommandLogConflict { .. }
            | Self::MetadataCommandLogGap { .. }
            | Self::MetadataCommandPendingConflict { .. }
            | Self::MetadataCommandContention { .. } => {
                StoreOperationFailureClass::MetadataCommandContention
            }
            Self::StalePayloadOperation { .. }
            | Self::StaleMetadataPrimaryBridge { .. }
            | Self::StaleMetadataOperation { .. }
            | Self::StaleMetadataRoute { .. }
            | Self::StaleMetadataReadProof { .. }
            | Self::RouteMapExpired { .. }
            | Self::RouteAdmissionClusterMismatch { .. }
            | Self::StaleMetadataCommand { .. }
            | Self::StaleShardOperation { .. }
            | Self::StaleShardLocation { .. }
            | Self::PgNotActive { .. }
            | Self::ShardPgNotActive { .. } => StoreOperationFailureClass::RetryableConvergence,
            Self::ShardStore { source, .. } => source.operation_failure_class(),
            Self::StorageRpc { failure, .. } => match failure.wire_code() {
                StorageRpcWireErrorCode::MetadataCommandContention => {
                    StoreOperationFailureClass::MetadataCommandContention
                }
                StorageRpcWireErrorCode::ResourceExhausted => {
                    StoreOperationFailureClass::ResourceExhausted
                }
                StorageRpcWireErrorCode::StaleShardLocation
                | StorageRpcWireErrorCode::InactivePgRoute
                | StorageRpcWireErrorCode::NonActingSetAccess
                | StorageRpcWireErrorCode::WrongClusterEpoch => {
                    StoreOperationFailureClass::RetryableConvergence
                }
                StorageRpcWireErrorCode::FrameDecode
                | StorageRpcWireErrorCode::PayloadDecode
                | StorageRpcWireErrorCode::UnknownNode
                | StorageRpcWireErrorCode::UnknownPg
                | StorageRpcWireErrorCode::UnsupportedOperation
                | StorageRpcWireErrorCode::Internal
                | StorageRpcWireErrorCode::ReclaimClaimNotFound
                | StorageRpcWireErrorCode::ShardDeleteInProgress
                | StorageRpcWireErrorCode::BucketWriteDrainConflict
                | StorageRpcWireErrorCode::BucketWriteDrainNotFound
                | StorageRpcWireErrorCode::ReclaimClaimConflict
                | StorageRpcWireErrorCode::NotFound
                | StorageRpcWireErrorCode::BucketWriteReservationConflict
                | StorageRpcWireErrorCode::BucketWriteReservationNotFound
                | StorageRpcWireErrorCode::ShardIntegrity
                | StorageRpcWireErrorCode::MultipartConditionalRequestConflict
                | StorageRpcWireErrorCode::MetadataTransferHistoricalRouteActive
                | StorageRpcWireErrorCode::TransportTimeout
                | StorageRpcWireErrorCode::TransportClosed => StoreOperationFailureClass::Other,
            },
            Self::NotFound
            | Self::IntegrityError { .. }
            | Self::ShardAckMismatch { .. }
            | Self::PayloadShardSetMismatch { .. }
            | Self::PlacedSegmentBackfillSourceUnavailable
            | Self::HistoricalPgRouteNotRetained { .. }
            | Self::ObjectPayloadReclaimFenceAuthorityMismatch
            | Self::PgDurableIdentityInvalid { .. }
            | Self::PgNotFound { .. }
            | Self::InvalidPgTopology { .. }
            | Self::ClusterPgNotFound { .. }
            | Self::ShardPgNotFound { .. }
            | Self::StorageRpcShardDeleteInProgress { .. }
            | Self::RouteCapabilitySubjectMismatch { .. }
            | Self::MultipartUploadIdIssuanceFailed
            | Self::MetadataCommandWrongPg { .. }
            | Self::MetadataCommandFromNonPrimary { .. }
            | Self::MetadataTransferEmpty { .. }
            | Self::MetadataCommandPendingOnNonPrimary { .. }
            | Self::MetadataCommandLogChecksumMismatch { .. }
            | Self::MetadataCommandLogHashMismatch { .. }
            | Self::MetadataCommandReplicaStateMissing { .. }
            | Self::MetadataCommandReplicaStateDiverged { .. }
            | Self::MetadataStateDigestMismatch { .. }
            | Self::MetadataTransferUnsupportedProof { .. }
            | Self::MetadataCheckpointInvalid { .. }
            | Self::NodeNotFound { .. }
            | Self::NodeNotInActingSet { .. }
            | Self::ShardIndexMismatch { .. }
            | Self::ShardScavengerObservationWrongPg { .. }
            | Self::ShardScavengerObservationShardIndexMismatch { .. }
            | Self::ShardScavengerObservationInconsistentReason { .. }
            | Self::InvalidKeyLength { .. }
            | Self::InvalidShardKeyHex
            | Self::ShardScavengerScanIncomplete { .. }
            | Self::PgSchemaInvalid { .. }
            | Self::MetadataDigestBootstrapInvalid { .. }
            | Self::Io { .. }
            | Self::Db { .. }
            | Self::ErasureCoding { .. } => StoreOperationFailureClass::Other,
        }
    }

    /// Return a bounded storage-owned operator diagnostic label.
    ///
    /// This label is for reporting only. Request outcome and retry policy must
    /// use [`Self::operation_failure_class`] instead. The label never contains
    /// values retained by the concrete error.
    #[must_use]
    pub fn diagnostic_cause_label(&self) -> &'static str {
        self.failure_diagnostic_category().cause_label()
    }

    fn failure_diagnostic_category(&self) -> StoreFailureDiagnosticCategory {
        match self {
            Self::NotFound | Self::PlacedSegmentBackfillSourceUnavailable => {
                StoreFailureDiagnosticCategory::NotFound
            }
            Self::IntegrityError { .. }
            | Self::ShardAckMismatch { .. }
            | Self::PayloadShardSetMismatch { .. }
            | Self::PgDurableIdentityInvalid { .. }
            | Self::MetadataCommandLogChecksumMismatch { .. }
            | Self::MetadataCommandLogHashMismatch { .. }
            | Self::MetadataCommandReplicaStateDiverged { .. }
            | Self::MetadataStateDigestMismatch { .. }
            | Self::MetadataCheckpointInvalid { .. } => StoreFailureDiagnosticCategory::Integrity,
            Self::HistoricalPgRouteNotRetained { .. }
            | Self::PgNotFound { .. }
            | Self::InvalidPgTopology { .. }
            | Self::ClusterPgNotFound { .. }
            | Self::ShardPgNotFound { .. }
            | Self::PgNotActive { .. }
            | Self::ShardPgNotActive { .. }
            | Self::StalePayloadOperation { .. }
            | Self::StaleMetadataPrimaryBridge { .. }
            | Self::StaleMetadataOperation { .. }
            | Self::StaleMetadataRoute { .. }
            | Self::StaleMetadataReadProof { .. }
            | Self::RouteMapExpired { .. }
            | Self::RouteAdmissionClusterMismatch { .. }
            | Self::RouteCapabilitySubjectMismatch { .. }
            | Self::StaleMetadataCommand { .. }
            | Self::MetadataCommandWrongPg { .. }
            | Self::MetadataCommandFromNonPrimary { .. }
            | Self::MetadataCommandPendingOnNonPrimary { .. }
            | Self::StaleShardOperation { .. }
            | Self::StaleShardLocation { .. }
            | Self::NodeNotFound { .. }
            | Self::NodeNotInActingSet { .. }
            | Self::ShardIndexMismatch { .. } => StoreFailureDiagnosticCategory::Topology,
            Self::MetadataCommandLogConflict { .. }
            | Self::MetadataCommandLogGap { .. }
            | Self::MetadataCommandPendingConflict { .. }
            | Self::MetadataCommandContention { .. } => {
                StoreFailureDiagnosticCategory::MetadataContention
            }
            Self::ObjectPayloadReclaimFenceAuthorityMismatch
            | Self::MetadataTransferEmpty { .. }
            | Self::MetadataCommandReplicaStateMissing { .. }
            | Self::MetadataTransferUnsupportedProof { .. }
            | Self::ShardScavengerObservationWrongPg { .. }
            | Self::ShardScavengerObservationShardIndexMismatch { .. }
            | Self::ShardScavengerObservationInconsistentReason { .. }
            | Self::ShardScavengerScanIncomplete { .. }
            | Self::StorageRpcShardDeleteInProgress { .. } => {
                StoreFailureDiagnosticCategory::MetadataConsistency
            }
            Self::ClusterMapHistoryReferenceLimitExceeded { .. }
            | Self::StorageRpcResourceExhausted { .. } => {
                StoreFailureDiagnosticCategory::ResourceExhausted
            }
            Self::ShardStore { source, .. } => source.failure_diagnostic_category(),
            Self::StorageRpc { failure, .. } => match failure.wire_code() {
                StorageRpcWireErrorCode::TransportTimeout
                | StorageRpcWireErrorCode::TransportClosed => {
                    StoreFailureDiagnosticCategory::RpcTransport
                }
                StorageRpcWireErrorCode::FrameDecode
                | StorageRpcWireErrorCode::PayloadDecode
                | StorageRpcWireErrorCode::UnsupportedOperation => {
                    StoreFailureDiagnosticCategory::RpcProtocol
                }
                StorageRpcWireErrorCode::ResourceExhausted => {
                    StoreFailureDiagnosticCategory::ResourceExhausted
                }
                StorageRpcWireErrorCode::ShardIntegrity => {
                    StoreFailureDiagnosticCategory::Integrity
                }
                StorageRpcWireErrorCode::NotFound => StoreFailureDiagnosticCategory::NotFound,
                StorageRpcWireErrorCode::UnknownNode
                | StorageRpcWireErrorCode::UnknownPg
                | StorageRpcWireErrorCode::StaleShardLocation
                | StorageRpcWireErrorCode::InactivePgRoute
                | StorageRpcWireErrorCode::NonActingSetAccess
                | StorageRpcWireErrorCode::WrongClusterEpoch
                | StorageRpcWireErrorCode::MetadataTransferHistoricalRouteActive => {
                    StoreFailureDiagnosticCategory::Topology
                }
                StorageRpcWireErrorCode::MetadataCommandContention => {
                    StoreFailureDiagnosticCategory::MetadataContention
                }
                StorageRpcWireErrorCode::ReclaimClaimNotFound
                | StorageRpcWireErrorCode::ShardDeleteInProgress
                | StorageRpcWireErrorCode::BucketWriteDrainConflict
                | StorageRpcWireErrorCode::BucketWriteDrainNotFound
                | StorageRpcWireErrorCode::ReclaimClaimConflict
                | StorageRpcWireErrorCode::BucketWriteReservationConflict
                | StorageRpcWireErrorCode::BucketWriteReservationNotFound
                | StorageRpcWireErrorCode::MultipartConditionalRequestConflict => {
                    StoreFailureDiagnosticCategory::MetadataConsistency
                }
                StorageRpcWireErrorCode::Internal => {
                    StoreFailureDiagnosticCategory::InternalInvariant
                }
            },
            Self::PgSchemaInvalid { .. } | Self::MetadataDigestBootstrapInvalid { .. } => {
                StoreFailureDiagnosticCategory::Schema
            }
            Self::Io { .. } => StoreFailureDiagnosticCategory::Io,
            Self::Db { .. } => StoreFailureDiagnosticCategory::Database,
            Self::InvalidKeyLength { .. }
            | Self::InvalidShardKeyHex
            | Self::ErasureCoding { .. } => StoreFailureDiagnosticCategory::Codec,
            Self::MultipartUploadIdIssuanceFailed => {
                StoreFailureDiagnosticCategory::InternalInvariant
            }
        }
    }

    /// Construct the semantic resource-exhaustion state without exposing a
    /// remote storage-node diagnostic.
    #[must_use]
    pub fn storage_node_resource_exhausted(node_id: u32, operation: &'static str) -> Self {
        Self::StorageRpcResourceExhausted {
            node_id,
            operation,
            detail: StorageNodeFailureDetail::new("semantic resource exhaustion"),
        }
    }

    /// Construct the semantic shard-deletion state without exposing a remote
    /// storage-node diagnostic.
    #[must_use]
    pub fn storage_node_shard_delete_in_progress(node_id: u32, operation: &'static str) -> Self {
        Self::StorageRpcShardDeleteInProgress {
            node_id,
            operation,
            detail: StorageNodeFailureDetail::new("semantic shard deletion in progress"),
        }
    }

    /// Classify a transient storage-node failure without exposing its wire code.
    ///
    /// Nested shard-store failures are unwrapped because callers make retry
    /// decisions for the logical operation, not for the internal adapter layer.
    #[must_use]
    pub(crate) fn storage_node_failure_class(&self) -> Option<StorageNodeFailureClass> {
        match self {
            Self::ShardStore { source, .. } => source.storage_node_failure_class(),
            Self::StaleMetadataReadProof { .. } => {
                Some(StorageNodeFailureClass::ShardLocationStale)
            }
            Self::StorageRpc { failure, .. } => match failure.wire_code() {
                StorageRpcWireErrorCode::StaleShardLocation => {
                    Some(StorageNodeFailureClass::ShardLocationStale)
                }
                StorageRpcWireErrorCode::InactivePgRoute
                | StorageRpcWireErrorCode::NonActingSetAccess
                | StorageRpcWireErrorCode::WrongClusterEpoch => {
                    Some(StorageNodeFailureClass::PgRouteUnavailable)
                }
                StorageRpcWireErrorCode::MetadataCommandContention => {
                    Some(StorageNodeFailureClass::MetadataCommandContention)
                }
                StorageRpcWireErrorCode::MetadataTransferHistoricalRouteActive => {
                    Some(StorageNodeFailureClass::MetadataTransferHistoricalRouteActive)
                }
                StorageRpcWireErrorCode::TransportTimeout
                | StorageRpcWireErrorCode::TransportClosed => {
                    Some(StorageNodeFailureClass::TransportInterrupted)
                }
                StorageRpcWireErrorCode::FrameDecode
                | StorageRpcWireErrorCode::PayloadDecode
                | StorageRpcWireErrorCode::UnknownNode
                | StorageRpcWireErrorCode::UnknownPg
                | StorageRpcWireErrorCode::UnsupportedOperation
                | StorageRpcWireErrorCode::Internal
                | StorageRpcWireErrorCode::ResourceExhausted
                | StorageRpcWireErrorCode::ReclaimClaimNotFound
                | StorageRpcWireErrorCode::ShardDeleteInProgress
                | StorageRpcWireErrorCode::BucketWriteDrainConflict
                | StorageRpcWireErrorCode::BucketWriteDrainNotFound
                | StorageRpcWireErrorCode::ReclaimClaimConflict
                | StorageRpcWireErrorCode::NotFound
                | StorageRpcWireErrorCode::BucketWriteReservationConflict
                | StorageRpcWireErrorCode::BucketWriteReservationNotFound
                | StorageRpcWireErrorCode::ShardIntegrity
                | StorageRpcWireErrorCode::MultipartConditionalRequestConflict => None,
            },
            _ => None,
        }
    }

    /// Report whether a local or remote storage operation established that its
    /// requested payload was absent.
    ///
    /// Callers must still decide whether absence is meaningful for the logical
    /// operation. This method keeps that decision independent of the private RPC
    /// wire representation.
    #[must_use]
    pub fn is_payload_not_found(&self) -> bool {
        match self {
            Self::NotFound => true,
            Self::ShardStore { source, .. } => source.is_payload_not_found(),
            Self::StorageRpc { failure, .. } => {
                failure.wire_code() == StorageRpcWireErrorCode::NotFound
            }
            _ => false,
        }
    }

    /// Return a bounded diagnostic category suitable for metrics labels.
    #[must_use]
    pub(crate) fn diagnostic_kind(&self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::IntegrityError { .. } => "integrity_error",
            Self::ShardAckMismatch { .. } => "shard_ack_mismatch",
            Self::PayloadShardSetMismatch { .. } => "payload_shard_set_mismatch",
            Self::PlacedSegmentBackfillSourceUnavailable => {
                "placed_segment_backfill_source_unavailable"
            }
            Self::HistoricalPgRouteNotRetained { .. } => "historical_pg_route_not_retained",
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
            Self::StorageRpc { .. } => "storage_node_failure",
            Self::StorageRpcResourceExhausted { .. } => "storage_rpc_resource_exhausted",
            Self::StorageRpcShardDeleteInProgress { .. } => "storage_rpc_shard_delete_in_progress",
            Self::StalePayloadOperation { .. } => "stale_payload_operation",
            Self::StaleMetadataPrimaryBridge { .. } => "stale_metadata_primary_bridge",
            Self::StaleMetadataOperation { .. } => "stale_metadata_operation",
            Self::StaleMetadataRoute { .. } => "stale_metadata_route",
            Self::StaleMetadataReadProof { .. } => "stale_metadata_read_proof",
            Self::RouteMapExpired { .. } => "route_map_expired",
            Self::RouteAdmissionClusterMismatch { .. } => "route_admission_cluster_mismatch",
            Self::RouteCapabilitySubjectMismatch { .. } => "route_capability_subject_mismatch",
            Self::MultipartUploadIdIssuanceFailed => "multipart_upload_id_issuance_failed",
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
            Self::PgSchemaInvalid { .. } => "pg_schema_invalid",
            Self::MetadataDigestBootstrapInvalid { .. } => "metadata_digest_bootstrap_invalid",
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

    #[error("failed to generate opaque storage-cluster runtime identity")]
    RuntimeIdentityGeneration,

    #[error("runtime route-map lease could not bind to the process clock: {message}")]
    RouteMapLeaseBinding { message: String },

    #[error("static route authority must have unbounded validity")]
    StaticRouteAuthorityBoundedValidity,

    #[error("embedded standalone route topology changed after durable identity preparation")]
    StandaloneEmbeddedRouteIdentityChanged,

    #[error("dynamic route authority requires a runtime-map publication capability")]
    DynamicRouteAuthorityRequiresRuntimeMapHandle,

    #[error("dynamic route authority for epoch {epoch} must have bounded validity")]
    DynamicRouteAuthorityUnboundedValidity { epoch: ClusterEpoch },

    #[error(
        "dynamic route authority epoch {authority} does not match local route-map epoch {local}"
    )]
    DynamicRouteAuthorityEpochMismatch {
        local: ClusterEpoch,
        authority: ClusterEpoch,
    },

    #[error(
        "dynamic route authority validity {authority:?} does not match local route-map validity {local:?}"
    )]
    DynamicRouteAuthorityValidityMismatch {
        local: RouteMapValidity,
        authority: RouteMapValidity,
    },

    #[error("dynamic route authority node set does not match the local route map")]
    DynamicRouteAuthorityNodeSetMismatch,

    #[error("dynamic route authority endpoint for node {id} does not match the local route map")]
    DynamicRouteAuthorityNodeEndpointMismatch { id: u32 },

    #[error("dynamic route authority current PG routes do not match the local route map")]
    DynamicRouteAuthorityPgRoutesMismatch,

    #[error("dynamic route authority historical PG routes do not match the local route map")]
    DynamicRouteAuthorityHistoricalPgRoutesMismatch,

    #[error("dynamic route authority historical epochs do not match the local route map")]
    DynamicRouteAuthorityHistoricalEpochsMismatch,

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

    #[error(
        "remote storage-node client node {id} endpoint does not match its authoritative runtime-map endpoint"
    )]
    RemoteStorageNodeClientEndpointAuthorityMismatch { id: u32 },

    #[error("runtime-map node {id} has no installed remote storage-node client")]
    RuntimeMapStorageNodeClientMissing { id: u32 },

    #[error("resolved storage RPC endpoints require authenticated client configuration")]
    ResolvedStorageRpcEndpointsRequireAuthentication,

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

    #[error("route effect fence rejected: {source}")]
    RouteEffectRejected {
        #[source]
        source: StoreError,
    },

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

    #[error("metadata invariant violation during {context}: {reason}")]
    InvariantViolation {
        context: &'static str,
        reason: String,
    },

    #[error("database error: {context}: {source}")]
    Db {
        context: &'static str,
        #[source]
        source: DatabaseError,
    },
}

impl MetadataError {
    /// Report whether this metadata failure represents command contention.
    ///
    /// This interpretation is storage-owned because the individual reservation,
    /// generation, and command variants are metadata implementation details.
    #[must_use]
    pub fn is_command_contention(&self) -> bool {
        match self {
            Self::BucketWriteReservationConflict { .. }
            | Self::BucketWriteReservationNotFound { .. }
            | Self::ObjectGenerationReservationConflict { .. }
            | Self::ObjectVersionReservationConflict { .. }
            | Self::StaleBucketMetadataCommand { .. }
            | Self::StaleObjectWriteCommand { .. } => true,
            Self::BucketNotFound { .. }
            | Self::InvalidBucketName { .. }
            | Self::InvalidObjectKey { .. }
            | Self::BucketAlreadyExists
            | Self::BucketNotEmpty
            | Self::BucketNotFinalizedForDelete { .. }
            | Self::BucketWriteDraining
            | Self::BucketWriteDrainConflict { .. }
            | Self::BucketWriteDrainNotFound { .. }
            | Self::ReclaimClaimConflict { .. }
            | Self::ReclaimClaimNotFound { .. }
            | Self::RouteEffectRejected { .. }
            | Self::ObjectNotFound
            | Self::MethodNotAllowedOnDeleteMarker
            | Self::InvalidVersioningTransition { .. }
            | Self::NoSuchUpload { .. }
            | Self::UploadNotInProgress { .. }
            | Self::PartNotFound { .. }
            | Self::StreamSessionNotFound { .. }
            | Self::StreamSessionNotInProgress { .. }
            | Self::StreamSegmentConflict { .. }
            | Self::ObjectGenerationReservationNotFound { .. }
            | Self::NotImplemented { .. }
            | Self::InvariantViolation { .. }
            | Self::Db { .. } => false,
        }
    }
}

pub(crate) enum BucketSnapshotLoadError {
    Store(StoreError),
    Metadata(MetadataError),
}

impl BucketSnapshotLoadError {
    /// Return a bounded storage-owned label for operator diagnostics.
    #[must_use]
    pub fn diagnostic_cause_label(&self) -> &'static str {
        match self {
            Self::Store(error) => OperationFailureDiagnosticCategory::from_store(error),
            Self::Metadata(error) => OperationFailureDiagnosticCategory::from_metadata(error),
        }
        .cause_label()
    }
}

impl From<StoreError> for BucketSnapshotLoadError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<MetadataError> for BucketSnapshotLoadError {
    fn from(error: MetadataError) -> Self {
        Self::Metadata(error)
    }
}

impl std::fmt::Debug for BucketSnapshotLoadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BucketSnapshotLoadError")
            .field("cause_label", &self.diagnostic_cause_label())
            .finish()
    }
}

impl std::fmt::Display for BucketSnapshotLoadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("bucket snapshot load failed")
    }
}

impl std::error::Error for BucketSnapshotLoadError {}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PgMetadataTransferError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Apply(#[from] BucketSnapshotLoadError),
    #[error("PG metadata transfer reconstruction failed: {message}")]
    Reconstruction { message: String },
    #[error("PG peering node {node_id:?} still has a pending metadata command")]
    PendingMetadataCommand { node_id: crate::NodeId },
    #[error("PG metadata transfer route changed during reconstruction: {message}")]
    RouteRefreshRequired { message: String },
}

impl PgMetadataTransferError {
    pub(crate) fn reconstruction(error: crate::peering::PgPeeringReconstructionError) -> Self {
        let error = match error {
            crate::peering::PgPeeringReconstructionError::PendingMetadataCommand { node_id } => {
                return Self::PendingMetadataCommand { node_id };
            }
            error => error,
        };
        let route_refresh_required = matches!(
            error,
            crate::peering::PgPeeringReconstructionError::StaleReplicaEpoch { .. }
        );
        let message = error.to_string();
        if route_refresh_required {
            Self::RouteRefreshRequired { message }
        } else {
            Self::Reconstruction { message }
        }
    }

    /// Report whether retrying this transfer requires a fresh authoritative
    /// route. This policy is storage-owned because it depends on private local,
    /// RPC, reconstruction, and nested apply failure representations.
    #[must_use]
    pub(crate) fn requires_route_refresh_retry(&self) -> bool {
        match self {
            Self::Store(error) => store_error_requires_metadata_transfer_route_refresh(error),
            Self::Apply(BucketSnapshotLoadError::Store(error)) => {
                store_error_requires_metadata_transfer_route_refresh(error)
            }
            Self::Apply(BucketSnapshotLoadError::Metadata(_)) | Self::Reconstruction { .. } => {
                false
            }
            Self::PendingMetadataCommand { .. } => false,
            Self::RouteRefreshRequired { .. } => true,
        }
    }

    /// Report a safe transient import blocker that must be resolved by the
    /// normal metadata-command recovery path before import retries.
    #[must_use]
    pub(crate) fn is_transient_import_blocker(&self) -> bool {
        matches!(self, Self::PendingMetadataCommand { .. })
    }
}

fn store_error_requires_metadata_transfer_route_refresh(error: &StoreError) -> bool {
    if error
        .storage_node_failure_class()
        .is_some_and(storage_node_failure_requires_metadata_transfer_route_refresh)
    {
        return true;
    }
    match error {
        StoreError::RouteMapExpired { .. }
        | StoreError::StaleMetadataOperation { .. }
        | StoreError::StaleMetadataRoute { .. }
        | StoreError::StaleMetadataReadProof { .. }
        | StoreError::StaleShardLocation { .. } => true,
        StoreError::ShardStore { source, .. } => {
            store_error_requires_metadata_transfer_route_refresh(source)
        }
        _ => false,
    }
}

fn storage_node_failure_requires_metadata_transfer_route_refresh(
    failure: StorageNodeFailureClass,
) -> bool {
    match failure {
        StorageNodeFailureClass::ShardLocationStale
        | StorageNodeFailureClass::MetadataCommandContention
        | StorageNodeFailureClass::MetadataTransferHistoricalRouteActive
        | StorageNodeFailureClass::TransportInterrupted => true,
        StorageNodeFailureClass::PgRouteUnavailable => false,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetadataFailureDiagnosticCategory {
    BucketWriteDraining,
    BucketWriteReservationConflict,
    BucketWriteDrainConflict,
    ReclaimClaimConflict,
    ObjectGenerationReservationConflict,
    ObjectVersionReservationConflict,
    StaleBucketCommand,
    StaleObjectCommand,
    Database,
    Other,
}

impl MetadataFailureDiagnosticCategory {
    fn from_error(error: &MetadataError) -> Self {
        match error {
            MetadataError::BucketWriteDraining => Self::BucketWriteDraining,
            MetadataError::BucketWriteReservationConflict { .. } => {
                Self::BucketWriteReservationConflict
            }
            MetadataError::BucketWriteDrainConflict { .. } => Self::BucketWriteDrainConflict,
            MetadataError::ReclaimClaimConflict { .. } => Self::ReclaimClaimConflict,
            MetadataError::ObjectGenerationReservationConflict { .. } => {
                Self::ObjectGenerationReservationConflict
            }
            MetadataError::ObjectVersionReservationConflict { .. } => {
                Self::ObjectVersionReservationConflict
            }
            MetadataError::StaleBucketMetadataCommand { .. } => Self::StaleBucketCommand,
            MetadataError::StaleObjectWriteCommand { .. } => Self::StaleObjectCommand,
            MetadataError::Db { .. } => Self::Database,
            MetadataError::BucketNotFound { .. }
            | MetadataError::InvalidBucketName { .. }
            | MetadataError::InvalidObjectKey { .. }
            | MetadataError::BucketAlreadyExists
            | MetadataError::BucketNotEmpty
            | MetadataError::BucketNotFinalizedForDelete { .. }
            | MetadataError::BucketWriteReservationNotFound { .. }
            | MetadataError::BucketWriteDrainNotFound { .. }
            | MetadataError::ReclaimClaimNotFound { .. }
            | MetadataError::RouteEffectRejected { .. }
            | MetadataError::ObjectNotFound
            | MetadataError::MethodNotAllowedOnDeleteMarker
            | MetadataError::InvalidVersioningTransition { .. }
            | MetadataError::NoSuchUpload { .. }
            | MetadataError::UploadNotInProgress { .. }
            | MetadataError::PartNotFound { .. }
            | MetadataError::StreamSessionNotFound { .. }
            | MetadataError::StreamSessionNotInProgress { .. }
            | MetadataError::StreamSegmentConflict { .. }
            | MetadataError::ObjectGenerationReservationNotFound { .. }
            | MetadataError::NotImplemented { .. }
            | MetadataError::InvariantViolation { .. } => Self::Other,
        }
    }

    const fn cause_label(self) -> &'static str {
        match self {
            Self::BucketWriteDraining => "bucket_write_draining",
            Self::BucketWriteReservationConflict => "bucket_write_reservation_conflict",
            Self::BucketWriteDrainConflict => "bucket_write_drain_conflict",
            Self::ReclaimClaimConflict => "reclaim_claim_conflict",
            Self::ObjectGenerationReservationConflict => "object_generation_reservation_conflict",
            Self::ObjectVersionReservationConflict => "object_version_reservation_conflict",
            Self::StaleBucketCommand => "stale_bucket_metadata_command",
            Self::StaleObjectCommand => "stale_object_write_command",
            Self::Database => "metadata_db_error",
            Self::Other => "metadata_failure",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OperationFailureDiagnosticCategory {
    Store(StoreFailureDiagnosticCategory),
    Metadata(MetadataFailureDiagnosticCategory),
}

impl OperationFailureDiagnosticCategory {
    fn from_store(error: &StoreError) -> Self {
        Self::Store(error.failure_diagnostic_category())
    }

    fn from_metadata(error: &MetadataError) -> Self {
        Self::Metadata(MetadataFailureDiagnosticCategory::from_error(error))
    }

    const fn cause_label(self) -> &'static str {
        match self {
            Self::Store(category) => category.cause_label(),
            Self::Metadata(category) => category.cause_label(),
        }
    }
}

pub(crate) enum BucketWriteDrainError {
    Store(StoreError),
    Metadata(MetadataError),
}

impl BucketWriteDrainError {
    /// Return a bounded storage-owned label for operator diagnostics.
    #[must_use]
    pub fn diagnostic_cause_label(&self) -> &'static str {
        match self {
            Self::Store(error) => OperationFailureDiagnosticCategory::from_store(error),
            Self::Metadata(error) => OperationFailureDiagnosticCategory::from_metadata(error),
        }
        .cause_label()
    }
}

impl From<StoreError> for BucketWriteDrainError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<MetadataError> for BucketWriteDrainError {
    fn from(error: MetadataError) -> Self {
        Self::Metadata(error)
    }
}

impl std::fmt::Debug for BucketWriteDrainError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BucketWriteDrainError")
            .field("cause_label", &self.diagnostic_cause_label())
            .finish()
    }
}

impl std::fmt::Display for BucketWriteDrainError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("bucket write drain failed")
    }
}

impl std::error::Error for BucketWriteDrainError {}

/// Logical response policy for a failed bucket-snapshot operation.
///
/// Metadata-command contention remains distinct because callers select either
/// `SlowDown` or `OperationAborted` according to the S3 operation. Physical
/// storage, database, routing, command-log, and RPC representations are not
/// exposed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BucketSnapshotLoadFailureKind {
    SlowDown,
    MetadataCommandContention,
    BucketNotEmpty,
    BucketNotFound {
        name: BucketName,
    },
    NoSuchUpload {
        upload_id: String,
    },
    InvalidVersioningTransition {
        from: crate::types::BucketVersioningState,
        to: crate::types::BucketVersioningState,
    },
    InternalError,
}

/// Opaque failure returned by public bucket-snapshot operations.
///
/// Callers may select an S3 response from [`Self::into_kind`] and emit the
/// bounded owner-provided diagnostic label. The concrete storage error is
/// classified and discarded before this value crosses the crate boundary.
pub struct BucketSnapshotLoadFailure {
    kind: BucketSnapshotLoadFailureKind,
    diagnostic_category: OperationFailureDiagnosticCategory,
}

impl BucketSnapshotLoadFailure {
    #[must_use]
    pub fn kind(&self) -> &BucketSnapshotLoadFailureKind {
        &self.kind
    }

    #[must_use]
    pub fn into_kind(self) -> BucketSnapshotLoadFailureKind {
        self.kind
    }

    /// Return a bounded storage-owned label for operator diagnostics.
    #[must_use]
    pub fn diagnostic_cause_label(&self) -> &'static str {
        self.diagnostic_category.cause_label()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn for_test(kind: BucketSnapshotLoadFailureKind) -> Self {
        Self {
            kind,
            diagnostic_category: OperationFailureDiagnosticCategory::Metadata(
                MetadataFailureDiagnosticCategory::Other,
            ),
        }
    }
}

impl From<BucketSnapshotLoadError> for BucketSnapshotLoadFailure {
    fn from(error: BucketSnapshotLoadError) -> Self {
        let diagnostic_category = match &error {
            BucketSnapshotLoadError::Store(error) => {
                OperationFailureDiagnosticCategory::from_store(error)
            }
            BucketSnapshotLoadError::Metadata(error) => {
                OperationFailureDiagnosticCategory::from_metadata(error)
            }
        };
        let kind = match error {
            BucketSnapshotLoadError::Store(error) => match error.operation_failure_class() {
                StoreOperationFailureClass::ResourceExhausted
                | StoreOperationFailureClass::RetryableConvergence => {
                    BucketSnapshotLoadFailureKind::SlowDown
                }
                StoreOperationFailureClass::MetadataCommandContention => {
                    BucketSnapshotLoadFailureKind::MetadataCommandContention
                }
                StoreOperationFailureClass::Other => BucketSnapshotLoadFailureKind::InternalError,
            },
            BucketSnapshotLoadError::Metadata(error) if error.is_command_contention() => {
                BucketSnapshotLoadFailureKind::MetadataCommandContention
            }
            BucketSnapshotLoadError::Metadata(MetadataError::BucketNotEmpty) => {
                BucketSnapshotLoadFailureKind::BucketNotEmpty
            }
            BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound { name }) => {
                BucketSnapshotLoadFailureKind::BucketNotFound { name }
            }
            BucketSnapshotLoadError::Metadata(MetadataError::NoSuchUpload { upload_id }) => {
                BucketSnapshotLoadFailureKind::NoSuchUpload { upload_id }
            }
            BucketSnapshotLoadError::Metadata(MetadataError::InvalidVersioningTransition {
                from,
                to,
            }) => BucketSnapshotLoadFailureKind::InvalidVersioningTransition { from, to },
            BucketSnapshotLoadError::Metadata(_) => BucketSnapshotLoadFailureKind::InternalError,
        };
        Self {
            kind,
            diagnostic_category,
        }
    }
}

impl std::fmt::Debug for BucketSnapshotLoadFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BucketSnapshotLoadFailure")
            .field("kind", &self.kind)
            .field("cause_label", &self.diagnostic_category.cause_label())
            .finish()
    }
}

impl std::fmt::Display for BucketSnapshotLoadFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("bucket snapshot load failed")
    }
}

impl std::error::Error for BucketSnapshotLoadFailure {}

/// Logical response policy for a failed bucket-write drain operation.
///
/// This deliberately omits database, PG, route, command-log, RPC, and I/O
/// representations. Adding an internal [`BucketWriteDrainError`] case requires
/// an explicit decision in its conversion to [`BucketWriteDrainFailure`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BucketWriteDrainFailureKind {
    OperationAborted,
    SlowDown,
    BucketNotEmpty,
    BucketNotFound { name: BucketName },
    InternalError,
}

/// Opaque failure returned by public bucket-write drain operations.
///
/// Callers may select an S3 response from [`Self::into_kind`] and may emit the
/// bounded owner-provided diagnostic label. The underlying storage error is
/// classified and discarded before this value crosses the crate boundary.
pub struct BucketWriteDrainFailure {
    kind: BucketWriteDrainFailureKind,
    diagnostic_category: OperationFailureDiagnosticCategory,
}

impl BucketWriteDrainFailure {
    #[must_use]
    pub fn kind(&self) -> &BucketWriteDrainFailureKind {
        &self.kind
    }

    #[must_use]
    pub fn into_kind(self) -> BucketWriteDrainFailureKind {
        self.kind
    }

    /// Return a bounded storage-owned label for operator diagnostics.
    #[must_use]
    pub fn diagnostic_cause_label(&self) -> &'static str {
        self.diagnostic_category.cause_label()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn for_test(kind: BucketWriteDrainFailureKind) -> Self {
        Self {
            kind,
            diagnostic_category: OperationFailureDiagnosticCategory::Metadata(
                MetadataFailureDiagnosticCategory::Other,
            ),
        }
    }
}

impl From<BucketWriteDrainError> for BucketWriteDrainFailure {
    fn from(error: BucketWriteDrainError) -> Self {
        let diagnostic_category = match &error {
            BucketWriteDrainError::Store(error) => {
                OperationFailureDiagnosticCategory::from_store(error)
            }
            BucketWriteDrainError::Metadata(error) => {
                OperationFailureDiagnosticCategory::from_metadata(error)
            }
        };
        let kind = match error {
            BucketWriteDrainError::Store(error) => match error.operation_failure_class() {
                StoreOperationFailureClass::MetadataCommandContention => {
                    BucketWriteDrainFailureKind::OperationAborted
                }
                StoreOperationFailureClass::ResourceExhausted
                | StoreOperationFailureClass::RetryableConvergence => {
                    BucketWriteDrainFailureKind::SlowDown
                }
                StoreOperationFailureClass::Other => BucketWriteDrainFailureKind::InternalError,
            },
            BucketWriteDrainError::Metadata(error) if error.is_command_contention() => {
                BucketWriteDrainFailureKind::OperationAborted
            }
            BucketWriteDrainError::Metadata(MetadataError::BucketNotEmpty) => {
                BucketWriteDrainFailureKind::BucketNotEmpty
            }
            BucketWriteDrainError::Metadata(MetadataError::BucketNotFound { name }) => {
                BucketWriteDrainFailureKind::BucketNotFound { name }
            }
            BucketWriteDrainError::Metadata(_) => BucketWriteDrainFailureKind::InternalError,
        };
        Self {
            kind,
            diagnostic_category,
        }
    }
}

impl std::fmt::Debug for BucketWriteDrainFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BucketWriteDrainFailure")
            .field("kind", &self.kind)
            .field("cause_label", &self.diagnostic_category.cause_label())
            .finish()
    }
}

impl std::fmt::Display for BucketWriteDrainFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("bucket write drain failed")
    }
}

impl std::error::Error for BucketWriteDrainFailure {}

pub enum ObjectPgActionError {
    Store(StoreError),
    Metadata(MetadataError),
    InvalidRequest { reason: String },
    StaleObjectReadSubject,
    StaleDirectPutCommitSnapshot,
    StaleStreamFinalizeSnapshot,
    StaleMultipartCompletionSnapshot,
    MultipartConditionalRequestConflict,
}

impl ObjectPgActionError {
    /// Return a bounded storage-owned label for operator diagnostics.
    #[must_use]
    pub fn diagnostic_cause_label(&self) -> &'static str {
        match self {
            Self::Store(error) => error.diagnostic_cause_label(),
            Self::Metadata(_) => "metadata_failure",
            Self::InvalidRequest { .. } => "invalid_request",
            Self::StaleObjectReadSubject => "stale_object_read_subject",
            Self::StaleDirectPutCommitSnapshot => "stale_direct_put_commit_snapshot",
            Self::StaleStreamFinalizeSnapshot => "stale_stream_finalize_snapshot",
            Self::StaleMultipartCompletionSnapshot => "stale_multipart_completion_snapshot",
            Self::MultipartConditionalRequestConflict => "multipart_conditional_request_conflict",
        }
    }

    /// Report whether the underlying object-PG operation encountered metadata
    /// command contention without exposing its storage representation.
    #[must_use]
    pub fn is_metadata_command_contention(&self) -> bool {
        match self {
            Self::Store(error) => {
                error.operation_failure_class()
                    == StoreOperationFailureClass::MetadataCommandContention
            }
            Self::Metadata(error) => error.is_command_contention(),
            Self::InvalidRequest { .. }
            | Self::StaleObjectReadSubject
            | Self::StaleDirectPutCommitSnapshot
            | Self::StaleStreamFinalizeSnapshot
            | Self::StaleMultipartCompletionSnapshot
            | Self::MultipartConditionalRequestConflict => false,
        }
    }
}

impl From<StoreError> for ObjectPgActionError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<MetadataError> for ObjectPgActionError {
    fn from(error: MetadataError) -> Self {
        Self::Metadata(error)
    }
}

impl std::fmt::Debug for ObjectPgActionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectPgActionError")
            .field("cause_label", &self.diagnostic_cause_label())
            .finish()
    }
}

impl std::fmt::Display for ObjectPgActionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("object placement-group action failed")
    }
}

impl std::error::Error for ObjectPgActionError {}

/// Exhaustive logical outcome of an object-read storage operation.
///
/// This deliberately omits PG, node, route, database, and command-log
/// representations. Callers must make an explicit protocol decision for every
/// outcome when this enum grows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectReadFailureKind {
    ObjectNotFound,
    ResourceExhausted,
    MetadataCommandContention,
    RetryableConvergence,
    InternalError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectOperationFailureDiagnosticCategory {
    Operation(OperationFailureDiagnosticCategory),
    InvalidRequest,
    StaleObjectReadSubject,
    UnexpectedObjectOperationOutcome,
}

impl ObjectOperationFailureDiagnosticCategory {
    const fn cause_label(self) -> &'static str {
        match self {
            Self::Operation(category) => category.cause_label(),
            Self::InvalidRequest => "invalid_request",
            Self::StaleObjectReadSubject => "stale_object_read_subject",
            Self::UnexpectedObjectOperationOutcome => "unexpected_object_operation_outcome",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectOperationFailureKind {
    ObjectNotFound,
    ResourceExhausted,
    MetadataCommandContention,
    RetryableConvergence,
    InternalError,
}

fn classify_object_pg_action(
    error: ObjectPgActionError,
) -> (
    ObjectOperationFailureKind,
    ObjectOperationFailureDiagnosticCategory,
) {
    let diagnostic_category = match &error {
        ObjectPgActionError::Store(error) => ObjectOperationFailureDiagnosticCategory::Operation(
            OperationFailureDiagnosticCategory::from_store(error),
        ),
        ObjectPgActionError::Metadata(error) => {
            ObjectOperationFailureDiagnosticCategory::Operation(
                OperationFailureDiagnosticCategory::from_metadata(error),
            )
        }
        ObjectPgActionError::InvalidRequest { .. } => {
            ObjectOperationFailureDiagnosticCategory::InvalidRequest
        }
        ObjectPgActionError::StaleObjectReadSubject => {
            ObjectOperationFailureDiagnosticCategory::StaleObjectReadSubject
        }
        ObjectPgActionError::StaleDirectPutCommitSnapshot
        | ObjectPgActionError::StaleStreamFinalizeSnapshot
        | ObjectPgActionError::StaleMultipartCompletionSnapshot
        | ObjectPgActionError::MultipartConditionalRequestConflict => {
            ObjectOperationFailureDiagnosticCategory::UnexpectedObjectOperationOutcome
        }
    };
    let kind = match &error {
        ObjectPgActionError::Store(error) => match error.operation_failure_class() {
            StoreOperationFailureClass::ResourceExhausted => {
                ObjectOperationFailureKind::ResourceExhausted
            }
            StoreOperationFailureClass::MetadataCommandContention => {
                ObjectOperationFailureKind::MetadataCommandContention
            }
            StoreOperationFailureClass::RetryableConvergence => {
                ObjectOperationFailureKind::RetryableConvergence
            }
            StoreOperationFailureClass::Other => ObjectOperationFailureKind::InternalError,
        },
        ObjectPgActionError::Metadata(MetadataError::ObjectNotFound) => {
            ObjectOperationFailureKind::ObjectNotFound
        }
        ObjectPgActionError::Metadata(error) if error.is_command_contention() => {
            ObjectOperationFailureKind::MetadataCommandContention
        }
        ObjectPgActionError::Metadata(_)
        | ObjectPgActionError::InvalidRequest { .. }
        | ObjectPgActionError::StaleObjectReadSubject
        | ObjectPgActionError::StaleDirectPutCommitSnapshot
        | ObjectPgActionError::StaleStreamFinalizeSnapshot
        | ObjectPgActionError::StaleMultipartCompletionSnapshot
        | ObjectPgActionError::MultipartConditionalRequestConflict => {
            ObjectOperationFailureKind::InternalError
        }
    };
    (kind, diagnostic_category)
}

/// Opaque failure returned by public object-read capabilities.
///
/// The semantic kind is sufficient for request translation. The diagnostic
/// category is retained and rendered only as a bounded storage-owned label;
/// public formatting and the error chain never expose the underlying storage
/// implementation error.
pub struct ObjectReadFailure {
    kind: ObjectReadFailureKind,
    diagnostic_category: ObjectOperationFailureDiagnosticCategory,
}

impl ObjectReadFailure {
    #[must_use]
    pub const fn kind(&self) -> ObjectReadFailureKind {
        self.kind
    }

    #[must_use]
    pub const fn diagnostic_cause_label(&self) -> &'static str {
        self.diagnostic_category.cause_label()
    }

    pub(crate) fn from_object_pg_action(error: ObjectPgActionError) -> Self {
        let (operation_kind, diagnostic_category) = classify_object_pg_action(error);
        let kind = match operation_kind {
            ObjectOperationFailureKind::ObjectNotFound => ObjectReadFailureKind::ObjectNotFound,
            ObjectOperationFailureKind::ResourceExhausted => {
                ObjectReadFailureKind::ResourceExhausted
            }
            ObjectOperationFailureKind::MetadataCommandContention => {
                ObjectReadFailureKind::MetadataCommandContention
            }
            ObjectOperationFailureKind::RetryableConvergence => {
                ObjectReadFailureKind::RetryableConvergence
            }
            ObjectOperationFailureKind::InternalError => ObjectReadFailureKind::InternalError,
        };
        Self {
            kind,
            diagnostic_category,
        }
    }

    pub(crate) fn from_store(error: StoreError) -> Self {
        Self::from_object_pg_action(ObjectPgActionError::Store(error))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) const fn for_test(kind: ObjectReadFailureKind) -> Self {
        let diagnostic_category = match kind {
            ObjectReadFailureKind::ObjectNotFound => {
                ObjectOperationFailureDiagnosticCategory::Operation(
                    OperationFailureDiagnosticCategory::Metadata(
                        MetadataFailureDiagnosticCategory::Other,
                    ),
                )
            }
            ObjectReadFailureKind::ResourceExhausted => {
                ObjectOperationFailureDiagnosticCategory::Operation(
                    OperationFailureDiagnosticCategory::Store(
                        StoreFailureDiagnosticCategory::ResourceExhausted,
                    ),
                )
            }
            ObjectReadFailureKind::MetadataCommandContention => {
                ObjectOperationFailureDiagnosticCategory::Operation(
                    OperationFailureDiagnosticCategory::Metadata(
                        MetadataFailureDiagnosticCategory::ObjectGenerationReservationConflict,
                    ),
                )
            }
            ObjectReadFailureKind::RetryableConvergence => {
                ObjectOperationFailureDiagnosticCategory::Operation(
                    OperationFailureDiagnosticCategory::Store(
                        StoreFailureDiagnosticCategory::Topology,
                    ),
                )
            }
            ObjectReadFailureKind::InternalError => {
                ObjectOperationFailureDiagnosticCategory::Operation(
                    OperationFailureDiagnosticCategory::Store(
                        StoreFailureDiagnosticCategory::InternalInvariant,
                    ),
                )
            }
        };
        Self {
            kind,
            diagnostic_category,
        }
    }
}

impl std::fmt::Debug for ObjectReadFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectReadFailure")
            .field("kind", &self.kind)
            .field("cause_label", &self.diagnostic_cause_label())
            .finish()
    }
}

impl std::fmt::Display for ObjectReadFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("object read failed")
    }
}

impl std::error::Error for ObjectReadFailure {}

/// Exhaustive logical outcome of an object-metadata mutation.
///
/// This deliberately omits PG, node, route, database, command-log, and RPC
/// representations. Callers may select operation-specific S3 responses for
/// absence and contention, but cannot inspect the storage implementation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectMetadataMutationFailureKind {
    ObjectNotFound,
    ResourceExhausted,
    MetadataCommandContention,
    RetryableConvergence,
    InternalError,
}

/// Opaque failure returned by public object-metadata mutation capabilities.
///
/// The semantic kind is sufficient for request translation. The diagnostic
/// category is retained and rendered only as a bounded storage-owned label;
/// public formatting and the error chain never expose the underlying storage
/// implementation error.
pub struct ObjectMetadataMutationFailure {
    kind: ObjectMetadataMutationFailureKind,
    diagnostic_category: ObjectOperationFailureDiagnosticCategory,
}

impl ObjectMetadataMutationFailure {
    #[must_use]
    pub const fn kind(&self) -> ObjectMetadataMutationFailureKind {
        self.kind
    }

    #[must_use]
    pub const fn diagnostic_cause_label(&self) -> &'static str {
        self.diagnostic_category.cause_label()
    }

    pub(crate) fn from_object_pg_action(error: ObjectPgActionError) -> Self {
        let (operation_kind, diagnostic_category) = classify_object_pg_action(error);
        let kind = match operation_kind {
            ObjectOperationFailureKind::ObjectNotFound => {
                ObjectMetadataMutationFailureKind::ObjectNotFound
            }
            ObjectOperationFailureKind::ResourceExhausted => {
                ObjectMetadataMutationFailureKind::ResourceExhausted
            }
            ObjectOperationFailureKind::MetadataCommandContention => {
                ObjectMetadataMutationFailureKind::MetadataCommandContention
            }
            ObjectOperationFailureKind::RetryableConvergence => {
                ObjectMetadataMutationFailureKind::RetryableConvergence
            }
            ObjectOperationFailureKind::InternalError => {
                ObjectMetadataMutationFailureKind::InternalError
            }
        };
        Self {
            kind,
            diagnostic_category,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) const fn for_test(kind: ObjectMetadataMutationFailureKind) -> Self {
        let read_kind = match kind {
            ObjectMetadataMutationFailureKind::ObjectNotFound => {
                ObjectReadFailureKind::ObjectNotFound
            }
            ObjectMetadataMutationFailureKind::ResourceExhausted => {
                ObjectReadFailureKind::ResourceExhausted
            }
            ObjectMetadataMutationFailureKind::MetadataCommandContention => {
                ObjectReadFailureKind::MetadataCommandContention
            }
            ObjectMetadataMutationFailureKind::RetryableConvergence => {
                ObjectReadFailureKind::RetryableConvergence
            }
            ObjectMetadataMutationFailureKind::InternalError => {
                ObjectReadFailureKind::InternalError
            }
        };
        let read_failure = ObjectReadFailure::for_test(read_kind);
        Self {
            kind,
            diagnostic_category: read_failure.diagnostic_category,
        }
    }
}

impl std::fmt::Debug for ObjectMetadataMutationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectMetadataMutationFailure")
            .field("kind", &self.kind)
            .field("cause_label", &self.diagnostic_cause_label())
            .finish()
    }
}

impl std::fmt::Display for ObjectMetadataMutationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("object metadata mutation failed")
    }
}

impl std::error::Error for ObjectMetadataMutationFailure {}

/// Exhaustive logical outcome of an admitted stream-upload operation.
///
/// Session state and the client-visible invalid-request reason are logical
/// protocol inputs. PG, route, node, database, command-log, and RPC details
/// remain owned by storage and are reduced to a bounded diagnostic label.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamUploadFailureKind {
    NoSuchUpload,
    SessionNotFound,
    SessionNotInProgress,
    SegmentConflict,
    ResourceExhausted,
    MetadataCommandContention,
    RetryableConvergence,
    InvalidRequest,
    InternalError,
}

/// Opaque failure returned by public admitted stream-upload capabilities.
enum StreamUploadFailureOutcome {
    NoSuchUpload { upload_id: String },
    SessionNotFound,
    SessionNotInProgress,
    SegmentConflict,
    ResourceExhausted,
    MetadataCommandContention,
    RetryableConvergence,
    InvalidRequest { reason: String },
    InternalError,
}

pub struct StreamUploadFailure {
    outcome: StreamUploadFailureOutcome,
    diagnostic_category: ObjectOperationFailureDiagnosticCategory,
}

impl StreamUploadFailure {
    #[must_use]
    pub const fn kind(&self) -> StreamUploadFailureKind {
        match &self.outcome {
            StreamUploadFailureOutcome::NoSuchUpload { .. } => {
                StreamUploadFailureKind::NoSuchUpload
            }
            StreamUploadFailureOutcome::SessionNotFound => StreamUploadFailureKind::SessionNotFound,
            StreamUploadFailureOutcome::SessionNotInProgress => {
                StreamUploadFailureKind::SessionNotInProgress
            }
            StreamUploadFailureOutcome::SegmentConflict => StreamUploadFailureKind::SegmentConflict,
            StreamUploadFailureOutcome::ResourceExhausted => {
                StreamUploadFailureKind::ResourceExhausted
            }
            StreamUploadFailureOutcome::MetadataCommandContention => {
                StreamUploadFailureKind::MetadataCommandContention
            }
            StreamUploadFailureOutcome::RetryableConvergence => {
                StreamUploadFailureKind::RetryableConvergence
            }
            StreamUploadFailureOutcome::InvalidRequest { .. } => {
                StreamUploadFailureKind::InvalidRequest
            }
            StreamUploadFailureOutcome::InternalError => StreamUploadFailureKind::InternalError,
        }
    }

    #[must_use]
    pub const fn diagnostic_cause_label(&self) -> &'static str {
        self.diagnostic_category.cause_label()
    }

    #[must_use]
    pub fn no_such_upload_id(&self) -> Option<&str> {
        match &self.outcome {
            StreamUploadFailureOutcome::NoSuchUpload { upload_id } => Some(upload_id),
            _ => None,
        }
    }

    #[must_use]
    pub fn invalid_request_reason(&self) -> Option<&str> {
        match &self.outcome {
            StreamUploadFailureOutcome::InvalidRequest { reason } => Some(reason),
            _ => None,
        }
    }

    pub(crate) fn from_object_pg_action(error: ObjectPgActionError) -> Self {
        let diagnostic_category = match &error {
            ObjectPgActionError::Store(error) => {
                ObjectOperationFailureDiagnosticCategory::Operation(
                    OperationFailureDiagnosticCategory::from_store(error),
                )
            }
            ObjectPgActionError::Metadata(error) => {
                ObjectOperationFailureDiagnosticCategory::Operation(
                    OperationFailureDiagnosticCategory::from_metadata(error),
                )
            }
            ObjectPgActionError::InvalidRequest { .. } => {
                ObjectOperationFailureDiagnosticCategory::InvalidRequest
            }
            ObjectPgActionError::StaleObjectReadSubject => {
                ObjectOperationFailureDiagnosticCategory::StaleObjectReadSubject
            }
            ObjectPgActionError::StaleDirectPutCommitSnapshot
            | ObjectPgActionError::StaleStreamFinalizeSnapshot
            | ObjectPgActionError::StaleMultipartCompletionSnapshot
            | ObjectPgActionError::MultipartConditionalRequestConflict => {
                ObjectOperationFailureDiagnosticCategory::UnexpectedObjectOperationOutcome
            }
        };
        let outcome = match error {
            ObjectPgActionError::Store(error) => match error.operation_failure_class() {
                StoreOperationFailureClass::ResourceExhausted => {
                    StreamUploadFailureOutcome::ResourceExhausted
                }
                StoreOperationFailureClass::MetadataCommandContention => {
                    StreamUploadFailureOutcome::MetadataCommandContention
                }
                StoreOperationFailureClass::RetryableConvergence => {
                    StreamUploadFailureOutcome::RetryableConvergence
                }
                StoreOperationFailureClass::Other => StreamUploadFailureOutcome::InternalError,
            },
            ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { upload_id }) => {
                StreamUploadFailureOutcome::NoSuchUpload { upload_id }
            }
            ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound { .. }) => {
                StreamUploadFailureOutcome::SessionNotFound
            }
            ObjectPgActionError::Metadata(MetadataError::StreamSessionNotInProgress { .. }) => {
                StreamUploadFailureOutcome::SessionNotInProgress
            }
            ObjectPgActionError::Metadata(MetadataError::StreamSegmentConflict { .. }) => {
                StreamUploadFailureOutcome::SegmentConflict
            }
            ObjectPgActionError::Metadata(error) if error.is_command_contention() => {
                StreamUploadFailureOutcome::MetadataCommandContention
            }
            ObjectPgActionError::InvalidRequest { reason } => {
                StreamUploadFailureOutcome::InvalidRequest { reason }
            }
            ObjectPgActionError::Metadata(_)
            | ObjectPgActionError::StaleObjectReadSubject
            | ObjectPgActionError::StaleDirectPutCommitSnapshot
            | ObjectPgActionError::StaleStreamFinalizeSnapshot
            | ObjectPgActionError::StaleMultipartCompletionSnapshot
            | ObjectPgActionError::MultipartConditionalRequestConflict => {
                StreamUploadFailureOutcome::InternalError
            }
        };
        Self {
            outcome,
            diagnostic_category,
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn for_test(kind: StreamUploadFailureKind) -> Self {
        let error = match kind {
            StreamUploadFailureKind::NoSuchUpload => {
                ObjectPgActionError::Metadata(MetadataError::NoSuchUpload {
                    upload_id: "opaque-test-upload-id".to_string(),
                })
            }
            StreamUploadFailureKind::SessionNotFound => {
                ObjectPgActionError::Metadata(MetadataError::StreamSessionNotFound {
                    session_id: "private-test-session-id".to_string(),
                })
            }
            StreamUploadFailureKind::SessionNotInProgress => {
                ObjectPgActionError::Metadata(MetadataError::StreamSessionNotInProgress {
                    state: 7,
                })
            }
            StreamUploadFailureKind::SegmentConflict => {
                ObjectPgActionError::Metadata(MetadataError::StreamSegmentConflict {
                    segment_index: 11,
                })
            }
            StreamUploadFailureKind::ResourceExhausted => ObjectPgActionError::Store(
                StoreError::storage_node_resource_exhausted(7, "private test operation"),
            ),
            StreamUploadFailureKind::MetadataCommandContention => {
                ObjectPgActionError::Store(StoreError::MetadataCommandContention {
                    context: "private test contention",
                })
            }
            StreamUploadFailureKind::RetryableConvergence => {
                ObjectPgActionError::Store(StoreError::RouteMapExpired {
                    cluster_epoch: ClusterEpoch::INITIAL,
                    valid_until_ms: 1,
                    now_ms: 2,
                })
            }
            StreamUploadFailureKind::InvalidRequest => ObjectPgActionError::InvalidRequest {
                reason: "opaque test invalid request".to_string(),
            },
            StreamUploadFailureKind::InternalError => ObjectPgActionError::Store(StoreError::Io {
                context: "private test stream operation",
                source: std::io::Error::other("private test stream source"),
            }),
        };
        Self::from_object_pg_action(error)
    }
}

impl std::fmt::Debug for StreamUploadFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamUploadFailure")
            .field("kind", &self.kind())
            .field("cause_label", &self.diagnostic_cause_label())
            .finish()
    }
}

impl std::fmt::Display for StreamUploadFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("stream upload failed")
    }
}

impl std::error::Error for StreamUploadFailure {}

#[cfg(test)]
mod tests {
    use super::*;

    fn remote_failure(code: StorageRpcErrorCode) -> StoreError {
        StoreError::StorageRpc {
            node_id: 7,
            operation: "test operation",
            failure: code,
            detail: StorageNodeFailureDetail::new("wire diagnostic must not drive caller policy"),
        }
    }

    #[test]
    fn storage_rpc_codes_map_to_semantic_failure_classes_inside_storage() {
        assert_eq!(
            remote_failure(StorageRpcErrorCode::StaleShardLocation).storage_node_failure_class(),
            Some(StorageNodeFailureClass::ShardLocationStale)
        );
        for code in [
            StorageRpcErrorCode::InactivePgRoute,
            StorageRpcErrorCode::NonActingSetAccess,
            StorageRpcErrorCode::WrongClusterEpoch,
        ] {
            assert_eq!(
                remote_failure(code).storage_node_failure_class(),
                Some(StorageNodeFailureClass::PgRouteUnavailable)
            );
        }
        assert_eq!(
            remote_failure(StorageRpcErrorCode::MetadataCommandContention)
                .storage_node_failure_class(),
            Some(StorageNodeFailureClass::MetadataCommandContention)
        );
        assert_eq!(
            remote_failure(StorageRpcErrorCode::MetadataTransferHistoricalRouteActive)
                .storage_node_failure_class(),
            Some(StorageNodeFailureClass::MetadataTransferHistoricalRouteActive)
        );
        for code in [
            StorageRpcErrorCode::TransportTimeout,
            StorageRpcErrorCode::TransportClosed,
        ] {
            assert_eq!(
                remote_failure(code).storage_node_failure_class(),
                Some(StorageNodeFailureClass::TransportInterrupted)
            );
        }
        for code in [
            StorageRpcErrorCode::FrameDecode,
            StorageRpcErrorCode::PayloadDecode,
            StorageRpcErrorCode::UnknownNode,
            StorageRpcErrorCode::UnknownPg,
            StorageRpcErrorCode::UnsupportedOperation,
            StorageRpcErrorCode::Internal,
            StorageRpcErrorCode::ResourceExhausted,
            StorageRpcErrorCode::ReclaimClaimNotFound,
            StorageRpcErrorCode::ShardDeleteInProgress,
            StorageRpcErrorCode::BucketWriteDrainConflict,
            StorageRpcErrorCode::BucketWriteDrainNotFound,
            StorageRpcErrorCode::ReclaimClaimConflict,
            StorageRpcErrorCode::NotFound,
            StorageRpcErrorCode::BucketWriteReservationConflict,
            StorageRpcErrorCode::BucketWriteReservationNotFound,
            StorageRpcErrorCode::ShardIntegrity,
            StorageRpcErrorCode::MultipartConditionalRequestConflict,
        ] {
            assert_eq!(remote_failure(code).storage_node_failure_class(), None);
        }
    }

    #[test]
    fn payload_absence_classification_unwraps_local_and_remote_storage_errors() {
        assert!(StoreError::NotFound.is_payload_not_found());
        assert!(remote_failure(StorageRpcErrorCode::NotFound).is_payload_not_found());
        assert!(!remote_failure(StorageRpcErrorCode::TransportTimeout).is_payload_not_found());

        let nested = StoreError::ShardStore {
            node_id: 7,
            pg_id: 11,
            cluster_epoch: ClusterEpoch::INITIAL,
            source: Box::new(remote_failure(StorageRpcErrorCode::NotFound)),
        };
        assert!(nested.is_payload_not_found());
    }

    #[test]
    fn semantic_failure_classification_unwraps_shard_store_context() {
        let failure = StoreError::ShardStore {
            node_id: 7,
            pg_id: 11,
            cluster_epoch: ClusterEpoch::INITIAL,
            source: Box::new(remote_failure(StorageRpcErrorCode::TransportClosed)),
        };

        assert_eq!(
            failure.storage_node_failure_class(),
            Some(StorageNodeFailureClass::TransportInterrupted)
        );
    }

    #[test]
    fn operation_failure_classification_is_storage_owned() {
        let epoch_two = ClusterEpoch::new(2).unwrap();

        for failure in [
            StoreError::ClusterMapHistoryReferenceLimitExceeded { count: 2, max: 1 },
            StoreError::storage_node_resource_exhausted(7, "resource test"),
            remote_failure(StorageRpcErrorCode::ResourceExhausted),
        ] {
            assert_eq!(
                failure.operation_failure_class(),
                StoreOperationFailureClass::ResourceExhausted
            );
        }

        for failure in [
            StoreError::MetadataCommandLogConflict {
                node_id: 1,
                pg_id: 2,
                cluster_epoch: ClusterEpoch::INITIAL,
                log_index: 3,
            },
            StoreError::MetadataCommandLogGap {
                node_id: 1,
                pg_id: 2,
                cluster_epoch: ClusterEpoch::INITIAL,
                log_index: 4,
                expected_log_index: 3,
            },
            StoreError::MetadataCommandPendingConflict {
                pg_id: 2,
                cluster_epoch: ClusterEpoch::INITIAL,
                existing_log_index: 3,
                candidate_log_index: 4,
            },
            StoreError::MetadataCommandContention { context: "test" },
            remote_failure(StorageRpcErrorCode::MetadataCommandContention),
        ] {
            assert_eq!(
                failure.operation_failure_class(),
                StoreOperationFailureClass::MetadataCommandContention
            );
        }

        for failure in [
            StoreError::StalePayloadOperation {
                pg_id: 2,
                operation_epoch: ClusterEpoch::INITIAL,
                current_epoch: epoch_two,
            },
            StoreError::StaleMetadataPrimaryBridge {
                metadata_node_id: 1,
                operation_epoch: ClusterEpoch::INITIAL,
                current_epoch: epoch_two,
            },
            StoreError::StaleMetadataOperation {
                pg_id: 2,
                operation_epoch: ClusterEpoch::INITIAL,
                current_epoch: epoch_two,
            },
            StoreError::StaleMetadataRoute {
                pg_id: 2,
                route_epoch: ClusterEpoch::INITIAL,
                current_epoch: epoch_two,
            },
            StoreError::StaleMetadataReadProof {
                node_id: 1,
                pg_id: 2,
            },
            StoreError::RouteMapExpired {
                cluster_epoch: ClusterEpoch::INITIAL,
                valid_until_ms: 1,
                now_ms: 2,
            },
            StoreError::RouteAdmissionClusterMismatch {
                admitted_epoch: ClusterEpoch::INITIAL,
                operation_epoch: epoch_two,
            },
            StoreError::StaleMetadataCommand {
                node_id: 1,
                pg_id: 2,
                command_epoch: ClusterEpoch::INITIAL,
                current_epoch: epoch_two,
            },
            StoreError::StaleShardOperation {
                node_id: 1,
                pg_id: 2,
                operation_epoch: ClusterEpoch::INITIAL,
                current_epoch: epoch_two,
            },
            StoreError::StaleShardLocation {
                node_id: 1,
                pg_id: 2,
                location_epoch: ClusterEpoch::INITIAL,
                current_epoch: epoch_two,
            },
            StoreError::PgNotActive {
                pg_id: 2,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: PgState::Peering,
            },
            StoreError::ShardPgNotActive {
                node_id: 1,
                pg_id: 2,
                cluster_epoch: ClusterEpoch::INITIAL,
                state: PgState::Peering,
            },
            remote_failure(StorageRpcErrorCode::StaleShardLocation),
            remote_failure(StorageRpcErrorCode::InactivePgRoute),
            remote_failure(StorageRpcErrorCode::NonActingSetAccess),
            remote_failure(StorageRpcErrorCode::WrongClusterEpoch),
        ] {
            assert_eq!(
                failure.operation_failure_class(),
                StoreOperationFailureClass::RetryableConvergence
            );
        }

        for code in [
            StorageRpcErrorCode::FrameDecode,
            StorageRpcErrorCode::PayloadDecode,
            StorageRpcErrorCode::UnknownNode,
            StorageRpcErrorCode::UnknownPg,
            StorageRpcErrorCode::UnsupportedOperation,
            StorageRpcErrorCode::Internal,
            StorageRpcErrorCode::ReclaimClaimNotFound,
            StorageRpcErrorCode::ShardDeleteInProgress,
            StorageRpcErrorCode::BucketWriteDrainConflict,
            StorageRpcErrorCode::BucketWriteDrainNotFound,
            StorageRpcErrorCode::ReclaimClaimConflict,
            StorageRpcErrorCode::NotFound,
            StorageRpcErrorCode::BucketWriteReservationConflict,
            StorageRpcErrorCode::BucketWriteReservationNotFound,
            StorageRpcErrorCode::ShardIntegrity,
            StorageRpcErrorCode::MultipartConditionalRequestConflict,
            StorageRpcErrorCode::MetadataTransferHistoricalRouteActive,
            StorageRpcErrorCode::TransportTimeout,
            StorageRpcErrorCode::TransportClosed,
        ] {
            assert_eq!(
                remote_failure(code).operation_failure_class(),
                StoreOperationFailureClass::Other,
                "unexpected request classification for {code:?}"
            );
        }

        for class in [
            StoreOperationFailureClass::ResourceExhausted,
            StoreOperationFailureClass::MetadataCommandContention,
            StoreOperationFailureClass::RetryableConvergence,
            StoreOperationFailureClass::Other,
        ] {
            let source = crate::test_support::store_error_for_operation_failure_class(class);
            let nested = StoreError::ShardStore {
                node_id: 1,
                pg_id: 2,
                cluster_epoch: ClusterEpoch::INITIAL,
                source: Box::new(source),
            };
            assert_eq!(nested.operation_failure_class(), class);
        }
    }

    #[test]
    fn store_failure_reports_bounded_category_without_raw_detail() {
        const SECRET: &str = "secret-storage-operation";
        let failure = StoreFailure::from(StoreError::Io {
            context: SECRET,
            source: std::io::Error::other("secret operating-system detail"),
        });

        assert_eq!(failure.class(), StoreOperationFailureClass::Other);
        assert_eq!(failure.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(failure.to_string(), "storage operation failed");
        let debug = format!("{failure:?}");
        assert!(debug.contains("store_io_failure"));
        assert!(!debug.contains(SECRET));
        assert!(!debug.contains("operating-system"));
        assert!(std::error::Error::source(&failure).is_none());
    }

    #[test]
    fn store_failure_diagnostic_categories_distinguish_failure_domains() {
        let cases = [
            (
                StoreError::NotFound,
                StoreOperationFailureClass::Other,
                "store_not_found",
            ),
            (
                StoreError::IntegrityError {
                    expected: 1,
                    actual: 2,
                },
                StoreOperationFailureClass::Other,
                "store_integrity_failure",
            ),
            (
                StoreError::PgNotFound { pg_id: 7 },
                StoreOperationFailureClass::Other,
                "store_topology_failure",
            ),
            (
                StoreError::MetadataCommandLogConflict {
                    node_id: 1,
                    pg_id: 2,
                    cluster_epoch: ClusterEpoch::INITIAL,
                    log_index: 3,
                },
                StoreOperationFailureClass::MetadataCommandContention,
                "store_metadata_command_contention",
            ),
            (
                StoreError::PgSchemaInvalid {
                    reason: "secret schema detail".to_string(),
                },
                StoreOperationFailureClass::Other,
                "store_schema_failure",
            ),
            (
                StoreError::Db {
                    context: "secret database operation",
                    source: DatabaseError::new("secret database detail"),
                },
                StoreOperationFailureClass::Other,
                "store_database_failure",
            ),
            (
                remote_failure(StorageRpcErrorCode::TransportClosed),
                StoreOperationFailureClass::Other,
                "store_rpc_transport_failure",
            ),
            (
                remote_failure(StorageRpcErrorCode::FrameDecode),
                StoreOperationFailureClass::Other,
                "store_rpc_protocol_failure",
            ),
        ];

        for (error, expected_class, expected_label) in cases {
            let failure = StoreFailure::from(error);
            assert_eq!(failure.class(), expected_class);
            assert_eq!(failure.diagnostic_cause_label(), expected_label);
            let rendered = format!("{failure:?} {failure}");
            assert!(!rendered.contains("secret"));
            assert!(std::error::Error::source(&failure).is_none());
        }

        let nested = StoreFailure::from(StoreError::ShardStore {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: ClusterEpoch::INITIAL,
            source: Box::new(StoreError::Db {
                context: "nested secret operation",
                source: DatabaseError::new("nested secret detail"),
            }),
        });
        assert_eq!(nested.diagnostic_cause_label(), "store_database_failure");
        assert!(!format!("{nested:?}").contains("secret"));
    }

    #[test]
    fn bucket_write_drain_error_debug_redacts_nested_diagnostic() {
        const SECRET_CONTEXT: &str = "secret bucket drain operation";
        const SECRET_SOURCE: &str = "secret bucket drain source";
        let error = BucketWriteDrainError::Store(StoreError::Io {
            context: SECRET_CONTEXT,
            source: std::io::Error::other(SECRET_SOURCE),
        });

        assert_eq!(error.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(error.to_string(), "bucket write drain failed");
        let debug = format!("{error:?}");
        assert!(debug.contains("store_io_failure"));
        assert!(!debug.contains(SECRET_CONTEXT));
        assert!(!debug.contains(SECRET_SOURCE));
        assert!(std::error::Error::source(&error).is_none());
    }

    #[test]
    fn bucket_snapshot_errors_convert_to_exhaustive_logical_failures() {
        let failure = |error| BucketSnapshotLoadFailure::from(error).into_kind();

        for class in [
            StoreOperationFailureClass::ResourceExhausted,
            StoreOperationFailureClass::RetryableConvergence,
        ] {
            assert_eq!(
                failure(BucketSnapshotLoadError::Store(
                    crate::test_support::store_error_for_operation_failure_class(class),
                )),
                BucketSnapshotLoadFailureKind::SlowDown
            );
        }
        assert_eq!(
            failure(BucketSnapshotLoadError::Store(
                crate::test_support::store_error_for_operation_failure_class(
                    StoreOperationFailureClass::MetadataCommandContention,
                ),
            )),
            BucketSnapshotLoadFailureKind::MetadataCommandContention
        );
        assert_eq!(
            failure(BucketSnapshotLoadError::Store(StoreError::NotFound)),
            BucketSnapshotLoadFailureKind::InternalError
        );

        let bucket = BucketName::try_from("logical-snapshot-bucket").unwrap();
        assert_eq!(
            failure(BucketSnapshotLoadError::Metadata(
                MetadataError::StaleBucketMetadataCommand {
                    name: bucket.clone(),
                    bucket_execution_generation: 9,
                },
            )),
            BucketSnapshotLoadFailureKind::MetadataCommandContention
        );
        assert_eq!(
            failure(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketNotEmpty
            )),
            BucketSnapshotLoadFailureKind::BucketNotEmpty
        );
        assert_eq!(
            failure(BucketSnapshotLoadError::Metadata(
                MetadataError::BucketNotFound {
                    name: bucket.clone(),
                },
            )),
            BucketSnapshotLoadFailureKind::BucketNotFound { name: bucket }
        );
        assert_eq!(
            failure(BucketSnapshotLoadError::Metadata(
                MetadataError::NoSuchUpload {
                    upload_id: "missing-upload".to_string(),
                },
            )),
            BucketSnapshotLoadFailureKind::NoSuchUpload {
                upload_id: "missing-upload".to_string(),
            }
        );
        assert_eq!(
            failure(BucketSnapshotLoadError::Metadata(
                MetadataError::InvalidVersioningTransition {
                    from: crate::BucketVersioningState::Suspended,
                    to: crate::BucketVersioningState::Disabled,
                },
            )),
            BucketSnapshotLoadFailureKind::InvalidVersioningTransition {
                from: crate::BucketVersioningState::Suspended,
                to: crate::BucketVersioningState::Disabled,
            }
        );
        assert_eq!(
            failure(BucketSnapshotLoadError::Metadata(
                MetadataError::ObjectNotFound
            )),
            BucketSnapshotLoadFailureKind::InternalError
        );
    }

    #[test]
    fn bucket_snapshot_failure_discards_raw_diagnostic_detail() {
        const SECRET_CONTEXT: &str = "secret bucket snapshot operation";
        const SECRET_SOURCE: &str = "secret bucket snapshot source";
        let failure =
            BucketSnapshotLoadFailure::from(BucketSnapshotLoadError::Store(StoreError::Io {
                context: SECRET_CONTEXT,
                source: std::io::Error::other(SECRET_SOURCE),
            }));

        assert_eq!(
            failure.kind(),
            &BucketSnapshotLoadFailureKind::InternalError
        );
        assert_eq!(failure.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(failure.to_string(), "bucket snapshot load failed");
        let debug = format!("{failure:?}");
        assert!(debug.contains("store_io_failure"));
        assert!(!debug.contains(SECRET_CONTEXT));
        assert!(!debug.contains(SECRET_SOURCE));
        assert!(std::error::Error::source(&failure).is_none());

        let metadata =
            BucketSnapshotLoadFailure::from(BucketSnapshotLoadError::Metadata(MetadataError::Db {
                context: SECRET_CONTEXT,
                source: DatabaseError::new(SECRET_SOURCE),
            }));
        assert_eq!(metadata.diagnostic_cause_label(), "metadata_db_error");
        let rendered = format!("{metadata:?} {metadata}");
        assert!(!rendered.contains(SECRET_CONTEXT));
        assert!(!rendered.contains(SECRET_SOURCE));
        assert!(std::error::Error::source(&metadata).is_none());
    }

    #[test]
    fn object_read_errors_convert_to_exhaustive_logical_failures() {
        let convert = |error| ObjectReadFailure::from_object_pg_action(error).kind();

        for (class, expected) in [
            (
                StoreOperationFailureClass::ResourceExhausted,
                ObjectReadFailureKind::ResourceExhausted,
            ),
            (
                StoreOperationFailureClass::MetadataCommandContention,
                ObjectReadFailureKind::MetadataCommandContention,
            ),
            (
                StoreOperationFailureClass::RetryableConvergence,
                ObjectReadFailureKind::RetryableConvergence,
            ),
            (
                StoreOperationFailureClass::Other,
                ObjectReadFailureKind::InternalError,
            ),
        ] {
            assert_eq!(
                convert(ObjectPgActionError::Store(
                    crate::test_support::store_error_for_operation_failure_class(class),
                )),
                expected
            );
        }

        assert_eq!(
            convert(ObjectPgActionError::Metadata(MetadataError::ObjectNotFound)),
            ObjectReadFailureKind::ObjectNotFound
        );
        assert_eq!(
            convert(ObjectPgActionError::Metadata(
                MetadataError::ObjectGenerationReservationConflict {
                    reservation_id: "opaque-reservation".to_string(),
                    generation_id: 7,
                },
            )),
            ObjectReadFailureKind::MetadataCommandContention
        );
        assert_eq!(
            convert(ObjectPgActionError::InvalidRequest {
                reason: "not valid for an admitted read".to_string(),
            }),
            ObjectReadFailureKind::InternalError
        );
        assert_eq!(
            convert(ObjectPgActionError::StaleObjectReadSubject),
            ObjectReadFailureKind::InternalError
        );
    }

    #[test]
    fn object_read_failure_discards_raw_diagnostic_detail() {
        const SECRET_CONTEXT: &str = "secret object read operation";
        const SECRET_SOURCE: &str = "secret object read source";
        let failure =
            ObjectReadFailure::from_object_pg_action(ObjectPgActionError::Store(StoreError::Io {
                context: SECRET_CONTEXT,
                source: std::io::Error::other(SECRET_SOURCE),
            }));

        assert_eq!(failure.kind(), ObjectReadFailureKind::InternalError);
        assert_eq!(failure.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(failure.to_string(), "object read failed");
        let debug = format!("{failure:?}");
        assert!(debug.contains("store_io_failure"));
        assert!(!debug.contains(SECRET_CONTEXT));
        assert!(!debug.contains(SECRET_SOURCE));
        assert!(std::error::Error::source(&failure).is_none());

        let metadata = ObjectReadFailure::from_object_pg_action(ObjectPgActionError::Metadata(
            MetadataError::Db {
                context: SECRET_CONTEXT,
                source: DatabaseError::new(SECRET_SOURCE),
            },
        ));
        assert_eq!(metadata.kind(), ObjectReadFailureKind::InternalError);
        assert_eq!(metadata.diagnostic_cause_label(), "metadata_db_error");
        let rendered = format!("{metadata:?} {metadata}");
        assert!(!rendered.contains(SECRET_CONTEXT));
        assert!(!rendered.contains(SECRET_SOURCE));
        assert!(std::error::Error::source(&metadata).is_none());
    }

    #[test]
    fn object_metadata_mutation_errors_convert_to_exhaustive_logical_failures() {
        let convert = |error| ObjectMetadataMutationFailure::from_object_pg_action(error).kind();

        for (class, expected) in [
            (
                StoreOperationFailureClass::ResourceExhausted,
                ObjectMetadataMutationFailureKind::ResourceExhausted,
            ),
            (
                StoreOperationFailureClass::MetadataCommandContention,
                ObjectMetadataMutationFailureKind::MetadataCommandContention,
            ),
            (
                StoreOperationFailureClass::RetryableConvergence,
                ObjectMetadataMutationFailureKind::RetryableConvergence,
            ),
            (
                StoreOperationFailureClass::Other,
                ObjectMetadataMutationFailureKind::InternalError,
            ),
        ] {
            assert_eq!(
                convert(ObjectPgActionError::Store(
                    crate::test_support::store_error_for_operation_failure_class(class),
                )),
                expected
            );
        }

        assert_eq!(
            convert(ObjectPgActionError::Metadata(MetadataError::ObjectNotFound)),
            ObjectMetadataMutationFailureKind::ObjectNotFound
        );
        assert_eq!(
            convert(ObjectPgActionError::Metadata(
                MetadataError::ObjectVersionReservationConflict {
                    version_id: crate::VersionId::from_u64(7),
                },
            )),
            ObjectMetadataMutationFailureKind::MetadataCommandContention
        );
        for error in [
            ObjectPgActionError::InvalidRequest {
                reason: "not valid for an admitted metadata mutation".to_string(),
            },
            ObjectPgActionError::StaleObjectReadSubject,
            ObjectPgActionError::StaleDirectPutCommitSnapshot,
            ObjectPgActionError::StaleStreamFinalizeSnapshot,
            ObjectPgActionError::StaleMultipartCompletionSnapshot,
            ObjectPgActionError::MultipartConditionalRequestConflict,
        ] {
            assert_eq!(
                convert(error),
                ObjectMetadataMutationFailureKind::InternalError
            );
        }
    }

    #[test]
    fn object_metadata_mutation_failure_discards_raw_diagnostic_detail() {
        const SECRET_CONTEXT: &str = "secret object mutation operation";
        const SECRET_SOURCE: &str = "secret object mutation source";
        let failure = ObjectMetadataMutationFailure::from_object_pg_action(
            ObjectPgActionError::Store(StoreError::Io {
                context: SECRET_CONTEXT,
                source: std::io::Error::other(SECRET_SOURCE),
            }),
        );

        assert_eq!(
            failure.kind(),
            ObjectMetadataMutationFailureKind::InternalError
        );
        assert_eq!(failure.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(failure.to_string(), "object metadata mutation failed");
        let debug = format!("{failure:?}");
        assert!(debug.contains("store_io_failure"));
        assert!(!debug.contains(SECRET_CONTEXT));
        assert!(!debug.contains(SECRET_SOURCE));
        assert!(std::error::Error::source(&failure).is_none());

        let metadata = ObjectMetadataMutationFailure::from_object_pg_action(
            ObjectPgActionError::Metadata(MetadataError::Db {
                context: SECRET_CONTEXT,
                source: DatabaseError::new(SECRET_SOURCE),
            }),
        );
        assert_eq!(metadata.diagnostic_cause_label(), "metadata_db_error");
        let rendered = format!("{metadata:?} {metadata}");
        assert!(!rendered.contains(SECRET_CONTEXT));
        assert!(!rendered.contains(SECRET_SOURCE));
        assert!(std::error::Error::source(&metadata).is_none());
    }

    #[test]
    fn stream_upload_errors_convert_to_exhaustive_logical_failures() {
        let convert = |error| StreamUploadFailure::from_object_pg_action(error).kind();

        for kind in [
            StreamUploadFailureKind::NoSuchUpload,
            StreamUploadFailureKind::SessionNotFound,
            StreamUploadFailureKind::SessionNotInProgress,
            StreamUploadFailureKind::SegmentConflict,
            StreamUploadFailureKind::ResourceExhausted,
            StreamUploadFailureKind::MetadataCommandContention,
            StreamUploadFailureKind::RetryableConvergence,
            StreamUploadFailureKind::InvalidRequest,
            StreamUploadFailureKind::InternalError,
        ] {
            assert_eq!(StreamUploadFailure::for_test(kind).kind(), kind);
        }

        let no_such_upload = StreamUploadFailure::for_test(StreamUploadFailureKind::NoSuchUpload);
        assert_eq!(
            no_such_upload.no_such_upload_id(),
            Some("opaque-test-upload-id")
        );
        let invalid = StreamUploadFailure::for_test(StreamUploadFailureKind::InvalidRequest);
        assert_eq!(
            invalid.invalid_request_reason(),
            Some("opaque test invalid request")
        );
        assert_eq!(
            convert(ObjectPgActionError::StaleStreamFinalizeSnapshot),
            StreamUploadFailureKind::InternalError
        );
    }

    #[test]
    fn stream_upload_failure_discards_raw_diagnostic_detail() {
        const SECRET_CONTEXT: &str = "secret stream upload operation";
        const SECRET_SOURCE: &str = "secret stream upload source";
        let failure = StreamUploadFailure::from_object_pg_action(ObjectPgActionError::Store(
            StoreError::Io {
                context: SECRET_CONTEXT,
                source: std::io::Error::other(SECRET_SOURCE),
            },
        ));

        assert_eq!(failure.kind(), StreamUploadFailureKind::InternalError);
        assert_eq!(failure.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(failure.to_string(), "stream upload failed");
        let rendered = format!("{failure:?} {failure}");
        assert!(rendered.contains("store_io_failure"));
        assert!(!rendered.contains(SECRET_CONTEXT));
        assert!(!rendered.contains(SECRET_SOURCE));
        assert!(std::error::Error::source(&failure).is_none());
    }

    #[test]
    fn bucket_write_drain_errors_convert_to_exhaustive_logical_failures() {
        let failure = |error| BucketWriteDrainFailure::from(error).into_kind();

        for class in [
            StoreOperationFailureClass::ResourceExhausted,
            StoreOperationFailureClass::RetryableConvergence,
        ] {
            assert_eq!(
                failure(BucketWriteDrainError::Store(
                    crate::test_support::store_error_for_operation_failure_class(class),
                )),
                BucketWriteDrainFailureKind::SlowDown
            );
        }
        assert_eq!(
            failure(BucketWriteDrainError::Store(
                crate::test_support::store_error_for_operation_failure_class(
                    StoreOperationFailureClass::MetadataCommandContention,
                ),
            )),
            BucketWriteDrainFailureKind::OperationAborted
        );
        assert_eq!(
            failure(BucketWriteDrainError::Store(StoreError::NotFound)),
            BucketWriteDrainFailureKind::InternalError
        );

        let bucket = BucketName::try_from("logical-drain-bucket").unwrap();
        assert_eq!(
            failure(BucketWriteDrainError::Metadata(
                MetadataError::StaleBucketMetadataCommand {
                    name: bucket.clone(),
                    bucket_execution_generation: 9,
                },
            )),
            BucketWriteDrainFailureKind::OperationAborted
        );
        assert_eq!(
            failure(BucketWriteDrainError::Metadata(
                MetadataError::BucketNotEmpty
            )),
            BucketWriteDrainFailureKind::BucketNotEmpty
        );
        assert_eq!(
            failure(BucketWriteDrainError::Metadata(
                MetadataError::BucketNotFound {
                    name: bucket.clone(),
                },
            )),
            BucketWriteDrainFailureKind::BucketNotFound { name: bucket }
        );
        assert_eq!(
            failure(BucketWriteDrainError::Metadata(
                MetadataError::ObjectNotFound
            )),
            BucketWriteDrainFailureKind::InternalError
        );
    }

    #[test]
    fn bucket_write_drain_failure_retains_bounded_metadata_diagnostic_category() {
        let bucket = BucketName::try_from("diagnostic-drain-bucket").unwrap();
        let cases = [
            (
                BucketWriteDrainError::Metadata(MetadataError::Db {
                    context: "secret database operation",
                    source: DatabaseError::new("secret database detail"),
                }),
                "metadata_db_error",
            ),
            (
                BucketWriteDrainError::Metadata(MetadataError::StaleBucketMetadataCommand {
                    name: bucket,
                    bucket_execution_generation: 7,
                }),
                "stale_bucket_metadata_command",
            ),
            (
                BucketWriteDrainError::Metadata(MetadataError::ObjectVersionReservationConflict {
                    version_id: crate::VersionId::from_u64(9),
                }),
                "object_version_reservation_conflict",
            ),
        ];

        for (error, expected_label) in cases {
            let failure = BucketWriteDrainFailure::from(error);
            assert_eq!(failure.diagnostic_cause_label(), expected_label);
            let rendered = format!("{failure:?} {failure}");
            assert!(!rendered.contains("secret"));
            assert!(std::error::Error::source(&failure).is_none());
        }
    }

    #[test]
    fn bucket_write_drain_failure_discards_raw_diagnostic_detail() {
        const SECRET_CONTEXT: &str = "secret bucket drain operation";
        const SECRET_SOURCE: &str = "secret bucket drain source";
        let failure = BucketWriteDrainFailure::from(BucketWriteDrainError::Store(StoreError::Io {
            context: SECRET_CONTEXT,
            source: std::io::Error::other(SECRET_SOURCE),
        }));

        assert_eq!(failure.kind(), &BucketWriteDrainFailureKind::InternalError);
        assert_eq!(failure.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(failure.to_string(), "bucket write drain failed");
        let debug = format!("{failure:?}");
        assert!(debug.contains("store_io_failure"));
        assert!(!debug.contains(SECRET_CONTEXT));
        assert!(!debug.contains(SECRET_SOURCE));
        assert!(std::error::Error::source(&failure).is_none());
    }

    #[test]
    fn exported_storage_wrapper_debug_redacts_nested_diagnostics() {
        const SECRET_CONTEXT: &str = "secret exported wrapper operation";
        const SECRET_SOURCE: &str = "secret exported wrapper source";
        let snapshot = BucketSnapshotLoadError::Store(StoreError::Io {
            context: SECRET_CONTEXT,
            source: std::io::Error::other(SECRET_SOURCE),
        });
        let object = ObjectPgActionError::InvalidRequest {
            reason: "secret invalid request detail".to_string(),
        };

        assert_eq!(snapshot.diagnostic_cause_label(), "store_io_failure");
        assert_eq!(snapshot.to_string(), "bucket snapshot load failed");
        assert_eq!(object.diagnostic_cause_label(), "invalid_request");
        assert_eq!(object.to_string(), "object placement-group action failed");
        for rendered in [format!("{snapshot:?}"), format!("{object:?}")] {
            assert!(!rendered.contains("secret"));
            assert!(!rendered.contains(SECRET_CONTEXT));
            assert!(!rendered.contains(SECRET_SOURCE));
        }
        assert!(std::error::Error::source(&snapshot).is_none());
        assert!(std::error::Error::source(&object).is_none());
    }

    #[test]
    fn metadata_contention_classification_is_storage_owned() {
        let bucket = crate::types::BucketName::try_from("bucket".to_string()).unwrap();
        let key = crate::types::ObjectKey::try_from("key".to_string()).unwrap();
        for failure in [
            MetadataError::BucketWriteReservationConflict {
                reservation_id: "reservation".to_string(),
            },
            MetadataError::BucketWriteReservationNotFound {
                reservation_id: "reservation".to_string(),
            },
            MetadataError::ObjectGenerationReservationConflict {
                reservation_id: "reservation".to_string(),
                generation_id: 1,
            },
            MetadataError::ObjectVersionReservationConflict {
                version_id: crate::types::VersionId::from_u64(1),
            },
            MetadataError::StaleBucketMetadataCommand {
                name: bucket.clone(),
                bucket_execution_generation: 1,
            },
            MetadataError::StaleObjectWriteCommand {
                bucket,
                key,
                write_sequence: 1,
                generation_id: Some(1),
            },
        ] {
            assert!(failure.is_command_contention());
            assert!(ObjectPgActionError::Metadata(failure).is_metadata_command_contention());
        }

        assert!(!MetadataError::ObjectNotFound.is_command_contention());
        assert!(!ObjectPgActionError::Store(StoreError::NotFound).is_metadata_command_contention());
        assert!(
            ObjectPgActionError::Store(StoreError::MetadataCommandContention { context: "test" })
                .is_metadata_command_contention()
        );
    }

    #[test]
    fn metadata_transfer_route_refresh_policy_is_storage_owned_and_exhaustive() {
        for code in [
            StorageRpcErrorCode::StaleShardLocation,
            StorageRpcErrorCode::MetadataCommandContention,
            StorageRpcErrorCode::MetadataTransferHistoricalRouteActive,
            StorageRpcErrorCode::TransportTimeout,
            StorageRpcErrorCode::TransportClosed,
        ] {
            assert!(
                PgMetadataTransferError::Store(remote_failure(code)).requires_route_refresh_retry(),
                "{code:?} should refresh the metadata-transfer route"
            );
        }
        for code in [
            StorageRpcErrorCode::InactivePgRoute,
            StorageRpcErrorCode::NonActingSetAccess,
            StorageRpcErrorCode::WrongClusterEpoch,
            StorageRpcErrorCode::Internal,
        ] {
            assert!(
                !PgMetadataTransferError::Store(remote_failure(code))
                    .requires_route_refresh_retry(),
                "{code:?} must not select route-refresh retry"
            );
        }

        for error in [
            StoreError::RouteMapExpired {
                cluster_epoch: ClusterEpoch::INITIAL,
                valid_until_ms: 1,
                now_ms: 2,
            },
            StoreError::StaleMetadataOperation {
                pg_id: 2,
                operation_epoch: ClusterEpoch::INITIAL,
                current_epoch: ClusterEpoch::new(2).unwrap(),
            },
            StoreError::StaleMetadataRoute {
                pg_id: 2,
                route_epoch: ClusterEpoch::INITIAL,
                current_epoch: ClusterEpoch::new(2).unwrap(),
            },
            StoreError::StaleShardLocation {
                node_id: 1,
                pg_id: 2,
                location_epoch: ClusterEpoch::INITIAL,
                current_epoch: ClusterEpoch::new(2).unwrap(),
            },
        ] {
            assert!(PgMetadataTransferError::Store(error).requires_route_refresh_retry());
        }

        let nested = StoreError::ShardStore {
            node_id: 1,
            pg_id: 2,
            cluster_epoch: ClusterEpoch::INITIAL,
            source: Box::new(StoreError::RouteMapExpired {
                cluster_epoch: ClusterEpoch::INITIAL,
                valid_until_ms: 1,
                now_ms: 2,
            }),
        };
        assert!(PgMetadataTransferError::Store(nested).requires_route_refresh_retry());

        let applied = PgMetadataTransferError::Apply(BucketSnapshotLoadError::Store(
            StoreError::RouteMapExpired {
                cluster_epoch: ClusterEpoch::INITIAL,
                valid_until_ms: 1,
                now_ms: 2,
            },
        ));
        assert!(applied.requires_route_refresh_retry());

        let stale_reconstruction = PgMetadataTransferError::reconstruction(
            crate::peering::PgPeeringReconstructionError::StaleReplicaEpoch {
                node_id: crate::NodeId::new(1),
                replica_epoch: ClusterEpoch::INITIAL,
                cluster_epoch: ClusterEpoch::new(2).unwrap(),
            },
        );
        assert!(matches!(
            stale_reconstruction,
            PgMetadataTransferError::RouteRefreshRequired { .. }
        ));
        assert!(stale_reconstruction.requires_route_refresh_retry());

        let pending_reconstruction = PgMetadataTransferError::reconstruction(
            crate::peering::PgPeeringReconstructionError::PendingMetadataCommand {
                node_id: crate::NodeId::new(1),
            },
        );
        assert!(matches!(
            pending_reconstruction,
            PgMetadataTransferError::PendingMetadataCommand { .. }
        ));
        assert!(!pending_reconstruction.requires_route_refresh_retry());
        assert!(pending_reconstruction.is_transient_import_blocker());

        let permanent_reconstruction = PgMetadataTransferError::reconstruction(
            crate::peering::PgPeeringReconstructionError::PrimaryMissing {
                primary: crate::NodeId::new(1),
            },
        );
        assert!(matches!(
            permanent_reconstruction,
            PgMetadataTransferError::Reconstruction { .. }
        ));
        assert!(!permanent_reconstruction.requires_route_refresh_retry());
        assert!(!permanent_reconstruction.is_transient_import_blocker());
    }

    #[test]
    fn storage_node_failure_formatting_and_diagnostics_are_redacted() {
        const SECRET: &str = "remote storage-node secret diagnostic";
        let failure = StoreError::StorageRpc {
            node_id: 7,
            operation: "test operation",
            failure: StorageRpcErrorCode::Internal,
            detail: StorageNodeFailureDetail::new(SECRET),
        };

        let display = failure.to_string();
        let debug = format!("{failure:?}");
        assert!(!display.contains(SECRET));
        assert!(!debug.contains(SECRET));
        assert!(!debug.contains("Internal"));
        assert_eq!(failure.diagnostic_kind(), "storage_node_failure");
        assert_eq!(
            remote_failure(StorageRpcErrorCode::TransportClosed).diagnostic_kind(),
            "storage_node_failure"
        );

        let exhausted = StoreError::StorageRpcResourceExhausted {
            node_id: 7,
            operation: "test operation",
            detail: StorageNodeFailureDetail::new(SECRET),
        };
        assert!(!exhausted.to_string().contains(SECRET));
        assert!(!format!("{exhausted:?}").contains(SECRET));
    }
}
