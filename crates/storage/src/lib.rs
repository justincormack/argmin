#![allow(
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::doc_markdown,
    clippy::items_after_statements,
    clippy::manual_let_else,
    clippy::match_same_arms,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::needless_pass_by_value,
    clippy::redundant_closure_for_method_calls,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::uninlined_format_args,
    clippy::unreadable_literal,
    clippy::wildcard_imports
)]

/// Storage layer for argmin2.
///
/// Request-serving code enters storage through [`StorageCluster`]. Storage
/// processes are assembled through the bootstrap and server types in
/// [`storage_node_server`]. Raw node, PG-store, and shard-store
/// implementations are private engine details.
pub mod clock;
pub mod cluster;
pub mod control_plane;
pub mod control_plane_auth;
pub mod control_plane_command;
pub(crate) mod control_plane_lease;
pub mod control_plane_raft;
pub(crate) mod data_dir;
pub(crate) mod durable_journal;
pub mod error;
pub(crate) mod metadata_command;
mod node_runtime;
pub(crate) mod peering;
pub mod pg_topology;
pub mod shard_key_hash;
#[allow(dead_code)]
pub(crate) mod storage_rpc;
pub(crate) mod storage_rpc_auth;
pub mod storage_rpc_transport;
pub mod types;

// Narrow facades preserve the crate's established module paths while the
// concrete engine, adapters, and server share a private compiler boundary.
mod node {
    pub use crate::node_runtime::node_facade::*;
}

pub(crate) mod node_client {
    pub use crate::node_runtime::client_facade::*;
}

pub mod storage_node_server {
    pub use crate::node_runtime::server_facade::*;
}

mod pg_store {
    pub use crate::node_runtime::pg_store_facade::*;
}

mod traits {
    pub(crate) use crate::node_runtime::traits_facade::*;
}

pub use cluster::{
    ActiveBucketMetadataScan, ActiveBucketRoute, ActiveBucketRoutePair, ActiveMultipartObjectRoute,
    ActiveObjectMetadataMutationRoute, ActiveObjectMetadataScan, ActiveObjectReadRoute,
    BucketWriteSnapshotAction, DurableReclaimScanBatch, DurableReclaimScanOutcome,
    LeasedObjectReadSnapshot, LeasedObjectReadSnapshotOutcome, LocalClusterMap,
    LocalNodeStoreConfig, LocalPgRoute, LocalUnixMetadataCommandNodeClientConfig,
    LocalUnixShardNodeClientConfig, LocalUnixStorageNodeClientAdmissionSettings,
    LocalUnixStorageNodeClientConfig, ObjectPayloadLease, PgMetadataTransferArtifact,
    PlacedSegmentShardBackfillCandidateEnqueueSummary,
    PlacedSegmentShardBackfillCandidateScanCursor, PlacedSegmentShardBackfillCopyTarget,
    PlacedSegmentShardBackfillPlan, PlacedSegmentShardHealth, PlacedSegmentShardSetHealth,
    PlacedSegmentShardSetRisk, PlacedSegmentShardValidation, ProcessLocalRegistryKey,
    ReleasedObjectPayloadLease, RetainedObjectPayloadRead, RetainedStreamUploadCleanup,
    ShardLocation, StorageCluster, StorageClusterRouteAdmission, StorageClusterRuntimeMapHandle,
    StorageClusterRuntimeMapRefreshLoop, StorageClusterRuntimeMapRefreshLoopFailure,
    StorageClusterRuntimeMapRefreshLoopStatus, StorageClusterRuntimeMapRefreshLoopStatusHandle,
    StorageClusterRuntimeMapRefreshLoopSuccess,
};
#[cfg(feature = "test-hooks")]
pub use cluster::{
    MetadataCommandApplyContextTestHook, MetadataCommandApplyContextTestHookGuard,
    MetadataCommandApplyTestContext, MetadataCommandApplyTestKind,
};
pub use error::{
    BucketSnapshotLoadError, BucketWriteDrainError, ClusterBuildError, MetadataError,
    ObjectPgActionError, PgMetadataTransferError, ShardIoError, StorageNodeFailureClass,
    StorageNodeFailureDetail, StoreError,
};
pub use metadata_command::BucketWriteReservationProof;
#[cfg(test)]
pub(crate) use node::LocalStorageNode;
#[cfg(feature = "test-hooks")]
pub use node::{
    install_bucket_scoped_test_hooks, BucketScopedTestHookGuard, BucketScopedTestHooks,
};
pub use node::{
    BucketCreateAttemptOutcome, BucketDeleteBeginRoot, BucketDeleteFinalizeOutcome, ReclaimWorkItem,
};
pub(crate) use node_runtime::role_facade::ObjectMetadataScanPgId;
pub use node_runtime::role_facade::{BucketPgId, DataPgId, ObjectMetadataPgId};
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) use pg_store::PgStore;
pub use pg_store::{
    MetadataCheckpointRow, MetadataCheckpointTableBlock, MetadataCheckpointTableDigest,
    MetadataCheckpointValue, MetadataCommandCheckpoint, MetadataCommandCheckpointValidationError,
    MetadataCommandLogCompactionStatus, MetadataCommandLogStats,
    PgClusterMapHistoryReferenceSummary, PgClusterMapHistoryRouteReference,
    PgClusterMapHistoryRouteReferenceKind, PgClusterMapHistoryRouteReferences,
    MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES, MAX_PG_DURABLE_IDENTITY_BYTES,
};
pub use pg_topology::PgTopology;
pub use placement::NodeId;
pub use shard_key_hash::{
    direct_put_segment_key_hash, multipart_part_segment_key_hash, object_key_hash,
    segment_key_hash, stream_segment_key_hash,
};
pub use storage_rpc::StorageNodeFailure;
pub(crate) use storage_rpc_auth::StorageRpcClientAuthConfig;
pub use storage_rpc_auth::{
    AdminStorageRpcClientCapability, FrontendStorageRpcClientCapability,
    MaintenanceStorageRpcClientCapability, StorageNodeStorageRpcClientCapability,
    StorageRpcServerAuthConfig, StorageRpcTransportLimits, STORAGE_RPC_AUTH_MAX_ENVELOPE_LEN,
};
#[cfg(test)]
pub(crate) use traits::PgMetadataStore;
pub use types::{
    key_prefix_upper_bound, object_key_common_prefix, object_key_prefix_upper_bound,
    AbortMultipartUploadCleanup, AclGrants, AuthorizedMultipartUploadRecord,
    BeginUploadPartStreamSessionReq, BucketAclSummary, BucketDeleteAttemptOutcomeKind,
    BucketDeleteAttemptOutcomeRecord, BucketDeleteAttemptPhase, BucketDeleteDebugBucketRow,
    BucketDeleteDebugDrain, BucketDeleteDebugFinalizeClaim, BucketDeleteDebugObjectVersionKind,
    BucketDeleteDebugObjectVersionSample, BucketDeleteDebugObjectVersionSampleError,
    BucketDeleteDebugPayloadReclaimClaim, BucketDeleteDebugPayloadReclaimClaimError,
    BucketDeleteDebugPayloadReclaimRoot, BucketDeleteDebugPayloadReclaimRootError,
    BucketDeleteDebugPendingCommand, BucketDeleteDebugSnapshot, BucketDeleteFinalizeClaimRecord,
    BucketDeleteFinalizeRoot, BucketEncryptionConfig, BucketFastPathIdentity, BucketFastPathInfo,
    BucketFastPathPolicy, BucketFastPathTags, BucketInfo, BucketName, BucketNameError,
    BucketObjectLockConfig, BucketObjectOwnership, BucketOwnershipControls, BucketSnapshot,
    BucketSnapshotPair, BucketSnapshotRequest, BucketSnapshotTagsRequest, BucketState,
    BucketSubresourceAux, BucketSubresourceKind, BucketVersioningState, BucketWriteDrainRecord,
    BucketWriteDrainState, BucketWriteReservationRecord, CanonicalUserId, ChecksumAlgorithm,
    ChecksumBytes, ChecksumType, ClusterEpoch, CommitDirectPutObjectReq, CommitMultipartReq,
    CompleteMultipartCommitCleanup, CompleteMultipartCommitOutcome, CompleteMultipartCommitRequest,
    CompletedMultipartStalePayload, CreateBucketConfig, CreateMultipartUploadOutcome,
    CreateMultipartUploadReq, CreateStreamUploadReq, DataLayout, DeleteCurrentObjectOutcome,
    DeleteMarkerRecord, DeleteSpecificObjectVersionOutcome, DeletedCurrentObject,
    DeletedSpecificObjectVersion, DirectPutCommitSnapshot, DirectPutCommitStorageSnapshot,
    DirectPutWrittenSegment, EcShape, EffectiveBucketEncryptionConfig, EtagKind,
    ExpireCurrentObjectOutcome, FinalizeDirectPutObjectOutcome, FinalizeStreamPartCleanup,
    FinalizeStreamPartOutcome, FinalizeStreamPutOutcome, GenerationId,
    InsertCurrentDeleteMarkerOutcome, InvalidChecksumConfig, LegalHoldStatus,
    LifecycleSweepBuckets, LifecycleSweepClaimRecord, LifecycleSweepRoot, LifecycleSweepRootSource,
    ListMultipartUploadsPageStart, ListMultipartUploadsReq, ListMultipartUploadsResp,
    ListObjectVersionsReq, ListObjectVersionsResp, ListObjectsReq, ListObjectsResp, ListPartsReq,
    ListPartsResp, ListedBucketMultipartUploads, ListedBucketObjectVersions, ListedBucketObjects,
    ListedMultipartParts, LiveObjectRecord, LoadedBucketSubresource, ManagedEncryptionAlgorithm,
    MultipartChecksumConfig, MultipartCompletionFingerprint, MultipartCompletionPreflight,
    MultipartCompletionReplay, MultipartCompletionSnapshot, MultipartObjectIdentity,
    MultipartPartRecord, MultipartPartSegmentRecord, MultipartReclaimPartRecord,
    MultipartReclaimPartSegmentRecord, MultipartReclaimRecord, MultipartUploadIdKey,
    MultipartUploadListMarker, MultipartUploadManagementLookup, MultipartUploadRecord,
    ObjectEncryption, ObjectEncryptionDecodeError, ObjectEncryptionType, ObjectEtag, ObjectKey,
    ObjectKeyError, ObjectLayout, ObjectLockDefaultRetention, ObjectLockMode, ObjectLockState,
    ObjectPartRangeRecord, ObjectPartRecord, ObjectPayloadReclaimClaimRecord,
    ObjectPayloadReclaimKind, ObjectReadAuthSubject, ObjectReadAuthSubjectIdentity,
    ObjectReadSnapshot, ObjectReadSnapshotMode, ObjectReadSnapshotOutcome, ObjectRetention,
    ObjectSegmentRecord, ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord,
    ObjectState, OwnerIdentity, PayloadReclaimRoot, PgId, PgState,
    PlacedSegmentShardBackfillClaimAcquire, PlacedSegmentShardBackfillClaimAcquireParams,
    PlacedSegmentShardBackfillClaimRecord, PlacedSegmentShardBackfillRecord,
    PlacedSegmentShardBackfillWorkItem, PlacedSegmentShardRepairClaimAcquire,
    PlacedSegmentShardRepairClaimAcquireParams, PlacedSegmentShardRepairClaimRecord,
    PlacedSegmentShardRepairRecord, PlacedSegmentShardRepairWorkItem,
    PrepareStreamUploadSegmentAppendReq, PreparedStreamPartCommit, PreparedStreamPutCommit,
    PublicAccessBlockConfig, PutBucketSubresource, PutDeleteMarkerReq, PutLiveObjectReq,
    PutLiveObjectValidationError, PutObjectReq, RawChecksum, RetentionPeriod, RouteMapValidUntilMs,
    RouteMapValidity, SegmentStoredBytesRequest, SerializedMetadataBlob,
    SerializedSystemMetadataBlob, SerializedTagSet, SessionId, SessionIdError, ShardData,
    ShardIndex, ShardKey, ShardScavengerObservation, ShardScavengerObservationKey,
    ShardScavengerObservationReason, ShardScavengerObservationRecord, ShardStat, ShardStatus,
    SseCustomerObjectState, SseS3ObjectState, StorageClass, StoredBucketSubresource,
    StoredLegalHoldStatus, StoredObject, StreamPutCommitInput, StreamPutFinalizeSnapshot,
    StreamPutFinalizeStorageSnapshot, StreamUploadCommandRecord, StreamUploadKind,
    StreamUploadPartSnapshot, StreamUploadPartStorageSnapshot, StreamUploadRecord,
    StreamUploadRecordPage, StreamUploadSegmentRecord, StreamUploadState, StreamUploadTarget,
    TerminalStreamCleanupRecord, UploadId, UploadIdError, UploadState, VersionId, WriteAck,
    WrittenShardAck, BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN,
    MULTIPART_PART_SEGMENT_STAGING_VERSION_ID, MULTIPART_UPLOAD_ID_KEY_LEN,
    OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN, OBJECT_ENCRYPTION_SEGMENT_NONCE_PREFIX_LEN,
    OBJECT_ENCRYPTION_SEGMENT_NONCE_SCOPE_LEN, OBJECT_ENCRYPTION_SEGMENT_TAG_LEN,
    OBJECT_ENCRYPTION_WRAPPED_DEK_LEN, OBJECT_ENCRYPTION_WRAP_NONCE_LEN,
    PLACED_SEGMENT_SHARD_BACKFILL_CLAIM_ID_MAX_LEN,
    PLACED_SEGMENT_SHARD_BACKFILL_LAST_ERROR_MAX_LEN, PLACED_SEGMENT_SHARD_BACKFILL_LIST_LIMIT,
    PLACED_SEGMENT_SHARD_BACKFILL_OWNER_TOKEN_MAX_LEN,
    PLACED_SEGMENT_SHARD_REPAIR_CLAIM_ID_MAX_LEN, PLACED_SEGMENT_SHARD_REPAIR_LAST_ERROR_MAX_LEN,
    PLACED_SEGMENT_SHARD_REPAIR_LIST_LIMIT, PLACED_SEGMENT_SHARD_REPAIR_OWNER_TOKEN_MAX_LEN,
    SESSION_ID_LEN, SHARD_KEY_HEX_LEN, SHARD_KEY_HEX_PREFIX_LEN, SHARD_KEY_LEN,
    SSE_C_CHECKSUM_NONCE_LEN, SSE_C_SEGMENT_NONCE_PREFIX_LEN, SSE_C_SEGMENT_NONCE_SCOPE_LEN,
    SSE_C_VALIDATOR_HMAC_LEN, SSE_C_VALIDATOR_SALT_LEN, SSE_C_WRAPPED_DEK_LEN,
    SSE_C_WRAP_NONCE_LEN, SSE_C_WRAP_SALT_LEN, SSE_S3_CHECKSUM_NONCE_LEN,
    SSE_S3_SEGMENT_NONCE_PREFIX_LEN, SSE_S3_WRAPPED_DEK_LEN, SSE_S3_WRAP_NONCE_LEN,
    UPLOAD_ID_ALPHABET, UPLOAD_ID_LEN,
};

#[cfg(test)]
mod tests;
