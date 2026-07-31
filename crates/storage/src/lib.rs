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
pub mod deadline_io;
pub(crate) mod durable_journal;
pub mod error;
mod maintenance;
pub(crate) mod metadata_command;
mod node_runtime;
pub(crate) mod peering;
pub mod pg_topology;
pub mod shard_key_hash;
mod standalone;
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

#[cfg(test)]
pub(crate) use cluster::DurableReclaimScanOutcome;
pub use cluster::{
    ActiveBucketMetadataScan, ActiveBucketRoute, ActiveBucketRoutePair, ActiveMultipartObjectRoute,
    ActiveObjectMetadataMutationRoute, ActiveObjectMetadataScan, ActiveObjectReadRoute,
    ActivePutObjectRoute, BucketWriteSnapshotAction, LeasedObjectReadSnapshot,
    LeasedObjectReadSnapshotOutcome, LocalClusterMap, LocalNodeStoreConfig, LocalPgRoute,
    LocalUnixMetadataCommandNodeClientConfig, LocalUnixShardNodeClientConfig,
    LocalUnixStorageNodeClientAdmissionSettings, LocalUnixStorageNodeClientConfig,
    ObjectPayloadLease, PgMetadataTransferArtifact, PlacedSegmentShardHealth,
    PlacedSegmentShardSetHealth, PlacedSegmentShardSetRisk, PlacedSegmentShardValidation,
    PreparedStandaloneEmbeddedTopology, ProcessLocalRegistryKey, ReleasedObjectPayloadLease,
    RetainedObjectPayloadRead, RetainedStreamUploadCleanup, ShardLocation, StorageCluster,
    StorageClusterRouteAdmission, StorageClusterRouteHandle, StorageClusterRuntimeMapHandle,
    StorageClusterRuntimeMapRefreshError, StorageClusterRuntimeMapRefreshLoop,
    StorageClusterRuntimeMapRefreshLoopFailure, StorageClusterRuntimeMapRefreshLoopStatus,
    StorageClusterRuntimeMapRefreshLoopStatusHandle, StorageClusterRuntimeMapRefreshLoopSuccess,
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
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub use maintenance::StorageStreamSessionSweepTestSummary;
#[cfg(feature = "test-hooks")]
#[doc(hidden)]
pub use maintenance::{
    install_reclaim_worker_test_hooks, StorageReclaimWorkerTestHookGuard,
    StorageReclaimWorkerTestHooks,
};
pub use maintenance::{
    StorageMaintenanceAdmission, StorageMaintenancePermit, StorageMaintenanceStartError,
    StorageReclaimSweeper, StorageShardBackfillSweeper, StorageShardRepairSweeper,
    StorageShardScavengerSweeper, StorageStreamSessionSweeper,
};

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestBucketDeleteFinalizeRoot {
    pub bucket: BucketName,
    pub bucket_incarnation_generation: u64,
}

#[cfg(any(test, feature = "test-hooks"))]
impl From<BucketDeleteFinalizeRoot> for TestBucketDeleteFinalizeRoot {
    fn from(root: BucketDeleteFinalizeRoot) -> Self {
        Self {
            bucket: root.bucket,
            bucket_incarnation_generation: root.bucket_incarnation_generation,
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl From<&TestBucketDeleteFinalizeRoot> for BucketDeleteFinalizeRoot {
    fn from(root: &TestBucketDeleteFinalizeRoot) -> Self {
        Self {
            bucket: root.bucket.clone(),
            bucket_incarnation_generation: root.bucket_incarnation_generation,
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestBucketDeleteBeginRoot(BucketDeleteBeginRoot);

#[cfg(any(test, feature = "test-hooks"))]
impl TestBucketDeleteBeginRoot {
    pub fn bucket(&self) -> &BucketName {
        self.0.bucket()
    }
}

#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestReclaimWorkItem {
    ObjectPayload((BucketName, ObjectKey, GenerationId)),
    BucketDeleteBegin(TestBucketDeleteBeginRoot),
    BucketDelete(TestBucketDeleteFinalizeRoot),
}

#[cfg(any(test, feature = "test-hooks"))]
impl From<ReclaimWorkItem> for TestReclaimWorkItem {
    fn from(work: ReclaimWorkItem) -> Self {
        match work {
            ReclaimWorkItem::ObjectPayload(root) => Self::ObjectPayload(root),
            ReclaimWorkItem::BucketDeleteBegin(root) => {
                Self::BucketDeleteBegin(TestBucketDeleteBeginRoot(root))
            }
            ReclaimWorkItem::BucketDelete(root) => Self::BucketDelete(root.into()),
        }
    }
}

/// Test-only input for one segment in an impossible durable reclaim fixture.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestObjectSegmentsReclaimSegmentRecord {
    pub segment_index: u32,
    pub segment_okh: [u8; 16],
    pub segment_vid: GenerationId,
    pub data_pg_id: u32,
    pub ec: EcShape,
}

/// Test-only input for an impossible durable segmented-object reclaim fixture.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestObjectSegmentsReclaimRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
    pub created_at: u64,
    pub segments: Vec<TestObjectSegmentsReclaimSegmentRecord>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl From<&TestObjectSegmentsReclaimRecord> for types::ObjectSegmentsReclaimRecord {
    fn from(reclaim: &TestObjectSegmentsReclaimRecord) -> Self {
        Self {
            bucket: reclaim.bucket.clone(),
            key: reclaim.key.clone(),
            generation_id: reclaim.generation_id,
            created_at: reclaim.created_at,
            segments: reclaim
                .segments
                .iter()
                .map(|segment| types::ObjectSegmentsReclaimSegmentRecord {
                    segment_index: segment.segment_index,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    data_pg_id: segment.data_pg_id,
                    ec: segment.ec,
                })
                .collect(),
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl From<types::ObjectSegmentsReclaimRecord> for TestObjectSegmentsReclaimRecord {
    fn from(reclaim: types::ObjectSegmentsReclaimRecord) -> Self {
        Self {
            bucket: reclaim.bucket,
            key: reclaim.key,
            generation_id: reclaim.generation_id,
            created_at: reclaim.created_at,
            segments: reclaim
                .segments
                .into_iter()
                .map(|segment| TestObjectSegmentsReclaimSegmentRecord {
                    segment_index: segment.segment_index,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    data_pg_id: segment.data_pg_id,
                    ec: segment.ec,
                })
                .collect(),
        }
    }
}

/// Test-only input for one segment in an impossible durable multipart reclaim fixture.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestMultipartReclaimPartSegmentRecord {
    pub part_number: u32,
    pub segment_index: u32,
    pub segment_okh: [u8; 16],
    pub segment_vid: GenerationId,
    pub data_pg_id: u32,
    pub ec: EcShape,
}

/// Test-only input for one multipart part in an impossible durable reclaim fixture.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestMultipartReclaimPartRecord {
    pub part_number: u32,
    pub segments: Vec<TestMultipartReclaimPartSegmentRecord>,
}

/// Test-only input for an impossible durable multipart reclaim fixture.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestMultipartReclaimRecord {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
    pub created_at: u64,
    pub parts: Vec<TestMultipartReclaimPartRecord>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl From<&TestMultipartReclaimRecord> for types::MultipartReclaimRecord {
    fn from(reclaim: &TestMultipartReclaimRecord) -> Self {
        Self {
            bucket: reclaim.bucket.clone(),
            key: reclaim.key.clone(),
            generation_id: reclaim.generation_id,
            created_at: reclaim.created_at,
            parts: reclaim
                .parts
                .iter()
                .map(|part| types::MultipartReclaimPartRecord {
                    part_number: part.part_number,
                    segments: part
                        .segments
                        .iter()
                        .map(|segment| types::MultipartReclaimPartSegmentRecord {
                            part_number: segment.part_number,
                            segment_index: segment.segment_index,
                            segment_okh: segment.segment_okh,
                            segment_vid: segment.segment_vid,
                            data_pg_id: segment.data_pg_id,
                            ec: segment.ec,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

/// Test-only observation of a bucket-scoped durable reclaim root.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestPayloadReclaimRoot {
    pub bucket: BucketName,
    pub key: ObjectKey,
    pub generation_id: GenerationId,
}

/// Test-only logical observation of accepted bucket-deletion progress.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TestBucketDeleteProgress {
    pub bucket_state: Option<BucketState>,
    pub has_durable_write_drain: bool,
    pub has_pending_metadata_command: bool,
}

/// Test-only durable bucket-deletion outcome fixture.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestBucketDeleteAttemptOutcomeKind {
    Retryable,
    NotEmpty,
    StaleGeneration,
    MarkDeleting,
}

#[cfg(any(test, feature = "test-hooks"))]
impl From<TestBucketDeleteAttemptOutcomeKind> for types::BucketDeleteAttemptOutcomeKind {
    fn from(value: TestBucketDeleteAttemptOutcomeKind) -> Self {
        match value {
            TestBucketDeleteAttemptOutcomeKind::Retryable => Self::Retryable,
            TestBucketDeleteAttemptOutcomeKind::NotEmpty => Self::NotEmpty,
            TestBucketDeleteAttemptOutcomeKind::StaleGeneration => Self::StaleGeneration,
            TestBucketDeleteAttemptOutcomeKind::MarkDeleting => Self::MarkDeleting,
        }
    }
}

/// Test-only durable bucket-deletion phase fixture.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestBucketDeleteAttemptPhase {
    Initial,
    ReservationWait,
    PostReservationObjectDrain,
    StreamCleanup,
    FinalVisibilityCheck,
    FinalVisibilityProven,
    MarkDeleting,
}

#[cfg(any(test, feature = "test-hooks"))]
impl From<TestBucketDeleteAttemptPhase> for types::BucketDeleteAttemptPhase {
    fn from(value: TestBucketDeleteAttemptPhase) -> Self {
        match value {
            TestBucketDeleteAttemptPhase::Initial => Self::Initial,
            TestBucketDeleteAttemptPhase::ReservationWait => Self::ReservationWait,
            TestBucketDeleteAttemptPhase::PostReservationObjectDrain => {
                Self::PostReservationObjectDrain
            }
            TestBucketDeleteAttemptPhase::StreamCleanup => Self::StreamCleanup,
            TestBucketDeleteAttemptPhase::FinalVisibilityCheck => Self::FinalVisibilityCheck,
            TestBucketDeleteAttemptPhase::FinalVisibilityProven => Self::FinalVisibilityProven,
            TestBucketDeleteAttemptPhase::MarkDeleting => Self::MarkDeleting,
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl From<types::BucketDeleteAttemptPhase> for TestBucketDeleteAttemptPhase {
    fn from(value: types::BucketDeleteAttemptPhase) -> Self {
        match value {
            types::BucketDeleteAttemptPhase::Initial => Self::Initial,
            types::BucketDeleteAttemptPhase::ReservationWait => Self::ReservationWait,
            types::BucketDeleteAttemptPhase::PostReservationObjectDrain => {
                Self::PostReservationObjectDrain
            }
            types::BucketDeleteAttemptPhase::StreamCleanup => Self::StreamCleanup,
            types::BucketDeleteAttemptPhase::FinalVisibilityCheck => Self::FinalVisibilityCheck,
            types::BucketDeleteAttemptPhase::FinalVisibilityProven => Self::FinalVisibilityProven,
            types::BucketDeleteAttemptPhase::MarkDeleting => Self::MarkDeleting,
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl From<types::PayloadReclaimRoot> for TestPayloadReclaimRoot {
    fn from(root: types::PayloadReclaimRoot) -> Self {
        Self {
            bucket: root.bucket,
            key: root.key,
            generation_id: root.generation_id,
        }
    }
}
pub use metadata_command::BucketWriteReservationProof;
#[cfg(test)]
pub(crate) use node::LocalStorageNode;
#[cfg(feature = "test-hooks")]
pub use node::{
    install_bucket_scoped_test_hooks, BucketScopedTestHookGuard, BucketScopedTestHooks,
};
pub use node::{BucketCreateAttemptOutcome, BucketDeleteFinalizeOutcome};
pub(crate) use node::{BucketDeleteBeginRoot, ReclaimWorkItem};
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
pub use standalone::{
    StandaloneRouteIdentity, StandaloneRouteIdentityError, StandaloneRouteIdentityLock,
    StandaloneRouteIdentityPreparation,
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
#[cfg(test)]
pub(crate) use types::BucketSubresourceAux;
pub use types::{
    key_prefix_upper_bound, object_key_common_prefix, object_key_prefix_upper_bound,
    AbortMultipartUploadCleanup, AclGrants, AuthorizedMultipartUploadRecord,
    BeginUploadPartStreamSessionReq, BucketAclSummary, BucketDeleteDiagnostic,
    BucketEncryptionConfig, BucketFastPathIdentity, BucketFastPathInfo, BucketFastPathPolicy,
    BucketFastPathTags, BucketInfo, BucketName, BucketNameError, BucketObjectLockConfig,
    BucketObjectOwnership, BucketOwnershipControls, BucketSnapshot, BucketSnapshotPair,
    BucketSnapshotRequest, BucketSnapshotTagsRequest, BucketState, BucketVersioningState,
    BucketWriteDrainRecord, BucketWriteDrainState, BucketWriteReservationRecord, CanonicalUserId,
    ChecksumAlgorithm, ChecksumBytes, ChecksumType, ClusterEpoch, CommitDirectPutObjectReq,
    CommitMultipartReq, CompleteMultipartCommitCleanup, CompleteMultipartCommitOutcome,
    CompleteMultipartCommitRequest, CompletedMultipartStalePayload, CreateBucketConfig,
    CreateMultipartUploadOutcome, CreateMultipartUploadReq, CreateStreamUploadReq, DataLayout,
    DeleteCurrentObjectOutcome, DeleteMarkerRecord, DeleteSpecificObjectVersionOutcome,
    DeletedCurrentObject, DeletedSpecificObjectVersion, DirectPutCommitSnapshot,
    DirectPutCommitStorageSnapshot, DirectPutWrittenSegment, EcShape,
    EffectiveBucketEncryptionConfig, EtagKind, ExpireCurrentObjectOutcome,
    FinalizeDirectPutObjectOutcome, FinalizeStreamPartCleanup, FinalizeStreamPartOutcome,
    FinalizeStreamPutOutcome, GenerationId, InsertCurrentDeleteMarkerOutcome,
    InvalidChecksumConfig, LegalHoldStatus, LifecycleSweepBuckets, LifecycleSweepClaimRecord,
    LifecycleSweepRoot, LifecycleSweepRootSource, ListMultipartUploadsPageStart,
    ListMultipartUploadsReq, ListMultipartUploadsResp, ListObjectVersionsReq,
    ListObjectVersionsResp, ListObjectsReq, ListObjectsResp, ListPartsReq, ListPartsResp,
    ListedBucketMultipartUploads, ListedBucketObjectVersions, ListedBucketObjects,
    ListedMultipartParts, LiveObjectRecord, LoadedBucketSubresource, ManagedEncryptionAlgorithm,
    MultipartChecksumConfig, MultipartCompletionFingerprint, MultipartCompletionPreflight,
    MultipartCompletionReplay, MultipartCompletionSnapshot, MultipartObjectIdentity,
    MultipartPartRecord, MultipartPartSegmentRecord, MultipartUploadIdKey,
    MultipartUploadListMarker, MultipartUploadManagementLookup, MultipartUploadRecord,
    ObjectEncryption, ObjectEncryptionStateError, ObjectEtag, ObjectKey, ObjectKeyError,
    ObjectLayout, ObjectLockDefaultRetention, ObjectLockMode, ObjectLockState,
    ObjectPartRangeRecord, ObjectPartRecord, ObjectReadAuthSubject, ObjectReadAuthSubjectIdentity,
    ObjectReadSnapshot, ObjectReadSnapshotMode, ObjectReadSnapshotOutcome, ObjectRetention,
    ObjectSegmentRecord, ObjectState, OpaqueBucketSubresourceKind, OwnerIdentity, PgId, PgState,
    PrepareStreamUploadSegmentAppendReq, PreparedStreamPartCommit, PreparedStreamPutCommit,
    PublicAccessBlockConfig, PutBucketSubresource, PutDeleteMarkerReq, PutLiveObjectReq,
    PutLiveObjectValidationError, PutObjectReq, RawChecksum, RetentionPeriod, RouteMapValidUntilMs,
    RouteMapValidity, SegmentStoredBytesRequest, SerializedBucketTagSet, SerializedMetadataBlob,
    SerializedSystemMetadataBlob, SerializedTagSet, SessionId, SessionIdError, ShardData,
    ShardIndex, ShardKey, ShardScavengerObservation, ShardScavengerObservationKey,
    ShardScavengerObservationReason, ShardScavengerObservationRecord, ShardStat, ShardStatus,
    SseCustomerObjectState, SseS3ObjectState, StorageClass, StoredLegalHoldStatus, StoredObject,
    StreamPutCommitInput, StreamPutFinalizeSnapshot, StreamPutFinalizeStorageSnapshot,
    StreamUploadCommandRecord, StreamUploadKind, StreamUploadPartSnapshot,
    StreamUploadPartStorageSnapshot, StreamUploadRecord, StreamUploadRecordPage,
    StreamUploadSegmentRecord, StreamUploadState, StreamUploadTarget, TerminalStreamCleanupRecord,
    UploadId, UploadIdError, UploadState, VersionId, WriteAck, WrittenShardAck,
    MULTIPART_PART_SEGMENT_STAGING_VERSION_ID, MULTIPART_UPLOAD_ID_KEY_LEN,
    OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN, OBJECT_ENCRYPTION_SEGMENT_NONCE_PREFIX_LEN,
    OBJECT_ENCRYPTION_SEGMENT_NONCE_SCOPE_LEN, OBJECT_ENCRYPTION_SEGMENT_TAG_LEN,
    OBJECT_ENCRYPTION_WRAPPED_DEK_LEN, OBJECT_ENCRYPTION_WRAP_NONCE_LEN, SESSION_ID_LEN,
    SHARD_KEY_HEX_LEN, SHARD_KEY_HEX_PREFIX_LEN, SHARD_KEY_LEN, SSE_C_CHECKSUM_NONCE_LEN,
    SSE_C_SEGMENT_NONCE_PREFIX_LEN, SSE_C_SEGMENT_NONCE_SCOPE_LEN, SSE_C_VALIDATOR_HMAC_LEN,
    SSE_C_VALIDATOR_SALT_LEN, SSE_C_WRAPPED_DEK_LEN, SSE_C_WRAP_NONCE_LEN, SSE_C_WRAP_SALT_LEN,
    SSE_S3_CHECKSUM_NONCE_LEN, SSE_S3_SEGMENT_NONCE_PREFIX_LEN, SSE_S3_WRAPPED_DEK_LEN,
    SSE_S3_WRAP_NONCE_LEN, UPLOAD_ID_ALPHABET, UPLOAD_ID_LEN,
};
pub(crate) use types::{
    BucketDeleteAttemptOutcomeKind, BucketDeleteAttemptOutcomeRecord, BucketDeleteAttemptPhase,
    BucketDeleteFinalizeClaimRecord, BucketDeleteFinalizeRoot, BucketSubresourceKind,
    ObjectPayloadReclaimClaimRecord, ObjectPayloadReclaimKind, PayloadReclaimRoot,
    BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN,
};
#[cfg(test)]
pub(crate) use types::{
    ObjectSegmentsReclaimRecord, PlacedSegmentShardBackfillClaimAcquire,
    PlacedSegmentShardBackfillClaimAcquireParams, PlacedSegmentShardBackfillClaimRecord,
    PlacedSegmentShardBackfillRecord, PlacedSegmentShardBackfillWorkItem,
};

#[cfg(test)]
mod tests;
