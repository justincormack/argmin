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
mod cluster;
pub mod control_plane;
pub mod control_plane_auth;
mod control_plane_client_bootstrap;
pub mod control_plane_command;
pub(crate) mod control_plane_lease;
mod control_plane_operator_admin;
mod control_plane_pg_admin;
pub mod control_plane_raft;
mod control_plane_raft_durability;
mod control_plane_raft_host;
mod control_plane_raft_peer_bootstrap;
mod control_plane_server_auth;
mod control_plane_server_bootstrap;
mod control_plane_service_client;
pub(crate) mod data_dir;
pub mod deadline_io;
pub(crate) mod durable_journal;
pub mod error;
mod live_pg_transfer;
mod maintenance;
pub(crate) mod metadata_command;
mod node_runtime;
pub(crate) mod peering;
pub mod pg_topology;
mod shard_key_hash;
mod standalone;
mod static_topology;
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
    ActiveBucketMetadataScan, ActiveBucketRoute, ActiveMultipartObjectRoute,
    ActiveObjectMetadataMutationRoute, ActiveObjectMetadataScan, ActiveObjectReadRoute,
    ActivePutObjectRoute, BucketIdentityGenerations, BucketWriteSnapshotAction,
    LeasedObjectReadSnapshot, LeasedObjectReadSnapshotOutcome, LocalClusterMap,
    LocalNodeStoreConfig, LocalPgRoute, LocalUnixMetadataCommandNodeClientConfig,
    LocalUnixShardNodeClientConfig, LocalUnixStorageNodeClientAdmissionSettings,
    LocalUnixStorageNodeClientConfig, ObjectPayloadLease, PlacedSegmentShardHealth,
    PlacedSegmentShardSetHealth, PlacedSegmentShardSetRisk, PlacedSegmentShardValidation,
    PreparedStandaloneEmbeddedTopology, ProcessLocalRegistryKey, ReleasedObjectPayloadLease,
    RetainedObjectPayloadRead, RetainedStreamUploadCleanup, ShardLocation, StorageCluster,
    StorageClusterRouteAdmission, StorageClusterRouteHandle, StorageClusterRuntimeMapHandle,
    StorageClusterRuntimeMapRefreshError, StorageClusterRuntimeMapRefreshLoop,
    StorageClusterRuntimeMapRefreshLoopFailure, StorageClusterRuntimeMapRefreshLoopStatus,
    StorageClusterRuntimeMapRefreshLoopStatusHandle, StorageClusterRuntimeMapRefreshLoopSuccess,
};
pub use control_plane_client_bootstrap::{
    control_plane_clock_recovery_socket_path, ControlPlaneAdminClientBootstrap,
    ControlPlaneAdminClientBootstrapError, ControlPlaneAdminCredentialBinding,
};
pub use control_plane_operator_admin::{
    ControlPlaneAuthorityClockAdminClient, ControlPlaneAuthorityClockAdminStatus,
    ControlPlaneOperatorAdminError, ControlPlaneRaftAdminClient,
};
pub use control_plane_pg_admin::{
    set_offline_control_plane_pg_acting_set, ControlPlanePgAdminClient, ControlPlanePgAdminError,
    ControlPlanePgAdminInputError, ControlPlanePgMetadataTransferInstall,
    ControlPlanePgStatusClient,
};
pub use control_plane_raft_durability::{
    ControlPlaneRaftAuthorityDurability, ControlPlaneRaftCheckpointMonitor,
    ControlPlaneRaftOuterIdentityPublicationError, ControlPlaneRaftOuterIdentityPublisher,
};
#[cfg(feature = "test-hooks")]
pub use control_plane_raft_durability::{
    ControlPlaneRaftCheckpointBlockForTest, ControlPlaneRaftCheckpointMonitorForTest,
};
pub use control_plane_raft_host::{
    ControlPlaneRaftAuthorityHost, ControlPlaneRaftHeartbeatLeaseExpiry,
};
pub use control_plane_raft_peer_bootstrap::{
    ControlPlaneRaftAuthorityService, ControlPlaneRaftOuterIdentityStartup,
    ControlPlaneRaftPeerAuthCredentialInput, ControlPlaneRaftPeerBootstrap,
    ControlPlaneRaftPeerBootstrapError, ControlPlaneRaftPeerServerListenerInput,
    ControlPlaneRaftPeerTopologyBinding, PreparedControlPlaneRaftAuthority,
};
#[cfg(feature = "test-hooks")]
pub use control_plane_raft_peer_bootstrap::{
    ControlPlaneRaftPeerTestServer, ControlPlaneRaftPeerTestServerError,
};
pub use control_plane_server_auth::{ControlPlaneRpcServerAuth, ControlPlaneRpcServerAuthError};
#[cfg(feature = "test-hooks")]
pub use control_plane_server_bootstrap::{
    ControlPlaneRpcOrdinaryTestServer, ControlPlaneRpcOrdinaryTestServerError,
};
pub use control_plane_server_bootstrap::{
    ControlPlaneRpcServerBootstrap, ControlPlaneRpcServerBootstrapError,
    ControlPlaneRpcServerListenerInput, ControlPlaneRpcServerLoops,
};
pub use control_plane_service_client::{
    ControlPlaneFrontendClient, ControlPlaneServiceClientBootstrapError,
    ControlPlaneStorageNodeClient,
};
pub use error::{
    BucketSnapshotLoadError, BucketWriteDrainError, ClusterBuildError, MetadataError,
    ObjectPgActionError, ShardIoError, StoreError, StoreFailure, StoreOperationFailureClass,
};
pub(crate) use error::{StorageNodeFailureClass, StorageNodeFailureDetail};
pub use live_pg_transfer::{
    LivePgMetadataTransferAdmin, LivePgMetadataTransferControlPlaneClient,
    LivePgMetadataTransferError, LivePgMetadataTransferFailpoint, LivePgMetadataTransferSummary,
};
pub use maintenance::{
    StorageMaintenanceAdmission, StorageMaintenancePermit, StorageMaintenanceStartError,
    StorageReclaimSweeper, StorageShardBackfillSweeper, StorageShardRepairSweeper,
    StorageShardScavengerSweeper, StorageStreamSessionSweeper,
};

/// Feature-gated support for tests which must cross the storage crate boundary.
///
/// The completed boundary is limited to logical observations, opaque semantic
/// fixtures, deterministic fault guards, and test-runtime lifecycle controls.
/// Phase 5 is moving the remaining physical test seams here temporarily so
/// they are explicit and owner-scoped before replacing them with opaque
/// scenarios or owner-local tests. Do not treat membership in this module as
/// evidence that an item already satisfies the final boundary.
///
/// AWS-facing tests must not enable this module.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub mod test_support {
    use super::*;

    /// Construct an opaque representative of an operation-level storage failure.
    ///
    /// Cross-crate tests use this to verify their protocol translation without
    /// depending on a PG, route, command-log, or RPC error variant.
    #[must_use]
    pub fn store_error_for_operation_failure_class(
        class: StoreOperationFailureClass,
    ) -> StoreError {
        match class {
            StoreOperationFailureClass::ResourceExhausted => {
                StoreError::storage_node_resource_exhausted(1, "test operation")
            }
            StoreOperationFailureClass::MetadataCommandContention => {
                StoreError::MetadataCommandContention {
                    context: "opaque test contention",
                }
            }
            StoreOperationFailureClass::RetryableConvergence => StoreError::RouteMapExpired {
                cluster_epoch: ClusterEpoch::INITIAL,
                valid_until_ms: 1,
                now_ms: 2,
            },
            StoreOperationFailureClass::Other => StoreError::NotFound,
        }
    }

    #[cfg(feature = "test-hooks")]
    pub use super::cluster::{
        MetadataCommandApplyContextTestHook, MetadataCommandApplyContextTestHookGuard,
        MetadataCommandApplyTestContext, MetadataCommandApplyTestKind, TestDirectPutWrittenSegment,
    };
    #[cfg(feature = "test-hooks")]
    pub use super::maintenance::{
        install_reclaim_worker_test_hooks, StorageReclaimWorkerTestHookGuard,
        StorageReclaimWorkerTestHooks, StorageStreamSessionSweepTestSummary,
    };
    #[cfg(feature = "test-hooks")]
    pub use super::node::{
        install_bucket_scoped_test_hooks, BucketScopedTestHookGuard, BucketScopedTestHooks,
    };

    /// Logical input used to drive one shard-backfill worker attempt.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct StorageShardBackfillTestWorkItem {
        pub request: SegmentStoredBytesRequest,
        pub source_cluster_epoch: ClusterEpoch,
        pub desired_cluster_epoch: ClusterEpoch,
    }

    /// Removes every placed shard file for one logical segment of a captured
    /// committed object payload.
    ///
    /// Durable acknowledgements are intentionally retained so the subsequent
    /// read traverses the production missing-payload path. Storage owns the EC
    /// geometry, placement, and physical shard identities used by this fault.
    pub fn inject_object_payload_segment_loss(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
        segment_index: u32,
    ) -> Result<(), StoreError> {
        let segment = snapshot
            .segments()
            .iter()
            .find(|segment| segment.segment_index == segment_index)
            .ok_or_else(|| StoreError::Io {
                context: "select object payload segment for fault injection",
                source: std::io::Error::other(format!(
                    "captured payload has no segment {segment_index}"
                )),
            })?;
        for shard_index in 0..segment.ec_k + segment.ec_m {
            cluster.test_inject_object_payload_shard_loss(snapshot, segment_index, shard_index)?;
        }
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct TestBucketDeleteFinalizeRoot {
        bucket: BucketName,
        bucket_incarnation_generation: u64,
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl TestBucketDeleteFinalizeRoot {
        pub fn bucket(&self) -> &BucketName {
            &self.bucket
        }
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

    /// Test-only observation of a bucket-scoped durable reclaim root.
    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct TestPayloadReclaimRoot {
        pub bucket: BucketName,
        pub key: ObjectKey,
        pub generation_id: GenerationId,
    }

    /// Test-only logical observation of one durable multipart upload.
    ///
    /// The production record remains storage-private. This projection lets
    /// cross-crate behavioral tests inspect the S3-visible state they established
    /// without depending on the database/RPC record type.
    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Clone, PartialEq, Eq)]
    pub struct TestMultipartUploadRecord {
        pub upload_id: UploadId,
        pub bucket: BucketName,
        pub key: ObjectKey,
        pub initiated_at: u64,
        pub state: UploadState,
        pub tags: Option<s3_types::TagSet>,
        pub metadata_blob: SerializedMetadataBlob,
        pub system_metadata_blob: SerializedSystemMetadataBlob,
        pub initiator: OwnerIdentity,
        pub owner: OwnerIdentity,
        pub acl_grants: AclGrants,
        pub public_read: bool,
        pub object_generation_id: GenerationId,
        pub object_lock: ObjectLockState,
        pub checksum: Option<MultipartChecksumConfig>,
        pub encryption: ObjectEncryption,
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl std::fmt::Debug for TestMultipartUploadRecord {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("TestMultipartUploadRecord")
                .field("upload_id", &self.upload_id)
                .field("bucket", &self.bucket)
                .field("key", &self.key)
                .field("initiated_at", &self.initiated_at)
                .field("state", &self.state)
                .field("tag_count", &self.tags.as_ref().map(s3_types::TagSet::len))
                .field("object_generation_id", &self.object_generation_id)
                .finish_non_exhaustive()
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl From<types::MultipartUploadRecord> for TestMultipartUploadRecord {
        fn from(upload: types::MultipartUploadRecord) -> Self {
            Self {
                upload_id: upload.upload_id,
                bucket: upload.bucket,
                key: upload.key,
                initiated_at: upload.initiated_at,
                state: upload.state,
                tags: upload.tags.map(|tags| tags.tag_set().clone()),
                metadata_blob: upload.metadata_blob,
                system_metadata_blob: upload.system_metadata_blob,
                initiator: upload.initiator,
                owner: upload.owner,
                acl_grants: upload.acl_grants,
                public_read: upload.public_read,
                object_generation_id: upload.object_generation_id,
                object_lock: upload.object_lock,
                checksum: upload.checksum,
                encryption: upload.encryption,
            }
        }
    }

    /// Logical test-only observation of one in-progress multipart part.
    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct TestMultipartPartObservation {
        pub generation: u32,
        pub size: u64,
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl From<types::MultipartPartRecord> for TestMultipartPartObservation {
        fn from(part: types::MultipartPartRecord) -> Self {
            Self {
                generation: part.generation,
                size: part.size,
            }
        }
    }

    /// Opaque evidence for the exact payload generations selected by a test.
    ///
    /// Physical placement and shard identities remain storage-owned. Callers
    /// can compare logical layout facts and ask storage to verify whether the
    /// captured payload is wholly present or absent.
    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Clone)]
    pub struct TestMultipartPartPayloadSnapshot {
        segments: Vec<types::MultipartPartSegmentRecord>,
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl std::fmt::Debug for TestMultipartPartPayloadSnapshot {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("TestMultipartPartPayloadSnapshot")
                .field("segment_count", &self.segments.len())
                .finish()
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl TestMultipartPartPayloadSnapshot {
        pub(crate) fn new(segments: Vec<types::MultipartPartSegmentRecord>) -> Self {
            Self { segments }
        }

        pub(crate) fn segments(&self) -> &[types::MultipartPartSegmentRecord] {
            &self.segments
        }

        pub fn is_empty(&self) -> bool {
            self.segments.is_empty()
        }

        pub fn segment_count(&self) -> usize {
            self.segments.len()
        }

        pub fn part_segment_count(&self, part_number: u32) -> usize {
            self.segments
                .iter()
                .filter(|segment| segment.part_number == part_number)
                .count()
        }

        pub fn single_segment_size(&self, part_number: u32) -> Option<u64> {
            let mut segments = self
                .segments
                .iter()
                .filter(|segment| segment.part_number == part_number);
            let size = segments.next()?.size;
            segments.next().is_none().then_some(size)
        }

        pub fn part_payload_identity_differs_from(&self, other: &Self, part_number: u32) -> bool {
            let identities = |snapshot: &Self| {
                snapshot
                    .segments
                    .iter()
                    .filter(|segment| segment.part_number == part_number)
                    .map(|segment| (segment.segment_okh, segment.segment_vid))
                    .collect::<Vec<_>>()
            };
            identities(self) != identities(other)
        }

        pub fn parts_have_distinct_stored_checksums(
            &self,
            first_part_number: u32,
            second_part_number: u32,
        ) -> bool {
            let checksums = |part_number| {
                self.segments
                    .iter()
                    .filter(|segment| segment.part_number == part_number)
                    .map(|segment| segment.segment_crc64)
                    .collect::<Vec<_>>()
            };
            let first = checksums(first_part_number);
            let second = checksums(second_part_number);
            !first.is_empty() && !second.is_empty() && first != second
        }
    }

    /// Logical layout of one staged stream-upload segment.
    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TestStreamUploadSegmentObservation {
        pub segment_index: u32,
        pub size: u64,
    }

    /// Opaque evidence for the exact staged payload of one stream upload.
    ///
    /// Physical placement and shard identities remain storage-owned. Callers
    /// may observe whether any segment was staged and ask storage to verify
    /// that every captured shard row and file has been removed.
    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Clone)]
    pub struct TestStreamUploadPayloadSnapshot {
        segments: Vec<types::StreamUploadSegmentRecord>,
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl std::fmt::Debug for TestStreamUploadPayloadSnapshot {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("TestStreamUploadPayloadSnapshot")
                .field("segment_count", &self.segments.len())
                .finish()
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl TestStreamUploadPayloadSnapshot {
        pub(crate) fn new(segments: Vec<types::StreamUploadSegmentRecord>) -> Self {
            Self { segments }
        }

        pub(crate) fn segments(&self) -> &[types::StreamUploadSegmentRecord] {
            &self.segments
        }

        pub fn is_empty(&self) -> bool {
            self.segments.is_empty()
        }

        pub fn segment_count(&self) -> usize {
            self.segments.len()
        }

        pub fn has_same_staged_payload_as(&self, other: &Self) -> bool {
            self.segments == other.segments
        }

        pub fn layout(&self) -> Vec<TestStreamUploadSegmentObservation> {
            self.segments
                .iter()
                .map(|segment| TestStreamUploadSegmentObservation {
                    segment_index: segment.segment_index,
                    size: segment.size,
                })
                .collect()
        }
    }

    /// Logical layout of one committed object segment.
    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TestObjectSegmentObservation {
        pub segment_index: u32,
        pub size: u64,
        /// Whether the mandatory stored CRC64 value is nonzero.
        pub has_nonzero_stored_checksum: bool,
    }

    /// Opaque evidence for the exact committed object payload selected by a test.
    ///
    /// Segment hashes, generations, PG placement, EC geometry, and placement
    /// epochs remain owned by storage. Cross-crate tests may inspect the logical
    /// segment layout and pass this evidence back to storage-owned assertions or
    /// fault scenarios.
    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Clone)]
    pub struct TestObjectPayloadSnapshot {
        segments: Vec<types::ObjectSegmentRecord>,
        stored_size_extra: usize,
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl std::fmt::Debug for TestObjectPayloadSnapshot {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("TestObjectPayloadSnapshot")
                .field("segment_count", &self.segments.len())
                .finish()
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl TestObjectPayloadSnapshot {
        pub(crate) fn new(
            segments: Vec<types::ObjectSegmentRecord>,
            stored_size_extra: usize,
        ) -> Self {
            Self {
                segments,
                stored_size_extra,
            }
        }

        pub(crate) fn segments(&self) -> &[types::ObjectSegmentRecord] {
            &self.segments
        }

        pub(crate) fn stored_size_for(
            &self,
            segment: &types::ObjectSegmentRecord,
        ) -> Option<usize> {
            usize::try_from(segment.size)
                .ok()?
                .checked_add(self.stored_size_extra)
        }

        pub fn is_empty(&self) -> bool {
            self.segments.is_empty()
        }

        pub fn segment_count(&self) -> usize {
            self.segments.len()
        }

        pub fn layout(&self) -> Vec<TestObjectSegmentObservation> {
            self.segments
                .iter()
                .map(|segment| TestObjectSegmentObservation {
                    segment_index: segment.segment_index,
                    size: segment.size,
                    has_nonzero_stored_checksum: segment.segment_crc64 != 0,
                })
                .collect()
        }
    }

    /// Logical observation of one repair discovered for a captured payload.
    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct TestObjectPayloadRepairObservation {
        pub segment_index: u32,
        pub shard_index: u8,
        pub last_error: Option<String>,
    }

    /// Opaque evidence for the exact file state of one captured payload shard.
    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Clone, PartialEq, Eq)]
    pub struct TestObjectPayloadShardFileSnapshot {
        bytes: Option<Vec<u8>>,
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl TestObjectPayloadShardFileSnapshot {
        pub(crate) fn new(bytes: Option<Vec<u8>>) -> Self {
            Self { bytes }
        }

        pub fn is_missing(&self) -> bool {
            self.bytes.is_none()
        }

        pub fn has_same_state_as(&self, other: &Self) -> bool {
            self == other
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl std::fmt::Debug for TestObjectPayloadShardFileSnapshot {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("TestObjectPayloadShardFileSnapshot")
                .field("present", &self.bytes.is_some())
                .field("length", &self.bytes.as_ref().map(Vec::len))
                .finish()
        }
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
        PostReservationStreamCleanup,
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
                TestBucketDeleteAttemptPhase::PostReservationStreamCleanup => {
                    Self::PostReservationStreamCleanup
                }
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
                types::BucketDeleteAttemptPhase::FinalVisibilityProven => {
                    Self::FinalVisibilityProven
                }
                types::BucketDeleteAttemptPhase::MarkDeleting => Self::MarkDeleting,
                types::BucketDeleteAttemptPhase::PostReservationStreamCleanup => {
                    Self::PostReservationStreamCleanup
                }
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
}

pub use metadata_command::BucketWriteReservationProof;
#[cfg(test)]
pub(crate) use node::LocalStorageNode;
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
pub(crate) use shard_key_hash::{direct_put_segment_key_hash, stream_segment_key_hash};
pub use shard_key_hash::{multipart_part_segment_key_hash, object_key_hash, segment_key_hash};
pub use standalone::{
    StandaloneRouteIdentity, StandaloneRouteIdentityError, StandaloneRouteIdentityLock,
    StandaloneRouteIdentityPreparation,
};
pub use static_topology::{
    derive_static_initial_control_plane_topology, derive_static_initial_pg_placement,
    derive_uncertified_initial_control_plane_topology, StaticInitialControlPlaneTopology,
    StaticInitialPgPlacement, StaticStorageFailureDomain, StaticStorageNodeEndpoint,
    StaticStoragePlacementError, StaticStoragePlacementNode, StaticStorageTopologyError,
    UncertifiedInitialControlPlaneTopology,
};
pub use storage_rpc::StorageNodeFailure;
pub(crate) use storage_rpc_auth::StorageRpcClientAuthConfig;
pub use storage_rpc_auth::{
    AdminStorageRpcClientCapability, FrontendStorageRpcClientCapability,
    MaintenanceStorageRpcClientCapability, StorageNodeStorageRpcClientCapability,
    StorageRpcServerAuthConfig, StorageRpcTransportLimits, STORAGE_RPC_AUTH_MAX_ENVELOPE_LEN,
};
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) use test_support::{
    TestBucketDeleteAttemptOutcomeKind, TestBucketDeleteAttemptPhase, TestBucketDeleteFinalizeRoot,
    TestBucketDeleteProgress, TestMultipartPartObservation, TestMultipartPartPayloadSnapshot,
    TestMultipartUploadRecord, TestObjectPayloadRepairObservation,
    TestObjectPayloadShardFileSnapshot, TestObjectPayloadSnapshot, TestPayloadReclaimRoot,
    TestStreamUploadPayloadSnapshot,
};
#[cfg(test)]
pub(crate) use traits::PgMetadataStore;
#[cfg(test)]
pub(crate) use types::BucketSubresourceAux;
#[cfg(test)]
pub(crate) use types::DirectPutWrittenSegment;
pub(crate) use types::MultipartUploadIdKey;
#[cfg(test)]
pub(crate) use types::UPLOAD_ID_LEN;
pub use types::{
    key_prefix_upper_bound, object_key_common_prefix, object_key_prefix_upper_bound, AclGrants,
    AuthorizedMultipartCompletionReplay, AuthorizedMultipartCompletionSnapshot,
    AuthorizedMultipartUploadAbort, AuthorizedMultipartUploadCompletion,
    AuthorizedMultipartUploadListParts, AuthorizedMultipartUploadPart,
    BeginUploadPartStreamSessionReq, BucketAclSummary, BucketDeleteDiagnostic,
    BucketEncryptionConfig, BucketFastPathIdentity, BucketFastPathInfo, BucketFastPathPolicy,
    BucketFastPathTags, BucketInfo, BucketName, BucketNameError, BucketObjectLockConfig,
    BucketObjectOwnership, BucketOwnershipControls, BucketSnapshot, BucketSnapshotRequest,
    BucketSnapshotTagsRequest, BucketState, BucketVersioningState, BucketWriteDrainRecord,
    BucketWriteDrainState, BucketWriteReservationRecord, CanonicalUserId, ChecksumAlgorithm,
    ChecksumBytes, ChecksumType, ClusterEpoch, CompleteMultipartCommitInput,
    CompleteMultipartCommitOutcome, CompleteMultipartCommitRequest, CreateBucketConfig,
    CreateMultipartUploadInput, CreateMultipartUploadOutcome, CreateStreamUploadReq, DataLayout,
    DeleteCurrentObjectOutcome, DeleteMarkerRecord, DeleteSpecificObjectVersionOutcome,
    DeletedCurrentObject, DeletedSpecificObjectVersion, DirectPutCommitSnapshot,
    DirectPutCommitStorageSnapshot, DirectPutPayloadWrite, EcShape,
    EffectiveBucketEncryptionConfig, EtagKind, ExpireCurrentObjectOutcome,
    FinalizeDirectPutObjectOutcome, FinalizeStreamPartOutcome, FinalizeStreamPutOutcome,
    GenerationId, InsertCurrentDeleteMarkerOutcome, InvalidChecksumConfig, LegalHoldStatus,
    LifecycleSweepBuckets, LifecycleSweepClaimRecord, LifecycleSweepRoot, LifecycleSweepRootSource,
    ListObjectVersionsReq, ListObjectVersionsResp, ListObjectsReq, ListObjectsResp,
    ListedBucketMultipartUploads, ListedBucketObjectVersions, ListedBucketObjects,
    ListedMultipartPart, ListedMultipartParts, ListedMultipartUpload, LiveObjectRecord,
    LoadedBucketSubresource, ManagedEncryptionAlgorithm, MultipartChecksumConfig,
    MultipartCompletionFingerprint, MultipartCompletionPart, MultipartCompletionReplayCandidate,
    MultipartLifecycleUpload, MultipartUploadAbortCandidate, MultipartUploadAbortLookup,
    MultipartUploadAuthorizationIdentity, MultipartUploadCompletionCandidate,
    MultipartUploadCompletionContext, MultipartUploadCompletionLookup, MultipartUploadIdAuthority,
    MultipartUploadListMarker, MultipartUploadListPartsCandidate, MultipartUploadListPartsLookup,
    MultipartUploadPartCandidate, ObjectEncryption, ObjectEncryptionStateError, ObjectEtag,
    ObjectKey, ObjectKeyError, ObjectLayout, ObjectLockDefaultRetention, ObjectLockMode,
    ObjectLockState, ObjectPayloadPlacementDiagnosticError, ObjectPayloadSegment,
    ObjectReadAuthSubject, ObjectReadAuthSubjectIdentity, ObjectReadMultipartPart,
    ObjectReadSnapshot, ObjectReadSnapshotMode, ObjectReadSnapshotOutcome, ObjectRetention,
    ObjectSegmentRecord, ObjectState, OpaqueBucketSubresourceKind, OwnerIdentity, PgId, PgState,
    PrepareStreamUploadSegmentAppendReq, PreparedDirectPutObjectCommit, PreparedStreamPartCommit,
    PreparedStreamPutCommit, PublicAccessBlockConfig, PutBucketSubresource, PutDeleteMarkerReq,
    PutLiveObjectReq, PutLiveObjectValidationError, PutObjectReq, RawChecksum, RetentionPeriod,
    RouteMapValidUntilMs, RouteMapValidity, SegmentStoredBytesRequest, SerializedBucketTagSet,
    SerializedMetadataBlob, SerializedSystemMetadataBlob, SerializedTagSet, SessionId,
    SessionIdError, ShardData, ShardIndex, ShardKey, ShardScavengerObservation,
    ShardScavengerObservationKey, ShardScavengerObservationReason, ShardScavengerObservationRecord,
    ShardStat, ShardStatus, SseCustomerObjectState, SseS3ObjectState, StorageClass,
    StoredLegalHoldStatus, StoredObject, StreamPartFinalizeInput, StreamPartFinalizeSnapshot,
    StreamPutCommitInput, StreamPutFinalizeSnapshot, StreamPutFinalizeStorageSnapshot,
    StreamSegmentAppendInput, StreamSegmentAppendOutcome, StreamUploadCommandRecord,
    StreamUploadKind, StreamUploadRecord, StreamUploadRecordPage, StreamUploadSegmentRecord,
    StreamUploadState, StreamUploadTarget, TerminalStreamCleanupRecord, UploadId, UploadIdError,
    UploadState, VersionId, WriteAck, WrittenShardAck, MULTIPART_PART_SEGMENT_STAGING_VERSION_ID,
    OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN, OBJECT_ENCRYPTION_SEGMENT_NONCE_PREFIX_LEN,
    OBJECT_ENCRYPTION_SEGMENT_NONCE_SCOPE_LEN, OBJECT_ENCRYPTION_SEGMENT_TAG_LEN,
    OBJECT_ENCRYPTION_WRAPPED_DEK_LEN, OBJECT_ENCRYPTION_WRAP_NONCE_LEN, SESSION_ID_LEN,
    SHARD_KEY_HEX_LEN, SHARD_KEY_HEX_PREFIX_LEN, SHARD_KEY_LEN, SSE_C_CHECKSUM_NONCE_LEN,
    SSE_C_SEGMENT_NONCE_PREFIX_LEN, SSE_C_SEGMENT_NONCE_SCOPE_LEN, SSE_C_VALIDATOR_HMAC_LEN,
    SSE_C_VALIDATOR_SALT_LEN, SSE_C_WRAPPED_DEK_LEN, SSE_C_WRAP_NONCE_LEN, SSE_C_WRAP_SALT_LEN,
    SSE_S3_CHECKSUM_NONCE_LEN, SSE_S3_SEGMENT_NONCE_PREFIX_LEN, SSE_S3_WRAPPED_DEK_LEN,
    SSE_S3_WRAP_NONCE_LEN,
};
pub(crate) use types::{
    AbortMultipartUploadCleanup, AuthorizedMultipartUploadRecord, CommitDirectPutObjectReq,
    CompleteMultipartCommitCleanup, CreateMultipartUploadReq, FinalizeStreamPartCleanup,
    MultipartCompletionPreflight, MultipartCompletionSnapshot, MultipartObjectIdentity,
    MultipartUploadManagementLookup,
};
pub(crate) use types::{
    BucketDeleteAttemptOutcomeKind, BucketDeleteAttemptOutcomeRecord, BucketDeleteAttemptPhase,
    BucketDeleteFinalizeClaimRecord, BucketDeleteFinalizeRoot, BucketSubresourceKind,
    ObjectPayloadReclaimClaimRecord, ObjectPayloadReclaimKind, PayloadReclaimRoot,
    BUCKET_DELETE_ATTEMPT_OUTCOME_DETAIL_MAX_LEN,
};
pub(crate) use types::{
    ListMultipartUploadsPageStart, ListMultipartUploadsReq, ListMultipartUploadsResp,
};
pub(crate) use types::{MultipartPartRecord, MultipartPartSegmentRecord, MultipartUploadRecord};
#[cfg(test)]
pub(crate) use types::{
    ObjectSegmentsReclaimRecord, PlacedSegmentShardBackfillClaimAcquire,
    PlacedSegmentShardBackfillClaimAcquireParams, PlacedSegmentShardBackfillClaimRecord,
    PlacedSegmentShardBackfillRecord, PlacedSegmentShardBackfillWorkItem,
};
pub(crate) use types::{StreamUploadPartSnapshot, StreamUploadPartStorageSnapshot};

#[cfg(test)]
mod tests;
