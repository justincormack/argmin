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
mod error;
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
    LocalUnixStorageNodeClientConfig, ObjectPayloadLease, PreparedStandaloneEmbeddedTopology,
    ProcessLocalRegistryKey, ReleasedObjectPayloadLease, RetainedObjectPayloadRead,
    RetainedStreamUploadCleanup, StorageCluster, StorageClusterRouteAdmission,
    StorageClusterRouteHandle, StorageClusterRuntimeMapHandle,
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
    BucketListingFailure, BucketListingFailureKind, BucketSnapshotLoadFailure,
    BucketSnapshotLoadFailureKind, BucketWriteDrainFailure, BucketWriteDrainFailureKind,
    ClusterBuildError, DirectPutFailure, DirectPutFailureKind, LifecycleMaintenanceFailure,
    LifecycleMaintenanceFailureKind, LifecycleMutationFailure, LifecycleMutationFailureKind,
    MultipartCompletionFailure, MultipartCompletionFailureKind, MultipartManagementFailure,
    MultipartManagementFailureKind, ObjectMetadataListingFailure, ObjectMetadataListingFailureKind,
    ObjectMetadataMutationFailure, ObjectMetadataMutationFailureKind, ObjectReadFailure,
    ObjectReadFailureKind, StoreFailure, StoreOperationFailureClass, StreamUploadFailure,
    StreamUploadFailureKind,
};
pub(crate) use error::{
    BucketSnapshotLoadError, BucketWriteDrainError, MetadataError, ObjectPgActionError,
    StorageNodeFailureClass, StorageNodeFailureDetail, StoreError,
};
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
    use std::fmt;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use super::*;

    /// Opaque failure from a storage-owned test operation.
    ///
    /// Cross-crate tests receive only a bounded diagnostic category. The
    /// underlying PG, shard, route, database, and RPC error representation is
    /// consumed inside storage.
    pub struct TestStorageFailure {
        diagnostic_cause_label: &'static str,
    }

    impl TestStorageFailure {
        #[must_use]
        pub const fn diagnostic_cause_label(&self) -> &'static str {
            self.diagnostic_cause_label
        }

        fn from_store(error: StoreError) -> Self {
            Self {
                diagnostic_cause_label: error.diagnostic_cause_label(),
            }
        }

        fn from_object_pg_action(error: ObjectPgActionError) -> Self {
            Self {
                diagnostic_cause_label: error.diagnostic_cause_label(),
            }
        }
    }

    impl fmt::Debug for TestStorageFailure {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("TestStorageFailure")
                .field("cause_label", &self.diagnostic_cause_label)
                .finish()
        }
    }

    impl fmt::Display for TestStorageFailure {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                formatter,
                "storage test operation failed ({})",
                self.diagnostic_cause_label
            )
        }
    }

    impl std::error::Error for TestStorageFailure {}

    #[cfg(test)]
    mod test_storage_failure_tests {
        use super::*;

        #[test]
        fn formatting_retains_only_the_bounded_storage_category() {
            const SECRET_CONTEXT: &str = "secret test-support storage operation";
            const SECRET_SOURCE: &str = "secret test-support storage source";
            let failure = TestStorageFailure::from_store(StoreError::Io {
                context: SECRET_CONTEXT,
                source: std::io::Error::other(SECRET_SOURCE),
            });

            assert_eq!(failure.diagnostic_cause_label(), "store_io_failure");
            for rendered in [format!("{failure}"), format!("{failure:?}")] {
                assert!(rendered.contains("store_io_failure"));
                assert!(!rendered.contains(SECRET_CONTEXT));
                assert!(!rendered.contains(SECRET_SOURCE));
            }
            assert!(std::error::Error::source(&failure).is_none());
        }

        #[test]
        fn object_pg_validation_detail_is_redacted() {
            const SECRET_REASON: &str = "secret lifecycle test-support validation detail";
            let failure =
                TestStorageFailure::from_object_pg_action(ObjectPgActionError::InvalidRequest {
                    reason: SECRET_REASON.to_string(),
                });

            assert_eq!(failure.diagnostic_cause_label(), "invalid_request");
            for rendered in [format!("{failure}"), format!("{failure:?}")] {
                assert!(rendered.contains("invalid_request"));
                assert!(!rendered.contains(SECRET_REASON));
            }
            assert!(std::error::Error::source(&failure).is_none());
        }
    }

    /// Opaque failure selected by a higher-layer deterministic test hook.
    pub struct TestInjectedStorageFailure {
        error: StoreError,
    }

    impl TestInjectedStorageFailure {
        fn new(error: StoreError) -> Self {
            Self { error }
        }

        fn into_store_error(self) -> StoreError {
            self.error
        }
    }

    /// Inject a retryable convergence failure without exposing its route or PG
    /// representation.
    #[must_use]
    pub fn injected_retryable_convergence_failure() -> TestInjectedStorageFailure {
        TestInjectedStorageFailure::new(StoreError::RouteMapExpired {
            cluster_epoch: ClusterEpoch::INITIAL,
            valid_until_ms: 0,
            now_ms: 1,
        })
    }

    /// Inject an internal storage failure without exposing its implementation
    /// representation.
    #[must_use]
    pub fn injected_internal_storage_failure() -> TestInjectedStorageFailure {
        TestInjectedStorageFailure::new(StoreError::Io {
            context: "injected opaque storage test failure",
            source: std::io::Error::other("injected opaque storage test failure"),
        })
    }

    /// Open the storage-owned default cluster used by higher-layer unit tests.
    ///
    /// The physical topology is intentionally not configurable across the
    /// crate boundary. Tests which need a particular node, PG, shard, or EC
    /// arrangement belong in storage.
    pub fn open_default_test_storage_cluster(data_dir: &std::path::Path) -> Arc<StorageCluster> {
        let ec_config = ec::EcConfig::default();
        let ec_shape = EcShape {
            k: ec_config.data_shards(),
            m: ec_config.parity_shards(),
        };
        let node_count = u32::from(ec_shape.k) + u32::from(ec_shape.m);
        let node_ids = (0..node_count).map(NodeId::new).collect::<Vec<_>>();

        StorageCluster::open_static_local_nodes(data_dir, &node_ids, &[0], ec_shape)
            .expect("open default test storage cluster")
    }

    /// Opaque guard for a deterministic Direct PUT failure after durable
    /// metadata publication.
    pub struct TestDirectPutPostPublishErrorGuard {
        _inner: super::node::DirectPutMetadataPublishTestHookGuard,
    }

    /// Opaque guard for a deterministic action immediately before a payload
    /// shard write. Physical shard identity remains storage-owned.
    pub struct TestPayloadShardWriteActionGuard {
        _inner: super::cluster::PayloadShardWriteTestHookGuard,
    }

    pub type TestPayloadShardWriteAction = Arc<dyn Fn() + Send + Sync>;

    /// Opaque guard holding the metadata serialization boundary for one
    /// bucket. The routed PG and storage lock remain storage-owned.
    pub struct TestBucketMetadataHoldGuard<'a> {
        _inner: super::node::BucketPgTestGuard<'a>,
    }

    /// Curated opaque payload observations for cross-crate tests.
    ///
    /// The underlying storage operations remain crate-private. Importing this
    /// trait makes only storage-owned opaque snapshots and logical predicates
    /// available to downstream test code.
    pub trait StorageClusterPayloadTestSupport {
        /// Return the allocation count for the configured payload EC scratch
        /// pool without exposing its physical EC shape.
        fn test_default_payload_ec_scratch_allocation_count(&self) -> usize;

        fn test_install_before_payload_shard_write_action(
            &self,
            action: TestPayloadShardWriteAction,
        ) -> TestPayloadShardWriteActionGuard;

        fn test_install_direct_put_post_publish_error(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            reason: String,
        ) -> TestDirectPutPostPublishErrorGuard;

        fn test_capture_object_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
        ) -> Result<TestObjectPayloadSnapshot, TestStorageFailure>;

        fn test_object_payload_snapshot_is_fully_present(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure>;

        fn test_object_payload_snapshot_is_fully_absent(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure>;

        fn test_object_payload_snapshot_places_each_shard_on_a_distinct_node(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure>;

        fn test_object_payload_snapshot_uses_generation_layout(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure>;

        fn test_object_payload_snapshot_uses_transient_direct_put_layout(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure>;

        fn test_inject_object_payload_first_segment_unknown_data_pg(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<(), TestStorageFailure>;

        fn test_inject_object_payload_first_segment_checksum_mismatch(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<(), TestStorageFailure>;

        fn test_capture_multipart_upload_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<TestMultipartPartPayloadSnapshot, TestStorageFailure>;

        fn test_capture_multipart_part_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
            part_number: u32,
        ) -> Result<TestMultipartPartPayloadSnapshot, TestStorageFailure>;

        fn test_multipart_part_payload_snapshot_is_fully_present(
            &self,
            snapshot: &TestMultipartPartPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure>;

        fn test_multipart_part_payload_snapshot_is_fully_absent(
            &self,
            snapshot: &TestMultipartPartPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure>;

        fn test_capture_stream_upload_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            session_id: &SessionId,
        ) -> Result<TestStreamUploadPayloadSnapshot, TestStorageFailure>;

        fn test_stream_upload_payload_snapshot_is_fully_present(
            &self,
            snapshot: &TestStreamUploadPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure>;

        fn test_stream_upload_payload_snapshot_is_fully_absent(
            &self,
            snapshot: &TestStreamUploadPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure>;
    }

    impl StorageClusterPayloadTestSupport for StorageCluster {
        fn test_default_payload_ec_scratch_allocation_count(&self) -> usize {
            StorageCluster::test_default_payload_ec_scratch_allocation_count(self)
        }

        fn test_install_before_payload_shard_write_action(
            &self,
            action: TestPayloadShardWriteAction,
        ) -> TestPayloadShardWriteActionGuard {
            TestPayloadShardWriteActionGuard {
                _inner: StorageCluster::test_install_before_placed_payload_shard_write_hook(
                    self,
                    Arc::new(move |_, _| {
                        action();
                        Ok(())
                    }),
                ),
            }
        }

        fn test_install_direct_put_post_publish_error(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            reason: String,
        ) -> TestDirectPutPostPublishErrorGuard {
            let target_bucket = bucket.clone();
            let target_key = key.clone();
            let inner = StorageCluster::test_install_after_direct_put_metadata_publish_hook(
                self,
                Arc::new(move |actual_bucket, actual_key| {
                    if actual_bucket == &target_bucket && actual_key == &target_key {
                        Err(ObjectPgActionError::InvalidRequest {
                            reason: reason.clone(),
                        })
                    } else {
                        Ok(())
                    }
                }),
            );
            TestDirectPutPostPublishErrorGuard { _inner: inner }
        }

        fn test_capture_object_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
        ) -> Result<TestObjectPayloadSnapshot, TestStorageFailure> {
            StorageCluster::test_capture_object_payload(self, bucket, key, version_id)
                .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_object_payload_snapshot_is_fully_present(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure> {
            StorageCluster::test_object_payload_snapshot_is_fully_present(self, snapshot)
                .map_err(TestStorageFailure::from_store)
        }

        fn test_object_payload_snapshot_is_fully_absent(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure> {
            StorageCluster::test_object_payload_snapshot_is_fully_absent(self, snapshot)
                .map_err(TestStorageFailure::from_store)
        }

        fn test_object_payload_snapshot_places_each_shard_on_a_distinct_node(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure> {
            StorageCluster::test_object_payload_snapshot_places_each_shard_on_a_distinct_node(
                self, snapshot,
            )
            .map_err(TestStorageFailure::from_store)
        }

        fn test_object_payload_snapshot_uses_generation_layout(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure> {
            StorageCluster::test_object_payload_snapshot_uses_generation_layout(self, snapshot)
                .map_err(TestStorageFailure::from_store)
        }

        fn test_object_payload_snapshot_uses_transient_direct_put_layout(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure> {
            StorageCluster::test_object_payload_snapshot_uses_transient_direct_put_layout(
                self, snapshot,
            )
            .map_err(TestStorageFailure::from_store)
        }

        fn test_inject_object_payload_first_segment_unknown_data_pg(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<(), TestStorageFailure> {
            StorageCluster::test_inject_object_payload_first_segment_unknown_data_pg(self, snapshot)
                .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_inject_object_payload_first_segment_checksum_mismatch(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<(), TestStorageFailure> {
            StorageCluster::test_inject_object_payload_first_segment_checksum_mismatch(
                self, snapshot,
            )
            .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_capture_multipart_upload_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<TestMultipartPartPayloadSnapshot, TestStorageFailure> {
            StorageCluster::test_capture_multipart_upload_payload(self, bucket, key, upload_id)
                .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_capture_multipart_part_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
            part_number: u32,
        ) -> Result<TestMultipartPartPayloadSnapshot, TestStorageFailure> {
            StorageCluster::test_capture_multipart_part_payload(
                self,
                bucket,
                key,
                upload_id,
                part_number,
            )
            .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_multipart_part_payload_snapshot_is_fully_present(
            &self,
            snapshot: &TestMultipartPartPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure> {
            StorageCluster::test_multipart_part_payload_snapshot_is_fully_present(self, snapshot)
                .map_err(TestStorageFailure::from_store)
        }

        fn test_multipart_part_payload_snapshot_is_fully_absent(
            &self,
            snapshot: &TestMultipartPartPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure> {
            StorageCluster::test_multipart_part_payload_snapshot_is_fully_absent(self, snapshot)
                .map_err(TestStorageFailure::from_store)
        }

        fn test_capture_stream_upload_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            session_id: &SessionId,
        ) -> Result<TestStreamUploadPayloadSnapshot, TestStorageFailure> {
            StorageCluster::test_capture_stream_upload_payload(self, bucket, key, session_id)
                .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_stream_upload_payload_snapshot_is_fully_present(
            &self,
            snapshot: &TestStreamUploadPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure> {
            StorageCluster::test_stream_upload_payload_snapshot_is_fully_present(self, snapshot)
                .map_err(TestStorageFailure::from_store)
        }

        fn test_stream_upload_payload_snapshot_is_fully_absent(
            &self,
            snapshot: &TestStreamUploadPayloadSnapshot,
        ) -> Result<bool, TestStorageFailure> {
            StorageCluster::test_stream_upload_payload_snapshot_is_fully_absent(self, snapshot)
                .map_err(TestStorageFailure::from_store)
        }
    }

    /// Curated runtime-map validity controls for cross-crate tests.
    ///
    /// These operations let request-layer tests deterministically exercise
    /// captured-deadline and same-epoch renewal behavior without exposing the
    /// cluster's ordinary test-only implementation methods.
    pub trait StorageClusterRouteMapTestSupport {
        fn test_clone_with_dynamic_route_map_validity(
            &self,
            validity: RouteMapValidity,
        ) -> Result<Arc<StorageCluster>, ClusterBuildError>;

        fn test_store_route_map_validity(&self, validity: RouteMapValidity);

        fn test_store_route_map_lease(
            &self,
            validity: RouteMapValidity,
            local_valid_until_monotonic_ms: Option<u64>,
        );
    }

    impl StorageClusterRouteMapTestSupport for StorageCluster {
        fn test_clone_with_dynamic_route_map_validity(
            &self,
            validity: RouteMapValidity,
        ) -> Result<Arc<StorageCluster>, ClusterBuildError> {
            StorageCluster::test_clone_with_dynamic_route_map_validity(self, validity)
        }

        fn test_store_route_map_validity(&self, validity: RouteMapValidity) {
            StorageCluster::test_store_route_map_validity(self, validity);
        }

        fn test_store_route_map_lease(
            &self,
            validity: RouteMapValidity,
            local_valid_until_monotonic_ms: Option<u64>,
        ) {
            StorageCluster::test_store_route_map_lease(
                self,
                validity,
                local_valid_until_monotonic_ms,
            );
        }
    }

    /// Deterministic request/publication barrier controls for cross-crate
    /// runtime-map tests.
    pub trait StorageClusterRouteHandleTestSupport {
        fn test_wait_until_route_request_is_admitted(&self);
        fn test_wait_until_route_publication_is_pending(&self);
    }

    impl StorageClusterRouteHandleTestSupport for StorageClusterRouteHandle {
        fn test_wait_until_route_request_is_admitted(&self) {
            StorageClusterRouteHandle::test_wait_until_route_request_is_admitted(self);
        }

        fn test_wait_until_route_publication_is_pending(&self) {
            StorageClusterRouteHandle::test_wait_until_route_publication_is_pending(self);
        }
    }

    /// Opaque evidence for one object's durable metadata-command state.
    ///
    /// Cross-crate tests can compare command progress and materialized-state
    /// changes without learning the object's PG, primary node, or raw replica
    /// proof. Comparisons fail closed across storage domains or subjects.
    #[derive(Clone)]
    pub struct TestObjectMetadataCommandState {
        storage_domain: ProcessLocalRegistryKey,
        bucket: BucketName,
        key: ObjectKey,
        proof: control_plane::PgMetadataProof,
    }

    impl TestObjectMetadataCommandState {
        fn same_subject_as(&self, earlier: &Self) -> bool {
            self.storage_domain == earlier.storage_domain
                && self.bucket == earlier.bucket
                && self.key == earlier.key
        }

        #[must_use]
        pub fn is_same_position_as(&self, earlier: &Self) -> bool {
            self.same_subject_as(earlier)
                && self.proof.applied_log_index == earlier.proof.applied_log_index
        }

        #[must_use]
        pub fn advanced_exactly_by(&self, earlier: &Self, command_count: u64) -> bool {
            self.same_subject_as(earlier)
                && earlier.proof.applied_log_index.checked_add(command_count)
                    == Some(self.proof.applied_log_index)
        }

        #[must_use]
        pub fn log_hash_changed_since(&self, earlier: &Self) -> bool {
            self.same_subject_as(earlier)
                && self.proof.applied_log_hash != earlier.proof.applied_log_hash
        }

        #[must_use]
        pub fn state_digest_changed_since(&self, earlier: &Self) -> bool {
            self.same_subject_as(earlier) && self.proof.state_digest != earlier.proof.state_digest
        }
    }

    impl fmt::Debug for TestObjectMetadataCommandState {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("TestObjectMetadataCommandState")
                .field("storage_domain", &"[redacted]")
                .field("subject", &"[redacted]")
                .field("command_state", &"[redacted]")
                .finish()
        }
    }

    pub type TestMetadataCommandApplyHook = Arc<
        dyn Fn(MetadataCommandApplyTestKind) -> Result<(), TestInjectedStorageFailure>
            + Send
            + Sync,
    >;
    pub type TestMetadataCommandApplyHookGuard =
        super::cluster::MetadataCommandApplyContextTestHookGuard;

    /// Logical metadata-command evidence and deterministic faults for
    /// cross-crate request tests.
    ///
    /// Storage owns subject routing, primary selection, and construction of
    /// low-level command-log failures. Callers can select only a logical
    /// bucket/object subject and command kind.
    pub trait StorageClusterMetadataCommandTestSupport {
        fn test_capture_object_metadata_command_state(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
        ) -> Result<TestObjectMetadataCommandState, TestStorageFailure>;

        fn test_install_before_bucket_metadata_command_primary_apply_hook(
            &self,
            bucket: &BucketName,
            hook: TestMetadataCommandApplyHook,
        ) -> TestMetadataCommandApplyHookGuard;

        fn test_install_before_object_metadata_command_primary_apply_hook(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            hook: TestMetadataCommandApplyHook,
        ) -> TestMetadataCommandApplyHookGuard;

        fn test_install_bucket_metadata_command_log_conflict(
            &self,
            bucket: &BucketName,
            kind: MetadataCommandApplyTestKind,
        ) -> TestMetadataCommandApplyHookGuard;

        fn test_install_object_metadata_command_log_conflict(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            kind: MetadataCommandApplyTestKind,
        ) -> TestMetadataCommandApplyHookGuard;

        fn test_install_object_metadata_command_log_conflict_once(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            kind: MetadataCommandApplyTestKind,
        ) -> TestMetadataCommandApplyHookGuard;

        fn test_install_object_metadata_command_stale_apply_once(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            kind: MetadataCommandApplyTestKind,
        ) -> TestMetadataCommandApplyHookGuard;
    }

    impl StorageClusterMetadataCommandTestSupport for StorageCluster {
        fn test_capture_object_metadata_command_state(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
        ) -> Result<TestObjectMetadataCommandState, TestStorageFailure> {
            Ok(TestObjectMetadataCommandState {
                storage_domain: self.process_local_registry_key(),
                bucket: bucket.clone(),
                key: key.clone(),
                proof: self
                    .test_object_pg_metadata_proof(bucket, key)
                    .map_err(TestStorageFailure::from_store)?,
            })
        }

        fn test_install_before_bucket_metadata_command_primary_apply_hook(
            &self,
            bucket: &BucketName,
            hook: TestMetadataCommandApplyHook,
        ) -> TestMetadataCommandApplyHookGuard {
            let pg_id = PgId::new(self.test_bucket_pg_id_for(bucket));
            let primary_node = self
                .local_pg_route(pg_id)
                .expect("test bucket metadata route must exist")
                .primary_node_id();
            let bucket = bucket.clone();
            self.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
                if context.node_id == primary_node
                    && context.bucket.as_ref() == Some(&bucket)
                    && context.key.is_none()
                {
                    hook(context.kind).map_err(TestInjectedStorageFailure::into_store_error)?;
                }
                Ok(())
            }))
        }

        fn test_install_before_object_metadata_command_primary_apply_hook(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            hook: TestMetadataCommandApplyHook,
        ) -> TestMetadataCommandApplyHookGuard {
            let pg_id = PgId::new(self.test_object_pg_id_for(bucket, key));
            let primary_node = self
                .local_pg_route(pg_id)
                .expect("test object metadata route must exist")
                .primary_node_id();
            let bucket = bucket.clone();
            let key = key.clone();
            self.test_install_before_metadata_command_apply_context_hook(Arc::new(move |context| {
                if context.node_id == primary_node
                    && context.bucket.as_ref() == Some(&bucket)
                    && context.key.as_ref() == Some(&key)
                {
                    hook(context.kind).map_err(TestInjectedStorageFailure::into_store_error)?;
                }
                Ok(())
            }))
        }

        fn test_install_bucket_metadata_command_log_conflict(
            &self,
            bucket: &BucketName,
            kind: MetadataCommandApplyTestKind,
        ) -> TestMetadataCommandApplyHookGuard {
            let pg_id = self.test_bucket_pg_id_for(bucket);
            let primary_node = self
                .local_pg_route(PgId::new(pg_id))
                .expect("test bucket metadata route must exist")
                .primary_node_id();
            let cluster_epoch = self.operation_epoch();
            self.test_install_before_bucket_metadata_command_primary_apply_hook(
                bucket,
                Arc::new(move |observed_kind| {
                    if observed_kind == kind {
                        return Err(TestInjectedStorageFailure::new(
                            StoreError::MetadataCommandLogConflict {
                                node_id: primary_node.as_u32(),
                                pg_id,
                                cluster_epoch,
                                log_index: 1,
                            },
                        ));
                    }
                    Ok(())
                }),
            )
        }

        fn test_install_object_metadata_command_log_conflict(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            kind: MetadataCommandApplyTestKind,
        ) -> TestMetadataCommandApplyHookGuard {
            let pg_id = self.test_object_pg_id_for(bucket, key);
            let primary_node = self
                .local_pg_route(PgId::new(pg_id))
                .expect("test object metadata route must exist")
                .primary_node_id();
            let cluster_epoch = self.operation_epoch();
            self.test_install_before_object_metadata_command_primary_apply_hook(
                bucket,
                key,
                Arc::new(move |observed_kind| {
                    if observed_kind == kind {
                        return Err(TestInjectedStorageFailure::new(
                            StoreError::MetadataCommandLogConflict {
                                node_id: primary_node.as_u32(),
                                pg_id,
                                cluster_epoch,
                                log_index: 1,
                            },
                        ));
                    }
                    Ok(())
                }),
            )
        }

        fn test_install_object_metadata_command_log_conflict_once(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            kind: MetadataCommandApplyTestKind,
        ) -> TestMetadataCommandApplyHookGuard {
            let pg_id = self.test_object_pg_id_for(bucket, key);
            let primary_node = self
                .local_pg_route(PgId::new(pg_id))
                .expect("test object metadata route must exist")
                .primary_node_id();
            let cluster_epoch = self.operation_epoch();
            let pending_failure = AtomicBool::new(true);
            self.test_install_before_object_metadata_command_primary_apply_hook(
                bucket,
                key,
                Arc::new(move |observed_kind| {
                    if observed_kind == kind && pending_failure.swap(false, Ordering::SeqCst) {
                        return Err(TestInjectedStorageFailure::new(
                            StoreError::MetadataCommandLogConflict {
                                node_id: primary_node.as_u32(),
                                pg_id,
                                cluster_epoch,
                                log_index: 1,
                            },
                        ));
                    }
                    Ok(())
                }),
            )
        }

        fn test_install_object_metadata_command_stale_apply_once(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            kind: MetadataCommandApplyTestKind,
        ) -> TestMetadataCommandApplyHookGuard {
            let pg_id = self.test_object_pg_id_for(bucket, key);
            let operation_epoch = self.operation_epoch();
            let current_epoch = operation_epoch
                .get()
                .checked_add(1)
                .and_then(ClusterEpoch::new)
                .expect("test operation epoch must permit a successor");
            let pending_failure = AtomicBool::new(true);
            self.test_install_before_object_metadata_command_primary_apply_hook(
                bucket,
                key,
                Arc::new(move |observed_kind| {
                    if observed_kind == kind && pending_failure.swap(false, Ordering::SeqCst) {
                        return Err(TestInjectedStorageFailure::new(
                            StoreError::StaleMetadataOperation {
                                pg_id,
                                operation_epoch,
                                current_epoch,
                            },
                        ));
                    }
                    Ok(())
                }),
            )
        }
    }

    /// Logical lifecycle observations shared by the storage maintenance
    /// sweepers.
    ///
    /// The production sweeper types deliberately expose no publicly callable
    /// inherent test methods. Cross-crate tests must opt into this
    /// owner-namespaced surface to observe whether a worker is active or which
    /// runtime-map generation it currently follows.
    #[cfg(feature = "test-hooks")]
    pub trait StorageMaintenanceSweeperTestSupport {
        fn test_is_enabled(&self) -> bool;
        fn test_routes_to(&self, expected: &Arc<StorageCluster>) -> bool;
    }

    #[cfg(feature = "test-hooks")]
    impl StorageMaintenanceSweeperTestSupport for StorageReclaimSweeper {
        fn test_is_enabled(&self) -> bool {
            StorageReclaimSweeper::test_is_enabled(self)
        }

        fn test_routes_to(&self, expected: &Arc<StorageCluster>) -> bool {
            StorageReclaimSweeper::test_routes_to(self, expected)
        }
    }

    #[cfg(feature = "test-hooks")]
    impl StorageMaintenanceSweeperTestSupport for StorageShardScavengerSweeper {
        fn test_is_enabled(&self) -> bool {
            StorageShardScavengerSweeper::test_is_enabled(self)
        }

        fn test_routes_to(&self, expected: &Arc<StorageCluster>) -> bool {
            StorageShardScavengerSweeper::test_routes_to(self, expected)
        }
    }

    #[cfg(feature = "test-hooks")]
    impl StorageMaintenanceSweeperTestSupport for StorageShardRepairSweeper {
        fn test_is_enabled(&self) -> bool {
            StorageShardRepairSweeper::test_is_enabled(self)
        }

        fn test_routes_to(&self, expected: &Arc<StorageCluster>) -> bool {
            StorageShardRepairSweeper::test_routes_to(self, expected)
        }
    }

    #[cfg(feature = "test-hooks")]
    impl StorageMaintenanceSweeperTestSupport for StorageShardBackfillSweeper {
        fn test_is_enabled(&self) -> bool {
            StorageShardBackfillSweeper::test_is_enabled(self)
        }

        fn test_routes_to(&self, expected: &Arc<StorageCluster>) -> bool {
            StorageShardBackfillSweeper::test_routes_to(self, expected)
        }
    }

    #[cfg(feature = "test-hooks")]
    impl StorageMaintenanceSweeperTestSupport for StorageStreamSessionSweeper {
        fn test_is_enabled(&self) -> bool {
            StorageStreamSessionSweeper::test_is_enabled(self)
        }

        fn test_routes_to(&self, expected: &Arc<StorageCluster>) -> bool {
            StorageStreamSessionSweeper::test_routes_to(self, expected)
        }
    }

    /// Deterministic lifecycle control for the durable reclaim worker.
    #[cfg(feature = "test-hooks")]
    pub trait StorageReclaimSweeperTestSupport {
        fn test_stop(&self);
    }

    #[cfg(feature = "test-hooks")]
    impl StorageReclaimSweeperTestSupport for StorageReclaimSweeper {
        fn test_stop(&self) {
            StorageReclaimSweeper::test_stop(self);
        }
    }

    /// Deterministic execution of one queued shard-repair item.
    #[cfg(feature = "test-hooks")]
    pub trait StorageShardRepairSweeperTestSupport {
        fn test_repair_one_pending(&self) -> bool;
    }

    #[cfg(feature = "test-hooks")]
    impl StorageShardRepairSweeperTestSupport for StorageShardRepairSweeper {
        fn test_repair_one_pending(&self) -> bool {
            StorageShardRepairSweeper::test_repair_one_pending(self)
        }
    }

    /// Deterministic execution of one abandoned stream-session sweep.
    #[cfg(feature = "test-hooks")]
    pub trait StorageStreamSessionSweeperTestSupport {
        fn test_sweep_once(&self) -> StorageStreamSessionSweepTestSummary;
    }

    #[cfg(feature = "test-hooks")]
    impl StorageStreamSessionSweeperTestSupport for StorageStreamSessionSweeper {
        fn test_sweep_once(&self) -> StorageStreamSessionSweepTestSummary {
            StorageStreamSessionSweeper::test_sweep_once(self)
        }
    }

    /// Logical worker observations and storage-owned lifecycle setup for
    /// cross-crate coordinator tests.
    ///
    /// These operations deliberately avoid exposing durable rows, queue
    /// entries, or timestamps. Storage owns the physical setup and projects
    /// only the worker state or semantic transition required by the caller.
    pub trait StorageClusterLifecycleTestSupport {
        fn test_seed_missing_bucket_finalize_work(
            &self,
            bucket: &BucketName,
        ) -> Result<(), BucketWriteDrainFailure>;

        fn test_seed_current_bucket_finalize_work(
            &self,
            bucket: &BucketName,
        ) -> Result<TestBucketDeleteFinalizeRoot, BucketWriteDrainFailure>;

        fn test_duplicate_bucket_finalize_work(&self, root: &TestBucketDeleteFinalizeRoot);

        fn test_finalize_deleting_bucket_metadata_if_present(
            &self,
            bucket: &BucketName,
        ) -> Result<(), BucketWriteDrainFailure>;

        fn test_hold_bucket_metadata(
            &self,
            bucket: &BucketName,
        ) -> Result<TestBucketMetadataHoldGuard<'_>, TestStorageFailure>;

        fn test_bucket_presence(
            &self,
            bucket: &BucketName,
        ) -> Result<TestBucketPresence, BucketSnapshotLoadFailure>;

        fn test_bucket_execution_generation_is_newer_than(
            &self,
            bucket: &BucketName,
            generation: u64,
        ) -> Result<bool, BucketSnapshotLoadFailure>;

        fn test_capture_bucket_delete_begin_subject(
            &self,
            bucket: &BucketName,
        ) -> Result<TestBucketDeleteBeginSubject, BucketSnapshotLoadFailure>;

        fn test_begin_current_bucket_delete(
            &self,
            bucket: &BucketName,
        ) -> Result<(), BucketWriteDrainFailure>;

        fn test_enqueue_bucket_delete_begin_subject(&self, subject: &TestBucketDeleteBeginSubject);

        fn test_current_bucket_delete_marked_once(
            &self,
            subject: &TestBucketDeleteBeginSubject,
        ) -> Result<bool, BucketSnapshotLoadFailure>;

        fn test_current_bucket_is_distinct_active_incarnation(
            &self,
            subject: &TestBucketDeleteBeginSubject,
        ) -> Result<bool, BucketSnapshotLoadFailure>;

        fn test_seed_bucket_delete_attempt(
            &self,
            bucket: &BucketName,
            outcome: TestBucketDeleteAttemptOutcomeKind,
            phase: TestBucketDeleteAttemptPhase,
            detail: String,
        ) -> Result<TestBucketDeleteBeginSubject, BucketWriteDrainFailure>;

        fn test_observe_bucket_delete_progress(
            &self,
            bucket: &BucketName,
        ) -> Result<TestBucketDeleteProgress, BucketSnapshotLoadFailure>;

        fn test_stream_upload_reservation_exists(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            session_id: &SessionId,
        ) -> Result<bool, TestStorageFailure>;

        fn test_bucket_delete_finalize_outstanding_depth(&self) -> usize;

        fn test_object_payload_reclaim_outstanding_depth(&self) -> usize;

        fn test_seed_stale_lifecycle_sweep_claim(
            &self,
            bucket: &BucketName,
        ) -> Result<(), TestStorageFailure>;

        fn test_age_noncurrent_lifecycle_version(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
        ) -> Result<TestLifecycleObjectObservation, TestStorageFailure>;

        fn test_begin_durable_bucket_delete_drain(
            &self,
            bucket: &BucketName,
        ) -> Result<(), BucketWriteDrainFailure>;

        fn test_mark_multipart_upload_aborting(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<(), TestStorageFailure>;

        fn test_mark_multipart_upload_completing(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<(), TestStorageFailure>;

        fn test_mark_stream_upload_stale(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            session_id: &SessionId,
        ) -> Result<(), TestStorageFailure>;
    }

    impl StorageClusterLifecycleTestSupport for StorageCluster {
        fn test_seed_missing_bucket_finalize_work(
            &self,
            bucket: &BucketName,
        ) -> Result<(), BucketWriteDrainFailure> {
            StorageCluster::test_enqueue_missing_bucket_delete_finalize(self, bucket)
                .map_err(BucketWriteDrainFailure::from)
        }

        fn test_seed_current_bucket_finalize_work(
            &self,
            bucket: &BucketName,
        ) -> Result<TestBucketDeleteFinalizeRoot, BucketWriteDrainFailure> {
            StorageCluster::test_enqueue_current_bucket_delete_finalize(self, bucket)
                .map_err(BucketWriteDrainFailure::from)
        }

        fn test_duplicate_bucket_finalize_work(&self, root: &TestBucketDeleteFinalizeRoot) {
            StorageCluster::test_reenqueue_bucket_delete_finalize(self, root);
        }

        fn test_finalize_deleting_bucket_metadata_if_present(
            &self,
            bucket: &BucketName,
        ) -> Result<(), BucketWriteDrainFailure> {
            match StorageCluster::test_delete_bucket_metadata(self, bucket) {
                Ok(())
                | Err(BucketWriteDrainError::Metadata(MetadataError::BucketNotFound { .. })) => {
                    Ok(())
                }
                Err(error) => Err(error.into()),
            }
        }

        fn test_hold_bucket_metadata(
            &self,
            bucket: &BucketName,
        ) -> Result<TestBucketMetadataHoldGuard<'_>, TestStorageFailure> {
            Ok(TestBucketMetadataHoldGuard {
                _inner: StorageCluster::test_lock_bucket_pg(self, bucket)
                    .map_err(TestStorageFailure::from_store)?,
            })
        }

        fn test_bucket_presence(
            &self,
            bucket: &BucketName,
        ) -> Result<TestBucketPresence, BucketSnapshotLoadFailure> {
            match StorageCluster::test_head_bucket_raw(self, bucket) {
                Ok(info) => Ok(match info.state {
                    BucketState::Active => TestBucketPresence::Active,
                    BucketState::Deleting => TestBucketPresence::Deleting,
                }),
                Err(BucketSnapshotLoadError::Metadata(MetadataError::BucketNotFound {
                    ..
                })) => Ok(TestBucketPresence::Missing),
                Err(error) => Err(error.into()),
            }
        }

        fn test_bucket_execution_generation_is_newer_than(
            &self,
            bucket: &BucketName,
            generation: u64,
        ) -> Result<bool, BucketSnapshotLoadFailure> {
            Ok(
                StorageCluster::test_head_bucket_raw(self, bucket)?.bucket_execution_generation
                    > generation,
            )
        }

        fn test_capture_bucket_delete_begin_subject(
            &self,
            bucket: &BucketName,
        ) -> Result<TestBucketDeleteBeginSubject, BucketSnapshotLoadFailure> {
            let info = StorageCluster::test_head_bucket_raw(self, bucket)?;
            Ok(TestBucketDeleteBeginSubject {
                root: BucketDeleteBeginRoot {
                    bucket: bucket.clone(),
                    bucket_execution_generation: info.bucket_execution_generation,
                    bucket_incarnation_generation: info.bucket_incarnation_generation,
                },
            })
        }

        fn test_begin_current_bucket_delete(
            &self,
            bucket: &BucketName,
        ) -> Result<(), BucketWriteDrainFailure> {
            StorageCluster::test_begin_bucket_delete_if_current(self, bucket)
                .map_err(BucketWriteDrainFailure::from)
        }

        fn test_enqueue_bucket_delete_begin_subject(&self, subject: &TestBucketDeleteBeginSubject) {
            StorageCluster::test_enqueue_bucket_delete_begin(
                self,
                &subject.root.bucket,
                subject.root.bucket_execution_generation,
                subject.root.bucket_incarnation_generation,
            );
        }

        fn test_current_bucket_delete_marked_once(
            &self,
            subject: &TestBucketDeleteBeginSubject,
        ) -> Result<bool, BucketSnapshotLoadFailure> {
            let info = StorageCluster::test_head_bucket_raw(self, &subject.root.bucket)?;
            let expected_execution_generation =
                subject.root.bucket_execution_generation.checked_add(1);
            Ok(info.state == BucketState::Deleting
                && Some(info.bucket_execution_generation) == expected_execution_generation
                && info.bucket_incarnation_generation == subject.root.bucket_incarnation_generation)
        }

        fn test_current_bucket_is_distinct_active_incarnation(
            &self,
            subject: &TestBucketDeleteBeginSubject,
        ) -> Result<bool, BucketSnapshotLoadFailure> {
            let info = StorageCluster::test_head_bucket_raw(self, &subject.root.bucket)?;
            Ok(info.state == BucketState::Active
                && info.bucket_incarnation_generation != subject.root.bucket_incarnation_generation)
        }

        fn test_seed_bucket_delete_attempt(
            &self,
            bucket: &BucketName,
            outcome: TestBucketDeleteAttemptOutcomeKind,
            phase: TestBucketDeleteAttemptPhase,
            detail: String,
        ) -> Result<TestBucketDeleteBeginSubject, BucketWriteDrainFailure> {
            let post_reservation_next_object_pg_id = matches!(
                phase,
                TestBucketDeleteAttemptPhase::FinalVisibilityCheck
                    | TestBucketDeleteAttemptPhase::FinalVisibilityProven
            )
            .then_some(0);
            let root = StorageCluster::test_seed_bucket_delete_attempt_outcome(
                self,
                bucket,
                outcome,
                phase,
                detail,
                post_reservation_next_object_pg_id,
            )
            .map_err(BucketWriteDrainFailure::from)?;
            Ok(TestBucketDeleteBeginSubject { root })
        }

        fn test_observe_bucket_delete_progress(
            &self,
            bucket: &BucketName,
        ) -> Result<TestBucketDeleteProgress, BucketSnapshotLoadFailure> {
            StorageCluster::test_bucket_delete_progress(self, bucket)
                .map_err(BucketSnapshotLoadFailure::from)
        }

        fn test_stream_upload_reservation_exists(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            session_id: &SessionId,
        ) -> Result<bool, TestStorageFailure> {
            match StorageCluster::test_object_generation_reservation_for(
                self, bucket, key, session_id,
            ) {
                Ok(_) => Ok(true),
                Err(ObjectPgActionError::Metadata(
                    MetadataError::ObjectGenerationReservationNotFound { .. },
                )) => Ok(false),
                Err(error) => Err(TestStorageFailure::from_object_pg_action(error)),
            }
        }

        fn test_bucket_delete_finalize_outstanding_depth(&self) -> usize {
            StorageCluster::test_bucket_delete_finalize_outstanding_depth(self)
        }

        fn test_object_payload_reclaim_outstanding_depth(&self) -> usize {
            StorageCluster::test_object_payload_reclaim_outstanding_depth(self)
        }

        fn test_seed_stale_lifecycle_sweep_claim(
            &self,
            bucket: &BucketName,
        ) -> Result<(), TestStorageFailure> {
            StorageCluster::test_seed_stale_lifecycle_sweep_claim(self, bucket)
                .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_age_noncurrent_lifecycle_version(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
        ) -> Result<TestLifecycleObjectObservation, TestStorageFailure> {
            StorageCluster::test_age_noncurrent_live_object(self, bucket, key, version_id, 1)
                .map_err(TestStorageFailure::from_object_pg_action)?;
            capture_lifecycle_object_observation(self, bucket, key, version_id)
        }

        fn test_begin_durable_bucket_delete_drain(
            &self,
            bucket: &BucketName,
        ) -> Result<(), BucketWriteDrainFailure> {
            StorageCluster::test_begin_durable_bucket_delete_drain(self, bucket)
                .map_err(BucketWriteDrainFailure::from)
        }

        fn test_mark_multipart_upload_aborting(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<(), TestStorageFailure> {
            StorageCluster::test_mark_multipart_upload_aborting(self, bucket, key, upload_id)
                .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_mark_multipart_upload_completing(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<(), TestStorageFailure> {
            StorageCluster::test_mark_multipart_upload_completing(self, bucket, key, upload_id)
                .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_mark_stream_upload_stale(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            session_id: &SessionId,
        ) -> Result<(), TestStorageFailure> {
            StorageCluster::test_mark_stream_upload_stale(self, bucket, key, session_id)
                .map_err(TestStorageFailure::from_object_pg_action)
        }
    }

    /// Logical multipart observations and storage-owned semantic fault setup
    /// for cross-crate coordinator tests.
    ///
    /// Durable upload, part, and object-manifest records remain private to
    /// storage. Callers receive only the production authorization candidate,
    /// narrow logical part observations, or the result of an owner-defined
    /// corruption scenario.
    pub trait StorageClusterMultipartTestSupport {
        fn test_get_multipart_completion_candidate(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<MultipartUploadCompletionCandidate, TestStorageFailure>;

        fn test_get_multipart_part_observation(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
            part_number: u16,
        ) -> Result<TestMultipartPartObservation, TestStorageFailure>;

        fn test_get_object_part_numbers(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
        ) -> Result<Vec<u32>, TestStorageFailure>;

        fn test_inject_object_part_payload_checksum_mismatch(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
            part_number: u32,
        ) -> Result<(), TestStorageFailure>;

        fn test_inject_incomplete_multipart_manifest(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
            part_number: u32,
        ) -> Result<(), TestStorageFailure>;
    }

    impl StorageClusterMultipartTestSupport for StorageCluster {
        fn test_get_multipart_completion_candidate(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<MultipartUploadCompletionCandidate, TestStorageFailure> {
            StorageCluster::test_get_multipart_completion_candidate(self, bucket, key, upload_id)
                .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_get_multipart_part_observation(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
            part_number: u16,
        ) -> Result<TestMultipartPartObservation, TestStorageFailure> {
            StorageCluster::test_get_multipart_part_observation(
                self,
                bucket,
                key,
                upload_id,
                part_number,
            )
            .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_get_object_part_numbers(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
        ) -> Result<Vec<u32>, TestStorageFailure> {
            StorageCluster::test_get_object_part_numbers(self, bucket, key, version_id)
                .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_inject_object_part_payload_checksum_mismatch(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
            part_number: u32,
        ) -> Result<(), TestStorageFailure> {
            StorageCluster::test_inject_object_part_payload_checksum_mismatch(
                self,
                bucket,
                key,
                version_id,
                part_number,
            )
            .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_inject_incomplete_multipart_manifest(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
            part_number: u32,
        ) -> Result<(), TestStorageFailure> {
            StorageCluster::test_inject_incomplete_multipart_manifest(
                self,
                bucket,
                key,
                version_id,
                part_number,
            )
            .map_err(TestStorageFailure::from_object_pg_action)
        }
    }

    /// Narrow logical and at-rest observations for committed objects.
    pub trait StorageClusterObjectTestSupport {
        fn test_observe_stored_sse_customer_checksum(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
            cleartext_checksum: &str,
        ) -> Result<TestStoredSseCustomerChecksumObservation, TestStorageFailure>;

        fn test_delete_marker_version_has_owner(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
            expected_owner: &OwnerIdentity,
        ) -> Result<bool, TestStorageFailure>;
    }

    impl StorageClusterObjectTestSupport for StorageCluster {
        fn test_observe_stored_sse_customer_checksum(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
            cleartext_checksum: &str,
        ) -> Result<TestStoredSseCustomerChecksumObservation, TestStorageFailure> {
            StorageCluster::test_observe_stored_sse_customer_checksum(
                self,
                bucket,
                key,
                version_id,
                cleartext_checksum,
            )
            .map_err(TestStorageFailure::from_object_pg_action)
        }

        fn test_delete_marker_version_has_owner(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
            expected_owner: &OwnerIdentity,
        ) -> Result<bool, TestStorageFailure> {
            delete_marker_version_has_owner_raw_for_owner_test(
                self,
                bucket,
                key,
                version_id,
                expected_owner,
            )
            .map_err(TestStorageFailure::from_object_pg_action)
        }
    }

    pub(crate) fn delete_marker_version_has_owner_raw_for_owner_test(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        expected_owner: &OwnerIdentity,
    ) -> Result<bool, ObjectPgActionError> {
        let object = cluster.test_get_object_version(bucket, key, version_id)?;
        let StoredObject::DeleteMarker(marker) = object else {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "selected object version is not a delete marker".to_string(),
            });
        };
        Ok(marker.owner == *expected_owner)
    }

    mod retained_read;
    pub use retained_read::{TestRetainedReadPgMoveScenario, TestRetainedReadPgMoveScenarioError};

    mod payload_write;
    pub use payload_write::{
        install_payload_shard_write_attempt_hook,
        payload_shard_write_retryable_convergence_failure, TestPayloadShardWriteAttemptGuard,
        TestPayloadShardWriteAttemptHook, TestPayloadShardWriteAttempts,
    };

    mod pg_topology;
    pub use pg_topology::{PgTopologyPlacementTestSupport, TestObjectDataPgSelection};

    mod failure_scheduling;
    pub use failure_scheduling::{StorageClusterFailureTestSupport, TestStorageFailureGuard};

    mod clock;
    pub use clock::{
        test_time_override_guard, with_time_and_monotonic_override, TestClockOverrideControl,
        TestClockOverrideGuard,
    };

    mod scheduling;
    pub use scheduling::{
        StorageClusterSchedulingTestSupport, TestBucketDeleteExactDrainSchedulingAction,
        TestBucketDeleteExactDrainStart, TestBucketDeletePostReservationProgress,
        TestBucketDeletePostReservationProgressAction, TestStorageFallibleSchedulingAction,
        TestStorageSchedulingAction, TestStorageSchedulingGuard,
    };

    mod static_topology;
    pub use static_topology::StaticInitialControlPlaneTopologyTestSupport;

    mod stream_route;
    pub use stream_route::{
        ActivePutObjectRouteTestSupport, ActiveStreamRouteTestSupport,
        StorageClusterStreamSessionTestSupport,
    };

    mod topology;
    #[cfg(test)]
    pub(crate) use topology::stream_put_session_crosses_metadata_and_data_pgs_raw;
    pub use topology::{
        StorageClusterRuntimeMapTopologyTestSupport, StorageClusterTopologyTestSupport,
        TestStorageTopologyScenarioError,
    };

    /// Construct an opaque representative of an operation-level storage failure.
    ///
    /// Cross-crate tests use this to verify their protocol translation without
    /// depending on a PG, route, command-log, or RPC error variant.
    #[must_use]
    pub(crate) fn store_error_for_operation_failure_class(
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

    /// Construct an opaque operation-level storage failure.
    ///
    /// This is the representation accepted by public route-admission APIs;
    /// cross-crate tests can verify response policy without reconstructing a
    /// route, PG, command-log, or RPC error.
    #[must_use]
    pub fn store_failure_for_operation_failure_class(
        class: StoreOperationFailureClass,
    ) -> StoreFailure {
        store_error_for_operation_failure_class(class).into()
    }

    /// Construct an opaque generic storage failure containing a bounded I/O
    /// diagnostic and return the private fragments which must stay redacted.
    #[must_use]
    pub fn store_failure_diagnostic_fixture() -> (StoreFailure, &'static [&'static str]) {
        const SECRET_CONTEXT: &str = "secret generic storage fixture operation";
        const SECRET_SOURCE: &str = "secret generic storage fixture source";
        (
            StoreError::Io {
                context: SECRET_CONTEXT,
                source: std::io::Error::other(SECRET_SOURCE),
            }
            .into(),
            &[SECRET_CONTEXT, SECRET_SOURCE],
        )
    }

    /// Construct an opaque bucket-write drain failure from its logical outcome.
    ///
    /// This lets cross-crate response-mapping tests remain exhaustive without
    /// constructing storage implementation errors.
    #[must_use]
    pub fn bucket_write_drain_failure_for_kind(
        kind: BucketWriteDrainFailureKind,
    ) -> BucketWriteDrainFailure {
        BucketWriteDrainFailure::for_test(kind)
    }

    /// Construct an opaque bucket-snapshot failure from its logical outcome.
    ///
    /// This lets cross-crate response-mapping tests remain exhaustive without
    /// constructing storage implementation errors.
    #[must_use]
    pub fn bucket_snapshot_load_failure_for_kind(
        kind: BucketSnapshotLoadFailureKind,
    ) -> BucketSnapshotLoadFailure {
        BucketSnapshotLoadFailure::for_test(kind)
    }

    /// Construct an opaque bucket-listing failure from its logical outcome.
    #[must_use]
    pub fn bucket_listing_failure_for_kind(kind: BucketListingFailureKind) -> BucketListingFailure {
        BucketListingFailure::for_test(kind)
    }

    /// Construct an opaque lifecycle-maintenance failure from its logical outcome.
    #[must_use]
    pub fn lifecycle_maintenance_failure_for_kind(
        kind: LifecycleMaintenanceFailureKind,
    ) -> LifecycleMaintenanceFailure {
        LifecycleMaintenanceFailure::for_test(kind)
    }

    /// Construct an opaque lifecycle-mutation failure from its logical outcome.
    #[must_use]
    pub fn lifecycle_mutation_failure_for_kind(
        kind: LifecycleMutationFailureKind,
    ) -> LifecycleMutationFailure {
        LifecycleMutationFailure::for_test(kind)
    }

    /// Construct an opaque object-read failure from its logical outcome.
    ///
    /// This lets cross-crate response-mapping tests remain exhaustive without
    /// constructing storage implementation errors.
    #[must_use]
    pub fn object_read_failure_for_kind(kind: ObjectReadFailureKind) -> ObjectReadFailure {
        ObjectReadFailure::for_test(kind)
    }

    /// Construct an opaque object-metadata listing failure from its logical outcome.
    #[must_use]
    pub fn object_metadata_listing_failure_for_kind(
        kind: ObjectMetadataListingFailureKind,
    ) -> ObjectMetadataListingFailure {
        ObjectMetadataListingFailure::for_test(kind)
    }

    /// Construct an opaque object-metadata mutation failure from its logical outcome.
    #[must_use]
    pub fn object_metadata_mutation_failure_for_kind(
        kind: ObjectMetadataMutationFailureKind,
    ) -> ObjectMetadataMutationFailure {
        ObjectMetadataMutationFailure::for_test(kind)
    }

    /// Construct an opaque stream-upload failure from its logical outcome.
    #[must_use]
    pub fn stream_upload_failure_for_kind(kind: StreamUploadFailureKind) -> StreamUploadFailure {
        StreamUploadFailure::for_test(kind)
    }

    /// Construct an opaque direct-PutObject failure from its logical outcome.
    #[must_use]
    pub fn direct_put_failure_for_kind(kind: DirectPutFailureKind) -> DirectPutFailure {
        DirectPutFailure::for_test(kind)
    }

    /// Construct an opaque multipart-management failure from its logical outcome.
    #[must_use]
    pub fn multipart_management_failure_for_kind(
        kind: MultipartManagementFailureKind,
    ) -> MultipartManagementFailure {
        MultipartManagementFailure::for_test(kind)
    }

    /// Construct an opaque multipart-completion failure from its logical outcome.
    #[must_use]
    pub fn multipart_completion_failure_for_kind(
        kind: MultipartCompletionFailureKind,
    ) -> MultipartCompletionFailure {
        MultipartCompletionFailure::for_test(kind)
    }

    /// Construct an opaque multipart-management failure containing a bounded
    /// I/O diagnostic and return the private fragments which must stay redacted.
    #[must_use]
    pub fn multipart_management_failure_diagnostic_fixture(
    ) -> (MultipartManagementFailure, &'static [&'static str]) {
        const SECRET_CONTEXT: &str = "secret multipart management fixture operation";
        const SECRET_SOURCE: &str = "secret multipart management fixture source";
        (
            MultipartManagementFailure::from_store(StoreError::Io {
                context: SECRET_CONTEXT,
                source: std::io::Error::other(SECRET_SOURCE),
            }),
            &[SECRET_CONTEXT, SECRET_SOURCE],
        )
    }

    /// Construct an opaque multipart-completion failure containing a bounded
    /// I/O diagnostic and return the private fragments which must stay redacted.
    #[must_use]
    pub fn multipart_completion_failure_diagnostic_fixture(
    ) -> (MultipartCompletionFailure, &'static [&'static str]) {
        const SECRET_CONTEXT: &str = "secret multipart completion fixture operation";
        const SECRET_SOURCE: &str = "secret multipart completion fixture source";
        (
            MultipartCompletionFailure::from_object_pg_action(ObjectPgActionError::Store(
                StoreError::Io {
                    context: SECRET_CONTEXT,
                    source: std::io::Error::other(SECRET_SOURCE),
                },
            )),
            &[SECRET_CONTEXT, SECRET_SOURCE],
        )
    }

    /// Construct an opaque object-metadata listing failure containing a bounded
    /// I/O diagnostic and return the private fragments which must stay redacted.
    #[must_use]
    pub fn object_metadata_listing_failure_diagnostic_fixture(
    ) -> (ObjectMetadataListingFailure, &'static [&'static str]) {
        const SECRET_CONTEXT: &str = "secret object listing fixture operation";
        const SECRET_SOURCE: &str = "secret object listing fixture source";
        (
            ObjectMetadataListingFailure::from_store(StoreError::Io {
                context: SECRET_CONTEXT,
                source: std::io::Error::other(SECRET_SOURCE),
            }),
            &[SECRET_CONTEXT, SECRET_SOURCE],
        )
    }

    /// Construct an opaque bucket-listing failure containing a bounded I/O
    /// diagnostic and return the private fragments which must stay redacted.
    #[must_use]
    pub fn bucket_listing_failure_diagnostic_fixture(
    ) -> (BucketListingFailure, &'static [&'static str]) {
        const SECRET_CONTEXT: &str = "secret bucket listing fixture operation";
        const SECRET_SOURCE: &str = "secret bucket listing fixture source";
        (
            BucketListingFailure::from_store(StoreError::Io {
                context: SECRET_CONTEXT,
                source: std::io::Error::other(SECRET_SOURCE),
            }),
            &[SECRET_CONTEXT, SECRET_SOURCE],
        )
    }

    /// Construct an opaque lifecycle-maintenance failure containing a bounded
    /// I/O diagnostic and return the private fragments which must stay redacted.
    #[must_use]
    pub fn lifecycle_maintenance_failure_diagnostic_fixture(
    ) -> (LifecycleMaintenanceFailure, &'static [&'static str]) {
        const SECRET_CONTEXT: &str = "secret lifecycle maintenance fixture operation";
        const SECRET_SOURCE: &str = "secret lifecycle maintenance fixture source";
        (
            LifecycleMaintenanceFailure::from_store(StoreError::Io {
                context: SECRET_CONTEXT,
                source: std::io::Error::other(SECRET_SOURCE),
            }),
            &[SECRET_CONTEXT, SECRET_SOURCE],
        )
    }

    /// Construct an opaque lifecycle-mutation failure containing a bounded I/O
    /// diagnostic and return the private fragments which must stay redacted.
    #[must_use]
    pub fn lifecycle_mutation_failure_diagnostic_fixture(
    ) -> (LifecycleMutationFailure, &'static [&'static str]) {
        const SECRET_CONTEXT: &str = "secret lifecycle mutation fixture operation";
        const SECRET_SOURCE: &str = "secret lifecycle mutation fixture source";
        (
            LifecycleMutationFailure::from_store(StoreError::Io {
                context: SECRET_CONTEXT,
                source: std::io::Error::other(SECRET_SOURCE),
            }),
            &[SECRET_CONTEXT, SECRET_SOURCE],
        )
    }

    /// Construct an opaque direct-PutObject failure containing a bounded I/O
    /// diagnostic and return the private fragments which must stay redacted.
    #[must_use]
    pub fn direct_put_failure_diagnostic_fixture() -> (DirectPutFailure, &'static [&'static str]) {
        const SECRET_CONTEXT: &str = "secret direct PutObject fixture operation";
        const SECRET_SOURCE: &str = "secret direct PutObject fixture source";
        (
            DirectPutFailure::from_store(StoreError::Io {
                context: SECRET_CONTEXT,
                source: std::io::Error::other(SECRET_SOURCE),
            }),
            &[SECRET_CONTEXT, SECRET_SOURCE],
        )
    }

    /// Construct an opaque stream-upload failure containing a bounded I/O
    /// diagnostic and return the private fragments which must stay redacted.
    #[must_use]
    pub fn stream_upload_failure_diagnostic_fixture(
    ) -> (StreamUploadFailure, &'static [&'static str]) {
        const SECRET_CONTEXT: &str = "secret stream upload fixture operation";
        const SECRET_SOURCE: &str = "secret stream upload fixture source";
        (
            StreamUploadFailure::from_object_pg_action(ObjectPgActionError::Store(
                StoreError::Io {
                    context: SECRET_CONTEXT,
                    source: std::io::Error::other(SECRET_SOURCE),
                },
            )),
            &[SECRET_CONTEXT, SECRET_SOURCE],
        )
    }

    /// Construct an opaque object-metadata mutation failure containing a
    /// bounded I/O diagnostic and return the private fragments which must stay redacted.
    #[must_use]
    pub fn object_metadata_mutation_failure_diagnostic_fixture(
    ) -> (ObjectMetadataMutationFailure, &'static [&'static str]) {
        const SECRET_CONTEXT: &str = "secret object mutation fixture operation";
        const SECRET_SOURCE: &str = "secret object mutation fixture source";
        (
            ObjectMetadataMutationFailure::from_object_pg_action(ObjectPgActionError::Store(
                StoreError::Io {
                    context: SECRET_CONTEXT,
                    source: std::io::Error::other(SECRET_SOURCE),
                },
            )),
            &[SECRET_CONTEXT, SECRET_SOURCE],
        )
    }

    /// Construct an opaque object-read failure containing a bounded I/O
    /// diagnostic and return the private fragments which must stay redacted.
    #[must_use]
    pub fn object_read_failure_diagnostic_fixture() -> (ObjectReadFailure, &'static [&'static str])
    {
        const SECRET_CONTEXT: &str = "secret object read fixture operation";
        const SECRET_SOURCE: &str = "secret object read fixture source";
        (
            ObjectReadFailure::from_store(StoreError::Io {
                context: SECRET_CONTEXT,
                source: std::io::Error::other(SECRET_SOURCE),
            }),
            &[SECRET_CONTEXT, SECRET_SOURCE],
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub use super::cluster::MetadataCommandApplyTestKind;
    #[cfg(feature = "test-hooks")]
    pub use super::maintenance::{
        install_reclaim_worker_test_hooks, StorageReclaimWorkerTestHookGuard,
        StorageReclaimWorkerTestHooks, StorageStreamSessionSweepTestSummary,
    };
    #[cfg(feature = "test-hooks")]
    pub use super::node::{
        install_bucket_scoped_test_hooks, BucketScopedTestHookGuard, BucketScopedTestHooks,
    };

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
    ) -> Result<(), TestStorageFailure> {
        let segment = snapshot
            .segments()
            .iter()
            .find(|segment| segment.segment_index == segment_index)
            .ok_or_else(|| StoreError::Io {
                context: "select object payload segment for fault injection",
                source: std::io::Error::other(format!(
                    "captured payload has no segment {segment_index}"
                )),
            })
            .map_err(TestStorageFailure::from_store)?;
        for shard_index in 0..segment.ec_k + segment.ec_m {
            cluster
                .test_inject_object_payload_shard_loss(snapshot, segment_index, shard_index)
                .map_err(TestStorageFailure::from_store)?;
        }
        Ok(())
    }

    /// Reports whether the exact shard-owner set selected by an opaque payload
    /// snapshot currently holds deletion-exclusion leases for its generation.
    pub fn object_payload_snapshot_has_exact_shard_owner_leases(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<bool, TestStorageFailure> {
        cluster
            .test_object_payload_snapshot_has_exact_shard_owner_leases(snapshot)
            .map_err(TestStorageFailure::from_store)
    }

    /// Reports whether an opaque payload snapshot has no deletion-exclusion
    /// leases remaining on any storage node.
    pub fn object_payload_snapshot_has_no_leases(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<bool, TestStorageFailure> {
        cluster
            .test_object_payload_snapshot_has_no_leases(snapshot)
            .map_err(TestStorageFailure::from_store)
    }

    /// Opaque evidence for one storage-selected committed-payload shard fault.
    #[derive(Clone)]
    pub struct TestObjectPayloadShardFault {
        snapshot: TestObjectPayloadSnapshot,
        segment_index: u32,
        shard_index: u8,
    }

    impl std::fmt::Debug for TestObjectPayloadShardFault {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("TestObjectPayloadShardFault")
                .finish_non_exhaustive()
        }
    }

    /// Opaque evidence for a storage-selected set of committed-payload shard
    /// faults. Callers may choose a logical count, but storage retains the
    /// physical segment and shard selection.
    #[derive(Clone)]
    pub struct TestObjectPayloadShardFaultSet {
        faults: Vec<TestObjectPayloadShardFault>,
    }

    impl std::fmt::Debug for TestObjectPayloadShardFaultSet {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("TestObjectPayloadShardFaultSet")
                .field("fault_count", &self.faults.len())
                .finish_non_exhaustive()
        }
    }

    #[derive(Clone, Copy)]
    enum TestObjectPayloadShardRole {
        FirstData,
        FirstParity,
        LastParity,
    }

    fn select_object_payload_shard_fault(
        snapshot: &TestObjectPayloadSnapshot,
        role: TestObjectPayloadShardRole,
    ) -> Result<TestObjectPayloadShardFault, StoreError> {
        let segment = snapshot.segments().first().ok_or_else(|| StoreError::Io {
            context: "select object payload shard fault",
            source: std::io::Error::other("captured object payload has no segments"),
        })?;
        let shard_index = match role {
            TestObjectPayloadShardRole::FirstData => 0,
            TestObjectPayloadShardRole::FirstParity => segment.ec_k,
            TestObjectPayloadShardRole::LastParity => segment
                .ec_k
                .checked_add(segment.ec_m)
                .and_then(|count| count.checked_sub(1))
                .ok_or_else(|| StoreError::Io {
                    context: "select object payload shard fault",
                    source: std::io::Error::other("captured payload has no parity shard"),
                })?,
        };
        if matches!(
            role,
            TestObjectPayloadShardRole::FirstParity | TestObjectPayloadShardRole::LastParity
        ) && segment.ec_m == 0
        {
            return Err(StoreError::Io {
                context: "select object payload shard fault",
                source: std::io::Error::other("captured payload has no parity shard"),
            });
        }
        Ok(TestObjectPayloadShardFault {
            snapshot: snapshot.clone(),
            segment_index: segment.segment_index,
            shard_index,
        })
    }

    fn inject_object_payload_shard_loss_by_role(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
        role: TestObjectPayloadShardRole,
    ) -> Result<TestObjectPayloadShardFault, StoreError> {
        let fault = select_object_payload_shard_fault(snapshot, role)?;
        cluster.test_inject_object_payload_shard_loss(
            &fault.snapshot,
            fault.segment_index,
            fault.shard_index,
        )?;
        Ok(fault)
    }

    fn inject_object_payload_shard_corruption_by_role(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
        role: TestObjectPayloadShardRole,
    ) -> Result<TestObjectPayloadShardFault, StoreError> {
        let fault = select_object_payload_shard_fault(snapshot, role)?;
        cluster.test_inject_object_payload_shard_corruption(
            &fault.snapshot,
            fault.segment_index,
            fault.shard_index,
        )?;
        Ok(fault)
    }

    pub fn inject_object_payload_first_data_shard_loss(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<TestObjectPayloadShardFault, TestStorageFailure> {
        inject_object_payload_shard_loss_by_role(
            cluster,
            snapshot,
            TestObjectPayloadShardRole::FirstData,
        )
        .map_err(TestStorageFailure::from_store)
    }

    /// Removes the requested number of data shards from storage's first
    /// payload segment without exposing their physical indices.
    pub fn inject_object_payload_data_shard_losses(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
        count: usize,
    ) -> Result<TestObjectPayloadShardFaultSet, TestStorageFailure> {
        let segment = snapshot
            .segments()
            .first()
            .ok_or_else(|| StoreError::Io {
                context: "select object payload data-shard losses",
                source: std::io::Error::other("captured object payload has no segments"),
            })
            .map_err(TestStorageFailure::from_store)?;
        if count > usize::from(segment.ec_k) {
            return Err(TestStorageFailure::from_store(StoreError::Io {
                context: "select object payload data-shard losses",
                source: std::io::Error::other(format!(
                    "requested {count} data-shard losses from an EC layout with {} data shards",
                    segment.ec_k
                )),
            }));
        }
        let mut faults = Vec::with_capacity(count);
        for shard_index in 0..u8::try_from(count)
            .map_err(|_| StoreError::Io {
                context: "select object payload data-shard losses",
                source: std::io::Error::other("requested data-shard loss count does not fit u8"),
            })
            .map_err(TestStorageFailure::from_store)?
        {
            let fault = TestObjectPayloadShardFault {
                snapshot: snapshot.clone(),
                segment_index: segment.segment_index,
                shard_index,
            };
            cluster
                .test_inject_object_payload_shard_loss(
                    &fault.snapshot,
                    fault.segment_index,
                    fault.shard_index,
                )
                .map_err(TestStorageFailure::from_store)?;
            faults.push(fault);
        }
        Ok(TestObjectPayloadShardFaultSet { faults })
    }

    /// Schedules the requested number of data-shard repair wakes for storage's
    /// first payload segment without exposing their physical identities.
    pub fn schedule_object_payload_data_shard_repair_wakes(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
        count: usize,
    ) -> Result<TestObjectPayloadShardFaultSet, TestStorageFailure> {
        let segment = snapshot
            .segments()
            .first()
            .ok_or_else(|| StoreError::Io {
                context: "select object payload data-shard repair wakes",
                source: std::io::Error::other("captured object payload has no segments"),
            })
            .map_err(TestStorageFailure::from_store)?;
        if count > usize::from(segment.ec_k) {
            return Err(TestStorageFailure::from_store(StoreError::Io {
                context: "select object payload data-shard repair wakes",
                source: std::io::Error::other(format!(
                    "requested {count} data-shard repair wakes from an EC layout with {} data shards",
                    segment.ec_k
                )),
            }));
        }
        let mut faults = Vec::with_capacity(count);
        for shard_index in 0..u8::try_from(count)
            .map_err(|_| StoreError::Io {
                context: "select object payload data-shard repair wakes",
                source: std::io::Error::other("requested data-shard repair count does not fit u8"),
            })
            .map_err(TestStorageFailure::from_store)?
        {
            let fault = TestObjectPayloadShardFault {
                snapshot: snapshot.clone(),
                segment_index: segment.segment_index,
                shard_index,
            };
            cluster
                .test_schedule_object_payload_repair_wake(
                    &fault.snapshot,
                    fault.segment_index,
                    fault.shard_index,
                )
                .map_err(TestStorageFailure::from_store)?;
            faults.push(fault);
        }
        Ok(TestObjectPayloadShardFaultSet { faults })
    }

    /// Removes enough additional data shards to make repair of an already
    /// faulted first data shard exceed the payload's parity tolerance.
    pub fn inject_object_payload_additional_data_losses_beyond_parity(
        cluster: &StorageCluster,
        fault: &TestObjectPayloadShardFault,
    ) -> Result<TestObjectPayloadShardFaultSet, TestStorageFailure> {
        let segment = fault
            .snapshot
            .segments()
            .iter()
            .find(|segment| segment.segment_index == fault.segment_index)
            .ok_or_else(|| StoreError::Io {
                context: "select additional object payload data-shard losses",
                source: std::io::Error::other("fault segment is absent from its payload snapshot"),
            })
            .map_err(TestStorageFailure::from_store)?;
        if fault.shard_index != 0 || segment.ec_m >= segment.ec_k {
            return Err(TestStorageFailure::from_store(StoreError::Io {
                context: "select additional object payload data-shard losses",
                source: std::io::Error::other(
                    "first-data fault and at least one surviving data shard are required",
                ),
            }));
        }
        let mut faults = Vec::with_capacity(usize::from(segment.ec_m));
        for shard_index in 1..=segment.ec_m {
            let additional = TestObjectPayloadShardFault {
                snapshot: fault.snapshot.clone(),
                segment_index: fault.segment_index,
                shard_index,
            };
            cluster
                .test_inject_object_payload_shard_loss(
                    &additional.snapshot,
                    additional.segment_index,
                    additional.shard_index,
                )
                .map_err(TestStorageFailure::from_store)?;
            faults.push(additional);
        }
        Ok(TestObjectPayloadShardFaultSet { faults })
    }

    pub fn inject_object_payload_first_data_shard_corruption(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<TestObjectPayloadShardFault, TestStorageFailure> {
        inject_object_payload_shard_corruption_by_role(
            cluster,
            snapshot,
            TestObjectPayloadShardRole::FirstData,
        )
        .map_err(TestStorageFailure::from_store)
    }

    pub fn inject_object_payload_first_parity_shard_loss(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<TestObjectPayloadShardFault, TestStorageFailure> {
        inject_object_payload_shard_loss_by_role(
            cluster,
            snapshot,
            TestObjectPayloadShardRole::FirstParity,
        )
        .map_err(TestStorageFailure::from_store)
    }

    pub fn inject_object_payload_first_parity_shard_corruption(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<TestObjectPayloadShardFault, TestStorageFailure> {
        inject_object_payload_shard_corruption_by_role(
            cluster,
            snapshot,
            TestObjectPayloadShardRole::FirstParity,
        )
        .map_err(TestStorageFailure::from_store)
    }

    pub fn inject_object_payload_last_parity_shard_corruption(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<TestObjectPayloadShardFault, TestStorageFailure> {
        inject_object_payload_shard_corruption_by_role(
            cluster,
            snapshot,
            TestObjectPayloadShardRole::LastParity,
        )
        .map_err(TestStorageFailure::from_store)
    }

    /// Reports whether the selected shard still differs from its durable
    /// acknowledgement after the operation under test.
    pub fn object_payload_shard_fault_remains(
        cluster: &StorageCluster,
        fault: &TestObjectPayloadShardFault,
    ) -> Result<bool, TestStorageFailure> {
        cluster
            .test_object_payload_shard_file_matches_ack(
                &fault.snapshot,
                fault.segment_index,
                fault.shard_index,
            )
            .map(|matches| !matches)
            .map_err(TestStorageFailure::from_store)
    }

    pub fn object_payload_shard_faults_remain(
        cluster: &StorageCluster,
        faults: &TestObjectPayloadShardFaultSet,
    ) -> Result<bool, TestStorageFailure> {
        for fault in &faults.faults {
            if !object_payload_shard_fault_remains(cluster, fault)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Reports whether exactly the selected fault is queued for repair and
    /// currently has no recorded worker error.
    pub fn object_payload_shard_fault_has_pending_repair(
        cluster: &StorageCluster,
        fault: &TestObjectPayloadShardFault,
    ) -> Result<bool, TestStorageFailure> {
        let repairs = cluster
            .test_object_payload_repair_observations(&fault.snapshot)
            .map_err(TestStorageFailure::from_store)?;
        Ok(matches!(repairs.as_slice(), [repair]
            if repair.segment_index == fault.segment_index
                && repair.shard_index == fault.shard_index
                && repair.last_error.is_none()))
    }

    /// Reports whether exactly the selected fault is the sole queued repair
    /// and has a recorded worker error. The durable diagnostic text remains
    /// storage-private.
    pub fn object_payload_shard_fault_has_recorded_repair_error(
        cluster: &StorageCluster,
        fault: &TestObjectPayloadShardFault,
    ) -> Result<bool, TestStorageFailure> {
        let repairs = cluster
            .test_object_payload_repair_observations(&fault.snapshot)
            .map_err(TestStorageFailure::from_store)?;
        Ok(matches!(repairs.as_slice(), [repair]
            if repair.segment_index == fault.segment_index
                && repair.shard_index == fault.shard_index
                && repair.last_error.is_some()))
    }

    pub fn object_payload_shard_fault_has_no_pending_repair(
        cluster: &StorageCluster,
        fault: &TestObjectPayloadShardFault,
    ) -> Result<bool, TestStorageFailure> {
        cluster
            .test_object_payload_repair_observations(&fault.snapshot)
            .map(|repairs| repairs.is_empty())
            .map_err(TestStorageFailure::from_store)
    }

    /// Takes the repair wake for the fault's payload and reports whether it
    /// names the exact storage-selected shard.
    pub fn take_object_payload_shard_fault_repair_wake(
        cluster: &StorageCluster,
        fault: &TestObjectPayloadShardFault,
    ) -> Result<bool, TestStorageFailure> {
        cluster
            .test_take_object_payload_repair_wake(
                &fault.snapshot,
                fault.segment_index,
                fault.shard_index,
            )
            .map_err(TestStorageFailure::from_store)
    }

    /// Takes every selected repair wake in reverse selection order. This pins
    /// that an exact later-shard selection cannot consume an earlier shard's
    /// wake for the same payload.
    pub fn take_object_payload_shard_fault_wakes_in_reverse(
        cluster: &StorageCluster,
        faults: &TestObjectPayloadShardFaultSet,
    ) -> Result<bool, TestStorageFailure> {
        for fault in faults.faults.iter().rev() {
            if !take_object_payload_shard_fault_repair_wake(cluster, fault)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Narrow logical lifecycle observation for one live object version.
    ///
    /// The durable generation and payload identity remain private and may be
    /// passed back only to storage-owned lease and reclaim observations.
    #[derive(Clone)]
    pub struct TestLifecycleObjectObservation {
        last_modified: u64,
        became_noncurrent_at: Option<u64>,
        payload: TestObjectPayloadSnapshot,
    }

    /// Opaque storage-owned identity for one live object's payload reclaim.
    ///
    /// Cross-crate tests may pass this identity back to the narrow helpers
    /// below, but cannot select or forge a durable payload generation.
    #[derive(Clone)]
    pub struct TestObjectPayloadReclaimSubject {
        bucket: BucketName,
        key: ObjectKey,
        generation_id: GenerationId,
    }

    impl std::fmt::Debug for TestObjectPayloadReclaimSubject {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("TestObjectPayloadReclaimSubject")
                .field("bucket", &self.bucket)
                .field("key", &self.key)
                .finish_non_exhaustive()
        }
    }

    impl std::fmt::Debug for TestLifecycleObjectObservation {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("TestLifecycleObjectObservation")
                .field("last_modified", &self.last_modified)
                .field("became_noncurrent_at", &self.became_noncurrent_at)
                .finish_non_exhaustive()
        }
    }

    impl TestLifecycleObjectObservation {
        pub fn last_modified(&self) -> u64 {
            self.last_modified
        }

        pub fn became_noncurrent_at(&self) -> Option<u64> {
            self.became_noncurrent_at
        }
    }

    fn object_payload_subject(
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<(&BucketName, &ObjectKey, GenerationId), StoreError> {
        let first = snapshot.segments().first().ok_or_else(|| StoreError::Io {
            context: "select captured object payload subject",
            source: std::io::Error::other("captured object payload has no segments"),
        })?;
        if snapshot
            .segments()
            .iter()
            .any(|segment| segment.bucket != first.bucket || segment.key != first.key)
        {
            return Err(StoreError::Io {
                context: "validate captured object payload subject",
                source: std::io::Error::other(
                    "captured object payload contains multiple object subjects",
                ),
            });
        }
        let generation_id = snapshot.generation_id().ok_or_else(|| StoreError::Io {
            context: "select captured object payload generation",
            source: std::io::Error::other("captured object payload has no generation"),
        })?;
        Ok((&first.bucket, &first.key, generation_id))
    }

    pub fn capture_lifecycle_object_observation(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<TestLifecycleObjectObservation, TestStorageFailure> {
        let stored = cluster
            .test_get_object_version(bucket, key, version_id)
            .map_err(TestStorageFailure::from_object_pg_action)?;
        let live = stored
            .as_live()
            .ok_or_else(|| ObjectPgActionError::InvalidRequest {
                reason: "selected lifecycle object is not a live version".to_string(),
            })
            .map_err(TestStorageFailure::from_object_pg_action)?;
        let payload = cluster
            .test_capture_object_payload(bucket, key, version_id)
            .map_err(TestStorageFailure::from_object_pg_action)?;
        let (_, _, payload_generation) =
            object_payload_subject(&payload).map_err(TestStorageFailure::from_store)?;
        if payload_generation != live.generation_id {
            return Err(TestStorageFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "object version changed while capturing lifecycle payload evidence"
                        .to_string(),
                },
            ));
        }
        Ok(TestLifecycleObjectObservation {
            last_modified: live.last_modified,
            became_noncurrent_at: live.became_noncurrent_at,
            payload,
        })
    }

    pub fn acquire_lifecycle_object_payload_lease(
        cluster: &Arc<StorageCluster>,
        observation: &TestLifecycleObjectObservation,
    ) -> Result<ObjectPayloadLease, TestStorageFailure> {
        cluster
            .test_acquire_object_payload_lease_for_snapshot(&observation.payload)
            .map_err(TestStorageFailure::from_store)
    }

    pub fn lifecycle_object_has_reclaim_root(
        cluster: &StorageCluster,
        observation: &TestLifecycleObjectObservation,
    ) -> Result<bool, TestStorageFailure> {
        let (bucket, key, generation_id) =
            object_payload_subject(&observation.payload).map_err(TestStorageFailure::from_store)?;
        cluster
            .test_get_object_segments_reclaim(bucket, key, generation_id)
            .map(|reclaim| reclaim.is_some())
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    pub fn capture_object_payload_reclaim_subject(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<TestObjectPayloadReclaimSubject, TestStorageFailure> {
        let stored = cluster
            .test_get_object_version(bucket, key, version_id)
            .map_err(TestStorageFailure::from_object_pg_action)?;
        let live = stored
            .as_live()
            .ok_or_else(|| ObjectPgActionError::InvalidRequest {
                reason: "selected reclaim subject is not a live object version".to_string(),
            })
            .map_err(TestStorageFailure::from_object_pg_action)?;
        Ok(TestObjectPayloadReclaimSubject {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id: live.generation_id,
        })
    }

    fn prepare_object_payload_reclaim_subject_without_root(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<TestObjectPayloadReclaimSubject, TestStorageFailure> {
        let generation_id = cluster
            .test_next_unreferenced_object_generation(bucket, key)
            .map_err(TestStorageFailure::from_object_pg_action)?;
        let subject = TestObjectPayloadReclaimSubject {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
        };
        if object_payload_has_reclaim_root(cluster, &subject)? {
            return Err(TestStorageFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "selected no-root reclaim subject already has durable reclaim metadata"
                        .to_string(),
                },
            ));
        }
        Ok(subject)
    }

    /// Seeds one canonical storage-owned segmented reclaim root and returns
    /// only its opaque logical subject to the caller.
    pub fn seed_segmented_object_payload_reclaim(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        created_at: u64,
    ) -> Result<TestObjectPayloadReclaimSubject, TestStorageFailure> {
        let subject = prepare_object_payload_reclaim_subject_without_root(cluster, bucket, key)?;
        cluster
            .test_seed_segmented_payload_reclaim(bucket, key, subject.generation_id, created_at)
            .map_err(TestStorageFailure::from_object_pg_action)?;
        Ok(subject)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn acquire_object_payload_reclaim_lease(
        cluster: &Arc<StorageCluster>,
        subject: &TestObjectPayloadReclaimSubject,
    ) -> Result<ObjectPayloadLease, TestStorageFailure> {
        cluster
            .acquire_object_payload_lease(&subject.bucket, &subject.key, subject.generation_id)
            .map_err(TestStorageFailure::from_store)
    }

    pub fn object_payload_has_reclaim_root(
        cluster: &StorageCluster,
        subject: &TestObjectPayloadReclaimSubject,
    ) -> Result<bool, TestStorageFailure> {
        cluster
            .test_payload_reclaim_exists(&subject.bucket, &subject.key, subject.generation_id)
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    pub fn object_payload_reclaim_is_active(
        cluster: &StorageCluster,
        subject: &TestObjectPayloadReclaimSubject,
    ) -> bool {
        cluster.test_object_payload_reclaim_is_active(
            &subject.bucket,
            &subject.key,
            subject.generation_id,
        )
    }

    pub fn object_payload_reclaim_root_count_for(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<usize, TestStorageFailure> {
        cluster
            .test_payload_reclaim_count_for_object(bucket, key)
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    pub fn stream_upload_session_count_for_object(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<usize, TestStorageFailure> {
        cluster
            .test_list_all_stream_uploads()
            .map(|sessions| {
                sessions
                    .into_iter()
                    .filter(|session| session.bucket == *bucket && session.key == *key)
                    .count()
            })
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    /// Returns the logical session identities for one object without exposing
    /// durable stream-upload records or their physical placement.
    pub fn stream_upload_session_ids_for_object(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<SessionId>, TestStorageFailure> {
        cluster
            .test_list_all_stream_uploads()
            .map(|sessions| {
                let mut session_ids = sessions
                    .into_iter()
                    .filter(|session| session.bucket == *bucket && session.key == *key)
                    .map(|session| session.session_id)
                    .collect::<Vec<_>>();
                session_ids.sort();
                session_ids
            })
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    pub fn stream_upload_session_exists(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<bool, TestStorageFailure> {
        cluster
            .test_list_all_stream_uploads()
            .map(|sessions| {
                sessions.into_iter().any(|session| {
                    session.bucket == *bucket
                        && session.key == *key
                        && session.session_id == *session_id
                })
            })
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    /// Returns the durable cleanup deadline for one exact stream session.
    ///
    /// `None` covers both an absent session and a session deliberately created
    /// without a deadline. Callers which need to distinguish those states must
    /// first use [`stream_upload_session_exists`].
    pub fn stream_upload_session_cleanup_after(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<Option<u64>, TestStorageFailure> {
        cluster
            .test_list_all_stream_uploads()
            .map(|sessions| {
                sessions
                    .into_iter()
                    .find(|session| {
                        session.bucket == *bucket
                            && session.key == *key
                            && session.session_id == *session_id
                    })
                    .and_then(|session| session.cleanup_after)
            })
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    /// Returns the logical number of durable stream-upload sessions without
    /// exposing their storage-owned records to downstream crates.
    pub fn stream_upload_session_count(
        cluster: &StorageCluster,
    ) -> Result<usize, TestStorageFailure> {
        cluster
            .test_list_all_stream_uploads()
            .map(|sessions| sessions.len())
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    /// Runs one storage-owned abandoned-session cleanup pass and returns the
    /// number of sessions removed.
    ///
    /// This is a deterministic test-runtime lifecycle operation. Callers do
    /// not select PGs, inspect durable session records, or bypass the
    /// production cleanup implementation.
    #[must_use]
    pub fn sweep_abandoned_stream_upload_sessions(
        cluster: &StorageCluster,
        max_age_ms: u64,
    ) -> usize {
        cluster.test_scavenge_abandoned_stream_sessions(max_age_ms)
    }

    /// Returns the number of sessions for one exact UploadPart target.
    ///
    /// Storage retains ownership of the durable session record and target
    /// representation; callers supply only the S3-visible upload identity.
    pub fn upload_part_stream_session_count(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<usize, TestStorageFailure> {
        cluster
            .test_list_all_stream_uploads()
            .map(|sessions| {
                sessions
                    .into_iter()
                    .filter(|session| {
                        session.bucket == *bucket
                            && session.key == *key
                            && matches!(
                                &session.target,
                                StreamUploadTarget::UploadPart {
                                    upload_id: target_upload_id,
                                    part_number: target_part_number,
                                } if target_upload_id == upload_id
                                    && *target_part_number == part_number
                            )
                    })
                    .count()
            })
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    pub fn multipart_upload_count_for_object(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<usize, TestStorageFailure> {
        cluster
            .test_list_multipart_uploads_for_bucket(bucket)
            .map(|uploads| {
                uploads
                    .into_iter()
                    .filter(|upload| upload.key == *key)
                    .count()
            })
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    pub fn multipart_upload_count_for_bucket(
        cluster: &StorageCluster,
        bucket: &BucketName,
    ) -> Result<usize, TestStorageFailure> {
        cluster
            .test_list_multipart_uploads_for_bucket(bucket)
            .map(|uploads| uploads.len())
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    pub fn multipart_upload_ids_for_object(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<UploadId>, TestStorageFailure> {
        cluster
            .test_list_multipart_uploads_for_bucket(bucket)
            .map(|uploads| {
                uploads
                    .into_iter()
                    .filter(|upload| upload.key == *key)
                    .map(|upload| upload.upload_id)
                    .collect()
            })
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    pub fn multipart_upload_exists(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<bool, TestStorageFailure> {
        match cluster.test_get_multipart_upload(bucket, key, upload_id) {
            Ok(_) => Ok(true),
            Err(ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { .. })) => Ok(false),
            Err(error) => Err(TestStorageFailure::from_object_pg_action(error)),
        }
    }

    pub fn multipart_upload_initiated_at(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<u64, TestStorageFailure> {
        cluster
            .test_get_multipart_upload(bucket, key, upload_id)
            .map(|upload| upload.initiated_at)
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    pub fn multipart_upload_has_owners(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_initiator: &OwnerIdentity,
        expected_owner: &OwnerIdentity,
    ) -> Result<bool, TestStorageFailure> {
        cluster
            .test_get_multipart_upload(bucket, key, upload_id)
            .map(|upload| {
                upload.initiator == *expected_initiator && upload.owner == *expected_owner
            })
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    pub fn multipart_upload_matches_creation_metadata(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_tags: Option<&s3_types::TagSet>,
        expected_metadata: &SerializedMetadataBlob,
        expected_system_metadata: &SerializedSystemMetadataBlob,
    ) -> Result<bool, TestStorageFailure> {
        cluster
            .test_get_multipart_upload(bucket, key, upload_id)
            .map(|upload| {
                upload.tags.as_ref().map(SerializedTagSet::tag_set) == expected_tags
                    && upload.metadata_blob == *expected_metadata
                    && upload.system_metadata_blob == *expected_system_metadata
            })
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    /// Opaque storage-owned identity for the generation reserved by one
    /// multipart upload before it is completed and its durable upload row is
    /// removed.
    #[derive(Clone)]
    pub struct TestMultipartUploadGenerationSubject {
        bucket: BucketName,
        key: ObjectKey,
        generation_id: GenerationId,
    }

    impl std::fmt::Debug for TestMultipartUploadGenerationSubject {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("TestMultipartUploadGenerationSubject")
                .field("bucket", &self.bucket)
                .field("key", &self.key)
                .finish_non_exhaustive()
        }
    }

    pub fn capture_multipart_upload_generation_subject(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<TestMultipartUploadGenerationSubject, TestStorageFailure> {
        cluster
            .test_get_multipart_upload(bucket, key, upload_id)
            .map(|upload| TestMultipartUploadGenerationSubject {
                bucket: upload.bucket,
                key: upload.key,
                generation_id: upload.object_generation_id,
            })
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    pub fn completed_object_uses_multipart_upload_generation(
        cluster: &StorageCluster,
        subject: &TestMultipartUploadGenerationSubject,
        version_id: VersionId,
    ) -> Result<bool, TestStorageFailure> {
        let object = cluster
            .test_get_object_version(&subject.bucket, &subject.key, version_id)
            .map_err(TestStorageFailure::from_object_pg_action)?;
        Ok(object
            .as_live()
            .is_some_and(|live| live.generation_id == subject.generation_id))
    }

    pub(crate) fn multipart_upload_state_raw_for_owner_test(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<UploadState, ObjectPgActionError> {
        let upload = cluster.test_get_multipart_upload(bucket, key, upload_id)?;
        if upload.bucket != *bucket || upload.key != *key {
            return Err(ObjectPgActionError::Store(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "observe multipart upload state",
                },
            ));
        }
        Ok(upload.state)
    }

    pub fn multipart_upload_state(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<UploadState, TestStorageFailure> {
        multipart_upload_state_raw_for_owner_test(cluster, bucket, key, upload_id)
            .map_err(TestStorageFailure::from_object_pg_action)
    }

    pub fn enqueue_object_payload_reclaim(
        cluster: &StorageCluster,
        subject: &TestObjectPayloadReclaimSubject,
    ) {
        cluster.enqueue_object_payload_reclaim(
            &subject.bucket,
            &subject.key,
            subject.generation_id,
        );
    }

    pub fn reclaim_object_payload_if_unleased(
        cluster: &StorageCluster,
        subject: &TestObjectPayloadReclaimSubject,
    ) -> Result<bool, StoreFailure> {
        cluster
            .test_reclaim_object_payload_if_unleased(
                &subject.bucket,
                &subject.key,
                subject.generation_id,
            )
            .map_err(|error| match error {
                ObjectPgActionError::Store(error) => StoreFailure::from(error),
                ObjectPgActionError::Metadata(error) => StoreFailure::from_metadata(error),
                ObjectPgActionError::InvalidRequest { .. }
                | ObjectPgActionError::StaleObjectReadSubject
                | ObjectPgActionError::StaleDirectPutCommitSnapshot
                | ObjectPgActionError::StaleStreamFinalizeSnapshot
                | ObjectPgActionError::StaleMultipartCompletionSnapshot
                | ObjectPgActionError::MultipartConditionalRequestConflict => {
                    StoreFailure::from(StoreError::RouteCapabilitySubjectMismatch {
                        operation: "observe payload reclaim test outcome",
                    })
                }
            })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Clone, PartialEq, Eq)]
    pub struct TestBucketDeleteFinalizeRoot {
        bucket: BucketName,
        bucket_incarnation_generation: u64,
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl fmt::Debug for TestBucketDeleteFinalizeRoot {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("TestBucketDeleteFinalizeRoot")
                .field("subject", &"[redacted]")
                .finish()
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl TestBucketDeleteFinalizeRoot {
        pub(crate) fn from_root(root: BucketDeleteFinalizeRoot) -> Self {
            Self {
                bucket: root.bucket,
                bucket_incarnation_generation: root.bucket_incarnation_generation,
            }
        }

        pub(crate) fn to_root(&self) -> BucketDeleteFinalizeRoot {
            BucketDeleteFinalizeRoot {
                bucket: self.bucket.clone(),
                bucket_incarnation_generation: self.bucket_incarnation_generation,
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

    /// Narrow observation of SSE-C checksum persistence for one committed
    /// object version.
    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TestStoredSseCustomerChecksumObservation {
        pub has_encrypted_checksum: bool,
        pub contains_supplied_cleartext: bool,
    }

    #[cfg(any(test, feature = "test-hooks"))]
    impl TestMultipartPartObservation {
        pub(crate) fn from_record(part: types::MultipartPartRecord) -> Self {
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
        generation_id: Option<GenerationId>,
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
            generation_id: Option<GenerationId>,
        ) -> Self {
            Self {
                segments,
                stored_size_extra,
                generation_id,
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

        pub(crate) fn generation_id(&self) -> Option<GenerationId> {
            self.generation_id
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
    pub(crate) struct TestObjectPayloadRepairObservation {
        pub(crate) segment_index: u32,
        pub(crate) shard_index: u8,
        pub(crate) last_error: Option<String>,
    }

    /// Test-only logical observation of accepted bucket-deletion progress.
    #[cfg(any(test, feature = "test-hooks"))]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TestBucketDeleteProgress {
        pub bucket_state: Option<BucketState>,
        pub has_durable_write_drain: bool,
        pub has_pending_metadata_command: bool,
    }

    /// Logical visibility of one bucket while exercising deletion behavior.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum TestBucketPresence {
        Missing,
        Active,
        Deleting,
    }

    /// Opaque storage-issued identity for one bucket-delete-begin attempt.
    #[derive(Clone)]
    pub struct TestBucketDeleteBeginSubject {
        root: BucketDeleteBeginRoot,
    }

    impl std::fmt::Debug for TestBucketDeleteBeginSubject {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("TestBucketDeleteBeginSubject")
                .field("bucket", &self.root.bucket)
                .finish_non_exhaustive()
        }
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
    PgClusterMapHistoryRouteReferenceKind, PgClusterMapHistoryRouteReferenceLimitError,
    PgClusterMapHistoryRouteReferences, MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES,
    MAX_PG_DURABLE_IDENTITY_BYTES,
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
    TestObjectPayloadRepairObservation, TestObjectPayloadSnapshot,
    TestStoredSseCustomerChecksumObservation, TestStreamUploadPayloadSnapshot,
};
#[cfg(test)]
pub(crate) use traits::PgMetadataStore;
#[cfg(test)]
pub(crate) use types::BucketSubresourceAux;
#[cfg(test)]
pub(crate) use types::DirectPutWrittenSegment;
pub(crate) use types::MultipartUploadIdKey;
#[cfg(test)]
pub(crate) use types::SegmentStoredBytesRequest;
#[cfg(test)]
pub(crate) use types::StreamUploadCommandRecord;
pub(crate) use types::TerminalStreamCleanupRecord;
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
    LoadedBucketSubresource, ManagedEncryptionAlgorithm, MetadataCheckpointDiagnostic,
    MetadataCheckpointDiagnosticOutcome, MultipartChecksumConfig, MultipartCompletionFingerprint,
    MultipartCompletionPart, MultipartCompletionReplayCandidate, MultipartLifecycleUpload,
    MultipartUploadAbortCandidate, MultipartUploadAbortLookup,
    MultipartUploadAuthorizationIdentity, MultipartUploadCompletionCandidate,
    MultipartUploadCompletionContext, MultipartUploadCompletionLookup, MultipartUploadIdAuthority,
    MultipartUploadListMarker, MultipartUploadListPartsCandidate, MultipartUploadListPartsLookup,
    MultipartUploadPartCandidate, ObjectEncryption, ObjectEncryptionStateError, ObjectEtag,
    ObjectKey, ObjectKeyError, ObjectLayout, ObjectLockDefaultRetention, ObjectLockMode,
    ObjectLockState, ObjectPayloadPlacementDiagnostic, ObjectPayloadPlacementDiagnosticOutcome,
    ObjectPayloadSegment, ObjectReadAuthSubject, ObjectReadAuthSubjectIdentity,
    ObjectReadMultipartPart, ObjectReadSnapshot, ObjectReadSnapshotMode, ObjectReadSnapshotOutcome,
    ObjectRetention, ObjectSegmentRecord, ObjectState, OpaqueBucketSubresourceKind, OwnerIdentity,
    PgId, PgState, PrepareStreamUploadSegmentAppendReq, PreparedDirectPutObjectCommit,
    PreparedStreamPartCommit, PreparedStreamPutCommit, PublicAccessBlockConfig,
    PutBucketSubresource, PutDeleteMarkerReq, PutLiveObjectReq, PutLiveObjectValidationError,
    PutObjectReq, RawChecksum, RetentionPeriod, RouteMapValidUntilMs, RouteMapValidity,
    SerializedBucketTagSet, SerializedMetadataBlob, SerializedSystemMetadataBlob, SerializedTagSet,
    SessionId, SessionIdError, ShardData, ShardIndex, ShardKey, ShardScavengerObservation,
    ShardScavengerObservationKey, ShardScavengerObservationReason, ShardScavengerObservationRecord,
    ShardStat, ShardStatus, SseCustomerObjectState, SseS3ObjectState, StorageClass,
    StoredLegalHoldStatus, StoredObject, StreamPartFinalizeInput, StreamPartFinalizeSnapshot,
    StreamPutCommitInput, StreamPutFinalizeSnapshot, StreamPutFinalizeStorageSnapshot,
    StreamSegmentAppendInput, StreamSegmentAppendOutcome, StreamUploadKind, StreamUploadRecord,
    StreamUploadRecordPage, StreamUploadSegmentRecord, StreamUploadState, StreamUploadTarget,
    UploadId, UploadIdError, UploadState, VersionId, WriteAck, WrittenShardAck,
    MULTIPART_PART_SEGMENT_STAGING_VERSION_ID, OBJECT_ENCRYPTION_CHECKSUM_NONCE_LEN,
    OBJECT_ENCRYPTION_SEGMENT_NONCE_PREFIX_LEN, OBJECT_ENCRYPTION_SEGMENT_NONCE_SCOPE_LEN,
    OBJECT_ENCRYPTION_SEGMENT_TAG_LEN, OBJECT_ENCRYPTION_WRAPPED_DEK_LEN,
    OBJECT_ENCRYPTION_WRAP_NONCE_LEN, SESSION_ID_LEN, SHARD_KEY_HEX_LEN, SHARD_KEY_HEX_PREFIX_LEN,
    SHARD_KEY_LEN, SSE_C_CHECKSUM_NONCE_LEN, SSE_C_SEGMENT_NONCE_PREFIX_LEN,
    SSE_C_SEGMENT_NONCE_SCOPE_LEN, SSE_C_VALIDATOR_HMAC_LEN, SSE_C_VALIDATOR_SALT_LEN,
    SSE_C_WRAPPED_DEK_LEN, SSE_C_WRAP_NONCE_LEN, SSE_C_WRAP_SALT_LEN, SSE_S3_CHECKSUM_NONCE_LEN,
    SSE_S3_SEGMENT_NONCE_PREFIX_LEN, SSE_S3_WRAPPED_DEK_LEN, SSE_S3_WRAP_NONCE_LEN,
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
