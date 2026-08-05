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
    use std::sync::Arc;

    use super::*;

    /// Curated opaque payload observations for cross-crate tests.
    ///
    /// The underlying storage operations remain crate-private. Importing this
    /// trait makes only storage-owned opaque snapshots and logical predicates
    /// available to downstream test code.
    pub trait StorageClusterPayloadTestSupport {
        fn test_capture_object_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
        ) -> Result<TestObjectPayloadSnapshot, ObjectPgActionError>;

        fn test_object_payload_snapshot_is_fully_present(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, StoreError>;

        fn test_object_payload_snapshot_is_fully_absent(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, StoreError>;

        fn test_object_payload_snapshot_places_each_shard_on_a_distinct_node(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, StoreError>;

        fn test_object_payload_snapshot_uses_generation_layout(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
            generation_id: GenerationId,
        ) -> Result<bool, StoreError>;

        fn test_object_payload_snapshot_uses_transient_direct_put_layout(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
            generation_id: GenerationId,
        ) -> Result<bool, StoreError>;

        fn test_capture_multipart_upload_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<TestMultipartPartPayloadSnapshot, ObjectPgActionError>;

        fn test_capture_multipart_part_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
            part_number: u32,
        ) -> Result<TestMultipartPartPayloadSnapshot, ObjectPgActionError>;

        fn test_multipart_part_payload_snapshot_is_fully_present(
            &self,
            snapshot: &TestMultipartPartPayloadSnapshot,
        ) -> Result<bool, StoreError>;

        fn test_multipart_part_payload_snapshot_is_fully_absent(
            &self,
            snapshot: &TestMultipartPartPayloadSnapshot,
        ) -> Result<bool, StoreError>;

        fn test_capture_stream_upload_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            session_id: &SessionId,
        ) -> Result<TestStreamUploadPayloadSnapshot, ObjectPgActionError>;

        fn test_stream_upload_payload_snapshot_is_fully_present(
            &self,
            snapshot: &TestStreamUploadPayloadSnapshot,
        ) -> Result<bool, StoreError>;

        fn test_stream_upload_payload_snapshot_is_fully_absent(
            &self,
            snapshot: &TestStreamUploadPayloadSnapshot,
        ) -> Result<bool, StoreError>;
    }

    impl StorageClusterPayloadTestSupport for StorageCluster {
        fn test_capture_object_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            version_id: VersionId,
        ) -> Result<TestObjectPayloadSnapshot, ObjectPgActionError> {
            StorageCluster::test_capture_object_payload(self, bucket, key, version_id)
        }

        fn test_object_payload_snapshot_is_fully_present(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, StoreError> {
            StorageCluster::test_object_payload_snapshot_is_fully_present(self, snapshot)
        }

        fn test_object_payload_snapshot_is_fully_absent(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, StoreError> {
            StorageCluster::test_object_payload_snapshot_is_fully_absent(self, snapshot)
        }

        fn test_object_payload_snapshot_places_each_shard_on_a_distinct_node(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
        ) -> Result<bool, StoreError> {
            StorageCluster::test_object_payload_snapshot_places_each_shard_on_a_distinct_node(
                self, snapshot,
            )
        }

        fn test_object_payload_snapshot_uses_generation_layout(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
            generation_id: GenerationId,
        ) -> Result<bool, StoreError> {
            StorageCluster::test_object_payload_snapshot_uses_generation_layout(
                self,
                snapshot,
                generation_id,
            )
        }

        fn test_object_payload_snapshot_uses_transient_direct_put_layout(
            &self,
            snapshot: &TestObjectPayloadSnapshot,
            generation_id: GenerationId,
        ) -> Result<bool, StoreError> {
            StorageCluster::test_object_payload_snapshot_uses_transient_direct_put_layout(
                self,
                snapshot,
                generation_id,
            )
        }

        fn test_capture_multipart_upload_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<TestMultipartPartPayloadSnapshot, ObjectPgActionError> {
            StorageCluster::test_capture_multipart_upload_payload(self, bucket, key, upload_id)
        }

        fn test_capture_multipart_part_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
            part_number: u32,
        ) -> Result<TestMultipartPartPayloadSnapshot, ObjectPgActionError> {
            StorageCluster::test_capture_multipart_part_payload(
                self,
                bucket,
                key,
                upload_id,
                part_number,
            )
        }

        fn test_multipart_part_payload_snapshot_is_fully_present(
            &self,
            snapshot: &TestMultipartPartPayloadSnapshot,
        ) -> Result<bool, StoreError> {
            StorageCluster::test_multipart_part_payload_snapshot_is_fully_present(self, snapshot)
        }

        fn test_multipart_part_payload_snapshot_is_fully_absent(
            &self,
            snapshot: &TestMultipartPartPayloadSnapshot,
        ) -> Result<bool, StoreError> {
            StorageCluster::test_multipart_part_payload_snapshot_is_fully_absent(self, snapshot)
        }

        fn test_capture_stream_upload_payload(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            session_id: &SessionId,
        ) -> Result<TestStreamUploadPayloadSnapshot, ObjectPgActionError> {
            StorageCluster::test_capture_stream_upload_payload(self, bucket, key, session_id)
        }

        fn test_stream_upload_payload_snapshot_is_fully_present(
            &self,
            snapshot: &TestStreamUploadPayloadSnapshot,
        ) -> Result<bool, StoreError> {
            StorageCluster::test_stream_upload_payload_snapshot_is_fully_present(self, snapshot)
        }

        fn test_stream_upload_payload_snapshot_is_fully_absent(
            &self,
            snapshot: &TestStreamUploadPayloadSnapshot,
        ) -> Result<bool, StoreError> {
            StorageCluster::test_stream_upload_payload_snapshot_is_fully_absent(self, snapshot)
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

    /// Logical worker observations and storage-owned lifecycle setup for
    /// cross-crate coordinator tests.
    ///
    /// These operations deliberately avoid exposing durable rows, queue
    /// entries, or timestamps. Storage owns the physical setup and projects
    /// only the worker state or semantic transition required by the caller.
    pub trait StorageClusterLifecycleTestSupport {
        fn test_bucket_delete_finalize_outstanding_depth(&self) -> usize;

        fn test_object_payload_reclaim_outstanding_depth(&self) -> usize;

        fn test_seed_stale_lifecycle_sweep_claim(
            &self,
            bucket: &BucketName,
        ) -> Result<(), ObjectPgActionError>;

        fn test_begin_durable_bucket_delete_drain(
            &self,
            bucket: &BucketName,
        ) -> Result<(), BucketWriteDrainError>;

        fn test_mark_multipart_upload_aborting(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<(), ObjectPgActionError>;

        fn test_mark_multipart_upload_completing(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<(), ObjectPgActionError>;

        fn test_mark_stream_upload_stale(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            session_id: &SessionId,
        ) -> Result<(), ObjectPgActionError>;
    }

    impl StorageClusterLifecycleTestSupport for StorageCluster {
        fn test_bucket_delete_finalize_outstanding_depth(&self) -> usize {
            StorageCluster::test_bucket_delete_finalize_outstanding_depth(self)
        }

        fn test_object_payload_reclaim_outstanding_depth(&self) -> usize {
            StorageCluster::test_object_payload_reclaim_outstanding_depth(self)
        }

        fn test_seed_stale_lifecycle_sweep_claim(
            &self,
            bucket: &BucketName,
        ) -> Result<(), ObjectPgActionError> {
            StorageCluster::test_seed_stale_lifecycle_sweep_claim(self, bucket)
        }

        fn test_begin_durable_bucket_delete_drain(
            &self,
            bucket: &BucketName,
        ) -> Result<(), BucketWriteDrainError> {
            StorageCluster::test_begin_durable_bucket_delete_drain(self, bucket)
        }

        fn test_mark_multipart_upload_aborting(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<(), ObjectPgActionError> {
            StorageCluster::test_mark_multipart_upload_aborting(self, bucket, key, upload_id)
        }

        fn test_mark_multipart_upload_completing(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            upload_id: &UploadId,
        ) -> Result<(), ObjectPgActionError> {
            StorageCluster::test_mark_multipart_upload_completing(self, bucket, key, upload_id)
        }

        fn test_mark_stream_upload_stale(
            &self,
            bucket: &BucketName,
            key: &ObjectKey,
            session_id: &SessionId,
        ) -> Result<(), ObjectPgActionError> {
            StorageCluster::test_mark_stream_upload_stale(self, bucket, key, session_id)
        }
    }

    mod retained_read;
    pub use retained_read::{TestRetainedReadPgMoveScenario, TestRetainedReadPgMoveScenarioError};

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
        MetadataCommandApplyTestContext, MetadataCommandApplyTestKind,
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

    /// Reports whether the exact shard-owner set selected by an opaque payload
    /// snapshot currently holds deletion-exclusion leases for its generation.
    pub fn object_payload_snapshot_has_exact_shard_owner_leases(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        cluster.test_object_payload_snapshot_has_exact_shard_owner_leases(snapshot)
    }

    /// Reports whether an opaque payload snapshot has no deletion-exclusion
    /// leases remaining on any storage node.
    pub fn object_payload_snapshot_has_no_leases(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<bool, StoreError> {
        cluster.test_object_payload_snapshot_has_no_leases(snapshot)
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
    ) -> Result<TestObjectPayloadShardFault, StoreError> {
        inject_object_payload_shard_loss_by_role(
            cluster,
            snapshot,
            TestObjectPayloadShardRole::FirstData,
        )
    }

    /// Removes the requested number of data shards from storage's first
    /// payload segment without exposing their physical indices.
    pub fn inject_object_payload_data_shard_losses(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
        count: usize,
    ) -> Result<TestObjectPayloadShardFaultSet, StoreError> {
        let segment = snapshot.segments().first().ok_or_else(|| StoreError::Io {
            context: "select object payload data-shard losses",
            source: std::io::Error::other("captured object payload has no segments"),
        })?;
        if count > usize::from(segment.ec_k) {
            return Err(StoreError::Io {
                context: "select object payload data-shard losses",
                source: std::io::Error::other(format!(
                    "requested {count} data-shard losses from an EC layout with {} data shards",
                    segment.ec_k
                )),
            });
        }
        let mut faults = Vec::with_capacity(count);
        for shard_index in 0..u8::try_from(count).map_err(|_| StoreError::Io {
            context: "select object payload data-shard losses",
            source: std::io::Error::other("requested data-shard loss count does not fit u8"),
        })? {
            let fault = TestObjectPayloadShardFault {
                snapshot: snapshot.clone(),
                segment_index: segment.segment_index,
                shard_index,
            };
            cluster.test_inject_object_payload_shard_loss(
                &fault.snapshot,
                fault.segment_index,
                fault.shard_index,
            )?;
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
    ) -> Result<TestObjectPayloadShardFaultSet, StoreError> {
        let segment = snapshot.segments().first().ok_or_else(|| StoreError::Io {
            context: "select object payload data-shard repair wakes",
            source: std::io::Error::other("captured object payload has no segments"),
        })?;
        if count > usize::from(segment.ec_k) {
            return Err(StoreError::Io {
                context: "select object payload data-shard repair wakes",
                source: std::io::Error::other(format!(
                    "requested {count} data-shard repair wakes from an EC layout with {} data shards",
                    segment.ec_k
                )),
            });
        }
        let mut faults = Vec::with_capacity(count);
        for shard_index in 0..u8::try_from(count).map_err(|_| StoreError::Io {
            context: "select object payload data-shard repair wakes",
            source: std::io::Error::other("requested data-shard repair count does not fit u8"),
        })? {
            let fault = TestObjectPayloadShardFault {
                snapshot: snapshot.clone(),
                segment_index: segment.segment_index,
                shard_index,
            };
            cluster.test_schedule_object_payload_repair_wake(
                &fault.snapshot,
                fault.segment_index,
                fault.shard_index,
            )?;
            faults.push(fault);
        }
        Ok(TestObjectPayloadShardFaultSet { faults })
    }

    /// Removes enough additional data shards to make repair of an already
    /// faulted first data shard exceed the payload's parity tolerance.
    pub fn inject_object_payload_additional_data_losses_beyond_parity(
        cluster: &StorageCluster,
        fault: &TestObjectPayloadShardFault,
    ) -> Result<TestObjectPayloadShardFaultSet, StoreError> {
        let segment = fault
            .snapshot
            .segments()
            .iter()
            .find(|segment| segment.segment_index == fault.segment_index)
            .ok_or_else(|| StoreError::Io {
                context: "select additional object payload data-shard losses",
                source: std::io::Error::other("fault segment is absent from its payload snapshot"),
            })?;
        if fault.shard_index != 0 || segment.ec_m >= segment.ec_k {
            return Err(StoreError::Io {
                context: "select additional object payload data-shard losses",
                source: std::io::Error::other(
                    "first-data fault and at least one surviving data shard are required",
                ),
            });
        }
        let mut faults = Vec::with_capacity(usize::from(segment.ec_m));
        for shard_index in 1..=segment.ec_m {
            let additional = TestObjectPayloadShardFault {
                snapshot: fault.snapshot.clone(),
                segment_index: fault.segment_index,
                shard_index,
            };
            cluster.test_inject_object_payload_shard_loss(
                &additional.snapshot,
                additional.segment_index,
                additional.shard_index,
            )?;
            faults.push(additional);
        }
        Ok(TestObjectPayloadShardFaultSet { faults })
    }

    pub fn inject_object_payload_first_data_shard_corruption(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<TestObjectPayloadShardFault, StoreError> {
        inject_object_payload_shard_corruption_by_role(
            cluster,
            snapshot,
            TestObjectPayloadShardRole::FirstData,
        )
    }

    pub fn inject_object_payload_first_parity_shard_loss(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<TestObjectPayloadShardFault, StoreError> {
        inject_object_payload_shard_loss_by_role(
            cluster,
            snapshot,
            TestObjectPayloadShardRole::FirstParity,
        )
    }

    pub fn inject_object_payload_first_parity_shard_corruption(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<TestObjectPayloadShardFault, StoreError> {
        inject_object_payload_shard_corruption_by_role(
            cluster,
            snapshot,
            TestObjectPayloadShardRole::FirstParity,
        )
    }

    pub fn inject_object_payload_last_parity_shard_corruption(
        cluster: &StorageCluster,
        snapshot: &TestObjectPayloadSnapshot,
    ) -> Result<TestObjectPayloadShardFault, StoreError> {
        inject_object_payload_shard_corruption_by_role(
            cluster,
            snapshot,
            TestObjectPayloadShardRole::LastParity,
        )
    }

    /// Reports whether the selected shard still differs from its durable
    /// acknowledgement after the operation under test.
    pub fn object_payload_shard_fault_remains(
        cluster: &StorageCluster,
        fault: &TestObjectPayloadShardFault,
    ) -> Result<bool, StoreError> {
        cluster
            .test_object_payload_shard_file_matches_ack(
                &fault.snapshot,
                fault.segment_index,
                fault.shard_index,
            )
            .map(|matches| !matches)
    }

    pub fn object_payload_shard_faults_remain(
        cluster: &StorageCluster,
        faults: &TestObjectPayloadShardFaultSet,
    ) -> Result<bool, StoreError> {
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
    ) -> Result<bool, StoreError> {
        let repairs = cluster.test_object_payload_repair_observations(&fault.snapshot)?;
        Ok(matches!(repairs.as_slice(), [repair]
            if repair.segment_index == fault.segment_index
                && repair.shard_index == fault.shard_index
                && repair.last_error.is_none()))
    }

    /// Returns the worker error for the selected fault, if that exact fault is
    /// the sole queued repair.
    pub fn object_payload_shard_fault_repair_error(
        cluster: &StorageCluster,
        fault: &TestObjectPayloadShardFault,
    ) -> Result<Option<String>, StoreError> {
        let repairs = cluster.test_object_payload_repair_observations(&fault.snapshot)?;
        Ok(match repairs.as_slice() {
            [repair]
                if repair.segment_index == fault.segment_index
                    && repair.shard_index == fault.shard_index =>
            {
                repair.last_error.clone()
            }
            _ => None,
        })
    }

    pub fn object_payload_shard_fault_has_no_pending_repair(
        cluster: &StorageCluster,
        fault: &TestObjectPayloadShardFault,
    ) -> Result<bool, StoreError> {
        cluster
            .test_object_payload_repair_observations(&fault.snapshot)
            .map(|repairs| repairs.is_empty())
    }

    /// Takes the repair wake for the fault's payload and reports whether it
    /// names the exact storage-selected shard.
    pub fn take_object_payload_shard_fault_repair_wake(
        cluster: &StorageCluster,
        fault: &TestObjectPayloadShardFault,
    ) -> Result<bool, StoreError> {
        cluster.test_take_object_payload_repair_wake(
            &fault.snapshot,
            fault.segment_index,
            fault.shard_index,
        )
    }

    /// Takes every selected repair wake in reverse selection order. This pins
    /// that an exact later-shard selection cannot consume an earlier shard's
    /// wake for the same payload.
    pub fn take_object_payload_shard_fault_wakes_in_reverse(
        cluster: &StorageCluster,
        faults: &TestObjectPayloadShardFaultSet,
    ) -> Result<bool, StoreError> {
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
    ) -> Result<TestLifecycleObjectObservation, ObjectPgActionError> {
        let stored = cluster.test_get_object_version(bucket, key, version_id)?;
        let live = stored
            .as_live()
            .ok_or_else(|| ObjectPgActionError::InvalidRequest {
                reason: "selected lifecycle object is not a live version".to_string(),
            })?;
        let payload = cluster.test_capture_object_payload(bucket, key, version_id)?;
        let (_, _, payload_generation) =
            object_payload_subject(&payload).map_err(ObjectPgActionError::Store)?;
        if payload_generation != live.generation_id {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "object version changed while capturing lifecycle payload evidence"
                    .to_string(),
            });
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
    ) -> Result<ObjectPayloadLease, ObjectPgActionError> {
        cluster
            .test_acquire_object_payload_lease_for_snapshot(&observation.payload)
            .map_err(ObjectPgActionError::Store)
    }

    pub fn lifecycle_object_has_reclaim_root(
        cluster: &StorageCluster,
        observation: &TestLifecycleObjectObservation,
    ) -> Result<bool, ObjectPgActionError> {
        let (bucket, key, generation_id) =
            object_payload_subject(&observation.payload).map_err(ObjectPgActionError::Store)?;
        cluster
            .test_get_object_segments_reclaim(bucket, key, generation_id)
            .map(|reclaim| reclaim.is_some())
    }

    pub fn capture_object_payload_reclaim_subject(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<TestObjectPayloadReclaimSubject, ObjectPgActionError> {
        let stored = cluster.test_get_object_version(bucket, key, version_id)?;
        let live = stored
            .as_live()
            .ok_or_else(|| ObjectPgActionError::InvalidRequest {
                reason: "selected reclaim subject is not a live object version".to_string(),
            })?;
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
    ) -> Result<TestObjectPayloadReclaimSubject, ObjectPgActionError> {
        let generation_id = cluster.test_next_unreferenced_object_generation(bucket, key)?;
        let subject = TestObjectPayloadReclaimSubject {
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
        };
        if object_payload_has_reclaim_root(cluster, &subject)? {
            return Err(ObjectPgActionError::InvalidRequest {
                reason: "selected no-root reclaim subject already has durable reclaim metadata"
                    .to_string(),
            });
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
    ) -> Result<TestObjectPayloadReclaimSubject, ObjectPgActionError> {
        let subject = prepare_object_payload_reclaim_subject_without_root(cluster, bucket, key)?;
        cluster.test_seed_segmented_payload_reclaim(
            bucket,
            key,
            subject.generation_id,
            created_at,
        )?;
        Ok(subject)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn acquire_object_payload_reclaim_lease(
        cluster: &Arc<StorageCluster>,
        subject: &TestObjectPayloadReclaimSubject,
    ) -> Result<ObjectPayloadLease, ObjectPgActionError> {
        cluster
            .acquire_object_payload_lease(&subject.bucket, &subject.key, subject.generation_id)
            .map_err(ObjectPgActionError::Store)
    }

    pub fn object_payload_has_reclaim_root(
        cluster: &StorageCluster,
        subject: &TestObjectPayloadReclaimSubject,
    ) -> Result<bool, ObjectPgActionError> {
        cluster.test_payload_reclaim_exists(&subject.bucket, &subject.key, subject.generation_id)
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
    ) -> Result<usize, ObjectPgActionError> {
        cluster.test_payload_reclaim_count_for_object(bucket, key)
    }

    pub fn stream_upload_session_count_for_object(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<usize, ObjectPgActionError> {
        cluster.test_list_all_stream_uploads().map(|sessions| {
            sessions
                .into_iter()
                .filter(|session| session.bucket == *bucket && session.key == *key)
                .count()
        })
    }

    /// Returns the logical session identities for one object without exposing
    /// durable stream-upload records or their physical placement.
    pub fn stream_upload_session_ids_for_object(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<SessionId>, ObjectPgActionError> {
        cluster.test_list_all_stream_uploads().map(|sessions| {
            let mut session_ids = sessions
                .into_iter()
                .filter(|session| session.bucket == *bucket && session.key == *key)
                .map(|session| session.session_id)
                .collect::<Vec<_>>();
            session_ids.sort();
            session_ids
        })
    }

    pub fn stream_upload_session_exists(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
    ) -> Result<bool, ObjectPgActionError> {
        cluster.test_list_all_stream_uploads().map(|sessions| {
            sessions.into_iter().any(|session| {
                session.bucket == *bucket
                    && session.key == *key
                    && session.session_id == *session_id
            })
        })
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
    ) -> Result<Option<u64>, ObjectPgActionError> {
        cluster.test_list_all_stream_uploads().map(|sessions| {
            sessions
                .into_iter()
                .find(|session| {
                    session.bucket == *bucket
                        && session.key == *key
                        && session.session_id == *session_id
                })
                .and_then(|session| session.cleanup_after)
        })
    }

    /// Returns the logical number of durable stream-upload sessions without
    /// exposing their storage-owned records to downstream crates.
    pub fn stream_upload_session_count(
        cluster: &StorageCluster,
    ) -> Result<usize, ObjectPgActionError> {
        cluster
            .test_list_all_stream_uploads()
            .map(|sessions| sessions.len())
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
    ) -> Result<usize, ObjectPgActionError> {
        cluster.test_list_all_stream_uploads().map(|sessions| {
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
    }

    pub fn multipart_upload_count_for_object(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<usize, ObjectPgActionError> {
        cluster
            .test_list_multipart_uploads_for_bucket(bucket)
            .map(|uploads| {
                uploads
                    .into_iter()
                    .filter(|upload| upload.key == *key)
                    .count()
            })
    }

    pub fn multipart_upload_count_for_bucket(
        cluster: &StorageCluster,
        bucket: &BucketName,
    ) -> Result<usize, ObjectPgActionError> {
        cluster
            .test_list_multipart_uploads_for_bucket(bucket)
            .map(|uploads| uploads.len())
    }

    pub fn multipart_upload_ids_for_object(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<UploadId>, ObjectPgActionError> {
        cluster
            .test_list_multipart_uploads_for_bucket(bucket)
            .map(|uploads| {
                uploads
                    .into_iter()
                    .filter(|upload| upload.key == *key)
                    .map(|upload| upload.upload_id)
                    .collect()
            })
    }

    pub fn multipart_upload_exists(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<bool, ObjectPgActionError> {
        match cluster.test_get_multipart_upload(bucket, key, upload_id) {
            Ok(_) => Ok(true),
            Err(ObjectPgActionError::Metadata(MetadataError::NoSuchUpload { .. })) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub fn multipart_upload_initiated_at(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<u64, ObjectPgActionError> {
        cluster
            .test_get_multipart_upload(bucket, key, upload_id)
            .map(|upload| upload.initiated_at)
    }

    pub fn multipart_upload_has_owners(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_initiator: &OwnerIdentity,
        expected_owner: &OwnerIdentity,
    ) -> Result<bool, ObjectPgActionError> {
        cluster
            .test_get_multipart_upload(bucket, key, upload_id)
            .map(|upload| {
                upload.initiator == *expected_initiator && upload.owner == *expected_owner
            })
    }

    pub fn multipart_upload_matches_creation_metadata(
        cluster: &StorageCluster,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_tags: Option<&s3_types::TagSet>,
        expected_metadata: &SerializedMetadataBlob,
        expected_system_metadata: &SerializedSystemMetadataBlob,
    ) -> Result<bool, ObjectPgActionError> {
        cluster
            .test_get_multipart_upload(bucket, key, upload_id)
            .map(|upload| {
                upload.tags.as_ref().map(SerializedTagSet::tag_set) == expected_tags
                    && upload.metadata_blob == *expected_metadata
                    && upload.system_metadata_blob == *expected_system_metadata
            })
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
    ) -> Result<TestMultipartUploadGenerationSubject, ObjectPgActionError> {
        cluster
            .test_get_multipart_upload(bucket, key, upload_id)
            .map(|upload| TestMultipartUploadGenerationSubject {
                bucket: upload.bucket,
                key: upload.key,
                generation_id: upload.object_generation_id,
            })
    }

    pub fn completed_object_uses_multipart_upload_generation(
        cluster: &StorageCluster,
        subject: &TestMultipartUploadGenerationSubject,
        version_id: VersionId,
    ) -> Result<bool, ObjectPgActionError> {
        let object = cluster.test_get_object_version(&subject.bucket, &subject.key, version_id)?;
        Ok(object
            .as_live()
            .is_some_and(|live| live.generation_id == subject.generation_id))
    }

    pub fn multipart_upload_state(
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
    ) -> Result<bool, ObjectPgActionError> {
        cluster.test_reclaim_object_payload_if_unleased(
            &subject.bucket,
            &subject.key,
            subject.generation_id,
        )
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
    TestObjectPayloadRepairObservation, TestObjectPayloadSnapshot, TestStreamUploadPayloadSnapshot,
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
