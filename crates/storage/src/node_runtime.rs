//! Private storage-node runtime implementation.
//!
//! Raw node state, concrete node clients, and the storage-node server share
//! this parent so descendant-only visibility can enforce their implementation
//! boundary. Crate-facing contracts are re-exported through narrow facade
//! modules in `lib.rs`.

#[allow(dead_code)]
#[path = "node.rs"]
mod engine;

#[path = "node_client.rs"]
mod clients;

#[path = "storage_node_server.rs"]
mod server;

#[allow(dead_code)]
#[path = "pg_store.rs"]
mod pg_store;

#[allow(dead_code)]
#[path = "traits.rs"]
mod traits;

pub(super) mod node_facade {
    #[cfg(feature = "test-hooks")]
    pub use super::engine::BucketScopedTestHookGuard;
    #[cfg(test)]
    pub(crate) use super::engine::LocalStorageNode;
    #[cfg(any(test, feature = "test-hooks"))]
    pub use super::engine::{install_bucket_scoped_test_hooks, BucketScopedTestHooks};
    pub(crate) use super::engine::{
        maybe_run_after_begin_bucket_delete_drain_hook,
        maybe_run_after_bucket_delete_finalize_claim_hook,
        maybe_run_after_bucket_delete_finalize_hook,
        maybe_run_before_lifecycle_bucket_write_proof_acquire_hook,
        maybe_run_before_lifecycle_context_load_hook, maybe_run_bucket_write_drain_wait_hook,
        LocalNodeRuntime, ReclaimQueueInsert, OBJECT_PAYLOAD_RECLAIM_MAX_OUTSTANDING_PER_PG,
    };
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) use super::engine::{
        maybe_run_after_direct_put_metadata_publish_hook,
        maybe_run_after_object_metadata_command_publish_hook, BucketPgTestGuard,
        DirectPutMetadataPublishTestHookGuard, ObjectMetadataCommandPublishTestHookGuard,
        SharedStorageNode,
    };
    pub use super::engine::{
        BucketCreateAttemptOutcome, BucketDeleteBeginRoot, BucketDeleteFinalizeOutcome,
        ReclaimWorkItem,
    };
}

pub(super) mod client_facade {
    pub use super::clients::*;
}

pub(super) mod server_facade {
    pub use super::server::{
        storage_node_control_plane_heartbeat_interval, validate_storage_node_process_configs,
        PreparedStorageNodeServer, StorageNodeBootstrap, StorageNodeControlPlaneRefresh,
        StorageNodeControlPlaneRefreshLoop, StorageNodeControlPlaneRefreshLoopStatus,
        StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeProcessConfigParts,
        StorageNodeServer, StorageNodeServerError,
        STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MAX_INTERVAL_MS,
        STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_LEASE_MS,
        STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MIN_USABLE_LEASE_MS,
    };
}

pub(super) mod pg_store_facade {
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) use super::pg_store::PgStore;
    pub use super::pg_store::{
        MetadataCheckpointRow, MetadataCheckpointTableBlock, MetadataCheckpointTableDigest,
        MetadataCheckpointValue, MetadataCommandCheckpoint,
        MetadataCommandCheckpointValidationError, MetadataCommandLogCompactionStatus,
        MetadataCommandLogStats, PgClusterMapHistoryReferenceSummary,
        PgClusterMapHistoryRouteReference, PgClusterMapHistoryRouteReferenceKind,
        PgClusterMapHistoryRouteReferences, MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES,
    };
    pub(crate) use super::pg_store::{
        ScavengerShardFile, ScavengerShardFileScan, ScavengerShardRow,
        METADATA_CANONICAL_STATE_ENCODING_VERSION,
    };
}

pub(super) mod traits_facade {
    pub(crate) use super::traits::DurableBucketWriteReservationAcquire;
    #[cfg(test)]
    pub(crate) use super::traits::DurableBucketWriteReservationHeartbeat;
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) use super::traits::PgMetadataStore;
    #[cfg(test)]
    pub(crate) use super::traits::{ShardStore, StorageNode};
}
