//! Private storage-node runtime implementation.
//!
//! Raw node state, concrete node clients, and the storage-node server share
//! this parent so descendant-only visibility can enforce their implementation
//! boundary. Crate-facing contracts are re-exported through narrow facade
//! modules in `lib.rs`.

use crate::metadata_command::{
    is_stream_create_bucket_write_operation_kind, AbortStreamUploadCommand,
    MetadataCommandEnvelope, MetadataCommandPayload,
};
use crate::types::{BucketName, ObjectKey, PgId, SessionId};

/// PG containing payload shard data for an object segment or multipart part.
///
/// A standalone placement-only `PgTopology` returns raw `PgId` values. Only
/// installed runtime maps and storage nodes may promote those values after
/// validating placement or an active/retained route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DataPgId(PgId);

impl DataPgId {
    pub(in crate::node_runtime) const fn from_validated_placement(pg_id: PgId) -> Self {
        Self(pg_id)
    }

    #[cfg(test)]
    pub(crate) const fn new_for_test(pg_id: PgId) -> Self {
        Self(pg_id)
    }

    #[must_use]
    pub const fn pg_id(self) -> PgId {
        self.0
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl From<DataPgId> for PgId {
    fn from(value: DataPgId) -> Self {
        value.pg_id()
    }
}

/// PG containing bucket metadata for one bucket name.
///
/// The private field lives inside the node-runtime boundary so a standalone
/// placement-only `PgTopology` cannot mint this role. Installed local runtime
/// maps and storage nodes construct it after topology/route validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BucketPgId(PgId);

impl BucketPgId {
    #[cfg(test)]
    pub(crate) const fn new_for_test(pg_id: PgId) -> Self {
        Self(pg_id)
    }

    #[must_use]
    pub const fn pg_id(self) -> PgId {
        self.0
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl From<BucketPgId> for PgId {
    fn from(value: BucketPgId) -> Self {
        value.pg_id()
    }
}

/// PG containing object metadata for one `(bucket, key)` namespace entry.
///
/// Like `BucketPgId`, this role can only be minted inside an installed
/// node-runtime authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectMetadataPgId(PgId);

impl ObjectMetadataPgId {
    #[cfg(test)]
    pub(crate) const fn new_for_test(pg_id: PgId) -> Self {
        Self(pg_id)
    }

    #[must_use]
    pub const fn pg_id(self) -> PgId {
        self.0
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl From<ObjectMetadataPgId> for PgId {
    fn from(value: ObjectMetadataPgId) -> Self {
        value.pg_id()
    }
}

/// Exact retained stream-abort command produced by validated node state.
///
/// Construction is confined to the private node-runtime boundary shared by
/// the embedded adapter, RPC client, and storage-node server. Cluster callers
/// may inspect and fan out this value, but cannot manufacture one from an
/// arbitrary metadata command or redirect it to another PG.
#[derive(Debug)]
pub(crate) struct PreparedRetainedStreamUploadAbort {
    pg_id: ObjectMetadataPgId,
    command: MetadataCommandEnvelope,
}

impl PreparedRetainedStreamUploadAbort {
    pub(in crate::node_runtime) fn new_if_matches(
        pg_id: ObjectMetadataPgId,
        cluster_epoch: crate::types::ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        command: MetadataCommandEnvelope,
    ) -> Option<Self> {
        let MetadataCommandPayload::AbortStreamUpload(abort) = command.payload() else {
            return None;
        };
        let proof_matches = abort.stream_create_bucket_write_reservation.is_none()
            || abort
                .stream_create_bucket_write_reservation
                .as_ref()
                .is_some_and(|proof| {
                    proof.bucket == *bucket
                        && proof.cluster_epoch == cluster_epoch
                        && is_stream_create_bucket_write_operation_kind(&proof.operation_kind)
                        && proof.target_context.as_deref() == Some(key.as_str())
                });
        if command.id().cluster_epoch() != cluster_epoch
            || command.id().pg_id() != pg_id.pg_id()
            || abort.bucket != *bucket
            || abort.key != *key
            || abort.session_id != *session_id
            || abort
                .staged_segments
                .iter()
                .any(|segment| segment.session_id != *session_id)
            || !proof_matches
        {
            return None;
        }
        Some(Self { pg_id, command })
    }

    #[must_use]
    pub(crate) const fn pg_id(&self) -> ObjectMetadataPgId {
        self.pg_id
    }

    #[must_use]
    pub(crate) const fn command(&self) -> &MetadataCommandEnvelope {
        &self.command
    }

    #[must_use]
    pub(crate) fn abort(&self) -> &AbortStreamUploadCommand {
        let MetadataCommandPayload::AbortStreamUpload(abort) = self.command.payload() else {
            unreachable!("prepared retained stream abort must contain an abort command");
        };
        abort
    }
}

#[cfg(test)]
mod prepared_retained_stream_upload_abort_tests {
    use super::*;
    use crate::metadata_command::{
        MetadataCommandId, MetadataCommandLogIndex, MetadataCommandPayload,
    };
    use crate::types::{ClusterEpoch, GenerationId, StreamUploadSegmentRecord};

    fn abort_command(
        pg_id: PgId,
        cluster_epoch: ClusterEpoch,
        bucket: &BucketName,
        key: &ObjectKey,
        session_id: &SessionId,
        staged_segments: Vec<StreamUploadSegmentRecord>,
    ) -> MetadataCommandEnvelope {
        MetadataCommandEnvelope::new(
            MetadataCommandId::new(
                cluster_epoch,
                pg_id,
                MetadataCommandLogIndex::new(1).unwrap(),
            ),
            MetadataCommandPayload::AbortStreamUpload(Box::new(AbortStreamUploadCommand {
                bucket: bucket.clone(),
                key: key.clone(),
                session_id: session_id.clone(),
                staged_segments,
                stream_create_bucket_write_reservation: None,
            })),
        )
    }

    #[test]
    fn prepared_abort_binds_route_subject_and_staged_session() {
        let cluster_epoch = ClusterEpoch::INITIAL;
        let pg_id = ObjectMetadataPgId::new_for_test(PgId::new(0));
        let bucket = crate::tests::bucket_name("prepared-retained-abort-bucket");
        let key = crate::tests::object_key("prepared-retained-abort-key");
        let session_id = crate::tests::stream_session_id("prepared-abort");
        let segment = StreamUploadSegmentRecord {
            session_id: session_id.clone(),
            segment_index: 0,
            size: 17,
            segment_crc64: 41,
            payload_crc64: 41,
            segment_okh: [0x61; 16],
            segment_vid: GenerationId::new(2).unwrap(),
            data_pg_id: 0,
            placement_cluster_epoch: cluster_epoch,
            ec_k: 1,
            ec_m: 0,
        };
        let command = abort_command(
            pg_id.pg_id(),
            cluster_epoch,
            &bucket,
            &key,
            &session_id,
            vec![segment.clone()],
        );

        let prepared = PreparedRetainedStreamUploadAbort::new_if_matches(
            pg_id,
            cluster_epoch,
            &bucket,
            &key,
            &session_id,
            command.clone(),
        )
        .expect("matching retained abort must be promoted");
        assert_eq!(prepared.pg_id(), pg_id);
        assert_eq!(prepared.command(), &command);
        assert_eq!(prepared.abort().staged_segments, vec![segment.clone()]);

        let wrong_bucket = crate::tests::bucket_name("wrong-prepared-retained-abort-bucket");
        let wrong_key = crate::tests::object_key("wrong-prepared-retained-abort-key");
        let wrong_session = crate::tests::stream_session_id("wrong-abort");
        assert!(PreparedRetainedStreamUploadAbort::new_if_matches(
            pg_id,
            cluster_epoch,
            &wrong_bucket,
            &key,
            &session_id,
            command.clone(),
        )
        .is_none());
        assert!(PreparedRetainedStreamUploadAbort::new_if_matches(
            pg_id,
            cluster_epoch,
            &bucket,
            &wrong_key,
            &session_id,
            command.clone(),
        )
        .is_none());
        assert!(PreparedRetainedStreamUploadAbort::new_if_matches(
            pg_id,
            cluster_epoch,
            &bucket,
            &key,
            &wrong_session,
            command.clone(),
        )
        .is_none());

        let wrong_epoch = ClusterEpoch::new(cluster_epoch.get() + 1).unwrap();
        assert!(PreparedRetainedStreamUploadAbort::new_if_matches(
            pg_id,
            wrong_epoch,
            &bucket,
            &key,
            &session_id,
            command.clone(),
        )
        .is_none());
        assert!(PreparedRetainedStreamUploadAbort::new_if_matches(
            ObjectMetadataPgId::new_for_test(PgId::new(1)),
            cluster_epoch,
            &bucket,
            &key,
            &session_id,
            command,
        )
        .is_none());

        let wrong_segment_command = abort_command(
            pg_id.pg_id(),
            cluster_epoch,
            &bucket,
            &key,
            &session_id,
            vec![StreamUploadSegmentRecord {
                session_id: wrong_session,
                ..segment
            }],
        );
        assert!(PreparedRetainedStreamUploadAbort::new_if_matches(
            pg_id,
            cluster_epoch,
            &bucket,
            &key,
            &session_id,
            wrong_segment_command,
        )
        .is_none());
    }
}

/// Installed object-metadata PG selected for a listing scan.
///
/// Unlike `ObjectMetadataPgId`, this role is not bound to one exact object
/// key. Installed runtime topology may mint it for each configured metadata
/// PG while fan-out listing code scans the complete object namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectMetadataScanPgId(PgId);

impl ObjectMetadataScanPgId {
    #[cfg(test)]
    pub(crate) const fn new_for_test(pg_id: PgId) -> Self {
        Self(pg_id)
    }

    #[must_use]
    pub const fn pg_id(self) -> PgId {
        self.0
    }

    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl From<ObjectMetadataScanPgId> for PgId {
    fn from(value: ObjectMetadataScanPgId) -> Self {
        value.pg_id()
    }
}

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
        maybe_run_before_begin_bucket_delete_drain_hook,
        maybe_run_before_lifecycle_bucket_write_proof_acquire_hook,
        maybe_run_before_lifecycle_context_load_hook, maybe_run_bucket_write_drain_wait_hook,
        LocalNodeRuntime, ReclaimQueueInsert, OBJECT_PAYLOAD_RECLAIM_MAX_OUTSTANDING_PER_PG,
    };
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) use super::engine::{
        maybe_run_after_direct_put_metadata_publish_hook, BucketPgTestGuard,
        DirectPutMetadataPublishHook, DirectPutMetadataPublishTestHookGuard, SharedStorageNode,
    };
    #[cfg(test)]
    pub(crate) use super::engine::{
        maybe_run_after_object_metadata_command_publish_hook,
        ObjectMetadataCommandPublishTestHookGuard,
    };
    pub use super::engine::{BucketCreateAttemptOutcome, BucketDeleteFinalizeOutcome};
    pub(crate) use super::engine::{BucketDeleteBeginRoot, ReclaimWorkItem};
}

pub(super) mod role_facade {
    pub use super::{BucketPgId, DataPgId, ObjectMetadataPgId, ObjectMetadataScanPgId};
}

pub(super) mod client_facade {
    pub use super::clients::*;
}

pub(super) mod server_facade {
    pub use super::server::{
        initialize_storage_node_state, inspect_initialized_storage_node_state,
        storage_node_control_plane_heartbeat_interval, validate_storage_node_process_configs,
        PreparedStorageNodeServer, StorageNodeBootstrap, StorageNodeControlPlaneRefresh,
        StorageNodeControlPlaneRefreshLoop, StorageNodeControlPlaneRefreshLoopStatus,
        StorageNodePgRoute, StorageNodeProcessConfig, StorageNodeProcessConfigParts,
        StorageNodeRpcListenerConfig, StorageNodeServer, StorageNodeServerError,
        StorageNodeStateDiagnostic, StorageNodeStateInitializationGuard,
        StorageNodeStateInspection, STORAGE_NODE_CONTROL_PLANE_HEARTBEAT_MAX_INTERVAL_MS,
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
        MAX_PG_DURABLE_IDENTITY_BYTES,
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
