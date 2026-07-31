#[cfg(test)]
use super::runtime::LifecycleSweepStats;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::authz_types::{AuthorizedPutObjectWrite, ValidatedBucket};
use super::payload::PayloadBufferPool;
use super::read_core::ReadRuntime;
use super::request_types::AuthorizePutObjectRequest;
use super::response_types::{BucketSummary, ModernBucketSummary};
use super::runtime::{
    acquire_shard_backfill_sweeper, acquire_shard_repair_sweeper, acquire_shard_scavenger_sweeper,
    acquire_stream_session_sweeper, LifecycleSweeper, ReclaimSweeper, ShardBackfillSweeper,
    ShardRepairSweeper, ShardScavengerSweeper, StreamSessionSweeper,
};
#[cfg(test)]
use super::trusted_bucket_name;
use super::{
    map_store_error, shared_caches_for_storage_cluster, Coordinator, CoordinatorSharedCaches,
};
use crate::error::ServerError;
use crate::sse::{SseCustomerValidatorConfig, StaticManagedKeyProvider};
#[cfg(test)]
use storage::PgTopology;
use storage::{
    BucketFastPathInfo, BucketInfo, BucketName, BucketState, SessionId, StorageCluster,
    StorageClusterRouteAdmission, StorageClusterRouteHandle,
};

fn static_storage_cluster_route_handle(
    storage_cluster: Arc<StorageCluster>,
) -> Result<StorageClusterRouteHandle, ServerError> {
    StorageClusterRouteHandle::from_static_cluster(storage_cluster).map_err(|error| {
        ServerError::InternalError {
            reason: error.to_string(),
        }
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackgroundWorkerMode {
    pub object_reclaim_and_bucket_finalize: bool,
    pub lifecycle: bool,
    pub shard_scavenger: bool,
    pub shard_repair: bool,
    pub shard_backfill: bool,
    pub stream_session: bool,
}

impl BackgroundWorkerMode {
    pub const fn all() -> Self {
        Self {
            object_reclaim_and_bucket_finalize: true,
            lifecycle: true,
            shard_scavenger: true,
            shard_repair: true,
            shard_backfill: true,
            stream_session: true,
        }
    }

    pub const fn none() -> Self {
        Self {
            object_reclaim_and_bucket_finalize: false,
            lifecycle: false,
            shard_scavenger: false,
            shard_repair: false,
            shard_backfill: false,
            stream_session: false,
        }
    }

    pub const fn remote_frontend_phase_10_6() -> Self {
        Self {
            object_reclaim_and_bucket_finalize: true,
            lifecycle: true,
            shard_scavenger: true,
            shard_repair: true,
            shard_backfill: true,
            stream_session: true,
        }
    }
}

struct CoordinatorStorageContext {
    foreground_handle: StorageClusterRouteHandle,
    background_handle: StorageClusterRouteHandle,
    foreground_cluster: Arc<StorageCluster>,
    background_cluster: Arc<StorageCluster>,
}

impl CoordinatorStorageContext {
    fn new(
        foreground_handle: StorageClusterRouteHandle,
        background_handle: StorageClusterRouteHandle,
    ) -> Self {
        let foreground_cluster = foreground_handle.current();
        let background_cluster = background_handle.current();
        Self {
            foreground_handle,
            background_handle,
            foreground_cluster,
            background_cluster,
        }
    }

    fn shared(handle: StorageClusterRouteHandle, cluster: Arc<StorageCluster>) -> Self {
        Self {
            foreground_handle: handle.clone(),
            background_handle: handle,
            foreground_cluster: Arc::clone(&cluster),
            background_cluster: cluster,
        }
    }
}

impl Coordinator {
    pub(super) fn random_session_id(error_reason: &'static str) -> Result<SessionId, ServerError> {
        const HEX: &[u8; 16] = b"0123456789abcdef";

        let rng = ring::rand::SystemRandom::new();
        let mut id_bytes = [0u8; 16];
        ring::rand::SecureRandom::fill(&rng, &mut id_bytes).map_err(|_| {
            ServerError::InternalError {
                reason: error_reason.to_string(),
            }
        })?;

        let mut encoded = [0u8; storage::SESSION_ID_LEN];
        for (index, byte) in id_bytes.iter().copied().enumerate() {
            encoded[index * 2] = HEX[(byte >> 4) as usize];
            encoded[index * 2 + 1] = HEX[(byte & 0x0f) as usize];
        }

        let encoded =
            String::from_utf8(encoded.to_vec()).expect("hex-encoded session IDs must be UTF-8");
        Ok(SessionId::try_from(encoded).expect("generated session IDs must be valid"))
    }

    #[cfg(test)]
    pub(super) fn create_stream_put_session_for_authorized_write(
        &self,
        authorized: &AuthorizedPutObjectWrite,
    ) -> Result<SessionId, ServerError> {
        self.create_stream_put_session_for_authorized_write_with_storage_node(
            &self.storage_node(),
            authorized,
        )
    }

    pub(super) fn create_stream_put_session_for_authorized_write_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        authorized: &AuthorizedPutObjectWrite,
    ) -> Result<SessionId, ServerError> {
        self.create_stream_put_session_for_authorized_write_with_storage_node_and_cleanup_deadline(
            storage_node,
            authorized,
            None,
        )
    }

    pub(super) fn create_stream_put_session_for_authorized_write_with_storage_node_and_cleanup_deadline(
        &self,
        storage_node: &Arc<StorageCluster>,
        authorized: &AuthorizedPutObjectWrite,
        cleanup_after: Option<u64>,
    ) -> Result<SessionId, ServerError> {
        let stored_encryption = authorized.write_encryption.object_encryption();
        let session_id = Self::random_session_id("failed to generate session ID")?;
        storage_node
            .create_put_object_stream_session_record_with_cleanup_deadline(
                authorized.bucket_typed(),
                authorized.key_typed(),
                &session_id,
                stored_encryption,
                cleanup_after,
            )
            .map_err(Self::map_object_pg_action_error)?;

        Ok(session_id)
    }

    pub fn prepare_put_object_write(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        self.authorize_put_object_write(req)
    }

    pub fn prepare_put_object_write_with_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        req: &AuthorizePutObjectRequest<'_>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        self.authorize_put_object_write_with_storage_node(storage_node, req)
    }

    pub fn prepare_put_object_write_with_storage_admission(
        &self,
        admission: &storage::StorageClusterRouteAdmission,
        req: &AuthorizePutObjectRequest<'_>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        self.require_storage_route_admission(admission)?;
        self.authorize_put_object_write_on_admitted_route(admission, req)
    }

    pub(super) fn bucket_summary(info: BucketInfo) -> BucketSummary {
        Self::bucket_summary_ref(&info)
    }

    fn bucket_summary_ref(info: &BucketInfo) -> BucketSummary {
        BucketSummary {
            name: info.name.clone(),
            owner_principal: info.owner_principal.clone(),
            owner_canonical_id: info.owner_canonical_id.clone(),
            created_at: info.created_at,
            acl_grants: info.acl_grants.clone(),
            public_read: info.public_read,
            public_write: info.public_write,
            versioning: info.versioning,
            object_lock: info.object_lock,
            public_access_block: info.public_access_block,
            ownership_controls: info.ownership_controls,
            bucket_policy_present: info.bucket_policy_present,
            bucket_policy_public: info.bucket_policy_public,
            bucket_policy_generation: info.bucket_policy_generation,
            bucket_lifecycle_present: info.bucket_lifecycle_present,
            bucket_lifecycle_generation: info.bucket_lifecycle_generation,
            multipart_upload_id_key: info.multipart_upload_id_key().clone(),
            bucket_abac_enabled: info.bucket_abac_enabled,
            encryption: info.encryption,
        }
    }

    pub(super) fn modern_bucket_summary_fast(info: BucketFastPathInfo) -> ModernBucketSummary {
        ModernBucketSummary {
            name: info.name,
            owner_principal: info.owner_principal,
            owner_canonical_id: info.owner_canonical_id,
            created_at: info.created_at,
            versioning: info.versioning,
            object_lock: info.object_lock,
            public_access_block: info.public_access_block,
            ownership_controls: info.ownership_controls,
            bucket_policy_present: info.bucket_policy_present,
            bucket_policy_public: info.bucket_policy_public,
            bucket_policy_generation: info.bucket_policy_generation,
            bucket_lifecycle_present: info.bucket_lifecycle_present,
            bucket_lifecycle_generation: info.bucket_lifecycle_generation,
            multipart_upload_id_key: info.multipart_upload_id_key,
            bucket_abac_enabled: info.bucket_abac_enabled,
            encryption: info.encryption,
        }
    }

    pub(super) fn bucket_summary_for_boe_modern_fast_path(
        info: ModernBucketSummary,
    ) -> BucketSummary {
        BucketSummary {
            name: info.name,
            owner_principal: info.owner_principal,
            owner_canonical_id: info.owner_canonical_id,
            created_at: info.created_at,
            acl_grants: s3_types::AclGrants::default(),
            public_read: false,
            public_write: false,
            versioning: info.versioning,
            object_lock: info.object_lock,
            public_access_block: info.public_access_block,
            ownership_controls: info.ownership_controls,
            bucket_policy_present: info.bucket_policy_present,
            bucket_policy_public: info.bucket_policy_public,
            bucket_policy_generation: info.bucket_policy_generation,
            bucket_lifecycle_present: info.bucket_lifecycle_present,
            bucket_lifecycle_generation: info.bucket_lifecycle_generation,
            multipart_upload_id_key: info.multipart_upload_id_key,
            bucket_abac_enabled: info.bucket_abac_enabled,
            encryption: info.encryption,
        }
    }

    #[cfg(test)]
    pub(super) fn unchecked_active_bucket_summary(
        &self,
        name: &str,
    ) -> Result<BucketSummary, ServerError> {
        let name = trusted_bucket_name(name);
        let info = self
            .storage_node()
            .head_bucket_info(&name)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        if info.state != BucketState::Active {
            return Err(ServerError::BucketNotFound {
                name: name.to_string(),
            });
        }
        Ok(Self::bucket_summary(info))
    }

    pub(super) fn unchecked_active_bucket_summary_for(
        &self,
        name: &BucketName,
    ) -> Result<BucketSummary, ServerError> {
        self.unchecked_active_bucket_summary_for_storage_node(&self.storage_node(), name)
    }

    pub(super) fn unchecked_active_bucket_summary_for_admitted_route(
        &self,
        admission: &StorageClusterRouteAdmission,
        name: &BucketName,
    ) -> Result<BucketSummary, ServerError> {
        let info = admission
            .active_bucket_route(name)
            .map_err(map_store_error)?
            .head_bucket_info()
            .map_err(Self::map_bucket_snapshot_load_error)?;
        if info.state != BucketState::Active {
            return Err(ServerError::BucketNotFound {
                name: name.to_string(),
            });
        }
        Ok(Self::bucket_summary(info))
    }

    pub(super) fn unchecked_active_bucket_summary_for_storage_node(
        &self,
        storage_node: &Arc<StorageCluster>,
        name: &BucketName,
    ) -> Result<BucketSummary, ServerError> {
        let info = storage_node
            .head_bucket_info(name)
            .map_err(Self::map_bucket_snapshot_load_error)?;
        if info.state != BucketState::Active {
            return Err(ServerError::BucketNotFound {
                name: name.to_string(),
            });
        }
        Ok(Self::bucket_summary(info))
    }

    pub(super) fn checked_active_bucket_summary_for_admitted_route(
        &self,
        admission: &StorageClusterRouteAdmission,
        name: &BucketName,
        expected_bucket_owner: Option<&str>,
    ) -> Result<ValidatedBucket, ServerError> {
        self.require_storage_route_admission(admission)?;
        Self::validate_expected_bucket_owner(
            self.unchecked_active_bucket_summary_for_admitted_route(admission, name)?,
            expected_bucket_owner,
        )
    }

    /// Create a new coordinator over a cluster-shaped storage handle.
    pub fn new_with_storage_cluster(
        storage_cluster: Arc<StorageCluster>,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
    ) -> Result<Self, ServerError> {
        let lifecycle_sweeper_factory =
            |storage_handle: &StorageClusterRouteHandle, read_runtime: ReadRuntime| {
                LifecycleSweeper::acquire_shared(storage_handle, read_runtime)
            };
        Self::new_with_shared_caches_and_lifecycle_sweeper_factory(
            Arc::clone(&storage_cluster),
            shared_caches_for_storage_cluster(&storage_cluster),
            region,
            sse_c_validator,
            None,
            lifecycle_sweeper_factory,
        )
    }

    /// Create a new coordinator with managed encryption over a cluster-shaped storage handle.
    pub fn new_with_managed_key_provider_for_storage_cluster(
        storage_cluster: Arc<StorageCluster>,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: StaticManagedKeyProvider,
    ) -> Result<Self, ServerError> {
        let lifecycle_sweeper_factory =
            |storage_handle: &StorageClusterRouteHandle, read_runtime: ReadRuntime| {
                LifecycleSweeper::acquire_shared(storage_handle, read_runtime)
            };
        Self::new_with_shared_caches_and_lifecycle_sweeper_factory(
            Arc::clone(&storage_cluster),
            shared_caches_for_storage_cluster(&storage_cluster),
            region,
            sse_c_validator,
            Some(managed_key_provider),
            lifecycle_sweeper_factory,
        )
    }

    /// Create a new coordinator over a cluster-shaped storage handle without
    /// starting background sweepers.
    ///
    /// Tests use this when background work would make assertions
    /// nondeterministic.
    pub fn new_with_managed_key_provider_for_storage_cluster_without_background_sweepers(
        storage_cluster: Arc<StorageCluster>,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: StaticManagedKeyProvider,
    ) -> Result<Self, ServerError> {
        Self::new_with_managed_key_provider_for_storage_cluster_with_background_worker_mode(
            storage_cluster,
            region,
            sse_c_validator,
            managed_key_provider,
            BackgroundWorkerMode::none(),
        )
    }

    pub fn new_with_managed_key_provider_for_storage_cluster_with_background_worker_mode(
        storage_cluster: Arc<StorageCluster>,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: StaticManagedKeyProvider,
        background_worker_mode: BackgroundWorkerMode,
    ) -> Result<Self, ServerError> {
        Self::new_with_managed_key_provider_for_storage_cluster_route_handle_with_background_worker_mode(
            static_storage_cluster_route_handle(storage_cluster)?,
            region,
            sse_c_validator,
            managed_key_provider,
            background_worker_mode,
        )
    }

    pub fn new_with_managed_key_provider_for_storage_cluster_route_handle_with_background_worker_mode(
        storage_cluster: StorageClusterRouteHandle,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: StaticManagedKeyProvider,
        background_worker_mode: BackgroundWorkerMode,
    ) -> Result<Self, ServerError> {
        Self::new_with_managed_key_provider_for_storage_cluster_route_handles_with_background_worker_mode(
            storage_cluster.clone(),
            storage_cluster,
            region,
            sse_c_validator,
            managed_key_provider,
            background_worker_mode,
        )
    }

    pub fn new_with_managed_key_provider_for_storage_cluster_route_handles_with_background_worker_mode(
        storage_cluster: StorageClusterRouteHandle,
        maintenance_storage_cluster: StorageClusterRouteHandle,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: StaticManagedKeyProvider,
        background_worker_mode: BackgroundWorkerMode,
    ) -> Result<Self, ServerError> {
        let storage_context =
            CoordinatorStorageContext::new(storage_cluster, maintenance_storage_cluster);
        let lifecycle_sweeper_factory =
            |storage_handle: &StorageClusterRouteHandle, read_runtime: ReadRuntime| {
                if background_worker_mode.lifecycle {
                    LifecycleSweeper::acquire_shared(storage_handle, read_runtime)
                } else {
                    Ok(LifecycleSweeper::disabled())
                }
            };
        let shard_scavenger_sweeper_factory = |storage_handle: &StorageClusterRouteHandle| {
            if background_worker_mode.shard_scavenger {
                acquire_shard_scavenger_sweeper(storage_handle)
            } else {
                Ok(ShardScavengerSweeper::disabled(storage_handle.clone()))
            }
        };
        let shard_repair_sweeper_factory = |storage_handle: &StorageClusterRouteHandle| {
            if background_worker_mode.shard_repair {
                acquire_shard_repair_sweeper(storage_handle)
            } else {
                Ok(ShardRepairSweeper::disabled(storage_handle.clone()))
            }
        };
        let shard_backfill_sweeper_factory = |storage_handle: &StorageClusterRouteHandle| {
            if background_worker_mode.shard_backfill {
                acquire_shard_backfill_sweeper(storage_handle)
            } else {
                Ok(ShardBackfillSweeper::disabled(storage_handle.clone()))
            }
        };
        let stream_session_sweeper_factory = |storage_handle: &StorageClusterRouteHandle| {
            if background_worker_mode.stream_session {
                acquire_stream_session_sweeper(storage_handle)
            } else {
                Ok(StreamSessionSweeper::disabled(storage_handle.clone()))
            }
        };
        Self::new_with_shared_caches_and_background_storage_and_sweeper_factories(
            shared_caches_for_storage_cluster(&storage_context.foreground_cluster),
            storage_context,
            region,
            sse_c_validator,
            Some(managed_key_provider),
            (
                background_worker_mode.object_reclaim_and_bucket_finalize,
                lifecycle_sweeper_factory,
                shard_scavenger_sweeper_factory,
                shard_repair_sweeper_factory,
                shard_backfill_sweeper_factory,
                stream_session_sweeper_factory,
            ),
        )
    }

    #[cfg(test)]
    pub(super) fn new_with_lifecycle_sweeper_factory_for_storage_cluster<F>(
        storage_cluster: Arc<StorageCluster>,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: Option<StaticManagedKeyProvider>,
        lifecycle_sweeper_factory: F,
    ) -> Result<Self, ServerError>
    where
        F: FnOnce(
            &StorageClusterRouteHandle,
            ReadRuntime,
        ) -> Result<Arc<LifecycleSweeper>, ServerError>,
    {
        Self::new_with_shared_caches_and_lifecycle_sweeper_factory(
            Arc::clone(&storage_cluster),
            shared_caches_for_storage_cluster(&storage_cluster),
            region,
            sse_c_validator,
            managed_key_provider,
            lifecycle_sweeper_factory,
        )
    }

    #[cfg(test)]
    pub(super) fn new_with_background_sweeper_factories_for_storage_cluster<F, G, H, I, J>(
        storage_cluster: Arc<StorageCluster>,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: Option<StaticManagedKeyProvider>,
        background_sweepers: (bool, F, G, H, I, J),
    ) -> Result<Self, ServerError>
    where
        F: FnOnce(
            &StorageClusterRouteHandle,
            ReadRuntime,
        ) -> Result<Arc<LifecycleSweeper>, ServerError>,
        G: FnOnce(&StorageClusterRouteHandle) -> Result<Arc<ShardScavengerSweeper>, ServerError>,
        H: FnOnce(&StorageClusterRouteHandle) -> Result<Arc<ShardRepairSweeper>, ServerError>,
        I: FnOnce(&StorageClusterRouteHandle) -> Result<Arc<ShardBackfillSweeper>, ServerError>,
        J: FnOnce(&StorageClusterRouteHandle) -> Result<Arc<StreamSessionSweeper>, ServerError>,
    {
        Self::new_with_shared_caches_and_background_sweeper_factories(
            static_storage_cluster_route_handle(Arc::clone(&storage_cluster))?,
            Arc::clone(&storage_cluster),
            shared_caches_for_storage_cluster(&storage_cluster),
            region,
            sse_c_validator,
            managed_key_provider,
            background_sweepers,
        )
    }

    pub(super) fn new_with_shared_caches_and_lifecycle_sweeper_factory<F>(
        storage_cluster: Arc<StorageCluster>,
        shared_caches: Arc<CoordinatorSharedCaches>,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: Option<StaticManagedKeyProvider>,
        lifecycle_sweeper_factory: F,
    ) -> Result<Self, ServerError>
    where
        F: FnOnce(
            &StorageClusterRouteHandle,
            ReadRuntime,
        ) -> Result<Arc<LifecycleSweeper>, ServerError>,
    {
        Self::new_with_shared_caches_and_background_sweeper_factories(
            static_storage_cluster_route_handle(Arc::clone(&storage_cluster))?,
            storage_cluster,
            shared_caches,
            region,
            sse_c_validator,
            managed_key_provider,
            (
                BackgroundWorkerMode::all().object_reclaim_and_bucket_finalize,
                lifecycle_sweeper_factory,
                acquire_shard_scavenger_sweeper,
                acquire_shard_repair_sweeper,
                acquire_shard_backfill_sweeper,
                acquire_stream_session_sweeper,
            ),
        )
    }

    pub(super) fn new_with_shared_caches_and_background_sweeper_factories<F, G, H, I, J>(
        storage_handle: StorageClusterRouteHandle,
        storage_cluster: Arc<StorageCluster>,
        shared_caches: Arc<CoordinatorSharedCaches>,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: Option<StaticManagedKeyProvider>,
        background_sweepers: (bool, F, G, H, I, J),
    ) -> Result<Self, ServerError>
    where
        F: FnOnce(
            &StorageClusterRouteHandle,
            ReadRuntime,
        ) -> Result<Arc<LifecycleSweeper>, ServerError>,
        G: FnOnce(&StorageClusterRouteHandle) -> Result<Arc<ShardScavengerSweeper>, ServerError>,
        H: FnOnce(&StorageClusterRouteHandle) -> Result<Arc<ShardRepairSweeper>, ServerError>,
        I: FnOnce(&StorageClusterRouteHandle) -> Result<Arc<ShardBackfillSweeper>, ServerError>,
        J: FnOnce(&StorageClusterRouteHandle) -> Result<Arc<StreamSessionSweeper>, ServerError>,
    {
        Self::new_with_shared_caches_and_background_storage_and_sweeper_factories(
            shared_caches,
            CoordinatorStorageContext::shared(storage_handle, storage_cluster),
            region,
            sse_c_validator,
            managed_key_provider,
            background_sweepers,
        )
    }

    fn new_with_shared_caches_and_background_storage_and_sweeper_factories<F, G, H, I, J>(
        shared_caches: Arc<CoordinatorSharedCaches>,
        storage_context: CoordinatorStorageContext,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: Option<StaticManagedKeyProvider>,
        background_sweepers: (bool, F, G, H, I, J),
    ) -> Result<Self, ServerError>
    where
        F: FnOnce(
            &StorageClusterRouteHandle,
            ReadRuntime,
        ) -> Result<Arc<LifecycleSweeper>, ServerError>,
        G: FnOnce(&StorageClusterRouteHandle) -> Result<Arc<ShardScavengerSweeper>, ServerError>,
        H: FnOnce(&StorageClusterRouteHandle) -> Result<Arc<ShardRepairSweeper>, ServerError>,
        I: FnOnce(&StorageClusterRouteHandle) -> Result<Arc<ShardBackfillSweeper>, ServerError>,
        J: FnOnce(&StorageClusterRouteHandle) -> Result<Arc<StreamSessionSweeper>, ServerError>,
    {
        let CoordinatorStorageContext {
            foreground_handle: storage_handle,
            background_handle: background_storage_handle,
            foreground_cluster: storage_cluster,
            background_cluster: background_storage_cluster,
        } = storage_context;
        #[cfg(test)]
        let pg_topology =
            PgTopology::new(background_storage_cluster.test_pg_ids()).map_err(|reason| {
                ServerError::InternalError {
                    reason: reason.to_string(),
                }
            })?;
        let payload_buffer_pool =
            PayloadBufferPool::new(storage_cluster.default_payload_ec_shape());
        let read_runtime = ReadRuntime {
            storage: super::read_core::ReadStorage::Cluster(Arc::clone(
                &background_storage_cluster,
            )),
            #[cfg(test)]
            pg_topology: pg_topology.clone(),
            payload_buffer_pool: Arc::clone(&payload_buffer_pool),
            sse_c_validator: sse_c_validator.clone(),
            managed_key_provider: managed_key_provider.clone(),
        };
        let (
            start_reclaim_worker,
            lifecycle_sweeper_factory,
            shard_scavenger_sweeper_factory,
            shard_repair_sweeper_factory,
            shard_backfill_sweeper_factory,
            stream_session_sweeper_factory,
        ) = background_sweepers;
        let reclaim_sweeper = if start_reclaim_worker {
            ReclaimSweeper::acquire_shared(&background_storage_handle).map_err(|error| {
                ServerError::InternalError {
                    reason: error.to_string(),
                }
            })?
        } else {
            ReclaimSweeper::disabled(background_storage_handle.clone())
        };
        let lifecycle_sweeper =
            lifecycle_sweeper_factory(&background_storage_handle, read_runtime.clone())?;
        let shard_scavenger_sweeper = shard_scavenger_sweeper_factory(&background_storage_handle)?;
        let shard_repair_sweeper = shard_repair_sweeper_factory(&background_storage_handle)?;
        let shard_backfill_sweeper = shard_backfill_sweeper_factory(&background_storage_handle)?;
        let stream_session_sweeper = stream_session_sweeper_factory(&background_storage_handle)?;
        Ok(Self {
            storage_node: storage_handle,
            shared_caches,
            payload_buffer_pool,
            region,
            sse_c_validator,
            managed_key_provider,
            _reclaim_sweeper: reclaim_sweeper,
            _shard_scavenger_sweeper: shard_scavenger_sweeper,
            _shard_repair_sweeper: shard_repair_sweeper,
            _shard_backfill_sweeper: shard_backfill_sweeper,
            _stream_session_sweeper: stream_session_sweeper,
            _lifecycle_sweeper: lifecycle_sweeper,
        })
    }

    pub(super) fn read_runtime(&self) -> ReadRuntime {
        let storage_node = self.storage_node();
        self.read_runtime_for_storage_node(storage_node)
    }

    pub(super) fn read_runtime_for_storage_node(
        &self,
        storage_node: Arc<StorageCluster>,
    ) -> ReadRuntime {
        ReadRuntime {
            storage: super::read_core::ReadStorage::Cluster(Arc::clone(&storage_node)),
            #[cfg(test)]
            pg_topology: PgTopology::new(storage_node.test_pg_ids())
                .expect("coordinator storage node should expose a valid PG topology"),
            payload_buffer_pool: Arc::clone(&self.payload_buffer_pool),
            sse_c_validator: self.sse_c_validator.clone(),
            managed_key_provider: self.managed_key_provider.clone(),
        }
    }

    pub(super) fn read_runtime_for_retained_payload_read(
        &self,
        retained_payload_read: storage::RetainedObjectPayloadRead,
        bucket: &storage::BucketName,
        key: &storage::ObjectKey,
        generation_id: storage::GenerationId,
        snapshot: &storage::ObjectReadSnapshot,
    ) -> Result<ReadRuntime, ServerError> {
        if !retained_payload_read.covers_complete_object_payload_layout(
            bucket,
            key,
            generation_id,
            snapshot
                .object_segments
                .iter()
                .chain(snapshot.multipart_part_segments.iter()),
        ) {
            return Err(ServerError::InternalError {
                reason: "retained payload authority does not cover the complete object layout"
                    .to_string(),
            });
        }
        Ok(ReadRuntime {
            storage: super::read_core::ReadStorage::Retained(Arc::new(retained_payload_read)),
            #[cfg(test)]
            pg_topology: PgTopology::new(self.storage_node().test_pg_ids())
                .expect("coordinator storage node should expose a valid PG topology"),
            payload_buffer_pool: Arc::clone(&self.payload_buffer_pool),
            sse_c_validator: self.sse_c_validator.clone(),
            managed_key_provider: self.managed_key_provider.clone(),
        })
    }

    pub(super) fn storage_node(&self) -> Arc<StorageCluster> {
        self.storage_node.current()
    }

    pub fn storage_node_for_request(&self) -> Arc<StorageCluster> {
        self.storage_node()
    }

    pub fn admit_storage_route_for_request(
        &self,
    ) -> Result<StorageClusterRouteAdmission, ServerError> {
        self.storage_node
            .admit_current_route()
            .map_err(map_store_error)
    }

    pub(super) fn require_storage_route_admission(
        &self,
        admission: &StorageClusterRouteAdmission,
    ) -> Result<(), ServerError> {
        self.storage_node
            .require_admission_valid_now(admission)
            .map_err(map_store_error)
    }

    pub fn shares_storage_route_admission_with(&self, other: &Self) -> bool {
        self.storage_node
            .shares_route_admission_with(&other.storage_node)
    }

    #[cfg(test)]
    #[allow(clippy::used_underscore_binding)]
    pub(crate) fn background_worker_mode_for_test(&self) -> BackgroundWorkerMode {
        use std::sync::atomic::Ordering;

        BackgroundWorkerMode {
            object_reclaim_and_bucket_finalize: self._reclaim_sweeper.test_is_enabled(),
            lifecycle: !self._lifecycle_sweeper.stop.load(Ordering::SeqCst),
            shard_scavenger: self._shard_scavenger_sweeper.test_is_enabled(),
            shard_repair: self._shard_repair_sweeper.test_is_enabled(),
            shard_backfill: self._shard_backfill_sweeper.test_is_enabled(),
            stream_session: self._stream_session_sweeper.test_is_enabled(),
        }
    }

    #[cfg(test)]
    pub(super) fn run_lifecycle_sweep_at(
        &self,
        now_millis: u64,
    ) -> Result<LifecycleSweepStats, ServerError> {
        storage::clock::with_time_override(now_millis, || {
            self.read_runtime().run_lifecycle_sweep_at(now_millis)
        })
    }

    /// Run one lifecycle sweep at a caller-provided timestamp.
    ///
    /// This is a local integration-test hook used by `s3-local-tests` so
    /// lifecycle execution can be driven deterministically without sleeps.
    pub fn run_lifecycle_sweep_for_test(&self, now_millis: u64) -> Result<(), ServerError> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let stats = storage::clock::with_time_override(now_millis, || {
                self.read_runtime().run_lifecycle_sweep_at(now_millis)
            })?;
            if stats.busy_claims == 0 && stats.skipped_expired_delete_markers == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ServerError::InternalError {
                    reason: format!(
                        "lifecycle test sweep still had {} busy claims and {} skipped expired delete-marker candidates after bounded wait",
                        stats.busy_claims, stats.skipped_expired_delete_markers
                    ),
                });
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    pub fn region(&self) -> &str {
        &self.region
    }
}
