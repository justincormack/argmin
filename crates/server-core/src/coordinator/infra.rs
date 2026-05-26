#[cfg(test)]
use super::runtime::LifecycleSweepStats;
use std::sync::Arc;

use super::authz_types::{AuthorizedPutObjectWrite, ValidatedBucket};
use super::payload::PayloadBufferPool;
use super::read_core::ReadRuntime;
use super::request_types::AuthorizePutObjectRequest;
use super::response_types::{BucketSummary, ModernBucketSummary};
use super::runtime::{LifecycleSweeper, ReclaimSweeper, ShardScavengerSweeper};
#[cfg(test)]
use super::trusted_bucket_name;
use super::{shared_caches_for_storage_cluster, Coordinator, CoordinatorSharedCaches};
use crate::error::ServerError;
use crate::sse::{SseCustomerValidatorConfig, StaticManagedKeyProvider};
#[cfg(test)]
use storage::PgTopology;
use storage::{BucketFastPathInfo, BucketInfo, BucketName, BucketState, SessionId, StorageCluster};

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

    pub(super) fn create_stream_put_session_for_authorized_write(
        &self,
        authorized: &AuthorizedPutObjectWrite,
    ) -> Result<SessionId, ServerError> {
        let stored_encryption = authorized.write_encryption.object_encryption();
        let session_id = Self::random_session_id("failed to generate session ID")?;
        self.storage_node
            .create_put_object_stream_session_record(
                authorized.bucket_typed(),
                authorized.key_typed(),
                &session_id,
                stored_encryption,
            )
            .map_err(|error| match error {
                storage::ObjectPgActionError::Store(error) => ServerError::Store(error),
                storage::ObjectPgActionError::InvalidRequest { reason } => {
                    ServerError::InvalidRequest { reason }
                }
                storage::ObjectPgActionError::Metadata(error) => ServerError::Metadata(error),
                storage::ObjectPgActionError::StaleObjectReadSubject => {
                    ServerError::InternalError {
                        reason: "stale object read subject escaped storage retry loop".to_string(),
                    }
                }
                storage::ObjectPgActionError::StaleDirectPutCommitSnapshot => {
                    ServerError::InternalError {
                        reason: "stale direct PUT commit snapshot escaped storage retry loop"
                            .to_string(),
                    }
                }
                storage::ObjectPgActionError::StaleStreamFinalizeSnapshot => {
                    ServerError::InternalError {
                        reason: "stale stream finalize snapshot escaped storage retry loop"
                            .to_string(),
                    }
                }
                storage::ObjectPgActionError::StaleMultipartCompletionSnapshot => {
                    ServerError::InternalError {
                        reason: "stale multipart completion snapshot escaped storage retry loop"
                            .to_string(),
                    }
                }
            })?;

        Ok(session_id)
    }

    pub fn prepare_put_object_write(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        self.authorize_put_object_write(req)
    }

    #[cfg(test)]
    pub(super) fn prune_completed_multipart_uploads_for_bucket_with_limit(
        &self,
        bucket: &BucketName,
        keep: usize,
    ) -> Result<(), ServerError> {
        self.storage_node
            .prune_completed_multipart_uploads_for_bucket_with_limit(bucket, keep)
            .map_err(Coordinator::map_object_pg_action_error)
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
            .storage_node
            .head_bucket_info(&name)
            .map_err(|error| match error {
                storage::BucketSnapshotLoadError::Store(error) => ServerError::Store(error),
                storage::BucketSnapshotLoadError::Metadata(
                    storage::MetadataError::BucketNotFound { name },
                ) => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                storage::BucketSnapshotLoadError::Metadata(other) => ServerError::Metadata(other),
            })?;
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
        let info = self
            .storage_node
            .head_bucket_info(name)
            .map_err(|error| match error {
                storage::BucketSnapshotLoadError::Store(error) => ServerError::Store(error),
                storage::BucketSnapshotLoadError::Metadata(
                    storage::MetadataError::BucketNotFound { name },
                ) => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                storage::BucketSnapshotLoadError::Metadata(other) => ServerError::Metadata(other),
            })?;
        if info.state != BucketState::Active {
            return Err(ServerError::BucketNotFound {
                name: name.to_string(),
            });
        }
        Ok(Self::bucket_summary(info))
    }

    pub(super) fn checked_active_bucket_summary_for(
        &self,
        name: &BucketName,
        expected_bucket_owner: Option<&str>,
    ) -> Result<ValidatedBucket, ServerError> {
        Self::validate_expected_bucket_owner(
            self.unchecked_active_bucket_summary_for(name)?,
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
            |storage_cluster: &Arc<StorageCluster>, read_runtime: ReadRuntime| {
                LifecycleSweeper::acquire_shared(storage_cluster, read_runtime)
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
            |storage_cluster: &Arc<StorageCluster>, read_runtime: ReadRuntime| {
                LifecycleSweeper::acquire_shared(storage_cluster, read_runtime)
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

    #[cfg(test)]
    pub(super) fn new_with_lifecycle_sweeper_factory_for_storage_cluster<F>(
        storage_cluster: Arc<StorageCluster>,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: Option<StaticManagedKeyProvider>,
        lifecycle_sweeper_factory: F,
    ) -> Result<Self, ServerError>
    where
        F: FnOnce(&Arc<StorageCluster>, ReadRuntime) -> Result<Arc<LifecycleSweeper>, ServerError>,
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
    pub(super) fn new_with_background_sweeper_factories_for_storage_cluster<F, G>(
        storage_cluster: Arc<StorageCluster>,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: Option<StaticManagedKeyProvider>,
        background_sweepers: (bool, F, G),
    ) -> Result<Self, ServerError>
    where
        F: FnOnce(&Arc<StorageCluster>, ReadRuntime) -> Result<Arc<LifecycleSweeper>, ServerError>,
        G: FnOnce(&Arc<StorageCluster>) -> Result<Arc<ShardScavengerSweeper>, ServerError>,
    {
        Self::new_with_shared_caches_and_background_sweeper_factories(
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
        F: FnOnce(&Arc<StorageCluster>, ReadRuntime) -> Result<Arc<LifecycleSweeper>, ServerError>,
    {
        Self::new_with_shared_caches_and_background_sweeper_factories(
            storage_cluster,
            shared_caches,
            region,
            sse_c_validator,
            managed_key_provider,
            (
                true,
                lifecycle_sweeper_factory,
                ShardScavengerSweeper::acquire_shared,
            ),
        )
    }

    pub(super) fn new_with_shared_caches_and_background_sweeper_factories<F, G>(
        storage_cluster: Arc<StorageCluster>,
        shared_caches: Arc<CoordinatorSharedCaches>,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: Option<StaticManagedKeyProvider>,
        background_sweepers: (bool, F, G),
    ) -> Result<Self, ServerError>
    where
        F: FnOnce(&Arc<StorageCluster>, ReadRuntime) -> Result<Arc<LifecycleSweeper>, ServerError>,
        G: FnOnce(&Arc<StorageCluster>) -> Result<Arc<ShardScavengerSweeper>, ServerError>,
    {
        #[cfg(test)]
        let pg_topology = PgTopology::new(storage_cluster.test_pg_ids()).map_err(|reason| {
            ServerError::InternalError {
                reason: reason.to_string(),
            }
        })?;
        let payload_buffer_pool =
            PayloadBufferPool::new(storage_cluster.default_payload_ec_shape());
        let read_runtime = ReadRuntime {
            storage_node: Arc::clone(&storage_cluster),
            #[cfg(test)]
            pg_topology: pg_topology.clone(),
            payload_buffer_pool: Arc::clone(&payload_buffer_pool),
            sse_c_validator: sse_c_validator.clone(),
            managed_key_provider: managed_key_provider.clone(),
        };
        let (start_reclaim_worker, lifecycle_sweeper_factory, shard_scavenger_sweeper_factory) =
            background_sweepers;
        let reclaim_sweeper = if start_reclaim_worker {
            ReclaimSweeper::spawn(Arc::clone(&storage_cluster), read_runtime.clone())?
        } else {
            ReclaimSweeper::disabled(Arc::clone(&storage_cluster))
        };
        let lifecycle_sweeper = lifecycle_sweeper_factory(&storage_cluster, read_runtime.clone())?;
        let shard_scavenger_sweeper = shard_scavenger_sweeper_factory(&storage_cluster)?;
        Ok(Self {
            storage_node: storage_cluster,
            shared_caches,
            payload_buffer_pool,
            region,
            sse_c_validator,
            managed_key_provider,
            _reclaim_sweeper: reclaim_sweeper,
            _shard_scavenger_sweeper: shard_scavenger_sweeper,
            _lifecycle_sweeper: lifecycle_sweeper,
        })
    }

    pub(super) fn read_runtime(&self) -> ReadRuntime {
        ReadRuntime {
            storage_node: Arc::clone(&self.storage_node),
            #[cfg(test)]
            pg_topology: PgTopology::new(self.storage_node.test_pg_ids())
                .expect("coordinator storage node should expose a valid PG topology"),
            payload_buffer_pool: Arc::clone(&self.payload_buffer_pool),
            sse_c_validator: self.sse_c_validator.clone(),
            managed_key_provider: self.managed_key_provider.clone(),
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
        storage::clock::with_time_override(now_millis, || {
            self.read_runtime().run_lifecycle_sweep_at(now_millis)
        })?;
        Ok(())
    }

    pub fn region(&self) -> &str {
        &self.region
    }
}
