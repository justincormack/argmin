#[cfg(test)]
use super::runtime::LifecycleSweepStats;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use ec::{EcConfig, ErasureCodec};

use super::authz_types::{AuthorizedPutObjectWrite, ValidatedBucket};
use super::payload::{EncodeScratchPool, PayloadBufferPool};
use super::read_core::ReadRuntime;
use super::request_types::AuthorizePutObjectRequest;
use super::response_types::BucketSummary;
use super::runtime::{LifecycleSweeper, ReclaimSweeper};
#[cfg(test)]
use super::{trusted_bucket_name, trusted_object_key};
use super::{Coordinator, PgTopology};
use crate::error::ServerError;
use crate::sse::{SseCustomerValidatorConfig, StaticManagedKeyProvider};
#[cfg(test)]
use storage::ObjectKey;
use storage::{
    BucketFastPathInfo, BucketInfo, BucketName, BucketState, ReclaimWorkItem, SessionId,
    SharedStorageNode,
};

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

    pub(super) fn bucket_summary_fast(info: BucketFastPathInfo) -> BucketSummary {
        BucketSummary {
            name: info.name,
            owner_principal: info.owner_principal,
            owner_canonical_id: info.owner_canonical_id,
            created_at: info.created_at,
            acl_grants: info.acl_grants,
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

    #[cfg(test)]
    pub(super) fn unchecked_active_bucket_summary(
        &self,
        name: &str,
    ) -> Result<BucketSummary, ServerError> {
        let name = trusted_bucket_name(name);
        if let Some(info) = self.storage_node.get_bucket_fast_path(&name) {
            if info.state == BucketState::Active {
                return Ok(Self::bucket_summary_fast(info));
            }
            return Err(ServerError::BucketNotFound {
                name: name.to_string(),
            });
        }

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
        if let Some(info) = self.storage_node.get_bucket_fast_path(name) {
            if info.state == BucketState::Active {
                return Ok(Self::bucket_summary_fast(info));
            }
            return Err(ServerError::BucketNotFound {
                name: name.to_string(),
            });
        }

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

    /// Create a new coordinator.
    pub fn new(
        storage_node: Arc<SharedStorageNode>,
        ec_config: EcConfig,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
    ) -> Result<Self, ServerError> {
        let lifecycle_sweeper_factory =
            |storage_node: &Arc<SharedStorageNode>, read_runtime: ReadRuntime| {
                LifecycleSweeper::acquire_shared(storage_node, read_runtime)
            };
        Self::new_with_lifecycle_sweeper_factory(
            storage_node,
            ec_config,
            region,
            sse_c_validator,
            None,
            lifecycle_sweeper_factory,
        )
    }

    /// Create a new coordinator with a managed object-encryption wrapping-key provider.
    pub fn new_with_managed_key_provider(
        storage_node: Arc<SharedStorageNode>,
        ec_config: EcConfig,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: StaticManagedKeyProvider,
    ) -> Result<Self, ServerError> {
        let lifecycle_sweeper_factory =
            |storage_node: &Arc<SharedStorageNode>, read_runtime: ReadRuntime| {
                LifecycleSweeper::acquire_shared(storage_node, read_runtime)
            };
        Self::new_with_lifecycle_sweeper_factory(
            storage_node,
            ec_config,
            region,
            sse_c_validator,
            Some(managed_key_provider),
            lifecycle_sweeper_factory,
        )
    }

    pub(super) fn new_with_lifecycle_sweeper_factory<F>(
        storage_node: Arc<SharedStorageNode>,
        ec_config: EcConfig,
        region: String,
        sse_c_validator: Option<SseCustomerValidatorConfig>,
        managed_key_provider: Option<StaticManagedKeyProvider>,
        lifecycle_sweeper_factory: F,
    ) -> Result<Self, ServerError>
    where
        F: FnOnce(
            &Arc<SharedStorageNode>,
            ReadRuntime,
        ) -> Result<Arc<LifecycleSweeper>, ServerError>,
    {
        let ec_codec = Arc::new(ErasureCodec::new(ec_config)?);
        let pg_topology = PgTopology::new(storage_node.pg_ids()).map_err(|reason| {
            ServerError::InternalError {
                reason: reason.to_string(),
            }
        })?;
        let payload_buffer_pool = PayloadBufferPool::new(ec_config);
        let read_runtime = ReadRuntime {
            storage_node: Arc::clone(&storage_node),
            ec_codec: Arc::clone(&ec_codec),
            ec_config,
            #[cfg(test)]
            pg_topology: pg_topology.clone(),
            payload_buffer_pool: Arc::clone(&payload_buffer_pool),
            sse_c_validator: sse_c_validator.clone(),
            managed_key_provider: managed_key_provider.clone(),
        };
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker_node = Arc::clone(&storage_node);
        let reclaim_runtime = read_runtime.clone();
        let handle = std::thread::Builder::new()
            .name("argmin-reclaim".to_string())
            .spawn(move || {
                while let Some(work) = worker_node.wait_for_reclaim_work(&worker_stop) {
                    match work {
                        ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)) => {
                            let _ = reclaim_runtime.try_reclaim_object_payload_for(
                                &bucket,
                                &key,
                                generation_id,
                            );
                        }
                        ReclaimWorkItem::BucketDelete(bucket) => {
                            let _ = reclaim_runtime.try_finalize_bucket_delete_for(&bucket);
                        }
                    }
                }
            })
            .map_err(|e| ServerError::InternalError {
                reason: format!("failed to start reclaim worker: {e}"),
            })?;
        let lifecycle_sweeper = lifecycle_sweeper_factory(&storage_node, read_runtime.clone())?;
        let sweeper_storage_node = Arc::clone(&storage_node);
        Ok(Self {
            storage_node,
            bucket_policy_cache: RwLock::new(HashMap::new()),
            bucket_lifecycle_cache: RwLock::new(HashMap::new()),
            pg_topology,
            ec_codec,
            ec_config,
            encode_scratch_pool: EncodeScratchPool::new(ec_config),
            payload_buffer_pool,
            region,
            sse_c_validator,
            managed_key_provider,
            _reclaim_sweeper: ReclaimSweeper {
                storage_node: sweeper_storage_node,
                stop,
                handle: Some(handle),
            },
            _lifecycle_sweeper: lifecycle_sweeper,
        })
    }

    pub(super) fn read_runtime(&self) -> ReadRuntime {
        ReadRuntime {
            storage_node: Arc::clone(&self.storage_node),
            ec_codec: Arc::clone(&self.ec_codec),
            ec_config: self.ec_config,
            #[cfg(test)]
            pg_topology: self.pg_topology.clone(),
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

    #[cfg(test)]
    pub(super) fn object_pg_id_for(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.pg_topology.object_pg_for(bucket, key)
    }

    #[cfg(test)]
    pub(super) fn object_pg_id(&self, bucket: &str, key: &str) -> u32 {
        self.object_pg_id_for(&trusted_bucket_name(bucket), &trusted_object_key(key))
    }

    pub(super) fn shard_pg_id_raw(&self, bucket: &str, key: &str, generation: u64) -> u32 {
        self.pg_topology.shard_pg(bucket, key, generation)
    }
}
