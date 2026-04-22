#[cfg(test)]
use super::runtime::LifecycleSweepStats;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, MutexGuard, RwLock};

use ec::{EcConfig, ErasureCodec};

use super::authz_types::{AuthorizedPutObjectWrite, ValidatedBucket};
use super::payload::{EncodeScratchPool, PayloadBufferPool};
use super::pg_guards::{BucketObjectPgGuards, TwoPgGuards};
use super::read_core::ReadRuntime;
use super::request_types::{AuthorizePutObjectRequest, BucketScopedRequest};
use super::response_types::BucketSummary;
use super::runtime::{LifecycleSweeper, ReclaimSweeper};
#[cfg(test)]
use super::{trusted_bucket_name, trusted_object_key};
use super::{Coordinator, PgTopology};
use crate::error::ServerError;
use crate::sse::{SseCustomerValidatorConfig, StaticManagedKeyProvider};
#[cfg(test)]
use storage::GenerationId;
use storage::PgMetadataStore;
use storage::{
    BucketFastPathInfo, BucketInfo, BucketName, BucketState, CreateStreamUploadReq, ObjectKey,
    ReclaimWorkItem, SessionId, SharedStorageNode, StreamUploadTarget, UploadId,
};

impl Coordinator {
    pub(super) fn create_stream_put_session_for_authorized_write(
        &self,
        authorized: &AuthorizedPutObjectWrite,
    ) -> Result<SessionId, ServerError> {
        let stored_encryption = authorized.write_encryption.object_encryption();

        let rng = ring::rand::SystemRandom::new();
        let mut id_bytes = [0u8; 16];
        ring::rand::SecureRandom::fill(&rng, &mut id_bytes).map_err(|_| {
            ServerError::InternalError {
                reason: "failed to generate session ID".to_string(),
            }
        })?;
        let encoded = id_bytes.iter().fold(String::with_capacity(32), |mut s, b| {
            use std::fmt::Write;
            write!(s, "{b:02x}").unwrap();
            s
        });
        let session_id =
            SessionId::try_from(encoded).expect("generated stream put session IDs must be valid");

        let meta_pg_id = self.object_pg_id_for(authorized.bucket_typed(), authorized.key_typed());
        let pg = self.storage_node.get_pg(meta_pg_id)?;
        pg.create_stream_upload(&CreateStreamUploadReq {
            session_id: session_id.clone(),
            bucket: authorized.bucket_typed().clone(),
            key: authorized.key_typed().clone(),
            target: StreamUploadTarget::PutObject,
            encryption: stored_encryption,
        })?;

        Ok(session_id)
    }

    pub fn prepare_put_object_write(
        &self,
        req: &AuthorizePutObjectRequest<'_>,
    ) -> Result<AuthorizedPutObjectWrite, ServerError> {
        self.authorize_put_object_write(req)
    }

    fn unchecked_bucket_write_reservation_for(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketSummary, ServerError> {
        loop {
            let bucket_pg = self.get_bucket_pg_for(bucket)?;
            match storage::PgMetadataStore::acquire_bucket_write_reservation(&*bucket_pg, bucket) {
                Ok(info) => {
                    self.storage_node.upsert_bucket_fast_path((&info).into());
                    return Ok(Self::bucket_summary(info));
                }
                Err(storage::MetadataError::BucketWriteDraining) => {
                    drop(bucket_pg);
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(storage::MetadataError::BucketNotFound { name }) => {
                    return Err(ServerError::BucketNotFound {
                        name: name.to_string(),
                    });
                }
                Err(other) => return Err(ServerError::Metadata(other)),
            }
        }
    }

    fn release_bucket_write_reservation_for(&self, bucket: &BucketName) -> Result<(), ServerError> {
        let bucket_pg = self.get_bucket_pg_for(bucket)?;
        storage::PgMetadataStore::release_bucket_write_reservation(&*bucket_pg, bucket).map_err(
            |e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            },
        )
    }

    pub(super) fn with_unchecked_bucket_write_reservation_for<T>(
        &self,
        bucket: &BucketName,
        action: impl FnOnce(BucketSummary) -> Result<T, ServerError>,
    ) -> Result<T, ServerError> {
        let bucket_info = self.unchecked_bucket_write_reservation_for(bucket)?;
        let result = action(bucket_info);
        let release_result = self.release_bucket_write_reservation_for(bucket);
        match (result, release_result) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(err)) => Err(err),
            (Err(err), Ok(())) => Err(err),
            (Err(err), Err(_)) => Err(err),
        }
    }

    pub(super) fn with_bucket_write_reservation_for<R, T>(
        &self,
        req: &R,
        action: impl FnOnce(ValidatedBucket) -> Result<T, ServerError>,
    ) -> Result<T, ServerError>
    where
        R: BucketScopedRequest + ?Sized,
    {
        let expected_bucket_owner = req.expected_bucket_owner();
        self.with_unchecked_bucket_write_reservation_for(req.bucket_name_typed(), |bucket_info| {
            let bucket_info =
                Self::validate_expected_bucket_owner(bucket_info, expected_bucket_owner)?;
            action(bucket_info)
        })
    }

    pub(super) fn next_completed_multipart_upload_order_for_bucket_name(
        &self,
        bucket: &BucketName,
    ) -> Result<u64, ServerError> {
        let bucket_pg = self.get_bucket_pg_for(bucket)?;
        bucket_pg
            .next_completed_multipart_upload_order_for_bucket(bucket)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
    }

    pub(super) fn prune_completed_multipart_uploads_for_bucket_with_limit(
        &self,
        bucket: &str,
        keep: usize,
    ) -> Result<(), ServerError> {
        let mut uploads: Vec<(u32, UploadId, u64)> = Vec::new();
        self.pg_topology.for_each_pg(|pg_id| {
            let pg = self
                .storage_node
                .get_pg(pg_id)
                .map_err(ServerError::Store)?;
            let local = pg
                .list_completed_multipart_uploads_for_bucket(bucket)
                .map_err(ServerError::Metadata)?;
            uploads.extend(
                local
                    .into_iter()
                    .map(|(upload_id, completion_order)| (pg_id, upload_id, completion_order)),
            );
            Ok::<(), ServerError>(())
        })?;

        uploads.sort_by_key(|entry| std::cmp::Reverse(entry.2));
        for (pg_id, upload_id, _) in uploads.into_iter().skip(keep) {
            let pg = self
                .storage_node
                .get_pg(pg_id)
                .map_err(ServerError::Store)?;
            pg.delete_completed_multipart_upload(&upload_id)
                .map_err(ServerError::Metadata)?;
        }
        Ok(())
    }

    pub(super) fn begin_bucket_write_drain_for(
        &self,
        bucket: &BucketName,
    ) -> Result<(), ServerError> {
        loop {
            let bucket_pg = self.get_bucket_pg_for(bucket)?;
            match storage::PgMetadataStore::begin_bucket_write_drain(&*bucket_pg, bucket) {
                Ok(()) => return Ok(()),
                Err(storage::MetadataError::BucketWriteDraining) => {
                    drop(bucket_pg);
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(storage::MetadataError::BucketNotFound { name }) => {
                    return Err(ServerError::BucketNotFound {
                        name: name.to_string(),
                    });
                }
                Err(other) => return Err(ServerError::Metadata(other)),
            }
        }
    }

    pub(super) fn wait_for_bucket_write_reservations_to_drain_for(
        &self,
        bucket: &BucketName,
    ) -> Result<(), ServerError> {
        loop {
            let bucket_pg = self.get_bucket_pg_for(bucket)?;
            let info =
                storage::PgMetadataStore::head_bucket(&*bucket_pg, bucket).map_err(
                    |e| match e {
                        storage::MetadataError::BucketNotFound { name } => {
                            ServerError::BucketNotFound {
                                name: name.to_string(),
                            }
                        }
                        other => ServerError::Metadata(other),
                    },
                )?;
            if info.active_write_reservations == 0 {
                return Ok(());
            }
            drop(bucket_pg);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
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

        let bucket_pg = self.get_bucket_pg(name.as_str())?;
        self.load_active_bucket_summary_from_pg(&bucket_pg, &name)
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

        let bucket_pg = self.get_bucket_pg_for(name)?;
        self.load_active_bucket_summary_from_pg(&bucket_pg, name)
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

    pub(super) fn bucket_pg_id_for(&self, bucket: &BucketName) -> u32 {
        self.pg_topology.bucket_pg_for(bucket)
    }

    pub(super) fn object_pg_id_for(&self, bucket: &BucketName, key: &ObjectKey) -> u32 {
        self.pg_topology.object_pg_for(bucket, key)
    }

    #[cfg(test)]
    pub(super) fn bucket_pg_id(&self, bucket: &str) -> u32 {
        self.bucket_pg_id_for(&trusted_bucket_name(bucket))
    }

    #[cfg(test)]
    pub(super) fn object_pg_id(&self, bucket: &str, key: &str) -> u32 {
        self.object_pg_id_for(&trusted_bucket_name(bucket), &trusted_object_key(key))
    }

    pub(super) fn shard_pg_id_raw(&self, bucket: &str, key: &str, generation: u64) -> u32 {
        self.pg_topology.shard_pg(bucket, key, generation)
    }

    #[cfg(test)]
    pub(super) fn shard_pg_id(&self, bucket: &str, key: &str, generation_id: GenerationId) -> u32 {
        self.shard_pg_id_raw(bucket, key, generation_id.get())
    }

    #[cfg(test)]
    pub(super) fn get_bucket_pg(
        &self,
        bucket: &str,
    ) -> Result<MutexGuard<'_, storage::PgStore>, ServerError> {
        let pg_id = self.bucket_pg_id(bucket);
        Ok(self.storage_node.get_pg(pg_id)?)
    }

    pub(super) fn get_bucket_pg_for(
        &self,
        bucket: &BucketName,
    ) -> Result<MutexGuard<'_, storage::PgStore>, ServerError> {
        let pg_id = self.bucket_pg_id_for(bucket);
        Ok(self.storage_node.get_pg(pg_id)?)
    }

    pub(super) fn lock_bucket_and_object_pgs_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<BucketObjectPgGuards<'_>, ServerError> {
        let (bucket_guard, object_guard) = self
            .storage_node
            .lock_two_pgs(
                self.bucket_pg_id_for(bucket),
                self.object_pg_id_for(bucket, key),
            )
            .map_err(ServerError::Store)?;
        Ok(BucketObjectPgGuards::new(bucket_guard, object_guard))
    }

    pub(super) fn lock_object_pgs_for_write_ids(
        &self,
        meta_pg_id: u32,
        shard_pg_id: u32,
    ) -> Result<TwoPgGuards<'_>, ServerError> {
        let (meta_guard, shard_guard) = self
            .storage_node
            .lock_two_pgs(meta_pg_id, shard_pg_id)
            .map_err(ServerError::Store)?;
        Ok(TwoPgGuards::new(meta_guard, shard_guard))
    }

    pub(super) fn load_active_bucket_summary_from_pg(
        &self,
        bucket_pg: &storage::PgStore,
        name: &BucketName,
    ) -> Result<BucketSummary, ServerError> {
        let info = storage::PgMetadataStore::head_bucket(bucket_pg, name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })?;
        self.storage_node.upsert_bucket_fast_path((&info).into());
        Ok(Self::bucket_summary(info))
    }
}
