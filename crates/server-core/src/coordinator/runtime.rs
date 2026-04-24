use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use ec::{EcConfig, ErasureCodec};
use s3_types::BucketLifecycleConfiguration;
use storage::traits::ShardStore;
use storage::{
    BucketInfo, BucketName, EcShape, GenerationId, MultipartReclaimPartRecord, ObjectEncryption,
    ObjectKey, OwnerIdentity, ShardKey, SharedStorageNode, UploadId, UploadState, VersionId,
};

use super::payload::{PooledPayloadBuffer, SharedPayloadBuffer};
use super::read_core::{
    MultipartReader, PayloadLease, ReadChunk, ReadRuntime, SegmentListReader, SegmentPayloadRecord,
};
#[cfg(test)]
use super::test_hooks::maybe_run_object_segments_first_segment_hook;
#[cfg(feature = "deep-tracing")]
use super::TRACE_TARGET;
use super::{lock_mutex_unpoisoned, Coordinator, LIFECYCLE_SWEEP_INTERVAL_MILLIS};
#[cfg(test)]
use super::{trusted_bucket_name, trusted_object_key};
use crate::error::ServerError;
use crate::pg::object_key_hash;
use crate::sse::{
    decrypt_managed_encryption_segment, decrypt_sse_customer_segment, SseCustomerRequest,
    SseCustomerSegmentScope,
};

static LIFECYCLE_SWEEPER_REGISTRY: OnceLock<Mutex<HashMap<usize, Weak<LifecycleSweeper>>>> =
    OnceLock::new();

/// The coordinator ties together EC, storage, and metadata.
pub(super) struct ReclaimSweeper {
    pub(super) storage_node: Arc<SharedStorageNode>,
    pub(super) stop: Arc<AtomicBool>,
    pub(super) handle: Option<JoinHandle<()>>,
}

pub(super) struct LifecycleSweeper {
    pub(super) stop: Arc<AtomicBool>,
    pub(super) wake: Arc<(Mutex<bool>, Condvar)>,
    pub(super) handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LifecycleSweepStats {
    pub(super) scanned_buckets: u64,
    pub(super) expired_current_objects: u64,
    pub(super) expired_noncurrent_versions: u64,
    pub(super) expired_delete_markers: u64,
    pub(super) aborted_multipart_uploads: u64,
}

impl Drop for ReclaimSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.storage_node.wake_reclaim_workers();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for LifecycleSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        *lock_mutex_unpoisoned(&self.wake.0) = true;
        self.wake.1.notify_all();
        if let Some(handle) = lock_mutex_unpoisoned(&self.handle).take() {
            let _ = handle.join();
        }
    }
}

impl LifecycleSweeper {
    pub(super) fn acquire_shared(
        storage_node: &Arc<SharedStorageNode>,
        runtime: ReadRuntime,
    ) -> Result<Arc<Self>, ServerError> {
        let registry = LIFECYCLE_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry: std::sync::MutexGuard<'_, HashMap<usize, Weak<LifecycleSweeper>>> =
            lock_mutex_unpoisoned(registry);
        registry.retain(|_, sweeper| sweeper.upgrade().is_some());

        let key = Arc::as_ptr(storage_node) as usize;
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(existing);
        }

        let sweeper = Self::spawn(runtime)?;
        registry.insert(key, Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(runtime: ReadRuntime) -> Result<Arc<Self>, ServerError> {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let sweeper = Arc::new(Self {
            stop: Arc::clone(&stop),
            wake: Arc::clone(&wake),
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-lifecycle".to_string())
            .spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    let _ = runtime.run_lifecycle_sweep_at(Coordinator::now_millis());
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let stop_guard = lock_mutex_unpoisoned(&wake.0);
                    if *stop_guard {
                        break;
                    }
                    let _ = wake
                        .1
                        .wait_timeout_while(
                            stop_guard,
                            Duration::from_millis(LIFECYCLE_SWEEP_INTERVAL_MILLIS),
                            |stop_requested| !*stop_requested,
                        )
                        .unwrap_or_else(|e| e.into_inner());
                }
            })
            .map_err(|e| ServerError::InternalError {
                reason: format!("failed to start lifecycle worker: {e}"),
            })?;
        *lock_mutex_unpoisoned(&sweeper.handle) = Some(handle);
        Ok(sweeper)
    }

    #[cfg(test)]
    pub(super) fn disabled() -> Arc<Self> {
        Arc::new(Self {
            stop: Arc::new(AtomicBool::new(true)),
            wake: Arc::new((Mutex::new(true), Condvar::new())),
            handle: Mutex::new(None),
        })
    }
}

impl ReadRuntime {
    fn map_bucket_snapshot_error(error: storage::BucketSnapshotLoadError) -> ServerError {
        match error {
            storage::BucketSnapshotLoadError::Store(error) => ServerError::Store(error),
            storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { name },
            ) => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            storage::BucketSnapshotLoadError::Metadata(error) => ServerError::Metadata(error),
        }
    }

    pub(super) fn object_payload_reclaim_exists_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, ServerError> {
        self.storage_node
            .payload_reclaim_exists(bucket, key, generation_id)
            .map_err(|error| match error {
                storage::ObjectPgActionError::Store(error) => ServerError::Store(error),
                storage::ObjectPgActionError::Metadata(error) => ServerError::Metadata(error),
                storage::ObjectPgActionError::InvalidRequest { reason } => {
                    ServerError::InvalidRequest { reason }
                }
            })
    }

    pub(super) fn enqueue_object_payload_reclaim_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        self.storage_node
            .enqueue_object_payload_reclaim(bucket, key, generation_id);
    }

    #[cfg(test)]
    pub(super) fn enqueue_object_payload_reclaim(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) {
        self.enqueue_object_payload_reclaim_for(
            &trusted_bucket_name(bucket),
            &trusted_object_key(key),
            generation_id,
        );
    }

    pub(super) fn enqueue_bucket_delete_finalize_for(&self, bucket: &BucketName) {
        self.storage_node.enqueue_bucket_delete_finalize(bucket);
    }

    fn lifecycle_config_for_bucket_info(
        &self,
        bucket_info: &BucketInfo,
    ) -> Result<Option<Arc<BucketLifecycleConfiguration>>, ServerError> {
        if !bucket_info.bucket_lifecycle_present {
            return Ok(None);
        }

        let raw_config = self
            .storage_node
            .get_bucket_subresource(&bucket_info.name, storage::BucketSubresourceKind::Lifecycle)
            .map_err(Self::map_bucket_snapshot_error)?;

        raw_config
            .map(|config_xml| {
                s3_types::parse_lifecycle_configuration_xml(config_xml.as_bytes()).map_err(
                    |error| ServerError::InternalError {
                        reason: format!(
                            "stored lifecycle configuration for {} failed to parse at sweep time: {error}",
                            bucket_info.name
                        ),
                    },
                )
            })
            .transpose()
            .map(|config| config.map(Arc::new))
    }

    pub(super) fn run_lifecycle_sweep_at(
        &self,
        now_millis: u64,
    ) -> Result<LifecycleSweepStats, ServerError> {
        let mut stats = LifecycleSweepStats {
            scanned_buckets: 0,
            expired_current_objects: 0,
            expired_noncurrent_versions: 0,
            expired_delete_markers: 0,
            aborted_multipart_uploads: 0,
        };
        let mut processed_buckets: HashSet<BucketName> = HashSet::new();
        let sweep_buckets = self
            .storage_node
            .list_lifecycle_sweep_buckets()
            .map_err(Coordinator::map_object_pg_action_error)?;

        for bucket in sweep_buckets.lifecycle_buckets {
            if processed_buckets.insert(bucket.name.clone()) {
                stats.scanned_buckets += 1;
            }
            self.expire_due_current_objects_for_bucket(&bucket, now_millis, &mut stats)?;
            self.expire_due_noncurrent_versions_for_bucket(&bucket, now_millis, &mut stats)?;
            self.expire_due_delete_markers_for_bucket(&bucket, now_millis, &mut stats)?;
            self.abort_due_multipart_uploads_for_bucket(&bucket, now_millis, &mut stats)?;
        }

        for bucket in sweep_buckets.aborting_buckets {
            if processed_buckets.insert(bucket.clone()) {
                stats.scanned_buckets += 1;
            }
            stats.aborted_multipart_uploads +=
                self.finish_aborting_multipart_uploads_for_bucket(&bucket)?;
        }

        Ok(stats)
    }

    fn expire_due_current_objects_for_bucket(
        &self,
        bucket_info: &BucketInfo,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let Some(config) = self.lifecycle_config_for_bucket_info(bucket_info)? else {
            return Ok(());
        };

        let mut candidates = Vec::new();
        let objects = self
            .storage_node
            .list_all_objects_for_bucket(&bucket_info.name)
            .map_err(Coordinator::map_object_pg_action_error)?;
        for object in objects {
            let Some(record) = object.into_live() else {
                continue;
            };
            let tags = match record.tags.as_deref() {
                Some(tags_xml) => Coordinator::parse_serialized_tag_set(tags_xml)?,
                None => Vec::new(),
            };
            let Some(expiration) = Coordinator::evaluate_current_object_lifecycle_expiration(
                &config,
                record.key.as_str(),
                &tags,
                record.size,
                record.last_modified,
            ) else {
                continue;
            };
            if expiration.expiry_time_millis <= now_millis {
                candidates.push((record.key, record.version_id));
            }
        }

        for (key, version_id) in candidates {
            if self.expire_current_object_if_due(&bucket_info.name, &key, version_id, now_millis)? {
                stats.expired_current_objects += 1;
            }
        }

        Ok(())
    }

    fn finish_aborting_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<u64, ServerError> {
        let mut candidates = Vec::new();
        let uploads = self
            .storage_node
            .list_all_multipart_uploads_for_bucket(bucket)
            .map_err(Coordinator::map_object_pg_action_error)?;
        for upload in uploads {
            if upload.state == UploadState::Aborting {
                candidates.push((upload.key, upload.upload_id));
            }
        }

        let mut finished = 0u64;
        for (key, upload_id) in candidates {
            if self.abort_multipart_upload_internal_for(bucket, &key, &upload_id)? {
                finished += 1;
            }
        }
        Ok(finished)
    }

    fn expire_due_noncurrent_versions_for_bucket(
        &self,
        bucket_info: &BucketInfo,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let Some(config) = self.lifecycle_config_for_bucket_info(bucket_info)? else {
            return Ok(());
        };

        let mut candidate_keys = Vec::new();
        let versions = self
            .storage_node
            .list_all_object_versions_for_bucket(&bucket_info.name)
            .map_err(Coordinator::map_object_pg_action_error)?;
        let mut group_start = 0usize;
        while group_start < versions.len() {
            let key = versions[group_start].key().clone();
            let mut group_end = group_start + 1;
            while group_end < versions.len() && versions[group_end].key() == &key {
                group_end += 1;
            }

            if !Coordinator::evaluate_due_noncurrent_version_expirations(
                &config,
                &versions[group_start..group_end],
                now_millis,
            )?
            .is_empty()
            {
                candidate_keys.push(key);
            }
            group_start = group_end;
        }

        for key in candidate_keys {
            stats.expired_noncurrent_versions +=
                self.expire_noncurrent_versions_if_due(&bucket_info.name, &key, now_millis)?;
        }

        Ok(())
    }

    fn expire_current_object_if_due(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        now_millis: u64,
    ) -> Result<bool, ServerError> {
        let _bucket_guard = self.storage_node.lock_bucket(bucket);
        let bucket_info = match self.storage_node.head_bucket_info(bucket) {
            Ok(info) => info,
            Err(storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { .. },
            )) => return Ok(false),
            Err(error) => return Err(Self::map_bucket_snapshot_error(error)),
        };

        let Some(config) = self.lifecycle_config_for_bucket_info(&bucket_info)? else {
            return Ok(false);
        };

        let owner = OwnerIdentity::new(
            bucket_info.owner_principal.clone(),
            bucket_info.owner_canonical_id.clone(),
        );
        let outcome = self
            .storage_node
            .expire_current_object_if(
                bucket,
                key,
                expected_version_id,
                bucket_info.versioning,
                owner,
                |record| {
                    let tags = match record.tags.as_deref() {
                        Some(tags_xml) => Coordinator::parse_serialized_tag_set(tags_xml)?,
                        None => Vec::new(),
                    };
                    let Some(expiration) =
                        Coordinator::evaluate_current_object_lifecycle_expiration(
                            &config,
                            key.as_str(),
                            &tags,
                            record.size,
                            record.last_modified,
                        )
                    else {
                        return Ok::<bool, ServerError>(false);
                    };
                    Ok::<bool, ServerError>(expiration.expiry_time_millis <= now_millis)
                },
            )
            .map_err(Coordinator::map_object_pg_action_error)??;

        let Some(outcome) = outcome else {
            return Ok(false);
        };
        if let Some(generation_id) = outcome.reclaim_generation_id {
            self.enqueue_object_payload_reclaim_for(bucket, key, generation_id);
        }
        Ok(true)
    }

    fn expire_noncurrent_versions_if_due(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        now_millis: u64,
    ) -> Result<u64, ServerError> {
        let _bucket_guard = self.storage_node.lock_bucket(bucket);
        let bucket_info = match self.storage_node.head_bucket_info(bucket) {
            Ok(info) => info,
            Err(storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { .. },
            )) => return Ok(0),
            Err(error) => return Err(Self::map_bucket_snapshot_error(error)),
        };

        let Some(config) = self.lifecycle_config_for_bucket_info(&bucket_info)? else {
            return Ok(0);
        };

        let current_unix_seconds = Coordinator::current_unix_seconds()?;
        let reclaimed_generation_ids = self
            .storage_node
            .delete_noncurrent_live_versions_if(bucket, key, |versions| {
                let due_versions = Coordinator::evaluate_due_noncurrent_version_expirations(
                    &config, versions, now_millis,
                )?;
                let due_version_ids = due_versions
                    .iter()
                    .map(|candidate| candidate.version_id)
                    .collect::<HashSet<_>>();

                let eligible_version_ids = versions
                    .iter()
                    .filter_map(|stored| stored.as_live())
                    .filter(|record| due_version_ids.contains(&record.version_id))
                    .filter(|record| {
                        Coordinator::validate_delete_against_object_lock(
                            record.object_lock,
                            false,
                            false,
                            current_unix_seconds,
                        )
                        .is_ok()
                    })
                    .map(|record| record.version_id)
                    .collect();
                Ok::<HashSet<VersionId>, ServerError>(eligible_version_ids)
            })
            .map_err(Coordinator::map_object_pg_action_error)??;

        for generation_id in &reclaimed_generation_ids {
            self.enqueue_object_payload_reclaim_for(bucket, key, *generation_id);
        }

        Ok(reclaimed_generation_ids.len() as u64)
    }

    fn expire_due_delete_markers_for_bucket(
        &self,
        bucket_info: &BucketInfo,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let Some(config) = self.lifecycle_config_for_bucket_info(bucket_info)? else {
            return Ok(());
        };

        let mut candidates = Vec::new();
        let versions = self
            .storage_node
            .list_all_object_versions_for_bucket(&bucket_info.name)
            .map_err(Coordinator::map_object_pg_action_error)?;
        let mut group_start = 0usize;
        while group_start < versions.len() {
            let key = versions[group_start].key().clone();
            let mut group_end = group_start + 1;
            while group_end < versions.len() && versions[group_end].key() == &key {
                group_end += 1;
            }

            if let Some(expiration) = Coordinator::evaluate_due_expired_delete_marker(
                &config,
                &versions[group_start..group_end],
                now_millis,
            ) {
                candidates.push((key, expiration.version_id));
            }
            group_start = group_end;
        }

        for (key, version_id) in candidates {
            if self.expire_delete_marker_if_due(&bucket_info.name, &key, version_id, now_millis)? {
                stats.expired_delete_markers += 1;
            }
        }

        Ok(())
    }

    fn expire_delete_marker_if_due(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        now_millis: u64,
    ) -> Result<bool, ServerError> {
        let _bucket_guard = self.storage_node.lock_bucket(bucket);
        let bucket_info = match self.storage_node.head_bucket_info(bucket) {
            Ok(info) => info,
            Err(storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { .. },
            )) => return Ok(false),
            Err(error) => return Err(Self::map_bucket_snapshot_error(error)),
        };

        let Some(config) = self.lifecycle_config_for_bucket_info(&bucket_info)? else {
            return Ok(false);
        };

        self.storage_node
            .delete_expired_delete_marker_if(bucket, key, expected_version_id, |versions| {
                let Some(expiration) =
                    Coordinator::evaluate_due_expired_delete_marker(&config, versions, now_millis)
                else {
                    return Ok::<bool, ServerError>(false);
                };
                Ok::<bool, ServerError>(expiration.version_id == expected_version_id)
            })
            .map_err(Coordinator::map_object_pg_action_error)?
    }

    fn abort_due_multipart_uploads_for_bucket(
        &self,
        bucket_info: &BucketInfo,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let Some(config) = self.lifecycle_config_for_bucket_info(bucket_info)? else {
            return Ok(());
        };

        let mut candidates = Vec::new();
        let uploads = self
            .storage_node
            .list_all_multipart_uploads_for_bucket(&bucket_info.name)
            .map_err(Coordinator::map_object_pg_action_error)?;
        for upload in uploads {
            if upload.state != UploadState::InProgress && upload.state != UploadState::Aborting {
                continue;
            }
            let Some(headers) = Coordinator::evaluate_multipart_lifecycle_abort_headers(
                &config,
                upload.key.as_str(),
                upload.initiated_at,
            ) else {
                continue;
            };
            if headers.abort_time_millis <= now_millis {
                candidates.push((upload.key, upload.upload_id));
            }
        }

        for (key, upload_id) in candidates {
            if self.abort_multipart_upload_if_due(
                &bucket_info.name,
                &key,
                &upload_id,
                now_millis,
            )? {
                stats.aborted_multipart_uploads += 1;
            }
        }

        Ok(())
    }

    pub(super) fn abort_multipart_upload_if_due(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        now_millis: u64,
    ) -> Result<bool, ServerError> {
        let _bucket_guard = self.storage_node.lock_bucket(bucket);
        let bucket_info = match self.storage_node.head_bucket_info(bucket) {
            Ok(info) => info,
            Err(storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::BucketNotFound { .. },
            )) => return Ok(false),
            Err(error) => return Err(Self::map_bucket_snapshot_error(error)),
        };

        let upload = match self
            .storage_node
            .load_multipart_upload(bucket, key, upload_id)
        {
            Ok(upload) => upload,
            Err(storage::BucketSnapshotLoadError::Metadata(
                storage::MetadataError::NoSuchUpload { .. },
            )) => return Ok(false),
            Err(error) => return Err(Self::map_bucket_snapshot_error(error)),
        };

        if upload.state == UploadState::Aborting {
            return self.abort_multipart_upload_internal_for(bucket, key, upload_id);
        }
        if upload.state != UploadState::InProgress {
            return Ok(false);
        }

        let Some(config) = self.lifecycle_config_for_bucket_info(&bucket_info)? else {
            return Ok(false);
        };

        let Some(headers) = Coordinator::evaluate_multipart_lifecycle_abort_headers(
            &config,
            key.as_str(),
            upload.initiated_at,
        ) else {
            return Ok(false);
        };
        if headers.abort_time_millis > now_millis {
            return Ok(false);
        }

        self.abort_multipart_upload_internal_for(bucket, key, upload_id)
    }

    pub(super) fn abort_multipart_upload_internal_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
    ) -> Result<bool, ServerError> {
        self.storage_node
            .abort_multipart_upload(bucket, key, upload_id)
            .map_err(Coordinator::map_object_pg_action_error)
    }

    pub(super) fn acquire_object_payload_lease_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> PayloadLease {
        self.storage_node
            .acquire_object_payload_lease(bucket, key, generation_id);
        PayloadLease {
            runtime: self.clone(),
            bucket: bucket.clone(),
            key: key.clone(),
            generation_id,
        }
    }

    #[cfg(test)]
    pub(super) fn acquire_object_payload_lease(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> PayloadLease {
        self.acquire_object_payload_lease_for(
            &trusted_bucket_name(bucket),
            &trusted_object_key(key),
            generation_id,
        )
    }

    pub(super) fn try_reclaim_object_payload_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), ServerError> {
        enum ReclaimPayload {
            Simple(storage::SimplePayloadReclaimRecord),
            Segments(storage::ObjectSegmentsReclaimRecord),
            Multipart(storage::MultipartReclaimRecord),
        }

        let delete_ec_shards = |storage_node: &Arc<SharedStorageNode>,
                                shard_pg_id: u32,
                                okh: &[u8; 16],
                                generation_id: GenerationId,
                                ec: EcShape|
         -> Result<(), ServerError> {
            let shard_pg = storage_node.get_pg(shard_pg_id)?;
            let total = ec.k as usize + ec.m as usize;
            for i in 0..total {
                let shard_key = ShardKey::new(okh, generation_id.get(), i as u8);
                shard_pg.delete_shard(&shard_key)?;
            }
            Ok(())
        };

        if self
            .storage_node
            .object_payload_lease_count(bucket, key, generation_id)
            != 0
        {
            return Ok(());
        }

        let meta_pg_id = self.pg_topology.object_pg(bucket.as_str(), key.as_str());
        let reclaim = {
            let meta_guard = self.storage_node.get_pg(meta_pg_id)?;
            let meta_pg: &storage::PgStore = &meta_guard;

            if self
                .storage_node
                .object_payload_lease_count(bucket, key, generation_id)
                != 0
            {
                return Ok(());
            }

            if let Some(reclaim) = storage::PgMetadataStore::get_simple_payload_reclaim(
                meta_pg,
                bucket,
                key,
                generation_id,
            )? {
                Some(ReclaimPayload::Simple(reclaim))
            } else if let Some(reclaim) = storage::PgMetadataStore::get_object_segments_reclaim(
                meta_pg,
                bucket,
                key,
                generation_id,
            )? {
                Some(ReclaimPayload::Segments(reclaim))
            } else {
                storage::PgMetadataStore::get_multipart_reclaim(
                    meta_pg,
                    bucket,
                    key,
                    generation_id,
                )?
                .map(ReclaimPayload::Multipart)
            }
        };

        let Some(reclaim) = reclaim else {
            return Ok(());
        };

        if self
            .storage_node
            .object_payload_lease_count(bucket, key, generation_id)
            != 0
        {
            return Ok(());
        }

        match &reclaim {
            ReclaimPayload::Simple(reclaim) => {
                let shard_pg_id =
                    self.pg_topology
                        .shard_pg(bucket.as_str(), key.as_str(), generation_id.get());
                let okh = object_key_hash(bucket.as_str(), key.as_str());
                delete_ec_shards(
                    &self.storage_node,
                    shard_pg_id,
                    &okh,
                    generation_id,
                    reclaim.ec,
                )?;
            }
            ReclaimPayload::Segments(reclaim) => {
                for segment in &reclaim.segments {
                    delete_ec_shards(
                        &self.storage_node,
                        segment.shard_pg_id,
                        &segment.segment_okh,
                        segment.segment_vid,
                        segment.ec,
                    )?;
                }
            }
            ReclaimPayload::Multipart(reclaim) => {
                for part in &reclaim.parts {
                    match part {
                        MultipartReclaimPartRecord::ShardSet {
                            part_okh,
                            part_vid,
                            shard_pg_id,
                            ec,
                            ..
                        } => {
                            delete_ec_shards(
                                &self.storage_node,
                                *shard_pg_id,
                                part_okh,
                                *part_vid,
                                *ec,
                            )?;
                        }
                        MultipartReclaimPartRecord::Segments { segments, .. } => {
                            for segment in segments {
                                delete_ec_shards(
                                    &self.storage_node,
                                    segment.shard_pg_id,
                                    &segment.segment_okh,
                                    segment.segment_vid,
                                    segment.ec,
                                )?;
                            }
                        }
                    }
                }
            }
        }

        let meta_guard = self.storage_node.get_pg(meta_pg_id)?;
        let meta_pg: &storage::PgStore = &meta_guard;
        if self
            .storage_node
            .object_payload_lease_count(bucket, key, generation_id)
            != 0
        {
            return Ok(());
        }

        match reclaim {
            ReclaimPayload::Simple(_) => {
                storage::PgMetadataStore::delete_simple_payload_reclaim(
                    meta_pg,
                    bucket,
                    key,
                    generation_id,
                )?;
            }
            ReclaimPayload::Segments(_) => {
                storage::PgMetadataStore::delete_object_segments_reclaim(
                    meta_pg,
                    bucket,
                    key,
                    generation_id,
                )?;
            }
            ReclaimPayload::Multipart(_) => {
                storage::PgMetadataStore::delete_multipart_reclaim(
                    meta_pg,
                    bucket,
                    key,
                    generation_id,
                )?;
            }
        }
        self.enqueue_bucket_delete_finalize_for(bucket);
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn try_reclaim_object_payload(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> Result<(), ServerError> {
        self.try_reclaim_object_payload_for(
            &trusted_bucket_name(bucket),
            &trusted_object_key(key),
            generation_id,
        )
    }

    pub(super) fn try_finalize_bucket_delete_for(
        &self,
        bucket: &BucketName,
    ) -> Result<(), ServerError> {
        match self.storage_node.try_finalize_bucket_delete(bucket) {
            Ok(
                storage::BucketDeleteFinalizeOutcome::NotFound
                | storage::BucketDeleteFinalizeOutcome::NotDeleting
                | storage::BucketDeleteFinalizeOutcome::Pending
                | storage::BucketDeleteFinalizeOutcome::Finalized,
            ) => Ok(()),
            Err(storage::BucketWriteDrainError::Store(other)) => Err(ServerError::Store(other)),
            Err(storage::BucketWriteDrainError::Metadata(other)) => {
                Err(ServerError::Metadata(other))
            }
        }
    }

    fn read_segment_payload(
        &self,
        segment: &SegmentPayloadRecord,
        part_number: Option<u32>,
        sse_customer_request: Option<&SseCustomerRequest>,
    ) -> Result<Arc<SharedPayloadBuffer>, ServerError> {
        let k = segment.ec_k as usize;
        let m = segment.ec_m as usize;
        let padded = segment.stored_size().div_ceil(k) * k;
        let shard_size = padded / k;

        if shard_size == 0 {
            let plaintext =
                self.decrypt_segment_if_needed(segment, part_number, sse_customer_request, &[])?;
            return Ok(Arc::new(SharedPayloadBuffer::from_unpooled(plaintext)));
        }

        if let Some(buf) = self.try_read_segment_payload_direct(
            segment,
            part_number,
            sse_customer_request,
            k,
            shard_size,
            padded,
        )? {
            return Ok(buf.into_shared());
        }

        self.read_segment_payload_locked(
            segment,
            part_number,
            sse_customer_request,
            k,
            m,
            padded,
            shard_size,
        )
        .map(PooledPayloadBuffer::into_shared)
    }

    fn try_read_segment_payload_direct(
        &self,
        segment: &SegmentPayloadRecord,
        part_number: Option<u32>,
        sse_customer_request: Option<&SseCustomerRequest>,
        k: usize,
        shard_size: usize,
        padded: usize,
    ) -> Result<Option<PooledPayloadBuffer>, ServerError> {
        let Some(expected_crc64) = segment.segment_crc64 else {
            return Ok(None);
        };

        let mut buf = self.payload_buffer_pool.checkout(padded);
        buf.resize_zeroed(padded);
        for i in 0..k {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            let start = i * shard_size;
            let end = start + shard_size;
            match self.storage_node.read_shard_file_into(
                segment.shard_pg_id,
                &shard_key,
                &mut buf[start..end],
            ) {
                Ok(()) => {}
                Err(storage::StoreError::PgNotFound { pg_id }) => {
                    return Err(ServerError::Store(storage::StoreError::PgNotFound {
                        pg_id,
                    }));
                }
                Err(_) => return Ok(None),
            }
        }

        buf.truncate(segment.stored_size());
        let actual_crc64 = checksum::crc64::checksum(&buf);
        if actual_crc64 != expected_crc64 {
            return Ok(None);
        }
        let plaintext =
            self.decrypt_segment_if_needed(segment, part_number, sse_customer_request, &buf)?;
        buf.resize_zeroed(0);
        buf.extend_from_slice(&plaintext);
        Ok(Some(buf))
    }

    #[allow(clippy::too_many_arguments)]
    fn read_segment_payload_locked(
        &self,
        segment: &SegmentPayloadRecord,
        part_number: Option<u32>,
        sse_customer_request: Option<&SseCustomerRequest>,
        k: usize,
        m: usize,
        padded: usize,
        shard_size: usize,
    ) -> Result<PooledPayloadBuffer, ServerError> {
        let pg = self.storage_node.get_pg(segment.shard_pg_id)?;

        let mut all_shards = vec![None; k + m];
        let mut present_count = 0;

        let read_shard = |i: usize,
                          all_shards: &mut [Option<Vec<u8>>],
                          present_count: &mut usize| {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            if let Ok(sd) = pg.read_shard(&shard_key) {
                all_shards[i] = Some(sd.data);
                *present_count += 1;
            }
        };

        for i in 0..k {
            read_shard(i, &mut all_shards, &mut present_count);
        }

        if present_count < k {
            for i in k..(k + m) {
                if present_count >= k {
                    break;
                }
                read_shard(i, &mut all_shards, &mut present_count);
            }
        }

        if present_count < k {
            return Err(ServerError::Store(storage::StoreError::NotFound));
        }

        let mut recovered = None;
        let mut recovered_ranges = vec![None; k];

        if !(0..k).all(|i| all_shards[i].is_some()) {
            let missing_needed: Vec<usize> = (0..k).filter(|&i| all_shards[i].is_none()).collect();

            let present_indices: Vec<usize> =
                (0..(k + m)).filter(|&i| all_shards[i].is_some()).collect();
            let present_refs: Vec<&[u8]> = present_indices
                .iter()
                .map(|&i| all_shards[i].as_ref().unwrap().as_slice())
                .collect();

            let tmp_codec;
            let codec = if segment.ec_k == self.ec_config.data_shards
                && segment.ec_m == self.ec_config.parity_shards
            {
                self.ec_codec.as_ref()
            } else {
                let ec_config = EcConfig::new(segment.ec_k, segment.ec_m)?;
                tmp_codec = ErasureCodec::new(ec_config)?;
                &tmp_codec
            };

            let recovered_len = missing_needed.len() * shard_size;
            let mut recovered_buf = self.payload_buffer_pool.checkout(recovered_len);
            recovered_buf.resize_zeroed(recovered_len);
            let mut output_refs: Vec<&mut [u8]> = recovered_buf
                .chunks_mut(shard_size)
                .take(missing_needed.len())
                .collect();

            codec.reconstruct(
                &present_indices,
                &present_refs,
                &missing_needed,
                &mut output_refs,
            )?;

            for (slot, &missing_idx) in missing_needed.iter().enumerate() {
                let start = slot * shard_size;
                recovered_ranges[missing_idx] = Some((start, start + shard_size));
            }
            recovered = Some(recovered_buf);
        }

        let mut buf = self.payload_buffer_pool.checkout(padded);
        buf.resize_zeroed(0);
        for (idx, shard) in all_shards.iter().take(k).enumerate() {
            if let Some(shard) = shard.as_ref() {
                buf.extend_from_slice(shard);
            } else if let Some((start, end)) = recovered_ranges[idx] {
                buf.extend_from_slice(&recovered.as_ref().unwrap()[start..end]);
            } else {
                unreachable!("missing reconstructed shard for data index {idx}");
            }
        }
        buf.truncate(segment.stored_size());
        if let Some(expected_crc64) = segment.segment_crc64 {
            let actual_crc64 = checksum::crc64::checksum(&buf);
            if actual_crc64 != expected_crc64 {
                return Err(ServerError::Store(storage::StoreError::IntegrityError {
                    expected: expected_crc64,
                    actual: actual_crc64,
                }));
            }
        }
        let plaintext =
            self.decrypt_segment_if_needed(segment, part_number, sse_customer_request, &buf)?;
        buf.resize_zeroed(0);
        buf.extend_from_slice(&plaintext);
        Ok(buf)
    }

    fn decrypt_segment_if_needed(
        &self,
        segment: &SegmentPayloadRecord,
        part_number: Option<u32>,
        sse_customer_request: Option<&SseCustomerRequest>,
        stored_bytes: &[u8],
    ) -> Result<Vec<u8>, ServerError> {
        match &segment.encryption {
            ObjectEncryption::None => Ok(stored_bytes.to_vec()),
            ObjectEncryption::SseCustomer(state) => {
                let request = sse_customer_request.ok_or(ServerError::InvalidRequest {
                    reason: "SSE-C headers are required for this object".to_string(),
                })?;
                let validator =
                    self.sse_c_validator
                        .as_ref()
                        .ok_or(ServerError::InternalError {
                            reason: "SSE-C validator key is not configured".to_string(),
                        })?;
                let segment_scope = part_number
                    .map_or(Ok(SseCustomerSegmentScope::object()), |p| {
                        SseCustomerSegmentScope::multipart_part(p)
                    })?;
                decrypt_sse_customer_segment(
                    validator,
                    state,
                    request,
                    segment_scope,
                    segment.segment_index,
                    stored_bytes,
                    segment.size as usize,
                )
            }
            ObjectEncryption::SseS3(state) => {
                let provider =
                    self.managed_key_provider
                        .as_ref()
                        .ok_or(ServerError::InternalError {
                            reason: "SSE-S3 key provider is not configured".to_string(),
                        })?;
                let segment_scope = part_number
                    .map_or(Ok(SseCustomerSegmentScope::object()), |p| {
                        SseCustomerSegmentScope::multipart_part(p)
                    })?;
                decrypt_managed_encryption_segment(
                    provider,
                    state,
                    segment_scope,
                    segment.segment_index,
                    stored_bytes,
                    segment.size as usize,
                )
            }
        }
    }
}

impl SegmentListReader {
    pub(super) fn next_chunk(
        &mut self,
        target_size: usize,
    ) -> Result<Option<ReadChunk>, ServerError> {
        loop {
            if let Some((loaded, offset, end_offset)) = &mut self.loaded_segment {
                if *offset < *end_offset {
                    let end = (*offset + target_size).min(*end_offset);
                    let out = ReadChunk::from_shared_range(Arc::clone(loaded), *offset, end);
                    if end == *end_offset {
                        self.loaded_segment = None;
                    } else {
                        *offset = end;
                    }
                    return Ok(Some(out));
                }
                self.loaded_segment = None;
            }

            if self.next_segment_index >= self.segments.len() {
                return Ok(None);
            }

            let slice = self.segments[self.next_segment_index].clone();
            #[cfg(feature = "deep-tracing")]
            if let Some(trace) = observability::current_context() {
                let read_object_offset_start =
                    slice.segment_object_offset_start + slice.start_offset;
                let read_object_offset_end_exclusive =
                    slice.segment_object_offset_start + slice.end_offset;
                let read_object_offset_len =
                    read_object_offset_end_exclusive - read_object_offset_start;
                let read_segment_offset_len = slice.end_offset - slice.start_offset;
                if let Some(part_layout) = slice.part_number.zip(slice.part_order).zip(
                    slice
                        .part_object_offset_start
                        .zip(slice.part_object_offset_end_exclusive),
                ) {
                    let (
                        (part_number, part_order),
                        (part_object_offset_start, part_object_offset_end_exclusive),
                    ) = part_layout;
                    let _ = observability::event_in_context(
                        &trace,
                        TRACE_TARGET,
                        "read_segment_layout",
                        Some(format_args!(
                            "bucket={:?} key={:?} part_order={} part_number={} part_object_offset_start={} part_object_offset_len={} part_object_offset_end_exclusive={} segment_index={} segment_size={} segment_object_offset_start={} segment_object_offset_end_exclusive={} read_object_offset_start={} read_object_offset_len={} read_object_offset_end_exclusive={} read_segment_offset_start={} read_segment_offset_len={} read_segment_offset_end_exclusive={} shard_pg_id={} ec_k={} ec_m={}",
                            self.bucket,
                            self.key,
                            part_order,
                            part_number,
                            part_object_offset_start,
                            part_object_offset_end_exclusive - part_object_offset_start,
                            part_object_offset_end_exclusive,
                            slice.segment_index,
                            slice.payload.size,
                            slice.segment_object_offset_start,
                            slice.segment_object_offset_end_exclusive,
                            read_object_offset_start,
                            read_object_offset_len,
                            read_object_offset_end_exclusive,
                            slice.start_offset,
                            read_segment_offset_len,
                            slice.end_offset,
                            slice.payload.shard_pg_id,
                            slice.payload.ec_k,
                            slice.payload.ec_m,
                        )),
                    );
                } else {
                    let _ = observability::event_in_context(
                        &trace,
                        TRACE_TARGET,
                        "read_segment_layout",
                        Some(format_args!(
                            "bucket={:?} key={:?} segment_index={} segment_size={} segment_object_offset_start={} segment_object_offset_end_exclusive={} read_object_offset_start={} read_object_offset_len={} read_object_offset_end_exclusive={} read_segment_offset_start={} read_segment_offset_len={} read_segment_offset_end_exclusive={} shard_pg_id={} ec_k={} ec_m={}",
                            self.bucket,
                            self.key,
                            slice.segment_index,
                            slice.payload.size,
                            slice.segment_object_offset_start,
                            slice.segment_object_offset_end_exclusive,
                            read_object_offset_start,
                            read_object_offset_len,
                            read_object_offset_end_exclusive,
                            slice.start_offset,
                            read_segment_offset_len,
                            slice.end_offset,
                            slice.payload.shard_pg_id,
                            slice.payload.ec_k,
                            slice.payload.ec_m,
                        )),
                    );
                }
            }

            let data = self
                .runtime
                .read_segment_payload(
                    &slice.payload,
                    slice.part_number,
                    self.sse_customer_request.as_ref(),
                )
                .map_err(|e| match e {
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: self.bucket.clone(),
                            key: self.key.clone(),
                        }
                    }
                    other => other,
                })?;
            self.next_segment_index += 1;
            self.loaded_segment = Some((data, slice.start_offset, slice.end_offset));
            #[cfg(test)]
            if self.next_segment_index == 1 {
                maybe_run_object_segments_first_segment_hook(&self.bucket, &self.key);
            }
        }
    }
}

impl MultipartReader {
    pub(super) fn next_chunk(
        &mut self,
        target_size: usize,
    ) -> Result<Option<ReadChunk>, ServerError> {
        loop {
            if let Some(current) = &mut self.current_part {
                let chunk = current.next_chunk(target_size)?;
                if chunk.is_some() {
                    return Ok(chunk);
                }
                self.current_part = None;
            }

            if self.next_part_index >= self.parts.len() {
                return Ok(None);
            }

            let part = self.parts[self.next_part_index].clone();
            self.next_part_index += 1;
            #[cfg(feature = "deep-tracing")]
            if let Some(trace) = observability::current_context() {
                let _ = observability::event_in_context(
                    &trace,
                    TRACE_TARGET,
                    "read_multipart_part_layout",
                    Some(format_args!(
                        "bucket={:?} key={:?} part_order={} part_number={} part_object_offset_start={} part_object_offset_len={} part_object_offset_end_exclusive={} segment_count={}",
                        self.bucket,
                        self.key,
                        part.layout.part_order,
                        part.layout.part_number,
                        part.layout.object_offset_start,
                        part.layout.object_offset_end_exclusive - part.layout.object_offset_start,
                        part.layout.object_offset_end_exclusive,
                        part.segments.len(),
                    )),
                );
            }
            self.current_part = Some(SegmentListReader {
                runtime: self.runtime.clone(),
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                segments: part.segments,
                next_segment_index: 0,
                loaded_segment: None,
                sse_customer_request: self.sse_customer_request.clone(),
            });
        }
    }
}
