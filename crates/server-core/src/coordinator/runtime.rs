// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use s3_types::BucketLifecycleConfiguration;
use storage::{
    BucketInfo, BucketName, GenerationId, ObjectEncryption, ObjectKey, ProcessLocalRegistryKey,
    StorageCluster, StorageClusterRouteHandle, StorageMaintenanceAdmission, UploadId, UploadState,
    VersionId,
};

use super::payload::SharedPayloadBuffer;
#[cfg(test)]
use super::read_core::PayloadLease;
use super::read_core::{ReadRuntime, ReadStorage, SegmentPayloadRecord};
use super::TRACE_TARGET;
use super::{lock_mutex_unpoisoned, Coordinator, LIFECYCLE_SWEEP_INTERVAL_MILLIS};
use crate::error::ServerError;
use crate::sse::{
    decrypt_managed_encryption_segment, decrypt_sse_customer_segment, SseCustomerRequest,
    SseCustomerSegmentScope,
};

static LIFECYCLE_SWEEPER_REGISTRY: OnceLock<
    Mutex<HashMap<ProcessLocalRegistryKey, Weak<LifecycleSweeper>>>,
> = OnceLock::new();
const LIFECYCLE_SWEEP_HEARTBEAT_INTERVAL_ITEMS: usize = 256;
const LIFECYCLE_SWEEP_ERROR_CONTEXT_MAX_CHARS: usize = 1024;

pub(super) struct LifecycleSweeper {
    pub(super) stop: Arc<AtomicBool>,
    pub(super) wake: Arc<(Mutex<bool>, Condvar)>,
    pub(super) handle: Mutex<Option<JoinHandle<()>>>,
}

pub(super) use storage::StorageReclaimSweeper as ReclaimSweeper;
pub(super) use storage::StorageShardBackfillSweeper as ShardBackfillSweeper;
pub(super) use storage::StorageShardRepairSweeper as ShardRepairSweeper;
pub(super) use storage::StorageShardScavengerSweeper as ShardScavengerSweeper;
pub(super) use storage::StorageStreamSessionSweeper as StreamSessionSweeper;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LifecycleSweepStats {
    pub(super) discovered_roots: u64,
    pub(super) acquired_claims: u64,
    pub(super) busy_claims: u64,
    pub(super) recovered_expired_claims: u64,
    pub(super) released_claims: u64,
    pub(super) failed_claims: u64,
    pub(super) scanned_buckets: u64,
    pub(super) expired_current_objects: u64,
    pub(super) expired_noncurrent_versions: u64,
    pub(super) expired_delete_markers: u64,
    pub(super) skipped_expired_delete_markers: u64,
    pub(super) aborted_multipart_uploads: u64,
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
        storage_handle: &StorageClusterRouteHandle,
        runtime: ReadRuntime,
    ) -> Result<Arc<Self>, ServerError> {
        let registry = LIFECYCLE_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry: std::sync::MutexGuard<
            '_,
            HashMap<ProcessLocalRegistryKey, Weak<LifecycleSweeper>>,
        > = lock_mutex_unpoisoned(registry);
        registry.retain(|_, sweeper| sweeper.upgrade().is_some());

        let storage_cluster = storage_handle.current();
        let key = storage_cluster.process_local_registry_key();
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(existing);
        }

        let admission = StorageMaintenanceAdmission::acquire_shared(storage_handle);
        let sweeper = Self::spawn(storage_handle.clone(), runtime, admission)?;
        registry.insert(key, Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(
        storage_handle: StorageClusterRouteHandle,
        runtime: ReadRuntime,
        admission: Arc<StorageMaintenanceAdmission>,
    ) -> Result<Arc<Self>, ServerError> {
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
                    if let Some(_permit) = admission.try_lifecycle_cleanup() {
                        let current_runtime =
                            lifecycle_runtime_for_sweep(&storage_handle, &runtime);
                        let _ = current_runtime.run_lifecycle_sweep_at(Coordinator::now_millis());
                    }
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

    pub(super) fn disabled() -> Arc<Self> {
        Arc::new(Self {
            stop: Arc::new(AtomicBool::new(true)),
            wake: Arc::new((Mutex::new(true), Condvar::new())),
            handle: Mutex::new(None),
        })
    }
}

pub(super) fn lifecycle_runtime_for_sweep(
    storage_handle: &StorageClusterRouteHandle,
    runtime: &ReadRuntime,
) -> ReadRuntime {
    runtime.with_storage_node(storage_handle.current())
}

pub(super) fn acquire_stream_session_sweeper(
    storage_handle: &StorageClusterRouteHandle,
) -> Result<Arc<StreamSessionSweeper>, ServerError> {
    StreamSessionSweeper::acquire_shared(storage_handle).map_err(|error| {
        ServerError::InternalError {
            reason: error.to_string(),
        }
    })
}

pub(super) fn acquire_shard_scavenger_sweeper(
    storage_handle: &StorageClusterRouteHandle,
) -> Result<Arc<ShardScavengerSweeper>, ServerError> {
    ShardScavengerSweeper::acquire_shared(storage_handle).map_err(|error| {
        ServerError::InternalError {
            reason: error.to_string(),
        }
    })
}

pub(super) fn acquire_shard_repair_sweeper(
    storage_handle: &StorageClusterRouteHandle,
) -> Result<Arc<ShardRepairSweeper>, ServerError> {
    ShardRepairSweeper::acquire_shared(storage_handle).map_err(|error| ServerError::InternalError {
        reason: error.to_string(),
    })
}

pub(super) fn acquire_shard_backfill_sweeper(
    storage_handle: &StorageClusterRouteHandle,
) -> Result<Arc<ShardBackfillSweeper>, ServerError> {
    ShardBackfillSweeper::acquire_shared(storage_handle).map_err(|error| {
        ServerError::InternalError {
            reason: error.to_string(),
        }
    })
}

impl ReadRuntime {
    pub(super) fn storage_node(&self) -> &Arc<StorageCluster> {
        match &self.storage {
            ReadStorage::Cluster(storage_node) => storage_node,
            ReadStorage::Active(_) | ReadStorage::Retained(_) => {
                panic!("raw storage operations require a cluster-backed read runtime")
            }
        }
    }

    fn with_storage_node(&self, storage_node: Arc<StorageCluster>) -> Self {
        Self {
            storage: ReadStorage::Cluster(Arc::clone(&storage_node)),
            payload_buffer_pool: Arc::clone(&self.payload_buffer_pool),
            sse_c_validator: self.sse_c_validator.clone(),
            managed_key_provider: self.managed_key_provider.clone(),
        }
    }

    fn map_bucket_snapshot_error(error: storage::BucketSnapshotLoadFailure) -> ServerError {
        Coordinator::map_bucket_snapshot_load_error(error)
    }

    pub(super) fn map_lifecycle_maintenance_failure(
        error: storage::LifecycleMaintenanceFailure,
    ) -> ServerError {
        match error.kind() {
            storage::LifecycleMaintenanceFailureKind::ResourceExhausted
            | storage::LifecycleMaintenanceFailureKind::MetadataCommandContention
            | storage::LifecycleMaintenanceFailureKind::RetryableConvergence => {
                ServerError::SlowDown
            }
            storage::LifecycleMaintenanceFailureKind::InternalError => {
                ServerError::LifecycleMaintenance(error)
            }
        }
    }

    pub(super) fn map_lifecycle_mutation_failure(
        error: storage::LifecycleMutationFailure,
    ) -> ServerError {
        match error.kind() {
            storage::LifecycleMutationFailureKind::ResourceExhausted
            | storage::LifecycleMutationFailureKind::MetadataCommandContention
            | storage::LifecycleMutationFailureKind::RetryableConvergence => ServerError::SlowDown,
            storage::LifecycleMutationFailureKind::InternalError => {
                ServerError::LifecycleMutation(error)
            }
        }
    }

    pub(super) fn enqueue_object_payload_reclaim_for(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) {
        self.storage_node()
            .enqueue_object_payload_reclaim(bucket, key, generation_id);
    }

    fn lifecycle_config_for_bucket_info(
        &self,
        bucket_info: &BucketInfo,
    ) -> Result<Option<Arc<BucketLifecycleConfiguration>>, ServerError> {
        if !bucket_info.bucket_lifecycle_present {
            return Ok(None);
        }

        let raw_config = self
            .storage_node()
            .get_bucket_subresource(
                &bucket_info.name,
                storage::OpaqueBucketSubresourceKind::Lifecycle,
            )
            .map_err(Self::map_bucket_snapshot_error)?;

        Self::parse_lifecycle_config(bucket_info.name.as_str(), raw_config.as_deref())
    }

    fn parse_lifecycle_config(
        bucket: &str,
        raw_config: Option<&str>,
    ) -> Result<Option<Arc<BucketLifecycleConfiguration>>, ServerError> {
        raw_config
            .map(|config_xml| {
                s3_types::parse_lifecycle_configuration_xml(config_xml.as_bytes()).map_err(
                    |error| ServerError::InternalError {
                        reason: format!(
                            "stored lifecycle configuration for {bucket} failed to parse at sweep time: {error}",
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
            discovered_roots: 0,
            acquired_claims: 0,
            busy_claims: 0,
            recovered_expired_claims: 0,
            released_claims: 0,
            failed_claims: 0,
            scanned_buckets: 0,
            expired_current_objects: 0,
            expired_noncurrent_versions: 0,
            expired_delete_markers: 0,
            skipped_expired_delete_markers: 0,
            aborted_multipart_uploads: 0,
        };
        let mut processed_buckets: HashSet<(BucketName, u64)> = HashSet::new();
        let claim_now_millis = storage::clock::wall_time_millis();
        let sweep_roots = self
            .storage_node()
            .list_lifecycle_sweep_roots(claim_now_millis)
            .map_err(Self::map_lifecycle_maintenance_failure)?;
        stats.discovered_roots = sweep_roots.len() as u64;
        let _ = observability::event(
            TRACE_TARGET,
            "lifecycle_sweep_pass_start",
            Some(format_args!(
                "roots={} lifecycle_now_millis={} claim_now_millis={}",
                stats.discovered_roots, now_millis, claim_now_millis
            )),
        );

        for root in sweep_roots {
            let claim_now_millis = storage::clock::wall_time_millis();
            let Some(claim) = self
                .storage_node()
                .acquire_lifecycle_sweep_claim(
                    &root.bucket,
                    root.bucket_incarnation_generation,
                    claim_now_millis,
                )
                .map_err(Self::map_lifecycle_maintenance_failure)?
            else {
                stats.busy_claims += 1;
                let _ = observability::event(
                    TRACE_TARGET,
                    "lifecycle_sweep_claim_busy",
                    Some(format_args!(
                        "bucket={:?} incarnation={} source={:?}",
                        root.bucket, root.bucket_incarnation_generation, root.source
                    )),
                );
                continue;
            };
            stats.acquired_claims += 1;
            if claim.attempt_count > 1 {
                stats.recovered_expired_claims += 1;
            }
            let _ = observability::event(
                TRACE_TARGET,
                "lifecycle_sweep_claim_acquired",
                Some(format_args!(
                    "bucket={:?} incarnation={} claim_id={} source={:?} attempt_count={}",
                    claim.bucket,
                    claim.bucket_incarnation_generation,
                    claim.claim_id,
                    root.source,
                    claim.attempt_count
                )),
            );
            if !processed_buckets.insert((root.bucket.clone(), root.bucket_incarnation_generation))
            {
                self.storage_node()
                    .release_lifecycle_sweep_claim(&claim)
                    .map_err(Self::map_lifecycle_maintenance_failure)?;
                stats.released_claims += 1;
                continue;
            }
            let claim = self
                .storage_node()
                .heartbeat_lifecycle_sweep_claim(&claim, storage::clock::wall_time_millis())
                .map_err(Self::map_lifecycle_maintenance_failure)?;

            stats.scanned_buckets += 1;
            let result =
                self.run_claimed_lifecycle_sweep_for_bucket(&claim, now_millis, &mut stats);
            match result {
                Ok(()) => {
                    self.storage_node()
                        .release_lifecycle_sweep_claim(&claim)
                        .map_err(Self::map_lifecycle_maintenance_failure)?;
                    stats.released_claims += 1;
                    let _ = observability::event(
                        TRACE_TARGET,
                        "lifecycle_sweep_claim_released",
                        Some(format_args!(
                            "bucket={:?} incarnation={} claim_id={}",
                            claim.bucket, claim.bucket_incarnation_generation, claim.claim_id
                        )),
                    );
                }
                Err(error) => {
                    stats.failed_claims += 1;
                    let error_context = lifecycle_sweep_error_context(&error);
                    let record_result = self
                        .storage_node()
                        .record_lifecycle_sweep_claim_error(&claim, &error_context);
                    match record_result {
                        Ok(_) => {
                            let _ = observability::event(
                                TRACE_TARGET,
                                "lifecycle_sweep_claim_error_recorded",
                                Some(format_args!(
                                    "bucket={:?} incarnation={} claim_id={} failed_claims={} error={}",
                                    claim.bucket,
                                    claim.bucket_incarnation_generation,
                                    claim.claim_id,
                                    stats.failed_claims,
                                    error_context
                                )),
                            );
                        }
                        Err(record_error) => {
                            let _ = observability::event(
                                TRACE_TARGET,
                                "lifecycle_sweep_claim_error_record_failed",
                                Some(format_args!(
                                    "bucket={:?} claim_id={} failed_claims={} error={} record_error={record_error}",
                                    claim.bucket, claim.claim_id, stats.failed_claims, error_context
                                )),
                            );
                        }
                    }
                    return Err(error);
                }
            }
        }

        let _ = observability::event(
            TRACE_TARGET,
            "lifecycle_sweep_pass_complete",
            Some(format_args!(
                "roots={} acquired_claims={} busy_claims={} recovered_expired_claims={} released_claims={} failed_claims={} scanned_buckets={} expired_current_objects={} expired_noncurrent_versions={} expired_delete_markers={} skipped_expired_delete_markers={} aborted_multipart_uploads={}",
                stats.discovered_roots,
                stats.acquired_claims,
                stats.busy_claims,
                stats.recovered_expired_claims,
                stats.released_claims,
                stats.failed_claims,
                stats.scanned_buckets,
                stats.expired_current_objects,
                stats.expired_noncurrent_versions,
                stats.expired_delete_markers,
                stats.skipped_expired_delete_markers,
                stats.aborted_multipart_uploads
            )),
        );

        Ok(stats)
    }

    fn run_claimed_lifecycle_sweep_for_bucket(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let bucket = &claim.bucket;
        let bucket_info = match self.storage_node().head_bucket_info(bucket) {
            Ok(bucket_info) => bucket_info,
            Err(error)
                if matches!(
                    error.kind(),
                    storage::BucketSnapshotLoadFailureKind::BucketNotFound { .. }
                ) =>
            {
                return Ok(())
            }
            Err(error) => return Err(Self::map_bucket_snapshot_error(error)),
        };
        if bucket_info.bucket_incarnation_generation != claim.bucket_incarnation_generation {
            return Ok(());
        }

        self.expire_due_current_objects_for_bucket(claim, &bucket_info, now_millis, stats)?;
        self.expire_due_noncurrent_versions_for_bucket(claim, &bucket_info, now_millis, stats)?;
        self.expire_due_delete_markers_for_bucket(claim, &bucket_info, now_millis, stats)?;
        self.abort_due_multipart_uploads_for_bucket(claim, &bucket_info, now_millis, stats)?;
        self.heartbeat_lifecycle_sweep_claim(claim)?;
        stats.aborted_multipart_uploads +=
            self.finish_aborting_multipart_uploads_for_bucket(claim, &bucket_info.name)?;
        Ok(())
    }

    fn heartbeat_lifecycle_sweep_claim(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
    ) -> Result<(), ServerError> {
        self.storage_node()
            .heartbeat_lifecycle_sweep_claim(claim, storage::clock::wall_time_millis())
            .map(drop)
            .map_err(Self::map_lifecycle_maintenance_failure)
    }

    fn expire_due_current_objects_for_bucket(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
        bucket_info: &BucketInfo,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let Some(config) = self.lifecycle_config_for_bucket_info(bucket_info)? else {
            return Ok(());
        };

        let mut candidates = Vec::new();
        let objects = self
            .storage_node()
            .list_all_objects_for_bucket(&bucket_info.name)
            .map_err(Self::map_lifecycle_maintenance_failure)?;
        for (index, object) in objects.into_iter().enumerate() {
            if index > 0 && index % LIFECYCLE_SWEEP_HEARTBEAT_INTERVAL_ITEMS == 0 {
                self.heartbeat_lifecycle_sweep_claim(claim)?;
            }
            let Some(record) = object.into_live() else {
                continue;
            };
            let tags = match record.tags.as_deref() {
                Some(tags) => tags.clone().into_pairs(),
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
            self.heartbeat_lifecycle_sweep_claim(claim)?;
            if self.expire_current_object_if_due(
                &bucket_info.name,
                &key,
                version_id,
                claim.bucket_incarnation_generation,
                now_millis,
            )? {
                stats.expired_current_objects += 1;
            }
        }

        Ok(())
    }

    fn finish_aborting_multipart_uploads_for_bucket(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
        bucket: &BucketName,
    ) -> Result<u64, ServerError> {
        let mut candidates = Vec::new();
        let uploads = self
            .storage_node()
            .list_all_multipart_uploads_for_bucket(bucket)
            .map_err(Self::map_lifecycle_maintenance_failure)?;
        for (index, upload) in uploads.into_iter().enumerate() {
            if index > 0 && index % LIFECYCLE_SWEEP_HEARTBEAT_INTERVAL_ITEMS == 0 {
                self.heartbeat_lifecycle_sweep_claim(claim)?;
            }
            if upload.state() == UploadState::Aborting {
                candidates.push(upload.into_key_and_upload_id());
            }
        }

        let mut finished = 0u64;
        for (key, upload_id) in candidates {
            if self.abort_multipart_upload_for_lifecycle_sweep(
                bucket,
                &key,
                &upload_id,
                claim.bucket_incarnation_generation,
            )? {
                finished += 1;
            }
        }
        Ok(finished)
    }

    fn expire_due_noncurrent_versions_for_bucket(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
        bucket_info: &BucketInfo,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let Some(config) = self.lifecycle_config_for_bucket_info(bucket_info)? else {
            return Ok(());
        };

        let mut candidate_keys = Vec::new();
        let versions = self
            .storage_node()
            .list_all_object_versions_for_bucket(&bucket_info.name)
            .map_err(Self::map_lifecycle_maintenance_failure)?;
        let mut group_start = 0usize;
        let mut groups_seen = 0usize;
        while group_start < versions.len() {
            if groups_seen > 0
                && groups_seen.is_multiple_of(LIFECYCLE_SWEEP_HEARTBEAT_INTERVAL_ITEMS)
            {
                self.heartbeat_lifecycle_sweep_claim(claim)?;
            }
            groups_seen += 1;
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
            self.heartbeat_lifecycle_sweep_claim(claim)?;
            stats.expired_noncurrent_versions += self.expire_noncurrent_versions_if_due(
                &bucket_info.name,
                &key,
                claim.bucket_incarnation_generation,
                now_millis,
            )?;
        }

        Ok(())
    }

    pub(super) fn expire_current_object_if_due(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        expected_bucket_incarnation_generation: u64,
        now_millis: u64,
    ) -> Result<bool, ServerError> {
        let outcome = self
            .storage_node()
            .expire_current_object_if_due(
                bucket,
                key,
                expected_version_id,
                expected_bucket_incarnation_generation,
                |raw_lifecycle, record| {
                    let Some(config) =
                        Self::parse_lifecycle_config(bucket.as_str(), raw_lifecycle)?
                    else {
                        return Ok::<bool, ServerError>(false);
                    };
                    let tags = match record.tags.as_deref() {
                        Some(tags) => tags.clone().into_pairs(),
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
            .map_err(Self::map_lifecycle_mutation_failure)??;

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
        expected_bucket_incarnation_generation: u64,
        now_millis: u64,
    ) -> Result<u64, ServerError> {
        let current_unix_seconds = Coordinator::current_unix_seconds()?;
        let reclaimed_generation_ids = self
            .storage_node()
            .delete_noncurrent_live_versions_if_due(
                bucket,
                key,
                expected_bucket_incarnation_generation,
                |raw_lifecycle, versions| {
                    let Some(config) =
                        Self::parse_lifecycle_config(bucket.as_str(), raw_lifecycle)?
                    else {
                        return Ok::<HashSet<VersionId>, ServerError>(HashSet::new());
                    };
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
                },
            )
            .map_err(Self::map_lifecycle_mutation_failure)??;

        for generation_id in &reclaimed_generation_ids {
            self.enqueue_object_payload_reclaim_for(bucket, key, *generation_id);
        }

        Ok(reclaimed_generation_ids.len() as u64)
    }

    fn expire_due_delete_markers_for_bucket(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
        bucket_info: &BucketInfo,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let Some(config) = self.lifecycle_config_for_bucket_info(bucket_info)? else {
            return Ok(());
        };

        let mut candidates = Vec::new();
        let versions = self
            .storage_node()
            .list_all_object_versions_for_bucket(&bucket_info.name)
            .map_err(Self::map_lifecycle_maintenance_failure)?;
        let mut group_start = 0usize;
        let mut groups_seen = 0usize;
        while group_start < versions.len() {
            if groups_seen > 0
                && groups_seen.is_multiple_of(LIFECYCLE_SWEEP_HEARTBEAT_INTERVAL_ITEMS)
            {
                self.heartbeat_lifecycle_sweep_claim(claim)?;
            }
            groups_seen += 1;
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
            self.heartbeat_lifecycle_sweep_claim(claim)?;
            if self.expire_delete_marker_if_due(
                &bucket_info.name,
                &key,
                version_id,
                claim.bucket_incarnation_generation,
                now_millis,
            )? {
                stats.expired_delete_markers += 1;
            } else {
                stats.skipped_expired_delete_markers += 1;
            }
        }

        Ok(())
    }

    fn expire_delete_marker_if_due(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        expected_version_id: VersionId,
        expected_bucket_incarnation_generation: u64,
        now_millis: u64,
    ) -> Result<bool, ServerError> {
        self.storage_node()
            .delete_expired_delete_marker_if_due(
                bucket,
                key,
                expected_version_id,
                expected_bucket_incarnation_generation,
                |raw_lifecycle, versions| {
                    let Some(config) =
                        Self::parse_lifecycle_config(bucket.as_str(), raw_lifecycle)?
                    else {
                        return Ok::<bool, ServerError>(false);
                    };
                    let Some(expiration) = Coordinator::evaluate_due_expired_delete_marker(
                        &config, versions, now_millis,
                    ) else {
                        return Ok::<bool, ServerError>(false);
                    };
                    Ok::<bool, ServerError>(expiration.version_id == expected_version_id)
                },
            )
            .map_err(Self::map_lifecycle_mutation_failure)?
    }

    fn abort_due_multipart_uploads_for_bucket(
        &self,
        claim: &storage::LifecycleSweepClaimRecord,
        bucket_info: &BucketInfo,
        now_millis: u64,
        stats: &mut LifecycleSweepStats,
    ) -> Result<(), ServerError> {
        let Some(config) = self.lifecycle_config_for_bucket_info(bucket_info)? else {
            return Ok(());
        };

        let mut candidates = Vec::new();
        let uploads = self
            .storage_node()
            .list_all_multipart_uploads_for_bucket(&bucket_info.name)
            .map_err(Self::map_lifecycle_maintenance_failure)?;
        for (index, upload) in uploads.into_iter().enumerate() {
            if index > 0 && index % LIFECYCLE_SWEEP_HEARTBEAT_INTERVAL_ITEMS == 0 {
                self.heartbeat_lifecycle_sweep_claim(claim)?;
            }
            if upload.state() != UploadState::InProgress && upload.state() != UploadState::Aborting
            {
                continue;
            }
            let Some(headers) = Coordinator::evaluate_multipart_lifecycle_abort_headers(
                &config,
                upload.key().as_str(),
                upload.initiated_at(),
            ) else {
                continue;
            };
            if headers.abort_time_millis <= now_millis {
                candidates.push(upload.into_key_and_upload_id());
            }
        }

        for (key, upload_id) in candidates {
            self.heartbeat_lifecycle_sweep_claim(claim)?;
            if self.abort_multipart_upload_if_due(
                &bucket_info.name,
                &key,
                &upload_id,
                claim.bucket_incarnation_generation,
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
        expected_bucket_incarnation_generation: u64,
        now_millis: u64,
    ) -> Result<bool, ServerError> {
        #[cfg(feature = "deep-tracing")]
        if let Some(trace) = observability::current_context() {
            let _ = observability::event_in_context(
                &trace,
                TRACE_TARGET,
                "multipart_abort_request",
                Some(format_args!(
                    "source=lifecycle-check bucket={:?} key={:?} upload_id={:?} now_millis={}",
                    bucket, key, upload_id, now_millis
                )),
            );
        }
        self.storage_node()
            .abort_multipart_upload_if_due(
                bucket,
                key,
                upload_id,
                expected_bucket_incarnation_generation,
                |raw_lifecycle, initiated_at| {
                    let Some(config) =
                        Self::parse_lifecycle_config(bucket.as_str(), raw_lifecycle)?
                    else {
                        return Ok::<bool, ServerError>(false);
                    };

                    let Some(headers) = Coordinator::evaluate_multipart_lifecycle_abort_headers(
                        &config,
                        key.as_str(),
                        initiated_at,
                    ) else {
                        return Ok::<bool, ServerError>(false);
                    };
                    Ok::<bool, ServerError>(headers.abort_time_millis <= now_millis)
                },
            )
            .map_err(Self::map_lifecycle_mutation_failure)?
    }

    pub(super) fn abort_multipart_upload_for_lifecycle_sweep(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        expected_bucket_incarnation_generation: u64,
    ) -> Result<bool, ServerError> {
        self.storage_node()
            .abort_multipart_upload_for_lifecycle_sweep(
                bucket,
                key,
                upload_id,
                expected_bucket_incarnation_generation,
            )
            .map_err(Self::map_lifecycle_mutation_failure)
    }

    pub(super) fn prepare_object_payload_read<'a>(
        mut self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        segments: impl IntoIterator<Item = &'a SegmentPayloadRecord>,
    ) -> Result<Self, ServerError> {
        let segments = segments.into_iter().collect::<Vec<_>>();
        match &self.storage {
            ReadStorage::Retained(retained) => {
                if !retained.contains_object_payload_segments(
                    bucket,
                    key,
                    generation_id,
                    segments.iter().map(|segment| &segment.storage_segment),
                ) {
                    return Err(ServerError::InternalError {
                        reason: "object body is outside its retained payload-read snapshot"
                            .to_string(),
                    });
                }
            }
            ReadStorage::Active(active) => {
                if !active.contains_object_payload_segments(
                    bucket,
                    key,
                    generation_id,
                    segments.iter().map(|segment| &segment.storage_segment),
                ) {
                    return Err(ServerError::InternalError {
                        reason: "object body is outside its active payload-read authority"
                            .to_string(),
                    });
                }
            }
            ReadStorage::Cluster(storage_node) => {
                let active = storage_node
                    .acquire_object_payload_read(
                        bucket,
                        key,
                        generation_id,
                        segments.iter().map(|segment| &segment.storage_segment),
                    )
                    .map_err(super::map_object_read_failure)?;
                self.storage = ReadStorage::Active(Arc::new(active));
            }
        }
        Ok(self)
    }

    #[cfg(test)]
    pub(super) fn acquire_object_payload_lease(
        &self,
        subject: &storage::test_support::TestObjectPayloadReclaimSubject,
    ) -> PayloadLease {
        let lease = storage::test_support::acquire_object_payload_reclaim_lease(
            self.storage_node(),
            subject,
        )
        .expect("current storage cluster should acquire payload lease");
        PayloadLease { lease: Some(lease) }
    }

    #[cfg(test)]
    pub(super) fn try_reclaim_object_payload(
        &self,
        subject: &storage::test_support::TestObjectPayloadReclaimSubject,
    ) -> Result<bool, ServerError> {
        storage::test_support::reclaim_object_payload_if_unleased(self.storage_node(), subject)
            .map_err(super::map_store_failure)
    }

    #[cfg(test)]
    pub(super) fn try_finalize_bucket_delete_for(
        &self,
        bucket: &BucketName,
    ) -> Result<(), ServerError> {
        match self
            .storage_node()
            .try_finalize_bucket_delete(bucket)
            .map_err(super::bucket::map_bucket_write_drain_failure)?
        {
            storage::BucketDeleteFinalizeOutcome::NotFound
            | storage::BucketDeleteFinalizeOutcome::NotDeleting
            | storage::BucketDeleteFinalizeOutcome::StaleIncarnation
            | storage::BucketDeleteFinalizeOutcome::Continue
            | storage::BucketDeleteFinalizeOutcome::Pending
            | storage::BucketDeleteFinalizeOutcome::Finalized => Ok(()),
        }
    }

    pub(super) fn read_checked_segment_payload(
        &self,
        segment: &SegmentPayloadRecord,
        part_number: Option<u32>,
        sse_customer_request: Option<&SseCustomerRequest>,
    ) -> Result<Arc<SharedPayloadBuffer>, ServerError> {
        let mut buf = self.payload_buffer_pool.checkout(0);
        let read_result =
            match &self.storage {
                ReadStorage::Active(active) => active
                    .read_segment_payload_stored_bytes_into(&segment.storage_segment, &mut buf),
                ReadStorage::Retained(retained) => retained
                    .read_segment_payload_stored_bytes_into(&segment.storage_segment, &mut buf),
                ReadStorage::Cluster(_) => {
                    return Err(ServerError::InternalError {
                        reason: "payload read runtime was not prepared with lease-bound authority"
                            .to_string(),
                    });
                }
            };
        read_result.map_err(super::map_object_read_failure)?;
        if matches!(segment.encryption, ObjectEncryption::None) {
            Ok(buf.into_shared())
        } else {
            let plaintext =
                self.decrypt_segment_if_needed(segment, part_number, sse_customer_request, &buf)?;
            buf.resize_zeroed(0);
            buf.extend_from_slice(&plaintext);
            Ok(buf.into_shared())
        }
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
                    segment.segment_index(),
                    stored_bytes,
                    segment.size() as usize,
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
                    segment.segment_index(),
                    stored_bytes,
                    segment.size() as usize,
                )
            }
        }
    }
}

fn lifecycle_sweep_error_context(error: &ServerError) -> String {
    let raw = format!("{error:?}");
    if raw.chars().count() <= LIFECYCLE_SWEEP_ERROR_CONTEXT_MAX_CHARS {
        return raw;
    }

    raw.chars()
        .take(LIFECYCLE_SWEEP_ERROR_CONTEXT_MAX_CHARS)
        .collect()
}
