use super::*;
#[cfg(test)]
use s3_types::{AclGrants, BucketVersioningState};
use std::collections::HashMap;

use crate::BucketInfo;
#[cfg(test)]
use crate::{
    BucketEncryptionConfig, BucketObjectLockConfig, BucketOwnershipControls, CreateBucketConfig,
    PublicAccessBlockConfig, PutBucketSubresource,
};

impl SharedStorageNode {
    #[cfg(test)]
    pub fn create_bucket_with_config_and_load_info(
        &self,
        config: &CreateBucketConfig<'_>,
    ) -> Result<BucketCreateAttemptOutcome, BucketSnapshotLoadError> {
        let bucket = BucketName::try_from(config.name).map_err(|reason| {
            crate::error::MetadataError::InvalidBucketName {
                reason: reason.to_string(),
            }
        })?;
        let _bucket_guard = self.lock_bucket(&bucket);
        let pg_id = self.pg_topology.bucket_pg_for(&bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        match bucket_pg.create_bucket_with_config(config) {
            Ok(()) => Ok(BucketCreateAttemptOutcome::Created(
                bucket_pg.head_bucket(&bucket)?,
            )),
            Err(crate::error::MetadataError::BucketAlreadyExists) => Ok(
                BucketCreateAttemptOutcome::Exists(bucket_pg.head_bucket_raw(&bucket)?),
            ),
            Err(other) => Err(other.into()),
        }
    }

    #[cfg(test)]
    fn mutate_bucket_and_load_info(
        &self,
        bucket: &BucketName,
        mutate: impl FnOnce(&PgStore, &BucketName) -> Result<(), crate::error::MetadataError>,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        mutate(&bucket_pg, bucket)?;
        Ok(bucket_pg.head_bucket_raw(bucket)?)
    }

    pub fn load_bucket_snapshot(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        Self::load_bucket_snapshot_from_pg(&bucket_pg, bucket, request)
    }

    pub fn begin_bucket_write_drain(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketWriteDrainGuard<'_>, BucketWriteDrainError> {
        loop {
            let pg_id = self.pg_topology.bucket_pg_for(bucket);
            let bucket_pg = self.get_pg(pg_id)?;
            match PgMetadataStore::begin_bucket_write_drain(&*bucket_pg, bucket) {
                Ok(()) => break,
                Err(crate::error::MetadataError::BucketWriteDraining) => {
                    let (generation_lock, generation_cvar) =
                        &self.bucket_coordination[self.bucket_lock_index(bucket)];
                    let mut generation = generation_lock.lock().unwrap();
                    let observed_generation = *generation;
                    drop(bucket_pg);
                    while *generation == observed_generation {
                        generation = generation_cvar.wait(generation).unwrap();
                    }
                }
                Err(other) => return Err(other.into()),
            }
        }

        loop {
            let pg_id = self.pg_topology.bucket_pg_for(bucket);
            let bucket_pg = self.get_pg(pg_id)?;
            let info = PgMetadataStore::head_bucket(&*bucket_pg, bucket)?;
            if info.active_write_reservations == 0 {
                return Ok(BucketWriteDrainGuard {
                    node: self,
                    bucket: bucket.clone(),
                    persisted: false,
                });
            }
            let (generation_lock, generation_cvar) =
                &self.bucket_coordination[self.bucket_lock_index(bucket)];
            let mut generation = generation_lock.lock().unwrap();
            let observed_generation = *generation;
            super::maybe_run_bucket_write_drain_wait_hook(bucket);
            drop(bucket_pg);
            while *generation == observed_generation {
                generation = generation_cvar.wait(generation).unwrap();
            }
        }
    }

    #[cfg(test)]
    pub fn begin_bucket_delete(&self, bucket: &BucketName) -> Result<(), BucketWriteDrainError> {
        let drain = self.begin_bucket_write_drain(bucket)?;
        super::maybe_run_after_begin_bucket_delete_drain_hook(bucket);

        let mut bucket_not_empty = false;
        self.pg_topology.for_each_pg(|pg_id| {
            let pg = self.get_pg(pg_id)?;
            let versions = pg.list_object_versions(&ListObjectVersionsReq {
                bucket: bucket.clone(),
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                start_at: None,
                max_keys: 1,
            })?;
            if !versions.versions.is_empty() {
                bucket_not_empty = true;
                return Ok::<(), BucketWriteDrainError>(());
            }

            let uploads = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                bucket: bucket.clone(),
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1,
            })?;
            if !uploads.uploads.is_empty() {
                bucket_not_empty = true;
                return Ok(());
            }

            let sessions = pg.list_all_stream_uploads()?;
            if sessions
                .iter()
                .any(|session| session.bucket == bucket.as_str())
            {
                bucket_not_empty = true;
            }
            Ok(())
        })?;

        if bucket_not_empty {
            return Err(crate::error::MetadataError::BucketNotEmpty.into());
        }

        self.mark_bucket_deleting(bucket)?;
        drain.persist();
        Ok(())
    }

    #[cfg(test)]
    pub fn mark_bucket_deleting(&self, bucket: &BucketName) -> Result<(), BucketWriteDrainError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        PgMetadataStore::mark_bucket_deleting(&*bucket_pg, bucket)?;
        bucket_pg.refresh_metadata_command_state_digest()?;
        drop(bucket_pg);
        self.notify_bucket_coordination_change(bucket);
        Ok(())
    }

    #[cfg(test)]
    pub fn delete_bucket_metadata(&self, bucket: &BucketName) -> Result<(), BucketWriteDrainError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        PgMetadataStore::delete_bucket(&*bucket_pg, bucket)?;
        Ok(())
    }

    #[cfg(test)]
    pub fn put_bucket_versioning_and_load_info(
        &self,
        bucket: &BucketName,
        state: BucketVersioningState,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.mutate_bucket_and_load_info(bucket, |bucket_pg, bucket| {
            PgMetadataStore::put_bucket_versioning(bucket_pg, bucket, state)
        })
    }

    #[cfg(test)]
    pub fn put_bucket_object_lock_and_load_info(
        &self,
        bucket: &BucketName,
        config: BucketObjectLockConfig,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.mutate_bucket_and_load_info(bucket, |bucket_pg, bucket| {
            PgMetadataStore::put_bucket_object_lock(bucket_pg, bucket, config)
        })
    }

    #[cfg(test)]
    pub fn put_bucket_encryption_and_load_info(
        &self,
        bucket: &BucketName,
        config: BucketEncryptionConfig,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.mutate_bucket_and_load_info(bucket, |bucket_pg, bucket| {
            PgMetadataStore::put_bucket_encryption(bucket_pg, bucket, config)
        })
    }

    #[cfg(test)]
    pub fn put_bucket_public_access_block_and_load_info(
        &self,
        bucket: &BucketName,
        config: PublicAccessBlockConfig,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.mutate_bucket_and_load_info(bucket, |bucket_pg, bucket| {
            PgMetadataStore::put_bucket_public_access_block(bucket_pg, bucket, config)
        })
    }

    #[cfg(test)]
    pub fn delete_bucket_public_access_block_and_load_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.mutate_bucket_and_load_info(bucket, |bucket_pg, bucket| {
            PgMetadataStore::delete_bucket_public_access_block(bucket_pg, bucket)
        })
    }

    #[cfg(test)]
    pub fn put_bucket_ownership_controls_and_load_info(
        &self,
        bucket: &BucketName,
        config: BucketOwnershipControls,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.mutate_bucket_and_load_info(bucket, |bucket_pg, bucket| {
            PgMetadataStore::put_bucket_ownership_controls(bucket_pg, bucket, config)
        })
    }

    #[cfg(test)]
    pub fn delete_bucket_ownership_controls_and_load_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.mutate_bucket_and_load_info(bucket, |bucket_pg, bucket| {
            PgMetadataStore::delete_bucket_ownership_controls(bucket_pg, bucket)
        })
    }

    #[cfg(test)]
    pub fn put_bucket_abac_enabled_and_load_info(
        &self,
        bucket: &BucketName,
        enabled: bool,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.mutate_bucket_and_load_info(bucket, |bucket_pg, bucket| {
            PgMetadataStore::put_bucket_abac_enabled(bucket_pg, bucket, enabled)
        })
    }

    #[cfg(test)]
    pub fn put_bucket_acl_and_load_info(
        &self,
        bucket: &BucketName,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.mutate_bucket_and_load_info(bucket, |bucket_pg, bucket| {
            PgMetadataStore::put_bucket_acl(
                bucket_pg,
                bucket,
                acl_grants,
                public_read,
                public_write,
            )
        })
    }

    #[cfg(test)]
    pub fn put_bucket_subresource_and_load_info(
        &self,
        bucket: &BucketName,
        req: PutBucketSubresource<'_>,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.mutate_bucket_and_load_info(bucket, |bucket_pg, bucket| {
            PgMetadataStore::put_bucket_subresource(bucket_pg, bucket, req)
        })
    }

    #[cfg(test)]
    pub fn delete_bucket_subresource_and_load_info(
        &self,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.mutate_bucket_and_load_info(bucket, |bucket_pg, bucket| {
            PgMetadataStore::delete_bucket_subresource(bucket_pg, bucket, kind)
        })
    }

    pub fn head_bucket_info(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        let bucket_pg = self.get_pg(self.pg_topology.bucket_pg_for(bucket))?;
        Ok(PgMetadataStore::head_bucket(&*bucket_pg, bucket)?)
    }

    pub fn load_bucket_execution_generations_for_pg(
        &self,
        pg_id: u32,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, u64>, BucketSnapshotLoadError> {
        let bucket_pg = self.get_pg(pg_id)?;
        Ok(bucket_pg.load_bucket_execution_generations(buckets)?)
    }

    pub fn get_bucket_subresource(
        &self,
        bucket: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<String>, BucketSnapshotLoadError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        Ok(
            PgMetadataStore::get_bucket_subresource(&*bucket_pg, bucket, kind)?
                .map(|stored| stored.body),
        )
    }

    #[cfg(test)]
    pub fn try_finalize_bucket_delete(
        &self,
        bucket: &BucketName,
    ) -> Result<BucketDeleteFinalizeOutcome, BucketWriteDrainError> {
        let _bucket_guard = self.lock_bucket(bucket);
        let bucket_pg_id = self.pg_topology.bucket_pg_for(bucket);
        {
            let bucket_pg = self.get_pg(bucket_pg_id)?;
            let info = match PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket) {
                Ok(info) => info,
                Err(crate::error::MetadataError::BucketNotFound { .. }) => {
                    return Ok(BucketDeleteFinalizeOutcome::NotFound);
                }
                Err(other) => return Err(other.into()),
            };
            if info.state != BucketState::Deleting {
                return Ok(BucketDeleteFinalizeOutcome::NotDeleting);
            }
        }

        let mut found_visible_data = false;
        let mut found_reclaim_root = false;
        let mut reclaim_roots = Vec::new();
        self.pg_topology.for_each_pg(|pg_id| {
            let pg = self.get_pg(pg_id)?;
            let versions = pg.list_object_versions(&ListObjectVersionsReq {
                bucket: bucket.clone(),
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                start_at: None,
                max_keys: 1,
            })?;
            if !versions.versions.is_empty() {
                found_visible_data = true;
                return Ok::<(), BucketWriteDrainError>(());
            }

            let uploads = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                bucket: bucket.clone(),
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1,
            })?;
            if !uploads.uploads.is_empty() {
                found_visible_data = true;
                return Ok(());
            }

            if let Some(root) = PgMetadataStore::get_bucket_payload_reclaim_root(&*pg, bucket)? {
                found_reclaim_root = true;
                reclaim_roots.push(root);
            }
            Ok(())
        })?;

        if found_visible_data {
            return Ok(BucketDeleteFinalizeOutcome::Pending);
        }

        for root in &reclaim_roots {
            if self.object_payload_lease_count(&root.bucket, &root.key, root.generation_id) == 0 {
                self.enqueue_object_payload_reclaim(&root.bucket, &root.key, root.generation_id);
            }
        }

        if found_reclaim_root || self.bucket_object_payload_lease_count(bucket) != 0 {
            return Ok(BucketDeleteFinalizeOutcome::Pending);
        }

        self.pg_topology.for_each_pg(|pg_id| {
            let pg = self.get_pg(pg_id)?;
            PgMetadataStore::delete_completed_multipart_uploads_for_bucket(&*pg, bucket)?;
            Ok::<(), BucketWriteDrainError>(())
        })?;

        match self.delete_bucket_metadata(bucket) {
            Ok(()) => Ok(BucketDeleteFinalizeOutcome::Finalized),
            Err(crate::error::BucketWriteDrainError::Metadata(
                crate::error::MetadataError::BucketNotFound { .. },
            )) => Ok(BucketDeleteFinalizeOutcome::NotFound),
            Err(other) => Err(other),
        }
    }

    pub(crate) fn with_bucket_write_reservation_snapshot<T, E>(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<Result<T, E>, BucketSnapshotLoadError>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        loop {
            let pg_id = self.pg_topology.bucket_pg_for(bucket);
            let bucket_pg = self.get_pg(pg_id)?;
            match PgMetadataStore::acquire_bucket_write_reservation(&*bucket_pg, bucket) {
                Ok(_info) => {
                    let result = (|| {
                        let snapshot =
                            Self::load_bucket_snapshot_from_pg(&bucket_pg, bucket, request)?;
                        drop(bucket_pg);
                        action(snapshot)
                    })();
                    let release_result = self.release_bucket_write_reservation(bucket);
                    return Self::finish_bucket_write_snapshot_operation(result, release_result);
                }
                Err(crate::error::MetadataError::BucketWriteDraining) => {
                    super::maybe_run_bucket_write_reservation_retry_hook(bucket);
                    let (generation_lock, generation_cvar) =
                        &self.bucket_coordination[self.bucket_lock_index(bucket)];
                    let mut generation = generation_lock.lock().unwrap();
                    let observed_generation = *generation;
                    match PgMetadataStore::head_bucket(&*bucket_pg, bucket) {
                        Ok(_) => {}
                        Err(crate::error::MetadataError::BucketNotFound { .. }) => {
                            return Err(crate::error::MetadataError::BucketNotFound {
                                name: bucket.clone(),
                            }
                            .into());
                        }
                        Err(other) => return Err(other.into()),
                    }
                    drop(bucket_pg);
                    super::maybe_run_after_bucket_write_reservation_retry_hook(bucket);
                    while *generation == observed_generation {
                        generation = generation_cvar.wait(generation).unwrap();
                    }
                }
                Err(other) => return Err(other.into()),
            }
        }
    }

    pub fn with_bucket_write_snapshot<T, E>(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<T, E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        self.with_bucket_write_reservation_snapshot(bucket, request, |snapshot| {
            Ok(action(snapshot))
        })
    }

    pub fn load_bucket_snapshot_pair(
        &self,
        source: (&BucketName, BucketSnapshotRequest),
        destination: (&BucketName, BucketSnapshotRequest),
    ) -> Result<BucketSnapshotPair, BucketSnapshotLoadError> {
        if source.0 == destination.0 {
            let merged_request = BucketSnapshotRequest {
                policy: source.1.policy || destination.1.policy,
                tags: match (source.1.tags, destination.1.tags) {
                    (BucketSnapshotTagsRequest::Always, _)
                    | (_, BucketSnapshotTagsRequest::Always) => BucketSnapshotTagsRequest::Always,
                    (BucketSnapshotTagsRequest::IfBucketAbacEnabled, _)
                    | (_, BucketSnapshotTagsRequest::IfBucketAbacEnabled) => {
                        BucketSnapshotTagsRequest::IfBucketAbacEnabled
                    }
                    (
                        BucketSnapshotTagsRequest::NotRequested,
                        BucketSnapshotTagsRequest::NotRequested,
                    ) => BucketSnapshotTagsRequest::NotRequested,
                },
                lifecycle: source.1.lifecycle || destination.1.lifecycle,
                cors: source.1.cors || destination.1.cors,
            };
            let bucket = self.load_bucket_snapshot(source.0, merged_request)?;
            return Ok(BucketSnapshotPair::Same {
                bucket: Box::new(bucket),
            });
        }

        let guards = self.lock_bucket_pair_pgs(
            self.pg_topology.bucket_pg_for(source.0),
            self.pg_topology.bucket_pg_for(destination.0),
        )?;
        match guards {
            BucketPairPgGuards::Same { bucket } => Ok(BucketSnapshotPair::Distinct {
                source: Box::new(Self::load_bucket_snapshot_from_pg(
                    &bucket, source.0, source.1,
                )?),
                destination: Box::new(Self::load_bucket_snapshot_from_pg(
                    &bucket,
                    destination.0,
                    destination.1,
                )?),
            }),
            BucketPairPgGuards::Distinct {
                source: source_pg,
                destination: destination_pg,
            } => Ok(BucketSnapshotPair::Distinct {
                source: Box::new(Self::load_bucket_snapshot_from_pg(
                    &source_pg, source.0, source.1,
                )?),
                destination: Box::new(Self::load_bucket_snapshot_from_pg(
                    &destination_pg,
                    destination.0,
                    destination.1,
                )?),
            }),
        }
    }

    pub(crate) fn load_bucket_snapshot_from_pg(
        bucket_pg: &PgStore,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        let bucket_info = bucket_pg.head_bucket(bucket)?;
        let policy = Self::load_bucket_snapshot_subresource(
            bucket_pg,
            bucket,
            request.policy,
            BucketSubresourceKind::Policy,
        )?;
        let tags = Self::load_bucket_snapshot_subresource(
            bucket_pg,
            bucket,
            request.tags.should_load(&bucket_info),
            BucketSubresourceKind::Tagging,
        )?;
        let lifecycle = Self::load_bucket_snapshot_subresource(
            bucket_pg,
            bucket,
            request.lifecycle,
            BucketSubresourceKind::Lifecycle,
        )?;
        let cors = Self::load_bucket_snapshot_subresource(
            bucket_pg,
            bucket,
            request.cors,
            BucketSubresourceKind::Cors,
        )?;

        Ok(BucketSnapshot {
            bucket: bucket_info,
            request,
            policy,
            tags,
            lifecycle,
            cors,
        })
    }

    fn load_bucket_snapshot_subresource(
        bucket_pg: &PgStore,
        bucket: &BucketName,
        requested: bool,
        kind: BucketSubresourceKind,
    ) -> Result<LoadedBucketSubresource<String>, BucketSnapshotLoadError> {
        if !requested {
            return Ok(LoadedBucketSubresource::NotRequested);
        }
        Ok(
            match PgMetadataStore::get_bucket_subresource(bucket_pg, bucket, kind)? {
                Some(stored) => LoadedBucketSubresource::Loaded(stored.body),
                None => LoadedBucketSubresource::Missing,
            },
        )
    }

    pub(super) fn release_bucket_write_reservation(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        PgMetadataStore::release_bucket_write_reservation(&*bucket_pg, bucket)?;
        drop(bucket_pg);
        self.notify_bucket_coordination_change(bucket);
        Ok(())
    }

    pub(super) fn end_bucket_write_drain(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        PgMetadataStore::end_bucket_write_drain(&*bucket_pg, bucket)?;
        drop(bucket_pg);
        self.notify_bucket_coordination_change(bucket);
        Ok(())
    }

    pub(super) fn finish_bucket_write_snapshot_operation<T, E>(
        result: Result<Result<T, E>, BucketSnapshotLoadError>,
        release_result: Result<(), BucketSnapshotLoadError>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        match (result, release_result) {
            (Ok(Ok(value)), Ok(())) => Ok(Ok(value)),
            (Ok(Ok(_)), Err(err)) => Err(err),
            (Ok(Err(err)), Ok(())) => Ok(Err(err)),
            (Ok(Err(err)), Err(_)) => Ok(Err(err)),
            (Err(err), Ok(())) => Err(err),
            (Err(err), Err(_)) => Err(err),
        }
    }
}
