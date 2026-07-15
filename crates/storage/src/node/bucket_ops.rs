use super::*;
#[cfg(test)]
use s3_types::{AclGrants, BucketVersioningState};
use std::collections::HashMap;

#[cfg(test)]
use crate::metadata_command::{
    DeleteFinalizedBucketCommand, MetadataCommandEnvelope, MetadataCommandId,
    MetadataCommandLogIndex, MetadataCommandPayload,
};
#[cfg(test)]
use crate::{
    BucketAclSummary, BucketEncryptionConfig, BucketObjectLockConfig, BucketOwnershipControls,
    CreateBucketConfig, PublicAccessBlockConfig, PutBucketSubresource,
};
use crate::{BucketFastPathIdentity, BucketInfo};

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

    #[cfg(test)]
    pub fn mark_bucket_deleting(&self, bucket: &BucketName) -> Result<(), BucketWriteDrainError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        PgMetadataStore::mark_bucket_deleting(&*bucket_pg, bucket)?;
        bucket_pg.refresh_metadata_command_state_digest()?;
        Ok(())
    }

    #[cfg(test)]
    pub fn delete_bucket_metadata(&self, bucket: &BucketName) -> Result<(), BucketWriteDrainError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        let deleting = PgMetadataStore::head_bucket_record_raw(&*bucket_pg, bucket)?;
        let state = bucket_pg.metadata_command_replica_state()?;
        let log_index = MetadataCommandLogIndex::new(state.applied_log_index + 1)
            .expect("metadata command log index is non-zero");
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(ClusterEpoch::INITIAL, PgId::new(pg_id), log_index),
            MetadataCommandPayload::DeleteFinalizedBucket(DeleteFinalizedBucketCommand::new(
                bucket.clone(),
                deleting.bucket_execution_generation,
                deleting.bucket_incarnation_generation,
            )),
        );
        bucket_pg
            .apply_metadata_command_and_record(0, &command)
            .map_err(|error| match error {
                BucketSnapshotLoadError::Store(error) => BucketWriteDrainError::Store(error),
                BucketSnapshotLoadError::Metadata(error) => BucketWriteDrainError::Metadata(error),
            })?;
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
        summary: BucketAclSummary,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.mutate_bucket_and_load_info(bucket, |bucket_pg, bucket| {
            PgMetadataStore::put_bucket_acl(bucket_pg, bucket, acl_grants, summary)
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

    pub fn load_bucket_fast_path_identities_for_pg(
        &self,
        pg_id: u32,
        buckets: &[BucketName],
    ) -> Result<HashMap<BucketName, BucketFastPathIdentity>, BucketSnapshotLoadError> {
        let bucket_pg = self.get_pg(pg_id)?;
        Ok(bucket_pg.load_bucket_fast_path_identities(buckets)?)
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
        let bucket_pg_id = self.pg_topology.bucket_pg_for(bucket);
        {
            let bucket_pg = self.get_pg(bucket_pg_id)?;
            let info = match PgMetadataStore::head_bucket_raw(&*bucket_pg, bucket) {
                Ok(info) => info,
                Err(crate::error::MetadataError::BucketNotFound { .. }) => {
                    self.finish_bucket_delete_finalize_work(bucket);
                    return Ok(BucketDeleteFinalizeOutcome::NotFound);
                }
                Err(other) => return Err(other.into()),
            };
            if info.state != BucketState::Deleting {
                self.finish_bucket_delete_finalize_work(bucket);
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

        if found_reclaim_root {
            return Ok(BucketDeleteFinalizeOutcome::Pending);
        }

        match self.delete_bucket_metadata(bucket) {
            Ok(()) => {
                self.finish_bucket_delete_finalize_work(bucket);
                Ok(BucketDeleteFinalizeOutcome::Finalized)
            }
            Err(crate::error::BucketWriteDrainError::Metadata(
                crate::error::MetadataError::BucketNotFound { .. },
            )) => {
                self.finish_bucket_delete_finalize_work(bucket);
                Ok(BucketDeleteFinalizeOutcome::NotFound)
            }
            Err(other) => Err(other),
        }
    }

    pub(crate) fn load_bucket_snapshot_from_pg(
        bucket_pg: &PgStore,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        let bucket_info = bucket_pg.head_bucket(bucket)?;
        Self::load_bucket_snapshot_from_info_and_pg(bucket_pg, bucket_info, request)
    }

    fn load_bucket_snapshot_from_info_and_pg(
        bucket_pg: &PgStore,
        bucket_info: BucketInfo,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, BucketSnapshotLoadError> {
        let policy = Self::load_bucket_snapshot_subresource(
            bucket_pg,
            &bucket_info.name,
            request.policy,
            BucketSubresourceKind::Policy,
        )?;
        let tags = Self::load_bucket_snapshot_subresource(
            bucket_pg,
            &bucket_info.name,
            request.tags.should_load(&bucket_info),
            BucketSubresourceKind::Tagging,
        )?;
        let lifecycle = Self::load_bucket_snapshot_subresource(
            bucket_pg,
            &bucket_info.name,
            request.lifecycle,
            BucketSubresourceKind::Lifecycle,
        )?;
        let cors = Self::load_bucket_snapshot_subresource(
            bucket_pg,
            &bucket_info.name,
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

    #[cfg(test)]
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
