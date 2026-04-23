use super::*;

impl SharedStorageNode {
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
                    drop(bucket_pg);
                    std::thread::sleep(std::time::Duration::from_millis(1));
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
            drop(bucket_pg);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    pub fn begin_bucket_delete(&self, bucket: &BucketName) -> Result<(), BucketWriteDrainError> {
        let drain = self.begin_bucket_write_drain(bucket)?;

        let mut bucket_not_empty = false;
        self.pg_topology.for_each_pg(|pg_id| {
            let pg = self.get_pg(pg_id)?;
            let versions = pg.list_object_versions(&ListObjectVersionsReq {
                bucket: bucket.clone(),
                prefix: None,
                key_marker: None,
                version_id_marker: None,
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

    pub fn mark_bucket_deleting(&self, bucket: &BucketName) -> Result<(), BucketWriteDrainError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        PgMetadataStore::mark_bucket_deleting(&*bucket_pg, bucket)?;
        Ok(())
    }

    pub fn delete_bucket_metadata(&self, bucket: &BucketName) -> Result<(), BucketWriteDrainError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        PgMetadataStore::delete_bucket(&*bucket_pg, bucket)?;
        Ok(())
    }

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

    pub fn with_bucket_write_snapshot<T, E>(
        &self,
        bucket: &BucketName,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<T, E>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        loop {
            let pg_id = self.pg_topology.bucket_pg_for(bucket);
            let bucket_pg = self.get_pg(pg_id)?;
            match PgMetadataStore::acquire_bucket_write_reservation(&*bucket_pg, bucket) {
                Ok(_info) => {
                    let snapshot = Self::load_bucket_snapshot_from_pg(&bucket_pg, bucket, request)?;
                    drop(bucket_pg);
                    let result = action(snapshot);
                    let release_result = self.release_bucket_write_reservation(bucket);
                    return Self::finish_bucket_write_snapshot(result, release_result);
                }
                Err(crate::error::MetadataError::BucketWriteDraining) => {
                    drop(bucket_pg);
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(other) => return Err(other.into()),
            }
        }
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

    pub(super) fn load_bucket_snapshot_from_pg(
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

    fn release_bucket_write_reservation(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketSnapshotLoadError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        PgMetadataStore::release_bucket_write_reservation(&*bucket_pg, bucket)?;
        Ok(())
    }

    pub(super) fn end_bucket_write_drain(
        &self,
        bucket: &BucketName,
    ) -> Result<(), BucketWriteDrainError> {
        let pg_id = self.pg_topology.bucket_pg_for(bucket);
        let bucket_pg = self.get_pg(pg_id)?;
        PgMetadataStore::end_bucket_write_drain(&*bucket_pg, bucket)?;
        Ok(())
    }

    pub(super) fn finish_bucket_write_snapshot<T, E>(
        result: Result<T, E>,
        release_result: Result<(), BucketSnapshotLoadError>,
    ) -> Result<Result<T, E>, BucketSnapshotLoadError> {
        match (result, release_result) {
            (Ok(value), Ok(())) => Ok(Ok(value)),
            (Ok(_), Err(err)) => Err(err),
            (Err(err), Ok(())) => Ok(Err(err)),
            (Err(err), Err(_)) => Ok(Err(err)),
        }
    }
}
