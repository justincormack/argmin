/// Request-scoped, subject-bound authority for removing one abandoned stream
/// upload, including a prompt attempt after the active route deadline elapses.
///
/// This capability is intentionally non-cloneable. It retains only the route
/// generation and object identity captured while the request was admitted. It
/// is not the durable cleanup handoff: frontend-created stream sessions also
/// persist the admission's immutable authority deadline, allowing the
/// current-route sweeper to resume cleanup after this capability and the
/// originating [`StorageClusterRouteAdmission`] have been dropped.
pub struct RetainedStreamUploadCleanup {
    cluster: Arc<StorageCluster>,
    cluster_epoch: ClusterEpoch,
    object_pg_id: ObjectMetadataPgId,
    bucket: BucketName,
    key: ObjectKey,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StreamSessionSweepSummary {
    pub(crate) discovered: usize,
    pub(crate) due: usize,
    pub(crate) cleaned: usize,
    pub(crate) reservation_check_failed: usize,
    pub(crate) abort_failed: usize,
}

impl RetainedStreamUploadCleanup {
    pub fn abort(&self, session_id: &SessionId) -> Result<(), crate::StreamUploadFailure> {
        self.cluster
            .abort_stream_upload_session_with_retained_cleanup(
                self.cluster_epoch,
                self.object_pg_id,
                &self.bucket,
                &self.key,
                session_id,
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }
}

/// Non-cloneable active bucket-metadata authority derived from one admitted
/// frontend request.
///
/// The bucket and its routed PG are fixed at construction. Operations do not
/// accept either value again, so callers cannot combine authority for one
/// bucket with another subject or PG. Every operation also rechecks the
/// admission's captured absolute deadline before reaching a node client.
///
/// ```compile_fail
/// use storage::ActiveBucketRoute;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn cache_route(route: &ActiveBucketRoute<'_>) {
///     require_clone(route);
/// }
/// ```
pub struct ActiveBucketRoute<'admission> {
    admission: &'admission StorageClusterRouteAdmission,
    bucket: BucketName,
    pg_id: BucketPgId,
}

struct BucketMetadataMutationEffectRoute<'a> {
    pg_id: BucketPgId,
    bucket: &'a BucketName,
    effect_fence: AdmittedRouteEffectFence,
}

/// Non-cloneable active authority for an account-scoped bucket metadata scan.
///
/// The scan is fixed to the admitted runtime-map generation. It rechecks the
/// captured deadline before every bucket-PG node access, so a long scan cannot
/// continue under a later renewal of the same generation.
///
/// ```compile_fail
/// use storage::ActiveBucketMetadataScan;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn cache_scan(scan: &ActiveBucketMetadataScan<'_>) {
///     require_clone(scan);
/// }
/// ```
pub struct ActiveBucketMetadataScan<'admission> {
    admission: &'admission StorageClusterRouteAdmission,
    owner_canonical_id: CanonicalUserId,
}

impl ActiveBucketMetadataScan<'_> {
    pub fn list_buckets_for_owner(&self) -> Result<Vec<BucketInfo>, crate::BucketListingFailure> {
        self.admission
            .cluster
            .list_buckets_for_owner_with_route_validation(self.owner_canonical_id.as_str(), || {
                self.admission.require_valid_now_raw()
            })
            .map_err(crate::BucketListingFailure::from_object_pg_action)
    }
}

/// Non-cloneable active authority for a bucket-scoped object metadata scan.
///
/// The bucket and runtime-map generation are fixed at construction. Every
/// object-metadata page read rechecks the request admission's captured
/// deadline, so pagination or fan-out cannot continue under a later renewal.
///
/// ```compile_fail
/// use storage::ActiveObjectMetadataScan;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn cache_scan(scan: &ActiveObjectMetadataScan<'_>) {
///     require_clone(scan);
/// }
/// ```
pub struct ActiveObjectMetadataScan<'admission> {
    admission: &'admission StorageClusterRouteAdmission,
    bucket: BucketName,
}

/// Non-cloneable active authority for one object's metadata snapshot.
///
/// The bucket, key, and routed object-metadata PG are fixed at construction.
/// Snapshot loading rechecks the request admission immediately before every
/// node access, including stale-subject retries.
///
/// ```compile_fail
/// use storage::ActiveObjectReadRoute;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn cache_route(route: &ActiveObjectReadRoute<'_>) {
///     require_clone(route);
/// }
/// ```
pub struct ActiveObjectReadRoute<'admission> {
    admission: &'admission StorageClusterRouteAdmission,
    bucket: BucketName,
    key: ObjectKey,
    version_id: Option<VersionId>,
    snapshot_mode: ObjectReadSnapshotMode,
    pg_id: ObjectMetadataPgId,
}

/// Non-cloneable active authority for one object's metadata mutations.
///
/// The bucket, key, requested version, and routed object-metadata PG are fixed
/// at construction. Mutation entry and every retry recheck the request's
/// immutable admitted deadline. Once a metadata command is durably installed,
/// applying it is convergence of that already-authorized command rather than
/// a new frontend effect.
///
/// ```compile_fail
/// use storage::ActiveObjectMetadataMutationRoute;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn cache_route(route: &ActiveObjectMetadataMutationRoute<'_>) {
///     require_clone(route);
/// }
/// ```
pub struct ActiveObjectMetadataMutationRoute<'admission> {
    admission: &'admission StorageClusterRouteAdmission,
    bucket: BucketName,
    key: ObjectKey,
    version_id: Option<VersionId>,
    pg_id: ObjectMetadataPgId,
}

/// Non-cloneable active authority for one complete PutObject workflow.
///
/// Bucket authorization, current-object authorization, generation
/// reservation, staged payload writes, and final metadata publication remain
/// bound to the same bucket, key, runtime-map generation, publication domain,
/// and immutable request deadline.
///
/// ```compile_fail
/// use storage::ActivePutObjectRoute;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn cache_route(route: &ActivePutObjectRoute<'_>) {
///     require_clone(route);
/// }
/// ```
pub struct ActivePutObjectRoute<'admission> {
    admission: &'admission StorageClusterRouteAdmission,
    bucket: BucketName,
    key: ObjectKey,
    bucket_pg_id: BucketPgId,
    object_pg_id: ObjectMetadataPgId,
}

/// Non-cloneable active authority for multipart control operations on one
/// object.
///
/// The bucket, key, and routed object-metadata PG are fixed at construction.
/// Multipart operations do not accept those values again, so a prepared
/// request for another object cannot be published through this capability.
///
/// ```compile_fail
/// use storage::ActiveMultipartObjectRoute;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn cache_route(route: &ActiveMultipartObjectRoute<'_>) {
///     require_clone(route);
/// }
/// ```
pub struct ActiveMultipartObjectRoute<'admission> {
    admission: &'admission StorageClusterRouteAdmission,
    bucket: BucketName,
    key: ObjectKey,
    pg_id: ObjectMetadataPgId,
}

struct ObjectReadMetadataRoute<'a> {
    bucket: &'a BucketName,
    key: &'a ObjectKey,
    version_id: Option<VersionId>,
    snapshot_mode: ObjectReadSnapshotMode,
    pg_id: ObjectMetadataPgId,
}

struct ObjectMetadataMutationEffectRoute<'a> {
    pg_id: ObjectMetadataPgId,
    bucket: &'a BucketName,
    key: &'a ObjectKey,
    requested_version_id: Option<VersionId>,
    effect_fence: AdmittedRouteEffectFence,
}

#[derive(Clone, Copy)]
struct PutObjectMutationEffectRoute<'a> {
    bucket_pg_id: BucketPgId,
    object_pg_id: ObjectMetadataPgId,
    bucket: &'a BucketName,
    key: &'a ObjectKey,
    effect_fence: AdmittedRouteEffectFence,
}

#[derive(Clone, Copy)]
struct PlacedSegmentPayloadWrite<'a> {
    segment_okh: &'a [u8; 16],
    segment_vid: GenerationId,
    data: &'a [u8],
}

struct MultipartObjectMutationEffectRoute<'a> {
    pg_id: ObjectMetadataPgId,
    bucket: &'a BucketName,
    key: &'a ObjectKey,
    effect_fence: AdmittedRouteEffectFence,
}

impl ActiveObjectReadRoute<'_> {
    /// Load the authorization subject for this exact object route.
    ///
    /// This is the narrow metadata-only path used by object subresource
    /// reads. It rechecks the immutable admitted deadline immediately before
    /// the storage-node access and does not confer mutation authority.
    pub fn load_object_if<T, E>(
        &self,
        action: impl FnOnce(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<T, E>, ObjectReadFailure> {
        let route = ObjectReadMetadataRoute {
            bucket: &self.bucket,
            key: &self.key,
            version_id: self.version_id,
            snapshot_mode: self.snapshot_mode,
            pg_id: self.pg_id,
        };
        self.admission
            .cluster
            .load_object_if_on_route(&route, action, || self.admission.require_valid_now_raw())
            .map_err(ObjectReadFailure::from_object_pg_action)
    }

    pub fn load_object_read_snapshot_if<T, E>(
        &self,
        action: impl FnMut(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<ObjectReadSnapshotOutcome<T>, E>, ObjectReadFailure> {
        let route = ObjectReadMetadataRoute {
            bucket: &self.bucket,
            key: &self.key,
            version_id: self.version_id,
            snapshot_mode: self.snapshot_mode,
            pg_id: self.pg_id,
        };
        self.admission
            .cluster
            .load_object_read_snapshot_if_on_route(&route, action, || {
                self.admission.require_valid_now_raw()
            })
            .map_err(ObjectReadFailure::from_object_pg_action)
    }

    pub fn load_leased_object_read_snapshot_if<T, E>(
        &self,
        action: impl FnMut(&StoredObject) -> Result<T, E>,
    ) -> Result<Result<LeasedObjectReadSnapshotOutcome<T>, E>, ObjectReadFailure> {
        let route = ObjectReadMetadataRoute {
            bucket: &self.bucket,
            key: &self.key,
            version_id: self.version_id,
            snapshot_mode: self.snapshot_mode,
            pg_id: self.pg_id,
        };
        let outcome = self
            .admission
            .cluster
            .load_leased_object_read_snapshot_if_on_route(&route, action, || {
                self.admission.require_valid_now_raw()
            })
            .map_err(ObjectReadFailure::from_object_pg_action)?;
        Ok(outcome.map(|mut outcome| {
            outcome.leased_snapshot.repair_fence = Some(RetainedActiveRouteRepairFence {
                gate: self.admission._permit.gate.clone(),
                publication_generation: self.admission._permit.gate.publication_generation(),
                admitted_lease: self.admission.admitted_lease,
            });
            outcome
        }))
    }

    /// Retain narrowly scoped payload authority for the exact snapshot loaded
    /// through this route.
    ///
    /// The returned capability owns only deletion-exclusion leases and exact
    /// segment descriptors. It does not retain this request's publication
    /// admission and therefore cannot perform another object metadata lookup.
    pub fn retain_object_payload_read(
        &self,
        leased_snapshot: LeasedObjectReadSnapshot,
    ) -> Result<Option<RetainedObjectPayloadRead>, ObjectReadFailure> {
        if !Arc::ptr_eq(&self.admission.cluster, &leased_snapshot.cluster)
            || leased_snapshot.bucket != self.bucket
            || leased_snapshot.key != self.key
            || leased_snapshot.version_id != self.version_id
            || leased_snapshot.snapshot_mode != self.snapshot_mode
            || leased_snapshot.pg_id != self.pg_id
        {
            return Err(ObjectReadFailure::from_store(
                StoreError::PayloadShardSetMismatch {
                    reason: "leased object snapshot provenance does not match active read route"
                        .to_string(),
                },
            ));
        }
        self.admission
            .cluster
            .retain_object_payload_read_from_leased_snapshot(leased_snapshot, || {
                self.admission.require_valid_now_raw()
            })
            .map_err(ObjectReadFailure::from_store)
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_probe_object_pg_available(&self) -> Result<bool, ObjectReadFailure> {
        self.admission
            .require_valid_now_raw()
            .map_err(ObjectReadFailure::from_store)?;
        self.admission
            .cluster
            .try_probe_object_pg_available(&self.bucket, &self.key)
            .map_err(ObjectReadFailure::from_object_pg_action)
    }
}

impl ActiveObjectMetadataMutationRoute<'_> {
    fn effect_route(&self) -> ObjectMetadataMutationEffectRoute<'_> {
        ObjectMetadataMutationEffectRoute {
            pg_id: self.pg_id,
            bucket: &self.bucket,
            key: &self.key,
            requested_version_id: self.version_id,
            effect_fence: self.admission.effect_fence(),
        }
    }

    pub fn put_tags_if<E>(
        &self,
        tags: &SerializedTagSet,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<VersionId, E>, ObjectMetadataMutationFailure> {
        self.admission
            .cluster
            .put_object_metadata_if_with_route_validation(
                self.effect_route(),
                || self.admission.require_valid_now_raw(),
                |stored| {
                    let version_id = action(stored)?;
                    Ok((
                        version_id,
                        version_id,
                        PutObjectMetadataMutation::PutTags(tags.clone()),
                    ))
                },
            )
            .map_err(ObjectMetadataMutationFailure::from_object_pg_action)
    }

    pub fn delete_tags_if<E>(
        &self,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<(), E>, ObjectMetadataMutationFailure> {
        self.admission
            .cluster
            .put_object_metadata_if_with_route_validation(
                self.effect_route(),
                || self.admission.require_valid_now_raw(),
                |stored| {
                    let version_id = action(stored)?;
                    Ok(((), version_id, PutObjectMetadataMutation::DeleteTags))
                },
            )
            .map_err(ObjectMetadataMutationFailure::from_object_pg_action)
    }

    /// Returns the version id the retention was applied to.
    pub fn put_retention_if<E>(
        &self,
        retention: ObjectRetention,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<VersionId, E>, ObjectMetadataMutationFailure> {
        self.admission
            .cluster
            .put_object_metadata_if_with_route_validation(
                self.effect_route(),
                || self.admission.require_valid_now_raw(),
                |stored| {
                    let version_id = action(stored)?;
                    Ok((
                        version_id,
                        version_id,
                        PutObjectMetadataMutation::PutRetention(retention),
                    ))
                },
            )
            .map_err(ObjectMetadataMutationFailure::from_object_pg_action)
    }

    /// Returns the version id the legal hold was applied to.
    pub fn put_legal_hold_if<E>(
        &self,
        legal_hold: StoredLegalHoldStatus,
        mut action: impl FnMut(&StoredObject) -> Result<VersionId, E>,
    ) -> Result<Result<VersionId, E>, ObjectMetadataMutationFailure> {
        self.admission
            .cluster
            .put_object_metadata_if_with_route_validation(
                self.effect_route(),
                || self.admission.require_valid_now_raw(),
                |stored| {
                    let version_id = action(stored)?;
                    Ok((
                        version_id,
                        version_id,
                        PutObjectMetadataMutation::PutLegalHold(legal_hold),
                    ))
                },
            )
            .map_err(ObjectMetadataMutationFailure::from_object_pg_action)
    }

    pub fn put_acl_if<E>(
        &self,
        mut action: impl FnMut(&StoredObject) -> Result<(VersionId, AclGrants, bool), E>,
    ) -> Result<Result<VersionId, E>, ObjectMetadataMutationFailure> {
        self.admission
            .cluster
            .put_object_metadata_if_with_route_validation(
                self.effect_route(),
                || self.admission.require_valid_now_raw(),
                |stored| {
                    let (version_id, acl_grants, public_read) = action(stored)?;
                    Ok((
                        version_id,
                        version_id,
                        PutObjectMetadataMutation::PutAcl {
                            acl_grants,
                            public_read,
                        },
                    ))
                },
            )
            .map_err(ObjectMetadataMutationFailure::from_object_pg_action)
    }

    pub fn delete_current_object_if<T, E>(
        &self,
        action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteCurrentObjectOutcome<T>, E>, ObjectMetadataMutationFailure> {
        self.admission
            .cluster
            .delete_current_object_if_with_route_validation(
                self.effect_route(),
                || self.admission.require_valid_now_raw(),
                action,
            )
            .map_err(ObjectMetadataMutationFailure::from_object_pg_action)
    }

    pub fn delete_specific_object_version_if<T, E>(
        &self,
        action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<DeleteSpecificObjectVersionOutcome<T>, E>, ObjectMetadataMutationFailure>
    {
        self.admission
            .cluster
            .delete_specific_object_version_if_with_route_validation(
                self.effect_route(),
                || self.admission.require_valid_now_raw(),
                action,
            )
            .map_err(ObjectMetadataMutationFailure::from_object_pg_action)
    }

    pub fn insert_current_delete_marker_if<T, E>(
        &self,
        versioning: BucketVersioningState,
        owner: OwnerIdentity,
        action: impl FnMut(Option<&StoredObject>) -> Result<T, E>,
    ) -> Result<Result<InsertCurrentDeleteMarkerOutcome<T>, E>, ObjectMetadataMutationFailure> {
        self.admission
            .cluster
            .insert_current_delete_marker_if_with_route_validation(
                self.effect_route(),
                || self.admission.require_valid_now_raw(),
                versioning,
                owner,
                action,
            )
            .map_err(ObjectMetadataMutationFailure::from_object_pg_action)
    }

    pub fn enqueue_object_payload_reclaim(&self, generation_id: GenerationId) {
        self.admission.cluster.enqueue_object_payload_reclaim(
            &self.bucket,
            &self.key,
            generation_id,
        );
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_probe_object_pg_available(&self) -> Result<bool, ObjectMetadataMutationFailure> {
        self.admission.require_valid_now_raw().map_err(|error| {
            ObjectMetadataMutationFailure::from_object_pg_action(ObjectPgActionError::Store(error))
        })?;
        self.admission
            .cluster
            .try_probe_object_pg_available(&self.bucket, &self.key)
            .map_err(ObjectMetadataMutationFailure::from_object_pg_action)
    }
}

impl DirectPutPayloadWrite<'_> {
    fn issued_by(&self, admission: &StorageClusterRouteAdmission) -> bool {
        std::ptr::eq(self.owner, admission)
    }

    fn disarm(&self) {
        self.armed.set(false);
    }

    fn cleanup_on_owner(&self) {
        if !self.armed.replace(false) {
            return;
        }
        self.owner
            .cluster
            .delete_direct_put_segment_payload_shards_at_epoch(
                self.placement_cluster_epoch,
                self.written.data_pg_id,
                self.written.ec,
                &self.segment_okh,
                self.segment_vid,
                &self.written.written_shards,
            );
        let _ = self.owner.cluster.release_object_generation_reservation(
            &self.bucket,
            &self.key,
            &self.generation_reservation_id,
        );
    }
}

impl Drop for DirectPutPayloadWrite<'_> {
    fn drop(&mut self) {
        self.cleanup_on_owner();
    }
}

enum AdmittedStreamAppendError {
    Storage(ObjectPgActionError),
    Maintenance(crate::StreamUploadFailure),
}

impl From<ObjectPgActionError> for AdmittedStreamAppendError {
    fn from(error: ObjectPgActionError) -> Self {
        Self::Storage(error)
    }
}

impl From<StoreError> for AdmittedStreamAppendError {
    fn from(error: StoreError) -> Self {
        Self::Storage(ObjectPgActionError::Store(error))
    }
}

impl ActivePutObjectRoute<'_> {
    fn effect_route(&self) -> PutObjectMutationEffectRoute<'_> {
        PutObjectMutationEffectRoute {
            bucket_pg_id: self.bucket_pg_id,
            object_pg_id: self.object_pg_id,
            bucket: &self.bucket,
            key: &self.key,
            effect_fence: self.admission.effect_fence(),
        }
    }

    pub fn load_existing_live_object(
        &self,
    ) -> Result<Option<StoredObject>, crate::DirectPutFailure> {
        let route = ObjectReadMetadataRoute {
            bucket: &self.bucket,
            key: &self.key,
            version_id: None,
            snapshot_mode: ObjectReadSnapshotMode::MetadataOnly,
            pg_id: self.object_pg_id,
        };
        match self.admission.cluster.load_object_if_on_route(
            &route,
            |stored| Ok::<_, std::convert::Infallible>(stored.clone()),
            || self.admission.require_valid_now_raw(),
        ) {
            Ok(Ok(stored @ StoredObject::Live(_))) => Ok(Some(stored)),
            Ok(Ok(StoredObject::DeleteMarker(_)))
            | Err(ObjectPgActionError::Metadata(MetadataError::ObjectNotFound)) => Ok(None),
            Ok(Err(never)) => match never {},
            Err(error) => Err(crate::DirectPutFailure::from_object_pg_action(error)),
        }
    }

    pub fn with_bucket_write_snapshot<T, E>(
        &self,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<T, E>,
    ) -> Result<Result<T, E>, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .with_bucket_write_snapshot_with_route_validation(
                BucketMetadataMutationEffectRoute {
                    pg_id: self.bucket_pg_id,
                    bucket: &self.bucket,
                    effect_fence: self.admission.effect_fence(),
                },
                || self.admission.require_valid_now_raw(),
                request,
                action,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn with_bucket_write_snapshot_for_command<T, E>(
        &self,
        request: BucketSnapshotRequest,
        action: impl FnOnce(
            BucketSnapshot,
            BucketWriteReservationProof,
        ) -> BucketWriteSnapshotAction<T, E>,
    ) -> Result<Result<T, E>, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .with_put_object_bucket_write_snapshot_for_command_with_route_validation(
                self.effect_route(),
                || self.admission.require_valid_now_raw(),
                request,
                action,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn reserve_generation(
        &self,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, crate::DirectPutFailure> {
        self.admission
            .cluster
            .reserve_put_object_generation_with_route_validation(
                self.effect_route(),
                reservation_id,
                || self.admission.require_valid_now_raw(),
            )
            .map_err(crate::DirectPutFailure::from_object_pg_action)
    }

    pub fn create_stream_session<T, E>(
        &self,
        request: BucketSnapshotRequest,
        cleanup_after: Option<u64>,
        action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, CreateStreamUploadReq), E>,
    ) -> Result<Result<T, E>, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .create_put_object_stream_session_with_route_validation(
                self.effect_route(),
                || self.admission.require_valid_now_raw(),
                request,
                cleanup_after,
                action,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn create_stream_session_record(
        &self,
        session_id: &SessionId,
        encryption: ObjectEncryption,
        cleanup_after: Option<u64>,
    ) -> Result<(), crate::StreamUploadFailure> {
        self.admission
            .cluster
            .create_put_object_stream_session_record_with_route_validation(
                self.effect_route(),
                session_id,
                encryption,
                cleanup_after,
                || self.admission.require_valid_now_raw(),
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    pub fn load_stream_session(
        &self,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, crate::StreamUploadFailure> {
        self.admission
            .require_valid_now_raw()
            .map_err(ObjectPgActionError::from)
            .map_err(crate::StreamUploadFailure::from_object_pg_action)?;
        self.admission
            .cluster
            .load_stream_upload_session_on_route(self.effect_route(), session_id)
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    pub fn abort_stream_session(
        &self,
        session_id: &SessionId,
    ) -> Result<(), crate::StreamUploadFailure> {
        self.admission
            .cluster
            .abort_stream_upload_session_with_route_validation(
                self.effect_route(),
                session_id,
                || self.admission.require_valid_now_raw(),
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    pub fn append_stream_segment(
        &self,
        input: StreamSegmentAppendInput<'_>,
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        self.admission
            .cluster
            .append_stream_segment_with_route_validation(
                self.effect_route(),
                input,
                || self.admission.require_valid_now_raw(),
                || {},
                || Ok(()),
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    /// Append one segment while maintaining a caller-owned reservation between
    /// preparation, erasure-coded shard writes, and metadata commit.
    pub fn append_stream_segment_with_lease_maintenance(
        &self,
        input: StreamSegmentAppendInput<'_>,
        mut maintain_lease: impl FnMut() -> Result<(), crate::StreamUploadFailure>,
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        let result = self
            .admission
            .cluster
            .append_stream_segment_with_route_validation(
                self.effect_route(),
                input,
                || self.admission.require_valid_now_raw(),
                || {},
                || maintain_lease().map_err(AdmittedStreamAppendError::Maintenance),
            );
        match result {
            Ok(outcome) => Ok(outcome),
            Err(AdmittedStreamAppendError::Storage(error)) => {
                Err(crate::StreamUploadFailure::from_object_pg_action(error))
            }
            Err(AdmittedStreamAppendError::Maintenance(error)) => Err(error),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub(crate) fn test_append_stream_segment_with_after_prepare(
        &self,
        input: StreamSegmentAppendInput<'_>,
        after_prepare: impl FnMut(),
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        self.admission
            .cluster
            .append_stream_segment_with_route_validation(
                self.effect_route(),
                input,
                || self.admission.require_valid_now_raw(),
                after_prepare,
                || Ok(()),
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub(crate) fn test_append_stream_segment_with_after_prepare_and_lease_maintenance(
        &self,
        input: StreamSegmentAppendInput<'_>,
        after_prepare: impl FnMut(),
        mut maintain_lease: impl FnMut() -> Result<(), crate::StreamUploadFailure>,
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        let result = self
            .admission
            .cluster
            .append_stream_segment_with_route_validation(
                self.effect_route(),
                input,
                || self.admission.require_valid_now_raw(),
                after_prepare,
                || maintain_lease().map_err(AdmittedStreamAppendError::Maintenance),
            );
        match result {
            Ok(outcome) => Ok(outcome),
            Err(AdmittedStreamAppendError::Storage(error)) => {
                Err(crate::StreamUploadFailure::from_object_pg_action(error))
            }
            Err(AdmittedStreamAppendError::Maintenance(error)) => Err(error),
        }
    }

    pub fn heartbeat_stream_session(
        &self,
        session_id: &SessionId,
    ) -> Result<(), crate::StreamUploadFailure> {
        self.admission
            .cluster
            .heartbeat_put_object_stream_session_with_route_validation(
                self.effect_route(),
                session_id,
                || self.admission.require_valid_now_raw(),
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    pub fn finalize_stream<T, E>(
        &self,
        session_id: &SessionId,
        total_size: u64,
        action: impl FnMut(StreamPutFinalizeSnapshot) -> Result<PreparedStreamPutCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPutOutcome<T>, E>, crate::StreamUploadFailure> {
        self.admission
            .cluster
            .finalize_put_object_stream_with_route_validation(
                self.effect_route(),
                session_id,
                total_size,
                || self.admission.require_valid_now_raw(),
                action,
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    pub fn write_direct_object_payload(
        &self,
        generation_reservation_id: &SessionId,
        generation_id: GenerationId,
        logical_size: u64,
        data: &[u8],
    ) -> Result<DirectPutPayloadWrite<'_>, crate::DirectPutFailure> {
        let segment_index = 0;
        let segment_okh =
            crate::direct_put_segment_key_hash(generation_reservation_id, segment_index);
        let written = self
            .admission
            .cluster
            .write_direct_put_segment_payload_shards_with_route_validation(
                self.effect_route(),
                generation_id,
                segment_index,
                &segment_okh,
                data,
                || self.admission.require_valid_now_raw(),
            )
            .map_err(crate::DirectPutFailure::from_store)?;
        Ok(DirectPutPayloadWrite {
            owner: self.admission,
            armed: std::cell::Cell::new(true),
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            generation_reservation_id: generation_reservation_id.clone(),
            generation_id,
            logical_size,
            segment_index,
            segment_crc64: checksum::crc64::checksum(data),
            segment_okh,
            segment_vid: generation_id,
            placement_cluster_epoch: self.admission.cluster.operation_epoch(),
            written,
        })
    }

    pub fn commit_direct_object<E>(
        &self,
        payload: DirectPutPayloadWrite<'_>,
        prepared: &PreparedDirectPutObjectCommit,
        action: impl FnMut(DirectPutCommitSnapshot) -> Result<(), E>,
    ) -> Result<Result<FinalizeDirectPutObjectOutcome, E>, crate::DirectPutFailure> {
        if !payload.issued_by(self.admission)
            || payload.bucket != self.bucket
            || payload.key != self.key
            || payload.placement_cluster_epoch != self.admission.cluster.operation_epoch()
        {
            self.admission
                .cluster
                .release_bucket_write_reservation_proof(&prepared.bucket_write_reservation)
                .map_err(bucket_snapshot_error_to_object_pg_action_error)
                .map_err(crate::DirectPutFailure::from_object_pg_action)?;
            return Err(crate::DirectPutFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "direct PUT payload does not match admitted object route".to_string(),
                },
            ));
        }
        if payload.placement_cluster_epoch != prepared.bucket_write_reservation.cluster_epoch {
            self.admission
                .cluster
                .release_bucket_write_reservation_proof(&prepared.bucket_write_reservation)
                .map_err(bucket_snapshot_error_to_object_pg_action_error)
                .map_err(crate::DirectPutFailure::from_object_pg_action)?;
            return Err(crate::DirectPutFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "direct PUT payload does not match bucket write reservation epoch"
                        .to_string(),
                },
            ));
        }
        let request = CommitDirectPutObjectReq {
            bucket: payload.bucket.clone(),
            key: payload.key.clone(),
            generation_reservation_id: payload.generation_reservation_id.clone(),
            versioning: prepared.versioning,
            owner: prepared.owner.clone(),
            acl_grants: prepared.acl_grants.clone(),
            public_read: prepared.public_read,
            generation_id: payload.generation_id,
            size: payload.logical_size,
            etag_crc64: prepared.etag_crc64,
            ec: payload.written.ec,
            tags: prepared.tags.clone(),
            metadata_blob: prepared.metadata_blob.clone(),
            system_metadata_blob: prepared.system_metadata_blob.clone(),
            object_lock: prepared.object_lock,
            encryption: prepared.encryption.clone(),
            segment_index: payload.segment_index,
            segment_crc64: payload.segment_crc64,
            segment_okh: payload.segment_okh,
            segment_vid: payload.segment_vid,
            data_pg_id: payload.written.data_pg_id,
            bucket_write_reservation: prepared.bucket_write_reservation.clone(),
        };
        self.admission
            .cluster
            .commit_direct_put_object_from_payload_shards_with_route_validation(
                self.effect_route(),
                &request,
                &payload.written.written_shards,
                || payload.disarm(),
                || self.admission.require_valid_now_raw(),
                action,
            )
            .map_err(crate::DirectPutFailure::from_object_pg_action)
    }

    pub fn release_generation_reservation(&self, reservation_id: &SessionId) {
        let _ = self
            .admission
            .cluster
            .release_object_generation_reservation(&self.bucket, &self.key, reservation_id);
    }

    pub fn discard_direct_object_payload(
        &self,
        payload: DirectPutPayloadWrite<'_>,
    ) -> Result<(), crate::DirectPutFailure> {
        let subject_matches = payload.issued_by(self.admission)
            && payload.bucket == self.bucket
            && payload.key == self.key;
        payload.cleanup_on_owner();
        if !subject_matches {
            return Err(crate::DirectPutFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "direct PUT payload does not match admitted object route".to_string(),
                },
            ));
        }
        Ok(())
    }

    pub fn enqueue_object_payload_reclaim(&self, generation_id: GenerationId) {
        self.admission.cluster.enqueue_object_payload_reclaim(
            &self.bucket,
            &self.key,
            generation_id,
        );
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_probe_object_pg_available(&self) -> Result<bool, crate::DirectPutFailure> {
        self.admission
            .require_valid_now_raw()
            .map_err(crate::DirectPutFailure::from_store)?;
        self.admission
            .cluster
            .try_probe_object_pg_available(&self.bucket, &self.key)
            .map_err(crate::DirectPutFailure::from_object_pg_action)
    }
}

impl ActiveMultipartObjectRoute<'_> {
    fn effect_route(&self) -> MultipartObjectMutationEffectRoute<'_> {
        MultipartObjectMutationEffectRoute {
            pg_id: self.pg_id,
            bucket: &self.bucket,
            key: &self.key,
            effect_fence: self.admission.effect_fence(),
        }
    }

    fn stream_effect_route(&self) -> PutObjectMutationEffectRoute<'_> {
        PutObjectMutationEffectRoute {
            bucket_pg_id: self.admission.cluster.bucket_metadata_pg(&self.bucket),
            object_pg_id: self.pg_id,
            bucket: &self.bucket,
            key: &self.key,
            effect_fence: self.admission.effect_fence(),
        }
    }

    pub fn create_multipart_upload_with_ordered_id<T, E>(
        &self,
        request: BucketSnapshotRequest,
        action: impl FnMut(
            BucketSnapshot,
            Option<StoredObject>,
        ) -> Result<(T, crate::CreateMultipartUploadInput), E>,
    ) -> Result<Result<crate::CreateMultipartUploadOutcome<T>, E>, crate::BucketSnapshotLoadFailure>
    {
        self.admission
            .cluster
            .create_multipart_upload_with_ordered_id_with_route_validation(
                self.effect_route(),
                || self.admission.require_valid_now_raw(),
                request,
                action,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    /// Resolve one multipart upload for request authorization on this exact
    /// Classify one multipart upload through the logical abort-authorization boundary.
    pub fn lookup_multipart_upload_for_abort(
        &self,
        upload_id: &UploadId,
    ) -> Result<crate::MultipartUploadAbortLookup, crate::MultipartManagementFailure> {
        self.admission
            .cluster
            .lookup_multipart_upload_management_with_route_validation(
                self.effect_route(),
                upload_id,
                || self.admission.require_valid_now_raw(),
            )
            .map(crate::MultipartUploadAbortLookup::from_management_lookup)
            .map_err(crate::MultipartManagementFailure::from_object_pg_action)
    }

    /// Classify one multipart upload through the logical ListParts authorization boundary.
    pub fn lookup_multipart_upload_for_list_parts(
        &self,
        upload_id: &UploadId,
    ) -> Result<crate::MultipartUploadListPartsLookup, crate::MultipartManagementFailure> {
        self.admission
            .cluster
            .lookup_multipart_upload_management_with_route_validation(
                self.effect_route(),
                upload_id,
                || self.admission.require_valid_now_raw(),
            )
            .map(crate::MultipartUploadListPartsLookup::from_management_lookup)
            .map_err(crate::MultipartManagementFailure::from_object_pg_action)
    }

    /// Classify one multipart upload through the logical completion-authorization boundary.
    pub fn lookup_multipart_upload_for_completion(
        &self,
        upload_id: &UploadId,
    ) -> Result<crate::MultipartUploadCompletionLookup, crate::MultipartManagementFailure> {
        self.admission
            .cluster
            .lookup_multipart_upload_management_with_route_validation(
                self.effect_route(),
                upload_id,
                || self.admission.require_valid_now_raw(),
            )
            .map(crate::MultipartUploadCompletionLookup::from_management_lookup)
            .map_err(crate::MultipartManagementFailure::from_object_pg_action)
    }

    /// Load an in-progress upload through the logical UploadPart authorization boundary.
    pub fn load_multipart_upload_for_part(
        &self,
        upload_id: &UploadId,
    ) -> Result<crate::MultipartUploadPartCandidate, crate::MultipartManagementFailure> {
        self.admission
            .cluster
            .load_in_progress_multipart_upload_with_route_validation(
                self.effect_route(),
                upload_id,
                || self.admission.require_valid_now_raw(),
            )
            .map(crate::MultipartUploadPartCandidate::from_record)
            .map_err(crate::MultipartManagementFailure::from_object_pg_action)
    }

    /// Require an in-progress upload on this exact admitted object route without
    /// exposing its durable record.
    pub fn require_in_progress_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<(), crate::MultipartManagementFailure> {
        self.admission
            .cluster
            .load_in_progress_multipart_upload_with_route_validation(
                self.effect_route(),
                upload_id,
                || self.admission.require_valid_now_raw(),
            )
            .map(drop)
            .map_err(crate::MultipartManagementFailure::from_object_pg_action)
    }

    /// Load an in-progress upload for storage-owned tests.
    #[cfg(test)]
    pub(crate) fn load_in_progress_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, ObjectPgActionError> {
        self.admission
            .cluster
            .load_in_progress_multipart_upload_with_route_validation(
                self.effect_route(),
                upload_id,
                || self.admission.require_valid_now_raw(),
            )
    }

    /// Create an UploadPart stream session for the exact upload authorized on
    /// this admitted multipart route.
    pub fn create_upload_part_stream_session(
        &self,
        authorized_upload: crate::AuthorizedMultipartUploadPart,
        session_id: &SessionId,
    ) -> Result<SessionId, crate::StreamUploadFailure> {
        let internal_authorized_upload =
            AuthorizedMultipartUploadRecord::assume_authorized(authorized_upload.record().clone());
        self.admission
            .cluster
            .create_upload_part_stream_session_with_route_validation(
                self.effect_route(),
                &internal_authorized_upload,
                authorized_upload.part_number(),
                session_id,
                self.admission.authority_valid_until_ms(),
                || self.admission.require_valid_now_raw(),
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    pub fn load_stream_session(
        &self,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, crate::StreamUploadFailure> {
        self.admission
            .require_valid_now_raw()
            .map_err(ObjectPgActionError::from)
            .map_err(crate::StreamUploadFailure::from_object_pg_action)?;
        self.admission
            .cluster
            .load_stream_upload_session_on_route(self.stream_effect_route(), session_id)
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    pub fn abort_stream_session(
        &self,
        session_id: &SessionId,
    ) -> Result<(), crate::StreamUploadFailure> {
        self.admission
            .cluster
            .abort_stream_upload_session_with_route_validation(
                self.stream_effect_route(),
                session_id,
                || self.admission.require_valid_now_raw(),
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    pub fn append_stream_segment(
        &self,
        input: StreamSegmentAppendInput<'_>,
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        self.admission
            .cluster
            .append_stream_segment_with_route_validation(
                self.stream_effect_route(),
                input,
                || self.admission.require_valid_now_raw(),
                || {},
                || Ok(()),
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    pub(crate) fn test_append_stream_segment_with_after_prepare(
        &self,
        input: StreamSegmentAppendInput<'_>,
        after_prepare: impl FnMut(),
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        self.admission
            .cluster
            .append_stream_segment_with_route_validation(
                self.stream_effect_route(),
                input,
                || self.admission.require_valid_now_raw(),
                after_prepare,
                || Ok(()),
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    pub fn finalize_stream_part<T, E>(
        &self,
        input: StreamPartFinalizeInput<'_>,
        action: impl FnMut(StreamPartFinalizeSnapshot) -> Result<PreparedStreamPartCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPartOutcome<T>, E>, crate::StreamUploadFailure> {
        self.admission
            .cluster
            .finalize_upload_part_stream_with_route_validation(
                self.effect_route(),
                input,
                || self.admission.require_valid_now_raw(),
                action,
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_probe_object_pg_available(&self) -> Result<bool, crate::MultipartManagementFailure> {
        self.admission
            .require_valid_now_raw()
            .map_err(crate::MultipartManagementFailure::from_store)?;
        self.admission
            .cluster
            .try_probe_object_pg_available(&self.bucket, &self.key)
            .map_err(crate::MultipartManagementFailure::from_object_pg_action)
    }

    #[cfg(feature = "test-hooks")]
    fn try_load_in_progress_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<Option<MultipartUploadRecord>, crate::MultipartManagementFailure> {
        self.admission
            .require_valid_now_raw()
            .map_err(crate::MultipartManagementFailure::from_store)?;
        self.admission
            .cluster
            .try_load_in_progress_multipart_upload(&self.bucket, &self.key, upload_id)
            .map_err(crate::MultipartManagementFailure::from_object_pg_action)
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_load_multipart_upload_for_completion(
        &self,
        upload_id: &UploadId,
    ) -> Result<Option<crate::MultipartUploadCompletionCandidate>, crate::MultipartManagementFailure>
    {
        self.try_load_in_progress_multipart_upload(upload_id)
            .map(|upload| upload.map(crate::MultipartUploadCompletionCandidate::from_record))
    }

    /// List parts for the exact upload authorized through this admitted
    /// object route.
    pub fn list_parts_for_authorized_upload(
        &self,
        authorized_upload: &crate::AuthorizedMultipartUploadListParts,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListedMultipartParts, crate::MultipartManagementFailure> {
        self.admission
            .cluster
            .list_multipart_parts_for_authorized_upload_with_route_validation(
                self.effect_route(),
                authorized_upload,
                part_number_marker,
                max_parts,
                || self.admission.require_valid_now_raw(),
            )
            .map_err(crate::MultipartManagementFailure::from_object_pg_action)
    }

    /// Load the completion snapshot for the exact upload authorized through
    /// this admitted object route.
    pub fn load_multipart_completion_snapshot(
        &self,
        authorized_upload: crate::AuthorizedMultipartUploadCompletion,
        requested_part_numbers: &[u32],
    ) -> Result<crate::AuthorizedMultipartCompletionSnapshot, crate::MultipartCompletionFailure>
    {
        let upload = authorized_upload.into_record();
        let internal_authorized_upload =
            AuthorizedMultipartUploadRecord::assume_authorized(upload.clone());
        let snapshot = self
            .admission
            .cluster
            .load_multipart_completion_snapshot_with_route_validation(
                self.effect_route(),
                &internal_authorized_upload,
                requested_part_numbers,
                || self.admission.require_valid_now_raw(),
            )
            .map_err(crate::MultipartCompletionFailure::from_object_pg_action)?;
        Ok(crate::AuthorizedMultipartCompletionSnapshot::new(
            snapshot, upload,
        ))
    }

    /// Publish completion for this exact admitted multipart object route.
    pub fn complete_multipart_upload_commit_serialized(
        &self,
        request: CompleteMultipartCommitRequest,
    ) -> Result<CompleteMultipartCommitOutcome, crate::MultipartCompletionFailure> {
        self.admission
            .cluster
            .complete_multipart_upload_commit_serialized_with_route_validation(
                self.effect_route(),
                request,
                || self.admission.require_valid_now_raw(),
            )
            .map_err(crate::MultipartCompletionFailure::from_object_pg_action)
    }

    /// Schedule reclaim for a generation displaced by a completion on this
    /// exact admitted object route.
    pub fn enqueue_object_payload_reclaim(&self, generation_id: GenerationId) {
        self.admission.cluster.enqueue_object_payload_reclaim(
            &self.bucket,
            &self.key,
            generation_id,
        );
    }

    /// Abort the exact upload authorized through this admitted object route.
    pub fn abort_authorized_multipart_upload(
        &self,
        authorized_upload: &crate::AuthorizedMultipartUploadAbort,
    ) -> Result<bool, crate::MultipartManagementFailure> {
        self.admission
            .cluster
            .abort_authorized_multipart_upload_locked(
                self.effect_route(),
                authorized_upload,
                || self.admission.require_valid_now_raw(),
            )
            .map_err(crate::MultipartManagementFailure::from_object_pg_action)
    }
}

struct ObjectMetadataScanRoute<'a> {
    bucket: &'a BucketName,
    require_valid_route: &'a dyn Fn() -> Result<(), StoreError>,
}

impl ObjectMetadataScanRoute<'_> {
    fn require_valid(&self) -> Result<(), StoreError> {
        (self.require_valid_route)()
    }
}

impl ActiveObjectMetadataScan<'_> {
    pub fn list_objects(
        &self,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        continuation_token: Option<&ObjectKey>,
        max_keys: u32,
    ) -> Result<ListedBucketObjects, crate::ObjectMetadataListingFailure> {
        let require_valid_route = || self.admission.require_valid_now_raw();
        let scan = ObjectMetadataScanRoute {
            bucket: &self.bucket,
            require_valid_route: &require_valid_route,
        };
        self.admission
            .cluster
            .list_objects_for_bucket_with_route_validation(
                &scan,
                prefix,
                delimiter,
                continuation_token,
                max_keys,
            )
            .map_err(crate::ObjectMetadataListingFailure::from_object_pg_action)
    }

    pub fn list_object_versions(
        &self,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        key_marker: Option<&ObjectKey>,
        version_id_marker: Option<VersionId>,
        max_keys: u32,
    ) -> Result<ListedBucketObjectVersions, crate::ObjectMetadataListingFailure> {
        let require_valid_route = || self.admission.require_valid_now_raw();
        let scan = ObjectMetadataScanRoute {
            bucket: &self.bucket,
            require_valid_route: &require_valid_route,
        };
        self.admission
            .cluster
            .list_object_versions_for_bucket_with_route_validation(
                &scan,
                prefix,
                delimiter,
                key_marker,
                version_id_marker,
                max_keys,
            )
            .map_err(crate::ObjectMetadataListingFailure::from_object_pg_action)
    }

    pub fn list_multipart_uploads(
        &self,
        prefix: Option<&ObjectKey>,
        delimiter: Option<&str>,
        key_marker: Option<&ObjectKey>,
        upload_id_marker: Option<&crate::UploadId>,
        max_uploads: u32,
    ) -> Result<ListedBucketMultipartUploads, crate::ObjectMetadataListingFailure> {
        let require_valid_route = || self.admission.require_valid_now_raw();
        let scan = ObjectMetadataScanRoute {
            bucket: &self.bucket,
            require_valid_route: &require_valid_route,
        };
        self.admission
            .cluster
            .list_multipart_uploads_for_bucket_with_route_validation(
                &scan,
                prefix,
                delimiter,
                key_marker,
                upload_id_marker,
                max_uploads,
            )
            .map_err(crate::ObjectMetadataListingFailure::from_object_pg_action)
    }
}

impl ActiveBucketRoute<'_> {
    pub fn bucket(&self) -> &BucketName {
        &self.bucket
    }

    fn mutation_effect_route(&self) -> BucketMetadataMutationEffectRoute<'_> {
        BucketMetadataMutationEffectRoute {
            pg_id: self.pg_id,
            bucket: &self.bucket,
            effect_fence: self.admission.effect_fence(),
        }
    }

    fn with_metadata_route<T>(
        &self,
        action: impl FnOnce(&dyn BucketMetadataRoute) -> Result<T, BucketSnapshotLoadError>,
    ) -> Result<T, BucketSnapshotLoadError> {
        self.admission.require_valid_now_raw()?;
        let cluster = &self.admission.cluster;
        let node = cluster
            .local_map
            .metadata_pg_read_node(cluster.operation_epoch(), self.pg_id.pg_id())?;
        let route = node
            .bucket_metadata_client()
            .open_bucket_metadata_read_route(
                cluster.operation_epoch(),
                self.pg_id,
                &self.bucket,
                node.authorization(),
            )?;
        action(route.as_ref())
    }

    pub fn create_bucket_with_config_and_load_info(
        &self,
        config: &crate::CreateBucketConfig<'_>,
    ) -> Result<crate::BucketCreateAttemptOutcome, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .create_bucket_with_config_and_load_info_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                config,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn try_finalize_bucket_delete(
        &self,
    ) -> Result<crate::BucketDeleteFinalizeOutcome, crate::BucketWriteDrainFailure> {
        self.admission
            .require_valid_now_raw()
            .map_err(crate::BucketWriteDrainError::from)
            .map_err(crate::BucketWriteDrainFailure::from)?;
        self.admission
            .cluster
            .try_finalize_bucket_delete(&self.bucket)
    }

    pub fn begin_bucket_delete_if_current(
        &self,
        bucket_identity: crate::cluster::BucketIdentityGenerations,
    ) -> Result<(), crate::BucketWriteDrainFailure> {
        self.admission
            .cluster
            .begin_bucket_delete_if_current_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                bucket_identity,
            )
            .map_err(crate::BucketWriteDrainFailure::from)
    }

    pub fn enqueue_bucket_delete_finalize(&self, bucket_incarnation_generation: u64) {
        self.admission
            .cluster
            .enqueue_bucket_delete_finalize(crate::BucketDeleteFinalizeRoot {
                bucket: self.bucket.clone(),
                bucket_incarnation_generation,
            });
    }

    pub fn head_bucket_info(&self) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.with_metadata_route(|route| route.head_bucket_info())
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn get_bucket_subresource(
        &self,
        kind: crate::OpaqueBucketSubresourceKind,
    ) -> Result<Option<String>, crate::BucketSnapshotLoadFailure> {
        self.with_metadata_route(|route| route.get_bucket_subresource(kind.stored_kind()))
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn get_bucket_tags(
        &self,
    ) -> Result<Option<SerializedBucketTagSet>, crate::BucketSnapshotLoadFailure> {
        self.with_metadata_route(|route| route.get_bucket_tags())
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn load_bucket_snapshot(
        &self,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, crate::BucketSnapshotLoadFailure> {
        self.with_metadata_route(|route| route.load_bucket_snapshot(request))
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn load_bucket_delete_authorization_snapshot(
        &self,
        request: BucketSnapshotRequest,
    ) -> Result<BucketSnapshot, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .load_bucket_delete_authorization_snapshot_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                request,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn load_active_bucket_delete_attempt_authorization_snapshot(
        &self,
        request: BucketSnapshotRequest,
    ) -> Result<Option<BucketSnapshot>, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .load_active_bucket_delete_attempt_authorization_snapshot_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                request,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn with_bucket_write_snapshot<T, E>(
        &self,
        request: BucketSnapshotRequest,
        action: impl FnOnce(BucketSnapshot) -> Result<T, E>,
    ) -> Result<Result<T, E>, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .with_bucket_write_snapshot_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                request,
                action,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn put_bucket_versioning_and_load_info(
        &self,
        state: BucketVersioningState,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .put_bucket_versioning_and_load_info_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                state,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    fn put_bucket_property_and_load_info(
        &self,
        mutation: BucketPropertyMutation,
    ) -> Result<BucketInfo, BucketSnapshotLoadError> {
        self.admission
            .cluster
            .put_bucket_property_command_and_load_info_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                mutation,
            )
    }

    pub fn put_bucket_object_lock_and_load_info(
        &self,
        config: BucketObjectLockConfig,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property_and_load_info(BucketPropertyMutation::ObjectLock(config))
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn put_bucket_encryption_and_load_info(
        &self,
        config: BucketEncryptionConfig,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property_and_load_info(BucketPropertyMutation::Encryption(config))
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn put_bucket_public_access_block_and_load_info(
        &self,
        config: PublicAccessBlockConfig,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property_and_load_info(BucketPropertyMutation::PublicAccessBlock(Some(
            config,
        )))
        .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn delete_bucket_public_access_block_and_load_info(
        &self,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property_and_load_info(BucketPropertyMutation::PublicAccessBlock(None))
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn put_bucket_ownership_controls_and_load_info(
        &self,
        config: BucketOwnershipControls,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property_and_load_info(BucketPropertyMutation::OwnershipControls(Some(
            config,
        )))
        .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn delete_bucket_ownership_controls_and_load_info(
        &self,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property_and_load_info(BucketPropertyMutation::OwnershipControls(None))
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn put_bucket_abac_enabled_and_load_info(
        &self,
        enabled: bool,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property_and_load_info(BucketPropertyMutation::AbacEnabled(enabled))
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn put_bucket_acl_and_load_info(
        &self,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .put_bucket_acl_and_load_info_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                acl_grants,
                summary,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn put_bucket_subresource_and_load_info(
        &self,
        req: PutBucketSubresource<'_>,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .put_bucket_subresource_and_load_info_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                req,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn delete_bucket_subresource_and_load_info(
        &self,
        kind: crate::OpaqueBucketSubresourceKind,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .delete_bucket_subresource_and_load_info_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                kind.stored_kind(),
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn delete_bucket_tags_and_load_info(
        &self,
    ) -> Result<BucketInfo, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .delete_bucket_subresource_and_load_info_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                BucketSubresourceKind::Tagging,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    #[cfg(feature = "test-hooks")]
    pub fn try_probe_bucket_pg_available(&self) -> Result<bool, crate::BucketSnapshotLoadFailure> {
        let result = (|| {
            self.admission.require_valid_now_raw()?;
            self.admission
                .cluster
                .try_probe_bucket_pg_available(&self.bucket)
        })();
        result.map_err(crate::BucketSnapshotLoadFailure::from)
    }
}
