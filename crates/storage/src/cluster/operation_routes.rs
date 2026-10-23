// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

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
/// admission's generation-gated effective deadline before reaching a node
/// client.
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
/// effective deadline before every bucket-PG node access. Same-generation
/// renewal is visible only while no replacement publication is pending.
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
    bucket_incarnation_generation: Option<u64>,
    pg_id: ObjectMetadataPgId,
}

/// Non-cloneable active authority for one object's metadata mutations.
///
/// The bucket, key, requested version, and routed object-metadata PG are fixed
/// at construction. Mutation entry and every retry recheck the request's
/// effective admitted deadline. Once a metadata command is durably installed,
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
/// and generation-gated request deadline.
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

/// Opaque ownership of one reserved direct-PutObject generation.
///
/// The reservation identity is minted internally by an admitted
/// [`ActivePutObjectRoute`] before payload encryption begins; production
/// callers cannot reacquire the same durable reservation. The token is
/// deliberately non-cloneable and binds the route admission, object subject,
/// exact direct-PUT bucket-write authority, bucket incarnation, reservation
/// identity, and generation. Staging consumes the token and
/// transfers ownership to
/// [`DirectPutPayloadWrite`]; dropping it first releases the durable
/// reservation.
///
/// ```compile_fail
/// use storage::DirectPutGenerationReservation;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn duplicate(reservation: &DirectPutGenerationReservation<'_>) {
///     require_clone(reservation);
/// }
/// ```
pub struct DirectPutGenerationReservation<'admission> {
    owner: &'admission StorageClusterRouteAdmission,
    armed: std::cell::Cell<bool>,
    bucket: BucketName,
    key: ObjectKey,
    reservation_id: SessionId,
    generation_id: GenerationId,
    bucket_incarnation_generation: u64,
}

impl<'admission> DirectPutGenerationReservation<'admission> {
    /// Returns the immutable payload generation reserved for this write.
    #[must_use]
    pub fn generation_id(&self) -> GenerationId {
        self.generation_id
    }

    /// Binds this exact reserved object generation to one encryption instance
    /// before the first payload byte is encrypted.
    #[must_use]
    pub fn bind_encryption(
        self,
        encryption: &ObjectEncryption,
    ) -> DirectPutEncryptionReservation<'admission> {
        let subject = ObjectEncryptionSubject::standard_object(
            self.bucket_incarnation_generation,
            &self.bucket,
            &self.key,
            self.generation_id,
            encryption,
        );
        DirectPutEncryptionReservation {
            generation: self,
            subject,
        }
    }
}

impl std::fmt::Debug for DirectPutGenerationReservation<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectPutGenerationReservation")
            .finish_non_exhaustive()
    }
}

/// Non-cloneable direct-PUT generation reservation bound to one exact
/// canonical encryption subject.
///
/// ```compile_fail
/// use storage::DirectPutEncryptionReservation;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn duplicate(reservation: &DirectPutEncryptionReservation<'_>) {
///     require_clone(reservation);
/// }
/// ```
pub struct DirectPutEncryptionReservation<'admission> {
    generation: DirectPutGenerationReservation<'admission>,
    subject: ObjectEncryptionSubject,
}

impl<'admission> DirectPutEncryptionReservation<'admission> {
    /// Returns the exact storage-minted object subject used to bind DEK
    /// wrapping and encrypted checksum metadata before this reservation is
    /// consumed by the segment writer.
    #[must_use]
    pub fn object_encryption_subject(&self) -> &ObjectEncryptionSubject {
        &self.subject
    }

    /// Extends this exact object subject with the sole standard-object segment
    /// and transfers the generation reservation into that segment capability.
    #[must_use]
    pub fn bind_standard_segment(
        self,
        logical_plaintext_len: u64,
    ) -> DirectPutSegmentEncryptionReservation<'admission> {
        let segment_subject =
            SegmentEncryptionSubject::standard_object(self.subject, 0, logical_plaintext_len);
        DirectPutSegmentEncryptionReservation {
            generation: self.generation,
            subject: segment_subject,
        }
    }
}

impl std::fmt::Debug for DirectPutEncryptionReservation<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectPutEncryptionReservation")
            .finish_non_exhaustive()
    }
}

/// Non-cloneable direct-PUT generation reservation bound to the exact
/// standard-object segment encryption subject.
///
/// ```compile_fail
/// use storage::DirectPutSegmentEncryptionReservation;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn duplicate(reservation: &DirectPutSegmentEncryptionReservation<'_>) {
///     require_clone(reservation);
/// }
/// ```
pub struct DirectPutSegmentEncryptionReservation<'admission> {
    generation: DirectPutGenerationReservation<'admission>,
    subject: SegmentEncryptionSubject,
}

impl DirectPutSegmentEncryptionReservation<'_> {
    /// Returns the opaque segment subject which the encryption boundary must
    /// consume.
    #[must_use]
    pub fn encryption_subject(&self) -> &SegmentEncryptionSubject {
        &self.subject
    }
}

impl std::fmt::Debug for DirectPutSegmentEncryptionReservation<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DirectPutSegmentEncryptionReservation")
            .finish_non_exhaustive()
    }
}

/// Non-cloneable authority to finalize one exact PutObject stream session.
///
/// The subject is minted only after loading the session through an admitted
/// [`ActivePutObjectRoute`]. Its fields remain private so callers cannot pair
/// one session's encryption or reservation authority with another session.
///
/// ```compile_fail
/// use storage::AuthorizedStreamPutFinalization;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn cache_authority(authority: &AuthorizedStreamPutFinalization<'_, '_>) {
///     require_clone(authority);
/// }
/// ```
pub struct AuthorizedStreamPutFinalization<'route, 'admission> {
    route: &'route ActivePutObjectRoute<'admission>,
    session: StreamUploadRecord,
}

/// Non-cloneable authority to encrypt and append one exact PutObject stream
/// segment.
///
/// Storage derives this capability from the durable stream session and its
/// existing object-generation reservation. Callers can inspect the bound
/// encryption state and opaque segment subject, but cannot assemble or alter
/// the bucket incarnation, object generation, segment index, or logical size.
///
/// ```compile_fail
/// use storage::AuthorizedStreamPutSegment;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn duplicate(segment: &AuthorizedStreamPutSegment<'_, '_>) {
///     require_clone(segment);
/// }
/// ```
pub struct AuthorizedStreamPutSegment<'route, 'admission> {
    route: &'route ActivePutObjectRoute<'admission>,
    session: StreamUploadRecord,
    generation_id: GenerationId,
    segment_index: u32,
    subject: SegmentEncryptionSubject,
}

/// Non-cloneable authority to encrypt and append one exact UploadPart stream
/// segment.
///
/// Storage derives the subject from the in-progress upload, exact stream
/// session, immutable part payload generation, and segment geometry. The
/// caller can inspect only the bound encryption state and opaque subject.
///
/// ```compile_fail
/// use storage::AuthorizedStreamPartSegment;
///
/// fn require_clone<T: Clone>(_: &T) {}
/// fn duplicate(segment: &AuthorizedStreamPartSegment<'_, '_>) {
///     require_clone(segment);
/// }
/// ```
pub struct AuthorizedStreamPartSegment<'route, 'admission> {
    route: &'route ActiveMultipartObjectRoute<'admission>,
    session: StreamUploadRecord,
    upload: MultipartUploadRecord,
    part_generation_id: GenerationId,
    segment_index: u32,
    subject: SegmentEncryptionSubject,
}

impl AuthorizedStreamPartSegment<'_, '_> {
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session.session_id
    }

    #[must_use]
    pub fn segment_index(&self) -> u32 {
        self.segment_index
    }

    #[must_use]
    pub fn encryption(&self) -> &ObjectEncryption {
        &self.session.encryption
    }

    #[must_use]
    pub fn encryption_subject(&self) -> &SegmentEncryptionSubject {
        &self.subject
    }

    #[must_use]
    pub fn matches_encryption_and_geometry(
        &self,
        encryption: &ObjectEncryption,
        segment_index: u32,
        logical_plaintext_len: u64,
    ) -> bool {
        let StreamUploadTarget::UploadPart {
            upload_id,
            part_number,
        } = &self.session.target
        else {
            return false;
        };
        self.subject.matches_multipart_part_segment(
            encryption,
            upload_id,
            *part_number,
            self.part_generation_id,
            segment_index,
            logical_plaintext_len,
        )
    }

    fn matches_current_session(&self, current: &StreamUploadRecord) -> bool {
        self.session.has_same_append_authority(current)
    }
}

impl AuthorizedStreamPutSegment<'_, '_> {
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session.session_id
    }

    #[must_use]
    pub fn segment_index(&self) -> u32 {
        self.segment_index
    }

    #[must_use]
    pub fn encryption(&self) -> &ObjectEncryption {
        &self.session.encryption
    }

    #[must_use]
    pub fn encryption_subject(&self) -> &SegmentEncryptionSubject {
        &self.subject
    }

    fn matches_current_session(&self, current: &StreamUploadRecord) -> bool {
        self.session.has_same_append_authority(current)
    }
}

impl AuthorizedStreamPutFinalization<'_, '_> {
    #[must_use]
    pub fn encryption(&self) -> &ObjectEncryption {
        &self.session.encryption
    }

    fn matches_current_session(&self, current: &StreamUploadRecord) -> bool {
        self.session.has_same_stable_identity(current)
    }
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
    bucket_incarnation_generation: Option<u64>,
    pg_id: ObjectMetadataPgId,
}

impl ObjectReadMetadataRoute<'_> {
    fn bind_payload_encryption_subjects(
        &self,
        snapshot: &mut crate::ObjectReadSnapshot,
    ) -> Result<(), ObjectPgActionError> {
        let Some(bucket_incarnation_generation) = self.bucket_incarnation_generation else {
            return Ok(());
        };
        snapshot
            .bind_payload_encryption_subjects(bucket_incarnation_generation)
            .map_err(|reason| {
                ObjectPgActionError::Store(StoreError::PayloadShardSetMismatch {
                    reason: reason.to_string(),
                })
            })
    }
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

struct StreamAppendCallbacks<Validate, BeforePrepare, RequireRoute, AfterPrepare, MaintainLease> {
    validate_session: Validate,
    before_prepare: BeforePrepare,
    require_valid_route: RequireRoute,
    after_prepare: AfterPrepare,
    maintain_lease: MaintainLease,
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
            bucket_incarnation_generation: self.bucket_incarnation_generation,
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
            bucket_incarnation_generation: self.bucket_incarnation_generation,
            pg_id: self.pg_id,
        };
        self.admission
            .cluster
            .load_object_read_snapshot_if_on_route(&route, action, || {
                self.admission.require_valid_now_raw()
            })
            .map_err(ObjectReadFailure::from_object_pg_action)
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

impl DirectPutGenerationReservation<'_> {
    fn issued_by(&self, admission: &StorageClusterRouteAdmission) -> bool {
        std::ptr::eq(self.owner, admission)
    }

    fn disarm(&self) {
        self.armed.set(false);
    }

    fn release_on_owner(&self) {
        if !self.armed.replace(false) {
            return;
        }
        let _ = self.owner.cluster.release_object_generation_reservation(
            &self.bucket,
            &self.key,
            &self.reservation_id,
        );
    }
}

impl Drop for DirectPutGenerationReservation<'_> {
    fn drop(&mut self) {
        self.release_on_owner();
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

impl<'admission> ActivePutObjectRoute<'admission> {
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
            bucket_incarnation_generation: None,
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
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<DirectPutGenerationReservation<'admission>, crate::DirectPutFailure> {
        let reservation_id = random_hex_identifier("").map_err(|_| {
                crate::DirectPutFailure::from_object_pg_action(ObjectPgActionError::Store(
                    StoreError::Io {
                        context: "generate direct PUT generation reservation ID",
                        source: std::io::Error::other(
                            "failed to generate random generation reservation ID",
                        ),
                    },
                ))
            })?;
        let reservation_id = SessionId::try_from(reservation_id)
            .expect("hex-encoded direct PUT reservation IDs must be valid");
        self.reserve_generation_with_id(&reservation_id, bucket_write_reservation)
    }

    fn reserve_generation_with_id(
        &self,
        reservation_id: &SessionId,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<DirectPutGenerationReservation<'admission>, crate::DirectPutFailure> {
        if !bucket_write_reservation.matches_exact_mutation_subject(
            self.admission.cluster.operation_epoch(),
            &self.bucket,
            PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
            Some(self.key.as_str()),
        ) {
            return Err(crate::DirectPutFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "direct PUT generation reservation does not match bucket write authority"
                        .to_string(),
                },
            ));
        }
        let remaining_validity = self
            .admission
            .remaining_validity_raw()
            .map_err(ObjectPgActionError::Store)
            .map_err(crate::DirectPutFailure::from_object_pg_action)?;
        let reservation_primary = self
            .admission
            .cluster
            .local_map
            .metadata_pg_primary_node(
                self.admission.cluster.operation_epoch(),
                self.bucket_pg_id.pg_id(),
            )
            .map_err(ObjectPgActionError::Store)
            .map_err(crate::DirectPutFailure::from_object_pg_action)?;
        let reservation_client = reservation_primary.bucket_write_reservation_client();
        let reservation_route = reservation_client
            .open_bucket_write_reservation_route(
                self.admission.cluster.operation_epoch(),
                self.bucket_pg_id,
                &self.bucket,
            )
            .map_err(bucket_snapshot_error_to_object_pg_action_error)
            .map_err(crate::DirectPutFailure::from_object_pg_action)?;
        let proof_validation = match remaining_validity {
            Some(remaining) => reservation_route.validate_bucket_write_reservation_proof_until(
                bucket_write_reservation,
                Instant::now() + remaining,
            ),
            None => reservation_route
                .validate_bucket_write_reservation_proof(bucket_write_reservation),
        };
        proof_validation
            .map_err(bucket_snapshot_error_to_object_pg_action_error)
            .map_err(crate::DirectPutFailure::from_object_pg_action)?;
        let generation_id = self
            .admission
            .cluster
            .reserve_put_object_generation_with_route_validation(
                self.effect_route(),
                reservation_id,
                || self.admission.require_valid_now_raw(),
            )
            .map_err(crate::DirectPutFailure::from_object_pg_action)?;
        Ok(DirectPutGenerationReservation {
            owner: self.admission,
            armed: std::cell::Cell::new(true),
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            reservation_id: reservation_id.clone(),
            generation_id,
            bucket_incarnation_generation: bucket_write_reservation
                .bucket_incarnation_generation,
        })
    }

    /// Reserves a generation with a deterministic identity for owner-local
    /// recovery and interleaving fixtures.
    #[cfg(test)]
    pub(super) fn test_reserve_generation_with_id(
        &self,
        reservation_id: &SessionId,
    ) -> Result<DirectPutGenerationReservation<'admission>, crate::DirectPutFailure> {
        let reservation = self
            .admission
            .cluster
            .acquire_durable_bucket_write_reservation(
                &self.bucket,
                PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                Some(self.key.as_str()),
            )
            .map_err(bucket_snapshot_error_to_object_pg_action_error)
            .map_err(crate::DirectPutFailure::from_object_pg_action)?;
        let bucket_write_reservation = BucketWriteReservationProof::from(&reservation.record);
        let result = self.reserve_generation_with_id(reservation_id, &bucket_write_reservation);
        let _ = self
            .admission
            .cluster
            .release_bucket_write_reservation_proof(&bucket_write_reservation);
        result
    }

    /// Exercises proof-subject rejection with a deterministic generation
    /// reservation identity.
    #[cfg(test)]
    pub(super) fn test_reserve_generation_with_id_and_proof(
        &self,
        reservation_id: &SessionId,
        bucket_write_reservation: &BucketWriteReservationProof,
    ) -> Result<DirectPutGenerationReservation<'admission>, crate::DirectPutFailure> {
        self.reserve_generation_with_id(reservation_id, bucket_write_reservation)
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

}

impl<'admission> ActiveMultipartObjectRoute<'admission> {
    fn load_stream_part_generation(
        &self,
        session_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        self.admission
            .require_valid_now_raw()
            .map_err(ObjectPgActionError::from)?;
        self.admission
            .cluster
            .object_generation_metadata_primary_client(&self.bucket, &self.key)?
            .open_object_generation_metadata_route(
                self.admission.cluster.operation_epoch(),
                self.pg_id,
                &self.bucket,
                &self.key,
            )?
            .object_generation_reservation(session_id)
    }

    /// Binds one stream segment to the exact multipart upload, UploadPart
    /// session, and non-recycling part payload generation before encryption.
    pub fn authorize_stream_part_segment<'route>(
        &'route self,
        upload_id: &UploadId,
        session_id: &SessionId,
        part_number: u32,
        segment_index: u32,
        logical_plaintext_len: u64,
    ) -> Result<AuthorizedStreamPartSegment<'route, 'admission>, crate::StreamUploadFailure> {
        let session = self.load_stream_session(session_id)?;
        let upload = self
            .load_in_progress_multipart_upload(upload_id)
            .map_err(crate::StreamUploadFailure::from_object_pg_action)?;
        if session.bucket != self.bucket
            || session.key != self.key
            || session.session_id != *session_id
            || session.target
                != (StreamUploadTarget::UploadPart {
                    upload_id: upload_id.clone(),
                    part_number,
                })
            || session.state != StreamUploadState::InProgress
            || upload.upload_id != *upload_id
            || upload.bucket != self.bucket
            || upload.key != self.key
            || upload.state != UploadState::InProgress
            || session.encryption != upload.encryption
        {
            return Err(crate::StreamUploadFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "stream segment subject does not match the admitted UploadPart route"
                        .to_string(),
                },
            ));
        }
        let part_generation_id = self
            .load_stream_part_generation(session_id)
            .map_err(crate::StreamUploadFailure::from_object_pg_action)?;
        self.admission
            .require_valid_now_raw()
            .map_err(ObjectPgActionError::from)
            .map_err(crate::StreamUploadFailure::from_object_pg_action)?;
        let subject = SegmentEncryptionSubject::multipart_part(
            ObjectEncryptionSubject::standard_object(
                session.bucket_incarnation_generation,
                &self.bucket,
                &self.key,
                upload.object_generation_id,
                &session.encryption,
            ),
            upload_id,
            part_number,
            part_generation_id,
            segment_index,
            logical_plaintext_len,
        );
        Ok(AuthorizedStreamPartSegment {
            route: self,
            session,
            upload,
            part_generation_id,
            segment_index,
            subject,
        })
    }

    fn validate_stream_part_segment_authorization(
        &self,
        authorization: &AuthorizedStreamPartSegment<'_, '_>,
        storage_bytes: &[u8],
    ) -> Result<(), crate::StreamUploadFailure> {
        let logical_size = authorization
            .session
            .encryption
            .segment_logical_size(storage_bytes.len() as u64)
            .ok_or_else(|| {
                crate::StreamUploadFailure::from_object_pg_action(
                    ObjectPgActionError::InvalidRequest {
                        reason: "encrypted stream segment shorter than authentication tag"
                            .to_string(),
                    },
                )
            })?;
        let StreamUploadTarget::UploadPart {
            upload_id,
            part_number,
        } = &authorization.session.target
        else {
            return Err(crate::StreamUploadFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "stream part segment authority has a non-UploadPart target".to_string(),
                },
            ));
        };
        if !std::ptr::eq(self, authorization.route)
            || authorization.session.bucket != self.bucket
            || authorization.session.key != self.key
            || authorization.upload.upload_id != *upload_id
            || authorization.upload.bucket != self.bucket
            || authorization.upload.key != self.key
            || !authorization.subject.matches_multipart_part_segment(
                &authorization.session.encryption,
                upload_id,
                *part_number,
                authorization.part_generation_id,
                authorization.segment_index,
                logical_size,
            )
        {
            return Err(crate::StreamUploadFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "stream segment authorization does not match the admitted UploadPart route or payload"
                        .to_string(),
                },
            ));
        }
        let current_generation = self
            .load_stream_part_generation(&authorization.session.session_id)
            .map_err(crate::StreamUploadFailure::from_object_pg_action)?;
        let current_upload = self
            .load_in_progress_multipart_upload(upload_id)
            .map_err(crate::StreamUploadFailure::from_object_pg_action)?;
        if current_generation != authorization.part_generation_id
            || current_upload != authorization.upload
        {
            return Err(crate::StreamUploadFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "stream segment UploadPart authority no longer matches durable state"
                        .to_string(),
                },
            ));
        }
        Ok(())
    }

    fn append_authorized_stream_part_segment_inner(
        &self,
        authorization: AuthorizedStreamPartSegment<'_, '_>,
        payload_crc64: u64,
        storage_bytes: &[u8],
        before_prepare: impl FnMut(),
        after_prepare: impl FnMut(),
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        self.validate_stream_part_segment_authorization(&authorization, storage_bytes)?;
        let input = StreamSegmentAppendInput {
            session_id: &authorization.session.session_id,
            segment_index: authorization.segment_index,
            payload_crc64,
            storage_bytes,
        };
        let expected_append_state = crate::ExpectedStreamAppendState::UploadPart {
            session: authorization.session.clone(),
            upload: Box::new(authorization.upload.clone()),
            part_generation_id: authorization.part_generation_id,
        };
        self.admission
            .cluster
            .append_stream_segment_with_route_validation(
                self.stream_effect_route(),
                input,
                Some(expected_append_state),
                StreamAppendCallbacks {
                    validate_session: |current: &StreamUploadRecord| {
                        if authorization.matches_current_session(current) {
                            Ok(())
                        } else {
                            Err(ObjectPgActionError::InvalidRequest {
                                reason: "stream part segment authorization no longer matches the durable session"
                                    .to_string(),
                            })
                        }
                    },
                    before_prepare,
                    require_valid_route: || self.admission.require_valid_now_raw(),
                    after_prepare,
                    maintain_lease: || Ok(()),
                },
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    pub fn append_authorized_stream_part_segment(
        &self,
        authorization: AuthorizedStreamPartSegment<'_, '_>,
        payload_crc64: u64,
        storage_bytes: &[u8],
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        self.append_authorized_stream_part_segment_inner(
            authorization,
            payload_crc64,
            storage_bytes,
            || {},
            || {},
        )
    }

    #[cfg(test)]
    pub(crate) fn test_append_authorized_stream_part_segment_with_before_prepare(
        &self,
        authorization: AuthorizedStreamPartSegment<'_, '_>,
        payload_crc64: u64,
        storage_bytes: &[u8],
        before_prepare: impl FnMut(),
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        self.append_authorized_stream_part_segment_inner(
            authorization,
            payload_crc64,
            storage_bytes,
            before_prepare,
            || {},
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_append_authorized_stream_part_segment_with_after_prepare(
        &self,
        authorization: AuthorizedStreamPartSegment<'_, '_>,
        payload_crc64: u64,
        storage_bytes: &[u8],
        after_prepare: impl FnMut(),
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        self.append_authorized_stream_part_segment_inner(
            authorization,
            payload_crc64,
            storage_bytes,
            || {},
            after_prepare,
        )
    }

}

impl<'admission> ActivePutObjectRoute<'admission> {
    fn load_stream_object_generation(
        &self,
        session_id: &SessionId,
    ) -> Result<GenerationId, ObjectPgActionError> {
        self.admission
            .require_valid_now_raw()
            .map_err(ObjectPgActionError::from)?;
        let generation_client = self
            .admission
            .cluster
            .object_generation_metadata_primary_client(&self.bucket, &self.key)
            .map_err(ObjectPgActionError::from)?;
        generation_client
            .open_object_generation_metadata_route(
                self.admission.cluster.operation_epoch(),
                self.object_pg_id,
                &self.bucket,
                &self.key,
            )
            .and_then(|route| route.object_generation_reservation(session_id))
    }

    /// Binds one stream segment to the exact durable PutObject session and
    /// object-generation reservation before encryption begins.
    pub fn authorize_stream_segment<'route>(
        &'route self,
        session_id: &SessionId,
        segment_index: u32,
        logical_plaintext_len: u64,
    ) -> Result<AuthorizedStreamPutSegment<'route, 'admission>, crate::StreamUploadFailure> {
        let session = self.load_stream_session(session_id)?;
        let Some(bucket_write_reservation) = session.bucket_write_reservation.as_ref() else {
            return Err(crate::StreamUploadFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "PutObject stream session is missing bucket write proof".to_string(),
                },
            ));
        };
        if session.bucket != self.bucket
            || session.key != self.key
            || session.session_id != *session_id
            || session.target != StreamUploadTarget::PutObject
            || session.state != StreamUploadState::InProgress
            || bucket_write_reservation.bucket != self.bucket
            || bucket_write_reservation.operation_kind
                != PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND
            || bucket_write_reservation.target_context.as_deref() != Some(self.key.as_str())
        {
            return Err(crate::StreamUploadFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason:
                        "stream segment subject does not match the admitted PutObject route"
                            .to_string(),
                },
            ));
        }
        let generation_id = self
            .load_stream_object_generation(session_id)
            .map_err(crate::StreamUploadFailure::from_object_pg_action)?;
        self.admission
            .require_valid_now_raw()
            .map_err(ObjectPgActionError::from)
            .map_err(crate::StreamUploadFailure::from_object_pg_action)?;
        let subject = SegmentEncryptionSubject::standard_object(
            ObjectEncryptionSubject::standard_object(
                bucket_write_reservation.bucket_incarnation_generation,
                &self.bucket,
                &self.key,
                generation_id,
                &session.encryption,
            ),
            segment_index,
            logical_plaintext_len,
        );
        Ok(AuthorizedStreamPutSegment {
            route: self,
            session,
            generation_id,
            segment_index,
            subject,
        })
    }

    pub fn authorize_stream_finalization<'route>(
        &'route self,
        session_id: &SessionId,
    ) -> Result<AuthorizedStreamPutFinalization<'route, 'admission>, crate::StreamUploadFailure>
    {
        let session = self.load_stream_session(session_id)?;
        if session.bucket != self.bucket
            || session.key != self.key
            || session.session_id != *session_id
            || session.target != StreamUploadTarget::PutObject
        {
            return Err(crate::StreamUploadFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "stream finalization subject does not match the admitted PutObject route"
                        .to_string(),
                },
            ));
        }
        Ok(AuthorizedStreamPutFinalization {
            route: self,
            session,
        })
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

    fn validate_stream_segment_authorization(
        &self,
        authorization: &AuthorizedStreamPutSegment<'_, '_>,
        storage_bytes: &[u8],
    ) -> Result<(), crate::StreamUploadFailure> {
        let logical_size = authorization
            .session
            .encryption
            .segment_logical_size(storage_bytes.len() as u64)
            .ok_or_else(|| {
                crate::StreamUploadFailure::from_object_pg_action(
                    ObjectPgActionError::InvalidRequest {
                        reason: "stream segment has invalid stored codec geometry".to_string(),
                    },
                )
            })?;
        if !std::ptr::eq(self, authorization.route)
            || authorization.session.bucket != self.bucket
            || authorization.session.key != self.key
            || authorization.session.target != StreamUploadTarget::PutObject
            || !authorization.subject.matches_standard_object_segment(
                &authorization.session.encryption,
                authorization.segment_index,
                logical_size,
            )
        {
            return Err(crate::StreamUploadFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason:
                        "stream segment authorization does not match the admitted PutObject route or payload"
                            .to_string(),
                },
            ));
        }
        let current_generation = match self
            .load_stream_object_generation(&authorization.session.session_id)
        {
            Ok(generation_id) => generation_id,
            Err(ObjectPgActionError::Metadata(
                MetadataError::ObjectGenerationReservationNotFound { .. },
            )) => {
                return Err(crate::StreamUploadFailure::from_object_pg_action(
                    ObjectPgActionError::InvalidRequest {
                        reason: "stream segment object generation reservation changed".to_string(),
                    },
                ));
            }
            Err(error) => {
                return Err(crate::StreamUploadFailure::from_object_pg_action(error));
            }
        };
        if current_generation != authorization.generation_id {
            return Err(crate::StreamUploadFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "stream segment object generation reservation changed".to_string(),
                },
            ));
        }
        Ok(())
    }

    /// Appends bytes encrypted under the exact subject minted from this
    /// route's durable stream session and object-generation reservation.
    pub fn append_authorized_stream_segment(
        &self,
        authorization: AuthorizedStreamPutSegment<'_, '_>,
        payload_crc64: u64,
        storage_bytes: &[u8],
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        self.validate_stream_segment_authorization(&authorization, storage_bytes)?;
        let input = StreamSegmentAppendInput {
            session_id: &authorization.session.session_id,
            segment_index: authorization.segment_index,
            payload_crc64,
            storage_bytes,
        };
        let expected_append_state = crate::ExpectedStreamAppendState::PutObject {
            session: authorization.session.clone(),
            generation_id: authorization.generation_id,
        };
        self.admission
            .cluster
            .append_stream_segment_with_route_validation(
                self.effect_route(),
                input,
                Some(expected_append_state),
                StreamAppendCallbacks {
                    validate_session: |current: &StreamUploadRecord| {
                        if authorization.matches_current_session(current) {
                            Ok(())
                        } else {
                            Err(ObjectPgActionError::InvalidRequest {
                                reason: "stream segment authorization no longer matches the durable session"
                                    .to_string(),
                            })
                        }
                    },
                    before_prepare: || {},
                    require_valid_route: || self.admission.require_valid_now_raw(),
                    after_prepare: || {},
                    maintain_lease: || Ok(()),
                },
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    #[cfg(test)]
    pub(crate) fn test_append_authorized_stream_segment_with_before_prepare(
        &self,
        authorization: AuthorizedStreamPutSegment<'_, '_>,
        payload_crc64: u64,
        storage_bytes: &[u8],
        before_prepare: impl FnMut(),
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        self.validate_stream_segment_authorization(&authorization, storage_bytes)?;
        let input = StreamSegmentAppendInput {
            session_id: &authorization.session.session_id,
            segment_index: authorization.segment_index,
            payload_crc64,
            storage_bytes,
        };
        let expected_append_state = crate::ExpectedStreamAppendState::PutObject {
            session: authorization.session.clone(),
            generation_id: authorization.generation_id,
        };
        self.admission
            .cluster
            .append_stream_segment_with_route_validation(
                self.effect_route(),
                input,
                Some(expected_append_state),
                StreamAppendCallbacks {
                    validate_session: |current: &StreamUploadRecord| {
                        if authorization.matches_current_session(current) {
                            Ok(())
                        } else {
                            Err(ObjectPgActionError::InvalidRequest {
                                reason: "stream segment authorization no longer matches the durable session"
                                    .to_string(),
                            })
                        }
                    },
                    before_prepare,
                    require_valid_route: || self.admission.require_valid_now_raw(),
                    after_prepare: || {},
                    maintain_lease: || Ok(()),
                },
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    /// Appends one authorized segment while maintaining the caller-owned
    /// reservation between preparation, shard writes, and publication.
    pub fn append_authorized_stream_segment_with_lease_maintenance(
        &self,
        authorization: AuthorizedStreamPutSegment<'_, '_>,
        payload_crc64: u64,
        storage_bytes: &[u8],
        mut maintain_lease: impl FnMut() -> Result<(), crate::StreamUploadFailure>,
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        self.validate_stream_segment_authorization(&authorization, storage_bytes)?;
        let input = StreamSegmentAppendInput {
            session_id: &authorization.session.session_id,
            segment_index: authorization.segment_index,
            payload_crc64,
            storage_bytes,
        };
        let expected_append_state = crate::ExpectedStreamAppendState::PutObject {
            session: authorization.session.clone(),
            generation_id: authorization.generation_id,
        };
        let result = self
            .admission
            .cluster
            .append_stream_segment_with_route_validation(
                self.effect_route(),
                input,
                Some(expected_append_state),
                StreamAppendCallbacks {
                    validate_session: |current: &StreamUploadRecord| {
                        if authorization.matches_current_session(current) {
                            Ok(())
                        } else {
                            Err(ObjectPgActionError::InvalidRequest {
                                reason: "stream segment authorization no longer matches the durable session"
                                    .to_string(),
                            })
                        }
                    },
                    before_prepare: || {},
                    require_valid_route: || self.admission.require_valid_now_raw(),
                    after_prepare: || {},
                    maintain_lease: || {
                        maintain_lease().map_err(AdmittedStreamAppendError::Maintenance)
                    },
                },
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
    pub(crate) fn test_append_authorized_stream_segment_with_after_prepare(
        &self,
        authorization: AuthorizedStreamPutSegment<'_, '_>,
        payload_crc64: u64,
        storage_bytes: &[u8],
        after_prepare: impl FnMut(),
    ) -> Result<StreamSegmentAppendOutcome, crate::StreamUploadFailure> {
        self.validate_stream_segment_authorization(&authorization, storage_bytes)?;
        let input = StreamSegmentAppendInput {
            session_id: &authorization.session.session_id,
            segment_index: authorization.segment_index,
            payload_crc64,
            storage_bytes,
        };
        let expected_append_state = crate::ExpectedStreamAppendState::PutObject {
            session: authorization.session.clone(),
            generation_id: authorization.generation_id,
        };
        self.admission
            .cluster
            .append_stream_segment_with_route_validation(
                self.effect_route(),
                input,
                Some(expected_append_state),
                StreamAppendCallbacks {
                    validate_session: |current: &StreamUploadRecord| {
                        if authorization.matches_current_session(current) {
                            Ok(())
                        } else {
                            Err(ObjectPgActionError::InvalidRequest {
                                reason: "stream segment authorization no longer matches the durable session"
                                    .to_string(),
                            })
                        }
                    },
                    before_prepare: || {},
                    require_valid_route: || self.admission.require_valid_now_raw(),
                    after_prepare,
                    maintain_lease: || Ok(()),
                },
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
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
        authorization: &AuthorizedStreamPutFinalization<'_, '_>,
        total_size: u64,
        action: impl FnMut(StreamPutFinalizeSnapshot) -> Result<PreparedStreamPutCommit<T>, E>,
    ) -> Result<Result<FinalizeStreamPutOutcome<T>, E>, crate::StreamUploadFailure> {
        if !std::ptr::eq(self, authorization.route)
            || authorization.session.bucket != self.bucket
            || authorization.session.key != self.key
            || authorization.session.target != StreamUploadTarget::PutObject
        {
            return Err(crate::StreamUploadFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "stream finalization authority does not match the admitted PutObject route"
                        .to_string(),
                },
            ));
        }
        self.admission
            .cluster
            .finalize_put_object_stream_with_route_validation(
                self.effect_route(),
                &authorization.session.session_id,
                total_size,
                || self.admission.require_valid_now_raw(),
                |current| {
                    if authorization.matches_current_session(current) {
                        Ok(())
                    } else {
                        Err(ObjectPgActionError::InvalidRequest {
                            reason:
                                "stream finalization authority no longer matches the durable session"
                                    .to_string(),
                        })
                    }
                },
                action,
            )
            .map_err(crate::StreamUploadFailure::from_object_pg_action)
    }

    pub fn write_direct_object_segment_payload(
        &self,
        encryption_reservation: DirectPutSegmentEncryptionReservation<'admission>,
        data: &[u8],
    ) -> Result<DirectPutPayloadWrite<'admission>, crate::DirectPutFailure> {
        let DirectPutSegmentEncryptionReservation {
            generation: generation_reservation,
            subject: encryption_subject,
        } = encryption_reservation;
        if !generation_reservation.issued_by(self.admission)
            || generation_reservation.bucket != self.bucket
            || generation_reservation.key != self.key
        {
            return Err(crate::DirectPutFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "direct PUT generation reservation does not match admitted object route"
                        .to_string(),
                },
            ));
        }
        let payload = self.write_direct_object_payload_for_reserved_generation(
            &generation_reservation.reservation_id,
            generation_reservation.generation_id,
            generation_reservation.bucket_incarnation_generation,
            encryption_subject,
            data,
        )?;
        generation_reservation.disarm();
        Ok(payload)
    }

    /// Owner-local adapter for fixtures that prepare encrypted bytes before
    /// entering the production segment-subject boundary.
    #[cfg(test)]
    pub(crate) fn write_direct_object_payload(
        &self,
        encryption_reservation: DirectPutEncryptionReservation<'admission>,
        logical_size: u64,
        data: &[u8],
    ) -> Result<DirectPutPayloadWrite<'admission>, crate::DirectPutFailure> {
        self.write_direct_object_segment_payload(
            encryption_reservation.bind_standard_segment(logical_size),
            data,
        )
    }

    fn write_direct_object_payload_for_reserved_generation(
        &self,
        generation_reservation_id: &SessionId,
        generation_id: GenerationId,
        bucket_incarnation_generation: u64,
        encryption_subject: SegmentEncryptionSubject,
        data: &[u8],
    ) -> Result<DirectPutPayloadWrite<'admission>, crate::DirectPutFailure> {
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
        let logical_size = encryption_subject.logical_plaintext_len();
        Ok(DirectPutPayloadWrite {
            owner: self.admission,
            armed: std::cell::Cell::new(true),
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            generation_reservation_id: generation_reservation_id.clone(),
            generation_id,
            bucket_incarnation_generation,
            encryption_subject,
            logical_size,
            segment_index,
            segment_crc64: checksum::crc64::checksum(data),
            segment_okh,
            segment_vid: generation_id,
            placement_cluster_epoch: self.admission.cluster.operation_epoch(),
            written,
        })
    }

    /// Constructs an owner-local malformed/recovery fixture after the normal
    /// one-use generation capability has been consumed.
    #[cfg(test)]
    pub(super) fn test_write_direct_object_payload_for_generation(
        &self,
        generation_reservation_id: &SessionId,
        generation_id: GenerationId,
        encryption: &ObjectEncryption,
        logical_size: u64,
        data: &[u8],
    ) -> Result<DirectPutPayloadWrite<'admission>, crate::DirectPutFailure> {
        let reservation = self
            .admission
            .cluster
            .acquire_durable_bucket_write_reservation(
                &self.bucket,
                PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND,
                Some(self.key.as_str()),
            )
            .map_err(bucket_snapshot_error_to_object_pg_action_error)
            .map_err(crate::DirectPutFailure::from_object_pg_action)?;
        let proof = BucketWriteReservationProof::from(&reservation.record);
        let bucket_incarnation_generation = proof.bucket_incarnation_generation;
        let encryption_subject = SegmentEncryptionSubject::standard_object(
            ObjectEncryptionSubject::standard_object(
                bucket_incarnation_generation,
                &self.bucket,
                &self.key,
                generation_id,
                encryption,
            ),
            0,
            logical_size,
        );
        self.admission
            .cluster
            .release_bucket_write_reservation_proof(&proof)
            .map_err(bucket_snapshot_error_to_object_pg_action_error)
            .map_err(crate::DirectPutFailure::from_object_pg_action)?;
        self.write_direct_object_payload_for_reserved_generation(
            generation_reservation_id,
            generation_id,
            bucket_incarnation_generation,
            encryption_subject,
            data,
        )
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
        if payload.placement_cluster_epoch != prepared.bucket_write_reservation.cluster_epoch
            || payload.bucket_incarnation_generation
                != prepared
                    .bucket_write_reservation
                    .bucket_incarnation_generation
        {
            self.admission
                .cluster
                .release_bucket_write_reservation_proof(&prepared.bucket_write_reservation)
                .map_err(bucket_snapshot_error_to_object_pg_action_error)
                .map_err(crate::DirectPutFailure::from_object_pg_action)?;
            return Err(crate::DirectPutFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "direct PUT payload does not match bucket write reservation subject"
                        .to_string(),
                },
            ));
        }
        let expected_encryption_subject = SegmentEncryptionSubject::standard_object(
            ObjectEncryptionSubject::standard_object(
                payload.bucket_incarnation_generation,
                &payload.bucket,
                &payload.key,
                payload.generation_id,
                &prepared.encryption,
            ),
            payload.segment_index,
            payload.logical_size,
        );
        if payload.encryption_subject != expected_encryption_subject {
            self.admission
                .cluster
                .release_bucket_write_reservation_proof(&prepared.bucket_write_reservation)
                .map_err(bucket_snapshot_error_to_object_pg_action_error)
                .map_err(crate::DirectPutFailure::from_object_pg_action)?;
            return Err(crate::DirectPutFailure::from_object_pg_action(
                ObjectPgActionError::InvalidRequest {
                    reason: "direct PUT payload does not match its encryption subject".to_string(),
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

    /// Load the exact in-progress upload on this admitted object route.
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
        self.begin_bucket_delete_if_current_with_finalize_capacity(
            bucket_identity,
            crate::maintenance::bucket_delete_finalize_admission_capacity(
                &self.admission.cluster,
            ),
        )
    }

    fn begin_bucket_delete_if_current_with_finalize_capacity(
        &self,
        bucket_identity: crate::cluster::BucketIdentityGenerations,
        finalize_capacity: usize,
    ) -> Result<(), crate::BucketWriteDrainFailure> {
        let root = crate::BucketDeleteFinalizeRoot {
            bucket: self.bucket.clone(),
            bucket_incarnation_generation: bucket_identity.bucket_incarnation_generation,
        };
        let runtime_state = self.admission.cluster.local_map.runtime_state();
        let mut finalize_admission = Some(
            runtime_state
            .try_admit_bucket_delete_finalize(root, finalize_capacity)
            .ok_or(crate::BucketWriteDrainError::BucketDeleteFinalizeBackpressure)
            .map_err(crate::BucketWriteDrainFailure::from)?,
        );
        let result = self
            .admission
            .cluster
            .begin_bucket_delete_if_current_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                bucket_identity,
                || {
                    finalize_admission
                        .take()
                        .expect("bucket delete admission must be unresolved")
                        .retain(crate::BucketDeleteBeginRoot {
                            bucket: self.bucket.clone(),
                            bucket_execution_generation: bucket_identity
                                .bucket_execution_generation,
                            bucket_incarnation_generation: bucket_identity
                                .bucket_incarnation_generation,
                        });
                },
            )
            .map_err(crate::BucketWriteDrainFailure::from);
        if result.is_ok() {
            finalize_admission
                .take()
                .expect("successful bucket delete admission must be unresolved")
                .commit();
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn begin_bucket_delete_with_finalize_capacity_for_test(
        &self,
        bucket_identity: crate::cluster::BucketIdentityGenerations,
        finalize_capacity: usize,
    ) -> Result<(), crate::BucketWriteDrainFailure> {
        self.begin_bucket_delete_if_current_with_finalize_capacity(
            bucket_identity,
            finalize_capacity,
        )
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

    pub fn put_bucket_versioning(
        &self,
        state: BucketVersioningState,
    ) -> Result<BucketMutationReceipt, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .put_bucket_versioning_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                state,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    fn put_bucket_property(
        &self,
        mutation: BucketPropertyMutation,
    ) -> Result<BucketMutationReceipt, BucketSnapshotLoadError> {
        self.admission
            .cluster
            .put_bucket_property_command_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                mutation,
            )
    }

    pub fn put_bucket_object_lock(
        &self,
        config: BucketObjectLockConfig,
    ) -> Result<BucketMutationReceipt, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property(BucketPropertyMutation::ObjectLock(config))
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn put_bucket_encryption(
        &self,
        config: BucketEncryptionConfig,
    ) -> Result<BucketMutationReceipt, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property(BucketPropertyMutation::Encryption(config))
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn put_bucket_public_access_block(
        &self,
        config: PublicAccessBlockConfig,
    ) -> Result<BucketMutationReceipt, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property(BucketPropertyMutation::PublicAccessBlock(Some(
            config,
        )))
        .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn delete_bucket_public_access_block(
        &self,
    ) -> Result<BucketMutationReceipt, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property(BucketPropertyMutation::PublicAccessBlock(None))
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn put_bucket_ownership_controls(
        &self,
        config: BucketOwnershipControls,
    ) -> Result<BucketMutationReceipt, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property(BucketPropertyMutation::OwnershipControls(Some(
            config,
        )))
        .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn delete_bucket_ownership_controls(
        &self,
    ) -> Result<BucketMutationReceipt, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property(BucketPropertyMutation::OwnershipControls(None))
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn put_bucket_abac_enabled(
        &self,
        enabled: bool,
    ) -> Result<BucketMutationReceipt, crate::BucketSnapshotLoadFailure> {
        self.put_bucket_property(BucketPropertyMutation::AbacEnabled(enabled))
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn put_bucket_acl(
        &self,
        acl_grants: &AclGrants,
        summary: BucketAclSummary,
    ) -> Result<BucketMutationReceipt, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .put_bucket_acl_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                acl_grants,
                summary,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn put_bucket_subresource(
        &self,
        req: PutBucketSubresource<'_>,
    ) -> Result<BucketMutationReceipt, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .put_bucket_subresource_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                req,
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn delete_bucket_subresource(
        &self,
        kind: crate::OpaqueBucketSubresourceKind,
    ) -> Result<BucketMutationReceipt, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .delete_bucket_subresource_with_route_validation(
                self.mutation_effect_route(),
                || self.admission.require_valid_now_raw(),
                kind.stored_kind(),
            )
            .map_err(crate::BucketSnapshotLoadFailure::from)
    }

    pub fn delete_bucket_tags(
        &self,
    ) -> Result<BucketMutationReceipt, crate::BucketSnapshotLoadFailure> {
        self.admission
            .cluster
            .delete_bucket_subresource_with_route_validation(
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
