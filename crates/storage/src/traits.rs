/// Trait definitions for the storage layer.
use crate::error::{MetadataError, StoreError};
use crate::metadata_command::BucketRecord;
use crate::types::*;

/// Per-PG shard store. One instance per PG directory.
///
/// All operations are synchronous. Implementations must ensure data
/// integrity via CRC64-NVME checksums on every read.
pub trait ShardStore {
    /// Write a shard to storage. Returns the CRC64 and stored size.
    ///
    /// The implementation computes CRC64-NVME over the data, writes
    /// atomically (temp + fsync + rename), and records the shard in
    /// the per-PG index.
    fn write_shard(&self, key: &ShardKey, data: &[u8]) -> Result<WriteAck, StoreError>;

    /// Read a shard from storage. Verifies CRC64-NVME on every read.
    ///
    /// Returns `IntegrityError` if the checksum does not match (the shard
    /// is quarantined). Returns `NotFound` if the shard does not exist.
    fn read_shard(&self, key: &ShardKey) -> Result<ShardData, StoreError>;

    /// Delete a shard. Idempotent — returns Ok even if the shard does
    /// not exist.
    fn delete_shard(&self, key: &ShardKey) -> Result<(), StoreError>;

    /// Stat a shard without reading its data.
    fn stat_shard(&self, key: &ShardKey) -> Result<ShardStat, StoreError>;
}

/// Per-PG object metadata store.
///
/// Tracks S3 object records within a single placement group.
/// The coordinator fans out across all PGs for operations like
/// ListObjectsV2.
pub(crate) trait PgMetadataStore {
    // ── Bucket metadata methods ─────────────────────────────────────

    /// Test-only direct bucket row seeder.
    #[cfg(test)]
    fn create_bucket(
        &self,
        name: &BucketName,
        owner_principal: &str,
        owner_canonical_id: &CanonicalUserId,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<(), MetadataError>;

    /// Delete a bucket row after bucket deletion has been finalized.
    ///
    /// This is the explicit finalized-delete exception to command-owned bucket
    /// mutation: the bucket has already been moved to `Deleting` by a routed
    /// metadata command, new writes are drained, all PGs have been checked for
    /// visible data and reclaim roots, and the cluster layer fans out this row
    /// deletion to the bucket PG acting set.
    fn delete_finalized_bucket(&self, name: &BucketName) -> Result<(), MetadataError>;

    /// Get bucket metadata.
    fn head_bucket(&self, name: &BucketName) -> Result<BucketInfo, MetadataError>;

    /// Get bucket metadata including non-active lifecycle states.
    fn head_bucket_raw(&self, name: &BucketName) -> Result<BucketInfo, MetadataError>;

    /// Get the exact bucket table row including raw storage-only columns.
    fn head_bucket_record_raw(&self, name: &BucketName) -> Result<BucketRecord, MetadataError>;

    /// List all buckets owned by the given owner within this PG.
    fn list_buckets(&self, owner_canonical_id: &str) -> Result<Vec<BucketInfo>, MetadataError>;

    /// List all active buckets in this PG with lifecycle configuration set.
    fn list_buckets_with_lifecycle(&self) -> Result<Vec<BucketInfo>, MetadataError>;

    /// List bucket names in this PG that currently have at least one multipart
    /// upload in `Aborting` state.
    fn list_buckets_with_aborting_multipart_uploads(
        &self,
    ) -> Result<Vec<BucketName>, MetadataError>;

    /// Mark a bucket as deleting so it is hidden from normal operations while
    /// background cleanup drains outstanding reclaim work.
    #[cfg(test)]
    fn mark_bucket_deleting(&self, name: &BucketName) -> Result<(), MetadataError>;

    /// Acquire a durable bucket write reservation record for one bucket
    /// incarnation.
    #[allow(dead_code)]
    #[allow(clippy::too_many_arguments)]
    fn acquire_durable_bucket_write_reservation(
        &self,
        name: &BucketName,
        reservation_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        operation_kind: &str,
        created_at: u64,
        lease_deadline: Option<u64>,
        target_context: Option<&str>,
    ) -> Result<BucketWriteReservationRecord, MetadataError>;

    /// Read a durable bucket write reservation by exact key.
    #[allow(dead_code)]
    fn durable_bucket_write_reservation(
        &self,
        name: &BucketName,
        reservation_id: &str,
    ) -> Result<Option<BucketWriteReservationRecord>, MetadataError>;

    /// List durable write reservations for a bucket.
    #[allow(dead_code)]
    fn durable_bucket_write_reservations(
        &self,
        name: &BucketName,
    ) -> Result<Vec<BucketWriteReservationRecord>, MetadataError>;

    /// Release a durable bucket write reservation by exact identity.
    #[allow(dead_code)]
    fn release_durable_bucket_write_reservation(
        &self,
        name: &BucketName,
        reservation_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        bucket_execution_generation: u64,
        bucket_incarnation_generation: u64,
    ) -> Result<(), MetadataError>;

    /// Release a metadata-command bucket write proof.
    ///
    /// This is the terminal command cleanup path for commands that transfer a
    /// bucket write reservation into the metadata command stream. It releases
    /// the durable reservation row in one PG transaction so a terminal command
    /// cannot lose its retry driver while leaving the bucket fenced.
    fn release_metadata_command_bucket_write_reservation(
        &self,
        name: &BucketName,
        reservation_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        bucket_execution_generation: u64,
        bucket_incarnation_generation: u64,
    ) -> Result<(), MetadataError>;

    /// Begin a durable bucket write drain for one bucket incarnation.
    #[allow(dead_code)]
    fn begin_durable_bucket_write_drain(
        &self,
        name: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        created_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<BucketWriteDrainRecord, MetadataError>;

    /// Read a durable bucket write-drain record.
    #[allow(dead_code)]
    fn durable_bucket_write_drain(
        &self,
        name: &BucketName,
    ) -> Result<Option<BucketWriteDrainRecord>, MetadataError>;

    /// Clear a durable bucket write drain by exact identity.
    #[allow(dead_code)]
    fn clear_durable_bucket_write_drain(
        &self,
        name: &BucketName,
        drain_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        bucket_execution_generation: u64,
    ) -> Result<(), MetadataError>;

    /// Atomically clear an expired durable drain for an active bucket.
    ///
    /// Returns the cleared record when the drain's lease deadline is present
    /// and not later than `now`. Non-expired and missing drains return `Ok(None)`.
    #[allow(dead_code)]
    fn clear_expired_durable_bucket_write_drain(
        &self,
        name: &BucketName,
        now: u64,
    ) -> Result<Option<BucketWriteDrainRecord>, MetadataError>;

    /// Set bucket versioning state.
    ///
    /// Validates transitions: Disabled→Enabled and Enabled↔Suspended are allowed.
    /// Enabled→Disabled is rejected.
    #[cfg(test)]
    fn put_bucket_versioning(
        &self,
        name: &BucketName,
        state: BucketVersioningState,
    ) -> Result<(), MetadataError>;

    /// Store bucket-level Object Lock configuration.
    #[cfg(test)]
    fn put_bucket_object_lock(
        &self,
        name: &BucketName,
        config: BucketObjectLockConfig,
    ) -> Result<(), MetadataError>;

    /// Update a bucket's public ACL flags.
    #[cfg(test)]
    fn put_bucket_acl(
        &self,
        name: &BucketName,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<(), MetadataError>;

    /// Store an opaque bucket subresource with typed auxiliary summary data.
    ///
    /// This is the generic storage boundary for bucket-scoped configuration
    /// payloads whose primary stored form is an opaque string.
    #[cfg(test)]
    fn put_bucket_subresource(
        &self,
        name: &BucketName,
        req: PutBucketSubresource<'_>,
    ) -> Result<(), MetadataError>;

    /// Retrieve an opaque bucket subresource and its typed summary data.
    fn get_bucket_subresource(
        &self,
        name: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<Option<StoredBucketSubresource>, MetadataError>;

    /// Delete an opaque bucket subresource. Idempotent.
    #[cfg(test)]
    fn delete_bucket_subresource(
        &self,
        name: &BucketName,
        kind: BucketSubresourceKind,
    ) -> Result<(), MetadataError>;

    /// Store the bucket public access block configuration.
    #[cfg(test)]
    fn put_bucket_public_access_block(
        &self,
        name: &BucketName,
        config: PublicAccessBlockConfig,
    ) -> Result<(), MetadataError>;

    /// Retrieve the bucket public access block configuration.
    #[cfg(test)]
    fn get_bucket_public_access_block(
        &self,
        name: &BucketName,
    ) -> Result<Option<PublicAccessBlockConfig>, MetadataError>;

    /// Delete the bucket public access block configuration. Idempotent.
    #[cfg(test)]
    fn delete_bucket_public_access_block(&self, name: &BucketName) -> Result<(), MetadataError>;

    /// Store the bucket ownership controls configuration.
    #[cfg(test)]
    fn put_bucket_ownership_controls(
        &self,
        name: &BucketName,
        config: BucketOwnershipControls,
    ) -> Result<(), MetadataError>;

    /// Retrieve the bucket ownership controls configuration.
    #[cfg(test)]
    fn get_bucket_ownership_controls(
        &self,
        name: &BucketName,
    ) -> Result<Option<BucketOwnershipControls>, MetadataError>;

    /// Delete the bucket ownership controls configuration. Idempotent.
    #[cfg(test)]
    fn delete_bucket_ownership_controls(&self, name: &BucketName) -> Result<(), MetadataError>;

    /// Store whether bucket ABAC is enabled for the bucket.
    #[cfg(test)]
    fn put_bucket_abac_enabled(
        &self,
        name: &BucketName,
        enabled: bool,
    ) -> Result<(), MetadataError>;

    /// Retrieve whether bucket ABAC is enabled for the bucket.
    #[cfg(test)]
    fn get_bucket_abac_enabled(&self, name: &BucketName) -> Result<bool, MetadataError>;

    /// Store the currently supported bucket encryption configuration subset.
    #[cfg(test)]
    fn put_bucket_encryption(
        &self,
        name: &BucketName,
        config: BucketEncryptionConfig,
    ) -> Result<(), MetadataError>;

    /// Retrieve the currently supported bucket encryption configuration subset.
    #[cfg(test)]
    fn get_bucket_encryption(
        &self,
        name: &BucketName,
    ) -> Result<BucketEncryptionConfig, MetadataError>;

    /// Test-only object row seeder.
    ///
    /// Production object publication must go through metadata command apply so
    /// digest-covered command-owned rows cannot be bypassed.
    #[cfg(test)]
    fn put_object_meta(&self, req: &PutObjectReq) -> Result<(), MetadataError>;

    /// Get the latest object record (highest version_id).
    ///
    /// Returns the latest version whether live or delete marker.
    /// The coordinator decides what to do with delete markers.
    fn get_object_meta(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<StoredObject, MetadataError>;

    /// Get a specific version of an object.
    fn get_object_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<StoredObject, MetadataError>;

    /// Update the ACL grants for a specific live object version.
    #[cfg(test)]
    fn put_object_acl(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        acl_grants: &AclGrants,
        public_read: bool,
    ) -> Result<(), MetadataError>;

    /// Update retention metadata for a specific live object version.
    #[cfg(test)]
    fn put_object_retention(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        retention: ObjectRetention,
    ) -> Result<(), MetadataError>;

    /// Update legal hold metadata for a specific live object version.
    #[cfg(test)]
    fn put_object_legal_hold(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        legal_hold: StoredLegalHoldStatus,
    ) -> Result<(), MetadataError>;

    /// Delete all versions of an object's metadata.
    #[cfg(test)]
    fn delete_object_meta(&self, bucket: &BucketName, key: &ObjectKey)
        -> Result<(), MetadataError>;

    /// Delete a specific version of an object's metadata.
    #[cfg(test)]
    fn delete_object_version(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError>;

    /// List objects within this PG matching the request filters.
    ///
    /// Returns only the latest live version per key (excludes keys
    /// where the latest version is a delete marker).
    fn list_objects(&self, req: &ListObjectsReq) -> Result<ListObjectsResp, MetadataError>;

    /// List all object versions within this PG, including delete markers.
    ///
    /// Returns versions ordered by key, then newest write first for each key.
    fn list_object_versions(
        &self,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, MetadataError>;

    /// List all versions for a single key, newest write first.
    fn list_object_versions_for_key(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<Vec<StoredObject>, MetadataError>;

    /// Get the next candidate version_id for a key without reserving it.
    ///
    /// Returns 1 if no versions exist.
    fn next_version_id(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<VersionId, MetadataError>;

    /// Get the next internal payload generation_id for a key.
    ///
    /// Returns 1 if no live object generations exist.
    fn next_generation_id(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
    ) -> Result<GenerationId, MetadataError>;

    /// Reserve a unique internal payload generation for an in-flight object write.
    #[cfg(test)]
    fn reserve_object_generation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, MetadataError>;

    /// Look up an in-flight object payload generation reservation.
    fn get_object_generation_reservation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<GenerationId, MetadataError>;

    /// Test-only direct release of an object payload generation reservation.
    #[cfg(test)]
    fn delete_object_generation_reservation(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        reservation_id: &SessionId,
    ) -> Result<(), MetadataError>;

    /// Insert a durable reclaim record for a standard segmented payload.
    #[cfg(any(test, feature = "test-hooks"))]
    fn put_object_segments_reclaim(
        &self,
        reclaim: &ObjectSegmentsReclaimRecord,
    ) -> Result<(), MetadataError>;

    /// Look up a durable reclaim record for a standard segmented payload generation.
    fn get_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectSegmentsReclaimRecord>, MetadataError>;

    /// Test-only direct delete of a standard segmented payload reclaim record.
    #[cfg(test)]
    fn delete_object_segments_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError>;

    /// Insert a durable reclaim record for a multipart payload.
    #[cfg(any(test, feature = "test-hooks"))]
    fn put_multipart_reclaim(&self, reclaim: &MultipartReclaimRecord) -> Result<(), MetadataError>;

    /// Look up a durable reclaim record for a multipart payload generation.
    fn get_multipart_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<Option<MultipartReclaimRecord>, MetadataError>;

    /// Test-only direct delete of a multipart payload reclaim record.
    #[cfg(test)]
    fn delete_multipart_reclaim(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError>;

    /// Return whether any durable reclaim record exists for this exact generation.
    fn payload_reclaim_exists(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
    ) -> Result<bool, MetadataError>;

    /// Return one reclaim root in the bucket, if any exist.
    ///
    /// Used by synchronous bucket deletion to drain deferred reclaim work.
    fn get_bucket_payload_reclaim_root(
        &self,
        bucket: &BucketName,
    ) -> Result<Option<PayloadReclaimRoot>, MetadataError>;

    /// Return one reclaim root on this metadata PG, if any exist.
    ///
    /// Used by reclaim workers to recover durable roots when local wakeup hints
    /// were lost across process restart.
    fn get_payload_reclaim_root(&self) -> Result<Option<PayloadReclaimRoot>, MetadataError>;

    /// Return deleting bucket finalizer roots on this metadata PG.
    ///
    /// Used by bucket finalizer workers to recover durable roots when local
    /// wakeup hints were lost across process restart. Expired singleton
    /// finalizer claims are returned first so their exact work remains
    /// recoverable even when another deleting bucket sorts earlier.
    fn get_bucket_delete_finalize_roots(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<BucketDeleteFinalizeRoot>, MetadataError>;

    /// Return lifecycle sweep roots on this bucket metadata PG.
    ///
    /// Expired lifecycle claims are returned first so their exact bucket
    /// incarnation remains recoverable even when another lifecycle bucket sorts
    /// earlier. Busy claims are skipped by ordinary lifecycle root scanning.
    fn get_lifecycle_sweep_roots(
        &self,
        now: u64,
        limit: usize,
    ) -> Result<Vec<LifecycleSweepRoot>, MetadataError>;

    /// Acquire the single durable object-payload reclaim claim for this PG.
    ///
    /// Returns `Ok(None)` when the reclaim root is absent or a non-expired
    /// claim owned by another worker is active.
    #[allow(clippy::too_many_arguments)]
    fn acquire_object_payload_reclaim_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        key: &ObjectKey,
        generation_id: GenerationId,
        reclaim_kind: ObjectPayloadReclaimKind,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<ObjectPayloadReclaimClaimRecord>, MetadataError>;

    /// Release a durable object-payload reclaim claim by exact token-fenced identity.
    #[allow(clippy::too_many_arguments)]
    fn release_object_payload_reclaim_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        key: &ObjectKey,
        generation_id: GenerationId,
        reclaim_kind: ObjectPayloadReclaimKind,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
    ) -> Result<(), MetadataError>;

    /// Acquire the single durable bucket-delete finalizer claim for this PG.
    #[allow(clippy::too_many_arguments)]
    fn acquire_bucket_delete_finalize_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<BucketDeleteFinalizeClaimRecord>, MetadataError>;

    /// Release a durable bucket-delete finalizer claim by exact token-fenced identity.
    fn release_bucket_delete_finalize_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
    ) -> Result<(), MetadataError>;

    /// Acquire a durable lifecycle sweep claim for one bucket incarnation.
    #[allow(clippy::too_many_arguments)]
    fn acquire_lifecycle_sweep_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        claimed_at: u64,
        lease_deadline: Option<u64>,
        now: u64,
    ) -> Result<Option<LifecycleSweepClaimRecord>, MetadataError>;

    /// Heartbeat a durable lifecycle sweep claim by exact token-fenced identity.
    #[allow(clippy::too_many_arguments)]
    fn heartbeat_lifecycle_sweep_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        heartbeat_at: u64,
        lease_deadline: Option<u64>,
    ) -> Result<LifecycleSweepClaimRecord, MetadataError>;

    /// Record retry context on a durable lifecycle sweep claim by exact token-fenced identity.
    #[allow(clippy::too_many_arguments)]
    fn record_lifecycle_sweep_claim_error(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
        last_error: &str,
    ) -> Result<LifecycleSweepClaimRecord, MetadataError>;

    /// Release a durable lifecycle sweep claim by exact token-fenced identity.
    fn release_lifecycle_sweep_claim(
        &self,
        bucket: &BucketName,
        bucket_incarnation_generation: u64,
        claim_id: &str,
        owner_token: &str,
        cluster_epoch: ClusterEpoch,
    ) -> Result<(), MetadataError>;

    /// Store tags for an object version (serialized XML string).
    #[cfg(test)]
    fn put_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        tags: &str,
    ) -> Result<(), MetadataError>;

    /// Retrieve tags for an object version. Returns None if not set.
    fn get_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Option<String>, MetadataError>;

    /// Delete tags for an object version. Idempotent.
    #[cfg(test)]
    fn delete_object_tags(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError>;

    // ── Multipart upload methods ───────────────────────────────────

    /// Create a new multipart upload record.
    #[cfg(test)]
    fn create_multipart_upload(&self, req: &CreateMultipartUploadReq) -> Result<(), MetadataError>;

    /// Get an in-progress multipart upload record.
    fn get_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<MultipartUploadRecord, MetadataError>;

    /// Test hook for forcing an upload state.
    #[cfg(any(test, feature = "test-hooks"))]
    fn set_upload_state(
        &self,
        upload_id: &UploadId,
        new_state: UploadState,
    ) -> Result<(), MetadataError>;

    /// Delete a multipart upload and its parts (CASCADE).
    #[cfg(test)]
    fn delete_multipart_upload(&self, upload_id: &UploadId) -> Result<(), MetadataError>;

    /// Get a completed multipart upload record retained for abort semantics.
    fn get_completed_multipart_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<Option<CompletedMultipartUploadRecord>, MetadataError>;

    /// Delete all completed multipart upload records for a bucket.
    #[cfg(test)]
    fn delete_completed_multipart_uploads_for_bucket(
        &self,
        bucket: &BucketName,
    ) -> Result<(), MetadataError>;

    /// List multipart uploads for a bucket with pagination.
    fn list_multipart_uploads(
        &self,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, MetadataError>;

    /// Upsert a part row for an in-progress upload. Returns the previous
    /// generation if the part was overwritten.
    #[cfg(test)]
    fn upsert_multipart_part(
        &self,
        part: &MultipartPartRecord,
    ) -> Result<Option<u32>, MetadataError>;

    /// Atomically upsert a multipart part and replace its staged segment rows.
    ///
    /// Returns the previous part generation, if any, together with the prior
    /// staged segment rows for this upload/part_number.
    #[cfg(test)]
    fn upsert_multipart_part_segments(
        &self,
        part: &MultipartPartRecord,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(Option<u32>, Vec<MultipartPartSegmentRecord>), MetadataError>;

    /// Get a specific part of an in-progress upload.
    fn get_multipart_part(
        &self,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<MultipartPartRecord, MetadataError>;

    /// List parts of an in-progress upload with pagination.
    fn list_multipart_parts(&self, req: &ListPartsReq) -> Result<ListPartsResp, MetadataError>;

    /// Commit manifest rows into `object_parts` for a completed multipart object.
    #[cfg(any(test, feature = "test-hooks"))]
    fn commit_object_parts(&self, parts: &[ObjectPartRecord]) -> Result<(), MetadataError>;

    /// Read committed manifest parts for a multipart object.
    fn get_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectPartRecord>, MetadataError>;

    /// Read committed manifest parts overlapping a byte range within a multipart object.
    ///
    /// `start` is inclusive and `end_exclusive` is exclusive.
    #[cfg(test)]
    fn get_object_parts_overlapping_range(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        start: u64,
        end_exclusive: u64,
    ) -> Result<Vec<ObjectPartRangeRecord>, MetadataError>;

    /// Test-only direct delete of committed manifest parts for an object version.
    #[cfg(any(test, feature = "test-hooks"))]
    fn delete_object_parts(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError>;

    /// Test-only direct multipart completion seeder.
    ///
    /// Production multipart completion must go through metadata command apply
    /// so command-owned object, upload, completed-upload, and streamed
    /// UploadPart session state cannot bypass the object-PG command stream.
    ///
    /// In a single transaction:
    /// 1. Transition upload to `Completing`
    /// 2. Write/overwrite the object metadata row
    /// 3. Delete any prior `object_parts` for this version_id (null-version overwrite)
    /// 4. Insert new `object_parts` manifest rows
    /// 5. Reparent only selected streamed part segments to the object version
    /// 6. Delete omitted streamed part segment rows and return omitted payloads for shard cleanup
    /// 7. Record the completed upload for AbortMultipartUpload semantics
    /// 8. Delete the `multipart_uploads` + `multipart_parts` rows
    #[cfg(test)]
    fn complete_multipart_commit(
        &self,
        upload_id: &UploadId,
        completion_order: u64,
        obj: &CommitMultipartReq,
        parts: &[ObjectPartRecord],
    ) -> Result<CompleteMultipartCommitCleanup, MetadataError>;

    // ── Streaming upload session methods ──────────────────────────────

    /// Create a new streaming upload session.
    #[cfg(any(test, feature = "test-hooks"))]
    fn create_stream_upload(&self, req: &CreateStreamUploadReq) -> Result<(), MetadataError>;

    /// Get a streaming upload session by ID.
    fn get_stream_upload(
        &self,
        session_id: &SessionId,
    ) -> Result<StreamUploadRecord, MetadataError>;

    /// Allocate a durable, per-session stream segment payload generation.
    fn allocate_stream_segment_vid(
        &self,
        session_id: &SessionId,
    ) -> Result<GenerationId, MetadataError>;

    /// Test-only direct transition of a streaming upload session state.
    #[cfg(test)]
    fn set_stream_upload_state(
        &self,
        session_id: &SessionId,
        new_state: StreamUploadState,
    ) -> Result<(), MetadataError>;

    /// Test-only direct delete of a streaming upload session and staging segments.
    #[cfg(test)]
    fn delete_stream_upload(&self, session_id: &SessionId) -> Result<(), MetadataError>;

    /// List all streaming upload sessions on this PG.
    ///
    /// Used by the startup scavenger to find abandoned sessions.
    fn list_all_stream_uploads(&self) -> Result<Vec<StreamUploadRecord>, MetadataError>;

    /// Test-only direct append of a staging segment record.
    #[cfg(test)]
    fn append_stream_segment(
        &self,
        segment: &StreamUploadSegmentRecord,
    ) -> Result<(), MetadataError>;

    /// List staging segment records for a streaming session, ordered by segment_index.
    fn list_stream_segments(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<StreamUploadSegmentRecord>, MetadataError>;

    /// Atomically finalize a streaming PutObject.
    ///
    /// In a single transaction:
    /// 1. Transition session to Completing
    /// 2. Write/overwrite the object metadata row
    /// 3. Delete any prior object_segments for this version_id
    /// 4. Insert committed object segment rows
    /// 5. Delete the stream_uploads + stream_upload_segments staging rows
    /// 6. Mark session Completed (implicitly via deletion)
    #[cfg(test)]
    fn commit_stream_put(
        &self,
        session_id: &SessionId,
        obj: &CommitStreamPutReq,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), MetadataError>;

    /// Atomically write or replace a live standard segmented object.
    ///
    /// In a single transaction:
    /// 1. Write/overwrite the object metadata row
    /// 2. Delete any prior object_segments for this version_id
    /// 3. Insert committed object segment rows
    #[cfg(any(test, feature = "test-hooks"))]
    fn put_object_with_segments(
        &self,
        obj: &PutLiveObjectReq,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), MetadataError>;

    /// Atomically finalize a streaming UploadPart.
    ///
    /// In a single transaction:
    /// 1. Transition session to Completing
    /// 2. Upsert multipart part metadata
    /// 3. Return any prior committed multipart part segment rows displaced by
    ///    this re-upload so callers can clean up their shard data
    /// 4. Insert committed multipart part segment rows
    /// 5. Delete the stream_uploads + stream_upload_segments staging rows
    #[cfg(test)]
    fn commit_stream_part(
        &self,
        session_id: &SessionId,
        part: &MultipartPartRecord,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError>;

    /// Read committed segments for a StandardInternal object.
    fn get_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<Vec<ObjectSegmentRecord>, MetadataError>;

    /// Test-only direct delete of committed segments for an object version.
    #[cfg(test)]
    fn delete_object_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError>;

    /// Read committed segments for a multipart part.
    fn get_multipart_part_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
        part_number: u32,
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError>;

    /// Read staged segments for one multipart upload part.
    fn get_multipart_part_segments_for_upload_part(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        upload_id: &UploadId,
        part_number: u32,
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError>;

    /// Test-only direct delete of committed part segments for an object version.
    #[cfg(test)]
    fn delete_multipart_part_segments(
        &self,
        bucket: &BucketName,
        key: &ObjectKey,
        version_id: VersionId,
    ) -> Result<(), MetadataError>;

    /// Get all segment records for a given upload_id (any version_id / part_number).
    /// Used during abort to collect shard refs before deletion.
    fn get_all_multipart_part_segments_for_upload(
        &self,
        upload_id: &UploadId,
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError>;

    /// Test-only direct delete of all segment records for a given upload_id.
    #[cfg(test)]
    fn delete_multipart_part_segments_by_upload_id(
        &self,
        upload_id: &UploadId,
    ) -> Result<(), MetadataError>;
}

/// Multiplexes across PG stores on a single node.
pub trait StorageNode {
    /// Get the shard store for a specific PG.
    fn get_pg_store(&self, pg_id: u32) -> Result<&dyn ShardStore, StoreError>;

    /// List all PG IDs managed by this node.
    fn pg_ids(&self) -> &[u32];
}
