/// Trait definitions for the storage layer.
use crate::error::{MetadataError, StoreError};
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
pub trait PgMetadataStore {
    // ── Bucket metadata methods ─────────────────────────────────────

    /// Create a new bucket.
    fn create_bucket(
        &self,
        name: &str,
        owner_principal: &str,
        owner_canonical_id: &CanonicalUserId,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<(), MetadataError>;

    /// Delete a bucket row.
    ///
    /// Emptiness checks are handled at coordinator level.
    fn delete_bucket(&self, name: &str) -> Result<(), MetadataError>;

    /// Get bucket metadata.
    fn head_bucket(&self, name: &str) -> Result<BucketInfo, MetadataError>;

    /// Get bucket metadata including non-active lifecycle states.
    fn head_bucket_raw(&self, name: &str) -> Result<BucketInfo, MetadataError>;

    /// List all buckets owned by the given owner within this PG.
    fn list_buckets(&self, owner_principal: &str) -> Result<Vec<BucketInfo>, MetadataError>;

    /// Mark a bucket as deleting so it is hidden from normal operations while
    /// background cleanup drains outstanding reclaim work.
    fn mark_bucket_deleting(&self, name: &str) -> Result<(), MetadataError>;

    /// Acquire a short-lived bucket write reservation.
    ///
    /// Reservations fence bucket deletion while cross-PG write publication is
    /// in progress. Returns the current bucket metadata on success.
    fn acquire_bucket_write_reservation(&self, name: &str) -> Result<BucketInfo, MetadataError>;

    /// Release a previously acquired bucket write reservation.
    fn release_bucket_write_reservation(&self, name: &str) -> Result<(), MetadataError>;

    /// Block new bucket write reservations while deletion drains in-flight
    /// writers and verifies emptiness.
    fn begin_bucket_write_drain(&self, name: &str) -> Result<(), MetadataError>;

    /// Re-open the bucket for new write reservations after a failed delete.
    fn end_bucket_write_drain(&self, name: &str) -> Result<(), MetadataError>;

    /// Set bucket versioning state.
    ///
    /// Validates transitions: Disabled→Enabled and Enabled↔Suspended are allowed.
    /// Enabled→Disabled is rejected.
    fn put_bucket_versioning(
        &self,
        name: &str,
        state: BucketVersioningState,
    ) -> Result<(), MetadataError>;

    /// Store bucket-level Object Lock configuration.
    fn put_bucket_object_lock(
        &self,
        name: &str,
        config: BucketObjectLockConfig,
    ) -> Result<(), MetadataError>;

    /// Store a CORS configuration for a bucket (serialized XML string).
    fn put_bucket_cors(&self, name: &str, config: &str) -> Result<(), MetadataError>;

    /// Retrieve a bucket's CORS configuration. Returns None if not set.
    fn get_bucket_cors(&self, name: &str) -> Result<Option<String>, MetadataError>;

    /// Delete a bucket's CORS configuration. Idempotent.
    fn delete_bucket_cors(&self, name: &str) -> Result<(), MetadataError>;

    /// Store tags for a bucket (serialized XML string).
    fn put_bucket_tags(&self, name: &str, tags: &str) -> Result<(), MetadataError>;

    /// Retrieve a bucket's tags. Returns None if not set.
    fn get_bucket_tags(&self, name: &str) -> Result<Option<String>, MetadataError>;

    /// Delete a bucket's tags. Idempotent.
    fn delete_bucket_tags(&self, name: &str) -> Result<(), MetadataError>;

    /// Store a public access block configuration for a bucket (serialized XML string).
    fn put_bucket_public_access_block(&self, name: &str, config: &str)
        -> Result<(), MetadataError>;

    /// Retrieve a bucket's public access block configuration. Returns None if not set.
    fn get_bucket_public_access_block(&self, name: &str) -> Result<Option<String>, MetadataError>;

    /// Delete a bucket's public access block configuration. Idempotent.
    fn delete_bucket_public_access_block(&self, name: &str) -> Result<(), MetadataError>;

    /// Store a bucket policy document and its derived public/non-public summary.
    fn put_bucket_policy(
        &self,
        name: &str,
        policy: &str,
        is_public: bool,
    ) -> Result<(), MetadataError>;

    /// Retrieve a bucket's policy document. Returns None if not set.
    fn get_bucket_policy(&self, name: &str) -> Result<Option<String>, MetadataError>;

    /// Delete a bucket's policy document. Idempotent.
    fn delete_bucket_policy(&self, name: &str) -> Result<(), MetadataError>;

    /// Update a bucket's public ACL flags.
    fn put_bucket_acl(
        &self,
        name: &str,
        acl_grants: &AclGrants,
        public_read: bool,
        public_write: bool,
    ) -> Result<(), MetadataError>;

    /// Store ownership controls for a bucket.
    fn put_bucket_ownership_controls(&self, name: &str, config: &str) -> Result<(), MetadataError>;

    /// Retrieve a bucket's ownership controls. Returns None if not set.
    fn get_bucket_ownership_controls(&self, name: &str) -> Result<Option<String>, MetadataError>;

    /// Delete a bucket's ownership controls. Idempotent.
    fn delete_bucket_ownership_controls(&self, name: &str) -> Result<(), MetadataError>;

    /// Store the currently supported bucket encryption configuration subset.
    fn put_bucket_encryption(
        &self,
        name: &str,
        config: BucketEncryptionConfig,
    ) -> Result<(), MetadataError>;

    /// Retrieve the currently supported bucket encryption configuration subset.
    fn get_bucket_encryption(&self, name: &str) -> Result<BucketEncryptionConfig, MetadataError>;

    /// Insert or replace an object record.
    ///
    /// For VersionId::Null (unversioned): INSERT OR REPLACE (overwrite).
    /// For VersionId::Versioned (versioned): INSERT only (new version).
    fn put_object_meta(&self, req: &PutObjectReq) -> Result<(), MetadataError>;

    /// Get the latest object record (highest version_id).
    ///
    /// Returns the latest version whether live or delete marker.
    /// The coordinator decides what to do with delete markers.
    fn get_object_meta(&self, bucket: &str, key: &str) -> Result<StoredObject, MetadataError>;

    /// Get a specific version of an object.
    fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<StoredObject, MetadataError>;

    /// Update the ACL grants for a specific live object version.
    fn put_object_acl(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
        acl_grants: &AclGrants,
        public_read: bool,
    ) -> Result<(), MetadataError>;

    /// Delete all versions of an object's metadata.
    fn delete_object_meta(&self, bucket: &str, key: &str) -> Result<(), MetadataError>;

    /// Delete a specific version of an object's metadata.
    fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<(), MetadataError>;

    /// List objects within this PG matching the request filters.
    ///
    /// Returns only the latest live version per key (excludes keys
    /// where the latest version is a delete marker).
    fn list_objects(&self, req: &ListObjectsReq) -> Result<ListObjectsResp, MetadataError>;

    /// List all object versions within this PG, including delete markers.
    ///
    /// Returns versions ordered by (key ASC, version_id DESC).
    fn list_object_versions(
        &self,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, MetadataError>;

    /// Get the next version_id for a key (MAX(version_id) + 1).
    ///
    /// Returns 1 if no versions exist.
    fn next_version_id(&self, bucket: &str, key: &str) -> Result<VersionId, MetadataError>;

    /// Get the next internal payload generation_id for a key.
    ///
    /// Returns 1 if no live object generations exist.
    fn next_generation_id(&self, bucket: &str, key: &str) -> Result<GenerationId, MetadataError>;

    /// Insert a durable reclaim record for a simple single-shard-set payload.
    fn put_simple_payload_reclaim(
        &self,
        reclaim: &SimplePayloadReclaimRecord,
    ) -> Result<(), MetadataError>;

    /// Look up a durable reclaim record for a simple payload generation.
    fn get_simple_payload_reclaim(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> Result<Option<SimplePayloadReclaimRecord>, MetadataError>;

    /// Delete a durable reclaim record for a simple payload generation.
    fn delete_simple_payload_reclaim(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError>;

    /// Insert a durable reclaim record for a standard segmented payload.
    fn put_object_segments_reclaim(
        &self,
        reclaim: &ObjectSegmentsReclaimRecord,
    ) -> Result<(), MetadataError>;

    /// Look up a durable reclaim record for a standard segmented payload generation.
    fn get_object_segments_reclaim(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> Result<Option<ObjectSegmentsReclaimRecord>, MetadataError>;

    /// Delete a durable reclaim record for a standard segmented payload generation.
    fn delete_object_segments_reclaim(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError>;

    /// Insert a durable reclaim record for a multipart payload.
    fn put_multipart_reclaim(&self, reclaim: &MultipartReclaimRecord) -> Result<(), MetadataError>;

    /// Look up a durable reclaim record for a multipart payload generation.
    fn get_multipart_reclaim(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> Result<Option<MultipartReclaimRecord>, MetadataError>;

    /// Delete a durable reclaim record for a multipart payload generation.
    fn delete_multipart_reclaim(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> Result<(), MetadataError>;

    /// Return one reclaim root in the bucket, if any exist.
    ///
    /// Used by synchronous bucket deletion to drain deferred reclaim work.
    fn get_bucket_payload_reclaim_root(
        &self,
        bucket: &str,
    ) -> Result<Option<PayloadReclaimRoot>, MetadataError>;

    /// Store tags for an object version (serialized XML string).
    fn put_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
        tags: &str,
    ) -> Result<(), MetadataError>;

    /// Retrieve tags for an object version. Returns None if not set.
    fn get_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<Option<String>, MetadataError>;

    /// Delete tags for an object version. Idempotent.
    fn delete_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<(), MetadataError>;

    // ── Multipart upload methods ───────────────────────────────────

    /// Create a new multipart upload record.
    fn create_multipart_upload(&self, req: &CreateMultipartUploadReq) -> Result<(), MetadataError>;

    /// Get an in-progress multipart upload record.
    fn get_multipart_upload(&self, upload_id: &str)
        -> Result<MultipartUploadRecord, MetadataError>;

    /// Transition an upload's state. Only valid transitions from InProgress
    /// are accepted; returns `UploadNotInProgress` otherwise.
    fn set_upload_state(
        &self,
        upload_id: &str,
        new_state: UploadState,
    ) -> Result<(), MetadataError>;

    /// Delete a multipart upload and its parts (CASCADE).
    fn delete_multipart_upload(&self, upload_id: &str) -> Result<(), MetadataError>;

    /// List multipart uploads for a bucket with pagination.
    fn list_multipart_uploads(
        &self,
        req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, MetadataError>;

    /// Upsert a part row for an in-progress upload. Returns the previous
    /// generation if the part was overwritten.
    fn upsert_multipart_part(
        &self,
        part: &MultipartPartRecord,
    ) -> Result<Option<u32>, MetadataError>;

    /// Atomically upsert a multipart part and replace its staged segment rows.
    ///
    /// Returns the previous part generation, if any, together with the prior
    /// staged segment rows for this upload/part_number.
    fn upsert_multipart_part_segments(
        &self,
        part: &MultipartPartRecord,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(Option<u32>, Vec<MultipartPartSegmentRecord>), MetadataError>;

    /// Get a specific part of an in-progress upload.
    fn get_multipart_part(
        &self,
        upload_id: &str,
        part_number: u32,
    ) -> Result<MultipartPartRecord, MetadataError>;

    /// List parts of an in-progress upload with pagination.
    fn list_multipart_parts(&self, req: &ListPartsReq) -> Result<ListPartsResp, MetadataError>;

    /// Commit manifest rows into `object_parts` for a completed multipart object.
    fn commit_object_parts(&self, parts: &[ObjectPartRecord]) -> Result<(), MetadataError>;

    /// Read committed manifest parts for a multipart object.
    fn get_object_parts(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<Vec<ObjectPartRecord>, MetadataError>;

    /// Read committed manifest parts overlapping a byte range within a multipart object.
    ///
    /// `start` is inclusive and `end_exclusive` is exclusive.
    fn get_object_parts_overlapping_range(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
        start: u64,
        end_exclusive: u64,
    ) -> Result<Vec<ObjectPartRangeRecord>, MetadataError>;

    /// Delete committed manifest parts for an object version.
    fn delete_object_parts(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<(), MetadataError>;

    /// Atomically finalize a multipart upload.
    ///
    /// In a single transaction:
    /// 1. Transition upload to `Completing`
    /// 2. Write/overwrite the object metadata row
    /// 3. Delete any prior `object_parts` for this version_id (null-version overwrite)
    /// 4. Insert new `object_parts` manifest rows
    /// 5. Delete the `multipart_uploads` + `multipart_parts` rows
    fn complete_multipart_commit(
        &self,
        upload_id: &str,
        obj: &CommitMultipartReq,
        parts: &[ObjectPartRecord],
    ) -> Result<(), MetadataError>;

    // ── Streaming upload session methods ──────────────────────────────

    /// Create a new streaming upload session.
    fn create_stream_upload(&self, req: &CreateStreamUploadReq) -> Result<(), MetadataError>;

    /// Get a streaming upload session by ID.
    fn get_stream_upload(&self, session_id: &str) -> Result<StreamUploadRecord, MetadataError>;

    /// Transition a streaming upload session state. Only valid from InProgress.
    fn set_stream_upload_state(
        &self,
        session_id: &str,
        new_state: StreamUploadState,
    ) -> Result<(), MetadataError>;

    /// Delete a streaming upload session and its staging segments (CASCADE).
    fn delete_stream_upload(&self, session_id: &str) -> Result<(), MetadataError>;

    /// List all streaming upload sessions on this PG.
    ///
    /// Used by the startup scavenger to find abandoned sessions.
    fn list_all_stream_uploads(&self) -> Result<Vec<StreamUploadRecord>, MetadataError>;

    /// Append a staging segment record to an in-progress streaming session.
    fn append_stream_segment(
        &self,
        segment: &StreamUploadSegmentRecord,
    ) -> Result<(), MetadataError>;

    /// List staging segment records for a streaming session, ordered by segment_index.
    fn list_stream_segments(
        &self,
        session_id: &str,
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
    fn commit_stream_put(
        &self,
        session_id: &str,
        obj: &CommitStreamPutReq,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), MetadataError>;

    /// Atomically write or replace a live standard segmented object.
    ///
    /// In a single transaction:
    /// 1. Write/overwrite the object metadata row
    /// 2. Delete any prior object_segments for this version_id
    /// 3. Insert committed object segment rows
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
    /// 3. Insert committed multipart part segment rows
    /// 4. Delete the stream_uploads + stream_upload_segments staging rows
    fn commit_stream_part(
        &self,
        session_id: &str,
        part: &MultipartPartRecord,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), MetadataError>;

    /// Read committed segments for a StandardInternal object.
    fn get_object_segments(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<Vec<ObjectSegmentRecord>, MetadataError>;

    /// Delete committed segments for an object version.
    fn delete_object_segments(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<(), MetadataError>;

    /// Read committed segments for a multipart part.
    fn get_multipart_part_segments(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
        part_number: u32,
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError>;

    /// Delete all committed part segments for an object version.
    fn delete_multipart_part_segments(
        &self,
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<(), MetadataError>;

    /// Get all segment records for a given upload_id (any version_id / part_number).
    /// Used during abort to collect shard refs before deletion.
    fn get_all_multipart_part_segments_for_upload(
        &self,
        upload_id: &str,
    ) -> Result<Vec<MultipartPartSegmentRecord>, MetadataError>;

    /// Delete all segment records for a given upload_id.
    fn delete_multipart_part_segments_by_upload_id(
        &self,
        upload_id: &str,
    ) -> Result<(), MetadataError>;
}

/// Multiplexes across PG stores on a single node.
pub trait StorageNode {
    /// Get the shard store for a specific PG.
    fn get_pg_store(&self, pg_id: u32) -> Result<&dyn ShardStore, StoreError>;

    /// List all PG IDs managed by this node.
    fn pg_ids(&self) -> &[u32];
}
