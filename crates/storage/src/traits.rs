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
    /// Insert or replace an object record.
    ///
    /// For version_id=0 (unversioned): INSERT OR REPLACE (overwrite).
    /// For version_id>0 (versioned): INSERT only (new version).
    fn put_object_meta(&self, req: &PutObjectMetaReq) -> Result<(), MetadataError>;

    /// Get the latest object record (highest version_id).
    ///
    /// Returns the latest version whether live or delete marker.
    /// The coordinator decides what to do with delete markers.
    fn get_object_meta(&self, bucket: &str, key: &str) -> Result<ObjectRecord, MetadataError>;

    /// Get a specific version of an object.
    fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
    ) -> Result<ObjectRecord, MetadataError>;

    /// Delete all versions of an object's metadata.
    fn delete_object_meta(&self, bucket: &str, key: &str) -> Result<(), MetadataError>;

    /// Delete a specific version of an object's metadata.
    fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
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
    fn next_version_id(&self, bucket: &str, key: &str) -> Result<u64, MetadataError>;
}

/// Global metadata service (bucket table).
///
/// For v1-minimal this is a local SQLite database. In the distributed
/// version it becomes Raft-replicated.
pub trait GlobalService {
    /// Create a new bucket.
    fn create_bucket(
        &self,
        name: &str,
        owner_principal: &str,
        public_read: bool,
    ) -> Result<(), MetadataError>;

    /// Delete a bucket. Fails if the bucket is not empty.
    fn delete_bucket(&self, name: &str) -> Result<(), MetadataError>;

    /// Get bucket metadata.
    fn head_bucket(&self, name: &str) -> Result<BucketInfo, MetadataError>;

    /// List all buckets owned by the given owner.
    fn list_buckets(&self, owner_principal: &str) -> Result<Vec<BucketInfo>, MetadataError>;

    /// Set bucket versioning state.
    ///
    /// Validates transitions: Disabled→Enabled and Enabled↔Suspended are allowed.
    /// Enabled→Disabled is rejected.
    fn put_bucket_versioning(&self, name: &str, state: u8) -> Result<(), MetadataError>;

    /// Store a CORS configuration for a bucket (serialized XML string).
    fn put_bucket_cors(&self, name: &str, config: &str) -> Result<(), MetadataError>;

    /// Retrieve a bucket's CORS configuration. Returns None if not set.
    fn get_bucket_cors(&self, name: &str) -> Result<Option<String>, MetadataError>;

    /// Delete a bucket's CORS configuration. Idempotent.
    fn delete_bucket_cors(&self, name: &str) -> Result<(), MetadataError>;
}

/// Multiplexes across PG stores on a single node.
pub trait StorageNode {
    /// Get the shard store for a specific PG.
    fn get_pg_store(&self, pg_id: u32) -> Result<&dyn ShardStore, StoreError>;

    /// List all PG IDs managed by this node.
    fn pg_ids(&self) -> &[u32];
}
