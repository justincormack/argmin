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
    /// Insert or replace an object record (upsert for unversioned buckets).
    fn put_object_meta(&self, req: &PutObjectMetaReq) -> Result<(), MetadataError>;

    /// Get the current object record.
    fn get_object_meta(&self, bucket: &str, key: &str) -> Result<ObjectRecord, MetadataError>;

    /// Delete an object record.
    fn delete_object_meta(&self, bucket: &str, key: &str) -> Result<(), MetadataError>;

    /// List objects within this PG matching the request filters.
    fn list_objects(&self, req: &ListObjectsReq) -> Result<ListObjectsResp, MetadataError>;
}

/// Global metadata service (bucket table).
///
/// For v1-minimal this is a local SQLite database. In the distributed
/// version it becomes Raft-replicated.
pub trait GlobalService {
    /// Create a new bucket.
    fn create_bucket(&self, name: &str, owner_id: u64) -> Result<(), MetadataError>;

    /// Delete a bucket. Fails if the bucket is not empty.
    fn delete_bucket(&self, name: &str) -> Result<(), MetadataError>;

    /// Get bucket metadata.
    fn head_bucket(&self, name: &str) -> Result<BucketInfo, MetadataError>;

    /// List all buckets owned by the given owner.
    fn list_buckets(&self, owner_id: u64) -> Result<Vec<BucketInfo>, MetadataError>;
}

/// Multiplexes across PG stores on a single node.
pub trait StorageNode {
    /// Get the shard store for a specific PG.
    fn get_pg_store(&self, pg_id: u32) -> Result<&dyn ShardStore, StoreError>;

    /// List all PG IDs managed by this node.
    fn pg_ids(&self) -> &[u32];
}
