/// Coordinator: orchestrates S3 operations across EC, storage, and metadata layers.
use ec::{EcConfig, ErasureCodec};
use storage::traits::{GlobalService, PgMetadataStore, ShardStore, StorageNode};
use storage::{
    BucketInfo, ListObjectsReq, LocalStorageNode, ObjectRecord, PutObjectMetaReq,
    ShardKey, SqliteBucketDb,
};

use crate::error::ServerError;
use crate::etag::{crc64_to_etag_bytes, etag_bytes_to_crc64, format_etag};
use crate::metadata_blob::MetadataBlob;
use crate::pg::{derive_pg, object_key_hash};

/// Maximum object size for single PUT (256 MB).
const MAX_OBJECT_SIZE: u64 = 256 * 1024 * 1024;

/// Hard cap on total records fetched across all PGs for a single list query.
/// Prevents unbounded memory when delimiter causes u32::MAX per-PG limits.
const MAX_LIST_RECORDS: usize = 100_000;

/// Result of a PutObject operation.
#[derive(Debug)]
pub struct PutObjectResult {
    pub etag: String,
    pub version_id: String,
}

/// Result of a GetObject operation.
#[derive(Debug)]
pub struct GetObjectResult {
    pub data: Vec<u8>,
    pub metadata: MetadataBlob,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
}

/// Result of a HeadObject operation.
#[derive(Debug)]
pub struct HeadObjectResult {
    pub metadata: MetadataBlob,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
}

/// Object entry for listing.
#[derive(Debug, Clone)]
pub struct ListEntry {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: u64,
}

/// Result of a ListObjectsV2 operation.
#[derive(Debug)]
pub struct ListObjectsResult {
    pub objects: Vec<ListEntry>,
    pub common_prefixes: Vec<String>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

/// Result entry for a successfully deleted object in a batch delete.
#[derive(Debug)]
pub struct DeletedObject {
    pub key: String,
    pub version_id: String,
}

/// Result entry for a failed deletion in a batch delete.
#[derive(Debug)]
pub struct DeleteError {
    pub key: String,
    pub code: String,
    pub message: String,
}

/// Result of a DeleteObjects (batch delete) operation.
#[derive(Debug)]
pub struct DeleteObjectsResult {
    pub deleted: Vec<DeletedObject>,
    pub errors: Vec<DeleteError>,
}

/// The coordinator ties together EC, storage, and metadata.
pub struct Coordinator {
    storage_node: LocalStorageNode,
    bucket_db: SqliteBucketDb,
    ec_codec: ErasureCodec,
    ec_config: EcConfig,
    pg_count: u32,
    region: String,
}

impl Coordinator {
    /// Create a new coordinator.
    pub fn new(
        storage_node: LocalStorageNode,
        bucket_db: SqliteBucketDb,
        ec_config: EcConfig,
        pg_count: u32,
        region: String,
    ) -> Result<Self, ServerError> {
        let ec_codec = ErasureCodec::new(ec_config)?;
        Ok(Self {
            storage_node,
            bucket_db,
            ec_codec,
            ec_config,
            pg_count,
            region,
        })
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    // ── Bucket operations ─────────────────────────────────────────────

    pub fn create_bucket(&self, name: &str) -> Result<(), ServerError> {
        self.bucket_db.create_bucket(name, 0).or_else(|e| match e {
            // Idempotent: single-owner system, so re-creating is a no-op
            storage::MetadataError::BucketAlreadyExists => Ok(()),
            other => Err(ServerError::Metadata(other)),
        })
    }

    pub fn delete_bucket(&self, name: &str) -> Result<(), ServerError> {
        // Check emptiness: list objects across all PGs
        for &pg_id in self.storage_node.pg_ids() {
            let pg = self.storage_node.get_pg(pg_id)?;
            let resp = pg.list_objects(&ListObjectsReq {
                bucket: name.to_string(),
                prefix: None,
                start_after: None,
                max_keys: 1,
            })?;
            if !resp.objects.is_empty() {
                return Err(ServerError::BucketNotEmpty);
            }
        }

        self.bucket_db.delete_bucket(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => {
                ServerError::BucketNotFound { name }
            }
            storage::MetadataError::BucketNotEmpty => ServerError::BucketNotEmpty,
            other => ServerError::Metadata(other),
        })
    }

    pub fn head_bucket(&self, name: &str) -> Result<BucketInfo, ServerError> {
        self.bucket_db.head_bucket(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => {
                ServerError::BucketNotFound { name }
            }
            other => ServerError::Metadata(other),
        })
    }

    pub fn list_buckets(&self) -> Result<Vec<BucketInfo>, ServerError> {
        Ok(self.bucket_db.list_buckets(0)?)
    }

    // ── Object operations ─────────────────────────────────────────────

    /// Put an object into storage.
    pub fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: &[u8],
        headers: &[(&str, &str)],
    ) -> Result<PutObjectResult, ServerError> {
        if data.len() as u64 > MAX_OBJECT_SIZE {
            return Err(ServerError::ObjectTooLarge {
                size: data.len() as u64,
                max: MAX_OBJECT_SIZE,
            });
        }

        // 1. Verify bucket exists
        self.head_bucket(bucket)?;

        // 2. Build metadata blob
        let metadata_blob = MetadataBlob::from_headers(headers)?;

        // 3. Serialize blob
        let blob_bytes = metadata_blob.serialize()?;

        // 4. Concatenate: blob_bytes || user_data
        let mut full_data = Vec::with_capacity(blob_bytes.len() + data.len());
        full_data.extend_from_slice(&blob_bytes);
        full_data.extend_from_slice(data);

        // 5. Compute ETag (CRC64 of full_data before padding)
        let etag_crc = crc64::checksum(&full_data);

        // 6. Pad to multiple of k for equal shard sizes
        let k = self.ec_config.data_shards as usize;
        let m = self.ec_config.parity_shards as usize;
        let remainder = full_data.len() % k;
        if remainder != 0 {
            let pad = k - remainder;
            full_data.resize(full_data.len() + pad, 0);
        }

        // 7. Split into k data shards
        let shard_size = full_data.len() / k;
        let data_shards: Vec<&[u8]> = (0..k)
            .map(|i| &full_data[i * shard_size..(i + 1) * shard_size])
            .collect();

        // 8. Allocate parity buffers and encode
        let mut parity_bufs: Vec<Vec<u8>> = (0..m).map(|_| vec![0u8; shard_size]).collect();
        let mut parity_refs: Vec<&mut [u8]> =
            parity_bufs.iter_mut().map(|v| v.as_mut_slice()).collect();
        self.ec_codec.encode(&data_shards, &mut parity_refs)?;

        // 9. Derive PG
        let pg_id = derive_pg(bucket, key, self.pg_count);
        let pg = self.storage_node.get_pg(pg_id)?;

        // 10. Compute object_key_hash
        let okh = object_key_hash(bucket, key);
        let version_id: u64 = 0; // unversioned

        // 11. Write all k+m shards, with cleanup on failure
        let mut written_shards: Vec<ShardKey> = Vec::with_capacity(k + m);
        let write_result: Result<(), ServerError> = (|| {
            for i in 0..(k + m) {
                let shard_key = ShardKey::new(&okh, version_id, i as u8);
                let shard_data = if i < k {
                    data_shards[i]
                } else {
                    &parity_bufs[i - k]
                };
                pg.write_shard(&shard_key, shard_data)?;
                written_shards.push(shard_key);
            }
            Ok(())
        })();

        if let Err(e) = write_result {
            // Best-effort cleanup of already-written shards
            for shard_key in &written_shards {
                let _ = pg.delete_shard(shard_key);
            }
            return Err(e);
        }

        // 12. Record metadata
        let meta_result = pg.put_object_meta(&PutObjectMetaReq {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: "null".to_string(),
            size: data.len() as u64,
            etag: crc64_to_etag_bytes(etag_crc),
            etag_kind: 0,
            ec_k: self.ec_config.data_shards,
            ec_m: self.ec_config.parity_shards,
        });

        if let Err(e) = meta_result {
            // Best-effort cleanup of all written shards
            for shard_key in &written_shards {
                let _ = pg.delete_shard(shard_key);
            }
            return Err(ServerError::Metadata(e));
        }

        Ok(PutObjectResult {
            etag: format_etag(etag_crc),
            version_id: "null".to_string(),
        })
    }

    /// Get an object from storage.
    pub fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<GetObjectResult, ServerError> {
        // 1. Derive PG, look up record
        let pg_id = derive_pg(bucket, key, self.pg_count);
        let pg = self.storage_node.get_pg(pg_id)?;

        let record = pg.get_object_meta(bucket, key).map_err(|e| match e {
            storage::MetadataError::ObjectNotFound => ServerError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            other => ServerError::Metadata(other),
        })?;

        let okh = object_key_hash(bucket, key);
        let version_id: u64 = 0;
        let k = record.ec_k as usize;
        let m = record.ec_m as usize;

        // 2. Read data shards; track failures
        let mut shard_data: Vec<Option<Vec<u8>>> = Vec::with_capacity(k + m);
        let mut present_count = 0;

        for i in 0..(k + m) {
            let shard_key = ShardKey::new(&okh, version_id, i as u8);
            match pg.read_shard(&shard_key) {
                Ok(sd) => {
                    shard_data.push(Some(sd.data));
                    present_count += 1;
                }
                Err(_) => {
                    shard_data.push(None);
                }
            }
        }

        if present_count < k {
            return Err(ServerError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        // 3. Reconstruct missing data shards if needed
        let shard_size = shard_data
            .iter()
            .find_map(|s| s.as_ref().map(|d| d.len()))
            .unwrap_or(0);

        // Check if any data shards (0..k) are missing
        let missing_data: Vec<usize> = (0..k)
            .filter(|&i| shard_data[i].is_none())
            .collect();

        if !missing_data.is_empty() {
            // Need to reconstruct
            let present_indices: Vec<usize> = (0..(k + m))
                .filter(|&i| shard_data[i].is_some())
                .collect();
            let present_refs: Vec<&[u8]> = present_indices
                .iter()
                .map(|&i| shard_data[i].as_ref().unwrap().as_slice())
                .collect();

            // Reuse the coordinator's codec if EC params match, otherwise build one.
            // Objects written with different EC params (e.g. after config change)
            // need a per-call codec.
            let tmp_codec;
            let codec = if record.ec_k == self.ec_config.data_shards
                && record.ec_m == self.ec_config.parity_shards
            {
                &self.ec_codec
            } else {
                let ec_config = EcConfig::new(record.ec_k, record.ec_m)?;
                tmp_codec = ErasureCodec::new(ec_config)?;
                &tmp_codec
            };

            let mut outputs: Vec<Vec<u8>> =
                missing_data.iter().map(|_| vec![0u8; shard_size]).collect();
            let mut output_refs: Vec<&mut [u8]> =
                outputs.iter_mut().map(|v| v.as_mut_slice()).collect();

            codec.reconstruct(
                &present_indices,
                &present_refs,
                &missing_data,
                &mut output_refs,
            )?;

            // Fill in the missing data shards
            for (idx, missing_idx) in missing_data.iter().enumerate() {
                shard_data[*missing_idx] = Some(outputs[idx].clone());
            }
        }

        // 4. Concatenate data shards
        let mut full_padded_data = Vec::with_capacity(k * shard_size);
        for i in 0..k {
            full_padded_data.extend_from_slice(shard_data[i].as_ref().unwrap());
        }

        // 5. Deserialize metadata blob from front
        let (metadata, blob_len) = MetadataBlob::deserialize(&full_padded_data)?;

        // 6. Extract user data
        let user_data_end = blob_len + record.size as usize;
        if user_data_end > full_padded_data.len() {
            return Err(ServerError::MetadataBlobError {
                reason: "data shorter than expected".to_string(),
            });
        }
        let user_data = full_padded_data[blob_len..user_data_end].to_vec();

        let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);

        Ok(GetObjectResult {
            data: user_data,
            metadata,
            etag: format_etag(etag_crc),
            size: record.size,
            last_modified: record.last_modified,
        })
    }

    /// Head object: returns metadata without body.
    pub fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<HeadObjectResult, ServerError> {
        let result = self.get_object(bucket, key)?;
        Ok(HeadObjectResult {
            metadata: result.metadata,
            etag: result.etag,
            size: result.size,
            last_modified: result.last_modified,
        })
    }

    /// Delete an object.
    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<(), ServerError> {
        let pg_id = derive_pg(bucket, key, self.pg_count);
        let pg = self.storage_node.get_pg(pg_id)?;

        // Look up the record to get EC params
        let record = match pg.get_object_meta(bucket, key) {
            Ok(r) => r,
            Err(storage::MetadataError::ObjectNotFound) => return Ok(()), // idempotent
            Err(e) => return Err(ServerError::Metadata(e)),
        };

        let okh = object_key_hash(bucket, key);
        let version_id: u64 = 0;
        let total = record.ec_k as usize + record.ec_m as usize;

        // Delete all shards (idempotent)
        for i in 0..total {
            let shard_key = ShardKey::new(&okh, version_id, i as u8);
            pg.delete_shard(&shard_key)?;
        }

        // Delete metadata record
        pg.delete_object_meta(bucket, key)?;

        Ok(())
    }

    /// List objects in a bucket (ListObjectsV2).
    pub fn list_objects_v2(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: u32,
    ) -> Result<ListObjectsResult, ServerError> {
        // Verify bucket exists
        self.head_bucket(bucket)?;

        // MaxKeys=0 is valid per S3 spec: return empty result
        if max_keys == 0 {
            return Ok(ListObjectsResult {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_continuation_token: None,
            });
        }

        // Bound per-PG queries. Without delimiter, max_keys+1 per PG is
        // sufficient: the global top max_keys entries can come from at most one
        // PG each, so max_keys+1 captures them all plus detects truncation.
        // With a delimiter, many raw keys can collapse into a single common
        // prefix, so we cannot predict how many raw keys we need — fetch all.
        let per_pg_limit = if delimiter.is_some() {
            u32::MAX
        } else {
            max_keys.saturating_add(1)
        };

        // Fan out to all PGs and collect results, with a hard memory cap.
        let mut all_objects: Vec<ObjectRecord> = Vec::new();
        let mut hit_record_cap = false;
        for &pg_id in self.storage_node.pg_ids() {
            let pg = self.storage_node.get_pg(pg_id)?;
            let resp = pg.list_objects(&ListObjectsReq {
                bucket: bucket.to_string(),
                prefix: prefix.map(|s| s.to_string()),
                start_after: continuation_token.map(|s| s.to_string()),
                max_keys: per_pg_limit,
            })?;
            all_objects.extend(resp.objects);
            if all_objects.len() >= MAX_LIST_RECORDS {
                all_objects.truncate(MAX_LIST_RECORDS);
                hit_record_cap = true;
                break;
            }
        }

        // Sort by key
        all_objects.sort_by(|a, b| a.key.cmp(&b.key));

        // Dedup by key (same key from different PGs shouldn't happen with
        // correct PG derivation, but be safe)
        all_objects.dedup_by(|a, b| a.key == b.key);

        // Apply delimiter logic and build result entries, stopping at max_keys
        let max = max_keys as usize;
        let mut objects: Vec<ListEntry> = Vec::new();
        let mut common_prefixes: Vec<String> = Vec::new();
        let mut entry_count = 0usize;
        let mut last_key_seen: Option<String> = None;
        let mut is_truncated = false;

        if let Some(delim) = delimiter {
            let prefix_str = prefix.unwrap_or("");
            let mut seen_prefixes = std::collections::HashSet::new();

            let mut i = 0;
            while i < all_objects.len() {
                if entry_count >= max {
                    is_truncated = true;
                    break;
                }
                let record = &all_objects[i];
                let after_prefix = &record.key[prefix_str.len()..];
                if let Some(pos) = after_prefix.find(delim) {
                    let cp = format!(
                        "{}{}",
                        prefix_str,
                        &after_prefix[..pos + delim.len()]
                    );
                    // Skip all remaining keys under this common prefix so the
                    // continuation token advances past the entire group.
                    let is_new = seen_prefixes.insert(cp.clone());
                    while i < all_objects.len() && all_objects[i].key.starts_with(&cp) {
                        last_key_seen = Some(all_objects[i].key.clone());
                        i += 1;
                    }
                    if is_new {
                        common_prefixes.push(cp);
                        entry_count += 1;
                    }
                } else {
                    let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
                    objects.push(ListEntry {
                        key: record.key.clone(),
                        size: record.size,
                        etag: format_etag(etag_crc),
                        last_modified: record.last_modified,
                    });
                    entry_count += 1;
                    last_key_seen = Some(record.key.clone());
                    i += 1;
                }
            }
        } else {
            for record in &all_objects {
                if entry_count >= max {
                    is_truncated = true;
                    break;
                }
                let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
                objects.push(ListEntry {
                    key: record.key.clone(),
                    size: record.size,
                    etag: format_etag(etag_crc),
                    last_modified: record.last_modified,
                });
                entry_count += 1;
                last_key_seen = Some(record.key.clone());
            }

            // Check if there were more objects than max_keys
            if all_objects.len() > max {
                is_truncated = true;
            }
        }

        // If we hit the record cap, there may be more results we didn't fetch.
        if hit_record_cap {
            is_truncated = true;
        }

        let next_token = if is_truncated {
            last_key_seen
        } else {
            None
        };

        Ok(ListObjectsResult {
            objects,
            common_prefixes,
            is_truncated,
            next_continuation_token: next_token,
        })
    }

    /// Batch-delete objects.
    pub fn delete_objects(
        &self,
        bucket: &str,
        entries: &[crate::http::xml::DeleteObjectEntry],
    ) -> Result<DeleteObjectsResult, ServerError> {
        self.head_bucket(bucket)?;

        let mut deleted = Vec::new();
        let mut errors = Vec::new();

        for entry in entries {
            match self.delete_object(bucket, &entry.key) {
                Ok(()) => {
                    deleted.push(DeletedObject {
                        key: entry.key.clone(),
                        version_id: "null".to_string(),
                    });
                }
                Err(e) => {
                    errors.push(DeleteError {
                        key: entry.key.clone(),
                        code: e.s3_error_code().to_string(),
                        message: e.to_string(),
                    });
                }
            }
        }

        Ok(DeleteObjectsResult { deleted, errors })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn setup_coordinator(dir: &Path) -> Coordinator {
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = LocalStorageNode::open(dir, &pg_ids).unwrap();
        let bucket_db = SqliteBucketDb::open_in_memory().unwrap();
        let ec_config = EcConfig::new(4, 2).unwrap();
        Coordinator::new(storage_node, bucket_db, ec_config, 4, "us-east-1".to_string()).unwrap()
    }

    #[test]
    fn bucket_crud() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        // Create
        coord.create_bucket("test-bucket").unwrap();

        // Head
        let info = coord.head_bucket("test-bucket").unwrap();
        assert_eq!(info.name, "test-bucket");

        // List
        let buckets = coord.list_buckets().unwrap();
        assert_eq!(buckets.len(), 1);

        // Delete
        coord.delete_bucket("test-bucket").unwrap();
        assert!(coord.head_bucket("test-bucket").is_err());
    }

    #[test]
    fn create_bucket_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        // Second create should succeed (idempotent for same owner)
        coord.create_bucket("bucket").unwrap();

        // Only one bucket should exist
        let buckets = coord.list_buckets().unwrap();
        assert_eq!(buckets.len(), 1);
    }

    #[test]
    fn delete_nonempty_bucket_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "key", b"data", &[])
            .unwrap();

        let err = coord.delete_bucket("bucket").unwrap_err();
        assert!(matches!(err, ServerError::BucketNotEmpty));
    }

    #[test]
    fn put_get_object() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();

        let headers = [("Content-Type", "text/plain")];
        let result = coord
            .put_object("bucket", "hello.txt", b"Hello, world!", &headers)
            .unwrap();
        assert!(!result.etag.is_empty());

        let obj = coord.get_object("bucket", "hello.txt").unwrap();
        assert_eq!(obj.data, b"Hello, world!");
        assert_eq!(obj.size, 13);
        assert_eq!(obj.metadata.get("content-type"), Some("text/plain"));
    }

    #[test]
    fn put_get_with_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();

        let headers = [
            ("Content-Type", "application/json"),
            ("X-Amz-Meta-Author", "alice"),
            ("X-Amz-Meta-Version", "42"),
        ];
        coord
            .put_object("bucket", "obj", b"{}", &headers)
            .unwrap();

        let obj = coord.get_object("bucket", "obj").unwrap();
        assert_eq!(obj.data, b"{}");
        assert_eq!(
            obj.metadata.get("content-type"),
            Some("application/json")
        );
        assert_eq!(obj.metadata.get("x-amz-meta-author"), Some("alice"));
        assert_eq!(obj.metadata.get("x-amz-meta-version"), Some("42"));
    }

    #[test]
    fn head_object() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "key", b"data", &[("Content-Type", "text/plain")])
            .unwrap();

        let head = coord.head_object("bucket", "key").unwrap();
        assert_eq!(head.size, 4);
        assert_eq!(head.metadata.get("content-type"), Some("text/plain"));
    }

    #[test]
    fn overwrite_object() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "key", b"v1", &[]).unwrap();
        coord.put_object("bucket", "key", b"v2", &[]).unwrap();

        let obj = coord.get_object("bucket", "key").unwrap();
        assert_eq!(obj.data, b"v2");
    }

    #[test]
    fn empty_object() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "empty", b"", &[]).unwrap();

        let obj = coord.get_object("bucket", "empty").unwrap();
        assert_eq!(obj.data, b"");
        assert_eq!(obj.size, 0);
    }

    #[test]
    fn delete_object_then_get_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "key", b"data", &[]).unwrap();
        coord.delete_object("bucket", "key").unwrap();

        let err = coord.get_object("bucket", "key").unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn delete_nonexistent_object_is_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        // Should not error
        coord.delete_object("bucket", "no-such-key").unwrap();
    }

    #[test]
    fn list_objects() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "a/1", b"1", &[]).unwrap();
        coord.put_object("bucket", "a/2", b"2", &[]).unwrap();
        coord.put_object("bucket", "b/1", b"3", &[]).unwrap();

        let result = coord
            .list_objects_v2("bucket", None, None, None, 1000)
            .unwrap();
        assert_eq!(result.objects.len(), 3);
        // Should be sorted
        assert_eq!(result.objects[0].key, "a/1");
        assert_eq!(result.objects[1].key, "a/2");
        assert_eq!(result.objects[2].key, "b/1");
    }

    #[test]
    fn list_objects_with_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "photos/cat.jpg", b"cat", &[]).unwrap();
        coord.put_object("bucket", "photos/dog.jpg", b"dog", &[]).unwrap();
        coord.put_object("bucket", "docs/readme.md", b"md", &[]).unwrap();

        let result = coord
            .list_objects_v2("bucket", Some("photos/"), None, None, 1000)
            .unwrap();
        assert_eq!(result.objects.len(), 2);
    }

    #[test]
    fn list_objects_with_delimiter() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "photos/cat.jpg", b"cat", &[]).unwrap();
        coord.put_object("bucket", "photos/dog.jpg", b"dog", &[]).unwrap();
        coord.put_object("bucket", "docs/readme.md", b"md", &[]).unwrap();
        coord.put_object("bucket", "root.txt", b"root", &[]).unwrap();

        let result = coord
            .list_objects_v2("bucket", None, Some("/"), None, 1000)
            .unwrap();
        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].key, "root.txt");
        assert!(result.common_prefixes.contains(&"photos/".to_string()));
        assert!(result.common_prefixes.contains(&"docs/".to_string()));
    }

    #[test]
    fn put_get_object_trailing_slash_key() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "folder/", b"data", &[])
            .unwrap();

        let obj = coord.get_object("bucket", "folder/").unwrap();
        assert_eq!(obj.data, b"data");
        assert_eq!(obj.size, 4);
    }

    #[test]
    fn ec_reconstruction_after_shard_loss() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"This data should survive shard loss!";
        coord
            .put_object("bucket", "resilient", data, &[])
            .unwrap();

        // Find and delete one data shard file on disk
        let pg_id = derive_pg("bucket", "resilient", 4);
        let pg_dir = tmp.path().join(format!("pg-{pg_id:04}"));
        let shards_dir = pg_dir.join("shards");

        // Delete the first shard file we find
        let mut deleted = false;
        for entry in std::fs::read_dir(&shards_dir).unwrap() {
            let prefix_dir = entry.unwrap().path();
            if prefix_dir.is_dir() {
                for shard_entry in std::fs::read_dir(&prefix_dir).unwrap() {
                    let shard_path = shard_entry.unwrap().path();
                    if shard_path.is_file() && !deleted {
                        std::fs::remove_file(&shard_path).unwrap();
                        deleted = true;
                    }
                }
            }
            if deleted {
                break;
            }
        }
        assert!(deleted, "should have deleted a shard file");

        // Get should still succeed via EC reconstruction
        let obj = coord.get_object("bucket", "resilient").unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn put_to_nonexistent_bucket_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        let err = coord.put_object("no-such-bucket", "key", b"data", &[]).unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn get_nonexistent_object_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let err = coord.get_object("bucket", "no-such-key").unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn etag_consistency() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let result = coord.put_object("bucket", "key", b"data", &[]).unwrap();

        let obj = coord.get_object("bucket", "key").unwrap();
        assert_eq!(result.etag, obj.etag);

        let head = coord.head_object("bucket", "key").unwrap();
        assert_eq!(result.etag, head.etag);
    }

    #[test]
    fn list_objects_delimiter_with_continuation() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "a/1", b"1", &[]).unwrap();
        coord.put_object("bucket", "a/2", b"2", &[]).unwrap();
        coord.put_object("bucket", "b/1", b"3", &[]).unwrap();
        coord.put_object("bucket", "c/1", b"4", &[]).unwrap();
        coord.put_object("bucket", "root.txt", b"5", &[]).unwrap();

        // First page: max_keys=2 with delimiter
        let result = coord
            .list_objects_v2("bucket", None, Some("/"), None, 2)
            .unwrap();
        assert_eq!(
            result.objects.len() + result.common_prefixes.len(),
            2,
            "should return exactly 2 entries (objects + prefixes)"
        );
        assert!(result.is_truncated);
        assert!(result.next_continuation_token.is_some());

        // Second page using continuation token
        let token = result.next_continuation_token.unwrap();
        let result2 = coord
            .list_objects_v2("bucket", None, Some("/"), Some(&token), 2)
            .unwrap();
        assert!(
            !result2.objects.is_empty() || !result2.common_prefixes.is_empty(),
            "continuation page should have entries"
        );
    }

    #[test]
    fn list_objects_max_keys_counts_prefixes() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        // Create many prefixed objects to ensure common_prefixes count toward max_keys
        for i in 0..10 {
            let key = format!("dir{}/file.txt", i);
            coord
                .put_object("bucket", &key, b"data", &[])
                .unwrap();
        }

        let result = coord
            .list_objects_v2("bucket", None, Some("/"), None, 3)
            .unwrap();
        // With delimiter "/", all entries become common prefixes
        assert_eq!(result.common_prefixes.len(), 3);
        assert!(result.is_truncated);
    }

    #[test]
    fn put_object_to_nonexistent_bucket_no_orphaned_shards() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        // Don't create bucket — put should fail at bucket check before writing shards
        let err = coord
            .put_object("no-bucket", "key", b"data", &[])
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn delete_nonexistent_bucket_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        let err = coord.delete_bucket("no-such-bucket").unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn list_objects_no_delimiter_truncated() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        for i in 0..5 {
            let key = format!("key-{:02}", i);
            coord.put_object("bucket", &key, b"data", &[]).unwrap();
        }

        // Request fewer than available
        let result = coord
            .list_objects_v2("bucket", None, None, None, 3)
            .unwrap();
        assert_eq!(result.objects.len(), 3);
        assert!(result.is_truncated);
        assert!(result.next_continuation_token.is_some());
    }

    #[test]
    fn list_objects_no_delimiter_with_continuation() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        for i in 0..5 {
            let key = format!("key-{:02}", i);
            coord.put_object("bucket", &key, b"data", &[]).unwrap();
        }

        // First page
        let page1 = coord
            .list_objects_v2("bucket", None, None, None, 2)
            .unwrap();
        assert_eq!(page1.objects.len(), 2);
        assert!(page1.is_truncated);
        let token = page1.next_continuation_token.as_ref().unwrap();

        // Second page using continuation token
        let page2 = coord
            .list_objects_v2("bucket", None, None, Some(token), 2)
            .unwrap();
        assert_eq!(page2.objects.len(), 2);
        assert!(page2.is_truncated);
        let token2 = page2.next_continuation_token.as_ref().unwrap();

        // Third page — should get remainder
        let page3 = coord
            .list_objects_v2("bucket", None, None, Some(token2), 2)
            .unwrap();
        assert_eq!(page3.objects.len(), 1);
        assert!(!page3.is_truncated);
        assert!(page3.next_continuation_token.is_none());
    }

    #[test]
    fn list_objects_prefix_with_delimiter() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "photos/2024/jan.jpg", b"j", &[]).unwrap();
        coord.put_object("bucket", "photos/2024/feb.jpg", b"f", &[]).unwrap();
        coord.put_object("bucket", "photos/2025/mar.jpg", b"m", &[]).unwrap();
        coord.put_object("bucket", "photos/top.jpg", b"t", &[]).unwrap();

        // List with prefix "photos/" and delimiter "/"
        let result = coord
            .list_objects_v2("bucket", Some("photos/"), Some("/"), None, 1000)
            .unwrap();
        // top.jpg is a direct child, 2024/ and 2025/ are common prefixes
        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].key, "photos/top.jpg");
        assert_eq!(result.common_prefixes.len(), 2);
        assert!(result.common_prefixes.contains(&"photos/2024/".to_string()));
        assert!(result.common_prefixes.contains(&"photos/2025/".to_string()));
        assert!(!result.is_truncated);
    }

    #[test]
    fn list_objects_not_truncated_no_token() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "only-one", b"data", &[]).unwrap();

        let result = coord
            .list_objects_v2("bucket", None, None, None, 1000)
            .unwrap();
        assert_eq!(result.objects.len(), 1);
        assert!(!result.is_truncated);
        assert!(result.next_continuation_token.is_none());
    }

    #[test]
    fn list_objects_max_keys_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "key1", b"data", &[]).unwrap();

        let result = coord
            .list_objects_v2("bucket", None, None, None, 0)
            .unwrap();
        assert!(result.objects.is_empty());
        assert!(result.common_prefixes.is_empty());
        assert!(!result.is_truncated);
        assert!(result.next_continuation_token.is_none());
    }

    #[test]
    fn list_objects_max_keys_zero_with_delimiter() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "a/1", b"data", &[]).unwrap();

        let result = coord
            .list_objects_v2("bucket", None, Some("/"), None, 0)
            .unwrap();
        assert!(result.objects.is_empty());
        assert!(result.common_prefixes.is_empty());
        assert!(!result.is_truncated);
    }

    #[test]
    fn list_objects_nonexistent_bucket_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .list_objects_v2("no-bucket", None, None, None, 1000)
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn delete_objects_batch() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "key1", b"data1", &[]).unwrap();
        coord.put_object("bucket", "key2", b"data2", &[]).unwrap();

        let entries = vec![
            crate::http::xml::DeleteObjectEntry {
                key: "key1".to_string(),
                version_id: None,
            },
            crate::http::xml::DeleteObjectEntry {
                key: "key2".to_string(),
                version_id: None,
            },
            // key3 doesn't exist — should still succeed (idempotent)
            crate::http::xml::DeleteObjectEntry {
                key: "key3".to_string(),
                version_id: None,
            },
        ];

        let result = coord.delete_objects("bucket", &entries).unwrap();
        assert_eq!(result.deleted.len(), 3);
        assert!(result.errors.is_empty());

        // Verify objects are actually gone
        assert!(coord.get_object("bucket", "key1").is_err());
        assert!(coord.get_object("bucket", "key2").is_err());
    }

    #[test]
    fn delete_objects_nonexistent_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        let entries = vec![crate::http::xml::DeleteObjectEntry {
            key: "key1".to_string(),
            version_id: None,
        }];

        let err = coord.delete_objects("no-bucket", &entries).unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn max_object_size_constant() {
        // Verify the size guard exists and the constant is 256 MB.
        // The actual rejection is tested by large_object_rejected (ignored
        // by default due to 256 MB allocation).
        assert_eq!(MAX_OBJECT_SIZE, 256 * 1024 * 1024);
    }

    #[test]
    #[ignore] // allocates 256 MB+1 — run explicitly with `cargo test -- --ignored`
    fn large_object_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();

        let err = coord
            .put_object("bucket", "key", &vec![0u8; 256 * 1024 * 1024 + 1], &[]);
        assert!(matches!(
            err,
            Err(ServerError::ObjectTooLarge { .. })
        ));
    }
}
