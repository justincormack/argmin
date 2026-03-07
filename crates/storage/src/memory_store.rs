use std::cell::RefCell;
/// In-memory PG store for testing. No disk I/O.
///
/// Implements both `ShardStore` and `PgMetadataStore` backed by `HashMap`s.
/// Still computes and verifies CRC64 for correctness testing.
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{MetadataError, StoreError};
use crate::traits::{PgMetadataStore, ShardStore};
use crate::types::*;

struct ShardRecord {
    data: Vec<u8>,
    crc64: u64,
    created_at: u64,
    last_verified: Option<u64>,
}

/// In-memory shard and metadata store for testing.
///
/// Objects are keyed by (bucket, key, version_id) to support versioning.
pub struct MemoryPgStore {
    shards: RefCell<HashMap<ShardKey, ShardRecord>>,
    objects: RefCell<HashMap<(String, String, u64), ObjectRecord>>,
}

impl MemoryPgStore {
    pub fn new() -> Self {
        Self {
            shards: RefCell::new(HashMap::new()),
            objects: RefCell::new(HashMap::new()),
        }
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn now_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    /// Corrupt stored shard data without updating CRC.
    /// Subsequent read_shard will detect CRC mismatch → IntegrityError.
    /// Uses a simple LCG seeded by `seed` to pick `flip_count` byte positions to XOR.
    /// Returns false if the shard doesn't exist.
    #[cfg(test)]
    pub fn corrupt_shard_data(&self, key: &ShardKey, flip_count: u8, seed: u64) -> bool {
        let mut shards = self.shards.borrow_mut();
        let Some(record) = shards.get_mut(key) else {
            return false;
        };
        if record.data.is_empty() || flip_count == 0 {
            return true;
        }
        // Simple LCG: state = state * 6364136223846793005 + 1
        let mut state = seed;
        for _ in 0..flip_count {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let pos = (state >> 33) as usize % record.data.len();
            record.data[pos] ^= 0xFF;
        }
        true
    }

    /// Remove a shard directly (bypass delete_shard trait method).
    /// Returns false if the shard didn't exist.
    #[cfg(test)]
    pub fn remove_shard(&self, key: &ShardKey) -> bool {
        self.shards.borrow_mut().remove(key).is_some()
    }
}

impl Default for MemoryPgStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardStore for MemoryPgStore {
    fn write_shard(&self, key: &ShardKey, data: &[u8]) -> Result<WriteAck, StoreError> {
        let crc = checksum::crc64::checksum(data);
        let stored_size = data.len() as u64;

        self.shards.borrow_mut().insert(
            key.clone(),
            ShardRecord {
                data: data.to_vec(),
                crc64: crc,
                created_at: Self::now_secs(),
                last_verified: None,
            },
        );

        Ok(WriteAck {
            crc64: crc,
            stored_size,
        })
    }

    fn read_shard(&self, key: &ShardKey) -> Result<ShardData, StoreError> {
        let shards = self.shards.borrow();
        let record = shards.get(key).ok_or(StoreError::NotFound)?;

        // Verify CRC even in memory (catches logic errors in tests).
        let actual_crc = checksum::crc64::checksum(&record.data);
        if actual_crc != record.crc64 {
            return Err(StoreError::IntegrityError {
                expected: record.crc64,
                actual: actual_crc,
            });
        }

        Ok(ShardData {
            data: record.data.clone(),
            crc64: actual_crc,
        })
    }

    fn delete_shard(&self, key: &ShardKey) -> Result<(), StoreError> {
        self.shards.borrow_mut().remove(key);
        Ok(())
    }

    fn stat_shard(&self, key: &ShardKey) -> Result<ShardStat, StoreError> {
        let shards = self.shards.borrow();
        let record = shards.get(key).ok_or(StoreError::NotFound)?;

        Ok(ShardStat {
            size: record.data.len() as u64,
            crc64: record.crc64,
            created_at: record.created_at,
            last_verified: record.last_verified,
        })
    }
}

impl PgMetadataStore for MemoryPgStore {
    fn put_object_meta(&self, req: &PutObjectMetaReq) -> Result<(), MetadataError> {
        let now = Self::now_millis();
        let record = ObjectRecord {
            bucket: req.bucket.clone(),
            key: req.key.clone(),
            version_id: req.version_id,
            size: req.size,
            total_size: req.total_size,
            etag: req.etag.clone(),
            etag_kind: req.etag_kind,
            last_modified: now,
            storage_class: 0,
            ec_k: req.ec_k,
            ec_m: req.ec_m,
            status: req.status,
            tags: None,
            data_layout: req.data_layout.unwrap_or(DataLayout::ChunkManifestInternal),
            parts_count: req.parts_count,
            metadata_blob: req.metadata_blob.clone(),
        };
        self.objects.borrow_mut().insert(
            (req.bucket.clone(), req.key.clone(), req.version_id),
            record,
        );
        Ok(())
    }

    fn get_object_meta(&self, bucket: &str, key: &str) -> Result<ObjectRecord, MetadataError> {
        let objects = self.objects.borrow();
        // Find the latest version (highest version_id) for this bucket/key
        objects
            .values()
            .filter(|r| r.bucket == bucket && r.key == key)
            .max_by_key(|r| (r.last_modified, r.version_id))
            .cloned()
            .ok_or(MetadataError::ObjectNotFound)
    }

    fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
    ) -> Result<ObjectRecord, MetadataError> {
        let objects = self.objects.borrow();
        objects
            .get(&(bucket.to_string(), key.to_string(), version_id))
            .cloned()
            .ok_or(MetadataError::ObjectNotFound)
    }

    fn delete_object_meta(&self, bucket: &str, key: &str) -> Result<(), MetadataError> {
        let mut objects = self.objects.borrow_mut();
        objects.retain(|k, _| !(k.0 == bucket && k.1 == key));
        Ok(())
    }

    fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
    ) -> Result<(), MetadataError> {
        self.objects
            .borrow_mut()
            .remove(&(bucket.to_string(), key.to_string(), version_id));
        Ok(())
    }

    fn list_objects(&self, req: &ListObjectsReq) -> Result<ListObjectsResp, MetadataError> {
        let objects = self.objects.borrow();

        // Group by (bucket, key) and find latest version per key
        let mut latest_by_key: HashMap<(&str, &str), &ObjectRecord> = HashMap::new();
        for record in objects.values() {
            if record.bucket != req.bucket {
                continue;
            }
            let entry = latest_by_key
                .entry((&record.bucket, &record.key))
                .or_insert(record);
            if record.version_id > entry.version_id {
                *entry = record;
            }
        }

        // Filter: only latest live versions, apply prefix/start_after
        let mut matching: Vec<&ObjectRecord> = latest_by_key
            .values()
            .filter(|o| {
                if o.status != 0 {
                    return false;
                }
                if let Some(ref prefix) = req.prefix {
                    if !o.key.starts_with(prefix) {
                        return false;
                    }
                }
                if let Some(ref start_after) = req.start_after {
                    if o.key.as_str() <= start_after.as_str() {
                        return false;
                    }
                }
                true
            })
            .copied()
            .collect();

        matching.sort_by(|a, b| a.key.cmp(&b.key));

        let is_truncated = matching.len() > req.max_keys as usize;
        let result: Vec<ObjectRecord> = matching
            .into_iter()
            .take(req.max_keys as usize)
            .cloned()
            .collect();

        let next_start_after = if is_truncated {
            result.last().map(|o| o.key.clone())
        } else {
            None
        };

        Ok(ListObjectsResp {
            objects: result,
            is_truncated,
            next_start_after,
        })
    }

    fn list_object_versions(
        &self,
        req: &ListObjectVersionsReq,
    ) -> Result<ListObjectVersionsResp, MetadataError> {
        let objects = self.objects.borrow();
        let mut matching: Vec<&ObjectRecord> = objects
            .values()
            .filter(|o| {
                if o.bucket != req.bucket {
                    return false;
                }
                if let Some(ref prefix) = req.prefix {
                    if !o.key.starts_with(prefix) {
                        return false;
                    }
                }
                if let Some(ref key_marker) = req.key_marker {
                    if o.key.as_str() < key_marker.as_str() {
                        return false;
                    }
                    if o.key.as_str() == key_marker.as_str() {
                        if let Some(vid_marker) = req.version_id_marker {
                            if o.version_id >= vid_marker {
                                return false;
                            }
                        } else {
                            return false;
                        }
                    }
                }
                true
            })
            .collect();

        matching.sort_by(|a, b| a.key.cmp(&b.key).then(b.version_id.cmp(&a.version_id)));

        let is_truncated = matching.len() > req.max_keys as usize;
        let result: Vec<ObjectRecord> = matching
            .into_iter()
            .take(req.max_keys as usize)
            .cloned()
            .collect();

        let (next_key_marker, next_version_id_marker) = if is_truncated {
            result
                .last()
                .map(|o| (Some(o.key.clone()), Some(o.version_id)))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };

        Ok(ListObjectVersionsResp {
            versions: result,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
        })
    }

    fn next_version_id(&self, bucket: &str, key: &str) -> Result<u64, MetadataError> {
        let objects = self.objects.borrow();
        let max = objects
            .values()
            .filter(|r| r.bucket == bucket && r.key == key)
            .map(|r| r.version_id)
            .max();
        Ok(max.map(|v| v + 1).unwrap_or(1))
    }

    fn put_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
        tags: &str,
    ) -> Result<(), MetadataError> {
        let mut objects = self.objects.borrow_mut();
        let k = (bucket.to_string(), key.to_string(), version_id);
        match objects.get_mut(&k) {
            Some(record) => {
                record.tags = Some(tags.to_string());
                Ok(())
            }
            None => Err(MetadataError::ObjectNotFound),
        }
    }

    fn get_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
    ) -> Result<Option<String>, MetadataError> {
        let objects = self.objects.borrow();
        let k = (bucket.to_string(), key.to_string(), version_id);
        match objects.get(&k) {
            Some(record) => Ok(record.tags.clone()),
            None => Err(MetadataError::ObjectNotFound),
        }
    }

    fn delete_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: u64,
    ) -> Result<(), MetadataError> {
        let mut objects = self.objects.borrow_mut();
        let k = (bucket.to_string(), key.to_string(), version_id);
        match objects.get_mut(&k) {
            Some(record) => {
                record.tags = None;
                Ok(())
            }
            None => Err(MetadataError::ObjectNotFound),
        }
    }

    // ── Multipart upload methods (stubs — implemented in Step 3) ──

    fn create_multipart_upload(
        &self,
        _req: &CreateMultipartUploadReq,
    ) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn get_multipart_upload(
        &self,
        _upload_id: &str,
    ) -> Result<MultipartUploadRecord, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn set_upload_state(
        &self,
        _upload_id: &str,
        _new_state: UploadState,
    ) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn delete_multipart_upload(&self, _upload_id: &str) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn list_multipart_uploads(
        &self,
        _req: &ListMultipartUploadsReq,
    ) -> Result<ListMultipartUploadsResp, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn upsert_multipart_part(
        &self,
        _part: &MultipartPartRecord,
    ) -> Result<Option<u32>, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn get_multipart_part(
        &self,
        _upload_id: &str,
        _part_number: u32,
    ) -> Result<MultipartPartRecord, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn list_multipart_parts(&self, _req: &ListPartsReq) -> Result<ListPartsResp, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn commit_object_parts(&self, _parts: &[ObjectPartRecord]) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn get_object_parts(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: u64,
    ) -> Result<Vec<ObjectPartRecord>, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn delete_object_parts(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: u64,
    ) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    fn complete_multipart_commit(
        &self,
        _upload_id: &str,
        _obj: &PutObjectMetaReq,
        _parts: &[ObjectPartRecord],
    ) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "multipart metadata (pending Step 3)",
        })
    }

    // ── Streaming upload session methods (stubs) ─────────────────────

    fn create_stream_upload(&self, _req: &CreateStreamUploadReq) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }

    fn get_stream_upload(&self, _session_id: &str) -> Result<StreamUploadRecord, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }

    fn set_stream_upload_state(
        &self,
        _session_id: &str,
        _new_state: StreamUploadState,
    ) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }

    fn delete_stream_upload(&self, _session_id: &str) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }

    fn list_all_stream_uploads(&self) -> Result<Vec<StreamUploadRecord>, MetadataError> {
        Ok(vec![])
    }

    fn append_stream_chunk(&self, _chunk: &StreamUploadChunkRecord) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }

    fn list_stream_chunks(
        &self,
        _session_id: &str,
    ) -> Result<Vec<StreamUploadChunkRecord>, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }

    fn commit_stream_put(
        &self,
        _session_id: &str,
        _obj: &PutObjectMetaReq,
        _chunks: &[StreamObjectChunkRecord],
    ) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }

    fn commit_stream_part(
        &self,
        _session_id: &str,
        _part: &MultipartPartRecord,
        _chunks: &[MultipartPartChunkRecord],
    ) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }

    fn get_stream_object_chunks(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: u64,
    ) -> Result<Vec<StreamObjectChunkRecord>, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }

    fn delete_stream_object_chunks(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: u64,
    ) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }

    fn get_multipart_part_chunks(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: u64,
        _part_number: u32,
    ) -> Result<Vec<MultipartPartChunkRecord>, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }

    fn delete_multipart_part_chunks(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: u64,
    ) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }

    fn get_all_multipart_part_chunks_for_upload(
        &self,
        _upload_id: &str,
    ) -> Result<Vec<MultipartPartChunkRecord>, MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }

    fn delete_multipart_part_chunks_by_upload_id(
        &self,
        _upload_id: &str,
    ) -> Result<(), MetadataError> {
        Err(MetadataError::NotImplemented {
            context: "streaming uploads (pending Phase 2)",
        })
    }
}
