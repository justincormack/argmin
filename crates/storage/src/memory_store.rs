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
pub struct MemoryPgStore {
    shards: RefCell<HashMap<ShardKey, ShardRecord>>,
    objects: RefCell<HashMap<(String, String), ObjectRecord>>,
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
        let crc = crc64::checksum(data);
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
        let actual_crc = crc64::checksum(&record.data);
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
            version_id: req.version_id.clone(),
            size: req.size,
            total_size: req.total_size,
            etag: req.etag.clone(),
            etag_kind: req.etag_kind,
            last_modified: now,
            storage_class: 0,
            ec_k: req.ec_k,
            ec_m: req.ec_m,
            status: 0,
        };
        self.objects
            .borrow_mut()
            .insert((req.bucket.clone(), req.key.clone()), record);
        Ok(())
    }

    fn get_object_meta(&self, bucket: &str, key: &str) -> Result<ObjectRecord, MetadataError> {
        let objects = self.objects.borrow();
        objects
            .get(&(bucket.to_string(), key.to_string()))
            .filter(|r| r.status == 0)
            .cloned()
            .ok_or(MetadataError::ObjectNotFound)
    }

    fn delete_object_meta(&self, bucket: &str, key: &str) -> Result<(), MetadataError> {
        self.objects
            .borrow_mut()
            .remove(&(bucket.to_string(), key.to_string()));
        Ok(())
    }

    fn list_objects(&self, req: &ListObjectsReq) -> Result<ListObjectsResp, MetadataError> {
        let objects = self.objects.borrow();
        let mut matching: Vec<&ObjectRecord> = objects
            .values()
            .filter(|o| {
                if o.bucket != req.bucket || o.status != 0 {
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
}
