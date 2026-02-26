/// Coordinator: orchestrates S3 operations across EC, storage, and metadata layers.
use ec::{EcConfig, ErasureCodec};
use storage::traits::{GlobalService, PgMetadataStore, ShardStore, StorageNode};
use storage::{
    BucketInfo, ListObjectsReq, LocalStorageNode, ObjectRecord, PutObjectMetaReq,
    ShardKey, SqliteBucketDb,
};

use crate::conditional::{
    check_delete_conditions, check_read_conditions, check_write_conditions, DeleteCondition,
    ReadCondition, WriteCondition,
};
use crate::error::ServerError;
use crate::etag::{crc64_to_etag_bytes, etag_bytes_to_crc64, format_etag};
use crate::metadata_blob::MetadataBlob;
use crate::pg::{derive_pg, object_key_hash};
use crate::range::ByteRange;

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

/// Result of a range GetObject operation (206 Partial Content).
#[derive(Debug)]
pub struct GetObjectRangeResult {
    pub data: Vec<u8>,
    pub metadata: MetadataBlob,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub range_start: u64,
    pub range_end: u64,
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
        cond: &WriteCondition,
    ) -> Result<PutObjectResult, ServerError> {
        if data.len() as u64 > MAX_OBJECT_SIZE {
            return Err(ServerError::ObjectTooLarge {
                size: data.len() as u64,
                max: MAX_OBJECT_SIZE,
            });
        }

        // 1. Verify bucket exists
        self.head_bucket(bucket)?;

        // 1b. Check write conditions if any are set
        if !cond.is_empty() {
            let pg_id = derive_pg(bucket, key, self.pg_count);
            let pg = self.storage_node.get_pg(pg_id)?;
            let existing_etag = match pg.get_object_meta(bucket, key) {
                Ok(record) => {
                    let crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
                    Some(format_etag(crc))
                }
                Err(storage::MetadataError::ObjectNotFound) => None,
                Err(e) => return Err(ServerError::Metadata(e)),
            };
            check_write_conditions(cond, existing_etag.as_deref())?;
        }

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
            total_size: (blob_bytes.len() + data.len()) as u64,
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

    /// Read specific data shard indices from a PG, falling back to EC reconstruction
    /// if any are missing. Always reads whole shards — each shard is CRC64-verified
    /// by the underlying `read_shard()` call.
    ///
    /// Returns (shard_data_vec, shard_size) where shard_data_vec contains one Vec<u8>
    /// per requested index in `needed`, in the same order.
    fn read_data_shards(
        &self,
        pg: &storage::PgStore,
        okh: &[u8; 16],
        version_id: u64,
        record: &ObjectRecord,
        needed: &[usize],
    ) -> Result<(Vec<Vec<u8>>, usize), ServerError> {
        let k = record.ec_k as usize;
        let m = record.ec_m as usize;

        // Try reading just the needed shards first
        let mut result_shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(needed.len());
        let mut all_present = true;
        let mut shard_size = 0;

        for &idx in needed {
            let shard_key = ShardKey::new(okh, version_id, idx as u8);
            match pg.read_shard(&shard_key) {
                Ok(sd) => {
                    shard_size = sd.data.len();
                    result_shards.push(Some(sd.data));
                }
                Err(_) => {
                    all_present = false;
                    result_shards.push(None);
                }
            }
        }

        // Happy path: all needed shards present
        if all_present {
            let shards: Vec<Vec<u8>> = result_shards.into_iter().map(|s| s.unwrap()).collect();
            if shards.is_empty() {
                return Ok((shards, 0));
            }
            return Ok((shards, shard_size));
        }

        // Fallback: read all k+m shards for EC reconstruction
        let mut all_shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(k + m);
        let mut present_count = 0;

        for i in 0..(k + m) {
            let shard_key = ShardKey::new(okh, version_id, i as u8);
            match pg.read_shard(&shard_key) {
                Ok(sd) => {
                    shard_size = sd.data.len();
                    all_shards.push(Some(sd.data));
                    present_count += 1;
                }
                Err(_) => {
                    all_shards.push(None);
                }
            }
        }

        if present_count < k {
            return Err(ServerError::Store(storage::StoreError::NotFound));
        }

        // Find which of the needed data shards are missing
        let missing_needed: Vec<usize> = needed
            .iter()
            .copied()
            .filter(|&i| all_shards[i].is_none())
            .collect();

        if !missing_needed.is_empty() {
            let present_indices: Vec<usize> = (0..(k + m))
                .filter(|&i| all_shards[i].is_some())
                .collect();
            let present_refs: Vec<&[u8]> = present_indices
                .iter()
                .map(|&i| all_shards[i].as_ref().unwrap().as_slice())
                .collect();

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
                missing_needed.iter().map(|_| vec![0u8; shard_size]).collect();
            let mut output_refs: Vec<&mut [u8]> =
                outputs.iter_mut().map(|v| v.as_mut_slice()).collect();

            codec.reconstruct(
                &present_indices,
                &present_refs,
                &missing_needed,
                &mut output_refs,
            )?;

            for (idx, &missing_idx) in missing_needed.iter().enumerate() {
                all_shards[missing_idx] = Some(outputs[idx].clone());
            }
        }

        // Extract just the needed shards in order
        let shards: Vec<Vec<u8>> = needed
            .iter()
            .map(|&i| all_shards[i].take().unwrap())
            .collect();

        Ok((shards, shard_size))
    }

    /// Compute shard_size from total stored size and EC k.
    ///
    /// `total_size` is the pre-padding size (metadata + user data).
    /// Returns the per-shard size after padding to a multiple of k.
    fn compute_shard_size(total_size: u64, ec_k: u8) -> usize {
        let k = ec_k as u64;
        let padded = total_size.div_ceil(k) * k;
        (padded / k) as usize
    }

    /// Compute data shard indices covering byte range [start, end] (inclusive) in the stored blob.
    fn shards_for_byte_range(start: usize, end: usize, shard_size: usize, ec_k: u8) -> Vec<usize> {
        if shard_size == 0 {
            return vec![];
        }
        let first = start / shard_size;
        let last = (end / shard_size).min(ec_k as usize - 1);
        (first..=last).collect()
    }

    /// Read a byte range [start, end] (inclusive) from the stored blob (metadata + user data).
    ///
    /// Returns the requested bytes. Reads only the shards covering the range,
    /// falling back to EC reconstruction if any are missing.
    ///
    /// **Integrity note:** Each shard is CRC64-verified on read by `read_shard()`.
    /// There are no sub-shard checksums, so we must always read *whole* shards
    /// and discard bytes outside the requested range after verification. This
    /// means range requests that don't align to shard boundaries read more data
    /// than strictly necessary — this is unavoidable without finer-grained checksums.
    fn read_range(
        &self,
        pg: &storage::PgStore,
        okh: &[u8; 16],
        version_id: u64,
        record: &ObjectRecord,
        start: usize,
        end: usize,
    ) -> Result<Vec<u8>, ServerError> {
        let shard_size = Self::compute_shard_size(record.total_size, record.ec_k);
        if shard_size == 0 {
            return Ok(vec![]);
        }

        let needed = Self::shards_for_byte_range(start, end, shard_size, record.ec_k);
        if needed.is_empty() {
            return Ok(vec![]);
        }

        let (shard_data, _) = self.read_data_shards(pg, okh, version_id, record, &needed)?;

        // Assemble the buffer covering the needed shards
        let first_shard = needed[0];
        let buf_start = first_shard * shard_size;
        let mut buf = Vec::with_capacity(shard_data.len() * shard_size);
        for shard in &shard_data {
            buf.extend_from_slice(shard);
        }

        // Extract the requested range from the buffer
        let local_start = start - buf_start;
        let local_end = (end - buf_start).min(buf.len() - 1);
        Ok(buf[local_start..=local_end].to_vec())
    }

    /// Get an object from storage.
    pub fn get_object(
        &self,
        bucket: &str,
        key: &str,
        cond: &ReadCondition,
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
        let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);

        // Check conditions before reading shard data
        let etag_str = format_etag(etag_crc);
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        if record.total_size > 0 {
            // New path: use read_range to read only needed data shards
            let total = record.total_size as usize;
            let data = self
                .read_range(pg, &okh, version_id, &record, 0, total - 1)
                .map_err(|e| match e {
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        }
                    }
                    other => other,
                })?;

            let metadata_size = (record.total_size - record.size) as usize;
            let (metadata, _) = MetadataBlob::deserialize(&data[..metadata_size])?;
            let user_data = data[metadata_size..].to_vec();

            return Ok(GetObjectResult {
                data: user_data,
                metadata,
                etag: format_etag(etag_crc),
                size: record.size,
                last_modified: record.last_modified,
            });
        }

        // Legacy path: total_size == 0, read all k data shards
        let k = record.ec_k as usize;
        let all_data_indices: Vec<usize> = (0..k).collect();
        let (shard_data, shard_size) =
            self.read_data_shards(pg, &okh, version_id, &record, &all_data_indices)
                .map_err(|e| match e {
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        }
                    }
                    other => other,
                })?;

        let mut full_padded_data = Vec::with_capacity(k * shard_size);
        for shard in &shard_data {
            full_padded_data.extend_from_slice(shard);
        }

        let (metadata, blob_len) = MetadataBlob::deserialize(&full_padded_data)?;
        let user_data_end = blob_len + record.size as usize;
        if user_data_end > full_padded_data.len() {
            return Err(ServerError::MetadataBlobError {
                reason: "data shorter than expected".to_string(),
            });
        }
        let user_data = full_padded_data[blob_len..user_data_end].to_vec();

        Ok(GetObjectResult {
            data: user_data,
            metadata,
            etag: format_etag(etag_crc),
            size: record.size,
            last_modified: record.last_modified,
        })
    }

    /// Head object: returns metadata without body.
    ///
    /// When total_size is known, reads only the shards covering the metadata blob.
    /// Falls back to shard-0-first approach for legacy objects (total_size == 0).
    pub fn head_object(
        &self,
        bucket: &str,
        key: &str,
        cond: &ReadCondition,
    ) -> Result<HeadObjectResult, ServerError> {
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
        let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);

        // Check conditions before reading shard data
        let etag_str = format_etag(etag_crc);
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        if record.total_size > 0 {
            // New path: read only the metadata portion
            let metadata_size = (record.total_size - record.size) as usize;
            let data = self
                .read_range(pg, &okh, version_id, &record, 0, metadata_size - 1)
                .map_err(|e| match e {
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        }
                    }
                    other => other,
                })?;

            let (metadata, _) = MetadataBlob::deserialize(&data)?;
            return Ok(HeadObjectResult {
                metadata,
                etag: format_etag(etag_crc),
                size: record.size,
                last_modified: record.last_modified,
            });
        }

        // Legacy path: total_size == 0, read shard 0 first
        let (shards, shard_size) =
            self.read_data_shards(pg, &okh, version_id, &record, &[0])
                .map_err(|e| match e {
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        }
                    }
                    other => other,
                })?;

        let shard0 = &shards[0];
        let k = record.ec_k as usize;

        let need_more = if shard_size < 4 {
            true
        } else {
            let blob_len =
                u32::from_le_bytes([shard0[0], shard0[1], shard0[2], shard0[3]]) as usize;
            blob_len > shard_size
        };

        if !need_more {
            let (metadata, _) = MetadataBlob::deserialize(shard0)?;
            return Ok(HeadObjectResult {
                metadata,
                etag: format_etag(etag_crc),
                size: record.size,
                last_modified: record.last_modified,
            });
        }

        let all_data_indices: Vec<usize> = (0..k).collect();
        let (shards, _) =
            self.read_data_shards(pg, &okh, version_id, &record, &all_data_indices)
                .map_err(|e| match e {
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        }
                    }
                    other => other,
                })?;

        let mut combined = Vec::with_capacity(k * shard_size);
        for shard in &shards {
            combined.extend_from_slice(shard);
        }

        let (metadata, _) = MetadataBlob::deserialize(&combined)?;
        Ok(HeadObjectResult {
            metadata,
            etag: format_etag(etag_crc),
            size: record.size,
            last_modified: record.last_modified,
        })
    }

    /// Get a byte range of an object from storage (for HTTP Range requests).
    ///
    /// Returns 206 Partial Content data. Requires total_size to be set.
    pub fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        range: ByteRange,
        cond: &ReadCondition,
    ) -> Result<GetObjectRangeResult, ServerError> {
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
        let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);

        // Check conditions before reading shard data
        let etag_str = format_etag(etag_crc);
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        // Resolve byte range against user data size
        let (user_start, user_end) = range
            .resolve(record.size)
            .ok_or(ServerError::InvalidRange {
                total_size: record.size,
            })?;

        if record.total_size > 0 {
            let metadata_size = (record.total_size - record.size) as usize;

            // Read metadata (always need it for response headers)
            let meta_data = self
                .read_range(pg, &okh, version_id, &record, 0, metadata_size - 1)
                .map_err(|e| match e {
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        }
                    }
                    other => other,
                })?;
            let (metadata, _) = MetadataBlob::deserialize(&meta_data)?;

            // Read user data range
            let blob_start = metadata_size + user_start as usize;
            let blob_end = metadata_size + user_end as usize;
            let user_data = self
                .read_range(pg, &okh, version_id, &record, blob_start, blob_end)
                .map_err(|e| match e {
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        }
                    }
                    other => other,
                })?;

            return Ok(GetObjectRangeResult {
                data: user_data,
                metadata,
                etag: format_etag(etag_crc),
                size: record.size,
                last_modified: record.last_modified,
                range_start: user_start,
                range_end: user_end,
            });
        }

        // Legacy path: total_size == 0, fall back to full read
        let k = record.ec_k as usize;
        let all_data_indices: Vec<usize> = (0..k).collect();
        let (shard_data, shard_size) =
            self.read_data_shards(pg, &okh, version_id, &record, &all_data_indices)
                .map_err(|e| match e {
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        }
                    }
                    other => other,
                })?;

        let mut full_padded_data = Vec::with_capacity(k * shard_size);
        for shard in &shard_data {
            full_padded_data.extend_from_slice(shard);
        }

        let (metadata, blob_len) = MetadataBlob::deserialize(&full_padded_data)?;
        let data_start = blob_len + user_start as usize;
        let data_end = blob_len + user_end as usize;
        if data_end >= full_padded_data.len() {
            return Err(ServerError::MetadataBlobError {
                reason: "data shorter than expected".to_string(),
            });
        }
        let user_data = full_padded_data[data_start..=data_end].to_vec();

        Ok(GetObjectRangeResult {
            data: user_data,
            metadata,
            etag: format_etag(etag_crc),
            size: record.size,
            last_modified: record.last_modified,
            range_start: user_start,
            range_end: user_end,
        })
    }

    /// Delete an object.
    pub fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        cond: &DeleteCondition,
    ) -> Result<(), ServerError> {
        let pg_id = derive_pg(bucket, key, self.pg_count);
        let pg = self.storage_node.get_pg(pg_id)?;

        // Look up the record to get EC params
        let record = match pg.get_object_meta(bucket, key) {
            Ok(r) => r,
            Err(storage::MetadataError::ObjectNotFound) => {
                if !cond.is_empty() {
                    return Err(ServerError::PreconditionFailed);
                }
                return Ok(()); // idempotent
            }
            Err(e) => return Err(ServerError::Metadata(e)),
        };

        // Check delete conditions
        if !cond.is_empty() {
            let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
            let etag_str = format_etag(etag_crc);
            check_delete_conditions(cond, &etag_str)?;
        }

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
        cond: &DeleteCondition,
    ) -> Result<DeleteObjectsResult, ServerError> {
        self.head_bucket(bucket)?;

        let mut deleted = Vec::new();
        let mut errors = Vec::new();

        for entry in entries {
            match self.delete_object(bucket, &entry.key, cond) {
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
    use crate::conditional::{DeleteCondition, ReadCondition, WriteCondition};
    use std::path::{Path, PathBuf};

    const NO_READ: &ReadCondition = &ReadCondition {
        if_match: None,
        if_none_match: None,
        if_modified_since: None,
        if_unmodified_since: None,
    };
    const NO_WRITE: &WriteCondition = &WriteCondition {
        if_match: None,
        if_none_match_any: false,
    };
    const NO_DELETE: &DeleteCondition = &DeleteCondition { if_match: None };

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
            .put_object("bucket", "key", b"data", &[], NO_WRITE)
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
            .put_object("bucket", "hello.txt", b"Hello, world!", &headers, NO_WRITE)
            .unwrap();
        assert!(!result.etag.is_empty());

        let obj = coord.get_object("bucket", "hello.txt", NO_READ).unwrap();
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
            .put_object("bucket", "obj", b"{}", &headers, NO_WRITE)
            .unwrap();

        let obj = coord.get_object("bucket", "obj", NO_READ).unwrap();
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
            .put_object("bucket", "key", b"data", &[("Content-Type", "text/plain")], NO_WRITE)
            .unwrap();

        let head = coord.head_object("bucket", "key", NO_READ).unwrap();
        assert_eq!(head.size, 4);
        assert_eq!(head.metadata.get("content-type"), Some("text/plain"));
    }

    #[test]
    fn overwrite_object() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "key", b"v1", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "key", b"v2", &[], NO_WRITE).unwrap();

        let obj = coord.get_object("bucket", "key", NO_READ).unwrap();
        assert_eq!(obj.data, b"v2");
    }

    #[test]
    fn empty_object() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "empty", b"", &[], NO_WRITE).unwrap();

        let obj = coord.get_object("bucket", "empty", NO_READ).unwrap();
        assert_eq!(obj.data, b"");
        assert_eq!(obj.size, 0);
    }

    #[test]
    fn delete_object_then_get_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "key", b"data", &[], NO_WRITE).unwrap();
        coord.delete_object("bucket", "key", NO_DELETE).unwrap();

        let err = coord.get_object("bucket", "key", NO_READ).unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn delete_nonexistent_object_is_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        // Should not error
        coord.delete_object("bucket", "no-such-key", NO_DELETE).unwrap();
    }

    #[test]
    fn list_objects() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "a/1", b"1", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "a/2", b"2", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "b/1", b"3", &[], NO_WRITE).unwrap();

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
        coord.put_object("bucket", "photos/cat.jpg", b"cat", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "photos/dog.jpg", b"dog", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "docs/readme.md", b"md", &[], NO_WRITE).unwrap();

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
        coord.put_object("bucket", "photos/cat.jpg", b"cat", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "photos/dog.jpg", b"dog", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "docs/readme.md", b"md", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "root.txt", b"root", &[], NO_WRITE).unwrap();

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
            .put_object("bucket", "folder/", b"data", &[], NO_WRITE)
            .unwrap();

        let obj = coord.get_object("bucket", "folder/", NO_READ).unwrap();
        assert_eq!(obj.data, b"data");
        assert_eq!(obj.size, 4);
    }

    // ── Disk manipulation helpers for EC tests ────────────────────────

    /// Compute shard file path on disk for a given object and shard index.
    fn shard_file_path(
        data_dir: &Path,
        bucket: &str,
        key: &str,
        shard_index: u8,
        pg_count: u32,
    ) -> PathBuf {
        let pg_id = derive_pg(bucket, key, pg_count);
        let okh = object_key_hash(bucket, key);
        let version_id: u64 = 0;
        let shard_key = ShardKey::new(&okh, version_id, shard_index);
        data_dir
            .join(format!("pg-{pg_id:04}"))
            .join("shards")
            .join(shard_key.hex_prefix())
            .join(shard_key.hex())
    }

    /// Delete a specific shard file from disk.
    fn delete_shard_on_disk(
        data_dir: &Path,
        bucket: &str,
        key: &str,
        shard_index: u8,
        pg_count: u32,
    ) {
        let path = shard_file_path(data_dir, bucket, key, shard_index, pg_count);
        std::fs::remove_file(&path).unwrap_or_else(|e| {
            panic!("failed to delete shard {shard_index} at {}: {e}", path.display())
        });
    }

    /// Corrupt a specific shard file on disk (flip first byte).
    /// PgStore's read_shard will detect CRC mismatch.
    fn corrupt_shard_on_disk(
        data_dir: &Path,
        bucket: &str,
        key: &str,
        shard_index: u8,
        pg_count: u32,
    ) {
        let path = shard_file_path(data_dir, bucket, key, shard_index, pg_count);
        let mut data = std::fs::read(&path).unwrap_or_else(|e| {
            panic!("failed to read shard {shard_index} at {}: {e}", path.display())
        });
        assert!(!data.is_empty(), "shard file is empty");
        data[0] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();
    }

    // ── EC fault injection tests ────────────────────────────────────

    #[test]
    fn ec_reconstruction_after_shard_loss() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"This data should survive shard loss!";
        coord
            .put_object("bucket", "resilient", data, &[], NO_WRITE)
            .unwrap();

        // Delete one data shard using the helper
        delete_shard_on_disk(tmp.path(), "bucket", "resilient", 0, 4);

        // Get should still succeed via EC reconstruction
        let obj = coord.get_object("bucket", "resilient", NO_READ).unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn ec_drop_one_data_shard_get() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC single shard loss test data";
        coord.put_object("bucket", "obj1", data, &[], NO_WRITE).unwrap();

        delete_shard_on_disk(tmp.path(), "bucket", "obj1", 0, 4);

        let obj = coord.get_object("bucket", "obj1", NO_READ).unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn ec_drop_m_shards_at_limit() {
        // Config: k=4, m=2. Dropping exactly m=2 shards should still recover.
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC m-shard loss limit test data";
        coord.put_object("bucket", "obj2", data, &[], NO_WRITE).unwrap();

        // Delete 2 data shards (indices 0 and 1)
        delete_shard_on_disk(tmp.path(), "bucket", "obj2", 0, 4);
        delete_shard_on_disk(tmp.path(), "bucket", "obj2", 1, 4);

        let obj = coord.get_object("bucket", "obj2", NO_READ).unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn ec_drop_m_plus_one_shards_fails() {
        // Config: k=4, m=2. Dropping m+1=3 shards should fail.
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC m+1 shard loss test data";
        coord.put_object("bucket", "obj3", data, &[], NO_WRITE).unwrap();

        // Delete 3 shards (indices 0, 1, 2)
        delete_shard_on_disk(tmp.path(), "bucket", "obj3", 0, 4);
        delete_shard_on_disk(tmp.path(), "bucket", "obj3", 1, 4);
        delete_shard_on_disk(tmp.path(), "bucket", "obj3", 2, 4);

        let err = coord.get_object("bucket", "obj3", NO_READ).unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn ec_corrupt_one_data_shard_recovery() {
        // Corrupt shard 0 on disk. PgStore detects CRC mismatch, EC reconstructs.
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC corruption recovery test data";
        coord.put_object("bucket", "obj4", data, &[], NO_WRITE).unwrap();

        corrupt_shard_on_disk(tmp.path(), "bucket", "obj4", 0, 4);

        let obj = coord.get_object("bucket", "obj4", NO_READ).unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn ec_range_get_with_missing_shard() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"Hello, World! Range test with EC recovery";
        coord.put_object("bucket", "obj5", data, &[], NO_WRITE).unwrap();

        // Delete shard 0 (covers the beginning of the data)
        delete_shard_on_disk(tmp.path(), "bucket", "obj5", 0, 4);

        // Range get should still succeed via EC reconstruction
        let result = coord
            .get_object_range("bucket", "obj5", ByteRange::Range { start: 0, end: 4 }, NO_READ)
            .unwrap();
        assert_eq!(result.data, b"Hello");
    }

    #[test]
    fn ec_drop_parity_shard_data_still_works() {
        // Delete parity shard (index k=4). Only data shards needed for normal read.
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC parity shard drop test";
        coord.put_object("bucket", "obj6", data, &[], NO_WRITE).unwrap();

        // Delete first parity shard (index 4, since k=4)
        delete_shard_on_disk(tmp.path(), "bucket", "obj6", 4, 4);

        let obj = coord.get_object("bucket", "obj6", NO_READ).unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn put_to_nonexistent_bucket_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        let err = coord.put_object("no-such-bucket", "key", b"data", &[], NO_WRITE).unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn get_nonexistent_object_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let err = coord.get_object("bucket", "no-such-key", NO_READ).unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn etag_consistency() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let result = coord.put_object("bucket", "key", b"data", &[], NO_WRITE).unwrap();

        let obj = coord.get_object("bucket", "key", NO_READ).unwrap();
        assert_eq!(result.etag, obj.etag);

        let head = coord.head_object("bucket", "key", NO_READ).unwrap();
        assert_eq!(result.etag, head.etag);
    }

    #[test]
    fn list_objects_delimiter_with_continuation() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord.put_object("bucket", "a/1", b"1", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "a/2", b"2", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "b/1", b"3", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "c/1", b"4", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "root.txt", b"5", &[], NO_WRITE).unwrap();

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
                .put_object("bucket", &key, b"data", &[], NO_WRITE)
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
            .put_object("no-bucket", "key", b"data", &[], NO_WRITE)
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
            coord.put_object("bucket", &key, b"data", &[], NO_WRITE).unwrap();
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
            coord.put_object("bucket", &key, b"data", &[], NO_WRITE).unwrap();
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
        coord.put_object("bucket", "photos/2024/jan.jpg", b"j", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "photos/2024/feb.jpg", b"f", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "photos/2025/mar.jpg", b"m", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "photos/top.jpg", b"t", &[], NO_WRITE).unwrap();

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
        coord.put_object("bucket", "only-one", b"data", &[], NO_WRITE).unwrap();

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
        coord.put_object("bucket", "key1", b"data", &[], NO_WRITE).unwrap();

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
        coord.put_object("bucket", "a/1", b"data", &[], NO_WRITE).unwrap();

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
        coord.put_object("bucket", "key1", b"data1", &[], NO_WRITE).unwrap();
        coord.put_object("bucket", "key2", b"data2", &[], NO_WRITE).unwrap();

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

        let result = coord.delete_objects("bucket", &entries, NO_DELETE).unwrap();
        assert_eq!(result.deleted.len(), 3);
        assert!(result.errors.is_empty());

        // Verify objects are actually gone
        assert!(coord.get_object("bucket", "key1", NO_READ).is_err());
        assert!(coord.get_object("bucket", "key2", NO_READ).is_err());
    }

    #[test]
    fn delete_objects_nonexistent_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        let entries = vec![crate::http::xml::DeleteObjectEntry {
            key: "key1".to_string(),
            version_id: None,
        }];

        let err = coord.delete_objects("no-bucket", &entries, NO_DELETE).unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    /// Simulate the exact Ceph test suite cleanup workflow:
    /// 1. Create bucket + objects
    /// 2. GET /?versions → list_objects_v2 (no delimiter) to discover all keys
    /// 3. Build DeleteObjects XML from the version listing
    /// 4. Parse that XML back (as the server would)
    /// 5. POST /?delete → delete_objects with parsed entries
    /// 6. Verify bucket is empty and can be deleted
    #[test]
    fn ceph_cleanup_workflow() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("test-bucket").unwrap();
        coord.put_object("test-bucket", "dir/file1.txt", b"hello", &[], NO_WRITE).unwrap();
        coord.put_object("test-bucket", "dir/file2.txt", b"world", &[], NO_WRITE).unwrap();
        coord.put_object("test-bucket", "root.txt", b"root", &[], NO_WRITE).unwrap();

        // Step 1: ListObjectVersions — reuses list_objects_v2 with no delimiter
        let list_result = coord
            .list_objects_v2("test-bucket", None, None, None, 1000)
            .unwrap();
        assert_eq!(list_result.objects.len(), 3);

        // Step 2: Build XML like Ceph cleanup would, using keys from listing
        let versions_xml = crate::http::xml::list_object_versions_xml(
            "test-bucket",
            None,
            None,
            1000,
            &list_result,
        );
        // Verify the XML has all three objects with version_id="null"
        assert!(versions_xml.contains("<Key>dir/file1.txt</Key>"));
        assert!(versions_xml.contains("<Key>dir/file2.txt</Key>"));
        assert!(versions_xml.contains("<Key>root.txt</Key>"));
        for _ in 0..3 {
            assert!(versions_xml.contains("<VersionId>null</VersionId>"));
        }

        // Step 3: Build a DeleteObjects XML body from the listed keys
        // (this is what the Ceph client sends)
        let mut delete_xml = String::from("<Delete>");
        for obj in &list_result.objects {
            delete_xml.push_str(&format!("<Object><Key>{}</Key></Object>", obj.key));
        }
        delete_xml.push_str("</Delete>");

        // Step 4: Parse the delete XML (as our server would on receiving the POST)
        let (entries, quiet) =
            crate::http::xml::parse_delete_objects_xml(delete_xml.as_bytes()).unwrap();
        assert_eq!(entries.len(), 3);
        assert!(!quiet);

        // Step 5: Batch delete
        let delete_result = coord.delete_objects("test-bucket", &entries, NO_DELETE).unwrap();
        assert_eq!(delete_result.deleted.len(), 3);
        assert!(delete_result.errors.is_empty());

        // Step 6: Bucket should now be empty and deletable
        let list_after = coord
            .list_objects_v2("test-bucket", None, None, None, 1000)
            .unwrap();
        assert!(list_after.objects.is_empty());
        coord.delete_bucket("test-bucket").unwrap();
    }

    /// Same workflow but with paginated listing and quiet-mode delete.
    #[test]
    fn ceph_cleanup_workflow_paginated() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        for i in 0..5 {
            let key = format!("key-{:02}", i);
            coord.put_object("bucket", &key, b"data", &[], NO_WRITE).unwrap();
        }

        // Page 1: max_keys=2
        let page1 = coord
            .list_objects_v2("bucket", None, None, None, 2)
            .unwrap();
        assert_eq!(page1.objects.len(), 2);
        assert!(page1.is_truncated);
        let token = page1.next_continuation_token.clone().unwrap();

        // Page 2
        let page2 = coord
            .list_objects_v2("bucket", None, None, Some(&token), 2)
            .unwrap();
        assert_eq!(page2.objects.len(), 2);
        let token2 = page2.next_continuation_token.clone().unwrap();

        // Page 3
        let page3 = coord
            .list_objects_v2("bucket", None, None, Some(&token2), 2)
            .unwrap();
        assert_eq!(page3.objects.len(), 1);
        assert!(!page3.is_truncated);

        // Collect all keys across pages
        let all_keys: Vec<String> = page1
            .objects
            .iter()
            .chain(page2.objects.iter())
            .chain(page3.objects.iter())
            .map(|o| o.key.clone())
            .collect();
        assert_eq!(all_keys.len(), 5);

        // Build quiet-mode delete XML
        let mut delete_xml = String::from("<Delete><Quiet>true</Quiet>");
        for key in &all_keys {
            delete_xml.push_str(&format!("<Object><Key>{}</Key></Object>", key));
        }
        delete_xml.push_str("</Delete>");

        let (entries, quiet) =
            crate::http::xml::parse_delete_objects_xml(delete_xml.as_bytes()).unwrap();
        assert_eq!(entries.len(), 5);
        assert!(quiet);

        let delete_result = coord.delete_objects("bucket", &entries, NO_DELETE).unwrap();
        assert_eq!(delete_result.deleted.len(), 5);
        assert!(delete_result.errors.is_empty());

        // Verify quiet-mode XML omits <Deleted> elements
        let result_xml = crate::http::xml::delete_objects_result_xml(
            &delete_result.deleted,
            &delete_result.errors,
            quiet,
        );
        assert!(!result_xml.contains("<Deleted>"));
        assert!(result_xml.contains("DeleteResult"));

        // Bucket is empty, can be deleted
        coord.delete_bucket("bucket").unwrap();
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
            .put_object("bucket", "key", &vec![0u8; 256 * 1024 * 1024 + 1], &[], NO_WRITE);
        assert!(matches!(
            err,
            Err(ServerError::ObjectTooLarge { .. })
        ));
    }

    // ── shard planning unit tests ──────────────────────────────────────

    #[test]
    fn compute_shard_size_exact_multiple() {
        // 100 bytes, k=4 → no padding needed → 25 per shard
        assert_eq!(Coordinator::compute_shard_size(100, 4), 25);
    }

    #[test]
    fn compute_shard_size_needs_padding() {
        // 101 bytes, k=4 → pad to 104 → 26 per shard
        assert_eq!(Coordinator::compute_shard_size(101, 4), 26);
    }

    #[test]
    fn compute_shard_size_small() {
        // 1 byte, k=4 → pad to 4 → 1 per shard
        assert_eq!(Coordinator::compute_shard_size(1, 4), 1);
    }

    #[test]
    fn compute_shard_size_zero() {
        // 0 bytes, k=4 → 0 per shard
        assert_eq!(Coordinator::compute_shard_size(0, 4), 0);
    }

    #[test]
    fn shards_for_byte_range_single_shard() {
        // shard_size=25, range [0,24] → shard 0
        assert_eq!(Coordinator::shards_for_byte_range(0, 24, 25, 4), vec![0]);
    }

    #[test]
    fn shards_for_byte_range_spans_two() {
        // shard_size=25, range [20,30] → shards 0,1
        assert_eq!(Coordinator::shards_for_byte_range(20, 30, 25, 4), vec![0, 1]);
    }

    #[test]
    fn shards_for_byte_range_all_shards() {
        // shard_size=25, range [0,99] → shards 0,1,2,3
        assert_eq!(
            Coordinator::shards_for_byte_range(0, 99, 25, 4),
            vec![0, 1, 2, 3]
        );
    }

    #[test]
    fn shards_for_byte_range_last_shard_only() {
        // shard_size=25, range [75,99] → shard 3
        assert_eq!(Coordinator::shards_for_byte_range(75, 99, 25, 4), vec![3]);
    }

    #[test]
    fn shards_for_byte_range_clamped_to_k() {
        // end falls past last shard → clamp to k-1
        assert_eq!(
            Coordinator::shards_for_byte_range(75, 200, 25, 4),
            vec![3]
        );
    }

    #[test]
    fn shards_for_byte_range_zero_shard_size() {
        assert_eq!(Coordinator::shards_for_byte_range(0, 10, 0, 4), vec![]);
    }

    // ── range GET tests ────────────────────────────────────────────────

    #[test]
    fn get_object_range_basic() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "key", b"Hello, World!", &[], NO_WRITE)
            .unwrap();

        // bytes=0-4 → "Hello"
        let result = coord
            .get_object_range("bucket", "key", ByteRange::Range { start: 0, end: 4 }, NO_READ)
            .unwrap();
        assert_eq!(result.data, b"Hello");
        assert_eq!(result.range_start, 0);
        assert_eq!(result.range_end, 4);
        assert_eq!(result.size, 13);
    }

    #[test]
    fn get_object_range_suffix() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "key", b"Hello, World!", &[], NO_WRITE)
            .unwrap();

        // bytes=-6 → "World!"  (last 6 bytes)
        let result = coord
            .get_object_range("bucket", "key", ByteRange::Suffix { length: 6 }, NO_READ)
            .unwrap();
        assert_eq!(result.data, b"World!");
        assert_eq!(result.range_start, 7);
        assert_eq!(result.range_end, 12);
    }

    #[test]
    fn get_object_range_from_start() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "key", b"Hello, World!", &[], NO_WRITE)
            .unwrap();

        // bytes=7- → "World!"
        let result = coord
            .get_object_range("bucket", "key", ByteRange::FromStart { start: 7 }, NO_READ)
            .unwrap();
        assert_eq!(result.data, b"World!");
    }

    #[test]
    fn get_object_range_unsatisfiable() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "key", b"Hello", &[], NO_WRITE)
            .unwrap();

        // bytes=100- → unsatisfiable
        let err = coord
            .get_object_range("bucket", "key", ByteRange::FromStart { start: 100 }, NO_READ)
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRange { total_size: 5 }));
    }

    #[test]
    fn get_object_range_clamps_end() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "key", b"Hello", &[], NO_WRITE)
            .unwrap();

        // bytes=0-99999 on 5-byte object → clamp to 0-4
        let result = coord
            .get_object_range(
                "bucket",
                "key",
                ByteRange::Range {
                    start: 0,
                    end: 99999,
                },
                NO_READ,
            )
            .unwrap();
        assert_eq!(result.data, b"Hello");
        assert_eq!(result.range_start, 0);
        assert_eq!(result.range_end, 4);
    }

    // ── Conditional request integration tests ────────────────────────

    #[test]
    fn put_if_none_match_star_creates() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let cond = WriteCondition {
            if_none_match_any: true,
            ..Default::default()
        };
        let result = coord
            .put_object("bucket", "new-key", b"data", &[], &cond)
            .unwrap();
        assert!(!result.etag.is_empty());
    }

    #[test]
    fn put_if_none_match_star_rejects_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "key", b"v1", &[], NO_WRITE)
            .unwrap();

        let cond = WriteCondition {
            if_none_match_any: true,
            ..Default::default()
        };
        let err = coord
            .put_object("bucket", "key", b"v2", &[], &cond)
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn put_if_match_updates() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let r1 = coord
            .put_object("bucket", "key", b"v1", &[], NO_WRITE)
            .unwrap();
        let cond = WriteCondition {
            if_match: Some(r1.etag.clone()),
            ..Default::default()
        };
        let r2 = coord
            .put_object("bucket", "key", b"v2", &[], &cond)
            .unwrap();
        assert_ne!(r1.etag, r2.etag);

        let obj = coord.get_object("bucket", "key", NO_READ).unwrap();
        assert_eq!(obj.data, b"v2");
    }

    #[test]
    fn put_if_match_stale_etag_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let r1 = coord
            .put_object("bucket", "key", b"v1", &[], NO_WRITE)
            .unwrap();
        // Overwrite so etag changes
        coord
            .put_object("bucket", "key", b"v2", &[], NO_WRITE)
            .unwrap();

        let cond = WriteCondition {
            if_match: Some(r1.etag),
            ..Default::default()
        };
        let err = coord
            .put_object("bucket", "key", b"v3", &[], &cond)
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn get_if_match_returns_object() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = coord
            .put_object("bucket", "key", b"data", &[], NO_WRITE)
            .unwrap();
        let cond = ReadCondition {
            if_match: Some(put.etag),
            ..Default::default()
        };
        let obj = coord.get_object("bucket", "key", &cond).unwrap();
        assert_eq!(obj.data, b"data");
    }

    #[test]
    fn get_if_match_wrong_etag_returns_412() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "key", b"data", &[], NO_WRITE)
            .unwrap();

        let cond = ReadCondition {
            if_match: Some("\"0000000000000000\"".to_string()),
            ..Default::default()
        };
        let err = coord.get_object("bucket", "key", &cond).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn get_if_none_match_returns_304() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = coord
            .put_object("bucket", "key", b"data", &[], NO_WRITE)
            .unwrap();
        let cond = ReadCondition {
            if_none_match: Some(put.etag),
            ..Default::default()
        };
        let err = coord.get_object("bucket", "key", &cond).unwrap_err();
        assert!(matches!(err, ServerError::NotModified { .. }));
    }

    #[test]
    fn head_if_none_match_returns_304() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = coord
            .put_object("bucket", "key", b"data", &[], NO_WRITE)
            .unwrap();
        let cond = ReadCondition {
            if_none_match: Some(put.etag),
            ..Default::default()
        };
        let err = coord.head_object("bucket", "key", &cond).unwrap_err();
        assert!(matches!(err, ServerError::NotModified { .. }));
    }

    #[test]
    fn delete_if_match_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = coord
            .put_object("bucket", "key", b"data", &[], NO_WRITE)
            .unwrap();
        let cond = DeleteCondition {
            if_match: Some(put.etag),
        };
        coord.delete_object("bucket", "key", &cond).unwrap();
        assert!(coord.get_object("bucket", "key", NO_READ).is_err());
    }

    #[test]
    fn delete_if_match_wrong_etag_returns_412() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "key", b"data", &[], NO_WRITE)
            .unwrap();

        let cond = DeleteCondition {
            if_match: Some("\"0000000000000000\"".to_string()),
        };
        let err = coord
            .delete_object("bucket", "key", &cond)
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn delete_objects_if_match_per_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let p1 = coord
            .put_object("bucket", "key1", b"data1", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "key2", b"data2", &[], NO_WRITE)
            .unwrap();

        // Use key1's etag for both entries; key2 will fail the condition
        let cond = DeleteCondition {
            if_match: Some(p1.etag),
        };
        let entries = vec![
            crate::http::xml::DeleteObjectEntry {
                key: "key1".to_string(),
                version_id: None,
            },
            crate::http::xml::DeleteObjectEntry {
                key: "key2".to_string(),
                version_id: None,
            },
        ];
        let result = coord.delete_objects("bucket", &entries, &cond).unwrap();
        assert_eq!(result.deleted.len(), 1);
        assert_eq!(result.deleted[0].key, "key1");
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].key, "key2");
    }

    #[test]
    fn range_get_if_match_returns_data() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = coord
            .put_object("bucket", "key", b"Hello, World!", &[], NO_WRITE)
            .unwrap();
        let cond = ReadCondition {
            if_match: Some(put.etag),
            ..Default::default()
        };
        let result = coord
            .get_object_range("bucket", "key", ByteRange::Range { start: 0, end: 4 }, &cond)
            .unwrap();
        assert_eq!(result.data, b"Hello");
    }
}
