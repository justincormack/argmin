/// Coordinator: orchestrates S3 operations across EC, storage, and metadata layers.
use std::sync::{Arc, MutexGuard};

use ec::{EcConfig, ErasureCodec};
use storage::traits::{GlobalService, PgMetadataStore, ShardStore};
use storage::{
    BucketInfo, DataLayout, ListObjectVersionsReq, ListObjectsReq, ObjectRecord, PutObjectMetaReq,
    ShardKey, SharedStorageNode, SqliteBucketDb,
};

use crate::conditional::{
    check_copy_source_conditions, check_delete_conditions, check_read_conditions,
    check_write_conditions, DeleteCondition, ReadCondition, WriteCondition,
};
use crate::error::ServerError;
use crate::etag::{crc64_to_etag_bytes, etag_bytes_to_crc64, format_etag};
use crate::metadata_blob::MetadataBlob;
use crate::pg::{derive_pg, derive_pg_shards, object_key_hash};
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
    pub version_id: u64,
}

/// Result of a GetObject operation.
#[derive(Debug)]
pub struct GetObjectResult {
    pub data: Vec<u8>,
    pub metadata: MetadataBlob,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub version_id: u64,
    pub tags: Option<String>,
}

/// Result of a HeadObject operation.
#[derive(Debug)]
pub struct HeadObjectResult {
    pub metadata: MetadataBlob,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub version_id: u64,
    pub tags: Option<String>,
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
    pub version_id: u64,
    pub tags: Option<String>,
}

/// Metadata handling directive for `CopyObject`.
#[derive(Debug, Clone, Copy)]
pub enum MetadataDirective {
    /// Preserve source object's metadata.
    Copy,
    /// Replace metadata with values from request headers.
    Replace,
}

/// Result of a `CopyObject` operation.
#[derive(Debug)]
pub struct CopyObjectResult {
    pub etag: String,
    pub last_modified: u64,
    pub version_id: u64,
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
    pub owner_principal: String,
}

/// Entry in a ListObjectVersions result.
#[derive(Debug, Clone)]
pub struct VersionEntry {
    pub key: String,
    pub version_id: u64,
    pub is_latest: bool,
    pub size: u64,
    pub etag: String,
    pub last_modified: u64,
    pub is_delete_marker: bool,
}

/// Result of a ListObjectVersions operation.
#[derive(Debug)]
pub struct ListObjectVersionsResult {
    pub versions: Vec<VersionEntry>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<u64>,
}

/// Result of a DeleteObject operation.
#[derive(Debug)]
pub struct DeleteObjectResult {
    pub version_id: u64,
    pub delete_marker: bool,
}

/// Result entry for a successfully deleted object in a batch delete.
#[derive(Debug)]
pub struct DeletedObject {
    pub key: String,
    pub version_id: u64,
    pub delete_marker: bool,
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

/// Ordered PG guard pair for object operations.
///
/// Constructed only by `lock_object_pgs_for_read` / `lock_object_pgs_for_write`.
/// Object paths should not hand-roll multi-PG locking.
struct TwoPgGuards<'a> {
    meta: MutexGuard<'a, storage::PgStore>,
    shard: Option<MutexGuard<'a, storage::PgStore>>,
}

impl<'a> TwoPgGuards<'a> {
    fn new(
        meta: MutexGuard<'a, storage::PgStore>,
        shard: Option<MutexGuard<'a, storage::PgStore>>,
    ) -> Self {
        Self { meta, shard }
    }

    fn meta(&self) -> &storage::PgStore {
        &self.meta
    }

    fn shard(&self) -> &storage::PgStore {
        self.shard.as_deref().unwrap_or(&self.meta)
    }
}

struct LockedReadObject<'a> {
    record: ObjectRecord,
    pgs: TwoPgGuards<'a>,
}

struct LockedWriteObject<'a> {
    version_id: u64,
    pgs: TwoPgGuards<'a>,
}

/// The coordinator ties together EC, storage, and metadata.
pub struct Coordinator {
    storage_node: Arc<SharedStorageNode>,
    bucket_db: SqliteBucketDb,
    ec_codec: ErasureCodec,
    ec_config: EcConfig,
    pg_count: u32,
    region: String,
}

impl Coordinator {
    /// Create a new coordinator.
    pub fn new(
        storage_node: Arc<SharedStorageNode>,
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
        self.create_bucket_for_owner("default-owner", name, false)
    }

    pub fn create_bucket_for_owner(
        &self,
        owner_principal: &str,
        name: &str,
        public_read: bool,
    ) -> Result<(), ServerError> {
        self.bucket_db
            .create_bucket(name, owner_principal, public_read)
            .or_else(|e| match e {
                storage::MetadataError::BucketAlreadyExists => {
                    let existing = self.head_bucket(name)?;
                    if existing.owner_principal == owner_principal {
                        Ok(())
                    } else {
                        Err(ServerError::BucketAlreadyExists)
                    }
                }
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
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound { name },
            storage::MetadataError::BucketNotEmpty => ServerError::BucketNotEmpty,
            other => ServerError::Metadata(other),
        })
    }

    pub fn head_bucket(&self, name: &str) -> Result<BucketInfo, ServerError> {
        self.bucket_db.head_bucket(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound { name },
            other => ServerError::Metadata(other),
        })
    }

    pub fn list_buckets(&self) -> Result<Vec<BucketInfo>, ServerError> {
        self.list_buckets_for_owner("default-owner")
    }

    pub fn list_buckets_for_owner(
        &self,
        owner_principal: &str,
    ) -> Result<Vec<BucketInfo>, ServerError> {
        Ok(self.bucket_db.list_buckets(owner_principal)?)
    }

    pub fn put_bucket_versioning(&self, name: &str, state: u8) -> Result<(), ServerError> {
        // Verify bucket exists
        self.head_bucket(name)?;
        self.bucket_db
            .put_bucket_versioning(name, state)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => {
                    ServerError::BucketNotFound { name }
                }
                storage::MetadataError::InvalidVersioningTransition { from, to } => {
                    ServerError::InvalidRequest {
                        reason: format!("invalid versioning transition from {} to {}", from, to),
                    }
                }
                other => ServerError::Metadata(other),
            })
    }

    pub fn get_bucket_versioning(&self, name: &str) -> Result<u8, ServerError> {
        let info = self.head_bucket(name)?;
        Ok(info.versioning)
    }

    pub fn put_bucket_cors(&self, name: &str, config: &str) -> Result<(), ServerError> {
        self.head_bucket(name)?;
        self.bucket_db
            .put_bucket_cors(name, config)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => {
                    ServerError::BucketNotFound { name }
                }
                other => ServerError::Metadata(other),
            })
    }

    pub fn get_bucket_cors(&self, name: &str) -> Result<Option<String>, ServerError> {
        self.head_bucket(name)?;
        self.bucket_db.get_bucket_cors(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound { name },
            other => ServerError::Metadata(other),
        })
    }

    pub fn delete_bucket_cors(&self, name: &str) -> Result<(), ServerError> {
        self.head_bucket(name)?;
        self.bucket_db
            .delete_bucket_cors(name)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => {
                    ServerError::BucketNotFound { name }
                }
                other => ServerError::Metadata(other),
            })
    }

    // ── Bucket tagging ────────────────────────────────────────────────

    pub fn put_bucket_tags(&self, name: &str, tags: &str) -> Result<(), ServerError> {
        self.head_bucket(name)?;
        self.bucket_db
            .put_bucket_tags(name, tags)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => {
                    ServerError::BucketNotFound { name }
                }
                other => ServerError::Metadata(other),
            })
    }

    pub fn get_bucket_tags(&self, name: &str) -> Result<Option<String>, ServerError> {
        self.head_bucket(name)?;
        self.bucket_db.get_bucket_tags(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound { name },
            other => ServerError::Metadata(other),
        })
    }

    pub fn delete_bucket_tags(&self, name: &str) -> Result<(), ServerError> {
        self.head_bucket(name)?;
        self.bucket_db
            .delete_bucket_tags(name)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => {
                    ServerError::BucketNotFound { name }
                }
                other => ServerError::Metadata(other),
            })
    }

    // ── Public access block ───────────────────────────────────────────

    pub fn put_bucket_public_access_block(
        &self,
        name: &str,
        config: &str,
    ) -> Result<(), ServerError> {
        self.head_bucket(name)?;
        self.bucket_db
            .put_bucket_public_access_block(name, config)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => {
                    ServerError::BucketNotFound { name }
                }
                other => ServerError::Metadata(other),
            })
    }

    pub fn get_bucket_public_access_block(
        &self,
        name: &str,
    ) -> Result<Option<String>, ServerError> {
        self.head_bucket(name)?;
        self.bucket_db
            .get_bucket_public_access_block(name)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => {
                    ServerError::BucketNotFound { name }
                }
                other => ServerError::Metadata(other),
            })
    }

    pub fn delete_bucket_public_access_block(&self, name: &str) -> Result<(), ServerError> {
        self.head_bucket(name)?;
        self.bucket_db
            .delete_bucket_public_access_block(name)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => {
                    ServerError::BucketNotFound { name }
                }
                other => ServerError::Metadata(other),
            })
    }

    // ── Bucket ACL ───────────────────────────────────────────────────

    pub fn put_bucket_acl(&self, name: &str, public_read: bool) -> Result<(), ServerError> {
        self.head_bucket(name)?;
        self.bucket_db
            .put_bucket_acl(name, public_read)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => {
                    ServerError::BucketNotFound { name }
                }
                other => ServerError::Metadata(other),
            })
    }

    // ── Ownership controls ────────────────────────────────────────────

    pub fn put_bucket_ownership_controls(
        &self,
        name: &str,
        config: &str,
    ) -> Result<(), ServerError> {
        self.head_bucket(name)?;
        self.bucket_db
            .put_bucket_ownership_controls(name, config)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => {
                    ServerError::BucketNotFound { name }
                }
                other => ServerError::Metadata(other),
            })
    }

    pub fn get_bucket_ownership_controls(&self, name: &str) -> Result<Option<String>, ServerError> {
        self.head_bucket(name)?;
        self.bucket_db
            .get_bucket_ownership_controls(name)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => {
                    ServerError::BucketNotFound { name }
                }
                other => ServerError::Metadata(other),
            })
    }

    pub fn delete_bucket_ownership_controls(&self, name: &str) -> Result<(), ServerError> {
        self.head_bucket(name)?;
        self.bucket_db
            .delete_bucket_ownership_controls(name)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => {
                    ServerError::BucketNotFound { name }
                }
                other => ServerError::Metadata(other),
            })
    }

    // ── Object tagging ──────────────────────────────────────────────

    pub fn put_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<u64>,
        tags: &str,
    ) -> Result<(), ServerError> {
        self.head_bucket(bucket)?;
        let pg_id = derive_pg(bucket, key, self.pg_count);
        let pg = self.storage_node.get_pg(pg_id)?;
        let record = match version_id {
            Some(vid) => pg.get_object_version(bucket, key, vid),
            None => pg.get_object_meta(bucket, key),
        }
        .map_err(|e| match e {
            storage::MetadataError::ObjectNotFound => ServerError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            other => ServerError::Metadata(other),
        })?;
        pg.put_object_tags(bucket, key, record.version_id, tags)
            .map_err(ServerError::Metadata)
    }

    pub fn get_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<u64>,
    ) -> Result<Option<String>, ServerError> {
        self.head_bucket(bucket)?;
        let pg_id = derive_pg(bucket, key, self.pg_count);
        let pg = self.storage_node.get_pg(pg_id)?;
        let record = match version_id {
            Some(vid) => pg.get_object_version(bucket, key, vid),
            None => pg.get_object_meta(bucket, key),
        }
        .map_err(|e| match e {
            storage::MetadataError::ObjectNotFound => ServerError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            other => ServerError::Metadata(other),
        })?;
        pg.get_object_tags(bucket, key, record.version_id)
            .map_err(ServerError::Metadata)
    }

    pub fn delete_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<u64>,
    ) -> Result<(), ServerError> {
        self.head_bucket(bucket)?;
        let pg_id = derive_pg(bucket, key, self.pg_count);
        let pg = self.storage_node.get_pg(pg_id)?;
        let record = match version_id {
            Some(vid) => pg.get_object_version(bucket, key, vid),
            None => pg.get_object_meta(bucket, key),
        }
        .map_err(|e| match e {
            storage::MetadataError::ObjectNotFound => ServerError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            other => ServerError::Metadata(other),
        })?;
        pg.delete_object_tags(bucket, key, record.version_id)
            .map_err(ServerError::Metadata)
    }

    // ── Object operations ─────────────────────────────────────────────

    /// Core write path: serialize metadata, EC-encode, write shards, record metadata.
    /// Shared by `put_object` and `copy_object`.
    ///
    /// Callers are responsible for locking the PGs and passing references.
    /// `meta_pg` and `shard_pg` may point to the same `PgStore`.
    #[allow(clippy::too_many_arguments)]
    fn write_object_inner(
        &self,
        bucket: &str,
        key: &str,
        metadata_blob: &MetadataBlob,
        user_data: &[u8],
        version_id: u64,
        meta_pg: &storage::PgStore,
        shard_pg: &storage::PgStore,
    ) -> Result<PutObjectResult, ServerError> {
        // 1. Serialize blob
        let blob_bytes = metadata_blob.serialize()?;

        // 2. Concatenate: blob_bytes || user_data
        let mut full_data = Vec::with_capacity(blob_bytes.len() + user_data.len());
        full_data.extend_from_slice(&blob_bytes);
        full_data.extend_from_slice(user_data);

        // 3. Compute ETag (CRC64 of full_data before padding)
        let etag_crc = crc64::checksum(&full_data);

        // 4. Pad to multiple of k for equal shard sizes
        let k = self.ec_config.data_shards as usize;
        let m = self.ec_config.parity_shards as usize;
        let remainder = full_data.len() % k;
        if remainder != 0 {
            let pad = k - remainder;
            full_data.resize(full_data.len() + pad, 0);
        }

        // 5. Split into k data shards
        let shard_size = full_data.len() / k;
        let data_shards: Vec<&[u8]> = (0..k)
            .map(|i| &full_data[i * shard_size..(i + 1) * shard_size])
            .collect();

        // 6. Allocate parity buffers and encode
        let mut parity_bufs: Vec<Vec<u8>> = (0..m).map(|_| vec![0u8; shard_size]).collect();
        let mut parity_refs: Vec<&mut [u8]> =
            parity_bufs.iter_mut().map(|v| v.as_mut_slice()).collect();
        self.ec_codec.encode(&data_shards, &mut parity_refs)?;

        // 7. Compute object_key_hash
        let okh = object_key_hash(bucket, key);

        // 9. Write all k+m shards, with cleanup on failure
        let mut written_shards: Vec<ShardKey> = Vec::with_capacity(k + m);
        let write_result: Result<(), ServerError> = (|| {
            for i in 0..(k + m) {
                let shard_key = ShardKey::new(&okh, version_id, i as u8);
                let shard_data = if i < k {
                    data_shards[i]
                } else {
                    &parity_bufs[i - k]
                };
                shard_pg.write_shard(&shard_key, shard_data)?;
                written_shards.push(shard_key);
            }
            Ok(())
        })();

        if let Err(e) = write_result {
            // Best-effort cleanup of already-written shards
            for shard_key in &written_shards {
                let _ = shard_pg.delete_shard(shard_key);
            }
            return Err(e);
        }

        // 10. Record metadata (to metadata PG)
        let meta_result = meta_pg.put_object_meta(&PutObjectMetaReq {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
            status: 0,
            size: user_data.len() as u64,
            total_size: (blob_bytes.len() + user_data.len()) as u64,
            etag: crc64_to_etag_bytes(etag_crc),
            etag_kind: 0,
            ec_k: self.ec_config.data_shards,
            ec_m: self.ec_config.parity_shards,
        });

        if let Err(e) = meta_result {
            // Best-effort cleanup of all written shards
            for shard_key in &written_shards {
                let _ = shard_pg.delete_shard(shard_key);
            }
            return Err(ServerError::Metadata(e));
        }

        Ok(PutObjectResult {
            etag: format_etag(etag_crc),
            version_id,
        })
    }

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

        // 1. Verify bucket exists and get versioning state
        let bucket_info = self.head_bucket(bucket)?;

        let metadata_blob = MetadataBlob::from_headers(headers)?;
        let LockedWriteObject { version_id, pgs } =
            self.lock_object_pgs_for_write(bucket, key, bucket_info.versioning)?;
        let meta_pg = pgs.meta();
        let shard_pg = pgs.shard();

        // 2. Check write conditions if any are set
        if !cond.is_empty() {
            let existing_etag = match meta_pg.get_object_meta(bucket, key) {
                Ok(record) => {
                    let crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
                    Some(format_etag(crc))
                }
                Err(storage::MetadataError::ObjectNotFound) => None,
                Err(e) => return Err(ServerError::Metadata(e)),
            };
            if cond.if_match.is_some() && existing_etag.is_none() {
                return Err(ServerError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
            check_write_conditions(cond, existing_etag.as_deref())?;
        }

        // 3. Write object while holding both PG locks.
        self.write_object_inner(
            bucket,
            key,
            &metadata_blob,
            data,
            version_id,
            meta_pg,
            shard_pg,
        )
    }

    /// Copy an object from one location to another.
    ///
    /// Supports conditional headers on both source and destination,
    /// and metadata directive (COPY preserves source metadata, REPLACE
    /// uses new headers).
    #[allow(clippy::too_many_arguments)]
    pub fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        src_version_id: Option<u64>,
        dst_bucket: &str,
        dst_key: &str,
        src_cond: &ReadCondition,
        dst_cond: &WriteCondition,
        directive: MetadataDirective,
        new_headers: &[(&str, &str)],
    ) -> Result<CopyObjectResult, ServerError> {
        // Phase 1: Read source object
        let (src_metadata, user_data) = {
            let LockedReadObject {
                record: src_record,
                pgs,
            } = self.lock_object_pgs_for_read(src_bucket, src_key, src_version_id)?;
            let src_shard_pg = pgs.shard();

            Self::require_inline_layout(&src_record)?;

            let src_etag_crc = etag_bytes_to_crc64(&src_record.etag).unwrap_or(0);
            let src_etag = format_etag(src_etag_crc);
            check_copy_source_conditions(src_cond, &src_etag, src_record.last_modified)?;

            let src_okh = object_key_hash(src_bucket, src_key);
            let src_version_id = src_record.version_id;
            let total = src_record.total_size as usize;
            let data = self
                .read_range(
                    src_shard_pg,
                    &src_okh,
                    src_version_id,
                    &src_record,
                    0,
                    total - 1,
                )
                .map_err(|e| match e {
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: src_bucket.to_string(),
                            key: src_key.to_string(),
                        }
                    }
                    other => other,
                })?;

            // Verify full-object CRC against stored etag
            let actual_crc = crc64::checksum(&data);
            if actual_crc != src_etag_crc {
                return Err(ServerError::IntegrityError {
                    bucket: src_bucket.to_string(),
                    key: src_key.to_string(),
                    expected: src_etag_crc,
                    actual: actual_crc,
                });
            }

            let metadata_size = (src_record.total_size - src_record.size) as usize;
            let (src_metadata, _) = MetadataBlob::deserialize(&data[..metadata_size])?;
            let user_data = data[metadata_size..].to_vec();
            (src_metadata, user_data)
        }; // source locks dropped here

        // Phase 2: Write destination object
        let metadata_blob = match directive {
            MetadataDirective::Copy => src_metadata,
            MetadataDirective::Replace => MetadataBlob::from_headers(new_headers)?,
        };

        let dst_bucket_info = self.head_bucket(dst_bucket)?;
        let LockedWriteObject {
            version_id: dst_version_id,
            pgs,
        } = self.lock_object_pgs_for_write(dst_bucket, dst_key, dst_bucket_info.versioning)?;
        let dst_meta_pg = pgs.meta();
        let dst_shard_pg = pgs.shard();

        // Check dest write conditions
        if !dst_cond.is_empty() {
            let existing_etag = match dst_meta_pg.get_object_meta(dst_bucket, dst_key) {
                Ok(record) => {
                    let crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
                    Some(format_etag(crc))
                }
                Err(storage::MetadataError::ObjectNotFound) => None,
                Err(e) => return Err(ServerError::Metadata(e)),
            };
            check_write_conditions(dst_cond, existing_etag.as_deref())?;
        }

        let put_result = self.write_object_inner(
            dst_bucket,
            dst_key,
            &metadata_blob,
            &user_data,
            dst_version_id,
            dst_meta_pg,
            dst_shard_pg,
        )?;

        // Read back dest metadata to get the authoritative last_modified
        let dst_record = dst_meta_pg
            .get_object_meta(dst_bucket, dst_key)
            .map_err(ServerError::Metadata)?;

        Ok(CopyObjectResult {
            etag: put_result.etag,
            last_modified: dst_record.last_modified,
            version_id: put_result.version_id,
        })
    }

    fn lookup_object_record(
        meta_pg: &storage::PgStore,
        bucket: &str,
        key: &str,
        version_id: Option<u64>,
    ) -> Result<ObjectRecord, ServerError> {
        match version_id {
            Some(vid) => meta_pg.get_object_version(bucket, key, vid),
            None => meta_pg.get_object_meta(bucket, key),
        }
        .map_err(|e| match e {
            storage::MetadataError::ObjectNotFound => ServerError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            other => ServerError::Metadata(other),
        })
    }

    /// Verify that the object uses InlineLegacy layout.
    ///
    /// Returns `NotImplemented` for MultipartManifest objects, which require
    /// the multipart-aware read path (Step 9).
    fn require_inline_layout(record: &ObjectRecord) -> Result<(), ServerError> {
        if record.data_layout != DataLayout::InlineLegacy {
            return Err(ServerError::NotImplemented {
                feature: "multipart object reads".to_string(),
            });
        }
        Ok(())
    }

    /// Lock metadata and shard PGs for a consistent object read view.
    ///
    /// For latest-version reads (`version_id = None`), shard placement depends on
    /// the current metadata row's version_id. If `meta_pg_id > shard_pg_id`, we
    /// drop and relock in global ascending order, then re-read metadata to ensure
    /// the record still maps to the locked shard PG.
    fn lock_object_pgs_for_read<'a>(
        &'a self,
        bucket: &str,
        key: &str,
        version_id: Option<u64>,
    ) -> Result<LockedReadObject<'a>, ServerError> {
        let meta_pg_id = derive_pg(bucket, key, self.pg_count);

        loop {
            let meta_guard = self.storage_node.get_pg(meta_pg_id)?;
            let record = Self::lookup_object_record(&meta_guard, bucket, key, version_id)?;
            let shard_pg_id = derive_pg_shards(bucket, key, record.version_id, self.pg_count);

            if shard_pg_id == meta_pg_id {
                return Ok(LockedReadObject {
                    record,
                    pgs: TwoPgGuards::new(meta_guard, None),
                });
            }

            if meta_pg_id < shard_pg_id {
                let shard_guard = self.storage_node.get_pg(shard_pg_id)?;
                return Ok(LockedReadObject {
                    record,
                    pgs: TwoPgGuards::new(meta_guard, Some(shard_guard)),
                });
            }

            // Need lower-id shard PG first to avoid deadlocks with writers.
            drop(meta_guard);

            let (meta_guard, shard_guard) =
                self.storage_node.lock_two_pgs(meta_pg_id, shard_pg_id)?;
            let record = Self::lookup_object_record(&meta_guard, bucket, key, version_id)?;
            let verify_shard_pg_id =
                derive_pg_shards(bucket, key, record.version_id, self.pg_count);

            // Latest-version target changed while relocking; try again with new mapping.
            if verify_shard_pg_id != shard_pg_id {
                continue;
            }

            return Ok(LockedReadObject {
                record,
                pgs: TwoPgGuards::new(meta_guard, shard_guard),
            });
        }
    }

    /// Lock metadata and shard PGs for an object write.
    ///
    /// Computes a candidate version ID from metadata while holding the metadata PG
    /// lock, then locks shard PG in global order and revalidates when needed.
    fn lock_object_pgs_for_write<'a>(
        &'a self,
        bucket: &str,
        key: &str,
        versioning_state: u8,
    ) -> Result<LockedWriteObject<'a>, ServerError> {
        let meta_pg_id = derive_pg(bucket, key, self.pg_count);

        loop {
            let meta_guard = self.storage_node.get_pg(meta_pg_id)?;
            let version_id = if versioning_state == 1 {
                meta_guard.next_version_id(bucket, key)?
            } else {
                0
            };
            let shard_pg_id = derive_pg_shards(bucket, key, version_id, self.pg_count);

            if shard_pg_id == meta_pg_id {
                return Ok(LockedWriteObject {
                    version_id,
                    pgs: TwoPgGuards::new(meta_guard, None),
                });
            }

            if meta_pg_id < shard_pg_id {
                let shard_guard = self.storage_node.get_pg(shard_pg_id)?;
                if versioning_state == 1 {
                    let current = meta_guard.next_version_id(bucket, key)?;
                    if current != version_id {
                        continue;
                    }
                }
                return Ok(LockedWriteObject {
                    version_id,
                    pgs: TwoPgGuards::new(meta_guard, Some(shard_guard)),
                });
            }

            // Need lower-id shard PG first to avoid deadlocks with readers/writers.
            drop(meta_guard);

            let (meta_guard, shard_guard) =
                self.storage_node.lock_two_pgs(meta_pg_id, shard_pg_id)?;
            let version_id = if versioning_state == 1 {
                meta_guard.next_version_id(bucket, key)?
            } else {
                0
            };
            let verify_shard_pg_id = derive_pg_shards(bucket, key, version_id, self.pg_count);
            if verify_shard_pg_id != shard_pg_id {
                continue;
            }

            return Ok(LockedWriteObject {
                version_id,
                pgs: TwoPgGuards::new(meta_guard, shard_guard),
            });
        }
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
            let present_indices: Vec<usize> =
                (0..(k + m)).filter(|&i| all_shards[i].is_some()).collect();
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

            let mut outputs: Vec<Vec<u8>> = missing_needed
                .iter()
                .map(|_| vec![0u8; shard_size])
                .collect();
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
        version_id: Option<u64>,
        cond: &ReadCondition,
    ) -> Result<GetObjectResult, ServerError> {
        let LockedReadObject { record, pgs } =
            self.lock_object_pgs_for_read(bucket, key, version_id)?;

        // If latest version is a delete marker, return 404 with x-amz-delete-marker
        if record.status == 1 {
            return Err(ServerError::DeleteMarkerHit {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        Self::require_inline_layout(&record)?;

        let okh = object_key_hash(bucket, key);
        let object_version_id = record.version_id;
        let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
        let shard_pg = pgs.shard();

        // Check conditions before reading shard data
        let etag_str = format_etag(etag_crc);
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        let total = record.total_size as usize;
        let data = self
            .read_range(shard_pg, &okh, object_version_id, &record, 0, total - 1)
            .map_err(|e| match e {
                ServerError::Store(storage::StoreError::NotFound) => ServerError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                },
                other => other,
            })?;

        // Verify full-object CRC against stored etag
        let actual_crc = crc64::checksum(&data);
        if actual_crc != etag_crc {
            return Err(ServerError::IntegrityError {
                bucket: bucket.to_string(),
                key: key.to_string(),
                expected: etag_crc,
                actual: actual_crc,
            });
        }

        let metadata_size = (record.total_size - record.size) as usize;
        let (metadata, _) = MetadataBlob::deserialize(&data[..metadata_size])?;
        let user_data = data[metadata_size..].to_vec();

        Ok(GetObjectResult {
            data: user_data,
            metadata,
            etag: format_etag(etag_crc),
            size: record.size,
            last_modified: record.last_modified,
            version_id: record.version_id,
            tags: record.tags,
        })
    }

    /// Head object: returns metadata without body.
    ///
    /// Reads only the shards covering the metadata blob.
    pub fn head_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<u64>,
        cond: &ReadCondition,
    ) -> Result<HeadObjectResult, ServerError> {
        let LockedReadObject { record, pgs } =
            self.lock_object_pgs_for_read(bucket, key, version_id)?;

        // If latest version is a delete marker, return 404 with x-amz-delete-marker
        if record.status == 1 {
            return Err(ServerError::DeleteMarkerHit {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        Self::require_inline_layout(&record)?;

        let okh = object_key_hash(bucket, key);
        let object_version_id = record.version_id;
        let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
        let shard_pg = pgs.shard();

        // Check conditions before reading shard data
        let etag_str = format_etag(etag_crc);
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        let metadata_size = (record.total_size - record.size) as usize;
        let data = self
            .read_range(
                shard_pg,
                &okh,
                object_version_id,
                &record,
                0,
                metadata_size - 1,
            )
            .map_err(|e| match e {
                ServerError::Store(storage::StoreError::NotFound) => ServerError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                },
                other => other,
            })?;

        let (metadata, _) = MetadataBlob::deserialize(&data)?;
        Ok(HeadObjectResult {
            metadata,
            etag: format_etag(etag_crc),
            size: record.size,
            last_modified: record.last_modified,
            version_id: record.version_id,
            tags: record.tags,
        })
    }

    /// Get a byte range of an object from storage (for HTTP Range requests).
    ///
    /// Returns 206 Partial Content data.
    pub fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<u64>,
        range: ByteRange,
        cond: &ReadCondition,
    ) -> Result<GetObjectRangeResult, ServerError> {
        let LockedReadObject { record, pgs } =
            self.lock_object_pgs_for_read(bucket, key, version_id)?;

        // If latest version is a delete marker, return 404 with x-amz-delete-marker
        if record.status == 1 {
            return Err(ServerError::DeleteMarkerHit {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        Self::require_inline_layout(&record)?;

        let okh = object_key_hash(bucket, key);
        let object_version_id = record.version_id;
        let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
        let shard_pg = pgs.shard();

        // Check conditions before reading shard data
        let etag_str = format_etag(etag_crc);
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        // Resolve byte range against user data size
        let (user_start, user_end) =
            range
                .resolve(record.size)
                .ok_or(ServerError::InvalidRange {
                    total_size: record.size,
                })?;

        let metadata_size = (record.total_size - record.size) as usize;

        // Read metadata (always need it for response headers)
        let meta_data = self
            .read_range(
                shard_pg,
                &okh,
                object_version_id,
                &record,
                0,
                metadata_size - 1,
            )
            .map_err(|e| match e {
                ServerError::Store(storage::StoreError::NotFound) => ServerError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                },
                other => other,
            })?;
        let (metadata, _) = MetadataBlob::deserialize(&meta_data)?;

        // Read user data range
        let blob_start = metadata_size + user_start as usize;
        let blob_end = metadata_size + user_end as usize;
        let user_data = self
            .read_range(
                shard_pg,
                &okh,
                object_version_id,
                &record,
                blob_start,
                blob_end,
            )
            .map_err(|e| match e {
                ServerError::Store(storage::StoreError::NotFound) => ServerError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                },
                other => other,
            })?;

        Ok(GetObjectRangeResult {
            data: user_data,
            metadata,
            etag: format_etag(etag_crc),
            size: record.size,
            last_modified: record.last_modified,
            range_start: user_start,
            range_end: user_end,
            version_id: record.version_id,
            tags: record.tags,
        })
    }

    /// Delete an object.
    pub fn delete_object(
        &self,
        bucket: &str,
        key: &str,
        request_version_id: Option<u64>,
        cond: &DeleteCondition,
    ) -> Result<DeleteObjectResult, ServerError> {
        let bucket_info = self.head_bucket(bucket)?;

        match (bucket_info.versioning, request_version_id) {
            // Unversioned bucket: physical delete (current behavior)
            (0, _) => {
                let LockedReadObject { record, pgs } =
                    match self.lock_object_pgs_for_read(bucket, key, None) {
                        Ok(locked) => locked,
                        Err(ServerError::ObjectNotFound { .. }) => {
                            if !cond.is_empty() {
                                return Err(ServerError::PreconditionFailed);
                            }
                            return Ok(DeleteObjectResult {
                                version_id: 0,
                                delete_marker: false,
                            });
                        }
                        Err(other) => return Err(other),
                    };

                let meta_pg = pgs.meta();
                let shard_pg = pgs.shard();

                Self::require_inline_layout(&record)?;

                // Check delete conditions
                if !cond.is_empty() {
                    let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
                    let etag_str = format_etag(etag_crc);
                    check_delete_conditions(cond, &etag_str, record.last_modified, record.size)?;
                }

                let okh = object_key_hash(bucket, key);
                let vid = record.version_id;
                let total = record.ec_k as usize + record.ec_m as usize;

                // Delete all shards (idempotent)
                for i in 0..total {
                    let shard_key = ShardKey::new(&okh, vid, i as u8);
                    shard_pg.delete_shard(&shard_key)?;
                }

                // Delete metadata record
                meta_pg.delete_object_meta(bucket, key)?;

                Ok(DeleteObjectResult {
                    version_id: 0,
                    delete_marker: false,
                })
            }

            // Versioned/Suspended + specific versionId: permanent delete that version
            (_, Some(vid)) => {
                let LockedReadObject { record, pgs } =
                    match self.lock_object_pgs_for_read(bucket, key, Some(vid)) {
                        Ok(locked) => locked,
                        Err(ServerError::ObjectNotFound { .. }) => {
                            return Ok(DeleteObjectResult {
                                version_id: vid,
                                delete_marker: false,
                            });
                        }
                        Err(other) => return Err(other),
                    };

                let meta_pg = pgs.meta();
                let shard_pg = pgs.shard();

                // Delete shards if it's a live object (not a delete marker)
                if record.status == 0 {
                    Self::require_inline_layout(&record)?;

                    let okh = object_key_hash(bucket, key);
                    let total = record.ec_k as usize + record.ec_m as usize;

                    for i in 0..total {
                        let shard_key = ShardKey::new(&okh, vid, i as u8);
                        shard_pg.delete_shard(&shard_key)?;
                    }
                }

                meta_pg.delete_object_version(bucket, key, vid)?;

                let is_delete_marker = record.status == 1;

                Ok(DeleteObjectResult {
                    version_id: vid,
                    delete_marker: is_delete_marker,
                })
            }

            // Versioned/Suspended + no versionId: insert delete marker
            (_, None) => {
                let meta_pg_id = derive_pg(bucket, key, self.pg_count);
                let meta_pg = self.storage_node.get_pg(meta_pg_id)?;
                let marker_vid = meta_pg.next_version_id(bucket, key)?;
                meta_pg.put_object_meta(&PutObjectMetaReq {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id: marker_vid,
                    status: 1,
                    size: 0,
                    total_size: 0,
                    etag: vec![],
                    etag_kind: 0,
                    ec_k: 0,
                    ec_m: 0,
                })?;

                Ok(DeleteObjectResult {
                    version_id: marker_vid,
                    delete_marker: true,
                })
            }
        }
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
        let bucket_info = self.head_bucket(bucket)?;

        // MaxKeys=0 is valid per S3 spec: return empty result
        if max_keys == 0 {
            return Ok(ListObjectsResult {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_continuation_token: None,
                owner_principal: bucket_info.owner_principal,
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
        let mut last_entry: Option<String> = None;
        let mut is_truncated = false;
        let token = continuation_token;

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
                    let cp = format!("{}{}", prefix_str, &after_prefix[..pos + delim.len()]);
                    // Skip all remaining keys under this common prefix so the
                    // continuation token advances past the entire group.
                    let is_new = seen_prefixes.insert(cp.clone());
                    while i < all_objects.len() && all_objects[i].key.starts_with(&cp) {
                        i += 1;
                    }
                    if is_new && token.is_none_or(|t| cp.as_str() > t) {
                        common_prefixes.push(cp.clone());
                        entry_count += 1;
                        last_entry = Some(cp);
                    }
                } else {
                    if token.is_none_or(|t| record.key.as_str() > t) {
                        let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
                        objects.push(ListEntry {
                            key: record.key.clone(),
                            size: record.size,
                            etag: format_etag(etag_crc),
                            last_modified: record.last_modified,
                        });
                        entry_count += 1;
                        last_entry = Some(record.key.clone());
                    }
                    i += 1;
                }
            }
        } else {
            for record in &all_objects {
                if entry_count >= max {
                    is_truncated = true;
                    break;
                }
                if token.is_none_or(|t| record.key.as_str() > t) {
                    let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
                    objects.push(ListEntry {
                        key: record.key.clone(),
                        size: record.size,
                        etag: format_etag(etag_crc),
                        last_modified: record.last_modified,
                    });
                    entry_count += 1;
                    last_entry = Some(record.key.clone());
                }
            }

            // Check if there were more objects than max_keys (only if no token).
            if token.is_none() && all_objects.len() > max {
                is_truncated = true;
            }
        }

        // If we hit the record cap, there may be more results we didn't fetch.
        if hit_record_cap {
            is_truncated = true;
        }

        let next_token = if is_truncated { last_entry } else { None };

        Ok(ListObjectsResult {
            objects,
            common_prefixes,
            is_truncated,
            next_continuation_token: next_token,
            owner_principal: bucket_info.owner_principal,
        })
    }

    /// List object versions in a bucket.
    pub fn list_object_versions(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<u64>,
        max_keys: u32,
    ) -> Result<ListObjectVersionsResult, ServerError> {
        let _bucket_info = self.head_bucket(bucket)?;

        if max_keys == 0 {
            return Ok(ListObjectVersionsResult {
                versions: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_version_id_marker: None,
            });
        }

        // Fan out to all PGs and collect version records
        let mut all_versions: Vec<ObjectRecord> = Vec::new();
        for &pg_id in self.storage_node.pg_ids() {
            let pg = self.storage_node.get_pg(pg_id)?;
            let resp = pg.list_object_versions(&ListObjectVersionsReq {
                bucket: bucket.to_string(),
                prefix: prefix.map(|s| s.to_string()),
                key_marker: key_marker.map(|s| s.to_string()),
                version_id_marker,
                max_keys: max_keys.saturating_add(1),
            })?;
            all_versions.extend(resp.versions);
        }

        // Sort by (key ASC, version_id DESC)
        all_versions.sort_by(|a, b| a.key.cmp(&b.key).then(b.version_id.cmp(&a.version_id)));

        // Build result entries, tracking is_latest per key
        let max = max_keys as usize;
        let mut versions: Vec<VersionEntry> = Vec::new();
        let mut last_key: Option<&str> = None;

        for record in &all_versions {
            if versions.len() >= max {
                break;
            }
            let is_latest = last_key.is_none_or(|k| k != record.key);
            if is_latest {
                last_key = Some(&record.key);
            }

            let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
            versions.push(VersionEntry {
                key: record.key.clone(),
                version_id: record.version_id,
                is_latest,
                size: record.size,
                etag: format_etag(etag_crc),
                last_modified: record.last_modified,
                is_delete_marker: record.status == 1,
            });
        }

        let is_truncated = all_versions.len() > max;
        let (next_key_marker, next_version_id_marker) = if is_truncated {
            if let Some(last) = versions.last() {
                (Some(last.key.clone()), Some(last.version_id))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        Ok(ListObjectVersionsResult {
            versions,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
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
            let vid = entry.version_id.as_deref().and_then(|v| {
                if v == "null" {
                    Some(0)
                } else {
                    v.parse::<u64>().ok()
                }
            });
            match self.delete_object(bucket, &entry.key, vid, cond) {
                Ok(result) => {
                    deleted.push(DeletedObject {
                        key: entry.key.clone(),
                        version_id: result.version_id,
                        delete_marker: result.delete_marker,
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
    use std::sync::Barrier;
    use std::thread;

    const NO_READ: &ReadCondition = &ReadCondition {
        if_match: None,
        if_none_match: None,
        if_modified_since: None,
        if_unmodified_since: None,
    };
    const NO_WRITE: &WriteCondition = &WriteCondition {
        if_match: None,
        if_none_match: None,
    };
    const NO_DELETE: &DeleteCondition = &DeleteCondition {
        if_match: None,
        if_match_last_modified_time: None,
        if_match_size: None,
    };

    fn setup_coordinator(dir: &Path) -> Coordinator {
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
        let bucket_db = SqliteBucketDb::open_in_memory().unwrap();
        let ec_config = EcConfig::new(4, 2).unwrap();
        Coordinator::new(
            storage_node,
            bucket_db,
            ec_config,
            4,
            "us-east-1".to_string(),
        )
        .unwrap()
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
    fn create_bucket_different_owner_conflicts() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();
        let err = coord
            .create_bucket_for_owner("owner-b", "bucket", false)
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketAlreadyExists));
    }

    #[test]
    fn list_buckets_scoped_by_owner() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("owner-a", "bucket-a", false)
            .unwrap();
        coord
            .create_bucket_for_owner("owner-b", "bucket-b", false)
            .unwrap();

        let a = coord.list_buckets_for_owner("owner-a").unwrap();
        assert_eq!(a.len(), 1);
        assert_eq!(a[0].name, "bucket-a");
        assert_eq!(a[0].owner_principal, "owner-a");

        let b = coord.list_buckets_for_owner("owner-b").unwrap();
        assert_eq!(b.len(), 1);
        assert_eq!(b[0].name, "bucket-b");
        assert_eq!(b[0].owner_principal, "owner-b");
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

        let obj = coord
            .get_object("bucket", "hello.txt", None, NO_READ)
            .unwrap();
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

        let obj = coord.get_object("bucket", "obj", None, NO_READ).unwrap();
        assert_eq!(obj.data, b"{}");
        assert_eq!(obj.metadata.get("content-type"), Some("application/json"));
        assert_eq!(obj.metadata.get("x-amz-meta-author"), Some("alice"));
        assert_eq!(obj.metadata.get("x-amz-meta-version"), Some("42"));
    }

    #[test]
    fn head_object() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(
                "bucket",
                "key",
                b"data",
                &[("Content-Type", "text/plain")],
                NO_WRITE,
            )
            .unwrap();

        let head = coord.head_object("bucket", "key", None, NO_READ).unwrap();
        assert_eq!(head.size, 4);
        assert_eq!(head.metadata.get("content-type"), Some("text/plain"));
    }

    #[test]
    fn overwrite_object() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "key", b"v1", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "key", b"v2", &[], NO_WRITE)
            .unwrap();

        let obj = coord.get_object("bucket", "key", None, NO_READ).unwrap();
        assert_eq!(obj.data, b"v2");
    }

    #[test]
    fn empty_object() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "empty", b"", &[], NO_WRITE)
            .unwrap();

        let obj = coord.get_object("bucket", "empty", None, NO_READ).unwrap();
        assert_eq!(obj.data, b"");
        assert_eq!(obj.size, 0);
    }

    #[test]
    fn delete_object_then_get_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "key", b"data", &[], NO_WRITE)
            .unwrap();
        coord
            .delete_object("bucket", "key", None, NO_DELETE)
            .unwrap();

        let err = coord
            .get_object("bucket", "key", None, NO_READ)
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn delete_nonexistent_object_is_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        // Should not error
        coord
            .delete_object("bucket", "no-such-key", None, NO_DELETE)
            .unwrap();
    }

    #[test]
    fn list_objects() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "a/1", b"1", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "a/2", b"2", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "b/1", b"3", &[], NO_WRITE)
            .unwrap();

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
        coord
            .put_object("bucket", "photos/cat.jpg", b"cat", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "photos/dog.jpg", b"dog", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "docs/readme.md", b"md", &[], NO_WRITE)
            .unwrap();

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
        coord
            .put_object("bucket", "photos/cat.jpg", b"cat", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "photos/dog.jpg", b"dog", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "docs/readme.md", b"md", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "root.txt", b"root", &[], NO_WRITE)
            .unwrap();

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

        let obj = coord
            .get_object("bucket", "folder/", None, NO_READ)
            .unwrap();
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
        let version_id: u64 = 0;
        let pg_id = derive_pg_shards(bucket, key, version_id, pg_count);
        let okh = object_key_hash(bucket, key);
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
            panic!(
                "failed to delete shard {shard_index} at {}: {e}",
                path.display()
            )
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
            panic!(
                "failed to read shard {shard_index} at {}: {e}",
                path.display()
            )
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
        let obj = coord
            .get_object("bucket", "resilient", None, NO_READ)
            .unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn ec_drop_one_data_shard_get() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC single shard loss test data";
        coord
            .put_object("bucket", "obj1", data, &[], NO_WRITE)
            .unwrap();

        delete_shard_on_disk(tmp.path(), "bucket", "obj1", 0, 4);

        let obj = coord.get_object("bucket", "obj1", None, NO_READ).unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn ec_drop_m_shards_at_limit() {
        // Config: k=4, m=2. Dropping exactly m=2 shards should still recover.
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC m-shard loss limit test data";
        coord
            .put_object("bucket", "obj2", data, &[], NO_WRITE)
            .unwrap();

        // Delete 2 data shards (indices 0 and 1)
        delete_shard_on_disk(tmp.path(), "bucket", "obj2", 0, 4);
        delete_shard_on_disk(tmp.path(), "bucket", "obj2", 1, 4);

        let obj = coord.get_object("bucket", "obj2", None, NO_READ).unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn ec_drop_m_plus_one_shards_fails() {
        // Config: k=4, m=2. Dropping m+1=3 shards should fail.
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC m+1 shard loss test data";
        coord
            .put_object("bucket", "obj3", data, &[], NO_WRITE)
            .unwrap();

        // Delete 3 shards (indices 0, 1, 2)
        delete_shard_on_disk(tmp.path(), "bucket", "obj3", 0, 4);
        delete_shard_on_disk(tmp.path(), "bucket", "obj3", 1, 4);
        delete_shard_on_disk(tmp.path(), "bucket", "obj3", 2, 4);

        let err = coord
            .get_object("bucket", "obj3", None, NO_READ)
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn ec_corrupt_one_data_shard_recovery() {
        // Corrupt shard 0 on disk. PgStore detects CRC mismatch, EC reconstructs.
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC corruption recovery test data";
        coord
            .put_object("bucket", "obj4", data, &[], NO_WRITE)
            .unwrap();

        corrupt_shard_on_disk(tmp.path(), "bucket", "obj4", 0, 4);

        let obj = coord.get_object("bucket", "obj4", None, NO_READ).unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn ec_range_get_with_missing_shard() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"Hello, World! Range test with EC recovery";
        coord
            .put_object("bucket", "obj5", data, &[], NO_WRITE)
            .unwrap();

        // Delete shard 0 (covers the beginning of the data)
        delete_shard_on_disk(tmp.path(), "bucket", "obj5", 0, 4);

        // Range get should still succeed via EC reconstruction
        let result = coord
            .get_object_range(
                "bucket",
                "obj5",
                None,
                ByteRange::Range { start: 0, end: 4 },
                NO_READ,
            )
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
        coord
            .put_object("bucket", "obj6", data, &[], NO_WRITE)
            .unwrap();

        // Delete first parity shard (index 4, since k=4)
        delete_shard_on_disk(tmp.path(), "bucket", "obj6", 4, 4);

        let obj = coord.get_object("bucket", "obj6", None, NO_READ).unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn put_to_nonexistent_bucket_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .put_object("no-such-bucket", "key", b"data", &[], NO_WRITE)
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn get_nonexistent_object_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let err = coord
            .get_object("bucket", "no-such-key", None, NO_READ)
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn etag_consistency() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let result = coord
            .put_object("bucket", "key", b"data", &[], NO_WRITE)
            .unwrap();

        let obj = coord.get_object("bucket", "key", None, NO_READ).unwrap();
        assert_eq!(result.etag, obj.etag);

        let head = coord.head_object("bucket", "key", None, NO_READ).unwrap();
        assert_eq!(result.etag, head.etag);
    }

    #[test]
    fn list_objects_delimiter_with_continuation() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "a/1", b"1", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "a/2", b"2", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "b/1", b"3", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "c/1", b"4", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "root.txt", b"5", &[], NO_WRITE)
            .unwrap();

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
            coord
                .put_object("bucket", &key, b"data", &[], NO_WRITE)
                .unwrap();
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
            coord
                .put_object("bucket", &key, b"data", &[], NO_WRITE)
                .unwrap();
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
        coord
            .put_object("bucket", "photos/2024/jan.jpg", b"j", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "photos/2024/feb.jpg", b"f", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "photos/2025/mar.jpg", b"m", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "photos/top.jpg", b"t", &[], NO_WRITE)
            .unwrap();

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
        coord
            .put_object("bucket", "only-one", b"data", &[], NO_WRITE)
            .unwrap();

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
        coord
            .put_object("bucket", "key1", b"data", &[], NO_WRITE)
            .unwrap();

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
        coord
            .put_object("bucket", "a/1", b"data", &[], NO_WRITE)
            .unwrap();

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
        coord
            .put_object("bucket", "key1", b"data1", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "key2", b"data2", &[], NO_WRITE)
            .unwrap();

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
        assert!(coord.get_object("bucket", "key1", None, NO_READ).is_err());
        assert!(coord.get_object("bucket", "key2", None, NO_READ).is_err());
    }

    #[test]
    fn delete_objects_nonexistent_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        let entries = vec![crate::http::xml::DeleteObjectEntry {
            key: "key1".to_string(),
            version_id: None,
        }];

        let err = coord
            .delete_objects("no-bucket", &entries, NO_DELETE)
            .unwrap_err();
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
        coord
            .put_object("test-bucket", "dir/file1.txt", b"hello", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("test-bucket", "dir/file2.txt", b"world", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("test-bucket", "root.txt", b"root", &[], NO_WRITE)
            .unwrap();

        // Step 1: ListObjectVersions
        let versions_result = coord
            .list_object_versions("test-bucket", None, None, None, 1000)
            .unwrap();
        assert_eq!(versions_result.versions.len(), 3);

        // Step 2: Build XML like Ceph cleanup would, using keys from listing
        let versions_xml = crate::http::xml::list_object_versions_xml(
            "test-bucket",
            None,
            None,
            1000,
            &versions_result,
        );
        // Verify the XML has all three objects with version_id="null"
        assert!(versions_xml.contains("<Key>dir/file1.txt</Key>"));
        assert!(versions_xml.contains("<Key>dir/file2.txt</Key>"));
        assert!(versions_xml.contains("<Key>root.txt</Key>"));
        for _ in 0..3 {
            assert!(versions_xml.contains("<VersionId>null</VersionId>"));
        }

        // Also verify we can still list for the delete step below
        let list_result = coord
            .list_objects_v2("test-bucket", None, None, None, 1000)
            .unwrap();
        assert_eq!(list_result.objects.len(), 3);

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
        let delete_result = coord
            .delete_objects("test-bucket", &entries, NO_DELETE)
            .unwrap();
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
            coord
                .put_object("bucket", &key, b"data", &[], NO_WRITE)
                .unwrap();
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

        let err = coord.put_object(
            "bucket",
            "key",
            &vec![0u8; 256 * 1024 * 1024 + 1],
            &[],
            NO_WRITE,
        );
        assert!(matches!(err, Err(ServerError::ObjectTooLarge { .. })));
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
        assert_eq!(
            Coordinator::shards_for_byte_range(20, 30, 25, 4),
            vec![0, 1]
        );
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
        assert_eq!(Coordinator::shards_for_byte_range(75, 200, 25, 4), vec![3]);
    }

    #[test]
    fn shards_for_byte_range_zero_shard_size() {
        let empty: Vec<usize> = vec![];
        assert_eq!(Coordinator::shards_for_byte_range(0, 10, 0, 4), empty);
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
            .get_object_range(
                "bucket",
                "key",
                None,
                ByteRange::Range { start: 0, end: 4 },
                NO_READ,
            )
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
            .get_object_range(
                "bucket",
                "key",
                None,
                ByteRange::Suffix { length: 6 },
                NO_READ,
            )
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
            .get_object_range(
                "bucket",
                "key",
                None,
                ByteRange::FromStart { start: 7 },
                NO_READ,
            )
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
            .get_object_range(
                "bucket",
                "key",
                None,
                ByteRange::FromStart { start: 100 },
                NO_READ,
            )
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
                None,
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
            if_none_match: Some("*".to_string()),
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
            if_none_match: Some("*".to_string()),
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

        let obj = coord.get_object("bucket", "key", None, NO_READ).unwrap();
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
        let obj = coord.get_object("bucket", "key", None, &cond).unwrap();
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
        let err = coord.get_object("bucket", "key", None, &cond).unwrap_err();
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
        let err = coord.get_object("bucket", "key", None, &cond).unwrap_err();
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
        let err = coord.head_object("bucket", "key", None, &cond).unwrap_err();
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
            ..Default::default()
        };
        coord.delete_object("bucket", "key", None, &cond).unwrap();
        assert!(coord.get_object("bucket", "key", None, NO_READ).is_err());
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
            ..Default::default()
        };
        let err = coord
            .delete_object("bucket", "key", None, &cond)
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
            ..Default::default()
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
            .get_object_range(
                "bucket",
                "key",
                None,
                ByteRange::Range { start: 0, end: 4 },
                &cond,
            )
            .unwrap();
        assert_eq!(result.data, b"Hello");
    }

    // ── CopyObject tests ──────────────────────────────────────────────

    #[test]
    fn copy_object_basic() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let headers = [("Content-Type", "text/plain")];
        coord
            .put_object("bucket", "src", b"hello copy", &headers, NO_WRITE)
            .unwrap();

        let result = coord
            .copy_object(
                "bucket",
                "src",
                None,
                "bucket",
                "dst",
                NO_READ,
                NO_WRITE,
                MetadataDirective::Copy,
                &[],
            )
            .unwrap();
        assert!(!result.etag.is_empty());

        let obj = coord.get_object("bucket", "dst", None, NO_READ).unwrap();
        assert_eq!(obj.data, b"hello copy");
    }

    #[test]
    fn copy_object_metadata_copy_directive() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let headers = [
            ("Content-Type", "image/png"),
            ("X-Amz-Meta-Author", "alice"),
        ];
        coord
            .put_object("bucket", "src", b"data", &headers, NO_WRITE)
            .unwrap();

        coord
            .copy_object(
                "bucket",
                "src",
                None,
                "bucket",
                "dst",
                NO_READ,
                NO_WRITE,
                MetadataDirective::Copy,
                &[],
            )
            .unwrap();

        let obj = coord.get_object("bucket", "dst", None, NO_READ).unwrap();
        assert_eq!(obj.metadata.get("content-type"), Some("image/png"));
        assert_eq!(obj.metadata.get("x-amz-meta-author"), Some("alice"));
    }

    #[test]
    fn copy_object_metadata_replace_directive() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let headers = [
            ("Content-Type", "image/png"),
            ("X-Amz-Meta-Author", "alice"),
        ];
        coord
            .put_object("bucket", "src", b"data", &headers, NO_WRITE)
            .unwrap();

        let new_headers = [("Content-Type", "text/html"), ("X-Amz-Meta-Version", "2")];
        coord
            .copy_object(
                "bucket",
                "src",
                None,
                "bucket",
                "dst",
                NO_READ,
                NO_WRITE,
                MetadataDirective::Replace,
                &new_headers,
            )
            .unwrap();

        let obj = coord.get_object("bucket", "dst", None, NO_READ).unwrap();
        assert_eq!(obj.data, b"data");
        assert_eq!(obj.metadata.get("content-type"), Some("text/html"));
        assert_eq!(obj.metadata.get("x-amz-meta-version"), Some("2"));
        // Old metadata should be gone
        assert_eq!(obj.metadata.get("x-amz-meta-author"), None);
    }

    #[test]
    fn copy_object_same_key_replace_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let headers = [("Content-Type", "text/plain")];
        coord
            .put_object("bucket", "key", b"data", &headers, NO_WRITE)
            .unwrap();

        let new_headers = [("Content-Type", "application/json")];
        coord
            .copy_object(
                "bucket",
                "key",
                None,
                "bucket",
                "key",
                NO_READ,
                NO_WRITE,
                MetadataDirective::Replace,
                &new_headers,
            )
            .unwrap();

        let obj = coord.get_object("bucket", "key", None, NO_READ).unwrap();
        assert_eq!(obj.data, b"data");
        assert_eq!(obj.metadata.get("content-type"), Some("application/json"));
    }

    #[test]
    fn copy_object_source_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let err = coord
            .copy_object(
                "bucket",
                "no-such-key",
                None,
                "bucket",
                "dst",
                NO_READ,
                NO_WRITE,
                MetadataDirective::Copy,
                &[],
            )
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn copy_object_dest_bucket_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "src", b"data", &[], NO_WRITE)
            .unwrap();

        let err = coord
            .copy_object(
                "bucket",
                "src",
                None,
                "no-bucket",
                "dst",
                NO_READ,
                NO_WRITE,
                MetadataDirective::Copy,
                &[],
            )
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn copy_object_source_if_match_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord
            .put_object("bucket", "src", b"data", &[], NO_WRITE)
            .unwrap();

        let src_cond = ReadCondition {
            if_match: Some("\"0000000000000000\"".to_string()),
            ..Default::default()
        };
        let err = coord
            .copy_object(
                "bucket",
                "src",
                None,
                "bucket",
                "dst",
                &src_cond,
                NO_WRITE,
                MetadataDirective::Copy,
                &[],
            )
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn copy_object_dest_if_none_match_prevents_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_object("bucket", "src", b"data", &[], NO_WRITE)
            .unwrap();
        coord
            .put_object("bucket", "dst", b"existing", &[], NO_WRITE)
            .unwrap();

        let dst_cond = WriteCondition {
            if_none_match: Some("*".to_string()),
            ..Default::default()
        };
        let err = coord
            .copy_object(
                "bucket",
                "src",
                None,
                "bucket",
                "dst",
                NO_READ,
                &dst_cond,
                MetadataDirective::Copy,
                &[],
            )
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn copy_object_dest_if_match_allows_update() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_object("bucket", "src", b"new data", &[], NO_WRITE)
            .unwrap();
        let existing = coord
            .put_object("bucket", "dst", b"old data", &[], NO_WRITE)
            .unwrap();

        let dst_cond = WriteCondition {
            if_match: Some(existing.etag),
            ..Default::default()
        };
        let result = coord
            .copy_object(
                "bucket",
                "src",
                None,
                "bucket",
                "dst",
                NO_READ,
                &dst_cond,
                MetadataDirective::Copy,
                &[],
            )
            .unwrap();
        assert!(!result.etag.is_empty());

        let obj = coord.get_object("bucket", "dst", None, NO_READ).unwrap();
        assert_eq!(obj.data, b"new data");
    }

    #[test]
    fn copy_object_cross_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("src-bucket").unwrap();
        coord.create_bucket("dst-bucket").unwrap();

        let headers = [("Content-Type", "text/plain")];
        coord
            .put_object(
                "src-bucket",
                "key",
                b"cross bucket data",
                &headers,
                NO_WRITE,
            )
            .unwrap();

        coord
            .copy_object(
                "src-bucket",
                "key",
                None,
                "dst-bucket",
                "key",
                NO_READ,
                NO_WRITE,
                MetadataDirective::Copy,
                &[],
            )
            .unwrap();

        let obj = coord
            .get_object("dst-bucket", "key", None, NO_READ)
            .unwrap();
        assert_eq!(obj.data, b"cross bucket data");
        assert_eq!(obj.metadata.get("content-type"), Some("text/plain"));

        // Source should still exist
        let src = coord
            .get_object("src-bucket", "key", None, NO_READ)
            .unwrap();
        assert_eq!(src.data, b"cross bucket data");
    }

    // ── Bucket versioning tests ──────────────────────────────────────

    #[test]
    fn bucket_versioning_default_disabled() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let state = coord.get_bucket_versioning("bucket").unwrap();
        assert_eq!(state, 0);
    }

    #[test]
    fn bucket_versioning_enable() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord.put_bucket_versioning("bucket", 1).unwrap();
        assert_eq!(coord.get_bucket_versioning("bucket").unwrap(), 1);
    }

    #[test]
    fn bucket_versioning_enable_then_suspend() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord.put_bucket_versioning("bucket", 1).unwrap();
        coord.put_bucket_versioning("bucket", 2).unwrap();
        assert_eq!(coord.get_bucket_versioning("bucket").unwrap(), 2);
    }

    #[test]
    fn bucket_versioning_suspend_then_enable() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord.put_bucket_versioning("bucket", 1).unwrap();
        coord.put_bucket_versioning("bucket", 2).unwrap();
        coord.put_bucket_versioning("bucket", 1).unwrap();
        assert_eq!(coord.get_bucket_versioning("bucket").unwrap(), 1);
    }

    #[test]
    fn bucket_versioning_cannot_disable_from_enabled() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord.put_bucket_versioning("bucket", 1).unwrap();
        let err = coord.put_bucket_versioning("bucket", 0).unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    #[test]
    fn bucket_versioning_nonexistent_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        let err = coord.put_bucket_versioning("no-bucket", 1).unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn put_object_returns_version_id_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let result = coord
            .put_object("bucket", "key", b"data", &[], NO_WRITE)
            .unwrap();
        assert_eq!(result.version_id, 0);
    }

    #[test]
    fn get_object_returns_version_id() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_object("bucket", "key", b"data", &[], NO_WRITE)
            .unwrap();
        let obj = coord.get_object("bucket", "key", None, NO_READ).unwrap();
        assert_eq!(obj.version_id, 0);
    }

    #[test]
    fn head_object_returns_version_id() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_object("bucket", "key", b"data", &[], NO_WRITE)
            .unwrap();
        let head = coord.head_object("bucket", "key", None, NO_READ).unwrap();
        assert_eq!(head.version_id, 0);
    }

    #[test]
    fn versioned_put_is_safe_across_concurrent_frontends() {
        let tmp = tempfile::tempdir().unwrap();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let bucket_db_path = tmp.path().join("buckets.db");
        let ec_config = EcConfig::new(4, 2).unwrap();

        let make_coord = || {
            let bucket_db = SqliteBucketDb::open(&bucket_db_path).unwrap();
            Coordinator::new(
                Arc::clone(&storage_node),
                bucket_db,
                ec_config,
                4,
                "us-east-1".to_string(),
            )
            .unwrap()
        };

        let admin = make_coord();
        admin.create_bucket("bucket").unwrap();
        admin.put_bucket_versioning("bucket", 1).unwrap();

        // Repeat to increase the chance of exposing races.
        for i in 0..20 {
            let coord_a = make_coord();
            let coord_b = make_coord();
            let key = format!("key-{i}");
            let key_a = key.clone();
            let key_b = key;

            let barrier = Arc::new(Barrier::new(3));
            let b1 = Arc::clone(&barrier);
            let b2 = Arc::clone(&barrier);

            let t1 = thread::spawn(move || {
                b1.wait();
                coord_a.put_object("bucket", &key_a, b"v1", &[], NO_WRITE)
            });
            let t2 = thread::spawn(move || {
                b2.wait();
                coord_b.put_object("bucket", &key_b, b"v2", &[], NO_WRITE)
            });

            barrier.wait();

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            assert!(r1.is_ok(), "first concurrent put failed: {r1:?}");
            assert!(r2.is_ok(), "second concurrent put failed: {r2:?}");

            let v1 = r1.unwrap().version_id;
            let v2 = r2.unwrap().version_id;
            assert_ne!(v1, v2, "concurrent puts must not reuse version IDs");
        }
    }

    #[test]
    fn get_object_is_consistent_during_concurrent_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let bucket_db_path = tmp.path().join("buckets.db");
        let ec_config = EcConfig::new(4, 2).unwrap();

        let make_coord = || {
            let bucket_db = SqliteBucketDb::open(&bucket_db_path).unwrap();
            Coordinator::new(
                Arc::clone(&storage_node),
                bucket_db,
                ec_config,
                4,
                "us-east-1".to_string(),
            )
            .unwrap()
        };

        let admin = make_coord();
        admin.create_bucket("bucket").unwrap();

        let object_size = 512 * 1024;
        admin
            .put_object("bucket", "key", &vec![b'A'; object_size], &[], NO_WRITE)
            .unwrap();

        let mut current = b'A';
        for _ in 0..50 {
            let next = if current == b'A' { b'B' } else { b'A' };
            let new_payload = vec![next; object_size];

            let reader = make_coord();
            let writer = make_coord();
            let barrier = Arc::new(Barrier::new(3));
            let b1 = Arc::clone(&barrier);
            let b2 = Arc::clone(&barrier);

            let t_write = thread::spawn(move || {
                b1.wait();
                writer.put_object("bucket", "key", &new_payload, &[], NO_WRITE)
            });
            let t_read = thread::spawn(move || {
                b2.wait();
                reader.get_object("bucket", "key", None, NO_READ)
            });

            barrier.wait();

            let write_res = t_write.join().unwrap();
            assert!(
                write_res.is_ok(),
                "concurrent overwrite failed: {write_res:?}"
            );

            let read_res = t_read.join().unwrap();
            let obj = read_res.expect("get_object must not fail during overwrite");
            assert_eq!(obj.data.len(), object_size);
            let uniform =
                obj.data.iter().all(|&b| b == current) || obj.data.iter().all(|&b| b == next);
            assert!(
                uniform,
                "read must return a complete old or new object image"
            );

            current = next;
        }
    }

    #[test]
    fn delete_object_is_consistent_during_concurrent_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let bucket_db_path = tmp.path().join("buckets.db");
        let ec_config = EcConfig::new(4, 2).unwrap();

        let make_coord = || {
            let bucket_db = SqliteBucketDb::open(&bucket_db_path).unwrap();
            Coordinator::new(
                Arc::clone(&storage_node),
                bucket_db,
                ec_config,
                4,
                "us-east-1".to_string(),
            )
            .unwrap()
        };

        let admin = make_coord();
        admin.create_bucket("bucket").unwrap();

        let object_size = 256 * 1024;
        admin
            .put_object("bucket", "key", &vec![b'A'; object_size], &[], NO_WRITE)
            .unwrap();

        for i in 0..50 {
            let expected_byte = if i % 2 == 0 { b'B' } else { b'C' };
            let payload = vec![expected_byte; object_size];

            let writer = make_coord();
            let deleter = make_coord();
            let barrier = Arc::new(Barrier::new(3));
            let b1 = Arc::clone(&barrier);
            let b2 = Arc::clone(&barrier);

            let t_write = thread::spawn(move || {
                b1.wait();
                writer.put_object("bucket", "key", &payload, &[], NO_WRITE)
            });
            let t_delete = thread::spawn(move || {
                b2.wait();
                deleter.delete_object("bucket", "key", None, NO_DELETE)
            });

            barrier.wait();

            let write_res = t_write.join().unwrap();
            assert!(
                write_res.is_ok(),
                "concurrent overwrite failed: {write_res:?}"
            );

            let delete_res = t_delete.join().unwrap();
            assert!(
                delete_res.is_ok(),
                "concurrent delete failed: {delete_res:?}"
            );

            let check = make_coord().get_object("bucket", "key", None, NO_READ);
            match check {
                Ok(obj) => {
                    assert_eq!(obj.data.len(), object_size);
                    assert!(
                        obj.data.iter().all(|&b| b == expected_byte),
                        "if object exists after put/delete race, it must be a full new image"
                    );
                }
                Err(ServerError::ObjectNotFound { .. }) => {}
                Err(other) => panic!("unexpected read result after put/delete race: {other:?}"),
            }
        }
    }

    #[test]
    fn delete_object_returns_result() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_object("bucket", "key", b"data", &[], NO_WRITE)
            .unwrap();
        let result = coord
            .delete_object("bucket", "key", None, NO_DELETE)
            .unwrap();
        assert_eq!(result.version_id, 0);
        assert!(!result.delete_marker);
    }
}
