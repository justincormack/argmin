/// Coordinator: orchestrates S3 operations across EC, storage, and metadata layers.
use std::sync::{Arc, MutexGuard};

use ec::{EcConfig, ErasureCodec};
use storage::traits::{GlobalService, PgMetadataStore, ShardStore};
use storage::{
    BucketInfo, ChecksumAlgorithm, ChecksumType, CreateMultipartUploadReq, DataLayout,
    ListMultipartUploadsReq, ListObjectVersionsReq, ListObjectsReq, ListPartsReq,
    MultipartPartRecord, MultipartUploadRecord, ObjectPartRecord, ObjectRecord, PutObjectMetaReq,
    ShardKey, SharedStorageNode, SqliteBucketDb, UploadState,
};

use crate::conditional::{
    check_copy_source_conditions, check_delete_conditions, check_read_conditions,
    check_write_conditions, DeleteCondition, ReadCondition, WriteCondition,
};
use crate::error::ServerError;
use crate::etag::{
    compute_multipart_etag, crc64_to_etag_bytes, etag_bytes_to_crc64, format_etag,
    format_object_etag,
};
use crate::metadata_blob::MetadataBlob;
use crate::pg::{derive_pg, derive_pg_shards, object_key_hash, part_key_hash};
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

/// Result of a HeadObject with partNumber.
#[derive(Debug)]
pub struct HeadObjectPartResult {
    pub metadata: MetadataBlob,
    pub etag: String,
    pub part_size: u64,
    pub total_size: u64,
    pub last_modified: u64,
    pub parts_count: u32,
    pub version_id: u64,
    pub tags: Option<String>,
    /// Per-part checksum: (header_name, base64_value).
    pub checksum: Option<(String, String)>,
}

/// A single part entry for GetObjectAttributes ObjectParts response.
#[derive(Debug)]
pub struct ObjectPartEntry {
    pub part_number: u32,
    pub size: u64,
    /// Base64-encoded checksum for this part (None if no checksum).
    pub checksum: Option<String>,
}

/// Pagination info for ObjectParts in GetObjectAttributes.
#[derive(Debug)]
pub struct ObjectPartsInfo {
    pub total_parts_count: u32,
    /// True for checksummed multipart uploads (full detail: parts, pagination).
    /// False for non-checksummed multipart (only PartsCount in XML).
    pub has_detail: bool,
    pub parts: Vec<ObjectPartEntry>,
    pub is_truncated: bool,
    pub next_part_number_marker: Option<u32>,
    pub max_parts: u32,
    pub part_number_marker: u32,
}

/// Result of a GetObjectAttributes operation.
#[derive(Debug)]
pub struct GetObjectAttributesResult {
    pub metadata: MetadataBlob,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub version_id: u64,
    pub object_parts: Option<ObjectPartsInfo>,
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

/// Result of a part-level GetObject operation (206 Partial Content).
#[derive(Debug)]
pub struct GetObjectPartResult {
    pub data: Vec<u8>,
    pub metadata: MetadataBlob,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub part_start: u64,
    pub part_end: u64,
    pub parts_count: u32,
    pub version_id: u64,
    pub tags: Option<String>,
    /// Per-part checksum: (header_name, base64_value).
    pub checksum: Option<(String, String)>,
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

/// Result of an UploadPart operation.
#[derive(Debug)]
pub struct UploadPartResult {
    pub etag: String,
    /// Checksum algorithm used for this part (if any).
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    /// Raw checksum bytes for this part (if any).
    pub checksum_bytes: Option<Vec<u8>>,
}

/// Result of an UploadPartCopy operation.
#[derive(Debug)]
pub struct UploadPartCopyResult {
    pub etag: String,
    pub last_modified: u64,
}

/// Internal result from the shared part-write path.
struct WritePartInnerResult {
    etag: String,
    checksum_algorithm: Option<ChecksumAlgorithm>,
    checksum_bytes: Option<Vec<u8>>,
    last_modified: u64,
}

/// Result of a CreateMultipartUpload operation.
#[derive(Debug)]
pub struct CreateMultipartUploadResult {
    pub upload_id: String,
}

/// A single part entry in a CompleteMultipartUpload request.
#[derive(Debug, Clone)]
pub struct CompletePart {
    pub part_number: u32,
    pub etag: String,
    /// Per-part checksum from the request XML: (algorithm implied by element name, base64 value).
    pub checksum: Option<(ChecksumAlgorithm, String)>,
}

/// Result of a CompleteMultipartUpload operation.
#[derive(Debug)]
pub struct CompleteMultipartUploadResult {
    pub etag: String,
    pub version_id: u64,
    /// Object-level checksum algorithm (if configured).
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    /// Object-level checksum type.
    pub checksum_type: Option<ChecksumType>,
    /// Object-level checksum (base64-encoded).
    pub checksum_value: Option<String>,
}

/// Minimum part size for non-final parts (5 MiB).
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Entry in a ListParts result.
#[derive(Debug, Clone)]
pub struct PartEntry {
    pub part_number: u32,
    pub size: u64,
    pub etag: String,
    pub last_modified: u64,
    /// Base64-encoded checksum for this part (None if no checksum).
    pub checksum: Option<String>,
}

/// Result of a ListParts operation.
#[derive(Debug)]
pub struct ListPartsResult {
    pub parts: Vec<PartEntry>,
    pub is_truncated: bool,
    pub next_part_number_marker: Option<u32>,
    /// Upload-level checksum algorithm.
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    /// Upload-level checksum type.
    pub checksum_type: Option<ChecksumType>,
}

/// Entry in a ListMultipartUploads result.
#[derive(Debug, Clone)]
pub struct MultipartUploadEntry {
    pub key: String,
    pub upload_id: String,
    pub initiated: u64,
}

/// Result of a ListMultipartUploads operation.
#[derive(Debug)]
pub struct ListMultipartUploadsResult {
    pub uploads: Vec<MultipartUploadEntry>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
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
        // Check emptiness: list all object versions (including delete markers)
        // and multipart uploads across all PGs.
        for &pg_id in self.storage_node.pg_ids() {
            let pg = self.storage_node.get_pg(pg_id)?;
            let resp = pg.list_object_versions(&ListObjectVersionsReq {
                bucket: name.to_string(),
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                max_keys: 1,
            })?;
            if !resp.versions.is_empty() {
                return Err(ServerError::BucketNotEmpty);
            }
            let mpu_resp = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                bucket: name.to_string(),
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1,
            })?;
            if !mpu_resp.uploads.is_empty() {
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
            data_layout: None,
            parts_count: None,
            metadata_blob: None,
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

            // Reject delete markers — they are not copyable objects.
            if src_record.status == 1 {
                return Err(ServerError::ObjectNotFound {
                    bucket: src_bucket.to_string(),
                    key: src_key.to_string(),
                });
            }

            let src_etag = format_object_etag(
                &src_record.etag,
                src_record.etag_kind,
                src_record.parts_count,
            );
            check_copy_source_conditions(src_cond, &src_etag, src_record.last_modified)?;

            let not_found = |e: ServerError| match e {
                ServerError::Store(storage::StoreError::NotFound) => ServerError::ObjectNotFound {
                    bucket: src_bucket.to_string(),
                    key: src_key.to_string(),
                },
                other => other,
            };

            if src_record.data_layout == DataLayout::MultipartManifest {
                // Multipart source: metadata from row, data from parts.
                let meta_pg = pgs.meta();
                let obj_parts = meta_pg
                    .get_object_parts(src_bucket, src_key, src_record.version_id)
                    .map_err(ServerError::Metadata)?;
                drop(pgs);

                let data = if src_record.size == 0 {
                    vec![]
                } else {
                    self.read_multipart_range(
                        src_bucket,
                        src_key,
                        &obj_parts,
                        0,
                        src_record.size as usize - 1,
                    )
                    .map_err(not_found)?
                };

                let metadata = src_record
                    .metadata_blob
                    .as_ref()
                    .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
                    .transpose()?
                    .unwrap_or_default();

                (metadata, data)
            } else {
                // Inline legacy source.
                let src_shard_pg = pgs.shard();
                let src_etag_crc = etag_bytes_to_crc64(&src_record.etag).unwrap_or(0);
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
                    .map_err(not_found)?;

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
            }
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

    /// Delete all shards for a list of object parts.
    fn delete_part_shards(&self, parts: &[ObjectPartRecord]) -> Result<(), ServerError> {
        for part in parts {
            let pg = self.storage_node.get_pg(part.shard_pg_id)?;
            let total = part.ec_k as usize + part.ec_m as usize;
            for i in 0..total {
                let shard_key = ShardKey::new(&part.part_okh, part.part_vid, i as u8);
                pg.delete_shard(&shard_key)?;
            }
        }
        Ok(())
    }

    /// Read a full part's data from its shard PG.
    ///
    /// Uses the part's `shard_pg_id`, `part_okh`, `part_vid`, and EC config
    /// to locate and reconstruct the part data.
    fn read_part_data(&self, part: &ObjectPartRecord) -> Result<Vec<u8>, ServerError> {
        let pg = self.storage_node.get_pg(part.shard_pg_id)?;
        let k = part.ec_k as usize;
        let m = part.ec_m as usize;

        // Compute shard size from part size and EC config.
        let padded = (part.size as usize).div_ceil(k) * k;
        let shard_size = padded / k;

        if shard_size == 0 {
            return Ok(vec![]);
        }

        // Read all k data shards (indices 0..k).
        let needed: Vec<usize> = (0..k).collect();
        let mut all_shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(k + m);
        let mut present_count = 0;

        for i in 0..(k + m) {
            let shard_key = ShardKey::new(&part.part_okh, part.part_vid, i as u8);
            match pg.read_shard(&shard_key) {
                Ok(sd) => {
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

        // Check if all data shards are present (happy path).
        let all_data_present = (0..k).all(|i| all_shards[i].is_some());
        if !all_data_present {
            // EC reconstruct missing data shards.
            let missing_needed: Vec<usize> = needed
                .iter()
                .copied()
                .filter(|&i| all_shards[i].is_none())
                .collect();

            let present_indices: Vec<usize> =
                (0..(k + m)).filter(|&i| all_shards[i].is_some()).collect();
            let present_refs: Vec<&[u8]> = present_indices
                .iter()
                .map(|&i| all_shards[i].as_ref().unwrap().as_slice())
                .collect();

            let tmp_codec;
            let codec = if part.ec_k == self.ec_config.data_shards
                && part.ec_m == self.ec_config.parity_shards
            {
                &self.ec_codec
            } else {
                let ec_config = EcConfig::new(part.ec_k, part.ec_m)?;
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

        // Concatenate data shards and truncate to actual part size.
        let mut buf = Vec::with_capacity(padded);
        for shard in all_shards.iter().take(k) {
            buf.extend_from_slice(shard.as_ref().unwrap());
        }
        buf.truncate(part.size as usize);
        Ok(buf)
    }

    /// Read a byte range from a multipart object by traversing its part manifest.
    ///
    /// Maps [start, end] (inclusive) to the relevant parts, reads each,
    /// and concatenates the needed slices.
    fn read_multipart_range(
        &self,
        bucket: &str,
        key: &str,
        parts: &[ObjectPartRecord],
        start: usize,
        end: usize,
    ) -> Result<Vec<u8>, ServerError> {
        let total_len = end - start + 1;
        let mut result = Vec::with_capacity(total_len);
        let mut offset: usize = 0;

        for part in parts {
            let part_start = offset;
            let part_end = offset + part.size as usize; // exclusive

            if part_start > end {
                break; // Past the requested range.
            }
            if part.size == 0 || part_end <= start {
                offset = part_end;
                continue; // Zero-size or before the requested range.
            }

            // This part overlaps with [start, end].
            let slice_start = start.saturating_sub(part_start);
            let slice_end = if end < part_end - 1 {
                end - part_start
            } else {
                part.size as usize - 1
            };

            let data = self.read_part_data(part)?;
            result.extend_from_slice(&data[slice_start..=slice_end]);
            offset = part_end;
        }

        // Verify manifest covered the full requested range.
        if result.len() != total_len {
            return Err(ServerError::IntegrityError {
                bucket: bucket.to_string(),
                key: key.to_string(),
                expected: total_len as u64,
                actual: result.len() as u64,
            });
        }

        Ok(result)
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

        let etag_str = format_object_etag(&record.etag, record.etag_kind, record.parts_count);
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        if record.data_layout == DataLayout::MultipartManifest {
            // Multipart: metadata is in object row, data spans multiple parts.
            let meta_pg = pgs.meta();
            let obj_parts = meta_pg
                .get_object_parts(bucket, key, record.version_id)
                .map_err(ServerError::Metadata)?;
            drop(pgs);

            let data = if record.size == 0 {
                vec![]
            } else {
                self.read_multipart_range(bucket, key, &obj_parts, 0, record.size as usize - 1)
                    .map_err(|e| match e {
                        ServerError::Store(storage::StoreError::NotFound) => {
                            ServerError::ObjectNotFound {
                                bucket: bucket.to_string(),
                                key: key.to_string(),
                            }
                        }
                        other => other,
                    })?
            };

            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

            Ok(GetObjectResult {
                data,
                metadata,
                etag: etag_str,
                size: record.size,
                last_modified: record.last_modified,
                version_id: record.version_id,
                tags: record.tags,
            })
        } else {
            // Inline legacy: metadata + user data stored as single blob in shards.
            let okh = object_key_hash(bucket, key);
            let object_version_id = record.version_id;
            let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
            let shard_pg = pgs.shard();

            let total = record.total_size as usize;
            let data = self
                .read_range(shard_pg, &okh, object_version_id, &record, 0, total - 1)
                .map_err(|e| match e {
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        }
                    }
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
                etag: etag_str,
                size: record.size,
                last_modified: record.last_modified,
                version_id: record.version_id,
                tags: record.tags,
            })
        }
    }

    /// Retrieve a single part of an object by part number.
    ///
    /// For multipart objects, returns the data for the specified part along with
    /// its checksum and byte range within the full object.
    /// For non-multipart objects, `part_number == 1` returns the full body.
    pub fn get_object_part(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<u64>,
        part_number: u32,
        cond: &ReadCondition,
    ) -> Result<GetObjectPartResult, ServerError> {
        let LockedReadObject { record, pgs } =
            self.lock_object_pgs_for_read(bucket, key, version_id)?;

        if record.status == 1 {
            return Err(ServerError::DeleteMarkerHit {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        let etag_str = format_object_etag(&record.etag, record.etag_kind, record.parts_count);
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        if record.data_layout == DataLayout::MultipartManifest {
            let meta_pg = pgs.meta();
            let obj_parts = meta_pg
                .get_object_parts(bucket, key, record.version_id)
                .map_err(ServerError::Metadata)?;
            drop(pgs);

            // Find the requested part
            let part = obj_parts
                .iter()
                .find(|p| p.part_number == part_number)
                .ok_or(ServerError::InvalidPart { part_number })?;

            let data = self.read_part_data(part).map_err(|e| match e {
                ServerError::Store(storage::StoreError::NotFound) => ServerError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                },
                other => other,
            })?;

            // Compute byte offset of this part within the full object
            let part_start: u64 = obj_parts
                .iter()
                .take_while(|p| p.part_number < part_number)
                .map(|p| p.size)
                .sum();
            let part_end = part_start + part.size.saturating_sub(1);

            // Decode per-part checksum
            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

            let checksum = if let Some(raw) = &part.checksum {
                use base64::Engine;
                // Look up algorithm from object metadata
                metadata
                    .get("x-amz-checksum-algorithm")
                    .and_then(ChecksumAlgorithm::from_str)
                    .map(|algo| {
                        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
                        (algo.header_name().to_string(), b64)
                    })
            } else {
                None
            };

            Ok(GetObjectPartResult {
                data,
                metadata,
                etag: etag_str,
                size: record.size,
                last_modified: record.last_modified,
                part_start,
                part_end,
                parts_count: obj_parts.len() as u32,
                version_id: record.version_id,
                tags: record.tags,
                checksum,
            })
        } else {
            // Non-multipart: only partNumber=1 is valid
            if part_number != 1 {
                return Err(ServerError::InvalidPart { part_number });
            }

            // Read full object data (same as get_object inline path)
            let okh = object_key_hash(bucket, key);
            let object_version_id = record.version_id;
            let etag_crc = etag_bytes_to_crc64(&record.etag).unwrap_or(0);
            let shard_pg = pgs.shard();

            let total = record.total_size as usize;
            let raw = self
                .read_range(shard_pg, &okh, object_version_id, &record, 0, total - 1)
                .map_err(|e| match e {
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        }
                    }
                    other => other,
                })?;

            let actual_crc = crc64::checksum(&raw);
            if actual_crc != etag_crc {
                return Err(ServerError::IntegrityError {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    expected: etag_crc,
                    actual: actual_crc,
                });
            }

            let metadata_size = (record.total_size - record.size) as usize;
            let (metadata, _) = MetadataBlob::deserialize(&raw[..metadata_size])?;
            let user_data = raw[metadata_size..].to_vec();

            Ok(GetObjectPartResult {
                data: user_data,
                metadata,
                etag: etag_str,
                size: record.size,
                last_modified: record.last_modified,
                part_start: 0,
                part_end: record.size.saturating_sub(1),
                parts_count: 1,
                version_id: record.version_id,
                tags: record.tags,
                checksum: None,
            })
        }
    }

    /// Head a single part of an object by part number (no body).
    pub fn head_object_part(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<u64>,
        part_number: u32,
        cond: &ReadCondition,
    ) -> Result<HeadObjectPartResult, ServerError> {
        let LockedReadObject { record, pgs } =
            self.lock_object_pgs_for_read(bucket, key, version_id)?;

        if record.status == 1 {
            return Err(ServerError::DeleteMarkerHit {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        let etag_str = format_object_etag(&record.etag, record.etag_kind, record.parts_count);
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        if record.data_layout == DataLayout::MultipartManifest {
            let meta_pg = pgs.meta();
            let obj_parts = meta_pg
                .get_object_parts(bucket, key, record.version_id)
                .map_err(ServerError::Metadata)?;
            drop(pgs);

            let part = obj_parts
                .iter()
                .find(|p| p.part_number == part_number)
                .ok_or(ServerError::InvalidPart { part_number })?;

            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

            let checksum = if let Some(raw) = &part.checksum {
                use base64::Engine;
                metadata
                    .get("x-amz-checksum-algorithm")
                    .and_then(ChecksumAlgorithm::from_str)
                    .map(|algo| {
                        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
                        (algo.header_name().to_string(), b64)
                    })
            } else {
                None
            };

            Ok(HeadObjectPartResult {
                metadata,
                etag: etag_str,
                part_size: part.size,
                total_size: record.size,
                last_modified: record.last_modified,
                parts_count: obj_parts.len() as u32,
                version_id: record.version_id,
                tags: record.tags,
                checksum,
            })
        } else {
            if part_number != 1 {
                return Err(ServerError::InvalidPart { part_number });
            }

            let metadata = {
                let okh = object_key_hash(bucket, key);
                let object_version_id = record.version_id;
                let shard_pg = pgs.shard();
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
                        ServerError::Store(storage::StoreError::NotFound) => {
                            ServerError::ObjectNotFound {
                                bucket: bucket.to_string(),
                                key: key.to_string(),
                            }
                        }
                        other => other,
                    })?;
                let (m, _) = MetadataBlob::deserialize(&data)?;
                m
            };

            Ok(HeadObjectPartResult {
                metadata,
                etag: etag_str,
                part_size: record.size,
                total_size: record.size,
                last_modified: record.last_modified,
                parts_count: 1,
                version_id: record.version_id,
                tags: record.tags,
                checksum: None,
            })
        }
    }

    /// Head object: returns metadata without body.
    ///
    /// For inline objects, reads only the shards covering the metadata blob.
    /// For multipart objects, metadata is in the object row (no shard read).
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

        let etag_str = format_object_etag(&record.etag, record.etag_kind, record.parts_count);
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        let metadata = if record.data_layout == DataLayout::MultipartManifest {
            // Multipart: metadata stored in object row, no shard read needed.
            record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default()
        } else {
            // Inline legacy: read metadata from shard data.
            let okh = object_key_hash(bucket, key);
            let object_version_id = record.version_id;
            let shard_pg = pgs.shard();

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
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        }
                    }
                    other => other,
                })?;

            let (m, _) = MetadataBlob::deserialize(&data)?;
            m
        };

        Ok(HeadObjectResult {
            metadata,
            etag: etag_str,
            size: record.size,
            last_modified: record.last_modified,
            version_id: record.version_id,
            tags: record.tags,
        })
    }

    /// Retrieve object attributes, optionally including multipart ObjectParts
    /// with pagination support.
    pub fn get_object_attributes(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<u64>,
        cond: &ReadCondition,
        want_parts: bool,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<GetObjectAttributesResult, ServerError> {
        let LockedReadObject { record, pgs } =
            self.lock_object_pgs_for_read(bucket, key, version_id)?;

        if record.status == 1 {
            return Err(ServerError::DeleteMarkerHit {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        let etag_str = format_object_etag(&record.etag, record.etag_kind, record.parts_count);
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        let metadata = if record.data_layout == DataLayout::MultipartManifest {
            record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default()
        } else {
            let okh = object_key_hash(bucket, key);
            let object_version_id = record.version_id;
            let shard_pg = pgs.shard();

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
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: bucket.to_string(),
                            key: key.to_string(),
                        }
                    }
                    other => other,
                })?;

            let (m, _) = MetadataBlob::deserialize(&data)?;
            m
        };

        let object_parts = if want_parts && record.data_layout == DataLayout::MultipartManifest {
            // Check if this multipart upload used checksums
            let has_checksum = metadata.get("x-amz-checksum-algorithm").is_some();

            if has_checksum {
                // Checksummed multipart: full detail with parts, pagination
                let meta_pg = pgs.meta();
                let all_parts = meta_pg.get_object_parts(bucket, key, record.version_id)?;
                let total_parts_count = all_parts.len() as u32;
                let marker = part_number_marker.unwrap_or(0);

                let filtered: Vec<_> = all_parts
                    .into_iter()
                    .filter(|p| p.part_number > marker)
                    .collect();

                let is_truncated = max_parts > 0 && filtered.len() > max_parts as usize;
                let take_count = (max_parts as usize).min(filtered.len());
                let page: Vec<ObjectPartEntry> = filtered
                    .into_iter()
                    .take(take_count)
                    .map(|p| {
                        use base64::Engine;
                        let checksum = p
                            .checksum
                            .as_ref()
                            .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes));
                        ObjectPartEntry {
                            part_number: p.part_number,
                            size: p.size,
                            checksum,
                        }
                    })
                    .collect();

                let next_part_number_marker = if !page.is_empty() {
                    page.last().map(|p| p.part_number)
                } else {
                    Some(marker)
                };

                Some(ObjectPartsInfo {
                    total_parts_count,
                    has_detail: true,
                    parts: page,
                    is_truncated,
                    next_part_number_marker,
                    max_parts,
                    part_number_marker: marker,
                })
            } else {
                // Non-checksummed multipart: only PartsCount
                let meta_pg = pgs.meta();
                let all_parts = meta_pg.get_object_parts(bucket, key, record.version_id)?;
                let total_parts_count = all_parts.len() as u32;

                Some(ObjectPartsInfo {
                    total_parts_count,
                    has_detail: false,
                    parts: Vec::new(),
                    is_truncated: false,
                    next_part_number_marker: None,
                    max_parts,
                    part_number_marker: part_number_marker.unwrap_or(0),
                })
            }
        } else {
            None
        };

        Ok(GetObjectAttributesResult {
            metadata,
            etag: etag_str,
            size: record.size,
            last_modified: record.last_modified,
            version_id: record.version_id,
            object_parts,
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

        let etag_str = format_object_etag(&record.etag, record.etag_kind, record.parts_count);
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        // Resolve byte range against user data size
        let (user_start, user_end) =
            range
                .resolve(record.size)
                .ok_or(ServerError::InvalidRange {
                    total_size: record.size,
                })?;

        let not_found = |e: ServerError| match e {
            ServerError::Store(storage::StoreError::NotFound) => ServerError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            other => other,
        };

        let (metadata, user_data) = if record.data_layout == DataLayout::MultipartManifest {
            // Multipart: metadata from object row, data spans parts.
            let meta_pg = pgs.meta();
            let obj_parts = meta_pg
                .get_object_parts(bucket, key, record.version_id)
                .map_err(ServerError::Metadata)?;
            drop(pgs);

            let data = self
                .read_multipart_range(
                    bucket,
                    key,
                    &obj_parts,
                    user_start as usize,
                    user_end as usize,
                )
                .map_err(not_found)?;

            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

            (metadata, data)
        } else {
            // Inline legacy: metadata + user data in shards.
            let okh = object_key_hash(bucket, key);
            let object_version_id = record.version_id;
            let shard_pg = pgs.shard();

            let metadata_size = (record.total_size - record.size) as usize;

            let meta_data = self
                .read_range(
                    shard_pg,
                    &okh,
                    object_version_id,
                    &record,
                    0,
                    metadata_size - 1,
                )
                .map_err(not_found)?;
            let (metadata, _) = MetadataBlob::deserialize(&meta_data)?;

            let blob_start = metadata_size + user_start as usize;
            let blob_end = metadata_size + user_end as usize;
            let data = self
                .read_range(
                    shard_pg,
                    &okh,
                    object_version_id,
                    &record,
                    blob_start,
                    blob_end,
                )
                .map_err(not_found)?;

            (metadata, data)
        };

        Ok(GetObjectRangeResult {
            data: user_data,
            metadata,
            etag: etag_str,
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

                // Check delete conditions
                if !cond.is_empty() {
                    let etag_str =
                        format_object_etag(&record.etag, record.etag_kind, record.parts_count);
                    check_delete_conditions(cond, &etag_str)?;
                }

                if record.data_layout == DataLayout::MultipartManifest {
                    // Multipart: collect parts, delete metadata under lock,
                    // then delete part shards after releasing the lock.
                    let obj_parts = meta_pg
                        .get_object_parts(bucket, key, record.version_id)
                        .map_err(ServerError::Metadata)?;
                    meta_pg.delete_object_parts(bucket, key, record.version_id)?;
                    meta_pg.delete_object_meta(bucket, key)?;
                    drop(pgs);
                    self.delete_part_shards(&obj_parts)?;
                } else {
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
                }

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
                    if record.data_layout == DataLayout::MultipartManifest {
                        let obj_parts = meta_pg
                            .get_object_parts(bucket, key, vid)
                            .map_err(ServerError::Metadata)?;
                        meta_pg.delete_object_parts(bucket, key, vid)?;
                        meta_pg.delete_object_version(bucket, key, vid)?;
                        drop(pgs);
                        self.delete_part_shards(&obj_parts)?;

                        return Ok(DeleteObjectResult {
                            version_id: vid,
                            delete_marker: false,
                        });
                    }

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
                    data_layout: None,
                    parts_count: None,
                    metadata_blob: None,
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
                        objects.push(ListEntry {
                            key: record.key.clone(),
                            size: record.size,
                            etag: format_object_etag(
                                &record.etag,
                                record.etag_kind,
                                record.parts_count,
                            ),
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
                    objects.push(ListEntry {
                        key: record.key.clone(),
                        size: record.size,
                        etag: format_object_etag(
                            &record.etag,
                            record.etag_kind,
                            record.parts_count,
                        ),
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

            versions.push(VersionEntry {
                key: record.key.clone(),
                version_id: record.version_id,
                is_latest,
                size: record.size,
                etag: format_object_etag(&record.etag, record.etag_kind, record.parts_count),
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

    // ── Multipart upload operations ───────────────────────────────────

    /// Initiate a multipart upload.
    ///
    /// Generates a random upload ID, serializes the metadata blob, and
    /// inserts a new multipart upload record in the metadata PG for (bucket, key).
    pub fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        metadata: &MetadataBlob,
        checksum_algorithm: Option<ChecksumAlgorithm>,
        checksum_type: Option<ChecksumType>,
    ) -> Result<CreateMultipartUploadResult, ServerError> {
        let bucket_info = self.head_bucket(bucket)?;

        // Generate 16 random bytes → 32-char hex upload ID.
        let rng = ring::rand::SystemRandom::new();
        let mut id_bytes = [0u8; 16];
        ring::rand::SecureRandom::fill(&rng, &mut id_bytes).map_err(|_| {
            ServerError::InternalError {
                reason: "failed to generate upload ID".to_string(),
            }
        })?;
        let upload_id = id_bytes.iter().fold(String::with_capacity(32), |mut s, b| {
            use std::fmt::Write;
            write!(s, "{b:02x}").unwrap();
            s
        });

        let metadata_blob = metadata.serialize()?;

        // Lock metadata PG and insert upload record.
        let meta_pg_id = derive_pg(bucket, key, self.pg_count);
        let pg = self.storage_node.get_pg(meta_pg_id)?;
        pg.create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: upload_id.clone(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            metadata_blob,
            owner_principal: Some(bucket_info.owner_principal),
            checksum_algorithm,
            checksum_type,
        })?;

        Ok(CreateMultipartUploadResult { upload_id })
    }

    /// Upload a part to an in-progress multipart upload.
    ///
    /// Validates part number, resolves the upload, EC-encodes the data,
    /// writes shards, upserts the part record, and best-effort deletes
    /// any prior generation's shards.
    pub fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
        claimed_checksum: Option<(ChecksumAlgorithm, &str)>,
    ) -> Result<UploadPartResult, ServerError> {
        let inner =
            self.write_part_inner(bucket, key, upload_id, part_number, data, claimed_checksum)?;
        Ok(UploadPartResult {
            etag: inner.etag,
            checksum_algorithm: inner.checksum_algorithm,
            checksum_bytes: inner.checksum_bytes,
        })
    }

    /// Copy a byte range from an existing object as a multipart upload part.
    #[allow(clippy::too_many_arguments)]
    pub fn upload_part_copy(
        &self,
        src_bucket: &str,
        src_key: &str,
        src_version_id: Option<u64>,
        dst_bucket: &str,
        dst_key: &str,
        upload_id: &str,
        part_number: u32,
        src_cond: &ReadCondition,
        copy_source_range: Option<(u64, u64)>,
    ) -> Result<UploadPartCopyResult, ServerError> {
        // Phase 1: Read source object (only the needed range)
        let source_data = {
            let LockedReadObject {
                record: src_record,
                pgs,
            } = self.lock_object_pgs_for_read(src_bucket, src_key, src_version_id)?;

            // Reject delete markers — they are not copyable objects.
            if src_record.status == 1 {
                return Err(ServerError::ObjectNotFound {
                    bucket: src_bucket.to_string(),
                    key: src_key.to_string(),
                });
            }

            let src_etag = format_object_etag(
                &src_record.etag,
                src_record.etag_kind,
                src_record.parts_count,
            );
            check_copy_source_conditions(src_cond, &src_etag, src_record.last_modified)?;

            let source_size = src_record.size;

            // Validate range against source size up front.
            // AWS returns InvalidArgument (400) for out-of-bounds copy-source-range.
            if let Some((_, end)) = copy_source_range {
                if end >= source_size {
                    return Err(ServerError::InvalidArgument {
                        reason: format!(
                            "Range specified is not valid for source object of size: {source_size}"
                        ),
                    });
                }
            }

            let (read_start, read_end) =
                copy_source_range.unwrap_or((0, source_size.saturating_sub(1)));

            let not_found = |e: ServerError| match e {
                ServerError::Store(storage::StoreError::NotFound) => ServerError::ObjectNotFound {
                    bucket: src_bucket.to_string(),
                    key: src_key.to_string(),
                },
                other => other,
            };

            if source_size == 0 {
                vec![]
            } else if src_record.data_layout == DataLayout::MultipartManifest {
                let meta_pg = pgs.meta();
                let obj_parts = meta_pg
                    .get_object_parts(src_bucket, src_key, src_record.version_id)
                    .map_err(ServerError::Metadata)?;
                drop(pgs);

                self.read_multipart_range(
                    src_bucket,
                    src_key,
                    &obj_parts,
                    read_start as usize,
                    read_end as usize,
                )
                .map_err(not_found)?
            } else {
                let src_shard_pg = pgs.shard();
                let src_okh = object_key_hash(src_bucket, src_key);
                let src_version_id = src_record.version_id;
                let metadata_size = (src_record.total_size - src_record.size) as usize;

                self.read_range(
                    src_shard_pg,
                    &src_okh,
                    src_version_id,
                    &src_record,
                    metadata_size + read_start as usize,
                    metadata_size + read_end as usize,
                )
                .map_err(not_found)?
            }
        }; // source locks dropped here

        let part_data = &source_data;

        // Phase 3: Write part data (no claimed checksum for copy)
        let inner =
            self.write_part_inner(dst_bucket, dst_key, upload_id, part_number, part_data, None)?;
        Ok(UploadPartCopyResult {
            etag: inner.etag,
            last_modified: inner.last_modified,
        })
    }

    /// Shared implementation for writing a multipart part.
    ///
    /// Validates part number, locks PGs, EC-encodes data, writes shards,
    /// upserts part metadata, and cleans up prior generations.
    fn write_part_inner(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
        claimed_checksum: Option<(ChecksumAlgorithm, &str)>,
    ) -> Result<WritePartInnerResult, ServerError> {
        // 1. Validate part number range [1, 10000].
        if part_number == 0 || part_number > 10_000 {
            return Err(ServerError::InvalidArgument {
                reason: format!("part number must be between 1 and 10000, got {part_number}"),
            });
        }

        // 2. Lock meta PG and shard PG in global ascending order.
        //
        //    The shard PG depends on the generation, which is read from metadata.
        //    We use the same loop-and-revalidate pattern as lock_object_pgs_for_write:
        //    if meta_pg_id > shard_pg_id, drop, relock in order, and re-read.
        let meta_pg_id = derive_pg(bucket, key, self.pg_count);

        let (meta_pg, shard_guard, generation, _shard_pg_id, upload_checksum_algo) = loop {
            let meta_pg = self.storage_node.get_pg(meta_pg_id)?;

            // Validate upload exists, belongs to this bucket/key, and is InProgress.
            let upload = meta_pg.get_multipart_upload(upload_id)?;
            if upload.bucket != bucket || upload.key != key {
                return Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
            if upload.state != UploadState::InProgress {
                return Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
            let upload_algo = upload.checksum_algorithm;

            // Determine next generation for this part number.
            let generation = match meta_pg.get_multipart_part(upload_id, part_number) {
                Ok(existing) => existing.generation + 1,
                Err(storage::MetadataError::PartNotFound { .. }) => 0,
                Err(e) => return Err(ServerError::Metadata(e)),
            };

            let shard_pg_id = derive_pg_shards(
                &format!("mpu/{upload_id}"),
                &format!("{part_number}/{generation}"),
                generation as u64,
                self.pg_count,
            );

            if shard_pg_id == meta_pg_id {
                break (meta_pg, None, generation, shard_pg_id, upload_algo);
            }

            if meta_pg_id < shard_pg_id {
                // Already in ascending order.
                let shard_guard = self.storage_node.get_pg(shard_pg_id)?;
                break (
                    meta_pg,
                    Some(shard_guard),
                    generation,
                    shard_pg_id,
                    upload_algo,
                );
            }

            // Out of order: drop meta_pg, relock both in ascending order, revalidate.
            drop(meta_pg);
            let (meta_pg, shard_guard) = self.storage_node.lock_two_pgs(meta_pg_id, shard_pg_id)?;

            let upload = meta_pg.get_multipart_upload(upload_id)?;
            if upload.bucket != bucket || upload.key != key {
                return Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
            if upload.state != UploadState::InProgress {
                return Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
            let upload_algo = upload.checksum_algorithm;

            let generation = match meta_pg.get_multipart_part(upload_id, part_number) {
                Ok(existing) => existing.generation + 1,
                Err(storage::MetadataError::PartNotFound { .. }) => 0,
                Err(e) => return Err(ServerError::Metadata(e)),
            };

            let verify_shard_pg_id = derive_pg_shards(
                &format!("mpu/{upload_id}"),
                &format!("{part_number}/{generation}"),
                generation as u64,
                self.pg_count,
            );

            // Generation changed while relocking — shard PG may differ. Retry.
            if verify_shard_pg_id != shard_pg_id {
                continue;
            }

            break (meta_pg, shard_guard, generation, shard_pg_id, upload_algo);
        };

        // 3. Validate and compute part checksum.
        //    The upload's checksum_algorithm is the single source of truth.
        //    Parts may only carry a checksum if the upload was configured with one,
        //    and it must match. This prevents untagged raw bytes from being stored.
        let claimed_algo = claimed_checksum.as_ref().map(|(a, _)| *a);
        let effective_algo = match (upload_checksum_algo, claimed_algo) {
            (Some(upload_algo), Some(part_algo)) if upload_algo != part_algo => {
                return Err(ServerError::InvalidRequest {
                    reason: format!(
                        "checksum algorithm mismatch: upload configured with {} but part sent {}",
                        upload_algo.as_str(),
                        part_algo.as_str()
                    ),
                });
            }
            (Some(algo), _) => Some(algo),
            // AWS SDK v2+ sends CRC32 by default on all requests. Accept the
            // checksum for verification even when the upload has no algorithm;
            // it won't contribute to the object-level checksum.
            (None, Some(part_algo)) => Some(part_algo),
            (None, None) => None,
        };

        let checksum_bytes = effective_algo.map(|algo| compute_checksum(algo, data));

        // Verify claimed checksum value if present.
        if let (Some((_, claimed_b64)), Some(ref actual)) = (&claimed_checksum, &checksum_bytes) {
            use base64::Engine;
            let actual_b64 = base64::engine::general_purpose::STANDARD.encode(actual);
            if *claimed_b64 != actual_b64 {
                return Err(ServerError::InvalidRequest {
                    reason: "checksum mismatch".to_string(),
                });
            }
        }

        // 4. Compute part identity.
        let part_okh = part_key_hash(upload_id, part_number, generation);
        let part_vid = generation as u64;

        // 5. EC-encode part data (no metadata blob for parts — raw data only).
        let etag_crc = crc64::checksum(data);

        let k = self.ec_config.data_shards as usize;
        let m = self.ec_config.parity_shards as usize;
        let mut padded = data.to_vec();
        let remainder = padded.len() % k;
        if remainder != 0 {
            padded.resize(padded.len() + (k - remainder), 0);
        }

        let shard_size = padded.len() / k;
        let data_shards: Vec<&[u8]> = (0..k)
            .map(|i| &padded[i * shard_size..(i + 1) * shard_size])
            .collect();
        let mut parity_bufs: Vec<Vec<u8>> = (0..m).map(|_| vec![0u8; shard_size]).collect();
        let mut parity_refs: Vec<&mut [u8]> =
            parity_bufs.iter_mut().map(|v| v.as_mut_slice()).collect();
        self.ec_codec.encode(&data_shards, &mut parity_refs)?;

        // 6. Write shards, with cleanup on failure.
        let shard_pg: &storage::PgStore = shard_guard.as_deref().unwrap_or(&meta_pg);
        let mut written_shards: Vec<ShardKey> = Vec::with_capacity(k + m);
        let write_result: Result<(), ServerError> = (|| {
            for i in 0..(k + m) {
                let shard_key = ShardKey::new(&part_okh, part_vid, i as u8);
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
            for shard_key in &written_shards {
                let _ = shard_pg.delete_shard(shard_key);
            }
            return Err(e);
        }

        // 7. Upsert part metadata.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let upsert_result = meta_pg.upsert_multipart_part(&MultipartPartRecord {
            upload_id: upload_id.to_string(),
            part_number,
            generation,
            size: data.len() as u64,
            etag: crc64_to_etag_bytes(etag_crc),
            etag_kind: 0,
            part_okh,
            part_vid,
            ec_k: self.ec_config.data_shards,
            ec_m: self.ec_config.parity_shards,
            last_modified: now,
            checksum: checksum_bytes.clone(),
        });

        let prev_gen = match upsert_result {
            Ok(prev) => prev,
            Err(e) => {
                // Best-effort cleanup of written shards.
                for shard_key in &written_shards {
                    let _ = shard_pg.delete_shard(shard_key);
                }
                return Err(e.into());
            }
        };

        // 8. Best-effort delete prior generation's shards.
        //    Drop all held PG guards first to avoid deadlock, since the
        //    old generation may map to any PG including those we hold.
        drop(shard_guard);
        drop(meta_pg);
        if let Some(old_gen) = prev_gen {
            let old_okh = part_key_hash(upload_id, part_number, old_gen);
            let old_vid = old_gen as u64;
            let old_shard_pg_id = derive_pg_shards(
                &format!("mpu/{upload_id}"),
                &format!("{part_number}/{old_gen}"),
                old_vid,
                self.pg_count,
            );
            if let Ok(old_pg) = self.storage_node.get_pg(old_shard_pg_id) {
                for i in 0..(k + m) {
                    let old_key = ShardKey::new(&old_okh, old_vid, i as u8);
                    let _ = old_pg.delete_shard(&old_key);
                }
            }
        }

        Ok(WritePartInnerResult {
            etag: format_etag(etag_crc),
            checksum_algorithm: effective_algo,
            checksum_bytes,
            last_modified: now,
        })
    }

    /// Complete a multipart upload, committing a manifest object.
    ///
    /// Validates the part list, checks ETags and sizes, writes the final
    /// object metadata row with `MultipartManifest` layout, commits
    /// manifest rows into `object_parts`, and deletes in-progress state.
    pub fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[CompletePart],
        claimed_checksum: Option<(ChecksumAlgorithm, &str)>,
    ) -> Result<CompleteMultipartUploadResult, ServerError> {
        // 1. Validate bucket exists and get versioning state.
        let bucket_info = self.head_bucket(bucket)?;

        // 2. Validate part list: non-empty and strictly increasing part numbers.
        if parts.is_empty() {
            return Err(ServerError::InvalidRequest {
                reason: "part list must not be empty".to_string(),
            });
        }
        for window in parts.windows(2) {
            if window[0].part_number >= window[1].part_number {
                return Err(ServerError::InvalidPartOrder);
            }
        }

        // 3. Lock meta PG and validate upload.
        let meta_pg_id = derive_pg(bucket, key, self.pg_count);
        let meta_pg = self.storage_node.get_pg(meta_pg_id)?;

        let upload = meta_pg.get_multipart_upload(upload_id)?;
        if upload.bucket != bucket || upload.key != key {
            return Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        if upload.state != UploadState::InProgress {
            return Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }

        // Resolve checksum configuration early so per-part validation can use it.
        let checksum_algo = upload.checksum_algorithm;
        let checksum_type = match (checksum_algo, upload.checksum_type) {
            (Some(algo), None) => Some(ChecksumType::default_for(algo)),
            (_, ct) => ct,
        };

        // 4. Validate all parts exist and ETags match.
        let mut part_records: Vec<MultipartPartRecord> = Vec::with_capacity(parts.len());
        for cp in parts {
            let part = match meta_pg.get_multipart_part(upload_id, cp.part_number) {
                Ok(p) => p,
                Err(storage::MetadataError::PartNotFound { .. }) => {
                    return Err(ServerError::InvalidPart {
                        part_number: cp.part_number,
                    });
                }
                Err(e) => return Err(ServerError::Metadata(e)),
            };

            let stored_etag = etag_bytes_to_crc64(&part.etag)
                .map(format_etag)
                .unwrap_or_default();
            if stored_etag != cp.etag {
                return Err(ServerError::InvalidPart {
                    part_number: cp.part_number,
                });
            }

            // When the upload has a checksum algorithm, every part must include
            // its checksum in the complete request.
            if checksum_algo.is_some() && cp.checksum.is_none() {
                return Err(ServerError::InvalidRequest {
                    reason: format!("part {} missing required checksum", cp.part_number),
                });
            }

            // Validate per-part checksum from request against stored value.
            if let Some((ref claimed_algo, ref claimed_b64)) = cp.checksum {
                // The checksum element type must match the upload's algorithm.
                if let Some(upload_algo) = checksum_algo {
                    if *claimed_algo != upload_algo {
                        return Err(ServerError::InvalidRequest {
                            reason: format!(
                                "checksum element type {} does not match upload algorithm {}",
                                claimed_algo.as_str(),
                                upload_algo.as_str()
                            ),
                        });
                    }
                }
                use base64::Engine;
                match &part.checksum {
                    Some(stored_bytes) => {
                        let stored_b64 =
                            base64::engine::general_purpose::STANDARD.encode(stored_bytes);
                        if *claimed_b64 != stored_b64 {
                            return Err(ServerError::InvalidRequest {
                                reason: "part checksum mismatch".to_string(),
                            });
                        }
                    }
                    None => {
                        // Request claims a checksum but none was stored for this part.
                        return Err(ServerError::InvalidRequest {
                            reason: "part checksum mismatch".to_string(),
                        });
                    }
                }
            }

            part_records.push(part);
        }

        // 5. Enforce part-size constraints: all non-final parts >= 5 MiB.
        if part_records.len() > 1 {
            for part in &part_records[..part_records.len() - 1] {
                if part.size < MIN_PART_SIZE {
                    return Err(ServerError::EntityTooSmall {
                        part_number: part.part_number,
                        size: part.size,
                        min: MIN_PART_SIZE,
                    });
                }
            }
        }

        // 6. Allocate version_id using existing versioning rules.
        let version_id = if bucket_info.versioning == 1 {
            meta_pg.next_version_id(bucket, key)?
        } else {
            0
        };

        // 7. Compute composite multipart ETag.
        let part_etags: Vec<&[u8]> = part_records.iter().map(|p| p.etag.as_slice()).collect();
        let (etag_bytes, etag_str) = compute_multipart_etag(&part_etags);

        // 8. Compute total object size.
        let total_size: u64 = part_records.iter().map(|p| p.size).sum();

        // 8b. Compute object-level checksum if the upload was configured with one.
        let checksum_value = if let (Some(algo), Some(ctype)) = (checksum_algo, checksum_type) {
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD;
            match ctype {
                ChecksumType::Composite => {
                    // Concatenate raw part checksums, hash them, append -N.
                    let mut concat = Vec::new();
                    for part in &part_records {
                        match &part.checksum {
                            Some(bytes) => concat.extend_from_slice(bytes),
                            None => {
                                return Err(ServerError::InvalidRequest {
                                    reason:
                                        "COMPOSITE checksum requires all parts to have checksums"
                                            .to_string(),
                                });
                            }
                        }
                    }
                    let hash = compute_checksum(algo, &concat);
                    Some(format!("{}-{}", b64.encode(&hash), part_records.len()))
                }
                ChecksumType::FullObject => {
                    // Combine part CRCs using mathematical combine.
                    match algo {
                        ChecksumAlgorithm::Crc32 => {
                            let mut combined: u32 = 0;
                            for part in &part_records {
                                let bytes = part.checksum.as_ref().ok_or_else(|| {
                                    ServerError::InvalidRequest {
                                        reason: "FULL_OBJECT checksum requires all parts to have checksums".to_string(),
                                    }
                                })?;
                                let part_crc =
                                    u32::from_be_bytes(bytes.as_slice().try_into().map_err(
                                        |_| ServerError::InvalidRequest {
                                            reason: "invalid CRC32 checksum length".to_string(),
                                        },
                                    )?);
                                combined = checksum::crc32::combine(combined, part_crc, part.size);
                            }
                            Some(b64.encode(combined.to_be_bytes()))
                        }
                        ChecksumAlgorithm::Crc32c => {
                            let mut combined: u32 = 0;
                            for part in &part_records {
                                let bytes = part.checksum.as_ref().ok_or_else(|| {
                                    ServerError::InvalidRequest {
                                        reason: "FULL_OBJECT checksum requires all parts to have checksums".to_string(),
                                    }
                                })?;
                                let part_crc =
                                    u32::from_be_bytes(bytes.as_slice().try_into().map_err(
                                        |_| ServerError::InvalidRequest {
                                            reason: "invalid CRC32C checksum length".to_string(),
                                        },
                                    )?);
                                combined = checksum::crc32c::combine(combined, part_crc, part.size);
                            }
                            Some(b64.encode(combined.to_be_bytes()))
                        }
                        ChecksumAlgorithm::Crc64nvme => {
                            let mut combined: u64 = 0;
                            for part in &part_records {
                                let bytes = part.checksum.as_ref().ok_or_else(|| {
                                    ServerError::InvalidRequest {
                                        reason: "FULL_OBJECT checksum requires all parts to have checksums".to_string(),
                                    }
                                })?;
                                let part_crc =
                                    u64::from_be_bytes(bytes.as_slice().try_into().map_err(
                                        |_| ServerError::InvalidRequest {
                                            reason: "invalid CRC64NVME checksum length".to_string(),
                                        },
                                    )?);
                                combined = crc64::combine(combined, part_crc, part.size);
                            }
                            Some(b64.encode(combined.to_be_bytes()))
                        }
                        // SHA algorithms don't support FULL_OBJECT for multipart.
                        // This was rejected at CreateMultipartUpload time.
                        ChecksumAlgorithm::Sha1 | ChecksumAlgorithm::Sha256 => {
                            unreachable!("SHA + FULL_OBJECT rejected at CreateMultipartUpload time")
                        }
                    }
                }
            }
        } else {
            None
        };

        // 8b'. Validate claimed object-level checksum if provided.
        if let Some((claimed_algo, claimed_value)) = claimed_checksum {
            // Algorithm of the header must match the upload's algorithm.
            match checksum_algo {
                Some(upload_algo) if claimed_algo != upload_algo => {
                    return Err(ServerError::InvalidRequest {
                        reason: format!(
                            "checksum header algorithm {} does not match upload algorithm {}",
                            claimed_algo.as_str(),
                            upload_algo.as_str()
                        ),
                    });
                }
                None => {
                    // Client sent a checksum header but upload has no checksum algorithm.
                    return Err(ServerError::InvalidRequest {
                        reason: "checksum header sent but upload has no checksum algorithm"
                            .to_string(),
                    });
                }
                _ => {}
            }
            // Value must match computed checksum.
            if let Some(ref computed) = checksum_value {
                if computed != claimed_value {
                    return Err(ServerError::InvalidRequest {
                        reason: "checksum mismatch".to_string(),
                    });
                }
            }
        }

        // 8c. Persist checksum in metadata blob.
        let mut metadata_blob_bytes = upload.metadata_blob.clone();
        if let (Some(algo), Some(ref val)) = (checksum_algo, &checksum_value) {
            let (mut blob, _) =
                crate::metadata_blob::MetadataBlob::deserialize(&metadata_blob_bytes)?;
            blob.set(algo.header_name(), val);
            blob.set("x-amz-checksum-algorithm", algo.as_str());
            if let Some(ctype) = checksum_type {
                blob.set("x-amz-checksum-type", ctype.as_str());
            }
            metadata_blob_bytes = blob.serialize().map_err(|e| ServerError::InvalidRequest {
                reason: format!("failed to serialize metadata blob: {e}"),
            })?;
        }

        // 9. Build the object metadata and manifest parts.
        let obj_req = PutObjectMetaReq {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id,
            status: 0,
            size: total_size,
            total_size,
            etag: etag_bytes,
            etag_kind: 1, // multipart-composite CRC64
            ec_k: 0,      // per-part, not per-object
            ec_m: 0,
            data_layout: Some(DataLayout::MultipartManifest),
            parts_count: Some(part_records.len() as u32),
            metadata_blob: Some(metadata_blob_bytes),
        };

        let object_parts: Vec<ObjectPartRecord> = part_records
            .iter()
            .map(|p| {
                let shard_pg_id = derive_pg_shards(
                    &format!("mpu/{}", p.upload_id),
                    &format!("{}/{}", p.part_number, p.generation),
                    p.part_vid,
                    self.pg_count,
                );
                ObjectPartRecord {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id,
                    part_number: p.part_number,
                    size: p.size,
                    etag: p.etag.clone(),
                    etag_kind: p.etag_kind,
                    part_okh: p.part_okh,
                    part_vid: p.part_vid,
                    ec_k: p.ec_k,
                    ec_m: p.ec_m,
                    shard_pg_id,
                    checksum: p.checksum.clone(),
                }
            })
            .collect();

        // 10. Atomically: transition to Completing, write object row,
        //     replace object_parts, commit manifest, delete upload+parts.
        meta_pg
            .complete_multipart_commit(upload_id, &obj_req, &object_parts)
            .map_err(ServerError::Metadata)?;

        Ok(CompleteMultipartUploadResult {
            etag: etag_str,
            version_id,
            checksum_algorithm: checksum_algo,
            checksum_type: checksum_type,
            checksum_value,
        })
    }

    /// Abort an in-progress multipart upload.
    ///
    /// Transitions to Aborting, best-effort deletes all part shard sets,
    /// then deletes the upload and part metadata rows.
    pub fn abort_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(), ServerError> {
        // 1. Lock meta PG and validate upload.
        let meta_pg_id = derive_pg(bucket, key, self.pg_count);
        let meta_pg = self.storage_node.get_pg(meta_pg_id)?;

        let upload = meta_pg.get_multipart_upload(upload_id)?;
        if upload.bucket != bucket || upload.key != key {
            return Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }

        // 2. Transition to Aborting. Allow already-Aborting for idempotence.
        //    Completing → treat as NoSuchUpload (upload is being finalized).
        match meta_pg.set_upload_state(upload_id, UploadState::Aborting) {
            Ok(()) => {}
            Err(storage::MetadataError::UploadNotInProgress { state })
                if state == UploadState::Aborting as u8 =>
            {
                // Already aborting — continue cleanup idempotently.
            }
            Err(storage::MetadataError::UploadNotInProgress { .. }) => {
                return Err(ServerError::NoSuchUpload {
                    upload_id: upload_id.to_string(),
                });
            }
            Err(e) => return Err(e.into()),
        }

        // 3. Collect all parts for shard cleanup.
        let all_parts = meta_pg
            .list_multipart_parts(&ListPartsReq {
                upload_id: upload_id.to_string(),
                part_number_marker: None,
                max_parts: u32::MAX,
            })
            .map_err(ServerError::Metadata)?;

        // 4. Drop meta PG lock before shard cleanup to avoid deadlocks.
        drop(meta_pg);

        // 5. Best-effort delete all shard sets for each part.
        for part in &all_parts.parts {
            let shard_pg_id = derive_pg_shards(
                &format!("mpu/{upload_id}"),
                &format!("{}/{}", part.part_number, part.generation),
                part.part_vid,
                self.pg_count,
            );
            if let Ok(shard_pg) = self.storage_node.get_pg(shard_pg_id) {
                let k = part.ec_k as usize;
                let m = part.ec_m as usize;
                for i in 0..(k + m) {
                    let shard_key = ShardKey::new(&part.part_okh, part.part_vid, i as u8);
                    let _ = shard_pg.delete_shard(&shard_key);
                }
            }
        }

        // 6. Re-acquire meta PG and delete upload + parts (CASCADE).
        let meta_pg = self.storage_node.get_pg(meta_pg_id)?;
        meta_pg
            .delete_multipart_upload(upload_id)
            .map_err(ServerError::Metadata)?;

        Ok(())
    }

    /// List parts of an in-progress multipart upload.
    pub fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number_marker: Option<u32>,
        max_parts: u32,
    ) -> Result<ListPartsResult, ServerError> {
        // 1. Lock meta PG and validate upload.
        let meta_pg_id = derive_pg(bucket, key, self.pg_count);
        let meta_pg = self.storage_node.get_pg(meta_pg_id)?;

        let upload = meta_pg.get_multipart_upload(upload_id)?;
        if upload.bucket != bucket || upload.key != key {
            return Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }
        if upload.state != UploadState::InProgress {
            return Err(ServerError::NoSuchUpload {
                upload_id: upload_id.to_string(),
            });
        }

        // 2. Delegate to storage layer.
        let resp = meta_pg
            .list_multipart_parts(&ListPartsReq {
                upload_id: upload_id.to_string(),
                part_number_marker,
                max_parts,
            })
            .map_err(ServerError::Metadata)?;

        // 3. Convert to coordinator result types with formatted ETags.
        let parts = resp
            .parts
            .iter()
            .map(|p| {
                use base64::Engine;
                let etag_crc = etag_bytes_to_crc64(&p.etag).unwrap_or(0);
                let checksum = p
                    .checksum
                    .as_ref()
                    .map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes));
                PartEntry {
                    part_number: p.part_number,
                    size: p.size,
                    etag: format_etag(etag_crc),
                    last_modified: p.last_modified,
                    checksum,
                }
            })
            .collect();

        Ok(ListPartsResult {
            parts,
            is_truncated: resp.is_truncated,
            next_part_number_marker: resp.next_part_number_marker,
            checksum_algorithm: upload.checksum_algorithm,
            checksum_type: upload.checksum_type,
        })
    }

    /// List in-progress multipart uploads for a bucket.
    ///
    /// Fans out across all PGs, merges results sorted by (key, upload_id),
    /// and applies pagination.
    pub fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        max_uploads: u32,
    ) -> Result<ListMultipartUploadsResult, ServerError> {
        self.head_bucket(bucket)?;

        if max_uploads == 0 {
            return Ok(ListMultipartUploadsResult {
                uploads: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_upload_id_marker: None,
            });
        }

        // Fan out to all PGs and collect results, with a hard memory cap.
        let mut all_uploads: Vec<MultipartUploadRecord> = Vec::new();
        let mut hit_record_cap = false;
        for &pg_id in self.storage_node.pg_ids() {
            let pg = self.storage_node.get_pg(pg_id)?;
            let resp = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                bucket: bucket.to_string(),
                prefix: prefix.map(|s| s.to_string()),
                key_marker: key_marker.map(|s| s.to_string()),
                upload_id_marker: upload_id_marker.map(|s| s.to_string()),
                max_uploads: max_uploads.saturating_add(1),
            })?;
            all_uploads.extend(resp.uploads);
            if all_uploads.len() >= MAX_LIST_RECORDS {
                all_uploads.truncate(MAX_LIST_RECORDS);
                hit_record_cap = true;
                break;
            }
        }

        // Sort by (key ASC, initiated_at ASC) per S3 spec, with upload_id
        // as tiebreaker for identical timestamps.
        all_uploads.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then(a.initiated_at.cmp(&b.initiated_at))
                .then(a.upload_id.cmp(&b.upload_id))
        });

        // Truncate to max_uploads + detect truncation.
        let max = max_uploads as usize;
        let is_truncated = hit_record_cap || all_uploads.len() > max;
        all_uploads.truncate(max);

        let (next_key_marker, next_upload_id_marker) = if is_truncated {
            if let Some(last) = all_uploads.last() {
                (Some(last.key.clone()), Some(last.upload_id.clone()))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        let uploads = all_uploads
            .into_iter()
            .map(|u| MultipartUploadEntry {
                key: u.key,
                upload_id: u.upload_id,
                initiated: u.initiated_at,
            })
            .collect();

        Ok(ListMultipartUploadsResult {
            uploads,
            is_truncated,
            next_key_marker,
            next_upload_id_marker,
        })
    }
}

/// Compute raw checksum bytes for the given algorithm and data.
fn compute_checksum(algo: ChecksumAlgorithm, data: &[u8]) -> Vec<u8> {
    match algo {
        ChecksumAlgorithm::Crc32 => checksum::crc32::checksum(data).to_be_bytes().to_vec(),
        ChecksumAlgorithm::Crc32c => checksum::crc32c::checksum(data).to_be_bytes().to_vec(),
        ChecksumAlgorithm::Crc64nvme => crc64::checksum(data).to_be_bytes().to_vec(),
        ChecksumAlgorithm::Sha256 => ring::digest::digest(&ring::digest::SHA256, data)
            .as_ref()
            .to_vec(),
        ChecksumAlgorithm::Sha1 => {
            ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, data)
                .as_ref()
                .to_vec()
        }
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
    const NO_DELETE: &DeleteCondition = &DeleteCondition { if_match: None };

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

    // ── Multipart upload tests ────────────────────────────────────────

    #[test]
    fn create_multipart_upload_returns_upload_id() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let result = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        // Upload ID should be 32 hex chars (16 random bytes).
        assert_eq!(result.upload_id.len(), 32);
        assert!(result.upload_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn create_multipart_upload_unique_ids() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let r1 = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();
        let r2 = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();
        assert_ne!(r1.upload_id, r2.upload_id);
    }

    #[test]
    fn create_multipart_upload_requires_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        let metadata = MetadataBlob::new();
        let err = coord
            .create_multipart_upload("no-such-bucket", "key", &metadata, None, None)
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn list_multipart_uploads_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let result = coord
            .list_multipart_uploads("bucket", None, None, None, 1000)
            .unwrap();
        assert!(result.uploads.is_empty());
        assert!(!result.is_truncated);
    }

    #[test]
    fn list_multipart_uploads_returns_created() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let r1 = coord
            .create_multipart_upload("bucket", "alpha", &metadata, None, None)
            .unwrap();
        let r2 = coord
            .create_multipart_upload("bucket", "beta", &metadata, None, None)
            .unwrap();

        let result = coord
            .list_multipart_uploads("bucket", None, None, None, 1000)
            .unwrap();
        assert_eq!(result.uploads.len(), 2);

        // Should be sorted by key ascending.
        assert_eq!(result.uploads[0].key, "alpha");
        assert_eq!(result.uploads[0].upload_id, r1.upload_id);
        assert_eq!(result.uploads[1].key, "beta");
        assert_eq!(result.uploads[1].upload_id, r2.upload_id);
        assert!(!result.is_truncated);
    }

    #[test]
    fn list_multipart_uploads_sorted_by_key_then_initiated() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        // Create two uploads for the same key.
        let r1 = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();
        let r2 = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        let result = coord
            .list_multipart_uploads("bucket", None, None, None, 1000)
            .unwrap();
        assert_eq!(result.uploads.len(), 2);

        // Both same key — sorted by initiation time (ascending).
        assert!(result.uploads[0].initiated <= result.uploads[1].initiated);
        // Both upload IDs present.
        let ids: Vec<&str> = result
            .uploads
            .iter()
            .map(|u| u.upload_id.as_str())
            .collect();
        assert!(ids.contains(&r1.upload_id.as_str()));
        assert!(ids.contains(&r2.upload_id.as_str()));
    }

    #[test]
    fn list_multipart_uploads_pagination() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        // Create 3 uploads for distinct keys so ordering is deterministic.
        coord
            .create_multipart_upload("bucket", "a", &metadata, None, None)
            .unwrap();
        coord
            .create_multipart_upload("bucket", "b", &metadata, None, None)
            .unwrap();
        coord
            .create_multipart_upload("bucket", "c", &metadata, None, None)
            .unwrap();

        // Page 1: max_uploads=2.
        let page1 = coord
            .list_multipart_uploads("bucket", None, None, None, 2)
            .unwrap();
        assert_eq!(page1.uploads.len(), 2);
        assert!(page1.is_truncated);
        assert_eq!(page1.uploads[0].key, "a");
        assert_eq!(page1.uploads[1].key, "b");
        assert!(page1.next_key_marker.is_some());
        assert!(page1.next_upload_id_marker.is_some());

        // Page 2: use markers from page 1.
        let page2 = coord
            .list_multipart_uploads(
                "bucket",
                None,
                page1.next_key_marker.as_deref(),
                page1.next_upload_id_marker.as_deref(),
                2,
            )
            .unwrap();
        assert_eq!(page2.uploads.len(), 1);
        assert!(!page2.is_truncated);
        assert_eq!(page2.uploads[0].key, "c");
    }

    #[test]
    fn list_multipart_uploads_prefix_filter() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        coord
            .create_multipart_upload("bucket", "photos/a.jpg", &metadata, None, None)
            .unwrap();
        coord
            .create_multipart_upload("bucket", "photos/b.jpg", &metadata, None, None)
            .unwrap();
        coord
            .create_multipart_upload("bucket", "docs/readme.md", &metadata, None, None)
            .unwrap();

        let result = coord
            .list_multipart_uploads("bucket", Some("photos/"), None, None, 1000)
            .unwrap();
        assert_eq!(result.uploads.len(), 2);
        assert!(result.uploads.iter().all(|u| u.key.starts_with("photos/")));
    }

    #[test]
    fn list_multipart_uploads_max_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        let result = coord
            .list_multipart_uploads("bucket", None, None, None, 0)
            .unwrap();
        assert!(result.uploads.is_empty());
        assert!(!result.is_truncated);
    }

    #[test]
    fn list_multipart_uploads_requires_bucket() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .list_multipart_uploads("no-such-bucket", None, None, None, 1000)
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn create_multipart_upload_preserves_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::from_headers(&[
            ("Content-Type", "image/png"),
            ("X-Amz-Meta-Author", "test"),
        ])
        .unwrap();

        let result = coord
            .create_multipart_upload("bucket", "photo.png", &metadata, None, None)
            .unwrap();

        // Verify we can retrieve the upload and its metadata blob is stored.
        let meta_pg_id = derive_pg("bucket", "photo.png", coord.pg_count);
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let record = pg.get_multipart_upload(&result.upload_id).unwrap();
        assert_eq!(record.bucket, "bucket");
        assert_eq!(record.key, "photo.png");

        // Deserialize and verify the metadata blob.
        let (blob, _) = MetadataBlob::deserialize(&record.metadata_blob).unwrap();
        assert_eq!(blob.get("content-type"), Some("image/png"));
        assert_eq!(blob.get("x-amz-meta-author"), Some("test"));
    }

    #[test]
    fn delete_bucket_blocked_by_multipart_uploads() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        // Bucket has no objects but has an in-progress MPU — should fail.
        let err = coord.delete_bucket("bucket").unwrap_err();
        assert!(matches!(err, ServerError::BucketNotEmpty));
    }

    #[test]
    fn no_such_upload_from_storage() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // Directly call get_multipart_upload on a PG with a bogus upload ID.
        let meta_pg_id = derive_pg("bucket", "key", coord.pg_count);
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let err: ServerError = pg.get_multipart_upload("nonexistent").unwrap_err().into();
        assert!(matches!(err, ServerError::NoSuchUpload { .. }));
        assert_eq!(err.s3_error_code(), "NoSuchUpload");
        assert_eq!(err.http_status(), 404);
    }

    #[test]
    fn list_multipart_uploads_same_key_pagination() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        // Create 3 uploads for the same key.
        let mut upload_ids = Vec::new();
        for _ in 0..3 {
            let r = coord
                .create_multipart_upload("bucket", "key", &metadata, None, None)
                .unwrap();
            upload_ids.push(r.upload_id);
        }

        // Page 1: max_uploads=2 — should get first 2 by initiation time.
        let page1 = coord
            .list_multipart_uploads("bucket", None, None, None, 2)
            .unwrap();
        assert_eq!(page1.uploads.len(), 2);
        assert!(page1.is_truncated);
        assert_eq!(page1.uploads[0].key, "key");
        assert_eq!(page1.uploads[1].key, "key");
        // Initiation time ordering.
        assert!(page1.uploads[0].initiated <= page1.uploads[1].initiated);

        // Page 2: use markers from page 1 — should get remaining upload.
        let page2 = coord
            .list_multipart_uploads(
                "bucket",
                None,
                page1.next_key_marker.as_deref(),
                page1.next_upload_id_marker.as_deref(),
                2,
            )
            .unwrap();
        assert_eq!(page2.uploads.len(), 1);
        assert!(!page2.is_truncated);
        assert_eq!(page2.uploads[0].key, "key");

        // All 3 upload IDs should be covered across both pages.
        let mut seen: Vec<String> = page1
            .uploads
            .iter()
            .chain(page2.uploads.iter())
            .map(|u| u.upload_id.clone())
            .collect();
        seen.sort();
        let mut expected = upload_ids.clone();
        expected.sort();
        assert_eq!(seen, expected);
    }

    // ── UploadPart tests ──────────────────────────────────────────────

    #[test]
    fn upload_part_first_upload() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        let result = coord
            .upload_part("bucket", "key", &create.upload_id, 1, b"hello world", None)
            .unwrap();

        // ETag should be a quoted hex CRC64.
        assert!(result.etag.starts_with('"'));
        assert!(result.etag.ends_with('"'));

        // Verify part metadata was recorded.
        let meta_pg_id = derive_pg("bucket", "key", coord.pg_count);
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let part = pg.get_multipart_part(&create.upload_id, 1).unwrap();
        assert_eq!(part.part_number, 1);
        assert_eq!(part.generation, 0);
        assert_eq!(part.size, 11); // "hello world".len()
    }

    #[test]
    fn upload_part_reupload_increments_generation() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        // First upload → generation 0.
        coord
            .upload_part("bucket", "key", &create.upload_id, 1, b"first", None)
            .unwrap();

        // Re-upload same part number → generation 1.
        let result = coord
            .upload_part("bucket", "key", &create.upload_id, 1, b"second", None)
            .unwrap();

        let meta_pg_id = derive_pg("bucket", "key", coord.pg_count);
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let part = pg.get_multipart_part(&create.upload_id, 1).unwrap();
        assert_eq!(part.generation, 1);
        assert_eq!(part.size, 6); // "second".len()

        // ETag should reflect the new data.
        let expected_crc = crc64::checksum(b"second");
        assert_eq!(result.etag, format_etag(expected_crc));
    }

    #[test]
    fn upload_part_invalid_part_number_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        let err = coord
            .upload_part("bucket", "key", &create.upload_id, 0, b"data", None)
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidArgument { .. }));
    }

    #[test]
    fn upload_part_invalid_part_number_exceeds_max() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        let err = coord
            .upload_part("bucket", "key", &create.upload_id, 10_001, b"data", None)
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidArgument { .. }));
    }

    #[test]
    fn upload_part_nonexistent_upload() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let err = coord
            .upload_part("bucket", "key", "bogus-upload-id", 1, b"data", None)
            .unwrap_err();
        assert!(matches!(err, ServerError::NoSuchUpload { .. }));
    }

    #[test]
    fn upload_part_multiple_parts() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        coord
            .upload_part("bucket", "key", &create.upload_id, 1, b"part-one", None)
            .unwrap();
        coord
            .upload_part("bucket", "key", &create.upload_id, 2, b"part-two", None)
            .unwrap();
        coord
            .upload_part("bucket", "key", &create.upload_id, 3, b"part-three", None)
            .unwrap();

        // Verify all three parts exist.
        let meta_pg_id = derive_pg("bucket", "key", coord.pg_count);
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();

        let parts_resp = pg
            .list_multipart_parts(&storage::ListPartsReq {
                upload_id: create.upload_id.clone(),
                part_number_marker: None,
                max_parts: 100,
            })
            .unwrap();
        assert_eq!(parts_resp.parts.len(), 3);
        assert_eq!(parts_resp.parts[0].part_number, 1);
        assert_eq!(parts_resp.parts[1].part_number, 2);
        assert_eq!(parts_resp.parts[2].part_number, 3);
    }

    #[test]
    fn upload_part_repeated_reupload_generations() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        // Upload same part 4 times — generation should increment each time.
        for i in 0..4u32 {
            let data = format!("version-{i}");
            coord
                .upload_part("bucket", "key", &create.upload_id, 1, data.as_bytes(), None)
                .unwrap();
        }

        let meta_pg_id = derive_pg("bucket", "key", coord.pg_count);
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let part = pg.get_multipart_part(&create.upload_id, 1).unwrap();
        assert_eq!(part.generation, 3);
        assert_eq!(part.size, "version-3".len() as u64);
    }

    #[test]
    fn upload_part_boundary_part_numbers() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        // Part 1 (min valid).
        coord
            .upload_part("bucket", "key", &create.upload_id, 1, b"a", None)
            .unwrap();
        // Part 10000 (max valid).
        coord
            .upload_part("bucket", "key", &create.upload_id, 10_000, b"z", None)
            .unwrap();

        let meta_pg_id = derive_pg("bucket", "key", coord.pg_count);
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        pg.get_multipart_part(&create.upload_id, 1).unwrap();
        pg.get_multipart_part(&create.upload_id, 10_000).unwrap();
    }

    #[test]
    fn upload_part_wrong_bucket_key_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        // Try uploading with wrong key — should be rejected even if upload_id is valid.
        let err = coord
            .upload_part("bucket", "wrong-key", &create.upload_id, 1, b"data", None)
            .unwrap_err();
        assert!(matches!(err, ServerError::NoSuchUpload { .. }));

        // Try uploading with wrong bucket.
        coord.create_bucket("other-bucket").unwrap();
        let err = coord
            .upload_part("other-bucket", "key", &create.upload_id, 1, b"data", None)
            .unwrap_err();
        assert!(matches!(err, ServerError::NoSuchUpload { .. }));
    }

    #[test]
    fn upload_part_same_part_last_writer_wins() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        // Simulate concurrent same-part uploads sequentially.
        // Each successive upload should overwrite, with generation incrementing.
        let etag1 = coord
            .upload_part("bucket", "key", &create.upload_id, 1, b"writer-A", None)
            .unwrap()
            .etag;
        let etag2 = coord
            .upload_part("bucket", "key", &create.upload_id, 1, b"writer-B", None)
            .unwrap()
            .etag;
        let etag3 = coord
            .upload_part("bucket", "key", &create.upload_id, 1, b"writer-C", None)
            .unwrap()
            .etag;

        // Each write has different data → different ETags.
        assert_ne!(etag1, etag2);
        assert_ne!(etag2, etag3);

        // Final state should reflect the last writer.
        let meta_pg_id = derive_pg("bucket", "key", coord.pg_count);
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let part = pg.get_multipart_part(&create.upload_id, 1).unwrap();
        assert_eq!(part.generation, 2); // 0, 1, 2
        assert_eq!(part.size, "writer-C".len() as u64);
        assert_eq!(format_etag(crc64::checksum(b"writer-C")), etag3);
    }

    // --- CompleteMultipartUpload tests ---

    /// Helper: create upload with given parts, returning (upload_id, vec of etags).
    fn create_upload_with_parts(
        coord: &Coordinator,
        bucket: &str,
        key: &str,
        part_data: &[(u32, &[u8])],
    ) -> (String, Vec<CompletePart>) {
        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(bucket, key, &metadata, None, None)
            .unwrap();
        let mut complete_parts = Vec::new();
        for &(part_number, data) in part_data {
            let result = coord
                .upload_part(bucket, key, &create.upload_id, part_number, data, None)
                .unwrap();
            complete_parts.push(CompletePart {
                part_number,
                etag: result.etag,
                checksum: None,
            });
        }
        (create.upload_id, complete_parts)
    }

    #[test]
    fn complete_multipart_upload_happy_path() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // Use 5MiB+ parts for non-final parts.
        let big_part = vec![0xABu8; 5 * 1024 * 1024];
        let small_last = b"final-part";

        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, &big_part), (2, small_last)]);

        let result = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap();

        // ETag should be composite format: "hex-2"
        assert!(result.etag.ends_with("-2\""), "etag = {}", result.etag);

        // Object should be visible via get_object metadata.
        let meta_pg_id = derive_pg("bucket", "key", coord.pg_count);
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let obj = pg.get_object_meta("bucket", "key").unwrap();
        assert_eq!(obj.data_layout, DataLayout::MultipartManifest);
        assert_eq!(obj.parts_count, Some(2));
        assert_eq!(obj.size, big_part.len() as u64 + small_last.len() as u64);

        // object_parts should be committed.
        let committed = pg
            .get_object_parts("bucket", "key", result.version_id)
            .unwrap();
        assert_eq!(committed.len(), 2);
        assert_eq!(committed[0].part_number, 1);
        assert_eq!(committed[1].part_number, 2);

        // Upload should be deleted.
        let err = pg.get_multipart_upload(&upload_id).unwrap_err();
        assert!(matches!(err, storage::MetadataError::NoSuchUpload { .. }));
    }

    #[test]
    fn complete_multipart_upload_missing_part() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, mut parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1"), (3, b"data3")]);

        // Request completion with part 2 which was never uploaded.
        parts.insert(
            1,
            CompletePart {
                part_number: 2,
                etag: "\"0000000000000000\"".to_string(),
                checksum: None,
            },
        );

        let err = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidPart { part_number: 2 }));
    }

    #[test]
    fn complete_multipart_upload_wrong_etag() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, mut parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);

        // Tamper with the ETag.
        parts[0].etag = "\"ffffffffffffffff\"".to_string();

        let err = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidPart { part_number: 1 }));
    }

    #[test]
    fn complete_multipart_upload_invalid_order() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1"), (2, b"data2")]);

        // Reverse the order.
        let reversed = vec![parts[1].clone(), parts[0].clone()];
        let err = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &reversed, None)
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidPartOrder));
    }

    #[test]
    fn complete_multipart_upload_too_small_non_final_part() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // Part 1 is only 10 bytes (below 5 MiB minimum for non-final).
        let (upload_id, parts) = create_upload_with_parts(
            &coord,
            "bucket",
            "key",
            &[(1, b"small-part"), (2, b"last-part")],
        );

        let err = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap_err();
        assert!(matches!(
            err,
            ServerError::EntityTooSmall { part_number: 1, .. }
        ));
    }

    #[test]
    fn complete_multipart_upload_single_part_any_size() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // A single part can be any size (it's the "final" part).
        let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"tiny")]);

        let result = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap();
        assert!(result.etag.ends_with("-1\""));
    }

    #[test]
    fn complete_multipart_upload_empty_part_list() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        let err = coord
            .complete_multipart_upload("bucket", "key", &create.upload_id, &[], None)
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    #[test]
    fn complete_multipart_upload_retry_after_validation_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // Upload two small parts.
        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"small"), (2, b"last")]);

        // First attempt fails because part 1 is too small.
        let err = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap_err();
        assert!(matches!(err, ServerError::EntityTooSmall { .. }));

        // Upload remains usable — re-upload part 1 with large data and retry.
        let big_data = vec![0u8; 5 * 1024 * 1024];
        let new_part1 = coord
            .upload_part("bucket", "key", &upload_id, 1, &big_data, None)
            .unwrap();

        let retry_parts = vec![
            CompletePart {
                part_number: 1,
                etag: new_part1.etag,
                checksum: None,
            },
            parts[1].clone(),
        ];
        let result = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &retry_parts, None)
            .unwrap();
        assert!(result.etag.ends_with("-2\""));
    }

    #[test]
    fn complete_multipart_upload_duplicate_part_numbers() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);

        // Duplicate part number 1.
        let duped = vec![parts[0].clone(), parts[0].clone()];
        let err = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &duped, None)
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidPartOrder));
    }

    #[test]
    fn complete_multipart_upload_overwrite_unversioned() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // First multipart upload to key.
        let (upload_id1, parts1) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"first-upload")]);
        let result1 = coord
            .complete_multipart_upload("bucket", "key", &upload_id1, &parts1, None)
            .unwrap();
        assert!(result1.etag.ends_with("-1\""));

        // Second multipart upload to the same key (unversioned, version_id=0).
        let big_part = vec![0u8; 5 * 1024 * 1024];
        let (upload_id2, parts2) = create_upload_with_parts(
            &coord,
            "bucket",
            "key",
            &[(1, &big_part), (2, b"second-data-b")],
        );
        let result2 = coord
            .complete_multipart_upload("bucket", "key", &upload_id2, &parts2, None)
            .unwrap();
        assert!(result2.etag.ends_with("-2\""));
        assert_ne!(result1.etag, result2.etag);

        // Verify the object was overwritten — should have 2 parts now.
        let meta_pg_id = derive_pg("bucket", "key", coord.pg_count);
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let obj = pg.get_object_meta("bucket", "key").unwrap();
        assert_eq!(obj.parts_count, Some(2));

        // Old manifest parts (from first upload) should be replaced.
        let committed = pg.get_object_parts("bucket", "key", 0).unwrap();
        assert_eq!(committed.len(), 2);
    }

    #[test]
    fn complete_multipart_upload_list_shows_composite_etag() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);
        let result = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap();

        // list_objects_v2 should return the composite ETag with -N suffix.
        let list = coord
            .list_objects_v2("bucket", None, None, None, 100)
            .unwrap();
        assert_eq!(list.objects.len(), 1);
        assert_eq!(list.objects[0].etag, result.etag);
        assert!(
            list.objects[0].etag.ends_with("-1\""),
            "etag = {}",
            list.objects[0].etag
        );
    }

    #[test]
    fn complete_multipart_upload_list_versions_shows_composite_etag() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord.put_bucket_versioning("bucket", 1).unwrap();

        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);
        let result = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap();

        let versions = coord
            .list_object_versions("bucket", None, None, None, 100)
            .unwrap();
        assert_eq!(versions.versions.len(), 1);
        assert_eq!(versions.versions[0].etag, result.etag);
        assert!(
            versions.versions[0].etag.ends_with("-1\""),
            "etag = {}",
            versions.versions[0].etag
        );
    }

    // --- AbortMultipartUpload tests ---

    #[test]
    fn abort_multipart_upload_success() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, _parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part1"), (2, b"part2")]);

        coord
            .abort_multipart_upload("bucket", "key", &upload_id)
            .unwrap();

        // Upload should no longer exist.
        let err = coord
            .upload_part("bucket", "key", &upload_id, 1, b"nope", None)
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );

        // ListMultipartUploads should be empty.
        let uploads = coord
            .list_multipart_uploads("bucket", None, None, None, 100)
            .unwrap();
        assert!(uploads.uploads.is_empty());
    }

    #[test]
    fn abort_multipart_upload_nonexistent() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let err = coord
            .abort_multipart_upload("bucket", "key", "no-such-upload")
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );
    }

    #[test]
    fn abort_multipart_upload_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        // First abort succeeds.
        coord
            .abort_multipart_upload("bucket", "key", &create.upload_id)
            .unwrap();

        // Second abort: upload is already deleted, returns UploadNotFound.
        let err = coord
            .abort_multipart_upload("bucket", "key", &create.upload_id)
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected UploadNotFound on second abort, got {err:?}"
        );
    }

    #[test]
    fn abort_multipart_upload_wrong_bucket_key() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord.create_bucket("other").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        let err = coord
            .abort_multipart_upload("other", "key", &create.upload_id)
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );
    }

    #[test]
    fn abort_does_not_affect_completed_object() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // Create and complete an upload.
        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);
        coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap();

        // Abort the same upload_id should fail (already deleted by complete).
        let err = coord
            .abort_multipart_upload("bucket", "key", &upload_id)
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );

        // Object should still exist (visible in listing).
        let list = coord
            .list_objects_v2("bucket", None, None, None, 100)
            .unwrap();
        assert_eq!(list.objects.len(), 1);
        assert_eq!(list.objects[0].key, "key");
    }

    #[test]
    fn upload_part_after_abort_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();
        coord
            .upload_part("bucket", "key", &create.upload_id, 1, b"data", None)
            .unwrap();

        coord
            .abort_multipart_upload("bucket", "key", &create.upload_id)
            .unwrap();

        let err = coord
            .upload_part("bucket", "key", &create.upload_id, 2, b"more", None)
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );
    }

    // --- ListParts tests ---

    #[test]
    fn list_parts_basic() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, _parts) = create_upload_with_parts(
            &coord,
            "bucket",
            "key",
            &[(1, b"data1"), (3, b"data3"), (5, b"data5")],
        );

        let result = coord
            .list_parts("bucket", "key", &upload_id, None, 100)
            .unwrap();
        assert_eq!(result.parts.len(), 3);
        assert_eq!(result.parts[0].part_number, 1);
        assert_eq!(result.parts[1].part_number, 3);
        assert_eq!(result.parts[2].part_number, 5);
        assert_eq!(result.parts[0].size, 5); // "data1"
        assert!(!result.is_truncated);
        assert!(result.next_part_number_marker.is_none());
    }

    #[test]
    fn list_parts_pagination() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, _parts) = create_upload_with_parts(
            &coord,
            "bucket",
            "key",
            &[(1, b"a"), (2, b"b"), (3, b"c"), (4, b"d")],
        );

        // Page 1: max_parts=2
        let page1 = coord
            .list_parts("bucket", "key", &upload_id, None, 2)
            .unwrap();
        assert_eq!(page1.parts.len(), 2);
        assert_eq!(page1.parts[0].part_number, 1);
        assert_eq!(page1.parts[1].part_number, 2);
        assert!(page1.is_truncated);
        assert!(page1.next_part_number_marker.is_some());

        // Page 2: continue from marker
        let page2 = coord
            .list_parts(
                "bucket",
                "key",
                &upload_id,
                page1.next_part_number_marker,
                2,
            )
            .unwrap();
        assert_eq!(page2.parts.len(), 2);
        assert_eq!(page2.parts[0].part_number, 3);
        assert_eq!(page2.parts[1].part_number, 4);
        assert!(!page2.is_truncated);
    }

    #[test]
    fn list_parts_etag_format() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, complete_parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"hello")]);

        let result = coord
            .list_parts("bucket", "key", &upload_id, None, 100)
            .unwrap();
        assert_eq!(result.parts.len(), 1);
        // ListParts ETag should match the ETag returned by UploadPart.
        assert_eq!(result.parts[0].etag, complete_parts[0].etag);
    }

    #[test]
    fn list_parts_wrong_bucket_key() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord.create_bucket("other").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        let err = coord
            .list_parts("other", "key", &create.upload_id, None, 100)
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );
    }

    #[test]
    fn list_parts_nonexistent_upload() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let err = coord
            .list_parts("bucket", "key", "no-such-upload", None, 100)
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );
    }

    #[test]
    fn list_parts_after_reupload_shows_latest() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        // Upload part 1, then overwrite it.
        coord
            .upload_part("bucket", "key", &create.upload_id, 1, b"original", None)
            .unwrap();
        let reupload = coord
            .upload_part("bucket", "key", &create.upload_id, 1, b"replaced", None)
            .unwrap();

        let result = coord
            .list_parts("bucket", "key", &create.upload_id, None, 100)
            .unwrap();
        assert_eq!(result.parts.len(), 1);
        assert_eq!(result.parts[0].etag, reupload.etag);
        assert_eq!(result.parts[0].size, "replaced".len() as u64);
    }

    #[test]
    fn list_parts_rejected_when_aborting() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();
        coord
            .upload_part("bucket", "key", &create.upload_id, 1, b"data", None)
            .unwrap();

        // Manually transition to Aborting (simulates the window during abort).
        let meta_pg_id = derive_pg("bucket", "key", coord.pg_count);
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        pg.set_upload_state(&create.upload_id, UploadState::Aborting)
            .unwrap();
        drop(pg);

        let err = coord
            .list_parts("bucket", "key", &create.upload_id, None, 100)
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );
    }

    #[test]
    fn abort_completing_upload_returns_no_such_upload() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload("bucket", "key", &metadata, None, None)
            .unwrap();

        // Manually transition to Completing (simulates concurrent complete).
        let meta_pg_id = derive_pg("bucket", "key", coord.pg_count);
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        pg.set_upload_state(&create.upload_id, UploadState::Completing)
            .unwrap();
        drop(pg);

        let err = coord
            .abort_multipart_upload("bucket", "key", &create.upload_id)
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );
    }

    // --- Multipart-aware read tests (Step 9) ---

    const MIN_PART: usize = 5 * 1024 * 1024; // 5 MiB

    /// Make part data: first MIN_PART bytes are `fill`, rest is padding.
    /// For the final part, `size` can be less than MIN_PART.
    fn make_part(fill: u8, size: usize) -> Vec<u8> {
        vec![fill; size]
    }

    /// Helper: create a completed multipart object with given (part_number, data) pairs.
    fn create_completed_multipart_vec(
        coord: &Coordinator,
        bucket: &str,
        key: &str,
        part_data: &[(u32, Vec<u8>)],
    ) -> CompleteMultipartUploadResult {
        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(bucket, key, &metadata, None, None)
            .unwrap();
        let mut complete_parts = Vec::new();
        for (part_number, data) in part_data {
            let result = coord
                .upload_part(bucket, key, &create.upload_id, *part_number, data, None)
                .unwrap();
            complete_parts.push(CompletePart {
                part_number: *part_number,
                etag: result.etag,
                checksum: None,
            });
        }
        coord
            .complete_multipart_upload(bucket, key, &create.upload_id, &complete_parts, None)
            .unwrap()
    }

    #[test]
    fn get_multipart_object_full() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let part2 = make_part(0xBB, 100);
        let expected: Vec<u8> = [part1.as_slice(), part2.as_slice()].concat();

        let result =
            create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

        let obj = coord
            .get_object("bucket", "key", None, &ReadCondition::default())
            .unwrap();
        assert_eq!(obj.data, expected);
        assert_eq!(obj.etag, result.etag);
        assert_eq!(obj.size, expected.len() as u64);
        assert!(obj.etag.ends_with("-2\""), "etag = {}", obj.etag);
    }

    #[test]
    fn get_multipart_object_single_part() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, b"only-part".to_vec())]);

        let obj = coord
            .get_object("bucket", "key", None, &ReadCondition::default())
            .unwrap();
        assert_eq!(obj.data, b"only-part");
    }

    #[test]
    fn head_multipart_object() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let part2 = make_part(0xBB, 200);
        let total_size = part1.len() + part2.len();

        let result =
            create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

        let head = coord
            .head_object("bucket", "key", None, &ReadCondition::default())
            .unwrap();
        assert_eq!(head.size, total_size as u64);
        assert_eq!(head.etag, result.etag);
        assert!(head.etag.ends_with("-2\""), "etag = {}", head.etag);
    }

    #[test]
    fn get_multipart_object_range_within_part() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let part2 = make_part(0xBB, 100);

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

        // Range within first part: bytes 10-19
        let range = coord
            .get_object_range(
                "bucket",
                "key",
                None,
                ByteRange::Range { start: 10, end: 19 },
                &ReadCondition::default(),
            )
            .unwrap();
        assert_eq!(range.data, vec![0xAA; 10]);
        assert_eq!(range.range_start, 10);
        assert_eq!(range.range_end, 19);
    }

    #[test]
    fn get_multipart_object_range_spanning_parts() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let part2 = make_part(0xBB, MIN_PART);
        let part3 = make_part(0xCC, 100);

        create_completed_multipart_vec(
            &coord,
            "bucket",
            "key",
            &[(1, part1), (2, part2), (3, part3)],
        );

        // Range spanning part1/part2 boundary: last 4 bytes of part1 + first 4 of part2
        let boundary = MIN_PART as u64;
        let range = coord
            .get_object_range(
                "bucket",
                "key",
                None,
                ByteRange::Range {
                    start: boundary - 4,
                    end: boundary + 3,
                },
                &ReadCondition::default(),
            )
            .unwrap();
        let mut expected = vec![0xAA; 4];
        expected.extend_from_slice(&[0xBB; 4]);
        assert_eq!(range.data, expected);
    }

    #[test]
    fn get_multipart_object_range_suffix() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let part2 = make_part(0xBB, 100);

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

        // Suffix range: last 50 bytes (all within part2)
        let range = coord
            .get_object_range(
                "bucket",
                "key",
                None,
                ByteRange::Suffix { length: 50 },
                &ReadCondition::default(),
            )
            .unwrap();
        assert_eq!(range.data, vec![0xBB; 50]);
    }

    #[test]
    fn copy_multipart_source() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("src-bucket").unwrap();
        coord.create_bucket("dst-bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let part2 = make_part(0xBB, 200);
        let expected: Vec<u8> = [part1.as_slice(), part2.as_slice()].concat();

        create_completed_multipart_vec(&coord, "src-bucket", "src-key", &[(1, part1), (2, part2)]);

        // Copy multipart source to destination (creates inline object).
        coord
            .copy_object(
                "src-bucket",
                "src-key",
                None,
                "dst-bucket",
                "dst-key",
                &ReadCondition::default(),
                &WriteCondition::default(),
                MetadataDirective::Copy,
                &[],
            )
            .unwrap();

        // Destination should have the concatenated data as inline object.
        let dst = coord
            .get_object("dst-bucket", "dst-key", None, &ReadCondition::default())
            .unwrap();
        assert_eq!(dst.data, expected);
    }

    #[test]
    fn get_multipart_object_zero_byte_single_part() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, vec![])]);

        let obj = coord
            .get_object("bucket", "key", None, &ReadCondition::default())
            .unwrap();
        assert!(obj.data.is_empty());
        assert_eq!(obj.size, 0);
    }

    #[test]
    fn get_multipart_object_zero_byte_final_part() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let expected = part1.clone();

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, vec![])]);

        let obj = coord
            .get_object("bucket", "key", None, &ReadCondition::default())
            .unwrap();
        assert_eq!(obj.data, expected);
        assert_eq!(obj.size, MIN_PART as u64);
    }

    #[test]
    fn get_object_part_zero_byte_single_part() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, vec![])]);

        let result = coord
            .get_object_part("bucket", "key", None, 1, &ReadCondition::default())
            .unwrap();
        assert!(result.data.is_empty());
        assert_eq!(result.size, 0);
        assert_eq!(result.parts_count, 1);
        assert_eq!(result.part_start, 0);
        assert_eq!(result.part_end, 0);
    }

    #[test]
    fn get_object_part_zero_byte_final_part() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1.clone()), (2, vec![])]);

        // Part 1 should return full data
        let result = coord
            .get_object_part("bucket", "key", None, 1, &ReadCondition::default())
            .unwrap();
        assert_eq!(result.data, part1);
        assert_eq!(result.part_start, 0);
        assert_eq!(result.part_end, MIN_PART as u64 - 1);

        // Part 2 (zero-byte) should return empty data
        let result = coord
            .get_object_part("bucket", "key", None, 2, &ReadCondition::default())
            .unwrap();
        assert!(result.data.is_empty());
        assert_eq!(result.parts_count, 2);
        assert_eq!(result.part_start, MIN_PART as u64);
        assert_eq!(result.part_end, MIN_PART as u64);
    }

    #[test]
    fn head_multipart_object_zero_byte() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, vec![])]);

        let head = coord
            .head_object("bucket", "key", None, &ReadCondition::default())
            .unwrap();
        assert_eq!(head.size, 0);
    }

    #[test]
    fn copy_multipart_source_zero_byte() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("src").unwrap();
        coord.create_bucket("dst").unwrap();

        create_completed_multipart_vec(&coord, "src", "key", &[(1, vec![])]);

        coord
            .copy_object(
                "src",
                "key",
                None,
                "dst",
                "key",
                &ReadCondition::default(),
                &WriteCondition::default(),
                MetadataDirective::Copy,
                &[],
            )
            .unwrap();

        let dst = coord
            .get_object("dst", "key", None, &ReadCondition::default())
            .unwrap();
        assert!(dst.data.is_empty());
    }

    #[test]
    fn read_multipart_range_detects_incomplete_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // Create a multipart object, then corrupt manifest by deleting a part row.
        let part1 = make_part(0xAA, MIN_PART);
        let part2 = make_part(0xBB, 100);

        let result =
            create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

        // Get the real manifest, then replace with only part 2 (gap: part 1 missing).
        let meta_pg_id = derive_pg("bucket", "key", coord.pg_count);
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let real_parts = pg
            .get_object_parts("bucket", "key", result.version_id)
            .unwrap();
        assert_eq!(real_parts.len(), 2);
        let part2_record = real_parts[1].clone(); // real part 2 with valid shards
        pg.delete_object_parts("bucket", "key", result.version_id)
            .unwrap();
        pg.commit_object_parts(&[part2_record]).unwrap();
        drop(pg);

        let err = coord
            .get_object("bucket", "key", None, &ReadCondition::default())
            .unwrap_err();
        assert!(
            matches!(err, ServerError::IntegrityError { .. }),
            "expected IntegrityError for incomplete manifest, got {err:?}"
        );
    }

    // ── CompleteMultipartUpload checksum tests ──────────────────────────

    /// Helper: create a multipart upload with a checksum algorithm, upload parts with checksums,
    /// and return (upload_id, complete_parts_with_checksums, part_data_list).
    fn create_checksum_upload(
        coord: &Coordinator,
        bucket: &str,
        key: &str,
        algo: ChecksumAlgorithm,
        ctype: Option<ChecksumType>,
        part_data: &[&[u8]],
    ) -> (String, Vec<CompletePart>) {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(bucket, key, &metadata, Some(algo), ctype)
            .unwrap();
        let mut complete_parts = Vec::new();
        for (i, data) in part_data.iter().enumerate() {
            let part_number = (i + 1) as u32;
            let checksum_b64 = b64.encode(compute_checksum(algo, data));
            let result = coord
                .upload_part(
                    bucket,
                    key,
                    &create.upload_id,
                    part_number,
                    data,
                    Some((algo, &checksum_b64)),
                )
                .unwrap();
            complete_parts.push(CompletePart {
                part_number,
                etag: result.etag,
                checksum: Some((algo, checksum_b64)),
            });
        }
        (create.upload_id, complete_parts)
    }

    #[test]
    fn complete_multipart_sha256_composite_checksum() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let big = vec![0xABu8; 5 * 1024 * 1024];
        let small = b"final-part";
        let (upload_id, parts) = create_checksum_upload(
            &coord,
            "bucket",
            "key",
            ChecksumAlgorithm::Sha256,
            None, // defaults to COMPOSITE
            &[&big, small],
        );

        let result = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap();

        assert_eq!(result.checksum_algorithm, Some(ChecksumAlgorithm::Sha256));
        assert_eq!(result.checksum_type, Some(ChecksumType::Composite));
        let val = result.checksum_value.unwrap();
        assert!(val.ends_with("-2"), "expected -2 suffix, got {val}");

        // Verify the composite checksum manually:
        // hash(concat(raw_sha256_part1, raw_sha256_part2))
        let raw1 = compute_checksum(ChecksumAlgorithm::Sha256, &big);
        let raw2 = compute_checksum(ChecksumAlgorithm::Sha256, small);
        let mut concat = Vec::new();
        concat.extend_from_slice(&raw1);
        concat.extend_from_slice(&raw2);
        let expected_hash = compute_checksum(ChecksumAlgorithm::Sha256, &concat);
        let expected = format!("{}-2", b64.encode(&expected_hash));
        assert_eq!(val, expected);
    }

    #[test]
    fn complete_multipart_crc32_full_object_checksum() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let big = vec![0xABu8; 5 * 1024 * 1024];
        let small = b"final-part";
        let (upload_id, parts) = create_checksum_upload(
            &coord,
            "bucket",
            "key",
            ChecksumAlgorithm::Crc32,
            Some(ChecksumType::FullObject),
            &[&big, small],
        );

        let result = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap();

        assert_eq!(result.checksum_algorithm, Some(ChecksumAlgorithm::Crc32));
        assert_eq!(result.checksum_type, Some(ChecksumType::FullObject));

        // Verify: combine matches computing CRC32 of concatenated data.
        let mut full_data = big.clone();
        full_data.extend_from_slice(small);
        let expected_crc = checksum::crc32::checksum(&full_data);
        let expected = b64.encode(expected_crc.to_be_bytes());
        assert_eq!(result.checksum_value.unwrap(), expected);
    }

    #[test]
    fn complete_multipart_crc32c_full_object_checksum() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let big = vec![0xCDu8; 5 * 1024 * 1024];
        let small = b"last";
        let (upload_id, parts) = create_checksum_upload(
            &coord,
            "bucket",
            "key",
            ChecksumAlgorithm::Crc32c,
            Some(ChecksumType::FullObject),
            &[&big, small],
        );

        let result = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap();

        assert_eq!(result.checksum_algorithm, Some(ChecksumAlgorithm::Crc32c));
        assert_eq!(result.checksum_type, Some(ChecksumType::FullObject));

        let mut full_data = big.clone();
        full_data.extend_from_slice(small);
        let expected_crc = checksum::crc32c::checksum(&full_data);
        let expected = b64.encode(expected_crc.to_be_bytes());
        assert_eq!(result.checksum_value.unwrap(), expected);
    }

    #[test]
    fn complete_multipart_crc64nvme_full_object_checksum() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let big = vec![0xEFu8; 5 * 1024 * 1024];
        let small = b"end";
        let (upload_id, parts) = create_checksum_upload(
            &coord,
            "bucket",
            "key",
            ChecksumAlgorithm::Crc64nvme,
            Some(ChecksumType::FullObject),
            &[&big, small],
        );

        let result = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap();

        assert_eq!(
            result.checksum_algorithm,
            Some(ChecksumAlgorithm::Crc64nvme)
        );
        assert_eq!(result.checksum_type, Some(ChecksumType::FullObject));

        let mut full_data = big.clone();
        full_data.extend_from_slice(small);
        let expected_crc = crc64::checksum(&full_data);
        let expected = b64.encode(expected_crc.to_be_bytes());
        assert_eq!(result.checksum_value.unwrap(), expected);
    }

    #[test]
    fn complete_multipart_crc32_composite_checksum() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // CRC32 + COMPOSITE is intentionally allowed (produces hash-of-hashes-N).
        let big = vec![0x11u8; 5 * 1024 * 1024];
        let small = b"tail";
        let (upload_id, parts) = create_checksum_upload(
            &coord,
            "bucket",
            "key",
            ChecksumAlgorithm::Crc32,
            Some(ChecksumType::Composite),
            &[&big, small],
        );

        let result = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap();

        assert_eq!(result.checksum_algorithm, Some(ChecksumAlgorithm::Crc32));
        assert_eq!(result.checksum_type, Some(ChecksumType::Composite));
        let val = result.checksum_value.unwrap();
        assert!(val.ends_with("-2"), "expected -2 suffix, got {val}");

        // Verify: hash of concatenated raw CRC32 bytes.
        let raw1 = compute_checksum(ChecksumAlgorithm::Crc32, &big);
        let raw2 = compute_checksum(ChecksumAlgorithm::Crc32, small);
        let mut concat = Vec::new();
        concat.extend_from_slice(&raw1);
        concat.extend_from_slice(&raw2);
        let hash = compute_checksum(ChecksumAlgorithm::Crc32, &concat);
        let expected = format!("{}-2", b64.encode(&hash));
        assert_eq!(val, expected);
    }

    #[test]
    fn complete_multipart_bad_part_checksum_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let big = vec![0xAAu8; 5 * 1024 * 1024];
        let small = b"end";
        let (upload_id, mut parts) = create_checksum_upload(
            &coord,
            "bucket",
            "key",
            ChecksumAlgorithm::Crc32,
            Some(ChecksumType::FullObject),
            &[&big, small],
        );

        // Tamper with part 1's checksum value in the request.
        parts[0].checksum = Some((ChecksumAlgorithm::Crc32, "AAAAAAAA".to_string()));

        let err = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap_err();
        assert!(
            matches!(err, ServerError::InvalidRequest { .. }),
            "expected InvalidRequest, got {err:?}"
        );
    }

    #[test]
    fn complete_multipart_no_checksum_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let big = vec![0u8; 5 * 1024 * 1024];
        let small = b"last";
        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, &big), (2, small)]);

        let result = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap();

        assert_eq!(result.checksum_algorithm, None);
        assert_eq!(result.checksum_type, None);
        assert_eq!(result.checksum_value, None);
    }

    #[test]
    fn complete_multipart_wrong_checksum_element_type_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let big = vec![0xAAu8; 5 * 1024 * 1024];
        let small = b"end";
        let (upload_id, mut parts) = create_checksum_upload(
            &coord,
            "bucket",
            "key",
            ChecksumAlgorithm::Crc32,
            Some(ChecksumType::FullObject),
            &[&big, small],
        );

        // Replace the CRC32 checksum with a SHA256-tagged element (wrong algorithm).
        // Use the correct CRC32 value so only the element type is wrong.
        let correct_value = parts[0].checksum.as_ref().unwrap().1.clone();
        parts[0].checksum = Some((ChecksumAlgorithm::Sha256, correct_value));

        let err = coord
            .complete_multipart_upload("bucket", "key", &upload_id, &parts, None)
            .unwrap_err();
        assert!(
            matches!(err, ServerError::InvalidRequest { .. }),
            "expected InvalidRequest for wrong element type, got {err:?}"
        );
    }
}
