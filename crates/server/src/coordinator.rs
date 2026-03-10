/// Coordinator: orchestrates S3 operations across EC, storage, and metadata layers.
use std::sync::{Arc, MutexGuard};

use ec::{EcConfig, ErasureCodec};
use storage::traits::{PgMetadataStore, ShardStore};
use storage::{
    BucketInfo, BucketName, ChecksumAlgorithm, ChecksumType, CommitMultipartReq,
    CommitStreamPutReq, CreateMultipartUploadReq, CreateStreamUploadReq, EcShape,
    ListMultipartUploadsReq, ListObjectVersionsReq, ListObjectsReq, ListPartsReq, LiveObjectRecord,
    MultipartPartChunkRecord, MultipartPartRecord, MultipartUploadRecord, ObjectKey, ObjectLayout,
    ObjectPartRecord, PutDeleteMarkerReq, PutLiveObjectReq, PutObjectReq, SessionId, ShardKey,
    SharedStorageNode, StoredObject, StreamObjectChunkRecord, StreamUploadChunkRecord,
    StreamUploadState, StreamUploadTarget, UploadId, UploadState,
};

use crate::conditional::{
    check_copy_source_conditions, check_delete_conditions, check_read_conditions,
    check_write_conditions, DeleteCondition, ReadCondition, WriteCondition,
};
use crate::error::ServerError;
use crate::etag::{compute_multipart_etag, crc64_to_etag_bytes, etag_bytes_to_crc64, format_etag};
use crate::metadata_blob::MetadataBlob;
#[cfg(test)]
use crate::pg::derive_pg_shards;
use crate::pg::{chunk_key_hash, object_key_hash, part_key_hash, PgTopology};
use crate::range::ByteRange;

/// Maximum object size for single PUT or upload part (5 GiB, matches AWS S3).
pub const MAX_OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024;

/// A checksum claim parsed from HTTP headers or trailers.
///
/// Base64 decoding and length validation happen at construction time,
/// so the coordinator receives already-decoded, validated bytes.
#[derive(Debug, Clone)]
pub struct ChecksumClaim {
    algorithm: ChecksumAlgorithm,
    expected_bytes: Vec<u8>,
}

impl ChecksumClaim {
    /// Parse a base64-encoded checksum value, validating format and length.
    pub fn from_base64(
        algorithm: ChecksumAlgorithm,
        b64: &str,
    ) -> Result<Self, ServerError> {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|_| ServerError::InvalidRequest {
                reason: "invalid base64 in checksum value".to_string(),
            })?;
        let expected_len = algorithm.expected_byte_length();
        if bytes.len() != expected_len {
            return Err(ServerError::InvalidRequest {
                reason: format!(
                    "checksum length {} does not match {} (expected {})",
                    bytes.len(),
                    algorithm.as_str(),
                    expected_len,
                ),
            });
        }
        Ok(Self {
            algorithm,
            expected_bytes: bytes,
        })
    }

    /// The checksum algorithm.
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        self.algorithm
    }

    /// The decoded checksum bytes.
    pub fn expected_bytes(&self) -> &[u8] {
        &self.expected_bytes
    }
}

/// Hard cap on total records fetched across all PGs for a single list query.
/// Prevents unbounded memory when delimiter causes u32::MAX per-PG limits.
const MAX_LIST_RECORDS: usize = 100_000;

/// Result of a PutObject operation.
#[derive(Debug)]
pub struct PutObjectResult {
    pub etag: String,
    pub version_id: storage::VersionId,
}

/// Result of beginning a streaming UploadPart session.
#[derive(Debug)]
pub struct BeginStreamPartResult {
    pub session_id: String,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
}

/// Result of a GetObject operation.
#[derive(Debug)]
pub struct GetObjectResult {
    pub data: Vec<u8>,
    pub metadata: MetadataBlob,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub version_id: storage::VersionId,
    pub tags: Option<String>,
}

/// Result of a HeadObject operation.
#[derive(Debug)]
pub struct HeadObjectResult {
    pub metadata: MetadataBlob,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub version_id: storage::VersionId,
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
    pub version_id: storage::VersionId,
    pub tags: Option<String>,
    /// Per-part checksum (algorithm + raw bytes).
    pub checksum: Option<storage::RawChecksum>,
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
    pub version_id: storage::VersionId,
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
    pub version_id: storage::VersionId,
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
    pub version_id: storage::VersionId,
    pub tags: Option<String>,
    /// Per-part checksum (algorithm + raw bytes).
    pub checksum: Option<storage::RawChecksum>,
}

/// Metadata handling directive for `CopyObject`.
#[derive(Debug)]
pub enum MetadataDirective<'a> {
    /// Preserve source object's metadata.
    Copy,
    /// Replace metadata with an already-parsed blob and optional checksum algorithm.
    ///
    /// The `MetadataBlob` should already have checksum value headers stripped
    /// (they can't be verified on CopyObject since there's no body).
    /// If `checksum_algorithm` is provided, a fresh checksum will be computed
    /// from the copied data.
    Replace {
        metadata: &'a MetadataBlob,
        checksum_algorithm: Option<ChecksumAlgorithm>,
    },
}

/// Parsed copy-source reference, shared by CopyObject and UploadPartCopy.
#[derive(Debug)]
pub struct CopySource<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<storage::VersionId>,
    pub condition: &'a ReadCondition,
}

/// Parsed CopyObject request from the HTTP layer.
#[derive(Debug)]
pub struct CopyObjectRequest<'a> {
    pub source: CopySource<'a>,
    pub dst_bucket: &'a str,
    pub dst_key: &'a str,
    pub dst_condition: &'a WriteCondition,
    pub directive: MetadataDirective<'a>,
}

/// Parsed UploadPartCopy request from the HTTP layer.
#[derive(Debug)]
pub struct UploadPartCopyRequest<'a> {
    pub source: CopySource<'a>,
    pub dst_bucket: &'a str,
    pub dst_key: &'a str,
    pub upload_id: &'a str,
    pub part_number: u32,
    pub copy_source_range: Option<(u64, u64)>,
}

/// Request for a PutObject operation.
#[derive(Debug)]
pub struct PutObjectRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub data: &'a [u8],
    pub metadata: &'a MetadataBlob,
    pub cond: &'a WriteCondition,
}

/// Request for a GetObject or HeadObject operation.
#[derive(Debug)]
pub struct GetObjectRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<storage::VersionId>,
    pub cond: &'a ReadCondition,
}

/// Request for a GetObjectPart or HeadObjectPart operation.
#[derive(Debug)]
pub struct GetObjectPartRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<storage::VersionId>,
    pub part_number: u32,
    pub cond: &'a ReadCondition,
}

/// Request for a GetObjectRange operation.
#[derive(Debug)]
pub struct GetObjectRangeRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<storage::VersionId>,
    pub range: ByteRange,
    pub cond: &'a ReadCondition,
}

/// Request for a DeleteObject operation.
#[derive(Debug)]
pub struct DeleteObjectRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<storage::VersionId>,
    pub cond: &'a DeleteCondition,
}

/// Request for a ListObjectsV2 operation.
#[derive(Debug)]
pub struct ListObjectsV2Request<'a> {
    pub bucket: &'a str,
    pub prefix: Option<&'a str>,
    pub delimiter: Option<&'a str>,
    pub continuation_token: Option<&'a str>,
    pub max_keys: u32,
}

/// Request for a ListObjectVersions operation.
#[derive(Debug)]
pub struct ListObjectVersionsRequest<'a> {
    pub bucket: &'a str,
    pub prefix: Option<&'a str>,
    pub key_marker: Option<&'a str>,
    pub version_id_marker: Option<storage::VersionId>,
    pub max_keys: u32,
}

/// Request for a ListParts operation.
#[derive(Debug)]
pub struct ListPartsRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
    pub part_number_marker: Option<u32>,
    pub max_parts: u32,
}

/// Request for a ListMultipartUploads operation.
#[derive(Debug)]
pub struct ListMultipartUploadsRequest<'a> {
    pub bucket: &'a str,
    pub prefix: Option<&'a str>,
    pub key_marker: Option<&'a str>,
    pub upload_id_marker: Option<&'a str>,
    pub max_uploads: u32,
}

/// A single entry in a batch-delete request, with an already-parsed version ID.
#[derive(Debug)]
pub struct DeleteEntry<'a> {
    pub key: &'a str,
    pub version_id: Option<storage::VersionId>,
}

/// Request for a DeleteObjects (multi-delete) operation.
#[derive(Debug)]
pub struct DeleteObjectsRequest<'a> {
    pub bucket: &'a str,
    pub entries: &'a [DeleteEntry<'a>],
    pub cond: &'a DeleteCondition,
}

/// Request for an AbortMultipartUpload operation.
#[derive(Debug)]
pub struct AbortMultipartUploadRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
}

/// Request for a CreateMultipartUpload operation.
#[derive(Debug)]
pub struct CreateMultipartUploadRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub metadata: &'a MetadataBlob,
    pub checksum: Option<storage::MultipartChecksumConfig>,
}

/// Request for an UploadPart operation.
#[derive(Debug)]
pub struct UploadPartRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
    pub part_number: u32,
    pub data: &'a [u8],
    pub claimed_checksum: Option<&'a ChecksumClaim>,
}

/// Request for a GetObjectAttributes operation.
#[derive(Debug)]
pub struct GetObjectAttributesRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<storage::VersionId>,
    pub cond: &'a ReadCondition,
    pub want_parts: bool,
    pub part_number_marker: Option<u32>,
    pub max_parts: u32,
}

/// Request for a CompleteMultipartUpload operation.
#[derive(Debug)]
pub struct CompleteMultipartUploadRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
    pub parts: &'a [CompletePart],
    pub claimed_checksum: Option<(ChecksumAlgorithm, &'a str)>,
}

/// Parsed request for finalizing a streaming PutObject.
#[derive(Debug)]
pub struct FinalizeStreamPutRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub session_id: &'a str,
    pub crc64: u64,
    pub total_size: u64,
    pub metadata_blob: &'a MetadataBlob,
    pub cond: &'a WriteCondition,
}

/// Parsed request for finalizing a streaming UploadPart.
#[derive(Debug)]
pub struct FinalizeStreamPartRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub session_id: &'a str,
    pub upload_id: &'a str,
    pub part_number: u32,
    pub crc64: u64,
    pub total_size: u64,
    pub claimed_checksum: Option<&'a ChecksumClaim>,
    pub computed_checksum: Option<storage::RawChecksum>,
}

/// Result of a `CopyObject` operation.
#[derive(Debug)]
pub struct CopyObjectResult {
    pub etag: String,
    pub last_modified: u64,
    pub version_id: storage::VersionId,
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
    pub version_id: storage::VersionId,
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
    pub next_version_id_marker: Option<storage::VersionId>,
}

/// Result of a DeleteObject operation.
#[derive(Debug)]
pub struct DeleteObjectResult {
    pub version_id: storage::VersionId,
    pub delete_marker: bool,
}

/// Result entry for a successfully deleted object in a batch delete.
#[derive(Debug)]
pub struct DeletedObject {
    pub key: String,
    pub version_id: storage::VersionId,
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
    /// Verified checksum for this part (if any).
    pub checksum: Option<storage::RawChecksum>,
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
    checksum: Option<storage::RawChecksum>,
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
    pub version_id: storage::VersionId,
    /// Object-level checksum algorithm (if configured).
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    /// Object-level checksum type.
    pub checksum_type: Option<ChecksumType>,
    /// Object-level checksum (base64-encoded).
    pub checksum_value: Option<String>,
}

/// Minimum part size for non-final parts (5 MiB).
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Maximum number of parts in a multipart upload (matches AWS S3).
const MAX_PARTS: usize = 10_000;

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
    record: StoredObject,
    pgs: TwoPgGuards<'a>,
}

struct LockedWriteObject<'a> {
    version_id: storage::VersionId,
    pgs: TwoPgGuards<'a>,
}

/// The coordinator ties together EC, storage, and metadata.
pub struct Coordinator {
    storage_node: Arc<SharedStorageNode>,
    pg_topology: PgTopology,
    ec_codec: ErasureCodec,
    ec_config: EcConfig,
    region: String,
}

impl Coordinator {
    /// Create a new coordinator.
    pub fn new(
        storage_node: Arc<SharedStorageNode>,
        ec_config: EcConfig,
        region: String,
    ) -> Result<Self, ServerError> {
        let ec_codec = ErasureCodec::new(ec_config)?;
        let pg_topology = PgTopology::new(storage_node.pg_ids()).map_err(|reason| {
            ServerError::InternalError {
                reason: reason.to_string(),
            }
        })?;
        Ok(Self {
            storage_node,
            pg_topology,
            ec_codec,
            ec_config,
            region,
        })
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    fn bucket_pg_id(&self, bucket: &str) -> u32 {
        self.pg_topology.bucket_pg(bucket)
    }

    fn object_pg_id(&self, bucket: &str, key: &str) -> u32 {
        self.pg_topology.object_pg(bucket, key)
    }

    fn shard_pg_id(&self, bucket: &str, key: &str, version_id: storage::VersionId) -> u32 {
        self.pg_topology.shard_pg(bucket, key, version_id.to_u64())
    }

    fn get_bucket_pg(&self, bucket: &str) -> Result<MutexGuard<'_, storage::PgStore>, ServerError> {
        let pg_id = self.bucket_pg_id(bucket);
        Ok(self.storage_node.get_pg(pg_id)?)
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
        let _bucket_guard = self.storage_node.lock_bucket(name);
        let bucket_pg = self.get_bucket_pg(name)?;
        match bucket_pg.create_bucket(name, owner_principal, public_read) {
            Ok(()) => Ok(()),
            Err(storage::MetadataError::BucketAlreadyExists) => {
                let existing = bucket_pg.head_bucket(name).map_err(|e| match e {
                    storage::MetadataError::BucketNotFound { name } => {
                        ServerError::BucketNotFound {
                            name: name.to_string(),
                        }
                    }
                    other => ServerError::Metadata(other),
                })?;
                if existing.owner_principal == owner_principal {
                    Ok(())
                } else {
                    Err(ServerError::BucketAlreadyExists)
                }
            }
            Err(other) => Err(ServerError::Metadata(other)),
        }
    }

    pub fn delete_bucket(&self, name: &str) -> Result<(), ServerError> {
        let _bucket_guard = self.storage_node.lock_bucket(name);

        // Check emptiness: list all object versions (including delete markers)
        // and multipart uploads across all PGs.
        self.pg_topology.for_each_pg(|pg_id| {
            let pg = self.storage_node.get_pg(pg_id)?;
            let resp = pg.list_object_versions(&ListObjectVersionsReq {
                bucket: BucketName::from(name),
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                max_keys: 1,
            })?;
            if !resp.versions.is_empty() {
                return Err(ServerError::BucketNotEmpty);
            }
            let mpu_resp = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                bucket: BucketName::from(name),
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1,
            })?;
            if !mpu_resp.uploads.is_empty() {
                return Err(ServerError::BucketNotEmpty);
            }
            Ok::<(), ServerError>(())
        })?;

        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg.delete_bucket(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            storage::MetadataError::BucketNotEmpty => ServerError::BucketNotEmpty,
            other => ServerError::Metadata(other),
        })
    }

    pub fn head_bucket(&self, name: &str) -> Result<BucketInfo, ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg.head_bucket(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
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
        let mut out = Vec::new();
        self.pg_topology.for_each_pg(|pg_id| {
            let pg = self.storage_node.get_pg(pg_id)?;
            let mut buckets = pg.list_buckets(owner_principal)?;
            out.append(&mut buckets);
            Ok::<(), ServerError>(())
        })?;
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    pub fn put_bucket_versioning(
        &self,
        name: &str,
        state: storage::BucketVersioningState,
    ) -> Result<(), ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg
            .put_bucket_versioning(name, state)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                storage::MetadataError::InvalidVersioningTransition { from, to } => {
                    ServerError::InvalidRequest {
                        reason: format!(
                            "invalid versioning transition from {:?} to {:?}",
                            from, to
                        ),
                    }
                }
                other => ServerError::Metadata(other),
            })
    }

    pub fn get_bucket_versioning(
        &self,
        name: &str,
    ) -> Result<storage::BucketVersioningState, ServerError> {
        let info = self.head_bucket(name)?;
        Ok(info.versioning)
    }

    pub fn put_bucket_cors(&self, name: &str, config: &str) -> Result<(), ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg
            .put_bucket_cors(name, config)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
    }

    pub fn get_bucket_cors(&self, name: &str) -> Result<Option<String>, ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg.get_bucket_cors(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })
    }

    pub fn delete_bucket_cors(&self, name: &str) -> Result<(), ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg.delete_bucket_cors(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })
    }

    // ── Bucket tagging ────────────────────────────────────────────────

    pub fn put_bucket_tags(&self, name: &str, tags: &str) -> Result<(), ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg.put_bucket_tags(name, tags).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })
    }

    pub fn get_bucket_tags(&self, name: &str) -> Result<Option<String>, ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg.get_bucket_tags(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })
    }

    pub fn delete_bucket_tags(&self, name: &str) -> Result<(), ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg.delete_bucket_tags(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })
    }

    // ── Public access block ───────────────────────────────────────────

    pub fn put_bucket_public_access_block(
        &self,
        name: &str,
        config: &str,
    ) -> Result<(), ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg
            .put_bucket_public_access_block(name, config)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
    }

    pub fn get_bucket_public_access_block(
        &self,
        name: &str,
    ) -> Result<Option<String>, ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg
            .get_bucket_public_access_block(name)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
    }

    pub fn delete_bucket_public_access_block(&self, name: &str) -> Result<(), ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg
            .delete_bucket_public_access_block(name)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
    }

    // ── Bucket ACL ───────────────────────────────────────────────────

    pub fn put_bucket_acl(&self, name: &str, public_read: bool) -> Result<(), ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg
            .put_bucket_acl(name, public_read)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
    }

    // ── Ownership controls ────────────────────────────────────────────

    pub fn put_bucket_ownership_controls(
        &self,
        name: &str,
        config: &str,
    ) -> Result<(), ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg
            .put_bucket_ownership_controls(name, config)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
    }

    pub fn get_bucket_ownership_controls(&self, name: &str) -> Result<Option<String>, ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg
            .get_bucket_ownership_controls(name)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
    }

    pub fn delete_bucket_ownership_controls(&self, name: &str) -> Result<(), ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg
            .delete_bucket_ownership_controls(name)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
    }

    // ── Object tagging ──────────────────────────────────────────────

    pub fn put_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<storage::VersionId>,
        tags: &str,
    ) -> Result<(), ServerError> {
        self.head_bucket(bucket)?;
        let pg_id = self.object_pg_id(bucket, key);
        let pg = self.storage_node.get_pg(pg_id)?;
        let stored = match version_id {
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
        if stored.is_delete_marker() {
            return Err(ServerError::MethodNotAllowed);
        }
        pg.put_object_tags(bucket, key, stored.version_id(), tags)
            .map_err(ServerError::Metadata)
    }

    pub fn get_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<storage::VersionId>,
    ) -> Result<Option<String>, ServerError> {
        self.head_bucket(bucket)?;
        let pg_id = self.object_pg_id(bucket, key);
        let pg = self.storage_node.get_pg(pg_id)?;
        let stored = match version_id {
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
        if stored.is_delete_marker() {
            return Err(ServerError::MethodNotAllowed);
        }
        pg.get_object_tags(bucket, key, stored.version_id())
            .map_err(ServerError::Metadata)
    }

    pub fn delete_object_tags(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<storage::VersionId>,
    ) -> Result<(), ServerError> {
        self.head_bucket(bucket)?;
        let pg_id = self.object_pg_id(bucket, key);
        let pg = self.storage_node.get_pg(pg_id)?;
        let stored = match version_id {
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
        if stored.is_delete_marker() {
            return Err(ServerError::MethodNotAllowed);
        }
        pg.delete_object_tags(bucket, key, stored.version_id())
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
        version_id: storage::VersionId,
        meta_pg: &storage::PgStore,
        shard_pg: &storage::PgStore,
    ) -> Result<(PutObjectResult, Vec<StreamObjectChunkRecord>), ServerError> {
        // 1. Serialize metadata blob for DB storage (not embedded in shard data).
        let blob_bytes = metadata_blob.serialize()?;

        // 2. Compute ETag (CRC64 of user data only).
        let etag_crc = checksum::crc64::checksum(user_data);

        // 3. Pad user data to multiple of k for equal shard sizes.
        let k = self.ec_config.data_shards as usize;
        let m = self.ec_config.parity_shards as usize;
        let mut padded_data = user_data.to_vec();
        let remainder = padded_data.len() % k;
        if remainder != 0 {
            let pad = k - remainder;
            padded_data.resize(padded_data.len() + pad, 0);
        }

        // 4. Split into k data shards
        let shard_size = padded_data.len() / k;
        let data_shards: Vec<&[u8]> = (0..k)
            .map(|i| &padded_data[i * shard_size..(i + 1) * shard_size])
            .collect();

        // 5. Allocate parity buffers and encode
        let mut parity_bufs: Vec<Vec<u8>> = (0..m).map(|_| vec![0u8; shard_size]).collect();
        let mut parity_refs: Vec<&mut [u8]> =
            parity_bufs.iter_mut().map(|v| v.as_mut_slice()).collect();
        self.ec_codec.encode(&data_shards, &mut parity_refs)?;

        // 6. Compute object_key_hash
        let okh = object_key_hash(bucket, key);

        // 7. Write all k+m shards, with cleanup on failure
        let mut written_shards: Vec<ShardKey> = Vec::with_capacity(k + m);
        let write_result: Result<(), ServerError> = (|| {
            for i in 0..(k + m) {
                let shard_key = ShardKey::new(&okh, version_id.to_u64(), i as u8);
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

        // 8. Record metadata (to metadata PG).
        //    Metadata blob stored in DB row; size == user data length.
        let user_size = user_data.len() as u64;
        let meta_result = meta_pg.put_object_meta(&PutObjectReq::Live(PutLiveObjectReq {
            bucket: BucketName::from(bucket),
            key: ObjectKey::from(key),
            version_id,
            size: user_size,
            etag: storage::ObjectEtag::single_part(etag_crc),
            ec: EcShape {
                k: self.ec_config.data_shards,
                m: self.ec_config.parity_shards,
            },
            layout: ObjectLayout::ChunkManifest,
            metadata_blob: Some(blob_bytes),
        }));

        if let Err(e) = meta_result {
            // Best-effort cleanup of all written shards
            for shard_key in &written_shards {
                let _ = shard_pg.delete_shard(shard_key);
            }
            return Err(ServerError::Metadata(e));
        }

        // Clean up any stale stream_object_chunks metadata rows from a prior
        // stream-write of this (bucket, key, version_id). Metadata deletion
        // must succeed to prevent stale chunk manifests from shadowing the new
        // object on subsequent reads. Shard data cleanup happens after PG locks
        // are released by the caller.
        let stale_chunks = meta_pg
            .get_stream_object_chunks(bucket, key, version_id)
            .map_err(ServerError::Metadata)?;
        if !stale_chunks.is_empty() {
            meta_pg
                .delete_stream_object_chunks(bucket, key, version_id)
                .map_err(ServerError::Metadata)?;
        }

        Ok((
            PutObjectResult {
                etag: format_etag(etag_crc),
                version_id,
            },
            stale_chunks,
        ))
    }

    /// Put an object into storage.
    pub fn put_object(&self, req: &PutObjectRequest) -> Result<PutObjectResult, ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let data = req.data;
        let metadata_blob = req.metadata;
        let cond = req.cond;
        let _bucket_guard = self.storage_node.lock_bucket(bucket);

        if data.len() as u64 > MAX_OBJECT_SIZE {
            return Err(ServerError::ObjectTooLarge {
                size: data.len() as u64,
                max: MAX_OBJECT_SIZE,
            });
        }

        // 1. Verify bucket exists and get versioning state
        let bucket_info = self.head_bucket(bucket)?;
        let LockedWriteObject { version_id, pgs } =
            self.lock_object_pgs_for_write(bucket, key, bucket_info.versioning)?;
        let meta_pg = pgs.meta();
        let shard_pg = pgs.shard();

        // 2. Check write conditions if any are set
        if !cond.is_empty() {
            let existing_etag = match meta_pg.get_object_meta(bucket, key) {
                Ok(stored) => stored.as_live().map(|record| record.etag.format()),
                Err(storage::MetadataError::ObjectNotFound) => None,
                Err(e) => return Err(ServerError::Metadata(e)),
            };
            if matches!(cond, WriteCondition::IfMatch(_)) && existing_etag.is_none() {
                return Err(ServerError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
            check_write_conditions(cond, existing_etag.as_deref())?;
        }

        // 3. Write object while holding both PG locks.
        let (result, stale_chunks) = self.write_object_inner(
            bucket,
            key,
            metadata_blob,
            data,
            version_id,
            meta_pg,
            shard_pg,
        )?;
        drop(pgs);

        // Best-effort shard cleanup after releasing PG locks to avoid
        // lock-order inversion with chunk shard PGs.
        let _ = self.delete_chunk_shards(&stale_chunks);

        Ok(result)
    }

    // ── Streaming upload session API ──────────────────────────────────

    /// Begin a streaming PutObject upload session.
    ///
    /// Creates a session on the metadata PG for `(bucket, key)`. The caller
    /// feeds chunks via `append_stream_chunk` and commits via
    /// `finalize_stream_put`.
    pub fn begin_stream_put(&self, bucket: &str, key: &str) -> Result<String, ServerError> {
        let _bucket_guard = self.storage_node.lock_bucket(bucket);

        // Verify bucket exists.
        let _bucket_info = self.head_bucket(bucket)?;

        // Generate session ID (same pattern as multipart upload_id).
        let rng = ring::rand::SystemRandom::new();
        let mut id_bytes = [0u8; 16];
        ring::rand::SecureRandom::fill(&rng, &mut id_bytes).map_err(|_| {
            ServerError::InternalError {
                reason: "failed to generate session ID".to_string(),
            }
        })?;
        let session_id = id_bytes.iter().fold(String::with_capacity(32), |mut s, b| {
            use std::fmt::Write;
            write!(s, "{b:02x}").unwrap();
            s
        });

        // Lock metadata PG and create session.
        let meta_pg_id = self.object_pg_id(bucket, key);
        let pg = self.storage_node.get_pg(meta_pg_id)?;
        pg.create_stream_upload(&CreateStreamUploadReq {
            session_id: SessionId::from(session_id.as_str()),
            bucket: BucketName::from(bucket),
            key: ObjectKey::from(key),
            target: StreamUploadTarget::PutObject,
        })?;

        Ok(session_id)
    }

    /// Begin a streaming UploadPart session.
    ///
    /// Creates a `StreamUploadKind::UploadPart` session tied to the given
    /// multipart upload. Validates that the upload exists and is InProgress.
    pub fn begin_stream_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
    ) -> Result<BeginStreamPartResult, ServerError> {
        // Validate part number range.
        if part_number == 0 || part_number > 10_000 {
            return Err(ServerError::InvalidArgument {
                reason: format!("part number must be between 1 and 10000, got {part_number}"),
            });
        }

        // Lock metadata PG and validate upload exists.
        let meta_pg_id = self.object_pg_id(bucket, key);
        let pg = self.storage_node.get_pg(meta_pg_id)?;

        let upload = pg.get_multipart_upload(upload_id)?;
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

        // Generate session ID.
        let rng = ring::rand::SystemRandom::new();
        let mut id_bytes = [0u8; 16];
        ring::rand::SecureRandom::fill(&rng, &mut id_bytes).map_err(|_| {
            ServerError::InternalError {
                reason: "failed to generate session ID".to_string(),
            }
        })?;
        let session_id = id_bytes.iter().fold(String::with_capacity(32), |mut s, b| {
            use std::fmt::Write;
            write!(s, "{b:02x}").unwrap();
            s
        });

        pg.create_stream_upload(&CreateStreamUploadReq {
            session_id: SessionId::from(session_id.as_str()),
            bucket: BucketName::from(bucket),
            key: ObjectKey::from(key),
            target: StreamUploadTarget::UploadPart {
                upload_id: UploadId::from(upload_id),
                part_number,
            },
        })?;

        Ok(BeginStreamPartResult {
            session_id,
            checksum_algorithm: upload.checksum.map(|c| c.algorithm()),
        })
    }

    /// Append a chunk of data to an in-progress streaming session.
    ///
    /// Locks the metadata/session PG and the chunk's shard PG in global
    /// ascending order. Validates the session is InProgress, EC-encodes the
    /// chunk, writes shards, and records a staging chunk row.
    ///
    /// The caller must not hold any PG locks when calling this method.
    pub fn append_stream_chunk(
        &self,
        bucket: &str,
        key: &str,
        session_id: &str,
        chunk_index: u32,
        data: &[u8],
    ) -> Result<(), ServerError> {
        let meta_pg_id = self.object_pg_id(bucket, key);

        // Derive chunk shard placement.
        let chunk_okh = chunk_key_hash(session_id, chunk_index);
        let chunk_vid: u64 = 0;
        let shard_pg_id = self.shard_pg_id(
            &format!("chunk/{session_id}"),
            &chunk_index.to_string(),
            storage::VersionId::Null,
        );

        // Lock metadata PG + shard PG in global ascending order.
        let (meta_guard, shard_guard) = if shard_pg_id == meta_pg_id {
            (self.storage_node.get_pg(meta_pg_id)?, None)
        } else if meta_pg_id < shard_pg_id {
            let mg = self.storage_node.get_pg(meta_pg_id)?;
            let sg = self.storage_node.get_pg(shard_pg_id)?;
            (mg, Some(sg))
        } else {
            let (mg, sg) = self.storage_node.lock_two_pgs(meta_pg_id, shard_pg_id)?;
            (mg, sg)
        };
        let shard_pg: &storage::PgStore = shard_guard.as_deref().unwrap_or(&meta_guard);

        // Validate session is InProgress and matches bucket/key/op_kind.
        let session = meta_guard.get_stream_upload(session_id)?;
        if session.state != StreamUploadState::InProgress {
            return Err(ServerError::InvalidRequest {
                reason: "stream session is not in progress".to_string(),
            });
        }
        if session.bucket != bucket || session.key != key {
            return Err(ServerError::InvalidRequest {
                reason: "session bucket/key mismatch".to_string(),
            });
        }
        // Reject duplicate chunk_index — writing shards then failing on PK
        // constraint would delete the already-staged chunk's shard data.
        let existing_chunks = meta_guard
            .list_stream_chunks(session_id)
            .map_err(ServerError::Metadata)?;
        if existing_chunks.iter().any(|c| c.chunk_index == chunk_index) {
            return Err(ServerError::InvalidRequest {
                reason: format!("duplicate chunk_index {chunk_index}"),
            });
        }

        // EC-encode chunk data.
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

        // Write shards with cleanup on failure.
        let mut written_shards: Vec<ShardKey> = Vec::with_capacity(k + m);
        let write_result: Result<(), ServerError> = (|| {
            for i in 0..(k + m) {
                let shard_key = ShardKey::new(&chunk_okh, chunk_vid, i as u8);
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

        // Record staging chunk row.
        let chunk_result = meta_guard.append_stream_chunk(&StreamUploadChunkRecord {
            session_id: SessionId::from(session_id),
            chunk_index,
            size: data.len() as u64,
            chunk_okh,
            chunk_vid,
            shard_pg_id,
            ec_k: self.ec_config.data_shards,
            ec_m: self.ec_config.parity_shards,
        });

        if let Err(e) = chunk_result {
            // Best-effort cleanup of written shards.
            for shard_key in &written_shards {
                let _ = shard_pg.delete_shard(shard_key);
            }
            return Err(ServerError::Metadata(e));
        }

        Ok(())
    }

    /// Finalize a streaming PutObject session.
    ///
    /// Locks the metadata PG, allocates a version_id, builds committed chunk
    /// manifest from staging rows, and atomically commits the object via
    /// `commit_stream_put`.
    ///
    /// The caller passes the running CRC64 checksum, total size, and metadata
    /// blob computed during the append phase. No chunk data is re-read.
    pub fn finalize_stream_put(
        &self,
        req: &FinalizeStreamPutRequest,
    ) -> Result<PutObjectResult, ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let session_id = req.session_id;
        let crc64 = req.crc64;
        let total_size = req.total_size;
        let metadata_blob = req.metadata_blob;
        let cond = req.cond;
        let _bucket_guard = self.storage_node.lock_bucket(bucket);

        let bucket_info = self.head_bucket(bucket)?;
        let blob_bytes = metadata_blob.serialize()?;

        let meta_pg_id = self.object_pg_id(bucket, key);
        let meta_guard = self.storage_node.get_pg(meta_pg_id)?;

        // Validate session is InProgress and matches bucket/key.
        let session = meta_guard.get_stream_upload(session_id)?;
        if session.state != StreamUploadState::InProgress {
            return Err(ServerError::InvalidRequest {
                reason: "stream session is not in progress".to_string(),
            });
        }
        if session.bucket != bucket || session.key != key {
            return Err(ServerError::InvalidRequest {
                reason: "session bucket/key mismatch".to_string(),
            });
        }
        if session.target != StreamUploadTarget::PutObject {
            return Err(ServerError::InvalidRequest {
                reason: "session is not a PutObject session".to_string(),
            });
        }

        // Check write conditions.
        if !cond.is_empty() {
            let existing_etag = match meta_guard.get_object_meta(bucket, key) {
                Ok(stored) => stored.as_live().map(|record| record.etag.format()),
                Err(storage::MetadataError::ObjectNotFound) => None,
                Err(e) => return Err(ServerError::Metadata(e)),
            };
            if matches!(cond, WriteCondition::IfMatch(_)) && existing_etag.is_none() {
                return Err(ServerError::ObjectNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
            check_write_conditions(cond, existing_etag.as_deref())?;
        }

        // Allocate version_id.
        let version_id = if bucket_info.versioning == storage::BucketVersioningState::Enabled {
            meta_guard.next_version_id(bucket, key)?
        } else {
            storage::VersionId::Null
        };

        // Build committed chunk manifest from staging rows and validate total_size.
        let staging_chunks = meta_guard
            .list_stream_chunks(session_id)
            .map_err(ServerError::Metadata)?;
        let chunks_total: u64 = staging_chunks.iter().map(|c| c.size).sum();
        if chunks_total != total_size {
            return Err(ServerError::InvalidRequest {
                reason: format!(
                    "total_size mismatch: caller passed {total_size} but staged chunks sum to {chunks_total}"
                ),
            });
        }
        let committed_chunks: Vec<StreamObjectChunkRecord> = staging_chunks
            .iter()
            .map(|c| StreamObjectChunkRecord {
                bucket: BucketName::from(bucket),
                key: ObjectKey::from(key),
                version_id,
                chunk_index: c.chunk_index,
                size: c.size,
                chunk_okh: c.chunk_okh,
                chunk_vid: c.chunk_vid,
                shard_pg_id: c.shard_pg_id,
                ec_k: c.ec_k,
                ec_m: c.ec_m,
            })
            .collect();

        // Atomic finalize: commit object metadata + chunk manifest, delete staging.
        meta_guard
            .commit_stream_put(
                session_id,
                &CommitStreamPutReq {
                    bucket: BucketName::from(bucket),
                    key: ObjectKey::from(key),
                    version_id,
                    size: total_size,
                    etag_crc64: crc64,
                    ec: EcShape {
                        k: self.ec_config.data_shards,
                        m: self.ec_config.parity_shards,
                    },
                    metadata_blob: Some(blob_bytes),
                },
                &committed_chunks,
            )
            .map_err(ServerError::Metadata)?;

        Ok(PutObjectResult {
            etag: format_etag(crc64),
            version_id,
        })
    }

    /// Finalize a streaming UploadPart session.
    ///
    /// Locks the metadata PG, builds committed chunk manifest from staging
    /// rows, and atomically commits the part via `commit_stream_part`.
    /// `computed_checksum` is the actual checksum bytes computed incrementally
    /// during streaming. If `None`, the checksum is derived from `claimed_checksum`.
    pub fn finalize_stream_part(
        &self,
        req: FinalizeStreamPartRequest,
    ) -> Result<UploadPartResult, ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let session_id = req.session_id;
        let upload_id = req.upload_id;
        let part_number = req.part_number;
        let crc64 = req.crc64;
        let total_size = req.total_size;
        let claimed_checksum = req.claimed_checksum;
        let computed_checksum = req.computed_checksum;
        let meta_pg_id = self.object_pg_id(bucket, key);
        let meta_guard = self.storage_node.get_pg(meta_pg_id)?;

        // Validate session.
        let session = meta_guard.get_stream_upload(session_id)?;
        if session.state != StreamUploadState::InProgress {
            return Err(ServerError::InvalidRequest {
                reason: "stream session is not in progress".to_string(),
            });
        }
        if session.bucket != bucket || session.key != key {
            return Err(ServerError::InvalidRequest {
                reason: "session bucket/key mismatch".to_string(),
            });
        }
        match &session.target {
            StreamUploadTarget::UploadPart {
                upload_id: sess_upload_id,
                part_number: sess_part_number,
            } if sess_upload_id == upload_id && *sess_part_number == part_number => {}
            StreamUploadTarget::UploadPart { .. } => {
                return Err(ServerError::InvalidRequest {
                    reason: "session upload_id/part_number mismatch".to_string(),
                });
            }
            StreamUploadTarget::PutObject => {
                return Err(ServerError::InvalidRequest {
                    reason: "session is not an UploadPart session".to_string(),
                });
            }
        }

        // Validate upload still exists and is InProgress.
        let upload = meta_guard.get_multipart_upload(upload_id)?;
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

        // Resolve checksum algorithm: upload-level takes precedence.
        let claimed_algo = claimed_checksum.map(|c| c.algorithm());
        let upload_checksum_algo = upload.checksum.map(|c| c.algorithm());
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
            (None, Some(part_algo)) => Some(part_algo),
            (None, None) => None,
        };

        // Use only a computed checksum from the streaming loop. This prevents
        // persisting unverified checksum claims from request headers.
        let checksum_bytes = if let Some(cksum) = computed_checksum {
            let algo = cksum.algorithm();
            let bytes = cksum.bytes();
            if let Some(ea) = effective_algo {
                if ea != algo {
                    return Err(ServerError::InvalidRequest {
                        reason: format!(
                            "computed checksum algorithm {} doesn't match effective {}",
                            algo.as_str(),
                            ea.as_str()
                        ),
                    });
                }
            }
            if let Some(claim) = &claimed_checksum {
                if claim.algorithm() != algo {
                    return Err(ServerError::InvalidRequest {
                        reason: format!(
                            "claimed checksum algorithm {} doesn't match computed {}",
                            claim.algorithm().as_str(),
                            algo.as_str()
                        ),
                    });
                }
                if claim.expected_bytes() != bytes {
                    return Err(ServerError::BadDigest);
                }
            }
            Some(bytes.to_vec())
        } else if effective_algo.is_some() || claimed_checksum.is_some() {
            return Err(ServerError::InvalidRequest {
                reason: "missing computed checksum for streaming upload part".to_string(),
            });
        } else {
            None
        };

        // Determine generation for this part.
        let generation = match meta_guard.get_multipart_part(upload_id, part_number) {
            Ok(existing) => existing.generation + 1,
            Err(storage::MetadataError::PartNotFound { .. }) => 0,
            Err(e) => return Err(ServerError::Metadata(e)),
        };

        // Build committed chunk manifest from staging rows.
        let staging_chunks = meta_guard
            .list_stream_chunks(session_id)
            .map_err(ServerError::Metadata)?;
        let chunks_total: u64 = staging_chunks.iter().map(|c| c.size).sum();
        if chunks_total != total_size {
            return Err(ServerError::InvalidRequest {
                reason: format!(
                    "total_size mismatch: caller passed {total_size} but staged chunks sum to {chunks_total}"
                ),
            });
        }

        let committed_chunks: Vec<MultipartPartChunkRecord> = staging_chunks
            .iter()
            .map(|c| MultipartPartChunkRecord {
                bucket: BucketName::from(bucket),
                key: ObjectKey::from(key),
                upload_id: UploadId::from(upload_id),
                version_id: u64::MAX, // staging sentinel — reparented at CompleteMultipartUpload time
                part_number,
                chunk_index: c.chunk_index,
                size: c.size,
                chunk_okh: c.chunk_okh,
                chunk_vid: c.chunk_vid,
                shard_pg_id: c.shard_pg_id,
                ec_k: c.ec_k,
                ec_m: c.ec_m,
            })
            .collect();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let part_record = MultipartPartRecord {
            upload_id: UploadId::from(upload_id),
            part_number,
            generation,
            size: total_size,
            etag: crc64_to_etag_bytes(crc64),
            etag_kind: storage::EtagKind::Crc64,
            part_okh: [0u8; 16], // no single-shard placement for streamed parts
            part_vid: generation as u64,
            ec_k: self.ec_config.data_shards,
            ec_m: self.ec_config.parity_shards,
            last_modified: now,
            checksum: checksum_bytes.clone(),
        };

        // Atomic commit: upsert part, insert chunks, delete staging.
        meta_guard
            .commit_stream_part(session_id, &part_record, &committed_chunks)
            .map_err(ServerError::Metadata)?;

        // Best-effort cleanup of prior generation's shards.
        // The prior generation used the non-streaming path, so clean its
        // single shard set. If it was also a streamed part, clean its chunks.
        drop(meta_guard);
        if generation > 0 {
            let old_gen = generation - 1;
            // Clean old non-streaming shards.
            let old_okh = part_key_hash(upload_id, part_number, old_gen);
            let old_vid = old_gen as u64;
            let old_shard_pg_id = self.shard_pg_id(
                &format!("mpu/{upload_id}"),
                &format!("{part_number}/{old_gen}"),
                storage::VersionId::from_u64(old_vid),
            );
            if let Ok(old_pg) = self.storage_node.get_pg(old_shard_pg_id) {
                let k = self.ec_config.data_shards as usize;
                let m = self.ec_config.parity_shards as usize;
                for i in 0..(k + m) {
                    let old_key = ShardKey::new(&old_okh, old_vid, i as u8);
                    let _ = old_pg.delete_shard(&old_key);
                }
            }
            // Clean old streamed-part chunks (if prior generation was streamed).
            // commit_stream_part already handles deleting prior multipart_part_chunks
            // in its transaction, but the shard data on disk needs cleanup.
            if let Ok(pg) = self.storage_node.get_pg(meta_pg_id) {
                if let Ok(old_chunks) = pg.get_multipart_part_chunks(
                    bucket,
                    key,
                    storage::VersionId::from_u64(old_vid),
                    part_number,
                ) {
                    drop(pg);
                    let _ = self.delete_chunk_shards_generic(&old_chunks);
                }
            }
        }

        let checksum = match (effective_algo, checksum_bytes) {
            (Some(algo), Some(bytes)) => {
                Some(storage::RawChecksum::new(algo, bytes).map_err(|_| {
                    ServerError::InternalError {
                        reason: "computed checksum length does not match algorithm".into(),
                    }
                })?)
            }
            _ => None,
        };

        Ok(UploadPartResult {
            etag: format_etag(crc64),
            checksum,
        })
    }

    /// Abort a streaming upload session.
    ///
    /// Marks the session as Aborted and deletes staging rows. Best-effort
    /// cleans up shard data written during append.
    pub fn abort_stream_put(
        &self,
        bucket: &str,
        key: &str,
        session_id: &str,
    ) -> Result<(), ServerError> {
        let meta_pg_id = self.object_pg_id(bucket, key);
        let meta_guard = self.storage_node.get_pg(meta_pg_id)?;

        // Validate session exists and matches bucket/key.
        let session = meta_guard.get_stream_upload(session_id)?;
        if session.bucket != bucket || session.key != key {
            return Err(ServerError::InvalidRequest {
                reason: "session bucket/key mismatch".to_string(),
            });
        }

        // Collect staging chunks for shard cleanup before deleting session.
        let staging_chunks = meta_guard
            .list_stream_chunks(session_id)
            .map_err(ServerError::Metadata)?;

        // Set state to Aborted, then delete session (CASCADE deletes staging chunks).
        meta_guard
            .set_stream_upload_state(session_id, StreamUploadState::Aborted)
            .map_err(ServerError::Metadata)?;
        meta_guard
            .delete_stream_upload(session_id)
            .map_err(ServerError::Metadata)?;

        // Drop the PG lock before best-effort shard cleanup, which may need
        // to lock other PGs.
        drop(meta_guard);

        // Best-effort cleanup of chunk shards.
        for chunk in &staging_chunks {
            if let Ok(shard_guard) = self.storage_node.get_pg(chunk.shard_pg_id) {
                let k = chunk.ec_k as usize;
                let m = chunk.ec_m as usize;
                for i in 0..(k + m) {
                    let shard_key = ShardKey::new(&chunk.chunk_okh, chunk.chunk_vid, i as u8);
                    let _ = shard_guard.delete_shard(&shard_key);
                }
            }
        }

        Ok(())
    }

    /// Scavenge abandoned streaming upload sessions across all PGs.
    ///
    /// Aborts any session older than `max_age_ms` milliseconds. Intended to
    /// be called at startup and periodically to clean up sessions left behind
    /// by crashed processes.
    ///
    /// Returns the number of sessions scavenged.
    pub fn scavenge_stale_sessions(&self, max_age_ms: u64) -> usize {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff = now.saturating_sub(max_age_ms);
        let mut count = 0;

        let _ = self.pg_topology.for_each_pg(|pg_id| {
            let pg = match self.storage_node.get_pg(pg_id) {
                Ok(pg) => pg,
                Err(_) => return Ok::<(), ()>(()),
            };

            let sessions = match pg.list_all_stream_uploads() {
                Ok(s) => s,
                Err(_) => return Ok::<(), ()>(()),
            };

            // Drop the PG lock before aborting — abort_stream_put acquires
            // its own locks in the correct order.
            drop(pg);

            for session in sessions {
                if session.created_at < cutoff
                    && self
                        .abort_stream_put(&session.bucket, &session.key, &session.session_id)
                        .is_ok()
                {
                    count += 1;
                }
            }
            Ok::<(), ()>(())
        });

        count
    }

    /// Copy an object from one location to another.
    ///
    /// Supports conditional headers on both source and destination,
    /// and metadata directive (COPY preserves source metadata, REPLACE
    /// uses new headers).
    pub fn copy_object(
        &self,
        req: &CopyObjectRequest,
    ) -> Result<CopyObjectResult, ServerError> {
        let src_bucket = req.source.bucket;
        let src_key = req.source.key;
        let src_version_id = req.source.version_id;
        let dst_bucket = req.dst_bucket;
        let dst_key = req.dst_key;
        let src_cond = req.source.condition;
        let dst_cond = req.dst_condition;
        let directive = &req.directive;
        let _bucket_guard = self.storage_node.lock_bucket(dst_bucket);

        // Phase 1: Read source object
        let (src_metadata, user_data) = {
            let LockedReadObject {
                record: src_stored,
                pgs,
            } = self.lock_object_pgs_for_read(src_bucket, src_key, src_version_id)?;

            // Reject delete markers — they are not copyable objects.
            // AWS returns 400/InvalidRequest when an explicit versionId targets a
            // delete marker, and 404/NoSuchKey when current version is a delete marker.
            let src_record = match src_stored {
                StoredObject::Live(r) => r,
                StoredObject::DeleteMarker(_) => {
                    return if src_version_id.is_some() {
                        Err(ServerError::InvalidRequest {
                            reason: "The source of a copy request may not specifically refer to a delete marker by version id.".to_string(),
                        })
                    } else {
                        Err(ServerError::ObjectNotFound {
                            bucket: src_bucket.to_string(),
                            key: src_key.to_string(),
                        })
                    };
                }
            };

            let src_etag = src_record.etag.format();
            check_copy_source_conditions(src_cond, &src_etag, src_record.last_modified)?;

            let not_found = |e: ServerError| match e {
                ServerError::Store(storage::StoreError::NotFound) => ServerError::ObjectNotFound {
                    bucket: src_bucket.to_string(),
                    key: src_key.to_string(),
                },
                other => other,
            };

            if matches!(src_record.layout, ObjectLayout::MultipartManifest { .. }) {
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
                // Non-multipart source: metadata from DB row, user data from shards.
                let src_etag_crc = src_record.etag.crc64();
                let user_size = src_record.size as usize;

                // Check for chunk manifest (stream-put objects).
                let meta_pg = pgs.meta();
                let chunks = meta_pg
                    .get_stream_object_chunks(src_bucket, src_key, src_record.version_id)
                    .map_err(ServerError::Metadata)?;

                let user_data = if user_size == 0 {
                    drop(pgs);
                    vec![]
                } else if !chunks.is_empty() {
                    drop(pgs);
                    self.read_chunk_manifest_range(src_bucket, src_key, &chunks, 0, user_size - 1)
                        .map_err(not_found)?
                } else {
                    let src_okh = object_key_hash(src_bucket, src_key);
                    let src_shard_pg = pgs.shard();
                    let data = self
                        .read_range(
                            src_shard_pg,
                            &src_okh,
                            src_record.version_id,
                            &src_record,
                            0,
                            user_size - 1,
                        )
                        .map_err(not_found)?;
                    drop(pgs);
                    data
                };

                // Verify CRC against stored etag (user data only).
                let actual_crc = checksum::crc64::checksum(&user_data);
                if actual_crc != src_etag_crc {
                    return Err(ServerError::IntegrityError {
                        bucket: src_bucket.to_string(),
                        key: src_key.to_string(),
                        expected: src_etag_crc,
                        actual: actual_crc,
                    });
                }

                let src_metadata = src_record
                    .metadata_blob
                    .as_ref()
                    .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
                    .transpose()?
                    .unwrap_or_default();

                (src_metadata, user_data)
            }
        }; // source locks dropped here

        // Phase 2: Write destination object
        let metadata_blob = match directive {
            MetadataDirective::Copy => src_metadata,
            MetadataDirective::Replace {
                metadata: new_metadata,
                checksum_algorithm,
            } => {
                let mut blob = (*new_metadata).clone();

                // If a checksum algorithm is specified, compute a fresh checksum
                // from the copied data and store it in the metadata.
                if let Some(algo) = checksum_algorithm {
                    use base64::Engine;
                    let cksum = compute_checksum(*algo, &user_data);
                    let b64 = base64::engine::general_purpose::STANDARD.encode(&cksum);
                    blob.set(algo.header_name(), &b64);
                }

                blob
            }
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
                Ok(stored) => stored.as_live().map(|record| record.etag.format()),
                Err(storage::MetadataError::ObjectNotFound) => None,
                Err(e) => return Err(ServerError::Metadata(e)),
            };
            check_write_conditions(dst_cond, existing_etag.as_deref())?;
        }

        let (put_result, stale_chunks) = self.write_object_inner(
            dst_bucket,
            dst_key,
            &metadata_blob,
            &user_data,
            dst_version_id,
            dst_meta_pg,
            dst_shard_pg,
        )?;

        // Read back dest metadata to get the authoritative last_modified
        let dst_stored = dst_meta_pg
            .get_object_meta(dst_bucket, dst_key)
            .map_err(ServerError::Metadata)?;
        drop(pgs);

        // Best-effort shard cleanup after releasing PG locks.
        let _ = self.delete_chunk_shards(&stale_chunks);

        Ok(CopyObjectResult {
            etag: put_result.etag,
            last_modified: dst_stored.last_modified(),
            version_id: put_result.version_id,
        })
    }

    fn lookup_object_record(
        meta_pg: &storage::PgStore,
        bucket: &str,
        key: &str,
        version_id: Option<storage::VersionId>,
    ) -> Result<StoredObject, ServerError> {
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
        version_id: Option<storage::VersionId>,
    ) -> Result<LockedReadObject<'a>, ServerError> {
        let meta_pg_id = self.object_pg_id(bucket, key);

        loop {
            let meta_guard = self.storage_node.get_pg(meta_pg_id)?;
            let record = Self::lookup_object_record(&meta_guard, bucket, key, version_id)?;
            let shard_pg_id = self.shard_pg_id(bucket, key, record.version_id());

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
            let verify_shard_pg_id = self.shard_pg_id(bucket, key, record.version_id());

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
        versioning_state: storage::BucketVersioningState,
    ) -> Result<LockedWriteObject<'a>, ServerError> {
        let meta_pg_id = self.object_pg_id(bucket, key);

        loop {
            let meta_guard = self.storage_node.get_pg(meta_pg_id)?;
            let version_id = if versioning_state == storage::BucketVersioningState::Enabled {
                meta_guard.next_version_id(bucket, key)?
            } else {
                storage::VersionId::Null
            };
            let shard_pg_id = self.shard_pg_id(bucket, key, version_id);

            if shard_pg_id == meta_pg_id {
                return Ok(LockedWriteObject {
                    version_id,
                    pgs: TwoPgGuards::new(meta_guard, None),
                });
            }

            if meta_pg_id < shard_pg_id {
                let shard_guard = self.storage_node.get_pg(shard_pg_id)?;
                if versioning_state == storage::BucketVersioningState::Enabled {
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
            let version_id = if versioning_state == storage::BucketVersioningState::Enabled {
                meta_guard.next_version_id(bucket, key)?
            } else {
                storage::VersionId::Null
            };
            let verify_shard_pg_id = self.shard_pg_id(bucket, key, version_id);
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
        version_id: storage::VersionId,
        record: &LiveObjectRecord,
        needed: &[usize],
    ) -> Result<(Vec<Vec<u8>>, usize), ServerError> {
        let k = record.ec.k as usize;
        let m = record.ec.m as usize;

        // Try reading just the needed shards first
        let mut result_shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(needed.len());
        let mut all_present = true;
        let mut shard_size = 0;

        for &idx in needed {
            let shard_key = ShardKey::new(okh, version_id.to_u64(), idx as u8);
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
            let shard_key = ShardKey::new(okh, version_id.to_u64(), i as u8);
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
            let codec = if record.ec.k == self.ec_config.data_shards
                && record.ec.m == self.ec_config.parity_shards
            {
                &self.ec_codec
            } else {
                let ec_config = EcConfig::new(record.ec.k, record.ec.m)?;
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

    /// Compute shard_size from object size and EC k.
    ///
    /// `size` is the pre-padding user-data size.
    /// Returns the per-shard size after padding to a multiple of k.
    fn compute_shard_size(size: u64, ec_k: u8) -> usize {
        let k = ec_k as u64;
        let padded = size.div_ceil(k) * k;
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

    /// Read a byte range [start, end] (inclusive) from the stored user data.
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
        version_id: storage::VersionId,
        record: &LiveObjectRecord,
        start: usize,
        end: usize,
    ) -> Result<Vec<u8>, ServerError> {
        let shard_size = Self::compute_shard_size(record.size, record.ec.k);
        if shard_size == 0 {
            return Ok(vec![]);
        }

        let needed = Self::shards_for_byte_range(start, end, shard_size, record.ec.k);
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

    /// Read full data for a single stream object chunk from its shard PG.
    ///
    /// Parallel to `read_part_data` but for `StreamObjectChunkRecord`.
    fn read_chunk_data(&self, chunk: &StreamObjectChunkRecord) -> Result<Vec<u8>, ServerError> {
        let pg = self.storage_node.get_pg(chunk.shard_pg_id)?;
        let k = chunk.ec_k as usize;
        let m = chunk.ec_m as usize;

        let padded = (chunk.size as usize).div_ceil(k) * k;
        let shard_size = padded / k;

        if shard_size == 0 {
            return Ok(vec![]);
        }

        let needed: Vec<usize> = (0..k).collect();
        let mut all_shards: Vec<Option<Vec<u8>>> = Vec::with_capacity(k + m);
        let mut present_count = 0;

        for i in 0..(k + m) {
            let shard_key = ShardKey::new(&chunk.chunk_okh, chunk.chunk_vid, i as u8);
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

        let all_data_present = (0..k).all(|i| all_shards[i].is_some());
        if !all_data_present {
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
            let codec = if chunk.ec_k == self.ec_config.data_shards
                && chunk.ec_m == self.ec_config.parity_shards
            {
                &self.ec_codec
            } else {
                let ec_config = EcConfig::new(chunk.ec_k, chunk.ec_m)?;
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

        let mut buf = Vec::with_capacity(padded);
        for shard in all_shards.iter().take(k) {
            buf.extend_from_slice(shard.as_ref().unwrap());
        }
        buf.truncate(chunk.size as usize);
        Ok(buf)
    }

    /// Read a byte range from a chunk-manifest object by traversing its chunks.
    ///
    /// Maps [start, end] (inclusive) to the relevant chunks, reads each,
    /// and concatenates the needed slices.
    fn read_chunk_manifest_range(
        &self,
        bucket: &str,
        key: &str,
        chunks: &[StreamObjectChunkRecord],
        start: usize,
        end: usize,
    ) -> Result<Vec<u8>, ServerError> {
        let total_len = end - start + 1;
        let mut result = Vec::with_capacity(total_len);
        let mut offset: usize = 0;

        for chunk in chunks {
            let chunk_start = offset;
            let chunk_end = offset + chunk.size as usize; // exclusive

            if chunk_start > end {
                break;
            }
            if chunk.size == 0 || chunk_end <= start {
                offset = chunk_end;
                continue;
            }

            let slice_start = start.saturating_sub(chunk_start);
            let slice_end = if end < chunk_end - 1 {
                end - chunk_start
            } else {
                chunk.size as usize - 1
            };

            let data = self.read_chunk_data(chunk)?;
            result.extend_from_slice(&data[slice_start..=slice_end]);
            offset = chunk_end;
        }

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

    /// Read a streaming part's data via its chunk manifest.
    ///
    /// Converts `MultipartPartChunkRecord`s to `StreamObjectChunkRecord`s
    /// and delegates to `read_chunk_manifest_range`.
    fn read_streaming_part_data(
        &self,
        bucket: &str,
        key: &str,
        chunks: &[storage::MultipartPartChunkRecord],
        start: usize,
        end: usize,
    ) -> Result<Vec<u8>, ServerError> {
        let stream_chunks: Vec<storage::StreamObjectChunkRecord> = chunks
            .iter()
            .map(|c| storage::StreamObjectChunkRecord {
                bucket: c.bucket.clone(),
                key: c.key.clone(),
                version_id: storage::VersionId::from_u64(c.version_id),
                chunk_index: c.chunk_index,
                size: c.size,
                chunk_okh: c.chunk_okh,
                chunk_vid: c.chunk_vid,
                shard_pg_id: c.shard_pg_id,
                ec_k: c.ec_k,
                ec_m: c.ec_m,
            })
            .collect();
        self.read_chunk_manifest_range(bucket, key, &stream_chunks, start, end)
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

    /// Delete all shards for a list of stream object chunks.
    fn delete_chunk_shards(&self, chunks: &[StreamObjectChunkRecord]) -> Result<(), ServerError> {
        for chunk in chunks {
            self.delete_chunk_shard_set(
                chunk.shard_pg_id,
                &chunk.chunk_okh,
                chunk.chunk_vid,
                chunk.ec_k,
                chunk.ec_m,
            )?;
        }
        Ok(())
    }

    fn delete_chunk_shards_generic(
        &self,
        chunks: &[MultipartPartChunkRecord],
    ) -> Result<(), ServerError> {
        for chunk in chunks {
            self.delete_chunk_shard_set(
                chunk.shard_pg_id,
                &chunk.chunk_okh,
                chunk.chunk_vid,
                chunk.ec_k,
                chunk.ec_m,
            )?;
        }
        Ok(())
    }

    fn delete_chunk_shard_set(
        &self,
        shard_pg_id: u32,
        chunk_okh: &[u8; 16],
        chunk_vid: u64,
        ec_k: u8,
        ec_m: u8,
    ) -> Result<(), ServerError> {
        let pg = self.storage_node.get_pg(shard_pg_id)?;
        let total = ec_k as usize + ec_m as usize;
        for i in 0..total {
            let shard_key = ShardKey::new(chunk_okh, chunk_vid, i as u8);
            pg.delete_shard(&shard_key)?;
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

        // Metadata PG for looking up chunk manifests of streaming parts.
        let meta_pg_id = self.object_pg_id(bucket, key);

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

            let data = if part.part_okh == [0u8; 16] {
                // Streaming part: read via chunk manifest from metadata PG.
                let chunks = {
                    let meta_pg = self.storage_node.get_pg(meta_pg_id)?;
                    meta_pg
                        .get_multipart_part_chunks(bucket, key, part.version_id, part.part_number)
                        .map_err(ServerError::Metadata)?
                };
                self.read_streaming_part_data(bucket, key, &chunks, 0, part.size as usize - 1)?
            } else {
                self.read_part_data(part)?
            };
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
    pub fn get_object(&self, req: &GetObjectRequest) -> Result<GetObjectResult, ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let version_id = req.version_id;
        let cond = req.cond;
        let LockedReadObject {
            record: stored,
            pgs,
        } = self.lock_object_pgs_for_read(bucket, key, version_id)?;

        // If latest version is a delete marker, return 404 with x-amz-delete-marker
        let record = match stored {
            StoredObject::Live(r) => r,
            StoredObject::DeleteMarker(_) => {
                return Err(ServerError::DeleteMarkerHit {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        };

        let etag_str = record.etag.format();
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
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
            // Non-multipart: metadata from DB row, user data from shards.
            let etag_crc = record.etag.crc64();
            let user_size = record.size as usize;

            // Check for chunk manifest (stream-put objects).
            let meta_pg = pgs.meta();
            let chunks = meta_pg
                .get_stream_object_chunks(bucket, key, record.version_id)
                .map_err(ServerError::Metadata)?;

            let user_data = if user_size == 0 {
                drop(pgs);
                vec![]
            } else if !chunks.is_empty() {
                // Chunk-manifest object: read from per-chunk shard sets.
                drop(pgs);
                self.read_chunk_manifest_range(bucket, key, &chunks, 0, user_size - 1)
                    .map_err(|e| match e {
                        ServerError::Store(storage::StoreError::NotFound) => {
                            ServerError::ObjectNotFound {
                                bucket: bucket.to_string(),
                                key: key.to_string(),
                            }
                        }
                        other => other,
                    })?
            } else {
                // Single shard set (non-streamed write).
                let okh = object_key_hash(bucket, key);
                let shard_pg = pgs.shard();
                let data = self
                    .read_range(shard_pg, &okh, record.version_id, &record, 0, user_size - 1)
                    .map_err(|e| match e {
                        ServerError::Store(storage::StoreError::NotFound) => {
                            ServerError::ObjectNotFound {
                                bucket: bucket.to_string(),
                                key: key.to_string(),
                            }
                        }
                        other => other,
                    })?;
                drop(pgs);
                data
            };

            // Verify CRC against stored etag (user data only).
            let actual_crc = checksum::crc64::checksum(&user_data);
            if actual_crc != etag_crc {
                return Err(ServerError::IntegrityError {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    expected: etag_crc,
                    actual: actual_crc,
                });
            }

            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

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
        req: &GetObjectPartRequest,
    ) -> Result<GetObjectPartResult, ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let version_id = req.version_id;
        let part_number = req.part_number;
        let cond = req.cond;
        let LockedReadObject {
            record: stored,
            pgs,
        } = self.lock_object_pgs_for_read(bucket, key, version_id)?;

        let record = match stored {
            StoredObject::Live(r) => r,
            StoredObject::DeleteMarker(_) => {
                return Err(ServerError::DeleteMarkerHit {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        };

        let etag_str = record.etag.format();
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
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

            let data = if part.size == 0 {
                Ok(vec![])
            } else if part.part_okh == [0u8; 16] {
                // Streaming part: read via chunk manifest from metadata PG.
                let meta_pg_id = self.object_pg_id(bucket, key);
                let chunks = {
                    let meta_pg = self.storage_node.get_pg(meta_pg_id)?;
                    meta_pg
                        .get_multipart_part_chunks(bucket, key, record.version_id, part.part_number)
                        .map_err(ServerError::Metadata)?
                };
                self.read_streaming_part_data(bucket, key, &chunks, 0, part.size as usize - 1)
            } else {
                self.read_part_data(part)
            }
            .map_err(|e| match e {
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
                // Look up algorithm from object metadata and validate byte length.
                match metadata
                    .get("x-amz-checksum-algorithm")
                    .and_then(ChecksumAlgorithm::parse)
                {
                    Some(algo) => {
                        Some(storage::RawChecksum::new(algo, raw.clone()).map_err(|_| {
                            ServerError::InternalError {
                                reason: format!(
                                    "stored checksum length {} does not match {} (expected {})",
                                    raw.len(),
                                    algo.as_str(),
                                    algo.expected_byte_length(),
                                ),
                            }
                        })?)
                    }
                    None => None,
                }
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

            let etag_crc = record.etag.crc64();
            let user_size = record.size as usize;

            // Check for chunk manifest (stream-put objects).
            let meta_pg = pgs.meta();
            let chunks = meta_pg
                .get_stream_object_chunks(bucket, key, record.version_id)
                .map_err(ServerError::Metadata)?;

            let user_data = if user_size == 0 {
                drop(pgs);
                vec![]
            } else if !chunks.is_empty() {
                drop(pgs);
                self.read_chunk_manifest_range(bucket, key, &chunks, 0, user_size - 1)
                    .map_err(|e| match e {
                        ServerError::Store(storage::StoreError::NotFound) => {
                            ServerError::ObjectNotFound {
                                bucket: bucket.to_string(),
                                key: key.to_string(),
                            }
                        }
                        other => other,
                    })?
            } else {
                let okh = object_key_hash(bucket, key);
                let shard_pg = pgs.shard();
                let data = self
                    .read_range(shard_pg, &okh, record.version_id, &record, 0, user_size - 1)
                    .map_err(|e| match e {
                        ServerError::Store(storage::StoreError::NotFound) => {
                            ServerError::ObjectNotFound {
                                bucket: bucket.to_string(),
                                key: key.to_string(),
                            }
                        }
                        other => other,
                    })?;
                drop(pgs);
                data
            };

            let actual_crc = checksum::crc64::checksum(&user_data);
            if actual_crc != etag_crc {
                return Err(ServerError::IntegrityError {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    expected: etag_crc,
                    actual: actual_crc,
                });
            }

            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

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
        req: &GetObjectPartRequest,
    ) -> Result<HeadObjectPartResult, ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let version_id = req.version_id;
        let part_number = req.part_number;
        let cond = req.cond;
        let LockedReadObject {
            record: stored,
            pgs,
        } = self.lock_object_pgs_for_read(bucket, key, version_id)?;

        let record = match stored {
            StoredObject::Live(r) => r,
            StoredObject::DeleteMarker(_) => {
                return Err(ServerError::DeleteMarkerHit {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        };

        let etag_str = record.etag.format();
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
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
                match metadata
                    .get("x-amz-checksum-algorithm")
                    .and_then(ChecksumAlgorithm::parse)
                {
                    Some(algo) => {
                        Some(storage::RawChecksum::new(algo, raw.clone()).map_err(|_| {
                            ServerError::InternalError {
                                reason: format!(
                                    "stored checksum length {} does not match {} (expected {})",
                                    raw.len(),
                                    algo.as_str(),
                                    algo.expected_byte_length(),
                                ),
                            }
                        })?)
                    }
                    None => None,
                }
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

            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

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
    /// Metadata is always read from the DB row (no shard read needed).
    pub fn head_object(&self, req: &GetObjectRequest) -> Result<HeadObjectResult, ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let version_id = req.version_id;
        let cond = req.cond;
        let LockedReadObject { record: stored, .. } =
            self.lock_object_pgs_for_read(bucket, key, version_id)?;

        // If latest version is a delete marker, return 404 with x-amz-delete-marker
        let record = match stored {
            StoredObject::Live(r) => r,
            StoredObject::DeleteMarker(_) => {
                return Err(ServerError::DeleteMarkerHit {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        };

        let etag_str = record.etag.format();
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        // Metadata always from DB row (both multipart and non-multipart).
        let metadata = record
            .metadata_blob
            .as_ref()
            .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
            .transpose()?
            .unwrap_or_default();

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
        req: &GetObjectAttributesRequest,
    ) -> Result<GetObjectAttributesResult, ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let version_id = req.version_id;
        let cond = req.cond;
        let want_parts = req.want_parts;
        let part_number_marker = req.part_number_marker;
        let max_parts = req.max_parts;
        let LockedReadObject {
            record: stored,
            pgs,
        } = self.lock_object_pgs_for_read(bucket, key, version_id)?;

        let record = match stored {
            StoredObject::Live(r) => r,
            StoredObject::DeleteMarker(_) => {
                return Err(ServerError::DeleteMarkerHit {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        };

        let etag_str = record.etag.format();
        check_read_conditions(cond, &etag_str, record.last_modified)?;

        // Metadata always from DB row (both multipart and non-multipart).
        let metadata = record
            .metadata_blob
            .as_ref()
            .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
            .transpose()?
            .unwrap_or_default();

        let object_parts =
            if want_parts && matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
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
                            let checksum = p.checksum.as_ref().map(|bytes| {
                                base64::engine::general_purpose::STANDARD.encode(bytes)
                            });
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
        req: &GetObjectRangeRequest,
    ) -> Result<GetObjectRangeResult, ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let version_id = req.version_id;
        let range = req.range;
        let cond = req.cond;
        let LockedReadObject {
            record: stored,
            pgs,
        } = self.lock_object_pgs_for_read(bucket, key, version_id)?;

        // If latest version is a delete marker, return 404 with x-amz-delete-marker
        let record = match stored {
            StoredObject::Live(r) => r,
            StoredObject::DeleteMarker(_) => {
                return Err(ServerError::DeleteMarkerHit {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                });
            }
        };

        let etag_str = record.etag.format();
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

        let (metadata, user_data) =
            if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
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
                // Non-multipart: metadata from DB row, user data from shards.
                let metadata = record
                    .metadata_blob
                    .as_ref()
                    .map(|b| MetadataBlob::deserialize(b).map(|(m, _)| m))
                    .transpose()?
                    .unwrap_or_default();

                // Check for chunk manifest (stream-put objects).
                let meta_pg = pgs.meta();
                let chunks = meta_pg
                    .get_stream_object_chunks(bucket, key, record.version_id)
                    .map_err(ServerError::Metadata)?;

                let data = if !chunks.is_empty() {
                    drop(pgs);
                    self.read_chunk_manifest_range(
                        bucket,
                        key,
                        &chunks,
                        user_start as usize,
                        user_end as usize,
                    )
                    .map_err(not_found)?
                } else {
                    let okh = object_key_hash(bucket, key);
                    let shard_pg = pgs.shard();
                    let d = self
                        .read_range(
                            shard_pg,
                            &okh,
                            record.version_id,
                            &record,
                            user_start as usize,
                            user_end as usize,
                        )
                        .map_err(not_found)?;
                    drop(pgs);
                    d
                };

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
    pub fn delete_object(&self, req: &DeleteObjectRequest) -> Result<DeleteObjectResult, ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let request_version_id = req.version_id;
        let cond = req.cond;
        let bucket_info = self.head_bucket(bucket)?;

        match (bucket_info.versioning, request_version_id) {
            // Unversioned bucket: physical delete (current behavior)
            (storage::BucketVersioningState::Disabled, _) => {
                let LockedReadObject {
                    record: stored,
                    pgs,
                } = match self.lock_object_pgs_for_read(bucket, key, None) {
                    Ok(locked) => locked,
                    Err(ServerError::ObjectNotFound { .. }) => {
                        if !cond.is_empty() {
                            return Err(ServerError::PreconditionFailed);
                        }
                        return Ok(DeleteObjectResult {
                            version_id: storage::VersionId::Null,
                            delete_marker: false,
                        });
                    }
                    Err(other) => return Err(other),
                };

                // Unversioned bucket objects are always live (no delete markers).
                let record = match stored {
                    StoredObject::Live(r) => r,
                    StoredObject::DeleteMarker(_) => {
                        return Ok(DeleteObjectResult {
                            version_id: storage::VersionId::Null,
                            delete_marker: false,
                        });
                    }
                };

                let meta_pg = pgs.meta();
                let shard_pg = pgs.shard();

                // Check delete conditions
                if !cond.is_empty() {
                    let etag_str = record.etag.format();
                    check_delete_conditions(cond, &etag_str)?;
                }

                if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
                    // Multipart: collect parts, delete metadata under lock,
                    // then delete part shards after releasing the lock.
                    let obj_parts = meta_pg
                        .get_object_parts(bucket, key, record.version_id)
                        .map_err(ServerError::Metadata)?;
                    // Collect chunk manifests for streaming parts before deleting metadata.
                    let mut streaming_chunks: Vec<MultipartPartChunkRecord> = Vec::new();
                    for part in &obj_parts {
                        if part.part_okh == [0u8; 16] {
                            let chunks = meta_pg
                                .get_multipart_part_chunks(
                                    bucket,
                                    key,
                                    record.version_id,
                                    part.part_number,
                                )
                                .map_err(ServerError::Metadata)?;
                            streaming_chunks.extend(chunks);
                        }
                    }
                    if !streaming_chunks.is_empty() {
                        meta_pg
                            .delete_multipart_part_chunks(bucket, key, record.version_id)
                            .map_err(ServerError::Metadata)?;
                    }
                    meta_pg.delete_object_parts(bucket, key, record.version_id)?;
                    meta_pg.delete_object_meta(bucket, key)?;
                    drop(pgs);
                    self.delete_part_shards(&obj_parts)?;
                    if !streaming_chunks.is_empty() {
                        self.delete_chunk_shards_generic(&streaming_chunks)?;
                    }
                } else {
                    let vid = record.version_id;

                    // Check for chunk manifest (stream-put objects).
                    let chunks = meta_pg
                        .get_stream_object_chunks(bucket, key, vid)
                        .map_err(ServerError::Metadata)?;

                    if !chunks.is_empty() {
                        meta_pg
                            .delete_stream_object_chunks(bucket, key, vid)
                            .map_err(ServerError::Metadata)?;
                        meta_pg.delete_object_meta(bucket, key)?;
                        drop(pgs);
                        let _ = self.delete_chunk_shards(&chunks);
                    } else {
                        let okh = object_key_hash(bucket, key);
                        let total = record.ec.k as usize + record.ec.m as usize;

                        for i in 0..total {
                            let shard_key = ShardKey::new(&okh, vid.to_u64(), i as u8);
                            shard_pg.delete_shard(&shard_key)?;
                        }

                        meta_pg.delete_object_meta(bucket, key)?;
                    }
                }

                Ok(DeleteObjectResult {
                    version_id: storage::VersionId::Null,
                    delete_marker: false,
                })
            }

            // Versioned/Suspended + specific versionId: permanent delete that version
            (_, Some(vid)) => {
                let LockedReadObject {
                    record: stored,
                    pgs,
                } = match self.lock_object_pgs_for_read(bucket, key, Some(vid)) {
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
                let is_delete_marker = stored.is_delete_marker();

                // Delete shards if it's a live object (not a delete marker)
                if let StoredObject::Live(record) = &stored {
                    if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
                        let obj_parts = meta_pg
                            .get_object_parts(bucket, key, vid)
                            .map_err(ServerError::Metadata)?;
                        // Collect chunk manifests for streaming parts.
                        let mut streaming_chunks: Vec<MultipartPartChunkRecord> = Vec::new();
                        for part in &obj_parts {
                            if part.part_okh == [0u8; 16] {
                                let chunks = meta_pg
                                    .get_multipart_part_chunks(bucket, key, vid, part.part_number)
                                    .map_err(ServerError::Metadata)?;
                                streaming_chunks.extend(chunks);
                            }
                        }
                        if !streaming_chunks.is_empty() {
                            meta_pg
                                .delete_multipart_part_chunks(bucket, key, vid)
                                .map_err(ServerError::Metadata)?;
                        }
                        meta_pg.delete_object_parts(bucket, key, vid)?;
                        meta_pg.delete_object_version(bucket, key, vid)?;
                        drop(pgs);
                        self.delete_part_shards(&obj_parts)?;
                        if !streaming_chunks.is_empty() {
                            self.delete_chunk_shards_generic(&streaming_chunks)?;
                        }

                        return Ok(DeleteObjectResult {
                            version_id: vid,
                            delete_marker: false,
                        });
                    }

                    // Check for chunk manifest (stream-put objects).
                    let chunks = meta_pg
                        .get_stream_object_chunks(bucket, key, vid)
                        .map_err(ServerError::Metadata)?;

                    if !chunks.is_empty() {
                        meta_pg
                            .delete_stream_object_chunks(bucket, key, vid)
                            .map_err(ServerError::Metadata)?;
                        meta_pg.delete_object_version(bucket, key, vid)?;
                        drop(pgs);
                        let _ = self.delete_chunk_shards(&chunks);

                        return Ok(DeleteObjectResult {
                            version_id: vid,
                            delete_marker: false,
                        });
                    }

                    let okh = object_key_hash(bucket, key);
                    let total = record.ec.k as usize + record.ec.m as usize;

                    for i in 0..total {
                        let shard_key = ShardKey::new(&okh, vid.to_u64(), i as u8);
                        shard_pg.delete_shard(&shard_key)?;
                    }
                }

                meta_pg.delete_object_version(bucket, key, vid)?;

                Ok(DeleteObjectResult {
                    version_id: vid,
                    delete_marker: is_delete_marker,
                })
            }

            // Versioned/Suspended + no versionId: insert delete marker
            (_, None) => {
                let meta_pg_id = self.object_pg_id(bucket, key);
                let meta_pg = self.storage_node.get_pg(meta_pg_id)?;
                let marker_vid = meta_pg.next_version_id(bucket, key)?;
                meta_pg.put_object_meta(&PutObjectReq::DeleteMarker(PutDeleteMarkerReq {
                    bucket: BucketName::from(bucket),
                    key: ObjectKey::from(key),
                    version_id: marker_vid,
                }))?;

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
        req: &ListObjectsV2Request,
    ) -> Result<ListObjectsResult, ServerError> {
        let bucket = req.bucket;
        let prefix = req.prefix;
        let delimiter = req.delimiter;
        let continuation_token = req.continuation_token;
        let max_keys = req.max_keys;
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
        let mut all_objects: Vec<StoredObject> = Vec::new();
        let mut hit_record_cap = false;
        self.pg_topology.for_each_pg(|pg_id| {
            if hit_record_cap {
                return Ok::<(), ServerError>(());
            }
            let pg = self.storage_node.get_pg(pg_id)?;
            let resp = pg.list_objects(&ListObjectsReq {
                bucket: BucketName::from(bucket),
                prefix: prefix.map(ObjectKey::from),
                start_after: continuation_token.map(ObjectKey::from),
                max_keys: per_pg_limit,
            })?;
            all_objects.extend(resp.objects);
            if all_objects.len() >= MAX_LIST_RECORDS {
                all_objects.truncate(MAX_LIST_RECORDS);
                hit_record_cap = true;
            }
            Ok::<(), ServerError>(())
        })?;

        // Sort by key
        all_objects.sort_by(|a, b| a.key().cmp(b.key()));

        // Dedup by key (same key from different PGs shouldn't happen with
        // correct PG derivation, but be safe)
        all_objects.dedup_by(|a, b| a.key() == b.key());

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
                let obj = &all_objects[i];
                let obj_key = obj.key();
                let after_prefix = &obj_key[prefix_str.len()..];
                if let Some(pos) = after_prefix.find(delim) {
                    let cp = format!("{}{}", prefix_str, &after_prefix[..pos + delim.len()]);
                    // Skip all remaining keys under this common prefix so the
                    // continuation token advances past the entire group.
                    let is_new = seen_prefixes.insert(cp.clone());
                    while i < all_objects.len() && all_objects[i].key().starts_with(&cp) {
                        i += 1;
                    }
                    if is_new && token.is_none_or(|t| cp.as_str() > t) {
                        common_prefixes.push(cp.clone());
                        entry_count += 1;
                        last_entry = Some(cp);
                    }
                } else {
                    if token.is_none_or(|t| obj_key.as_str() > t) {
                        let record = obj
                            .as_live()
                            .expect("list_objects returns only live objects");
                        objects.push(ListEntry {
                            key: obj_key.to_string(),
                            size: record.size,
                            etag: record.etag.format(),
                            last_modified: record.last_modified,
                        });
                        entry_count += 1;
                        last_entry = Some(obj_key.to_string());
                    }
                    i += 1;
                }
            }
        } else {
            for obj in &all_objects {
                if entry_count >= max {
                    is_truncated = true;
                    break;
                }
                let obj_key = obj.key();
                if token.is_none_or(|t| obj_key.as_str() > t) {
                    let record = obj
                        .as_live()
                        .expect("list_objects returns only live objects");
                    objects.push(ListEntry {
                        key: obj_key.to_string(),
                        size: record.size,
                        etag: record.etag.format(),
                        last_modified: record.last_modified,
                    });
                    entry_count += 1;
                    last_entry = Some(obj_key.to_string());
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
        req: &ListObjectVersionsRequest,
    ) -> Result<ListObjectVersionsResult, ServerError> {
        let bucket = req.bucket;
        let prefix = req.prefix;
        let key_marker = req.key_marker;
        let version_id_marker = req.version_id_marker;
        let max_keys = req.max_keys;
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
        let mut all_versions: Vec<StoredObject> = Vec::new();
        self.pg_topology.for_each_pg(|pg_id| {
            let pg = self.storage_node.get_pg(pg_id)?;
            let resp = pg.list_object_versions(&ListObjectVersionsReq {
                bucket: BucketName::from(bucket),
                prefix: prefix.map(ObjectKey::from),
                key_marker: key_marker.map(ObjectKey::from),
                version_id_marker,
                max_keys: max_keys.saturating_add(1),
            })?;
            all_versions.extend(resp.versions);
            Ok::<(), ServerError>(())
        })?;

        // Sort by (key ASC, version_id DESC)
        all_versions.sort_by(|a, b| {
            a.key()
                .cmp(b.key())
                .then(b.version_id().to_u64().cmp(&a.version_id().to_u64()))
        });

        // Build result entries, tracking is_latest per key
        let max = max_keys as usize;
        let mut versions: Vec<VersionEntry> = Vec::new();
        let mut last_key: Option<&str> = None;

        for obj in &all_versions {
            if versions.len() >= max {
                break;
            }
            let obj_key = obj.key();
            let is_latest = last_key.is_none_or(|k| k != obj_key.as_str());
            if is_latest {
                last_key = Some(obj_key);
            }

            let (size, etag) = match obj.as_live() {
                Some(record) => (record.size, record.etag.format()),
                None => (0, String::new()),
            };

            versions.push(VersionEntry {
                key: obj_key.to_string(),
                version_id: obj.version_id(),
                is_latest,
                size,
                etag,
                last_modified: obj.last_modified(),
                is_delete_marker: obj.is_delete_marker(),
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
        req: &DeleteObjectsRequest,
    ) -> Result<DeleteObjectsResult, ServerError> {
        let bucket = req.bucket;
        let entries = req.entries;
        let cond = req.cond;
        self.head_bucket(bucket)?;

        let mut deleted = Vec::new();
        let mut errors = Vec::new();

        for entry in entries {
            match self.delete_object(&DeleteObjectRequest { bucket, key: entry.key, version_id: entry.version_id, cond }) {
                Ok(result) => {
                    deleted.push(DeletedObject {
                        key: entry.key.to_string(),
                        version_id: result.version_id,
                        delete_marker: result.delete_marker,
                    });
                }
                Err(e) => {
                    errors.push(DeleteError {
                        key: entry.key.to_string(),
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
        req: &CreateMultipartUploadRequest,
    ) -> Result<CreateMultipartUploadResult, ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let metadata = req.metadata;
        let _bucket_guard = self.storage_node.lock_bucket(bucket);
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
        let meta_pg_id = self.object_pg_id(bucket, key);
        let pg = self.storage_node.get_pg(meta_pg_id)?;
        pg.create_multipart_upload(&CreateMultipartUploadReq {
            upload_id: UploadId::from(upload_id.as_str()),
            bucket: BucketName::from(bucket),
            key: ObjectKey::from(key),
            metadata_blob,
            owner_principal: Some(bucket_info.owner_principal),
            checksum: req.checksum,
        })?;

        Ok(CreateMultipartUploadResult { upload_id })
    }

    /// Upload a part to an in-progress multipart upload.
    ///
    /// Validates part number, resolves the upload, EC-encodes the data,
    /// writes shards, upserts the part record, and best-effort deletes
    /// any prior generation's shards.
    pub fn upload_part(&self, req: &UploadPartRequest) -> Result<UploadPartResult, ServerError> {
        let inner = self.write_part_inner(
            req.bucket,
            req.key,
            req.upload_id,
            req.part_number,
            req.data,
            req.claimed_checksum,
        )?;
        Ok(UploadPartResult {
            etag: inner.etag,
            checksum: inner.checksum,
        })
    }

    /// Copy a byte range from an existing object as a multipart upload part.
    pub fn upload_part_copy(
        &self,
        req: &UploadPartCopyRequest,
    ) -> Result<UploadPartCopyResult, ServerError> {
        let src_bucket = req.source.bucket;
        let src_key = req.source.key;
        let src_version_id = req.source.version_id;
        let dst_bucket = req.dst_bucket;
        let dst_key = req.dst_key;
        let upload_id = req.upload_id;
        let part_number = req.part_number;
        let src_cond = req.source.condition;
        let copy_source_range = req.copy_source_range;
        // Phase 1: Read source object (only the needed range)
        let source_data = {
            let LockedReadObject {
                record: src_stored,
                pgs,
            } = self.lock_object_pgs_for_read(src_bucket, src_key, src_version_id)?;

            // Reject delete markers — they are not copyable objects.
            let src_record = match src_stored {
                StoredObject::Live(r) => r,
                StoredObject::DeleteMarker(_) => {
                    return if src_version_id.is_some() {
                        Err(ServerError::InvalidRequest {
                            reason: "The source of a copy request may not specifically refer to a delete marker by version id.".to_string(),
                        })
                    } else {
                        Err(ServerError::ObjectNotFound {
                            bucket: src_bucket.to_string(),
                            key: src_key.to_string(),
                        })
                    };
                }
            };

            let src_etag = src_record.etag.format();
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
            } else if matches!(src_record.layout, ObjectLayout::MultipartManifest { .. }) {
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
                // Non-multipart source: check for chunk manifest first.
                let meta_pg = pgs.meta();
                let chunks = meta_pg
                    .get_stream_object_chunks(src_bucket, src_key, src_record.version_id)
                    .map_err(ServerError::Metadata)?;

                if !chunks.is_empty() {
                    drop(pgs);
                    self.read_chunk_manifest_range(
                        src_bucket,
                        src_key,
                        &chunks,
                        read_start as usize,
                        read_end as usize,
                    )
                    .map_err(not_found)?
                } else {
                    let src_shard_pg = pgs.shard();
                    let src_okh = object_key_hash(src_bucket, src_key);

                    self.read_range(
                        src_shard_pg,
                        &src_okh,
                        src_record.version_id,
                        &src_record,
                        read_start as usize,
                        read_end as usize,
                    )
                    .map_err(not_found)?
                }
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
        claimed_checksum: Option<&ChecksumClaim>,
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
        let meta_pg_id = self.object_pg_id(bucket, key);

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
            let upload_algo = upload.checksum.map(|c| c.algorithm());

            // Determine next generation for this part number.
            let generation = match meta_pg.get_multipart_part(upload_id, part_number) {
                Ok(existing) => existing.generation + 1,
                Err(storage::MetadataError::PartNotFound { .. }) => 0,
                Err(e) => return Err(ServerError::Metadata(e)),
            };

            let shard_pg_id = self.shard_pg_id(
                &format!("mpu/{upload_id}"),
                &format!("{part_number}/{generation}"),
                storage::VersionId::from_u64(generation as u64),
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
            let upload_algo = upload.checksum.map(|c| c.algorithm());

            let generation = match meta_pg.get_multipart_part(upload_id, part_number) {
                Ok(existing) => existing.generation + 1,
                Err(storage::MetadataError::PartNotFound { .. }) => 0,
                Err(e) => return Err(ServerError::Metadata(e)),
            };

            let verify_shard_pg_id = self.shard_pg_id(
                &format!("mpu/{upload_id}"),
                &format!("{part_number}/{generation}"),
                storage::VersionId::from_u64(generation as u64),
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
        let claimed_algo = claimed_checksum.map(|c| c.algorithm());
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
        if let (Some(claim), Some(ref actual)) = (&claimed_checksum, &checksum_bytes) {
            if claim.expected_bytes() != actual.as_slice() {
                return Err(ServerError::InvalidRequest {
                    reason: "checksum mismatch".to_string(),
                });
            }
        }

        // 4. Compute part identity.
        let part_okh = part_key_hash(upload_id, part_number, generation);
        let part_vid = generation as u64;

        // 5. EC-encode part data (no metadata blob for parts — raw data only).
        let etag_crc = checksum::crc64::checksum(data);

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
            upload_id: UploadId::from(upload_id),
            part_number,
            generation,
            size: data.len() as u64,
            etag: crc64_to_etag_bytes(etag_crc),
            etag_kind: storage::EtagKind::Crc64,
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
            let old_shard_pg_id = self.shard_pg_id(
                &format!("mpu/{upload_id}"),
                &format!("{part_number}/{old_gen}"),
                storage::VersionId::from_u64(old_vid),
            );
            if let Ok(old_pg) = self.storage_node.get_pg(old_shard_pg_id) {
                for i in 0..(k + m) {
                    let old_key = ShardKey::new(&old_okh, old_vid, i as u8);
                    let _ = old_pg.delete_shard(&old_key);
                }
            }
        }

        let checksum = match (effective_algo, checksum_bytes) {
            (Some(algo), Some(bytes)) => {
                Some(storage::RawChecksum::new(algo, bytes).map_err(|_| {
                    ServerError::InternalError {
                        reason: "computed checksum length does not match algorithm".into(),
                    }
                })?)
            }
            _ => None,
        };

        Ok(WritePartInnerResult {
            etag: format_etag(etag_crc),
            checksum,
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
        req: &CompleteMultipartUploadRequest,
    ) -> Result<CompleteMultipartUploadResult, ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let upload_id = req.upload_id;
        let parts = req.parts;
        let claimed_checksum = req.claimed_checksum;
        let _bucket_guard = self.storage_node.lock_bucket(bucket);

        // 1. Validate bucket exists and get versioning state.
        let bucket_info = self.head_bucket(bucket)?;

        // 2. Validate part list: non-empty, within max count, and strictly increasing.
        if parts.is_empty() {
            return Err(ServerError::InvalidRequest {
                reason: "part list must not be empty".to_string(),
            });
        }
        if parts.len() > MAX_PARTS {
            return Err(ServerError::InvalidRequest {
                reason: format!(
                    "part list exceeds maximum of {MAX_PARTS} parts, got {}",
                    parts.len()
                ),
            });
        }
        for window in parts.windows(2) {
            if window[0].part_number >= window[1].part_number {
                return Err(ServerError::InvalidPartOrder);
            }
        }

        // 3. Lock meta PG and validate upload.
        let meta_pg_id = self.object_pg_id(bucket, key);
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
        let checksum_algo = upload.checksum.map(|c| c.algorithm());
        let checksum_type = upload.checksum.map(|c| c.checksum_type());

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
        let version_id = if bucket_info.versioning == storage::BucketVersioningState::Enabled {
            meta_pg.next_version_id(bucket, key)?
        } else {
            storage::VersionId::Null
        };

        // 7. Compute composite multipart ETag.
        let part_etags: Vec<&[u8]> = part_records.iter().map(|p| p.etag.as_slice()).collect();
        let (etag_bytes_vec, etag_str) = compute_multipart_etag(&part_etags);
        let mut etag_crc64 = [0u8; 8];
        etag_crc64.copy_from_slice(&etag_bytes_vec);

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
                                combined = checksum::crc64::combine(combined, part_crc, part.size);
                            }
                            Some(b64.encode(combined.to_be_bytes()))
                        }
                        // SHA algorithms don't support FULL_OBJECT for multipart.
                        // Validated at CreateMultipartUpload time, but guard defensively.
                        ChecksumAlgorithm::Sha1 | ChecksumAlgorithm::Sha256 => {
                            return Err(ServerError::InternalError {
                                reason: format!(
                                    "FULL_OBJECT checksum type is not supported for {}",
                                    algo.as_str()
                                ),
                            });
                        }
                    }
                }
            }
        } else {
            None
        };

        // 8b'. Validate claimed object-level checksum if provided.
        // This uses a raw (algorithm, string) pair rather than ChecksumClaim because
        // composite checksums are formatted as "base64-N", not plain base64.
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
        let obj_req = CommitMultipartReq {
            bucket: BucketName::from(bucket),
            key: ObjectKey::from(key),
            version_id,
            size: total_size,
            etag_crc64,
            ec: EcShape { k: 0, m: 0 }, // per-part, not per-object
            metadata_blob: Some(metadata_blob_bytes),
        };

        let object_parts: Vec<ObjectPartRecord> = part_records
            .iter()
            .map(|p| {
                let shard_pg_id = self.shard_pg_id(
                    &format!("mpu/{}", p.upload_id),
                    &format!("{}/{}", p.part_number, p.generation),
                    storage::VersionId::from_u64(p.part_vid),
                );
                ObjectPartRecord {
                    bucket: BucketName::from(bucket),
                    key: ObjectKey::from(key),
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
            checksum_type,
            checksum_value,
        })
    }

    /// Abort an in-progress multipart upload.
    ///
    /// Transitions to Aborting, best-effort deletes all part shard sets,
    /// then deletes the upload and part metadata rows.
    pub fn abort_multipart_upload(
        &self,
        req: &AbortMultipartUploadRequest,
    ) -> Result<(), ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let upload_id = req.upload_id;
        // 1. Lock meta PG and validate upload.
        let meta_pg_id = self.object_pg_id(bucket, key);
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
                upload_id: UploadId::from(upload_id),
                part_number_marker: None,
                max_parts: u32::MAX,
            })
            .map_err(ServerError::Metadata)?;

        // 3b. Collect streaming chunk manifests for shard cleanup.
        let streaming_chunks = meta_pg
            .get_all_multipart_part_chunks_for_upload(upload_id)
            .map_err(ServerError::Metadata)?;

        // 4. Drop meta PG lock before shard cleanup to avoid deadlocks.
        drop(meta_pg);

        // 5. Best-effort delete all shard sets for each non-streaming part.
        for part in &all_parts.parts {
            if part.part_okh == [0u8; 16] {
                continue; // streaming part — handled below
            }
            let shard_pg_id = self.shard_pg_id(
                &format!("mpu/{upload_id}"),
                &format!("{}/{}", part.part_number, part.generation),
                storage::VersionId::from_u64(part.part_vid),
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

        // 5b. Delete shard data for streaming part chunks first, then
        //     delete the manifest rows. This order ensures that if shard
        //     deletion fails, the chunk refs survive for retry.
        if !streaming_chunks.is_empty() {
            self.delete_chunk_shards_generic(&streaming_chunks)?;
        }

        // 6. Re-acquire meta PG and delete upload + parts (CASCADE)
        //    and chunk manifest rows.
        let meta_pg = self.storage_node.get_pg(meta_pg_id)?;
        if !streaming_chunks.is_empty() {
            meta_pg
                .delete_multipart_part_chunks_by_upload_id(upload_id)
                .map_err(ServerError::Metadata)?;
        }
        meta_pg
            .delete_multipart_upload(upload_id)
            .map_err(ServerError::Metadata)?;

        Ok(())
    }

    /// List parts of an in-progress multipart upload.
    pub fn list_parts(&self, req: &ListPartsRequest) -> Result<ListPartsResult, ServerError> {
        let bucket = req.bucket;
        let key = req.key;
        let upload_id = req.upload_id;
        let part_number_marker = req.part_number_marker;
        let max_parts = req.max_parts;
        // 1. Lock meta PG and validate upload.
        let meta_pg_id = self.object_pg_id(bucket, key);
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
                upload_id: UploadId::from(upload_id),
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
            checksum_algorithm: upload.checksum.map(|c| c.algorithm()),
            checksum_type: upload.checksum.map(|c| c.checksum_type()),
        })
    }

    /// List in-progress multipart uploads for a bucket.
    ///
    /// Fans out across all PGs, merges results sorted by (key, upload_id),
    /// and applies pagination.
    pub fn list_multipart_uploads(
        &self,
        req: &ListMultipartUploadsRequest,
    ) -> Result<ListMultipartUploadsResult, ServerError> {
        let bucket = req.bucket;
        let prefix = req.prefix;
        let key_marker = req.key_marker;
        let upload_id_marker = req.upload_id_marker;
        let max_uploads = req.max_uploads;
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
        self.pg_topology.for_each_pg(|pg_id| {
            if hit_record_cap {
                return Ok::<(), ServerError>(());
            }
            let pg = self.storage_node.get_pg(pg_id)?;
            let resp = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                bucket: BucketName::from(bucket),
                prefix: prefix.map(ObjectKey::from),
                key_marker: key_marker.map(ObjectKey::from),
                upload_id_marker: upload_id_marker.map(UploadId::from),
                max_uploads: max_uploads.saturating_add(1),
            })?;
            all_uploads.extend(resp.uploads);
            if all_uploads.len() >= MAX_LIST_RECORDS {
                all_uploads.truncate(MAX_LIST_RECORDS);
                hit_record_cap = true;
            }
            Ok::<(), ServerError>(())
        })?;

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
                (Some(last.key.to_string()), Some(last.upload_id.to_string()))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        let uploads = all_uploads
            .into_iter()
            .map(|u| MultipartUploadEntry {
                key: u.key.to_string(),
                upload_id: u.upload_id.to_string(),
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
        ChecksumAlgorithm::Crc64nvme => checksum::crc64::checksum(data).to_be_bytes().to_vec(),
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
    use crate::conditional::{DeleteCondition, ReadCondition, SpecificEtag, WriteCondition};
    use std::path::{Path, PathBuf};
    use std::sync::mpsc;
    use std::sync::Barrier;
    use std::thread;
    use std::time::Duration;

    const NO_READ: &ReadCondition = &ReadCondition {
        if_match: None,
        if_none_match: None,
        if_modified_since: None,
        if_unmodified_since: None,
    };
    const NO_WRITE: &WriteCondition = &WriteCondition::None;
    const NO_DELETE: &DeleteCondition = &DeleteCondition::None;

    fn setup_coordinator(dir: &Path) -> Coordinator {
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();
        Coordinator::new(storage_node, ec_config, "us-east-1".to_string()).unwrap()
    }

    #[test]
    fn bucket_crud() {
        let tmp = test_util::tempdir();
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
        let tmp = test_util::tempdir();
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
        let tmp = test_util::tempdir();
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
        let tmp = test_util::tempdir();
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
    fn list_buckets_globally_sorted() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("zz-top").unwrap();
        coord.create_bucket("alpha").unwrap();
        coord.create_bucket("mango").unwrap();
        coord.create_bucket("beta").unwrap();

        let names: Vec<String> = coord
            .list_buckets()
            .unwrap()
            .into_iter()
            .map(|b| b.name.to_string())
            .collect();
        assert_eq!(names, vec!["alpha", "beta", "mango", "zz-top"]);
    }

    #[test]
    fn list_buckets_with_sparse_pg_topology() {
        let tmp = test_util::tempdir();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &[0, 2, 5]).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();
        let coord = Coordinator::new(storage_node, ec_config, "us-east-1".to_string()).unwrap();
        let bucket = "bucket-sparse";
        coord.create_bucket(bucket).unwrap();

        let names: Vec<String> = coord
            .list_buckets()
            .unwrap()
            .into_iter()
            .map(|b| b.name.to_string())
            .collect();
        assert_eq!(names, vec![bucket]);
    }

    #[test]
    fn list_objects_with_sparse_pg_topology() {
        let tmp = test_util::tempdir();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &[0, 2, 5]).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();
        let coord = Coordinator::new(storage_node, ec_config, "us-east-1".to_string()).unwrap();
        let bucket = "bucket-sparse";
        coord.create_bucket(bucket).unwrap();

        let resp = coord
            .list_objects_v2(&ListObjectsV2Request { bucket, prefix: None, delimiter: None, continuation_token: None, max_keys: 1000 })
            .unwrap();
        assert!(resp.objects.is_empty());
    }

    #[test]
    fn list_object_versions_with_sparse_pg_topology() {
        let tmp = test_util::tempdir();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &[0, 2, 5]).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();
        let coord = Coordinator::new(storage_node, ec_config, "us-east-1".to_string()).unwrap();
        let bucket = "bucket-sparse";
        coord.create_bucket(bucket).unwrap();

        let resp = coord
            .list_object_versions(&ListObjectVersionsRequest { bucket, prefix: None, key_marker: None, version_id_marker: None, max_keys: 1000 })
            .unwrap();
        assert!(resp.versions.is_empty());
    }

    #[test]
    fn list_multipart_uploads_with_sparse_pg_topology() {
        let tmp = test_util::tempdir();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &[0, 2, 5]).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();
        let coord = Coordinator::new(storage_node, ec_config, "us-east-1".to_string()).unwrap();
        let bucket = "bucket-sparse";
        let key = "key-sparse";
        coord.create_bucket(bucket).unwrap();
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket, key, metadata: &MetadataBlob::new(), checksum: None })
            .unwrap();

        let resp = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest { bucket, prefix: None, key_marker: None, upload_id_marker: None, max_uploads: 1000 })
            .unwrap();
        assert_eq!(resp.uploads.len(), 1);
        assert_eq!(resp.uploads[0].key, key);
    }

    #[test]
    fn delete_nonempty_bucket_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let err = coord.delete_bucket("bucket").unwrap_err();
        assert!(matches!(err, ServerError::BucketNotEmpty));
    }

    #[test]
    fn put_object_waits_for_bucket_lock() {
        let tmp = test_util::tempdir();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();

        let admin = Coordinator::new(
            Arc::clone(&storage_node),
            ec_config,
            "us-east-1".to_string(),
        )
        .unwrap();
        let writer = Coordinator::new(
            Arc::clone(&storage_node),
            ec_config,
            "us-east-1".to_string(),
        )
        .unwrap();
        admin.create_bucket("bucket").unwrap();

        let guard = storage_node.lock_bucket("bucket");
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let res = writer.put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE });
            tx.send(res).unwrap();
        });

        // Writer should block while bucket lock is held.
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(guard);

        let res = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            res.is_ok(),
            "put_object should succeed after lock release: {res:?}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn delete_bucket_waits_for_bucket_lock() {
        let tmp = test_util::tempdir();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();

        let admin = Coordinator::new(
            Arc::clone(&storage_node),
            ec_config,
            "us-east-1".to_string(),
        )
        .unwrap();
        let deleter = Coordinator::new(
            Arc::clone(&storage_node),
            ec_config,
            "us-east-1".to_string(),
        )
        .unwrap();
        admin.create_bucket("bucket").unwrap();

        let guard = storage_node.lock_bucket("bucket");
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let res = deleter.delete_bucket("bucket");
            tx.send(res).unwrap();
        });

        // Delete should block while bucket lock is held.
        assert!(rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(guard);

        let res = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            res.is_ok(),
            "delete_bucket should succeed after lock release: {res:?}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn put_get_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();

        let headers = [("Content-Type", "text/plain")];
        let result = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "hello.txt", data: b"Hello, world!", metadata: &MetadataBlob::from_headers(&headers).unwrap(), cond: NO_WRITE })
            .unwrap();
        assert!(!result.etag.is_empty());

        let obj = coord
            .get_object(&GetObjectRequest { bucket: "bucket", key: "hello.txt", version_id: None, cond: NO_READ })
            .unwrap();
        assert_eq!(obj.data, b"Hello, world!");
        assert_eq!(obj.size, 13);
        assert_eq!(obj.metadata.get("content-type"), Some("text/plain"));
    }

    #[test]
    fn put_get_with_metadata() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();

        let headers = [
            ("Content-Type", "application/json"),
            ("X-Amz-Meta-Author", "alice"),
            ("X-Amz-Meta-Version", "42"),
        ];
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "obj", data: b"{}", metadata: &MetadataBlob::from_headers(&headers).unwrap(), cond: NO_WRITE })
            .unwrap();

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "obj", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, b"{}");
        assert_eq!(obj.metadata.get("content-type"), Some("application/json"));
        assert_eq!(obj.metadata.get("x-amz-meta-author"), Some("alice"));
        assert_eq!(obj.metadata.get("x-amz-meta-version"), Some("42"));
    }

    #[test]
    fn head_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::from_headers(&[("Content-Type", "text/plain")]).unwrap(), cond: NO_WRITE })
            .unwrap();

        let head = coord.head_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(head.size, 4);
        assert_eq!(head.metadata.get("content-type"), Some("text/plain"));
    }

    #[test]
    fn overwrite_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"v1", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"v2", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, b"v2");
    }

    #[test]
    fn empty_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "empty", data: b"", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "empty", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, b"");
        assert_eq!(obj.size, 0);
    }

    #[test]
    fn delete_object_then_get_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .delete_object(&DeleteObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_DELETE })
            .unwrap();

        let err = coord
            .get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn delete_nonexistent_object_is_ok() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        // Should not error
        coord
            .delete_object(&DeleteObjectRequest { bucket: "bucket", key: "no-such-key", version_id: None, cond: NO_DELETE })
            .unwrap();
    }

    #[test]
    fn list_objects() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "a/1", data: b"1", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "a/2", data: b"2", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "b/1", data: b"3", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: None, continuation_token: None, max_keys: 1000 })
            .unwrap();
        assert_eq!(result.objects.len(), 3);
        // Should be sorted
        assert_eq!(result.objects[0].key, "a/1");
        assert_eq!(result.objects[1].key, "a/2");
        assert_eq!(result.objects[2].key, "b/1");
    }

    #[test]
    fn list_objects_with_prefix() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "photos/cat.jpg", data: b"cat", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "photos/dog.jpg", data: b"dog", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "docs/readme.md", data: b"md", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: Some("photos/"), delimiter: None, continuation_token: None, max_keys: 1000 })
            .unwrap();
        assert_eq!(result.objects.len(), 2);
    }

    #[test]
    fn list_objects_with_delimiter() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "photos/cat.jpg", data: b"cat", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "photos/dog.jpg", data: b"dog", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "docs/readme.md", data: b"md", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "root.txt", data: b"root", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: Some("/"), continuation_token: None, max_keys: 1000 })
            .unwrap();
        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].key, "root.txt");
        assert!(result.common_prefixes.contains(&"photos/".to_string()));
        assert!(result.common_prefixes.contains(&"docs/".to_string()));
    }

    #[test]
    fn put_get_object_trailing_slash_key() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "folder/", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest { bucket: "bucket", key: "folder/", version_id: None, cond: NO_READ })
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"This data should survive shard loss!";
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "resilient", data, metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // Delete one data shard using the helper
        delete_shard_on_disk(tmp.path(), "bucket", "resilient", 0, 4);

        // Get should still succeed via EC reconstruction
        let obj = coord
            .get_object(&GetObjectRequest { bucket: "bucket", key: "resilient", version_id: None, cond: NO_READ })
            .unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn ec_drop_one_data_shard_get() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC single shard loss test data";
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "obj1", data, metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        delete_shard_on_disk(tmp.path(), "bucket", "obj1", 0, 4);

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "obj1", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn ec_drop_m_shards_at_limit() {
        // Config: k=4, m=2. Dropping exactly m=2 shards should still recover.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC m-shard loss limit test data";
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "obj2", data, metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // Delete 2 data shards (indices 0 and 1)
        delete_shard_on_disk(tmp.path(), "bucket", "obj2", 0, 4);
        delete_shard_on_disk(tmp.path(), "bucket", "obj2", 1, 4);

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "obj2", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn ec_drop_m_plus_one_shards_fails() {
        // Config: k=4, m=2. Dropping m+1=3 shards should fail.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC m+1 shard loss test data";
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "obj3", data, metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // Delete 3 shards (indices 0, 1, 2)
        delete_shard_on_disk(tmp.path(), "bucket", "obj3", 0, 4);
        delete_shard_on_disk(tmp.path(), "bucket", "obj3", 1, 4);
        delete_shard_on_disk(tmp.path(), "bucket", "obj3", 2, 4);

        let err = coord
            .get_object(&GetObjectRequest { bucket: "bucket", key: "obj3", version_id: None, cond: NO_READ })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn ec_corrupt_one_data_shard_recovery() {
        // Corrupt shard 0 on disk. PgStore detects CRC mismatch, EC reconstructs.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC corruption recovery test data";
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "obj4", data, metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        corrupt_shard_on_disk(tmp.path(), "bucket", "obj4", 0, 4);

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "obj4", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn ec_range_get_with_missing_shard() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"Hello, World! Range test with EC recovery";
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "obj5", data, metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // Delete shard 0 (covers the beginning of the data)
        delete_shard_on_disk(tmp.path(), "bucket", "obj5", 0, 4);

        // Range get should still succeed via EC reconstruction
        let result = coord
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "obj5", version_id: None, range: ByteRange::Range { start: 0, end: 4 }, cond: NO_READ })
            .unwrap();
        assert_eq!(result.data, b"Hello");
    }

    #[test]
    fn ec_drop_parity_shard_data_still_works() {
        // Delete parity shard (index k=4). Only data shards needed for normal read.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC parity shard drop test";
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "obj6", data, metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // Delete first parity shard (index 4, since k=4)
        delete_shard_on_disk(tmp.path(), "bucket", "obj6", 4, 4);

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "obj6", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, data);
    }

    #[test]
    fn put_to_nonexistent_bucket_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .put_object(&PutObjectRequest { bucket: "no-such-bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn get_nonexistent_object_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let err = coord
            .get_object(&GetObjectRequest { bucket: "bucket", key: "no-such-key", version_id: None, cond: NO_READ })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn etag_consistency() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let result = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(result.etag, obj.etag);

        let head = coord.head_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(result.etag, head.etag);
    }

    #[test]
    fn list_objects_delimiter_with_continuation() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "a/1", data: b"1", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "a/2", data: b"2", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "b/1", data: b"3", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "c/1", data: b"4", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "root.txt", data: b"5", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // First page: max_keys=2 with delimiter
        let result = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: Some("/"), continuation_token: None, max_keys: 2 })
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
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: Some("/"), continuation_token: Some(&token), max_keys: 2 })
            .unwrap();
        assert!(
            !result2.objects.is_empty() || !result2.common_prefixes.is_empty(),
            "continuation page should have entries"
        );
    }

    #[test]
    fn list_objects_max_keys_counts_prefixes() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        // Create many prefixed objects to ensure common_prefixes count toward max_keys
        for i in 0..10 {
            let key = format!("dir{}/file.txt", i);
            coord
                .put_object(&PutObjectRequest { bucket: "bucket", key: &key, data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
                .unwrap();
        }

        let result = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: Some("/"), continuation_token: None, max_keys: 3 })
            .unwrap();
        // With delimiter "/", all entries become common prefixes
        assert_eq!(result.common_prefixes.len(), 3);
        assert!(result.is_truncated);
    }

    #[test]
    fn put_object_to_nonexistent_bucket_no_orphaned_shards() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        // Don't create bucket — put should fail at bucket check before writing shards
        let err = coord
            .put_object(&PutObjectRequest { bucket: "no-bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn delete_nonexistent_bucket_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = coord.delete_bucket("no-such-bucket").unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn list_objects_no_delimiter_truncated() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        for i in 0..5 {
            let key = format!("key-{:02}", i);
            coord
                .put_object(&PutObjectRequest { bucket: "bucket", key: &key, data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
                .unwrap();
        }

        // Request fewer than available
        let result = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: None, continuation_token: None, max_keys: 3 })
            .unwrap();
        assert_eq!(result.objects.len(), 3);
        assert!(result.is_truncated);
        assert!(result.next_continuation_token.is_some());
    }

    #[test]
    fn list_objects_no_delimiter_with_continuation() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        for i in 0..5 {
            let key = format!("key-{:02}", i);
            coord
                .put_object(&PutObjectRequest { bucket: "bucket", key: &key, data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
                .unwrap();
        }

        // First page
        let page1 = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: None, continuation_token: None, max_keys: 2 })
            .unwrap();
        assert_eq!(page1.objects.len(), 2);
        assert!(page1.is_truncated);
        let token = page1.next_continuation_token.as_ref().unwrap();

        // Second page using continuation token
        let page2 = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: None, continuation_token: Some(token), max_keys: 2 })
            .unwrap();
        assert_eq!(page2.objects.len(), 2);
        assert!(page2.is_truncated);
        let token2 = page2.next_continuation_token.as_ref().unwrap();

        // Third page — should get remainder
        let page3 = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: None, continuation_token: Some(token2), max_keys: 2 })
            .unwrap();
        assert_eq!(page3.objects.len(), 1);
        assert!(!page3.is_truncated);
        assert!(page3.next_continuation_token.is_none());
    }

    #[test]
    fn list_objects_prefix_with_delimiter() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "photos/2024/jan.jpg", data: b"j", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "photos/2024/feb.jpg", data: b"f", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "photos/2025/mar.jpg", data: b"m", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "photos/top.jpg", data: b"t", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // List with prefix "photos/" and delimiter "/"
        let result = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: Some("photos/"), delimiter: Some("/"), continuation_token: None, max_keys: 1000 })
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "only-one", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: None, continuation_token: None, max_keys: 1000 })
            .unwrap();
        assert_eq!(result.objects.len(), 1);
        assert!(!result.is_truncated);
        assert!(result.next_continuation_token.is_none());
    }

    #[test]
    fn list_objects_max_keys_zero() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key1", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: None, continuation_token: None, max_keys: 0 })
            .unwrap();
        assert!(result.objects.is_empty());
        assert!(result.common_prefixes.is_empty());
        assert!(!result.is_truncated);
        assert!(result.next_continuation_token.is_none());
    }

    #[test]
    fn list_objects_max_keys_zero_with_delimiter() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "a/1", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: Some("/"), continuation_token: None, max_keys: 0 })
            .unwrap();
        assert!(result.objects.is_empty());
        assert!(result.common_prefixes.is_empty());
        assert!(!result.is_truncated);
    }

    #[test]
    fn list_objects_nonexistent_bucket_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "no-bucket", prefix: None, delimiter: None, continuation_token: None, max_keys: 1000 })
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn delete_objects_batch() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key1", data: b"data1", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key2", data: b"data2", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let entries = vec![
            DeleteEntry {
                key: "key1",
                version_id: None,
            },
            DeleteEntry {
                key: "key2",
                version_id: None,
            },
            // key3 doesn't exist — should still succeed (idempotent)
            DeleteEntry {
                key: "key3",
                version_id: None,
            },
        ];

        let result = coord.delete_objects(&DeleteObjectsRequest { bucket: "bucket", entries: &entries, cond: NO_DELETE }).unwrap();
        assert_eq!(result.deleted.len(), 3);
        assert!(result.errors.is_empty());

        // Verify objects are actually gone
        assert!(coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key1", version_id: None, cond: NO_READ }).is_err());
        assert!(coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key2", version_id: None, cond: NO_READ }).is_err());
    }

    #[test]
    fn delete_objects_nonexistent_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let entries = vec![DeleteEntry {
            key: "key1",
            version_id: None,
        }];

        let err = coord
            .delete_objects(&DeleteObjectsRequest { bucket: "no-bucket", entries: &entries, cond: NO_DELETE })
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("test-bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "test-bucket", key: "dir/file1.txt", data: b"hello", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "test-bucket", key: "dir/file2.txt", data: b"world", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "test-bucket", key: "root.txt", data: b"root", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // Step 1: ListObjectVersions
        let versions_result = coord
            .list_object_versions(&ListObjectVersionsRequest { bucket: "test-bucket", prefix: None, key_marker: None, version_id_marker: None, max_keys: 1000 })
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
            .list_objects_v2(&ListObjectsV2Request { bucket: "test-bucket", prefix: None, delimiter: None, continuation_token: None, max_keys: 1000 })
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
        let (xml_entries, quiet) =
            crate::http::xml::parse_delete_objects_xml(delete_xml.as_bytes()).unwrap();
        assert_eq!(xml_entries.len(), 3);
        assert!(!quiet);
        let entries: Vec<DeleteEntry> = xml_entries.iter().map(|e| DeleteEntry { key: &e.key, version_id: None }).collect();

        // Step 5: Batch delete
        let delete_result = coord
            .delete_objects(&DeleteObjectsRequest { bucket: "test-bucket", entries: &entries, cond: NO_DELETE })
            .unwrap();
        assert_eq!(delete_result.deleted.len(), 3);
        assert!(delete_result.errors.is_empty());

        // Step 6: Bucket should now be empty and deletable
        let list_after = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "test-bucket", prefix: None, delimiter: None, continuation_token: None, max_keys: 1000 })
            .unwrap();
        assert!(list_after.objects.is_empty());
        coord.delete_bucket("test-bucket").unwrap();
    }

    /// Same workflow but with paginated listing and quiet-mode delete.
    #[test]
    fn ceph_cleanup_workflow_paginated() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        for i in 0..5 {
            let key = format!("key-{:02}", i);
            coord
                .put_object(&PutObjectRequest { bucket: "bucket", key: &key, data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
                .unwrap();
        }

        // Page 1: max_keys=2
        let page1 = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: None, continuation_token: None, max_keys: 2 })
            .unwrap();
        assert_eq!(page1.objects.len(), 2);
        assert!(page1.is_truncated);
        let token = page1.next_continuation_token.clone().unwrap();

        // Page 2
        let page2 = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: None, continuation_token: Some(&token), max_keys: 2 })
            .unwrap();
        assert_eq!(page2.objects.len(), 2);
        let token2 = page2.next_continuation_token.clone().unwrap();

        // Page 3
        let page3 = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: None, continuation_token: Some(&token2), max_keys: 2 })
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

        let (xml_entries, quiet) =
            crate::http::xml::parse_delete_objects_xml(delete_xml.as_bytes()).unwrap();
        assert_eq!(xml_entries.len(), 5);
        assert!(quiet);
        let entries: Vec<DeleteEntry> = xml_entries.iter().map(|e| DeleteEntry { key: &e.key, version_id: None }).collect();

        let delete_result = coord.delete_objects(&DeleteObjectsRequest { bucket: "bucket", entries: &entries, cond: NO_DELETE }).unwrap();
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
        // Verify the constant matches AWS S3 single PUT limit (5 GiB).
        assert_eq!(MAX_OBJECT_SIZE, 5 * 1024 * 1024 * 1024);
    }

    #[test]
    fn max_parts_constant() {
        assert_eq!(MAX_PARTS, 10_000);
    }

    #[test]
    fn complete_multipart_too_many_parts() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &MetadataBlob::new(), checksum: None })
            .unwrap();

        // Build a part list with MAX_PARTS + 1 entries.
        let parts: Vec<_> = (1..=MAX_PARTS as u32 + 1)
            .map(|n| CompletePart {
                part_number: n,
                etag: "dummy".to_string(),
                checksum: None,
            })
            .collect();

        let err = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, parts: &parts, claimed_checksum: None })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"Hello, World!", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // bytes=0-4 → "Hello"
        let result = coord
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "key", version_id: None, range: ByteRange::Range { start: 0, end: 4 }, cond: NO_READ })
            .unwrap();
        assert_eq!(result.data, b"Hello");
        assert_eq!(result.range_start, 0);
        assert_eq!(result.range_end, 4);
        assert_eq!(result.size, 13);
    }

    #[test]
    fn get_object_range_suffix() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"Hello, World!", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // bytes=-6 → "World!"  (last 6 bytes)
        let result = coord
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "key", version_id: None, range: ByteRange::Suffix { length: 6 }, cond: NO_READ })
            .unwrap();
        assert_eq!(result.data, b"World!");
        assert_eq!(result.range_start, 7);
        assert_eq!(result.range_end, 12);
    }

    #[test]
    fn get_object_range_from_start() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"Hello, World!", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // bytes=7- → "World!"
        let result = coord
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "key", version_id: None, range: ByteRange::FromStart { start: 7 }, cond: NO_READ })
            .unwrap();
        assert_eq!(result.data, b"World!");
    }

    #[test]
    fn get_object_range_unsatisfiable() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"Hello", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // bytes=100- → unsatisfiable
        let err = coord
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "key", version_id: None, range: ByteRange::FromStart { start: 100 }, cond: NO_READ })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRange { total_size: 5 }));
    }

    #[test]
    fn get_object_range_clamps_end() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"Hello", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // bytes=0-99999 on 5-byte object → clamp to 0-4
        let result = coord
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "key", version_id: None, range: ByteRange::Range {
                    start: 0,
                    end: 99999,
                }, cond: NO_READ })
            .unwrap();
        assert_eq!(result.data, b"Hello");
        assert_eq!(result.range_start, 0);
        assert_eq!(result.range_end, 4);
    }

    // ── Conditional request integration tests ────────────────────────

    #[test]
    fn put_if_none_match_star_creates() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let cond = WriteCondition::IfNoneMatchStar;
        let result = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "new-key", data: b"data", metadata: &MetadataBlob::new(), cond: &cond })
            .unwrap();
        assert!(!result.etag.is_empty());
    }

    #[test]
    fn put_if_none_match_star_rejects_overwrite() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"v1", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let cond = WriteCondition::IfNoneMatchStar;
        let err = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"v2", metadata: &MetadataBlob::new(), cond: &cond })
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn put_if_match_updates() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let r1 = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"v1", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        let cond = WriteCondition::IfMatch(SpecificEtag::new(r1.etag.clone()).unwrap());
        let r2 = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"v2", metadata: &MetadataBlob::new(), cond: &cond })
            .unwrap();
        assert_ne!(r1.etag, r2.etag);

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, b"v2");
    }

    #[test]
    fn put_if_match_stale_etag_rejected() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let r1 = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"v1", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        // Overwrite so etag changes
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"v2", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let cond = WriteCondition::IfMatch(SpecificEtag::new(r1.etag).unwrap());
        let err = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"v3", metadata: &MetadataBlob::new(), cond: &cond })
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn get_if_match_returns_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        let cond = ReadCondition {
            if_match: Some(put.etag),
            ..Default::default()
        };
        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &cond }).unwrap();
        assert_eq!(obj.data, b"data");
    }

    #[test]
    fn get_if_match_wrong_etag_returns_412() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let cond = ReadCondition {
            if_match: Some("\"0000000000000000\"".to_string()),
            ..Default::default()
        };
        let err = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &cond }).unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn get_if_none_match_returns_304() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        let cond = ReadCondition {
            if_none_match: Some(put.etag),
            ..Default::default()
        };
        let err = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &cond }).unwrap_err();
        assert!(matches!(err, ServerError::NotModified { .. }));
    }

    #[test]
    fn head_if_none_match_returns_304() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        let cond = ReadCondition {
            if_none_match: Some(put.etag),
            ..Default::default()
        };
        let err = coord.head_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &cond }).unwrap_err();
        assert!(matches!(err, ServerError::NotModified { .. }));
    }

    #[test]
    fn delete_if_match_succeeds() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        let cond = DeleteCondition::IfMatch(put.etag);
        coord.delete_object(&DeleteObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &cond }).unwrap();
        assert!(coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).is_err());
    }

    #[test]
    fn delete_if_match_wrong_etag_returns_412() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let cond = DeleteCondition::IfMatch("\"0000000000000000\"".to_string());
        let err = coord
            .delete_object(&DeleteObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &cond })
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn delete_objects_if_match_per_entry() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let p1 = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key1", data: b"data1", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key2", data: b"data2", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // Use key1's etag for both entries; key2 will fail the condition
        let cond = DeleteCondition::IfMatch(p1.etag);
        let entries = vec![
            DeleteEntry {
                key: "key1",
                version_id: None,
            },
            DeleteEntry {
                key: "key2",
                version_id: None,
            },
        ];
        let result = coord.delete_objects(&DeleteObjectsRequest { bucket: "bucket", entries: &entries, cond: &cond }).unwrap();
        assert_eq!(result.deleted.len(), 1);
        assert_eq!(result.deleted[0].key, "key1");
        assert_eq!(result.errors.len(), 1);
        assert_eq!(result.errors[0].key, "key2");
    }

    #[test]
    fn range_get_if_match_returns_data() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"Hello, World!", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        let cond = ReadCondition {
            if_match: Some(put.etag),
            ..Default::default()
        };
        let result = coord
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "key", version_id: None, range: ByteRange::Range { start: 0, end: 4 }, cond: &cond })
            .unwrap();
        assert_eq!(result.data, b"Hello");
    }

    // ── CopyObject tests ──────────────────────────────────────────────

    #[test]
    fn copy_object_basic() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let headers = [("Content-Type", "text/plain")];
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "src", data: b"hello copy", metadata: &MetadataBlob::from_headers(&headers).unwrap(), cond: NO_WRITE })
            .unwrap();

        let result = coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "bucket",
                    key: "src",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "bucket",
                dst_key: "dst",
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Copy,
            })
            .unwrap();
        assert!(!result.etag.is_empty());

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "dst", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, b"hello copy");
    }

    #[test]
    fn copy_object_metadata_copy_directive() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let headers = [
            ("Content-Type", "image/png"),
            ("X-Amz-Meta-Author", "alice"),
        ];
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "src", data: b"data", metadata: &MetadataBlob::from_headers(&headers).unwrap(), cond: NO_WRITE })
            .unwrap();

        coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "bucket",
                    key: "src",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "bucket",
                dst_key: "dst",
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Copy,
            })
            .unwrap();

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "dst", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.metadata.get("content-type"), Some("image/png"));
        assert_eq!(obj.metadata.get("x-amz-meta-author"), Some("alice"));
    }

    #[test]
    fn copy_object_metadata_replace_directive() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let headers = [
            ("Content-Type", "image/png"),
            ("X-Amz-Meta-Author", "alice"),
        ];
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "src", data: b"data", metadata: &MetadataBlob::from_headers(&headers).unwrap(), cond: NO_WRITE })
            .unwrap();

        let new_headers = [("Content-Type", "text/html"), ("X-Amz-Meta-Version", "2")];
        let new_metadata = MetadataBlob::from_headers(&new_headers).unwrap();
        coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "bucket",
                    key: "src",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "bucket",
                dst_key: "dst",
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Replace { metadata: &new_metadata, checksum_algorithm: None },
            })
            .unwrap();

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "dst", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, b"data");
        assert_eq!(obj.metadata.get("content-type"), Some("text/html"));
        assert_eq!(obj.metadata.get("x-amz-meta-version"), Some("2"));
        // Old metadata should be gone
        assert_eq!(obj.metadata.get("x-amz-meta-author"), None);
    }

    #[test]
    fn copy_object_same_key_replace_metadata() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let headers = [("Content-Type", "text/plain")];
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::from_headers(&headers).unwrap(), cond: NO_WRITE })
            .unwrap();

        let new_metadata = MetadataBlob::from_headers(&[("Content-Type", "application/json")]).unwrap();
        coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "bucket",
                    key: "key",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "bucket",
                dst_key: "key",
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Replace { metadata: &new_metadata, checksum_algorithm: None },
            })
            .unwrap();

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, b"data");
        assert_eq!(obj.metadata.get("content-type"), Some("application/json"));
    }

    #[test]
    fn copy_object_replace_strips_unverified_inline_checksum() {
        // Regression: CopyObject with REPLACE must not persist client-supplied
        // checksum value headers, since there is no body to verify them against.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "src", data: b"hello", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        // Metadata blob with only content-type (checksum value headers should
        // be stripped at the HTTP boundary before reaching the coordinator).
        let new_metadata = MetadataBlob::from_headers(&[("Content-Type", "text/plain")]).unwrap();
        coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "bucket",
                    key: "src",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "bucket",
                dst_key: "dst",
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Replace { metadata: &new_metadata, checksum_algorithm: None },
            })
            .unwrap();

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "dst", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, b"hello");
        // No checksum should be present since none was requested.
        assert_eq!(obj.metadata.get("x-amz-checksum-crc32c"), None);
    }

    #[test]
    fn copy_object_replace_recomputes_checksum_from_algorithm() {
        // When x-amz-checksum-algorithm is specified on CopyObject REPLACE,
        // the checksum should be computed from the copied data.
        use base64::Engine;
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let data = b"hello";
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "src", data, metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let new_metadata = MetadataBlob::from_headers(&[("Content-Type", "text/plain")]).unwrap();
        coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "bucket",
                    key: "src",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "bucket",
                dst_key: "dst",
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Replace {
                    metadata: &new_metadata,
                    checksum_algorithm: Some(ChecksumAlgorithm::Crc32c),
                },
            })
            .unwrap();

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "dst", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, data);
        // Checksum should be the real CRC32C of "hello", not missing.
        let expected_crc = checksum::crc32c::checksum(data);
        let expected_b64 =
            base64::engine::general_purpose::STANDARD.encode(expected_crc.to_be_bytes());
        assert_eq!(
            obj.metadata.get("x-amz-checksum-crc32c"),
            Some(expected_b64.as_str())
        );
    }

    #[test]
    fn checksum_algorithm_parse_rejects_bogus() {
        // Invalid checksum algorithm strings are rejected at the parse boundary
        // (HTTP layer), so they can never reach the coordinator as typed values.
        assert!(ChecksumAlgorithm::parse("BOGUS").is_none());
        assert!(ChecksumAlgorithm::parse("").is_none());
        // Valid ones are accepted.
        assert_eq!(ChecksumAlgorithm::parse("CRC32C"), Some(ChecksumAlgorithm::Crc32c));
    }

    #[test]
    fn copy_object_source_not_found() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let err = coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "bucket",
                    key: "no-such-key",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "bucket",
                dst_key: "dst",
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Copy,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn copy_object_dest_bucket_not_found() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "src", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let err = coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "bucket",
                    key: "src",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "no-bucket",
                dst_key: "dst",
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Copy,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn copy_object_source_if_match_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "src", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let src_cond = ReadCondition {
            if_match: Some("\"0000000000000000\"".to_string()),
            ..Default::default()
        };
        let err = coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "bucket",
                    key: "src",
                    version_id: None,
                    condition: &src_cond,
                },
                dst_bucket: "bucket",
                dst_key: "dst",
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Copy,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn copy_object_dest_if_none_match_prevents_overwrite() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "src", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "dst", data: b"existing", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let dst_cond = WriteCondition::IfNoneMatchStar;
        let err = coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "bucket",
                    key: "src",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "bucket",
                dst_key: "dst",
                dst_condition: &dst_cond,
                directive: MetadataDirective::Copy,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn copy_object_dest_if_match_allows_update() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "src", data: b"new data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        let existing = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "dst", data: b"old data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let dst_cond = WriteCondition::IfMatch(SpecificEtag::new(existing.etag).unwrap());
        let result = coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "bucket",
                    key: "src",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "bucket",
                dst_key: "dst",
                dst_condition: &dst_cond,
                directive: MetadataDirective::Copy,
            })
            .unwrap();
        assert!(!result.etag.is_empty());

        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "dst", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.data, b"new data");
    }

    #[test]
    fn copy_object_cross_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("src-bucket").unwrap();
        coord.create_bucket("dst-bucket").unwrap();

        let headers = [("Content-Type", "text/plain")];
        coord
            .put_object(&PutObjectRequest { bucket: "src-bucket", key: "key", data: b"cross bucket data", metadata: &MetadataBlob::from_headers(&headers).unwrap(), cond: NO_WRITE })
            .unwrap();

        coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "src-bucket",
                    key: "key",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "dst-bucket",
                dst_key: "key",
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Copy,
            })
            .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest { bucket: "dst-bucket", key: "key", version_id: None, cond: NO_READ })
            .unwrap();
        assert_eq!(obj.data, b"cross bucket data");
        assert_eq!(obj.metadata.get("content-type"), Some("text/plain"));

        // Source should still exist
        let src = coord
            .get_object(&GetObjectRequest { bucket: "src-bucket", key: "key", version_id: None, cond: NO_READ })
            .unwrap();
        assert_eq!(src.data, b"cross bucket data");
    }

    // ── Bucket versioning tests ──────────────────────────────────────

    #[test]
    fn bucket_versioning_default_disabled() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let state = coord.get_bucket_versioning("bucket").unwrap();
        assert_eq!(state, storage::BucketVersioningState::Disabled);
    }

    #[test]
    fn bucket_versioning_enable() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_bucket_versioning("bucket", storage::BucketVersioningState::Enabled)
            .unwrap();
        assert_eq!(
            coord.get_bucket_versioning("bucket").unwrap(),
            storage::BucketVersioningState::Enabled
        );
    }

    #[test]
    fn bucket_versioning_enable_then_suspend() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_bucket_versioning("bucket", storage::BucketVersioningState::Enabled)
            .unwrap();
        coord
            .put_bucket_versioning("bucket", storage::BucketVersioningState::Suspended)
            .unwrap();
        assert_eq!(
            coord.get_bucket_versioning("bucket").unwrap(),
            storage::BucketVersioningState::Suspended
        );
    }

    #[test]
    fn bucket_versioning_suspend_then_enable() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_bucket_versioning("bucket", storage::BucketVersioningState::Enabled)
            .unwrap();
        coord
            .put_bucket_versioning("bucket", storage::BucketVersioningState::Suspended)
            .unwrap();
        coord
            .put_bucket_versioning("bucket", storage::BucketVersioningState::Enabled)
            .unwrap();
        assert_eq!(
            coord.get_bucket_versioning("bucket").unwrap(),
            storage::BucketVersioningState::Enabled
        );
    }

    #[test]
    fn bucket_versioning_cannot_disable_from_enabled() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_bucket_versioning("bucket", storage::BucketVersioningState::Enabled)
            .unwrap();
        let err = coord
            .put_bucket_versioning("bucket", storage::BucketVersioningState::Disabled)
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    #[test]
    fn bucket_versioning_nonexistent_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .put_bucket_versioning("no-bucket", storage::BucketVersioningState::Enabled)
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn put_object_returns_version_id_zero() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let result = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        assert_eq!(result.version_id, storage::VersionId::Null);
    }

    #[test]
    fn get_object_returns_version_id() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        let obj = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(obj.version_id, storage::VersionId::Null);
    }

    #[test]
    fn head_object_returns_version_id() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        let head = coord.head_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(head.version_id, storage::VersionId::Null);
    }

    #[test]
    fn versioned_put_is_safe_across_concurrent_frontends() {
        let tmp = test_util::tempdir();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();

        let make_coord = || {
            Coordinator::new(
                Arc::clone(&storage_node),
                ec_config,
                "us-east-1".to_string(),
            )
            .unwrap()
        };

        let admin = make_coord();
        admin.create_bucket("bucket").unwrap();
        admin
            .put_bucket_versioning("bucket", storage::BucketVersioningState::Enabled)
            .unwrap();

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
                coord_a.put_object(&PutObjectRequest { bucket: "bucket", key: &key_a, data: b"v1", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            });
            let t2 = thread::spawn(move || {
                b2.wait();
                coord_b.put_object(&PutObjectRequest { bucket: "bucket", key: &key_b, data: b"v2", metadata: &MetadataBlob::new(), cond: NO_WRITE })
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
        let tmp = test_util::tempdir();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();

        let make_coord = || {
            Coordinator::new(
                Arc::clone(&storage_node),
                ec_config,
                "us-east-1".to_string(),
            )
            .unwrap()
        };

        let admin = make_coord();
        admin.create_bucket("bucket").unwrap();

        let object_size = 512 * 1024;
        admin
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: &vec![b'A'; object_size], metadata: &MetadataBlob::new(), cond: NO_WRITE })
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
                writer.put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: &new_payload, metadata: &MetadataBlob::new(), cond: NO_WRITE })
            });
            let t_read = thread::spawn(move || {
                b2.wait();
                reader.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ })
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
        let tmp = test_util::tempdir();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();

        let make_coord = || {
            Coordinator::new(
                Arc::clone(&storage_node),
                ec_config,
                "us-east-1".to_string(),
            )
            .unwrap()
        };

        let admin = make_coord();
        admin.create_bucket("bucket").unwrap();

        let object_size = 256 * 1024;
        admin
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: &vec![b'A'; object_size], metadata: &MetadataBlob::new(), cond: NO_WRITE })
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
                writer.put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: &payload, metadata: &MetadataBlob::new(), cond: NO_WRITE })
            });
            let t_delete = thread::spawn(move || {
                b2.wait();
                deleter.delete_object(&DeleteObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_DELETE })
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

            let check = make_coord().get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ });
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"data", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();
        let result = coord
            .delete_object(&DeleteObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_DELETE })
            .unwrap();
        assert_eq!(result.version_id, storage::VersionId::Null);
        assert!(!result.delete_marker);
    }

    // ── Multipart upload tests ────────────────────────────────────────

    #[test]
    fn create_multipart_upload_returns_upload_id() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let result = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        // Upload ID should be 32 hex chars (16 random bytes).
        assert_eq!(result.upload_id.len(), 32);
        assert!(result.upload_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn create_multipart_upload_unique_ids() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let r1 = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();
        let r2 = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();
        assert_ne!(r1.upload_id, r2.upload_id);
    }

    #[test]
    fn create_multipart_upload_requires_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let metadata = MetadataBlob::new();
        let err = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "no-such-bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn list_multipart_uploads_empty() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let result = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest { bucket: "bucket", prefix: None, key_marker: None, upload_id_marker: None, max_uploads: 1000 })
            .unwrap();
        assert!(result.uploads.is_empty());
        assert!(!result.is_truncated);
    }

    #[test]
    fn list_multipart_uploads_returns_created() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let r1 = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "alpha", metadata: &metadata, checksum: None })
            .unwrap();
        let r2 = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "beta", metadata: &metadata, checksum: None })
            .unwrap();

        let result = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest { bucket: "bucket", prefix: None, key_marker: None, upload_id_marker: None, max_uploads: 1000 })
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        // Create two uploads for the same key.
        let r1 = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();
        let r2 = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        let result = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest { bucket: "bucket", prefix: None, key_marker: None, upload_id_marker: None, max_uploads: 1000 })
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        // Create 3 uploads for distinct keys so ordering is deterministic.
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "a", metadata: &metadata, checksum: None })
            .unwrap();
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "b", metadata: &metadata, checksum: None })
            .unwrap();
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "c", metadata: &metadata, checksum: None })
            .unwrap();

        // Page 1: max_uploads=2.
        let page1 = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest { bucket: "bucket", prefix: None, key_marker: None, upload_id_marker: None, max_uploads: 2 })
            .unwrap();
        assert_eq!(page1.uploads.len(), 2);
        assert!(page1.is_truncated);
        assert_eq!(page1.uploads[0].key, "a");
        assert_eq!(page1.uploads[1].key, "b");
        assert!(page1.next_key_marker.is_some());
        assert!(page1.next_upload_id_marker.is_some());

        // Page 2: use markers from page 1.
        let page2 = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket: "bucket",
                prefix: None,
                key_marker: page1.next_key_marker.as_deref(),
                upload_id_marker: page1.next_upload_id_marker.as_deref(),
                max_uploads: 2,
            })
            .unwrap();
        assert_eq!(page2.uploads.len(), 1);
        assert!(!page2.is_truncated);
        assert_eq!(page2.uploads[0].key, "c");
    }

    #[test]
    fn list_multipart_uploads_prefix_filter() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "photos/a.jpg", metadata: &metadata, checksum: None })
            .unwrap();
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "photos/b.jpg", metadata: &metadata, checksum: None })
            .unwrap();
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "docs/readme.md", metadata: &metadata, checksum: None })
            .unwrap();

        let result = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest { bucket: "bucket", prefix: Some("photos/"), key_marker: None, upload_id_marker: None, max_uploads: 1000 })
            .unwrap();
        assert_eq!(result.uploads.len(), 2);
        assert!(result.uploads.iter().all(|u| u.key.starts_with("photos/")));
    }

    #[test]
    fn list_multipart_uploads_max_zero() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        let result = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest { bucket: "bucket", prefix: None, key_marker: None, upload_id_marker: None, max_uploads: 0 })
            .unwrap();
        assert!(result.uploads.is_empty());
        assert!(!result.is_truncated);
    }

    #[test]
    fn list_multipart_uploads_requires_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest { bucket: "no-such-bucket", prefix: None, key_marker: None, upload_id_marker: None, max_uploads: 1000 })
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn create_multipart_upload_preserves_metadata() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::from_headers(&[
            ("Content-Type", "image/png"),
            ("X-Amz-Meta-Author", "test"),
        ])
        .unwrap();

        let result = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "photo.png", metadata: &metadata, checksum: None })
            .unwrap();

        // Verify we can retrieve the upload and its metadata blob is stored.
        let meta_pg_id = coord.object_pg_id("bucket", "photo.png");
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        // Bucket has no objects but has an in-progress MPU — should fail.
        let err = coord.delete_bucket("bucket").unwrap_err();
        assert!(matches!(err, ServerError::BucketNotEmpty));
    }

    #[test]
    fn no_such_upload_from_storage() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // Directly call get_multipart_upload on a PG with a bogus upload ID.
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let err: ServerError = pg.get_multipart_upload("nonexistent").unwrap_err().into();
        assert!(matches!(err, ServerError::NoSuchUpload { .. }));
        assert_eq!(err.s3_error_code(), "NoSuchUpload");
        assert_eq!(err.http_status(), 404);
    }

    #[test]
    fn list_multipart_uploads_same_key_pagination() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        // Create 3 uploads for the same key.
        let mut upload_ids = Vec::new();
        for _ in 0..3 {
            let r = coord
                .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
                .unwrap();
            upload_ids.push(r.upload_id);
        }

        // Page 1: max_uploads=2 — should get first 2 by initiation time.
        let page1 = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest { bucket: "bucket", prefix: None, key_marker: None, upload_id_marker: None, max_uploads: 2 })
            .unwrap();
        assert_eq!(page1.uploads.len(), 2);
        assert!(page1.is_truncated);
        assert_eq!(page1.uploads[0].key, "key");
        assert_eq!(page1.uploads[1].key, "key");
        // Initiation time ordering.
        assert!(page1.uploads[0].initiated <= page1.uploads[1].initiated);

        // Page 2: use markers from page 1 — should get remaining upload.
        let page2 = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket: "bucket",
                prefix: None,
                key_marker: page1.next_key_marker.as_deref(),
                upload_id_marker: page1.next_upload_id_marker.as_deref(),
                max_uploads: 2,
            })
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        let result = coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: b"hello world", claimed_checksum: None })
            .unwrap();

        // ETag should be a quoted hex CRC64.
        assert!(result.etag.starts_with('"'));
        assert!(result.etag.ends_with('"'));

        // Verify part metadata was recorded.
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let part = pg.get_multipart_part(&create.upload_id, 1).unwrap();
        assert_eq!(part.part_number, 1);
        assert_eq!(part.generation, 0);
        assert_eq!(part.size, 11); // "hello world".len()
    }

    #[test]
    fn upload_part_reupload_increments_generation() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        // First upload → generation 0.
        coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: b"first", claimed_checksum: None })
            .unwrap();

        // Re-upload same part number → generation 1.
        let result = coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: b"second", claimed_checksum: None })
            .unwrap();

        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let part = pg.get_multipart_part(&create.upload_id, 1).unwrap();
        assert_eq!(part.generation, 1);
        assert_eq!(part.size, 6); // "second".len()

        // ETag should reflect the new data.
        let expected_crc = checksum::crc64::checksum(b"second");
        assert_eq!(result.etag, format_etag(expected_crc));
    }

    #[test]
    fn upload_part_invalid_part_number_zero() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        let err = coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 0, data: b"data", claimed_checksum: None })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidArgument { .. }));
    }

    #[test]
    fn upload_part_invalid_part_number_exceeds_max() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        let err = coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 10_001, data: b"data", claimed_checksum: None })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidArgument { .. }));
    }

    #[test]
    fn upload_part_nonexistent_upload() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let err = coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: "bogus-upload-id", part_number: 1, data: b"data", claimed_checksum: None })
            .unwrap_err();
        assert!(matches!(err, ServerError::NoSuchUpload { .. }));
    }

    #[test]
    fn upload_part_multiple_parts() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: b"part-one", claimed_checksum: None })
            .unwrap();
        coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 2, data: b"part-two", claimed_checksum: None })
            .unwrap();
        coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 3, data: b"part-three", claimed_checksum: None })
            .unwrap();

        // Verify all three parts exist.
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();

        let parts_resp = pg
            .list_multipart_parts(&storage::ListPartsReq {
                upload_id: UploadId::from(create.upload_id.as_str()),
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        // Upload same part 4 times — generation should increment each time.
        for i in 0..4u32 {
            let data = format!("version-{i}");
            coord
                .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: data.as_bytes(), claimed_checksum: None })
                .unwrap();
        }

        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let part = pg.get_multipart_part(&create.upload_id, 1).unwrap();
        assert_eq!(part.generation, 3);
        assert_eq!(part.size, "version-3".len() as u64);
    }

    #[test]
    fn upload_part_boundary_part_numbers() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        // Part 1 (min valid).
        coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: b"a", claimed_checksum: None })
            .unwrap();
        // Part 10000 (max valid).
        coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 10_000, data: b"z", claimed_checksum: None })
            .unwrap();

        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        pg.get_multipart_part(&create.upload_id, 1).unwrap();
        pg.get_multipart_part(&create.upload_id, 10_000).unwrap();
    }

    #[test]
    fn upload_part_wrong_bucket_key_rejected() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        // Try uploading with wrong key — should be rejected even if upload_id is valid.
        let err = coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "wrong-key", upload_id: &create.upload_id, part_number: 1, data: b"data", claimed_checksum: None })
            .unwrap_err();
        assert!(matches!(err, ServerError::NoSuchUpload { .. }));

        // Try uploading with wrong bucket.
        coord.create_bucket("other-bucket").unwrap();
        let err = coord
            .upload_part(&UploadPartRequest { bucket: "other-bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: b"data", claimed_checksum: None })
            .unwrap_err();
        assert!(matches!(err, ServerError::NoSuchUpload { .. }));
    }

    #[test]
    fn upload_part_same_part_last_writer_wins() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        // Simulate concurrent same-part uploads sequentially.
        // Each successive upload should overwrite, with generation incrementing.
        let etag1 = coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: b"writer-A", claimed_checksum: None })
            .unwrap()
            .etag;
        let etag2 = coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: b"writer-B", claimed_checksum: None })
            .unwrap()
            .etag;
        let etag3 = coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: b"writer-C", claimed_checksum: None })
            .unwrap()
            .etag;

        // Each write has different data → different ETags.
        assert_ne!(etag1, etag2);
        assert_ne!(etag2, etag3);

        // Final state should reflect the last writer.
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let part = pg.get_multipart_part(&create.upload_id, 1).unwrap();
        assert_eq!(part.generation, 2); // 0, 1, 2
        assert_eq!(part.size, "writer-C".len() as u64);
        assert_eq!(format_etag(checksum::crc64::checksum(b"writer-C")), etag3);
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
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket, key, metadata: &metadata, checksum: None })
            .unwrap();
        let mut complete_parts = Vec::new();
        for &(part_number, data) in part_data {
            let result = coord
                .upload_part(&UploadPartRequest { bucket, key, upload_id: &create.upload_id, part_number, data, claimed_checksum: None })
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // Use 5MiB+ parts for non-final parts.
        let big_part = vec![0xABu8; 5 * 1024 * 1024];
        let small_last = b"final-part";

        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, &big_part), (2, small_last)]);

        let result = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
            .unwrap();

        // ETag should be composite format: "hex-2"
        assert!(result.etag.ends_with("-2\""), "etag = {}", result.etag);

        // Object should be visible via get_object metadata.
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let obj = pg.get_object_meta("bucket", "key").unwrap();
        let live_obj = obj.as_live().expect("expected live object");
        assert!(matches!(
            live_obj.layout,
            ObjectLayout::MultipartManifest { .. }
        ));
        assert_eq!(live_obj.layout.parts_count(), Some(2));
        assert_eq!(
            live_obj.size,
            big_part.len() as u64 + small_last.len() as u64
        );

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
        let tmp = test_util::tempdir();
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidPart { part_number: 2 }));
    }

    #[test]
    fn complete_multipart_upload_wrong_etag() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, mut parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);

        // Tamper with the ETag.
        parts[0].etag = "\"ffffffffffffffff\"".to_string();

        let err = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidPart { part_number: 1 }));
    }

    #[test]
    fn complete_multipart_upload_invalid_order() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1"), (2, b"data2")]);

        // Reverse the order.
        let reversed = vec![parts[1].clone(), parts[0].clone()];
        let err = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &reversed, claimed_checksum: None })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidPartOrder));
    }

    #[test]
    fn complete_multipart_upload_too_small_non_final_part() {
        let tmp = test_util::tempdir();
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
            .unwrap_err();
        assert!(matches!(
            err,
            ServerError::EntityTooSmall { part_number: 1, .. }
        ));
    }

    #[test]
    fn complete_multipart_upload_single_part_any_size() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // A single part can be any size (it's the "final" part).
        let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"tiny")]);

        let result = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
            .unwrap();
        assert!(result.etag.ends_with("-1\""));
    }

    #[test]
    fn complete_multipart_upload_empty_part_list() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        let err = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, parts: &[], claimed_checksum: None })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    #[test]
    fn complete_multipart_upload_retry_after_validation_failure() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // Upload two small parts.
        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"small"), (2, b"last")]);

        // First attempt fails because part 1 is too small.
        let err = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
            .unwrap_err();
        assert!(matches!(err, ServerError::EntityTooSmall { .. }));

        // Upload remains usable — re-upload part 1 with large data and retry.
        let big_data = vec![0u8; 5 * 1024 * 1024];
        let new_part1 = coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &upload_id, part_number: 1, data: &big_data, claimed_checksum: None })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &retry_parts, claimed_checksum: None })
            .unwrap();
        assert!(result.etag.ends_with("-2\""));
    }

    #[test]
    fn complete_multipart_upload_duplicate_part_numbers() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);

        // Duplicate part number 1.
        let duped = vec![parts[0].clone(), parts[0].clone()];
        let err = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &duped, claimed_checksum: None })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidPartOrder));
    }

    #[test]
    fn complete_multipart_upload_overwrite_unversioned() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // First multipart upload to key.
        let (upload_id1, parts1) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"first-upload")]);
        let result1 = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id1, parts: &parts1, claimed_checksum: None })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id2, parts: &parts2, claimed_checksum: None })
            .unwrap();
        assert!(result2.etag.ends_with("-2\""));
        assert_ne!(result1.etag, result2.etag);

        // Verify the object was overwritten — should have 2 parts now.
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let obj = pg.get_object_meta("bucket", "key").unwrap();
        let live_obj = obj.as_live().expect("expected live object");
        assert_eq!(live_obj.layout.parts_count(), Some(2));

        // Old manifest parts (from first upload) should be replaced.
        let committed = pg
            .get_object_parts("bucket", "key", storage::VersionId::Null)
            .unwrap();
        assert_eq!(committed.len(), 2);
    }

    #[test]
    fn complete_multipart_upload_list_shows_composite_etag() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);
        let result = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
            .unwrap();

        // list_objects_v2 should return the composite ETag with -N suffix.
        let list = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: None, continuation_token: None, max_keys: 100 })
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord
            .put_bucket_versioning("bucket", storage::BucketVersioningState::Enabled)
            .unwrap();

        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);
        let result = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
            .unwrap();

        let versions = coord
            .list_object_versions(&ListObjectVersionsRequest { bucket: "bucket", prefix: None, key_marker: None, version_id_marker: None, max_keys: 100 })
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, _parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part1"), (2, b"part2")]);

        coord
            .abort_multipart_upload(&AbortMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id })
            .unwrap();

        // Upload should no longer exist.
        let err = coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &upload_id, part_number: 1, data: b"nope", claimed_checksum: None })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );

        // ListMultipartUploads should be empty.
        let uploads = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest { bucket: "bucket", prefix: None, key_marker: None, upload_id_marker: None, max_uploads: 100 })
            .unwrap();
        assert!(uploads.uploads.is_empty());
    }

    #[test]
    fn abort_multipart_upload_nonexistent() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let err = coord
            .abort_multipart_upload(&AbortMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: "no-such-upload" })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );
    }

    #[test]
    fn abort_multipart_upload_idempotent() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        // First abort succeeds.
        coord
            .abort_multipart_upload(&AbortMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id })
            .unwrap();

        // Second abort: upload is already deleted, returns UploadNotFound.
        let err = coord
            .abort_multipart_upload(&AbortMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected UploadNotFound on second abort, got {err:?}"
        );
    }

    #[test]
    fn abort_multipart_upload_wrong_bucket_key() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord.create_bucket("other").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        let err = coord
            .abort_multipart_upload(&AbortMultipartUploadRequest { bucket: "other", key: "key", upload_id: &create.upload_id })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );
    }

    #[test]
    fn abort_does_not_affect_completed_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // Create and complete an upload.
        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);
        coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
            .unwrap();

        // Abort the same upload_id should fail (already deleted by complete).
        let err = coord
            .abort_multipart_upload(&AbortMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );

        // Object should still exist (visible in listing).
        let list = coord
            .list_objects_v2(&ListObjectsV2Request { bucket: "bucket", prefix: None, delimiter: None, continuation_token: None, max_keys: 100 })
            .unwrap();
        assert_eq!(list.objects.len(), 1);
        assert_eq!(list.objects[0].key, "key");
    }

    #[test]
    fn upload_part_after_abort_rejected() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();
        coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: b"data", claimed_checksum: None })
            .unwrap();

        coord
            .abort_multipart_upload(&AbortMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id })
            .unwrap();

        let err = coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 2, data: b"more", claimed_checksum: None })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );
    }

    // --- ListParts tests ---

    #[test]
    fn list_parts_basic() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, _parts) = create_upload_with_parts(
            &coord,
            "bucket",
            "key",
            &[(1, b"data1"), (3, b"data3"), (5, b"data5")],
        );

        let result = coord
            .list_parts(&ListPartsRequest { bucket: "bucket", key: "key", upload_id: &upload_id, part_number_marker: None, max_parts: 100 })
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
        let tmp = test_util::tempdir();
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
            .list_parts(&ListPartsRequest { bucket: "bucket", key: "key", upload_id: &upload_id, part_number_marker: None, max_parts: 2 })
            .unwrap();
        assert_eq!(page1.parts.len(), 2);
        assert_eq!(page1.parts[0].part_number, 1);
        assert_eq!(page1.parts[1].part_number, 2);
        assert!(page1.is_truncated);
        assert!(page1.next_part_number_marker.is_some());

        // Page 2: continue from marker
        let page2 = coord
            .list_parts(&ListPartsRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                part_number_marker: page1.next_part_number_marker,
                max_parts: 2,
            })
            .unwrap();
        assert_eq!(page2.parts.len(), 2);
        assert_eq!(page2.parts[0].part_number, 3);
        assert_eq!(page2.parts[1].part_number, 4);
        assert!(!page2.is_truncated);
    }

    #[test]
    fn list_parts_etag_format() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, complete_parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"hello")]);

        let result = coord
            .list_parts(&ListPartsRequest { bucket: "bucket", key: "key", upload_id: &upload_id, part_number_marker: None, max_parts: 100 })
            .unwrap();
        assert_eq!(result.parts.len(), 1);
        // ListParts ETag should match the ETag returned by UploadPart.
        assert_eq!(result.parts[0].etag, complete_parts[0].etag);
    }

    #[test]
    fn list_parts_wrong_bucket_key() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord.create_bucket("other").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        let err = coord
            .list_parts(&ListPartsRequest { bucket: "other", key: "key", upload_id: &create.upload_id, part_number_marker: None, max_parts: 100 })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );
    }

    #[test]
    fn list_parts_nonexistent_upload() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let err = coord
            .list_parts(&ListPartsRequest { bucket: "bucket", key: "key", upload_id: "no-such-upload", part_number_marker: None, max_parts: 100 })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );
    }

    #[test]
    fn list_parts_after_reupload_shows_latest() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        // Upload part 1, then overwrite it.
        coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: b"original", claimed_checksum: None })
            .unwrap();
        let reupload = coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: b"replaced", claimed_checksum: None })
            .unwrap();

        let result = coord
            .list_parts(&ListPartsRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number_marker: None, max_parts: 100 })
            .unwrap();
        assert_eq!(result.parts.len(), 1);
        assert_eq!(result.parts[0].etag, reupload.etag);
        assert_eq!(result.parts[0].size, "replaced".len() as u64);
    }

    #[test]
    fn list_parts_rejected_when_aborting() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();
        coord
            .upload_part(&UploadPartRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number: 1, data: b"data", claimed_checksum: None })
            .unwrap();

        // Manually transition to Aborting (simulates the window during abort).
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        pg.set_upload_state(&create.upload_id, UploadState::Aborting)
            .unwrap();
        drop(pg);

        let err = coord
            .list_parts(&ListPartsRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id, part_number_marker: None, max_parts: 100 })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );
    }

    #[test]
    fn abort_completing_upload_returns_no_such_upload() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        // Manually transition to Completing (simulates concurrent complete).
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        pg.set_upload_state(&create.upload_id, UploadState::Completing)
            .unwrap();
        drop(pg);

        let err = coord
            .abort_multipart_upload(&AbortMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &create.upload_id })
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
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket, key, metadata: &metadata, checksum: None })
            .unwrap();
        let mut complete_parts = Vec::new();
        for (part_number, data) in part_data {
            let result = coord
                .upload_part(&UploadPartRequest { bucket, key, upload_id: &create.upload_id, part_number: *part_number, data, claimed_checksum: None })
                .unwrap();
            complete_parts.push(CompletePart {
                part_number: *part_number,
                etag: result.etag,
                checksum: None,
            });
        }
        coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket, key, upload_id: &create.upload_id, parts: &complete_parts, claimed_checksum: None })
            .unwrap()
    }

    #[test]
    fn get_multipart_object_full() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let part2 = make_part(0xBB, 100);
        let expected: Vec<u8> = [part1.as_slice(), part2.as_slice()].concat();

        let result =
            create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

        let obj = coord
            .get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &ReadCondition::default() })
            .unwrap();
        assert_eq!(obj.data, expected);
        assert_eq!(obj.etag, result.etag);
        assert_eq!(obj.size, expected.len() as u64);
        assert!(obj.etag.ends_with("-2\""), "etag = {}", obj.etag);
    }

    #[test]
    fn get_multipart_object_single_part() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, b"only-part".to_vec())]);

        let obj = coord
            .get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &ReadCondition::default() })
            .unwrap();
        assert_eq!(obj.data, b"only-part");
    }

    #[test]
    fn head_multipart_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let part2 = make_part(0xBB, 200);
        let total_size = part1.len() + part2.len();

        let result =
            create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

        let head = coord
            .head_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &ReadCondition::default() })
            .unwrap();
        assert_eq!(head.size, total_size as u64);
        assert_eq!(head.etag, result.etag);
        assert!(head.etag.ends_with("-2\""), "etag = {}", head.etag);
    }

    #[test]
    fn get_multipart_object_range_within_part() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let part2 = make_part(0xBB, 100);

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

        // Range within first part: bytes 10-19
        let range = coord
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "key", version_id: None, range: ByteRange::Range { start: 10, end: 19 }, cond: &ReadCondition::default() })
            .unwrap();
        assert_eq!(range.data, vec![0xAA; 10]);
        assert_eq!(range.range_start, 10);
        assert_eq!(range.range_end, 19);
    }

    #[test]
    fn get_multipart_object_range_spanning_parts() {
        let tmp = test_util::tempdir();
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
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "key", version_id: None, range: ByteRange::Range {
                    start: boundary - 4,
                    end: boundary + 3,
                }, cond: &ReadCondition::default() })
            .unwrap();
        let mut expected = vec![0xAA; 4];
        expected.extend_from_slice(&[0xBB; 4]);
        assert_eq!(range.data, expected);
    }

    #[test]
    fn get_multipart_object_range_suffix() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let part2 = make_part(0xBB, 100);

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

        // Suffix range: last 50 bytes (all within part2)
        let range = coord
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "key", version_id: None, range: ByteRange::Suffix { length: 50 }, cond: &ReadCondition::default() })
            .unwrap();
        assert_eq!(range.data, vec![0xBB; 50]);
    }

    #[test]
    fn copy_multipart_source() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("src-bucket").unwrap();
        coord.create_bucket("dst-bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let part2 = make_part(0xBB, 200);
        let expected: Vec<u8> = [part1.as_slice(), part2.as_slice()].concat();

        create_completed_multipart_vec(&coord, "src-bucket", "src-key", &[(1, part1), (2, part2)]);

        // Copy multipart source to destination (creates inline object).
        coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "src-bucket",
                    key: "src-key",
                    version_id: None,
                    condition: &ReadCondition::default(),
                },
                dst_bucket: "dst-bucket",
                dst_key: "dst-key",
                dst_condition: &WriteCondition::default(),
                directive: MetadataDirective::Copy,
            })
            .unwrap();

        // Destination should have the concatenated data as inline object.
        let dst = coord
            .get_object(&GetObjectRequest { bucket: "dst-bucket", key: "dst-key", version_id: None, cond: &ReadCondition::default() })
            .unwrap();
        assert_eq!(dst.data, expected);
    }

    #[test]
    fn get_multipart_object_zero_byte_single_part() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, vec![])]);

        let obj = coord
            .get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &ReadCondition::default() })
            .unwrap();
        assert!(obj.data.is_empty());
        assert_eq!(obj.size, 0);
    }

    #[test]
    fn get_multipart_object_zero_byte_final_part() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let expected = part1.clone();

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, vec![])]);

        let obj = coord
            .get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &ReadCondition::default() })
            .unwrap();
        assert_eq!(obj.data, expected);
        assert_eq!(obj.size, MIN_PART as u64);
    }

    #[test]
    fn get_object_part_zero_byte_single_part() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, vec![])]);

        let result = coord
            .get_object_part(&GetObjectPartRequest { bucket: "bucket", key: "key", version_id: None, part_number: 1, cond: &ReadCondition::default() })
            .unwrap();
        assert!(result.data.is_empty());
        assert_eq!(result.size, 0);
        assert_eq!(result.parts_count, 1);
        assert_eq!(result.part_start, 0);
        assert_eq!(result.part_end, 0);
    }

    #[test]
    fn get_object_part_zero_byte_final_part() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1.clone()), (2, vec![])]);

        // Part 1 should return full data
        let result = coord
            .get_object_part(&GetObjectPartRequest { bucket: "bucket", key: "key", version_id: None, part_number: 1, cond: &ReadCondition::default() })
            .unwrap();
        assert_eq!(result.data, part1);
        assert_eq!(result.part_start, 0);
        assert_eq!(result.part_end, MIN_PART as u64 - 1);

        // Part 2 (zero-byte) should return empty data
        let result = coord
            .get_object_part(&GetObjectPartRequest { bucket: "bucket", key: "key", version_id: None, part_number: 2, cond: &ReadCondition::default() })
            .unwrap();
        assert!(result.data.is_empty());
        assert_eq!(result.parts_count, 2);
        assert_eq!(result.part_start, MIN_PART as u64);
        assert_eq!(result.part_end, MIN_PART as u64);
    }

    #[test]
    fn head_object_part_non_multipart() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"hello world", metadata: &MetadataBlob::from_headers(&[("x-amz-meta-foo", "bar")]).unwrap(), cond: NO_WRITE })
            .unwrap();

        // partNumber=1 on non-multipart object returns the full object.
        let result = coord
            .head_object_part(&GetObjectPartRequest { bucket: "bucket", key: "key", version_id: None, part_number: 1, cond: &ReadCondition::default() })
            .unwrap();
        assert_eq!(result.part_size, 11);
        assert_eq!(result.total_size, 11);
        assert_eq!(result.parts_count, 1);
        assert_eq!(result.metadata.get("x-amz-meta-foo"), Some("bar"));

        // partNumber=2 on non-multipart object returns InvalidPart.
        let err = coord
            .head_object_part(&GetObjectPartRequest { bucket: "bucket", key: "key", version_id: None, part_number: 2, cond: &ReadCondition::default() })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidPart { part_number: 2 }));
    }

    #[test]
    fn head_object_part_non_multipart_zero_byte() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"", metadata: &MetadataBlob::new(), cond: NO_WRITE })
            .unwrap();

        let result = coord
            .head_object_part(&GetObjectPartRequest { bucket: "bucket", key: "key", version_id: None, part_number: 1, cond: &ReadCondition::default() })
            .unwrap();
        assert_eq!(result.part_size, 0);
        assert_eq!(result.total_size, 0);
        assert_eq!(result.parts_count, 1);
    }

    #[test]
    fn head_multipart_object_zero_byte() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, vec![])]);

        let head = coord
            .head_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &ReadCondition::default() })
            .unwrap();
        assert_eq!(head.size, 0);
    }

    #[test]
    fn copy_multipart_source_zero_byte() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("src").unwrap();
        coord.create_bucket("dst").unwrap();

        create_completed_multipart_vec(&coord, "src", "key", &[(1, vec![])]);

        coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "src",
                    key: "key",
                    version_id: None,
                    condition: &ReadCondition::default(),
                },
                dst_bucket: "dst",
                dst_key: "key",
                dst_condition: &WriteCondition::default(),
                directive: MetadataDirective::Copy,
            })
            .unwrap();

        let dst = coord
            .get_object(&GetObjectRequest { bucket: "dst", key: "key", version_id: None, cond: &ReadCondition::default() })
            .unwrap();
        assert!(dst.data.is_empty());
    }

    #[test]
    fn read_multipart_range_detects_incomplete_manifest() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        // Create a multipart object, then corrupt manifest by deleting a part row.
        let part1 = make_part(0xAA, MIN_PART);
        let part2 = make_part(0xBB, 100);

        let result =
            create_completed_multipart_vec(&coord, "bucket", "key", &[(1, part1), (2, part2)]);

        // Get the real manifest, then replace with only part 2 (gap: part 1 missing).
        let meta_pg_id = coord.object_pg_id("bucket", "key");
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
            .get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &ReadCondition::default() })
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
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket, key, metadata: &metadata, checksum: Some(storage::MultipartChecksumConfig::new(algo, ctype).unwrap()) })
            .unwrap();
        let mut complete_parts = Vec::new();
        for (i, data) in part_data.iter().enumerate() {
            let part_number = (i + 1) as u32;
            let checksum_b64 = b64.encode(compute_checksum(algo, data));
            let claim = ChecksumClaim::from_base64(algo, &checksum_b64).unwrap();
            let result = coord
                .upload_part(&UploadPartRequest { bucket, key, upload_id: &create.upload_id, part_number, data, claimed_checksum: Some(&claim) })
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
        let tmp = test_util::tempdir();
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
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
        let tmp = test_util::tempdir();
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
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
        let tmp = test_util::tempdir();
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
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
        let tmp = test_util::tempdir();
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
            .unwrap();

        assert_eq!(
            result.checksum_algorithm,
            Some(ChecksumAlgorithm::Crc64nvme)
        );
        assert_eq!(result.checksum_type, Some(ChecksumType::FullObject));

        let mut full_data = big.clone();
        full_data.extend_from_slice(small);
        let expected_crc = checksum::crc64::checksum(&full_data);
        let expected = b64.encode(expected_crc.to_be_bytes());
        assert_eq!(result.checksum_value.unwrap(), expected);
    }

    #[test]
    fn complete_multipart_crc32_composite_checksum() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD;
        let tmp = test_util::tempdir();
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
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
        let tmp = test_util::tempdir();
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::InvalidRequest { .. }),
            "expected InvalidRequest, got {err:?}"
        );
    }

    #[test]
    fn complete_multipart_no_checksum_returns_none() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let big = vec![0u8; 5 * 1024 * 1024];
        let small = b"last";
        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, &big), (2, small)]);

        let result = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
            .unwrap();

        assert_eq!(result.checksum_algorithm, None);
        assert_eq!(result.checksum_type, None);
        assert_eq!(result.checksum_value, None);
    }

    #[test]
    fn complete_multipart_wrong_checksum_element_type_rejected() {
        let tmp = test_util::tempdir();
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &upload_id, parts: &parts, claimed_checksum: None })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::InvalidRequest { .. }),
            "expected InvalidRequest for wrong element type, got {err:?}"
        );
    }

    // ── Streaming upload session tests ──────────────────────────────────

    #[test]
    fn stream_put_happy_path() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // Begin session.
        let session_id = coord.begin_stream_put("bucket", "mykey").unwrap();
        assert_eq!(session_id.len(), 32);

        // Append two chunks.
        let chunk0 = b"hello ";
        let chunk1 = b"world";
        coord
            .append_stream_chunk("bucket", "mykey", &session_id, 0, chunk0)
            .unwrap();
        coord
            .append_stream_chunk("bucket", "mykey", &session_id, 1, chunk1)
            .unwrap();

        // Finalize with caller-computed CRC64 and total_size.
        let mut full_data = Vec::new();
        full_data.extend_from_slice(chunk0);
        full_data.extend_from_slice(chunk1);
        let crc = checksum::crc64::checksum(&full_data);
        let metadata = MetadataBlob::from_headers(&[("x-amz-meta-foo", "bar")]).unwrap();
        let result = coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "mykey",
                session_id: &session_id,
                crc64: crc,
                total_size: full_data.len() as u64,
                metadata_blob: &metadata,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        assert_eq!(result.etag, format_etag(crc));
        assert_eq!(result.version_id, storage::VersionId::Null);

        // Verify object is visible via head_object.
        let head = coord.head_object(&GetObjectRequest { bucket: "bucket", key: "mykey", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(head.size, full_data.len() as u64);
        assert_eq!(head.etag, format_etag(crc));
        assert_eq!(head.metadata.get("x-amz-meta-foo"), Some("bar"));
    }

    #[test]
    fn stream_put_zero_byte_object() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "mykey").unwrap();

        // Finalize with no chunks appended — zero-byte object.
        let crc = checksum::crc64::checksum(&[]);
        let metadata = MetadataBlob::new();
        let result = coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "mykey",
                session_id: &session_id,
                crc64: crc,
                total_size: 0,
                metadata_blob: &metadata,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        assert_eq!(result.etag, format_etag(crc));

        let head = coord.head_object(&GetObjectRequest { bucket: "bucket", key: "mykey", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(head.size, 0);
    }

    #[test]
    fn stream_put_abort() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "mykey").unwrap();
        coord
            .append_stream_chunk("bucket", "mykey", &session_id, 0, b"data")
            .unwrap();

        // Abort the session.
        coord
            .abort_stream_put("bucket", "mykey", &session_id)
            .unwrap();

        // Object should not exist.
        let err = coord
            .head_object(&GetObjectRequest { bucket: "bucket", key: "mykey", version_id: None, cond: NO_READ })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn stream_put_append_after_finalize_fails() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "mykey").unwrap();
        let crc = checksum::crc64::checksum(&[]);
        let metadata = MetadataBlob::new();
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "mykey",
                session_id: &session_id,
                crc64: crc,
                total_size: 0,
                metadata_blob: &metadata,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Session is deleted after finalize — append should fail.
        let err = coord
            .append_stream_chunk("bucket", "mykey", &session_id, 0, b"data")
            .unwrap_err();
        assert!(
            matches!(
                err,
                ServerError::Metadata(storage::MetadataError::StreamSessionNotFound { .. })
            ),
            "expected StreamSessionNotFound, got {err:?}"
        );
    }

    #[test]
    fn stream_put_finalize_after_abort_fails() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "mykey").unwrap();
        coord
            .abort_stream_put("bucket", "mykey", &session_id)
            .unwrap();

        let crc = checksum::crc64::checksum(&[]);
        let metadata = MetadataBlob::new();
        let err = coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "mykey",
                session_id: &session_id,
                crc64: crc,
                total_size: 0,
                metadata_blob: &metadata,
                cond: &WriteCondition::default(),
            })
            .unwrap_err();
        assert!(
            matches!(
                err,
                ServerError::Metadata(storage::MetadataError::StreamSessionNotFound { .. })
            ),
            "expected StreamSessionNotFound, got {err:?}"
        );
    }

    #[test]
    fn stream_put_bucket_key_mismatch_append() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "key1").unwrap();

        // Attempt append with wrong key.
        let err = coord
            .append_stream_chunk("bucket", "key2", &session_id, 0, b"data")
            .unwrap_err();
        // The session lives on key1's metadata PG. If key2 maps to a different PG,
        // the session won't be found. If same PG, the bucket/key check catches it.
        assert!(
            matches!(
                err,
                ServerError::InvalidRequest { .. }
                    | ServerError::Metadata(storage::MetadataError::StreamSessionNotFound { .. })
            ),
            "expected mismatch error, got {err:?}"
        );
    }

    #[test]
    fn stream_put_bucket_key_mismatch_finalize() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "key1").unwrap();

        let crc = checksum::crc64::checksum(&[]);
        let metadata = MetadataBlob::new();
        let err = coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key2",
                session_id: &session_id,
                crc64: crc,
                total_size: 0,
                metadata_blob: &metadata,
                cond: &WriteCondition::default(),
            })
            .unwrap_err();
        assert!(
            matches!(
                err,
                ServerError::InvalidRequest { .. }
                    | ServerError::Metadata(storage::MetadataError::StreamSessionNotFound { .. })
            ),
            "expected mismatch error, got {err:?}"
        );
    }

    #[test]
    fn stream_put_nonexistent_bucket() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());

        let err = coord.begin_stream_put("nonexistent", "key").unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn stream_put_overwrite_existing_object() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // Write an existing object via normal put.
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"old-data", metadata: &MetadataBlob::new(), cond: &WriteCondition::default() })
            .unwrap();

        // Stream-put a new version.
        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        let new_data = b"new-streamed-data";
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, new_data)
            .unwrap();

        let crc = checksum::crc64::checksum(new_data.as_slice());
        let metadata = MetadataBlob::new();
        let result = coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: crc,
                total_size: new_data.len() as u64,
                metadata_blob: &metadata,
                cond: &WriteCondition::default(),
            })
            .unwrap();
        assert_eq!(result.etag, format_etag(crc));

        // Head should show the new object.
        let head = coord.head_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(head.size, new_data.len() as u64);
    }

    #[test]
    fn stream_put_with_write_condition() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // Write initial object.
        let initial = coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"initial", metadata: &MetadataBlob::new(), cond: &WriteCondition::default() })
            .unwrap();

        // Stream put with if-match on the correct etag succeeds.
        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"updated")
            .unwrap();
        let crc = checksum::crc64::checksum(b"updated");
        let metadata = MetadataBlob::new();
        let cond = WriteCondition::IfMatch(SpecificEtag::new(initial.etag.clone()).unwrap());
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: crc,
                total_size: 7,
                metadata_blob: &metadata,
                cond: &cond,
            })
            .unwrap();

        // Stream put with if-match on a wrong etag fails.
        let session_id2 = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id2, 0, b"third")
            .unwrap();
        let bad_cond =
            WriteCondition::IfMatch(SpecificEtag::new("\"0000000000000000\"".to_string()).unwrap());
        let err = coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id2,
                crc64: checksum::crc64::checksum(b"third"),
                total_size: 5,
                metadata_blob: &metadata,
                cond: &bad_cond,
            })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::PreconditionFailed),
            "expected PreconditionFailed, got {err:?}"
        );
    }

    #[test]
    fn stream_put_multiple_chunks_correct_manifest() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "key").unwrap();

        // Append 3 chunks.
        let chunks: Vec<&[u8]> = vec![b"aaa", b"bbb", b"ccc"];
        for (i, chunk) in chunks.iter().enumerate() {
            coord
                .append_stream_chunk("bucket", "key", &session_id, i as u32, chunk)
                .unwrap();
        }

        let mut full_data = Vec::new();
        for chunk in &chunks {
            full_data.extend_from_slice(chunk);
        }
        let crc = checksum::crc64::checksum(&full_data);
        let metadata = MetadataBlob::new();
        let result = coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: crc,
                total_size: full_data.len() as u64,
                metadata_blob: &metadata,
                cond: &WriteCondition::default(),
            })
            .unwrap();
        assert_eq!(result.etag, format_etag(crc));

        // Verify the committed chunk manifest exists in the metadata PG.
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let committed = pg
            .get_stream_object_chunks("bucket", "key", result.version_id)
            .unwrap();
        assert_eq!(committed.len(), 3);
        for (i, chunk) in committed.iter().enumerate() {
            assert_eq!(chunk.chunk_index, i as u32);
            assert_eq!(chunk.size, 3); // "aaa", "bbb", "ccc" are all 3 bytes
        }
    }

    #[test]
    fn stream_put_abort_cleans_up_shards() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"data-to-clean")
            .unwrap();

        // Record shard keys before abort for verification.
        let chunk_okh = crate::pg::chunk_key_hash(&session_id, 0);
        let shard_pg_id = coord.shard_pg_id(
            &format!("chunk/{session_id}"),
            "0",
            storage::VersionId::Null,
        );

        coord
            .abort_stream_put("bucket", "key", &session_id)
            .unwrap();

        // Verify shards were cleaned up.
        let pg = coord.storage_node.get_pg(shard_pg_id).unwrap();
        for i in 0..6 {
            // k=4, m=2
            let shard_key = ShardKey::new(&chunk_okh, 0, i);
            let result = pg.read_shard(&shard_key);
            assert!(result.is_err(), "shard {i} should have been deleted");
        }
    }

    #[test]
    fn stream_put_get_object_readable() {
        // Stream-finalized objects use chunk manifests for shard data.
        // GET reads from stream_object_chunks to reconstruct the object.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"hello")
            .unwrap();
        let crc = checksum::crc64::checksum(b"hello");
        let metadata = MetadataBlob::new();
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: crc,
                total_size: 5,
                metadata_blob: &metadata,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // HEAD works (metadata-only).
        let head = coord.head_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(head.size, 5);

        // GET returns the correct data.
        let result = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(result.data, b"hello");
        assert_eq!(result.size, 5);
    }

    #[test]
    fn stream_put_get_multi_chunk() {
        // Stream-put with multiple chunks: GET reconstructs all chunks.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"aaaa")
            .unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 1, b"bbbb")
            .unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 2, b"cc")
            .unwrap();

        let full_data = b"aaaabbbbcc";
        let crc = checksum::crc64::checksum(full_data);
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: crc,
                total_size: 10,
                metadata_blob: &MetadataBlob::new(),
                cond: &WriteCondition::default(),
            })
            .unwrap();

        let result = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(result.data, full_data);
        assert_eq!(result.size, 10);
    }

    #[test]
    fn stream_put_range_read() {
        // Range reads on stream-put objects work correctly.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"AAAA")
            .unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 1, b"BBBB")
            .unwrap();

        let full_data = b"AAAABBBB";
        let crc = checksum::crc64::checksum(full_data);
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: crc,
                total_size: 8,
                metadata_blob: &MetadataBlob::new(),
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Range within first chunk.
        let r1 = coord
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "key", version_id: None, range: ByteRange::Range { start: 0, end: 3 }, cond: NO_READ })
            .unwrap();
        assert_eq!(r1.data, b"AAAA");

        // Range spanning chunks.
        let r2 = coord
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "key", version_id: None, range: ByteRange::Range { start: 2, end: 5 }, cond: NO_READ })
            .unwrap();
        assert_eq!(r2.data, b"AABB");

        // Range within second chunk.
        let r3 = coord
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "key", version_id: None, range: ByteRange::Range { start: 4, end: 7 }, cond: NO_READ })
            .unwrap();
        assert_eq!(r3.data, b"BBBB");

        // Suffix range.
        let r4 = coord
            .get_object_range(&GetObjectRangeRequest { bucket: "bucket", key: "key", version_id: None, range: ByteRange::Suffix { length: 3 }, cond: NO_READ })
            .unwrap();
        assert_eq!(r4.data, b"BBB");
    }

    #[test]
    fn stream_put_copy_object() {
        // CopyObject from a stream-put source works correctly.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "src").unwrap();
        coord
            .append_stream_chunk("bucket", "src", &session_id, 0, b"copy-me")
            .unwrap();
        let crc = checksum::crc64::checksum(b"copy-me");
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "src",
                session_id: &session_id,
                crc64: crc,
                total_size: 7,
                metadata_blob: &MetadataBlob::new(),
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Copy to destination.
        coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "bucket",
                    key: "src",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "bucket",
                dst_key: "dst",
                dst_condition: &WriteCondition::default(),
                directive: MetadataDirective::Copy,
            })
            .unwrap();

        // Destination should be a normal (non-chunk-manifest) object.
        let result = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "dst", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(result.data, b"copy-me");
    }

    #[test]
    fn stream_put_zero_byte_get() {
        // Zero-byte stream-put objects are readable.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "empty").unwrap();
        let crc = checksum::crc64::checksum(b"");
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "empty",
                session_id: &session_id,
                crc64: crc,
                total_size: 0,
                metadata_blob: &MetadataBlob::new(),
                cond: &WriteCondition::default(),
            })
            .unwrap();

        let result = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "empty", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(result.data, b"");
        assert_eq!(result.size, 0);
    }

    #[test]
    fn stream_put_get_object_part() {
        // partNumber=1 on stream-put objects returns the full body.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"partdata")
            .unwrap();
        let crc = checksum::crc64::checksum(b"partdata");
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: crc,
                total_size: 8,
                metadata_blob: &MetadataBlob::new(),
                cond: &WriteCondition::default(),
            })
            .unwrap();

        let result = coord
            .get_object_part(&GetObjectPartRequest { bucket: "bucket", key: "key", version_id: None, part_number: 1, cond: NO_READ })
            .unwrap();
        assert_eq!(result.data, b"partdata");
    }

    #[test]
    fn stream_put_overwrite_with_normal_put_cleans_chunks() {
        // P0 fix: normal PUT after stream-write must clear stale chunk rows.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // Stream-write an object.
        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"stream-data")
            .unwrap();
        let crc = checksum::crc64::checksum(b"stream-data");
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: crc,
                total_size: 11,
                metadata_blob: &MetadataBlob::new(),
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Verify stream-put is readable.
        let r1 = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(r1.data, b"stream-data");

        // Overwrite with a normal PUT.
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "key", data: b"normal-data", metadata: &MetadataBlob::new(), cond: &WriteCondition::default() })
            .unwrap();

        // GET should return the new data, not stale chunk data.
        let r2 = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(r2.data, b"normal-data");
    }

    #[test]
    fn stream_put_delete_cleans_chunks() {
        // P1 fix: delete must clean up stream_object_chunks and their shards.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"delete-me")
            .unwrap();
        let crc = checksum::crc64::checksum(b"delete-me");
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: crc,
                total_size: 9,
                metadata_blob: &MetadataBlob::new(),
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Delete the object.
        coord
            .delete_object(&DeleteObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: &crate::conditional::DeleteCondition::default() })
            .unwrap();

        // Object should be gone.
        let err = coord
            .get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn stream_put_upload_part_copy_from_stream_source() {
        // P2 fix: UploadPartCopy must be able to read stream-written source objects.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // Stream-write a source object.
        let session_id = coord.begin_stream_put("bucket", "src").unwrap();
        coord
            .append_stream_chunk("bucket", "src", &session_id, 0, b"source-data")
            .unwrap();
        let crc = checksum::crc64::checksum(b"source-data");
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "src",
                session_id: &session_id,
                crc64: crc,
                total_size: 11,
                metadata_blob: &MetadataBlob::new(),
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Create a multipart upload for the destination.
        let upload = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "dst", metadata: &MetadataBlob::new(), checksum: None })
            .unwrap();

        // UploadPartCopy from the stream-written source.
        let result = coord
            .upload_part_copy(&UploadPartCopyRequest {
                source: CopySource {
                    bucket: "bucket",
                    key: "src",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "bucket",
                dst_key: "dst",
                upload_id: &upload.upload_id,
                part_number: 1,
                copy_source_range: None,
            })
            .unwrap();
        assert!(!result.etag.is_empty());
    }

    #[test]
    fn stream_put_duplicate_chunk_index_rejected() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"first")
            .unwrap();

        // Appending the same chunk_index again should be rejected.
        let err = coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"second")
            .unwrap_err();
        assert!(
            matches!(err, ServerError::InvalidRequest { .. }),
            "expected InvalidRequest for duplicate chunk_index, got {err:?}"
        );

        // Original chunk should still be intact — verify by finalizing.
        let crc = checksum::crc64::checksum(b"first");
        let metadata = MetadataBlob::new();
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: crc,
                total_size: 5,
                metadata_blob: &metadata,
                cond: &WriteCondition::default(),
            })
            .unwrap();
    }

    #[test]
    fn stream_put_total_size_mismatch_rejected() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"hello")
            .unwrap();

        // Finalize with wrong total_size.
        let crc = checksum::crc64::checksum(b"hello");
        let metadata = MetadataBlob::new();
        let err = coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: crc,
                total_size: 999, // wrong — actual is 5
                metadata_blob: &metadata,
                cond: &WriteCondition::default(),
            })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::InvalidRequest { .. }),
            "expected InvalidRequest for total_size mismatch, got {err:?}"
        );
    }

    #[test]
    fn stream_append_accepts_upload_part_session() {
        // append_stream_chunk accepts both PutObject and UploadPart sessions.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        pg.create_stream_upload(&CreateStreamUploadReq {
            session_id: SessionId::from("upload-part-session"),
            bucket: BucketName::from("bucket"),
            key: ObjectKey::from("key"),
            target: StreamUploadTarget::UploadPart {
                upload_id: UploadId::from("mpu-123"),
                part_number: 1,
            },
        })
        .unwrap();
        drop(pg);

        coord
            .append_stream_chunk("bucket", "key", "upload-part-session", 0, b"data")
            .unwrap();
    }

    // ── Phase 3a: Streaming UploadPart tests ─────────────────────────

    #[test]
    fn stream_part_happy_path() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // Create a multipart upload first.
        let mpu = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &MetadataBlob::new(), checksum: None })
            .unwrap();

        // Begin a streaming part session.
        let session = coord
            .begin_stream_part("bucket", "key", &mpu.upload_id, 1)
            .unwrap();
        let session_id = session.session_id;

        // Append chunks.
        let data = b"hello streaming part";
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, data)
            .unwrap();

        // Finalize.
        let crc = checksum::crc64::checksum(data);
        let result = coord
            .finalize_stream_part(FinalizeStreamPartRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                upload_id: &mpu.upload_id,
                part_number: 1,
                crc64: crc,
                total_size: data.len() as u64,
                claimed_checksum: None,
                computed_checksum: None,
            })
            .unwrap();
        assert!(!result.etag.is_empty());
    }

    #[test]
    fn stream_part_no_upload_rejected() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let err = coord
            .begin_stream_part("bucket", "key", "nonexistent", 1)
            .unwrap_err();
        assert!(matches!(err, ServerError::NoSuchUpload { .. }));
    }

    #[test]
    fn stream_part_invalid_part_number_rejected() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let mpu = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &MetadataBlob::new(), checksum: None })
            .unwrap();

        // Part 0 is invalid.
        let err = coord
            .begin_stream_part("bucket", "key", &mpu.upload_id, 0)
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidArgument { .. }));

        // Part 10001 is invalid.
        let err = coord
            .begin_stream_part("bucket", "key", &mpu.upload_id, 10_001)
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidArgument { .. }));
    }

    #[test]
    fn checksum_claim_invalid_base64_rejected() {
        // P2: Malformed base64 in claimed checksum must return an error,
        // not silently accept a None checksum.
        let err = ChecksumClaim::from_base64(
            storage::ChecksumAlgorithm::Crc32,
            "not-valid-base64!!!",
        )
        .unwrap_err();
        assert!(
            matches!(err, ServerError::InvalidRequest { .. }),
            "expected InvalidRequest for bad base64, got {err:?}"
        );
    }

    #[test]
    fn checksum_claim_wrong_length_rejected() {
        // A valid base64 string with the wrong byte length for the algorithm.
        use base64::Engine;
        let too_long = base64::engine::general_purpose::STANDARD.encode([0u8; 8]); // CRC32 expects 4
        let err = ChecksumClaim::from_base64(
            storage::ChecksumAlgorithm::Crc32,
            &too_long,
        )
        .unwrap_err();
        assert!(
            matches!(err, ServerError::InvalidRequest { .. }),
            "expected InvalidRequest for wrong length, got {err:?}"
        );
    }

    #[test]
    fn finalize_stream_part_wrong_op_kind_rejected() {
        // A PutObject session cannot be finalized as UploadPart.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"data")
            .unwrap();

        let mpu = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &MetadataBlob::new(), checksum: None })
            .unwrap();

        let err = coord
            .finalize_stream_part(FinalizeStreamPartRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                upload_id: &mpu.upload_id,
                part_number: 1,
                crc64: checksum::crc64::checksum(b"data"),
                total_size: 4,
                claimed_checksum: None,
                computed_checksum: None,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    #[test]
    fn concurrent_streamed_mpu_isolation_on_unversioned_key() {
        // Regression: two streamed MPUs on the same unversioned key must not
        // corrupt each other's chunk data. Upload A completes first; upload B
        // completes second (overwriting A). Each must read back its own data.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();

        // Create two MPUs for the same key.
        let mpu_a = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();
        let mpu_b = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        // Helper: stream a single part with given data.
        let stream_part = |upload_id: &str, data: &[u8]| -> CompletePart {
            let sess = coord
                .begin_stream_part("bucket", "key", upload_id, 1)
                .unwrap()
                .session_id;
            coord
                .append_stream_chunk("bucket", "key", &sess, 0, data)
                .unwrap();
            let crc = checksum::crc64::checksum(data);
            let result = coord
                .finalize_stream_part(FinalizeStreamPartRequest {
                    bucket: "bucket",
                    key: "key",
                    session_id: &sess,
                    upload_id,
                    part_number: 1,
                    crc64: crc,
                    total_size: data.len() as u64,
                    claimed_checksum: None,
                    computed_checksum: None,
                })
                .unwrap();
            CompletePart {
                part_number: 1,
                etag: result.etag,
                checksum: None,
            }
        };

        let data_a = b"AAAA-data-for-upload-A";
        let data_b = b"BBBB-data-for-upload-B";

        // Both uploads stage their parts concurrently (interleaved).
        let part_a = stream_part(&mpu_a.upload_id, data_a);
        let part_b = stream_part(&mpu_b.upload_id, data_b);

        // Complete A first.
        let result_a = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &mpu_a.upload_id, parts: &[part_a], claimed_checksum: None })
            .unwrap();

        // Read back A's data — should be A's content.
        let obj_a = coord
            .get_object_part(&GetObjectPartRequest { bucket: "bucket", key: "key", version_id: None, part_number: 1, cond: &ReadCondition::default() })
            .unwrap();
        assert_eq!(
            obj_a.data, data_a,
            "after completing A, reading part 1 should return A's data"
        );

        // Complete B — overwrites A on unversioned bucket.
        let result_b = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &mpu_b.upload_id, parts: &[part_b], claimed_checksum: None })
            .unwrap();

        // Read back B's data — should be B's content, not A's.
        let obj_b = coord
            .get_object_part(&GetObjectPartRequest { bucket: "bucket", key: "key", version_id: None, part_number: 1, cond: &ReadCondition::default() })
            .unwrap();
        assert_eq!(
            obj_b.data, data_b,
            "after completing B, reading part 1 should return B's data"
        );

        // Sanity: version IDs should both be 0 (unversioned).
        assert_eq!(result_a.version_id, storage::VersionId::Null);
        assert_eq!(result_b.version_id, storage::VersionId::Null);
    }

    #[test]
    fn abort_streamed_mpu_cleans_chunk_manifest_and_shards() {
        // Regression: aborting an MPU with streamed parts must delete
        // multipart_part_chunks rows and their shard data.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let mpu = coord
            .create_multipart_upload(&CreateMultipartUploadRequest { bucket: "bucket", key: "key", metadata: &metadata, checksum: None })
            .unwrap();

        // Upload a streaming part.
        let sess = coord
            .begin_stream_part("bucket", "key", &mpu.upload_id, 1)
            .unwrap()
            .session_id;
        let data = b"streamed-part-data-for-abort-test";
        coord
            .append_stream_chunk("bucket", "key", &sess, 0, data)
            .unwrap();
        let crc = checksum::crc64::checksum(data);
        coord
            .finalize_stream_part(FinalizeStreamPartRequest {
                bucket: "bucket",
                key: "key",
                session_id: &sess,
                upload_id: &mpu.upload_id,
                part_number: 1,
                crc64: crc,
                total_size: data.len() as u64,
                claimed_checksum: None,
                computed_checksum: None,
            })
            .unwrap();

        // Capture chunk records before abort for shard verification.
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let chunks_before = {
            let meta_pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let chunks = meta_pg
                .get_all_multipart_part_chunks_for_upload(&mpu.upload_id)
                .unwrap();
            assert!(!chunks.is_empty(), "chunks should exist before abort");
            chunks
        };

        // Verify shard data exists before abort.
        for chunk in &chunks_before {
            let shard_pg = coord.storage_node.get_pg(chunk.shard_pg_id).unwrap();
            let total = chunk.ec_k as usize + chunk.ec_m as usize;
            for i in 0..total {
                let shard_key = ShardKey::new(&chunk.chunk_okh, chunk.chunk_vid, i as u8);
                assert!(
                    shard_pg.read_shard(&shard_key).is_ok(),
                    "shard {i} should exist before abort"
                );
            }
        }

        // Abort the MPU.
        coord
            .abort_multipart_upload(&AbortMultipartUploadRequest { bucket: "bucket", key: "key", upload_id: &mpu.upload_id })
            .unwrap();

        // Verify chunk manifest rows are gone.
        {
            let meta_pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let chunks = meta_pg
                .get_all_multipart_part_chunks_for_upload(&mpu.upload_id)
                .unwrap();
            assert!(
                chunks.is_empty(),
                "chunk manifest rows should be deleted after abort"
            );
        }

        // Verify shard data is gone.
        for chunk in &chunks_before {
            let shard_pg = coord.storage_node.get_pg(chunk.shard_pg_id).unwrap();
            let total = chunk.ec_k as usize + chunk.ec_m as usize;
            for i in 0..total {
                let shard_key = ShardKey::new(&chunk.chunk_okh, chunk.chunk_vid, i as u8);
                assert!(
                    shard_pg.read_shard(&shard_key).is_err(),
                    "shard {i} should be deleted after abort"
                );
            }
        }
    }

    // ── Phase 5: Cleanup hardening tests ────────────────────────────

    #[test]
    fn scavenge_stale_sessions_cleans_old() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // Begin a session (creates with current timestamp).
        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"data")
            .unwrap();

        // Scavenge with a very large max_age so all sessions created "now" are stale.
        // We pass max_age = u64::MAX which makes cutoff = now.saturating_sub(MAX) = 0,
        // meaning all sessions with created_at > 0 would NOT be stale. Instead, use
        // a generous window: any session older than 1ms is stale.
        std::thread::sleep(std::time::Duration::from_millis(5));
        let count = coord.scavenge_stale_sessions(1);
        assert_eq!(count, 1);

        // Session should be gone — appending should fail.
        let err = coord
            .append_stream_chunk("bucket", "key", &session_id, 1, b"more")
            .unwrap_err();
        assert!(
            matches!(
                err,
                ServerError::Metadata(storage::MetadataError::StreamSessionNotFound { .. })
            ),
            "expected session not found after scavenge, got {err:?}"
        );
    }

    #[test]
    fn scavenge_does_not_affect_committed_objects() {
        // A committed (finalized) session should have no staging rows, so
        // scavenge should not affect the object or its chunk manifest.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &session_id, 0, b"safe-data")
            .unwrap();
        let crc = checksum::crc64::checksum(b"safe-data");
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: crc,
                total_size: 9,
                metadata_blob: &MetadataBlob::new(),
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Scavenge with max_age=0 — should find nothing to clean.
        let count = coord.scavenge_stale_sessions(0);
        assert_eq!(count, 0);

        // Object should still be readable.
        let result = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(result.data, b"safe-data");
    }

    #[test]
    fn object_not_visible_before_finalize() {
        // Atomic visibility: object is not readable before finalize.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "new-key").unwrap();
        coord
            .append_stream_chunk("bucket", "new-key", &session_id, 0, b"pending")
            .unwrap();

        // Key should not exist yet.
        let err = coord
            .get_object(&GetObjectRequest { bucket: "bucket", key: "new-key", version_id: None, cond: NO_READ })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));

        // HEAD should also fail.
        let err = coord
            .head_object(&GetObjectRequest { bucket: "bucket", key: "new-key", version_id: None, cond: NO_READ })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn chunk_manifest_integrity_readback() {
        // Storage-level verification: committed chunk manifest rows match
        // what was written, and shard data is intact.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = coord.begin_stream_put("bucket", "verify").unwrap();
        coord
            .append_stream_chunk("bucket", "verify", &session_id, 0, b"chunk-0-")
            .unwrap();
        coord
            .append_stream_chunk("bucket", "verify", &session_id, 1, b"chunk-1-")
            .unwrap();
        let full = b"chunk-0-chunk-1-";
        let crc = checksum::crc64::checksum(full);
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "verify",
                session_id: &session_id,
                crc64: crc,
                total_size: 16,
                metadata_blob: &MetadataBlob::new(),
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Read back via storage layer directly.
        let meta_pg_id = coord.object_pg_id("bucket", "verify");
        {
            let meta_pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let record = meta_pg.get_object_meta("bucket", "verify").unwrap();
            let chunks = meta_pg
                .get_stream_object_chunks("bucket", "verify", record.version_id())
                .unwrap();

            assert_eq!(chunks.len(), 2);
            assert_eq!(chunks[0].chunk_index, 0);
            assert_eq!(chunks[0].size, 8);
            assert_eq!(chunks[1].chunk_index, 1);
            assert_eq!(chunks[1].size, 8);
        } // Drop PG lock before coordinator calls.

        // Verify full readback via coordinator.
        let result = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "verify", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(result.data, full);

        // Verify CRC matches.
        assert_eq!(checksum::crc64::checksum(&result.data), crc);
    }

    #[test]
    fn stream_put_delete_then_reput() {
        // Overwrite cycle: stream-put → delete → normal put → GET succeeds.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // 1. Stream-write.
        let session_id = coord.begin_stream_put("bucket", "cycle").unwrap();
        coord
            .append_stream_chunk("bucket", "cycle", &session_id, 0, b"v1")
            .unwrap();
        let crc = checksum::crc64::checksum(b"v1");
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "cycle",
                session_id: &session_id,
                crc64: crc,
                total_size: 2,
                metadata_blob: &MetadataBlob::new(),
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // 2. Delete.
        coord
            .delete_object(&DeleteObjectRequest { bucket: "bucket", key: "cycle", version_id: None, cond: &crate::conditional::DeleteCondition::default() })
            .unwrap();

        // 3. Normal put.
        coord
            .put_object(&PutObjectRequest { bucket: "bucket", key: "cycle", data: b"v2-normal", metadata: &MetadataBlob::new(), cond: &WriteCondition::default() })
            .unwrap();

        // 4. GET should return normal-put data, no chunk manifest interference.
        let result = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "cycle", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(result.data, b"v2-normal");
    }

    #[test]
    fn stream_put_overwrite_with_stream_put() {
        // Stream-write → stream-write overwrite: second write's chunks replace first.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // First stream-write.
        let s1 = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &s1, 0, b"old-data")
            .unwrap();
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &s1,
                crc64: checksum::crc64::checksum(b"old-data"),
                total_size: 8,
                metadata_blob: &MetadataBlob::new(),
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Second stream-write (overwrite).
        let s2 = coord.begin_stream_put("bucket", "key").unwrap();
        coord
            .append_stream_chunk("bucket", "key", &s2, 0, b"new-data")
            .unwrap();
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &s2,
                crc64: checksum::crc64::checksum(b"new-data"),
                total_size: 8,
                metadata_blob: &MetadataBlob::new(),
                cond: &WriteCondition::default(),
            })
            .unwrap();

        let result = coord.get_object(&GetObjectRequest { bucket: "bucket", key: "key", version_id: None, cond: NO_READ }).unwrap();
        assert_eq!(result.data, b"new-data");
    }
}
