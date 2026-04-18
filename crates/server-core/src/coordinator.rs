/// Coordinator: orchestrates S3 operations across EC, storage, and metadata layers.
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{
    Arc, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak,
};
use std::thread::JoinHandle;

use checksum::{
    ChecksumAlgorithm, ChecksumBytes, ChecksumType, MultipartChecksumConfig, RawChecksum,
};
use ec::{EcConfig, ErasureCodec};
use s3_types::{
    aws_account_id_from_principal, parse_account_regional_bucket_name, AccountIdentity, AclGrant,
    AclGrantee, AclGrants, AclPermission, BucketNamespace, BucketVersioningState, CanonicalUserId,
    LegalHoldStatus, ObjectLockDefaultRetention, ObjectLockMode, ObjectRetention, RetentionPeriod,
    StoredLegalHoldStatus, VersionId,
};
use storage::traits::{PgMetadataStore, ShardStore};
#[cfg(test)]
use storage::SimplePayloadReclaimRecord;
use storage::{
    BucketEncryptionConfig, BucketFastPathInfo, BucketInfo, BucketLifecycleConfiguration,
    BucketName, BucketObjectLockConfig, BucketOwnershipControls, BucketState, CommitMultipartReq,
    CommitStreamPutReq, CreateMultipartUploadReq, CreateStreamUploadReq, EcShape,
    EffectiveBucketEncryptionConfig, GenerationId, LifecycleDate, LifecycleExpiration,
    LifecycleRule, LifecycleRuleStatus, ListMultipartUploadsReq, ListObjectVersionsReq,
    ListObjectsReq, ListPartsReq, LiveObjectRecord, ManagedEncryptionAlgorithm,
    MultipartPartRecord, MultipartPartSegmentRecord, MultipartReclaimPartRecord,
    MultipartReclaimPartSegmentRecord, MultipartReclaimRecord, MultipartUploadRecord,
    ObjectEncryption, ObjectKey, ObjectLayout, ObjectLockState, ObjectPartRecord,
    ObjectSegmentRecord, ObjectSegmentsReclaimRecord, ObjectSegmentsReclaimSegmentRecord,
    OwnerIdentity, PublicAccessBlockConfig, PutDeleteMarkerReq, PutLiveObjectReq, PutObjectReq,
    ReclaimWorkItem, SerializedMetadataBlob, SerializedSystemMetadataBlob, SerializedTagSet,
    SessionId, ShardKey, SharedStorageNode, StoredObject, StreamUploadRecord,
    StreamUploadSegmentRecord, StreamUploadState, StreamUploadTarget, UploadId, UploadState,
    UPLOAD_ID_ALPHABET, UPLOAD_ID_LEN,
};

use self::authz_results::*;
pub use self::authz_types::{
    ActiveWriteEncryption, ActiveWriteEncryptionRef, AuthorizedPutObjectWrite,
};
use self::authz_types::{AuthorizedPutObjectWriteAcl, ValidatedBucket};
use self::internal_types::*;
#[cfg(test)]
use self::payload::encode_parity_scratch_len;
use self::request_support::authorization_policy_context_for_put_object_write_acl;
pub use self::request_types::*;
use self::request_types::{
    AuthorizedWriteTags, BucketCreateOutcome, BucketScopedAuthorizationRequest,
    BucketScopedRequest, ExpectedBucketOwnerRequest, PreparedPutCommit, PutCommitRequest,
};
pub use self::response_types::*;
use self::response_types::{DeleteMarkerLifecycleExpiration, NoncurrentLifecycleExpiration};
use self::runtime_support::*;
pub use crate::checksum_claim::{ChecksumClaim, EncodedChecksumClaim};
use crate::conditional::{
    check_copy_source_conditions, check_delete_conditions, check_read_conditions,
    check_write_conditions, DeleteCondition, ReadCondition, WriteCondition,
};
use crate::error::ServerError;
pub use storage::BucketObjectOwnership;

fn lock_mutex_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|err| err.into_inner())
}

#[cfg(test)]
fn trusted_bucket_name(name: impl Into<String>) -> BucketName {
    BucketName::try_from(name.into())
        .expect("coordinator must only construct BucketName from validated values")
}

#[cfg(test)]
fn trusted_object_key(key: impl Into<String>) -> ObjectKey {
    ObjectKey::try_from(key.into())
        .expect("coordinator must only construct ObjectKey from validated values")
}

#[cfg(test)]
fn trusted_upload_id(seed: &str) -> UploadId {
    let mut bytes = [b'.'; UPLOAD_ID_LEN];
    let mut encoded = String::with_capacity(seed.len() * 2);
    for byte in seed.bytes() {
        use std::fmt::Write;
        write!(encoded, "{byte:02x}").unwrap();
    }
    let take = encoded.len().min(UPLOAD_ID_LEN);
    bytes[..take].copy_from_slice(&encoded.as_bytes()[..take]);
    UploadId::try_from(String::from_utf8(bytes.to_vec()).unwrap())
        .expect("coordinator tests must use valid upload IDs")
}

#[cfg(test)]
fn trusted_session_id(seed: &str) -> SessionId {
    let mut bytes = [b'0'; storage::SESSION_ID_LEN];
    let mut encoded = String::with_capacity(seed.len() * 2);
    for byte in seed.bytes() {
        use std::fmt::Write;
        write!(encoded, "{byte:02x}").unwrap();
    }
    let take = encoded.len().min(storage::SESSION_ID_LEN);
    bytes[..take].copy_from_slice(&encoded.as_bytes()[..take]);
    SessionId::try_from(String::from_utf8(bytes.to_vec()).unwrap())
        .expect("coordinator tests must use valid session IDs")
}

fn parse_list_object_key(value: &str) -> Result<ObjectKey, ServerError> {
    ObjectKey::try_from(value).map_err(|error| ServerError::InvalidArgument {
        reason: error.to_string(),
    })
}

fn optional_list_object_key(value: Option<&str>) -> Result<Option<ObjectKey>, ServerError> {
    value
        .filter(|value| !value.is_empty())
        .map(parse_list_object_key)
        .transpose()
}

fn read_rwlock_unpoisoned<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|err| err.into_inner())
}

fn write_rwlock_unpoisoned<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|err| err.into_inner())
}
use crate::etag::{compute_multipart_etag, crc64_to_etag_bytes, etag_bytes_to_crc64, format_etag};
use crate::metadata_blob::MetadataBlob;
use crate::pg::{object_key_hash, part_key_hash, stream_segment_key_hash, PgTopology};
use crate::range::ByteRange;
use crate::sse::{
    decrypt_managed_encryption_checksum, decrypt_managed_encryption_segment,
    decrypt_sse_customer_checksum, decrypt_sse_customer_segment, prepare_managed_encryption_write,
    prepare_sse_customer_write, resume_managed_encryption_write, resume_sse_customer_write,
    validate_sse_customer_read, ManagedEncryptionWriteContext, SseCustomerRequest,
    SseCustomerResponseHeaders, SseCustomerSegmentScope, SseCustomerValidatorConfig,
    SseCustomerWriteContext, StaticManagedKeyProvider, SSE_C_SEGMENT_TAG_LEN,
};
use crate::system_metadata::SystemMetadata;

const TRACE_TARGET: &str = "server_core";
const COMPLETED_MULTIPART_UPLOADS_PER_BUCKET_LIMIT: usize = 10_000;

/// Maximum object size for single PUT or upload part (5 GiB, matches AWS S3).
pub const MAX_OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024;

/// Fixed internal segment size for newly committed segmented payloads.
pub const INTERNAL_SEGMENT_SIZE: usize = 8 * 1024 * 1024;

const LIFECYCLE_SWEEP_INTERVAL_MILLIS: u64 = 1000;

/// Hard cap on total records fetched across all PGs for a single list query.
/// Prevents unbounded memory when delimiter causes u32::MAX per-PG limits.
const MAX_LIST_RECORDS: usize = 100_000;
const S3_MAX_LIST_KEYS: u32 = 1_000;

#[derive(Debug, Clone)]
pub struct ReadChunk {
    data: Arc<SharedPayloadBuffer>,
    start: usize,
    end: usize,
}

#[derive(Debug)]
struct PayloadBufferPool {
    default_capacity: usize,
    max_cached: usize,
    cached: Mutex<Vec<Vec<u8>>>,
    #[cfg(test)]
    allocations: std::sync::atomic::AtomicUsize,
}

struct PooledPayloadBuffer {
    pool: Arc<PayloadBufferPool>,
    buf: Option<Vec<u8>>,
}

#[derive(Debug)]
struct SharedPayloadBuffer {
    pool: Option<Arc<PayloadBufferPool>>,
    buf: Vec<u8>,
}

struct EncodeScratchPool {
    scratch_len: usize,
    max_cached: usize,
    cached: Mutex<Vec<Vec<u8>>>,
    #[cfg(test)]
    allocations: std::sync::atomic::AtomicUsize,
}

struct EncodeScratch<'a> {
    pool: &'a EncodeScratchPool,
    buf: Option<Vec<u8>>,
}

#[derive(Clone)]
struct ReadRuntime {
    storage_node: Arc<SharedStorageNode>,
    ec_codec: Arc<ErasureCodec>,
    ec_config: EcConfig,
    pg_topology: PgTopology,
    payload_buffer_pool: Arc<PayloadBufferPool>,
    sse_c_validator: Option<SseCustomerValidatorConfig>,
    managed_key_provider: Option<StaticManagedKeyProvider>,
}

#[derive(Debug, Clone)]
struct SegmentPayloadRecord {
    segment_index: u32,
    size: u64,
    segment_crc64: Option<u64>,
    segment_okh: [u8; 16],
    segment_vid: GenerationId,
    shard_pg_id: u32,
    ec_k: u8,
    ec_m: u8,
    encryption: ObjectEncryption,
}

#[cfg_attr(not(feature = "deep-tracing"), allow(dead_code))]
#[derive(Debug, Clone)]
struct SegmentSliceRecord {
    payload: SegmentPayloadRecord,
    segment_index: usize,
    segment_object_offset_start: usize,
    segment_object_offset_end_exclusive: usize,
    start_offset: usize,
    end_offset: usize,
    part_number: Option<u32>,
    part_order: Option<usize>,
    part_object_offset_start: Option<usize>,
    part_object_offset_end_exclusive: Option<usize>,
}

struct WrittenShard {
    key: ShardKey,
    ack: storage::WriteAck,
}

#[derive(Debug, Clone)]
struct MultipartPartReadLayout {
    part_number: u32,
    part_order: usize,
    object_offset_start: usize,
    object_offset_end_exclusive: usize,
}

struct SegmentListReader {
    runtime: ReadRuntime,
    bucket: String,
    key: String,
    segments: Vec<SegmentSliceRecord>,
    next_segment_index: usize,
    loaded_segment: Option<(Arc<SharedPayloadBuffer>, usize, usize)>,
    sse_customer_request: Option<SseCustomerRequest>,
}

#[cfg_attr(not(feature = "deep-tracing"), allow(dead_code))]
#[derive(Debug, Clone)]
struct SnapshottedMultipartPartRange {
    layout: MultipartPartReadLayout,
    segments: Vec<SegmentSliceRecord>,
}

struct MultipartReader {
    runtime: ReadRuntime,
    bucket: String,
    key: String,
    parts: Vec<SnapshottedMultipartPartRange>,
    next_part_index: usize,
    current_part: Option<SegmentListReader>,
    sse_customer_request: Option<SseCustomerRequest>,
}

struct ReadObjectContext<'a> {
    runtime: ReadRuntime,
    bucket: &'a BucketName,
    key: &'a ObjectKey,
    generation_id: GenerationId,
    sse_customer_request: Option<SseCustomerRequest>,
}

// Boxing the segment reader would add heap traffic on the normal read path.
#[allow(clippy::large_enum_variant)]
enum ReadHandleInner {
    Segments(SegmentListReader),
    Multipart(Box<MultipartReader>),
    TestBuffered(Option<Vec<u8>>),
}

struct PayloadLease {
    runtime: ReadRuntime,
    bucket: BucketName,
    key: ObjectKey,
    generation_id: GenerationId,
}

/// Core-owned streaming object body.
pub struct ReadHandle {
    bucket: String,
    key: String,
    inner: ReadHandleInner,
    lease: Option<PayloadLease>,
    trace: Option<observability::TraceContext>,
    expected_size: usize,
    bytes_emitted: usize,
    expected_crc64: Option<u64>,
    crc64: checksum::crc64::Hasher,
}

impl std::fmt::Debug for ReadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadHandle")
            .field("bucket", &self.bucket)
            .field("key", &self.key)
            .field("has_lease", &self.lease.is_some())
            .field("expected_size", &self.expected_size)
            .field("bytes_emitted", &self.bytes_emitted)
            .field("expected_crc64", &self.expected_crc64)
            .finish_non_exhaustive()
    }
}

impl ReadHandle {
    fn segment_slices_for_range(
        segments: Vec<SegmentPayloadRecord>,
        start: usize,
        end: usize,
        object_offset_base: usize,
        part_layout: Option<&MultipartPartReadLayout>,
    ) -> Vec<SegmentSliceRecord> {
        let mut slices = Vec::new();
        let mut offset = 0usize;

        for (segment_index, payload) in segments.into_iter().enumerate() {
            let segment_end = offset + payload.size as usize;
            if offset > end {
                break;
            }
            if payload.size != 0 && segment_end > start {
                let start_offset = start.saturating_sub(offset);
                let end_offset = (end + 1).saturating_sub(offset).min(payload.size as usize);
                slices.push(SegmentSliceRecord {
                    payload,
                    segment_index,
                    segment_object_offset_start: object_offset_base + offset,
                    segment_object_offset_end_exclusive: object_offset_base + segment_end,
                    start_offset,
                    end_offset,
                    part_number: part_layout.map(|layout| layout.part_number),
                    part_order: part_layout.map(|layout| layout.part_order),
                    part_object_offset_start: part_layout.map(|layout| layout.object_offset_start),
                    part_object_offset_end_exclusive: part_layout
                        .map(|layout| layout.object_offset_end_exclusive),
                });
            }
            offset = segment_end;
        }

        slices
    }

    fn multipart_ranges_for_range(
        parts: Vec<SnapshottedMultipartPart>,
        start: usize,
        end: usize,
    ) -> Vec<SnapshottedMultipartPartRange> {
        let mut ranges = Vec::new();

        for (part_order, part) in parts.into_iter().enumerate() {
            let part_start = part.object_offset_start;
            let part_end = part_start + part.record.size as usize;
            if part_start > end {
                break;
            }
            if part.record.size != 0 && part_end > start {
                let start_offset = start.saturating_sub(part_start);
                let end_offset = (end + 1)
                    .saturating_sub(part_start)
                    .min(part.record.size as usize);
                let layout = MultipartPartReadLayout {
                    part_number: part.record.part_number,
                    part_order,
                    object_offset_start: part_start,
                    object_offset_end_exclusive: part_end,
                };
                ranges.push(SnapshottedMultipartPartRange {
                    layout: layout.clone(),
                    segments: Self::segment_slices_for_range(
                        part.segments,
                        start_offset,
                        end_offset - 1,
                        part_start,
                        Some(&layout),
                    ),
                });
            }
        }

        ranges
    }

    fn from_segments(
        ctx: ReadObjectContext<'_>,
        segments: Vec<SegmentPayloadRecord>,
        expected_size: usize,
        expected_crc64: Option<u64>,
    ) -> Self {
        let ReadObjectContext {
            runtime,
            bucket,
            key,
            generation_id,
            sse_customer_request,
        } = ctx;
        let bucket_owned = bucket.as_str().to_string();
        let key_owned = key.as_str().to_string();
        Self {
            bucket: bucket_owned.clone(),
            key: key_owned.clone(),
            lease: Some(runtime.acquire_object_payload_lease_for(bucket, key, generation_id)),
            trace: observability::current_context(),
            expected_size,
            bytes_emitted: 0,
            expected_crc64,
            crc64: checksum::crc64::Hasher::new(),
            inner: ReadHandleInner::Segments(SegmentListReader {
                runtime,
                bucket: bucket_owned,
                key: key_owned,
                segments: Self::segment_slices_for_range(
                    segments,
                    0,
                    expected_size.saturating_sub(1),
                    0,
                    None,
                ),
                next_segment_index: 0,
                loaded_segment: None,
                sse_customer_request,
            }),
        }
    }

    fn from_segments_range(
        ctx: ReadObjectContext<'_>,
        segments: Vec<SegmentPayloadRecord>,
        start: usize,
        end: usize,
    ) -> Self {
        let ReadObjectContext {
            runtime,
            bucket,
            key,
            generation_id,
            sse_customer_request,
        } = ctx;
        let expected_size = end - start + 1;
        let bucket_owned = bucket.as_str().to_string();
        let key_owned = key.as_str().to_string();
        Self {
            bucket: bucket_owned.clone(),
            key: key_owned.clone(),
            lease: Some(runtime.acquire_object_payload_lease_for(bucket, key, generation_id)),
            trace: observability::current_context(),
            expected_size,
            bytes_emitted: 0,
            expected_crc64: None,
            crc64: checksum::crc64::Hasher::new(),
            inner: ReadHandleInner::Segments(SegmentListReader {
                runtime,
                bucket: bucket_owned,
                key: key_owned,
                segments: Self::segment_slices_for_range(segments, start, end, 0, None),
                next_segment_index: 0,
                loaded_segment: None,
                sse_customer_request,
            }),
        }
    }

    fn from_multipart(
        runtime: ReadRuntime,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        parts: Vec<SnapshottedMultipartPart>,
        expected_size: usize,
        sse_customer_request: Option<SseCustomerRequest>,
    ) -> Self {
        Self {
            bucket: bucket.as_str().to_string(),
            key: key.as_str().to_string(),
            lease: Some(runtime.acquire_object_payload_lease_for(bucket, key, generation_id)),
            trace: observability::current_context(),
            expected_size,
            bytes_emitted: 0,
            expected_crc64: None,
            crc64: checksum::crc64::Hasher::new(),
            inner: ReadHandleInner::Multipart(Box::new(MultipartReader {
                runtime,
                bucket: bucket.as_str().to_string(),
                key: key.as_str().to_string(),
                parts: Self::multipart_ranges_for_range(parts, 0, expected_size.saturating_sub(1)),
                next_part_index: 0,
                current_part: None,
                sse_customer_request,
            })),
        }
    }

    fn from_multipart_range(
        runtime: ReadRuntime,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        parts: Vec<SnapshottedMultipartPart>,
        range: (usize, usize),
        sse_customer_request: Option<SseCustomerRequest>,
    ) -> Self {
        let (start, end) = range;
        let expected_size = end - start + 1;
        Self {
            bucket: bucket.as_str().to_string(),
            key: key.as_str().to_string(),
            lease: Some(runtime.acquire_object_payload_lease_for(bucket, key, generation_id)),
            trace: observability::current_context(),
            expected_size,
            bytes_emitted: 0,
            expected_crc64: None,
            crc64: checksum::crc64::Hasher::new(),
            inner: ReadHandleInner::Multipart(Box::new(MultipartReader {
                runtime,
                bucket: bucket.as_str().to_string(),
                key: key.as_str().to_string(),
                parts: Self::multipart_ranges_for_range(parts, start, end),
                next_part_index: 0,
                current_part: None,
                sse_customer_request,
            })),
        }
    }

    pub fn from_buffered_bytes(data: Vec<u8>) -> Self {
        let len = data.len();
        Self {
            bucket: "<buffered>".to_string(),
            key: "<buffered>".to_string(),
            inner: ReadHandleInner::TestBuffered(Some(data)),
            lease: None,
            trace: observability::current_context(),
            expected_size: len,
            bytes_emitted: 0,
            expected_crc64: None,
            crc64: checksum::crc64::Hasher::new(),
        }
    }

    pub fn next_chunk(&mut self, target_size: usize) -> Result<Option<ReadChunk>, ServerError> {
        let _trace = self.trace.clone().map(observability::AttachedTrace::new);
        observability::trace_scope!(
            TRACE_TARGET,
            "ReadHandle::next_chunk",
            "bucket={:?} key={:?} target_size={}",
            self.bucket,
            self.key,
            target_size
        );
        if target_size == 0 {
            return Ok(Some(ReadChunk::from_vec(Vec::new())));
        }

        let next = match &mut self.inner {
            ReadHandleInner::Segments(reader) => reader.next_chunk(target_size),
            ReadHandleInner::Multipart(reader) => reader.next_chunk(target_size),
            ReadHandleInner::TestBuffered(data) => Ok(data.take().map(ReadChunk::from_vec)),
        }?;

        if let Some(chunk) = next {
            self.bytes_emitted += chunk.len();
            self.crc64.update(chunk.as_ref());
            Ok(Some(chunk))
        } else {
            if self.bytes_emitted != self.expected_size {
                return Err(ServerError::IntegrityError {
                    bucket: self.bucket.clone(),
                    key: self.key.clone(),
                    expected: self.expected_size as u64,
                    actual: self.bytes_emitted as u64,
                });
            }
            if let Some(expected_crc64) = self.expected_crc64 {
                let actual_crc64 = self.crc64.finalize();
                if actual_crc64 != expected_crc64 {
                    return Err(ServerError::IntegrityError {
                        bucket: self.bucket.clone(),
                        key: self.key.clone(),
                        expected: expected_crc64,
                        actual: actual_crc64,
                    });
                }
            }
            Ok(None)
        }
    }
}

fn segment_payloads_from_object_segments(
    segments: Vec<ObjectSegmentRecord>,
    encryption: ObjectEncryption,
) -> Vec<SegmentPayloadRecord> {
    segments
        .into_iter()
        .map(|segment| SegmentPayloadRecord {
            segment_index: segment.segment_index,
            size: segment.size,
            segment_crc64: segment.segment_crc64,
            segment_okh: segment.segment_okh,
            segment_vid: segment.segment_vid,
            shard_pg_id: segment.shard_pg_id,
            ec_k: segment.ec_k,
            ec_m: segment.ec_m,
            encryption: encryption.clone(),
        })
        .collect()
}

/// Minimum part size for non-final parts (5 MiB).
const MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

/// Maximum number of parts in a multipart upload (matches AWS S3).
const MAX_PARTS: usize = 10_000;

pub struct Coordinator {
    storage_node: Arc<SharedStorageNode>,
    bucket_policy_cache: RwLock<HashMap<BucketName, CachedBucketPolicy>>,
    bucket_lifecycle_cache: RwLock<HashMap<BucketName, CachedBucketLifecycle>>,
    pg_topology: PgTopology,
    ec_codec: Arc<ErasureCodec>,
    ec_config: EcConfig,
    encode_scratch_pool: EncodeScratchPool,
    payload_buffer_pool: Arc<PayloadBufferPool>,
    region: String,
    sse_c_validator: Option<SseCustomerValidatorConfig>,
    managed_key_provider: Option<StaticManagedKeyProvider>,
    _reclaim_sweeper: ReclaimSweeper,
    _lifecycle_sweeper: Arc<LifecycleSweeper>,
}

impl Coordinator {
    fn now_millis() -> u64 {
        storage::clock::current_time_millis()
    }
}

/// Compute an inline checksum value for the given algorithm and data.
fn compute_checksum(algo: ChecksumAlgorithm, data: &[u8]) -> RawChecksum {
    match algo {
        ChecksumAlgorithm::Crc32 => {
            RawChecksum::new(algo, checksum::crc32::checksum(data).to_be_bytes())
        }
        ChecksumAlgorithm::Crc32c => {
            RawChecksum::new(algo, checksum::crc32c::checksum(data).to_be_bytes())
        }
        ChecksumAlgorithm::Crc64nvme => {
            RawChecksum::new(algo, checksum::crc64::checksum(data).to_be_bytes())
        }
        ChecksumAlgorithm::Sha256 => RawChecksum::new(
            algo,
            ring::digest::digest(&ring::digest::SHA256, data).as_ref(),
        ),
        ChecksumAlgorithm::Sha1 => RawChecksum::new(
            algo,
            ring::digest::digest(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY, data).as_ref(),
        ),
    }
    .expect("checksum helper produces bytes matching the requested algorithm")
}

enum StreamingChecksumAccumulator {
    Crc32(checksum::crc32::Hasher),
    Crc32c(checksum::crc32c::Hasher),
    Crc64(checksum::crc64::Hasher),
    Sha1(ring::digest::Context),
    Sha256(ring::digest::Context),
}

impl StreamingChecksumAccumulator {
    fn new(algo: ChecksumAlgorithm) -> Self {
        match algo {
            ChecksumAlgorithm::Crc32 => Self::Crc32(checksum::crc32::Hasher::new()),
            ChecksumAlgorithm::Crc32c => Self::Crc32c(checksum::crc32c::Hasher::new()),
            ChecksumAlgorithm::Crc64nvme => Self::Crc64(checksum::crc64::Hasher::new()),
            ChecksumAlgorithm::Sha1 => Self::Sha1(ring::digest::Context::new(
                &ring::digest::SHA1_FOR_LEGACY_USE_ONLY,
            )),
            ChecksumAlgorithm::Sha256 => {
                Self::Sha256(ring::digest::Context::new(&ring::digest::SHA256))
            }
        }
    }

    fn algorithm(&self) -> ChecksumAlgorithm {
        match self {
            Self::Crc32(_) => ChecksumAlgorithm::Crc32,
            Self::Crc32c(_) => ChecksumAlgorithm::Crc32c,
            Self::Crc64(_) => ChecksumAlgorithm::Crc64nvme,
            Self::Sha1(_) => ChecksumAlgorithm::Sha1,
            Self::Sha256(_) => ChecksumAlgorithm::Sha256,
        }
    }

    fn update(&mut self, data: &[u8]) {
        match self {
            Self::Crc32(hasher) => hasher.update(data),
            Self::Crc32c(hasher) => hasher.update(data),
            Self::Crc64(hasher) => hasher.update(data),
            Self::Sha1(hasher) => hasher.update(data),
            Self::Sha256(hasher) => hasher.update(data),
        }
    }

    fn finalize(self) -> RawChecksum {
        match self {
            Self::Crc32(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Crc32, hasher.finalize().to_be_bytes())
            }
            Self::Crc32c(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Crc32c, hasher.finalize().to_be_bytes())
            }
            Self::Crc64(hasher) => RawChecksum::new(
                ChecksumAlgorithm::Crc64nvme,
                hasher.finalize().to_be_bytes(),
            ),
            Self::Sha1(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Sha1, hasher.finish().as_ref())
            }
            Self::Sha256(hasher) => {
                RawChecksum::new(ChecksumAlgorithm::Sha256, hasher.finish().as_ref())
            }
        }
        .expect("streaming checksum accumulator produces bytes matching the algorithm")
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub mod test_helpers;

#[cfg(test)]
mod access_control_tests;
mod authz;
#[cfg(test)]
mod authz_model_tests;
mod authz_results;
mod authz_types;
mod bucket;
#[cfg(test)]
mod bucket_tests;
mod copy;
mod delete;
mod infra;
mod internal_types;
mod lifecycle;
mod listing;
mod multipart;
#[cfg(test)]
mod multipart_reclaim_trace_tests;
#[cfg(test)]
mod multipart_stateful_tests;
#[cfg(test)]
mod multipart_tests;
#[cfg(test)]
mod multipart_trace_tests;
mod object_metadata;
mod object_state;
#[cfg(test)]
mod object_state_tests;
mod payload;
mod put;
mod read;
#[cfg(test)]
mod read_tests;
mod request_support;
mod request_types;
mod response_types;
mod runtime;
mod runtime_support;
mod streaming;
#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests {
    use super::test_helpers::{self, UploadPartRequest};
    use super::test_support::*;
    use super::*;
    use crate::conditional::{DeleteCondition, SpecificEtag, WriteCondition};
    use crate::sse::SSE_CUSTOMER_ALGORITHM;
    use std::panic::AssertUnwindSafe;
    use std::path::{Path, PathBuf};
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn lock_mutex_unpoisoned_recovers_after_panic() {
        let lock = Mutex::new(vec![1usize]);
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = lock.lock().unwrap();
            panic!("poison mutex");
        }));

        lock_mutex_unpoisoned(&lock).push(2);
        assert_eq!(*lock_mutex_unpoisoned(&lock), vec![1, 2]);
    }

    #[test]
    fn rwlock_helpers_recover_after_panic() {
        let lock = RwLock::new(HashMap::from([("bucket".to_string(), 1usize)]));
        let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let mut guard = lock.write().unwrap();
            guard.insert("poisoned".to_string(), 2);
            panic!("poison rwlock");
        }));

        write_rwlock_unpoisoned(&lock).insert("ok".to_string(), 3);
        let guard = read_rwlock_unpoisoned(&lock);
        assert_eq!(guard.get("bucket"), Some(&1));
        assert_eq!(guard.get("poisoned"), Some(&2));
        assert_eq!(guard.get("ok"), Some(&3));
    }

    #[test]
    fn put_object_effective_policy_context_derives_explicit_sse_s3() {
        let metadata = MetadataBlob::default();
        let system_metadata = SystemMetadata::default();
        let request = PutObjectRequest {
            object: object_request("bucket", "key", test_requester()),
            data: b"body",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: PutObjectWriteAcl::None,
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            encryption: WriteEncryptionRequest::managed(ManagedEncryptionAlgorithm::Aes256),
        };

        assert_eq!(
            request
                .effective_policy_context()
                .unwrap()
                .managed_encryption,
            Some(ManagedEncryptionAlgorithm::Aes256)
        );
    }

    #[test]
    fn put_object_effective_policy_context_overrides_conflicting_encryption_fields() {
        let metadata = MetadataBlob::default();
        let system_metadata = SystemMetadata::default();
        let sse_customer = test_sse_customer_request();
        let request = PutObjectRequest {
            object: object_request("bucket", "key", test_requester()),
            data: b"body",
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            cond: NO_WRITE,
            acl: PutObjectWriteAcl::None,
            policy_context: PutObjectPolicyContext::default()
                .with_managed_encryption(Some(ManagedEncryptionAlgorithm::Aes256)),
            object_lock: ObjectLockState::default(),
            encryption: WriteEncryptionRequest::sse_customer(&sse_customer),
        };

        let policy_context = request.effective_policy_context().unwrap();
        assert_eq!(policy_context.managed_encryption, None);
        assert_eq!(
            policy_context.sse_customer_algorithm,
            Some(SSE_CUSTOMER_ALGORITHM)
        );
    }

    #[test]
    fn create_multipart_effective_policy_context_derives_explicit_sse_s3() {
        let metadata = MetadataBlob::default();
        let system_metadata = SystemMetadata::default();
        let request = CreateMultipartUploadRequest {
            object: object_request("bucket", "key", test_requester()),
            metadata: &metadata,
            system_metadata: &system_metadata,
            tags: None,
            checksum: None,
            acl: PutObjectWriteAcl::None,
            policy_context: PutObjectPolicyContext::default(),
            object_lock: ObjectLockState::default(),
            encryption: WriteEncryptionRequest::managed(ManagedEncryptionAlgorithm::Aes256),
        };

        assert_eq!(
            request
                .effective_policy_context()
                .unwrap()
                .managed_encryption,
            Some(ManagedEncryptionAlgorithm::Aes256)
        );
    }

    #[test]
    fn begin_stream_put_effective_policy_context_uses_request_encryption() {
        let sse_customer = test_sse_customer_request();
        let cleared = WriteEncryptionRequest::none().with_policy_context(
            PutObjectPolicyContext::default()
                .with_managed_encryption(Some(ManagedEncryptionAlgorithm::Aes256))
                .with_sse_customer_algorithm(Some("AES256"))
                .with_default_canned_acl(PutObjectWriteAcl::None.policy_condition_value()),
        );
        assert_eq!(cleared.managed_encryption, None);
        assert_eq!(cleared.sse_customer_algorithm, None);

        let sse_c = WriteEncryptionRequest::sse_customer(&sse_customer).with_policy_context(
            PutObjectPolicyContext::default()
                .with_default_canned_acl(PutObjectWriteAcl::None.policy_condition_value()),
        );
        assert_eq!(sse_c.managed_encryption, None);
        assert_eq!(sse_c.sse_customer_algorithm, Some(SSE_CUSTOMER_ALGORITHM));
    }

    fn wait_until_bucket_gone(coord: &Coordinator, name: &str) {
        for _ in 0..200 {
            if matches!(
                coord.unchecked_active_bucket_summary(name),
                Err(ServerError::BucketNotFound { .. })
            ) {
                let bucket_pg = coord.get_bucket_pg(name).unwrap();
                if bucket_pg
                    .head_bucket_raw(&trusted_bucket_name(name))
                    .is_err()
                {
                    return;
                }
            }
            thread::sleep(Duration::from_millis(5));
        }
        panic!("bucket {name} was not fully removed");
    }

    fn find_key_with_object_pg_ne_bucket_pg(
        coord: &Coordinator,
        bucket: &str,
        prefix: &str,
    ) -> String {
        let bucket_pg_id = coord.bucket_pg_id(bucket);
        for suffix in 0..1024 {
            let key = format!("{prefix}-{suffix}");
            if coord.object_pg_id(bucket, &key) != bucket_pg_id {
                return key;
            }
        }
        panic!("failed to find a key with object_pg_id != bucket_pg_id");
    }

    #[test]
    fn put_object_persists_explicit_object_owner_identity() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        let owner_canonical_id = CanonicalUserId::from_principal("custom-object-owner");
        let owner = AccountIdentity::new("owner-a", owner_canonical_id.clone(), "Owner A");
        let requester = Requester::authenticated(owner.clone());

        coord
            .create_bucket(&CreateBucketRequest {
                name: trusted_bucket_name("bucket"),
                requester: requester.clone(),
                namespace: BucketNamespace::Global,
                acl: CreateBucketAcl::DefaultPrivate,
                ownership: BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();

        coord
            .put_object(&PutObjectRequest {
                object: object_request_with_expected_owner(
                    "bucket",
                    "key",
                    requester.clone(),
                    None,
                ),
                data: b"hello",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
            })
            .unwrap();

        let meta_pg = coord
            .storage_node
            .get_pg(coord.object_pg_id("bucket", "key"))
            .unwrap();
        let live = meta_pg
            .get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
            .unwrap()
            .into_live()
            .expect("expected live object");
        assert_eq!(live.owner.principal, owner.principal());
        assert_eq!(live.owner.canonical_id, owner_canonical_id);
    }

    #[test]
    fn delete_marker_persists_explicit_owner_identity() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        let owner_canonical_id = CanonicalUserId::from_principal("custom-delete-owner");
        let owner = AccountIdentity::new("owner-a", owner_canonical_id.clone(), "Owner A");
        let requester = Requester::authenticated(owner.clone());

        coord
            .create_bucket(&CreateBucketRequest {
                name: trusted_bucket_name("bucket"),
                requester: requester.clone(),
                namespace: BucketNamespace::Global,
                acl: CreateBucketAcl::DefaultPrivate,
                ownership: BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();
        put_bucket_versioning_test(
            &coord,
            "bucket",
            BucketVersioningState::Enabled,
            requester.clone(),
            None,
        )
        .unwrap();
        coord
            .put_object(&PutObjectRequest {
                object: object_request_with_expected_owner(
                    "bucket",
                    "key",
                    requester.clone(),
                    None,
                ),
                data: b"hello",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
            })
            .unwrap();

        coord
            .delete_object(&delete_object_request(
                "bucket",
                "key",
                None,
                requester.clone(),
                false,
                NO_DELETE,
            ))
            .unwrap();

        let meta_pg = coord
            .storage_node
            .get_pg(coord.object_pg_id("bucket", "key"))
            .unwrap();
        let marker = match meta_pg
            .get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
            .unwrap()
        {
            StoredObject::DeleteMarker(marker) => marker,
            other => panic!("expected delete marker, got {other:?}"),
        };
        assert_eq!(marker.owner.principal, owner.principal());
        assert_eq!(marker.owner.canonical_id, owner_canonical_id);
    }

    #[test]
    fn multipart_upload_and_complete_persist_explicit_owner_identity() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        let owner_canonical_id = CanonicalUserId::from_principal("custom-mpu-owner");
        let owner = AccountIdentity::new("owner-a", owner_canonical_id.clone(), "Owner A");
        let requester = Requester::authenticated(owner.clone());
        let expected_owner =
            OwnerIdentity::new(owner.principal().to_string(), owner_canonical_id.clone());

        coord
            .create_bucket(&CreateBucketRequest {
                name: trusted_bucket_name("bucket"),
                requester: requester.clone(),
                namespace: BucketNamespace::Global,
                acl: CreateBucketAcl::DefaultPrivate,
                ownership: BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();

        let upload = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                object: object_request_with_expected_owner(
                    "bucket",
                    "key",
                    requester.clone(),
                    None,
                ),
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                checksum: None,

                acl: NO_PUT_OBJECT_ACL.into(),
                encryption: WriteEncryptionRequest::none(),
                object_lock: ObjectLockState::default(),
                policy_context: PutObjectPolicyContext::default(),
            })
            .unwrap();

        let meta_pg = coord
            .storage_node
            .get_pg(coord.object_pg_id("bucket", "key"))
            .unwrap();
        let upload_record = meta_pg.get_multipart_upload(&upload.upload_id).unwrap();
        assert_eq!(upload_record.initiator, Some(expected_owner.clone()));
        assert_eq!(upload_record.owner, expected_owner);
        drop(meta_pg);

        test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                upload: multipart_object_request_with_expected_owner(
                    "bucket",
                    "key",
                    &upload.upload_id,
                    requester.clone(),
                    None,
                ),
                part_number: 1,
                data: b"multipart-data",
                claimed_checksum: None,

                sse_customer: None,
            },
        )
        .unwrap();

        coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                upload: multipart_object_request_with_expected_owner(
                    "bucket",
                    "key",
                    &upload.upload_id,
                    requester.clone(),
                    None,
                ),
                parts: &[CompletePart {
                    part_number: 1,
                    etag: format_etag(checksum::crc64::checksum(b"multipart-data")),
                    checksum: None,
                }],
                claimed_checksum: None,
                expected_object_size: None,
                cond: &WriteCondition::default(),
                sse_customer: None,
            })
            .unwrap();

        let meta_pg = coord
            .storage_node
            .get_pg(coord.object_pg_id("bucket", "key"))
            .unwrap();
        let live = meta_pg
            .get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
            .unwrap()
            .into_live()
            .expect("expected completed object");
        assert_eq!(live.owner.principal, owner.principal());
        assert_eq!(live.owner.canonical_id, owner_canonical_id);
    }

    #[test]
    fn create_multipart_upload_bucket_owner_preferred_promotes_bucket_owner_with_full_control_acl()
    {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        let bucket_owner = AccountIdentity::new(
            "owner-a",
            CanonicalUserId::from_principal("bucket-owner-canonical"),
            "Bucket Owner",
        );
        let writer = AccountIdentity::new(
            "writer-a",
            CanonicalUserId::from_principal("writer-canonical"),
            "Writer",
        );

        coord
            .create_bucket(&CreateBucketRequest {
                name: trusted_bucket_name("bucket"),
                requester: Requester::authenticated(bucket_owner.clone()),
                namespace: BucketNamespace::Global,
                acl: CreateBucketAcl::DefaultPrivate,
                ownership: BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();
        put_bucket_canned_acl_test(
            &coord,
            "bucket",
            BucketAcl::PublicReadWrite,
            Requester::authenticated(bucket_owner.clone()),
            None,
        )
        .unwrap();
        put_bucket_ownership_controls_test(&coord,
                "bucket",
                "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerPreferred</ObjectOwnership></Rule></OwnershipControls>",
                Requester::authenticated(bucket_owner.clone()), None)
            .unwrap();

        let upload = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                object: object_request_with_expected_owner(
                    "bucket",
                    "key",
                    Requester::authenticated(writer.clone()),
                    None,
                ),
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                checksum: None,

                acl: PutObjectAcl::BucketOwnerFullControl.into(),
                encryption: WriteEncryptionRequest::none(),
                object_lock: ObjectLockState::default(),
                policy_context: PutObjectPolicyContext::default(),
            })
            .unwrap();

        let meta_pg = coord
            .storage_node
            .get_pg(coord.object_pg_id("bucket", "key"))
            .unwrap();
        let upload_record = meta_pg.get_multipart_upload(&upload.upload_id).unwrap();
        assert_eq!(
            upload_record.initiator,
            Some(OwnerIdentity::new(
                writer.principal().to_string(),
                writer.canonical_user_id().clone(),
            ))
        );
        assert_eq!(
            upload_record.owner,
            OwnerIdentity::new(
                bucket_owner.principal().to_string(),
                bucket_owner.canonical_user_id().clone(),
            )
        );
    }

    #[test]
    fn create_bucket_idempotent_create_does_not_overwrite_ownership_controls() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket(&CreateBucketRequest {
                name: trusted_bucket_name("bucket"),
                requester: test_helpers::requester("owner-a"),
                namespace: BucketNamespace::Global,
                acl: CreateBucketAcl::DefaultPrivate,
                ownership: BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();

        coord
            .create_bucket(&CreateBucketRequest {
                name: trusted_bucket_name("bucket"),
                requester: test_helpers::requester("owner-a"),
                namespace: BucketNamespace::Global,
                acl: CreateBucketAcl::DefaultPrivate,
                ownership: BucketObjectOwnership::BucketOwnerEnforced,
                object_lock_enabled: false,
            })
            .unwrap();

        let controls = get_bucket_ownership_controls_test(
            &coord,
            "bucket",
            test_helpers::requester("owner-a"),
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            controls,
            BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::ObjectWriter,
            }
        );
    }

    #[test]
    fn create_bucket_rejects_public_read_with_owner_enforced() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .create_bucket(&CreateBucketRequest {
                name: trusted_bucket_name("bucket"),
                requester: test_helpers::requester("owner-a"),
                namespace: BucketNamespace::Global,
                acl: CreateBucketAcl::Canned(BucketAcl::PublicRead),
                ownership: BucketObjectOwnership::BucketOwnerEnforced,
                object_lock_enabled: false,
            })
            .unwrap_err();
        assert!(matches!(
            err,
            ServerError::InvalidBucketAclWithObjectOwnership
        ));
    }

    #[test]
    fn create_bucket_rejects_public_read_with_object_writer() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .create_bucket(&CreateBucketRequest {
                name: trusted_bucket_name("bucket"),
                requester: test_helpers::requester("owner-a"),
                namespace: BucketNamespace::Global,
                acl: CreateBucketAcl::Canned(BucketAcl::PublicRead),
                ownership: BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap_err();
        assert!(matches!(
            err,
            ServerError::InvalidBucketAclWithBlockPublicAccessError
        ));
    }

    #[test]
    fn create_bucket_allows_default_private_with_owner_enforced() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket(&CreateBucketRequest {
                name: trusted_bucket_name("bucket"),
                requester: test_helpers::requester("owner-a"),
                namespace: BucketNamespace::Global,
                acl: CreateBucketAcl::DefaultPrivate,
                ownership: BucketObjectOwnership::BucketOwnerEnforced,
                object_lock_enabled: false,
            })
            .unwrap();

        let controls = get_bucket_ownership_controls_test(
            &coord,
            "bucket",
            test_helpers::requester("owner-a"),
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            controls,
            BucketOwnershipControls {
                object_ownership: BucketObjectOwnership::BucketOwnerEnforced,
            }
        );
    }

    #[test]
    fn get_bucket_acl_bucket_owner_enforced_allows_same_account_owner_view() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        let bucket_owner = AccountIdentity::new(
            "arn:aws:iam::111122223333:root",
            CanonicalUserId::from_principal("bucket-owner-acl-canonical"),
            "Bucket Owner",
        );
        let same_account_user = AccountIdentity::new(
            "arn:aws:iam::111122223333:user/reader",
            CanonicalUserId::from_principal("bucket-same-account-acl-canonical"),
            "Same Account Reader",
        );
        let owner_requester = Requester::authenticated(bucket_owner.clone());
        let same_account_requester =
            Requester::authenticated_owner_account_admin(same_account_user);

        create_bucket_for_owner_with_flags(
            &coord,
            bucket_owner.principal(),
            bucket_owner.canonical_user_id(),
            "bucket",
            false,
            false,
            false,
        )
        .unwrap();
        put_bucket_ownership_controls_test(
            &coord,
            "bucket",
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
            owner_requester.clone(),
            None,
        )
        .unwrap();

        let boe_acl = get_bucket_acl_test(&coord, "bucket", same_account_requester, None).unwrap();
        assert_eq!(
            boe_acl.owner_canonical_id,
            bucket_owner.canonical_user_id().clone()
        );
        assert_eq!(boe_acl.acl_grants.iter().count(), 1);
        assert!(boe_acl.acl_grants.iter().any(|grant| {
            grant
                == &AclGrant::new(
                    AclGrantee::CanonicalUser(bucket_owner.canonical_user_id().clone()),
                    AclPermission::FullControl,
                )
        }));
    }

    #[test]
    fn authorize_get_bucket_acl_bucket_owner_enforced_allows_same_account_owner_view() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        let bucket_owner = AccountIdentity::new(
            "arn:aws:iam::111122223333:root",
            CanonicalUserId::from_principal("bucket-owner-acl-canonical"),
            "Bucket Owner",
        );
        let same_account_user = AccountIdentity::new(
            "arn:aws:iam::111122223333:user/reader",
            CanonicalUserId::from_principal("bucket-same-account-acl-canonical"),
            "Same Account Reader",
        );
        let owner_requester = Requester::authenticated(bucket_owner.clone());
        let same_account_requester =
            Requester::authenticated_owner_account_admin(same_account_user);

        create_bucket_for_owner_with_flags(
            &coord,
            bucket_owner.principal(),
            bucket_owner.canonical_user_id(),
            "bucket",
            false,
            false,
            false,
        )
        .unwrap();
        put_bucket_ownership_controls_test(
            &coord,
            "bucket",
            "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
            owner_requester,
            None,
        )
        .unwrap();

        let authorized = coord
            .authorize_get_bucket_acl(&bucket_request_with_expected_owner(
                "bucket",
                same_account_requester,
                None,
            ))
            .unwrap();
        assert_eq!(
            authorized.result.owner_canonical_id,
            bucket_owner.canonical_user_id().clone()
        );
        assert_eq!(authorized.result.acl_grants.iter().count(), 1);
        assert!(authorized.result.acl_grants.iter().any(|grant| {
            grant
                == &AclGrant::new(
                    AclGrantee::CanonicalUser(bucket_owner.canonical_user_id().clone()),
                    AclPermission::FullControl,
                )
        }));
    }

    #[test]
    fn create_bucket_rejects_explicit_private_with_owner_enforced() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .create_bucket(&CreateBucketRequest {
                name: trusted_bucket_name("bucket"),
                requester: test_helpers::requester("owner-a"),
                namespace: BucketNamespace::Global,
                acl: CreateBucketAcl::Canned(BucketAcl::Private),
                ownership: BucketObjectOwnership::BucketOwnerEnforced,
                object_lock_enabled: false,
            })
            .unwrap_err();
        assert!(matches!(
            err,
            ServerError::InvalidBucketAclWithObjectOwnership
        ));
    }

    #[test]
    fn create_bucket_persists_explicit_grants() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        let owner = AccountIdentity::new(
            "owner-a",
            CanonicalUserId::from_principal("owner-create-grants-canonical"),
            "Owner A",
        );
        let writer = AccountIdentity::new(
            "writer-a",
            CanonicalUserId::from_principal("writer-create-grants-canonical"),
            "Writer A",
        );
        let owner_requester = Requester::authenticated(owner.clone());
        let writer_requester = Requester::authenticated(writer.clone());

        coord
            .create_bucket(&CreateBucketRequest {
                name: trusted_bucket_name("bucket"),
                requester: owner_requester.clone(),
                namespace: BucketNamespace::Global,
                acl: CreateBucketAcl::Grants(AclGrants::new(vec![
                    AclGrant::new(
                        AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                        AclPermission::Read,
                    ),
                    AclGrant::new(
                        AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                        AclPermission::Write,
                    ),
                    AclGrant::new(
                        AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                        AclPermission::ReadAcp,
                    ),
                    AclGrant::new(
                        AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                        AclPermission::WriteAcp,
                    ),
                    AclGrant::new(
                        AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
                        AclPermission::FullControl,
                    ),
                ])),
                ownership: BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();

        let acl = get_bucket_acl_test(&coord, "bucket", owner_requester.clone(), None).unwrap();
        assert!(grants_contain(
            &acl.acl_grants,
            &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
            AclPermission::Read,
        ));
        assert!(grants_contain(
            &acl.acl_grants,
            &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
            AclPermission::Write,
        ));
        assert!(grants_contain(
            &acl.acl_grants,
            &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
            AclPermission::ReadAcp,
        ));
        assert!(grants_contain(
            &acl.acl_grants,
            &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
            AclPermission::WriteAcp,
        ));
        assert!(grants_contain(
            &acl.acl_grants,
            &AclGrantee::CanonicalUser(writer.canonical_user_id().clone()),
            AclPermission::FullControl,
        ));
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", writer_requester, None),
                data: b"granted-write",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
    }

    #[test]
    fn list_buckets_with_sparse_pg_topology() {
        let tmp = test_util::tempdir();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &[0, 2, 5]).unwrap());
        let coord = setup_coordinator_with_shared_storage(storage_node);
        let bucket = "bucket-sparse";
        coord
            .create_bucket_for_owner("default-owner", bucket, false)
            .unwrap();

        let names: Vec<String> = coord
            .list_buckets(&ListBucketsRequest {
                requester: test_helpers::requester("default-owner"),
            })
            .unwrap()
            .into_iter()
            .map(|b| b.name.into_string())
            .collect();
        assert_eq!(names, vec![bucket]);
    }

    #[test]
    fn list_objects_with_sparse_pg_topology() {
        let tmp = test_util::tempdir();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &[0, 2, 5]).unwrap());
        let coord = setup_coordinator_with_shared_storage(storage_node);
        let bucket = "bucket-sparse";
        coord
            .create_bucket_for_owner("default-owner", bucket, false)
            .unwrap();

        let resp = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
            })
            .unwrap();
        assert!(resp.objects.is_empty());
    }

    #[test]
    fn list_object_versions_with_sparse_pg_topology() {
        let tmp = test_util::tempdir();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &[0, 2, 5]).unwrap());
        let coord = setup_coordinator_with_shared_storage(storage_node);
        let bucket = "bucket-sparse";
        coord
            .create_bucket_for_owner("default-owner", bucket, false)
            .unwrap();

        let resp = coord
            .list_object_versions(&ListObjectVersionsRequest {
                bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                max_keys: 1000,
            })
            .unwrap();
        assert!(resp.versions.is_empty());
    }

    #[test]
    fn list_object_versions_clamps_oversized_max_keys() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        let metadata = MetadataBlob::new();
        let system_metadata = SystemMetadata::EMPTY;

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        put_bucket_versioning_test(
            &coord,
            "bucket",
            BucketVersioningState::Enabled,
            test_requester(),
            None,
        )
        .unwrap();

        for index in 0..1005 {
            let key = format!("key-{index:04}");
            test_helpers::put_object(
                &coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        "bucket",
                        &key,
                        test_requester(),
                        None,
                    ),
                    data: b"value",
                    metadata: &metadata,
                    system_metadata: &system_metadata,
                    tags: None,
                    cond: NO_WRITE,
                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
            .unwrap();
        }

        let resp = coord
            .list_object_versions(&ListObjectVersionsRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                max_keys: 5000,
            })
            .unwrap();

        assert_eq!(resp.versions.len(), 1000);
        assert!(resp.is_truncated);
        assert_eq!(resp.next_key_marker.as_deref(), Some("key-0999"));
        assert_eq!(resp.next_version_id_marker, Some(VersionId::from_u64(1)));
    }

    #[test]
    fn list_object_versions_paginates_across_pgs() {
        fn key_for_prefix_on_distinct_pg(
            coord: &Coordinator,
            bucket: &str,
            prefix: &str,
            excluded_pg_ids: &[u32],
        ) -> String {
            for index in 0..10_000 {
                let key = format!("{prefix}-{index:04}");
                let pg_id = coord.object_pg_id(bucket, &key);
                if !excluded_pg_ids.contains(&pg_id) {
                    return key;
                }
            }
            panic!("failed to find key for prefix {prefix}");
        }

        let tmp = test_util::tempdir();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &[0, 2, 5]).unwrap());
        let coord = setup_coordinator_with_shared_storage(storage_node);
        let metadata = MetadataBlob::new();
        let system_metadata = SystemMetadata::EMPTY;

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        put_bucket_versioning_test(
            &coord,
            "bucket",
            BucketVersioningState::Enabled,
            test_requester(),
            None,
        )
        .unwrap();

        let key_a = key_for_prefix_on_distinct_pg(&coord, "bucket", "a", &[]);
        let pg_a = coord.object_pg_id("bucket", &key_a);
        let key_b = key_for_prefix_on_distinct_pg(&coord, "bucket", "b", &[pg_a]);
        let pg_b = coord.object_pg_id("bucket", &key_b);
        let key_c = key_for_prefix_on_distinct_pg(&coord, "bucket", "c", &[pg_a, pg_b]);

        let older_a = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    &key_a,
                    test_requester(),
                    None,
                ),
                data: b"older-a",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        let newer_a = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    &key_a,
                    test_requester(),
                    None,
                ),
                data: b"newer-a",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    &key_b,
                    test_requester(),
                    None,
                ),
                data: b"value-b",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    &key_c,
                    test_requester(),
                    None,
                ),
                data: b"value-c",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let first_page = coord
            .list_object_versions(&ListObjectVersionsRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                max_keys: 2,
            })
            .unwrap();
        assert_eq!(first_page.versions.len(), 2);
        assert_eq!(first_page.versions[0].key, key_a);
        assert_eq!(first_page.versions[0].version_id, newer_a.version_id);
        assert_eq!(first_page.versions[1].key, key_a);
        assert_eq!(first_page.versions[1].version_id, older_a.version_id);
        assert!(first_page.versions[0].is_latest);
        assert!(!first_page.versions[1].is_latest);
        assert!(first_page.is_truncated);
        assert_eq!(first_page.next_key_marker.as_deref(), Some(key_a.as_str()));
        assert_eq!(first_page.next_version_id_marker, Some(older_a.version_id));

        let second_page = coord
            .list_object_versions(&ListObjectVersionsRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                key_marker: first_page.next_key_marker.as_deref(),
                version_id_marker: first_page.next_version_id_marker,
                max_keys: 2,
            })
            .unwrap();
        assert_eq!(second_page.versions.len(), 2);
        assert_eq!(second_page.versions[0].key, key_b);
        assert_eq!(second_page.versions[0].version_id, VersionId::from_u64(1));
        assert!(second_page.versions[0].is_latest);
        assert_eq!(second_page.versions[1].key, key_c);
        assert_eq!(second_page.versions[1].version_id, VersionId::from_u64(1));
        assert!(second_page.versions[1].is_latest);
        assert!(!second_page.is_truncated);
        assert_eq!(second_page.next_key_marker, None);
        assert_eq!(second_page.next_version_id_marker, None);
    }

    #[test]
    fn list_multipart_uploads_with_sparse_pg_topology() {
        let tmp = test_util::tempdir();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &[0, 2, 5]).unwrap());
        let coord = setup_coordinator_with_shared_storage(storage_node);
        let bucket = "bucket-sparse";
        let key = "key-sparse";
        coord
            .create_bucket_for_owner("default-owner", bucket, false)
            .unwrap();
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                object: object_request_with_expected_owner(bucket, key, test_requester(), None),
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                checksum: None,
                acl: NO_PUT_OBJECT_ACL.into(),
                encryption: WriteEncryptionRequest::none(),
                object_lock: ObjectLockState::default(),
                policy_context: PutObjectPolicyContext::default(),
            })
            .unwrap();

        let resp = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket: bucket_request_with_expected_owner(bucket, test_requester(), None),
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1000,
            })
            .unwrap();
        assert_eq!(resp.uploads.len(), 1);
        assert_eq!(resp.uploads[0].key, key);
    }

    #[test]
    fn delete_nonempty_bucket_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let err = delete_bucket_test(&coord, "bucket").unwrap_err();
        assert!(matches!(err, ServerError::BucketNotEmpty));
    }

    #[test]
    fn put_object_persists_tags_in_initial_write() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        let tags_xml =
            "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: Some(tags_xml),
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.tags.as_deref(), Some(tags_xml));
    }

    #[test]
    fn put_object_with_tags_allows_same_account_owner_account() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        let same_account_canonical_id = CanonicalUserId::from_principal("111122223333");
        let bucket_owner = AccountIdentity::new(
            "arn:aws:iam::111122223333:root",
            same_account_canonical_id.clone(),
            "Bucket Owner",
        );
        let same_account_account_principal = AccountIdentity::new(
            "111122223333",
            same_account_canonical_id,
            "Same Account Owner Principal",
        );

        coord
            .create_bucket(&CreateBucketRequest {
                name: trusted_bucket_name("bucket"),
                requester: Requester::authenticated(bucket_owner.clone()),
                namespace: BucketNamespace::Global,
                acl: CreateBucketAcl::DefaultPrivate,
                ownership: BucketObjectOwnership::ObjectWriter,
                object_lock_enabled: false,
            })
            .unwrap();

        let tags_xml =
            "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "key",
                    Requester::authenticated_owner_account_admin(same_account_account_principal),
                    None,
                ),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: Some(tags_xml),
                cond: NO_WRITE,
                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let tags = get_object_tags_test(
            &coord,
            "bucket",
            "key",
            None,
            Requester::authenticated(bucket_owner),
            None,
        )
        .unwrap();
        assert_eq!(tags.as_deref(), Some(tags_xml));
    }

    #[test]
    fn put_object_does_not_wait_for_bucket_lock() {
        let tmp = test_util::tempdir();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let admin = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
        let writer = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
        admin
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        let guard = storage_node.lock_bucket(&trusted_bucket_name("bucket"));
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let res = test_helpers::put_object(
                &writer,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        "bucket",
                        "key",
                        test_requester(),
                        None,
                    ),
                    data: b"data",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,

                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            );
            tx.send(res).unwrap();
        });

        let res = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(guard);
        assert!(
            res.is_ok(),
            "put_object should succeed without waiting on bucket lock: {res:?}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn create_multipart_upload_does_not_wait_for_bucket_lock() {
        let tmp = test_util::tempdir();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let admin = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
        let creator = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
        admin
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        let guard = storage_node.lock_bucket(&trusted_bucket_name("bucket"));
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let metadata = MetadataBlob::new();
            let res = creator.create_multipart_upload(&CreateMultipartUploadRequest {
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                metadata: &metadata,
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                checksum: None,

                acl: NO_PUT_OBJECT_ACL.into(),
                encryption: WriteEncryptionRequest::none(),
                object_lock: ObjectLockState::default(),
                policy_context: PutObjectPolicyContext::default(),
            });
            tx.send(res).unwrap();
        });

        let res = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(guard);
        assert!(
            res.is_ok(),
            "create_multipart_upload should succeed without waiting on bucket lock: {res:?}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn delete_bucket_does_not_wait_for_bucket_lock() {
        let tmp = test_util::tempdir();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let admin = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
        let deleter = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
        admin
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        let guard = storage_node.lock_bucket(&trusted_bucket_name("bucket"));
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let res = delete_bucket_test(&deleter, "bucket");
            tx.send(res).unwrap();
        });

        let res = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(guard);
        assert!(
            res.is_ok(),
            "delete_bucket should succeed without waiting on bucket lock: {res:?}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn head_object_lazily_populates_bucket_fast_path() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let key = find_key_with_object_pg_ne_bucket_pg(&coord, "bucket", "head-fast");
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        coord
            .storage_node
            .remove_bucket_fast_path(&trusted_bucket_name("bucket"));
        assert!(coord
            .storage_node
            .get_bucket_fast_path(&trusted_bucket_name("bucket"))
            .is_none());

        let head = coord
            .head_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    &key,
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(head.size, 4);

        let cached = coord
            .storage_node
            .get_bucket_fast_path(&trusted_bucket_name("bucket"))
            .expect("head_object should repopulate bucket fast path");
        assert_eq!(cached.name.as_str(), "bucket");
        assert_eq!(cached.state, BucketState::Active);
    }

    #[test]
    fn head_object_does_not_wait_for_bucket_pg_when_fast_path_is_warm() {
        let tmp = test_util::tempdir();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let admin = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
        let reader = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));

        admin
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let key = find_key_with_object_pg_ne_bucket_pg(&admin, "bucket", "head-fast");
        test_helpers::put_object(
            &admin,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        storage_node.remove_bucket_fast_path(&trusted_bucket_name("bucket"));
        reader
            .head_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    &key,
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();

        let bucket_pg = admin.get_bucket_pg("bucket").unwrap();
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let res = reader.head_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    &key,
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            });
            tx.send(res).unwrap();
        });

        let head = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("head_object should not block on bucket pg")
            .unwrap();
        drop(bucket_pg);
        assert_eq!(head.size, 4);
        handle.join().unwrap();
    }

    #[test]
    fn delete_object_does_not_wait_for_bucket_pg_when_fast_path_is_warm() {
        let tmp = test_util::tempdir();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let admin = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
        let deleter = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));

        admin
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let key = find_key_with_object_pg_ne_bucket_pg(&admin, "bucket", "delete-fast");
        test_helpers::put_object(
            &admin,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", &key, test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        storage_node.remove_bucket_fast_path(&trusted_bucket_name("bucket"));
        admin
            .head_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    &key,
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();

        let bucket_pg = admin.get_bucket_pg("bucket").unwrap();
        let (tx, rx) = mpsc::channel();
        let key_for_delete = key.clone();
        let handle = thread::spawn(move || {
            let res = deleter.delete_object(&delete_object_request(
                "bucket",
                &key_for_delete,
                None,
                test_requester(),
                false,
                NO_DELETE,
            ));
            tx.send(res).unwrap();
        });

        let deleted = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("delete_object should not block on bucket pg")
            .unwrap();
        drop(bucket_pg);
        assert!(!deleted.delete_marker);
        assert!(matches!(
            admin.get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    &key,
                    None,
                    test_requester(),
                    None
                ),
                cond: NO_READ,
            }),
            Err(ServerError::ObjectNotFound { .. })
        ));
        handle.join().unwrap();
    }

    #[test]
    fn complete_multipart_upload_does_not_wait_for_bucket_lock() {
        let tmp = test_util::tempdir();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let admin = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
        let completer = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
        admin
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        let (upload_id, parts) = create_upload_with_parts(&admin, "bucket", "key", &[(1, b"part")]);

        let guard = storage_node.lock_bucket(&trusted_bucket_name("bucket"));
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let res = completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
                upload: multipart_object_request_with_expected_owner(
                    "bucket",
                    "key",
                    &upload_id,
                    test_requester(),
                    None,
                ),
                parts: &parts,
                claimed_checksum: None,

                expected_object_size: None,

                cond: &WriteCondition::default(),

                sse_customer: None,
            });
            tx.send(res).unwrap();
        });

        let res = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        drop(guard);
        assert!(
            res.is_ok(),
            "complete_multipart_upload should succeed without waiting on bucket lock: {res:?}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn complete_multipart_upload_waits_for_multipart_completion_lock() {
        let tmp = test_util::tempdir();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let admin = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
        let completer = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
        admin
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        let (upload_id, parts) = create_upload_with_parts(&admin, "bucket", "key", &[(1, b"part")]);

        let guard = storage_node.lock_multipart_completion_bucket(&trusted_bucket_name("bucket"));
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let res = completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
                upload: multipart_object_request_with_expected_owner(
                    "bucket",
                    "key",
                    &upload_id,
                    test_requester(),
                    None,
                ),
                parts: &parts,
                claimed_checksum: None,
                expected_object_size: None,
                cond: &WriteCondition::default(),
                sse_customer: None,
            });
            tx.send(res).unwrap();
        });

        assert!(
            matches!(
                rx.recv_timeout(Duration::from_millis(200)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "complete_multipart_upload should wait for multipart completion lock"
        );
        drop(guard);
        let res = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            res.is_ok(),
            "complete_multipart_upload should succeed after multipart completion lock is released: {res:?}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn complete_multipart_upload_does_not_deadlock_when_bucket_policy_shares_pg() {
        let tmp = test_util::tempdir();
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(tmp.path(), &pg_ids).unwrap());
        let admin = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));
        let completer = setup_coordinator_with_shared_storage(Arc::clone(&storage_node));

        admin
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        put_bucket_policy_test(
            &admin,
            "bucket",
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"default-owner"},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
            test_requester(),
            None,
        )
        .unwrap();
        admin.clear_bucket_policy_cache(&trusted_bucket_name("bucket"));

        let bucket_pg_id = admin.bucket_pg_id("bucket");
        let key = (0..1024)
            .map(|i| format!("same-pg-{i}"))
            .find(|candidate| admin.object_pg_id("bucket", candidate) == bucket_pg_id)
            .expect("expected to find a key whose object PG matches the bucket PG");

        let (upload_id, parts) = create_upload_with_parts(&admin, "bucket", &key, &[(1, b"part")]);

        let (tx, rx) = mpsc::channel();
        let key_for_complete = key.clone();
        let handle = thread::spawn(move || {
            let res = completer.complete_multipart_upload(&CompleteMultipartUploadRequest {
                upload: multipart_object_request_with_expected_owner(
                    "bucket",
                    &key_for_complete,
                    &upload_id,
                    test_requester(),
                    None,
                ),
                parts: &parts,
                claimed_checksum: None,
                expected_object_size: None,
                cond: &WriteCondition::default(),
                sse_customer: None,
            });
            tx.send(res).unwrap();
        });

        let res = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("complete_multipart_upload should not deadlock on bucket policy lookup");
        assert!(
            res.is_ok(),
            "complete_multipart_upload should succeed when bucket policy shares the metadata PG: {res:?}"
        );
        handle.join().unwrap();
    }

    #[test]
    fn delete_bucket_rejects_active_stream_put_session() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        let err = delete_bucket_test(&coord, "bucket").unwrap_err();
        assert!(matches!(err, ServerError::BucketNotEmpty));

        coord
            .abort_stream_put("bucket", "key", &session_id)
            .unwrap();
        delete_bucket_test(&coord, "bucket").unwrap();
        wait_until_bucket_gone(&coord, "bucket");
    }

    #[test]
    fn put_get_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        let headers = [("Content-Type", "text/plain")];
        let metadata = MetadataBlob::from_headers(&headers).unwrap();
        let system_metadata = SystemMetadata::from_headers(&headers).unwrap();
        let result = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "hello.txt",
                    test_requester(),
                    None,
                ),
                data: b"Hello, world!",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        assert!(!result.etag.is_empty());

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "hello.txt",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"Hello, world!");
        assert_eq!(obj.size, 13);
        assert_eq!(obj.system_metadata.content_type(), Some("text/plain"));
    }

    #[test]
    fn put_get_with_metadata() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        let headers = [
            ("Content-Type", "application/json"),
            ("X-Amz-Meta-Author", "alice"),
            ("X-Amz-Meta-Version", "42"),
        ];
        let metadata = MetadataBlob::from_headers(&headers).unwrap();
        let system_metadata = SystemMetadata::from_headers(&headers).unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "obj", test_requester(), None),
                data: b"{}",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "obj",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"{}");
        assert_eq!(obj.system_metadata.content_type(), Some("application/json"));
        assert_eq!(obj.metadata.get("x-amz-meta-author"), Some("alice"));
        assert_eq!(obj.metadata.get("x-amz-meta-version"), Some("42"));
    }

    #[test]
    fn head_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let metadata = MetadataBlob::from_headers(&[("Content-Type", "text/plain")]).unwrap();
        let system_metadata =
            SystemMetadata::from_headers(&[("Content-Type", "text/plain")]).unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"data",
                metadata: &metadata,
                system_metadata: &system_metadata,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let head = coord
            .head_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(head.size, 4);
        assert_eq!(head.system_metadata.content_type(), Some("text/plain"));
    }

    #[test]
    fn overwrite_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"v1",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"v2",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"v2");
    }

    #[test]
    fn empty_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "empty",
                    test_requester(),
                    None,
                ),
                data: b"",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "empty",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"");
        assert_eq!(obj.size, 0);
    }

    #[test]
    fn delete_object_then_get_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        coord
            .delete_object(&delete_object_request(
                "bucket",
                "key",
                None,
                test_requester(),
                false,
                NO_DELETE,
            ))
            .unwrap();

        let err = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn delete_object_eventually_reclaims_simple_shards() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"simple-data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let (generation_id, ec) = {
            let meta_pg_id = coord.object_pg_id("bucket", "key");
            let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            match pg
                .get_object_meta(&trusted_bucket_name("bucket"), &trusted_object_key("key"))
                .unwrap()
            {
                StoredObject::Live(record) => (record.generation_id, record.ec),
                other @ StoredObject::DeleteMarker(_) => {
                    panic!("expected live object, got {other:?}")
                }
            }
        };
        let shard_pg_id = coord.shard_pg_id("bucket", "key", generation_id);
        let okh = object_key_hash(
            trusted_bucket_name("bucket").as_str(),
            trusted_object_key("key").as_str(),
        );

        coord
            .delete_object(&delete_object_request(
                "bucket",
                "key",
                None,
                test_requester(),
                false,
                NO_DELETE,
            ))
            .unwrap();

        wait_for_shard_set_deletion(&coord, shard_pg_id, &okh, generation_id, ec);
    }

    #[test]
    fn delete_nonexistent_object_is_ok() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        // Should not error
        coord
            .delete_object(&delete_object_request(
                "bucket",
                "no-such-key",
                None,
                test_requester(),
                false,
                NO_DELETE,
            ))
            .unwrap();
    }

    #[test]
    fn list_objects() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "a/1", test_requester(), None),
                data: b"1",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "a/2", test_requester(), None),
                data: b"2",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "b/1", test_requester(), None),
                data: b"3",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
            })
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

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "photos/cat.jpg",
                    test_requester(),
                    None,
                ),
                data: b"cat",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "photos/dog.jpg",
                    test_requester(),
                    None,
                ),
                data: b"dog",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "docs/readme.md",
                    test_requester(),
                    None,
                ),
                data: b"md",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: Some("photos/"),
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
            })
            .unwrap();
        assert_eq!(result.objects.len(), 2);
    }

    #[test]
    fn list_objects_with_delimiter() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "photos/cat.jpg",
                    test_requester(),
                    None,
                ),
                data: b"cat",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "photos/dog.jpg",
                    test_requester(),
                    None,
                ),
                data: b"dog",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "docs/readme.md",
                    test_requester(),
                    None,
                ),
                data: b"md",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "root.txt",
                    test_requester(),
                    None,
                ),
                data: b"root",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: Some("/"),
                continuation_token: None,
                max_keys: 1000,
            })
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

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "folder/",
                    test_requester(),
                    None,
                ),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "folder/",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"data");
        assert_eq!(obj.size, 4);
    }

    // ── Disk manipulation helpers for EC tests ────────────────────────

    /// Compute shard file path on disk for a given object and shard index.
    fn shard_file_path(
        coord: &Coordinator,
        data_dir: &Path,
        bucket: &str,
        key: &str,
        shard_index: u8,
    ) -> PathBuf {
        let (shard_pg_id, okh, generation_id) = {
            let meta_pg_id = coord.object_pg_id(bucket, key);
            let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let record = pg
                .get_object_meta(&trusted_bucket_name(bucket), &trusted_object_key(key))
                .unwrap();
            let segments = pg
                .get_object_segments(
                    &trusted_bucket_name(bucket),
                    &trusted_object_key(key),
                    record.version_id(),
                )
                .unwrap();
            if let Some(segment) = segments.first() {
                (
                    segment.shard_pg_id,
                    segment.segment_okh,
                    segment.segment_vid,
                )
            } else {
                let live = record.as_live().expect("expected live object");
                let bucket_name = trusted_bucket_name(bucket);
                let object_key = trusted_object_key(key);
                (
                    coord.shard_pg_id(bucket, key, live.generation_id),
                    object_key_hash(bucket_name.as_str(), object_key.as_str()),
                    live.generation_id,
                )
            }
        };
        let shard_key = ShardKey::new(&okh, generation_id.get(), shard_index);
        data_dir
            .join(format!("pg-{shard_pg_id:04}"))
            .join("shards")
            .join(shard_key.hex_prefix())
            .join(shard_key.hex())
    }

    /// Delete a specific shard file from disk.
    fn delete_shard_on_disk(
        coord: &Coordinator,
        data_dir: &Path,
        bucket: &str,
        key: &str,
        shard_index: u8,
    ) {
        let path = shard_file_path(coord, data_dir, bucket, key, shard_index);
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
        coord: &Coordinator,
        data_dir: &Path,
        bucket: &str,
        key: &str,
        shard_index: u8,
    ) {
        let path = shard_file_path(coord, data_dir, bucket, key, shard_index);
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

    fn wait_until(description: &str, timeout: Duration, mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + timeout;
        loop {
            if predicate() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {description}"
            );
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn wait_for_shard_set_deletion(
        coord: &Coordinator,
        shard_pg_id: u32,
        okh: &[u8; 16],
        generation_id: GenerationId,
        ec: EcShape,
    ) {
        wait_until("shard-set reclaim", Duration::from_secs(1), || -> bool {
            let pg = coord.storage_node.get_pg(shard_pg_id).unwrap();
            (0..(ec.k as usize + ec.m as usize)).all(|i| {
                let shard_key = ShardKey::new(okh, generation_id.get(), i as u8);
                matches!(
                    pg.stat_shard(&shard_key),
                    Err(storage::StoreError::NotFound)
                )
            })
        });
    }

    // ── EC fault injection tests ────────────────────────────────────

    #[test]
    fn ec_reconstruction_after_shard_loss() {
        if !backend_supports_parity_recovery() {
            return;
        }
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let data = b"This data should survive shard loss!";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "resilient",
                    test_requester(),
                    None,
                ),
                data,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        // Delete one data shard using the helper
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "resilient", 0);

        // Get should still succeed via EC reconstruction
        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "resilient",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), data);
    }

    #[test]
    fn ec_drop_one_data_shard_get() {
        if !backend_supports_parity_recovery() {
            return;
        }
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let data = b"EC single shard loss test data";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "obj1",
                    test_requester(),
                    None,
                ),
                data,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj1", 0);

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "obj1",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), data);
    }

    #[test]
    fn ec_degraded_read_reuses_reconstruction_scratch() {
        if !backend_supports_parity_recovery() {
            return;
        }
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let data = vec![5u8; INTERNAL_SEGMENT_SIZE];
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "obj-reconstruct",
                    test_requester(),
                    None,
                ),
                data: &data,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj-reconstruct", 0);

        assert_eq!(coord.payload_buffer_pool.allocation_count(), 0);

        let first = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "obj-reconstruct",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(first.body.read_all().unwrap(), data);
        assert_eq!(coord.payload_buffer_pool.allocation_count(), 2);

        let second = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "obj-reconstruct",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(second.body.read_all().unwrap(), data);
        assert_eq!(coord.payload_buffer_pool.allocation_count(), 2);
    }

    #[test]
    fn ec_drop_m_shards_at_limit() {
        if !backend_supports_parity_recovery() {
            return;
        }
        // Config: k=4, m=2. Dropping exactly m=2 shards should still recover.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let data = b"EC m-shard loss limit test data";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "obj2",
                    test_requester(),
                    None,
                ),
                data,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        // Delete 2 data shards (indices 0 and 1)
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj2", 0);
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj2", 1);

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "obj2",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), data);
    }

    #[test]
    fn ec_drop_m_plus_one_shards_fails() {
        // Config: k=4, m=2. Dropping m+1=3 shards should fail.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let data = b"EC m+1 shard loss test data";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "obj3",
                    test_requester(),
                    None,
                ),
                data,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        // Delete 3 shards (indices 0, 1, 2)
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj3", 0);
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj3", 1);
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj3", 2);

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "obj3",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        let err = obj.body.read_all().unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn ec_corrupt_one_data_shard_recovery() {
        if !backend_supports_parity_recovery() {
            return;
        }
        // Corrupt shard 0 on disk. PgStore detects CRC mismatch, EC reconstructs.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let data = b"EC corruption recovery test data";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "obj4",
                    test_requester(),
                    None,
                ),
                data,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        corrupt_shard_on_disk(&coord, tmp.path(), "bucket", "obj4", 0);

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "obj4",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), data);
    }

    #[test]
    fn ec_range_get_with_missing_shard() {
        if !backend_supports_parity_recovery() {
            return;
        }
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let data = b"Hello, World! Range test with EC recovery";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "obj5",
                    test_requester(),
                    None,
                ),
                data,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        // Delete shard 0 (covers the beginning of the data)
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj5", 0);

        // Range get should still succeed via EC reconstruction
        let result = coord
            .get_object_range(&GetObjectRangeRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "obj5",
                    None,
                    test_requester(),
                    None,
                ),
                range: ByteRange::Range { start: 0, end: 4 },
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"Hello");
    }

    #[test]
    fn ec_drop_parity_shard_data_still_works() {
        if !backend_supports_parity_recovery() {
            return;
        }
        // Delete parity shard (index k=4). Only data shards needed for normal read.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let data = b"EC parity shard drop test";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "obj6",
                    test_requester(),
                    None,
                ),
                data,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        // Delete first parity shard (index 4, since k=4)
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj6", 4);

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "obj6",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), data);
    }

    #[test]
    fn ec_healthy_read_skips_corrupt_parity_shards() {
        if !backend_supports_parity_recovery() {
            return;
        }
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let data = b"EC healthy read should skip parity shards";
        let put = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "obj7",
                    test_requester(),
                    None,
                ),
                data,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let segment = {
            let meta_pg_id = coord.object_pg_id("bucket", "obj7");
            let meta_pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let segments = meta_pg
                .get_object_segments(
                    &trusted_bucket_name("bucket"),
                    &trusted_object_key("obj7"),
                    put.version_id,
                )
                .unwrap();
            assert_eq!(segments.len(), 1);
            segments[0].clone()
        };

        corrupt_shard_on_disk(&coord, tmp.path(), "bucket", "obj7", 4);

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "obj7",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), data);

        let shard_pg = coord.storage_node.get_pg(segment.shard_pg_id).unwrap();
        let parity_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), 4);
        assert!(
            shard_pg.stat_shard(&parity_key).is_ok(),
            "healthy-path read should not touch parity shard 4"
        );
    }

    #[test]
    fn ec_reconstruction_stops_after_first_needed_parity_shard() {
        if !backend_supports_parity_recovery() {
            return;
        }
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let data = b"EC reconstruction should stop after first needed parity";
        let put = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "obj8",
                    test_requester(),
                    None,
                ),
                data,
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let segment = {
            let meta_pg_id = coord.object_pg_id("bucket", "obj8");
            let meta_pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let segments = meta_pg
                .get_object_segments(
                    &trusted_bucket_name("bucket"),
                    &trusted_object_key("obj8"),
                    put.version_id,
                )
                .unwrap();
            assert_eq!(segments.len(), 1);
            segments[0].clone()
        };

        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj8", 0);
        corrupt_shard_on_disk(&coord, tmp.path(), "bucket", "obj8", 5);

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "obj8",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), data);

        let shard_pg = coord.storage_node.get_pg(segment.shard_pg_id).unwrap();
        let parity_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), 5);
        assert!(
            shard_pg.stat_shard(&parity_key).is_ok(),
            "reconstruction should stop once enough shards are present"
        );
    }

    #[test]
    fn put_to_nonexistent_bucket_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "no-such-bucket",
                    "key",
                    test_requester(),
                    None,
                ),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn get_nonexistent_object_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let err = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "no-such-key",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn etag_consistency() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        let result = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(result.etag, obj.etag);

        let head = coord
            .head_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(result.etag, head.etag);
    }

    #[test]
    fn list_objects_delimiter_with_continuation() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "a/1", test_requester(), None),
                data: b"1",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "a/2", test_requester(), None),
                data: b"2",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "b/1", test_requester(), None),
                data: b"3",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "c/1", test_requester(), None),
                data: b"4",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "root.txt",
                    test_requester(),
                    None,
                ),
                data: b"5",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        // First page: max_keys=2 with delimiter
        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: Some("/"),
                continuation_token: None,
                max_keys: 2,
            })
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
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: Some("/"),
                continuation_token: Some(&token),
                max_keys: 2,
            })
            .unwrap();
        assert!(
            !result2.objects.is_empty() || !result2.common_prefixes.is_empty(),
            "continuation page should have entries"
        );
    }

    #[test]
    fn list_objects_delimiter_continuation_skips_large_common_prefix() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        for i in 0..1500 {
            let key = format!("dir/file-{i:04}.txt");
            test_helpers::put_object(
                &coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        "bucket",
                        &key,
                        test_requester(),
                        None,
                    ),
                    data: b"data",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,

                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
            .unwrap();
        }
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "z.txt",
                    test_requester(),
                    None,
                ),
                data: b"z",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let page1 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: Some("/"),
                continuation_token: None,
                max_keys: 1,
            })
            .unwrap();
        assert!(page1.objects.is_empty());
        assert_eq!(page1.common_prefixes, vec!["dir/".to_string()]);
        assert!(page1.is_truncated);

        let token = page1
            .next_continuation_token
            .as_deref()
            .expect("first page should return a continuation token")
            .to_string();
        let page2 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: Some("/"),
                continuation_token: Some(&token),
                max_keys: 1,
            })
            .unwrap();
        assert_eq!(page2.common_prefixes, Vec::<String>::new());
        assert_eq!(page2.objects.len(), 1);
        assert_eq!(page2.objects[0].key, "z.txt");
        assert!(!page2.is_truncated);
    }

    #[test]
    fn list_objects_delimiter_with_no_upper_bound_common_prefix_is_final_page() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        let delimiter = "\u{10ffff}";

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "a", test_requester(), None),
                data: b"a",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    &format!("{delimiter}child"),
                    test_requester(),
                    None,
                ),
                data: b"b",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let page1 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: Some(delimiter),
                continuation_token: None,
                max_keys: 1,
            })
            .unwrap();
        assert_eq!(page1.objects.len(), 1);
        assert_eq!(page1.objects[0].key, "a");
        assert!(page1.common_prefixes.is_empty());
        assert!(page1.is_truncated);

        let token = page1
            .next_continuation_token
            .as_deref()
            .expect("first page should return a continuation token")
            .to_string();
        let page2 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: Some(delimiter),
                continuation_token: Some(&token),
                max_keys: 1,
            })
            .unwrap();
        assert!(page2.objects.is_empty());
        assert_eq!(page2.common_prefixes, vec![delimiter.to_string()]);
        assert!(!page2.is_truncated);
        assert!(page2.next_continuation_token.is_none());
    }

    #[test]
    fn list_objects_delimiter_continuation_with_boundary_token_does_not_panic() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        let delimiter = "\x7f";
        let token = format!("{}{}", "a".repeat(1023), delimiter);

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "z", test_requester(), None),
                data: b"z",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let page2 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: Some(delimiter),
                continuation_token: Some(&token),
                max_keys: 1,
            })
            .unwrap();
        assert_eq!(page2.common_prefixes, Vec::<String>::new());
        assert_eq!(page2.objects.len(), 1);
        assert_eq!(page2.objects[0].key, "z");
        assert!(!page2.is_truncated);
        assert!(page2.next_continuation_token.is_none());
    }

    #[test]
    fn list_objects_delimiter_common_prefix_boundary_falls_back_without_error() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        let delimiter = "\x7f";
        let common_prefix = format!("{}{}", "a".repeat(1023), delimiter);

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    &common_prefix,
                    test_requester(),
                    None,
                ),
                data: b"prefix",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "z", test_requester(), None),
                data: b"z",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let page1 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: Some(delimiter),
                continuation_token: None,
                max_keys: 1,
            })
            .unwrap();
        assert!(page1.objects.is_empty());
        assert_eq!(page1.common_prefixes, vec![common_prefix.clone()]);
        assert!(page1.is_truncated);

        let token = page1
            .next_continuation_token
            .as_deref()
            .expect("first page should return a continuation token")
            .to_string();
        assert_eq!(token, common_prefix);

        let page2 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: Some(delimiter),
                continuation_token: Some(&token),
                max_keys: 1,
            })
            .unwrap();
        assert!(page2.common_prefixes.is_empty());
        assert_eq!(page2.objects.len(), 1);
        assert_eq!(page2.objects[0].key, "z");
        assert!(!page2.is_truncated);
        assert!(page2.next_continuation_token.is_none());
    }

    #[test]
    fn list_objects_max_keys_counts_prefixes() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        // Create many prefixed objects to ensure common_prefixes count toward max_keys
        for i in 0..10 {
            let key = format!("dir{i}/file.txt");
            test_helpers::put_object(
                &coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        "bucket",
                        &key,
                        test_requester(),
                        None,
                    ),
                    data: b"data",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,

                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
            .unwrap();
        }

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: Some("/"),
                continuation_token: None,
                max_keys: 3,
            })
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
        let err = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "no-bucket",
                    "key",
                    test_requester(),
                    None,
                ),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn delete_nonexistent_bucket_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = delete_bucket_test(&coord, "no-such-bucket").unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn list_objects_no_delimiter_truncated() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        for i in 0..5 {
            let key = format!("key-{i:02}");
            test_helpers::put_object(
                &coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        "bucket",
                        &key,
                        test_requester(),
                        None,
                    ),
                    data: b"data",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,

                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
            .unwrap();
        }

        // Request fewer than available
        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 3,
            })
            .unwrap();
        assert_eq!(result.objects.len(), 3);
        assert!(result.is_truncated);
        assert!(result.next_continuation_token.is_some());
    }

    #[test]
    fn list_objects_no_delimiter_with_continuation() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        for i in 0..5 {
            let key = format!("key-{i:02}");
            test_helpers::put_object(
                &coord,
                &PutObjectRequest {
                    encryption: WriteEncryptionRequest::none(),
                    policy_context: PutObjectPolicyContext::default(),
                    object_lock: ObjectLockState::default(),
                    object: object_request_with_expected_owner(
                        "bucket",
                        &key,
                        test_requester(),
                        None,
                    ),
                    data: b"data",
                    metadata: &MetadataBlob::new(),
                    system_metadata: &SystemMetadata::EMPTY,
                    tags: None,
                    cond: NO_WRITE,

                    acl: NO_PUT_OBJECT_ACL.into(),
                },
            )
            .unwrap();
        }

        // First page
        let page1 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 2,
            })
            .unwrap();
        assert_eq!(page1.objects.len(), 2);
        assert!(page1.is_truncated);
        let token = page1.next_continuation_token.as_ref().unwrap();

        // Second page using continuation token
        let page2 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: None,
                continuation_token: Some(token),
                max_keys: 2,
            })
            .unwrap();
        assert_eq!(page2.objects.len(), 2);
        assert!(page2.is_truncated);
        let token2 = page2.next_continuation_token.as_ref().unwrap();

        // Third page — should get remainder
        let page3 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: None,
                continuation_token: Some(token2),
                max_keys: 2,
            })
            .unwrap();
        assert_eq!(page3.objects.len(), 1);
        assert!(!page3.is_truncated);
        assert!(page3.next_continuation_token.is_none());
    }

    #[test]
    fn list_objects_prefix_with_delimiter() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "photos/2024/jan.jpg",
                    test_requester(),
                    None,
                ),
                data: b"j",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "photos/2024/feb.jpg",
                    test_requester(),
                    None,
                ),
                data: b"f",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "photos/2025/mar.jpg",
                    test_requester(),
                    None,
                ),
                data: b"m",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "photos/top.jpg",
                    test_requester(),
                    None,
                ),
                data: b"t",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        // List with prefix "photos/" and delimiter "/"
        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: Some("photos/"),
                delimiter: Some("/"),
                continuation_token: None,
                max_keys: 1000,
            })
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

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "only-one",
                    test_requester(),
                    None,
                ),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
            })
            .unwrap();
        assert_eq!(result.objects.len(), 1);
        assert!(!result.is_truncated);
        assert!(result.next_continuation_token.is_none());
    }

    #[test]
    fn list_objects_max_keys_zero() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "key1",
                    test_requester(),
                    None,
                ),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 0,
            })
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

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "a/1", test_requester(), None),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                prefix: None,
                delimiter: Some("/"),
                continuation_token: None,
                max_keys: 0,
            })
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
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner("no-bucket", test_requester(), None),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn list_objects_nonexistent_bucket_for_non_owner_still_returns_bucket_not_found() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: bucket_request_with_expected_owner(
                    "no-bucket",
                    test_helpers::requester("other-user"),
                    None,
                ),
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn delete_objects_batch() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "key1",
                    test_requester(),
                    None,
                ),
                data: b"data1",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "key2",
                    test_requester(),
                    None,
                ),
                data: b"data2",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let entries = vec![
            DeleteEntry {
                key: trusted_object_key("key1"),
                version_id: None,
                cond: DeleteCondition::None,
            },
            DeleteEntry {
                key: trusted_object_key("key2"),
                version_id: None,
                cond: DeleteCondition::None,
            },
            // key3 doesn't exist — should still succeed (idempotent)
            DeleteEntry {
                key: trusted_object_key("key3"),
                version_id: None,
                cond: DeleteCondition::None,
            },
        ];

        let result = coord
            .delete_objects(&DeleteObjectsRequest {
                bucket: bucket_request_with_expected_owner("bucket", test_requester(), None),
                entries: &entries,
                bypass_governance: false,
            })
            .unwrap();
        assert_eq!(result.deleted.len(), 3);
        assert!(result.errors.is_empty());

        // Verify objects are actually gone
        assert!(coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key1",
                    None,
                    test_requester(),
                    None
                ),
                cond: NO_READ,
            })
            .is_err());
        assert!(coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key2",
                    None,
                    test_requester(),
                    None
                ),
                cond: NO_READ,
            })
            .is_err());
    }

    #[test]
    fn delete_objects_nonexistent_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let entries = vec![DeleteEntry {
            key: trusted_object_key("key1"),
            version_id: None,
            cond: DeleteCondition::None,
        }];

        let err = coord
            .delete_objects(&DeleteObjectsRequest {
                bucket: bucket_request_with_expected_owner("no-bucket", test_requester(), None),
                entries: &entries,
                bypass_governance: false,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
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
        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                checksum: None,

                acl: NO_PUT_OBJECT_ACL.into(),
                encryption: WriteEncryptionRequest::none(),
                object_lock: ObjectLockState::default(),
                policy_context: PutObjectPolicyContext::default(),
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                upload: multipart_object_request_with_expected_owner(
                    "bucket",
                    "key",
                    &create.upload_id,
                    test_requester(),
                    None,
                ),
                parts: &parts,
                claimed_checksum: None,

                expected_object_size: None,

                cond: &WriteCondition::default(),

                sse_customer: None,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    // ── shard planning unit tests ──────────────────────────────────────

    #[test]
    fn compute_shard_size_exact_multiple() {
        // 100 bytes, k=4 → no padding needed → 25 per shard
        assert_eq!(compute_shard_size(100, 4), 25);
    }

    #[test]
    fn compute_shard_size_needs_padding() {
        // 101 bytes, k=4 → pad to 104 → 26 per shard
        assert_eq!(compute_shard_size(101, 4), 26);
    }

    #[test]
    fn compute_shard_size_small() {
        // 1 byte, k=4 → pad to 4 → 1 per shard
        assert_eq!(compute_shard_size(1, 4), 1);
    }

    #[test]
    fn compute_shard_size_zero() {
        // 0 bytes, k=4 → 0 per shard
        assert_eq!(compute_shard_size(0, 4), 0);
    }

    #[test]
    fn shards_for_byte_range_single_shard() {
        // shard_size=25, range [0,24] → shard 0
        assert_eq!(shards_for_byte_range(0, 24, 25, 4), vec![0]);
    }

    #[test]
    fn shards_for_byte_range_spans_two() {
        // shard_size=25, range [20,30] → shards 0,1
        assert_eq!(shards_for_byte_range(20, 30, 25, 4), vec![0, 1]);
    }

    #[test]
    fn shards_for_byte_range_all_shards() {
        // shard_size=25, range [0,99] → shards 0,1,2,3
        assert_eq!(shards_for_byte_range(0, 99, 25, 4), vec![0, 1, 2, 3]);
    }

    #[test]
    fn shards_for_byte_range_last_shard_only() {
        // shard_size=25, range [75,99] → shard 3
        assert_eq!(shards_for_byte_range(75, 99, 25, 4), vec![3]);
    }

    #[test]
    fn shards_for_byte_range_clamped_to_k() {
        // end falls past last shard → clamp to k-1
        assert_eq!(shards_for_byte_range(75, 200, 25, 4), vec![3]);
    }

    #[test]
    fn shards_for_byte_range_zero_shard_size() {
        let empty: Vec<usize> = vec![];
        assert_eq!(shards_for_byte_range(0, 10, 0, 4), empty);
    }

    // ── range GET tests ────────────────────────────────────────────────

    #[test]
    fn get_object_range_basic() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"Hello, World!",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        // bytes=0-4 → "Hello"
        let result = coord
            .get_object_range(&GetObjectRangeRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key",
                    None,
                    test_requester(),
                    None,
                ),
                range: ByteRange::Range { start: 0, end: 4 },
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"Hello");
        assert_eq!(result.range_start, 0);
        assert_eq!(result.range_end, 4);
        assert_eq!(result.size, 13);
    }

    #[test]
    fn get_object_range_suffix() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"Hello, World!",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        // bytes=-6 → "World!"  (last 6 bytes)
        let result = coord
            .get_object_range(&GetObjectRangeRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key",
                    None,
                    test_requester(),
                    None,
                ),
                range: ByteRange::Suffix { length: 6 },
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"World!");
        assert_eq!(result.range_start, 7);
        assert_eq!(result.range_end, 12);
    }

    #[test]
    fn get_object_range_from_start() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"Hello, World!",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        // bytes=7- → "World!"
        let result = coord
            .get_object_range(&GetObjectRangeRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key",
                    None,
                    test_requester(),
                    None,
                ),
                range: ByteRange::FromStart { start: 7 },
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"World!");
    }

    #[test]
    fn get_object_range_unsatisfiable() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"Hello",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        // bytes=100- → unsatisfiable
        let err = coord
            .get_object_range(&GetObjectRangeRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key",
                    None,
                    test_requester(),
                    None,
                ),
                range: ByteRange::FromStart { start: 100 },
                cond: NO_READ,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRange { total_size: 5 }));
    }

    #[test]
    fn get_object_range_clamps_end() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"Hello",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        // bytes=0-99999 on 5-byte object → clamp to 0-4
        let result = coord
            .get_object_range(&GetObjectRangeRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key",
                    None,
                    test_requester(),
                    None,
                ),
                range: ByteRange::Range {
                    start: 0,
                    end: 99999,
                },
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"Hello");
        assert_eq!(result.range_start, 0);
        assert_eq!(result.range_end, 4);
    }

    // ── Conditional request integration tests ────────────────────────

    #[test]
    fn put_if_none_match_star_creates() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        let cond = WriteCondition::IfNoneMatchStar;
        let result = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner(
                    "bucket",
                    "new-key",
                    test_requester(),
                    None,
                ),
                data: b"data",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: &cond,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        assert!(!result.etag.is_empty());
    }

    #[test]
    fn put_if_none_match_star_rejects_overwrite() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"v1",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let cond = WriteCondition::IfNoneMatchStar;
        let err = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"v2",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: &cond,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn put_if_match_updates() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        let r1 = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"v1",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        let cond = WriteCondition::IfMatch(SpecificEtag::new(r1.etag.clone()).unwrap());
        let r2 = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"v2",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: &cond,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        assert_ne!(r1.etag, r2.etag);

        let obj = coord
            .get_object(&GetObjectRequest {
                sse_customer: None,
                object: object_version_request_with_expected_owner(
                    "bucket",
                    "key",
                    None,
                    test_requester(),
                    None,
                ),
                cond: NO_READ,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"v2");
    }

    #[test]
    fn put_if_match_stale_etag_rejected() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("default-owner", "bucket", false)
            .unwrap();

        let r1 = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"v1",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();
        // Overwrite so etag changes
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"v2",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: NO_WRITE,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap();

        let cond = WriteCondition::IfMatch(SpecificEtag::new(r1.etag).unwrap());
        let err = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                encryption: WriteEncryptionRequest::none(),
                policy_context: PutObjectPolicyContext::default(),
                object_lock: ObjectLockState::default(),
                object: object_request_with_expected_owner("bucket", "key", test_requester(), None),
                data: b"v3",
                metadata: &MetadataBlob::new(),
                system_metadata: &SystemMetadata::EMPTY,
                tags: None,
                cond: &cond,

                acl: NO_PUT_OBJECT_ACL.into(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }
}
