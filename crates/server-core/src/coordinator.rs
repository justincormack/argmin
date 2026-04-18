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
#[cfg(test)]
use self::test_hooks::*;
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
#[cfg(test)]
mod core_tests;
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
mod streaming;
#[cfg(test)]
mod test_hooks;
#[cfg(test)]
mod test_support;
