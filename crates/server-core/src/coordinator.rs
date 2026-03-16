/// Coordinator: orchestrates S3 operations across EC, storage, and metadata layers.
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;

use checksum::{ChecksumAlgorithm, ChecksumType, MultipartChecksumConfig, RawChecksum};
use ec::{EcConfig, ErasureCodec};
use s3_types::{BucketVersioningState, CanonicalUserId, VersionId};
use storage::traits::{PgMetadataStore, ShardStore};
use storage::{
    BucketInfo, BucketName, BucketState, CommitMultipartReq, CommitStreamPutReq,
    CreateMultipartUploadReq, CreateStreamUploadReq, EcShape, GenerationId,
    ListMultipartUploadsReq, ListObjectVersionsReq, ListObjectsReq, ListPartsReq,
    MultipartPartRecord, MultipartPartSegmentRecord, MultipartReclaimPartRecord,
    MultipartReclaimPartSegmentRecord, MultipartReclaimRecord, MultipartUploadRecord, ObjectKey,
    ObjectLayout, ObjectPartRecord, ObjectSegmentRecord, ObjectSegmentsReclaimRecord,
    ObjectSegmentsReclaimSegmentRecord, PutDeleteMarkerReq, PutObjectReq, ReclaimWorkItem,
    SerializedMetadataBlob, SerializedTagSet, SessionId, ShardKey, SharedStorageNode, StoredObject,
    StreamUploadSegmentRecord, StreamUploadState, StreamUploadTarget, UploadId, UploadState,
};
#[cfg(test)]
use storage::{PutLiveObjectReq, SimplePayloadReclaimRecord};

use crate::conditional::{
    check_copy_source_conditions, check_delete_conditions, check_read_conditions,
    check_write_conditions, DeleteCondition, ReadCondition, WriteCondition,
};
use crate::error::ServerError;
use crate::etag::{compute_multipart_etag, crc64_to_etag_bytes, etag_bytes_to_crc64, format_etag};
use crate::metadata_blob::MetadataBlob;
use crate::pg::{object_key_hash, part_key_hash, stream_segment_key_hash, PgTopology};
use crate::range::ByteRange;

const TRACE_TARGET: &str = "server_core";

/// Maximum object size for single PUT or upload part (5 GiB, matches AWS S3).
pub const MAX_OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024;

/// Fixed internal segment size for newly committed segmented payloads.
pub const INTERNAL_SEGMENT_SIZE: usize = 8 * 1024 * 1024;

/// A checksum claim parsed from HTTP headers or trailers.
///
/// Base64 decoding and length validation happen at construction time,
/// so the coordinator receives already-decoded, validated bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChecksumClaim {
    algorithm: ChecksumAlgorithm,
    expected_bytes: Vec<u8>,
}

impl ChecksumClaim {
    /// Parse a base64-encoded checksum value, validating format and length.
    pub fn from_base64(algorithm: ChecksumAlgorithm, b64: &str) -> Result<Self, ServerError> {
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

    /// The expected checksum value as canonical base64.
    pub fn to_base64(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(&self.expected_bytes)
    }
}

/// A typed encoded checksum claim whose serialized form is preserved as-is.
///
/// Used for multipart-complete object-level checksum claims, where some valid
/// values are composite forms such as `base64-N`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedChecksumClaim {
    algorithm: ChecksumAlgorithm,
    encoded_value: String,
}

impl EncodedChecksumClaim {
    #[must_use]
    pub fn new(algorithm: ChecksumAlgorithm, encoded_value: String) -> Self {
        Self {
            algorithm,
            encoded_value,
        }
    }

    #[must_use]
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        self.algorithm
    }

    #[must_use]
    pub fn encoded_value(&self) -> &str {
        &self.encoded_value
    }
}

/// Hard cap on total records fetched across all PGs for a single list query.
/// Prevents unbounded memory when delimiter causes u32::MAX per-PG limits.
const MAX_LIST_RECORDS: usize = 100_000;

/// Result of a PutObject operation.
#[derive(Debug)]
pub struct PutObjectResult {
    pub etag: String,
    pub version_id: VersionId,
}

/// Core-owned bucket summary exposed above the storage layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketSummary {
    pub name: String,
    pub owner_principal: String,
    pub owner_canonical_id: CanonicalUserId,
    pub created_at: u64,
    pub public_read: bool,
    pub public_write: bool,
    pub versioning: BucketVersioningState,
    pub public_access_block: Option<String>,
    pub ownership_controls: Option<String>,
}

/// Result of a GetBucketAcl operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetBucketAclResult {
    pub owner_principal: String,
    pub owner_canonical_id: CanonicalUserId,
    pub acl: BucketAcl,
}

/// Result of beginning a streaming UploadPart session.
#[derive(Debug)]
pub struct BeginStreamPartResult {
    pub session_id: String,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
}

#[derive(Debug, Clone)]
pub struct ReadChunk {
    data: Arc<SharedPayloadBuffer>,
    start: usize,
    end: usize,
}

impl ReadChunk {
    fn from_vec(data: Vec<u8>) -> Self {
        let len = data.len();
        Self {
            data: Arc::new(SharedPayloadBuffer::from_unpooled(data)),
            start: 0,
            end: len,
        }
    }

    fn from_shared_range(data: Arc<SharedPayloadBuffer>, start: usize, end: usize) -> Self {
        debug_assert!(start <= end);
        debug_assert!(end <= data.len());
        Self { data, start, end }
    }

    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl AsRef<[u8]> for ReadChunk {
    fn as_ref(&self) -> &[u8] {
        &self.data.buf[self.start..self.end]
    }
}

impl std::ops::Deref for ReadChunk {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
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

impl PayloadBufferPool {
    fn new(ec_config: EcConfig) -> Arc<Self> {
        let default_capacity = segment_payload_buffer_capacity(ec_config);
        let max_cached = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(1);
        Arc::new(Self {
            default_capacity,
            max_cached,
            cached: Mutex::new(Vec::new()),
            #[cfg(test)]
            allocations: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn checkout(self: &Arc<Self>, required_capacity: usize) -> PooledPayloadBuffer {
        let min_capacity = required_capacity.max(self.default_capacity);
        let mut cached = self.cached.lock().unwrap();
        let maybe_idx = cached
            .iter()
            .rposition(|buf| buf.capacity() >= min_capacity);
        let mut buf = maybe_idx.map_or_else(
            || {
                #[cfg(test)]
                self.allocations.fetch_add(1, Ordering::Relaxed);
                Vec::with_capacity(min_capacity)
            },
            |idx| cached.swap_remove(idx),
        );
        drop(cached);
        buf.clear();
        PooledPayloadBuffer {
            pool: Arc::clone(self),
            buf: Some(buf),
        }
    }

    fn recycle(&self, mut buf: Vec<u8>) {
        if buf.capacity() < self.default_capacity {
            return;
        }
        buf.clear();
        let mut cached = self.cached.lock().unwrap();
        if cached.len() < self.max_cached {
            cached.push(buf);
        }
    }

    #[cfg(test)]
    fn allocation_count(&self) -> usize {
        self.allocations.load(Ordering::Relaxed)
    }
}

impl PooledPayloadBuffer {
    fn resize_zeroed(&mut self, len: usize) {
        self.buf.as_mut().unwrap().resize(len, 0);
    }

    fn truncate(&mut self, len: usize) {
        self.buf.as_mut().unwrap().truncate(len);
    }

    fn into_shared(mut self) -> Arc<SharedPayloadBuffer> {
        Arc::new(SharedPayloadBuffer {
            pool: Some(Arc::clone(&self.pool)),
            buf: self.buf.take().unwrap(),
        })
    }
}

impl std::ops::Deref for PooledPayloadBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        self.buf.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for PooledPayloadBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buf.as_mut().unwrap()
    }
}

impl Drop for PooledPayloadBuffer {
    fn drop(&mut self) {
        let Some(buf) = self.buf.take() else {
            return;
        };
        self.pool.recycle(buf);
    }
}

impl SharedPayloadBuffer {
    fn from_unpooled(buf: Vec<u8>) -> Self {
        Self { pool: None, buf }
    }

    fn len(&self) -> usize {
        self.buf.len()
    }
}

impl std::ops::Deref for SharedPayloadBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl Drop for SharedPayloadBuffer {
    fn drop(&mut self) {
        let Some(pool) = self.pool.take() else {
            return;
        };
        pool.recycle(std::mem::take(&mut self.buf));
    }
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

impl EncodeScratchPool {
    fn new(ec_config: EcConfig) -> Self {
        let scratch_len = encode_parity_scratch_len(ec_config);
        let max_cached = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(1);
        Self {
            scratch_len,
            max_cached,
            cached: Mutex::new(Vec::new()),
            #[cfg(test)]
            allocations: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn checkout(&self) -> EncodeScratch<'_> {
        let buf = self.cached.lock().unwrap().pop().unwrap_or_else(|| {
            #[cfg(test)]
            self.allocations.fetch_add(1, Ordering::Relaxed);
            vec![0u8; self.scratch_len]
        });
        EncodeScratch {
            pool: self,
            buf: Some(buf),
        }
    }

    #[cfg(test)]
    fn allocation_count(&self) -> usize {
        self.allocations.load(Ordering::Relaxed)
    }
}

impl EncodeScratch<'_> {
    fn as_mut_slice(&mut self, len: usize) -> &mut [u8] {
        debug_assert!(len <= self.pool.scratch_len);
        &mut self.buf.as_mut().unwrap()[..len]
    }

    fn as_slice(&self, len: usize) -> &[u8] {
        debug_assert!(len <= self.pool.scratch_len);
        &self.buf.as_ref().unwrap()[..len]
    }
}

impl Drop for EncodeScratch<'_> {
    fn drop(&mut self) {
        let Some(buf) = self.buf.take() else {
            return;
        };
        let mut cached = self.pool.cached.lock().unwrap();
        if cached.len() < self.pool.max_cached {
            cached.push(buf);
        }
    }
}

fn encode_parity_scratch_len(ec_config: EcConfig) -> usize {
    let k = ec_config.data_shards as usize;
    let m = ec_config.parity_shards as usize;
    let padded = INTERNAL_SEGMENT_SIZE.div_ceil(k) * k;
    let shard_size = padded / k;
    shard_size.saturating_mul(m)
}

fn segment_payload_buffer_capacity(ec_config: EcConfig) -> usize {
    let k = ec_config.data_shards as usize;
    INTERNAL_SEGMENT_SIZE.div_ceil(k) * k
}

#[derive(Clone)]
struct ReadRuntime {
    storage_node: Arc<SharedStorageNode>,
    ec_codec: Arc<ErasureCodec>,
    ec_config: EcConfig,
    pg_topology: PgTopology,
    payload_buffer_pool: Arc<PayloadBufferPool>,
}

#[derive(Debug, Clone)]
struct SegmentPayloadRecord {
    size: u64,
    segment_crc64: Option<u64>,
    segment_okh: [u8; 16],
    segment_vid: GenerationId,
    shard_pg_id: u32,
    ec_k: u8,
    ec_m: u8,
}

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
}

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
}

enum ReadHandleInner {
    Segments(SegmentListReader),
    Multipart(Box<MultipartReader>),
    TestBuffered(Option<Vec<u8>>),
}

struct PayloadLease {
    runtime: ReadRuntime,
    bucket: String,
    key: String,
    generation_id: GenerationId,
}

impl Drop for PayloadLease {
    fn drop(&mut self) {
        if self.storage_node().release_object_payload_lease(
            &self.bucket,
            &self.key,
            self.generation_id,
        ) == 0
        {
            self.runtime.enqueue_object_payload_reclaim(
                &self.bucket,
                &self.key,
                self.generation_id,
            );
        }
    }
}

impl PayloadLease {
    fn storage_node(&self) -> &SharedStorageNode {
        &self.runtime.storage_node
    }
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
        runtime: ReadRuntime,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
        segments: Vec<SegmentPayloadRecord>,
        expected_size: usize,
        expected_crc64: Option<u64>,
    ) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            lease: Some(runtime.acquire_object_payload_lease(bucket, key, generation_id)),
            trace: observability::current_context(),
            expected_size,
            bytes_emitted: 0,
            expected_crc64,
            crc64: checksum::crc64::Hasher::new(),
            inner: ReadHandleInner::Segments(SegmentListReader {
                runtime,
                bucket: bucket.to_string(),
                key: key.to_string(),
                segments: Self::segment_slices_for_range(
                    segments,
                    0,
                    expected_size.saturating_sub(1),
                    0,
                    None,
                ),
                next_segment_index: 0,
                loaded_segment: None,
            }),
        }
    }

    fn from_segments_range(
        runtime: ReadRuntime,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
        segments: Vec<SegmentPayloadRecord>,
        start: usize,
        end: usize,
    ) -> Self {
        let expected_size = end - start + 1;
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            lease: Some(runtime.acquire_object_payload_lease(bucket, key, generation_id)),
            trace: observability::current_context(),
            expected_size,
            bytes_emitted: 0,
            expected_crc64: None,
            crc64: checksum::crc64::Hasher::new(),
            inner: ReadHandleInner::Segments(SegmentListReader {
                runtime,
                bucket: bucket.to_string(),
                key: key.to_string(),
                segments: Self::segment_slices_for_range(segments, start, end, 0, None),
                next_segment_index: 0,
                loaded_segment: None,
            }),
        }
    }

    fn from_multipart(
        runtime: ReadRuntime,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
        parts: Vec<SnapshottedMultipartPart>,
        expected_size: usize,
    ) -> Self {
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            lease: Some(runtime.acquire_object_payload_lease(bucket, key, generation_id)),
            trace: observability::current_context(),
            expected_size,
            bytes_emitted: 0,
            expected_crc64: None,
            crc64: checksum::crc64::Hasher::new(),
            inner: ReadHandleInner::Multipart(Box::new(MultipartReader {
                runtime,
                bucket: bucket.to_string(),
                key: key.to_string(),
                parts: Self::multipart_ranges_for_range(parts, 0, expected_size.saturating_sub(1)),
                next_part_index: 0,
                current_part: None,
            })),
        }
    }

    fn from_multipart_range(
        runtime: ReadRuntime,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
        parts: Vec<SnapshottedMultipartPart>,
        start: usize,
        end: usize,
    ) -> Self {
        let expected_size = end - start + 1;
        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            lease: Some(runtime.acquire_object_payload_lease(bucket, key, generation_id)),
            trace: observability::current_context(),
            expected_size,
            bytes_emitted: 0,
            expected_crc64: None,
            crc64: checksum::crc64::Hasher::new(),
            inner: ReadHandleInner::Multipart(Box::new(MultipartReader {
                runtime,
                bucket: bucket.to_string(),
                key: key.to_string(),
                parts: Self::multipart_ranges_for_range(parts, start, end),
                next_part_index: 0,
                current_part: None,
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
            "bucket={} key={} target_size={}",
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
) -> Vec<SegmentPayloadRecord> {
    segments
        .into_iter()
        .map(|segment| SegmentPayloadRecord {
            size: segment.size,
            segment_crc64: segment.segment_crc64,
            segment_okh: segment.segment_okh,
            segment_vid: segment.segment_vid,
            shard_pg_id: segment.shard_pg_id,
            ec_k: segment.ec_k,
            ec_m: segment.ec_m,
        })
        .collect()
}

/// Result of a GetObject operation.
#[derive(Debug)]
pub struct GetObjectResult {
    pub body: ReadHandle,
    pub metadata: MetadataBlob,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub version_id: VersionId,
    pub tags: Option<String>,
}

/// Result of a HeadObject operation.
#[derive(Debug)]
pub struct HeadObjectResult {
    pub metadata: MetadataBlob,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub version_id: VersionId,
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
    pub version_id: VersionId,
    pub tags: Option<String>,
    /// Per-part checksum (algorithm + raw bytes).
    pub checksum: Option<RawChecksum>,
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
    pub version_id: VersionId,
    pub object_parts: Option<ObjectPartsInfo>,
}

/// Result of a range GetObject operation (206 Partial Content).
#[derive(Debug)]
pub struct GetObjectRangeResult {
    pub body: ReadHandle,
    pub metadata: MetadataBlob,
    pub etag: String,
    pub size: u64,
    pub last_modified: u64,
    pub range_start: u64,
    pub range_end: u64,
    pub version_id: VersionId,
    pub tags: Option<String>,
}

/// Result of a part-level GetObject operation (206 Partial Content).
#[derive(Debug)]
pub struct GetObjectPartResult {
    pub body: ReadHandle,
    pub metadata: MetadataBlob,
    pub etag: String,
    pub size: u64,
    pub part_size: u64,
    pub last_modified: u64,
    pub part_start: u64,
    pub part_end: u64,
    pub parts_count: u32,
    pub version_id: VersionId,
    pub tags: Option<String>,
    /// Per-part checksum (algorithm + raw bytes).
    pub checksum: Option<RawChecksum>,
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

/// Tagging handling directive for `CopyObject`.
#[derive(Debug)]
pub enum TaggingDirective<'a> {
    Copy,
    Replace(Option<&'a str>),
}

/// Parsed copy-source reference, shared by CopyObject and UploadPartCopy.
#[derive(Debug)]
pub struct CopySource<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<VersionId>,
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
    pub tagging: TaggingDirective<'a>,
    pub requester: Requester<'a>,
    pub acl: PutObjectAcl<'a>,
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
    pub requester: Requester<'a>,
}

/// Request for a PutObject operation (test-only convenience wrapper).
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug)]
pub struct PutObjectRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub data: &'a [u8],
    pub metadata: &'a MetadataBlob,
    pub tags: Option<&'a str>,
    pub cond: &'a WriteCondition,
    pub requester: Requester<'a>,
    pub acl: PutObjectAcl<'a>,
}

/// Authenticated requester context needed by core-side authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Requester<'a> {
    principal: Option<&'a str>,
    #[cfg(test)]
    is_system: bool,
}

impl<'a> Requester<'a> {
    #[must_use]
    pub const fn anonymous() -> Self {
        Self {
            principal: None,
            #[cfg(test)]
            is_system: false,
        }
    }

    #[must_use]
    pub const fn principal(principal: &'a str) -> Self {
        Self {
            principal: Some(principal),
            #[cfg(test)]
            is_system: false,
        }
    }

    #[must_use]
    pub const fn from_principal(principal: Option<&'a str>) -> Self {
        Self {
            principal,
            #[cfg(test)]
            is_system: false,
        }
    }

    #[must_use]
    pub const fn principal_opt(self) -> Option<&'a str> {
        self.principal
    }

    #[cfg(test)]
    #[must_use]
    const fn system() -> Self {
        Self {
            principal: None,
            is_system: true,
        }
    }
}

/// Parsed x-amz-acl value relevant to PutObject authorization rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PutObjectAcl<'a> {
    #[default]
    None,
    Private,
    BucketOwnerFullControl,
    Other(&'a str),
}

/// Parsed bucket ACL value relevant to bucket ACL and ownership-control rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketAcl {
    Private,
    PublicRead,
    PublicReadWrite,
    AuthenticatedRead,
}

/// Object ownership mode relevant to CreateBucket semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketObjectOwnership {
    BucketOwnerEnforced,
    BucketOwnerPreferred,
    ObjectWriter,
}

impl BucketObjectOwnership {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BucketOwnerEnforced => "BucketOwnerEnforced",
            Self::BucketOwnerPreferred => "BucketOwnerPreferred",
            Self::ObjectWriter => "ObjectWriter",
        }
    }
}

/// Request for a CreateBucket operation.
#[derive(Debug)]
pub struct CreateBucketRequest<'a> {
    pub name: &'a str,
    pub requester: Requester<'a>,
    pub acl: BucketAcl,
    pub ownership: BucketObjectOwnership,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketCreateOutcome {
    Created,
    AlreadyOwned,
}

/// Request for a ListBuckets operation.
#[derive(Debug)]
pub struct ListBucketsRequest<'a> {
    pub requester: Requester<'a>,
}

/// Request for a GetObject or HeadObject operation.
#[derive(Debug)]
pub struct GetObjectRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<VersionId>,
    pub cond: &'a ReadCondition,
    pub requester: Requester<'a>,
}

/// Request for a HeadBucket operation.
#[derive(Debug)]
pub struct HeadBucketRequest<'a> {
    pub bucket: &'a str,
    pub requester: Requester<'a>,
}

/// Request for a GetObjectPart or HeadObjectPart operation.
#[derive(Debug)]
pub struct GetObjectPartRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<VersionId>,
    pub part_number: u32,
    pub cond: &'a ReadCondition,
    pub requester: Requester<'a>,
}

/// Request for a GetObjectRange operation.
#[derive(Debug)]
pub struct GetObjectRangeRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<VersionId>,
    pub range: ByteRange,
    pub cond: &'a ReadCondition,
    pub requester: Requester<'a>,
}

/// Request for a DeleteObject operation.
#[derive(Debug)]
pub struct DeleteObjectRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<VersionId>,
    pub cond: &'a DeleteCondition,
    pub requester: Requester<'a>,
}

/// Request for a ListObjectsV2 operation.
#[derive(Debug)]
pub struct ListObjectsV2Request<'a> {
    pub bucket: &'a str,
    pub prefix: Option<&'a str>,
    pub delimiter: Option<&'a str>,
    pub continuation_token: Option<&'a str>,
    pub max_keys: u32,
    pub requester: Requester<'a>,
}

/// Request for a ListObjectVersions operation.
#[derive(Debug)]
pub struct ListObjectVersionsRequest<'a> {
    pub bucket: &'a str,
    pub prefix: Option<&'a str>,
    pub key_marker: Option<&'a str>,
    pub version_id_marker: Option<VersionId>,
    pub max_keys: u32,
    pub requester: Requester<'a>,
}

/// Request for a ListParts operation.
#[derive(Debug)]
pub struct ListPartsRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
    pub part_number_marker: Option<u32>,
    pub max_parts: u32,
    pub requester: Requester<'a>,
}

/// Request for a ListMultipartUploads operation.
#[derive(Debug)]
pub struct ListMultipartUploadsRequest<'a> {
    pub bucket: &'a str,
    pub prefix: Option<&'a str>,
    pub key_marker: Option<&'a str>,
    pub upload_id_marker: Option<&'a str>,
    pub max_uploads: u32,
    pub requester: Requester<'a>,
}

/// A single entry in a batch-delete request, with an already-parsed version ID.
#[derive(Debug)]
pub struct DeleteEntry<'a> {
    pub key: &'a str,
    pub version_id: Option<VersionId>,
}

/// Request for a DeleteObjects (multi-delete) operation.
#[derive(Debug)]
pub struct DeleteObjectsRequest<'a> {
    pub bucket: &'a str,
    pub entries: &'a [DeleteEntry<'a>],
    pub cond: &'a DeleteCondition,
    pub requester: Requester<'a>,
}

/// Request for a DeleteBucket operation.
#[derive(Debug)]
pub struct DeleteBucketRequest<'a> {
    pub name: &'a str,
    pub requester: Requester<'a>,
}

/// Request for an AbortMultipartUpload operation.
#[derive(Debug)]
pub struct AbortMultipartUploadRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
    pub requester: Requester<'a>,
}

/// Request for a CreateMultipartUpload operation.
#[derive(Debug)]
pub struct CreateMultipartUploadRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub metadata: &'a MetadataBlob,
    pub checksum: Option<MultipartChecksumConfig>,
    pub requester: Requester<'a>,
}

/// Request for an UploadPart operation (test-only convenience wrapper).
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug)]
pub struct UploadPartRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
    pub part_number: u32,
    pub data: &'a [u8],
    pub claimed_checksum: Option<&'a ChecksumClaim>,
    pub requester: Requester<'a>,
}

/// Request for a GetObjectAttributes operation.
#[derive(Debug)]
pub struct GetObjectAttributesRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub version_id: Option<VersionId>,
    pub cond: &'a ReadCondition,
    pub want_parts: bool,
    pub part_number_marker: Option<u32>,
    pub max_parts: u32,
    pub requester: Requester<'a>,
}

/// Request for a CompleteMultipartUpload operation.
#[derive(Debug)]
pub struct CompleteMultipartUploadRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
    pub parts: &'a [CompletePart],
    pub claimed_checksum: Option<&'a EncodedChecksumClaim>,
    pub requester: Requester<'a>,
}

/// Request for beginning a streaming PutObject session.
#[derive(Debug)]
pub struct BeginStreamPutRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub requester: Requester<'a>,
    pub acl: PutObjectAcl<'a>,
}

/// Request for beginning a streaming UploadPart session.
#[derive(Debug)]
pub struct BeginStreamPartRequest<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    pub upload_id: &'a str,
    pub part_number: u32,
    pub requester: Requester<'a>,
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
    pub tags: Option<&'a str>,
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
    pub computed_checksum: Option<RawChecksum>,
}

/// Result of a `CopyObject` operation.
#[derive(Debug)]
pub struct CopyObjectResult {
    pub etag: String,
    pub last_modified: u64,
    pub version_id: VersionId,
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
    pub owner_canonical_id: CanonicalUserId,
}

/// Entry in a ListObjectVersions result.
#[derive(Debug, Clone)]
pub struct VersionEntry {
    pub key: String,
    pub version_id: VersionId,
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
    pub next_version_id_marker: Option<VersionId>,
    pub owner_principal: String,
    pub owner_canonical_id: CanonicalUserId,
}

/// Result of a DeleteObject operation.
#[derive(Debug)]
pub struct DeleteObjectResult {
    pub version_id: VersionId,
    pub delete_marker: bool,
}

/// Result entry for a successfully deleted object in a batch delete.
#[derive(Debug)]
pub struct DeletedObject {
    pub key: String,
    pub version_id: VersionId,
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
    pub checksum: Option<RawChecksum>,
}

/// Result of an UploadPartCopy operation.
#[derive(Debug)]
pub struct UploadPartCopyResult {
    pub etag: String,
    pub last_modified: u64,
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
    /// Per-part checksum from the request XML.
    pub checksum: Option<ChecksumClaim>,
}

/// Result of a CompleteMultipartUpload operation.
#[derive(Debug)]
pub struct CompleteMultipartUploadResult {
    pub etag: String,
    pub version_id: VersionId,
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

/// PG guards held while an object metadata snapshot is live.
///
/// Read-side payload access relies on generation-scoped leases, so current
/// object read paths only need the metadata PG guard.
struct ObjectPgGuards<'a> {
    meta: MutexGuard<'a, storage::PgStore>,
}

impl<'a> ObjectPgGuards<'a> {
    fn new(meta: MutexGuard<'a, storage::PgStore>) -> Self {
        Self { meta }
    }

    fn meta(&self) -> &storage::PgStore {
        &self.meta
    }
}

struct LockedReadObject<'a> {
    record: StoredObject,
    pgs: ObjectPgGuards<'a>,
}

#[derive(Debug, Clone)]
struct SnapshottedMultipartPart {
    record: ObjectPartRecord,
    object_offset_start: usize,
    segments: Vec<SegmentPayloadRecord>,
}

#[derive(Debug, Clone)]
enum StaleObjectPayload {
    Segments {
        generation_id: GenerationId,
        segments: Vec<ObjectSegmentRecord>,
    },
    Multipart {
        generation_id: GenerationId,
        parts: Vec<ObjectPartRecord>,
        streaming_segments: Vec<MultipartPartSegmentRecord>,
    },
}

#[cfg(test)]
#[derive(Default, Clone)]
struct ReclamationTestHooks {
    target: Option<(String, String)>,
    after_multipart_snapshot: Option<Arc<dyn Fn() + Send + Sync>>,
    after_multipart_delete_metadata: Option<Arc<dyn Fn() + Send + Sync>>,
    after_object_segments_first_segment: Option<Arc<dyn Fn() + Send + Sync>>,
    after_object_segments_delete_metadata: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[cfg(test)]
static RECLAMATION_TEST_HOOKS: OnceLock<Mutex<ReclamationTestHooks>> = OnceLock::new();
#[cfg(test)]
static RECLAMATION_TEST_SERIAL: OnceLock<Mutex<()>> = OnceLock::new();

#[cfg(test)]
struct ReclamationTestHookGuard;

#[cfg(test)]
impl Drop for ReclamationTestHookGuard {
    fn drop(&mut self) {
        let hooks =
            RECLAMATION_TEST_HOOKS.get_or_init(|| Mutex::new(ReclamationTestHooks::default()));
        *hooks.lock().unwrap() = ReclamationTestHooks::default();
    }
}

/// The coordinator ties together EC, storage, and metadata.
struct ReclaimSweeper {
    storage_node: Arc<SharedStorageNode>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for ReclaimSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.storage_node.wake_reclaim_workers();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub struct Coordinator {
    storage_node: Arc<SharedStorageNode>,
    pg_topology: PgTopology,
    ec_codec: Arc<ErasureCodec>,
    ec_config: EcConfig,
    encode_scratch_pool: EncodeScratchPool,
    payload_buffer_pool: Arc<PayloadBufferPool>,
    region: String,
    _reclaim_sweeper: ReclaimSweeper,
}

#[cfg(test)]
fn install_reclamation_test_hooks(hooks: ReclamationTestHooks) -> ReclamationTestHookGuard {
    let slot = RECLAMATION_TEST_HOOKS.get_or_init(|| Mutex::new(ReclamationTestHooks::default()));
    *slot.lock().unwrap() = hooks;
    ReclamationTestHookGuard
}

#[cfg(test)]
fn maybe_run_multipart_snapshot_hook(bucket: &str, key: &str) {
    let hooks = RECLAMATION_TEST_HOOKS
        .get_or_init(|| Mutex::new(ReclamationTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks
        .target
        .as_ref()
        .is_some_and(|(b, k)| b == bucket && k == key)
    {
        if let Some(hook) = hooks.after_multipart_snapshot {
            hook();
        }
    }
}

#[cfg(test)]
fn maybe_run_multipart_delete_metadata_hook(bucket: &str, key: &str) {
    let hooks = RECLAMATION_TEST_HOOKS
        .get_or_init(|| Mutex::new(ReclamationTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks
        .target
        .as_ref()
        .is_some_and(|(b, k)| b == bucket && k == key)
    {
        if let Some(hook) = hooks.after_multipart_delete_metadata {
            hook();
        }
    }
}

#[cfg(test)]
fn maybe_run_object_segments_first_segment_hook(bucket: &str, key: &str) {
    let hooks = RECLAMATION_TEST_HOOKS
        .get_or_init(|| Mutex::new(ReclamationTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks
        .target
        .as_ref()
        .is_some_and(|(b, k)| b == bucket && k == key)
    {
        if let Some(hook) = hooks.after_object_segments_first_segment {
            hook();
        }
    }
}

#[cfg(test)]
fn maybe_run_object_segments_delete_metadata_hook(bucket: &str, key: &str) {
    let hooks = RECLAMATION_TEST_HOOKS
        .get_or_init(|| Mutex::new(ReclamationTestHooks::default()))
        .lock()
        .unwrap()
        .clone();
    if hooks
        .target
        .as_ref()
        .is_some_and(|(b, k)| b == bucket && k == key)
    {
        if let Some(hook) = hooks.after_object_segments_delete_metadata {
            hook();
        }
    }
}

impl ReadRuntime {
    fn enqueue_object_payload_reclaim(&self, bucket: &str, key: &str, generation_id: GenerationId) {
        self.storage_node
            .enqueue_object_payload_reclaim(bucket, key, generation_id);
    }

    fn enqueue_bucket_delete_finalize(&self, bucket: &str) {
        self.storage_node.enqueue_bucket_delete_finalize(bucket);
    }

    fn acquire_object_payload_lease(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> PayloadLease {
        self.storage_node
            .acquire_object_payload_lease(bucket, key, generation_id);
        PayloadLease {
            runtime: self.clone(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            generation_id,
        }
    }

    fn try_reclaim_object_payload(
        &self,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
    ) -> Result<(), ServerError> {
        enum ReclaimPayload {
            Simple(storage::SimplePayloadReclaimRecord),
            Segments(storage::ObjectSegmentsReclaimRecord),
            Multipart(storage::MultipartReclaimRecord),
        }

        let delete_ec_shards = |storage_node: &Arc<SharedStorageNode>,
                                shard_pg_id: u32,
                                okh: &[u8; 16],
                                generation_id: GenerationId,
                                ec: EcShape|
         -> Result<(), ServerError> {
            let shard_pg = storage_node.get_pg(shard_pg_id)?;
            let total = ec.k as usize + ec.m as usize;
            for i in 0..total {
                let shard_key = ShardKey::new(okh, generation_id.get(), i as u8);
                shard_pg.delete_shard(&shard_key)?;
            }
            Ok(())
        };

        if self
            .storage_node
            .object_payload_lease_count(bucket, key, generation_id)
            != 0
        {
            return Ok(());
        }

        let meta_pg_id = self.pg_topology.object_pg(bucket, key);
        let reclaim = {
            let meta_guard = self.storage_node.get_pg(meta_pg_id)?;
            let meta_pg: &storage::PgStore = &meta_guard;

            if self
                .storage_node
                .object_payload_lease_count(bucket, key, generation_id)
                != 0
            {
                return Ok(());
            }

            if let Some(reclaim) = meta_pg.get_simple_payload_reclaim(bucket, key, generation_id)? {
                Some(ReclaimPayload::Simple(reclaim))
            } else if let Some(reclaim) =
                meta_pg.get_object_segments_reclaim(bucket, key, generation_id)?
            {
                Some(ReclaimPayload::Segments(reclaim))
            } else {
                meta_pg
                    .get_multipart_reclaim(bucket, key, generation_id)?
                    .map(ReclaimPayload::Multipart)
            }
        };

        let Some(reclaim) = reclaim else {
            return Ok(());
        };

        if self
            .storage_node
            .object_payload_lease_count(bucket, key, generation_id)
            != 0
        {
            return Ok(());
        }

        match &reclaim {
            ReclaimPayload::Simple(reclaim) => {
                let shard_pg_id = self.pg_topology.shard_pg(bucket, key, generation_id.get());
                let okh = object_key_hash(bucket, key);
                delete_ec_shards(
                    &self.storage_node,
                    shard_pg_id,
                    &okh,
                    generation_id,
                    reclaim.ec,
                )?;
            }
            ReclaimPayload::Segments(reclaim) => {
                for segment in &reclaim.segments {
                    delete_ec_shards(
                        &self.storage_node,
                        segment.shard_pg_id,
                        &segment.segment_okh,
                        segment.segment_vid,
                        segment.ec,
                    )?;
                }
            }
            ReclaimPayload::Multipart(reclaim) => {
                for part in &reclaim.parts {
                    match part {
                        MultipartReclaimPartRecord::ShardSet {
                            part_okh,
                            part_vid,
                            shard_pg_id,
                            ec,
                            ..
                        } => {
                            delete_ec_shards(
                                &self.storage_node,
                                *shard_pg_id,
                                part_okh,
                                *part_vid,
                                *ec,
                            )?;
                        }
                        MultipartReclaimPartRecord::Segments { segments, .. } => {
                            for segment in segments {
                                delete_ec_shards(
                                    &self.storage_node,
                                    segment.shard_pg_id,
                                    &segment.segment_okh,
                                    segment.segment_vid,
                                    segment.ec,
                                )?;
                            }
                        }
                    }
                }
            }
        }

        let meta_guard = self.storage_node.get_pg(meta_pg_id)?;
        let meta_pg: &storage::PgStore = &meta_guard;
        if self
            .storage_node
            .object_payload_lease_count(bucket, key, generation_id)
            != 0
        {
            return Ok(());
        }

        match reclaim {
            ReclaimPayload::Simple(_) => {
                meta_pg.delete_simple_payload_reclaim(bucket, key, generation_id)?;
            }
            ReclaimPayload::Segments(_) => {
                meta_pg.delete_object_segments_reclaim(bucket, key, generation_id)?;
            }
            ReclaimPayload::Multipart(_) => {
                meta_pg.delete_multipart_reclaim(bucket, key, generation_id)?;
            }
        }
        self.enqueue_bucket_delete_finalize(bucket);
        Ok(())
    }

    fn try_finalize_bucket_delete(&self, bucket: &str) -> Result<(), ServerError> {
        let _bucket_guard = self.storage_node.lock_bucket(bucket);
        let bucket_pg_id = self.pg_topology.bucket_pg(bucket);
        {
            let bucket_pg = self.storage_node.get_pg(bucket_pg_id)?;
            let info = match bucket_pg.head_bucket_raw(bucket) {
                Ok(info) => info,
                Err(storage::MetadataError::BucketNotFound { .. }) => return Ok(()),
                Err(other) => return Err(ServerError::Metadata(other)),
            };
            if info.state != BucketState::Deleting {
                return Ok(());
            }
        }

        let mut found_visible_data = false;
        let mut found_reclaim_root = false;
        let mut reclaim_roots = Vec::new();
        self.pg_topology.for_each_pg(|pg_id| {
            let pg = self.storage_node.get_pg(pg_id)?;
            let versions = pg.list_object_versions(&ListObjectVersionsReq {
                bucket: BucketName::from(bucket),
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                max_keys: 1,
            })?;
            if !versions.versions.is_empty() {
                found_visible_data = true;
                return Ok(());
            }
            let uploads = pg.list_multipart_uploads(&ListMultipartUploadsReq {
                bucket: BucketName::from(bucket),
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1,
            })?;
            if !uploads.uploads.is_empty() {
                found_visible_data = true;
                return Ok(());
            }
            if let Some(root) = pg.get_bucket_payload_reclaim_root(bucket)? {
                found_reclaim_root = true;
                reclaim_roots.push(root);
            }
            Ok::<(), ServerError>(())
        })?;

        if found_visible_data {
            return Ok(());
        }
        for root in &reclaim_roots {
            if self.storage_node.object_payload_lease_count(
                root.bucket.as_str(),
                root.key.as_str(),
                root.generation_id,
            ) == 0
            {
                self.enqueue_object_payload_reclaim(
                    root.bucket.as_str(),
                    root.key.as_str(),
                    root.generation_id,
                );
            }
        }
        if found_reclaim_root || self.storage_node.bucket_object_payload_lease_count(bucket) != 0 {
            return Ok(());
        }

        let bucket_pg = self.storage_node.get_pg(bucket_pg_id)?;
        match bucket_pg.delete_bucket(bucket) {
            Ok(()) => Ok(()),
            Err(storage::MetadataError::BucketNotFound { .. }) => Ok(()),
            Err(other) => Err(ServerError::Metadata(other)),
        }
    }

    fn read_segment_payload(
        &self,
        segment: &SegmentPayloadRecord,
    ) -> Result<Arc<SharedPayloadBuffer>, ServerError> {
        let k = segment.ec_k as usize;
        let m = segment.ec_m as usize;
        let padded = (segment.size as usize).div_ceil(k) * k;
        let shard_size = padded / k;

        if shard_size == 0 {
            return Ok(Arc::new(SharedPayloadBuffer::from_unpooled(Vec::new())));
        }

        if let Some(buf) = self.try_read_segment_payload_direct(segment, k, shard_size, padded)? {
            return Ok(buf.into_shared());
        }

        self.read_segment_payload_locked(segment, k, m, padded, shard_size)
            .map(PooledPayloadBuffer::into_shared)
    }

    fn try_read_segment_payload_direct(
        &self,
        segment: &SegmentPayloadRecord,
        k: usize,
        shard_size: usize,
        padded: usize,
    ) -> Result<Option<PooledPayloadBuffer>, ServerError> {
        let Some(expected_crc64) = segment.segment_crc64 else {
            return Ok(None);
        };

        let mut buf = self.payload_buffer_pool.checkout(padded);
        buf.resize_zeroed(padded);
        for i in 0..k {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            let start = i * shard_size;
            let end = start + shard_size;
            match self.storage_node.read_shard_file_into(
                segment.shard_pg_id,
                &shard_key,
                &mut buf[start..end],
            ) {
                Ok(()) => {}
                Err(storage::StoreError::PgNotFound { pg_id }) => {
                    return Err(ServerError::Store(storage::StoreError::PgNotFound {
                        pg_id,
                    }));
                }
                Err(_) => return Ok(None),
            }
        }

        buf.truncate(segment.size as usize);
        let actual_crc64 = checksum::crc64::checksum(&buf);
        if actual_crc64 != expected_crc64 {
            return Ok(None);
        }
        Ok(Some(buf))
    }

    fn read_segment_payload_locked(
        &self,
        segment: &SegmentPayloadRecord,
        k: usize,
        m: usize,
        padded: usize,
        shard_size: usize,
    ) -> Result<PooledPayloadBuffer, ServerError> {
        let pg = self.storage_node.get_pg(segment.shard_pg_id)?;

        let mut all_shards = vec![None; k + m];
        let mut present_count = 0;

        let read_shard = |i: usize,
                          all_shards: &mut [Option<Vec<u8>>],
                          present_count: &mut usize| {
            let shard_key = ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
            if let Ok(sd) = pg.read_shard(&shard_key) {
                all_shards[i] = Some(sd.data);
                *present_count += 1;
            }
        };

        for i in 0..k {
            read_shard(i, &mut all_shards, &mut present_count);
        }

        if present_count < k {
            for i in k..(k + m) {
                if present_count >= k {
                    break;
                }
                read_shard(i, &mut all_shards, &mut present_count);
            }
        }

        if present_count < k {
            return Err(ServerError::Store(storage::StoreError::NotFound));
        }

        let mut recovered = None;
        let mut recovered_ranges = vec![None; k];

        if !(0..k).all(|i| all_shards[i].is_some()) {
            let missing_needed: Vec<usize> = (0..k).filter(|&i| all_shards[i].is_none()).collect();

            let present_indices: Vec<usize> =
                (0..(k + m)).filter(|&i| all_shards[i].is_some()).collect();
            let present_refs: Vec<&[u8]> = present_indices
                .iter()
                .map(|&i| all_shards[i].as_ref().unwrap().as_slice())
                .collect();

            let tmp_codec;
            let codec = if segment.ec_k == self.ec_config.data_shards
                && segment.ec_m == self.ec_config.parity_shards
            {
                self.ec_codec.as_ref()
            } else {
                let ec_config = EcConfig::new(segment.ec_k, segment.ec_m)?;
                tmp_codec = ErasureCodec::new(ec_config)?;
                &tmp_codec
            };

            let recovered_len = missing_needed.len() * shard_size;
            let mut recovered_buf = self.payload_buffer_pool.checkout(recovered_len);
            recovered_buf.resize_zeroed(recovered_len);
            let mut output_refs: Vec<&mut [u8]> = recovered_buf
                .chunks_mut(shard_size)
                .take(missing_needed.len())
                .collect();

            codec.reconstruct(
                &present_indices,
                &present_refs,
                &missing_needed,
                &mut output_refs,
            )?;

            for (slot, &missing_idx) in missing_needed.iter().enumerate() {
                let start = slot * shard_size;
                recovered_ranges[missing_idx] = Some((start, start + shard_size));
            }
            recovered = Some(recovered_buf);
        }

        let mut buf = self.payload_buffer_pool.checkout(padded);
        buf.resize_zeroed(0);
        for (idx, shard) in all_shards.iter().take(k).enumerate() {
            if let Some(shard) = shard.as_ref() {
                buf.extend_from_slice(shard);
            } else if let Some((start, end)) = recovered_ranges[idx] {
                buf.extend_from_slice(&recovered.as_ref().unwrap()[start..end]);
            } else {
                unreachable!("missing reconstructed shard for data index {idx}");
            }
        }
        buf.truncate(segment.size as usize);
        if let Some(expected_crc64) = segment.segment_crc64 {
            let actual_crc64 = checksum::crc64::checksum(&buf);
            if actual_crc64 != expected_crc64 {
                return Err(ServerError::Store(storage::StoreError::IntegrityError {
                    expected: expected_crc64,
                    actual: actual_crc64,
                }));
            }
        }
        Ok(buf)
    }
}

impl SegmentListReader {
    fn next_chunk(&mut self, target_size: usize) -> Result<Option<ReadChunk>, ServerError> {
        loop {
            if let Some((loaded, offset, end_offset)) = &mut self.loaded_segment {
                if *offset < *end_offset {
                    let end = (*offset + target_size).min(*end_offset);
                    let out = ReadChunk::from_shared_range(Arc::clone(loaded), *offset, end);
                    if end == *end_offset {
                        self.loaded_segment = None;
                    } else {
                        *offset = end;
                    }
                    return Ok(Some(out));
                }
                self.loaded_segment = None;
            }

            if self.next_segment_index >= self.segments.len() {
                return Ok(None);
            }

            let slice = self.segments[self.next_segment_index].clone();
            if let Some(trace) = observability::current_context() {
                let read_object_offset_start =
                    slice.segment_object_offset_start + slice.start_offset;
                let read_object_offset_end_exclusive =
                    slice.segment_object_offset_start + slice.end_offset;
                let read_object_offset_len =
                    read_object_offset_end_exclusive - read_object_offset_start;
                let read_segment_offset_len = slice.end_offset - slice.start_offset;
                if let Some(part_layout) = slice.part_number.zip(slice.part_order).zip(
                    slice
                        .part_object_offset_start
                        .zip(slice.part_object_offset_end_exclusive),
                ) {
                    let (
                        (part_number, part_order),
                        (part_object_offset_start, part_object_offset_end_exclusive),
                    ) = part_layout;
                    let _ = observability::event_in_context(
                        &trace,
                        TRACE_TARGET,
                        "read_segment_layout",
                        Some(format_args!(
                            "bucket={} key={} part_order={} part_number={} part_object_offset_start={} part_object_offset_len={} part_object_offset_end_exclusive={} segment_index={} segment_size={} segment_object_offset_start={} segment_object_offset_end_exclusive={} read_object_offset_start={} read_object_offset_len={} read_object_offset_end_exclusive={} read_segment_offset_start={} read_segment_offset_len={} read_segment_offset_end_exclusive={} shard_pg_id={} ec_k={} ec_m={}",
                            self.bucket,
                            self.key,
                            part_order,
                            part_number,
                            part_object_offset_start,
                            part_object_offset_end_exclusive - part_object_offset_start,
                            part_object_offset_end_exclusive,
                            slice.segment_index,
                            slice.payload.size,
                            slice.segment_object_offset_start,
                            slice.segment_object_offset_end_exclusive,
                            read_object_offset_start,
                            read_object_offset_len,
                            read_object_offset_end_exclusive,
                            slice.start_offset,
                            read_segment_offset_len,
                            slice.end_offset,
                            slice.payload.shard_pg_id,
                            slice.payload.ec_k,
                            slice.payload.ec_m,
                        )),
                    );
                } else {
                    let _ = observability::event_in_context(
                        &trace,
                        TRACE_TARGET,
                        "read_segment_layout",
                        Some(format_args!(
                            "bucket={} key={} segment_index={} segment_size={} segment_object_offset_start={} segment_object_offset_end_exclusive={} read_object_offset_start={} read_object_offset_len={} read_object_offset_end_exclusive={} read_segment_offset_start={} read_segment_offset_len={} read_segment_offset_end_exclusive={} shard_pg_id={} ec_k={} ec_m={}",
                            self.bucket,
                            self.key,
                            slice.segment_index,
                            slice.payload.size,
                            slice.segment_object_offset_start,
                            slice.segment_object_offset_end_exclusive,
                            read_object_offset_start,
                            read_object_offset_len,
                            read_object_offset_end_exclusive,
                            slice.start_offset,
                            read_segment_offset_len,
                            slice.end_offset,
                            slice.payload.shard_pg_id,
                            slice.payload.ec_k,
                            slice.payload.ec_m,
                        )),
                    );
                }
            }

            let data = self
                .runtime
                .read_segment_payload(&slice.payload)
                .map_err(|e| match e {
                    ServerError::Store(storage::StoreError::NotFound) => {
                        ServerError::ObjectNotFound {
                            bucket: self.bucket.clone(),
                            key: self.key.clone(),
                        }
                    }
                    other => other,
                })?;
            self.next_segment_index += 1;
            self.loaded_segment = Some((data, slice.start_offset, slice.end_offset));
            #[cfg(test)]
            if self.next_segment_index == 1 {
                maybe_run_object_segments_first_segment_hook(&self.bucket, &self.key);
            }
        }
    }
}

impl MultipartReader {
    fn next_chunk(&mut self, target_size: usize) -> Result<Option<ReadChunk>, ServerError> {
        loop {
            if let Some(current) = &mut self.current_part {
                let chunk = current.next_chunk(target_size)?;
                if chunk.is_some() {
                    return Ok(chunk);
                }
                self.current_part = None;
            }

            if self.next_part_index >= self.parts.len() {
                return Ok(None);
            }

            let part = self.parts[self.next_part_index].clone();
            self.next_part_index += 1;
            if let Some(trace) = observability::current_context() {
                let _ = observability::event_in_context(
                    &trace,
                    TRACE_TARGET,
                    "read_multipart_part_layout",
                    Some(format_args!(
                        "bucket={} key={} part_order={} part_number={} part_object_offset_start={} part_object_offset_len={} part_object_offset_end_exclusive={} segment_count={}",
                        self.bucket,
                        self.key,
                        part.layout.part_order,
                        part.layout.part_number,
                        part.layout.object_offset_start,
                        part.layout.object_offset_end_exclusive - part.layout.object_offset_start,
                        part.layout.object_offset_end_exclusive,
                        part.segments.len(),
                    )),
                );
            }
            self.current_part = Some(SegmentListReader {
                runtime: self.runtime.clone(),
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                segments: part.segments,
                next_segment_index: 0,
                loaded_segment: None,
            });
        }
    }
}

impl Coordinator {
    fn requester_can_bucket_admin(requester: Requester<'_>, owner_principal: &str) -> bool {
        #[cfg(test)]
        if requester.is_system {
            return true;
        }

        requester.principal_opt() == Some(owner_principal)
    }

    fn requester_can_object_write(
        requester: Requester<'_>,
        owner_principal: &str,
        public_write: bool,
    ) -> bool {
        #[cfg(test)]
        if requester.is_system {
            return true;
        }

        requester.principal_opt() == Some(owner_principal) || public_write
    }

    fn requester_can_read_bucket(
        requester: Requester<'_>,
        owner_principal: &str,
        public_read: bool,
    ) -> bool {
        #[cfg(test)]
        if requester.is_system {
            return true;
        }

        requester.principal_opt() == Some(owner_principal) || public_read
    }

    // Ownership-controls XML is stored in canonical form by the HTTP layer.
    fn is_bucket_owner_enforced(config_xml: Option<&str>) -> bool {
        config_xml.is_some_and(|xml| {
            xml.contains("<ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>")
        })
    }

    // Public-access-block XML is stored in canonical form by the HTTP layer.
    fn ignores_public_acls(config_xml: Option<&str>) -> bool {
        config_xml.is_some_and(|xml| xml.contains("<IgnorePublicAcls>true</IgnorePublicAcls>"))
    }

    fn blocks_public_acls(config_xml: Option<&str>) -> bool {
        config_xml.is_some_and(|xml| xml.contains("<BlockPublicAcls>true</BlockPublicAcls>"))
    }

    fn effective_public_read(bucket: &BucketSummary) -> bool {
        bucket.public_read && !Self::ignores_public_acls(bucket.public_access_block.as_deref())
    }

    fn effective_public_write(bucket: &BucketSummary) -> bool {
        bucket.public_write && !Self::ignores_public_acls(bucket.public_access_block.as_deref())
    }

    fn requester_principal_required(requester: Requester<'_>) -> Result<&str, ServerError> {
        requester.principal_opt().ok_or(ServerError::AccessDenied)
    }

    fn ownership_controls_xml(ownership: BucketObjectOwnership) -> String {
        format!(
            "<OwnershipControls><Rule><ObjectOwnership>{}</ObjectOwnership></Rule></OwnershipControls>",
            ownership.as_str()
        )
    }

    fn now_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn authorize_bucket_read_requester(
        &self,
        requester: Requester<'_>,
        bucket: &str,
    ) -> Result<BucketSummary, ServerError> {
        let info = self.head_bucket(bucket)?;
        if Self::requester_can_read_bucket(
            requester,
            &info.owner_principal,
            Self::effective_public_read(&info),
        ) {
            Ok(info)
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    fn authorize_bucket_admin_requester(
        &self,
        requester: Requester<'_>,
        bucket: &str,
    ) -> Result<BucketSummary, ServerError> {
        let info = self.head_bucket(bucket)?;
        if Self::requester_can_bucket_admin(requester, &info.owner_principal) {
            Ok(info)
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    fn authorize_object_write_requester(
        &self,
        requester: Requester<'_>,
        bucket: &str,
    ) -> Result<BucketSummary, ServerError> {
        let info = self.head_bucket(bucket)?;
        if Self::requester_can_object_write(
            requester,
            &info.owner_principal,
            Self::effective_public_write(&info),
        ) {
            Ok(info)
        } else {
            Err(ServerError::AccessDenied)
        }
    }

    fn bucket_summary(info: BucketInfo) -> BucketSummary {
        BucketSummary {
            name: info.name.into_string(),
            owner_principal: info.owner_principal,
            owner_canonical_id: info.owner_canonical_id,
            created_at: info.created_at,
            public_read: info.public_read,
            public_write: info.public_write,
            versioning: info.versioning,
            public_access_block: info.public_access_block,
            ownership_controls: info.ownership_controls,
        }
    }

    /// Create a new coordinator.
    pub fn new(
        storage_node: Arc<SharedStorageNode>,
        ec_config: EcConfig,
        region: String,
    ) -> Result<Self, ServerError> {
        let ec_codec = Arc::new(ErasureCodec::new(ec_config)?);
        let pg_topology = PgTopology::new(storage_node.pg_ids()).map_err(|reason| {
            ServerError::InternalError {
                reason: reason.to_string(),
            }
        })?;
        let payload_buffer_pool = PayloadBufferPool::new(ec_config);
        let read_runtime = ReadRuntime {
            storage_node: Arc::clone(&storage_node),
            ec_codec: Arc::clone(&ec_codec),
            ec_config,
            pg_topology: pg_topology.clone(),
            payload_buffer_pool: Arc::clone(&payload_buffer_pool),
        };
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker_node = Arc::clone(&storage_node);
        let handle = std::thread::Builder::new()
            .name("argmin-reclaim".to_string())
            .spawn(move || {
                while let Some(work) = worker_node.wait_for_reclaim_work(&worker_stop) {
                    match work {
                        ReclaimWorkItem::ObjectPayload((bucket, key, generation_id)) => {
                            let _ = read_runtime.try_reclaim_object_payload(
                                &bucket,
                                &key,
                                generation_id,
                            );
                        }
                        ReclaimWorkItem::BucketDelete(bucket) => {
                            let _ = read_runtime.try_finalize_bucket_delete(&bucket);
                        }
                    }
                }
            })
            .map_err(|e| ServerError::InternalError {
                reason: format!("failed to start reclaim worker: {e}"),
            })?;
        let sweeper_storage_node = Arc::clone(&storage_node);
        Ok(Self {
            storage_node,
            pg_topology,
            ec_codec,
            ec_config,
            encode_scratch_pool: EncodeScratchPool::new(ec_config),
            payload_buffer_pool,
            region,
            _reclaim_sweeper: ReclaimSweeper {
                storage_node: sweeper_storage_node,
                stop,
                handle: Some(handle),
            },
        })
    }

    fn read_runtime(&self) -> ReadRuntime {
        ReadRuntime {
            storage_node: Arc::clone(&self.storage_node),
            ec_codec: Arc::clone(&self.ec_codec),
            ec_config: self.ec_config,
            pg_topology: self.pg_topology.clone(),
            payload_buffer_pool: Arc::clone(&self.payload_buffer_pool),
        }
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

    fn shard_pg_id_raw(&self, bucket: &str, key: &str, generation: u64) -> u32 {
        self.pg_topology.shard_pg(bucket, key, generation)
    }

    #[cfg(test)]
    fn shard_pg_id(&self, bucket: &str, key: &str, generation_id: GenerationId) -> u32 {
        self.shard_pg_id_raw(bucket, key, generation_id.get())
    }

    fn get_bucket_pg(&self, bucket: &str) -> Result<MutexGuard<'_, storage::PgStore>, ServerError> {
        let pg_id = self.bucket_pg_id(bucket);
        Ok(self.storage_node.get_pg(pg_id)?)
    }

    // ── Bucket operations ─────────────────────────────────────────────

    pub fn create_bucket(&self, name: &str) -> Result<(), ServerError> {
        self.create_bucket_for_owner("default-owner", name, false)
    }

    pub fn create_bucket_for_requester(
        &self,
        req: &CreateBucketRequest<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::create_bucket_for_requester",
            "bucket={}",
            req.name
        );
        let owner_principal = Self::requester_principal_required(req.requester)?;
        let (public_read, public_write) = match req.acl {
            BucketAcl::Private => (false, false),
            BucketAcl::PublicRead => (true, false),
            BucketAcl::PublicReadWrite => (true, true),
            BucketAcl::AuthenticatedRead => {
                return Err(ServerError::NotImplemented {
                    feature: "authenticated-read ACL".to_string(),
                });
            }
        };

        if req.ownership == BucketObjectOwnership::BucketOwnerEnforced
            && (public_read || public_write)
        {
            return Err(ServerError::InvalidBucketAclWithObjectOwnership);
        }

        let create_outcome = self.create_bucket_for_owner_with_acl(
            owner_principal,
            req.name,
            public_read,
            public_write,
        )?;
        match create_outcome {
            BucketCreateOutcome::Created => self.put_bucket_ownership_controls(
                req.name,
                &Self::ownership_controls_xml(req.ownership),
                Requester::principal(owner_principal),
            ),
            BucketCreateOutcome::AlreadyOwned => Ok(()),
        }
    }

    pub fn create_bucket_for_owner(
        &self,
        owner_principal: &str,
        name: &str,
        public_read: bool,
    ) -> Result<(), ServerError> {
        self.create_bucket_for_owner_with_acl(owner_principal, name, public_read, false)?;
        Ok(())
    }

    fn create_bucket_for_owner_with_acl(
        &self,
        owner_principal: &str,
        name: &str,
        public_read: bool,
        public_write: bool,
    ) -> Result<BucketCreateOutcome, ServerError> {
        let _bucket_guard = self.storage_node.lock_bucket(name);
        let bucket_pg = self.get_bucket_pg(name)?;
        let owner_canonical_id = CanonicalUserId::from_principal(owner_principal);
        match bucket_pg.create_bucket(
            name,
            owner_principal,
            &owner_canonical_id,
            public_read,
            public_write,
        ) {
            Ok(()) => Ok(BucketCreateOutcome::Created),
            Err(storage::MetadataError::BucketAlreadyExists) => {
                let existing = bucket_pg.head_bucket_raw(name).map_err(|e| match e {
                    storage::MetadataError::BucketNotFound { name } => {
                        ServerError::BucketNotFound {
                            name: name.to_string(),
                        }
                    }
                    other => ServerError::Metadata(other),
                })?;
                if existing.state == BucketState::Active
                    && existing.owner_principal == owner_principal
                {
                    Ok(BucketCreateOutcome::AlreadyOwned)
                } else {
                    Err(ServerError::BucketAlreadyExists)
                }
            }
            Err(other) => Err(ServerError::Metadata(other)),
        }
    }

    pub fn delete_bucket(&self, req: &DeleteBucketRequest<'_>) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket",
            "bucket={}",
            req.name
        );
        let name = req.name;
        let _bucket_guard = self.storage_node.lock_bucket(name);
        let _bucket_info = self.authorize_bucket_admin_requester(req.requester, name)?;

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
        bucket_pg.mark_bucket_deleting(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })?;
        self.read_runtime().enqueue_bucket_delete_finalize(name);
        Ok(())
    }

    pub fn head_bucket(&self, name: &str) -> Result<BucketSummary, ServerError> {
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg
            .head_bucket(name)
            .map(Self::bucket_summary)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
    }

    pub fn head_bucket_for_requester(
        &self,
        req: &HeadBucketRequest<'_>,
    ) -> Result<BucketSummary, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::head_bucket_for_requester",
            "bucket={}",
            req.bucket
        );
        self.authorize_bucket_read_requester(req.requester, req.bucket)
    }

    pub fn list_buckets(&self) -> Result<Vec<BucketSummary>, ServerError> {
        self.list_buckets_for_owner("default-owner")
    }

    pub fn list_buckets_for_requester(
        &self,
        req: &ListBucketsRequest<'_>,
    ) -> Result<Vec<BucketSummary>, ServerError> {
        observability::trace_scope!(TRACE_TARGET, "Coordinator::list_buckets_for_requester");
        self.list_buckets_for_owner(Self::requester_principal_required(req.requester)?)
    }

    pub fn list_buckets_for_owner(
        &self,
        owner_principal: &str,
    ) -> Result<Vec<BucketSummary>, ServerError> {
        let mut out = Vec::new();
        self.pg_topology.for_each_pg(|pg_id| {
            let pg = self.storage_node.get_pg(pg_id)?;
            let mut buckets = pg.list_buckets(owner_principal)?;
            out.extend(buckets.drain(..).map(Self::bucket_summary));
            Ok::<(), ServerError>(())
        })?;
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    pub fn put_bucket_versioning(
        &self,
        name: &str,
        state: BucketVersioningState,
        requester: Requester<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_versioning",
            "bucket={} state={:?}",
            name,
            state
        );
        let _bucket_info = self.authorize_bucket_admin_requester(requester, name)?;
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg
            .put_bucket_versioning(name, state)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                storage::MetadataError::InvalidVersioningTransition { from, to } => {
                    ServerError::InvalidRequest {
                        reason: format!("invalid versioning transition from {from:?} to {to:?}"),
                    }
                }
                other => ServerError::Metadata(other),
            })
    }

    pub fn get_bucket_versioning(
        &self,
        name: &str,
        requester: Requester<'_>,
    ) -> Result<BucketVersioningState, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_versioning",
            "bucket={}",
            name
        );
        let info = self.authorize_bucket_read_requester(requester, name)?;
        Ok(info.versioning)
    }

    pub fn put_bucket_cors(
        &self,
        name: &str,
        config: &str,
        requester: Requester<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_cors",
            "bucket={} bytes={}",
            name,
            config.len()
        );
        let _bucket_info = self.authorize_bucket_admin_requester(requester, name)?;
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

    pub fn get_bucket_cors_unchecked(&self, name: &str) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_cors_unchecked",
            "bucket={}",
            name
        );
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg.get_bucket_cors(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })
    }

    pub fn get_bucket_cors(
        &self,
        name: &str,
        requester: Requester<'_>,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_cors",
            "bucket={}",
            name
        );
        let _bucket_info = self.authorize_bucket_read_requester(requester, name)?;
        self.get_bucket_cors_unchecked(name)
    }

    pub fn delete_bucket_cors(
        &self,
        name: &str,
        requester: Requester<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_cors",
            "bucket={}",
            name
        );
        let _bucket_info = self.authorize_bucket_admin_requester(requester, name)?;
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg.delete_bucket_cors(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })
    }

    // ── Bucket tagging ────────────────────────────────────────────────

    pub fn put_bucket_tags(
        &self,
        name: &str,
        tags: &str,
        requester: Requester<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_tags",
            "bucket={} bytes={}",
            name,
            tags.len()
        );
        let _bucket_info = self.authorize_bucket_admin_requester(requester, name)?;
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg.put_bucket_tags(name, tags).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })
    }

    pub fn get_bucket_tags(
        &self,
        name: &str,
        requester: Requester<'_>,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_tags",
            "bucket={}",
            name
        );
        let _bucket_info = self.authorize_bucket_read_requester(requester, name)?;
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg.get_bucket_tags(name).map_err(|e| match e {
            storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                name: name.to_string(),
            },
            other => ServerError::Metadata(other),
        })
    }

    pub fn delete_bucket_tags(
        &self,
        name: &str,
        requester: Requester<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_tags",
            "bucket={}",
            name
        );
        let _bucket_info = self.authorize_bucket_admin_requester(requester, name)?;
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
        requester: Requester<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_public_access_block",
            "bucket={} bytes={}",
            name,
            config.len()
        );
        let _bucket_info = self.authorize_bucket_admin_requester(requester, name)?;
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
        requester: Requester<'_>,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_public_access_block",
            "bucket={}",
            name
        );
        let _bucket_info = self.authorize_bucket_admin_requester(requester, name)?;
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

    pub fn delete_bucket_public_access_block(
        &self,
        name: &str,
        requester: Requester<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_public_access_block",
            "bucket={}",
            name
        );
        let _bucket_info = self.authorize_bucket_admin_requester(requester, name)?;
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

    pub fn put_bucket_acl(
        &self,
        name: &str,
        acl: BucketAcl,
        requester: Requester<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_acl",
            "bucket={} acl={:?}",
            name,
            acl
        );
        let _bucket_info = self.authorize_bucket_admin_requester(requester, name)?;
        let ownership_controls = self.get_bucket_ownership_controls(name, requester)?;
        if Self::is_bucket_owner_enforced(ownership_controls.as_deref()) {
            return Err(ServerError::AccessControlListNotSupported);
        }
        let pab = self.get_bucket_public_access_block(name, requester)?;
        if matches!(
            acl,
            BucketAcl::PublicRead | BucketAcl::PublicReadWrite | BucketAcl::AuthenticatedRead
        ) && Self::blocks_public_acls(pab.as_deref())
        {
            return Err(ServerError::AccessDenied);
        }
        let (public_read, public_write) = match acl {
            BucketAcl::Private => (false, false),
            BucketAcl::PublicRead => (true, false),
            BucketAcl::PublicReadWrite => (true, true),
            BucketAcl::AuthenticatedRead => {
                return Err(ServerError::NotImplemented {
                    feature: "authenticated-read ACL".to_string(),
                });
            }
        };
        let bucket_pg = self.get_bucket_pg(name)?;
        bucket_pg
            .put_bucket_acl(name, public_read, public_write)
            .map_err(|e| match e {
                storage::MetadataError::BucketNotFound { name } => ServerError::BucketNotFound {
                    name: name.to_string(),
                },
                other => ServerError::Metadata(other),
            })
    }

    pub fn get_bucket_acl(
        &self,
        name: &str,
        requester: Requester<'_>,
    ) -> Result<GetBucketAclResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_acl",
            "bucket={}",
            name
        );
        let bucket = self.authorize_bucket_admin_requester(requester, name)?;
        let acl = if bucket.public_write {
            BucketAcl::PublicReadWrite
        } else if bucket.public_read {
            BucketAcl::PublicRead
        } else {
            BucketAcl::Private
        };
        Ok(GetBucketAclResult {
            owner_principal: bucket.owner_principal,
            owner_canonical_id: bucket.owner_canonical_id,
            acl,
        })
    }

    // ── Ownership controls ────────────────────────────────────────────

    pub fn put_bucket_ownership_controls(
        &self,
        name: &str,
        config: &str,
        requester: Requester<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_bucket_ownership_controls",
            "bucket={} bytes={}",
            name,
            config.len()
        );
        let bucket_info = self.authorize_bucket_admin_requester(requester, name)?;
        if Self::is_bucket_owner_enforced(Some(config))
            && (bucket_info.public_read || bucket_info.public_write)
        {
            return Err(ServerError::InvalidBucketAclWithObjectOwnership);
        }
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

    pub fn get_bucket_ownership_controls(
        &self,
        name: &str,
        requester: Requester<'_>,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_bucket_ownership_controls",
            "bucket={}",
            name
        );
        let _bucket_info = self.authorize_bucket_admin_requester(requester, name)?;
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

    pub fn delete_bucket_ownership_controls(
        &self,
        name: &str,
        requester: Requester<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_bucket_ownership_controls",
            "bucket={}",
            name
        );
        let _bucket_info = self.authorize_bucket_admin_requester(requester, name)?;
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
        version_id: Option<VersionId>,
        tags: &str,
        requester: Requester<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::put_object_tags",
            "bucket={} key={} bytes={}",
            bucket,
            key,
            tags.len()
        );
        let _bucket_info = self.authorize_object_write_requester(requester, bucket)?;
        let pg_id = self.object_pg_id(bucket, key);
        let pg = self.storage_node.get_pg(pg_id)?;
        let stored = match version_id {
            Some(vid) => pg.get_object_version(bucket, key, vid),
            None => pg.get_object_meta(bucket, key),
        }
        .map_err(|e| match e {
            storage::MetadataError::ObjectNotFound if version_id.is_some() => {
                ServerError::VersionNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id: version_id.unwrap().to_string(),
                }
            }
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
        version_id: Option<VersionId>,
        requester: Requester<'_>,
    ) -> Result<Option<String>, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_object_tags",
            "bucket={} key={}",
            bucket,
            key
        );
        let _bucket_info = self.authorize_bucket_read_requester(requester, bucket)?;
        let pg_id = self.object_pg_id(bucket, key);
        let pg = self.storage_node.get_pg(pg_id)?;
        let stored = match version_id {
            Some(vid) => pg.get_object_version(bucket, key, vid),
            None => pg.get_object_meta(bucket, key),
        }
        .map_err(|e| match e {
            storage::MetadataError::ObjectNotFound if version_id.is_some() => {
                ServerError::VersionNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id: version_id.unwrap().to_string(),
                }
            }
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
        version_id: Option<VersionId>,
        requester: Requester<'_>,
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_object_tags",
            "bucket={} key={}",
            bucket,
            key
        );
        let _bucket_info = self.authorize_object_write_requester(requester, bucket)?;
        let pg_id = self.object_pg_id(bucket, key);
        let pg = self.storage_node.get_pg(pg_id)?;
        let stored = match version_id {
            Some(vid) => pg.get_object_version(bucket, key, vid),
            None => pg.get_object_meta(bucket, key),
        }
        .map_err(|e| match e {
            storage::MetadataError::ObjectNotFound if version_id.is_some() => {
                ServerError::VersionNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id: version_id.unwrap().to_string(),
                }
            }
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

    // ── Streaming upload session API ──────────────────────────────────

    /// Begin a streaming PutObject upload session.
    ///
    /// Creates a session on the metadata PG for `(bucket, key)`. The caller
    /// feeds segments via `append_stream_segment` and commits via
    /// `finalize_stream_put`.
    pub fn begin_stream_put(&self, req: &BeginStreamPutRequest<'_>) -> Result<String, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::begin_stream_put",
            "bucket={} key={}",
            req.bucket,
            req.key
        );
        let bucket = req.bucket;
        let key = req.key;
        let _bucket_guard = self.storage_node.lock_bucket(bucket);

        let bucket_info = self.authorize_object_write_requester(req.requester, bucket)?;
        if Self::is_bucket_owner_enforced(bucket_info.ownership_controls.as_deref())
            && !matches!(
                req.acl,
                PutObjectAcl::None | PutObjectAcl::Private | PutObjectAcl::BucketOwnerFullControl
            )
        {
            return Err(ServerError::AccessControlListNotSupported);
        }

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
        req: &BeginStreamPartRequest<'_>,
    ) -> Result<BeginStreamPartResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::begin_stream_part",
            "bucket={} key={} upload_id={} part_number={}",
            req.bucket,
            req.key,
            req.upload_id,
            req.part_number
        );
        let bucket = req.bucket;
        let key = req.key;
        let upload_id = req.upload_id;
        let part_number = req.part_number;

        // Validate part number range.
        if part_number == 0 || part_number > 10_000 {
            return Err(ServerError::InvalidArgument {
                reason: format!("part number must be between 1 and 10000, got {part_number}"),
            });
        }

        let _bucket_info = self.authorize_object_write_requester(req.requester, bucket)?;

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
            checksum_algorithm: upload.checksum.map(MultipartChecksumConfig::algorithm),
        })
    }

    /// Append a segment of data to an in-progress streaming session.
    ///
    /// Locks the metadata/session PG and the segment's shard PG in global
    /// ascending order. Validates the session is InProgress, EC-encodes the
    /// segment, writes shards, and records a staging segment row.
    ///
    /// The caller must not hold any PG locks when calling this method.
    pub fn append_stream_segment(
        &self,
        bucket: &str,
        key: &str,
        session_id: &str,
        segment_index: u32,
        data: &[u8],
    ) -> Result<(), ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::append_stream_segment",
            "bucket={} key={} session_id={} segment_index={} bytes={}",
            bucket,
            key,
            session_id,
            segment_index,
            data.len()
        );
        let meta_pg_id = self.object_pg_id(bucket, key);

        // Derive stream segment shard placement.
        let segment_okh = stream_segment_key_hash(session_id, segment_index);
        let segment_vid = GenerationId::MIN;
        let shard_pg_id = self.shard_pg_id_raw(
            &format!("segment/{session_id}"),
            &segment_index.to_string(),
            segment_vid.get(),
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
        if let Some(trace) = observability::current_context() {
            let segment_offset_start = u64::from(segment_index) * INTERNAL_SEGMENT_SIZE as u64;
            let segment_offset_len = data.len() as u64;
            let segment_offset_end_exclusive = segment_offset_start + segment_offset_len;
            match &session.target {
                StreamUploadTarget::PutObject => {
                    let _ = observability::event_in_context(
                        &trace,
                        TRACE_TARGET,
                        "stream_put_segment_layout",
                        Some(format_args!(
                            "bucket={} key={} session_id={} segment_index={} object_offset_start={} object_offset_len={} object_offset_end_exclusive={}",
                            bucket,
                            key,
                            session_id,
                            segment_index,
                            segment_offset_start,
                            segment_offset_len,
                            segment_offset_end_exclusive
                        )),
                    );
                }
                StreamUploadTarget::UploadPart {
                    upload_id,
                    part_number,
                } => {
                    let _ = observability::event_in_context(
                        &trace,
                        TRACE_TARGET,
                        "stream_part_segment_layout",
                        Some(format_args!(
                            "bucket={} key={} upload_id={} part_number={} session_id={} segment_index={} part_offset_start={} part_offset_len={} part_offset_end_exclusive={}",
                            bucket,
                            key,
                            upload_id,
                            part_number,
                            session_id,
                            segment_index,
                            segment_offset_start,
                            segment_offset_len,
                            segment_offset_end_exclusive
                        )),
                    );
                }
            }
        }
        // Reject duplicate segment_index — writing shards then failing on PK
        // constraint would delete the already-staged segment's shard data.
        let existing_segments = meta_guard
            .list_stream_segments(session_id)
            .map_err(ServerError::Metadata)?;
        if existing_segments
            .iter()
            .any(|segment| segment.segment_index == segment_index)
        {
            return Err(ServerError::InvalidRequest {
                reason: format!("duplicate segment_index {segment_index}"),
            });
        }

        // EC-encode segment data.
        let k = self.ec_config.data_shards as usize;
        let m = self.ec_config.parity_shards as usize;
        let remainder = data.len() % k;
        let mut padded = Vec::new();
        let shard_source: &[u8] = if remainder == 0 {
            data
        } else {
            padded.reserve_exact(data.len() + (k - remainder));
            padded.extend_from_slice(data);
            padded.resize(data.len() + (k - remainder), 0);
            &padded
        };

        let shard_size = shard_source.len() / k;
        let data_shards: Vec<&[u8]> = (0..k)
            .map(|i| &shard_source[i * shard_size..(i + 1) * shard_size])
            .collect();
        let mut written_shards: Vec<ShardKey> = Vec::with_capacity(k + m);
        let write_result: Result<(), ServerError> = if shard_size == 0 {
            let mut parity_bufs: Vec<Vec<u8>> = (0..m).map(|_| Vec::new()).collect();
            let mut parity_refs: Vec<&mut [u8]> = parity_bufs
                .iter_mut()
                .map(std::vec::Vec::as_mut_slice)
                .collect();
            self.ec_codec.encode(&data_shards, &mut parity_refs)?;
            (|| {
                for (i, shard_data) in data_shards.iter().enumerate() {
                    let shard_key = ShardKey::new(&segment_okh, segment_vid.get(), i as u8);
                    shard_pg.write_shard(&shard_key, shard_data)?;
                    written_shards.push(shard_key);
                }
                for (parity_index, shard_data) in parity_bufs.iter().enumerate() {
                    let shard_key =
                        ShardKey::new(&segment_okh, segment_vid.get(), (k + parity_index) as u8);
                    shard_pg.write_shard(&shard_key, shard_data)?;
                    written_shards.push(shard_key);
                }
                Ok(())
            })()
        } else {
            let parity_len =
                m.checked_mul(shard_size)
                    .ok_or_else(|| ServerError::InternalError {
                        reason: "parity scratch length overflow".to_string(),
                    })?;
            let mut scratch = self.encode_scratch_pool.checkout();
            {
                let parity = scratch.as_mut_slice(parity_len);
                let mut parity_refs: Vec<&mut [u8]> = parity.chunks_exact_mut(shard_size).collect();
                self.ec_codec.encode(&data_shards, &mut parity_refs)?;
            }
            let parity = scratch.as_slice(parity_len);
            (|| {
                for (i, shard_data) in data_shards.iter().enumerate() {
                    let shard_key = ShardKey::new(&segment_okh, segment_vid.get(), i as u8);
                    shard_pg.write_shard(&shard_key, shard_data)?;
                    written_shards.push(shard_key);
                }
                for (parity_index, shard_data) in parity.chunks_exact(shard_size).enumerate() {
                    let shard_key =
                        ShardKey::new(&segment_okh, segment_vid.get(), (k + parity_index) as u8);
                    shard_pg.write_shard(&shard_key, shard_data)?;
                    written_shards.push(shard_key);
                }
                Ok(())
            })()
        };

        if let Err(e) = write_result {
            for shard_key in &written_shards {
                let _ = shard_pg.delete_shard(shard_key);
            }
            return Err(e);
        }

        // Record staging segment row.
        let segment_result = meta_guard.append_stream_segment(&StreamUploadSegmentRecord {
            session_id: SessionId::from(session_id),
            segment_index,
            size: data.len() as u64,
            segment_crc64: Some(checksum::crc64::checksum(data)),
            segment_okh,
            segment_vid,
            shard_pg_id,
            ec_k: self.ec_config.data_shards,
            ec_m: self.ec_config.parity_shards,
        });

        if let Err(e) = segment_result {
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
    /// Locks the metadata PG, allocates a version_id, builds committed segment
    /// metadata from staging rows, and atomically commits the object via
    /// `commit_stream_put`.
    ///
    /// The caller passes the running CRC64 checksum, total size, and metadata
    /// blob computed during the append phase. No segment data is re-read.
    pub fn finalize_stream_put(
        &self,
        req: &FinalizeStreamPutRequest,
    ) -> Result<PutObjectResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::finalize_stream_put",
            "bucket={} key={} session_id={} bytes={}",
            req.bucket,
            req.key,
            req.session_id,
            req.total_size
        );
        let bucket = req.bucket;
        let key = req.key;
        let session_id = req.session_id;
        let crc64 = req.crc64;
        let total_size = req.total_size;
        let metadata_blob = req.metadata_blob;
        let tags = req.tags;
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
        let version_id = if bucket_info.versioning == BucketVersioningState::Enabled {
            meta_guard.next_version_id(bucket, key)?
        } else {
            VersionId::Null
        };
        let generation_id = meta_guard.next_generation_id(bucket, key)?;
        let stale_payload = if version_id.is_null() {
            Self::snapshot_overwritten_null_version_payload(&meta_guard, bucket, key)?
        } else {
            None
        };

        // Build committed object segments from staging rows and validate total_size.
        let staging_segments = meta_guard
            .list_stream_segments(session_id)
            .map_err(ServerError::Metadata)?;
        let segments_total: u64 = staging_segments.iter().map(|segment| segment.size).sum();
        if segments_total != total_size {
            return Err(ServerError::InvalidRequest {
                reason: format!(
                    "total_size mismatch: caller passed {total_size} but staged segments sum to {segments_total}"
                ),
            });
        }
        let committed_segments: Vec<ObjectSegmentRecord> = staging_segments
            .iter()
            .map(|segment| ObjectSegmentRecord {
                bucket: BucketName::from(bucket),
                key: ObjectKey::from(key),
                version_id,
                segment_index: segment.segment_index,
                size: segment.size,
                segment_crc64: segment.segment_crc64,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                shard_pg_id: segment.shard_pg_id,
                ec_k: segment.ec_k,
                ec_m: segment.ec_m,
            })
            .collect();

        // Atomic finalize: commit object metadata + object segments, delete staging.
        meta_guard
            .commit_stream_put(
                session_id,
                &CommitStreamPutReq {
                    bucket: BucketName::from(bucket),
                    key: ObjectKey::from(key),
                    version_id,
                    generation_id,
                    size: total_size,
                    etag_crc64: crc64,
                    ec: EcShape {
                        k: self.ec_config.data_shards,
                        m: self.ec_config.parity_shards,
                    },
                    tags: tags.map(SerializedTagSet::from),
                    metadata_blob: Some(SerializedMetadataBlob::from(blob_bytes)),
                },
                &committed_segments,
            )
            .map_err(ServerError::Metadata)?;

        if let Some(ref payload) = stale_payload {
            match payload {
                StaleObjectPayload::Segments {
                    generation_id,
                    segments,
                } => {
                    // `commit_stream_put` already replaced the live object segments rows
                    // for VersionId::Null, so only enqueue reclaim for the old payload.
                    Self::enqueue_object_segments_reclaim(
                        &meta_guard,
                        bucket,
                        key,
                        *generation_id,
                        segments,
                    )?;
                }
                StaleObjectPayload::Multipart { .. } => {
                    Self::delete_stale_object_payload_metadata(
                        &meta_guard,
                        bucket,
                        key,
                        version_id,
                        payload,
                    )?;
                }
            }
        }

        drop(meta_guard);
        if let Some(ref payload) = stale_payload {
            self.delete_stale_object_payload(bucket, key, payload);
        }

        Ok(PutObjectResult {
            etag: format_etag(crc64),
            version_id,
        })
    }

    /// Finalize a streaming UploadPart session.
    ///
    /// Locks the metadata PG, builds committed object segments from staging
    /// rows, and atomically commits the part via `commit_stream_part`.
    /// `computed_checksum` is the actual checksum bytes computed incrementally
    /// during streaming. If `None`, the checksum is derived from `claimed_checksum`.
    pub fn finalize_stream_part(
        &self,
        req: FinalizeStreamPartRequest,
    ) -> Result<UploadPartResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::finalize_stream_part",
            "bucket={} key={} upload_id={} part_number={} session_id={} bytes={}",
            req.bucket,
            req.key,
            req.upload_id,
            req.part_number,
            req.session_id,
            req.total_size
        );
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
        let claimed_algo = claimed_checksum.map(ChecksumClaim::algorithm);
        let upload_checksum_algo = upload.checksum.map(MultipartChecksumConfig::algorithm);
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
            (Some(algo), Some(_)) => Some(algo),
            // AWS rejects parts without a checksum when the upload requires one.
            (Some(upload_algo), None) => {
                return Err(ServerError::InvalidRequest {
                    reason: format!(
                        "Checksum Type mismatch occurred, expected checksum Type: {}, actual checksum Type: null",
                        upload_algo.as_str().to_lowercase()
                    ),
                });
            }
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

        // Build committed object segments from staging rows.
        let staging_segments = meta_guard
            .list_stream_segments(session_id)
            .map_err(ServerError::Metadata)?;
        let segments_total: u64 = staging_segments.iter().map(|segment| segment.size).sum();
        if segments_total != total_size {
            return Err(ServerError::InvalidRequest {
                reason: format!(
                    "total_size mismatch: caller passed {total_size} but staged segments sum to {segments_total}"
                ),
            });
        }

        let committed_segments: Vec<MultipartPartSegmentRecord> = staging_segments
            .iter()
            .map(|segment| MultipartPartSegmentRecord {
                bucket: BucketName::from(bucket),
                key: ObjectKey::from(key),
                upload_id: UploadId::from(upload_id),
                version_id: u64::MAX, // staging sentinel — reparented at CompleteMultipartUpload time
                part_number,
                segment_index: segment.segment_index,
                size: segment.size,
                segment_crc64: segment.segment_crc64,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                shard_pg_id: segment.shard_pg_id,
                ec_k: segment.ec_k,
                ec_m: segment.ec_m,
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
            part_vid: GenerationId::new(u64::from(generation) + 1)
                .expect("multipart part generation must be nonzero"),
            ec_k: self.ec_config.data_shards,
            ec_m: self.ec_config.parity_shards,
            last_modified: now,
            checksum: checksum_bytes.clone(),
        };

        // Atomic commit: upsert part, insert segments, delete staging.
        meta_guard
            .commit_stream_part(session_id, &part_record, &committed_segments)
            .map_err(ServerError::Metadata)?;

        // Best-effort cleanup of prior generation's shards.
        // The prior generation used the non-streaming path, so clean its
        // single shard set. If it was also a streamed part, clean its segments.
        drop(meta_guard);
        if generation > 0 {
            let old_gen = generation - 1;
            // Clean old non-streaming shards.
            let old_okh = part_key_hash(upload_id, part_number, old_gen);
            let old_vid =
                GenerationId::new(u64::from(old_gen) + 1).expect("old generation must be nonzero");
            let old_shard_pg_id = self.shard_pg_id_raw(
                &format!("mpu/{upload_id}"),
                &format!("{part_number}/{old_gen}"),
                old_vid.get(),
            );
            if let Ok(old_pg) = self.storage_node.get_pg(old_shard_pg_id) {
                let k = self.ec_config.data_shards as usize;
                let m = self.ec_config.parity_shards as usize;
                for i in 0..(k + m) {
                    let old_key = ShardKey::new(&old_okh, old_vid.get(), i as u8);
                    let _ = old_pg.delete_shard(&old_key);
                }
            }
            // Clean old streamed-part segments (if prior generation was streamed).
            // `commit_stream_part` already handles deleting prior
            // `multipart_part_segments` rows in its transaction, but the shard
            // data on disk still needs cleanup.
            if let Ok(pg) = self.storage_node.get_pg(meta_pg_id) {
                if let Ok(old_segments) = pg.get_multipart_part_segments(
                    bucket,
                    key,
                    VersionId::from_u64(old_vid.get()),
                    part_number,
                ) {
                    drop(pg);
                    let _ = self.delete_segment_shards_generic(&old_segments);
                }
            }
        }

        let checksum = match (effective_algo, checksum_bytes) {
            (Some(algo), Some(bytes)) => {
                Some(
                    RawChecksum::new(algo, bytes).map_err(|_| ServerError::InternalError {
                        reason: "computed checksum length does not match algorithm".into(),
                    })?,
                )
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

        // Collect staging segments for shard cleanup before deleting session.
        let staging_segments = meta_guard
            .list_stream_segments(session_id)
            .map_err(ServerError::Metadata)?;

        // Set state to Aborted, then delete session (CASCADE deletes staging segments).
        meta_guard
            .set_stream_upload_state(session_id, StreamUploadState::Aborted)
            .map_err(ServerError::Metadata)?;
        meta_guard
            .delete_stream_upload(session_id)
            .map_err(ServerError::Metadata)?;

        // Drop the PG lock before best-effort shard cleanup, which may need
        // to lock other PGs.
        drop(meta_guard);

        // Best-effort cleanup of staged segment shards.
        for segment in &staging_segments {
            if let Ok(shard_guard) = self.storage_node.get_pg(segment.shard_pg_id) {
                let k = segment.ec_k as usize;
                let m = segment.ec_m as usize;
                for i in 0..(k + m) {
                    let shard_key =
                        ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
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
    pub fn copy_object(&self, req: &CopyObjectRequest) -> Result<CopyObjectResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::copy_object",
            "src_bucket={} src_key={} dst_bucket={} dst_key={}",
            req.source.bucket,
            req.source.key,
            req.dst_bucket,
            req.dst_key
        );
        let src_bucket = req.source.bucket;
        let src_key = req.source.key;
        let src_version_id = req.source.version_id;
        let dst_bucket = req.dst_bucket;
        let dst_key = req.dst_key;
        let src_cond = req.source.condition;
        let dst_cond = req.dst_condition;
        let directive = &req.directive;
        let requester = req.requester;
        let acl = req.acl;

        let dst_bucket_info = self.authorize_object_write_requester(requester, dst_bucket)?;

        if Self::is_bucket_owner_enforced(dst_bucket_info.ownership_controls.as_deref())
            && !matches!(
                acl,
                PutObjectAcl::None | PutObjectAcl::Private | PutObjectAcl::BucketOwnerFullControl
            )
        {
            return Err(ServerError::AccessControlListNotSupported);
        }

        let _src_bucket_info = self.authorize_bucket_read_requester(requester, src_bucket)?;

        // Phase 1: Snapshot source metadata and prepare a read handle.
        let (src_metadata, src_tags, mut source_body) = {
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

            if src_record.size > MAX_OBJECT_SIZE {
                return Err(ServerError::ObjectTooLarge {
                    size: src_record.size,
                    max: MAX_OBJECT_SIZE,
                });
            }

            if matches!(src_record.layout, ObjectLayout::MultipartManifest { .. }) {
                // Multipart source: metadata from row, data from parts.
                let meta_pg = pgs.meta();
                let obj_parts = Self::snapshot_multipart_parts(
                    meta_pg,
                    src_bucket,
                    src_key,
                    src_record.version_id,
                )?;
                let body = if src_record.size == 0 {
                    drop(pgs);
                    #[cfg(test)]
                    maybe_run_multipart_snapshot_hook(src_bucket, src_key);
                    ReadHandle::from_buffered_bytes(Vec::new())
                } else {
                    let body = ReadHandle::from_multipart(
                        self.read_runtime(),
                        src_bucket,
                        src_key,
                        src_record.generation_id,
                        obj_parts,
                        src_record.size as usize,
                    );
                    drop(pgs);
                    #[cfg(test)]
                    maybe_run_multipart_snapshot_hook(src_bucket, src_key);
                    body
                };

                let metadata = src_record
                    .metadata_blob
                    .as_ref()
                    .map(|b| MetadataBlob::deserialize(b.as_slice()).map(|(m, _)| m))
                    .transpose()?
                    .unwrap_or_default();

                (metadata, src_record.tags.clone(), body)
            } else {
                // Non-multipart source: metadata from DB row, user data from shards.
                let src_etag_crc = src_record.etag.crc64();

                // Non-multipart payloads now read through committed object segments.
                let meta_pg = pgs.meta();
                let segments = meta_pg
                    .get_object_segments(src_bucket, src_key, src_record.version_id)
                    .map_err(ServerError::Metadata)?;

                let body = if src_record.size == 0 {
                    drop(pgs);
                    ReadHandle::from_buffered_bytes(Vec::new())
                } else {
                    let body = ReadHandle::from_segments(
                        self.read_runtime(),
                        src_bucket,
                        src_key,
                        src_record.generation_id,
                        segment_payloads_from_object_segments(segments),
                        src_record.size as usize,
                        Some(src_etag_crc),
                    );
                    drop(pgs);
                    body
                };

                let src_metadata = src_record
                    .metadata_blob
                    .as_ref()
                    .map(|b| MetadataBlob::deserialize(b.as_slice()).map(|(m, _)| m))
                    .transpose()?
                    .unwrap_or_default();

                (src_metadata, src_record.tags.clone(), body)
            }
        }; // source locks dropped here

        // Phase 2: Stream into destination staging session.
        let mut metadata_blob = match directive {
            MetadataDirective::Copy => src_metadata,
            MetadataDirective::Replace {
                metadata: new_metadata,
                ..
            } => (*new_metadata).clone(),
        };
        let tags = match &req.tagging {
            TaggingDirective::Copy => src_tags,
            TaggingDirective::Replace(tags) => tags.map(SerializedTagSet::from),
        };
        let mut replacement_checksum = match directive {
            MetadataDirective::Replace {
                checksum_algorithm: Some(algo),
                ..
            } => Some(StreamingChecksumAccumulator::new(*algo)),
            _ => None,
        };
        let session_id = self.begin_stream_put(&BeginStreamPutRequest {
            bucket: dst_bucket,
            key: dst_key,
            requester,
            acl,
        })?;
        let not_found = |e: ServerError| match e {
            ServerError::Store(storage::StoreError::NotFound) => ServerError::ObjectNotFound {
                bucket: src_bucket.to_string(),
                key: src_key.to_string(),
            },
            other => other,
        };
        let copy_result = (|| {
            let mut crc64 = checksum::crc64::Hasher::new();
            let mut total_size = 0u64;
            let mut segment_index = 0u32;

            while let Some(chunk) = source_body
                .next_chunk(INTERNAL_SEGMENT_SIZE)
                .map_err(not_found)?
            {
                total_size = total_size.checked_add(chunk.len() as u64).ok_or_else(|| {
                    ServerError::InternalError {
                        reason: "copy size overflow".to_string(),
                    }
                })?;
                crc64.update(&chunk);
                if let Some(checksum) = replacement_checksum.as_mut() {
                    checksum.update(&chunk);
                }
                self.append_stream_segment(
                    dst_bucket,
                    dst_key,
                    &session_id,
                    segment_index,
                    &chunk,
                )?;
                segment_index =
                    segment_index
                        .checked_add(1)
                        .ok_or_else(|| ServerError::InternalError {
                            reason: "too many copy segments".to_string(),
                        })?;
            }

            if let Some(checksum) = replacement_checksum.take() {
                use base64::Engine;

                let algo = checksum.algorithm();
                let b64 = base64::engine::general_purpose::STANDARD.encode(checksum.finalize());
                metadata_blob.set(algo.header_name(), &b64);
            }

            let put_result = self.finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: dst_bucket,
                key: dst_key,
                session_id: &session_id,
                crc64: crc64.finalize(),
                total_size,
                metadata_blob: &metadata_blob,
                tags: tags.as_deref(),
                cond: dst_cond,
            })?;

            let dst_meta_pg = self
                .storage_node
                .get_pg(self.object_pg_id(dst_bucket, dst_key))?;
            let dst_stored = dst_meta_pg
                .get_object_meta(dst_bucket, dst_key)
                .map_err(ServerError::Metadata)?;

            Ok(CopyObjectResult {
                etag: put_result.etag,
                last_modified: dst_stored.last_modified(),
                version_id: put_result.version_id,
            })
        })();
        if copy_result.is_err() {
            let _ = self.abort_stream_put(dst_bucket, dst_key, &session_id);
        }
        copy_result
    }

    fn lookup_object_record(
        meta_pg: &storage::PgStore,
        bucket: &str,
        key: &str,
        version_id: Option<VersionId>,
    ) -> Result<StoredObject, ServerError> {
        match version_id {
            Some(vid) => meta_pg.get_object_version(bucket, key, vid),
            None => meta_pg.get_object_meta(bucket, key),
        }
        .map_err(|e| match e {
            storage::MetadataError::ObjectNotFound if version_id.is_some() => {
                ServerError::VersionNotFound {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    version_id: version_id.unwrap().to_string(),
                }
            }
            storage::MetadataError::ObjectNotFound => ServerError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            },
            other => ServerError::Metadata(other),
        })
    }

    /// Lock metadata PG for a consistent object read/delete view.
    ///
    /// Latest-version readers snapshot object metadata while holding the metadata
    /// PG lock, then construct `ReadHandle`s that acquire a generation-scoped
    /// payload lease before this guard is released. Committed payload
    /// generations are immutable, and reclaim is lease-gated, so read-side
    /// paths no longer need to relock a synthetic shard PG.
    fn lock_object_pgs_for_read<'a>(
        &'a self,
        bucket: &str,
        key: &str,
        version_id: Option<VersionId>,
    ) -> Result<LockedReadObject<'a>, ServerError> {
        let meta_pg_id = self.object_pg_id(bucket, key);
        let meta_guard = self.storage_node.get_pg(meta_pg_id)?;
        let record = Self::lookup_object_record(&meta_guard, bucket, key, version_id)?;
        Ok(LockedReadObject {
            record,
            pgs: ObjectPgGuards::new(meta_guard),
        })
    }

    fn multipart_part_payloads(
        meta_pg: &storage::PgStore,
        bucket: &str,
        key: &str,
        version_id: VersionId,
        part: &ObjectPartRecord,
    ) -> Result<Vec<SegmentPayloadRecord>, ServerError> {
        if part.part_okh != [0u8; 16] {
            return Ok(vec![SegmentPayloadRecord {
                size: part.size,
                segment_crc64: None,
                segment_okh: part.part_okh,
                segment_vid: part.part_vid,
                shard_pg_id: part.shard_pg_id,
                ec_k: part.ec_k,
                ec_m: part.ec_m,
            }]);
        }

        meta_pg
            .get_multipart_part_segments(bucket, key, version_id, part.part_number)
            .map(|segments| {
                segments
                    .into_iter()
                    .map(|segment| SegmentPayloadRecord {
                        size: segment.size,
                        segment_crc64: segment.segment_crc64,
                        segment_okh: segment.segment_okh,
                        segment_vid: segment.segment_vid,
                        shard_pg_id: segment.shard_pg_id,
                        ec_k: segment.ec_k,
                        ec_m: segment.ec_m,
                    })
                    .collect()
            })
            .map_err(ServerError::Metadata)
    }

    fn snapshot_multipart_parts(
        meta_pg: &storage::PgStore,
        bucket: &str,
        key: &str,
        version_id: VersionId,
    ) -> Result<Vec<SnapshottedMultipartPart>, ServerError> {
        let parts = meta_pg
            .get_object_parts(bucket, key, version_id)
            .map_err(ServerError::Metadata)?;
        let mut snapshotted = Vec::with_capacity(parts.len());
        let mut object_offset_start = 0usize;
        for part in parts {
            let part_size = part.size as usize;
            let segments = Self::multipart_part_payloads(meta_pg, bucket, key, version_id, &part)?;
            snapshotted.push(SnapshottedMultipartPart {
                record: part,
                object_offset_start,
                segments,
            });
            object_offset_start += part_size;
        }
        Ok(snapshotted)
    }

    fn snapshot_multipart_parts_overlapping_range(
        meta_pg: &storage::PgStore,
        bucket: &str,
        key: &str,
        version_id: VersionId,
        start: u64,
        end_exclusive: u64,
    ) -> Result<Vec<SnapshottedMultipartPart>, ServerError> {
        let parts = meta_pg
            .get_object_parts_overlapping_range(bucket, key, version_id, start, end_exclusive)
            .map_err(ServerError::Metadata)?;
        let mut snapshotted = Vec::with_capacity(parts.len());
        for part in parts {
            let segments =
                Self::multipart_part_payloads(meta_pg, bucket, key, version_id, &part.part)?;
            snapshotted.push(SnapshottedMultipartPart {
                record: part.part,
                object_offset_start: part.object_offset_start as usize,
                segments,
            });
        }
        Ok(snapshotted)
    }

    fn snapshot_overwritten_null_version_payload(
        meta_pg: &storage::PgStore,
        bucket: &str,
        key: &str,
    ) -> Result<Option<StaleObjectPayload>, ServerError> {
        let stored = match meta_pg.get_object_version(bucket, key, VersionId::Null) {
            Ok(stored) => stored,
            Err(storage::MetadataError::ObjectNotFound) => return Ok(None),
            Err(e) => return Err(ServerError::Metadata(e)),
        };
        let record = match stored {
            StoredObject::Live(record) => record,
            StoredObject::DeleteMarker(_) => return Ok(None),
        };

        match record.layout {
            ObjectLayout::MultipartManifest { .. } => {
                let parts = meta_pg
                    .get_object_parts(bucket, key, VersionId::Null)
                    .map_err(ServerError::Metadata)?;
                let mut streaming_segments = Vec::new();
                for part in &parts {
                    if part.part_okh == [0u8; 16] {
                        let segments = meta_pg
                            .get_multipart_part_segments(
                                bucket,
                                key,
                                VersionId::Null,
                                part.part_number,
                            )
                            .map_err(ServerError::Metadata)?;
                        streaming_segments.extend(segments);
                    }
                }
                Ok(Some(StaleObjectPayload::Multipart {
                    generation_id: record.generation_id,
                    parts,
                    streaming_segments,
                }))
            }
            ObjectLayout::Standard => {
                let segments = meta_pg
                    .get_object_segments(bucket, key, VersionId::Null)
                    .map_err(ServerError::Metadata)?;
                Ok(Some(StaleObjectPayload::Segments {
                    generation_id: record.generation_id,
                    segments,
                }))
            }
        }
    }

    fn delete_stale_object_payload_metadata(
        meta_pg: &storage::PgStore,
        bucket: &str,
        key: &str,
        version_id: VersionId,
        payload: &StaleObjectPayload,
    ) -> Result<(), ServerError> {
        match payload {
            StaleObjectPayload::Segments {
                generation_id,
                segments,
            } => {
                Self::enqueue_object_segments_reclaim(
                    meta_pg,
                    bucket,
                    key,
                    *generation_id,
                    segments,
                )?;
                meta_pg
                    .delete_object_segments(bucket, key, version_id)
                    .map_err(ServerError::Metadata)
            }
            StaleObjectPayload::Multipart {
                generation_id,
                parts,
                streaming_segments,
            } => {
                Self::enqueue_multipart_reclaim(
                    meta_pg,
                    bucket,
                    key,
                    *generation_id,
                    parts,
                    streaming_segments,
                )?;
                if !streaming_segments.is_empty() {
                    meta_pg
                        .delete_multipart_part_segments(bucket, key, version_id)
                        .map_err(ServerError::Metadata)?;
                }
                meta_pg
                    .delete_object_parts(bucket, key, version_id)
                    .map_err(ServerError::Metadata)
            }
        }
    }

    fn delete_stale_object_payload(&self, bucket: &str, key: &str, payload: &StaleObjectPayload) {
        match payload {
            StaleObjectPayload::Segments { generation_id, .. } => self
                .read_runtime()
                .enqueue_object_payload_reclaim(bucket, key, *generation_id),
            StaleObjectPayload::Multipart { generation_id, .. } => self
                .read_runtime()
                .enqueue_object_payload_reclaim(bucket, key, *generation_id),
        }
    }

    fn enqueue_object_segments_reclaim(
        meta_pg: &storage::PgStore,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
        segments: &[ObjectSegmentRecord],
    ) -> Result<(), ServerError> {
        meta_pg
            .put_object_segments_reclaim(&ObjectSegmentsReclaimRecord {
                bucket: BucketName::from(bucket),
                key: ObjectKey::from(key),
                generation_id,
                created_at: Self::now_millis(),
                segments: segments
                    .iter()
                    .map(|segment| ObjectSegmentsReclaimSegmentRecord {
                        segment_index: segment.segment_index,
                        segment_okh: segment.segment_okh,
                        segment_vid: segment.segment_vid,
                        shard_pg_id: segment.shard_pg_id,
                        ec: EcShape {
                            k: segment.ec_k,
                            m: segment.ec_m,
                        },
                    })
                    .collect(),
            })
            .map_err(ServerError::Metadata)
    }

    fn enqueue_multipart_reclaim(
        meta_pg: &storage::PgStore,
        bucket: &str,
        key: &str,
        generation_id: GenerationId,
        parts: &[ObjectPartRecord],
        streaming_segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), ServerError> {
        use std::collections::BTreeMap;

        let mut segments_by_part: BTreeMap<u32, Vec<MultipartReclaimPartSegmentRecord>> =
            BTreeMap::new();
        for segment in streaming_segments {
            segments_by_part
                .entry(segment.part_number)
                .or_default()
                .push(MultipartReclaimPartSegmentRecord {
                    part_number: segment.part_number,
                    segment_index: segment.segment_index,
                    segment_okh: segment.segment_okh,
                    segment_vid: segment.segment_vid,
                    shard_pg_id: segment.shard_pg_id,
                    ec: EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                });
        }

        let parts = parts
            .iter()
            .map(|part| {
                if part.part_okh == [0u8; 16] {
                    MultipartReclaimPartRecord::Segments {
                        part_number: part.part_number,
                        segments: segments_by_part
                            .remove(&part.part_number)
                            .unwrap_or_default(),
                    }
                } else {
                    MultipartReclaimPartRecord::ShardSet {
                        part_number: part.part_number,
                        part_okh: part.part_okh,
                        part_vid: part.part_vid,
                        shard_pg_id: part.shard_pg_id,
                        ec: EcShape {
                            k: part.ec_k,
                            m: part.ec_m,
                        },
                    }
                }
            })
            .collect();

        meta_pg
            .put_multipart_reclaim(&MultipartReclaimRecord {
                bucket: BucketName::from(bucket),
                key: ObjectKey::from(key),
                generation_id,
                created_at: Self::now_millis(),
                parts,
            })
            .map_err(ServerError::Metadata)
    }

    fn delete_segment_shards_generic(
        &self,
        segments: &[MultipartPartSegmentRecord],
    ) -> Result<(), ServerError> {
        for segment in segments {
            self.delete_segment_shard_set(
                segment.shard_pg_id,
                &segment.segment_okh,
                segment.segment_vid,
                segment.ec_k,
                segment.ec_m,
            )?;
        }
        Ok(())
    }

    fn delete_segment_shard_set(
        &self,
        shard_pg_id: u32,
        segment_okh: &[u8; 16],
        segment_vid: GenerationId,
        ec_k: u8,
        ec_m: u8,
    ) -> Result<(), ServerError> {
        let pg = self.storage_node.get_pg(shard_pg_id)?;
        let total = ec_k as usize + ec_m as usize;
        for i in 0..total {
            let shard_key = ShardKey::new(segment_okh, segment_vid.get(), i as u8);
            pg.delete_shard(&shard_key)?;
        }
        Ok(())
    }

    /// Get an object from storage.
    pub fn get_object(&self, req: &GetObjectRequest) -> Result<GetObjectResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_object",
            "bucket={} key={} version_id={:?}",
            req.bucket,
            req.key,
            req.version_id
        );
        let bucket = req.bucket;
        let key = req.key;
        let version_id = req.version_id;
        let cond = req.cond;
        let requester = req.requester;
        let _bucket_info = self.authorize_bucket_read_requester(requester, bucket)?;
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
            let obj_parts =
                Self::snapshot_multipart_parts(meta_pg, bucket, key, record.version_id)?;

            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b.as_slice()).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

            let body = ReadHandle::from_multipart(
                self.read_runtime(),
                bucket,
                key,
                record.generation_id,
                obj_parts,
                record.size as usize,
            );
            drop(pgs);
            #[cfg(test)]
            maybe_run_multipart_snapshot_hook(bucket, key);

            Ok(GetObjectResult {
                body,
                metadata,
                etag: etag_str,
                size: record.size,
                last_modified: record.last_modified,
                version_id: record.version_id,
                tags: record.tags.map(Into::into),
            })
        } else {
            // Non-multipart: metadata from DB row, user data from shards.
            let etag_crc = record.etag.crc64();
            let user_size = record.size as usize;

            // Non-multipart payloads now read through committed object segments.
            let meta_pg = pgs.meta();
            let segments = meta_pg
                .get_object_segments(bucket, key, record.version_id)
                .map_err(ServerError::Metadata)?;

            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b.as_slice()).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

            let body = if user_size == 0 {
                drop(pgs);
                ReadHandle::from_buffered_bytes(vec![])
            } else {
                let body = ReadHandle::from_segments(
                    self.read_runtime(),
                    bucket,
                    key,
                    record.generation_id,
                    segment_payloads_from_object_segments(segments),
                    user_size,
                    Some(etag_crc),
                );
                drop(pgs);
                body
            };

            Ok(GetObjectResult {
                body,
                metadata,
                etag: etag_str,
                size: record.size,
                last_modified: record.last_modified,
                version_id: record.version_id,
                tags: record.tags.map(Into::into),
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
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_object_part",
            "bucket={} key={} part_number={} version_id={:?}",
            req.bucket,
            req.key,
            req.part_number,
            req.version_id
        );
        let bucket = req.bucket;
        let key = req.key;
        let version_id = req.version_id;
        let part_number = req.part_number;
        let cond = req.cond;
        let requester = req.requester;
        let _bucket_info = self.authorize_bucket_read_requester(requester, bucket)?;
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
            let obj_parts =
                Self::snapshot_multipart_parts(meta_pg, bucket, key, record.version_id)?;

            // Find the requested part
            let part = obj_parts
                .iter()
                .find(|p| p.record.part_number == part_number)
                .ok_or(ServerError::InvalidPart { part_number })?;

            let part_start = part.object_offset_start as u64;
            let part_end = part_start + part.record.size.saturating_sub(1);

            // Decode per-part checksum
            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b.as_slice()).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

            let checksum = if let Some(raw) = &part.record.checksum {
                // Look up algorithm from object metadata and validate byte length.
                match metadata
                    .get("x-amz-checksum-algorithm")
                    .and_then(ChecksumAlgorithm::parse)
                {
                    Some(algo) => Some(RawChecksum::new(algo, raw.clone()).map_err(|_| {
                        ServerError::InternalError {
                            reason: format!(
                                "stored checksum length {} does not match {} (expected {})",
                                raw.len(),
                                algo.as_str(),
                                algo.expected_byte_length(),
                            ),
                        }
                    })?),
                    None => None,
                }
            } else {
                None
            };

            let mut part_body = part.clone();
            part_body.object_offset_start = 0;
            let body = ReadHandle::from_multipart(
                self.read_runtime(),
                bucket,
                key,
                record.generation_id,
                vec![part_body],
                part.record.size as usize,
            );
            drop(pgs);
            #[cfg(test)]
            maybe_run_multipart_snapshot_hook(bucket, key);

            Ok(GetObjectPartResult {
                body,
                metadata,
                etag: etag_str,
                size: record.size,
                part_size: part.record.size,
                last_modified: record.last_modified,
                part_start,
                part_end,
                parts_count: obj_parts.len() as u32,
                version_id: record.version_id,
                tags: record.tags.map(Into::into),
                checksum,
            })
        } else {
            // Non-multipart: only partNumber=1 is valid
            if part_number != 1 {
                return Err(ServerError::InvalidPart { part_number });
            }

            let etag_crc = record.etag.crc64();
            let user_size = record.size as usize;

            // Non-multipart payloads now read through committed object segments.
            let meta_pg = pgs.meta();
            let segments = meta_pg
                .get_object_segments(bucket, key, record.version_id)
                .map_err(ServerError::Metadata)?;

            let body = if user_size == 0 {
                drop(pgs);
                ReadHandle::from_buffered_bytes(vec![])
            } else {
                let body = ReadHandle::from_segments(
                    self.read_runtime(),
                    bucket,
                    key,
                    record.generation_id,
                    segment_payloads_from_object_segments(segments),
                    user_size,
                    Some(etag_crc),
                );
                drop(pgs);
                body
            };

            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b.as_slice()).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

            Ok(GetObjectPartResult {
                body,
                metadata,
                etag: etag_str,
                size: record.size,
                part_size: record.size,
                last_modified: record.last_modified,
                part_start: 0,
                part_end: record.size.saturating_sub(1),
                parts_count: 1,
                version_id: record.version_id,
                tags: record.tags.map(Into::into),
                checksum: None,
            })
        }
    }

    /// Head a single part of an object by part number (no body).
    pub fn head_object_part(
        &self,
        req: &GetObjectPartRequest,
    ) -> Result<HeadObjectPartResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::head_object_part",
            "bucket={} key={} part_number={} version_id={:?}",
            req.bucket,
            req.key,
            req.part_number,
            req.version_id
        );
        let bucket = req.bucket;
        let key = req.key;
        let version_id = req.version_id;
        let part_number = req.part_number;
        let cond = req.cond;
        let requester = req.requester;
        let _bucket_info = self.authorize_bucket_read_requester(requester, bucket)?;
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
                .map(|b| MetadataBlob::deserialize(b.as_slice()).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

            let checksum = if let Some(raw) = &part.checksum {
                match metadata
                    .get("x-amz-checksum-algorithm")
                    .and_then(ChecksumAlgorithm::parse)
                {
                    Some(algo) => Some(RawChecksum::new(algo, raw.clone()).map_err(|_| {
                        ServerError::InternalError {
                            reason: format!(
                                "stored checksum length {} does not match {} (expected {})",
                                raw.len(),
                                algo.as_str(),
                                algo.expected_byte_length(),
                            ),
                        }
                    })?),
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
                tags: record.tags.map(Into::into),
                checksum,
            })
        } else {
            if part_number != 1 {
                return Err(ServerError::InvalidPart { part_number });
            }

            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b.as_slice()).map(|(m, _)| m))
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
                tags: record.tags.map(Into::into),
                checksum: None,
            })
        }
    }

    /// Head object: returns metadata without body.
    ///
    /// Metadata is always read from the DB row (no shard read needed).
    pub fn head_object(&self, req: &GetObjectRequest) -> Result<HeadObjectResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::head_object",
            "bucket={} key={} version_id={:?}",
            req.bucket,
            req.key,
            req.version_id
        );
        let bucket = req.bucket;
        let key = req.key;
        let version_id = req.version_id;
        let cond = req.cond;
        let requester = req.requester;
        let _bucket_info = self.authorize_bucket_read_requester(requester, bucket)?;
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
            .map(|b| MetadataBlob::deserialize(b.as_slice()).map(|(m, _)| m))
            .transpose()?
            .unwrap_or_default();

        Ok(HeadObjectResult {
            metadata,
            etag: etag_str,
            size: record.size,
            last_modified: record.last_modified,
            version_id: record.version_id,
            tags: record.tags.map(Into::into),
        })
    }

    /// Retrieve object attributes, optionally including multipart ObjectParts
    /// with pagination support.
    pub fn get_object_attributes(
        &self,
        req: &GetObjectAttributesRequest,
    ) -> Result<GetObjectAttributesResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_object_attributes",
            "bucket={} key={} want_parts={} version_id={:?}",
            req.bucket,
            req.key,
            req.want_parts,
            req.version_id
        );
        let bucket = req.bucket;
        let key = req.key;
        let version_id = req.version_id;
        let cond = req.cond;
        let want_parts = req.want_parts;
        let part_number_marker = req.part_number_marker;
        let max_parts = req.max_parts;
        let requester = req.requester;
        let _bucket_info = self.authorize_bucket_read_requester(requester, bucket)?;
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
            .map(|b| MetadataBlob::deserialize(b.as_slice()).map(|(m, _)| m))
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

                    let next_part_number_marker = if page.is_empty() {
                        Some(marker)
                    } else {
                        page.last().map(|p| p.part_number)
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
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::get_object_range",
            "bucket={} key={} version_id={:?} requested_range={}",
            req.bucket,
            req.key,
            req.version_id,
            req.range
        );
        let bucket = req.bucket;
        let key = req.key;
        let version_id = req.version_id;
        let range = req.range;
        let cond = req.cond;
        let requester = req.requester;
        let _bucket_info = self.authorize_bucket_read_requester(requester, bucket)?;
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
        let (user_start, user_end) = match range.resolve(record.size) {
            Some(resolved) => resolved,
            None => {
                if let Some(trace) = observability::current_context() {
                    let _ = observability::event_in_context(
                        &trace,
                        TRACE_TARGET,
                        "get_object_range_invalid",
                        Some(format_args!(
                            "bucket={} key={} version_id={:?} requested_range={} object_size={}",
                            bucket, key, version_id, range, record.size
                        )),
                    );
                }
                return Err(ServerError::InvalidRange {
                    total_size: record.size,
                });
            }
        };
        if let Some(trace) = observability::current_context() {
            let _ = observability::event_in_context(
                &trace,
                TRACE_TARGET,
                "get_object_range_resolved",
                Some(format_args!(
                    "bucket={} key={} version_id={:?} requested_range={} object_size={} resolved_start={} resolved_end={} resolved_len={}",
                    bucket,
                    key,
                    version_id,
                    range,
                    record.size,
                    user_start,
                    user_end,
                    user_end - user_start + 1
                )),
            );
        }

        let (metadata, body) = if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
            // Multipart: metadata from object row, data spans parts.
            let meta_pg = pgs.meta();
            let obj_parts = Self::snapshot_multipart_parts_overlapping_range(
                meta_pg,
                bucket,
                key,
                record.version_id,
                user_start,
                user_end + 1,
            )?;
            if obj_parts.is_empty() {
                return Err(ServerError::InternalError {
                    reason: format!(
                        "multipart range resolved to no parts for {bucket}/{key} version {version_id:?} at {user_start}-{user_end}"
                    ),
                });
            }

            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b.as_slice()).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

            let body = ReadHandle::from_multipart_range(
                self.read_runtime(),
                bucket,
                key,
                record.generation_id,
                obj_parts,
                user_start as usize,
                user_end as usize,
            );
            drop(pgs);
            #[cfg(test)]
            maybe_run_multipart_snapshot_hook(bucket, key);
            (metadata, body)
        } else {
            // Non-multipart: metadata from DB row, user data from shards.
            let metadata = record
                .metadata_blob
                .as_ref()
                .map(|b| MetadataBlob::deserialize(b.as_slice()).map(|(m, _)| m))
                .transpose()?
                .unwrap_or_default();

            // Non-multipart payloads now read through committed object segments.
            let meta_pg = pgs.meta();
            let segments = meta_pg
                .get_object_segments(bucket, key, record.version_id)
                .map_err(ServerError::Metadata)?;

            let body = ReadHandle::from_segments_range(
                self.read_runtime(),
                bucket,
                key,
                record.generation_id,
                segment_payloads_from_object_segments(segments),
                user_start as usize,
                user_end as usize,
            );
            drop(pgs);

            (metadata, body)
        };

        Ok(GetObjectRangeResult {
            body,
            metadata,
            etag: etag_str,
            size: record.size,
            last_modified: record.last_modified,
            range_start: user_start,
            range_end: user_end,
            version_id: record.version_id,
            tags: record.tags.map(Into::into),
        })
    }

    /// Delete an object.
    pub fn delete_object(
        &self,
        req: &DeleteObjectRequest,
    ) -> Result<DeleteObjectResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_object",
            "bucket={} key={} version_id={:?}",
            req.bucket,
            req.key,
            req.version_id
        );
        let bucket = req.bucket;
        let key = req.key;
        let request_version_id = req.version_id;
        let cond = req.cond;
        let requester = req.requester;
        let bucket_info = self.authorize_object_write_requester(requester, bucket)?;

        match (bucket_info.versioning, request_version_id) {
            // Unversioned bucket: physical delete (current behavior)
            (BucketVersioningState::Disabled, _) => {
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
                            version_id: VersionId::Null,
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
                            version_id: VersionId::Null,
                            delete_marker: false,
                        });
                    }
                };

                let meta_pg = pgs.meta();

                // Check delete conditions
                if !cond.is_empty() {
                    let etag_str = record.etag.format();
                    check_delete_conditions(cond, &etag_str)?;
                }

                if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
                    // Multipart: collect parts, delete metadata under lock,
                    // then reclaim part payloads after releasing the lock.
                    let obj_parts = meta_pg
                        .get_object_parts(bucket, key, record.version_id)
                        .map_err(ServerError::Metadata)?;
                    // Collect object segments for streaming parts before deleting metadata.
                    let mut streaming_segments: Vec<MultipartPartSegmentRecord> = Vec::new();
                    for part in &obj_parts {
                        if part.part_okh == [0u8; 16] {
                            let segments = meta_pg
                                .get_multipart_part_segments(
                                    bucket,
                                    key,
                                    record.version_id,
                                    part.part_number,
                                )
                                .map_err(ServerError::Metadata)?;
                            streaming_segments.extend(segments);
                        }
                    }
                    Self::enqueue_multipart_reclaim(
                        meta_pg,
                        bucket,
                        key,
                        record.generation_id,
                        &obj_parts,
                        &streaming_segments,
                    )?;
                    if !streaming_segments.is_empty() {
                        meta_pg
                            .delete_multipart_part_segments(bucket, key, record.version_id)
                            .map_err(ServerError::Metadata)?;
                    }
                    meta_pg.delete_object_parts(bucket, key, record.version_id)?;
                    meta_pg.delete_object_meta(bucket, key)?;
                    drop(pgs);
                    #[cfg(test)]
                    maybe_run_multipart_delete_metadata_hook(bucket, key);
                    self.read_runtime().enqueue_object_payload_reclaim(
                        bucket,
                        key,
                        record.generation_id,
                    );
                } else {
                    let vid = record.version_id;

                    // Segment-manifest payloads are now the only non-multipart layout.
                    let segments = meta_pg
                        .get_object_segments(bucket, key, vid)
                        .map_err(ServerError::Metadata)?;

                    Self::enqueue_object_segments_reclaim(
                        meta_pg,
                        bucket,
                        key,
                        record.generation_id,
                        &segments,
                    )?;
                    meta_pg
                        .delete_object_segments(bucket, key, vid)
                        .map_err(ServerError::Metadata)?;
                    meta_pg.delete_object_meta(bucket, key)?;
                    drop(pgs);
                    #[cfg(test)]
                    maybe_run_object_segments_delete_metadata_hook(bucket, key);
                    self.read_runtime().enqueue_object_payload_reclaim(
                        bucket,
                        key,
                        record.generation_id,
                    );
                }

                Ok(DeleteObjectResult {
                    version_id: VersionId::Null,
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
                    Err(
                        ServerError::ObjectNotFound { .. } | ServerError::VersionNotFound { .. },
                    ) => {
                        return Ok(DeleteObjectResult {
                            version_id: vid,
                            delete_marker: false,
                        });
                    }
                    Err(other) => return Err(other),
                };

                let meta_pg = pgs.meta();
                let is_delete_marker = stored.is_delete_marker();

                // Delete shards if it's a live object (not a delete marker)
                if let StoredObject::Live(record) = &stored {
                    if matches!(record.layout, ObjectLayout::MultipartManifest { .. }) {
                        let obj_parts = meta_pg
                            .get_object_parts(bucket, key, vid)
                            .map_err(ServerError::Metadata)?;
                        // Collect object segments for streaming parts.
                        let mut streaming_segments: Vec<MultipartPartSegmentRecord> = Vec::new();
                        for part in &obj_parts {
                            if part.part_okh == [0u8; 16] {
                                let segments = meta_pg
                                    .get_multipart_part_segments(bucket, key, vid, part.part_number)
                                    .map_err(ServerError::Metadata)?;
                                streaming_segments.extend(segments);
                            }
                        }
                        Self::enqueue_multipart_reclaim(
                            meta_pg,
                            bucket,
                            key,
                            record.generation_id,
                            &obj_parts,
                            &streaming_segments,
                        )?;
                        if !streaming_segments.is_empty() {
                            meta_pg
                                .delete_multipart_part_segments(bucket, key, vid)
                                .map_err(ServerError::Metadata)?;
                        }
                        meta_pg.delete_object_parts(bucket, key, vid)?;
                        meta_pg.delete_object_version(bucket, key, vid)?;
                        drop(pgs);
                        #[cfg(test)]
                        maybe_run_multipart_delete_metadata_hook(bucket, key);
                        self.read_runtime().enqueue_object_payload_reclaim(
                            bucket,
                            key,
                            record.generation_id,
                        );

                        return Ok(DeleteObjectResult {
                            version_id: vid,
                            delete_marker: false,
                        });
                    }

                    // Segment-manifest payloads are now the only non-multipart layout.
                    let segments = meta_pg
                        .get_object_segments(bucket, key, vid)
                        .map_err(ServerError::Metadata)?;

                    Self::enqueue_object_segments_reclaim(
                        meta_pg,
                        bucket,
                        key,
                        record.generation_id,
                        &segments,
                    )?;
                    meta_pg
                        .delete_object_segments(bucket, key, vid)
                        .map_err(ServerError::Metadata)?;
                    meta_pg.delete_object_version(bucket, key, vid)?;
                    drop(pgs);
                    #[cfg(test)]
                    maybe_run_object_segments_delete_metadata_hook(bucket, key);
                    self.read_runtime().enqueue_object_payload_reclaim(
                        bucket,
                        key,
                        record.generation_id,
                    );
                    return Ok(DeleteObjectResult {
                        version_id: vid,
                        delete_marker: false,
                    });
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
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::list_objects_v2",
            "bucket={} max_keys={}",
            req.bucket,
            req.max_keys
        );
        let bucket = req.bucket;
        let prefix = req.prefix;
        let delimiter = req.delimiter;
        let continuation_token = req.continuation_token;
        let max_keys = req.max_keys;
        let bucket_info = self.authorize_bucket_read_requester(req.requester, bucket)?;

        // MaxKeys=0 is valid per S3 spec: return empty result
        if max_keys == 0 {
            return Ok(ListObjectsResult {
                objects: Vec::new(),
                common_prefixes: Vec::new(),
                is_truncated: false,
                next_continuation_token: None,
                owner_principal: bucket_info.owner_principal,
                owner_canonical_id: bucket_info.owner_canonical_id,
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
            owner_canonical_id: bucket_info.owner_canonical_id,
        })
    }

    /// List object versions in a bucket.
    pub fn list_object_versions(
        &self,
        req: &ListObjectVersionsRequest,
    ) -> Result<ListObjectVersionsResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::list_object_versions",
            "bucket={} max_keys={}",
            req.bucket,
            req.max_keys
        );
        let bucket = req.bucket;
        let prefix = req.prefix;
        let key_marker = req.key_marker;
        let version_id_marker = req.version_id_marker;
        let max_keys = req.max_keys;
        let bucket_info = self.authorize_bucket_read_requester(req.requester, bucket)?;

        if max_keys == 0 {
            return Ok(ListObjectVersionsResult {
                versions: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_version_id_marker: None,
                owner_principal: bucket_info.owner_principal,
                owner_canonical_id: bucket_info.owner_canonical_id,
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
            owner_principal: bucket_info.owner_principal,
            owner_canonical_id: bucket_info.owner_canonical_id,
        })
    }

    /// Batch-delete objects.
    pub fn delete_objects(
        &self,
        req: &DeleteObjectsRequest,
    ) -> Result<DeleteObjectsResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::delete_objects",
            "bucket={} objects={}",
            req.bucket,
            req.entries.len()
        );
        let bucket = req.bucket;
        let entries = req.entries;
        let cond = req.cond;
        let requester = req.requester;
        let _bucket_info = self.authorize_bucket_admin_requester(requester, bucket)?;

        let mut deleted = Vec::new();
        let mut errors = Vec::new();

        for entry in entries {
            match self.delete_object(&DeleteObjectRequest {
                bucket,
                key: entry.key,
                version_id: entry.version_id,
                cond,
                requester,
            }) {
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
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::create_multipart_upload",
            "bucket={} key={}",
            req.bucket,
            req.key
        );
        let bucket = req.bucket;
        let key = req.key;
        let metadata = req.metadata;
        let _bucket_guard = self.storage_node.lock_bucket(bucket);
        let bucket_info = self.authorize_object_write_requester(req.requester, bucket)?;

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
            metadata_blob: SerializedMetadataBlob::from(metadata_blob),
            owner_principal: Some(bucket_info.owner_principal),
            checksum: req.checksum,
        })?;

        Ok(CreateMultipartUploadResult { upload_id })
    }

    /// Copy a byte range from an existing object as a multipart upload part.
    pub fn upload_part_copy(
        &self,
        req: &UploadPartCopyRequest,
    ) -> Result<UploadPartCopyResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::upload_part_copy",
            "src_bucket={} src_key={} dst_bucket={} dst_key={} upload_id={} part_number={}",
            req.source.bucket,
            req.source.key,
            req.dst_bucket,
            req.dst_key,
            req.upload_id,
            req.part_number
        );
        let src_bucket = req.source.bucket;
        let src_key = req.source.key;
        let src_version_id = req.source.version_id;
        let dst_bucket = req.dst_bucket;
        let dst_key = req.dst_key;
        let upload_id = req.upload_id;
        let part_number = req.part_number;
        let src_cond = req.source.condition;
        let copy_source_range = req.copy_source_range;
        let requester = req.requester;

        let _dst_bucket_info = self.authorize_object_write_requester(requester, dst_bucket)?;
        let _src_bucket_info = self.authorize_bucket_read_requester(requester, src_bucket)?;

        let not_found = |e: ServerError| match e {
            ServerError::Store(storage::StoreError::NotFound) => ServerError::ObjectNotFound {
                bucket: src_bucket.to_string(),
                key: src_key.to_string(),
            },
            other => other,
        };

        // Phase 1: Snapshot source metadata and prepare a ranged read handle.
        let mut source_body = {
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
            let copy_size = if source_size == 0 {
                0
            } else {
                read_end - read_start + 1
            };
            if copy_size > MAX_OBJECT_SIZE {
                return Err(ServerError::ObjectTooLarge {
                    size: copy_size,
                    max: MAX_OBJECT_SIZE,
                });
            }

            if source_size == 0 {
                drop(pgs);
                ReadHandle::from_buffered_bytes(Vec::new())
            } else if matches!(src_record.layout, ObjectLayout::MultipartManifest { .. }) {
                let meta_pg = pgs.meta();
                let obj_parts = Self::snapshot_multipart_parts(
                    meta_pg,
                    src_bucket,
                    src_key,
                    src_record.version_id,
                )?;
                let body = ReadHandle::from_multipart_range(
                    self.read_runtime(),
                    src_bucket,
                    src_key,
                    src_record.generation_id,
                    obj_parts,
                    read_start as usize,
                    read_end as usize,
                );
                drop(pgs);
                #[cfg(test)]
                maybe_run_multipart_snapshot_hook(src_bucket, src_key);
                body
            } else {
                // Non-multipart source: check for object segments first.
                let meta_pg = pgs.meta();
                let segments = meta_pg
                    .get_object_segments(src_bucket, src_key, src_record.version_id)
                    .map_err(ServerError::Metadata)?;

                let body = ReadHandle::from_segments_range(
                    self.read_runtime(),
                    src_bucket,
                    src_key,
                    src_record.generation_id,
                    segment_payloads_from_object_segments(segments),
                    read_start as usize,
                    read_end as usize,
                );
                drop(pgs);
                body
            }
        }; // source locks dropped here

        // Phase 2: Stream into the destination multipart part session.
        let session = self.begin_stream_part(&BeginStreamPartRequest {
            bucket: dst_bucket,
            key: dst_key,
            upload_id,
            part_number,
            requester,
        })?;
        let session_id = &session.session_id;
        let result = (|| {
            let mut crc64 = checksum::crc64::Hasher::new();
            let mut total_size = 0u64;
            let mut segment_index = 0u32;
            let mut computed_checksum = session
                .checksum_algorithm
                .map(StreamingChecksumAccumulator::new);

            while let Some(chunk) = source_body
                .next_chunk(INTERNAL_SEGMENT_SIZE)
                .map_err(not_found)?
            {
                total_size = total_size.checked_add(chunk.len() as u64).ok_or_else(|| {
                    ServerError::InternalError {
                        reason: "upload part copy size overflow".to_string(),
                    }
                })?;
                crc64.update(&chunk);
                if let Some(checksum) = computed_checksum.as_mut() {
                    checksum.update(&chunk);
                }
                self.append_stream_segment(dst_bucket, dst_key, session_id, segment_index, &chunk)?;
                segment_index =
                    segment_index
                        .checked_add(1)
                        .ok_or_else(|| ServerError::InternalError {
                            reason: "too many upload part copy segments".to_string(),
                        })?;
            }

            let computed_checksum = match computed_checksum {
                Some(checksum) => Some(
                    RawChecksum::new(checksum.algorithm(), checksum.finalize()).map_err(|_| {
                        ServerError::InternalError {
                            reason: "checksum byte length mismatch".to_string(),
                        }
                    })?,
                ),
                None => None,
            };
            // UploadPartCopy has no checksum header/body claim from the client.
            // When the multipart upload is checksum-configured, treat the
            // server-computed checksum as the authoritative part claim.
            let claimed_checksum = computed_checksum.as_ref().map(|checksum| ChecksumClaim {
                algorithm: checksum.algorithm(),
                expected_bytes: checksum.bytes().to_vec(),
            });

            self.finalize_stream_part(FinalizeStreamPartRequest {
                bucket: dst_bucket,
                key: dst_key,
                session_id,
                upload_id,
                part_number,
                crc64: crc64.finalize(),
                total_size,
                claimed_checksum: claimed_checksum.as_ref(),
                computed_checksum,
            })
        })();
        if result.is_err() {
            let _ = self.abort_stream_put(dst_bucket, dst_key, session_id);
        }
        let inner = result?;
        let meta_pg = self
            .storage_node
            .get_pg(self.object_pg_id(dst_bucket, dst_key))?;
        let last_modified = meta_pg
            .get_multipart_part(upload_id, part_number)
            .map_err(ServerError::Metadata)?
            .last_modified;
        Ok(UploadPartCopyResult {
            etag: inner.etag,
            last_modified,
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
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::complete_multipart_upload",
            "bucket={} key={} upload_id={} parts={}",
            req.bucket,
            req.key,
            req.upload_id,
            req.parts.len()
        );
        let bucket = req.bucket;
        let key = req.key;
        let upload_id = req.upload_id;
        let parts = req.parts;
        let claimed_checksum = req.claimed_checksum;
        let _bucket_guard = self.storage_node.lock_bucket(bucket);

        // 1. Validate bucket exists and get versioning state.
        let bucket_info = self.authorize_object_write_requester(req.requester, bucket)?;

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
        let checksum_algo = upload.checksum.map(MultipartChecksumConfig::algorithm);
        let checksum_type = upload.checksum.map(MultipartChecksumConfig::checksum_type);

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
            if let Some(ref claim) = cp.checksum {
                // The checksum element type must match the upload's algorithm.
                if let Some(upload_algo) = checksum_algo {
                    if claim.algorithm() != upload_algo {
                        return Err(ServerError::InvalidRequest {
                            reason: format!(
                                "checksum element type {} does not match upload algorithm {}",
                                claim.algorithm().as_str(),
                                upload_algo.as_str()
                            ),
                        });
                    }
                }
                match &part.checksum {
                    Some(stored_bytes) => {
                        if claim.expected_bytes() != stored_bytes {
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
        let version_id = if bucket_info.versioning == BucketVersioningState::Enabled {
            meta_pg.next_version_id(bucket, key)?
        } else {
            VersionId::Null
        };
        let generation_id = meta_pg.next_generation_id(bucket, key)?;
        let stale_payload = if version_id.is_null() {
            Self::snapshot_overwritten_null_version_payload(&meta_pg, bucket, key)?
        } else {
            None
        };

        // 7. Compute composite multipart ETag.
        let part_etags: Vec<&[u8]> = part_records.iter().map(|p| p.etag.as_slice()).collect();
        let (etag_bytes_vec, etag_str) = compute_multipart_etag(&part_etags);
        let mut etag_crc64 = [0u8; 8];
        etag_crc64.copy_from_slice(&etag_bytes_vec);

        // 8. Compute total object size.
        let total_size: u64 = part_records.iter().map(|p| p.size).sum();
        if let Some(trace) = observability::current_context() {
            let _ = observability::event_in_context(
                &trace,
                TRACE_TARGET,
                "complete_multipart_layout",
                Some(format_args!(
                    "bucket={} key={} upload_id={} parts={} total_size={}",
                    bucket,
                    key,
                    upload_id,
                    part_records.len(),
                    total_size
                )),
            );
            let mut object_offset_start = 0u64;
            for (part_order, (requested_part, stored_part)) in
                parts.iter().zip(part_records.iter()).enumerate()
            {
                let object_offset_len = stored_part.size;
                let object_offset_end_exclusive = object_offset_start + object_offset_len;
                let _ = observability::event_in_context(
                    &trace,
                    TRACE_TARGET,
                    "complete_multipart_part_layout",
                    Some(format_args!(
                        "bucket={} key={} upload_id={} part_order={} part_number={} part_size={} object_offset_start={} object_offset_len={} object_offset_end_exclusive={} etag={}",
                        bucket,
                        key,
                        upload_id,
                        part_order,
                        requested_part.part_number,
                        stored_part.size,
                        object_offset_start,
                        object_offset_len,
                        object_offset_end_exclusive,
                        requested_part.etag
                    )),
                );
                object_offset_start = object_offset_end_exclusive;
            }
        }

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
        if let Some(claimed) = claimed_checksum {
            // Algorithm of the header must match the upload's algorithm.
            match checksum_algo {
                Some(upload_algo) if claimed.algorithm() != upload_algo => {
                    return Err(ServerError::InvalidRequest {
                        reason: format!(
                            "checksum header algorithm {} does not match upload algorithm {}",
                            claimed.algorithm().as_str(),
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
                if computed != claimed.encoded_value() {
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
                crate::metadata_blob::MetadataBlob::deserialize(metadata_blob_bytes.as_slice())?;
            blob.set(algo.header_name(), val);
            blob.set("x-amz-checksum-algorithm", algo.as_str());
            if let Some(ctype) = checksum_type {
                blob.set("x-amz-checksum-type", ctype.as_str());
            }
            metadata_blob_bytes = SerializedMetadataBlob::from(blob.serialize().map_err(|e| {
                ServerError::InvalidRequest {
                    reason: format!("failed to serialize metadata blob: {e}"),
                }
            })?);
        }

        // 9. Build the object metadata and manifest parts.
        let obj_req = CommitMultipartReq {
            bucket: BucketName::from(bucket),
            key: ObjectKey::from(key),
            version_id,
            generation_id,
            size: total_size,
            etag_crc64,
            ec: EcShape { k: 0, m: 0 }, // per-part, not per-object
            metadata_blob: Some(metadata_blob_bytes),
        };

        let object_parts: Vec<ObjectPartRecord> = part_records
            .iter()
            .map(|p| {
                let shard_pg_id = self.shard_pg_id_raw(
                    &format!("mpu/{}", p.upload_id),
                    &format!("{}/{}", p.part_number, p.generation),
                    p.part_vid.get(),
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

        if let Some(ref payload) = stale_payload {
            match payload {
                StaleObjectPayload::Multipart {
                    generation_id,
                    parts,
                    streaming_segments,
                } => {
                    // `complete_multipart_commit` already replaced the live
                    // object_parts rows for VersionId::Null, so only enqueue
                    // reclaim for the old payload.
                    Self::enqueue_multipart_reclaim(
                        &meta_pg,
                        bucket,
                        key,
                        *generation_id,
                        parts,
                        streaming_segments,
                    )?;
                }
                StaleObjectPayload::Segments { .. } => {
                    Self::delete_stale_object_payload_metadata(
                        &meta_pg, bucket, key, version_id, payload,
                    )?;
                }
            }
        }

        drop(meta_pg);
        if let Some(ref payload) = stale_payload {
            self.delete_stale_object_payload(bucket, key, payload);
        }

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
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::abort_multipart_upload",
            "bucket={} key={} upload_id={}",
            req.bucket,
            req.key,
            req.upload_id
        );
        let bucket = req.bucket;
        let key = req.key;
        let upload_id = req.upload_id;
        let _bucket_info = self.authorize_object_write_requester(req.requester, bucket)?;
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

        // 3b. Collect streaming object segments for shard cleanup.
        let streaming_segments = meta_pg
            .get_all_multipart_part_segments_for_upload(upload_id)
            .map_err(ServerError::Metadata)?;

        // 4. Drop meta PG lock before shard cleanup to avoid deadlocks.
        drop(meta_pg);

        // 5. Best-effort delete all shard sets for each non-streaming part.
        for part in &all_parts.parts {
            if part.part_okh == [0u8; 16] {
                continue; // streaming part — handled below
            }
            let shard_pg_id = self.shard_pg_id_raw(
                &format!("mpu/{upload_id}"),
                &format!("{}/{}", part.part_number, part.generation),
                part.part_vid.get(),
            );
            if let Ok(shard_pg) = self.storage_node.get_pg(shard_pg_id) {
                let k = part.ec_k as usize;
                let m = part.ec_m as usize;
                for i in 0..(k + m) {
                    let shard_key = ShardKey::new(&part.part_okh, part.part_vid.get(), i as u8);
                    let _ = shard_pg.delete_shard(&shard_key);
                }
            }
        }

        // 5b. Delete shard data for streaming part segments first, then
        //     delete the manifest rows. This order ensures that if shard
        //     deletion fails, the segment refs survive for retry.
        if !streaming_segments.is_empty() {
            self.delete_segment_shards_generic(&streaming_segments)?;
        }

        // 6. Re-acquire meta PG and delete upload + parts (CASCADE)
        //    and object segments rows.
        let meta_pg = self.storage_node.get_pg(meta_pg_id)?;
        if !streaming_segments.is_empty() {
            meta_pg
                .delete_multipart_part_segments_by_upload_id(upload_id)
                .map_err(ServerError::Metadata)?;
        }
        meta_pg
            .delete_multipart_upload(upload_id)
            .map_err(ServerError::Metadata)?;

        Ok(())
    }

    /// List parts of an in-progress multipart upload.
    pub fn list_parts(&self, req: &ListPartsRequest) -> Result<ListPartsResult, ServerError> {
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::list_parts",
            "bucket={} key={} upload_id={} max_parts={}",
            req.bucket,
            req.key,
            req.upload_id,
            req.max_parts
        );
        let bucket = req.bucket;
        let key = req.key;
        let upload_id = req.upload_id;
        let part_number_marker = req.part_number_marker;
        let max_parts = req.max_parts;
        let _bucket_info = self.authorize_bucket_read_requester(req.requester, bucket)?;
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
            checksum_algorithm: upload.checksum.map(MultipartChecksumConfig::algorithm),
            checksum_type: upload.checksum.map(MultipartChecksumConfig::checksum_type),
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
        observability::trace_scope!(
            TRACE_TARGET,
            "Coordinator::list_multipart_uploads",
            "bucket={} max_uploads={}",
            req.bucket,
            req.max_uploads
        );
        let bucket = req.bucket;
        let prefix = req.prefix;
        let key_marker = req.key_marker;
        let upload_id_marker = req.upload_id_marker;
        let max_uploads = req.max_uploads;
        let _bucket_info = self.authorize_bucket_read_requester(req.requester, bucket)?;

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

    fn finalize(self) -> Vec<u8> {
        match self {
            Self::Crc32(hasher) => hasher.finalize().to_be_bytes().to_vec(),
            Self::Crc32c(hasher) => hasher.finalize().to_be_bytes().to_vec(),
            Self::Crc64(hasher) => hasher.finalize().to_be_bytes().to_vec(),
            Self::Sha1(hasher) => hasher.finish().as_ref().to_vec(),
            Self::Sha256(hasher) => hasher.finish().as_ref().to_vec(),
        }
    }
}

/// Test helpers that exercise the real streaming upload path.
///
/// Available in-crate during `#[cfg(test)]` and cross-crate via the
/// `test-utils` Cargo feature.
#[cfg(any(test, feature = "test-utils"))]
pub mod test_helpers {
    use super::*;

    /// Put an object via the streaming path (begin → append → finalize).
    ///
    /// Aborts the streaming session on any append/finalize error to avoid
    /// leaking session rows and staged shard data.
    pub fn put_object(
        coord: &Coordinator,
        req: &PutObjectRequest<'_>,
    ) -> Result<PutObjectResult, ServerError> {
        let session_id = coord.begin_stream_put(&BeginStreamPutRequest {
            bucket: req.bucket,
            key: req.key,
            requester: req.requester,
            acl: req.acl,
        })?;
        let result = (|| {
            for (idx, chunk) in req.data.chunks(INTERNAL_SEGMENT_SIZE).enumerate() {
                coord.append_stream_segment(req.bucket, req.key, &session_id, idx as u32, chunk)?;
            }
            let crc = checksum::crc64::checksum(req.data);
            coord.finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: req.bucket,
                key: req.key,
                session_id: &session_id,
                crc64: crc,
                total_size: req.data.len() as u64,
                metadata_blob: req.metadata,
                tags: req.tags,
                cond: req.cond,
            })
        })();
        if result.is_err() {
            let _ = coord.abort_stream_put(req.bucket, req.key, &session_id);
        }
        result
    }

    /// Upload a multipart part via the streaming path (begin → append → finalize).
    ///
    /// Aborts the streaming session on any append/finalize error to avoid
    /// leaking session rows and staged shard data.
    pub fn upload_part(
        coord: &Coordinator,
        req: &UploadPartRequest<'_>,
    ) -> Result<UploadPartResult, ServerError> {
        let session = coord.begin_stream_part(&BeginStreamPartRequest {
            bucket: req.bucket,
            key: req.key,
            upload_id: req.upload_id,
            part_number: req.part_number,
            requester: req.requester,
        })?;
        let session_id = &session.session_id;
        let result = (|| {
            for (idx, chunk) in req.data.chunks(INTERNAL_SEGMENT_SIZE).enumerate() {
                coord.append_stream_segment(req.bucket, req.key, session_id, idx as u32, chunk)?;
            }
            let crc = checksum::crc64::checksum(req.data);
            let computed_checksum = {
                let algo = req
                    .claimed_checksum
                    .map(ChecksumClaim::algorithm)
                    .or(session.checksum_algorithm);
                match algo {
                    Some(a) => Some(RawChecksum::new(a, compute_checksum(a, req.data)).map_err(
                        |_| ServerError::InternalError {
                            reason: "checksum byte length mismatch".to_string(),
                        },
                    )?),
                    None => None,
                }
            };
            coord.finalize_stream_part(FinalizeStreamPartRequest {
                bucket: req.bucket,
                key: req.key,
                session_id,
                upload_id: req.upload_id,
                part_number: req.part_number,
                crc64: crc,
                total_size: req.data.len() as u64,
                claimed_checksum: req.claimed_checksum,
                computed_checksum,
            })
        })();
        if result.is_err() {
            let _ = coord.abort_stream_put(req.bucket, req.key, session_id);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::test_helpers;
    use super::*;
    use crate::conditional::{DeleteCondition, ReadCondition, SpecificEtag, WriteCondition};
    use std::path::{Path, PathBuf};
    use std::sync::mpsc;
    use std::sync::Barrier;
    use std::thread;
    use std::time::{Duration, Instant};

    const NO_READ: &ReadCondition = &ReadCondition {
        if_match: None,
        if_none_match: None,
        if_modified_since: None,
        if_unmodified_since: None,
    };
    const NO_WRITE: &WriteCondition = &WriteCondition::None;
    const NO_DELETE: &DeleteCondition = &DeleteCondition::None;
    const TEST_REQUESTER: Requester<'static> = Requester::system();
    const NO_PUT_OBJECT_ACL: PutObjectAcl<'static> = PutObjectAcl::None;

    fn read_all_body(mut body: ReadHandle) -> Result<Vec<u8>, ServerError> {
        let mut out = Vec::new();
        while let Some(chunk) = body.next_chunk(INTERNAL_SEGMENT_SIZE)? {
            out.extend_from_slice(&chunk);
        }
        Ok(out)
    }

    trait ReadHandleTestExt {
        fn read_all(self) -> Result<Vec<u8>, ServerError>;
    }

    impl ReadHandleTestExt for ReadHandle {
        fn read_all(self) -> Result<Vec<u8>, ServerError> {
            read_all_body(self)
        }
    }

    fn compute_shard_size(size: u64, ec_k: u8) -> usize {
        let k = u64::from(ec_k);
        let padded = size.div_ceil(k) * k;
        (padded / k) as usize
    }

    fn shards_for_byte_range(start: usize, end: usize, shard_size: usize, ec_k: u8) -> Vec<usize> {
        if shard_size == 0 {
            return vec![];
        }
        let first = start / shard_size;
        let last = (end / shard_size).min(ec_k as usize - 1);
        (first..=last).collect()
    }

    fn setup_coordinator(dir: &Path) -> Coordinator {
        let pg_ids: Vec<u32> = (0..4).collect();
        let storage_node = Arc::new(SharedStorageNode::open(dir, &pg_ids).unwrap());
        let ec_config = EcConfig::new(4, 2).unwrap();
        Coordinator::new(storage_node, ec_config, "us-east-1".to_string()).unwrap()
    }

    fn delete_bucket_test(coord: &Coordinator, name: &str) -> Result<(), ServerError> {
        coord.delete_bucket(&DeleteBucketRequest {
            name,
            requester: TEST_REQUESTER,
        })
    }

    fn wait_until_bucket_gone(coord: &Coordinator, name: &str) {
        for _ in 0..200 {
            if matches!(
                coord.head_bucket(name),
                Err(ServerError::BucketNotFound { .. })
            ) {
                let bucket_pg = coord.get_bucket_pg(name).unwrap();
                if bucket_pg.head_bucket_raw(name).is_err() {
                    return;
                }
            }
            thread::sleep(Duration::from_millis(5));
        }
        panic!("bucket {name} was not fully removed");
    }

    fn begin_stream_put_test(
        coord: &Coordinator,
        bucket: &str,
        key: &str,
    ) -> Result<String, ServerError> {
        coord.begin_stream_put(&BeginStreamPutRequest {
            bucket,
            key,
            requester: TEST_REQUESTER,
            acl: NO_PUT_OBJECT_ACL,
        })
    }

    fn begin_stream_part_test(
        coord: &Coordinator,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
    ) -> Result<BeginStreamPartResult, ServerError> {
        coord.begin_stream_part(&BeginStreamPartRequest {
            bucket,
            key,
            upload_id,
            part_number,
            requester: TEST_REQUESTER,
        })
    }

    fn find_fresh_key_with_meta_pg_gt_shard_pg(
        coord: &Coordinator,
        bucket: &str,
        prefix: &str,
    ) -> String {
        for suffix in 0..1024 {
            let key = format!("{prefix}-{suffix}");
            let meta_pg_id = coord.object_pg_id(bucket, &key);
            let shard_pg_id = coord.shard_pg_id(bucket, &key, GenerationId::MIN);
            if meta_pg_id > shard_pg_id {
                return key;
            }
        }
        panic!("failed to find a key with meta_pg_id > shard_pg_id");
    }

    fn assert_object_maps_meta_pg_gt_shard_pg(coord: &Coordinator, bucket: &str, key: &str) {
        let meta_pg_id = coord.object_pg_id(bucket, key);
        let meta_pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let generation_id = match meta_pg.get_object_meta(bucket, key).unwrap() {
            StoredObject::Live(record) => record.generation_id,
            StoredObject::DeleteMarker(other) => {
                panic!("expected live object for {bucket}/{key}, got {other:?}")
            }
        };
        let shard_pg_id = coord.shard_pg_id(bucket, key, generation_id);
        assert!(
            meta_pg_id > shard_pg_id,
            "expected test object {bucket}/{key} to map to old read slow path: meta_pg_id={meta_pg_id} shard_pg_id={shard_pg_id}"
        );
    }

    struct MultipartMetadataRaceSync {
        snapshot_reached: Arc<Barrier>,
        snapshot_resume: Arc<Barrier>,
        delete_reached: Arc<Barrier>,
        delete_resume: Arc<Barrier>,
        _serial_guard: MutexGuard<'static, ()>,
        _guard: ReclamationTestHookGuard,
    }

    fn install_multipart_metadata_race_hooks(bucket: &str, key: &str) -> MultipartMetadataRaceSync {
        let serial = RECLAMATION_TEST_SERIAL
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap();
        let snapshot_reached = Arc::new(Barrier::new(2));
        let snapshot_resume = Arc::new(Barrier::new(2));
        let delete_reached = Arc::new(Barrier::new(2));
        let delete_resume = Arc::new(Barrier::new(2));
        let snapshot_reached_hook = Arc::clone(&snapshot_reached);
        let snapshot_resume_hook = Arc::clone(&snapshot_resume);
        let delete_reached_hook = Arc::clone(&delete_reached);
        let delete_resume_hook = Arc::clone(&delete_resume);
        let guard = install_reclamation_test_hooks(ReclamationTestHooks {
            target: Some((bucket.to_string(), key.to_string())),
            after_multipart_snapshot: Some(Arc::new(move || {
                snapshot_reached_hook.wait();
                snapshot_resume_hook.wait();
            })),
            after_multipart_delete_metadata: Some(Arc::new(move || {
                delete_reached_hook.wait();
                delete_resume_hook.wait();
            })),
            ..ReclamationTestHooks::default()
        });
        MultipartMetadataRaceSync {
            snapshot_reached,
            snapshot_resume,
            delete_reached,
            delete_resume,
            _serial_guard: serial,
            _guard: guard,
        }
    }

    struct ObjectSegmentsDeleteRaceSync {
        first_segment_reached: Arc<Barrier>,
        first_segment_resume: Arc<Barrier>,
        delete_reached: Arc<Barrier>,
        delete_resume: Arc<Barrier>,
        _serial_guard: MutexGuard<'static, ()>,
        _guard: ReclamationTestHookGuard,
    }

    fn install_object_segments_delete_race_hooks(
        bucket: &str,
        key: &str,
    ) -> ObjectSegmentsDeleteRaceSync {
        let serial = RECLAMATION_TEST_SERIAL
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap();
        let first_segment_reached = Arc::new(Barrier::new(2));
        let first_segment_resume = Arc::new(Barrier::new(2));
        let delete_reached = Arc::new(Barrier::new(2));
        let delete_resume = Arc::new(Barrier::new(2));
        let first_segment_reached_hook = Arc::clone(&first_segment_reached);
        let first_segment_resume_hook = Arc::clone(&first_segment_resume);
        let delete_reached_hook = Arc::clone(&delete_reached);
        let delete_resume_hook = Arc::clone(&delete_resume);
        let guard = install_reclamation_test_hooks(ReclamationTestHooks {
            target: Some((bucket.to_string(), key.to_string())),
            after_object_segments_first_segment: Some(Arc::new(move || {
                first_segment_reached_hook.wait();
                first_segment_resume_hook.wait();
            })),
            after_object_segments_delete_metadata: Some(Arc::new(move || {
                delete_reached_hook.wait();
                delete_resume_hook.wait();
            })),
            ..ReclamationTestHooks::default()
        });
        ObjectSegmentsDeleteRaceSync {
            first_segment_reached,
            first_segment_resume,
            delete_reached,
            delete_resume,
            _serial_guard: serial,
            _guard: guard,
        }
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
        delete_bucket_test(&coord, "test-bucket").unwrap();
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
            .map(|b| b.name.clone())
            .collect();
        assert_eq!(names, vec!["alpha", "beta", "mango", "zz-top"]);
    }

    #[test]
    fn list_buckets_for_requester_rejects_anonymous() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .list_buckets_for_requester(&ListBucketsRequest {
                requester: Requester::anonymous(),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn create_bucket_for_requester_sets_ownership_controls() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_requester(&CreateBucketRequest {
                name: "bucket",
                requester: Requester::principal("owner-a"),
                acl: BucketAcl::Private,
                ownership: BucketObjectOwnership::ObjectWriter,
            })
            .unwrap();

        let controls = coord
            .get_bucket_ownership_controls("bucket", Requester::principal("owner-a"))
            .unwrap()
            .unwrap();
        assert!(controls.contains("<ObjectOwnership>ObjectWriter</ObjectOwnership>"));
    }

    #[test]
    fn create_bucket_for_requester_idempotent_create_does_not_overwrite_ownership_controls() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord
            .create_bucket_for_requester(&CreateBucketRequest {
                name: "bucket",
                requester: Requester::principal("owner-a"),
                acl: BucketAcl::Private,
                ownership: BucketObjectOwnership::ObjectWriter,
            })
            .unwrap();

        coord
            .create_bucket_for_requester(&CreateBucketRequest {
                name: "bucket",
                requester: Requester::principal("owner-a"),
                acl: BucketAcl::Private,
                ownership: BucketObjectOwnership::BucketOwnerEnforced,
            })
            .unwrap();

        let controls = coord
            .get_bucket_ownership_controls("bucket", Requester::principal("owner-a"))
            .unwrap()
            .unwrap();
        assert!(controls.contains("<ObjectOwnership>ObjectWriter</ObjectOwnership>"));
        assert!(!controls.contains("<ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>"));
    }

    #[test]
    fn create_bucket_for_requester_rejects_public_read_with_owner_enforced() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .create_bucket_for_requester(&CreateBucketRequest {
                name: "bucket",
                requester: Requester::principal("owner-a"),
                acl: BucketAcl::PublicRead,
                ownership: BucketObjectOwnership::BucketOwnerEnforced,
            })
            .unwrap_err();
        assert!(matches!(
            err,
            ServerError::InvalidBucketAclWithObjectOwnership
        ));
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
            .map(|b| b.name.clone())
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
            .list_objects_v2(&ListObjectsV2Request {
                bucket,
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
                requester: TEST_REQUESTER,
            })
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
            .list_object_versions(&ListObjectVersionsRequest {
                bucket,
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                max_keys: 1000,
                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket,
                key,
                metadata: &MetadataBlob::new(),
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let resp = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket,
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1000,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(resp.uploads.len(), 1);
        assert_eq!(resp.uploads[0].key, key);
    }

    #[test]
    fn delete_nonempty_bucket_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
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
        coord.create_bucket("bucket").unwrap();

        let tags_xml =
            "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: Some(tags_xml),
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.tags.as_deref(), Some(tags_xml));
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
            let res = test_helpers::put_object(
                &writer,
                &PutObjectRequest {
                    bucket: "bucket",
                    key: "key",
                    data: b"data",
                    metadata: &MetadataBlob::new(),
                    tags: None,
                    cond: NO_WRITE,
                    requester: TEST_REQUESTER,
                    acl: NO_PUT_OBJECT_ACL,
                },
            );
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
            let res = delete_bucket_test(&deleter, "bucket");
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
        let result = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "hello.txt",
                data: b"Hello, world!",
                metadata: &MetadataBlob::from_headers(&headers).unwrap(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        assert!(!result.etag.is_empty());

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "hello.txt",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"Hello, world!");
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
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "obj",
                data: b"{}",
                metadata: &MetadataBlob::from_headers(&headers).unwrap(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "obj",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"{}");
        assert_eq!(obj.metadata.get("content-type"), Some("application/json"));
        assert_eq!(obj.metadata.get("x-amz-meta-author"), Some("alice"));
        assert_eq!(obj.metadata.get("x-amz-meta-version"), Some("42"));
    }

    #[test]
    fn head_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::from_headers(&[("Content-Type", "text/plain")]).unwrap(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let head = coord
            .head_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(head.size, 4);
        assert_eq!(head.metadata.get("content-type"), Some("text/plain"));
    }

    #[test]
    fn overwrite_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"v1",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"v2",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"v2");
    }

    #[test]
    fn empty_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "empty",
                data: b"",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "empty",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"");
        assert_eq!(obj.size, 0);
    }

    #[test]
    fn delete_object_then_get_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        coord
            .delete_object(&DeleteObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
            .unwrap();

        let err = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn delete_object_eventually_reclaims_simple_shards() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"simple-data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let (generation_id, ec) = {
            let meta_pg_id = coord.object_pg_id("bucket", "key");
            let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            match pg.get_object_meta("bucket", "key").unwrap() {
                StoredObject::Live(record) => (record.generation_id, record.ec),
                other @ StoredObject::DeleteMarker(_) => {
                    panic!("expected live object, got {other:?}")
                }
            }
        };
        let shard_pg_id = coord.shard_pg_id("bucket", "key", generation_id);
        let okh = object_key_hash("bucket", "key");

        coord
            .delete_object(&DeleteObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
            .unwrap();

        wait_for_shard_set_deletion(&coord, shard_pg_id, &okh, generation_id, ec);
    }

    #[test]
    fn delete_nonexistent_object_is_ok() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        // Should not error
        coord
            .delete_object(&DeleteObjectRequest {
                bucket: "bucket",
                key: "no-such-key",
                version_id: None,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
            .unwrap();
    }

    #[test]
    fn list_objects() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "a/1",
                data: b"1",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "a/2",
                data: b"2",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "b/1",
                data: b"3",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
                requester: TEST_REQUESTER,
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

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "photos/cat.jpg",
                data: b"cat",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "photos/dog.jpg",
                data: b"dog",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "docs/readme.md",
                data: b"md",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: Some("photos/"),
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.objects.len(), 2);
    }

    #[test]
    fn list_objects_with_delimiter() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "photos/cat.jpg",
                data: b"cat",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "photos/dog.jpg",
                data: b"dog",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "docs/readme.md",
                data: b"md",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "root.txt",
                data: b"root",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: Some("/"),
                continuation_token: None,
                max_keys: 1000,
                requester: TEST_REQUESTER,
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

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "folder/",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "folder/",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
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
            let record = pg.get_object_meta(bucket, key).unwrap();
            let segments = pg
                .get_object_segments(bucket, key, record.version_id())
                .unwrap();
            if let Some(segment) = segments.first() {
                (
                    segment.shard_pg_id,
                    segment.segment_okh,
                    segment.segment_vid,
                )
            } else {
                let live = record.as_live().expect("expected live object");
                (
                    coord.shard_pg_id(bucket, key, live.generation_id),
                    object_key_hash(bucket, key),
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"This data should survive shard loss!";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "resilient",
                data,
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // Delete one data shard using the helper
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "resilient", 0);

        // Get should still succeed via EC reconstruction
        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "resilient",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), data);
    }

    #[test]
    fn ec_drop_one_data_shard_get() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC single shard loss test data";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "obj1",
                data,
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj1", 0);

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "obj1",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), data);
    }

    #[test]
    fn ec_degraded_read_reuses_reconstruction_scratch() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = vec![5u8; INTERNAL_SEGMENT_SIZE];
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "obj-reconstruct",
                data: &data,
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj-reconstruct", 0);

        assert_eq!(coord.payload_buffer_pool.allocation_count(), 0);

        let first = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "obj-reconstruct",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(first.body.read_all().unwrap(), data);
        assert_eq!(coord.payload_buffer_pool.allocation_count(), 2);

        let second = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "obj-reconstruct",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(second.body.read_all().unwrap(), data);
        assert_eq!(coord.payload_buffer_pool.allocation_count(), 2);
    }

    #[test]
    fn ec_drop_m_shards_at_limit() {
        // Config: k=4, m=2. Dropping exactly m=2 shards should still recover.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC m-shard loss limit test data";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "obj2",
                data,
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // Delete 2 data shards (indices 0 and 1)
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj2", 0);
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj2", 1);

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "obj2",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), data);
    }

    #[test]
    fn ec_drop_m_plus_one_shards_fails() {
        // Config: k=4, m=2. Dropping m+1=3 shards should fail.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC m+1 shard loss test data";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "obj3",
                data,
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // Delete 3 shards (indices 0, 1, 2)
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj3", 0);
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj3", 1);
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj3", 2);

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "obj3",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        let err = obj.body.read_all().unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn ec_corrupt_one_data_shard_recovery() {
        // Corrupt shard 0 on disk. PgStore detects CRC mismatch, EC reconstructs.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC corruption recovery test data";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "obj4",
                data,
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        corrupt_shard_on_disk(&coord, tmp.path(), "bucket", "obj4", 0);

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "obj4",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), data);
    }

    #[test]
    fn ec_range_get_with_missing_shard() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"Hello, World! Range test with EC recovery";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "obj5",
                data,
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // Delete shard 0 (covers the beginning of the data)
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj5", 0);

        // Range get should still succeed via EC reconstruction
        let result = coord
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "obj5",
                version_id: None,
                range: ByteRange::Range { start: 0, end: 4 },
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"Hello");
    }

    #[test]
    fn ec_drop_parity_shard_data_still_works() {
        // Delete parity shard (index k=4). Only data shards needed for normal read.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC parity shard drop test";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "obj6",
                data,
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // Delete first parity shard (index 4, since k=4)
        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj6", 4);

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "obj6",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), data);
    }

    #[test]
    fn ec_healthy_read_skips_corrupt_parity_shards() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC healthy read should skip parity shards";
        let put = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "obj7",
                data,
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let segment = {
            let meta_pg_id = coord.object_pg_id("bucket", "obj7");
            let meta_pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let segments = meta_pg
                .get_object_segments("bucket", "obj7", put.version_id)
                .unwrap();
            assert_eq!(segments.len(), 1);
            segments[0].clone()
        };

        corrupt_shard_on_disk(&coord, tmp.path(), "bucket", "obj7", 4);

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "obj7",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
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
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let data = b"EC reconstruction should stop after first needed parity";
        let put = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "obj8",
                data,
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let segment = {
            let meta_pg_id = coord.object_pg_id("bucket", "obj8");
            let meta_pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let segments = meta_pg
                .get_object_segments("bucket", "obj8", put.version_id)
                .unwrap();
            assert_eq!(segments.len(), 1);
            segments[0].clone()
        };

        delete_shard_on_disk(&coord, tmp.path(), "bucket", "obj8", 0);
        corrupt_shard_on_disk(&coord, tmp.path(), "bucket", "obj8", 5);

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "obj8",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
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
                bucket: "no-such-bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn get_nonexistent_object_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let err = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "no-such-key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn etag_consistency() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        let result = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.etag, obj.etag);

        let head = coord
            .head_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.etag, head.etag);
    }

    #[test]
    fn list_objects_delimiter_with_continuation() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "a/1",
                data: b"1",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "a/2",
                data: b"2",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "b/1",
                data: b"3",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "c/1",
                data: b"4",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "root.txt",
                data: b"5",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // First page: max_keys=2 with delimiter
        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: Some("/"),
                continuation_token: None,
                max_keys: 2,
                requester: TEST_REQUESTER,
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
                bucket: "bucket",
                prefix: None,
                delimiter: Some("/"),
                continuation_token: Some(&token),
                max_keys: 2,
                requester: TEST_REQUESTER,
            })
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
            let key = format!("dir{i}/file.txt");
            test_helpers::put_object(
                &coord,
                &PutObjectRequest {
                    bucket: "bucket",
                    key: &key,
                    data: b"data",
                    metadata: &MetadataBlob::new(),
                    tags: None,
                    cond: NO_WRITE,
                    requester: TEST_REQUESTER,
                    acl: NO_PUT_OBJECT_ACL,
                },
            )
            .unwrap();
        }

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: Some("/"),
                continuation_token: None,
                max_keys: 3,
                requester: TEST_REQUESTER,
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
                bucket: "no-bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
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

        coord.create_bucket("bucket").unwrap();
        for i in 0..5 {
            let key = format!("key-{i:02}");
            test_helpers::put_object(
                &coord,
                &PutObjectRequest {
                    bucket: "bucket",
                    key: &key,
                    data: b"data",
                    metadata: &MetadataBlob::new(),
                    tags: None,
                    cond: NO_WRITE,
                    requester: TEST_REQUESTER,
                    acl: NO_PUT_OBJECT_ACL,
                },
            )
            .unwrap();
        }

        // Request fewer than available
        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 3,
                requester: TEST_REQUESTER,
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

        coord.create_bucket("bucket").unwrap();
        for i in 0..5 {
            let key = format!("key-{i:02}");
            test_helpers::put_object(
                &coord,
                &PutObjectRequest {
                    bucket: "bucket",
                    key: &key,
                    data: b"data",
                    metadata: &MetadataBlob::new(),
                    tags: None,
                    cond: NO_WRITE,
                    requester: TEST_REQUESTER,
                    acl: NO_PUT_OBJECT_ACL,
                },
            )
            .unwrap();
        }

        // First page
        let page1 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 2,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(page1.objects.len(), 2);
        assert!(page1.is_truncated);
        let token = page1.next_continuation_token.as_ref().unwrap();

        // Second page using continuation token
        let page2 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: None,
                continuation_token: Some(token),
                max_keys: 2,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(page2.objects.len(), 2);
        assert!(page2.is_truncated);
        let token2 = page2.next_continuation_token.as_ref().unwrap();

        // Third page — should get remainder
        let page3 = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: None,
                continuation_token: Some(token2),
                max_keys: 2,
                requester: TEST_REQUESTER,
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

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "photos/2024/jan.jpg",
                data: b"j",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "photos/2024/feb.jpg",
                data: b"f",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "photos/2025/mar.jpg",
                data: b"m",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "photos/top.jpg",
                data: b"t",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // List with prefix "photos/" and delimiter "/"
        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: Some("photos/"),
                delimiter: Some("/"),
                continuation_token: None,
                max_keys: 1000,
                requester: TEST_REQUESTER,
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

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "only-one",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
                requester: TEST_REQUESTER,
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

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key1",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 0,
                requester: TEST_REQUESTER,
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

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "a/1",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let result = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: Some("/"),
                continuation_token: None,
                max_keys: 0,
                requester: TEST_REQUESTER,
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
                bucket: "no-bucket",
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn delete_objects_batch() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key1",
                data: b"data1",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key2",
                data: b"data2",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
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

        let result = coord
            .delete_objects(&DeleteObjectsRequest {
                bucket: "bucket",
                entries: &entries,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.deleted.len(), 3);
        assert!(result.errors.is_empty());

        // Verify objects are actually gone
        assert!(coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key1",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .is_err());
        assert!(coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key2",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .is_err());
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
            .delete_objects(&DeleteObjectsRequest {
                bucket: "no-bucket",
                entries: &entries,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
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
        coord.create_bucket("bucket").unwrap();

        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &MetadataBlob::new(),
                checksum: None,

                requester: TEST_REQUESTER,
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
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
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

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"Hello, World!",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // bytes=0-4 → "Hello"
        let result = coord
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                range: ByteRange::Range { start: 0, end: 4 },
                cond: NO_READ,
                requester: TEST_REQUESTER,
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

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"Hello, World!",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // bytes=-6 → "World!"  (last 6 bytes)
        let result = coord
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                range: ByteRange::Suffix { length: 6 },
                cond: NO_READ,
                requester: TEST_REQUESTER,
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

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"Hello, World!",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // bytes=7- → "World!"
        let result = coord
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                range: ByteRange::FromStart { start: 7 },
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"World!");
    }

    #[test]
    fn get_object_range_unsatisfiable() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"Hello",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // bytes=100- → unsatisfiable
        let err = coord
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                range: ByteRange::FromStart { start: 100 },
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRange { total_size: 5 }));
    }

    #[test]
    fn get_object_range_clamps_end() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"Hello",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // bytes=0-99999 on 5-byte object → clamp to 0-4
        let result = coord
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                range: ByteRange::Range {
                    start: 0,
                    end: 99999,
                },
                cond: NO_READ,
                requester: TEST_REQUESTER,
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
        coord.create_bucket("bucket").unwrap();

        let cond = WriteCondition::IfNoneMatchStar;
        let result = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "new-key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: &cond,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        assert!(!result.etag.is_empty());
    }

    #[test]
    fn put_if_none_match_star_rejects_overwrite() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"v1",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let cond = WriteCondition::IfNoneMatchStar;
        let err = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"v2",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: &cond,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn put_if_match_updates() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let r1 = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"v1",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        let cond = WriteCondition::IfMatch(SpecificEtag::new(r1.etag.clone()).unwrap());
        let r2 = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"v2",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: &cond,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        assert_ne!(r1.etag, r2.etag);

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"v2");
    }

    #[test]
    fn put_if_match_stale_etag_rejected() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let r1 = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"v1",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        // Overwrite so etag changes
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"v2",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let cond = WriteCondition::IfMatch(SpecificEtag::new(r1.etag).unwrap());
        let err = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"v3",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: &cond,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn put_object_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let err = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: Requester::principal("other-user"),
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn put_object_rejects_acl_on_bucket_owner_enforced_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord
            .put_bucket_ownership_controls(
                "bucket",
                "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
                TEST_REQUESTER,
            )
            .unwrap();

        let err = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: PutObjectAcl::Other("public-read"),
            },
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::AccessControlListNotSupported));
    }

    #[test]
    fn put_bucket_ownership_controls_rejects_public_read_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", true)
            .unwrap();

        let err = coord
            .put_bucket_ownership_controls(
                "bucket",
                "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
                Requester::principal("owner-a"),
            )
            .unwrap_err();
        assert!(matches!(
            err,
            ServerError::InvalidBucketAclWithObjectOwnership
        ));
    }

    #[test]
    fn put_bucket_acl_rejects_block_public_acls() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();
        coord
            .put_bucket_public_access_block(
                "bucket",
                "<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls><BlockPublicPolicy>false</BlockPublicPolicy><RestrictPublicBuckets>false</RestrictPublicBuckets></PublicAccessBlockConfiguration>",
                Requester::principal("owner-a"),
            )
            .unwrap();

        let err = coord
            .put_bucket_acl(
                "bucket",
                BucketAcl::PublicRead,
                Requester::principal("owner-a"),
            )
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn get_object_tags_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: Requester::principal("owner-a"),
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let err = coord
            .get_object_tags("bucket", "key", None, Requester::principal("other-user"))
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn delete_object_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: Requester::principal("owner-a"),
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let err = coord
            .delete_object(&DeleteObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_DELETE,
                requester: Requester::principal("other-user"),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn delete_objects_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();

        let entries = vec![DeleteEntry {
            key: "key",
            version_id: None,
        }];
        let err = coord
            .delete_objects(&DeleteObjectsRequest {
                bucket: "bucket",
                entries: &entries,
                cond: NO_DELETE,
                requester: Requester::principal("other-user"),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn get_object_rejects_private_read_for_non_owner() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"secret",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: Requester::principal("owner-a"),
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let err = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: Requester::principal("other-user"),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn get_object_allows_public_read_for_anonymous() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", true)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"public",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: Requester::principal("owner-a"),
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: Requester::anonymous(),
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"public");
    }

    #[test]
    fn head_bucket_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();

        let err = coord
            .head_bucket_for_requester(&HeadBucketRequest {
                bucket: "bucket",
                requester: Requester::principal("other-user"),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn head_bucket_allows_public_read_for_anonymous() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", true)
            .unwrap();

        let info = coord
            .head_bucket_for_requester(&HeadBucketRequest {
                bucket: "bucket",
                requester: Requester::anonymous(),
            })
            .unwrap();
        assert_eq!(info.name, "bucket");
    }

    #[test]
    fn list_objects_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: Requester::principal("owner-a"),
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let err = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 1000,
                requester: Requester::principal("other-user"),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn delete_bucket_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();

        let err = coord
            .delete_bucket(&DeleteBucketRequest {
                name: "bucket",
                requester: Requester::principal("other-user"),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn create_multipart_upload_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();

        let err = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &MetadataBlob::new(),
                checksum: None,
                requester: Requester::principal("other-user"),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn upload_part_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();

        let upload = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &MetadataBlob::new(),
                checksum: None,
                requester: Requester::principal("owner-a"),
            })
            .unwrap();

        let err = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload.upload_id,
                part_number: 1,
                data: b"data",
                claimed_checksum: None,
                requester: Requester::principal("other-user"),
            },
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn complete_multipart_upload_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();

        let upload = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &MetadataBlob::new(),
                checksum: None,
                requester: Requester::principal("owner-a"),
            })
            .unwrap();

        let err = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload.upload_id,
                parts: &[],
                claimed_checksum: None,
                requester: Requester::principal("other-user"),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn abort_multipart_upload_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();

        let upload = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &MetadataBlob::new(),
                checksum: None,
                requester: Requester::principal("owner-a"),
            })
            .unwrap();

        let err = coord
            .abort_multipart_upload(&AbortMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload.upload_id,
                requester: Requester::principal("other-user"),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn begin_stream_put_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();

        let err = coord
            .begin_stream_put(&BeginStreamPutRequest {
                bucket: "bucket",
                key: "key",
                requester: Requester::principal("other-user"),
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn begin_stream_part_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();

        let upload = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &MetadataBlob::new(),
                checksum: None,
                requester: Requester::principal("owner-a"),
            })
            .unwrap();

        let err = coord
            .begin_stream_part(&BeginStreamPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload.upload_id,
                part_number: 1,
                requester: Requester::principal("other-user"),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn begin_stream_put_rejects_acl_on_bucket_owner_enforced_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();
        coord
            .put_bucket_ownership_controls(
                "bucket",
                "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
                Requester::principal("owner-a"),
            )
            .unwrap();

        let err = coord
            .begin_stream_put(&BeginStreamPutRequest {
                bucket: "bucket",
                key: "key",
                requester: Requester::principal("owner-a"),
                acl: PutObjectAcl::Other("public-read"),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessControlListNotSupported));
    }

    #[test]
    fn get_if_match_returns_object() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        let cond = ReadCondition {
            if_match: Some(put.etag.into()),
            ..Default::default()
        };
        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &cond,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"data");
    }

    #[test]
    fn get_if_match_wrong_etag_returns_412() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let cond = ReadCondition {
            if_match: Some("\"0000000000000000\"".into()),
            ..Default::default()
        };
        let err = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &cond,
                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn get_if_none_match_returns_304() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        let cond = ReadCondition {
            if_none_match: Some(put.etag.into()),
            ..Default::default()
        };
        let err = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &cond,
                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::NotModified { .. }));
    }

    #[test]
    fn head_if_none_match_returns_304() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        let cond = ReadCondition {
            if_none_match: Some(put.etag.into()),
            ..Default::default()
        };
        let err = coord
            .head_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &cond,
                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::NotModified { .. }));
    }

    #[test]
    fn delete_if_match_succeeds() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let put = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        let cond = DeleteCondition::IfMatch(put.etag.into());
        coord
            .delete_object(&DeleteObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &cond,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert!(coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .is_err());
    }

    #[test]
    fn delete_if_match_wrong_etag_returns_412() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let cond = DeleteCondition::IfMatch("\"0000000000000000\"".into());
        let err = coord
            .delete_object(&DeleteObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &cond,
                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn delete_objects_if_match_per_entry() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let p1 = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key1",
                data: b"data1",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key2",
                data: b"data2",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // Use key1's etag for both entries; key2 will fail the condition
        let cond = DeleteCondition::IfMatch(p1.etag.into());
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
        let result = coord
            .delete_objects(&DeleteObjectsRequest {
                bucket: "bucket",
                entries: &entries,
                cond: &cond,
                requester: TEST_REQUESTER,
            })
            .unwrap();
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

        let put = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"Hello, World!",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        let cond = ReadCondition {
            if_match: Some(put.etag.into()),
            ..Default::default()
        };
        let result = coord
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                range: ByteRange::Range { start: 0, end: 4 },
                cond: &cond,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"Hello");
    }

    // ── CopyObject tests ──────────────────────────────────────────────

    #[test]
    fn copy_object_basic() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let headers = [("Content-Type", "text/plain")];
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data: b"hello copy",
                metadata: &MetadataBlob::from_headers(&headers).unwrap(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
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
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();
        assert!(!result.etag.is_empty());

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "dst",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"hello copy");
    }

    #[test]
    fn copy_object_rejects_private_source_read_for_non_owner() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "src-bucket", false)
            .unwrap();
        coord
            .create_bucket_for_owner("owner-b", "dst-bucket", false)
            .unwrap();

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "src-bucket",
                key: "src",
                data: b"private",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: Requester::principal("owner-a"),
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let err = coord
            .copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "src-bucket",
                    key: "src",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "dst-bucket",
                dst_key: "dst",
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Copy,
                tagging: TaggingDirective::Copy,
                requester: Requester::principal("owner-b"),
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn copy_object_rejects_acl_on_bucket_owner_enforced_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        coord
            .put_bucket_ownership_controls(
                "bucket",
                "<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
                TEST_REQUESTER,
            )
            .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

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
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Copy,
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: PutObjectAcl::Other("public-read"),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessControlListNotSupported));
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
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data: b"data",
                metadata: &MetadataBlob::from_headers(&headers).unwrap(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
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
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "dst",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
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
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data: b"data",
                metadata: &MetadataBlob::from_headers(&headers).unwrap(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
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
                directive: MetadataDirective::Replace {
                    metadata: &new_metadata,
                    checksum_algorithm: None,
                },
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "dst",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"data");
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
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::from_headers(&headers).unwrap(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let new_metadata =
            MetadataBlob::from_headers(&[("Content-Type", "application/json")]).unwrap();
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
                directive: MetadataDirective::Replace {
                    metadata: &new_metadata,
                    checksum_algorithm: None,
                },
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"data");
        assert_eq!(obj.metadata.get("content-type"), Some("application/json"));
    }

    #[test]
    fn copy_object_tagging_copy_preserves_source_tags() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let tags_xml =
            "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: Some(tags_xml),
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
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
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "dst",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.tags.as_deref(), Some(tags_xml));
    }

    #[test]
    fn copy_object_tagging_replace_overwrites_source_tags() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let src_tags =
            "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
        let dst_tags =
            "<Tagging><TagSet><Tag><Key>tier</Key><Value>gold</Value></Tag></TagSet></Tagging>";
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: Some(src_tags),
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
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
                tagging: TaggingDirective::Replace(Some(dst_tags)),
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "dst",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.tags.as_deref(), Some(dst_tags));
    }

    #[test]
    fn copy_object_replace_strips_unverified_inline_checksum() {
        // Regression: CopyObject with REPLACE must not persist client-supplied
        // checksum value headers, since there is no body to verify them against.
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data: b"hello",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
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
                directive: MetadataDirective::Replace {
                    metadata: &new_metadata,
                    checksum_algorithm: None,
                },
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "dst",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"hello");
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
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data,
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
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
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "dst",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), data);
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
        assert_eq!(
            ChecksumAlgorithm::parse("CRC32C"),
            Some(ChecksumAlgorithm::Crc32c)
        );
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
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn copy_object_dest_bucket_not_found() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
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
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn copy_object_source_if_match_fails() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let src_cond = ReadCondition {
            if_match: Some("\"0000000000000000\"".into()),
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
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn copy_object_dest_if_none_match_prevents_overwrite() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "dst",
                data: b"existing",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
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
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::PreconditionFailed));
    }

    #[test]
    fn copy_object_dest_if_match_allows_update() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data: b"new data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        let existing = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "dst",
                data: b"old data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
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
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();
        assert!(!result.etag.is_empty());

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "dst",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"new data");
    }

    #[test]
    fn copy_object_cross_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("src-bucket").unwrap();
        coord.create_bucket("dst-bucket").unwrap();

        let headers = [("Content-Type", "text/plain")];
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "src-bucket",
                key: "key",
                data: b"cross bucket data",
                metadata: &MetadataBlob::from_headers(&headers).unwrap(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
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
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "dst-bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"cross bucket data");
        assert_eq!(obj.metadata.get("content-type"), Some("text/plain"));

        // Source should still exist
        let src = coord
            .get_object(&GetObjectRequest {
                bucket: "src-bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(src.body.read_all().unwrap(), b"cross bucket data");
    }

    // ── Bucket versioning tests ──────────────────────────────────────

    #[test]
    fn bucket_versioning_default_disabled() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let state = coord
            .get_bucket_versioning("bucket", TEST_REQUESTER)
            .unwrap();
        assert_eq!(state, BucketVersioningState::Disabled);
    }

    #[test]
    fn bucket_versioning_enable() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_bucket_versioning("bucket", BucketVersioningState::Enabled, TEST_REQUESTER)
            .unwrap();
        assert_eq!(
            coord
                .get_bucket_versioning("bucket", TEST_REQUESTER)
                .unwrap(),
            BucketVersioningState::Enabled
        );
    }

    #[test]
    fn bucket_versioning_enable_then_suspend() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_bucket_versioning("bucket", BucketVersioningState::Enabled, TEST_REQUESTER)
            .unwrap();
        coord
            .put_bucket_versioning("bucket", BucketVersioningState::Suspended, TEST_REQUESTER)
            .unwrap();
        assert_eq!(
            coord
                .get_bucket_versioning("bucket", TEST_REQUESTER)
                .unwrap(),
            BucketVersioningState::Suspended
        );
    }

    #[test]
    fn bucket_versioning_suspend_then_enable() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_bucket_versioning("bucket", BucketVersioningState::Enabled, TEST_REQUESTER)
            .unwrap();
        coord
            .put_bucket_versioning("bucket", BucketVersioningState::Suspended, TEST_REQUESTER)
            .unwrap();
        coord
            .put_bucket_versioning("bucket", BucketVersioningState::Enabled, TEST_REQUESTER)
            .unwrap();
        assert_eq!(
            coord
                .get_bucket_versioning("bucket", TEST_REQUESTER)
                .unwrap(),
            BucketVersioningState::Enabled
        );
    }

    #[test]
    fn bucket_versioning_cannot_disable_from_enabled() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        coord
            .put_bucket_versioning("bucket", BucketVersioningState::Enabled, TEST_REQUESTER)
            .unwrap();
        let err = coord
            .put_bucket_versioning("bucket", BucketVersioningState::Disabled, TEST_REQUESTER)
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidRequest { .. }));
    }

    #[test]
    fn bucket_versioning_nonexistent_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .put_bucket_versioning("no-bucket", BucketVersioningState::Enabled, TEST_REQUESTER)
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn put_bucket_versioning_rejects_non_owner_requester() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord
            .create_bucket_for_owner("owner-a", "bucket", false)
            .unwrap();

        let err = coord
            .put_bucket_versioning(
                "bucket",
                BucketVersioningState::Enabled,
                Requester::principal("other-user"),
            )
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn put_object_returns_version_id_zero() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let result = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        assert_eq!(result.version_id, VersionId::Null);
    }

    #[test]
    fn get_object_returns_version_id() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.version_id, VersionId::Null);
    }

    #[test]
    fn head_object_returns_version_id() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        let head = coord
            .head_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(head.version_id, VersionId::Null);
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
            .put_bucket_versioning("bucket", BucketVersioningState::Enabled, TEST_REQUESTER)
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
                test_helpers::put_object(
                    &coord_a,
                    &PutObjectRequest {
                        bucket: "bucket",
                        key: &key_a,
                        data: b"v1",
                        metadata: &MetadataBlob::new(),
                        tags: None,
                        cond: NO_WRITE,
                        requester: TEST_REQUESTER,
                        acl: NO_PUT_OBJECT_ACL,
                    },
                )
            });
            let t2 = thread::spawn(move || {
                b2.wait();
                test_helpers::put_object(
                    &coord_b,
                    &PutObjectRequest {
                        bucket: "bucket",
                        key: &key_b,
                        data: b"v2",
                        metadata: &MetadataBlob::new(),
                        tags: None,
                        cond: NO_WRITE,
                        requester: TEST_REQUESTER,
                        acl: NO_PUT_OBJECT_ACL,
                    },
                )
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

        let object_size = (2 * 1024 * 1024) + 137;
        test_helpers::put_object(
            &admin,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: &vec![b'A'; object_size],
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let mut current = b'A';
        for _ in 0..20 {
            let next = if current == b'A' { b'B' } else { b'A' };
            let new_payload = vec![next; object_size];

            let reader = make_coord();
            let writer = make_coord();
            let barrier = Arc::new(Barrier::new(3));
            let b1 = Arc::clone(&barrier);
            let b2 = Arc::clone(&barrier);

            let t_write = thread::spawn(move || {
                b1.wait();
                test_helpers::put_object(
                    &writer,
                    &PutObjectRequest {
                        bucket: "bucket",
                        key: "key",
                        data: &new_payload,
                        metadata: &MetadataBlob::new(),
                        tags: None,
                        cond: NO_WRITE,
                        requester: TEST_REQUESTER,
                        acl: NO_PUT_OBJECT_ACL,
                    },
                )
            });
            let t_read = thread::spawn(move || {
                b2.wait();
                reader.get_object(&GetObjectRequest {
                    bucket: "bucket",
                    key: "key",
                    version_id: None,
                    cond: NO_READ,
                    requester: TEST_REQUESTER,
                })
            });

            barrier.wait();

            let write_res = t_write.join().unwrap();
            assert!(
                write_res.is_ok(),
                "concurrent overwrite failed: {write_res:?}"
            );

            let read_res = t_read.join().unwrap();
            let obj = read_res.expect("get_object must not fail during overwrite");
            let data = obj.body.read_all().unwrap();
            assert_eq!(data.len(), object_size);
            let uniform = data.iter().all(|&b| b == current) || data.iter().all(|&b| b == next);
            assert!(
                uniform,
                "read must return a complete old or new object image"
            );

            current = next;
        }
    }

    #[test]
    fn copy_object_is_consistent_during_concurrent_overwrite() {
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
        admin.create_bucket("src-bucket").unwrap();
        admin.create_bucket("dst-bucket").unwrap();

        let object_size = (2 * 1024 * 1024) + 137;
        test_helpers::put_object(
            &admin,
            &PutObjectRequest {
                bucket: "src-bucket",
                key: "src",
                data: &vec![b'A'; object_size],
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let mut current = b'A';
        for i in 0..12 {
            let next = if current == b'A' { b'B' } else { b'A' };
            let new_payload = vec![next; object_size];
            let dst_key = format!("dst-{i}");
            let dst_key_for_copy = dst_key.clone();

            let writer = make_coord();
            let copier = make_coord();
            let barrier = Arc::new(Barrier::new(3));
            let b1 = Arc::clone(&barrier);
            let b2 = Arc::clone(&barrier);

            let t_write = thread::spawn(move || {
                b1.wait();
                test_helpers::put_object(
                    &writer,
                    &PutObjectRequest {
                        bucket: "src-bucket",
                        key: "src",
                        data: &new_payload,
                        metadata: &MetadataBlob::new(),
                        tags: None,
                        cond: NO_WRITE,
                        requester: TEST_REQUESTER,
                        acl: NO_PUT_OBJECT_ACL,
                    },
                )
            });
            let t_copy = thread::spawn(move || {
                b2.wait();
                copier.copy_object(&CopyObjectRequest {
                    source: CopySource {
                        bucket: "src-bucket",
                        key: "src",
                        version_id: None,
                        condition: NO_READ,
                    },
                    dst_bucket: "dst-bucket",
                    dst_key: &dst_key_for_copy,
                    dst_condition: NO_WRITE,
                    directive: MetadataDirective::Copy,
                    tagging: TaggingDirective::Copy,
                    requester: TEST_REQUESTER,
                    acl: NO_PUT_OBJECT_ACL,
                })
            });

            barrier.wait();

            let write_res = t_write.join().unwrap();
            assert!(
                write_res.is_ok(),
                "concurrent overwrite failed: {write_res:?}"
            );

            let copy_res = t_copy.join().unwrap();
            assert!(
                copy_res.is_ok(),
                "copy_object must not fail during overwrite: {copy_res:?}"
            );

            let copied_obj = admin
                .get_object(&GetObjectRequest {
                    bucket: "dst-bucket",
                    key: &dst_key,
                    version_id: None,
                    cond: NO_READ,
                    requester: TEST_REQUESTER,
                })
                .unwrap();
            let data = copied_obj.body.read_all().unwrap();
            assert_eq!(data.len(), object_size);
            let uniform = data.iter().all(|&b| b == current) || data.iter().all(|&b| b == next);
            assert!(
                uniform,
                "copied object must contain a complete old or new source image"
            );

            current = next;
        }
    }

    #[test]
    fn upload_part_copy_is_consistent_during_concurrent_overwrite() {
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

        let object_size = (2 * 1024 * 1024) + 137;
        test_helpers::put_object(
            &admin,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data: &vec![b'A'; object_size],
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let mut current = b'A';
        for i in 0..8 {
            let next = if current == b'A' { b'B' } else { b'A' };
            let new_payload = vec![next; object_size];
            let dst_key = format!("dst-{i}");
            let upload = admin
                .create_multipart_upload(&CreateMultipartUploadRequest {
                    bucket: "bucket",
                    key: &dst_key,
                    metadata: &MetadataBlob::new(),
                    checksum: None,
                    requester: TEST_REQUESTER,
                })
                .unwrap();
            let dst_key_for_copy = dst_key.clone();
            let upload_id_for_copy = upload.upload_id.clone();

            let writer = make_coord();
            let copier = make_coord();
            let barrier = Arc::new(Barrier::new(3));
            let b1 = Arc::clone(&barrier);
            let b2 = Arc::clone(&barrier);

            let t_write = thread::spawn(move || {
                b1.wait();
                test_helpers::put_object(
                    &writer,
                    &PutObjectRequest {
                        bucket: "bucket",
                        key: "src",
                        data: &new_payload,
                        metadata: &MetadataBlob::new(),
                        tags: None,
                        cond: NO_WRITE,
                        requester: TEST_REQUESTER,
                        acl: NO_PUT_OBJECT_ACL,
                    },
                )
            });
            let t_copy = thread::spawn(move || {
                b2.wait();
                copier.upload_part_copy(&UploadPartCopyRequest {
                    source: CopySource {
                        bucket: "bucket",
                        key: "src",
                        version_id: None,
                        condition: NO_READ,
                    },
                    dst_bucket: "bucket",
                    dst_key: &dst_key_for_copy,
                    upload_id: &upload_id_for_copy,
                    part_number: 1,
                    copy_source_range: None,
                    requester: TEST_REQUESTER,
                })
            });

            barrier.wait();

            let write_res = t_write.join().unwrap();
            assert!(
                write_res.is_ok(),
                "concurrent overwrite failed: {write_res:?}"
            );

            let copy_res = t_copy.join().unwrap();
            let copy_res = copy_res.expect("upload_part_copy must not fail during overwrite");

            admin
                .complete_multipart_upload(&CompleteMultipartUploadRequest {
                    bucket: "bucket",
                    key: &dst_key,
                    upload_id: &upload.upload_id,
                    parts: &[CompletePart {
                        part_number: 1,
                        etag: copy_res.etag,
                        checksum: None,
                    }],
                    claimed_checksum: None,
                    requester: TEST_REQUESTER,
                })
                .unwrap();

            let copied_obj = admin
                .get_object(&GetObjectRequest {
                    bucket: "bucket",
                    key: &dst_key,
                    version_id: None,
                    cond: NO_READ,
                    requester: TEST_REQUESTER,
                })
                .unwrap();
            let data = copied_obj.body.read_all().unwrap();
            assert_eq!(data.len(), object_size);
            let uniform = data.iter().all(|&b| b == current) || data.iter().all(|&b| b == next);
            assert!(
                uniform,
                "uploaded copied part must contain a complete old or new source image"
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
        test_helpers::put_object(
            &admin,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: &vec![b'A'; object_size],
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
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
                test_helpers::put_object(
                    &writer,
                    &PutObjectRequest {
                        bucket: "bucket",
                        key: "key",
                        data: &payload,
                        metadata: &MetadataBlob::new(),
                        tags: None,
                        cond: NO_WRITE,
                        requester: TEST_REQUESTER,
                        acl: NO_PUT_OBJECT_ACL,
                    },
                )
            });
            let t_delete = thread::spawn(move || {
                b2.wait();
                deleter.delete_object(&DeleteObjectRequest {
                    bucket: "bucket",
                    key: "key",
                    version_id: None,
                    cond: NO_DELETE,
                    requester: TEST_REQUESTER,
                })
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

            let check = make_coord().get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            });
            match check {
                Ok(obj) => {
                    let data = obj.body.read_all().unwrap();
                    assert_eq!(data.len(), object_size);
                    assert!(
                        data.iter().all(|&b| b == expected_byte),
                        "if object exists after put/delete race, it must be a full new image"
                    );
                }
                Err(ServerError::ObjectNotFound { .. }) => {}
                Err(other) => panic!("unexpected read result after put/delete race: {other:?}"),
            }
        }
    }

    #[test]
    fn multipart_get_object_survives_metadata_delete_mid_read() {
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
        admin.create_bucket("race-bucket").unwrap();
        let key = find_fresh_key_with_meta_pg_gt_shard_pg(&admin, "race-bucket", "race-key-get");
        let (_, expected) =
            create_completed_multipart_with_streamed_tail(&admin, "race-bucket", &key);
        assert_object_maps_meta_pg_gt_shard_pg(&admin, "race-bucket", &key);

        let sync = install_multipart_metadata_race_hooks("race-bucket", &key);
        let reader = make_coord();
        let deleter = make_coord();
        let read_key = key.clone();
        let delete_key = key.clone();

        let t_read = thread::spawn(move || {
            reader
                .get_object(&GetObjectRequest {
                    bucket: "race-bucket",
                    key: &read_key,
                    version_id: None,
                    cond: NO_READ,
                    requester: TEST_REQUESTER,
                })
                .and_then(|result| result.body.read_all())
        });
        sync.snapshot_reached.wait();

        let t_delete = thread::spawn(move || {
            deleter.delete_object(&DeleteObjectRequest {
                bucket: "race-bucket",
                key: &delete_key,
                version_id: None,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
        });
        sync.delete_reached.wait();

        sync.snapshot_resume.wait();
        let read_res = t_read.join().unwrap();
        sync.delete_resume.wait();
        let delete_res = t_delete.join().unwrap();
        assert!(delete_res.is_ok(), "delete failed: {delete_res:?}");
        let read_res = read_res
            .expect("multipart get_object should succeed once source part metadata is snapshotted");
        assert_eq!(read_res, expected);
    }

    #[test]
    fn multipart_get_object_part_survives_metadata_delete_mid_read() {
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
        admin.create_bucket("race-bucket").unwrap();
        let key = find_fresh_key_with_meta_pg_gt_shard_pg(&admin, "race-bucket", "race-key-part");
        let (_, expected) =
            create_completed_multipart_with_streamed_tail(&admin, "race-bucket", &key);
        assert_object_maps_meta_pg_gt_shard_pg(&admin, "race-bucket", &key);
        let expected_tail = b"streamed-tail-data".to_vec();
        assert_eq!(
            &expected[expected.len() - expected_tail.len()..],
            expected_tail.as_slice()
        );

        let sync = install_multipart_metadata_race_hooks("race-bucket", &key);
        let reader = make_coord();
        let deleter = make_coord();
        let read_key = key.clone();
        let delete_key = key.clone();

        let t_read = thread::spawn(move || {
            reader
                .get_object_part(&GetObjectPartRequest {
                    bucket: "race-bucket",
                    key: &read_key,
                    version_id: None,
                    part_number: 2,
                    cond: NO_READ,
                    requester: TEST_REQUESTER,
                })
                .and_then(|res| {
                    let part_start = res.part_start;
                    let body = res.body.read_all()?;
                    Ok((part_start, body))
                })
        });
        sync.snapshot_reached.wait();

        let t_delete = thread::spawn(move || {
            deleter.delete_object(&DeleteObjectRequest {
                bucket: "race-bucket",
                key: &delete_key,
                version_id: None,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
        });
        sync.delete_reached.wait();

        sync.snapshot_resume.wait();
        let read_res = t_read.join().unwrap();
        sync.delete_resume.wait();
        let delete_res = t_delete.join().unwrap();
        assert!(delete_res.is_ok(), "delete failed: {delete_res:?}");
        let (part_start, body) = read_res.expect(
            "multipart get_object_part should succeed once part segment metadata is snapshotted",
        );
        assert_eq!(body, expected_tail);
        assert_eq!(part_start, MIN_PART as u64);
    }

    #[test]
    fn object_segments_get_object_survives_delete_mid_read() {
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
        admin.create_bucket("race-bucket").unwrap();
        let key =
            find_fresh_key_with_meta_pg_gt_shard_pg(&admin, "race-bucket", "race-key-segments");

        let session_id = begin_stream_put_test(&admin, "race-bucket", &key).unwrap();
        admin
            .append_stream_segment("race-bucket", &key, &session_id, 0, b"segment-zero-")
            .unwrap();
        admin
            .append_stream_segment("race-bucket", &key, &session_id, 1, b"segment-one")
            .unwrap();
        let expected = b"segment-zero-segment-one".to_vec();
        admin
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "race-bucket",
                key: &key,
                session_id: &session_id,
                crc64: checksum::crc64::checksum(&expected),
                total_size: expected.len() as u64,
                metadata_blob: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();
        assert_object_maps_meta_pg_gt_shard_pg(&admin, "race-bucket", &key);

        let sync = install_object_segments_delete_race_hooks("race-bucket", &key);
        let reader = make_coord();
        let deleter = make_coord();
        let read_key = key.clone();
        let delete_key = key.clone();

        let t_read = thread::spawn(move || {
            reader
                .get_object(&GetObjectRequest {
                    bucket: "race-bucket",
                    key: &read_key,
                    version_id: None,
                    cond: NO_READ,
                    requester: TEST_REQUESTER,
                })
                .and_then(|result| result.body.read_all())
        });
        sync.first_segment_reached.wait();

        let t_delete = thread::spawn(move || {
            deleter.delete_object(&DeleteObjectRequest {
                bucket: "race-bucket",
                key: &delete_key,
                version_id: None,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
        });
        sync.delete_reached.wait();

        sync.first_segment_resume.wait();
        let read_res = t_read.join().unwrap();
        sync.delete_resume.wait();
        let delete_res = t_delete.join().unwrap();
        assert!(delete_res.is_ok(), "delete failed: {delete_res:?}");
        let body = read_res.expect("segmented get_object should survive delete mid-read");
        assert_eq!(body, expected);
    }

    #[test]
    fn upload_part_copy_survives_source_metadata_delete_mid_read() {
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
        admin.create_bucket("race-bucket").unwrap();
        let source_key =
            find_fresh_key_with_meta_pg_gt_shard_pg(&admin, "race-bucket", "race-key-copy");
        create_completed_multipart_with_streamed_tail(&admin, "race-bucket", &source_key);
        assert_object_maps_meta_pg_gt_shard_pg(&admin, "race-bucket", &source_key);
        let upload = admin
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "race-bucket",
                key: "dst",
                metadata: &MetadataBlob::new(),
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let sync = install_multipart_metadata_race_hooks("race-bucket", &source_key);
        let copier = make_coord();
        let deleter = make_coord();
        let copy_key = source_key.clone();
        let delete_key = source_key.clone();

        let t_copy = thread::spawn(move || {
            copier.upload_part_copy(&UploadPartCopyRequest {
                source: CopySource {
                    bucket: "race-bucket",
                    key: &copy_key,
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "race-bucket",
                dst_key: "dst",
                upload_id: &upload.upload_id,
                part_number: 1,
                copy_source_range: None,
                requester: TEST_REQUESTER,
            })
        });
        sync.snapshot_reached.wait();

        let t_delete = thread::spawn(move || {
            deleter.delete_object(&DeleteObjectRequest {
                bucket: "race-bucket",
                key: &delete_key,
                version_id: None,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
        });
        sync.delete_reached.wait();

        sync.snapshot_resume.wait();
        let copy_res = t_copy.join().unwrap();
        sync.delete_resume.wait();
        let delete_res = t_delete.join().unwrap();
        assert!(delete_res.is_ok(), "delete failed: {delete_res:?}");
        assert!(
            copy_res.is_ok(),
            "upload_part_copy should succeed once source multipart metadata is snapshotted: {copy_res:?}"
        );
    }

    #[test]
    fn copy_object_survives_source_metadata_delete_mid_read() {
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
        admin.create_bucket("src-bucket").unwrap();
        admin.create_bucket("dst-bucket").unwrap();
        let (_, expected) =
            create_completed_multipart_with_streamed_tail(&admin, "src-bucket", "race-key-copy");

        let sync = install_multipart_metadata_race_hooks("src-bucket", "race-key-copy");
        let copier = make_coord();
        let deleter = make_coord();

        let t_copy = thread::spawn(move || {
            copier.copy_object(&CopyObjectRequest {
                source: CopySource {
                    bucket: "src-bucket",
                    key: "race-key-copy",
                    version_id: None,
                    condition: NO_READ,
                },
                dst_bucket: "dst-bucket",
                dst_key: "copied",
                dst_condition: NO_WRITE,
                directive: MetadataDirective::Copy,
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
        });
        sync.snapshot_reached.wait();

        let t_delete = thread::spawn(move || {
            deleter.delete_object(&DeleteObjectRequest {
                bucket: "src-bucket",
                key: "race-key-copy",
                version_id: None,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
        });
        sync.delete_reached.wait();

        sync.snapshot_resume.wait();
        let copy_res = t_copy.join().unwrap();
        sync.delete_resume.wait();
        let delete_res = t_delete.join().unwrap();
        assert!(delete_res.is_ok(), "delete failed: {delete_res:?}");
        assert!(
            copy_res.is_ok(),
            "copy_object should succeed once source multipart metadata is snapshotted: {copy_res:?}"
        );

        let dst = admin
            .get_object(&GetObjectRequest {
                bucket: "dst-bucket",
                key: "copied",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(dst.body.read_all().unwrap(), expected);
    }

    #[test]
    fn delete_object_returns_result() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();
        let result = coord
            .delete_object(&DeleteObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.version_id, VersionId::Null);
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();
        let r2 = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_ne!(r1.upload_id, r2.upload_id);
    }

    #[test]
    fn create_multipart_upload_requires_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let metadata = MetadataBlob::new();
        let err = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "no-such-bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn list_multipart_uploads_empty() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let result = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket: "bucket",
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1000,
                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "alpha",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();
        let r2 = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "beta",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let result = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket: "bucket",
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1000,
                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();
        let r2 = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let result = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket: "bucket",
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1000,
                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "a",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "b",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "c",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Page 1: max_uploads=2.
        let page1 = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket: "bucket",
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 2,
                requester: TEST_REQUESTER,
            })
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
                requester: TEST_REQUESTER,
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "photos/a.jpg",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "photos/b.jpg",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();
        coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "docs/readme.md",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let result = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket: "bucket",
                prefix: Some("photos/"),
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1000,
                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let result = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket: "bucket",
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 0,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert!(result.uploads.is_empty());
        assert!(!result.is_truncated);
    }

    #[test]
    fn list_multipart_uploads_requires_bucket() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());

        let err = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket: "no-such-bucket",
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 1000,
                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "photo.png",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Verify we can retrieve the upload and its metadata blob is stored.
        let meta_pg_id = coord.object_pg_id("bucket", "photo.png");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let record = pg.get_multipart_upload(&result.upload_id).unwrap();
        assert_eq!(record.bucket, "bucket");
        assert_eq!(record.key, "photo.png");

        // Deserialize and verify the metadata blob.
        let (blob, _) = MetadataBlob::deserialize(record.metadata_blob.as_slice()).unwrap();
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Bucket has no objects but has an in-progress MPU — should fail.
        let err = delete_bucket_test(&coord, "bucket").unwrap_err();
        assert!(matches!(err, ServerError::BucketNotEmpty));
    }

    #[test]
    fn delete_bucket_drains_unqueued_payload_reclaim() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let generation_id = GenerationId::new(1).unwrap();
        let meta_pg_id = coord.object_pg_id("bucket", "ghost");
        {
            let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            pg.put_simple_payload_reclaim(&SimplePayloadReclaimRecord {
                bucket: "bucket".into(),
                key: "ghost".into(),
                generation_id,
                ec: EcShape { k: 4, m: 2 },
                created_at: 1,
            })
            .unwrap();
        }

        delete_bucket_test(&coord, "bucket").unwrap();
        assert!(matches!(
            coord.head_bucket("bucket"),
            Err(ServerError::BucketNotFound { .. })
        ));

        wait_until_bucket_gone(&coord, "bucket");

        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        assert!(pg
            .get_simple_payload_reclaim("bucket", "ghost", generation_id)
            .unwrap()
            .is_none());
    }

    #[test]
    fn delete_bucket_returns_before_payload_lease_and_reclaim_complete() {
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

        let metadata = MetadataBlob::new();
        test_helpers::put_object(
            &admin,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"hello world",
                metadata: &metadata,
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let held_read = admin
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();

        let generation_id = {
            let meta_pg_id = admin.object_pg_id("bucket", "key");
            let pg = admin.storage_node.get_pg(meta_pg_id).unwrap();
            match pg.get_object_meta("bucket", "key").unwrap() {
                StoredObject::Live(record) => record.generation_id,
                other @ StoredObject::DeleteMarker(_) => {
                    panic!("expected live object, got {other:?}")
                }
            }
        };

        admin
            .delete_object(&DeleteObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
            .unwrap();

        let meta_pg_id = admin.object_pg_id("bucket", "key");
        {
            let pg = admin.storage_node.get_pg(meta_pg_id).unwrap();
            assert!(pg
                .get_object_segments_reclaim("bucket", "key", generation_id)
                .unwrap()
                .is_some());
        }

        delete_bucket_test(&deleter, "bucket").unwrap();
        assert!(matches!(
            deleter.head_bucket("bucket"),
            Err(ServerError::BucketNotFound { .. })
        ));
        assert!(matches!(
            deleter.create_bucket("bucket"),
            Err(ServerError::BucketAlreadyExists)
        ));

        {
            let pg = admin.storage_node.get_pg(meta_pg_id).unwrap();
            assert!(pg
                .get_object_segments_reclaim("bucket", "key", generation_id)
                .unwrap()
                .is_some());
        }

        drop(held_read);
        wait_until_bucket_gone(&deleter, "bucket");
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
                .create_multipart_upload(&CreateMultipartUploadRequest {
                    bucket: "bucket",
                    key: "key",
                    metadata: &metadata,
                    checksum: None,

                    requester: TEST_REQUESTER,
                })
                .unwrap();
            upload_ids.push(r.upload_id);
        }

        // Page 1: max_uploads=2 — should get first 2 by initiation time.
        let page1 = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket: "bucket",
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 2,
                requester: TEST_REQUESTER,
            })
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
                requester: TEST_REQUESTER,
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let result = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"hello world",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
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
        assert_eq!(part.part_okh, [0u8; 16]);
        assert_eq!(part.part_vid, GenerationId::MIN);

        let segments = pg
            .get_all_multipart_part_segments_for_upload(&create.upload_id)
            .unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].part_number, 1);
        assert_eq!(segments[0].segment_index, 0);
        assert_eq!(segments[0].size, 11);
    }

    #[test]
    fn upload_part_reupload_increments_generation() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // First upload → generation 0.
        test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"first",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap();

        // Re-upload same part number → generation 1.
        let result = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"second",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let err = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 0,
                data: b"data",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let err = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 10_001,
                data: b"data",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::InvalidArgument { .. }));
    }

    #[test]
    fn upload_part_nonexistent_upload() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let err = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: "bogus-upload-id",
                part_number: 1,
                data: b"data",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"part-one",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap();
        test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 2,
                data: b"part-two",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap();
        test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 3,
                data: b"part-three",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Upload same part 4 times — generation should increment each time.
        for i in 0..4u32 {
            let data = format!("version-{i}");
            test_helpers::upload_part(
                &coord,
                &UploadPartRequest {
                    bucket: "bucket",
                    key: "key",
                    upload_id: &create.upload_id,
                    part_number: 1,
                    data: data.as_bytes(),
                    claimed_checksum: None,
                    requester: TEST_REQUESTER,
                },
            )
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Part 1 (min valid).
        test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"a",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap();
        // Part 10000 (max valid).
        test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 10_000,
                data: b"z",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Try uploading with wrong key — should be rejected even if upload_id is valid.
        let err = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "wrong-key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"data",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap_err();
        assert!(matches!(err, ServerError::NoSuchUpload { .. }));

        // Try uploading with wrong bucket.
        coord.create_bucket("other-bucket").unwrap();
        let err = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "other-bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"data",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Simulate concurrent same-part uploads sequentially.
        // Each successive upload should overwrite, with generation incrementing.
        let etag1 = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"writer-A",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap()
        .etag;
        let etag2 = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"writer-B",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap()
        .etag;
        let etag3 = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"writer-C",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket,
                key,
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();
        let mut complete_parts = Vec::new();
        for &(part_number, data) in part_data {
            let result = test_helpers::upload_part(
                coord,
                &UploadPartRequest {
                    bucket,
                    key,
                    upload_id: &create.upload_id,
                    part_number,
                    data,
                    claimed_checksum: None,
                    requester: TEST_REQUESTER,
                },
            )
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
    fn delete_multipart_object_eventually_reclaims_part_shards() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let (upload_id, parts) = create_upload_with_parts(&coord, "bucket", "key", &[(1, b"part")]);
        let result = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let parts_to_reclaim = {
            let meta_pg_id = coord.object_pg_id("bucket", "key");
            let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            pg.get_object_parts("bucket", "key", result.version_id)
                .unwrap()
        };

        coord
            .delete_object(&DeleteObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
            .unwrap();

        for part in parts_to_reclaim {
            wait_for_shard_set_deletion(
                &coord,
                part.shard_pg_id,
                &part.part_okh,
                part.part_vid,
                EcShape {
                    k: part.ec_k,
                    m: part.ec_m,
                },
            );
        }
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &reversed,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let err = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                parts: &[],
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::EntityTooSmall { .. }));

        // Upload remains usable — re-upload part 1 with large data and retry.
        let big_data = vec![0u8; 5 * 1024 * 1024];
        let new_part1 = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                part_number: 1,
                data: &big_data,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &retry_parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &duped,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id1,
                parts: &parts1,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id2,
                parts: &parts2,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .get_object_parts("bucket", "key", VersionId::Null)
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // list_objects_v2 should return the composite ETag with -N suffix.
        let list = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 100,
                requester: TEST_REQUESTER,
            })
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
            .put_bucket_versioning("bucket", BucketVersioningState::Enabled, TEST_REQUESTER)
            .unwrap();

        let (upload_id, parts) =
            create_upload_with_parts(&coord, "bucket", "key", &[(1, b"data1")]);
        let result = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let versions = coord
            .list_object_versions(&ListObjectVersionsRequest {
                bucket: "bucket",
                prefix: None,
                key_marker: None,
                version_id_marker: None,
                max_keys: 100,
                requester: TEST_REQUESTER,
            })
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
            .abort_multipart_upload(&AbortMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Upload should no longer exist.
        let err = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                part_number: 1,
                data: b"nope",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );

        // ListMultipartUploads should be empty.
        let uploads = coord
            .list_multipart_uploads(&ListMultipartUploadsRequest {
                bucket: "bucket",
                prefix: None,
                key_marker: None,
                upload_id_marker: None,
                max_uploads: 100,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert!(uploads.uploads.is_empty());
    }

    #[test]
    fn abort_multipart_upload_nonexistent() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        let err = coord
            .abort_multipart_upload(&AbortMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: "no-such-upload",

                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // First abort succeeds.
        coord
            .abort_multipart_upload(&AbortMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Second abort: upload is already deleted, returns UploadNotFound.
        let err = coord
            .abort_multipart_upload(&AbortMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,

                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let err = coord
            .abort_multipart_upload(&AbortMultipartUploadRequest {
                bucket: "other",
                key: "key",
                upload_id: &create.upload_id,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Abort the same upload_id should fail (already deleted by complete).
        let err = coord
            .abort_multipart_upload(&AbortMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,

                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::NoSuchUpload { .. }),
            "expected NoSuchUpload, got {err:?}"
        );

        // Object should still exist (visible in listing).
        let list = coord
            .list_objects_v2(&ListObjectsV2Request {
                bucket: "bucket",
                prefix: None,
                delimiter: None,
                continuation_token: None,
                max_keys: 100,
                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();
        test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"data",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap();

        coord
            .abort_multipart_upload(&AbortMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let err = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 2,
                data: b"more",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
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
            .list_parts(&ListPartsRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                part_number_marker: None,
                max_parts: 100,
                requester: TEST_REQUESTER,
            })
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
            .list_parts(&ListPartsRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                part_number_marker: None,
                max_parts: 2,
                requester: TEST_REQUESTER,
            })
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
                requester: TEST_REQUESTER,
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
            .list_parts(&ListPartsRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                part_number_marker: None,
                max_parts: 100,
                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let err = coord
            .list_parts(&ListPartsRequest {
                bucket: "other",
                key: "key",
                upload_id: &create.upload_id,
                part_number_marker: None,
                max_parts: 100,
                requester: TEST_REQUESTER,
            })
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
            .list_parts(&ListPartsRequest {
                bucket: "bucket",
                key: "key",
                upload_id: "no-such-upload",
                part_number_marker: None,
                max_parts: 100,
                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Upload part 1, then overwrite it.
        test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"original",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap();
        let reupload = test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"replaced",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap();

        let result = coord
            .list_parts(&ListPartsRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number_marker: None,
                max_parts: 100,
                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();
        test_helpers::upload_part(
            &coord,
            &UploadPartRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number: 1,
                data: b"data",
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap();

        // Manually transition to Aborting (simulates the window during abort).
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        pg.set_upload_state(&create.upload_id, UploadState::Aborting)
            .unwrap();
        drop(pg);

        let err = coord
            .list_parts(&ListPartsRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,
                part_number_marker: None,
                max_parts: 100,
                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Manually transition to Completing (simulates concurrent complete).
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        pg.set_upload_state(&create.upload_id, UploadState::Completing)
            .unwrap();
        drop(pg);

        let err = coord
            .abort_multipart_upload(&AbortMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &create.upload_id,

                requester: TEST_REQUESTER,
            })
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket,
                key,
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();
        let mut complete_parts = Vec::new();
        for (part_number, data) in part_data {
            let result = test_helpers::upload_part(
                coord,
                &UploadPartRequest {
                    bucket,
                    key,
                    upload_id: &create.upload_id,
                    part_number: *part_number,
                    data,
                    claimed_checksum: None,

                    requester: TEST_REQUESTER,
                },
            )
            .unwrap();
            complete_parts.push(CompletePart {
                part_number: *part_number,
                etag: result.etag,
                checksum: None,
            });
        }
        coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket,
                key,
                upload_id: &create.upload_id,
                parts: &complete_parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap()
    }

    fn create_completed_multipart_with_streamed_tail(
        coord: &Coordinator,
        bucket: &str,
        key: &str,
    ) -> (CompleteMultipartUploadResult, Vec<u8>) {
        let metadata = MetadataBlob::new();
        let create = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket,
                key,
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let part1 = make_part(0xAA, MIN_PART);
        let part1_result = test_helpers::upload_part(
            coord,
            &UploadPartRequest {
                bucket,
                key,
                upload_id: &create.upload_id,
                part_number: 1,
                data: &part1,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            },
        )
        .unwrap();

        let session = begin_stream_part_test(coord, bucket, key, &create.upload_id, 2).unwrap();
        let part2 = b"streamed-tail-data".to_vec();
        coord
            .append_stream_segment(bucket, key, &session.session_id, 0, &part2)
            .unwrap();
        let part2_crc = checksum::crc64::checksum(&part2);
        let part2_result = coord
            .finalize_stream_part(FinalizeStreamPartRequest {
                bucket,
                key,
                session_id: &session.session_id,
                upload_id: &create.upload_id,
                part_number: 2,
                crc64: part2_crc,
                total_size: part2.len() as u64,
                claimed_checksum: None,
                computed_checksum: None,
            })
            .unwrap();

        let complete = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket,
                key,
                upload_id: &create.upload_id,
                parts: &[
                    CompletePart {
                        part_number: 1,
                        etag: part1_result.etag,
                        checksum: None,
                    },
                    CompletePart {
                        part_number: 2,
                        etag: part2_result.etag,
                        checksum: None,
                    },
                ],
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let expected = [part1, part2].concat();
        (complete, expected)
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
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), expected);
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
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), b"only-part");
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
            .head_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
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
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                range: ByteRange::Range { start: 10, end: 19 },
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(range.body.read_all().unwrap(), vec![0xAA; 10]);
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
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                range: ByteRange::Range {
                    start: boundary - 4,
                    end: boundary + 3,
                },
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        let mut expected = vec![0xAA; 4];
        expected.extend_from_slice(&[0xBB; 4]);
        assert_eq!(range.body.read_all().unwrap(), expected);
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
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                range: ByteRange::Suffix { length: 50 },
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(range.body.read_all().unwrap(), vec![0xBB; 50]);
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
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();

        // Destination should have the concatenated data as inline object.
        let dst = coord
            .get_object(&GetObjectRequest {
                bucket: "dst-bucket",
                key: "dst-key",
                version_id: None,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(dst.body.read_all().unwrap(), expected);
    }

    #[test]
    fn get_multipart_object_zero_byte_single_part() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, vec![])]);

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert!(obj.body.read_all().unwrap().is_empty());
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
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.body.read_all().unwrap(), expected);
        assert_eq!(obj.size, MIN_PART as u64);
    }

    #[test]
    fn get_object_part_zero_byte_single_part() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        create_completed_multipart_vec(&coord, "bucket", "key", &[(1, vec![])]);

        let result = coord
            .get_object_part(&GetObjectPartRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                part_number: 1,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert!(result.body.read_all().unwrap().is_empty());
        assert_eq!(result.part_size, 0);
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
            .get_object_part(&GetObjectPartRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                part_number: 1,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), part1);
        assert_eq!(result.part_size, MIN_PART as u64);
        assert_eq!(result.part_start, 0);
        assert_eq!(result.part_end, MIN_PART as u64 - 1);

        // Part 2 (zero-byte) should return empty data
        let result = coord
            .get_object_part(&GetObjectPartRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                part_number: 2,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert!(result.body.read_all().unwrap().is_empty());
        assert_eq!(result.part_size, 0);
        assert_eq!(result.parts_count, 2);
        assert_eq!(result.part_start, MIN_PART as u64);
        assert_eq!(result.part_end, MIN_PART as u64);
    }

    #[test]
    fn head_object_part_non_multipart() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"hello world",
                metadata: &MetadataBlob::from_headers(&[("x-amz-meta-foo", "bar")]).unwrap(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // partNumber=1 on non-multipart object returns the full object.
        let result = coord
            .head_object_part(&GetObjectPartRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                part_number: 1,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.part_size, 11);
        assert_eq!(result.total_size, 11);
        assert_eq!(result.parts_count, 1);
        assert_eq!(result.metadata.get("x-amz-meta-foo"), Some("bar"));

        // partNumber=2 on non-multipart object returns InvalidPart.
        let err = coord
            .head_object_part(&GetObjectPartRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                part_number: 2,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::InvalidPart { part_number: 2 }));
    }

    #[test]
    fn head_object_part_non_multipart_zero_byte() {
        let tmp = test_util::tempdir();
        let coord = setup_coordinator(tmp.path());
        coord.create_bucket("bucket").unwrap();

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let result = coord
            .head_object_part(&GetObjectPartRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                part_number: 1,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
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
            .head_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
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
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();

        let dst = coord
            .get_object(&GetObjectRequest {
                bucket: "dst",
                key: "key",
                version_id: None,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert!(dst.body.read_all().unwrap().is_empty());
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
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap()
            .body
            .read_all()
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
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket,
                key,
                metadata: &metadata,
                checksum: Some(MultipartChecksumConfig::new(algo, ctype).unwrap()),

                requester: TEST_REQUESTER,
            })
            .unwrap();
        let mut complete_parts = Vec::new();
        for (i, data) in part_data.iter().enumerate() {
            let part_number = (i + 1) as u32;
            let checksum_b64 = b64.encode(compute_checksum(algo, data));
            let claim = ChecksumClaim::from_base64(algo, &checksum_b64).unwrap();
            let result = test_helpers::upload_part(
                coord,
                &UploadPartRequest {
                    bucket,
                    key,
                    upload_id: &create.upload_id,
                    part_number,
                    data,
                    claimed_checksum: Some(&claim),
                    requester: TEST_REQUESTER,
                },
            )
            .unwrap();
            complete_parts.push(CompletePart {
                part_number,
                etag: result.etag,
                checksum: Some(ChecksumClaim::from_base64(algo, &checksum_b64).unwrap()),
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
        parts[0].checksum =
            Some(ChecksumClaim::from_base64(ChecksumAlgorithm::Crc32, "AAAAAA==").unwrap());

        let err = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        assert_eq!(result.checksum_algorithm, None);
        assert_eq!(result.checksum_type, None);
        assert_eq!(result.checksum_value, None);
    }

    #[test]
    fn complete_multipart_wrong_checksum_element_type_rejected() {
        use base64::Engine;

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
        // Use a structurally valid SHA256 value so the request reaches the algorithm check.
        let wrong_sha256_value = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        parts[0].checksum = Some(
            ChecksumClaim::from_base64(ChecksumAlgorithm::Sha256, &wrong_sha256_value).unwrap(),
        );

        let err = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &upload_id,
                parts: &parts,
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
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
        let session_id = begin_stream_put_test(&coord, "bucket", "mykey").unwrap();
        assert_eq!(session_id.len(), 32);

        // Append two segments.
        let segment0 = b"hello ";
        let segment1 = b"world";
        coord
            .append_stream_segment("bucket", "mykey", &session_id, 0, segment0)
            .unwrap();
        coord
            .append_stream_segment("bucket", "mykey", &session_id, 1, segment1)
            .unwrap();

        // Finalize with caller-computed CRC64 and total_size.
        let mut full_data = Vec::new();
        full_data.extend_from_slice(segment0);
        full_data.extend_from_slice(segment1);
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        assert_eq!(result.etag, format_etag(crc));
        assert_eq!(result.version_id, VersionId::Null);

        // Verify object is visible via head_object.
        let head = coord
            .head_object(&GetObjectRequest {
                bucket: "bucket",
                key: "mykey",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(head.size, full_data.len() as u64);
        assert_eq!(head.etag, format_etag(crc));
        assert_eq!(head.metadata.get("x-amz-meta-foo"), Some("bar"));
    }

    #[test]
    fn stream_put_zero_byte_object() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "mykey").unwrap();

        // Finalize with no segments appended — zero-byte object.
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        assert_eq!(result.etag, format_etag(crc));

        let head = coord
            .head_object(&GetObjectRequest {
                bucket: "bucket",
                key: "mykey",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(head.size, 0);
    }

    #[test]
    fn finalize_stream_put_persists_tags_in_initial_commit() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "mykey").unwrap();
        coord
            .append_stream_segment("bucket", "mykey", &session_id, 0, b"hello")
            .unwrap();

        let tags_xml =
            "<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>";
        let result = coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "mykey",
                session_id: &session_id,
                crc64: checksum::crc64::checksum(b"hello"),
                total_size: 5,
                metadata_blob: &MetadataBlob::new(),
                tags: Some(tags_xml),
                cond: &WriteCondition::default(),
            })
            .unwrap();
        assert_eq!(result.version_id, VersionId::Null);

        let obj = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "mykey",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(obj.tags.as_deref(), Some(tags_xml));
    }

    #[test]
    fn stream_put_abort() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "mykey").unwrap();
        coord
            .append_stream_segment("bucket", "mykey", &session_id, 0, b"data")
            .unwrap();

        // Abort the session.
        coord
            .abort_stream_put("bucket", "mykey", &session_id)
            .unwrap();

        // Object should not exist.
        let err = coord
            .head_object(&GetObjectRequest {
                bucket: "bucket",
                key: "mykey",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn stream_put_append_after_finalize_fails() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "mykey").unwrap();
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Session is deleted after finalize — append should fail.
        let err = coord
            .append_stream_segment("bucket", "mykey", &session_id, 0, b"data")
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

        let session_id = begin_stream_put_test(&coord, "bucket", "mykey").unwrap();
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
                tags: None,
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

        let session_id = begin_stream_put_test(&coord, "bucket", "key1").unwrap();

        // Attempt append with wrong key.
        let err = coord
            .append_stream_segment("bucket", "key2", &session_id, 0, b"data")
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

        let session_id = begin_stream_put_test(&coord, "bucket", "key1").unwrap();

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
                tags: None,
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

        let err = begin_stream_put_test(&coord, "nonexistent", "key").unwrap_err();
        assert!(matches!(err, ServerError::BucketNotFound { .. }));
    }

    #[test]
    fn stream_put_overwrite_existing_object() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // Write an existing object via normal put.
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"old-data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // Stream-put a new version.
        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        let new_data = b"new-streamed-data";
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, new_data)
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();
        assert_eq!(result.etag, format_etag(crc));

        // Head should show the new object.
        let head = coord
            .head_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(head.size, new_data.len() as u64);
    }

    #[test]
    fn stream_put_with_write_condition() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // Write initial object.
        let initial = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"initial",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // Stream put with if-match on the correct etag succeeds.
        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"updated")
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
                tags: None,
                cond: &cond,
            })
            .unwrap();

        // Stream put with if-match on a wrong etag fails.
        let session_id2 = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id2, 0, b"third")
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
                tags: None,
                cond: &bad_cond,
            })
            .unwrap_err();
        assert!(
            matches!(err, ServerError::PreconditionFailed),
            "expected PreconditionFailed, got {err:?}"
        );
    }

    #[test]
    fn stream_put_multiple_segments_correct_manifest() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();

        // Append 3 segments.
        let segments: Vec<&[u8]> = vec![b"aaa", b"bbb", b"ccc"];
        for (i, segment) in segments.iter().enumerate() {
            coord
                .append_stream_segment("bucket", "key", &session_id, i as u32, segment)
                .unwrap();
        }

        let mut full_data = Vec::new();
        for segment in &segments {
            full_data.extend_from_slice(segment);
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();
        assert_eq!(result.etag, format_etag(crc));

        // Verify the committed object segments exist in the metadata PG.
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
        let committed = pg
            .get_object_segments("bucket", "key", result.version_id)
            .unwrap();
        assert_eq!(committed.len(), 3);
        for (i, segment) in committed.iter().enumerate() {
            assert_eq!(segment.segment_index, i as u32);
            assert_eq!(segment.size, 3); // "aaa", "bbb", "ccc" are all 3 bytes
        }
    }

    #[test]
    fn stream_put_abort_cleans_up_shards() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"data-to-clean")
            .unwrap();

        // Record shard keys before abort for verification.
        let segment_okh = crate::pg::stream_segment_key_hash(&session_id, 0);
        let shard_pg_id =
            coord.shard_pg_id(&format!("segment/{session_id}"), "0", GenerationId::MIN);

        coord
            .abort_stream_put("bucket", "key", &session_id)
            .unwrap();

        // Verify shards were cleaned up.
        let pg = coord.storage_node.get_pg(shard_pg_id).unwrap();
        for i in 0..6 {
            // k=4, m=2
            let shard_key = ShardKey::new(&segment_okh, 0, i);
            let result = pg.read_shard(&shard_key);
            assert!(result.is_err(), "shard {i} should have been deleted");
        }
    }

    #[test]
    fn stream_put_get_object_readable() {
        // Stream-finalized objects use object segments for shard data.
        // GET reads from object_segments to reconstruct the object.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"hello")
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // HEAD works (metadata-only).
        let head = coord
            .head_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(head.size, 5);

        // GET returns the correct data.
        let result = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"hello");
        assert_eq!(result.size, 5);
    }

    #[test]
    fn stream_put_get_multi_segment() {
        // Stream-put with multiple segments: GET reconstructs all segments.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"aaaa")
            .unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 1, b"bbbb")
            .unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 2, b"cc")
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        let result = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), full_data);
        assert_eq!(result.size, 10);
    }

    #[test]
    fn segment_list_reader_next_chunk_moves_whole_loaded_segment() {
        let dir = test_util::tempdir();
        let ec_config = EcConfig::new(4, 2).unwrap();
        let runtime = ReadRuntime {
            storage_node: Arc::new(SharedStorageNode::open(dir.path(), &[0]).unwrap()),
            ec_codec: Arc::new(ErasureCodec::new(ec_config).unwrap()),
            ec_config,
            pg_topology: PgTopology::new(&[0]).unwrap(),
            payload_buffer_pool: PayloadBufferPool::new(ec_config),
        };

        let data = vec![1u8, 2, 3, 4];
        let ptr = data.as_ptr();
        let len = data.len();
        let mut reader = SegmentListReader {
            runtime,
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            segments: vec![],
            next_segment_index: 0,
            loaded_segment: Some((Arc::new(SharedPayloadBuffer::from_unpooled(data)), 0, len)),
        };

        let chunk = reader.next_chunk(len).unwrap().unwrap();
        assert_eq!(chunk.as_ref(), [1u8, 2, 3, 4]);
        assert_eq!(chunk.as_ref().as_ptr(), ptr);
        assert!(reader.loaded_segment.is_none());
        assert!(reader.next_chunk(len).unwrap().is_none());
    }

    #[test]
    fn get_object_reuses_payload_buffer_for_repeated_segment_reads() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        let data = vec![7u8; INTERNAL_SEGMENT_SIZE];
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, &data)
            .unwrap();
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: checksum::crc64::checksum(&data),
                total_size: data.len() as u64,
                metadata_blob: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
            })
            .unwrap();

        assert_eq!(coord.payload_buffer_pool.allocation_count(), 0);

        let first = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(first.body.read_all().unwrap(), data);
        assert_eq!(coord.payload_buffer_pool.allocation_count(), 1);

        let second = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(second.body.read_all().unwrap(), data);
        assert_eq!(coord.payload_buffer_pool.allocation_count(), 1);
    }

    #[test]
    fn stream_put_range_read() {
        // Range reads on stream-put objects work correctly.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"AAAA")
            .unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 1, b"BBBB")
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Range within first segment.
        let r1 = coord
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                range: ByteRange::Range { start: 0, end: 3 },
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(r1.body.read_all().unwrap(), b"AAAA");

        // Range spanning segments.
        let r2 = coord
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                range: ByteRange::Range { start: 2, end: 5 },
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(r2.body.read_all().unwrap(), b"AABB");

        // Range within second segment.
        let r3 = coord
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                range: ByteRange::Range { start: 4, end: 7 },
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(r3.body.read_all().unwrap(), b"BBBB");

        // Suffix range.
        let r4 = coord
            .get_object_range(&GetObjectRangeRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                range: ByteRange::Suffix { length: 3 },
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(r4.body.read_all().unwrap(), b"BBB");
    }

    #[test]
    fn stream_put_copy_object() {
        // CopyObject from a stream-put source works correctly.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "src").unwrap();
        coord
            .append_stream_segment("bucket", "src", &session_id, 0, b"copy-me")
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
                tags: None,
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
                tagging: TaggingDirective::Copy,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            })
            .unwrap();

        // Buffered copy destinations now commit through the same segmented
        // path as normal PutObject writes.
        {
            let meta_pg_id = coord.object_pg_id("bucket", "dst");
            let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let segments = pg
                .get_object_segments("bucket", "dst", VersionId::Null)
                .unwrap();
            assert_eq!(segments.len(), 1);
            assert_eq!(segments[0].size, 7);
        }

        let result = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "dst",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"copy-me");
    }

    #[test]
    fn buffered_put_writes_object_segments() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let data = vec![0x5A; (INTERNAL_SEGMENT_SIZE * 2) + 123];
        let result = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: &data,
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        {
            let meta_pg_id = coord.object_pg_id("bucket", "key");
            let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let segments = pg
                .get_object_segments("bucket", "key", result.version_id)
                .unwrap();
            assert_eq!(segments.len(), 3);
            assert_eq!(segments[0].segment_index, 0);
            assert_eq!(segments[0].size, INTERNAL_SEGMENT_SIZE as u64);
            assert_eq!(segments[1].segment_index, 1);
            assert_eq!(segments[1].size, INTERNAL_SEGMENT_SIZE as u64);
            assert_eq!(segments[2].segment_index, 2);
            assert_eq!(segments[2].size, 123);
        }

        let get = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(get.body.read_all().unwrap(), data);
    }

    #[test]
    fn buffered_put_overwrite_eventually_reclaims_old_segments() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let old_data = vec![0x41; INTERNAL_SEGMENT_SIZE + 17];
        let first = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: &old_data,
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let old_segments = {
            let meta_pg_id = coord.object_pg_id("bucket", "key");
            let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            pg.get_object_segments("bucket", "key", first.version_id)
                .unwrap()
        };
        assert_eq!(old_segments.len(), 2);

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"new-data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        for segment in old_segments {
            wait_for_shard_set_deletion(
                &coord,
                segment.shard_pg_id,
                &segment.segment_okh,
                segment.segment_vid,
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            );
        }
    }

    #[test]
    fn stream_put_zero_byte_get() {
        // Zero-byte stream-put objects are readable.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "empty").unwrap();
        let crc = checksum::crc64::checksum(b"");
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "empty",
                session_id: &session_id,
                crc64: crc,
                total_size: 0,
                metadata_blob: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        let result = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "empty",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"");
        assert_eq!(result.size, 0);
    }

    #[test]
    fn stream_put_get_object_part() {
        // partNumber=1 on stream-put objects returns the full body.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"partdata")
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        let result = coord
            .get_object_part(&GetObjectPartRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                part_number: 1,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"partdata");
        assert_eq!(result.part_size, 8);
    }

    #[test]
    fn stream_put_overwrite_with_normal_put_cleans_segments() {
        // P0 fix: normal PUT after stream-write must clear stale segment rows.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // Stream-write an object.
        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"stream-data")
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Verify stream-put is readable.
        let r1 = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(r1.body.read_all().unwrap(), b"stream-data");

        // Overwrite with a normal PUT.
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "key",
                data: b"normal-data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // GET should return the new data, not stale segment data.
        let r2 = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(r2.body.read_all().unwrap(), b"normal-data");
    }

    #[test]
    fn stream_put_delete_cleans_segments() {
        // P1 fix: delete must clean up object_segments and their shards.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"delete-me")
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Delete the object.
        coord
            .delete_object(&DeleteObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: &crate::conditional::DeleteCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Object should be gone.
        let err = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn stream_put_delete_eventually_reclaims_segment_shards() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"delete-me")
            .unwrap();
        let crc = checksum::crc64::checksum(b"delete-me");
        let result = coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &session_id,
                crc64: crc,
                total_size: 9,
                metadata_blob: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        let segments = {
            let meta_pg_id = coord.object_pg_id("bucket", "key");
            let pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            pg.get_object_segments("bucket", "key", result.version_id)
                .unwrap()
        };

        coord
            .delete_object(&DeleteObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_DELETE,
                requester: TEST_REQUESTER,
            })
            .unwrap();

        for segment in segments {
            wait_for_shard_set_deletion(
                &coord,
                segment.shard_pg_id,
                &segment.segment_okh,
                segment.segment_vid,
                EcShape {
                    k: segment.ec_k,
                    m: segment.ec_m,
                },
            );
        }
    }

    #[test]
    fn stream_put_upload_part_copy_from_stream_source() {
        // P2 fix: UploadPartCopy must be able to read stream-written source objects.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // Stream-write a source object.
        let session_id = begin_stream_put_test(&coord, "bucket", "src").unwrap();
        coord
            .append_stream_segment("bucket", "src", &session_id, 0, b"source-data")
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Create a multipart upload for the destination.
        let upload = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "dst",
                metadata: &MetadataBlob::new(),
                checksum: None,

                requester: TEST_REQUESTER,
            })
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
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert!(!result.etag.is_empty());
    }

    #[test]
    fn upload_part_copy_streams_multisegment_source_and_persists_checksum() {
        use base64::Engine;

        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let data: Vec<u8> = (0..((2 * INTERNAL_SEGMENT_SIZE) + 12_345))
            .map(|i| (i % 251) as u8)
            .collect();

        let session_id = begin_stream_put_test(&coord, "bucket", "src").unwrap();
        let mut crc64 = checksum::crc64::Hasher::new();
        for (idx, chunk) in data.chunks(INTERNAL_SEGMENT_SIZE).enumerate() {
            coord
                .append_stream_segment("bucket", "src", &session_id, idx as u32, chunk)
                .unwrap();
            crc64.update(chunk);
        }
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "src",
                session_id: &session_id,
                crc64: crc64.finalize(),
                total_size: data.len() as u64,
                metadata_blob: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        let upload = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "dst",
                metadata: &MetadataBlob::new(),
                checksum: Some(
                    MultipartChecksumConfig::new(ChecksumAlgorithm::Crc32c, None).unwrap(),
                ),
                requester: TEST_REQUESTER,
            })
            .unwrap();

        let part = coord
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
                requester: TEST_REQUESTER,
            })
            .unwrap();

        let parts = coord
            .list_parts(&ListPartsRequest {
                bucket: "bucket",
                key: "dst",
                upload_id: &upload.upload_id,
                part_number_marker: None,
                max_parts: 1000,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(parts.checksum_algorithm, Some(ChecksumAlgorithm::Crc32c));
        assert_eq!(parts.parts.len(), 1);
        let expected_checksum = base64::engine::general_purpose::STANDARD
            .encode(checksum::crc32c::checksum(&data).to_be_bytes());
        assert_eq!(
            parts.parts[0].checksum.as_deref(),
            Some(expected_checksum.as_str())
        );

        coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "dst",
                upload_id: &upload.upload_id,
                parts: &[CompletePart {
                    part_number: 1,
                    etag: part.etag,
                    checksum: Some(
                        ChecksumClaim::from_base64(
                            ChecksumAlgorithm::Crc32c,
                            parts.parts[0].checksum.as_deref().unwrap(),
                        )
                        .unwrap(),
                    ),
                }],
                claimed_checksum: None,
                requester: TEST_REQUESTER,
            })
            .unwrap();

        let copied = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "dst",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(copied.body.read_all().unwrap(), data);
    }

    #[test]
    fn upload_part_copy_rejects_non_owner_requester() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "src",
                data: b"source-data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: NO_WRITE,
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let upload = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "dst",
                metadata: &MetadataBlob::new(),
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        let err = coord
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
                requester: Requester::principal("other-user"),
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::AccessDenied));
    }

    #[test]
    fn stream_put_duplicate_segment_index_rejected() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"first")
            .unwrap();

        // Appending the same segment_index again should be rejected.
        let err = coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"second")
            .unwrap_err();
        assert!(
            matches!(err, ServerError::InvalidRequest { .. }),
            "expected InvalidRequest for duplicate segment_index, got {err:?}"
        );

        // Original segment should still be intact — verify by finalizing.
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();
    }

    #[test]
    fn stream_put_total_size_mismatch_rejected() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"hello")
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
                tags: None,
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
        // append_stream_segment accepts both PutObject and UploadPart sessions.
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
            .append_stream_segment("bucket", "key", "upload-part-session", 0, b"data")
            .unwrap();
    }

    #[test]
    fn stream_append_reuses_encode_scratch_for_aligned_segments() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        assert_eq!(coord.encode_scratch_pool.allocation_count(), 0);

        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"data")
            .unwrap();
        assert_eq!(coord.encode_scratch_pool.allocation_count(), 1);

        coord
            .append_stream_segment("bucket", "key", &session_id, 1, b"more")
            .unwrap();
        assert_eq!(coord.encode_scratch_pool.allocation_count(), 1);
    }

    // ── Phase 3a: Streaming UploadPart tests ─────────────────────────

    #[test]
    fn stream_part_happy_path() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // Create a multipart upload first.
        let mpu = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &MetadataBlob::new(),
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Begin a streaming part session.
        let session = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1).unwrap();
        let session_id = session.session_id;

        // Append segments.
        let data = b"hello streaming part";
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, data)
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

        let err = begin_stream_part_test(&coord, "bucket", "key", "nonexistent", 1).unwrap_err();
        assert!(matches!(err, ServerError::NoSuchUpload { .. }));
    }

    #[test]
    fn stream_part_invalid_part_number_rejected() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let mpu = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &MetadataBlob::new(),
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Part 0 is invalid.
        let err = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 0).unwrap_err();
        assert!(matches!(err, ServerError::InvalidArgument { .. }));

        // Part 10001 is invalid.
        let err =
            begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 10_001).unwrap_err();
        assert!(matches!(err, ServerError::InvalidArgument { .. }));
    }

    #[test]
    fn checksum_claim_invalid_base64_rejected() {
        // P2: Malformed base64 in claimed checksum must return an error,
        // not silently accept a None checksum.
        let err = ChecksumClaim::from_base64(ChecksumAlgorithm::Crc32, "not-valid-base64!!!")
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
        let err = ChecksumClaim::from_base64(ChecksumAlgorithm::Crc32, &too_long).unwrap_err();
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

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"data")
            .unwrap();

        let mpu = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &MetadataBlob::new(),
                checksum: None,

                requester: TEST_REQUESTER,
            })
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
        // corrupt each other's segment data. Upload A completes first; upload B
        // completes second (overwriting A). Each must read back its own data.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();

        // Create two MPUs for the same key.
        let mpu_a = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();
        let mpu_b = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Helper: stream a single part with given data.
        let stream_part = |upload_id: &str, data: &[u8]| -> CompletePart {
            let sess = begin_stream_part_test(&coord, "bucket", "key", upload_id, 1)
                .unwrap()
                .session_id;
            coord
                .append_stream_segment("bucket", "key", &sess, 0, data)
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
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &mpu_a.upload_id,
                parts: &[part_a],
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Read back A's data — should be A's content.
        let obj_a = coord
            .get_object_part(&GetObjectPartRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                part_number: 1,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(
            obj_a.body.read_all().unwrap(),
            data_a,
            "after completing A, reading part 1 should return A's data"
        );

        // Complete B — overwrites A on unversioned bucket.
        let result_b = coord
            .complete_multipart_upload(&CompleteMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &mpu_b.upload_id,
                parts: &[part_b],
                claimed_checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Read back B's data — should be B's content, not A's.
        let obj_b = coord
            .get_object_part(&GetObjectPartRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                part_number: 1,
                cond: &ReadCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(
            obj_b.body.read_all().unwrap(),
            data_b,
            "after completing B, reading part 1 should return B's data"
        );

        // Sanity: version IDs should both be 0 (unversioned).
        assert_eq!(result_a.version_id, VersionId::Null);
        assert_eq!(result_b.version_id, VersionId::Null);
    }

    #[test]
    fn abort_streamed_mpu_cleans_object_segments_and_shards() {
        // Regression: aborting an MPU with streamed parts must delete
        // multipart_part_segments rows and their shard data.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let metadata = MetadataBlob::new();
        let mpu = coord
            .create_multipart_upload(&CreateMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                metadata: &metadata,
                checksum: None,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Upload a streaming part.
        let sess = begin_stream_part_test(&coord, "bucket", "key", &mpu.upload_id, 1)
            .unwrap()
            .session_id;
        let data = b"streamed-part-data-for-abort-test";
        coord
            .append_stream_segment("bucket", "key", &sess, 0, data)
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

        // Capture segment records before abort for shard verification.
        let meta_pg_id = coord.object_pg_id("bucket", "key");
        let segments_before = {
            let meta_pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let segments = meta_pg
                .get_all_multipart_part_segments_for_upload(&mpu.upload_id)
                .unwrap();
            assert!(!segments.is_empty(), "segments should exist before abort");
            segments
        };

        // Verify shard data exists before abort.
        for segment in &segments_before {
            let shard_pg = coord.storage_node.get_pg(segment.shard_pg_id).unwrap();
            let total = segment.ec_k as usize + segment.ec_m as usize;
            for i in 0..total {
                let shard_key =
                    ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
                assert!(
                    shard_pg.read_shard(&shard_key).is_ok(),
                    "shard {i} should exist before abort"
                );
            }
        }

        // Abort the MPU.
        coord
            .abort_multipart_upload(&AbortMultipartUploadRequest {
                bucket: "bucket",
                key: "key",
                upload_id: &mpu.upload_id,

                requester: TEST_REQUESTER,
            })
            .unwrap();

        // Verify object segments rows are gone.
        {
            let meta_pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let segments = meta_pg
                .get_all_multipart_part_segments_for_upload(&mpu.upload_id)
                .unwrap();
            assert!(
                segments.is_empty(),
                "object segments rows should be deleted after abort"
            );
        }

        // Verify shard data is gone.
        for segment in &segments_before {
            let shard_pg = coord.storage_node.get_pg(segment.shard_pg_id).unwrap();
            let total = segment.ec_k as usize + segment.ec_m as usize;
            for i in 0..total {
                let shard_key =
                    ShardKey::new(&segment.segment_okh, segment.segment_vid.get(), i as u8);
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
        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"data")
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
            .append_stream_segment("bucket", "key", &session_id, 1, b"more")
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
        // scavenge should not affect the object or its object segments.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &session_id, 0, b"safe-data")
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Scavenge with max_age=0 — should find nothing to clean.
        let count = coord.scavenge_stale_sessions(0);
        assert_eq!(count, 0);

        // Object should still be readable.
        let result = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"safe-data");
    }

    #[test]
    fn object_not_visible_before_finalize() {
        // Atomic visibility: object is not readable before finalize.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "new-key").unwrap();
        coord
            .append_stream_segment("bucket", "new-key", &session_id, 0, b"pending")
            .unwrap();

        // Key should not exist yet.
        let err = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "new-key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));

        // HEAD should also fail.
        let err = coord
            .head_object(&GetObjectRequest {
                bucket: "bucket",
                key: "new-key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap_err();
        assert!(matches!(err, ServerError::ObjectNotFound { .. }));
    }

    #[test]
    fn object_segments_integrity_readback() {
        // Storage-level verification: committed object segments rows match
        // what was written, and shard data is intact.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let session_id = begin_stream_put_test(&coord, "bucket", "verify").unwrap();
        coord
            .append_stream_segment("bucket", "verify", &session_id, 0, b"chunk-0-")
            .unwrap();
        coord
            .append_stream_segment("bucket", "verify", &session_id, 1, b"chunk-1-")
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Read back via storage layer directly.
        let meta_pg_id = coord.object_pg_id("bucket", "verify");
        {
            let meta_pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let record = meta_pg.get_object_meta("bucket", "verify").unwrap();
            let segments = meta_pg
                .get_object_segments("bucket", "verify", record.version_id())
                .unwrap();

            assert_eq!(segments.len(), 2);
            assert_eq!(segments[0].segment_index, 0);
            assert_eq!(segments[0].size, 8);
            assert_eq!(
                segments[0].segment_crc64,
                Some(checksum::crc64::checksum(b"chunk-0-"))
            );
            assert_eq!(segments[1].segment_index, 1);
            assert_eq!(segments[1].size, 8);
            assert_eq!(
                segments[1].segment_crc64,
                Some(checksum::crc64::checksum(b"chunk-1-"))
            );
        } // Drop PG lock before coordinator calls.

        // Verify full readback via coordinator.
        let result = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "verify",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        let data = result.body.read_all().unwrap();
        assert_eq!(data, full);

        // Verify CRC matches.
        assert_eq!(checksum::crc64::checksum(&data), crc);
    }

    #[test]
    fn get_object_rejects_bad_segment_crc64() {
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        let put = test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "bad-segment-crc",
                data: b"segment-data",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        let meta_pg_id = coord.object_pg_id("bucket", "bad-segment-crc");
        {
            let meta_pg = coord.storage_node.get_pg(meta_pg_id).unwrap();
            let record = meta_pg
                .get_object_meta("bucket", "bad-segment-crc")
                .unwrap();
            let live = record.as_live().unwrap();
            let mut segments = meta_pg
                .get_object_segments("bucket", "bad-segment-crc", put.version_id)
                .unwrap();
            assert_eq!(segments.len(), 1);
            segments[0].segment_crc64 = Some(segments[0].segment_crc64.unwrap() ^ 1);

            meta_pg
                .put_object_with_segments(
                    &PutLiveObjectReq {
                        bucket: live.bucket.clone(),
                        key: live.key.clone(),
                        version_id: live.version_id,
                        generation_id: live.generation_id,
                        size: live.size,
                        etag: live.etag,
                        ec: live.ec,
                        layout: live.layout,
                        tags: live.tags.clone(),
                        metadata_blob: live.metadata_blob.clone(),
                    },
                    &segments,
                )
                .unwrap();
        }

        let result = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "bad-segment-crc",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        let err = result.body.read_all().unwrap_err();
        assert!(matches!(
            err,
            ServerError::Store(storage::StoreError::IntegrityError { .. })
        ));
    }

    #[test]
    fn stream_put_delete_then_reput() {
        // Overwrite cycle: stream-put → delete → normal put → GET succeeds.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // 1. Stream-write.
        let session_id = begin_stream_put_test(&coord, "bucket", "cycle").unwrap();
        coord
            .append_stream_segment("bucket", "cycle", &session_id, 0, b"v1")
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
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // 2. Delete.
        coord
            .delete_object(&DeleteObjectRequest {
                bucket: "bucket",
                key: "cycle",
                version_id: None,
                cond: &crate::conditional::DeleteCondition::default(),
                requester: TEST_REQUESTER,
            })
            .unwrap();

        // 3. Normal put.
        test_helpers::put_object(
            &coord,
            &PutObjectRequest {
                bucket: "bucket",
                key: "cycle",
                data: b"v2-normal",
                metadata: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
                requester: TEST_REQUESTER,
                acl: NO_PUT_OBJECT_ACL,
            },
        )
        .unwrap();

        // 4. GET should return normal-put data, no object segments interference.
        let result = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "cycle",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"v2-normal");
    }

    #[test]
    fn stream_put_overwrite_with_stream_put() {
        // Stream-write → stream-write overwrite: second write's segments replace first.
        let dir = test_util::tempdir();
        let coord = setup_coordinator(dir.path());
        coord.create_bucket("bucket").unwrap();

        // First stream-write.
        let s1 = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &s1, 0, b"old-data")
            .unwrap();
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &s1,
                crc64: checksum::crc64::checksum(b"old-data"),
                total_size: 8,
                metadata_blob: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        // Second stream-write (overwrite).
        let s2 = begin_stream_put_test(&coord, "bucket", "key").unwrap();
        coord
            .append_stream_segment("bucket", "key", &s2, 0, b"new-data")
            .unwrap();
        coord
            .finalize_stream_put(&FinalizeStreamPutRequest {
                bucket: "bucket",
                key: "key",
                session_id: &s2,
                crc64: checksum::crc64::checksum(b"new-data"),
                total_size: 8,
                metadata_blob: &MetadataBlob::new(),
                tags: None,
                cond: &WriteCondition::default(),
            })
            .unwrap();

        let result = coord
            .get_object(&GetObjectRequest {
                bucket: "bucket",
                key: "key",
                version_id: None,
                cond: NO_READ,
                requester: TEST_REQUESTER,
            })
            .unwrap();
        assert_eq!(result.body.read_all().unwrap(), b"new-data");
    }
}
