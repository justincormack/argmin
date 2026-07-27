use std::sync::Arc;

use storage::{
    BucketName, GenerationId, MultipartPartSegmentRecord, ObjectEncryption, ObjectKey,
    ObjectPartRecord, ObjectPayloadLease, ObjectSegmentRecord, RetainedObjectPayloadRead,
    StorageCluster,
};

use super::object_state::SnapshottedMultipartPart;
use super::payload::{PayloadBufferPool, SharedPayloadBuffer};
#[cfg(test)]
use super::test_hooks::maybe_run_object_segments_first_segment_hook;
use super::TRACE_TARGET;
use crate::error::ServerError;
use crate::sse::{SseCustomerRequest, SseCustomerValidatorConfig, StaticManagedKeyProvider};
#[cfg(test)]
use storage::PgTopology;

#[derive(Debug, Clone)]
pub struct ReadChunk {
    pub(super) data: Arc<SharedPayloadBuffer>,
    pub(super) start: usize,
    pub(super) end: usize,
}

#[derive(Clone)]
pub(super) enum ReadStorage {
    Cluster(Arc<StorageCluster>),
    Retained(Arc<RetainedObjectPayloadRead>),
}

#[derive(Clone)]
pub(super) struct ReadRuntime {
    pub(super) storage: ReadStorage,
    #[cfg(test)]
    pub(super) pg_topology: PgTopology,
    pub(super) payload_buffer_pool: Arc<PayloadBufferPool>,
    pub(super) sse_c_validator: Option<SseCustomerValidatorConfig>,
    pub(super) managed_key_provider: Option<StaticManagedKeyProvider>,
}

#[derive(Debug, Clone)]
pub(super) struct SegmentPayloadRecord {
    pub(super) segment_index: u32,
    pub(super) size: u64,
    pub(super) segment_crc64: u64,
    pub(super) segment_okh: [u8; 16],
    pub(super) segment_vid: GenerationId,
    pub(super) data_pg_id: u32,
    pub(super) placement_cluster_epoch: storage::ClusterEpoch,
    pub(super) ec_k: u8,
    pub(super) ec_m: u8,
    pub(super) encryption: ObjectEncryption,
}

#[cfg_attr(not(feature = "deep-tracing"), allow(dead_code))]
#[derive(Debug, Clone)]
pub(super) struct SegmentSliceRecord {
    pub(super) payload: SegmentPayloadRecord,
    pub(super) segment_index: usize,
    pub(super) segment_object_offset_start: usize,
    pub(super) segment_object_offset_end_exclusive: usize,
    pub(super) start_offset: usize,
    pub(super) end_offset: usize,
    pub(super) part_number: Option<u32>,
    pub(super) part_order: Option<usize>,
    pub(super) part_object_offset_start: Option<usize>,
    pub(super) part_object_offset_end_exclusive: Option<usize>,
}

#[derive(Debug, Clone)]
pub(super) struct MultipartPartReadLayout {
    pub(super) part_number: u32,
    pub(super) part_order: usize,
    pub(super) object_offset_start: usize,
    pub(super) object_offset_end_exclusive: usize,
    pub(super) part_size: usize,
    pub(super) payload_crc64: u64,
}

pub(super) struct SegmentListReader {
    runtime: ReadRuntime,
    #[cfg_attr(not(any(test, feature = "deep-tracing")), allow(dead_code))]
    bucket: String,
    #[cfg_attr(not(any(test, feature = "deep-tracing")), allow(dead_code))]
    key: String,
    segments: Vec<SegmentSliceRecord>,
    next_segment_index: usize,
    loaded_segment: Option<(Arc<SharedPayloadBuffer>, usize, usize)>,
    sse_customer_request: Option<SseCustomerRequest>,
    verification: SegmentListVerification,
    verified_size: usize,
    verified_crc64: checksum::crc64::Hasher,
}

#[derive(Debug, Clone, Copy)]
enum SegmentListVerification {
    StoredSegmentCrcOnly,
    FullPayloadCrc {
        expected_size: usize,
        expected_crc64: u64,
    },
}

#[cfg_attr(not(feature = "deep-tracing"), allow(dead_code))]
#[derive(Debug, Clone)]
pub(super) struct SnapshottedMultipartPartRange {
    pub(super) layout: MultipartPartReadLayout,
    pub(super) segments: Vec<SegmentSliceRecord>,
    pub(super) verify_payload_crc: bool,
}

pub(super) struct MultipartReader {
    runtime: ReadRuntime,
    bucket: String,
    key: String,
    parts: Vec<SnapshottedMultipartPartRange>,
    next_part_index: usize,
    current_part: Option<SegmentListReader>,
    sse_customer_request: Option<SseCustomerRequest>,
}

pub(super) struct ReadObjectContext<'a> {
    pub(super) runtime: ReadRuntime,
    pub(super) bucket: &'a BucketName,
    pub(super) key: &'a ObjectKey,
    pub(super) generation_id: GenerationId,
    pub(super) sse_customer_request: Option<SseCustomerRequest>,
}

// Boxing the segment reader would add heap traffic on the normal read path.
#[allow(clippy::large_enum_variant)]
pub(super) enum ReadHandleInner {
    Segments(SegmentListReader),
    Multipart(Box<MultipartReader>),
    TestBuffered(Option<Vec<u8>>),
}

pub(super) struct PayloadLease {
    pub(super) lease: Option<ObjectPayloadLease>,
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

impl SegmentListReader {
    fn new_stored_segment_crc_checked(
        runtime: ReadRuntime,
        bucket: String,
        key: String,
        segments: Vec<SegmentSliceRecord>,
        sse_customer_request: Option<SseCustomerRequest>,
    ) -> Self {
        Self {
            runtime,
            bucket,
            key,
            segments,
            next_segment_index: 0,
            loaded_segment: None,
            sse_customer_request,
            verification: SegmentListVerification::StoredSegmentCrcOnly,
            verified_size: 0,
            verified_crc64: checksum::crc64::Hasher::new(),
        }
    }

    fn new_full_payload_crc_checked(
        runtime: ReadRuntime,
        bucket: String,
        key: String,
        segments: Vec<SegmentSliceRecord>,
        sse_customer_request: Option<SseCustomerRequest>,
        expected_size: usize,
        expected_crc64: u64,
    ) -> Self {
        Self {
            runtime,
            bucket,
            key,
            segments,
            next_segment_index: 0,
            loaded_segment: None,
            sse_customer_request,
            verification: SegmentListVerification::FullPayloadCrc {
                expected_size,
                expected_crc64,
            },
            verified_size: 0,
            verified_crc64: checksum::crc64::Hasher::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn test_loaded_segment(
        runtime: ReadRuntime,
        bucket: String,
        key: String,
        data: Vec<u8>,
    ) -> Self {
        let len = data.len();
        Self {
            runtime,
            bucket,
            key,
            segments: vec![],
            next_segment_index: 0,
            loaded_segment: Some((Arc::new(SharedPayloadBuffer::from_unpooled(data)), 0, len)),
            sse_customer_request: None,
            verification: SegmentListVerification::StoredSegmentCrcOnly,
            verified_size: 0,
            verified_crc64: checksum::crc64::Hasher::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn test_loaded_segment_is_none(&self) -> bool {
        self.loaded_segment.is_none()
    }

    pub(super) fn next_chunk(
        &mut self,
        target_size: usize,
    ) -> Result<Option<ReadChunk>, ServerError> {
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
                if let SegmentListVerification::FullPayloadCrc {
                    expected_size,
                    expected_crc64,
                } = self.verification
                {
                    if self.verified_size != expected_size {
                        return Err(ServerError::IntegrityError {
                            bucket: self.bucket.clone(),
                            key: self.key.clone(),
                            expected: expected_size as u64,
                            actual: self.verified_size as u64,
                        });
                    }
                    let actual_crc64 = self.verified_crc64.finalize();
                    if actual_crc64 != expected_crc64 {
                        return Err(ServerError::IntegrityError {
                            bucket: self.bucket.clone(),
                            key: self.key.clone(),
                            expected: expected_crc64,
                            actual: actual_crc64,
                        });
                    }
                }
                return Ok(None);
            }

            let slice = self.segments[self.next_segment_index].clone();
            #[cfg(feature = "deep-tracing")]
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
                            "bucket={:?} key={:?} part_order={} part_number={} part_object_offset_start={} part_object_offset_len={} part_object_offset_end_exclusive={} segment_index={} segment_size={} segment_object_offset_start={} segment_object_offset_end_exclusive={} read_object_offset_start={} read_object_offset_len={} read_object_offset_end_exclusive={} read_segment_offset_start={} read_segment_offset_len={} read_segment_offset_end_exclusive={} data_pg_id={} ec_k={} ec_m={}",
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
                            slice.payload.data_pg_id,
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
                            "bucket={:?} key={:?} segment_index={} segment_size={} segment_object_offset_start={} segment_object_offset_end_exclusive={} read_object_offset_start={} read_object_offset_len={} read_object_offset_end_exclusive={} read_segment_offset_start={} read_segment_offset_len={} read_segment_offset_end_exclusive={} data_pg_id={} ec_k={} ec_m={}",
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
                            slice.payload.data_pg_id,
                            slice.payload.ec_k,
                            slice.payload.ec_m,
                        )),
                    );
                }
            }

            let data = self.runtime.read_checked_segment_payload(
                &slice.payload,
                slice.part_number,
                self.sse_customer_request.as_ref(),
            )?;
            if matches!(
                self.verification,
                SegmentListVerification::FullPayloadCrc { .. }
            ) {
                self.verified_size += data.len();
                self.verified_crc64.update(data.as_ref());
            }
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
    pub(super) fn next_chunk(
        &mut self,
        target_size: usize,
    ) -> Result<Option<ReadChunk>, ServerError> {
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
            #[cfg(feature = "deep-tracing")]
            if let Some(trace) = observability::current_context() {
                let _ = observability::event_in_context(
                    &trace,
                    TRACE_TARGET,
                    "read_multipart_part_layout",
                    Some(format_args!(
                        "bucket={:?} key={:?} part_order={} part_number={} part_object_offset_start={} part_object_offset_len={} part_object_offset_end_exclusive={} segment_count={}",
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
            self.current_part = Some(if part.verify_payload_crc {
                SegmentListReader::new_full_payload_crc_checked(
                    self.runtime.clone(),
                    self.bucket.clone(),
                    self.key.clone(),
                    part.segments,
                    self.sse_customer_request.clone(),
                    part.layout.part_size,
                    part.layout.payload_crc64,
                )
            } else {
                SegmentListReader::new_stored_segment_crc_checked(
                    self.runtime.clone(),
                    self.bucket.clone(),
                    self.key.clone(),
                    part.segments,
                    self.sse_customer_request.clone(),
                )
            });
        }
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
        verify_payload_crc: bool,
    ) -> Vec<SnapshottedMultipartPartRange> {
        let mut ranges = Vec::new();

        for (part_order, part) in parts.into_iter().enumerate() {
            let part_start = part.object_offset_start;
            let part_size = part.record.size as usize;
            let part_end = part_start + part_size;
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
                    part_size,
                    payload_crc64: part.record.payload_crc64,
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
                    verify_payload_crc,
                });
            }
        }

        ranges
    }

    pub(super) fn from_segments(
        ctx: ReadObjectContext<'_>,
        segments: Vec<SegmentPayloadRecord>,
        expected_size: usize,
        expected_crc64: Option<u64>,
    ) -> Result<Self, ServerError> {
        let ReadObjectContext {
            runtime,
            bucket,
            key,
            generation_id,
            sse_customer_request,
        } = ctx;
        let bucket_owned = bucket.as_str().to_string();
        let key_owned = key.as_str().to_string();
        let segments =
            Self::segment_slices_for_range(segments, 0, expected_size.saturating_sub(1), 0, None);
        let lease = runtime.prepare_object_payload_read(
            bucket,
            key,
            generation_id,
            segments.iter().map(|slice| &slice.payload),
        )?;
        Ok(Self {
            bucket: bucket_owned.clone(),
            key: key_owned.clone(),
            lease,
            trace: observability::current_context(),
            expected_size,
            bytes_emitted: 0,
            expected_crc64,
            crc64: checksum::crc64::Hasher::new(),
            inner: ReadHandleInner::Segments(match expected_crc64 {
                Some(expected_crc64) => SegmentListReader::new_full_payload_crc_checked(
                    runtime,
                    bucket_owned,
                    key_owned,
                    segments,
                    sse_customer_request,
                    expected_size,
                    expected_crc64,
                ),
                None => SegmentListReader::new_stored_segment_crc_checked(
                    runtime,
                    bucket_owned,
                    key_owned,
                    segments,
                    sse_customer_request,
                ),
            }),
        })
    }

    pub(super) fn from_segments_range(
        ctx: ReadObjectContext<'_>,
        segments: Vec<SegmentPayloadRecord>,
        start: usize,
        end: usize,
    ) -> Result<Self, ServerError> {
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
        let segments = Self::segment_slices_for_range(segments, start, end, 0, None);
        let lease = runtime.prepare_object_payload_read(
            bucket,
            key,
            generation_id,
            segments.iter().map(|slice| &slice.payload),
        )?;
        Ok(Self {
            bucket: bucket_owned.clone(),
            key: key_owned.clone(),
            lease,
            trace: observability::current_context(),
            expected_size,
            bytes_emitted: 0,
            expected_crc64: None,
            crc64: checksum::crc64::Hasher::new(),
            inner: ReadHandleInner::Segments(SegmentListReader::new_stored_segment_crc_checked(
                runtime,
                bucket_owned,
                key_owned,
                segments,
                sse_customer_request,
            )),
        })
    }

    pub(super) fn from_multipart(
        runtime: ReadRuntime,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        parts: Vec<SnapshottedMultipartPart>,
        expected_size: usize,
        sse_customer_request: Option<SseCustomerRequest>,
    ) -> Result<Self, ServerError> {
        let ranges =
            Self::multipart_ranges_for_range(parts, 0, expected_size.saturating_sub(1), true);
        let lease = runtime.prepare_object_payload_read(
            bucket,
            key,
            generation_id,
            ranges
                .iter()
                .flat_map(|range| range.segments.iter().map(|slice| &slice.payload)),
        )?;
        Ok(Self {
            bucket: bucket.as_str().to_string(),
            key: key.as_str().to_string(),
            lease,
            trace: observability::current_context(),
            expected_size,
            bytes_emitted: 0,
            expected_crc64: None,
            crc64: checksum::crc64::Hasher::new(),
            inner: ReadHandleInner::Multipart(Box::new(MultipartReader {
                runtime,
                bucket: bucket.as_str().to_string(),
                key: key.as_str().to_string(),
                parts: ranges,
                next_part_index: 0,
                current_part: None,
                sse_customer_request,
            })),
        })
    }

    pub(super) fn from_multipart_range(
        runtime: ReadRuntime,
        bucket: &BucketName,
        key: &ObjectKey,
        generation_id: GenerationId,
        parts: Vec<SnapshottedMultipartPart>,
        range: (usize, usize),
        sse_customer_request: Option<SseCustomerRequest>,
    ) -> Result<Self, ServerError> {
        let (start, end) = range;
        let expected_size = end - start + 1;
        let ranges = Self::multipart_ranges_for_range(parts, start, end, false);
        let lease = runtime.prepare_object_payload_read(
            bucket,
            key,
            generation_id,
            ranges
                .iter()
                .flat_map(|range| range.segments.iter().map(|slice| &slice.payload)),
        )?;
        Ok(Self {
            bucket: bucket.as_str().to_string(),
            key: key.as_str().to_string(),
            lease,
            trace: observability::current_context(),
            expected_size,
            bytes_emitted: 0,
            expected_crc64: None,
            crc64: checksum::crc64::Hasher::new(),
            inner: ReadHandleInner::Multipart(Box::new(MultipartReader {
                runtime,
                bucket: bucket.as_str().to_string(),
                key: key.as_str().to_string(),
                parts: ranges,
                next_part_index: 0,
                current_part: None,
                sse_customer_request,
            })),
        })
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

pub(super) fn segment_payloads_from_object_segments(
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
            data_pg_id: segment.data_pg_id,
            placement_cluster_epoch: segment.placement_cluster_epoch,
            ec_k: segment.ec_k,
            ec_m: segment.ec_m,
            encryption: encryption.clone(),
        })
        .collect()
}

pub(super) fn snapshotted_multipart_parts_from_storage(
    parts: Vec<ObjectPartRecord>,
    multipart_part_segments: Vec<MultipartPartSegmentRecord>,
    encryption: &ObjectEncryption,
) -> Vec<SnapshottedMultipartPart> {
    use std::collections::BTreeMap;

    let mut segments_by_part: BTreeMap<u32, Vec<MultipartPartSegmentRecord>> = BTreeMap::new();
    for segment in multipart_part_segments {
        segments_by_part
            .entry(segment.part_number)
            .or_default()
            .push(segment);
    }

    let mut snapshotted = Vec::with_capacity(parts.len());
    let mut object_offset_start = 0usize;
    for part in parts {
        let part_size = part.size as usize;
        let segments = segments_by_part
            .remove(&part.part_number)
            .unwrap_or_default()
            .into_iter()
            .map(|segment| SegmentPayloadRecord {
                segment_index: segment.segment_index,
                size: segment.size,
                segment_crc64: segment.segment_crc64,
                segment_okh: segment.segment_okh,
                segment_vid: segment.segment_vid,
                data_pg_id: segment.data_pg_id,
                placement_cluster_epoch: segment.placement_cluster_epoch,
                ec_k: segment.ec_k,
                ec_m: segment.ec_m,
                encryption: encryption.clone(),
            })
            .collect();
        snapshotted.push(SnapshottedMultipartPart {
            record: part,
            object_offset_start,
            segments,
        });
        object_offset_start += part_size;
    }

    snapshotted
}
