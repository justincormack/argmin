// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

#[cfg(test)]
use storage::ObjectPayloadLease;
use storage::{
    ActiveObjectPayloadRead, BucketName, GenerationId, ObjectEncryption, ObjectKey,
    ObjectPayloadFullSegmentRead, ObjectPayloadFullSegmentReadOutcome,
    ObjectPayloadRangeReadOutcome, ObjectPayloadSegment, ObjectReadMultipartPart,
    RetainedObjectPayloadRead, SegmentPayloadCodec, StorageCluster,
};

use super::object_state::SnapshottedMultipartPart;
use super::payload::{PayloadBufferPool, SharedPayloadBuffer};
#[cfg(test)]
use super::test_hooks::maybe_run_object_segments_first_segment_hook;
use super::TRACE_TARGET;
use crate::error::ServerError;
use crate::sse::{
    AuthorizedCobblestoneSegmentStreamRead, SseCustomerRequest, SseCustomerValidatorConfig,
    StaticManagedKeyProvider,
};

#[derive(Debug, Clone)]
pub struct ReadChunk {
    pub(super) data: Arc<SharedPayloadBuffer>,
    pub(super) start: usize,
    pub(super) end: usize,
}

#[derive(Clone)]
pub(super) enum ReadStorage {
    Cluster(Arc<StorageCluster>),
    Active(Arc<ActiveObjectPayloadRead>),
    Retained(Arc<RetainedObjectPayloadRead>),
}

#[derive(Clone)]
pub(super) struct ReadRuntime {
    pub(super) storage: ReadStorage,
    pub(super) payload_buffer_pool: Arc<PayloadBufferPool>,
    pub(super) sse_c_validator: Option<SseCustomerValidatorConfig>,
    pub(super) managed_key_provider: Option<StaticManagedKeyProvider>,
}

#[derive(Debug, Clone)]
pub(super) struct SegmentPayloadRecord {
    pub(super) storage_segment: ObjectPayloadSegment,
    pub(super) encryption: ObjectEncryption,
}

impl SegmentPayloadRecord {
    pub(super) fn segment_index(&self) -> u32 {
        self.storage_segment.segment_index()
    }

    pub(super) fn size(&self) -> u64 {
        self.storage_segment.size()
    }
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
    streaming_segment: Option<StreamingFullSegment>,
    sse_customer_request: Option<SseCustomerRequest>,
    verification: SegmentListVerification,
    verified_size: usize,
    verified_crc64: checksum::crc64::Hasher,
}

struct StreamingFullSegment {
    slice: SegmentSliceRecord,
    physical: Option<ObjectPayloadFullSegmentRead>,
    crypto: AuthorizedCobblestoneSegmentStreamRead,
    plaintext_released: usize,
    terminal_plaintext: Option<Vec<u8>>,
}

/// Bound authenticated plaintext retained after the first response record.
///
/// The first record is still returned immediately for low TTFB. Subsequent
/// records are coalesced only up to the storage read-ahead window, avoiding
/// both per-record response handoffs and the former whole-segment buffer.
pub(super) const AUTHENTICATED_READ_COALESCE_BYTES: usize = 2 * 1024 * 1024;

enum StreamingFullSegmentStep {
    Plaintext(Vec<u8>),
    Complete(Vec<u8>),
    FullSegmentFallbackRequired,
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

#[cfg(test)]
pub(super) struct PayloadLease {
    pub(super) lease: Option<ObjectPayloadLease>,
}

/// Core-owned streaming object body.
pub struct ReadHandle {
    bucket: String,
    key: String,
    inner: ReadHandleInner,
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
            streaming_segment: None,
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
            streaming_segment: None,
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
            streaming_segment: None,
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

    fn record_verified_plaintext(&mut self, plaintext: &[u8]) {
        if matches!(
            self.verification,
            SegmentListVerification::FullPayloadCrc { .. }
        ) {
            self.verified_size += plaintext.len();
            observability::record_payload_read_metrics(observability::PayloadReadMetricDelta {
                crc_bytes: u64::try_from(plaintext.len()).unwrap_or(u64::MAX),
                ..observability::PayloadReadMetricDelta::default()
            });
            self.verified_crc64.update(plaintext);
        }
    }

    fn advance_streaming_segment(
        streaming: &mut StreamingFullSegment,
        target_size: usize,
    ) -> Result<StreamingFullSegmentStep, ServerError> {
        let first_response_record = streaming.plaintext_released == 0;
        let coalesce_limit = target_size.clamp(1, AUTHENTICATED_READ_COALESCE_BYTES);
        let mut authenticated = if first_response_record {
            Vec::new()
        } else {
            Vec::with_capacity(coalesce_limit)
        };
        loop {
            let Some(range) = streaming.crypto.next_ciphertext_range() else {
                return Err(ServerError::InternalError {
                    reason: "Cobblestone stream exhausted without terminal physical validation"
                        .to_string(),
                });
            };
            let mut ciphertext = Vec::new();
            let outcome = streaming
                .physical
                .as_mut()
                .expect("streaming segment retains physical authority until completion")
                .read_next_stored_range_into(range, &mut ciphertext)
                .map_err(super::map_object_read_failure)?;
            if outcome == ObjectPayloadRangeReadOutcome::FullSegmentFallbackRequired {
                return Ok(StreamingFullSegmentStep::FullSegmentFallbackRequired);
            }
            let plaintext = match streaming.crypto.authenticate_next(ciphertext) {
                Ok(plaintext) => plaintext,
                Err(_) => return Ok(StreamingFullSegmentStep::FullSegmentFallbackRequired),
            };
            if streaming.crypto.input_complete() {
                if streaming.crypto.finish().is_err() {
                    return Ok(StreamingFullSegmentStep::FullSegmentFallbackRequired);
                }
                let physical = streaming
                    .physical
                    .take()
                    .expect("streaming segment finishes physical authority exactly once");
                let mut terminal_plaintext =
                    streaming.terminal_plaintext.take().unwrap_or_default();
                terminal_plaintext.extend_from_slice(&plaintext);
                if authenticated.capacity() == 0 {
                    authenticated = terminal_plaintext;
                } else {
                    authenticated.extend_from_slice(&terminal_plaintext);
                }
                return match physical.finish().map_err(super::map_object_read_failure)? {
                    ObjectPayloadFullSegmentReadOutcome::Complete => {
                        Ok(StreamingFullSegmentStep::Complete(authenticated))
                    }
                    ObjectPayloadFullSegmentReadOutcome::FullSegmentFallbackRequired => {
                        Ok(StreamingFullSegmentStep::FullSegmentFallbackRequired)
                    }
                };
            }
            if plaintext.is_empty() {
                continue;
            }
            if streaming.crypto.next_record_is_empty_terminal() {
                debug_assert!(streaming.terminal_plaintext.is_none());
                streaming.terminal_plaintext = Some(plaintext);
                continue;
            }
            streaming
                .physical
                .as_mut()
                .expect("nonterminal stream retains physical authority")
                .enable_bounded_read_ahead_after_first_authenticated_record();
            if authenticated.capacity() == 0 {
                authenticated = plaintext;
            } else {
                authenticated.extend_from_slice(&plaintext);
            }
            if first_response_record || authenticated.len() >= coalesce_limit {
                return Ok(StreamingFullSegmentStep::Plaintext(authenticated));
            }
        }
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
                    observability::record_payload_read_metrics(
                        observability::PayloadReadMetricDelta {
                            logical_bytes: u64::try_from(out.len()).unwrap_or(u64::MAX),
                            ..observability::PayloadReadMetricDelta::default()
                        },
                    );
                    return Ok(Some(out));
                }
                self.loaded_segment = None;
            }

            if let Some(mut streaming) = self.streaming_segment.take() {
                match Self::advance_streaming_segment(&mut streaming, target_size)? {
                    StreamingFullSegmentStep::Plaintext(plaintext) => {
                        streaming.plaintext_released += plaintext.len();
                        self.record_verified_plaintext(&plaintext);
                        let end = plaintext.len();
                        self.loaded_segment = Some((
                            Arc::new(SharedPayloadBuffer::from_unpooled(plaintext)),
                            0,
                            end,
                        ));
                        self.streaming_segment = Some(streaming);
                    }
                    StreamingFullSegmentStep::Complete(plaintext) => {
                        self.record_verified_plaintext(&plaintext);
                        let end = plaintext.len();
                        self.loaded_segment = Some((
                            Arc::new(SharedPayloadBuffer::from_unpooled(plaintext)),
                            0,
                            end,
                        ));
                        #[cfg(test)]
                        if self.next_segment_index == 1 {
                            maybe_run_object_segments_first_segment_hook(&self.bucket, &self.key);
                        }
                    }
                    StreamingFullSegmentStep::FullSegmentFallbackRequired => {
                        let data = self.runtime.read_checked_segment_payload(
                            &streaming.slice.payload,
                            streaming.slice.part_number,
                            self.sse_customer_request.as_ref(),
                        )?;
                        if data.len() != streaming.slice.payload.size() as usize
                            || streaming.plaintext_released > data.len()
                        {
                            return Err(ServerError::InternalError {
                                reason:
                                    "Cobblestone fallback returned inconsistent plaintext geometry"
                                        .to_string(),
                            });
                        }
                        let start = streaming.plaintext_released;
                        self.record_verified_plaintext(&data[start..]);
                        let end = data.len();
                        self.loaded_segment = Some((data, start, end));
                        #[cfg(test)]
                        if self.next_segment_index == 1 {
                            maybe_run_object_segments_first_segment_hook(&self.bucket, &self.key);
                        }
                    }
                }
                continue;
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
                            "bucket={:?} key={:?} part_order={} part_number={} part_object_offset_start={} part_object_offset_len={} part_object_offset_end_exclusive={} segment_index={} segment_size={} segment_object_offset_start={} segment_object_offset_end_exclusive={} read_object_offset_start={} read_object_offset_len={} read_object_offset_end_exclusive={} read_segment_offset_start={} read_segment_offset_len={} read_segment_offset_end_exclusive={}",
                            self.bucket,
                            self.key,
                            part_order,
                            part_number,
                            part_object_offset_start,
                            part_object_offset_end_exclusive - part_object_offset_start,
                            part_object_offset_end_exclusive,
                            slice.segment_index,
                            slice.payload.size(),
                            slice.segment_object_offset_start,
                            slice.segment_object_offset_end_exclusive,
                            read_object_offset_start,
                            read_object_offset_len,
                            read_object_offset_end_exclusive,
                            slice.start_offset,
                            read_segment_offset_len,
                            slice.end_offset,
                        )),
                    );
                } else {
                    let _ = observability::event_in_context(
                        &trace,
                        TRACE_TARGET,
                        "read_segment_layout",
                        Some(format_args!(
                            "bucket={:?} key={:?} segment_index={} segment_size={} segment_object_offset_start={} segment_object_offset_end_exclusive={} read_object_offset_start={} read_object_offset_len={} read_object_offset_end_exclusive={} read_segment_offset_start={} read_segment_offset_len={} read_segment_offset_end_exclusive={}",
                            self.bucket,
                            self.key,
                            slice.segment_index,
                            slice.payload.size(),
                            slice.segment_object_offset_start,
                            slice.segment_object_offset_end_exclusive,
                            read_object_offset_start,
                            read_object_offset_len,
                            read_object_offset_end_exclusive,
                            slice.start_offset,
                            read_segment_offset_len,
                            slice.end_offset,
                        )),
                    );
                }
            }

            let is_partial_segment =
                slice.start_offset != 0 || slice.end_offset != slice.payload.size() as usize;
            if !is_partial_segment
                && slice.payload.encryption.payload_codec()
                    == SegmentPayloadCodec::Cobblestone256SegmentV1
            {
                let (physical, crypto) = self.runtime.begin_checked_segment_payload_stream(
                    &slice.payload,
                    slice.part_number,
                    self.sse_customer_request.as_ref(),
                )?;
                self.next_segment_index += 1;
                self.streaming_segment = Some(StreamingFullSegment {
                    slice,
                    physical: Some(physical),
                    crypto,
                    plaintext_released: 0,
                    terminal_plaintext: None,
                });
                continue;
            }
            let (data, loaded_start, loaded_end) = if is_partial_segment {
                self.runtime.read_checked_segment_payload_range(
                    &slice.payload,
                    slice.part_number,
                    self.sse_customer_request.as_ref(),
                    slice.start_offset..slice.end_offset,
                )?
            } else {
                let data = self.runtime.read_checked_segment_payload(
                    &slice.payload,
                    slice.part_number,
                    self.sse_customer_request.as_ref(),
                )?;
                (data, slice.start_offset, slice.end_offset)
            };
            self.record_verified_plaintext(data.as_ref());
            self.next_segment_index += 1;
            self.loaded_segment = Some((data, loaded_start, loaded_end));
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
            let segment_end = offset + payload.size() as usize;
            if offset > end {
                break;
            }
            if payload.size() != 0 && segment_end > start {
                let start_offset = start.saturating_sub(offset);
                let end_offset = (end + 1)
                    .saturating_sub(offset)
                    .min(payload.size() as usize);
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
            let part_size = part.part.size() as usize;
            let part_end = part_start + part_size;
            if part_start > end {
                break;
            }
            if part.part.size() != 0 && part_end > start {
                let start_offset = start.saturating_sub(part_start);
                let end_offset = (end + 1)
                    .saturating_sub(part_start)
                    .min(part.part.size() as usize);
                let layout = MultipartPartReadLayout {
                    part_number: part.part.part_number(),
                    part_order,
                    object_offset_start: part_start,
                    object_offset_end_exclusive: part_end,
                    part_size,
                    payload_crc64: part.part.payload_crc64(),
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
        let runtime =
            runtime.prepare_object_payload_read(bucket, key, generation_id, segments.iter())?;
        let segments =
            Self::segment_slices_for_range(segments, 0, expected_size.saturating_sub(1), 0, None);
        Ok(Self {
            bucket: bucket_owned.clone(),
            key: key_owned.clone(),
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
        let runtime =
            runtime.prepare_object_payload_read(bucket, key, generation_id, segments.iter())?;
        let segments = Self::segment_slices_for_range(segments, start, end, 0, None);
        Ok(Self {
            bucket: bucket_owned.clone(),
            key: key_owned.clone(),
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
        let runtime = runtime.prepare_object_payload_read(
            bucket,
            key,
            generation_id,
            parts.iter().flat_map(|part| part.segments.iter()),
        )?;
        let ranges =
            Self::multipart_ranges_for_range(parts, 0, expected_size.saturating_sub(1), true);
        Ok(Self {
            bucket: bucket.as_str().to_string(),
            key: key.as_str().to_string(),
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
        let runtime = runtime.prepare_object_payload_read(
            bucket,
            key,
            generation_id,
            parts.iter().flat_map(|part| part.segments.iter()),
        )?;
        let ranges = Self::multipart_ranges_for_range(parts, start, end, false);
        Ok(Self {
            bucket: bucket.as_str().to_string(),
            key: key.as_str().to_string(),
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
    segments: Vec<ObjectPayloadSegment>,
    encryption: ObjectEncryption,
) -> Vec<SegmentPayloadRecord> {
    segments
        .into_iter()
        .map(|segment| SegmentPayloadRecord {
            storage_segment: segment,
            encryption: encryption.clone(),
        })
        .collect()
}

pub(super) fn snapshotted_multipart_parts_from_storage(
    parts: Vec<ObjectReadMultipartPart>,
    multipart_part_segments: Vec<ObjectPayloadSegment>,
    encryption: &ObjectEncryption,
) -> Result<Vec<SnapshottedMultipartPart>, ServerError> {
    use std::collections::BTreeMap;

    let mut segments_by_part: BTreeMap<u32, Vec<ObjectPayloadSegment>> = BTreeMap::new();
    for segment in multipart_part_segments {
        let part_number = segment
            .part_number()
            .ok_or_else(|| ServerError::InternalError {
                reason: "multipart snapshot contains a standard-object payload segment".to_string(),
            })?;
        segments_by_part
            .entry(part_number)
            .or_default()
            .push(segment);
    }

    let mut snapshotted = Vec::with_capacity(parts.len());
    let mut object_offset_start = 0usize;
    for part in parts {
        let part_size = part.size() as usize;
        let segments = segments_by_part
            .remove(&part.part_number())
            .unwrap_or_default()
            .into_iter()
            .map(|segment| SegmentPayloadRecord {
                storage_segment: segment,
                encryption: encryption.clone(),
            })
            .collect();
        snapshotted.push(SnapshottedMultipartPart {
            part,
            object_offset_start,
            segments,
        });
        object_offset_start += part_size;
    }

    if !segments_by_part.is_empty() {
        return Err(ServerError::InternalError {
            reason: "multipart snapshot contains segments without a corresponding part".to_string(),
        });
    }

    Ok(snapshotted)
}
