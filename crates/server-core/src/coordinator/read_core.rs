use std::sync::Arc;

use storage::{
    BucketName, GenerationId, MultipartPartSegmentRecord, ObjectEncryption, ObjectKey,
    ObjectPartRecord, ObjectPayloadLease, ObjectSegmentRecord, StorageCluster,
};

use super::object_state::SnapshottedMultipartPart;
use super::payload::{PayloadBufferPool, SharedPayloadBuffer};
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
pub(super) struct ReadRuntime {
    pub(super) storage_node: Arc<StorageCluster>,
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
    pub(super) segment_crc64: Option<u64>,
    pub(super) segment_okh: [u8; 16],
    pub(super) segment_vid: GenerationId,
    pub(super) data_pg_id: u32,
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
}

pub(super) struct SegmentListReader {
    pub(super) runtime: ReadRuntime,
    pub(super) bucket: String,
    pub(super) key: String,
    pub(super) segments: Vec<SegmentSliceRecord>,
    pub(super) next_segment_index: usize,
    pub(super) loaded_segment: Option<(Arc<SharedPayloadBuffer>, usize, usize)>,
    pub(super) sse_customer_request: Option<SseCustomerRequest>,
}

#[cfg_attr(not(feature = "deep-tracing"), allow(dead_code))]
#[derive(Debug, Clone)]
pub(super) struct SnapshottedMultipartPartRange {
    pub(super) layout: MultipartPartReadLayout,
    pub(super) segments: Vec<SegmentSliceRecord>,
}

pub(super) struct MultipartReader {
    pub(super) runtime: ReadRuntime,
    pub(super) bucket: String,
    pub(super) key: String,
    pub(super) parts: Vec<SnapshottedMultipartPartRange>,
    pub(super) next_part_index: usize,
    pub(super) current_part: Option<SegmentListReader>,
    pub(super) sse_customer_request: Option<SseCustomerRequest>,
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
        let lease = runtime.acquire_object_payload_lease_for(bucket, key, generation_id)?;
        Ok(Self {
            bucket: bucket_owned.clone(),
            key: key_owned.clone(),
            lease: Some(lease),
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
        let lease = runtime.acquire_object_payload_lease_for(bucket, key, generation_id)?;
        Ok(Self {
            bucket: bucket_owned.clone(),
            key: key_owned.clone(),
            lease: Some(lease),
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
        let lease = runtime.acquire_object_payload_lease_for(bucket, key, generation_id)?;
        Ok(Self {
            bucket: bucket.as_str().to_string(),
            key: key.as_str().to_string(),
            lease: Some(lease),
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
        let lease = runtime.acquire_object_payload_lease_for(bucket, key, generation_id)?;
        Ok(Self {
            bucket: bucket.as_str().to_string(),
            key: key.as_str().to_string(),
            lease: Some(lease),
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
        let segments = if part.part_okh != [0u8; 16] {
            vec![SegmentPayloadRecord {
                segment_index: 0,
                size: part.size,
                segment_crc64: None,
                segment_okh: part.part_okh,
                segment_vid: part.part_vid,
                data_pg_id: part.data_pg_id,
                ec_k: part.ec_k,
                ec_m: part.ec_m,
                encryption: encryption.clone(),
            }]
        } else {
            segments_by_part
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
                    ec_k: segment.ec_k,
                    ec_m: segment.ec_m,
                    encryption: encryption.clone(),
                })
                .collect()
        };
        snapshotted.push(SnapshottedMultipartPart {
            record: part,
            object_offset_start,
            segments,
        });
        object_offset_start += part_size;
    }

    snapshotted
}
