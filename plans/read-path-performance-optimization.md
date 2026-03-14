# Segment Integrity Checksums For Bounded Range Reads

## Context

The earlier broad read-path optimization plan no longer matches current reality.
Fresh measurements on current segmented data show:

- `2 GiB` GETs are back near the original expected throughput
- `20 GiB` GETs are also strong, around `25.49s` locally
- the severe read regression that originally motivated the plan no longer reproduces on fresh current data

That means the immediate read-side gap is narrower:

- we want explicit logical-segment integrity metadata in SQLite
- we want segmented reads and small ranges to validate at the segment boundary
- we do not currently need a broader storage/read-path rewrite for throughput

This plan replaces the older broad optimization scope with a focused integrity step.

## Goal

Persist a CRC64 for every committed and staged logical segment, and verify it on segmented reads.

This gives us:

- an integrity boundary aligned with the current fixed `4 MiB` internal segment model
- bounded validation for small range requests
- a clean foundation if we later introduce more selective physical shard reads

## Non-Goals

This work does not attempt to:

- redesign the current segmented read pipeline
- remove `fs::read()` from `read_shard`
- optimize large-read throughput further right now
- replace the existing shard-level CRC64 checks

Shard CRC64 remains in place. Segment CRC64 is an additional logical integrity layer.

## Design

### Integrity unit

- one CRC64-NVME per logical segment
- checksum is computed over the logical segment bytes before EC encoding
- segment size remains the fixed internal segment size (`4 MiB` today)

### Storage placement

Persist `segment_crc64` in SQLite on:

- `stream_upload_segments`
- `object_segments`
- `multipart_part_segments`

This is sufficient for the current segmented read path.

Reclaim metadata does not need segment checksums because reclaim does not serve reads.

### Read verification

On segmented reads:

1. read and reconstruct the touched logical segment
2. compute CRC64 over the reconstructed logical segment bytes
3. compare with the stored `segment_crc64`
4. only then slice and emit the requested subrange

This means a small range request still reads and validates each touched logical segment in full, but it no longer depends on whole-object or whole-part integrity state.

### Backward compatibility

There are no hard backward-compatibility guarantees for old experimental layouts, but the schema change should still be tolerant of preexisting local dev data where reasonable.

Practical approach:

- add nullable `segment_crc64` columns via migration
- require new writes to populate them
- verify on read only when present

That gives us the feature immediately without turning old local data into an unreadable trap.

## Implementation Steps

### Phase 1: Schema and types

1. add nullable `segment_crc64` columns to:
   - `stream_upload_segments`
   - `object_segments`
   - `multipart_part_segments`
2. thread the field through:
   - `StreamUploadSegmentRecord`
   - `ObjectSegmentRecord`
   - `MultipartPartSegmentRecord`
   - `SegmentPayloadRecord`
3. add migration logic for existing SQLite databases

### Phase 2: Write paths

Populate `segment_crc64` on all current segment-producing paths:

- buffered `PutObject`
- buffered `CopyObject` destination writes
- streaming `PutObject`
- buffered `UploadPart`
- streaming `UploadPart`

### Phase 3: Read paths

Verify `segment_crc64` on:

- `ReadRuntime::read_segment_payload(...)`
- therefore all current segmented `GET`, `Range`, `Part`, and copy-source reads

For rows that do not yet have `segment_crc64`, skip logical-segment verification and continue relying on shard CRC64.

### Phase 4: Tests

Add/adjust tests for:

- segment metadata round-trip in storage
- buffered object writes populate segment CRCs
- streaming writes populate segment CRCs
- multipart part writes populate segment CRCs
- read fails if segment bytes reconstruct but do not match stored segment CRC
- small range reads still succeed and verify touched segments

## Completion Criteria

This plan is complete when:

- every new segment row stores `segment_crc64`
- all current segmented reads validate it when present
- old rows without the field remain readable
- tests cover both write population and read verification
