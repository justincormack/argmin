# Segment Buffer Reuse

Status: active

## Goal

Reduce hot-path heap churn around `8 MiB` internal segments without changing S3
behavior.

The intended end state is:

- healthy object-data reads and writes do not allocate proportional to the
  number of shards touched
- segment payload buffers are reused where practical
- `server-core` stops owning avoidable payload scratch allocations on the
  healthy path

This follows the guidance in
[guides/engineering_lessons.md](/home/justin/src/github.com/justincormack/argmin/guides/engineering_lessons.md),
especially "callers own all buffers; the library owns nothing".

## Scope

This is only about internal segment payload movement. It does not change:

- S3 request or response semantics
- on-disk layout
- EC parameters
- tracing behavior except where extra detail helps validate the work

## Completed

1. healthy read path now reads shard files directly into the assembled segment
   buffer instead of allocating one `Vec` per shard
2. aligned write segments no longer clone into `padded`; only short final
   segments allocate a padded copy
3. response reads now use an owned chunk view instead of copying partial
   segment slices into fresh `Vec<u8>` buffers
4. `server-http` streaming ingest no longer uses `drain(..).collect()` to copy
   full `8 MiB` segments before handing them to `server-core`
5. healthy write paths now reuse pooled parity scratch instead of allocating
   new parity buffers for every encoded segment
6. `server-http` streaming ingest now reuses pooled `8 MiB` payload buffers
   instead of allocating a fresh segment buffer for each flushed chunk
7. healthy `server-core` reads now reuse pooled payload buffers for loaded
   segments, so repeated range/object reads and read-driven copy flows do not
   allocate a fresh segment `Vec` on each read

## Remaining

1. decide how far to take scratch reuse on recovery paths:
   - reconstruction buffers on fallback reads
   - any checksum or small control-path scratch that is still per-segment
2. measure whether the remaining small owner-object churn
   (`Arc`/`Bytes::from_owner`) is worth another abstraction layer, or whether
   the current pooled payload buffers are sufficient for the healthy path

## Notes

- Segment buffers should be fixed-capacity `INTERNAL_SEGMENT_SIZE` with logical
  `len <= INTERNAL_SEGMENT_SIZE`, not fixed-length `8 MiB`.
- Rare reconstruct or recovery paths may still allocate scratch at first; the
  priority is the healthy data path.
- `CopyObject` and `UploadPartCopy` now benefit automatically from the pooled
  read path because they stream through `ReadHandle`.
