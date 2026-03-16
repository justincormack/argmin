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

## Remaining

1. introduce a reusable segment-buffer abstraction for the healthy path so
   segment-sized buffers can be recycled instead of freshly allocated
2. thread that buffer ownership across read, copy, and write boundaries so
   `server-core` can hand off payload buffers instead of repeatedly creating
   new `Vec`/`Arc<Vec>` owners
3. decide how far to take scratch reuse on recovery paths:
   - parity buffers during normal encode
   - reconstruction buffers on fallback reads
   - any checksum or small control-path scratch that is still per-segment

## Notes

- Segment buffers should be fixed-capacity `INTERNAL_SEGMENT_SIZE` with logical
  `len <= INTERNAL_SEGMENT_SIZE`, not fixed-length `8 MiB`.
- Rare reconstruct or recovery paths may still allocate scratch at first; the
  priority is the healthy data path.
- `CopyObject` and `UploadPartCopy` should naturally benefit once reads return
  reusable owned chunks and writes can consume them directly.
