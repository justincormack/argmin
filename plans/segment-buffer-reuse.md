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

## Planned steps

1. healthy read path: read shard files directly into caller-provided segment
   buffers instead of allocating one `Vec` per shard
2. write path: avoid cloning aligned segment payloads into `padded`; only
   allocate a padded copy when the final segment length is not divisible by `k`
3. response path: replace `ReadHandle::next_chunk() -> Vec<u8>` with an owned
   chunk type that can carry a reusable segment buffer without copying
4. ingress path: remove `drain(..).collect()` segment flush copies in
   `server-http` write buffering
5. pooling: introduce a small reusable segment-buffer abstraction once the read
   and write APIs can hand ownership across layers cleanly

## Notes

- Segment buffers should be fixed-capacity `INTERNAL_SEGMENT_SIZE` with logical
  `len <= INTERNAL_SEGMENT_SIZE`, not fixed-length `8 MiB`.
- Rare reconstruct or recovery paths may still allocate scratch at first; the
  priority is the healthy data path.
- `CopyObject` and `UploadPartCopy` should naturally benefit once reads return
  reusable owned chunks and writes can consume them directly.
