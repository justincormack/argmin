# Segment Buffer Reuse

Status: completed

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
8. degraded `server-core` reads now reuse pooled reconstruction scratch instead
   of allocating one `Vec` per recovered shard and cloning those shards back
   into the assembly path
9. streaming PUT and UploadPart ingress now consume non-decoded Hyper frame
   bytes directly instead of copying each frame into a fresh `Vec<u8>`
10. checksum claims and computed checksum values now use inline fixed-size
    storage instead of heap `Vec<u8>` allocations for 4/8/20/32-byte checksum
    values
11. storage-side multipart and committed-part checksum fields now use inline
    checksum storage too, so `server-core` no longer round-trips those through
    small heap `Vec<u8>` blobs away from the SQLite bind/read edge

## Deferred

1. tiny control-path scratch such as EC helper vectors
   This is not segment-sized payload churn anymore, and it is not on the
   healthy hot path. The expected payoff is allocator-noise reduction and code
   tidying only, so we are not pursuing it in this plan.
2. remaining owner-object churn around `ReadChunk`/`Arc`/`Bytes::from_owner`
   The payload copies and segment-sized allocations are already gone. What
   remains is small ownership machinery needed to hand buffers safely across
   the async HTTP boundary. Removing it would require a more invasive
   abstraction, and the likely upside is marginal unless profiling shows this
   exact path has become hot under small-range/high-QPS workloads.

## Notes

- Segment buffers should be fixed-capacity `INTERNAL_SEGMENT_SIZE` with logical
  `len <= INTERNAL_SEGMENT_SIZE`, not fixed-length `8 MiB`.
- Rare reconstruct or recovery paths may still allocate scratch at first; the
  priority is the healthy data path.
- `CopyObject` and `UploadPartCopy` now benefit automatically from the pooled
  read path because they stream through `ReadHandle`.
- This plan is considered complete because the remaining work is no longer
  about segment-scale payload allocation on the healthy path; it is deferred
  until profiling shows a concrete need.
