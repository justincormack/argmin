# PUT Write Path Investigation

Status: active

## Goal

Understand why local `2 GiB` multipart PUT is materially slower than `2 GiB`
GET, and remove avoidable write-path coordination costs without changing S3
behavior.

## Current Findings

- The traced `GET` path is healthy: range setup is sub-millisecond and the main
  cost is shard reads.
- The traced multipart `PUT` path is not dominated by EC encode or shard IO.
  `ErasureCodec::encode` is sub-millisecond and the six `write_shard` calls are
  only a small part of each part upload.
- `append_stream_segment` currently holds the metadata PG across session
  validation, encode, and shard writes.
- `finalize_stream_part` then contends on the same metadata PG, so concurrent
  part uploads serialize more than they should.
- The HTTP streaming ingest path still has a sizeable untraced gap between
  request start, segment append, and finalize, so ingress timing needs to be
  broken out before changing the coordinator.
- The ingress-side trace points are now in place for streaming `PutObject` and
  `UploadPart`.
- `append_stream_segment` now validates under the metadata PG, drops it for
  cross-PG shard writes, and revalidates before publishing the staging row.
  The same-PG case is intentionally unchanged because there is no separate PG
  lock to release.

## Steps

1. Done: add ingress-side tracing for streaming `PutObject` and `UploadPart`
   so the next traces show body-read start, segment-ready points, body-read
   completion, and finalize handoff.
2. Done: reduce `append_stream_segment` metadata PG hold time so it does not
   keep the metadata PG locked across cross-PG encode and shard writes.
3. Rerun the same local PUT trace and reassess whether `finalize_stream_part`
   still contends materially on the metadata PG.
4. If needed, tighten the finalize path after the append critical section is
   reduced.

## Notes

- The first optimization target is write-path coordination, not EC math.
- Any locking change must preserve the current correctness invariants for
  streamed session validation and staged segment metadata.
- The reduced lock scope relies on current request structure: a given stream
  session appends segments sequentially within one request, and clients do not
  have direct access to session IDs.
