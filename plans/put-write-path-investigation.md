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
- `append_stream_segment` now validates under the metadata PG, writes shard
  files durably without any PG lock, then briefly republishes shard metadata
  and the staging segment row under ordered PG locks.
- The remaining HTTP-side delay is not `spawn_blocking` queueing. The new trace
  shows most of the residual PUT cost waiting in `acquire_frontend`, so the
  frontend pool mutex is now the dominant gate on `UploadPart` append/finalize.
- After removing the frontend mutex, the next bottleneck is shard-PG lock hold
  time in `PgStore::write_shard`: durable file IO and SQLite publication still
  happen together under the shard PG mutex.
- The shard durable write path is now split from metadata visibility, and shard
  row publication is batched into one SQLite transaction per segment. When the
  shard PG and metadata PG are the same, shard rows plus the stream-segment row
  publish in one transaction.

## Steps

1. Done: add ingress-side tracing for streaming `PutObject` and `UploadPart`
   so the next traces show body-read start, segment-ready points, body-read
   completion, and finalize handoff.
2. Done: reduce `append_stream_segment` metadata PG hold time so it does not
   keep the metadata PG locked across cross-PG encode and shard writes.
3. Done: rerun the local PUT trace and confirm the next bottleneck is the
   frontend pool mutex, not coordinator lock scope or `spawn_blocking`.
4. Done: remove frontend pool mutex contention for streaming append/finalize
   and rerun the same local PUT trace.
5. Done: split shard durable file IO from shard metadata publication so the
   shard PG mutex only covers SQLite visibility, not file write and fsync.
6. In progress: rerun the local PUT trace and confirm the remaining cost is the
   actual durable shard write path, not shard metadata publication or HTTP
   coordination.

## Notes

- The first optimization target is write-path coordination, not EC math.
- Any locking change must preserve the current correctness invariants for
  streamed session validation and staged segment metadata.
- The reduced lock scope relies on current request structure: a given stream
  session appends segments sequentially within one request, and clients do not
  have direct access to session IDs.
- `prepare_streaming_put` / `prepare_streaming_part` still need the full
  frontend for auth and request-header handling. The goal here is narrower:
  stop serializing steady-state append/finalize on the frontend pool.
