## Streaming Transfers Plan

### Decision

Adopt **option 3**: internal chunk-manifest layout for normal `PutObject`
writes, with bounded-memory request/response streaming.

This keeps the existing PG serialization model intact while avoiding long PG
lock windows for slow clients.

### Compatibility assumption

We do **not** need to support existing on-disk object data. This plan therefore
does not include legacy data-layout compatibility or migration work.
`InlineLegacy` is explicitly retired and removed as a supported data layout.

### Why we need this

Current server behavior buffers full request bodies and full object responses in
memory, and hard-limits single-object writes to 256 MiB. That is incompatible
with S3-size targets (5 GiB single PUT / multipart part).

### Goals

1. Support single PUT and multipart part payloads up to 5 GiB without full
   in-memory buffering.
2. Preserve current concurrency invariants: per-PG serialized correctness and
   deterministic lock ordering.
3. Avoid long PG occupancy due to slow network clients.
4. Keep bounded memory under backpressure.
5. Keep behavior AWS-compatible at API level.

### Non-goals

1. Replacing the EC engine with a streaming EC API (we stripe in the caller).
2. Redesigning object semantics (versioning, conditional headers, delete
   markers).
3. Exposing internal chunk layout as user-visible multipart state.
4. Backward-compatible reads of pre-streaming object data formats.

## Architecture

### Core idea

For large/non-buffered writes, treat a normal `PutObject` as an internal
multi-chunk object:

1. Ingest request body incrementally in frontend buffers.
2. Write chunks as independent shard sets via short coordinator calls.
3. Atomically publish final object metadata + chunk manifest at finalize.

This is similar to multipart internals, but not exposed as multipart API.

### Small-object policy

1. Initial implementation uses the same chunk-manifest write path for all object
   sizes; there is no small-object bypass threshold.
2. Objects smaller than chunk payload size are represented as a single committed
   chunk.
3. This keeps one correctness path; any small-object fast path is a later
   optimization, not part of this plan.

### ETag and checksum semantics

1. Streaming transfer does not change externally visible ETag behavior.
2. ETags are produced and formatted exactly as they are today for each operation.
3. Checksum algorithms, validation rules, and response/header/XML behavior remain
   exactly as current behavior.
4. Internal chunk metadata may store per-chunk integrity fields, but object-level
   checksum semantics are unchanged.

### Data layout changes

Use exactly these object data layouts in `DataLayout`:

1. `ChunkManifestInternal = 0` (normal object layout; replaces
   `InlineLegacy` at discriminant 0)
2. `MultipartManifest = 1` (explicit S3 multipart)

There is no `InlineLegacy` variant after Phase 1.

Store user metadata in object metadata row for `ChunkManifestInternal` (same
strategy as `MultipartManifest`) to keep `HeadObject` cheap.

### New internal metadata

Add internal staging tables (names illustrative):

1. `stream_uploads`
2. `stream_upload_chunks`
3. `stream_object_chunks` (committed internal chunk manifest rows)
4. `multipart_part_chunks` (committed chunk manifest rows for `UploadPart`)

`stream_uploads` tracks one in-progress streaming write session for both
`PutObject` and `UploadPart` (operation kind on session row).
`stream_upload_chunks` stores staging chunk records (size, etag/checksum, shard
location, EC params, sequence index).

Finalize semantics are explicit and transactional:

1. `PutObject` finalize inserts committed chunk rows into
   `stream_object_chunks`, commits object metadata, and removes
   `stream_uploads`/`stream_upload_chunks` staging rows in the same transaction.
2. `UploadPart` finalize inserts committed chunk rows into
   `multipart_part_chunks`, upserts multipart part metadata, and removes
   `stream_uploads`/`stream_upload_chunks` staging rows in the same transaction.
3. If finalize does not commit, staging rows remain and are cleaned by explicit
   abort path or startup scavenger.

### Placement policy

1. Internal streamed chunk shard sets use the normal shard placement algorithm
   (same model as regular object writes and multipart part writes).
2. Chunks for one logical object are not pinned to one disk; different chunk
   shard sets may land on different PGs/disks.
3. `stream_upload_chunks`, `stream_object_chunks`, and `multipart_part_chunks`
   store placement references (PG/shard location metadata), not co-located
   payload bytes.
4. Session metadata placement is explicit: `stream_uploads` and
   `stream_upload_chunks` live on a session PG selected by placement hash of
   `session_id` (not an ad-hoc in-memory map).

## Concurrency model

### Invariants to preserve

1. Single-object multi-PG operations still lock in global ascending PG ID order.
2. Session state transitions are serialized by session-PG transactions under the
   session PG lock (existing PG mutex model, not a separate correctness mutex).
3. Metadata decisions for finalize happen under metadata PG lock.
4. We never hold a PG lock while waiting for network input from client.
5. Object visibility remains atomic: object is invisible until finalize commit.

### Lock window design

1. **Chunk write**: short lock window.
   - Lock session PG + all shard PGs for that chunk in global ascending order.
   - Do not lock metadata PG during append.
   - Validate session state.
   - Encode+write shard set for one chunk.
   - Upsert chunk row.
   - Unlock.
2. **Finalize**: short critical section.
   - Lock session PG + metadata PG (plus any required secondary PG by existing
     ordering rules), in global ascending PG order.
   - Re-check write preconditions.
   - Allocate version id.
   - Atomically commit object row + chunk manifest pointer rows.
   - Mark session complete and remove staging rows in the same transaction.
   - Unlock.

No long-running lock spans request transfer time.

## Request/response streaming design

### PUT ingest

Refactor HTTP handling for PUT-like streaming operations so body is not collected
up front:

1. Parse headers/path/query.
2. Authenticate/authorize.
3. Route.
4. For streaming-capable write operations, consume `Incoming` body frame-by-frame
   and feed coordinator chunk appends.

Buffered request handling is kept only for control-plane/small-body APIs
(`CompleteMultipartUpload`, `DeleteObjects`, `PutBucket*`, POST policy form,
etc.). Object-data write APIs (`PutObject`, `UploadPart`) must be fully covered
by Phase 3a/3b below.

### UploadPart path decision

1. `UploadPart` uses the same streaming session/append/finalize engine as
   `PutObject` (operation kind distinguishes behavior).
2. There is no separate buffered-only `UploadPart` implementation path.
3. `UploadPart` finalize targets multipart part metadata/chunk tables; it does
   not write `stream_object_chunks`.

### Write-body mode coverage checklist

Phase 3 implementation must explicitly cover these request-body modes for object
data writes:

1. Non-chunked HTTP body (`Content-Length`) for `PutObject`.
2. Non-chunked HTTP body (`Content-Length`) for `UploadPart`.
3. aws-chunked signed payload:
   `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`.
4. aws-chunked signed payload + trailer:
   `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`.
5. aws-chunked unsigned trailer:
   `STREAMING-UNSIGNED-PAYLOAD-TRAILER`.

Out of scope and intentionally rejected:

1. `STREAMING-UNSIGNED-PAYLOAD` (AWS rejects this token).

### Response body type prerequisite

1. Before Phase 4, migrate HTTP response body type from `Response<Full<Bytes>>`
   to a stream-capable body type across handler plumbing.
2. Small metadata/control responses can still use one-shot bodies wrapped in the
   same response type.
3. This is a prerequisite for true streaming GET/Range responses.

### GET/Range response

Introduce streaming response bodies for object reads:

1. Build response headers from metadata first.
2. Stream bytes chunk-by-chunk from manifest source.
3. Preserve range semantics and conditional semantics.

This is required before enabling >256 MiB objects generally.

### CopyObject

Replace full-object materialization in copy with bounded-memory transfer:

1. Read source in chunks (or chunk-manifest pass-through read).
2. Feed destination internal streaming session.
3. Finalize destination atomically.

No full source object in RAM.

## Chunk sizing and memory policy

### Recommended defaults

1. Network ingest buffer: 64 KiB minimum (can be larger adaptively).
2. Default internal write chunk payload: 4 MiB (configurable).
3. EC chunk payload target: `k * stripe_size`.
4. Initial `stripe_size`: 1 MiB per data shard (tunable).

Example for `k=4`: chunk payload 4 MiB, parity computed for one 4 MiB stripe.

### Memory controls

1. Global byte-budget semaphore for buffered in-flight chunk bytes.
2. Per-request chunk buffer cap.
3. Existing request admission semaphore remains.
4. Idle timeout remains for stalled clients.

Result: bounded memory independent of object size.

## Detailed phase plan

### Phase 0: Design freeze and invariants

1. Add this decision and invariants to guide docs (`guides/object-concurrency.md`
   and related docs if needed), including streaming lock patterns and PR
   checklist updates for streaming code paths.
2. Define exact table schemas and finalize naming.
3. Document explicit rollout precondition: fresh data dir / no legacy object
   format support.

**Exit criteria**
1. Schema and lifecycle state machine approved.
2. Concurrency invariants documented.

### Phase 1: Storage/schema foundation

1. Replace `DataLayout::InlineLegacy` with
   `DataLayout::ChunkManifestInternal` at discriminant `0`; keep
   `DataLayout::MultipartManifest = 1`.
2. Add staging + committed chunk manifest tables (`stream_uploads`,
   `stream_upload_chunks`, `stream_object_chunks`, `multipart_part_chunks`).
3. Add storage trait methods for:
   - create/get/update/abort session
   - append/list chunk rows
   - atomic finalize commit
4. Add schema tests and round-trip tests.

**Exit criteria**
1. Storage-layer tests pass.
2. Atomic finalize transaction semantics proven by tests.

### Phase 2: Coordinator streaming session API

Add coordinator methods:

1. `begin_stream_put(...) -> session_id`
2. `append_stream_chunk(session_id, bytes, chunk_index, checksum_ctx...)`
3. `finalize_stream_put(session_id) -> PutObjectResult`
4. `abort_stream_put(session_id)`

Include:

1. Session state machine (`InProgress`, `Completing`, `Completed`, `Aborted`).
2. Preconditions checked at finalize under lock.
3. Best-effort + deterministic cleanup paths.

**Exit criteria**
1. Unit tests for happy path + all state errors.
2. Fault tests for partial shard writes and finalize failures.

### Phase 3a: HTTP streaming integration (non-chunked)

1. Introduce streamed PUT execution path in serve/http layer.
2. Stop collecting full body for `PutObject` and `UploadPart`.
3. Feed decoded payload chunks into coordinator append API.
4. Finalize and return response headers/etag/version.

**Exit criteria**
1. `PutObject` and `UploadPart` pass with non-chunked request bodies.
2. Existing object-write tests remain green.
3. New large-object integration tests pass for non-chunked mode.
4. Memory usage remains bounded under slow client tests.

### Phase 3b: HTTP streaming integration (aws-chunked incremental decode)

1. Replace full-buffer aws-chunked decode path with incremental decode.
2. Feed decoded payload bytes directly into coordinator chunk appends.
3. Preserve existing signature/trailer/checksum validation semantics while
   decoding incrementally.
4. Route all supported aws-chunked object-data writes through streaming path.

**Exit criteria**
1. `PutObject` and `UploadPart` pass for all supported aws-chunked tokens:
   - `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`
   - `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`
   - `STREAMING-UNSIGNED-PAYLOAD-TRAILER`
2. aws-chunked negative-case tests remain green with exact AWS parity codes.
3. No full request-body buffering remains for object-data writes.

### Phase 4: Read-path streaming and `CopyObject` bounded transfer

1. Introduce streaming GET/Range response path.
2. Add chunk-manifest readers for `ChunkManifestInternal`.
3. Rework `CopyObject` to bounded chunk transfer.
4. Keep conditional/read integrity checks unchanged semantically.

**Exit criteria**
1. GET/HEAD/Range tests pass for large streamed objects.
2. Copy tests pass without full-object buffering.

### Phase 5: Delete/overwrite/cleanup hardening

1. Ensure delete and overwrite paths clean chunk-manifest shards correctly.
2. Ensure no race can delete newly committed objects during stale cleanup.
3. Add startup scavenger for abandoned sessions.

**Exit criteria**
1. Concurrency stress tests pass.
2. Crash-restart cleanup tests pass.
3. Storage-level chunk-manifest readback/integrity checks pass without requiring
   HTTP streaming read path (full HTTP read assertions remain in Phase 4+).

### Phase 6: Limit lift and rollout

1. Lift single PUT/part limits to 5 GiB where AWS-compatible.
2. Add metrics and operational docs.

**Exit criteria**
1. Integration suites pass locally and against AWS where applicable.
2. Performance and memory budgets are within target.

## Test strategy

### Correctness tests

1. End-to-end streamed PUT/GET/HEAD/Range/Copy/Delete.
2. Conditional write semantics at finalize race points.
3. Versioned and unversioned overwrite semantics.
4. Multipart interoperability with streamed single PUT objects.
5. Fresh-cluster bootstrap tests (no legacy-format assumptions).

### Concurrency tests

1. Slow upload on key A must not stall unrelated key B on same PG longer than
   one chunk write window.
2. Concurrent overwrite/delete/read races on chunk-manifest objects.
3. Atomic visibility: object not readable before finalize.

### Fault tests

1. Shards written, metadata append fails.
2. Session append succeeds, process crashes before finalize.
3. Finalize partial failure and retry behavior.
4. Startup scavenger correctness.

### Resource tests

1. Peak RSS bounded under N concurrent 5 GiB uploads.
2. Backpressure behavior with saturated byte budget.
3. No unbounded queue growth.

## Open decisions

1. Whether `UploadPartCopy` should reuse the same internal chunk append path
   directly, or keep an operation-specific wrapper over it.
2. This is not a Phase 1 schema blocker: both options persist committed part
   chunks via `multipart_part_chunks`; the decision changes coordinator/control
   flow, not on-disk committed part-chunk format.

## Recommended implementation order

1. Phase 1 (schema + traits)
2. Phase 2 (coordinator session lifecycle)
3. Phase 3a (non-chunked streaming ingest)
4. Phase 3b (aws-chunked incremental streaming ingest)
5. Phase 5 cleanup hardening (before enabling larger limits)
6. Phase 4 read/copy streaming
7. Phase 6 limit lift and rollout

This order keeps correctness and crash safety ahead of throughput tuning.
Phase 5 validation uses storage-level manifest/shard integrity checks; full
HTTP read-path validation is completed in Phase 4.
