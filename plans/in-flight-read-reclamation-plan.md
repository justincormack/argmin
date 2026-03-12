# In-flight Read / Reclamation Plan

## Context

This issue was discovered while moving authorization into `server-core`, but it
is not primarily an auth problem and should be tracked separately.

The key observation is:

1. authorizing a read at operation start is fine
2. the real risk is that an in-flight read may lose access to backing data if a
   concurrent delete or overwrite reclaims shards part way through the read

This plan is about object-lifecycle and data-availability semantics, not about
the HTTP/core/storage boundary.

## Problem Statement

Some read paths snapshot object metadata, release metadata guards, and then
perform a sequence of later shard or chunk reads.

At the same time, delete and overwrite paths remove metadata visibility and
then immediately reclaim shard data for the old generation.

That creates a race:

1. read starts against object generation N
2. read successfully consumes early parts or chunks
3. delete or overwrite removes metadata and reclaims remaining shard sets for N
4. later steps of the read fail even though the operation had already started

There is also a separate but related implementation problem:

1. current `GET` paths materialize the full response body in memory inside
   `Coordinator`
2. that is not acceptable for full-object `GET`, which can be up to 50 TB
3. the final reclamation design needs to match the eventual streaming read
   lifetime, not the current `Vec<u8>` lifetime

## Affected Surface

The risk is clearest for read paths that do not consume all needed shard data
under a single metadata snapshot:

1. `GetObject` for multipart-manifest objects
2. `GetObject` for chunk-manifest objects
3. `GetObjectRange` for those same layouts
4. `GetObjectPart` / `HeadObjectPart` where the source part is backed by
   streaming chunk manifests
5. copy-source reads that reuse the same internal data-reconstruction helpers

Simple `HEAD` is not affected because it does not perform later shard reads.

Simple single-generation non-multipart reads are less exposed because they do
not walk a sequence of later part or chunk manifests after releasing the
initial metadata snapshot.

## Broader Scope Than Delete

This is not only `delete_object`.

The same immediate-reclamation pattern exists in:

1. unversioned overwrite paths that delete prior-generation shards after
   committing a new generation
2. stream-put finalize paths that clean stale chunk manifests and shards
3. multipart completion paths that replace prior generations and then reclaim
   the old shard data
4. delete paths for both current-object delete and explicit-version delete

So the right framing is:

1. in-flight reads
2. versus immediate reclamation of old generations

## Current Serialization Points

The relevant serialization points are:

1. per-PG mutexes in `SharedStorageNode`
2. the bucket stripe lock for some write paths

These are not sufficient to protect long reads because:

1. read paths do not hold all relevant metadata and shard PG guards for the
   full duration of a multipart or chunk-manifest read
2. bucket locking is orthogonal to the lifetime of underlying shard data
3. delete and overwrite paths intentionally drop PG guards before reclaiming
   old shards to avoid deadlock and broad lock retention

So this is not a missing-authz-lock issue. It is a data-lifetime issue.

## Why an AWS ACL Revocation Test Is Not Useful

The previously discussed AWS experiment about revoking public-read mid-transfer
does not answer the important question here.

Once a `GET` has started, the client-facing S3 API does not have a meaningful
surface for a late authorization failure part way through the response. So
authorize-at-operation-start semantics are acceptable.

The issue we need to solve is not "should auth be rechecked mid-stream?".
It is "should already-started reads keep their backing data available long
enough to finish?".

## Current Failure Modes

There are two related races.

### 1. Metadata follow-on race

Some read helpers look up later chunk manifests during the read instead of
fully snapshotting them up front.

If a delete or overwrite removes those metadata rows first, the read can fail
before shard reclamation is even reached.

### 2. Physical shard lifetime race

Even when enough metadata has been snapshotted, delete and overwrite currently
reclaim old shard sets immediately after dropping metadata guards.

So a read that has already decided which old generation it is reading can still
fail on a later shard read.

## Retention Model

The metadata snapshot and the retained payload are different things.

1. read metadata can be copied into memory for the request
2. once copied, object metadata rows can be deleted immediately
3. what must stay alive is the old payload:
   - normal object shard set
   - stream-put chunk list and chunk shard sets
   - multipart part shard sets plus any streamed-part chunk shard sets

The right abstraction is therefore not "keep object metadata alive". It is:

1. in-memory request snapshot of metadata
2. retained payload lease that prevents physical reclamation while a read is
   still using that old generation

This also exposes a structural gap in the current model:

1. external `VersionId` is an S3-facing namespace concept
2. retained payload identity is an internal immutable-generation concept
3. for versioned objects those happen to align today
4. for unversioned overwrite they do not

So the final design should not keep treating visible `VersionId` and physical
payload identity as the same thing.

The desired semantics are like `unlink` on a Unix file:

1. namespace entry disappears immediately
2. already-open handle keeps the underlying payload alive

## Recommendation

Do not solve this by holding bucket locks across large reads.

That is too coarse and ties correctness to a lock that is not really about
underlying shard lifetime.

Instead, solve it in three layers:

1. snapshot all metadata needed for the read before releasing metadata guards
2. make read paths truly streaming so the payload-retention lifetime is the real
   request lifetime, not an intermediate in-memory buffer
3. only then finalize and implement deferred reclamation of old payloads

That gives a cleaner model:

1. metadata visibility changes immediately
2. payload is retained while a read lease exists
3. physical reclamation becomes deferred cleanup after the lease is gone

## Proposed Implementation Phases

### Phase 1: Reproducer tests

Add deterministic coordinator-level tests that force the race.

Target cases:

1. multipart `GetObject` while `DeleteObject` runs after the first part read
2. chunk-manifest `GetObject` while `DeleteObject` runs after the first chunk
3. multipart or chunk-manifest `GetObject` while an unversioned overwrite
   replaces the object and reclaims the old generation
4. copy-source read against an old generation while delete or overwrite reclaims
   it

The point is to prove the current behavior and then lock in the intended fix.

Status:

1. completed for the metadata-follow-on race on multipart-manifest reads
2. covered by deterministic coordinator tests that pause after the read-side
   multipart metadata snapshot and after delete-side metadata removal, but
   before shard reclamation
3. current focused coverage:
   - multipart `GetObject`
   - multipart `GetObjectPart` for a streamed part
   - multipart copy-source via `UploadPartCopy`
   - multipart copy-source via `CopyObject`
4. the remaining physical shard lifetime race is intentionally left for
   deferred reclamation work below

### Phase 2: Snapshot completeness

Change read paths so they collect all metadata needed for the current read
before releasing metadata guards.

This likely means:

1. multipart reads prefetch any streaming-part chunk manifests they may need
2. chunk-manifest reads prefetch the full chunk list they will traverse
3. copy-source helpers reuse the same fully snapshotted internal representation

This fixes the metadata-half of the race.

Status:

1. completed
2. `GetObject`, `GetObjectRange`, `GetObjectPart`, `CopyObject`, and
   `UploadPartCopy` now snapshot streaming-part chunk manifests before dropping
   metadata guards
3. chunk-manifest object reads were already snapshotting their full
   `stream_object_chunks` list before this pass, so no behavior change was
   needed there

### Phase 3: True streaming reads

Refactor read paths so `GET` does not materialize full object bodies in memory
inside `Coordinator`.

This should happen before the deferred-reclamation design is finalized, because
the final payload-retention lifetime needs to match the real response lifetime.

Target shape:

1. `server-core` returns a streaming read handle or iterator-like body source,
   not a full `Vec<u8>`
2. `server-http` turns that into the response body and releases it on normal
   completion or cancellation
3. range and part reads use the same streaming foundation where practical
4. copy-source reads can continue to be internal consumers of the same
   snapshotted payload representation

Important current nuance:

1. today a read lease would only need to live until `Coordinator` finishes
   reconstruction, because the full body is already in memory
2. after this phase, the lease must live until response completion or
   cancellation
3. this is not only a cleanup; it is required for correctness of the external
   interface because full-object `GET` can be vastly larger than memory

Status:

1. completed for the current read and copy-source surfaces
2. `GetObject`, `GetObjectRange`, and `GetObjectPart` now use the core-owned
   `ReadHandle`
3. `server-http` now adapts `ReadHandle` to a streaming Hyper body and holds
   the request permit for the full response lifetime
4. all `GET` body paths now stream through `ReadHandle`, including simple
   single-shard-set objects
5. simple single-shard-set reads now acquire a retained-payload lease tied to
   the `ReadHandle` lifetime, so overwrite/delete cannot reclaim the current
   generation while the body is still being read
6. `CopyObject` and `UploadPartCopy` now reuse the same stepped reader
   foundation for multipart-manifest, chunk-manifest, and simple
   single-shard-set source reads
7. simple single-shard-set copy sources now acquire the same retained-payload
   lease model as `GET`, so source overwrite/delete cannot reclaim the current
   generation while the copy is still draining bytes

Proposed implementation shape:

1. replace `GetObjectResult.data`, `GetObjectRangeResult.data`, and
   `GetObjectPartResult.data` with a core-owned streaming body handle
2. keep that handle in `server-core`; do not return Hyper or HTTP body types
   from core
3. adapt the core handle to the concrete HTTP response body in `server-http`
4. let the handle own the snapshotted payload description, so the same handle
   can later also own the retained-payload lease from Phase 4

Suggested core shape:

1. a `ReadHandle` type exposed by `server-core`
2. explicit internal variants rather than a generic async trait hierarchy:
   - `ShardSetReader`
   - `ChunkManifestReader`
   - `MultipartReader`
3. a bounded stepping API such as `next_chunk(target_size) ->
   Result<Option<Vec<u8>>, ServerError>`
4. existing full-buffer helpers such as multipart and chunk-manifest range reads
   should be refactored underneath this into incremental stepping logic

Suggested HTTP shape:

1. `S3Response` should stop requiring `body: Vec<u8>` for object reads
2. introduce a response body enum so control-plane responses can remain buffered
   while object reads become streaming
3. `server-http` should own the concrete Hyper body adaptation and the lifecycle
   of any request permit needed for the in-flight response

Request admission / lifetime:

1. the request semaphore permit should remain held for the full response-body
   lifetime, not only until headers are produced
2. that preserves the current "in-flight request" admission semantics and avoids
   letting many slow downloads bypass backpressure
3. when the streaming body finishes or is dropped, the permit is released

Integrity behavior:

1. full-object reads currently verify CRC after reconstructing the entire body
2. once reads are streamed, integrity checking must become incremental
3. the streaming reader should maintain rolling integrity state and verify at
   end of stream
4. if the final integrity check fails after some bytes were already sent, the
   body must terminate as a failed stream rather than attempting to send a late
   S3 XML error

Recommended rollout inside Phase 3:

1. introduce the new core `ReadHandle` and HTTP body enum without changing
   reclamation yet
2. convert `GetObject`
3. convert `GetObjectRange`
4. convert `GetObjectPart`
5. move copy-source internal readers onto the same stepping primitives
6. only after this, finalize the retained-payload lease design in Phase 4

### Phase 4: Retained payloads and deferred reclamation

Change delete and overwrite paths so they stop deleting old shard data
synchronously in the foreground operation.

The current write or delete should:

1. remove namespace visibility or install the new current generation
2. persist a reclaim record describing the old payload
3. return success without physically removing those shards inline

Then a later cleanup path can reclaim the data once there are no active leases.

Model:

1. metadata snapshot for the read stays in memory only
2. active read leases are in-memory only
3. reclaim records for old payloads are durable and live in the existing
   SQLite metadata DB

This phase deliberately comes after true streaming reads, because the streaming
body abstraction determines:

1. where read leases are acquired
2. where they are released on normal completion
3. what cancellation or dropped-response cleanup has to do

The in-memory/durable split is intentional:

1. active reads do not survive process crash
2. reclaimable old payloads do survive crash
3. so lease counts do not need durable storage, but reclaim records do

Likely pieces:

1. retained payload descriptor for:
   - single shard set
   - stream chunk list
   - multipart part/chunk payload
2. in-memory active lease table keyed by retained payload or reclaim ID
3. durable reclaim queue/table scanned by a background sweeper

Implementation decision:

1. keep reclaim records in the existing metadata DB
2. represent each reclaimable payload as a durable metadata row
3. make a background sweeper the default reclaim path
4. treat any current foreground reclaim-on-last-lease-drop behavior as
   transitional rather than the intended long-term model

The important property is:

1. foreground reads do not depend on immediate physical reclamation

Bucket-delete interaction:

1. normal payload reclamation should happen in the background
2. `DeleteBucket` should remain synchronous for now
3. `DeleteBucket` should explicitly drain bucket-local reclaim work before
   removing bucket metadata
4. a bucket-lifecycle `Deleting` state is not required unless bucket deletion
   itself later becomes asynchronous

One key design point:

1. versioned objects already have a usable generation identity
2. unversioned overwrites do not
3. so retained payloads for old unversioned incarnations need an internal
   immutable generation ID distinct from the visible `VersionId`

Recommended direction:

1. keep `VersionId` as the external S3-facing concept
2. introduce a separate internal `GenerationId` for physical payload identity
3. allocate a fresh `GenerationId` for every live object write, including
   unversioned overwrites
4. let visible object metadata point at the current live `GenerationId`
5. let deferred reclamation operate on old `GenerationId`s, not on visible
   `(bucket, key, version_id)` names

This avoids a bad compromise where unversioned overwrite keeps reusing the
same physical identity just because the external S3 API exposes the null
version.

Implications:

1. simple single-shard-set streaming reads should ultimately pin a
   `GenerationId`, not "whatever currently lives at null version"
2. overwrite can replace the visible current object immediately while the old
   `GenerationId` remains retained
3. manifest rows, chunk rows, and shard keys should move toward being keyed by
   internal generation identity rather than visible version identity
4. delete markers remain a namespace-level concept; they do not require payload
   retention

Suggested rollout after Phase 3:

1. introduce `GenerationId` as a distinct internal type
2. thread it through storage records and shard/manifests where payload identity
   matters
3. update unversioned write paths to allocate fresh generations instead of
   reusing `VersionId::Null` as the physical identity
4. only then finalize the retained-payload lease and reclaim-record design

Status:

1. partially completed
2. `GenerationId` now exists as a distinct internal type
3. multipart/stream chunk payload records already use `GenerationId`
4. live object rows now also carry `generation_id`, and normal live writes,
   stream finalization, and multipart completion allocate a fresh internal
   generation per write
5. simple single-shard-set shard placement and shard keys now use
   `generation_id` rather than visible `VersionId`
6. simple single-shard-set payloads now have:
   - in-memory active leases in `SharedStorageNode`
   - a durable `simple_payload_reclaims` table in metadata storage
   - a working reclaim path, though the current last-lease-drop cleanup is
     still transitional and should move to the background sweeper model
7. normal `PutObject`, `CopyObject`, unversioned/simple `DeleteObject`,
   version-specific simple deletes, and simple stale payloads displaced by
   stream finalization or multipart completion now enqueue reclaim records
   instead of deleting simple shard sets inline
8. chunk-manifest payloads now also have:
   - a durable `chunk_manifest_reclaims` root table plus child chunk rows
   - a root-generation lease model on chunk-manifest `ReadHandle`s
   - reclaim-on-drop behavior for current delete, version-specific delete, and
     stale chunk-manifest payloads displaced by simple writes
9. multipart-manifest payload reclamation is still on the older immediate or
   leak-prone paths and remains the next design step

### Phase 5: Expand reclamation coverage

After the first delete/read fix, apply the same deferred-reclamation model to
all old-generation cleanup sites:

1. unversioned overwrite in normal `PutObject`
2. stream-put overwrite cleanup
3. multipart-complete old-generation cleanup
4. any other path that eagerly deletes stale chunk or part shard sets

This should end with one consistent old-generation reclamation mechanism, not a
mix of deferred and immediate cleanup.

Chosen design for multipart-manifest and chunk-manifest payloads:

1. use mirrored reclaim tables in the existing metadata DB rather than
   tombstoning live rows or introducing a generic polymorphic reclaim table
2. use the object's `generation_id` as the reclaim-root identity for the old
   object generation
3. store child reclaim rows for the physical payloads that actually need later
   shard deletion
4. keep leases keyed by the reclaim root `(bucket, key, generation_id)`, not by
   individual parts or chunks

Concrete reclaim shapes:

1. chunk-manifest payloads:
   - `chunk_manifest_reclaims`
     - `bucket`
     - `key`
     - `generation_id`
     - `created_at`
   - `chunk_manifest_reclaim_chunks`
     - `bucket`
     - `key`
     - `generation_id`
     - `chunk_index`
     - `chunk_okh`
     - `chunk_vid`
     - `shard_pg_id`
     - `ec_k`
     - `ec_m`
2. multipart-manifest payloads:
   - `multipart_reclaims`
     - `bucket`
     - `key`
     - `generation_id`
     - `created_at`
   - `multipart_reclaim_parts`
     - `bucket`
     - `key`
     - `generation_id`
     - `part_number`
     - `storage_kind` (`ShardSet` or `ChunkManifest`)
     - `part_okh`
     - `part_vid`
     - `shard_pg_id`
     - `ec_k`
     - `ec_m`
   - `multipart_reclaim_part_chunks`
     - `bucket`
     - `key`
     - `generation_id`
     - `part_number`
     - `chunk_index`
     - `chunk_okh`
     - `chunk_vid`
     - `shard_pg_id`
     - `ec_k`
     - `ec_m`

Important design notes:

1. reclaim rows should be more explicit than the current live multipart
   manifest rows; in particular, reclaim rows should not reuse the live-table
   "all-zero `part_okh` means streamed part" sentinel
2. chunk and part child payload identities are already stable enough for later
   shard deletion because they are addressed by immutable `chunk_okh/chunk_vid`
   and `part_okh/part_vid` pairs
3. the allocator uniqueness requirement is primarily on the reclaim root
   `generation_id`, so `next_generation_id()` must include the new reclaim root
   tables the same way it already includes `simple_payload_reclaims`

Status:

1. chunk-manifest reclaim is now implemented:
   - reclaim tables exist in the metadata DB
   - `next_generation_id()` includes chunk-manifest reclaim roots
   - chunk-manifest reads acquire root-generation payload leases
   - direct delete and stale simple-write displacement now enqueue
     chunk-manifest reclaim records instead of deleting chunk shards inline
2. multipart-manifest reclaim is now implemented:
   - reclaim tables exist in the metadata DB
   - `next_generation_id()` includes multipart reclaim roots
   - multipart reads and copy-source reads acquire root-generation payload
     leases before metadata locks are dropped
   - direct delete and stale overwrite displacement now enqueue multipart
     reclaim records instead of deleting part/chunk shards inline
   - immediate reclaim still exists as a transitional path, but now avoids
     mixed PG lock ordering by snapshotting the reclaim record under the
     metadata lock, dropping that lock, reclaiming shard data, then reacquiring
     metadata only to delete the reclaim row

Required coordinator/storage changes for this design:

1. change the background sweeper to:
   - enumerate reclaim roots
   - skip roots with active leases
   - delete child shard sets idempotently
   - delete child reclaim rows and then the root row
2. move the current transitional foreground reclaim callers over to the
   background sweeper once that path exists

## Design Constraints

1. do not hold bucket locks for the full duration of large reads
2. do not require long-lived locks across all shard PGs of a multipart object
3. do not reintroduce user-visible namespace visibility after delete
4. keep cleanup idempotent and restart-safe
5. preserve the current correctness of versioned delete semantics
6. do not require object metadata rows to stay live once the read snapshot has
   been copied
7. final retention semantics must match the true streaming response lifetime,
   not the current fully-buffered implementation
8. do not keep whole-object `GET` responses in memory; full-object `GET` can be
   up to 50 TB

## Validation

After each phase:

1. `cargo fmt --all`
2. `cargo test -p server-core -p server-http`
3. `cargo clippy -p server-core -p server-http --all-targets --no-deps -- -D warnings -W clippy::pedantic`

Add focused regression tests for:

1. read continues successfully after concurrent delete once it has started
2. read continues successfully after concurrent unversioned overwrite once it
   has started
3. namespace visibility still changes immediately for new readers after delete
   or overwrite
4. deferred cleanup eventually removes stale shard data

## Open Questions

1. the exact multipart-manifest reclaim record shape, especially for streamed
   parts
2. whether multipart reclaim needs any identity beyond the root object
   `generation_id`

Resolved decisions:

1. cleanup should run in a background thread by default
2. tests should not assume immediate physical deletion of old shards; if they
   do, that is a test bug rather than intended behavior
3. `DeleteBucket` should stay synchronous for now and drain bucket-local
   reclaim work rather than becoming an asynchronous bucket lifecycle

Additional follow-up:

1. recent AWS behavior around bucket deletion while uploads still exist is a
   reminder that bucket lifecycle semantics are less tightly coupled than
   object lifecycle semantics and need explicit drain rules
2. if bucket deletion is ever made asynchronous later, an explicit
   bucket-lifecycle `Deleting` state will be required to block new operations

## Recommendation Summary

Recommended next move:

1. write the deterministic reproducer tests first
2. fix metadata snapshot completeness
3. refactor read paths to be truly streaming
4. then finalize and implement deferred old-generation reclamation
5. then apply that mechanism consistently to delete and overwrite paths
