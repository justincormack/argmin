# Bucket Write Reservation Barrier

## Problem

Small-object `PUT` is still paying high fixed overhead from the coarse bucket lock:

- `begin_stream_put` acquires `SharedStorageNode::lock_bucket(bucket)`
- `finalize_stream_put` acquires `SharedStorageNode::lock_bucket(bucket)` again
- `CreateMultipartUpload` and `CompleteMultipartUpload` also use the same bucket lock

Recent traces show the lock acquisition delay is now one of the dominant fixed costs on tiny writes, even after shard durability and HTTP-side improvements.

However, the bucket lock cannot simply be removed. It is currently acting as a bucket-level transaction boundary across multiple PGs:

- bucket lifecycle state lives on the bucket PG
- object rows, stream-upload sessions, and multipart uploads live on object PGs

Without a bucket-level fence, this race is possible:

1. a writer checks that the bucket is active
2. `DeleteBucket` scans and sees the bucket as empty
3. `DeleteBucket` marks the bucket deleting
4. the writer publishes a new object/session/upload on another PG

That would violate S3 bucket deletion semantics.

## Goal

Replace the coarse bucket mutex on normal writes with a reservation barrier that:

- preserves bucket deletion correctness
- blocks new writes once bucket deletion begins
- lets existing writes drain cleanly
- avoids serializing unrelated writes to the same bucket through one mutex

## Intended model

### Bucket states

The first implementation keeps the persisted lifecycle enum as:

- `Active`
- `Deleting`

and layers an internal drain barrier on top with two bucket metadata fields:

- `write_reservations_blocked`
- `active_write_reservations`

That gives the coordinator an effective `Draining` phase without changing the
bucket state enum itself.

### Write reservations

Normal bucket-mutating object operations acquire a short-lived bucket write
reservation before publishing cross-PG state:

- direct `PutObject`
- `begin_stream_put`
- `create_multipart_upload`
- `complete_multipart_upload`
- other write paths that create visible state

Reservation acquisition should:

- run against the bucket PG
- check bucket state is `Active`
- increment an in-flight write counter

Reservation release should:

- decrement the in-flight write counter
- happen immediately after the protected publish step completes

For streamed `PutObject`, the current implementation uses reservations around:

- `begin_stream_put` session publication
- `finalize_stream_put` object publication

not for the full lifetime of an in-progress stream session. Live stream sessions
are treated as bucket content during delete-bucket emptiness checks instead.

### DeleteBucket flow

`DeleteBucket` should:

1. atomically transition the bucket from `Active` to `Draining`
2. reject if already non-active in a way that should fail
3. wait for active bucket write reservations to drain
4. check emptiness:
   - object versions/delete markers across all PGs
   - multipart uploads across all PGs
   - streaming upload sessions across all PGs
5. if empty, transition to `Deleting` and continue final delete
6. if not empty, return `BucketNotEmpty` and restore the bucket to `Active`

This replaces the current coarse mutex sequencing with explicit bucket lifecycle state.

## Expected performance effect

This should remove the current bucket-lock convoy on tiny writes.

For small `PUT`, the main expected win is:

- no bucket mutex acquisition in `begin_stream_put`
- no bucket mutex acquisition in `finalize_stream_put`

It also unlocks a cleaner direct small-object `PutObject` fast path later:

- one write reservation
- one object publish
- no streamed session lifecycle for single-segment known-length puts

## Current status

Done:

1. Added bucket drain/reservation metadata to the bucket PG.
2. Added storage/core acquire/release/drain APIs.
3. Converted streamed `PutObject` begin/finalize to use bucket reservations instead of the coarse bucket lock.
4. Updated `DeleteBucket` to:
   - begin the drain barrier
   - wait for active reservation publishers to finish
   - treat live stream sessions as bucket content
5. Converted `CreateMultipartUpload` and `CompleteMultipartUpload` to use bucket reservations instead of the coarse bucket lock.
6. Removed the coarse bucket lock from `DeleteBucket`; it now relies on the drain barrier directly and retries if another delete is already in the draining phase.
7. Re-evaluated the remaining bucket mutex uses and left them only on non-hot lifecycle paths.
8. Added a shared active-bucket fast path for hot object auth, so `GET`/`HEAD`/`DELETE`/`PUT` no longer serialize on `head_bucket()` reads against the bucket PG.

Deferred:

1. `CreateBucket` still uses the coarse bucket mutex for lifecycle serialization and idempotent create/create-delete races. This path is not hot and does not affect the tiny-object write bottleneck.
2. Async delete finalization still uses the coarse bucket mutex to serialize bucket-row removal and reclaim-root teardown against other lifecycle operations. This path is not on request latency.

## Validation

- existing bucket deletion correctness tests
- new races:
  - `DeleteBucket` vs `begin_stream_put`
  - `DeleteBucket` vs `finalize_stream_put`
  - `DeleteBucket` vs `create_multipart_upload`
  - `DeleteBucket` vs `complete_multipart_upload`
- bucket fast-path correctness:
  - object `HEAD` and `DELETE` continue to work while the bucket PG mutex is held elsewhere
  - bucket ACL updates become visible to object-read auth without waiting for a DB fallback
  - bucket versioning updates become visible to object-delete semantics without waiting for a DB fallback
- trace confirmation that small-object `PUT` no longer waits on `SharedStorageNode::lock_bucket`
- targeted warp small-object rerun

## Notes

- The coarse bucket lock is no longer on hot write paths. Remaining uses are on
  non-hot lifecycle/finalization paths such as bucket create and delete
  finalization, and should only move if there is a clear correctness or
  contention reason.
- This work should be done before introducing a direct tiny-`PutObject` fast path, so the fast path has a correct bucket-deletion story from the start.

## Outcome

The reservation barrier now covers the hot bucket-mutating request paths:

- streamed `PutObject` begin/finalize
- `CreateMultipartUpload`
- `CompleteMultipartUpload`
- `DeleteBucket`

That removes the coarse bucket mutex from the small-object write critical path
while preserving the bucket deletion fence that was previously implicit in the
mutex.

That work is now paired with a shared active-bucket fast path for hot object
auth. The fast path lives on the shared storage node, is updated synchronously
by the owning bucket PG on active-bucket mutations, and serves the steady-state
bucket fields object ops need (`owner`, `public_read`, `public_write`,
`versioning`, `public_access_block`, `ownership_controls`, and active
visibility) without taking the bucket PG mutex or hitting SQLite on every
request.

This plan is complete. The remaining coarse bucket mutex uses are intentionally
deferred lifecycle paths, not part of the hot request path.
