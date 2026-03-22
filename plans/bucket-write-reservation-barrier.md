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

Bucket lifecycle should distinguish at least:

- `Active`
- `Draining`
- `Deleting`

`Active` allows new write reservations.

`Draining` rejects new write reservations but allows existing reserved operations to finish.

`Deleting` means no visible or in-flight bucket content remains and final deletion can proceed.

### Write reservations

Normal bucket-mutating object operations acquire a bucket write reservation before publishing object/session state:

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
- trigger bucket-delete wakeup/finalization if the bucket is draining and the count reaches zero

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

## Implementation plan

1. Add bucket lifecycle and in-flight reservation metadata to the bucket PG.
2. Add reservation acquire/release APIs on storage/core.
3. Convert streamed `PutObject` to hold a reservation for the session lifetime instead of using the coarse bucket lock.
4. Convert multipart create/complete to use reservations.
5. Update `DeleteBucket` to use `Draining` plus reservation drain, and to treat live stream sessions as bucket content.
6. Remove the coarse bucket lock from hot object write paths once the new barrier is proven.

## Validation

- existing bucket deletion correctness tests
- new races:
  - `DeleteBucket` vs `begin_stream_put`
  - `DeleteBucket` vs `finalize_stream_put`
  - `DeleteBucket` vs `create_multipart_upload`
  - `DeleteBucket` vs `complete_multipart_upload`
- trace confirmation that small-object `PUT` no longer waits on `SharedStorageNode::lock_bucket`
- targeted warp small-object rerun

## Notes

- The current bucket lock is still needed until this barrier exists.
- This work should be done before introducing a direct tiny-`PutObject` fast path, so the fast path has a correct bucket-deletion story from the start.
