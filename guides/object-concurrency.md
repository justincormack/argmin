<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Object Concurrency Invariants

Status: active guidance. This guide describes the lock-order and concurrency
rules that current object paths are expected to follow.

This guide defines the lock and read/write rules for object operations in
`crates/server-core/src/coordinator.rs`.

## Scope

Applies to object data/metadata operations:

- `put_object`
- `complete_multipart_upload`
- `begin_stream_put` / `append_stream_segment` / `finalize_stream_put` / `abort_stream_put`
- `copy_object` (source read and destination write)
- `upload_part` / streamed `UploadPart` finalize
- `get_object`
- `head_object`
- `get_object_range`
- `delete_object`

Bucket metadata operations are in scope when they coordinate with object
metadata paths (for example, `DeleteBucket` emptiness checks).

## Required Invariants

1. Single-object multi-PG operations must lock PGs in global ascending PG ID order.
2. Streaming segment appends must never hold a lock while waiting for network input.
3. Streaming segment appends must validate session state under the metadata/session PG, then release PG locks before durable shard file IO.
4. Streaming finalize lock scope includes metadata/session PG (and any required secondary PGs), in global order.
5. Session state transitions are serialized via metadata-PG transactions over session rows (no ad-hoc in-memory correctness lock).
6. Latest-version reads must snapshot metadata and acquire any payload-generation lease before releasing the metadata PG lock.
7. Version ID allocation for versioned writes must be derived while holding the metadata PG lock and revalidated when relocking is required.
8. Full-object reads (`GET` and copy-source full reads) must verify reconstructed data CRC against stored ETag.
9. Streaming must not change external ETag/checksum semantics (headers/XML/validation).
10. Shard payload must become visible only after durable shard files exist; raw shard files may exist before metadata publication, but reads must treat metadata rows as the visibility boundary.
11. Mixed bucket+object metadata operations must lock PGs in global ascending PG
    ID order when more than one PG is held.
12. `CompleteMultipartUpload` must replicate its bucket-write dependency through
    the bucket-PG command stream before publishing the object-PG completion, via
    `AdvanceMultipartCompletionBarrier`.
13. The multipart completion barrier must be durable and storage-node owned;
    request paths must not use process-local multipart-completion mutexes for
    correctness.
14. The barrier may advance one fixed-size bucket scalar, but must not retain
    per-upload completion or abort history.
15. Exact completion replay metadata must publish atomically with the completed
    object version, and disappear only when that version is replaced or removed.
16. Copy-source authorization must return its exact metadata snapshot together
    with a broad payload-generation lease acquired on the actual storage nodes.
    The lease must remain visible across frontend/runtime-map replacement.
    Copy processing may replace it only after acquiring the shard-specific read
    lease, so source overwrite/delete reclaim never observes an unleased
    interval. Long-lived Unix generation-lease sessions consume the same
    aggregate configured RPC admission limit and the shared non-control budget.
    All lease acquisition stops one slot short of that shared budget, preserving
    narrow-lease to read-handle progress. Broad acquisition stops one additional
    slot earlier, so a new broad lease cannot consume the broad-to-narrow
    transition slot while existing broad leases acquire shard-scoped
    successors. Each broad lease is released after its successor is acquired.
    The remaining reserved capacity must allow short completion/control
    operations to progress.

## Required Coordinator APIs

For object read/write/copy paths, use these helpers instead of open-coded lock
orchestration:

- `lock_object_pgs_for_read(...)`
- `lock_object_pgs_for_write(...)`

Use `TwoPgGuards::meta()` and `TwoPgGuards::shard()` for write-side access.
Read-side helpers snapshot metadata under the metadata PG lock and must not
open-code read-side shard relocking.

For streaming write paths, use dedicated coordinator streaming APIs and shared
lock helpers for:

- metadata/session PG validation, unlocked shard file durability, then ordered shard/segment publication
- metadata/session PG finalize lock orchestration
- transactional finalize that commits metadata and removes staging rows together

For multipart completion publication, use:

- the storage-cluster completion barrier, which publishes an
  `AdvanceMultipartCompletionBarrier` bucket-PG metadata command under the
  current completion reservation before constructing the object-PG completion
  command; a barrier left pending by an earlier reservation is drained before
  a fresh barrier is allocated
- the storage-node-owned metadata command serialization boundary for both the
  bucket-PG barrier command and object-PG completion command
- object-version-scoped replay metadata; never add terminal upload history or
  a pruning path

Do not open-code this pattern in object paths:

- `get_pg(meta)` then `get_pg(shard)`
- manual lock-order branching
- ad-hoc relock/retry loops
- open-coded durable bucket write reservation acquire/release pairs in migrated
  production paths; use a shared storage wrapper that owns acquire, snapshot
  load, protected action, and release
- lock-holding network reads in streaming paths
- shard-file visibility that bypasses metadata publication
- shard-file writes while holding a PG mutex unless the code is intentionally changing the write visibility model
- allocating multipart completion order outside the bucket-PG metadata command
  stream
- using process-local multipart-completion mutexes as a correctness boundary
- using `completed_at` timestamps or upload IDs as the authoritative prune order

## Why These Rules Exist

- Without lock ordering, multi-PG operations can deadlock.
- Without short append lock windows, slow clients or durable shard IO can block unrelated work.
- Without metadata-locked snapshotting plus payload leases, reads can race with
  overwrite/delete reclaim and observe mixed snapshots or missing payload.
- Without command-owned version allocation, concurrent versioned writes can
  choose the same `version_id`.
- Without a durable-before-visible rule, reads can observe metadata that points
  at shard files that were never fully committed, or bypass cleanup rules by
  treating orphan files as live payload.
- Without transactional finalize cleanup, stale staging rows can leak or race
  with retries/recovery.
- Without the durable bucket-PG completion barrier, replicated object-PG commit
  recovery can observe a half-published bucket-write dependency.
- Without storage-node-owned command serialization, multiple frontend processes
  can observe each other's half-complete barrier/object-publication windows.
- Without shared reservation wrappers and explicit error-path review, fallible
  operations can leak bucket write reservations or release the wrong error
  shape.

## PR Checklist (Object Paths)

1. Does this change touch object read/write/delete/copy flow?
2. If yes, does it use:
   - `lock_object_pgs_for_read` / `lock_object_pgs_for_write` for non-streaming
     object paths?
   - shared streaming lock helpers/APIs for streamed append/finalize paths?
3. Are lock-order assumptions unchanged and explicit (including metadata/session PG)?
4. For streaming append: metadata/session validation before durable file IO, no network wait under lock, and shard/segment metadata publication only after shard durability succeeds?
5. For streaming finalize: metadata commit + staging-row removal in one
   transaction?
6. Are CRC checks preserved for full-object reads?
7. Are external ETag/checksum semantics unchanged?
8. For streamed writes, is the visibility boundary still metadata publication rather than raw shard-file presence?
9. For bucket+object mixed operations, is lock order explicit and ascending by PG ID?
10. For `CompleteMultipartUpload`, is the bucket-write dependency replicated
    through a fresh `AdvanceMultipartCompletionBarrier` under the current
    reservation before the object-PG completion command is built?
11. Is exact replay metadata still atomically scoped to the completed object
    version, with no per-upload terminal history or cleanup path?
12. If this change touches reservations, drains, guards, or storage critical sections:
   - is acquisition and release owned by a shared helper rather than open-coded?
   - does every fallible path (`?`, early return, closure error) still release?
   - does the protected action run inside the intended reservation/critical-section lifetime?
   - if both the action and release fail, does the action error still win?
   - did you grep for sibling helpers with the same acquire/release pattern?
   - did you add a regression for release-on-error and, where relevant, “blocked action holds off conflicting work” behavior?
13. Did you run:
   - `cargo clippy --workspace -- -D warnings`
   - `cargo test -p server-http --lib`
   - `cargo test -p s3-tests`
   - atomic stress loop (`test_atomic_dual_write`)
