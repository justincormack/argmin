# Object Concurrency Invariants

Status: active guidance. This guide describes the lock-order and concurrency
rules that current object paths are expected to follow.

This guide defines the lock and read/write rules for object operations in
`crates/server-core/src/coordinator.rs`.

## Scope

Applies to object data/metadata operations:

- `put_object`
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

Do not open-code this pattern in object paths:

- `get_pg(meta)` then `get_pg(shard)`
- manual lock-order branching
- ad-hoc relock/retry loops
- lock-holding network reads in streaming paths
- shard-file visibility that bypasses metadata publication
- shard-file writes while holding a PG mutex unless the code is intentionally changing the write visibility model

## Why These Rules Exist

- Without lock ordering, multi-PG operations can deadlock.
- Without short append lock windows, slow clients or durable shard IO can block unrelated work.
- Without metadata-locked snapshotting plus payload leases, reads can race with
  overwrite/delete reclaim and observe mixed snapshots or missing payload.
- Without locked version allocation, concurrent versioned writes can choose the
  same `version_id`.
- Without a durable-before-visible rule, reads can observe metadata that points
  at shard files that were never fully committed, or bypass cleanup rules by
  treating orphan files as live payload.
- Without transactional finalize cleanup, stale staging rows can leak or race
  with retries/recovery.

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
10. Did you run:
   - `cargo clippy --workspace -- -D warnings`
   - `cargo test -p server-http --lib`
   - `cargo test -p s3-tests`
   - atomic stress loop (`test_atomic_dual_write`)
