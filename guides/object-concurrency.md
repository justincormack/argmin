# Object Concurrency Invariants

This guide defines the lock and read/write rules for object operations in
`crates/server/src/coordinator.rs`.

## Scope

Applies to object data/metadata operations:

- `put_object`
- `begin_stream_put` / `append_stream_chunk` / `finalize_stream_put` / `abort_stream_put`
- `copy_object` (source read and destination write)
- `upload_part` / streamed `UploadPart` finalize
- `get_object`
- `head_object`
- `get_object_range`
- `delete_object`

Bucket-only operations are out of scope.

## Required Invariants

1. Single-object multi-PG operations must lock PGs in global ascending PG ID order.
2. Streaming chunk appends must never hold a lock while waiting for network input.
3. Streaming chunk append lock scope is metadata/session PG + chunk shard PGs.
4. Streaming finalize lock scope includes metadata/session PG (and any required secondary PGs), in global order.
5. Session state transitions are serialized via metadata-PG transactions over session rows (no ad-hoc in-memory correctness lock).
6. Latest-version reads must bind metadata lookup and shard placement under one consistent lock window.
7. Version ID allocation for versioned writes must be derived while holding the metadata PG lock and revalidated when relocking is required.
8. Full-object reads (`GET` and copy-source full reads) must verify reconstructed data CRC against stored ETag.
9. Streaming must not change external ETag/checksum semantics (headers/XML/validation).

## Required Coordinator APIs

For object read/write/copy paths, use these helpers instead of open-coded lock
orchestration:

- `lock_object_pgs_for_read(...)`
- `lock_object_pgs_for_write(...)`

Use `TwoPgGuards::meta()` and `TwoPgGuards::shard()` for access.

For streaming write paths, use dedicated coordinator streaming APIs and shared
lock helpers for:

- metadata/session PG + shard PG append lock orchestration
- metadata/session PG finalize lock orchestration
- transactional finalize that commits metadata and removes staging rows together

Do not open-code this pattern in object paths:

- `get_pg(meta)` then `get_pg(shard)`
- manual lock-order branching
- ad-hoc relock/retry loops
- lock-holding network reads in streaming paths

## Why These Rules Exist

- Without lock ordering, multi-PG operations can deadlock.
- Without short append lock windows, slow clients can block unrelated work.
- Without consistent read locking, metadata and shard reads can race, causing
  mixed snapshots and integrity failures.
- Without locked version allocation, concurrent versioned writes can choose the
  same `version_id`.
- Without transactional finalize cleanup, stale staging rows can leak or race
  with retries/recovery.

## PR Checklist (Object Paths)

1. Does this change touch object read/write/delete/copy flow?
2. If yes, does it use:
   - `lock_object_pgs_for_read` / `lock_object_pgs_for_write` for non-streaming
     object paths?
   - shared streaming lock helpers/APIs for streamed append/finalize paths?
3. Are lock-order assumptions unchanged and explicit (including metadata/session PG)?
4. For streaming append: metadata/session PG + shard PG lock scope, and no network wait under lock?
5. For streaming finalize: metadata commit + staging-row removal in one
   transaction?
6. Are CRC checks preserved for full-object reads?
7. Are external ETag/checksum semantics unchanged?
8. Did you run:
   - `cargo clippy --workspace -- -D warnings`
   - `cargo test -p server --lib`
   - `cargo test -p s3-tests`
   - atomic stress loop (`test_atomic_dual_write`)
