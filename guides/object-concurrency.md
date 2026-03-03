# Object Concurrency Invariants

This guide defines the lock and read/write rules for object operations in
`crates/server/src/coordinator.rs`.

## Scope

Applies to object data/metadata operations:

- `put_object`
- `copy_object` (source read and destination write)
- `get_object`
- `head_object`
- `get_object_range`
- `delete_object`

Bucket-only operations are out of scope.

## Required Invariants

1. Single-object multi-PG operations must lock PGs in global ascending PG ID order.
2. Latest-version reads must bind metadata lookup and shard placement under one
   consistent lock window.
3. Version ID allocation for versioned writes must be derived while holding the
   metadata PG lock and revalidated when relocking is required.
4. Full-object reads (`GET` and copy-source full reads) must verify reconstructed
   data CRC against stored ETag.

## Required Coordinator APIs

For object read/write/copy paths, use these helpers instead of open-coded lock
orchestration:

- `lock_object_pgs_for_read(...)`
- `lock_object_pgs_for_write(...)`

Use `TwoPgGuards::meta()` and `TwoPgGuards::shard()` for access.

Do not open-code this pattern in object paths:

- `get_pg(meta)` then `get_pg(shard)`
- manual lock-order branching
- ad-hoc relock/retry loops

## Why These Rules Exist

- Without lock ordering, multi-PG operations can deadlock.
- Without consistent read locking, metadata and shard reads can race, causing
  mixed snapshots and integrity failures.
- Without locked version allocation, concurrent versioned writes can choose the
  same `version_id`.

## PR Checklist (Object Paths)

1. Does this change touch object read/write/delete/copy flow?
2. If yes, does it use `lock_object_pgs_for_read` / `lock_object_pgs_for_write`?
3. Are lock-order assumptions unchanged and explicit?
4. Are CRC checks preserved for full-object reads?
5. Did you run:
   - `cargo clippy --workspace -- -D warnings`
   - `cargo test -p server --lib`
   - `cargo test -p s3-tests`
   - atomic stress loop (`test_atomic_dual_write`)
