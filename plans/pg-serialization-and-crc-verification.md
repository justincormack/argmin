# Plan: Fix PG serialization and read-path CRC verification

## Context

`test_atomic_dual_write` is flaky because two concurrent PUT requests to the same unversioned key land on different frontends, each with its own `PgStore` instance for the same PG. The shard file renames interleave across frontends, producing mixed shards (some from write A, some from write B). The read path then returns corrupt data without detecting it because there is no CRC verification after shard reassembly.

Two correctness issues to fix:
1. **Read-path CRC gap**: `get_object` and `copy_object` read full objects but never verify the reconstructed data CRC against the stored etag. This is a missing core correctness guarantee — all reads should be checking checksums.
2. **PG serialization**: The design intent is that each PG is serialized for correctness. Currently each of the 4 frontends opens its own `PgStore` (own `rusqlite::Connection`, same filesystem directory), allowing concurrent unserialized access to the same PG.

## Part 1: Read-path CRC verification

### Background

The etag is `crc64::checksum(metadata_blob_bytes || user_data)` computed before EC-padding (`coordinator.rs:502`). Individual shard CRCs are verified on `read_shard()` (`pg_store.rs:232`), but this doesn't catch:
- Mixed shards from different concurrent writes (each shard individually valid)
- EC reconstruction producing wrong output (reconstructed shards aren't CRC-checked)
- Bugs in shard assembly logic

### Where to add verification

| Read path | Reads full data? | Action |
|-----------|-----------------|--------|
| `get_object` (coord:952) | Yes — `read_range(0, total_size-1)` | Add CRC check |
| `copy_object` (coord:687) | Yes — `read_range(0, total-1)` on source | Add CRC check |
| `head_object` (coord:1024) | No — only metadata shards | Per-shard CRC suffices |
| `get_object_range` (coord:1090) | No — only a byte range | Per-shard CRC suffices |

### Changes

**`crates/server/src/error.rs`** — Add variant:
```rust
#[error("data integrity error for {bucket}/{key}")]
IntegrityError { bucket: String, key: String, expected: u64, actual: u64 },
```
- `s3_error_code` → `"InternalError"`, `http_status` → `500`

**`crates/server/src/coordinator.rs`** — In `get_object`, after line 1004 (after `read_range` succeeds):
```rust
let actual_crc = crc64::checksum(&data);
if actual_crc != etag_crc {
    return Err(ServerError::IntegrityError { ... });
}
```
`etag_crc` is already available at line 985.

Same in `copy_object` after line 702, using `src_etag_crc` from line 675.

## Part 2: PG serialization

### Design

Introduce `SharedStorageNode` — wraps each `PgStore` in `std::sync::Mutex`. All frontends share one `Arc<SharedStorageNode>`. This serializes all operations within a PG while allowing parallelism across PGs.

`std::sync::Mutex` is correct: all PG access happens in `spawn_blocking` (sync context in `serve.rs:193`). `PgStore` is `Send` (`rusqlite::Connection` is `Send`), so `Mutex<PgStore>` is `Send + Sync`.

### New type

**`crates/storage/src/node.rs`** — Add alongside `LocalStorageNode` (kept for single-frontend unit tests):

```rust
pub struct SharedStorageNode {
    stores: HashMap<u32, Mutex<PgStore>>,
    pg_id_list: Vec<u32>,
    data_dir: PathBuf,
}
```

Methods:
- `open(data_dir, pg_ids)` — same as `LocalStorageNode::open` but wraps each in `Mutex`
- `get_pg(pg_id) -> Result<MutexGuard<PgStore>, StoreError>`
- `pg_ids() -> &[u32]`, `data_dir() -> &Path`

### Two-PG locking

Several coordinator methods access two PGs (meta_pg from `derive_pg` + shard_pg from `derive_pg_shards`). These can be the same or different PG IDs.

Rules:
- Same PG ID → lock once, use one guard for both roles
- Different PG IDs → lock in ascending ID order to prevent deadlocks

Helper on `SharedStorageNode`:
```rust
pub fn lock_two_pgs(&self, pg_a: u32, pg_b: u32)
    -> Result<(MutexGuard<PgStore>, Option<MutexGuard<PgStore>>), StoreError>
```
Returns `(guard_a, None)` when same PG, `(guard_a, Some(guard_b))` when different (locked ascending).

At call sites:
```rust
let (meta_guard, shard_guard) = self.storage_node.lock_two_pgs(meta_pg_id, shard_pg_id)?;
let meta_pg: &PgStore = &meta_guard;
let shard_pg: &PgStore = match &shard_guard { Some(g) => g, None => meta_pg };
```

### Coordinator changes

**`crates/server/src/coordinator.rs`**:

1. `storage_node: LocalStorageNode` → `storage_node: Arc<SharedStorageNode>`
2. `Coordinator::new()` takes `Arc<SharedStorageNode>`
3. All 24 `get_pg()` calls now return `MutexGuard<PgStore>`. Since `MutexGuard<T>: Deref<Target=T>`, single-PG call sites work unchanged.
4. Refactor `write_object_inner` to take `meta_pg: &PgStore, shard_pg: &PgStore` as params (callers do the locking).

**Two-PG methods** using `lock_two_pgs`:

| Method | How PG IDs are known | Approach |
|--------|---------------------|----------|
| `put_object` (unversioned) | Both known upfront (version_id=0) | `lock_two_pgs` then call `write_object_inner` |
| `put_object` (versioned) | shard_pg depends on version_id from meta_pg | Lock meta_pg, get version_id, compute shard_pg_id. If shard < meta (wrong order): drop, relock in order, re-read version_id |
| `get_object` | Both known after metadata read | Lock meta_pg, read record, derive shard_pg_id, lock second if different |
| `head_object` | Same | Same |
| `get_object_range` | Same | Same |
| `delete_object` | Same (derive shard_pg_id from record) | Same |
| `copy_object` | Two phases | Phase 1: lock source PGs, read, drop. Phase 2: `write_object_inner` locks dest PGs |

**Single-PG methods** (trivial — `MutexGuard` is drop-in):
- `delete_bucket`, `put_object_tags`, `get_object_tags`, `delete_object_tags`, `list_objects_v2`, `list_object_versions`

### Server creation changes

**`crates/server/src/main.rs`** — Create one `Arc<SharedStorageNode>`, share across workers:
```rust
let storage_node = Arc::new(SharedStorageNode::open(data_dir, &pg_ids)?);
for _ in 0..config.workers {
    let coordinator = Coordinator::new(Arc::clone(&storage_node), bucket_db, ...)?;
}
```

**`crates/s3-tests/src/server.rs`** — Same pattern, one `Arc<SharedStorageNode>` shared across `POOL_SIZE` frontends.

## Files to modify

| File | Change |
|------|--------|
| `crates/storage/src/node.rs` | Add `SharedStorageNode` with `Mutex<PgStore>` + `lock_two_pgs` |
| `crates/storage/src/lib.rs` | Add `pub use node::SharedStorageNode` |
| `crates/server/src/error.rs` | Add `IntegrityError` variant + mappings |
| `crates/server/src/coordinator.rs` | `Arc<SharedStorageNode>`, refactor `write_object_inner`, CRC verification, all `get_pg` call sites |
| `crates/server/src/main.rs` | Shared `Arc<SharedStorageNode>` across workers |
| `crates/s3-tests/src/server.rs` | Shared `Arc<SharedStorageNode>` across frontends |

## Implementation order

1. Add `IntegrityError` variant to `ServerError`
2. Add CRC verification in `get_object` and `copy_object`
3. Implement `SharedStorageNode` in storage crate
4. Update `Coordinator` to use `Arc<SharedStorageNode>` + refactor two-PG methods
5. Update `main.rs` and test server to share storage node
6. Update coordinator unit test helper `setup_coordinator`

## Verification

1. `cargo clippy --workspace -- -D warnings`
2. `cargo test -p storage` — storage unit tests
3. `cargo test -p server-http --lib` — coordinator + server unit tests
4. `cargo test -p s3-tests` — all integration tests
5. Run `test_atomic_dual_write` in a loop to confirm no more flakiness:
   `for i in $(seq 1 50); do cargo test -p s3-tests --test atomic test_atomic_dual_write || break; done`
