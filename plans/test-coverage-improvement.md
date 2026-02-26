# Test Coverage Improvement Plan

## Context

Phase 3 (S3 server) is implemented with 285 passing tests. `cargo llvm-cov` shows
79.8% overall line coverage, but several files are at 0% or low coverage. This plan
adds targeted unit tests to close the gaps — prioritized by impact (lines recovered)
and ease (pure unit tests first).

Current state: 4465 covered lines, 900 missed → 79.84%

## Step 1: `crates/server/src/error.rs` — 0% → ~100% (31 lines)

Pure unit tests. No dependencies beyond constructing error variants.

**Tests to add:**
- `s3_error_code()` for all 13 match arms (Auth variants, Store, BucketNotFound, BucketAlreadyExists, BucketNotEmpty, ObjectNotFound, InvalidRequest, MetadataBlobError, ObjectTooLarge, MethodNotAllowed)
- `http_status()` for all status codes (404, 409, 400, 403, 405, 500)
- `From` conversions: `StoreError → ServerError`, `AuthError → ServerError`, etc.
- Verify the wildcard `Auth(_)` arm (e.g. `MissingSignedHeader`) maps to "AccessDenied"
- Verify wildcard `_` arm in `http_status` returns 500

## Step 2: `crates/server/src/http/response.rs` — 0% → ~90% (289 lines)

`S3Response` is a plain struct — no tiny_http needed. All constructors are testable.

**Tests to add:**
- `format_http_date(0)` → `"Thu, 01 Jan 1970 00:00:00 GMT"`
- `format_http_date` for a known date (e.g. 2024-01-15 12:30:45)
- `days_to_date` for edge cases: leap year, year boundary, Feb 29
- `put_object()` → verify ETag header, version-id header, status 200
- `get_object()` with content-type present → verify content-type forwarded
- `get_object()` without content-type → verify `application/octet-stream` default
- `get_object()` with x-amz-meta-* headers → verify passthrough
- `get_object()` with all 6 standard metadata headers (content-encoding, cache-control, etc.)
- `head_object()` → verify headers present, no body
- `delete_object()` → 204, no body
- `create_bucket()` → Location header format
- `delete_bucket()` → 204
- `head_bucket()` → 200
- `list_buckets()` → verify XML body well-formedness
- `list_objects_v2()` → verify XML body
- `error()` → verify status code and XML error body for a few error types

## Step 3: `crates/server/src/config.rs` — 0% → ~90% (69 lines)

Env var parsing. Tests need isolation to avoid cross-test interference.

**Tests to add:**
- Missing `ARGMIN_ACCESS_KEY_ID` → error
- Missing `ARGMIN_SECRET_ACCESS_KEY` → error
- All defaults applied when only required vars set
- Custom values for all optional vars
- Invalid `ARGMIN_PG_COUNT` (non-integer) → parse error
- `ARGMIN_PG_COUNT=0` → explicit error
- Invalid `ARGMIN_EC_K` / `ARGMIN_EC_M` → parse error

**Approach:** Refactor `from_env` to accept a closure/trait for env lookups so tests
can provide values without touching real env vars. Or use `std::sync::Mutex` to
serialize tests that manipulate environment.

## Step 4: `crates/server/src/http/request.rs` — 72% → ~85% (68 missed)

`from_http()` requires `tiny_http::Request` (deferred). Focus on remaining unit-testable gaps.

**Tests to add:**
- `percent_decode` with `%` at end of string (truncated escape)
- `percent_decode` with non-hex chars after `%` (e.g. `%ZZ`)
- `percent_decode` with uppercase hex (`%2F` vs `%2f`)
- `hex_val` for all branches: `0-9`, `a-f`, `A-F`, invalid byte
- `query_param` with empty query string
- `query_param` with param that has no `=` sign
- `query_param` with multiple params, match is not first
- `header_pairs()` — construct S3Request, verify output format
- `header()` returning `None` for missing header

## Step 5: `crates/storage/src/types.rs` — 75% → ~95% (13 missed)

**Tests to add:**
- `ShardStatus::from_u8(0)` → `Some(Live)`
- `ShardStatus::from_u8(1)` → `Some(Deleting)`
- `ShardStatus::from_u8(2)` → `Some(Quarantined)`
- `ShardStatus::from_u8(3)` → `None`
- `ShardStatus::from_u8(255)` → `None`
- `ShardKey` Debug formatting: `format!("{:?}", key)` contains hex

## Step 6: `crates/storage/src/pg_store.rs` — 73% → ~82% (158 missed)

**Tests to add:**
- `prefix_end("foo")` → `Some("fop")`
- `prefix_end("")` → `Some("\x01")` or similar (verify behavior)
- `prefix_end("\xff")` → `None`
- `prefix_end("\xfe\xff")` → `Some("\xff")`
- `prefix_end("abc\xff")` → `Some("abd")`
- `list_objects` with `(Some(prefix), Some(start_after))` combination — pagination within prefix
- `stat_shard` on quarantined shard (write shard, corrupt file, read to trigger quarantine, then stat)
- `pg_id()` accessor returns correct value

Note: `prefix_end` is private — tests go in the existing `#[cfg(test)]` module inside the file.

## Step 7: `crates/storage/src/bucket_db.rs` — 71% → ~85% (45 missed)

**Tests to add:**
- `connection()` accessor — call it, verify it works
- `open()` with non-existent parent directory → error
- `list_buckets` with multiple buckets — verify all fields (region, versioning)
- `head_bucket` result fields — verify region and versioning values

## Step 8: `crates/storage/src/node.rs` — 89% → ~95% (7 missed)

**Tests to add:**
- `data_dir()` accessor returns correct path
- `get_pg_store()` (trait method) with non-existent PG ID → `PgNotFound` error

## Step 9: `crates/auth/src/credential.rs` — minor gap

**Tests to add:**
- `CredentialStore::new()` + `get()` returns `None` for missing key
- `add()` then `get()` returns `Some`
- Overwrite: `add()` same key twice, `get()` returns latest
- `Default::default()` returns empty store

## Deferred

- **`http/mod.rs`** (242 lines, 0%): `authenticate()` and `dispatch()` take `&S3Request` so they're technically unit-testable by constructing `S3Request` manually with a `Coordinator`. However, `Coordinator::new` needs a real `LocalStorageNode` + `SqliteBucketDb` + `ErasureCodec`, making these closer to integration tests. Defer to a follow-up.
- **`main.rs`** (81 lines, 0%): Binary entry point, not unit-testable.
- **`ec/src/codec.rs`** (93% → higher): The missed lines are mostly error paths for invalid EC parameters that ISA-L rejects. Low priority.

## Estimated Impact

| Step | File | Lines recovered | Effort |
|------|------|----------------|--------|
| 1 | error.rs | ~31 | Small |
| 2 | response.rs | ~260 | Medium |
| 3 | config.rs | ~62 | Small |
| 4 | request.rs | ~30 | Small |
| 5 | types.rs | ~10 | Tiny |
| 6 | pg_store.rs | ~40 | Medium |
| 7 | bucket_db.rs | ~20 | Small |
| 8 | node.rs | ~5 | Tiny |
| 9 | credential.rs | ~5 | Tiny |
| **Total** | | **~463 lines** | |

Projected coverage: ~(4465 + 463) / (4465 + 900) ≈ **91.8%**

## Verification

1. `cargo test --workspace` — all existing + new tests pass (0 failures)
2. `cargo llvm-cov --workspace` — verify coverage improved to target
3. No new dependencies added (tests use only existing test infrastructure)
