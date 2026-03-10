# Server HTTP / Storage Boundary Plan

## Context

The `server` split is now in place:

1. `crates/server-core` holds coordinator/core logic
2. `crates/server-http` holds HTTP parsing/rendering and the runtime-facing
   frontend

That split is useful, but one boundary is still not as clean as it should be:
`server-http` still depends directly on `storage`.

Today that happens for two different reasons:

1. runtime bootstrap in [`crates/server-http/src/main.rs`](../crates/server-http/src/main.rs)
   constructs `storage::SharedStorageNode` directly
2. HTTP code imports shared value/domain types from `storage`, for example:
   - [`crates/server-http/src/http/mod.rs`](../crates/server-http/src/http/mod.rs)
   - [`crates/server-http/src/http/response.rs`](../crates/server-http/src/http/response.rs)
   - [`crates/server-http/src/http/xml.rs`](../crates/server-http/src/http/xml.rs)
   - [`crates/server-http/src/http/serve.rs`](../crates/server-http/src/http/serve.rs)

Also, `server-core` is still concrete on `storage::SharedStorageNode`,
`storage::PgStore`, and `MutexGuard<PgStore>` in
[`crates/server-core/src/coordinator.rs`](../crates/server-core/src/coordinator.rs).

So the immediate goal is not "abstract storage completely". The immediate goal
is to remove the avoidable `server-http -> storage` coupling first, with low
risk and without forcing a trait-heavy rewrite.

## Goals

1. Remove `server-http`'s direct dependency on `storage` for shared value types
2. Move concrete runtime/bootstrap wiring out of `server-http`
3. Keep `storage` independent of `server-core` and `server-http`
4. Avoid introducing trait/lifetime complexity into `Coordinator` before it is
   justified

## Non-goals

1. Do not redesign `Coordinator` around generic storage traits yet
2. Do not split `storage` into abstract/concrete crates in the first pass
3. Do not move every type out of `storage`; only move types that are truly
   shared domain values

## Recommendation

Do the cleanup in four steps:

1. move checksum-domain shared types into the existing `checksum` crate
2. extract a small `s3-types` leaf crate for the remaining non-checksum shared
   domain/value types
3. move the binary/runtime wiring into a tiny `argmin-s3` crate
4. reassess whether a deeper `storage` split is still worth the churn

This gets most of the architectural benefit without destabilizing the current
core/storage interaction.

## Phase A: Move Checksum Domain Types into `checksum`

`checksum` already exists and is already depended on by `storage`,
`server-core`, and `server-http`.

Use it for checksum-domain value/config types instead of creating another new
crate for those.

### Types to move into `checksum`

1. `ChecksumAlgorithm`
2. `ChecksumType`
3. `RawChecksum`
4. `MultipartChecksumConfig`
5. `InvalidChecksumConfig`

These are the types currently making `server-http` depend on `storage` for
checksum-related reasons.

### Why this is preferable

1. `checksum` is already the home of checksum logic
2. these types are genuinely checksum-domain types
3. it avoids creating a new crate just to hold checksum enums/value objects

### What should not move into `checksum`

1. `VersionId`
2. `BucketVersioningState`
3. bucket/object metadata structs
4. storage record types

Those are shared domain types, but not checksum-domain types.

## Phase B: Add `s3-types` for Non-checksum Shared Value Types

Create a new leaf crate with no dependency on `storage`, `server-core`, or
`server-http`.

Suggested name:

1. `crates/s3-types`

It should hold only stable non-checksum domain/value types that are genuinely
shared across layers.

### Initial types to move into `s3-types`

1. `VersionId`
2. `BucketVersioningState`

### Candidates for a later second pass

1. `BucketName`
2. `ObjectKey`
3. `UploadId`
4. `SessionId`
5. `ObjectEtag`

Move these only if the remaining boundary friction justifies it.

### Types to leave in `storage` for now

Leave these where they are in the first pass:

1. `PgStore`
2. `SharedStorageNode`
3. `BucketInfo`
4. `ObjectRecord`/`StoredObject`-adjacent storage records
5. shard I/O records and DB-specific row models
6. string newtypes like `BucketName`, `ObjectKey`, `UploadId`, `SessionId`

Reason:

1. `PgStore`/`SharedStorageNode` are backend implementation details
2. `BucketInfo` is still closer to storage/core than to a pure shared leaf
3. the string newtypes may eventually belong in a leaf crate, but they are not
   required to remove `server-http -> storage` today

### Migration shape

1. move the checksum-domain types listed above into `checksum`
2. add `s3-types`
3. move the initial non-checksum shared types there
4. update `storage` to depend on `checksum` and `s3-types`
5. update `server-core` to depend on `checksum` and `s3-types`
6. update `server-http` to import those values from `checksum`,
   `s3-types`, or `server-core`, not `storage`

### Important follow-up inside `server-core`

`server-core` request/result DTOs should prefer leaf-crate/shared value types,
not storage-owned types.

Example:

1. checksum-bearing results should use `checksum::RawChecksum`
2. version fields should use `s3_types::VersionId`
3. multipart checksum configuration should use
   `checksum::MultipartChecksumConfig`

## Phase C: Stop Leaking Storage Records to HTTP

Even after Phases A-B, some `server-http` imports from `storage` may remain if
`server-core` still exposes storage-owned structs in its API.

The main rule should be:

1. `server-http` should depend on `server-core` DTOs and leaf shared types
2. `server-http` should not depend on storage record types

### Expected cleanup

The most likely remaining example is `BucketInfo`.

Instead of passing `storage::BucketInfo` directly into response/XML code,
`server-core` should return a core-owned DTO such as:

```rust
pub struct BucketSummary {
    pub name: String,
    pub created_at: u64,
    pub public_read: bool,
    pub owner_principal: String,
    pub versioning: BucketVersioningState,
}
```

This keeps HTTP out of storage-shaped records.

### Exit criterion for Phase C

`crates/server-http/Cargo.toml` should no longer need `storage` as a normal
dependency.

Dev-dependency use in HTTP tests is acceptable temporarily, but the library
crate should be clean.

## Phase D: Move Runtime Wiring into a Tiny Binary Crate

Once the shared-type cleanup is done, move the concrete startup path out of
`server-http`.

Create:

1. `crates/argmin-s3`

Move into that crate:

1. `src/main.rs`
2. `config.rs`
3. concrete `SharedStorageNode::open(...)` wiring
4. EC self-test startup
5. `Coordinator::new(...)` startup wiring
6. TCP listener bind and `serve(...)` launch

After that:

1. `server-http` becomes a library crate only
2. `argmin-s3` owns the concrete runtime composition
3. `argmin-s3` can legitimately depend on `storage`

### Why move `config.rs` too

`config.rs` is not really HTTP logic. It is runtime/bootstrap configuration.
Keeping it in `server-http` makes the library carry binary concerns.

## Phase E: Reassess the Deeper Storage Split

Only after Phases A-D are complete should we decide whether the remaining
`server-core -> storage` concreteness is actually a problem.

Current concrete coupling includes:

1. `Coordinator` stores `Arc<SharedStorageNode>`
2. helper code works with `MutexGuard<PgStore>`
3. many internal methods take `&PgStore`

That means a full storage abstraction is not a small follow-up. It would
require substantial API surgery.

### If we still want that later

The next shape would likely be:

1. keep traits in `storage` or a new `storage-api` crate
2. move `PgStore`/`SharedStorageNode` into a concrete backend crate such as
   `storage-local`
3. introduce a small core-facing runtime interface instead of exposing
   `MutexGuard<PgStore>` through `Coordinator`

### Recommendation

Do not start here.

First finish the cleaner, lower-risk steps above. Revisit this only if there is
a concrete need:

1. alternate storage backends
2. cleaner embedding of `server-core`
3. easier testing with non-SQLite backends

## Suggested Implementation Order

1. move checksum-domain shared types into `checksum`
2. add `crates/s3-types`
3. move `VersionId` and `BucketVersioningState` into it
4. update `storage`, `server-core`, `server-http` imports
5. introduce core-owned DTOs where HTTP still sees storage records
6. remove normal `storage` dependency from `server-http`
7. create `crates/argmin-s3`
8. move `main.rs` and `config.rs` there
9. update workspace members and package docs
10. reassess whether a full storage abstraction is still worth doing

## Validation

After each step:

1. `cargo fmt --all`
2. `cargo test -p storage -p server-core -p server-http`
3. `cargo clippy -p server-core -p server-http --all-targets --no-deps -- -D warnings -W clippy::pedantic`

After the binary split:

1. `cargo test -p argmin-s3 -p server-http -p server-core -p s3-tests`
2. `cargo clippy -p argmin-s3 -p server-http -p server-core --all-targets --no-deps -- -D warnings -W clippy::pedantic`

## Open Questions

1. whether the string newtypes should stay in `storage` for now, or move to the
   leaf crate in a second pass
2. whether `ObjectEtag` should remain storage-owned or join `s3-types` once
   the first shared-type pass is stable
3. whether `BucketInfo` should stay storage-owned or be replaced entirely by
   core-owned DTOs at the `server-core -> server-http` boundary
4. whether `s3-tests` should keep constructing `Coordinator` directly, or move
   more tests to the binary/runtime composition path after `argmin-s3` exists

## Recommendation Summary

Recommended next move:

1. move checksum-domain shared types into `checksum`
2. create `crates/s3-types` for the non-checksum shared value types
3. use that to remove `server-http`'s normal dependency on `storage`
4. then move the binary wiring into `crates/argmin-s3`

Do not start with a generic `Coordinator<S>` rewrite.
