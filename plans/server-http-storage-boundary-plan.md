# Server HTTP / Storage Boundary Plan

## Status

Completed:

1. Phase A: checksum-domain shared types moved into `checksum`
2. Phase B: `VersionId` and `BucketVersioningState` moved into `s3-types`
3. Phase C: `server-core` now returns a core-owned `BucketSummary` instead of
   leaking `storage::BucketInfo` to HTTP
4. Phase D: runtime wiring moved into `crates/argmin-s3`
5. `server-http` no longer has a normal dependency on `storage`

Current state:

1. `server-http` remains a library/frontend crate
2. `argmin-s3` is now the concrete runtime entrypoint and legitimately depends
   on `storage`
3. `storage` remains independent of `server-core` and `server-http`
4. authentication is still HTTP-facing, but authorization and several
   state-dependent policy checks are still happening in `server-http`

## Context

The `server` split is now in place:

1. `crates/server-core` holds coordinator/core logic
2. `crates/server-http` holds HTTP parsing/rendering and the runtime-facing
   frontend

That split is now materially cleaner:

1. shared checksum-domain types live in `checksum`
2. shared non-checksum S3 value types currently live in `s3-types`
3. `server-http` no longer imports storage-owned types in normal code
4. runtime/bootstrap wiring now lives in [`crates/argmin-s3`](../crates/argmin-s3)

Also, `server-core` is still concrete on `storage::SharedStorageNode`,
`storage::PgStore`, and `MutexGuard<PgStore>` in
[`crates/server-core/src/coordinator.rs`](../crates/server-core/src/coordinator.rs).

So the remaining question is no longer the `server-http -> storage` boundary.
The next boundary issue is that `server-http` still performs authorization and
other checks that depend on backend state, while `server-core` executes the
operation later against potentially different state.

Only after that is cleaned up does it make sense to revisit whether the deeper
`server-core -> storage` concreteness is worth abstracting further.

## Goals

1. Remove `server-http`'s direct dependency on `storage` for shared value types
2. Move concrete runtime/bootstrap wiring out of `server-http`
3. Move authorization and backend-state policy checks into `server-core`
4. Keep `storage` independent of `server-core` and `server-http`
5. Avoid introducing trait/lifetime complexity into `Coordinator` before it is
   justified

## Non-goals

1. Do not redesign `Coordinator` around generic storage traits yet
2. Do not split `storage` into abstract/concrete crates in the first pass
3. Do not move every type out of `storage`; only move types that are truly
   shared domain values
4. Do not move S3 policy semantics into `storage`

## Recommendation

Do the cleanup in five steps:

1. move checksum-domain shared types into the existing `checksum` crate
2. extract a small `s3-types` leaf crate for the remaining non-checksum shared
   domain/value types
3. move the binary/runtime wiring into a tiny `argmin-s3` crate
4. move authorization and backend-state policy checks into `server-core`
5. reassess whether a deeper `storage` split is still worth the churn

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
    pub public_access_block: Option<String>,
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

## Phase E: Move Authorization and Backend-state Policy into `server-core`

This is now the main remaining layering issue.

`server-http` should authenticate raw HTTP requests. It should not make the
final authorization decision for bucket/object operations, and it should not
enforce policy rules that depend on current bucket state.

### What stays in `server-http`

1. SigV4 header/presign/POST authentication
2. aws-chunked signature verification
3. request syntax validation
4. parsing raw headers/XML/query into typed core requests

### What moves to `server-core`

1. read/write authorization decisions
2. public-read/public-access-block interpretation
3. BucketOwnerEnforced / ACL compatibility checks that depend on stored bucket
   state
4. BlockPublicAcls-style checks that depend on stored bucket state
5. source-read + destination-write authorization for copy operations

### Why

The current shape has a real TOCTOU problem:

1. `server-http` reads bucket state and authorizes
2. `server-core` later acquires locks and executes against current state
3. `server-core` does not re-authorize

That means the check is happening in the wrong layer and not at the point of
use.

### Actual serialization points

This move is about putting authorization at the point of use, but it is
important to be precise about where serialization really comes from.

The serialization points today are:

1. per-PG mutexes in `SharedStorageNode`
2. the bucket stripe lock in `SharedStorageNode`

That means:

1. moving authz into `server-core` fixes the layering and bypass problem
2. it does not by itself create stronger serialization
3. the strength of the guarantee depends on which locks are held during authz

#### Write-side

Normal bucket/object writes already take the bucket lock before executing.

For write paths, authz and bucket-state policy checks should run:

1. in `server-core`
2. after taking the bucket lock
3. immediately before the write proceeds

That gives a strong point-of-use guarantee for bucket-owner, ACL, and
ownership-controls checks.

#### Read-side

Object reads already take the relevant metadata/shard PG mutexes, but they do
not currently take the bucket lock.

So for read paths, moving authz into `server-core` gives:

1. correct layering
2. no authz bypass for non-HTTP callers
3. authorization against current bucket state at the time the core operation
   begins

It does not automatically guarantee that bucket visibility/policy cannot
change during a long-running read unless reads also start taking the bucket
lock.

So read-side locking is a policy choice, not an automatic consequence of this
phase.

### Total surface to move

#### Pure authorization helpers

1. `authorize_bucket_read`
2. `authorize_bucket_write`
3. `authz::can_read_bucket`
4. `authz::can_write_bucket`

#### Operations currently using HTTP-side authorization

1. `DeleteBucket`
2. `HeadBucket`
3. `ListObjectsV1`
4. `ListObjectsV2`
5. `PutObject`
6. `CopyObject`
7. `GetObject`
8. `HeadObject`
9. `DeleteObject`
10. `ListObjectVersions`
11. `GetObjectAttributes`
12. `PutBucketCors`
13. `GetBucketCors`
14. `DeleteBucketCors`
15. `PutBucketTagging`
16. `GetBucketTagging`
17. `DeleteBucketTagging`
18. `PutObjectTagging`
19. `GetObjectTagging`
20. `DeleteObjectTagging`
21. `PutBucketPublicAccessBlock`
22. `GetBucketPublicAccessBlock`
23. `DeleteBucketPublicAccessBlock`
24. `PutBucketOwnershipControls`
25. `GetBucketOwnershipControls`
26. `DeleteBucketOwnershipControls`
27. `PutBucketAcl`
28. `CreateMultipartUpload`
29. `UploadPart`
30. `UploadPartCopy`
31. `CompleteMultipartUpload`
32. `AbortMultipartUpload`
33. `ListMultipartUploads`
34. `ListParts`
35. `POST Object`
36. streaming `PutObject`
37. streaming `UploadPart`

#### Backend-state policy checks currently in `server-http`

1. `authorize_bucket_read`:
   `public_read` plus `IgnorePublicAcls` derived from stored public access block
2. `PutObject`, `CopyObject`, and streaming `PutObject`:
   BucketOwnerEnforced vs `x-amz-acl`
3. `PutBucketOwnershipControls`:
   BucketOwnerEnforced vs existing public-read bucket ACL
4. `PutBucketAcl`:
   BucketOwnerEnforced and BlockPublicAcls checks from stored ownership/PAB

### Recommended interface shape

Add a small core-facing requester type rather than passing raw HTTP request
state through:

```rust
pub struct Requester<'a> {
    pub principal: Option<&'a str>,
}
```

Or, if useful, keep it slightly richer:

```rust
pub enum Requester<'a> {
    Anonymous,
    Principal(&'a str),
}
```

The important point is:

1. `server-http` authenticates and constructs `Requester`
2. `server-core` authorizes using `Requester` plus current bucket/object state
3. `storage` stays auth-agnostic

### Migration order inside this phase

1. move `authz.rs` policy code into `server-core`
2. add `Requester` to the relevant core request types
3. move `PUT`/`GET`/`DELETE` object authorization first
4. move copy-source/destination authorization next
5. move bucket subresource authorization and stored-state policy checks
6. move `POST Object` and streaming write authz
7. delete the HTTP-side `authorize_bucket_*` helpers

### First cut

Start with the normal `PUT Object` path:

1. `server-http` authenticates and parses `PutObjectRequest`
2. `server-core::put_object` receives `Requester`
3. `server-core::put_object` acquires the bucket lock
4. `server-core::put_object` reads current bucket state
5. `server-core::put_object` authorizes and enforces BucketOwnerEnforced
6. only then does it proceed to write

That validates the shape before touching the broader surface.

### Read-side policy decision

We should make this explicit rather than leaving it implicit in the code:

#### Option A: strong bucket-policy serialization for reads

1. read authz in `server-core`
2. take the bucket lock for reads that depend on bucket visibility/policy
3. guarantee that bucket policy/ACL does not change during the read

Tradeoff:

1. stronger semantics
2. more contention, especially for large/streamed reads and list operations

#### Option B: authorize against current state when the read begins

1. read authz in `server-core`
2. do not take the bucket lock for reads
3. rely on PG mutexes for object consistency only
4. accept that a bucket ACL/public-read change may race with an in-flight read

Tradeoff:

1. less contention
2. weaker semantics for long-running reads

Current recommendation:

1. require strong point-of-use authz under the bucket lock for writes
2. move reads to `server-core` authz first without adding bucket locking
3. validate AWS behavior before deciding whether read-side bucket locking is
   needed

## Phase F: Reassess the Deeper Storage Split

Only after Phases A-E are complete should we decide whether the remaining
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
9. move authz and state-dependent policy checks into `server-core`
10. update workspace members and package docs
11. reassess whether a full storage abstraction is still worth doing

## Validation

After each step:

1. `cargo fmt --all`
2. `cargo test -p storage -p server-core -p server-http`
3. `cargo clippy -p server-core -p server-http --all-targets --no-deps -- -D warnings -W clippy::pedantic`

After the binary split:

1. `cargo test -p argmin-s3 -p server-http -p server-core -p s3-tests`
2. `cargo clippy -p argmin-s3 -p server-http -p server-core --all-targets --no-deps -- -D warnings -W clippy::pedantic`

After the authz/policy move:

1. add focused tests for `PUT`, `COPY`, bucket ACL/public-access-block, and
   ownership-controls behavior
2. add at least one regression test proving the coordinator rejects an
   unauthorized direct call that would previously have succeeded if called
   without the HTTP layer
3. add an AWS behavior experiment for in-flight reads:
   start with public-read enabled, begin a large read, change the bucket ACL or
   effective public visibility mid-read, and observe whether the in-flight read
   continues or fails
4. repeat the same experiment for:
   a normal object stream,
   multipart by `partNumber`,
   and range reads if practical

## Open Questions

1. whether the string newtypes should stay in `storage` for now, or move to the
   leaf crate in a second pass
2. whether `ObjectEtag` should remain storage-owned or join `s3-types` once
   the first shared-type pass is stable
3. whether `BucketInfo` should stay storage-owned or be replaced entirely by
   core-owned DTOs at the `server-core -> server-http` boundary
4. whether `s3-tests` should keep constructing `Coordinator` directly, or move
   more tests to the binary/runtime composition path after `argmin-s3` exists
5. what consistency guarantee to give read-side authorization:
   whether `GET`/`HEAD`/list/copy-source auth should take bucket locks, or
   whether reading current bucket state immediately before the operation is
   sufficient
6. whether AWS allows an already-started public read to complete after
   public-read is revoked mid-transfer; this should be treated as an empirical
   compatibility question, not assumed

## Recommendation Summary

Recommended next move:

1. move authorization and stored-state policy checks into `server-core`
2. start with normal `PUT Object`
3. then cover copy, bucket ACL/public-access-block/ownership-controls, POST,
   and streaming paths
4. only after that revisit whether a deeper storage abstraction is worth it

Do not start with a generic `Coordinator<S>` rewrite.
