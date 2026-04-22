## Coordinator / Storage Capability Refactor

Status: in progress

This plan follows the now-completed PG serialization work in
`plans/completed/pg-serialization-and-crc-verification.md` and the earlier
boundary note in `plans/completed/server-http-storage-boundary-plan.md`.

The current system is safer than the original per-frontend `PgStore` design,
but it still exposes the wrong abstraction to `server-core`: coordinator code
still talks in terms of PGs and still decides when to acquire storage guards.

That is not strong enough. The recent bucket-ABAC deadlock showed the real
remaining problem:

- request paths can acquire some storage state
- later discover they also need more bucket state
- then re-enter storage and reacquire a PG mutex

Even with ordered PG helpers, this is still a misuse-prone design because the
coordinator is composing storage implementation details directly.

## Goal

Replace PG-shaped coordinator access with linear request-scoped capabilities:

1. load a bucket once
2. derive object or multipart access from that bucket capability
3. keep PGs entirely hidden below the storage boundary

The coordinator should no longer be able to reacquire bucket state later in a
request path, and should no longer be able to acquire PGs directly.

This is both a correctness refactor and an abstraction refactor:

- correctness: eliminate late bucket re-entry, duplicate bucket loads, and the
  lock-order/TOCTOU problems that come with them
- abstraction: make PG placement and locking an internal storage concern rather
  than coordinator-visible behavior

## Why This Is Needed

The current design still has these structural problems:

1. PGs are exposed at the wrong level.
   - `server-core` still uses `SharedStorageNode::get_pg(...)`,
     `lock_two_pgs(...)`, `MutexGuard<PgStore>`, and PG-derived helper methods.
   - That leaks storage topology and locking into normal request logic.

2. Bucket state is not a single-use request-scoped handle.
   - a request path can load bucket metadata, then later load bucket policy,
     bucket tags, lifecycle config, or other bucket subresources separately
   - this is bad for locking, performance, and request-local consistency

3. Correctness depends on call-site discipline.
   - the current wrappers reduce mistakes, but they do not make invalid flows
     unrepresentable
   - the coordinator can still load the same bucket twice or re-enter storage
     after object access has started

4. The linear request flow is simpler than the current exposed API.
   - most request paths are:
     - load bucket
     - maybe load one object
     - sometimes load a second object for copy
     - done
   - the public internal API should reflect that shape directly

## Target Model

The new shape should be bucket-first and handle-based.

At a high level:

```rust
let bucket = coordinator.load_bucket(...)?;
let object = bucket.load_object(...)?;
```

or for writes:

```rust
let bucket = coordinator.load_bucket_for_write(...)?;
let prepared = bucket.prepare_put_object(...)?;
```

or for copy:

```rust
let src_bucket = coordinator.load_bucket(...)?;
let dst_bucket = coordinator.load_bucket_for_write(...)?;
let src = src_bucket.load_object(...)?;
let dst = dst_bucket.prepare_copy_destination(...)?;
```

The key rule is:

- a request path gets one bucket handle per bucket involved
- all bucket-derived decisions come from that handle
- all object/multipart access is derived from it
- PGs never appear in coordinator request logic

## Desired Properties

The refactor should make these states impossible or at least structurally
unnatural:

1. loading the same bucket twice in one request path
2. loading bucket tags or policy late after object state is already locked
3. reacquiring a bucket PG from inside authz or object-state helpers
4. passing raw `MutexGuard<PgStore>` or PG IDs around coordinator code
5. calling object-level storage helpers without already holding the relevant
   bucket capability

It should also improve:

1. request-local consistency
   - one bucket load means one coherent bucket view for the whole request
2. performance
   - avoid repeated bucket metadata/subresource fetches in the same request
3. reviewability
   - request code should read as bucket -> object -> action, not PG juggling

## Non-goals

1. do not redesign `storage` around generic traits for their own sake
2. do not collapse bucket/object authorization into storage
3. do not mix this plan with unrelated auth behavior changes
4. do not change external S3 behavior as part of the refactor
5. do not change normal request semantics as part of the handle migration; any
   observable behavior change must be justified independently and pinned
   against AWS outside this plan
6. do not solve all background-sweeper / fanout operations with the same API
   shape if they need a separate internal interface

## Interface Direction

The exact names can change, but the model should likely include request-scoped,
non-cloneable handle types similar to:

- `LoadedBucket`
- `LoadedBucketForWrite`
- `LoadedObject`
- `LoadedObjectVersion`
- `PreparedObjectWrite`
- `LoadedMultipartUpload`

These should be coordinator/storage boundary types, not HTTP-layer types.

In this plan, "handle" means a request-scoped typed object at the
coordinator/storage boundary. It does not mean a generic RAII mutex guard or a
capability-security token. The term is used narrowly for this typed
request-scoped abstraction.

Likely contents of a loaded bucket handle:

- bucket summary / owner / versioning / encryption / ownership-controls state
- bucket policy
- bucket tags
- public access block state
- maybe lifecycle/object-lock config when that request family needs it
- any write reservation / drain state if the request is a write path

The important point is not optional laziness inside the handle. The
important point is that the handle owns the full bucket view needed for the
request family before any derived object or multipart access begins.

For this refactor, that means:

- all bucket state needed by a request path must be derivable at initial bucket
  acquisition time
- that needed state is a function of:
  - request shape / flags
  - and, where required, bucket state itself
- the storage boundary should therefore fetch the full required bucket state in
  one acquisition step

The handle must not perform later lazy bucket-subresource loads after
object or multipart derivation has started. Otherwise it would preserve the
same late re-entry failure mode under a different interface shape.

## Phase 0: Handle Skeleton and Contracts

Before migrating any request family, land the handle skeleton and the contract
decisions this refactor depends on.

This phase should make the following explicit:

1. type-level invariants
   - `LoadedBucket` and related handle types should be non-cloneable
   - the move/borrow story for `load_object(...)` / `prepare_*` derivation
     should be visible in the type signatures
   - the public boundary should ultimately make duplicate bucket acquisition
     within a single request path structurally unnatural
2. lock/snapshot lifetime
   - handles are request-scoped snapshots, not whole-request PG mutex guards
   - long-lived operations such as streaming PUT and multipart complete must
     not hold bucket/object PG mutexes for the duration of the HTTP request
   - writes may validate from the request snapshot and acquire narrower commit
     locks only at publication time
3. dual-bucket ordering
   - the storage boundary must expose a deterministic source/destination
     acquire primitive for copy and `UploadPartCopy`
   - ordering must be storage-owned and role-preserving
4. multipart scope
   - the "load bucket once" rule applies per HTTP request, not per multipart
     upload lifetime
   - each multipart request re-derives its bucket handle from current storage
     state
5. request-family loading policy
   - each request family defines up front the full bucket-state set it may need
   - initial bucket acquisition loads exactly that request-family state set in
     one step
   - late bucket-state expansion is not allowed

Acceptance criteria:

- reviewers can inspect the intended move/borrow/clone story directly in the
  type skeleton
- the request-scoped snapshot contract is written down explicitly
- the dual-bucket ordering primitive shape is explicit before copy migration
- the multipart per-request contract is explicit before multipart migration
- no request-path migration starts before phase 0 is reviewed

Phase 0 status:

- completed
- landed shape:
  - `storage` owns bucket snapshot acquisition through:
    - `SharedStorageNode::load_bucket_snapshot(...)`
    - `SharedStorageNode::load_bucket_snapshot_pair(...)`
  - storage owns dual-bucket ordering and same-bucket collapse for the bucket
    snapshot path
  - storage owns PG topology for the bucket snapshot path via
    `storage::pg_topology::PgTopology`
  - `server-core` owns the semantic request-family adapter and expected-owner
    validation in `coordinator/bucket_handles.rs`
  - the current phase-0 types remain internal scaffolding and do not yet
    migrate real request families
- pinned guardrails:
  - same-bucket pair loads collapse to one underlying bucket handle
  - same-bucket pair loads still validate both expected-owner constraints
  - conditional bucket-tag snapshot loading is tested directly at the storage
    layer for the ABAC-enabled and ABAC-disabled cases
  - a loaded bucket snapshot keeps its pre-mutation bucket-tag view even if the
    bucket is mutated afterwards
- explicit phase-0 deferral:
  - the current generic `BucketHandleLoader` is still a repeat-call internal
    adapter, not the final single-use request-family entry point
  - full type-level duplicate-load prevention is deferred to the migration
    phases where concrete request families stop calling the generic loader and
    instead receive one bucket handle per bucket as part of their operation
    entry path

## Storage Boundary Change

`SharedStorageNode` should stop being a coordinator-facing “get a PG” API.

Instead, storage-facing entry points should be shaped around the logical access
pattern:

- load all required bucket metadata / subresources in one step
- derive object or multipart views from that loaded bucket
- perform publication/commit using prepared write capabilities

The storage layer may still use PGs internally, but that must become an
implementation detail.

That means:

- `get_pg(...)` and `lock_two_pgs(...)` should disappear from normal
  coordinator request logic
- `MutexGuard<PgStore>` should stop appearing in coordinator-facing helper
  signatures outside narrowly-scoped migration shims

## Migration Strategy

This should be done in phases. The success condition is structural, so it is
better to move one request family at a time than to add another thin wrapper
over the current PG API.

### Phase 1: Initial Bucket-Read Migrations

Use the new handle skeleton for the simplest bucket-only read paths.

Start with operations that already have a clean single-bucket flow, for
example:

- `HeadBucket`
- `GetBucketAcl`
- `GetBucketVersioning`
- `GetBucketTagging`
- `GetBucketPolicyStatus`

Acceptance criteria:

- these paths stop calling `get_pg(...)` directly from coordinator request
  code
- bucket policy / bucket tags are loaded through the bucket capability, not by
  separate coordinator helpers

Phase 1 status:

- in progress
- migrated so far:
  - `HeadBucket`
  - `GetBucketAcl`
  - `GetBucketTagging`
  - `GetBucketVersioning`
  - `GetBucketLocation`
  - `GetBucketPolicyStatus`
- phase-1 scope is now effectively covered; the next work is the broader
  phase-2 bucket read/admin/config surface

### Phase 2: Bucket Read/Admin/Config Paths

Expand the bucket handle model across the remaining normal bucket-side read
and admin/config request surface.

This includes bucket-only request families such as:

- `GetBucketPolicy`
- `GetBucketCors`
- `GetBucketEncryption`
- `GetBucketOwnershipControls`
- `GetBucketPublicAccessBlock`
- `GetBucketObjectLockConfiguration`
- `GetBucketLifecycle`
- `GetBucketAbac`
- `ListObjectsV1`
- `ListObjectsV2`
- `ListObjectVersions`
- `ListMultipartUploads`
- `GetBucketLocation`
- any other normal bucket-side read/admin path still using direct PG access

Acceptance criteria:

- all normal bucket-side read/admin/config request paths use the bucket
  handle model
- bucket-only reads no longer have separate ad hoc bucket-loading logic outside
  the capability path
- authz for bucket-only reads/admin paths consumes the loaded bucket handle
  rather than fetching bucket state independently

### Phase 3: Single-Object Read Paths

Move the ordinary read family to the bucket-first shape:

- `GetObject`
- `HeadObject`
- `GetObjectRange`
- `GetObjectAttributes`
- object subresource reads (`GetObjectAcl`, `GetObjectTagging`, object-lock
  reads)

Acceptance criteria:

- request path starts with bucket-handle load, then object derivation
- object read helpers no longer re-enter bucket loading internally
- authz consumes the loaded bucket handle instead of fetching bucket tags or
  policy later

### Phase 4: Single-Object Write/Delete Paths

Move the ordinary write and delete family:

- `PutObject`
- `DeleteObject`
- `PutObjectAcl`
- `PutObjectTagging`
- object-lock mutation paths

This phase should also absorb bucket write reservations into the handle
model instead of leaving them as separate coordinator-side side channels.

Acceptance criteria:

- write paths use a write-scoped bucket handle
- object write preparation is derived from that handle
- no path separately acquires bucket reservation state and then later reacquires
  bucket metadata/subresources

### Phase 5: Bucket Write/Admin/Config Paths

Move the bucket-only mutation and admin/config family to the same bucket
handle model.

Cover the normal request surface such as:

- `PutBucketPolicy`
- `DeleteBucketPolicy`
- `PutBucketAcl`
- `PutBucketVersioning`
- `PutBucketCors`
- `PutBucketTagging`
- `DeleteBucketTagging`
- `PutBucketLifecycle`
- `DeleteBucketLifecycle`
- `PutBucketEncryption`
- `DeleteBucketEncryption`
- `PutBucketPublicAccessBlock`
- `DeleteBucketPublicAccessBlock`
- `PutBucketOwnershipControls`
- `DeleteBucketOwnershipControls`
- `PutBucketObjectLockConfiguration`
- `PutBucketAbac`
- any other ordinary bucket mutation path still using direct PG access

Acceptance criteria:

- bucket-only mutation paths use the bucket handle directly rather than
  independently acquiring bucket PG state
- write reservations / drain handling are integrated into the bucket capability
  model for these operations
- bucket policy / tags / config state needed for authz is part of initial
  bucket acquisition, not loaded later by helpers

### Phase 6: DeleteBucket and Bucket Teardown

`DeleteBucket` should be treated separately from ordinary bucket metadata
updates because it is a teardown operation, not a metadata swap.

Acceptance criteria:

- `DeleteBucket` uses the bucket-first handle model
- bucket teardown has an explicit reservation/drain and cleanup protocol
- `DeleteBucket` is not hidden inside the ordinary bucket-mutation phase

### Checkpoint After Phase 4

After single-object write/delete migration is complete, stop and review whether
the handle design is still correct before continuing into the heavier bucket
mutation, teardown, multipart, and copy phases.

This checkpoint exists because phase 4 is the first point where write
reservations, publication locks, and commit-time sequencing are all exercised
together.

Questions to answer at the checkpoint:

- is the request-scoped snapshot contract still the right model
- is the write-scoped bucket handle shape still correct
- are write reservations integrated cleanly enough into the design
- has any hidden late-load or duplicate-bucket path survived
- do the phase 5+ migrations still look mechanical rather than requiring a new
  redesign

Do not continue into later phases until this review is complete.

### Phase 7: Multipart and Streaming Paths

These are the most sensitive because they were involved in the recent deadlock
shape and still have request-local storage re-entry patterns.

Cover:

- `CreateMultipartUpload`
- `UploadPart`
- `CompleteMultipartUpload`
- `AbortMultipartUpload`
- streaming `PutObject`
- streaming part/session flows

Acceptance criteria:

- multipart request code derives required bucket context once per request
- authz and policy checks do not late-load bucket state from inside multipart
  helpers
- storage/lock correctness is encoded by the capability flow rather than by
  additional local PG-order reasoning

### Phase 8: Copy / UploadPartCopy Dual-Bucket Flows

Copy requires two top-level bucket capabilities and is the main two-bucket
request family.

Cover:

- `CopyObject`
- `UploadPartCopy`

Acceptance criteria:

- source and destination buckets are loaded once each per request
- source object and destination write preparation are derived from those
  handles
- dual-bucket ordering is hidden behind the storage handle layer, not open
  coded in request handlers

### Phase 9: Remove Coordinator PG Surface

Once the migrated families no longer depend on PG-shaped APIs, remove the
remaining coordinator-visible PG access where possible.

Expected end state:

- no normal request path in `server-core` calls `storage_node.get_pg(...)`
- no normal request path in `server-core` calls `storage_node.lock_two_pgs(...)`
- PG guards become storage-internal or narrow migration/test-only details

This phase may still leave test-only direct PG access in some low-level tests,
but production coordinator request code should no longer depend on it.

## Special Cases

Not every operation has the same access pattern. The plan should treat these
explicitly rather than forcing them into the wrong mold.

### Copy

Copy is still linear, but with two buckets:

- load source bucket
- load destination bucket
- derive source object and destination write from them

### Fanout / sweeper / listing internals

Background sweepers and cross-PG fanout operations may need separate storage
entry points. They should not block this refactor, and they do not need to be
shoehorned into the request-scoped bucket/object capability API if that would
make the design worse.

### Bucket cache

If some paths only need a fast bucket view, that optimization should sit behind
the bucket-loading API. It should not remain as a second public coordinator path
that request code can call independently of the main bucket handle.

## Risks

1. A shallow wrapper could preserve the same failure mode.
   - If the new API still lets code “get the bucket again” indirectly, or lets
     a handle lazily fetch bucket state after object/multipart derivation
     has begun, the refactor will not have solved the real problem.

2. Mixing authz and storage migration too loosely could hide semantic changes.
   - This must stay a structural refactor with AWS behavior held constant.

3. Multipart/copy flows may reveal hidden assumptions.
   - That is expected and should be handled by migrating them after the simpler
     single-bucket flows.

4. Upfront bucket-state loading can increase request cost.
   - That cost should be chosen honestly per request family up front, not
     hidden behind later conditional bucket loads.

## Verification

For each phase:

- run `cargo fmt --all`
- run targeted `server-core` tests for the migrated family
- run targeted `s3-tests` for the migrated family
- run `cargo clippy --all-targets --all-features -- -D warnings`

Before the final commit of the refactor:

- run `/bin/bash -lc 'S3_TEST_TIMEOUT_SECS=30 cargo test --workspace --no-fail-fast'`

Specific regression focus:

- request paths that previously loaded bucket state more than once
- multipart and copy paths that previously reacquired bucket/object PGs late
- bucket-policy and ABAC-enabled paths that require bucket tags
- missing-object and version-specific authorization probes

## Success Criteria

This plan is complete when all of the following are true:

1. phase 0 lands before request-family migration begins
2. normal coordinator request code no longer acquires PGs directly
3. bucket state is loaded once per bucket per request path
4. initial bucket acquisition loads all bucket state required by that request
   family before any derived object/multipart access begins
5. object and multipart access is derived from loaded bucket handles
6. late bucket re-entry from authz/object helpers is structurally removed
7. bucket-only read/admin/config and mutation paths are migrated too
8. `DeleteBucket` has an explicit migrated teardown path
9. the current AWS-backed auth behavior remains unchanged
10. the resulting request code reads in logical bucket/object terms rather than
   storage-topology terms
