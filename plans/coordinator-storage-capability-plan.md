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
   - but any bucket write reservation that participates in delete/write
     exclusion must cover the full write action it protects, not just snapshot
     acquisition
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
- write-side reservation lifetime must be storage-owned as well:
  - coordinator should call a storage-owned write-scoped action primitive
  - storage should acquire reservation, load snapshot, run the write action,
    and release reservation as one boundary-owned protocol
  - coordinator must not reconstruct that lifetime by calling a snapshot
    loader and then running the write action afterwards

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

- completed
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
- `GetBucketLocation`
- any other normal bucket-side read/admin path still using direct PG access

Acceptance criteria:

- all normal bucket-side read/admin/config request paths use the bucket
  handle model
- bucket-only reads no longer have separate ad hoc bucket-loading logic outside
  the capability path
- authz for bucket-only reads/admin paths consumes the loaded bucket handle
  rather than fetching bucket state independently

Phase 2 status:

- completed
- summary-backed reads migrated end to end so far:
  - `GetBucketAbac`
  - `GetBucketEncryption`
  - `GetBucketOwnershipControls`
  - `GetBucketPublicAccessBlock`
  - `GetBucketObjectLockConfiguration`
- subresource-body reads now migrated end to end through the bucket handle path:
  - `GetBucketPolicy`
  - `GetBucketCors`
  - `GetBucketLifecycle`
- phase-2 scope is now effectively covered for the normal single-bucket
  read/admin/config paths; the deferred bucket-wide listing and iteration
  paths remain in phase 4b

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

Phase 3 status:

- completed
- migrated so far:
  - `GetObject`
  - `HeadObject`
  - `GetObjectAttributes`
  - `GetObjectAcl`
  - `GetObjectTagging`
  - `GetObjectRetention`
  - `GetObjectLegalHold`
- this also covers the shared ordinary-read authorization path reused by:
  - `GetObjectRange`
  - part-number object reads that flow through the same read authorization
- this also covers the versioned forms that flow through the same read
  authorization paths for ACL/tagging/object-lock reads
- phase-3 scope is now effectively covered for the ordinary single-object read
  family; the remaining old helper usage on tagging/ACL/object-lock writes is
  deferred to later write/delete phases

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

Phase 4 status:

- completed
- migrated so far:
  - `PutObject` authorization now uses a reservation-backed bucket handle
    rather than separate bucket policy/tag loads
  - direct `PutObject` commit and streaming `PutObject` finalize now carry
    lifecycle through the same write-scoped bucket handle rather than doing a
    later bucket lifecycle load from the cache/subresource path
  - `DeleteObject` now uses the ordinary loaded bucket-handle path for bucket
    policy / ABAC inputs
  - the shared mutation helpers for:
    - `PutObjectTagging`
    - `DeleteObjectTagging`
    - `PutObjectAcl`
    - `PutObjectRetention`
    - `PutObjectLegalHold`
    now load bucket policy / ABAC inputs through the loaded bucket-handle path
- remaining old-style reservation / bucket-reload paths are now in later-phase
  multipart/copy flows rather than the ordinary single-object write/delete
  surface

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

This review should be repeated after phase 4c. Phase 4 completed the ordinary
single-object write/delete migration, but write-scoped bucket handle loading
still carries coordinator-visible PG and reservation mechanics. So the current
checkpoint is a first review of the handle shape, not the final review of the
write-side abstraction boundary.

### Phase 4b: Bucket-wide Listing and Iteration Paths

Defer the bucket-wide list/iteration family until after the single-object
read/write shape has been exercised and reviewed.

These operations are bucket-scoped at the API level but they are not the same
shape as the simpler single-bucket metadata reads above, because they iterate
across object state and can touch multiple PGs:

- `ListObjectsV1`
- `ListObjectsV2`
- `ListObjectVersions`
- `ListMultipartUploads`

Acceptance criteria:

- the operation still begins from one bucket handle load
- the bucket handle provides the only bucket-derived auth/policy/config input
- object/list iteration uses storage-owned fanout/iteration primitives rather
  than coordinator-managed PG walking
- the final design is informed by the object-path shape proven in phases 3 and
  4, not guessed earlier from the bucket-only read surface

### Phase 4c: Write-Scoped Bucket Handle Boundary Cleanup

Finish the write-side abstraction boundary before moving on to later bucket
mutation and multipart/copy phases.

Current state after phase 4:

- ordinary single-object write/delete request paths use write-scoped bucket
  handles
- the read-side phase-4 work remains complete; the open follow-up is on the
  write/delete side only
- but the coordinator still owns part of the write-side storage mechanism:
  - `with_bucket_write_handle_for(...)` still acquires bucket write
    reservations
  - it still resolves the bucket PG through `get_bucket_pg_for(...)`
  - `load_bucket_handle_from_reserved_pg(...)` still loads bucket subresources
    directly from a coordinator-visible PG

That means PGs and reservation mechanics are still leaking through the
coordinator boundary on the write path even though the request-family flow is
already bucket-first.

Covered surface:

- ordinary `PutObject`
- direct commit and streaming finalize paths already migrated in phase 4
- the shared single-object write helpers that currently depend on
  `with_bucket_write_handle_for(...)`

Acceptance criteria:

- coordinator no longer calls `get_bucket_pg_for(...)` as part of the
  write-scoped bucket-handle path
- coordinator no longer passes bucket PGs into bucket-handle loading helpers
- reservation acquire/release protocol for write-scoped bucket snapshots is
  storage-owned
- `BucketHandleLoader` remains a semantic adapter, not a storage-mechanism
  owner

Phase 4c status:

- partially completed
- `storage::SharedStorageNode` now owns write-side bucket reservation +
  snapshot loading through `with_bucket_write_snapshot(...)`
- ordinary single-object write/delete paths no longer resolve bucket PGs as
  part of the write-scoped bucket-handle flow
- remaining coordinator-visible bucket PG reads in this area are on older
  bucket-read or multipart/copy paths, not the migrated single-object
  write/delete surface
- follow-up required:
  - `with_bucket_write_snapshot(...)` currently only brackets snapshot loading,
    not the full write action
  - that is not sufficient for delete/write exclusion because bucket delete
    still waits on active write reservations to drain
  - the correct end-state requires a storage-owned write-action primitive whose
    reservation lifetime covers the full protected write action

### Re-Review After Phase 4c

Once phase 4c lands, rerun the phase-4 checkpoint review specifically against
the write-scoped handle boundary.

Questions to re-check:

- does ordinary single-object write/delete flow now avoid all
  coordinator-visible PG access on the bucket side
- is write-side reservation handling now fully hidden behind storage-owned
  bucket snapshot acquisition
- has any new duplicate-load or late bucket-load seam appeared while removing
  the PG leak
- are phases 5+ still mechanical migrations from the resulting boundary

Re-review status:

- reopened
- this reopen applies only to the single-object write/delete half of phase 4
- ordinary single-object write/delete flow now avoids coordinator-visible
  bucket PG access on the migrated bucket side
- write-side reservation handling is not yet correct for the migrated
  single-object surface because reservation lifetime currently covers snapshot
  loading, not the full write action
- this means the re-review remains open until the write-action boundary below
  lands and delete/write exclusion is revalidated

### Phase 4d: Write Reservation Lifetime Correction

Correct the storage boundary introduced in phase 4c so that write reservation
semantics remain unchanged while PGs stay hidden.

The required interface shape is:

- storage owns a write-scoped action primitive
- that primitive:
  - acquires the bucket write reservation
  - loads the requested bucket snapshot
  - constructs the loaded bucket handle
  - runs the protected write action
  - releases the reservation afterwards
- coordinator passes the semantic request and write closure, but does not
  observe or reconstruct reservation lifetime itself

This is intentionally stronger than `with_bucket_write_snapshot(...)`.
Snapshot loading alone is not the contract. The contract is “run this
write-scoped action while the bucket write reservation is held”.

The critical design rule here is stronger still:

- migrated write paths must not allow coordinator to observe, return, or hold
  bucket-side PG guards across this boundary
- if a migrated path still returns `MutexGuard<PgStore>` or otherwise requires
  coordinator-visible PG state to finish the operation, that path is not yet
  actually migrated to the correct write boundary

So phase 4d is not “repair one helper”; it is the point where the write-side
interface becomes operation-shaped enough that coordinator can no longer use
PGs incorrectly by construction.

This suggests a broader transactional pattern too:

- when an operation's authorization depends on mutable object/upload state that
  must stay coherent with the following storage action, storage should own a
  single transaction-shaped primitive
- the coordinator should pass a pure semantic/auth closure into that primitive
  rather than performing one storage lookup for auth and a second storage call
  for the follow-on action
- that primitive should:
  - acquire the relevant PG or reservation once
  - load and validate the mutable state needed for auth
  - invoke the coordinator-provided pure auth/semantic closure
  - if authorized, perform the follow-on read/write action under the same
    storage-owned critical section
  - return only pure result data to coordinator

This is not required for every operation. Bucket-only reads that consume an
already loaded bucket handle do not need it. It is required when splitting auth
and storage work would otherwise introduce a mutable-state TOCTOU gap.

Covered surface:

- the remaining normal production write/delete paths that still rely on
  `with_bucket_write_handle_for(...)` or otherwise split bucket-auth work from
  the later protected write action
- concretely, that means:
  - the single-object write/delete production paths from the write half of
    phase 4 / 4c
  - bucket mutation/admin/config writes from phase 5
  - `DeleteBucket` revalidation in phase 6 against the corrected exclusion
    contract
  - plain streamed `PutObject` in phase 7b
  - any later write-side request family that still uses
    `with_bucket_write_handle_for(...)`

Not in scope:

- read-only phases 1-3
- the completed read side of phase 4
- multipart paths already moved to storage-owned transactional boundaries in
  phase 7, unless a specific remaining production path is found to still
  depend on the older helper

Acceptance criteria:

- storage exposes a write-scoped action primitive, not just a write-scoped
  snapshot loader
- the bucket write reservation lifetime covers the full protected write action
- bucket deletion cannot complete while a protected write action is still
  running
- the delete/write synchronization behavior matches the pre-4c semantics
- coordinator still does not see bucket PGs or reservation mechanics directly
- migrated write paths do not return or retain bucket-side PG guards outside
  the storage-owned write action boundary
- state-coupled object/upload operations use a storage-owned transactional
  primitive with a pure coordinator auth closure where needed, rather than
  separate pre-auth and post-auth storage calls over mutable state

Verification focus:

- targeted race regression proving delete cannot complete while a write action
  blocked inside the new storage-owned write action primitive is still active
- targeted multipart/session regression proving same-PG migrated write paths do
  not deadlock while using the corrected boundary
- rerun ordinary single-object write/delete regressions
- rerun bucket mutation write regressions
- rerun migrated multipart write regressions

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

Phase 5 status:

- reopened, because these normal bucket write/admin/config paths still need
  the phase-4d write-action boundary cleanup
- first migrated slice:
  - `PutBucketCors`
  - `DeleteBucketCors`
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
  - `PutBucketVersioning`
  - `PutBucketObjectLockConfiguration`
- these paths now authorize from a write-scoped loaded bucket handle instead of
  `checked_active_bucket_summary_for(...)` plus later bucket policy/cache
  loads
- bucket policy, ABAC tags, and bucket-state-dependent validation on this slice
  now come from the initial write-scoped bucket acquisition
- second migrated slice:
  - `PutBucketPolicy`
  - `DeleteBucketPolicy`
  - `PutBucketAbac`
  - bucket tag control auth for `TagResource` / `UntagResource`
- bucket-policy management actions in this slice now preserve the existing
  owner-root bypass semantics while moving to the same write-scoped loaded
  bucket handle path
- final migrated slice:
  - `PutBucketAcl`
- structural follow-up required:
  - these paths are still part of the open write/delete security recheck
    because they depend on the older write-scoped helper shape rather than the
    stronger phase-4d write-action boundary
  - some paths may also still rely on later coordinator-visible object/bucket
    PG work after auth/config loading, which phase 4d must drive behind the
    storage-owned write boundary
  - so their bucket-first auth/config shape is migrated, but their final
    delete/write synchronization contract and no-PG-leak boundary are not yet
    settled
- once phase 4d lands, rerun this phase’s migrated write surface against the
  new storage-owned write-action primitive and then re-close phase 5
- `DeleteBucket` remains separate in phase 6 and bucket-wide listing/fanout
  remains deferred to phase 4b

### Phase 6: DeleteBucket and Bucket Teardown

`DeleteBucket` should be treated separately from ordinary bucket metadata
updates because it is a teardown operation, not a metadata swap.

Acceptance criteria:

- `DeleteBucket` uses the bucket-first handle model
- bucket teardown has an explicit reservation/drain and cleanup protocol
- `DeleteBucket` is not hidden inside the ordinary bucket-mutation phase

Phase 6 status:

- reopened, but only because `DeleteBucket` is the consumer of the write
  exclusion contract that phase 4d is correcting
- `DeleteBucket` authorization now uses the write-scoped loaded bucket
  handle path instead of the older validated-summary plus cached-policy path
- storage now owns bucket delete start/finalize protocol for the normal request
  path:
  - `begin_bucket_delete(...)` owns drain start, bucket-wide emptiness scan, and
    `mark_bucket_deleting(...)`
  - `try_finalize_bucket_delete(...)` owns the bucket-wide finalize scan,
    reclaim-root requeue decisions, completed-multipart cleanup, and final
    metadata deletion
- coordinator now only:
  - authorizes `DeleteBucket`
  - clears request-level caches/fast-path entries
  - enqueues deferred finalize work
- follow-up required:
  - phase 6 is structurally correct on the delete side, but it must be
    revalidated once phase 4d re-establishes the intended write/delete
    exclusion contract
  - this does not mean the delete path itself needs a second architectural
    rewrite; it means the delete/write race regressions need to be rerun
    against the corrected write-side boundary

### Phase 7: Multipart Paths

These are the most sensitive because they were involved in the recent deadlock
shape and still have request-local storage re-entry patterns.

Cover:

- `CreateMultipartUpload`
- `UploadPart`
- `CompleteMultipartUpload`
- `AbortMultipartUpload`
- streaming part/session flows

Acceptance criteria:

- multipart request code derives required bucket context once per request
- authz and policy checks do not late-load bucket state from inside multipart
  helpers
- storage/lock correctness is encoded by the capability flow rather than by
  additional local PG-order reasoning

Phase 7 status:

- completed
- migrated so far:
  - `CreateMultipartUpload`
  - `UploadPart`
  - `CompleteMultipartUpload`
  - `BeginStreamPart`
  - `ListParts`
  - `AbortMultipartUpload`
- these now use the write-scoped bucket handle path for bucket policy / ABAC
  inputs instead of the older reservation-only summary path
- `BeginStreamPart` is now the first multipart/session write path moved onto
  the corrected phase-4d shape:
  - the protected action runs inside the storage-owned write reservation
  - coordinator no longer receives or returns a metadata PG guard for this path
  - multipart upload lookup and stream-session creation stay inside the
    storage-owned object-PG step
- `CompleteMultipartUpload` now matches the same corrected boundary rule for
  the multipart completion path:
  - coordinator no longer drives metadata-PG preparation or manifest commit
    directly
  - requested-part snapshot loading and final multipart manifest commit are
    storage-owned object-PG operations
  - version/generation allocation and stale-payload metadata cleanup now stay
    inside the storage-owned completion commit step
- `ListParts` now matches the same coordinator/storage boundary rule for the
  multipart management read path:
  - coordinator no longer receives or returns a metadata PG guard
  - storage now holds the object-PG step once, invokes a pure auth closure,
    and lists parts under that same storage-owned critical section
  - unauthorized callers do not force part-row materialization before denial,
    and the upload-state recheck and part listing stay in one transaction
- `AbortMultipartUpload` now matches the same rule for the multipart cleanup
  path:
  - coordinator no longer performs multipart metadata PG orchestration
  - shard cleanup and multipart metadata deletion are owned by a single
    storage-side abort operation
  - the normal request path no longer carries abort-specific PG/shard deletion
    mechanics in `runtime.rs`
- `CreateMultipartUpload` now matches the same corrected boundary rule for the
  multipart create path:
  - coordinator no longer performs direct object-PG lookup or multipart row
    creation
  - storage owns the write reservation, bucket snapshot load, current-object
    lookup, and multipart row creation in one transaction
  - coordinator contributes a pure auth/semantic closure over the loaded
    bucket handle and existing live object
- follow-up required on migrated slice:
- any migrated multipart/session path that still returns coordinator-visible
  PG guards is only partially migrated and must be redesigned so storage owns
  the protected write action end to end
- the intended multipart surface is now covered:
  - `CreateMultipartUpload`, `BeginStreamPart`, `UploadPart`,
    `ListParts`, `AbortMultipartUpload`, and `CompleteMultipartUpload`
    now have corrected storage-owned object-PG boundaries on their normal
    request path
- any newly discovered multipart/session helper outside that set should be
  treated as a regression against this phase
- the remaining write/delete security issue has already been addressed on the
  multipart surface by moving these paths onto storage-owned transactional
  boundaries
- only a newly discovered multipart production path still using the older
  helper shape would justify reopening part of phase 7

### Phase 7b: Plain Streaming PutObject Path

This is session-shaped but not really multipart, so it should not stay mixed
into the multipart closeout.

Cover:

- streaming `PutObject`
- any remaining plain stream-upload session lifecycle helpers

Acceptance criteria:

- the plain streamed `PutObject` path follows the same corrected phase-4d
  write-action boundary as the multipart/session paths
- coordinator does not receive or return PG guards on the normal streamed
  `PutObject` flow
- auth, session creation, append, finalize, and abort semantics are preserved
  while moving any remaining storage critical sections behind storage-owned
  operations

Phase 7b status:

- open
- deferred out of multipart phase 7 so multipart can be treated as complete on
  its own terms

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
- no migrated normal request path in `server-core` receives or returns
  `MutexGuard<PgStore>`
- PG guards become storage-internal or narrow migration/test-only details

This phase may still leave test-only direct PG access in some low-level tests,
but production coordinator request code should no longer depend on it.

### Phase 10: Fast-path Review and Realignment

After the main request-family migrations land, do a deliberate pass over the
bucket fast path and decide which parts of the bucket-first handle model should
participate in it.

This phase is about cache contents and cache usage policy, not about changing
external S3 semantics. The main questions are:

- whether additional bucket state should live in the fast path for correctness
  or performance
  - especially bucket tags when bucket ABAC is enabled
  - and any other bucket subresources that migrated request families now need
    repeatedly
- which request families should be allowed to satisfy their bucket-handle load
  from warm fast-path state
- which request families should always force a real bucket snapshot load even
  when fast-path metadata is warm
  - for example, when they require a true policy snapshot rather than a cache
    hint
- whether any existing fast-path reads should be narrowed because they weaken
  the request-scoped snapshot contract

This review should cover at least:

- ordinary object reads
- bucket-policy-dependent reads
- bucket ABAC-enabled paths
- bucket-only read/admin paths that now consistently require the same
  subresources

Acceptance criteria:

- the intended post-refactor fast-path contract is written down explicitly
- any additional bucket state added to the fast path is justified and pinned by
  tests
- any request family that uses the fast path preserves the request-scoped
  snapshot guarantees established by the handle model
- any request family that cannot preserve those guarantees is documented as
  requiring a real bucket snapshot load

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
