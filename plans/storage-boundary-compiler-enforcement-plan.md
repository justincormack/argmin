# Storage Boundary Compiler-Enforcement Plan

Status: active — Phases 0–2 complete; Phase 3 in progress

## Goal

Replace the storage-cluster boundary's growing set of source-text checks with
Rust module, crate, type, and capability boundaries wherever possible.

`scripts/check-storage-cluster-boundaries` remains transitional scaffolding:
repair it first, run it in CI, and retire each section only after the
corresponding invalid call path is impossible to express or is covered by a
smaller semantic check.

The end state should make it impossible for production request code to:

- bypass cluster routing and perform raw storage-node shard I/O
- access `PgStore` or `PgMetadataStore` directly
- use the wrong PG role for an operation
- start new work without an appropriate current-route capability
- publish a metadata command through a retry/convergence path with the wrong
  semantics
- acquire payload access without the required lease/read-handle capability
- compile production code that depends on test-only mutation hooks

## Motivation

The current boundary check is valuable but too brittle to be the primary
enforcement mechanism:

- it is approximately 2,800 lines of shell, AWK, and regular expressions
- it tracks Rust function context without understanding the Rust syntax tree
- file moves and nested test modules create false production matches
- wrapper renames make semantic inventories appear to change
- explicit allowlists drift after otherwise valid architectural changes
- it is not currently invoked by `scripts/ci`, so ordinary full test runs do
  not detect that drift

The present failure illustrates all of these weaknesses. The June 16 splits of
the local cluster tests and node-client implementation invalidated path-based
assumptions made by several scanners. Later multipart, payload-repair,
bucket-delete, reclaim, and bounded-retry changes made explicit inventories
stale. Review of the reported production matches did not find an actual
boundary bypass: they were tests, the sanctioned local node-client adapter, or
new placed-I/O and metadata-client paths.

This is the same class of maintenance addressed by the earlier
`Refresh storage boundary classifications` change. Fixing the inventories
again is necessary, but does not prevent recurrence.

## Existing invariants

This plan preserves the behavioral requirements in
`guides/storage-cluster-invariants.md` and
`guides/metadata-command-stream.md`:

- metadata and payload operations route through the cluster map
- only active routes serve new work
- retained routes authorize only the explicitly supported recovery and cleanup
  operations
- payload bytes use placed I/O and appropriate leases/read handles
- command-owned metadata changes only through the metadata-command stream
- pending-command conflict handling follows the publisher's documented
  snapshot/retry/convergence class
- test hooks are unavailable to normal production builds

Moving an invariant out of the boundary script does not weaken it. Every
retired textual check must have a named structural replacement.

## Current architectural weakness

The `storage` crate currently contains all of these layers:

1. raw `PgStore` and shard-file implementation
2. `SharedStorageNode`
3. local and Unix node-client adapters
4. cluster routing and placement
5. metadata-command publication and convergence
6. the public storage facade consumed by server-core and the binary

Several raw APIs are only `pub(crate)`, but crate-wide visibility still permits
unrelated production modules to call them. The crate also publicly exports
`PgStore`, `SharedStorageNode`, `LocalStorageNode`, `ShardStore`, and
`StorageNode`, although production consumers mostly need `StorageCluster` and
runtime/bootstrap entry points.

The node-client traits provide a useful abstraction boundary, but many methods
still accept a bare `PgId`. Existing `BucketPgId`, `ObjectMetadataPgId`, and
`DataPgId` newtypes are therefore not yet enforcing the intended PG role at
all call sites.

## Target boundary

### Public production surface

Server-core should see only the cluster-level storage facade and the response
and capability types it needs. It must not be able to name:

- `PgStore`
- `PgMetadataStore`
- raw shard stores
- `SharedStorageNode`
- local node-client implementations
- pending-slot installation helpers

The application binary may construct a storage-node server and a
`StorageCluster`, but should do so through bootstrap/server constructors rather
than opening `SharedStorageNode` directly.

### Node implementation boundary

Raw metadata and shard methods should be private to the node implementation
and its local client adapter. The adapter implements the same node-client
traits used by the Unix client; cluster code receives only those traits.

This can initially be enforced with module layout:

```text
storage
├── node_runtime                 private raw implementation
│   ├── pg_store
│   ├── shared_node
│   └── local_client_adapter     descendant allowed to use raw APIs
├── node_client                  crate-private interfaces and Unix client
├── cluster                      depends only on node_client interfaces
└── bootstrap                    assembles local or Unix-backed runtime
```

Private methods can then be visible to descendants of `node_runtime` without
being visible to the sibling `cluster` module. Avoid `pub(crate)` on raw
methods when a narrower module visibility is possible.

### Optional crate boundary

If the module arrangement remains awkward or cyclic, split the layers into
internal workspace crates:

```text
storage-protocol/types
        ↑              ↑
storage-node      storage-cluster
        \              /
         storage-runtime/bootstrap
```

Both node and cluster depend on the protocol/types crate. The node implements
the client protocol; the cluster can no longer depend on the node
implementation. Server-core depends only on the cluster facade.

This is a decision point, not an initial requirement. Prefer the smaller module
visibility change if it produces a clear, non-cyclic boundary. No new external
production dependency is required by either design.

## Type and capability model

### PG-role types

Replace role-specific bare `PgId` parameters with:

- `BucketPgId`
- `ObjectMetadataPgId`
- `ObjectMetadataScanPgId`
- `DataPgId`

Generic administrative operations may continue to use `PgId` where the PG
role is genuinely unconstrained. Conversions back to bare `PgId` belong at the
node/RPC boundary, not throughout cluster request code.

The role wrappers must not be forgeable convenience newtypes. Their existing
public `new(PgId)` constructors must become private to the trusted
topology/routing modules (or at most visible to the private protocol
validation layer), and there must be no public `From<PgId>` conversion into a
role-specific type. Trusted construction is:

- `BucketPgId` from the configured bucket-placement function applied to the
  request's validated bucket name
- `ObjectMetadataPgId` from the configured object-placement function applied
  to the validated `(bucket, key)`
- `ObjectMetadataScanPgId` from an object-metadata PG present in the installed
  topology; unlike the exact-object role, listing fan-out must scan every such
  PG and therefore cannot validate the PG against one `(bucket, key)`
- `DataPgId` from the payload placement result for the validated segment or
  shard request

RPC wire decoding must not confer a role merely by wrapping a received integer.
Decode the raw wire value first, validate it against the request identity and
the installed topology/placement, and only then construct the role-specific
value inside the trusted module. Tests may use explicit test-only constructors;
production callers do not receive an escape hatch.

### Route capabilities

Introduce private-field capability values representing validated authority:

- `ActivePgRoute` for new work under the current route
- `RetainedCleanupRoute` for explicitly allowed historical cleanup/release
- a narrower recovery capability if command recovery requires different
  authority from cleanup

Only the routing layer may construct these values. Low-level client operations
that can create or mutate durable state require the appropriate capability
rather than accepting only a PG ID and epoch.

Route capabilities are request-scoped authority, not cacheable topology
snapshots:

- local capability types are non-`Clone`, non-`Copy`, have private fields, and
  borrow or own the request's route-admission guard
- their lifetime is tied to that guard so they cannot be stored in a
  long-lived cluster object or durable record
- a request may borrow one capability for several ordered calls, but each
  durable effect rechecks the operation class, route identity/incarnation, and
  absolute validity deadline while the admission guard prevents a replacement
  runtime map from publishing
- passing the deadline invalidates the capability even if the request still
  owns its guard
- APIs that transfer or finish one-shot authority may consume a narrower
  capability; ordinary multi-step request scopes borrow the non-cloneable
  capability rather than manufacturing copies

Capabilities are not serialized as trusted Rust values. A Unix request carries
only route evidence. The storage-node server acquires its own request-scoped
route-admission guard, validates the supplied route identity, deadline,
command/cleanup reference, and operation class against installed state, and
constructs a server-local capability immediately before the operation.

Retained-route authority is narrower than active authority. It names the exact
historical route plus the cleanup, release, or recovery subject that justified
using it; it cannot call new-work APIs. It is valid only while the current
runtime map still retains and authorizes that exact subject, and is revalidated
at every durable effect just like active authority.

### Payload capabilities

Continue the existing direction established by `ObjectPayloadLease` and shard
read handles:

- selected payload snapshots carry their generation lease
- shard reads require a shard-scoped read handle or a validated internal
  handoff that owns one
- physical deletion requires an exact reclaim/fence capability
- best-effort cleanup receives a separate retained-route cleanup capability;
  it must not reuse a normal active-write API with errors discarded

Raw shard locations and keys alone must not be sufficient to invoke production
read or delete operations.

### Metadata publisher classes

Represent the command-stream classifications in Rust:

- `SnapshotSensitive`
- `ApplyValidated`
- `AllocatorCleanup`
- `TerminalSessionRetry`
- `MatchingOutcomeRetry`

The exact representation can be an enum carried by a private publish intent,
separate publisher types, or command/publisher traits. The important
properties are:

- raw pending-slot installation helpers are private
- a publisher selects one typed path
- install collision and terminal outcomes are represented by exhaustive enums
- snapshot-sensitive paths must return to a fresh-snapshot loop after
  unrelated contention
- terminal/matching paths can preserve an equivalent pending command only
  through the typed API that defines that behavior
- adding a publisher or classification requires an exhaustive Rust match or
  trait implementation, rather than editing a shell count

Classification belongs to the publisher operation, not blindly to the command
variant: the same low-level command shape may be used by operations with
different restart or convergence ownership.

Before typed publication is introduced, establish one authoritative
classification registry. Use a unique `PublisherId` Rust enum with an
exhaustive mapping to command kind, canonical publisher name, and
`PublisherClass`. A publisher ID can occur only once in that mapping, so
duplicate classifications are rejected structurally. Production publisher
entry points reference their ID, and a test/check verifies that every
discovered publisher has exactly one ID and every registered ID has a live
publisher. The guide table is generated from, or mechanically checked against,
this registry. Symbol/helper counts are discovery evidence, not classification
authority.

The existing duplicate for `establish_multipart_completion_barrier` is resolved
as `AllocatorCleanup`: it owns monotonic allocator state under an external
completion reservation, must allocate a fresh barrier after contention, and
must not treat another request's barrier as proof. It is not `ApplyValidated`,
because the applied barrier command does not carry and revalidate the
reservation identity.

## Work plan

### Phase 0 — restore the transitional guardrail

1. Fix production-file discovery:
   - exclude `*/tests/*` and `*_tests.rs` consistently
   - distinguish the sanctioned local node-client adapter from bypassing
     production callers
   - remove assumptions tied to the former monolithic file layout
2. Fix function attribution where nested helper functions confuse the AWK
   tracker.
3. Canonicalize implementation wrappers to semantic publisher names, for
   example:
   - `create_multipart_upload_inner` → `create_multipart_upload`
   - `commit_stream_segment_append_with_work_budget` →
     `commit_stream_segment_append`
4. Add the authoritative, duplicate-rejecting `PublisherId` classification
   registry:
   - classify every current publisher exactly once
   - make the guide table generated from or mechanically checked against it
   - require each discovered production publisher to reference one registered
     ID
   - reject registered IDs with no live publisher
5. Audit and refresh the payload, raw metadata method, publisher, and conflict
   inventories against the current guides. Do not mechanically accept current
   output without classifying each production match.
6. Run the repaired check from `scripts/ci`.
7. Add a short comment to each inventory explaining whether it is:
   - a temporary migration prohibition
   - a public-surface inventory
   - a semantic classification that Rust does not yet enforce

Completion:

- `./scripts/check-storage-cluster-boundaries` passes
- `scripts/ci` invokes it
- nested test-file changes do not alter production inventories
- every production publisher has one authoritative classification
- duplicate or missing publisher classifications fail a Rust test/check
- the guide and canonicalized publisher inventory agree with the registry

Implementation update (2026-07-19):

- production scans now consistently exclude nested `tests/` directories and
  `*_tests.rs`, and recognize `node_client/local.rs` as the sanctioned local
  adapter
- publisher discovery uses one shared Rust-literal/comment-aware balanced-scope scanner
  for both helper calls and live markers; a fixture proves arbitrarily named
  inline `cfg(test)` modules and sanctioned `test-hooks` items cannot affect
  either inventory, even with unmatched braces in comments or literals, while
  comma-terminated test-only fields and enum variants do not corrupt later
  function attribution, and an `any(test, feature = "live-feature")` publisher
  remains production-visible
- top-level function attribution no longer lets nested helpers replace their
  owning production method
- the pending-publisher and conflict inventories canonicalize implementation
  wrappers and generic signatures to semantic publisher names
- `MetadataCommandPublisherId` is the authoritative, duplicate-rejecting Rust
  registry; every live publisher entry point carries a marker, the boundary
  check rejects missing/dead markers, and a Rust test mechanically compares
  the registry with the guide table
- the refreshed inventories separately identify temporary migration
  prohibitions, public raw surfaces, and semantic checks not yet enforced by
  Rust
- the audit found and classified the previously omitted
  `delete_bucket_from_acting_set` / `DeleteFinalizedBucket` publisher as
  `SnapshotSensitive`
- `scripts/ci` now runs `scripts/check-storage-cluster-boundaries`
- validation passed the boundary check, strict workspace Clippy, the focused
  registry/guide test, and all 7,091 workspace tests

### Phase 1 — reduce the public storage surface

1. Inventory external production users of:
   - `SharedStorageNode`
   - `LocalStorageNode`
   - `PgStore`
   - `ShardStore`
   - `StorageNode`
2. Add bootstrap/server constructors so `argmin-s3` does not open
   `SharedStorageNode` directly.
3. Stop publicly re-exporting raw node/store types and traits.
4. Make raw modules private where possible.
5. Keep test construction available through unit tests or dedicated
   test-support APIs, not the production public surface.

Completion:

- server-core cannot import raw node or PG-store types
- the application binary uses only bootstrap/server and cluster constructors
- public documentation exposes the cluster facade rather than storage-engine
  internals
- boundary-script public-API inventories covering these exports are removed

Implementation update (2026-07-19):

- the external-user inventory found one production raw-node consumer: the
  control-plane-managed startup heartbeat path in `argmin-s3`; server-core and
  the other workspace crates did not consume raw node, PG-store, or shard-store
  types
- `StorageNodeBootstrap` now owns the pre-bind data-directory lock,
  incarnation advance, raw-node open and recovery, startup heartbeat
  construction, refreshed process-config construction, and durable runtime-map
  persistence; consuming finalization returns a one-shot prepared server that
  keeps the exact persisted process config coupled to the data-directory guard
  until bind, while `argmin-s3` receives only the heartbeat, prepared server,
  and incarnation
- the raw `node`, `pg_store`, and `traits` modules are private, and
  `SharedStorageNode`, `LocalStorageNode`, `PgStore`, `ShardStore`, and
  `StorageNode` are no longer publicly re-exported
- the local-cluster raw-node accessors are crate-private and `LocalNodeStore`
  is no longer publicly exported, so external request code cannot recover a
  raw node through the public cluster facade
- crate documentation and the recovery guide now present `StorageCluster`,
  `StorageNodeBootstrap`, and `StorageNodeServer` as the production boundary
- methods used only by raw-engine tests or awaiting Phase 2 migration remain
  explicitly dead-code-tolerant inside the three private implementation
  modules; this avoids reopening the public surface while preserving the
  staged internal-boundary work
- the metadata-method checker inventory was reclassified from a public-surface
  inventory to a temporary internal migration prohibition: module privacy now
  enforces the external boundary, while the scan still prevents sibling-module
  bypasses until Phase 2 changes the internal layout
- focused bootstrap coverage proves the initial heartbeat is built without
  exposing the node, the control-plane runtime map is converted and persisted,
  the prepared server actually binds the exact persisted configuration, and a
  restart advances incarnation while using and binding the persisted epoch and
  PG observation; a separate regression rejects binding an older config after
  a newer runtime configuration has been persisted
- initial validation passed the storage boundary check, strict workspace
  Clippy, and all 7,104 workspace tests
- after the one-shot prepared-bind review correction, the focused storage and
  application bootstrap/bind regressions, boundary check, and strict workspace
  Clippy passed; a full 7,105-test run reached 3,313 passes before two untouched
  authorization-model matrix tests failed under unusually high machine load,
  and both failures passed together on an isolated rerun

### Phase 2 — enforce the internal node boundary

1. Move the local client adapter under the private node-runtime implementation,
   or choose the internal crate split if module privacy cannot express the
   boundary cleanly.
2. Narrow raw shard and metadata methods from `pub(crate)` to private or
   descendant-only visibility.
3. Make `StorageCluster` hold node-client interfaces/factories, never a
   `SharedStorageNode` reference.
4. Route local and Unix operation paths through the same client contracts.
5. Preserve test coverage for local/Unix parity and runtime-map refresh.

Completion:

- cluster/request code cannot compile a raw shard-file or `PgStore` call
- only the local adapter and storage-node server can reach raw node operations
- direct shard-I/O and direct `PgMetadataStore` textual scans are retired

Implementation update (2026-07-19):

- `LocalNodeStore` no longer stores or constructs an
  `Arc<SharedStorageNode>`. It owns an opaque `LocalNodeRuntime`, created by
  the node implementation, plus only the same node-client trait objects used
  by Unix-backed routes.
- `LocalNodeRuntime` is the sole cluster-side production factory for the
  concrete local adapter. It returns a bundle of trait objects, so cluster
  construction cannot obtain or name `LocalStorageNodeClient`.
- the remaining startup recovery, process-local identity, topology lookup,
  and erasure-code encoding calls are explicit narrow runtime capabilities;
  production cluster/request code has no raw-node accessor. Existing raw
  inspection remains available only under `cfg(test)` or `test-hooks`.
- direct raw-node calls in request operations were replaced by the test-only
  accessor, preserving the internal fault-injection and durable-state tests
  without exposing that path in production builds.
- moving `node_client/local.rs` alone under `node` was not a sound boundary:
  it depends on the shared client snapshot/validation helpers, while the
  storage-node server also needs the concrete adapter and raw node. Instead,
  the raw node engine, client implementation, storage-node server, `PgStore`,
  and raw storage traits now share one private `node_runtime` parent. Narrow
  facade modules preserve the established `node_client` contracts and public
  `storage_node_server` path without exporting implementation types.
- `LocalStorageNodeClient` is visible only to descendants of
  `node_runtime`. Production cluster code can name the local/Unix client
  traits and value contracts, but cannot name the concrete local adapter,
  `SharedStorageNode`, `PgStore`, or `PgMetadataStore`. Raw engine/store
  exposure remains available only to crate tests and `test-hooks`.
- the raw cluster-node, raw shard-file, migrated shard-owner, and production
  `PgMetadataStore` allowlist scans were retired. Their boundary property is
  now enforced by Rust module visibility. The client-level placed-delete and
  placed-read caller inventories remain until Phase 3 supplies the
  corresponding capabilities; other remaining checker inventories likewise
  retain semantic checks that module privacy cannot express.
- the transitional boundary check, strict all-target/all-feature Clippy, the
  2,141-test default-feature `storage` suite, and the full 7,106-test nextest
  suite pass for this slice. The explicit default-feature run prevents
  workspace feature unification from masking internal test-facade gaps.

Retired migration checks and their compiler replacements:

| Retired check | Replacement |
| --- | --- |
| `production_cluster_direct_local_node_bypasses` | `SharedStorageNode` is reachable only through the private `node_runtime::engine` subtree, with test-only facade exposure. |
| `direct_shard_matches` | Raw shard-file methods are on the private engine; cluster code can name only placed-shard client contracts. |
| `migrated_storage_node_bypasses` | `LocalNodeStore` owns an opaque `LocalNodeRuntime` and trait objects, with no production raw-node accessor. |
| `production_metadata_methods` | `PgStore` and `PgMetadataStore` are private `node_runtime` implementation details and are facade-exposed only to tests or `test-hooks`. |

### Phase 3 — adopt role-specific PG IDs and route capabilities

1. Classify every node-client method by bucket, object-metadata, data, or
   genuinely generic PG role.
2. Restrict role-specific PG constructors to trusted topology/routing
   validation and remove public raw-`PgId` construction/conversions.
3. Change trait and request signatures to the appropriate newtype.
4. Add non-cloneable request-scoped active, retained-cleanup, and recovery
   route capabilities with private constructors and admission-guard-bound
   lifetimes.
5. Require these capabilities on stateful node-client operations and
   revalidate identity, operation class, and deadline at each durable effect.
6. Validate raw serialized route evidence and construct a server-local
   capability at the Unix server boundary.
7. Add local and Unix adversarial tests for:
   - capability use after deadline expiry
   - capability reuse after a runtime-map transition
   - retained capability use for a different command, cleanup subject, or
     operation class
   - a decoded PG ID that does not match the request's bucket/object/placement
8. Remove now-redundant epoch/PG argument combinations that can represent
   contradictory state.

Completion:

- production callers cannot forge role-specific PG IDs from a bare `PgId`
- wrong-role PG calls fail at compile time
- new work cannot call a stateful client method without active-route authority
- cleanup/recovery cannot accidentally use normal new-work authority
- stale, expired, transitioned, or subject-mismatched capabilities fail without
  mutation on both local and Unix paths
- route and PG-role grep checks are retired or reduced to API-surface checks

Node-client role classification (2026-07-19):

| Interface | PG role | Capability direction |
| --- | --- | --- |
| `BucketMetadataNodeClient` | bucket metadata | active bucket route; reads may later receive a read-only projection |
| `BucketWriteReservationNodeClient` | bucket metadata | active bucket route for acquire/heartbeat; retained subject-bound cleanup for release/drain recovery |
| `ObjectGenerationMetadataNodeClient`, `ObjectVersionMetadataNodeClient`, `DirectPutMetadataNodeClient` | object metadata | active object route, with completion-specific admission represented as a narrower operation class |
| `ObjectListingMetadataNodeClient` | object metadata scan | active object-metadata route for the scanned PG; listing fan-out constructs one capability per routed PG |
| `ObjectMutationMetadataNodeClient` | object metadata | active object route; stale-payload cleanup requires a separate retained cleanup authority |
| `ObjectReadMetadataNodeClient` | object metadata | active object route plus the existing subject identity/read lease |
| `PlacedShardNodeClient`, `ShardAckNodeClient` | data | active data route; historical inspection/deletion requires retained data cleanup authority |
| `ShardScavengerNodeClient` | data for shard rows/files, generic metadata PG for durable observation rows | active or retained authority according to the scanned location; observation publication is primary-only |
| `ShardReadHandleNodeClient` | data locations carried in the handle request | active/retained authority is inherited from each validated `ShardLocation`; the lease itself remains non-cloneable |
| `ObjectPayloadLeaseNodeClient` | object subject rather than a caller-supplied PG | subject-bound payload lease/reclaim capability; placement is validated when shard locations are acquired |
| `MetadataCommandNodeClient` | genuinely generic metadata PG | active publisher authority, retained recovery authority, or peering/transfer authority selected from the command/recovery operation class |
| `StorageNodeClient` | transitional mixed aggregate | must not preserve raw role-specific duplicates; split/delegate to the typed interfaces before Phase 3 completes |

The generic classifications are intentional. A metadata command can target a
bucket or object PG, and recovery/transfer operates on a PG before a request
bucket/object identity exists. Those calls require a route capability and
command/operation binding rather than a falsely specific PG-role wrapper.

Implementation updates (2026-07-19):

First Phase 3 slice:

- `BucketPgId` and `ObjectMetadataPgId` now live inside the private
  `node_runtime` boundary with private fields and no raw public or
  crate-visible constructor. The freely constructible public `PgTopology`
  remains a placement-only calculator returning raw PG numbers; only an
  installed `LocalNodeRuntime` or `SharedStorageNode` can mint role IDs.
  Unit tests retain an explicit `BucketPgId::new_for_test` escape hatch under
  `cfg(test)` only.
- every `BucketMetadataNodeClient` method, including the duplicate methods on
  the transitional `StorageNodeClient` aggregate, now requires `BucketPgId`.
  The local adapter erases the role only when entering the private raw node,
  and the Unix adapter erases it only while encoding RPC route evidence.
- the Unix server constructs its own bucket role only after validating the
  raw route evidence. This work exposed and closed missing bucket-placement
  validation on bucket-head and create-bucket-command-build RPCs; both had
  previously checked route existence without proving that the request PG was
  the bucket's routed primary.
- an adversarial Unix regression uses the test-only constructor to submit a
  configured but wrong bucket PG and requires both head and create-command
  build to fail before node access.
- review correction: the initial implementation incorrectly exposed role
  constructors on public `PgTopology`, which allowed an arbitrary singleton
  topology to mint either role. Those constructors were removed; external
  placement-only callers, including the PG-backfill UAT, use `bucket_pg_for`
  and `object_pg_for` instead.
- the corrected slice passed the boundary checker, formatting, workspace-wide
  strict Clippy, the PG-backfill UAT build, the focused wrong-bucket-PG Unix
  regression, and the full workspace suite (7,106 tests).

Second Phase 3 slice:

- `BucketWriteReservationNodeClient` and its duplicate methods on the
  transitional `StorageNodeClient` aggregate now require `BucketPgId`.
  Cluster callers obtain the role from their installed runtime map after
  selecting the existing active or retained route. The Unix adapter erases
  the role only into raw RPC route evidence, and the server reconstructs it
  only after the applicable existing route, primary, and bucket-placement
  checks pass.
- this role conversion deliberately does not collapse active new-work
  authority and retained cleanup/recovery authority. Acquire, heartbeat,
  release, drain recovery, delete-finalizer, and lifecycle-sweep operations
  preserve their previous route-validation modes; the later request-scoped
  capability slice must make those modes distinct in the type system.
- the adversarial Unix regression now also submits a test-forged, configured
  but wrong bucket PG to reservation acquire, requires failure, and verifies
  that neither the correct nor wrong PG gained a reservation. Existing
  retained proof-release wrong-PG and non-primary regressions remain in place.
- the second slice passed the focused active/retained Unix regressions, the
  boundary checker, formatting, workspace-wide strict Clippy, and the full
  workspace suite (7,126 tests).
- `DataPgId` construction, the remaining object/data client signatures, and
  request-scoped route capability values remain open in Phase 3.

Third Phase 3 slice:

- `ObjectGenerationMetadataNodeClient`,
  `ObjectVersionMetadataNodeClient`, and their duplicate methods on the
  transitional `StorageNodeClient` aggregate now require
  `ObjectMetadataPgId`. Cluster callers derive the role for the exact
  `(bucket, key)` from the installed runtime topology before selecting or
  invoking a node client.
- local adapters erase the role only at the private raw-node boundary. Unix
  adapters erase it only into serialized route evidence; the Unix server
  reconstructs its own role from the installed topology after the existing
  route and object-placement checks succeed. Acting-set version allocation
  now also derives both its selected PG and client argument from the same
  typed role.
- the test-only object-role constructor is confined to `cfg(test)`. An
  adversarial Unix regression uses it to submit a configured but wrong object
  PG, requires both generation-reservation lookup and version allocation to
  fail at the server boundary, and verifies a reservation seeded on the
  correct PG remains readable through a correctly routed canary.
- direct PUT, object listing/mutation/read, and data-client signatures,
  `DataPgId` construction, and request-scoped route capability values remain
  open in Phase 3.
- the third slice passed its focused local/Unix object-generation and
  object-version tests, the boundary checker, formatting, workspace-wide
  strict Clippy, and the full workspace suite (7,132 tests).

Fourth Phase 3 slice:

- `DirectPutMetadataNodeClient`, its duplicate snapshot method on the
  transitional `StorageNodeClient` aggregate, and
  `BuildDirectPutCommitCommandReq` now require `ObjectMetadataPgId`. This
  closes both raw-PG entry points: snapshot loading accepted a direct
  argument, while command construction previously carried a raw PG inside its
  request value.
- direct PUT finalization derives one object role for the exact
  `(bucket, key)` from the installed runtime topology and uses its raw
  projection only for generic pending-command routing. Local command-ID
  allocation erases the role at the private store boundary; Unix request and
  response validation erase it only at the RPC boundary.
- the Unix server reconstructs the direct-PUT object role only after route,
  primary, and exact object-placement validation. The two-PG adversarial
  regression now also requires a test-forged wrong object PG to fail for both
  direct-PUT snapshot loading and command construction, while a correctly
  routed snapshot canary succeeds.
- review correction: the adversarial fixture seeds the same generation
  reservation on both PGs and requires the exact `PayloadDecode` placement
  error. An implementation that reached the wrong PG would therefore succeed
  at snapshot lookup and proceed with command construction rather than
  satisfying the test through an unrelated missing-reservation error.
- object listing/mutation/read and data-client signatures, `DataPgId`
  construction, and request-scoped route capability values remain open in
  Phase 3.
- the fourth slice passed its focused direct-PUT and wrong-object-PG Unix
  regressions, the boundary checker, formatting, workspace-wide strict
  Clippy, and the full workspace suite (7,132 tests).

Fifth Phase 3 slice:

- `ObjectListingMetadataNodeClient` now requires the distinct
  `ObjectMetadataScanPgId` role for object, object-version, and multipart
  upload pages. Reusing `ObjectMetadataPgId` would be incorrect because these
  calls deliberately fan out across every installed object-metadata PG rather
  than routing one exact `(bucket, key)`.
- production code cannot construct a scan role from an arbitrary raw `PgId`.
  Cluster fan-out obtains it from the installed runtime map after enumerating
  configured metadata PGs. The type remains crate-private because no external
  production API needs to name a node-level listing scan.
- local adapters erase the role only at the private raw-node boundary. Unix
  adapters erase it into route evidence, and the Unix server reconstructs it
  only after validating the route and PG primary against installed topology.
- a Unix regression proves that all three listing interfaces accept an
  installed scan PG even though it is not tied to a specific request key, and
  that a test-forged unknown scan PG fails with `UnknownPg` before local node
  dispatch.
- object mutation/read and data-client signatures, `DataPgId` construction,
  and request-scoped route capability values remain open in Phase 3.
- the fifth slice passed its focused installed/unknown scan-PG Unix
  regression, the boundary checker, formatting, workspace-wide strict Clippy,
  and the full workspace suite (7,141 tests).

Sixth Phase 3 slice:

- `ObjectReadMetadataNodeClient` and its three duplicate methods on the
  transitional `StorageNodeClient` aggregate now require
  `ObjectMetadataPgId` for authorization-subject, coherent-snapshot, and
  subject-bound tag reads.
- cluster read paths derive one exact-object role for `(bucket, key)` from the
  installed runtime topology. They retain its raw projection only for generic
  route selection and retry-budget diagnostics, while every node-client call
  receives the typed role.
- local adapters erase the role only at the private raw-node boundary. Unix
  adapters serialize its raw route evidence, and the Unix server reconstructs
  the role only after route, primary, and exact object-placement validation.
- the adversarial two-PG Unix regression seeds the same live object, identity,
  and tags on both the correct and wrong PGs. A node call that bypassed
  placement validation would therefore succeed; all three test-forged
  wrong-role reads must instead fail with the exact `PayloadDecode` placement
  error, while the correctly routed authorization-subject canary succeeds.
- review correction: both raw inserts run under one fixed test clock, and the
  fixture explicitly loads both PG-local authorization subjects and requires
  their complete identities to be equal before exercising the Unix boundary.
  This prevents timestamp drift from turning the snapshot/tag checks into
  incidental stale-subject failures.
- object mutation and data-client signatures, `DataPgId` construction, and
  request-scoped route capability values remain open in Phase 3.
- the sixth slice passed its focused correct/wrong object-read PG Unix
  regressions, the boundary checker, formatting, workspace-wide strict Clippy,
  and the full workspace suite (7,141 tests).

Seventh Phase 3 slice:

- the object metadata-property update path now requires
  `ObjectMetadataPgId` for both
  `ObjectMutationMetadataNodeClient::load_put_object_metadata_snapshot` and
  `BuildPutObjectMetadataCommandReq`, including the duplicate snapshot method
  on the transitional `StorageNodeClient` aggregate. This covers tag, ACL,
  retention, and legal-hold mutations that share the snapshot-sensitive
  publisher.
- cluster mutation code derives one exact-object role for `(bucket, key)` and
  uses its raw projection only for generic pending-command routing. Local
  command-ID allocation erases the role at the private store boundary; Unix
  request encoding and command-response route validation erase it only at the
  RPC boundary.
- the Unix server reconstructs the object role after the existing route,
  primary, exact object-placement, and bucket-write-reservation binding checks.
- the two-PG adversarial regression now requires both metadata snapshot loading
  and command construction to fail with the exact `PayloadDecode` placement
  error for a test-forged wrong role. Identical fixed-clock object state exists
  on both PGs, while a correctly routed metadata snapshot is the positive
  canary, so an unguarded wrong-PG call would otherwise succeed.
- object deletion, multipart/stream mutation, reclaim/scan, and data-client
  signatures, `DataPgId` construction, and request-scoped route capability
  values remain open in Phase 3.
- the seventh slice passed its focused correct/wrong object-metadata PG Unix
  regressions, the boundary checker, formatting, workspace-wide strict Clippy,
  and the full workspace suite (7,150 tests).

Eighth Phase 3 slice:

- exact-object deletion now requires `ObjectMetadataPgId` for current and
  specific-version snapshots, lifecycle's per-key version list, and the
  delete-specific, delete-current, and insert-delete-marker command builders.
  The duplicate snapshot/list methods on the transitional
  `StorageNodeClient` aggregate and all three command request values carry the
  same role.
- cluster delete and lifecycle paths derive one exact-object role for
  `(bucket, key)`. They retain its raw projection only for generic
  pending-command routing, reservation, installation, and application helpers;
  every object-specific node-client call receives the typed role. PutObject
  stream-session and multipart-upload initiation use the role for their shared
  current-object snapshot while retaining raw routing for their still-open
  stream/multipart interfaces.
- local adapters erase the role only at the private raw-node boundary. Unix
  request and response validation erase it at the RPC boundary, and the Unix
  server reconstructs it only after route, primary, exact object-placement,
  and, for command builders, bucket-write-reservation validation.
- the two-PG adversarial Unix regression uses identical fixed-clock live
  object state on the correct and wrong PGs. Correctly routed current and
  specific snapshot loads are positive canaries; wrong-role current/specific
  snapshots, lifecycle listing, and all three command builders must fail with
  the exact `PayloadDecode` placement error. An unguarded call would otherwise
  find a coherent object and build a valid command on the wrong PG.
- multipart/stream mutation, reclaim/scan, and data-client signatures,
  `DataPgId` construction, and request-scoped route capability values remain
  open in Phase 3.
- the eighth slice passed its focused correct/wrong deletion-PG Unix
  regressions, the boundary checker, formatting, workspace-wide strict Clippy,
  and the full workspace suite (7,150 tests).

Ninth Phase 3 slice:

- upload initiation now requires `ObjectMetadataPgId` for PutObject/UploadPart
  stream-session match detection, multipart-upload match detection, and both
  create-command request values. The duplicate match methods on the
  transitional `StorageNodeClient` aggregate carry the same exact-object role.
- PutObject stream creation, multipart initiation, UploadPart session
  creation, and the reservation-preserving internal stream-create path derive
  one exact-object role for `(bucket, key)`. Raw `PgId` remains only for the
  still-generic pending-command, reservation, installation, and application
  helpers used around those typed node-client calls.
- local adapters erase the role only at the private raw-node boundary. Unix
  adapters serialize its raw route evidence and validate command responses
  against its raw projection. The Unix server reconstructs the role only
  after route, primary, exact object-placement, and, for command builders,
  bucket-write-reservation validation.
- the two-PG adversarial Unix regression uses the existing identical
  fixed-clock object state. Correctly routed absent stream and multipart match
  checks are positive canaries; test-forged wrong-role match checks and both
  command builders must fail with the exact `PayloadDecode` placement error.
  Without server-side placement validation, the wrong PG has sufficient
  coherent state to return an absent match or build the command.
- stream-session lookup/finalization, multipart read/management/completion,
  reclaim/scan, and data-client signatures, `DataPgId` construction, and
  request-scoped route capability values remain open in Phase 3.
- the ninth slice passed its focused correct/wrong upload-initiation PG Unix
  regressions, the boundary checker, formatting, workspace-wide strict Clippy,
  and the full workspace suite (7,150 tests).

Tenth Phase 3 slice:

- exact-object stream mutation now requires `ObjectMetadataPgId` for session
  and staged-segment reads, segment-append preparation, PutObject and
  UploadPart finalization snapshots, bucket-write-reservation renewal, and
  both final commit command request values. The duplicate methods on the
  transitional `StorageNodeClient` aggregate carry the same role.
- cluster stream creation heartbeat, append, abort, and finalization paths
  derive one exact-object role for `(bucket, key)`. Its raw projection remains
  only around generic pending-command routing, command installation,
  application, and recovery helpers.
- local adapters erase the role only at the private raw-node boundary. Unix
  adapters serialize its raw route evidence and validate command responses
  against the raw projection. The Unix server reconstructs the role only
  after route, primary, exact object-placement, and, for final commit builders,
  bucket-write-reservation validation.
- a dedicated two-PG Unix regression installs equivalent PutObject stream
  state, generation reservations, stored bucket-write proofs, multipart
  uploads, and UploadPart stream sessions on the correct and wrong PGs.
  Correct session, segment, append-preparation, proof-renewal, both
  finalization, and UploadPart command-build calls are positive canaries.
  Test-forged wrong roles must fail with the exact `PayloadDecode` placement
  error for those operations and both final commit builders. The wrong-PG
  append case submits the append-preparation RPC directly so a preliminary
  session-load rejection cannot mask missing validation on that endpoint.
- multipart read/management/completion, bucket-wide stream scans,
  reclaim/scan, and data-client signatures, `DataPgId` construction, and
  request-scoped route capability values remain open in Phase 3.
- the tenth slice passed its focused correct/wrong stream-metadata PG Unix
  regressions, the boundary checker, formatting, workspace-wide strict Clippy,
  and the full workspace suite (7,151 tests).

Eleventh Phase 3 slice:

- exact-object multipart state now requires `ObjectMetadataPgId` for upload
  lookup, in-progress and listing lookup, completion snapshot and preflight,
  authorized part listing, management lookup, null-version stale-payload
  lookup, abort cleanup, and complete/abort command request values. The
  duplicate methods on the transitional `StorageNodeClient` aggregate carry
  the same role.
- cluster multipart initiation follow-up, UploadPart stream creation,
  completion, management, lifecycle, and abort paths derive one exact-object
  role for `(bucket, key)`. Its raw projection remains only around generic
  pending-command routing, command installation, application, version
  reservation, and recovery helpers.
- local adapters erase the role only at the private raw-node boundary. Unix
  adapters serialize its raw route evidence and validate command responses
  against the raw projection. The Unix server reconstructs the role only
  after route, primary, and exact object-placement validation.
- a dedicated two-PG Unix regression installs the same multipart upload and
  part on the correct and wrong PG under a fixed clock. Every exact multipart
  read and command builder has a correctly routed positive canary; test-forged
  wrong roles must fail with the exact `PayloadDecode` placement error.
  Equivalent wrong-PG state ensures a missing/conflicting record cannot mask a
  placement-validation gap.
- bucket-wide stream scans, reclaim/scan and data-client signatures,
  `DataPgId` construction, and request-scoped route capability values remain
  open in Phase 3.
- the eleventh slice passed its focused correct/wrong multipart-metadata PG
  Unix regressions, the boundary checker, formatting, workspace-wide strict
  Clippy, and the full workspace suite (7,196 tests).

Twelfth Phase 3 slice:

- bucket-scoped and all-PG stream-upload scans now require
  `ObjectMetadataScanPgId`. The duplicate methods on the transitional
  `StorageNodeClient` aggregate carry the same role.
- cluster bucket-deletion visibility and cleanup paths, together with the
  best-effort abandoned-session scan, derive scan roles only from object
  metadata PGs in the installed topology. Raw PG IDs remain only for routing
  and returned-row placement validation.
- local adapters erase the role only at the private raw-node boundary. Unix
  adapters serialize its raw route evidence, and the Unix server reconstructs
  the role only after route and primary validation. Both Unix scan endpoints
  validate every returned stream-upload row against the scanned PG before
  responding.
- expanded Unix regressions prove that both scan endpoints accept an
  installed object-metadata scan PG, reject an unknown PG with `UnknownPg`,
  require the routed primary, and reject equivalent durable rows deliberately
  stored on the wrong PG.
- reclaim/scan and data-client signatures, `DataPgId` construction, and
  request-scoped route capability values remain open in Phase 3.
- the twelfth slice passed its focused stream-upload scan Unix regressions,
  the boundary checker, formatting, workspace-wide strict Clippy, and the full
  workspace suite (7,196 tests).

Thirteenth Phase 3 slice:

- exact-object payload-reclaim existence, durable command lookup, claim
  acquisition, and claim release now require `ObjectMetadataPgId`.
  Bucket-scoped and all-PG reclaim-root discovery, together with per-PG claim
  discovery, require `ObjectMetadataScanPgId`. The duplicate methods on the
  transitional `StorageNodeClient` aggregate carry the same roles.
- cluster reclaim execution derives one exact-object role for `(bucket, key)`;
  bucket-delete visibility/debug and durable reclaim discovery derive scan
  roles only from installed object-metadata PGs. Raw PG IDs remain only for
  routing, generic metadata-command work, diagnostics, and returned-row
  placement validation.
- local adapters erase the roles only at the private raw-node boundary. Unix
  adapters serialize their raw route evidence, and the Unix server reconstructs
  the applicable role only after route, primary, and, for exact operations,
  object-placement validation.
- both reclaim-root scan endpoints and the reclaim-claim scan endpoint now
  validate every returned `(bucket, key)` against the scanned PG before
  responding. This closes a pre-existing Unix boundary gap where misplaced
  durable reclaim state could be returned to the cluster.
- a dedicated two-PG Unix regression installs equivalent reclaim roots and
  active claims on the correct and wrong PGs. Correct exact-object and scan
  calls are positive canaries; test-forged wrong exact roles and misplaced
  scan results must fail with the exact `PayloadDecode` placement error. It
  also proves a rejected wrong-PG claim release does not mutate the claim.
- shard-ack/scavenger data-client signatures, `DataPgId` construction, and
  request-scoped route capability values remain open in Phase 3.
- the thirteenth slice passed its focused payload-reclaim Unix regressions,
  the boundary checker, formatting, workspace-wide strict Clippy, and the full
  workspace suite (7,222 tests).

Fourteenth Phase 3 slice:

- written-shard acknowledgement registration, validation, loading,
  historical inspection, current deletion, and retained-epoch deletion now
  require `DataPgId` throughout `ShardAckNodeClient`. Cluster callers derive
  the data role before selecting the metadata-primary acknowledgement client;
  local adapters erase it only at the private PG-store boundary.
- the Unix client deliberately erases `DataPgId` to raw `PgId` in the wire
  request. Decoding does not confer a role: the Unix server validates the raw
  PG against the applicable active or retained route and primary constraints,
  then constructs its server-local `DataPgId` before accessing acknowledgement
  rows.
- an adversarial Unix regression submits a decoded unknown data PG to all five
  acknowledgement RPC operation classes and requires `UnknownPg`. It checks
  the record target remains absent immediately after rejected registration
  and verifies a seeded acknowledgement canary retains its exact value after
  both rejected registration and rejected deletion, so opposite mutation bugs
  cannot cancel out. Existing positive, non-primary, historical-inspection,
  and stale-route tests remain canaries.
- repair/backfill and scavenger data-client signatures, non-forgeable
  `DataPgId` construction, and request-scoped route capability values remain
  open in Phase 3. This slice removes raw bucket/object/data role confusion
  from the basic acknowledgement API but does not yet claim trusted data-role
  construction.
- the fourteenth slice passed its focused active, retained, non-primary,
  stale-route, and unknown-data-PG regressions, the boundary checker,
  formatting, workspace-wide strict Clippy, and the full workspace suite
  (7,223 tests).

Fifteenth Phase 3 slice:

- durable placed-shard repair and backfill registration, listing, counting,
  existence checks, claim acquisition/completion/error recording, and
  resolution now require `DataPgId` throughout `ShardAckNodeClient`. Cluster
  callers derive the data role from the routed PG or the work item's
  `SegmentStoredBytesRequest`; local adapters erase it only at the private
  PG-store boundary.
- the Unix client erases `DataPgId` to the existing raw route request. The
  server first validates that raw route and its primary, then requires every
  item-bearing request or claim to name the same data PG before constructing
  the server-local role. Route/work-item mismatches fail as `PayloadDecode`
  rather than reaching a PG and surfacing an internal store error.
- a two-PG adversarial Unix regression seeds claimed repair and backfill
  canaries on PG 0, submits every item-bearing operation through configured PG
  1, and requires `PayloadDecode`. Route-only list/count/claim-acquire
  operations submitted to unknown PG 9 require `UnknownPg`; afterward the PG 0
  canaries remain exact and PG 1 remains empty.
- the remaining mixed-role `ShardScavengerNodeClient` signatures,
  non-forgeable `DataPgId` construction, and request-scoped route capability
  values remain open in Phase 3.
- the fifteenth slice passed its focused RPC-codec, durable-claim,
  scavenger-to-backfill, and adversarial Unix regressions, the boundary
  checker, formatting, workspace-wide strict Clippy, and the full workspace
  suite (7,224 tests).

Sixteenth Phase 3 slice:

- `ShardScavengerNodeClient` now separates its remaining mixed roles:
  shard-file/row and observation operations require `DataPgId`, while
  payload-reference fan-out requires `ObjectMetadataScanPgId`. Cluster
  scavenger, backfill-candidate, and best-effort stream-cleanup paths derive
  those roles from installed topology before calling a node.
- Unix clients erase those roles to the existing raw route fields. The
  list-files request also now decodes its PG as raw `PgId`, so untrusted RPC
  bytes do not confer a data role. The server constructs the applicable data
  or object-scan role after raw route validation. Metadata-backed row,
  payload-reference, and observation operations additionally require the
  metadata primary; list-files intentionally scans each routed acting-set
  node's local files without a primary requirement. Observation record/resolve
  also require the embedded observation data PG to equal the routed PG before
  accessing a store.
- a two-PG adversarial Unix regression seeds complete observation canaries on
  PG 0 and PG 1. Record and resolve requests routed through PG 1 but naming PG
  0 require `PayloadDecode`; file, row, payload-reference, and observation
  scans for unknown PG 9 require `UnknownPg`. Reopening storage proves both
  canary rows, including timestamps, counts, error, and resolution state,
  remain exact.
- all `ShardScavengerNodeClient` PG parameters are now role-typed.
  Non-forgeable `DataPgId` construction and request-scoped route capability
  values remain open in Phase 3.
- the sixteenth slice passed its focused RPC-codec, embedded/Unix scavenger,
  backfill-candidate, and adversarial Unix regressions, the boundary checker,
  formatting, workspace-wide strict Clippy, and the full workspace suite
  (7,244 tests).

Seventeenth Phase 3 slice:

- `DataPgId` now lives beside the other role IDs inside the private
  `node_runtime` boundary. It has no public or crate-visible raw constructor;
  only installed `SharedStorageNode`/`LocalNodeRuntime` authorities can promote
  a configured raw PG, while unit tests retain an explicit `new_for_test`
  escape hatch under `cfg(test)`.
- public placement-only `PgTopology` data-placement methods now return raw
  `PgId`. `LocalClusterMap` converts their result through its installed node
  runtime before exposing a `DataPgId`, and cluster paths that recover a data
  PG from durable request/segment state fail closed with `ClusterPgNotFound`
  unless the PG belongs to the installed runtime.
- shard write, repair, read, range-read, delete, and read-handle Unix request
  codecs now decode `StorageRpcShardLocation`, whose PG remains raw. Active,
  retained-cleanup, and historical-inspection server validation promotes that
  wire location to the typed `ShardLocation`; read-handle responses are
  compared with the exact raw request before the Unix client returns its
  original trusted typed locations.
- existing unknown/stale PG, epoch, node, shard-I/O non-mutation, retained
  historical access, and read-handle regressions continue to cover both local
  and Unix boundaries. The Rust API now makes the non-forgeability property
  structural rather than dependent on a source scanner.
- request-scoped active, retained-cleanup, and recovery route capabilities
  remain open in Phase 3.
- the seventeenth slice passed formatting, workspace all-target compilation,
  the storage boundary checker, all 2,156 storage tests, workspace-wide strict
  Clippy, and the full workspace suite (7,244 tests).

Eighteenth Phase 3 slice:

- `StorageClusterRuntimeMapHandle` now owns a route-admission gate shared by
  every handle clone. `admit_current_route` returns a non-cloneable
  `StorageClusterRouteAdmission`; replacement runtime-map publication drains
  admitted requests and blocks new admission until publication completes.
- the admission owns its pinned `StorageCluster` and permit but deliberately
  does not dereference to the cluster API. `StorageCluster` itself is also no
  longer `Clone`. Storage operations therefore cannot bypass the captured
  deadline through the renewable cluster lease; operation-specific access
  remains unavailable until those boundaries accept the admission explicitly.
- the authority and process-local monotonic deadlines now form one coherent
  lock-protected lease snapshot. Admission captures that pair under one read
  lock, while renewal, replacement, and expiry update it under the matching
  write lock. `require_valid_now` checks both current invalidation and the
  captured lease, so a later same-generation renewal cannot extend a
  long-running request's authority.
- invalid unbounded candidates are rejected before beginning a drain. Focused
  concurrency tests prove valid publication waits for release without polling
  or sleeps, expired maps cannot be admitted, and an admitted deadline remains
  expired after the underlying map is renewed. A deterministic capture/renewal
  interleaving additionally proves renewal cannot publish one half of a lease
  pair while admission captures the other: a test-only renewal `try_write`
  fails while the capture hook holds the read guard, then succeeds immediately
  after that guard is released.
- the boundary checker now canonicalizes the recovery implementation helpers
  introduced by the retained-route command reissue work back to their
  registered semantic operations. The raw metadata-PG exception follows the
  same implementation body, keeping the checker synchronized without adding a
  second publisher classification.
- this slice establishes the frontend guard and publication barrier only.
  Threading it through coordinator request entry, then deriving operation- and
  subject-bound active/retained/recovery capabilities for node-client calls,
  remains open. The storage-node Unix boundary already has a per-frame
  publication gate, but still needs server-local typed capabilities and
  per-effect validation.
- validation passed formatting, the storage boundary checker, the focused
  eight-test runtime-map admission suite, its compile-fail API boundary
  doctest, workspace-wide checks and strict Clippy, and the full workspace
  suite (7,275 tests).

Nineteenth Phase 3 slice:

- buffered HTTP operations now acquire a `StorageClusterRouteAdmission` after
  their bounded body has been collected, routed, and authenticated, but before
  region enforcement or coordinator dispatch. Routing and AWS-facing
  authentication errors therefore retain precedence over an expired route
  map; optional bucket-region enrichment is attempted only under a valid
  admission and route expiry merely suppresses that header. Actual-response
  CORS enrichment follows the same best-effort rule, so an authentication
  error with `Origin` cannot perform an unadmitted metadata lookup. OPTIONS
  acquires admission after successful routing and before its unauthenticated
  CORS metadata lookup.
  Streaming PUT, POST, and UploadPart acquire it after request authentication
  but before their first storage lookup, and store the non-cloneable guard in
  the streaming context through request-body ingestion, finalization, or abort
  cleanup. Unauthenticated transport parsing and slow pre-auth bodies
  therefore cannot delay runtime-map publication.
- the guard is deliberately released once the response has been constructed.
  Client response backpressure is not active route authority; streaming reads
  retain their existing payload/read handles. Route capability checks at those
  lower read boundaries remain part of the later active/retained capability
  work.
- a multi-worker HTTP pool must now share one runtime-map admission domain.
  Handle clones have an explicit identity check, server startup rejects
  independently constructed domains, and both the primary S3 test server and
  the admission integration server now construct their worker coordinators
  from one shared `StorageClusterRuntimeMapHandle`.
- a deterministic serve regression starts an authenticated streaming PUT,
  sends a partial signed body, waits until its context is admitted, begins
  replacement publication, and proves the old map remains current until the
  client disconnects and abort cleanup releases the context. A storage-level
  regression separately proves that only handle clones share an admission
  domain. A socket-level buffered-request regression expires the route map,
  sends a bad SigV4 signature with `Origin`, and requires
  `SignatureDoesNotMatch` rather than the internal `OperationAborted`
  admission error while also proving expired admission suppresses CORS
  metadata enrichment. A per-frontend test-only counter at the CORS metadata
  load boundary requires zero lookup attempts, so the regression does not
  infer the guard solely from absent response headers.
- this slice threads the frontend publication guard through real request
  entry, but it does not yet expose the guarded cluster API. Operation- and
  subject-bound active, retained-cleanup, and recovery capabilities, captured
  deadline validation at each effect, and Unix server-local capability
  construction remain open.
- validation passed formatting, the storage boundary checker, all 774
  server-HTTP tests, the focused S3 admission integration test,
  workspace-wide strict Clippy, and the full workspace suite (7,278 tests).

### Phase 4 — type metadata-command publication

1. Replace the Phase 0 registry's discovery-only linkage with typed publisher
   APIs while retaining its authoritative IDs and exhaustive classification.
   Introduce exhaustive install outcome types.
2. Move pending-slot installation, exact-command matching, and generic conflict
   draining behind those typed publishers.
3. Convert one class at a time, beginning with snapshot-sensitive operations,
   then allocator cleanup, terminal-session retry, matching-outcome retry, and
   apply-validated paths.
4. Keep deterministic install-race regressions for each exceptional
   terminal/matching publisher.
5. Derive or test the documentation inventory from the Rust classification
   rather than function-name counts.

Completion:

- production operations cannot call raw pending-slot installers
- every publisher has an explicit Rust classification
- adding or changing a publisher requires an exhaustive compiler-visible
  classification
- the publisher and `MetadataCommandLogConflict` count inventories are removed
  from the shell check

### Phase 5 — isolate test support

1. Inventory feature-gated and `cfg(test)` raw mutation/read hooks used outside
   their defining module.
2. Move cross-crate fixtures into an internal test-support crate or explicit
   dev-only API.
3. Ensure production dependency graphs do not enable test hooks.
4. Keep narrowly scoped deterministic fault-injection guards where production
   code must contain the hook point.

Completion:

- production crates cannot name test-only storage mutation APIs
- tests retain direct state inspection where it is necessary to prove durable
  invariants
- textual scans for public test hooks are retired where the dependency graph
  enforces absence

### Phase 6 — shrink and redefine the boundary check

For each remaining section, record:

- the invariant
- why Rust visibility/types cannot enforce it
- why an ordinary unit or integration test is insufficient
- the stable input being inspected

Expected long-term survivors are small semantic checks such as:

- format/encoding version coordination
- temporary prohibitions while a migration is incomplete
- feature/dependency configuration that Cargo does not otherwise reject
- documentation consistency that cannot be generated from Rust definitions

Delete historical symbol bans once the forbidden APIs are structurally absent.
Do not retain exact source-line/function-count inventories merely as
documentation.

Completion:

- the script is short, stable across test/file moves, and run in CI
- every remaining check has a documented reason it cannot be a type, module,
  crate, or test invariant
- `guides/storage-cluster-invariants.md` describes the structural boundary,
  not a parallel hand-maintained public-method count

## Validation strategy

Each phase must run:

- `./scripts/check-storage-cluster-boundaries`
- `cargo fmt --all`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo nextest run`

Boundary-changing phases also require:

- local and Unix client parity tests for affected operations
- runtime-map transition tests for active versus retained authority
- adversarial local and Unix tests for expired, stale, reused, and
  subject-mismatched route capabilities
- deterministic contention/recovery tests for affected command publishers
- review of public exports and enabled Cargo features

Where a check is retired, include a regression demonstrating the replacement:

- a compiler-enforced inaccessible API or wrong-type call need not be tested by
  parsing compiler diagnostics, but the new boundary should be evident from
  module/crate visibility and reviewed as part of the change
- PG-role and route-capability constructors, local application-time
  revalidation, and Unix server-side revalidation require positive and
  adversarial unit tests
- typed publisher outcomes require deterministic tests for every outcome

## Risks

- A large crate split can create dependency cycles or move too many shared
  types into a protocol crate. Use module privacy first and split only when the
  boundary is clearer.
- A broad protocol/types crate can recreate the same weak boundary under a new
  name. Export request/response values, not raw engine handles or mutation
  helpers.
- Client-side route capabilities must not be trusted across Unix RPC. The
  server remains the authority and revalidates immediately before mutation.
- A locally held capability must not imply indefinite authority. Its
  admission-guard lifetime and absolute deadline are both enforced, and every
  durable effect revalidates the capability.
- Excessively generic typed publisher abstractions can obscure operation
  semantics. Prefer a small number of explicit classes and exhaustive outcome
  enums over a framework of deeply nested generics.
- Removing public test helpers without a replacement can weaken durable-state
  verification. Move them to dev-only support before hiding them.
- Retiring a textual check prematurely creates a real gap. Keep a
  check-to-replacement table in each implementation change.

## Completion criteria

This plan is complete when:

1. the repaired transitional boundary check runs in CI
2. server-core and ordinary application code cannot name raw node/store APIs
3. cluster code reaches storage nodes only through local/Unix client
   interfaces
4. PG role and route authority are represented by non-forgeable types with
   private trusted construction; route capabilities are request-scoped,
   non-cloneable, deadline-bound, and revalidated at durable effects
5. raw pending-command installation is inaccessible to operation publishers
6. publisher retry/convergence classes are explicit and exhaustively handled
   in Rust
7. test-only mutation APIs are absent from production dependency surfaces
8. the remaining boundary script contains only justified semantic checks that
   cannot reasonably be enforced by Rust or Cargo
