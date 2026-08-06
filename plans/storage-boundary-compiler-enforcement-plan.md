# Storage Boundary Compiler-Enforcement Plan

Status: active — Phases 0–4 complete; Phase 5 in progress

Related plans:

- [static-cluster-configuration-plan.md](completed/static-cluster-configuration-plan.md)
- [control-plane-auth-identity-plan.md](control-plane-auth-identity-plan.md)
- [multihost-followup-plan.md](multihost-followup-plan.md)
- [storage-upgrade-versioning-plan.md](storage-upgrade-versioning-plan.md)

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

## Relationship To Storage RPC Authentication

Reconciliation decision (2026-07-22): storage RPC authentication and compiler-
enforced operation capabilities are complementary authorization layers. They
must share one dispatch contract, but they must not be collapsed into one
serializable or role-wide capability.

The transport-independent storage RPC auth slice owns **process authority**:

- authenticate cluster, topology generation/digest, source principal, target
  storage node, request/response direction, concrete wire message kind,
  request id, freshness, and complete frame integrity;
- apply one exhaustive `StorageRpcMessageKind`-to-principal-role matrix before
  dispatch; and
- reject a signer that is valid for the cluster but not permitted to attempt
  that wire operation.

This plan owns **concrete storage authority**:

- derive trusted PG-role values only from installed topology and placement;
- validate active, retained-cleanup, recovery, peering, or transfer authority
  against current local state;
- bind authority to the exact route generation, subject, command/proof,
  operation class, admission domain, and deadline required by the handler; and
- require the resulting non-forgeable local capability at the storage effect.

The wire role matrix is intentionally coarse. For example, a maintenance
principal may be allowed to attempt a reclaim RPC, but that permission does not
authorize an arbitrary object, PG, historical route, or shard deletion. An
authenticated frontend principal likewise represents a trusted internal S3
coordinator after user-facing authorization; it does not turn every decoded
bucket, object, or PG identifier into trusted storage authority.

Conversely, a local route capability is not a credential. It does not identify
a remote process, is never serialized, and cannot be reconstructed merely by
decoding a valid-MAC frame. Unix and TCP servers receive only authenticated
wire evidence and construct fresh server-local capabilities after local route
and subject validation. Embedded/local adapters enter at that trusted local
validation boundary; they do not need a synthetic network credential, but they
must satisfy the same operation-capability requirements before storage effects.

The common dispatch order is normative:

1. apply bounded connection/frame admission and decode the untrusted envelope;
2. verify cryptographic identity, topology, target, direction, operation,
   request binding, freshness, and complete-frame integrity;
3. apply the exhaustive principal-role permission for the decoded wire kind;
4. decode raw route, PG, subject, and command evidence without conferring a
   Rust role or capability;
5. acquire the local route-admission domain and validate the handler's active,
   retained, recovery, peering, or transfer preconditions;
6. construct the narrow server-local PG-role and operation capability;
7. invoke the capability-requiring node API and then bind the authenticated
   response to the request.

Failure at either authorization layer is terminal before the storage effect.
The role matrix must not duplicate route-state predicates, and route-capability
code must not infer process identity from transport location or filesystem
ownership.

There is also one change-control rule for the two work streams. Adding or
changing a storage RPC requires an exhaustive update or explicit proof of no
change for all of:

- wire frame and allocation limits;
- principal-role permission for the message kind;
- the handler's trusted PG-role and operation-capability construction;
- client-side capability-bearing call paths;
- relevant frontend, storage-node, maintenance, repair, recovery, or admin
  workflow manifests; and
- valid-credential/wrong-role plus valid-role/wrong-route adversarial tests.

The existing `storage_rpc_auth::authorized_roles()` table is the current
wire-role authority. It should be described as a role matrix, not as the local
route-capability model. It may later be represented by a more typed policy
registry, but this plan does not require a generic enum that would erase the
subject- and lifetime-bound capability distinctions.

Implementation may continue incrementally, but the sequence is explicit:

1. preserve the landed bounded auth codec and exhaustive wire-role matrix as
   the outer policy foundation;
2. activate manifest credentials and enforce that policy at the Unix boundary,
   retaining all existing route/subject checks while capability migration is
   incomplete; this is complete for split-role replicated frontend and
   storage-node processes, including operation-scoped frontend/maintenance
   signer selection, auth retention across map refresh/recovery, durable static
   storage identity verification under a lifetime lock, and a process-wide
   pre-authentication allocation budget. Replicated `combined` remains
   fail-closed and is not a supported static version-1 process topology;
3. continue converting handlers and node-client APIs by complete workflow,
   adding a composed test that crosses both auth and local capability layers
   for each converted workflow;
4. reuse the same dispatch adapter for TCP only after Unix enforcement is
   complete; and
5. claim replicated storage RPC completion only when every stateful wire kind
   reaches a capability-requiring effect boundary. Until then, passing the
   outer role check is not evidence that compiler enforcement is complete.

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

The deadline rule applies to active work and to cleanup that can create,
renew, or broaden durable authority. A one-shot retained release of an exact
already-held subject is deliberately non-escalating: it may proceed after the
active route deadline so a timed-out request can shed a reservation, lock, or
lease. Such a capability must be bound to the exact durable subject, must
revalidate its retained route, placement, admission class, and admission
domain immediately before the effect, and cannot acquire or renew authority.

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
6. At an authenticated RPC boundary, accept only the frame/principal pair
   already verified by the storage RPC auth layer; then validate its raw
   serialized route evidence and construct a server-local capability. The
   verified principal is not itself that capability. Unix is wired first and
   TCP reuses the same post-auth dispatch adapter.
7. Add embedded, Unix, and TCP-when-enabled adversarial tests for:
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
  mutation on embedded and every enabled RPC transport path
- route and PG-role grep checks are retired or reduced to API-surface checks

Node-client role classification (2026-07-19):

| Interface | PG role | Capability direction |
| --- | --- | --- |
| `BucketMetadataNodeClient` | bucket metadata | exact reads/mutations use an active route bound to one epoch, bucket PG, and bucket; delete-replica inspection uses a separate narrower route; owner listing and batch generation/fast-path reads open an active scan route bound to one epoch and bucket PG |
| `BucketWriteReservationNodeClient` | bucket metadata | exact reservation/drain/claim operations open an active route bound to one epoch, bucket PG, and bucket; PG-wide delete/lifecycle discovery opens a separate active scan route bound to one epoch and bucket PG |
| `RetainedBucketWriteReservationNodeClient` | bucket metadata | opens a retained cleanup route bound to one bucket metadata PG and exact bucket; the returned interface releases exact reservation proofs, drains, and worker claims without accepting a replacement PG or bucket |
| `ObjectGenerationMetadataNodeClient` | object metadata | opens an active route bound to one epoch, exact object PG, bucket, and key; reservation lookup and next-generation inspection cannot replace that subject |
| `ObjectVersionMetadataNodeClient` | object metadata | opens an active route bound to one epoch, exact object PG, bucket, and key; ordinary and completion-priority version inspection retain distinct admission classes within that route |
| `DirectPutMetadataNodeClient` | object metadata | opens an active primary route bound to one epoch, exact object PG, bucket, and key; commit snapshot and command construction cannot replace route identity |
| `ObjectListingMetadataNodeClient` | object metadata scan | active object-metadata route for the scanned PG; listing fan-out constructs one capability per routed PG |
| `ObjectMutationMetadataNodeClient` | object metadata and object-metadata scan | exact mutation, stream-session, multipart, and payload-reclaim operations open active routes bound to one epoch and their complete object/upload/generation subjects; maintenance discovery opens a separate active scan route bound to one epoch and one installed object-metadata scan PG, including placement-verifiable witnesses for aborting multipart uploads |
| `RetainedObjectMutationMetadataNodeClient` | object metadata | opens a retained route bound to one epoch, object-metadata PG, bucket, and key; the returned interface prepares stream aborts and releases payload-reclaim claims without accepting replacement route or object arguments |
| `ObjectReadMetadataNodeClient` | object metadata | opens an active route bound to one epoch, exact object PG, bucket, and key; version selection and subject-identity validation remain operations within that fixed route, with payload reads separately retaining their read lease |
| `PlacedShardNodeClient`, `ShardAckNodeClient` | data | active placed-shard I/O opens an exact route bound to one node, epoch, data PG, shard index, and shard key; shard acknowledgement, current-placement cleanup, and repair/backfill work open an active route bound to one epoch and data PG |
| `RetainedPlacedShardNodeClient`, `RetainedShardAckNodeClient` | data | retained exact-placement inspection and cleanup for historical shard bytes and acknowledgement rows |
| `ShardScavengerNodeClient` | data for shard rows/files and object-metadata reference scans | node-wide history-reference reporting remains subject-free; PG scans open active routes bound to one epoch and exact data or object-metadata scan PG |
| `ShardScavengerObservationNodeClient` | data PG for durable observation rows | opens an active primary-only route bound to one data PG; the returned interface records, lists, and resolves non-authoritative scavenger findings without accepting a replacement PG |
| `ShardReadHandleNodeClient` | data locations carried in the handle request | opens a consumed route bound to one operation ID and an exact per-node batch of validated locations/keys; active/retained authority is inherited from each location and the resulting lease remains non-cloneable |
| `ObjectPayloadLeaseNodeClient` | object subject rather than a caller-supplied PG | active subject-bound lease acquisition, reclaim-begin, and observation; placement is validated when shard locations are acquired |
| `RetainedObjectPayloadReclaimNodeClient` | object subject rather than a caller-supplied PG | retained exact-claim reclaim completion and fence cleanup; opaque lease objects retain their own release authority |
| `MetadataCommandInspectionNodeClient` | genuinely generic metadata PG | read-only command state, checkpoints, retained-log ranges, acceptance, and abandonment inspection |
| `MetadataCommandNodeClient` | genuinely generic metadata PG | ordinary active publisher and convergence authority; serialized primary acceptance/application opens a non-nestable critical section bound to one PG and epoch, while recovery-only operations remain unavailable |
| `MetadataCommandPeeringNodeClient` | genuinely generic metadata PG | opens a scoped peering route bound to one destination PG and epoch; the returned interface owns quiesced-PG replay validation, retained-log catch-up, and metadata-transfer initialization/adoption without accepting replacement destination route arguments, command-ID allocation, or pending-slot publication |
| `MetadataCommandRecoveryNodeClient` | genuinely generic metadata PG | opens a non-nestable primary recovery critical section bound to one PG and epoch; historical-replica apply and tombstone mutation instead open distinct single-use routes borrowing the exact PG, epoch, authorized source, optional abandoned source, and command |
| `RetainedMetadataCommandNodeClient` | genuinely generic metadata PG | opens a non-cloneable retained stream-abort route borrowing one opaque prepared command; apply and finish accept no replacement PG, epoch, or command arguments |
| `StorageNodeClient` | removed transitional mixed aggregate | production and cross-boundary test callers use role-specific interfaces; deliberately process-local assertions use the sanctioned raw-node test facade; private local helpers implement the embedded adapter without exposing another trait surface |

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

Twentieth Phase 3 slice:

- `StorageClusterRouteAdmission::active_bucket_route` now derives the first
  operation-facing route capability from the exact admitted frontend map.
  `ActiveBucketRoute` has private fields, is non-`Clone`, borrows its
  admission, and fixes both the bucket identity and installed `BucketPgId` at
  construction. Its operations take neither identity again, so a caller
  cannot combine authority for one bucket with another bucket or PG.
- the initial guarded API covers active bucket-info and bucket-subresource
  reads. Each call revalidates the admission's captured absolute deadline
  immediately before selecting the admitted cluster's node client. A later
  renewal of the underlying map therefore cannot extend an existing request's
  capability, and the operation never reselects `current()` from the runtime
  map handle.
- bucket-existence lookups used for wrong-region and denied/auth-error response
  headers, OPTIONS CORS lookup, and actual-response CORS lookup now require the
  exact admission and use `ActiveBucketRoute`. Streaming response CORS obtains
  a fresh admitted route after the write has finished; optional authentication
  error enrichment continues to fail closed by omitting the header if route
  admission or the guarded read fails.
- a compile-fail boundary proves the capability is not cloneable. A focused
  deadline regression derives a bucket route, renews the underlying map, then
  proves a read after the original deadline fails before node access. The Unix
  bucket-metadata integration regression now exercises both guarded bucket
  head and subresource reads against an installed Unix client, in addition to
  the embedded HTTP coverage.
- this slice intentionally exposes only the active bucket-read projection.
  Main coordinator dispatch, object/data operations, durable bucket effects,
  retained cleanup/recovery capabilities, capability requirements on the
  node-client traits, and Unix server-local capability construction remain
  open. Existing unguarded `StorageCluster` methods cannot be retired until
  their request and background callers are classified and migrated.
- validation passed formatting, the storage boundary checker, both storage
  compile-fail doctests, the focused captured-deadline and Unix bucket-client
  regressions, all 779 server-HTTP tests, workspace-wide strict Clippy, and the
  full parallel workspace suite (7,325 tests). The full run exposed a separate
  synthetic-clock mismatch in an authenticated Unix control-plane fixture:
  its client advanced a fixed authority timestamp with local elapsed time
  while its server clock stayed frozen. That fixture now uses the existing
  fixed-clock request path, keeping its authentication assertion deterministic
  under scheduler delay.

Twenty-first Phase 3 slice:

- storage-node RPC frames now carry an explicit private admission class:
  ordinary operations receive `Active`, while the metadata-command PG-lock
  release path receives the narrower `RetainedCleanup` class that may enter
  during transition drain. The permit is passed into frame dispatch instead
  of existing only as an unnameable local guard.
- bucket head and bucket-subresource read handlers derive a private,
  non-`Clone` `StorageNodeActiveBucketRoute` before reaching the local node
  client. Its construction requires a permit from the same admission domain
  and the `Active` class, validates the decoded node, epoch, PG, primary, and
  bucket placement, then fixes the bucket and trusted `BucketPgId` for the
  capability lifetime. A retained-cleanup permit cannot be promoted into
  active authority.
- the server runtime config and its bound process-monotonic lease now form one
  lock-protected `StorageNodeRuntimeRouteState`. Per-frame refresh clones that
  coherent pair while holding one read guard; validity-only renewal replaces
  the pair under one write guard. The server-local capability captures both
  deadlines from the frame snapshot and revalidates that fence immediately
  before each local head or subresource read. A same-generation validity
  extension can continue without draining admitted frames, but cannot extend
  an already admitted request. The existing metadata-command mutation fence
  was renamed to the more accurate shared `StorageNodeRouteFence`; its durable
  commit-guard behavior is unchanged.
- a deterministic storage-node regression reads a seeded bucket and missing
  CORS subresource through the capability and rejects construction from both a
  foreign admission domain and a retained-cleanup permit. It then pauses frame
  snapshot capture while holding the coherent-state read guard, proves a
  5,000ms-to-10,000ms renewal is blocked at that exact write boundary, and
  completes capability construction after renewal publication. The capability
  still fails at 6,000ms with the frame's captured 5,000ms deadline. Existing
  Unix positive and wrong-bucket-PG regressions continue to cover wire
  decoding, primary/placement validation, and both migrated operations.
- this is the first server-local active capability and intentionally covers
  only the two read RPCs exposed by `ActiveBucketRoute`. Bucket snapshots,
  mutations, object/data operations, and retained cleanup/recovery still use
  their existing validation paths and remain open for capability migration.
- validation passed formatting, the storage boundary checker, all 161
  storage-node server tests, the focused Unix bucket-head and subresource
  integration regressions, workspace-wide strict Clippy, and the full
  parallel workspace suite (7,326 tests).

Twenty-second Phase 3 slice:

This historical slice's dormant two-bucket snapshot capability and pair RPC
were removed in the eighth Phase 4 slice. Production CopyObject and
UploadPartCopy use their narrower source-read and destination-write
capabilities instead.

- `ActiveBucketRoute` now covers a full single-bucket authorization snapshot,
  while the new non-`Clone` `ActiveBucketRoutePair` fixes both bucket subjects
  and their installed bucket PGs for CopyObject-style two-bucket snapshots.
  The pair preserves the existing same-bucket request union and deterministic
  cross-PG load order, but revalidates the admission's captured deadline
  before each node selection and node-client access.
- the Unix storage-node `BucketSnapshotLoad` and `BucketSnapshotPairLoad`
  handlers now require the exact active frame permit and derive corresponding
  server-local bucket capabilities before accessing `LocalStorageNodeClient`.
  Both sides of a pair independently validate node, epoch, route, primary,
  bucket placement, admission class, and admission domain. Snapshot loading
  revalidates the coherent frame deadline immediately before node access.
- the frontend Unix integration canary now creates two storage-node-owned
  buckets and reads both a single authorization snapshot and a distinct
  snapshot pair through the public admitted-route capabilities. The
  deterministic deadline regressions cover single and paired snapshots on
  both the frontend and server-local capabilities.
- the two-PG adversarial Unix regression stores the same bucket on both PGs,
  proves the correctly routed snapshot succeeds, and requires exact
  `PayloadDecode` placement failures for a forged wrong role in either side of
  a snapshot pair and at the durable write-reservation boundary. Equivalent
  wrong-PG durable state ensures missing data cannot mask a server-side
  validation gap. Its one-shot server acceptor count exactly matches all seven
  asserted RPCs, so a client timeout cannot masquerade as placement rejection.
- full-suite validation exposed an existing lifecycle-sweep Unix RPC test that
  started seven one-shot server acceptors for six requests and then waited
  forever for the unused thread. Its acceptor count now documents and exactly
  matches the six exercised RPCs.
- unguarded compatibility methods remain while their coordinator callers are
  migrated. Bucket mutation command building, write reservations,
  retained-cleanup/recovery authority, object/data operations, and capability
  requirements on the node-client traits remain open in Phase 3.
- validation passed formatting, the storage boundary checker, all 161
  storage-node server tests, three compile-fail API boundary doctests, the
  focused local and Unix snapshot regressions, workspace-wide checks and
  strict Clippy, and the full parallel workspace suite (7,326 tests).

Twenty-third Phase 3 slice:

- the Unix storage-node reservation acquire, proof-validation, and heartbeat
  handlers now derive `StorageNodeActiveBucketRoute` from the exact active
  frame permit before reaching `LocalStorageNodeClient`. Each effect
  revalidates the captured route deadline and fixes the decoded bucket and
  trusted `BucketPgId`; a proof or acquire subject cannot be substituted after
  construction.
- reservation release is classified separately as `RetainedCleanup` at frame
  admission and constructs a private, one-shot
  `StorageNodeRetainedBucketWriteReservationRoute`. It binds the complete
  durable record, consumes the capability on release, and revalidates the
  admission domain/class, retained route, retained-route primary, and bucket
  placement immediately before deletion. Active authority cannot be promoted
  into retained cleanup authority.
- exact release remains deliberately available after the active route
  deadline. This is a state-reducing cleanup exception rather than renewed
  write authority: acquire, validate, and heartbeat retain the captured
  active deadline, while release cannot create or extend a reservation. The
  general capability model now records this distinction explicitly.
- the deterministic server-local regression acquires and validates a
  reservation under a 5,000ms active capability, publishes a same-generation
  extension, then proves the original capability cannot heartbeat or acquire
  at 6,000ms and leaves durable state byte-for-byte unchanged. Foreign-domain
  and wrong-class permits cannot construct cleanup authority; the exact
  retained capability then releases the original subject despite active-route
  expiry. A separate route-transition regression moves the current primary,
  retains the old route, and proves cleanup uses the retained historical
  primary rather than requiring or mutating through the successor route.
- the two-PG Unix adversarial regression now also seeds a real reservation on
  the configured but wrong bucket PG. A retained release must return exact
  `PayloadDecode` and leave that record unchanged, proving placement rejection
  precedes local deletion. Existing Unix canaries cover the complete active
  acquire/validate/heartbeat and retained release lifecycle, including an old
  reservation identity released through a newer current route.
- drains, lifecycle/delete claims, metadata-command proof release, frontend
  admission threading, capability requirements on node-client traits, and
  object/data active and retained capabilities remain open in Phase 3.
- validation passed formatting, the storage boundary checker, all 162
  storage-node server tests, the focused active/retained reservation and Unix
  wrong-PG regressions, workspace-wide checks and strict Clippy, and the full
  parallel workspace suite (7,327 tests).

Twenty-fourth Phase 3 slice:

- security review found that the frontend publication barrier's lifetime was
  still controlled by a streaming client's body duration. PUT, POST Object,
  and UploadPart held their route admission through a per-frame idle timeout,
  so a write-authorized client could trickle frames indefinitely. Once a
  replacement entered `Draining`, that one request also blocked every later
  route admission.
- streaming body waits are now bounded by both the configured per-frame idle
  timeout and the admission's captured process-monotonic route deadline. The
  captured deadline is not extended by same-generation lease renewal. Route
  expiry returns `OperationAborted` and makes one prompt retained cleanup
  attempt through an explicit non-cloneable capability narrowed to the
  admitted object. It may prepare and replicate only the exact
  `AbortStreamUpload` command, release its retained bucket-write proof, delete
  its staged shards and acknowledgements, and remove the exact pending slot.
  Local and Unix clients use dedicated retained abort RPC kinds; storage-node
  decoding revalidates the old active route, primary/acting-set role, object
  placement, command epoch, and exact abort payload.
- review showed that retrying retained cleanup indefinitely while keeping the
  frontend admission alive merely recreated the publication stall when an old
  acting-set node was unavailable. Every frontend-created PUT, POST Object,
  and UploadPart stream session now persists the admission's immutable
  authority deadline as its replicated durable cleanup handoff. POST Object
  and UploadPart derive cleanup authority before their first durable stream
  mutation; PUT derives it before authorization and reservation preparation.
  All three initial prepare/begin calls now require the captured admission and
  revalidate it immediately before accessing storage. If the
  prompt retained abort fails, the request drops its route admission and the
  stream-session sweeper resumes cleanup independently once the durable
  deadline is due. The sweeper follows the current runtime-map handle on every
  pass rather than retaining the expired map, and deadline cleanup covers both
  PutObject and UploadPart sessions. Command, storage-RPC, and canonical-state
  encoding versions advance to 3 for the new durable field; the lightweight
  command-log header decoder also skips the field so an applied create command
  can prove and remove its exact pending slot.
- promoted PUT and POST heartbeat workers now retain only weak context
  references while sleeping. They can no longer keep route admission alive
  for the production ten-second heartbeat interval after the handler and its
  cleanup worker have dropped their context.
- streamed PUT, POST Object, and UploadPart now carry the captured admission
  into capability-bearing coordinator effect APIs. Initial write preparation,
  session creation, every append, direct PUT commit, heartbeat, and stream
  finalization revalidate the
  immutable admission immediately before dispatch through its captured
  runtime-map generation; pairing it with another generation fails closed.
  Same-epoch renewal regressions complete the body before expiry, renew the
  raw generation, advance through the captured deadline, and prove that late
  append, commit, heartbeat, and finalization return `OperationAborted` while
  leaving the durable session available for cleanup.
- runtime-map installation now revalidates a bounded candidate after admitted
  requests have drained and after acquiring the current-map and generation
  locks, immediately before any pinned-generation lease or current-map
  mutation. A candidate that expired during the drain is rejected, the old map
  remains installed, the publication guard reopens admission, and the refresh
  loop reports a distinct `expired_route_map_validity` failure.
- deterministic storage tests pin the captured remaining lifetime and pause
  installation after drain while advancing the installer thread's clock,
  proving an expired candidate cannot mutate current state. HTTP coverage
  proves an authenticated streaming PUT blocks publication while its route is
  valid, promotes beyond the single-segment buffer, then route expiry removes
  its durable session, generation reservation, staged metadata, shard files,
  and acknowledgements before publishing the replacement while the client
  connection remains open. Matching promoted POST Object and UploadPart
  regressions pin retained cleanup; the POST case also proves its sleeping
  heartbeat does not retain admission. Failure-injection cases prove a failed
  prompt abort releases publication while the durable session remains, then
  deadline cleanup removes its metadata, reservation, acknowledgements, and
  shards. A same-store runtime-map refresh regression proves the background
  sweeper cleans through the newly published map. Capability-before-mutation
  hooks prove PUT, POST Object, and UploadPart cannot prepare a write or create
  a session when same-epoch renewal keeps the raw cluster live but the
  captured admission expires after cleanup authority has been derived. A
  shared timeout helper gives PUT, POST Object, and UploadPart the same
  deadline behavior.
- direct-install and background-refresh fixtures now bind candidates to valid
  local deadlines. The background fixture uses current authority time because
  caller-thread test-clock overrides are intentionally not inherited by the
  refresh worker.
- validation passed formatting, the storage boundary checker, workspace-wide
  strict Clippy, and the focused initial-mutation and post-creation expiry
  regressions for PUT, POST Object, and UploadPart. The full parallel suite
  started 7,364 tests and reached the separately fixed conditional-delete race
  failure before cancellation; the preceding slice's 7,363-test full run
  remains clean.

Twenty-fifth Phase 3 slice:

- metadata-command bucket-write proof release now enters the storage-node
  server under `RetainedCleanup` admission rather than ordinary active
  admission. The handler constructs a private, non-`Clone`, one-shot
  `StorageNodeRetainedMetadataCommandProofRoute` before reaching
  `LocalStorageNodeClient`; active authority cannot be promoted into this
  cleanup path.
- the capability binds the admission domain and class, target node, retained
  route epoch, raw and trusted bucket PG, and the complete
  `BucketWriteReservationProof`. Construction and the consuming release both
  revalidate the retained route primary, bucket placement, and exact proof
  epoch. The existing retained durable-record release shares the admission,
  primary, and placement validator but deliberately does not require its
  record epoch to equal the route epoch: delete/drain recovery must be able to
  remove an exact expired older-epoch record through the current retained
  primary.
- proof release remains deliberately usable after active route expiry because
  it can only remove the exact reservation named by the proof; it cannot
  acquire or renew authority. The Unix positive regression now binds an
  expired active route and succeeds only because frame dispatch selects the
  retained-cleanup class. Server-local adversarial coverage rejects foreign
  admission domains, active-class permits, and a mismatched proof route epoch
  while comparing the complete durable reservation before and after every
  rejection. Existing Unix recovery coverage pins older-epoch durable-record
  release through the current retained route.
- the wrong-PG Unix regression now seeds byte-for-byte equivalent reservation
  state on both configured PGs. A missing placement check would therefore
  delete the wrong-PG canary; exact `PayloadDecode` rejection leaves both
  durable records unchanged. Existing conflict and non-primary regressions
  continue to pin proof identity and retained-primary validation.
- validation passed formatting, the storage boundary checker, all eight
  focused retained proof/record and Unix adversarial regressions,
  workspace-wide strict Clippy, and the full parallel workspace suite (7,379
  tests).
- drains, lifecycle/delete claims, frontend capability migration,
  capability requirements on node-client traits, and object/data active and
  retained capabilities remain open in Phase 3.

Twenty-sixth Phase 3 slice:

- the complete bucket-write drain RPC family now constructs server-local
  route capabilities before reaching `LocalStorageNodeClient`. Begin,
  heartbeat, expired-drain cleanup, existence, and get require the exact
  active frame permit and a bucket-bound `StorageNodeActiveBucketRoute`; each
  call revalidates the frame's captured deadline immediately before the local
  read or mutation. Begin additionally binds its new drain epoch to the
  admitted route, while heartbeat binds the complete record's bucket subject.
- exact drain clear is classified separately as `RetainedCleanup` and consumes
  a private, non-`Clone` `StorageNodeRetainedBucketWriteDrainRoute`. It binds
  the admission domain/class, retained route, primary, bucket placement,
  trusted bucket PG, and complete drain record. Like exact reservation
  release, it may remove an older-epoch record through the current retained
  primary but cannot create or extend drain authority.
- a deterministic server-local regression creates and heartbeats a drain
  under a 5,000ms captured route, renews the raw server route to 10,000ms, and
  proves the original capability cannot begin, heartbeat, clear an expired
  drain, or read at 6,000ms. The durable record remains byte-for-byte intact;
  active-class and foreign-domain permits cannot construct retained authority,
  while the exact retained capability then clears the record.
- the Unix expired-route regression proves frame dispatch assigns exact clear
  to retained cleanup. A dedicated two-PG adversarial matrix seeds identical
  buckets and drain records on both configured PGs, then requires exact
  `PayloadDecode` for wrong-PG begin, heartbeat, expired clear, exact clear,
  existence, and get. The complete records on both PGs are compared after
  every rejection, so mutation bugs cannot cancel one another.
- validation passed formatting, the storage boundary checker, all six focused
  active/retained and Unix drain regressions, workspace-wide strict Clippy,
  and the full parallel workspace suite (7,382 tests).
- lifecycle/delete claims, frontend capability migration, capability
  requirements on node-client traits, and object/data active and retained
  capabilities remain open in Phase 3.

Twenty-seventh Phase 3 slice:

- the bucket-delete finalizer claim RPC family now constructs private
  server-local route capabilities before reaching `LocalStorageNodeClient`.
  Claim acquisition and lookup require a bucket-bound
  `StorageNodeActiveBucketRoute` and revalidate the frame's captured deadline
  immediately before the local operation. Acquisition additionally requires
  the requested claim epoch to equal the admitted route epoch.
- exact finalizer-claim release is classified as `RetainedCleanup` and
  consumes a private, non-`Clone`
  `StorageNodeRetainedBucketDeleteFinalizeClaimRoute`. The capability binds
  the admission domain/class, target node, retained route epoch, raw and
  trusted bucket PG, and complete claim record. Unlike older-epoch drain and
  reservation-record cleanup, the claim itself names the route epoch and PG,
  so construction and use require both identities to match exactly.
- a deterministic server-local regression acquires and reads a claim under a
  5,000ms captured route, renews the raw server route to 10,000ms, and proves
  the original active capability cannot acquire or read at 6,000ms. The
  complete durable claim remains unchanged; active-class and foreign-domain
  permits cannot construct retained authority, while the exact retained
  capability then releases the claim.
- the Unix expired-route regression proves frame dispatch assigns exact
  finalizer-claim release to retained cleanup. A dedicated two-PG adversarial
  matrix seeds equivalent deleting buckets and claims on both configured PGs,
  then requires exact `PayloadDecode` for wrong-PG acquire, lookup, and
  release. Both complete durable claim records are compared after every
  rejection, so missing placement checks and mutation-before-rejection bugs
  cannot hide behind absent or cancelling state.
- validation passed formatting, the storage boundary checker, all four
  finalizer-claim and existing finalize-coordination regressions,
  workspace-wide strict Clippy, and the full parallel workspace suite (7,385
  tests).
- lifecycle claims, frontend capability migration, capability requirements on
  node-client traits, and object/data active and retained capabilities remain
  open in Phase 3.

Twenty-eighth Phase 3 slice:

- the lifecycle-sweep claim RPC family now constructs private server-local
  route capabilities before reaching `LocalStorageNodeClient`. Acquisition,
  heartbeat, and error recording require active bucket authority and
  revalidate the frame's captured deadline immediately before mutation.
  Acquisition binds the new claim epoch to the admitted route; heartbeat and
  error recording require the complete claim bucket, epoch, and PG identity
  to match that route.
- exact lifecycle-claim release is classified separately as
  `RetainedCleanup` and consumes a private, non-`Clone`
  `StorageNodeRetainedLifecycleSweepClaimRoute`. It binds the admission
  domain/class, target node, retained route epoch, raw and trusted bucket PG,
  and complete claim. Because the claim records their issuing route, both
  construction and use require its epoch and PG to match exactly. Retained
  authority cannot acquire, heartbeat, or change claim error state.
- a deterministic server-local regression acquires, heartbeats, and records
  an error on a non-expiring claim under a 5,000ms captured route, renews the
  raw server route to 10,000ms, and proves the original active capability
  cannot acquire, heartbeat, or record another error at 6,000ms. The complete
  durable claim remains unchanged; active-class and foreign-domain permits
  cannot construct retained authority, while exact retained release succeeds.
- the Unix expired-route regression proves frame dispatch assigns exact
  lifecycle-claim release to retained cleanup. A dedicated two-PG adversarial
  matrix seeds equivalent lifecycle buckets and claims on both configured
  PGs, then requires exact `PayloadDecode` for wrong-PG acquire, heartbeat,
  error recording, and release. Both complete durable claim records are
  compared after every rejection, so missing placement checks and
  mutation-before-rejection bugs cannot hide behind absent or cancelling
  state.
- validation passed formatting, the storage boundary checker, all four
  lifecycle capability, expired-route, wrong-PG, and existing coordination
  regressions, workspace-wide strict Clippy, and the full parallel workspace
  suite (7,388 tests).
- frontend capability migration, capability requirements on node-client
  traits, and object/data active and retained capabilities remain open in
  Phase 3.

Twenty-ninth Phase 3 slice:

- object-generation allocation and reservation lookup now require a private,
  non-`Clone` `StorageNodeActivePrimaryObjectRoute` before reaching
  `LocalStorageNodeClient`. The capability binds the exact bucket, key,
  installed `ObjectMetadataPgId`, frame admission domain/class, primary route,
  and captured route deadline. Object-version selection uses the matching
  acting-set `StorageNodeActiveObjectRoute`, preserving its intentional
  all-replica maximum scan rather than incorrectly requiring the metadata
  primary.
- both capability classes revalidate the frame's immutable deadline
  immediately before node access. Primary construction preserves the prior
  route, primary, and exact object-placement validation order; decoded wire
  PGs never confer trusted object-role authority.
- a deterministic server-local regression seeds a durable generation
  reservation, exercises reservation lookup, next-generation selection, and
  next-version selection, then extends the raw same-epoch route validity and
  proves all three captured capabilities fail at their original deadline.
  Foreign-domain and retained-cleanup permits cannot construct active object
  authority, and the complete durable reservation remains unchanged.
- existing Unix positive and two-PG adversarial tests cover all three RPCs,
  including exact `PayloadDecode` rejection with equivalent wrong-PG state so
  an unguarded node access would succeed.
- direct PUT, object reads and mutations, listing scans, data operations,
  frontend capability migration, and capability requirements on node-client
  traits remain open in Phase 3.
- validation passed formatting, the storage boundary checker, the focused
  active-object deadline regression, all three Unix positive/wrong-PG
  regressions, all 2,197 storage tests, workspace-wide strict Clippy, and the
  full parallel workspace suite (7,399 tests).

Thirtieth Phase 3 slice:

- direct PUT commit snapshot loading and command construction now consume the
  exact `StorageNodeActivePrimaryObjectRoute` established from the frame.
  Both revalidate the immutable captured deadline immediately before local
  node access; the command builder derives its trusted object PG and command
  epoch from the route capability rather than accepting duplicate wire
  authority.
- capability migration exposed a contradictory-state gap in the internal RPC:
  command construction checked the routed bucket and reservation proof but
  did not require the routed object key to equal the embedded direct PUT
  request key. The handler now rejects that mismatch as exact
  `PayloadDecode`, and the capability independently binds the command request
  to its bucket/key subject before constructing a command.
- the deterministic active-object regression now loads a real direct PUT
  snapshot, builds and validates its command, rejects a mismatched routed key,
  extends the raw same-epoch route validity, and proves both operations fail
  at the capability's original deadline while the durable generation
  reservation remains exact.
- review tightened the command capability so it no longer accepts a separate
  reservation proof: command construction derives the proof from the bound
  direct PUT request and rejects request subjects or proof buckets that do not
  match its object route. Direct capability regressions independently pin both
  rejection paths instead of relying on the RPC handler's defensive prechecks.
- existing Unix snapshot, command-build, and equivalent-state wrong-PG tests
  remain positive and adversarial canaries for the converted wire boundary.
- object reads and mutations, listing scans, data operations, frontend
  capability migration, and capability requirements on node-client traits
  remain open in Phase 3.
- validation passed formatting, the storage boundary checker, the focused
  active-object/direct-PUT deadline, key-binding, and proof-binding regression,
  all three Unix direct-PUT/wrong-PG regressions, all 2,197 storage tests,
  workspace-wide strict Clippy, and the full parallel workspace suite (7,399
  tests).

Thirty-first Phase 3 slice:

- object-read authorization-subject loading, coherent snapshot loading, and
  subject-bound tag loading now consume the exact
  `StorageNodeActivePrimaryObjectRoute` established from the frame. The
  capability fixes the object subject and trusted `ObjectMetadataPgId`, and
  revalidates the frame's immutable captured deadline immediately before each
  local node-client access.
- the RPC handlers preserve the existing `ObjectNotFound` and stale-subject
  protocol outcomes while route failures remain distinct storage RPC errors.
  Decoded node, epoch, PG, bucket, and key fields therefore cannot reach any
  of the three object-read effects without active admission, primary
  validation, and exact object placement.
- the deterministic active-object regression now seeds a tagged live object,
  reads its authorization subject, coherent standard-segment snapshot, and
  tags through the capability, then extends the raw same-epoch route and
  proves all three reads fail at the original captured deadline. Fixture
  creation precedes direct-PUT command construction so it does not bypass or
  contend with the PG pending-command protocol.
- existing Unix positive coverage exercises the full subject/snapshot/tag
  sequence. The two-PG adversarial matrix stores byte-identical objects and
  tags on both configured PGs, proves the identities are equal, and requires
  exact `PayloadDecode` for all three wrong-PG reads, so missing placement
  validation cannot hide behind absent or stale state.
- object mutations, listing scans, data operations, frontend capability
  migration, and capability requirements on node-client traits remain open in
  Phase 3.
- validation passed formatting, the storage boundary checker, the focused
  active-object read deadline regression, all three Unix object-read positive
  and equivalent-state wrong-PG regressions, all 2,199 storage tests,
  workspace-wide strict Clippy, and the full parallel workspace suite (7,422
  tests).

Thirty-second Phase 3 slice:

- object-metadata PUT and current/specific object-delete snapshot loading now
  consume the exact `StorageNodeActivePrimaryObjectRoute` established from
  the frame. Metadata PUT, current/specific delete, and delete-marker command
  construction use the same capability and derive their trusted
  `ObjectMetadataPgId`, command epoch, bucket, and key from it rather than
  decoded request fields.
- command capability construction preserves the existing fail-closed
  precedence by rejecting a reservation proof for another bucket before route
  validation. Each command effect revalidates the immutable captured deadline
  and binds the proof's epoch, operation kind, bucket, and exact object-key
  target immediately before reaching the local node client, so a same-bucket
  proof from another operation, object, or epoch cannot be substituted after
  capability construction.
- capability migration exposed an additional identity gap: the explicit
  stale-payload form of an insert-delete-marker request could supply an
  arbitrary reclaim record, including another bucket/key or another retained
  generation of the same object. Storage-node capability paths now reject
  every explicit payload record as exact `PayloadDecode` before command
  allocation. They additionally bind reclaim mode to marker identity: a null
  marker must derive reclaim from the current null live-object snapshot, while
  a numbered marker must carry no reclaim or expected source. Any supplied
  snapshot source must itself be a null live object for the exact routed
  subject.
- the deterministic active-object regression positively loads all three
  mutation snapshots, rejects substituted proof and stale-payload subjects,
  renews the raw same-epoch route, and proves all seven snapshot/command
  effects fail at the capability's original deadline. The Unix wire
  regression independently pins stale-payload substitution, crossed null and
  numbered marker reclaim modes, and same-bucket operation/key/epoch proof
  substitution, while existing positive and equivalent-state wrong-PG
  coverage exercises the complete converted handler family before node
  access.
- lifecycle object-version scans, stream and multipart mutations, payload
  reclaim, remaining object mutations, listing scans, data operations,
  frontend capability migration, and capability requirements on node-client
  traits remain open in Phase 3.
- validation passed formatting, the storage boundary checker, the focused
  active-object mutation deadline/substitution regression, both Unix mutation
  positive and equivalent-state wrong-PG regressions, all 2,199 storage tests,
  workspace-wide strict Clippy, and the full parallel workspace suite (7,422
  tests).

Thirty-third Phase 3 slice:

- the exact-object version scan used by lifecycle expiration now consumes the
  `StorageNodeActivePrimaryObjectRoute` established from the frame. The
  capability fixes the trusted `ObjectMetadataPgId`, bucket, and key and
  revalidates the frame's immutable captured deadline immediately before the
  local metadata read.
- the handler retains the existing request-shape precedence: a lifecycle scan
  carrying a version ID is rejected as `PayloadDecode` before route
  construction. Valid requests can no longer reach the node client without
  active admission, primary validation, and exact object placement.
- the deterministic active-object regression positively reads the complete
  version list and proves the scan fails at the capability's original
  deadline after a raw same-epoch validity extension. The Unix two-PG
  regression reads the expected list through the correct route, then requires
  exact `PayloadDecode` for an equivalent-state wrong PG that would otherwise
  return the same object history.
- stream and multipart mutations, payload reclaim, remaining object
  mutations, PG-wide listing scans, data operations, frontend capability
  migration, and capability requirements on node-client traits remain open
  in Phase 3.
- validation passed formatting, the storage boundary checker, the focused
  active-object deadline regression, both Unix lifecycle positive and
  equivalent-state wrong-PG paths, all 2,200 storage tests, workspace-wide
  strict Clippy, and the full parallel workspace suite (7,436 tests).

Thirty-fourth Phase 3 slice:

- exact-object multipart upload loading, in-progress upload loading, and the
  listing-specific in-progress load now consume the
  `StorageNodeActivePrimaryObjectRoute` established from the frame. The
  capability fixes the trusted `ObjectMetadataPgId`, bucket, and key and
  revalidates the frame's immutable captured deadline immediately before each
  local metadata read.
- the shared handler retains the operations' distinct public error domains:
  ordinary upload loading continues to expose `BucketSnapshotLoadError`, while
  the two in-progress forms continue to expose `ObjectPgActionError`.
  `NoSuchUpload` remains a successful protocol outcome for all three forms,
  and route failure remains a distinct storage RPC error.
- the outer RPC authorization registry already classifies all three messages
  for frontend and maintenance callers. This slice preserves that role policy
  while ensuring the decoded route and object subject remain untrusted until
  the post-auth active-primary capability is constructed.
- the deterministic active-object regression now positively loads the same
  in-progress upload through all three capability methods, extends the raw
  same-epoch route validity, and proves each method fails at the capability's
  original captured deadline. The existing Unix two-PG multipart regression
  seeds an identical upload and part on both PGs, proves all three correct-PG
  reads succeed, and requires exact `PayloadDecode` from all three wrong-PG
  requests, so rejection cannot be caused by absent multipart state.
- stream and multipart mutations, payload reclaim, remaining object
  mutations and lookups, PG-wide listing scans, data operations, frontend
  capability migration, and capability requirements on node-client traits
  remain open in Phase 3.
- validation passed formatting, the storage boundary checker, the focused
  captured-deadline and equivalent-state Unix wrong-PG regressions, all 2,209
  storage tests, workspace-wide strict Clippy, and the full parallel workspace
  suite (7,446 tests).

Thirty-fifth Phase 3 slice:

- multipart completion snapshot and preflight loading, authorized part
  listing, management lookup, completion stale-payload-source loading, and
  abort-cleanup loading now consume the exact
  `StorageNodeActivePrimaryObjectRoute` established from the frame. Each
  capability method derives its trusted `ObjectMetadataPgId`, bucket, and key
  from that route and revalidates the frame's immutable captured deadline
  immediately before its local metadata read.
- the three operations carrying an `AuthorizedMultipartUploadRecord` now bind
  that record's bucket and key to the active route inside the capability. A
  decoded record for another object is rejected as exact `PayloadDecode`
  before node access instead of using the caller-controlled record subject
  with the route's trusted PG.
- the handlers retain their existing `NoSuchUpload`, `PartNotFound`, missing,
  and stale-snapshot protocol outcomes. The outer RPC authorization registry
  remains frontend-only for completion snapshot/preflight, part listing, and
  management lookup, and frontend/maintenance for stale-source and abort
  cleanup.
- the deterministic active-object regression positively exercises all six
  methods, uses a second same-PG object route to prove an otherwise readable
  authorized upload is rejected at the capability subject boundary, extends
  raw same-epoch validity, and proves every method fails at the original
  captured deadline. The existing Unix multipart matrix seeds identical
  upload and part state on two PGs, proves all correct-PG paths succeed, and
  requires exact `PayloadDecode` for all equivalent-state wrong-PG paths.
- stream and multipart mutations, payload reclaim, remaining object
  mutations and lookups, PG-wide listing scans, data operations, frontend
  capability migration, and capability requirements on node-client traits
  remain open in Phase 3.
- validation passed formatting, the storage boundary checker, the focused
  captured-deadline/subject-binding and equivalent-state Unix wrong-PG
  regressions, all 2,209 storage tests, workspace-wide strict Clippy, and the
  full parallel workspace suite (7,446 tests).

Thirty-sixth Phase 3 slice:

- CreateMultipartUpload retry matching and command construction now consume
  the exact `StorageNodeActivePrimaryObjectRoute` established from the frame.
  Both capability methods derive the trusted object PG and epoch from that
  route and revalidate its immutable captured deadline immediately before the
  local metadata operation.
- the capability binds the decoded create request's bucket and key, any
  retry-matched command's upload subject, and any caller-supplied current
  object snapshot to the active object route. It also binds both retry-command
  and new-command reservation proofs to the exact
  `create-multipart-upload` operation, route key, route epoch, and bucket.
- the handlers preserve the existing authorization registry and public error
  mappings. In particular, proof bucket mismatch retains its established
  precedence over route validation, while all remaining decoded route,
  subject, and proof evidence stays untrusted until the post-auth capability
  is constructed.
- the deterministic active-route regression proves positive retry matching
  and command construction, rejects mismatched create and expected-command
  subjects and a same-bucket proof for another operation, and proves both
  methods fail at the capability's original deadline after raw same-epoch
  route renewal. The Unix regressions retain a correctly routed positive
  canary and require exact `PayloadDecode` for equivalent-state wrong-PG
  matching and command construction.
- stream and remaining multipart mutations, payload reclaim, remaining object
  mutations and lookups, PG-wide listing scans, data operations, frontend
  capability migration, and capability requirements on node-client traits
  remain open in Phase 3.
- validation passed formatting, the storage boundary checker, the focused
  active-route and Unix positive/wrong-PG regressions, all 2,209 storage tests,
  workspace-wide strict Clippy, and the full parallel workspace suite (7,446
  tests).

Thirty-seventh Phase 3 slice:

- stream-upload retry matching, session loading, segment listing, PutObject
  reservation-proof renewal, and segment-append preparation now consume the
  exact `StorageNodeActivePrimaryObjectRoute` established from the admitted
  frame. Each effect derives its trusted object PG, bucket, key, and placement
  epoch from that capability and revalidates the immutable captured deadline
  immediately before local node access.
- retry matching binds both the decoded create request and any expected
  `CreateStreamUploadCommand` to the active object subject. The command's
  bucket-write proof must match the route epoch and key plus the operation
  implied by its target: `put-object-stream-create` or
  `upload-part-stream-create`.
- PutObject stream proof renewal now independently binds both the current and
  renewed proofs to `put-object-stream-create`, the active route, and one
  stable reservation identity before mutation. Only the lease deadline may
  differ. Append preparation stamps the segment with the capability's trusted
  epoch rather than the decoded wire epoch.
- the deterministic active-object regression creates a durable PutObject
  stream and an UploadPart stream and positively exercises both retry-proof
  branches plus the other four operations. It rejects a foreign command
  subject, PutObject/UploadPart crossed operation proofs in both directions,
  and a mismatched renewal identity, then extends raw same-epoch route validity
  and proves every effect fails at the capability's original deadline while
  the durable renewed proof remains exact. The two stream-create operation
  names are canonical metadata-command constants shared by acquisition and
  validation. Existing Unix regressions retain correct-route canaries and
  equivalent-state wrong-PG rejection for session load, segment load, proof
  renewal, and append preparation; the absent-session matrix independently
  pins wrong-PG retry matching.
- stream creation/finalization and remaining multipart mutations, retained
  stream cleanup, payload reclaim, PG-wide listing scans, data operations,
  frontend capability migration, and capability requirements on node-client
  traits remain open in Phase 3.
- validation passed formatting, the storage boundary checker, the focused
  capability and Unix positive/wrong-PG regressions, all 2,209 storage tests,
  workspace-wide strict Clippy, and the full parallel workspace suite (7,452
  tests).

Thirty-eighth Phase 3 slice:

- PutObject and UploadPart stream-session creation command construction now
  consumes the exact `StorageNodeActivePrimaryObjectRoute` established from
  the admitted frame. The capability derives the trusted object PG and
  placement epoch from the route and revalidates its immutable captured
  deadline immediately before local command construction.
- the capability binds the decoded create request to the active bucket/key,
  requires its precondition variant to match the stream target, and binds any
  current-object or multipart-upload precondition to that same subject. An
  UploadPart precondition must also name the upload ID carried by the target.
  The reservation proof must match the route epoch and key plus the exact
  target-specific operation: `put-object-stream-create` or
  `upload-part-stream-create`.
- the storage-node handler preserves the existing stale-snapshot,
  missing-upload, and command-build error mappings, but no longer reconstructs
  authority from raw decoded PG/epoch fields before calling the local client.
  Proof bucket mismatch retains its established pre-route precedence; all
  other subject, precondition, and proof evidence is checked by the
  post-auth server-local capability.
- the deterministic active-route regression positively constructs commands
  for PutObject with both current-snapshot modes and for UploadPart. It rejects
  a foreign create subject, a foreign current-object snapshot, a crossed
  target/precondition, crossed PutObject/UploadPart proof authority, and an
  UploadPart snapshot for another upload ID. Both target paths fail at the
  capability's original deadline after same-epoch route renewal.
- the equivalent-state two-PG Unix regression now constructs a successful
  UploadPart stream command on the routed PG, requires exact `PayloadDecode`
  for the same durable upload on a forged PG, and independently rejects a
  same-route PutObject proof substituted into UploadPart creation. Existing
  PutObject, missing-upload, wrong-PG, and stale-epoch command tests now use
  their canonical target-specific proof operations so another rejection
  cannot mask the behavior under test.
- review correction: Unix response validation now also requires the returned
  command's `cleanup_after` value to equal the request exactly. An adversarial
  validator regression mutates only that deadline and rejects the response,
  preventing a faulty node from shortening, removing, or extending durable
  abandoned-session cleanup.
- stream finalization and remaining multipart mutations, retained stream
  cleanup, payload reclaim, PG-wide listing scans, data operations, frontend
  capability migration, and capability requirements on node-client traits
  remain open in Phase 3.
- validation passed formatting, the storage boundary checker, the focused
  capability and Unix positive/wrong-PG regressions, all 2,209 storage tests,
  workspace-wide strict Clippy, and the full parallel workspace suite (7,463
  tests).

Thirty-ninth Phase 3 slice:

- PutObject and UploadPart streaming-finalization snapshot loads and metadata
  command construction now consume the exact
  `StorageNodeActivePrimaryObjectRoute` established from the admitted frame.
  The capability derives the object PG and placement epoch from the route and
  revalidates its immutable captured deadline immediately before each load or
  command build.
- PutObject finalization binds the session, in-progress state, staged segment
  identities, current-object stale-payload source, and reclaim description to
  the active bucket/key. Its commit command must carry the exact
  `put-object-stream-create` proof used by the durable session. UploadPart
  finalization independently binds the session target, multipart upload,
  existing part, displaced segments, committed part, and committed segments
  to one upload/part and requires the distinct
  `upload-part-stream-finalize` proof.
- the finalize proof operation is now a canonical metadata-command constant
  shared by acquisition and server-local validation. The storage-node
  handlers preserve stale-snapshot and public error mappings, but no longer
  pass decoded PG/epoch authority directly to the local command builder.
- the deterministic active-route regression positively loads both snapshots
  and constructs both command kinds. It rejects foreign snapshot subjects,
  a foreign committed-part identity, and crossed PutObject/UploadPart finalize
  proofs, then proves all four effects fail at the capability's original
  deadline after a same-epoch route renewal. The equivalent-state two-PG Unix
  matrix adds correct-route command canaries, exact wrong-PG rejection, and
  crossed-operation proof rejection for both finalizers; the stale-epoch
  UploadPart regression now uses a semantically valid finalize proof.
- review correction: PutObject finalization no longer accepts a caller-chosen
  command proof. It derives the sole command proof from the snapshotted durable
  stream session, while the current bucket snapshot uses a temporary
  reservation that is never transferred to the object command. The redundant
  second stream-create proof was removed from `CommitDirectPutObjectCommand`,
  so successful publication can release exactly one proof. This incompatible
  command-layout change advances the fail-closed metadata-command encoding to
  version 4 and rejects version 3.
- the active-route capability and local command builder independently require
  the RPC proof to equal the persisted session proof, and Unix response
  validation binds the returned command proof to that snapshot. Direct
  active-route, direct local, and Unix RPC regressions substitute a second
  otherwise-valid proof, while an adversarial response regression mutates only
  the returned proof. A cluster regression creates two simultaneously valid
  PutObject stream reservations for the same key, finalizes one session, and
  proves the other session and its exact durable reservation remain intact.
- proof binding is also enforced centrally by `PgMetadataStore` before any new
  direct-PUT command application, including generic pending-command recovery.
  Binding compares the reservation's stable identity rather than its renewable
  lease deadline: heartbeat renewal persists the newer deadline on the object
  PG primary, while replicas may retain the create-time deadline until the
  terminal command removes the session. A three-replica regression proves this
  divergence exists, then heartbeats and finalizes the stream and requires all
  object replicas, command-log states, and bucket-reservation replicas to
  converge.
  A current-format recovery regression installs a commit for one stream session
  carrying a second live same-key session's proof. It proves the command and
  proof are independently admissible, then verifies direct application and the
  generic drain both reject the substitution without advancing any replica,
  publishing an object, clearing the pending slot, deleting either session, or
  releasing either reservation. Exact already-recorded command replay retains
  its separate terminal-cleanup path. Failure and stale-route regressions prove
  the live session reservation remains owned by the stream until abort or
  successful publication.
- remaining multipart mutations, retained stream cleanup, payload reclaim,
  PG-wide listing scans, data operations, frontend capability migration, and
  capability requirements on node-client traits remain open in Phase 3.
- validation passed formatting, the storage boundary checker, the focused
  active-route and Unix RPC regressions, all 2,211 storage tests, and
  workspace-wide strict Clippy. The full parallel workspace suite remains a
  pre-commit check for this slice.

Fortieth Phase 3 slice:

- CompleteMultipartUpload and both ordinary and authorization-bound
  AbortMultipartUpload metadata-command builders now consume the admitted
  `StorageNodeActivePrimaryObjectRoute`. The server-local capability derives
  the trusted object PG and placement epoch from that route and revalidates
  its immutable captured deadline immediately before command construction.
- completion binds the bucket, key, upload ID, selected and omitted part
  records, selected and omitted streaming segments, terminal stream cleanup,
  and any null-version stale-payload source to one routed object and upload.
  The capability derives the committed object-part layout from the installed
  topology instead of accepting it as caller authority and requires the exact
  `complete-multipart-upload` reservation operation for the routed key.
- both abort variants require the canonical `abort-multipart-upload`
  reservation operation and bind every cleanup row to the routed upload. The
  authorization-bound variant additionally requires the authorized upload and
  cleanup upload snapshot to be identical. The cluster acquisition paths now
  share the same canonical operation constant rather than repeating string
  literals.
- the storage-node handlers retain missing, stale-snapshot, and command-build
  error mappings, but no longer reconstruct command authority from decoded
  raw PG/epoch fields. The equivalent-state two-PG Unix matrix adds successful
  completion and abort canaries, exact wrong-PG rejection, and crossed
  completion/abort proof rejection for all three builders.
- the deterministic active-route regression seeds a real multipart part and
  positively constructs all three terminal commands. It rejects a foreign
  completion subject, a foreign abort-cleanup snapshot, and crossed operation
  proofs, then proves all three effects fail at the capability's original
  deadline after a same-epoch route renewal.
- retained stream cleanup, payload reclaim, PG-wide listing scans, data
  operations, frontend capability migration, and capability requirements on
  node-client traits remain open in Phase 3.
- validation passed formatting, the storage boundary checker, the focused
  capability and Unix RPC regressions, all 2,222 storage tests, and
  workspace-wide strict Clippy, plus the full parallel workspace suite (7,483
  tests).

Forty-first Phase 3 slice:

- retained stream-upload cleanup now enters storage through explicit retained
  route capabilities rather than separately validating decoded route fields
  before raw local-client calls. Prepare and pending-slot finish require the
  retained historical primary; replica application requires an admitted
  retained acting-set member. Each effect revalidates the retained admission
  domain, active historical route, role, object placement, cluster epoch,
  command PG, command kind, bucket/key/session subject, staged-segment subject,
  and optional stream-create reservation subject immediately before node
  access.
- abort command application now centrally binds the optional stream-create
  reservation to the proof persisted in the durable stream session. The
  comparison uses the complete stable reservation identity while deliberately
  excluding the renewable lease deadline, so a primary heartbeat followed by
  replica application remains convergent. Missing, added, or substituted
  proofs fail closed before session or segment mutation on every ordinary,
  recovery, and retained-cleanup application path.
- the Unix response validator independently rejects returned abort commands
  whose command route, segment sessions, or reservation operation/target do
  not match the requested cleanup subject. Its syntactic operation check
  accepts both canonical stream-create operations. UploadPart create proofs
  are transient command authority and are not persisted on the stream session,
  so ordinary retained UploadPart abort commands remain proofless; any supplied
  proof must still match the durable session exactly at central application.
- the expired-route Unix regression now seeds equivalent sessions, staged
  segments, generation reservations, and an exact pending abort on both object
  PGs. Wrong-PG prepare, apply, and finish calls must return `PayloadDecode`,
  after which the complete wrong-PG state remains exact; correct-PG retained
  cleanup remains a positive canary. A PgStore command-dispatch regression
  rejects an otherwise-valid same-key proof substitution without mutation and
  accepts the same stable proof with a replica-lagging lease deadline. The same
  expired-route Unix workflow also cleans a staged UploadPart stream while
  preserving its in-progress multipart upload.
- payload reclaim, PG-wide listing scans, data operations, frontend capability
  migration, and capability requirements on node-client traits remain open in
  Phase 3.
- validation passed formatting, the storage boundary checker, the focused
  retained-cleanup and central command-application regressions, all 2,224
  storage tests, workspace-wide strict Clippy, and the full parallel workspace
  suite (7,485 tests).

Forty-second Phase 3 slice:

- subject-specific payload-reclaim existence checks, reclaim-record loading,
  and durable claim acquisition now consume the exact
  `StorageNodeActivePrimaryObjectRoute` established from the admitted frame.
  The capability derives the trusted object PG, bucket, key, and cluster epoch
  from that route and revalidates its immutable captured deadline immediately
  before each local metadata read or claim mutation.
- object-payload reclaim claim release is now explicitly retained-cleanup work,
  rather than an active frame that happened to use a cleanup route lookup. A
  retained claim capability binds the decoded node, historical route epoch,
  object PG, bucket, key, and complete durable claim, requires the retained
  primary route, and repeats those checks immediately before exact release.
  The shared retained object-route validator now serves both stream abort and
  reclaim cleanup without stream-specific naming.
- the server no longer exposes its raw object-PG construction helper for these
  request handlers. The existing equivalent-state two-PG Unix matrix still
  proves positive correctly routed operations and exact `PayloadDecode` for
  every wrong-PG subject-specific reclaim operation. A deterministic
  server-local capability regression extends the raw same-epoch route, proves
  the captured existence/load/claim authority expires at its original
  deadline without mutating the exact durable claim, carries the claim's exact
  `(cluster_epoch, pg_id)` through two route transitions, and then releases
  that claim through retained authority. Durable history-reference collection,
  its coarse retention summary, storage/control-plane codecs, persisted
  control-plane state, and diagnostics now have a distinct object-payload
  reclaim-claim reference class. The control-plane command, control-plane RPC,
  persisted control-plane state, and storage-RPC versions advance to 13, 9,
  26, and 4 respectively so older binaries fail closed. Exact release removes
  that history reference. A Unix regression independently proves exact claim
  release succeeds after active route expiry.
- PG-wide reclaim-root and claim discovery remain grouped with the open
  object-metadata scan capability work. Payload lease/fence control, data-shard
  deletion, remaining data operations, frontend capability migration, and
  capability requirements on node-client traits also remain open in Phase 3.
- focused payload-reclaim capability and Unix transport regressions, formatting,
  the storage boundary checker, all 2,230 storage tests, and workspace-wide
  strict Clippy pass. The full parallel workspace suite also passes (7,508
  tests).

Forty-third Phase 3 slice:

- every remaining PG-wide object-metadata scan now consumes one
  `StorageNodeActivePrimaryObjectScanRoute` established from the admitted RPC
  frame. This includes object, object-version, multipart-upload, bucket stream,
  PG stream, bucket reclaim-root, PG reclaim-root, reclaim-claim, and shard
  scavenger payload-reference scans. The capability is non-cloneable and
  borrows active admission, derives its trusted `ObjectMetadataScanPgId` only
  after current route and primary validation, and revalidates its immutable
  captured deadline immediately before each local metadata read.
- returned roots are bound back to the capability's scan PG through installed
  object placement. Bucket-scoped discovery additionally requires the returned
  bucket to equal the request, while claim discovery requires the durable
  claim's recorded PG and object placement to match the scan route. This keeps
  response validation inside the server-local effect boundary rather than
  relying only on the Unix client validator.
- the RPC wire shapes, allocation bounds, admission classes, and each
  operation's existing frontend or maintenance role policy are unchanged. The
  installed/unknown scan-PG Unix matrix now covers all scan interfaces.
  Existing equivalent-state two-PG regressions retain positive routed roots
  and claims and exact `PayloadDecode` when a row stored on another scan PG
  names an object placed elsewhere. The deterministic server-local reclaim
  regression rejects foreign and retained-cleanup admission, positively invokes
  every scan method, extends the raw same-epoch route, and proves each read
  fails at the capability's original deadline before node access.
- payload lease/fence control, data-shard deletion, remaining data operations,
  frontend capability migration, and capability requirements on node-client
  traits remain open in Phase 3.
- focused active-scan capability and Unix installed/unknown/equivalent-state
  regressions, formatting, the storage boundary checker, all 2,230 storage
  tests, workspace-wide strict Clippy, and the full parallel workspace suite
  pass (7,508 tests).

Forty-fourth Phase 3 slice:

- shard-payload deletion and primary shard-acknowledgement deletion now consume
  distinct non-cloneable retained-cleanup capabilities established from the
  admitted RPC frame. Both capabilities require the server's admission domain,
  reject active admission, validate the exact node/epoch/PG against the current
  or retained cleanup route, and construct `DataPgId` only after that route is
  trusted. Acknowledgement deletion additionally binds the retained primary.
- the payload-delete capability binds the complete `ShardLocation` and
  `ShardKey`, rejects a location whose shard index does not match the key, and
  revalidates the retained route and subject immediately before acquiring the
  read/delete exclusion fence and deleting the shard file. The acknowledgement
  capability likewise revalidates immediately before deleting its exact
  durable row. The old handler-local cleanup conversion is removed; RPC wire
  shapes and terminal idempotence remain unchanged.
- deterministic capability coverage rejects active and foreign admission,
  rejects a crossed location/key subject while preserving the exact payload,
  and positively deletes both the payload and acknowledgement row. The Unix
  handler matrix retains correct-route and idempotent positive canaries,
  primary enforcement, exact `UnknownPg` failures, read-handle exclusion, and
  now verifies invalid-route requests cannot delete an existing payload.
- payload lease/fence control, remaining active and historical data operations,
  frontend capability migration, and capability requirements on node-client
  traits remain open in Phase 3.
- focused retained data-delete regressions, formatting, the storage boundary
  checker, all 2,236 storage tests, workspace-wide strict Clippy, and the full
  parallel workspace suite pass (7,494 tests).

Forty-fifth Phase 3 slice:

- object-payload lease control now carries the route epoch on every node-client
  call and Unix RPC. Acquire, count, and reclaim-begin require active route
  admission and an immutable current-route fence revalidated immediately
  before touching node state. Release, reclaim-finish, retained-fence finish,
  and fence clear are decoded before admission and use the retained-cleanup
  domain, so runtime-map draining cannot strand cleanup behind publication.
- each Unix lease session is now bound to one epoch and exact
  bucket/key/generation subject. A crossed or unacquired release fails before
  mutation, an exact retry after response loss returns the recorded terminal
  count, and disconnect cleanup releases only a lease the session still owns.
- reclaim begin, finish, and clear carry the exact durable reclaim-claim proof.
  Node state stores that proof with both its active reclaim and fence; cleanup
  from a different claim cannot clear either. A later durable worker can adopt
  a retained fence only after the prior active worker has finished, preventing
  late cleanup from the prior worker from clearing the replacement fence.
- the incompatible lease-control wire extension advances the storage RPC frame
  encoding to version 5 and explicitly rejects version 4. Codec coverage spans
  every control operation and its optional authority. Deterministic local,
  capability, session, and installed Unix regressions cover admission-domain
  crossing, foreign admission, crossed release subjects, crossed reclaim
  claims, retained-fence handoff, and exact terminal retry.
- remaining active and historical data operations, frontend capability
  migration, and capability requirements on the node-client traits remain open
  in Phase 3.
- focused lease/fence capability and codec regressions, formatting, the storage
  boundary checker, all 2,240 storage tests, workspace-wide strict Clippy, and
  the full parallel workspace suite pass (7,498 tests).

Forty-sixth Phase 3 slice:

- active shard write, repair-write, full-read, and range-read handlers now
  consume a non-cloneable `StorageNodeActiveShardRoute` established from the
  admitted RPC frame. The capability derives `ShardLocation` only after
  current route validation, binds it to the exact `ShardKey`, rejects a
  crossed location/key shard index, captures the immutable current-route
  fence, and revalidates that fence immediately before every file effect.
- primary shard-ack record, validate, and load handlers now use a distinct
  `StorageNodeActivePrimaryDataRoute`. It constructs `DataPgId` only after
  current route and primary validation, borrows active admission, and
  revalidates the captured route deadline immediately before each durable ack
  read or mutation. Wire shapes and existing exact-retry behavior are
  unchanged.
- deterministic server-local coverage positively exercises every capability
  method, rejects retained and foreign admission, rejects a crossed
  location/key subject, extends the same-epoch raw route, and proves the
  captured capabilities still expire at their original deadline without
  changing shard payload or acknowledgement canaries. The existing shard RPC
  codec continues to reject crossed subjects at both encode and decode, while
  installed Unix tests retain route, primary, integrity, exact-retry, and
  non-mutation coverage.
- historical shard/ack inspection, read-handle session control, remaining
  frontend capability migration, and capability requirements on the
  node-client traits remain open in Phase 3.
- focused active-data capability and handler regressions, formatting, the
  storage boundary checker, all 2,241 storage tests, workspace-wide strict
  Clippy, and the full parallel workspace suite pass (7,506 tests).

Forty-seventh Phase 3 slice:

- historical shard payload and acknowledgement inspection now run in the
  retained-cleanup admission domain rather than holding active publication
  admission. Separate non-cloneable server-local capabilities validate the
  exact retained node/epoch/PG route immediately before each read. Payload
  inspection additionally binds the complete `ShardLocation` to the exact
  `ShardKey`; acknowledgement inspection requires the node to be the primary
  recorded by that retained route.
- the historical acknowledgement node-client method now requires the retained
  route epoch. Unix requests carry that epoch instead of substituting the
  client's current epoch, allowing the server to distinguish the historical
  primary from the current primary. The wire shape and encoding version are
  unchanged because the epoch field was already present.
- the former configured-PG-only historical validation path is removed. A node
  that merely retains local PG files but is absent from the exact current or
  historical acting set can no longer inspect them through RPC; an
  unretained route likewise fails closed. Retained inspection remains
  available after current route-map expiry so repair and backfill can finish
  from durable historical authority. It can run during draining and must
  complete before route-map publication proceeds.
- deterministic capability coverage rejects active and foreign admission,
  crossed shard subjects, unretained epochs, and a non-primary historical ack
  route while preserving positive payload and ack reads after current-route
  expiry. Installed Unix regressions cover exact historical payload routing,
  cross-epoch acknowledgement loading, and reconstruction through retained
  routes. The remote backfill workflow now installs the same retained source
  route on the frontend and storage nodes, proving historical acknowledgement
  loading through the production cross-epoch topology rather than relying on
  configured-PG fallback.
- read-handle session control, remaining frontend capability migration, and
  capability requirements on the node-client traits remain open in Phase 3.
- focused retained-inspection and installed Unix regressions, the boundary
  checker, formatting, the full storage suite (2,242 tests), workspace-wide
  strict Clippy, and the full parallel workspace suite (7,510 tests) pass.

Forty-eighth Phase 3 slice:

- read-handle acquisition now requires a non-cloneable
  `StorageNodeActiveReadHandleAcquireRoute` borrowed from the admitted RPC
  frame. The capability validates every current route, binds each complete
  `ShardLocation` to its exact `ShardKey`, owns the complete sorted acquisition
  subject and read-operation ID, and mutably borrows the exact server session
  whose registry/node domain was validated at construction. It captures the
  immutable current-route fence and revalidates that fence immediately before
  changing that session's node-wide deletion-exclusion registry.
- explicit read-handle release now runs in retained-cleanup admission and uses
  a separate session-bound capability. It deliberately does not re-resolve the
  old shard routes: the successful acquisition and the live Unix session are
  the authority for releasing only that session's exact operation ID. The
  capability mutably borrows the validated target session, so it cannot later
  be applied to another session. Session disconnect continues to release its
  owned handles through RAII without a second route lookup.
- the long-lived read-handle lease intentionally does not retain an active
  route-admission permit. Its deletion-exclusion entries survive route-map
  publication, while publication remains free to drain the acquisition frame.
  Explicit release is admitted during `Draining`, must complete before
  publication advances, and remains usable after an earlier publication has
  replaced the route that authorized acquisition.
- deterministic capability coverage rejects retained and foreign acquisition
  admission, crossed location/key subjects, active and foreign release
  admission, a foreign read-handle registry with the correct node, a foreign
  node with the correct registry, and acquisition after the capability's
  captured deadline even after a same-epoch raw-route renewal. It also proves
  releasing one operation cannot release another. An installed Unix regression
  proves a live handle survives publication, releases afterward, and a
  successor handle releases while the next publication is draining.
- remaining frontend capability migration and capability requirements on the
  node-client traits remain open in Phase 3.
- focused capability and installed Unix regressions, all 2,244 storage tests,
  formatting, the storage boundary checker, workspace-wide strict Clippy, and
  the full parallel workspace suite pass (7,512 tests).

Forty-ninth Phase 3 slice:

- ordinary buffered S3 dispatch now passes the already-authenticated request's
  `StorageClusterRouteAdmission` into the routed-operation adapter. `HeadBucket`
  is the first non-streaming coordinator workflow migrated through that
  boundary: its bucket, policy, and conditional ABAC-tag view are loaded as one
  snapshot through an `ActiveBucketRoute` derived from the request admission.
- the production raw `Coordinator::head_bucket` entry point is removed, so the
  HTTP workflow cannot silently resample the renewable runtime-map handle after
  admission. Crate-local authorization tests retain a helper which acquires a
  fresh admission before calling the same production path; direct authorization
  tests remain test-only.
- a deterministic regression captures a finite request admission, renews the
  underlying same-generation route so the raw cluster remains usable, advances
  beyond the captured deadline, and requires `HeadBucket` to fail as
  `OperationAborted` before its bucket snapshot load. Existing active-bucket
  route coverage continues to exercise the same snapshot operation through an
  installed Unix storage-node client.
- parsed bucket-policy cache reuse now compares the cached execution,
  incarnation, and policy generations directly with the already-admitted
  `LoadedBucketHandle`. It no longer performs a second raw fast-path identity
  lookup through the renewable cluster handle. The admitted loader also binds
  the admission to this coordinator's exact publication-admission gate and
  runtime-map generation before deriving its bucket route, rejecting a
  capability minted by another coordinator domain even when both handles
  currently contain the same `Arc<StorageCluster>`.
- a warm-policy-cache regression installs a failing raw-identity lookup hook,
  proves `HeadBucket` succeeds without consuming that hook while its captured
  admission is valid, then proves same-generation renewal cannot extend that
  admission. A cache-unit matrix independently rejects mismatched execution,
  incarnation, policy, and watcher-observed generations. A separate
  positive/negative domain canary uses two independent runtime-map handles over
  one cluster and rejects the foreign handle's otherwise usable admission
  before loading its bucket. Storage-level coverage pins the same gate identity
  independently of the coordinator workflow.
- retained stream-cleanup capability construction now validates that the
  admission belongs to the coordinator's exact publication gate before any
  mutation authority escapes. A same-cluster/different-runtime-handle
  regression creates a durable stream session, proves a foreign admission
  cannot mint cleanup authority or remove the session, then uses the local
  admission as a positive abort canary.
- admitted bucket-existence and CORS enrichment reads now perform the same
  coordinator publication-domain validation before deriving their active
  bucket routes. Two same-cluster/different-runtime-handle regressions retain
  an admission to the original store, publish a distinct replacement store
  through only the local handle, and prove the foreign coordinator can still
  observe the old bucket/CORS state while the local coordinator rejects that
  admission and its own admission observes the replacement state.
- the other buffered coordinator workflows, their production raw entry points,
  and capability requirements on the node-client traits remain open in Phase 3.
- focused HeadBucket policy/ABAC and captured-deadline regressions, installed
  Unix bucket-metadata coverage, formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full parallel workspace suite pass
  (7,534 tests).

Fiftieth Phase 3 slice:

- `GetBucketLocation` and `GetBucketVersioning` now consume the buffered
  request's existing `StorageClusterRouteAdmission` and load their complete
  policy/conditional-ABAC bucket view through its `ActiveBucketRoute`. Their
  production raw-route coordinator entry points are removed; crate-local test
  callers retain helpers which acquire fresh admission before entering the
  same production methods.
- both authorization paths now separate admitted bucket loading from policy
  evaluation on a `LoadedBucketHandle`. This keeps the existing AWS-compatible
  policy, ownership, expected-owner, and bucket-tag semantics while preventing
  either operation from resampling the renewable runtime-map handle after HTTP
  request admission.
- the shared bucket-metadata deadline regression now proves that HeadBucket,
  GetBucketLocation, and GetBucketVersioning all reject an expired captured
  admission after a same-generation renewal leaves the raw cluster usable. A
  same-cluster/different-runtime-handle regression supplies positive foreign
  canaries and proves all three operations reject that foreign publication
  domain before bucket access.
- the other buffered coordinator workflows, their production raw entry points,
  and capability requirements on the node-client traits remain open in Phase
  3.
- focused coordinator, HTTP-dispatch, bucket-policy, and bucket-tag ABAC tests,
  formatting, the storage boundary checker, and workspace-wide strict Clippy
  pass. The full parallel workspace suite passes (7,549 tests).

Fifty-first Phase 3 slice:

- `GetBucketObjectLockConfiguration`, `GetBucketEncryption`, and
  `GetBucketCors` now consume the buffered request's existing
  `StorageClusterRouteAdmission`. Their complete policy, conditional-ABAC, and
  requested subresource views load through the admitted `ActiveBucketRoute`;
  production coordinator entry points can no longer resample the renewable
  runtime-map handle. Crate-local tests retain convenience methods which admit
  a fresh request before entering the same production paths.
- the common bucket-action authorizer now separates route-bound loading from
  policy evaluation on a `LoadedBucketHandle`, and the CORS authorizer does the
  same for its larger handle request. This preserves the established
  expected-owner, bucket-policy, owner-account-admin, object-lock-not-found,
  encryption-default, and missing-CORS behavior while making the route
  capability boundary explicit.
- the shared bucket-metadata regressions now cover all six admitted operations:
  HeadBucket, location, versioning, object-lock configuration, encryption, and
  CORS. They prove an immutable captured deadline is not extended by a
  same-generation route renewal and that an admission from a distinct
  publication domain is rejected even when both runtime-map handles contain
  the same cluster. Positive foreign-domain canaries cover all six operations.
- the other buffered coordinator workflows, their production raw entry points,
  and capability requirements on the node-client traits remain open in Phase
  3.
- focused coordinator, HTTP object-lock, and endpoint-neutral S3 bucket-config
  tests pass, including bucket-policy and conditional-ABAC cases. Formatting,
  the storage boundary checker, and workspace-wide strict Clippy pass. The full
  parallel workspace suite passes (7,549 tests).

Fifty-second Phase 3 slice:

- `GetBucketTagging`, `GetBucketAbac`, and `GetBucketLifecycle` now consume the
  buffered request's existing `StorageClusterRouteAdmission`. Their tag,
  summary-only ABAC, and lifecycle views load through the admitted route, and
  their production coordinator entry points can no longer resample the
  renewable runtime-map handle. Crate-local coordinator tests retain helpers
  which acquire fresh admission before entering the production paths.
- tagging and lifecycle authorization now separate admitted handle loading
  from the unchanged policy evaluation. The ABAC path similarly evaluates its
  owner-account-admin rule against an admitted summary-only handle. Existing
  expected-owner, conditional bucket-tag ABAC, cross-account bucket-policy,
  missing-tag-set, disabled-ABAC, and missing-lifecycle behavior is preserved.
- the shared bucket-metadata regressions now cover nine operations and three
  additional handle shapes. Positive canaries and negative same-cluster/
  different-publication-domain cases prove the admitted tag, summary-only, and
  lifecycle paths all reject foreign authority; the captured-deadline matrix
  proves same-generation renewal cannot extend any of them.
- the other buffered coordinator workflows, their production raw entry points,
  and capability requirements on the node-client traits remain open in Phase
  3.
- focused coordinator, tagging, lifecycle, ABAC-admin, expected-owner,
  bucket-policy, and conditional-ABAC tests pass (126 tests). Formatting passes;
  the storage boundary checker and workspace-wide strict Clippy pass. The full
  parallel workspace suite passes (7,549 tests).

Fifty-third Phase 3 slice:

- `GetBucketPublicAccessBlock`, `GetBucketOwnershipControls`, and
  `GetBucketAcl` now consume the buffered request's existing
  `StorageClusterRouteAdmission`. Their policy and conditional-ABAC views load
  through the admitted route, and their production coordinator entry points no
  longer resample the renewable runtime-map handle. Crate-local tests retain
  helpers which acquire fresh admission before entering the production paths.
- public-access-block and ownership-control authorization now factor the
  unchanged policy result from the admitted `LoadedBucketHandle`. Bucket ACL
  authorization does the same while preserving its separate ACL-read fallback
  and BOE synthetic owner-only ACL rendering. Existing expected-owner,
  cross-account policy, conditional bucket-tag ABAC, missing-configuration,
  and ACL grant semantics are unchanged.
- the shared bucket-metadata regressions now cover twelve operations. Positive
  canaries and negative same-cluster/different-publication-domain cases cover
  the two optional summary fields and ACL rendering, while the captured-
  deadline matrix proves same-generation renewal cannot extend any of these
  requests.
- bucket-policy document and policy-status reads remain a separate buffered
  slice because they have missing-policy and owner-root precedence rules. The
  other buffered coordinator workflows and capability requirements on the
  node-client traits also remain open in Phase 3.
- focused coordinator, bucket-ACL, ownership-controls, public-access-block,
  expected-owner, bucket-policy, and conditional-ABAC tests pass (102 tests).
  Formatting, the storage boundary checker, and workspace-wide strict Clippy
  pass. The full parallel workspace suite passes (7,549 tests).

Fifty-fourth Phase 3 slice:

- `GetBucketPolicy` and `GetBucketPolicyStatus` now consume the buffered
  request's existing `StorageClusterRouteAdmission`. Their policy and
  conditional-ABAC views load through the admitted route, and their production
  coordinator entry points can no longer resample the renewable runtime-map
  handle. Crate-local tests retain helpers which acquire fresh admission before
  entering the same production paths.
- policy-document authorization now separates admitted handle loading from the
  unchanged policy evaluation and root-principal bypass. Policy-status
  authorization similarly evaluates the admitted handle while preserving its
  distinct lack of a root carve-out and its owner-versus-non-owner precedence
  when no policy is present.
- the shared bucket-metadata regressions now cover fourteen operations. A
  policy-present positive canary and negative same-cluster/different-
  publication-domain cases cover both endpoints, while the captured-deadline
  matrix proves same-generation renewal cannot extend either request.
- the other buffered coordinator workflows and capability requirements on the
  node-client traits remain open in Phase 3.
- focused coordinator, endpoint-neutral policy/status, expected-owner, root-
  carve-out, missing-policy, explicit-deny, and conditional-ABAC tests pass (35
  tests). Formatting, the storage boundary checker, and workspace-wide strict
  Clippy pass. The full parallel workspace suite passes (7,549 tests).

Fifty-fifth Phase 3 slice:

- `ListBuckets` now consumes the buffered request's existing
  `StorageClusterRouteAdmission`; its production coordinator entry point can no
  longer resample the renewable runtime-map handle. The crate-local coordinator
  tests retain a helper which acquires fresh admission before entering the same
  production path.
- `ActiveBucketMetadataScan` is a non-cloneable, account-scoped capability over
  the admitted runtime-map generation. It binds the authorized canonical owner
  identity at construction, owns no independently reusable cluster handle, and
  revalidates the admission's captured deadline immediately before every
  bucket-PG node access. The old raw cluster scan is now available only to tests
  and explicit test-hook builds.
- the metadata-listing completion hook now covers bucket as well as object and
  multipart scans. A deterministic three-PG regression expires the route after
  the first bucket-PG response and requires the capability to fail before the
  next access. The shared coordinator regressions add positive, expired-
  deadline, and same-cluster/different-publication-domain `ListBuckets`
  coverage alongside the fourteen subject-bound bucket reads.
- object and multipart listings, object reads and mutations, the remaining
  buffered coordinator workflows, and capability requirements on the
  node-client traits remain open in Phase 3.
- focused storage, coordinator, HTTP, endpoint-neutral response/root, and
  local-only anonymous-listing tests pass (18 tests). Formatting, the storage
  boundary checker, and workspace-wide strict Clippy pass. The full parallel
  workspace suite passes (7,557 tests).

Fifty-sixth Phase 3 slice:

- `ListObjects` V1/V2, `ListObjectVersions`, and `ListMultipartUploads` now
  consume the buffered request's existing `StorageClusterRouteAdmission` for
  both bucket-policy authorization and object-metadata fan-out. Their
  production coordinator entry points can no longer resample the renewable
  runtime-map handle; crate tests and the `test-utils` feature retain wrappers
  which acquire admission before entering the same production paths.
- `ActiveObjectMetadataScan` is a non-cloneable capability fixed to one bucket
  and the admitted runtime-map generation. Its API accepts only listing
  parameters, not another bucket or PG, and revalidates the admission's
  immutable captured deadline immediately before every object-metadata page
  read, including delimiter-driven cursor refills. The former raw cluster
  listing methods are now available only to tests and explicit test-hook
  builds.
- a deterministic three-PG regression captures a short request deadline,
  renews the same underlying route generation to a later deadline, advances
  time after the first PG response, and requires bucket, object, version, and
  multipart scans all to stop before the second PG access. The coordinator
  deadline and same-cluster/different-publication-domain matrices cover all
  three object listing families. The existing epoch-transition regression now
  starts publication asynchronously, proves it enters draining behind the
  admitted listing, and joins it after the request releases admission.
- object reads and mutations, the remaining buffered coordinator workflows,
  and capability requirements on the node-client traits remain open in Phase
  3.
- the focused boundary matrix passes (4 tests), as does the 151-test
  coordinator/HTTP/endpoint-neutral listing suite. Formatting, the storage
  boundary checker, and workspace-wide strict Clippy pass. The full parallel
  workspace suite passes (7,562 tests).

Fifty-seventh Phase 3 slice:

- `HeadObject`, part-level `HeadObject`, and `GetObjectAttributes` now consume
  the buffered request's existing `StorageClusterRouteAdmission`. Their
  production coordinator entry points can no longer resample the renewable
  runtime-map handle; crate tests and the `test-utils` feature retain wrappers
  which acquire admission before entering the same production paths.
- `ActiveObjectReadRoute` is a non-cloneable capability fixed to one bucket,
  key, requested version, snapshot mode, and object-metadata PG in the
  admitted runtime-map generation. Snapshot authorization rechecks the
  request's immutable deadline immediately before both the subject load and
  the subject-bound snapshot load, including every stale-subject retry.
  Lifecycle-expiration header evaluation now loads any required lifecycle
  configuration through the admitted bucket route as well.
- adversarial regressions reject same-cluster admissions from a different
  frontend publication domain and expire an admission between the object
  subject and snapshot reads after renewing the underlying raw route. A warm
  BOE fast-path regression separately expires authority after bucket
  authorization but before the object read. The existing HEAD epoch-change
  test now starts publication asynchronously, proves it waits behind the
  admitted read, and joins it after the request releases admission.
- response-body `GetObject`, ranged GET, and part-level GET paths, object
  mutations, the remaining buffered coordinator workflows, and capability
  requirements on the node-client traits remain open in Phase 3.
- the focused deadline/domain matrix passes (3 tests), as do the 112 focused
  coordinator/HTTP object-read tests, all 21 endpoint-neutral object-attribute
  tests, and all 7 focused lifecycle HEAD tests. Formatting, the storage
  boundary checker, and workspace-wide strict Clippy pass. The full parallel
  workspace suite passes (7,568 tests).

Fifty-eighth Phase 3 slice:

- response-body `GetObject`, ranged GET, and part-level GET now consume the
  buffered request's existing `StorageClusterRouteAdmission` for bucket
  authorization, object authorization/snapshot loading, lifecycle-header
  evaluation, and the payload-authority handoff. Their production coordinator
  entry points can no longer resample the renewable runtime-map handle;
  crate-local tests and the `test-utils` feature retain wrappers which acquire
  admission before entering the same production paths.
- full-payload snapshot loading now acquires the broad object-generation lease
  before loading the exact subject-bound snapshot and revalidates the
  admission's immutable deadline at every load, retry, lease acquisition, and
  handoff boundary. The result is an opaque non-cloneable
  `LeasedObjectReadSnapshot` which binds the exact snapshot, route provenance,
  originating cluster, and broad lease. Only consuming that token can derive
  `RetainedObjectPayloadRead`; it acquires every narrow lease before releasing
  the broad lease, making the metadata-to-payload reclaim handoff a type-owned
  transition rather than a caller convention. The authorization result and
  token share one immutable snapshot allocation rather than cloning multipart
  part and segment vectors. CopyObject and UploadPartCopy reuse the buffered
  request's admission for source-bucket authorization and leased source
  snapshot loading, then use the same token and retained reader. Their repair
  fence therefore carries the request's publication generation and immutable
  deadline instead of being absent on the former raw snapshot path.
- the retained response-body capability deliberately owns no long-lived
  route-publication admission and cannot perform another object metadata
  read. It binds one
  bucket/key/generation plus a private allowlist of complete segment
  descriptors; ordinary, range, and part readers reject any crossed segment.
  Segment locations are validated against retained PG routes while lease RPC
  acquisition remains authorized by the current request epoch. Payload bytes
  always use exact retained-route inspection, even when a segment's placement
  epoch equals the originating frontend epoch, so a storage-node map advance
  before the first body read neither rejects a valid old object nor contacts
  the wrong current nodes.
- read recovery preserves durable repair reporting while the originating
  frontend generation and the admission's immutable deadline remain current.
  The leased token captures both; the retained reader can acquire a short
  permit and record repair work only if both still validate. Once publication
  begins or completes, or the admitted deadline expires without publication,
  repair reporting is skipped instead of sending a stale active RPC, while
  reconstruction continues over the exact retained route.
- deterministic regressions expire the captured admission after exact snapshot
  load for all three GET variants, reject a crossed segment descriptor and a
  handoff token presented through another version's route, prove historical
  leases bind only the recorded old shard owner, and exercise an installed
  Unix body created at epoch N whose first read reconstructs a corrupt shard
  only after the same storage nodes and frontend install epoch N+1. A separate
  expiry-without-publication regression renews only the raw route, then proves
  retained reconstruction does not record repair after the captured deadline.
  CopyObject and UploadPartCopy regressions corrupt a source shard, prove the
  copy reconstructs coherent bytes, and require the source segment's durable
  background repair record. Their existing publication interleavings now start
  map publication asynchronously and prove it waits behind the request
  admission rather than deadlocking inside a synchronous hook.
  Existing runtime-map publication regressions now prove publication reaches
  draining behind snapshot/handoff,
  completes after admission is released, and the retained body remains
  readable afterward.
- object mutations, the remaining buffered coordinator workflows, and
  capability requirements on the node-client traits remain open in Phase 3.
- focused handoff/deadline/publication regressions and the broader 223-test
  storage/coordinator/HTTP object-read matrix pass. Formatting, diff validation,
  the storage boundary checker, and workspace-wide strict Clippy pass. The full
  parallel workspace suite passes (7,575 tests).

Fifty-ninth Phase 3 slice:

- GetObjectTagging, GetObjectAcl, GetObjectRetention, and GetObjectLegalHold
  now consume the buffered request's existing `StorageClusterRouteAdmission`
  for bucket policy context and exact object-subject loading. Their production
  HTTP paths cannot resample the renewable runtime-map handle; direct test and
  test-support wrappers acquire admission before entering the same production
  methods.
- `ActiveObjectReadRoute` now exposes a metadata-only subject load fixed to
  its bucket, key, requested version, object-metadata PG, originating cluster,
  and frontend publication domain. It rechecks the request's immutable
  deadline immediately before node access and confers no mutation authority.
  GetObjectTagging returns the tags from that same authorized subject, avoiding
  a separate unadmitted tag read while preserving one coherent linearization.
- the shared captured-deadline matrix now covers all four subresources, and
  their same-cluster/different-publication-domain matrix includes positive
  local canaries before rejecting foreign admission. A direct capability
  regression renews only the raw route and proves metadata subject loading
  still rejects the expired captured deadline. The existing object-metadata
  publication test now starts its read-side publication asynchronously and
  proves it waits behind admission. The existing same-PG tagging and retention
  regressions now probe availability through the admitted object-read route
  immediately before subject loading, proving bucket policy-context loading
  has released the object-PG lock.
- object mutations, the remaining buffered coordinator workflows, and
  capability requirements on the node-client traits remain open in Phase 3.
- the focused deadline/domain/publication regressions and 28 focused
  coordinator/HTTP object-subresource tests pass. Formatting, diff validation,
  the storage boundary checker, and workspace-wide strict Clippy pass. The full
  parallel workspace suite passes (7,577 tests).

Sixtieth Phase 3 slice:

- PutObjectTagging, DeleteObjectTagging, PutObjectAcl, PutObjectRetention, and
  PutObjectLegalHold now consume the buffered request's existing
  `StorageClusterRouteAdmission` for bucket policy context and mutation. Direct
  coordinator/test-support wrappers acquire admission before entering the same
  production methods.
- `ActiveObjectMetadataMutationRoute` is a non-cloneable authority bound to one
  bucket, key, requested version, object-metadata PG, originating cluster, and
  frontend publication domain. It rechecks the immutable request deadline
  before reservation acquisition, snapshot loading, command construction, and
  command installation. Application of the exact durably installed command is
  convergence after the authorized effect boundary. The former raw cluster
  entry points remain only under `cfg(test)`.
- the shared captured-deadline and same-cluster/different-publication-domain
  matrices now cover all five mutations plus DeleteObjectTagging, include
  positive canaries, and prove rejected calls preserve tags, retention, legal
  hold, and ACL state exactly. The object-metadata publication regression now
  proves both mutation and read admissions block publication. Existing
  post-install epoch-transition tests publish asynchronously, require the map
  to reach draining behind the admitted request, and prove the installed
  command converges exactly once before publication completes.
- DeleteObject/DeleteObjects, the remaining buffered coordinator workflows,
  and capability requirements on the node-client traits remain open in Phase
  3.
- the nine focused deadline/domain/publication/epoch-transition regressions
  pass. Formatting, diff validation, the storage boundary checker, and
  workspace-wide strict Clippy pass. The full parallel workspace suite passes
  (7,577 tests).

Sixtieth Phase 3 review correction:

- the earlier slice's checks immediately before reservation acquisition and
  pending-command installation did not make the deadline check atomic with the
  durable effect. A lock wait, test hook, or storage RPC round trip could cross the
  request's captured deadline after that check while a same-epoch renewal kept
  the raw route live.
- `AdmittedRouteEffectFence` now binds the frontend admission's cluster epoch,
  authority timestamp, and immutable, conservatively bound monotonic deadline.
  Object-metadata mutation must supply this fence; the local and storage RPC
  bucket-reservation and pending-slot clients carry it to the storage node,
  which revalidates clock health and the effective deadline after
  routing/locking and immediately before inserting the durable reservation or
  pending slot.
- monotonic timestamps are explicitly absent from the wire format. The two
  changed RPC requests carry the original authority timestamp and a delegated
  wall-clock upper bound. The receiving host subtracts the inter-host skew
  budget and binds that bound to its own monotonic clock. Storage RPC frame
  encoding advances to version 6. Ordinary recovery/maintenance calls
  retain their existing non-frontend path, while admitted mutation cannot omit
  the fence.
- a deterministic same-epoch-renewal regression advances time inside the
  pre-install hook to after the effective monotonic deadline but before the raw
  authority timestamp, and proves the object mutation is rejected, publishes
  no tags, releases its reservation, and can subsequently succeed. A raw Unix
  RPC regression pins the same interval for both durable effects while the
  storage node's renewed route remains live, with neither row inserted. A
  production TLS/TCP regression enters through the fenced reservation and
  pending-slot client APIs, gives frontend and storage threads deliberately
  different monotonic origins, proves a pre-deadline reservation succeeds, and
  proves a post-deadline pending insert is rejected without mutation.
- focused regressions, formatting, diff validation, the storage boundary
  checker, and workspace-wide strict Clippy pass. The full parallel workspace
  suite passes (7,580 tests).

Sixty-first Phase 3 slice:

- DeleteObject and DeleteObjects now consume the buffered request's existing
  `StorageClusterRouteAdmission` for bucket policy context, object-subject
  authorization, and mutation. DeleteObjects retains that one admission over
  its complete entry sequence. Direct coordinator/test wrappers acquire an
  admission before entering the same production paths.
- `ActiveObjectMetadataMutationRoute` now owns the current-version delete,
  specific-version delete, and delete-marker insertion boundaries. Each route
  is fixed to one bucket, key, optional version, object-metadata PG,
  originating cluster, and frontend publication domain. The raw storage
  publishers revalidate that route before reservation, snapshot, command
  construction, and pending-slot installation; their former unfenced entry
  points remain test-only.
- the DeleteObject bucket-write reservation, delete command, delete-marker
  command, and enabled-versioning marker-version reservation all carry the
  admission's immutable `AdmittedRouteEffectFence` to their deepest durable
  insertion boundary. Exact installed-command application and payload-reclaim
  enqueue remain convergence after that boundary.
- deterministic regressions renew only the raw same-epoch route, expire the
  captured admission in the pending-install hook, and prove unversioned
  deletion, enabled-versioning marker insertion, and every DeleteObjects entry
  fail with `OperationAborted` without changing object state. The shared
  same-cluster/different-publication-domain matrix now rejects both delete
  operations while preserving its object canary.
- the existing post-install epoch-transition delete regression now publishes
  asynchronously: publication reaches draining behind the admitted request,
  the already-installed delete converges exactly once, and publication
  completes only after request admission is released.
- remaining buffered coordinator workflows and capability requirements on the
  node-client traits remain open in Phase 3.
- 36 focused coordinator delete/auth regressions, 30 storage object-command
  regressions, and 25 HTTP delete regressions pass. Formatting, diff
  validation, the storage boundary checker, and workspace-wide strict Clippy
  pass. The full parallel workspace suite passes (7,592 tests).

Sixty-second Phase 3 slice:

- Put/Delete Bucket CORS, Tagging, Policy, and Lifecycle now consume the
  buffered request's existing `StorageClusterRouteAdmission` for both bucket
  authorization and mutation. The S3 Control ListTagsForResource,
  TagResource, and both UntagResource publication forms use the same admitted
  path rather than resampling the renewable runtime-map handle. Direct test
  and test-support wrappers acquire admission before entering those production
  methods.
- `ActiveBucketRoute` now owns the bucket-write authorization snapshot and
  bucket-subresource publisher for its fixed bucket and bucket-metadata PG.
  The former raw subresource publisher is test/test-hook only. Authorization
  reservation acquisition, snapshot loading, command construction, and the
  pending-slot insertion all recheck the request authority; exact installed
  command application remains convergence.
- bucket-control pending-slot insertion now accepts the immutable
  `AdmittedRouteEffectFence`. Local, Unix-session, and ordinary Unix clients
  validate it, and the storage RPC handler rebinds the portable deadline and
  validates it after the PG lock and immediately before the atomic
  drain-aware insertion. The existing wire request already carried the
  optional portable effect deadline, so no encoding change was required.
- the shared captured-deadline and same-cluster/different-publication-domain
  matrices cover the eight S3 bucket-subresource mutations and all S3 Control
  tagging paths, with exact post-rejection CORS, tag, lifecycle, and policy
  canaries. A deterministic pending-install regression covers both put and
  delete mutation shapes, and the raw Unix deadline regression now covers the
  bucket-control insertion variant as well.
- the existing before/after-authorization runtime-map pinning regressions now
  publish the replacement map asynchronously. They prove publication reaches
  draining behind the admitted mutation, the mutation completes against its
  captured route, and publication proceeds only after request admission is
  released, without deadlocking the hook inside the admitted request.
- bucket property/versioning/ACL mutations, CreateBucket/DeleteBucket,
  multipart control operations, other remaining buffered coordinator
  workflows, and capability requirements on the node-client traits remain
  open in Phase 3.
- four focused admission/effect-boundary regressions and a 196-test bucket
  configuration/control matrix pass. Formatting, diff validation, the storage
  boundary checker, workspace-wide strict Clippy, and the full parallel
  workspace suite pass (7,593 tests).

Sixty-third Phase 3 slice:

- PutBucketVersioning, PutBucketObjectLockConfiguration, Put/Delete
  BucketEncryption, PutBucketAbac, Put/Delete BucketPublicAccessBlock,
  Put/Delete BucketOwnershipControls, and PutBucketAcl now consume the
  buffered request's existing `StorageClusterRouteAdmission` for both bucket
  authorization and mutation. Their production HTTP paths cannot resample the
  renewable runtime-map handle; direct test/test-support wrappers acquire an
  admission before entering the same production methods.
- `ActiveBucketRoute` now owns the versioning, bucket-property, and bucket-ACL
  publishers for its fixed bucket and bucket-metadata PG. Each publisher
  revalidates request authority during snapshot/command construction and
  carries the admission's immutable `AdmittedRouteEffectFence` to the deepest
  bucket-control pending-slot insertion. Exact installed-command application
  remains convergence after that authorized effect boundary.
- the generic raw bucket-property/versioning/ACL storage publishers are now
  test/test-hook only. CreateBucket's legacy-region `AlreadyOwned` ACL rewrite
  retains one explicitly named transitional storage entry point; it remains
  coupled to CreateBucket and will migrate with that operation rather than
  leaving a general production PutBucketAcl bypass.
- the captured-deadline and same-cluster/different-publication-domain matrices
  cover all ten operations, with positive local canaries and exact versioning,
  object-lock, encryption, ABAC, public-access-block, ownership-control, and
  ACL state checks after rejection. A deterministic pending-install regression
  independently expires each of the three command families—property,
  versioning, and ACL—after same-epoch raw-route renewal and proves no durable
  mutation before a fresh positive retry.
- the publisher registry, contention/convergence guide, and temporary boundary
  inventory now name the route-validating canonical entry points.
- CreateBucket/DeleteBucket, multipart control operations, other remaining
  buffered coordinator workflows, and capability requirements on the
  node-client traits remain open in Phase 3.
- the three focused deadline/domain/effect-boundary regressions, 23 focused
  HTTP bucket-mutation tests, all 1,248 server-core tests, formatting, diff
  validation, the storage boundary checker, and workspace-wide strict Clippy
  pass. The full parallel workspace suite passes (7,592 tests).

Sixty-fourth Phase 3 slice:

- CreateBucket and DeleteBucket now consume the buffered request's existing
  `StorageClusterRouteAdmission`. Their production HTTP and coordinator paths
  cannot resample the renewable runtime-map handle after authentication;
  direct test/test-support wrappers acquire an admission before entering the
  same production methods.
- `ActiveBucketRoute` now owns the lifecycle authority for its fixed bucket and
  bucket-metadata PG. CreateBucket command construction and publication,
  DeleteBucket's ordinary and preserved-attempt authorization snapshots, its
  durable bucket-write drain, and its `MarkBucketDeleting` command all remain
  on that admitted route. The post-mark finalizer enqueue is subject-bound
  convergence and does not resample a storage route.
- CreateBucket's legacy-region `AlreadyOwned` ACL rewrite now uses the same
  admitted route and ordinary fenced ACL publisher. The transitional raw ACL
  entry point has been removed. Create retries against a deleting bucket use a
  subject-bound finalization operation before retrying publication.
- both lifecycle publishers revalidate the admitted route during snapshot and
  command construction and carry its immutable `AdmittedRouteEffectFence` to
  pending-slot insertion. DeleteBucket also carries that fence to its earlier
  durable write-drain acquisition. Expiry before drain insertion leaves no
  drain or pending command; expiry after a valid drain but before the marker
  retains the already-authorized drain for the established retry/convergence
  path while publishing no delete marker.
- bucket-write-drain begin RPCs now carry the same portable wall-clock effect
  deadline as reservation and pending-slot insertion RPCs. Storage RPC frame
  encoding advances to version 8 and rejects version 7. Local, Unix, and
  TLS/TCP paths validate the fence at the storage effect boundary; the TLS
  regression uses distinct frontend/storage monotonic origins, proves a valid
  drain is admitted, and proves an expired request creates no drain row. The
  per-kind request cap includes the complete optional deadline encoding and an
  exact maximum-length framed request proves admission at that bound. The
  lifecycle-claim cap no longer derives from the now-different drain layout;
  its independent maximum-length framed request is pinned too.
- the captured-deadline and same-cluster/different-publication-domain matrices
  now cover both lifecycle operations. Deterministic tests independently expire
  CreateBucket at pending insertion and DeleteBucket at its drain and marker
  boundaries, with exact absent-bucket, active-bucket, drain, and pending-slot
  canaries. A crossed-subject CreateBucket regression also proves that a route
  for one bucket cannot publish a configuration naming another bucket. The
  existing DeleteBucket publication race now publishes on a separate thread
  and proves replacement remains pending until the admitted lifecycle request
  finishes, avoiding a synchronous test-hook self-deadlock.
- the new bucket-PG pending-install hook point is limited to callers carrying an
  admitted effect fence. This preserves the earlier object-PG race-hook timing:
  an unfenced multipart completion cannot consume its hook during the preceding
  bucket completion barrier. The publisher registry, contention guide, scanner,
  and temporary semantic inventories use the route-validating canonical entry
  points.
- background recovery continues previously authorized durable DeleteBucket
  attempts through an explicitly named production convergence entry point.
  Its adopted-attempt root is opaque outside `storage`: only durable recovery
  decoding and storage-owned queueing can construct one, while server-core has
  read-only subject/generation accessors. The raw begin-root enqueue operation
  is crate-private, with a separately named `test-hooks` wrapper for recovery
  fixtures, so ordinary callers cannot turn known bucket generations into
  convergence authority. The unadmitted generic frontend-shaped wrapper
  remains test-only.
- focused lifecycle deadline, crossed-subject, portable TLS deadline, and
  multipart hook-isolation regressions pass. The storage boundary checker,
  all-target/all-feature build, workspace-wide strict Clippy, formatting, and
  diff checks pass. The full parallel workspace suite passes (7,603 tests).
- multipart control operations, other remaining buffered coordinator
  workflows, and capability requirements on the node-client traits remain
  open in Phase 3.

Sixty-fifth Phase 3 slice:

- CreateMultipartUpload now consumes the buffered request's existing
  `StorageClusterRouteAdmission`. Production HTTP and coordinator code cannot
  resample the renewable runtime-map handle after authentication; the direct
  test/test-support wrapper acquires an admission before entering the same
  production method.
- the non-cloneable `ActiveMultipartObjectRoute` fixes the bucket, object key,
  object-metadata PG, publication domain, runtime-map generation, and immutable
  admitted deadline for multipart control work. CreateMultipartUpload no
  longer accepts its routed subject separately after capability construction,
  and the storage boundary rejects a prepared create command naming another
  bucket or key before publication.
- multipart initiation revalidates the admitted route around bucket and object
  snapshot loading and after the authorization/action closure. Its durable
  bucket-write reservation acquisition and snapshot-sensitive pending-command
  insertion both enforce the admission's portable effect fence at the storage
  effect boundary. Once a command is durably installed, subsequent apply is
  convergence and retains the existing reservation-release semantics.
- lifecycle configuration needed for CreateMultipartUpload response headers is
  loaded in that same admitted bucket snapshot and parsed only after write
  authorization succeeds. The parsed configuration travels in the authorized
  result through durable creation; post-commit response construction now only
  evaluates that snapshot and cannot resample storage or turn a committed
  upload into a late route-expiry error.
- raw CreateMultipartUpload storage entry points are limited to tests and the
  `test-hooks` feature. The publisher registry, contention guide, bucket-write
  drain guide, scanner, and temporary semantic inventories identify the
  route-validating inner publisher as the canonical production entry point.
- the captured-deadline matrix now covers multipart initiation. A deterministic
  same-epoch-renewal regression expires the original admission between
  authorization and pending-slot insertion, proves no upload is published,
  and then proves a fresh admission succeeds. Same-cluster/different-
  publication-domain and crossed-object-subject regressions prove rejection
  before mutation, with exact empty-upload canaries.
- a separate deterministic regression advances the clock beyond the admitted
  deadline immediately after durable multipart publication. It requires the
  response to succeed from the captured lifecycle configuration, including the
  matching abort-rule header, and verifies exactly one durable upload.
- focused deadline, publication-domain, and crossed-subject regressions pass.
  The storage boundary checker, all-target/all-feature build, workspace-wide
  strict Clippy, formatting, and diff checks pass. The full parallel workspace
  suite passes (7,609 tests).
- CompleteMultipartUpload, AbortMultipartUpload, ListParts, and the remaining
  multipart control/read operations remain open for the same capability
  family. Other buffered coordinator workflows and capability requirements on
  node-client traits also remain open in Phase 3.

Sixty-sixth Phase 3 slice:

- AbortMultipartUpload now consumes the buffered request's existing
  `StorageClusterRouteAdmission` from HTTP dispatch through bucket/upload
  authorization and terminal mutation. Production coordinator code cannot
  resample the renewable runtime-map handle; direct test/test-support wrappers
  acquire an admission before entering the same path.
- `ActiveMultipartObjectRoute` now owns multipart-management lookup and the
  authorized abort publisher for its fixed bucket, key, object-metadata PG,
  publication domain, runtime-map generation, and immutable admitted deadline.
  A crossed-object authorized upload is rejected before mutation.
- foreground authorized abort acquires its durable bucket-write reservation
  through the admitted effect fence and carries that same fence to the deepest
  pending-slot insertion. The current upload row is still compared with the
  authorized row before command construction; exact installed abort
  application remains convergence. The unbounded raw authorized-abort wrapper
  is test/test-hook only, while lifecycle abort retains its distinct recovery
  authority.
- the captured-deadline regression renews the same raw route, expires the
  original admission at pending installation, proves the upload and
  reservation ownership remain intact, and then proves a fresh admission can
  abort it. Same-cluster/different-publication-domain and crossed-object
  regressions prove rejection without mutation.
- the two existing runtime-map publication races now publish asynchronously,
  prove publication reaches draining behind the admitted abort at both the
  bucket-summary and upload-lookup boundaries, and join after the request
  releases admission. This preserves the original captured-route assertions
  without a synchronous hook deadlock.
- the publisher scanner and semantic inventory distinguish effect-fenced
  pending-slot insertion from the unbounded helper, and the metadata-command
  and bucket-write-drain guides record the foreground abort boundary.
- all 1,256 server-core tests, the 58-test storage multipart matrix, focused
  deadline/domain/crossed-subject tests, formatting, diff validation, the
  storage boundary checker, and workspace-wide strict Clippy pass. The full
  parallel workspace suite passes (7,614 tests).
- CompleteMultipartUpload, ListParts, and the remaining multipart control/read
  operations remain open for the same capability family. Other buffered
  coordinator workflows and capability requirements on node-client traits
  also remain open in Phase 3.

Sixty-seventh Phase 3 slice:

- ListParts now consumes the buffered request's existing
  `StorageClusterRouteAdmission` from HTTP dispatch through bucket/upload
  authorization, object-PG part listing, and lifecycle response-header lookup.
  Production coordinator code cannot resample the renewable runtime-map
  handle; direct test/test-support wrappers acquire an admission before
  entering the same path.
- `ActiveMultipartObjectRoute` now owns the authorized ListParts read for its
  fixed bucket, key, object-metadata PG, publication domain, runtime-map
  generation, and immutable admitted deadline. The raw cluster wrapper is
  test/test-hook only, and a crossed same-PG authorized upload is rejected
  before node access.
- ListParts authorization resolves the bucket summary and upload-management
  record through admitted capabilities. Lifecycle response headers are loaded
  through the same admission after the part listing, so neither the primary
  read nor this later bucket-subresource read can silently adopt a same-epoch
  renewal or a newly published runtime map.
- deterministic same-epoch-renewal regressions expire the original admission
  once after authorization and once after the object-PG list. They require
  `OperationAborted` at the respective storage boundary, preserve the upload,
  and then prove a fresh admission returns the empty listing and captured
  lifecycle abort rule. A runtime-map publication race proves the admitted
  request completes against its pinned map before publication proceeds.
- same-cluster/different-publication-domain and crossed-object-subject
  regressions prove rejection before node access while positive canaries use
  the owning coordinator and correctly routed subject.
- all 27 focused ListParts/storage/HTTP tests pass. Formatting, diff
  validation, the storage boundary checker, workspace-wide strict Clippy, and
  the full parallel workspace suite pass (7,617 tests).
- CompleteMultipartUpload and the remaining multipart control/read operations
  remain open for the same capability family. Other buffered coordinator
  workflows and capability requirements on node-client traits also remain
  open in Phase 3.

Sixty-eighth Phase 3 slice:

- CompleteMultipartUpload's request-layer target preflight now consumes the
  buffered request's existing `StorageClusterRouteAdmission`. This is the
  AWS-ordering lookup performed after routing/authentication and upload-ID
  parsing but before checksum-header and completion-XML validation.
- `ActiveMultipartObjectRoute` now exposes an admitted in-progress-upload
  lookup for its fixed bucket, key, object-metadata PG, publication domain,
  runtime-map generation, and immutable deadline. The raw cluster lookup
  delegates to the same route-validating implementation with test-independent
  unbounded authority for callers not yet migrated.
- the preflight deliberately retains its established semantics: an active row
  succeeds directly; otherwise the issued upload ID is authenticated against
  the admitted bucket incarnation. It does not reuse the multipart-management
  lookup or drain pending commands, so terminal replay convergence cannot
  change malformed-header/XML precedence.
- a deterministic same-epoch-renewal regression expires the original
  admission before the object-PG lookup, requires `OperationAborted`, preserves
  the active upload, and proves a fresh admission succeeds. A same-cluster/
  different-publication-domain regression rejects foreign admission, while a
  same-PG crossed-key storage regression proves the admitted route cannot find
  another key's upload and retains a correct-route positive canary.
- obsolete raw checked-bucket-summary helpers were removed after the preflight
  moved its fallback bucket read to the admitted form.
- all 88 focused CompleteMultipartUpload/storage/HTTP tests pass. Formatting,
  diff validation, the storage boundary checker, workspace-wide strict Clippy,
  and the full parallel workspace suite pass (7,618 tests).
- the full CompleteMultipartUpload authorization, snapshot, commit, replay,
  lifecycle-response, and reclaim-enqueue workflow remains open for the next
  capability slice. Other buffered coordinator workflows and capability
  requirements on node-client traits also remain open in Phase 3.

Sixty-ninth Phase 3 slice:

- the full CompleteMultipartUpload workflow now consumes the buffered
  request's existing `StorageClusterRouteAdmission` from bucket/policy/upload
  authorization through completion snapshot loading, durable publication,
  replay response construction, lifecycle response evaluation, and displaced
  payload reclaim scheduling. Production coordinator code cannot resample the
  renewable runtime-map handle; direct test/test-support wrappers acquire an
  admission before entering the same production path.
- completion authorization loads policy, conditional ABAC bucket tags, and
  lifecycle configuration in one admitted bucket-write snapshot. BOE and ACL
  authorization resolve the upload-management state through the same admitted
  multipart object route. Both in-progress and replay results carry the parsed
  lifecycle configuration, so neither replay nor post-commit response-header
  construction performs a later storage read.
- `ActiveMultipartObjectRoute` now owns the authorized completion snapshot,
  serialized completion publisher, and displaced-generation reclaim enqueue
  for its fixed bucket, key, object-metadata PG, publication domain,
  runtime-map generation, and immutable deadline. Snapshot loading rejects a
  crossed same-PG authorized upload before node access; the final publisher
  independently rejects a request whose bucket/key does not match its route.
- the route-validating completion publisher carries the immutable admitted
  effect fence through completion-reservation acquisition, version
  reservation, the bucket-PG completion barrier, and final object-PG pending
  insertion. Same-epoch raw-route renewal cannot extend any fresh durable
  effect. Exact pending-command application remains convergence and preserves
  the existing proof-release and matching-outcome semantics.
- the completion-specific Unix reservation path retains its reserved
  completion admission class while transporting only the portable admitted
  deadline; the storage process conservatively binds that deadline to its own
  monotonic clock through the existing fenced reservation RPC boundary.
- deterministic coverage expires an admitted completion at its final pending
  insertion after the barrier is durable, requires `OperationAborted`, proves
  the upload remains active, and then completes with a fresh admission. A
  second regression expires the admission after durable object publication
  and proves the response still succeeds with the captured lifecycle rule.
  Same-cluster/different-publication-domain and crossed-object-subject tests
  prove rejection before mutation or node access while retaining owning-route
  positive canaries.
- the null-version stale-snapshot retry revalidates the immutable admission
  before reloading the current stale-payload source. Expiry at the end of
  command construction now releases the transient completion reservation and
  returns `OperationAborted` without performing that second storage read; a
  deterministic competing-completion regression pins the retry boundary and
  fresh-admission recovery.
- the publisher registry, contention guide, storage-cluster invariants,
  bucket-write drain guide, scanner, and temporary semantic inventories now
  identify the route-validating completion publisher as the canonical
  production entry point. The obsolete raw lifecycle-response loader and its
  internal authorization token were removed.
- focused completion deadline, lifecycle, publication-domain, and
  crossed-subject regressions pass, along with formatting, the storage
  boundary checker, workspace-wide strict Clippy, and the full parallel
  workspace suite (7,623 tests).
- remaining buffered coordinator workflows and capability requirements on
  node-client traits remain open in Phase 3.

Seventieth Phase 3 slice:

- the request-layer active-upload preflight shared by ordinary UploadPart and
  UploadPartCopy now consumes the request's existing
  `StorageClusterRouteAdmission`. Both the streaming and buffered HTTP paths
  use the same `ActiveMultipartObjectRoute` as their later authorization and
  mutation work instead of resampling the renewable runtime-map handle between
  authentication and operation-specific part/checksum/copy-range validation.
- the lookup retains its AWS-compatible ordering and error behavior: it
  requires a currently in-progress upload before parsing those
  operation-specific fields and maps a missing, terminal, wrong-key, or
  invalid upload ID to `NoSuchUpload`. Unlike completion preflight, it does not
  authenticate a terminal upload ID for possible replay.
- the shared multipart preflight deadline regression now proves a same-epoch
  raw-route renewal cannot extend either completion or active-upload lookup
  authority, preserves the active upload after rejection, and succeeds under
  a fresh admission. The multipart publication-domain matrix rejects the
  active-upload lookup through a same-cluster admission minted by another
  runtime-map handle while retaining an owning-coordinator positive canary.
  Existing storage coverage independently proves the admitted route cannot
  load a crossed same-PG key's upload.
- the focused coordinator preflight/domain tests and the 22-test server-http
  UploadPart matrix pass. Formatting, diff validation, the storage boundary
  checker, workspace-wide strict Clippy, and the full parallel workspace suite
  pass (7,634 tests).
- remaining buffered coordinator workflows and capability requirements on
  node-client traits remain open in Phase 3.

Seventy-first Phase 3 slice:

- buffered PutObject now consumes the request's existing
  `StorageClusterRouteAdmission` instead of discarding it after routing and
  authentication. Direct and promoted-stream paths share the same admitted
  entry point; direct test wrappers acquire an admission before entering the
  production workflow.
- the non-cloneable `ActivePutObjectRoute` fixes the bucket, key,
  bucket-metadata PG, object-metadata PG, publication domain, and immutable
  deadline for the whole direct workflow. Authorization subject loading,
  bucket-write snapshot/reservation, object-generation reservation, staged
  payload placement, final object publication, lifecycle response data, and
  displaced-generation reclaim no longer resample the renewable raw cluster.
- generation reservation and final command installation carry the admission's
  effect fence to the pending-slot durable boundary. Staged shard writes now
  carry the same fence through `PlacedShardNodeClient`: embedded nodes validate
  immediately before writing the shard file, while Unix/TLS requests serialize
  a portable wall-clock upper bound and bind it to the storage host's own
  monotonic clock. Process-local monotonic timestamps never enter the wire
  format. Storage RPC encoding advances to version 9 and explicitly rejects
  version 8; repair writes cannot accept frontend request authority.
- deterministic same-epoch-renewal regressions expire the original admission
  at generation reservation and immediately before the first shard-file
  effect. They prove no object or staged shard is published, while a fresh
  admission succeeds. The existing same-cluster/different-handle matrix now
  rejects PutObject through a foreign publication domain. The TLS/TCP test
  exercises the production client conversion with deliberately different
  frontend and storage-node monotonic origins, proving both an admitted shard
  write and an expired write that creates no shard file.
- the direct and promoted PUT runtime-map handoff regressions now begin
  publication on a separate thread, prove it enters draining while request
  admission is held, and require publication to finish after the pinned
  request completes. This preserves the intended handoff assertion without
  synchronously deadlocking publication against the request permit.
- focused direct-PUT, storage-RPC codec, TLS/TCP deadline, publication-domain,
  HTTP streaming, and runtime-map handoff regressions pass. Formatting, diff
  validation, the storage boundary checker, workspace-wide strict Clippy, and
  the full parallel workspace suite pass (7,636 tests).
- remaining buffered coordinator workflows and capability requirements on
  node-client traits remain open in Phase 3.

Seventy-second Phase 3 slice:

- promoted buffered PutObject now retains its non-cloneable
  `ActivePutObjectRoute` through stream-session creation, each encrypted shard
  write, segment-append command publication, bucket/lifecycle snapshot loading,
  and final object publication. The admitted create, append, and finalize APIs
  carry the immutable request effect fence to their durable reservation,
  shard-file, and pending-slot boundaries instead of performing a precheck and
  delegating to the renewable raw cluster.
- raw and admitted stream workflows share small route interfaces in
  server-core, preserving the test/copy helpers that intentionally use
  unbounded internal authority without allowing production admitted PUT or
  POST Object paths to fall back to them. Metadata-command publisher registry,
  guide, and checker entries now name the route-validating create/finalize
  publishers.
- partial direct-PUT shard placement now treats route expiry exactly like a
  shard-write failure: every shard already written by the caller is removed
  before the error escapes. A deterministic second-shard regression proves
  that the first successful write is removed when the captured admission
  expires immediately before the second write.
- deterministic same-epoch-renewal regressions expire admission immediately
  before stream-create pending installation, after promoted shard writes but
  before append command allocation, and during finalization command building.
  They require `OperationAborted`, no object publication, no leaked promoted
  shard/session state after caller cleanup, and a fresh stream-create canary.
- the focused four-test effect-boundary matrix, formatting, diff validation,
  the storage boundary checker, and workspace-wide strict Clippy pass. The
  full parallel workspace run reached 3,541 passing tests before the known,
  separately owned experimental Raft heartbeat write-amplification gate failed;
  the remaining tests were cancelled and that unrelated failure is explicitly
  excluded from this slice.
- remaining buffered coordinator workflows and capability requirements on
  node-client traits remain open in Phase 3.

Seventy-third Phase 3 slice:

- CopyObject now consumes the buffered request's existing admission for both
  sides of the operation. Destination PutObject authorization no longer loads
  the bucket/current-object state through a renewable raw cluster, and source
  payload retention must consume the leased snapshot through the exact
  `ActiveObjectReadRoute` that matches its bucket, key, requested version,
  object-metadata PG, publication domain, and immutable deadline.
- one `ActivePutObjectRoute` owns destination stream-session creation,
  encrypted shard placement, segment-append publication, lifecycle snapshot,
  final object publication, and displaced-generation reclaim. Stream cleanup
  authority is derived before the first durable destination mutation, so an
  expired or failed copy can abort staged state without retaining request
  admission or resampling the current runtime map. CopyObject's synchronous
  retained cleanup retries only `OperationAborted`/`SlowDown` for a bounded
  interval; the retry sleep is capped to the remaining budget and the deadline
  is rechecked immediately before every later RPC. Frontend PUT, POST Object,
  and UploadPart cleanup remains deliberately one-shot so it releases route
  publication immediately to the durable cleanup handoff. The session's
  durable cleanup deadline remains the recovery handoff after either
  capability is dropped. The production raw `Coordinator::copy_object` entry
  point is test-only.
- a deterministic same-epoch-renewal regression expires the original
  admission after destination shards are written but before append command
  allocation. It requires `OperationAborted`, no destination object, no staged
  shard files, and no stream session. The same-cluster/different-runtime-map
  matrix now rejects CopyObject through foreign admission while an owning
  coordinator canary copies and reads the expected bytes.
- all 50 focused server-core CopyObject/retained-cleanup tests, the 30-test
  endpoint-neutral CopyObject suite, HTTP response coverage, formatting, diff
  validation, the storage boundary checker, and workspace-wide strict Clippy
  pass. The final parallel workspace run passes all 7,641 selected tests; the
  known, separately owned experimental Raft heartbeat write-amplification gate
  is explicitly excluded from that run.
- UploadPartCopy, other remaining buffered coordinator workflows, and
  capability requirements on node-client traits remain open in Phase 3.

Seventy-third Phase 3 review correction:

- CopyObject's bounded retained-cleanup loop no longer starts one final RPC
  after its retry deadline. A short-budget exhaustion regression uses a retry
  delay longer than the remaining budget, requires exactly one attempted RPC,
  preserves the durable stream session for recovery, and then proves cleanup
  succeeds once the injected contention is removed. The pre-existing promoted
  POST Object and UploadPart handoff regressions continue to pin that frontend
  cleanup makes only one prompt attempt and cannot delay route publication.

Seventy-fourth Phase 3 slice:

- UploadPartCopy now consumes the buffered request's existing admission for
  both source and destination. Source authorization and payload retention use
  the exact `ActiveObjectReadRoute` matching the authorized snapshot. The
  destination bucket, in-progress upload, and all later part effects use one
  non-cloneable `ActiveMultipartObjectRoute`; the production raw coordinator
  entry point is test-only.
- `ActiveMultipartObjectRoute` now owns UploadPartCopy stream-session creation,
  session/encryption reload, encrypted shard placement, segment-append
  publication, and part finalization. Session creation revalidates the exact
  authorized upload row before publication, and the immutable admitted effect
  fence reaches each bucket-write reservation, shard-file write, and
  create/append/finalize pending-command insertion rather than stopping at a
  coordinator precheck.
- retained stream cleanup authority is derived before the first destination
  mutation and the stream session persists the admitted cleanup deadline.
  Failed or expired copies make a bounded synchronous cleanup attempt without
  relying on the expired active route, while durable recovery remains the
  handoff if prompt cleanup cannot finish.
- a deterministic same-epoch-renewal regression expires the captured
  admission after destination shards are staged but before append command-ID
  allocation. It requires `OperationAborted`, removes every staged shard and
  the stream session, and preserves the in-progress multipart upload. A
  same-cluster/different-runtime-map regression proves the owning coordinator
  can publish one copied part while a foreign-domain admission cannot publish
  another.
- the metadata-command publisher registry, command-stream guide, boundary
  checker allowlists, bucket-write-drain guide, and storage capability
  invariants now identify the route-validating UploadPart stream-create and
  stream-finalize publishers.
- all 16 focused storage UploadPart-stream tests, 22 focused server-core
  UploadPartCopy tests, three server-HTTP routing/response tests, and eight
  endpoint-neutral multipart UploadPartCopy tests pass. Formatting, diff
  validation, the storage boundary checker, and workspace-wide strict Clippy
  pass.
- ordinary streamed UploadPart, other remaining buffered coordinator
  workflows, and capability requirements on node-client traits remain open in
  Phase 3.

Seventy-fourth Phase 3 review correction:

- append preparation is now a fenced durable effect rather than a read-like
  preflight. The admitted fence crosses Unix/TLS RPC as a host-portable
  deadline, is rebound to the storage host's monotonic clock, and is validated
  immediately beside `next_segment_vid` allocation. The RPC frame encoding is
  advanced to version 10 and its maximum-size deadline-bearing request is
  admitted exactly at the per-kind cap.
- the raw unbounded UploadPart stream-create and stream-finalize
  `StorageCluster` wrappers are test/test-hook-only. Production UploadPartCopy
  can reach those effects only through `ActiveMultipartObjectRoute`.
- `server-core/test-utils` explicitly forwards `storage/test-hooks`, so its raw
  test adapter and the gated storage wrappers share one feature boundary even
  when server-core is compiled independently rather than through server-http's
  dev-dependency feature unification.
- `ActiveMultipartObjectRoute` no longer accepts a caller-supplied cleanup
  deadline. It derives and persists the captured admission authority deadline
  itself, preventing `None` from creating an indefinitely unswept session.
- a delayed-RPC regression renews the raw active route, delivers append
  preparation after the original admitted deadline, and proves the durable
  stream allocator does not consume a VID. The UploadPartCopy expiry
  regression also inspects the live session before rejection and pins its
  admission-derived cleanup deadline.
- the 20-test focused storage stream/RPC matrix, all 22 server-core
  UploadPartCopy tests, the three related PUT/CopyObject/UploadPartCopy effect
  boundary regressions, and 14 endpoint-neutral UploadPart tests pass. Both
  standalone `server-core --features test-utils` and `server-core
  --all-features` checks pass. Formatting, diff validation, the storage
  boundary checker, workspace-wide strict Clippy, and the full 7,671-test
  workspace suite pass.

Seventy-fifth Phase 3 slice:

- ordinary streamed UploadPart now uses the request's admitted
  `ActiveMultipartObjectRoute` for authorization-time upload loading, stream
  session creation, encryption/session reload, segment allocation and shard
  publication, and finalization. Its production frontend context no longer
  carries a raw renewable `StorageCluster` handle.
- session creation compares the current in-progress upload with the exact
  request-entry authorized upload record and derives the durable cleanup
  deadline from the admitted route. Append preparation, shard writes, pending
  installation, and finalization therefore share the same immutable effect
  fence already used by UploadPartCopy.
- the superseded raw coordinator and `StorageCluster` UploadPart session-create,
  encryption-load, and append adapters are test/test-hook-only. The duplicate
  production `BeginUploadPartStreamSession` publisher classification is
  removed; ordinary UploadPart and UploadPartCopy now share the single
  route-validating `CreateUploadPartStreamSession` publisher.
- a deterministic same-epoch-renewal regression expires admission after
  ordinary UploadPart shards are staged but before append command-ID
  allocation. It requires `OperationAborted`, proves the staged shards and
  segment row are absent, cleans the session through retained authority, and
  preserves the active MPU. The multipart publication-domain matrix now also
  rejects foreign-domain ordinary UploadPart create, append, and finalize
  while positive canaries publish through the owning coordinator.
- all 62 focused server-core stream-part/UploadPart tests, 24 server-HTTP
  UploadPart tests, and 44 endpoint-neutral S3 UploadPart tests pass, together
  with both independent `server-core` feature checks, the storage boundary
  checker, workspace-wide strict Clippy, and the full 7,667-test workspace
  suite.
- other remaining buffered coordinator workflows and capability requirements
  on node-client traits remain open in Phase 3.

Seventy-sixth Phase 3 slice:

- admitted PutObject and POST Object stream contexts no longer carry a raw,
  renewable `StorageCluster` alongside their request admission. Preparation,
  session creation, append, heartbeat, direct commit, and finalization derive
  the subject-bound `ActivePutObjectRoute` from the coordinator's own
  publication domain. Promoted buffered PUT also derives retained cleanup
  authority before its first durable stream mutation.
- stream heartbeat now carries the immutable admitted effect fence to both
  durable mutations: bucket-reservation lease renewal and the stream session's
  persisted reservation-proof update. Embedded nodes validate immediately at
  the metadata effect; Unix/TLS RPC sends a portable wall deadline which the
  storage process conservatively binds to its own monotonic clock. The active
  route epoch authorizes the new effect while an older durable reservation
  proof remains valid across a route-epoch transition.
- the production raw unbounded heartbeat adapters are test/test-hook-only.
  Maximum-sized deadline-bearing heartbeat and proof-update requests are
  admitted exactly at their per-kind caps, and storage RPC encoding advances
  to version 11 with explicit version-10 rejection.
- deterministic storage-node capability regressions expire the request fence
  while the renewed active route remains valid and prove neither durable lease
  nor stream proof changes. Unix tests cover the complete heartbeat/update
  codecs, current-route renewal of an older proof, and wrong-PG rejection.
- remaining buffered coordinator workflows and capability requirements on
  node-client traits remain open in Phase 3.

Seventy-sixth Phase 3 review correction:

- the object-PG stream-proof update no longer requires the durable proof's
  acquisition epoch to equal the separately authorized active route epoch. It
  validates the bucket/key/operation subject, requires current and renewed
  proofs to have the same complete stable identity, and relies on the PG-store
  comparison with the persisted session proof before changing only the lease
  deadline. The admitted effect fence still must match the current active
  route epoch at the storage-node boundary.
- an end-to-end Unix cluster regression seeds an epoch-N reservation and
  stream session on distinct bucket and object PGs, serves them through an
  epoch-N+1 route, runs the complete stream heartbeat, and proves both the
  bucket reservation and persisted session proof advance to the same deadline
  while retaining their epoch-N stable identity.

Seventy-seventh Phase 3 slice:

- the transitional `StorageNodeClient` aggregate no longer exposes object
  generation allocation, object version allocation, or direct-PUT snapshot
  and command construction. Those operations are available only through the
  role-specific `ObjectGenerationMetadataNodeClient`,
  `ObjectVersionMetadataNodeClient`, and `DirectPutMetadataNodeClient`
  interfaces. Their duplicate raw method set and duplicate local
  implementation are removed, so adding or changing one of these operations
  has one compiler-visible interface and implementation.
- the two admitted stream-heartbeat mutation boundaries no longer accept a
  separately caller-supplied route epoch alongside their
  `AdmittedRouteEffectFence`. Bucket-reservation renewal and persisted stream
  proof update derive the authorizing current route epoch from the fence;
  the durable reservation proof deliberately retains its independent stable
  acquisition epoch. This removes a contradictory state without conflating
  route authority with proof identity.
- the remaining bucket metadata/write-reservation, object mutation/read, and
  payload-reclaim duplicates in the transitional aggregate, together with
  capability requirements on the role-specific node-client traits, remain
  open in Phase 3.
- the seven focused embedded/Unix generation, version, direct-PUT, and
  heartbeat regressions pass, as do all 2,359 storage tests, formatting, the
  storage boundary checker, and workspace-wide strict Clippy.

Seventy-eighth Phase 3 slice:

- the transitional `StorageNodeClient` aggregate no longer exposes object-read
  authorization subjects, subject-bound snapshots, or subject-bound tag
  reads. These operations are available only through
  `ObjectReadMetadataNodeClient`, whose local adapter now owns the sole
  embedded implementation as the Unix adapter already did.
- this preserves the existing typed object-metadata PG and exact
  bucket/key/version/subject identity boundary while preventing new mixed-client
  callers from bypassing the role-specific interface by adding another raw
  aggregate call.
- bucket metadata/write-reservation, object mutation, and payload-reclaim
  duplicates in the transitional aggregate, together with capability
  requirements on the role-specific node-client traits, remain open in Phase
  3.
- the focused Unix object-read boundary regression and all 2,361 storage tests
  pass.

Seventy-ninth Phase 3 slice:

- the transitional `StorageNodeClient` aggregate no longer exposes bucket
  snapshot, paired-snapshot, raw/info lookup, command-construction, or
  subresource-read operations. These operations are available only through
  `BucketMetadataNodeClient`, whose local adapter now owns the sole embedded
  implementation as the Unix adapter already did.
- this preserves the role-typed bucket PG boundary for bucket creation,
  multipart completion barriers, bucket versioning/ACL/property/subresource
  mutations, and admitted bucket reads while preventing new mixed-client
  callers from bypassing it through the aggregate interface.
- bucket write-reservation, object mutation and multipart-state, and
  payload-reclaim/lifecycle duplicates in the transitional aggregate,
  together with capability requirements on the role-specific node-client
  traits, remain open in Phase 3.
- four focused bucket RPC regressions and all 2,361 storage tests pass, as do
  formatting, the storage boundary checker, and workspace-wide strict Clippy.

Eightieth Phase 3 slice:

- the transitional `StorageNodeClient` aggregate no longer exposes durable
  bucket-write reservation acquisition, validation, heartbeat, release, or
  bucket-write drain and delete-attempt coordination. These operations are
  available only through `BucketWriteReservationNodeClient`, whose local
  adapter now owns the sole embedded implementation as the Unix adapter
  already did.
- multipart completion barrier construction now validates its durable proof
  through the reservation role rather than reaching back through the mixed
  aggregate. Fenced acquisition, drain installation, and heartbeat continue
  to enforce the admitted effect boundary immediately before the PG-store
  mutation.
- object mutation and multipart-state plus payload-reclaim/lifecycle
  duplicates in the transitional aggregate, together with capability
  requirements on the remaining role-specific node-client traits, remain open
  in Phase 3.
- five focused bucket reservation/drain RPC regressions and all 2,361 storage
  tests pass, as do formatting, the storage boundary checker, and
  workspace-wide strict Clippy.

Eighty-first Phase 3 slice:

- the transitional `StorageNodeClient` aggregate no longer exposes object
  metadata mutation snapshots and command construction, current/specific
  delete snapshots and command construction, delete-marker construction, or
  per-key lifecycle version loading. These operations are available only
  through `ObjectMutationMetadataNodeClient`, whose local adapter now owns the
  sole embedded implementation as the Unix adapter already did.
- the role continues to bind typed object-metadata PG placement, expected
  stored-object or version-list snapshots, delete targets, reservation proofs,
  and command epochs at command construction. Moving the implementation does
  not weaken the existing stale-subject and stale-payload checks.
- multipart/stream-session and payload-reclaim/lifecycle duplicates in the
  transitional aggregate, together with capability requirements on the
  remaining role-specific node-client traits, remain open in Phase 3.
- two focused object mutation RPC regressions and all 2,362 storage tests pass,
  as do formatting, the storage boundary checker, and workspace-wide strict
  Clippy.

Eighty-second Phase 3 slice:

- the transitional mixed `StorageNodeClient` aggregate is removed. Its final
  multipart/stream-session, payload-reclaim/lifecycle, and bucket-worker
  duplicates no longer form a second compiler-visible node-client interface;
  callers can name only the already role-specific traits.
- the local adapter retains private same-module helpers for the shared embedded
  implementations used by `ObjectMutationMetadataNodeClient` and
  `BucketWriteReservationNodeClient`. These helpers cannot be reached by
  cluster routing or other production modules, while the trait boundaries
  remain the sole callable surface.
- the two remaining test-only aggregate consumers now use their intended
  boundaries directly: shard-scavenger observation reads use
  `ShardScavengerNodeClient`, and process-local payload-lease inspection uses
  the raw node test facade. `LocalNodeStore` and `LocalNodeClients` therefore
  no longer retain a mixed client trait object.
- capability requirements on the remaining role-specific node-client traits
  remain open in Phase 3.
- the two focused redirected-consumer regressions and all 2,366 storage tests
  pass, as do formatting, the storage boundary checker, and workspace-wide
  strict Clippy. The full 7,679-test workspace suite also passes.

Eighty-second Phase 3 review correction:

- guidance now distinguishes role-specific production and cross-boundary test
  interfaces from the sanctioned raw-node facade used for deliberately
  process-local assertions. The boundary checker reports the same replacement
  instead of directing developers to the removed aggregate.

Eighty-third Phase 3 slice:

- bucket-write acquisition and retained cleanup are now separate compiler-
  visible node-client roles. `BucketWriteReservationNodeClient` retains active-
  route acquisition, heartbeat, worker scans, and claim creation, while
  `RetainedBucketWriteReservationNodeClient` owns exact reservation/proof
  release, drain clearing, and delete-finalizer/lifecycle-claim release.
- local and Unix adapters implement both roles, but `LocalNodeStore` and
  `LocalNodeClients` preserve separate trait objects. Cluster call sites must
  therefore choose the active or retained interface explicitly; an active
  reservation client can no longer invoke a retained cleanup operation.
- existing retained-route Unix regressions now invoke the retained role
  directly, including stale-route and wrong-PG rejection for reservation,
  drain, delete-finalizer claim, and lifecycle claim cleanup. Opaque capability
  arguments for the remaining role-specific node-client traits remain open in
  Phase 3.
- all 54 focused Unix bucket-role regressions and all 2,366 storage tests pass,
  as do formatting, the storage boundary checker, and workspace-wide strict
  Clippy. The full 7,679-test workspace suite also passes.

Eighty-fourth Phase 3 slice:

- active object mutation and retained object cleanup are now separate node-
  client roles. `ObjectMutationMetadataNodeClient` retains active object,
  multipart, stream-session, and payload-reclaim mutation, while
  `RetainedObjectMutationMetadataNodeClient` owns exact retained stream-abort
  preparation and payload-reclaim claim release.
- local and Unix adapters implement both roles, but `LocalNodeStore` and
  `LocalNodeClients` retain distinct trait objects. Retained stream cleanup
  selects the recovery route's retained client, and the reclaim worker now
  acquires its durable claim through the active role before releasing that
  exact claim through the retained role.
- five focused Unix regressions cover expired-route retained cleanup, abort-
  command subject binding, active mutation parity, and equivalent wrong-PG
  rejection. Opaque capability arguments for the remaining role-specific
  node-client traits remain open in Phase 3.
- all 2,366 storage tests pass, as do formatting, the storage boundary checker,
  and workspace-wide strict Clippy. The full 7,679-test workspace suite also
  passes.

Eighty-fifth Phase 3 slice:

- active data-route operations and retained exact-placement operations are now
  separate node-client roles. `PlacedShardNodeClient` and
  `ShardAckNodeClient` retain current serving, mutation, repair, and backfill
  work, while `RetainedPlacedShardNodeClient` and
  `RetainedShardAckNodeClient` own historical shard/ack inspection and exact
  retained cleanup.
- local and Unix adapters implement both roles, but `LocalNodeStore` and
  `LocalNodeClients` retain distinct trait objects. Historical reads and
  retained-epoch cleanup therefore cannot be invoked through an active shard
  or acknowledgement client.
- focused Unix regressions cover historical shard inspection, cross-epoch
  segment reads, and old-primary direct-PUT/CopyObject cleanup of both remote
  shard files and acknowledgement rows. Opaque capability arguments for the
  remaining role-specific node-client traits remain open in Phase 3.
- all 2,366 storage tests pass, as do formatting, the storage boundary checker,
  and workspace-wide strict Clippy. The full 7,679-test workspace suite also
  passes.

Eighty-sixth Phase 3 slice:

- object-payload lease acquisition and reclaim cleanup are now separate node-
  client roles, matching the existing storage-RPC admission split.
  `ObjectPayloadLeaseNodeClient` retains active lease acquisition,
  reclaim-begin, and count operations, while
  `RetainedObjectPayloadReclaimNodeClient` owns exact-authority reclaim finish
  and fence clearing. The opaque lease object continues to own its exact
  release operation.
- local and Unix adapters implement both roles, but `LocalNodeStore` and
  `LocalNodeClients` retain distinct trait objects. Multi-node reclaim begin
  now records only retained cleanup clients for rollback, so an active lease
  client cannot finish or clear a reclaim fence.
- focused embedded and Unix regressions cover runtime-map-surviving lease
  release, reclaim begin/finish, crossed claim authority, active-lease
  exclusion, and fence retention across cleanup failure. Opaque capability
  arguments for the remaining role-specific node-client traits remain open in
  Phase 3.
- all 2,366 storage tests pass, as do formatting, the storage boundary checker,
  and workspace-wide strict Clippy. The full 7,679-test workspace suite also
  passes.

Eighty-seventh Phase 3 slice:

- shard inventory/reference scanning and durable observation state are now
  separate node-client roles. `ShardScavengerNodeClient` is read-only and owns
  route-history, shard-file, shard-row, and object-reference scans;
  `ShardScavengerObservationNodeClient` owns primary-only observation record,
  list, and resolution operations.
- local and Unix adapters implement both roles, but `LocalNodeStore` and
  `LocalNodeClients` retain distinct trait objects. The cluster audit selects
  an observation client only from the routed data-PG primary, while per-node
  file inventory remains available only through the scan client.
- focused coverage includes all shard-scavenger tests and the Unix shard
  client parity regression, pinning primary-only durable observation state and
  read-only scan behavior. Opaque capability arguments for the remaining
  role-specific node-client traits remain open in Phase 3.
- all 2,366 storage tests pass, as do the focused Unix parity regression,
  formatting, the storage boundary checker, workspace-wide strict Clippy, and
  the full workspace test suite.

Eighty-eighth Phase 3 slice:

- retained stream-abort application and pending-slot finish are no longer
  callable through the ordinary metadata-command publisher interface. The new
  `RetainedMetadataCommandNodeClient` contains only those two exact retained-
  route operations; `MetadataCommandNodeClient` retains active publication,
  convergence, and the not-yet-separated recovery/reissue method family.
- local and Unix adapters implement both roles, but `LocalNodeStore` and
  `LocalNodeClients` keep distinct trait objects. Both the full Unix-node
  installer and the metadata-command-only installer wire the retained role,
  while retained stream cleanup explicitly selects it from each historical
  acting-set node.
- the existing Unix retained-abort regressions now invoke the retained role
  directly and continue to pin expired-route cleanup, wrong-PG rejection,
  subject binding, PutObject cleanup, and UploadPart cleanup. Splitting broader
  recovery/reissue from active publication and requiring opaque capabilities
  on the resulting stateful traits remain open in Phase 3; peering/transfer is
  separated by the next slice below.
- both focused retained-abort Unix regressions and the full storage suite pass,
  as do formatting, the storage boundary checker, and workspace-wide strict
  Clippy. The full 7,688-test workspace suite also passes.

Eighty-ninth Phase 3 slice:

- metadata-command state inspection and quiesced-PG peering/transfer mutation
  are now distinct from ordinary publisher authority.
  `MetadataCommandInspectionNodeClient` exposes only replica state,
  checkpoint, retained-log, acceptance, and abandonment reads;
  `MetadataCommandPeeringNodeClient` adds replay-state validation, retained-log
  catch-up, and transfer initialization/adoption, but cannot allocate command
  IDs or install pending request-path commands. `MetadataCommandNodeClient`
  retains active publication/convergence and its still-combined recovery and
  reissue operations.
- local and Unix adapters implement the three roles separately, and both
  Unix-client installers preserve distinct trait objects in `LocalNodeStore`.
  Peering inspection/export paths select the read-only role, while startup
  replay, catch-up, and transfer import explicitly select the peering role.
  The locked metadata-command publisher session no longer exposes transfer or
  peering replay methods.
- existing Unix RPC and metadata-transfer regressions now invoke the narrow
  inspection and peering interfaces directly. The storage boundary checker
  recognizes the three role-specific accessors without treating their method
  calls as raw PG access. Splitting recovery/reissue from active publication
  and requiring opaque capabilities on the remaining stateful traits remain
  open in Phase 3.
- all 2,370 storage tests pass, including the focused 39-test Unix metadata-RPC
  and 80-test metadata-transfer groups. Formatting, the storage boundary
  checker, and workspace-wide strict Clippy pass, as does the full 7,689-test
  workspace suite.

Ninetieth Phase 3 slice:

- pending-command reissue, explicitly authorized recovery apply, and durable
  abandonment are now removed from `MetadataCommandNodeClient` and owned by
  `MetadataCommandRecoveryNodeClient`. The recovery role exposes only the
  command-state reads needed to make replacement and apply decisions while
  holding its critical section; it cannot allocate command IDs, install fresh
  request-path commands, remove completed slots, or apply ordinary commands.
- `LocalNodeStore` and `LocalNodeClients` retain a separate recovery trait
  object, and both Unix-client installers wire that role independently.
  Ordinary primary-first fanout continues through the active publisher;
  reissue and authorized historical-route recovery select the recovery role
  explicitly, including a distinct locked Unix recovery session. Acting-set
  maximum-log inspection during reissue uses only the read-only inspection
  role and never acquires an active publisher.
- the successful Unix idempotent-reissue regression now performs both slot
  replacements through one locked recovery session, while malformed
  abandonment responses are exercised through the same narrow role. The
  storage boundary checker recognizes recovery clients and no longer exempts
  the complete reissue function from inspection. Replacing the remaining
  stateful method families with opaque operation capabilities remains open in
  Phase 3.
- all 2,370 storage tests pass, including the focused 104-test recovery and
  39-test Unix metadata-RPC groups. Formatting, the storage boundary checker,
  and workspace-wide strict Clippy pass, as does the full 7,689-test workspace
  suite.

Ninety-first Phase 3 slice:

- metadata-command recovery locking now returns a distinct
  `MetadataCommandRecoveryCriticalSection` bound to the PG and epoch selected
  when it is opened. Its read, reissue, recovery-apply, and abandonment methods
  no longer accept caller-supplied PG or epoch values, so authority acquired
  for one recovery subject cannot be redirected to another.
- every recovery mutation, including replica application and abandonment, now
  opens this scoped interface. The base `MetadataCommandRecoveryNodeClient`
  exposes only critical-section construction. Read-only reissue reconciliation
  continues through `MetadataCommandInspectionNodeClient`, avoiding both
  recovery mutation authority and ordinary publisher authority.
- local and Unix regressions pass a command for another PG through a critical
  section bound to PG 0, require fail-closed rejection, and verify the bound PG
  remains unmodified. Replacing the remaining active and retained stateful
  method families with opaque operation capabilities remains open in Phase 3.
- all 2,372 storage tests pass, including the focused 104-test recovery and
  40-test Unix metadata-RPC groups. Formatting, the storage boundary checker,
  and workspace-wide strict Clippy pass, as does the full 7,691-test workspace
  suite.

Ninety-first Phase 3 review correction:

- the embedded recovery critical section now validates every command envelope
  against both its captured PG and captured epoch before any PgStore access.
  This includes acceptance reads, both reissue forms, recovery application,
  and abandonment; authorized and abandoned source envelopes are checked as
  well as the mutation target.
- an epoch-1 embedded section presented with an epoch-2 command now fails with
  `StaleMetadataOperation` and leaves both epoch log tips unchanged, matching
  the fail-closed route binding already enforced by the Unix RPC boundary.
- all 2,373 storage tests pass, including the focused 104-test recovery group.
  Formatting, the storage boundary checker, and workspace-wide strict Clippy
  pass, as does the full 7,692-test workspace suite.

Ninety-second Phase 3 slice:

- ordinary primary metadata-command application now opens a distinct
  `MetadataCommandCriticalSection` bound to the PG and epoch selected when the
  active lock is acquired. Its acceptance and apply methods accept only the
  command envelope, so the locked authority cannot be redirected by supplying
  another route to an individual operation.
- the embedded implementation validates every command against the captured PG
  and epoch before PgStore access. The Unix implementation retains the PG-lock
  session and derives the request route from that session; its production
  interface exposes only the two operations actually used under the active
  critical section.
- embedded regressions require wrong-PG and future-epoch commands to fail
  without advancing either log. A Unix regression requires the explicit
  command-versus-session route-mismatch diagnostic and verifies no log entry is
  created. Replacing the remaining active and retained stateful method families
  with opaque operation capabilities remains open in Phase 3.
- all 2,392 storage tests pass, including the focused active-section
  regressions and 41-test Unix metadata-RPC group. Formatting, the storage
  boundary checker, and workspace-wide strict Clippy pass, as does the full
  7,714-test workspace suite.

Ninety-third Phase 3 slice:

- retained stream-abort preparation now returns an opaque
  `PreparedRetainedStreamUploadAbort` whose construction is confined to the
  private node-runtime boundary. Promotion binds the command envelope to its
  object-metadata PG, cluster epoch, bucket, key, session, staged-segment
  sessions, and optional canonical PutObject/UploadPart stream-create
  reservation proof.
- retained abort apply and pending-slot finish consume that prepared value and
  no longer accept caller-supplied PGs or command envelopes. Embedded, Unix,
  storage-node RPC, and cluster fanout paths derive both values from the same
  opaque authority; the RPC server promotes hostile wire commands only after
  its retained-route and subject validation.
- adversarial coverage keeps sending raw wrong-PG apply and finish frames at
  the Unix boundary and requires `PayloadDecode`, while the constructor
  regression rejects mismatched PG, epoch, bucket, key, session, and staged
  segment provenance. Replacing the remaining active and retained stateful
  method families with opaque operation capabilities remains open in Phase 3.
- all 2,393 storage tests pass, including the focused retained-abort Unix and
  opaque-provenance regressions. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,715-test workspace suite also
  pass.

Ninety-fourth Phase 3 slice:

- quiesced metadata-command peering and transfer mutation now opens a scoped
  `MetadataCommandPeeringRoute` bound to the destination PG and epoch selected
  by the cluster workflow. Replay, transfer initialization/adoption, and
  checkpoint-base installation no longer accept a replacement destination PG
  or epoch at the individual effect call.
- embedded and Unix implementations validate replay envelopes and every
  command in a rebased transfer batch against the captured destination route
  before storage access or RPC. Checkpoint installation separately binds the
  checkpoint PG while preserving its source epoch as transfer provenance, so
  a checkpoint from an older epoch can still initialize the current
  destination epoch.
- startup recovery, retained-log catch-up, checkpoint import, and metadata
  transfer callers now retain the scoped route across their mutation sequence.
  Embedded regressions require wrong-PG and future-epoch command redirection to
  fail without advancing either log; the Unix regression requires the same
  failures, plus wrong-PG checkpoint rejection, before any RPC can be sent.
  Replacing the remaining stateful method families with opaque operation
  capabilities remains open in Phase 3.
- all 2,401 storage tests pass, including the focused peering-route and
  cross-epoch checkpoint-import regressions. Formatting, the storage boundary
  checker, workspace-wide strict Clippy, and the full 7,726-test workspace
  suite also pass.

Ninety-fifth Phase 3 slice:

- shard-scavenger observation mutation now opens a scoped
  `ShardScavengerObservationRoute` bound to the data PG selected by the active
  primary route. Record, list, and resolve operations no longer accept a
  replacement PG after the route is opened.
- embedded and Unix routes validate the data PG embedded in record/resolve
  subjects before PgStore or transport access, and validate every listed row
  before returning it through the scoped interface. A foreign-PG Unix response
  fails as a transport payload error. Storage-node RPC dispatch keeps its
  independent active-route, primary, and wire-subject validation before it
  constructs the embedded route; the cluster scavenger retains one scoped
  route throughout each PG audit.
- an embedded regression seeds exact canaries in both PGs and requires
  redirected record/resolve attempts to leave both unchanged. A Unix
  regression performs the same subject redirection without a listening server,
  proving rejection before RPC. A fake Unix peer returns an otherwise valid
  foreign-PG list row and must be rejected at the response boundary, while the
  full Unix shard-client test covers successful record/list/resolve dispatch.
  Replacing the remaining stateful method families with scoped or opaque
  operation capabilities remains open in Phase 3.
- all 2,404 storage tests pass, including the focused scoped-route, malformed
  response, raw RPC,
  and full Unix dispatch regressions. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,729-test workspace suite also
  pass.

Ninety-sixth Phase 3 slice:

- retained bucket-write cleanup now opens a scoped
  `RetainedBucketWriteReservationRoute` bound to the bucket metadata PG and
  exact bucket selected by the retained cluster route. Reservation-record and
  metadata-command-proof release, drain clearing, and delete-finalizer and
  lifecycle-claim release no longer accept a replacement PG after the route is
  opened.
- embedded and Unix routes validate every cleanup subject against the captured
  bucket before PgStore or transport access. Claim cleanup additionally binds
  the PG embedded in each durable claim to the captured bucket PG. Storage-node
  RPC dispatch retains its independent retained-route, primary, and wire-
  subject validation before constructing the embedded route.
- embedded and Unix regressions redirect all five cleanup subject forms to a
  foreign bucket and redirect both claim types to a foreign embedded PG. Every
  attempt must return the exact route-subject mismatch; the Unix regression
  runs without a listening server, proving rejection before RPC. Replacing the
  remaining stateful method families with scoped or opaque operation
  capabilities remains open in Phase 3.
- all 2,406 storage tests pass, including the focused scoped-route regressions
  and the full 47-test Unix bucket-RPC module. Formatting, the storage boundary
  checker, workspace-wide strict Clippy, and the full 7,731-test workspace
  suite also pass.

Ninety-seventh Phase 3 slice:

- retained object-metadata cleanup now opens a scoped
  `RetainedObjectMutationMetadataRoute` bound to the historical cluster epoch,
  object-metadata PG, bucket, and key selected by the retained cluster route.
  Stream-abort preparation no longer accepts replacement route or object
  arguments, and payload-reclaim claim release no longer accepts a replacement
  PG.
- embedded and Unix routes bind reclaim claims to the captured epoch, PG,
  bucket, and key before PgStore or transport access. The embedded factory
  additionally validates bucket/key placement before constructing the route;
  the Unix factory rejects an epoch that does not match its retained client.
  Storage-node RPC dispatch keeps its independent retained-route, primary, and
  wire-subject validation before constructing the embedded route.
- embedded and Unix regressions require foreign claim object, PG, and epoch
  subjects to fail before storage or RPC. The embedded regression also rejects
  route construction for a foreign object PG, while the Unix regression
  rejects foreign-epoch construction without a listening server. Existing
  expired-route and wrong-PG Unix regressions continue to exercise successful
  retained stream abort and reclaim-claim cleanup through the scoped route.
  Replacing the remaining stateful method families with scoped or opaque
  operation capabilities remains open in Phase 3.
- all 2,415 storage tests pass, including the focused retained-route cases and
  the full 30-test Unix object-RPC module. Formatting, the storage boundary
  checker, workspace-wide strict Clippy, and the full 7,741-test workspace
  suite also pass.

Ninety-eighth Phase 3 slice:

- retained object-payload reclaim completion now opens a scoped
  `RetainedObjectPayloadReclaimRoute` bound to the historical cluster epoch,
  exact bucket/key/generation root, and reclaim-claim proof selected when the
  reclaim fence is acquired. Finish and fence-clear operations no longer
  accept replacement route or authority arguments after that route is opened.
- embedded and Unix factories reject reclaim authority from a different epoch
  before storage or transport access; the Unix factory also rejects a route
  epoch that differs from its retained client. Cluster rollback, terminal
  completion, and fence clearing retain the same opened route throughout each
  cleanup attempt. Storage-node RPC dispatch keeps its independent retained-
  route, node, epoch, operation, and authority validation.
- an embedded regression creates matching fences for two object roots under
  one authority, then proves the scoped route can finish and clear only its
  bound root while the foreign root remains fenced. A no-listener Unix
  regression proves foreign route and authority epochs fail before RPC, while
  the existing crossed-authority Unix regression covers successful requests
  and server-side proof rejection through the new route. Replacing the
  remaining stateful method families with scoped or opaque operation
  capabilities remains open in Phase 3.
- all 2,429 storage tests pass as part of the full workspace run, including the
  three focused retained-reclaim route regressions. Formatting, the storage
  boundary checker, workspace-wide strict Clippy, and the full 7,755-test
  workspace suite also pass.

Ninety-ninth Phase 3 slice:

- active object-payload lease access now opens an `ObjectPayloadLeaseRoute`
  bound to one admitted cluster epoch and exact bucket/key/generation root.
  Broad and shard-location lease acquisition, lease counting, and reclaim-
  fence acquisition no longer accept replacement route subjects after the
  route is opened.
- embedded and Unix routes keep the same node-local lease semantics and Unix
  admission classes, while reclaim begin additionally requires its claim proof
  to match the route epoch before storage or transport access. The Unix
  factory rejects an epoch that differs from its installed active client, and
  storage-node RPC dispatch keeps its independent active-route, node, epoch,
  operation, and authority validation.
- an embedded regression proves one route acquires, counts, and fences only
  its bound object generation and rejects foreign-epoch authority. A no-
  listener Unix regression proves foreign route and authority epochs fail
  before RPC. Existing Unix saturation and crossed-authority regressions cover
  broad/narrow admission handoff, successful dispatch, and server-side proof
  rejection through the scoped route. The coarse-payload-lease boundary scan
  now excludes separately compiled `tests.rs` modules, matching its existing
  `tests/` and `*_tests.rs` exclusions instead of treating test-only calls as
  production readers. Replacing the remaining stateful method families with
  scoped or opaque operation capabilities remains open in Phase 3.
- all 2,431 storage tests pass as part of the full workspace run, including the
  four focused active payload-lease route regressions. Formatting, the storage
  boundary checker, workspace-wide strict Clippy, and the full 7,757-test
  workspace suite also pass.

One-hundredth Phase 3 slice:

- retained placed-shard access now opens a `RetainedPlacedShardRoute` bound to
  one exact historical `ShardLocation` and `ShardKey`. Historical inspection
  and cleanup no longer accept replacement placement or shard subjects after
  the route is opened.
- embedded and Unix factories reject a foreign node, PG, epoch, or shard index
  before storage or transport access as applicable. The opened routes retain
  the same historical-read acknowledgement validation and read-handle deletion
  fence semantics, while storage-node RPC dispatch keeps its independent
  retained-route, acting-set, and wire-subject validation.
- an embedded regression proves the route reads and deletes only its captured
  shard while a foreign shard remains intact, and rejects an unavailable PG
  before storage access. A no-listener Unix regression proves foreign node,
  future epoch, and shard-index subjects fail before RPC. The installed-Unix
  integration regression now performs both a historical read and cleanup
  delete against the remote storage node, proving the complete transport path.
  Replacing the remaining stateful method families with scoped or opaque
  operation capabilities remains open in Phase 3.
- all 2,433 storage tests pass, including the four focused retained placed-
  shard route regressions. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,759-test workspace suite also
  pass.

One-hundred-and-first Phase 3 slice:

- retained shard-acknowledgement access now opens a `RetainedShardAckRoute`
  bound to one historical cluster epoch, data PG, and shard key. Historical
  acknowledgement inspection and deletion no longer accept replacement route
  or shard subjects after the route is opened.
- embedded factories prove the captured data PG exists before constructing the
  route, while Unix factories reject future route epochs before transport
  access. Storage-node RPC dispatch retains its independent retained-route,
  primary, PG-role, and wire-subject validation before loading or deleting the
  acknowledgement row.
- an embedded regression records two acknowledgement rows and proves the
  opened route can inspect and delete only its captured row, leaving the
  foreign row intact; it also rejects an unavailable PG before storage access.
  A no-listener Unix regression rejects future-epoch construction before RPC.
  The installed-Unix historical shard regression now exercises old-epoch
  acknowledgement inspection and cleanup against the retained data-PG primary
  alongside the corresponding payload bytes. Existing old-primary direct-PUT
  and CopyObject regressions continue to prove retained acknowledgement
  cleanup through the scoped route. Replacing the remaining stateful method
  families with scoped or opaque operation capabilities remains open in Phase
  3.
- all 2,435 storage tests pass, including the focused retained shard-
  acknowledgement route regressions. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,761-test workspace suite also
  pass.

One-hundred-and-second Phase 3 slice:

- active shard-acknowledgement, repair, and backfill access now opens a
  `ShardAckRoute` bound to one active cluster epoch and data PG. Ack-row
  registration, validation, inspection, and deletion, plus durable repair and
  backfill record/claim workflows, no longer accept a replacement PG or epoch
  at each operation.
- embedded and Unix routes validate every repair/backfill work item and claim
  against the captured PG and epoch before storage or transport access. They
  also validate every listed or acquired durable row before returning it;
  acquired claims must also match the requested claim ID, owner token,
  acquisition timestamp, and lease deadline. Malformed Unix responses are
  reported as transport payload errors rather than conferring foreign-PG or
  foreign-claim state through the scoped interface. Storage-node RPC dispatch
  retains its independent active-route, primary, PG-role, and wire-subject
  checks before constructing the embedded route.
- embedded coverage records correct-PG repair and backfill canaries, rejects
  crossed-PG records and a crossed-epoch claim without changing those
  canaries, and rejects unavailable PGs when constructing the route. A no-
  listener Unix regression rejects foreign epochs and crossed-PG work before
  RPC. A fake Unix peer returns otherwise valid foreign-PG repair and backfill
  lists, both of which fail at the response boundary. Additional malicious
  responses independently vary each repair/backfill acquisition identity
  field and are rejected before the claim can reach a worker. Existing
  installed-Unix ack and repair/backfill tests continue to exercise successful
  dispatch through the scoped route. Active placed-shard I/O and other
  remaining stateful method families still require scoped or opaque
  capabilities in Phase 3.
- all 2,441 storage tests pass, including the focused active shard-ack route
  regressions. Formatting, the storage boundary checker, workspace-wide strict
  Clippy, and the full 7,768-test workspace suite also pass.

One-hundred-and-third Phase 3 slice:

- active placed-shard access now opens a non-cloneable `PlacedShardRoute`
  bound to one node, cluster epoch, data PG, shard index, and exact shard key.
  Writes, admitted writes, repair writes, reads, and deletes no longer accept
  replaceable placement or key arguments after route construction. The
  existing retained route remains the separate interface for historical reads
  and cleanup after route transitions.
- embedded and Unix route factories reject crossed node and shard-index
  subjects before storage or transport access. The embedded route also proves
  the data PG exists locally, while the Unix route rejects a stale or future
  epoch before opening a socket. The admitted-write path derives its durable
  effect check and portable RPC deadline from the epoch captured by the route,
  rather than accepting another operation epoch at the write call.
- the cluster placement layer continues to validate the active topology and
  acting set before opening the node route. Storage-node RPC dispatch retains
  its independent authenticated route, active-state, acting-set, shard-index,
  and effect-deadline checks. Private Unix encoding helpers remain behind the
  scoped route, so ordinary production callers can only obtain active shard
  I/O through the bound interface.
- embedded coverage proves that the route writes, reads, and deletes only its
  bound shard while preserving a same-PG foreign-key canary, and rejects
  crossed node, shard-index, and unavailable-PG construction. A no-listener
  Unix regression rejects crossed node/index/epoch subjects before RPC.
  Existing installed-Unix, pluggable-client, read-range, read-handle, and
  portable TLS deadline tests exercise successful I/O and the durable effect
  boundary through the scoped route.
- the standalone route-identity fixture added concurrently on the branch was
  completed with the required `metadata_transfer_destination_epoch: None`
  field so the all-feature storage test target remains constructible.
- all 2,463 storage tests pass, including the focused embedded, Unix, TLS,
  pluggable-client, and standalone route-identity regressions. Formatting, the
  storage boundary checker, workspace-wide strict Clippy, and the full
  7,794-test workspace suite also pass.

One-hundred-and-fourth Phase 3 slice:

- shard read-handle acquisition now opens a non-cloneable
  `ShardReadHandleRoute` bound to one operation ID and an exact batch of shard
  locations and keys. The route is consumed when it acquires the opaque lease,
  so callers cannot redirect or reuse the selected batch after construction.
  The low-level Unix session constructor is private in production; its
  crate-visible adapter remains test-only for session idempotence and
  disconnect cleanup coverage.
- embedded and Unix factories reject empty batches, crossed route/location
  epochs, foreign nodes, and shard-index/key mismatches before storage or
  transport access. Embedded construction additionally proves every data PG
  exists locally, while Unix construction proves the route epoch matches the
  installed client. The cluster layer retains its independent topology and
  acting-set validation for every entry before grouping the batch by node, and
  storage-node dispatch retains its independent active-route, per-location,
  session-domain, and deadline checks.
- embedded coverage exercises successful acquire/release and rejects crossed
  epoch, node, shard-index, unavailable-PG, and empty subjects. A no-listener
  Unix regression rejects foreign epoch/node/index subjects before RPC, while
  a malicious peer returning a different location is rejected at the scoped
  route's response boundary. Existing installed-Unix deletion fencing,
  aggregate admission saturation, and partial multi-node acquisition failure
  regressions exercise successful leases and cleanup through the route.
- all 2,465 storage tests pass, including the focused embedded, Unix,
  admission-saturation, and multi-node rollback regressions. Formatting, the
  storage boundary checker, workspace-wide strict Clippy, and the full
  7,796-test workspace suite also pass.

One-hundred-and-fifth Phase 3 slice:

- shard-scavenger file/row inventory now opens a `ShardScavengerDataRoute`
  bound to one active epoch and exact data PG. Object payload-reference
  inventory separately opens a `ShardScavengerObjectScanRoute` bound to one
  active epoch and exact object-metadata scan PG. Scan methods no longer accept
  replacement PG arguments after route construction. The node-wide exact
  history-reference report remains on the parent interface because it has no
  caller-selected PG or operation subject.
- embedded factories prove each captured PG exists before storage access, and
  Unix factories reject an epoch differing from the installed client before
  transport access. Private Unix encoding helpers remain behind the scoped
  routes. Cluster scavenger audit, backfill discovery, and payload-ownership
  checks continue to select nodes and role-specific PG IDs from the installed
  topology before opening those routes.
- storage-node file scans now construct an active, admission-domain-bound data
  scan capability instead of validating only the raw route tuple. Row scans
  use the existing active primary-data capability, while payload-reference
  scans retain the active primary object-scan capability. All three recheck
  the captured immutable route deadline immediately before node access.
- embedded coverage exercises both correct-PG routes and rejects unavailable
  data and object PGs before storage. A no-listener Unix regression rejects
  future-epoch routes before RPC. Existing installed-Unix, role-validation,
  primary-only, and file-list regressions exercise successful dispatch; the
  active data capability regression now pins file/row scan admission-domain
  rejection and expiry before node access.
- all 2,470 storage tests pass, including the non-primary PG-mutex regression
  proving shard-file inventory does not acquire the metadata-store lock.
  Formatting, the storage boundary checker, workspace-wide strict Clippy, and
  the full 7,801-test workspace suite also pass.

One-hundred-and-sixth Phase 3 slice:

- object-read metadata access now opens an `ObjectReadMetadataRoute` bound to
  one active cluster epoch, exact object-metadata PG, bucket, and key. Loading
  the authorization subject, loading its identity-checked snapshot, and
  reading its identity-checked tags no longer accept replacement placement or
  object arguments after route construction. Version selection remains an
  operation within the fixed object route because one request may retry the
  same selected version after an identity conflict.
- embedded construction validates the PG against the installed topology's
  placement for the exact bucket/key and proves the PG store is open before
  access. Unix construction rejects an epoch differing from its installed
  client before transport access and keeps the raw RPC helpers private behind
  the scoped route. Storage-node dispatch retains its independent active
  admission-domain, route, placement, primary, and captured-deadline checks
  before constructing the embedded route.
- cluster authorization/snapshot/tag retry loops open the route once and can
  vary only the version and expected durable identity. Local coverage rejects
  a configured but crossed object PG before storage; a no-listener Unix
  regression rejects a future epoch before RPC. Existing installed-Unix
  correct/wrong-PG tests exercise all three operations through the route, and
  the storage-node active-object regression continues to pin immutable
  deadline enforcement before node access.
- all 2,472 storage tests pass. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,803-test workspace suite also
  pass.

One-hundred-and-seventh Phase 3 slice:

- object, object-version, and multipart-upload page reads now open an
  `ObjectListingMetadataRoute` bound to one active cluster epoch and exact
  object-metadata scan PG. Page operations no longer accept a replacement PG
  after route construction, while their bucket, prefix, and cursor requests
  remain per-operation values within that fixed scan partition.
- embedded construction proves the scan PG is open before storage access.
  Unix construction rejects an epoch differing from its installed client
  before transport access and keeps the raw RPC encoding helpers private
  behind the scoped route. Storage-node dispatch retains its independent
  active admission-domain, route, primary, PG-role, and captured-deadline
  checks before constructing the embedded route.
- cluster listing fan-out opens one route for each selected PG. Bucket-delete
  visibility checks reuse one route for their version and multipart-upload
  probes, and diagnostic scans also construct the scoped route before reading
  a page. Embedded coverage rejects an unavailable scan PG before storage; a
  no-listener Unix regression rejects a future epoch before RPC. The existing
  installed-Unix matrix exercises all three successful operations and keeps
  server-side unknown-PG rejection pinned.
- all 2,474 storage tests pass. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,805-test workspace suite also
  pass.

One-hundred-and-seventh Phase 3 review correction:

- scoped listing authority now covers returned subjects as well as request
  routing. Embedded object, object-version, and multipart-upload page reads
  fail closed if any returned `(bucket, key)` maps to a different object
  metadata PG, and the storage-node response boundary independently validates
  every row before encoding it for an RPC peer.
- Unix object-listing routes own an immutable snapshot of the installed PG
  topology. The full Unix client installer and the role-specific listing
  installer bind that snapshot when the client is constructed; opening a
  listing route without it fails before transport. Response validators use
  the snapshot and scoped scan PG to reject a misplaced row from an otherwise
  authenticated peer before it can enter the cluster-wide merge.
- adversarial embedded and Unix regressions inject a foreign-PG object row,
  object-version row, and multipart-upload row while retaining the expected
  bucket and valid pagination shape. All three listing forms must reject the
  response rather than returning partially trusted results.
- all 2,476 storage tests pass. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,807-test workspace suite also
  pass.

One-hundred-and-seventh Phase 3 review follow-up:

- a misplaced durable row is normally detected by the embedded scoped route
  before the storage-node response boundary can inspect the successful page.
  The listing capability now translates that listing-specific
  `RouteCapabilitySubjectMismatch` into `PayloadDecode`; it does not pass
  through the general bucket-snapshot mapper as an internal server failure.
- an installed Unix server/client regression writes an object and multipart
  upload deliberately into the wrong durable PG, then proves object,
  object-version, and multipart-upload listing RPCs all return
  `PayloadDecode`. The direct Unix response-validator regression remains to
  cover a faulty authenticated peer that encodes a successful but crossed-PG
  page.
- all 2,477 storage tests pass as part of the full suite. Formatting, the
  storage boundary checker, workspace-wide strict Clippy, and the full
  7,808-test workspace suite also pass.

One-hundred-and-eighth Phase 3 slice:

- object-generation metadata access now opens an
  `ObjectGenerationMetadataRoute` bound to one active cluster epoch, exact
  object-metadata PG, bucket, and key. Reservation lookup and next-generation
  inspection no longer accept replacement placement or object arguments after
  route construction.
- embedded construction validates the PG against the installed topology's
  placement for the exact bucket/key and proves the PG store is open before
  access. Unix construction rejects an epoch differing from its installed
  client before transport and keeps raw RPC encoding behind the scoped route.
  Storage-node dispatch retains its independent active admission-domain,
  route, placement, primary, and captured-deadline checks before constructing
  the embedded route.
- object-generation reservation allocation opens the route once per retry
  iteration and uses that same scoped route for the reservation lookup and
  both next-generation observations. Embedded coverage rejects a configured
  but crossed object PG before storage, a no-listener Unix regression rejects
  a future epoch before RPC, and the installed Unix equivalent-state matrix
  requires `PayloadDecode` for both operations on a wrong PG while preserving
  correct-PG canaries.
- all 2,481 storage tests pass. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,813-test workspace suite also
  pass.

One-hundred-and-ninth Phase 3 slice:

- object-version metadata access now opens an `ObjectVersionMetadataRoute`
  bound to one active cluster epoch, exact object-metadata PG, bucket, and
  key. Version inspection no longer accepts replacement placement or object
  arguments after route construction.
- ordinary allocation and multipart-completion allocation remain separate
  zero-argument route operations so the Unix adapter preserves their normal
  and completion-reserved admission classes. Acting-set inspection opens one
  scoped route per selected node and chooses the required operation without
  resupplying the object subject.
- embedded construction validates exact object placement and an open PG before
  access. Unix construction rejects a foreign epoch before transport, while
  storage-node dispatch independently retains active admission-domain, route,
  placement, and captured-deadline validation. Embedded and no-listener Unix
  regressions pin construction failures, and installed Unix equivalent-state
  coverage requires `PayloadDecode` for both ordinary and completion-priority
  operations on a wrong PG.
- all 2,484 storage tests pass. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,815-test workspace suite also
  pass.

One-hundred-and-tenth Phase 3 slice:

- direct-PUT metadata access now opens a `DirectPutMetadataRoute` bound to one
  active cluster epoch, exact object-metadata primary PG, bucket, and key.
  Commit-snapshot loading accepts only the reservation and generation within
  that subject, while command construction no longer accepts replaceable PG
  or epoch fields.
- the immutable direct-PUT commit description still carries bucket and key
  because it is also the command payload source. Embedded and Unix routes
  validate those fields and both carried bucket-write proofs against their
  captured subject before storage or transport. Unix response validation uses
  the route-owned epoch, PG, bucket, and key rather than trusting the commit
  description to define response authority.
- cluster commit/retry processing opens one route from the selected primary
  before snapshot retries and reuses it through command construction.
  Storage-node dispatch retains independent active admission-domain, route,
  placement, primary, and captured-deadline checks before opening the embedded
  route. Raw PG and epoch fields are removed from
  `BuildDirectPutCommitCommandReq`.
- embedded coverage rejects a configured crossed PG before storage, a
  no-listener Unix regression rejects a foreign epoch before RPC, the
  installed equivalent-state matrix retains exact `PayloadDecode` wrong-PG
  checks, and the Unix build regression rejects a crossed commit description
  locally without issuing another RPC.
- the transitional boundary check now recognizes only the scoped direct-PUT
  route as the permitted receiver for commit-snapshot reads and command
  builds; its diagnostic no longer directs new code toward the removed raw
  client methods.
- reviewer follow-up found that the route initially bound only the proof's
  bucket and equality, while the production direct path still acquired the
  generic `bucket-write-snapshot` reservation. Direct buffered PUT now owns
  the canonical `put-object-direct-commit` operation identity and exact key
  target. Both embedded and Unix route builders reject crossed operation,
  target, or epoch proofs before command construction or transport, and the
  storage-node active-primary route independently enforces the same subject.
- metadata-command fanout now accepts a `CommitDirectPutObject` reservation
  only when its bucket, key target, and command epoch match and its operation
  is either the canonical direct-commit identity or the independently
  canonical promoted-stream creation identity. A live same-bucket proof for
  another operation or key therefore cannot authorize publication through a
  malformed pending/recovery command. Adversarial coverage proves central
  validation rejects both crossed identities and recovery abandons a malformed
  pending command without publishing an object.
- all 2,487 storage tests pass. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,819-test workspace suite also
  pass.

One-hundred-and-eleventh Phase 3 slice:

- ordinary PUT-object metadata mutation now opens a `PutObjectMetadataRoute`
  bound to one active cluster epoch, exact object-metadata primary PG, bucket,
  and key. Snapshot loading accepts only version selection within that subject;
  command construction no longer accepts replaceable PG, epoch, bucket, or key
  fields.
- embedded route construction validates exact object placement and an open PG
  before storage access. Unix construction rejects a foreign epoch before
  transport and owns the wire object subject. Storage-node dispatch retains
  independent active admission-domain, route, placement, primary, and captured
  deadline checks before opening the embedded route.
- command construction requires the carried reservation proof to match the
  canonical `put-object-metadata` operation, exact key target, bucket, and route
  epoch before storage or transport access. Metadata-command fanout and
  recovery independently enforce the same stable subject, so a malformed
  pending command cannot bypass the scoped builder.
- embedded coverage rejects a configured crossed PG before storage, a
  no-listener Unix regression rejects a foreign epoch before RPC, and the
  installed equivalent-state matrix continues to require `PayloadDecode` for
  wrong-PG snapshot and command operations. Crossed operation, target, and
  epoch proofs fail locally before another RPC. A recovery regression uses
  live same-bucket crossed reservations and proves malformed pending work is
  abandoned without changing the object on any acting-set node.
- the transitional boundary check now recognizes only the scoped PUT-object
  metadata route as the permitted receiver for snapshot reads and command
  builds. Other active object-mutation method families remain open in Phase 3.
- all 2,499 storage tests pass. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,825-test workspace suite also
  pass.

One-hundred-and-twelfth Phase 3 slice:

- current/specific delete snapshots, lifecycle version inspection, delete
  command construction, and delete-marker construction now share an
  `ObjectDeleteMetadataRoute` bound to one active cluster epoch, exact
  object-metadata primary PG, bucket, and key. Operation methods can no longer
  replace those route identities.
- embedded route construction rejects crossed placement before storage. Unix
  route construction rejects a foreign epoch before transport, and installed
  wrong-PG requests continue through server-side placement validation and
  return `PayloadDecode` rather than failing because equivalent state is
  absent.
- each builder validates its canonical reservation operation, exact key
  target, bucket, and route epoch before storage or transport. Expected
  objects, lifecycle version lists, delete targets, stale-payload sources, and
  explicit reclaim descriptions are also constrained to the route subject.
- lifecycle workers now acquire the same canonical `delete-current-object`,
  `delete-object-version`, or `insert-delete-marker` reservation identity as
  the durable command they construct. This closes the former embedded-only
  gap where lifecycle-specific labels bypassed the Unix/storage-node proof
  checks.
- metadata-command fanout and recovery independently validate delete and
  marker command proof subjects. `DeleteObjectVersionCommand` now preserves
  whether it was authorized as a current-object or explicit-version deletion,
  and central validation requires that mode's exact canonical reservation
  operation. Live current/specific crossed-operation and crossed-target
  regressions reject proof substitution.
- command application reconstructs a live delete's expected reclaim record
  from the durable object segments or multipart part segments and requires
  full equality, normalizing only the cleanup timestamp. Null delete-marker
  replacement applies the same exact check and forbids a reclaim when no live
  payload is replaced. Malformed recovery-envelope regressions cover foreign
  roots, omitted durable shards, and forged shard identities, proving both
  delete command shapes fail transactionally without deleting the object or
  creating an attacker-selected reclaim root.
- preserving delete authorization mode changes durable command encoding, so
  metadata-command encoding advances from version 5 to 6 and explicitly
  rejects version 5 rather than interpreting the old ambiguous layout.
- the transitional boundary check now requires all production delete
  snapshots, lifecycle lists, and delete/marker command builds to flow through
  the scoped route. Remaining object-mutation families stay open in Phase 3.
- all 2,504 storage tests pass. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,830-test workspace suite also
  pass.

One-hundred-and-thirteenth Phase 3 slice:

- multipart-upload creation idempotence matching and command construction now
  share a `MultipartUploadCreationMetadataRoute` bound to one active cluster
  epoch, exact object-metadata primary PG, bucket, and key. The operation
  methods can no longer accept replacement route identities, and the command
  build request no longer carries raw PG or epoch fields.
- embedded route construction validates object placement and an open PG before
  storage access. Unix route construction rejects a foreign epoch before
  transport, owns the wire object subject, and validates the returned command
  against the captured route and original request. Storage-node dispatch keeps
  independent active-route, placement, primary, deadline, object-subject, and
  proof checks before opening the embedded route.
- command construction and idempotence matching require the canonical
  `create-multipart-upload` reservation operation, exact key target, bucket,
  and route epoch. Metadata-command fanout and recovery independently enforce
  the same proof subject. A malformed pending creation carrying a live
  same-bucket proof for another operation is therefore abandoned without
  publishing an upload on any acting-set replica.
- embedded coverage rejects a configured crossed PG before storage and crossed
  operation, target, and epoch proofs at the scoped route. No-listener Unix
  coverage rejects foreign authority before RPC; installed equivalent-state
  coverage retains exact `PayloadDecode` wrong-PG checks and a correct-PG
  positive canary. Adversarial response validation remains bound to the exact
  request and route.
- the transitional boundary checker now permits creation matching and command
  construction only through the scoped multipart-creation route. Multipart
  management/completion/abort, stream-session, and payload-reclaim families
  remain open in Phase 3.
- all 2,519 storage tests pass. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,843-test workspace suite also
  pass.

One-hundred-and-thirteenth Phase 3 review correction:

- idempotence matching now requires the expected command to represent the
  exact creation request, including upload issuance identity, metadata,
  ownership, ACL, object-lock, checksum, and encryption fields. A command for
  another same-key upload can no longer select its upload ID and report a
  successful match for the current request.
- the request-to-command comparison is shared with applied-command selection,
  preserving ordered-upload-ID issuance semantics in one implementation.
  Embedded and pre-RPC Unix regressions reject crossed upload IDs and metadata,
  while storage-node capability coverage independently rejects a malformed
  authenticated RPC request before the embedded lookup.
- RPC request encoding and decoding use that same comparison rather than an
  exact upload-ID duplicate. Codec and installed-Unix regressions preserve the
  valid idempotence-recovery case where a provisional `(0, 0)` request carries
  the authenticated command and stored upload ID ordered by command position.

One-hundred-and-fourteenth Phase 3 slice:

- ordinary multipart-upload loading, in-progress loading, listing-oriented
  loading, and terminal-management/replay lookup now share a
  `MultipartUploadLookupMetadataRoute` bound to one active cluster epoch,
  exact object-metadata primary PG, bucket, and key. Operations may select an
  upload ID within that fixed object subject but cannot replace routing or
  placement authority.
- embedded construction validates exact object placement and an open PG before
  storage access. Unix construction rejects an epoch differing from the
  installed client before transport and owns the wire object subject. Common
  Unix loading preserves operation-specific decode and response-validation
  diagnostics while validating every returned upload against the route and
  requested upload ID.
- cluster UploadPart creation, conditional completion, lifecycle abort,
  ordinary lookup, and listing paths open the scoped route before reading an
  upload. Storage-node dispatch retains independent active admission-domain,
  route, placement, primary, and captured-deadline validation before opening
  the embedded route.
- embedded coverage rejects a configured crossed object PG before storage, and
  a no-listener Unix regression rejects a foreign epoch before RPC. The
  installed equivalent-state matrix exercises all four operations through
  correct and wrong PG routes, preserving exact `PayloadDecode` rejection even
  when the crossed PG contains an identical upload and parts.
- the transitional boundary checker now requires production multipart lookup
  reads to flow through the scoped route. Completion snapshot/parts access,
  completion and abort command construction, stream-session mutation, and
  payload-reclaim families remain open in Phase 3.
- all 2,526 storage tests pass. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,845-test workspace suite also
  pass.

One-hundred-and-fifteenth Phase 3 slice:

- multipart completion snapshots, completion preflight reads, and ListParts
  now share an `AuthorizedMultipartUploadMetadataRoute`. The route captures
  one active cluster epoch, exact object-metadata primary PG, and the complete
  authorized upload record; operation methods accept only part selection and
  pagination parameters, so callers cannot substitute another upload after
  route construction.
- embedded construction validates the upload's exact object placement and an
  open PG before storage access. Unix construction rejects an epoch differing
  from the installed client before transport, while every request is derived
  from the route-owned upload rather than caller-supplied object or upload
  identity.
- cluster completion and ListParts paths open the scoped route only after
  validating their admitted object subject and captured deadline. Storage-node
  dispatch independently validates active admission, route, placement,
  primary, deadline, and the authorized upload subject before opening its
  embedded route.
- embedded coverage rejects a crossed configured object PG before storage and
  a no-listener Unix regression rejects a foreign epoch before RPC. The
  installed equivalent-state Unix matrix exercises snapshot, preflight, and
  ListParts through correct and wrong PG routes where both PGs contain the
  same upload and part, requiring exact `PayloadDecode` rejection rather than
  an incidental missing-state failure.
- the transitional boundary checker now requires production authorized
  multipart snapshot, preflight, and part-list reads to use the scoped route.
  Completion and abort command construction, stream-session mutation, and
  payload-reclaim families remain open in Phase 3.
- all 2,528 storage tests pass. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full 7,847-test workspace suite also
  pass.

One-hundred-and-sixteenth Phase 3 slice:

- multipart completion stale-null-payload inspection and completion-command
  construction now share a `MultipartCompletionMutationMetadataRoute` bound
  to one active cluster epoch, exact object-metadata primary PG, bucket, and
  key. The command-build request no longer carries independently substitutable
  PG or epoch fields.
- the capability validates the complete request subject before storage or
  transport: selected and omitted parts and segments belong to the bound
  upload and object, terminal stream cleanup belongs to that upload, any stale
  payload source is the bound null live object, and the durable bucket-write
  proof exactly matches the completion operation, epoch, bucket, and target
  key. Embedded and Unix implementations share this validation.
- embedded construction validates exact object placement and an open PG
  before storage access. Unix construction rejects an epoch differing from
  the installed client before transport, derives both RPC object identity and
  response validation from route-owned authority, and retains independent
  storage-node admission, placement, primary, deadline, request-subject, and
  proof validation.
- cluster completion opens one scoped route for the initial stale-source read,
  command construction, and any stale-snapshot retry reload. Embedded and
  no-listener tests reject crossed PG and epoch authority; the installed Unix
  equivalent-state matrix requires `PayloadDecode` for the wrong PG even when
  both PGs contain identical upload and part state. Crossed operation, target,
  epoch, and request subjects fail before RPC.
- the transitional boundary checker now requires production completion
  stale-source reads and command builds to flow through the scoped route.
  Abort command construction, stream-session mutation, and payload-reclaim
  families remain open in Phase 3.
- all 2,535 storage tests pass. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full no-fail-fast 7,853-test workspace
  suite also pass.

One-hundred-and-seventeenth Phase 3 slice:

- multipart abort cleanup inspection and both ordinary and authorized abort
  command builders now share a `MultipartAbortMutationMetadataRoute` bound to
  one active cluster epoch, exact object-metadata primary PG, bucket, key, and
  upload ID. Build requests no longer carry independently substitutable PG,
  epoch, bucket, key, or upload identity.
- one shared subject validator binds the durable bucket-write proof to the
  exact abort operation, epoch, bucket, and target key. It also requires every
  part, streaming part segment, terminal stream session, and stream segment in
  the cleanup snapshot to belong to the routed upload; authorized aborts
  additionally require the authorized record and cleanup upload to match the
  routed upload exactly.
- embedded construction rejects incorrect topology placement and closed PGs
  before storage access. Unix construction rejects foreign epochs before
  transport and derives RPC and response-validation subjects from the route.
  Storage-node dispatch retains independent active admission, placement,
  primary, deadline, request-subject, and proof validation.
- the installed equivalent-state Unix matrix stores the same upload and part
  on both PGs, proves the correct route can inspect and build both abort forms,
  and requires `PayloadDecode` from the wrong PG. Crossed proof operation,
  target, epoch, and authorized upload ID fail before RPC; embedded and
  no-listener tests pin placement and epoch rejection at route construction.
- fanout and recovery now independently bind every persisted abort command's
  internal upload/cleanup subject and reservation proof to the command epoch,
  bucket, abort operation, key, and upload ID. Abandoning a malformed crossed
  or co-crossed subject envelope does not release the valid reservation owned
  by the unrelated operation. Durable recovery regressions require the upload
  and every node's exact reservation state to remain unchanged.
- PgStore requires the command's complete part, multipart-segment,
  stream-session, and stream-session-segment cleanup snapshot to equal durable
  state before the first mutation. A forged same-label segment referencing an
  unrelated shard set leaves both uploads, all part metadata, every shard, and
  the pending recovery envelope intact; unit coverage independently pins part
  and multipart-segment mismatch rejection.
- the transitional boundary checker now requires production abort cleanup
  reads and command builds to flow through the scoped route. Stream-session
  mutation and payload-reclaim families remain open in Phase 3.
- all 2,541 storage tests pass. Formatting, the storage boundary checker,
  workspace-wide strict Clippy, and the full no-fail-fast 7,862-test workspace
  suite also pass.

One-hundred-and-eighteenth Phase 3 slice:

- stream-upload creation idempotence matching and command construction now
  share a `StreamUploadCreationMetadataRoute` bound to one active cluster
  epoch, exact object-metadata primary PG, bucket, and key. Build requests no
  longer carry independently substitutable PG or epoch values; both embedded
  and Unix adapters derive routing solely from the scoped route.
- the route requires every creation request and authenticated expected command
  to match its exact object subject. Expected commands must also retain the
  in-progress creation state, initial segment allocator floor, target-specific
  canonical reservation operation, route epoch, and key target. Command builds
  independently bind PUT versus UploadPart preconditions and reservation
  proofs to the route and requested target.
- the storage-node active-primary route retains independent request,
  precondition, reservation, and response validation before delegating through
  the same scoped embedded interface. Existing installed-Unix wrong-PG and
  stale-epoch regressions continue to prove server-side rejection rather than
  relying only on the frontend type boundary.
- command fanout and recovery now independently require a stream-create
  reservation to use the canonical PUT or UploadPart operation selected by the
  command target, at the command epoch and exact bucket/key. Malformed
  same-epoch envelopes cannot publish a session or release an unrelated valid
  reservation; normal and recovery-route non-mutation regressions pin both
  boundaries.
- immutable cleanup authority is part of idempotent creation identity. Durable
  rows must match the applied creation command's cleanup deadline, and a
  drained contender is eligible as the caller's command only when its deadline
  also matches the current request. PUT and UploadPart regressions cover both
  the row/command and caller/contender comparisons.
- local regressions require a crossed PG, request subject, expected command,
  and reservation operation to fail without mutation. A Unix regression uses
  no listening server and requires future-epoch construction and crossed-key
  matching to fail before RPC. The transitional boundary checker now permits
  stream creation matching and command building only through the scoped route.
  The legacy test-support coordinator adapter now follows the production
  sequence as well: authorize the loaded upload, then let the target-specific
  creation route acquire its canonical reservation instead of transferring a
  generic bucket-write snapshot proof.
  Stream-session lookup, append, heartbeat/finalization mutation, and
  payload-reclaim families remain open in Phase 3.
- all 2,557 storage tests, all 1,263 server-core tests, and the full 7,877-test
  workspace suite pass, including the multipart model trace and installed-Unix
  creation regressions. Workspace-wide strict Clippy and the transitional
  storage boundary checker also pass.

One-hundred-and-nineteenth Phase 3 slice:

- active stream-session reads, staging-segment reads, append preparation, and
  PutObject reservation renewal now consume a
  `StreamUploadSessionMetadataRoute` bound to one active cluster epoch, exact
  object-metadata primary PG, bucket, key, and session ID. The raw variants
  taking independently substitutable PG/object/session arguments are removed
  from the aggregate object-mutation node-client interface.
- embedded and Unix adapters derive every wire or store request from the
  scoped route. Append preparation rejects a request carrying another session
  before allocator access, while its portable effect deadline is revalidated
  immediately before the durable segment-ID allocation. Reservation renewal
  similarly revalidates at the durable row update and requires the exact
  PutObject operation, bucket/key target, stable reservation identity, durable
  session proof, and PutObject target.
- reservation subject validation intentionally permits a durable proof from a
  retained older epoch: current active-route authority is carried separately
  by the effect fence, while both current and renewed proofs must retain one
  identical stable historical identity. The existing cross-epoch heartbeat
  regression pins successful bucket-PG renewal followed by the object-PG
  session-row update.
- cluster append publication, heartbeat refresh, retry inspection, and stream
  abort snapshot loading now retain one scoped session route rather than
  repeatedly rebuilding raw object arguments. Embedded coverage proves a
  crossed session cannot advance another session's allocator; installed-Unix
  coverage retains equivalent wrong-PG state and requires `PayloadDecode`,
  rejects future route epochs and crossed operations before RPC, and rejects
  PutObject renewal against an UploadPart session without mutation.
- stream finalization command construction, abort command construction, and
  payload-reclaim families remain open in Phase 3.
- all 2,560 storage tests and the full 7,880-test workspace suite pass.
  Workspace-wide strict Clippy and the transitional storage boundary checker
  also pass.

One-hundred-and-twentieth Phase 3 slice:

- PutObject and UploadPart stream-finalization snapshot loading and commit
  command construction now use distinct scoped metadata routes. The PUT route
  owns the exact object PG, epoch, bucket, key, and stream session; the part
  route additionally owns the upload ID and part number. Their build requests
  no longer carry independently substitutable route or target fields.
- embedded and Unix builders require the target-specific canonical reservation
  operation and derive every store or wire identity from the scoped route.
  The embedded UploadPart route also rejects a part record whose upload ID or
  part number differs from its captured target before allocating a command ID.
  The storage-node adapter independently validates the authenticated active
  route, snapshot, payload, and reservation before delegating through the same
  scoped embedded interface. Unix response validation is bound to the exact
  serialized request rather than a second independently supplied identity.
- finalization command construction now carries the admission's immutable
  effect fence to the storage node. PUT revalidates it immediately before both
  object write-sequence selection and command-ID allocation; UploadPart
  revalidates immediately before command-ID allocation. A regression invokes
  the embedded scoped builders after the conservative monotonic deadline but
  before the raw authority timestamp and proves neither path allocates a
  command ID.
- Unix requests carry only the portable authority/effective wall deadline; the
  storage process conservatively binds it to its own monotonic clock. Exact
  PUT and UploadPart codec round trips pin this field. Storage RPC frame
  encoding advances to version 12 with explicit version-11 rejection; the
  current-format inventory is updated accordingly.
- installed-Unix coverage proves correct finalization commands, wrong-PG
  rejection through the server, future-epoch rejection before RPC, crossed
  operation rejection, substituted PUT proof rejection, and malformed response
  rejection for both finalization targets. The transitional boundary checker
  now permits finalization access only through the scoped routes.
- central fanout/recovery validation now binds every `CommitStreamPart`
  reservation to the command epoch, bucket, key, and canonical UploadPart
  finalize operation. A live-session recovery regression rejects valid
  reservations crossed by operation, key, or bucket, then inserts the
  crossed-operation envelope and proves the session and reservation remain
  exact while no part is published.
- streamed PutObject has one authoritative payload size: the body-derived
  `total_size` passed to finalization. The independently supplied size fields
  have been removed from both `PreparedStreamPutCommit` and
  `StreamPutCommitInput`; segment-total validation and published object size
  therefore cannot disagree by construction. The version-12 RPC layout
  encodes only this single size.
- abort command construction and payload-reclaim families remain open in
  Phase 3.
- all 2,560 storage tests and the full 7,880-test workspace suite pass.
  Workspace-wide strict Clippy and the transitional storage boundary checker
  also pass.

One-hundred-and-twenty-first Phase 3 slice:

- ordinary and authorization-bound multipart abort command construction now
  carries the request admission's immutable effect fence through the scoped
  embedded and Unix routes. The storage node conservatively rebinds the
  portable wall deadline and the embedded builder revalidates immediately
  before allocating the metadata command ID, after confirming the durable
  cleanup snapshot.
- raw storage/lifecycle abort entry points explicitly use unbounded internal
  authority; the admitted server request path passes its captured fence. An
  expired-fence regression covers both abort builders after the conservative
  monotonic deadline but before the raw authority timestamp and proves that
  neither allocates a command ID.
- exact ordinary and authorized abort codec round trips pin the portable
  deadline. Storage RPC frame encoding advances to version 13 with explicit
  version-12 rejection, and the current-format inventory is updated.
- an installed Unix regression sends a fence that remains valid on the
  frontend but has expired when conservatively rebound to the storage host's
  unrelated monotonic clock. Both ordinary and authorization-bound abort
  command builds fail as stale-route requests, and the object PG command-log
  index remains unchanged.
- payload-reclaim command construction remains open in Phase 3.
- all 2,561 storage tests and the full 7,883-test workspace suite pass.
  Workspace-wide strict Clippy and the transitional storage boundary checker
  also pass.

One-hundred-and-twenty-second Phase 3 slice:

- payload-reclaim deletion command construction now goes through a scoped
  `ObjectPayloadReclaimMetadataRoute` bound to the route epoch, object
  PG, bucket, key, and generation. The builder accepts the complete durable
  reclaim root and claim record, derives the command's claim proof itself,
  and rejects crossed PG, epoch, object, generation, or reclaim-layout
  subjects before command-ID allocation.
- the embedded builder reloads and requires exact equality with both the
  durable reclaim root and singleton claim immediately before constructing
  the command. The Unix client validates the returned command against the
  same complete request subject, so an authenticated faulty peer cannot
  substitute another reclaim root or claim.
- the maintenance worker captures one immutable route-map effect fence after
  claim acquisition and before deleting payload shards. The same fence
  reaches both the embedded command-ID allocation boundary and the final
  pending-slot insertion boundary; a same-generation route renewal cannot
  extend an in-flight reclaim attempt's authority between command construction
  and its first durable publication.
- storage RPC adds the maintenance-only
  `ObjectPayloadReclaimCommandBuild` operation. Its request carries a
  host-portable deadline, full reclaim root, and full claim record; codecs
  reject inconsistent subjects and the storage node conservatively rebinds
  the deadline to its own monotonic clock. Its per-kind admission cap is the
  supported metadata-command byte limit plus the maximum routing, generation,
  full-claim, and deadline overhead, so a near-limit supported reclaim command
  cannot be rejected merely because the build request carries more context.
- installed Unix coverage proves a correct command build, rejects equivalent
  wrong-PG durable state as `PayloadDecode`, and rejects a deadline that is
  still valid on the frontend but expired after storage-host rebinding without
  advancing the PG command log. Codec coverage also rejects a crossed claim
  before transport, and framed admission accepts the exact reclaim-build cap
  while rejecting one byte over it. A deterministic local interleaving renews
  the raw same-epoch route, expires the captured fence after command build in
  the pre-install hook, and proves the pending slot and every replica log stay
  unchanged while the claim is released and the reclaim root remains
  retryable.
- scoped payload-reclaim discovery and claim acquisition remain open in
  Phase 3; this slice removes only the raw command-construction authority.
- all 2,565 storage tests and the full 7,887-test workspace suite pass.
  Workspace-wide strict Clippy and the transitional storage boundary checker
  also pass.

One-hundred-and-twenty-third Phase 3 slice:

- payload-reclaim root discovery, claim acquisition, and deletion-command
  construction now share one `ObjectPayloadReclaimMetadataRoute` bound to the
  route epoch, object PG, bucket, key, and generation. The maintenance worker
  opens that route once and can no longer independently supply routing fields
  to raw root-load or claim-acquisition methods.
- the raw per-object reclaim load and claim-acquisition methods are removed
  from `ObjectMutationMetadataNodeClient`. Embedded and Unix routes derive the
  complete wire/storage subject from their captured authority; bucket-delete
  diagnostics also use the scoped route rather than resampling a raw object
  mutation call.
- the storage-node adapter opens the same embedded route after active-primary
  validation. It reloads the routed reclaim before claim mutation and rejects
  a requested reclaim kind that does not match that durable root as
  `PayloadDecode`, rather than allowing a crossed layout to reach PgStore.
- installed Unix coverage retains equivalent wrong-PG canaries for root load,
  claim acquisition, and command construction. A separate crossed-kind
  regression proves the malformed request creates no singleton claim before a
  matching request succeeds through the same scoped route.
- claim acquisition now receives the same immutable effect fence captured
  before reclaim discovery and used by command construction and pending-slot
  installation. Embedded acquisition validates it at transaction entry and
  again immediately before each possible claim deletion or insertion, after
  all preceding claim/root reads. Unix acquisition carries a portable
  deadline and rebinds it to the storage host's monotonic clock. The storage
  node intersects that delegated fence with its own captured route fence, so
  neither side can extend the other's authority. Storage RPC frame encoding
  advances to version 14.
- a deterministic same-epoch-renewal regression expires the captured fence
  after the initial route check, transaction start, and durable-state reads,
  proving no claim row or command-log entry is created and the reclaim root
  remains retryable. A second transaction regression proves an expired claim
  is not deleted when authority expires after it is read. Unix regressions
  cover frontend deadline projection, codec transport, storage-host rebinding,
  both client-first and storage-route-first expiry, and rejection without
  claim mutation.
- all 2,576 storage tests and the full 7,898-test workspace suite pass.
  Workspace-wide strict Clippy and the transitional storage boundary checker
  also pass.

One-hundred-and-twenty-fourth Phase 3 slice:

- the remaining raw object-mutation scan methods are replaced by an
  `ObjectMutationScanMetadataRoute` bound to one route epoch and one installed
  object-metadata scan PG. Bucket-delete discovery/cleanup, durable reclaim
  adoption, diagnostic scans, and best-effort stream-session inspection open
  this route before issuing any page/root/claim read and cannot replace its PG
  on individual calls.
- per-object reclaim existence is no longer a raw mutation-client method. It
  is an operation on the existing `ObjectPayloadReclaimMetadataRoute`, whose
  authority already fixes the object PG, bucket, key, and generation.
- embedded and Unix scan routes validate every returned stream session,
  reclaim root, and reclaim claim against the scoped PG; bucket-specific
  results are also bound to the requested bucket. The storage-node adapter
  maps a misplaced embedded row to `PayloadDecode` before encoding a response,
  while the Unix route independently rejects an authenticated peer response
  carrying a foreign subject.
- installed Unix regressions retain positive primary canaries and equivalent
  wrong-PG durable rows for both stream pages, both reclaim-root forms, and
  reclaim claims. A malicious-response regression covers all five result
  shapes, and route construction rejects a foreign epoch before transport.
- the transitional cluster-source checker no longer bans these scan operation
  names: the removed raw trait methods make an unscoped production call a
  compiler error, while retaining a receiver-name exception would merely
  duplicate that stronger boundary textually.
- all 2,579 storage tests and the full 7,901-test workspace suite pass.
  Workspace-wide strict Clippy and the transitional storage boundary checker
  also pass.

One-hundred-and-twenty-fifth Phase 3 slice:

- bucket owner listing and batch execution-generation/fast-path reads now open
  a `BucketMetadataScanRoute` bound to one route epoch and one installed
  bucket-metadata PG. The three operations no longer accept a replacement PG,
  and the full and role-specific Unix client installers bind the immutable PG
  topology used by the route.
- embedded and Unix routes validate every requested or returned bucket against
  the scoped PG. Owner-list responses additionally retain their owner binding,
  reject duplicate bucket names, while batch responses must remain a subset of
  the requested buckets. An authenticated Unix peer cannot return a duplicate
  or foreign-PG bucket through any of the three result shapes.
- the storage-node RPC handlers now consume the frame's active admission
  permit and construct an active bucket-scan capability carrying the captured
  route fence. This closes the previous path that validated only the renewable
  raw route tuple; scans now reject a foreign admission domain, retained
  admission, and use after their immutable deadline even when the underlying
  same-epoch route has been renewed.
- deterministic coverage rejects foreign-epoch and foreign-PG requests before
  transport, all three malicious response shapes at the Unix client boundary,
  duplicate owner-list rows, a misplaced durable bucket as `PayloadDecode`
  through an installed Unix server, and all three operations after the
  storage-node capability deadline. The existing role-specific Unix routing
  test remains a positive canary for owner filtering and both batch reads.
- the transitional source checker no longer recognizes the removed raw scan
  method names. The scoped route is now the only compiler-visible production
  interface for these operations; direct `PgMetadataStore` bucket reads remain
  banned in cluster code.
- all 2,585 storage tests and the full 7,907-test workspace suite pass.
  Workspace-wide strict Clippy and the transitional storage boundary checker
  also pass.

One-hundred-and-twenty-sixth Phase 3 slice:

This historical slice's pair-shaped snapshot route was removed in the eighth
Phase 4 slice after confirming that it had no production request-path caller.

- exact bucket metadata operations now open a `BucketMetadataRoute` bound to
  one route epoch, installed bucket-metadata PG, and bucket. Bucket heads,
  snapshots, subresources, tags, create/barrier construction, and bucket
  control command construction/matching no longer accept replacement PG or
  bucket arguments after route construction.
- paired snapshot loading and delete-replica inspection retain separate,
  narrower `BucketMetadataRoutePair` and
  `BucketDeleteReplicaMetadataRoute` authorities. The exceptional replica
  inspection path therefore cannot acquire ordinary primary mutation or read
  authority, while paired loading cannot issue unrelated exact operations.
- embedded and Unix exact routes validate bucket placement before storage or
  transport access. Command builders additionally bind command IDs to the
  route epoch and PG, create configuration to the route bucket, and pending
  command subjects to the route bucket. Storage-node dispatch constructs its
  local exact route only after the existing RPC route validation and renders
  subject mismatches as `PayloadDecode` rather than an internal error.
- active cluster routes, cross-bucket snapshot loading, bucket deletion,
  lifecycle and authorization reads, multipart barriers, bucket-control
  mutation, and diagnostic/test adapters now consume the scoped interfaces.
  A no-server Unix regression proves crossed epoch, PG, and bucket builder
  subjects fail before transport, while the installed wrong-PG suite retains
  its positive exact-snapshot canary.
- the transitional checker no longer bans the removed raw operation names.
  The production trait now makes an unscoped exact call a compiler error;
  direct `SharedStorageNode` and `PgMetadataStore` bucket access remains
  checked explicitly.
- review corrections bind negative Unix responses as strictly as successful
  ones: exact head/snapshot `BucketNotFound` names must equal the route bucket,
  while pair responses must name either the source or destination. Foreign
  subjects fail as `PayloadDecode`; matching single and destination-pair
  canaries preserve the public `BucketNotFound` result.
- multipart-completion barrier construction now validates the reservation's
  epoch, bucket, canonical operation, and target context in the scoped Unix
  route before encoding or transport. A no-server regression crosses each
  field independently and proves that no RPC attempt starts.
- all 2,588 storage tests and the full 7,910-test workspace suite pass.
  Workspace-wide strict Clippy and the transitional storage boundary checker
  also pass.

One-hundred-and-twenty-seventh Phase 3 slice:

- exact bucket-write reservation, drain, DeleteBucket progress/finalizer, and
  lifecycle-claim operations now open a `BucketWriteReservationRoute` bound
  to one route epoch, installed bucket-metadata PG, and bucket. Once opened,
  the route no longer accepts replacement PG or bucket arguments. The parent
  `BucketWriteReservationNodeClient` retains only route construction and the
  four PG-wide worker-discovery scans, which will move to a separate scan
  capability rather than weakening the exact route.
- embedded and Unix route construction validates bucket placement through the
  installed topology. New reservation/drain/claim acquisition is also bound
  to the route epoch, while validation and heartbeat deliberately preserve a
  durable reservation or drain's older creation epoch across an active-route
  transition. The route therefore distinguishes current transport/publication
  authority from the stable identity of previously persisted work.
- active cluster writers, DeleteBucket progress and finalization, lifecycle
  workers, diagnostics, local-cluster recovery validation, and storage-node
  dispatch now consume the scoped interface. The role-specific Unix installer
  transfers the immutable PG topology into its client, matching the full and
  bucket-metadata installers.
- a no-server Unix regression proves crossed buckets and a crossed epoch for a
  new acquisition fail as `PayloadDecode` without starting transport. Existing
  installed-Unix tests retain positive acquisition, validation, heartbeat,
  drain, finalizer, lifecycle, current-route/old-durable-epoch, and equivalent
  wrong-PG non-mutation canaries. Malicious-response coverage also rejects
  duplicate reservation identities and a finalizer claim outside the route's
  bucket/PG subject.
- all 2,592 storage tests and all 7,914 workspace tests pass, together with
  workspace-wide strict Clippy, formatting, and the transitional storage
  boundary checker. The checker now relies on the compiler-scoped exact route
  for drain/lifecycle claim operations and retains its lexical guard only for
  the still-broad PG scan and retained-release interfaces.

One-hundred-and-twenty-eighth Phase 3 slice:

- the three remaining PG-wide DeleteBucket and lifecycle bucket-metadata
  worker-discovery operations now open a `BucketWriteReservationScanRoute`
  bound to one route epoch and one installed bucket-metadata PG. The parent
  `BucketWriteReservationNodeClient` exposes only exact-route and scan-route
  construction; neither returned interface accepts a replacement PG.
- the former lifecycle-bucket scan also combined aborting multipart uploads
  from the object-metadata partition. That mixed-role result is split rather
  than weakening the new bucket scan: `ObjectMutationScanMetadataRoute` now
  returns one bucket/key witness for each aborting-upload bucket in its scoped
  object PG, and the cluster merges the separately authorized scans.
- embedded and Unix scan routes validate every request marker and returned
  bucket or bucket/key witness against the applicable scoped PG. Unix response
  validation also rejects duplicate finalizer/lifecycle identities, duplicate
  bucket/witness rows, and non-increasing DeleteBucket-begin pages so an
  authenticated faulty peer cannot inject work outside the capability or
  destabilize pagination.
- storage-node dispatch consumes the frame's active route-admission permit and
  constructs the scoped scan capability before any of the four reads. The
  exact delete-attempt outcome and reservation-list handlers now likewise use
  the admitted exact-bucket capability rather than holding admission only by
  calling convention.
- the mixed lifecycle/aborting-upload response is split into separate bucket-
  and object-metadata RPCs, so storage RPC frame encoding advances to version
  15 and explicitly rejects the incompatible version-14 layout.
- deterministic no-server coverage rejects foreign epochs and foreign-PG
  pagination markers before transport. Malicious-peer coverage rejects all
  bucket-metadata response shapes and the object-metadata witness response,
  plus duplicate or unordered responses. Installed Unix and routed embedded
  tests remain positive canaries for drain, finalizer, lifecycle-root,
  lifecycle-bucket, and aborting-upload discovery.
- the transitional lifecycle scan/release source parser is removed. Exact,
  retained, and PG-wide scan authority are now separate compiler-visible
  interfaces; the remaining direct `PgMetadataStore` worker-access ban stays
  in place.
- all 2,597 storage tests and all 7,919 workspace tests pass, together with
  workspace-wide strict Clippy, formatting, and the transitional storage
  boundary checker.

One-hundred-and-twenty-ninth Phase 3 slice:

- retained stream-abort application and pending-slot finish no longer remain
  as operations directly callable on `RetainedMetadataCommandNodeClient`.
  The client now opens a non-cloneable
  `RetainedStreamUploadAbortMetadataRoute` borrowing both itself and one
  opaque `PreparedRetainedStreamUploadAbort`; the returned operations accept
  no PG, epoch, command, or stream subject arguments.
- embedded route construction requires that the prepared command's exact
  object-metadata PG is installed. Unix construction additionally binds the
  prepared command epoch to the retained node client before transport. RPC
  dispatch retains its independent retained-route, acting-set, primary, and
  prepared-command validation before using the embedded route.
- historical acting-set fanout and pending-slot finish now retain the prepared
  authority for the complete operation instead of repeatedly passing it as an
  operation argument. The installed Unix regression remains a positive canary
  for both PutObject and UploadPart cleanup and now proves that a crossed epoch
  is rejected before contacting a storage-node socket.
- all 2,602 storage tests and all 7,924 workspace tests pass, including the
  focused retained-abort constructor, Unix response-binding, and installed
  cleanup regressions. Formatting, the transitional storage boundary checker,
  and workspace-wide strict Clippy also pass.

One-hundred-and-thirtieth Phase 3 slice:

- certificate-bound recovery apply and abandonment on historical replicas are
  no longer direct methods accepting replaceable PG, epoch, recovery-source,
  abandoned-source, and command arguments. The recovery client now opens
  distinct `MetadataCommandRecoveryReplicaApplyRoute` and
  `MetadataCommandRecoveryReplicaAbandonRoute` capabilities borrowing that
  complete subject; each route is consumed by its one operation.
- primary recovery retains its separate non-nestable critical section because
  it owns pending-slot inspection and replacement. The replica route exposes
  neither operation, so callers cannot accidentally promote single-replica
  recovery authority into reporting-primary authority.
- one shared recovery-certificate validator now rejects unrelated payloads,
  non-increasing command indexes, same-payload commands carrying an abandoned
  source, and cleanup derivatives with missing, unrelated, or non-preceding
  abandoned sources. Embedded and Unix route construction apply it alongside
  the captured PG/epoch checks before mutation or transport.
- embedded cleanup routes additionally require the exact abandoned reissue to
  have a durable tombstone on the target replica before exposing either
  operation. The storage-node RPC adapter reuses the shared structural
  validator and independently proves current runtime-map recovery
  authorization, historical acting-set membership, the durable tombstone, and
  its bounded mutation fence.
- the cluster recovery fanout now holds the exact single-use route through
  each historical-replica effect. Embedded non-mutation and Unix no-server
  regressions cross the route epoch and each command subject independently,
  then exercise same-PG unrelated payloads, invalid index linkage, and
  missing/extra abandoned sources for both operation-specific routes. The
  embedded positive canary requires the durable tombstone, while the installed
  Unix historical-recovery regression remains the positive transport canary.
- all 2,604 storage tests and all 7,926 workspace tests pass, including the
  focused recovery family and installed Unix historical-recovery path.
  Formatting, the transitional storage boundary checker, and workspace-wide
  strict Clippy also pass.

One-hundred-and-thirty-first Phase 3 slice:

- retained EC reads no longer require deletion-exclusion leases from every
  shard owner before they can reconstruct. Dedicated retained-only broad and
  narrow acquisitions record exactly which available storage nodes accepted
  the subject-bound leases. A transport-unavailable node is omitted, while a
  reachable node that refuses the broad lease because reclamation has started
  still makes the snapshot attempt retry from fresh state. The existing all-or-
  release broad and shard-location lease APIs used by active and maintenance
  paths remain strict.
- `RetainedObjectPayloadRead` now owns the immutable leased-node set as well
  as the exact logical segment snapshot. Every retained shard read recomputes
  placement from that snapshot and skips locations outside the leased set, so
  connection failure cannot accidentally authorize reading an unleased shard.
  Narrow acquisition is restricted to nodes protected by the partial broad
  snapshot lease. The broad lease is released only after every nonempty
  segment proves that at least its EC `k` shard locations are covered by the
  narrow set; an insufficient subset fails closed and releases all broad and
  partially acquired narrow leases.
- deterministic embedded regressions prove reconstruction at exactly the
  available leased subset, reject a subset below `k`, verify failed handoff
  releases all leases, and make any attempted read from an unleased location
  fail the test. An installed-Unix regression makes one non-primary shard
  node's socket unavailable before snapshot loading and proves both partial
  broad acquisition and the narrow handoff reconstruct the retained body from
  the remaining nodes through the real connect-failure, read-handle, and
  retained-read paths.
- the multihost `storage-node-kill-fails-closed` UAT now proves a fresh GET,
  HEAD, and List remain available after a non-primary shard node is killed,
  while a new PUT still fails closed. All 2,626 storage tests and all 7,949
  workspace tests pass. Formatting, workspace-wide strict Clippy, and the
  transitional storage boundary checker also pass.

Phase 3 completion audit (2026-08-02):

- **Phase 3 is complete.** A fresh audit after the later storage, server, and
  test changes found no production path which can construct a role-specific PG
  ID from an arbitrary raw `PgId`. The role fields and production constructors
  remain private to the installed node-runtime boundary; the only raw
  constructors are `cfg(test)`, and wire requests continue to carry raw PG
  evidence rather than pre-authorized role types.
- every request-path stateful node-client family is either a factory for an
  exact active route or an explicitly separate retained-cleanup, peering, or
  recovery interface. The returned operation traits omit replacement PG,
  epoch, and subject arguments wherever authority has already been selected.
  The public request capabilities remain non-cloneable, have private fields,
  borrow their originating admission where appropriate, and preserve the
  immutable admitted deadline through the durable-effect boundary.
- the generic `MetadataCommandNodeClient` remains intentionally confined to
  storage-owned active publication and convergence code. It is not a request-
  path escape from the subject-bound routes. Replacing its raw pending-slot
  operations with compiler-classified publisher types is the explicit Phase 4
  objective below; the Phase 3 audit does not treat that deferred publisher
  typing as already complete.
- embedded adapters validate their captured role, route, subject, operation,
  and deadline before storage access. Authenticated Unix and TLS/TCP requests
  share the storage-node dispatch boundary, which decodes raw route evidence,
  validates it against the installed topology and authenticated node route,
  and only then constructs server-local role and operation capabilities.
  Retained cleanup and recovery use narrower interfaces and cannot acquire
  ordinary new-work publisher authority.
- the transitional boundary checker now guards the remaining API seams rather
  than granting authority through its allowlists. Its metadata-command
  publisher inventories remain only because Phase 4 has not yet replaced them
  with compiler-visible classifications.
- audit validation passed for the default production storage feature set, all
  storage targets/features, `server-core/test-utils`, all server-http
  targets/features, `./scripts/check-storage-cluster-boundaries`, strict
  workspace Clippy, and all 7,949 tests in the current workspace tree. The
  preceding completed slice separately passed all 2,626 storage tests and the
  multihost `storage-node-kill-fails-closed` UAT. No Phase 3 residual item
  remains open.

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

First Phase 4 slice (2026-08-02):

- the authoritative registry now generates one sealed zero-sized token for
  every publisher ID. Registry entries classified as `SnapshotSensitive`
  alone implement `SnapshotSensitiveMetadataCommandPublisher`, so changing a
  migrated publisher's class makes its production call site fail to compile
  instead of relying on a shell inventory update.
- the snapshot-sensitive trait has its own private seal, implemented only by
  the matching registry macro arm. Publisher tokens have private fields and
  every reference to their crate-visible macro constructor, including a
  function-item reference, is mechanically rejected outside
  `metadata_command_publisher!`. Another module therefore cannot grant a
  terminal or allocator publisher snapshot-sensitive authority.
- `metadata_command_publisher!` still supplies the canonical live-entry marker
  used to prove that every registered publisher has one production owner, but
  it now also returns that owner's exact typed token. The boundary checker
  requires a marker beside every migrated typed install call, while the Rust
  type system prevents any publisher outside the registered
  `SnapshotSensitive` class from using that path.
- the shared object-PG snapshot-sensitive installer now requires that token
  and returns the exhaustive, must-use `SnapshotSensitiveInstallOutcome` with
  `Installed` and `ContenderDrained` variants.
  Object metadata mutation and deletion, lifecycle expiry, multipart creation,
  and UploadPart stream-session creation now carry their registered token to
  the installer. Their established fresh-snapshot retry and durable-effect
  fence behavior is unchanged.
- the transitional publisher scanner treats the typed helper as the new
  compiler-enforced boundary and no longer count-checks its migrated call
  sites. Any production publisher that still calls a raw installer remains in
  the temporary inventory. The guide records the compiler-enforced boundary;
  lower-level snapshot-sensitive publishers and the four other classes remain
  for later Phase 4 slices.
- validation passed with the 18-test pending-install contention family, the
  registry/guide and typed-marker tests, the storage boundary checker,
  all-target/all-feature strict Clippy, and the full 7,948-test workspace suite.

Second Phase 4 slice (2026-08-02):

- all five `AllocatorCleanup` publishers now receive tokens implementing a
  class-specific sealed `AllocatorCleanupMetadataCommandPublisher` trait.
  Generation reservation, version reservation, both generation-release paths,
  and the multipart completion barrier can no longer select their pending-slot
  path with a token from another publisher class.
- object-PG fresh-command installation returns the exhaustive, must-use
  `AllocatorCleanupFreshInstallOutcome`, distinguishing installation, a drained
  occupied slot, and a handled log-index conflict. Prebuilt object- and
  bucket-PG commands return the exhaustive, must-use
  `AllocatorCleanupPendingInstallOutcome`. Each owner loop retains its existing
  retry budget, delay diagnostic, durable-effect fence, and exact-command
  convergence behavior.
- allocator/cleanup typed calls no longer participate in the temporary shell
  helper-count inventory. The shared scanner requires their authoritative
  registry marker, and its adversarial fixture proves an unmarked typed call is
  rejected. Unmigrated production publishers remain inventoried until their
  remaining classes receive typed APIs.
- validation passed the 11-test allocator contention matrix, the storage
  boundary checker, formatting, workspace-wide strict Clippy, and the full
  7,949-test workspace suite.

Third Phase 4 slice (2026-08-02):

- all five `TerminalSessionRetry` publishers now receive tokens implementing a
  class-specific sealed `TerminalSessionRetryMetadataCommandPublisher` trait.
  PutObject and UploadPart stream finalization, stream-session abort, and both
  multipart-abort paths cannot select this exceptional install behavior with a
  token from another publisher class.
- the typed installer returns the exhaustive, must-use
  `TerminalSessionRetryInstallOutcome`: installed, matching contender visible,
  unrelated contender visible, or contention without a visible command. It
  never drains on the install boundary. Matching terminal evidence therefore
  remains available for the owner loop or immediate exact-command convergence;
  unrelated work is reconsidered only after the owner loop reruns its matching
  predicate.
- UploadPart finalization retains the matching command envelope long enough to
  prove whether it owns the newly acquired bucket-write reservation before
  deciding whether caller cleanup may release that proof. Multipart aborts
  retain their existing exact matching and bucket-incarnation checks. Stream
  PUT finalization and general stream abort preserve matching commands until
  their established top-of-loop completion branches consume them.
- terminal-session typed calls no longer participate in the temporary shell
  helper-count inventory. The scanner requires their authoritative registry
  marker and its adversarial fixture rejects an unmarked typed call. Remaining
  raw publishers stay inventoried for later Phase 4 classes.
- validation passed the focused 20-test terminal matching/install-race family,
  the storage boundary checker, formatting, workspace-wide strict Clippy, and
  the full 7,950-test workspace suite.

Fourth Phase 4 slice (2026-08-02):

- the sole `MatchingOutcomeRetry` publisher, serialized multipart completion,
  now receives a token implementing the class-specific sealed
  `MatchingOutcomeRetryMetadataCommandPublisher` trait.
- its typed installer returns the exhaustive, must-use
  `MatchingOutcomeRetryInstallOutcome`: installed, matching contender visible,
  unrelated contender visible, or contention without a visible command. The
  installer never drains at this boundary, including after a command-log
  conflict, so an equivalent completion command remains available to supply
  the exact response row owned by the winning request.
- matching evidence includes the command envelope. Multipart completion uses
  it to distinguish a winning command that owns the caller's bucket-write
  proof from one using a different proof, releasing only caller-owned authority
  that did not enter the pending command. Unrelated contention returns to the
  outer owner loop, which reruns exact-request matching before generic drain.
- the migrated publisher no longer participates in the temporary shell helper
  inventory. The scanner requires its authoritative registry marker, and its
  adversarial fixture rejects an unmarked matching-outcome typed call.
- validation passed the full 29-test multipart-completion storage module, the
  storage boundary checker, formatting, workspace-wide strict Clippy, and the
  full 7,951-test workspace suite.

Fifth Phase 4 slice (2026-08-02):

- both `ApplyValidated` publishers now receive tokens implementing the
  class-specific sealed `ApplyValidatedMetadataCommandPublisher` trait.
  CreateBucket and stream-segment append cannot select their retry behavior
  with a token from another publisher class. With the final publisher class
  typed, the registry generator no longer has a catch-all class arm: adding a
  class now requires an explicit compiler-visible token definition.
- prebuilt CreateBucket installation returns the exhaustive, must-use
  `ApplyValidatedPendingInstallOutcome`, requiring the owner loop to
  distinguish installation from a drained contender and explicitly restart
  after contention.
- stream append uses the exhaustive, must-use
  `ApplyValidatedFreshInstallOutcome`. It distinguishes successful fresh-ID
  installation, a drained visible pending contender, and a handled log-index
  conflict. This preserves the existing retry-budget diagnostics and the rule
  that every collision after shard registration makes payload ownership
  ambiguous and therefore requires reference-checked cleanup.
- apply-validated typed calls no longer participate in the temporary shell
  helper-count inventory. The scanner requires their authoritative registry
  marker, and its adversarial fixture rejects an unmarked apply-validated
  typed call.
- validation passed the focused 24-test CreateBucket and stream-append
  contention/cleanup matrix, the storage boundary checker, formatting,
  workspace-wide strict Clippy, and the full parallel 7,952-test workspace
  suite.

Sixth Phase 4 slice (2026-08-02):

- the four bucket-control `SnapshotSensitive` publishers now carry their
  registry token through a typed control-slot installer. Bucket versioning,
  ACL, bucket properties, and bucket subresources can no longer call the raw
  control-slot retry helper from their production owner loops.
- the typed boundary returns the existing exhaustive, must-use
  `SnapshotSensitiveInstallOutcome`. Each publisher must distinguish a
  successful install from a drained contender and restart from its current
  bucket snapshot after contention; admitted publishers retain their immutable
  effect fence at the durable control-slot insertion boundary.
- the migrated calls leave the temporary shell helper-count inventory. The
  scanner still requires each authoritative registry marker, and a dedicated
  adversarial fixture proves an unmarked typed bucket-control call is rejected.
- validation passed the 31-test focused bucket-control contention and deadline
  matrix, the storage boundary checker, formatting, workspace-wide strict
  Clippy, and the full parallel 7,967-test workspace suite.

Seventh Phase 4 slice (2026-08-02):

- bucket-delete begin and acting-set finalization now carry their
  `SnapshotSensitive` registry tokens through a typed bucket-PG pending-slot
  installer. Neither production owner loop can call the raw bucket-PG retry
  helper directly.
- the typed boundary returns the exhaustive, must-use
  `SnapshotSensitiveInstallOutcome`. A drained contender restarts fresh
  MarkBucketDeleting or DeleteFinalizedBucket construction; finalization keeps
  its existing pre-install exact-command branch, so a matching pending delete
  is finished through its command-owned convergence path rather than drained.
- admitted delete begin retains its immutable effect fence at pending-slot
  insertion. Background finalization remains unfenced request work but retains
  its bounded work budget and current deleting-bucket identity checks.
- the two migrated calls leave the temporary helper-count inventory. The
  scanner continues to require their authoritative registry markers, and an
  adversarial fixture rejects an unmarked typed bucket-PG call.
- validation passed the 74-test focused bucket-delete
  contention/finalization matrix, the storage boundary checker, formatting,
  workspace-wide strict Clippy, and the full parallel 7,967-test workspace
  suite.

Eighth Phase 4 slice (2026-08-03):

- removed the dormant generic two-bucket snapshot stack: the coordinator
  `LoadedBucketPair` scaffold, raw and admitted cluster pair loaders,
  pair-shaped node-client route, and `BucketSnapshotPairLoad` RPC. None had a
  production S3 request-path caller.
- the pair operation was not an atomic cross-bucket snapshot; its local
  implementation performed two sequential reads and its dedicated RPC only
  batched the same-node active-primary case. Keeping that optimization would
  require a parallel Peering authorization implementation without adding a
  correctness property.
- CopyObject and UploadPartCopy continue to use their operation-specific
  source object-read and destination PutObject/multipart capabilities. If an
  oracle-backed operation later needs coordinated bucket authorization, it
  should receive a role-specific capability rather than restoring a generic
  pair of read authorities.
- removed pair-only tests and the obsolete raw-pair boundary-check inventory;
  existing single-bucket active and Peering route tests retain the underlying
  read-authority coverage.
- validation passed the storage RPC wire-kind authorization inventory, the
  storage boundary checker, formatting, workspace compilation, workspace-wide
  strict Clippy, and the full parallel 7,970-test workspace suite.

Ninth Phase 4 slice (2026-08-03):

- migrated the four remaining production publishers that directly selected a
  raw object-PG pending-slot installer. Direct PUT commit, low-level PutObject
  stream-session creation, admitted PutObject stream-session creation, and
  payload reclaim now carry their registered `SnapshotSensitive` token through
  `install_snapshot_sensitive_metadata_command_or_drain` and exhaustively
  distinguish installation from a drained contender.
- direct PUT retains its payload-ownership transition only after the typed
  outcome proves command installation. Both stream-create paths release their
  generation reservation and restart from fresh state after a drained
  contender. Payload reclaim preserves its durable claim and retries command
  construction after contention.
- the shared snapshot-sensitive installer drains exactly the contender it
  observed and then returns `ContenderDrained`; it does not loop until the PG
  slot is empty. This preserves each owner loop's route-authority and work-
  budget checkpoints under sustained same-PG publication. A deterministic
  regression inserts a second contender immediately after the first drain and
  requires it to remain pending when the typed install call returns.
- removed the now-unused raw install-and-drain wrapper. The boundary checker no
  longer has a production publisher allowlist: any raw publisher call is a
  failure, while the authoritative registry marker and class-specific token
  checks remain exhaustive.
- the separate `MetadataCommandLogConflict` convergence inventory remains for
  the next Phase 4 slice; its counts now reflect that the typed snapshot
  installer owns the common pending-install conflict branch.
- validation passed the seven focused pending-install contention, reservation,
  and reclaim regressions, the storage boundary checker, formatting, workspace
  compilation, workspace-wide strict Clippy, and the full parallel 7,973-test
  workspace suite.

Tenth Phase 4 slice (2026-08-03):

- every registry token now implements one common sealed
  `MetadataCommandPublisher` trait in addition to its single class-specific
  trait. Generic single-contender and idempotence-collecting object-PG drains
  require that common token, so publisher-owned progress cannot omit its
  authoritative registry identity.
- every pre-publish `MetadataCommandLogConflict` branch now drains at most one
  contender before returning to the owner loop's route-authority, work-budget,
  and fresh-snapshot checks. The separate collecting drain remains for
  preflights that must inspect applied command results to recognize an
  idempotent stream or multipart creation.
- post-acceptance convergence remains a storage-engine operation. Reporting a
  pending command as the current request's result still requires an
  `ExactPendingObjectMetadataCommand` constructed only after exact request
  matching, while generic recovery drains cannot manufacture that proof.
- the publisher scanner now rejects a registered publisher which calls an
  unclassified recovery/inspection drain, and its adversarial fixture covers
  both a missing token marker and a marked raw-drain bypass. This stable symbol
  prohibition replaces the former occurrence-count inventory without
  reintroducing a hand-maintained publisher list.
- removed the final `MetadataCommandLogConflict` function/count allowlist and
  updated the command-stream guide to describe the compiler-visible publisher,
  conflict-drain, and exact-command convergence boundaries. The Phase 4 audit
  confirms all production publishers have one Rust classification and no raw
  pending installer or raw publisher drain remains reachable from a registered
  owner path; Phase 4 is complete.
- validation passed the six focused publisher/conflict/adversarial regressions,
  the storage boundary checker, formatting, workspace-wide strict Clippy, and
  the full parallel workspace suite (7,978 tests).

Eleventh Phase 4 correction (2026-08-03):

- corrected the remaining publisher loops which could drain successive
  contenders without returning to their route and request-work boundaries.
  Stream append and stream abort now drain exactly one observed command, apply
  contention backoff, and re-enter their bounded owner loop. The same audit
  corrected multipart abort and streamed PUT/UploadPart finalization.
- removed the production unclassified all-command drain. Direct-PUT cleanup
  now delegates directly to the separately bounded generation-release
  publisher instead of hiding recovery in an unmarked best-effort wrapper.
- storage-owned multipart lookup, lifecycle inspection, and append preparation
  now use an opaque, non-copy recovery-drain authority borrowing one shared
  finite work budget. Publisher idempotence collection similarly retains its
  publisher token and one shared budget across the complete preflight.
- strengthened the publisher scanner so an unclassified drain is rejected
  even from an unmarked wrapper; the adversarial fixture pins both marked and
  unmarked bypass attempts. Deterministic stream append and abort regressions
  inject a second contender after the first drain and prove it remains pending
  when the owner returns to its exhausted budget boundary.
- closed the lower-level bypass as well: the primitive drain now requires a
  non-optional typed per-invocation authority derived from either the
  publisher token or the opaque recovery authority. Historical recovery
  wrappers create an explicitly bounded recovery authority rather than asking
  the primitive to manufacture a default budget. Authority fields are private
  to a child module, preventing struct-literal forgery, and the checker now
  inventories every use of the live constructor, recovery conversion,
  primitive, and recovery wrappers. Adversarial fixtures pin unmarked live
  primitive/construction/wrapper calls and constructor function-item aliases.
- removed the separately callable recovery-finishing helpers. The joined
  leader guard, authority budget, and recovery mutation now share one lexical
  scope inside the authority-gated drain primitive. The audited live-symbol
  inventory also covers construction of the lower recovery execution route
  and invocation of its bucket-PG finisher; fixtures reject both those bypass
  forms and the retired finishing-helper spelling.
- closed the remaining lower-mutation escape hatch. Joining recovery now
  produces an opaque leader capability which owns the actual leader guard;
  historical apply, reissued apply, abandonment, reissue, and pending-slot
  removal require a proof borrowed from that live capability. The recovery
  execution route carries the same unforgeable proof. Its fields cannot be
  constructed outside the authority child module, and its lifetime prevents
  the proof surviving the guard. The boundary checker now inventories every
  lower recovery mutator, leader construction, and execution-route struct
  literal; adversarial fixtures pin each formerly expressible bypass.
- bound the opaque recovery-leader proof to the joined guard's exact PG,
  log-index, and command-checksum subject. Every lower recovery mutation now
  rejects a proof for another command before node access. Ordinary reissue and
  the sanctioned abandoned-command cleanup follow-up derive a new proof bound
  to the exact replacement only after validating the same-epoch, increasing-log
  chain and allowed payload relationship; generic proof substitution is no
  longer possible. An adversarial acting-set regression uses a live leader for
  command A against command B and pins zero applied nodes and no durable
  mutation, while the terminal cleanup regression pins the valid derivative.
- corrected cleanup-derivative reissue without weakening its certificate.
  Once a proof has an abandoned-command predecessor, every lower mutation and
  later same-payload reissue must carry that exact source; callers cannot omit
  or substitute it. The deterministic regression now covers abandoned stream
  creation, certified generation-release derivation, a consumed cleanup log
  index, same-payload cleanup reissue, and terminal convergence, and separately
  asserts that dropping predecessor context fails before node access.
- validation passed the focused contention regressions, the storage boundary
  checker, formatting, workspace-wide strict Clippy, and the full parallel
  workspace suite (7,981 tests).

### Phase 5 — isolate test support

Audit update (2026-08-03): this phase is materially smaller in architectural
scope than when the plan was written. The containment work tracked by
`storage-upgrade-versioning-plan.md` moved storage-node protocols, physical
payload representations, maintenance workers, claims, reclaim/finalization
state, and most impossible-state fixtures into `storage`. The ordinary
normal/build dependency graphs for `argmin-s3`, `server-core`, and
`server-http` do not enable `storage/test-hooks` or
`server-core/test-utils`. Phase 5 must preserve that property, but it does not
need to repeat the completed production-representation containment work.

The remaining problem is test-surface ownership rather than a general crate
split. A separate `storage-test-support` crate is not the default design: it
could reach owner-private state only by making more storage internals public.
Prefer one explicit feature-gated `storage::test_support` module which can use
crate-private implementation details and expose only semantic scenarios,
logical observations, and deterministic fault guards. A separate crate is
appropriate only for composition helpers implemented entirely through the
normal public logical API.

The namespace is a migration boundary, not an automatic certification of each
item placed in it. Until slice 3 is complete, the remaining physical
observations and mutation helpers must be treated as explicit transitional
exceptions and inventoried here. Moving a symbol under `test_support` does not
make raw PG IDs, shard identities, EC layouts, durable rows, or generic
mutation methods acceptable final APIs.

The audit found four remaining classes of work:

1. **Completed — raw or semantically different test execution paths.** The
   coordinator's raw stream segment/finalization adapters and storage-node-only
   begin-stream adapters were removed. Cross-crate streamed PUT and UploadPart
   helpers now use admitted production capabilities, including retained cleanup
   authority captured before the first durable mutation. The unbounded storage
   entry points are test-hook gated and remain only for storage-owner tests
   which explicitly target the lower storage boundary. Cross-crate behavioral
   tests must continue to use the admitted production operation; a test helper
   must never depend on weaker route, deadline, or subject validation than
   production.

2. **Substantially completed — owner-local impossible-state fixtures.** Logical
   committed-object segment layout, storage identity, placement distribution,
   and exact payload presence/reclamation assertions now use an opaque
   storage-owned payload snapshot. Object-payload corruption and repair tests
   now use that snapshot through storage-owned fault scenarios and logical
   repair observations. The reclaim-queue state-machine and ordering tests are
   now storage-owner-local; the remaining coordinator lease-drop regressions
   observe only logical queue depth and opaque reclaim-root presence.

   The final audit found older residual cases which predate those migrations,
   so this item is not yet complete. The stream append/abort cleanup physical
   assertions are now storage-owner-local; retained coordinator cases assert
   only the selected error, cleanup trace, or opaque staged-payload outcome.
   UploadPartCopy source loss now uses a storage-owned whole-segment-loss
   scenario rather than caller-reconstructed shard paths. The direct-PUT
   pre-storage failure path now serializes request-owned metadata before
   entering the reserved bucket-write snapshot operation; a deterministic
   snapshot-load hook has a successful-PUT canary and remains untouched by the
   rejected request. The reclaim cleanup/retry physical invariant is
   storage-owner-local. Backfill worker tests are now storage-owner-local and
   use the private work-item, route, shard-acknowledgement, health, and
   placed-file model directly. Retained-placement and shard-selection tests
   still reconstruct physical routes or shard identities outside `storage`;
   those remaining physical invariants must move to `storage`, while any
   retained coordinator test should invoke an opaque owner scenario and
   assert only the coordinator-visible error, trace, or cleanup outcome.

   Tests whose assertion is about storage corruption, recovery, physical
   layout, claims, or queue invariants belong in `storage`. Where an
   S3/coordinator response to a storage failure genuinely requires a
   cross-crate test, expose an opaque scenario-level fault or logical
   observation rather than physical PG, shard, row, claim, or command records.
   This work is the test-fixture portion of pending item 14 in
   `storage-upgrade-versioning-plan.md`; that plan retains ownership of its
   separate debug-PG containment work.

3. **In progress — consolidated dev-only support.** Move retained cross-crate
   test DTOs, observations, and hook installers out of the `storage` crate root
   and off production types where practical, into `storage::test_support`.
   Classify every exported item as one of:

   - a logical read-only observation;
   - an opaque semantic fixture/scenario;
   - a deterministic scheduling, contention, expiry, or failure guard; or
   - a test-runtime lifecycle operation such as wake, drain, or cleanliness
     verification.

   Raw record constructors, physical mutation methods, generic storage-node
   access, and owner-private format values are not acceptable cross-crate
   categories. Keep process-level opaque Raft/control-plane test clients where
   the test necessarily spans a process boundary. The AWS-facing `s3-tests`
   harness and its S3/STS wrapper suites must not enable or name storage test
   hooks; local physical-invariant tests belong with the storage owner rather
   than behind an S3 harness abstraction.

4. **Feature-graph enforcement complete; feature cleanup remains.** The stable
   CI/boundary check obtains the complete workspace-member set from Cargo
   metadata and examines every member's normal/build feature graph. The
   classification is fail closed: maintain one explicit, reviewed allowlist of
   test-only packages which may enable test support, reject an unclassified
   workspace member, and reject a stale allowlist entry. The initial
   normal/build allowlist is empty:
   even non-published AWS-facing harness packages must not inherit private
   storage support. Adding an exception requires documenting why it is
   test-only and why it needs storage-private support. Every non-allowlisted
   workspace member must prove that neither
   `storage/test-hooks` nor `server-core/test-utils` is enabled. Dev-dependency
   edges may enable the features while compiling a package's tests, but do not
   exempt that package's normal/build graph from the check.
   Review `server-core/test-utils` after the raw adapters are removed and stop
   forwarding `storage/test-hooks` if its remaining cross-crate helpers no
   longer require storage-private support. Keep all-feature compilation for
   validating the test surface, but do not confuse that deliberately enabled
   graph with a production dependency graph.

Implementation slices:

1. Inventory every externally consumed storage test-support symbol by owner,
   consumer, category above, and whether it changes durable state. Record the
   clean normal/build feature-graph baseline and the exhaustive workspace
   package classification, with an empty hook-enabled allowlist.
2. Remove the raw/different-path coordinator adapters and make behavioral test
   helpers use admitted production capabilities.
3. Relocate impossible-state tests and replace necessary cross-crate raw
   mutations with owner-defined opaque scenarios; consolidate the retained
   surface under `storage::test_support`.
4. Minimize downstream test feature forwarding, add the production feature-
   graph check, update the upgrade/versioning plan's overlapping item, and
   retire only the textual hook checks made structurally redundant by this
   work.

Implementation update (2026-08-03):

- added a fail-closed Cargo feature-graph inventory to the storage boundary
  checker. It derives every workspace member from Cargo metadata, checks each
  normal/build graph's resolved package feature sets (including features on
  the selected root package), rejects stale or publishable exceptions, and
  currently has an empty hook-enabled allowlist. A miniature Cargo workspace
  independently pins extraction of both forbidden features when they are
  enabled through root defaults;
- removed `storage/test-hooks` from the AWS-facing `s3-tests` harness. Its two
  unused reclaim/scavenger physical-invariant methods were deleted rather than
  hidden behind another S3-facing abstraction, so `s3-http-tests`,
  `s3-local-tests`, and `sts-tests` no longer inherit the feature either;
- moved the coordinator's streamed PUT and UploadPart test helpers onto one
  captured `StorageClusterRouteAdmission` spanning session creation, segment
  append, and finalization, with the same retained cleanup authority captured
  before the first durable mutation. The raw segment/finalization route
  implementations and storage-node-specific test adapters were removed, and
  the underlying unbounded storage entry points are now test-hook gated;
- moved active test aborts onto the admitted PutObject/UploadPart capability.
  Expiry regressions now use the retained cleanup capability used by
  production, rather than directing cleanup through an arbitrary raw
  `StorageCluster`; and
- rewrote the UploadPart runtime-map regressions to prove that admitted
  creation/finalization holds publication until completion, instead of
  pinning an independently captured raw cluster pointer or synchronously
  publishing while the admitted operation is paused; and
- introduced the owner-scoped `storage::test_support` namespace and moved the
  existing cross-crate logical projections, reclaim/delete fixtures,
  deterministic metadata/bucket hook types, and reclaim-worker lifecycle
  controls off the storage crate root. Storage keeps only crate-private aliases
  needed by its implementation, while downstream tests must explicitly name
  the test-support boundary. The concrete `cluster` module is now private;
  production capabilities remain available only through the curated storage
  façade, and cluster-local test DTOs/guards cannot escape accidentally;
- replaced the cross-crate segmented and multipart reclaim-record constructors
  with storage-owned scenario methods. Callers can select only the logical
  bucket, key, generation, and ordering timestamp; storage derives the hash,
  data-PG placement, segment/part shape, and EC geometry. Reclaim observation
  exposes presence only, rather than returning the physical durable record;
- replaced cross-crate segment-layout replacement, multipart-part checksum-row
  mutation, and multipart-part deletion with storage-owned fault scenarios.
  Coordinator tests now select only a logical object/version/part and assert
  the resulting coordinator error; storage owns the unknown-PG, checksum
  mismatch, and incomplete-manifest representation. The test-only PG topology
  previously carried by `ReadRuntime` solely to construct reclaim placement
  was removed;
- replaced the generic upload-state mutation with explicit storage-owned
  aborting/completing scenarios, and replaced caller-authored lifecycle claim
  generation, owner, and deadline fields with one stale-incarnation claim
  scenario. Cross-crate tests still exercise the coordinator recovery and
  response behavior without constructing durable storage records;
- replaced the cross-crate stream-session timestamp mutation with an opaque
  stale-session scenario. Storage now owns the durable timestamp used to stage
  that state; only storage-owner tests which explicitly exercise timestamp
  handling retain access to the lower mutation primitive;
- replaced the exported committed multipart-part record with a logical ordered
  part-number observation. Physical placement is a storage-owned invariant and
  is no longer reconstructed or asserted by coordinator tests;
- replaced exported in-progress multipart part and segment records with a
  logical generation/size observation and an opaque storage-owned payload
  snapshot. Coordinator cleanup tests can retain exact pre-mutation evidence
  and ask storage whether it is wholly present or absent. Storage verifies both
  the acknowledgement row at the captured historical data-PG primary and the
  physical file at each captured historical shard placement, while callers
  cannot inspect PG IDs, shard hashes, EC layout, placement epochs, or payload
  generations; and
- made bucket-delete finalization roots opaque to downstream crates. Storage
  now selects the durable bucket incarnation for current and newly deleting
  buckets, admits the deliberately missing-bucket scenario only after proving
  acting-set-wide absence and uses its reserved non-production incarnation,
  and permits queue replay only with a root previously issued by storage; and
- replaced committed-object layout and ordinary payload-presence assertions
  with an opaque storage-owned payload snapshot. Downstream tests see only
  logical segment index, size, and whether the mandatory stored CRC64 value is
  nonzero; storage
  verifies generation-derived versus transient direct-PUT identity, data-PG
  selection, distinct shard-node placement, and both durable shard rows and
  files for exact pre-mutation evidence. The raw record seam has been renamed
  explicitly as transitional and is retained only by the not-yet-migrated
  retained-placement, reclaim, and shard-selection tests; and
- replaced HTTP and coordinator streaming-cleanup tests' raw staged-segment
  records and caller-reconstructed shard paths with an opaque storage-owned
  stream-upload payload snapshot. Downstream tests can observe only segment
  count, compare two snapshots for unchanged staged payload, and ask storage
  to prove that the captured payload is wholly present before cleanup and
  wholly absent afterward; storage checks both the durable shard
  acknowledgement row and physical file at every captured historical
  placement. The duplicate-append race now observes only the winning logical
  segment and final S3-visible bytes; the durable stream-generation allocator
  remains pinned by its storage-owner test rather than being reconstructed by
  the coordinator test. The raw staged-segment loader is now crate-private
  behind the opaque capture operation; and
- replaced cross-crate object-payload shard mutation and repair-queue records
  with storage-owned snapshot scenarios. Coordinator GET, range GET,
  CopyObject, UploadPartCopy, admitted-route expiry, and repair-worker tests now
  select only logical segment/shard ordinals. Storage owns exact historical
  placement, missing-file and corruption injection, durable-ack comparison,
  opaque before/after file-state evidence, repair-target matching, and wake-hint
  consumption. Payload capture reads segments and encryption metadata under one
  object-PG lock, including a replacement-interleaving regression, while wake
  selection atomically removes only work matching the supplied snapshot and is
  pinned with two queued payloads. Explicit managed-encryption repair coverage
  verifies that logical and stored segment sizes remain distinct. The raw
  repair DTOs and queue accessors were removed; and
- moved the reclaim queue's four state-machine property tests and four
  deterministic ordering/finalization regressions into storage-owner tests.
  Final payload-lease release and reclaim scheduling are now one storage-owned
  production operation shared by retained reads and the coordinator wrapper.
  The coordinator retains only logical lease-drop assertions using queue depth
  and opaque reclaim-root presence; the raw reclaim-work DTO and cross-crate
  dequeue, finish, wake, and outcome adapters were removed. The security
  suite's stateful and all groups now select the storage-owner module, and the
  15 historical Proptest regressions moved with their property tests so the
  source-relative replay corpus remains active; and
- moved stream append/abort physical cleanup invariants into storage-owner
  tests. Storage now pins complete cleanup after a post-prepare session abort,
  placed-file deletion failure leaving only orphan files, and acknowledgement
  deletion failure leaving only durable rows. Coordinator tests retain the
  operation error and typed cleanup-trace contracts, while the concurrent
  append/abort test uses opaque staged-payload evidence. UploadPartCopy source
  failure now invokes a storage-owned whole-segment-loss scenario and retains
  only its coordinator-visible failure and destination-session cleanup checks;
  and
- completed the direct-PUT/reclaim cleanup slice. Request-owned metadata is
  serialized before the reserved bucket-write snapshot operation. A
  deterministic snapshot-load hook proves that the rejected request does not
  enter that operation and a successful PUT canary proves the hook is live,
  followed by an ordinary GET.
  The reclaim placed-file deletion failure and retry test is now
  storage-owner-local, where it verifies the exact acknowledgement-row and
  shard-file state before and after convergence; the duplicate raw coordinator
  assertion and shard-set helper were removed; and
- moved the four shard-backfill worker regressions into storage-owner tests,
  including the installed-Unix remote-node case and the refreshed runtime-map
  case. The cross-crate backfill work-item/record DTOs and generic test methods
  were removed; storage tests use the private durable work item, route-health,
  acknowledgement, and queue APIs owned by the implementation; and
- completed the retained-read and range-read shard-selection migration. One
  storage-owned opaque scenario now selects the metadata/data PG split,
  validates the original payload placement provenance, starts and advances the
  six Unix storage nodes, chooses and corrupts a shard, retains the historical
  route, and publishes the moved-data-PG map. The coordinator regression
  controls only write, response creation, publication, and body consumption.
  Range-read lease assertions now ask storage whether the exact owner-selected
  shard set is leased and later fully released, without exposing node counts.
  Healthy/degraded EC and repair regressions use opaque
  first-data/first-parity/last-parity or logical-count fault evidence, so
  storage owns EC-index selection and callers observe only whether the selected
  fault remains, is queued, or is repaired. The indexed fault mutators are
  crate-private, and the boundary checker rejects cross-crate physical shard
  mutation, file, and repair-row APIs. The raw committed-segment accessor was
  removed, and raw shard construction, acknowledgement, and file-path helpers
  are storage-unit-test-only; and
- narrowed the lifecycle sweep suite to a storage-owned logical object
  observation. It exposes only the timestamps needed to choose the sweep
  deadline, while retaining the object generation and payload subject
  privately for an opaque deletion-exclusion lease and reclaim-root presence
  query. Current, versioned, suspended-null, and noncurrent expiration tests no
  longer read durable live-object generations or reclaim records directly; and
- introduced an opaque storage-issued object-payload reclaim subject for
  coordinator reclaim behavior. Runtime-map queue ownership, worker
  rediscovery, delete cleanup, claim release/failure, active-slot, lease, and
  durable-root assertions now pass this subject back to storage-owned helpers;
  they no longer extract or reconstruct the payload generation. The multipart
  state-machine harness now observes only a logical per-object reclaim-root
  count, and the raw bucket reclaim-root projection is removed. Synthetic
  segmented-root cases are storage-owned opaque scenarios, while the no-root
  lease case now captures an object written through the production PUT path;
  callers no longer choose generation `1`, and the raw `StorageCluster`
  generation/reclaim test methods are crate-private. The state-machine harness
  also observes only per-object session/upload counts and the selected upload's
  logical state; it no longer imports `StreamUploadRecord` or the broad
  multipart-upload projection. The reclaim count is an exact bucket/key query,
  rather than filtering the one-root-per-PG recovery scan; synthetic reclaim
  subjects select a generation above every durable object, reclaim, upload,
  and reservation reference on the acting set; and multipart state projection
  rejects a returned upload whose bucket/key differs from the requested
  subject; and
- removed cross-crate use of the raw test-only stream-session listing.
  Buffered PUT, multipart, and state-machine tests now use storage-owned
  logical counts for all sessions, one object, or one exact UploadPart target,
  and that raw cluster listing is crate-private. An owner-local positive matrix
  creates matching and crossed bucket, key, upload-ID, part-number, and
  PutObject sessions and pins every field in the per-object and exact-target
  predicates. The later expiry/cleanup migration also removed the raw
  best-effort record listing from downstream tests. Those tests now use exact
  logical session IDs, existence, counts, and cleanup deadlines, with an
  owner-local matrix pinning bucket, key, session identity, deadline presence,
  and crossed subjects. The raw best-effort scan remains private to storage's
  production sweeper and storage-owner tests; and
- removed the broad cross-crate multipart-upload projection. Coordinator and
  property tests now use separate storage-owned observations for existence,
  per-bucket/per-object counts and IDs, initiation time, owner selection,
  creation-metadata equality, and upload state. Completion generation
  continuity uses an opaque pre-completion subject which storage compares with
  the completed object after the upload row is removed. Metadata blobs,
  encryption state, generation IDs, and durable upload records no longer flow
  from storage into the coordinator harness. The raw get/list methods are
  crate-private, and the redundant in-progress test adapter and projection
  type were removed.

Final Phase 5 audit (2026-08-04):

- the fail-closed feature-graph check and the complete storage boundary checker
  pass. Every workspace member's normal/build graph is classified, the
  hook-enabled allowlist remains empty, and neither `storage/test-hooks` nor
  `server-core/test-utils` leaks into an ordinary package graph. The
  AWS-facing harnesses remain clean;
- Phase 5 cannot yet be marked complete. `server-core` still has residual raw
  storage test seams in `coordinator/core_tests.rs` and
  `coordinator/test_support.rs`. These seams
  expose or reconstruct raw PG IDs, route snapshots, generation identities,
  EC shapes, shard keys, physical shard paths, shard acknowledgement rows,
  durable object/upload records, and backfill work records;
- the stream append/abort cleanup, direct-PUT/reclaim cleanup, and
  UploadPartCopy source-loss migrations are complete;
- shard-backfill regressions have moved to `storage`, including refreshed-map,
  obsolete-source/payload, and installed-Unix execution. Retained-placement
  and shard-selection migration is also complete: physical route construction,
  historical-route publication, placement validation, Unix node lifecycle,
  EC-index selection, and shard-fault inspection are storage-owned, while the
  coordinator test retains only the meaningful response-lifetime ordering and
  body/result assertions;
- lifecycle sweep tests use the narrow storage-owned lifecycle observation for
  deadline timestamps, payload leases, and reclaim-root presence. The ordinary
  coordinator reclaim-worker and multipart cleanup paths now use an opaque
  object-scoped reclaim subject, including a storage-owned synthetic
  segmented-root scenario and a production-written no-root lease case. The
  multipart state-machine harness now uses exact logical session/upload counts
  and upload state. The other coordinator suites now use purpose-specific
  multipart-upload observations and an opaque completion-generation subject;
  the broad upload record projection is gone. Stream-session observation now
  uses exact logical counts, IDs, existence, target counts, and cleanup
  deadlines; no downstream test receives the production sweeper's raw record
  listing;
- the runtime-map validity, admission barriers, fault scheduling hooks, opaque
  payload snapshots, worker wake/drain controls, and process-level Raft test
  servers fit the allowed support categories. They should be consolidated
  under `storage::test_support`, but they should not be moved owner-local when
  the test's actual assertion is HTTP/coordinator behavior across the
  publication boundary; and
- `server-core/test-utils` no longer forwards `storage/test-hooks`. Its public
  `put_object`, `upload_part`, and requester helpers use production admitted
  paths. The obsolete raw-cluster stream-heartbeat helper was replaced by the
  admitted production route, and the stale-session sweep is now either an
  owner-local coordinator-unit helper or the storage-owned logical lifecycle
  operation used by HTTP tests. `server-core --features test-utils` now
  compiles without `storage/test-hooks`; `server-core` unit tests may continue
  to enable `storage/test-hooks` through their dev-dependency while the
  remaining migrations are performed;
- opaque committed-object, multipart-part, and staged-stream payload capture
  and presence/layout predicates are now exposed only through the curated
  `storage::test_support::StorageClusterPayloadTestSupport` trait. The
  physical `StorageCluster` implementations are crate-private, so downstream
  tests must explicitly import the owner namespace and cannot discover these
  operations on the ordinary production surface. The snapshot values remain
  opaque and expose only their existing logical projections; and
- captured route-validity/lease mutation and deterministic
  request-admission/publication barriers are now exposed only through the
  curated `StorageClusterRouteMapTestSupport` and
  `StorageClusterRouteHandleTestSupport` traits. Those feature-gated trait
  methods remain intentionally callable by downstream tests; the corresponding
  inherent helper methods on the production route types are crate-private,
  keeping these allowed test-runtime lifecycle controls explicit in
  `storage::test_support`;
- bucket-delete/reclaim worker depth observations and storage-owned lifecycle
  setup for stale claims, durable delete drains, terminal multipart states,
  and stale stream sessions are exposed only through
  `StorageClusterLifecycleTestSupport`. These feature-gated trait methods
  remain intentionally callable by downstream tests; their corresponding
  inherent `StorageCluster` helpers are crate-private, and no durable claim,
  queue, upload, or session record crosses the owner boundary;
- multipart authorization candidates, logical part observations, committed
  manifest part numbers, and owner-defined checksum/incomplete-manifest fault
  scenarios are exposed only through `StorageClusterMultipartTestSupport`.
  These feature-gated trait methods remain intentionally callable by
  downstream tests; their corresponding inherent `StorageCluster` helpers are
  crate-private, so the production cluster surface no longer exposes these
  test-only multipart entry points; and
- buffered/direct-PUT read-path tests no longer reload a raw `StoredObject` to
  recover its generation. The opaque payload snapshot retains that provenance
  inside storage, and the storage-owned transient-layout predicate consumes it
  directly; `coordinator/read_tests.rs` no longer uses the raw object-record
  test seam;
- multipart tests now use production `HeadObject` results for response-visible
  size and metadata, opaque payload evidence for generation/layout checks, and
  a narrow storage-owned SSE-C at-rest checksum observation. They no longer
  load raw current-object records. The feature-gated object-observation trait
  remains intentionally downstream-callable, while its corresponding inherent
  `StorageCluster` helper is crate-private.
- the authorization model no longer loads raw object records through a
  test-only `StorageCluster` adapter. It obtains its required immutable
  `StoredObject` input through the production admitted metadata-read route,
  while the direct GetObject authorization tests now enter the same admitted
  authorization path as production. The raw object-read route variant and its
  raw bucket/object snapshot loaders have been removed from `server-core`;
- `coordinator/test_topology.rs` no longer exposes PG IDs, generation
  reservations, durable object records, or reconstructed route snapshots.
  Storage now owns the semantic selection of same-PG, distinct-PG, and
  metadata/data cross-PG keys and sessions, plus the opaque transition which
  places one selected object's metadata PG into Peering. That transition is
  owned by the exact runtime-map handle: it derives the current cluster and
  local stores from the handle and conditionally publishes only if that
  generation remains current. Crossed publication domains and an intervening
  generation are storage-owner non-publication canaries. The coordinator tests
  retain only object keys and logical placement predicates. Storage-owner
  canaries also pin every key-selection relation and reject requests for more
  distinct placements than the topology contains;
- lifecycle and bucket-delete coordinator tests no longer load raw current or
  explicit-version object records. They assert visible namespace state through
  production `ListObjectVersions`, including requester authorization, while
  deadline selection uses the narrow lifecycle observation. The one test which
  must age a noncurrent version now invokes an owner-defined lifecycle scenario;
  its crate-private mutation atomically requires an already-noncurrent live
  version. Storage-owned regressions prove that current live versions and
  delete markers are rejected without changing durable metadata; and
- `coordinator/core_tests.rs` no longer loads raw object records. Live PUT and
  completed-multipart owner persistence is asserted through production
  `GetObjectAcl`, lifecycle conflict coverage retains the version ID returned
  by production PUT, and the response-invisible delete-marker owner invariant
  uses one delete-marker-specific logical storage-owned predicate. It rejects
  live versions even when their owner matches. Owner-local positive,
  crossed-version, and crossed-owner canaries independently pin its principal
  and canonical-ID comparisons without creating an alternate live-object
  inspection path; and
- bucket-delete and promoted-stream cleanup tests no longer load raw bucket
  records or object-generation reservation IDs. Storage issues an opaque
  bucket-delete-begin subject derived from the acquired durable drain and its
  fenced bucket incarnation, owns queued-root construction, and projects only
  logical bucket presence, delete progress,
  exactly-once marking, distinct-incarnation, reservation-existence, and
  execution-generation ordering predicates. Crossed-bucket/key/session and
  recreated-incarnation owner canaries pin those predicates. The raw
  `StorageCluster` bucket record, current-delete-begin, queued-begin,
  reservation-generation, delete-progress, and EC scratch-counter methods are
  crate-private; downstream access is through the curated lifecycle or payload
  test-support traits; and
- the last coordinator callers of direct committed-segment metadata mutation
  now use snapshot-bound storage-owned fault scenarios. Unknown data-PG and
  checksum-mismatch faults derive the exact bucket, key, version, and payload
  generation from opaque evidence. An immediate SQLite transaction validates
  that subject and changes only the selected segment column. Current numbered
  versions are supported, replaced null generations fail closed without
  modifying the current object, owner-local same-PG canaries pin crossed-key
  isolation and exact field mutation, and the former subject-argument mutation
  methods are removed and mechanically prohibited outside storage; and
- staged payload-write effect tests no longer receive shard locations, shard
  keys, or call physical file probes from `server-core`. Storage now returns
  opaque attempt evidence, exposes only the one-based attempt count needed for
  deterministic expiry injection, and verifies both durable acknowledgement
  rows and shard files are absent after cleanup. The evidence retains its exact
  originating cluster and exposes a parameterless absence check, so callers
  cannot substitute an empty same-topology cluster. An owner-local
  committed-write/crossed-cluster canary pins attempt ordering, cluster
  binding, and proves the absence predicate is not vacuous. The raw physical
  write hook and file probe are crate-private and the boundary checker rejects
  their cross-crate or public reintroduction; and
- request-deadline regressions no longer reconstruct local node paths, PG
  lists, EC shape, or route snapshots in `server-core` merely to open a second
  runtime over the same stores. They use a storage-owned process-local
  route-authority clone which shares the exact installed runtime, topology, and
  retained route history while binding the requested test validity. This makes
  duplicate-runtime SQLite access and foreign-root substitution structurally
  unavailable. An owner-local store/registry/retained-epoch canary pins those
  provenance guarantees; and
- runtime-map refresh, retained-route publication, primary movement, and
  deliberately stale-current-route fixtures no longer construct local node
  configs, PG IDs, route snapshots, acting sets, or epochs in `server-core`.
  They invoke storage-owned topology scenarios derived from the exact current
  runtime-map handle and process-local cluster. The scenarios preserve the
  process-local registry and all retained route history, conditionally publish
  against the captured generation, choose alternate primaries internally, and
  return only logical epochs or clusters. Same-epoch refreshes preserve the
  complete local route state, including a certified Peering metadata-read
  route, and Peering transitions retain every older historical epoch. The
  deliberately stale scenario mutates the map before its route authority is
  minted, so only the intended route/cluster-epoch mismatch is present.
  An owner-local canary recomputes the final map digest and requires it to
  match the minted authority, directly pinning that construction order. Other
  owner-local canaries pin complete Peering route preservation, retained
  history, changed-primary selection, rejection when no alternate exists, and
  fallible epoch overflow. The raw route-clone constructor is now
  crate-private; and
- coordinator listing, version-pagination, bucket-delete-frontier, and reclaim
  capacity tests no longer enumerate storage PG IDs or search for keys by a
  caller-supplied raw PG. Storage-owned topology support selects keys on the
  same metadata PG, on the same PG as an opaque reference key, or across
  distinct PGs and returns only the keys. The DCC-3 global-listing fixtures use
  a stronger scan-ordered selector, so their lexically smallest group is
  guaranteed to occupy the final metadata scan position. Owner-local canaries
  pin each placement relation and the exact sorted scan ordering. The obsolete
  `StorageCluster::test_pg_ids` surface has been removed entirely; and
- maintenance-sweeper status, route-following observations, deterministic
  reclaim shutdown, one-item shard repair, and one-pass stream-session cleanup
  are now available cross-crate only through explicit
  `storage::test_support` traits. The production sweeper types retain only
  crate-private inherent implementations, unused deterministic backfill
  execution remains owner-local, and the boundary checker scans every storage
  source module to reject public reintroduction of the raw inherent methods.
  The same source-root scan helper runs against production and a multi-module
  fixture tree, pinning discovery of a nested unrelated-module inherent impl
  while ignoring its crate-private control; and
- coordinator epoch-transition and metadata-command fault tests no longer
  obtain object/bucket PG IDs, primary node IDs, raw replica proofs, or the
  generic apply-context hook. `StorageClusterMetadataCommandTestSupport`
  returns an opaque object-scoped command-state snapshot with exact-position,
  bounded-advance, log-hash, and state-digest predicates, and installs
  bucket/object primary hooks whose placement and subject filtering remain
  inside storage. Storage also owns construction of deterministic command-log
  conflicts and stale-apply failures. Owner-local canaries compare crossed keys
  and crossed storage domains at equal command positions, pin exact command
  advancement and redacted evidence, and use a three-replica acting set to
  prove that subject-scoped hooks observe only the primary. The corresponding inherent raw
  proof, PG-selection, and apply-context methods are crate-private, and the
  boundary checker rejects cross-crate use or public reintroduction anywhere
  under the storage source tree.

The audited cross-crate support families are:

| Surface family | Principal consumers | Durable mutation | Final disposition |
| --- | --- | --- | --- |
| admitted `put_object`/`upload_part` test helpers | `server-http` | production operation | retained; unnecessary feature forwarding removed |
| opaque object, multipart-part, and stream payload snapshots | `server-core`, `server-http` | no | retain under `storage::test_support` |
| scheduling/deadline/publication hooks and worker lifecycle controls | `server-core`, `server-http`, `argmin-s3` | controlled fault/lifecycle action | retain under `storage::test_support` |
| raw object/version/upload/session/bucket observations | `server-core` | no | replace with narrow logical observations or owner-local assertions |
| raw PG IDs, metadata proofs, route snapshots, and route-map mutation | `server-core` | sometimes | move owner invariants to `storage`; retain only opaque topology/publication scenarios |
| shard keys, EC layouts, acknowledgement rows, shard paths/files, and physical segment records | `server-core` | yes and no | move to `storage`; expose only opaque fault/evidence scenarios for coordinator response tests |
| backfill work records and direct shard/ack construction | storage-owner tests | yes | completed; no cross-crate DTO remains |
| raw bucket-delete/reclaim roots and durable-state mutation | `server-core` | yes and no | use opaque storage-issued roots and logical progress/outcome observations |
| process-level authenticated Raft/control-plane test servers | `argmin-s3`, storage integration tests | controlled process lifecycle | retain as the sanctioned process-boundary exception |

Remaining implementation order after this audit:

1. **Completed:** stream cleanup, direct-PUT/reclaim cleanup, and UploadPartCopy
   source loss have owner-local physical assertions and only logical or opaque
   coordinator-facing coverage;
2. **Completed:** retained-placement and shard-selection physical invariants
   are storage-owned; the genuinely cross-crate runtime-map case uses one
   opaque topology scenario, and the backfill migration is complete;
3. **Completed:** multipart-upload observations are logical owner-defined
   values or opaque generation evidence; raw upload generation/record access
   has been removed from the coordinator harnesses;
4. **In progress:** consolidate the surviving guards, observations, and
   lifecycle controls under `storage::test_support` and remove obsolete
   inherent `StorageCluster` test methods. The `server-core/test-utils`
   forwarding of `storage/test-hooks` is removed; its externally consumed
   helpers now compile over the production storage feature set. The opaque
   object, multipart-part, and stream-payload snapshot family is consolidated
   behind one curated test-support trait and its inherent implementations are
   crate-private. Route-map validity/lease controls and request/publication
   barriers are likewise consolidated behind owner-namespaced test-support
   traits whose feature-gated methods remain downstream-callable, while their
   corresponding inherent helper methods are crate-private. Logical worker
   depth observations and storage-owned lifecycle setup are likewise
   consolidated behind `StorageClusterLifecycleTestSupport`, with the raw
   inherent helpers made crate-private. The logical multipart observation and
   semantic-fault family is consolidated behind
   `StorageClusterMultipartTestSupport`; its feature-gated trait methods remain
   downstream-callable while the corresponding inherent helpers are
   crate-private. Bucket-delete progress/current-incarnation scenarios,
   stream-reservation existence, and EC scratch reuse are likewise behind the
   lifecycle/payload traits. Committed-segment unknown-route and checksum
   faults are bound to opaque payload snapshots behind the payload trait.
   Staged shard-write effect tests use opaque attempt evidence and ordinal-only
   failure hooks. Same-store deadline fixtures use a storage-owned
   process-local route-authority clone rather than exporting local store paths,
   PG IDs, EC shape, or route snapshots, or opening a duplicate runtime.
   Runtime-map refresh/primary-move/stale-route fixtures likewise use the
   curated storage-owned topology interface. Listing and reclaim fixtures
   likewise request only semantic same/distinct or scan-ordered metadata-
   placement groups; raw PG enumeration has been removed. Maintenance worker
   observations and deterministic actions are likewise consolidated behind
   owner-namespaced test-support traits, with their inherent implementations
   crate-private. Metadata-command transition evidence and fault injection are
   likewise consolidated behind opaque state and subject-scoped hooks; raw
   PG IDs, replica proofs, primary IDs, and apply contexts no longer cross into
   `server-core`. The remaining bucket serialization and Direct PUT
   post-publication scheduling seams now likewise expose only opaque
   `storage::test_support` guards: coordinator tests can request a metadata
   serialization hold from a logical bucket or a subject-bound fixed
   post-publication error, but cannot name a bucket PG, node hook registry, or
   arbitrary storage callback. A crossed-key canary proves that the Direct PUT
   fault cannot contaminate another object on the same storage domain. The
   underlying guard types, callback aliases, hook registries, and inherent
   hook/lock methods are crate-private or module-private, and a crate-wide
   source-root check with a nested-module fixture rejects their public or
   cross-crate reintroduction. The CopyObject lease-maintenance regression
   likewise installs only an opaque pre-payload-write action; shard locations,
   keys, and injected storage errors remain owner-private. Bucket-finalization
   tests likewise create
   missing/current work, duplicate an opaque storage-issued root, or finalize
   deleting metadata only through lifecycle test-support semantics. The root's
   durable incarnation is debug-redacted, the raw queue/finalization methods
   are crate-private, and a matching crate-wide fixture check rejects their
   public or cross-crate reintroduction; and
5. rerun the public-export/feature audit and mark Phase 5 complete only when
   the remaining raw topology/support seams and their plan exceptions are
   gone.

Completion:

- every workspace package has a fail-closed classification, and normal/build
  dependency graphs cannot enable test support except for explicitly reviewed
  test-only packages on the narrow allowlist
- production-visible coordinator and storage APIs contain no raw path retained
  solely for tests
- cross-crate behavioral tests use the same admitted capability path as
  production unless they explicitly target a lower owner boundary
- impossible-state construction and assertions about storage physical layout,
  recovery internals, claims, and queues are owner-local; cross-crate tests may
  invoke an owner-defined opaque failure scenario when their assertion is the
  coordinator, HTTP, or S3 response to that scenario
- retained cross-crate support is feature-gated, owner-namespaced, and limited
  to logical observations, opaque semantic scenarios, deterministic fault
  guards, and test-runtime lifecycle operations
- tests retain the durable-state evidence necessary to prove invariants
  without importing owner-private records or physical routing values
- textual scans for public test hooks are retired only where module visibility,
  feature graphs, or typed test-support APIs enforce the same invariant

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

- embedded and enabled RPC-transport client parity tests for affected
  operations
- TCP parity once that transport is enabled, using the same post-auth dispatch
  adapter rather than a second capability-construction path
- runtime-map transition tests for active versus retained authority
- adversarial embedded and enabled-transport tests for expired, stale, reused,
  and subject-mismatched route capabilities
- composed authenticated RPC tests that independently cover a valid credential
  with the wrong role and a valid role with invalid route/subject evidence
- workflow-to-transport tests proving frontend and maintenance code receive
  distinct role-typed signing capabilities, with no generic client-side
  credential set or operation-driven signer selection
- deterministic contention/recovery tests for affected command publishers
- review of public exports and enabled Cargo features

Where a check is retired, include a regression demonstrating the replacement:

- a compiler-enforced inaccessible API or wrong-type call need not be tested by
  parsing compiler diagnostics, but the new boundary should be evident from
  module/crate visibility and reviewed as part of the change
- PG-role and route-capability constructors, embedded application-time
  revalidation, and server-side revalidation through the shared post-auth
  adapter on every enabled RPC transport require positive and adversarial tests
- typed publisher outcomes require deterministic tests for every outcome

## Risks

- A large crate split can create dependency cycles or move too many shared
  types into a protocol crate. Use module privacy first and split only when the
  boundary is clearer.
- A broad protocol/types crate can recreate the same weak boundary under a new
  name. Export request/response values, not raw engine handles or mutation
  helpers.
- Client-side route capabilities must not be trusted across any RPC transport.
  The shared server adapter, entered after the transport's required
  authentication, remains the authority and revalidates immediately before
  mutation on Unix and TCP.
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
3. cluster code reaches storage nodes only through embedded node-client or
   transport-neutral RPC client interfaces, with every enabled RPC transport
   entering the same capability-construction adapter after mandatory auth or
   the explicit standalone-local auth opt-out
4. authenticated workflows receive one role-typed signer capability and the
   transport retains that exact signer for response verification; credentials
   are never ambient authority selected by an RPC kind
5. PG role and route authority are represented by non-forgeable types with
   private trusted construction; route capabilities are request-scoped,
   non-cloneable, deadline-bound, and revalidated at durable effects
6. raw pending-command installation is inaccessible to operation publishers
7. publisher retry/convergence classes are explicit and exhaustively handled
   in Rust
8. test-only mutation APIs are absent from production dependency surfaces
9. the remaining boundary script contains only justified semantic checks that
   cannot reasonably be enforced by Rust or Cargo
