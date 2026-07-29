# Storage Boundary Compiler-Enforcement Plan

Status: active — Phases 0–2 complete; Phase 3 in progress

Related plans:

- [static-cluster-configuration-plan.md](static-cluster-configuration-plan.md)
- [control-plane-auth-identity-plan.md](control-plane-auth-identity-plan.md)
- [multihost-transition-plan.md](multihost-transition-plan.md)

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
   pre-authentication allocation budget. `combined` remains fail-closed pending
   equivalent workflow-specific credential composition;
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
| `BucketMetadataNodeClient` | bucket metadata | active bucket route; reads may later receive a read-only projection |
| `BucketWriteReservationNodeClient` | bucket metadata | active bucket route for acquisition, heartbeat, worker scans, and claim creation |
| `RetainedBucketWriteReservationNodeClient` | bucket metadata | opens a retained cleanup route bound to one bucket metadata PG and exact bucket; the returned interface releases exact reservation proofs, drains, and worker claims without accepting a replacement PG or bucket |
| `ObjectGenerationMetadataNodeClient`, `ObjectVersionMetadataNodeClient`, `DirectPutMetadataNodeClient` | object metadata | active object route, with completion-specific admission represented as a narrower operation class |
| `ObjectListingMetadataNodeClient` | object metadata scan | active object-metadata route for the scanned PG; listing fan-out constructs one capability per routed PG |
| `ObjectMutationMetadataNodeClient` | object metadata | active object route for object, multipart, stream-session, and payload-reclaim mutation |
| `RetainedObjectMutationMetadataNodeClient` | object metadata | retained subject-bound stream abort preparation and exact payload-reclaim claim release |
| `ObjectReadMetadataNodeClient` | object metadata | active object route plus the existing subject identity/read lease |
| `PlacedShardNodeClient`, `ShardAckNodeClient` | data | active data route for serving I/O, current placement cleanup, and repair/backfill work |
| `RetainedPlacedShardNodeClient`, `RetainedShardAckNodeClient` | data | retained exact-placement inspection and cleanup for historical shard bytes and acknowledgement rows |
| `ShardScavengerNodeClient` | data for shard rows/files and object-metadata reference scans | read-only inventory authority for the validated active or retained scan location |
| `ShardScavengerObservationNodeClient` | data PG for durable observation rows | opens an active primary-only route bound to one data PG; the returned interface records, lists, and resolves non-authoritative scavenger findings without accepting a replacement PG |
| `ShardReadHandleNodeClient` | data locations carried in the handle request | active/retained authority is inherited from each validated `ShardLocation`; the lease itself remains non-cloneable |
| `ObjectPayloadLeaseNodeClient` | object subject rather than a caller-supplied PG | active subject-bound lease acquisition, reclaim-begin, and observation; placement is validated when shard locations are acquired |
| `RetainedObjectPayloadReclaimNodeClient` | object subject rather than a caller-supplied PG | retained exact-claim reclaim completion and fence cleanup; opaque lease objects retain their own release authority |
| `MetadataCommandInspectionNodeClient` | genuinely generic metadata PG | read-only command state, checkpoints, retained-log ranges, acceptance, and abandonment inspection |
| `MetadataCommandNodeClient` | genuinely generic metadata PG | ordinary active publisher and convergence authority; serialized primary acceptance/application opens a non-nestable critical section bound to one PG and epoch, while recovery-only operations remain unavailable |
| `MetadataCommandPeeringNodeClient` | genuinely generic metadata PG | opens a scoped peering route bound to one destination PG and epoch; the returned interface owns quiesced-PG replay validation, retained-log catch-up, and metadata-transfer initialization/adoption without accepting replacement destination route arguments, command-ID allocation, or pending-slot publication |
| `MetadataCommandRecoveryNodeClient` | genuinely generic metadata PG | opens a non-nestable recovery critical section bound to one PG and epoch; the returned interface owns pending-slot reissue, explicitly authorized recovery apply, and durable abandonment without accepting replacement route arguments |
| `RetainedMetadataCommandNodeClient` | genuinely generic metadata PG | exact retained-route authority for applying and finishing an opaque stream-upload abort prepared from validated node state; callers cannot supply or redirect its PG or command |
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
