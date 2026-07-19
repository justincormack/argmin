# Storage Boundary Compiler-Enforcement Plan

Status: active — Phase 0 complete

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
