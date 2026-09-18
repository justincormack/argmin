<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Storage-Node Crate Extraction

Status: proposed; not started.

## Goal

Separate the per-node storage implementation from the distributed storage
framework. The node crate owns all SQLite usage and local physical storage;
the distributed layer coordinates nodes through narrow typed operations.
Preserve the existing S3 behavior, durability guarantees, and compiler-enforced
boundaries while making these responsibilities easier to understand and change.

This is an internal workspace refactor, not a new deployment mode, upgrade
mechanism, or public Rust API commitment. Do not introduce compatibility readers,
migrations, or new external production dependencies as part of extraction.

## Existing Groundwork

The completed [compiler-enforcement plan](completed/storage-boundary-compiler-enforcement-plan.md#node-implementation-boundary)
introduced a private `node_runtime` parent around the engine, PgStore, node
clients, server, and raw storage traits. It also recorded an
[optional crate split](completed/storage-boundary-compiler-enforcement-plan.md#optional-crate-boundary).
Those module boundaries are already useful; extraction must preserve them,
not replace private modules with broadly public crate APIs.

Today these components still share `storage` with cluster routing, replication,
control-plane/Raft code, and maintenance orchestration. Node code depends on
shared types and command/proof definitions. The metadata-transfer staging
outbox also connects a SQLite-backed store to a control-plane client. Moving
the module tree without resolving these dependencies would create cycles.

## Target Dependency Shape

Proposed workspace crate names:

```text
server-core / server-http / argmin-s3
                  |
                  v
               storage             distributed orchestration and public facade
                /   \
               v     v
      storage-node -> storage-contracts
```

An arrow denotes a production dependency. Existing utility and logical-domain
crates are omitted. `storage-node` and `storage-contracts` must not depend on
`storage`, `server-core`, or `server-http`. Higher layers continue to enter
through `storage`; they do not gain direct node-engine dependencies.

Keep bootstrap composition in `storage` initially. A separate runtime/bootstrap
crate or control-plane crate is not required for this first extraction.
The contracts crate is permitted only for genuinely shared definitions with
documented owners; it must not become a home for all of `types.rs` or raw
database records merely to make compilation succeed.

## Ownership

| Area | Target owner | Boundary |
| --- | --- | --- |
| PgStore, SQLite driver, SQL, schema/catalogue, row decoding, transactions, digest caches, database errors | `storage-node` | Private implementation; no connection, transaction, row, SQL callback, or driver-error escape. |
| Metadata-transfer staging database, local artifact files, evidence persistence, receipt persistence, and restart validation | `storage-node` | Bounded semantic operations and opaque handles, not access to the staging store. |
| Shard files, local read fences, local reservations, node identity/locks, local layout, synchronization, and local recovery | `storage-node` | Capabilities retain the exact local owner and lifetime; release and cleanup run against that owner. |
| Storage-node RPC framing, authentication binding, operation codecs, server dispatch, and local/remote client adapters | `storage-node` | Keep byte codecs private; expose scoped client/server construction and typed operations. Local and remote adapters implement the same contracts. |
| Installed node-route publication, node-local route admission, and server-local capability minting | `storage-node` | Own `StorageNodeRouteAdmissionGate` independently of cluster admission. Validate caller-supplied route evidence against installed node state and bind each server-local capability to this gate. |
| Replicated metadata-command logical model and canonical command encoding; shared proof/carrier definitions | `storage-contracts` | One owner for encoding and validation. Opaque encoded carriers may cross to persistence/transport; raw row models do not. Command application remains node-owned; publication/recovery coordination remains distributed. |
| Shared node identities, operation contracts, and semantic results | `storage-contracts` | Extract only definitions actually needed by both sides. Retain logical types in their existing domain crates where appropriate. |
| Cluster route publication/admission, acting sets, replication/fanout, command convergence, placement, repair/backfill/reclaim orchestration, and live transfer orchestration | `storage` | Own `StorageClusterRouteAdmission` for distributed work. Use node operations; cannot mint node-local admission authority, open stores, or perform physical mutations around the existing state machines. |
| Control-plane authority, Raft, leases, cluster certification, and operator workflows | `storage` | Remain in this crate for now; no general control-plane client or authority dependency from the node engine. |
| Existing S3-facing logical storage API and bootstrap facade | `storage` | Preserve caller semantics and bounded failures; no new PG/EC/database exposure to coordinator or HTTP code. |

Cluster admission and node admission are separate security boundaries, not two
implementations of one transferable capability. Wire callers supply only
untrusted route evidence; successful cluster admission does not authorize node
execution by itself. The node independently validates that evidence and mints
its own server-local capabilities against its installed route/publication
domain. Keep their constructors and gate identity inside `storage-node`, not
in the shared contracts crate or an injected caller-provided minting callback.

Physical execution and distributed scheduling are distinct responsibilities.
For example, deleting a claimed local shard belongs to the node; selecting
repair work across current routes and obtaining the required claim and cluster
admission belongs to distributed orchestration. Node-local admission remains
required for execution. Moving physical code must not create a
public unclaimed repair/delete shortcut.

The same distinction applies to staging evidence: node-owned persistence can
produce an opaque bounded page; `storage` coordinates its authenticated
publication and records the validated receipt through a node operation. Where
node startup needs control-plane services, inject a narrow contract implemented
by the composition layer rather than adding a reverse crate dependency.

## Phase 0 — Close the Extraction Inventory

- [ ] Inventory production and test dependencies of `node_runtime`, including
  engine/client/server facades, PgStore, staging, local filesystem utilities,
  RPC/authentication helpers, and all `crate::` references into distributed code.
- [ ] Assign every moved type, codec, error, capability constructor, and test
  facility to exactly one owner. Expand the ownership table into a concrete
  module/API move list before editing Cargo manifests.
- [ ] Identify the minimal contracts set. Separate logical values from durable
  rows and distinguish evidence carriers from capabilities that authorize work.
- [ ] Resolve command/proof dependencies without relocating control-plane
  snapshots, Raft internals, or physical rows wholesale into contracts.
- [ ] Define who may mint each capability and how the receiver validates it.
  Moving private constructors into a dependency must not make arbitrary callers
  able to fabricate authority, cross node identities, or bypass admission.
  Inventory cluster and node admission gates separately, including installed
  node-route publication and every server-local capability constructor.
- [ ] Inventory node-to-control-plane calls and decide the narrow injected
  interface or composition-layer operation for each, including staging outbox,
  heartbeat/refresh, bootstrap, and recovery paths.
- [ ] Map every affected format to its new owner in the
  [format ledger](../guides/storage-format-ledger.md), including nested command,
  proof, checkpoint, digest, RPC, and filesystem formats.
- [ ] Inventory test-only raw access and feature propagation. Keep impossible
  database states and physical corruption assertions with the node owner;
  preserve higher-level logical scenarios through narrow test support.

Exit: an acyclic concrete move list and constructor/format ownership decisions.
This is a planning gate, not a requirement to complete the extraction first.
If the proposed ownership cannot satisfy existing capability guarantees, amend
this plan before proceeding rather than widening visibility to unblock a move.

## Phase 1 — Establish Shared Contracts

- [ ] Introduce the minimal internal contracts crate from the approved inventory.
- [ ] Move shared logical definitions and their owner codecs/tests together.
  Keep codec internals private and expose only the necessary opaque-carrier
  operations. Do not duplicate encoders or raw integer-to-proof constructors.
- [ ] Replace reverse dependencies with the approved narrow contracts.
  Contracts must not depend on SQLite, the node implementation, or OpenRaft.
- [ ] Preserve error classifications and bounded diagnostic labels. Concrete
  database/IO implementation causes remain inside their owner, not exposed
  through public fields, `Debug`, `Display`, `source()`, or conversion traits.
- [ ] Add compiler-negative coverage for capability construction and forbidden
  representation access before broadening any cross-crate visibility.

Exit: both layers can use the same contracts without a cycle, duplicate
representation authority, or new caller-visible physical state.

## Phase 2 — Extract the Node Implementation

- [ ] Move the engine, PgStore, shard implementation, staging persistence,
  storage-node server, and local/remote adapters into `storage-node` in bounded,
  coherent slices. Preserve private descendant-only boundaries inside it.
- [ ] Move `rusqlite` and SQLite feature configuration out of `storage` and into
  the node crate. All SQL, including test corruption setup and staging SQL,
  belongs there; distributed code must not acquire a SQLite dev-dependency.
- [ ] Move local state/layout/recovery helpers to their assigned owner and keep
  shared filesystem/transport utilities at the lowest non-cyclic owner required
  by the inventory. Do not copy helpers to avoid choosing ownership.
- [ ] Compose node runtimes through narrow factories retained by the `storage`
  facade. Preserve standalone embedded and remote Unix/TLS paths.
- [ ] Preserve owner-bound handles across cleanup, drop, cancellation, and
  publication. Keep work budgets, absolute deadlines, retry classification,
  checkpoint-before-response, and ambiguous-mutation semantics unchanged.
- [ ] Keep local mutation validation and remote dispatch validation aligned;
  extraction must not let an embedded adapter bypass checks enforced over RPC.
  Preserve node-side rejection of stale or crossed route evidence even when
  the caller holds cluster admission. Retain tests for node-local publication
  fencing and capabilities bound to the wrong node admission domain, plus
  compiler-negative tests preventing external server-local capability minting.
- [ ] Remove superseded facade aliases and forwarding paths once their callers
  are moved. No parallel old/new engine API remains at completion.

Exit: SQLite and physical local state are implemented solely by the node crate;
distributed operations use its scoped interfaces without raw implementation access.

## Phase 3 — Tests, Enforcement, and Documentation

- [ ] Move owner-local tests with their implementation, including schema and
  codec goldens, malformed-state tests, transaction rollback, file replacement,
  restart/replay, RPC validation, and injected durability failures.
- [ ] Preserve end-to-end distributed tests for replication, route replacement,
  repair/reclaim/backfill, and transfer. Do not replace them with only helper
  round trips or manual single-step worker calls.
- [ ] Keep every existing test in the default `cargo nextest run` inventory.
  Any target relocation must preserve execution, not hide tests behind an
  opt-in feature. Keep raw test hooks unavailable in production builds.
- [ ] Update the storage boundary checker, compiler-negative fixtures, and
  test-feature dependency checks to recognize the new owners and source paths.
  Enforce the dependency direction and sole SQLite owner rather than retaining
  stale path-based checks that silently scan the wrong crate.
- [ ] Verify default-feature and all-feature builds separately. Forward crypto
  provider selection without accidentally linking both providers, and retain
  the documented ring/OpenSSL build and test paths.
- [ ] Update crate documentation, the format ledger, invariant guides, and test
  commands to use the new owners. Keep historical completed plans historical.

## Format and Verification Rules

Follow the [versioning guide](../guides/versioning.md). A pure move preserves
bytes and versions: schema catalogues, enum tags, command encoding, checksums,
digest domains, authentication transcripts, and persisted paths must remain
identical. Preserve the frozen fixtures and their full version vectors when
relocating tests. Do not rewrite expected bytes merely to make a moved test pass.

If extraction reveals a necessary semantic or format change, separate that
decision from the move and follow the guide's parent-commit evidence and
version-advancement rules. No format is silently advanced within an uncommitted
slice. Coordinate with concurrent format work using the current ledger rather
than hard-coding today's version numbers into this plan.

Run focused owner and boundary tests for each slice, with review before commit.
Run the full `cargo nextest run` before committing, and the standard
`./scripts/ci` gate for the completed extraction. Verification includes the
default-feature node/distributed builds, warnings-denied Clippy, embedded and
Unix/TLS operation, restart/process tests, and crypto-provider feature checks.
Existing shared S3 tests remain behavioral evidence; unexpected AWS-facing
changes require investigation and the same tests against AWS, not new
target-specific expectations.

## Completion

- `storage-node` owns every SQLite implementation and local physical store.
- `storage` orchestrates distributed work without raw node/store access.
- Shared definitions have one owner and no reverse dependency or authority-
  minting bypass; higher layers still see only the logical storage facade.
- Cluster and node route admission remain independent; node-local capabilities
  are issued only after node-owned validation and are bound to its own gate.
- All previous tests remain executable, format evidence is unchanged for pure
  moves, and architectural checks enforce the new crate boundaries.
- The ledger and guides identify the actual owners. Further control-plane or
  cluster crate decomposition, if useful, is separate work.
