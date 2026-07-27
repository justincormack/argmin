# Internal Format Ownership, Upgrade And Versioning Plan

Status: draft

## Context

The project currently does **not** support upgrades from older on-disk formats or older
wire/control-plane snapshots. That is intentional while Phase 11 stabilisation and Phase
12 Raft work are still churning core metadata, control-plane, route-map, and command-log
structures. Until that churn settles, compatibility code for old formats makes the current
baseline harder to reason about and can make APIs worse by carrying optional or fallback
states that are not truly optional.

The recent nullable multipart initiator cleanup is the example to avoid: a field that was
kept nullable for a migration we do not currently support leaked into production caller
semantics and made the baseline more complex. The near-term rule should remain simple:
current code opens only current-format state, and any old/unknown format fails closed.

This plan defines how to move from that rule to deliberate versioned upgrade support later.
It also moves the former Phase 11 Slice 2 trigger-SQL-body work here: stale trigger
definitions are an upgrade correctness problem, not a current stabilisation task, because
there should not be any supported legacy triggers yet.

Version markers are not sufficient by themselves. Every durable or cross-process
representation must have one owning crate that can change, reject, negotiate, and eventually
migrate that representation without requiring unrelated callers to understand its internals.
The public boundary is the typed logical operation; SQL, database filenames, wire frames,
numeric wire tags, raw persisted records, and implementation-specific errors remain private
to the owner.

Topology resize is tracked separately in `storage-topology-resize-plan.md`. Some durable
format/version decisions will be prerequisites for resize, but resize also changes live
placement semantics and needs its own topology-generation design.

## Current Policy

- No migration compatibility is required for existing deployments.
- Old, missing, or unsupported format versions should fail closed rather than be repaired or
  silently interpreted.
- Do not add fallback parsing or nullable fields for hypothetical legacy data during the
  current pre-alpha phase.
- When a structure changes now, update the baseline format and tests directly.
- A representation that may require independent compatibility or migration has exactly one
  owning crate.
- Callers use typed logical operations and semantic errors. They do not inspect or construct
  the owner's disk, wire, or nested serialization representation.
- Tests that construct malformed or impossible persisted/wire state live in the owning crate.
  Cross-crate and process tests exercise the logical API or an owner-provided opaque test
  facility.

## Version Boundary Ownership Rule

A version boundary includes more than a top-level schema or frame. It includes:

- database schemas, database engines, table/column names, and database error types
- directory layouts, filenames, file magic, durability/synchronisation rules, journals,
  sentinels, and restart artifacts
- transport framing, request/response tags, authentication envelopes, retry classifications,
  and protocol-specific errors
- independently encoded values embedded in rows, command logs, snapshots, or RPC payloads
- externally retained tokens, manifests, identity files, and operator-facing diagnostic
  schemas

For every such boundary:

1. Name one owner crate. A module is not a sufficient ownership boundary when its
   representation remains public to another crate.
2. Keep construction, parsing, encoding, decoding, opening, validation, and future migration
   inside the owner. These functions should normally be private or `pub(crate)`.
3. Expose typed logical inputs, outputs, capabilities, and semantic failures. Do not expose
   raw database handles, SQL, backend errors, wire tags, framed `Vec<u8>` values, WAL records,
   or implementation filenames.
4. If transport setup must be supplied by another crate, pass endpoint/listener configuration
   or a generic connected stream into the owner. The owner still performs framing, version
   checks, authentication binding, and response decoding.
5. If one owner stores an opaque format owned by another crate, preserve it as an opaque typed
   value. Only the format owner may interpret it, and both the nested format and its containing
   format must have explicit version/change rules.
6. Translate implementation failures at the boundary. Detailed backend diagnostics may be
   recorded within the owner, but callers receive only stable semantic classifications and
   redacted diagnostics appropriate to their layer.
7. Keep malformed-format, corruption, unsupported-version, and impossible-state tests in the
   owner. Higher-level tests should not manufacture raw databases, files, WAL records, or RPC
   frames.
8. Enforce the boundary primarily with Rust visibility and crate dependencies, supplemented by
   repository checks for representation details that are easy to leak accidentally.

This rule does not require one crate per format. A crate may own several closely related
formats, provided their representations do not escape and the ownership remains unambiguous.

## Goals

1. Remove legacy compatibility paths that are already present but not required.
2. Identify every durable or cross-process format that will need explicit versioning.
3. Add clear baseline current-version markers to those formats.
4. Define the future upgrade framework, including trigger-body correctness, but do not
   implement upgrade-from-old-format behavior until after Phase 12 Raft structure churn has
   settled.
5. Make every version boundary private to one owner crate before compatibility branches or
   migrations are introduced.
6. Remove representation-specific errors, retry decisions, and test setup from non-owning
   crates.

## Non-Goals For Now

- Supporting any existing pre-alpha store in production.
- Opening older SQLite schemas and migrating them in place.
- Carrying mixed-version clusters.
- Maintaining old RPC/control-plane encodings beyond explicit current-version rejection
  tests.

## Phase 0: Legacy Compatibility Audit And Removal

Audit the codebase for compatibility paths that were added speculatively or for earlier
scratch formats. Remove them unless they are needed for the current format.

Known audit targets:

- Nullable or fallback data fields that should be mandatory in the current schema.
  - The multipart initiator identity cleanup removed one such class. Use it as the model:
    make caller semantics explicit, require the field at construction/storage/RPC
    boundaries, and reject absent data instead of filling it later.
- Control-plane state parsing that still accepts version ranges or has per-version branches.
  - The control-plane snapshot parser now rejects non-current versions and requires
    current-format fields such as `max_committed_timestamp_ms`. Keep tests on
    current-format fixtures plus unsupported-version rejection cases, not retained
    old-version parsers.
- Open-time cleanup that mutates state to compensate for older layouts.
  - Current startup/recovery repair should be for crash leftovers in the current format, not
    schema migration.
- Any `Option<T>` in durable records, RPC payloads, or command payloads where the value is
  always required for the current format and `None` exists only for legacy compatibility.

Exit criteria:

- A grep/audit document lists every removed legacy path and every current-format recovery
  path that might superficially look like migration code.
- Current-format recovery paths must explain why they are crash recovery rather than
  legacy-version compatibility.
- New code review rule: no speculative migration fallback while upgrades are unsupported.

PG SQLite schema baseline completed 2026-07-27:

- The PG database now records one current `PRAGMA user_version` as part of
  transactional schema creation. Version zero is accepted only for a database
  with no user schema objects; existing unversioned and differently versioned
  databases fail closed.
- All speculative `ALTER TABLE` migrations and data backfills were removed.
  Current columns, indexes, constraints, and triggers are created directly as
  the baseline schema.
- Metadata-digest bootstrap has a current-format incomplete marker created with
  the schema. Missing markers, digest rows, or triggers on a completed store are
  corruption and fail closed rather than being treated as an older store.
- The unused bucket-subresource discriminants 2 and 3 were removed from the SQL
  constraint, matching the current typed writer and decoder exactly.

This completes the PG SQLite schema cleanup and baseline version marker. It does not complete
the storage-format ownership boundary: `argmin-s3` still knows the `pg-NNNN` directory layout,
the `metadata.db` filename, SQLite file magic, and metadata-file synchronisation behavior.
Those operations must move behind a storage-owned initialization/inspection/synchronisation
API, and the associated impossible-layout tests must move into `storage`.

## Phase 1: Version Boundary Inventory And Containment

Document every durable or cross-process format that needs an explicit baseline version, assign
its owner, and close the representation leaks before adding further version machinery.

Initial inventory:

- PG SQLite store schema, engine identity, database errors, and open/version checks.
- PG directory and file layout, metadata database filename, shard layout, durable identity,
  and initialization/synchronisation rules.
- Metadata digest trigger definitions and the `metadata_table_digests` cached rows.
- Metadata command log payload encoding and command checksum semantics.
- Metadata command checkpoint encoding.
- Canonical metadata state digest encoding.
- Pending metadata command slots and scoped provenance fields.
- Bucket/object/stream/multipart durable rows whose shape participates in command-log
  replay or recovery.
- Object payload/shard storage metadata, including storage-byte and payload-byte integrity
  fields.
- Reclaim/finalizer/repair/backfill durable queues and claim rows.
- Nested object formats stored inside rows and carried through commands/RPCs: user metadata,
  system metadata, tag sets, ACLs, and encryption state.
- Control-plane authority state, clock checkpoint, identity, journal, initialized marker, and
  restart artifacts.
- Control-plane command, snapshot, RPC, and authentication-envelope formats.
- Storage-node RPC framing, per-kind payloads, authentication bindings, transport generation,
  and wire-error mapping.
- Raft peer RPC framing and authentication, Raft log entries, snapshots, membership and
  application-state encodings, restart checkpoint/sentinel, and WAL file/record formats.
- Static cluster manifest schema and static storage/control-plane identity files.
- Temporary-credential session-token envelope and key-generation identity.
- Protocol identifiers that constrain compatibility, including internal TLS ALPN values.
- Production-facing or persisted diagnostics if operators or tooling are expected to consume
  them across binary versions.
- Test-only local debug/diagnostic endpoints do not need versioning: tests that consume them
  are tied to the fixed version of the code under test, and their shape can change with the
  test harness.

For each boundary, record:

- owner crate
- public logical API and semantic error surface
- private representation modules/types and any current cross-crate leaks
- current version marker location
- whether old versions are rejected, silently accepted, or partially parsed
- whether peers negotiate versions, reject mismatches before dispatch, or currently assume an
  identical binary
- whether the format can be upgraded in place, must be rebuilt from another authority, or
  must be treated as incompatible
- owner-local malformed/impossible-state tests and cross-crate logical integration tests

Containment audit targets:

- Raw database handles, SQL, database filenames, backend magic, or backend error types outside
  the persistence owner.
- Raw RPC frames, wire request/response types, numeric tags, protocol error codes, or framing
  helpers outside the protocol owner.
- Public WAL/journal records, snapshot codecs, restart sentinels, or file-layout helpers used
  by non-owning crates.
- Public encode/decode methods for internal nested formats when no external crate legitimately
  owns or interprets the representation.
- Cross-crate tests that create impossible database, file, journal, or wire states directly.

Initial ownership assessment:

| Boundary | Current owner | Current containment work |
| --- | --- | --- |
| PG schema and physical PG/shard layout | `storage` | SQL and the driver are now private to `PgStore`; remove SQLite magic, `metadata.db`, `pg-NNNN`, direct fsync, and raw shard-layout knowledge from `argmin-s3`. |
| Metadata command log, checkpoints, and canonical metadata digests | `storage` | Codecs are largely crate-private; inventory every embedded nested format and keep recovery/corruption tests local. |
| Storage-node RPC | `storage` | The main codec and wire error codes are private; `StorageNodeServer`, `StoreError`, and `StorageNodeFailureClass` form the logical facade. Remaining work is ALPN/TLS profile containment. |
| Control-plane durable state, RPC, and auth envelope | `storage` | Replace raw frame transports and binary-owned request verification/response framing with a storage-controlled client/server facade. |
| Raft peer protocol, restart artifact, and WAL | `storage` | Hide raw frame decoders, framing helpers, WAL records/files, and layout helpers; move direct WAL construction tests into storage. |
| Object user/system metadata blobs | `server-core` | Keep storage's carriers opaque; make serialization entry points crate-private unless another owner has a demonstrated need to interpret them. |
| Tag and ACL canonical value formats | `s3-types` | Keep validation and canonical value codecs central; treat their embeddings in storage rows/RPCs as separately versioned containing formats. |
| Object encryption state | `storage` | Keep the durable codec private to storage while exposing only typed encryption state to callers. |
| Session-token envelope | `auth` | Keep sealing/opening/version selection in auth; callers handle only issued token strings and semantic authentication results. |
| Static manifest and process identity files | `argmin-s3` | The codecs are currently crate-local; inventory their coupling to storage/control-plane durable layout. |
| Shared operator metric schema | `observability` | Decide explicitly which metrics are compatibility contracts before versioning the shared schema. Subsystem-specific persisted diagnostics must be inventoried as separate boundaries owned by their producing crate rather than treated as one shared format. |

These are the current owners, not placeholders shared between crates. A later extraction into
a dedicated crate would be a deliberate ownership transfer: move the complete private
representation and logical facade together, update the dependency direction, and remove the
old owner's access in the same change. Until then, `storage` exclusively owns the
control-plane and Raft formats, and `argmin-s3` exclusively owns the static manifest and
process-identity formats.

### RPC Boundary Inventory (2026-07-27)

This inventory covers the storage-node, control-plane, and Raft peer RPC surfaces. All three
protocols are owned by `storage`. `argmin-s3` owns process configuration and lifecycle, but it
must eventually pass typed endpoint/listener and credential configuration to storage-owned
clients and servers rather than implementing any wire exchange itself. No separate protocol
crate is justified by the current dependency graph.

The protocols currently assume identical binaries. They have exact current-version rejection,
but no version negotiation or supported compatibility window:

| Surface | Current wire baseline | Authentication baseline | Negotiation and current disposition |
| --- | --- | --- | --- |
| Storage-node RPC | `STORAGE_RPC_FRAME_ENCODING_VERSION = 8` in `storage_rpc.rs`; frame magic, message-kind tags, checksums, and payload codecs are crate-private. | Binding version 2 and transport-envelope version 1 in `storage_rpc_auth.rs`. | Exact versions are required before dispatch. There is no negotiation. Treat any other version as incompatible until mixed-version operation is designed. |
| Control-plane RPC | `CONTROL_PLANE_RPC_VERSION = 9` in `control_plane.rs`; the frame contains magic, version, request kind, length, checksum, and payload. | Shared control-plane authentication-envelope version 1 in `control_plane_auth.rs`. | The frame and auth decoders reject non-current versions before logical dispatch. There is no negotiation. Treat any other version as incompatible. |
| Raft peer RPC | `CONTROL_PLANE_RAFT_PEER_RPC_VERSION = 2` in `control_plane_raft.rs`; request, response, snapshot, peer-identity, checksum, and numeric OpenRaft tags share this baseline. | Shared control-plane authentication-envelope version 1, with the authenticated operation and peer identity bound to the inner frame. | The decoder rejects non-current versions before OpenRaft dispatch. There is no negotiation, and OpenRaft peers currently require the same binary. Treat any other version as incompatible. |

These are ephemeral wire formats, so there is no in-place migration or authoritative rebuild
operation. A mismatch closes the exchange without dispatch. Any future rolling-upgrade support
must negotiate a compatible protocol before requests are sent; it must not add fallback parsing
to the current codecs.

The public boundary and containment status for each surface are as follows.

#### Storage-node RPC

- The intended logical client boundary is `StorageCluster` plus its typed operations. Client
  construction uses `LocalUnixStorageNodeClientConfig`, `StorageRpcClientEndpoint`, admission
  settings, and typed authentication capabilities. The intended server boundary is
  `PreparedStorageNodeServer`/`StorageNodeServer` plus `StorageNodeRpcListenerConfig` and
  `StorageRpcServerAuthConfig`.
- Storage already owns connection establishment, deadlines, framing, request/response codecs,
  authentication, dispatch, and server accept loops for both Unix and TLS/TCP transports. The
  binary supplies endpoint, listener, TLS, credential, and lifecycle configuration and invokes
  `serve_forever()`; it does not handle storage-node frames.
- The wire-error containment slice is complete. `StorageRpcErrorCode` is now a crate-private
  alias for the opaque `StorageNodeFailure`; only storage can construct or match its protocol
  values. `StoreError::storage_node_failure_class()` maps transient cases to the public semantic
  `StorageNodeFailureClass`, leaving `server-core` and `argmin-s3` to select their own retry
  policies without knowing wire codes. Remote diagnostic text is retained only in the opaque
  `StorageNodeFailureDetail`; its public `Debug` and `Display` are redacted, and generic storage
  failures expose one bounded diagnostic kind rather than their wire-specific identity.
  Raw-code mapping and redaction tests are storage-owned, caller policies use exhaustive matches
  over the semantic enum, and the repository boundary check prevents wire types, opaque
  diagnostic values, or their fields from being used outside storage.
- `storage_rpc_transport` also publicly exposes the protocol ALPN and generic stream traits even
  though no external production caller uses the stream traits. `argmin-s3` constructs Rustls
  configurations with `argmin-storage-rpc/1` directly and tests the literal protocol profile.
  Endpoint/listener configuration is a valid public input, but ALPN selection and validation are
  protocol representation and must move behind storage-owned TLS endpoint constructors.
- Retry policy remains intentionally caller-owned while representation translation is
  storage-owned. `server-core` and `argmin-s3` make exhaustive decisions over
  `StorageNodeFailureClass`; storage-internal protocol handling alone may inspect wire codes or
  retained remote diagnostic text.
- Raw codec, malformed-frame, authentication, client, and server tests are already in `storage`.
  Cross-crate S3/process tests mostly use logical storage operations. The remaining
  `argmin-s3` ALPN/endpoint-shape tests should use typed configuration and leave protocol-profile
  assertions in `storage`.

#### Control-plane RPC

- The intended logical client boundary is `UnixControlPlaneClient` or
  `AuthenticatedUnixControlPlaneClient` through the control-plane admin, heartbeat, runtime-map,
  and authority-clock traits. The authority implementations and typed snapshots/results also
  belong to `storage`.
- The Unix client path is storage-owned, but the general client extension point is a raw
  `ControlPlaneRpcFrameTransport`. It gives `argmin-s3` a fully encoded request `Vec<u8>`, frame
  limit, deadline, and request-sent state and expects a fully encoded response `Vec<u8>`.
  `static_cluster_config.rs` consequently implements Unix and TLS/TCP connect, deadline, ALPN,
  complete-frame write/read, size enforcement, and construction of protocol errors.
- The server boundary is substantially open. `argmin-s3/main.rs` binds Unix and TCP listeners,
  performs TLS and ALPN handling, manages worker and pre-auth byte budgets, reads raw frames,
  invokes storage authentication and endpoint-admission helpers, dispatches verified requests,
  writes framed responses, and classifies response-write I/O errors. Public
  `ControlPlaneRpcRequest`, `VerifiedControlPlaneRpcRequest`, `ControlPlaneRpcResponse`, raw
  read/verify/write functions, and `ControlPlaneAuthEnvelope::decode_frame` exist chiefly to
  support that binary-owned protocol loop.
- `CONTROL_PLANE_RPC_TLS_ALPN` is public and `argmin-s3` constructs and validates
  `argmin-control-plane/1` Rustls profiles directly. As with storage-node RPC, the static manifest
  may specify endpoint and TLS material, but storage must own ALPN and protocol-profile assembly.
- `ControlPlaneError` currently mixes logical authority failures with public `Io`, `RpcProtocol`,
  and string-valued `RpcRemote` wire/transport failures. The storage clients own much of the
  retry logic, including read-only endpoint failover and operation-specific response-loss
  confirmation, but `argmin-s3` still destructures I/O errors for metrics and parses rendered
  control-plane and runtime-map messages to make retry decisions. These need owner-defined
  semantic classifications and a stable server diagnostic surface.
- Storage contains comprehensive codec, version, authentication, retry, and Unix client/server
  tests. `argmin-s3` also has raw-frame tests for pre-auth admission, TLS/ALPN, authentication,
  response publication, and transport failure injection. Process-level lifecycle and durability
  tests may remain there, but malformed-frame and protocol-profile tests must move to `storage`;
  process tests should call a logical client or an opaque storage-owned test facility.

#### Raft peer RPC

- The intended logical boundary is `ControlPlaneRaftAuthority` and its typed bootstrap,
  lifecycle, linearized-command, status, and leader-routed-admin handles. The OpenRaft network,
  peer server, authentication policy, and durability-before-ack behavior are all storage-owned
  protocol concerns.
- Storage owns the Unix peer network and maps transport failures into OpenRaft `Unreachable` or
  `Network` errors. For configured TLS/TCP peers, however, it exports
  `ControlPlaneRaftPeerFrameTransport` and `ControlPlaneRaftPeerFrameExchange`, exposing encoded
  frames, limits, deadlines, context strings, and protocol-error construction.
  `static_cluster_config.rs` implements the TCP length prefix, frame allocation, TLS/ALPN, and
  raw response exchange.
- The inbound server is also binary-owned. `argmin-s3/main.rs` binds listeners, establishes TLS,
  reads the transport length prefix, decodes the shared authentication envelope, extracts frame
  kind/identity/operation, validates admission, calls raw OpenRaft frame handlers through
  `ControlPlaneRaftAuthority::raft()`, signs and writes the response, and coordinates durable
  checkpoint publication before acknowledgement.
- The resulting public representation includes peer request/response/snapshot types,
  `encode_frame`/`decode_frame`, frame-kind and identity decoders, transport read/write helpers,
  the raw frame handlers, shared auth-envelope codecs, the ALPN constant, and the underlying
  OpenRaft `Raft` handle. None is an appropriate binary-facing logical API.
- Retry classification is partly correct inside storage: the network maps typed transport
  failures to the OpenRaft error categories that drive peer retry. It is nevertheless coupled to
  public `ControlPlaneError::Io` construction and context strings supplied by `argmin-s3`.
  Storage must own the concrete TCP/Unix transport errors and their OpenRaft classification.
- Raw codec, version, identity-binding, authentication, transport, and OpenRaft dispatch tests
  already exist in `control_plane_raft.rs`. `argmin-s3` duplicates extensive raw-frame,
  malformed-envelope, TLS/ALPN, admission, and response-publication testing. The protocol cases
  belong in `storage`; only process lifecycle, crash, and durability-observation tests should
  remain in the binary crate, using logical or opaque owner-provided facilities.

This completes the RPC inventory only; it does not satisfy Phase 1 containment. The bounded
implementation order is:

1. Replace the control-plane raw client frame-transport extension with storage-owned Unix and
   TLS/TCP endpoint configuration. Keep static-manifest parsing in `argmin-s3`, but hide frames,
   ALPN, request-sent tracking, and transport error construction.
2. Add a storage-owned control-plane server facade that accepts listener, resource-limit,
   authentication, authority-clock, and durability-publication configuration while owning TLS,
   frame admission, verification, dispatch, response framing, and transport diagnostics.
3. Replace the Raft raw client frame transport with storage-owned peer endpoint configuration,
   then add a storage-owned peer server facade that preserves the existing pre-auth allocation
   bound and durability-before-ack invariant.
4. Make raw control-plane/Raft frame, auth-envelope, ALPN, OpenRaft-handle, and transport-error
   APIs private after callers and protocol tests have moved. Add repository checks for the
   concrete leaks only after the replacement facades exist.

Phase 1 exit criteria:

- Every version boundary has one recorded owner crate and a documented public logical API.
- Representation details and implementation errors do not cross the owner crate boundary.
- Impossible-state tests are colocated with the format implementation; process tests use
  supported operations or opaque owner-provided test facilities.
- Every identified cross-crate representation leak is removed and replaced by the owning
  crate's logical API. Recording a leak as future work does not satisfy this phase's exit gate.
- Boundary checks cover known high-risk leaks, while compiler visibility remains the primary
  enforcement mechanism.

## Phase 2: Baseline Version Markers

After the inventory, ensure each format has a single explicit current baseline version.

Rules:

- Current-version constants should be named and local to the format owner.
- Parser/open paths should reject missing or unsupported versions with typed errors.
- Current-format construction should always write the current version.
- Version constants, wire tags, magic values, and codecs should not be public merely to let a
  non-owning crate assemble transport or test fixtures.
- Unsupported versions must be rejected inside the owner before logical dispatch or state
  mutation.
- If a format is self-describing by magic plus version, malformed/unknown magic and unknown
  version should have separate tests.
- If a format is SQLite-backed, use a dedicated metadata table or `PRAGMA user_version`
  consistently; do not infer schema version from incidental table/column presence alone.

This phase still does not implement upgrade steps. It only creates a clean baseline that a
later upgrade framework can reason about.

## Phase 3: Future Upgrade Framework

Implement this only after Phase 12 Raft structure churn is stable enough that upgrade tests
will not be rewritten continuously.

Upgrade framework requirements:

- A store opens by reading the format version before serving or repairing state.
- Upgrade steps are explicit, ordered, idempotent where possible, and crash-safe.
- Each step has preconditions, postconditions, and a verification scan.
- Upgrade code must not reuse normal crash-recovery cleanup as an implicit migration.
- A failed upgrade leaves the store closed and diagnosable.
- Mixed-version cluster behavior must be defined before any rolling upgrade support:
  either unsupported with a hard version gate, or supported through explicit compatibility
  windows and Raft/control-plane feature negotiation.

Testing requirements:

- Golden stores/snapshots for every supported old version.
- Crash injection between every upgrade step and its version bump.
- Reopen-after-crash property tests that prove idempotence or fail-closed behavior.
- Differential tests that compare upgraded state against a fresh current-format store built
  from equivalent logical operations.
- Unsupported-too-old and unsupported-too-new tests for every versioned boundary.
- Format fixtures and corruption injection live in the owner crate. Cross-crate and process
  suites prove behavior through the logical API and must not depend on physical filenames,
  raw frames, SQL, or journal record constructors.
- Boundary tests prove implementation-specific errors and representation details cannot be
  observed or classified by callers.

## Phase 4: Trigger SQL Body Verification

This is the former Phase 11 Slice 2. It belongs here because stale trigger definitions arise
when a store created under an older schema is opened by newer code.

Current `metadata_digest_triggers_complete` only checks that trigger names exist in
`sqlite_master`. Under real upgrade support, that is insufficient: an old trigger with the
same name but different SQL could keep updating cached table digests using an obsolete row
digest expression.

Upgrade-time behavior should be:

1. Store the expected trigger SQL hash or schema-generation tag for every digest-tracked
   table.
2. On open, compare the stored/current expected trigger identity with the installed trigger
   SQL.
3. If the store version says the trigger body is from an older supported generation, run the
   explicit upgrade step: drop and recreate the triggers, then force a full materialised
   table-digest recompute before serving.
4. If the trigger body is unknown, malformed, or inconsistent with the declared version,
   fail closed rather than repairing silently.
5. Keep the Slice 1 materialised digest scan as corruption detection; do not treat it as the
   trigger upgrade mechanism.

Trigger upgrade tests:

- Open a golden old-version store with old trigger SQL and verify the upgrade recreates the
  trigger bodies and refreshes cached digests.
- Inject a crash after trigger recreation but before digest recompute and verify reopen
  resumes or fails closed according to the step contract.
- Corrupt only the trigger SQL identity while leaving materialised rows valid and verify the
  store does not serve until the mismatch is resolved through a supported upgrade path.
- Corrupt materialised rows or state digest and verify upgrade does not mask real
  corruption.

## Phase 5: Rolling Upgrade And Raft Interaction

This phase should wait until the Phase 12 replicated control plane has a stable log/snapshot
shape.

Requirements and open questions:

- Storage nodes and frontends must eventually support different binary versions during a
  node-by-node cluster upgrade. The design may impose a bounded compatibility window, but it
  cannot require all frontends and storage nodes to restart atomically on the same binary.
- Define the supported version skew range and feature-gate rules for frontend-to-storage,
  storage-node-to-control-plane, and control-plane/Raft interactions.
- Does Raft log replay require old command decoders forever, or can upgrades require
  checkpoint/snapshot compaction first?
- How are feature gates represented in the control plane?
- Can metadata PG schema upgrades happen independently per PG, or must the whole cluster be
  quiesced?
- How does rolling binary upgrade interact with topology resize? See
  `storage-topology-resize-plan.md`; a cluster should not run resize operations across
  binaries that disagree on placement-generation semantics.
- What is the rollback story after a node has written a newer format?

Until these are answered, do not add rolling-upgrade compatibility code.

## Completed Containment Slices

The storage-owned PG layout slice is complete:

- `storage` now owns configured PG path construction, metadata-store identity checks,
  durable-identity binding, shard-inventory inspection, and initialization synchronisation
  behind semantic storage-node initialization and inspection APIs.
- The low-level PG identity and inventory operations are crate-private. `argmin-s3` no longer
  knows the PG directory pattern, metadata-store filename or representation, SQLite file
  identity, or shard-tree layout.
- Impossible physical-layout tests for placeholder or symlinked metadata stores, missing
  payloads, unindexed crash residue, and symlinked shard roots are owned by `pg_store`.
- Storage-node errors retain low-level initialization diagnostics opaquely: callers receive a
  semantic PG-state error and cannot inspect the underlying `StoreError` or its error chain.
- `scripts/check-storage-cluster-boundaries` rejects new physical PG, metadata-store, or shard
  layout knowledge in `argmin-s3`.
- The native storage-node data-directory lock filename is private. Outer deployment
  initialization receives a snapshot of all directory entry names except the exact held native
  lock; this API does not classify any other entry by ownership or initialization phase. Storage
  validates that the omitted directory entry still refers to the exact held lock file.
- Native-lock symlink and replacement tests are owned by `storage`; the boundary check rejects
  exposing the constant or literal filename to `argmin-s3`.

Residual containment work includes closing the inventoried RPC/control-plane/Raft leaks,
hiding WAL and restart-format constructors, and replacing any remaining higher-layer
implementation-error matching. These remain explicit work below.

## Immediate Next Steps

1. Replace the raw control-plane client frame transport with storage-owned Unix and TLS/TCP
   endpoint configuration.
2. Move control-plane server framing, authentication, admission, dispatch, and transport
   diagnostics behind a storage-owned server facade.
3. Move Raft TLS/TCP client exchange and inbound peer serving behind storage-owned facades,
   preserving the pre-auth allocation bound and durability-before-ack invariant.
4. Privatize the raw control-plane/Raft frame, auth-envelope, ALPN, OpenRaft-handle, and
   transport-error APIs and relocate malformed-wire tests into `storage`.
5. Hide public WAL/restart-format constructors and move direct WAL/impossible-state tests into
   the owner.
6. Replace other higher-layer matching on database/RPC implementation errors with owner-defined
   semantic errors or classification methods.
8. Inventory and restrict nested durable codecs for metadata, tags, ACLs, and encryption;
   record how containing formats advance when a nested format changes.
9. Audit existing version/fallback code and remove unsupported legacy compatibility where it
   worsens current invariants.
10. Add or tighten current-version rejection tests for existing versioned formats.
11. Add boundary checks for the concrete leaks found in this audit, while relying on crate
   privacy for the durable enforcement.
12. Remove the trigger-verification item from Phase 11 stabilisation tracking and keep this
    plan as the upgrade home for it; defer trigger body hashing/recreation until the upgrade
    framework is deliberately started.
