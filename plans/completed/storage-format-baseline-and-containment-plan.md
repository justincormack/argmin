# Internal Format Ownership, Upgrade And Versioning Plan

Status: archived historical record. Phases 0–2 are complete. The unimplemented future-upgrade
sections are superseded by
[`storage-upgrade-versioning-plan.md`](../storage-upgrade-versioning-plan.md).
References below to Phase 12 mean the replicated-control-plane work archived in
[`multihost-transition-plan.md`](multihost-transition-plan.md#phase-12-replicated-control-plane).

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

The stable engineering policy extracted from this plan lives in
[`guides/versioning.md`](../../guides/versioning.md). In particular, format versions and their
evidence are established only at commit boundaries. The parent commit must already contain the
evidence for the version being replaced, and the same format must not be advanced twice within one
commit, because otherwise the claimed earlier version has no independently verifiable repository
state. The maintained current inventory and evidence ledger lives in
[`guides/storage-format-ledger.md`](../../guides/storage-format-ledger.md); this plan remains the
historical implementation record.

Version markers are not sufficient by themselves. Every durable or cross-process
representation must have one owning crate that can change, reject, negotiate, and eventually
migrate that representation without requiring unrelated callers to understand its internals.
The public boundary is the typed logical operation; SQL, database filenames, wire frames,
numeric wire tags, raw persisted records, and implementation-specific errors remain private
to the owner.

Topology resize is tracked separately in [`storage-topology-resize-plan.md`](../storage-topology-resize-plan.md). Some durable
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

### Legacy Compatibility Audit (2026-07-29)

The current durable and cross-process format decoders were audited for version ranges,
per-version parsing branches, old magic values, normalising fallbacks, and speculative migration
paths. No decoder was found that accepts an older durable or wire representation: the PG schema,
metadata commands and checkpoints, nested metadata/tag/ACL/encryption codecs, storage RPC,
control-plane state/RPC/authentication formats, Raft peer/WAL/restart formats, static identities,
and session-token envelope all require their exact current version or representation.

The audit found these residual changes rather than another old-format reader:

- **Completed 2026-07-29:** the unused
  `ControlPlaneAuthorityClock::new_from_process_clock` constructor was removed. The equivalent
  sample-driven constructors are now restricted to tests and the `test-hooks` feature, while
  production construction requires restart-checkpoint continuity.
- **Completed 2026-07-29:** the active `legacy-local` path was replaced by the explicit
  static-authority design below. Every `StorageCluster` now has a mandatory static or dynamic
  route proof, every no-control-plane process durably binds the storage-owned canonical static
  route identity before serving, and the process topology is named `all-in-one` rather than being
  conflated with the separately configured `standalone` deployment mode.
- **Completed 2026-07-29:** session-token version selection is contained in `auth`.
  `IdentityProvider::seal_session_credential` is the semantic issuance API; the v1 prefix and
  representation-size constants are crate-private. The boundary check rejects versioned sealing
  calls or representation constants outside `auth`, and rejects making those APIs public inside
  the owner.

An initial owner-local pass added unsupported-too-old and unsupported-too-new fixtures for the
following exact-version checks. Each fixture recomputes the enclosing checksum, digest, or
authenticator where the containing format authenticates its version field, so it reaches a
version rejection rather than a generic corruption path:

- static cluster manifest schema and both static storage/control-plane identity files
- system metadata and the encrypted checksum-metadata projection
- abandoned metadata-command log entries
- storage RPC authentication transport and binding
- control-plane clock checkpoint, durable identity, initialized marker, and journal file/record
- Raft WAL file header and durable restart sentinel

The maintained [format ledger](../../guides/storage-format-ledger.md) re-audits these at the first
production rejecting reader and through startup or dispatch. It records missing neighbouring versions and does not treat the
duplicated test-only Raft WAL header decoder as production-path evidence.

These current-format recovery paths were reviewed and are not migration compatibility:

- SQLite version zero is accepted only when the database has no user schema objects; it is the
  engine's fresh-database state, while every nonempty unversioned database fails closed.
- PG open removes orphan temporary shard files, reconciles provably older-epoch pending command
  slots, and repairs cache-only digest drift only after validating materialized state.
- Durable journals accept a missing or empty file only at replay offset zero and recover only a
  bounded torn tail after the last complete validated frame.
- Single-authority startup reconciles interrupted current identity, prepared-snapshot, journal,
  and checkpoint publication, while missing acknowledged durability fails closed.
- Raft startup handles a missing initial WAL, a bounded torn WAL tail, and interrupted publication
  of the current restart artifact/sentinel pair.
- Metadata-transfer checkpoint fallback is a current reconstruction strategy selected when a
  retained command prefix cannot reconstruct the state; it does not decode an older format.

No compatibility-labelled behavior remains in the shared production storage invariants.
Environment/configuration fallback endpoints and AWS policy fallback rules are current
availability or service-semantics behavior, not storage-format compatibility.

### Standalone Route Authority Decision (2026-07-29)

Standalone topology is an explicit immutable authority, not an incomplete dynamic runtime-map
generation. `Standalone` remains the deployment mode; the former `legacy-local` process topology
is now named `all-in-one`, keeping deployment guarantees distinct from process composition.

The authority model has two closed variants:

- **Static authority:** carries a mandatory canonical route-map content digest and an explicit
  immutable authority proof. Its validity is unbounded because its topology cannot be published or
  refreshed while the process is running; this is a positive static invariant, not absence of a
  lease.
- **Dynamic authority:** carries a mandatory canonical route-map content digest, freshness proof,
  and bounded route-map lease. It is the only variant that supports same-epoch renewal, generation
  publication, control-plane refresh, and refresh-loop construction.

Storage owns both authority types, canonical route-digest construction, route admission, and the
durable binding of standalone storage topology. `argmin-s3` supplies validated logical
configuration and retains ownership of manifest/process identity parsing; it must not construct or
interpret route digests. The shared storage-operation implementation remains internal and common to
both modes, while distinct static and dynamic handles expose only their valid capabilities. A
server-core-facing opaque route handle may dispatch between those handles, but must not recreate
optional digest or validity states.

Changing a running process from static to dynamic authority, or the reverse, is unsupported. The
process must restart and construct the new authority mode before serving. Consequently, the current
same-epoch `None`-digest/unbounded-validity to bounded/digested transition is removed rather than
generalised.

The implementation scope includes every no-control-plane construction path, not only the embedded
all-in-one builder:

- standalone manifests and environment-only all-in-one startup;
- no-control-plane remote frontend construction through
  `StorageCluster::from_static_local_map`;
- standalone storage-node configuration using `RouteMapValidity::Forever`.

Manifest-based standalone startup must bind the storage-owned canonical route digest into its
durable standalone identity before serving. Environment-only startup must establish an equivalent
explicit durable binding; it must not remain a weaker unbound production path. A topology change
without the corresponding deliberate identity/epoch change fails closed on restart.

Implementation order:

1. Define storage-owned static and dynamic route-authority proofs and a canonical digest over every
   routing input used by local and remote static topology.
2. Replace public production `from_local_map` construction with explicit static-authority
   construction; keep impossible unbound construction only on the test surface where needed.
3. Restrict runtime-map installation, renewal, and refresh loops to the dynamic handle, whose
   constructor requires a bounded lease and digest.
4. Move coordinator/frontend wiring to a common opaque route handle without duplicating storage
   operations or spreading authority-mode branches through request handling.
5. Bind standalone topology durably, migrate all three no-control-plane paths above, remove the
   `None` digest and special transition, and rename the process topology to `all-in-one` while
   retaining `standalone` as the deployment mode.

Implementation status (2026-07-29):

- The first invariant-bearing slice is complete in the working tree. `StorageCluster` now carries
  a closed static/dynamic authority proof instead of an optional runtime-map digest. Static proof
  construction requires unbounded validity and hashes the cluster epoch, metadata primary,
  erasure-coding shape, node identities and embedded directories or RPC endpoints, current and
  historical PG routes, and retained historical epochs. Unix paths are hashed as raw platform
  bytes, so distinct non-UTF-8 endpoints cannot collide through lossy conversion.
- Dynamic proof construction requires bounded validity and carries the runtime-map content digest
  and freshness proof. Before attaching that proof, storage checks epoch and validity plus the
  complete node/advertised-endpoint map, current PG routes, historical PG routes, and retained
  historical epochs against the runtime snapshot. RPC-backed constructors additionally require one
  installed client per runtime node with the exact authoritative advertised endpoint. Unbounded or
  mismatched dynamic maps fail at construction rather than surviving until a later publication
  attempt. Same-epoch renewal and pinned-generation lease extension additionally remain bound to
  the authority incarnation that issued the installed generation.
- The old unbounded/undigested to bounded/digested same-epoch transition is removed.
- The constructor and capability slice is complete. Public local construction is explicitly
  static through `from_static_local_map` and `open_static_local_nodes`; the generic names are
  removed. Coordinators and request processing receive only `StorageClusterRouteHandle`, which
  supports current-generation access and admission. `StorageClusterRuntimeMapHandle` is a
  distinct capability whose constructor rejects a static generation and is the only public
  surface for publication, renewal, and refresh-loop construction. Frontend startup retains that
  capability only for control-plane-backed topology and passes the opaque common route handle to
  server-core. Direct route-handle construction proves static authority and therefore cannot
  create a second admission/publication domain for a dynamic generation. Cross-crate interleaving
  tests retain the dynamic capability used to derive their request route handle; there is no
  reverse test hook that upgrades a request handle back into publication authority. Public
  coordinator constructors that accept an `Arc<StorageCluster>` are likewise static-only, while
  dynamic wiring must pass the route handle derived from its retained runtime-map capability. The
  repository boundary check locks these constructor and capability surfaces.
- Item 5 is complete. Storage owns an opaque, exact-versioned standalone route identity and its
  crash-recoverable durable preparation, atomic publication, checksum validation, and exclusive
  runtime lock. The preparation retains and locks the exact directory descriptor and lock inode,
  validates both named entries before and after binding, and performs identity publication relative
  to the held directory descriptor. Immediately before publication it revalidates the exact marker,
  published or pending identity inode and contents, and unpublished directory scaffolding. Artifact
  inspection is nonblocking, so special files fail closed rather than stalling startup. A crash
  marker admits only bounded empty private directory scaffolding or a separately validated pending
  identity publication; arbitrary state without a published identity fails closed. Embedded
  topology is consumed into a non-forgeable
  storage-owned preparation;
  callers can bind its opaque canonical identity and then open only that same prepared topology.
  The durable identity is checked and locked before any PG database is opened or recovery runs,
  and storage requires the prepared and opened identities to agree. Manifest
  initialization, environment-only all-in-one startup, no-control-plane
  remote frontends, and no-control-plane storage-node/combined processes all bind the canonical
  static route identity before serving. Combined processes use a storage-owned composition of
  their frontend route map and local storage-node route configuration, so both RPC topology and
  the local durable path are bound. Missing identity beside existing state, corrupted or
  unsupported identity formats, topology/endpoint/path changes, concurrent process ownership, and
  attempts to bind dynamic authority fail closed. The old `legacy-local` process role and parser
  value are removed; `all-in-one` now names process composition and `standalone` remains the
  deployment mode.

Exit criteria:

- No production `StorageCluster` generation can lack a canonical content digest.
- Dynamic generations cannot be constructed with unbounded validity, and static generations
  cannot call publication, renewal, or refresh APIs.
- Static and dynamic authority cannot transition in-process.
- Environment-only and manifest-based standalone topology changes without the required durable
  identity/epoch change fail closed before serving.
- The boundary check rejects unbound production constructors and use of dynamic refresh APIs from
  static startup paths, public generic route-handle constructors, and any reverse conversion from
  request handles to dynamic publication capability.
- Tests pin static construction/admission, dynamic bounded construction/renewal, digest mismatch,
  durable standalone identity mismatch, and restart-only mode changes.

Generating an arbitrary digest, assigning a far-future deadline, or self-renewing a lease without
an independent authority is explicitly not an acceptable implementation: those approaches change
field shapes without establishing the authority semantics the fields represent.

## Phase 1: Version Boundary Inventory And Containment

Document every durable or cross-process format that needs an explicit baseline version, assign
its owner, and close the representation leaks before adding further version machinery.

**Status: complete 2026-08-08.** Phase 1 was reopened on 2026-07-30 after the durable-backfill
convergence fix exposed semantic storage leaks that the original representation audit did not
cover. The subsequent containment slices moved PG identity, route state, physical payload
placement, maintenance claim protocols, control-plane topology workflows, implementation errors,
and storage-node TLS profile construction behind storage-owned boundaries. The matrix and immediate
steps below retain the detailed evidence for that completed work.

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
- PG identifiers, route states, acting sets, cluster epochs used for routing, historical-route
  details, physical shard locations, or EC placement inputs outside `storage`.
- Public storage maintenance claims, queue records, scan cursors, retry taxonomies, or
  acquire/execute/complete protocols orchestrated by another crate.
- Control-plane commands, route snapshots, metadata-transfer proofs, or topology-transition state
  machines interpreted by the process/configuration layer rather than a storage-owned facade.
- Public storage errors whose PG, database, shard, route, command-log, or RPC variants are matched
  by callers instead of being translated into owner-defined semantic classifications.
- Cross-crate tests that create impossible database, file, journal, or wire states directly.

Initial ownership assessment:

| Boundary | Current owner | Containment status |
| --- | --- | --- |
| PG schema and physical PG/shard layout | `storage` | Complete: SQL, driver, SQLite identity, filenames, directory layout, synchronization, and impossible-state tests are storage-owned. |
| Metadata command log, checkpoints, and canonical metadata digests | `storage` | Complete: codecs and recovery/corruption fixtures are private; every embedded nested format is inventoried below. |
| Storage-node RPC | `storage` | Complete containment: framing, wire errors, Unix/TLS transports, TLS 1.3 and private ALPN profile construction, authentication, dispatch, and malformed-wire/profile tests are storage-owned. The process supplies only deployment endpoint, trust-root, certificate-identity, listener, and lifecycle inputs. |
| Control-plane durable state, RPC, and auth envelope | `storage` | Complete: client and server transport are contained behind typed Unix/TLS endpoints and opaque storage-owned facades. |
| Raft peer protocol, restart artifact, and WAL | `storage` | Peer wire and durable representations are contained: raw frames, restart artifacts, WAL records/files, and layout helpers are private; process tests use logical clients and opaque semantic recovery inspection. |
| PG topology, route state, and physical payload placement | `storage` | Complete containment: production request, administration, placement, maintenance, and local debug paths use opaque storage-owned capabilities or owner-rendered diagnostics. Direct `PgId`, checkpoint-summary, object-snapshot, and placement-error access is rejected in `server-http`. |
| Physical storage maintenance workflows | `storage` | Complete: shard scavenging, repair, backfill, payload reclaim, accepted bucket-delete continuation/finalization, and abandoned stream-session cleanup run behind opaque storage-owned workers; their durable cursors, claims, work records, cleanup roots, and debug snapshots are private. |
| Control-plane topology and metadata-transfer workflows | `storage` | Complete containment: topology commands, PG fencing, route/proof interpretation, metadata transfer, convergence, certification, durability publication, and retry classification are storage-owned. The process supplies logical operator or deployment inputs through opaque administration and host capabilities. |
| Storage implementation-error taxonomy | `storage` | Complete containment: storage exhaustively classifies operation failures, and `StoreFailure` retains only a semantic request class plus a bounded operator category rather than the implementation error. `StoreError`, `MetadataError`, `ObjectPgActionError`, `ShardIoError`, and their module are crate-private; public operations expose only logical or bounded opaque failures. |
| Object user/system metadata blobs | `server-core` | Complete: serialization is crate-private and storage carries only opaque validated blobs. |
| Tag and ACL canonical value formats | `s3-types` | Complete: validation and canonical codecs are centralized; storage owns and validates their containing row, command, checkpoint, digest, and RPC formats. |
| Object encryption state | `storage` | Complete: the durable codec is private and callers receive only typed encryption state. |
| Session-token envelope | `auth` | Exact-current version selection and representation constants are private to `auth`; callers use semantic credential issuance and authentication APIs, enforced by the boundary check. |
| Static manifest and process identity files | `argmin-s3` | The outer manifest and process-identity codecs remain process-owned. Storage validates and interprets the manifest's storage-topology subdocument through its own builder and returns opaque certified topology/bootstrap capabilities; raw PG topology no longer crosses back into the process workflow. |
| Shared operator metric schema | `observability` | Excluded from this storage-upgrade boundary: current metrics are neither persisted state nor an internal wire format, and no stable external metric-schema contract exists pre-release. If one is declared later, `observability` owns a separate compatibility plan. |

These are the current owners, not placeholders shared between crates. A later extraction into
a dedicated crate would be a deliberate ownership transfer: move the complete private
representation and logical facade together, update the dependency direction, and remove the
old owner's access in the same change. Until then, `storage` exclusively owns the
control-plane and Raft formats, and `argmin-s3` exclusively owns the static manifest and
process-identity formats.

### Nested Durable Codec Inventory: Object Metadata (2026-07-28)

`server-core` owns both object-metadata codecs. Its public logical surface is `MetadataBlob`,
`MetadataEntry`, `SystemMetadata`, and `ObjectChecksumMetadata`; byte serialization and parsing
are crate-private. `storage` carries `SerializedMetadataBlob` and
`SerializedSystemMetadataBlob` as opaque values through persistence, commands, and RPCs, but
does not interpret their inner versions, fields, or errors. No other crate may construct or
inspect those serialized carriers directly.

Both nested formats require their exact current version:

- User metadata version 1 has a self-inclusive little-endian `u32` length, a little-endian
  `u16` entry count, and ordered UTF-8 key/value pairs with little-endian `u16` lengths.
- System metadata version 1 has a little-endian `u16` field bitmap, its optional typed header
  values in fixed bit order with little-endian `u16` lengths, and an optional checksum record.

The owner rejects unsupported versions, unknown system-field bits, incomplete input, declared
length mismatches, bytes left inside a declared user-metadata frame, and bytes after either
complete representation. Empty bytes are not a representation of empty system metadata; the
current empty encoding is the explicit version-and-zero-flags frame. Exact-byte owner-local
goldens pin both empty formats and representative full field order, enum tags, endianness, and
length encoding. User-metadata construction and decode share one canonical-entry validator for
the lowercase `x-amz-meta-` token-name and stored Latin-1/header-value domain, so corrupt entries
cannot be silently hidden by response filtering. The system-metadata baseline separately pins all
ten checksum-algorithm tags, both checksum-type tags, and the absent-type sentinel.

The same nested bytes are embedded in these storage-owned containing formats:

| Containing format | Current baseline | Metadata embedding |
| --- | --- | --- |
| PG SQLite schema | schema version 6 | `objects`, `multipart_uploads`, and `stream_uploads` store user and system metadata blob columns. |
| Metadata command | encoding version 8 | Object, multipart-upload, and stream-session command values carry both opaque blobs. |
| Storage-node RPC | frame encoding version 22 | Logical object, multipart, and stream request/response payloads carry both opaque blobs. |
| Canonical PG state | encoding version 5 | The metadata columns participate in canonical row and state digests. |
| Metadata command checkpoint | encoding version 2 | Checkpoint table blocks carry the metadata columns and bind them into row, table, state, and checkpoint digests. |

Changing either metadata encoding requires a new inner version and coordinated advancement of
every containing format that can persist, replay, hash, or transmit the changed bytes. There are
no old-version or prefix-decoding fallbacks. The separately encrypted checksum projection used
by SSE-C and SSE-S3 is a private `server-core` version-1 codec nested inside the storage-owned
encryption state; changing it also requires advancing both encryption-state versions and all of
their containing versions.

### Nested Durable Codec Inventory: Object Tags (2026-07-28)

`s3-types` owns the shared AWS tag grammar and logical `TagKey`, `TagValue`, `Tag`, and `TagSet`
types. The service layer chooses only the operation-specific cardinality: object tag sets permit
at most `MAX_OBJECT_TAGS` (10), while bucket-resource tag sets permit at most `MAX_BUCKET_TAGS`
(50). HTTP request parsers may accept the AWS request spellings established by their service
oracles, but they normalize successful requests into the shared logical types before calling
`server-core`.

The current stored object-tag representation is the `ARGMIN-TAGSET/1\n` frame containing the exact
private `TagSet::to_xml()` payload: an XML 1.0 UTF-8 declaration, newline, S3-namespaced
`Tagging`/`TagSet` envelope, ordered `Tag` members, and canonical escaping.
`storage::SerializedTagSet` wraps the opaque owner carrier and its validated logical value.
External callers can construct it only from a logical `TagSet` and can inspect only that logical
value. Owner, PG, metadata-command, and storage-RPC decoders reject unframed, unsupported,
malformed, and well-formed-but-noncanonical values; request-parser tolerance therefore cannot
create additional durable representations. Exact owner-local goldens pin empty, Unicode,
escaping, and both cardinality profiles.

The object-tag XML is embedded in these storage-owned containing formats:

| Containing format | Current baseline | Object-tag embedding |
| --- | --- | --- |
| PG SQLite schema | schema version 6 | `objects`, `multipart_uploads`, and `stream_uploads` store optional framed object tags. |
| Metadata command | encoding version 8 | Object, multipart-upload, and stream-session command values carry the opaque tag set. |
| Storage-node RPC | frame encoding version 22 | Logical object, multipart, stream, mutation, and tag-read payloads carry the opaque tag set. |
| Canonical PG state | encoding version 5 | The tag columns participate in canonical row and state digests. |
| Metadata command checkpoint | encoding version 2 | Checkpoint table blocks carry tag columns and bind them into row, table, state, and checkpoint digests. |

The [format ledger](../../guides/storage-format-ledger.md) records the `s3-types`-owned inner frame rather than treating these storage
containers as the permanent tag version boundary. The coordinated initial container-version
advance is implemented without a legacy or prefix fallback.

Bucket tags use the same `s3-types` logical values and framed `StoredTagSet` representation,
with their separate 50-tag cardinality enforced by `storage::SerializedBucketTagSet`. The public
storage API accepts and returns that typed carrier; generic string-based reads and deletes accept
only `OpaqueBucketSubresourceKind`, which cannot represent tagging. The full persisted
subresource discriminator, auxiliary data, and generic stored row are private to `storage`.
Metadata-command and storage-RPC mutation decoders discriminate tagging before accepting its body,
and PG, snapshot, command, and RPC reads require the exact current XML rather than merely
well-formed tag XML. Metadata-checkpoint export and installation also verify tagging rows through
the same storage-owned row decoder after validating row integrity, including the canonical body
and null auxiliary-data invariant for both live rows and tombstones. A checksum-valid impossible
tagging row therefore cannot cross that boundary. Exact owner-local representation tests and
malformed/noncanonical decoder tests pin this boundary.

The bucket-tag XML is embedded in these storage-owned containing formats:

| Containing format | Current baseline | Bucket-tag embedding |
| --- | --- | --- |
| PG SQLite schema | schema version 6 | `bucket_subresources` stores framed bucket tags under the private tagging discriminator. |
| Metadata command | encoding version 8 | Bucket-subresource put/delete commands carry the discriminated typed tag mutation. |
| Storage-node RPC | frame encoding version 22 | Bucket snapshots, typed reads, and subresource mutations carry the opaque bucket-tag set. |
| Canonical PG state | encoding version 5 | The bucket-subresource body participates in canonical row and state digests. |
| Metadata command checkpoint | encoding version 2 | Checkpoint table blocks carry the bucket-subresource row and bind it into row, table, state, and checkpoint digests. |

The inner frame applies jointly to object and bucket tags because they share one logical tag
representation. Its first introduction advanced every affected container in one coherent slice.
Unframed XML and unsupported framed versions are rejected rather than retained as alternate
representations.

### Nested Durable Codec Inventory: ACL Grants (2026-07-28)

`s3-types` owns the logical `AclGrant`, `AclGrantee`, `AclPermission`, and `AclGrants` values and
the opaque `StoredAclGrants` durable carrier. Storage APIs carry logical grants or that opaque
carrier; HTTP and `server-core` neither produce nor consume durable text. Every value begins with
`ARGMIN-ACL-GRANTS/1\n`; the empty set has an empty payload, and each nonempty grant is one
newline-terminated `kind:value:PERMISSION` record in canonical sort order with duplicates removed.
Canonical-user IDs are lowercase. Exact goldens pin the empty frame and an all-grantee,
all-permission frame.

The owner carrier is the only encoder/decoder and rejects framing errors, unsupported versions,
malformed payloads, and every parseable but noncanonical spelling, including reordered or
duplicate grants, CRLF separators, normalized canonical-user IDs, and a missing final newline.
PG, metadata-command, and storage-RPC paths consume only the validated carrier.
Metadata-checkpoint export and installation find every `acl_grants` column in the storage-owned
table inventory and exact-decode it after row-integrity validation; checksum-valid noncanonical
ACL rows cannot be transferred.

The ACL representation is embedded in these storage-owned containing formats:

| Containing format | Current baseline | ACL embedding |
| --- | --- | --- |
| PG SQLite schema | schema version 6 | `buckets`, `objects`, and `multipart_uploads` store framed ACL carriers. |
| Metadata command | encoding version 8 | Bucket, object, multipart-upload, create, commit, and ACL mutation records carry framed ACL carriers. |
| Storage-node RPC | frame encoding version 22 | Logical bucket, object, multipart, stream-commit, and ACL mutation messages carry framed ACL carriers. |
| Canonical PG state | encoding version 5 | The three persisted ACL columns participate in canonical row and state digests. |
| Metadata command checkpoint | encoding version 2 | Checkpoint table blocks carry all three ACL columns and bind them into row, table, state, and checkpoint digests. |

The [format ledger](../../guides/storage-format-ledger.md) records the `s3-types`-owned inner frame. Its first introduction advanced
every affected container together; the empty ACL is an explicit current frame with an empty
canonical payload. Unframed strings and unsupported framed versions are rejected rather than
accepted through a fallback.

### Nested Durable Codec Inventory: Object Encryption State (2026-07-28)

`storage` owns the object-encryption discriminator and durable byte encoding. Its public logical
surface is `ObjectEncryption` plus the typed `SseCustomerObjectState` and `SseS3ObjectState`
constructors and cryptographic-component accessors. `server-core` owns the encryption operations
that consume and produce those typed values, but it does not select a persisted discriminator,
encode or decode bytes, inspect an encoding version, construct a concrete state layout directly,
or receive storage decode errors.

The private nested encoding currently has exact current-version decoding only:

- SSE-C state version 3 contains the validator-key identity and proof, customer-derived wrapping
  inputs, wrapped object DEK, segment nonce prefix, and sealed-checksum state.
- SSE-S3 state version 1 contains the managed wrapping-key identity, wrapping inputs, wrapped
  object DEK, segment nonce prefix, and sealed-checksum state.
- discriminator values 0, 1, and 2 select unencrypted, SSE-C, and SSE-S3 state respectively.

The same nested bytes are embedded in these storage-owned containing formats:

| Containing format | Current baseline | Encryption-state embedding |
| --- | --- | --- |
| PG SQLite schema | schema version 6 | `objects`, `multipart_uploads`, and `stream_uploads` store `encryption_type` plus `encryption_state`. |
| Metadata command | encoding version 8 | Object, multipart-upload, and stream-session command values carry the discriminator and nested state bytes. |
| Storage-node RPC | frame encoding version 22 | Logical object, multipart, and stream request/response payloads carry the discriminator and nested state bytes. |
| Canonical PG state | encoding version 5 | The three table representations above include both encryption columns in canonical digests. |
| Metadata command checkpoint | encoding version 2 | Checkpoint table blocks carry the raw encryption columns and bind them into row, table, state, and checkpoint digests. |

Changing either nested encryption encoding requires an explicit new inner version and coordinated
advancement of every containing format that can persist, replay, hash, or transmit the changed
bytes. Because upgrades are unsupported, current decoders reject every non-current inner or outer
version; this containment work does not add fallback readers. Malformed nested-state tests remain
inside `storage`, while cross-crate tests exercise only logical encryption behavior. Storage also
enforces the nested checksum-metadata length when logical state is constructed, so every public
state is encodable, and exact-byte goldens pin the discriminator, inner version, field order,
endianness, and length encoding for all three encryption variants. It does not yet require the
writer's canonical zero nonce when the encrypted checksum payload is empty.

The sealed checksum payload is a separate nested format audited in the
[format ledger](../../guides/storage-format-ledger.md). A change to its
self-versioned plaintext frame advances that inner version plus both outer encryption-state
versions and their containing formats under the coordinated nested-format rule above. A change to
the untagged AES-GCM algorithm, nonce/tag sizes, AAD domain, or canonical absent-ciphertext
representation advances the applicable SSE-C or SSE-S3 outer state version, because no inner
profile selector can be read before authentication. Enforcing the current writer's existing zero
nonce plus empty ciphertext as the sole accepted absent representation is decoder hardening, not a
representation change, and preserves SSE-C v3 and SSE-S3 v1. The existing rule advances containing
formats only when the canonical representation itself changes.

### RPC Boundary Inventory (2026-07-27)

This inventory covers the storage-node, control-plane, and Raft peer RPC surfaces. All three
protocols are owned by `storage`. `argmin-s3` owns process configuration and lifecycle, but it
now passes typed endpoint/listener and credential configuration to storage-owned clients and
servers and does not implement their wire exchanges. No separate protocol crate is justified by
the current dependency graph.

The protocols currently assume identical binaries. They have exact current-version rejection,
but no version negotiation or supported compatibility window:

| Surface | Current wire baseline | Authentication baseline | Negotiation and current disposition |
| --- | --- | --- | --- |
| Storage-node RPC | `STORAGE_RPC_FRAME_ENCODING_VERSION = 22` in `storage_rpc.rs`; frame magic, message-kind tags, checksums, and payload codecs are crate-private. | Binding version 2 and transport-envelope version 1 in `storage_rpc_auth.rs`. | Exact versions are required before dispatch. There is no negotiation. Treat any other version as incompatible until mixed-version operation is designed. |
| Control-plane RPC | `CONTROL_PLANE_RPC_VERSION = 17` in `control_plane.rs`; the frame contains magic, version, request kind, length, checksum, and payload. | Shared control-plane authentication-envelope version 1 in `control_plane_auth.rs`. | The frame and auth decoders reject non-current versions before logical dispatch. There is no negotiation. Treat any other version as incompatible. |
| Raft peer RPC | `CONTROL_PLANE_RAFT_PEER_RPC_VERSION = 3` in `control_plane_raft.rs`; request, response, snapshot, peer-identity, checksum, and numeric OpenRaft tags share this baseline. | Shared control-plane authentication-envelope version 1, with the authenticated operation and peer identity bound to the inner frame. | The decoder rejects non-current versions before OpenRaft dispatch. There is no negotiation, and OpenRaft peers currently require the same binary. Treat any other version as incompatible. |

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
  values. Storage privately maps wire-specific failures into the exhaustive public
  `StoreOperationFailureClass`; `StorageNodeFailureClass` and
  `StoreError::storage_node_failure_class()` are crate-private. `server-core` selects S3 request
  policy from `StoreOperationFailureClass` without knowing wire codes, while `argmin-s3` receives
  only its operation-specific opaque failures. Remote diagnostic text remains inside the private
  `StorageNodeFailureDetail` while a raw storage error is owner-local; conversion to
  `StoreFailure` retains only the semantic request class and one bounded operator category.
  Raw-code mapping, classification, and redaction tests are storage-owned, and the repository
  boundary check prevents wire types, private storage-node classification, opaque diagnostic
  values, or their fields from being used outside storage.
- Storage-node TLS profile containment is complete. `storage` constructs and validates the fixed
  TLS 1.3 client/server profiles and private `argmin-storage-rpc/1` ALPN inside typed endpoint and
  listener constructors. `argmin-s3` supplies only addresses, server names, trust roots,
  certificate identities, listener bindings, and lifecycle configuration. The ALPN constant and
  raw Rustls profiles are private, and the repository boundary check rejects profile construction
  or protocol-identifier use outside storage.
- Retry policy remains intentionally caller-owned while representation translation is
  storage-owned. `server-core` makes exhaustive S3-policy decisions over
  `StoreOperationFailureClass`; storage-internal protocol handling alone may inspect wire codes,
  `StorageNodeFailureClass`, or retained remote diagnostic text. Process operations receive their
  own storage-owned semantic failure surfaces rather than the storage-node class.
- Raw codec, malformed-frame, authentication, client, and server tests are already in `storage`.
  Cross-crate S3/process tests use logical storage operations or typed endpoint/listener
  configuration; exact TLS/ALPN profile assertions are owner-local.

#### Control-plane RPC

- The intended logical client boundary is `UnixControlPlaneClient` or
  `AuthenticatedUnixControlPlaneClient` through the control-plane admin, heartbeat, runtime-map,
  and authority-clock traits. The authority implementations and typed snapshots/results also
  belong to `storage`.
- The client transport containment slice is complete. Static configuration now supplies
  `ControlPlaneRpcClientEndpoint` values containing Unix paths or TLS/TCP host, port, server-name,
  connect-timeout, and trust-root inputs. `storage` owns DNS/connect deadlines, TLS profile and
  ALPN construction, framing, frame limits, request-publication tracking, response decoding, and
  endpoint failover. The former public `ControlPlaneRpcFrameTransport`, raw exchange value/error,
  and `with_frame_transport` extension are removed. Raw transport tests are storage-owned, and a
  repository check prevents those client-wire abstractions from returning outside `storage`.
- **Completed containment slice:** `ControlPlaneRpcServerListener` and
  `ControlPlaneRpcServerPolicy` now form the storage-owned server facade. `argmin-s3` binds the
  configured socket and supplies endpoint limits, credentials, certificate material,
  authority-clock state, and semantic durability/authority callbacks. Storage owns TLS 1.3 and
  ALPN profile construction, absolute Unix and TLS handshake/request-ingress deadlines, fresh
  absolute response-egress deadlines after dispatch/publication, worker and pre-auth byte
  admission, endpoint-role admission, authentication ordering, verified dispatch, response
  framing/finalization, and bounded write-error metrics.
- The raw control-plane request/response types, verified-request type, ALPN constant, one-shot
  stream handlers, and raw read/verify/build/write helpers are private again. Their protocol,
  malformed-input, resource, TLS/ALPN, and authentication tests are colocated in `storage`;
  binary tests retain only process lifecycle/configuration behavior through logical clients or the
  opaque facade. A repository check rejects reintroduction of these concrete server-wire symbols
  outside `storage`.
- `ControlPlaneError` currently mixes logical authority failures with public `Io`, `RpcProtocol`,
  and string-valued `RpcRemote` wire/transport failures. The storage clients own much of the
  retry logic, including read-only endpoint failover and operation-specific response-loss
  confirmation. The first error-containment slice removes cross-crate I/O destructuring and
  rendered-error parsing for leader routing, runtime-map observation, and frontend/storage-node
  startup retries. Frontend startup retains a typed fetch-or-readiness error until retry policy is
  applied; storage-node heartbeat startup likewise asks storage for semantic classification before
  rendering its terminal diagnostic. Authority readiness, authority-clock routing, unknown-node,
  and unknown acting-set-node failures retain typed identities across the RPC boundary; storage
  owns the retry classification and a boundary check prevents raw transport matching or the former
  startup format-then-parse helpers outside the crate. The follow-on slice removes every
  `argmin-s3` construction of `Io`, `RpcRemote`, and `RpcProtocol`: local configuration and missing
  command capabilities remain local errors, while durability, invariant, startup-timeout, and
  static-topology failures use explicit semantic variants whose retained diagnostics have redacted
  `Debug` and `Display` implementations. Static identity establishment preserves a typed split
  between identity/topology validation and durable inspection or publication failure, so filesystem
  replacement and synchronization failures remain durability failures. The repository check now
  rejects any use of the three raw variants outside `storage`, rather than only destructuring them
  for policy. The raw variants now carry public wrapper types with private storage-owned state:
  callers may identify `Io`, `RpcProtocol`, or `RpcRemote`, but cannot recover the retained I/O
  context, operating-system error, or RPC text. Their public `Debug` and `Display` output is
  redacted, and `Io` deliberately does not expose the underlying error through `Error::source()`.
  Storage-private constructors and accessors preserve owner-local transport classification and
  malformed-wire diagnostics without relying on the repository check for payload opacity. Static
  configuration also validates bootstrap replication safety through an opaque semantic error;
  encoded-entry diagnostics no longer cross into `argmin-s3` configuration errors. Storage-owned
  semantic reclassification retains the original error as an opaque cause, including when the
  process layer classifies restart-checkpoint failures as durability failures; public formatting
  and `Error::source()` remain redacted.
- Storage contains the codec, version, authentication, retry, resource-admission, TLS/ALPN, and
  client/server protocol tests. Process-level lifecycle and durability tests remain in
  `argmin-s3`, but use logical clients and the opaque storage-owned server facade.

#### Raft peer RPC

- The intended logical boundary is `ControlPlaneRaftAuthority` and its typed bootstrap,
  lifecycle, linearized-command, status, and leader-routed-admin handles. The OpenRaft network,
  peer server, authentication policy, and durability-before-ack behavior are all storage-owned
  protocol concerns.
- **Completed client containment slice:** `ControlPlaneRaftPeerClientEndpoint` accepts deployment
  addresses, TLS server names, and trust roots for Unix or TLS/TCP peers. Storage owns connection
  deadlines, TLS 1.3 and ALPN profile construction, framing and allocation limits, absolute I/O
  deadlines, and concrete transport-error classification into OpenRaft `Unreachable` or `Network`.
  The former public frame-transport extension and exchange value/error are private, their tests
  are storage-owned, and a repository check prevents their return outside `storage`.
- **Completed server-facade slice:** `ControlPlaneRaftPeerServerListener` and
  `ControlPlaneRaftPeerServerPolicy` own Unix/TLS listener transport, TLS 1.3 and ALPN profile
  construction, absolute handshake/request/dispatch deadlines and a fresh absolute response
  deadline installed after publication admission, worker admission, the shared pre-authentication
  allocation budget, authentication and peer admission ordering, OpenRaft dispatch, response
  signing/framing/finalization, and exact-once publication. The
  process supplies bound listeners, certificate material, logical deployment inputs, and an opaque
  durability callback. Storage decides when snapshot responses require checkpointing and ensures
  that checkpointing precedes every possible response write. The raw listener and policy are now
  crate-private behind the peer-bootstrap capability described below.
- **Completed representation-containment slice:** peer request/response/snapshot types,
  frame-kind and identity values, codecs, transport read/write helpers, raw frame handlers, the
  shared auth-envelope representation, the ALPN constant, and the underlying OpenRaft `Raft`
  handle are private to `storage`. Cross-crate process tests use an opaque semantic peer test
  client for votes and command appends, including an opaque pending response for crash races;
  binary durability tests use owner-provided snapshot, step-down, and election hooks rather than
  OpenRaft types.
- **Completed durable-format containment slice:** restart artifacts, state-machine/log-store
  restart values, WAL frames/records/files, replay configuration, and WAL path derivation are
  private to `storage`. Public durable authority constructors accept only the configured restart
  path and derive the owned WAL path internally. Checkpoint publication is likewise bound to that
  authority path, so a caller cannot publish an artifact independently of the WAL that will be
  compacted. Companion paths append their suffixes through `OsString`, preserving non-UTF-8
  artifact names without fallback-name collisions. Cross-crate crash tests inspect an opaque
  semantic recovered/checkpoint state; tests that manufacture committed-ahead artifacts or append
  synthetic WAL suffixes are colocated with the storage implementation. An owner-local restart
  test places a committed command only in the derived WAL and proves that the public durable
  authority applies it to the live state machine.
- Client retry classification is storage-owned and permanently tests that reachability failures
  map to OpenRaft `Unreachable` while protocol failures map to `Network`. The inbound server
  returns only an opaque terminal listener failure to the process; request and transport
  diagnostics remain storage-owned.
- Raw codec, version, identity-binding, authentication, transport, OpenRaft dispatch,
  poison-before-dispatch, response-loss durability, state-machine-isolation, TLS/ALPN,
  admission-budget, checkpoint-ordering, and response-publication tests are colocated in
  `control_plane_raft.rs`. The duplicate binary raw-frame, malformed-envelope, and direct-dispatch
  tests are removed. Process lifecycle, crash, and durability-observation tests remain in
  `argmin-s3`, using logical or opaque owner-provided facilities. A repository check rejects raw
  Raft frames, handlers, auth envelopes, ALPN, and `.raft()` access outside `storage`.

This completes the RPC inventory only; it does not satisfy Phase 1 containment. The bounded
implementation order is:

1. **Complete:** replace the control-plane raw client frame-transport extension with
   storage-owned Unix and TLS/TCP endpoint configuration. Static-manifest parsing remains in
   `argmin-s3`; frames, client ALPN construction, request-sent tracking, and transport error
   construction are storage-owned.
2. **Complete:** add a storage-owned control-plane server facade that accepts listener,
   resource-limit, authentication, authority-clock, and durability-publication configuration
   while owning TLS, frame admission, verification, dispatch, response framing, and transport
   diagnostics. Raw control-plane server symbols and ALPN are private and boundary-checked.
3. **Complete:** replace the Raft raw client frame transport with storage-owned Unix and TLS/TCP
   peer endpoint configuration and owner-defined OpenRaft transport classification.
4. **Complete:** add a storage-owned peer server facade that preserves the existing pre-auth
   allocation bound and durability-before-ack invariant. TLS/ALPN, worker admission, authenticated
   dispatch, response signing and finalization are storage-owned and boundary-checked.
5. **Complete:** make the remaining raw Raft frame, auth-envelope, ALPN, OpenRaft-handle, and
   transport-error APIs private; retain process coverage through logical or opaque semantic test
   facilities; and enforce the wire boundary with a repository check. The equivalent
   control-plane client and server cleanup is also complete.
6. **Complete:** make restart-artifact and WAL representations, replay/layout helpers, and
   explicit-WAL constructors private; derive the WAL location inside storage; relocate direct
   durable-format and impossible-state tests into `storage`; and boundary-check these symbols.

### Storage Topology And Maintenance Containment Audit (2026-07-30)

Commit `c0289530640d83be81f4bddbe30dd7bbbc5779b3` correctly fixed durable-backfill
route convergence inside storage, but its fairness fix retained `Option<PgId>` in
`server-core` and passed it back into a storage claim scan. That cursor is storage-owned scan
state, not S3 coordination state. The narrow fix is not to replace `PgId` with `u32`: the owner
must retain the state behind an opaque logical capability.

The follow-up audit found these related live production leaks:

| Leak | Current non-owner behavior | Required owner boundary |
| --- | --- | --- |
| Backfill fairness and claims | `server-core` retains the last claimed PG, constructs claim-acquire parameters, inspects work-item PG/epoch/EC fields, and drives acquire/backfill/complete/error transitions. | A storage-owned backfill worker/session retains its private cursor and claim state. Callers start, stop, and wake it through an opaque capability. |
| Shard scavenger and repair | `server-core` scans candidates, owns claim leases and retry policy, interprets physical failure variants, and records storage queue transitions. | Move physical shard maintenance state machines and their admission/retry classification into `storage`; make claim, cursor, and work-record representations private. |
| Payload reclaim and bucket finalization | `ReclaimSweeper` in `server-core` owns durable PG scanning, physical payload-reclaim queues, deferred roots, per-PG cooldown, bucket-delete continuation/finalization work, and retry classification. | Storage owns discovery, queueing, adoption, physical reclaim, asynchronous bucket cleanup/finalization, cleanup roots, and retry scheduling after a logical delete has been accepted. `server-core` retains the S3-visible delete request, preconditions, response, and lifecycle-policy decisions and invokes one logical accepted-deletion operation. |
| Abandoned stream-session cleanup | `StreamSessionSweeper` in `server-core` directly schedules and invokes storage session scavenging. | Storage owns stream-session expiry, discovery, cleanup, admission, and telemetry behind its maintenance runtime; this internal session lifecycle is not an S3-visible policy. |
| Physical object payload I/O | The read path now uses an opaque storage-owned payload-segment handle, but write/commit paths still carry data PG IDs, placement epochs, and EC `k/m`; public physical request and record representations remain pending containment. | Storage returns an opaque persisted payload-segment handle and owns placement, leases, reconstruction, historical routing, and physical read/write requests. `server-core` retains S3-visible byte-range, checksum, and encryption semantics. |
| Process control-plane orchestration | `argmin-s3` constructs `ControlPlaneCommand`, inspects `PgRouteSnapshot`/`PgState`/acting sets, and implements PG fencing plus live metadata-transfer convergence. | A storage-owned control-plane/admin facade owns topology commands and the complete metadata-transfer state machine. The process supplies lifecycle, endpoint, credentials, and operator inputs only. |
| Static topology configuration | `argmin-s3` parses and stores `Vec<(PgId, Vec<NodeId>)>` and performs storage placement interpretation. | Keep the outer manifest in `argmin-s3`, but hand its storage-topology subdocument or logical configuration inputs to a storage-owned validator/builder without exposing PG types. |
| Storage failure handling | Complete: `server-core` receives exhaustive semantic operation failures and bounded storage-owned diagnostic categories, never concrete storage implementation errors. | Keep the raw error taxonomy and conversion inside `storage`; public operations expose only logical values or opaque failures with bounded labels and redacted formatting/error chains. |
| HTTP debug operations | `server-http` parses a PG ID, obtains `StorageCluster`, invokes checkpoint operations, and formats storage snapshots. | Debug formats may remain unstable, but the operation and formatting must be owner-provided and opaque; debug status does not waive crate ownership. |
| Storage-specific observability | `server-core` emits repair/backfill events using PG IDs because it owns the leaked workers. | Storage emits its own topology/maintenance telemetry. `observability` may remain a generic sink, but another crate must not consume those dimensions to make storage decisions. |

Permitted cross-boundary values are deliberately narrower:

- `StorageClusterRouteHandle` and route-admission values remain opaque storage capabilities. A
  caller may retain or pass them but may not inspect a PG, route, generation lock, or publication
  domain through them.
- Process configuration may name nodes, endpoints, credentials, certificate identities, and
  listener bindings. Node identity as deployment configuration does not grant the process layer
  authority to construct acting sets or interpret PG transition state.
- `server-core` owns S3 semantics, authorization, lifecycle rule evaluation, S3-visible bucket
  deletion decisions and responses, object-visible byte ranges and checksums, and encryption
  request behavior. Once a logical deletion is durably accepted, storage creates and owns its
  cleanup roots and physical reclaim/finalization. `server-core` does not own physical placement,
  erasure-shard routing, historical route selection, storage-session scavenging, or storage
  recovery.
- Process and HTTP tests use logical operations or opaque owner-provided test/debug facilities.
  Impossible topology, claim, route, or physical payload states remain in storage tests.

The bounded implementation order is:

1. Move shard scavenger, repair, backfill, payload reclaim, asynchronous bucket cleanup/finalize,
   and abandoned stream-session worker state machines into a storage-owned maintenance runtime.
   Include the `c0289530` fairness cursor, durable PG scans, deferred queues and roots, per-PG
   cooldown, candidate scans, claims, leases, admission classes, retries, completion, expiry, and
   storage-specific telemetry. Keep lifecycle-rule evaluation and the S3-visible bucket-delete
   request/precondition/response path in `server-core`; invoke a logical accepted-deletion
   operation from which storage creates and owns all durable cleanup roots.
2. Make the maintenance cursor, claim, reclaim-work, cleanup-root, session-cleanup, work-record,
   and acquire/complete APIs crate-private once no external worker consumes them.
3. Replace physical segment fields and requests in `server-core` with opaque storage-owned payload
   handles and logical lease/read/write operations. Retain encryption transformation at the
   appropriate S3/storage seam without exposing placement.
4. Move deterministic static storage-placement interpretation behind a storage-owned builder so
   subsequent administration receives owner-validated topology input. Then move control-plane
   topology transitions and live metadata-transfer orchestration behind a storage-owned
   admin/service facade, and contain the remaining static certificate/bootstrap assembly while
   leaving outer manifest/process configuration in `argmin-s3`.
5. Replace cross-crate `StoreError` destructuring with exhaustive storage-owned semantic
   classifications and opaque diagnostics. Error translation into S3 outcomes remains contextual
   in `server-core`, but cannot depend on PG, database, shard, route, or wire variants.
6. Replace direct HTTP storage debug access with an opaque coordinator/storage diagnostic facade.
7. Make `PgId`, `PgState`, route snapshots, physical segment requests, and maintenance records
   non-public outside `storage` wherever no remaining logical administration surface requires
   them. Extend `check-storage-cluster-boundaries` during migration, then rely primarily on Rust
   visibility to prevent recurrence.

Progress on item 1 (2026-07-30): the first bounded slice moved durable backfill-candidate scan
state into `StorageBackfillCandidateScanner`. Its fairness cursor, candidate summary, enqueue
transition, implementation errors, and storage-specific telemetry are now storage-owned. The
public scheduler operation is only `scan()`, and the exact cursor/publication regression moved
from `server-core` to an owner-local storage test. The second bounded slice moved abandoned
stream-session expiry, discovery, cleanup, shared-worker registration, thread lifecycle, retry
diagnostics, and telemetry into `StorageStreamSessionSweeper`; `server-core` retains only the
opaque worker handle and startup-error translation. The route-publication regression now uses a
clock pinned before setup and advances it deterministically past the durable cleanup deadline.
The third bounded slice moved shard-audit scheduling, backfill-candidate discovery, routine
metadata-checkpoint scheduling, their shared pressure/admission policy, worker registration,
thread lifecycle, and storage-specific telemetry into `storage`. The raw audit and checkpoint
operations are now crate-private, the integration harness receives only an owner-formatted test
diagnostic, and the file-without-row impossible-state regression is owner-local. Admission,
shard-scavenger, and stream-session registries are keyed by route-publication domain rather than
process-local storage identity, so independent domains over one initial cluster cannot share a
worker and refreshing one domain cannot strand the other. Repair/backfill execution and
reclaim/finalization remained pending after that slice, so it did not mark item 1 complete.

Progress on item 1 (2026-07-31): the fourth bounded slice moved durable shard-repair scanning,
wake-queue consumption, claim identity and lease creation, known-damage admission, physical repair,
retry recording, completion, worker registration, thread lifecycle, diagnostics, and telemetry into
`StorageShardRepairSweeper`. `server-core` retains only the opaque worker and startup-error
translation. The repair registry uses route-publication-domain identity, remains shared across
storage-identity replacement, and stays distinct for independent publication domains over the same
initial cluster. Raw repair queue, claim, retry, completion, and preserving-repair-row execution
operations and repair work/claim representations are now crate-private. Physical single-shard,
selected-shard, and repair-if-needed mutation helpers are compiled only for owner-local storage
tests. Cross-crate integration tests receive dedicated test-only DTOs rather than the production
repair records. Worker regressions call the same storage-owned
single-step state machine as production and use a pinned clock for retry eligibility instead of
polling worker timing. Backfill execution and reclaim/finalization remain pending, so item 1 is not
yet complete.

The fifth bounded slice moves durable shard-backfill claim scanning and its fairness cursor, claim
identity and leases, risk-based admission, physical copy/reconstruction, obsolete-source
resolution, stale-route retry classification, durable error/completion transitions, worker
registration, lifecycle, diagnostics, and telemetry into `StorageShardBackfillSweeper`.
`server-core` retains only the opaque worker and sanitized startup-error translation. The worker
registry is keyed by route-publication domain, remains shared across storage-identity replacement,
and remains distinct for independent publication domains over the same initial cluster. Raw
backfill records, claims, plans, queue transitions, source-reference inspection, and physical
mutation entry points are crate-private; owner-local tests may use the raw transitions while
cross-crate composition tests receive dedicated test-only DTOs and a deterministic single-step
worker facility. Reclaim and asynchronous bucket cleanup/finalization remain pending, so item 1 is
not yet complete.

The sixth bounded slice moved durable reclaim scanning, queue ownership, deferred-root fairness,
per-PG and per-root cooldowns, physical object-payload reclaim, adopted bucket-delete continuation,
asynchronous bucket finalization, retry classification, worker registration, lifecycle,
diagnostics, and telemetry into `StorageReclaimSweeper`. The worker registry is keyed by the opaque
route-publication domain: it remains shared across storage-identity replacement and distinct for
independent domains over the same initial cluster. Reclaim scan batches, work items,
bucket-delete cleanup roots, queue transitions, physical execution methods, and wake/poll methods
are crate-private. Cross-crate composition tests use explicitly named test-only DTOs and methods;
the boundary checker rejects production raw representations or transitions outside `storage`.
With this slice, implementation-order item 1 is complete.

The seventh bounded slice completes implementation-order item 2. Durable payload-reclaim and
bucket-finalizer claims, reclaim roots and records, attempt outcomes and phases, and the structured
bucket-delete debug snapshot are crate-private. The local HTTP debug endpoint receives only an
opaque `BucketDeleteDiagnostic` rendered by `storage`; its populated impossible-state formatting
golden is owner-local. Cross-crate composition tests use explicitly `Test`-prefixed fixture DTOs
and logical progress observations compiled only with test hooks. The boundary checker rejects raw
maintenance records, claims, roots, outcome records, and debug snapshots outside `storage`, and
also rejects making those owner representations public again.

The eighth bounded slice begins implementation-order item 3 with the object read path. Storage
validates persisted object and multipart segment rows against the live object subject and converts
them into opaque `ObjectPayloadSegment` values before the snapshot crosses the crate boundary;
`server-core` can inspect only logical segment index, size, and multipart membership. Each handle
is bound to its bucket, key, and object generation, so logical lease acquisition rejects a handle
crossed with another subject. Storage owns expansion to physical shard locations,
deletion-exclusion lease acquisition, placement-epoch and historical-route selection, ciphertext
size adjustment, and construction of the stored-byte request. Retained streaming read authority
binds the full ordered opaque layout, including logical segment indices, and rejects changed or
omitted handles before a response reader is built. S3-visible byte-range assembly, whole-payload
checksums, and encryption transformation remain in `server-core`. Physical payload write/commit
fields and operations remain pending, so implementation-order item 3 is not yet complete. The
boundary checker rejects reintroduction of raw segment records, handle construction, PG,
placement-epoch, EC, shard-key, raw stored-byte-request, or shard-location lease handling into the
production coordinator read seam. That seam is an explicit inventory spanning its authorization,
object-state, infrastructure/runtime, read/copy, payload-transformation, and response modules; the
checker also rejects inventory drift when another production module begins participating in the
read seam.

The ninth bounded slice continues implementation-order item 3 with buffered/direct PutObject.
`server-core` supplies only the encrypted payload bytes, their logical size, the reserved logical
generation, S3-visible object metadata, and the conditional-write callback. Storage derives the
direct-staging segment hash, integrity checksum, data PG, EC placement, shard acknowledgements,
placement epoch, and durable segment record, retaining them in an opaque subject-bound
`DirectPutPayloadWrite`. The armed RAII handle retains and is lifetime-bound to the exact issuing
cluster admission and publication domain; ordinary drop, explicit discard, and crossed-route
failure clean shards and the generation reservation through that issuer rather than the receiving
route. Commit disarms it only after finding an already durable result, adopting the exact matching
pending command, or installing the new pending command. Cleanup authority therefore cannot
survive a successful commit and delete live payload. The lower commit state machine separately
tracks caller-owned and durable-command-owned payload. A matching pending command adopts the
current payload only when its complete physical segment identity also matches; a reused logical
reservation cannot claim a different staged body. A logical match with different physical
identity fails before command recovery. Storage disarms the generic caller guard without
releasing the command-owned generation or write proof, deletes only demonstrably disjoint caller
staging on a per-shard-key basis, and preserves overlapping shard keys that may belong to the
pending command even when the remainder of the caller batch is removed. After
adoption, ambiguous abandoned-log
inspection failures and retryable partial-command outcomes preserve the shards and generation
reservation for recovery; cleanup occurs only while ownership is still caller-held or after the
command has been conclusively abandoned and removed. Commit and
explicit discard consume the linear handle, reject a handle crossed to another admission, bucket,
or key, and commit also binds it to the bucket-write reservation epoch. The raw
written-shard result, durable direct-commit request, direct hash helper, and physical mutation
entry points are private to storage; cross-crate maintenance composition uses an explicitly
test-only DTO. The boundary checker rejects their reintroduction into the production direct-PUT
seam or as public storage APIs. Streaming append/finalization and multipart physical write/commit
fields remain pending, so implementation-order item 3 is not yet complete.

The tenth bounded slice continues implementation-order item 3 with streaming segment append for
both PutObject and UploadPart. `server-core` supplies only the session, logical segment index,
the plaintext checksum and transformed storage bytes through `StreamSegmentAppendInput`; storage
derives the stored-byte checksum itself.
Storage reloads the session while the admitted route is valid, derives the logical size from the
session encryption state, derives the segment hash and generation, prepares the durable record,
selects the data PG and EC placement, writes and validates shard acknowledgements, and publishes
the append command as one logical operation. The result exposes only the logical upload target and
logical size needed for S3-facing tracing. The former admitted-route prepare/write/ack/commit
sequence is removed; raw physical steps and the segment-hash helper are owner-test-only. The
existing prepare-boundary race hook now enters through an explicitly test-only storage operation,
so abort, duplicate-append, route-expiry, and cleanup regressions still exercise the same internal
boundary without returning physical records to `server-core`. The boundary checker rejects
prepared segment records, shard keys/acks, placement fields, hashes, or raw mutation steps in the
production streaming coordinator seam. Stream finalization, multipart completion, and remaining
public physical record representations are still pending, so implementation-order item 3 is not
yet complete.

The eleventh bounded slice continues implementation-order item 3 with streaming UploadPart
finalization. The coordinator now supplies only the logical upload identity, part number, staged
byte count, staged plaintext CRC64, S3 checksum result, and response value. Storage reloads and
validates the exact session/upload/staging snapshot, derives the replacement generation and part
version, selects the part EC and placement epoch from owner-held staging state, converts staging
rows into committed multipart-segment rows, constructs the durable part record, and owns pending
command retry equivalence. The callback sees only the upload checksum configuration, managed
encryption algorithm, and storage-derived logical staged size and plaintext CRC64, so it can apply
S3 checksum policy without receiving the durable upload record or constructing or inspecting
physical records. The complete physical storage/RPC snapshots are crate-private, and
the boundary checker rejects their re-export or the return of staging, placement, EC, generation,
or segment fields to the production multipart coordinator. Multipart completion and remaining
public physical record representations are still pending, so implementation-order item 3 is not
yet complete.

The twelfth bounded slice continues implementation-order item 3 with multipart completion. The
completion snapshot now exposes only the existing logical ETag and an immutable per-part view of
part number, logical size, ETag, stored S3 checksum, and storage-derived plaintext CRC64. Storage
retains the durable part records, selected streaming segments, stale-object identity and payload,
replacement cleanup, generations, EC placement, and historical route state. After applying S3
part, checksum, size, conditional, metadata, and encryption policy, the coordinator supplies a
logical completion input; the opaque snapshot consumes that input and transfers its owner-held
physical state into the storage commit request. The storage RPC representation is unchanged: its
decoder reconstructs the logical view from the decoded durable part records, and its encoder still
writes only the existing physical snapshot fields; the client binds the bucket, key, upload ID,
and reserved final-object generation from the already-authorized upload context. The opaque
snapshot derives that complete request subject when it constructs the commit request, so none of
those identity fields can be supplied or crossed by the coordinator. Compiler visibility is the
primary boundary, the public `Debug` views are redacted to logical fields, and checks reject
physical multipart-completion types or fields in the production coordinator, publicly exposed
physical fields, or caller-supplied subject fields. Multipart initiation, listing, abort, and
remaining public physical record representations are still pending, so implementation-order item
3 is not yet complete.

The thirteenth bounded slice continues implementation-order item 3 with ListParts. Storage now
retains the complete durable multipart-upload record and physical part records, and exposes only
the upload identity fields and logical per-part number, size, S3 ETag, timestamp, and checksum
needed to render the response. The storage-owned projection validates each stored ETag rather than
allowing malformed durable bytes to become a zero ETag in the coordinator. The storage RPC bytes
remain unchanged: encoding uses the private physical records, while decoding reconstructs the
logical view before the result can cross the crate boundary. The raw list request/response types
and the former cross-crate test hook are private or removed; the coordinator regression now tests
through the supported ListParts operation. Compiler visibility is the primary boundary, with
checks rejecting physical listing types or fields in the production coordinator, public raw
request/response representations, and physical fields in the opaque result's `Debug` output.
Multipart initiation, upload-listing, authorization/abort, and remaining public physical record
representations are still pending, so implementation-order item 3 is not yet complete.

The fourteenth bounded slice continues implementation-order item 3 with bucket-level multipart
upload listing. Per-PG SQL and storage RPC pagination continue to use complete durable upload
records privately inside storage, but the final cross-PG merge consumes those records into a
logical projection containing only key, upload ID, initiation time, owner, initiator, and checksum
configuration. The public aggregate exposes that projection, logical common prefixes, truncation,
and the logical continuation marker through read-only accessors. Raw page requests, page starts,
and physical responses are crate-private; the aggregate has a logical-only `Debug` view and no
equality implementation that could reveal hidden state. The RPC representation is unchanged.
Compiler visibility is the primary boundary, with checks rejecting raw listing types or direct
aggregate-field access in the production coordinator and rejecting durable upload records or
derived equality in the public aggregate. Multipart initiation, authorization/abort, and
remaining public physical record representations are still pending, so implementation-order item
3 is not yet complete.

The fifteenth bounded slice continues implementation-order item 3 with multipart initiation. The
coordinator callback now supplies `CreateMultipartUploadInput`, containing only the authorized
logical tags, metadata, identities, ACL, Object Lock state, checksum configuration, and encryption
state. Upload ID, bucket, and key cannot be supplied or crossed by the callback: storage issues a
stable provisional upload ID from the bucket-owned key and derives the subject from the admitted
multipart-object route when it constructs its private durable request. Storage retains that
issuance identity across internal retries and binds the final ordered ID to the published command.
An owner-local deterministic contention regression inserts a same-log-index command at the
production route's pre-install boundary, proves authorization is rerun, and proves the final
ordered ID retains the first provisional issuance identity.
Storage continues to derive initiation time, in-progress state, reserved object generation,
current-object identity, ordered upload ID, reservation proof, and command identity. Existing SQL,
metadata-command, and storage RPC representations remain unchanged. Raw multipart-create request
types and test mutation entry points are crate-private; owner-local impossible-state tests may
still exercise them directly. Compiler visibility is the primary boundary, with checks rejecting
the durable request in the production coordinator, public raw mutation surfaces, or caller-
supplied subject and durable-state fields in the logical input. Multipart authorization/abort and
remaining public physical record representations are still pending, so implementation-order item
3 is not yet complete.

The sixteenth bounded slice continues implementation-order item 3 by containing the per-bucket
multipart upload ID signing key. Key bytes, generation, durable encoding, ID issuance, listing
coordinates, encoding length/alphabet, and issuance-identity comparison are private to storage.
Higher layers retain only a
cloneable, opaque `MultipartUploadIdAuthority` and may ask the two logical questions required by
S3 authorization and terminal-upload behavior: whether an ID authenticates for a bucket/object
target and whether it was issued for a principal. The authority has a redacted `Debug` view and no
equality implementation, so key identity cannot be recovered indirectly through containing bucket
summaries or request handles. Existing metadata-command, database, and storage RPC representations
remain unchanged. Compiler visibility is the primary boundary, with checks rejecting key types or
field names outside storage, public key exports, and derived equality on the opaque authority.
Test-only callers receive an opaque deterministic authority and logical valid/overlong upload-ID
helpers; they do not provide raw key bytes or reproduce the ID encoding.
Multipart authorization and abort still expose durable upload/management records and remain the
next containment slice, so implementation-order item 3 is not yet complete.

The seventeenth bounded slice continues implementation-order item 3 with
AbortMultipartUpload authorization and mutation. Storage now consumes its broad management lookup
into an abort-specific result before crossing the crate boundary. Active uploads expose only the
owner and initiator identities needed for the S3 authorization decision and can be consumed once
into an opaque abort capability. Non-active uploads expose an identity-only projection with no
path to that capability; completed-upload replay state is reduced to the logical upload ID needed
for terminal authorization. The coordinator cannot clone, compare, inspect, or construct the
capability and can read only its upload ID for the S3-visible failure path. Storage retains the
complete durable upload record through local and Unix RPC command construction, cleanup
validation, and mutation. Existing database, metadata-command, and storage RPC representations
remain unchanged. Compiler visibility is the primary boundary, with checks rejecting broad
management/durable records in the abort authorization and result seams, raw-record accessors, and
derived clone/equality over the opaque candidate or capability. Multipart UploadPart, completion,
and ListParts authorization still expose the broad authorized durable upload wrapper and remain
pending, so implementation-order item 3 is not yet complete.

The eighteenth bounded slice continues implementation-order item 3 with ListParts authorization.
Storage consumes the broad management lookup before it crosses the crate boundary. Active uploads
expose only owner and initiator identities and can be consumed into a non-cloneable,
non-comparable ListParts-only capability. Terminal uploads share the logical multipart-management
identity projection introduced by the abort slice but have no path to any operation capability;
completed replay state is reduced to the logical upload ID needed for the established terminal
authorization decision. The coordinator passes the opaque capability back through the same
admitted multipart-object route and cannot inspect even its upload ID. Storage expands it back to
the complete durable record only inside the owner crate, preserving the existing local/Unix node
client, database, and storage RPC representations. The already-contained logical ListParts result
is unchanged. Compiler visibility is the primary boundary, with checks rejecting broad
management/durable records in ListParts authorization, results, and execution, and rejecting new
public methods, durable-state Debug output, clone, or equality surfaces on the candidate or
capability. UploadPart and
completion authorization still expose the broad authorized durable upload wrapper and remain
pending, so implementation-order item 3 is not yet complete.

The nineteenth bounded slice continues implementation-order item 3 with UploadPart and
UploadPartCopy authorization. Storage now loads an exact in-progress upload into a one-use
UploadPart candidate. The candidate exposes only the object key, owner and initiator identities,
checksum configuration, and encryption state required by S3 policy, response, and encryption
handling; it can be consumed with the validated part number into a non-cloneable, non-comparable
UploadPart-only capability. The coordinator carries that opaque, part-bound capability plus the
logical checksum algorithm and SSE-C response context. It derives the request subject from the
already-authorized request and passes the capability back through the same admitted
multipart-object route. Storage expands the
capability to its complete durable upload record only inside the owner crate before using the
existing local/Unix node-client, database, metadata-command, and storage-RPC representations.
Candidate and capability `Debug` output is fully redacted, including the upload ID. Compiler
visibility is the primary boundary, with checks rejecting durable upload records in both
UploadPart authorization paths, their result and execution seams, broad admitted-route mutation
arguments, new public capability methods, durable-state `Debug` output, clone, or equality
surfaces. Multipart completion authorization still exposes the broad authorized durable upload
wrapper and remains pending, so implementation-order item 3 is not yet complete.

The twentieth bounded slice continues implementation-order item 3 with
CompleteMultipartUpload authorization, terminal replay, snapshot acquisition, and commit
assembly. Storage consumes its broad management lookup into either an in-progress completion
candidate, a terminal replay candidate, or one unavailable state before the result crosses the
crate boundary. The in-progress candidate exposes only the object key, owner and initiator
identities, and encryption state needed for S3 authorization. Successful authorization consumes
it into a non-cloneable completion capability plus logical checksum, system-metadata, and Object
Lock context. The coordinator passes the capability back through the same admitted
multipart-object route; storage expands it to the durable upload only internally, validates the
snapshot subject, and returns an authorized snapshot that retains the upload's owner, ACL,
public-read state, tags, and user metadata as storage-owned commit defaults. Callers can supply
only the newly evaluated completion values when converting that snapshot into the durable commit
request. Completed-upload replay is reduced before crossing the boundary to the logical upload
ID, completion fingerprint, version, ETag, size, modification time, decoded tags, and encryption
state required by established S3 replay behavior; its bucket, key, and serialized system metadata
remain private. Existing database, metadata-command, local/Unix node-client, storage-RPC, and raw
snapshot representations are unchanged. Compiler visibility is the primary boundary, with checks
rejecting durable upload or replay records in authorization, result, and execution seams;
caller-supplied retained commit defaults; broad admitted-route arguments/results; new public
capability methods; durable-state `Debug` output; clone; or equality surfaces. The broad multipart
authorization seams are now contained. The pre-body completion and UploadPart target validators
use a storage-owned active-upload existence operation rather than receiving a durable record, and
the admitted multipart route's raw management lookup is removed while its active-record lookup is
test-only. Remaining public physical multipart record representations still require the wider
implementation-order item 3 audit, so that item is not yet complete.

The twenty-first bounded slice continues implementation-order item 3 by containing the
CompleteMultipartUpload publication result. The former public stale-payload enum exposed standard
object segments, multipart part records, streaming segment records, and their physical layout even
though the coordinator consumed only the replaced generation ID. That enum and its always-empty
physical vectors are removed. The public outcome retains its storage representation privately and
exposes only the completed version, optional stale generation, logical decoded tags, live size,
and modification time required for the S3 response, lifecycle evaluation, and reclaim enqueue.
Its `Debug` implementation renders only that logical projection. Existing reclaim commands,
metadata commands, database rows, node-client paths, and storage RPC formats remain unchanged and
continue to distinguish standard and multipart physical cleanup internally. Compiler visibility
is the primary boundary, with checks rejecting the former physical result type, public result
fields, or new public outcome methods outside the bounded logical projection. Production
multipart completion no longer exports a physical record representation; test-only multipart
fixtures and the public root exports that support them remain for the wider implementation-order
item 3 relocation audit, so that item is not yet complete.

The twenty-second bounded slice continues implementation-order item 3 by containing the remaining
multipart upload, part, cleanup, management, snapshot, replay, and test-observation
representations. Production multipart upload, in-progress part, streamed-part segment, cleanup,
management-lookup, completion snapshot/preflight/replay, object-identity, and direct test-commit
types are now private to `storage`. Lifecycle sweeping receives only a logical upload projection
containing the object key, upload ID, initiation time, and upload state; its due-decision callback
receives only the initiation time. Cross-crate behavioral tests use feature-gated, read-only test
projections rather than production durable records. Their multipart-upload `Debug` output reports
only logical identity, state, tag count, and generation and redacts tag contents, serialized
metadata, and encryption state. Existing database rows, metadata-command encodings, node-client
messages, and storage-RPC encodings are unchanged. Compiler visibility is the primary boundary,
with a repository check rejecting these raw representations outside `storage`, as public type
definitions, or through the public root export. The production object-read seam still consumes
`ObjectPartRecord`; containing that remaining physical multipart-manifest representation is the
next part of implementation-order item 3, so that item is not yet complete.

The twenty-third bounded slice completes the production portion of implementation-order item 3
by containing the object-read multipart-manifest representation. `ObjectPartRecord` and the
test-only range row are private to `storage`. Object-read snapshots instead expose an
`ObjectReadMultipartPart` logical projection containing only the part number, logical size,
payload CRC, and optional S3 checksum required by read, range, HEAD, and attributes behavior.
Storage retains the complete durable row internally for subject validation, storage-RPC encoding,
and retained payload authority; bucket, key, version, ETag encoding, part generation, placement
epoch, erasure-coding shape, and data PG do not cross the production boundary. Custom `Debug` and
equality implementations operate only on the logical projection, so hidden physical state cannot
be observed through formatting or comparison. Existing database rows, metadata commands,
node-client messages, and storage-RPC bytes are unchanged. Cross-crate tests use a feature-gated
read-only projection plus narrow `pg_store`-owned corruption and removal operations instead of a
raw row replacement API. Compiler visibility is the primary boundary, with a repository check
rejecting raw manifest rows outside `storage`, public raw definitions or exports, broad test row
replacement, public projection fields, unapproved projection accessors, and derived `Debug` or
equality over the retained row. Remaining test-only physical observations are bounded facilities
for placement and manifest assertions rather than production representation seams.

The twenty-fourth bounded slice begins implementation-order item 4 with deterministic static
storage placement. `argmin-s3` retains the outer manifest schema and validates its host, disk,
process, and storage-node references, but no longer imports the placement engine, constructs
placement topology levels, assigns internal domain IDs, selects placement constraints, or derives
PG acting sets. It passes only logical node IDs plus host/disk labels, the EC shape, PG count, and
selected deployment failure domain and complete declared host/disk domain lists to `storage`. The
storage-owned builder validates node-domain references, performs deterministic domain assignment,
bounds the PG allocation, constructs the placement-engine topology, derives
the acting sets with the existing placement-key domain, and returns only the logical node-ID sets
needed by the outer manifest digest and the next initial-map boundary. Its error type is opaque and
owner-formatted, and its placement result has a count-only `Debug` view. A repository check rejects
an `argmin-s3` placement dependency or direct placement-engine types. Initial topology certificate
assembly, raw control-plane commands, live topology transitions, and metadata-transfer
orchestration remain pending parts of item 4.

The twenty-fifth bounded slice continues implementation-order item 4 by containing the certified
initial static control-plane topology. After the outer manifest computes its stable topology
identity, it passes the topology generation and digest, logical Raft voter IDs, canonical logical
storage-node endpoints, and the previously owner-validated placement to `storage`. Storage binds
those inputs into one opaque `StaticInitialControlPlaneTopology`, validates the endpoint/node
identity set, decodes the topology digest, constructs and validates the private PG/node bootstrap
representations and `InitialClusterTopologyCertificate`, and retains the exact certified bootstrap
command. `ServerConfig` carries only that opaque value. Raft peer policy binding, pending-static
durable-authority construction, certified command submission, replication-envelope validation,
and established-snapshot certificate matching now accept the opaque topology through
storage-owned operations. `argmin-s3` no longer imports or constructs the certificate, computes
the bootstrap-map digest, constructs `BootstrapCertifiedInitialClusterMap`, or inspects the
snapshot's certificate. Submission compares the opaque topology's certificate with the exact
certificate retained by the authority's configured static peer policy before entering command
admission; a crossed topology therefore cannot append or commit a log entry. The acting-set
mutation/certificate-persistence impossible-state
regression is owner-local, while manifest tests retain only logical test projections for topology
generation, voter IDs, and acting-set node IDs. A repository check rejects those raw certificate,
command, and snapshot-inspection seams in `argmin-s3` and public fields on the opaque topology.
Environment-only legacy-local bootstrap still uses its distinct uncertified local command path.
Live topology transitions, metadata-transfer orchestration, and the remaining static
control-plane administration and transport-bootstrap orchestration are pending parts of item 4.

The twenty-sixth bounded slice starts the live-topology portion of implementation-order item 4 by
moving metadata-transfer route-refresh classification into `storage`. `argmin-s3` no longer
destructures `StoreError`, recursively unwraps shard failures, interprets storage-node failure
classes, or recognizes reconstruction failures from rendered message fragments. The
storage-owned `PgMetadataTransferError::requires_route_refresh_retry` operation exhaustively
classifies its public transfer-error variants, uses the private nested store/RPC representations,
and receives a typed reconstruction-specific route-refresh variant at the owner boundary.
Owner-local tests pin local route expiry/staleness, nested shard failures, apply failures, every
semantic storage-node class, and stale versus permanent reconstruction outcomes. Cross-crate
retry-loop tests inject only an opaque owner-provided retry failure, and the repository boundary
check rejects `StoreError` matching or storage-node failure classification in `argmin-s3`. The
route/proof state machine, runtime-map reconstruction, and transfer artifact orchestration remain
pending behind the planned storage-owned administration facade.

The twenty-seventh bounded slice contains the complete automatic live PG metadata-transfer state
machine behind `LivePgMetadataTransferAdmin`. The process layer supplies only the already parsed
logical PG/node IDs, EC and admission configuration, one authority-bound opaque control-plane
capability, and configured storage-node endpoints and credentials. Storage binds the read and
admin roles to one retained transport client and validates their authenticated cluster identities;
separately constructed endpoint sets cannot be supplied or compared by display labels. It then
dispatches only the private operations needed by the transfer; the capability does not implement
the general public runtime-map source trait. Storage now owns completed-transfer detection, fencing,
source-lease expiry, scoped route observation, source and destination runtime-map authorization,
cluster reconstruction, artifact export and import, destination-epoch rebasing, proof validation,
and the bounded stale-route retry loops. A successful acting-set install is followed by a fresh
PG-scoped serving read before destination I/O; the reconstructed mutation response is not treated
as serving authority. If another runner completes peering between that mutation and the fresh
read, the requested Active acting set is recognized as successful completion instead of being
rejected as a non-Peering destination, but only when its active metadata proof equals the expected
imported proof. Control-plane RPC version 14 carries that proof in Active route snapshots so both
post-install and import-refresh completion checks fail closed on superseding transitions. Runtime-map
content and current-state digest domains are now version 3 and bind the Active proof plus both
inner proof-carrier versions. The
static route-map content domain and combined standalone-route identity domain are likewise version
3 because both transitively encode those routes. Their containing standalone identity and
initialization-marker format is version 3; exact version-3 fixtures and explicit version-2 and
version-4 rejection fixtures prevent the current proof grammar from remaining under version 2.
Both artifacts revalidate the exact bytes read after the initial descriptor metadata check, so a
concurrent truncation or extension fails closed before fixed-offset parsing; deterministic
owner-local mutation tests pin both races for the marker and identity.
The operation returns only an opaque, redacted failure or a logical summary;
its route snapshots, transfer proofs, artifacts, reconstruction failures, and retry classification
are crate-private. Owner-local RPC-backed tests pin the already-completed path, the complete
export/install/import transfer with an unrelated unserved PG, deterministic post-fence
interruption, resume after post-install and post-import interruption, both stale-route refresh
paths, completion racing the post-install observation, single-transport credential binding,
matching-versus-mismatched concurrent completion proofs, and diagnostic redaction. The repository
boundary check
rejects automatic transfer representations or orchestration outside `storage` and rejects giving
the opaque transfer capability the general runtime-map source interface. Manual operator PG
fence/set/proof commands and the residual
static control-plane administration and transport-bootstrap orchestration remain pending parts of
item 4.

The twenty-eighth bounded slice contains the manual operator PG administration and scoped
readiness seam. `argmin-s3` now parses and renders only logical integer PG, node, epoch, and
metadata-proof components. A storage-owned `ControlPlanePgAdminClient` binds the configured
transport and optional admin credential, constructs the private PG/node/proof representations,
performs checked acting-set, fence, and metadata-transfer installation operations, and returns
only the resulting logical epoch. The metadata-transfer install input is opaque after construction,
validates its logical acting-set and epoch invariants before dispatch, and has a redacted debug
view. Offline acting-set mutation likewise opens and mutates the owner-private control-plane store
inside `storage`; the process retains only its pre-existing process-lock orchestration. A separate
opaque `ControlPlanePgStatusClient` owns the frontend-authenticated scoped runtime-map read and
validates that the requested PG is Active, leased, and serving on the exact operator-supplied
acting set before returning logical runtime and route epochs. Transport/protocol causes remain
retained behind a redacted storage error, while readiness mismatches use owner-defined semantic
diagnostics rather than process interpretation of route state. Owner-local coverage pins exact
proof construction and input rejection, offline mutation, plain and authenticated live dispatch,
and scoped readiness; process tests retain argument parsing, command composition, and CLI-visible
success behavior. The repository boundary check rejects PG/proof/store/runtime-map reconstruction
within the manual command seam and rejects representation accessors, public fields, derived
diagnostics, or a general runtime-map-source implementation on the opaque facades. Residual static
control-plane administration, authority-clock/Raft operator commands, transport bootstrap, and
the process-hosted control-plane authority implementation remain pending parts of item 4.

The twenty-ninth bounded slice contains the authority-clock and Raft operator-command seam.
Ordinary Raft leadership transfer, snapshot/purge, and election commands now pass only a logical
Raft node integer through an opaque `ControlPlaneRaftAdminClient`; storage owns plain-versus-signed
dispatch, operation-specific RPC behavior, response-loss classification, and retained transport
diagnostics. Authority-clock status and re-establishment use a distinct
`ControlPlaneAuthorityClockAdminClient` that cannot be constructed without an authenticated admin
credential whose scoped principal is explicitly `Admin`, so neither an unauthenticated recovery
endpoint nor a frontend, storage-node, or service credential can become an authority-clock
administrative capability.
The process no longer receives or interprets `ControlPlaneAuthorityClockStatus`; storage returns an
opaque status with an exact owner-rendered operator display and a redacted debug view. Both
capabilities retain implementation causes behind an opaque public error whose `Display`, `Debug`,
and `source()` cannot expose endpoints, routes, epochs, proofs, or remote diagnostics. Storage
classifies every mutating request that lacks a valid operation result as `RpcUnconfirmed`,
separately from valid remote rejections and failures before request publication. Authenticated
dispatch additionally requires that the result authenticate as the response to the exact operation.
This includes response loss, wrong outer kinds, malformed or incorrectly authenticated responses,
and invalid success payloads. The facade renders fixed operator guidance that the
mutation may have applied and must not be retried without an operation-specific confirmation check.
Read-only status failures cannot acquire that mutation classification. Owner-local tests pin
no-network rejection of missing and wrong-principal
credentials, exact established and blocked status rendering, opaque debug, authenticated dispatch,
retained-error redaction, valid authenticated remote rejection, and applied plain or authenticated
Raft commands followed by lost, wrong-kind, malformed, incorrectly authenticated, or undecodable
responses through the public operator facade. Authority-clock coverage also pins invalid signed
success payloads as unconfirmed before the existing confirmation state machine. The repository
boundary check rejects direct raw operator RPC calls,
clock-status interpretation in `argmin-s3`, and expanded or derived public surfaces on the opaque
facades. Recovery endpoint derivation and credential/transport assembly remain part of the pending
transport-bootstrap slice; the authority clock and Raft authority implementations remain pending
with the process-hosted control-plane authority slice. The residual duplicate static
control-plane bootstrap and administration path is completed by the thirty-third bounded slice.

The thirtieth bounded slice contains certified static Raft membership convergence and initial
topology establishment. `ControlPlaneRaftAuthority` now validates live effective and applied
membership against the exact static peer policy retained when that authority was constructed; the
process cannot supply an independent policy, inspect raw voter/learner sets, or decide when the
membership is safe to certify. Static outer-identity publication likewise asks the authority to
capture, validate, and durably publish its checkpoint against that same retained policy. The
process receives only an opaque successful-publication proof containing the logical clock-sidecar
binding; pending effective membership or apply convergence exposes no checkpoint or policy state.
The topology authority operation binds the opaque initial topology to its retained certificate,
validates established state, enforces the published-identity fail-closed gate, submits the
certified bootstrap command only from a serving authority, and owns the exact
leader/concurrent-bootstrap retry classification. Owner-local regressions pin applied versus
committed convergence, missing effective and applied membership, complete voter/learner/log
identity matching, crossed-topology rejection before log append, bootstrap prohibition after an
outer identity is published, successful establishment, and certified checkpoint publication
through the bound authority. The process-level regression now covers only opaque checkpoint-proof,
clock-sidecar, and outer-identity publication composition. The environment-only uncertified
initial-map path, the older process-hosted
`ExperimentalRaftControlPlane` bootstrap wrapper, recovery endpoint and credential/transport
assembly, transport bootstrap, and the process-hosted authority implementation remain pending
parts of item 4.

The thirty-first bounded slice contains the environment-only uncertified initial-map path. The
process converts its outer configuration entries only into logical storage-node endpoint values
and supplies logical PG integers to `derive_uncertified_initial_control_plane_topology`.
Storage canonicalizes and validates the endpoint set, constructs the private node/PG bootstrap
representations, checks the command against an empty state machine and the Raft replication
envelope, and retains it inside an opaque topology exposing only node and PG counts. The
single-authority operation owns empty-state observation, durable application, idempotent
already-initialized behavior, and the logical established epoch. The Raft authority similarly
owns pre-submission empty-state observation and post-publication resolution: a bootstrap rejection
or leader-routing race is concurrent success only after a fresh authority read proves that initial
state now exists. The submitted outcome is bound to the exact owner-built topology and exact
issuing Raft authority identity inside a one-use opaque value. That process-local identity is a
storage-owned cryptographically random token which is neither rendered nor serialized; allocation
addresses are not used as identifiers. The same token binds captured restart checkpoints to their
issuing authority. Another authority rejects either capability before inspecting its outcome or
local state, and the process cannot inspect or replace the Raft result. The existing
process-hosted wrapper still performs its required durable response publication while holding that
value between the two owner operations; the thirty-fourth bounded slice removes that split-phase
surface.
Owner-local tests pin the unchanged legacy command shape, invalid endpoint/PG rejection,
no-node no-op, single-authority durability and idempotence, successful Raft submission,
concurrent-success rejection before versus acceptance after initialization, and deterministic
two-authority crossing rejection. Process tests retain
startup composition and durable-publication failure behavior. The repository boundary check
rejects rebuilding the uncertified command or node/PG representations in the process bootstrap
functions and rejects expanding the opaque topology beyond its logical counts. The residual
certified-static administration and uncertified split-phase publication are completed by the
thirty-third and thirty-fourth slices respectively. Recovery endpoint and credential/transport
assembly, transport bootstrap, and the process-hosted authority implementation remain pending
parts of item 4.

The thirty-second bounded slice completes the storage-node TLS-profile containment gate.
`argmin-s3` retains manifest/file ownership and supplies only resolved socket addresses, TLS
server names, trust roots, and certified server identities. Storage now constructs the fixed
TLS 1.3 client and server profiles, installs the private `argmin-storage-rpc/1` ALPN identifier,
and retains the resulting Rustls configurations inside opaque `StorageRpcClientEndpoint` and
`StorageNodeRpcListenerConfig` values. Their raw enum variants, connection pool, TLS
configurations, and profile-building entry points no longer form a public cross-crate surface;
the process uses only logical Unix or TLS-TCP constructors and a transport-kind observation.
Owner-local tests pin the exact ALPN profile, reject TLS 1.2-only peers in both directions, and
retain the authenticated real-server boundary coverage. The repository boundary check rejects
storage ALPN use, raw endpoint/listener construction, and Rustls profile construction in the
static manifest mapper, and rejects making the private profile constant or raw constructors
public. Storage-node TLS profile ownership is complete; control-plane/Raft transport bootstrap
and the process-hosted control-plane authority remain separate item 12 work.

The thirty-third bounded slice removes the duplicate process-owned certified-static bootstrap
path. Static startup establishes the configured topology exactly once through
`ControlPlaneRaftAuthority::establish_static_initial_topology`, which owns the retained-policy
binding, membership convergence, snapshot validation, serving-state gate, submission, and
concurrent-result classification. The later process-hosted wrapper and periodic lease loop now
run only for the environment-only uncertified topology path; they no longer re-read certified
snapshots, submit a second certified command, or classify a certified rejection. The deleted
process impossible-state tests duplicated the owner-local authority tests that pin rejection
before initialization and idempotent concurrent establishment after initialization. The boundary
check rejects direct certified submission, static snapshot validation, and the removed
process-level classifier/helper surfaces. The underlying certified-command submission and static
snapshot-validation primitives are crate-private, and the boundary check also rejects making
either API public again. The uncertified wrapper still performs the required checkpoint
publication between its opaque prepare and resolution operations, so moving that wrapper remains
coupled to moving its durability publisher. Recovery endpoint and credential/transport assembly,
control-plane/Raft transport bootstrap, and the process-hosted control-plane authority remain
separate item 12 work.

The thirty-fourth bounded slice contains uncertified initial-topology submission, durable
publication, and resolution in one storage-owned authority operation. Each Raft authority now
owns exactly one opaque durability-publication domain; callers can clone its capability for RPC
response admission and poison propagation but cannot construct a second domain. Process control
plane and peer-durability wrappers retain only their authority and derive its publication domain
at each use, so they cannot combine one authority with another authority's capability. The
operation owns empty-state observation, leader-routing retries, command
submission, durable restart-checkpoint publication, concurrent-result classification, and logical
epoch resolution. It publishes no successful result until the committed command is present in a
restartable checkpoint. Already-visible topology is likewise successful only after the observing
authority has published its own local restart checkpoint; replicated apply visibility is not a
durable-publication proof. A checkpoint failure retains its storage diagnostic, atomically closes
response admission, waits for already admitted responses to drain, and leaves every clone
poisoned. The former public split-phase submission value and prepare/resolve methods are now
crate-private, and the process supplies only the opaque topology before logging its logical counts
and established epoch. Owner-local tests pin concurrent response admission versus exclusive
poison, one authority-wide poison domain, durable bootstrap restart recovery, idempotence, a real
follower-routing rejection followed by leader publication, follower-local checkpoint publication
when the leader's publication fails, checkpoint-failure poisoning, and the existing
concurrent-bootstrap classification. Process tests
retain only startup/runtime-map composition and use a storage-provided test authority with an
explicit checkpoint target. The boundary check rejects process use or public re-exposure of the
split-phase APIs and constrains the durability-publication capability to its opaque admission and
poison surface. It also rejects storing an independently supplied publication capability in
either process wrapper. Recovery endpoint and credential/transport assembly, control-plane/Raft transport
bootstrap, and the remaining process-hosted control-plane authority implementation remain item 12
work.

The thirty-fifth bounded slice contains operator control-plane client bootstrap. The process now
supplies only resolved ordinary or recovery endpoints, logical cluster/admin-instance identity,
and unscoped credential inputs to `ControlPlaneAdminClientBootstrap`. Storage validates the
endpoint set, selects the latest local credential, constructs and scopes the admin principal, and
retains both transport and credential opaquely before constructing PG, Raft, or authority-clock
operation clients. Authority-clock recovery Unix socket derivation is storage-owned with exact
owner-local fixtures; the process can request the semantic derived namespace but no longer owns
its hash or filename representation. Live PG metadata transfer receives a separate opaque admin
credential binding and combines it with the already retained read transport inside storage, so
the process cannot supply independent read/admin transports. Raw operation-client constructors
are crate-private, diagnostics redact endpoint and credential material, and a repository check
rejects the former process dispatch enum, raw constructors, local recovery derivation, or public
re-exposure of the raw client seams. Frontend/storage-node service-client bootstrap, server
listener/auth-verifier bootstrap, Raft-peer bootstrap, and the process-hosted authority
implementation remain item 12 work.

The thirty-sixth bounded slice contains frontend and storage-node control-plane service-client
bootstrap. The process supplies resolved endpoint inputs, logical frontend or node identity,
node incarnation, unscoped configured credentials, and an optional logical signing-credential
selection. Storage validates the endpoint set, selects the default or explicitly configured local
credential, constructs and scopes the exact frontend or storage-node principal, and retains plain
versus authenticated dispatch behind `ControlPlaneFrontendClient` and
`ControlPlaneStorageNodeClient`. The capabilities implement only the existing runtime-map or
heartbeat service traits plus bounded diagnostic/readiness operations, and their debug views
redact endpoint and credential material. PG status and live metadata transfer now derive their
read transport and scoped credential from the opaque frontend capability inside storage; their raw
client constructors are no longer available to the process. Owner-local tests pin default and
explicit credential selection, node-incarnation binding, incomplete/crossed configuration, and
redaction, while the existing process RPC test continues to prove an authenticated heartbeat is
accepted. A repository check rejects the former process dispatch enums, authenticated-client
construction, raw status/transfer composition, public fields, derived diagnostics, or expansion
of the bounded service-client surface. Server listener/auth-verifier bootstrap, Raft-peer
transport bootstrap, and the process-hosted authority implementation remain item 12 work.

The thirty-seventh bounded slice contains control-plane RPC server authentication bootstrap. The
process supplies only logical cluster and optional local-admin identities plus unscoped
storage-node, frontend, and admin credential inputs to `ControlPlaneRpcServerAuth`. Storage
validates the complete role set, requires admin credentials whenever server authentication is
enabled, validates the actual cluster identity against the authentication-envelope codec bound,
rejects crossed local-admin configuration, constructs every concrete credential and the
role-aware verifier, and attaches that verifier to server policies without exposing it. The
capability retains shared authentication metrics and owns the existing operator diagnostic
format; its `Debug` and diagnostic output redact secrets and authentication envelopes, while
byte-escaping every untrusted identifier into a single-line quoted value. The raw
verifier, concrete server-side credentials, and direct policy attachment are crate-private.
Owner-local tests pin all-role construction, rejection accounting, diagnostic shape and redaction,
the exact 256/257-byte cluster-identity boundary, adversarial diagnostic identifiers, plain mode,
and incomplete or crossed configuration without network use. Process tests retain the
production authenticated frontend and storage-node request paths through the opaque capability.
A repository check rejects process construction or formatting of raw verifier state and constrains
the capability to construction, authentication-state observation, and redacted diagnostics.
Server listener/resource-policy bootstrap, Raft-peer transport bootstrap, and the process-hosted
authority implementation remain item 12 work.

The thirty-eighth bounded slice contains control-plane RPC server listener and resource-policy
bootstrap. The process retains OS-owned concerns by binding configured Unix or TCP sockets and
loading the selected certificate identity, then hands those inputs and logical resource limits to
`ControlPlaneRpcServerBootstrap`. Storage constructs the fixed TLS 1.3/ALPN protocol profile,
validates listener limits, requires both ordinary and authority-clock-recovery endpoint groups,
rejects duplicated kernel socket identities across those roles, pairs each group with its fixed
operation role and shared worker/pre-authentication budgets, attaches the opaque server-auth
capability, and owns the listener threads. Separate single-
authority and Raft serving operations fix authority-clock request gating and the required
confirmation/publication hooks inside storage rather than allowing the process to compose those
policies. Raw listeners, resource policies, endpoint roles, and serving methods are crate-private;
cross-crate process tests use one feature-gated bounded ordinary-listener facility rather than the
production primitives. Diagnostics expose only transport kind, counts, limits, and authentication
state. Owner-local tests pin mandatory role pairing, invalid listener/resource configuration,
cloned Unix/TCP rejection across roles, and redaction, while the existing process tests retain
replay-before-bind, authenticated request, and storage-node startup behavior. A repository check
rejects process construction or composition of the raw server surface and constrains the opaque
production facade. Raft-peer transport bootstrap and the process-hosted control-plane authority
implementation remain item 12 work.

The thirty-ninth bounded slice contains Raft-peer transport bootstrap.
`ControlPlaneRaftPeerBootstrap` is the single opaque configuration shared by durable authority
construction, outbound peer networking, inbound peer serving, authentication, topology binding,
membership initialization, and startup leader-wait policy. The process supplies logical peer and
client endpoints, transport limits, timeouts, topology identity or certified initial topology,
unscoped peer credentials, an optional signing-credential selector, bound sockets, and certificate
material. Storage requires the credential principals to cover exactly the retained peer set,
validates that the local peer and complete client route set match the retained peer policy, selects
and scopes the local credential, applies the same topology and authentication policy to both
directions, decides which peer may initialize membership, constructs the durable authority, and
creates and owns the listener loops. Membership initialization derives only from the authority's
retained policy, while the server derives its policy from and retains that exact authority, so
neither path accepts a caller-paired second policy or authority. Peer-server poison state and
response publication use a storage-owned durability capability bound to that authority, and any
checkpoint callback failure poisons that domain before the error is returned. The remaining
process-hosted checkpoint callback receives the server's exact authority rather than retaining or
selecting another one and only reports its result; it cannot decide whether a failure poisons the
server. A crossed durability capability is rejected before any listener loop starts. The facade
also rejects missing listeners, duplicate endpoint identities, and cloned Unix or TCP listener
handles before serving.
Bootstrap and authentication diagnostics expose only peer counts, local node identity, credential
version, and aggregate outcomes; cluster, topology digest, credential identifiers, and secrets
remain redacted.
The raw peer authentication, transport, network, listener, server-policy, and replicated-authority
constructors are crate-private. Static-manifest capacity validation uses a narrow storage-owned
limit validator, and cross-crate process tests use bounded semantic test client/server facilities.
The process deliberately retains socket binding, checkpoint file/authority-clock sidecar
publication, the periodic checkpoint worker, and the checkpoint implementation invoked by the
storage-owned durability-before-ack gate until the final process-hosted authority slice. A
repository check rejects raw peer-policy composition or replicated-authority construction outside
`storage`. The process-hosted control-plane authority implementation is the only remaining item 12
work.

The fortieth bounded slice begins the final process-hosted authority work by containing the Raft
durability lifecycle. Each authority issues and retains one shared
`ControlPlaneRaftAuthorityDurability`, bound to the artifact path derived from that authority;
callers cannot construct a second lifecycle or supply a second path, publication domain, or
serving-read policy. Repeated issuance returns the same lock/marker/registration domain, serving
reads derive WAL poison validation versus artifact checkpointing from the authority's actual
durability configuration, and only one checkpoint monitor may register. Storage owns the shared
checkpoint lock, serving-read checkpoint marker, initial and
periodic checkpoint policy and tracker, WAL observation, periodic snapshot/checkpoint/purge ordering,
authority-clock sidecar load/invalidation/target derivation, static outer-identity certification
ordering, peer response checkpoint capability, checkpoint monitor thread, and monitor failure
poisoning. The process supplies its Tokio handle, fatal-process callback, and the narrow outer
manifest publisher required because the deployment manifest remains process-owned. Existing
process regressions continue to pin WAL-before-ack behavior, bounded election checkpointing,
static identity publication after a pre-existing artifact, serving-read behavior, concurrent
checkpoint serialization, and production-shaped write amplification; owner-local tests pin
checkpoint bounds, authority-derived path ownership, clock-sidecar initialization, redaction, and
rejection of an authority without configured durable state. A repository check rejects the former
process-owned path/lock/marker/policy/monitor machinery and public fields on the new opaque
capability.

This slice does not complete item 12. `ExperimentalRaftControlPlane` still lives in the process
and composes logical command submission, authority-clock/lease-horizon sampling, operation-level
checkpoint/poison handling, and the control-plane trait implementations. Those operations and
their remaining low-level authority calls must move behind a storage-owned authority host before
the raw durability and OpenRaft operation APIs can become crate-private.

The forty-first bounded slice adds that storage-owned steady-state authority host.
`ControlPlaneRaftAuthorityHost` now owns logical command submission, liveness-command checkpoint
policy, committed-command checkpoint/poison sequencing, authority-clock and lease-horizon
sampling, heartbeat expiry, volatile-heartbeat reconciliation, runtime-map reads, operator
administration, and the control-plane runtime-map/heartbeat/admin trait implementations. It also
binds ordinary and recovery RPC serving to the host's exact authority clock, checkpoint target,
linearized-authority confirmation, and durability-publication domain. The process no longer
defines a parallel Raft control-plane implementation or composes those capabilities.

The exact durable host lifecycle is authority-issued and retained. Repeated host issuance reuses
the first runtime, authority clock, durability lifecycle, and publication domain; the process
cannot pair an authority with an independent clock, checkpoint lifecycle, or runtime. Whether the
fresh-cluster leadership-term exception applies is recorded by the authority when it initializes
membership, rather than supplied as a constructor boolean. Owner-local tests pin shared issuance,
crossed-runtime derivation, in-memory rejection, and redacted diagnostics. Existing process
regressions exercise the same production host through bounded test hooks, and a repository check
rejects renewed process-owned command/clock/checkpoint/RPC composition and unapproved public host
surface.

This slice still does not complete item 12. The process retains deployment startup sequencing:
opening the authority through the peer bootstrap, starting peer service and the checkpoint
monitor, membership/catch-up waits, static topology and outer-identity publication, and initial
durability publication. That startup sequence must move behind a storage-owned authority-open
operation before the remaining low-level authority startup APIs can become crate-private.

The forty-second bounded slice completes item 12 by containing durable authority startup behind
an opaque two-phase storage operation. `ControlPlaneRaftPeerBootstrap::prepare_durable_authority`
opens, replays, and validates the exact durable authority and issues its one durability lifecycle
before the process binds an inbound peer socket. The resulting
`PreparedControlPlaneRaftAuthority` is the only value that can start service after binding; it owns
the authority, bootstrap policy, durability lifecycle, and logical outer-identity state without
exposing any of them. Its consuming start operation constructs the authority-bound peer durability
callback, validates and publishes peer listeners, starts the single checkpoint monitor,
initializes and checkpoints configured membership, waits for the required single-node leadership
or multi-node committed-state catch-up, establishes the bootstrap's retained certified topology,
publishes the initial restart artifact/clock sidecar/outer identity, and only then returns the
steady-state host. A failure after preparation poisons the authority's publication domain rather
than leaving a partially started authority eligible to serve.

The returned `ControlPlaneRaftAuthorityService` retains the host, checkpoint monitor, and peer
listener loops as one opaque lifecycle. Multi-node mode is derived from that service rather than
re-read from an independently retained process configuration. The process retains deployment
state-directory locking, socket binding, the opaque outer-identity publisher, fatal-process
callbacks, and logical steady-state configuration only. Superseded authority-open, durability,
peer-server, membership, leadership, topology, checkpoint, outer-publication, and direct host-start
operations are crate-private in production; explicitly named hooks remain feature-gated for
cross-crate process behavior tests. Owner-local regressions pin replay-before-listener typestate,
membership and initial checkpoint completion before return, advancing committed-watermark catch-up,
pre-open rejection of an outer identity without a certified topology, and poisoning after a
prepared startup fails. The repository boundary check rejects both process-side recomposition and
renewed public low-level startup methods.

The closed primitive inventory includes durable single-node authority construction, authority-clock
checkpoint binding, durability publication/lifecycle issuance, peer-server durability binding,
membership initialization and leader waits, durability metrics/WAL observation, snapshot trigger
and purge, restart-checkpoint capture/publication/persistence, certified static-identity checkpoint
publication, static-topology establishment, checkpoint-monitor start, outer-identity publication,
and direct durable-host start. Their production methods are crate-private; cross-crate tests use
only same-operation names carrying an explicit `_for_test` suffix behind `test-hooks`. The boundary
checker enumerates these exact primitive names rather than relying on the process's former call
spellings.

This audit covers production boundaries. Existing `PgTopology` use in `server-core` is test-gated;
those tests must migrate with the relevant owner-local impossible-state fixtures, but it is not a
separate production leak. UAT/process tests may continue to identify an operator-visible topology
target through a supported command surface, without importing the storage representation types.

Phase 1 exit criteria:

- Every version boundary has one recorded owner crate and a documented public logical API.
- Representation details and implementation errors do not cross the owner crate boundary.
- Impossible-state tests are colocated with the format implementation; process tests use
  supported operations or opaque owner-provided test facilities.
- Every identified cross-crate representation leak is removed and replaced by the owning
  crate's logical API. Recording a leak as future work does not satisfy this phase's exit gate.
- Boundary checks cover known high-risk leaks, while compiler visibility remains the primary
  enforcement mechanism.

All Phase 1 exit criteria above are now satisfied and boundary-checked. The completed Phase 2
evidence audit now lives in the maintained
[format ledger](../../guides/storage-format-ledger.md); it does not reopen containment or introduce
compatibility readers.

## Phase 2: Baseline Version Markers

Phase 2 completed the exact-current baseline and owner-local evidence audit. The normative,
maintained format inventory and evidence ledger now lives in
[`guides/storage-format-ledger.md`](../../guides/storage-format-ledger.md). Future format changes
must update that guide rather than this archived plan.

The historical implementation sequence remains available in repository history. Its stable
versioning rules are in [`guides/versioning.md`](../../guides/versioning.md), and future migration
or mixed-version support remains owned by
[`storage-upgrade-versioning-plan.md`](../storage-upgrade-versioning-plan.md).

## Superseded Future Proposal: Upgrade Framework

This section and the following trigger/Raft proposal were never implemented. They are retained as
historical context only; the active upgrade plan replaces them.

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

## Superseded Future Proposal: Trigger SQL Body Verification

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

## Superseded Future Proposal: Rolling Upgrade And Raft Interaction

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
  [`storage-topology-resize-plan.md`](../storage-topology-resize-plan.md); a cluster should not run resize operations across
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

The higher-layer control-plane error cleanup, control-plane and Raft transport containment, raw
diagnostic and Raft representation containment, durable Raft restart/WAL containment, nested
durable codecs, static route authority, session-token ownership, storage-node TLS profiles, and
semantic topology and maintenance work are complete and boundary-checked.

## Completed Implementation Checklist

Completed in the current containment pass: the control-plane client and server boundaries and the
Raft peer client and server transports are storage-owned and boundary-checked.

1. **Complete:** privatize the remaining raw Raft frame, auth-envelope, OpenRaft-handle, and
   transport-error APIs and relocate malformed-wire tests into `storage`.
2. **Complete:** hide public WAL/restart-format constructors and move direct WAL/impossible-state
   tests into the owner.
3. **Complete:** replace other higher-layer matching and construction of database/RPC
   implementation errors with owner-defined semantic errors or exhaustive classification methods.
   `ControlPlaneError` transport, response-loss, leader-routing, and runtime-map readiness
   classification is storage-owned. Callers cannot parse or recover retained implementation
   diagnostics, and cannot construct raw `Io`, `RpcProtocol`, or `RpcRemote` variants.
4. **Complete:** inventory and restrict nested durable codecs for metadata, tags, ACLs, and
   encryption; record how containing formats advance when a nested format changes. Object
   encryption, user/system metadata, object tags, bucket tags, and ACL grants have owner-local
   codecs, boundary checks, exact current-representation goldens, and containing-format
   inventories above.
5. **Complete:** the unused legacy authority-clock constructor is removed and sample-driven
   construction is test-only. The optional digest and live static-to-dynamic transition are now
   replaced by mandatory static/dynamic authority proofs, with bounded validity required at
   dynamic construction. Explicit static constructors, the opaque common route handle, the
   dynamic-only publication/refresh capability, frontend/coordinator migration, and boundary
   checks are complete. Every no-control-plane topology now binds its storage-owned canonical
   route identity durably before serving, and the former `legacy-local` process role is now the
   topology-specific `all-in-one` role without conflating it with standalone deployment mode.
6. **Complete:** owner-local exact-current rejection fixtures cover every boundary listed in the
   2026-07-29 audit, including resealed enclosing checksums, digests, and authenticators.
7. **Complete:** session-token version selection is contained inside `auth`, with semantic APIs
   and a boundary check rejecting external or public version-specific format surfaces.
8. **Complete:** trigger SQL body verification is explicitly moved out of Phase 11 stabilisation
   tracking and retained only in Phase 4 of this plan. Trigger hashing/recreation remains deferred
   until the upgrade framework is deliberately started; it is not current-format recovery work.
9. **Complete:** move storage-node TLS 1.3 and ALPN profile construction out of `argmin-s3` and
   behind storage-owned endpoint/listener constructors. Process configuration supplies trust
   roots, certificate identities, endpoint names, addresses, and bindings; `storage` supplies and
   validates the protocol profile. Make `STORAGE_RPC_TLS_ALPN` owner-private and extend the
   boundary check to prevent raw storage-RPC Rustls profile construction outside `storage`.
10. **Complete:** move shard scavenger, repair, backfill, payload reclaim, asynchronous bucket
    cleanup/finalization, and abandoned stream-session workers into a storage-owned maintenance
    runtime. Include scan fairness, durable PG scans, deferred queues, cooldown, claims, retries,
    admission, expiry, completion, and storage-specific telemetry. Keep lifecycle evaluation and
    the S3-visible bucket-delete request path in `server-core`; storage creates and owns cleanup
    roots after one logical accepted-deletion operation. Privatize all maintenance cursor, claim,
    reclaim-work, cleanup-root, session-cleanup, work-record, and transition APIs. The
    backfill-candidate cursor, shard-audit/checkpoint scheduler, shared maintenance admission,
    abandoned stream-session cleanup, repair, backfill, reclaim, and accepted bucket-delete
    continuation/finalization slices are complete. Raw reclaim queue and cleanup-root transitions
    are private. Durable reclaim/finalizer claims, roots, records, attempt outcomes, and structured
    debug snapshots are also private; HTTP receives an owner-rendered opaque diagnostic and
    cross-crate composition tests use test-only DTOs and logical observations.
11. **Complete:** replace server-core's data-PG, placement-epoch, EC-placement, shard-location,
    and historical-route handling with opaque storage-owned payload handles and logical
    I/O/lease operations. Buffered/direct PutObject, streaming PutObject and UploadPart,
    multipart completion, retained object reads, and multipart-manifest reads now cross the
    production boundary through subject-bound opaque capabilities or logical projections.
    Physical placement records and mutation APIs are private to `storage`; the remaining
    feature-gated projections are narrow, read-only test facilities for cross-crate behavioral
    assertions, while impossible physical-state tests are owner-local.
12. **Complete:** deterministic static storage-placement interpretation and certified initial
    topology/bootstrap assembly are storage-owned without transferring outer manifest ownership.
    Certified live Raft membership convergence and initial-topology establishment are now bound to
    the authority's retained static peer policy and topology certificate; the process neither
    supplies a second policy nor interprets membership/status or captured checkpoints to decide
    bootstrap and outer-identity publication safety.
    The automatic live PG metadata-transfer state machine, including route/proof inspection,
    runtime-map reconstruction, artifact movement, and retry classification, is contained behind
    an opaque storage-owned administration operation. Manual operator PG acting-set, fence,
    metadata-transfer installation, offline state mutation, and scoped readiness operations are
    also storage-owned and accept only logical integers or opaque inputs from the process.
    Authority-clock and Raft operator commands are now contained behind separate opaque
    storage-owned capabilities, with storage-rendered status and redacted retained diagnostics.
    The environment-only uncertified initial-map path is now contained behind an opaque
    storage-owned topology and authority operations. The duplicate process-owned certified-static
    bootstrap, snapshot validation, and rejection classification are removed; certified startup
    now uses only the authority-owned establishment operation. Uncertified submission, durable
    checkpoint publication, leader retry, and resolution are likewise one storage-owned operation
    using the authority's single opaque response-publication and poison domain. Operator recovery
    endpoint and credential/transport assembly plus frontend/storage-node service-client
    bootstrap, server-side authentication bootstrap, and server listener/resource-policy
    bootstrap and Raft-peer transport bootstrap are now storage-owned. Raft checkpoint-path
    derivation, checkpoint/sidecar coordination, serving-read checkpoint state, WAL checkpoint
    policy and monitor execution, and peer checkpoint callbacks are now contained by one
    authority-bound storage capability. The steady-state logical command, clock/lease-horizon,
    heartbeat, runtime-map, administration, and RPC-serving implementation is now contained by
    the authority-issued storage host. Durable deployment startup is now a two-phase storage-owned
    typestate: durable replay and validation precede process-owned socket binding, after which one
    consuming operation starts the peer/monitor lifecycle, initializes and checkpoints membership,
    waits for leadership or catch-up, establishes certified topology and outer identity, publishes
    initial durability, and returns the bound steady-state service. The low-level authority startup
    operations it replaces are production-private and boundary-checked.
13. **Complete:** replace cross-crate `StoreError` variant matching for request policy and
    diagnostics with exhaustive semantic classifications owned by storage.
    `StoreOperationFailureClass` now
    gives request coordinators only resource exhaustion, metadata-command contention, retryable
    convergence, or an unclassified semantic failure; storage exhaustively maps every local,
    nested shard, and RPC representation to that contract. Designated `server-core` translation
    adapters still receive concrete `StoreError` values from existing public storage operation
    signatures; they may query the owner-provided class but neither match nor render the concrete
    representation. When an unclassified failure must be retained by `ServerError`, `StoreFailure`
    consumes it and preserves only the semantic request class and a separate exhaustive, bounded
    operator category distinguishing absence, integrity, topology, metadata contention or
    consistency, exhaustion, RPC transport/protocol, schema, I/O, database, codec, and internal
    failures.
    Public diagnostics contain that category but never PG, shard, route, command-log, node,
    operation, path, database, or RPC values; `Display` and `Error::source` remain opaque.
    At item 13 completion, the then-exported `BucketWriteDrainError`,
    `BucketSnapshotLoadError`, and `ObjectPgActionError` wrappers exposed bounded labels and had
    redacted public formatting and error chains, so production diagnostics could not observe
    their nested implementation errors before protocol conversion. Item 14 subsequently made all
    three wrappers crate-private and removed concrete implementation-error transit. Item 13
    therefore established the policy and rendering contract which item 14 used to complete the
    structural boundary.
    Metadata contention interpretation is likewise owner-provided. The storage-node intermediate
    class is private, owner-local tests pin the classifications, bounded category coverage, and
    redaction, cross-crate classification tests use opaque semantic fixtures, and
    `check-storage-cluster-boundaries` rejects production variant matching or reconstruction of
    the removed storage-node policy.
14. **Complete — production error, debug, and dependency containment.** Completed on 2026-08-08.
    The Phase 5 test-support
    work in [`storage-boundary-compiler-enforcement-plan.md`](storage-boundary-compiler-enforcement-plan.md) completed on 2026-08-07. Raw
    stream/direct-PUT cleanup assertions, UploadPartCopy shard-loss injection, backfill and
    retained-placement fixtures, lifecycle/reclaim generation observations, and broad multipart
    state-model records are now storage-owner tests or opaque/logical owner-defined scenarios.
    Item 14 no longer owns any impossible-state or cross-crate test-fixture migration. Its
    bounded production and dependency cleanup completed as follows:

    1. **Complete — Structural error containment.** `StoreError`, `MetadataError`,
       `ObjectPgActionError`, and `ShardIoError` are crate-private storage implementation errors.
       The bucket-write-drain sub-slice completed on 2026-08-07:
       `BucketWriteDrainError` is crate-private, public bucket-delete operations return the opaque
       `BucketWriteDrainFailure` and exhaustive logical `BucketWriteDrainFailureKind`, raw
       classification tests are storage-owned, and a separate private bounded diagnostic category
       preserves safe store and metadata failure domains without retaining implementation errors.
       Cross-crate response tests use an owner-provided logical fixture, the stale-authorization
       regression drives the admitted production mutation from the authorization result, and the
       boundary check rejects raw drain-error transit or re-export.
       The bucket-snapshot sub-slice completed on 2026-08-07. `BucketSnapshotLoadError` is now
       crate-private; admitted bucket, direct logical read, lifecycle test-support, and bounded
       test-hook entry points return opaque `BucketSnapshotLoadFailure` values. Its exhaustive
       logical kind preserves throttling/convergence, operation-selected metadata contention,
       bucket absence/not-empty, missing-upload, and invalid-versioning outcomes, while a shared
       private diagnostic category retains only bounded store or metadata failure domains. Raw
       impossible-state tests use crate-private owner helpers, cross-crate translation tests use
       owner-provided logical fixtures, and the boundary check rejects raw snapshot-error transit
       or re-export.
       The object-read authorization sub-slice completed on 2026-08-07. Public admitted object-read
       capabilities now return opaque `ObjectReadFailure` values whose exhaustive logical kind is
       limited to object absence, resource exhaustion, metadata-command contention, retryable
       convergence, or internal failure. Storage owns conversion from object-PG, metadata, route,
       and payload-retention errors and preserves only a bounded diagnostic category. Missing-object
       translation remains context-specific in `server-core`, including the AccessDenied,
       NoSuchKey, and NoSuchVersion distinction, while all other outcomes are matched exhaustively.
       Internal failures remain typed as `ServerError::ObjectRead`, so HTTP request diagnostics
       retain the bounded storage category and cause chain without retaining implementation
       errors. Unused direct unadmitted storage-cluster read helpers were removed and the leased
       owner-test helper is test-only and crate-private. Cross-crate response tests use owner-provided logical
       fixtures, and the boundary check rejects raw error return types from every synchronous,
       leased, and payload-retention read entry point or their reintroduction into the
       authorization seam.
       The object-metadata mutation sub-slice completed on 2026-08-07. Public admitted tag,
       retention, legal-hold, ACL, and delete capabilities now return opaque
       `ObjectMetadataMutationFailure` values with an exhaustive logical kind for object absence,
       resource exhaustion, metadata-command contention, retryable convergence, and internal
       failure. Storage owns the common object-operation classification and retains only a bounded
       diagnostic category. `server-core` continues to select operation-specific policy:
       PutObjectTagging maps contention to `OperationAborted`, conditional delete maps it to
       `ConditionalRequestConflict`, other mutations throttle it, and missing-object disclosure
       remains authorization- and version-aware. Internal failures remain typed through
       `ServerError::ObjectMetadataMutation`, preserving bounded request diagnostics without raw
       storage errors. Unused direct legal-hold and retention readers were removed; remaining
       owner-test mutation/read helpers are crate-private and test-only. Owner-local exhaustive
       classification and redaction tests, cross-crate logical mapping tests, and the boundary
       checker prevent raw error transit through this operation family. The superseded
       per-subject object-tag read RPC, its wire codec and admission branch, and its old
       split-read retry test were removed; current object-read snapshots already bind tags to the
       authorized stored-object identity atomically. Because that removal changed the accepted
       message-kind grammar, storage RPC frame encoding advanced from 15 to 16 rather than leaving
       an incompatible grammar under the old marker.
       The stream-upload sub-slice completed on 2026-08-07. Public admitted PutObject and
       UploadPart stream-session creation, load, heartbeat, segment append, finalization, abort,
       and retained-cleanup capabilities now return opaque `StreamUploadFailure` values. Its
       exhaustive logical kind preserves multipart-upload and session absence/state, duplicate
       segment, resource exhaustion, metadata contention, retryable convergence, client-visible
       invalid-request, and internal outcomes; the wrapper retains only the upload ID or
       client-visible validation reason needed for unchanged S3 translation plus a bounded private
       diagnostic category. UploadPart continues to translate missing or terminal stream sessions
       to `NoSuchUpload`, duplicate segments remain `InvalidRequest`, and general convergence
       failures remain `SlowDown`. Lease maintenance during CopyObject segment writes now crosses
       an internal generic error channel, so callback failure identity is preserved without adding
       a forgeable raw-error sentinel. Internal failures remain typed through
       `ServerError::StreamUpload`, including bounded request diagnostics and HTTP redaction. The
       direct logical stream-session loader used by higher-layer tests also returns the opaque
       failure. The raw stream-session abort primitive is crate-private; the sole cross-crate abort
       test capability is feature-gated and returns the same opaque failure. Owner-local
       classification/redaction tests, exhaustive cross-crate mapping tests,
       HTTP sanitization coverage, and the boundary checker reject raw error transit through this
       streaming seam.
       The direct/buffered PutObject sub-slice completed on 2026-08-07. Admitted existing-object
       authorization reads, generation reservation, payload staging, commit, explicit discard,
       and the test-only route probe now return opaque `DirectPutFailure` values. Its exhaustive
       logical kind is limited to resource exhaustion, metadata-command contention, retryable
       convergence, and internal failure; conditional-request and object-existence decisions
       remain on the typed commit callback. Crossed payload capabilities, stale snapshots, and
       other impossible route states therefore fail internally instead of exposing storage
       invariant text as client-visible `InvalidRequest`. Storage retains only a bounded private
       diagnostic category, while `ServerError::DirectPut` preserves that category through request
       telemetry and redacts it from HTTP responses. The unadmitted raw reservation helper is now
       owner-test-only, the release helper is crate-private, and the shared logical existing-live-
       object loader reuses `ObjectReadFailure`. Owner-local direct-PUT state-machine tests,
       exhaustive classification and response-mapping tests, HTTP redaction coverage, and the
       boundary checker reject raw error transit or renewed public low-level mutation entry points.
       The admitted multipart-management and completion sub-slice completed on 2026-08-07.
       Multipart upload lookup for UploadPart, completion, abort, and ListParts; in-progress target
       validation; authorized part listing and abort; and test-only route probes now return opaque
       `MultipartManagementFailure`. Its exhaustive logical kind contains only upload absence,
       resource exhaustion, metadata-command contention, retryable convergence, and internal
       failure. Completion snapshot loading and publication return the separate opaque
       `MultipartCompletionFailure`, whose exhaustive kind additionally preserves missing-part,
       stale-snapshot, and conditional-conflict outcomes required by the completion retry and S3
       response policy. Only the logical missing part number crosses with that failure; upload IDs
       are taken from the already-authorized request rather than retained from implementation
       errors. Internal failures remain typed through distinct `ServerError` variants, preserving
       bounded storage diagnostics and HTTP redaction. The former unadmitted UploadPart lookup was
       removed, and raw owner-test completion/abort helpers are test-only and crate-private.
       Owner-local crossed-capability, route-expiry, exhaustive classification, and redaction
       tests; exhaustive coordinator mapping tests; the full multipart suites; HTTP sanitization;
       and the boundary checker reject raw errors on every public admitted multipart operation.
       The account-wide bucket-listing sub-slice completed on 2026-08-07. Admitted ListBuckets
       scans now return the opaque `BucketListingFailure`; its exhaustive logical kind contains
       only resource exhaustion, metadata-command contention, retryable convergence, and internal
       failure. Storage owns conversion from every bucket-PG fan-out, route-validity, metadata,
       and RPC error, while the coordinator preserves the existing `SlowDown` policy for the three
       retryable classes and retains internal failures through a typed `ServerError` variant with
       bounded diagnostics and HTTP redaction. The raw owner-local listing helper is test-only and
       crate-private. Owner-local exhaustive classification, route-expiry, and redaction tests;
       exhaustive coordinator response mapping; HTTP sanitization; and the boundary checker reject
       raw error transit through the public admitted bucket scan.
       The bucket-wide object-metadata listing sub-slice completed on 2026-08-07. Admitted
       ListObjectsV2, ListObjectVersions, and ListMultipartUploads scans now return the shared
       opaque `ObjectMetadataListingFailure`; its exhaustive logical kind contains only resource
       exhaustion, metadata-command contention, retryable convergence, and internal failure.
       Storage owns conversion from every per-PG fan-out, route-validity, metadata, and RPC error,
       while the coordinator preserves the existing `SlowDown` policy for all three retryable
       classes and retains internal failures through a typed `ServerError` variant with bounded
       diagnostics and HTTP redaction. The raw listing helpers are test-only and crate-private.
       Owner-local exhaustive classification, route-expiry, and redaction tests; exhaustive
       coordinator response mapping; HTTP sanitization; and the boundary checker reject raw error
       transit through all three public admitted scans.
       The first lifecycle-maintenance sub-slice completed on 2026-08-07. Lifecycle root
       discovery, claim acquisition, heartbeat, error recording, and release, plus bucket-wide
       object, version, and multipart candidate scans, now return the opaque
       `LifecycleMaintenanceFailure`. Its exhaustive logical kind contains only resource
       exhaustion, metadata-command contention, retryable convergence, and internal failure.
       Storage owns conversion from claim storage, per-PG fan-out, route-validity, metadata, and
       RPC errors. The runtime preserves the existing `SlowDown` policy for all three retryable
       classes and retains internal failures through a typed `ServerError` variant with bounded
       diagnostics and HTTP redaction. The raw lifecycle bucket-inventory helper is crate-private.
       Owner-local exhaustive classification and redaction tests, exhaustive runtime mapping,
       HTTP sanitization, and the boundary checker reject raw errors through this discovery,
       coordination, and candidate-scan seam. Lifecycle mutation publication is recorded
       separately below because its command-ownership and callback outcomes require a distinct
       contract.
       The lifecycle-mutation publication sub-slice completed on 2026-08-07. Current-object
       expiration, noncurrent-version expiration, expired-delete-marker removal, due multipart
       abort, and resumed abort completion now return the separate opaque
       `LifecycleMutationFailure`. Storage owns conversion from command ownership, recovery,
       route-validity, metadata, database, and RPC errors while preserving the lifecycle-policy
       callback as an independent nested result. Its exhaustive logical kind contains only
       resource exhaustion, metadata-command contention, retryable convergence, and internal
       failure. The runtime preserves the existing `SlowDown` behavior for the three retryable
       classes and retains internal publication failures through a typed `ServerError` variant
       with bounded diagnostics and HTTP redaction. The raw mutation implementations are
       crate-private; their visibility beyond `request_ops` exists solely for storage-owner
       partial-publication recovery tests. The former generic raw operation mapper is now
       test-only in `server-core`. Owner-local exhaustive classification,
       partial-publication recovery, and redaction tests; exhaustive runtime mapping; the existing
       lifecycle sweep and contention regressions; HTTP sanitization; and the boundary checker
       reject raw errors through the complete lifecycle mutation seam.
       The request route-admission sub-slice completed on 2026-08-07. Acquiring an admission,
       revalidating it against its issuing publication domain, inspecting its remaining absolute
       validity, deriving every active bucket/object/PutObject/multipart scan or mutation route,
       and retaining stream-upload cleanup authority now return the existing opaque
       `StoreFailure`. Storage keeps the exact route-map expiry and publication-domain mismatch
       errors behind crate-private validation methods used by admitted operation retry closures.
       `server-core` exhaustively maps the three retryable semantic classes to `SlowDown` and
       retains only the bounded diagnostic category for an unclassified failure; route
       construction no longer enters the raw `StoreError` adapter. `server-http` can bound body
       reads and distinguish an expired admission without observing the underlying route error.
       Owner-local expiry, crossed-domain, remaining-validity, classification, and redaction
       regressions plus the repository boundary check prevent raw errors from returning through
       this common runtime authority seam or being routed back through `map_store_error`.
       The object-payload read sub-slice completed on 2026-08-08. Both retained streaming-read
       authority and cluster-backed opaque-segment reads now return the existing
       `ObjectReadFailure`; the coordinator exhaustively preserves `SlowDown` for resource,
       contention, and convergence outcomes while retaining internal read failures only through
       the bounded storage-owned diagnostic category. The physical stored-byte request type, its
       fields, and the inspection/repair methods that accept it are crate-private; placement-epoch
       selection remains storage-private, and the unbound physical segment reader is
       owner-test-only. This removed the final production caller of the generic raw
       `map_store_error` adapter, which is now compiled only for legacy mapping regressions.
       Owner-local crossed-segment, stale-cluster, classification, and redaction tests; the
       existing coordinator response and HTTP sanitization coverage; and the boundary checker
       prevent raw payload-read errors or a production generic adapter from crossing storage.
       The cross-crate generic-error sub-slice completed on 2026-08-08. `server-core::ServerError`
       no longer retains `MetadataError` or implements generic conversion from `MetadataError` or
       `StoreError`; the obsolete raw object-PG and store-error translation adapters and their
       representation-based tests were removed. The one remaining public payload-read lease now
       returns `ObjectReadFailure`, including subject mismatch and route/placement failures. HTTP
       redaction and coordinator policy tests consume storage-owned opaque fixtures. Cross-crate
       shard-write, metadata-command, bucket-delete scheduling, and reclaim test hooks likewise
       inject or return opaque storage-owned failures rather than letting higher layers construct
       implementation errors. The boundary checker now rejects every `StoreError`,
       `MetadataError`, or `ObjectPgActionError` reference outside storage, including test and
       feature-gated code, and separately inventories the payload-read lease method.
       The payload test-support error sub-slice completed on 2026-08-08. The feature-gated
       `StorageClusterPayloadTestSupport` trait and its cross-crate payload-loss, corruption,
       repair, lease, and snapshot helpers now return the opaque `TestStorageFailure`. Storage
       consumes the underlying store or object-PG error at the owner boundary and retains only a
       bounded diagnostic cause label through private conversion methods; public `Debug`,
       `Display`, and `Error::source` cannot reveal PG, shard, placement, route, database, or RPC
       details. Durable repair-error text is asserted owner-locally, while higher-layer tests
       observe only whether the selected repair has a recorded error. Existing higher-layer tests
       otherwise continue to consume only logical snapshots, predicates, and fault capabilities.
       Owner-local redaction/rendering coverage and the boundary checker reject raw implementation
       errors, public raw-error conversion implementations, or rendered repair diagnostics from
       this payload-support family.
       The metadata-command test-support error sub-slice completed on 2026-08-08. Capturing opaque
       object command-state evidence now returns `TestStorageFailure`; storage consumes failures
       from subject routing, primary selection, and proof capture before the owner boundary. The
       public apply hook already accepted only the opaque `TestInjectedStorageFailure`, whose raw
       representation can be constructed and recovered only inside storage. Cross-crate tests
       continue to select only logical bucket/object subjects and command kinds. The boundary
       checker rejects raw implementation-error types anywhere in the public metadata-command
       support trait and pins the state-capture method to the opaque failure.
       The lifecycle/reclaim test-support error sub-slice completed on 2026-08-08. Bucket-metadata
       holds, lifecycle claim setup, durable upload-state transitions, lifecycle object evidence,
       payload leases, and reclaim-root setup and observation now return `TestStorageFailure` when
       they do not already use the narrower bucket failure contracts. Storage retains and
       interprets missing reservations before conversion, and consumes all other routing,
       metadata, payload-snapshot, lease, and reclaim errors inside the owner. Validation detail
       for impossible test states is reduced to the bounded `invalid_request` label; owner-local
       coverage pins its `Debug`, `Display`, and error-source redaction. The boundary checker scans
       the complete public lifecycle trait and pins every public lifecycle/reclaim helper to the
       opaque failure. Stream-session and multipart-upload inventory helpers remain together in
       multipart test support rather than being split by their use in cleanup tests.
       The multipart test-support error sub-slice completed on 2026-08-08. Completion candidates,
       part observations, committed-object part lists, owner-defined corruption injection,
       stream-session inventories, upload inventories and identities, creation-state observations,
       and generation-binding checks now return `TestStorageFailure`. Storage interprets absent
       uploads before conversion and consumes every other metadata, routing, stream-session,
       manifest, and object-PG error inside the owner. The exact crossed-subject error regression
       remains owner-local through a crate-private raw state observer; the cross-crate state
       observer is opaque. The boundary checker scans the complete multipart trait, pins every
       public stream-session and multipart helper to the opaque failure, and requires the raw
       observer to remain crate-private.
       The object/checksum test-support error sub-slice completed on 2026-08-08. Stored SSE-C
       checksum observations and delete-marker owner checks now return `TestStorageFailure`, with
       storage consuming object lookup, checksum decoding, and logical validation failures. The
       exact non-delete-marker validation regression remains owner-local through a crate-private
       raw observer. The boundary checker scans the complete object-support trait and requires the
       owner raw observer to remain crate-private.
       The topology and stream-session setup error sub-slice completed on 2026-08-08. The logical
       current-object placement and exact PutObject-session placement observations now return
       `TestStorageFailure`; storage consumes object lookup, session inventory, reservation, and
       subject-validation failures. Exact crossed-session validation remains owner-local through a
       crate-private raw observer. The one higher-layer cleanup-deadline fixture now creates its
       session through `StorageClusterStreamSessionTestSupport`, and both raw stream-session
       creation methods are crate-private. The boundary checker rejects raw errors throughout the
       public topology and stream-session traits, pins these three opaque return types, and requires
       their raw owner helpers to remain crate-private.
       The physical shard-write visibility sub-slice completed on 2026-08-08. The raw
       `StorageCluster` shard writer and `LocalClusterMap` shard writer and repair method are now
       crate-private; all callers were already storage-owned payload placement, repair, tracing, or
       owner-local tests. The boundary checker pins those three methods to crate visibility and the
       repository-wide concrete-error scan now rejects `ShardIoError` outside storage alongside the
       other raw implementation errors.
       The route-history query visibility sub-slice completed on 2026-08-08. Route-history
       reference collection and historical-route reconstruction on `StorageCluster` and
       `LocalClusterMap` are now crate-private; all callers were already storage-owned routing,
       recovery, maintenance, control-plane, or owner-local test code. The heartbeat and
       control-plane history values remain storage-owned protocol representations, while the
       cluster implementation methods which can return `StoreError` no longer form a public API.
       The three reference-aggregation methods and their node-client dispatch branch are compiled
       only for owner tests because the visibility change proved that they had no production
       caller; the storage-node RPC operation remains part of the current wire grammar. The
       boundary checker pins all five owner methods to crate visibility.
       The direct stream/multipart primitive visibility sub-slice completed on 2026-08-08.
       Direct PutObject-session heartbeat and finalization, UploadPart finalization, and raw
       multipart abort wrappers now compile only for owner tests; production already reaches those
       state machines through admitted operation-route capabilities. The lower UploadPart-session
       constructors used by owner tests are test-only. A raw authorized list-parts wrapper and an
       exported test-hook UploadPart-session constructor had no workspace callers and were removed
       rather than retained as dormant raw-error APIs; production list-parts uses the admitted
       route-validation state machine directly. The boundary checker pins all six owner-test
       wrappers and removal of both unused constructors.
       The payload-lease primitive visibility sub-slice completed on 2026-08-08. The opaque,
       subject-bound `ObjectPayloadLease` and `RetainedObjectPayloadRead` capabilities remain the
       cross-crate read boundary. Direct generation-wide lease acquisition is now crate-private and
       compiled only for storage test support and owner tests, including its lower cluster-map
       constructor; location-expanded acquisition is crate-private for storage's read composition
       and owner tests. The two direct lease-count observations and the lower bucket-wide node
       aggregation they use are owner-test-only. The boundary checker rejects renewed public raw
       lease constructors and pins the count helpers at every cluster/node layer to their test-only
       visibility.
       The physical-placement representation sub-slice completed on 2026-08-08. `ShardLocation`
       and the placed-shard health, validation, and risk records are now crate-private and are no
       longer root exports. Placement expansion, historical segment-location reconstruction,
       payload data-PG selection, and shard-node selection on public cluster/map handles are
       crate-private; the multipart-part data-PG predictor and local-map shard-node convenience
       wrapper compile only for owner tests. Storage RPC validation and encoding continue to
       translate through the private representation inside the owner. The boundary checker pins
       the private types, removed exports, method visibility, and test-support-only wrappers.
       Review then found that `PgTopology` still publicly exposed the lower payload data-PG set,
       segment, and multipart selectors, and the backfill UAT predicted those physical values
       directly. Those selectors are now crate-private or implementation-private. The UAT asks a
       feature-gated storage test facility for one of its two required logical key-placement
       relations and receives only the candidate key and data-PG identifier needed to drive the
       external topology transition. Ordinary AWS-facing `s3-tests` builds do not enable storage
       test hooks; both UAT launchers explicitly request the required feature. The boundary checker
       covers the lower topology API, external raw-selector calls, the test facility, and its Cargo
       feature gate. Because Cargo omits a binary whose required feature is disabled, the UAT's
       parser, committed-result predicate, search bound, and their unit tests live in the
       default-built `s3-tests` library target. Ordinary workspace `nextest` therefore continues to
       discover every regression without a second feature-specific test invocation.
       A compiler visibility audit performed after that conversion proved the earlier final-export
       inventory incomplete. `ClusterBuildError::OpenLocalNode` and
       `StorageNodeServerError::Store` now consume their concrete causes into bounded
       `StoreFailure` values; public formatting and error chains are redacted, while owner-only
       regressions retain exact lower-layer cause coverage. Reclaim/probe helpers and shard-key
       parsers which had no external logical contract are crate-private. The public route-history
       collection constructor now reports a narrow logical reference-limit error. The raw error
       module and all four concrete error types are crate-private and no longer root exports.

    2. **Complete — Debug-PG containment.** Completed on 2026-08-07. The bucket-delete,
       metadata-checkpoint, and object-payload-placement endpoints now consume opaque,
       storage-rendered diagnostics through coordinator methods. HTTP supplies only logical bucket,
       key, or opaque storage-selector text and exhaustively maps each diagnostic's bounded outcome;
       storage owns the selector grammar, constructs `PgId`, performs and interprets checkpoint
       work, loads object snapshots, renders physical placement, and reduces failures to bounded
       diagnostic labels. The coordinator contains no PG type, numeric selector, or PG-named value.
       `MetadataCommandCheckpointRecordSummary`, its raw checkpoint method, placement rendering,
       and placement errors are storage-private. Owner-local exact/redaction tests cover successful
       checkpoint rendering and both diagnostic failure domains; HTTP tests cover successful and
       owner-redacted conflict responses. The boundary checker rejects direct `PgId`, raw checkpoint
       summary/method, snapshot loader, or placement renderer/error use in `server-http`, including
       feature-gated debug code, rejects `PgId` in the coordinator seam, requires both coordinator
       and storage checkpoint diagnostics to accept `selector: &str`, and rejects renewed public
       visibility for the private storage primitives.

    3. **Complete — HTTP EC dependency containment.** Completed on 2026-08-07. The three
       duplicated HTTP test-cluster constructors were replaced by one storage-owned default test
       cluster facility whose signature exposes no PG, node, shard, or EC representation. The
       response redaction test now consumes a server-core-owned semantic fixture which supplies
       both the opaque internal failure and the private fragments that must remain redacted.
       `server-http` no longer has a direct EC dev-dependency, chooses no storage topology, and
       constructs no EC implementation error. The boundary checker rejects both a renewed direct
       dependency and `ec::` source use anywhere under `server-http`.

    4. **Complete — Final enforcement.** `check-storage-cluster-boundaries` rejects concrete
       `StoreError`/`MetadataError` and raw operation-wrapper use outside storage, their public
       export from storage after migration, and a direct `ec`
       dependency or `ec::` source use in `server-http`. Do not use an indiscriminate repository-wide
       representation text ban: enforce the actual non-owner production and feature-gated seams
       while retaining owner-local storage tests. The debug-PG portion now rejects the specific
       HTTP escape hatches rather than owner-local storage uses, and the HTTP EC portion rejects
       both dependency and source-level representation use. It now also pins the private error
       module and declarations, rejects renewed raw exports or startup-wrapper fields, and exercises
       those checks against a negative fixture. Crate visibility is the primary implementation-error
       boundary, with warnings-denied compiler checks detecting any effective-public raw signature.

After items 9 through 14 are complete, format work proceeds through the maintained
[format ledger](../../guides/storage-format-ledger.md) rather than reopening containment
opportunistically.
