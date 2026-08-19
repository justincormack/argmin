# Internal Format Ownership, Upgrade And Versioning Plan

Status: Phase 0 complete; Phase 1 containment complete; Phase 2 evidence audit in progress

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

The Phase 2 evidence inventory below re-audits these at the first production rejecting reader and
through startup or dispatch. It records missing neighbouring versions and does not treat the
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
| PG SQLite schema | schema version 3 | `objects`, `multipart_uploads`, and `stream_uploads` store user and system metadata blob columns. |
| Metadata command | encoding version 8 | Object, multipart-upload, and stream-session command values carry both opaque blobs. |
| Storage-node RPC | frame encoding version 21 | Logical object, multipart, and stream request/response payloads carry both opaque blobs. |
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
| PG SQLite schema | schema version 3 | `objects`, `multipart_uploads`, and `stream_uploads` store optional framed object tags. |
| Metadata command | encoding version 8 | Object, multipart-upload, and stream-session command values carry the opaque tag set. |
| Storage-node RPC | frame encoding version 21 | Logical object, multipart, stream, mutation, and tag-read payloads carry the opaque tag set. |
| Canonical PG state | encoding version 5 | The tag columns participate in canonical row and state digests. |
| Metadata command checkpoint | encoding version 2 | Checkpoint table blocks carry tag columns and bind them into row, table, state, and checkpoint digests. |

The Phase 2 gate below records the `s3-types`-owned inner frame rather than treating these storage
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
| PG SQLite schema | schema version 3 | `bucket_subresources` stores framed bucket tags under the private tagging discriminator. |
| Metadata command | encoding version 8 | Bucket-subresource put/delete commands carry the discriminated typed tag mutation. |
| Storage-node RPC | frame encoding version 21 | Bucket snapshots, typed reads, and subresource mutations carry the opaque bucket-tag set. |
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
| PG SQLite schema | schema version 3 | `buckets`, `objects`, and `multipart_uploads` store framed ACL carriers. |
| Metadata command | encoding version 8 | Bucket, object, multipart-upload, create, commit, and ACL mutation records carry framed ACL carriers. |
| Storage-node RPC | frame encoding version 21 | Logical bucket, object, multipart, stream-commit, and ACL mutation messages carry framed ACL carriers. |
| Canonical PG state | encoding version 5 | The three persisted ACL columns participate in canonical row and state digests. |
| Metadata command checkpoint | encoding version 2 | Checkpoint table blocks carry all three ACL columns and bind them into row, table, state, and checkpoint digests. |

The Phase 2 gate below records the `s3-types`-owned inner frame. Its first introduction advanced
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
| PG SQLite schema | schema version 3 | `objects`, `multipart_uploads`, and `stream_uploads` store `encryption_type` plus `encryption_state`. |
| Metadata command | encoding version 8 | Object, multipart-upload, and stream-session command values carry the discriminator and nested state bytes. |
| Storage-node RPC | frame encoding version 21 | Logical object, multipart, and stream request/response payloads carry the discriminator and nested state bytes. |
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

The sealed checksum payload is a separate nested format audited in Phase 2 below. A change to its
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
| Storage-node RPC | `STORAGE_RPC_FRAME_ENCODING_VERSION = 21` in `storage_rpc.rs`; frame magic, message-kind tags, checksums, and payload codecs are crate-private. | Binding version 2 and transport-envelope version 1 in `storage_rpc_auth.rs`. | Exact versions are required before dispatch. There is no negotiation. Treat any other version as incompatible until mixed-version operation is designed. |
| Control-plane RPC | `CONTROL_PLANE_RPC_VERSION = 14` in `control_plane.rs`; the frame contains magic, version, request kind, length, checksum, and payload. | Shared control-plane authentication-envelope version 1 in `control_plane_auth.rs`. | The frame and auth decoders reject non-current versions before logical dispatch. There is no negotiation. Treat any other version as incompatible. |
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

All Phase 1 exit criteria above are now satisfied and boundary-checked. Phase 2 therefore begins
with the evidence audit below; it does not reopen containment or introduce compatibility readers.

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
- Every versioned durable, wire, transport, configuration, digest, and database format must have
  frozen, complete owner-local representation evidence. A leaf format or independently versioned
  outer grammar has an immutable manifest keyed by its own version. A composite containing
  independently versioned nested payloads additionally has append-only fixtures keyed by the
  complete relevant version vector. An incompatible grammar change requires a new owning version;
  an inner-only advancement with unchanged outer grammar appends a new composite-vector fixture
  without rewriting the old fixture or advancing the outer version.
- A SQLite schema version applies that general rule to the complete physical schema. Adding,
  removing, or changing any table, column, constraint, index, foreign key, view, or trigger requires
  a new schema version and a new manifest.

This phase still does not implement upgrade steps. It only creates a clean baseline that a
later upgrade framework can reason about.

### Phase 2 Evidence Gate (2026-07-30)

Phase 2 starts with an owner-by-owner evidence audit, not with speculative format changes. A row
is `recorded` only when the plan identifies the private current marker, the current writer, the
owner-side rejection point, and permanent tests for all applicable cases:

1. the current writer always emits the current marker;
2. missing, too-old, and too-new versions fail before dispatch, mutation, or publication;
3. malformed magic and unsupported version are separate failures for magic-plus-version formats;
4. exact current bytes or equivalent sealed evidence pin the marker location and byte order,
   separating immutable outer grammar from composites containing independently versioned payloads;
5. authenticated or checksummed formats have resealed unsupported-version fixtures, so a checksum
   failure cannot accidentally stand in for version rejection; and
6. a nested representation either carries its own private marker or has an explicit rule binding
   every incompatible change to advancement of all containing formats.

The evidence in item 4 is append-only and version-indexed, not merely an exact fixture for whatever
the current writer happens to emit today. A format containing an independently self-describing
nested payload has two evidence layers:

- immutable outer-grammar evidence keyed only by the outer version, covering the outer marker,
  fields, ordering, widths, discriminants, and framing while treating nested bytes as an opaque
  field; and
- immutable composite fixtures keyed by the complete relevant version vector, for example
  `(replicated_snapshot_v1, control_plane_state_v28)`. Advancing only the inner version appends a
  new vector entry and retains the previous entry; it does not alter the outer-v1 grammar evidence.

If an inner change alters its containing field's width, ordering, optionality, framing, or other
outer grammar, the outer version advances as required by the dependency ledger. The representation
appropriate to each format family is:

- self-describing binary formats: the complete header, field order and widths, discriminants,
  optionality, and a mechanically exhaustive all-variant/all-branch exact-byte corpus;
- RPC and transport formats: the complete message-kind registry plus exact payload and outer-frame
  fixtures, including authenticated or checksummed unsupported-version fixtures;
- canonical text/configuration formats: the complete key/grammar manifest and exact canonical
  output fixtures;
- digest, hash-chain, and cryptographic binding formats: frozen domain separators, tag and field
  order, input encodings, and exact vectors;
- SQLite formats: the complete canonical physical catalogue described below; and
- unversioned nested representations: an explicit dependency ledger naming every containing
  version that must advance, with sealed evidence in every direct container.

Historical evidence remains even while its reader is unsupported. Retaining a manifest, exact
fixture, or rejection vector does not authorize a compatibility decoder. Existing `Recorded`
statuses must be re-audited against this append-only rule; a mutable current-only fixture does not
satisfy it.

Corpus completeness must be enforced rather than asserted by convention. Where a representation
has variants, message kinds, request/success/error shapes, or optional branches:

1. Compare fixture case identities against the authoritative production registry; do not maintain
   a disconnected test-only list that can omit a new production kind.
2. Construct cases through compiler-exhaustive matches over production enums, without wildcard or
   catch-all arms. Adding a variant must cause compilation failure until its evidence is supplied.
3. Construct framed structs without update syntax or defaults that can silently fill a new field,
   and explicitly cover present and absent forms of every optional field.
4. Require exact set equality between the production registry and fixture coverage for request,
   success, error, and optional payload families as applicable. Duplicate cases and unknown fixture
   identities fail as well as missing cases.
5. Where Rust's type system cannot enumerate a structural branch, expose a private owner registry
   from the production codec and compare it with the sealed corpus. A source-code grep is not
   sufficient completeness evidence.

`Evidence required` means a marker exists in current code but the complete evidence above has not
yet been consolidated in this matrix. `Design required` means the representation has no
self-describing marker and its outer-version binding must be made explicit before deciding whether
to add an inner frame. Neither status permits adding a fallback reader.

| Boundary family | Owner | Current candidate baseline | Gate status |
| --- | --- | --- | --- |
| PG SQLite schema and physical layout | `storage` | `PRAGMA user_version = 3`; version zero is valid only with no user schema objects | Recorded: immutable manifests retain the distinct physical catalogues for versions 1, 2, and 3. Version 2 introduced framed ACL defaults and proof-carrier version columns; version 3 added `metadata_command_pending_slot.publication_started`. A fresh database built through the production initializer must exactly match the manifest selected by the current version, while nonempty version-zero state and versions 1, 2, and 4 are rejected before initialization. |
| Applied metadata-command envelope | `storage` | magic `argmin-metadata-command`; command encoding 8 | Recorded: the exact v8 all-variant checksum corpus and rejected v7 append fixture are supplemented by independently resealed v7/v9 pending-slot and applied-log recovery fixtures and correctly authenticated current-RPC v7/v9 rejection before mutation dispatch |
| Abandoned metadata-command record | `storage` | magic `argmin-metadata-command-abandoned`; abandoned-command encoding 1 | Recorded: exact current bytes, typed framing/version failures, and resealed persisted old/new-version rejection before recovery mutation or publication |
| Metadata-command log hash chain | `storage` | private `ARGMIN-METADATA-COMMAND-LOG-V1` hash domain and opaque fixed-width `MetadataCommandLogHash` carrier version 1 | Recorded for the coordinated carrier slice: PG hash columns remain schema-bound and every exported proof carries the independent version and value; broader command-log-format evidence remains a separate row below |
| Metadata checkpoint payload | `storage` | private `ARGMCPKT` plus big-endian `u16` checkpoint encoding 2 | Recorded: exact sealed v2 bytes, typed direct rejection, correctly resealed framed v1/v3 and unframed-v1 rejection through catalogue loading, and authenticated install rejection before destination mutation |
| Canonical metadata state digest | `storage` | private domain `argmin.metadata.pg-state` and opaque fixed-width `CanonicalStateDigest` carrier version 5 | Recorded for the coordinated carrier slice: replica state and every exported proof carry the independent version and value, with exact digest and noncurrent-carrier fixtures |
| Storage-node persisted runtime configuration | `storage` | canonical text beginning `argmin-storage-node-runtime-config-v5`; every present PG read route contains both proof-carrier versions and values | Recorded for the coordinated carrier slice: exact v5 text, versions 4/6, incomplete/noncurrent carrier rejection, and restart/publication coverage are owner-local |
| Storage-node RPC frame | `storage` | magic `argmin-storage-rpc-frame`; frame encoding 21 | Recorded: exact current and historical bytes, typed framing/version failures, coordinated nested-format rejection, earliest streaming magic/version precedence, and unauthenticated plus authenticator-valid old/new server rejection before mutation dispatch |
| Storage-node RPC authentication binding | `storage` | magic `ARGSRPCB`; binding encoding 2 | Recorded: exact request/response bytes cover absent and present transcript tags, a fixed production-derived transcript digest seals its domain/length/input grammar, typed framing/version failures are distinct owner-locally, and authenticator-valid versions 0/3 are rejected before server dispatch or mutation |
| Storage-node RPC authenticated transport frame | `storage` | magic `ARGSRPCA`; transport encoding 1 | Recorded: exact v1 bytes, owner-typed framing/version failures, allocation-ordering evidence, and valid signed inner envelopes in server-facing versions 0/2 fixtures prove rejection before authentication, dispatch, or mutation |
| Control-plane logical state | `storage` | canonical text with leading `version=28` | Recorded: owner-typed missing/unsupported-version classification, a 3,596-byte representative aggregate sealing every record family and present/absent topology, lease, proof, transfer, prior-primary, route-reference, and pending-command branches, and versions 27/29 rejected through both single-authority open and replicated snapshot installation without mutation or publication |
| Control-plane command envelope | `storage` | magic `ARGCPCMD`; command encoding 15 | Recorded: owner-typed truncated/unknown-magic/unsupported-version failures, the sealed all-variant v15 aggregate, and correctly resealed versions 14/16 rejected directly, from a valid single-authority journal without replay or file replacement, and from a current peer-RPC container before OpenRaft dispatch or authority-state change |
| Control-plane replicated snapshot envelope | `storage` | magic `ARGCPSNP`; snapshot encoding 1 containing logical state 28 | Recorded: owner-typed truncated/unknown-magic/unsupported-version failures, exact v1 framing around canonical empty state v28, correctly resealed versions 0/2 rejected directly and before replicated state-machine replacement, and current peer-RPC containers receive the complete installation parse/invariant validation during admission before OpenRaft dispatch, checkpoint publication, or response publication |
| Control-plane RPC frame and payloads | `storage` | magic `argmin-control-plane-rpc`; frame/payload encoding 14 | Recorded: coordinated exact frame/proof grammar, owner-typed marker rejection, correctly checksummed versions 13/15 rejected before reservation, admission, or mutation, mutation response-loss confirmation, and an exhaustive exact request/success/semantic-error catalogue |
| Authenticated control-plane RPC payload binding | `storage` | untagged big-endian RPC kind plus logical payload, bound to RPC encoding 14 | Evidence required; kind binding is behavior-tested but has no exact signed fixture |
| Shared control-plane authentication envelope | `storage` | magic `ARGCPAUT`; authentication-envelope encoding 1 | Evidence required; role and verifier behavior is extensive, but current bytes and signed old/new rejection evidence are incomplete |
| Single-authority control-plane journal hash chain | `storage` | untagged CRC64 seed and transition algorithms, bound to journal-record encoding 2 | Recorded: fixed canonical-state-v28 seed, one-step and multi-step v15-command transition vectors, and exact journal-record-v2 bytes seal the algorithm, byte order, command contribution, and containing version |
| Shared durable-journal header and frame stream | `storage` | configured magic/version plus big-endian base offset, CRC64, and length/complement frames; bound to single-authority journal file 2, Raft WAL file 2, and Raft restart artifact 4 for logical-offset semantics | Design recorded below: incompatible shared framing changes advance both file versions, and offset changes also advance the restart artifact; joint exact evidence remains required |
| Authority-clock durable-state binding | `storage` | opaque 32-byte single-authority identity or Raft SHA-256 derivation domain `argmin-control-plane-clock-checkpoint-raft-v1\0`; carried without an inner marker | Design recorded below: width/semantic changes advance every containing format; exact derivation and cross-artifact fixtures remain required |
| Authority-clock restart checkpoint | `storage` | magic `ARGCPCLK`; checkpoint encoding 2 | Evidence required; resealed v1/v3 load rejection exists, but current bytes, typed rejection, and startup evidence are incomplete |
| Single-authority durable identity | `storage` | magic `ARGCPID\0`; identity encoding 1 | Evidence required; resealed v0/v2 reader rejection exists, but current bytes, typed rejection, and startup evidence are incomplete |
| Single-authority initialization marker | `storage` | magic `ARGCPINI`; marker encoding 1 | Evidence required; resealed v0/v2 reader rejection exists, but current bytes, typed rejection, and startup evidence are incomplete |
| Single-authority journal file | `storage` | magic `ARGCPSJL`; file encoding 2 using the shared durable-journal frame stream | Evidence required; resealed v1/v3 header rejection and startup framing behavior exist, but exact current file bytes and typed rejection are incomplete |
| Single-authority journal record | `storage` | magic `ARGCPSJR`; record encoding 2 | Evidence required; resealed v1/v3 reader rejection exists, but exact checkpoint/command bytes, typed rejection, and replay evidence are incomplete |
| Raft peer transport record | `storage` | untagged big-endian `u32` length prefix under the private `argmin-raft/1` TLS ALPN; the production record contains shared authentication envelope 1 | Design recorded below: incompatible length-framing changes advance the Raft peer RPC and ALPN baselines; exact framing evidence remains required |
| Raft peer RPC frame and payloads | `storage` | magic `ARGMINCPRAFTPEER`; peer RPC encoding 2, carried as the payload of shared authentication envelope 1 | Evidence required; current round trips and resealed version 3 rejection exist, but current bytes, version 1, typed rejection, and authenticated no-dispatch evidence are incomplete |
| Shared OpenRaft logical value encoding | `storage` | untagged big-endian OpenRaft primitive and wrapper codecs; the exact peer RPC 2, Raft restart artifact 4, and Raft WAL frame 1 dependency set varies by codec as recorded below | Design recorded below: incompatible changes advance exactly the applicable containing formats; per-container exact evidence remains required |
| Raft restart artifact | `storage` | magic `ARGMINCPRAFT`; restart encoding 4 | Evidence required; current round trip, checksum rejection, and direct version 5 rejection exist, but current bytes, version 3, typed rejection, and complete startup evidence are incomplete |
| Raft restart sentinel | `storage` | magic `ARGMINCPRAFTSEEN`; sentinel encoding 1 | Evidence required; current round trip and direct versions 0/2 rejection exist, but current bytes, typed rejection, and complete startup evidence are incomplete |
| Raft WAL file | `storage` | magic `ARGMINCPRAFTWALFILE`; file encoding 2 using the shared durable-journal frame stream | Evidence required; extensive replay/corruption behavior exists, but old/new rejection currently exercises a duplicated test-only decoder rather than the production reader |
| Raft WAL frame and records | `storage` | magic `ARGMINCPRAFTWAL`; frame/record encoding 1 | Evidence required; every record kind round-trips and malformed/version-2 rejection exists, but current bytes, version 0, typed rejection, and production replay evidence are incomplete |
| Standalone route identity and initialization marker | `storage` | static route-map digest 3; combined-route digest 3; shared artifact format 3 with distinct identity/initialization magic values | Recorded: exact current identity, marker, static digest, and combined digest fixtures; separate marker magic/version failures; exact versions 2/4; deterministic post-metadata truncation/extension rejection for both artifacts |
| User and system object metadata | `server-core` | user metadata 1; system metadata 1 | Recorded |
| Shared checksum algorithm/type tag encoding | `checksum` | untagged `u8` algorithm tags 0-9, type tags 0-1, and absent-type sentinel 255 | Design recorded below: current direct enum casts bind every containing format; an owner-local exact tag table and per-container evidence remain required |
| Encrypted checksum-metadata plaintext frame | `server-core` | checksum metadata 1; version, algorithm/type tags, big-endian `u16` UTF-8 value length, and value | Evidence required; direct versions 0/2 rejection and one encrypted current vector exist, but the exact plaintext corpus, typed rejection, and key-aware object-read evidence are incomplete |
| SSE-C checksum-sealing profile | `server-core` | AES-256-GCM with 12-byte nonce, 16-byte tag, and AAD `argmin:sse-c:checksum:v1`, selected by outer SSE-C state 3 | Evidence required; randomized round-trip and persisted logical behavior exist, but no exact cryptographic vector or malformed-inner production-read fixture exists |
| SSE-S3 checksum-sealing profile | `server-core` | AES-256-GCM with 12-byte nonce, 16-byte tag, and AAD `argmin:sse-s3:checksum:v1`, selected by outer SSE-S3 state 1 | Evidence required; one exact cryptographic vector and unit round-trip exist, but persisted logical and malformed-inner production-read evidence is incomplete |
| Object encryption state | `storage` | SSE-C 3; SSE-S3 1, selected by a typed outer discriminator | Evidence required; exact outer bytes and version rejection are pinned, but canonical empty-checksum carrier validation remains outstanding below |
| Object-tag and bucket-tag durable carrier | `s3-types` | private text frame `ARGMIN-TAGSET/1\n` plus owner-private canonical XML payload | Recorded: exact empty/Unicode/escaping frames, both cardinality profiles, typed framing/payload failures, round trips, and owner-bound API enforcement |
| ACL-grants durable carrier | `s3-types` | private text frame `ARGMIN-ACL-GRANTS/1\n` plus owner-private canonical grant payload | Recorded: exact empty and all-grantee/all-permission frames, typed framing/payload failures, round trips, and owner-bound API enforcement |
| Static cluster manifest schema | `argmin-s3` | required TOML `schema_version = 1` field | Evidence required; current examples and direct versions 0/2 rejection exist, but the parser interprets the complete v1 body before version rejection and has no typed schema-marker result |
| Static topology identity digest | `argmin-s3` | private domain `argmin-static-cluster-topology-v1` plus canonical tagged fields, SHA-256 rendered as lowercase hex | Evidence required; exact aggregate vectors and field-sensitivity tests exist, but the version dependency set is only now recorded below |
| Static process identity digest | `argmin-s3` | private domain `argmin-static-cluster-process-identity-v1` plus canonical tagged fields, SHA-256 rendered as lowercase hex | Evidence required; exact aggregate vectors and process-selection tests exist, but the version dependency set is only now recorded below |
| Static full-config fingerprint | `argmin-s3` | private domain `argmin-static-cluster-full-config-v1` plus canonical tagged fields, SHA-256 rendered as lowercase hex | Design required: exact vectors exist, but the operator-facing output does not identify the fingerprint version |
| Static storage identity and initialization marker | `argmin-s3` | magic `ARGSSID\0`; identity encoding 1; the same bytes are retained in each initialized PG | Evidence required; direct versions 0/2 rejection exists, but exact current bytes, typed rejection, and full initialization/startup no-mutation evidence are incomplete |
| Static control-plane outer identity | `argmin-s3` | magic `ARGSCPID`; identity encoding 2 with a trailing lowercase-hex SHA-256 digest | Evidence required; correctly resealed versions 1/3 are rejected directly, but exact current bytes, typed rejection, and full authority-startup no-mutation evidence are incomplete |
| Temporary-credential session token | `auth` | `ARGST1` envelope / version 1 | Recorded |
| Internal TLS protocol identifiers | `storage` | storage RPC, control-plane RPC, and Raft peer ALPN `/1` identifiers | Evidence required; all three identifiers and protocol-profile constructors are owner-private and boundary-checked |

#### Versioned-format bump regressions

Each format owner must give its bump test a conspicuous policy name, following the form
`current_<format>_matches_frozen_versioned_manifest_and_requires_version_bump`. A leaf-format test
selects immutable evidence by the writer's current version. A composite test selects immutable
outer-grammar evidence by the outer version and an append-only exact fixture by the complete
relevant version vector. Adding a field, variant, message kind, tag, domain, framing rule, or other
incompatible representation detail while leaving its owning version unchanged must fail. An
independently versioned inner change appends a vector fixture instead. Neither repair rewrites an
old entry.

Maintain every historical entry permanently. They provide old/new rejection inputs immediately
and become golden upgrade inputs for durable formats if migration support is later introduced.
Transport formats may remain current-version-only at runtime while still preserving old fixtures
that prove unsupported versions fail before authentication-sensitive dispatch or mutation.

#### PG SQLite schema-version manifest and bump regression

The `PRAGMA user_version` check alone does not prove that the schema associated with that version
is unchanged. The addition of `metadata_command_pending_slot.publication_started` while
`CURRENT_PG_SCHEMA_VERSION` remained 2 demonstrated this failure mode and now defines schema
version 3.

The SQLite-specific baseline is recorded by the owner-local
`current_pg_schema_matches_frozen_versioned_manifest_and_requires_version_bump` policy test:

1. An append-only manifest catalogue is keyed by every historical PG schema version. Version 1
   retains empty ACL defaults and unqualified replica-state hash/digest columns. Version 2 records
   the framed ACL defaults plus `applied_log_hash_encoding_version` and
   `state_digest_encoding_version`. Version 3 adds
   `metadata_command_pending_slot.publication_started`.
2. The test creates a fresh current database through the production schema initializer, reads the
   complete `sqlite_schema` catalogue through storage-owned code, canonicalizes it deterministically,
   and requires exact equality with the manifest selected by `CURRENT_PG_SCHEMA_VERSION`.
3. The catalogue retains every user table, index, view, and trigger. Complete normalized `CREATE`
   statements seal columns, defaults, constraints, foreign keys, strict/rowid properties, view and
   trigger bodies, and ordinary indexes. Selecting by owning table also retains SQLite-created
   primary/unique auto-indexes whose own SQL text is null.
4. Fresh construction separately proves that it writes version 3 to `PRAGMA user_version`.
   Nonempty version-zero stores and versions 1, 2, and 4 remain unsupported and fail
   closed. Any later schema change with the version constant left unchanged fails against the
   frozen manifest.
5. Every historical manifest remains permanent. The repair for an intentional physical change is
   to add the next version and append its manifest, never to rewrite an existing entry. These
   manifests become inputs to golden-store and upgrade tests when migrations are deliberately
   implemented.

This current-format bump regression is distinct from Phase 4. The manifest freezes complete
trigger bodies as part of each schema version; Phase 4 additionally defines how a supported old
store with an old trigger generation is recognized, migrated, and recovered after a crash.

#### Metadata-command family evidence inventory (2026-08-08)

The former combined metadata-command row contains two independently versioned, self-describing
records plus one untagged durable hash-chain representation. All are private to `storage`; neither
self-describing record has an older-version reader or a default-version fallback.

| Format | Defining marker | Current writer | First rejecting reader | Existing permanent evidence | Evidence still required |
| --- | --- | --- | --- | --- | --- |
| Applied metadata-command envelope | Private `METADATA_COMMAND_MAGIC` followed by little-endian `METADATA_COMMAND_ENCODING_VERSION = 8` in `metadata_command.rs` | `canonical_command_bytes()`, reached through `MetadataCommandEnvelope::command_bytes()` before pending-slot, applied-log, transfer, or storage-RPC publication | `decode_metadata_command_log_entry_header()` for retained/applied log inspection and `decode_metadata_command_envelope()` for pending-slot, scavenger, transfer, and storage-RPC use; persistence paths verify the separately stored CRC64 before either decoder | Recorded. Stable CRC64 fixtures cover every current payload kind and representative nested branches, including the framed tag/ACL values; every fixture round-trips the header and full-envelope decoders. Direct fixtures distinguish framing failures and reject versions 6, 7, and 9; the exact pre-v8 append checksum remains sealed as rejected v7 evidence. Owner-local persistence fixtures independently reseal v7/v9 pending slots and applied-log rows, including the applied hash chain, and prove recovery rejects them without changing the durable rows, replica state, or materialized state. Correctly authenticated current storage-RPC frames carrying independently checksummed v7/v9 envelopes are rejected before the mutation-dispatch sentinel and leave the durable command log and replica state unchanged. | None for the current v8 baseline. A future incompatible envelope change advances this inner version and appends permanent old/new fixtures; it does not add a pre-v8 reader. |
| Abandoned metadata-command record | Private `ABANDONED_METADATA_COMMAND_MAGIC` followed by little-endian `ABANDONED_METADATA_COMMAND_ENCODING_VERSION = 1` in `metadata_command.rs` | `abandoned_command_log_bytes()`, reached through `MetadataCommandEnvelope::abandoned_log_bytes()` before terminal-log persistence | `decode_metadata_command_log_entry_header()` after the separately stored CRC64 is verified by `PgStore::verify_metadata_command_log_entry()` | Recorded. A fixed v1 fixture pins the complete marker, little-endian version and identity fields, and original-command checksum. The applied and abandoned readers share a private typed format error; direct fixtures distinguish missing/truncated marker and version, unknown magic, trailing data, and unsupported versions 0 and 2. Owner-local persistence fixtures reseal versions 0 and 2 with valid row checksums, send them through production recovery, and prove rejection leaves replica state, materialized-state digest, command bytes, checksum, and unpublished recovery hashes unchanged. | None for the current v1 baseline. A future incompatible record change advances this inner version and appends permanent old/new fixtures; it does not add an older-version reader. |
| Metadata-command log hash chain | Private `ARGMIN-METADATA-COMMAND-LOG-V1` domain inside `metadata_command_log_hash()`; raw PG columns remain CRC64 integers while every exported value is the opaque carrier `MetadataCommandLogHash` version 1 | Applied publication and contiguous-tail advancement compute the hash before publishing it in the schema-v3 log/replica state; abandoned rows may first be recorded with null chain fields and are hashed when the contiguous tail advances | Log-suffix validation and abandoned-tail recovery recompute the chain before advancing durable replica state; transfer, checkpoint, runtime-map, control-plane, and RPC readers reject a noncurrent carrier before comparison or publication | Corrupt durable hash and unproven-prefix tests establish fail-closed recovery and transfer behavior. An exact v1 hash golden seals the domain, field order, little-endian integer widths, and CRC64 result. Exact carrier fixtures and wrong-version tests cover the direct containers advanced in the coordinated slice. | Recorded for the carrier introduction. A later hash-algorithm change advances only the inner version while the fixed version-plus-`u64` carrier shape remains unchanged; raw command-log columns remain bound to the PG schema. |

The applied envelope is persisted in pending-slot and metadata-command-log rows and is nested inside
storage-RPC and peering-transfer payloads, but it carries its own marker and version. Its version is
therefore authoritative for its bytes; an incompatible command-envelope change does not silently
rely on the SQLite schema or outer RPC-frame version. The abandoned record likewise remains
self-describing inside the metadata-command log. Materialized metadata checkpoints do not contain
either command-log representation. The later storage-RPC audit must still verify that its containers
do not bypass the inner applied-envelope decoder before dispatching the contained command.

#### Metadata checkpoint and canonical-state evidence inventory (2026-08-08)

The former combined checkpoint/canonical-state row contains a serialized checkpoint payload and a
canonical digest hierarchy. The coordinated baseline now gives both an authoritative private
version boundary; the table records the implemented first readers and evidence.

| Format | Defining marker | Current writer | First rejecting reader | Existing permanent evidence | Evidence still required |
| --- | --- | --- | --- | --- | --- |
| Metadata checkpoint payload | Private fixed magic `ARGMCPKT`, big-endian `u16` `METADATA_COMMAND_CHECKPOINT_ENCODING_VERSION = 2`, and integrity domain `argmin.metadata.command-checkpoint`; reserved version 1 denotes the rejected former unframed representation | `PgStore::metadata_command_checkpoint()` constructs and verifies the logical checkpoint; `record_metadata_command_checkpoint()` writes the framed payload to SQLite, and the same owner codec is nested in storage-RPC checkpoint messages | `decode_metadata_command_checkpoint_payload()` checks the complete magic and version before reading logical fields or allocating checkpoint tables/blocks. `MetadataCommandCheckpoint::verify()` recomputes a seal which includes the same marker and version. Catalogue loading skips invalid frames, while install RPC dispatch cannot begin until decoding succeeds. | A production-generated empty checkpoint verifies and seals exact current v2 integrity CRC, length, and SHA-256. Typed direct fixtures reject missing/truncated/unknown magic, unframed v1, noncurrent proof carriers, and correctly resealed framed versions 1 and 3. Catalogue fixtures persist correctly resealed unsupported frames and prove they are excluded without replica-state change. An authenticated peering install sends correctly resealed versions 1 and 3 through the real server boundary and proves `PayloadDecode` occurs without destination mutation. Existing semantic-corruption and rollback tests remain. | Recorded. The inner checkpoint version is authoritative; PG and RPC containers advanced for this initial field-shape change and advance again only if their own containing grammar changes. |
| Canonical metadata state digest | Private domain `argmin.metadata.pg-state`; opaque fixed-width `CanonicalStateDigest` contains `METADATA_CANONICAL_STATE_ENCODING_VERSION = 5` plus one `u64` value | SQLite digest triggers and owner refresh paths produce tagged row/table digests and the final typed digest; schema-v3 replica state persists its version, and checkpoints and `PgMetadataProof` carry the complete typed value | Owner constructors and every direct container reader require version 5 before comparison, command validation, transfer, route certification, or publication. Raw materialized rows, cached table digests, and command-log pre/post-state columns remain integer details bound to PG schema 3. | The digest-table inventory is explicit; incremental/cache equivalence and every materialized family remain covered. Exact v5 row/table/aggregate goldens, fixed carrier bytes/text, noncurrent carrier fixtures in checkpoint, RPC, runtime config, command, control-plane state/RPC, and route paths, and install rollback tests seal the coordinated boundary. | Recorded for version 5 and the fixed version-plus-`u64` carrier. Later semantic changes advance this inner version; containing versions remain stable when their carrier field grammar is unchanged. |

The row-value tags, table-digest tags, table inventory, column order, and blob/text length encoding are
subformats of canonical-state encoding 5 rather than independent public versions. Any incompatible
change to them advances `CanonicalStateDigest`'s inner version. The checkpoint checksum remains an
integrity seal, not a substitute for its readable frame marker.

#### Metadata checkpoint and proof-carrier design decision (2026-08-09)

Use independent storage-owned inner versions rather than binding every future change to every
outer container:

- `MetadataCommandLogHash` is an opaque fixed-width value containing private `u8` encoding version
  1 and one `u64` hash. Its version is independent of the current metadata-command envelope
  version 6 and the coordinated target version 7.
- `CanonicalStateDigest` is an opaque fixed-width value containing the canonical-state encoding
  version as a private `u8` and one `u64` digest. Existing evidence describes unqualified
  version-4 values; the coordinated carrier/tag/ACL implementation first writes the opaque value
  as version 5 because the canonical row bytes change in that same slice.
- `MetadataCommandCheckpoint` is encoded only through a private `ARGMCPKT` plus big-endian `u16`
  version-2 frame. Version 1 remains assigned to the current unframed bytes; the v2 marker/version
  participates in the integrity seal.
- `PgMetadataProof` contains the applied index plus the two opaque versioned values. It does not
  expose constructors or fields that can combine an unqualified hash or digest with a proof.

Binary container codecs write each fixed-width carrier as exactly one `u8` version followed by its
`u64` value using that container's established byte order for the value. Text containers write the
canonical unsigned-decimal `u8` version as its own field immediately before the canonical
unsigned-decimal `u64` value. Route-digest v3 hashes the `u8` version before the corresponding
`u64` value in that order. There is no public universal carrier byte codec. Exact-version equality
is required before hash/digest comparison. Empty/genesis proofs are constructed by the owner with
both current versions; zero is not a versionless escape.

Raw-to-current carrier issuance is capability-bound to the algorithm owners. The metadata-command
hash owner alone can issue a current `MetadataCommandLogHash` from a newly computed hash, while the
PG-store owner alone can issue either carrier when decoding or recomputing its schema-bound raw
state. Exact-version container decoders may reconstruct carriers only from the encoded version and
value together. Metadata transfer, peering, node-client, and storage-RPC paths carry those typed
carriers end to end; the manually entered PG-admin proof requires an explicit version beside every
value and rejects a noncurrent version before constructing the proof. The boundary checker
allowlists those owner and decoder sites and rejects another raw-to-current issuer.

Raw SQLite `metadata_command_log` hash and pre/post-state columns, cached row/table digests, and
materialized metadata rows remain integer implementation details bound to the PG schema. The
replica-state row adds both version columns and decodes its integers into the opaque types. Every
production value leaving the PG owner—including replica state, checkpoint state, retained command
ranges, transfer bases, expected pre/post-state values, and recovery evidence—uses the typed values
or carries an exact typed base proof that unambiguously governs the contained raw range. A batch
without such version context is invalid. This prevents different algorithms from being compared
merely because they both currently produce `u64`.

The carrier introduction and tag/ACL framing form one atomic baseline change. They advance this
exact direct dependency set together:

- PG schema 1 to 2, adding the replica-state version columns and binding all raw PG digest/hash
  columns to that schema while selecting the framed tag/ACL rows;
- applied metadata-command envelope 6 to 7 for the newly framed tag/ACL values;
- canonical metadata-state encoding 4 to 5 and checkpoint frame 2, with checkpoints carrying both
  versioned proof values and the framed canonical rows;
- storage-node persisted runtime configuration v4 to v5, adding both proof versions to every
  present metadata-read route;
- storage-RPC frame v16 to v17 for proof, expected-digest, retained-range, transfer, recovery, and
  checkpoint payloads;
- control-plane logical state v27 to v28, command envelope v14 to v15, and control-plane RPC v13
  to v14 wherever they directly encode a `PgMetadataProof` or either carrier;
- runtime-map content/current-state, static-route-map, and standalone-combined-route digest domains
  from v2 to v3 because their canonical proof grammar changes; and
- the standalone identity/initialization artifact from v2 to v3 so an artifact using the previous
  hidden route-identity grammar reaches typed version rejection rather than an identity mismatch.

The replicated snapshot frame remains v1 because it contains the already self-describing logical
state; Raft restart/WAL/peer containers likewise retain their versions when they contain only a
self-describing control-plane state, command, or snapshot frame. Storage/control-plane
authentication bindings retain their versions when their framed payload grammar is unchanged.
Their immutable outer-grammar fixtures remain unchanged. Each appends a composite fixture keyed by
the new complete version vector to prove the new current nested versions are present, while
retaining the fixture for the preceding vector.

The coordinated implementation replaced the former unversioned proof triples at every container
listed above. Owner fixtures now seal the versioned carrier bytes independently in the big-endian
control-plane command and RPC codecs, the little-endian storage-RPC codec, canonical control-plane
state, runtime configuration, checkpoints, and route digests. Each first reader has a focused
noncurrent-carrier fixture; there is no unversioned constructor or comparison path.

After this one-time change, advancing log-hash version 1 or canonical-state version 5 does not
advance an outer format while the version-plus-`u64` carrier shape and its position remain stable.
Route digests naturally change because they hash both inner versions and values, but their v3
grammar remains unchanged. A change in carrier width, ordering, optionality, or container field
position still advances that direct container. Changing checkpoint payload structure advances only
checkpoint frame v2 unless a containing length/framing rule also changes. There are no fallback
readers and no mixed-version comparison or translation paths.

#### Storage-node persisted runtime-config evidence inventory (2026-08-09)

The proof-carrier audit found one additional independently versioned direct container that the
initial Phase 2 matrix omitted.

| Format | Defining marker | Current writer | First rejecting reader | Existing permanent evidence | Evidence still required |
| --- | --- | --- | --- | --- | --- |
| Storage-node persisted runtime configuration | Private first line `argmin-storage-node-runtime-config-v5`, followed by canonical labeled text fields and route records; a present metadata-read route stores node, log index, log-hash version/value, and state-digest version/value as canonical decimal integers | `StorageNodeProcessConfig::persist_control_plane_runtime_config()` calls `encode_control_plane_runtime_config()`, writes and syncs a private staging file, atomically replaces the current file, and syncs the directory before an installed refresh becomes the restart baseline | The bounded regular-file loader parses the combined magic/version before node/epoch/validity/EC/path/PG/route state, then requires a complete current proof carrier before constructing `PgMetadataProof`. Control-plane-managed restart performs this read under the data-directory lock before advancing the incarnation, opening PG storage, or installing route authority. | Owner tests cover exact v5 text; both current and historical routes with present/absent proofs; malformed/unknown magic; versions 4/6; missing, partial, and noncurrent carriers; bounded count/file/type/symlink/FIFO handling; staging replacement; refresh publication; persisted restart; mismatch detection; and installation/admission ordering. | Recorded for v5. Later inner-version changes preserve runtime-config v5 while the fixed carrier field count/order remains unchanged. |

#### Storage-node RPC and authentication evidence inventory (2026-08-09)

The former combined storage-node RPC row contains three storage-specific formats. The outer
transport frame carries a shared control-plane authentication envelope whose payload is the
storage-specific binding, and that binding embeds the storage-RPC frame. The shared authentication
envelope is a fourth format and remains in the later shared authentication-envelope audit rather
than being counted twice here.

| Format | Defining marker | Current writer | First rejecting reader | Existing permanent evidence | Evidence still required |
| --- | --- | --- | --- | --- | --- |
| Storage-node RPC frame | Private length-prefixed `STORAGE_RPC_FRAME_MAGIC = argmin-storage-rpc-frame` followed by little-endian `STORAGE_RPC_FRAME_ENCODING_VERSION = 21` | `encode_storage_rpc_frame()` writes v21 and seals version, request ID, message kind, payload length, and payload with CRC64 before Unix or authenticated binding publication | `decode_storage_rpc_frame()` returns typed `Truncated`, `UnknownMagic`, `UnsupportedVersion`, checksum, and structural errors before payload decoding. The unauthenticated streaming reader validates the exact marker and version immediately after reading them, before request ID, message-kind interpretation, payload-length admission, allocation, or dispatch. Authenticated requests reach the same decoder inside `decode_binding()` only after the signed envelope is authenticated. | Recorded. The conspicuous current-format policy test seals exact v17, v18, v19, v20, and v21 frame bytes, including magic length, magic, little-endian version, field order, checksum, and payload. Independent hardcoded payloads preserve the v18 `StreamUploadNoSuchUpload` tag-7 advancement, the v20 exact `MetadataCommandLogGap` tag-8 advancement, and the v21 stream-segment append request without a caller-selected shard hash. Hardcoded v19 pending-slot-remove and proof-release payloads pin their absolute operation deadlines, with the pending-slot fixture also pinning absent insert-only fields. Direct fixtures distinguish truncated marker/version, unknown magic, unsupported versions 16 through 20 and 22, trailing bytes, unknown/retired kinds, kind tampering, checksum precedence, and payload bounds. Collision fixtures prove the stream reader reports unknown magic or unsupported version before invalid kind and oversized-length errors. Coordinated payload fixtures pin proof carriers, framed tags/ACLs, and checkpoints. Server fixtures carry a valid metadata mutation in resealed v20/v22 frames through both unauthenticated streaming and a valid current authentication envelope, proving rejection before the mutation sentinel, replica-state change, or durable command-log publication. | None for the current v21 baseline. The v19 advancement binds absolute operation deadlines, v20 preserves the exact trailing replica log gap, and v21 removes caller-selected stream segment shard identity. A future incompatible frame or message grammar change advances the frame version and appends permanent exact old/new fixtures; it does not add an older-version reader. |
| Storage-node RPC authentication binding | Private fixed `STORAGE_RPC_AUTH_BINDING_MAGIC = ARGSRPCB` followed by big-endian `STORAGE_RPC_AUTH_BINDING_VERSION = 2`; it contains topology authority, target node, optional request transcript, and a complete v21 storage-RPC frame | `encode_binding()` is used by both request and response signing before the bytes become the payload of a shared `ControlPlaneAuthEnvelope` | `decode_binding()` runs only after the shared envelope has been decoded and its authenticator accepted. Its private typed error distinguishes truncated magic/version, unknown magic, unsupported version, and malformed remainder before topology, transcript, or inner-frame parsing. The authenticated boundary deliberately maps all binding-format details to `StorageRpcAuthRejectionReason::Malformed`; failed binding verification prevents `VerifiedStorageRpcFrame` construction and server dispatch. | Recorded. Exact v2 request and response bindings seal topology generation/digest, target node, absent/present transcript tags, transcript bytes, inner-frame length, and a complete fixed v21 frame. A separate fixed digest over fixed envelope bytes invokes the production transcript derivation and seals the `argmin/storage-rpc/request-transcript/v1\0` domain, big-endian `u64` envelope length, and exact envelope contribution. Direct fixtures distinguish missing/truncated marker and version, unknown magic, and versions 0/3. Authenticator-valid versions 0/3 pass shared-envelope verification but fail binding decoding before mutation dispatch, replica-state change, or command-log publication. Existing request/response tests retain exact topology, target, operation, sequence, direction, caller credential, and transcript-binding behavior. | None for the current v2 baseline. A future incompatible binding or transcript algorithm change advances binding version 2 and appends permanent exact old/new fixtures; it does not add an older-version reader. The nested storage-RPC frame and shared authentication envelope retain their independent version boundaries. |
| Storage-node RPC authenticated transport frame | Private fixed `STORAGE_RPC_AUTH_TRANSPORT_MAGIC = ARGSRPCA` followed by big-endian `STORAGE_RPC_AUTH_TRANSPORT_VERSION = 1`, envelope length, its bitwise complement, and the shared authenticated envelope | `write_storage_rpc_auth_transport_frame()` writes v1 before Unix or TLS stream publication | The private typed header reader distinguishes truncated magic/version/length fields, unknown magic, unsupported version, invalid length complement, oversized frames, and retained I/O failures. It validates magic and version before length validation, byte-budget reservation, body allocation, authentication, or binding/frame decoding. Existing stream-facing APIs map that owner-local result to their bounded `io::Error` contract. | Recorded. An exact v1 golden seals magic, big-endian version, envelope length, bitwise-complement length, and payload bytes. Direct fixtures distinguish missing/truncated marker and version, unknown magic, versions 0/2, legacy storage-frame magic, corrupt length complement, configured/process byte budgets, allocation ordering, body lifetime, and flush failure. Server fixtures place a current authenticator-valid envelope and current binding/frame inside versions 0/2, prove the transport error wins before envelope authentication, and verify mutation dispatch, replica state, and the durable command log remain unchanged. | None for the current v1 baseline. A future incompatible transport grammar change advances version 1 and appends permanent exact old/new fixtures; it does not add an older-version reader. The shared authentication envelope, storage authentication binding, and storage-RPC frame retain their independent versions. |

The request transcript uses the private byte domain
`argmin/storage-rpc/request-transcript/v1\0`, including that trailing NUL byte, followed by the
authenticated request-envelope length encoded as a big-endian `u64` and then the exact authenticated
request-envelope bytes. Only the resulting untagged SHA-256 output is carried inside binding v2.
Treat that transcript algorithm as a subformat of the binding: any incompatible transcript change
must advance the binding version. Its evidence must include either a fixed transcript-digest golden
over fixed envelope bytes or a fixed request-to-response golden that invokes the production
transcript derivation; a binding fixture containing an arbitrary 32-byte transcript is insufficient.
The topology digest and shared authentication envelope retain their own version boundaries and are
not implicitly versioned by the binding.

#### Control-plane logical-state, command, and snapshot evidence inventory (2026-08-09)

The former combined control-plane logical-state row contains three independently versioned
formats. The command envelope is nested in single-authority journals, OpenRaft entries, restart
artifacts, WAL records, and peer RPC payloads. The replicated snapshot envelope contains the
canonical logical-state text. Both inner formats carry their own current marker, so their readers
remain authoritative wherever an outer durable or wire format embeds them.

| Format | Defining marker | Current writer | First rejecting reader | Existing permanent evidence | Evidence still required |
| --- | --- | --- | --- | --- | --- |
| Canonical control-plane logical state | Private `CURRENT_CONTROL_PLANE_STATE_VERSION = 28`, emitted as the first canonical text line `version=28` in `control_plane.rs` | `format_snapshot()` produces the only canonical state text used by the single-authority file store, state digests, and the replicated snapshot envelope | `parse_snapshot_without_publication_validation()` classifies a missing marker and a present non-28 marker through the private `ControlPlaneStateVersionError` before it can return owner-local state; `parse_snapshot()` additionally validates serving invariants. The containing `ControlPlaneError::Parse` retains distinct sanitized missing and unsupported-version messages. | Canonical format/parse round trips cover representative and complex state. A 3,596-byte length-delimited aggregate seals every record family plus present/absent topology, lease horizon, metadata proof, transfer, prior-primary lease, all route-reference kinds, and pending-command state. A simple exact text fixture keeps failures readable. Versions 27/29 are rejected from otherwise valid canonical state through single-authority authority open without replacing its file and through replicated snapshot installation without replacing live state. Noncanonical but logically equivalent text is rejected. | Recorded. Any incompatible field, ordering, enum spelling, optional-value grammar, proof-carrier use, or semantic interpretation change advances state version 28 and appends a new immutable aggregate; an independently incompatible snapshot envelope change advances snapshot version 1 instead. No reader accepts versions 27 or 29. |
| Control-plane command envelope | Private fixed `CONTROL_PLANE_COMMAND_MAGIC = ARGCPCMD` followed by big-endian `CONTROL_PLANE_COMMAND_VERSION = 15`, command tag and payload, then a big-endian CRC64 | `encode_control_plane_command()` is used before single-authority journal publication, static-topology certification, and embedding normal commands in OpenRaft durable and peer-RPC entries | `decode_control_plane_command()` verifies the CRC64, then classifies truncated input, unknown magic, and non-15 versions through private `ControlPlaneCommandFormatError` before command-tag or payload decoding. The containing `ControlPlaneError::CommandDecode` retains a sanitized owner diagnostic. Every journal, restart, WAL, and peer-RPC decoder that encounters a normal entry delegates to this inner decoder before returning the command for apply. | Round-trip tests cover all command variants and degenerate values. A sealed v15 aggregate pins all command variants and versioned proof fields. Direct fixtures distinguish missing/truncated marker or version, correctly CRC-resealed bad magic, and correctly resealed versions 14/16. Versions 14/16 are also nested in current single-authority journal records with valid outer framing, binding, and chain material; authority loading rejects them without replay or changing the checkpoint/journal. Current peer-RPC append-entries frames carrying the same nested versions have valid outer CRCs and are rejected both directly and by the production handler before OpenRaft dispatch, with the complete authority status unchanged. Existing checksum, unknown-tag, allocation-bound, and semantic fixtures remain. | Recorded. Any incompatible command tag, field layout, ordering, enum code, collection grammar, nested proof use, checksum coverage, or command semantic interpretation advances command version 15 and appends its immutable aggregate and container-vector fixtures. The single-authority record, peer RPC, restart, and WAL versions advance only when their own containing grammar changes; they continue delegating independently versioned command bytes to this reader. No reader accepts versions 14 or 16. |
| Control-plane replicated snapshot envelope | Private fixed `CONTROL_PLANE_SNAPSHOT_MAGIC = ARGCPSNP` followed by big-endian `CONTROL_PLANE_SNAPSHOT_VERSION = 1`, a big-endian length-prefixed canonical logical-state payload, and a big-endian CRC64 | `encode_control_plane_snapshot()` is called by `ReplicatedControlPlaneStateMachine::build_snapshot_artifact()` before OpenRaft snapshot, restart-artifact, or peer-RPC publication | `decode_control_plane_snapshot_contents()` verifies the CRC64, then classifies truncated input, unknown magic, and non-1 versions through private `ControlPlaneSnapshotFormatError` before length or UTF-8 processing. `decode_control_plane_snapshot_for_install()` then parses state version 28, enforces canonical state text and validates state invariants before returning a replacement. Both `install_snapshot_artifact()` and peer snapshot request admission use this same complete decoder, so no nested state failure reaches OpenRaft dispatch. The containing errors retain sanitized owner diagnostics. | An exact v1 golden seals the complete framing around canonical empty state v28 and decodes from the fixed bytes. Direct fixtures distinguish missing/truncated marker or version, correctly resealed bad magic, and correctly resealed versions 0/2. Both unsupported versions are rejected before replicated state-machine replacement. Current identity-bound peer snapshot frames carrying the same nested versions have valid outer checksums and are rejected by direct admission and the full server before OpenRaft dispatch. A current-v1 peer frame carrying invariant-invalid state is rejected at the same boundary. In every case authority status is unchanged and neither checkpoint nor response publication begins. Existing round-trip/replay, checksum, length, UTF-8, inner-state-version, canonicality, invariant, rollback, and stale-snapshot fixtures remain. | Recorded. Any incompatible snapshot marker, field width/order, length grammar, checksum coverage, payload interpretation, or envelope semantic change advances snapshot version 1 and appends immutable exact old/new and container-vector fixtures. Logical-state text remains independently governed by state version 28. No reader accepts snapshot versions 0 or 2. |
| Single-authority journal hash chain | No independent marker. `single_authority_snapshot_digest()` seeds the chain as CRC64 over the exact canonical state text. `single_authority_command_chain_digest()` advances it as CRC64 over the previous digest encoded as a big-endian `u64` followed by the exact encoded command envelope. | Initial publication, checkpoint anchoring/rebasing, and command append write the previous and resulting digests into `SingleAuthorityJournalRecord` version 2 | Journal replay first finds an anchor whose resulting digest equals the canonical snapshot digest, then requires each record's previous digest to equal the current chain and recomputes the resulting digest before applying its decoded command. Checkpoint capture and compaction repeat the same continuity checks before publication. | A frozen vector pins CRC64 seed `0xf8f178f8a5dbb7ba` over exact canonical empty state v28. Fixed `SetNodeMembership` and `MarkNodeAvailability` command-v15 bytes pin the first transition `0x10960ae613f1ca37` and chained second transition `0xbc5537771bb41d01`. An exact command journal-record-v2 fixture seals the binding, big-endian previous/resulting digests, kind, length, complete first command, and record checksum, and decodes back to the same logical record. Existing durable tests retain discontinuity, incorrect-result, checkpoint-anchor, rebasing, compaction, and restart-replay behavior. | Recorded. Both untagged algorithms are bound to `SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION = 2`: any incompatible change to the snapshot seed input/algorithm, previous-digest byte order, command-envelope contribution, or CRC algorithm advances the journal-record version and appends immutable seed, transition, and containing-record fixtures. Later journal-record evidence must still reseal unsupported outer versions around valid chain material and prove rejection before replay or publication. |

The canonical state version is authoritative for the text inside snapshot envelope version 1; an
incompatible logical-state change advances state version 28 even if the snapshot framing is
unchanged. Conversely, a framing/checksum/length change advances snapshot version 1 without
silently changing the inner state version. Command version 15 remains authoritative inside every
single-authority and OpenRaft container. Later audits of those outer formats must verify that they
always delegate to these inner readers before applying or publishing the contained state, but they
do not need to duplicate the inner byte encoding.

The single-authority chain is deliberately an outer-bound subformat of journal-record version 2,
not a fourth independently framed record. State version 28 and command version 15 still govern
their own bytes, while journal-record version 2 governs how those bytes seed and advance the chain.
Changing a nested state or command encoding advances its own version; changing the chain algorithm
or the way those encoded bytes contribute to it advances the journal-record version as well.

#### Control-plane RPC and shared-authentication evidence inventory (2026-08-09)

The former combined control-plane RPC/authentication row contains an outer control-plane RPC frame,
an untagged control-plane-specific authenticated payload binding, and a shared authentication
envelope. The shared envelope is also used by storage-node RPC authentication and Raft peer RPC;
it has one owner and one version audit here rather than being counted again in each consuming
protocol.

| Format | Defining marker | Current writer | First rejecting reader | Existing permanent evidence | Evidence still required |
| --- | --- | --- | --- | --- | --- |
| Control-plane RPC frame and operation payloads | Private `CONTROL_PLANE_RPC_MAGIC = argmin-control-plane-rpc` followed by a big-endian `CONTROL_PLANE_RPC_VERSION = 14`, big-endian kind and payload length, and a big-endian stored CRC64 computed over the magic plus little-endian version/kind/length and the payload | `encode_control_plane_rpc_frame()` is the common Unix/TLS request and response writer. The same v14 boundary governs every `ControlPlaneRpcKind` request payload, success response, semantic error response, and nested runtime-map/diagnostic representation. | `read_control_plane_rpc_frame_with_reservation()` reads and validates the exact magic and version before interpreting the kind or payload length and before byte-budget reservation, payload allocation, authentication, or logical dispatch. It classifies truncated markers, unknown magic, and unsupported versions through private `ControlPlaneRpcFrameFormatError`, then bounds and reserves the frame, reads the payload, and verifies the CRC64 before returning a request. | Recorded. The coordinated exact v14 frame fixture pins proof-carrier order. Direct correctly checksummed v13/v15 fixtures prove rejection before byte reservation, while production Unix-server fixtures prove those versions do not reach authority admission or mutate state. Marker fixtures distinguish truncation, unknown magic, and unsupported versions with production-reader precedence. Read-only retry retains a bounded truncated-marker category, while every plain mutation uses the request-publication bit: a checked mutation matrix applies the command, injects invalid magic, a correctly checksummed old version, bad checksum, wrong outer kind, or invalid success payload, and proves state-based confirmation without resubmission. A fixed aggregate covers every accepted kind, every request and success codec, and every private typed semantic response-status tag; expected local errors and decoded remote errors use deliberately distinct projections before exact variant/field comparison. The aggregate also covers every runtime-map freshness proof, both arms of every accepted node/route option, empty and present node/current-route/historical-route/history collections, and exact rejected fixtures for unbounded validity and each incomplete transfer-source option. It further covers every OpenRaft error-kind tag, both leadership-term option arms, all diagnostic metric-kind tags, every heartbeat PG-state and history-reference-kind tag, every recovery-failure and authority-clock blocked-reason tag, and both arms of every diagnostic history-reference and node-lease option. Each nested fixture registry is compared with the complete tag set accepted by its production decoder, and production readers validate every aggregate entry. The separate bound, budget, write-failure, operation, and authentication-policy tests remain. | None for the current v14 baseline. Any incompatible outer frame, kind table, logical operation payload, success response, semantic error, or nested diagnostic grammar change advances v14 and updates the complete aggregate; it does not add a v13 reader. |
| Authenticated control-plane RPC payload binding | No independent marker. `write_authenticated_control_plane_rpc_payload()` prefixes the logical v14 request or response payload with the RPC kind encoded as a big-endian `u16`; the outer v14 frame repeats the same kind and the signed envelope binds the inner bytes. | Every authenticated control-plane request and response constructs this prefix before calling `ControlPlaneScopedCredential::sign_envelope()` | After envelope decoding and authenticator verification, `read_authenticated_control_plane_rpc_payload()` decodes the inner kind and rejects any mismatch with the already validated outer frame kind before returning the logical payload for dispatch or response consumption. | Authenticated request/response tests cover correct kind binding, malformed envelopes, wrong roles/operations, response authentication, replay policy, and successful operation dispatch. No fixed signed bytes pin the duplicated kind prefix and logical payload composition. | Bind this untagged subformat to `CONTROL_PLANE_RPC_VERSION = 14`: changing the prefix width/order, duplicate-kind rule, or logical request/response payload encoding advances v14. Add fixed signed request and response fixtures whose authenticators are derived from the production envelope over exact v14 inner bytes, plus a validly re-signed mismatched-inner-kind fixture proving rejection before dispatch or response acceptance. |
| Shared control-plane authentication envelope | Private fixed `CONTROL_PLANE_AUTH_MAGIC = ARGCPAUT` followed by big-endian `CONTROL_PLANE_AUTH_VERSION = 1`; the covered bytes contain length-prefixed cluster/credential identity, credential version, source principal, target, operation, optional issue/expiry/sequence fields, nonce, and payload, followed by a length-prefixed HMAC-SHA256 authenticator | `ControlPlaneScopedCredential::sign_envelope()` builds the canonical covered bytes and authenticates them with HMAC-SHA256; `ControlPlaneAuthEnvelope::encode_frame()` appends the authenticator. Control-plane RPC, Raft peer RPC, and storage-node RPC all use this owner codec. | `ControlPlaneAuthEnvelope::decode_frame()` checks magic and version before decoding any identity, role, operation, replay, payload, or authenticator field. Consumers then authenticate the exact covered bytes with HMAC-SHA256 before constructing a verified operation capability. Decode failures currently use free-form `ControlPlaneError::RpcProtocol`; `ControlPlaneAuthRejectionReason::UnsupportedVersion` exists but is not produced, and consuming servers classify a present but undecodable envelope as `Malformed`. | Owner tests round-trip all principal roles, exercise principal/service/operation tags, optional fields, bounds, truncation, bad magic/version, unknown and retired tags, trailing bytes, authenticator mismatch, credential versioning, target/role/cluster binding, and replay policies. Consumer tests cover authenticated dispatch and rejection across the three protocols. The bad-version owner fixture mutates the signed version field without recomputing the authenticator, and no exact v1 byte fixture seals the complete tag table. | Introduce a private typed truncated/unknown-magic/unsupported-auth-version result and map unsupported versions deliberately rather than leaving the public rejection variant unreachable. Add a sealed aggregate v1 fixture covering every source-principal tag, both target principal/service forms and every service tag, every operation tag, the storage-RPC message-kind branches, and both arms of every optional field, with exact covered bytes and HMAC. Add validly re-signed versions 0 and 2 and consumer-level no-dispatch fixtures for control-plane RPC, Raft peer RPC, and storage-node RPC, proving envelope-version rejection occurs before credential lookup, replay evaluation, operation decoding, or mutation. |

RPC version 14 is authoritative for the outer control-plane frame, its kind table, all logical
request/response payload codecs, and the duplicated kind binding inside authenticated payloads.
Authentication-envelope version 1 is authoritative for the complete canonical covered-byte
definition, shared identity, target, operation, replay-field and payload-carrier layout, and the
HMAC-SHA256 authenticator algorithm and encoding. A signing-domain or algorithm change advances
version 1 even when the replacement authenticator has the same length. Changing v13 payload
semantics does not silently change the shared envelope; changing any v1-covered definition or
authenticator rule advances version 1 and requires all three consuming protocols to reject the
old/new envelope before dispatch.

#### Single-authority durable-artifact evidence inventory (2026-08-09)

The former combined single-authority durable-artifact row contains four self-describing artifacts,
the journal's length framing, and one untagged 32-byte binding shared across the identity,
initialization marker, authority-clock checkpoint, and journal records. The journal hash chain was
recorded separately above because its algorithm is a subformat of journal-record version 2.

| Format | Defining marker | Current writer | First rejecting reader | Existing permanent evidence | Evidence still required |
| --- | --- | --- | --- | --- | --- |
| Shared durable-journal header and frame stream | No independent marker. Private `DurableJournalFile` writes a caller-supplied magic/version, big-endian base offset and CRC64, then frames each owner record with a big-endian `u32` length and its bitwise complement. Both the single-authority journal and Raft WAL instantiate this codec with their own file magic/version 2. Raft restart artifact v4 additionally persists the WAL's logical replay offset. | `DurableJournalFile::ensure_file_header()`, append, replacement, and compaction are the common writers for both file families; Raft restart export copies the current logical WAL replay offset into its v4 artifact | `DurableJournalFile::decode_file_header()` validates the configured magic/version and base-offset header before shared frame parsing validates length/complement and bounds. Each consumer then invokes its own record decoder. Raft restore passes the v4 artifact's `wal_replay_offset` directly back into the shared WAL replay path. | Both consumers exercise the shared append, replay, torn-tail, truncation, offset, and compaction paths. Raft WAL tests additionally inject corrupt first/middle frame-length pairs; single-authority tests distinguish recoverable incomplete initial creation from fail-closed established corruption. The shared byte layout and its restart-offset dependency are not independently sealed. | Bind any incompatible header-field, checksum, length-prefix, complement, torn-tail, or framing semantic change to both `SINGLE_AUTHORITY_JOURNAL_FILE_VERSION = 2` and `CONTROL_PLANE_RAFT_WAL_FILE_VERSION = 2`. A change to logical base/replay/compaction-offset meaning must also advance `CONTROL_PLANE_RAFT_RESTART_VERSION = 4`; no implicit translation is permitted. Add a fixed shared-layout fixture instantiated with both owners' magic values, exact full-file goldens for both consumers, and a fixed restart/WAL pair proving the v4 replay offset selects the intended frame after compaction. The later Raft restart/WAL audit must reference the same decision rather than silently defining a divergent copy. |
| Authority-clock durable-state binding | `CONTROL_PLANE_CLOCK_CHECKPOINT_BINDING_LEN = 32`; single-authority storage generates opaque random bytes, while Raft derives SHA-256 over `argmin-control-plane-clock-checkpoint-raft-v1\0`, cluster-name length as big-endian `u64`, cluster-name bytes, and node ID as big-endian `u64`. The 32-byte output carries no marker. | Single-authority identity creation originates one value; the initialization marker, every journal record, and any later clock checkpoint carry it unchanged. Raft recomputes its derived value for clock checkpoint v2. | Single-authority state open requires the durable identity, established initialization marker, and every journal record to agree before replay or publication. The separate clock-checkpoint reader compares any stored checkpoint with the expected binding, but process startup currently converts that binding failure to an absent checkpoint rather than blocking authority replay. Raft clock-checkpoint decode likewise compares the stored value before accepting clock continuity. A mismatch is reported as an identity error, not a version error. | Tests reject wrong single-authority identities and wrong Raft cluster/node bindings; journal replay checks every record binding; the capability is opaque and redacted outside its owner. Current tests derive expected values with production code and do not pin the Raft domain or cross-artifact bytes. | Bind the 32-byte width and cross-artifact byte semantics to all containing versions: identity v1, initialization marker v1, clock checkpoint v2, and journal record v2 must all advance if the common representation changes. Bind changes to the Raft SHA-256 domain, field order, length encoding, or node encoding to clock checkpoint v2. Add an exact Raft derivation golden and one fixed single-authority binding fixture proving byte identity across all four containers. |
| Authority-clock restart checkpoint | Private fixed `CONTROL_PLANE_CLOCK_CHECKPOINT_MAGIC = ARGCPCLK` followed by big-endian `CONTROL_PLANE_CLOCK_CHECKPOINT_VERSION = 2`, the 32-byte binding, generation, tagged optional timestamp high-water, wall/health times, and a trailing big-endian CRC64 | `ControlPlaneAuthorityClockRestartCheckpoint::encode()` is persisted atomically by the single-authority and Raft clock-checkpoint stores before clock continuity can be established | The fixed-length sidecar reader rejects size changes before allocation; `ControlPlaneAuthorityClockRestartCheckpoint::decode()` verifies CRC64, magic, version, expected binding, generation, option canonicality, and trailing structure before a restart clock consumes it. Failures use free-form `ControlPlaneError::AuthorityClockCheckpoint`. | File round trips cover a present timestamp; correctly resealed versions 1 and 3 are rejected by the owner loader. Tests cover checksum corruption, zero generation, wrong binding, oversized sparse files, clock divergence, health regression, timestamp high-water mismatch, and refusal to replace a blocked checkpoint. At process startup, every `AuthorityClockCheckpoint` failure—including unsupported format and wrong binding—is logged and converted to `None`, after which authority open and checkpoint/bootstrap may proceed. If restored state has a committed timestamp high-water, the missing continuity proof leaves the clock unestablished, the invalid sidecar is removed, and ordinary serving remains disabled until authenticated recovery. Without a committed timestamp high-water, the process clock establishes immediately; startup does not remove the failed sidecar or gate serving on recovery. The v1/v3 fixture is therefore reader-level evidence, not startup rejection evidence. | Add a private typed truncated/unknown-magic/unsupported-checkpoint-version distinction. Add exact v2 goldens for absent and present timestamp branches, plus missing/truncated marker/version fixtures. Change process startup so unknown magic and unsupported versions fail before authority open, checkpoint/bootstrap mutation, sidecar removal, or publication. For integrity, binding, or clock-continuity failures, retain and separately test both current branches: continuity-required state must remain non-serving and invalidate the sidecar pending authenticated recovery, while state without a committed timestamp high-water may establish immediately and currently retains the failed sidecar. Exercise resealed versions 1 and 3 through complete single-authority and Raft startup and prove the hard version boundary; document any intentionally different Raft recovery policy explicitly. |
| Single-authority durable identity | Private fixed `CONTROL_PLANE_STATE_IDENTITY_MAGIC = ARGCPID\0` followed by big-endian `CONTROL_PLANE_STATE_IDENTITY_VERSION = 1`, the 32-byte binding, and a trailing big-endian CRC64 | `store_single_authority_clock_checkpoint_binding()` atomically publishes the identity before any state path can be initialized | `load_single_authority_clock_checkpoint_binding()` requires the exact fixed length, verifies CRC64, magic, and version, then returns the opaque binding. Existing state without an identity fails closed before initialization. Format failures use free-form `ControlPlaneError::AuthorityClockCheckpoint`. | Correctly resealed versions 0 and 2 are rejected by the owner reader. Tests cover missing identity beside existing state, identity-only interrupted initialization recovery, and durable sidecar write/sync failures. | Add a private typed truncated/unknown-magic/unsupported-identity-version distinction and exact v1 golden. Add checksum, missing/truncated marker/version, and separately resealed malformed-magic fixtures. Exercise v0/v2 through full authority open and prove the files are neither replaced nor used to create state, marker, journal, checkpoint, or serving authority. |
| Single-authority initialization marker | Private fixed `SINGLE_AUTHORITY_INITIALIZED_MAGIC = ARGCPINI` followed by big-endian `SINGLE_AUTHORITY_INITIALIZED_VERSION = 1`, the same 32-byte binding, and a trailing big-endian CRC64 | `store_single_authority_initialized_binding()` atomically publishes the marker only after the initial snapshot and journal checkpoint anchor are durable | `read_fixed_control_plane_sidecar()` rejects a non-exact marker length as free-form `ControlPlaneError::AuthorityClockCheckpoint` before marker parsing. `load_single_authority_initialized_binding()` then verifies CRC64, magic, and version with free-form `ControlPlaneError::CommandDecode`, and startup later requires equality with the durable identity before initialized-state replay. The current format-error classification is therefore mixed. | Correctly resealed versions 0 and 2 are rejected by the owner reader. Startup tests cover identity-only and several interrupted initial snapshot/journal publication points, missing established checkpoint/journal state, and durable publication ordering. | Replace the mixed classification with one private typed truncated/unknown-magic/unsupported-initialization-version result and exact v1 golden. Add checksum, missing/truncated marker/version, separately resealed malformed-magic, and binding-mismatch fixtures. Exercise v0/v2 through full authority open and prove rejection before journal replay, state publication, repair/replacement, or serving. |
| Single-authority journal file | Private `SINGLE_AUTHORITY_JOURNAL_FILE_MAGIC = ARGCPSJL` and `SINGLE_AUTHORITY_JOURNAL_FILE_VERSION = 2`, supplied to the shared durable-journal header/frame codec | The shared writer emits this owner's v2 header before the first `SingleAuthorityJournalRecord` and rewrites a current v2 file during compaction | Shared header/frame validation runs first with the single-authority magic/version; only then can record decoding and startup recovery proceed. A physically empty newly created file is valid only during incomplete first initialization, while an empty established journal fails closed. Failures are free-form `ControlPlaneError::CommandDecode`. | Correctly resealed file-header versions 1 and 3 are rejected. Tests cover append ambiguity, recoverable torn initial creation, fail-closed torn established first records, torn-tail truncation after replay, missing/empty established journals, offsets, compaction, sync ordering, and restart replay. | Add a private typed truncated/unknown-magic/unsupported-journal-file-version distinction. Add an exact owner-specific v2 header golden and an aggregate current file golden containing checkpoint and command records with exact shared length prefixes. Add missing/truncated marker/version, resealed malformed-magic, and zero/oversized-length fixtures not already covered by the shared framing evidence, then exercise versions 1 and 3 through authority open before record parsing, truncation, compaction, replay, mutation, or publication. |
| Single-authority journal record | Private fixed `SINGLE_AUTHORITY_JOURNAL_RECORD_MAGIC = ARGCPSJR` followed by big-endian `SINGLE_AUTHORITY_JOURNAL_RECORD_VERSION = 2`, the 32-byte binding, previous/resulting chain digests, record-kind tag, command length, optional complete v15 command envelope, and trailing big-endian CRC64 | Initialization/checkpoint publication writes kind 1 anchors; command commit writes kind 2 records after computing the bound journal-chain transition. Compaction decodes and re-encodes retained commands into current v2 records. | `SingleAuthorityJournalRecord::decode()` verifies CRC64, magic, and version before binding, chain, kind, length, or inner-command decoding. Full replay then validates binding, checkpoint anchor, chain continuity/digests, and command applicability before publishing recovered state. Failures use free-form `ControlPlaneError::CommandDecode`. | Correctly resealed record versions 1 and 3 are rejected by the owner decoder. Tests cover foreign-identity journals, chain discontinuity and complete interior omission, checkpoint anchors outside the retained chain, compaction/rebasing, ambiguous appends, and restart recovery. Existing fixtures are produced with current encoders rather than fixed bytes. | Add a private typed truncated/unknown-magic/unsupported-journal-record-version distinction. Add exact v2 checkpoint-anchor and command-record goldens, jointly pinning the binding, hash-chain fixture, kind tags, command length, inner v15 bytes, and record CRC64. Add checksum, missing/truncated marker/version, resealed malformed-magic, invalid kind/length combinations, and invalid inner-command fixtures; exercise valid-chain versions 1 and 3 through startup and prove rejection before inner-command decode, replay, state mutation, compaction, truncation, or publication. |

The shared durable-journal codec owns header/base-offset and frame-length semantics, but has no
independent serialized marker. Any incompatible change to it advances both single-authority
journal file version 2 and Raft WAL file version 2; changing the logical offset interpreted by the
Raft replay path also advances Raft restart artifact version 4. Journal-record version 2 separately
owns the single-authority record fields, record CRC64, bound hash-chain algorithm, and nesting of
complete command envelope v15. A change to record fields, record-kind semantics, record integrity,
or chain computation advances journal-record version 2. A nested command change still advances
command version 15 independently.

#### Raft peer RPC evidence inventory (2026-08-09)

The former Raft peer RPC row combines an untagged transport record, a self-describing peer frame,
and untagged OpenRaft logical-value codecs shared with durable Raft artifacts. In production, the
transport record contains the shared authentication envelope v1, whose payload is the peer frame
v2. The peer frame does not contain the authentication envelope. The shared envelope remains in
its own inventory above and is not counted as a peer-specific format here.

| Format | Defining marker | Current writer | First rejecting reader | Existing permanent evidence | Evidence still required |
| --- | --- | --- | --- | --- | --- |
| Raft peer transport record | No in-record marker. `write_control_plane_raft_peer_transport_frame()` writes the contained-frame length as a big-endian `u32`, followed by the exact frame bytes. TLS peers additionally negotiate private ALPN `argmin-raft/1`; Unix peers have no separate transport negotiation. In authenticated production operation, the contained frame is authentication envelope v1, which in turn carries peer frame v2. | The Unix and TLS peer clients frame every signed request and read every signed response through the common transport writer; the storage-owned peer server uses the same writer after authentication, dispatch, checkpoint/publication admission, and response signing. | `read_control_plane_raft_peer_transport_frame_with_reservation()` reads the four-byte length, rejects a value above the configured allocation limit before reservation or payload read, reserves the admitted size, and then reads exactly that many bytes. Only afterward can authentication-envelope and peer-frame decoding begin. Transport failures are currently free-form `ControlPlaneError` values rather than a private typed format rejection. | An owner round trip covers the prefix and payload together. A bounded-reader fixture proves an oversized length is rejected before payload read. Unix and TLS exchanges cover complete framed requests and responses, absolute deadlines, allocation admission, response loss, and required TLS ALPN negotiation. | Bind the untagged length-prefix semantics to peer RPC v2 and the Raft TLS ALPN `/1` profile: an incompatible width, byte-order, length meaning, or multi-record semantic change advances both baselines rather than adding a fallback reader. Add an exact transport-record golden, zero-length and truncated-prefix/payload fixtures, and no-dispatch server evidence for each rejection. The ALPN audit must separately pin old/new protocol identifiers; Unix remains an identical-binary protocol until explicit negotiation is designed. |
| Raft peer RPC frame and payloads | Private `CONTROL_PLANE_RAFT_PEER_RPC_MAGIC = ARGMINCPRAFTPEER`, followed by big-endian `CONTROL_PLANE_RAFT_PEER_RPC_VERSION = 2`, frame-kind tag, peer identity, request/response payload, and trailing big-endian CRC64 | Request, response, snapshot-request, and snapshot-response encoders write v2 before the transport signs the complete frame as the payload of authentication envelope v1. The identity subformat uses a presence tag, length-prefixed cluster name, tagged optional topology generation/digest, and source/target node IDs. | `raft_peer_rpc_frame_reader()` first bounds the outer transport elsewhere, then verifies the frame CRC64 before magic or version. It rejects non-v2 frames before reading kind, identity, operation, or payload. Production first authenticates the outer v1 envelope, then validates the signed inner identity/operation binding and invokes the same peer-frame reader before OpenRaft dispatch. Failures use free-form `ControlPlaneError::CommandDecode`. | Round trips cover all four frame kinds, all four request tags, the three response families, both topology-identity states, snapshots, cross-direction rejection, identity mismatches, size limits, and authenticated request/response binding. Direct malformed fixtures cover truncation, resealed bad magic, resealed version 3, checksum mismatch, trailing bytes, and an unknown request tag. Server tests cover authenticated dispatch, peer admission, checkpoint-before-ack, publication, and poison-before-dispatch, but do not carry unsupported peer versions through the signed production reader. The tests are owner-local in `control_plane_raft/tests/peer.rs`. | Introduce a private typed truncated/unknown-magic/unsupported-peer-version result. Add exact v2 aggregate fixtures that cover every frame kind; all request/response tags; all four append response outcomes; all three transfer-leader outcomes; identity and topology option arms; representative mandatory vote states; both values of ordinary boolean fields; both optional-log-ID arms; and blank, membership, and normal entry kinds. The normal entry may reference the separately sealed command-v15 fixture. Add correctly CRC-resealed versions 1 and 3, then place each inside a validly re-signed authentication envelope and exercise the production server reader, proving rejection before OpenRaft dispatch, checkpointing, publication, or response write. |
| Shared OpenRaft logical value encoding | No independent marker. Common big-endian helpers encode leader IDs, votes, optional votes, log IDs, optional log IDs, entries, plain memberships, stored-membership wrappers, snapshot metadata, and optional snapshot wrappers. Normal entries contain a separately self-describing command-v15 envelope; snapshot bytes contain a separately self-describing replicated-snapshot-v1 envelope. | Peer request/response/snapshot writers, restart-artifact capture, and WAL-frame append call the applicable private helpers. Leader IDs, votes, log IDs, optional log IDs, entries, and plain membership values are used by peer RPC v2, restart artifact v4, and WAL frame v1. Stored membership and snapshot metadata are used by peer v2 and restart v4. Optional votes and optional snapshots are restart-v4-only wrappers. | Each containing reader first rejects its own outer version, then uses the shared `RaftArtifactReader` logical decoders. There is no inner version at which a shared primitive or wrapper change can be rejected. | Current peer, restart, WAL, snapshot, membership, and recovery tests provide extensive round-trip and semantic coverage. They do not jointly seal the common tag values, field order, integer widths, option encoding, membership ordering, stored-membership wrapper, optional-vote/snapshot wrappers, or derived snapshot metadata across their exact containers. | Bind incompatible leader-ID, vote, log-ID, optional-log-ID, entry, or plain-membership changes to peer RPC v2, restart artifact v4, and WAL frame v1. Bind stored-membership or snapshot-metadata changes to peer v2 and restart v4 only. Bind optional-vote and optional-snapshot wrapper changes to restart v4 only. Add a fixed logical-value corpus plus peer, restart, and WAL fixtures that seal each shared value only where it is contained; include both arms of every applicable option wrapper and all entry/membership tags. The snapshot metadata currently retains a derived snapshot-id string solely for compatibility with an older OpenRaft representation; upgrades and mixed-version peers are unsupported, so decide whether to remove that compatibility field. Removing it is an incompatible peer/restart change and must advance both containing versions with new exact fixtures, not add a fallback reader. |

Peer RPC version 2 is authoritative for its frame magic, checksum, direction and operation tags,
identity layout, operation payloads, and the shared logical values as embedded in peer frames. The
authentication envelope independently authenticates the exact peer-frame bytes and binds the
cluster, principals, target, and operation; changing peer v2 does not silently advance shared
authentication-envelope v1. Conversely, changing the shared envelope requires the consumer-level
peer rejection evidence listed in that format's audit. Transport length framing is parsed before
either inner version, so its explicit peer-v2/ALPN binding is required even though no compatibility
window is supported.

#### Raft restart and WAL evidence inventory (2026-08-09)

The former combined Raft restart/WAL row contains four self-describing formats. The restart
artifact and WAL file form one recovery pair through the restart artifact's logical WAL replay
offset. The sentinel is an independent durable existence/identity marker: it is published before
the first restart artifact and prevents a missing artifact from being mistaken for an empty
authority. WAL file v2 uses the shared durable-journal header and frame stream inventoried above;
each journal payload is independently framed and checksummed as WAL frame v1.

| Format | Defining marker | Current writer | First rejecting reader | Existing permanent evidence | Evidence still required |
| --- | --- | --- | --- | --- | --- |
| Raft restart artifact | Private `CONTROL_PLANE_RAFT_RESTART_MAGIC = ARGMINCPRAFT`, followed by big-endian `CONTROL_PLANE_RAFT_RESTART_VERSION = 4`, length-prefixed cluster identity, node ID, logical WAL replay offset, log-store state, state-machine state, and trailing big-endian CRC64. Log-store and state-machine fields use the shared OpenRaft logical codecs; state snapshots contain separately versioned replicated-snapshot-v1 bytes. | Authority checkpoint capture obtains a mutually valid log-store/state-machine pair and current WAL replay offset. `store_durable_artifact_with_metrics()` validates the pair and established sentinel identity, encodes v4, writes and fsyncs a private temporary file, atomically renames it, and fsyncs the parent before WAL compaction can advance past the checkpoint. | `load_durable_artifact_for_restore()` reads the complete file; `decode_durable_artifact_before_restore_validation()` verifies CRC64 before magic/version, then decodes every nested value. Startup next requires the sentinel and configured identity, replays WAL from the persisted offset, validates the reconstructed pair and configured topology, and only then constructs the live authority. Format failures are free-form `ControlPlaneError::CommandDecode`. | Codec and file round trips cover populated log/state-machine state, optional fields, atomic replacement, stale temporary files, capture consistency, corruption, wrong identity/membership, cached-snapshot replay, committed-ahead recovery, WAL-only committed suffix application, checkpoint/WAL compaction, and full authority restart. A direct malformed fixture covers truncation, resealed bad magic, checksum failure, an unknown entry tag, and a correctly checksummed minimal version-5 frame. Existing fixtures are generated by the current writer rather than fixed bytes. | Introduce a private typed truncated/unknown-magic/unsupported-restart-version result. Add an exact aggregate v4 golden covering every restart field and all restart-only shared wrappers, including both optional-vote and optional-snapshot arms. Add correctly resealed versions 3 and 5 with structurally valid remaining fields. Exercise both through the public durable-authority open path with a valid current sentinel and WAL, proving rejection before WAL replay/truncation, state-machine construction, checkpoint repair/replacement, authority publication, or serving. The fixed restart/WAL pair must also prove that the v4 replay offset selects the intended v2 journal frame after compaction. |
| Raft restart sentinel | Private `CONTROL_PLANE_RAFT_RESTART_SENTINEL_MAGIC = ARGMINCPRAFTSEEN`, followed by big-endian `CONTROL_PLANE_RAFT_RESTART_SENTINEL_VERSION = 1`, length-prefixed cluster identity, node ID, and trailing big-endian CRC64 | Restart publication validates any existing sentinel, then atomically writes and parent-syncs v1 before encoding or replacing the restart artifact. The sentinel is deliberately not an artifact checksum or WAL identity; its stable existence records that absence of the main artifact is no longer an empty first start. | On existing-state startup, the restart artifact is decoded first and the sentinel is then loaded, version-checked, and identity-checked before WAL replay. If the artifact is missing, startup still loads the sentinel and fails closed instead of constructing empty state. Store likewise decodes and validates an existing sentinel before replacing either file. Failures use free-form `ControlPlaneError::CommandDecode`. | Owner tests round-trip v1, directly reject correctly resealed versions 0 and 2, reject mismatched sentinel identity before artifact creation, and cover missing-artifact/required-sentinel, artifact-without-sentinel, and wrong-sentinel-identity startup cases. There is no exact byte fixture, malformed magic/checksum matrix, or unsupported-version startup fixture. | Introduce a private typed truncated/unknown-magic/unsupported-sentinel-version result and add an exact v1 golden plus checksum, missing/truncated marker/version, malformed-magic, and trailing-byte fixtures. Exercise resealed versions 0 and 2 through startup both beside a current artifact and as the only established-state marker, proving they neither permit empty initialization nor cause artifact, sentinel, WAL, checkpoint, or serving publication. Sentinel v1 advances only when its own identity/existence representation changes; restart v4 or WAL changes alone do not require changing this independent marker. |
| Raft WAL file | Private `CONTROL_PLANE_RAFT_WAL_FILE_MAGIC = ARGMINCPRAFTWALFILE` and `CONTROL_PLANE_RAFT_WAL_FILE_VERSION = 2`, supplied to `DurableJournalFile` with a big-endian base offset, header CRC64, and shared length/complement frame stream | The shared journal writer creates the v2 owner header before the first WAL frame, appends and syncs every record before the corresponding in-memory OpenRaft mutation is published, and rewrites a current v2 file during checkpoint compaction | `DurableJournalFile::read_frames_from()` accepts an absent or physically empty WAL only at replay offset zero. Otherwise its production `decode_file_header()` verifies header CRC64, magic, and version before offset interpretation or frame scanning. Shared framing validation then rejects malformed interior frames and identifies a recoverable torn final frame; only after every returned WAL frame is decoded successfully may replay truncate that torn tail. Failures are free-form `ControlPlaneError::CommandDecode`. | Replay tests cover missing/empty files, live mutation parity, logical offsets, checkpoint compaction, torn-final-frame truncation, corrupt first/interior lengths, identity mismatch, invalid record sequences, fsync/parent-sync ambiguity, poison behavior, and durable publication ordering. Correctly resealed versions 1 and 3 are rejected only through `ControlPlaneRaftWalFile::decode_file_header()`, a test-only duplicate of the production shared-journal header codec; this does not prove the live WAL reader rejects before replay or repair. | Remove the duplicated test-only header codec as version evidence and test exact v2 bytes through the storage-owned shared-journal implementation. Add owner-specific full-file goldens containing all WAL-frame record kinds and the shared length/complement framing. Exercise correctly resealed file versions 1 and 3 through restart with a current v4 artifact/sentinel, proving rejection before frame decoding, torn-tail truncation, log/state mutation, checkpoint compaction, authority publication, or serving. Retain the shared rule that incompatible journal framing advances both owner file versions, while logical offset changes also advance restart v4. |
| Raft WAL frame and records | Private `CONTROL_PLANE_RAFT_WAL_MAGIC = ARGMINCPRAFTWAL`, followed by big-endian `CONTROL_PLANE_RAFT_WAL_VERSION = 1`, length-prefixed cluster identity, node ID, record-kind/body, and trailing big-endian CRC64. Record tags cover save-vote, append entries, save-committed, truncate-after, and purge. Their bodies use the applicable shared OpenRaft logical codecs. | Every durable OpenRaft log-store mutation constructs a v1 identity-bound frame and appends it as one payload in WAL-file v2 before publishing the accepted mutation in memory | After the v2 journal reader validates file/header/framing, `ControlPlaneRaftWalFrame::decode_frame()` verifies frame CRC64 before magic/version, decodes one record, rejects trailing bytes, and then validates cluster/node identity before replay. All frames are decoded before the logical record sequence is applied to a fresh log-store artifact. Failures use free-form `ControlPlaneError::CommandDecode`; invalid replay sequences become storage I/O failures. | Owner round trips cover all five record kinds, though optional committed/truncate values use only their present arms. Malformed fixtures cover truncation, resealed bad magic, a correctly checksummed version-2 frame, checksum mismatch, trailing bytes, and unknown record kind. File/restart tests cover replay equivalence, invalid sequences, WAL-only suffix application to the public authority, identity mismatch, and no publication on definite pre-append durability failure. No exact current frame bytes are sealed. | Introduce a private typed truncated/unknown-magic/unsupported-WAL-frame-version result. Add exact v1 aggregate frame fixtures covering all five record tags, both optional committed/truncate arms, and blank/membership/normal appended entries. Add correctly resealed versions 0 and 2 containing structurally valid records, place them inside valid current WAL-file v2 framing, and exercise restart with a current artifact/sentinel. Prove rejection before applying any WAL record, truncating the file, compacting the journal, constructing the live authority, or serving. |

Restart v4 owns the cluster/node identity, WAL replay-offset field, log-store/state-machine layout,
and its nesting of the shared OpenRaft logical values and replicated snapshot. WAL file v2 owns the
owner-specific header and shared journal framing, while WAL frame v1 independently owns frame
identity, record tags/bodies, and frame integrity. A WAL-record semantic change advances frame v1;
a shared logical-value change advances exactly the restart/peer/WAL containers listed in the
preceding audit. Sentinel v1 remains independent unless its own durable existence or identity
semantics change. None of these rules permits an older reader or default-version fallback.

#### Encrypted checksum-metadata evidence inventory (2026-08-09)

Encrypted object checksums combine one checksum-owned logical tag table, one shared
`server-core` plaintext frame, and two independently selected sealing profiles. The storage-owned
SSE-C and SSE-S3 states carry only a nonce and bounded opaque ciphertext; `storage` cannot inspect
the authenticated plaintext during row, command, RPC, checkpoint, or restart validation. The
first semantic reader is therefore the key-aware `server-core` object-read path, not a storage
decoder.

| Format | Defining representation | Current writer | First rejecting reader | Existing permanent evidence | Evidence still required |
| --- | --- | --- | --- | --- | --- |
| Shared checksum algorithm/type tags | `checksum::ChecksumAlgorithm` has explicit `u8` tags 0 through 9; `ChecksumType` has tags 0 and 1; optional checksum type uses 255 for absent. The representation has no independent marker. | `server-core` directly casts these enums into system-metadata v1 and encrypted-checksum plaintext v1. `storage` directly casts them into PG schema-v3 multipart columns, metadata-command v8, and storage-RPC v21; the PG values also enter canonical-state v5 and metadata-checkpoint v2. | Each containing decoder invokes `ChecksumAlgorithm::from_u8()` and `ChecksumType::from_u8()` only after its own outer version has been accepted. PG row decoding validates the integer columns, while canonical-state/checkpoint verification binds their raw values. | `server-core`'s system-metadata exact-byte corpus pins all ten algorithm tags, both type tags, and the absent sentinel. The `checksum` owner tests round-trip every algorithm and explicitly pin the two type tags, but coordinated algorithm renumbering would still pass the owner test. Metadata-command, RPC, PG, canonical-state, and checkpoint tests cover representative logical checksum configurations rather than one cross-container exact tag corpus. | Treat `checksum` as the sole owner of this shared nested tag table and add an owner-local exact tag fixture. Add a fixed representative tag fixture in every direct containing format. With the current shared encoding, any incompatible tag change advances system-metadata v1, encrypted-checksum plaintext v1, both outer encryption-state versions and their recorded containers, PG schema v3, metadata-command v8, storage-RPC v21, canonical-state v5, and metadata-checkpoint v2, plus the already recorded carriers of canonical-state semantics. A representation-neutral refactor from direct enum casts to checksum-owned conversion methods may preserve every byte without advancing a version. |
| Encrypted checksum-metadata plaintext | Private `SSE_C_CHECKSUM_METADATA_VERSION = 1`—despite its historical name, it is shared by SSE-C and SSE-S3—followed by algorithm tag, checksum-type tag or 255, big-endian `u16` UTF-8 value length, and the exact value bytes. Absence is represented outside the frame by empty ciphertext. | `encrypt_checksum_with_dek()` calls `encode_checksum_metadata()` only for a present logical checksum, then seals those bytes under the selected profile. `Coordinator::prepare_stored_system_metadata()` removes the cleartext checksum from system metadata before attaching the sealed value to the object-encryption state for PutObject, streaming PutObject, or CompleteMultipartUpload. | `decrypt_checksum_with_dek()` first authenticates/decrypts the selected profile, then `decode_checksum_metadata()` checks minimum length, exact version 1, algorithm/type tags, exact declared length, and UTF-8. `deserialize_visible_system_metadata()` invokes it for SSE-S3 automatically and for SSE-C only after valid customer-key headers are supplied, before returning read/list metadata or using source metadata for CopyObject. Failures are free-form `ServerError::InternalError`. | Unit tests round-trip one SHA-256/FULL_OBJECT frame through each profile and directly reject structurally current plaintext versions 0 and 2. The fixed SSE-S3 ciphertext vector indirectly seals one exact current plaintext. The complete tag table is pinned only through the separate system-metadata codec. There is no exact plaintext corpus or malformed algorithm/type/length/UTF-8 matrix. | Rename the private version constant to reflect the shared format without changing bytes. Introduce a private typed truncated/unsupported-version/invalid-tag/invalid-length/invalid-UTF-8 result and map it to a redacted internal server failure. Add exact v1 plaintext fixtures covering every shared tag, absent type, empty and multibyte values, and the effective 65,514-byte accepted/65,515-byte rejected value boundary. Encrypt correctly formed versions 0 and 2 under each current profile, embed them in current outer states, and prove key-aware Head/Get and Copy source reads reject before metadata response or destination mutation. An incompatible field-layout change advances plaintext v1, both outer encryption-state versions, and every recorded outer container. |
| SSE-C checksum-sealing profile | AES-256-GCM with a 12-byte random nonce, 16-byte authentication tag, and exact AAD `argmin:sse-c:checksum:v1`. The ciphertext has no profile marker; storage-owned SSE-C state v3 selects this profile and stores the nonce plus a big-endian `u16` ciphertext length. | `SseCustomerWriteContext::seal_checksum_metadata()` uses the per-object DEK already wrapped by the customer-derived key, writes the current plaintext, generates the checksum nonce, seals with the SSE-C AAD, and constructs the bounded outer state. An absent checksum is written as a zero nonce and empty ciphertext. | With valid SSE-C request headers, `decrypt_sse_customer_checksum()` validates the customer-key proof, unwraps the per-object DEK, authenticates the exact nonce/AAD/ciphertext, and only then invokes the plaintext reader. Without SSE-C headers the checksum intentionally remains hidden; a wrong key is rejected before decryption. | Randomized unit round-trip proves current sealing/opening. A coordinator test proves a persisted checksum is absent from cleartext system metadata, is recovered by HeadObject with the right key, and is denied with the wrong key. Storage exact-byte tests pin the outer v3 nonce/ciphertext carrier, but no fixed SSE-C cryptographic vector pins the checksum AAD, algorithm, tag, or nonce use. | Add a fixed SSE-C vector covering plaintext, derived/wrapped DEK inputs, nonce, exact AAD, ciphertext, and tag, plus wrong-nonce/AAD/ciphertext/tag rejection. Add owner-spanning logical persistence/read evidence using the fixed profile. Tighten construction and decode to accept only the current writer's zero-nonce/empty-ciphertext absent representation; this preserves SSE-C state v3 and every containing version because it rejects bytes the current writer has never emitted. The plaintext encoder currently accepts values through 65,535 bytes, while the outer `u16` must also contain the five-byte plaintext header and sixteen-byte GCM tag; centralize and test the effective 65,514-byte value bound before encryption. A future change to the sealing algorithm, nonce/tag size, AAD, or canonical absent representation advances outer SSE-C state v3 and all of its recorded containing formats. |
| SSE-S3 checksum-sealing profile | AES-256-GCM with a 12-byte random nonce, 16-byte authentication tag, and exact AAD `argmin:sse-s3:checksum:v1`. The ciphertext has no profile marker; storage-owned SSE-S3 state v1 selects this profile and stores the nonce plus a big-endian `u16` ciphertext length. | `ManagedEncryptionWriteContext::seal_checksum_metadata()` uses the per-object DEK wrapped under the selected managed key, then invokes the same plaintext writer and sealing primitive with the distinct managed AAD. An absent checksum is written as a zero nonce and empty ciphertext. | `decrypt_managed_encryption_checksum()` resolves the retained wrapping-key ID, unwraps the per-object DEK, authenticates the exact nonce/AAD/ciphertext, and invokes the shared plaintext reader before checksum metadata is returned by read/list/copy paths. | Unit round-trip covers current managed sealing. `sse_s3_wire_format_vectors_stay_stable` pins a complete fixed wrapping, segment, and checksum vector, including the current checksum plaintext, nonce, AAD, ciphertext, and authentication tag. There is no persisted coordinator-level checksum read, tamper matrix, malformed-inner fixture, or absent-carrier canonicality test. | Retain the exact current vector and add wrong-nonce/AAD/ciphertext/tag rejection plus persisted PutObject/Head/Get/Copy evidence. Tighten construction and decode to accept only the current writer's zero-nonce/empty-ciphertext absent representation; this preserves SSE-S3 state v1 and every containing version because it rejects bytes the current writer has never emitted. Apply the same 65,514-byte effective value bound as SSE-C and exercise encrypted plaintext versions 0 and 2 through the managed production read path. A future change to the sealing algorithm, nonce/tag size, AAD, or canonical absent representation advances outer SSE-S3 state v1 and all of its recorded containing formats. |

The checksum tag table is a `checksum`-owned shared nested representation, not an accidental
property of Rust enum declaration order. Until direct casts are replaced by owner-controlled
conversion methods, every listed containing version is bound to those numeric values. Plaintext
checksum framing has its own version inside the authenticated ciphertext, while the repository's
coordinated nested-format rule also advances both outer encryption-state families for an
incompatible plaintext change. The sealing profiles cannot select a replacement algorithm or AAD
before decrypting the payload, so a profile-only change advances the applicable outer
encryption-state version. Rejecting noncanonical absent carriers while retaining the writer's
existing zero/empty representation is explicitly not such a change and preserves the current
versions. No rule introduces a fallback decrypter or accepts an older plaintext frame.

#### Tag and ACL inner-carrier design decision (2026-08-09)

The current durable tag representation is also the AWS response XML, and the current ACL
representation is an unframed line grammar. Although both parsers are exact and owner-local, their
versions are implicit in five storage-owned containers. Permanent outer-only binding would make
those storage versions stand in for `s3-types` representation ownership and would keep tag
persistence coupled to HTTP response serialization. The selected design is therefore an explicit
private inner frame for each logical family.

| Family | Selected target frame | Owner and logical invariant | First rejecting reader |
| --- | --- | --- | --- |
| Object and bucket tags | Exact ASCII header `ARGMIN-TAGSET/1\n`, followed by an owner-private canonical tag XML payload. Object and bucket tags share identical frame bytes for the same ordered logical tags; storage still applies the typed 10-tag or 50-tag cardinality before construction and during decode. The empty tag set contains the complete header and canonical empty payload. | `s3-types` owns the private magic/version, durable payload writer/parser, tag grammar, order, and duplicate/cardinality validation. HTTP continues to serialize the logical `TagSet` as AWS XML through a distinct response path; storage production code may no longer persist `TagSet::to_xml()` output directly. | The owner reader recognizes the fixed `ARGMIN-TAGSET/` magic, then parses one nonempty, overflow-checked canonical decimal version token terminated by exactly one newline. Missing delimiters, empty/nondigit/overflow tokens, and leading zeroes return a typed malformed-or-noncanonical-version result; a canonical decimal other than 1 returns typed unsupported-version. Both occur before XML parsing or logical construction. The reader then requires the exact owner-private canonical payload and no trailing bytes. |
| ACL grants | Exact ASCII header `ARGMIN-ACL-GRANTS/1\n`, followed by the existing owner-private canonical grant-line payload. The empty ACL is the header with an empty payload rather than an empty durable string. | `s3-types` owns the private magic/version, grantee and permission tags, canonical sorting/deduplication, canonical-user spelling, and exact payload parser. HTTP and `server-core` continue to use only logical `AclGrants`. | The owner reader recognizes the fixed `ARGMIN-ACL-GRANTS/` magic, then applies the same nonempty, overflow-checked canonical-decimal and exact-newline grammar. Missing delimiters, empty/nondigit/overflow tokens, and leading zeroes return typed malformed-or-noncanonical-version; a canonical decimal other than 1 returns typed unsupported-version. Both occur before parsing any grant. The reader then requires the exact canonical payload and no alternate line endings, reordering, duplication, normalization, or trailing bytes. |

The target remains a text frame deliberately: it preserves the existing SQLite `TEXT` columns and
generic bucket-subresource body without a new dependency or a mixed SQLite storage class. The
header separates durable bytes from AWS response XML and supplies an owner-readable version; the
payload remains human-inspectable and reuses the already proven logical validation. A later binary
carrier would be an incompatible inner version rather than an unmarked rewrite.

Ownership and API requirements:

- Replace the unframed public codec methods with opaque `s3-types` stored-tag and stored-ACL
  carriers. Their version constants, header parser, and payload codecs remain private to the owner.
- Storage's object and bucket tag carriers wrap the owner carrier plus its logical projection.
  Storage persists/transmits only the framed text and never interprets the header or payload.
  ACL-bearing storage paths likewise convert through the owner carrier at every row, command, RPC,
  digest, and checkpoint boundary.
- The boundary checker allows durable-carrier byte access only in `s3-types` owner code/tests and
  the `storage` persistence boundary, rejecting it in every other crate. It rejects
  `TagSet::to_xml()` in storage production persistence paths, rejects the removed unframed ACL/tag
  codec methods, and rejects making the owner marker/version public. Negative fixtures cover
  qualified, method-call, multiline-impl, and conversion-trait bypasses.
- Cross-crate tests construct logical `TagSet` or `AclGrants` values. Exact frame and impossible
  representation tests remain in `s3-types`; storage tests inject malformed frames only through
  owner-local row/command/RPC/checkpoint facilities.

The first framing change alters every current persisted and transmitted value even though the
SQLite column types and outer field shapes remain text. Tags and ACLs were implemented together so
their shared container versions advanced once, not in successive baseline changes:

- PG SQLite schema version 1 advances to 2.
- Applied metadata-command encoding 6 advances to 7.
- Storage-node RPC frame encoding 16 advances to 17.
- Canonical metadata-state encoding 4 advances to 5.
- Metadata-checkpoint encoding 1 advances to the self-describing `ARGMCPKT` frame version 2, and
  the independent log-hash/state-digest carriers plus their exact one-time direct container
  versions advance as listed in the selected proof-carrier decision above.

The metadata-checkpoint and canonical-state carrier gate is complete and the coordinated
implementation updated the proof carriers, checkpoint frame, tag/ACL carriers, direct containers,
writers, and first readers atomically. Because upgrades are unsupported, unframed XML/grant
strings and inner versions 0 or 2 are corruption—not legacy inputs—and no migration or fallback
parser exists.

Implemented evidence for the coordinated slice:

- Owner exact-byte goldens for empty and representative v1 frames, every ACL grantee/permission,
  ordered Unicode tags and escaping, and both tag cardinality profiles.
- Typed missing/truncated/unknown-magic/malformed-or-noncanonical-version/unsupported-version/
  malformed-payload/noncanonical-payload fixtures. For both frames, the version-token matrix
  includes a missing newline delimiter, empty token, `/x\n`, `/01\n`, numeric overflow, and a
  noncanonical line ending, all rejected before payload parsing. Canonical versions 0 and 2 carry
  otherwise valid payloads and reach the distinct unsupported-version result.
- Owner round trips plus storage row, applied-command, direct RPC, canonical-digest, runtime-config,
  control-plane, and route-digest fixtures reject unsupported inner versions. Correctly resealed
  checkpoint versions 1 and 3 are rejected directly, through persisted catalogue selection, and
  through an authenticated install request before destination mutation.
- Read-after-write tests for objects, multipart uploads, buckets, and bucket tagging, plus unchanged
  AWS-facing tag XML and ACL response tests proving the durable split does not alter S3 behavior.
- Exact current fixtures pin every newly advanced outer format and immediately older/newer versions
  at each authoritative reader. Broader pre-existing evidence gaps for an outer format remain on
  that format's own matrix row; they do not reopen an old coordinated representation.

After the initial coordinated advancement, the inner frame is authoritative for tag/ACL payload
syntax. A later inner-only payload change need not advance PG, command, or RPC versions when their
opaque length-prefixed text field grammar is unchanged. If equivalent logical state produces a
different canonical digest, it advances the inner `CanonicalStateDigest` version; route and
identity digest values then change because their stable v3 grammar hashes that inner version, but
their outer versions do not advance. Changing a containing field shape, SQLite storage class, or
outer validation order still advances that containing format. This rule prevents both redundant
version coupling and silent digest drift.

#### Static manifest and outer-identity evidence inventory (2026-08-09)

The former combined static-cluster row contains six independently changeable boundaries. The
manifest is operator-authored semantic TOML rather than a canonical byte stream. Its three derived
digests use one private canonical field encoder: each field is a big-endian `u16` tag, a big-endian
`u64` byte length, and the exact value bytes; collections start with a big-endian `u32` item count
and frame each item with a big-endian `u16` item version and `u64` length. Each digest includes its
own v1 domain as a tagged field and renders SHA-256 as 64 lowercase hexadecimal characters. The two
outer identities then durably bind the topology and selected-process digests to storage or
control-plane state.

| Format | Current writer / derivation | First rejection or comparison point | Current evidence | Required evidence and decision |
| --- | --- | --- | --- | --- |
| Static cluster manifest schema | There is no production serializer. Operators, committed guide examples, and deployment tooling supply a required TOML `schema_version = 1` plus the complete v1 body. | `parse_static_cluster_manifest()` first deserializes the complete input as `StaticClusterManifestInput`, including all v1 fields and enums, and only then rejects a value other than 1 in `validate_static_cluster_manifest()`. Missing, wrongly typed, out-of-range, or duplicate schema fields and malformed current-body fields therefore become generic `invalid cluster manifest` strings rather than a schema-marker result. | Both guide examples parse. Struct-level unknown-field rejection and a large semantic validation matrix exist. Current-shaped versions 0 and 2 return the unsupported-schema string. | Introduce an owner-private typed missing/malformed/unsupported-schema result and a bounded schema preflight that parses TOML syntax and the marker without interpreting the current-version body. Only schema 1 may be deserialized into `StaticClusterManifestInput`. Pin missing, duplicate, negative, noninteger, out-of-`u32`, 0, and 2 markers, including old/new versions crossed with missing or invalid v1-only fields. Prove rejection before secret/material reads, endpoint/listener construction, state-directory locks, identity publication, or durable-state initialization. Because TOML order, quoting, and whitespace are intentionally noncanonical, exact-byte evidence does not apply; semantic standalone and replicated fixtures must instead pin that every committed example or generator emits schema 1. |
| Static topology identity digest | `topology_digest()` hashes the v1 topology domain, manifest schema, cluster generation, topology-affecting deployment/storage/Raft fields, canonical collections, storage-owned initial PG acting sets, and canonical Raft peer endpoints. | There is no digest reader or embedded version. The 64-character result becomes the topology authority for static initial topology, storage/control-plane authentication, the Raft cluster identity, and both outer identity files. Consumers compare the exact digest as logical identity. | Fixed standalone and replicated output vectors seal the aggregate current encoder. Collection reordering and a field-sensitivity matrix pin important included and excluded fields. | Keep the digest owner-private. Any incompatible field set, tag, integer width, collection framing, hash algorithm, or domain change advances the topology domain and manifest schema together. It also advances storage identity v1 and control-plane identity v2 so old durable state reaches a typed outer-version rejection rather than masquerading as a different current topology. Add exact canonical-input bytes for one compact fixture in addition to the aggregate digest vectors, and exercise the resulting digest through static-topology construction and both exact outer-identity fixtures. No storage RPC, control-plane RPC, or control-plane logical-state version advances merely because the logical topology identity value changes while their field grammar remains unchanged. |
| Static process identity digest | `process_identity_digest()` hashes the v1 process domain, topology digest, selected process/host/kind, and the selected process's canonical authority and storage-node records. | There is no digest reader or embedded version. The result is compared as a logical field in both outer identity formats. | The same fixed vectors seal standalone and replicated results; process selection, collection ordering, and included/excluded-field tests exist. | Any incompatible process field set, tag, canonical framing, hash algorithm, or domain change advances the process domain and manifest schema, plus both outer identity versions. Add one exact compact canonical-input fixture and prove that the fixed storage/control-plane identity bytes contain its expected digest. A topology-digest advance necessarily advances this digest because the topology result is an input. |
| Static full-config fingerprint | `full_config_fingerprint()` hashes the v1 full-config domain, manifest schema, and canonicalized complete configuration, including local paths, material references, credential rotation fields, and transport tuning. | The digest is not persisted or compared during startup. `validate-cluster-config` publishes it to operators under the unversioned `full_config_fingerprint=` label. | Fixed standalone and replicated vectors, order independence, and included/excluded-field tests pin current behavior. | Retain the current v1 algorithm, but make the operator surface explicitly report `full_config_fingerprint_version=1` beside the digest before treating it as a versioned diagnostic contract. Any incompatible field set, canonical framing, hash algorithm, or domain change advances both the private domain and reported version. It advances no durable outer format unless the same encoder change also affects topology or process identity. Add command-level output fixtures for the version and digest. |
| Static storage identity and initialization marker | `StaticStorageIdentity::encode()` writes `ARGSSID\0`, big-endian version 1, topology generation, storage-node ID, and four big-endian-`u16`-length-prefixed UTF-8 strings. The same bytes are atomically published as the initialization marker and final identity and are stored as the opaque `pg_durable_identity.identity_bytes` value in every initialized PG. | Initialization and startup open a bounded regular file, then `StaticStorageIdentity::decode()` checks magic and version before remaining fields and exact exhaustion. The decoded logical identity is compared with the current manifest before PG initialization or inspection; storage subsequently compares the complete opaque bytes with every PG. Failures are free-form strings. | Direct decoder tests mutate current bytes to versions 0 and 2. Initialization, interrupted marker recovery, missing/wrong identity, PG-set mismatch, and publication durability have behavioral coverage. There is no exact v1 byte fixture or full-path unsupported-version test. | Introduce private typed truncated/unknown-magic/unsupported-version/invalid-field/trailing-data results. Add exact v1 bytes, missing/truncated marker and version, bad magic, versions 0/2, UTF-8/length/trailing-data boundaries, and the 1,024/1,025-byte file bound. Exercise old/new versions in both the initialization marker and completed identity through initialization and ordinary startup with otherwise valid PG state, proving no marker/identity replacement, PG creation/open mutation, or cleanup. A storage-identity layout change advances v1 once for the marker, final file, and opaque PG value; the self-describing owner format means the PG schema need not advance solely because these bytes change. |
| Static control-plane outer identity | `StaticControlPlaneIdentity::encode()` writes `ARGSCPID`, big-endian version 2, topology generation, Raft node ID, one established flag byte, the same four length-prefixed UTF-8 fields, then 64 lowercase hexadecimal SHA-256 characters covering the preceding body. Initialization publishes `established = false`; successful durable Raft establishment replaces it with the same identity and `established = true`. | The bounded-file reader verifies the trailing digest before magic/version, then decodes all fields and exact exhaustion. Initialization, establishment, and startup compare the result with the current manifest and node before opening or certifying durable Raft state. Failures are free-form validation or persistence strings, and the shared decoder still renders several control-plane structural failures as `static storage identity`. | Correctly resealed versions 1 and 3 are rejected directly. Tests cover invalid digest, missing identity/state combinations, interrupted publication, identity mismatch, establishment ordering, and persistence failure. There is no exact v2 byte fixture or full-path unsupported-version test. | Introduce private typed truncated/integrity/unknown-magic/unsupported-version/invalid-field/trailing-data results and correct owner-specific diagnostics. Add exact v2 fixtures for both established states, missing/truncated digest/magic/version, resealed bad magic, resealed versions 1/3, invalid flag, UTF-8/length/trailing-data, and the 1,024/1,025-byte bound. Exercise resealed old/new files through initialization, establishment, and ordinary authority startup with an otherwise valid restart artifact and sentinel, proving no identity replacement, checkpoint/WAL/state publication, cleanup, or serving. A control-plane outer-identity layout or integrity change advances v2; a topology/process digest change advances this version in the same coordinated slice. |

The canonical field encoder is shared implementation but not an independently readable format.
Changing its framing advances all three digest domains and manifest schema 1. Because topology then
changes, both identity versions advance too. A full-config-only field-selection change advances
only its domain and reported diagnostic version. A field-layout change confined to one outer
identity advances only that identity version. There are no fallback readers: unsupported manifest
or identity versions fail closed, and operators must initialize fresh state with the current
binary.

The evidence audit proceeds in this bounded order after Phase 1 containment is complete:

1. **Complete:** storage-node TLS, topology, physical payload, maintenance workflow, control-plane
   admin, and implementation-error containment are complete and boundary-checked.
2. **In progress:** expand each `storage` family above to one line per independently changeable
   format, recording its defining constant, writer, first rejecting reader, exact-current fixture,
   and unsupported-version fixtures. The metadata-command and metadata-checkpoint/canonical-state
   families, storage-node RPC/authentication, control-plane logical state/command/snapshot,
   single-authority journal hash-chain binding and durable artifacts, control-plane RPC/shared
   authentication, Raft peer transport/RPC/shared logical values, and Raft restart/sentinel/WAL
   artifacts are recorded above.
3. **Complete:** do the same for the remaining `server-core`, `argmin-s3`, and `auth` rows, without
   exposing private constants or codecs to cross-crate tests. The shared checksum-tag and encrypted
   checksum-metadata families and the static-manifest/digest/identity family are recorded above;
   the temporary-credential envelope was already recorded by its owner.
4. **Complete:** the two `s3-types`-owned tag/ACL text frames and the independent
   checkpoint/log-hash/state-digest carriers are implemented with their one-time coordinated
   downstream advances and repository boundary enforcement.
5. **Complete:** the atomic checkpoint/proof/tag/ACL baseline was reviewed and verified. Continue
   only with the remaining owner-specific gaps in the matrix. Subsequent evidence work remains
   owner-bounded and must not add a fallback reader. The PG SQLite v3 manifest and automatic bump
   regression are now recorded as the first such owner-specific closure.

Phase 2 is complete only when every row is `Recorded`, no representation relies on an implicit
version assumption, and the audit finds no older-version parser, default-version fallback, or
mutation before version rejection.

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

The higher-layer control-plane error cleanup, control-plane and Raft transport containment, raw
diagnostic and Raft representation containment, durable Raft restart/WAL containment, nested
durable codecs, static route authority, session-token ownership, storage-node TLS profiles, and
semantic topology and maintenance work are complete and boundary-checked.

## Immediate Next Steps

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
    work in `completed/storage-boundary-compiler-enforcement-plan.md` completed on 2026-08-07. Raw
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

After items 9 through 14 are complete, work proceeds through the Phase 2 evidence gate rather than
reopening containment opportunistically.
