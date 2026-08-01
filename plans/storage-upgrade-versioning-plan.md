# Internal Format Ownership, Upgrade And Versioning Plan

Status: Phase 0 complete; Phase 1 reopened for storage topology and maintenance containment;
Phase 2 evidence audit planned

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

Owner-local unsupported-too-old and unsupported-too-new fixtures now lock every exact-version
check identified by the audit. Each test recomputes the enclosing checksum, digest, or
authenticator where the containing format authenticates its version field, so it reaches the
version rejection rather than a generic corruption path:

- static cluster manifest schema and both static storage/control-plane identity files
- system metadata and the encrypted checksum-metadata projection
- abandoned metadata-command log entries
- storage RPC authentication transport and binding
- control-plane clock checkpoint, durable identity, initialized marker, and journal file/record
- Raft WAL file header and durable restart sentinel

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

**Status: reopened 2026-07-30.** The representation-containment work completed so far remains
valid, but the durable-backfill convergence fix exposed a broader class of semantic storage leaks
that the original audit did not cover. PG identities, route state, physical payload placement,
maintenance claim protocols, control-plane topology workflows, and implementation error variants
still cross into `server-core`, `server-http`, or `argmin-s3`. Storage-node TLS profile construction
also remains outside `storage`. Phase 1 is complete only after both the representation and semantic
topology/maintenance boundaries below are closed and regression-checked.

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
| Storage-node RPC | `storage` | Framing, wire errors, transports, and malformed-wire tests are storage-owned. Residual: `argmin-s3` still imports `STORAGE_RPC_TLS_ALPN` and constructs raw Rustls client/server profiles before passing them into storage endpoints/listeners. |
| Control-plane durable state, RPC, and auth envelope | `storage` | Complete: client and server transport are contained behind typed Unix/TLS endpoints and opaque storage-owned facades. |
| Raft peer protocol, restart artifact, and WAL | `storage` | Peer wire and durable representations are contained: raw frames, restart artifacts, WAL records/files, and layout helpers are private; process tests use logical clients and opaque semantic recovery inspection. |
| PG topology, route state, and physical payload placement | `storage` | Incomplete: `server-core` and `argmin-s3` still construct and interpret PG identifiers, route snapshots, acting sets, placement epochs, EC shard requests, and physical shard locations. |
| Physical storage maintenance workflows | `storage` | Complete: shard scavenging, repair, backfill, payload reclaim, accepted bucket-delete continuation/finalization, and abandoned stream-session cleanup run behind opaque storage-owned workers; their durable cursors, claims, work records, cleanup roots, and debug snapshots are private. |
| Control-plane topology and metadata-transfer workflows | `storage` | Incomplete: `argmin-s3` constructs control-plane commands and implements PG fencing, route inspection, metadata transfer, and topology convergence. |
| Storage implementation-error taxonomy | `storage` | Incomplete: `server-core` matches PG, database, shard, route, command-log, and RPC `StoreError` variants for diagnostics and retry behavior. |
| Object user/system metadata blobs | `server-core` | Complete: serialization is crate-private and storage carries only opaque validated blobs. |
| Tag and ACL canonical value formats | `s3-types` | Complete: validation and canonical codecs are centralized; storage owns and validates their containing row, command, checkpoint, digest, and RPC formats. |
| Object encryption state | `storage` | Complete: the durable codec is private and callers receive only typed encryption state. |
| Session-token envelope | `auth` | Exact-current version selection and representation constants are private to `auth`; callers use semantic credential issuance and authentication APIs, enforced by the boundary check. |
| Static manifest and process identity files | `argmin-s3` | File codecs and process identities are contained, but the manifest's storage-topology section still becomes `Vec<(PgId, Vec<NodeId>)>` in `argmin-s3`; storage must own validation and interpretation of that subdocument. |
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
| PG SQLite schema | schema version 1 | `objects`, `multipart_uploads`, and `stream_uploads` store user and system metadata blob columns. |
| Metadata command | encoding version 5 | Object, multipart-upload, and stream-session command values carry both opaque blobs. |
| Storage-node RPC | frame encoding version 12 | Logical object, multipart, and stream request/response payloads carry both opaque blobs. |
| Canonical PG state | encoding version 4 | The metadata columns participate in canonical row and state digests. |
| Metadata command checkpoint | encoding version 1 | Checkpoint table blocks carry the metadata columns and bind them into row, table, state, and checkpoint digests. |

Changing either metadata encoding requires a new inner version and coordinated advancement of
every containing format that can persist, replay, hash, or transmit the changed bytes. There are
no old-version or prefix-decoding fallbacks. The separately encrypted checksum projection used
by SSE-C and SSE-S3 is a private `server-core` version-1 codec nested inside the storage-owned
encryption state; changing it also requires advancing the corresponding encryption inner version
and all of that format's containing versions.

### Nested Durable Codec Inventory: Object Tags (2026-07-28)

`s3-types` owns the shared AWS tag grammar and logical `TagKey`, `TagValue`, `Tag`, and `TagSet`
types. The service layer chooses only the operation-specific cardinality: object tag sets permit
at most `MAX_OBJECT_TAGS` (10), while bucket-resource tag sets permit at most `MAX_BUCKET_TAGS`
(50). HTTP request parsers may accept the AWS request spellings established by their service
oracles, but they normalize successful requests into the shared logical types before calling
`server-core`.

The current stored object-tag representation is the exact `TagSet::to_xml()` output: an XML 1.0
UTF-8 declaration, newline, S3-namespaced `Tagging`/`TagSet` envelope, ordered `Tag` members, and
canonical escaping. `storage::SerializedTagSet` is an opaque carrier containing that private XML
and its validated logical value. External callers can construct it only from a logical `TagSet`
and can inspect only that logical value. PG, metadata-command, and storage-RPC decoders reject
malformed tag values and every well-formed but noncanonical XML spelling; request-parser
tolerance therefore cannot create additional durable representations. Exact owner-local goldens
pin the empty and representative current XML, and impossible-representation tests remain in
`s3-types` and `storage`.

The object-tag XML is embedded in these storage-owned containing formats:

| Containing format | Current baseline | Object-tag embedding |
| --- | --- | --- |
| PG SQLite schema | schema version 1 | `objects`, `multipart_uploads`, and `stream_uploads` store optional canonical object-tag XML. |
| Metadata command | encoding version 5 | Object, multipart-upload, and stream-session command values carry the opaque tag set. |
| Storage-node RPC | frame encoding version 12 | Logical object, multipart, stream, mutation, and tag-read payloads carry the opaque tag set. |
| Canonical PG state | encoding version 4 | The tag columns participate in canonical row and state digests. |
| Metadata command checkpoint | encoding version 1 | Checkpoint table blocks carry tag columns and bind them into row, table, state, and checkpoint digests. |

An incompatible change to the canonical object-tag XML is prohibited until the Phase 2 design
gate chooses and records one of two strategies: introduce an explicit private inner version, or
treat the complete set of containing formats above as the version boundary and advance every one
of them together. The current decoders have no legacy or prefix fallback.

Bucket tags use the same `s3-types` logical values and exact `TagSet::to_xml()` representation,
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
| PG SQLite schema | schema version 1 | `bucket_subresources` stores canonical bucket-tag XML under the private tagging discriminator. |
| Metadata command | encoding version 5 | Bucket-subresource put/delete commands carry the discriminated typed tag mutation. |
| Storage-node RPC | frame encoding version 12 | Bucket snapshots, typed reads, and subresource mutations carry the opaque bucket-tag set. |
| Canonical PG state | encoding version 4 | The bucket-subresource body participates in canonical row and state digests. |
| Metadata command checkpoint | encoding version 1 | Checkpoint table blocks carry the bucket-subresource row and bind it into row, table, state, and checkpoint digests. |

The Phase 2 tag decision applies jointly to object and bucket tags because they share this
canonical XML. An incompatible change must either introduce one private tag inner version and
advance all affected containers, or advance every object-tag and bucket-tag containing format as
the deliberate version boundary. Current decoders have no old-version, prefix, or alternate-XML
fallback.

### Nested Durable Codec Inventory: ACL Grants (2026-07-28)

`s3-types` owns the logical `AclGrant`, `AclGrantee`, `AclPermission`, and `AclGrants` values and
their canonical durable representation. Storage APIs carry only logical `AclGrants`; HTTP and
`server-core` neither produce nor consume the durable string. The empty set is the empty string.
Each nonempty grant is one newline-terminated `kind:value:PERMISSION` record, with grants in the
logical type's canonical sort order and duplicates removed. Canonical-user IDs are stored in their
canonical lowercase form. Exact goldens pin both the empty and representative nonempty encoding.

`AclGrants::to_current_storage_string()` is the only encoder and
`AclGrants::parse_current_storage()` rejects malformed values and every parseable but
noncanonical spelling, including reordered or duplicate grants, CRLF separators, normalized
canonical-user IDs, and a missing final newline. PG, metadata-command, and storage-RPC decoders
all use this exact-current decoder. Metadata-checkpoint export and installation find every
`acl_grants` column in the storage-owned table inventory and exact-decode it after row-integrity
validation; checksum-valid noncanonical ACL rows cannot be transferred.

The ACL representation is embedded in these storage-owned containing formats:

| Containing format | Current baseline | ACL embedding |
| --- | --- | --- |
| PG SQLite schema | schema version 1 | `buckets`, `objects`, and `multipart_uploads` store canonical ACL strings. |
| Metadata command | encoding version 5 | Bucket, object, multipart-upload, create, commit, and ACL mutation records carry canonical ACL strings. |
| Storage-node RPC | frame encoding version 12 | Logical bucket, object, multipart, stream-commit, and ACL mutation messages carry canonical ACL strings. |
| Canonical PG state | encoding version 4 | The three persisted ACL columns participate in canonical row and state digests. |
| Metadata command checkpoint | encoding version 1 | Checkpoint table blocks carry all three ACL columns and bind them into row, table, state, and checkpoint digests. |

An incompatible ACL representation change is prohibited until the Phase 2 design gate chooses and
records either a private inner version or deliberate coordinated advancement of every containing
format above. Current decoders have no old-version, prefix, or normalizing fallback.

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
| PG SQLite schema | schema version 1 | `objects`, `multipart_uploads`, and `stream_uploads` store `encryption_type` plus `encryption_state`. |
| Metadata command | encoding version 5 | Object, multipart-upload, and stream-session command values carry the discriminator and nested state bytes. |
| Storage-node RPC | frame encoding version 12 | Logical object, multipart, and stream request/response payloads carry the discriminator and nested state bytes. |
| Canonical PG state | encoding version 4 | The three table representations above include both encryption columns in canonical digests. |
| Metadata command checkpoint | encoding version 1 | Checkpoint table blocks carry the raw encryption columns and bind them into row, table, state, and checkpoint digests. |

Changing either nested encryption encoding requires an explicit new inner version and coordinated
advancement of every containing format that can persist, replay, hash, or transmit the changed
bytes. Because upgrades are unsupported, current decoders reject every non-current inner or outer
version; this containment work does not add fallback readers. Malformed nested-state tests remain
inside `storage`, while cross-crate tests exercise only logical encryption behavior. Storage also
enforces the nested checksum-metadata length when logical state is constructed, so every public
state is encodable, and exact-byte goldens pin the discriminator, inner version, field order,
endianness, and length encoding for all three encryption variants.

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
| Storage-node RPC | `STORAGE_RPC_FRAME_ENCODING_VERSION = 13` in `storage_rpc.rs`; frame magic, message-kind tags, checksums, and payload codecs are crate-private. | Binding version 2 and transport-envelope version 1 in `storage_rpc_auth.rs`. | Exact versions are required before dispatch. There is no negotiation. Treat any other version as incompatible until mixed-version operation is designed. |
| Control-plane RPC | `CONTROL_PLANE_RPC_VERSION = 12` in `control_plane.rs`; the frame contains magic, version, request kind, length, checksum, and payload. | Shared control-plane authentication-envelope version 1 in `control_plane_auth.rs`. | The frame and auth decoders reject non-current versions before logical dispatch. There is no negotiation. Treat any other version as incompatible. |
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
- `storage_rpc_transport` still publicly exposes the protocol ALPN. `argmin-s3` uses it while
  constructing raw Rustls client and server configurations with `argmin-storage-rpc/1`, then
  passes those configurations into storage endpoints and listeners. Endpoint addresses, trust
  roots, certificate identities, and listener bindings are valid public inputs; TLS version,
  ALPN selection, and protocol-profile validation are storage RPC representation and must move
  behind storage-owned TLS endpoint/listener constructors. The ALPN constant must then become
  owner-private and the repository boundary check must reject both the constant and storage-RPC
  Rustls profile construction outside `storage`.
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
  process supplies bound listeners, certificate material, logical peer policy, and an opaque
  durability callback. Storage decides when snapshot responses require checkpointing and ensures
  that checkpointing precedes every possible response write.
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
| Storage failure handling | `server-core` exhaustively matches `StoreError` variants for PG, database, shard, route, command-log, RPC, backfill retry, and diagnostics. | Storage exposes exhaustive semantic operation/maintenance failure classes plus opaque retained diagnostics. Implementation variants and nested causes remain private. |
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
4. Move control-plane topology transitions and live metadata-transfer orchestration behind a
   storage-owned admin/service facade. Then move static storage-topology interpretation behind a
   storage-owned builder while leaving outer manifest/process configuration in `argmin-s3`.
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

The storage-node TLS-profile item and every topology/maintenance item in the audit above remain
unsatisfied Phase 1 exit criteria. Phase 2 may be inventoried in parallel, but implementation of
new version boundaries does not begin until all of this containment work is complete.

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

### Phase 2 Evidence Gate (2026-07-30)

Phase 2 starts with an owner-by-owner evidence audit, not with speculative format changes. A row
is `recorded` only when the plan identifies the private current marker, the current writer, the
owner-side rejection point, and permanent tests for all applicable cases:

1. the current writer always emits the current marker;
2. missing, too-old, and too-new versions fail before dispatch, mutation, or publication;
3. malformed magic and unsupported version are separate failures for magic-plus-version formats;
4. exact current bytes or an equivalent sealed fixture pin the marker location and byte order;
5. authenticated or checksummed formats have resealed unsupported-version fixtures, so a checksum
   failure cannot accidentally stand in for version rejection; and
6. a nested representation either carries its own private marker or has an explicit rule binding
   every incompatible change to advancement of all containing formats.

`Evidence required` means a marker exists in current code but the complete evidence above has not
yet been consolidated in this matrix. `Design required` means the representation has no
self-describing marker and its outer-version binding must be made explicit before deciding whether
to add an inner frame. Neither status permits adding a fallback reader.

| Boundary family | Owner | Current candidate baseline | Gate status |
| --- | --- | --- | --- |
| PG SQLite schema and physical layout | `storage` | `PRAGMA user_version = 1`; version zero is valid only with no user schema objects | Recorded |
| Metadata commands and abandoned-command records | `storage` | command encoding 5; abandoned-command encoding 1 | Evidence required |
| Metadata checkpoints and canonical state | `storage` | checkpoint encoding 1; canonical-state encoding 4 | Evidence required |
| Storage-node RPC and authentication | `storage` | frame encoding 11; auth binding 2; auth transport envelope 1 | Blocked on Phase 1 TLS, topology, payload, maintenance, and error containment; then evidence required |
| Control-plane logical state, commands, and snapshots | `storage` | state 26; command 14; snapshot 1 | Blocked on Phase 1 topology/admin-workflow containment; then evidence required |
| Control-plane RPC and authentication | `storage` | RPC 12; shared authentication envelope 1 | Evidence required |
| Single-authority control-plane durable artifacts | `storage` | clock checkpoint 2; state identity 1; initialized marker 1; journal file 2; journal record 2 | Evidence required |
| Raft peer RPC and authentication | `storage` | peer RPC 2; shared authentication envelope 1 | Evidence required |
| Raft restart, sentinel, and WAL artifacts | `storage` | restart 4; restart sentinel 1; WAL record 1; WAL file 2 | Evidence required |
| Standalone route identity and initialization marker | `storage` | shared format version 1 with distinct magic values; initialization-marker parsing currently compares the complete expected byte string | Evidence required: separate marker magic/version rejection and add too-old/too-new marker fixtures |
| User and system object metadata | `server-core` | user metadata 1; system metadata 1 | Recorded |
| Checksum metadata embedded in SSE-C and SSE-S3 state | `server-core` | checksum metadata 1 | Evidence required |
| Object encryption state | `storage` | SSE-C 3; SSE-S3 1, selected by a typed outer discriminator | Recorded |
| Object-tag and bucket-tag canonical XML | `s3-types` | no independent marker; exact canonical XML is embedded in versioned storage formats | Design required: formalize outer-version binding or add a private inner frame |
| ACL canonical string | `s3-types` | no independent marker; exact canonical string is embedded in versioned storage formats | Design required: formalize outer-version binding or add a private inner frame |
| Static cluster manifest and static identities | `argmin-s3` | manifest schema 1; storage identity 1; control-plane identity 2 | Storage-topology subdocument is blocked on Phase 1 ownership containment; remaining evidence required |
| Temporary-credential session token | `auth` | `ARGST1` envelope / version 1 | Recorded |
| Internal TLS protocol identifiers | `storage` | storage RPC, control-plane RPC, and Raft peer ALPN `/1` identifiers | Storage RPC is blocked on Phase 1 containment; the other two require evidence consolidation |

The evidence audit proceeds in this bounded order after Phase 1 containment is complete:

1. Confirm storage-node TLS, topology, physical payload, maintenance workflow, control-plane admin,
   and implementation-error containment are complete and boundary-checked.
2. Expand each `storage` family above to one line per independently changeable format, recording
   its defining constant, writer, first rejecting reader, exact-current fixture, and unsupported
   version fixtures.
3. Do the same for the `server-core`, `argmin-s3`, and `auth` rows, without exposing private
   constants or codecs to cross-crate tests.
4. Decide the tag-XML and ACL-string strategy. If their containing formats are the version
   boundary, record that as a deliberate invariant and require every incompatible canonical-codec
   change to advance every listed container. Otherwise introduce a private framed carrier and
   advance the current containing formats in the same slice.
5. Implement only the gaps proven by the matrix, one owner and one coherent format family at a
   time, updating the row to `Recorded` with its permanent test evidence.

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
durable codecs, static route authority, and session-token ownership work are complete and
boundary-checked. The completed work does not imply semantic topology containment: storage-node
TLS-profile construction and the topology/maintenance audit above remain Phase 1 work.

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
9. **Pending:** move storage-node TLS 1.3 and ALPN profile construction out of `argmin-s3` and
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
11. **Pending:** replace server-core's data-PG, placement-epoch, EC-placement, shard-location, and
    historical-route handling with opaque storage-owned payload handles and logical I/O/lease
    operations.
12. **Pending:** move PG topology transitions and live metadata-transfer orchestration behind a
    storage-owned control-plane/admin facade; move static storage-topology interpretation behind a
    storage-owned builder without transferring outer manifest ownership.
13. **Pending:** replace cross-crate `StoreError` variant matching with exhaustive semantic
    classifications and opaque diagnostics owned by storage.
14. **Pending:** contain local debug PG operations behind owner-provided opaque diagnostics, move
    impossible topology/route/claim fixtures into storage, and enforce the final boundary through
    crate visibility plus `check-storage-cluster-boundaries` coverage.

After items 9 through 14 are complete, work proceeds through the Phase 2 evidence gate rather than
reopening containment opportunistically.
