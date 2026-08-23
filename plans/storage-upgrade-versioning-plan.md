# Storage Upgrade And Mixed-Version Compatibility Plan

Status: design and implementation not started. Current binaries still accept only current
internal formats and homogeneous protocol versions.

## Related Documents

- [`guides/versioning.md`](../guides/versioning.md) defines the rules for changing and evidencing
  current formats.
- [`storage-format-baseline-and-containment-plan.md`](completed/storage-format-baseline-and-containment-plan.md)
  is the completed ownership, containment, format inventory, and exact-current evidence record.
- [`multihost-transition-plan.md`](completed/multihost-transition-plan.md#phase-12-replicated-control-plane)
  records the completed Phase 12 replicated-control-plane work that the old plan referenced
  without a link.
- [`storage-topology-resize-plan.md`](storage-topology-resize-plan.md) owns live PG-set and
  placement-topology changes. A rolling software upgrade must interlock with it but must not
  implement resize implicitly.
- [`versioned-physical-shard-files-option.md`](versioned-physical-shard-files-option.md) records an
  unaccepted alternative physical-shard design. This plan must not assume that option was chosen.

## Goal

Add deliberate upgrade support without weakening the exact-current format boundaries already in
place. The implementation must support each representation according to its actual lifecycle:

- local durable state is inspected and migrated under exclusive startup ownership before serving;
- internal RPC peers negotiate an interoperable protocol before exchanging requests;
- Raft upgrades preserve quorum, log, snapshot, and feature-activation safety across mixed
  binaries;
- nested encodings advance with an explicit containing-format dependency ledger;
- immutable object payloads use a declared read/write/rewrite policy rather than startup database
  migration; and
- operator configuration and durable identity changes are explicit and diagnosable.

The end result is not a general fallback reader. It is a bounded, tested compatibility window
with named source and destination releases, explicit lifecycle states, and a removal policy.

## Current Policy Until This Plan Starts

No upgrade or mixed-version behavior is supported today. Until Phase 0 below is approved and an
individual compatibility path is implemented:

- current writers emit only the current representation;
- current readers reject missing, older, and newer versions;
- a cluster must use one compatible binary set;
- a fresh deployment must be initialized by the current binary; and
- old-version fixtures are immutable evidence, not production compatibility support.

Do not add a migration, alternate parser, version range, compatibility default, or negotiated
fallback merely because it appears in this plan. Each becomes supported only when its own phase,
tests, operator contract, and removal rule are complete.

## Non-Goals

This plan does not version the public S3 protocol, promise Rust API stability, select a new
physical-shard layout, implement topology resize, or create a general backup system. It also does
not require every format to use one global version or one migration mechanism. AWS-visible behavior
must remain unchanged throughout internal upgrades, except for separately tested conformance fixes.

## Compatibility Vocabulary

The implementation must keep these concepts distinct:

- **Release version:** the operator-visible software release.
- **Format family and version:** one owner-controlled durable, wire, nested, digest, or identity
  representation.
- **Reader set:** versions a binary can validate and consume.
- **Writer version:** the single version a binary emits for a particular negotiated or activated
  mode.
- **Protocol version:** the selected wire grammar for one connection.
- **Capability:** an independently activatable semantic feature. A capability is not inferred
  from a protocol number unless the protocol definition makes that implication explicit.
- **Upgrade transition:** a supported directed edge from one complete release-format vector to
  another.
- **Activation:** the durable point after which a new writer format or semantic feature may be
  used.
- **Rollback:** returning to the previous binary and format vector. Restarting an old binary is
  not rollback if newer durable state has already been committed.

Compatibility is defined over a complete version vector, not one integer. A release may contain
independent SQL, checkpoint, RPC, authentication, Raft, nested-codec, identity, and payload
versions.

## Initial Policy Decisions

Phase 0 must turn the following proposed defaults into explicit accepted decisions before code is
written:

1. Support one adjacent release transition, `N-1 -> N`, initially. Do not promise arbitrary skips.
2. Implement one-way offline/startup migration first. After its commit point, rollback requires a
   pre-upgrade backup or an explicit reverse migration; no reverse migration is initially planned.
3. Add rolling interoperability only after offline migration and its crash harness are trusted.
4. During a rolling upgrade, new binaries read both sides of the supported window but continue to
   write the old durable/wire form until cluster-wide activation permits the new writer.
5. Permit no topology resize, membership replacement, credential-policy redesign, or unrelated
   repair while an upgrade transition is active.
6. Treat an unknown older version, a skipped transition, and every newer version as unsupported and
   fail closed.
7. Retain compatibility code for a documented bounded window, with its deletion requiring proof
   that no supported deployment can still contain the old representation.

If any of these defaults is rejected, update this plan before implementation. In particular,
offline-only upgrades and rolling upgrades have materially different storage, protocol, and
rollback requirements and must not be conflated.

## Format-Class Strategy

| Format class | Upgrade mechanism | Compatibility boundary |
| --- | --- | --- |
| Per-PG SQLite schema and catalogue | Explicit ordered startup migrations under the native state lock; transactional where SQLite permits | No PG opens or serves until its catalogue, rows, triggers, and schema marker verify at the destination version |
| Mutable identity, checkpoint, restart, sentinel, journal, and WAL artifacts | Owner-classified startup conversion using write-new, verify, sync, and atomic publication; coupled artifacts use one progress/commit generation | No replay, repair, authority construction, listener publication, or serving before the complete artifact set is current |
| Append-only command/log records | Either retain a bounded old-record decoder during the support window or require verified snapshot/compaction before activation | The choice is per owner and recorded explicitly; normal crash recovery must not guess |
| Storage, control-plane, and Raft RPC | Authenticated version/capability negotiation, then one selected encoder/decoder pair per connection | No application request bytes or mutation dispatch before negotiation and authentication succeed |
| TLS/ALPN and Unix transport prefaces | A common protocol-family negotiation design with transport-specific carriage | No heuristic retry with a different grammar after application bytes may have been sent |
| Nested metadata, tags, ACLs, encryption, checksums, and proof carriers | Independent inner migration when self-describing; otherwise migrate every container named by the dependency ledger | Old inner bytes are accepted only inside an explicitly supported containing transition |
| Canonical digests, hash chains, and cryptographic domains | Versioned recomputation with old and new inputs retained as evidence; authenticate the selected algorithm/version | Never compare values produced by different domains as though they were the same proof |
| Immutable object payload/shard representations | Read-old/write-new plus verified background rewrite, or an explicit maintenance outage | Startup must not scan and rewrite unbounded object data; metadata records the authoritative representation |
| Operator manifest/configuration | Explicit schema conversion or an operator-visible converter; never silent defaulting | Invalid or unsupported configuration fails before filesystem, network, or durable-state mutation |
| Static storage/control-plane identity | Convert only after all bound state is validated and migrated; publish atomically last | Identity must certify the exact post-upgrade state and cannot be used to bless unknown artifacts |

## Shared Upgrade Architecture

### Owner-local compatibility registry

Each format owner defines, privately:

- the current writer version;
- the exact set of supported source versions;
- directed migration or protocol-compatibility edges;
- containing-format dependencies;
- the first rejecting reader;
- the activation and rollback boundary; and
- immutable evidence for every supported edge.

Higher layers receive an opaque compatibility profile and semantic outcomes. They must not inspect
SQL, wire tags, filenames, record variants, or raw version constants. A storage-wide aggregate may
report whether a release vector is `Current`, `UpgradeRequired`, `UpgradeInProgress`,
`UnsupportedOlder`, `UnsupportedNewer`, or `Invalid`, but classification remains owner-defined.

The completed baseline inventory is the starting vector. When a format advances, the active plan
records the new supported edge and links its operator guide; the archived baseline is not edited to
pretend the new version existed historically.

### Prepared startup and migration capability

Startup should use an owner-issued typestate rather than allowing the process layer to compose
migrations:

1. acquire and bind the state-directory/native lock;
2. inspect every owned format without mutation;
3. return a complete semantic upgrade plan or a typed rejection;
4. require an explicit configured/operator authorization to migrate;
5. execute owner-local steps while listeners and maintenance remain absent;
6. verify logical invariants and exact destination representations;
7. atomically publish the final version/identity commit point; and
8. return a current-state capability from which normal serving can start.

A caller cannot skip inspection, run one physical step directly, or construct the ready
capability. If one PG or artifact fails, the process remains non-serving. Already completed steps
must be safe to recognize and resume after restart.

### Durable progress and crash semantics

Every non-transactional transition needs an owner-controlled progress record containing at least:

- transition identity and source/destination complete version vectors;
- stable state/cluster/process identity binding;
- current step and enough output identity to distinguish resume from foreign state;
- integrity protection appropriate to the surrounding artifacts; and
- an explicit commit point.

The state machine is `Inspected -> Prepared -> Applying -> Verifying -> Committed -> Ready`.
Crashes before `Committed` resume or fail closed according to the recorded step; crashes after it
must open only the destination representation. Cleanup of old files happens after commitment and
must not be required for correctness. Recovery code cannot infer upgrade progress from temporary
filenames or partially matching state.

## Workstream A: SQLite And PG State

This is the first implementation target because startup migration is bounded and does not require
mixed-version networking.

For every supported `vN -> vN+1` transition:

1. Keep an immutable complete vN SQLite catalogue and golden database built by the vN production
   writer at its historical commit boundary.
2. Under the native storage lock and before `PgStore` construction, verify `user_version`, the
   complete catalogue, durable identity, row invariants, and cached/materialized digests.
3. Run one explicit migration function. Use `BEGIN IMMEDIATE` and update `user_version` in the same
   transaction as all transactional schema/row changes.
4. Recreate changed indexes and triggers from owner definitions. For digest triggers, compare the
   owner-defined canonical catalogue SQL or a stored generation/hash over that definition, rebuild
   the materialized digest, and verify it; checking trigger names alone is insufficient.
5. Validate the complete destination catalogue and every changed row invariant before commit.
6. Reopen through the ordinary current-version reader and compare its logical state with an
   equivalent fresh current store.

Do not use `CREATE IF NOT EXISTS`, nullable compatibility columns, read-time row repair, or normal
crash cleanup as migration. Those patterns hide an unknown source state instead of proving a
supported transition.

Multi-PG startup must inspect all configured PGs first. Migration may then proceed PG by PG with a
durable aggregate plan, but the process cannot serve a subset. A crash after some PGs commit must
resume the remaining exact transition; it must not rerun a committed migration or accept a mixture
as ready.

Required tests include golden vN databases, every intermediate crash point, wrong catalogue under
a claimed version, stale trigger bodies, failed row conversions, multi-PG partial completion, disk
exhaustion, restart/resume, and differential logical state against a fresh vN+1 database.

## Workstream B: Durable Files, Journals, WALs, And Checkpoints

Each artifact family needs an explicit dependency graph and publication order. Identity,
initialization markers, clock checkpoints, restart artifacts, sentinels, journals, WALs, and outer
static identities cannot be migrated independently when one authenticates or certifies another.

The owner must choose one of two strategies per artifact:

- **Convert:** validate the complete old artifact, write a new artifact to a newly created file,
  sync it, read and verify it through the destination reader, atomically publish it, then sync the
  parent directory.
- **Regenerate:** derive the destination artifact from an authoritative current logical state,
  proving that the source can be discarded. Regeneration is not permission to ignore a malformed
  old artifact or missing acknowledged durability.

For journals and WALs, record whether old entries remain readable during the window or whether an
upgrade precondition forces a checkpoint and compaction to a point after every old record. If old
entries remain readable, the decoder is scoped to the supported source version and removed with
that compatibility edge; it is not an open-ended legacy parser.

Paired artifacts use a common generation/progress identity so a crash cannot combine one old file
with one new sidecar. Tests cover every publication boundary, torn new output, old/new pair
crossing, fsync and rename failure, relocation, insufficient space, and unknown source state.

## Workstream C: RPC, Transport, And Authentication Interoperability

Rolling upgrades require negotiation, not startup migration. Storage RPC, control-plane RPC, and
Raft peer RPC each retain independent application versions, but use one common negotiation model.

The transport design must decide whether TLS uses an ordered ALPN list of protocol major versions
or a stable family ALPN followed by an authenticated hello. Unix transport needs the equivalent
bounded preface. Whichever design is chosen:

1. peers exchange release compatibility ranges, application protocol versions, and capability
   sets before application framing;
2. selection is deterministic and chooses one exact protocol version;
3. the authentication transcript binds both offers, the selected version/capabilities, direction,
   endpoint identities, and transport context to prevent downgrade or cross-protocol reuse;
4. both peers instantiate only the selected encoder/decoder;
5. requests and responses use that version for the connection lifetime; and
6. no mutation is dispatched, reserved, or published before negotiation and authentication finish.

A valid authenticated remote semantic rejection remains definite. Any failure after request
publication without a valid authenticated operation result preserves the existing potentially
applied/confirmation semantics. A client must never retry another protocol grammar after bytes may
have reached the server.

Protocol versions and semantic capabilities are separate. A new binary can advertise decode
support before advertising permission to use a new command. Writers continue using the oldest
activated representation required by the connected supported peers. Unknown message kinds or
capabilities are rejected; they are not skipped unless the selected protocol explicitly defines a
length-delimited ignorable extension rule.

Required tests cover old client/new server and new client/old server in both directions, no common
version, downgrade tampering, authenticated offer/selection mismatch, capability asymmetry,
read-only and mutating calls, response loss and malformed responses after publication, deadlines,
failover, and exact wire fixtures for every negotiated pair. Tests use independently built release
artifacts or frozen protocol peers, not two copies of the current codec parameterized by a number.

## Workstream D: Raft And Cluster-Wide Activation

Raft compatibility combines peer RPC, persisted log entries, snapshots, restart state, membership,
and leader-emitted commands. It cannot be treated as ordinary request RPC.

Before a rolling Raft upgrade:

- the cluster is healthy with stable membership and no topology resize;
- every peer can negotiate the supported peer protocol;
- the authority records each member's release/protocol/capability profile;
- a current checkpoint and snapshot satisfy the old-log handling policy; and
- the upgrade coordinator proves quorum remains available at every operator step.

Upgrade learners and followers before the leader unless the accepted design proves another order.
Until all voting members and required learners advertise a capability, leaders emit only the old
command/log/snapshot form. Activation is a replicated control-plane command with a monotonic epoch;
process-local observation is insufficient.

The activation record and barrier are themselves part of the common pre-activation grammar. Both
releases must decode and apply the record using the old command, log-entry, RPC, canonical-state,
snapshot, WAL, and restart representations. The record identifies the transition, activation
epoch, and exact newly permitted writer/capability vector. It remains present in canonical state,
snapshots, and restart artifacts until every format it gates is outside the support window. An old
binary that observes an activated vector it cannot write or consume enters a typed incompatible
activation state before campaigning, serving, or mutating; it must not ignore the record.

The leader may switch encoders only after the activation entry is durably committed and applied by
its local state machine. Raft log ordering then makes every follower apply the barrier before any
new-format entry. A leadership change at that boundary derives writer selection solely from the
applied replicated activation state, never from process-local intent. This requires a preparatory
release that already understands the generic activation grammar before that grammar can gate the
first incompatible rolling transition.

The Raft workstream must decide explicitly:

- whether the bounded window retains old command/log decoders or requires compaction before
  activation;
- which restart, WAL, snapshot, and peer versions can coexist;
- how snapshot installation transfers the activated feature/version state;
- how a replacing or long-offline node proves compatibility before joining;
- whether rollback is allowed before activation only; and
- how an interrupted leader upgrade is resumed by a new leader.

Tests require real mixed-version multi-process clusters, leadership movement at every stage,
quorum loss boundaries, old/new snapshot transfer, log replay spanning activation, restart of each
binary version, response-publication ambiguity, and rejection of topology or membership changes
during the transition. Deterministic crash/restart cases cover activation before append, after
append but before commit, after commit but before local apply, after apply but before the first new
write, and after the first new-format entry. They prove snapshots, checkpoints, WAL/restart state,
and a replacement leader retain the same barrier, that no new-format entry precedes it, and that an
old binary fails closed after observing it.

## Workstream E: Nested Codecs, Digests, And Encryption Profiles

For a self-describing nested format, add a bounded old-version decoder only as part of a named
transition and convert it to the current logical type at the owner boundary. Writers emit one
activated version. For an untagged nested format, every containing version in the dependency
ledger advances or migrates atomically.

Use an expand/migrate/contract sequence when mixed binaries can encounter the value:

1. **Expand:** new readers understand the old and new supported forms, but all writers still emit
   old.
2. **Migrate/activate:** rewrite durable values or activate new writers only after every possible
   reader is expanded.
3. **Contract:** remove old writers first; remove old readers only after scans prove no old value
   and the support window expires.

Digest and hash-domain changes carry explicit algorithm/domain versions. Recompute from validated
logical state; do not translate one digest value into another. Proof comparisons reject differing
carrier versions until the containing transition explicitly validates and converts them.

Encryption-profile migration must define whether data is rewrapped or re-encrypted, how keys are
resolved, and how authenticity is verified before replacement. Never report a migrated checksum or
encryption state until the destination ciphertext has been opened through the production reader.

## Workstream F: Immutable Object Payload And Shard Formats

Unbounded payload data is not rewritten synchronously at process startup. Before introducing a
payload format version, decide whether it is recorded in object metadata, in each shard header, or
both, and update the separate physical-shard design if generation-addressed files are adopted.

The default migration model is:

- expanded readers can read the supported old payload format;
- new writes use the activated current format;
- scrub/repair owns a durable, throttled rewrite queue;
- each rewrite writes new bytes, verifies checksum/EC/decryption semantics, atomically publishes
  the new authoritative metadata, and only then schedules old payload reclaim; and
- completion is proved by an owner scan before old-reader removal.

The rewrite capability is bound to exact bucket/key/version/generation and route authority. It must
preserve active-read leases, Object Lock/retention semantics, checksums, encryption, multipart
manifests, and reclaim safety. Tests include crash at every write/publish/reclaim boundary, mixed
old/new segments, repair during migration, relocation, lost shards, and logical byte equivalence.

## Workstream G: Configuration And Durable Identities

Operator manifest changes use an explicit schema transition. Prefer a standalone validation/
conversion command that writes a new file for operator review; startup must not silently infer
missing fields or rewrite operator input. The command reports source/destination schema and the
resulting topology, process-identity, and full-config fingerprints.

Durable storage and control-plane identities are migrated only after all state they bind is at the
destination vector. The owner retains the validated old descriptor/bytes, writes and verifies the
new identity, and publishes it last. A new identity cannot make an unknown or partially migrated
database, journal, checkpoint, or restart artifact acceptable.

Tests cover old/new manifest schemas, explicit conversion output, unknown fields, missing secrets,
identity integrity, pathname/inode replacement, in-place mutation, interruption before and after
identity publication, and proof that listeners and state mutation remain absent on failure.

## Verification Framework

Before the first migration is implemented, add reusable owner-local and process-level harnesses:

- immutable source artifacts produced and committed at the historical writer boundary;
- a release-format-vector manifest tying each fixture to its owning versions;
- migration runners with deterministic fault injection at every state transition and filesystem/
  database durability operation;
- reopen loops proving idempotent resume or fail-closed behavior;
- logical differential comparison with a fresh destination store;
- mixed-version process launchers built from distinct commits or frozen peer binaries;
- protocol transcript capture with secret redaction and exact fixture comparison;
- negative unsupported-too-old, skipped-version, and too-new cases;
- mutation/publication sentinels proving rejection precedence; and
- boundary checks proving migration codecs, SQL, raw frames, and implementation diagnostics remain
  inside their owners.

The normal full suite remains required. Upgrade tests are additional gates, not replacements for
AWS compatibility, storage correctness, crash recovery, clippy, formatting, or boundary checks.

## Operator And Release Contract

Every supported transition ships an operator guide containing:

- exact supported source and destination releases and complete format vectors;
- required backup and free-space checks;
- whether outage or rolling operation is supported;
- node/process order, health gates, and commands;
- feature-activation and point-of-no-return markers;
- expected duration and background-rewrite behavior;
- restart/resume instructions for each failure state;
- rollback possibilities before and after activation;
- interactions prohibited during upgrade, including resize and membership changes; and
- how to verify completion and when old compatibility code may be removed.

Startup must never automatically perform a destructive or irreversible migration merely because it
encounters a supported old version. The operator/configuration contract must explicitly authorize
it, and diagnostics must state the planned transition without leaking owner-private representation
details.

## Implementation Order

### Phase 0: Approve the compatibility contract

- Accept or revise the initial policy decisions above.
- Choose the first real source/destination release pair and freeze both complete version vectors.
- Define backup, rollback, support-window, and compatibility-code removal policy.
- Decide whether the first deliverable is explicitly offline-only.

Exit: one written transition contract exists; no compatibility code has been added.

### Phase 1: Build the common evidence and crash harness

- Add the owner-local compatibility registries and opaque aggregate profile.
- Add historical-artifact provenance checks and the migration fault-injection runner.
- Add process-level no-serving/no-listener assertions and release-vector diagnostics.

Exit: a no-op current-to-current plan exercises the complete lifecycle without changing bytes.

### Phase 2: Implement the first SQLite migration

- Select one real PG schema transition.
- Implement inspect, transactional migrate, trigger/body verification, destination validation,
  multi-PG resume, and operator authorization.
- Publish the first version-specific operator guide.

Exit: every crash point resumes safely, logical differential tests pass, and unsupported versions
remain unchanged and non-serving.

### Phase 3: Migrate coupled durable artifacts

- Add owner-directed transitions for identities, checkpoints, journals, WALs, restart artifacts,
  and sentinels required by the selected release edge.
- Resolve old-record replay versus pre-upgrade compaction per family.

Exit: the complete process state, not only SQLite, crosses the release vector atomically and opens
through ordinary destination readers.

### Phase 4: Add negotiated storage and control-plane RPC

- Select the common TLS/Unix negotiation design and bind it into authentication.
- Implement adjacent-version storage and control-plane RPC in both directions.
- Preserve mutation ambiguity and failover semantics.

Exit: a mixed frontend/storage/control-plane deployment serves the existing S3 suite without
activating a new-only feature, and incompatible peers fail before requests.

### Phase 5: Add Raft rolling upgrade and activation

- Implement peer interoperability, replicated capability tracking, upgrade ordering, snapshot/log
  policy, and monotonic feature activation.
- Interlock with topology resize and membership changes.

Exit: mixed-version process tests retain quorum through leadership changes and safely reach the
new writer vector; the common-grammar activation record is committed and applied before the first
new-format entry, survives snapshot/restart, and prevents old nodes from campaigning, serving, or
mutating after activation.

### Phase 6: Add nested and payload transitions as needed

- Apply expand/migrate/contract to independently versioned nested values.
- Add durable background migration for any object/shard format change.

Exit: owner scans prove no supported state requires the retired reader before it is removed.

### Phase 7: Productionize and bound support

- Complete operator tooling, metrics, diagnostics, soak/fault testing, and release documentation.
- Exercise backup restore and every permitted rollback point.
- Record the earliest release at which each old reader may be deleted.

Exit: the supported transition can be executed and recovered by following only the published
operator guide, and the compatibility window has an enforceable end.

## Plan Completion Criteria

This plan is complete only when at least one real release transition supports both the declared
offline/startup migration model and rolling mixed-version RPC/Raft upgrade model end to end, every
involved format family follows its strategy above, crash and mixed-version tests pass from
immutable historical artifacts/binaries, operator rollback limits are accurate, and no
compatibility path exists outside an owner registry and named support window. If rolling support is
later removed from this scope, it must first move to a separately active successor plan with its
own completion gate; completing only the offline transition cannot close this plan as written.
