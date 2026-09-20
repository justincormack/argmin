# Multihost Production Follow-up Plan

Status: active

Started: 2026-08-05

Related documents:

- [completed multihost transition plan](completed/multihost-transition-plan.md)
- [static cluster configuration plan](completed/static-cluster-configuration-plan.md)
- [distributed correctness review, July 2026](completed/distributed-correctness-review-2026-07.md)
- [distributed correctness confidence review, July 2026](completed/distributed-correctness-confidence-review-2026-07.md)
- [control-plane authentication and identity plan](control-plane-auth-identity-plan.md)
- [storage boundary compiler-enforcement plan](completed/storage-boundary-compiler-enforcement-plan.md)
- [immutable placement capacity weights](immutable-placement-capacity-weights.md)
- [production backpressure plan](production-backpressure-plan.md)
- [DeleteBucket reservation classification](delete-bucket-reservation-classification-plan.md)
- [versioned physical shard files option](versioned-physical-shard-files-option.md)

## Purpose

The completed multihost transition delivered the first production-shaped
static replicated deployment: node-aware EC placement, replicated metadata,
durable recovery and migration, authenticated Unix and TLS/TCP storage RPC,
three-node OpenRaft control plane, static cluster manifests, multiple
frontends, degraded EC reads, and a quantitative three-host outage/rejoin gate.

That plan also accumulated years of implementation history. This follow-up
contains only work that remains useful after the initial static multihost
milestone. It owns:

- residual distributed correctness boundaries that are still explicitly open;
- convergence and operational closeout exposed by long-running soaks;
- dynamic topology, node replacement, and storage expansion;
- replicated-mode production graduation and operator workflows; and
- independent evidence beyond implementation-shaped unit and soak tests.

Historical findings are not automatically current defects. Before implementing
an item inherited from either July review, revalidate it against the current
code and record the exact remaining mechanism. Closed clock, WAL, route-drain,
LIST, authentication, compact-history, and snapshot-executor findings must not
be reopened under their old descriptions.

## Completed Baseline

The following are baseline assumptions, not work for this plan:

- standalone uses `all-in-one`; replicated deployments use separate
  `frontend`, `storage-node`, and `control-plane` processes;
- replicated internal RPC is authenticated over Unix and TLS/TCP, with
  process-role authorization followed by server-local route capabilities;
- replicated control-plane state uses OpenRaft with fsync-before-ack WAL
  durability, bounded checkpoint/compaction, poison gating, linearized serving
  reads, and explicit authority-clock recovery;
- static topology and process identity are bound to durable state and peer
  traffic through the version-1 cluster manifest;
- metadata commands, pending-command recovery, retained routes, shard repair,
  payload backfill, and metadata-PG transfer have durable retry paths;
- frontends can start while PGs are Peering, and certified clean Peering reads
  plus EC reconstruction preserve available GET, HEAD, and LIST operations;
- the composed quantitative gate passed on three persistent-storage hosts with
  116 PGs, 256 retained route advances, three whole-host outage/rejoin cycles,
  multiple authenticated frontends, and bounded WAL/checkpoint I/O; and
- the storage capability plan's Phase 4 publisher typing is complete. Later
  capability phases are complementary hardening, not a reason to weaken or
  duplicate the multihost boundaries here.

## Scope And Principles

1. AWS S3 behavior remains the external contract. Distributed failure handling
   must not invent easier client-visible semantics.
2. A successful mutation response requires the declared durable evidence.
   Ambiguous outcomes are confirmed, never blindly replayed.
3. Route, epoch, subject, and operation authority must remain coupled to the
   storage effect. Historical recovery authority is narrower than serving
   authority.
4. Corruption or ambiguity is contained as narrowly as possible and remains
   operator-visible. Fail-closed must not become an avoidable whole-cluster
   outage.
5. Static configuration describes initial identity and topology. It cannot
   activate a dynamic topology change or silently recreate a lost node.
6. Colocation does not collapse trust or resource boundaries. Replicated
   frontend and storage roles remain separate processes even on one host.
7. Every stochastic failure found by a soak should acquire a deterministic
   regression at the narrowest production boundary that reproduces it.

Specialized plans continue to own their local implementation details. This
plan owns the multihost invariant and release gate that consumes them:

- capacity weights remain in the immutable placement-weight plan;
- compiler-enforced storage capabilities continue in their own phased plan;
- overload policy and quantitative admission behavior remain in the production
  backpressure plan;
- reservation classification is an optional optimization after durable bucket
  deletion converges; and
- generation-addressed physical shard files remain an evaluated layout option,
  not a prerequisite for physical-identity fencing.

## Phase 0: Fault Model And Executable Safety Case

Write one production fault model before adding dynamic replacement. It must
state:

- crash-stop and crash-recovery assumptions, including whether disk or VM
  rollback is supported;
- network loss, duplication, reordering, asymmetric partition, and unbounded
  delay behavior;
- wall-clock skew/step limits, monotonic-clock behavior across suspend, and the
  operational response when the bound is violated;
- rename, file/directory sync, torn-write, ENOSPC, EIO, and acknowledged-write
  assumptions for supported filesystems;
- corruption detection and repair boundaries for control metadata, PG metadata,
  and shard payloads;
- backup/restore rules for individual nodes and the cluster; and
- the authenticated non-Byzantine peer assumption, including stale but valid
  credentials and state.

Maintain an executable invariant register. Each invariant records its owner,
linearization point, durable evidence, fence, recovery action, metric, and
tests. At minimum:

| ID | Invariant | Current follow-up |
|---|---|---|
| M1 | A serving map descends from the current cluster identity and a linearized authority read. | Keep typed serving/diagnostic boundaries and refresh tests. |
| M2 | A PG mutation commits only while its exact route permit remains current. | Keep frame drain and commit-coupled route tests. |
| M3 | A successor cannot serve while an old primary or admitted old operation can commit. | Keep lease/skew and route-publication barriers. |
| M4 | A success response implies required replicas/durable logs contain the operation, or retry can classify the outcome. | Complete typed ambiguity audit. |
| M5 | Active state after migration descends from the last acknowledged metadata proof. | Strengthen and model proof ancestry. |
| M6 | Reclaim cannot delete a reachable or in-flight physical shard through an epoch alias. | Implement physical-identity fencing. |
| M7 | One damaged or ambiguous PG cannot silently affect unrelated PGs. | Add per-PG containment and repair state. |
| M8 | Raft responses imply vote/log durability and snapshots cannot mix generations. | Retain crash matrix and OpenRaft conformance. |
| M9 | Dynamic membership/topology changes are exact, authorized committed transitions. | Implement certified topology lifecycle. |
| M10 | Externally visible S3 histories satisfy the AWS contract under retries and failure. | Add independent history checking. |

Add a drift check requiring protocol changes to name affected invariants and
their tests. The register should live close enough to code that it is updated
with command, proof, lease, and durability changes rather than during a later
review.

### Phase 0 Exit Criteria

1. The supported fault model is documented and referenced by deployment docs.
2. Every invariant above names concrete implementation owners and tests.
3. Release gates distinguish unsupported rollback/corruption from supported
   recovery rather than silently assuming either.

## Phase 1: Residual Distributed Correctness

### 1.1 Fence Physical Shard Identity Across Epochs

Close the still-open `RPC4` mechanism from the July reviews and
`guides/storage-cluster-invariants.md`: epoch-bearing `ShardLocation` values can
name the same physical shard file. Read handles and delete fences must therefore
key the physical object independently of routing epoch.

- define one canonical physical shard identity, at least `(data_pg_id,
  shard_key, shard_index/node where required)`;
- acquire all-or-release read protection for every physical shard candidate
  used by EC reconstruction;
- require reclaim, repair replacement, backfill cleanup, and scavenging to
  consult the same identity fence;
- retain route/epoch validation for authority, but do not use it as the file
  identity; and
- test old/new routes that alias one file, retained reads concurrent with
  reclaim, repair replacement, and process restart.

Implement the fence independently of whether physical files remain stable or
later become generation-addressed. If the versioned-file option is adopted, it
must preserve the same read-handle, publication, scavenging, and reclaim
invariants rather than replacing them with implicit filename assumptions.

### 1.2 Contain Recovery Failure Per PG

Revalidate the July `INT-2` concern against current pending-command recovery.
Identity and database-integrity failures remain fatal where safe isolation is
impossible, but a recoverable or quarantinable PG failure must not prevent
unrelated PGs on the storage node from serving and participating in repair.

- classify startup failures as process-fatal, PG-quarantinable, or automatic
  recovery;
- retain exact conflicting epoch/index/hash/digest evidence;
- publish the affected PG as non-serving without replacing cluster-wide route
  state with node-local payload health;
- provide a bounded repair/rejoin path and explicit operator status; and
- cover future-epoch pending evidence, corrupt metadata, missing payload,
  restart, and healthy-PG continuity.

An overnight 200-PG migration/failover soak at `71ed13d4` exposed one
containment gap at this boundary: a restarted primary reported a durable pending
command together with a lower-index later-epoch proof. The authority rejected
the whole heartbeat while the PG was still Active, allowing the node lease to
expire and moving all 200 PGs to Peering. Pending-command validation now retains
its exact historical-primary check but moves only the affected PG to Peering;
the node heartbeat and unrelated Active PGs remain live while replica
convergence validates the reset proof.

### 1.3 Make Peering Ancestry Explicit

Current exact proofs, transfer provenance, retained logs, and checkpoint
artifacts are substantially stronger than the July `CP6`/`CP7`/`CL6` baseline.
Audit the remaining cross-epoch acceptance paths and represent the result as a
typed `PeeringActivationProof` or equivalent that proves:

- source epoch, acting set, and authoritative proof identity;
- exact retained-log or checkpoint lineage to the destination proof;
- handling of abandoned/tombstoned commands;
- destination replica agreement and absence of unresolved pending commands;
- the fence epoch and transfer authorization used for any lower-index reset;
  and
- rejection of same-index forks, missing suffixes, stale certificates, and
  ordinary acting-set changes that bypass required transfer proof.

Wire retained-log catch-up and checkpoint-backed transfer only through that
constructor. Add generated schedules for partial replay, restart, source loss,
retry after import, and proof-floor reset.

### 1.4 Complete Typed Outcome Classification

Audit every mutating control-plane and storage RPC for three states:

- `ConfirmedApplied`;
- `ConfirmedNotApplied`; and
- `OutcomeUnknown`.

Transport connect/write/read errors must preserve whether dispatch could have
occurred. Only idempotent or explicitly confirmed-not-applied operations may be
automatically resubmitted. Checked clients must retain the exact target route,
request transcript, and signer across confirmation. Add compile-time or test
guardrails preventing a new ambiguous error from entering a generic retryable
class.

### Phase 1 Exit Criteria

1. Epoch aliases cannot evade physical read/delete fencing.
2. Supported PG-local damage does not disable unrelated PGs.
3. Every Active migration proof has explicit, testable ancestry.
4. No non-idempotent mutation retries an unknown outcome blindly.

## Phase 2: Cleanup And Background Convergence

### 2.1 Close DeleteBucket Attempt Convergence

Carry forward only the incomplete Phase 11 cleanup work:

- finish whole-operation retry boundaries for authorization plus begin-delete;
- make same-generation durable attempt state visible early enough that
  foreground requests and the background worker adopt rather than compete;
- prove progress through reservation wait, post-reservation object-PG drain,
  stream cleanup, final visibility, and `MarkBucketDeleting`;
- reconcile volatile begin/finalizer queues with durable rows after refresh and
  restart;
- add deterministic gates for refresh during begin, restart after drain,
  stale frontend after bucket recreation, and finalizer claim recovery; and
- close with repeated cleanup stress, route-change restart, and failover runs
  showing no HTTP 500, unexplained retry growth, or stale queue state.

Reservation classification optimization remains secondary. Do not weaken the
empty-bucket proof merely to shorten a retry.

### 2.2 Finish Background Admission Policy

- keep one process-local scheduler over repair, reclaim, lifecycle, stream
  cleanup, backfill, metadata checkpoint, and opportunistic scan work;
- preserve durable rows as truth and use volatile queues only as wake hints;
- retain urgent EC-risk work ahead of routine convergence;
- tune foreground-pressure thresholds and routine scan cadence from measured
  persistent-storage runs;
- ensure no worker performs hidden full-cluster scans outside the scheduler;
  and
- expose bounded queue, claim age, admission denial, retry, and completion
  metrics per work class.

### 2.3 Tune Metadata Checkpoint And History Retention

The bounded catalogue, checkpoint-plus-suffix transfer, compaction, and
storage-owned history floors are implemented. Remaining work is policy:

- measure retained checkpoint/log size across sustained migration and repair;
- define whether any extra historical candidates are retained for diagnostics
  or operator rollback, without promising unsupported rollback semantics;
- retain enough history for every durable transfer, backfill, payload, and
  pending-recovery reference; and
- prove references release and old history becomes reclaimable.

### Phase 2 Exit Criteria

1. Durable cleanup attempts converge independently of one HTTP request.
2. Background work remains bounded and makes progress under quiet capacity.
3. Checkpoint/log/history storage reaches a measured steady-state bound.

## Phase 3: Dynamic Topology, Replacement, And Expansion

Static manifests remain the initial identity and policy input. Runtime topology
changes require committed control-plane state and cannot activate through local
file edits.

### 3.1 Committed Topology Policy

- replicate active and prepared topology generation, canonical manifest digest,
  failure-domain policy, tolerance, node identities, and endpoint identities;
- use `PrepareTopologyPolicy`, authenticated per-node acknowledgements, and one
  committed `ActivateTopologyPolicy` transition;
- fence nodes whose local installed topology does not match the active or
  explicitly prepared digest; and
- preserve the old failure guarantee until every acting set and voter set
  satisfies the proposed one.

### 3.2 Certified Raft Membership Change

Serialize membership changes with topology activation. Before invoking native
OpenRaft membership change, commit an authorization bound to:

- topology generation and digest;
- exact source membership log ID and digest;
- exact target membership and ordered joint/final payload digests; and
- one unique transition identity.

Every native membership entry must consume the next authorized stage. Persist
in-progress state and a completed transition certificate through snapshots and
compaction. Unauthorized or mismatched committed membership is a fatal
replicated-state invariant violation. Test both activation/membership log
orders, leader change during joint consensus, cancellation before the first
stage, restart after each stage, and forged higher-index but uncertified
snapshots.

### 3.3 Automatic Unavailable-Node Placement Reconciliation

Temporary loss of a storage process must not require an operator to mutate every
affected PG before the cluster can use already-provisioned spare capacity. Once
the node's heartbeat lease and a configured failure grace period have expired,
one control-plane-owned reconciler should move affected PGs toward placements
that exclude the unavailable incarnation. This changes PG placement, not the
node's durable membership or identity. It supersedes the initial static
multihost rule that only a permanent `Out` transition triggers migration when
the committed topology already contains an eligible spare failure domain.

The 2026-08-25 four-host `2+1` Warp outage exposed the missing composition. The
authority expired the failed node and moved affected PGs to `Peering`, but no
production component submitted replacement acting sets: control-plane
diagnostics recorded zero acting-set mutations. Pending commands then retained
one blocked slot on affected PGs and foreground traffic progressively collapsed
into `SlowDown`. The multihost harness had hidden this gap by invoking the live
metadata-transfer administration command for its selected probe PG.

Implementation status (2026-08-26): the control-plane state-machine slice and
the first bounded production reconciliation worker are complete. Lease expiry
now persists the exact unavailable incarnation,
endpoint, lease deadline, and authority observation. The certified initial
topology also owns the EC shape, failure-domain identities and policy, failure
tolerance, and exact unavailable-replacement grace interval. A new replicated
compare-and-swap binds that evidence to the exact source PG and epoch, exact
proof-qualified surviving metadata route and process observation, policy-valid
Active spare process and placement, certified grace cutoff, predecessor
transition tip, and transition epoch. Its commit atomically records the durable
transition and converts the exact `Active` or already-`Peering` source route to
a source-authorized fenced `Peering` route. There is no preceding generic
`SetPgState` mutation, so a stale scan cannot fence a renewed incarnation or a
newer route when the exact begin compare-and-swap rejects.

The fence and transfer-install RPCs carry the exact transition PG, source epoch,
source acting set, destination acting set, and transition epoch. The state
machine rejects generic transfer mutations while a transition owns the PG and
rejects stale exact mutations after that transition is archived or superseded.
After transfer installation, one atomic readiness-and-activation command binds
every `k + m` destination's exact node incarnation, endpoint, live lease,
destination route, transition, topology, and authority time. It revalidates all
of that evidence in the activation compare-and-swap before changing the PG to
`Active`; no independently durable readiness token can be invalidated by the
heartbeat that would otherwise need to consume it. A same-incarnation lease
renewal invalidates an already-built command, and the worker must derive a new
atomic command from current evidence. State validation
retains the source route's proof floor, epoch, and imported provenance in route
history, binds source and destination transfer provenance to that reconstructed
history, and rejects coordinated transition evidence that no command could
produce.
Activation archives the completed tip into retained historical
dependency state; it no longer blocks a later ordinary placement change or a
CAS-bound successor transition. State v31 and command v19 sealed the active and
retained lineage, readiness, and certified placement policy. State v32 and
command v20 additionally sealed immutable canonical singleton begin/completion
replay receipts. State v32 retained the exact completion
evidence and protected source/activation routes needed to recompute each
receipt digest and reject command-unreachable completion epochs during snapshot
validation. Command v21 and state v33 promote begin and completion to canonical
bounded vectors and permit the resulting durable multi-member receipts. The
reconciliation scanner now returns every candidate from one bounded 16-PG
page. Before proposal, the authority rederives every member against one
immutable snapshot, excludes definitively invalid members, and closes one safe
prefix before either the 16-member bound or the exact 131,071-byte OpenRaft
entry ceiling. The command-byte limit is the resulting 131,042 bytes after the
normal-entry envelope, and both standalone and Raft publication enforce it.
The authority commits the page's included begin candidates in one
heartbeat-gated plural command, queues their metadata transfers with bounded
capacity, accumulates the completed transfers, and activates the resulting
ready group in one plural command. Only definitive pre-proposal member
rejections are split out. A proposal, durability, protocol, or other batch-wide
failure never falls back to serial mutations; size-excluded and stale members
are rederived from the next immutable snapshot so an earlier activation cannot
invalidate a later command built from the old epoch. Direct-admin entry points
use the same plural grammar with one member. The separate
destination-install/staging protocol and concurrent artifact-transfer workers
remain later slices. State v33
retains immutable command-v20/state-v32 rejection evidence in addition to the
older command-v19/state-v31 evidence.
The live metadata-transfer implementation now has an explicit protocol-neutral
`prepare`/`install`/`import` lifecycle. Preparation owns the exact transition
binding, source route and runtime-map generation, and exported artifact;
installation consumes that opaque state and produces a distinct installed
state containing the destination route and imported proof; only that installed
state can enter import. Preparation is regression-tested to leave the
destination acting set and transfer marker unchanged until the explicit install
boundary. The existing reconciliation worker still invokes the sequential
wrapper, so this refactor changes neither durable grammar nor epoch count and
requires no version advance. It is the internal ownership boundary on which
durable staging and plural destination installation will be built.
Preparation now also computes and owns the imported metadata proof for the
expected next destination epoch. A destination-epoch conflict refreshes the
source runtime map and replaces that compact prepared binding before retrying.
Once the immutable artifact has been staged, storage can derive and durably
publish a replacement proof receipt for the new target epoch from those same
fsynced bytes; the artifact digest and staging authorization do not change.
Artifact decoding and proof rebasing therefore remain outside control-plane
command derivation. This is still protocol-neutral and does not authorize
staging or route installation by itself.
The lifecycle now continues through opaque `Prepared`, `Authorized`,
`Published`, and `Staged` ownership states for unavailable-PG reconciliation.
`Prepared` is protocol-neutral and cannot reach a destination mutation API.
Only a snapshot-issued `Authorized` capability containing the complete exact
committed authorization batch can create the intent or publish the immutable
artifact and epoch proof to destination actors. Across storage RPC that
capability becomes an untrusted presentation; the destination upgrades it only
after exact matching against the complete batch receipt in its latest
authority-authenticated runtime map. Admin authentication alone is not staging
authority, and uncommitted, subset, or regrouped presentations fail before
catalogue or filesystem mutation. Partial destination failure retains the same
presentation for exact retry. Only the complete receipt set can become staged.
The composed nonempty regression then applies every destination's
durable evidence page, advances an unrelated cluster epoch, republishes the
proof receipts derived from the same staged bytes, installs the plural
destination transition, imports the retained artifact, and activates the PG.
The source artifact may describe an older retained route, but its epoch must
not exceed the staging authorization's exact source epoch. Production
reconciliation remains on the singular wrapper until the cleanup,
tombstone/finalized-floor, and durable artifact-retrieval protocol below can
replace it atomically; this slice does not leave two production ownership
paths active.
Command v22 and state v34 now add the fourth homogeneous batch boundary,
`AuthorizeUnavailablePgStagingIntents`. Each canonical member binds the exact
active unavailable transition, transition-derived staging generation,
artifact SHA-256 digest and byte length, and storage-owned staging format
version. The state machine validates the complete vector against one immutable
snapshot, commits every member or none, records a recomputable whole-batch
receipt on every transition, and leaves the global cluster-map epoch
unchanged. Exact whole-batch replay remains valid after unrelated epoch
advances; subset, regrouped, divergent, mixed replay/new, post-install, and
future-receipt states fail closed. Snapshot validation requires the receipt
epoch to precede destination installation, or the direct successor transition
when an uninstalled transition has already been retained. The same independent
16-member and 131,042-byte command ceilings apply at encoder, decoder, state-machine,
standalone, and Raft proposal boundaries. Immutable command-v21/state-v33
aggregates and nested container vectors remain rejection evidence. This
authorization command is still leader-internal. Snapshot builders now validate
the complete immutable result and exact OpenRaft entry size before returning a
plural command, and both standalone and Raft authorities expose that builder
through the shared `ControlPlaneAdmin` batch boundary. The destination staging
transport described below accepts only the opaque capability reconstructed
after exact comparison with the authority-published committed authorization
receipt; a locally prepared request or admin-signed storage request is not
sufficient authority.
Command v23 and state v35 now provide the replicated half of durable staging
evidence publication while keeping that protocol isolation. The new
`ApplyMetadataTransferStagingEvidencePage` command carries the exact
storage-owned canonical operation payload and page digest. Command decoding
revalidates the page before dispatch; application binds the actor to its
current node incarnation and endpoint, validates every publication or
tombstone member against the exact transition staging authorization, retains
the canonical evidence and authority-neutral apply receipt, and leaves the
cluster-map epoch unchanged. Exact current-page replay returns the same
receipt without mutation; conflicting generations, gaps, foreign actors,
divergent evidence, and missing snapshot members fail closed. Snapshot
validation treats the receipt actor as historical after admission, so a later
node incarnation cannot invalidate already accepted evidence. Immutable
command-v22/state-v34 aggregates remain rejection evidence. Control-plane RPC
v20 and the production storage outbox now submit this command through isolated
low-priority admission. Finalized-floor cleanup, tombstone publication, and
destination cleanup remain later protocol slices.
Command v24 and state v36 consume the retained publication evidence through
`InstallUnavailablePgPlacementTransitions`. The plural command binds one
shared destination epoch and a canonical ordered member vector; every member
binds its exact transition, metadata-transfer proof, destination acting set,
and ordered actor/incarnation/endpoint/publication-digest commitments. The
state machine validates the complete vector against one immutable source
snapshot before mutation, rejects mixed replay and new application, installs
every destination route at one global epoch, and retains a whole-batch receipt
whose digest is reconstructed during snapshot validation. Exact whole-batch
replay is a no-op; subset, reordered, divergent, missing-evidence, and
coordinated snapshot-forgery cases fail closed. Immutable command-v23 and
state-v35 aggregates remain rejection evidence. The singular unavailable-PG
install path remains temporarily available to the existing reconciliation
worker; it must be retired in the same slice that switches that worker to
authorization, staging, plural installation, and cleanup so outage recovery
is never disabled between commits. As with staging authorization, plural
installation now has one replication-safe snapshot builder and shared
standalone/Raft `ControlPlaneAdmin` submission boundary. Two-member durable
standalone tests pin all-or-none rejection, exact replay before and after
authority restart, and one shared destination epoch. A composed three-voter
OpenRaft regression additionally invokes both plural `ControlPlaneAdmin`
methods, verifies each intermediate snapshot on every replica, transfers
leadership, and replays both exact batches through the successor leader without
state or epoch mutation. The production worker does not consume that boundary
yet.
The storage-owned durable staging foundation was first sealed at v2 and remains
protocol-isolated. Staging-store format v2 has a fixed root manifest and exact
initialization-complete marker, exact SQLite catalogue,
exact transition/generation/artifact intent CAS, bounded
capacity and startup inventory, content-addressed fsync/rename/directory-sync
publication, import state, tombstone-before-unlink ordering, per-PG finalized
floors, and durable receipt/tombstone evidence deltas. Startup completes an
exact renamed-but-uncommitted publication transactionally, removes interrupted
temporary files, quarantines unexplained regular artifacts, and rejects
symlinks and special catalogue files, unsupported versions, changed schema,
missing or corrupt published artifacts, and coordinated receipt corruption.
Each evidence delta independently persists its historical actor tuple and
validates the canonical receipt against it across process-incarnation changes.
Recovered publication syncs both the artifact and its containing directory
before committing the receipt-bearing catalogue transaction. First
initialization publishes its fixed marker only after the exact v2 catalogue
and its directory entries are durable. Before that can complete, creation of
the staging root is synced through the parent storage data directory. A second
fixed establishment marker is then atomically published in that parent; its
presence requires the existing initialized root and exact v2 catalogue without
create/repair authority, so whole-root deletion, catalogue deletion, or
truncation cannot erase tombstones or finalized floors by becoming fresh
initialization. The complete v1 manifest, markers, publication receipt, page,
and apply-receipt bytes remain immutable rejection evidence. The former v2
fixtures seal the previous proof-bearing artifact and evidence grammar and
remain immutable rejection evidence under v3. The catalogue
now owns the local durable evidence-page protocol required below. It selects at
most 64 deltas beneath the 120 KiB operation-payload ceiling, persists the exact
canonical payload and digest before transmission, and replays those bytes
unchanged across newly queued deltas, process restart, and authentication-window
expiry. A canonical authority-neutral apply receipt atomically acknowledges
exactly those page members. The next page is assigned only after that receipt
is durable and binds its predecessor generation and receipt digest; malformed
receipts, altered members, and coordinated catalogue corruption fail closed
during operation or startup validation. Page bytes deliberately exclude
request IDs, timestamps, expiry, and authenticators. Control-plane-managed
storage-node startup now opens the established store before listeners and
starts one independently paced outbox worker with its own authenticated
control-plane client. The worker loads or replays one exact durable page,
wraps each attempt in a fresh authentication envelope, records only the exact
matching apply receipt, and fail-stops the process on local durability,
protocol, or integrity failure. Retryable transport, leadership, response-loss,
and bounded evidence-admission failures retain the page and use a one-second
backoff. Standalone storage nodes do not create this protocol state. The
control-plane RPC v22/authentication-envelope-v2
slice now exposes a dedicated storage-node-only evidence operation over
authenticated Unix and TLS, dispatches the existing replicated evidence
command, validates the exact canonical apply receipt, and separates its bounded
request-worker allowance from ordinary control-plane traffic. Raft submission
also serializes evidence proposals separately, yields the heartbeat update
gate to already-queued ordinary, membership, or lease-renewal mutations, and
defers at every pre-append preparation, dispatch, and proven-unappended retry
boundary when a new ordinary waiter appears. The bounded RaftCore enqueue is
itself raced against waiter registration and is accepted only when its
cancellation-safe send completes. The final ordinary waiter broadcasts its
completion so every admitted evidence caller can continue to durable
serialization. Authenticated evidence saturation is a typed v22 response;
response loss remains retryable, while observed
authentication, framing, protocol, and receipt-integrity failures fail-stop.
After RaftCore accepts a proposal, the evidence lane retains its own
durable-proposal serialization ownership but releases the heartbeat update
gate while awaiting commit and application. Volatile heartbeat renewals may
pass during that interval; ordinary commands and membership changes remain
queued behind the accepted evidence log entry. Completion waits for queued
volatile renewals, reacquires the update gate, and always reloads the latest
volatile overlay for the exact pre-dispatch term and
applied log before publishing the new overlay base; it never republishes a
stale overlay captured before dispatch. Promotion completion also preserves an
overlay that a renewal has already rebound to the promotion log. At most one
evidence proposal runs at a time, and evidence publication never shares the
storage node's heartbeat client or submission mutex.
Storage RPC v24 first exposed the destination half of the staging boundary as
three admin-only live-transfer operations. Intent creation carries the canonical
storage-owned transition/generation/artifact tuple and performs the durable
intent CAS. Artifact format v2 canonically carries the checkpoint, bounded
retained-log chunks, destination epoch, and claimed imported proof. Storage
reconstructs and validates the exact `PgMetadataTransferProof` before issuing
a publication receipt. Every publication receipt independently binds its exact
target epoch and derived transfer proof. The lightweight proof-publication
operation can derive a successor epoch's proof from the already durable
artifact without retransmitting or reauthorizing its immutable bytes. The
store retains at most 64 epoch-bound proof receipts per intent. Installation
requires one exact-target receipt from every destination and requires every
receipt to derive the request's exact transfer proof. Artifact A therefore
cannot authorize proof B, and an artifact-A proof for epoch N cannot authorize
installation at epoch N+1. Complete-artifact publication is bounded by the
frame ceiling, rechecks length and
SHA-256 during encoding and decoding, and returns only a canonical receipt
after the staging store's file and directory durability
barriers plus catalogue commit. Exact intent and publication replay return the
same durable outcome. The storage-owned protocol maximum is 63 MiB for this
complete-artifact v24 transport; control-plane authorization, intent
construction, store admission, and RPC encoding all reject larger artifacts as
permanent protocol violations before reserving staging capacity. Replicated
static configuration additionally requires every storage-RPC endpoint profile
to carry the maximum authenticated publication envelope, including the
canonical intent, frame, binding, and authentication overhead. Because the
client uses the minimum limit across alternate endpoints, validating every
endpoint prevents an exact authorization from becoming unpublishable after it
commits. Supporting a larger artifact requires a future chunked publication
grammar and coordinated format-version advance. Each destination also rejects an intent unless its
durable actor identity is in the exact destination acting set. Authenticated
Unix and TLS regressions exercise all three
operations through the production server and capability; ordinary frontend,
storage-node, and maintenance principals are excluded. The operations are
available only when the control-plane-managed node opened its established
staging store. They deliberately do not mutate a PG route or install imported
metadata. Reconciliation selection of committed staging authorizations,
receipt-set consumption during destination installation, tombstone cleanup,
and finalized-floor pruning remain later slices. Bounded compact actor-chain
checkpoint segments are implemented by the command-v26/state-v38 slice
described below.
Command v19 additionally sealed the
post-grace completion fence that prevents survivor heartbeats from indefinitely
reactivating the old acting set; immutable command v18 remains rejection
evidence. A control-plane RPC v17 adds exact transition-bound fence and install
operations plus typed transient authority-clock wait responses, retaining the
complete v16 operation corpus as rejection evidence. A
control-plane-owned cursor examines at most 16 PGs per tick, commits the exact
certified transition before dispatch, and retains one exact transition identity
through a capacity-one storage transfer worker.
Raft reconciliation derives both the begin command and the atomic
readiness-and-activation command while holding the same volatile-heartbeat
update gate used by heartbeat publication. The exact lease-bound CAS therefore
cannot be invalidated in the gap between reading a snapshot and submitting its
command; a renewal queued after derivation runs immediately after the command
has committed. This preserves exact lease equality rather than weakening the
authorization to accept a later, independently sampled lease. The sampled
authority time is also clamped to the gated snapshot's committed timestamp
high-water mark, so a preceding durable heartbeat cannot make the derived
command regress authority time. Direct Raft admin transition entry points use
the same gated derivation primitive.
Restart or authority failover rediscovers active durable transitions; repeated
manager polls cannot duplicate an in-flight transfer; stale completion cannot
activate a successor. Transfer and activation failures are deferred per PG so
one unavailable destination does not monopolize the global worker; explicit
durability, invariant, snapshot, and protocol failures quarantine only their
exact transition while scanning continues. The worker uses the existing
metadata-transfer path and activates only after every destination has supplied
current exact metadata proof and payload-write readiness. Historical payload
backfill, terminal dependency cleanup,
backpressure, maintenance modes, pre-install successor selection when a newly
chosen destination fails before route installation, operational metrics, and
the multihost outage release gate remain to be implemented.

The 2026-08-26 four-host read-only outage exposed the next availability bound.
One failed actor affected 83 of 116 PGs. Recovery advanced the global cluster
epoch from 18 to 267: exactly one begin, one destination-install, and one
activation epoch for each affected PG. Because every global epoch requires
fresh storage-node map installation and current-epoch PG observations, a
capacity-one worker serializes recovery behind three whole-cluster heartbeat
rounds per PG. The cluster eventually recovered all 116 PGs, but served only a
progressively increasing subset for several minutes and returned transient 503
responses for the rest. Removing command-derivation races reduces retries but
does not remove this linear outage duration.

The next reconciliation slice must make global epoch count independent of the
number of affected PGs. Prefer bounded batch commands for exact transition
begin, destination installation, and activation, with per-PG proofs and
all-or-nothing validation inside each batch. Metadata transfer and payload
readiness remain bounded and fair outside the state-machine lock; completed
proofs accumulate into bounded batches. A batch advances the global epoch once,
preserves each PG's independent transition lineage and replay identity, and
retries a failed member separately without discarding successful transfer work.
If those invariants cannot be represented cleanly with batch commands,
introduce independently versioned per-PG route generations before increasing
worker concurrency; merely adding workers around the current global-next-epoch
CAS would increase conflicts without reducing recovery time.

#### 3.3.1 Bounded unavailable-PG transition batches

The batching slice must change the replicated transition protocol, not merely
collect calls around the existing single-PG worker. The target protocol for one
bounded group is:

1. commit one batch begin command;
2. prepare and export each PG's metadata outside Raft;
3. commit one batch staging-intent authorization without advancing the cluster
   map epoch;
4. create destination intents, stage the exact artifacts, and commit bounded
   receipt evidence;
5. commit one batch destination-install command using the completed proofs;
6. import and validate metadata on every destination outside Raft; and
7. commit one batch readiness-and-activation command.

Use three homogeneous command domains rather than one mixed-stage command:
`BeginUnavailablePgPlacementTransitions`,
`InstallUnavailablePgPlacementTransitions`, and
`CompleteUnavailablePgPlacementTransitions`. Each command contains between one
and `MAX_UNAVAILABLE_PG_TRANSITION_BATCH` entries, requires canonical ascending
PG-ID order with no duplicates, validates every entry against one immutable
source snapshot, applies every mutation to one cloned destination snapshot,
and advances the global cluster epoch exactly once. Initial implementation
may use 16 as an independent entry-count safety bound, but the scan bound is
not a wire-size proof. Define a batch encoded-byte ceiling below the maximum
Raft command-entry payload after accounting for the command frame and fixed
envelope. The manager incrementally measures candidates with the production
command encoder and closes the batch before either the count or encoded-byte
bound would be exceeded. A legal single entry must be proven to fit; failure of
one entry to fit is a typed fatal configuration or invariant error, not a
retryable batch that the manager reconstructs forever. Decoder and state-machine
validation independently enforce both bounds. Expose count, encoded bytes,
fill, and latency metrics before tuning either limit.

Artifact selection uses a fourth homogeneous command,
`AuthorizeUnavailablePgStagingIntents`. It records the exact transition,
staging generation, canonical artifact digest and length, and staging-store
format version after export but before any destination intent or byte write.
It is a durable Raft state-machine mutation with its own batch receipt, but it
does not change a PG route or advance the global cluster-map epoch. Every
destination staging capability and receipt must consume that committed tuple;
an old leader cannot authorize a different artifact after leadership changes.
This fourth command uses the same entry-count and exact production-encoded
byte ceilings, canonical ordering, local pre-proposal splitting, decoder and
state-machine bounds, and legal-single-entry fit requirement as the three route
commands. The shared batch constructor owns those rules so adding a new batch
stage cannot silently bypass them.

Batch application is all-or-nothing. A stale, malformed, divergent, or
unauthorized member rejects the complete command without changing state. An
exact replay is accepted as a no-op only when every member is the exact prior
request; mixed new and replayed members are rejected. The manager then
rederives a batch without the stale member, deferring or quarantining only that
PG according to its typed failure. Every member retains its own exact
transition binding, predecessor tip, unavailable observation, source proof,
destination placement, and replay identity even though all members share the
batch's resulting global epoch. Multiple PG transitions may therefore
legitimately share one transition epoch or destination epoch, and snapshot and
history validation must reproduce that command-reachable state.

Member-local identities are not sufficient to distinguish an exact replay
from a subset or regrouping of a committed batch. Every batch therefore has a
canonical receipt covering the stage, source and target epochs, topology and
authority binding where applicable, ordered member count, and digest of the
complete canonically encoded member vector. Each affected transition records
the receipt identity for that stage, and the state retains one corresponding
receipt record containing the canonical digest and ordered member identities.
Replay succeeds only when the supplied complete vector reproduces that receipt
and every member points back to it. A subset, superset, reordered vector, or
regrouping across prior receipts is rejected even when every supplied member is
individually exact. Receipts survive snapshots and Raft-log compaction and are
pruned atomically only when every member's transition lineage and command
replay evidence have left the retained-history window; receipt storage must be
bounded by that same explicit retention policy rather than by process-local
cleanup.

Do not implement batch application by sequentially applying the current
single-PG commands: those commands each derive and advance the next global
epoch. Refactor each stage into pure validation against the unchanged source
snapshot followed by mutation helpers that accept the explicit batch target
epoch. Validate the full vector before invoking any mutation helper. Keep
ordinary metadata-transfer commands and unavailable-transition installation
as disjoint command domains; a generic metadata-transfer command must reject a
PG owned by an unavailable-placement transition rather than accepting an
optional transition binding.

The coordinated destination-install/staging command version removes the singular
`BeginUnavailablePgPlacementTransition` and
`CompleteUnavailablePgPlacementTransition` variants and removes the optional
unavailable-transition branch from `SetPgActingSetWithMetadataTransfer`.
Unavailable-transition route installation exists only through the plural
install command; the ordinary transfer command rejects an active transition.
Likewise, use a dedicated exact-transition fence operation rather than an
optional transition field on the generic fence command. Worker, direct-admin,
test, and recovery entry points submit plural commands, wrapping one entry when
only one PG is available. Its corresponding state version requires every active and
retained unavailable transition to carry the applicable batch receipts and
staging generation, so no singular command or decoded legacy state can bypass
batch accounting.

The current live-transfer operation cannot be batched as written because it
fences, exports, commits the destination route, and imports in one call. Split
it into explicit `prepare`, batch `install`, and `import` phases. Preparation
retains the exact transition binding, source route and proof, artifact digest,
and the imported proof precomputed for one expected target epoch. The immutable
artifact remains authorization-bound to its original digest; if another map
mutation consumes the expected epoch, each destination decodes those retained
bytes and publishes a new target-epoch/proof receipt rather than rewriting the
artifact. Artifact decoding, retained-command validation, canonical
re-encoding, hashing, and epoch rebasing all occur outside the heartbeat update
gate. Gated install
derivation only compares the current next epoch with the prepared expected
epoch and validates compact fixed-width receipt bindings. If the epoch changed,
it releases the gate, obtains exact successor proof receipts outside the gate,
and retries; it never processes up to a batch of metadata artifacts while
excluding heartbeat renewal.

An ownership token alone is not sufficient once the committed imported proof
is bound to exact artifact bytes. Before destination-route installation, the
prepare phase first binds its immutable artifact to the committed authorization,
then durably stages it under its transition binding and content digest on every
destination actor and obtains authenticated fsync-complete receipts. Every
destination mutation carries an opaque committed capability bound to the exact
`(destination node, PG)` batch member and rejects a missing, cross-member, or
mismatched capability before catalogue or filesystem mutation. Storage checks
that binding again against its durable actor identity rather than trusting the
RPC route. A newly committed epoch-neutral authorization can reach the sender
before the destination's next runtime-map refresh; that specific
same-epoch or newer-epoch not-yet-observed result is typed retryable and
non-mutating. Storage validates the canonical batch digest before authority
lookup, and a presentation absent from a strictly newer observed runtime-map
epoch is definitively stale; malformed, stale, regrouped, foreign-member, and
otherwise invalid presentations fail closed. Initial storage bootstrap carries
the authorizations from the authoritative startup map through listener binding,
so the first authenticated staging request after restart observes the same
authority state without a separate runtime-map installation.
`CreateStagingIntent` compare-and-swaps the complete
committed authorization tuple: exact transition, staging generation, artifact
digest, artifact byte length, and staging-store format version. Exact replay is
idempotent; any field mismatch is a typed intent conflict and no bytes are
accepted. This prevents concurrent or superseded leaders from selecting
different artifacts under one generation on different destinations.

A canonical staging receipt contains the storage node identity and incarnation,
endpoint identity, exact transition binding, artifact digest and byte length,
staging generation, storage-format version, and an
explicit fsync scope covering the artifact, catalogue record, and parent
directory publication. Transport authentication alone is not replicated proof.
Receipt evidence uses a mandatory control-plane RPC v22 operation separate
from lease heartbeat renewal. Each authenticated storage node maintains a
durable outbox of receipt and tombstone deltas. A page binds node identity and
incarnation, required `previous_generation` and
`previous_apply_receipt_digest` fields, a strictly monotonic
evidence-page generation, canonical page digest, and bounded ordered evidence
entries. For the first page, `previous_generation` is zero and
`previous_apply_receipt_digest` is the all-zero value of the canonical digest
width; no other page may use that genesis pair. Both fields are part of the
canonical operation payload covered by request authentication. Both entry
count and exact encoded bytes, including the predecessor digest, are capped
beneath the control-plane RPC frame and Raft-command limits; larger delta
inventories are sent as successive page generations. Exact page
retransmission is idempotent, gaps are rejected and retried, and a page cannot
be assembled or decoded on the lease-renewal path. Lease renewal uses
its existing bounded request and submission path regardless of receipt backlog
or failure, so staging evidence can stall without expiring a serving node.

Page assignment and acknowledgement are themselves durable staging-store
state. The node transactionally selects a bounded prefix of unacknowledged
deltas, assigns `(previous_generation, previous_apply_receipt_digest,
generation)`, encodes the canonical page operation-payload bytes and digest,
and persists that exact in-flight payload before transmission. The page builder
accepts an opaque durably recorded apply-receipt token, from which it alone
extracts the predecessor generation and digest; production callers cannot
supply or combine those raw fields. A separate internal genesis token produces
only the fixed genesis pair. The persisted bytes exclude the freshness-bound
RPC authentication envelope. Every transmission and retry wraps the identical
canonical operation payload in a newly generated request ID, timestamp,
expiry, and authenticator; replaying stale authentication material is
prohibited. It sends no later generation while one page is unacknowledged.
Response loss, a same-actor store reopen, newly queued deltas, or expiry of the
prior authentication window cause byte-for-byte retransmission of the retained
operation payload under a fresh envelope, never reconstruction with different
members.

Node-incarnation rollover is a distinct durable boundary. The staging catalogue
persists one singleton evidence actor, and every delta, in-flight page, and
apply receipt must bind that exact actor. After startup has validated the full
catalogue and artifact inventory, reopening the store for a newer incarnation
of the same node runs one immediate transaction that re-encodes every retained
publication and tombstone fact for the new actor, updates publication receipts,
marks every delta unacknowledged, removes the old in-flight page/apply receipt,
and advances the singleton actor. The new incarnation therefore starts at the
genesis page pair and never appends new-actor evidence to the old actor's page
chain. A different node ID, a non-advancing incarnation, or an endpoint change
without an incarnation advance fails closed before startup reconciliation can
mutate files or catalogue state. Every filesystem or catalogue mutation first
holds an immediate catalogue transaction, validates the singleton actor, and
retains that cross-handle serialization through its durable commit. If the old page committed but its
response was lost, the control plane may retain both actor chains; evidence is
keyed by the complete node/incarnation identity, and installation selects the
currently authorized actor's exact receipt. This duplication is safe and is
removed only by the ordinary finalized-floor cleanup protocol.

Applying a page creates a canonical, authority-backend-neutral apply receipt
bound to node identity and incarnation, previous generation, previous
apply-receipt digest, generation, page digest, and the resulting accepted
evidence generation. While detailed evidence remains retained, control-plane
state persists every contributing canonical page and receipt in that actor's
contiguous chain. This makes page membership independently reconstructible
during snapshot validation; actor identity alone is not evidence provenance.
The control plane cannot observe the node's durable recording from the response
it sends. It therefore retains the highest accepted page and apply receipt
as the chain tip across finalized-floor advancement, snapshots, journal
compaction, Raft compaction, and authority failover. The tip advances only when
the control plane accepts the exact successor page whose
`previous_generation` and previous apply-receipt digest cite it. Because the
node may construct that successor only after durably recording the cited
receipt, acceptance is the observable acknowledgement boundary. State v38 can
replace acknowledged non-tip page ranges with the bounded actor-chain
checkpoint segments below. It still does not permit per-PG detailed-evidence
pruning: pages and segments may mix PGs, and removing an uncovered member would
destroy snapshot-verifiable provenance. Finalized-floor cleanup remains gated
until actor-incarnation chain closure and the floor-driven detail-removal and
segment-collapse protocol are implemented.

A Raft log ID or standalone journal position may accompany the apply receipt
as diagnostic metadata, but is not part of receipt identity or required for
replay. The response uses a fresh authenticated envelope carrying the
canonical receipt. Only after the node durably records the exact receipt does
it retire the page's deltas, advance its acknowledged generation, and build
the next page. Exact replay lookup precedes finalized-floor and pruned-evidence
rejection: replay of any retained contributing page returns the same canonical apply receipt
under a fresh response envelope without recreating evidence, while any
different payload at that generation is rejected. For a new generation,
server admission requires both predecessor fields and compares them with the
retained receipt before changing evidence or replacing that receipt. An
omitted, malformed, genesis-on-successor, or incorrect predecessor digest is a
protocol error that leaves the retained receipt and all evidence unchanged. A
conflicting digest for an assigned generation is fatal protocol evidence.

Command v26 and state v38 add canonical compact actor-chain checkpoint
segments. A segment replaces an exact contiguous range of complete
accepted pages atomically and contains the exact actor tuple, predecessor
anchor, range-tip generation, canonical tip apply receipt and digest, plus a
sorted fixed-width commitment for every distinct evidence identity in that
range: the complete evidence key and SHA-256 digest of its canonical bytes. A
segment contains at most 64 commitments. Control-plane command v26 independently
limits a request to 64 pages, while state v38 independently limits a complete
checkpoint state record, including its key prefix and newline, to 120 KiB. The
command must also fit the replication-safe Raft-entry ceiling. The state machine
checks the complete canonical state record before removing any page and rejects
a range before traversal when its page count exceeds the bound. These
control-plane-owned count and encoded-byte limits do not inherit staging-page
format constants; changing either language requires the corresponding
control-plane command or state version advancement.

The checkpoint operation validates the source pages, receipts, predecessor
links, complete actor tuple, member set, and both bounds before removing those
page payloads. Every compacted link must reproduce the digest of the canonical
storage-owned apply receipt for its actor, predecessor, generation, and page
digest; interior links are not trusted as opaque digest pairs. The
first later segment or retained page must cite the preceding segment's exact
tip receipt. Snapshot validation reconstructs every boundary and rejects gaps,
overlap, reordered or duplicate commitments, altered member digests, an
oversized segment, and a segment or page that does not cite its predecessor.
Later complete page ranges may become separate checkpoint segments even while
an earlier segment retains an unresolved PG commitment; an unresolved member
therefore pins at most one bounded segment rather than the actor's later page
history.

The implementation retains every detailed evidence byte and keeps each
actor's complete current page tip replayable. Checkpoint links retain the
ordered `(sequence, evidence identity)` membership needed to reconstruct each
original canonical page digest; a commitment digest alone is not accepted as
proof that evidence appeared in that page. Snapshot validation reconstructs
the contiguous page/segment chain keyed by exact node, incarnation, and
endpoint, verifies every page membership, link, and tip receipt, and compares
each segment commitment with retained or finalized canonical evidence from
that exact actor.
Standalone restart and three-voter Raft tests cover multi-page and adjacent
segments, epoch-neutral replication, leadership-transfer replay, and corrupted
commitment/tip rejection. Actor-incarnation closure is implemented as described
below. At that checkpoint slice, per-PG floor pruning, covered-segment collapse,
and coalescing remained gated follow-up work.

Per-PG cleanup then appends a durable finalized-generation certificate and
advances the maximum finalized floor without replacing certificates for older
generations. This removes detailed bytes independently, but retains the
fixed-width commitment until its key is provably covered by its exact
certificate. Snapshot validation requires exact detailed bytes and digest for
every uncovered commitment and requires covered members to be absent. Once
every commitment in one segment is covered, a second exact CAS
collapses that segment to a tip-only chain anchor without waiting for any other
segment. A separate coalescing CAS consumes at most 64 adjacent covered
tip-only anchors beneath the same 120 KiB and Raft-entry ceilings. It validates
their contiguous predecessor chain, atomically replaces them with one canonical
anchor carrying the lowest predecessor, newest tip receipt, and the count and
digest of the canonical finalized source-segment vector. The source vector is
independent of prior coalescing groupings, so recursive coalescing and exact
replay of an absorbed checkpoint, collapse, or coalescing command preserve one
semantic identity. The immediate later segment or page remains valid because it
already cites the retained newest tip. Repeated bounded direct coalescing can
therefore retire arbitrarily long fully covered anchor history without
constructing an unbounded command. Snapshot validation reconstructs every leaf
source segment from finalized evidence and checks the cumulative digest,
boundary generations, predecessor and tip receipts, and first later citation.
This keeps retained chain topology bounded while preserving the provenance and
predecessor link needed by later segments.

A page is not eligible for checkpointing, floor-driven detail removal, or
receipt pruning while it is the actor's current accepted tip. If its apply
response is lost, the control plane retains the complete page, evidence, and
canonical receipt; the node must replay that page, durably record the returned
receipt, and submit a successor citing it. Only acceptance of that successor is
the observable proof that permits the former tip to enter a checkpoint segment.
Until a successor exists, finalized-floor cleanup may record that its cleanup
conditions are satisfied but must reject any CAS that would prune the tip's
detailed evidence. A compacted historical page is no longer an exact-replay
target. The new current tip remains fully retained and replayable across
response loss.

An actor-incarnation rollover cannot submit an old-actor successor, so store
and evidence format v4 provide a separately versioned actor-chain closure path
rather than treating new-actor genesis as implicit acknowledgement. The
storage rollover transaction durably records a canonical closure candidate.
It preserves the first unclosed actor tuple, the exact acknowledged
predecessor tip, and an optional exact ambiguously assigned successor across
repeated rollovers. The authority selects whichever of those two exact tips it
actually retained, so assigning page N+1 and crashing before dispatch cannot
make an authority that accepted only N irreconcilable. The candidate advances
the `through_actor` to the latest crossed incarnation, binds that actor's exact
endpoint, and binds the count, maximum durable sequence, and digest of the
complete semantic evidence prefix rebound into the new actor. Only the new
actor's genesis page carries the candidate. If that prefix spans multiple
bounded pages, closure remains unavailable until all of those pages have been
accepted. Candidate construction and decoding require
`rebound_entry_count <= rebound_max_sequence`. Authority application validates
the candidate's actor/incarnation ordering, current-node fence, retained first
tip, and every currently observable `through_actor` endpoint before considering
the rebound prefix incomplete. Only a strictly lower observed count whose
maximum sequence is still below the committed bound defers; an excess count or
an incomplete set that has already reached the committed maximum is rejected
without retaining or acknowledging the genesis page.

Applying the page under command v27/state v39 builds shared indexes for actor
tips, page entries, and detailed evidence, then reconstructs every candidate
in one ordered pass. It validates the current-or-successor node incarnation
fence and exact endpoints and verifies every source actor's retained evidence
has an exact rebound member. Each unclosed actor tip is consumed at most once,
so validation grows linearly with retained pages and evidence plus ordered-map
lookup rather than rescanning all history for every incarnation. It then records one immutable
closure certificate for each still-unclosed source tip through the candidate's
`through_actor`. A later rollover retains an existing A-to-B certificate and
adds B-to-C; it cannot rewrite A's destination or source tip. Each certificate
is bound to the destination genesis page and complete rebound prefix. The state
validator reconstructs the same closure set from durable chain evidence, so a
forged, omitted, widened, or conflicting certificate fails before state
replacement. A certificate makes the closed source tip eligible for a
checkpoint segment without claiming that the old response was received. The
candidate-bearing destination chain remains checkpoint-ineligible until the
certificate-retirement slice persists an equivalent compact dependency;
otherwise checkpointing its genesis would erase the only reconstruction
source. Missing,
incomplete, or conflicting closure evidence leaves the old page fully retained
and replayable. Response-loss, paged-prefix, checkpointed-tip, repeated-rollover,
standalone restart, Raft restart, and exact old/current format rejection are
covered. That closure slice left floor-driven evidence removal and
closure-certificate retirement gated for cleanup.

Receipt evidence has bounded low-priority admission independent of lease
renewal. It uses a separate connection/session allowance, request-worker
semaphore, submission queue, and at most one in-flight evidence proposal per
node. Lease traffic has reserved connection, worker, and Raft-host admission
capacity; an evidence backlog or saturated evidence queue receives typed
backpressure and cannot consume the final lease-renewal permit, hold the
heartbeat update gate ahead of a renewal, or run an internal proposal retry
loop while renewal work is queued. Evidence-page application is explicitly
cluster-map-epoch-neutral: it records durable control evidence without bumping
the global epoch, changing a PG route, invalidating a serving runtime map, or
requesting fresh PG observations.

Applying a receipt-evidence page records durable evidence keyed by transition,
staging generation, node, and incarnation rather than dropping it when a later
heartbeat or page omits the entry. Every page member must carry the page's exact
actor, and every retained detailed evidence record must resolve to that actor's
retained page chain. Snapshot validation reconstructs every actor/incarnation
page chain, requires its current node incarnation to be no older than the page
actor (with an exact endpoint match at equal incarnation), and requires the
detailed map to equal the exact union of retained page members. The control
plane retains each node incarnation's accepted evidence-page chain, cumulative
per-PG finalized staging-generation certificates, and their maximum floor.
Receipt evidence at or below that floor is rejected even if carried in
a later authenticated page; retransmission of a pre-cleanup page therefore
cannot resurrect tombstoned state after detailed evidence is pruned. The
install entry binds the artifact digest, expected target epoch, imported proof,
exact destination set, and canonical receipt-set digest. Every state-machine
replica verifies each referenced receipt against the committed evidence,
current node identity/incarnation and endpoint, committed staging authorization,
transition destination, byte length, format version, fsync scope, and exact
receipt-set digest. Snapshot validation repeats those relationships without
trusting the command builder. Receipt evidence is consumed by install but
retained through terminal import and cleanup evidence so replay and failover
remain independently verifiable.

The committed authorization also binds the target epoch encoded in the initial
artifact bytes. Destination publication decodes the canonical artifact before
mutation and rejects a target different from `artifact_target_epoch`; restart
recovery repeats that comparison after exact digest/length retrieval. Later
lightweight proof rebasing remains an explicit, separately evidenced operation
and cannot change which initial artifact the authorization admitted.

The staging authorization creates one cleanup obligation for every destination
in its exact authorized destination set, independent of which staging responses
or receipts the control plane observed. Pre-install cancellation and
supersession must send the exact tombstone operation to every authorized
destination. Each destination durably tombstones the generation and returns a
canonical cleanup receipt even when it has no local intent or artifact; this
rejects a delayed `CreateStagingIntent`, stage write, or lost committed response
for that generation. Installation requires a complete receipt set for that
same authorized destination set, so post-install cleanup retains, rather than
narrows, the original obligation set.

With that checkpoint representation in place, the global per-PG finalized
generation set advances only through an exact replicated,
cluster-map-epoch-neutral cleanup CAS. The CAS retains each older exact
certificate so response-loss replay remains possible after later generations
finalize. It rejects a generation that would cross any retained staging
authorization, including an authorized predecessor from which no evidence has
yet arrived. The CAS also rejects pruning any detailed member that still belongs to an
actor's current accepted page tip or to a historical page range not yet covered
by a validated checkpoint segment. Every obligated destination must have
committed a canonical cleanup receipt for the same transition, staging
generation, artifact tuple, and tombstoned local generation; partial cleanup
cannot advance the floor or prune staging authorization. Per-node tombstone
evidence rejects that node's stale receipt pages while evidence for an offline
destination remains admissible. Cleanup evidence and finalized-floor changes
do not advance the global epoch, change a PG route, invalidate a serving
runtime map, or request fresh PG observations.

Command v28 and state v40 implement the replicated finalized-floor boundary for
one PG at a time. `FinalizeMetadataTransferStagingGeneration` accepts only the
exact completed retained transition, its immutable staging authorization and
artifact tuple, and one sorted tombstone binding for every authorized
destination. Every tombstone must match retained detailed evidence from the
exact actor tuple and a retained checkpoint commitment; actor-closure-dependent
evidence remains ineligible until its closure certificate has an equivalent
compact retirement representation. Application removes only the validated
detailed evidence, retains the checkpoint commitment, and appends a compact
per-PG generation certificate containing the transition, generation, artifact
tuple, every ordered historical publication binding `(actor, target epoch,
transfer proof, evidence digest)`, ordered tombstones, and tombstone-set digest.
This preserves publication semantics across any number of lightweight epoch
proof rebases without retaining the complete detailed evidence bytes. The
publication bindings are bounded by the staging store's per-intent epoch-proof
limit, each checkpoint member reconstructs from its exact compact binding, and
the final target set must still match the durable destination-install receipt.
Snapshot validation builds one bounded actor-target publication index before
checkpoint reconstruction instead of rescanning a certificate for each member.
Older certificates remain durable when the maximum floor advances. Exact replay
of any finalized generation is a no-op, stale evidence at or below the maximum
floor rejects, and application is cluster-map-epoch neutral. Snapshot
validation reconstructs every certificate against its exact retained
transition, authorization, node identities, checkpoint page membership, and
commitments, so coordinated resealing cannot invent evidence or a
command-unreachable floor. Standalone restart, snapshot round-trip, multiple
same-PG generations, old and current exact replay, publish-at-E1 then
rebase/install-at-E2 finalization, historical-proof mutation, authorized but
unobserved predecessor rejection, partial/uncheckpointed rejection, stale-page
rejection, and direct/coordinated certificate-forgery cases are covered. A
three-voter Raft
composition additionally covers replication to
every voter and exact cleanup replay after leadership transfer. Storage RPC
v26 exposes committed-authorization-bound exact-intent tombstoning, durable
physical artifact deletion, canonical tombstone evidence, and exact replay
over local, authenticated Unix, and authenticated TLS paths. Manager-driven
all-destination tombstone orchestration now retains the staged transfer owner,
replays the exact committed authorization independently to every destination,
resolves each current cleanup actor from the exact completed authority
snapshot, validates each returned actor and intent, and constructs the
canonical sorted finalized-floor request only after the complete obligation
set succeeds. Destination incarnation or endpoint rollover therefore cannot
pin retries to the pre-install route. A partial failure leaves earlier durable
tombstones replayable and later destinations untouched, so retry converges
without narrowing the obligation set. Production-worker handoff and
finalized-floor advancement are active. Pre-install cancellation is also
active once a direct successor transition durably consumes the authorized
transition tip, but only while both durable `destination_epoch` and
`destination_install` remain absent. An installed predecessor requires its
separate post-install recovery path and cannot be destructively treated as a
pre-install cancellation. The exact successor epoch is the cancellation certificate,
every actor in the original authorized destination set must publish a
tombstone even when it reports no local intent, and the cumulative finalized
floor records the superseded disposition before old evidence can be pruned.
Delayed creation or publication under the cancelled generation is rejected by
the destination tombstone. Fenced-incarnation retirement substitutes remain
gated.

Storage RPC v27 adds committed-authorization-bound exact-artifact retrieval.
The request carries the same complete authority presentation and exact intent
as publication and tombstoning plus an exact offset and bounded length; both
the storage-node handler and staging store verify the destination-specific
capability before reading bytes. Each independently authenticated response is
bounded to 1 MiB. Responses use one protocol-wide 60-second authentication
window, independent of endpoint-local I/O timeouts; transport limits cannot
exceed that bound, and heterogeneous profiles therefore accept the same signed
response language. One absolute artifact deadline covers admission,
connection, and every chunk rather than granting a
fresh transport timeout per chunk. The client applies the operation-specific
authenticated-envelope cap before allocating the response body, then
reassembles the chunks and revalidates the complete exact length and SHA-256
digest against the durable intent before decoding or import.
Retrieval is admin-only, succeeds over authenticated Unix and TLS,
fails closed for malformed, stale, cross-member, absent, or tombstoned state,
and leaves immutable v26 request frames as rejection evidence. This closes the
durable source needed for production reconciliation to resume after authority
or worker restart without re-exporting from the historical primary. Staging
store, artifact, command, control-plane state/RPC, and authentication-envelope
versions are unchanged because retrieval adds no durable semantics.

The restart-safe ownership prerequisite now removes the staged transfer's
dependency on its original in-memory preparation. After destination install,
storage reconstructs the exact authorization and install from the retained
transition, upgrades one capability per destination from the complete durable
batch receipt, and retrieves canonical bytes from any authorized destination.
The decoder independently rechecks the intent digest and length and the full
artifact grammar. Because proof rebasing deliberately leaves the original
artifact bytes unchanged, reconstruction derives the imported proof at the
durable install epoch and requires it to equal the retained install proof; it
does not confuse the artifact's first encoded target with the later rebased
target. The reconstructed linear owner retains only the exact work binding,
intent, destination capabilities, decoded artifact, install epoch/proof, and
publication commitments needed for import, activation, and cleanup. Proof
republishing resolves current authority-certified destination identities, so a
resumed owner does not retain the pre-install endpoint or incarnation. Composed
coverage drops the original prepared owner after an unrelated epoch rebase,
reconstructs from destination storage, imports, activates, and completes the
existing all-destination cleanup path. The production worker remains on the
singular wrapper until the atomic state-machine handoff consumes this owner.

The pre-install owner is restart-safe from the authorization commit boundary,
including before the first destination has fsync-complete artifact bytes. The
durable authorization retains the exact source epoch, initial target epoch,
artifact digest, and length. Recovery first accepts any exact destination copy;
when every destination is absent it re-exports from the still-fenced retained
source route, re-encodes for the authorization's original target epoch, and
requires the resulting request to match the committed digest and length before
publishing any bytes. If a possible destination holder has a transient read
failure, a failed or mismatched source re-export cannot override that
uncertainty. A missing or unpublished copy is not authority to alter the
artifact: staging replay sends recovered canonical bytes to the complete
destination set, and every destination must accept the exact intent before
install. Observed authentication, protocol, or destination-artifact integrity
failure remains fatal. Artifact publication resolves current authority-
certified destination identities rather than retaining the preparation-time
route. Retryable publication drops volatile ownership, records per-PG deferral,
and lets the global scan continue. Install-preparation rejection and retryable
terminal finalization follow the same rule: volatile owners are released,
retry state is keyed by exact PG, transition epoch, and durable reconciliation
phase, and durable scan rediscovery proceeds without blocking unrelated or
successor PGs. A leader returning with stale transfer quarantine must therefore
accept a cleanup phase durably completed by another leader. Durable
authorization reconstructs the next
attempt. Composed coverage includes both zero-copy process loss with exact
source re-export and first-copy-only recovery after the historical source is
lost.

Destination-install uncertainty follows the same durable-owner rule. Before a
staged owner is rebased, the worker checks the current snapshot for its exact
durable install. A committed install is reconstructed and imported at its
original epoch; an absent install may be rebased and retried. Retryable or
ambiguous install errors drop volatile ownership and defer only that PG, so a
lost response cannot turn a committed prefix into a conflicting rebase and one
failed PG cannot monopolize reconciliation. Definitive authorization rejection
also discards the stale prepared batch and rederives from current durable state;
only observed fatal integrity or durability failures quarantine the transition.

Command v29 and state v41 implement the first bounded segment-retirement
step. `CollapseMetadataTransferStagingEvidenceCheckpointSegment` performs an
exact CAS over the actor incarnation, generation range, and canonical source
segment digest. It requires every commitment in that segment to have been
pruned through a matching finalized-generation certificate, then atomically
replaces the segment with a compact anchor retaining its predecessor, exact
tip apply receipt, range, and source digest. Each finalized checkpoint binding
retains the original page generation and entry sequence, allowing snapshot
validation to reconstruct every retired page digest, apply receipt, and the
complete source-segment digest from canonical finalized evidence. Exact replay
through the anchor is a no-op; stale digests, partial coverage, retained detail,
and coordinated anchor, binding, or membership forgeries reject without mutation. The operation does not
advance the cluster-map epoch. Standalone tests cover mixed and fully covered
segments, restart, exact replay, and mutation cases; a three-voter Raft test
covers replication, leadership transfer, and replay of checkpoint, collapse,
authorization, installation, and finalization commands after collapse.
Command v30 and state v42 implement bounded adjacent-anchor coalescing.
`CoalesceMetadataTransferStagingEvidenceCheckpointAnchors` performs an exact
CAS over the actor incarnation, generation range, and canonical finalized
source-segment count and digest. Each mutation consumes between two and 64
adjacent retained anchors whose predecessor and apply-receipt links form one
contiguous chain, then atomically replaces them with one anchor retaining the
lowest predecessor and newest tip. Recursive merges remain grouping-independent
because their durable identity is the original finalized leaf-segment vector,
not the immediately consumed anchors. Exact checkpoint and collapse replay for
one absorbed leaf, and semantic coalescing replay for a canonical range already
covered by a stronger merged anchor, are epoch-neutral no-ops; stale ranges,
forged source vectors, gaps, overlaps, and wrong actor identities reject without
mutation. Snapshot validation reconstructs every leaf segment and its receipt
chain from finalized canonical evidence before accepting the cumulative anchor
commitment. Standalone restart and three-voter Raft coverage include recursive
coalescing, replication to every voter, leadership transfer, and replay of
checkpoint, collapse, coalescing, authorization, installation, and cleanup
commands. Command v29/state v41 and all earlier fixed vectors remain immutable
rejection evidence. Closure-certificate retirement remains a later slice.

Command v31 and state v43 implement exact actor-closure certificate
retirement. `RetireMetadataTransferStagingActorClosure` compare-and-swaps the
source actor identity and canonical certificate digest, copies the complete
certificate into a distinct retired set, and leaves the cluster-map epoch
unchanged. Exact replay is a no-op; absent, stale, or divergent certificates
reject before mutation. Only an exact retired certificate permits a
candidate-bearing destination genesis page to enter a checkpoint. Checkpoint
page links and finalized checkpoint bindings retain the canonical closure
candidate, so later finalization, segment collapse, and anchor reconstruction
continue to reproduce the original page digest after detailed evidence is
pruned. Snapshot validation builds one compact page/segment/anchor actor-chain
index and independently revalidates every retired certificate's source tip,
destination genesis, and complete rebound prefix against that index; it does
not rescan all retained evidence once per incarnation. Later evidence-page
application merges exact retired certificates back into the reconstructed
active closure set, so unrelated publication cannot discard retirement
authority. Once the candidate-bearing destination genesis is checkpointed,
the exact retired certificate remains the rollover provenance for later raw
pages already retained from that destination actor; snapshot validation
resolves it through the certificate's independently validated
checkpoint/anchor chain rather than requiring the removed raw genesis page.
Command admission does not use retirement as authority for new finalized
evidence: retirement requires the complete rebound prefix to have already been
admitted. If a node incarnation advances after a staging generation is
finalized, its closure genesis must semantically replay the complete finalized
prefix as well as every still-live row. The authority validates those rows
against the exact finalized certificate without restoring detailed evidence;
an omitted finalized member rejects once the committed prefix boundary is
observable. Checkpoint bindings for such replay rows retain the exact rebound
actor endpoint and closure candidate so later segment and anchor reconstruction
remains canonical. The immutable command v30/state v42 aggregates and their
snapshot, standalone-journal, Raft-peer, restart, WAL, and storage-RPC
containers remain exact rejection evidence. A bounded production maintenance
cursor now schedules these existing epoch-neutral primitives through both
standalone and Raft authorities. Each reconciliation poll examines at most 16
records in each independent closure, page, segment, and anchor catalogue and
submits at most one low-priority command. Cursor progress is committed only
through the selected or actually examined record and only after durable command
success. Each catalogue sweep captures a fixed high-water key and wraps after
reaching it, independently of records appended later by the same or another
actor; mutable page, segment, and anchor boundaries are therefore reconsidered
when a successor or finalized floor can make them eligible. The starting phase
rotates after every selection, so sustained page publication cannot starve
segment collapse or anchor coalescing. Coalescing that consumes the captured
anchor high-water key completes that sweep before the replacement anchor is
revisited, so a removed ceiling cannot exclude later successors. It retires an exact closure before checkpointing
a closure-bearing genesis, never consumes an open actor tip, collapses only
fully finalized segments, and coalesces only adjacent validated anchors. Typed
leadership, transport, and authority-clock gaps use an independent retry
backoff and do not suppress PG transfer or activation polling; integrity,
durability, protocol, and fatal Raft outcomes propagate to the service loop and
fail stop. Standalone production-path regressions cover open-tip retention,
rollover ordering, bounded same-actor and cross-actor sweeps with continuously
appended higher generations, deferred-segment reconsideration, phase fairness,
collapse, coalescing, epoch neutrality, and fatal-versus-retryable maintenance
classification; the three-voter staged-transfer
composition drives checkpointing through the same Raft maintenance entry
point. This closes retention and closure-retirement scheduling before the
worker begins creating staged production evidence.

This cleanup CAS is intentionally one per PG, not a fifth multi-PG batch
command. Epoch-neutral cleanup does not need batching to amortize cluster-map
epoch changes, and independent commands avoid cross-PG failure coupling. The
manager may execute a bounded number concurrently, with the same queue and
encoded single-command limits as other control-plane maintenance, but each
command has its own PG-local replay identity and succeeds or fails
independently. Introducing a cleanup batch later would require its own explicit
count and byte bounds, all-or-nothing validation, canonical whole-batch receipt,
and stale-member splitting protocol.

A destination cleanup receipt may be replaced only by an exact durable
fenced-incarnation retirement certificate. That certificate identifies the
authorized node, incarnation, endpoint and storage-authentication generation,
transition, staging generation, and artifact tuple, and proves that certified
disk/incarnation loss or replacement has made every credential and endpoint
capability capable of issuing stale staging RPCs permanently unusable. Its CAS
must reject a merely offline, lease-expired, or potentially returning actor,
and must compose with node replacement and credential revocation before it can
satisfy cleanup. The certificate itself remains as the cleanup evidence for
that actor; it does not claim that bytes were physically removed from a disk
that may return under the retired identity.

Once every obligation has either its exact cleanup receipt or a valid
fenced-incarnation retirement certificate, the CAS advances the floor, prunes
detailed authorization and receipt state atomically, and retains a compact
floor-derived cleanup certificate containing the generation, complete
obligation set, cleanup receipt-set digest, and retirement-certificate digest.
An offline destination returning after partial cleanup therefore uses the
retained exact authorization to tombstone and remove its artifact before global
pruning can occur, while a certified permanently lost destination cannot retain
unbounded control-plane state.

Before the first destination write, the transition owns a durable staging
intent. Its staging generation is not caller-selected: it is exactly the
transition epoch, which is globally monotonic and therefore monotonic for each
PG. Batch begin derives that value, and both live-state and snapshot validation
require equality. After detailed transition and receipt history is pruned, the
control-plane per-PG finalized staging-generation floor remains and every
successor transition must have a greater epoch. The staging-authorization
command binds that generation to the artifact tuple, and each destination
commits the exact `CreateStagingIntent` CAS before accepting artifact bytes.
Stage, publish, import, and cleanup operations compare-and-swap the complete
transition, generation, digest, length, and format tuple. Install atomically
consumes the matching control-plane intent; it cannot refer to bytes from
another preparation attempt. Terminal cleanup first durably tombstones the
generation, then unlinks artifact bytes and fsyncs
the directory. A delayed stage or publish RPC for a tombstoned or lower
generation is rejected and cannot recreate an artifact after cleanup. Retain a
bounded per-PG finalized-generation floor after detailed tombstone pruning so
old requests remain rejected without accumulating one permanent row per
transition.

Thus leader or process failover after installation retrieves the same bytes
from destination staging rather than attempting to reconstruct them from the
route proof. Stage/read/remove operations require a dedicated,
transition-scoped storage capability and enforce aggregate byte and object
limits as admission backpressure.

If the exact source is lost before all staging receipts exist, no route install
is allowed: the PG remains fenced and deferred until the source returns or a
separately certified successor transition proves another exact source. Once
all destination staging receipts exist, source loss does not prevent import.
Partial staging and rejected/stale install batches retain exact ownership for
bounded retry. Pre-install cleanup requires an exact control-plane cancellation
or supersession certificate for the staging intent and CAS-tombstones its
generation on every destination in the staging authorization, including actors
for which no intent or receipt was observed; absence of an install receipt is
never sufficient authority.
Successful staging is removed only after activation and durable confirmation
that every destination imported the exact artifact. Cleanup is idempotent,
recoverable, and cannot race delayed writes because the tombstone precedes
physical removal. Tests must cover source loss before and after staging, leader
failure after install but before import, cleanup racing a delayed stage and
publish response, partial destination import, stale-batch cleanup, and
destination loss during each phase. Prepared and staged artifact bytes must
have aggregate bounds and must not accumulate unboundedly in manager memory or
destination storage.

Replace the manager's singleton in-flight state with bounded, per-PG stage
queues for begin candidates, transfer preparation, prepared installation,
destination import, and activation. Use bounded transfer concurrency and
per-PG deferral/quarantine state; four transfer workers is an appropriate
initial limit, subject to measured storage pressure. Flush a non-empty partial
batch after a short bounded delay so a final underfilled batch cannot wait
indefinitely. Do not begin more transitions than the configured active
transition, transfer-worker, and prepared-artifact capacity can retain. A
failed member must not discard already prepared work for unrelated members or
monopolize the scanner.

Deferral and quarantine identity is the exact
`(PG, transition epoch, durable reconciliation phase)`, not the PG or
transition alone. Batch discovery carries retained predecessor cleanup as an
ordered fallback beside active successor work. The successor remains primary
while it is runnable; if it is deferred, quarantined, or definitively rejected,
the worker may run the predecessor cleanup without clearing the successor's
retry state. A durable phase advance likewise supersedes stale process-local
quarantine from an earlier phase. This prevents either side of the same-PG
lineage from masking the other or deadlocking bounded staging capacity.

The production reconciliation worker now owns the complete staged lifecycle:
prepare, plural authorization, destination staging and evidence publication,
plural receipt-bound install, import, plural activation, destination
tombstoning, and finalized-floor advancement. One bounded scan page owns an
exact per-PG in-flight map and stage queue, while four transfer workers may
prepare and stage independently before successful members accumulate into
canonical install and activation batches. Retryable and fatal failures defer or
quarantine only their exact transition. Restart discovery resumes authorized,
installed, and cleanup-owned transitions from durable control-plane and
staging-store state rather than reconstructing a second owner. A worker panic
remains process-fatal, so loss of one lane cannot silently reduce capacity or
strand accepted work; standalone and Raft service loops observe worker health
before authority-time operations that may defer reconciliation. Composed
production coverage prepares two real transfers at one destination epoch,
forces one to install first, and requires the other to rebase and import before
both join one canonical activation batch.

Authorization response uncertainty retains the complete prepared owner and
replays the exact authorization batch; it never discards the sole artifact
bytes. After authorization, a retryable staging failure retains the authorized
owner until at least one exact destination artifact is durable. Active or new
outage work for a PG outranks terminal cleanup from retained predecessor
transitions while runnable, so an offline old cleanup actor cannot hide a
recoverable successor. A deferred or blocked successor yields to its retained
cleanup fallback, so the successor cannot in turn pin predecessor capacity.
Destination installation uses the production OpenRaft encoder to
select the largest fitting canonical prefix, rejects a legal singleton that
cannot fit, and leaves the unsubmitted suffix owned for epoch rebasing after
the prefix commits.

Begin, install, and activation batches must all use Raft's gated command
derivation primitive. Command construction reads the effective
durable-plus-heartbeat-overlay snapshot while holding the heartbeat update
gate, clamps authority time to that snapshot's committed high-water mark, and
retains the guard through proposal dispatch. Activation readiness is derived
inside that critical section from every destination's exact incarnation,
endpoint, lease, route, and proof. A renewal after a batch was prepared rejects
the stale command and requires rederivation; it never relaxes exact lease
equality.

This slice changes both command grammar and command-reachable durable state.
The receipt prerequisite advanced the control-plane command version from v19
to v20 and state version from v31 to v32. The first plural protocol boundary
advances begin/completion commands from v20 to v21 and durable receipt state
from v32 to v33, retaining immutable v19/v31 and v20/v32 rejection evidence
and updating the nested journal, Raft WAL, snapshot, aggregate, and retained
batch-receipt vectors. The epoch-neutral staging-authorization boundary
advances command v21 to v22 and state v33 to v34, retaining immutable v21/v33
aggregates and exact nested-container rejection evidence. The evidence-apply
prerequisite advances command v22 to v23 and state v34 to v35, retaining
immutable v22/v34 evidence. The plural destination-install slice advances
command v23 to v24 and state v35 to v36, retaining immutable v23/v35
evidence. That v24 slice made receipt-bound plural installation mandatory for
the new command while temporarily retaining the singular unavailable-
transition operation used by the production worker. The later staged-worker
handoff retires that path atomically when the worker begins consuming staging
authorization and evidence; it is not removed in an intermediate commit that
would disable outage recovery. The transition-scoped artifact staging
operations advance
storage RPC again from v23 to v24 for semantic artifact format v2 and the
epoch-bound proof-publication operation. Fixed v22/v23/v24 frame evidence,
authenticated Unix/TLS coverage, and explicit
exclusion from ordinary frontend, storage-node, and maintenance capabilities.
All three staging-operation admission bounds include the committed epoch,
complete batch digest, length-prefixed authorization command, and their
operation-specific fields; encoder-derived exact-bound fixtures prevent any
accepted authorization from exceeding its configured transport envelope.
The composed prepared-to-import lifecycle changes the accepted historical
artifact epoch semantics and therefore advances the storage-owned staging
store, catalogue, artifact, evidence page, and apply-receipt formats from v2
to v3. Storage RPC advances from v24 to v25, command encoding from v24 to v25,
and logical state from v36 to v37 so every containing reader rejects the old
semantics before dispatch or construction. Immutable v2, storage-RPC-v24,
command-v24, state-v36, replicated-snapshot, and standalone-journal fixtures
remain exact rejection evidence. No compatibility reader is added.
Receipt-evidence publication crosses the control-plane RPC boundary. Its v3
evidence grammar advances control-plane RPC from v20 to v21 while retaining
authentication envelope v2. Preserve complete v17/v1, v18/v2, v19/v2, and
v20/v2 rejection evidence and fixed v20 frames for a genesis page and a
successor carrying a non-genesis
`previous_apply_receipt_digest`, minimum, maximum-count, maximum-byte,
multipage, generation-gap, exact-replay, and tombstone/finalized-floor
evidence. Frame-limit constants and exact-boundary fixtures include the full
predecessor digest. Omitted, truncated, genesis-on-successor, and incorrect
digest fixtures must fail before dispatch without replacing the retained apply
receipt or mutating evidence. Authentication envelope v2 adds distinct
storage-node request and control-plane response operations so neither side can
reuse a generic heartbeat or runtime-map capability. Batch transition commands remain leader-internal
and require no additional
control-plane RPC operation beyond that receipt protocol.

Actor-incarnation chain closure advances the storage-owned staging store,
catalogue, evidence, page, and apply-receipt formats from v3 to v4 while the
immutable staged-artifact format remains v3. The closure-bearing page grammar
advances control-plane RPC v21 to v22; applying that page has new replicated
semantics, so command v26 advances to v27 and logical state v38 advances to
v39. Immutable store/evidence v3, RPC v21, command v26, and state v38 bytes
remain exact rejection evidence in their direct, authenticated, snapshot,
journal, Raft peer, restart, WAL, and compaction containers. Authentication
envelope v2 is unchanged, and no compatibility reader is added.

Finalized-floor cleanup adds command tag 22 and cumulative compact per-PG
generation certificates, including every bounded historical epoch-publication
binding needed to reconstruct pruned proof evidence. Checkpoint links
additionally retain canonical page membership so pruned evidence remains bound
to the page-chain digest. These
changes advance command v27 to v28 and logical state v39 to v40. The
immutable command-v27 and state-v39 aggregates and their snapshot,
single-authority journal, Raft peer, restart, WAL, compaction, and storage-RPC
containers remain exact rejection evidence. Storage staging format v4,
artifact format v3, storage RPC v25, control-plane RPC v22, and authentication
envelope v2 are unchanged because this slice adds no storage mutation or wire
operation. No compatibility reader is added.

Destination tombstoning advances storage RPC v25 to v26 by adding one
admin-only operation carrying the same untrusted complete staging-
authorization presentation and exact intent as the other destination staging
mutations. The storage node promotes that presentation only after exact
authority-state comparison for the destination `(node, PG)` member, then the
staging store repeats the committed-authorization check before deleting the
artifact and committing canonical tombstone evidence. Publication and
tombstone receipts have distinct validated kinds and cannot substitute for
one another. Exact v26 intent, artifact, proof, and tombstone frames are fixed;
v25 remains immutable rejection evidence, and authenticated Unix/TLS tests
cover physical deletion plus exact tombstone replay. Staging-store format v4,
artifact format v3, control-plane RPC v22, command v31, state v43, and
authentication envelope v2 are unchanged because their existing grammars
already represent the resulting tombstone evidence and authorization.
Durable artifact retrieval then advances storage RPC v26 to v27 with one
admin-only read operation carrying the same complete committed authorization
presentation, exact intent, offset, and one-MiB maximum chunk length. Exact v27
frames cover all five staging operations, and complete v26 frames remain
immutable rejection evidence. The chunk response payload has fixed byte
evidence; authentication tests cover a maximum chunk verified beyond five
seconds, while transport tests cover a delayed signed chunk, rejection before
allocation above the response envelope cap, and one absolute multi-chunk
operation deadline.

The staged worker handoff is now complete. Command v32 removes the optional
unavailable-transition binding from ordinary metadata-transfer tag 7, leaving
plural receipt-bound tag 20 as the only command that can install an unavailable
transition. The same uncommitted command-v32 grammar now binds each staging
authorization to its exact prepared artifact target epoch and records terminal
cleanup as either completed or superseded by one exact direct successor.
Control-plane RPC v23 removed the corresponding singular runtime-
map install operation and its authenticated operation catalogue entry. State
v44 persists those target epochs and cleanup dispositions and permits finalized
cleanup to retain the exact raw page-tip detail when no
successor yet makes that tip checkpointable; a later checkpoint atomically
replaces the detail with exact page-membership bindings. Immutable command-v31,
RPC-v22, and state-v43 aggregates remain rejection evidence, including nested
journal, Raft WAL, peer, restart, snapshot, and storage-RPC containers. The
current coordinated version vector is staging-store/evidence/page/apply-receipt
v4, staged-artifact v3, storage RPC v27, control-plane RPC v25, command v33,
state v45, and authentication envelope v2.

Durable artifact staging uses a separate storage-owned format rather than
silently extending the PG schema. Staging-store format v4 owns the
versioned root manifest, initialization-complete marker, outer establishment
marker in the storage data directory, generation catalogue, durable singleton
evidence actor,
content-addressed artifact files, published receipts, import status, tombstones, and per-PG finalized
generation floors. The catalogue also persists pending receipt/tombstone
deltas, the exact assigned in-flight evidence operation-payload bytes and
digest, and the canonical authority-neutral apply receipt through atomic delta
retirement. Fresh RPC authentication envelopes are never persisted as replay
material. Publication writes a generation-scoped temporary file,
fsyncs and validates its exact length and digest, atomically renames it, fsyncs
the containing directory, then commits and syncs the catalogue state before
issuing a receipt. Existing-file retry and startup recovery repeat the artifact
and directory durability fence before committing a recovered receipt. Receipt
evidence independently retains the producing node ID, incarnation, and
endpoint so restart validation does not trust identity fields copied only
inside the receipt. A newer same-node incarnation atomically rebinds all
retained evidence and resets the page chain only after this validation; stale
store handles and same-incarnation endpoint changes are rejected. Startup opens
an existing catalogue nonblocking with
`O_NOFOLLOW`, requires a regular file before SQLite admission, and validates
the manifest and complete catalogue/file
inventory before serving staging RPCs: unknown versions, digest or length
mismatch, missing published files, generation regression, and contradictory
receipt/tombstone state fail closed. Bounded startup reconciliation removes
unpublished temporary files, completes tombstone-directed unlink and directory
sync, and quarantines unexplained final files rather than authorizing them.
Store admission accounts for temporary, published, and tombstoned cleanup
bytes. Retain immutable v1-v3 rejection fixtures and fixed current v4 manifest,
initialization and establishment markers, catalogue, proof-bearing receipt,
page, apply receipt, closure candidate, and crash-state corpus, plus the
independently versioned current v3 artifact, in the
[storage format ledger](../guides/storage-format-ledger.md); no
upgrade decoder is required while the repository supports one format at a
time.

Required deterministic and generated coverage includes:

- one global epoch advance for each begin, install, and activation batch;
- zero mutation when any member is stale or invalid;
- exact whole-batch replay and rejection of partial or mixed replay;
- subset, superset, reordered, and regrouped replay after snapshot and log
  compaction;
- empty, duplicate, unordered, oversized, and over-frame-limit batches;
- production-encoder splitting at count and encoded-byte boundaries, including
  maximum acting sets and endpoint lengths;
- identical count and encoded-byte splitting, decoder rejection, and legal
  single-entry fit coverage for staging-intent authorization batches;
- coordinated destination-install/staging-version rejection of every singular
  unavailable-transition command path, including removed RPC kind 19 and
  ordinary metadata-transfer tag 7, plus singleton operation through each
  plural worker/admin entry point;
- multiple PGs sharing transition and destination epochs;
- heartbeat renewal between batch preparation and gated derivation;
- proof preparation and stale-epoch recomputation without artifact work under
  the heartbeat gate;
- partial transfer failure, bounded retry, and preservation of successful
  prepared work;
- authorization response loss and process loss before the first destination
  write, proving exact digest-checked source re-export from durable authority;
- retryable first-destination publication failure, proving the failed PG leaves
  foreground scheduling while unrelated recovery continues;
- committed destination-install response loss, proving exact installed-state
  recovery precedes rebasing, plus definitive regrouped-authorization rejection
  followed by current-state rederivation;
- retained predecessor cleanup concurrent with a newer outage for the same PG,
  proving active recovery is selected without abandoning terminal cleanup;
- destination-install splitting at the exact production OpenRaft byte ceiling,
  including legal-singleton fit and suffix rebasing after prefix commit;
- durable artifact retrieval and exact-byte import after leader/process loss,
  with source and destination loss at every staging boundary;
- independently verified committed staging receipts, forged receipt fields,
  stale incarnation/endpoint evidence, and incomplete fsync scope;
- queued-delta, assigned-page, and acknowledged-page restarts across an actor
  incarnation advance, proving atomic evidence rebinding, genesis reset,
  successful control-plane application, stale-handle fencing, and retention of
  independently attributable old/new actor chains;
- two-actor snapshot forgeries where a page contains another actor's evidence,
  same-actor detailed evidence is absent from every retained page, a page actor
  is from a future incarnation, or an equal incarnation has a different endpoint;
- receipt-page count and encoded-byte limits, generation gaps, page replay,
  backlog isolation from lease renewal, and compacted historical-page rejection
  after the control-plane finalized-generation floor advances;
- segmented mixed-PG page checkpointing with exact count and production-encoded
  byte boundaries, mandatory single-page fit, independent compaction of a later
  segment while an earlier segment retains an unresolved member, independent
  floor advancement for one member, retained proof for the other member,
  segment-to-segment and segment-to-page successor validation, per-segment
  collapse, and incrementally bounded coalescing of adjacent covered tip-only
  anchors at exact count and encoded-byte boundaries;
- genesis and successor predecessor-receipt fields, authentication failure
  after digest tampering, and admission rejection for omitted, malformed,
  genesis-on-successor, or incorrect digests without evidence mutation or
  retained-receipt replacement; page-builder tests consume only the opaque
  durable receipt token;
- crashes before exact page persistence, after transmission, after
  control-plane commit, after response loss, and before and after durable local
  acknowledgement, proving byte-identical operation-payload replay under fresh
  authentication and ordered delta retirement, including a restart delayed
  beyond the original authentication freshness window;
- exact apply-receipt replay after Raft snapshot and log compaction and after a
  standalone journal restart, with backend log positions changed or absent;
- a cleanup-evidence page committed with its response lost, followed by an
  attempted finalized-floor CAS that proves the current tip and its detailed
  evidence remain unpruned across snapshot or journal compaction and authority
  restart; exact replay must return the retained apply receipt and permit
  durable outbox retirement, after which a successor page citing that receipt
  is accepted and only then may the former tip be checkpointed and its covered
  detail pruned in both Raft and standalone modes; a no-successor branch must
  remain bounded to the one complete replayable tip and reject pruning;
- response loss followed by actor-incarnation rollover, proving the old tip
  remains unpruned until a new-format genesis and exact actor-chain closure CAS
  bind complete semantic evidence rebinding and old-actor fencing; malformed,
  partial, or absent closure evidence must retain the replayable old chain;
  this includes page N accepted, page N+1 durably assigned but never
  dispatched, and exact selection of N rather than the ambiguous successor;
- exact actor endpoint binding across A-to-B and A-to-B-to-C rollover,
  rejection of a foreign `through_actor` endpoint, checkpointing of a closed
  source tip, and rejection of candidate-bearing destination-chain
  checkpointing until certificate retirement;
- incomplete multi-page closure prefixes with valid authority defer, while a
  foreign currently observable `through_actor` endpoint rejects the genesis
  without state mutation; zero/impossible cardinality, excess observed members,
  and a short prefix that has reached its committed maximum also reject;
- blocked and saturated evidence connections, workers, and Raft proposals while
  lease deadlines continue to advance and the global cluster-map epoch remains
  unchanged;
- partial cleanup with one destination offline, its later authenticated return
  and cleanup, rejection of premature floor advancement, and exact all-actor
  floor advancement with replay through the compact cleanup certificate;
- pre-install cancellation where one authorized destination reports no intent
  and another committed staging but lost its response, proving both durably
  tombstone before the finalized floor advances; the same disposition rejects
  once either durable destination-install field exists, even if completion is
  absent;
- permanent destination loss using an exact fenced-incarnation retirement
  certificate after credential and endpoint retirement, plus rejection while
  that actor is merely offline, expired, or capable of returning;
- multiple independent epoch-neutral cleanup CAS operations across different
  PGs, including concurrent success and one stale failure, proving receipt,
  retirement-certificate, floor, pruning, and compact-certificate mutations do
  not change the global cluster-map epoch or couple PG outcomes;
- competing staging-intent authorization and destination creation with
  mismatched artifact digest, length, format version, or encoded target epoch,
  including restart retrieval from an otherwise checksum-valid surviving copy;
- an active successor deferred and quarantined in turn while its retained
  predecessor cleanup remains selectable and the successor's exact retry state
  remains intact;
- leadership returning with stale transfer quarantine after another leader
  durably completes the same transition, proving terminal cleanup remains
  selectable for that exact PG and transition;
- cleanup before install, delayed stage and publish after cleanup, generation
  reuse, tombstone restart, and finalized-generation-floor rejection;
- staging-store publication crashes before and after file fsync, rename,
  directory fsync, catalogue commit, and receipt issuance;
- existing-file and startup publication recovery preserve the same directory
  fsync-before-catalogue ordering, special catalogue files fail before SQLite,
  and same-length node/incarnation/endpoint receipt mutations disagree with
  independently persisted historical actor identity;
- first initialization resumes before catalogue creation and after exact v1
  catalogue commit; root creation is parent-directory-synced before either
  marker or any receipt, and an outer established marker makes missing root,
  missing/truncated catalogue, or version-zero catalogue state rejection-only
  without recreating the root or losing retirement state;
- restart and leader failover before and after every batch boundary;
- stale batch rejection after a successor transition;
- normalized equivalence between sequential single-PG model transitions and
  batched state; and
- a four-host outage release gate proving epoch growth is proportional to
  batch count rather than affected-PG count while unaffected PGs continue to
  serve.

Record submitted, applied, replayed, and rejected batch totals; entry counts
and encoded-byte fill ratios; queue, prepared-artifact, and durable-staging
depth and bytes; per-stage latency and deferrals; receipt retention/pruning;
and global epochs consumed per recovered PG. These metrics are part of the
release evidence, not optional diagnostics.

The first observability slice is implemented. Control-plane RPC v24 exposes
fixed-order begin, authorization, install, and activation batch counters,
member and replication-entry byte totals/maxima, encoded fill in parts per
million, epoch advances, and recovered-PG totals. The reconciliation worker
publishes bounded per-stage success/deferral/fatal latency counters plus exact
queue, in-flight, prepared-artifact, install, activation, finalization,
deferred, and blocked gauges. Storage-local metrics expose durable staging
entry and reserved-artifact byte usage. Authority diagnostics additionally
expose retained page, segment, anchor, detailed-evidence, finalized-floor,
active-closure, and retired-closure depth plus state-changing pruning totals;
those gauges are restored from durable state on authority open or snapshot
installation. Submission accounting occurs only at
the standalone/Raft proposal boundary, while apply/replay/reject accounting
occurs only after durable state-machine classification, so speculative command
construction cannot inflate release evidence. The four-host outage release
gate and its quantitative epoch/fill assertions remain the next slice.

The first implementation may use the committed static topology and the existing
metadata-transfer and payload-backfill primitives. It does not depend on adding
or removing nodes dynamically and is not capacity rebalancing. It must:

- select replacement actors deterministically from committed eligible nodes,
  failure-domain policy, current availability, and the exact source acting set.
  Selection must call the storage-owned placement operation defined by the
  immutable capacity-weight plan: use the retained derivation version and
  stable PG key to rank the complete committed weighted policy, then filter
  unavailable, already-acting, and failure-domain-conflicting nodes without
  renormalizing the transient eligible subset. The controller must not inspect
  weights, reconstruct placement nodes, reread the static manifest, or fall back
  to node-ID ordering;
- persist one idempotent transition identity containing the topology generation
  and digest, placement derivation version, source PG epoch/state/acting set,
  destination acting set, reason, and exact failure or planned-maintenance
  authorization;
- authorize an unplanned-outage transition with the unavailable node identity
  and incarnation, exact accepted lease/availability observation, and
  authority-clock grace cutoff;
- begin an unplanned migration only through one replicated control-plane
  compare-and-swap that verifies all transition fields are still current,
  authority time has crossed the exact grace cutoff, the observed incarnation
  has not renewed, no unexpired maintenance suppression covers it, and every
  destination remains eligible, then atomically records the transition and
  fences the source PG into `Peering`;
- begin a healthy-source `migrate-before-stop` transition only through a
  separately authorized compare-and-swap bound to the exact durable maintenance
  record, departing incarnation, topology, source PG, and destination, without
  pretending that its lease expired;
- reconcile affected PGs from a durable, cursor-based queue under one logical
  owner, with bounded concurrency, bytes in flight, retries, and per-node/PG
  fairness rather than issuing one global migration burst;
- fence the source route, reconstruct or transfer metadata from certified
  available replicas, and install the destination route as `Peering`;
- keep every destination PG in `Peering` until both the existing exact metadata
  proof is satisfied on its required destination replicas and every destination
  actor is leased, fenced to that route, and capable of accepting its assigned
  payload shard, so any new write can commit all `k + m` shards;
- retain the source placement in authenticated route history and copy or
  EC-reconstruct historical payload asynchronously after activation, without
  making outage duration proportional to the amount of stored payload;
- retain route history, pending-command recovery authority, reservations, and
  cleanup roots until every transition dependency has durably cleared;
- expose backlog depth, oldest age, active transitions, bytes, retry cause, and
  blocked PGs, and apply explicit admission backpressure when queued demand
  exceeds bounded worker capacity or remaining eligible failure-domain
  capacity falls below its configured safety minimum; and
- resume exactly after authority failover, process restart, response loss, or
  repeated unavailable/healthy observations without duplicating migration.

Published pending commands need an explicit outage path before this phase is
complete. If the historical primary and a surviving replica apply a command but
the third actor fails before terminal cleanup, the primary retains its slot.
The Active proof floor remains at the pre-command proof. After the outage fences
the PG to Peering, the primary is excluded as a transfer source because of its
slot and the clean survivor is excluded because its post-command proof differs
from that floor. Historical recovery still requires the failed actor, so neither
recovery nor replacement can make progress. The September 2026 four-node GET
outage reproduced this with 21 retained slots and 21 non-serving PGs. Preserve
both integrity checks until an authenticated protocol proves the exact
pre-command-to-published-command log lineage and either transfers the pending
slot and its cleanup dependencies to the replacement route or certifies safe
terminal cleanup under failed-actor fencing. The protocol must reject divergent
replica chains and survive leader failover, restart, and delayed failed-actor
return. A faster batch cadence cannot resolve this dependency cycle.

The September 20 mixed-workload outage also exposed a stale-floor variant:
PG 12 retained a certified Peering floor at log index 10, two surviving actors
agreed at index 14, and the primary held an unmarked pending slot at index 15.
The current source selector rejects the primary for its slot and the clean
replica for differing from the floor. Recovery of index 15 then waits for the
powered-off actor before it can abandon or converge the slot. Resolving only
index 15 would still leave the 10-to-14 floor gap. Implement the outage path as
one exact, durable proof-lineage handoff:

- Fence the old route and failed incarnation through a committed authority
  transition before any partial-actor cleanup can remove the pending slot.
  Retain the exact old route, unavailable observation, command identity, and
  cleanup ownership through response loss, restart, and later actor return.
- Obtain authenticated, bounded evidence from the available actors for every
  command and checkpoint link after the certified floor. Validate the chain
  from that floor, exact terminal dispositions, primary publication-start
  marker, and actor agreement. A checksum-valid but divergent or missing link
  must fail closed; equal tips alone do not certify the intervening chain.
- An unmarked primary slot may be abandoned only by a fenced compare-and-set
  under the primary PG command section, after proving no exact actor row
  contradicts the marker-before-dispatch invariant. A marked or ambiguously
  applied slot must retain and converge its exact command and dependency
  ownership on the replacement route; it cannot be reclassified as abandoned
  merely because an old actor is unreachable.
- Persist the verified source proof and terminal disposition as a replicated
  receipt that snapshot validation can reconstruct. Bind any source-floor
  advance and batch begin to that receipt, not to a mutable current heartbeat.
  Keep the replacement Peering until its imported proof and new-write shard
  readiness are complete. The off-route actor's terminalization/catch-up is a
  durable deferred obligation, and its old incarnation cannot serve or accept
  delayed mutations after the fence.
- Test unmarked, marked-before-witness, witness-only, primary-published, and
  trailing-replica cases; divergent chains; response loss; leader and storage
  restart; and the failed actor returning before and after activation. Include
  the floor-10/tip-14/pending-15 composition, not only a one-command fixture.
  The transition should still use the plural batch epoch boundary, with
  epoch-neutral evidence and cleanup publication.

Pending-command recovery scheduling is a separate latency bound. A failed PG
must not make other PGs wait through its ten-second command budget; use a small
fixed, persistent worker queue across discovery cycles, preserve per-PG single
flight, and rate limit repeated failures so outage recovery does not amplify
storage RPC load. Each task's retry floor starts at its own completion, not at
dispatch or completion of a batch. Fallback scans share the bound and cannot
block targeted discovery. A blocked PG must not delay another PG's retry or
newly discovered work. Rotate the target scan after the last dispatched PG so
continually reeligible low IDs cannot starve the tail, and admit due fallback
work before refilling target slots. Worker panic must fail-stop directly rather
than silently reducing pool capacity. Targeted and fallback workers share an
exact per-PG claim held through each attempt; fallback skips a claimed PG
before opening its recovery flight so it can reach other undiscovered work. A
scan that skipped any claim is incomplete and must retain a bounded fallback
retry even when its other PGs drained successfully; targeted discovery may
omit the skipped PG by then.

Deterministic replacement coverage must include multiple differently weighted
eligible spares and a changing availability subset. It must prove that retries,
authority failover, and subset changes preserve the ranking derived from the
complete committed policy and that the selected transition remains bound to the
exact topology generation, digest, derivation version, source acting set, and
availability evidence.

Queue discovery before the compare-and-swap is advisory and cancellable: a
renewed exact incarnation or changed topology/PG record invalidates that item
and requires recomputation. The compare-and-swap commit is the irreversible
boundary. Once it has durably recorded the transition and fenced the source PG,
recovery fails forward through the destination transition; a returning source
cannot cancel or revert it. Failure of a selected destination requires another
fenced successor transition rather than rollback to the stale source route.

Availability flapping must not cause automatic acting-set oscillation. A node
that returns becomes eligible for future placement only after its incarnation,
lease, local PG evidence, and retained history have been validated. Existing
PGs remain on their recovered placements until an explicit rebalance policy or
operator transition moves them, except for the maintenance-bound restoration
below; outage recovery itself is not implicit failback.

Planned maintenance follows the two modes defined in
[`temporary-write-availability.md`](../guides/temporary-write-availability.md).
Both use a durable record bound to a unique maintenance identity, topology
generation and digest, stable node identity, departing incarnation, mode,
authority-clock start/expiry, affected-PG source roots and progress cursor, and
operator authorization.

`migrate-before-stop` uses its healthy-source authorization to move current
placements before shutdown. It publishes a durable safe-to-stop result only
after every affected destination is `Active`, no current route requires the
node, and retained-placement evidence proves historical payload keeps at least
`k` readable shards without it. Completion does not move those PGs back.

`bounded-no-migration` suppresses only failure-driven CAS operations for its
exact departing incarnation and only before its authority-clock expiry. A
validated return before expiry clears the record without a placement change.
Expiry removes the suppression and enters ordinary automatic reconciliation.
If that reconciliation crossed its irreversible boundary before the node
returned, it must finish forward first. A validated return observation records
the exact returning incarnation, authenticated endpoint identity, accepted live
lease observation and deadline, and local PG-evidence generation and digest.
The same maintenance record then authorizes at most one controlled restoration
toward the recorded pre-maintenance acting set for each PG.

Restoration uses a new CAS that revalidates the return observation atomically:
the exact incarnation and endpoint remain current, its accepted lease is live
at authority time, and its PG evidence generation/digest remains accepted. The
CAS must also prove that the exact current PG is the unsuperseded terminal tip
of the maintenance outage-transition lineage, with no later operator intent or
unrelated recovery transition, and that topology and safety policy remain
unchanged. It then consumes that PG's restoration authority and fences the
restoration route into `Peering` in the same transaction. Any superseding
placement intent, even under the same topology and even if it later produces a
coincidentally matching route, permanently cancels the old maintenance record's
restoration authority for that PG. Restoration preserves all intermediate
route history and satisfies the same activation requirements as any placement
transition. This exception is maintenance intent, not general automatic
failback. Neither mode weakens lease fencing.

The multihost release gate must kill one acting storage host in a four-host
`2+1` deployment, without invoking any acting-set or metadata-transfer admin
command. It must target PGs that used the failed host, prove degraded reads from
the surviving `k` shards, observe deterministic replacement onto the spare,
and then sustain PUT, GET, HEAD, DELETE, multipart, and streamed-PUT traffic
after those PGs become writable. It must also prove that every affected PG is
eventually reconciled, and that a topology without a safe spare remains
degraded and retryable rather than activating an unsafe placement. Restart,
leader-failover, response-loss, and node-flapping variants must retain bounded
work and converge to the same placement.

Deterministic control-plane regressions must pause immediately before the
compare-and-swap and prove that exact-incarnation renewal, a changed topology
digest, and a changed source PG each reject the stale proposal without mutation.
A complementary regression must renew the source immediately after the commit
and prove the destination transition remains fenced and resumes after leader
restart. Activation tests must use one PG that serves both metadata and payload
placement and prove it remains `Peering` if either the metadata proof or
new-write destination readiness is incomplete. After both hold, a write must
still fail unless all `k + m` new shards commit, while a large historical
backfill may remain queued and old payload remains readable through retained
route history. Planned-maintenance tests must prove healthy-source migration
does not require lease expiry, an unexpired suppression rejects the automatic
CAS, return before expiry performs no migration, and return after an expired
transition completes one maintenance-bound restoration without dropping
intermediate history. Restoration regressions must pause immediately before its
CAS and prove return-lease expiry, endpoint/incarnation change, and changed PG
evidence each reject without mutation. A same-topology operator or unrelated
recovery transition after the outage chain must cancel restoration permanently,
rather than allowing the old maintenance record to override the newer intent.

### 3.4 Node Replacement Ceremony

Loss of established durable state is replacement, not initialization or path
relocation.

- allocate a fresh authority/storage incarnation;
- commit the exact old-to-new identity transition;
- fence the lost identity and stale credentials;
- add an authority as learner, catch it up, then promote it under the certified
  membership protocol;
- repair/backfill a replacement storage node before it becomes eligible for
  required acting-set responsibility; and
- reject empty-state startup under an established old identity.

Replacement must preserve quorum and at least `k` valid shards throughout the
declared failure-domain tolerance. Include lost authority disk, lost storage
disk, complete host loss, partial copied state, stale backup, and interrupted
replacement tests.

### 3.5 Storage Expansion And Rebalancing

- add nodes and disks first as non-serving prepared topology;
- use deterministic placement with immutable capacity weights once the
  separate weight plan is complete;
- create durable backfill/migration work from old to desired placement;
- activate each PG only after metadata proof and payload safety requirements
  hold;
- release old history and placement only after durable references clear; and
- support disk-to-host failure-domain evolution only through the committed
  topology protocol.

### 3.6 Operator API

Provide typed status, prepare, acknowledge, activate, replace, cancel, and
resume operations. Mutations are issued once; ambiguous outcomes use status and
transition identity for confirmation. Diagnostics expose blockers without raw
credentials or object identities.

### Phase 3 Exit Criteria

1. Membership cannot change without an exact committed authorization.
2. Lost state cannot be silently recreated under an old identity.
3. A lease-expired acting node is replaced automatically when committed spare
   capacity can restore the declared placement safely.
4. Replacement and expansion preserve the active failure-domain guarantee.
5. Every transition resumes safely after leader loss and restart.

## Phase 4: Replicated-Mode Production Graduation

### 4.1 Deployment Mode Selection

The experimental process, log, test, and wrapper labels have been removed.
The environment-only selector is now `ARGMIN_CONTROL_PLANE_RAFT_ENABLED`;
this naming cleanup does not complete production graduation.

Replace that selector with the validated deployment mode and static/dynamic
authority configuration. Standalone retains its single authority; replicated
mode always uses the replicated authority.

### 4.2 Keep OpenRaft Integration Auditable

- track the OpenRaft 0.10/1.0 stabilization path and review release changes
  before updating the pinned dependency;
- retain upstream-compatible log-store tests and Argmin's deliberately stricter
  restart/generation tests;
- preserve dedicated durability and CPU lanes for blocking WAL, snapshot,
  clone, decode, install, and retirement work; and
- require new async authority methods to pass the single-worker executor-
  progress regressions.

The existing executor-isolation implementation is baseline. This item prevents
regression; it is not a request to redesign the completed WAL lane.

The 2026-08-24 review upgraded the exact pin from 0.10.0-alpha.33 to
0.10.0-alpha.34. The relevant upstream changes make successful log-I/O
completion watermarks monotonic and move Raft ticks to a randomized,
fixed-origin `heartbeat_interval * 13 / 64` grid. Argmin's serialized WAL lane
already completes durability callbacks in order, while the upstream watermark
guard additionally protects initialization against out-of-order completions.
With Argmin's 250 ms heartbeat setting the new tick interval is 50 ms; this
preserves the 1.5-3 s election window and the explicit proposal-lease
confirmation margin.

### 4.3 Complete Operational Metrics And Resource Bounds

- add bounded runtime counters for storage RPC connection, session, admission,
  pre-auth byte, queue-wait, and saturation pressure so the multihost
  quantitative gate does not rely on log heuristics;
- alert on pending-command age, proof mismatch, PG quarantine, route-refresh
  failure, checkpoint/WAL poison, topology mismatch, and clock fences;
- retain bounded flight records around epoch, membership, transfer, and
  recovery decisions; and
- prove diagnostics remain available under ordinary worker saturation.

### 4.4 Credential And Certificate Lifecycle

Define an operational rotation procedure for symmetric RPC credentials, leaf
certificates, and trust roots. The initial contract may use staged rolling
restarts rather than online reload, but it must prove overlap windows, signer
selection, stale credential rejection, topology identity stability, and no
period where a quorum or required storage path has incompatible trust. Secret
distribution remains external to the manifest; ambient public roots are never
silently added.

### 4.5 Recovery And Restore Operations

Document and test full-cluster restart, authority quorum loss/recovery,
supported backup/restore, stale-backup rejection, clock-fence recovery, and
operator-visible handling of unrecoverable PGs. Recovery CLI operations remain
explicit and authenticated; they must not become autonomous safety overrides.

### Phase 4 Exit Criteria

1. Replicated mode has no experimental selection or naming dependency.
2. Resource pressure is quantitatively observable and bounded.
3. Credential and certificate rotation is documented and repeatable.
4. Supported operational recovery drills are documented and repeatable.

## Phase 5: Independent Correctness Evidence

### 5.1 Deterministic Distributed Simulator

Build a test-only virtual scheduler around existing authority, node-client,
clock, and store boundaries. Start with three authorities, three storage nodes,
two frontends, and two PGs. Generate shrinkable schedules containing:

- message drop, duplicate, reorder, delay, asymmetric partition, and reconnect;
- independent wall and monotonic clocks;
- process crash/restart at named durability points;
- short/torn writes, ENOSPC, EIO, and lost sync outcomes within the declared
  fault model; and
- S3 mutations, route changes, repair, reclaim, and membership transitions.

The oracle must be independent. Calling the same production apply function for
both system and model is differential testing, not a correctness model. Persist
every failing seed as a regression corpus entry.

### 5.2 Black-Box S3 History Checker

Record invocation, response, request identity, key/version, route epoch, node,
and durable command identity for concurrent PUT, GET, HEAD, DELETE, COPY,
conditionals, multipart, versioning, bucket deletion, and listing.

Check per-key histories against a small model. LIST checks the AWS-compatible
contract actually established by differential tests: ordering, continuation,
completed-before-invocation visibility where AWS guarantees it, and complete
quiescent pagination. Do not impose a request-wide linearization point across
independent keys when AWS does not.

### 5.3 Named Crash-Boundary Matrix

Add token-scoped pause points and kill a real process after each durability
boundary for:

- shard write/fsync/rename/directory sync and metadata publication;
- pending command insertion, each replica apply/ack, terminal cleanup, and
  compaction;
- transfer fence, export, import, replay, proof publication, and activation;
- reclaim claim, physical delete, metadata removal, and claim release;
- Raft WAL append/fsync, checkpoint publication, compaction, and response; and
- runtime configuration persist, route publication, permit acquisition,
  durable mutation, and response.

Each point must state whether restart completes, retries, quarantines, or
requires operator repair.

For metadata-command fanout, the matrix must enforce the publication and retry
contract in `guides/metadata-command-stream.md`: pre-dispatch rejection remains
abortable; the deterministic off-primary witness precedes primary publication;
confirmed or transport-ambiguous dispatch to either enters irrevocable
convergence; trailing replicas converge that exact command through a bounded,
deduplicated worker handoff; and no retryable S3 response is emitted after
witness dispatch. Cover witness and primary apply, every trailing-replica
position, each response-loss boundary, persistent replica outage, reservation
expiry during convergence, request-budget expiry before and after publication,
process restart, route transition, terminal cleanup, and loss of the publishing
primary plus its coordinator. Assert the request worker is released after
confirmed publication, the exact epoch/index/checksum survives every replay,
and the retained slot is not cleaned before all replicas converge. Run the same
cases through embedded and authenticated Unix/TCP clients so transport
classification cannot silently change the contract.

At the transport boundary, distinguish authenticated Unix and TLS/TCP
`NotSent` connection failures from post-commit response loss, malformed
payloads, and response-authentication rejection (`MayHaveApplied`). Exercise
one absolute confirmation deadline across admission, connect, write, read, and
state observation, including scheduler delay immediately before a retry.

### 5.4 Network And Process Nemesis

Add a bounded proxy/fault transport for asymmetric partition, response loss,
delay, partial frame, and slow-drip injection. Run continuous client traffic and
history checking while stopping, killing, partitioning, restarting, and
changing routes. Keep deterministic proxy scenarios in CI; reserve broad
randomized combinations for nightly and pre-release runs.

### 5.5 Small Formal Models

Model Argmin protocols that Raft does not solve:

- lease expiry, operation permits, deposed primary, and successor activation;
- Peering, proof ancestry, retained-log catch-up, and Active transition;
- shard durability, read handles, physical deletion, and reclaim; and
- cross-PG bucket drain and deletion.

Use TLA+, Stateright, or an equivalent test-only model. Model the Raft adapter
contract rather than reimplementing Raft.

### 5.6 Online Detection And Bounded Repair

- compare metadata proofs/digests across acting replicas periodically;
- run bounded SQLite integrity checks and shard checksum scrubs;
- expose exact per-PG quarantine evidence and safe repair progress;
- report why Peering cannot converge;
- detect timestamp jumps, serving deadlines outside policy, stale mutation
  rejection, old pending commands, retained-history growth, and poison; and
- ensure every detector has a bounded cost and cannot create scan
  amplification.

### Phase 5 Exit Criteria

1. Generated schedules shrink and persist deterministic regressions.
2. Black-box histories are checked independently of implementation state.
3. Every load-bearing durability sequence has a process crash test.
4. At least the lease/transition and reclaim protocols have small formal models.

## Phase 6: Multihost Release Gates

The existing three-host authenticated functional and quantitative gate remains
the baseline. Extend evidence without weakening that gate.

### Per Change

- formatting, warning-clean clippy, workspace nextest, boundary checks, and
  relevant UAT;
- fixed-seed invariant/property regressions;
- focused crash tests for changed durability boundaries; and
- invariant-register updates for protocol changes.

### Nightly

- thousands of deterministic simulator seeds with retained traces;
- concurrent black-box history checking with process/network faults;
- release-build authenticated multihost S3 workloads;
- 116+ PG and large retained-history/checkpoint/WAL cases; and
- long cleanup, repair, backfill, and metadata-transfer convergence runs.

### Pre-release

- run the complete supported deployment matrix: standalone restart/persistence,
  local replicated appliance mode across independent disk failure domains, and
  multihost replicated mode across host failure domains;
- for local replicated appliance mode, lose and restore every storage/authority
  disk in turn, including the current Raft leader's disk, and prove repair plus
  voter catch-up before restored service;
- use a fourth host as an independent client/supervisor so all three service
  hosts can be failed or partitioned without losing the harness;
- test asymmetric and complete network partitions, process kill, host reboot or
  power loss, storage-node and authority replacement, quorum loss/recovery, and
  full-cluster restart;
- run independent clocks, filesystems, and durability devices;
- inject ENOSPC, EIO, corruption, and power-loss-shaped disk outcomes under the
  declared fault model;
- run millions-of-object and sustained route/repair/reclaim churn workloads;
- include storage RPC pressure counters in the quantitative assertions; and
- require zero unexplained invariant violations, proof mismatches,
  quarantines, acknowledged-write loss, duplicate conditional success, or
  listing omission.

Every stochastic failure retains the exact smoke, iteration, process logs,
flight recorder, metrics, topology, and client history. A release blocker is
not closed by increasing a timeout without establishing the violated timing or
progress contract.

## Recommended Order

1. Complete Phase 0 and the physical-shard fence in Phase 1.
2. Close DeleteBucket/background convergence and add storage RPC pressure
   metrics while current soaks continue.
3. Build the deterministic simulator and history recorder incrementally around
   those active protocols.
4. Implement automatic unavailable-node placement reconciliation using the
   existing transfer and backfill primitives.
5. Implement committed topology, membership authorization, and permanent node
   replacement.
6. Add fourth-host network/hard-failure drills and replacement release gates.
7. Perform replicated-mode naming/configuration graduation only after the
   supported operational gates are repeatable.

## Plan Completion Criteria

This follow-up is complete when:

1. the executable safety case has no unowned invariant;
2. physical shard fencing, PG containment, proof ancestry, and outcome
   classification are closed;
3. cleanup and all durable background work converge under route churn and
   restart;
4. temporary node loss automatically uses committed spare capacity where safe,
   while authority/storage replacement and topology expansion are committed,
   resumable, and failure-domain safe;
5. replicated mode no longer depends on experimental configuration or naming;
6. deterministic simulation, black-box history checking, crash matrices, and
   formal models provide independent evidence; and
7. pre-release multihost gates pass with independent hosts, clocks, networks,
   and durability devices under continuous S3 traffic.

## Source Reconciliation

Carried forward from the completed multihost plan:

- incomplete Phase 11 cleanup/runtime-state convergence;
- physical shard identity fencing;
- background admission and historical checkpoint policy tuning;
- dynamic topology, membership, replacement, and expansion;
- automatic unavailable-node PG reconciliation onto committed spare capacity;
- Raft production naming/configuration graduation;
- storage RPC pressure metrics and hard-failure multihost gates.

Carried forward from the July reviews because the recommendation remains
valuable even where individual findings were later fixed:

- explicit fault model and executable invariant register;
- per-PG containment and proof ancestry;
- typed applied/not-applied/unknown outcomes;
- deterministic simulator, independent history checker, named crash matrix,
  network nemesis, formal models, and online invariant detection.

Not carried as open findings:

- DCC-1 route/mutation atomicity, DCC-2 clock/lease bounds, DCC-3 LIST caps,
  and DCC-4 release invariant checks;
- the pre-WAL Raft durability, automatic-election, artifact, poison, and peer-
  authentication findings;
- the old full-history/heartbeat write-amplification and whole-map refresh
  design; and
- findings whose mechanism was replaced by typed authenticated RPC, scoped
  serving-map reads, explicit clock recovery, or checkpoint/WAL isolation.

Those remain useful historical explanations in the archived documents, but
their original severity and code references are not current implementation
status.
