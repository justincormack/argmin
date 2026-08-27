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
command v20 additionally seal immutable canonical singleton begin/completion
replay receipts. State v32 retains the exact completion
evidence and protected source/activation routes needed to recompute each
receipt digest and reject command-unreachable completion epochs during snapshot
validation. State-v32 publication and decoding require exactly one receipt
member; durable multi-member receipts remain reserved for plural command v21
and state v33. State v32 retains state-v31 and command-v19 rejection evidence.
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

Command v21 removes the singular
`BeginUnavailablePgPlacementTransition` and
`CompleteUnavailablePgPlacementTransition` variants and removes the optional
unavailable-transition branch from `SetPgActingSetWithMetadataTransfer`.
Unavailable-transition route installation exists only through the plural
install command; the ordinary transfer command rejects an active transition.
Likewise, use a dedicated exact-transition fence operation rather than an
optional transition field on the generic fence command. Worker, direct-admin,
test, and recovery entry points submit plural commands, wrapping one entry when
only one PG is available. State-v33 validation requires every active and
retained unavailable transition to carry the applicable batch receipts and
staging generation, so no singular command or decoded legacy state can bypass
batch accounting.

The current live-transfer operation cannot be batched as written because it
fences, exports, commits the destination route, and imports in one call. Split
it into explicit `prepare`, batch `install`, and `import` phases. Preparation
retains the exact transition binding, source route and proof, artifact digest,
and the imported proof precomputed for one expected target epoch. Artifact
decoding, retained-command validation, canonical re-encoding, hashing, and
epoch rebasing all occur outside the heartbeat update gate. Gated install
derivation only compares the current next epoch with the prepared expected
epoch and validates compact fixed-width bindings. If the epoch changed, it
releases the gate and recomputes the proof outside the gate before retrying; it
never processes up to a batch of metadata artifacts while excluding heartbeat
renewal.

An ownership token alone is not sufficient once the committed imported proof
is bound to exact artifact bytes. Before destination-route installation, the
prepare phase durably stages the immutable artifact under its transition
binding and content digest on every destination actor and obtains authenticated
fsync-complete receipts. `CreateStagingIntent` compare-and-swaps the complete
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
Receipt evidence uses a mandatory control-plane RPC v18 operation separate
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
Response loss, process restart, newly queued deltas, or expiry of the prior
authentication window cause byte-for-byte retransmission of the retained
operation payload under a fresh envelope, never reconstruction with different
members.

Applying a page creates a canonical, authority-backend-neutral apply receipt
bound to node identity and incarnation, previous generation, previous
apply-receipt digest, generation, page digest, and the resulting accepted
evidence generation. The control-plane state persists that receipt together
with the highest accepted generation.
The control plane cannot observe the node's durable recording from the response
it sends. It therefore retains the highest accepted page and apply receipt
across finalized-floor advancement, detailed evidence pruning, snapshots,
journal compaction, Raft compaction, and authority failover. That receipt is
replaced only when the control plane accepts the exact successor page whose
`previous_generation` and previous apply-receipt digest cite it. Because the
node may construct that successor only after durably recording the cited
receipt, acceptance is the observable acknowledgement boundary. This retains
at most one such replay receipt per node incarnation rather than one per page.

A Raft log ID or standalone journal position may accompany the apply receipt
as diagnostic metadata, but is not part of receipt identity or required for
replay. The response uses a fresh authenticated envelope carrying the
canonical receipt. Only after the node durably records the exact receipt does
it retire the page's deltas, advance its acknowledged generation, and build
the next page. Exact replay lookup precedes finalized-floor and pruned-evidence
rejection: replay of the retained page returns the same canonical apply receipt
under a fresh response envelope without recreating evidence, while any
different payload at that generation is rejected. For a new generation,
server admission requires both predecessor fields and compares them with the
retained receipt before changing evidence or replacing that receipt. An
omitted, malformed, genesis-on-successor, or incorrect predecessor digest is a
protocol error that leaves the retained receipt and all evidence unchanged. A
conflicting digest for an assigned generation is fatal protocol evidence.

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
staging generation, and node rather than dropping it when a later heartbeat or
page omits the entry. The control plane retains each node incarnation's highest
accepted evidence-page generation and a per-PG finalized staging-generation
floor. Receipt evidence at or below that floor is rejected even if carried in
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

The global per-PG finalized floor advances only through an exact replicated,
cluster-map-epoch-neutral cleanup CAS. Every obligated destination must have
committed a canonical cleanup receipt for the same transition, staging
generation, artifact tuple, and tombstoned local generation; partial cleanup
cannot advance the floor or prune staging authorization. Per-node tombstone
evidence rejects that node's stale receipt pages while evidence for an offline
destination remains admissible. Cleanup evidence and finalized-floor changes
do not advance the global epoch, change a PG route, invalidate a serving
runtime map, or request fresh PG observations.

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
to v20 and state version from v31 to v32. Advance the plural command grammar
from v20 to v21 and its complete durable state from v32 to v33, retaining
immutable v19/v31 and v20/v32 rejection evidence and updating
the nested journal, Raft WAL, snapshot, aggregate, and retained batch-receipt
vectors. The new transition-scoped artifact staging operations cross the
storage RPC boundary and therefore require the corresponding storage-RPC
version advance, fixed old/new frame evidence, authenticated Unix/TLS coverage,
and explicit exclusion from ordinary frontend capabilities. Receipt-evidence
publication crosses the control-plane RPC boundary, so advance control-plane
RPC v17 to v18 unconditionally. Preserve complete v17 rejection evidence and
add fixed v18 frames for a genesis page, a successor carrying a non-genesis
`previous_apply_receipt_digest`, minimum, maximum-count, maximum-byte,
multipage, generation-gap, exact-replay, and tombstone/finalized-floor
evidence. Frame-limit constants and exact-boundary fixtures include the full
predecessor digest. Omitted, truncated, genesis-on-successor, and incorrect
digest fixtures must fail before dispatch without replacing the retained apply
receipt or mutating evidence. Batch transition commands remain leader-internal
and require no additional
control-plane RPC operation beyond that receipt protocol.

Durable artifact staging uses a separate storage-owned format rather than
silently extending the PG schema. Introduce staging-store format v1 with a
versioned root manifest, generation catalogue, content-addressed artifact
files, published receipts, import status, tombstones, and per-PG finalized
generation floors. The catalogue also persists pending receipt/tombstone
deltas, the exact assigned in-flight evidence operation-payload bytes and
digest, and the canonical authority-neutral apply receipt through atomic delta
retirement. Fresh RPC authentication envelopes are never persisted as replay
material. Publication writes a generation-scoped temporary file,
fsyncs and validates its exact length and digest, atomically renames it, fsyncs
the containing directory, then commits and syncs the catalogue state before
issuing a receipt. Startup validates the manifest and complete catalogue/file
inventory before serving staging RPCs: unknown versions, digest or length
mismatch, missing published files, generation regression, and contradictory
receipt/tombstone state fail closed. Bounded startup reconciliation removes
unpublished temporary files, completes tombstone-directed unlink and directory
sync, and quarantines unexplained final files rather than authorizing them.
Store admission accounts for temporary, published, and tombstoned cleanup
bytes. Add immutable v0/v2 rejection fixtures and a fixed v1 manifest,
catalogue, receipt, and crash-state corpus to the storage format ledger; no
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
- v21 rejection of every singular unavailable-transition command path and
  singleton operation through each plural worker/admin entry point;
- multiple PGs sharing transition and destination epochs;
- heartbeat renewal between batch preparation and gated derivation;
- proof preparation and stale-epoch recomputation without artifact work under
  the heartbeat gate;
- partial transfer failure, bounded retry, and preservation of successful
  prepared work;
- durable artifact retrieval and exact-byte import after leader/process loss,
  with source and destination loss at every staging boundary;
- independently verified committed staging receipts, forged receipt fields,
  stale incarnation/endpoint evidence, and incomplete fsync scope;
- receipt-page count and encoded-byte limits, generation gaps, page replay,
  backlog isolation from lease renewal, and old-page rejection after the
  control-plane finalized-generation floor advances;
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
- a cleanup-evidence page committed with its response lost, followed by
  finalized-floor advancement, detailed-evidence pruning, snapshot or journal
  compaction, and authority restart; exact replay must return the retained
  apply receipt, permit durable outbox retirement, and a successor page citing
  that receipt must become the new bounded replay receipt in both Raft and
  standalone modes;
- blocked and saturated evidence connections, workers, and Raft proposals while
  lease deadlines continue to advance and the global cluster-map epoch remains
  unchanged;
- partial cleanup with one destination offline, its later authenticated return
  and cleanup, rejection of premature floor advancement, and exact all-actor
  floor advancement with replay through the compact cleanup certificate;
- pre-install cancellation where one authorized destination reports no intent
  and another committed staging but lost its response, proving both durably
  tombstone before the finalized floor advances;
- permanent destination loss using an exact fenced-incarnation retirement
  certificate after credential and endpoint retirement, plus rejection while
  that actor is merely offline, expired, or capable of returning;
- multiple independent epoch-neutral cleanup CAS operations across different
  PGs, including concurrent success and one stale failure, proving receipt,
  retirement-certificate, floor, pruning, and compact-certificate mutations do
  not change the global cluster-map epoch or couple PG outcomes;
- competing staging-intent authorization and destination creation with
  mismatched artifact digest, length, or format version;
- cleanup before install, delayed stage and publish after cleanup, generation
  reuse, tombstone restart, and finalized-generation-floor rejection;
- staging-store publication crashes before and after file fsync, rename,
  directory fsync, catalogue commit, and receipt issuance;
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

### 4.1 Remove Experimental Selection And Naming

Replace `ARGMIN_CONTROL_PLANE_EXPERIMENTAL_RAFT` with the validated deployment
mode and static/dynamic authority configuration. Rename experimental process,
log, test, and wrapper labels in one auditable change. Standalone retains its
single authority; replicated mode always uses the replicated authority.

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
