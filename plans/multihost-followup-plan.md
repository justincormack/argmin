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

### 3.3 Node Replacement Ceremony

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

### 3.4 Storage Expansion And Rebalancing

- add nodes and disks first as non-serving prepared topology;
- use deterministic placement with immutable capacity weights once the
  separate weight plan is complete;
- create durable backfill/migration work from old to desired placement;
- activate each PG only after metadata proof and payload safety requirements
  hold;
- release old history and placement only after durable references clear; and
- support disk-to-host failure-domain evolution only through the committed
  topology protocol.

### 3.5 Operator API

Provide typed status, prepare, acknowledge, activate, replace, cancel, and
resume operations. Mutations are issued once; ambiguous outcomes use status and
transition identity for confirmation. Diagnostics expose blockers without raw
credentials or object identities.

### Phase 3 Exit Criteria

1. Membership cannot change without an exact committed authorization.
2. Lost state cannot be silently recreated under an old identity.
3. Replacement and expansion preserve the active failure-domain guarantee.
4. Every transition resumes safely after leader loss and restart.

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
4. Implement committed topology, membership authorization, and replacement.
5. Add fourth-host network/hard-failure drills and replacement release gates.
6. Perform replicated-mode naming/configuration graduation only after the
   supported operational gates are repeatable.

## Plan Completion Criteria

This follow-up is complete when:

1. the executable safety case has no unowned invariant;
2. physical shard fencing, PG containment, proof ancestry, and outcome
   classification are closed;
3. cleanup and all durable background work converge under route churn and
   restart;
4. authority/storage replacement and topology expansion are committed,
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
