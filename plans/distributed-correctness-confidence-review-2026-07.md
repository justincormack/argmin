# Distributed Correctness Confidence Review - July 2026

Review baseline: `0d61d3fc` (`Bound committed timestamp catch-up`), 2026-07-09.

This is a companion to
[`distributed-correctness-review-2026-07.md`](distributed-correctness-review-2026-07.md).
That review remains the detailed source for findings `R*`, `CP*`, `MD*`,
`CL*`, `RPC*`, `INT-*`, and `R3-*`. This document does three things:

1. records correctness gaps found after re-reading the current Phase 11 and
   Phase 12 paths as end-to-end protocols;
2. reorders the still-open findings into production blockers rather than file
   ownership groups; and
3. defines the hardening work needed to make confidence come from independent
   evidence, not only more example tests against the implementation.

Severity follows the earlier review. `HIGH` means a safety failure or a
production-blocking liveness failure on an intended deployment. `MEDIUM` means
a narrower correctness gap or a material weakness in the safety case.
`Confirmed` means the mechanism is present in the code. It does not imply that
the failure has already been reproduced in a process test.

## Executive conclusion

Phase 11 should not yet be treated as production-ready, and Phase 12 should not
be used to close Phase 11 correctness work. Three new high-severity mechanisms
were present at the review baseline:

1. route and lease authorization is checked before a mutation, but is not
   atomic with that mutation;
2. the committed-timestamp clamp does not clamp the serving lease deadline, so
   a forward authority-clock error can still mint a far-future lease; and
3. two cross-PG listing paths capped an arbitrary prefix of PG results before
   the global merge, which could permanently omit valid records from
   pagination. DCC-3 records the completed correction.

The earlier review's `CL1`, `CL2`, `CP2`, `CP3`, `CP4`, `CP5`, `INT-2`,
`INT-3`, and `RPC4` findings also remain material. Several are availability
fail-closed today, but they matter to safety because operators under pressure
will otherwise need unsafe manual recovery.

There is substantial useful evidence already: deterministic control-plane
command application, checksummed codecs, durable all-acting-replica metadata
fanout, focused recovery tests, real three-process OpenRaft tests, and ongoing
multihost correctness soaks. The soak repeat bound has been increased over
time and is running at `--repeat 100` as of this correction; those runs have
found multiple defects that were fixed in recent commits. The remaining gap
is that most tests still share the implementation's assumptions, clocks,
host, scheduler, and storage behavior, while rare soak failures can take
several hours to surface. Before production, the project needs an explicit
fault model, executable invariant register, deterministic fault simulator,
black-box history checking, crash-boundary matrix, and formalized release-mode
soak gates.

## New findings

### DCC-1. HIGH - route and lease validation is not atomic with mutation

Confidence: confirmed mechanism; the exact lost/late mutation consequence
depends on where a route transition observes the old PG state.

The per-frame config refresh fixed the earlier per-connection stale-snapshot
bug, but it did not create a fencing linearization point:

- `StorageNodeConnectionHandler` owns an `Arc<StorageNodeProcessConfig>` and a
  shared `RwLock<Arc<...>>` (`storage_node_server.rs:2253-2259`).
- At the start of each frame, `refresh_config_snapshot` clones the current
  `Arc` and releases the read lock (`:2274-2281`, called at `:2330`).
- Runtime config installation validates and persists the candidate, then swaps
  the shared `Arc` under the write lock (`:1863-1891`). It does not wait for
  active frame handlers or acquire the per-PG metadata-command locks.
- Metadata command application validates the route and lease using the cloned
  config (`:9245-9263`), can then wait up to 500 ms for the PG lock
  (`METADATA_COMMAND_LOCK_WAIT_TIMEOUT` at `:1970`, acquisition at `:9265`),
  and only then mutates and fsyncs the PG store (`:9268-9272`). There is no
  config-generation, epoch, or deadline check tied to the transaction commit.
- The same validate-then-call shape is used by many other mutating RPCs,
  including generation allocation, durable bucket reservations, drains,
  shard writes, and cleanup operations (`:3934` onward).

A request can therefore validate under Active epoch E, pause while waiting for
a lock or storage, and resume after the node has installed E+1 Peering or a
later Active route. It can also validate just before its route-map deadline and
commit after the deadline. This is especially dangerous with `CL1`, because a
successor is not required to wait out the deposed primary's lease and in-flight
operations. Depending on transfer ordering, the late E mutation can be absent
from the state used to activate the successor, or can reintroduce pending state
after the recovery fence was evaluated.

The live-transfer path demonstrates the missing link. It fences the PG, waits
until the captured source lease deadline has passed, and only then exports the
source artifact (`argmin-s3/src/main.rs:915-956`). Waiting prevents a new
well-behaved request from starting on that lease, but there is no drain or
permit barrier for a frame that already passed validation. Such a frame can
commit after the exported proof was captured. The storage-side PG lock makes
individual commands and transfer RPCs serial, but the export is assembled
across multiple RPCs and the lock is not held from lease fencing through
artifact capture and successor activation.

The sequential tests around `storage_node_connection_refreshes_config_for_each_frame`
and `storage_node_connection_route_validation_uses_refreshed_config` prove that
the next frame sees a refresh. They do not install a new route between the
authorization decision and durable mutation.

The coordinator tests named `*_epoch_change_before_metadata_apply_commits_once_on_pinned_route`
also do not cover this race (`server-core/src/coordinator/core_tests.rs:2218`
onward). Their helper replaces the frontend runtime map while retaining the
same underlying old-epoch stores (`:1831-1887`); it does not perform a source
lease fence, artifact export, store import, or successor activation. Those
tests correctly pin historical-route retry behavior, but cannot establish the
live-transfer safety property.

Required design:

1. Give every mutating storage RPC a `PgMutationPermit` (or equivalent) whose
   acquisition and lifetime are synchronized with route installation.
2. Make the per-PG fencing token part of the mutation's commit condition. For
   SQLite metadata, update and check the durable fence in the same serialized
   PG domain as command application. A second check immediately before commit
   is insufficient unless a newer fence cannot install between that check and
   commit.
3. Separate ordinary current-epoch mutation permits from narrowly typed
   historical recovery permits. A historical route must not be able to invoke
   the normal mutation surface.
4. Define expiry behavior for in-flight operations. Either stop admitting work
   with a proven completion guard before expiry and drain it, or delay successor
   activation until old permits cannot commit. Merely checking wall time at RPC
   entry is not a lease fence.
5. Apply the common permit to visible metadata mutation first, then audit shard
   publication, reclaim, reservation, and cleanup RPCs by effect rather than by
   handler name.

Required deterministic regression:

1. Pause an E metadata command after route validation but before PG lock
   acquisition.
2. Install E+1 Peering, complete transfer/catch-up, and install E+2 Active.
3. Resume the old frame.
4. Assert that no old-epoch metadata, pending slot, reservation, or visible
   payload publication is created and that the successor's proof is unchanged.
5. Repeat with the pause immediately before SQLite commit and with route expiry
   instead of explicit config replacement.

### DCC-2. HIGH - bounded timestamp catch-up still grants unbounded serving leases

Confidence: confirmed. This supersedes the latest statement that `R3-1` and
`R3-2` are fully addressed. Their exact crash-loop mechanisms are fixed, but
the replacement does not establish a bounded lease invariant.

`MAX_HEARTBEAT_LEASE_MS` is 10 seconds and
`MAX_COMMITTED_TIMESTAMP_FORWARD_JUMP_MS` is one hour
(`control_plane.rs:28-29`). On heartbeat apply:

- `bounded_committed_timestamp_step` clamps only
  `max_committed_timestamp_ms` (`:928-933`);
- `expected_lease_deadline_ms` is still the unbounded
  `heartbeat_at_ms + requested_lease_duration_ms` (`:1477-1480`);
- the stored deadline is the maximum of that value and the existing deadline
  (`:1496-1501`), so later corrected time cannot shorten it; and
- the state stores the unbounded heartbeat time and lease deadline while
  recording only the clamped committed high-water (`:1568-1571`,
  `:1620-1623`).

The single-authority wrapper computes the same unbounded deadline
(`:3786-3817`), as does the experimental Raft wrapper
(`argmin-s3/src/main.rs:1693-1721`). The focused regression explicitly expects
this split: the high-water advances by one hour while the node deadline remains
`heartbeat_at_ms + 100` (`control_plane.rs:20555-20587`).

Serving maps use the minimum active-primary lease deadline as their validity
deadline (`control_plane.rs:724-728`), and storage nodes/frontends compare that
authority-issued absolute deadline to their local wall clocks. A single
far-future heartbeat can therefore make the route map and old primary usable
until the far-future time. Automatic expiry cannot mark a dead node unavailable
until that deadline. Repeated heartbeats also advance the high-water by another
bounded step per command, so the bound is on one command, not on catch-up rate.
The default 250 ms refresh interval makes repeated ratcheting a normal path
(`config.rs:355-357`, `:551-561`).

If the authority clock is corrected after one heartbeat, the elevated
high-water rejects corrected timestamps while the far-future serving lease
remains. If it stays wrong, later commands rapidly ratchet the high-water
toward it. In both cases the safety and availability properties depend on an
unstated clock bound. Combined with `CL1`, a deposed primary can retain a much
longer serving window than the configured lease duration.

Required design:

1. Write and enforce the invariant
   `serving_deadline <= effective_committed_now + max_lease + skew_budget`.
   Catch-up commands that cannot satisfy it must not grant a serving lease.
2. Separate logical command timestamps from real-time lease authority. A
   logical high-water is useful for deterministic replay, but it is not proof
   that a real-time deadline is safe on another host.
3. Base bounded catch-up on monotonic elapsed time or an explicit operator
   recovery protocol, never on command count.
4. Do not preserve a deadline with `max(old, new)` after the time basis has
   been declared invalid. Re-establish authority and serving leases through a
   fenced recovery state.
5. Complete the `R4`/`CP2`/`CL4` monotonic lease design together. A hybrid
   logical clock alone does not bound real time or cross-host skew.

Required property test:

- Generate independent authority, frontend, and storage-node wall clocks;
  monotonic clocks; forward/backward steps; restarts; leader changes; heartbeat
  rates; and node counts. After every event, assert that no accepted serving
  map exceeds the stated lease/skew bound, that corrected clocks have a bounded
  recovery path, and that a successor cannot overlap an old mutation permit.
  The current property strategies monotonically add small increments to one
  `now_ms`, so they cannot find this class.

### DCC-3. HIGH - cap-before-merge can permanently omit LIST results - FIXED

Confidence: confirmed. The failure requires enough populated metadata PGs to
hit the global record cap; it can be reproduced with a lower injected cap in a
focused test.

At the review baseline, the no-delimiter `ListObjectsV2` path queried PGs
sequentially. Each PG returned up to `max_keys + 1` objects. Once the
concatenated vector reached `record_cap`, the code truncated it and stopped
querying PGs; only then did it sort the retained records globally
(`request_ops.rs:7727-7753`). The production coordinator passed
`MAX_LIST_RECORDS = 100_000`
(`server-core/src/coordinator.rs:259`, `coordinator/listing.rs:52-61`).

At `max-keys=1000`, 100 populated PGs can cross the cap. Records late in the
100th PG are discarded before the global ordering is known; with more PGs,
whole PGs are not queried. A discarded or unqueried key can sort before the
1,000 retained results. The response then uses a retained larger key as the
continuation token (`request_ops.rs:7755-7771`), so the omitted smaller key is
filtered out by every later page and may never be returned.

`ListMultipartUploads` repeated the same shape: stop and truncate before the
coordinator's global `(key, initiated_at, upload_id)` ordering
(`request_ops.rs:12606-12635`, `coordinator/multipart.rs:940-979`). This
contradicts the no-partial-list result required by Phase 11 exit criterion 7,
even without an epoch transition.

The delimiter and object-version paths already used per-PG cursors and a k-way
merge, but copying that implementation directly would have retained up to
`O(metadata PG count * max_keys)` records. The no-delimiter object and
multipart-upload paths have a simpler bounded solution: query every PG for its
first `max + 1` ordered records, feed each response into a reusable bounded
global selector, and retain only the smallest `max + 1` unique records seen so
far. Later records from one PG cannot enter the global first `max + 1` because
that PG response is already ordered and contains its first `max + 1` records.
The selected design orders objects by key and multipart uploads by
`(key, initiated_at, upload_id)`.

This keeps peak retained records at `O(max_keys)` for the global selector plus
one `O(max_keys)` PG response, independent of PG count. Every PG must still be
queried before returning a page: a memory cap may bound the candidate set, but
it cannot discard an arbitrary PG prefix before the global first N records are
known. The existing cursor merge remains appropriate for delimiter grouping
and object-version pagination and does not need to be generalized as part of
this fix.

Resolution: the arbitrary `MAX_LIST_RECORDS` concatenation cap and
`hit_record_cap` result state were removed. No-delimiter object and multipart
listing now query every metadata PG and feed each ordered PG response into a
bounded ordered selector that retains only the global smallest `max + 1`
records. Object candidates are unique and ordered by key; multipart candidates
are unique and ordered by `(key, initiated_at, upload_id)`. The selector never
temporarily exceeds its capacity, so retained global candidates remain
`O(max_keys)` independent of metadata PG count. A route error or expiry from
any later PG still fails the entire request rather than returning the retained
prefix.

Regression evidence:

1. `list_objects_paginates_global_order_when_smallest_keys_are_on_last_pg` and
   `list_multipart_uploads_paginates_global_order_when_smallest_keys_are_on_last_pg`
   put lexically small records on the last of three real metadata PGs, walk
   every two-record continuation page, and assert exact equality with an
   independent global ordering.
2. `object_and_multipart_listing_select_global_first_page_at_production_cap_volume`
   opens 100 real SQLite metadata PGs and seeds every PG with 1,001 objects and
   1,001 multipart uploads. Lexically smaller records are assigned to later
   PGs. The test crosses the former 100,000-record cap in both production
   listing paths and verifies that the global first page comes entirely from
   the correct final PG.
3. `production_scale_selection_is_bounded_across_one_hundred_pgs` feeds the
   production selector 100 ordered PG result streams of 1,001 records each,
   reproducing the former 100,100-candidate boundary while asserting after
   every insertion that no more than 1,001 records are retained.
   `selection_matches_reference_global_sort` independently property-tests the
   selector against a global sort and deduplication model over varying inputs
   and capacities.
4. `composite_bucket_listings_fail_closed_when_route_map_expires_during_pg_scan`
   expires the pinned route map after the first PG result and verifies that
   both object and multipart listing reject the request on the next PG.

### DCC-4. MEDIUM - load-bearing control-plane invariant checks disappear in release builds

Confidence: confirmed confidence gap. No current command is known to produce
an invalid snapshot through this omission alone.

`ClusterControlSnapshot::validate_invariants` and calls from command apply,
single-authority open, and commit are guarded by
`#[cfg(any(test, debug_assertions))]` (`control_plane.rs:935`, `:2316-2321`,
`:3629-3635`, `:4173-4179`). Release binaries can therefore persist and serve
a state-machine result that debug/test binaries would reject. Restart parsing
may catch some shapes later, but that is after an invalid map may have been
published.

Split validation into cheap always-on safety invariants and expensive audit
checks. Command application and snapshot install should return a typed fatal
state-machine error before publication, not rely on a debug-only panic. Run
the full process suite in release mode as a separate gate so `cfg`, overflow,
timeout, and optimization-sensitive differences are exercised.

## Revalidated production blockers

The companion findings do not replace the following open work from the prior
review.

### Safety and authority

- `CL1`: administrative Active-exit transitions do not wait out or explicitly
  fence the deposed primary's lease. Fix together with DCC-1.
- `CP2` and `CL4`: absolute authority deadlines are compared against unrelated
  host wall clocks with zero skew margin. DCC-2 makes the consequence larger.
- `INT-3`: historical metadata-command recovery is authorized by a retained
  Active route plus any covering Peering route. Its scope is broader and
  longer-lived than the one transition it is meant to repair.
- `RPC4`: read-handle fencing is keyed by epoch-bearing `ShardLocation`, while
  the physical shard path is keyed by shard identity without epoch. Handles
  for two epochs can alias one file without blocking deletion.
- `CP6`, `CP7`, and `CL6`: cross-epoch proof progress and peering relaxations
  establish shape/difference, but not a strong ancestry proof from the last
  acknowledged metadata state.

### Recovery and bounded failure

- `INT-2`: one future-epoch pending slot makes PG bind fail, and bind failure
  prevents the whole storage node from starting. Fail closed per PG, quarantine
  it, and retain enough evidence for repair instead of taking unrelated PGs
  down.
- `CL2`: the retained-log catch-up implementation still has no production
  caller. A divergent acting replica can wedge Peering permanently even when
  another replica has the needed log.
- `CP3`: a missing single-authority state file silently bootstraps a fresh
  authority/incarnation. Persist cluster identity and epoch floors outside the
  replaceable snapshot and require explicit disaster recovery.
- `CP5` and `CP11`: retained history and whole-map heartbeat responses still
  create size and one-bad-PG blast-radius cliffs. Test beyond the 8 MB frame
  limit and production map/history scale.
- `RPC5`, `INT-5`, and `INT-6`: response-loss ambiguity, slow-drip/idle handle
  behavior, and remaining Raft WAL startup/generation/tail cases need explicit
  retry and recovery contracts.

### Freshness and integrity

- `CP4`: runtime-map freshness proofs are encoded and tested, but no production
  consumer rejects reconstructed or otherwise non-serving proofs. Type serving
  and diagnostic maps differently so a proof cannot be ignored accidentally.
- `CP10`: the single-authority state file lacks a binary integrity envelope and
  cluster identity binding.
- `R6` through `R9` and the `INT-6` WAL sub-findings remain relevant before the
  experimental Raft path becomes the default, especially membership/restart
  coupling and artifact-to-WAL generation binding.

## Required safety case

### State the fault model first

The repository currently has implicit and sometimes conflicting assumptions
about time, process failure, and storage. Write one production fault model that
answers at least:

- crash-stop versus crash-recovery and whether VM/disk rollback is supported;
- network partition, loss, duplication, reordering, asymmetric reachability,
  and unbounded delay;
- maximum wall-clock skew/step, monotonic-clock behavior across suspend, and
  what happens when the bound is violated;
- atomic rename, file and directory fsync guarantees, torn writes, ENOSPC,
  EIO, and acknowledged-write loss assumptions;
- whether disk corruption is detected, repaired, or outside the model;
- whether operators may restore individual control or storage nodes from old
  backups; and
- non-Byzantine versus authenticated-but-stale peers.

Lease safety cannot be claimed without a clock bound or a non-time fencing
mechanism. Recovery correctness cannot be claimed without saying whether a
durable volume can move backward.

### Maintain an executable invariant register

For every invariant, record its owner, linearization point, durable evidence,
fence, recovery action, assertion, metric, and tests. The minimum register is:

| ID | Invariant | Required evidence |
|---|---|---|
| I1 | An accepted serving map descends from the current cluster identity and a linearized authority read. | Typed serving map, checked freshness proof, durable cluster/incarnation floor. |
| I2 | A visible PG mutation commits only while its exact epoch/fence permit is current. | Commit-coupled fence token and deterministic transition race test. |
| I3 | A successor cannot accept a conflicting mutation while an old primary or old in-flight permit can commit. | Lease/skew proof or explicit drain/wait-out evidence. |
| I4 | Success means every required acting replica durably contains the same command/proof, or retry can determine the outcome. | Replica proofs, typed ambiguity, crash-after-each-ack tests. |
| I5 | Active epoch E+1 is descended from the last acknowledged state at E. | Transfer/catch-up proof ancestry, not only index/difference comparisons. |
| I6 | Reclaim cannot delete a reachable or in-flight physical shard under any epoch alias. | Physical-identity read fence plus metadata reachability proof. |
| I7 | Each S3 operation meets its established AWS contract. LIST does not require a request-wide linearization point across overlapping multi-key mutations. | Black-box contract tests plus AWS differential cases where behavior is defined and observable. |
| I8 | Corruption or ambiguity in one PG cannot silently spread or take unrelated PGs down. | Per-PG quarantine, repair state, operator-visible diagnostics. |
| I9 | A Raft response implies vote/log/state durability under the declared disk model, and snapshot/WAL generations cannot be mixed. | Crash matrix, generation binding, OpenRaft conformance tests. |

Treat this register as code-adjacent design input. A change to command ordering,
proof shape, lease calculation, or durable sequencing must name the invariants
it preserves and update their tests.

### Strengthen types around authority

Several bypasses exist because validated and unvalidated states share types.
Prefer types that make the invalid call unrepresentable:

- `ServingRuntimeMap` versus `DiagnosticRuntimeMap`;
- `CurrentPgMutationPermit` versus `HistoricalRecoveryPermit`;
- a physical shard identity fence distinct from epoch-bearing routing location;
- `ConfirmedApplied`, `ConfirmedNotApplied`, and `OutcomeUnknown` RPC results;
- durable `ClusterIdentity` and `AuthorityFloor` required to open either
  single-authority or Raft state; and
- `PeeringActivationProof` whose constructor verifies ancestry and the acting
  set that supplied it.

## Confidence-building test program

### 1. Deterministic distributed simulator

Build a test-only event simulator around the existing control-plane, node
client, clock, and store traits. It should run at least three control-plane
nodes, three storage nodes, two frontends, and two PGs with:

- a virtual scheduler and separately controlled wall/monotonic clocks;
- message drop, duplicate, reorder, delay, partition, and reconnect;
- crash/restart at named durability boundaries;
- disk outcomes for success, short write, torn tail, ENOSPC, EIO, lost
  directory sync, and rollback where supported; and
- generated S3 operations, route changes, leader changes, repair, reclaim,
  and membership changes.

Use seeded property runs and shrink the event trace. Persist every failing seed
as a regression corpus entry. The simulator should compare externally visible
history to an independent small reference model; calling the same production
apply functions on both sides is differential implementation testing, not an
oracle.

### 2. Black-box history and linearizability checker

Record invocation, response, request identity, key/version, route epoch, node,
and durable command id for concurrent:

- PUT, GET, HEAD, DELETE, COPY, and conditional requests;
- versioning and delete markers;
- multipart initiate/upload/complete/abort;
- LIST objects, versions, uploads, and parts; and
- bucket create/delete plus bucket-wide drains.

Check per-key histories with a small linearizability model. For LIST, check the
defined contract only: completed-before-invocation visibility, ordering,
markers/tokens, and complete pagination in a quiescent history. Mixed views of
overlapping mutations to different keys are allowed and must not be rejected
for lacking a request-wide linearization point. Run workloads against AWS to
populate oracle cases where behavior is defined and observable, especially
response-loss retries and concurrent conditionals. Never infer AWS semantics
from what is easiest for the local architecture.

### 3. Named crash-boundary matrix

Add deterministic pause points and SIGKILL a real process after each boundary:

- shard create/write/fsync/rename/directory fsync, then metadata publication;
- pending command insertion, each replica apply/ack, terminal cleanup, and log
  compaction;
- transfer fence, export, import, catch-up, proof publication, and Active;
- reclaim claim, each physical delete, metadata removal, and claim release;
- Raft WAL append/fsync, artifact write/rename/directory sync, WAL compaction,
  and response write; and
- runtime config persist, publication, mutation permit acquisition, durable
  commit, and response write.

For each point, specify whether restart must complete, retry, quarantine, or
require operator repair. A generic "process restart works" test is not enough
to prove ordering at these boundaries.

### 4. Real process fault injection

The existing three-process Raft tests and multihost scripts are a useful base,
but they run on one host with one clock and mostly scripted sequential faults.
Add a proxy transport, initially around Unix sockets, to inject asymmetric
partitions, delay, response loss, partial frames, and slow-drip traffic. Then
run multi-VM tests with independent clocks, kernels, filesystems, and network
namespaces. Use continuous concurrent client traffic and history checking while
the nemesis kills, stops, partitions, restarts, and changes routes.

### 5. Scale and duration tests

Correctness changes at size because timeouts and caps become protocol inputs.
Test at and beyond:

- 100+ metadata PGs and the 100,000-record list cap;
- runtime maps/history snapshots around and above the 8 MB RPC frame cap;
- enough PGs and latency that sequential LIST fanout approaches route expiry;
- fsync latency above the 500 ms metadata-lock budget;
- admission/session limits with idle read handles and slow frames;
- WAL and metadata-log compaction thresholds; and
- millions of objects plus sustained reclaim/repair/route churn.

The existing `scripts/uat-correctness-soak` runs are already being executed on
an ongoing basis with increasing `--repeat` bounds, currently 100. They have
found a number of real issues that were subsequently fixed, so this is
meaningful correctness evidence and should continue throughout the hardening
work. Its current limitation is feedback latency: rare interleavings can take
several hours to fail. Record the failing smoke and iteration, retain all
process logs and flight-recorder state, and convert each discovered timing
failure into a deterministic pause-point regression. A pre-release candidate
should additionally survive a longer soak with fixed production-like capacity
and no unexplained quarantine, proof mismatch, pending-command age, or leaked
shard growth.

### 6. Small formal models

Use TLA+, Stateright, or an equivalent test-only model for the protocol that
OpenRaft does not solve:

- lease expiry, operation permits, deposed primary, and successor activation;
- PG Peering, log catch-up, proof ancestry, and Active transition;
- shard durability, metadata publication, read handles, and reclaim; and
- cross-PG bucket drain/delete.

Do not reimplement Raft as a first formal project. Model the adapter contract:
committed command ordering, durable response, snapshot/WAL generation, and
serving read freshness.

### 7. Online detection and bounded repair

Production confidence also requires detecting assumptions that tests missed:

- periodically recompute and compare metadata digests/proofs across acting
  replicas;
- run controlled SQLite integrity/foreign-key checks and shard checksum scrubs;
- expose per-PG quarantine with the exact conflicting epoch/index/hash and a
  safe repair workflow;
- wire retained-log catch-up into Peering and report why it cannot converge;
- metric and alert on timestamp high-water jumps/rate, observed clock skew,
  serving deadlines beyond policy, stale mutation rejection, pending-command
  age, protected history bytes, runtime-map bytes, proof mismatch, WAL poison,
  and artifact/WAL generation; and
- retain a bounded flight recorder around every epoch transition and recovery
  decision.

## CI and release gates

### Per change

- `cargo fmt`, warning-clean clippy, workspace nextest, fuzz target build, and
  existing UAT scripts;
- deterministic invariant/property tests with fixed regression seeds;
- focused process crash tests for any changed durability boundary; and
- `./scripts/check-storage-cluster-boundaries` plus an invariant-register drift
  check for affected protocol modules.

### Nightly

- thousands of deterministic simulator seeds with failure trace retention;
- full S3 UAT through three replicated control-plane processes, not only direct
  control-plane RPC tests;
- concurrent history checker plus network/process nemesis;
- release-build test/UAT execution; and
- production-scale map, history, PG-count, listing, and WAL tests.

### Pre-release

- multi-VM soak with independent clocks/disks/network and continuous S3 load;
- quorum loss/recovery, leader replacement, storage-node replacement, full
  cluster restart, backup/restore, and rollback-rejection drills;
- ENOSPC/EIO/corruption and power-loss-style disk-image tests under the declared
  filesystem model; and
- zero unexplained invariant violations, proof mismatches, quarantines, lost
  acknowledged writes, duplicate conditional successes, or listing omissions.

`scripts/ci` currently runs a broad debug-profile suite and many useful UAT
smokes, but does not invoke the correctness soak. The separate soak is
nevertheless being run on an ongoing basis by the development process at
increasing repeat bounds. Its scheduling, result retention, and eventual
release-gate role should be made explicit alongside complementary release-build,
continuous-traffic, network-partition, independent-clock, and full replicated
control-plane S3 tests. Deterministic reproduction is especially important
because the current stochastic failures can take hours to recur.

## Recommended order of work

1. Reopen the Phase 11 fencing exit criterion. Fix DCC-1 with `CL1`, including
   the deterministic pause-at-commit transition test.
2. Replace the timestamp/lease design in DCC-2 with an explicit clock fault
   model and multi-clock property test.
3. **Completed:** fix DCC-3 with bounded global smallest-`N` selection and pin
   complete pagination plus the 100-PG/100,100-record selector boundary.
4. Contain recovery failures per PG (`INT-2`), then connect retained-log
   catch-up to the production Peering state machine (`CL2`).
5. Fence physical shard identity across epochs (`RPC4`) and narrow historical
   mutation authority (`INT-3`).
6. Make freshness proof consumption, cluster identity/floors, proof ancestry,
   and bounded history part of the serving types (`CP3`-`CP7`).
7. Complete the Raft WAL generation/startup work and run the full S3 stack
   through three control-plane processes under the same fault harness.
8. Continue the increasing-repeat correctness soak throughout these slices.
   Once the blockers above are closed, promote it into a retained
   production-readiness gate and add operational recovery drills.

Phase 12 improves control-plane availability and ordering, but it does not
repair a storage-node fencing race, a cross-host lease model, or PG recovery by
itself. DCC-3's cross-PG listing defect was corrected independently. The
production gate should therefore be expressed in terms of the invariants
above, not simply completion of the Raft integration checklist.
