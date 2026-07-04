# Distributed Correctness Review — July 2026

Review of the multihost transition ahead of production, covering the Phase 12
OpenRaft control plane, the Phase 11 single-authority control plane, metadata
replication and PG store recovery, cluster routing/epoch fencing/peering, and
the storage RPC boundary. Roughly 150k lines were reviewed across five
parallel deep passes; every critical/high finding was re-verified directly
against the source before inclusion. Line numbers are as of the review date
and will drift.

Severity scale: **critical** (must fix before the affected mode ships),
**high** (correctness or production-blocking liveness on the current
production path), **medium** (real gap, narrower window or fail-closed
consequence), **low** (defect worth fixing, limited blast radius).
Confidence: **confirmed** (mechanism verified by code reading) or
**plausible** (mechanism read, occurrence depends on timing/deployment).

This document is split into Raft-specific findings (Part 1) and everything
else (Part 2), with shared cross-cutting themes and a testing/confidence plan
at the end. Finding IDs are stable so items can be broken out into work
slices: `R*` = Raft, `CP*` = single-authority control plane, `MD*` = metadata
replication, `CL*` = cluster/routing/peering, `RPC*` = storage RPC.

## Cross-cutting themes

Four systemic patterns produce most of the findings below; each is worth
treating as a design item, not just a bug list:

1. **Time is the weakest foundation.** Every fencing guarantee ultimately
   reduces to `SystemTime` comparisons with zero skew margin, no monotonic
   latching, and no per-node monotonicity guards. This appears independently
   in the single authority (CP1, CP2), the Raft path (R4), and node-side
   serving checks (CL4). The plan defers the monotonic-clock lease-read
   design to later Phase 12, but Phase 11 is the production path today and
   inherits all of it.
2. **Ack-before-durable.** Raft peer responses precede the durability
   checkpoint (R1), and `PRAGMA synchronous=NORMAL` under WAL means acked
   metadata commands are not power-loss durable (MD5), which the
   strict-write/pending-slot recovery model implicitly assumes.
3. **Load-bearing invariants are not executably pinned.** The primary-last
   fanout ordering was flipped to primary-first in June (`a9013451`) without
   the guide, the recovery safety argument, or any test noticing (MD1). When
   a written invariant that recovery correctness depends on can silently
   invert, that is a process gap as much as a code bug.
4. **Fail-closed machinery with bypass seams.** The digest gate can be masked
   by the clean-revision cache (MD2); freshness proofs are minted but no
   production consumer checks them (CP4); the peering catch-up path is dead
   code (CL2). RPC2's per-connection snapshot bypass is now fixed with
   per-frame config refresh. Each fail-closed mechanism needs a test proving
   it cannot be bypassed, not just that it fires.

---

# Part 1: Raft control plane (Phase 12)

Scope: `crates/storage/src/control_plane_raft.rs` (log store, state machine,
restart artifacts, peer transport, authority wrappers, read-index path),
`crates/storage/src/control_plane_command.rs`, experimental process wiring in
`crates/argmin-s3/src/main.rs`/`config.rs`.

## Findings

### R1. RESOLVED-SAFETY / FOLLOW-UP — Raft protocol acks must not be sent before durability

Original finding: peer vote / append-entries responses were sent before the
durability checkpoint, enabling double-voting (split brain) and loss of
client-acknowledged committed entries in multi-node mode. Confirmed at review
time.

- The log store is purely in-memory; `append()` reports entries flushed
  immediately (`control_plane_raft.rs:5828-5851`, `callback.io_completed(Ok(()))`
  at :5849) and `save_vote` is memory-only (:5802-5810). OpenRaft therefore
  treats votes/appends as persisted the moment they land in RAM.
- Status update: the process peer RPC worker now reads and handles the request,
  checkpoints the durable restart artifact, and only then writes the response
  frame. A checkpoint failure exits before any peer ack is written. This closes
  the current process-mode ack-before-durable safety hole for vote, append,
  transfer-leader, and snapshot frames.
- Scenario A (split brain): follower F grants a vote in term 5, responds,
  crashes before checkpoint, restarts from an artifact with a term-4 vote,
  and grants a second vote in term 5 to a different candidate. Two leaders in
  term 5; with F acking appends from both, conflicting entries can commit at
  the same index.
- Scenario B (acked-commit loss, no double vote needed): three nodes L, F1,
  F2. L commits entry E with F1's in-memory ack; L checkpoints and acks the
  client; F1 crashes pre-checkpoint and restarts without E; L crashes; F1+F2
  form a quorum and elect a leader without E. The client-acknowledged command
  is silently rolled back and L's copy truncated on rejoin.
- Single-node mode is not affected: submit → outcome → checkpoint → respond
  ordering is correct there (see R-OK1).
- Regression coverage now includes peer RPC checkpoint-failure and checkpoint
  pause cases. The pause coverage proves a vote can be volatile while the
  response is still blocked, proves no response frame is written before the
  durable artifact contains that vote, and proves a poison flip before ack
  still suppresses the response.

Follow-up: the fix uses a full restart-artifact checkpoint before each peer
response. That is safe but expensive. A small fsync'd vote/log WAL remains the
natural production design, with the full artifact demoted to a compaction
checkpoint. R3's torn-capture concern is closed for the current full-artifact
checkpoint path.

### R2. RESOLVED — No automatic leader election or leader heartbeats in the production config

Confirmed and fixed for the experimental Unix-peer process path.

- The experimental Raft config now has an explicit timer mode. Single-node and
  deterministic in-memory tests keep manual timers disabled, while
  `new_experimental_unix_peer_durable` uses automatic timers with OpenRaft
  tick, heartbeat, and election enabled.
- The startup seed trigger remains as a deterministic first-leader fast path,
  but leader loss no longer depends on the explicit
  `control-plane-trigger-raft-election` admin command. A three-process smoke
  kills the seed leader, waits for a surviving process to become serving
  through natural OpenRaft election, commits through that new leader, and
  verifies follower checkpoint convergence.
- Natural election also gets the same durability gate as the manual election
  path: after a successful read-index runtime-map read, the process checkpoints
  the durable restart artifact before returning the map, even if the node loses
  leadership before the follow-up status sample. When the node is still
  serving, the cache marker includes the local serving vote/term and full
  committed/applied log ids. A node cannot expose a naturally elected
  linearized authority before its durable restart artifact contains the new
  committed vote/term.

Residual: this enables timers only for the experimental Unix-peer process
mode. Deterministic unit/in-process tests intentionally keep manual election
control to avoid timeout-sensitive test behavior.

### R3. RESOLVED — Torn checkpoint capture: log store and state machine exported without a barrier

Confirmed and fixed for the current full-artifact checkpoint path.

- `capture_durable_restart_artifact` now captures the state-machine artifact
  before exporting the log store. If Raft advances concurrently, the later log
  export may be ahead of the captured state, which restart can replay from the
  retained committed suffix. The dangerous reverse shape — state ahead of the
  exported log/committed gate — is rejected by pair validation and retried.
- `store_durable_artifact` validates
  `validate_log_store_state_machine_pair` before encoding or replacing the
  durable artifact, so a torn or otherwise inconsistent pair cannot overwrite
  the last good checkpoint.
- Coverage:
  `control_plane_raft_durable_restart_artifact_capture_allows_log_ahead_of_state_machine`
  pins the replayable state-first/log-later capture shape,
  `control_plane_raft_durable_restart_artifact_capture_rejects_inconsistent_pair`
  pins the invalid state-ahead shape, and
  `control_plane_raft_durable_restart_artifact_store_rejects_inconsistent_pair_before_overwrite`
  proves a bad artifact does not replace the previous durable file.

Residual: this still uses full-artifact checkpoints. R1's WAL follow-up should
replace this with a smaller fsync'd vote/log durability path before production
scale, but the R3 torn-capture restart brick is closed.

### R4. MEDIUM — Lease semantics still need a monotonic-clock design

The immediate stale-time guardrails are in place, but the broader
monotonic-clock lease design is still deferred. Confirmed mechanics;
exploitability depends on clock discipline.

- Clock source is `SystemTime::now()` (`crates/storage/src/clock.rs:57-66`,
  returns 0 before epoch via `unwrap_or_default`). Expiry commands are
  stamped by the current leader's loop (main.rs:2008 →
  `ExpireHeartbeatLeases { expire_at_ms }` at :1428-1430); heartbeat
  deadlines are `authority_now_ms + requested` (:1468-1470). Apply is fully
  deterministic against the committed value (R-OK4), so replicas never
  diverge; the risk is real-time correctness:
  - `RecordNodeHeartbeat` apply overwrites `lease_deadline_ms`
    unconditionally (`control_plane.rs:1343/1394`) with no monotonicity
    check, so a regressed clock (or a command proposed before, but committed
    after, an `ExpireHeartbeatLeases`) can shorten or resurrect a lease.
    Resurrection impact is bounded (`can_serve_primary` requires
    `lease_deadline_ms > now_ms` at read time; the next scan re-expires) but
    the node flaps Healthy for up to one scan interval.
  - `now_ms` is captured before the globally serializing authority mutex
    (main.rs:2194 vs :2196-2199); with each command doing a full fsync
    checkpoint under that mutex, the timestamp used for runtime-map lease
    evaluation (main.rs:1446-1457) can be seconds stale — evaluating
    primary-lease liveness at a time in the past routes to a truly-expired
    primary.
  - A new leader with a slow clock can propose peering completions/expiries
    that treat actually-expired leases as live (`control_plane.rs:7751-7756`).
- R4a/R4b progress: process-level control-plane request/maintenance paths now
  re-read the authority clock only after acquiring the serializing authority
  mutex, immediately before constructing lease-expiry, heartbeat, and runtime-
  map read commands/evaluations. The experimental Raft process wrapper opts
  into that production resampling while tests can keep deterministic supplied
  timestamps. The replicated control-plane snapshot now carries
  `max_committed_timestamp_ms`, timestamp-bearing apply paths reject committed
  timestamp regressions, and heartbeats reject per-node lease-deadline
  regression. This narrows stale-time windows and prevents deterministic
  replay regression, but it does not solve the remaining R4 monotonic-clock /
  lease-read design: cross-process skew policy, restart clock discipline, and
  explicit successor-activation skew margins remain future work.

### R5. RESOLVED — Fresh-follower snapshot install can violate the log store's own invariants

Confirmed and fixed.

- `purge()` errors with "no committed restart gate" if `committed` is `None`
  (`control_plane_raft.rs:5900-5904`), and `save_committed` requires the
  committed log id to be a known local entry (:5850,
  `validate_known_log_id` :3781-3823, empty-log case :3786-3790).
- A brand-new node (no artifact → empty store,
  `restore_experimental_raft_durable_artifact` :2710-2716) whose first
  contact is a full-snapshot install (leader already purged its prefix) has
  `committed=None` and an empty log when OpenRaft installs the snapshot and
  purges through `snapshot.meta.last_log_id`. Either order of
  `save_committed`/`purge` trips an invariant → storage error → OpenRaft
  treats it as fatal. The existing follower-snapshot-catch-up test
  (:12650-12800) restores from an artifact that already has a committed gate,
  so this path is uncovered. Fail-closed (node cannot join), not state loss.
- Fixed by allowing `purge()` to establish the committed restart gate only for
  the one fresh empty-log snapshot-install shape: no existing committed gate,
  no retained entries, no previous purged boundary, and a persisted vote that
  covers the snapshot log id. Non-empty logs still reject purge without an
  existing committed gate.
- Coverage added:
  `control_plane_raft_log_store_allows_empty_snapshot_purge_to_establish_committed_gate`
  pins the narrow log-store rule, and
  `control_plane_openraft_fresh_follower_catches_up_from_leader_snapshot`
  exercises a voter that loses all local state after the leader snapshots and
  purges, then rejoins via `install_full_snapshot`.

### R6. MEDIUM-LOW — Any membership change bricks restart in peer mode

The restart artifact must match the static configured peer map exactly,
including OpenRaft's intermediate joint-consensus membership entries.
Confirmed.

- `validate_peer_policy_membership` (`control_plane_raft.rs:4196-4228`)
  requires every retained membership log entry, the applied membership, and
  the cached-snapshot membership to equal `Membership::from(configured_peers)`
  (`validate_configured_membership` :520-540, exact equality).
- The admin surface exposes `replace_voters`/`add_learner` (:2855-2880). A
  single `change_membership` writes a joint `[old, new]` membership entry
  into the log; the very next checkpoint embeds it, and the next restart
  fails closed. Learner membership likewise never matches. Acceptable only if
  membership change is out of scope for 12.3, but the APIs are live and
  nothing prevents calling them.
- Fixed for Phase 12.3 by making Unix-peer durable authorities remember their
  static configured peer policy and reject `replace_voters`/`add_learner`
  before calling OpenRaft. Dynamic configured peer-policy evolution remains a
  later Phase 12 production-cutover/membership slice, where committed
  membership changes must update durable config identity and restart
  validation together.

### R7. LOW — Missing artifact silently starts fresh

A deleted/misplaced state file on a single-node deployment silently
re-bootstraps at epoch 1, and on a multi-node peer re-opens the double-vote
hazard (persisted vote gone). Confirmed by design
(`control_plane_raft.rs:2710-2716`, bootstrap at main.rs:1907-1919,
:2073-2116). No "state previously existed" tripwire (sentinel lock file, or
peer cross-check of authority incarnation on rejoin).
- Fixed for Phase 12.3 by adding a durable sidecar sentinel next to the
  experimental OpenRaft restart artifact. Fresh startup is allowed only when
  both artifact and sentinel are absent; an existing sentinel with a missing
  artifact fails closed, and artifact/sentinel cluster or local-node identity
  mismatches are rejected before constructing OpenRaft state. Peer
  incarnation cross-checks remain part of a later production rejoin/membership
  slice.

### R8. LOW — Poison does not gate the Raft peer socket

`durable_poison` gates every client-facing path (submit main.rs:1403,
snapshot :1420, runtime map :1451, transfer :1558 — all serialized under the
single authority mutex, so it cannot be raced past on that surface), but
`spawn_experimental_raft_peer_rpc_worker` (main.rs:1721-1777) never checks
it: a durably-poisoned node keeps voting/replicating until the next
lease-scan tick hits the poison and exits (:2050, :2064-2067). Window is one
scan interval; each peer RPC's own checkpoint-failure `exit(1)` further
bounds it. Inconsistent rather than unsafe.
- Fixed by adding a shared durable-poison gate for the experimental Raft peer
  listener/worker path. Client-facing checkpoint failures now set both the
  detailed poison message and an atomic peer gate; peer workers reject before
  dispatch when the gate is already set, re-check after frame decode and
  identity validation immediately before OpenRaft dispatch, and re-check
  before writing a response so a worker blocked on I/O or the durable
  checkpoint lock cannot mutate or acknowledge after another path poisons the
  authority.

### R9. LOW — Peer identity envelope is assertion, not authentication; blocking I/O in async network

- Any process that can connect to the peer Unix socket can claim any
  configured `source` node id (`validate_incoming_frame_identity`,
  `control_plane_raft.rs:593-625` checks set membership only). The plan
  defers authentication (12.4) and filesystem permissions are the boundary;
  noted for completeness.
- Blocking Unix peer transport I/O is now offloaded from the async
  `RaftNetworkV2` methods onto Tokio's blocking pool. The transport still uses
  bounded `std::os::unix::net::UnixStream` connect/read/write with explicit
  socket timeouts, but stalled peers no longer occupy the async runtime worker
  that is polling the Raft network future. A regression pins this by racing a
  stalled peer read against an async runtime sleep, then confirming the RPC
  fails as `Unreachable` on the configured read timeout.
- Otherwise the RPC boundary held up: connect-per-RPC (no connection reuse →
  no request/response cross-matching), length-prefix bounds before allocation
  (:4694-4726), CRC + magic + version + direction + trailing-byte rejection
  (:4515-4574, reader at :5330-5443 with `read_collection_len` capacity
  guards), identity checked on both request and reversed response
  (:1020-1024). Delayed/stale frames from earlier terms are handled by Raft
  term logic; transfer-leader and snapshot frames carry votes and are
  validated by `handle_transfer_leader`/`install_full_snapshot` term checks.

### R10. RESOLVED — Process exits on some transient conditions in the serving loop

Status update: losing leadership between the serving check and the
`ExpireHeartbeatLeases` submit is now treated as benign leadership churn for
that scan iteration instead of `exit(1)`. The remaining
concurrently-committed bootstrap race is also closed: if
`BootstrapInitialClusterMap` returns the deterministic
`BootstrapRequiresEmptyState` rejection, or a transient forward-to-leader
error, the process performs a follow-up snapshot read and treats the error as
benign only if bootstrap state is now present. Empty state still fails closed.

### R11. INFO — Pinned alpha consensus dependency

`openraft = "=0.10.0-alpha.26"` (`crates/storage/Cargo.toml:15`). The adapter
compensates with unusually defensive invariant checks, but alpha semantics
(e.g. the exact `save_committed`/purge call ordering relied on by R5) may
shift.

## Raft: suspected issues ruled out (checked, correct)

- **R-OK1 — Client ack ordering (single node):** submit → outcome →
  checkpoint (temp file + `sync_all` + rename + parent-dir `sync_all`,
  `control_plane_raft.rs:4008-4041`, :4969-4980) → respond (main.rs:1399-1417,
  response written at :2208). A crash between OpenRaft apply and checkpoint
  loses only un-acked commands. The checkpoint write is atomic and fully
  synced including the file fsync.
- **R-OK2 — Read-index fail-closed is enforced in the serving path:**
  `runtime_map_via_openraft_read_index` (`control_plane_raft.rs:3527-3563`)
  obtains the barrier via `ReadPolicy::ReadIndex` (explicit quorum round, no
  clock lease), then fails with `ControlPlaneReadIndexNotApplied` if
  `last_applied < read_log_id` (:3546-3557). Serving from a state machine
  applied past the barrier is allowed and stamps the proof with the actual
  applied log id (:6163-6207) — linearizable. A deposed leader cannot pass
  the quorum round.
- **R-OK3 — Log/snapshot truncation invariants:** `truncate_after` refuses
  truncation below committed (:5853-5883); `purge` refuses regressing
  repurge, requires a committed gate and vote coverage, and promotes
  committed when purging past it (:5885-5915); snapshot install rejects index
  regression, same-index mismatch, and term regression (:6473-6521); the
  restart artifact cross-validates contiguity, committed/applied/purge
  ordering, and full snapshot+suffix replay equality (:4284-4421). No
  combination of independently-valid artifacts that silently loses state was
  found — inconsistent pairs fail closed.
- **R-OK4 — Deterministic apply boundary:** no apply-time clock reads (all
  timestamps come from command payloads), no I/O in replicated apply,
  `BTreeMap`/`BTreeSet` throughout, semantic rejections advance
  `last_applied` and are pure functions of (state, command)
  (`control_plane_command.rs:744-758`; raft mapping
  `control_plane_raft.rs:6231-6247`); replay equivalence pinned by tests.
  Command codec is canonical/total with strict bounds. The snapshot text
  formatter now prunes/sorts history before writing, and the parser rejects
  non-canonical byte forms by requiring a successful parse to reformat to the
  exact input bytes, closing the previous uppercase-hex/zero-padded/reordered
  equivalent-state caveat.
- **R-OK5 — Dual authority:** the experimental branch never returns
  (main.rs:1277-1280); both modes take the same exclusive `flock` on the
  state path (:171-229, :1290, :1838); flag toggling fails closed in both
  directions (raft loader rejects non-magic files
  `control_plane_raft.rs:3968-3973`; the legacy strict parser rejects the
  binary artifact `control_plane.rs:3273-3282` and only overwrites after a
  successful parse :3347). Caveats: advisory flock (same-host only), and the
  offline `control-plane-set-pg-acting-set` subcommand relies solely on
  format mismatch.

## Raft hardening recommendations

Phase 12.3 closeout: the experimental multi-process OpenRaft control-plane
slice now satisfies its stated exit criteria. Three-node process coverage
exercises durable peer startup, command replication, leader-routed
command/read/status service, leadership transfer, abrupt leader loss with
natural election, follower restart catch-up, and snapshot-transfer catch-up.
The remaining items below are either production-scale replacements for safe
but expensive 12.3 mechanisms, or deliberately deferred production-cutover
work.

1. **Replace full peer-response checkpoints with a vote/log WAL (R1 follow-up).**
   The process path now checkpoints before ack, so the immediate safety blocker
   is closed. A fsync'd WAL should still replace full-artifact-per-RPC
   durability before production-scale heartbeat and replication traffic.
2. **DONE — Atomic checkpoint capture + pre-write pair validation (R3).**
3. **DONE — Failover story (R2).** Unix-peer process mode now enables
   OpenRaft ticks, heartbeats, and elections while deterministic tests retain
   manual election control. Process coverage includes abrupt leader loss,
   natural election, post-failover commit, and follower checkpoint convergence.
4. **DONE — Cheap deterministic time guards (R4).** Process paths now re-read
   the monotonic clock after acquiring the authority mutex, immediately before
   constructing heartbeat, lease-expiry, and runtime-map operations. The
   replicated control-plane snapshot now carries `max_committed_timestamp_ms`;
   timestamp-bearing apply paths reject committed timestamp regressions, and
   `RecordNodeHeartbeat` also rejects per-node lease-deadline regression.
   These guards are pure functions of committed state, so deterministic replay
   is preserved.
5. **DONE — Fault-injection tests for the peer ack durability window (R1).**
   Deterministic checkpoint-pause coverage now asserts that a peer vote can be
   volatile while the client remains unacknowledged, that no response is
   released until the durable artifact contains the vote, and that poison before
   ack writes no response. The local "state previously existed" tripwire from
   R7 is also in place via the durable sidecar sentinel; peer
   authority-incarnation cross-checking remains deferred to the later production
   rejoin/membership slice.
6. **DONE — Cover the fresh-follower snapshot path (R5).** Leader purges,
   brand-new empty peer joins via `install_full_snapshot`; `purge()` now
   establishes the committed gate only for the fresh empty-log snapshot-install
   shape.
7. **DONE — Membership-change restart coverage (R6).** Unix-peer durable
   authorities reject `replace_voters`/`add_learner` in static peer mode until
   config-driven membership evolution is designed.
8. **DONE — Gate the peer socket on poison (R8).** A shared atomic poison
   flag is checked before peer dispatch and again before peer response write.
9. **DONE — Smaller parser/conformance items:** the snapshot text parser now
   accepts only canonical formatter bytes. The guarded log-store audit now runs
   the compatible `openraft::testing::log::suite` cases and documents the
   upstream deviations: OpenRaft's generic suite still uses a synthetic blank
   `(term=0,index=0)` entry, while Argmin accepts only the real bootstrap
   membership entry there, and Argmin rejects purge/truncate shapes that would
   cross or erase the committed restart watermark.

---

# Part 2: Phase 11 production path and data plane

## 2a. Single-authority control plane (`control_plane.rs`, authority process wiring)

Architecture note (context for CP1/CP2): the data path never consults the
authority per request. Fencing is (1) every storage RPC carries
`cluster_epoch` and the node rejects mismatches
(`storage_node_server.rs:9224-9232`); (2) the node's installed route map
carries `route_map_valid_until_ms` = min primary lease deadline, checked
against local wall clock on every serving validation
(`storage_node_server.rs:9158-9168`, :9235-9245); (3) primary-serialized ops
additionally check `route.primary_node_id == self`; (4) installs are
epoch-monotonic; (5) the authority only expires a lease after its wall clock
passes the deadline, and only then can peering elect a successor. Correctness
of the chain reduces to: lease deadlines must be monotonic per node on the
authority side, and wall clocks must agree. Both assumptions have holes.

### CP1. HIGH — Out-of-order heartbeat application regresses lease deadlines; no per-node monotonicity guard

Confirmed race (verified directly); timing plausible.

- `RecordNodeHeartbeat` unconditionally overwrites `lease_deadline_ms`,
  `last_heartbeat_ms`, and `last_observed_epoch` (`control_plane.rs:1341-1343`
  stale-epoch branch, :1392-1394 current branch). There is an incarnation
  regression check (:1303-1309) but no timestamp monotonicity check.
- The RPC worker captures `now_ms` after reading the request but before
  acquiring the authority mutex (main.rs:2187-2199). The client heartbeat
  retry loop (`control_plane.rs:4102-4133`, budget = lease duration, 10ms
  backoff, new connection per attempt) makes concurrent duplicates routine:
  the original request stalls on the mutex, the client times out (1s read
  timeout), the retry with a later `now_ms` wins the lock and grants deadline
  D2; the delayed original then applies with the earlier `now_ms`, regressing
  the recorded deadline to D1 < D2.
- Failure scenario: node A holds a map with `valid_until = D2` (issued from
  the D2 state); the authority now believes A's lease ends at D1. The expiry
  scan (main.rs:1334-1338) expires A at D1, PGs peer, a new primary activates
  — while A's installed map remains valid until D2 (up to a full lease
  duration later). Two Active primaries in overlapping windows: A serves
  stale reads and can ack old-epoch operations against any old-acting-set
  peer that has not refreshed.
- The same regression undermines the metadata-transfer fence wait:
  `FencePgForMetadataTransfer` captures the (regressed)
  `record.lease_deadline_ms` (`control_plane.rs:1746-1751`) and the
  orchestrator waits only until that value (main.rs:845-847), then exports
  while the quiesced source may still serve.
- Milder sibling: the stale-epoch overwrite of `last_observed_epoch` (:1341)
  applied after a current-epoch heartbeat flips the primary back to
  non-serving and makes `active_pg_route` fail (see CP11).

### CP2. HIGH — Fencing is wall-clock based with zero margin; a backward clock step (or multihost skew) reopens an expired serving window

Confirmed (design gap, partially acknowledged in code comments).

- All deadlines are `SystemTime`-derived ms (`clock.rs:57-66`); the node
  stops serving when `valid_until_ms <= now_ms`
  (`storage_node_server.rs:9158`, :9235); the authority expires when
  `lease_deadline_ms <= expire_at_ms` (`control_plane.rs:1477-1479`). No
  monotonic anchoring, no grace margin between "old primary must stop" and
  "successor may be activated" — they meet at the same instant.
- Single host, today: authority expires A's lease at D, peers, activates a
  successor at D+ε. NTP steps the clock back by more than ε: A's installed
  old-epoch map satisfies `valid_until > now` again and A resumes serving
  (stale reads; write fencing then rests solely on the per-node epoch check
  plus strict all-replica writes). On real multihost, constant skew δ gives
  the dead primary a δ-wide overlap with the successor on every failover.

### CP3. MEDIUM-HIGH — Loss/restore of the single state file silently resets authority incarnation and cluster epoch

Confirmed mechanics; scenario plausible.

- `FileControlPlaneStore::load` maps `NotFound` to `Ok(None)`
  (`control_plane.rs:3273-3282`); `open()` then starts from
  `ClusterControlSnapshot::empty()` with `AuthorityIncarnation::INITIAL` and
  does not bump (:3334-3349, restart bump only `if loaded_existing_state`);
  the manager auto-bootstraps when the node set is empty
  (main.rs:1300-1307, :2118-2143).
- The Phase 11 requirement "epochs and authority incarnations must never be
  reused" lives entirely in one file. Delete/misplace/restore-from-backup and
  the authority reissues incarnation 1, epoch 2. Live nodes fail closed-ish
  (`FutureNodeObservedEpoch`, `control_plane.rs:1310-1316`; the install
  downgrade guard `storage_node_server.rs:1233-1237` is in-memory only), but
  any restarted node or frontend accepts the reborn low-epoch world, producing
  a mixed-epoch cluster against durable PG replica state that remembers higher
  epochs. Nodes persist their own incarnation counter durably but never
  persist the highest authority incarnation/epoch they have observed.

### CP4. MEDIUM — Runtime-map freshness proofs are issued and parsed but validated by no production consumer

Confirmed absence.

- `RuntimeMapFreshnessProof` (`control_plane.rs:2343-2405`) is
  encoded/decoded and shape-checked (:5517-5555), but no non-test consumer of
  `freshness_proof()`/`is_serving_authority_read()`/`authority_incarnation()`
  exists outside the control-plane files.
  `StorageNodeProcessConfig::from_runtime_map`
  (`storage_node_server.rs:362-401`) and the frontend install
  (`cluster.rs:1123-1177`) accept any map with epoch ≥ current.
- Nothing structural prevents a `Reconstructed` map (no lease deadlines,
  `valid_until = None` → `is_route_map_valid_at` returns true forever,
  `storage_node_server.rs:407-410`) from being installed as a serving map.
  Today only orchestration paths receive `Reconstructed` maps, so the
  protection is convention, not enforcement. Authority-incarnation
  regressions (CP3) are likewise invisible to consumers.

### CP5. MEDIUM — Unbounded protected cluster-map history + full snapshot embedded in every heartbeat response, with an 8MB frame cap → cliff failure of all lease renewals

Confirmed mechanics.

- History pruning exempts protected records from the 256-record cap and stops
  when only protected records remain (`control_plane.rs:8270-8284`).
  Protection includes `retain_from_epoch =
  min(cluster_map_history_floor_epoch)` over all node records — including
  `Out`/`Removed` nodes that can never heartbeat again (:8252-8268;
  `SetNodeMembership(Removed)` keeps the record, :1180-1196). A dead node's
  last-reported floor freezes retention forever; every epoch bump appends a
  full nodes+PGs+observations record.
- Every heartbeat-refresh response embeds the entire formatted snapshot
  including all history (`write_heartbeat_lease` →
  `format_snapshot(lease.snapshot())`, `control_plane.rs:5113-5123`; snapshot
  cloned into every lease :3525-3532), and the runtime map re-derives one
  historical route per PG per retained epoch (:674-684). Frame reads reject
  payloads > 8MB (:4901-4905). When the state text crosses that threshold,
  every heartbeat refresh fails to decode → cluster-wide lease loss,
  fail-closed outage with no recovery except manual state surgery.
- Secondary cost: heartbeat is always `changed=true` (:1459-1464) so every
  heartbeat rewrites and fsyncs the whole state file (:3861-3873, :3284-3324);
  authority latency grows with history and feeds the CP1 retry amplification.
- Related sharp edge: a node whose reported floor references a pruned epoch
  has its heartbeats rejected permanently (:7699-7734) — safe but
  unrecoverable via the control plane.

### CP6. MEDIUM — Cross-epoch metadata-proof "progress" accepts any divergent proof once the epoch has advanced past the floor epoch

Confirmed behavior, low exploitability. (Same finding reached independently
from the routing side; see CL6.)

- `metadata_proof_satisfies_active_primary_observation_floor_impl`
  (`control_plane.rs:7878-7905`): with `LocalEpoch` provenance,
  `floor_epoch < observed_epoch && hash != 0 && hash != floor.hash && digest
  != floor.digest` is accepted as progress, and the heartbeat path promotes
  it as the new active floor (:1438-1451). Because `bump_epoch` (any
  unrelated membership/endpoint/availability change) leaves PGs Active while
  clearing observations (:727-733), every long-lived Active primary ends up
  one epoch past its `active_metadata_proof_epoch`, at which point the
  divergence check degrades to "different is fine". A primary whose durable
  store forked/rolled back without a process restart (no incarnation bump) is
  indistinguishable from legitimate progress; the promoted floor then makes
  honest replicas look divergent instead.
- This is the remaining consequence of epoch-local, unorderable log tuples
  after Slice 3 removed the same-log digest-only escape. Mitigated by:
  restart ⇒ incarnation bump ⇒ Peering agreement; threat model trusts nodes.

### CP7. LOW-MEDIUM — Peering completion requires agreement only among currently-healthy acting members, and the floor comparison is index-only

Confirmed behavior; requires a prior replication fault to matter.

- `validate_pg_peering_observations` silently skips acting-set members that
  are unhealthy or lease-expired (`control_plane.rs:7751-7757`); a PG can
  activate on one surviving member's proof. The floor check
  (`metadata_proof_satisfies_active_floor`, :7809-7814) is
  `observed.applied_log_index > floor.applied_log_index || observed == floor`
  — a higher index with an unrelated hash chain passes; nothing proves the
  floor is an ancestor of the observed proof. The `928f2b77` regression
  covers same-log divergence, not ahead-of-floor divergence.

### CP8. RESOLVED / FOLLOW-UP — Admin acting-set/fence RPC response-loss confirmation is no longer observational

Confirmed.

- `SetPgActingSet`/fence requests carry only `pg_id`(+set)
  (`control_plane.rs:5048-5079`); retry-after-response-loss confirms by
  observing the current route (:4221-4269) — it can claim success for a state
  actually produced by a different admin's command, or report
  `RpcUnconfirmed` for its own applied change that was superseded. The
  transfer orchestrator partially compensates (main.rs:979-986 plans the
  exact destination epoch). A CAS-style `expected_cluster_epoch` would make
  these unambiguous.
- Status update: checked control-plane RPC helpers no longer blindly
  resubmit after an ambiguous response loss. `set_pg_acting_set_checked`
  observes the current target-PG runtime-map snapshot and returns
  `RpcUnconfirmed` if that route differs from the requested acting set,
  preventing a stale retry from clobbering a later admin transition without
  requiring unrelated PGs to be serving. Metadata-transfer fence/install
  helpers use operation-specific observability predicates rather than treating
  any successful runtime-map response as confirmation. Those response-loss
  confirmation probes keep a bounded but longer read timeout than ordinary
  control-plane RPCs, so a slow proof observation does not turn an already
  applied command into `RpcUnconfirmed`. The remaining follow-up is the
  stronger CAS-style `expected_cluster_epoch` request field, tracked in the
  hardening plan.

### CP9. LOW — Single-writer enforcement is external to the library

The `flock` lives only in `argmin-s3` (main.rs:171-215, taken by the manager
:1290-1294 and the offline admin :645);
`SingleAuthorityControlPlane::open`/`FileControlPlaneStore::save` neither
acquire nor verify any lock, and `save` never re-checks on-disk incarnation
before rename. Any new caller that skips the lock can double-write. A
SIGSTOP'd manager keeps the flock (good — no second manager can start), but
the guarantee is advisory and single-filesystem.

### CP10. LOW — State file has no integrity envelope

Persistence is atomic (temp + fsync + rename + dir fsync,
`control_plane.rs:3284-3324`) and the parser fails closed on unknown lines
and inconsistent state (:6549-6744, validators :6768-6974). But there is no
whole-file checksum, record count, or terminator: a file truncated at a line
boundary that remains self-consistent parses successfully; nothing binds the
file to a cluster identity, so restoring the wrong cluster's file is
undetectable (compounds CP3).

### CP11. LOW (availability) — One lagging PG primary makes the whole-map read RPC fail

`runtime_map`/`pg_routes` propagate
`PgHasNoServingPrimary`/`PgPrimaryMissingActiveObservation` as an error for
the entire map (`control_plane.rs:454-460`, :516-521, :313-362), so frontends
cannot refresh any route between "primary stops heartbeating" and "expiry
scan moves the PG to Peering"; frontend maps then hit `valid_until` and fail
closed globally. The storage-node refresh path has per-case relaxations
(:364-452); the frontend path has none. Interacts with CP1 (a stale heartbeat
regressing `last_observed_epoch` triggers exactly this).

### Single authority: checked and found sound

- Epoch monotonicity within a live authority: all mutations flow through
  `apply_control_plane_command` → `bump_epoch`/`bump_authority_after_restart`;
  save-before-expose ordering is correct (`commit_snapshot` saves before
  swapping in-memory state :3861-3873; leases/maps built only from the
  committed snapshot).
- Fence/install crash windows: fence is idempotent; a fenced PG blocks
  peering completion (:1954-1960, :2052-2058); the marker survives node
  failures and authority restarts (history protection :8252-8268, re-checked
  by the parser :6906-6949); resumed transfers re-verify artifact ⟷ marker
  equality (main.rs:908-934). The only un-fence paths are the install itself
  and `SetPgState` (:1846-1901), which is not exposed over the RPC surface.
- Heartbeat refresh idempotency: re-applying a heartbeat is idempotent apart
  from the CP1 timestamp issue; fence retry re-issues (idempotent no-op);
  transfer-install retry is observation-only with a min-epoch + exact-marker
  predicate (:4550-4570).

## 2b. Metadata replication, command log, PG store recovery

### MD1. RESOLVED — Terminal pending cleanup no longer relies on primary-last fanout

Combined with bind-path recovery, this destroys convergence evidence.
Confirmed (verified directly: sort key, commit, guide text, recovery mode).

- `crates/storage/src/cluster/request_ops.rs:1380` and :1500:
  `nodes.sort_by_key(|node| node.node_id() != primary_node_id)` sorts the
  primary first (false < true). Commit `a9013451` ("Keep metadata reissue
  primary checks under lock", 2026-06-14) flipped this from `==`
  (primary-last) to `!=` (primary-first) for both apply and abandon fanout.
- `guides/metadata-command-stream.md:406-409` still mandates primary-last:
  "recovery convergence must use the same primary-last apply ordering as
  normal command fanout. A terminal primary log row is not sufficient reason
  to clean the primary pending slot until all required replicas have either
  converged or the PG has failed closed for repair."
- Failure scenario: primary installs the slot, applies+records locally
  (advancing its chain), crashes before any replica applies. On restart via
  `StorageNodeServer::bind` (`storage_node_server.rs:1013`) or the pre-bind
  heartbeat path (main.rs:2565), `PgStore::recover`
  (`pg_store/command_log.rs:2456`) runs
  `validate_metadata_command_replay_state` with `CleanTerminal` (:2413-2423,
  :3016-3019), and `validate_pending_metadata_command_slot_relation`
  (:3344-3361) classifies the slot as terminal on local evidence only and
  deletes it. The replicas never received the command and the only durable
  record that fanout was incomplete is gone. Primary-last is precisely what
  made local `CleanTerminal` safe (a terminal primary row implied all
  replicas had already applied); primary-first breaks that safety argument.
- The local-cluster builder path is unaffected: it uses the preserving
  variant plus cluster-wide agreement/convergence
  (`cluster/local.rs:3809-3893`, :3936-3980, :4070+ — which accepts both
  shapes, so the builder tolerates primary-first).
- After bind-path cleanup, replicas are permanently one command behind with
  no pending slot; the PG can only heal via peering/metadata-transfer repair
  (which is not wired — CL2) instead of cheap slot convergence, and until
  then every subsequent command hits MD2.
- Not acknowledged-write loss (the mid-fanout crash never acks the client);
  it converts a designed-convergeable state into a repair-required state and
  violates a written invariant.
- Status update: fixed by keeping the current primary-first fanout but
  removing the unsafe local cleanup dependency on primary-last ordering.
  `PgStore::recover` and storage-node bind recovery now preserve same-epoch
  terminal pending slots because a single replica cannot prove acting-set
  convergence. Terminal pending-slot cleanup is now command-owned or
  cluster-level after explicit acting-set evidence. Final bucket deletion was
  moved behind a replicated `DeleteFinalizedBucket` metadata command so that
  final cleanup is logged instead of relying on digest-only local repair.
  Regression coverage pins bind recovery preserving terminal slots, heartbeat
  observation remaining non-mutating, and divergent/finalized replica cleanup
  requiring acting-set evidence.

### MD2. RESOLVED — Gap-index command apply is rejected before mutation

Confirmed (verified directly: no contiguity check; post-mutation revision
marking).

- `apply_metadata_command_and_record` (`pg_store/metadata.rs:718-776`)
  accepts any command whose (epoch, pg, index) has no log row;
  `metadata_command_acceptance` (`command_log.rs:3443-3481`) has no
  contiguity check against `applied_log_index` (only PG id, digest mismatch,
  exact-entry lookup). Nor does `validate_metadata_command_for_replica`
  (`cluster/local.rs:3133-3218`).
- If a replica receives command N+1 while at applied index N-1 (exactly what
  MD1 produces), it applies the row mutations, inserts a sparse log row, and
  `advance_metadata_command_log_state_with_inserted` returns without
  advancing (`command_log.rs:4065-4069`), yielding `digest_revision:
  self.metadata_digest_revision()?` — the post-mutation revision inside the
  txn. `apply_metadata_command_and_record` then calls
  `mark_metadata_state_digest_clean_at_revision(record.digest_revision)`
  (`metadata.rs:767`). Result: `metadata_command_replica_state.state_digest`
  no longer matches the materialized rows, but
  `metadata_state_digest_mismatch` (`command_log.rs:3858-3876`)
  short-circuits on the clean-revision match and reports "no mismatch". The
  replica keeps returning `Ok` to fanout — the origin acks clients while this
  replica never advances its chain — until heartbeat
  (`metadata_command_replica_state_for_heartbeat`, :2543-2566, which bypasses
  the revision fast path) or restart replay validation fails closed.
- The guide (`metadata-command-stream.md:420`) says an unadvanced applied
  tail without matching materialized state "is not recoverable and must fail
  closed"; the online path both creates that unrecoverable durable state and
  masks it in-process.
- Status update: PgStore command acceptance/recording now rejects a fresh
  non-contiguous log index before materialized metadata rows are mutated, and
  the storage RPC codec/handler reject route epoch vs embedded command epoch
  disagreement. Focused regressions cover gap-index apply rollback, stale
  epoch rejection, and the RPC epoch boundary. This closes the concrete MD2
  sparse-apply/digest-mask path.

### MD3. RESOLVED / FOLLOW-UP — PgStore rejects stale command epochs before chain rewind

Confirmed mechanism; exploit window depends on lease discipline. (Same
finding reached independently from the routing side; see CL3 for the
reachability analysis.)

- `advance_metadata_command_log_state_with_inserted`
  (`command_log.rs:3884-3894`): if `state.cluster_epoch != command epoch`, it
  unconditionally resets replica state to `(command_epoch, index 0, hash 0)`
  and refreshes digests. This is how forward epoch transitions rebase, but it
  is direction-blind: a command carrying epoch N recorded on a store already
  at N+1 rewinds the durable replica state to epoch N.
- The only fences are cluster-map-level: `validate_metadata_command_for_replica`
  requires `command epoch == self.epoch` plus `require_route_map_valid_now()`
  (`cluster/local.rs:3150-3159`), i.e. lease-based fencing. A node that
  missed an epoch change entirely accepts stale-epoch commands until its
  route-map validity expires; any lease/clock bug lets a stale primary rewind
  a replica. Nothing legitimate ever needs a rewind (transfer paths install
  state through dedicated fenced entry points, `command_log.rs:1172-1579`).
- Status update: PgStore now rejects `command_epoch <
  replica_state.cluster_epoch` both at acceptance and at replica-state
  advancement, so stale commands cannot rewind the durable replica chain.
  Forward epoch adoption is still allowed for the first command in the new
  epoch; making that require an explicit epoch-transition/transfer token
  remains a follow-up.

### MD4. RESOLVED — Future-epoch orphan cleanup now fails closed

Plausible, narrow window.

- `clean_epoch_mismatched_orphan_pending_metadata_command_slot`
  (`command_log.rs:2055-2085`) deletes any slot whose epoch differs from the
  stored replica epoch when no terminal entry exists locally. The guide
  itself notes a legitimate command can install a future-epoch pending slot
  before apply/record advances the store epoch
  (`storage-cluster-invariants.md:135-137`). If the first command of epoch
  N+1 is applied on replicas before the primary records (this ordering exists
  in open-time convergence, `cluster/local.rs:3944-3946`, which is
  primary-last), a primary crash in that window leaves: slot epoch N+1,
  primary store epoch N, replicas advanced under N+1. Recovery phase A
  classifies the slot as an orphan and deletes it — violating the documented
  zero-replica-apply abandon boundary (`metadata-command-stream.md:428-433`).
  The PG then fails closed as divergent instead of converging. Not silent
  loss (client never acked; replica state survives), but recovery
  misclassifies an in-flight command as an orphan.
- Fix shape: distinguish slot-epoch-behind-store (true orphan) from
  slot-epoch-ahead-of-store (possible in-flight next-epoch command; needs
  acting-set evidence before deletion).
- Status update: local recovery now makes this direction-aware. Pending slots
  from older epochs remain locally cleanable as orphans, but future-epoch
  pending slots fail closed and remain durable instead of being erased without
  acting-set evidence. Focused PgStore regressions cover both directions, and
  the local-cluster reopen regression now asserts that a future-epoch slot is
  rejected rather than silently cleaned.

### MD5. MEDIUM — `PRAGMA synchronous=NORMAL` under WAL makes acked commands and pending slots non-durable across power loss

Confirmed pragma (`schema.rs:759-764`); consequence scenario plausible.

- In WAL/NORMAL, commits are only fsynced at checkpoint; OS crash/power loss
  can roll a node back to an earlier committed prefix (process crash is
  fine). Scenarios: (a) all acting replicas applied and acked; power loss on
  one replica rewinds its tail → restart agreement sees prefix disagreement
  with no pending slot anywhere → fail-closed divergence for an acknowledged
  write; (b) the primary loses its pending-slot insert while a replica
  already applied — exactly the "ambiguous" shape the design excludes. No
  compensating fsync barrier exists on the command-apply path.
- If NORMAL is a deliberate performance choice it needs an explicit
  `wal_checkpoint`/`synchronous=FULL`-on-command-commit story, or the docs
  must state that node power loss is a repair event, not a crash-recovery
  event.

### MD6. LOW — Guide/code drift on load-bearing invariants

`metadata-command-stream.md:406-409` (primary-last fanout), the in-flight
restart exception (:115-124 — code also accepts the primary-advanced shape,
`cluster/local.rs:4112-4210`), and `storage-cluster-invariants.md`'s recovery
boundary section (which does not mention that bind-path `CleanTerminal` can
remove a slot the acting set still needs). The guides are the
review/verification authority, so the drift is itself a correctness-process
defect. (The Phase 10.5 plan text "primary-last fanout ordering" at
plan line ~5754 is likewise stale.)

### MD7. LOW — Miscellaneous

- `compact_metadata_command_log` hardcodes `node_id 0` into slot reads and
  validation (`command_log.rs:3777`, :3785) — misattributed diagnostics in
  typed errors.
- `pending_metadata_command_slot_any_epoch` panics via `.expect` on a zero
  epoch/log-index slot row (`command_log.rs:1975-1979`, :1996-2000) instead
  of failing closed with a typed error; schema CHECKs make this unreachable
  via SQL, but a hand-corrupted DB gets a panic.
- Chain and digests are CRC64 throughout (`metadata_command.rs:1162-1177`);
  the chain link commits to the command checksum, not the bytes. Fine for the
  stated bitrot goal; not collision-safe against adversarial divergence. The
  xor+sum+count table digest (`command_log.rs:797`, :4650-4676) adequately
  mitigates accidental xor-cancellation.

### Metadata replication: checked and found sound

- **Apply determinism:** command payloads are storage-shaped post-images; all
  timestamps travel in the command (`metadata.rs:1038-1085`, :3830-3858,
  :4306+). Legacy clock-using mutators are `#[cfg(test)]` (`metadata.rs:6461`,
  :9213). Apply preconditions validate local state and fail to fanout rather
  than diverging silently. No autoincrement/iteration-order dependence found;
  counters use max()-advance semantics.
- **Atomicity:** acceptance + apply + log insert + hash-chain + digest
  maintenance + replica-state advance are one `BEGIN IMMEDIATE` txn
  (`metadata.rs:718-776`); rollback invalidates the clean-revision cache.
  Slot removal is a separate txn requiring an exactly-matching terminal log
  entry (`command_log.rs:2249-2341`). Truncated logs cannot pass validation
  (hash chain walked from checkpoint base to applied index :2904-2996;
  recovery digest check is a full materialized recompute :3039-3048).
- **Recovery ordering and coverage:** orphan cleanup precedes replay
  validation in `PgStore::recover` (:2456-2484) and the builder
  (`cluster/local.rs:1768-1806`); digest-drift detection deliberately
  precedes refresh (:2513-2541). All three serve paths run recovery: server
  bind (`storage_node_server.rs:1013`), pre-bind heartbeat (main.rs:2565),
  local builder (`cluster/local.rs:1787-1806`). No bypass found. Heartbeat
  observes without reconciling (`node.rs:1275-1304`).
- **Fanout/ack semantics:** the origin only acks after every acting node
  applied; retries converge the identical command id/bytes; partial-exact
  retry demands hash-chain proof from earlier-applied nodes
  (`cluster.rs:2050-2120`); reissue (new command id) is gated by acting-set
  max-index/payload proofs (`cluster.rs:2168-2310`). Single pending slot plus
  per-node `BEGIN IMMEDIATE` prevents cross-replica interleaving of two
  commands.
- **Transfer:** destinations reject pending slots and dirty state, verify
  self-checksummed checkpoints and digests, seed chains contiguously from 0
  with fail-closed conflicts (`command_log.rs:1172-1579`,
  `cluster.rs:3200-3470`); fencing has explicit source-lease deadlines.
  Moderate assurance (shape verified, not exhaustively traced).

## 2c. Cluster routing, epoch fencing, peering

Architecture note: a `StorageCluster` handle is immutable —
`operation_epoch` and the `LocalClusterMap` epoch are captured at
construction and are equal by construction (`cluster.rs:3733-3735`). Epoch
fencing is therefore not enforced by the handle's own `operation_epoch !=
cluster_epoch` comparisons (those can only fire for test-constructed
handles); the real fences are (a) the route-map wall-clock validity lease
checked frontend-side per routing decision (`cluster/local.rs:2946-2995`,
:1857-1870) and (b) per-RPC validation on the serving storage node. This is a
sound lease-fencing design for failure-driven transitions; the findings are
where it leaks.

### CL1. HIGH — Deposed primary can serve stale reads after a new primary accepts writes; no wait-out of the old primary's lease on administrative transitions

Mechanism confirmed; window deployment-dependent.

- `SetNodeMembership(Out)` immediately nulls the node's lease, marks its PGs
  `Peering`, and bumps the epoch (`control_plane.rs:1185-1209`). Peering
  completes as soon as the remaining healthy acting nodes report matching
  proofs at the new epoch — unhealthy/Out members are skipped
  (`control_plane.rs:7744-7757`), and `CompletePgPeering`
  (`control_plane.rs:1902-2007`) contains no fence on the deposed primary's
  previously-issued lease/route-map validity. Contrast: metadata transfers
  carry exactly this fence (`metadata_transfer_fence_source_lease_deadline_ms`,
  `control_plane.rs:1732-1824`), so the hazard class is known.
- The deposed node O cannot learn the new epoch: an Out node's heartbeat is
  rejected (`NodeCannotReceiveLease`, `control_plane.rs:1294-1302`), so O
  keeps its old config ("PG X Active, primary = O, epoch N") and its per-RPC
  checks pass until its local wall-clock `route_map_valid_until_ms` expires.
  Stale frontends (epoch-N maps within validity) route reads to O.
- Failure scenario: admin marks healthy node O Out at T; peering completes
  and new primary P accepts a PUT at T+2s; a frontend holding the epoch-N map
  reads the object via O at T+5s and gets the old version (or NoSuchKey).
  Window ≈ remaining route-map validity, up to a full heartbeat lease
  duration. Writes are safe (strict fanout hits refreshed replicas → typed
  epoch rejection); reads are not.
- The same pattern applies to any epoch bump where the previous primary
  remains process-alive but is excluded from the observation set (endpoint
  change, incarnation change: `control_plane.rs:1331-1340`, :1377-1391).

### CL2. HIGH (availability) — Divergent replicas permanently wedge a PG in `Peering`; the implemented catch-up path has no production caller

Confirmed.

- A crash mid-fanout leaves the primary ahead (see MD1); open-time recovery
  removes the terminal pending slot but does not reconcile the log suffix
  across replicas.
- Peering readiness demands identical proofs from all healthy acting nodes;
  `PgPeeringMetadataProofMismatch` is silently swallowed and the PG simply
  skipped (`control_plane.rs:1039-1067`), with no metric/alert and no repair
  action. The full reconciliation machinery —
  `reconstruct_pg_peering_from_retained_metadata_log`,
  `replay_pg_peering_catchup_from_retained_metadata_log`
  (`cluster.rs:2529-2656`), `peering.rs` replay-plan validation — is
  `#[allow(dead_code)]` with only test callers.
- `deterministic_pg_primary_for_snapshot` picks the first serving acting
  node, not the longest verified chain (`control_plane.rs:2213-2224`); if the
  picked primary is behind, even the (unwired) catch-up path fails closed
  with `ReplicaAheadOfPrimary` (`peering.rs:280-285`) — and nothing truncates
  unacked entries.
- Failure scenario: primary crashes after local apply, restarts (incarnation
  bump → epoch bump → Peering); replicas lag by one entry; proofs never
  converge; the PG serves nothing, forever, and nothing surfaces why. Data is
  safe (the extra entry was unacked); availability is not.

### CL3. RESOLVED — Storage RPC and PgStore now fence stale command epochs

Confirmed code; reachability plausible.

- The RPC layer's `request.cluster_epoch` is the client's map epoch, not the
  command's epoch (`node_client/unix_rpc.rs`
  `encode_metadata_command_request`), and the server never cross-checks
  `request.cluster_epoch == request.command.id().cluster_epoch()`
  (`storage_node_server.rs`
  `metadata_command_apply_and_record_response_with_allowed_states`).
- Reachability: (a) a client bug sending a stale-epoch command under a
  current-epoch request; (b) the TOCTOU window between `validate_pg_route`
  and `pg.apply_...` while `install_control_plane_runtime_config` swaps the
  config concurrently (`storage_node_server.rs:1211-1240` — no PG lock
  taken); (c) the in-process `LocalStorageNodeClient`, which performs no
  route/epoch validation at all (`node_client/local.rs:3986-4003`) —
  currently confined to single-process deployments where the epoch is static.
- Consequence when hit: replica-state epoch regression and cross-epoch log
  interleaving on one node → divergence → CL2 wedge, or fail-closed digest
  mismatch. Cheap to fix; pure defense-in-depth today.
- Status update: fixed with the MD3 hardening. The storage RPC encode/decode
  boundary now rejects route epoch vs embedded command epoch disagreement, and
  PgStore rejects stale command epochs before replica-state advancement. A
  stale client cannot use a current-epoch RPC envelope to apply an older
  command and rewind the replica chain.

### CL4. MEDIUM — Lease fencing compares authority-issued deadlines against local wall clocks with zero skew margin

Confirmed. (Node-side counterpart of CP2.)

- Serving nodes: `crate::clock::current_time_millis()` vs
  `route_map_valid_until_ms` (`storage_node_server.rs` validate); frontends:
  same pattern (`cluster/local.rs:1868-1870`). Deadlines are minted from
  `authority_now_ms` (`control_plane.rs:3510-3512`). A node whose clock runs
  slow by S keeps serving S past its authority-side expiry — exactly the
  window in which `ExpireHeartbeatLeases` may bump the epoch and re-activate
  the PG elsewhere. The expiry-driven fence is otherwise exactly aligned, so
  clock skew is the entire residual error term, and no margin is subtracted
  anywhere.

### CL5. MEDIUM — Object-payload leases and reclaim fences are process-local in multihost frontends; the invariants doc describes them as storage-node-owned

Confirmed.

- `install_unix_storage_node_clients` replaces every per-node client except
  `storage_client` (`cluster/local.rs:1998-2056`); only
  `LocalStorageNodeClient` implements `StorageNodeClient`
  (`node_client/local.rs:1612`), and multihost frontend maps are built
  topology-only (`cluster.rs:3781-3798`), so
  `try_acquire_object_payload_lease_on_locations`,
  `object_payload_lease_count`, and `try_begin_object_payload_reclaim`
  (`cluster/local.rs:2753-2846`, backed by `node.rs:1503-1620`) all operate
  on in-process stubs. A lease held by frontend A is invisible to reclaim on
  frontend B: `reclaim_object_payload_if_unleased_with_outcome`'s lease
  checks (`request_ops.rs:9494-9545`) pass and physical deletion begins.
- What actually saves cross-process readers is the storage-node-side shard
  delete fence: `shard_delete_response` takes `try_begin_shard_delete`, which
  conflicts with active RPC read handles (`storage_node_server.rs:6700-6712`,
  :8959-8966, :9686-9712). So the race degrades to retryable mid-read
  failures (the reader loses shards for segments whose read handles it has
  not yet acquired), not corruption — but
  `guides/storage-cluster-invariants.md:173` does not match the multihost
  implementation, and `reclaim_object_payload_if_unleased`'s name
  overpromises. Note `LocalStorageNodeClient::acquire_read_handles` is a
  no-op (`node_client/local.rs:132-146`), so in-process deployments rely
  solely on the payload-lease layer.

### CL6. LOW-MEDIUM — Cross-epoch peering-floor relaxation accepts any sufficiently-different proof; plain acting-set swaps bypass the transfer-proof path

Plausible. (Control-plane view of the same gap as CP6.)

- Because per-epoch logs restart at index 0 (MD3's reset), proofs are
  incomparable across epochs, and the floor check accepts any observed proof
  with `floor_epoch < observed_epoch` whose hash and digest merely differ
  (`control_plane.rs:7878-7905`). With strict writes this is safe for acked
  data in the failure path, but a plain `set_pg_acting_set` (no transfer
  proof) that swaps in stale-but-non-empty replicas could activate a PG below
  the last-active floor without tripping this check. Recommend guarding
  acting-set changes for non-empty PGs behind the transfer-proof path.

### CL7. LOW — Error shadowing in reclaim violates the project's own release-on-error rule

`request_ops.rs:9725-9731`: on action failure, `release_reclaim_claim()?`
propagates the release error, discarding the original action error.
`guides/object-concurrency.md` checklist item 12 explicitly requires the
action error to win.

### CL8. Notes (confirmed, not defects)

- The handle-internal staleness checks
  (`require_current_payload_operation_epoch`, `cluster.rs:3880-3890`;
  `StaleMetadataOperation` in `cluster/local.rs:2946-2957`) can never fire on
  production-constructed handles; document that the effective fences are the
  node-side RPC validation + validity lease, so future refactors do not
  assume the handle checks carry weight.
- Bucket write drain is solid within one primary: reservation acquire is
  SQL-atomic against the drain row (`pg_store/metadata.rs:5397-5419`),
  bucket-control pending-slot insert is SQL-atomic against
  `bucket_write_drains` (`pg_store/command_log.rs:2226-2247`). On bucket-PG
  primary failover, coordination rows (deliberately unreplicated per
  `guides/bucket-write-drain.md`) are lost to the new primary; apply-time
  proof validation then fails closed (safe, availability-only). An orphan
  un-expired drain row on a returning primary blocks that bucket's control
  writes until lease expiry (availability note).
- Best-effort cleanup spot-checks pass: typed route/control-plane errors are
  preserved and traced at the suppression point (`cluster.rs:10508-10594`),
  not collapsed into `NotFound`.
- Multipart complete: the bucket-PG order command precedes the object-PG
  completion command; an epoch change between the two fails typed at fanout
  and leaves only a harmless sequence gap. No atomicity hole found (moderate
  assurance).
- `pg_topology.rs` is deterministic and self-consistent; `pick_pg` is
  modulo-based, so changing the PG-ID set remaps nearly everything — fine
  only while PG count is fixed for a cluster's lifetime.

## 2d. Storage RPC layer and node client/server boundary

### RPC1. RESOLVED — Storage RPC sockets and metadata-command lock waits are bounded

A wedged (alive but stuck) client can fence a PG indefinitely and absorb node
capacity. Confirmed.

- Client one-shot RPCs: `UnixStream::connect` → write → blocking read with no
  `set_read_timeout`/`set_write_timeout` (`node_client/unix_rpc.rs:1390-1497`).
  Session RPCs likewise (`unix_sessions.rs:123-161`, :206-262). The server
  sets no timeouts either (`handle_session`,
  `storage_node_server.rs:1530-1613`). The Raft control-plane transport does
  set both, so this is a gap specific to this layer.
- Server-side metadata-command critical-section acquisition waits forever
  (`StorageNodeMetadataCommandLocks::acquire`,
  `storage_node_server.rs:1336-1404` — diagnostics but no deadline, no
  lease).
- Failure scenario: frontend A opens `MetadataCommandPgLockAcquire` for PG 5
  and then stalls (SIGSTOP, scheduler wedge, deadlock elsewhere) with the
  connection open. The server-side guard is only released on disconnect.
  Every other frontend's mutation on PG 5 blocks a server thread forever;
  each blocked RPC also pins one of `STORAGE_NODE_MAX_ACTIVE_SESSIONS` (1024)
  slots and a client-side admission permit, so one sick frontend can suppress
  an entire node's mutation capacity with no typed overload and no recovery
  short of killing a process. The 10.9 gate bounded admission waits but not
  in-RPC waits.
- Status update: fixed by applying read/write deadlines to one-shot Unix
  storage RPC sockets, persistent read-handle sessions, persistent
  metadata-command sessions, and accepted server-side sockets, using separate
  client-response, write, and server-idle budgets rather than one shared
  timeout. The metadata-command PG lock wait is intentionally shorter than the
  Unix client response timeout, so clients observe typed
  `MetadataCommandContention` rather than a socket timeout. The server treats
  idle socket timeout as session closure, so session-owned read handles and
  metadata-command guards are dropped through the normal connection cleanup
  path. Client-side socket read/write timeouts are classified as typed
  `TransportTimeout` storage RPC errors and mapped as retryable transport
  pressure, not payload/protocol decode failures. Focused regressions cover
  one-shot and metadata-command session client response timeouts, server
  idle-session timeout, metadata-command lock wait timeout, and the end-to-end
  Unix client contention path.
- Follow-up soak fix: long metadata-PG migration runs exposed that
  control-plane runtime-map responses could grow with retained historical PG
  routes until 1s readers disconnected and the authority logged `Broken pipe`.
  Storage-node heartbeat refreshes now include only historical routes at or
  above the node's reported history floor plus explicit metadata-transfer
  source routes required by current peering proofs. The UAT readiness/admin
  check uses a compact runtime-map status RPC instead of repeatedly fetching
  the full historical map. Full runtime-map snapshots still export retained
  history for consumers that need route reconstruction.

### RPC2. RESOLVED — Epoch validation now uses a refreshed per-frame config snapshot

A node keeps accepting mutations for an epoch it already knows is superseded.
Confirmed (verified directly).

- `connection_handler()` clones `config_snapshot()` at accept time
  (`storage_node_server.rs:1273-1287`); all route/epoch/primary checks in
  `dispatch_frame` use that clone. `install_control_plane_runtime_config`
  (:1220-1252) swaps the shared config, but existing long-lived connections
  (read-handle sessions, metadata-command critical sections, any connection
  accepted earlier) continue validating requests against the old epoch/routes
  for their lifetime.
- Failure scenario: a control-plane refresh installs epoch N+1 with a moved
  PG primary. A stale frontend that connected at epoch N keeps applying
  metadata commands and shard writes on the old primary, and the old primary
  accepts them — even though its own installed config already says N is
  stale. The only bound is the old snapshot's `route_map_valid_until_ms`
  checked against wall clock (:9234-9244); in static-config mode that field
  is `None` and there is no bound at all.
- Fix is cheap: re-read the current config per frame (an `Arc` swap read, not
  a clone per frame) instead of per connection, keeping the per-connection
  snapshot only for fields that must stay pinned per session.
- Status update: fixed by storing the storage-node runtime config as an
  `Arc<StorageNodeProcessConfig>` behind the refresh lock and having
  connection handlers refresh by cloning that `Arc` before dispatching each
  RPC frame. Existing session state remains connection-scoped, but route and
  epoch validation now sees the latest installed runtime map without cloning
  route tables on the hot path. Regression coverage keeps a single Unix
  connection open across a runtime-map refresh, verifies the second health
  frame reports the new epoch, and verifies an old-epoch read-handle acquire
  on that same socket is rejected without acquiring a read handle.

### RPC3. MEDIUM — Remote shard corruption/absence classification for EC reconstruction depends on matching error `Display` strings inside a generic `Internal` code

Confirmed.

- The server collapses `StoreError::ShardAckMismatch` (CRC/size mismatch on
  read) and most store errors to `StorageRpcErrorCode::Internal` +
  `error.to_string()` (`store_error_response`,
  `storage_node_server.rs:10056-10071`). The client-side EC read path
  re-derives "recoverable" from message text:
  `is_recoverable_remote_shard_read_error` requires `code == Internal &&
  (message == "shard not found" || message.contains(" ack mismatch: "))`
  (`cluster.rs:11474-11483`).
- Any rewording of those Display strings — or a mixed-version cluster
  formatting the message differently — silently converts a recoverable
  corrupted-shard read (which should trigger EC reconstruction per the
  invariants guide) into a fail-closed read error; conversely an unrelated
  `Internal` error containing " ack mismatch: " would be misclassified as
  recoverable and swallowed. Corruption should get its own
  `StorageRpcErrorCode` (e.g. `ShardIntegrity`) that round-trips to
  `StoreError::IntegrityError`.

### RPC4. MEDIUM (latent until epochs diverge) — Read-handle/delete fencing is keyed by `ShardLocation` including `cluster_epoch`, but the physical shard file is epoch-agnostic

Confirmed in code; live only from Phase 11 operations on.

- `ReadHandleShardKey`/`ShardLocationKey` include `cluster_epoch`
  (`storage_node_server.rs:9812-9844`); `try_begin_delete` only blocks a
  delete if handles exist under the identical key (:9770-9789). The file path
  is `shards_dir/prefix/hex(shard_key)` with no epoch
  (`pg_store/shards.rs:16-25`), so two locations differing only in epoch
  alias the same file. `ShardHistoricalRead` validates node+PG only, skipping
  epoch and PG state, and historical/backfill reads do not hold read handles.
- Failure scenario (post-epoch-transition): a reader holds a handle at the
  shard's original placement epoch; a reclaim/backfill worker addresses a
  delete at the same `(pg, shard_key)` under a retained historical or newer
  epoch; `try_begin_delete` sees zero handles under its key and unlinks the
  file under the reader. Recommend keying fences on `(data_pg_id, shard_key)`
  only (epoch and node id are validated separately).

### RPC5. MEDIUM — All client transport failures collapse to `StoreError::StorageRpc { code: PayloadDecode }`, erasing committed-but-response-lost vs never-sent

Confirmed.

- Write failures, read EOFs, and genuine decode failures all funnel through
  `rpc_payload_error(...)` (`unix_rpc.rs:1448`, :1495, :1618-1625;
  `unix_sessions.rs:140-145`, :235-246). A response lost after the server
  durably applied a command or fsynced a shard is indistinguishable from a
  connection refused before any side effect.
- Consequences are contained today because every mutating primitive is
  idempotent by durable identity and callers treat these errors fail-closed —
  but the 10.9 rule "unknown publish/commit outcome must fail closed, not map
  to retryable overload" is enforced only by this accidental conservatism.
  Nothing in the type system marks these errors ambiguous-side-effect; a
  future mapper change routing them to a retryable class would silently
  create double-apply exposure, and diagnostics cannot count EOFs separately
  from real codec bugs. Introduce distinct variants: `TransportConnect` (no
  side effect possible), `TransportSendPartial`/`TransportResponseLost` (side
  effect ambiguous), `PayloadDecode` (peer bug).

### RPC6. LOW — `write_shard_file_durable_if_absent` returns `StoreError::NotFound` after losing the create/delete race twice

`pg_store/shards.rs:113-205`: on `AlreadyExists` → file vanished before
comparison, it retries twice, then returns `NotFound` — for a write. Callers
interpret that with read/absence semantics. Return a dedicated
conflict/retryable error.

### RPC7. LOW — `serve_forever` exits on any accept error

`storage_node_server.rs:1055-1059`: a transient `accept` failure (EMFILE
while 1024 sessions hold fds, ECONNABORTED) terminates the accept loop while
the process keeps holding the data-dir lock — node-down with no
crash/restart signal. Retry with backoff for transient errno classes.

### RPC8. RESOLVED — Metadata-command session Drop no longer performs an RPC

`Drop for UnixStorageNodeMetadataCommandSession` →
`release_metadata_command_pg_lock` → write + blocking read on the session
stream (`unix_sessions.rs:281-294`, :421-425). With RPC1 (no timeouts) a
stuck-but-alive server blocks the dropping thread inside `Drop`, including on
unwind paths. Disconnect alone already releases the server-side lock; make
the Drop RPC best-effort with a short timeout or replace it with a plain
close.
- Status update: fixed by removing the blocking release round trip from
  `Drop`. Dropping a metadata-command session now shuts down the Unix stream,
  letting the storage-node session cleanup release the guard. A focused Unix
  RPC regression verifies the server sees connection closure rather than a
  release frame and that Drop returns promptly.

### RPC9. LOW — Multi-process client admission can exceed the server session cap with unbounded queuing

The per-`(node, socket)` client gate (`unix_admission.rs`) is per process;
the server reserves an active-session slot before `accept` and leaves excess
connections in the listen backlog (`storage_node_server.rs:1254-1271`). N
frontends can hold N×1024 permits against 1024 server slots; the overflow
waits invisibly and indefinitely (RPC1). No acked-dropped work (the server is
synchronous), but the deferred framed overload protocol is what actually
closes this.

### RPC10. Observations (not defects)

- The storage-node primary's critical section covers only the primary apply;
  replica fanout runs after the guard drops. Interleaved replica applies from
  racing frontends are rejected by log-index/hash-chain checks and converge
  via drain/reissue — correct, but worth a comment since it looks like a
  lock-scope bug.
- `register_written_shards_and_append_stream_segment` still uses `INSERT OR
  REPLACE` for shard rows (`pg_store/shards.rs:472`); it runs inside command
  apply where identity is command-stream-owned, matching the "internal helper
  only" carve-out — keep the boundary script aware of it.

### Storage RPC: verified-sound baseline

- **Framing:** magic + version + request id + kind + declared length + CRC64;
  per-message-kind request caps enforced before payload allocation
  (`message_kind_request_max_payload_len`, `storage_rpc.rs:3348-3538`);
  trailing bytes, unknown kinds, checksum mismatch fail closed. Range-read
  offsets are validated at decode and re-checked against actual file size
  server-side.
- **Epoch/route enforcement is server-side:** every mutating handler calls
  `validate_pg_route*` (node id, epoch equality, route state, acting-set
  membership, route-map validity deadline —
  `storage_node_server.rs:9207-9284`); PG-primary-only ops validate
  `route.primary_node_id` plus server-side re-derivation of the
  bucket/object→PG mapping (:9425-9467). The server does not trust client
  routing, and RPC2 now refreshes storage-node runtime config before each RPC
  frame is dispatched.
- **Shard write ack is after durability:** `write_shard_file_durable_if_absent`
  fdatasyncs the temp file, hard-links into place, fsyncs the parent dir, and
  is exact-idempotent — same-bytes retry returns the same ack; different
  bytes for an existing key fail closed without overwrite
  (`pg_store/shards.rs:101-206`). Ack rows are exact-idempotent (:343-444).
- **Session lifecycle:** server `StorageNodeSession::drop` releases all
  session-owned read handles and metadata-command PG-lock guards
  (`storage_node_server.rs:9985-9998`, :1499-1503), so client
  death/disconnect reclaims handles and fences. Read-handle acquire is
  idempotent per session by client op id + exact location set; release of an
  unknown id is idempotent.
- **Metadata command RPCs:** canonical command bytes + embedded checksum
  validated at decode; command-id route validated against the RPC route on
  both encode and decode (`storage_rpc.rs:3891-3925`); apply retry converges
  via acceptance/AlreadyApplied + hash-chain duplicate handling; typed
  contention outcomes ride in success envelopes rather than being collapsed.

---

# Hardening plan

## Production follow-up after Phase 12.3 closeout

1. R1 production durability follow-up: replace full peer-response
   restart-artifact checkpoints with an fsync'd vote/log WAL, demoting the
   full artifact to a compaction checkpoint. The current 12.3 process path is
   safe because peer responses are withheld until a durable checkpoint
   succeeds; the WAL is the production-scale replacement for that expensive
   safe path. Keep the deterministic pause/failure coverage for the
   no-double-vote and no-acked-entry-loss boundary.
2. R2 is closed for the experimental Unix-peer process path: OpenRaft timers,
   heartbeats, and natural elections are enabled there, while deterministic
   unit/in-process tests keep manual election control.
3. R3 is closed for the current full-artifact checkpoint path: checkpoint
   capture is state-first/log-later, and pre-write pair validation rejects
   state-ahead or otherwise inconsistent restart artifacts before replacing the
   last good checkpoint.

## Tier 1 — Phase 11 production path (live today)

4. **A single time-discipline slice** rather than piecemeal fixes: latch
   authority `now` monotonic across commands (persist the high-water mark in
   the state file), capture it inside the authority mutex, reject heartbeats
   that regress deadlines/timestamps or `last_observed_epoch` (CP1, R4),
   subtract an explicit skew margin on node-side validity and add the same
   margin before successor activation (CP2, CL4), and extend the
   deposed-primary lease fence from metadata transfer to all Active-exit
   transitions (CL1). This one design closes the whole dual-primary family.
5. **DONE — Restore a coherent fanout/recovery contract (MD1).** The chosen
   route was to keep primary-first fanout and remove the unsafe local cleanup
   dependency on primary-last ordering: bind/local recovery preserves
   same-epoch terminal slots, and cleanup requires command ownership or
   cluster-level acting-set evidence. Guides and regressions now pin this
   boundary.
6. **DONE / FOLLOW-UP — Contiguity and stale-epoch fences (MD2/MD3/MD4/CL3).**
   Contiguity enforcement at record time (`log_index == applied_log_index + 1`
   unless the entry already exists) rejects the gap before mutation, fixing
   the not-advanced clean-revision masking path (MD2). `PgStore` rejects
   `command_epoch < replica_state.cluster_epoch`, and the server/codec assert
   `request.cluster_epoch == command.id().cluster_epoch()` (MD3/CL3).
   Direction-aware orphan cleanup cleans only older-epoch slots and fails
   closed on future-epoch slots (MD4). Remaining follow-up: gate forward epoch
   adoption on an explicit transfer/epoch-transition token.
7. I/O deadlines on the storage RPC layer (client connect/read/write; server
   per-frame read and response write) mirroring the Raft transport, plus a
   lease/deadline on the server-side metadata-command critical section
   (frames on the session count as renewal; expiry drops the session) —
   closes RPC1/RPC8 and reduces the unbounded-wait portion of RPC9. **DONE:**
   per-frame config read for epoch validation (RPC2), storage-RPC socket
   deadlines, non-blocking metadata-command session Drop, and bounded
   metadata-command PG lock acquisition. **OPEN:** RPC9 still needs a
   deferred framed overload/backlog protocol for multi-process admission
   fairness.
8. Wire the peering catch-up path into the control plane — primary chosen as
   the serving node with the longest verified chain, explicit unacked-suffix
   abandonment rule for `ReplicaAheadOfPrimary` — or at minimum emit an
   operator-visible event instead of the silent skip at
   `control_plane.rs:1063` (CL2). Typed shard-integrity error code replacing
   string matching, with one release of dual-form acceptance for version skew
   (RPC3).

## Tier 2 — invariant strengthening

9. Epoch/incarnation floor persisted outside the single state file: sidecar
   highest-issued file checked at `open()`; storage nodes persist the highest
   authority incarnation+epoch they have accepted and refuse lower at
   startup; cluster ID in state file, Raft artifact, and runtime map
   (CP3, R7, CP10).
10. Consumers validate freshness proofs: `from_runtime_map` and frontend
    install require `is_serving_authority_read()` and monotonic authority
    incarnation; reject serving maps whose Active routes lack lease deadlines
    (CP4).
11. Transport error taxonomy split (connect-failed / side-effect-ambiguous /
    decode) with a guardrail that ambiguous variants can never map to a
    retryable class for non-idempotent operations (RPC5).
12. Decide `synchronous=NORMAL` explicitly (MD5): fsync on command commit, or
    document per-node power loss as a repair event and add the repair test
    for the acked-command-lost-no-pending-slot shape.
13. Bound protected history (age out floors of Removed/long-Unavailable nodes
    with operator override); remove the full snapshot from heartbeat
    responses (CP5). Per-PG degradation instead of whole-map read errors
    (CP11).
14. Re-key read-handle counts and delete fences on `(data_pg_id, shard_key)`;
    decide whether historical/peering reads must hold handles before Phase 11
    backfill can delete (RPC4). Resolve the payload-lease ownership mismatch:
    implement `StorageNodeClient` lease methods on `UnixStorageNodeClient` or
    amend the invariants guide and rename `reclaim_object_payload_if_unleased`
    (CL5).
15. CAS preconditions (`expected_cluster_epoch`) on admin acting-set/fence/
    transfer RPCs (CP8). Guard acting-set changes for PGs with non-empty
    active proofs behind the transfer-proof path (CL6). Epoch-tagged proof
    lineage so cross-epoch comparisons can verify ancestry rather than
    accepting "different hash and digest" (CP6/CP7).
16. Move the flock into `FileControlPlaneStore` (CP9); state-file
    checksum/record-count trailer (CP10). Fix the reclaim error shadowing at
    `request_ops.rs:9725` (CL7) and the small items in MD7/RPC6/RPC7/R8/R10.

## Testing and confidence-building before production

- **Clock-fault injection.** `clock.rs` is already a seam — make authority
  and node clocks injectable and add skew/step/regression scenarios to the
  control-plane proptest (CP1/CP2/CL4 would all have been caught). Today no
  test can represent two processes with different clocks.
- **Crash-durability harness.** The Slice 6 failpoint matrix is strong but
  in-process. Add a process-level SIGKILL harness at named boundaries
  (peer-ack→checkpoint, apply→slot-removal, fanout mid-flight) and a
  power-loss-shaped SQLite test (WAL rollback simulation) for MD5.
- **Model-check the deterministic cores.** The command-apply boundary and the
  control-plane state machine are pure `(state, command) → state` — ideal for
  `stateright` or TLA+. Model the lease/epoch/peering machine with two
  primaries and a skewed clock; assert "no two nodes serve the same PG at
  overlapping authority times". The heartbeat proptest exists; a model
  checker explores interleavings proptests will not hit.
- **Linearizability soak (Jepsen-style).** Black-box S3 history checking
  (read-after-write, list-after-write, version ordering) against the
  multi-process harness with a nemesis doing kill/restart, clock steps, and
  admin transitions (member-out during traffic would have surfaced CL1).
  Elle-style checking on a key→version projection.
- **OpenRaft conformance and gap tests.** Keep the guarded log-store
  compatibility harness that runs the upstream suite cases which match
  Argmin's bootstrap and restart-watermark model, and keep local regressions
  for the deliberately stricter deviations. Add fresh-follower-joins-via-
  snapshot (R5) coverage; keep the R6 static-peer reconfiguration rejection
  regression in the Unix-peer durable boundary; add the ack→checkpoint crash
  tests (R1).
- **Invariant-drift protection.** For each load-bearing guide invariant
  (fanout order, recovery classification, proof-floor rules), add either a
  test named after the guide section or a debug assertion. MD1 is the proof
  this gap class is real and recent.
- **Alert on swallowed conditions.** `PgPeeringMetadataProofMismatch` skips,
  lease-regression events, history-floor rejections — silent today, each an
  early warning of the failures above.
- **Size/soak bounds.** Heartbeat soak with growing history to hit the CP5
  8MB cliff in test rather than production; leadership-churn soak once R2 is
  fixed to shake out the exit-on-transient paths (R10).

## Overall assessment

The fail-closed instincts, idempotent primitives, and test culture are
genuinely strong — most of what the review probed held up, and the
verified-sound lists above are substantial. The gaps cluster where
single-process assumptions leak into the multihost world: time, ack-vs-
durability ordering, and prose invariants that code stopped honoring. Tier 0
plus the time-discipline slice (item 4) and the fanout/recovery contract
(item 5) are the items to treat as hard blockers for production.
