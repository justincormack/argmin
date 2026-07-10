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

An interim re-review of the fix work (baseline `88decac1` → `6b821bc8`,
2026-07-06) is recorded in the "Interim re-review — 2026-07-06" section
before the hardening plan: it verifies which resolutions hold, corrects two
statuses (CL1 still open, CP5 partial; CP1 actually resolved), assesses the
new Phase 12.4 WAL subsystem, and adds new findings `INT-1`–`INT-6` —
including two high-severity availability regressions introduced by the fixes
themselves (INT-1, INT-2).

A third re-review (baseline `25f47480` → `09805ad6`, 2026-07-09) is recorded
in the "Round-3 re-review — 2026-07-09" section: it covers the control-plane
authentication slice as a protocol (sound; residuals A1–A4), verifies the
MD5/RPC3/RPC5/INT-4-core fixes, and adds findings `R3-1`–`R3-5` — two more
high-severity availability regressions (R3-1, R3-2) from the INT-1 fix chain,
in the same committed-timestamp guard.

The companion [distributed correctness confidence review](distributed-correctness-confidence-review-2026-07.md)
reviews baseline `0d61d3fc`, adds findings `DCC-1` through `DCC-4`, and turns
the remaining work into an invariant, testing, fault-injection, and release-gate
program. The companion supersedes this document where it discusses the latest
committed-timestamp catch-up change.

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
2. **Ack-before-durable.** Raft peer responses used to precede the durability
   checkpoint (R1), and per-PG SQLite WAL previously used
   `synchronous=NORMAL` for acked metadata commands (MD5). Both now have
   focused fixes, but durability boundaries remain a recurring review theme.
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
- Status update (2026-07-09): the identity half is now resolved for the Unix
  peer transport. Multi-node peer mode requires scoped authenticated
  credentials; signed frames bind cluster, source, target, role, operation,
  payload, credential version, and bounded freshness where required. Missing,
  malformed, stale, wrong-principal, and bad-MAC frames fail before OpenRaft
  dispatch. TCP transport and external secret distribution remain production
  rollout work, not the original unauthenticated-envelope defect.

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

### CP1. RESOLVED (interim re-review 2026-07-06) — Out-of-order heartbeat application regresses lease deadlines; no per-node monotonicity guard

Confirmed race (verified directly); timing plausible. **Resolved on the
production path** by 0e1deb2b + 569bbb39 (see status update at the end of
this finding).

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
- Status update (interim re-review 2026-07-06): resolved on the Phase 11
  production path. The real fixes are 0e1deb2b + 569bbb39. The RPC worker and
  the maintenance expiry loop now sample `now_ms` inside the authority mutex
  (`argmin-s3/src/main.rs:2651-2657`, :1411-1414); no production
  apply/lease-evaluation path captures time pre-mutex. The apply-side guards
  live in the single shared `apply_control_plane_command` used by both the
  single authority and the Raft path: `NodeLeaseDeadlineRegression` rejection
  (`control_plane.rs:1438-1446`), snapshot-wide committed-timestamp
  monotonicity via `validate_committed_timestamp` (:859-869), and
  `last_observed_epoch` clamped monotonic (:211-216). Lease deadlines can only
  move forward; the stale-epoch `last_observed_epoch` overwrite sibling is
  closed by the clamp. Note: the committed-timestamp guard introduced INT-1
  (crash loop on clock regression) — see the interim re-review section.

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

### CP5. PARTIAL (interim re-review 2026-07-06) — Unbounded protected cluster-map history + full snapshot embedded in every heartbeat response, with an 8MB frame cap → cliff failure of all lease renewals

Confirmed mechanics. Heartbeat-response half fixed; durable-history half and
the frontend-startup snapshot RPC still expose the cliff (see status update
at the end of this finding).

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
- Status update (interim re-review 2026-07-06): partial.
  - Fixed: heartbeat refresh responses no longer embed the full snapshot —
    they carry a compact 5-field lease summary plus delta-compacted history
    scoped to the node's observed epoch and participation
    (`control_plane.rs:5040-5041`, :5492-5498, :603-632, :742-809), with a
    regression test pinning response size well below snapshot size
    (5395b096, 63654370, 64d18258).
  - Not fixed: durable history pruning is byte-identical to baseline
    (:8798-8840); Out/Removed nodes still pin `retain_from_epoch` forever
    (`SetNodeMembership` does not clear the floor, :1334-1343); no absolute
    cap. The fsync-per-heartbeat full-state rewrite and the pruned-floor
    permanent heartbeat rejection are also unchanged.
  - Cliff still reachable on a load-bearing path: the full
    `RuntimeMapSnapshot` RPC serializes all history × all PGs (:717-727,
    :5570-5571) under the unchanged 8MB cap (:22, :5280-5284), and S3
    frontend gateway startup requires it (`main.rs:3255-3301`, :3304-3320).
    Even the nominal 256-record cap crosses 8MB around ~800 PGs. Blast radius
    reduced from cluster-wide lease loss to "frontends cannot start/restart".

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

### MD5. RESOLVED — Per-PG SQLite WAL now uses `synchronous=FULL`

Confirmed pragma (`schema.rs:759-764`); consequence scenario plausible.

- In WAL/NORMAL, commits are only fsynced at checkpoint; OS crash/power loss
  can roll a node back to an earlier committed prefix (process crash is
  fine). Scenarios: (a) all acting replicas applied and acked; power loss on
  one replica rewinds its tail → restart agreement sees prefix disagreement
  with no pending slot anywhere → fail-closed divergence for an acknowledged
  write; (b) the primary loses its pending-slot insert while a replica
  already applied — exactly the "ambiguous" shape the design excludes. No
  compensating fsync barrier exists on the command-apply path.
- Status update: per-PG SQLite setup now uses WAL with `synchronous=FULL`,
  so command-apply commits are fsynced by SQLite before acknowledgement rather
  than relying on later checkpoint sync. A focused PgStore invariant test
  asserts `journal_mode=WAL` and `synchronous=FULL` so this durability contract
  does not silently regress.
- Round-3 verification (2026-07-09): holds — single pragma site
  (`schema.rs:759-765`) on the sole production PG DB open path, no carve-out,
  covers slot install / apply+record / slot removal. Blast-radius caveat
  (R3-3): every PG write txn now fsyncs while holding the per-PG mutex, under
  the 500ms metadata-command lock budget — a primary command lifecycle is ≥3
  fsynced txns plus one per replica. On slow-fsync media this can produce
  spurious `MetadataCommandContention`/`OperationAborted` bursts under load
  that NORMAL previously absorbed. Not a correctness issue; add an
  fsync-pressure case to the soak line and land the group-commit /
  write-amplification plans before scale testing.

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
- Status update (interim re-review 2026-07-06): **still fully open — do not
  count the peering-fence commits against this finding.** 9f4a97cd/4d05ea01
  fence something else: `authorize_node_service_for_snapshot`
  (`control_plane.rs:2239-2295`) validates the *incoming* primary's
  incarnation/epoch/lease against stale replayed batch completions, and
  4d05ea01 is a behavior-preserving extraction into
  `validate_pg_peering_completion` (:8185). Nothing captures or waits out the
  *deposed* primary's lease: `SetNodeMembership(Out)` still nulls the lease
  uncaptured (`record.lease_deadline_ms = None`, :1342, verified), and
  `mark_pgs_peering_for_nodes` clears the only candidate fence field
  (`metadata_transfer_fence_source_lease_deadline_ms = None`, :8878). The
  original scenario replays across all admin entry paths (Out/Removed,
  availability, incarnation/endpoint, acting-set swap). Because the deposed
  deadline is now destroyed at apply time, the fix needs capture scaffolding
  first: record the previous primary's last lease deadline when a PG leaves
  Active for any reason (the metadata-transfer path shows the pattern at
  :1912, :1983-1984) and refuse peering completion/readiness until `now_ms`
  exceeds it.
- Status update (2026-07-10): **resolved in the current tree.** PG state now
  persists the previous primary's node ID, incarnation, endpoint, and lease
  deadline separately from the metadata-transfer-only source fence. Every
  transition out of Active captures that identity before node lease mutation,
  including membership/availability changes, heartbeat incarnation or
  endpoint changes, acting-set changes, explicit state transitions, metadata
  transfer, and authority restart. Direct completion and
  `ready_pg_peering_completions` reject/skip a different proposed process while
  the old lease remains live. The exact same node/incarnation/endpoint may
  reactivate once ordinary peering proof checks pass; waiting out its own lease
  provides no safety and caused 116-PG startup to miss the readiness bound.
  Successful activation clears the fence. Canonical control-plane snapshot
  version 14 persists the complete identity through restart/history. Tests
  cover early direct rejection, automatic readiness suppression, immediate
  same-process reactivation, endpoint/incarnation distinction, exact-deadline
  activation after successor renewal, and persistence. The separate in-flight
  mutation side is tracked by DCC-1.

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
- Status update (interim re-review 2026-07-06): narrow partial. cb13fc8b
  wires a different, narrower recovery into the frontend refresh worker
  (`cluster.rs:1315-1321`, :4280): on refresh failure it re-drives the
  primary's *pending-command slot* forward, and on
  `PgPeeringPendingMetadataCommand` expires same-epoch map generations. That
  un-wedges only the "command still staged in the pending slot" case — and it
  introduced a fencing regression of its own (INT-3 in the interim re-review
  section). A pure proof mismatch (primary applied-and-cleared, replica lags)
  still wedges forever: `PgPeeringMetadataProofMismatch` is still silently
  swallowed in the readiness scan (`control_plane.rs:1209-1214`),
  `deterministic_pg_primary_for_snapshot` still picks first-serving not
  longest-chain (:2310), and nothing truncates unacked entries. The catch-up
  machinery remains `#[allow(dead_code)]` with test-only callers
  (`cluster.rs:2695-2756`).

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
  The first fix limited storage-node heartbeat refreshes to the reported
  history floor, but soak runs still hit large responses when a long-lived
  node had an old durable shard/backfill floor, and restart/bootstrap paths
  could still request thousands of unchanged per-PG historical routes. The
  refresh response is now shaped as compact route history: the first retained
  epoch in the response acts as the base for each PG, later epochs include
  only PG routes whose placement/peering configuration changed, and explicit
  metadata-transfer source routes are still included for current peering
  proofs. Storage-node config refresh merges that compact delta with locally
  retained history so old shard/backfill references are not forgotten. Runtime
  map reconstruction now resolves a historical route as the latest retained
  route at or before the requested epoch, then stamps it with the requested
  epoch for operation fencing. The UAT readiness/admin check uses a compact
  runtime-map status RPC instead of repeatedly fetching the full historical
  map, and the metadata-transfer completion/import-retry probes use the
  existing PG-scoped runtime-map RPC instead of full-map fetches. Full
  runtime-map snapshots still export retained history for consumers that need
  route reconstruction.

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

### RPC3. RESOLVED (round-3 verified, 2026-07-09) — Remote shard corruption/absence classification for EC reconstruction depends on matching error `Display` strings inside a generic `Internal` code

Fixed.

- `StorageRpcErrorCode::ShardIntegrity` now carries shard corruption/CRC
  mismatch responses across the Unix RPC boundary. `StoreError::ShardAckMismatch`
  and `StoreError::IntegrityError` no longer collapse to generic `Internal`.
- EC reconstruction now classifies recoverable remote shard read failures from
  typed RPC codes (`NotFound` and `ShardIntegrity`) rather than matching
  display strings. Regression coverage verifies that old `Internal` strings such
  as `"shard not found"` and `" ack mismatch: "` are not treated as recoverable.
- Round-3 verification: holds end-to-end (server encode
  `storage_node_server.rs:11116-11121`, wire code 23 both directions,
  client decode, EC classifier `cluster.rs:11811-11820` typed-only; the
  `operation` match is a client-local static label). Rollout caveat: no
  dual-form (string+code) acceptance release was shipped — an older peer
  receiving code 23/24 fails decode fail-closed, and a new frontend no longer
  treats an old node's `Internal`-string failure as recoverable, so rolling
  upgrades across this boundary turn recoverable EC reads into hard read
  failures (fail-closed, no wrong data). Fine for lockstep deployment; note
  it in the upgrade/versioning plan.

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

### RPC5. MEDIUM — Client transport failures are only partially typed, so committed-but-response-lost vs never-sent is still not explicit

Partially fixed.

- Client-side RPC read timeouts now surface as
  `StorageRpcErrorCode::TransportTimeout`, and stream closures such as EOF,
  reset, and broken pipe now surface as `StorageRpcErrorCode::TransportClosed`
  instead of `PayloadDecode`. Genuine frame/payload parse errors remain
  `PayloadDecode`, so diagnostics can distinguish closed transport from a peer
  codec bug.
- Connect failures still surface as `StoreError::Io { context: "connect
  storage-node RPC socket", .. }` rather than a typed RPC code. Write-side
  partial send vs response-lost is still not split: a transport close after a
  request may mean either that the peer never received the full request or that
  it durably applied the operation and the response was lost.
- Consequences are contained today because every mutating primitive is
  idempotent by durable identity and callers treat these errors fail-closed —
  but the 10.9 rule "unknown publish/commit outcome must fail closed, not map
  to retryable overload" is enforced only by this accidental conservatism.
  Nothing in the type system marks send-partial/response-lost as
  ambiguous-side-effect; a future mapper change routing them to a retryable
  class would silently create double-apply exposure. Remaining work is to make
  the send/connect boundary explicit, for example with variants such as
  `TransportConnect` (no side effect possible) and
  `TransportSendPartial`/`TransportResponseLost` (side effect ambiguous).
- Round-3 note (2026-07-09): the drift warned about above has half-happened —
  09805ad6 added `TransportClosed` (which conflates request-write failure and
  response-read failure, i.e. the ambiguous class) to four retryable
  classifiers (`cluster.rs:11611-11622`, coordinator
  `storage_rpc_code_is_retryable_route_state` → client-visible
  `OperationAborted`, backfill stale-retry `coordinator/runtime.rs:1919`, and
  the metadata-transfer transient matcher). Every current consumer was traced
  and is idempotent by durable identity, so this is safe today — but the
  guardrail item is now more urgent, not less, and the phase information that
  would enable it (distinct write-phase/read-phase operation labels) is
  currently discarded by all consumers. Keep this finding open until the
  taxonomy split lands.

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

# Interim re-review — 2026-07-06

Re-review of the fix work between baseline `88decac1` (review date) and HEAD
`6b821bc8` (~157 commits), including the Phase 12.4 WAL subsystem. Method:
three parallel verification passes (Raft/WAL, metadata layer, control
plane/RPC) reading HEAD code directly; the highest-impact claims below were
re-verified by hand. Per-finding status updates have been folded into the
finding sections above; this section holds the scoreboard, the new findings
(INT-*), the WAL assessment, and updated priorities.

## Verification scoreboard

Claimed resolutions that **hold**: R1 (checkpoint strictly precedes every
peer response, including WAL-mode compact; failure exits before ack), R2
(timers on for Unix-peer durable mode; natural-election durability gate
airtight on both authority-bearing surfaces), R3, R5, R6 (static-policy
rejection + startup revalidation of artifact and WAL-replayed membership),
R7 (sentinel tripwire; caveat: WAL deletion undetectable at replay offset 0 —
safe but silent), R8 (caveat: WAL-internal poison does not set the shared
peer gate; safety survives via `lock()` rejection + next checkpoint exit),
R10 (caveat: new fragile substring matching for benign errors; and see
INT-1), R4a/R4b guards (in the shared apply, covering both authorities — but
they introduced INT-1), MD1 (bind-path never cleans; builder requires
full-agreement proof; one stale guide sentence remains), MD2 (contiguity
fence at all four fenced entry points, pre-mutation, single txn), MD3/CL3
(follow-up — tokenless forward-epoch adoption — still real and open), MD4
(as fail-closed; but the fix created INT-2), RPC1 (all sockets bounded, lock
wait 500ms typed retryable, guard release safe; caveats INT-5), RPC2, RPC8,
CP8 (as described; CAS precondition still absent, but b44aee4c removed the
unchecked epoch-only fence RPC surface).

Claims that **do not survive scrutiny**:

- **CL1 was open at this interim baseline and is resolved in the current
  tree.** The control plane now captures the previous primary process identity
  and deadline on every Active exit and blocks direct/automatic activation of
  a different successor through it; see the later CL1 status update and DCC-1
  closeout.
- **CP5 is half-fixed.** Heartbeat responses are compact now, but durable
  history is still unbounded and frontend startup still fetches the full
  snapshot RPC under the 8MB cap — see the CP5 status update.
- **CP1 is better than the doc said** — genuinely resolved on the production
  path (the fix lives in 569bbb39 + 0e1deb2b); status updated above.

Still untouched since the original review: CP2/CL4 (zero skew margin — now
the top pre-existing gap), CP3 (parsing hardened by 569bbb39/6033c69c, but
NotFound→fresh-bootstrap and the missing incarnation floor unchanged), CP4
(validity bounding improved: wire decode rejects unbounded maps, installs
reject `Forever`; freshness proofs still have zero production consumers),
CP6/CP7, CP9, CP10 (partial: canonical re-encode equality on parse; no
checksum trailer or cluster identity), CP11 (a compact `runtime_map_status`
RPC exists but only the CLI uses it), CL5, CL6, CL7 (relocated to
`request_ops.rs:10537-10541`), RPC3, RPC4, RPC5 (partial:
`TransportTimeout` and `TransportClosed` typed; connect remains `StoreError::Io`
and send-partial vs response-lost is still not split), RPC6, RPC7, RPC9, MD7.

## New findings (INT-*)

A pattern worth naming: three of the five significant new issues were
introduced by fixes (INT-1, INT-2, INT-4), all in the fail-closed direction —
safety held, but availability regressions from hardening are becoming the
dominant defect class.

### INT-1. RESOLVED (2026-07-06) / HIGH (availability, production path) — Committed-timestamp guard converted clock skew into an unrecoverable crash loop

Confirmed mechanics (verified directly); trigger plausible.

- `validate_committed_timestamp` rejects any command timestamp below the
  replicated high-water (`control_plane.rs:859-869`), and
  `record_committed_timestamp` has no forward bound — pure max (:871-878).
  The high-water advances with every heartbeat/expiry.
- Both serving loops treat the resulting `CommittedTimestampRegression` from
  `ExpireHeartbeatLeases` as fatal `exit(1)`: Phase 11 at
  `argmin-s3/src/main.rs:~1425` (verified), the Raft loop at :2479-2482 (it
  is not a forward-to-leader error, so the benign matcher does not absorb
  it).
- Scenarios: (a) NTP steps the Phase 11 authority clock back → the production
  control plane crash-loops until wall clock passes the high-water; (b) after
  Raft failover, a new leader whose clock lags the old leader's last
  committed timestamp exits on its first lease scan → re-election, possibly
  of the other lagging node — leader flapping proportional to skew;
  (c) worst case: one node with a badly-future clock commits a single
  heartbeat → the high-water jumps years ahead → after fixing the clock,
  every expiry is rejected forever → permanent control-plane crash loop
  requiring manual state surgery.
- The pre-guard behavior degraded gracefully; the guard converts skew into
  fail-stop with no bound and no recovery path.
- Fix shape: treat the deterministic rejection as benign-wait with alerting
  in both scan loops, and bound forward timestamp jumps (sanity window
  against committed state) at proposal or apply time.
- Status update: implemented for both Phase 11 and experimental Raft lease
  expiry loops. Periodic expiry proposals now defer far-future local clocks
  without committing timestamp-only catch-up entries, raw committed commands
  fail closed on larger jumps, and timestamp-guard rejections from the scan
  loops are logged/deferred instead of exiting the process. The broader
  CP2/CL4 monotonic-clock/skew-margin policy remains a separate design item
  for production Raft deployment.
- Round-3 caveat (2026-07-09): the fda75348 recovery carve-out for the
  forward bound introduced two new high-severity availability regressions —
  R3-1 (the carve-out predicate is trivially true in a serving cluster, so a
  far-future clock step at the expiry scan can poison the durable high-water
  and mass-expire the cluster) and R3-2 (after a clean full expiry plus >1h
  downtime the carve-out can never fire, permanently wedging re-admission).
  See the round-3 re-review section. This guard has now produced a new
  high-severity issue in each of three review rounds; it needs a written
  invariant and an adversarial-clock proptest, not another point fix.

### INT-2. HIGH (availability) — The MD4 fix makes a future-epoch pending slot brick the whole node

Confirmed mechanism (verified directly); the crash timing is narrow per
write but recurs at every epoch bump.

- Recovery now hard-errors on a future-epoch slot
  (`pg_store/command_log.rs:2070-2077`, `MetadataCommandLogConflict`), and
  the error propagates through `recover_pg_metadata_command_state`
  (`node.rs:635-641`) → `StorageNodeServer::bind`
  (`storage_node_server.rs:1556`), failing the entire node.
- The window is not exotic: the durable replica-state epoch only advances
  when the first command of an epoch is applied (:4002-4011), and slot
  install legitimately precedes apply (documented at :2051-2057). After any
  epoch bump, if the PG primary crashes between pending-slot install and the
  first `apply_metadata_command_and_record` commit, restart yields
  slot-epoch N+1 vs store-epoch N → bind fails → crash loop.
- The frontend drain (cb13fc8b) cannot help because the node never comes up,
  and the cluster-level convergence path for future-epoch slots does not
  exist (CL2's reconciliation machinery is still not production-wired). The
  pre-fix code handled the common zero-apply variant of this window.
- Fix shape: at bind, a future-epoch slot with no terminal entry and
  store-epoch behind should quarantine per-PG (bind the PG fail-closed,
  serve the node's other PGs), not abort node startup.

### INT-3. CORE RESOLVED / MEDIUM (fencing regression) — The peering-refresh drain (cb13fc8b) widened the metadata-write fence

Confidence: medium — every individual gate crossing verified in code; the
composite requires an unhealthy-peering activation plus a stale pinned
frontend plus concurrent traffic.

- Server side: metadata-command handlers (including the normal Active apply
  path) now fall back to
  `validate_historical_active_pg_route_for_metadata_command_recovery`, which
  accepts `cluster_epoch < current` whenever a retained historical Active
  route at that epoch exists plus any current-or-retained Peering route at
  epoch `>= cluster_epoch` containing the node. The `>=` predicate keeps the
  acceptance window open long after the peering it was meant to serve.
- Frontend side: Recovery route mode skips `require_route_map_valid_now()`
  entirely (`cluster/local.rs:3193-3204`, :3320-3334), and the refresh
  worker triggers `drain_pending_metadata_commands_for_current_map` on every
  refresh error, not just the pending-command predicate.
- Composite scenario: PG activates at N+2 without the old primary (peering
  skips unhealthy nodes, `control_plane.rs:8290-8305`); the slot-holder
  returns with a preserved epoch-N slot; a frontend still pinned to the
  epoch-N map has its refresh fail for an unrelated reason and drains all
  PGs of its stale map, lease-expired, reissuing the epoch-N command;
  concurrently the current primary fans out `(N+2, 1)`. Per-node arrival
  order decides acceptance on not-yet-advanced replicas → identical log
  positions with divergent rows/digests → proof divergence → fail-closed
  wedge (CL2 repair unwired). No acked data lost; the strict
  pre-cb13fc8b fence (`command epoch == current` + valid lease) made this
  unreachable.
- Fix shape: `==`-epoch (or bounded) predicate on the historical acceptance
  window, gate the drain on the pending-command error and scope it per-PG,
  and decide explicitly whether Recovery mode may bypass lease validity.
- Related smaller items: the drain pass short-circuits on the first failing
  PG (`cluster.rs:4279-4303` — liveness, retried each interval); non-primary
  terminal slot cleanup in the builder still uses local-only evidence and
  runs before agreement validation (`cluster/local.rs:4030-4061`); pre-fix
  sparse log rows from the old MD2 bug are neither detected nor quarantined
  and get blessed into the chain by the slow advance loop when a later
  contiguous insert arrives (legacy data only).

- Status update (2026-07-09): **the fencing regression is resolved.** Normal
  historical apply now needs a process-local permit created only while the
  exact current Active route is drained into retained history. The permit is
  bounded by that route's validity deadline and cannot be reconstructed from
  persisted history after restart. Frame-wide route admission prevents a
  replacement config from publishing around the commit, and CL1 prevents a
  successor from activating before the old deadline. Thus a stale frontend
  drain cannot create the divergent post-activation write described above.
  The drain short-circuit and local terminal-cleanup observations remain
  liveness/recovery follow-ups, not this safety mechanism.

### INT-4. CORE RESOLVED (round-3, 2026-07-09) / availability sub-items open — Bucket-reservation lease coupling breaks replica convergence proofs (17e7d07e / 4c01bac9)

Confidence: medium — couplings verified in code (the reservation proof
carries the create-time `lease_deadline`, `metadata_command.rs:949-966`,
verified); triggers not executed.

Round-3 correction: the first bullet's lease-equality mechanism was already
identity-based at the round-2 baseline — convergence proof matching uses
reservation id/bucket/kind/context with `lease_deadline` explicitly excluded
from the equality (`pg_store/metadata.rs:5540-5599` region, with a comment
that renewals must not strand replica convergence). The stranded-replica
scenario is therefore closed. The reaping-without-pending-check, drain-row
orphan, and fixed-15s-lease sub-items below remain open.

- Convergence revalidation now requires `lease_deadline` equality between
  the command's reservation proof and the live row
  (`node_client/local.rs:1806`, `cluster.rs:4875-4894`) — but stream-create
  reservations are heartbeat-renewed (~10s,
  `request_ops.rs:2751-2826`), advancing the durable row while the command
  envelope pins the create-time lease. A lagging replica converging that
  command (not `AlreadyApplied`) fails closed with the primary already
  mutated: a stranded command, the divergence class
  `guides/bucket-write-drain.md:136-151` forbids.
- Companion: `wait_for_durable_bucket_write_reservations_empty` reaps any
  `lease_deadline <= now` row (`request_ops.rs:3560-3592`) without checking
  for a pending object-PG command referencing it; a `CommandOwned`
  reservation accepted on the primary but not yet on a replica gets reaped →
  `BucketWriteReservationNotFound` on convergence → same class.
- Also in this family: lease-matched drain clear + `NotFound → Ok` rollback
  can orphan a drain row until lease expiry (availability;
  `pg_store/metadata.rs:5842-5860`, `request_ops.rs:3366-3378`); new
  zero-margin wall-clock comparisons across nodes in the drain/reservation
  layer (same CP2/CL4 class); non-stream reservations carry a fixed
  non-heartbeated 15s lease — slow direct writes can be reaped pre-commit
  (safe, availability).
- Fix shape: match reservations by identity/generation rather than exact
  lease value (or CAS the renewal into the proof), and require a
  no-referencing-pending-command check before reaping.

### INT-5. MEDIUM — Timeout semantics: slow-drip and read-handle idle exemption

- Server RPC timeouts are per-syscall (`SO_RCVTIMEO`/`SO_SNDTIMEO` reset on
  any progress), so a peer dripping ≥1 byte/s holds a connection thread
  through a 64MiB frame (`storage_rpc.rs:76`) indefinitely; N clients pin N
  server threads. RPC1's bound holds for dead/idle peers, not slow ones. Fix:
  per-operation deadline checked across syscalls.
- The read-handle idle-timeout exemption (`storage_node_server.rs:2170-2174`)
  lets a connected-but-silent client pin shard read handles + a server
  thread indefinitely, blocking shard deletes/repairs on those locations.
  Fix: cap total idle time or require periodic renewal frames.
- Smaller: persistent session reuse after a write timeout can desync framing
  unless all callers discard the session on any transport error (not
  verified for all callers); metadata-command sessions lose the PG lock
  after 1s inter-frame idle (correct but a new liveness requirement);
  the new epoch-mismatch rejection reuses `PayloadDecode`
  (`storage_node_server.rs:10924-10939`), extending the RPC5 pattern.

### INT-6. Raft/WAL smaller findings

- Torn-WAL tail shapes other than short-file (zero-filled length prefix,
  in-bounds CRC-mismatched final frame) brick restart instead of truncating
  (`control_plane_raft.rs:5158-5187`, :5171-5175); coverage only exercises
  the file-one-byte-short shape. Treat final-frame anomalies as torn
  (truncate) or ship an operator recovery tool; add crash-shape tests.
- First-startup crash window: membership-init WAL append precedes the first
  checkpoint (`main.rs:2293-2312`); a crash between leaves non-empty WAL
  with no artifact/sentinel → fail closed needing manual WAL deletion. Same
  class: sentinel-then-artifact ordering on the very first checkpoint.
- RESOLVED (2026-07-06): the standalone peer-stream helpers
  (`control_plane_raft.rs:6532-6634`) are now private `#[cfg(test)]`
  helpers. Production code keeps using the frame-level handlers through the
  `argmin-s3` peer worker, which applies the checkpoint and poison contract
  before acknowledging peer RPCs.
- WAL-internal poison does not set the shared peer poison gate (safety
  survives; the documented R8 invariant is inaccurate) — bridge it.
- No artifact↔WAL generation binding (named in the 12.4 exit criteria, not
  implemented); WAL deletion silently undetectable at replay offset 0.
- Residual transient `exit(1)`s in the Raft serving loop: WAL-stat IO error
  inside `status()` (`main.rs:2446`), `accept()` EMFILE/ENFILE, and the
  benign-churn detector is an exact two-substring match on one OpenRaft
  alpha error phrasing (:2488-2495) — deepens the R11 coupling.
- Ordinary peer acks still pay full artifact encode+fsync plus WAL rewrite
  per RPC, with synchronous fsyncs inside async `RaftLogStorage` methods
  under a mutex — the acknowledged remaining 12.4 cost work, not a defect.

## WAL subsystem assessment

The new WAL is sound where it matters: append-only with no file reuse (the
classic stale-bytes-after-torn-frame misparse is structurally impossible),
fsync-before-publish with correctly classified poison boundaries (pre-frame
reject / poison-without-publish / publish-then-poison, all under one lock
guard), offset arithmetic consistent across compaction and crashes at every
point (artifact-then-compact ordering under a shared checkpoint lock;
`base_offset <= artifact.wal_replay_offset` holds across all crash points),
and fail-closed identity/offset checks on restart. Peer-ack ordering in WAL
mode preserves R1. The 18 WAL unit tests pass.

## Phase 12.4 exit-criteria trajectory

Crash fault-injection is well advanced (WAL suffix restore, peer
pre-response crash via unwritable state dir, checkpoint-pause coverage) but
lacks clock-regression-election and compaction-during-replay crash points.
Peer auth is documented only (`plans/control-plane-auth-identity-plan.md`);
nothing on the transport. Lease/skew semantics have guards and regression
tests but no skew-margin design — and INT-1 showed the guards actively need
that design. The Raft retry/confirmation contract is not implemented.
Observability is mostly done (WAL backed/offsets/poison, durable
vote/log/commit/applied, timestamp high-water) but WAL/artifact generation,
peer-auth failures, and retry-confirmation diagnostics are missing.

## Updated priorities

1. **INT-2** — per-PG quarantine instead of node-wide bind failure.
2. **CL1** — still the top pre-existing safety gap; now needs deposed-lease
   capture scaffolding before the fence can exist. Do it while the 9f4a97cd
   context is fresh.
3. **INT-3/INT-4** — tighten the drain predicate (`==` epoch, gate on the
   pending-command error, per-PG scope; decide Recovery-mode lease bypass
   explicitly) and decouple the reservation-lease proof matching; add the
   two targeted convergence tests.
4. **CP5 remaining half** — bound durable history and slim the startup
   snapshot RPC before any scale testing; it now gates frontend startup.
5. Phase 12.4 exit criteria with zero code so far: peer auth and the Raft
   retry/confirmation contract (replace the substring benign-error matching
   with typed classification while there).
6. CP2/CL4 skew margin remains the standing design item feeding INT-1, CL1,
   and the drain/reservation wall-clock comparisons — one time-discipline
   design closes the family.

# Round-3 re-review — 2026-07-09

Re-review of the work between `25f47480` (interim re-review commit) and HEAD
`09805ad6` (~153 commits). The range is dominated by the control-plane
authentication slice (plans/control-plane-auth-identity-plan.md, slices A-C)
plus S3-conformance and golden-test work; the distributed-correctness-relevant
changes were verified individually. Method: two parallel verification passes
(auth protocol + Raft; fix verification + open-item sweep), with the
highest-impact claims re-verified by hand. Per-finding status corrections
have been folded into the sections above (INT-1 caveat, INT-4 core, RPC3,
RPC5, MD5); this section holds the auth assessment, the new findings (R3-*),
and updated priorities.

## Control-plane auth protocol assessment

The new auth surface held up under protocol-level review:

- HMAC-SHA256 over canonical covered bytes (magic, version, cluster id,
  credential id+version, source principal, target, operation, issued/expires,
  sequence, nonce, payload — `control_plane_auth.rs:676-694`). Identity, RPC
  kind, direction, and payload are all bound on every path: Unix paths prefix
  the payload with the RPC kind (`control_plane.rs:7049-7079`), Raft peer
  frames cross-check inner frame kind and identity against the auth operation
  (`control_plane_raft.rs:6851-6918`), and responses are signed with the
  reversed identity and distinct operations, so request/response reflection
  and cross-role/cross-node splicing fail closed.
- Error responses are authenticated (both `Ok` and `Err` payloads are signed
  before the wire), so an attacker who cannot forge success cannot forge a
  behavior-changing failure either.
- Rotation is per-frame with exact id+version+principal matching; a lower
  version with a higher one configured reports `StaleCredential` rather than
  authenticating; signers pick the highest version. No dead window if the new
  credential is staged before the old is removed. Revocation requires process
  restart (no hot reload) — acceptable pre-production, must be named for
  cutover.
- Fail-closed coverage: multi-peer Raft mode requires peer auth credentials
  at config time (`config.rs:759-766`); the CA1 partial-role gap is enforced
  in both config validation and verifier construction, so admin mutations
  cannot be left unauthenticated while any Unix auth is enabled. CA2/CA4/CA7/
  CA8/CA9 verified fixed from a bypass angle.
- No regressions introduced: R1 ordering preserved (responses are signed
  before the durability checkpoint — CPU only — but written to the socket
  only after it), CP1 resample-under-lock preserved through ac4d2ac6 (clock
  sampling and mutation stay under the authority mutex; only response
  serialization moved out), no new I/O under locks.

Residuals (all bounded by the Unix-socket filesystem trust boundary today;
they become real work items when a TCP/remote transport lands):

- **A1 (MEDIUM-LOW):** the heartbeat auth replay window is sized to the
  requested lease duration (`control_plane.rs:1018-1028`, up to 10s) with the
  deadline computed from the server clock at apply. A captured heartbeat
  envelope replayed near window-end grants a fresh deadline ≈ issue+2× lease —
  extending a dead node's lease and widening the CP2/CL4 fencing window.
  Fix: a small fixed freshness bound (like the 5s read window) instead of
  the lease duration.
- **A2 (LOW):** responses carry no nonce/sequence echo binding them to their
  specific request — request→response correspondence rests on the
  connect-per-RPC transport plus a 5s freshness window. Within that window an
  on-path attacker can substitute a captured same-kind response (freshness
  downgrade, not forgery); since freshness proofs still have no production
  consumers (CP4), the downgrade is not independently caught.
- **A3 (LOW, by design per the plan's replay policy):** Raft
  append/vote/pre-vote/snapshot frames use `FencedByPayloadSemantics` — no
  freshness field; replay protection rests on OpenRaft term/log fences and
  credential lifetime. Only transfer-leader has a 5s window.
- **A4 (INFO):** response-verification skew constants are inconsistent
  (skew-0 for runtime-map, 10ms for admin) — part of the CP2/CL4 zero-skew
  family.

## Verification results

Fixes verified holding this round: MD5 (`synchronous=FULL`, with the R3-3
fsync-pressure caveat), RPC3 (typed shard integrity end-to-end, rollout
caveat), RPC5 partial classification (with the classifier-drift note),
INT-4's core lease-equality component (identity matching, `lease_deadline`
excluded), 3a123690 (MPU order-command authorization — build-time only, does
not extend INT-4), 0a908128 (ambiguous Raft admin triggers → `RpcUnconfirmed`,
no blind retry), 103e0880 (bounded check-applied timeouts; see the CP8 note
below), ac4d2ac6 (no CP1 regression), 98723f6d (peer stream helpers now
`#[cfg(test)]` — closes the INT-6 latent R1 bypass), 7b001de1 (test-only).

INT-1 fix chain (a1b1bf37 → fda75348): the crash-loop behavior is genuinely
fixed — both loops defer timestamp-guard rejections, heartbeats are
non-fatal, and a 1h forward bound exists — but the fda75348 carve-out
introduced R3-1 and R3-2 below.

## New findings (R3-*)

The round-2 pattern continued: the significant new issues are again
availability regressions introduced by fix work, and both are in the same
guard the previous two rounds already fixed once each.

### R3-1. HIGH (availability, production path) — the fda75348 carve-out neutralizes the committed-timestamp forward bound; a far-future clock step poisons the high-water durably

Confirmed mechanism (verified directly); trigger is a >1h forward clock step
(NTP misconfig, VM resume, fat-fingered date).

- `ExpireHeartbeatLeases` skips both the proposal-side deferral and the
  apply-side `CommittedTimestampTooFarAhead` validation whenever
  `has_heartbeat_lease_expiring_at(expire_at_ms)` is true
  (`control_plane.rs:1680-1686`, :3816-3827; Raft loop equivalent). That
  predicate is `lease_deadline_ms <= t` over live nodes (:908-918) —
  trivially true for any far-future `t` in a serving cluster, since live
  deadlines are ≈now+10s. The forward bound is therefore void for this
  command exactly when the cluster is healthy.
- Sequence: authority clock steps forward >1h → next expiry scan commits
  `ExpireHeartbeatLeases{far_future}` → `record_committed_timestamp` ratchets
  the durable high-water unbounded (:920-924) → every lease expires at once,
  all PGs → Peering, epoch bump → operator fixes the clock → every heartbeat
  at real time is rejected `CommittedTimestampRegression` (:1480) until wall
  clock passes the poisoned value — cluster-wide fail-closed outage of
  (clock error − 1h), persisted in the state file, recoverable only by
  manual state surgery.
- Before fda75348, a1b1bf37's deferral absorbed the identical event with
  zero state change and immediate recovery on clock fix. The carve-out's
  legitimate purpose (expiring stale leases after >1h of real downtime) is
  in-band indistinguishable from the poisoning case.
- Fix shape: bound what the command may ratchet — expire leases at
  `expire_at_ms` but advance the high-water by at most
  `previous + MAX_FORWARD_JUMP` per command — or gate the carve-out on an
  out-of-band downtime signal (e.g. state-file load-time delta) instead of
  the trivially-true expiring-lease predicate.

### R3-2. HIGH (availability) — the complementary shape: >1h downtime after a clean full expiry wedges the control plane permanently

Confirmed mechanism (verified directly); ordinary trigger.

- Expiry sets `availability = Unavailable, lease_deadline_ms = None`
  (`control_plane.rs:1702-1703`), and `has_heartbeat_lease_expiring_at`
  excludes exactly those records (requires not-Unavailable and a `Some`
  deadline, :908-918). After an ordered shutdown (storage nodes stop first,
  the authority's last scans expire them, then the authority stops) followed
  by a >1h gap: every re-admission `RecordNodeHeartbeat` is rejected
  `CommittedTimestampTooFarAhead` (:1480), the expiry scan finds no expiring
  lease so it defers forever, and no command in the set can advance the
  high-water — including heartbeats from brand-new nodes. Deterministic
  permanent wedge requiring state surgery.
- The fda75348 regression test covers only the leases-still-present downtime
  shape, so it cannot catch this.
- Fix shape: a bounded high-water catch-up on the heartbeat/re-admission
  path (or at authority restart), symmetric with the expiry carve-out.

R3-1 and R3-2 are two faces of one flaw: the carve-out is keyed on a signal
(live expiring leases) that neither distinguishes bogus clocks from real
downtime (R3-1) nor covers the real-downtime case it was built for once
leases are already cleared (R3-2). This guard has now produced a
high-severity availability issue in each of three review rounds. Before the
next attempt, write down the invariant (suggested: "committed time advances
at most MAX_JUMP per command regardless of path; every rejection class has a
bounded, automatic recovery path") and pin it with an adversarial-clock
proptest — the injectable-clock harness remains the top testing-gap
recommendation from round 1.

2026-07-09 update: R3-1/R3-2 are addressed by replacing the unbounded expiry
carve-out with bounded committed-timestamp ratcheting and by allowing
storage-node heartbeat re-admission to advance the durable high-water by at
most one forward-jump step per command. Expiry can still use the requested
`expire_at_ms` for lease expiry decisions, but it records only the bounded
high-water step; heartbeat re-admission after a clean full expiry can then
catch up automatically across subsequent heartbeats. Regressions now cover
healthy-cluster far-future expiry without unbounded ratchet, clean full expiry
followed by >1h downtime and heartbeat re-admission, repeated no-live-lease
far-future expiry deferral, and the experimental Raft wrapper path.

### R3-3. LOW-MEDIUM — `synchronous=FULL` fsync inside the per-PG mutex vs the 500ms lock budget

Folded into the MD5 status update above. Load-dependent availability, not
correctness; needs an fsync-pressure soak case and the group-commit /
write-amplification plans before scale testing.

### R3-4. LOW — typed control-plane errors are re-derived by Display-string sniffing after the RPC boundary, and the pattern is spreading

`PgPeeringPendingMetadataCommand` degrades to `RpcRemote{message}` across the
control-plane RPC and is reconstructed by
`message.contains("reported unresolved pending metadata command")`
(`cluster.rs:1379-1382`); the metadata-transfer retry classifiers in
`argmin-s3/src/main.rs:1280-1306` sniff several more Display shapes, and the
Raft serving loop's forward-to-leader benign matcher is still a two-substring
match on OpenRaft alpha error text. Any wording change silently alters
fencing/drain behavior on the remote path only (interacts with INT-3: the
sniff gates the cb13fc8b drain machinery). Fix: typed error codes on the
control-plane RPC error envelope.

### R3-5. INFO — 103e0880's check-applied retry strengthens the case for the CP8 CAS follow-up

The hardened route-change path re-submits `SetPgActingSet` after response
loss when the observed route epoch equals the pre-update epoch; confirmation
remains observational, so a delayed retry can still re-impose a stale
requested set over an intervening admin transition at a newer epoch.
Last-writer-wins admin semantics, no new mechanism beyond CP8's open
`expected_cluster_epoch` follow-up — which would eliminate it.

## Open items unchanged this round

INT-2 (future-epoch slot still node-fatal at bind — no per-PG quarantine),
INT-5 (per-syscall timeouts, read-handle idle exemption, `PayloadDecode`
reuse), CL2 (catch-up still dead code; proof mismatch still silent), CP2/CL4
(zero skew margin anywhere),
CP3, CP4 (still zero freshness-proof consumers), CP5's durable-history half
(pruning byte-identical; frontend startup still needs the full snapshot RPC
under the 8MB cap), CP6/CP7, CP9/CP10/CP11, CL5/CL6/CL7, RPC4/RPC6/RPC7/RPC9,
MD6 (the stale primary-last sentence is now two rounds old — one-line fix),
MD7, and INT-6 a/b/d/e/f (torn-WAL tail shapes, first-startup crash window,
WAL-poison gate bridge, artifact↔WAL generation binding, transient exit(1)
classes).

## Round-3 priorities

1. **DONE — R3-1/R3-2** — redesign the committed-timestamp carve-out
   (bounded ratchet + re-admission catch-up), with focused adversarial-clock
   regressions. Same availability tier as INT-2.
2. **INT-2** — per-PG bind quarantine (unchanged since round 2).
3. **CL1** — deposed-lease capture scaffolding, then the wait-out fence on
   all Active-exit transitions (unchanged since round 1; still the top
   pre-existing safety gap).
4. **A1** — shrink the heartbeat auth replay window to a fixed freshness
   bound; cheap and directly narrows the CP2/CL4 fencing exposure.
5. **INT-3 tightening, CP5's remaining half, and the R3-3 fsync/contention
   interaction** before any load/scale testing.
6. **Phase 12.4 exit criteria with no code yet:** Raft peer transport auth
   enforcement beyond the identity envelope, the Raft retry/confirmation
   contract, and replacing the substring benign-error matching (R3-4) with
   typed classification.
7. **CP2/CL4 skew margin** remains the standing design item feeding R3-1/R3-2,
   CL1, A1, and the drain/reservation wall-clock comparisons — one
   time-discipline design closes the family.

Meta-observation across three rounds: fix throughput is high and most
findings close cleanly, but the committed-timestamp / lease-time guard has
produced a new high-severity availability issue in every round (INT-1 in the
interim pass, then R3-1/R3-2 from its fix). That subsystem should not receive
another point fix without a written invariant and an adversarial-clock
proptest first — the injectable-clock harness is now the highest-value
testing investment.

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
12. Add a repair-path simulation for the acknowledged-command-lost shape
    (MD5 follow-up): the main durability setting now uses WAL with
    `synchronous=FULL`, but a destructive restored-filesystem test would still
    be useful to pin fail-closed behavior if an operator has to repair a node
    from an older disk image.
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
  (peer-ack→checkpoint, apply→slot-removal, fanout mid-flight). A
  power-loss-shaped restored-filesystem SQLite test remains useful as an MD5
  follow-up to pin fail-closed repair behavior if an operator restores a node
  from an older disk image; the primary commit-path setting now uses WAL with
  `synchronous=FULL`.
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
