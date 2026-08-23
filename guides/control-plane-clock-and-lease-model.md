<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Control-plane clock and lease model

This document defines the clock and failure assumptions for control-plane
serving leases. It is the normative model for DCC-2, CP2, and CL4.

## Clock domains

The system has three distinct notions of time:

- **Replicated logical time** orders deterministic state-machine commands. The
  persisted timestamp high-water belongs to this domain. It is not, by itself,
  evidence that a real-time lease is safe.
- **Process wall time** is used only when an authority proposes a real-time
  deadline or a consumer binds a newly received deadline. Wall time may step
  forward or backward and is never consulted again to extend an installed
  serving lease.
- **Process monotonic lease time** enforces an installed lease for the lifetime
  of a process. A restart discards every monotonic binding. Linux, Android, and
  OpenBSD use `CLOCK_BOOTTIME`; Apple platforms use the sleep-inclusive
  `CLOCK_MONOTONIC_RAW`; and FreeBSD uses `CLOCK_MONOTONIC`. These are the
  platform-qualified suspend-inclusive branches. NetBSD, DragonFly, and other
  Unix targets retain `CLOCK_MONOTONIC` support defensively: if that source
  pauses over suspend, admission-time clock-health validation detects the
  wall/monotonic divergence and fails bounded leases closed. A clock read
  failure makes bounded serving leases unusable.
- **Process clock-health time** detects wall-clock steps. It normally uses the
  same source as lease time. Apple instead uses adjusted `CLOCK_MONOTONIC` for
  health while retaining raw continuous time for lease expiry. This prevents
  normal frequency correction from accumulating as a false wall-clock step;
  if the adjusted source pauses over suspend, the resulting divergence is a
  fail-closed health event while the raw lease deadline still advances.

## Operational assumption

`CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS` is 1,000 ms. It is the maximum supported
pairwise wall-clock offset between any two authority, frontend, or storage-node
processes while they participate in serving. This is an operational safety
assumption, not a convergence target.

A consumer rejects a freshly read map if the authority's signed/read-index
issue time is more than the skew budget ahead of its local wall time. A wall
clock that violates the assumption must therefore fail closed rather than
receive or renew serving authority. Large forward local steps shorten a lease.
Large backward steps after installation cannot extend one because serving uses
the monotonic binding.

Clock health is independently latched for the authority and for every consumer.
A consumer restart, monotonic-source failure, suspend on an unqualified source,
or wall-clock drift beyond the budget does not extend an existing monotonic
binding, but it prohibits creating a new binding. Re-establishment requires a
healthy authority and a fresh within-budget comparison between that consumer
and the authority. Consequently, a delayed old map cannot be rebound after
wall-clock rollback, including after a successor has activated.

Production consumers compare wall-time elapsed against clock-health elapsed
from their first bounded lease binding. A drift violation latches the process
unhealthy until restart. They repeat this check on every serving admission, not
only when installing a map. A qualified suspend-inclusive lease deadline
continues to expire while the host sleeps; an unqualified source is also gated
by the admission-time health comparison. Test clock overrides bypass this
process-global latch; the pure independent-clock model exercises clock faults
without contaminating parallel tests.

## Authority invariant

Every deadline that can authorize serving must satisfy:

```text
serving_deadline
    <= effective_committed_now + max_lease + skew_budget
```

The effective committed time used for this check must come from an explicit
real-time authority policy. Advancing a logical timestamp high-water once per
command is not time recovery. A proposal that cannot satisfy the invariant is
non-serving and must not preserve an older deadline whose time basis has been
declared invalid.

In a Raft authority, covered heartbeat renewals may advance a leader-local
volatile overlay without appending every renewal to the WAL. Runtime-map reads
must therefore be served by the current linearized leader. A follower can have
an up-to-date replicated log while still lacking the leader's latest volatile
lease renewal; it returns a typed leader-routing result so a
multi-endpoint client retries the leader. Full snapshots, compact status,
diagnostics, scoped PG maps, and pending-command recovery listings all follow
this rule. On the leader, a runtime-map read also selects the replicated
ReadIndex lower bound, then captures the current applied state-machine snapshot
and its matching volatile overlay under the heartbeat-update gate. The capture
copies only retirement-aware `Arc`-backed immutable generation handles.
Runtime-map/status derivation and the reader's final generation release occur
on the blocking lane after the gate is released; cancellation and early errors
also defer a retained generation's release there. An applied tip that advances
after ReadIndex is therefore included without repeating the quorum read or
falling back to an older durable snapshot. Quorum I/O likewise occurs outside
the gate so reads cannot starve heartbeat renewal.

The authority process establishes a wall/monotonic reference before issuing
timestamp-bearing work. Healthy wall progress may advance committed time by
the corresponding monotonic elapsed time, even when the authority has been
idle for longer than the skew budget. A wall/monotonic divergence beyond the
budget latches the authority clock non-serving. Durable control-plane state is
paired with a checksummed, node-local checkpoint naming a durable timestamp
prefix, the exact nonzero authority-clock generation, one coherent
wall/health-clock sample, and a fixed durable-authority identity binding. Raft
bindings cover cluster identity plus local node ID;
single-authority state has a separately persisted random durable identity. A
checkpoint copied from another authority therefore fails closed even when both
processes share the same host clocks. On restart,
arbitrarily long elapsed time is accepted when both clocks advanced by the
same amount within the skew budget. The restored high-water may advance beyond
the checkpoint while that process remains clock-healthy, but it may never be
older than the checkpoint. A missing, corrupt, high-water-mismatched,
regressed, or divergent checkpoint fails closed and requires the explicit
authority clock re-establishment procedure. Successful re-establishment
replaces the checkpoint before the admin RPC acknowledges success; a
checkpoint persistence failure re-latches the authority non-serving. The
checkpoint is local clock-lineage evidence; it is not replicated authority and
must never be copied between Raft nodes.

A single-authority process that already holds the exclusive durable-state lock
may treat a valid identity-bound restart checkpoint as proof that it is a
continuation of the persisted no-term lease-horizon authority. It resumes that
same bounded horizon generation rather than creating a successor and waiting
for its own horizon to expire, but only when the checkpoint generation exactly
equals the horizon binding. Continuation never assigns or rolls back a clock
generation. A clock fault advances the in-memory generation, and successful
authenticated re-establishment durably checkpoints the further-advanced
generation before acknowledging recovery; a later restart therefore cannot
reuse an older persisted horizon as continuation evidence. This is a one-shot
startup capability: a missing or invalid checkpoint, a generation mismatch,
or any Raft-term-bound horizon retains the ordinary new-generation successor
fence. A detected clock fault consumes the current process's continuation
capability and durably removes the restart checkpoint before the process can
continue. Authenticated recovery also removes and directory-syncs the old
checkpoint before writing its higher-generation replacement. A crash or
replacement-write failure therefore leaves no reusable old evidence. The new
generation becomes restart evidence only after the replacement file and its
directory are durable. The continuation may
extend only through the normal committed horizon command and does not recover
any volatile heartbeat observation or process-local consumer lease.

Recovery checkpointing takes a fresh wall/health sample, validates it through
the already re-established authority clock, and persists that exact accepted
sample before sampling the signed response time. A clock step between the
admin request and persistence therefore aborts the response and re-latches the
clock instead of becoming a trusted restart baseline.

`AuthorityClockStatus` is observational: it may latch a genuine fault found by
its accepted clock sample, but it never writes or refreshes the restart
checkpoint. Checkpoint persistence and persistence-failure fencing apply only
to a successful `ReestablishAuthorityClock` mutation, so routine diagnostics
cannot turn transient sampling or sidecar I/O pressure into an outage.

Wall and clock-health reads are not an atomic operating-system operation. The
authority brackets each wall read with two health-clock reads and accepts it
only when both health reads fall in the same millisecond. It retries a wide
window locally; if every attempt is descheduled, that admission is deferred
without changing clock generation or health state. Scheduler delay therefore
cannot masquerade as wall-clock regression, and sampling uncertainty does not
consume or enlarge the stated skew budget.

Replicated apply independently rejects timestamp regression and validates that
every serving deadline is bounded relative to the command's committed time.
It cannot infer real elapsed time from the logical high-water; the
process-local authority gate supplies that evidence before command proposal.

### Explicit authority recovery

A clock discontinuity, missing health-clock sample, or new local Raft
leadership term latches the process-local authority clock non-serving. It does
not recover merely because a later sample looks plausible. Operators inspect
the authenticated `control-plane-authority-clock-status` RPC and, after the
host clock has been corrected and cluster skew is again within policy, invoke
`control-plane-reestablish-authority-clock` with admin credentials.
Status is an observing operation, not a passive read of the last serving
request. Under the process-local clock lock it first incorporates the current
committed timestamp high-water and validates the current Raft term, wall-clock
sample, and health-clock sample. Any newly observed fault is latched and
reported in that same response.

Recovery is fenced by the status observation. The request carries the exact
process-local clock generation, committed timestamp high-water, and current
Raft term. The server rejects a changed generation, timestamp, or term; a Raft
follower or leader that is not applied through committed state; an unavailable
health-clock sample; and wall time below the committed timestamp high-water.
A successful recovery binds a fresh wall/health-clock reference to the current
term and advances the generation. Replaying that request cannot clear a later
fault, including a second fault in the same Raft term. If the response is lost,
the client confirms the exact next generation and unchanged authority tuple;
it does not retry the state change blindly.

This operation is deliberately process-local. It does not rewrite replicated
time or assert that logical command count represents elapsed time. Admin
authentication proves who requested recovery; the operator remains
responsible for establishing that the corrected host clock and peer clocks
satisfy the deployment skew bound before invoking it.

## Consumer binding

For authority deadline `D`, local wall sample `W`, local monotonic sample `M`,
and skew budget `S`, a consumer installs:

```text
local_monotonic_deadline = M + max(0, D - S - W)
```

The consumer serves only while its current monotonic time is strictly less
than that deadline. Network and processing delay only reduce the remaining
time. The original authority deadline remains available for diagnostics and
protocol fencing, but is not re-evaluated against wall time while serving.

Restart or loss of either monotonic source clears the binding. A qualified
suspend-inclusive source advances the installed deadline during sleep. On an
unqualified source, a pause beyond the skew budget is detected at the next
admission and clears the binding; a smaller pause remains inside the same skew
margin used by the lease equations. The process remains non-serving after
binding loss until it installs a fresh linearized runtime map and creates a new
binding.

## Successor activation

A successor authority with wall sample `N` may pass an old-primary fence with
deadline `D` only when:

```text
N >= D + S
```

The consumer-side subtraction protects against a slow old consumer relative to
the original issuer. The successor-side addition protects against a fast new
leader relative to that issuer. Together, under the pairwise skew assumption,
an old serving or mutation permit cannot overlap successor activation.

## Failure behavior

- A far-future heartbeat or leader clock must not mint a far-future serving
  deadline. The process-local clock-health latch prevents repeated commands
  from ratcheting recovery.
- A backward authority step must not regress replicated timestamps or revive
  an expired deadline. Recovery is based on monotonic elapsed time or an
  explicit fenced operator procedure.
- Leader change does not transfer process-local monotonic bindings. The new
  leader re-establishes real-time authority before minting leases or passing a
  successor fence. Authority clock health is bound to one locally serving Raft
  term: a new local leadership term or an out-of-budget authority step clears
  it, and ordinary command traffic cannot restore it. A fresh cluster with no
  committed timestamp may bind its first term. Re-establishment must prove the
  new clock is within budget of the trusted participant clocks and of the
  original issuer of every outstanding deadline. Relabelling an old deadline
  with each new leader would allow skew to accumulate across elections and is
  forbidden.
- Frontend and storage-node wall clocks are independent. Neither may reopen a
  lease after binding.
- Clock-bound violations are safety failures and fail closed. They require
  visible diagnostics and operator correction; the system does not silently
  widen the skew budget.

## Executable model

`crates/storage/src/control_plane_lease.rs` implements the pure deadline bound,
consumer binding, and successor fence. Its property test generates independent
authority, frontend, and storage-node wall clocks; independent monotonic
clocks; forward and backward steps; restarts; leader changes; suspend events;
heartbeat rates; and successor activation attempts.

The authority process boundary now validates wall progress against explicit
clock-health samples and binds that authority to one local Raft term, while
replicated apply rejects timestamp regression and bounds serving deadlines.
Frontends and storage nodes bind fresh runtime maps to platform-qualified or
defensively monitored monotonic lease time, reject unavailable health samples,
revalidate clock health on serving admission, and retain the old map's
monotonic fence on historical storage-node mutation permits. Successor
activation waits through the skew margin. Remaining production work is the
authenticated authority clock re-establishment operation, its operator
diagnostics, and independent-host fault validation.
