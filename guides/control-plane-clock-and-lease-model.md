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
- **Process monotonic time** enforces an installed lease for the lifetime of a
  process. A restart discards every monotonic binding. A process suspend whose
  monotonic source cannot be proved to include suspend time is treated like a
  restart for serving purposes.

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
A consumer restart, untrusted suspend, or wall-clock drift beyond the budget
does not extend an existing monotonic binding, but it prohibits creating a new
binding. Re-establishment requires a healthy authority and a fresh
within-budget comparison between that consumer and the authority. Consequently,
a delayed old map cannot be rebound after wall-clock rollback, including after
a successor has activated.

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

Restart, loss of the monotonic source, or untrusted suspend clears the binding.
The process remains non-serving until it installs a fresh linearized runtime
map and creates a new binding.

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
  deadline and must not ratchet recovery once per command.
- A backward authority step must not regress replicated timestamps or revive
  an expired deadline. Recovery is based on monotonic elapsed time or an
  explicit fenced operator procedure.
- Leader change does not transfer process-local monotonic bindings. The new
  leader re-establishes real-time authority before minting leases or passing a
  successor fence. Authority clock health is an explicit, latched capability:
  leader change or an out-of-budget authority step clears it, and ordinary
  command traffic cannot restore it. Re-establishment must prove the new clock
  is within budget of the trusted participant clocks and of the original issuer
  of every outstanding deadline. Relabelling an old deadline with each new
  leader would allow skew to accumulate across elections and is forbidden.
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

The production integration must use these rules at all runtime-map install and
serving points before DCC-2, CP2, and CL4 can be marked fixed.
