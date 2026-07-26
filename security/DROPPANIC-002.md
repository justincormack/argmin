---
id: DROPPANIC-002
bug_class: drop-panic
title: Route-admission permit Drop impls panic via checked_sub().expect() while holding the admission Mutex
location: crates/storage/src/cluster.rs:1461
function: drop
confidence: Medium
worker: worker-12
fp_verdict: FALSE_POSITIVE
fp_rationale: "The admission permits are private, non-cloneable RAII capabilities created only after incrementing the matching counter under the same mutex. Each permit has exactly one destructor, and the counters have no independent decrement path. Completion, errors, and panic unwinding therefore remain balanced; no request can trigger the asserted underflow."
severity: NONE
attack_vector: Remote
exploitability: Not applicable
severity_rationale: "No security impact is reachable in the current ownership model. Saturating an admission counter would be fail-open and could permit route publication, successor activation, or excess RPC admission while an older capability remains live."
status: invalid
---

## Description
`StorageClusterRouteAdmissionPermit` is acquired once per admitted request
on the main cluster route path (`StorageClusterRouteAdmissionGate::acquire`,
called for every request that is admitted onto the current storage-cluster
route) and released when the permit is dropped at the end of the request.
Its `Drop` impl locks the gate's `state: Mutex<StorageClusterRouteAdmissionState>`
and then calls `.checked_sub(1).expect(...)` on `active_requests` **while
the guard is held**. If the counter is ever decremented more times than it
was incremented (e.g. by a future bug that constructs a permit without
going through `acquire()`, or a bug in `begin_publication`/route-transition
bookkeeping that also touches `active_requests`), this `.expect()` panics
inside the locked critical section, poisoning `state` and — if it happens
while another panic is already unwinding the same task — risking a full
process abort instead of a single failed request.

The identical pattern (`checked_sub(1).expect("... must not underflow")`
inside `Drop`, while holding a `Mutex` guard) recurs at:
- `crates/storage/src/storage_node_server.rs:3280` (`StorageNodeRouteAdmissionPermit::drop`)
- `crates/storage/src/node_client/unix_admission.rs:432-439` (`UnixStorageNodeObjectPayloadLeaseAdmissionPermit::drop`)
- `crates/storage/src/node_client/unix_admission.rs:127-138` (`UnixStorageNodeRpcAdmissionActive::release`, called from `UnixStorageNodeRpcAdmissionPermit::drop`)

All four are RAII admission/rate-limit counters on the request hot path
sharing the same design: a defensive `.expect()` assertion that a
counter never underflows, executed while a shared `Mutex` is held inside
`Drop`. This finding documents the pattern using `cluster.rs` as the
most central, always-on-path instance; the sibling sites should be fixed
the same way.

## Code
```rust
// crates/storage/src/cluster.rs
struct StorageClusterRouteAdmissionPermit {
    gate: StorageClusterRouteAdmissionGate,
}

impl Drop for StorageClusterRouteAdmissionPermit {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.active_requests = state
            .active_requests
            .checked_sub(1)
            .expect("route admission permit count must not underflow"); // <-- panics while `state` guard held
        self.gate.inner.changed.notify_all();
    }
}
```

## Data flow
- **Source:** every S3 request admitted onto the current storage-cluster
  route calls `StorageClusterRouteAdmissionGate::acquire()`
  (`crates/storage/src/cluster.rs:1365`), which returns a
  `StorageClusterRouteAdmissionPermit` held for the lifetime of the request.
- **Sink:** `impl Drop for StorageClusterRouteAdmissionPermit` at
  `crates/storage/src/cluster.rs:1450`.
- **Validation:** none beyond `checked_sub`'s own `None`-on-underflow —
  the `None` case is turned into a panic via `.expect()` rather than
  saturated or logged.

## Reachability trace
`S3 client request -> route admission -> StorageClusterRouteAdmissionGate::acquire
(crates/storage/src/cluster.rs:1365) -> ... request processed, permit held ...
-> (request completes / errors / panics) -> permit dropped ->
StorageClusterRouteAdmissionPermit::drop (crates/storage/src/cluster.rs:1450)`.

## Impact
This permit is constructed on essentially every admitted request, making
it one of the highest-traffic `Drop` impls in the codebase. A panic here
poisons the single process-wide `StorageClusterRouteAdmissionGateInner::state`
mutex used by every concurrent request's admission accounting, and — in
the double-panic-during-unwind case — can abort the entire process,
escalating any other reachable panic bug on the request path into a
full outage rather than one failed request.

## Mitigations checked
- Locking is poison-tolerant (`unwrap_or_else(|poisoned| poisoned.into_inner())`)
  but this only protects the *lock acquisition*, not the `.expect()` call
  performed after the lock is obtained.
- No evidence of a concrete double-acquire/double-release bug was found in
  the reviewed `acquire`/`begin_publication` bookkeeping — the invariant
  appears to hold under normal RAII usage today. This finding is filed
  because the gate criteria (panic op inside `Drop`, holding a `Mutex`,
  on a user-reachable, always-constructed type) are met, and because the
  same defensive-assert-inside-a-held-lock pattern is repeated at four
  separate sites, increasing the chance that a future edit to any one of
  them reintroduces a real accounting bug that turns this into a live
  abort/poison DoS.

## Recommendation
Replace `.checked_sub(1).expect(...)` inside these `Drop` impls with a
saturating decrement plus a `debug_assert!`/logged warning, so a future
accounting bug degrades to an incorrect (but recoverable) counter rather
than a panic while holding a process-wide `Mutex`. Apply the same fix to
the three sibling sites listed above.

## Validity assessment

This report is invalid. The presence of `checked_sub().expect()` in a `Drop`
implementation is not by itself a reachable panic.

`StorageClusterRouteAdmissionPermit` is private and non-cloneable. Its only
production constructors increment `active_requests` under the gate mutex and
immediately return one permit. The only decrement is that permit's destructor.
Publication changes the transition state but does not decrement the request
counter. Therefore every representable permit contributes exactly one count
and removes exactly one count, including when a request exits through an error
or panic unwind.

The sibling storage-node route and Unix RPC admission permits have the same
structure: private non-cloneable values are created only after the matching
class/session counters are incremented under the same lock, and their sole
destructor performs the matching decrement. The report found no independent
counter mutation, forged production constructor, double release, or
client-controlled state transition that could violate this ownership
invariant. The storage-node active-session guard follows the same pattern and
is likewise balanced.

The recommended saturating decrement would turn an impossible internal state
into fail-open behavior. An undercount could let route publication or
successor activation proceed while an old request or frame remains admitted,
or let the RPC admission layer exceed reserved and total capacity. Replacing
the assertion safely would require a persistent accounting-fault latch and
typed fail-closed errors from all admission and publication operations, not a
clamped counter. That larger reliability policy is not justified by a current
trigger.

## Resolution

Marked invalid after reviewing every production permit constructor and counter
mutation. No production code change was made because the RAII capability model
already makes underflow unrepresentable, while the proposed mitigation would
weaken route and resource-admission safety.
