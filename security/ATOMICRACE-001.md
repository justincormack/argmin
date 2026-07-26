---
id: ATOMICRACE-001
bug_class: concurrent-endpoint-failover
title: Shared endpoint hint can redirect an in-flight control-plane failover pass
location: crates/storage/src/control_plane.rs:10740
function: send_request_until
confidence: High
worker: worker-5
fp_verdict: TRUE_POSITIVE
fp_rationale: "The availability issue was valid, but the original mechanism and proposed fix were incomplete: preferred_socket_index is a best-effort hint rather than a monotonic counter. Reloading it during one bounded failover operation allowed concurrent callers to redirect that operation back to an endpoint it had already tried, potentially exhausting the retry budget without reaching a healthy endpoint."
severity: LOW
attack_vector: Internal
exploitability: Contention-dependent
severity_rationale: "Concurrent internal RPCs and leader-routing churn could cause bounded failover to miss a healthy endpoint and fail an operation. This is an availability issue without memory-safety, authentication, authorization, or committed-state impact."
status: fixed
---

## Original report description
`UnixControlPlaneClient` (and its framed-transport variant) is `#[derive(Clone)]`
with `preferred_socket_index: Arc<AtomicUsize>`. Clones of the client share the
same atomic and are used concurrently — every S3/control-plane RPC path that
calls `send_request_until` / `send_request_raw_response_until` may run on a
different worker thread at the same time, and each of those call sites reads
and then independently writes `preferred_socket_index` via
`preferred_socket_index()` (a `load`) followed by `prefer_socket_index(...)`
(a `store`), or via `advance_preferred_socket()` which combines the two. There
is no `compare_exchange`/`fetch_update` tying the read and the write together,
and no mutex serializes the pair, so two threads can race: both load the same
starting index, both compute the same "next" index, and one thread's store
clobbers the other's — a classic non-atomic RMW race on a value that is
supposed to be monotonically rotated for round-robin/failover endpoint
selection.

## Original code
```rust
fn preferred_socket_index(&self) -> usize {
    self.preferred_socket_index.load(Ordering::Acquire) % self.endpoint_count()
}

fn prefer_socket_index(&self, socket_index: usize) {
    self.preferred_socket_index
        .store(socket_index % self.endpoint_count(), Ordering::Release);
}

fn advance_preferred_socket(&self) {
    let next = (self.preferred_socket_index() + 1) % self.endpoint_count();
    self.prefer_socket_index(next);
}
```

## Original data flow
- **Source:** concurrent RPC call sites in `send_request_until` /
  `send_request_raw_response_until` (`crates/storage/src/control_plane.rs:10770-10889`),
  triggered by concurrent client requests that each drive a control-plane RPC
  (e.g. leader-routing retries and per-endpoint connect/exchange failover).
- **Sink:** `AtomicUsize::store` in `prefer_socket_index` racing against
  concurrent `AtomicUsize::load` in `preferred_socket_index` from another
  thread's in-flight call.
- **Validation:** none — no `compare_exchange`/`fetch_update` and no mutex
  guards the load-then-store pair; `Acquire`/`Release` orderings only fix
  visibility, not atomicity of the combined operation.

## Original reachability trace
`S3/control-plane client request handler thread(s)` → `UnixControlPlaneClient::send_request_until`
(or `::send_request_raw_response_until`) → `advance_preferred_socket` /
`prefer_socket_index` → racy `load` + `store` on the shared
`Arc<AtomicUsize>` also reachable from every other cloned handle of the same
client held by concurrently-running request threads.

## Original impact assessment (superseded)
A lost update only causes the client to pick a stale/incorrect "preferred"
starting endpoint index for the next RPC attempt. Because every call site
still iterates `0..endpoint_count()` starting from whatever index it read
(`for offset in 0..self.frame_transport_endpoints.len()`), correctness of
endpoint selection is not violated — in the worst case a lost update causes
one extra failed-endpoint retry (an already-known-bad endpoint is tried again
before falling through to a working one), which is a minor availability/
performance degradation, not a memory-safety or auth-bypass issue.

## Original mitigations checked
- No `// SAFETY:` comment applicable (this is safe-code atomic misuse, not
  `unsafe`).
- No `compare_exchange`/`fetch_update` used despite the value depending on
  itself (unlike the sound `fetch_update`-based CAS loops used elsewhere in
  this codebase, e.g. `try_acquire` in `crates/server-core/src/coordinator/runtime.rs`
  and `reserve_control_plane_rpc_worker` in `crates/argmin-s3/src/main.rs`).
- No `Mutex`/`RwLock` wraps the read-modify-write.

## Original recommendation (superseded)
Replace the load-then-store pair with a single atomic RMW, e.g.
`self.preferred_socket_index.fetch_add(1, Ordering::AcqRel)` for the "advance"
case (taking `% endpoint_count()` only when read back), or a `fetch_update`
closure for `prefer_socket_index` so the intended index is set atomically
relative to concurrent advances.

## Validity assessment

The finding identified a real concurrency-driven availability defect, so the
`TRUE_POSITIVE` verdict is retained. The original analysis understated its
impact and proposed the wrong state model, however:

- `preferred_socket_index` was intended as a best-effort starting hint, not a
  monotonic round-robin counter. Concurrent replacement of that hint is not by
  itself a correctness defect.
- The defect was that one logical bounded failover operation reloaded the
  shared hint after each routing or transport failure. A concurrent caller
  could therefore redirect the operation to an endpoint it had already tried.
  Repeated interference could consume the operation deadline without trying a
  healthy alternate endpoint, rather than causing only one extra retry.
- `fetch_add` or `fetch_update` would not establish the required invariant.
  Counting concurrent failures can skip a healthy endpoint, while an atomic
  set still allows another request to alter an in-flight operation's route.
- The trigger is concurrent internal control-plane activity or leader-routing
  churn. It is not directly controllable as an unauthenticated remote S3
  request, so the attack vector is classified as internal and
  contention-dependent.

## Resolution

Fixed in [`a3ecba14`](https://github.com/justincormack/argmin/commit/a3ecba14)
(`Make control-plane endpoint failover request-local`).

Every logical control-plane operation now captures a request-local endpoint
pass from the shared hint. Transport failures, authenticated routing
rejections, read retries, admin calls, heartbeats, and authority-clock recovery
advance that local pass, so each endpoint is visited at most once before a new
pass begins. Concurrent clients may update the shared hint for future
operations but cannot redirect an operation already in flight.

The shared atomic is now only a best-effort cache. A decoded and, where
required, authenticated response may publish its successful endpoint. Failure
updates use `compare_exchange`, so a stale failure cannot overwrite a newer
hint published by another request. Authority-clock status recovery preserves
the same pass across post-send response loss and proceeds to a healthy
alternate before beginning another pass.

The deterministic regressions
`authenticated_endpoint_failover_ignores_concurrent_shared_hint_changes` and
`authenticated_authority_clock_recovery_status_moves_past_response_loss`
cover concurrent cache replacement and first-endpoint response loss,
respectively. The fixing commit passed the complete workspace nextest suite
(7,568 tests) and Clippy across all targets and features with warnings denied.
