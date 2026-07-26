---
id: DROPPANIC-001
bug_class: drop-panic
title: StorageNodeSession::drop() can panic via checked_sub().expect() while holding the shared read-handle Mutex, risking abort-on-double-panic
location: crates/storage/src/storage_node_server.rs:17351
function: drop
confidence: Medium
worker: worker-12
fp_verdict: FALSE_POSITIVE
fp_rationale: "No client-reachable accounting mismatch exists. Shared read-handle acquisition and the private session-owned handle are created together; explicit release removes that handle once, and Drop releases only handles still marked acquired. Duplicate entries are counted symmetrically. An unrelated panic therefore unwinds one valid acquisition rather than triggering the asserted underflow."
severity: NONE
attack_vector: Remote
exploitability: Not applicable
severity_rationale: "The panic requires a prior internal invariant violation or memory corruption; no RPC input, disconnect, error return, or ordinary panic unwind can create the mismatch. Saturating the counters would weaken the live-read deletion fence and could create a real correctness defect."
status: invalid
---

## Description
`StorageNodeSession` is the per-connection RPC session state for the
storage-node server (constructed once per accepted RPC connection in
`handle_session`, and dropped whenever that connection/session ends —
normal completion, error return via `?`, or panic unwind). Its `Drop`
impl locks the process-wide `shared_handles: Arc<Mutex<StorageNodeReadHandleState>>`
and calls `StorageNodeReadHandleState::release`, which contains three
`.expect(...)` calls on `checked_sub`/`get_mut` results and one plain
`*entry -= 1` — all executed **while the Mutex guard is held**.

If any of these invariants is ever violated (an acquire/release
accounting mismatch — e.g. a future refactor of `acquire_read_handles`/
`release_read_handles` that lets `is_acquired` get out of sync with the
global counters, or two sessions racing on the same `ReadHandleShardKey`
in a way not covered by today's bookkeeping), `Drop::drop` panics
*inside* the locked critical section. A panic in `Drop` matters in two
independent ways here:

1. It poisons `shared_handles`. Every other in-flight and future
   `StorageNodeSession`'s `acquire_read_handles`/`release_read_handles`/
   `Drop` calls `.lock().unwrap_or_else(|e| e.into_inner())`, so the
   poison is silently swallowed on the *locking* side — but the state
   the panicking thread was mutating may now be inconsistent for every
   other client connected to this storage node (poison recovery gives
   you the guard back, not a promise the protected invariants still hold).
2. If `StorageNodeSession::drop` runs while the stack is already
   unwinding from an unrelated panic elsewhere in the same
   `handle_session` call (e.g. a distinct `.expect()`/`unwrap()` on
   attacker-influenced RPC input reachable earlier in the same request),
   a second panic during that unwind aborts the whole storage-node
   process — not just the one session/thread. That turns any other,
   unrelated panic-DoS bug in this request-handling path into a
   full-process abort instead of a single failed request.

## Code
```rust
// crates/storage/src/storage_node_server.rs
impl Drop for StorageNodeSession {
    fn drop(&mut self) {
        let mut shared_handles = self
            .shared_handles
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        for existing in self.read_operations.values_mut() {
            if existing.is_acquired {
                shared_handles.release(&existing.entries);   // <-- can panic while `shared_handles` guard is held
                existing.is_acquired = false;
            }
        }
        ...
    }
}

fn release(&mut self, entries: &[(ShardLocation, ShardKey)]) {
    self.live_read_operations = self
        .live_read_operations
        .checked_sub(1)
        .expect("read handle operation release without acquire");
    self.live_read_handle_locations = self
        .live_read_handle_locations
        .checked_sub(entries.len())
        .expect("read handle location release without acquire");
    for (location, shard_key) in entries {
        let key = ReadHandleShardKey::new(*location, shard_key);
        let entry = self
            .handle_counts
            .get_mut(&key)
            .expect("read handle release without acquire");
        *entry -= 1;   // unguarded decrement, also inside the locked section
        if *entry == 0 {
            self.handle_counts.remove(&key);
        }
    }
}
```

## Data flow
- **Source:** any storage-node RPC client (intra-cluster; ultimately driven by
  S3 client requests routed through the coordinator) opens a connection that
  is handled by `handle_session`, which constructs `StorageNodeSession::new(...)`
  at `crates/storage/src/storage_node_server.rs:6716`.
- **Sink:** `impl Drop for StorageNodeSession` at
  `crates/storage/src/storage_node_server.rs:17345`, which calls
  `StorageNodeReadHandleState::release` at `crates/storage/src/storage_node_server.rs:17004`.
- **Validation:** none beyond the `is_acquired` flag and the map removal
  performed by `acquire_read_handles`/`release_read_handles`; there is no
  fallback if the global counters and a session's local bookkeeping ever
  diverge — the mismatch is asserted via `.expect()`/`get_mut().expect()`
  rather than handled.

## Reachability trace
`accept RPC connection -> handle_session -> StorageNodeSession::new (line 6716)
-> ... acquire_read_handles/release_read_handles during the session ...
-> (connection closes / handler returns / handler panics) -> implicit drop of
`session` local -> StorageNodeSession::drop -> StorageNodeReadHandleState::release
(while holding `shared_handles` lock)`.

## Impact
Under REMOTE threat model this is intra-cluster storage RPC, but it sits on
the hot path of every read operation the coordinator performs on behalf of
S3 GetObject/HeadObject requests. A panic here while unwinding from another
panic aborts the whole storage-node process (multi-tenant DoS, not just a
single request failure); even without a concurrent unwind, the resulting
Mutex poisoning affects the single global `shared_handles` state shared by
every concurrently-connected RPC session on that node.

## Mitigations checked
- The *locking* of `shared_handles` is poison-tolerant everywhere
  (`unwrap_or_else(|e| e.into_inner())`), but that only prevents the lock
  acquisition itself from panicking on a poisoned mutex — it does not
  prevent `release()`'s own `.expect()`/`get_mut().expect()` calls from
  panicking, nor does it repair the counters after a poisoning panic.
  These asserts are unconditional (not `debug_assert!`), so they fire in
  release builds too.
- No `catch_unwind` boundary was found around `handle_session`'s per-connection
  dispatch that would contain a double panic locally rather than propagating
  it to an abort; this was not exhaustively traced across the whole listener
  loop given the size of `storage_node_server.rs`.
- The `is_acquired` bookkeeping in `acquire_read_handles`/`release_read_handles`
  appears self-consistent for the paths reviewed, so there is no demonstrated
  concrete trigger today — this finding documents the panic-in-Drop-while-
  holding-a-lock hazard and its abort/poison blast radius, which the fp+severity
  judge should weigh against how provably reachable a counter-mismatch is.

## Recommendation
Make `release()` infallible: saturate/clamp the counters instead of
`.expect()`-ing on an invariant, log-and-continue if `handle_counts` is
missing an entry, and never call code that can panic while a `MutexGuard`
from a process-wide shared lock is held inside a `Drop` impl. If the
invariant is truly load-bearing, check it *before* taking the lock (or via
a `debug_assert!` that is compiled out in release) rather than panicking
inside the critical section.

## Validity assessment

This report is invalid as a security finding. It identifies an assertion in
a destructor, but it does not identify a path that can violate the asserted
invariant.

`StorageNodeSession` exclusively owns its `read_operations` map and the
corresponding `is_acquired` flags. Acquisition increments the shared operation,
location, and per-shard counters before inserting exactly one acquired session
record. Explicit release removes that record and decrements the shared state
once. Session destruction visits only records that remain acquired. Repeated
release requests are idempotent because the first release removes the local
record, and duplicate shard entries are incremented and decremented with the
same multiplicity. Other sessions can share a shard key, but each contributes
and removes its own count under the same mutex.

Consequently, normal completion, RPC errors, disconnects, and an unrelated
panic unwind all destroy one balanced session-owned acquisition. They do not
make the destructor panic. Reaching an underflow or missing map entry requires
a prior internal accounting bug, unsafe memory corruption, or direct test-only
state fabrication. A hypothetical future bug is not a remotely reachable
trigger in the current implementation.

The proposed saturation/clamping mitigation is unsafe. These counters are the
load-bearing fence preventing shard deletion while reads still depend on the
payload. Silently reducing or removing a count after detecting inconsistent
state could permit deletion during a live read. If a future project-wide
non-panicking-destructor policy is adopted, this path would need a two-phase
validated release and a persistent fail-closed accounting-fault latch that
rejects new reads and shard deletion; it must not saturate or continue with an
undercounted state.

## Resolution

Marked invalid after ownership and mutation-path review. No production code
change was made because the private session capability already makes balanced
release the only representable production path, and the recommended change
would weaken storage correctness.
