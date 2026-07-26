---
id: ASYNCBLOCK-001
bug_class: async-blocking
title: Synchronous fsync'd WAL append runs directly on the tokio executor inside openraft's async RaftLogStorage trait methods
location: crates/storage/src/control_plane_raft.rs:9938
function: append
confidence: High
worker: worker-18
fp_verdict: TRUE_POSITIVE
fp_rationale: "The executor-blocking availability defect was valid, but the proposed per-call spawn_blocking wrapper was insufficient for OpenRaft's accepted/readable versus IOFlushed durability contract. The fix uses one bounded ordered durability lane, publishes accepted reads before append returns, and completes IOFlushed only after fsync."
severity: LOW
attack_vector: Internal
exploitability: Contention-dependent
severity_rationale: "Authenticated Raft/control-plane traffic combined with slow or contended storage could stall cooperative executor workers and degrade availability. There was no memory-safety, authentication, authorization, or committed-state bypass."
status: fixed
---

## Description
`ControlPlaneRaftLogStore` implements openraft's `RaftLogStorage` trait for the
control-plane's experimental multi-host Raft consensus module. All of the
trait's required methods are `async fn` (as openraft's API demands), but their
bodies perform **synchronous, blocking disk I/O with a full `fsync`** directly
in the function body with no `.await` anywhere inside — meaning the entire
disk round trip runs to completion on whatever tokio worker thread happens to
be polling this future, with no `tokio::task::spawn_blocking` /
`tokio::task::block_in_place` offload.

The call chain is: `append`/`save_vote`/`save_committed`/`truncate_after`/
`purge` (all `async fn`, lines 9913-9979) → `self.apply_record(...)` (sync fn,
line 7008) → `wal.append_record_for_log_store(record)` (line 7254) →
`self.journal.append_frame(&frame)` in `durable_journal.rs` → opens the WAL
file with `OpenOptions`, `write_all`s the frame, and calls `file.sync_all()`
(`durable_journal.rs:210`) — a synchronous `fsync(2)` — all without ever
yielding to the executor.

Since these are the openraft storage-trait methods invoked on **every** raft
log append (every control-plane command proposal on the leader, and every
`append_entries` replication call accepted on a follower/peer), this blocks
the tokio worker thread for the full duration of a disk write + fsync on
every single control-plane mutation and every incoming peer replication
frame. If the S3 HTTP-serving tasks share the same tokio runtime/thread pool
(the standard single-process deployment model described in the codebase
context), a slow disk, a burst of control-plane commands, or a high rate of
`append_entries` from cluster peers stalls that worker thread and delays or
starves any other task scheduled on it, including in-flight S3 request
handling — a remote-triggerable latency/DoS amplification vector.

## Code
```rust
    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = ControlPlaneRaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        {
            let mut inner = self.lock()?;
            if let Err(error) =
                self.apply_record(&mut inner, &ControlPlaneRaftWalRecord::Append(entries))
            {
                let message = error.to_string();
                callback.io_completed(Err(raft_log_store_error(message.clone())));
                return Err(raft_log_store_error(message));
            }
        }
        callback.io_completed(Ok(()));
        Ok(())
    }
```
`apply_record` (line 7008) calls `wal.append_record_for_log_store`, whose
implementation in `durable_journal.rs:191-213` synchronously opens, writes,
and `fsync`s the file:
```rust
let mut file = OpenOptions::new()
    .create(true)
    .read(true)
    .append(true)
    .open(&self.path)
    ...
self.write_replayable_bytes(&mut file, &frame_prefix, ...)?;
self.write_replayable_bytes(&mut file, frame, ...)?;
let file_sync_result = self.observer.before_file_sync(&self.path).and_then(|()| {
    file.sync_all().map_err(...)
});
```

## Data flow
- **Source:** any accepted raft log entry — a control-plane command submitted
  via `submit_control_plane_command` (leader path) or an `append_entries` RPC
  frame received from an authenticated peer (`control_plane_raft.rs:1926`,
  intra-cluster follower path), both of which flow into openraft's internal
  log-storage calls.
- **Sink:** `std::fs::File::sync_all()` (blocking fsync) at
  `crates/storage/src/durable_journal.rs:210`, invoked synchronously from the
  `async fn append`/`save_vote`/`save_committed`/`truncate_after`/`purge`
  trait methods with no `.await` boundary and no `spawn_blocking` wrapper.
- **Validation:** none — the blocking call always executes on whatever thread
  polls the future; there is no offload.

## Reachability trace
`ControlPlaneRaftAuthority::submit_control_plane_command` (or an incoming
`ControlPlaneRaftNetwork::append_entries` from a peer) → openraft internal
consensus engine → `RaftLogStorage::append`/`save_vote`/... (async trait impl,
`control_plane_raft.rs:9913-9979`) → `apply_record` (sync,
`control_plane_raft.rs:7008`) → `ControlPlaneRaftWalFile::append_record_for_log_store`
(`control_plane_raft.rs:7243`) → `DurableJournal::append_frame` /
`append_frame_inner` (`durable_journal.rs:144-213`) → `File::sync_all()`.

## Impact
A blocking fsync executed directly on a shared tokio worker thread stalls
that thread's cooperative scheduler until the disk I/O completes. Under a
slow/contended disk (or an attacker driving a high rate of authenticated
peer `append_entries` traffic or control-plane command submissions once the
multi-host Raft control plane is active), other tasks scheduled on the same
worker — including S3 request-handling futures on a shared runtime — are
delayed, degrading availability service-wide. This is the canonical
async-blocking-in-executor hazard the `async-blocking-finder` pass targets:
disk I/O performed directly inside `async fn` bodies with no
`spawn_blocking`/`block_in_place` offload.

## Mitigations checked
- No `spawn_blocking` / `block_in_place` wrapper anywhere in the call chain
  from the `async fn` trait methods down to `file.sync_all()`.
- `self.lock()` (the in-memory `std::sync::Mutex` guard acquired first) is a
  separate, short-lived, non-IO lock and is not itself the hazard — the
  fsync happens after the lock is dropped/held only around the in-memory
  state clone, but the actual disk write+fsync in
  `append_record_for_log_store` still runs synchronously on the executor.
- No `// SAFETY:` or doc comment anywhere in `durable_journal.rs` or
  `control_plane_raft.rs` acknowledges or justifies running fsync on the
  async executor thread.
- This is guarded by openraft's `tokio-rt` feature (per `Cargo.toml`) which
  assumes a standard tokio multi-thread runtime; there is no `current_thread`
  runtime opt-out that would make this moot.

## Recommendation
Wrap the log-store's WAL write path in `tokio::task::spawn_blocking` (or
`block_in_place` if only ever run on a multi-thread runtime), e.g. have
`append`/`save_vote`/`save_committed`/`truncate_after`/`purge` clone what they
need and call `tokio::task::spawn_blocking(move || apply_record(...)).await`
before returning. Because `apply_record` mutates `&mut inner` (a lock guard)
in place, the guard itself would need to move into the blocking closure (or
the mutation restructured to operate on an owned snapshot and be written back
afterward) — but the fsync-bearing disk write must not run inline on the
async executor thread.

## Validity assessment

The core report is a true positive. Synchronous WAL writes and `fsync` calls
were reachable from OpenRaft's async storage methods and could stall a Tokio
cooperative-runtime worker. The practical trigger is authenticated internal
Raft or control-plane activity combined with slow or contended durable
storage, so the attack vector is internal rather than directly remote from an
unauthenticated S3 client. The low availability severity remains appropriate.

The original recommendation was incomplete. OpenRaft 0.10 distinguishes
append acceptance from durable completion: `append()` must return once entries
are reader-visible, while the supplied `IOFlushed` callback completes only
after persistence. Independently wrapping every method in `spawn_blocking`
would move the stall but would still serialize Raft progress on each `fsync`,
and unordered blocking tasks could violate WAL ordering and checkpoint/
compaction barriers.

## Resolution

Fixed in
[`a9365a623e4b6c3a360bb41524547acac6fd5cae`](https://github.com/justincormack/argmin/commit/a9365a623e4b6c3a360bb41524547acac6fd5cae)
(`Isolate Raft WAL durability from async execution`). The broader OpenRaft
state-machine executor audit was completed in
[`52d4d7c5d980d11e9dd0b8bc2446e8cb707ba2ca`](https://github.com/justincormack/argmin/commit/52d4d7c5d980d11e9dd0b8bc2446e8cb707ba2ca)
(`Isolate OpenRaft state machine work from async runtime`).

Each WAL-backed authority now owns one bounded 64-slot serialized durability
lane on a dedicated OS thread. Append validation and candidate publication
make entries immediately visible to `LogReader`; the ordered lane then writes
and syncs the WAL, publishes the durable position, and completes `IOFlushed`.
Vote, committed-position, truncate, and purge operations use the same ordered
lane with their required durability semantics. Cancellation cannot discard an
accepted operation, queue saturation applies bounded backpressure, and WAL
failure or ambiguity retains fail-closed poison behavior. Checkpoint capture
and compaction cross the same publication barrier and cannot overtake pending
durability.

The follow-up commit moves expensive OpenRaft state-machine apply, snapshot
build/install, cached-snapshot refresh, and final retired-generation
destruction off cooperative executor workers. Deterministic single-worker
tests cover accepted read visibility during blocked sync, pending
`IOFlushed`, cancellation, bounded queueing, compaction barriers, state-machine
work, and actual post-publication destruction. The fixing slice passed the
complete control-plane release gate, Clippy across all targets and features,
and the workspace nextest suite (7,587 tests, excluding one separately known
S3 test under active repair).
