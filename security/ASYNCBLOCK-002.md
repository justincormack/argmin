---
id: ASYNCBLOCK-002
bug_class: async-blocking
title: Durable Raft restart-checkpoint encode+write+fsync runs synchronously inside async checkpoint capture path
location: crates/storage/src/control_plane_raft.rs:5201
function: persist_durable_restart_checkpoint
confidence: Medium
worker: worker-18
fp_verdict: TRUE_POSITIVE
fp_rationale: "The public async checkpoint convenience API did synchronously encode, persist, fsync, and compact on its Tokio worker. The report overstated the production scheduler reachability because the production publisher uses an explicit capture plus process-thread persistence boundary, but the async API itself was unsafe and reachable in tests and direct callers."
severity: LOW
attack_vector: Internal
exploitability: Operational-load-dependent
severity_rationale: "A large checkpoint through the async convenience API could stall one cooperative executor worker. Checkpoints are bounded internal operations and the production periodic publisher already persists on a dedicated process thread, limiting reachability and impact."
status: fixed
---

## Description
`ControlPlaneRaftAuthority::store_durable_restart_artifact` (`async fn`,
`control_plane_raft.rs:5184`) captures a durable restart checkpoint and then
calls the plain (non-async) `persist_durable_restart_checkpoint` function
directly — with no `.await`, no `spawn_blocking`. That function encodes the
entire control-plane snapshot/log-store restart artifact
(`checkpoint.artifact.store_durable_artifact_with_metrics`,
`control_plane_raft.rs:5258`) and writes it to disk synchronously, including
a sentinel file write, artifact encoding, and — per
`store_durable_artifact_inner` (`control_plane_raft.rs:7533`) — the full
write path used elsewhere in this module (`fs::write`/rename plus
`file.sync_all()` per `durable_journal.rs`/`control_plane.rs` write helpers).
Since `store_durable_restart_artifact` is itself `async fn` and is called
from the same tokio-runtime-hosted control-plane machinery as the S3-facing
HTTP layer, this potentially-large (whole cluster-map snapshot) synchronous
encode+write+fsync runs to completion on a shared executor thread with no
offload.

## Code
```rust
pub async fn store_durable_restart_artifact(
    &self,
    path: &Path,
) -> Result<Option<u64>, ControlPlaneError> {
    let checkpoint = self.capture_durable_restart_checkpoint().await?;
    self.persist_durable_restart_checkpoint(checkpoint, path)   // <-- sync, blocking, no spawn_blocking
}
```
```rust
pub fn persist_durable_restart_checkpoint(
    &self,
    checkpoint: ControlPlaneRaftCapturedRestartCheckpoint,
    path: &Path,
) -> Result<Option<u64>, ControlPlaneError> {
    ...
    checkpoint
        .artifact
        .store_durable_artifact_with_metrics(path, Some(&self.checkpoint_metrics))?; // encodes + writes + fsyncs
    if let Some(log_store) = &self.log_store {
        let result = log_store.compact_wal_through(wal_replay_offset); // more synchronous WAL I/O
        ...
    }
    Ok(committed_timestamp_high_water_ms)
}
```

## Data flow
- **Source:** periodic/administrative durable-checkpoint capture of the
  control-plane Raft state (triggered by internal checkpoint scheduling or
  snapshot-trigger operations reachable through the control-plane Raft
  authority).
- **Sink:** synchronous file encode/write/fsync in
  `store_durable_artifact_inner` (`control_plane_raft.rs:7533` onward) and
  `log_store.compact_wal_through` (further synchronous WAL file I/O),
  executed directly from the `async fn store_durable_restart_artifact`
  without any `spawn_blocking`/`block_in_place`.
- **Validation:** none — no offload guard anywhere in this call path.

## Reachability trace
`ControlPlaneRaftAuthority::store_durable_restart_artifact` (async,
`control_plane_raft.rs:5184`) → `persist_durable_restart_checkpoint` (sync,
`control_plane_raft.rs:5201`) → `ControlPlaneRaftRestartArtifact::store_durable_artifact_with_metrics`
(`control_plane_raft.rs:7518`) → `store_durable_artifact_inner`
(`control_plane_raft.rs:7533`) → file encode + write + `fsync`.

## Impact
Same class of hazard as ASYNCBLOCK-001 but on the checkpoint path: a
potentially large (whole cluster-map) synchronous encode-then-fsync running
on a shared tokio worker thread blocks that thread for the duration of the
write, delaying any other task (including S3 request handling) scheduled on
it. Checkpoints are less frequent than log appends, so the exploitability is
lower than ASYNCBLOCK-001, but the same architectural gap — an `async fn`
public API whose body performs full synchronous disk I/O with no
`spawn_blocking` — applies here as well.

## Mitigations checked
- No `spawn_blocking`/`block_in_place` anywhere between
  `store_durable_restart_artifact` and the underlying `sync_all()` calls.
- No doc comment or `// SAFETY:`-equivalent justification for running this
  synchronously on the executor.
- Frequency is lower than the per-entry WAL append path (ASYNCBLOCK-001),
  somewhat limiting real-world exploitability, but the code path is
  unconditionally reachable whenever a durable checkpoint is captured.

## Recommendation
Move the call to `persist_durable_restart_checkpoint` (and, ideally, the
whole `store_durable_artifact_inner` + `compact_wal_through` sequence) into
`tokio::task::spawn_blocking`, awaiting the `JoinHandle` from
`store_durable_restart_artifact`, so the encode/write/fsync work runs on the
blocking thread pool rather than an async executor thread.

## Validity assessment

The finding is a true positive for the public async convenience API:
`store_durable_restart_artifact()` performed whole-artifact persistence and
WAL compaction synchronously on the Tokio worker polling that future. Large
retained-history artifacts could therefore delay unrelated executor work.

The original impact statement conflated that API with the production periodic
checkpoint publisher. Production uses an explicit capture/persist boundary and
performs artifact persistence on dedicated process checkpoint threads. The
unsafe convenience path remained reachable by direct and test callers, and
its contract made future use from async code hazardous, but it was not an
unauthenticated remote checkpoint primitive. The attack vector is therefore
internal and operational-load-dependent, with low availability severity.

The later audit also found CPU-side variants of the same executor-isolation
problem: snapshot serialization, decode/install, state application, cache
refresh, and final destruction of large reference-counted generations. Those
were not part of the original fsync trace, but addressing them was necessary
to close the architectural issue rather than only its first reported sink.

## Resolution

Fixed in
[`a9365a623e4b6c3a360bb41524547acac6fd5cae`](https://github.com/justincormack/argmin/commit/a9365a623e4b6c3a360bb41524547acac6fd5cae)
(`Isolate Raft WAL durability from async execution`) and completed in
[`52d4d7c5d980d11e9dd0b8bc2446e8cb707ba2ca`](https://github.com/justincormack/argmin/commit/52d4d7c5d980d11e9dd0b8bc2446e8cb707ba2ca)
(`Isolate OpenRaft state machine work from async runtime`).

The async convenience method now captures its checkpoint, moves the owned
checkpoint and publication context into `tokio::task::spawn_blocking`, and
runs artifact encoding, file/directory sync, publication, and WAL compaction
there. The synchronous capture/persist API remains available for the
production checkpoint thread, avoiding nested executor assumptions while
preserving the durability and compaction ordering contract.

The follow-up commit captures immutable reference-counted state views cheaply
on the async side and performs snapshot construction, state-machine apply,
snapshot decode/install, and restart-cache refresh in the blocking pool.
Publication atomically swaps generations, then retires displaced state,
membership, and cached payload owners through a blocking-pool destructor so a
final `Arc` release cannot recursively free multi-megabyte state on Tokio.

The deterministic coverage includes
`experimental_raft_captured_checkpoint_persists_outside_state_machine_boundary`,
the single-worker apply/build/install executor tests, and the apply/install
retirement-phase tests. The fixing slice passed the complete control-plane
release gate, Clippy across all targets and features, and the workspace nextest
suite (7,587 tests, excluding one separately known S3 test under active
repair).
