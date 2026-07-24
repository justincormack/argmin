---
id: PTREXPOSE-002
bug_class: pointer-exposure
title: Raw Arc pointer address embedded in shard-repair claim/owner tokens sent over storage RPC
location: crates/server-core/src/coordinator/runtime.rs:1567
function: ShardRepairSweeper::spawn
confidence: High
worker: worker-22
fp_verdict: OUT_OF_SCOPE
fp_rationale: "Confirmed Arc::as_ptr()-derived registry key embedded in shard-repair claim_id/owner_token, but it only crosses the authenticated intra-cluster storage RPC transport and the remote node's metadata store -- never observable by an external S3 client -- so exploiting it requires already being (or compromising) an intra-cluster peer, which is outside the REMOTE threat model, same reasoning as PTREXPOSE-001."
status: fixed
---

## Description
`process_local_registry_key()` returns a raw pointer address
(`Arc::as_ptr(&self.node) as usize` at `crates/storage/src/node.rs:471-473`,
and equivalently `Arc::as_ptr(&self.local_map) as usize` at
`crates/storage/src/cluster/request_ops.rs:1332-1334`). Unlike the
`test_hook_scope_id` / `test_metadata_command_pg_lock_ptr` helpers in the same
crates, this function is **not** gated behind `#[cfg(test)]` /
`feature = "test-hooks"` — it is a normal, always-compiled accessor
(`crates/storage/src/cluster.rs:6766-6768`, `crates/storage/src/node.rs:471-473`).

In the background shard-repair worker, the raw address (`registry_key`) is
formatted directly into an `owner_token` and a `claim_id` string:

```rust
// crates/server-core/src/coordinator/runtime.rs:1484
let owner_token = format!("shard-repair-worker-{}", registry_key);
...
// crates/server-core/src/coordinator/runtime.rs:1567-1573
let claim_id = format!(
    "shard-repair-{}-{}-{}-{}",
    registry_key,
    work_item.request.data_pg_id,
    work_item.shard_index.get(),
    SHARD_REPAIR_CLAIM_COUNTER.fetch_add(1, Ordering::Relaxed)
);
```

Both strings are sent to a storage node over the storage RPC transport as part
of `acquire_placed_segment_shard_repair_claim`
(`crates/storage/src/cluster.rs:11918-11933` →
`crates/storage/src/node_client/unix_rpc.rs:456-471`, which encodes
`claim_id`/`owner_token` into `StorageRpcPlacedSegmentShardRepairClaimAcquireRequest`
and writes it to the wire), and is persisted by the receiving node's metadata
store. This leaks the coordinator process's heap address across the
intra-cluster RPC boundary, defeating ASLR for that process.

## Code
```rust
// crates/storage/src/node.rs:471-473 (source of the raw address; not test-gated)
pub(crate) fn process_local_registry_key(&self) -> usize {
    Arc::as_ptr(&self.node) as usize
}
```
```rust
// crates/server-core/src/coordinator/runtime.rs:1567-1580 (sink construction)
let claim_id = format!(
    "shard-repair-{}-{}-{}-{}",
    registry_key,
    work_item.request.data_pg_id,
    work_item.shard_index.get(),
    SHARD_REPAIR_CLAIM_COUNTER.fetch_add(1, Ordering::Relaxed)
);
let claim_acquire = PlacedSegmentShardRepairClaimAcquireParams {
    claim_id,
    owner_token: owner_token.clone(),
    claimed_at: now_ms,
    lease_deadline: now_ms.saturating_add(SHARD_REPAIR_CLAIM_LEASE_MILLIS),
    now: now_ms,
};
let claim = storage_cluster.acquire_placed_segment_shard_repair_claim(
    work_item.request.data_pg_id,
    &claim_acquire,
);
```

## Data flow
- **Source:** `Arc::as_ptr(&self.node) as usize` in
  `SharedStorageNode::process_local_registry_key`
  (`crates/storage/src/node.rs:471-473`), reached via
  `StorageCluster::process_local_registry_key`
  (`crates/storage/src/cluster.rs:6766-6768`).
- **Sink:** `claim_id` / `owner_token` strings built in
  `ShardRepairSweeper::spawn` (`crates/server-core/src/coordinator/runtime.rs:1484,1567`),
  passed to `StorageCluster::acquire_placed_segment_shard_repair_claim`
  (`crates/storage/src/cluster.rs:11918`) → `unix_rpc.rs:456`
  `encode_placed_segment_shard_repair_claim_acquire_request` → wire → remote/peer
  storage node's metadata store.
- **Validation:** none — `claim_id`/`owner_token` are opaque identity/ownership
  strings compared by equality (`validate_placed_segment_shard_repair_claim_identity`
  in `crates/storage/src/pg_store/scavenger.rs:611-614`); nothing strips the
  embedded address before it crosses the RPC boundary.

## Reachability trace
Background shard-repair thread (`ShardRepairSweeper::spawn`, started from
`ShardRepairSweeper::acquire_shared`, driven continuously while the server is
up and any placement group has durable-repair work) →
`storage_cluster.acquire_placed_segment_shard_repair_claim(...)`
(`crates/storage/src/cluster.rs:11918`) →
`node_client::unix_rpc::acquire_placed_segment_shard_repair_claim`
(`crates/storage/src/node_client/unix_rpc.rs:456`) → wire → storage-node RPC
handler → `pg_store::scavenger::acquire_placed_segment_shard_repair_claim`
(`crates/storage/src/pg_store/scavenger.rs:607`) → persisted claim record.

## Impact
Every shard-repair claim cycle leaks the coordinator process's `Arc` heap
address to whatever node holds the metadata-PG primary for the affected
placement group. In the project's stated in-progress multi-host /
storage-node split, that primary can be a distinct host/process from the
coordinator; a peer that is compromised (or simply logging/persisting claim
records) obtains ASLR-defeating layout information for the coordinator
process, which combined with any other memory-corruption bug materially eases
remote exploitation.

## Mitigations checked
- `process_local_registry_key` has no `#[cfg(debug_assertions)]` /
  `feature = "test-hooks"` gate (contrast with the clearly test-only
  `test_hook_scope_id` / `test_metadata_command_pg_lock_ptr` helpers in the
  same files) — it runs unconditionally in release builds as long as the
  shard-repair background worker is active.
- Storage RPC transport is authenticated/TLS, restricting *who* can observe
  the token, but does not stop the token itself from encoding process layout
  information to a legitimate-but-compromised peer.
- Claim/owner tokens are compared only by string equality; there is no
  server-side stripping, hashing, or opacity transform applied before persistence.

## Recommendation
Do not derive `claim_id`/`owner_token` from a pointer address. Use a
process-scoped random nonce (generated once at startup, e.g. via the same
`ring::rand::SystemRandom` used elsewhere for reservation IDs) or a monotonic
counter combined with `std::process::id()` instead of
`Arc::as_ptr(...) as usize`, so no raw memory address ever leaves the process.

## Resolution

Fixed in [`0646994c9e4f2f816980e5a123443013d5b9e6e2`](https://github.com/justincormack/argmin/commit/0646994c9e4f2f816980e5a123443013d5b9e6e2)
(`Replace pointer-derived RPC identities`) and hardened further in
[`96a7d625106ed52791a4d9bdd0e6bdea5755c7f2`](https://github.com/justincormack/argmin/commit/96a7d625106ed52791a4d9bdd0e6bdea5755c7f2)
(`Replace local pointer registry keys`).

Shard-repair workers now generate an independent cryptographically random
128-bit worker-lifetime identity. Claim IDs and owner tokens contain that
fixed-width opaque identity plus only operation-specific non-address metadata.
The same correction was applied proactively to the equivalent shard-backfill
worker path, which used the same local registry identity pattern. No repair or
backfill claim sent over storage RPC or retained by a storage node now contains
a process pointer.

The follow-up hardening replaced the production `Arc::as_ptr()`-derived
registry key itself with the opaque `ProcessLocalRegistryKey` type. The key is
allocated once per `SharedStorageNode` from a checked process-local sequence,
survives clones and runtime-map refreshes, exposes neither its numeric value nor
a serialization surface, and has a redacted `Debug` implementation. This keeps
the remaining process-local registry use explicit without leaving a pointer
identity available for future token construction.

The deterministic regressions
`background_worker_identity_is_opaque_fixed_width_hex` and
`process_local_registry_keys_are_unique_and_debug_redacted` cover the random
worker identity and the local typed-key boundary. The fixing commits passed the
complete workspace nextest suite (7,551 and 7,552 tests, respectively) and
affected-crate Clippy with warnings denied.
