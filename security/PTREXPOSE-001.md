---
id: PTREXPOSE-001
bug_class: pointer-exposure
title: Raw Arc pointer address leaked into bucket-write owner token sent over storage RPC and persisted to metadata DB
location: crates/storage/src/cluster.rs:7292
function: bucket_write_owner_token
confidence: High
worker: worker-22
fp_verdict: OUT_OF_SCOPE
fp_rationale: "Confirmed Arc::as_ptr() pointer leak into bucket_write_owner_token, but verified (rg over server-http/server-core) it is never echoed back to the external S3 client -- it only crosses the authenticated intra-cluster storage-RPC transport and the per-shard SQLite metadata store, so exploiting the leak requires already being (or compromising) a cluster peer or having direct DB/filesystem access, which is outside the REMOTE (network-only, no local shell) threat model."
status: fixed
---

## Description
`bucket_write_owner_token` builds a "who owns this in-flight bucket write"
identity token by formatting the raw memory address of `self.local_map`
(an `Arc<LocalNodeMap>`) with `{:p}`:

```rust
fn bucket_write_owner_token(&self) -> String {
    format!(
        "process:{}:cluster:{:p}",
        std::process::id(),
        Arc::as_ptr(&self.local_map)
    )
}
```

This token is not an opaque, in-process-only key: it is threaded through
bucket-write reservation/drain/finalize/reclaim RPCs
(`crates/storage/src/cluster/request_ops.rs:3207,3241,3274,3505,6465,7630,10733`)
and sent to the primary metadata node over the authenticated storage RPC
transport (`crates/storage/src/node_client/unix_object_rpc.rs`), and it is
also persisted verbatim as `bucket_write_owner_token TEXT` in the per-shard
SQLite `stream_uploads` table (`crates/storage/src/schema.rs:294`,
`crates/storage/src/pg_store/metadata.rs:2293`). Any party that can read
that column (e.g. an operator dumping the DB, a compromised co-located
storage node, or a future debug/inspection route that surfaces reservation
rows) recovers the exact heap address of the coordinator process's
`local_map` `Arc`, which defeats ASLR for that process and hands an
attacker who already has another memory-corruption primitive a working
base address to build a further exploit chain against.

## Code
```rust
fn bucket_write_owner_token(&self) -> String {
    format!(
        "process:{}:cluster:{:p}",
        std::process::id(),
        Arc::as_ptr(&self.local_map)
    )
}
```
Call site (one of several) that ships the token off-process:
```rust
// crates/storage/src/cluster/request_ops.rs:3206-3210
let reservation_id = self.next_bucket_write_reservation_id()?;
let owner_token = self.bucket_write_owner_token();
let record = node
    .bucket_write_reservation_client()
    .acquire_durable_bucket_write_reservation(
        ...
        owner_token: &owner_token,
        ...
```

## Data flow
- **Source:** `Arc::as_ptr(&self.local_map)` — heap address of the coordinator's
  local node-map `Arc`, in `bucket_write_owner_token` (`crates/storage/src/cluster.rs:7290-7296`).
- **Sink 1 (network):** encoded into `acquire_durable_bucket_write_reservation`
  / `acquire_completion_durable_bucket_write_reservation` / heartbeat/drain/finalize/reclaim
  RPC requests and sent to the metadata-PG primary node over
  `crates/storage/src/node_client/unix_object_rpc.rs` (Unix-socket storage RPC).
- **Sink 2 (durable storage):** written verbatim into the `stream_uploads.bucket_write_owner_token`
  column of the per-placement-group SQLite database
  (`crates/storage/src/pg_store/metadata.rs:2293`, schema at `crates/storage/src/schema.rs:294`).
- **Validation:** none — the value is used only as an opaque comparison string
  (`existing.owner_token == acquire.owner_token`), never range- or format-checked,
  and nothing strips or hashes the embedded pointer before it leaves the process.

## Reachability trace
`S3 PutObject / multipart / bucket-delete coordination path` (server-core coordinator)
→ `StorageCluster::{begin_bucket_write_reservation, drain, finalize, reclaim}` in
`crates/storage/src/cluster/request_ops.rs` → `self.bucket_write_owner_token()`
(`crates/storage/src/cluster.rs:7290`) → RPC request struct →
`crates/storage/src/node_client/unix_object_rpc.rs` (wire) → primary metadata node →
`crates/storage/src/pg_store/metadata.rs` `INSERT INTO stream_uploads (... bucket_write_owner_token ...)`.

## Impact
Leaks the coordinator process's heap base/layout information to any peer on the
intra-cluster storage-RPC channel and to anything that can read the persisted
SQLite metadata (backups, another compromised node in the in-progress
multi-host storage-node split, a future inspection tool). This defeats ASLR for
the coordinator process and materially eases exploitation of any co-present
memory-corruption bug on that node.

## Mitigations checked
- No `#[cfg(debug_assertions)]` or feature gate — this runs in every build,
  including release, on every bucket-write reservation/drain/finalize/reclaim.
- Not limited to test/test-hooks builds (unlike the `test_hook_scope_id`- and
  `process_local_registry_key`-style helpers elsewhere in the crate, which are
  gated behind `#[cfg(any(test, feature = "test-hooks"))]`).
- The storage RPC transport is authenticated/TLS per recent hardening, but that
  only restricts *who* can see the token, not whether the token itself should
  contain a raw address — a legitimate but compromised or malicious peer node
  still recovers ASLR-defeating information.

## Recommendation
Replace the `{:p}` address with a stable, opaque owner identifier that does not
derive from process memory layout, e.g. a random UUID/nonce generated once per
process start (or reuse the existing `ring::rand`-backed reservation-id
generator already used a few lines above for `next_bucket_write_reservation_id`),
combined with `std::process::id()` if process-scoping is still needed. Never
format a pointer value into a string that crosses the RPC boundary or reaches
persistent storage.

## Resolution

Fixed in [`0646994c9e4f2f816980e5a123443013d5b9e6e2`](https://github.com/justincormack/argmin/commit/0646994c9e4f2f816980e5a123443013d5b9e6e2)
(`Replace pointer-derived RPC identities`) and hardened further in
[`96a7d625106ed52791a4d9bdd0e6bdea5755c7f2`](https://github.com/justincormack/argmin/commit/96a7d625106ed52791a4d9bdd0e6bdea5755c7f2)
(`Replace local pointer registry keys`).

`StorageCluster` now generates a cryptographically random 128-bit owner
identity once and formats bucket-write ownership as a fixed-width opaque token.
The identity is retained across content-changing runtime-map refreshes, so
existing reservations keep a stable owner without embedding a process ID or
memory address in RPC frames or SQLite metadata. Random-generation failure is
reported explicitly rather than falling back to a predictable identity.

The follow-up hardening removed the underlying production
`Arc::as_ptr()`-derived registry identity as well. Each `SharedStorageNode` now
receives a checked, process-local `ProcessLocalRegistryKey` allocated once and
preserved by clones and refreshed cluster maps. Its numeric value is private,
its `Debug` representation is redacted, and it has no display or serialization
surface, preventing accidental reuse as an off-process token.

The deterministic regressions `bucket_write_owner_token_is_opaque` and
`process_local_registry_keys_are_unique_and_debug_redacted` prove that the
bucket owner token is fixed-width lowercase hexadecimal with no pointer value,
and that local registry keys are unique and debug-redacted. The fixing commits
passed the complete workspace nextest suite (7,551 and 7,552 tests,
respectively) and affected-crate Clippy with warnings denied.
