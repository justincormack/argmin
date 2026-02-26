## Fault Injection Framework (Shard-Only, Test-Only)

### Scope
- Fault injection applies only to shard data operations.
- Metadata operations are out of scope for the initial framework.
- Implementation is test-only (`#[cfg(test)]`), not shipped in production.

### Goals
- Deterministic, scriptable faults for targeted tests.
- Support missing data, incorrect data, and failed writes.
- Minimal changes to production code paths.
- Enable EC-layer tests for missing/corrupt shard behaviors.

### API Sketch

**Ops**
- `ShardFaultOp::Read`
- `ShardFaultOp::Write`
- `ShardFaultOp::Stat`
- `ShardFaultOp::Delete`

**Actions**
- `Pass`
- `ReturnNotFound`
- `ReturnIoError(String)`
- `CorruptData { flip_count: u8, seed: u64 }` (read only — see Corruption Semantics below)
- `ReturnIntegrityError` (read only — simulates CRC mismatch without mutating stored data)
- `DropWrite` (ack success, discard data)

**Policy**
```
trait ShardFaultPolicy: Send + Sync {
    fn on_op(
        &self,
        op: ShardFaultOp,
        key: &ShardKey,
        attempt: u32,
        len: Option<usize>,
    ) -> ShardFaultAction;
}
```

### Corruption Semantics

`CorruptData` must mutate the stored data in the inner store (not the
returned bytes) so the next `read_shard` call goes through the real CRC64
verification path and returns `IntegrityError`. This tests the actual
production code path for bit-rot detection.

If the corruption is applied *after* `read_shard` returns verified data,
it simulates an unrealistic scenario (corruption after verification) and
bypasses CRC checking entirely. That's the wrong layer.

Concretely, `CorruptData` should:
1. Let the inner `write_shard` succeed normally.
2. Mutate the stored bytes (e.g. via direct access to `MemoryPgStore` internals).
3. Subsequent `read_shard` genuinely fails CRC — no special handling needed.

For cases where you just want "CRC mismatch happened" without bothering to
set up actual byte-level corruption, `ReturnIntegrityError` directly returns
`StoreError::IntegrityError` with synthetic CRC values. Simpler for tests
that don't care about the corruption details.

### Integration with Coordinator Tests

The coordinator calls `self.storage_node.get_pg(pg_id)` which returns
`&PgStore` — a concrete type that implements both `ShardStore` and
`PgMetadataStore`. The interesting EC recovery tests live at the coordinator
level (put object, corrupt/drop shards, verify get still works).

To support this, the faulty wrapper must implement **both** `ShardStore`
and `PgMetadataStore`:
- `ShardStore` methods are intercepted by the fault policy.
- `PgMetadataStore` methods pass through to the inner store unchanged.

This avoids any changes to production traits or the coordinator itself.
The test setup creates a `FaultyPgStore` wrapping a `MemoryPgStore` (or
`PgStore`), injects it into a test-only coordinator constructor, and
scripts faults on individual shards.

### Rule Matching: Shard Index

For EC recovery tests, the natural matching dimension is **shard index**
(the last byte of `ShardKey`), not the full key. Typical rules:
- "Drop writes to shard index 2" — tests single-shard loss recovery
- "Corrupt shard index 0 on read" — tests data shard corruption
- "Return NotFound for shard indices 3, 4, 5" — tests exceeding m threshold

`ScriptedPolicy` rules should support matching on:
- `op` (Read/Write/Stat/Delete, or wildcard)
- `shard_index` (extracted from last byte of ShardKey, or wildcard)
- `attempt` (1-based counter per (op, key), or wildcard)

Full `ShardKey` matching is available but rarely needed — shard index
covers the EC test cases.

### Attempt Counters

Per `(op, key)` — not per-op-only. This allows transient fault scenarios
like "fail the 1st read of shard 2 but succeed on retry" without
affecting reads of other shards.

### Implementation Plan
1. Add a test-only module in `crates/storage` (e.g. `src/tests/faults.rs` or `src/test_util/faults.rs`).
2. Implement `FaultyPgStore<T: ShardStore + PgMetadataStore>` wrapper:
   - Holds `inner: T` and `policy: Arc<dyn ShardFaultPolicy>`.
   - Implements `ShardStore` with fault interception.
   - Implements `PgMetadataStore` as passthrough to `inner`.
   - Tracks per-(op,key) attempt counts.
3. Implement `ScriptedPolicy`:
   - Ordered list of rules matching `(op, shard_index, attempt)` with wildcards.
   - First matching rule wins; no match defaults to `Pass`.
   - Deterministic and readable in tests.
4. Add minimal EC-layer tests using `FaultyPgStore`:
   - Missing one data shard => reconstruct succeeds (<= m missing).
   - Corrupt shard => detect via CRC, reconstruct succeeds.
   - Missing > m shards => reconstruct fails.
   - Drop parity shard => data read still works (only data shards needed).
5. Keep hooks small and contained; no changes to production store traits.

### Test Plan
- Unit tests for `FaultyPgStore`:
  - Read fault returns NotFound/IoError.
  - CorruptData mutates stored bytes, subsequent read returns IntegrityError.
  - ReturnIntegrityError returns synthetic CRC mismatch directly.
  - DropWrite makes subsequent read NotFound.
- Coordinator-level EC tests:
  - put_object, drop 1 shard, get_object succeeds (EC reconstruction).
  - put_object, drop m shards, get_object succeeds (at the limit).
  - put_object, drop m+1 shards, get_object fails.
  - put_object, corrupt 1 data shard, get_object succeeds (CRC detects, EC reconstructs).
  - Range GET with missing shard in the requested range.

### Deferred
- `ShortRead` — CRC check catches truncated data anyway. Not needed initially.
- Metadata fault injection — separate concern, different failure modes.
- Latency injection — useful later for timeout testing but not for correctness.
