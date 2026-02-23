# Placement / Topology — API Design & Test Plan

## Scope

This document covers the Rust API for the placement subsystem (step 2 in the build
sequence). The placement layer answers one question: given a key and a cluster topology,
which nodes hold which shards?

Placement is deterministic and stateless with respect to individual objects. Any node
in the system can compute the same answer from the same cluster map, with no per-object
directory. This is the property that makes distributed operation work without central
coordination.

---

## Background & Constraints

### Role in the stack

Step 1 (Erasure Coding Engine) computes shards from bytes. Step 2 (Placement) decides
where those shards live. The two layers are independent: placement knows nothing about
erasure coding internals; the EC engine knows nothing about topology.

### Algorithm: Weighted Rendezvous Hashing (HRW)

We use weighted rendezvous hashing with failure-domain constraints.

Chosen over CRUSH for the following reasons:

| Property | CRUSH | Rendezvous HRW |
|---|---|---|
| Implementation complexity | High (tree traversal, bucket types, backtracking) | Low (score all nodes, sort, greedy accept) |
| Failure domain support | Rich multi-level rules | Rack-cap constraint sufficient for our scale |
| Movement on node add/remove | Minimal | Minimal — O(1/N) expected movement |
| Verifiability | Hard — Ceph-specific | Simple — one formula, one loop |

At our target scale (10–200 storage nodes), O(N) scoring per placement is negligible.

**Scoring formula.** For placement key K and node i with weight w\_i:

```
U_i  = hash(K || node_id_bytes_i) mapped to (0, 1]
score_i = -ln(U_i) / w_i
```

Select the `total_shards` nodes with the **lowest** scores, subject to the rack cap.

This is weighted HRW (Mitzenmacher & Upfal formulation, also called straw2 in Ceph
parlance). Weight proportional assignment follows from the exponential clock argument:
a node with weight w receives a fraction w / sum(w) of all placements in expectation.

**Constraint enforcement.** After scoring, walk nodes in ascending score order and
accept greedily: skip a node if its rack already has `max_shards_per_rack` accepted
shards. This is one linear pass, no backtracking.

### Hash function: rapidhash

`rapidhash` (crate: `rapidhash` by hoxxep) is chosen as the hash function. Properties:

- Stable output across platforms and Rust versions (unlike `std::hash` which is
  SipHash with per-process randomization)
- No SIMD required; fast for small fixed-size inputs
- Directly noted in design notes as preferred option
- Compatible with `portable-hash` stable-hash traits if needed later

**New dependency.** `rapidhash` is not yet in the workspace. This is the only new
crate added by this step.

### Failure domain model

Three-level hierarchy: cluster → racks → machines → disks.

This step enforces the rack level only via `max_shards_per_rack`. Machine-level
constraints (no two shards on the same machine) are handled automatically because
each disk is a distinct node — if the cluster has one disk per machine, machine-level
isolation is automatic. Multi-disk machines are a future extension.

**Default rack cap:** `parity_shards` (i.e. m for a (k, m) scheme). Losing an entire
rack consumes at most m shard positions, leaving k data shards intact — sufficient
for reconstruction.

If the constraint cannot be satisfied (e.g. too few racks), return
`Err(ConstraintUnsatisfiable)`. Silently relaxing is worse than an explicit error.
The caller (upper layer) can retry with `max_shards_per_rack = total_shards` to
disable the constraint for small clusters.

### Placement groups (PG layer)

The placement crate is PG-agnostic. `place(key: &[u8], ...)` works for any key. A
PG layer reduces to:

```rust
let pg_id: u32 = (hash64(object_key) % pg_count) as u32;
placer.place(&pg_id.to_le_bytes(), &mut out)?;
```

That indirection belongs in the layer above, not here.

### Memory policy

Following `guides/rust_memory_policy_strict.md`:

- **ZONE_INIT**: `ClusterMap::new`, `Placer::new` — allocation allowed; build sorted
  node metadata, pre-validate.
- **ZONE_HOT** (`Placer::place`): **zero heap allocation**. Scoring and selection use
  a stack-allocated `[ScoredNode; MAX_NODES]` array. At `MAX_NODES = 256` and 16
  bytes per entry, that is 4 KB stack usage — well within normal limits.

### What is NOT in this API

- No I/O, no async
- No knowledge of erasure coding internals (takes `total_shards`, not k and m)
- No live cluster membership tracking (ClusterMap is an immutable snapshot)
- No per-object state
- No repair or rebalancing decisions
- No machine-level isolation (rack level only for now)

---

## Crate & Module Structure

```
placement/
  src/
    lib.rs          -- public re-exports
    cluster.rs      -- ClusterMap, NodeInfo, NodeId, RackId
    config.rs       -- PlacementConfig, PlacementError
    placer.rs       -- Placer, rendezvous hashing algorithm
    hash.rs         -- hash(key || node_id) -> f64 score helper
```

Single crate: `placement`. No sub-crates needed (pure safe Rust, no FFI).

**Dependencies added by this step:**
- `rapidhash` — hash function
- `thiserror` — already in workspace (used by `ec` crate)

---

## Public API

### `NodeId` and `RackId`

```rust
/// Opaque identifier for a storage node (one disk = one node).
/// Assigned by the caller at ClusterMap construction time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(u32);

impl NodeId {
    pub fn new(id: u32) -> Self;
    pub fn as_u32(self) -> u32;
}

/// Opaque identifier for a rack (failure domain).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RackId(u32);

impl RackId {
    pub fn new(id: u32) -> Self;
    pub fn as_u32(self) -> u32;
}
```

IDs are caller-assigned opaque integers. The placement crate does not allocate them.

---

### `NodeInfo`

```rust
/// Description of one storage node, supplied at ClusterMap construction.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Application-assigned node ID. Must be unique within the map.
    pub id: NodeId,
    /// Failure domain: which rack this node belongs to.
    pub rack: RackId,
    /// Relative capacity weight. Proportional to storage capacity.
    /// Nodes with weight 0.0 are excluded from placement (treated as down).
    /// Must be finite and non-negative.
    pub weight: f64,
}
```

---

### `ClusterMap`

```rust
/// Immutable snapshot of the cluster topology.
///
/// Constructed once (ZONE_INIT) when cluster membership changes.
/// Cheap to clone (Arc-wrapped internals). Send + Sync.
pub struct ClusterMap { /* opaque */ }

impl ClusterMap {
    /// Construct from a slice of node descriptions.
    ///
    /// Returns Err if:
    /// - `nodes` is empty
    /// - any `weight` is negative or NaN or infinite
    /// - any NodeId is duplicated
    /// - `nodes.len() > MAX_NODES`
    ///
    /// ZONE_INIT: allocates.
    pub fn new(nodes: &[NodeInfo]) -> Result<Self, PlacementError>;

    /// Number of nodes with weight > 0.0.
    pub fn active_node_count(&self) -> usize;

    /// Total number of distinct rack IDs among active nodes.
    pub fn rack_count(&self) -> usize;

    /// Sum of weights across all active nodes.
    pub fn total_weight(&self) -> f64;
}

/// Hard upper bound on cluster size.
pub const MAX_NODES: usize = 256;
```

---

### `PlacementConfig`

```rust
/// Parameters for one placement scheme. Cheap to copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacementConfig {
    /// Total shards to place per stripe (k + m in EC terms).
    pub total_shards: u8,
    /// Maximum shards allowed in any one rack.
    /// Set to `total_shards` to disable the rack constraint.
    /// Default for a (k, m) EC scheme: m (parity count).
    pub max_shards_per_rack: u8,
}

impl PlacementConfig {
    pub fn new(total_shards: u8, max_shards_per_rack: u8) -> Result<Self, PlacementError>;
}
```

Validation in `new`:
- `total_shards >= 1`
- `max_shards_per_rack >= 1`
- `max_shards_per_rack <= total_shards`

---

### `Placer`

```rust
/// Stateless placement engine.
///
/// Constructed once at ZONE_INIT. `place()` is the hot path.
/// Send + Sync: place() takes &self; all mutable state is stack-local.
pub struct Placer { /* opaque */ }

impl Placer {
    /// Construct from a config and a cluster map.
    ///
    /// Returns Err(TooFewNodes) if active_node_count < total_shards
    /// (more shards than available nodes — physically impossible).
    ///
    /// ZONE_INIT: allocates.
    pub fn new(config: PlacementConfig, map: &ClusterMap) -> Result<Self, PlacementError>;

    pub fn config(&self) -> PlacementConfig;

    /// Deterministically assign total_shards nodes to shards for the given key.
    ///
    /// `key`:  arbitrary bytes — object key, PG id, etc. May be empty.
    /// `out`:  caller-allocated; length must equal config.total_shards.
    ///         On success, out[i] is the NodeId for shard i.
    ///         Shard index semantics (data vs parity) are the caller's concern.
    ///
    /// Returns Err(ConstraintUnsatisfiable) if the rack constraint cannot be met
    /// (cluster topology does not have enough independent racks). In that case
    /// `out` is not modified.
    ///
    /// Returns Err(OutputLengthMismatch) if out.len() != total_shards.
    ///
    /// ZONE_HOT: no heap allocation.
    pub fn place(&self, key: &[u8], out: &mut [NodeId]) -> Result<(), PlacementError>;
}
```

**Shard index convention** (matching the EC crate): `0..k` = data shards,
`k..k+m` = parity shards. The placement crate does not distinguish them; this is
a caller-level convention.

---

### `PlacementError`

```rust
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum PlacementError {
    #[error("empty cluster: no nodes provided")]
    EmptyCluster,

    #[error("node count {count} exceeds MAX_NODES ({max})")]
    TooManyNodes { count: usize, max: usize },

    #[error("duplicate node id {id}")]
    DuplicateNodeId { id: u32 },

    #[error("invalid weight {weight} for node {id}: must be finite and non-negative")]
    InvalidWeight { id: u32, weight: f64 },

    #[error("total_shards must be >= 1")]
    InvalidTotalShards,

    #[error("max_shards_per_rack must be >= 1 and <= total_shards")]
    InvalidMaxShardsPerRack,

    #[error("cannot place {shards} shards: only {nodes} active nodes available")]
    TooFewNodes { shards: usize, nodes: usize },

    #[error("rack constraint unsatisfiable: {shards} shards, max {max_per_rack} per rack, \
             only {rack_slots} slots across {racks} racks")]
    ConstraintUnsatisfiable {
        shards: usize,
        max_per_rack: usize,
        rack_slots: usize,
        racks: usize,
    },

    #[error("output slice length {got} != total_shards {expected}")]
    OutputLengthMismatch { got: usize, expected: usize },
}
```

No `String` fields — all variants carry only fixed-size data, complying with the
hot-path allocation policy.

---

## Scoring Implementation Detail

For node i, given input key `K`:

```
hash_input  = K ++ node_id.as_u32().to_le_bytes()   (concatenated)
h: u64      = rapidhash(hash_input)
U_i: f64    = ((h >> 11) + 1) as f64 / (1u64 << 53) as f64   // maps to (0, 1]
score_i     = -f64::ln(U_i) / weight_i
```

The `>> 11` and `+1` gives a 53-bit uniform value in `[1, 2^53]` mapped to `(0, 1]`,
matching the precision of `f64` mantissa and avoiding `ln(0) = -∞`.

Nodes with `weight = 0.0` are skipped before scoring (treated as absent).

Selection: fill `[ScoredNode; MAX_NODES]` on the stack, partial-sort to find the
top-`total_shards` entries with the rack cap applied greedily. Partial sort uses
an insertion-sort style scan (O(N * total_shards)), acceptable since both N ≤ 256
and total_shards ≤ 32.

---

## Resolved Design Decisions

### 1. Rendezvous HRW vs CRUSH

**Rendezvous HRW.** CRUSH is well-engineered for Ceph's scale and operational model,
but carries significant complexity (bucket types, rule language, backtracking on
failure). At our target scale the simpler algorithm is easier to verify, audit, and
reason about. The data distribution quality is equivalent for uniform or near-uniform
weights.

### 2. PG layer not in this crate

PG virtualization is a two-line transformation the caller applies to its key before
calling `place`. Embedding it here would add state (pg_count, pg map) with no benefit
until the metadata cluster layer is built.

### 3. Stack allocation bounded by MAX_NODES

`MAX_NODES = 256` keeps the stack frame at ~4 KB. This is simpler than requiring
caller-provided scratch (no `place_scratch_size` helper needed). The EC crate uses
the same pattern for matrix inversion (1024-byte stack array for 32×32 matrix). If
clusters ever exceed 256 nodes, we add a scratch buffer API at that point.

### 4. `place()` takes `&self`

All mutable state is stack-local. The `Placer` is safe to share across concurrent
request-handling threads without any locking.

### 5. Floating-point scoring determinism

`f64::ln` is correctly rounded per IEEE 754 on all supported targets (x86-64, ARM64).
Cross-architecture bit-identical results are not required for correctness — only that
a single machine consistently places the same key the same way. Two nodes with the
same cluster map will produce the same placement independently.

### 6. Error on unsatisfiable constraint

Return `Err(ConstraintUnsatisfiable)` rather than silently relaxing. The caller
(the operator configuration layer) handles the fallback — it knows whether it is
configuring an intentionally small cluster where constraints should be loosened.

### 7. Weights represent capacity, not IOPS

Weight drives long-run shard count proportional to disk capacity. Heterogeneous
disk sizes (e.g. mixing 4 TB and 8 TB nodes) are handled by setting weights 1.0 and
2.0 respectively. IOPS weighting is a separate concern for the load balancer, not
placement.

---

## Test Plan

### 1. Configuration validation

| Test | Expected |
|---|---|
| `PlacementConfig::new(6, 3)` | Ok |
| `total_shards = 0` | `InvalidTotalShards` |
| `max_shards_per_rack = 0` | `InvalidMaxShardsPerRack` |
| `max_shards_per_rack > total_shards` | `InvalidMaxShardsPerRack` |
| `ClusterMap::new([])` | `EmptyCluster` |
| `ClusterMap::new` with > MAX_NODES entries | `TooManyNodes` |
| Duplicate NodeId | `DuplicateNodeId` |
| Negative weight | `InvalidWeight` |
| NaN weight | `InvalidWeight` |
| Infinite weight | `InvalidWeight` |
| `active_node_count < total_shards` | `TooFewNodes` |

### 2. Output length mismatch

- `out.len() < total_shards` → `OutputLengthMismatch`
- `out.len() > total_shards` → `OutputLengthMismatch`

### 3. Determinism

Same cluster map + same key → same output, every call:
- Call `place` 100 times with the same key; assert all results equal.
- Construct a second `Placer` from identical inputs; assert outputs match.
- Rotate key bytes and assert output differs (hash sensitivity).

### 4. No duplicates

For any valid cluster and any key: `out` contains no repeated `NodeId`.
Test with `total_shards = node_count` (maximum assignment) to stress this.

### 5. Rack constraint respected

For a cluster with R racks and `max_shards_per_rack = c`:
- Count shards per rack in the output; assert each rack count ≤ c.
- Test with default rack cap (c = parity count) for standard (4,2) config.

### 6. ConstraintUnsatisfiable — correct detection

| Scenario | Expected |
|---|---|
| 1 rack, total_shards=6, max_per_rack=2 | `ConstraintUnsatisfiable` |
| 3 racks × 2 nodes, total_shards=6, max_per_rack=2 | Ok |
| All nodes weight 0.0 | `TooFewNodes` |

### 7. Even distribution (statistical)

Place 10,000 random keys on a balanced cluster (equal weights, 12 nodes across 3
racks, total_shards=6). For each node, count total shard assignments. Assert all
counts are within 20% of `10000 * 6 / 12 = 5000`. Validates hash quality.

### 8. Weighted distribution (statistical)

Cluster: one node with weight 2.0, four nodes with weight 1.0, total_shards=3.
Place 10,000 keys. Assert the heavy node receives approximately 2× as many shard
assignments as each light node (within 20% tolerance).

### 9. Minimal movement on node removal

- Place 10,000 keys on a cluster of N=12 nodes (equal weights).
- Remove one node (set weight to 0.0 or exclude from new ClusterMap).
- Re-place all 10,000 keys on the smaller cluster.
- Assert fraction of changed shard assignments ≤ 1/(N-1) + 0.05 epsilon.
  (Rendezvous guarantees ~1/N expected movement; only the removed node's
  assignments should move, to other nodes, not each other.)

### 10. Node addition — monotonicity

- Start with N nodes. Place 10,000 keys.
- Add a node (new ClusterMap).
- Re-place all keys.
- Assert: assignments that changed moved TO the new node. No two existing
  nodes swapped assignments with each other.

### 11. Minimum viable cluster

- 1 node, total_shards=1, max_per_rack=1 → Ok
- 1 node, total_shards=2 → `TooFewNodes`
- 6 nodes each on its own rack, total_shards=6, max_per_rack=1 → Ok
- 6 nodes, 2 racks of 3, total_shards=6, max_per_rack=3 → Ok
- 6 nodes, 2 racks of 3, total_shards=6, max_per_rack=2 → `ConstraintUnsatisfiable`

### 12. All nodes in one rack, constraint disabled

- `max_shards_per_rack = total_shards` → succeeds regardless of rack topology.

### 13. Weight-zero nodes excluded

- Cluster of 8 nodes; 3 have weight 0.0. total_shards=4.
- `active_node_count = 5`. Place succeeds; output never contains a zero-weight NodeId.

### 14. Hot path allocation (ZONE_HOT compliance)

Using the thread-local counting allocator pattern from the EC crate:
- `place()` on a pre-built `Placer`: **zero heap allocations**.

### 15. Property-based tests (proptest)

```
forall (
    node_count in 1..=64usize,
    rack_count  in 1..=node_count,
    total_shards in 1..=min(node_count, 32) as u8,
    key: Vec<u8>
):
    cluster = build_cluster(node_count, rack_count, equal_weights)
    max_per_rack = ceil(total_shards as f64 / rack_count as f64) as u8
    config = PlacementConfig::new(total_shards, max_per_rack)?
    placer = Placer::new(config, &cluster)?
    out = [NodeId(0); total_shards as usize]
    placer.place(&key, &mut out)?

    prop_assert!(no duplicates in out)
    prop_assert!(rack counts all <= max_per_rack)
```

Shrinking on failure → minimal (node_count, rack_count, total_shards, key)
counterexample.

### 16. Fuzz targets (deferred)

`fuzz_place`: arbitrary (cluster bytes, key bytes, config) — must not panic.
Register as `cargo-fuzz` target when proptest suite is stable.

### 17. Benchmarks (deferred)

Measure `place()` throughput (placements/second) for:

| Cluster size | total_shards | Notes |
|---|---|---|
| 12 nodes, 3 racks | 6 | default target config |
| 50 nodes, 5 racks | 8 | medium cluster |
| 256 nodes, 16 racks | 16 | MAX_NODES stress |

Track zero-allocation assertion and peak stack depth per CI run.
