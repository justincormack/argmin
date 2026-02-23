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

We use weighted rendezvous hashing with pluggable placement constraints.

Chosen over CRUSH for the following reasons:

| Property | CRUSH | Rendezvous HRW |
|---|---|---|
| Implementation complexity | High (tree traversal, bucket types, backtracking) | Low (score all nodes, streaming selection) |
| Failure domain support | Rich multi-level rules | Pluggable constraint function |
| Movement on node add/remove | Minimal | Minimal — O(1/N) expected movement |
| Verifiability | Hard — Ceph-specific | Simple — one formula, one loop |

**Scoring formula.** For placement key K and node i with weight w\_i:

```
U_i  = hash(K || node_id_bytes_i) mapped to (0, 1]
score_i = -ln(U_i) / w_i
```

Select the `total_shards` nodes with the **lowest** scores, subject to the constraint.

This is weighted HRW (Mitzenmacher & Upfal formulation). Weight proportional assignment
follows from the exponential clock argument: a node with weight w receives a fraction
w / sum(w) of all placements in expectation.

**Constraint enforcement** is pluggable. The algorithm calls the constraint function
once per candidate node per `place()` invocation. See `PlacementConstraint` below.

### Hash function: rapidhash

`rapidhash` (crate: `rapidhash` by hoxxep) is chosen. Properties:

- Stable output across platforms and Rust versions (unlike `std::hash`)
- No SIMD required; fast for small fixed-size inputs
- Directly noted in design notes as preferred option

**New dependency.** `rapidhash` is not yet in the workspace.

### Topology model

Node topology is an ordered list of `(Level, u32)` segments describing the node's
position in the physical hierarchy. This is not a hardcoded `rack: RackId` field — it
is a `TopologyKey` that can represent any depth of hierarchy:

```
(Zone, 1), (Rack, 3)
(Zone, 1), (Rack, 3), (Machine, 7)
(Datacenter, 2), (Zone, 1), (Rack, 3), (Machine, 7), (Disk, 0)
```

The placement constraint operates on `TopologyKey` by extracting whichever `Level`
it cares about. Nothing in the core algorithm has knowledge of specific levels.

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

- **ZONE_INIT**: `ClusterMap::new`, `Placer::new` — allocation allowed.
- **ZONE_HOT** (`Placer::place`): **zero heap allocation**. Two stack-allocated arrays
  bounded by `total_shards` (≤ 32): a candidate buffer (~768 bytes) and a group-count
  table (~256 bytes). `TopologyKey` is inline (SmallVec inline storage) for paths ≤ 4
  levels, so comparing and reading `TopologyKey` in the hot path does not allocate.
  The `admit` closure must not allocate.

### What is NOT in this API

- No I/O, no async
- No knowledge of erasure coding internals (takes `total_shards`, not k and m)
- No live cluster membership tracking (ClusterMap is an immutable snapshot)
- No hardcoded topology model — all topology structure lives in `TopologyKey` and the
  constraint

---

## Crate & Module Structure

```
placement/
  src/
    lib.rs          -- public re-exports
    topology.rs     -- Level, TopologyKey
    cluster.rs      -- ClusterMap, NodeInfo, NodeId
    config.rs       -- PlacementConfig, PlacementError
    constraint.rs   -- Admission, PlacementConstraint, built-in constructors
    placer.rs       -- Placer, rendezvous hashing algorithm
    hash.rs         -- hash(key || node_id) -> f64 score helper
```

Single crate: `placement`. No sub-crates needed (pure safe Rust, no FFI).

**Dependencies added by this step:**
- `rapidhash` — hash function
- `smallvec` — inline storage for `TopologyKey` segments
- `thiserror` — already in workspace (used by `ec` crate)

---

## Public API

### `NodeId`

```rust
/// Opaque identifier for a storage node (one disk = one node).
/// Assigned by the caller at ClusterMap construction time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(u32);

impl NodeId {
    pub fn new(id: u32) -> Self;
    pub fn as_u32(self) -> u32;
}
```

`RackId` is not a separate type. Rack identity is expressed as `(Level::Rack, u32)`
inside a `TopologyKey`.

---

### `Level`

```rust
/// A topology level tag — identifies which axis of the physical hierarchy a segment
/// refers to (rack, machine, zone, etc.).
///
/// Implemented as a newtype over u8 rather than an enum so that callers can define
/// their own levels without modifying this crate:
///
///   const DATACENTER: Level = Level(8);   // coarser than Zone
///   const ROW:        Level = Level(24);  // between Zone and Rack
///   const PDU:        Level = Level(40);  // between Rack and Machine
///
/// The built-in constants are spaced at multiples of 16, leaving room for callers
/// to insert before, after, or between them without colliding.
///
/// Ord is derived on the inner u8, so broader levels (lower values) sort before
/// narrower levels (higher values) — consistent with TopologyKey segment ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Level(pub u8);

impl Level {
    pub const ZONE:    Level = Level(16);
    pub const RACK:    Level = Level(32);
    pub const MACHINE: Level = Level(48);
    pub const DISK:    Level = Level(64);
}
```

---

### `TopologyKey`

```rust
/// The physical location of a node, as an ordered list of (Level, id) segments.
///
/// Ordered from broadest to most specific (e.g. Zone → Rack → Machine → Disk).
/// Inline storage for up to 4 segments; no heap allocation for the common case.
///
/// # Examples
///
/// Single-level (rack-only cluster):
///   TopologyKey::new(&[(Level::Rack, 3)])
///
/// Two-level (rack + machine):
///   TopologyKey::rack_machine(3, 7)
///
/// Three-level (zone + rack + machine):
///   TopologyKey::new(&[(Level::Zone, 1), (Level::Rack, 3), (Level::Machine, 7)])
pub struct TopologyKey(SmallVec<[(Level, u32); 4]>);

impl TopologyKey {
    /// Construct from a slice of (level, id) segments.
    pub fn new(segments: &[(Level, u32)]) -> Self;

    /// Convenience: a single rack segment.
    pub fn rack(rack: u32) -> Self;

    /// Convenience: rack + machine segments (the common two-level case).
    pub fn rack_machine(rack: u32, machine: u32) -> Self;

    /// Read-only view of the segments.
    pub fn segments(&self) -> &[(Level, u32)];

    /// Return the value for a given level, if present.
    /// Returns None if this key has no segment with the given Level.
    pub fn level(&self, kind: Level) -> Option<u32>;
}
```

`TopologyKey` implements `PartialEq`, `Eq`, `Hash`, `Clone`.

Two nodes with the same `TopologyKey` share all failure domains — this is the degenerate
case (both on the same machine and rack), which the constraint should penalise.

---

### `NodeInfo`

```rust
/// Description of one storage node, supplied at ClusterMap construction.
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// Application-assigned node ID. Must be unique within the map.
    pub id: NodeId,
    /// Physical location in the topology hierarchy.
    pub location: TopologyKey,
    /// Relative capacity weight. Proportional to storage capacity.
    /// Nodes with weight 0.0 are excluded from placement.
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
    /// - any weight is negative, NaN, or infinite
    /// - any NodeId is duplicated
    ///
    /// Nodes are stored sorted by NodeId (canonical scan order for place()).
    ///
    /// ZONE_INIT: allocates.
    pub fn new(nodes: &[NodeInfo]) -> Result<Self, PlacementError>;

    /// Number of nodes with weight > 0.0.
    pub fn active_node_count(&self) -> usize;

    /// Number of distinct values at a given topology level among active nodes.
    /// E.g. distinct_count(Level::Rack) returns the number of unique rack IDs.
    pub fn distinct_count(&self, level: Level) -> usize;

    /// Sum of weights across all active nodes.
    pub fn total_weight(&self) -> f64;
}
```

---

### `Admission`

```rust
/// The admission decision returned by a placement constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// No constraint applies; the node competes globally for a slot.
    Global,

    /// The node may only replace a candidate from the same constraint group
    /// (same group key as computed by PlacementConstraint::group_key).
    Constrained,

    /// Skip this node; it cannot be placed under any circumstances.
    Excluded,
}
```

---

### `PlacementConstraint`

```rust
/// A pluggable placement constraint.
///
/// Two responsibilities, kept separate so ZONE_INIT and ZONE_HOT work is cleanly
/// separated:
///
/// **`group_key`** — pure function of a node. Called once per node at `Placer::new`
/// (ZONE_INIT) and stored alongside each node. In the hot path, same-group candidates
/// are found by comparing stored u64 group keys — no re-evaluation of the constraint.
///
/// **`admit`** — called once per candidate node during `place()` (ZONE_HOT). Given
/// the current candidate selection and the proposed new node, returns the admission
/// decision. Captures EC config (k, m) and policy parameters via closure.
/// `Constrained` means: may only replace a candidate whose stored group key equals
/// `group_key(new_node)`.
///
/// **ZONE_HOT requirement**: `admit` must not allocate.
pub struct PlacementConstraint {
    pub group_key: Arc<dyn Fn(&NodeInfo) -> u64 + Send + Sync>,
    pub admit: Arc<dyn Fn(&[NodeInfo], &NodeInfo) -> Admission + Send + Sync>,
}

impl PlacementConstraint {
    /// Cap on the number of shards sharing a given topology level value.
    ///
    /// E.g. level_cap(Level::Rack, 2) → at most 2 shards per rack.
    ///      level_cap(Level::Zone, 1) → at most 1 shard per zone.
    ///
    /// Nodes that have no segment for `level` are treated as sharing a sentinel
    /// group (they compete globally among themselves), not as members of any
    /// capped group.
    pub fn level_cap(level: Level, max: usize) -> Self {
        PlacementConstraint {
            group_key: Arc::new(move |node| {
                node.location.level(level)
                    .map(|v| v as u64)
                    .unwrap_or(u64::MAX)   // sentinel: nodes without this level
            }),
            admit: Arc::new(move |candidates, new_node| {
                match new_node.location.level(level) {
                    None => Admission::Global,  // no segment for this level; unconstrained
                    Some(my_val) => {
                        let count = candidates.iter()
                            .filter(|c| c.location.level(level) == Some(my_val))
                            .count();
                        if count < max { Admission::Global } else { Admission::Constrained }
                    }
                }
            }),
        }
    }

    /// Convenience: rack cap. Equivalent to level_cap(Level::Rack, max).
    /// The default for a (k, m) EC scheme is max = m (parity count).
    pub fn rack_cap(max: usize) -> Self {
        Self::level_cap(Level::Rack, max)
    }

    /// No constraint: all nodes compete globally.
    pub fn none() -> Self {
        PlacementConstraint {
            group_key: Arc::new(|_| 0),
            admit: Arc::new(|_, _| Admission::Global),
        }
    }
}
```

Custom constraints have full access to the `TopologyKey` and can combine multiple
levels, consult external metadata, or implement EC-role-aware logic:

```rust
// Example: different caps for data shards vs parity shards, using candidates.len()
// as a proxy for current shard index (0..k = data, k..k+m = parity).
let k = ec_config.data_shards as usize;
PlacementConstraint {
    group_key: Arc::new(|node| {
        node.location.level(Level::Rack).unwrap_or(u32::MAX) as u64
    }),
    admit: Arc::new(move |candidates, new_node| {
        let shard_idx = candidates.len();          // next slot to fill
        let max = if shard_idx < k { 1 } else { 2 }; // stricter for data shards
        let my_rack = new_node.location.level(Level::Rack);
        let count = candidates.iter()
            .filter(|c| c.location.level(Level::Rack) == my_rack)
            .count();
        if count < max { Admission::Global } else { Admission::Constrained }
    }),
}
```

---

### `PlacementConfig`

```rust
/// Parameters for one placement scheme. Cheap to copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacementConfig {
    /// Total shards to place per stripe (k + m in EC terms).
    pub total_shards: u8,
}

impl PlacementConfig {
    pub fn new(total_shards: u8) -> Result<Self, PlacementError>;
}
```

Topology policy lives entirely in `PlacementConstraint`, not here.

---

### `Placer`

```rust
/// Stateless placement engine. Constructed once at ZONE_INIT; place() is the hot path.
/// Send + Sync: place() takes &self; all mutable state is stack-local.
pub struct Placer { /* opaque */ }

impl Placer {
    /// Construct from a config, cluster map, and constraint.
    ///
    /// Precomputes group_key for every node in the map (ZONE_INIT).
    /// Returns Err(TooFewNodes) if active_node_count < total_shards.
    ///
    /// ZONE_INIT: allocates.
    pub fn new(
        config: PlacementConfig,
        map: &ClusterMap,
        constraint: PlacementConstraint,
    ) -> Result<Self, PlacementError>;

    pub fn config(&self) -> PlacementConfig;

    /// Assign total_shards nodes to shards for the given key.
    ///
    /// `key`:  arbitrary bytes — object key, PG id, etc. May be empty.
    /// `out`:  caller-allocated; length must equal config.total_shards.
    ///         On success, out[i] is the NodeId for shard i.
    ///
    /// Returns Err(ConstraintUnsatisfiable) if the constraint prevents filling all
    /// slots. Returns Err(OutputLengthMismatch) if out.len() != total_shards.
    ///
    /// ZONE_HOT: no heap allocation. admit must not allocate.
    pub fn place(&self, key: &[u8], out: &mut [NodeId]) -> Result<(), PlacementError>;
}
```

---

### `PlacementError`

```rust
#[derive(Debug, thiserror::Error, PartialEq)]
pub enum PlacementError {
    #[error("empty cluster: no nodes provided")]
    EmptyCluster,

    #[error("duplicate node id {id}")]
    DuplicateNodeId { id: u32 },

    #[error("invalid weight {weight} for node {id}: must be finite and non-negative")]
    InvalidWeight { id: u32, weight: f64 },

    #[error("total_shards must be >= 1")]
    InvalidTotalShards,

    #[error("cannot place {shards} shards: only {nodes} active nodes available")]
    TooFewNodes { shards: usize, nodes: usize },

    #[error("constraint prevented filling all {shards} slots: only {filled} satisfiable")]
    ConstraintUnsatisfiable { shards: usize, filled: usize },

    #[error("output slice length {got} != total_shards {expected}")]
    OutputLengthMismatch { got: usize, expected: usize },
}
```

No topology-specific fields in errors — topology is the constraint's concern.

---

## Scoring and Selection Implementation Detail

### Scoring

For node i, given input key `K`:

```
hash_input  = K ++ node_id.as_u32().to_le_bytes()   (concatenated)
h: u64      = rapidhash(hash_input)
U_i: f64    = ((h >> 11) + 1) as f64 / (1u64 << 53) as f64   // maps to (0, 1]
score_i     = -f64::ln(U_i) / weight_i
```

Nodes with `weight = 0.0` are skipped before scoring.

### Streaming selection (O(total_shards) space)

Two stack-allocated arrays, both sized to `total_shards` (≤ 32):
- `candidates: [(score: f64, node_id: NodeId, group_key: u64); 32]`
- `group_counts: [(group_key: u64, count: u8); 32]`

`group_key` per candidate is the precomputed value from `constraint.group_key`.

For each node i in `ClusterMap` canonical order (sorted by `NodeId` at construction):

1. Compute `score_i`. Skip if `weight_i == 0.0`.
2. Look up `gk_i = precomputed_group_key[i]`.
3. Call `admission = constraint.admit(candidate_node_infos, &node_info_i)`.
4. **`Excluded`** → skip.
5. **`Global`**:
   - If `candidates` not full: insert `(score_i, node_id_i, gk_i)`.
   - Else: find `worst` = entry with highest score. If `score_i < worst.score`: evict
     `worst` (decrement its group count), insert new entry.
6. **`Constrained`**:
   - Find `worst_in_group` = highest-score entry where `group_key == gk_i`.
   - If `score_i < worst_in_group.score`: replace it (group count unchanged).
   - Else: skip.

After all nodes: if `candidates.len() < total_shards` → `ConstraintUnsatisfiable`.
Otherwise copy `candidates[i].node_id` into `out[i]`.

**Correctness proof** (exchange argument): a node rejected via `Constrained` (score ≥
worst in its group) is dominated within its group — any optimal selection containing
it can swap it for a lower-score same-group node already in the buffer. A node rejected
via `Global` (score ≥ global worst) is globally dominated. Both cases mean: no rejected
node can appear in any optimal selection.

---

## Resolved Design Decisions

### 1. Rendezvous HRW vs CRUSH

**Rendezvous HRW.** CRUSH complexity is not justified at our target scale. Simple to
verify, audit, and reason about. Distribution quality is equivalent for
uniform/near-uniform weights.

### 2. PG layer not in this crate

PG virtualization is two lines at the caller. Embedding it here would couple the
placement crate to the metadata layer prematurely.

### 3. Streaming selection, not sort-then-filter

The hot path uses a candidate buffer of size `total_shards` (≤ 32) — O(total_shards)
stack space regardless of cluster size. No `MAX_NODES` constant. The `ClusterMap` owns
a `Vec<NodeInfo>` (ZONE_INIT); only `place()` (ZONE_HOT) is bounded-stack.
O(N × total_shards) time: for N=1000 and total_shards=16, 16,000 iterations.

### 4. TopologyKey instead of RackId

`NodeInfo` does not have a `rack: RackId` field. Topology is a `TopologyKey`: an
ordered list of `(Level, u32)` segments. This means:

- New topology axes (zone, machine, power domain, etc.) require no struct changes.
- The constraint operates on whichever levels it cares about.
- `Level::RACK` is not privileged; it is just a named constant.

`SmallVec<[(Level, u32); 4]>` gives inline storage for paths up to 4 levels (which
covers every realistic hierarchy: zone → rack → machine → disk). No heap allocation
for the common case.

### 5. Level is a newtype, not an enum

`Level` is `struct Level(pub u8)` with associated constants, not an enum. This is
the key that makes spacing meaningful: downstream callers can define their own
`const MY_LEVEL: Level = Level(24)` and use it with `level_cap` or constraint
closures without any changes to this crate.

The built-in constants are spaced at multiples of 16 (Zone=16, Rack=32, Machine=48,
Disk=64), leaving 15 values between each pair for caller-defined levels. An enum
would not allow this — `#[non_exhaustive]` only prevents exhaustive matching;
it does not let callers add variants. A newtype gives real extensibility.

### 6. Pluggable constraint: two-function design

`group_key(node) -> u64` is a pure function of a node, precomputed once at
`Placer::new` (ZONE_INIT). This allows ZONE_HOT to find same-group candidates by
integer comparison without re-evaluating the constraint.

`admit(candidates, new_node) -> Admission` is called in ZONE_HOT. It has full
visibility into the current candidate selection and captures EC config and policy
parameters via closure.

`PlacementConfig` has no topology fields. All topology policy is in the constraint.

### 7. level_cap is the primitive; rack_cap is a convenience alias

`level_cap(level, max)` is the general form. `rack_cap(max)` is
`level_cap(Level::Rack, max)`. The default for a (k, m) EC scheme is
`rack_cap(m)`.

### 8. Nodes without a given level segment

If a node's `TopologyKey` has no segment for the constrained `Level`, `level_cap`
treats it as `Admission::Global` (unconstrained). This is a safe fallback: such nodes
were not given topology information for that axis and should not be excluded. The
`group_key` sentinel (`u64::MAX`) ensures they form their own group and don't
collide with real level values.

### 9. `place()` takes `&self`

All mutable state is stack-local. The `Placer` is safe to share across concurrent
request-handling threads without locking.

### 10. Floating-point determinism

`f64::ln` is correctly rounded per IEEE 754 on all supported targets. Only
same-machine consistency is required; cross-architecture bit-identical results are
not needed.

### 11. Error on unsatisfiable constraint

Return `Err(ConstraintUnsatisfiable { shards, filled })`. The caller handles the
fallback (e.g. retry with `PlacementConstraint::none()`). No topology specifics in
the error.

---

## Test Plan

### 1. Configuration validation

| Test | Expected |
|---|---|
| `PlacementConfig::new(6)` | Ok |
| `total_shards = 0` | `InvalidTotalShards` |
| `ClusterMap::new([])` | `EmptyCluster` |
| Duplicate NodeId | `DuplicateNodeId` |
| Negative weight | `InvalidWeight` |
| NaN weight | `InvalidWeight` |
| Infinite weight | `InvalidWeight` |
| `active_node_count < total_shards` | `TooFewNodes` |

### 2. TopologyKey construction and accessors

- `rack(3).level(Level::Rack)` → `Some(3)`
- `rack(3).level(Level::Zone)` → `None`
- `rack_machine(3, 7).level(Level::Machine)` → `Some(7)`
- `rack_machine(3, 7).segments()` → `&[(Rack, 3), (Machine, 7)]`
- `TopologyKey::new(&[])` → empty key; `level(Level::Rack)` → `None`

### 3. Output length mismatch

- `out.len() < total_shards` → `OutputLengthMismatch`
- `out.len() > total_shards` → `OutputLengthMismatch`

### 4. Determinism

Same cluster + constraint + key → same output every call:
- Call `place` 100 times; assert all results equal.
- Two `Placer`s from identical inputs; assert outputs match.
- Modified key → different output (hash sensitivity).

### 5. No duplicates

For any valid cluster and key: `out` contains no repeated `NodeId`.
Include `total_shards = active_node_count` to stress this.

### 6. rack_cap / level_cap constraint respected

- `level_cap(Level::Rack, 2)`: count shards per rack-id; assert each ≤ 2.
- `level_cap(Level::Zone, 1)`: at most 1 shard per zone value.
- `level_cap(Level::Machine, 1)` with `TopologyKey::rack_machine(r, m)`:
  no two shards on the same machine, regardless of rack.

### 7. ConstraintUnsatisfiable

| Scenario | Expected |
|---|---|
| 1 rack, total_shards=6, rack_cap(2) | `ConstraintUnsatisfiable` |
| 3 racks × 2 nodes, total_shards=6, rack_cap(2) | Ok |
| All nodes weight 0.0 | `TooFewNodes` |
| level_cap(Zone, 1) with 2 zones, total_shards=3 | `ConstraintUnsatisfiable` |

### 8. Nodes without the constrained level

- Cluster has nodes with and without `Level::Zone`.
- `level_cap(Level::Zone, 1)`: nodes lacking Zone are treated as unconstrained
  (returned `Global`); they do not block nodes that do have Zone segments.

### 9. PlacementConstraint::none()

- All nodes compete globally; `total_shards = node_count` fills all nodes.
- No duplicates.

### 10. Custom constraint via closure

Supply a closure implementing an AZ cap via a NodeId → AZ HashMap. Assert the output
respects the AZ cap. Validates the pluggable path works identically to the built-in.

### 11. Even distribution (statistical)

12 nodes across 3 racks (4 nodes each, equal weights), `total_shards=6`,
`rack_cap(2)`. Place 10,000 random keys. Each node should receive
`10000 * 6 / 12 = 5000` assignments ± 20%.

### 12. Weighted distribution (statistical)

One node with weight 2.0, four nodes with weight 1.0, `none()`, `total_shards=3`.
Place 10,000 keys. Heavy node receives ≈2× as many assignments as each light node
(± 20%).

### 13. Minimal movement on node removal

- Place 10,000 keys on N=12 nodes.
- Remove one node (weight 0.0 in new ClusterMap).
- Re-place all keys. Assert fraction of changed assignments ≤ 1/(N-1) + 0.05.

### 14. Node addition — monotonicity

- Add a node to a cluster; re-place 10,000 keys.
- Assert changed assignments moved TO the new node only. No existing pair swapped.

### 15. distinct_count

- Cluster with 12 nodes, 3 racks of 4: `distinct_count(Level::Rack)` = 3.
- Cluster with rack_machine topology: `distinct_count(Level::Machine)` = count of
  unique (machine) values across active nodes.
- Nodes lacking the queried level are not counted.

### 16. Large cluster (no footgun)

- 1000 nodes across 100 racks (10 nodes/rack), `total_shards=8`, `rack_cap(2)`.
- Place 1000 random keys. Assert success, no duplicates, rack cap respected.
- Assert zero heap allocations in `place()`.

### 17. TopologyKey inline storage (no heap)

- Construct `TopologyKey::new(&[(Zone,1),(Rack,2),(Machine,3),(Disk,0)])` (4 segments).
- Verify SmallVec inline path (no heap allocation during construction).
- 5-segment key spills to heap — this is expected and documented.

### 18. Hot path allocation (ZONE_HOT compliance)

Thread-local counting allocator (same pattern as EC crate):
- `place()` on a pre-built `Placer`: **zero heap allocations**.
- Verified for `rack_cap`, `level_cap`, and a custom closure constraint.

### 19. Property-based tests (proptest)

```
forall (
    node_count   in 1..=256usize,
    rack_count   in 1..=node_count,
    total_shards in 1..=min(node_count, 32) as u8,
    key: Vec<u8>
):
    cluster = build_cluster(node_count, rack_count, equal_weights)
              // each node gets TopologyKey::rack(rack_id)
    max_per_rack = ceil(total_shards / rack_count)
    placer = Placer::new(
        PlacementConfig::new(total_shards)?,
        &cluster,
        PlacementConstraint::rack_cap(max_per_rack),
    )?
    out = [NodeId(0); total_shards]
    placer.place(&key, &mut out)?

    prop_assert!(no duplicates in out)
    prop_assert!(rack counts all <= max_per_rack)
```

### 20. Fuzz targets (deferred)

`fuzz_place`: arbitrary (cluster bytes, key bytes, config, constraint params) —
must not panic. Register as `cargo-fuzz` target when proptest suite is stable.

### 21. Benchmarks (deferred)

| Cluster | total_shards | Constraint |
|---|---|---|
| 12 nodes, 3 racks | 6 | rack_cap(2) |
| 50 nodes, 5 racks | 8 | rack_cap(2) |
| 1000 nodes, 100 racks | 16 | rack_cap(2) |

Track zero-allocation assertion per CI run.
