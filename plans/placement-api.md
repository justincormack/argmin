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

**Floating-point determinism.** `f64::ln()` calls the platform's libm, whose results
are not required to be correctly rounded by IEEE 754. Results can differ across glibc
versions, musl, macOS libm, and between x86-64 and ARM64 — meaning two cluster nodes
could compute different placements for the same key if they run different hardware or
libc versions.

To support mixed ARM64/AMD64 and heterogeneous deployments, we use `libm::log` from the
`libm` crate (rust-lang/libm), which is a pure-Rust port of the musl math library.
It gives bit-identical results on all IEEE 754-compliant platforms by construction,
since it never calls the platform libc. This is a one-line change at the call site and
adds one dependency.

**Tie-breaking.** If two nodes have exactly equal scores (probability ~2^-64 per pair),
the tie is broken by scan order. Because `ClusterMap` stores nodes sorted by `NodeId`
at construction time, lower `NodeId` wins on a tie. This is deterministic.

**Constraint enforcement** is pluggable but subject to a correctness contract. See
`PlacementConstraint` below.

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
(Rack, 3)
(Rack, 3), (Machine, 7)
(Zone, 1), (Rack, 3), (Machine, 7), (Disk, 0)
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
  levels. The `admit` closure must not allocate.

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
- `libm` — pure-Rust port of musl math; used for `libm::log` to give bit-identical
  results across x86-64, ARM64, and any other IEEE 754 platform
- `thiserror` — already in workspace (used by `ec` crate)

---

## Public API

### `NodeId`

```rust
/// Opaque identifier for a storage node (one disk = one node).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(u32);

impl NodeId {
    pub fn new(id: u32) -> Self;
    pub fn as_u32(self) -> u32;
}
```

`RackId` is not a separate type. Rack identity is expressed as `(Level::RACK, u32)`
inside a `TopologyKey`.

---

### `Level`

```rust
/// A topology level tag — identifies which axis of the hierarchy a segment refers to.
///
/// Implemented as a newtype over u8 rather than an enum, so callers can define their
/// own levels without changing this crate:
///
///   const DATACENTER: Level = Level(8);   // coarser than Zone
///   const ROW:        Level = Level(24);  // between Zone and Rack
///   const PDU:        Level = Level(40);  // between Rack and Machine
///
/// Built-in constants are spaced at multiples of 16, leaving 15 values between each
/// pair for caller-defined levels.
///
/// Ord on the inner u8 means broader levels (lower values) sort before narrower ones,
/// consistent with TopologyKey segment ordering.
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
/// Segments must be ordered from broadest to most specific and must not repeat a
/// Level. `TopologyKey::new` enforces this: it sorts by Level and returns
/// Err(DuplicateLevel) if any Level appears more than once.
///
/// Inline storage for up to 4 segments; no heap allocation for the common case.
pub struct TopologyKey(SmallVec<[(Level, u32); 4]>);

impl TopologyKey {
    /// Construct from a slice of (Level, id) segments.
    ///
    /// Segments are sorted by Level automatically.
    /// Returns Err(DuplicateLevel) if any Level appears more than once.
    pub fn new(segments: &[(Level, u32)]) -> Result<Self, TopologyError>;

    /// Convenience: single RACK segment.
    pub fn rack(rack: u32) -> Self;

    /// Convenience: RACK + MACHINE segments (common two-level case).
    pub fn rack_machine(rack: u32, machine: u32) -> Self;

    /// Read-only view of the segments, in Level order.
    pub fn segments(&self) -> &[(Level, u32)];

    /// Return the value for a given level, if present.
    pub fn level(&self, kind: Level) -> Option<u32>;
}

#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("level {0:?} appears more than once in TopologyKey")]
    DuplicateLevel(Level),
}
```

`TopologyKey` implements `PartialEq`, `Eq`, `Hash`, `Clone`. Equality and hashing
operate on the sorted segment slice, so two keys constructed from the same segments
in different orders compare equal.

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
    /// Nodes are stored sorted by NodeId (canonical scan order for place(),
    /// and tie-breaking order for equal scores).
    ///
    /// ZONE_INIT: allocates.
    pub fn new(nodes: &[NodeInfo]) -> Result<Self, PlacementError>;

    /// Number of nodes with weight > 0.0.
    pub fn active_node_count(&self) -> usize;

    /// Number of distinct values at a given topology level among active nodes.
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

#### Correctness contract

The greedy streaming selection algorithm is proven correct only for **partition matroid
constraints**. A constraint is a valid partition matroid if and only if:

- Every node has a fixed group, determined solely by `group_key(node)`.
- Each group has a fixed capacity `max_per_group` that does not depend on the
  composition or count of other groups in the current selection.
- `admit(same_group_count, new_node)` returns `Global` when
  `same_group_count < max_per_group`, and `Constrained` when
  `same_group_count >= max_per_group`.

The per-group cap may vary by group identity (e.g. different racks could have
different caps), and it may depend on `new_node` itself (e.g. cap = EC parity count,
captured by closure). It must not depend on the count or identity of other groups.

If an `admit` function violates this contract (e.g. "admit only if total candidates
< N/2"), the algorithm may return a suboptimal or inconsistent placement. No
compile-time enforcement is possible; this is a documented caller responsibility.

```rust
/// A pluggable placement constraint.
///
/// Two responsibilities, kept separate so ZONE_INIT and ZONE_HOT work is cleanly
/// separated:
///
/// **`group_key`** — pure function of a node. Called once per node at `Placer::new`
/// (ZONE_INIT) and stored alongside each node. In the hot path, same-group candidates
/// are found by comparing stored u64 keys — no re-evaluation.
///
/// **`admit`** — called once per candidate node during `place()` (ZONE_HOT).
/// Arguments:
///   - `same_group_count`: number of candidates currently in the buffer whose stored
///     group_key matches group_key(new_node). Precomputed by the algorithm; no
///     iteration over the candidate buffer is required.
///   - `new_node`: the node being evaluated.
///
/// Returns the admission decision. Must implement a partition matroid (see above).
///
/// **ZONE_HOT requirement**: `admit` must not allocate. It must not access the full
/// candidate buffer — only `same_group_count` and `new_node` are provided.
pub struct PlacementConstraint {
    pub group_key: Arc<dyn Fn(&NodeInfo) -> u64 + Send + Sync>,
    pub admit: Arc<dyn Fn(usize, &NodeInfo) -> Admission + Send + Sync>,
}

impl PlacementConstraint {
    /// Cap on the number of shards sharing a given topology level value.
    ///
    /// Satisfies the partition matroid contract: group = level value, fixed cap.
    ///
    /// Nodes without a segment for `level` are treated as unconstrained (Global).
    /// Their group_key sentinel (u64::MAX) keeps them isolated from real level values.
    pub fn level_cap(level: Level, max: usize) -> Self {
        PlacementConstraint {
            group_key: Arc::new(move |node| {
                node.location.level(level)
                    .map(|v| v as u64)
                    .unwrap_or(u64::MAX)
            }),
            admit: Arc::new(move |same_group_count, new_node| {
                if new_node.location.level(level).is_none() {
                    return Admission::Global;  // node has no segment at this level
                }
                if same_group_count < max {
                    Admission::Global
                } else {
                    Admission::Constrained
                }
            }),
        }
    }

    /// Rack cap. Equivalent to level_cap(Level::RACK, max).
    /// Default for a (k, m) EC scheme: max = m (parity count).
    pub fn rack_cap(max: usize) -> Self {
        Self::level_cap(Level::RACK, max)
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

Custom constraints that satisfy the partition matroid contract:

```rust
// EC-role-aware cap: stricter for data shards than parity shards.
// Uses the group count via same_group_count; captures k via closure.
// This is still a valid partition matroid: the cap is fixed per call
// based on the node's identity (shard role isn't yet determined here —
// this applies the same cap to all shards; EC-role differentiation
// is a future extension requiring a more complex selection algorithm).
let max_per_rack: usize = 2;
PlacementConstraint {
    group_key: Arc::new(|node| {
        node.location.level(Level::RACK).unwrap_or(u32::MAX) as u64
    }),
    admit: Arc::new(move |same_group_count, _new_node| {
        if same_group_count < max_per_rack { Admission::Global }
        else { Admission::Constrained }
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

---

## Scoring and Selection Implementation Detail

### Scoring

For node i, given input key `K`:

```
hash_input  = K ++ node_id.as_u32().to_le_bytes()   (concatenated)
h: u64      = rapidhash(hash_input)
U_i: f64    = ((h >> 11) + 1) as f64 / (1u64 << 53) as f64   // maps to (0, 1]
score_i     = -libm::log(U_i) / weight_i
```

Nodes with `weight = 0.0` are skipped before scoring.

**Tie-breaking.** Exact score ties are broken by scan order. Because `ClusterMap`
stores nodes sorted by `NodeId`, a tie is broken in favour of the node with the
lower `NodeId`. This is deterministic.

**Floating-point note.** `libm::log` is used instead of `f64::ln()` to give
bit-identical results across x86-64, ARM64, and all other IEEE 754 platforms.
See the Background section and resolved decision 11.

### Streaming selection (O(total_shards) space)

Two stack-allocated arrays, both sized to `total_shards` (≤ 32):
- `candidates: [(score: f64, node_id: NodeId, group_key: u64); 32]`
- `group_counts: [(group_key: u64, count: u8); 32]`

Both arrays track their own length (`cand_len` and `gc_len` respectively).
`group_counts` is a flat association list, not a hash map. Lookup is a linear scan
for a matching `group_key`; since `total_shards ≤ 32`, this is at most 32 comparisons.

**Helpers used in the algorithm below:**

- `gc_get(gk)`: scan `group_counts[0..gc_len]` for an entry with `group_key == gk`;
  return its `count`, or 0 if not found.
- `gc_inc(gk)`: find or insert `(gk, 0)` in `group_counts`, then increment `count`.
- `gc_dec(gk)`: find the entry for `gk` and decrement `count`; remove if count
  reaches 0 (shift remaining entries down).

`group_key` per candidate is the precomputed value from `constraint.group_key`.

For each node i in `ClusterMap` canonical order (sorted by `NodeId`):

1. Compute `score_i`. Skip if `weight_i == 0.0`.
2. `gk_i = precomputed_group_key[i]`.
3. `gc = gc_get(gk_i)`.
4. Call `admission = constraint.admit(gc, &node_info_i)`.
5. **`Excluded`** → skip.
6. **`Global`**:
   - If `cand_len < total_shards`: append `(score_i, node_id_i, gk_i)` to candidates;
     `gc_inc(gk_i)`.
   - Else: find `worst` = entry in candidates with the highest score. If
     `score_i < worst.score`: replace `worst` with `(score_i, node_id_i, gk_i)`;
     `gc_dec(worst.group_key)`; `gc_inc(gk_i)`.
7. **`Constrained`**:
   - Find `worst_in_group` = entry in candidates with the highest score among those
     where `group_key == gk_i`.
   - If `score_i < worst_in_group.score`: replace `worst_in_group` with
     `(score_i, node_id_i, gk_i)`. Group count for `gk_i` is unchanged (one
     entry removed and one added for the same group).
   - Else: skip.

After all nodes: if `cand_len < total_shards` → `ConstraintUnsatisfiable`.
Otherwise copy `candidates[i].node_id` into `out[i]`.

**Correctness proof** (exchange argument, for partition matroid constraints):
A node rejected via `Constrained` (score ≥ worst in its group) is dominated within
its group — any optimal selection containing it can swap it for a lower-score
same-group node already in the buffer. A node rejected via `Global` (score ≥ global
worst) is globally dominated. Both cases mean no rejected node can appear in any
optimal selection. Correctness holds only when `admit` satisfies the partition matroid
contract documented in `PlacementConstraint`.

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
stack space regardless of cluster size. No `MAX_NODES` constant. O(N × total_shards)
time: for N=1000 and total_shards=16, 16,000 iterations.

### 4. TopologyKey instead of RackId

`NodeInfo` has `location: TopologyKey` instead of `rack: RackId`. New topology axes
require no struct changes. `Level::RACK` is not privileged.

`SmallVec<[(Level, u32); 4]>` gives inline storage for ≤4 levels (zone → rack →
machine → disk). No heap allocation for the common case.

### 5. Level is a newtype, not an enum

`Level` is `struct Level(pub u8)`. Callers define their own levels with
`const MY_LEVEL: Level = Level(24)`. Built-in constants are spaced at multiples of
16, leaving room to insert levels at any point in the hierarchy.

An `enum` would not allow this — `#[non_exhaustive]` only prevents exhaustive
matching; it does not let callers add variants. A newtype gives real extensibility.

### 6. TopologyKey sorts and validates at construction

`TopologyKey::new` sorts segments by `Level` automatically and returns
`Err(DuplicateLevel)` if any level appears more than once. This keeps the invariant
(ordered, unique levels) enforced at the boundary rather than relying on callers.

The convenience constructors (`rack`, `rack_machine`) are infallible because their
segments are statically known to be valid.

### 7. Constraint contract: partition matroids only

The greedy streaming algorithm is proven correct only for partition matroid
constraints (fixed group per node, fixed per-group cap). This is documented
explicitly as a caller contract on `PlacementConstraint`. Arbitrary `admit` logic
can produce suboptimal or inconsistent results.

The `admit` signature takes `(same_group_count: usize, new_node: &NodeInfo)` rather
than the full candidate slice. This enforces the partition matroid contract
structurally (the function cannot depend on the composition of other groups) and
eliminates any risk of allocation or `Vec` construction in the hot path.

### 8. level_cap is the primitive; rack_cap is a convenience alias

`level_cap(level, max)` is the general form. `rack_cap(max)` is
`level_cap(Level::RACK, max)`. Default for (k, m) EC: `rack_cap(m)`.

### 9. Nodes without a given level segment

If a node's `TopologyKey` has no segment for the constrained `Level`, `level_cap`
returns `Global` (unconstrained). `group_key` returns `u64::MAX` as a sentinel,
keeping such nodes in their own isolated group.

### 10. `place()` takes `&self`

All mutable state is stack-local. Safe to share across concurrent threads without
locking.

### 11. Cross-platform floating-point determinism via libm crate

`f64::ln()` calls the platform libc's `log()`, which is not required to be correctly
rounded by IEEE 754. Results can differ between glibc and musl, between glibc versions,
and between x86-64 and ARM64. A cluster running mixed hardware or a rolling OS upgrade
could compute inconsistent placements if two nodes disagree on a score.

We use `libm::log` from the `libm` crate (rust-lang/libm). This is a pure-Rust port of
the musl `log` implementation. Because it contains no platform calls and operates
entirely on IEEE 754 bit patterns, it gives bit-identical results on every conforming
platform. The change at the call site is trivial: replace `x.ln()` with `libm::log(x)` (C
convention: `log` = natural logarithm).

This supports mixed ARM64/AMD64 deployments, heterogeneous libc versions, and partial
OS upgrades without any operational constraints on homogeneity.

### 12. Tie-breaking: lower NodeId wins

`ClusterMap` sorts nodes by `NodeId` at construction. On an exact score tie, the
node encountered first in the scan wins, which is the node with the lower `NodeId`.
This is fully deterministic.

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

- `rack(3).level(Level::RACK)` → `Some(3)`
- `rack(3).level(Level::ZONE)` → `None`
- `rack_machine(3, 7).level(Level::MACHINE)` → `Some(7)`
- `rack_machine(3, 7).segments()` → `&[(RACK, 3), (MACHINE, 7)]` (sorted)
- Segments provided out of order are sorted automatically.
- Duplicate level → `Err(DuplicateLevel)`.
- `TopologyKey::new(&[])` → Ok; `level(Level::RACK)` → `None`.
- Custom caller-defined level: `const MY_LEVEL: Level = Level(24);`
  `TopologyKey::new(&[(MY_LEVEL, 5)])?.level(MY_LEVEL)` → `Some(5)`.

### 3. Output length mismatch

- `out.len() < total_shards` → `OutputLengthMismatch`
- `out.len() > total_shards` → `OutputLengthMismatch`

### 4. Determinism

Same cluster + constraint + key → same output every call:
- Call `place` 100 times; assert all results identical.
- Two `Placer`s from identical inputs; assert outputs match.
- Modified key → different output (hash sensitivity).

### 5. No duplicates

For any valid cluster and key: `out` contains no repeated `NodeId`.
Include `total_shards = active_node_count` to stress this.

### 6. rack_cap / level_cap constraint respected

- `level_cap(Level::RACK, 2)`: count shards per rack-id; assert each ≤ 2.
- `level_cap(Level::ZONE, 1)`: at most 1 shard per zone value.
- `level_cap(Level::MACHINE, 1)` with `rack_machine(r, m)`:
  no two shards on the same machine.

### 7. ConstraintUnsatisfiable

| Scenario | Expected |
|---|---|
| 1 rack, total_shards=6, rack_cap(2) | `ConstraintUnsatisfiable` |
| 3 racks × 2 nodes, total_shards=6, rack_cap(2) | Ok |
| All nodes weight 0.0 | `TooFewNodes` |
| level_cap(ZONE, 1) with 2 zones, total_shards=3 | `ConstraintUnsatisfiable` |

### 8. Nodes without the constrained level

- Mixed cluster: some nodes have `Level::ZONE`, some don't.
- `level_cap(Level::ZONE, 1)`: nodes lacking ZONE get `Global`; they don't block
  capped placement of nodes that do have ZONE.

### 9. PlacementConstraint::none()

- All nodes compete globally. No duplicates.
- `total_shards = node_count` fills all nodes.

### 10. Custom constraint (partition matroid)

Supply a closure implementing a machine cap via same_group_count. Assert output
respects the cap. Validates the pluggable path.

### 11. Constraint contract violation (documented, not enforced)

Deliberately provide an `admit` function that violates the partition matroid contract
(e.g., returns `Global` based on total candidate count, not just same-group count).
Document in the test that results may be suboptimal; assert only that the output is
valid (no duplicates, correct length). This is a negative test confirming the
documented limitation, not a correctness assertion.

### 12. Even distribution (statistical)

12 nodes across 3 racks (4 nodes each, equal weights), `total_shards=6`,
`rack_cap(2)`. Place 10,000 random keys. Each node receives
`10000 * 6 / 12 = 5000` assignments ± 20%.

### 13. Weighted distribution (statistical)

One node weight 2.0, four nodes weight 1.0, `none()`, `total_shards=3`.
Place 10,000 keys. Heavy node receives ≈2× assignments as each light node (± 20%).

### 14. Minimal movement on node removal

- Place 10,000 keys on N=12 nodes.
- Remove one node (weight 0.0 in new ClusterMap).
- Re-place. Assert changed fraction ≤ 1/(N-1) + 0.05.

### 15. Node addition — monotonicity

- Add a node; re-place 10,000 keys.
- Assert changed assignments moved TO the new node only.

### 16. Tie-breaking is deterministic

- Construct two nodes with identical scores for a given key (requires crafting
  node IDs such that `hash(key || id_a) == hash(key || id_b)`, which is
  impractical directly — instead, mock the scoring function in a unit test to
  inject equal scores).
- Assert the node with the lower NodeId is selected consistently.

### 17. distinct_count

- 12 nodes, 3 racks: `distinct_count(Level::RACK)` = 3.
- Nodes without RACK segment: not counted in RACK distinct_count.

### 18. Large cluster (no footgun)

- 1000 nodes, 100 racks, `total_shards=8`, `rack_cap(2)`.
- Place 1000 keys. Assert success, no duplicates, cap respected.
- Assert zero heap allocations in `place()`.

### 19. Hot path allocation (ZONE_HOT compliance)

Thread-local counting allocator (same pattern as EC crate):
- `place()` on a pre-built `Placer`: **zero heap allocations**.
- Verified for `rack_cap`, `level_cap`, and a custom closure constraint.

### 20. Property-based tests (proptest)

```
forall (
    node_count   in 1..=256usize,
    rack_count   in 1..=node_count,
    total_shards in 1..=min(node_count, 32) as u8,
    key: Vec<u8>
):
    cluster = build_cluster(node_count, rack_count, equal_weights)
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

### 21. Fuzz targets (deferred)

`fuzz_place`: arbitrary (cluster bytes, key bytes, config, constraint params) —
must not panic.

### 22. Benchmarks (deferred)

| Cluster | total_shards | Constraint |
|---|---|---|
| 12 nodes, 3 racks | 6 | rack_cap(2) |
| 50 nodes, 5 racks | 8 | rack_cap(2) |
| 1000 nodes, 100 racks | 16 | rack_cap(2) |

Track zero-allocation assertion per CI run.
