# Erasure Coding Engine — API Design & Test Plan

## Scope

This document covers the internal Rust API for the erasure coding subsystem and the test strategy. The backend is Intel ISA-L (a C library); our Rust code provides a safe wrapper around it.

---

## Background & Constraints

**Target EC parameters**: configurable, default (4, 2); expected range k=1..16, m=1..8, k+m ≤ 32.

**Backend: Intel ISA-L**

ISA-L (`libisal`) is chosen as the backend. It implements GF(2^8) Cauchy-matrix Reed-Solomon with automatic SIMD dispatch (generic / SSE3 / AVX2 / AVX-512). This is the same algorithm used by Ceph, HDFS, and most production storage systems. It is O(k·m) per encode, which is optimal for our k+m ≤ 32 target range.

Alternatives considered and rejected:
- `reed-solomon-simd` / leopard: uses GF(2^16), FFT-based O(n log n) — faster only at large n (100s of shards), complex, and slower than ISA-L for small codes like (4,2).
- `reed-solomon-erasure` (Rust): unmaintained for 5 years; no SIMD.

ISA-L is a C library. The FFI boundary is encapsulated entirely within the `ec` crate; no `unsafe` appears in any other crate. The `ec-sys` sub-crate holds the raw bindings and `build.rs`; `ec` (the public crate) is safe Rust.

**ISA-L key functions used:**
- `gf_gen_cauchy1_matrix(matrix, k+m, k)` — generates the **full** `(k+m)×k` systematic encoding matrix (identity rows 0..k, Cauchy parity rows k..k+m); call as `(a, k+p, k)` and pass `a[k*k..]` to `ec_init_tables` (ZONE_INIT)
- `ec_init_tables(k, m, encode_matrix, gf_tables)` — precompute GF multiply tables (ZONE_INIT)
- `ec_encode_data(len, k, m, gf_tables, data_ptrs, parity_ptrs)` — encode (ZONE_HOT)
- `gf_invert_matrix(in, out, n)` — matrix inversion for decode sub-matrix (ZONE_HOT, stack-allocated inputs)
- `ec_init_tables` + `ec_encode_data` again for reconstruct (using inverted decode matrix)

**Memory policy** (from `guides/rust_memory_policy_strict.md`):
- `ZONE_INIT`: codec construction, Cauchy matrix + GF table pre-computation — allocation allowed
- `ZONE_HOT` (encode/reconstruct): **zero heap allocation** — callers own all buffers; ISA-L itself does not allocate
- Allocation failure must return an error, never panic

---

## Crate & Module Structure

```
ec-sys/             -- raw ISA-L bindings (unsafe, generated or hand-written)
  build.rs          -- compile/link libisal; detect SIMD features
  src/lib.rs        -- extern "C" declarations

ec/                 -- public safe Rust crate (no unsafe outside this crate boundary)
  src/
    lib.rs          -- public re-exports
    codec.rs        -- ErasureCodec, EcConfig, EcError, VerifyResult
    tables.rs       -- GF table layout, pre-computation helpers (ZONE_INIT)
    reconstruct.rs  -- decode matrix inversion and reconstruct logic
```

ISA-L is either linked as a system library (`pkg-config --libs libisal`) or compiled from vendored source in `ec-sys/vendor/isa-l` — both modes supported via a `build.rs` feature flag.

---

## Public API

### `EcConfig`

```rust
/// Parameters for an erasure coding scheme.
/// Built once; cheap to copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EcConfig {
    /// k: number of data shards
    pub data_shards: u8,
    /// m: number of parity shards
    pub parity_shards: u8,
}

impl EcConfig {
    pub fn new(data_shards: u8, parity_shards: u8) -> Result<Self, EcError>;

    pub fn total_shards(self) -> usize;  // data_shards + parity_shards

    /// Storage overhead as a fraction, e.g. (4,2) => 1.5
    pub fn overhead(self) -> f64;
}

pub const MAX_TOTAL_SHARDS: usize = 32;
```

Validation in `EcConfig::new`:
- `data_shards >= 1`
- `parity_shards >= 1`
- `data_shards + parity_shards <= MAX_TOTAL_SHARDS`

---

### `ErasureCodec`

The main type. Opaque; backend-specific state is internal.

```rust
pub struct ErasureCodec { /* opaque */ }

impl ErasureCodec {
    /// Construct codec, pre-computing encoding matrix and GF tables.
    /// ZONE_INIT: allocates. Returns Err on invalid config.
    pub fn new(config: EcConfig) -> Result<Self, EcError>;

    pub fn config(&self) -> EcConfig;

    // ── Hot path ─────────────────────────────────────────────────────────

    /// Encode k data shards into m parity shards.
    ///
    /// `data`:   exactly `k` slices, all of equal length `shard_size`
    /// `parity`: exactly `m` slices, all of length `shard_size` (caller-allocated)
    ///
    /// ZONE_HOT: no heap allocation.
    pub fn encode(
        &self,
        data: &[&[u8]],
        parity: &mut [&mut [u8]],
    ) -> Result<(), EcError>;

    /// Required scratch buffer size (bytes) for `verify` at a given `shard_size`.
    /// Allocate once and reuse across many `verify` calls (e.g. a scrub pass).
    pub fn verify_scratch_size(&self, shard_size: usize) -> usize; // = m * shard_size

    /// Verify that parity shards are consistent with data shards.
    /// Re-encodes data into `scratch` and compares against `parity`.
    /// Returns `Err(ScratchTooSmall)` if scratch is < m * shard_size bytes.
    ///
    /// ZONE_HOT: no heap allocation.
    pub fn verify(
        &self,
        data: &[&[u8]],
        parity: &[&[u8]],
        scratch: &mut [u8],
    ) -> Result<VerifyResult, EcError>;

    /// Reconstruct missing shards from a subset of available shards.
    ///
    /// `present_indices`: sorted, deduplicated shard indices (0..k+m); length >= k.
    ///                    If length > k, only the first k entries are used.
    /// `present_data`:    one slice per entry in `present_indices`, all length `shard_size`.
    /// `recover_indices`: shard indices to reconstruct (may be data or parity);
    ///                    must not overlap `present_indices` and must not contain duplicates.
    /// `outputs`:         one &mut [u8] per entry in `recover_indices`, all length `shard_size`.
    ///
    /// ZONE_HOT: no heap allocation. Scratch space for matrix inversion is
    /// stack-allocated (bounded by MAX_TOTAL_SHARDS^2 bytes).
    pub fn reconstruct(
        &self,
        present_indices: &[usize],
        present_data: &[&[u8]],
        recover_indices: &[usize],
        outputs: &mut [&mut [u8]],
    ) -> Result<(), EcError>;
}
```

**Design notes:**
- Shard size is inferred from the first input slice; all others are validated equal.
- Shard size must be ≤ `i32::MAX` (ISA-L takes `len` as `c_int`); returns `ShardSizeTooLarge` otherwise.
- `present_indices` and `recover_indices` are disjoint; API validates and returns `OverlappingIndices`.
- Duplicates within `recover_indices` are rejected with `DuplicateRecoverIndex`.
- Shard index layout: `0..k` = data shards, `k..k+m` = parity shards.

---

### `VerifyResult`

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyResult {
    Ok,
    /// Parity shard at this index (k..k+m) failed to match.
    Mismatch(usize),
}
```

---

### `EcError`

```rust
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EcError {
    #[error("invalid config: {reason}")]
    InvalidConfig { reason: &'static str },

    #[error("wrong shard count: expected {expected}, got {got}")]
    ShardCount { expected: usize, got: usize },

    #[error("shard size mismatch: expected {expected}, got {got} at shard {index}")]
    ShardSizeMismatch { index: usize, expected: usize, got: usize },

    #[error("insufficient shards for reconstruction: need {need}, have {have}")]
    InsufficientShards { need: usize, have: usize },

    #[error("shard index {index} out of range 0..{total}")]
    ShardIndexOutOfRange { index: usize, total: usize },

    #[error("duplicate shard index {index}")]
    DuplicateShardIndex { index: usize },

    #[error("present and recover index sets overlap at shard {index}")]
    OverlappingIndices { index: usize },

    #[error("present_indices is not sorted at position {index}")]
    UnsortedIndices { index: usize },

    #[error("scratch buffer too small: need {required} bytes, got {provided}")]
    ScratchTooSmall { required: usize, provided: usize },

    #[error("shard size {size} exceeds i32::MAX; split into smaller stripes")]
    ShardSizeTooLarge { size: usize },

    #[error("duplicate recover shard index {index}")]
    DuplicateRecoverIndex { index: usize },

    /// Should not occur with a Cauchy encoding matrix; indicates a bug.
    #[error("internal: matrix inversion failed (singular)")]
    SingularMatrix,
}
```

No `String` fields — all variants carry only fixed-size data, complying with the hot-path allocation policy.

---

### What is NOT in this API

- No `async` — EC is pure CPU work; async belongs in the caller
- No I/O — codec operates on in-memory byte slices only
- No concept of files, objects, nodes, or placement
- No checksum computation — that is the Storage Node's job
- No runtime backend selection — ISA-L is the only backend; SIMD level is detected and dispatched internally by ISA-L at runtime

---

## Test Plan

### 1. Configuration validation (`EcConfig::new`)

| Test | Expected |
|---|---|
| `(1, 1)` | Ok |
| `(4, 2)` | Ok |
| `(16, 8)` | Ok |
| `(0, 2)` | `InvalidConfig` |
| `(2, 0)` | `InvalidConfig` |
| `(25, 8)` | `InvalidConfig` (sum = 33 > 32) |

### 2. Encode correctness

- Deterministic: same input always produces same parity shards.
- Linearity over GF(2^8): scaling a data shard by a constant scales the corresponding parity contribution by the same constant.
- All-zeros input → all-zeros parity.
- Random data → verify passes immediately after encode.

### 3. Verify correctness

- Fresh encode → `VerifyResult::Ok`.
- Flip one bit in any data shard → call encode again (expected: still Ok for re-encoded parity, but object is now "wrong" — verify tests the pairing).
- Corrupt one parity shard byte → `VerifyResult::Mismatch(idx)` for correct index.
- Restore that byte → `VerifyResult::Ok`.
- `scratch` too small → `Err(ScratchTooSmall)`.
- Allocate scratch once, call verify many times (scrub pass pattern) — assert same result each time.

### 4. Reconstruct — exhaustive for small k+m

For each standard config `(k, m)` in `[(2,1), (3,2), (4,2), (6,3), (8,4)]`:
- Exhaustively enumerate all `C(k+m, k)` combinations of k present shards.
- For each combination, reconstruct all missing shards.
- Assert reconstructed == original for every data shard.
- Assert reconstructed parity == re-encoded parity for every parity shard.

Combinatorial sizes: C(3,2)=3, C(6,4)=15, C(6,3)=20, C(9,6)=84, C(12,8)=495.
All tractable in CI.

### 5. Reconstruct — boundary conditions

- Exactly k shards present (minimum required).
- All k+m shards present (should succeed trivially, no reconstruction needed for present ones).
- Recover only parity shards (data all present).
- Recover only data shards (parity all present).
- Recover a mix.
- `recover_indices` is empty (no-op, should succeed).

### 6. Zero-length shards (`shard_size = 0`)

Tested explicitly as a first-class case (not just an error path):
- `encode` with `shard_size = 0`: succeeds, parity buffers are untouched (zero-length no-op).
- `verify` with `shard_size = 0`: returns `VerifyResult::Ok`.
- `reconstruct` with `shard_size = 0` and k present shards: succeeds, output buffers untouched.
- Full round-trip: encode empty shards, then reconstruct — succeeds.

Rationale: empty objects are real (zero-byte files, empty Docker layers). A special-case error for `shard_size = 0` would force callers to add their own branch and would have caused more complexity than the no-op behaviour, as learned from Docker registry handling.

### 7. Error conditions — no panics

Each of these must return `Err(...)`, never panic:
- `encode` with k-1 data slices
- `encode` with k+1 data slices
- `encode` with shards of different sizes
- `reconstruct` with k-1 present shards
- `reconstruct` with a shard index >= k+m
- `reconstruct` with a duplicate in `present_indices`
- `reconstruct` with `present_indices` not sorted
- `reconstruct` with overlap between `present_indices` and `recover_indices`
- `reconstruct` with `present_data` length != `present_indices` length
- `reconstruct` with `outputs` length != `recover_indices` length

### 8. Allocation tests (ZONE_HOT compliance)

Use a custom counting allocator (wrap `std::alloc::System`, count allocs).
Counting uses thread-local state (not a global atomic) so parallel test threads do not interfere.
- `encode` on pre-built codec: **zero heap allocations** (beyond caller ref-slice Vecs).
- `verify` on pre-built codec: **zero heap allocations** (caller provides scratch buffer).
- `reconstruct` on pre-built codec: **zero heap allocations**.

These tests are `#[cfg(test)]`-only, since the counting allocator is test infrastructure.

### 9. Property-based tests (proptest)

```
forall (k in 1..=8, m in 1..=4, shard_size in 0..=4096, data: Vec<u8>):
  let codec = ErasureCodec::new(EcConfig::new(k, m))?;
  // encode
  // for any subset of size k from k+m shards:
  //   reconstruct → data matches original
```

Shrinking on failure will produce a minimal (k, m, shard_size, data) counterexample.

### 10. Fuzz targets (deferred)

Fuzz targets are planned but not yet implemented. Deferred until the proptest suite
is stable and the API is frozen.

- `fuzz_encode`: arbitrary `(k, m, data_bytes)` — must not panic.
- `fuzz_reconstruct`: arbitrary present/absent shard pattern + data — must not panic.

Both will be registered as `cargo-fuzz` targets when implemented.

### 11. Benchmarks (`criterion`, deferred)

Measure encode throughput (GB/s) and reconstruct throughput:

| Config | Object size | Notes |
|---|---|---|
| (4, 2) | 1 MB | default/common |
| (6, 3) | 1 MB | |
| (8, 4) | 1 MB | |
| (4, 2) | 64 KB | small shard size |
| (4, 2) | 64 MB | large, cache-cold |

Track peak RSS and allocation count per CI run (per memory policy §9).

Benchmarks are deferred until the API is frozen. Criterion harness to be added to `crates/ec/benches/`.

---

## Resolved Design Decisions

### 1. Zero-length shards (`shard_size = 0`)

**Supported.** `ec_encode_data(0, ...)` is a no-op in ISA-L (the scalar base handles it as an empty loop). The Rust wrapper passes it through without special-casing. Tests must cover this explicitly because real callers will encounter it (empty objects, Docker layer blobs, etc.) and silent failure would be worse than an obvious no-op.

### 2. `present_indices` sort order

**Callers must pass sorted indices; the API validates and returns `EcError::UnsortedIndices` if not.** This avoids any hidden allocation or sort in the hot path, and callers at this internal API level can trivially sort a slice of ≤ 32 elements.

### 3. ISA-L linking

**System library only for now.** Depend on `pkg-config` finding `libisal`. A `vendor` Cargo feature can be added later if reproducible builds become a requirement. Keep it simple until needed.

### 4. `no_std`

Not a requirement. ISA-L requires libc. Dropped.

### 5. ISA-L alignment and minimum length

Verified against ISA-L source (`ec_highlevel_func.c`, SIMD assembly):

- **Any byte length including 1 works.** The top-level `ec_encode_data` dispatcher falls back to the scalar `ec_encode_data_base` for `len < 16` (SSE/AVX) or `len < 32` (AVX2/AVX-512). The caller never needs to pad.
- **Arbitrary pointer alignment works.** SIMD implementations use `movdqu`/`vmovdqu` (unaligned load/store) by default. 64-byte alignment is a performance hint, not a correctness requirement.
- **No constraints are exposed through our API.** The wrapper passes buffers directly to `ec_encode_data`; alignment and length handling is transparent.
- Performance note: 64-byte aligned allocations give best AVX-512 throughput. Storage nodes should use aligned allocations for their shard buffers, but this is a node-level concern, not an EC API constraint.

### 6. Large objects and memory budget

**The EC API does not stream; callers are responsible for striping.**

RS encoding is linear and stateless: encoding bytes `[a, b)` of each shard is entirely independent of bytes `[c, d)`. A 5GB object with k=4 would require 4 × 1.25 GB = 5 GB of data buffers plus 2 × 1.25 GB of parity buffers — 7.5 GB total if processed at once. That is not acceptable.

The solution is caller-side striping:
- Choose a stripe size (e.g., 4 MB per shard — fits in LLC, amortizes syscall overhead).
- Loop: for each stripe offset, call `encode` with slices of length `stripe_size` into the full shard buffers.
- Memory cost is `(k + m) × stripe_size` regardless of object size. For (4,2) with 4 MB stripes: 24 MB working set.

The existing `encode` signature already supports this naturally — the caller just passes a different window on each iteration. No streaming API is added to the EC crate. The striping loop belongs in the layer above (Storage Node / multipart staging), which controls I/O scheduling and memory budgets.

Recommended stripe sizes (to document in Storage Node plan):
- Default: 4 MB per shard (good L3 cache fit on most hardware)
- Minimum: no constraint from EC layer (even 1 byte works)
- Maximum: whatever the caller's memory budget allows
