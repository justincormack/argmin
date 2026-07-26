---
id: SAFETYDOC-003
bug_class: safety-doc
title: target_feature-gated unsafe fns in ec/gf.rs SIMD backends lack rustdoc `# Safety` sections
location: crates/ec/src/gf.rs:411
function: encode_rows
confidence: Medium
worker: worker-1
fp_verdict: TRUE_POSITIVE
fp_rationale: "Confirmed the aarch64_neon/x86_64_avx512/x86_64_avx2 target-feature functions lacked Safety documentation. Runtime feature selection was correct, but the report understated the contract: raw-pointer SIMD loops also depend on validated shard, matrix, and table geometry."
severity: LOW
attack_vector: Remote
exploitability: Theoretical
severity_rationale: "Currently-correct unsafe lacking a documented # Safety contract on the erasure-coding hot path -- defense-in-depth gap, matches the LOW hardening-gap tier."
status: fixed
---

## Description
The Reed-Solomon SIMD backends in `ec::gf` (`aarch64_neon`, `x86_64_avx512`,
`x86_64_avx2` modules) each define `#[target_feature(enable = "...")]
pub(super) unsafe fn encode_rows` and `apply_matrix_rows`. As with the
checksum crate's CRC backends, the sole reason these functions are `unsafe`
is the CPU-feature precondition attached by `#[target_feature]` — calling
them without the matching runtime CPU support is undefined behavior. None of
`encode_rows`, `apply_matrix_rows`, `dot_prod_table_row`, `dot_prod_row`,
`mul_chunk`, `write_with_nibble_table`, or `xor_with_nibble_table` across the
three SIMD modules has a rustdoc `# Safety` section stating that
precondition. The invariant is upheld today only because
`ec::codec::selected_backend()` performs the matching
`is_x86_feature_detected!` / `is_aarch64_feature_detected!` check before
selecting the `Backend::Avx512X86_64` / `Backend::Avx2X86_64` /
`Backend::NeonAarch64` variant that these functions are dispatched from.

## Code
```rust
#[target_feature(enable = "neon")]
pub(super) unsafe fn encode_rows(
    k: usize,
    tables: &[u8],
    data: &[&[u8]],
    outputs: &mut [&mut [u8]],
) {
    for (row, output) in outputs.iter_mut().enumerate() {
        dot_prod_table_row(k, row, tables, data, output);
    }
}
```
No `/// # Safety` doc comment precedes this function or its AVX2/AVX-512
counterparts (`crates/ec/src/gf.rs:689`, `crates/ec/src/gf.rs:902`) or their
`apply_matrix_rows` siblings.

## Data flow
N/A — file-level/documentation finding; the missing contract concerns the
CPU-capability precondition for invoking a `#[target_feature]` function, not
attacker-controlled data (the `data`/`outputs` shard buffers themselves are
already length-validated by `ec::codec` before this point, per the
`ShardSizeMismatch` checks in `encode_with_backend`/`reconstruct_shards`).

## Reachability trace
`ErasureCodec::encode(data, parity) -> encode_with_backend(..., selected_backend()) -> gf::encode_rows(backend, ...) -> match backend { Backend::NeonAarch64 => unsafe { aarch64_neon::encode_rows(...) } }`.
Erasure coding runs on every PUT (encode) and on every degraded-read
reconstruction (decode) path, both remotely triggerable via the S3 API.

## Impact
A future internal caller that invokes one of these `unsafe fn`s without
first re-deriving the matching feature-detection check (e.g. a new
benchmark, fuzz target, or a refactor that inlines/bypasses
`selected_backend()`) would execute unsupported SIMD instructions on
mismatched hardware, causing `SIGILL` — a process crash reachable from any
PUT/GET that hits the affected code path.

## Mitigations checked
- `// SAFETY:` comments: entirely absent from `gf.rs`'s three SIMD backend
  modules (`aarch64_neon`, `x86_64_avx512`, `x86_64_avx2`) — 26 unsafe
  blocks/fns in the file, only 3 `SAFETY:` comments total, none attached to
  the `#[target_feature]` entry points.
- Rustdoc `# Safety`: absent.
- The actual invariant is upheld today: `ec::codec::selected_backend()`
  (crates/ec/src/codec.rs) does call `is_x86_feature_detected!("avx512f")` /
  `"avx2"` / `is_aarch64_feature_detected!("neon")` before selecting the
  corresponding `Backend` variant.

## Recommendation
Add `/// # Safety` doc comments to every `#[target_feature(enable = "...")]`
`unsafe fn` in the `aarch64_neon`, `x86_64_avx512`, and `x86_64_avx2` modules
of `gf.rs`, stating that callers must have confirmed the enabled target
features are present via the appropriate `is_*_feature_detected!` macro
before calling.

## Validity assessment

Valid low-severity documentation finding, with an incomplete original safety
analysis. Backend selection correctly checks CPU features, but those features
are not the sole safety precondition: the SIMD implementations use raw pointer
loads and stores and therefore also rely on validated shard lengths, matrix
row lengths, table geometry, and the maximum shard count.

## Resolution

Fixed by the safety-policy hardening in this change. Every Neon, AVX2, and
AVX-512 target-feature function now documents both its CPU requirements and
its applicable data-shape invariants. The safe backend dispatch sites explain
how feature selection and codec validation establish those contracts. The EC
crate now denies undocumented unsafe blocks and missing safety documentation,
including test targets.
