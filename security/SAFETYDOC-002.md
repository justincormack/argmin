---
id: SAFETYDOC-002
bug_class: safety-doc
title: target_feature-gated unsafe fns in crc32.rs/crc64.rs lack rustdoc `# Safety` sections
location: crates/checksum/src/crc64.rs:466
function: extend
confidence: Medium
worker: worker-1
fp_verdict: TRUE_POSITIVE
fp_rationale: "Confirmed same gap as SAFETYDOC-001 reproduced in crc64.rs/crc32.rs: #[target_feature] unsafe fns with no # Safety doc, invariant upheld today only by the dispatch function's runtime feature check."
severity: LOW
attack_vector: Remote
exploitability: Theoretical
severity_rationale: "Currently-correct unsafe lacking a documented # Safety contract -- defense-in-depth gap, matches the LOW hardening-gap tier."
status: fixed
---

## Description
Same gap as in `crc32c.rs` (filed separately as SAFETYDOC-001), reproduced in
`crc64.rs` and `crc32.rs`: the `#[target_feature(enable = "...")]` `pub(?)
unsafe fn extend` / `extend_vpclmul` / `extend_avx512` / `extend_sha3`
functions (x86_64 pclmul/vpclmul/avx512 and AArch64 pmull/pmull+sha3
variants) have no rustdoc `# Safety` section documenting the CPU-capability
precondition that makes them `unsafe` in the first place. The precondition
is enforced only by the runtime `has_*_x86_64()` / `has_*_aarch64()` checks
in the crate-private dispatch functions, with no textual link back to the
`unsafe fn` definitions themselves.

## Code
```rust
#[target_feature(enable = "pclmulqdq")]
pub unsafe fn extend(crc: u64, data: &[u8]) -> u64 {
    let prefix_len = data.len() & !0x0F;
    ...
}

#[target_feature(enable = "pclmulqdq,vpclmulqdq,avx2")]
pub unsafe fn extend_vpclmul(crc: u64, data: &[u8]) -> u64 { ... }

#[target_feature(enable = "pclmulqdq,vpclmulqdq,avx512f")]
pub unsafe fn extend_avx512(crc: u64, data: &[u8]) -> u64 { ... }
```
None of these carry a `/// # Safety` doc comment. The equivalent functions in
`crc32.rs` (`x86_64_pclmul::extend`, `extend_vpclmul`) and the AArch64
`extend`/`extend_sha3` in `aarch64_pmull` have the same gap.

## Data flow
N/A — file-level/documentation finding; the missing contract concerns which
CPU/caller context may invoke the function, not attacker-controlled data.

## Reachability trace
`checksum::crc64::extend(crc, data) -> extend_dispatch match arm -> unsafe { extend_pclmul_x86_64(crc, data) } -> x86_64_pclmul::extend`.
CRC64-NVME is computed on every storage read per the codebase's stated
integrity-checking model, so this code path is on the hot path for every
remote GET.

## Impact
An internal caller that invokes one of these `unsafe fn`s without
re-deriving the matching `is_x86_feature_detected!` check (e.g. a future
benchmark, fuzz target, or backend-selection refactor) would execute
CPU instructions unsupported by the host, producing `SIGILL` — a remotely
triggerable crash on any request that reaches the checksum path on
unsupported hardware.

## Mitigations checked
- `// SAFETY:` comments: present on inner load/store helpers (`load_aligned`,
  `load_block`, etc.) but absent on the outer `#[target_feature]` entry
  points that are actually `unsafe fn`.
- Rustdoc `# Safety`: absent on all of `extend`, `extend_vpclmul`,
  `extend_avx512`, `extend_sha3` (crc64.rs) and `extend`, `extend_vpclmul`
  (crc32.rs).
- Current call sites do perform the correct runtime check, so this is latent
  documentation debt rather than an active bug today.

## Recommendation
Add `/// # Safety` doc comments to every `#[target_feature(enable = "...")]`
`unsafe fn` in `crc32.rs` and `crc64.rs` stating the required
`is_x86_feature_detected!`/`is_aarch64_feature_detected!` precondition,
mirroring the fix recommended for `crc32c.rs`.

## Validity assessment

Valid low-severity documentation finding. Current CRC32 and CRC64 backend
selection correctly checked the required CPU features, so the safe API did
not invoke unsupported instructions. The unsafe contracts remained implicit
at their definitions.

## Resolution

Fixed by the safety-policy hardening in this change. CRC32 and CRC64
target-feature entry points and helpers now document their exact x86 or Arm
feature requirements and any whole-block input requirements. Unsafe dispatch,
SIMD load, fold, and test-backend calls have local `SAFETY:` justifications,
enforced by crate-level Clippy policy.
