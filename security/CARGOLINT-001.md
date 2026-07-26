---
id: CARGOLINT-001
bug_class: cargo-lint-config
title: Unsafe-heavy checksum crate never escalates clippy::missing_safety_doc / undocumented_unsafe_blocks to deny
location: crates/checksum/Cargo.toml:1
function: (file-level)
confidence: Medium
worker: worker-19
fp_verdict: TRUE_POSITIVE
fp_rationale: "The undocumented-unsafe enforcement gap was valid, but the report incorrectly stated that CI did not deny warnings and implied missing_safety_doc covered the cited crate-private functions. CI already uses -D warnings; the restriction lint was not enabled, and private target-feature contracts required explicit documentation beyond that lint's reach."
severity: LOW
attack_vector: Remote
exploitability: Theoretical
severity_rationale: "Defense-in-depth hardening gap (missing lint escalation), not itself an exploitable vulnerability -- matches the LOW hardening-gap tier."
status: fixed
---

## Description
`checksum` is the single most unsafe-heavy crate in the workspace (121 `unsafe`
occurrences across `crc32.rs`, `crc32c.rs`, `crc64.rs` — hand-written x86_64
SSE4.1/PCLMULQDQ/AVX2/AVX-512 and aarch64 NEON/CRC/AES/SHA3 intrinsics used to
compute the CRC64-NVME integrity check that every read in this S3-compatible
server depends on). Neither the crate's own `Cargo.toml`, a workspace-level
`[lints]` table (there is none anywhere in the workspace root `Cargo.toml`),
nor any `#![deny(...)]` crate attribute exists anywhere in the repository.

Concretely: `crates/checksum/src/crc32c.rs` and `crc64.rs` each contain several
`pub unsafe fn` (e.g. `update`, `extend`, `extend_vpclmul`, `extend_avx512`,
`extend_sha3`) with **no `# Safety` doc-comment section** at all — only a
`#[target_feature(enable = "...")]` attribute. `clippy::missing_safety_doc` is
warn-by-default in rustc/clippy, so these functions are already sailing past
the *default* bar; because the lint is never escalated to `deny` anywhere in
CI-relevant config, a warning is easy to ignore or suppress and provides no
build-breaking guarantee that new unsafe intrinsics code documents its
preconditions (buffer length/alignment relative to the SIMD width, the
target-feature invariant the caller must uphold before calling the `unsafe fn`,
etc.).

## Code
```rust
// crates/checksum/src/crc32c.rs — no "# Safety" doc comment on a pub unsafe fn
#[target_feature(enable = "sse4.1,pclmulqdq,vpclmulqdq,avx2")]
pub unsafe fn update(crc: u32, data: &[u8]) -> u32 {
    let prefix_len = data.len() & !0x0F;
    if prefix_len < 128 {
        return super::update_scalar(crc, data);
    }
    ...
}
```
```toml
# crates/checksum/Cargo.toml — no [lints] table
[package]
name = "checksum"
version = "0.1.0"
edition = "2021"
```

## Data flow
N/A — file-level/manifest finding (no attacker-controlled data flow); this is
a build-configuration hygiene gap, not a runtime data-flow bug.

## Reachability trace
N/A — file-level finding. (For context: `checksum::crc64` backs the
CRC64-NVME integrity check invoked on every object read/write path in
`server-http`/`storage`, so regressions in this crate's unsafe SIMD code have
broad blast radius.)

## Impact
Without `deny(clippy::missing_safety_doc)` / `deny(clippy::undocumented_unsafe_blocks)`,
a future change to this crate's hand-rolled SIMD checksum code (the highest
unsafe-density code in the workspace) can add or modify `unsafe` blocks/fns
with unstated or mis-stated preconditions and still pass CI as long as
warnings aren't treated as hard failures. That erodes the auditability of the
exact code path guarding data-integrity checks that gate every read.

## Mitigations checked
- No workspace `[lints]` table, no per-crate `[lints]` table, no
  `RUSTFLAGS`-based `-D warnings`, and no `#![deny(...)]` crate attribute
  found anywhere in the repository (`rg` over all `Cargo.toml` and `*.rs`).
- `clippy::missing_safety_doc` is warn-by-default, but a plain `cargo clippy`
  run without `-D warnings` does not fail the build on it, so the warning is
  easy to lose in routine CI log noise.
- Existing `SAFETY` comments do exist elsewhere in this crate (`crc32.rs`,
  `crc32c.rs`, `crc64.rs` each have several), showing the team already cares
  about documenting unsafe invariants — the gap is that the practice isn't
  enforced, not that it's entirely absent.

## Recommendation
Add a workspace-level `[workspace.lints.clippy]` table (or at minimum a
per-crate `[lints]` table in `crates/checksum/Cargo.toml`) that escalates:
```toml
[lints.clippy]
missing_safety_doc = "deny"
undocumented_unsafe_blocks = "deny"
```
and have each crate opt in via `[lints] workspace = true`. Then add the
missing `# Safety` sections to the `pub unsafe fn`s in `crc32c.rs`/`crc64.rs`
so the new lint passes immediately rather than blocking on a large doc pass.

## Validity assessment

The unsafe-code auditability gap was valid, but two supporting claims were
incorrect. Normal CI already runs workspace Clippy with `-D warnings` in
`scripts/ci`. Also, `clippy::missing_safety_doc` does not report the cited
functions because their modules are private to the crate; running that lint
alone against `checksum` and `ec` passed before the fix. In contrast,
`clippy::undocumented_unsafe_blocks` is a restriction lint that was not
enabled. Enabling it exposed 36 checksum library diagnostics and 50 when test
targets were included.

## Resolution

Fixed by the safety-policy hardening in this change. The `checksum` and `ec`
crates now deny both `clippy::missing_safety_doc` and
`clippy::undocumented_unsafe_blocks`. Every locally compiled unsafe block and
implementation has a definition-site justification, and target-feature
functions have explicit `# Safety` contracts. The contracts include buffer
shape and block-size requirements where raw pointer operations impose more
than a CPU-feature precondition. Focused all-target/all-feature Clippy checks
fail on future undocumented unsafe blocks.
