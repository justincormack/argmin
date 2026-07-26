---
id: SAFETYDOC-001
bug_class: safety-doc
title: target_feature-gated unsafe fns in crc32c.rs lack rustdoc `# Safety` sections
location: crates/checksum/src/crc32c.rs:380
function: update
confidence: Medium
worker: worker-1
fp_verdict: TRUE_POSITIVE
fp_rationale: "Confirmed pub unsafe fn update (and siblings) carry #[target_feature] but no rustdoc # Safety section; the only current call site does perform the matching runtime feature check, so this is a currently-correct but undocumented unsafe contract."
severity: LOW
attack_vector: Remote
exploitability: Theoretical
severity_rationale: "Currently-correct unsafe (dispatch site does check CPU features) that lacks a documented # Safety contract -- classic defense-in-depth gap, matches the LOW hardening-gap tier."
status: fixed
---

## Description
`x86_64_vpclmul::update` (and its siblings `update_blocks_only`, and the
AArch64 `update` in `aarch64_crc_pmull`) are `pub unsafe fn`s whose entire
safety precondition is "the CPU running this process actually supports the
instruction set named in `#[target_feature(enable = "...")]`". Calling a
`#[target_feature]` function on hardware that lacks the feature is
undefined behavior (illegal instruction / potential silent misbehavior
depending on the target). None of these functions carry a rustdoc
`# Safety` section stating that precondition, so a future caller inside the
crate (or a refactor that hoists the dispatch logic) has no written contract
to consult — the only place the invariant is enforced is the runtime
`has_vpclmul_x86_64()` / `has_crc_pmull_aarch64()` checks in the dispatch
function, which live in a different part of the file with no cross-reference
from the `unsafe fn` itself.

This is a documentation-soundness gap, not a currently-triggered bug: today
every call site does perform the correct runtime feature check before
dispatching. But per the unsafe-boundary contract, every `unsafe fn` must
independently document the invariant its callers must uphold — relying on
"we happen to always call it correctly today" is exactly the pattern that
regresses silently when someone adds a new call site (e.g. a benchmark, a
fuzz harness, or a future backend-selection refactor) without re-deriving
the feature-detection requirement from first principles.

## Code
```rust
#[target_feature(enable = "sse4.1,pclmulqdq,vpclmulqdq,avx2")]
pub unsafe fn update(crc: u32, data: &[u8]) -> u32 {
    let prefix_len = data.len() & !0x0F;
    ...
}
```
No `/// # Safety` doc comment precedes this function (or `update_blocks_only`
in the same module, or `update`/`update_hw` in `aarch64_crc_pmull`).

## Data flow
N/A — file-level/documentation finding; the invariant is a CPU-capability
precondition, not attacker-controlled data flow. `crc` and `data` themselves
are ordinary values; the missing contract concerns which caller thread/CPU
context may invoke the function at all.

## Reachability trace
`checksum::crc32c::update(&mut self, data) -> update_dispatch(...) -> unsafe { x86_64_vpclmul::update(crc, data) }`.
The only current caller is the crate-internal dispatch function, which does
call `has_vpclmul_x86_64()` first — but that fact lives entirely outside this
function's own documentation.

## Impact
If a future internal caller (new dispatch path, benchmark harness, or
generic-over-backend refactor) invokes this `unsafe fn` without repeating the
CPU-feature check, the process executes an unsupported SIMD instruction —
`SIGILL` on the affected CPU (remote-triggerable denial of service on
whichever storage/checksum request happens to hit the code path, since CRC64
is computed on every read/write per the codebase's stated integrity model).

## Mitigations checked
- `// SAFETY:` comment: absent on the `unsafe fn` declarations themselves
  (present, correctly, on the *internal* SIMD load helpers like
  `load_aligned`, but not on the outer `update`/`extend` entry points that
  carry the `#[target_feature]` precondition).
- Rustdoc `# Safety`: absent.
- Today's only call site performs the matching runtime check, so this is not
  currently reachable with untrusted input — the gap is that nothing enforces
  it stays that way.

## Recommendation
Add a `/// # Safety` doc comment to each `#[target_feature(enable = "...")]`
`pub unsafe fn` (and `pub(super)`/private `unsafe fn` peers) stating
verbatim: "Caller must have verified via `is_x86_feature_detected!`/
`is_aarch64_feature_detected!` that the enabled target features are
supported by the current CPU before calling this function." This turns the
implicit, dispatch-site-only invariant into a documented contract that
`cargo doc` and future reviewers can see at the definition site.

## Validity assessment

Valid low-severity documentation finding. Current CRC32C backend selection
correctly checked the required CPU features, so no unsupported backend was
reachable through the safe API. The contract was nevertheless implicit and
could be violated by a future internal caller.

## Resolution

Fixed by the safety-policy hardening in this change. Every CRC32C
target-feature entry point and helper now documents its CPU-feature and block
shape preconditions. Unsafe dispatch and intrinsic blocks have local
`SAFETY:` justifications, and the checksum crate now denies undocumented
unsafe blocks and missing safety documentation.
