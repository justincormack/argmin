## Placement Deterministic Log Plan

Status: completed

This plan replaces the `libm` dependency used by the placement crate with an
internal deterministic natural-log implementation suitable for the placement
score path.

## Resolution

Completed by:

- `40c0bbf6` (`Restrict placement score log inputs`) introduced the placement-owned
  `Unit53` domain.
- `17008a28` (`Add placement core-math reference tooling`) added the committed
  reference corpus and optional CORE-MATH comparison helper.
- `48ef186d` (`Port placement log from core-math`) replaced the placement score
  path with the internal deterministic log implementation and removed `libm` from
  the placement crate.
- `45094ca9` (`Add core-math source references`) pinned the upstream CORE-MATH
  source reference used for the port.

The production score path now constructs a `Unit53` from the hash-derived
53-bit numerator and calls `deterministic_log_u53`. The implementation is checked
against a committed fixed corpus generated from CORE-MATH, and
`scripts/placement-log-core-math` can regenerate or check that corpus when a local
`tmp/core-math` checkout is available.

A random property test that proves 53-bit/correctly-rounded log output was not
added. Without an independent high-precision oracle such as MPFR/CORE-MATH at test
time, such a property test would either duplicate another log implementation in the
test suite or only check weaker invariants. The committed corpus plus optional
external comparison helper keeps the production and normal test dependency graph
small while preserving a reproducible validation path.

The current implementation in
`crates/placement/src/hash.rs` computes weighted rendezvous scores as:

```rust
score = -log(u) / weight
```

where `u` is derived from a 53-bit hash and therefore always lies in the closed
interval `[2^-53, 1]`.

That restricted input domain matters. We do not need a general-purpose `log`
for all `f64` inputs, NaNs, infinities, subnormals, or negative values. We need
a deterministic and stable `log` for one narrow, fully controlled domain that
placement can rely on across architectures and software updates.

## Goals

- remove the external `libm` dependency from the placement crate
- preserve cross-platform deterministic placement
- strengthen the contract from "stable implementation choice" to "stable,
  specified result"
- keep the production score path on `f64`
- validate the replacement against correctly-rounded reference data

## Non-goals

- do not change the weighted rendezvous formula
- do not reduce score precision to `f32`
- do not introduce platform libc calls
- do not port the full CORE-MATH `log` implementation unchanged if a smaller
  restricted-domain implementation is sufficient
- do not relax placement determinism to a statistical claim

## Constraints and Observations

- `NodeInfo.weight` is `f64`, and candidate ordering in placement compares full
  `f64` scores directly.
- Switching the score path to `f32` would change placement ranking precision and
  increase ordinary tie frequency, even if the `logf` implementation itself were
  correctly rounded.
- The input `u` is always finite, positive, normal, and bounded:
  `u = ((h >> 11) + 1) / 2^53`, so `u in [2^-53, 1]`.
- Because placement owns the generation of `u`, we can encode this domain in the
  API and make illegal states unrepresentable.
- The full CORE-MATH binary64 `log` implementation is substantially larger than
  the binary32 version, so the right target is a restricted-domain port, not a
  blind transliteration.

## Proposed Approach

Implement an internal placement-only `deterministic_log_u53` path with a narrow
API and explicit input contract, then switch `score()` to use it.

High-level shape:

1. Introduce a small internal wrapper type for placement score inputs.
   Example: a `Unit53` or similar newtype representing values generated from the
   53-bit hash mapping.

2. Replace direct construction of `u: f64` in `score()` with a helper that
   produces this wrapper from the hash bits.

3. Implement `deterministic_log_u53(x)` for that wrapper, returning an `f64`
   result with a fixed, documented contract.

4. Keep the external score formula unchanged:

```rust
score = -deterministic_log_u53(u) / weight
```

5. Remove `libm` from `crates/placement/Cargo.toml` once the new path is fully
   validated.

## Phase 1: Specify the Restricted Domain

Document the exact mathematical and bit-level contract for the log input:

- source is a 53-bit integer `n` in `[1, 2^53]`
- represented value is `u = n / 2^53`
- all inputs are finite and normal
- `u == 1.0` is reachable
- the smallest input is exactly `2^-53`

Acceptance criteria:

- the code no longer treats the log input as an arbitrary `f64`
- the restricted-domain contract is documented in code and in the placement
  design notes

## Phase 2: Choose the Deterministic Algorithm

Decide the implementation strategy before porting code.

Primary option:

- adapt CORE-MATH binary64 `log` to the restricted `[2^-53, 1]` domain,
  removing general-input handling that placement can never reach

What to evaluate explicitly:

- which special-case branches become dead under the placement contract
- whether the refinement path can be narrowed to a smaller set of hard cases
- whether the table footprint can be reduced for this domain
- whether a simpler fixed algorithm can still guarantee the required result

Acceptance criteria:

- written justification for the chosen algorithm
- explicit statement of whether the implementation is correctly rounded on the
  restricted domain or whether it targets a weaker but still fully specified
  deterministic contract

## Phase 3: Port the Implementation to Rust

Create an internal module in the placement crate, for example
`crates/placement/src/deterministic_log.rs`, and port only the pieces required
for the chosen restricted-domain algorithm.

Implementation requirements:

- pure Rust
- no platform libc calls
- preserve bit-level determinism
- avoid unnecessary unsafe code
- keep the API narrow so callers cannot bypass the restricted-domain contract

If a multi-precision refinement path is still needed, port only the supporting
types and tables that are actually used by the restricted-domain implementation.

Acceptance criteria:

- `score()` uses the internal deterministic log implementation
- `libm` is no longer referenced from the placement crate
- the implementation is reviewable as a placement-specific primitive rather than
  a generic math subsystem

## Phase 4: Reference Data and Differential Testing

Build a high-confidence validation story before relying on the new score path.

Test layers:

- exact regression vectors for boundary values:
  `u = 1`, `u = 2^-53`, values around powers of two, and selected hard cases
- randomized differential tests comparing the Rust implementation against
  correctly-rounded reference results generated offline
- score-level tests that compare old and new placement rankings on fixed
  fixtures, with the expectation that the new implementation becomes the source
  of truth after validation

Reference generation options:

- generate vectors using CORE-MATH or MPFR in a non-production helper script
- commit only the resulting test data or compact generator if reviewable

Acceptance criteria:

- deterministic log results match the chosen reference set exactly
- placement score tests cover both value correctness and ordering stability

## Phase 5: Placement-level Validation

Strengthen tests around the behavior the cluster actually depends on.

Add or extend tests for:

- deterministic scores across repeated runs
- ordering stability for equal-weight and mixed-weight node sets
- boundary inputs from the hash mapping
- tie behavior remaining limited to true score equality

Add a focused property test around score monotonicity:

- for fixed `u`, higher weight yields lower score
- for fixed weight, lower `u` yields higher score

Acceptance criteria:

- placement tests remain green with the new implementation
- no score-path regressions are observed in the existing placement suite

## Phase 6: Clean-up and Documentation

After the implementation and tests land:

- remove `libm` from the placement crate dependencies
- update `plans/completed/placement-api.md` to describe the new deterministic
  log strategy instead of the `libm` dependency
- document why `f64` remains required for placement scoring

Acceptance criteria:

- crate metadata and design docs match the implementation
- no stale references to the old `libm` score path remain in placement docs or code

## Verification

At minimum for the implementation change:

- run `cargo fmt --all`
- run `cargo clippy --all-targets --all-features -- -D warnings`
- run `cargo test -p placement`
- run `/bin/bash -lc 'S3_TEST_TIMEOUT_SECS=30 cargo test --workspace --no-fail-fast'`

If the reference-data generation path uses helper scripts or temporary code,
keep those out of the production dependency graph.

## Open Questions

- Can we prove correct rounding across the full `[2^-53, 1]` domain with a
  restricted port, or do we need a weaker deterministic specification for the
  first version?
- Is it worth encoding the hash-derived input as an integer-backed newtype and
  delaying `f64` materialization until inside `deterministic_log_u53`?
- Do we want exhaustive validation over all `2^24` binary32-style prefixes for
  selected subranges as an additional sanity check, even though production stays
  on `f64`?
