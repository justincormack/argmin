---
id: CARGOLINT-002
bug_class: cargo-lint-config
title: HTTP-parsing crates on the REMOTE attack surface contain zero unsafe today but never forbid(unsafe_code)
location: crates/server-http/Cargo.toml:1
function: (file-level)
confidence: Medium
worker: worker-19
fp_verdict: TRUE_POSITIVE
fp_rationale: "Confirmed zero unsafe today in server-http/auth/s3-types/server-core/observability and no forbid(unsafe_code)/[lints] anywhere; real preventive hardening gap on the primary remote-parsing crates."
severity: LOW
attack_vector: Remote
exploitability: Theoretical
severity_rationale: "Defense-in-depth hardening gap (no runtime exploit exists today, since these crates have no unsafe) -- matches the LOW hardening-gap tier."
status: fixed
---

## Description
`server-http` (the crate that terminates HTTP, parses S3 path-style requests,
XML bodies, chunked/streaming SigV4 payloads, and multipart uploads directly
from untrusted remote clients) currently contains **zero** `unsafe` blocks
(`rg -c '\bunsafe\b' crates/server-http/src` = 0). The same holds for
`auth` (SigV4 signature/canonical-request parsing), `s3-types` (shared S3
XML/type definitions), `server-core`, and `observability`. None of these
crates' `Cargo.toml` files, nor the workspace root `Cargo.toml`, declare
`#![forbid(unsafe_code)]` or `[lints] unsafe_code = "deny"`/`"forbid"`
anywhere. Because these are exactly the crates that parse the most
adversarial, attacker-controlled input in the whole system (headers, query
strings, XML, chunked bodies), locking in "this crate must never introduce
`unsafe`" at the manifest level is cheap, high-value defense-in-depth: it
converts "a future PR adds an `unsafe` block to save an allocation while
parsing attacker XML" from a silent capability change into a compile error
that requires deliberate lint override.

## Code
```toml
# crates/server-http/Cargo.toml — primary REMOTE HTTP/XML parsing surface,
# no unsafe today, but no lint guarantees it stays that way
[package]
name = "server-http"
version = "0.1.0"
edition = "2021"
# no [lints] table anywhere in this file or the workspace root
```

## Data flow
N/A — file-level/manifest finding (no attacker-controlled data flow); this is
a preventive build-configuration gap, not an exploitable runtime bug today.

## Reachability trace
N/A — file-level finding. (For context: `server-http` is the crate that
directly terminates the S3 HTTP entry point described as the primary REMOTE
attack surface in this review's threat model.)

## Impact
No runtime exploit exists today (the crate has no `unsafe`). The risk is
regression: nothing in the build configuration prevents a future contributor
from adding an `unsafe` block to this or the other listed unsafe-free,
remote-facing crates (e.g. for a "zero-copy" XML/header optimization) without
any additional review signal, since `unsafe_code` is allow-by-default in
rustc.

## Mitigations checked
- Confirmed via `rg -c '\bunsafe\b'` that `auth`, `observability`,
  `s3-http-tests`, `s3-local-tests`, `s3-tests`, `s3-types`, `server-core`,
  and `server-http` all report 0 unsafe occurrences today.
- No workspace `[lints]` table and no per-crate `[lints]` table exists to
  escalate `unsafe_code`.
- No `#![forbid(unsafe_code)]` / `#![deny(unsafe_code)]` crate-root attribute
  found via `rg` across the workspace.

## Recommendation
For the crates confirmed unsafe-free today (`server-http`, `auth`, `s3-types`,
`server-core`, `observability`), add either a crate-root attribute:
```rust
#![forbid(unsafe_code)]
```
or a manifest-level lint:
```toml
[lints.rust]
unsafe_code = "forbid"
```
Do **not** apply this to `checksum`, `ec`, `placement`, `storage`,
`argmin-s3`, or `test-util`, which legitimately use `unsafe` (SIMD
intrinsics, FFI via `libc`, raw socket/fd handling) and would fail to build
under `forbid(unsafe_code)`.

## Validity assessment

This was a valid preventive hardening finding, not an active vulnerability.
The named crates contained no unsafe code, but that property was conventional
rather than enforced.

## Resolution

Fixed by the safety-policy hardening in this change. `auth`, `observability`,
`s3-types`, `server-core`, and `server-http` now declare
`#![forbid(unsafe_code)]` at their crate roots. Any future unsafe block,
function, implementation, or macro expansion in those crates is therefore a
compile error requiring an explicit architectural policy change rather than a
local lint suppression.
