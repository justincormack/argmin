# Deterministic Test and Sleep Cleanup Plan

## Context

We have repeatedly found flaky local/unit-test failures caused by harnesses
that use sleeps, timeout-based polling, or helper-thread rendezvous with small
timeouts.

These failures are especially costly because they surface while we are trying
to diagnose real concurrency bugs, which makes it harder to distinguish
product regressions from broken tests.

The repo policy is now:

- unit tests, property tests, and local harnesses must be fully deterministic
- external compatibility suites such as `crates/s3-tests` may use bounded
  eventual checks when AWS convergence is the behavior under test

We also want to clean up production-side sleep loops. That is a separate
correctness and maintainability issue, but it is the same bad pattern family.

## Status

Status: planned.

## Goals

- remove nondeterministic waits and sleeps from local/unit/property tests
- replace timeout-based deadlock detection with deterministic harness
  coordination
- remove production sleep-loop retries where a proper signaling or state-based
  mechanism should exist

## Non-Goals

- changing AWS-backed `s3-tests` convergence behavior where eventual checks are
  intentionally modeling real AWS control-plane propagation
- broad refactors unrelated to deterministic coordination

## Phase 1: Clear Local Test Violations

Prioritize the direct local-test violations first.

### Target Files

- `crates/server-core/src/coordinator/multipart_stateful_tests.rs`
- `crates/server-core/src/coordinator/test_support.rs`
- `crates/storage/src/node.rs` test helpers

### Work

1. Replace `sleep(...)` based stale-session/reclaim assertions with explicit
   deterministic hooks or direct queue/session inspection.
2. Remove helper-thread plus `recv_timeout(...)` polling where the test can
   instead inspect the exact in-memory queue or state transition directly.
3. Keep any new helper narrowly scoped to test-only code when it exists only
   to support deterministic verification.

## Phase 2: Replace Timeout-Based Deadlock Tests With Deterministic Harnesses

These are more involved because many of them are trying to prove "does not
deadlock" or "does block until released" with helper threads and timeouts.

### Target Files

- `crates/server-core/src/coordinator/access_control_tests.rs`
- `crates/server-core/src/coordinator/core_tests.rs`

### Work

1. Add explicit hooks/barriers so the test can know when the worker/request has
   reached the blocked point.
2. Replace negative timeout assertions like "no reply within 100ms" with
   deterministic blocked-state assertions driven by the hook.
3. Replace positive timeout assertions like "reply within 1s" with explicit
   release plus join/receive after the harness knows the worker is ready.

## Phase 3: Remove Production Sleep Loops

Production sleeps are not a test flake issue, but they are still the wrong
pattern.

### Initial Targets

- `crates/storage/src/node/bucket_ops.rs`

### Work

1. Audit each sleep loop and identify the exact state transition it is waiting
   for.
2. Replace ad hoc `sleep(1ms)` retry loops with proper signaling, a
   deterministic retry contract, or a clearer blocking primitive.
3. Add focused regressions around the intended coordination so the new code
   does not regress back to timing-based behavior.

## Order

Recommended order:

1. `multipart_stateful_tests.rs`
2. `test_support.rs`
3. `storage/src/node.rs` test helpers
4. `access_control_tests.rs`
5. `core_tests.rs`
6. production sleep-loop cleanup in `storage/src/node/bucket_ops.rs`
