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

Status: in progress.

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

Status: completed.

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

### Completed Scope

- `crates/server-core/src/coordinator/multipart_stateful_tests.rs`
- `crates/server-core/src/coordinator/test_support.rs`
- `crates/storage/src/node.rs` test helpers

Direct local timing waits in these areas were replaced with deterministic
state inspection, direct queue access, or test-only hooks.

## Phase 2: Replace Timeout-Based Deadlock Tests With Deterministic Harnesses

Status: completed.

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

### Completed Scope

- `crates/server-core/src/coordinator/core_tests.rs`
- `crates/server-core/src/coordinator/access_control_tests.rs`

The same-PG lock/deadlock coverage now uses explicit deterministic probes at
the real sensitive seams instead of helper threads plus `recv_timeout(...)`.

### Rescan Result

A repo-wide rescan of local `server-core` and `storage` tests no longer finds
remaining `recv_timeout(...)` or sleep-based waits in those harnesses.

### Notes From Cleanup

- Background lifecycle sweepers were a repeated hidden source of flakiness in
  supposedly unrelated tests. Several same-PG and lifecycle-sensitive tests had
  to switch to no-sweeper coordinator setup so the explicit test actor was the
  only actor mutating state.
- A coarse “request started” or “bucket handle loaded” hook is not enough for
  deadlock/non-blocking tests. The harness must probe the actual late-sensitive
  seam, otherwise regressions merely move from “timeout” to “hang forever”.
- Deterministic deadlock tests work better when they fail with explicit probe
  errors like “would block before X” rather than depending on elapsed time.
- Test-only hooks must stay test-only. One intermediate version exposed hook
  installation in normal builds, which widened production surface
  unnecessarily; these hooks were moved behind test-only gating.
- Shared-storage concurrency tests also need background workers disabled when
  those workers can acquire the same PG/lock surface being probed, otherwise
  the probe catches unrelated interference instead of the targeted regression.

## Phase 3: Remove Production Sleep Loops

Status: next.

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

### Current Known Loops

- `crates/storage/src/node/bucket_ops.rs:65`
- `crates/storage/src/node/bucket_ops.rs:84`
- `crates/storage/src/node/bucket_ops.rs:422`

### Bucket Drain Notes

These loops are part of the bucket write-reservation / drain protocol.

- normal bucket-scoped write flows acquire a per-bucket write reservation
- bucket drain/delete flips `write_reservations_blocked = 1`, which prevents
  new reservations
- drain then waits for `active_write_reservations == 0` before the destructive
  transition can continue

This means there are two distinct cases:

- delete/drain path waiting for already-in-flight reserved work to finish:
  this waiting is semantically required
- fresh request path attempting to acquire a new reservation while drain is
  active: this should likely fail immediately, not poll

So the likely target behavior is:

- keep explicit coordination for the drain/delete path, but replace the
  `sleep(1ms)` polling with a better state-change/wakeup mechanism
- stop retrying `BucketWriteDraining` on request paths like
  `with_bucket_write_reservation_snapshot(...)`; surface a typed error instead

The implementation work should preserve the invariant that `DeleteBucket`
waits only for work that was already admitted before drain started, while new
bucket-scoped requests are rejected once drain is active.

### Current Characterization

Current storage characterization tests now pin this narrower behavior:

- temporary drain can still delay a fresh bucket-write snapshot request and
  later allow it to succeed
- once bucket delete becomes terminal, the request no longer keeps polling; it
  returns `BucketNotFound`

This is an improvement over the earlier behavior, but it is not yet a full
elimination of potentially long waits.

- the request path no longer waits after the bucket has transitioned to
  `Deleting`
- but it can still wait during the transient-drain phase while the bucket
  remains `Active`
- that residual wait is not for client payload transfer; bucket write
  reservations are held around metadata/auth/session-setup work, not body
  streaming
- however, the wait is still open-ended in principle because `DeleteBucket`
  must wait for already-admitted reservations to drain, and those metadata
  operations can still be delayed by IO stalls, lock contention, or hung work

So Phase 3 remains in progress:

- post-terminal request polling is gone
- transient-drain waiting is still potentially long and needs a stronger
  coordination or rejection policy if we want to remove that risk entirely

## Order

Recommended order:

1. production sleep-loop cleanup in `storage/src/node/bucket_ops.rs`
