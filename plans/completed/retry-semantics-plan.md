# Retry Semantics Plan

Status: completed / superseded by Phase 10.9 bounded-request-work and
production backpressure planning

## Resolution

The open design question in this note has been resolved by later request-work
bounding and backpressure work. Foreground request paths should not contain
unbounded coordination waits. When expected contention or overload cannot be
resolved within the request-local budget, it is surfaced as a typed S3-shaped
response rather than escaping as an SDK operation-attempt timeout, transport EOF,
or generic HTTP 500.

Current working model:

- request admission is bounded at the HTTP layer and returns `503 SlowDown` on
  admission timeout
- storage RPC admission/resource exhaustion maps to `503 SlowDown`
- metadata command contention, stale write-command races, and exhausted
  request-local metadata retry budgets map to `409 OperationAborted`
- request-local retry loops use bounded `RequestWorkBudget` values and jittered
  metadata contention backoff instead of unbounded polling
- background workers keep durable rows/claims as the retry source of truth and
  back off/retry later instead of forcing foreground requests to wait forever
- ambiguous post-side-effect outcomes must fail closed or complete cleanup
  before returning a retryable response

The broader remaining question is no longer "should we introduce retry
semantics?" It is production capacity policy: which resources should have
foreground/background reservations, how limits should be tuned, and whether
adaptive admission is justified. That work is tracked by
`plans/production-backpressure-plan.md`.

## Context

AWS S3 documents that some requests may fail with retryable `5xx` responses,
including `500 InternalError` and `503 SlowDown` / service-unavailable style
responses, and clients are expected to retry them.

We should have an explicit project position on when Argmin should:

- continue waiting internally for a short period
- fail immediately with a non-retryable semantic error
- return a retryable `5xx` so the client can retry against a better-defined
  later state

This is especially relevant for coordination boundaries where the server may
wait briefly on in-flight state transitions rather than on user payload transfer.

## Motivation

Some request paths can be delayed by internal coordination state.

One concrete example is bucket write-reservation / delete coordination:

- a fresh request may arrive while `DeleteBucket` has started draining writes
- after recent cleanup, the request no longer waits once the bucket becomes
  terminal (`Deleting` / not found)
- during transient-drain phases, the request-local budget decides whether to
  continue local recovery/drain work or return typed contention

That remaining wait is metadata/control-plane coordination, not payload IO. The
current rule is bounded local work first, then a typed retryable or
contention-shaped response rather than an unbounded in-process wait.

## Goals

- identify server-side wait sites where retryable failure may be better than
  long in-process waiting
- distinguish retryable coordination delays from true semantic failures
- define a principled mapping from internal wait states to S3-compatible
  externally visible behavior
- add measurement so any future threshold is based on real latency

## Non-Goals

- changing request behavior immediately without measurement
- using retryable `5xx` to mask correctness bugs or non-conforming semantics
- introducing broad generic retries without understanding each wait site

## Candidate Sites

Initial candidates examined:

- bucket write-reservation waits during transient bucket drain/delete
- any remaining request-path waits on internal metadata or lock coordination
- internal operations that currently poll on short sleeps and may outlive a
  sensible request budget

## Questions To Answer

1. Which internal wait sites are visible on foreground request paths? Covered by
   the Phase 10.9 unbounded-request-work audit.
2. For each site, what state is actually unresolved while waiting? Captured in
   typed metadata-command contention, stale command, drain, and storage-RPC
   overload errors.
3. When a request waits "too long", what result is most faithful to S3? Use
   bounded `OperationAborted` for expected metadata contention and `SlowDown`
   for capacity/resource exhaustion.
4. Should the retryable response be `500`, `503`, or differ by site? Expected
   contention and overload should not surface as generic HTTP 500.
5. What bounded local wait, if any, should occur before switching to a
   retryable response? Request-local budgets and storage admission timeouts own
   this per path.
6. How should we ensure that these responses remain observable and measurable
   rather than silently becoming normal control flow? Metrics/diagnostics now
   cover request admission, storage RPC admission, metadata-command budget
   exhaustion, and metadata-command backoff.

## Completed Work

1. Inventory request-path waits and classify them:
   - semantic wait
   - coordination wait
   - payload/streaming wait
2. Add lightweight measurement around candidate coordination waits so we know:
   - frequency
   - median / tail duration
   - whether they typically resolve quickly or can remain open-ended
3. For each candidate, decide:
   - always wait
   - always fail immediately
   - wait briefly, then return retryable `5xx`
4. Add focused tests for any adopted behavior so retry semantics are explicit
   and do not regress accidentally.

The implementation split the responses more specifically than the original
`5xx` wording:

- expected metadata-command contention returns `OperationAborted`
- capacity/resource exhaustion returns `SlowDown`
- semantic failures keep their normal non-retryable S3 shape
- unknown/ambiguous post-side-effect outcomes fail closed with diagnostics

## Relationship To Other Plans

- `plans/completed/deterministic-test-and-sleep-cleanup-plan.md`
  This plan identifies and removes timing-based waiting patterns. That work may
  reveal request paths where the right long-term behavior is not “better local
  waiting” but “bounded wait then retryable failure”.

- `plans/completed/coordinator-storage-capability-plan.md`
  As more coordination moves cleanly behind storage-owned boundaries, retry
  decisions should be made with a clear understanding of which layer owns the
  unresolved state.

- `plans/production-backpressure-plan.md`
  Owns the remaining production policy work: capacity resources, side-effect
  aware overload, foreground/background separation, read/list separation, and
  adaptive admission.

## Current Position

Current working position:

- no foreground request path should rely on unbounded coordination waiting
- expected metadata contention should be bounded and surfaced as
  `OperationAborted`
- capacity/resource exhaustion should be bounded and surfaced as `SlowDown`
- production tuning and adaptive policy belong in the production backpressure
  plan, not this older retry semantics note
