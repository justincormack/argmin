# Retry Semantics Plan

## Context

AWS S3 documents that some requests may fail with retryable `5xx` responses,
including `500 InternalError` and `503 SlowDown` / service-unavailable style
responses, and clients are expected to retry them.

We should have an explicit project position on when Argmin should:

- continue waiting internally for a short period
- fail immediately with a non-retryable semantic error
- return a retryable `5xx` so the client can retry against a better-defined
  later state

This is especially relevant for coordination boundaries where the server is
waiting on in-flight state transitions rather than on user payload transfer.

## Motivation

Some request paths are currently blocked by internal coordination state.

One concrete example is bucket write-reservation / delete coordination:

- a fresh request may arrive while `DeleteBucket` has started draining writes
- after recent cleanup, the request no longer waits once the bucket becomes
  terminal (`Deleting` / not found)
- but it may still wait during the transient-drain phase while the bucket is
  still `Active`

That remaining wait is metadata/control-plane coordination, not payload IO.
If it exceeds a small bounded period, it may be better to return a retryable
`5xx` and let the client retry, since by the time of retry the bucket state is
more likely to be determined.

We do not yet know the right threshold or response shape for those cases, so
this should be treated as a design/planning area rather than an immediate
behavior change.

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

Initial candidates to examine:

- bucket write-reservation waits during transient bucket drain/delete
- any remaining request-path waits on internal metadata or lock coordination
- internal operations that currently poll on short sleeps and may outlive a
  sensible request budget

## Questions To Answer

1. Which internal wait sites are visible on foreground request paths?
2. For each site, what state is actually unresolved while waiting?
3. When a request waits “too long”, what result is most faithful to S3?
4. Should the retryable response be `500`, `503`, or differ by site?
5. What bounded local wait, if any, should occur before switching to a
   retryable response?
6. How should we ensure that these responses remain observable and measurable
   rather than silently becoming normal control flow?

## Proposed Work

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

## Relationship To Other Plans

- `plans/completed/deterministic-test-and-sleep-cleanup-plan.md`
  This plan identifies and removes timing-based waiting patterns. That work may
  reveal request paths where the right long-term behavior is not “better local
  waiting” but “bounded wait then retryable failure”.

- `plans/coordinator-storage-capability-plan.md`
  As more coordination moves cleanly behind storage-owned boundaries, retry
  decisions should be made with a clear understanding of which layer owns the
  unresolved state.

## Current Position

Current working position:

- do not change general retry semantics yet
- keep the recent bucket-drain improvement that stops request-path waiting once
  delete is terminal
- treat “transient coordination wait might be better as retryable `5xx` after a
  short budget” as an explicit future design question
