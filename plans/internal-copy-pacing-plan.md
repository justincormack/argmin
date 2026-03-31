# Internal Copy Pacing Plan

## Problem

`CopyObject` and `UploadPartCopy` now use bounded-memory streaming paths and do
not hold long metadata locks, which is the right base behavior. However, once a
copy has snapshotted source metadata and started its chunk loop, it runs as fast
as local CPU and disk allow.

That differs from normal `PutObject` / `UploadPart` traffic, where the server is
naturally paced by client body arrival and socket backpressure. Internal copies
have no equivalent pacing point today.

The result is:

- one request slot stays occupied for the full lifetime of the copy
- one blocking worker stays occupied for the full lifetime of the copy
- the copy can drive shard reads, EC work, encryption, and shard writes at full
  local speed
- unrelated reads and writes can be slowed indirectly even though they are not
  blocked by a coarse lock

## Current State

- `CopyObject` and `UploadPartCopy` snapshot source metadata, build a
  `ReadHandle`, and then drop source metadata guards before streaming payload
  bytes
- source payloads stay alive via generation-scoped leases, not long PG lock
  holds
- copy transfer uses `INTERNAL_SEGMENT_SIZE` chunks and appends each segment to
  the destination staging session immediately
- the HTTP layer already has a global request semaphore and overloads with
  `SlowDown` when admission fails
- there is no dedicated admission control or byte budget for internal copy work
- there is no copy-specific fairness policy relative to non-copy reads/writes

## Why This Matters

This is not a correctness emergency:

- copy paths are bounded-memory
- copy paths do not hold source metadata locks for the full transfer
- concurrent overwrite/delete behavior already has coverage

But it is still an important performance and fairness gap:

- a small number of large copies can consume a disproportionate amount of local
  IO/CPU
- unrelated traffic can see higher tail latency or more `SlowDown` responses
- the server currently has no explicit policy for how much capacity internal
  copies are allowed to consume

This should be tracked now, but it does not need to preempt correctness work
unless copy-heavy workloads become part of normal testing or production use.

## Goals

- preserve normal synchronous S3 semantics for `CopyObject` and `UploadPartCopy`
- keep the existing short metadata lock windows
- bound the impact of internal copies on unrelated traffic
- use explicit permits/budgets rather than ad hoc sleeps
- cover both `CopyObject` and `UploadPartCopy`
- make pacing observable in traces and metrics

## Non-Goals

- exact emulation of AWS internal bandwidth behavior
- asynchronous background copy jobs or changed API semantics
- perfect multi-tenant fairness in the first iteration

## Options

### 1. Dedicated copy concurrency limit

Add a separate semaphore for internal copy requests, distinct from the existing
global request semaphore.

Pros:

- simplest change
- low correctness risk
- easy to reason about operationally
- gives immediate protection against many concurrent large copies

Cons:

- very coarse
- one tiny copy consumes the same slot as one very large copy
- does not smooth the bandwidth of a single copy

Placement:

- best at HTTP ingress so waiting copies do not occupy a blocking worker
- coordinator-side fallback is possible, but less efficient

### 2. Shared byte-rate limiter for internal copies

Introduce an explicit byte budget for internal copy traffic, consumed per
segment by `CopyObject` and `UploadPartCopy`.

A simple shape would be:

- one shared limiter across all internal copies
- budget consumed in `INTERNAL_SEGMENT_SIZE` units
- configurable refill rate and burst size
- optional wait timeout that converts prolonged contention into `SlowDown`

Pros:

- directly addresses the real issue: unpaced local read/write throughput
- scales more smoothly than a pure concurrency cap
- allows copies to make progress without letting them run flat-out

Cons:

- more design and testing work
- needs careful placement so waits do not create hidden queueing
- timeout and abort behavior must be well-defined once destination staging has
  begun

### 3. Adaptive pacing based on system pressure

Throttle copies only when the system is under pressure, for example when:

- request permits are low
- copy queue depth is non-zero
- observed copy wait times exceed a threshold

Pros:

- avoids throttling copies on an idle system
- can preserve peak copy speed when there is spare capacity

Cons:

- more moving parts
- harder to reason about, test, and tune
- should be follow-on work, not the first implementation

## Recommendation

Use a staged approach.

### Stage 1: observability

Add copy-specific visibility first:

- in-flight `CopyObject` count
- in-flight `UploadPartCopy` count
- total copied bytes
- copy duration
- time spent waiting on any pacing control
- copy aborts caused by pacing timeout

Without this, it is too easy to debate fairness problems without being able to
measure them.

### Stage 2: dedicated copy concurrency cap

Add a dedicated internal-copy semaphore as the first pacing control.

This is the smallest useful step because it:

- matches the existing explicit backpressure style in the HTTP layer
- avoids unbounded growth in copy pressure
- is easy to explain and operate

It should be treated as a future-work performance control, not as part of copy
correctness.

### Stage 3: optional byte-rate limiter

If the concurrency cap is too coarse, add a shared byte budget consumed per
segment by both internal copy APIs.

This should probably be layered on top of the concurrency cap rather than
replacing it.

### Stage 4: revisit adaptive behavior

Only after stages 1-3 exist and we have measurements should we consider
load-sensitive or adaptive policies.

## Implementation Notes

- avoid unbounded internal queues; waiting must be bounded and visible
- do not add pacing by sleeping blindly inside the copy loop
- if pacing waits too long after a destination staging session has started, the
  operation must abort cleanly and return a normal overload-style error
- any pacing mechanism should apply to both `CopyObject` and `UploadPartCopy`
  through a shared policy, not two separate ad hoc implementations

## Validation

When work starts, cover at least:

- unit tests for permit exhaustion and timeout behavior
- tests that paced copies still preserve current overwrite/delete race behavior
- mixed-workload tests with concurrent copies plus normal reads/writes
- benchmark runs comparing tail latency of unrelated traffic with and without
  paced internal copies

## Priority

Medium.

This is worth tracking because the current behavior has no fairness policy, but
the existing implementation is already correct on memory and lock scope. Unless
copy-heavy workloads are on the critical path right now, this should stay as
planned future work rather than immediate implementation.
