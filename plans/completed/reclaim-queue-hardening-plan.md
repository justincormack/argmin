# Reclaim Queue Hardening Plan

## Context

`security/codex-ecef3a4` describes a read-amplified denial of service in the
background payload reclaim path.

The current model uses generation-scoped read leases so old payload shards are
not reclaimed while an in-flight read still depends on them. That part is
correct.

The bug is in how reclamation is retried:

- reads acquire a payload lease for the generation they stream
- the final lease drop always enqueues reclaim work for that generation
- the reclaim worker later checks whether any durable reclaim record exists
- if no reclaim record exists, the worker returns without doing useful work

At the time of the finding, this meant normal reads could feed the in-memory
reclaim queue even when there was nothing pending reclaim. The queue was also
structurally unbounded on the affected path, so high-cardinality traffic could
grow it without limit.

## Status

Completed / superseded as an active hardening plan.

Phase 1 was completed in commit `1c6e141`.

Implemented:

- added an exact metadata predicate for whether a generation is still pending
  reclaim
- changed `PayloadLease::drop` to re-enqueue only when durable reclaim state
  still exists for that generation
- added focused regressions for:
  - no enqueue on final read lease drop when no reclaim record exists
  - correct retry after an earlier reclaim attempt was skipped because a lease
    was still active

This closes the read-amplified behavior described in
`security/codex-ecef3a4`.

The original Phase 2 note is now stale for the active coordinator path. Later
work added reclaim-queue observability, object-payload queue deduplication, a
per-PG cap on outstanding object-payload reclaim work, deferred durable-scan
retry, and worker-side cooldown/backoff. The current production coordinator
path runs reclaim through `StorageCluster` / `LocalClusterRuntimeState`, where
object-payload reclaim enqueue returns `PgCapacityDeferred` instead of growing
the in-memory queue without bound.

There are still broader background-capacity questions, but they are no longer
specific to the `security/codex-ecef3a4` read-amplification finding:

- bucket-delete finalization work is deduplicated by bucket, but not governed
  by the same per-PG object-payload outstanding cap
- physical delete pacing, background worker concurrency, and
  foreground/background reservations belong in
  `plans/background-worker-throttling-plan.md` and
  `plans/production-backpressure-plan.md`
- the older direct `SharedStorageNode` queue still has only root/bucket
  deduplication; it should not be treated as the production reclaim admission
  policy without either porting the local-runtime cap or removing that queue
  surface

## Goals

- remove the read-amplified queue growth described in
  `security/codex-ecef3a4`
- preserve correct deferred reclamation semantics for overwritten or deleted
  generations that still have active readers
- keep the design open for later background-worker capacity policy

## Non-Goals

- changing visible S3 behavior
- redesigning payload leasing
- solving every possible reclaim-queue pressure case in the first patch

## Phase 1: Gate Read-Side Re-Enqueue on Pending Reclaim

This is the immediate fix for `security/codex-ecef3a4`.

Status: completed in commit `1c6e141`.

### Design

Change the final lease-drop path so it only enqueues reclaim work when the
generation is actually pending reclaim in metadata.

That means:

- keep creating durable reclaim records on delete / overwrite paths as today
- keep allowing those paths to enqueue reclaim immediately
- when the worker observes an active lease and gives up for now, rely on the
  final lease drop to retry
- but only retry if a reclaim record still exists for that generation

### Why This Is Sufficient for the Finding

The security issue is specifically that ordinary reads of non-pending
generations can manufacture queue entries. If the final lease drop is gated on
durable reclaim state, plain reads no longer create reclaim work.

Delete and overwrite paths still enqueue the generations that really do need
reclamation, which preserves correctness.

### Implementation Outline

1. Add a storage helper that answers whether a given
   `(bucket, key, generation_id)` has any reclaim record present.
2. Use that helper from the `PayloadLease::drop` path before enqueuing.
3. Leave the reclaim worker behavior unchanged for the first phase.
4. Add narrow regression tests around the lease-drop retry behavior.

### Regression Coverage

Add at least:

1. a coordinator or storage-adjacent test proving a generation with no reclaim
   record does not get enqueued when its last read lease drops
2. a regression showing the intended retry still works:
   - reclaim is queued while a lease is active
   - worker observes the lease and returns without reclaiming
   - final lease drop sees the still-pending reclaim record
   - generation is re-enqueued and reclamation completes

## Phase 2: Queue Backpressure and Bounded Memory

This is follow-on hardening, not required to resolve
`security/codex-ecef3a4`.

Status: completed for object-payload reclaim on the active `StorageCluster`
path; superseded for broader background capacity policy.

Delete-heavy or overwrite-heavy workloads can still produce many legitimate
durable reclaim roots, but the active in-memory object-payload reclaim queue is
no longer an unbounded mirror of those roots. Durable metadata remains the source
of truth. The worker scans durable roots, enqueues only up to the per-PG
outstanding capacity, and retries deferred roots later.

Implemented shape:

- `LocalReclaimQueueState` tracks queued object roots, outstanding object roots,
  outstanding object roots by PG, and queued bucket-delete finalization roots
- `OBJECT_PAYLOAD_RECLAIM_MAX_OUTSTANDING_PER_PG` bounds active
  object-payload reclaim work per object metadata PG
- object-payload enqueue can return `Queued`, `Deduplicated`, or
  `PgCapacityDeferred`
- durable reclaim scans treat `PgCapacityDeferred` as a normal deferred outcome
  and keep durable metadata as the retry source
- reclaim workers call `finish_object_payload_reclaim_work` after terminal
  object-payload outcomes to release capacity
- reclaim queue observability exports total queue depth, object-payload queued
  depth, object-payload outstanding depth, and bucket-delete finalization depth

### Remaining Non-Security Follow-Up

The remaining work is not specific to this security finding:

1. decide whether bucket-delete finalization needs its own explicit admission
   policy beyond per-bucket deduplication
2. tune reclaim worker concurrency, physical delete pacing, and cooldown policy
   from measured production-style workloads
3. decide whether the direct `SharedStorageNode` queue should gain the same
   bounded policy or be removed from production-facing reclaim paths

### Design Constraints

- do not lose reclaim work silently
- do not block indefinitely while holding unrelated metadata locks
- preserve eventual cleanup after crashes via durable reclaim records
- keep queue pressure visible in tests and observability

### Validation

Current coverage includes at least:

1. object-payload queue capacity counts dequeued work as outstanding until the
   reclaim worker finishes it
2. additional object-payload enqueue attempts for the same PG return
   `PgCapacityDeferred` until capacity is released
3. reclaim trace/property tests cover deferred object-payload reclaim, bucket
   delete finalization hints, and mixed reclaim ordering

## Recommended Order

1. keep this plan as the historical record for `security/codex-ecef3a4`
2. use `plans/background-worker-throttling-plan.md` and
   `plans/production-backpressure-plan.md` for future reclaim throughput,
   physical delete pacing, and foreground/background capacity policy
