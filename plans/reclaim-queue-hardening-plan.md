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

This means normal reads can feed the in-memory reclaim queue even when there is
nothing pending reclaim. The queue is also structurally unbounded today, so
high-cardinality traffic can grow it without limit.

## Goals

- remove the read-amplified queue growth described in
  `security/codex-ecef3a4`
- preserve correct deferred reclamation semantics for overwritten or deleted
  generations that still have active readers
- keep the design open for later queue backpressure work

## Non-Goals

- changing visible S3 behavior
- redesigning payload leasing
- solving every possible reclaim-queue pressure case in the first patch

## Phase 1: Gate Read-Side Re-Enqueue on Pending Reclaim

This is the immediate fix for `security/codex-ecef3a4`.

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

Even after phase 1, delete-heavy or overwrite-heavy workloads can still produce
many legitimate reclaim entries. The in-memory queue should eventually have an
explicit capacity policy.

### Candidate Directions

- bounded queue depth with producer backpressure
- separate policies for object reclaim and bucket-delete finalization
- overload behavior if the queue cannot drain within a reasonable bound
- metrics and tracing for queue depth, wait time, and dropped or delayed work

### Design Constraints

- do not lose reclaim work silently
- do not block indefinitely while holding unrelated metadata locks
- preserve eventual cleanup after crashes via durable reclaim records
- keep queue pressure visible in tests and observability

### Validation

When phase 2 starts, cover at least:

1. queue depth cannot grow without bound in memory
2. concurrent producers experience explicit backpressure rather than hidden
   allocation growth
3. reclaim workers still make forward progress under mixed delete, overwrite,
   and read traffic

## Recommended Order

1. implement phase 1 and close `security/codex-ecef3a4`
2. keep this plan open for phase 2 when reclaim throughput and overload policy
   are ready to be designed together
