<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# RUST MEMORY POLICY --- STRICT MODE

*For Bounded, Backpressure-Driven Systems*

Status: strict policy target. This guide describes the intended memory model
and review bar for the codebase; it is not a claim that every current path
already satisfies every rule here.

------------------------------------------------------------------------

## GLOBAL INVARIANTS

1.  Memory usage must be bounded.
2.  No unbounded buffering is allowed.
3.  Allocation in hot paths is forbidden unless explicitly budgeted.
4.  Allocation failure must result in backpressure or graceful
    degradation.
5.  OOM and panic due to allocation are correctness bugs.

------------------------------------------------------------------------

# 1. ALLOCATION ZONES

## ZONE_INIT

-   Allocation allowed.
-   Pre-size long-lived data structures.
-   Build caches and pools.
-   No unbounded growth after initialization completes.

## ZONE_HOT (Requests / Queries / Operators)

-   No implicit allocation.
-   No collection growth without permit.
-   No per-item heap allocation.
-   No unbounded channels.
-   Must tolerate allocation failure.

## ZONE_BACKGROUND

-   Allocation allowed but must use separate budget.
-   Must yield under memory pressure.
-   Must not starve ZONE_HOT.

------------------------------------------------------------------------

# 2. TYPE RULES

## HOT PATH --- ALLOWED TYPES

### Views

-   `&[u8]`
-   `&str`
-   `&[T]`
-   `Bytes`

### Collections (bounded only)

-   `SmallVec`
-   `ArrayVec`
-   Pre-sized `Vec`
-   Pre-sized `HashMap`
-   Sorted `Vec<(K,V)>` for small maps

### Identifiers

-   Interned ID types
-   Index-based handles
-   Offsets instead of pointers when possible

------------------------------------------------------------------------

## HOT PATH --- FORBIDDEN WITHOUT PERMIT

-   `String`
-   `Vec::push` that may grow
-   `HashMap::insert` that may grow
-   `Box`
-   `Arc`
-   `format!`
-   `.to_string()`
-   `.collect::<Vec<_>>()`
-   Unbounded async channels

------------------------------------------------------------------------

# 3. COLLECTION RULES

1.  Always use `with_capacity`.
2.  Always call `try_reserve` before bulk insertion.
3.  Never rely on automatic resizing.
4.  Small collections (\<128 items) must use flat storage before using
    HashMap.
5.  Per-item allocation inside loops is forbidden.

------------------------------------------------------------------------

# 4. PERMIT SYSTEM

All dynamic memory growth requires a permit.

## PERMIT TYPES

-   `BytesPermit`
-   `ItemsPermit`
-   `WorkPermit`
-   `IngressPermit`

## PERMIT RULES

1.  Permit must be acquired before allocation.
2.  Permit must release on drop.
3.  Allocation without permit is a bug.
4.  Permit exhaustion must propagate backpressure.
5.  Permits must never be leaked.

------------------------------------------------------------------------

# 5. QUEUES AND CONCURRENCY

1.  All queues must be bounded.
2.  All channels must have explicit capacity.
3.  All worker pools must have explicit concurrency limits.
4.  Backpressure must propagate to ingress.
5.  No stage may buffer unboundedly.

------------------------------------------------------------------------

# 6. DATABASE-SPECIFIC RULES

## QUERY MEMORY

1.  Each query has a hard memory budget.
2.  Operators must track memory usage.
3.  On budget exhaustion, operators must:
    -   Spill to disk, or
    -   Switch algorithm, or
    -   Abort with controlled error.

## CACHES

1.  All caches require hard caps.
2.  All caches must support eviction.
3.  All caches must shrink under pressure.
4.  No implicit memoization.

------------------------------------------------------------------------

# 7. LOGGING RULES (HOT PATH)

1.  No heap allocation in logging.
2.  No `format!` in hot modules.
3.  Use structured logging fields.
4.  Pre-allocate logging buffers.

------------------------------------------------------------------------

# 8. ALLOCATION FAILURE POLICY

On allocation failure:

-   Return 429 / 503
-   Apply retry-after
-   Spill to disk
-   Shed load
-   Degrade algorithm

Never panic. Never crash. Never allow uncontrolled memory growth.

------------------------------------------------------------------------

# 9. CI ENFORCEMENT

1.  Add allocation-count tests for hot paths.
2.  Add failure-injection allocator tests.
3.  Reject code introducing unbounded channels.
4.  Reject code introducing implicit hot-path allocation.
5.  Track peak RSS and allocation counts in benchmarks.

------------------------------------------------------------------------

# 10. ANTI-PATTERNS

-   "Just buffer it"
-   Growing Vec in request loop
-   HashMap without capacity
-   Async channel without limit
-   Logging that allocates
-   Background task with no budget
-   Memory growth as flow control

------------------------------------------------------------------------

# SYSTEM PRINCIPLE

Memory is a constrained resource.

Every byte must be:

-   Bounded
-   Accounted for
-   Observable
-   Releasable

If memory pressure increases, the system must slow down --- not grow.
