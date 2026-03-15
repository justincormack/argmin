# Segment PG Placement Tradeoffs

## Context

The current segmented payload model stores explicit per-segment placement metadata,
so we are free to choose how widely one object generation spreads across data PGs.

This is separate from object metadata placement:

- object metadata still lives on the metadata PG derived from `(bucket, key)`
- this note is only about where the payload segments for one object generation land

The main design question is:

- should all segments for one object generation land on the same data PG
- should segments be independently distributed over all data PGs
- should segment placement be banded for better locality
- or should segments be spread over a bounded deterministic subset of PGs

## Goals

The placement choice should balance:

1. predictable request behavior
2. cluster-wide load balancing
3. single-object throughput potential
4. bounded fan-out and tail-latency exposure
5. operational simplicity

## Option 1: One Data PG Per Object Generation

All segments of one object generation are placed on the same data PG.

### Pros

- simplest mapping and easiest reasoning
- strong locality for sequential `PUT` and `GET`
- low cluster fan-out for one request
- easy to debug and reason about lock/traffic behavior
- one object stream only depends on one placement set

### Cons

- large objects can hot-spot one PG and its placement set
- single-object throughput is capped by one PG's backing nodes
- space distribution can skew badly if object sizes are heavy-tailed
- repair/recovery load for a large object is concentrated

### Assessment

This is the best choice for simplicity, but it is a poor long-term fit if large
objects are expected to dominate bytes stored or moved.

## Option 2: Fully Independent Segment Placement

Each segment chooses its data PG independently.

### Pros

- best global balance of bytes across PGs
- better distribution of large-object read/write pressure
- avoids one large object hot-spotting one PG
- potentially enables high single-object throughput if segment I/O is pipelined

### Cons

- one request can fan out widely across the cluster
- request tail latency becomes sensitive to many PGs instead of one
- more control-plane and lock/scheduling noise
- harder to debug and reason about operational behavior
- if read/write execution stays mostly serial, the extra fan-out cost is paid
  without getting much throughput benefit

### Assessment

This is attractive only if the implementation is explicitly designed to pipeline
work across many PGs. Otherwise it mostly adds noise and tail risk.

## Option 3: Banded Segment Placement

Placement is still derived from segment position, but consecutive runs of segments
share one placement decision.

Example:

- choose a band size `B`
- derive `band_index = segment_index / B`
- place all segments in that band on the same data PG

With the current `4 MiB` segment size:

- `B = 16` gives `64 MiB` locality bands
- `B = 32` gives `128 MiB` locality bands

### Pros

- very small change from per-segment placement
- materially improves sequential locality
- reduces PG transitions during large `PUT` and `GET`
- reduces lock, file, cache, and network context churn
- easy to tune with one constant

### Cons

- does not bound total fan-out for very large objects
- can still spread one large object over many PGs as object size grows
- weaker global balancing than fully independent placement

### Assessment

This is a good simple intermediate option.

It improves locality without forcing all data for one object generation onto one
PG, but it does not by itself provide the stronger long-term property of a bounded
object-local PG set.

## Option 4: Bounded Object-Local PG Set

Each object generation is assigned a small deterministic set of data PGs, and
its segments are spread across that set.

Example:

- choose an object-local PG-set width `W`
- derive the set deterministically from object identity
- map `segment_index` over that set, for example round-robin or hashed within
  the set

### Pros

- much better balance than one-PG-per-object
- much less fan-out than fully independent placement
- preserves locality and predictability
- gives room for future parallel segment scheduling
- request behavior does not scale with total PG count

### Cons

- more complex than width `1`
- not as perfectly balanced as full scatter
- still introduces some multi-PG coordination for one object

### Assessment

This is the best general tradeoff.

It avoids tying one object stream to one PG forever, but it also avoids making
one object stream look like a cluster-wide workload.

## Traffic Pattern Implications

This is the real tradeoff.

### One data PG per object

- sequential `PUT` and `GET` stay local to one placement set
- traffic is simpler and steadier
- hot objects are concentrated

### Fully independent placement

- one large object can become cluster-wide traffic
- load is spread well
- request behavior becomes noisier and more tail-sensitive

### Banded placement

- one object still spreads, but with longer local runs
- traffic is less noisy than per-segment placement
- distinct PG count can still grow with object size

### Bounded PG set

- one object spreads load, but only within a limited envelope
- cluster interference is bounded
- traffic remains easier to reason about

## Recommendation

Use a bounded object-local PG set.

Working recommendation:

1. keep a configurable object-local PG-set width
2. start with a small width such as `4`
3. also keep a configurable band size
4. derive the PG subset deterministically from object identity
5. place segments over that subset by `band_index`, not raw `segment_index`

Important rule:

- fan-out for one object should not scale with total PG count

That is the key reason to avoid full independent placement as the default.
Banding is still worthwhile even before subset bounding, but it should be understood
as a locality improvement, not the full solution.

## Placement Function Constraints

Any concrete placement function should preserve these invariants:

1. all segments of one object generation map to a deterministic PG subset
2. the subset is stable for that object generation
3. segments within one band share one PG choice
4. reclaim identity is still rooted at object generation, not per-segment placement
5. metadata placement remains unchanged
6. request fan-out is bounded by the configured subset width

## Practical Guidance

If we want the simplest first implementation:

- band placement alone is a reasonable first step
- `band_index = segment_index / 16` is a good initial default

That gives `64 MiB` locality runs with today's `4 MiB` segments and is a useful
improvement even if subset-bounded placement is deferred.

If we want a more future-proof default:

- width `4` is a reasonable first target
- pair it with a band size such as `16`

It is small enough to keep fan-out bounded and large enough to avoid the worst
single-PG hot-spot behavior.

## Deferred Questions

These do not need to be decided now:

1. whether segment-to-PG mapping inside the subset should be round-robin or hashed
2. whether width should vary by object size
3. whether band size should vary by object size or EC shape
4. whether multipart parts should preserve stronger locality within the same subset
5. whether future parallel read/write scheduling should align explicitly with the
   chosen subset width

## Current Conclusion

This is not an immediate implementation priority, but the design direction should be:

- do not scatter one object generation across all PGs by default
- do not assume one PG per object is the long-term answer
- treat banded placement as the simple locality improvement
- prefer a bounded deterministic PG subset per object generation
