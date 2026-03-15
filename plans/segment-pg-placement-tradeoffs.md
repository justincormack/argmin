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

## Option 3: Bounded Object-Local PG Set

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

### Bounded PG set

- one object spreads load, but only within a limited envelope
- cluster interference is bounded
- traffic remains easier to reason about

## Recommendation

Use a bounded object-local PG set.

Working recommendation:

1. keep a configurable object-local PG-set width
2. start with a small width such as `4`
3. derive the PG subset deterministically from object identity
4. place segments over that subset by `segment_index`

Important rule:

- fan-out for one object should not scale with total PG count

That is the key reason to avoid full independent placement as the default.

## Placement Function Constraints

Any concrete placement function should preserve these invariants:

1. all segments of one object generation map to a deterministic PG subset
2. the subset is stable for that object generation
3. reclaim identity is still rooted at object generation, not per-segment placement
4. metadata placement remains unchanged
5. request fan-out is bounded by the configured subset width

## Practical Guidance

If we want the simplest first implementation:

- width `1` is acceptable as a temporary choice

But it should be treated as an explicit simplification, not the intended steady
state.

If we want a more future-proof default:

- width `4` is a reasonable first target

It is small enough to keep fan-out bounded and large enough to avoid the worst
single-PG hot-spot behavior.

## Deferred Questions

These do not need to be decided now:

1. whether segment-to-PG mapping inside the subset should be round-robin or hashed
2. whether width should vary by object size
3. whether multipart parts should preserve stronger locality within the same subset
4. whether future parallel read/write scheduling should align explicitly with the
   chosen subset width

## Current Conclusion

This is not an immediate implementation priority, but the design direction should be:

- do not scatter one object generation across all PGs by default
- do not assume one PG per object is the long-term answer
- prefer a bounded deterministic PG subset per object generation
