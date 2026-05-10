# Temporary Write Availability

This guide records the current write-availability decision for transient node
failures during the multihost transition.

## Decision

Use strict writes for now.

Do not acknowledge a write unless all required metadata replicas and all
`k + m` payload shards for the chosen placement view have committed. Degraded
reads remain allowed where the PG state permits reads and at least `k` valid
payload shards are reachable.

Do not implement degraded writes as the default policy. A degraded write that
commits with a missing shard reduces effective parity until repair completes.
That changes the operational durability envelope and tends to imply
recommending extra parity globally just to cover temporary unavailability. That
is not the default product shape we want.

The preferred future policy for temporary write availability is handoff writes,
modelled as deterministic placement under a temporary availability overlay.

## Availability States

Keep temporary availability separate from durable membership changes:

- `active`: node is part of the placement set and can serve reads/writes.
- `transient down`: node is temporarily unreachable or unhealthy, but not
  removed from durable membership.
- `planned unavailable`: node is intentionally unavailable for maintenance, but
  expected to return without rebalancing the whole cluster.
- `out`: node is considered permanently unavailable or removed from durable
  membership; durable placement must eventually stop targeting it.

The exact timeout for moving from temporary unavailable to `out` is an
operational policy. It should be much longer than a short write retry window,
for example a maintenance or outage grace interval, because durable membership
changes can trigger expensive repair/rebalancing work.

## Strict Writes Now

Strict writes are the current implementation policy because they keep committed
metadata and payload state simple:

- committed payload generations always have the full intended shard count
- repair does not need to reason about acknowledged missing shards
- object durability does not vary silently with transient node health
- tests can fail closed when a target write node is unavailable

Strict writes lower write availability while a target node is temporarily
unavailable. That is acceptable until the handoff model below is designed and
implemented.

## Why Not Degraded Writes

Degraded writes allow success with fewer than `k + m` shards as long as at least
`k` shards are present. This is attractive for write availability but has a
different safety model:

- each affected object has lower failure tolerance until repair finishes
- a second failure before repair can lose newly acknowledged writes
- metadata must track per-shard missing/present state for committed generations
- repair becomes urgent for durability, not just convergence
- operators may need to add parity everywhere to compensate for transient
  degraded writes

Some fixed-placement systems accept this tradeoff. We should not make it the
default for Argmin unless we explicitly choose that product and operational
profile.

## Handoff Writes Later

Handoff writes should not be represented as arbitrary per-object shard-location
exceptions. Instead, derive placement from a temporary availability view:

- base cluster map generation `G` describes durable membership, weights, and
  topology
- availability generation `A` describes a temporary exclusion set, such as node
  `N` being unavailable
- the handoff placement for a payload is derived deterministically from
  `(G, A, object identity, segment identity, shard index)`
- committed object metadata records the placement view used for that payload
  generation, not a free-form list of handoff node IDs

This keeps shard locations deterministic. Reads and repair can recompute the
actual shard locations from the recorded placement view.

If the unavailable node returns, repair can migrate the handoff shards back to
the base placement view. If the node is later marked `out`, the same excluded
node placement may become the durable placement under a later cluster map
generation, avoiding a second conceptual model.

## Metadata Requirements

Before handoff writes can be implemented, metadata must be able to represent:

- the durable cluster map generation used by a committed payload generation
- the temporary availability generation or placement view used by that payload
- whether the placement view is still temporary, repaired back to base, or
  superseded by a later durable map generation
- enough information for read, delete, reclaim, and repair to recompute shard
  locations without guessing

Payload bytes are not part of the metadata command log. The command log records
metadata rows and payload identity/checksum references, while payload shard
files remain outside the log.

## Repair Semantics

Repair for handoff writes must be idempotent:

- a handoff payload with all `k + m` shards committed is readable immediately
- if the temporary node returns, repair may copy the handoff shard back to the
  base placement and then update metadata to the repaired placement view
- if the temporary node becomes permanently `out`, repair may converge the
  durable cluster map so the handoff placement becomes normal placement
- reads must handle old and new placement views during migration
- deletes and reclaim must delete every shard implied by the recorded placement
  view, not only the current base placement

## Phase 8 Scope

Phase 8 should not implement full handoff writes unless the preceding placement
and repair structure is ready.

The immediate Phase 8 work is:

- document strict writes as the implemented policy
- make transient write target unavailability fail closed
- keep degraded reads where EC reconstruction is safe
- define the placement-view model well enough that later migration/repair work
  does not block handoff writes
- add tests for strict-write failure under temporary target-node outage

Handoff writes are future work. Degraded writes remain a possible explicit
policy choice, but not the default.
