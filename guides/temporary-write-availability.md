<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

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

The preferred availability policy is committed PG placement onto an eligible
spare after a bounded failure grace period. This preserves strict writes: the
cluster changes the current placement before acknowledging new writes rather
than committing payloads with missing shards or inventing per-object handoff
locations.

## Availability States

Keep temporary availability separate from durable membership changes:

- `active`: node is part of the placement set and can serve reads/writes.
- `transient down`: node is temporarily unreachable or unhealthy, but not
  removed from durable membership.
- `planned unavailable`: node is intentionally fenced for maintenance under one
  of the explicit maintenance modes below.
- `out`: node is considered permanently unavailable or removed from durable
  membership; durable placement must eventually stop targeting it.

Temporary availability, PG placement, and durable membership are separate.
After lease expiry and a configured grace period, the control plane may replace
an unavailable actor in affected PGs while the node remains a durable member.
Moving the node itself to `out` is a separate, slower operator or policy
decision for permanent loss.

## Strict Writes Now

Strict writes are the current implementation policy because they keep committed
metadata and payload state simple:

- committed payload generations always have the full intended shard count
- repair does not need to reason about acknowledged missing shards
- object durability does not vary silently with transient node health
- tests can fail closed when a target write node is unavailable

Strict writes lower write availability while a target node is temporarily
unavailable and before a safe spare placement becomes active. If no eligible
spare can provide all `k + m` destinations, affected writes remain retryable
rather than weakening durability.

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

## Automatic Spare Placement

Temporary write recovery uses the same committed PG acting-set model as other
placement changes:

- committed topology generation `G` defines eligible nodes, failure domains,
  and immutable placement weights;
- one exact availability observation identifies the unavailable node identity,
  incarnation, accepted lease deadline, and authority-clock grace cutoff;
- one control-plane compare-and-swap verifies `G`, the source PG epoch/state and
  acting set, the availability observation, absence of an unexpired maintenance
  suppression, and destination eligibility;
- that transaction durably records an idempotent transition and fences the
  source PG into `Peering`; and
- the destination PG becomes `Active` only after its operation-specific safety
  proof succeeds.

Discovery and queueing before the compare-and-swap are cancellable. If the same
incarnation renews or topology changes first, the queued proposal is discarded
and recomputed. The committed compare-and-swap is the fail-forward boundary: a
later renewal cannot cancel the transition or restore the old acting set.

Every PG can hold metadata and can be selected for payload placement. It remains
`Peering` until both requirements hold: the exact reconstructed metadata proof
is present on all required destination replicas, and every destination actor
holds a current lease and can accept its assigned payload shard under that
route. Once `Active`, every acknowledged new payload must still commit all
`k + m` shards. Historical payload remains readable through authenticated route
history and is backfilled asynchronously; activation does not wait for all
stored data to move.

This is availability repair, not automatic failback or capacity balancing. A
returned node becomes eligible only after its incarnation and local evidence
are validated. Existing recovered PGs remain on their current acting sets until
an explicit rebalance or operator transition moves them, except for the
maintenance-bound restoration defined below.

## Planned Maintenance

Planned maintenance starts with a durable record containing a unique
maintenance identity, topology generation and digest, stable node identity,
departing incarnation, mode, authority-clock start/expiry, affected-PG source
roots and progress cursor, and operator authorization. It has two modes:

- `migrate-before-stop` is the default when write availability is required. The
  record authorizes a separate healthy-source compare-and-swap; it does not
  fabricate lease expiry. The operator prepares replacement acting sets while
  the source is healthy and receives a durable safe-to-stop result only after
  every destination is `Active`, no current route requires the node, and
  retained-placement evidence proves historical payload keeps at least `k`
  readable shards without it. Completion does not restore these placements.
- `bounded-no-migration` retains current acting sets for a declared short
  interval. Taking the node down during that interval knowingly makes affected
  strict writes retryable. The failure-driven CAS must reject the exact
  departing incarnation while this suppression remains unexpired. A validated
  return before expiry clears the record with no placement movement. Expiry
  enters ordinary automatic spare-placement reconciliation.

If an expired bounded window has already crossed the outage transition's
irreversible boundary, that transition finishes forward even when the node
returns. Validation records the exact returning incarnation, authenticated
endpoint identity, accepted live lease observation/deadline, and local
PG-evidence generation/digest. The maintenance record then authorizes at most
one controlled restoration toward its recorded pre-maintenance acting set for
each PG.

The restoration CAS atomically revalidates that exact return observation and
requires the accepted lease to remain live. It also requires the current PG to
be the unsuperseded terminal tip of the maintenance outage-transition lineage.
Any later operator intent or unrelated recovery transition permanently cancels
the old restoration authority for that PG, even under unchanged topology or if
the route later happens to match. A successful CAS consumes the authority and
fences restoration into `Peering`; restoration then uses the normal metadata
proof, new-write readiness, and route-history rules. This is explicit planned
maintenance intent, not general automatic failback. Neither mode can renew a
lease or weaken fencing.

## Metadata Requirements

Automatic spare placement requires durable metadata for:

- the topology generation and digest used to select the destination;
- the exact source PG epoch, state, acting set, and unavailable lease
  observation authorized by the transition;
- the destination acting set, unique transition identity, progress, and
  irreversible commit boundary;
- any planned-maintenance identity, departing incarnation, bounded suppression
  expiry, safe-to-stop result, outage-transition lineage and terminal tip,
  per-PG restoration authority/cancellation, exact returning incarnation and
  endpoint, accepted lease observation/deadline, local evidence
  generation/digest, and restoration progress;
- retained source and destination route history while historical payload,
  pending commands, reservations, cleanup, or repair still reference them; and
- enough placement-generation identity for read, delete, reclaim, and repair to
  locate every shard without guessing.

Payload bytes are not part of the metadata command log. The command log records
metadata rows and payload identity/checksum references, while payload shard
files remain outside the log.

## Historical Backfill Semantics

Backfill after spare activation must be idempotent and independently bounded:

- reads use the placement generation recorded for the payload and may EC
  reconstruct from any `k` validated historical shards;
- repair revalidates the exact current metadata reference before copying data;
- copied historical shards are verified before their destination is published;
- source route history remains retained until payload, cleanup, reclaim, and
  pending-command references have durably cleared;
- deletes and reclaim cover every retained placement generation, not only the
  current acting set; and
- a returning source does not redirect backfill or cause implicit failback;
  bounded-maintenance restoration remains a separately fenced placement
  transition with all intermediate history retained.

## Implementation Status

Strict writes and degraded EC reads are the current policy. Metadata transfer,
payload backfill, retained route history, and authenticated recovery provide the
underlying primitives. [Phase 3.3 of the multihost production follow-up
plan](../plans/multihost-followup-plan.md#33-automatic-unavailable-node-placement-reconciliation)
owns the durable controller that composes lease expiry, grace, spare selection,
transition fencing, activation, and paced historical backfill. Until that phase
is complete, transient target unavailability continues to fail writes closed
and operators must drive placement changes explicitly.
