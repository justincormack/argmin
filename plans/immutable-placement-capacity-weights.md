# Immutable Placement Capacity Weights

Status: proposed

## Goal

Allow a static cluster manifest to describe unequal storage capacity so initial
PG acting sets place more shards on larger storage nodes. Capacity weights are
immutable after bootstrap. Changing capacity later remains part of the separate
topology-resize and acting-set migration work.

The placement crate already accepts a nonnegative `NodeInfo.weight` and uses
deterministic weighted rendezvous scores, but both production adapters currently
construct every node with weight `1.0`. The control plane stores the resulting
acting sets, not the inputs used to calculate them.

## Configuration Contract

- Add a required `capacity_weight` to each `[[storage_nodes]]` record. Its
  manifest type is a nonzero `u32`, so the accepted range is
  `1..=4_294_967_295`.
- Define it as relative allocatable capacity for that storage node, not raw disk
  bytes. For example, usable capacities of 4 TB, 8 TB, and 8 TB can use weights
  `1`, `2`, and `2`.
- Deserialize the TOML integer as `NonZeroU32`, which rejects zero and values
  above the range, and encode its value canonically as `u32`. Every accepted
  value is exactly representable as `f64`; perform that exact conversion only
  when constructing `placement::NodeInfo`. Do not expose arbitrary
  floating-point configuration.
- Require standalone `capacity_weight = 1` because standalone placement has no
  choice.
- Include the weight in the cluster topology digest and process identity where
  applicable. An established deployment must reject a manifest whose weight was
  edited in place.
- Use the weight only while deriving and certifying the deterministic bootstrap
  acting sets. The bootstrap-map digest continues to bind the resulting node
  endpoints and acting sets.

## Placement Semantics

Weights affect how frequently an eligible failure domain is selected across
PGs. They do not weaken the one-shard-per-selected-domain rule.

If the number of eligible domains is exactly `k + m`, every PG must use every
domain and weights have no effect. Weighted placement becomes useful once at
least one additional domain exists. A production-shaped deployment should have
at least `k + m + failure_tolerance` eligible domains so it retains replacement
capacity after the declared failures. For EC 2+1 with one-host tolerance, this
means at least four hosts. Exact-fit configurations remain valid for minimal
deployments and tests, but validation and documentation must make their lack of
placement choice explicit.

A domain can receive at most one of a PG's `k + m` shards. Before exposing the
field, verify the current weighted top-k algorithm against an independent
reference model. If requested capacity shares cannot be represented within
that cap, validation must reject them or the implementation must calibrate the
selection weights. Do not claim raw `weight / total_weight` utilization unless
the multi-shard constrained distribution proves it.

## Implementation

1. Extend manifest parsing, canonical encoding, debug redaction, examples, and
   the multihost manifest generator with `capacity_weight`.
2. Pass validated weights into `validate_initial_pg_placement()` instead of
   hardcoding `1.0`.
3. Keep ordinary local/environment topology at weight `1`; it has no static
   capacity model.
4. Add diagnostics showing configured weight and initial PG counts per storage
   node so operators can inspect the realized distribution before startup.
5. Document that PG count controls distribution granularity and that immutable
   bootstrap weights do not trigger later rebalancing.

## Verification

- stable canonical-digest vectors and collection-order independence;
- rejection of zero, overflow, non-integer, and in-place post-bootstrap changes;
- exact-fit `k + m` topology proving unequal weights do not change acting sets;
- `k + m + 1` and larger topologies compared with an independent weighted
  sampling reference under disk and host constraints;
- scale invariance for proportionally equal integer weights;
- deterministic vectors across supported architectures;
- statistical PG-count tests for equal and unequal capacities, including the
  documented domain-cap limit; and
- bootstrap/restart tests proving the certified acting sets remain unchanged
  and no runtime path silently recomputes them with unit weights.

## Out Of Scope

- online reweighting;
- automatic capacity discovery from the filesystem;
- node addition, removal, or disk replacement; and
- automatic acting-set migration or data balancing after bootstrap.
