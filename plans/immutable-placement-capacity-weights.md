# Immutable Placement Capacity Weights

Status: proposed

## Goal

Allow a static cluster manifest to describe unequal storage-disk capacity so
initial PG acting sets select larger disks more frequently. Capacity weights
are immutable within a committed topology. Adding a disk and its initial
weight, or changing the capacity model later, remains part of the separate
topology-resize and acting-set migration work.

The placement crate already accepts a nonnegative `NodeInfo.weight` and uses
deterministic weighted rendezvous scores, but both production adapters currently
construct every node with weight `1.0`. The control plane stores the resulting
acting sets and a certified placement policy, but that policy does not yet
retain the inputs used to calculate weighted placement.

## Configuration Contract

- Add `capacity_weight` to each `[[disks]]` record used by a storage node. It is
  required for a storage-bearing disk and rejected on a disk that is not used
  for storage, so the field cannot silently become meaningless configuration.
  Its manifest type is a nonzero `u32`, with accepted range
  `1..=4_294_967_295`.
- Make the physical model explicit: every storage node has exactly one disk and
  every storage-bearing disk has exactly one storage node. Reject two storage
  nodes that reference the same disk. This was already the intended deployment
  architecture; making it an invariant prevents a disk's capacity from being
  counted once per process or data directory.
- Define the value as relative allocatable capacity for the disk, not raw disk
  bytes and not a promised utilization share. For example, usable capacities of
  4 TB, 8 TB, and 8 TB can use weights `1`, `2`, and `2`.
- Parse the TOML input as `Option<NonZeroU32>` only as needed to diagnose the
  conditional disk requirement. The validated manifest and storage-owned policy
  must use a non-optional bounded integer for every storage node. Do not allow an
  absent value to survive validation or introduce a default for old manifests.
- Encode the configured integer canonically as `u32`. Retain that integer in the
  storage-owned certified placement policy beside the node, host, and disk
  identity. The policy, rather than the manifest or process layer, is the sole
  production source for later placement, replacement, and rebalancing choices.
- Retain a storage-owned `placement_derivation_version: u16` in the same policy.
  Version 1 names the complete derivation contract below. Reject an unsupported
  derivation version when decoding or validating a policy; do not silently use
  the current algorithm for an unversioned or differently versioned policy.
  Include the marker in the certified topology's canonical bytes so the
  topology digest binds the algorithm that produced its acting sets.
- Require standalone `capacity_weight = 1` because standalone placement has no
  choice.
- Include the configured integer in the cluster topology, selected-process, and
  full-configuration encodings. Identities bind the configured integer, not a
  normalized or floating-point derivative. An established topology must reject
  an edited weight even when the edit is proportionally equivalent to all other
  weights and would currently produce the same acting sets.
- Existing disk weights are immutable within a topology lineage. A future
  topology-expansion transition may commit a new disk and its initial weight,
  but must carry the previously committed weights for existing disks unchanged.
- The bootstrap-map digest continues to bind only the resulting node endpoints
  and ordered acting sets. The enclosing certified topology and its topology
  digest separately bind the immutable placement policy and its weights.

## Placement Semantics

Weights are deterministic weighted-rendezvous selection parameters. They affect
how frequently an eligible disk, and therefore its storage node, is selected
across PGs. They are not requested utilization percentages and do not promise a
raw `weight / total_weight` shard share. Host-domain placement may select at most
one of a host's disks for a PG, so the constrained distribution also depends on
the other disks on that host. Weights do not weaken the one-shard-per-selected-
failure-domain rule.

If the number of eligible domains is exactly `k + m`, every PG must use every
domain and weights have no effect. Weighted placement becomes useful once at
least one additional domain exists. A production-shaped deployment should have
at least `k + m + failure_tolerance` eligible domains so it retains replacement
capacity after the declared failures. For EC 2+1 with one-host tolerance, this
means at least four hosts. Exact-fit configurations remain valid for minimal
deployments and tests, but validation and documentation must make their lack of
placement choice explicit.

A domain can receive at most one of a PG's `k + m` shards. This cap is part of
the placement contract, not a reason to reject a valid weight or calibrate it to
a requested share. Before exposing the field, verify the current weighted
top-k algorithm against an independent reference model under both disk and host
constraints. The reference must validate deterministic selection and the
one-domain cap; statistical tests may demonstrate the qualitative capacity
bias but must not turn it into an exact utilization guarantee.

Proportionally equal integer vectors must produce identical placement. Before
constructing `placement::NodeInfo`, divide the complete certified policy's
weights by their greatest common divisor and convert the resulting `u32` values
exactly to `f64`. Use that same policy normalization when considering a subset
of currently eligible nodes; do not renormalize according to transient
availability. The normalized values are ephemeral placement inputs and are
neither persisted nor included in identities.

Ordered acting sets are part of the certified result because their positions
select EC shard indices. The independent model and statistical checks do not
replace exact ordered acting-set vectors. Retain cross-architecture goldens that
would fail if selected membership stayed the same but node-to-shard ordering
changed.

Placement derivation version 1 covers the complete untagged algorithm, not just
the stored weights:

- the initial placement-key bytes: the existing
  `argmin-initial-pg-placement-v1` domain followed by the PG ID as a big-endian
  `u32`;
- canonical node ordering and conversion of node, host, and disk identities into
  placement topology;
- GCD normalization over every node in the complete committed policy and exact
  conversion of the resulting `u32` values to `f64`;
- the weighted rendezvous score formula and deterministic hash/logarithm
  implementation supplied by the placement owner;
- disk/host constraint admission, score and node-ID tie breaking, candidate
  replacement behavior, and final node-to-shard output ordering; and
- the eligible-spare ranking rule described below.

Bootstrap and later replacement use the same stable PG placement key. To choose
a replacement, storage computes the versioned ranking from the complete
committed policy, then walks that ranking while excluding the current acting
nodes, unavailable or otherwise ineligible nodes, and candidates whose disk or
host would violate the certified failure-domain cap. It selects the first
remaining candidate for each missing shard in deterministic shard-index order.
Availability, the source acting set, transition identity, topology generation,
and topology digest remain authenticated admission/CAS inputs, but are not hash
inputs that would unnecessarily reshuffle the rendezvous ranking. Filtering
occurs after full-policy normalization and scoring; a transient eligible subset
must never be renormalized or treated as a new placement policy.

An exact-fit cluster may therefore record unequal weights even though every PG
must initially use every disk. Those weights remain useful policy state: after
a separately authorized topology transition commits spare capacity, replacement
or rebalancing can rank the expanded eligible set without inventing or
recovering the original inputs.

## Versioned Format Transition

This is an incompatible manifest and durable-policy change. It must follow the
commit-boundary evidence rules in `guides/versioning.md`; a format must not be
changed and then use evidence created only in that same commit as proof of its
former current representation.

1. First land an evidence-only commit that freezes the current schema-1
   manifest behavior and current exact bytes or vectors for every affected
   format. Include the current unweighted certified bootstrap command and
   logical-state policy, plus exact ordered acting-set vectors for the currently
   untagged derivation, not only the static configuration digests.
2. In the following coordinated implementation commit, advance:
   - introduce independently identified placement derivation version 1 in the
     certified policy;
   - static manifest schema 1 to 2;
   - topology digest domain v1 to v2;
   - process identity digest domain v1 to v2;
   - full-config fingerprint domain and reported version v1 to v2;
   - static storage identity v1 to v2;
   - static control-plane outer identity v2 to v3;
   - control-plane command version 18 to 19, because the certified-bootstrap
     command gains each node's weight; and
   - control-plane logical-state version 31 to 32, because the retained
     certified placement policy gains each node's weight.
3. Retain all old fixtures append-only and add exact new fixtures plus old/new
   typed rejection before filesystem initialization, authority mutation,
   journal replay, snapshot installation, or listener publication, as
   applicable.
4. Inventory every containing journal, snapshot, Raft peer/restart, RPC, route-
   digest, and identity format. Independently versioned containers that already
   bind the nested command/state version need updated composite version-vector
   evidence rather than an automatic outer-version bump; any container that
   interprets the changed field grammar must advance with it.

Placement derivation version 1 is governed by an explicit dependency ledger.
Any incompatible change to one of its listed inputs or algorithms advances the
derivation version and appends old/new ordered-placement and replacement-ranking
fixtures. Because the manifest does not select the implementation version, the
same change also advances the static manifest schema, topology digest domain,
process identity domain, full-config fingerprint, and both outer identity
versions. The command and logical-state formats need not advance solely for a
new supported derivation-version value once they carry its fixed-width marker,
but their composite fixtures must record the new version vector; changing the
marker width, location, or interpretation advances those containing formats.

There is no schema-1 compatibility reader, default capacity, or upgrade path in
this pre-release change.

## Implementation

1. Extend disk parsing, semantic validation, canonical encoding, debug
   redaction, examples, and the multihost manifest generator with
   `capacity_weight`. Add the one-storage-node-per-storage-disk invariant at the
   same boundary.
2. Introduce a storage-owned bounded integer weight type. Resolve each storage
   node's disk weight before crossing into storage, and include it in
   `StaticStoragePlacementNode`, `CertifiedStorageNodeDomain`, and
   `CertifiedStoragePlacementPolicy` rather than exposing placement `f64`
   values.
3. Add the storage-owned derivation-version marker and one versioned placement
   implementation used by both initial acting-set derivation and eligible-spare
   ranking. Do not duplicate the key construction, normalization, scoring, or
   constraint logic in a future controller.
4. Normalize the complete certified integer vector and construct weighted
   `placement::NodeInfo` values inside storage instead of hardcoding `1.0`.
5. Encode, decode, and validate the derivation version and weight in the
   certified-bootstrap command and canonical control-plane state. Reject zero,
   missing, duplicate-disk, unsupported-version, and otherwise impossible
   policies before mutation or publication.
6. Expose only a storage-owned logical replacement-selection operation. Its
   caller supplies authenticated eligibility and current-placement facts; it
   must not receive raw weights or construct placement nodes itself. Phase 3.3
   will use the returned candidate in its existing durable transition/CAS
   protocol.
7. Keep ordinary local/environment topology at weight `1`; it has no static
   capacity model.
8. Add bounded diagnostics showing configured weight and initial PG counts per
   storage node so operators can inspect the realized distribution before
   startup.
9. Document that PG count controls distribution granularity, weights are ranking
   parameters rather than utilization promises, and committed weights do not by
   themselves trigger rebalancing.

## Verification

- parent-commit and new-version evidence for every format listed above, including
  exact certified command/state encodings and composite container vectors;
- stable canonical-digest vectors and collection-order independence;
- rejection of missing, zero, overflow, non-integer, capacity on a non-storage
  disk, multiple storage nodes on one disk, and in-place post-bootstrap changes;
- exact-fit `k + m` topology proving unequal weights do not change acting sets;
- `k + m + 1` and larger topologies compared with an independent weighted
  sampling reference under disk and host constraints;
- exact scale invariance for proportionally equal integer weights after GCD
  normalization, including configured vectors near the `u32` limit;
- exact ordered acting-set vectors across supported architectures;
- exact placement-derivation-version fixtures covering the key domain and PG-ID
  byte order, score/tie boundaries, disk and host constraints, candidate-buffer
  replacement, and node-to-shard ordering;
- statistical PG-count tests for equal and unequal capacities, including the
  documented domain cap without asserting raw capacity shares;
- policy round trips proving the exact configured integers, rather than
  normalized floats, survive command, state, snapshot, and restart boundaries;
- bootstrap/restart tests proving both the certified weights and ordered acting
  sets remain unchanged and no runtime path silently recomputes them with unit
  weights or rereads them from the manifest; and
- a deterministic replacement regression that starts from a committed policy
  with multiple differently weighted spares, varies the transient eligible set,
  and proves the selected destination follows the complete-policy ranking. The
  test must fail if the path falls back to node-ID order, rereads a manifest, or
  renormalizes only the currently eligible nodes.

## Out Of Scope

- online reweighting;
- automatic capacity discovery from the filesystem;
- node addition, removal, or disk replacement; and
- automatic acting-set migration or data balancing after bootstrap.

Availability-driven replacement of a lease-expired acting node with already
committed spare capacity is owned by Phase 3.3 of the multihost production
follow-up plan. Topology expansion and capacity rebalancing are owned by Phase
3.5. This plan owns the immutable integer policy consumed by both later paths;
neither controller may reconstruct weights from manifests, observed disk sizes,
or previous acting-set frequencies.
