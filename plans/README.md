# Plans and Current Roadmap

Last reviewed: 2026-10-28.

This file is the authoritative entry point for repository plans. It records
which work is current, what should happen next, and why other proposals are
deferred. Detailed design and implementation history remain in the linked plan
documents.

## Status definitions

- **Active:** implementation or investigation is currently under way.
- **Next:** approved, actionable work that should follow the higher-priority
  active items.
- **Blocked:** valuable work whose prerequisite or design decision is not yet
  complete.
- **Deferred:** intentionally not on the current execution path. Deferred plans
  live in [`deferred/`](deferred/) and name the condition that would reactivate
  them.
- **Completed:** finished implementation records. Completed plans live in
  [`completed/`](completed/); they are historical evidence rather than the
  current roadmap.

## Current priorities

### P0: correctness, durability, and production convergence

1. Fix [durable generation cleanup ownership](durable-generation-cleanup-ownership.md).
   Make the six ignored Direct PUT and stream late-write regressions pass by
   establishing one storage-owned write/cleanup exclusion contract.
2. Resolve the three independent durability gaps recorded under
   [known gaps and required follow-up](../guides/durable-storage.md#known-gaps-and-required-follow-up):
   - [DUR-3](../guides/durable-storage.md#dur-3-shard-unlink-durability-classification):
     classify and enforce shard-unlink durability.
   - [DUR-4](../guides/durable-storage.md#dur-4-mechanical-inventory-enforcement):
     mechanically enforce the persistence inventory.
   - [DUR-5](../guides/durable-storage.md#dur-5-capacity-exhaustion-safety-and-recovery-liveness):
     model ENOSPC, inode and quota exhaustion, recovery liveness, and reserved
     recovery headroom.
   These are separate planning gaps, not one capacity-exhaustion project.
3. Refresh and finish the [multihost production release gate](multihost-followup-plan.md):
   outage/replacement/rejoin, historical payload backfill, failure containment,
   terminal convergence, operational diagnostics, and recovery controls.
4. Use [bounded model checking](bounded-model-checking-plan.md) alongside the
   cleanup and multihost work. Model expansion supports the production fixes;
   it does not replace them.

### P1: AWS-facing correctness and production control

1. Implement [S3 error matching](s3-error-matching-plan.md) in bounded slices:
   effective operation classification, renderer inventory, closed internal
   causes, then contract-aware rendering for the historically problematic
   operations.
2. Refresh [endpoint routing](endpoint-routing-compat-plan.md) to remove work
   that has already landed, then implement virtual-hosted-style addressing.
3. Turn [persistent account and credential management](persistent-account-and-credential-management.md)
   into a concrete design before expanding STS or account-level policy work.
4. Finish bounded one-PG background scans and their metrics in
   [background worker throttling](background-worker-throttling-plan.md), then
   use representative multihost evidence to advance
   [production backpressure](production-backpressure-plan.md).
5. Refresh [AWS auth compatibility](aws-auth-compat-plan.md) around the actual
   remaining account-level Block Public Access and policy-context gaps.

### P2: operational maturity and measured performance

1. Add the minimal structured surface in [production logging](production-logging-plan.md).
2. Instrument real filesystems before deciding how far to take
   [shard write group commit](shard-write-group-commit.md).
3. Establish the policy, immutable evidence, and generic compatibility harness
   from [storage upgrade versioning](storage-upgrade-versioning-plan.md) before
   the first supported release. Rolling compatibility need not precede a
   stable release vector.
4. Resume [unified staged write publication](unified-staged-write-publication-plan.md)
   only after the shared cleanup/fencing contract is fixed. Terminal
   publication convergence comes before the optional small-UploadPart fast
   path.

## Active plan inventory

| Status | Plan | Next bounded slice | Dependency or note |
| --- | --- | --- | --- |
| Active | [Durable generation cleanup ownership](durable-generation-cleanup-ownership.md) | Implement shared write/cleanup exclusion and enable all six ignored regressions. | Highest-priority known correctness gap. |
| Active | [Multihost follow-up](multihost-followup-plan.md) | Reconcile the plan with landed replacement/capability work, then define the remaining release gate. | Keep outage and rejoin evidence authoritative. |
| Active | [Bounded model checking](bounded-model-checking-plan.md) | Model the selected cleanup ownership protocol and its restart/transport interleavings. | Companion to production fixes. |
| Active | [Background worker throttling](background-worker-throttling-plan.md) | Bound node/prefix/file work within one PG and expose worker metrics. | Required input to capacity policy. |
| Next | [S3 error matching](s3-error-matching-plan.md) | Add effective-operation classification and inventory every rendering boundary. | Preserve exact AWS behavior and post-header abort semantics. |
| Next | [Endpoint routing compatibility](endpoint-routing-compat-plan.md) | Refresh current-state claims and isolate virtual-hosted-style routing. | Trusted endpoint families and S3 Control routing already exist. |
| Next | [Persistent accounts and credentials](persistent-account-and-credential-management.md) | Replace the introductory sketch with the durable account, credential, rotation, and administration model. | Precedes broad STS/account-policy expansion. |
| Next | [AWS auth compatibility](aws-auth-compat-plan.md) | Remove completed/stale scope and define account-level BPA and missing policy-context slices. | Some work depends on the persistent account model. |
| Blocked | [Production backpressure](production-backpressure-plan.md) | Build representative workload profiles and fixed capacity policy after the multihost gate and worker metrics exist. | Do not tune from the local overload amplifier alone. |
| Next | [Production logging](production-logging-plan.md) | Introduce a small structured event API and deterministic test sink for high-value runtime failures. | Avoid a broad logging-framework migration. |
| Next | [Shard write group commit](shard-write-group-commit.md) | Measure file and directory synchronization costs on target filesystems. | Prototype only if directory synchronization is material. |
| Next | [Storage upgrade versioning](storage-upgrade-versioning-plan.md) | Approve compatibility policy and build the generic immutable-evidence/crash harness. | Required before the first supported release. |
| Blocked | [Unified staged write publication](unified-staged-write-publication-plan.md) | Split cleanup correctness from publication unification and optimization. | Blocked by durable cleanup ownership. |

## Deferred inventory

Deferred plans remain useful design records, but are not current work. Their
paths and links should remain valid so that the triggering evidence can revive
them without reconstructing the original reasoning.

| Plan | Reactivation trigger |
| --- | --- |
| [Coordinator/storage hot-path optimization](deferred/coordinator-storage-hot-path-optimization-plan.md) | A profile shows this boundary is a material request-path cost. |
| [DeleteBucket reservation classification](deferred/delete-bucket-reservation-classification-plan.md) | DeleteBucket convergence is stable and measurements justify classification changes. |
| [DeleteObjects batch commands](deferred/delete-objects-batch-command-plan.md) | Measurements show same-PG batching is common and metadata publication is the limiting cost. |
| [Encryption compatibility](deferred/encryption-compat-plan.md) | SSE-KMS becomes a roadmap priority and a key-management architecture is selected. |
| [Immutable placement capacity weights](deferred/immutable-placement-capacity-weights.md) | The target production topology requires heterogeneous capacity weighting. |
| [Internal copy pacing](deferred/internal-copy-pacing-plan.md) | Copy workloads demonstrably interfere with foreground traffic after shared admission is measured. |
| [Metadata write amplification](deferred/metadata-write-amplification-plan.md) | Measurements show metadata durability rather than shard durability is the limiting write cost. |
| [Multi-region namespace note](deferred/multi-region-namespace-note.md) | Multi-region namespace behavior becomes an explicit product goal. |
| [Public API review, July 2026](deferred/public-api-review-2026-07.md) | Revalidate its residual unchecked items against current code and distribute actionable findings to owner plans. |
| [S3 annotations](deferred/s3-annotations-plan.md) | Product demand justifies a new durable/authenticated annotation subsystem. |
| [SigV2 compatibility note](deferred/sigv2-compat-note.md) | A supported deployment requires legacy SigV2 interoperability. |
| [Storage-node crate extraction](deferred/storage-node-crate-extraction-plan.md) | Compile dependencies or ownership boundaries begin blocking development. |
| [Storage topology resize](deferred/storage-topology-resize-plan.md) | Replacement, outage convergence, and operator recovery APIs are stable. |
| [STS AssumeRole issuance](deferred/sts-assume-role-issuance.md) | Persistent accounts, roles, trust, and credential administration have a concrete owner model. |
| [Versioned physical shard files](deferred/versioned-physical-shard-files-option.md) | Repair or replacement evidence shows current physical fencing is insufficient. |
| [Website behavior note](deferred/website-behavior-note.md) | S3 website hosting becomes an explicit compatibility target. |

## Recently archived plans

- [Internal control-plane identity and authentication](completed/control-plane-auth-identity-plan.md):
  the shared authenticated boundary is complete; external secret distribution,
  credential lifecycle, and operator controls belong to production/multihost
  planning.
- [Production observability migration](completed/production-observability-migration-plan.md):
  the production path now uses bounded semantic telemetry and optional local
  deep tracing. An eBPF staging pilot and optional deletion of remaining local
  trace sites are operational follow-ups, not an incomplete migration gate.

## Maintenance rules

1. Update this index and the plan's own status in the same change whenever work
   starts, stops, becomes blocked, changes direction, or completes.
2. Every active plan must identify one reviewable next slice. A plan without an
   actionable next slice is deferred, completed, or needs to be split.
3. Deferred plans must state a concrete reactivation trigger; age alone does
   not reactivate them.
4. Move completed plans to `completed/` without deleting their implementation
   ledger or immutable evidence references.
5. Before moving or renaming a plan, update repository references and run the
   repository documentation/link checks that cover plans.
6. Re-review this roadmap after a major plan completes or at least monthly
   while implementation is active.
