## Coordinator / Storage Hot-path Optimization Follow-up

Status: in progress

This plan contains the optimization-only follow-up extracted from
`plans/completed/coordinator-storage-capability-plan.md` after the
coordinator/storage capability refactor completed.

The boundary work is done. What remains here is deliberately narrower:

- hot-path bucket policy optimization
- hot-path lifecycle specialization

These are performance and cache-shape follow-ons. They must not weaken the
request-scoped snapshot guarantees or AWS behavior pinning established by the
completed capability refactor.

## Scope

This plan covers only:

- Phase 11: bucket policy residualization for hot paths
- Phase 12: lifecycle fast-path specialization

It does not reopen:

- bucket/object capability boundary work
- write-reservation lifetime work
- PG-hiding/storage-ownership work
- BOE modern-auth seam work
- data-plane EC/shard-IO ownership work

## Preconditions

The completed boundary plan established:

- bucket-first request-scoped handles
- storage-owned write reservation and snapshot loading
- bounded BOE read fast path
- pull-freshness generation tracking
- parsed-policy consolidation into the shared BOE read cache
- removal of the old parsed lifecycle cache

This plan starts from that state.

## Non-goals

- do not change AWS-visible semantics for authorization or lifecycle headers
- do not broaden the BOE read fast path to unrelated request families just to
  justify an optimization
- do not reintroduce raw lifecycle or policy cache pressure unless it is
  clearly temporary and reviewed as such
- do not mix this work with unrelated authz or storage-boundary refactors

## Phase 11: Bucket Policy Residualization for Hot Paths

After the bounded fast-path execution-context cache exists and its freshness
contract is correct, consider a follow-on optimization for the hottest
request families: compile bucket policy into per-operation residual policy
evaluators rather than caching raw policy bodies or evaluating the full parsed
policy on every request.

This is explicitly a later optimization phase, not part of the initial
fast-path cache realignment. The first implementation should prioritize:

- bounded cache size
- cheap hot-path revalidation
- correct modern auth/config coverage
- full-snapshot fallback where needed

Only once that is stable should this optimization be considered.

The idea is:

- partition bucket policy by relevant action family
  - especially `GetObject` / `HeadObject`
  - later hot write/multipart families if warranted
- partially evaluate each relevant statement against bucket-static context
  known at cache-build time
  - bucket ARN / resource shape
  - bucket tags
  - bucket-owner/account-local facts
  - Public Access Block / ownership-controls interactions where applicable
  - other bucket-known condition inputs
- discard irrelevant or unsatisfiable branches for that operation family
- cache only the residual request-time policy logic that still depends on
  request-dynamic facts
  - principal / auth identity
  - request headers
  - transport context
  - source IP / VPC / time
  - object key / object tags where relevant

This means bucket tags do not necessarily need to remain as first-class
hot-path inputs if they are only used to resolve bucket-static policy
conditions: they can be substituted into the policy at cache-build time and
folded into the residual evaluator.

Potential advantages:

- smaller hot-path policy work
- less per-request condition evaluation
- bucket-static inputs like bucket tags disappear into the compiled residual
  policy rather than remaining as separately consulted state
- clearer separation between bucket-static and request-dynamic authorization
  inputs

Risks / constraints:

- this is an authorization optimization, so correctness risk is high
- it must preserve exact AWS-compatible authorization semantics
- it should not be attempted before the basic bounded-cache and freshness model
  is fully pinned by tests
- any residualization strategy must preserve the request-scoped snapshot
  contract for the bucket handle model

Initial target if this phase is pursued:

- `GetObject`
- `HeadObject`

Only after that proves correct and worthwhile should the same approach be
considered for:

- `PutObject`
- `CreateMultipartUpload`
- `UploadPart`
- `CompleteMultipartUpload`

Acceptance criteria:

- the bucket-static inputs eligible for substitution are explicitly listed
- the residual policy representation is explicit and reviewable
- the request-dynamic inputs still evaluated at request time are explicit
- read/head authorization remains pinned against AWS behavior after the
  residualization step
- the optimization is optional and clearly layered on top of the bounded
  fast-path cache rather than entangled with the initial cache rollout

## Phase 12: Lifecycle Fast-path Specialization

After the initial bounded cache rollout, treat lifecycle the same way as
policy: as something that should ideally not remain in raw form on the hot
path, but also cannot simply be removed because lifecycle-derived headers are
still observable on high-priority request families.

The goal of this phase is to replace raw lifecycle-cache pressure with a
specialized lifecycle-derived representation that can answer the hot-path
questions cheaply:

- current-object expiration summary for:
  - `GetObject`
  - `HeadObject`
  - object range / part reads where applicable
  - `CompleteMultipartUpload`
- multipart abort summary for:
  - `CreateMultipartUpload`
  - `ListParts`

The likely direction is:

- partially evaluate lifecycle rules against bucket-static inputs
  - especially bucket tags if lifecycle filters depend on them
- discard irrelevant rules for the specific hot-path query families
- retain only the narrower residual evaluator needed to answer:
  - earliest current-object expiration for a specific object/key/tags/size/time
  - earliest multipart abort summary for a specific upload/key/initiation time

This should let the system preserve AWS-compatible lifecycle-derived response
headers on hot paths without requiring raw lifecycle configuration to dominate
cache size.

This is explicitly a later optimization phase because:

- correctness matters for visible response-header behavior
- lifecycle evaluation is separate from primary authorization flow
- the fast-path boundedness and freshness contract should already be settled
  before introducing this specialization

Acceptance criteria:

- the hot lifecycle-derived questions are explicitly listed and tested
- the bucket-static inputs eligible for substitution are explicit
- the residual lifecycle representation is explicit and reviewable
- `GetObject` / `HeadObject` / `CompleteMultipartUpload` lifecycle headers stay
  pinned against AWS behavior
- the specialized representation materially reduces the need to carry raw
  lifecycle state in the hot cache

## Ordering

Do Phase 11 first.

Reasoning:

- policy residualization directly affects the hottest BOE read authorization
  path
- lifecycle specialization is narrower and can build on the same cache-shape
  lessons once policy residualization is understood

## Exit Criteria

This plan is complete when either:

- both phases land with AWS-pinned coverage, or
- one or both phases are explicitly rejected as not worthwhile after review,
  with that decision recorded here

Either outcome is acceptable. This is optimization work, not unfinished
correctness boundary work.
