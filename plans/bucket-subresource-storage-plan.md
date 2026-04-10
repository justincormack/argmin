# Bucket Subresource Storage Plan

## Context

Bucket metadata currently mixes two different kinds of state in one storage
surface:

1. bucket state that storage and the coordinator do need to understand
2. opaque S3 subresources that storage mostly just round-trips

Today the storage trait exposes one method per bucket subresource in
[traits.rs](/home/justin/src/github.com/justincormack/argmin/crates/storage/src/traits.rs),
and the bucket row stores those subresources as dedicated columns in
[schema.rs](/home/justin/src/github.com/justincormack/argmin/crates/storage/src/schema.rs).

That creates two problems:

1. the `server-core -> storage` boundary carries S3 feature shape too far down
2. adding a new opaque bucket subresource requires storage trait, schema, and
   `PgStore` churn even when storage does not need to understand the payload

At the same time, a fully generic `metadata_id -> blob` model would be too
weak. Some bucket metadata is part of hot-path authorization and request
execution, and some features need persisted derived fields or indexed queries.

Examples:

1. policy evaluation depends on persisted public/non-public classification and
   generation-based cache invalidation in
   [authz.rs](/home/justin/src/github.com/justincormack/argmin/crates/server-core/src/coordinator/authz.rs)
2. lifecycle evaluation depends on generation-based cache invalidation and a
   storage-side query for buckets with lifecycle configuration in
   [pg_store.rs](/home/justin/src/github.com/justincormack/argmin/crates/storage/src/pg_store.rs)
3. object hot paths depend on explicit `BucketFastPathInfo` fields in
   [types.rs](/home/justin/src/github.com/justincormack/argmin/crates/storage/src/types.rs)

So the right shape is not "make bucket metadata fully generic". The right shape
is "make opaque bucket subresources generic, while keeping hot-path bucket
state and derived summaries explicit".

## Goals

1. remove one-method-per-feature storage APIs for opaque bucket subresources
2. stop requiring bucket-table schema changes for every new opaque subresource
3. preserve explicit typed bucket state for hot paths and invariants
4. preserve AWS-compatible behavior and existing cache invalidation semantics
5. keep storage policy-agnostic: raw payload persistence only, no XML/JSON
   parsing moved into storage

## Non-goals

1. do not replace the entire bucket row with a generic key/value store
2. do not move versioning, object lock, ACL/public flags, bucket state, owner,
   or effective encryption into generic blobs
3. do not remove persisted derived fields needed for hot paths
4. do not change HTTP or S3 API behavior
5. do not introduce a free-form untyped metadata identifier surface
6. do not preserve compatibility with pre-refactor local metadata DB layouts

## Clean-Cut Assumption

We are still in the pre-release stage and explicitly do not carry local metadata
schema or data migration compatibility.

That means this refactor should take the simpler path:

1. optimize the fresh schema and storage interface for the desired end state
2. do not keep dual-write or fallback read logic for old bucket subresource
   columns
3. do not add additive schema migration steps purely to preserve existing local
   dev/test data

This is exactly the right stage to do this refactor. The payoff is that the
storage boundary becomes cleaner now, before more bucket subresource features
accumulate on the old shape.

## Bucket Metadata Split

### Keep First-Class

These fields should remain explicit typed bucket state:

1. bucket identity and ownership
2. bucket lifecycle state (`Active`, `Deleting`)
3. versioning state
4. object lock configuration
5. ACL/public-read/public-write state
6. write reservation / drain state
7. effective encryption semantics used on hot object paths

These fields either participate directly in invariants, are read on hot paths,
or are part of explicit authorization and write-publication rules.

### Move Behind a Generic Bucket-Subresource Layer

These are the initial candidates for generic subresource persistence:

1. CORS configuration
2. bucket tags
3. public access block configuration
4. ownership controls
5. bucket policy document
6. lifecycle configuration

These are all bucket-scoped payloads whose primary stored form is already an
opaque string blob. The coordinator parses or interprets them when needed.

Important clarification:

1. `public_access_block` and `ownership_controls` are consulted directly on hot
   auth and request-execution paths today
2. so even if their authoritative payload persistence moves behind the generic
   subresource layer, their current values should remain denormalized in
   `buckets` and exposed through `BucketFastPathInfo`
3. they are not candidates for a join-on-demand model on hot paths

In other words:

1. CORS and tags are straightforward opaque round-trip subresources
2. public access block and ownership controls are generic in payload
   persistence, but still first-class in hot-path summary/state loading
3. policy and lifecycle are generic in payload persistence, but keep their
   explicit derived fields

### Keep Explicit Derived Fields Where Needed

Generic payload storage does not remove the need for some explicit summary
fields:

1. bucket policy still needs persisted `is_public` classification
2. bucket policy still needs a monotonic generation for cache invalidation
3. bucket lifecycle still needs a monotonic generation for cache invalidation
4. lifecycle presence still needs to remain queryable for storage-side sweep
   fanout

So "generic subresource" here means generic payload persistence, not generic
derived metadata.

## Proposed Storage Model

Add a per-PG table for bucket subresources with a typed kind discriminator.

Working shape:

```sql
bucket_subresources (
    bucket_name TEXT NOT NULL,
    kind        INTEGER NOT NULL,
    body        BLOB NOT NULL,
    generation  INTEGER NOT NULL,
    aux_int_1   INTEGER,
    PRIMARY KEY (bucket_name, kind),
    FOREIGN KEY (bucket_name) REFERENCES buckets(name) ON DELETE CASCADE
)
```

This exact schema can change, but the important design constraints are:

1. `kind` is a closed enum, not an arbitrary string or user-supplied ID
2. the raw payload is stored once per `(bucket, kind)`
3. each subresource has a monotonic generation owned by storage
4. optional auxiliary typed columns are available for derived summaries where
   the fast path needs them

The initial `kind` enum should cover only the known bucket subresources we
already implement.

## Proposed Storage Interface

Replace feature-specific APIs for opaque bucket subresources with a small typed
surface.

Possible shape:

```rust
enum BucketSubresourceKind {
    Cors,
    Tagging,
    PublicAccessBlock,
    OwnershipControls,
    Policy,
    Lifecycle,
}

struct StoredBucketSubresource {
    body: Vec<u8>,
    generation: u64,
    aux: BucketSubresourceAux,
}
```

And trait methods along the lines of:

1. `put_bucket_subresource(...)`
2. `get_bucket_subresource(...)`
3. `delete_bucket_subresource(...)`

`BucketSubresourceAux` should remain typed and closed. For the initial cases:

1. policy needs `is_public`
2. lifecycle likely needs no aux field beyond generation
3. other subresources likely need no aux field initially

This keeps the storage API generic over subresource kind without degrading into
stringly-typed metadata blobs.

## Bucket Row Changes

The `buckets` table should stop carrying opaque payload columns that do not
need to stay denormalized for hot paths:

1. `cors_config`
2. `tags`
3. `bucket_policy`
4. `bucket_lifecycle`

The `buckets` table should keep the denormalized hot-path values that the
coordinator reads directly during auth and request execution:

1. `public_access_block`
2. `ownership_controls`

But the bucket row should continue to carry the summary fields needed on hot
paths:

1. `public_access_block`
2. `ownership_controls`
3. `bucket_policy_public`
4. `bucket_policy_generation`
5. `bucket_lifecycle_generation`

We should also explicitly decide whether these remain denormalized in
`buckets`, or move to the subresource table and get projected into
`BucketFastPathInfo` when loading the bucket. The working recommendation is:

1. keep the hot-path summaries and hot-path payload mirrors in `buckets`
2. keep the authoritative raw payloads in `bucket_subresources`
3. for `public_access_block` and `ownership_controls`, the denormalized value in
   `buckets` should remain the exact current payload string used by the
   coordinator fast path
4. for policy and lifecycle, keep the generation/summary fields in `buckets`

That keeps fast-path bucket loads simple and avoids join churn on common paths.

## Coordinator / Cache Implications

The coordinator should continue to own:

1. request parsing and validation
2. bucket policy parsing and public/non-public classification
3. lifecycle XML parsing and validation
4. cache population and invalidation logic

The main change is that it will call generic subresource storage helpers
instead of per-feature store methods.

For authorization, the coordinator should move away from shared
auth-and-storage wrappers like "get opaque subresource". Those helpers become
awkward as soon as S3 operations diverge in small but important ways.

The working direction is:

1. keep only raw generic bucket-subresource persistence helpers at the storage
   call boundary
2. add one authorization entrypoint per S3 bucket-subresource operation, even
   when several delegate internally to a common rule
3. return typed authorized request/token values with private construction, so
   storage and mutation helpers cannot be reached from an unauthorized path
4. unit test those per-operation authorization functions directly

This matches the pattern already used for authorized object writes and should
make bucket-config authorization easier to review, easier to test, and less
error-prone as AWS-compatible edge cases accumulate.

Important behavior to preserve:

1. `BucketSummary` and `BucketFastPathInfo` remain explicit typed structures
2. policy cache keys still use `bucket_policy_generation`
3. lifecycle cache keys still use `bucket_lifecycle_generation`
4. `public_access_block` and `ownership_controls` remain available without
   extra subresource fetches on hot auth paths
5. storage-side lifecycle listing still remains efficient and exact

## Implementation Phases

### Phase 1: Add Generic Subresource Types

Deliver:

1. `BucketSubresourceKind`
2. `StoredBucketSubresource`
3. typed aux structure for per-kind derived fields
4. storage trait methods for generic put/get/delete

This phase should not remove any existing feature-specific methods yet.

Exit criteria:

1. generic types compile
2. feature-specific methods can be reimplemented in terms of the new generic
   path internally if useful

### Phase 2: Add Storage Table and `PgStore` Support

Deliver:

1. new `bucket_subresources` table in per-PG schema
2. `PgStore` implementation for generic subresource CRUD
3. `ON DELETE CASCADE` cleanup when bucket rows are deleted
4. storage tests covering round-trip, overwrite, delete, and generation bumps

Design requirement:

1. `put` and `delete` must update any denormalized summary fields atomically
   with the subresource row mutation

That means policy and lifecycle writes likely need one transaction touching both
`bucket_subresources` and `buckets`.

Exit criteria:

1. storage tests pass
2. policy/lifecycle generation semantics remain unchanged
3. lifecycle presence remains queryable without scanning all subresource blobs

### Phase 3: Migrate Existing Bucket Subresources

Migrate feature by feature:

1. CORS
2. tagging
3. public access block
4. ownership controls
5. bucket policy
6. lifecycle

For each migrated feature:

1. switch coordinator calls to generic subresource methods
2. remove feature-specific `PgMetadataStore` methods
3. remove feature-specific `PgStore` implementations
4. keep error mapping and HTTP behavior unchanged

Recommended order:

1. start with the pure round-trip features: CORS, tagging
2. then public access block and ownership controls, keeping their denormalized
   bucket-row mirrors intact
3. last migrate policy and lifecycle because they carry derived-field semantics

Exit criteria:

1. no migrated feature still has dedicated store trait methods
2. all bucket subresource APIs still pass existing tests

### Phase 3b: Refactor Bucket-Subresource Authorization

After the first coordinator migration lands, normalize authorization around S3
operation boundaries instead of shared "generic subresource" helpers.

Deliver:

1. raw coordinator helpers for `store/load/delete` bucket subresources remain
   persistence-only and do not perform authorization
2. add one auth function per migrated S3 operation, for example:
   `authorize_get_bucket_cors`, `authorize_put_bucket_tagging`,
   `authorize_get_bucket_public_access_block`,
   `authorize_put_bucket_ownership_controls`
3. those auth functions return typed authorized values whose fields are not
   constructible outside the auth path
4. operation handlers consume those authorized values when calling storage or
   applying fast-path/cache updates
5. common bucket-admin or bucket-policy logic remains shared internally inside
   `authz.rs`, but no longer leaks as the public operation shape

Design requirement:

1. operation-specific semantic validation that is part of request acceptance
   should live in the per-operation auth path
2. examples include ownership-controls ACL compatibility, special
   public-access-block reads, and any future per-call AWS edge conditions

Exit criteria:

1. migrated bucket-subresource calls no longer depend on shared
   auth-and-storage wrappers
2. each migrated S3 call has a directly testable auth entrypoint
3. storage mutation helpers can only be reached from a typed authorized flow

### Phase 3a: Bucket Creation Seeding Rules

Ownership controls are not just a normal PUT/GET/DELETE bucket subresource.
Bucket creation currently seeds them immediately, and the refactor must keep
that behavior explicit.

Deliver:

1. create-bucket writes the ownership-controls subresource row when a new bucket
   is created
2. the same create path writes the denormalized `buckets.ownership_controls`
   value used by `BucketFastPathInfo`
3. those writes happen transactionally with bucket creation, so the persisted
   subresource and hot-path summary cannot diverge
4. idempotent create behavior must not overwrite an existing ownership-controls
   value for an already-owned bucket

This rule is specific to ownership controls. It should be called out separately
so the migration does not accidentally preserve only the hot-path mirror while
regressing persisted `GetBucketOwnershipControls` state for newly created
buckets.

Exit criteria:

1. new buckets still return the expected ownership-controls configuration
   immediately after creation
2. idempotent create still preserves existing ownership-controls state

### Phase 4: Simplify Bucket Types and Loading Paths

After migration, clean up `BucketInfo` and related loading logic.

Possible end state:

1. `BucketInfo` no longer eagerly carries all opaque bucket subresource bodies
2. `BucketFastPathInfo` remains a focused hot-path structure
3. `head_bucket` can load just typed bucket state plus any summary fields
4. subresource payloads are loaded only when a corresponding API or cache fill
   path needs them

This phase should be done carefully because some existing tests and helper code
assume `head_bucket_raw()` returns the raw policy/lifecycle payloads.

Exit criteria:

1. bucket-loading code is clearer and narrower than today
2. no hot path pays extra subresource load cost unnecessarily

### Phase 5: Remove Obsolete Schema and APIs

Deliver:

1. remove obsolete bucket payload columns from fresh schema creation
2. remove old feature-specific storage tests
3. remove dead helper code

Because we are pre-release, this should be a clean cut. Old local DB layouts do
not need compatibility handling.

Exit criteria:

1. fresh databases use the new layout only
2. no dead feature-specific bucket subresource store code remains

## Testing Plan

### Storage Tests

Add direct storage tests for:

1. put/get/delete round-trip for each subresource kind
2. overwrite bumps generation
3. delete bumps generation for kinds that use generation-based cache
4. policy `is_public` aux state is persisted correctly
5. lifecycle presence query matches current behavior
6. deleting a bucket removes associated subresources
7. create-bucket seeding writes ownership controls and its denormalized bucket
   mirror atomically

### Coordinator Tests

Keep or extend existing coordinator tests for:

1. put/get/delete bucket CORS
2. put/get/delete bucket tagging
3. put/get/delete public access block
4. put/get/delete ownership controls
5. put/get/delete bucket policy
6. put/get/delete lifecycle
7. policy cache invalidation by generation
8. lifecycle cache invalidation by generation
9. create-bucket still seeds ownership controls
10. idempotent create does not overwrite ownership controls

### AWS / Compatibility Risk Checks

The expected external behavior must remain unchanged for:

1. bucket policy authorization behavior
2. lifecycle CRUD and sweep behavior
3. ownership-controls and public-access-block interactions
4. bucket tagging and CORS round-trip behavior

Existing `s3-tests` coverage for those APIs should continue to pass.

## Risks and Mitigations

### Risk 1: Generic layer becomes too weak and stringly-typed

Mitigation:

1. use a closed enum for subresource kind
2. keep typed aux/summary fields
3. keep hot bucket state first-class

### Risk 2: Policy and lifecycle regress due to lost derived fields

Mitigation:

1. migrate policy and lifecycle last
2. preserve current generation semantics exactly
3. keep explicit tests for cache invalidation and lifecycle fanout

### Risk 3: Bucket loads accidentally become more expensive

Mitigation:

1. keep `BucketFastPathInfo` explicit
2. keep summary fields in the bucket row
3. keep `public_access_block` and `ownership_controls` denormalized in the
   bucket row for hot auth paths
4. avoid eager joins for subresource payloads on hot paths

### Risk 4: Schema churn obscures behavior during the refactor

Mitigation:

1. migrate one feature at a time
2. keep tests green after each feature move
3. remove old columns only after all code paths have switched over

## Recommended End State

The intended end state is:

1. bucket state and hot-path summaries remain explicit typed storage data
2. opaque bucket subresource payloads live in a generic typed subresource table
3. storage exposes generic subresource CRUD instead of one method per feature
4. policy and lifecycle keep their derived summaries and generation semantics
5. adding a new opaque bucket subresource no longer requires bucket-table schema
   surgery

## Current Recommendation

Proceed with the hybrid model, not a full generic metadata store.

The first implementation step should be:

1. add the generic `BucketSubresourceKind` and storage CRUD path
2. migrate CORS and tagging first
3. defer policy and lifecycle until the generic path has proven out on the
   simpler round-trip cases
