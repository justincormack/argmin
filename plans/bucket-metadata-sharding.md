# Bucket Metadata Sharding Plan

## Goal

Remove the centralized bucket metadata database and place bucket metadata using the same PG hashing model as object metadata.

This removes the single metadata bottleneck and makes all metadata paths distributed.

## High-Level Design

- Bucket metadata is stored in a `buckets` table inside each PG metadata DB.
- Bucket placement is deterministic by bucket name:
  - `bucket_pg = derive_bucket_pg(bucket_name, pg_count)`
- All bucket-scoped operations (`CreateBucket`, `HeadBucket`, `PutBucketVersioning`, bucket ACL/config ops, `DeleteBucket`) run against that single bucket PG.
- `ListBuckets` fans out to all PGs, merges rows, sorts by bucket name, returns the final list.

This matches the existing model already used by list-object operations: shard-local query + fanout + merge.

## Why This Is Safe

- Global uniqueness is preserved because each bucket name maps to exactly one PG.
- `PRIMARY KEY(name)` on that PG is sufficient to prevent duplicate creates.
- No cross-PG distributed transaction is needed for bucket row writes.

## Key Architectural Decisions

1. Placement helper
- Add explicit helper for clarity and future stability:
  - `derive_bucket_pg(name, pg_count)`
- Do not overload object placement helpers in coordinator call sites.

2. Storage surface
- Retire `GlobalService` for bucket metadata.
- Add bucket metadata methods to `PgMetadataStore` (or a new PG-scoped bucket trait) and implement in `PgStore`.
- Keep interfaces bucket-name based and deterministic to avoid caller ambiguity.

3. ListBuckets semantics
- Query all PGs: `list_buckets(owner_principal)` per PG.
- Merge and sort globally by bucket name asc before response.
- If any PG query fails, fail the request (no partial list).

4. DeleteBucket semantics
- Validate bucket exists via bucket PG row.
- Emptiness check remains cluster-wide (list versions/markers across object metadata PGs).
- Delete bucket row only after emptiness passes.

5. No legacy compatibility path
- This change is a clean cut.
- No migration from centralized bucket DB is required.

## Schema Changes

1. Per-PG schema
- Ensure `buckets` table is created by `init_pg_schema`.
- Add index for owner list path:
  - `(owner_principal, name)`

2. Remove centralized schema path
- Remove `init_bucket_schema` usage.
- Remove centralized bucket DB file bootstrap from server startup.

## Coordinator Changes

1. Replace centralized field
- Remove `bucket_db: SqliteBucketDb` from `Coordinator`.
- Use `storage_node.get_pg(derive_bucket_pg(bucket))` for bucket metadata operations.

2. Update bucket APIs
- `create_bucket`
- `head_bucket`
- `list_buckets` (fanout)
- `delete_bucket`
- `put/get/delete` for:
  - versioning
  - CORS
  - tags
  - public access block
  - ACL flag
  - ownership controls

3. Keep object operation contract unchanged
- Calls to `head_bucket` remain as today, but now resolve via bucket PG instead of centralized DB.

## HTTP / Startup Changes

- Remove centralized bucket DB initialization/config plumbing.
- Coordinator construction only needs PG metadata stores.
- No API contract changes at HTTP layer.

## Phased Implementation

### Phase 0: Prep and invariants

- Add `derive_bucket_pg` helper + unit tests for determinism/stability.
- Document lock ordering for bucket PG + object PG interactions.

Exit criteria:
- Helper tests pass.
- Concurrency notes updated in guides.

### Phase 1: Storage schema + traits

- Move bucket table creation into per-PG schema init.
- Add bucket methods to PG metadata trait + `PgStore` implementation.
- Add owner/name index.
- Add storage unit tests for all bucket CRUD/config methods on a single PG.

Exit criteria:
- Storage tests pass for bucket metadata operations without `SqliteBucketDb`.

### Phase 2: Coordinator routing

- Remove centralized bucket DB from coordinator state.
- Route all bucket ops to bucket PG.
- Implement `list_buckets` fanout + merge + global sort.
- Keep existing delete-empty behavior.

Exit criteria:
- Coordinator unit tests pass for bucket ops and list fanout ordering.

### Phase 3: Startup and wiring cleanup

- Remove centralized bucket DB construction and related config usage.
- Remove dead code paths using `GlobalService` for bucket metadata.

Exit criteria:
- Server boots and all bucket/object paths run using PG-backed metadata only.

### Phase 4: Test parity and hardening

- Update/expand integration tests:
  - bucket create/head/delete
  - list buckets ordering
  - versioning/CORS/tags/PAB/ownership/ACL
  - delete non-empty bucket still returns `BucketNotEmpty`
- Add fanout failure test: one PG fails during list -> request fails cleanly.

Exit criteria:
- s3-tests bucket suites pass.
- No centralized bucket DB code remains.

## Testing Checklist

1. Correctness
- Create same bucket concurrently from multiple clients -> one success, others bucket exists.
- ListBuckets returns globally sorted merged set across multiple PGs.
- All bucket config APIs round-trip correctly.

2. Behavior compatibility
- Existing bucket-related S3 test expectations remain unchanged.
- DeleteBucket semantics for versioned delete-marker-only buckets remain unchanged.

3. Failure modes
- Fanout partial failure in ListBuckets returns an error (not partial success).
- Bucket PG unavailable returns bucket-not-found vs internal-error consistently with current mappings.

## Risks and Mitigations

1. Risk: Fanout cost for ListBuckets
- Mitigation: expected low QPS and low cardinality; same model already accepted for list-object operations.

2. Risk: Refactor churn touching many call sites
- Mitigation: phase by layer (storage -> coordinator -> wiring), keep tests green each phase.

3. Risk: Semantic drift in bucket errors
- Mitigation: preserve current `ServerError` mappings and extend explicit tests for each bucket op.

## Out of Scope

- Account/global control-plane metadata unrelated to bucket rows.
- Any migration tooling from existing centralized bucket DB data.
