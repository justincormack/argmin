# Bucket and Object Name Type Hardening Plan

## Status

In progress.

Phases 1 and 2 are complete.

Phase 3 is started, but not complete yet.

What has landed so far:

1. `BucketName` / `ObjectKey` are now strict validated types.
2. Alternate parse paths such as copy-source and XML/multipart key parsing now
   return typed values instead of raw bucket/key strings.
3. Coordinator request wrappers such as `BucketRequest`, `ObjectRequest`,
   `ObjectVersionRequest`, `MultipartObjectRequest`, `DeleteEntry`, and
   `AppendStreamPartRequest` now carry typed bucket/key values directly.
4. `server-http` now constructs typed coordinator requests at the boundary and
   reuses typed authorized values for the streaming PUT path instead of feeding
   raw strings back into `server-core`.
5. Authz and the main object/multipart authorized-state path in `server-core`
   now preserve typed bucket/key values instead of immediately degrading them to
   `String`.
6. PG routing now has explicit typed entry points for validated S3
   bucket/object names, while the separate internal shard namespace
   (`segment/...`, `mpu/...`) continues to use a distinct raw path.
7. Generic bucket-scoped coordinator helpers such as active-bucket summary
   loading and bucket write reservations now take typed `BucketName`
   references rather than rediscovering raw strings from request wrappers.
8. Object delete, object read/authz state loading, and multipart authz entry
   points now use typed bucket/object PG routing and typed bucket-summary
   helpers instead of the older raw-string coordinator helpers.
9. The `PgMetadataStore` boundary is now substantially migrated to typed
   `BucketName` / `ObjectKey` parameters on both bucket and object paths, and
   the main coordinator object/multipart/read/reclaim flows now use those
   typed storage calls instead of a raw-string storage-dispatch surface.
10. `BucketSummary`, the coordinator bucket policy/lifecycle caches, and the
    `SharedStorageNode` bucket fast-path / bucket-lock / multipart-completion
    lock helpers now also carry `BucketName` directly instead of degrading
    hot-path bucket state back to `String`.

What is still open before Phase 3 can be marked complete:

1. `server-core` still has a small number of compatibility and convenience
   helpers that accept raw `&str` bucket/key values, mostly around external
   probes, cache keys, older raw PG helpers, and test-only entry points.
2. The final Phase 3 answer on raw internal PG/cache helper variants versus
   typed-only access is not complete yet.
3. The PG hashing failure-model cleanup is still only partly done: typed S3
   entry points now cover the main request path, but some older raw helper
   variants remain for internal namespaces and compatibility-shaped paths.

`security/codex-23ffb1b` remains open until the early migration phases in this
plan land. This plan is the intended fix path; it is not documenting work that
has already shipped.

## Trigger

`security/codex-23ffb1b` exposed a boundary failure that is larger than the
specific `x-amz-copy-source` bug.

The immediate panic came from the new PG hashing length assert, but the real
problem is that attacker-controlled bucket names and object keys can still move
through parts of the system as unchecked `String` values. That violates the
existing threat-model assumption that `server-core` only sees validated
bucket/key values.

## Decision

This should be fixed with a type-driven rewrite, not with more local validator
calls.

The core design decision is:

1. `BucketName` and `ObjectKey` are the validated, bounded domain types.
2. There should not be separate `ValidatedBucketName` /
   `ValidatedObjectKey` wrappers.
3. Before validation, values are just raw `&str`, `String`, or bytes from the
   wire.
4. After validation succeeds, callers hold `BucketName` / `ObjectKey` and later
   layers no longer re-accept raw strings for those domains.

This matches the earlier "parse once, validate once" direction, but finishes
the job instead of leaving `BucketName` / `ObjectKey` as permissive string
wrappers.

## Why this work is needed

Today the codebase has the worst of both models:

1. we have named types that look authoritative
2. but those types do not actually enforce the domain invariants
3. so downstream code can assume validation that never happened
4. and alternative parse paths can bypass the ad hoc validators

The current shape creates repeated risk:

1. `crates/storage/src/types.rs` defines `BucketName` / `ObjectKey` as plain
   `String` newtypes with infallible constructors
2. `crates/server-http/src/http/router.rs` contains bucket/key validation, but
   that validation is local to some request paths rather than encoded in the
   types
3. `crates/server-http/src/http/request.rs::parse_copy_source` returns raw
   `(String, String, Option<String>)`, which is how the recent bug escaped the
   normal routing checks
4. `guides/threat_model.md` already states that `server-core` assumes validated
   bucket/key strings, which is not currently guaranteed by the type system

This is exactly the kind of boundary weakness that keeps reappearing as new
features or alternate parsers are added.

## Goals

1. Make invalid or unbounded bucket names and object keys unrepresentable past
   the HTTP parsing boundary.
2. Ensure all attacker-controlled bucket/key inputs are validated exactly once
   before entering coordinator logic.
3. Remove infallible construction of `BucketName` / `ObjectKey` from raw
   external strings.
4. Preserve existing AWS-compatible validation rules and bounds:
   - bucket names: current S3 bucket-name rules
   - object keys: 1-1024 bytes, NUL rejected, all existing accepted forms
5. Keep `storage` independent of `server-http`.
6. Leave the codebase in a state where future request paths naturally use the
   typed boundary instead of remembering to call a validator manually.

## Non-goals

1. Redesigning unrelated request parsing.
2. Widening accepted bucket/key formats beyond AWS behavior.
3. Adding new dependencies.
4. Preserving the current permissive constructor API for convenience.

## Constraints

1. `storage` must remain independent of `server-http`.
2. Validation logic for these shared domain types must therefore live either:
   - in the crate that owns `BucketName` / `ObjectKey`, or
   - in a small leaf crate both sides can depend on
3. HTTP-specific error mapping must stay in `server-http`; shared type
   constructors should return domain-specific validation errors, not
   `ServerError`.
4. The first implementation slice must make an explicit decision about how
   strict `BucketName` / `ObjectKey` interact with persisted SQLite values and
   `FromSql` loads. This cannot be deferred to the end of the rewrite because
   the types are already used directly in storage rows today.

## Target Model

### Domain types

`BucketName` and `ObjectKey` remain the public names, but they become strict
types:

1. fallible construction only for untrusted input:
   - `TryFrom<&str>`
   - `TryFrom<String>`
   - optionally `parse` helpers if they improve readability
2. infallible `From<String>` / `From<&str>` implementations are removed
3. `new(...)` should either become fallible or be removed entirely if it would
   be ambiguous
4. any unchecked constructor, if still needed for tightly-scoped internal use,
   must be narrow and explicit about trust, for example:
   - crate-private
   - or clearly named as trusted/internal rather than "new"

### Boundary rule

Raw strings are acceptable only:

1. on the wire
2. in parsing helpers before validation
3. in tests that are intentionally exercising invalid input paths

Every request structure that crosses into coordinator logic should carry
`BucketName` / `ObjectKey`, not raw bucket/key strings.

### Shared validation errors

Introduce small domain errors such as:

1. `BucketNameError`
2. `ObjectKeyError`

These should capture enough reason detail for:

1. mapping to current HTTP error responses
2. precise unit tests
3. safe internal diagnostics

### Persistence and deserialization model

Phase 1 must choose and implement one clear rule for persisted
`BucketName` / `ObjectKey` values loaded from SQLite:

1. validate on `FromSql`
2. load raw row strings first, then validate before constructing strict types
3. use a narrow explicit trusted constructor for already-validated persisted
   data, with a documented invariant explaining why the load is trusted

The important point is not which option wins, but that the codebase has one
auditable answer immediately when `BucketName` / `ObjectKey` become strict.

### Failure model and invariant enforcement

The end state should not rely on attacker-reachable panic paths to enforce
bucket/key bounds.

During migration, the plan should move toward one of these end states:

1. PG hashing and similar low-level helpers accept only strict validated types
   and any remaining `assert!` is justified as an internal invariant
2. low-level helpers remain string-based but fail closed with a regular error
   instead of panicking when an invariant is violated

The plan does not need to choose the exact implementation today, but it should
make the intended failure model explicit so the rewrite does not stop after
strengthening only upstream callers.

## Work Plan

## Phase 1: Move validation into the domain types

1. Rework `BucketName` and `ObjectKey` in `crates/storage/src/types.rs` so they
   enforce validation and bounds.
2. Move the current bucket/key validation rules out of
   `server-http/src/http/router.rs` into shared constructors or shared helper
   functions used by the constructors.
3. Preserve the exact current validation behavior unless AWS-conformance review
   says otherwise.
4. Decide and implement the persistence/load model for strict
   `BucketName` / `ObjectKey`, including `rusqlite::FromSql` behavior and any
   trusted internal constructors needed for already-validated stored data.
5. Update call sites to use fallible construction instead of `from` or
   unchecked `new`.

Exit criteria:

1. `BucketName` and `ObjectKey` cannot be built from arbitrary strings without
   going through validation.
2. The bucket/key validation logic has a single authoritative home.
3. Storage row loads and other persistence/deserialization paths have one
   explicit, audited rule instead of an unresolved migration placeholder.

## Phase 2: Fix alternate parse paths to return typed values

1. Replace `parse_copy_source -> (String, String, Option<String>)` with a typed
   result, for example a `CopySource` struct containing:
   - `bucket: BucketName`
   - `key: ObjectKey`
   - `version_id: Option<String>` or a more specific type if that work already
     exists
2. Audit all non-routing bucket/key entry points, including:
   - `x-amz-copy-source`
   - XML request bodies containing object keys
   - multipart or POST surfaces that synthesize object keys
   - any direct request builders bypassing normal router path validation
3. Ensure percent-decoding happens before type construction where AWS expects
   decoded values to be validated.

Exit criteria:

1. No request parser that yields bucket/key data for core logic returns raw
   bucket/key strings.
2. The `copy-source` class of bug is structurally impossible.

## Phase 3: Tighten coordinator-facing request shapes

1. Change request structs and helper APIs so coordinator-facing operations take
   typed bucket/key values directly.
2. Remove any remaining "validate later" conventions from `server-http`.
3. Audit authz, PG selection, and storage-dispatch entry points so they depend
   on the typed invariants rather than comments or call-order assumptions.
4. Make the PG hashing failure model explicit:
   - either it accepts only strict validated types and keeps any remaining
     assertion as a trusted invariant
   - or it is changed to fail closed without panicking on invalid input

Current status:

1. Complete:
   coordinator-facing request structs now carry typed bucket/key values.
2. Complete:
   `server-http` request construction and the streaming PUT append path now use
   typed values at the coordinator boundary.
3. Partially complete:
   authz, PG selection, and the main object/multipart authorized-state path now
   preserve typed values much further downstream than before.
   Bucket-scoped coordinator helper traits now expose typed bucket access, and
   the raw read-lock and bucket-write-reservation helper variants that were
   only serving older request shapes have been removed from the touched paths.
   Object-tagging/object-lock/object-ACL authorization tokens now also carry
   typed bucket/key values, and the main PutObject / streaming PutObject commit
   helpers reuse those typed values instead of reconstructing trusted names.
   The stale-payload snapshot/delete-marker/reclaim helper path and upload-part
   stream-session helper path now also take typed bucket/key values internally.
   The list fan-out paths now reuse typed buckets and parsed typed
   prefix/marker keys instead of rebuilding trusted names inside each PG query,
   and the streaming append/abort helper path now keeps typed bucket/key values
   through PG routing and session binding as well. Bucket-only coordinator
   helpers such as bucket write reservations, delete-bucket drain/wait, active
   bucket summary loading, and completed-multipart tombstone ordering now use
   typed bucket names on their main path implementations rather than bouncing
   through raw-string wrappers. The lifecycle sweep now also carries typed
   bucket/object names from stored metadata through current-object expiration,
   noncurrent-version expiration, and aborting-upload completion instead of
   reconstructing trusted names at each operation. The `server-http` streaming
   POST / PUT / UploadPart contexts now also store typed bucket/key values
   across their async lifetime instead of degrading them back to `String`, and
   coordinator cleanup entry points for those sessions now take typed names on
   their production path. The non-test `bucket_exists` region-probe path and
   authz bucket-policy PG lookup also now route through typed bucket parsing /
   typed PG accessors rather than leaning on test-only raw helpers. The
   reclaim queue, bucket-delete finalize queue, payload-lease bookkeeping, and
   `PayloadLease` drop path now also carry typed bucket/key values instead of
   raw strings, and the reclaim worker consumes those typed queue items
   directly on its production path. The read-path `ReadObjectContext`,
   multipart/segment `ReadHandle` constructors, stale-payload cleanup path, and
   multipart-abort path now also reuse typed bucket/key values instead of
   reconstructing trusted strings. The object-side `PgMetadataStore` boundary
   now likewise takes typed bucket/object parameters for the main metadata,
   version, reclaim, tags, and parts/segments APIs, so the production
   coordinator/storage dispatch path is substantially typed end to end.
   `BucketSummary`, the bucket policy/lifecycle caches, and the
   `SharedStorageNode` bucket fast-path / bucket-lock / multipart-completion
   lock helpers now also keep typed bucket names on the production path
   instead of downgrading those hot-path coordinator internals back to
   `String`. This pushes the remaining raw-string boundary outward toward the
   true ingress points and a shrinking set of compatibility helpers.
4. Still open:
   some older coordinator-internal helper APIs still accept raw `&str`
   bucket/key parameters, now mostly around external convenience probes, older
   raw PG helper variants, and a few compatibility wrappers that still sit
   above typed PG entry points.
5. Still open:
   the final Phase 3 answer on the failure model is only partly in place today.
   Typed PG entry points exist for validated S3 names, but raw helper entry
   points still remain for internal namespaces and older call paths.

Exit criteria:

1. `server-core` no longer accepts raw external bucket/key strings on its main
   request path APIs.
2. PG hashing and lock-routing code only sees bounded, validated types.
3. The code no longer has an ambiguous mix of "typed upstream, panic
   downstream" for bucket/key bound enforcement.

## Phase 4: Narrow or remove unchecked internal construction

1. Inventory all remaining internal constructors for `BucketName` /
   `ObjectKey`.
2. Separate truly internal trusted creation from convenience creation.
3. Make trusted creation narrow and obvious:
   - crate-private where possible
   - explicit naming where crate-private is not possible
4. Review any remaining persistence and deserialization paths not already
   covered in phase 1:
   - validate on load if data can still be attacker-derived
   - or justify trusted loading based on already-validated storage invariants

Exit criteria:

1. There is no casual unchecked way to construct these domain types.
2. Any remaining trusted path is justified and easy to audit.

## Phase 5: Regression and compatibility coverage

1. Add unit tests directly on `BucketName` and `ObjectKey` construction.
2. Add targeted request-parser tests for every alternate bucket/key source.
3. Add regression tests for oversized and malformed inputs, especially:
   - oversized copy-source bucket names
   - oversized copy-source object keys
   - invalid percent-decoding
   - NUL in object keys
   - bucket-name edge cases currently enforced by routing
4. Add or extend fuzz coverage for parser surfaces that produce typed bucket/key
   values.
5. Confirm AWS-compat behavior for accepted and rejected edge cases remains the
   same where intended.

Exit criteria:

1. The original security finding has direct regression coverage.
2. Boundary validation is tested at both the type level and the parser level.

## Landing Strategy

This rewrite is large enough that it should land in deliberate slices, but the
slices should move monotonically toward the end state rather than leaving a
long-lived mixed model.

Recommended sequence:

1. make `BucketName` / `ObjectKey` fallible and authoritative
2. settle persistence and `FromSql` behavior at the same time, not later
3. convert parsers that currently bypass routing validation
4. convert coordinator-facing request structs and helper signatures
5. remove permissive constructors and clean up the remaining call sites
6. finish with broad regression coverage and a final audit grep

The key rule during the migration should be:

1. do not add new raw-string bucket/key plumbing
2. when touching a path, convert it to the typed boundary rather than layering
   on another validator call
3. do not add new panic-based length assumptions for raw bucket/key strings
   while the rewrite is still incomplete

## Review Checklist

Every patch in this rewrite should be reviewable against the same checklist:

1. Does any external bucket/key input still cross a boundary as `String` or
   `&str`?
2. Is there exactly one authoritative validator for each domain type?
3. Are object-key limits enforced by byte length, not character count?
4. Are error mappings preserved at the HTTP boundary?
5. Is any unchecked constructor clearly justified and narrowly scoped?
6. Did the patch remove a bypass path, or only add another ad hoc check?

## Success Criteria

This work is done when all of the following are true:

1. `BucketName` and `ObjectKey` mean "already validated" everywhere in the
   codebase.
2. Unvalidated names are represented only as raw wire data before parsing.
3. `parse_copy_source` and equivalent alternate parsers return typed results.
4. Coordinator and PG selection code cannot be reached with oversized or
   otherwise invalid bucket/key values from HTTP requests.
5. The security bug from `security/codex-23ffb1b` is fixed as part of a broader
   elimination of the underlying type weakness rather than a one-off patch.
6. The storage/persistence story for strict name types is explicit and
   auditable.
7. The final design has a documented non-ambiguous failure model for bound
   violations in PG routing and similar low-level helpers.

## Related Plans

1. `plans/parser-hardening-plan.md`
   - complementary, but narrower
   - focused on parser safety patterns generally
2. `plans/completed/interface-types-and-illegal-states.md`
   - established `BucketName` / `ObjectKey` as domain types
   - this plan finishes that direction by making the types enforce their own
     invariants
