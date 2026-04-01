# Property Testing Expansion Plan

## Scope

This plan expands property-based testing in the areas where example-driven tests
are least effective: version ordering, suspended-bucket null-version semantics,
lifecycle expiration state transitions, and paginated listing invariants.

In scope:
- storage-level stateful property tests for object version metadata behavior
- reference-model comparison for current-version selection and listing order
- coordinator-level property tests for lifecycle sweep outcomes
- pagination round-trip properties for `ListObjectsV2` and `ListObjectVersions`
- deterministic generators for timestamp ties and suspended-bucket edge cases

Out of scope:
- replacing existing AWS-compat example tests
- broad HTTP/XML property testing
- randomized fault injection for unrelated storage or networking paths

## Why This Is Worth Doing

The recent lifecycle/versioning bugs were not simple parser mistakes. They came
from valid but awkward state combinations:
- a newer null version competing with an older numbered version
- identical `last_modified` values
- currentness leaking through multiple layers with different ordering rules
- lifecycle mutations interacting with suspended versioning semantics

These are exactly the cases property tests are good at finding. The current
suite already has strong example coverage, but it still depends on humans
enumerating the bad interleavings up front.

We already use `proptest` in several crates, including `storage`, so this is an
expansion of the current test strategy rather than a new testing stack.

## Principles

### 1. Keep AWS compatibility as the external oracle

Example-based AWS-port and protocol tests remain the source of truth for HTTP
behavior and wire compatibility. Property tests should primarily validate our
internal state machines and ordering invariants so they can find bugs earlier
and shrink them well.

### 2. Use a small reference model

The most effective property tests here will compare the real implementation to a
small in-memory model with simpler, obviously correct rules for:
- per-key write order
- current version selection
- live-key visibility
- version listing order
- lifecycle current-version expiration effects

The model should be intentionally narrow and explicit rather than trying to
simulate all of S3.

### 3. Bias generation toward known hard cases

Uniform random generation will miss important edge cases too often. Generators
should deliberately emphasize:
- `VersionId::Null` versus numbered versions
- versioning transitions to and from `Suspended`
- delete marker creation and deletion
- identical `last_modified` timestamps
- repeated writes to the same key
- small page sizes for pagination

### 4. Test invariants, not implementation details

The properties should assert observable behavior:
- which version is current
- whether a key is listed
- whether version order is correct
- whether pagination is lossless

They should not lock tests to internal SQL shape or incidental helper structure.

## Target Areas

## 1. Storage version-state model

Add a stateful property test module in `crates/storage/src/tests/metadata_tests.rs`
or a nearby dedicated test module.

Model state per `(bucket, key)`:
- ordered writes, each with:
  - version id (`null` or numbered)
  - live object or delete marker
  - logical write order
- bucket versioning state

Operations to generate:
- create bucket
- set versioning `Enabled`
- set versioning `Suspended`
- put live version with explicit version id chosen by the test harness
- put null live version
- put delete marker with explicit version id
- put null delete marker
- delete specific version

Core properties:
- `get_object_meta` matches the model's current version
- `list_objects` includes a key iff the model's current version is live
- `list_objects` returns the same live object as `get_object_meta`
- `list_object_versions` returns versions in model write order for each key
- exactly one listed version per key has `is_latest = true`
- suspended-bucket null versions and null delete markers dominate older numbered
  versions even when `last_modified` ties

## 2. Pagination properties

Expand the current property coverage from `ListObjectsV2` key pagination into
full stateful pagination checks for both object and version listings.

Properties:
- paginating through all pages yields exactly the same sequence as one large
  request
- no duplicates across pages
- no omissions across pages
- continuation markers always advance
- marker-based resumption works when the marker points into:
  - the middle of a key's version chain
  - the final version of a key
  - a delete marker
  - a null version in a suspended bucket

This should exercise both storage-level pagination and coordinator fan-in.

## 3. Coordinator lifecycle sweep model

Add a focused property-test module for the phase-2 lifecycle worker in
`crates/server-core/src/coordinator.rs` or a dedicated nearby test helper file.

Keep the model intentionally narrow:
- only the phase-2 implemented surface
- current-version expiration
- nonversioned, enabled, and suspended buckets
- no noncurrent expiration, storage-class transitions, or multipart execution

Generated setup:
- bucket versioning state
- short history of writes and delete markers on a small key set
- lifecycle config with a simple `Expiration.Days` rule
- deterministic eligibility timestamps, including UTC-boundary edge cases

Properties:
- lifecycle sweep mutates namespace exactly as the model predicts
- nonversioned expiration permanently removes the object
- enabled-bucket expiration makes the prior live version noncurrent via a new
  delete marker
- suspended-bucket expiration yields a current null delete marker
- `list_objects` and `list_object_versions` after the sweep match the model

## 4. Write-order tie forcing

The recent `write_sequence` fix should become a permanent property target rather
than only a pair of regressions.

Add generator support that periodically forces multiple writes for the same key
to share the same `last_modified` value in the real store while the model
continues to track logical write order separately.

Required property:
- all currentness and listing outcomes are invariant under timestamp ties

## Implementation Plan

### Phase 1: Shared model helpers

Add test-only helpers for:
- simple version-state model structs
- model application of generated operations
- comparison helpers from model state to `StoredObject` / coordinator results
- generator strategies for versioning states, keys, operations, and page sizes

Keep this helper code local to the crates under test unless reuse becomes
obvious. Do not introduce a new crate for this initially.

Status update (2026-04-01): completed for the storage crate.
- Added `crates/storage/src/tests/property_test_support.rs` with a local
  `VersionStateModel`, `ModelOp`, object/version snapshot helpers, and
  deterministic trace rendering for shrinking and regression extraction.
- Added separate strategy families for bounded stateful traces and for the
  pre-existing pagination properties, so phase-2 helpers stay intentionally
  small without reducing pagination coverage for empty buckets, duplicate
  overwrites, or larger key sets.
- Constrained generated stateful traces to reachable versioning behavior by
  synthesizing operations from the current model state instead of emitting
  state-blind raw writes/delete markers.
- Added helper tests that pin down suspended-null precedence, restored
  currentness after deleting the head version, snapshot `is_latest` behavior,
  and generator bounds/reachability invariants.
- Rewired the existing storage pagination properties to consume the shared
  pagination strategies while preserving their broader search space.

### Phase 2: Storage stateful properties

Implement the highest-value storage properties first:
- current-version selection
- live-key visibility
- version ordering
- timestamp-tie invariance

This is the best starting point because failures shrink well and avoid HTTP or
cross-node noise.

Status update (2026-04-01): completed for the initial storage target set.
- Added phase-2 stateful differentials in
  `crates/storage/src/tests/metadata_tests.rs` driven by the shared phase-1
  helpers.
- Added a real `PgStore` trace executor that applies generated `ModelOp`
  sequences against a created bucket while the in-memory model advances in
  lockstep.
- Added per-step differential checks for:
  - `get_object_meta` current-version selection
  - `list_objects` live-key visibility
  - `list_object_versions` ordering and `is_latest`
- Added a timestamp-tie invariance property that replays the same generated
  trace into a second store, forces per-key `last_modified` ties with SQL
  updates, and verifies currentness/listing outcomes remain unchanged and still
  match the model.

### Phase 3: Pagination differential properties

Add round-trip properties that compare:
- paginated versus unpaginated results
- storage results versus coordinator results where appropriate

This phase should explicitly cover marker handling for `ListObjectVersions`,
because marker bugs tend to survive ordinary example tests.

Status update (2026-04-01): storage-side pagination differentials completed;
coordinator fan-in comparison remains for a follow-up slice.
- Added phase-3 storage properties in
  `crates/storage/src/tests/metadata_tests.rs` that run after every generated
  state transition from the shared phase-1/2 trace model.
- Added paginated-vs-unpaginated differential checks for both `list_objects`
  and `list_object_versions`, including page-flattening equality, no-loss/no-
  duplication coverage, and explicit marker advancement assertions.
- Added suffix-resumption checks for:
  - `ListObjects` `start_after`
  - `ListObjectVersions` `(key_marker, version_id_marker)` resumption from every
    returned version entry
  - `ListObjectVersions` key-only `key_marker` resumption after whole per-key
    version chains

### Phase 4: Coordinator lifecycle properties

Once the storage model is stable, add coordinator-level lifecycle sweep
properties using the deterministic sweep hook already added for lifecycle tests.

Restrict the generated operation count and key count aggressively so failures are
fast and shrink cleanly.

## Test Design Constraints

### Keep runtime bounded

These properties should be cheap enough for routine local and CI use.

Initial bounds:
- small key sets, for example `1..=4`
- short operation traces, for example `1..=20`
- small page sizes, for example `1..=5`
- lifecycle scenarios with only one or two rules

Increase complexity only after the first properties are stable.

### Prefer deterministic reproduction

When a property fails, the failing seed and minimal shrunk case should be easy
to copy into a normal regression test. New property modules should include
comments or helper output that make this conversion straightforward.

### Avoid broad random HTTP tests initially

HTTP/XML property tests are possible later, but they are a poor first target
here:
- they are slower
- they shrink worse
- they duplicate protocol coverage that AWS-port tests already provide

The initial effort should stay close to the versioning and lifecycle state
machines.

## Concrete First Properties

These should be implemented first, in order:

1. Storage currentness differential:
   generated per-key write traces match `get_object_meta`
2. Storage version listing differential:
   `list_object_versions` matches model write order and `is_latest`
3. Storage live listing differential:
   `list_objects` matches model current-live visibility
4. Storage pagination round-trip:
   paginated and unpaginated listings are identical
5. Coordinator suspended-bucket lifecycle differential:
   current-version expiration matches model for enabled and suspended buckets

## Risks

- Stateful generators can become too broad and produce hard-to-understand
  failures. The operation set needs to stay intentionally small at first.
- If the model copies implementation assumptions, the tests will not find the
  right bugs. The model must use simple logical write order rather than mirroring
  SQL ordering details.
- Coordinator property tests can become slow if they create too many real
  objects or buckets per case.

## Verification

1. `cargo test -p storage`
2. `cargo test -p server-core`
3. `cargo test --workspace`
4. `cargo clippy --all-targets --all-features -- -D warnings`
5. Add at least one minimized fixed-seed regression when a new property exposes
   a real bug, so the failure remains covered even if generators change later
