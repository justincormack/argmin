# Lifecycle Expiration Compatibility Plan

## Scope

This plan covers the minimum bucket lifecycle surface we should implement now
to support AWS-compatible retention semantics without introducing storage-class
transitions.

In scope:
- bucket lifecycle configuration CRUD on `/?lifecycle`
- lifecycle rule parsing, validation, persistence, and rendering
- current-version expiration for nonversioned, versioning-enabled, and
  versioning-suspended buckets
- noncurrent version expiration
- expired object delete marker cleanup
- abort incomplete multipart uploads
- lifecycle response headers:
  - `x-amz-expiration` on object write/read metadata surfaces
  - `x-amz-abort-date` and `x-amz-abort-rule-id` on multipart surfaces
- rule matching for the latest lifecycle filter forms:
  - legacy top-level `Prefix`
  - empty `Filter`
  - `Filter.Prefix`
  - `Filter.Tag`
  - `Filter.And`
  - object size filters for object expiration rules

Out of scope:
- storage-class transitions
- archive restore / restore expiry
- restore-related lifecycle behavior
- replication-specific lifecycle behavior
- directory-bucket-specific lifecycle behavior

## Why This Is The Base

The base implementation cannot stop at "delete current objects after N days".
AWS lifecycle expiration semantics are version-aware:
- expiring a current version in a versioning-enabled bucket creates a delete
  marker instead of permanently deleting the version
- noncurrent versions need separate expiration behavior
- expired delete markers need cleanup behavior
- incomplete multipart uploads are part of the same lifecycle surface

That means lifecycle config CRUD plus `Expiration`,
`NoncurrentVersionExpiration`, `ExpiredObjectDeleteMarker`, and
`AbortIncompleteMultipartUpload` are the minimum coherent subset.

## AWS Compatibility Constraints

### 1. Lifecycle is asynchronous

AWS explicitly documents lifecycle expiration and transition as asynchronous.
There can be a delay between an object becoming eligible and the lifecycle
action actually mutating namespace visibility.

That means we should **not** implement "virtual disappearance" on `GET`,
`HEAD`, `LIST`, or `ListObjectVersions` purely because a rule says an object is
past its scheduled expiration time. Doing so would be stricter than AWS and
would make lifecycle timing behavior diverge from real S3.

Design consequence:
- request paths may compute lifecycle metadata and opportunistically enqueue
  background work
- request paths should not synthesize deletion state that has not yet been
  materialized durably by the lifecycle worker

This is also why many Ceph lifecycle timing tests are marked `fails_on_aws`:
they are useful for scope discovery, but they are not normative for AWS timing.

### 2. Expiration semantics depend on bucket versioning state

For current-version expiration:
- nonversioned bucket:
  - permanently remove the object
- versioning-enabled bucket:
  - add a delete marker, making the former current version noncurrent
- versioning-suspended bucket:
  - create a null-version delete marker that replaces the null current version

Additional current-version rules:
- `Expiration` only applies to the current version
- if the current version is already a delete marker and other versions exist,
  `Expiration` does nothing
- if the current version is the only remaining version and is a delete marker,
  it can be removed as an expired object delete marker

### 3. Noncurrent expiration is based on when a version became noncurrent

`NoncurrentVersionExpiration` is not keyed off version creation time. It is
keyed off the time a version stopped being current.

This requires a first-class noncurrent transition timestamp in stored object
metadata. Current schema does not have that.

### 4. Scheduling uses UTC day boundaries

AWS rounds lifecycle day-based eligibility to midnight UTC. We need one shared
helper that computes:
- `Expiration.Days`
- `NoncurrentVersionExpiration.NoncurrentDays`
- `AbortIncompleteMultipartUpload.DaysAfterInitiation`

against the same AWS-style UTC boundary rules.

### 5. Object Lock still applies

Lifecycle actions bypass normal external auth, but they do not bypass Object
Lock semantics. The worker must:
- allow current-version expiration to add a delete marker where AWS does
- skip permanent deletion of protected noncurrent versions
- skip deletion blocked by legal hold / retention state

### 6. Bucket policy does not block lifecycle

AWS documents that bucket-policy denies do not stop lifecycle expiration.
Lifecycle work therefore must use internal server paths, not user-auth paths.

## Current Repo Gaps

There is no lifecycle configuration implementation today:
- router has no `?lifecycle` operation
- HTTP layer has no lifecycle XML parser or renderer
- coordinator has no lifecycle CRUD methods or evaluation cache
- storage has no lifecycle fields on bucket metadata
- object metadata has no `became_noncurrent_at` / equivalent field
- there is no lifecycle background scheduler or worker
- response builders do not emit lifecycle headers
- the internal Rust `crates/s3-tests` suite has no lifecycle test file yet

There is, however, useful infrastructure we should reuse:
- bucket-scoped config patterns already exist for CORS, tagging, encryption,
  ownership controls, and bucket policy
- bucket policy already uses generation-based cache invalidation across
  multiple `Coordinator` instances
- reclaim workers already exist for physical payload cleanup after namespace
  deletion
- multipart uploads already persist `initiated_at`

## Proposed Design

### 1. Add a first-class typed lifecycle model

Introduce typed lifecycle structs in `crates/storage/src/types.rs` rather than
working directly with raw XML on hot paths.

The model should make illegal states unrepresentable:
- `BucketLifecycleConfiguration`
- `LifecycleRule`
- `LifecycleRuleFilter`
- `LifecycleExpiration`
- `NoncurrentVersionExpiration`
- `AbortIncompleteMultipartUpload`

Validation should reject invalid combinations up front, for example:
- more than 1,000 rules
- rule with no action
- `Expiration` with both `Days` and `Date`
- `Expiration` with `ExpiredObjectDeleteMarker` plus `Days` or `Date`
- invalid `Status`
- rule ID length > 255
- duplicate rule IDs when IDs are present
- `Days == 0`
- invalid date format
- `NewerNoncurrentVersions` outside AWS-supported bounds
- `NewerNoncurrentVersions` without a `Filter`
- tag-filter use where AWS forbids it:
  - `ExpiredObjectDeleteMarker`
  - `AbortIncompleteMultipartUpload`

The parser should accept both:
- legacy top-level `Prefix`
- modern `Filter`

The renderer should always emit canonical lifecycle XML from the typed model.

### 2. Persist lifecycle config on the bucket row

Extend bucket metadata with:
- `lifecycle_config` nullable blob/text
- `lifecycle_generation` monotonic integer

The persisted representation should be the normalized typed config, not just the
raw request XML. We want evaluation to consume a validated internal form.

Mirror bucket policy cache structure:
- bucket fast path keeps only:
  - lifecycle present / absent
  - lifecycle generation
- coordinator keeps a generation-keyed parsed lifecycle cache

This avoids reparsing lifecycle XML for every `HEAD`, `PUT Object`,
`CreateMultipartUpload`, and lifecycle scan.

### 3. Track when versions become noncurrent

Extend object metadata with:
- `became_noncurrent_at` nullable timestamp

Set it whenever a previously current live version is superseded by:
- a newer live version
- a delete marker
- a versioning-suspended null-version overwrite that displaces the old current

Leave it `NULL` for the current version.

This timestamp is required for AWS-correct noncurrent expiration evaluation and
for `NewerNoncurrentVersions`.

Because we are pre-release, no migration compatibility work is needed beyond
schema update logic for fresh/opened dev databases.

### 4. Add lifecycle CRUD to HTTP / router / coordinator / storage

Add operations for:
- `PUT /bucket?lifecycle`
- `GET /bucket?lifecycle`
- `DELETE /bucket?lifecycle`

Expected behavior:
- `PUT` requires `Content-MD5`, replaces the entire configuration, and returns `200`
- `GET` on absent config returns `NoSuchLifecycleConfiguration` / `404`
- `DELETE` is idempotent and returns `204`

Implementation should follow existing bucket-config patterns:
- route in `crates/server-http/src/http/router.rs`
- dispatch in `crates/server-http/src/http/mod.rs`
- XML parse/render in `crates/server-http/src/http/xml.rs`
- response builders in `crates/server-http/src/http/response.rs`
- coordinator CRUD methods with bucket-admin authorization
- storage trait + `PgStore` methods

### 5. Build one lifecycle evaluation engine

Add a shared evaluator in `server-core` that answers:
- which delete-oriented lifecycle rule applies to this current object version?
- when does it become eligible?
- which noncurrent versions are eligible right now?
- which incomplete multipart uploads are eligible right now?

The evaluator should:
- apply prefix/tag/and/size filtering
- compute UTC-midnight-based eligibility
- choose the earliest applicable expiration for current-version headers
- handle `NewerNoncurrentVersions`
- distinguish:
  - current live version
  - current delete marker
  - noncurrent live version
  - delete-marker-only key

This same evaluator should drive both:
- response header emission
- background worker decisions

### 6. Add lifecycle response headers

For objects:
- `PutObject`, `CopyObject`, `CompleteMultipartUpload`, `HeadObject`, and
  `GetObject` should emit `x-amz-expiration` when a current-version expiration
  rule applies to that object

For multipart uploads:
- `CreateMultipartUpload` and `ListParts` should emit:
  - `x-amz-abort-date`
  - `x-amz-abort-rule-id`
  when an abort-incomplete-multipart rule applies

These headers should be computed from the same evaluator and should not require
the object/upload to already be due.

### 7. Add a lifecycle background service

We need an async lifecycle manager, but it cannot simply be "one full scanner
per `Coordinator`". `argmin-s3` creates one `Coordinator` per frontend worker,
so naive per-coordinator scanning would duplicate work N times.

Current implementation note:
- Phase 2 currently ships with a per-`Coordinator` background sweeper that
  rechecks bucket/object state under lock before applying expiry actions, so
  semantics are correct but scheduler sharing is still a follow-up item

Required design property:
- one shared scheduler domain per `SharedStorageNode`, not per frontend

Recommended structure:
- add a storage-node-scoped lifecycle work queue similar in spirit to reclaim
- have one scheduler thread periodically enqueue bucket lifecycle scan work
- allow one or more workers to process queued bucket scans

Each scan should:
1. load the bucket lifecycle config
2. scan live current objects for `Expiration`
3. scan version history for `NoncurrentVersionExpiration` and
   `ExpiredObjectDeleteMarker`
4. scan multipart uploads for `AbortIncompleteMultipartUpload`
5. apply due lifecycle actions durably
6. enqueue payload reclaim work when namespace-visible rows are removed

### 8. Lifecycle actions must use internal metadata operations

The worker should not call public request handlers. It should use dedicated
internal coordinator helpers that preserve S3 semantics without external auth.

Needed helpers:
- expire current live object in nonversioned bucket
- expire current live object in versioning-enabled bucket by creating a delete
  marker
- expire current live object in versioning-suspended bucket by creating the
  null-version delete marker behavior
- permanently delete a specific noncurrent version
- remove an expired object delete marker
- abort a specific multipart upload

These helpers must preserve existing invariants:
- delete-marker semantics
- reclaim queue population
- multipart state transitions
- bucket lock ordering / deadlock safety
- Object Lock enforcement

### 9. Keep lifecycle work out of request paths

The base implementation should not opportunistically enqueue lifecycle work from
`GET`, `HEAD`, `LIST`, or multipart read paths.

Reasoning:
- expired objects are usually cold, so request-driven acceleration is unlikely
  to provide meaningful value
- adding lifecycle hooks to hot request paths increases complexity and coupling
- the dedicated background lifecycle service should be sufficient for correctness
  and for predictable operational behavior

If real deployments later show unacceptable backlog or sweep latency, we can
revisit request-assisted nudging as a separate follow-up, but it should not be
part of the initial design.

## Implementation Phases

Current status:
- Phase 1 is complete.
- Phase 2 is complete.
- Phase 3 is complete.
- Phases 4 and 5 remain pending.

### Phase 1: Lifecycle config CRUD and rule model (Completed)

Implement:
- typed lifecycle model
- XML parse/render
- router / dispatch / response plumbing
- storage persistence
- generation-based lifecycle cache
- validation and error mapping

Do not yet execute lifecycle actions, but do wire:
- `x-amz-expiration`
- `x-amz-abort-*`

This gives us lifecycle policy CRUD plus rule introspection first.

Completed in the current implementation:
- bucket lifecycle config CRUD
- strict AWS-compatible lifecycle XML validation
- `Content-MD5` enforcement for `PUT /bucket?lifecycle`
- `x-amz-expiration` for current object versions
- `x-amz-abort-*` for matching multipart uploads

### Phase 2: Current-version expiration (Completed)

Implement:
- UTC eligibility helper
- current-version expiration for all three versioning states
- expired current delete-marker handling rules
- payload reclaim integration

Add deterministic tests for:
- nonversioned object removal
- versioning-enabled delete-marker creation
- versioning-suspended null-version delete-marker behavior

Completed in the current implementation:
- deterministic sweep path for lifecycle execution tests
- background sweeper invoking current-version expiration
- current-version expiration for nonversioned buckets with reclaim integration
- current-version expiration for versioning-enabled buckets via delete marker
- current-version expiration for versioning-suspended buckets via null delete marker

### Phase 3: Noncurrent version expiration (Completed)

Implement:
- `became_noncurrent_at`
- noncurrent eligibility evaluation
- `NewerNoncurrentVersions`
- Object Lock skip behavior for permanent deletion

This is the point where lifecycle becomes truly AWS-correct for versioned
buckets.

Completed in the current implementation:
- nullable `became_noncurrent_at` tracking on live object versions
- transactional noncurrent-transition updates when a new live version or delete
  marker supersedes the current live version
- exact per-key noncurrent lifecycle evaluation in the background sweeper
- `NewerNoncurrentVersions` retention during noncurrent deletion
- Object Lock skip behavior for permanently deleting noncurrent versions
- deterministic tests covering versioned, suspended, retention-count, and
  Object Lock noncurrent expiration behavior

### Phase 4: Expired delete markers and incomplete multipart uploads

Implement:
- explicit `ExpiredObjectDeleteMarker`
- automatic delete-marker cleanup implied by `Expiration.Days`
- abort incomplete multipart uploads
- multipart abort response headers

### Phase 5: External compatibility pass

Add or port Rust integration coverage across `s3-tests` and
`s3-local-tests`, then run:
- local targeted tests
- full repo test suite
- AWS-backed lifecycle spot checks for any behavior where docs are incomplete

## Test Plan

### 1. Parser / validation tests

Add HTTP XML tests for:
- minimal valid lifecycle config
- full valid config using current delete-oriented actions
- duplicate rule IDs
- ID length > 255
- invalid `Status`
- invalid date format
- `Expiration.Days == 0`
- invalid action combinations
- invalid filter combinations
- `NewerNoncurrentVersions` without filter
- forbidden tag filters for EODM / abort-MPU rules

### 2. Storage tests

Add `PgStore` coverage for:
- bucket lifecycle config round trip
- lifecycle generation increments on replace/delete
- object `became_noncurrent_at` updates on overwrite / delete-marker publish
- multipart lifecycle scan inputs (`initiated_at`) remain stable

### 3. Coordinator tests

Add deterministic `server-core` tests for:
- `GET` lifecycle config absent -> `NoSuchLifecycleConfiguration`
- `DELETE` lifecycle config idempotence
- `x-amz-expiration` header emission for:
  - prefix rules
  - tag rules
  - `And` rules
  - size-filtered rules
- versioning-enabled current expiration produces delete marker
- versioning-suspended current expiration follows null-version rules
- noncurrent expiration deletes only versions old enough to qualify
- `NewerNoncurrentVersions` retains the newest required noncurrent versions
- expired object delete marker cleanup
- abort incomplete multipart uploads
- Object Lock prevents permanent noncurrent deletion
- bucket policy deny does not block lifecycle worker actions

These tests should not rely on sleeps. Add a controllable lifecycle clock or a
manual "run one lifecycle sweep at time T" hook.

### 4. AWS-portable integration tests (`crates/s3-tests`)

Add `crates/s3-tests/tests/lifecycle.rs` and port only the AWS-portable subset
of the Ceph lifecycle tests.

Port first:
- lifecycle set / get / delete
- absent get -> `NoSuchLifecycleConfiguration`
- invalid ID / duplicate ID / invalid status / invalid date / days=0
- prefix-filter expiration header tests
- tag / `And` filter expiration header tests
- config allowing noncurrent expiration
- config allowing EODM
- config allowing abort incomplete multipart uploads
- multipart abort header reporting where AWS exposes it

Do not put sweep-driven expiration assertions here. `crates/s3-tests` should
stay focused on:
- lifecycle configuration API behavior
- validation behavior
- request/response header behavior that can be observed on AWS
- any ambiguity-resolution checks we want to compare directly against AWS

Do **not** port Ceph timing tests verbatim when they are marked `fails_on_aws`.
Also do not add tests here that require local control of the background
lifecycle worker, because those cannot run meaningfully against S3.

### 5. Local-only lifecycle integration tests (`crates/s3-local-tests`)

Use `crates/s3-local-tests` for full integration tests that keep the same
general structure as `s3-tests` but rely on local-only test hooks.

Add lifecycle-specific local helpers there for:
- a controllable lifecycle clock, or
- a manual "run one lifecycle sweep at time T" hook

That crate should own the tests for:
- current expiration in nonversioned buckets
- current expiration in versioning-enabled buckets
- current expiration in versioning-suspended buckets
- noncurrent version expiration
- `NewerNoncurrentVersions`
- expired object delete marker cleanup
- abort incomplete multipart uploads
- Object Lock interactions with lifecycle deletion
- reclaim integration after lifecycle-driven namespace deletion

These should be deterministic end-to-end integration tests, not unit tests, but
they should not attempt to run against AWS.

### 6. AWS verification pass

Before locking down ambiguous edge behavior, run targeted AWS checks for cases
where docs are incomplete.

Most important verification items:
- omitted `ID` behavior on `GET`
- exact rule chosen for `x-amz-expiration` when multiple expiration rules match
- exact header formatting details for encoded rule IDs

### 7. Final verification commands

At the end of implementation, run:
- `cargo fmt`
- `cargo clippy --all-targets --all-features -- -D warnings`
- targeted lifecycle test packages in both `s3-tests` and `s3-local-tests`
- full `cargo test`
- full AWS-backed `cargo test -p s3-tests --no-fail-fast`

## Recommended Ordering

1. lifecycle rule model + CRUD (completed)
2. deterministic lifecycle clock / sweep hook
3. current-version expiration
4. noncurrent version expiration
5. expired delete markers
6. abort incomplete multipart uploads
7. external lifecycle compatibility suite

## Source Notes

Primary AWS references used for this plan:
- PutBucketLifecycleConfiguration
  - https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html
- GetBucketLifecycleConfiguration
  - https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLifecycleConfiguration.html
- DeleteBucketLifecycle
  - https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketLifecycle.html
- LifecycleRule
  - https://docs.aws.amazon.com/AmazonS3/latest/API/API_LifecycleRule.html
- Expiring objects
  - https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-expire-general-considerations.html
- Lifecycle configuration examples
  - https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-configuration-examples.html
- Troubleshooting S3 Lifecycle issues
  - https://docs.aws.amazon.com/AmazonS3/latest/userguide/troubleshoot-lifecycle.html
- CreateMultipartUpload
  - https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
- PutObject
  - https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html

Ceph compatibility references used for scope mapping:
- `tmp/s3-tests/s3tests/functional/test_s3.py`
- `tmp/s3-tests/TEST-NOTES.md`
