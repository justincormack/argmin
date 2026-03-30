# Object Lock / WORM Compatibility Plan

## Scope

This plan covers the missing AWS S3 Object Lock surface that currently blocks
the Ceph object-lock gap:
- 39 Ceph tests in `tmp/s3-tests/s3tests/functional/test_s3.py`
- bucket Object Lock enable/configuration
- version-level retention and legal hold state
- governance and compliance retention modes
- WORM delete semantics, including governance bypass
- default retention rules and their interaction with versioning
- object-lock request/response headers on write and read paths
- multipart upload support for locked objects

This should be treated as a core S3 compatibility feature, not a narrow test
workaround. The data model and API surface need to match AWS, then the tests
need to validate that behavior.

This plan does not try to finish every AWS feature adjacent to Object Lock.
Replication, lifecycle, server-access-logging restrictions, and bucket-policy
condition keys related to retention should be treated as follow-up work once the
core Object Lock surface is correct.

## Current Status

Implemented already:
- Phase 1 test port: `crates/s3-tests/tests/object_lock.rs` exists, was
  validated against real AWS with all 39 cases enabled, and currently keeps the
  38 completed bucket/object API cases active while the remaining 10
  later-phase cases stay `ignore`d until their features land
- explicit Object Lock domain types exist across `s3-types`, storage, and
  coordinator code
- durable Object Lock persistence exists for bucket metadata, live object
  versions, and multipart upload state
- HTTP routing no longer misroutes `?object-lock`, `?retention`, or
  `?legal-hold` into plain bucket/object CRUD
- bucket Object Lock control-plane support:
  - `CreateBucket` honors `x-amz-bucket-object-lock-enabled`
  - `PUT /<bucket>?object-lock`
  - `GET /<bucket>?object-lock`
- bucket-level Object Lock invariants are now enforced in both coordinator and
  storage:
  - Object Lock requires bucket versioning `Enabled`
  - Object Lock cannot be disabled once enabled
  - versioning cannot be suspended on an Object Lock bucket
- AWS-specific bucket error mapping exists for:
  - `InvalidBucketState`
  - `ObjectLockConfigurationNotFoundError`
- object-level Object Lock API support for existing versions:
  - `PUT /<bucket>/<key>?retention`
  - `GET /<bucket>/<key>?retention`
  - `PUT /<bucket>/<key>?legal-hold`
  - `GET /<bucket>/<key>?legal-hold`
- retention/legal-hold XML parsing/rendering and governance/compliance
  transition enforcement for API-driven updates
- write-path Object Lock support for new destination versions:
  - `PutObject`
  - streaming `PutObject` finalize path
  - `CopyObject`
  - `CreateMultipartUpload`
  - `CompleteMultipartUpload`
- bucket default retention is applied on newly committed versions when the
  write request does not carry explicit retention
- bucket versioning
- version-specific reads/deletes and delete markers
- `HeadObject` / `GetObject` metadata plumbing
- multipart upload create/complete paths
- enough auth/ACL foundations for owner-driven object operations

Missing today:
- `HeadObject` / `GetObject` do not project Object Lock headers
- `DeleteObject` / `DeleteObjects` do not enforce WORM semantics or governance
  bypass
- bucket-policy action coverage does not include Object Lock actions
- the remaining 10 `ignore`d cases in `crates/s3-tests/tests/object_lock.rs`
  are now concentrated in later phases:
  - `HeadObject` Object Lock headers
  - WORM delete / multi-delete enforcement

## AWS Contract To Match

The relevant AWS behavior from the official S3 documentation and the Ceph tests
is:

1. Bucket rules
- Object Lock works only on versioned buckets.
- After Object Lock is enabled on a bucket, Object Lock cannot be disabled and
  versioning cannot be suspended.
- Object Lock can be enabled when creating a bucket or later on an existing
  bucket via `PutObjectLockConfiguration`.

2. Bucket configuration rules
- `ObjectLockEnabled` only accepts `Enabled`.
- `DefaultRetention` requires a mode and exactly one of `Days` or `Years`.
- `Days` and `Years` together are malformed.

3. Object-version rules
- lock state is stored per object version, not per key
- retention and legal holds protect only the specified version
- retention/legal hold do not prevent new versions from being written
- retention/legal hold do not prevent simple `DELETE` from adding a delete
  marker

4. Retention modes
- `GOVERNANCE` deletion/shortening can be bypassed only with explicit
  bypass-governance intent
- `COMPLIANCE` cannot be shortened and cannot be relaxed back to governance
- legal holds block delete even if governance bypass is supplied

5. API/header behavior
- `PutObject`, `CopyObject`, and `CreateMultipartUpload` accept Object Lock
  headers on the destination object
- `HeadObject` / `GetObject` return Object Lock headers from version metadata
- `PutObjectRetention`, `GetObjectRetention`, `PutObjectLegalHold`, and
  `GetObjectLegalHold` are version-aware object APIs

6. Integrity requirements
- AWS documents `Content-MD5` / SDK checksum requirements for
  `PutObjectLockConfiguration`, `PutObjectRetention`, `PutObjectLegalHold`, and
  uploads that set retention inline

## Implementation Plan

### Phase 1: Port The Ceph Test Surface

Create `crates/s3-tests/tests/object_lock.rs` by porting all 39 Ceph
`test_object_lock_*` cases from
`tmp/s3-tests/s3tests/functional/test_s3.py`.

Group them the same way the Python suite naturally does:
- bucket configuration
- retention APIs
- legal hold APIs
- upload/header propagation
- delete and multi-delete interactions
- governance/compliance transition rules

Recommended rollout:
- land the full test file early
- validate the full ported test set against real AWS before marking any cases
  ignored, so the Rust tests themselves are proven correct before they become
  the local compatibility target
- temporarily ignore only the cases that are confirmed-valid against AWS and
  still failing solely because Argmin does not implement the feature yet
- remove all ignores before considering the feature complete

AWS validation note from the Phase 1 port:
- the native `object_lock.rs` port passed against AWS with no `ignore`s needed
- `PutObjectLockConfiguration` with bucket default retention `Days=0` or
  `Years=-1` returns `400 InvalidArgument` on AWS, not Ceph's
  `InvalidRetentionPeriod`

This gives us an executable target while the storage and HTTP work is in
progress.

### Phase 2: Add Typed Object Lock Domain Types

Add explicit types instead of threading raw strings and timestamps through the
stack:
- `ObjectLockMode` with `Governance | Compliance`
- `LegalHoldStatus` with `On | Off`
- a tri-state legal-hold representation for stored object metadata so we can
  distinguish:
  - never set
  - explicitly off
  - explicitly on
- `RetentionPeriod` as `Days(nonzero)` or `Years(nonzero)`
- `BucketObjectLockConfig` with:
  - enabled flag
  - optional default retention rule
- `ObjectRetention` with:
  - mode
  - retain-until timestamp

Design rule:
- do not hide Object Lock state inside generic metadata blobs
- make it first-class typed metadata because delete enforcement and response
  headers need direct access to it on hot paths

Likely files:
- `crates/s3-types/src/lib.rs`
- `crates/storage/src/types.rs`
- `crates/server-core/src/coordinator.rs`

### Phase 3: Persist Bucket And Version Lock State

Extend durable metadata so Object Lock is queryable without reconstructing HTTP
state:

1. Bucket metadata
- add bucket-level Object Lock enablement
- add optional default retention mode
- add optional default retention days/years
- add check constraints so illegal combinations are not representable

2. Object version metadata
- add optional retention mode/date on live object versions
- add legal-hold tri-state on live object versions
- forbid lock metadata on delete markers via schema constraints

3. Multipart upload metadata
- persist pending Object Lock state from `CreateMultipartUpload`
- carry that state into the committed version on `CompleteMultipartUpload`

4. Fast-path metadata
- extend `BucketInfo` / `BucketFastPathInfo` with Object Lock bucket state so
  versioning checks do not always fall back to DB lookups

Likely files:
- `crates/storage/src/schema.rs`
- `crates/storage/src/types.rs`
- `crates/storage/src/traits.rs`
- `crates/storage/src/pg_store.rs`

Because the repository is still pre-release, schema updates do not need a
backward-compatibility migration path; the schema can be updated directly.

### Phase 4: Bucket Object Lock APIs

Status update:
- complete
- implemented together with the bucket-level versioning invariants because AWS
  treats those rules as inseparable from bucket Object Lock enablement
- validated by the 11 active bucket-level cases in
  `crates/s3-tests/tests/object_lock.rs`

Implement the bucket-level Object Lock surface:
- `CreateBucket` support for `x-amz-bucket-object-lock-enabled`
- `PUT /<bucket>?object-lock`
- `GET /<bucket>?object-lock`

Required behavior:
- creating a bucket with Object Lock enabled must persist Object Lock enabled
  state and force versioning to `Enabled`
- `PutObjectLockConfiguration` on an existing bucket must fail unless the bucket
  is already versioning-enabled
- `PutObjectLockConfiguration` on a suspended bucket must fail
- enabling Object Lock on an existing bucket becomes a one-way transition

Validation required:
- `ObjectLockEnabled` accepts only `Enabled`
- `DefaultRetention` requires mode + exactly one of days/years
- invalid mode/status values map to AWS-style malformed XML behavior

Likely files:
- `crates/server-http/src/http/router.rs`
- `crates/server-http/src/http/mod.rs`
- `crates/server-http/src/http/xml.rs`
- `crates/server-http/src/http/response.rs`
- `crates/server-core/src/coordinator.rs`

### Phase 5: Versioning Invariants Once Object Lock Is Enabled

Status update:
- complete as part of Phase 4 follow-through
- enforced in both coordinator and storage, so illegal bucket state is not
  representable through direct storage calls either

Update versioning control-plane behavior so it matches AWS once Object Lock is
on:
- reject `PutBucketVersioning(Suspended)` on Object Lock buckets
- reject any effective disable path on Object Lock buckets
- keep idempotent `Enabled -> Enabled` behavior

This needs to be enforced in both:
- coordinator/business logic
- storage-level transition validation where appropriate

Current versioning logic already centralizes transition checks, so this should
be implemented by extending that existing model rather than adding HTTP-only
special cases.

### Phase 6: Object Retention And Legal-Hold APIs

Status update:
- implemented for the direct object APIs
- local verification currently has `30 passed / 10 ignored` in
  `crates/s3-tests/tests/object_lock.rs`
- validated against real AWS with the direct API tests active, including:
  - governance-to-compliance with bypass
  - governance-to-compliance without bypass
  - compliance-to-governance rejection
  - version-aware legal-hold `versionId` reads and writes

Implement:
- `PUT /<bucket>/<key>?retention`
- `GET /<bucket>/<key>?retention`
- `PUT /<bucket>/<key>?legal-hold`
- `GET /<bucket>/<key>?legal-hold`

Required behavior:
- operate on the current version when `versionId` is absent
- operate on the specified version when `versionId` is present
- reject the APIs on buckets without Object Lock enabled
- validate retention mode values exactly
- validate legal-hold status values exactly

Retention update rules:
- allow increasing a governance retention period
- reject shortening governance retention without explicit bypass
- allow shortening governance retention with explicit bypass intent
- reject compliance-to-governance change
- reject compliance shortening
- allow governance-to-compliance only when AWS does; this was verified against
  AWS during Phase 6

Likely files:
- `crates/server-http/src/http/router.rs`
- `crates/server-http/src/http/mod.rs`
- `crates/server-http/src/http/xml.rs`
- `crates/server-http/src/http/response.rs`
- `crates/server-core/src/coordinator.rs`
- `crates/storage/src/traits.rs`
- `crates/storage/src/pg_store.rs`

### Phase 7: Write Paths Must Carry Object Lock State

Status update:
- complete
- local and AWS validation currently have `38 passed / 10 ignored` in
  `crates/s3-tests/tests/object_lock.rs`
- verified against real AWS for:
  - inline `PutObject` Object Lock headers
  - large inline `PutObject` Object Lock requests rejected before stream ingest
  - `CopyObject` destination Object Lock headers
  - `CopyObject` to a plain bucket rejected before source-body streaming
  - multipart initiation with Object Lock headers
  - bucket default retention applied on `PutObject`
  - bucket default retention applied when `CompleteMultipartUpload` publishes
    the final version

Extend all destination-object creation paths to accept and persist Object Lock
state:
- `PutObject`
- streaming `PutObject` finalize path
- `CopyObject`
- `CreateMultipartUpload`
- `CompleteMultipartUpload`

Rules:
- explicit object-lock request headers win
- otherwise, if the bucket has a default retention rule, apply that rule to the
  new version
- legal hold is explicit only; bucket default retention does not imply legal
  hold
- lock metadata belongs to the committed version, not just to transient upload
  state

This should be implemented once in the core object publication path where
possible, so we do not end up with divergent Object Lock behavior across:
- normal put
- copy
- multipart completion

Likely touch points:
- `prepare_put_commit_locked`
- `finalize_put_commit_metadata_locked`
- `copy_object`
- `create_multipart_upload`
- `complete_multipart_upload`

### Phase 8: Response Headers On Read Paths

Project stored Object Lock state back onto read responses:
- `HeadObject`
- `GetObject`

Headers to populate:
- `x-amz-object-lock-mode`
- `x-amz-object-lock-retain-until-date`
- `x-amz-object-lock-legal-hold`

Important subtlety:
- AWS omits the legal-hold header if a version has never had a legal hold
  applied
- that is why the stored representation needs more than a simple boolean

The Ceph tests only check a subset of this, but the response header behavior
should be implemented correctly while the storage model is fresh.

### Phase 9: Enforce WORM Delete Semantics

Update delete behavior to match AWS:

1. `DeleteObject` with `versionId`
- deleting a retained live version must fail with `AccessDenied`
- governance-retained versions can be deleted only with explicit bypass
- legal hold must still block delete even if bypass is present
- delete-marker versions should remain deletable normally

2. simple `DeleteObject` without `versionId`
- must still insert a delete marker on versioned/Object Lock buckets
- must not treat retention on the current live version as a reason to reject the
  delete-marker insertion

3. `DeleteObjects`
- add request-level governance-bypass handling
- preserve per-object success/error reporting
- retained versions should produce per-entry `AccessDenied` errors while other
  deletions in the batch still succeed

Current code already routes multi-delete through single-delete behavior, so the
cleanest shape is:
- add bypass state to delete request types
- enforce retention/legal-hold checks once in `delete_object`
- let `delete_objects` reuse that logic

Likely files:
- `crates/server-core/src/coordinator.rs`
- `crates/server-http/src/http/mod.rs`

### Phase 10: Error Mapping And Auth Surface

Add explicit server-side error variants and mappings instead of squeezing Object
Lock failures through generic `InvalidRequest` where AWS exposes distinct codes.

At minimum we will need AWS-accurate handling for:
- `InvalidBucketState`
- `ObjectLockConfigurationNotFoundError`
- `AccessDenied` on blocked retention/legal-hold deletes
- malformed XML cases for invalid modes/status/default-retention shapes

Also extend bucket-policy action coverage for the Object Lock permissions AWS
documents:
- `s3:GetBucketObjectLockConfiguration`
- `s3:GetObjectRetention`
- `s3:PutObjectRetention`
- `s3:GetObjectLegalHold`
- `s3:PutObjectLegalHold`
- `s3:BypassGovernanceRetention`

The initial owner-driven implementation can land before full policy-granularity
tests exist, but the action names should be added while the APIs are introduced
so we do not need another compatibility pass later.

## Validation Plan

Primary target:
- `cargo test -p s3-tests --test object_lock`

Required regression suites:
- `cargo test -p s3-tests --test versioning`
- `cargo test -p s3-tests --test object_delete`
- `cargo test -p s3-tests --test multipart`
- `cargo test -p s3-tests --test copy_object`
- `cargo test -p s3-tests --test malformed_xml`
- `cargo test -p s3-tests --test object_crud`
- `cargo test -p s3-tests --test bucket_crud`

Repository standards before commit:
- `cargo fmt`
- `cargo clippy --all-targets --all-features -- -D warnings`
- full relevant test suite, and ideally full workspace test suite before the
  final commit for the feature branch

AWS verification:
- run the object-lock subset against real AWS while implementing
- explicitly verify the few behaviors that are not fully nailed down by the
  current Ceph tests:
  - enabling Object Lock on an existing bucket and token/header expectations
  - behavior of `GetObjectRetention` / `GetObjectLegalHold` when nothing was set
  - null-version behavior after retroactive Object Lock enablement

## Expected File Set

The feature should primarily live in:
- `crates/s3-types/src/lib.rs`
- `crates/storage/src/schema.rs`
- `crates/storage/src/types.rs`
- `crates/storage/src/traits.rs`
- `crates/storage/src/pg_store.rs`
- `crates/server-core/src/coordinator.rs`
- `crates/server-core/src/error.rs`
- `crates/server-http/src/http/router.rs`
- `crates/server-http/src/http/mod.rs`
- `crates/server-http/src/http/response.rs`
- `crates/server-http/src/http/xml.rs`
- `crates/auth/src/bucket_policy.rs`
- `crates/s3-tests/tests/object_lock.rs`

## References

Primary AWS sources used for scoping:
- AWS S3 user guide: Configuring Object Lock
  - <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-configure.html>
- AWS S3 user guide: Locking objects with Object Lock
  - <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html>
- AWS S3 user guide: Object Lock considerations / governance bypass
  - <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-managing.html>
- AWS API reference: `CreateBucket`
  - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html>
- AWS API reference: `PutObjectLockConfiguration`
  - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLockConfiguration.html>
- AWS API reference: `GetObjectRetention`
  - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectRetention.html>
- AWS API reference: `PutObjectRetention`
  - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectRetention.html>
- AWS API reference: `GetObjectLegalHold`
  - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectLegalHold.html>
- AWS API reference: `PutObjectLegalHold`
  - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectLegalHold.html>
- AWS API reference: `HeadObject`
  - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html>
- AWS API reference: `PutObject`
  - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html>
- AWS API reference: `CreateMultipartUpload`
  - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html>

Ceph test source:
- `tmp/s3-tests/s3tests/functional/test_s3.py`
- `tmp/s3-tests/s3tests/functional/__init__.py`
