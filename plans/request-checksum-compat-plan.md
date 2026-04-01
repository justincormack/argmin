# Request Checksum Compatibility Plan

## Scope

This plan tracks the remaining compatibility work for request-body checksum
requirements on S3 operations that currently use `Content-MD5` and the newer
SDK checksum family (`x-amz-sdk-checksum-algorithm` plus a matching
`x-amz-checksum-*` header or checksum trailer).

This plan covers:
- implemented S3 operations in Argmin whose AWS-compatible behavior depends on
  request checksum enforcement
- the full `s3-tests` coverage needed to prove both required and optional cases
- the distinction between `Content-MD5`-only operations and operations that
  accept either checksum mechanism

This plan does not cover:
- unsupported S3 APIs such as bucket logging, bucket website, bucket request
  payment, or bucket replication
- directory bucket behavior
- response checksum behavior

## AWS Rules To Match

The current AWS API reference and SDK model imply the following rules:

1. `Content-MD5` is a complete request checksum mechanism by itself.

2. `x-amz-sdk-checksum-algorithm` is not a checksum by itself.
- it must be accompanied by a matching `x-amz-checksum-*` header or
  `x-amz-trailer`
- sending only `x-amz-sdk-checksum-algorithm` is invalid

3. Most checksum-required bucket and object subresource writes accept either:
- `Content-MD5`
- or the newer SDK checksum family

4. `DeleteObjects` is the important exception.
- AWS documents `Content-MD5` specifically as required for general purpose
  buckets
- this operation should not be loosened to "any checksum family is fine"
  without confirming that behavior against AWS

5. `PutObject` is the important allow-missing exception.
- checksum is generally optional
- but AWS requires `Content-MD5` or `x-amz-sdk-checksum-algorithm` when the
  upload sets Object Lock retention

6. Existing SDK-driven positive tests are not sufficient proof of enforcement.
- for many operations the AWS SDK auto-populates request checksums
- those tests prove that one checksum-bearing request path succeeds
- they do not prove that Argmin rejects missing checksums
- they also do not prove that Argmin accepts raw SDK-checksum-only requests

## Current Argmin Status

Current server enforcement in `crates/server-http/src/http/mod.rs`:
- `require_content_md5(req)` is used only for `DeleteObjects` and
  `PutBucketLifecycle`
- `validate_content_md5(req)` is used for `PutObject`,
  `PutObjectRetention`, and `PutObjectLegalHold`
- the newer checksum family is only parsed and validated on `PutObject` and
  multipart object data paths
- the remaining checksum-required metadata APIs currently do not enforce
  request checksum requirements

Current focused `s3-tests` coverage:
- `DeleteObjects` missing `Content-MD5` rejection:
  `crates/s3-tests/tests/checksums.rs`
- `PutBucketLifecycle` missing `Content-MD5` rejection:
  `crates/s3-tests/tests/lifecycle.rs`

That means the current integration suite does not yet protect the broader AWS
compatibility surface.

## Required Operation Matrix

The table below lists the implemented operations whose request checksum
requirements need to be correct.

### Implemented Operations With AWS Checksum Requirement

| AWS operation | Argmin operation | AWS requirement | Current Argmin behavior | Current tests | Coverage gap |
| --- | --- | --- | --- | --- | --- |
| `DeleteObjects` | `DeleteObjects` | `Content-MD5` required specifically | Correctly requires `Content-MD5` | Focused missing-header negative in `checksums.rs`; broad positive-path use in `object_delete.rs`, `versioning.rs`, `object_lock.rs`, `expected_bucket_owner.rs` | No explicit test that SDK checksum headers do not substitute for `Content-MD5`; no raw positive test proving the exact accepted request shape |
| `PutBucketAcl` | `PutBucketAcl` | Request checksum required | No request checksum enforcement | Positive-path coverage in `ownership.rs`, `public_access_block.rs`, `access_matrix.rs`, `expected_bucket_owner.rs`, `multipart.rs`, `copy_object.rs`, `object_lock.rs` | No missing-checksum negative; no raw `Content-MD5` positive; no raw SDK-checksum-only positive |
| `PutBucketCors` | `PutBucketCors` | Request checksum required | No request checksum enforcement | Positive-path coverage in `cors.rs`, `expected_bucket_owner.rs` | No missing-checksum negative; no raw `Content-MD5` positive; no raw SDK-checksum-only positive |
| `PutBucketEncryption` | `PutBucketEncryption` | Request checksum required | No request checksum enforcement | Positive-path coverage in `bucket_encryption.rs`, `expected_bucket_owner.rs` | No missing-checksum negative; no raw `Content-MD5` positive; no raw SDK-checksum-only positive |
| `PutBucketLifecycleConfiguration` | `PutBucketLifecycle` | Request checksum required | Requires `Content-MD5` only | Focused missing-header negative in `lifecycle.rs`; positive-path lifecycle coverage in `lifecycle.rs` | No raw SDK-checksum-only positive; no explicit test that both checksum families remain accepted if enforcement is generalized |
| `PutBucketOwnershipControls` | `PutBucketOwnershipControls` | Request checksum required | No request checksum enforcement | Positive-path coverage in `ownership.rs`, `multipart.rs`, `presigned.rs`, `bucket_policy.rs`, `object_lock.rs`, `access_matrix.rs`, `copy_object.rs`, `object_crud.rs`, `expected_bucket_owner.rs` | No missing-checksum negative; no raw `Content-MD5` positive; no raw SDK-checksum-only positive |
| `PutBucketPolicy` | `PutBucketPolicy` | Request checksum required | No request checksum enforcement | Positive-path coverage in `bucket_policy.rs`, `public_access_block.rs`, `post_object.rs`, `object_lock.rs`, `tagging.rs`, `expected_bucket_owner.rs` | No missing-checksum negative; no raw `Content-MD5` positive; no raw SDK-checksum-only positive |
| `PutBucketTagging` | `PutBucketTagging` | Request checksum required | No request checksum enforcement | Positive-path coverage in `tagging.rs`, `expected_bucket_owner.rs` | No missing-checksum negative; no raw `Content-MD5` positive; no raw SDK-checksum-only positive |
| `PutBucketVersioning` | `PutBucketVersioning` | Request checksum required | No request checksum enforcement | Positive-path coverage in `versioning.rs`, `object_delete.rs`, `multipart.rs`, `expected_bucket_owner.rs`, `object_lock.rs`, `object_attributes.rs`, `copy_object.rs`, `deep_coverage.rs`, `bucket_list.rs`, `tagging.rs`, `conditional.rs`, `bucket_crud.rs` | No missing-checksum negative; no raw `Content-MD5` positive; no raw SDK-checksum-only positive |
| `PutObjectAcl` | `PutObjectAcl` | Request checksum required | No request checksum enforcement | Positive-path coverage in `versioning.rs`, `object_crud.rs`, `access_matrix.rs`, `copy_object.rs`, `expected_bucket_owner.rs` | No missing-checksum negative; no raw `Content-MD5` positive; no raw SDK-checksum-only positive |
| `PutObjectLegalHold` | `PutObjectLegalHold` | Request checksum required | Only validates `Content-MD5` if present | Positive-path coverage in `object_lock.rs`, `expected_bucket_owner.rs` | No missing-checksum negative; no raw `Content-MD5` positive; no raw SDK-checksum-only positive; current behavior is too weak |
| `PutObjectLockConfiguration` | `PutBucketObjectLockConfiguration` | Request checksum required | No request checksum enforcement | Positive-path coverage in `object_lock.rs`, `expected_bucket_owner.rs` | No missing-checksum negative; no raw `Content-MD5` positive; no raw SDK-checksum-only positive |
| `PutObjectRetention` | `PutObjectRetention` | Request checksum required | Only validates `Content-MD5` if present | Positive-path coverage in `object_lock.rs`, `expected_bucket_owner.rs` | No missing-checksum negative; no raw `Content-MD5` positive; no raw SDK-checksum-only positive; current behavior is too weak |
| `PutObjectTagging` | `PutObjectTagging` | Request checksum required | No request checksum enforcement | Positive-path coverage in `tagging.rs`, `deep_coverage.rs`, `expected_bucket_owner.rs` | No missing-checksum negative; no raw `Content-MD5` positive; no raw SDK-checksum-only positive |
| `PutPublicAccessBlock` | `PutBucketPublicAccessBlock` | Request checksum required | No request checksum enforcement | Positive-path coverage in `public_access_block.rs`, `expected_bucket_owner.rs`, `multipart.rs`, `copy_object.rs` | No missing-checksum negative; no raw `Content-MD5` positive; no raw SDK-checksum-only positive |

### Unsupported AWS Operations With Checksum Requirement

These are out of immediate implementation scope, but listed here so the AWS
surface is visible:
- `PutBucketLogging`
- `PutBucketReplication`
- `PutBucketRequestPayment`
- `PutBucketWebsite`

If Argmin implements any of these APIs later, checksum enforcement and tests
should be added at the same time.

## Exception And Guardrail Matrix

These are operations where the important compatibility requirement is that
missing `Content-MD5` must continue to be allowed in at least some cases.

| AWS operation | AWS requirement | Current Argmin behavior | Current tests | Coverage gap |
| --- | --- | --- | --- | --- |
| `PutObject` | Checksum generally optional; required when Object Lock retention is set; `x-amz-sdk-checksum-algorithm` requires matching checksum value/trailer | Optional `Content-MD5` validation and checksum-family validation already exist, but there is no Object Lock-specific required-checksum enforcement | Positive `Content-MD5` and checksum-family coverage in `checksums.rs`; broad object-lock behavior coverage in `object_lock.rs` | No explicit raw allow-missing test; no Object Lock missing-checksum negative; no Object Lock SDK-checksum-only positive |
| `UploadPart` | No general `Content-MD5` requirement for normal SigV4 uploads; optional checksum mechanisms must remain accepted when present | Optional `Content-MD5` and checksum-family support exists on multipart data paths | Positive `Content-MD5` in `checksums.rs`; broader multipart checksum coverage in `checksums.rs` and `multipart.rs` | No explicit raw allow-missing test, so a shared refactor could accidentally make UploadPart too strict |
| `CompleteMultipartUpload` | No request checksum requirement | No request checksum enforcement | Broad positive-path multipart coverage in `checksums.rs` and `multipart.rs` | No explicit allow-missing guard test |

## Test Requirements

For each implemented operation in the required matrix, the final `s3-tests`
coverage should include:

1. Missing checksum negative
- send a raw signed request without `Content-MD5`
- without `x-amz-sdk-checksum-algorithm`
- and without any `x-amz-checksum-*` header or trailer
- assert AWS-matching failure

2. `Content-MD5` positive
- send a raw signed request with valid `Content-MD5`
- assert success

3. SDK checksum-family positive
- send a raw signed request with:
  `x-amz-sdk-checksum-algorithm`
- and a matching `x-amz-checksum-*` header
- assert success

`DeleteObjects` needs a different matrix:
- missing checksum must fail
- valid `Content-MD5` must succeed
- SDK checksum family should not be treated as a substitute unless confirmed
  against AWS

The exception matrix needs explicit guard tests:

1. `PutObject`
- missing checksum succeeds in the normal case
- missing checksum fails when Object Lock retention is requested
- `Content-MD5` succeeds for Object Lock upload
- SDK checksum family succeeds for Object Lock upload

2. `UploadPart`
- missing checksum succeeds
- optional `Content-MD5` still succeeds
- optional SDK checksum family still succeeds

3. `CompleteMultipartUpload`
- missing checksum succeeds

## Test Strategy

The tests need to bypass SDK auto-checksum behavior wherever the checksum
presence itself is under test.

Recommended approach:
- build on the existing raw signed-request helpers already present in
  `lifecycle.rs`, `object_lock.rs`, `public_access_block.rs`, `ownership.rs`,
  and the existing SDK `mutate_request` hooks
- keep existing SDK-based positive-path tests
- add explicit raw-request conformance tests for checksum presence and absence

The helper surface should support:
- no checksum headers
- `Content-MD5`
- `x-amz-sdk-checksum-algorithm` plus matching `x-amz-checksum-*`
- intentionally malformed checksum combinations

This is preferable to trying to infer absence from SDK-driven requests, because
the SDK model auto-populates checksums for many of these operations.

## Implementation Phases

### Phase 1: Consolidate Existing Test Helpers

We already have raw signed-request helper patterns in the test suite. The work
here is to consolidate and extend them so new checksum tests do not duplicate
signing logic per file.

Extend the existing helper surface to cover:
- raw body subresource `PUT`
- raw `POST ?delete`
- optional `Content-MD5`
- optional SDK checksum-family headers

Deliverable:
- a small shared helper API that lets tests express the checksum mechanism
  directly without rewriting request-signing code in each test file

### Phase 2: Missing-Coverage Tests

Add focused tests for every implemented required operation.

Deliverables:
- one missing-checksum negative per required operation
- one raw `Content-MD5` positive per required operation
- one raw SDK-checksum-family positive per required operation, except
  `DeleteObjects` until AWS behavior is confirmed

### Phase 3: Exception Tests

Add explicit allow-missing and conditional-requirement tests for:
- `PutObject`
- `UploadPart`
- `CompleteMultipartUpload`

Deliverables:
- guard tests that prevent broad shared enforcement from becoming stricter than
  AWS

### Phase 4: Server Enforcement

Replace the current ad hoc `Content-MD5` checks with explicit request-checksum
requirements per operation.

Recommended enforcement shape:
- `ContentMd5Only`
- `ContentMd5OrSdkChecksum`
- `Optional`
- `OptionalExceptObjectLockRetention`

Deliverables:
- a single per-operation requirement table in `server-http`
- central validation that distinguishes:
  - missing checksum
  - bare `x-amz-sdk-checksum-algorithm`
  - checksum algorithm/value mismatch
  - bad digest vs invalid digest

### Phase 5: AWS Confirmation Pass

Before finalizing enforcement, run the focused new tests against AWS for the
implemented operations that currently have no raw checksum conformance coverage.

This is especially important for:
- `DeleteObjects` checksum-family substitution behavior
- lifecycle request-checksum-family acceptance
- object lock request-checksum requirement boundaries on `PutObject`

## Verification Plan

During implementation:
- run the targeted `s3-tests` files touched by the new cases

Before completion:
- run the full test suite
- rerun the narrowed checksum conformance subset against AWS

Likely targeted commands while working:
- `cargo test -p s3-tests --test checksums`
- `cargo test -p s3-tests --test lifecycle`
- `cargo test -p s3-tests --test object_lock`
- `cargo test -p s3-tests --test tagging`
- `cargo test -p s3-tests --test public_access_block`
- `cargo test -p s3-tests --test ownership`
- `cargo test -p s3-tests --test cors`
- `cargo test -p s3-tests --test bucket_policy`
- `cargo test -p s3-tests --test bucket_encryption`
- `cargo test -p s3-tests --test versioning`
- `cargo test -p s3-tests --test expected_bucket_owner`

Repository-wide verification before merge:
- `cargo fmt`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --workspace --no-fail-fast`

## Notes

- `PutBucketLifecycleConfiguration` is the current AWS API name; Argmin still
  uses `PutBucketLifecycle` in routing and internal naming.
- AWS’s current direction is "request checksum required" rather than
  "`Content-MD5` required everywhere".
- The test plan therefore must prove both acceptance and rejection behavior,
  not just missing-`Content-MD5` failures.
