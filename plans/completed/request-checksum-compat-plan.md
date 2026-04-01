# Request Checksum Compatibility Plan

## Scope

This plan tracks the remaining compatibility work for request-body checksum
requirements on S3 operations that currently use `Content-MD5` and the newer
checksum family (`x-amz-checksum-algorithm` plus a matching
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

2. `x-amz-checksum-algorithm` is not a checksum by itself.
- it must be accompanied by a matching `x-amz-checksum-*` header or
  `x-amz-trailer`
- sending only `x-amz-checksum-algorithm` is invalid on operations where AWS
  requires a request checksum, but AWS may ignore it on allow-missing
  operations

3. Most checksum-required bucket and object subresource writes accept either:
- `Content-MD5`
- or the newer checksum family

4. `DeleteObjects` is not `Content-MD5`-only in practice.
- AWS documents `Content-MD5` specifically for general purpose buckets
- AWS also accepts the newer checksum family as an alternative
- the AWS-matching missing-checksum message is:
  `Missing required header for this request: Content-MD5 OR x-amz-checksum-*`

5. `PutObject` is the important allow-missing exception.
- checksum is generally optional
- but AWS requires `Content-MD5` or `x-amz-checksum-*` when the
  upload sets Object Lock retention

6. Existing SDK-driven positive tests are not sufficient proof of enforcement.
- for many operations the AWS SDK auto-populates request checksums
- those tests prove that one checksum-bearing request path succeeds
- they do not prove that Argmin rejects missing checksums
- they also do not prove that Argmin accepts raw SDK-checksum-only requests

## Current Argmin Status

Current server enforcement in `crates/server-http/src/http/mod.rs`:
- `DeleteObjects` requires either `Content-MD5` or a checksum value header
  from the newer checksum family
- the implemented checksum-required bucket and object subresource writes that
  AWS actually enforces now require either `Content-MD5` or a checksum value
  header from the newer checksum family
- `PutObject` remains allow-missing in the normal case, but now requires a
  request checksum when Object Lock retention headers are present
- bare `x-amz-checksum-algorithm` without a matching `x-amz-checksum-*`
  header or trailer is rejected on checksum-required operations, matching AWS
- `UploadPart` continues to allow missing checksums, and bare
  `x-amz-checksum-algorithm` on its own remains allowed there, matching AWS

Current focused `s3-tests` coverage:
- `DeleteObjects` missing `Content-MD5` rejection:
  `crates/s3-tests/tests/checksums.rs`
- `PutBucketLifecycle` missing `Content-MD5` rejection:
  `crates/s3-tests/tests/lifecycle.rs`
- raw request checksum matrix coverage:
  `crates/s3-tests/tests/request_checksums.rs`
  - bucket and object subresource writes: per-operation allow-missing or
    missing-checksum-negative coverage, plus `Content-MD5` and checksum-family
    positives
  - ACL behavior verified against AWS:
    `PutBucketAcl` and `PutObjectAcl` allow missing checksums for both XML-body
    ACL requests and header-only canned ACL requests
  - object subresource writes implemented in the matrix:
    `PutObjectAcl`, `PutObjectTagging`, `PutObjectLegalHold`,
    `PutObjectRetention`
  - exceptions: allow-missing `PutObject`, `UploadPart`,
    `CompleteMultipartUpload`, Object Lock `PutObject`
    missing-checksum-negative plus checksum-bearing positives, and optional
    checksum-bearing `UploadPart` positives
  - guardrail: bare `x-amz-checksum-algorithm` rejection

## Required Operation Matrix

The table below lists the implemented operations whose request checksum
requirements need to be correct.

### Implemented Operations With AWS Checksum Requirement

| AWS operation | Argmin operation | AWS requirement | Current Argmin behavior | Current tests | Coverage gap |
| --- | --- | --- | --- | --- | --- |
| `DeleteObjects` | `DeleteObjects` | Request checksum required; AWS accepts `Content-MD5` or the newer checksum family | Matches AWS: missing checksum rejected, `Content-MD5` and checksum-family requests accepted | Focused missing-header negative in `checksums.rs`; raw missing-header negative plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; broad positive-path use in `object_delete.rs`, `versioning.rs`, `object_lock.rs`, `expected_bucket_owner.rs` | No remaining focused integration gap for implemented behavior |
| `PutBucketAcl` | `PutBucketAcl` | AWS allows missing checksum, including XML-body ACL updates confirmed by focused AWS tests | Matches AWS allow-missing behavior; checksum headers are still validated if present | Raw XML allow-missing plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; header-only allow-missing guard in `request_checksums.rs`; broader positive-path coverage in `ownership.rs`, `public_access_block.rs`, `access_matrix.rs`, `expected_bucket_owner.rs`, `multipart.rs`, `copy_object.rs`, `object_lock.rs` | No remaining focused integration gap for implemented behavior |
| `PutBucketCors` | `PutBucketCors` | Request checksum required | Requires `Content-MD5` or checksum family | Raw missing-checksum negative plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; positive-path coverage in `cors.rs`, `expected_bucket_owner.rs` | No remaining focused integration gap for implemented behavior |
| `PutBucketEncryption` | `PutBucketEncryption` | AWS allows missing checksum | Matches AWS allow-missing behavior; checksum headers are still validated if present | Raw allow-missing plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; positive-path coverage in `bucket_encryption.rs`, `expected_bucket_owner.rs` | No remaining focused integration gap for implemented behavior |
| `PutBucketLifecycleConfiguration` | `PutBucketLifecycle` | Request checksum required; AWS uses the missing-header message `Missing required header for this request: Content-MD5` | Matches AWS: checksum required, `Content-MD5` or checksum-family accepted, lifecycle-specific missing-checksum message | Focused missing-header negative in `lifecycle.rs`; raw missing-header negative plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; positive-path lifecycle coverage in `lifecycle.rs` | No remaining focused integration gap for implemented behavior |
| `PutBucketOwnershipControls` | `PutBucketOwnershipControls` | AWS allows missing checksum | Matches AWS allow-missing behavior; checksum headers are still validated if present | Raw allow-missing plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; positive-path coverage in `ownership.rs`, `multipart.rs`, `presigned.rs`, `bucket_policy.rs`, `object_lock.rs`, `access_matrix.rs`, `copy_object.rs`, `object_crud.rs`, `expected_bucket_owner.rs` | No remaining focused integration gap for implemented behavior |
| `PutBucketPolicy` | `PutBucketPolicy` | AWS allows missing checksum | Matches AWS allow-missing behavior; checksum headers are still validated if present | Raw allow-missing plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; positive-path coverage in `bucket_policy.rs`, `public_access_block.rs`, `post_object.rs`, `object_lock.rs`, `tagging.rs`, `expected_bucket_owner.rs` | No remaining focused integration gap for implemented behavior |
| `PutBucketTagging` | `PutBucketTagging` | Request checksum required | Requires `Content-MD5` or checksum family | Raw missing-checksum negative plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; positive-path coverage in `tagging.rs`, `expected_bucket_owner.rs` | No remaining focused integration gap for implemented behavior |
| `PutBucketVersioning` | `PutBucketVersioning` | AWS allows missing checksum | Matches AWS allow-missing behavior; checksum headers are still validated if present | Raw allow-missing plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; positive-path coverage in `versioning.rs`, `object_delete.rs`, `multipart.rs`, `expected_bucket_owner.rs`, `object_lock.rs`, `object_attributes.rs`, `copy_object.rs`, `deep_coverage.rs`, `bucket_list.rs`, `tagging.rs`, `conditional.rs`, `bucket_crud.rs` | No remaining focused integration gap for implemented behavior |
| `PutObjectAcl` | `PutObjectAcl` | AWS allows missing checksum, including XML-body ACL updates confirmed by focused AWS tests | Matches AWS allow-missing behavior; checksum headers are still validated if present | Raw XML allow-missing plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; header-only allow-missing guard in `request_checksums.rs`; broader positive-path coverage in `versioning.rs`, `object_crud.rs`, `access_matrix.rs`, `copy_object.rs`, `expected_bucket_owner.rs` | No remaining focused integration gap for implemented behavior |
| `PutObjectLegalHold` | `PutObjectLegalHold` | Request checksum required | Requires `Content-MD5` or checksum family | Raw missing-checksum negative plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; positive-path coverage in `object_lock.rs`, `expected_bucket_owner.rs` | No remaining focused integration gap for implemented behavior |
| `PutObjectLockConfiguration` | `PutBucketObjectLockConfiguration` | Request checksum required | Requires `Content-MD5` or checksum family | Raw missing-checksum negative plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; positive-path coverage in `object_lock.rs`, `expected_bucket_owner.rs` | No remaining focused integration gap for implemented behavior |
| `PutObjectRetention` | `PutObjectRetention` | Request checksum required | Requires `Content-MD5` or checksum family | Raw missing-checksum negative plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; positive-path coverage in `object_lock.rs`, `expected_bucket_owner.rs` | No remaining focused integration gap for implemented behavior |
| `PutObjectTagging` | `PutObjectTagging` | AWS allows missing checksum | Matches AWS allow-missing behavior; checksum headers are still validated if present | Raw allow-missing plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; positive-path coverage in `tagging.rs`, `deep_coverage.rs`, `expected_bucket_owner.rs` | No remaining focused integration gap for implemented behavior |
| `PutPublicAccessBlock` | `PutBucketPublicAccessBlock` | AWS allows missing checksum | Matches AWS allow-missing behavior; checksum headers are still validated if present | Raw allow-missing plus `Content-MD5` and checksum-family positives in `request_checksums.rs`; positive-path coverage in `public_access_block.rs`, `expected_bucket_owner.rs`, `multipart.rs`, `copy_object.rs` | No remaining focused integration gap for implemented behavior |

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
| `PutObject` | Checksum generally optional; required when Object Lock retention is set; checksum-algorithm declarations require a matching checksum value/trailer when AWS is enforcing a checksum | Matches AWS behavior for allow-missing, Object Lock required-checksum, and algorithm-only rejection on required operations | Positive `Content-MD5` and checksum-family coverage in `checksums.rs`; broad object-lock behavior coverage in `object_lock.rs`; raw allow-missing, Object Lock missing-checksum negative, and Object Lock checksum-bearing positives in `request_checksums.rs` | No remaining focused integration gap for implemented behavior |
| `UploadPart` | No general `Content-MD5` requirement for normal SigV4 uploads; optional checksum mechanisms must remain accepted when present | Allow-missing remains supported; bare `x-amz-checksum-algorithm` on its own is also allowed here, matching AWS | Positive `Content-MD5` in `checksums.rs`; broader multipart checksum coverage in `checksums.rs` and `multipart.rs`; raw allow-missing plus optional checksum-bearing positives in `request_checksums.rs` | No remaining focused integration gap for implemented behavior |
| `CompleteMultipartUpload` | No request checksum requirement | No request checksum enforcement | Broad positive-path multipart coverage in `checksums.rs` and `multipart.rs`; raw allow-missing guard in `request_checksums.rs` | No remaining focused integration gap for allow-missing behavior |

## Test Requirements

For each implemented operation in the required matrix, the final `s3-tests`
coverage should include:

1. Missing checksum negative
- send a raw signed request without `Content-MD5`
- without `x-amz-checksum-algorithm`
- and without any `x-amz-checksum-*` header or trailer
- assert AWS-matching failure

2. `Content-MD5` positive
- send a raw signed request with valid `Content-MD5`
- assert success

3. SDK checksum-family positive
- send a raw signed request with:
  `x-amz-checksum-algorithm`
- and a matching `x-amz-checksum-*` header
- assert success

`DeleteObjects` needs a different matrix:
- missing checksum must fail
- valid `Content-MD5` must succeed
- SDK checksum family must also succeed, matching AWS

The exception matrix needs explicit guard tests:

1. `PutObject`
- missing checksum succeeds in the normal case
- missing checksum fails when Object Lock retention is requested
- `Content-MD5` succeeds for Object Lock upload
- SDK checksum family succeeds for Object Lock upload

2. `UploadPart`
- missing checksum succeeds
- optional `Content-MD5` still succeeds
- optional checksum family still succeeds

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
- `x-amz-checksum-algorithm` plus matching `x-amz-checksum-*`
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

Status:
- completed via shared raw request helpers in `crates/s3-tests/src/helpers.rs`

### Phase 2: Missing-Coverage Tests

Add focused tests for every implemented required operation.

Deliverables:
- one missing-checksum negative per required operation
- one raw `Content-MD5` positive per required operation
- one raw SDK-checksum-family positive per required operation, except
  `DeleteObjects` until AWS behavior is confirmed

Status:
- completed for the currently implemented checksum-required operations,
  including `DeleteObjects` checksum-family substitution and the raw
  Object Lock `PutObject` checksum-bearing boundary coverage

### Phase 3: Exception Tests

Add explicit allow-missing and conditional-requirement tests for:
- `PutObject`
- `UploadPart`
- `CompleteMultipartUpload`

Deliverables:
- guard tests that prevent broad shared enforcement from becoming stricter than
  AWS

Status:
- completed for the current exception set: `PutObject`, `UploadPart`, and
  `CompleteMultipartUpload`

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
  - bare `x-amz-checksum-algorithm`
  - checksum algorithm/value mismatch
  - bad digest vs invalid digest

Status:
- completed for the currently implemented checksum requirement table

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
