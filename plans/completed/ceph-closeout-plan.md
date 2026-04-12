# Ceph Test Closeout Plan

## Scope

This plan covers the remaining Ceph `s3-tests` parity work for behavior that is
already implemented or intended to be implemented in the near term.

It is a closeout plan, not a new feature plan.

In scope:
- remaining small Ceph-derived test gaps for already-implemented S3 behavior
- one remaining bucket-policy compatibility gap on the implemented surface
- retiring stale historical gap estimates

Out of scope:
- bucket logging
- `SSE-KMS`
- the remaining encryption roadmap already tracked in
  `plans/encryption-compat-plan.md`
- the bucket-policy owner-root carveout tracked in
  `plans/completed/bucket-policy-root-principal-compat-plan.md`
- any Ceph-only RGW extensions or non-AWS namespace behavior

## Current Assessment

The old aggregate gap estimates are no longer accurate.

After checking the current tree against the historical porting notes and the
Ceph tests:
- raw HTTP / header / signed-request coverage is no longer a major gap
- bucket listing is effectively closed out
- multipart "edge case" coverage is not missing in the way older estimates
  suggested
- `SSE-S3 default encryption` should not be tracked here as a generic Ceph gap;
  encryption follow-up is already split into `plans/encryption-compat-plan.md`

Evidence for that reassessment:
- the only active commented-out conformance test in `crates/s3-tests/tests` is
  the bucket-policy `IfExists` case in `crates/s3-tests/tests/bucket_policy.rs`
- the raw signed-request checksum matrix is already tracked as complete in
  `plans/completed/request-checksum-compat-plan.md`
- the old bootstrap counts in `plans/completed/s3-integration-tests.md` are
  explicitly marked stale

## Remaining Ceph Closeout Work

### 1. Bucket policy `IfExists`

Status: implemented on the currently supported condition-key subset.

Originally missing Ceph case:
- `test_bucket_policy_set_condition_operator_end_with_IfExists`

Current state:
- `auth` now accepts and evaluates `StringEqualsIfExists`,
  `StringLikeIfExists`, and `StringNotEqualsIfExists` for the currently
  supported string-condition subset
- the stale commented integration placeholder in
  `crates/s3-tests/tests/bucket_policy.rs` has been replaced with an active
  end-to-end test on a supported S3 condition key

Follow-up verification:
- keep the targeted AWS-backed verification for this case in the closeout log
  for the implementation commit

### 2. Bucket recreate / ACL parity ports

Status: implemented.

Completed Ceph cases:
- `test_bucket_recreate_overwrite_acl`
- `test_bucket_recreate_new_acl`
- `test_bucket_recreate_not_overriding`
- `test_bucket_concurrent_set_canned_acl`

Current state:
- `crates/s3-tests/tests/bucket_crud.rs` now covers same-owner recreate object
  preservation, recreate ACL overwrite, valid recreate with new canonical-user
  ACL grants, rejection of `CreateBucket` public ACL requests, and the
  region-specific `BucketAlreadyOwnedByYou` branch outside `us-east-1`
- `crates/s3-tests/tests/bucket_acl.rs` now covers concurrent canned ACL
  updates
- `server-core` now matches AWS CreateBucket semantics for same-owner existing
  buckets:
  - `us-east-1`: `200 OK`, preserve contents, allow ACL reset for otherwise
    valid requests
  - other regions: `409 BucketAlreadyOwnedByYou`
- AWS-backed verification now covers:
  - `us-east-1` recreate with explicit canonical-user grants
  - `us-east-1` recreate with `public-read` returning
    `InvalidBucketAclWithBlockPublicAccessError`
  - `us-west-2` same-owner CreateBucket
  - `us-west-2` recreate with the same explicit-grant request returning
    `BucketAlreadyOwnedByYou`
  - `us-west-2` recreate with `public-read` returning
    `InvalidBucketAclWithBlockPublicAccessError`
- the external `s3-tests` setup now creates its distinct-owner probe buckets
  with the configured region's `LocationConstraint`, so multi-region AWS runs
  work correctly

### 3. Versioning concurrent create/remove ports

Status: implemented.

Completed Ceph cases:
- `test_versioned_concurrent_object_create_concurrent_remove`
- `test_versioned_concurrent_object_create_and_remove`

Current state:
- both concurrency races are now ported in
  `crates/s3-tests/tests/versioning.rs`

## Not Remaining For This Plan

These should not stay on the closeout list:
- raw HTTP request tests as a generic category
- bucket listing as a generic category
- multipart edge cases as a generic category
- bucket-policy `NotPrincipal` rejection
- bucket-policy owner-root self-deny / root carveout behavior
- encryption rows that belong to `plans/encryption-compat-plan.md`

## AWS Verification Fallout

The AWS-backed closeout pass surfaced a few important corrections that were not
obvious from the earlier Ceph-porting work alone.

### 1. Concurrent bucket ACL updates are not a strict all-success contract

Ceph's concurrent canned-ACL case is useful coverage, but AWS may return
`409 OperationAborted` when multiple `PutBucketAcl` requests race on the same
bucket.

Implication:
- the test contract should accept either:
  - success
  - `409 OperationAborted`
- while still asserting that at least one request succeeds and that the final
  ACL state is correct
- the local server does not need to emulate a timing-dependent propagation or
  async internal-control-plane path just to force this exact transient
  response

### 2. BOE accepted canned-ACL subset was misread during the first ownership pass

The first pass over the Object Ownership documentation treated
`BucketOwnerEnforced` as allowing only:
- no ACL
- `bucket-owner-full-control`

Focused AWS verification showed that both:
- `private`
- `bucket-owner-read`

are also accepted for object write-style requests on BOE buckets in the cases
we exercise here.

Implication:
- BOE regression tests must not assert `AccessControlListNotSupported` for
  `private` or `bucket-owner-read`
- the implementation should match AWS's actual accepted canned-ACL subset, not
  the stricter interpretation from the initial doc read

### 3. The ownership test helper initially used invalid bucket-policy actions

The first cross-account ownership helper attempted to authorize multipart flows
with bucket-policy actions such as:
- `s3:CreateMultipartUpload`
- `s3:UploadPart`
- `s3:CompleteMultipartUpload`

AWS rejects those policy actions as invalid.

Implication:
- the test helper must use the real bucket-policy action surface and keep
  multipart-specific authorization expectations separate from object-ownership
  expectations
- AWS verification was necessary here because the invalid helper shape would
  have looked fine against a too-permissive local implementation

### 4. The local `bucket_acl` slowdown exposed a test-harness issue, not a
product need to serialize

The `bucket_acl` binary hanging under parallelism was initially tempting to
work around by serialization, but that would have hidden the signal.

What actually happened:
- an aggressive anonymous-GET retry/timeout helper made the failure easier to
  trigger but was not the right fix
- the local embedded test server was configured with a lower connection cap
  than the production default, which amplified connection pressure in this test
  binary

Implication:
- keep parallel execution; it is valuable for surfacing these issues
- prefer fixing harness/runtime mismatches and bad assumptions rather than
  weakening the test runner

### 5. Closeout work needs explicit AWS verification, not just Ceph parity

The net result of this batch is that the closeout process itself found
behavioral corrections outside the original Ceph gap list.

Conclusion:
- Ceph parity is no longer the only useful closeout signal
- focused AWS verification should remain part of any final closeout pass for:
  - ACLs
  - ownership controls
  - region-specific create/recreate behavior
  - concurrency-sensitive control-plane cases

## Exit Criteria

This closeout plan is complete when:
- the bucket recreate / ACL subset is either ported or explicitly closed as
  already covered
- we have a short written statement that Ceph closeout is done for implemented
  S3 behavior, excluding:
  - bucket logging
  - encryption areas still tracked separately
  - the root-principal bucket-policy carveout plan

Status: complete. Ceph closeout is done for implemented S3 behavior, excluding:
- bucket logging
- encryption areas still tracked separately
- the root-principal bucket-policy carveout plan
