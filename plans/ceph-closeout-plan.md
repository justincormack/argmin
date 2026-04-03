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
  `plans/bucket-policy-root-principal-compat-plan.md`
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

This is the only confirmed remaining product gap on the currently implemented
surface.

Missing Ceph case:
- `test_bucket_policy_set_condition_operator_end_with_IfExists`

Current state:
- the Rust port exists as a commented-out test in
  `crates/s3-tests/tests/bucket_policy.rs`
- the comment explicitly states that `IfExists` condition operators are not yet
  implemented

Deliver:
- implement `StringLikeIfExists` semantics for the currently supported
  condition-key subset
- enable the commented Rust integration test
- run the targeted AWS-backed verification for that case

### 2. Bucket recreate / ACL parity ports

These look like closeout test gaps rather than large product gaps.

Remaining Ceph cases to account for:
- `test_bucket_recreate_overwrite_acl`
- `test_bucket_recreate_new_acl`
- `test_bucket_recreate_not_overriding`
- `test_bucket_concurrent_set_canned_acl`

Current state:
- general same-owner recreate and delete-then-recreate behavior is already
  covered in `crates/s3-tests/tests/bucket_crud.rs`
- bucket ACL compatibility itself is now broadly covered in
  `crates/s3-tests/tests/bucket_acl.rs`
- what remains is the narrow recreate/concurrency subset above

Deliver:
- port these cases directly where they still add distinct behavior coverage
- if one of them is already fully implied by existing tests, document that and
  close it explicitly instead of duplicating it silently

### 3. Versioning concurrent create/remove ports

These remain credible uncovered Ceph concurrency cases.

Remaining Ceph cases:
- `test_versioned_concurrent_object_create_concurrent_remove`
- `test_versioned_concurrent_object_create_and_remove`

Current state:
- versioning coverage is otherwise broad in `crates/s3-tests/tests/versioning.rs`
- there is already some concurrent versioning coverage, but not these exact
  create/remove races

Deliver:
- port both Ceph cases into `crates/s3-tests/tests/versioning.rs`
- keep them deterministic and compatible with both local and AWS runs

## Not Remaining For This Plan

These should not stay on the closeout list:
- raw HTTP request tests as a generic category
- bucket listing as a generic category
- multipart edge cases as a generic category
- bucket-policy `NotPrincipal` rejection
- bucket-policy owner-root self-deny / root carveout behavior
- encryption rows that belong to `plans/encryption-compat-plan.md`

## Recommended Order

1. Finish bucket-policy `IfExists`.
2. Port the two versioning concurrent create/remove tests.
3. Close the bucket recreate / ACL subset, either by porting or by explicitly
   documenting when an existing Rust test already covers the same behavior.

## Exit Criteria

This closeout plan is complete when:
- the `IfExists` bucket-policy case is implemented and tested
- the two remaining versioning concurrency cases are ported
- the bucket recreate / ACL subset is either ported or explicitly closed as
  already covered
- we have a short written statement that Ceph closeout is done for implemented
  S3 behavior, excluding:
  - bucket logging
  - encryption areas still tracked separately
  - the root-principal bucket-policy carveout plan
