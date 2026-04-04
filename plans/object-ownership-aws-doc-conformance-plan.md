# Object Ownership AWS-Doc Conformance Plan

## Scope

This plan covers a focused conformance pass for S3 Object Ownership behavior
against AWS documentation and focused AWS verification.

Primary reference:
- `https://docs.aws.amazon.com/AmazonS3/latest/userguide/about-object-ownership.html`

In scope:
- `BucketOwnerEnforced`, `BucketOwnerPreferred`, and `ObjectWriter`
- when ACLs are accepted, rejected, or ignored
- how ownership mode affects object owner, ACL read/write behavior, and access
  outcomes
- region-independent Object Ownership semantics
- local and AWS-backed regression coverage for the implemented surface

Out of scope:
- bucket logging
- `SSE-KMS`
- broader account-model redesign beyond what is required for conformance
- bucket-policy root-principal carveout behavior

## Why This Plan Exists

The Ceph closeout work is complete, but the AWS-backed verification pass found
several Object Ownership details that were not obvious from the earlier Ceph
porting work alone:
- `BucketOwnerEnforced` accepts more canned ACL inputs than we first assumed
- same-account non-owner behavior under BOE needed explicit cloaking coverage
- ownership tests can accidentally encode invalid AWS policy assumptions unless
  verified directly

So the remaining task is no longer Ceph parity. It is a short AWS-doc and
AWS-behavior conformance pass for Object Ownership itself.

## Current Baseline

Already covered:
- default bucket ownership mode is `BucketOwnerEnforced`
- explicit bucket ownership control CRUD
- cross-account ownership behavior for `BucketOwnerPreferred` and
  `ObjectWriter`
- BOE rejection of unsupported object ACL updates
- BOE acceptance of the AWS-observed canned ACL subset used on object write
  paths
- same-account non-owner existing-object and missing-object discovery behavior
  under BOE

What still needs to be tightened:
- a concise matrix of documented vs observed ACL acceptance under each
  ownership mode and request type
- explicit regression coverage for "ACLs disabled" behavior beyond the cases
  we found while fixing fallout
- a written record of any AWS doc ambiguities or observed exceptions

## Work Items

### 1. Write the ownership behavior matrix

Create a small checked-in matrix, derived from the AWS doc and verified tests,
for:
- `CreateBucket`
- `PutBucketOwnershipControls`
- `PutObject`
- `CopyObject`
- `CreateMultipartUpload`
- `PutObjectAcl`
- `GetObjectAcl`
- `PutBucketAcl`
- `GetBucketAcl`

For each ownership mode, record:
- whether ACLs are enabled, disabled, or ignored
- which canned ACLs are accepted
- which ACL operations must fail
- whether ACL reads still return stored/restorable ACL state

This should live in the plan itself unless it grows large enough to justify a
guide.

### 2. Fill any missing implemented-surface tests

Use the matrix to identify gaps in:
- `crates/s3-tests/tests/ownership.rs`
- `crates/s3-tests/tests/object_crud.rs`
- `crates/s3-tests/tests/bucket_acl.rs`
- `crates/server-core/src/coordinator.rs` regression tests

Priority cases:
- BOE accepted canned ACL subset across all object write-style entry points
- BOE rejected canned ACL subset across the same entry points
- ACL read semantics under BOE after mode changes
- same-account non-owner vs cross-account behavior where ownership mode changes
  authorization or cloaking

### 3. Re-run a focused AWS ownership subset

Run a narrow AWS-backed ownership suite and keep the exact verified cases in
the commit log / plan notes.

Minimum AWS subset:
- `test_create_bucket_bucket_owner_enforced`
- `test_put_bucket_ownership_bucket_owner_enforced`
- `test_bucket_owner_enforced_acl_read_and_restore_semantics`
- `test_bucket_owner_preferred_cross_account_object_ownership_matrix`
- `test_object_writer_cross_account_object_ownership_matrix`

Add any new targeted tests from phase 2 to this subset.

### 4. Record any remaining intentional differences

If AWS behavior is:
- underdocumented
- inconsistent with the doc wording
- timing-dependent in a way we should not emulate exactly

then record that explicitly with:
- the AWS-observed behavior
- what we enforce locally
- why that is the correct compatibility contract

This should stay short and only cover real exceptions.

## Exit Criteria

This plan is complete when:
- the ownership behavior matrix is written down
- the implemented ownership surface has explicit regression coverage for the
  documented and AWS-observed cases we support
- the focused AWS ownership subset passes
- any doc/behavior mismatches are documented rather than left implicit

At that point, Object Ownership compatibility should be considered closed for
the currently implemented surface.
