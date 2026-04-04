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

## Ownership Behavior Matrix

Sources:
- AWS user guide: `https://docs.aws.amazon.com/AmazonS3/latest/userguide/about-object-ownership.html`
- AWS `CreateBucket` API: `https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateBucket.html`
- AWS `PutObject` API: `https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html`
- AWS `CopyObject` API: `https://docs.aws.amazon.com/AmazonS3/latest/API/API_CopyObject.html`
- AWS `PutBucketOwnershipControls` API:
  `https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketOwnershipControls.html`

### Documented Baseline

| Operation | BucketOwnerEnforced | BucketOwnerPreferred | ObjectWriter |
| --- | --- | --- | --- |
| `CreateBucket` | Default for new buckets. `CreateBucket` docs say any ACL on create requires choosing a non-default ownership mode first. | ACLs enabled. Bucket ACLs allowed on create. | ACLs enabled. Bucket ACLs allowed on create. |
| `PutBucketOwnershipControls` | Enables ACL-disabled mode. Existing bucket/object ACL permissions must first be migrated and bucket ACL reset to private. | Enables ACLs. | Enables ACLs. |
| `PutObject` | Docs say only no ACL or `bucket-owner-full-control` should be accepted. Other ACLs should fail with `AccessControlListNotSupported`. | All uploads accepted; bucket owner owns only if upload uses `bucket-owner-full-control`. | All uploads accepted; writer owns object. |
| `CopyObject` | Docs say only no ACL or `bucket-owner-full-control` should be accepted. Other ACLs should fail. Destination object owned by bucket owner. | ACLs enabled. Ownership follows `bucket-owner-full-control` rule. | ACLs enabled. Writer/caller owns destination object. |
| `CreateMultipartUpload` | User guide says uploads accepted are only no ACL or `bucket-owner-full-control`. | ACLs enabled. Ownership follows `bucket-owner-full-control` rule at completion. | ACLs enabled. Initiator/writer owns completed object. |
| `PutObjectAcl` | Requests to set/update ACLs fail. | Allowed. | Allowed. |
| `GetObjectAcl` | Supported, but ACL read should show bucket owner full control while BOE is active. | Allowed. | Allowed. |
| `PutBucketAcl` | Requests to set/update ACLs fail. | Allowed. | Allowed. |
| `GetBucketAcl` | Supported, but ACL read should show bucket owner full control while BOE is active. | Allowed. | Allowed. |

### AWS-Observed Adjustments We Already Implement

Focused AWS verification during the Ceph closeout found one important
implemented-surface exception to the documented BOE write-path contract:

| Operation | Docs say | AWS accepted in our verification |
| --- | --- | --- |
| `PutObject` | no ACL, `bucket-owner-full-control` | also accepted `private` and `bucket-owner-read` |
| `CopyObject` | no ACL, `bucket-owner-full-control` | also accepted `private` and `bucket-owner-read` |
| `CreateMultipartUpload` | no ACL, `bucket-owner-full-control` | also accepted `private` and `bucket-owner-read` |

The current implementation and tests already match this narrower
AWS-observed contract.

### Current Test Coverage Against The Matrix

Covered now:
- default ownership is `BucketOwnerEnforced`
- create / get / delete ownership controls
- BOE create-time and update-time rejection when bucket ACL is still public
- BOE write-path acceptance for:
  - no ACL
  - `bucket-owner-full-control`
  - `private`
  - `bucket-owner-read`
- BOE write-path rejection for `public-read`
- BOE rejection of `PutBucketAcl`
- `BucketOwnerPreferred` and `ObjectWriter` cross-account ownership outcomes
- object ACL read/restore semantics across BOE transitions
- same-account non-owner existing and missing object discovery under BOE

Still worth pinning down explicitly:
- short written note that AWS docs still state the stricter BOE upload rule,
  while AWS behavior for the implemented general-purpose bucket surface accepts
  `private` and `bucket-owner-read`
- the focused AWS rerun for the ownership subset below
- a final note on whether any remaining doc wording should be treated as
  ambiguous rather than normative for the implemented surface

## Work Items

### 1. Write the ownership behavior matrix

Status: complete.

The matrix above now records:
- the documented contract
- the AWS-observed BOE exception on object write-style requests
- the concrete remaining regression targets

### 2. Fill any missing implemented-surface tests

Status: complete for the current local coverage pass.

Completed in this pass:
- BOE rejected canned ACL coverage for:
  - `public-read-write`
  - `authenticated-read`
  - `aws-exec-read`
  across:
  - `PutObject`
  - `CopyObject`
  - `CreateMultipartUpload`
- BOE rejected explicit ACL grant coverage across:
  - `PutObject`
  - `CopyObject`
  - `CreateMultipartUpload`
  - `PutObjectAcl`
- BOE `GetBucketAcl` response coverage for owner full-control rendering
- same-account bucket-owner-account `GetBucketAcl` regression coverage in
  `server-core`

Implementation fallout found and fixed:
- the `CopyObject` HTTP path was only parsing `x-amz-acl` and was silently
  ignoring `x-amz-grant-*`
- `CopyObjectRequest` now carries `PutObjectWriteAcl`, so explicit grants are
  enforced the same way as `PutObject` and multipart initiation
- `GetBucketAcl` now collapses to bucket-owner full control under BOE, and BOE
  bucket-ACL authorization ignores stored ACL grants

Constraint clarified during this pass:
- bucket ACL "restore semantics" are not a meaningful BOE transition case the
  way object ACL restore semantics are, because AWS requires the bucket ACL to
  be private before enabling `BucketOwnerEnforced`

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

Add from this pass:
- `test_bucket_owner_enforced_rejects_remaining_canned_object_acls`
- `test_bucket_owner_enforced_rejects_explicit_object_acl_grants`
- `test_bucket_owner_enforced_bucket_acl_read_and_restore_semantics`

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

Current known exception to record:
- AWS docs still describe BOE object uploads in the stricter
  no-ACL / `bucket-owner-full-control` form, but focused AWS verification on
  the implemented general-purpose bucket surface accepted `private` and
  `bucket-owner-read` for `PutObject`, `CopyObject`, and
  `CreateMultipartUpload`

## Exit Criteria

This plan is complete when:
- the ownership behavior matrix is written down
- the implemented ownership surface has explicit regression coverage for the
  documented and AWS-observed cases we support
- the focused AWS ownership subset passes
- any doc/behavior mismatches are documented rather than left implicit

At that point, Object Ownership compatibility should be considered closed for
the currently implemented surface.
