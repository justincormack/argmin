# Ceph Authz Diff Review Plan

## Goal

Track an operation-by-operation authz review of `crates/server-core/src/coordinator/authz.rs`
against Ceph RGW S3 compatibility code, starting with `PutObject`.

This is a review and test-planning document, not an implementation design.

Ignore SSE-KMS-specific differences for now. KMS object encryption is still
unsupported here.

## Current `PutObject` Summary

- Ceph `PutObject` authorization feeds `s3:RequestObjectTag/*` into policy
  evaluation but appears to check only `s3:PutObject`.
- Our `PutObject` path checks `s3:PutObject`, and when inline tags are present
  it also requires `s3:PutObjectTagging`.
- We have local `server-core` coverage for that dual-permission rule, but we do
  not yet have a clear AWS-backed `crates/s3-tests` case for the negative path.
- Ceph always injects `s3:x-amz-acl` into its IAM environment, even when the
  header is absent. Our policy engine treats an absent header as absent.
- That makes `Null` conditions on `s3:x-amz-acl` a likely compatibility gap and
  a clear missing external test.
- Ceph also exposes `s3:ResourceTag/*` during `PutObject`; our current
  evaluable object-action condition set does not.
- A separate Ceph/argmin difference around overwriting existing objects in
  public-write buckets is already covered by our tests and currently matches AWS
  on our side.

Current external coverage already exists for:

- `s3:x-amz-copy-source`
- `s3:x-amz-acl`
- `s3:x-amz-grant-full-control`
- `s3:RequestObjectTag/*`
- SSE-S3 and SSE-C `PutObject` conditions

The clearest missing external `PutObject` coverage is:

- inline-tagged `PutObject` denial when `s3:PutObjectTagging` is missing
- `Null` behavior for an absent `s3:x-amz-acl` header
- remaining grant-header condition keys
- `PutObject`-specific `RestrictPublicBuckets`

## Recommended `PutObject` Tests

Priority order:

1. Add an AWS-backed `PutObject` test where bucket policy allows only
   `s3:PutObject`, the request includes inline tags, and the request is denied
   until `s3:PutObjectTagging` is also allowed.
2. Add AWS-backed `PutObject` tests for `Null` / `StringNotEquals` conditions on
   `s3:x-amz-acl`, with the header absent and present.
3. Add AWS-backed `PutObject` tests for the remaining grant-header condition
   keys:
   - `s3:x-amz-grant-read`
   - `s3:x-amz-grant-write`
   - `s3:x-amz-grant-read-acp`
   - `s3:x-amz-grant-write-acp`
4. Add a `PutObject`-specific `RestrictPublicBuckets` test. Current external
   coverage is centered on `GetObject`.
5. Investigate `s3:ResourceTag/*` for `PutObject` after the higher-confidence
   gaps above are covered.

## Cross-Cutting Note For Remaining Object Operations

- Ceph threads `s3:ResourceTag/*` through most object-operation policy checks
  by loading object or bucket tags into the IAM environment.
- Our bucket-policy evaluator still does not support `s3:ResourceTag/*` for
  object actions.
- This affects `GetObject`, `HeadObject`, `GetObjectAttributes`,
  `DeleteObject`, `CopyObject`, object ACL/tagging operations, and object lock
  operations.
- I have not found AWS-backed `crates/s3-tests` coverage for this yet, so this
  should stay as one later investigation instead of repeating it under every
  operation.

## Reviewed Remaining Object Operations

Skipping `GetObject` `partNumber` and `Range` variants for now. They should
share the same main auth path as standard `GetObject`.

### `GetObject`, `HeadObject`, `GetObjectAttributes`

- `GetObject` and `HeadObject` match Ceph at the main auth-action level:
  `s3:GetObject` or `s3:GetObjectVersion`.
- `GetObjectAttributes` also matches Ceph: both implementations require the
  normal read action and the attributes-specific action.
- External coverage is already good here through `object_crud`,
  `access_matrix`, `public_access_block`, `ownership`, and
  `object_attributes_auth`.
- No new operation-specific gap stood out beyond the cross-cutting
  `s3:ResourceTag/*` follow-up.

### `DeleteObject`, `DeleteObjects`

- The main action mapping matches Ceph: `s3:DeleteObject` or
  `s3:DeleteObjectVersion`, and `DeleteObjects` is checked per entry.
- External coverage is already good in `object_crud`, `object_delete`,
  `versioning`, and `object_lock`, including governance-retention version
  deletes and mixed-result multi-delete behavior.
- One lower-confidence follow-up remains: Ceph checks
  `s3:BypassGovernanceRetention` whenever the bypass header is present on an
  object-lock bucket, while our delete path checks it only when deleting a live
  specific version. I have not found an AWS-backed test for bypass-header
  deletes against missing versions or non-version deletes.

### `CopyObject`

- Source-side auth aligns with Ceph on `GetObject` or `GetObjectVersion`.
- Destination-side auth is intentionally stricter in our code because it reuses
  `PutObject` checks. That means we enforce destination ACL and grant condition
  keys, destination SSE condition keys, and `s3:PutObjectTagging` when
  `x-amz-tagging-directive=REPLACE` carries inline tags.
- Ceph's copy verify path clearly covers `s3:x-amz-copy-source` and
  `s3:x-amz-metadata-directive`, but it does not obviously thread destination
  ACL and grant headers or request tags through auth.
- Existing external coverage already catches `grant-full-control`,
  `copy-source`, `metadata-directive`, and destination SSE conditions.
- Missing copy-specific external tests:
  - `CopyObject` with `REPLACE` tags should deny until
    `s3:PutObjectTagging` is allowed.
  - `CopyObject` destination canned ACL and remaining grant-header condition
    keys.

### `GetObjectAcl`, `PutObjectAcl`

- Action mapping and version-specific action selection line up with Ceph.
- `GetObjectAcl` coverage is already reasonable, including BOE behavior and an
  AWS-backed `ExistingObjectTag` bucket-policy case.
- `PutObjectAcl` coverage is also reasonable for bucket-policy allow, version
  actions, deny-on-public canned ACL, and `grant-read` conditions.
- The same absent `s3:x-amz-acl` question that showed up for `PutObject` also
  applies here: Ceph always inserts `s3:x-amz-acl` into its IAM environment,
  while our `PutObjectAcl` policy context leaves it absent when the request
  does not use a canned ACL.
- I have not found AWS-backed `PutObjectAcl` coverage for `Null` or
  `StringNotEquals` on absent `s3:x-amz-acl`.
- I also have not found AWS-backed `PutObjectAcl` coverage for the remaining
  ACL grant condition keys.

### `GetObjectTagging`, `PutObjectTagging`, `DeleteObjectTagging`

- The main auth-action mapping matches Ceph, including version-specific
  actions.
- Our policy handling already covers the same important condition families Ceph
  uses here: `ExistingObjectTag/*` for reads and deletes, and
  `RequestObjectTag/*` for writes.
- External coverage is already strong in `tagging.rs`, `deep_coverage.rs`, and
  BOE tests: basic CRUD, delete-marker `405`s, nonexistent-key behavior,
  `ExistingObjectTag` and `RequestObjectTag` bucket-policy cases, and
  cross-account behavior.
- No new operation-specific gap stood out beyond the cross-cutting
  `s3:ResourceTag/*` follow-up.

### `GetObjectRetention`, `PutObjectRetention`, `GetObjectLegalHold`, `PutObjectLegalHold`

- The main auth-action mapping matches Ceph.
- Our retention update path and tests already cover the important
  governance-bypass behavior, and external `object_lock.rs` coverage is good
  for allow and deny, bypass retention, delete-version interaction, and lock
  configuration leak prevention.
- No new operation-specific gap stood out here beyond the cross-cutting
  `s3:ResourceTag/*` follow-up.

## Next Tests After `PutObject`

- Add a `CopyObject` test where `x-amz-tagging-directive=REPLACE` plus inline
  tags requires `s3:PutObjectTagging`.
- Add `PutObjectAcl` tests for `Null` and `StringNotEquals` on absent
  `s3:x-amz-acl`.
- Add `CopyObject` destination ACL condition tests for canned ACL plus the
  remaining grant-header keys.
- Decide AWS behavior for delete requests that send
  `x-amz-bypass-governance-retention` against missing versions or non-version
  deletes, then add a focused test if needed.

## Reviewed Multipart Operations

### `CreateMultipartUpload`

- The main action mapping matches Ceph: destination authorization is anchored
  on `s3:PutObject`.
- Like our `PutObject` path, our initiate-multipart path also enforces
  `s3:PutObjectTagging` when inline tags are present and threads canned ACL and
  grant headers into bucket-policy evaluation.
- Ceph's initiate-multipart verify path appears narrower. It checks
  `s3:PutObject` and request encryption headers, but it does not obviously
  authorize inline tags or ACL and grant headers before the upload is created.
- Existing external coverage already covers object-resource scoping,
  request-object-tag conditions, anonymous denial on public-write buckets, and
  BOE ACL rejection.
- Missing external coverage:
  - negative `CreateMultipartUpload` with inline tags when only
    `s3:PutObject` is allowed
  - bucket-policy condition coverage for `s3:x-amz-acl`
  - bucket-policy condition coverage for the ACL grant headers

### `UploadPart`, `UploadPartCopy`, `CompleteMultipartUpload`

- The main action mapping aligns with Ceph:
  - destination writes use `s3:PutObject`
  - `UploadPartCopy` also checks source `s3:GetObject` or
    `s3:GetObjectVersion`
- Existing external coverage is decent here:
  - `multipart.rs` covers the main lifecycle
  - `bucket_policy.rs` covers `UploadPartCopy` source `copy-source` auth
  - `bucket_policy.rs` also covers destination SSE policy handling across
    create, upload-part-copy, and complete
- No high-confidence multipart-specific auth mismatch stood out here beyond the
  `CreateMultipartUpload` follow-ups above.

### `AbortMultipartUpload`

- Ceph checks the dedicated IAM action `s3:AbortMultipartUpload`.
- Our auth path does not evaluate bucket policy for aborts at all. It only
  allows the bucket owner account admin, the upload owner, or the initiator.
- Our bucket-policy model does not currently represent
  `s3:AbortMultipartUpload`, so bucket policy cannot allow or deny this
  operation.
- This is the clearest multipart auth gap.
- I did not find AWS-backed `crates/s3-tests` coverage for bucket-policy allow
  or deny on `AbortMultipartUpload`.
- Current local coordinator tests encode the ownership and initiator behavior,
  not the missing bucket-policy action.

### `ListParts`

- Ceph checks the dedicated IAM action `s3:ListMultipartUploadParts`.
- Our auth path does not evaluate bucket policy for `ListParts`. It uses the
  same ownership and initiator gate as abort.
- Our bucket-policy model does not currently represent
  `s3:ListMultipartUploadParts`, so bucket policy cannot allow or deny this
  operation.
- This is the other major multipart auth gap.
- Existing external coverage covers basic `ListParts` behavior and BOE
  owner/root behavior, but I did not find AWS-backed bucket-policy coverage for
  `s3:ListMultipartUploadParts`.
- Current local coordinator tests encode the ownership-only behavior here too.

## Recommended Multipart Tests

Priority order:

1. Add an AWS-backed `AbortMultipartUpload` test where a cross-account caller
   can create an MPU with `s3:PutObject` but cannot abort it until
   `s3:AbortMultipartUpload` is also allowed.
2. Add an AWS-backed `ListParts` test where a cross-account caller can create
   and upload parts with `s3:PutObject` but cannot list them until
   `s3:ListMultipartUploadParts` is also allowed.
3. Add an AWS-backed `CreateMultipartUpload` test where inline tags are present
   and `s3:PutObjectTagging` is required in addition to `s3:PutObject`.
4. Add `CreateMultipartUpload` bucket-policy condition tests for
   `s3:x-amz-acl` and the remaining ACL grant headers.
5. After the high-confidence gaps above, decide whether we need a focused AWS
   test for cross-principal `UploadPart` or `CompleteMultipartUpload` on an MPU
   created by another principal.

## Reviewed Bucket Operations

### Cross-Cutting Bucket Note

- The config-style bucket operations mostly line up with Ceph because our
  bucket-policy model already supports the same action families and our auth
  code actually uses them.
- The larger bucket-side gaps are the operations where Ceph checks a dedicated
  IAM action but our auth path still falls back to bucket ACL or public-read,
  or to bucket-owner-account-admin only.
- Those operations are `HeadBucket`, `GetBucketLocation`, `GetBucketAcl`,
  `PutBucketAcl`, `GetBucketVersioning`, `PutBucketVersioning`,
  `ListObjectVersions`, `ListMultipartUploads`, and likely `DeleteBucket`.
- `CreateBucket` and `ListBuckets` are a different class: Ceph checks
  account-level IAM actions, while our auth currently just requires an
  authenticated account plus local validation.

### `CreateBucket`, `ListBuckets`

- Ceph checks user-scoped IAM `s3:CreateBucket` and `s3:ListAllMyBuckets`,
  plus `s3:PutBucketOwnershipControls` when create uses
  `x-amz-object-ownership`.
- Our auth path requires an authenticated account and performs namespace, ACL,
  ownership, and region validation, but it does not evaluate those IAM
  actions.
- This looks like a broader account-level IAM gap rather than a bucket-policy
  gap, so I would not make these the next `s3-tests` targets unless we are
  ready to add identity-policy coverage.

### `HeadBucket`, `GetBucketLocation`

- Ceph checks `s3:ListBucket` for `HeadBucket` and `s3:GetBucketLocation` for
  `GetBucketLocation`.
- Our `HeadBucket` auth uses `authorize_bucket_read_for`, which means bucket
  ACL or public-read only. `GetBucketLocation` is routed through that same
  head-bucket auth path in `server-http`.
- `PolicyAction::ListBucket` already exists and is used for `ListObjectsV2`,
  so `HeadBucket` is a clear auth mismatch. `GetBucketLocation` also looks
  likely to differ because we do not model a separate `GetBucketLocation`
  action at all.
- Existing external coverage covers basic behavior and ACL or public-read
  cases, but I have not found AWS-backed bucket-policy allow or deny tests for
  either operation.

### `DeleteBucket`

- Ceph checks `s3:DeleteBucket`.
- Our auth path only allows bucket owner account admin.
- Our bucket-policy model does not currently represent `s3:DeleteBucket`.
- Existing external coverage covers basic empty-bucket behavior and same-account
  root or non-root admin behavior, not bucket-policy allow or deny.
- I would treat this as a lower-confidence follow-up until we pin AWS behavior
  with a focused test.

### `PutBucketCors`, `GetBucketCors`, `DeleteBucketCors`, `PutBucketTagging`, `GetBucketTagging`, `DeleteBucketTagging`, `PutBucketPolicy`, `GetBucketPolicy`, `DeleteBucketPolicy`, `PutPublicAccessBlock`, `GetPublicAccessBlock`, `DeletePublicAccessBlock`, `PutBucketOwnershipControls`, `GetBucketOwnershipControls`, `DeleteBucketOwnershipControls`, `PutBucketLifecycleConfiguration`, `GetBucketLifecycleConfiguration`, `DeleteBucketLifecycle`, `PutBucketEncryption`, `GetBucketEncryption`, `DeleteBucketEncryption`, `PutBucketObjectLockConfiguration`, `GetBucketObjectLockConfiguration`, `GetBucketPolicyStatus`

- These families largely line up with Ceph at the main action level.
- Our bucket-policy model already includes the same action families, and the
  auth code actually uses them.
- External coverage is already strong in `bucket_policy.rs`,
  `bucket_policy_root.rs`, `public_access_block.rs`, `object_lock.rs`,
  `bucket_admin_root.rs`, and `ownership.rs`.
- No new high-confidence operation-specific gap stood out in this group.

### `GetBucketVersioning`, `PutBucketVersioning`

- Ceph checks `s3:GetBucketVersioning` and `s3:PutBucketVersioning`.
- Our auth requires bucket owner account admin for both operations and never
  evaluates bucket policy.
- Our bucket-policy model does not currently represent either action.
- Existing external coverage in `versioning.rs` and `bucket_admin_root.rs`
  covers state transitions and same-account admin behavior, but I did not find
  AWS-backed bucket-policy allow or deny coverage.

### `ListObjectsV2`

- This path already matches Ceph well: both sides authorize with
  `s3:ListBucket`, and our bucket-policy implementation includes the relevant
  `prefix`, `delimiter`, and `max-keys` condition handling.
- External coverage is already strong in `bucket_policy.rs`, `bucket_crud.rs`,
  `deep_coverage.rs`, and related list tests.
- No new operation-specific gap stood out here.

### `ListObjectVersions`

- Ceph uses `s3:ListBucketVersions`.
- Our auth uses `authorize_bucket_read_for`, so it falls back to bucket ACL or
  public-read instead of a dedicated policy action.
- Our bucket-policy model does not currently represent
  `s3:ListBucketVersions`.
- Existing external coverage in `versioning.rs` is strong for functional
  behavior, but I did not find AWS-backed auth coverage for
  `s3:ListBucketVersions`.

### `ListMultipartUploads`

- Ceph uses `s3:ListBucketMultipartUploads`.
- Our auth also uses `authorize_bucket_read_for`, so bucket policy cannot allow
  or deny this operation today.
- Our bucket-policy model does not currently represent
  `s3:ListBucketMultipartUploads`.
- Existing external coverage in `multipart.rs` and `deep_coverage.rs` is
  functional, not auth-focused.

### `GetBucketAcl`, `PutBucketAcl`

- Ceph checks `s3:GetBucketAcl` and `s3:PutBucketAcl`. On `PutBucketAcl`, it
  also threads `s3:x-amz-acl` and the grant headers into IAM evaluation.
- Our `GetBucketAcl` and `PutBucketAcl` paths only consult ACL grants and
  bucket-admin ownership rules. They never evaluate bucket policy.
- Our bucket-policy model does not currently represent either action.
- External coverage is already decent for ACL semantics, BOE, public-ACL
  blocking, and canonical-user grants, but I have not found AWS-backed
  bucket-policy allow or deny tests for either operation.
- If we later add `PutBucketAcl` bucket-policy support, we will also need
  condition coverage for `s3:x-amz-acl` and the grant headers.

## Recommended Bucket Tests

Priority order:

1. Add AWS-backed `GetBucketAcl` and `PutBucketAcl` tests showing that
   cross-account access stays denied until `s3:GetBucketAcl` or
   `s3:PutBucketAcl` is explicitly allowed by bucket policy.
2. Add AWS-backed `HeadBucket` tests showing whether `s3:ListBucket` alone is
   enough to allow or deny the request. After that, add a focused
   `GetBucketLocation` check to confirm whether AWS uses
   `s3:GetBucketLocation` separately from `HeadBucket`.
3. Add AWS-backed `GetBucketVersioning` and `PutBucketVersioning`
   bucket-policy tests.
4. Add an AWS-backed `ListObjectVersions` test for `s3:ListBucketVersions`.
5. Add an AWS-backed `ListMultipartUploads` test for
   `s3:ListBucketMultipartUploads`.
6. After the higher-confidence gaps above, decide whether we need a focused
   AWS test for `DeleteBucket` under bucket policy.
7. Defer `CreateBucket` and `ListBuckets` until we have a clear
   identity-policy test strategy, since those are account-level IAM actions
   rather than bucket-policy actions.

## Authz Operation Checklist

AWS-facing authz entrypoints currently present in
`crates/server-core/src/coordinator/authz.rs`:

### Bucket Operations

- [x] `CreateBucket` -> `authorize_create_bucket`
- [x] `HeadBucket` -> `authorize_head_bucket`
- [x] `DeleteBucket` -> `authorize_delete_bucket`
- [x] `PutBucketCors` -> `authorize_put_bucket_cors`
- [x] `GetBucketCors` -> `authorize_get_bucket_cors`
- [x] `DeleteBucketCors` -> `authorize_delete_bucket_cors`
- [x] `PutBucketTagging` -> `authorize_put_bucket_tagging`
- [x] `GetBucketTagging` -> `authorize_get_bucket_tagging`
- [x] `DeleteBucketTagging` -> `authorize_delete_bucket_tagging`
- [x] `PutBucketPolicy` -> `authorize_put_bucket_policy`
- [x] `GetBucketPolicy` -> `authorize_get_bucket_policy`
- [x] `DeleteBucketPolicy` -> `authorize_delete_bucket_policy`
- [x] `PutPublicAccessBlock` -> `authorize_put_bucket_public_access_block`
- [x] `GetPublicAccessBlock` -> `authorize_get_bucket_public_access_block`
- [x] `DeletePublicAccessBlock` -> `authorize_delete_bucket_public_access_block`
- [x] `PutBucketOwnershipControls` -> `authorize_put_bucket_ownership_controls`
- [x] `GetBucketOwnershipControls` -> `authorize_get_bucket_ownership_controls`
- [x] `DeleteBucketOwnershipControls` -> `authorize_delete_bucket_ownership_controls`
- [x] `PutBucketLifecycleConfiguration` -> `authorize_put_bucket_lifecycle`
- [x] `GetBucketLifecycleConfiguration` -> `authorize_get_bucket_lifecycle`
- [x] `DeleteBucketLifecycle` -> `authorize_delete_bucket_lifecycle`
- [x] `PutBucketEncryption` -> `authorize_put_bucket_encryption`
- [x] `GetBucketEncryption` -> `authorize_get_bucket_encryption`
- [x] `DeleteBucketEncryption` -> `authorize_delete_bucket_encryption`
- [x] `PutBucketVersioning` -> `authorize_put_bucket_versioning`
- [x] `GetBucketVersioning` -> `authorize_get_bucket_versioning`
- [x] `ListObjectsV2` -> `authorize_list_objects_v2`
- [x] `ListBuckets` -> `authorize_list_buckets`
- [x] `ListObjectVersions` -> `authorize_list_object_versions`
- [x] `ListMultipartUploads` -> `authorize_list_multipart_uploads`
- [x] `PutBucketObjectLockConfiguration` -> `authorize_put_bucket_object_lock_configuration`
- [x] `GetBucketObjectLockConfiguration` -> `authorize_get_bucket_object_lock_configuration`
- [x] `GetBucketPolicyStatus` -> `authorize_get_bucket_policy_status`
- [x] `GetBucketAcl` -> `authorize_get_bucket_acl`
- [x] `PutBucketAcl` -> `authorize_put_bucket_acl`

### Object Operations

- [x] `PutObject` -> `authorize_put_object_write`
- [x] `GetObject` -> `authorize_get_object`
- [x] `HeadObject` -> `authorize_head_object`
- [x] `GetObjectAttributes` -> `authorize_get_object_attributes`
- [x] `DeleteObject` -> `authorize_delete_object`
- [x] `DeleteObjects` -> `authorize_delete_objects_entry`
- [x] `CopyObject` -> `authorize_copy_object`
- [x] `GetObjectAcl` -> `authorize_get_object_acl`
- [x] `PutObjectAcl` -> `authorize_put_object_acl`
- [x] `GetObjectTagging` -> `authorize_get_object_tags`
- [x] `PutObjectTagging` -> `authorize_put_object_tags`
- [x] `DeleteObjectTagging` -> `authorize_delete_object_tags`
- [x] `GetObjectRetention` -> `authorize_get_object_retention`
- [x] `PutObjectRetention` -> `authorize_put_object_retention`
- [x] `GetObjectLegalHold` -> `authorize_get_object_legal_hold`
- [x] `PutObjectLegalHold` -> `authorize_put_object_legal_hold`

### Multipart Operations

- [x] `CreateMultipartUpload` -> `authorize_create_multipart_upload`
- [x] `UploadPart` -> `authorize_begin_stream_part`
- [x] `UploadPartCopy` -> `authorize_upload_part_copy`
- [x] `CompleteMultipartUpload` -> `authorize_complete_multipart_upload`
- [x] `AbortMultipartUpload` -> `authorize_abort_multipart_upload`
- [x] `ListParts` -> `authorize_list_parts`
