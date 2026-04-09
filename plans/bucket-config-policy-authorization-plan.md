# Bucket Config Policy Authorization Plan

## Problem

Several bucket-level configuration APIs in `Coordinator` still authorize only
the bucket admin path via `authorize_bucket_admin_requester`.

That is not fully AWS-compatible for operations where AWS allows access via the
corresponding bucket-policy action, for example `PutBucketCors` with
`s3:PutBucketCORS`.

We do not implement IAM policy evaluation yet, but we do implement bucket
policies. So the compatibility target for these APIs should be:

- bucket admin is allowed
- non-owner access is allowed when the bucket policy explicitly allows the
  corresponding bucket action
- explicit bucket-policy deny still wins

## Goals

1. Add the missing bucket-level policy actions to `auth::PolicyAction`.
2. Introduce a shared authz helper for "bucket admin or bucket-policy action X".
3. Switch the affected bucket configuration APIs to that helper.
4. Add local unit tests and AWS-backed `s3-tests` coverage.

## Status

Completed:

- Added bucket-level `PolicyAction` support and bucket-resource validation for:
  - `GetBucketCORS` / `PutBucketCORS`
  - `GetBucketTagging` / `PutBucketTagging`
  - `GetLifecycleConfiguration` / `PutLifecycleConfiguration`
  - `GetBucketOwnershipControls` / `PutBucketOwnershipControls`
  - `GetEncryptionConfiguration` / `PutEncryptionConfiguration`
- Added `authorize_bucket_admin_or_bucket_policy_action(...)` in
  `crates/server-core/src/coordinator/authz.rs`.
- Switched these coordinator APIs to the shared helper:
  - bucket CORS
  - bucket tagging
  - bucket lifecycle
  - bucket ownership controls
  - bucket encryption
- Added coordinator unit coverage for allow/deny cases on those API families.
- Added AWS-backed `s3-tests` coverage for:
  - CORS cross-account allow
  - bucket tagging cross-account allow
  - lifecycle cross-account allow
  - ownership controls cross-account allow
  - bucket encryption cross-account allow
- Hardened the AWS-backed tests for propagation and stale-success after delete.

Validated against AWS:

- CORS
- bucket tagging
- lifecycle
- ownership controls
- bucket encryption

Still open:

- verify the remaining bucket-admin-only APIs against AWS docs and behavior
- decide whether any additional bucket config APIs should move to the helper
- keep versioning unchanged unless AWS behavior is confirmed to allow
  bucket-policy delegation

## Scope

Confirmed or likely affected APIs include:

- `GetBucketCORS` / `PutBucketCORS` / `DeleteBucketCORS`
- `GetBucketTagging` / `PutBucketTagging` / `DeleteBucketTagging`
- `GetLifecycleConfiguration` / `PutLifecycleConfiguration` / `DeleteBucketLifecycle`
- `GetBucketOwnershipControls` / `PutBucketOwnershipControls` / `DeleteBucketOwnershipControls`
- `GetEncryptionConfiguration` / `PutEncryptionConfiguration`

Still to verify before changing:

- `GetBucketVersioning` / `PutBucketVersioning`
- any remaining bucket-admin-only configuration APIs such as
  `PutBucketPublicAccessBlock` / `DeleteBucketPublicAccessBlock`
  and `PutBucketObjectLockConfiguration`

Each API should be verified against AWS documentation before changing it, so we
only widen authorization where AWS actually allows the operation via bucket
policy.

## Steps

1. Inventory AWS permission requirements for the affected bucket APIs.
2. Add the missing bucket-level actions in
   `crates/auth/src/bucket_policy.rs`.
3. Add a shared helper in `crates/server-core/src/coordinator/authz.rs` for:
   - bucket admin access
   - bucket-policy allow for a specific action
   - explicit deny handling
   - `RestrictPublicBuckets` interaction
   - `expected_bucket_owner` enforcement
4. Convert the bucket config APIs that should use the helper.
5. Add coordinator unit tests for:
   - explicit cross-account allow
   - missing allow denied
   - explicit deny wins
   - `expected_bucket_owner` still enforced
6. Add AWS-backed `s3-tests` for representative bucket config APIs.
7. Update compatibility docs only if any gaps remain after the code changes.

Current remaining steps:

1. Verify the remaining bucket-admin-only APIs against AWS docs and real AWS.
2. Extend `PolicyAction` and the shared helper only where AWS actually delegates
   access via bucket policy.
3. Add the matching local and AWS-backed tests for any newly widened APIs.
4. Run broader verification before commit.

## Recommended Order

1. Confirm AWS action names and semantics per API.
2. Add `PolicyAction` variants.
3. Implement the shared authz helper.
4. Convert one vertical slice first, starting with bucket CORS.
5. Add unit and AWS tests for that slice.
6. Convert the rest of the bucket config APIs.
7. Run broader verification.

## Verification

At minimum:

- `cargo fmt`
- `cargo test -p auth`
- targeted `cargo test -p server-core ...` for the new authz tests
- targeted AWS-backed `cargo test -p s3-tests ...` for new bucket config tests
- `cargo clippy --all-targets --all-features -- -D warnings`

Before commit, run the full test suite if feasible.
