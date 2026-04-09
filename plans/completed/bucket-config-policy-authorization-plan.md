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
  - `GetBucketPublicAccessBlock` / `PutBucketPublicAccessBlock`
  - `GetBucketObjectLockConfiguration` / `PutBucketObjectLockConfiguration`
- Added `authorize_bucket_admin_or_bucket_policy_action(...)` in
  `crates/server-core/src/coordinator/authz.rs`.
- Switched these coordinator APIs to the shared helper:
  - bucket CORS
  - bucket tagging
  - bucket lifecycle
  - bucket ownership controls
  - bucket encryption
  - bucket public access block write/delete
  - bucket object lock configuration write
- Added coordinator unit coverage for allow/deny cases on those API families.
- Added AWS-backed `s3-tests` coverage for:
  - CORS cross-account allow
  - bucket tagging cross-account allow
  - lifecycle cross-account allow
  - ownership controls cross-account allow
  - bucket encryption cross-account allow
  - bucket public access block cross-account allow
  - bucket object lock configuration cross-account allow
- Hardened the AWS-backed tests for propagation and stale-success after delete.

Validated against AWS:

- CORS
- bucket tagging
- lifecycle
- ownership controls
- bucket encryption
- bucket public access block
- bucket object lock configuration

Versioning note:

- `GetBucketVersioning` remains owner-only.
- AWS API docs explicitly say `GetBucketVersioning` requires the bucket owner.
- `PutBucketVersioning` is also being kept owner-only.
- That matches the current AWS compatibility judgment unless contrary behavior is
  found on real AWS later.

## Scope

Confirmed or likely affected APIs include:

- `GetBucketCORS` / `PutBucketCORS` / `DeleteBucketCORS`
- `GetBucketTagging` / `PutBucketTagging` / `DeleteBucketTagging`
- `GetLifecycleConfiguration` / `PutLifecycleConfiguration` / `DeleteBucketLifecycle`
- `GetBucketOwnershipControls` / `PutBucketOwnershipControls` / `DeleteBucketOwnershipControls`
- `GetEncryptionConfiguration` / `PutEncryptionConfiguration`
- `GetBucketPublicAccessBlock` / `PutBucketPublicAccessBlock` / `DeletePublicAccessBlock`
- `GetBucketObjectLockConfiguration` / `PutBucketObjectLockConfiguration`

Still to verify before changing:

- `GetBucketVersioning` / `PutBucketVersioning`
- any remaining bucket-admin-only configuration APIs beyond versioning

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

Current state:

1. This plan's implemented scope is complete.
2. Versioning remains intentionally owner-only.
3. Any future changes should come from a new follow-up plan if AWS evidence
   shows additional bucket-admin-only APIs should be widened.

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
