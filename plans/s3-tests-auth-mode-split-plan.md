# S3 Tests Auth Mode Split Plan

## Goal

Make the external `s3-tests` ownership mode explicit and easy to audit:

- BOE/default tests and ACL-mode tests should not be mixed in the same file when the mode is relevant.
- Public access tests should live in public-access-focused files instead of being scattered through feature files.
- Add targeted ACL-mode coverage for important feature surfaces that are currently covered mostly through the BOE default path.

This is a test-suite organization and coverage project. It should not change server behavior.

## Definitions

- **BOE/default test**: the operation under test runs against a bucket that remains `BucketOwnerEnforced`. In current AWS and local behavior, ordinary `CreateBucket` without `x-amz-object-ownership` is BOE.
- **ACL-mode test**: the operation under test depends on legacy ACL semantics or runs against a bucket switched to `ObjectWriter` or `BucketOwnerPreferred`.
- **Public access test**: the operation under test checks anonymous access, AllUsers or AuthenticatedUsers grants, public canned ACLs, public bucket policies, or PublicAccessBlock interaction with public access.
- **Transition test**: the operation under test changes ownership mode or checks behavior before/after BOE enablement. These should stay with ownership-focused tests unless the scenario is mainly a public-access case.

## Current Shape

The main helper `s3_tests::create_bucket` builds a normal create request and does not set an ownership override. The HTTP frontend defaults missing `x-amz-object-ownership` to `BucketOwnerEnforced`, so most ordinary tests are already BOE/default.

ACL-mode tests currently appear in several places:

- `access_matrix.rs`: bucket ACL x object ACL matrix; entirely legacy/public ACL oriented.
- `bucket_acl.rs`: bucket ACL surface; mostly ACL-mode, with one BOE/default rejection case.
- `bucket_anon.rs`: anonymous/private/public behavior; public ACL helper driven.
- `public_access_block.rs`: mixes PAB configuration tests, public ACL blocking, ignore-public-ACL behavior, and public policy restriction.
- Feature files with ACL-mode cases mixed in: `object_crud.rs`, `multipart.rs`, `copy_object.rs`, `versioning.rs`, `range.rs`, `post_object.rs`, `tagging.rs`, `request_checksums.rs`, `expected_bucket_owner.rs`, `bucket_crud.rs`, `bucket_policy.rs`, and `ownership.rs`.

## Target File Layout

Use suffixes for auth-mode-specific tests:

- Keep feature files such as `object_crud.rs`, `multipart.rs`, `sse_c.rs`, and `versioning.rs` for BOE/default behavior.
- Move legacy ACL-mode cases into `*_acl.rs` files:
  - `object_crud_acl.rs`
  - `multipart_acl.rs`
  - `copy_object_acl.rs`
  - `versioning_acl.rs`
  - `request_checksums_acl.rs`
  - `expected_bucket_owner_acl.rs`
  - `bucket_crud_acl.rs`
- Rename or replace ACL-dominant files:
  - `access_matrix.rs` -> `public_access_acl_matrix.rs`
  - `bucket_acl.rs` stays as `bucket_acl.rs`, but move BOE-only rejection coverage to ownership or BOE files if it grows.
- Keep ownership transition behavior in `ownership.rs`, or split later into:
  - `ownership_create.rs`
  - `ownership_transition.rs`
  - `ownership_cross_account_acl.rs`

Public access should be grouped by mechanism:

- `public_access_acl.rs`: anonymous access via public bucket/object ACLs, including public-read/public-read-write and AuthenticatedUsers ACL behavior.
- `public_access_acl_matrix.rs`: bucket ACL x object ACL matrix.
- `public_access_block.rs`: CRUD and canonical XML for the PublicAccessBlock subresource.
- `public_access_block_acl.rs`: BlockPublicAcls and IgnorePublicAcls behavior for bucket/object ACLs.
- `public_access_policy.rs`: public bucket policy behavior, BlockPublicPolicy, and RestrictPublicBuckets.

Avoid moving ABAC tests merely because they use a tag value such as `security=public`; those are bucket-policy condition tests, not public-access tests.

## Helper Cleanup

Add explicit test helpers in `crates/s3-tests/src/helpers.rs` before moving tests:

- `create_boe_bucket(client) -> String`
  - Creates a bucket through the default path and documents that BOE is expected.
  - Use this when a test is intentionally BOE/default rather than incidentally default.
- `create_bucket_with_ownership(client, ObjectOwnership) -> String`
  - Creates a bucket with an explicit ownership header.
- `create_acl_enabled_bucket(client, ObjectOwnership) -> String`
  - Creates or switches a bucket into `ObjectWriter` or `BucketOwnerPreferred` and disables bucket-level PublicAccessBlock when ACL grants need to be public.
- Keep `create_public_bucket` and `create_public_write_bucket`, but reimplement them through the new ACL helper so setup is consistent.

This should remove local duplicate helpers like `setup_acl_enabled_bucket` and `set_object_writer_ownership` over time.

## Coverage Additions

Add focused ACL-mode smoke coverage for surfaces currently dominated by BOE/default tests. These should be small, not a duplicate of every BOE case.

First priority:

- `sse_c_acl.rs`
  - `ObjectWriter` bucket with SSE-C single-part put/get/head.
  - `ObjectWriter` bucket with SSE-C range get.
  - `ObjectWriter` bucket with SSE-C multipart upload and complete.
  - Cross-account object owner case if AWS behavior is clear and stable enough.
- `bucket_encryption_acl.rs`
  - `ObjectWriter` bucket where bucket encryption blocks/allows SSE-C as expected.
- `object_lock_acl.rs`
  - Public-write/object-writer setup cases that already exist should move here or into `public_access_*` depending on intent.
- `lifecycle_acl.rs`
  - Minimal lifecycle put/get/delete on an ACL-enabled bucket.
- `checksums_acl.rs`
  - At least one object checksum write/read path and one multipart checksum path under ACL mode.

Second priority:

- `conditional_acl.rs`: conditional put/get/head behavior on `ObjectWriter`.
- `headers_acl.rs`: raw signed request/header shape for ACL-mode buckets where authz path matters.
- `website_redirect_acl.rs`: metadata persistence on ACL-enabled bucket.

Do not add broad matrix duplication until these smoke tests have found no gaps.

## Migration Order

1. Add shared helper functions and update a small number of existing tests to use them.
2. Move public access tests out of unrelated feature files into `public_access_*` files. Run `cargo test -p s3-tests -- --list` before and after and verify the total count is unchanged.
3. Move ACL-mode cases out of mixed feature files into `*_acl.rs`. Again verify test count and names.
4. Add `sse_c_acl.rs` and the first priority ACL-mode smoke tests.
5. Add the remaining first-priority ACL smoke tests.
6. Re-run local `cargo test -p s3-tests`, then AWS `./scripts/aws-tests` before committing.

## Progress

- Added explicit ownership helpers and routed public ACL setup through `create_acl_enabled_bucket`.
- Added the initial `sse_c_acl.rs` smoke coverage early because it was isolated and did not depend on the file-move sequence.
- Split `public_access_block_acl.rs` out of `public_access_block.rs` for BlockPublicAcls and IgnorePublicAcls behavior over bucket/object ACLs.
- Split `public_access_policy.rs` out of `public_access_block.rs` for BlockPublicPolicy, RestrictPublicBuckets, and policy-denied PublicAccessBlock behavior.
- Renamed `access_matrix.rs` to `public_access_acl_matrix.rs` because the file is entirely bucket ACL x object ACL public-access behavior.
- Split public ACL anonymous access cases out of `bucket_anon.rs` into `public_access_acl.rs`; `bucket_anon.rs` now keeps private/default and nonexistent anonymous bucket behavior.
- Moved anonymous public ACL object-tagging checks from `tagging.rs` into `public_access_acl.rs`; bucket-policy tagging checks remain in `tagging.rs`.
- Moved explicit public-write ACL multipart access checks from `multipart.rs` into `public_access_acl.rs`; cross-account multipart ownership/admin ACL cases remain in `multipart.rs` pending a later `multipart_acl.rs` split.
- Split remaining multipart ACL ownership/admin cases into `multipart_acl.rs`, and moved the public-read multipart ACL anonymous GET case into `public_access_acl.rs`.
- Split CopyObject ACL coverage by mechanism: cross-account ObjectWriter ACL copy behavior moved to `copy_object_acl.rs`, public-read CopyObject ACL behavior moved to `public_access_acl.rs`, and BlockPublicAcls CopyObject rejection moved to `public_access_block_acl.rs`.
- Split bucket/object ACL checksum subresource tests from `request_checksums.rs` into `request_checksums_acl.rs`.
- Split object ACL CRUD, canned ACL, explicit grant, and cross-account object ACL matrix coverage from `object_crud.rs` into `object_crud_acl.rs`.
- Moved public object ACL create/PutObjectAcl/header-grant cases from `object_crud_acl.rs` into `public_access_acl.rs`.

## Verification Checklist

- `cargo fmt`
- `cargo test -p s3-tests -- --list`
- `cargo test -p s3-tests`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `./scripts/aws-tests`

For pure file moves, compare test inventory before/after so coverage is preserved:

```bash
cargo test -p s3-tests -- --list 2>&1 | rg ': test$' | sort > /tmp/s3-tests.before
cargo test -p s3-tests -- --list 2>&1 | rg ': test$' | sort > /tmp/s3-tests.after
diff -u /tmp/s3-tests.before /tmp/s3-tests.after
```
