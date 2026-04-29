# S3 Tests Auth Mode Split Plan

## Goal

Make the external `s3-tests` ownership mode explicit and easy to audit:

- BOE/default tests and ACL-mode tests should not be mixed in the same file when the mode is relevant.
- Public access tests should live in public-access-focused files instead of being scattered through feature files.
- Add targeted ACL-mode coverage for important feature surfaces that are currently covered mostly through the BOE default path.

This is primarily a test-suite organization and coverage project. The main
split work should not change server behavior; the final AWS verification did
uncover one S3 conformance fix for `PutObject` without `Content-Length`.

## Definitions

- **BOE/default test**: the operation under test runs against a bucket that remains `BucketOwnerEnforced`. In current AWS and local behavior, ordinary `CreateBucket` without `x-amz-object-ownership` is BOE.
- **ACL-mode test**: the operation under test depends on legacy ACL semantics or runs against a bucket switched to `ObjectWriter` or `BucketOwnerPreferred`.
- **Public access test**: the operation under test checks anonymous access, AllUsers or AuthenticatedUsers grants, public canned ACLs, public bucket policies, or PublicAccessBlock interaction with public access.
- **Transition test**: the operation under test changes ownership mode or checks behavior before/after BOE enablement. These should stay with ownership-focused tests unless the scenario is mainly a public-access case.

## Final Shape

The main helper `s3_tests::create_bucket` builds a normal create request and does not set an ownership override. The HTTP frontend defaults missing `x-amz-object-ownership` to `BucketOwnerEnforced`, so most ordinary tests are already BOE/default.

ACL-mode tests now have explicit homes:

- `bucket_acl.rs`: bucket ACL surface and bucket ACL setup/recreate behavior.
- `object_crud_acl.rs`, `multipart_acl.rs`, `copy_object_acl.rs`, `versioning_acl.rs`, `request_checksums_acl.rs`, and `expected_bucket_owner_acl.rs`: legacy object/subresource ACL behavior split out of feature files.
- `sse_c_acl.rs`, `bucket_encryption_acl.rs`, `object_lock_acl.rs`, `lifecycle_acl.rs`, `checksums_acl.rs`, `conditional_acl.rs`, `website_redirect_acl.rs`, and `headers_acl.rs`: focused ACL-mode smoke coverage for feature surfaces whose broad coverage remains in BOE/default files.
- `bucket_policy.rs`: bucket-policy condition coverage remains there, including ACL-related condition keys, because the feature under test is policy evaluation.
- `ownership.rs`: ownership-control and ownership-transition behavior remains there; public-access-specific ownership cases moved out.

Public access tests now have explicit homes:

- `public_access_acl.rs`: anonymous access via public bucket/object ACLs, public-read/public-read-write, AuthenticatedUsers, and public ACL ownership edge cases.
- `public_access_acl_matrix.rs`: bucket ACL x object ACL matrix.
- `public_access_block.rs`: PublicAccessBlock CRUD and canonical XML.
- `public_access_block_acl.rs`: BlockPublicAcls and IgnorePublicAcls behavior for bucket/object ACLs.
- `public_access_policy.rs`: public bucket policy behavior, BlockPublicPolicy, and RestrictPublicBuckets.
- `public_access_headers.rs`, `public_access_post_object.rs`, `public_access_bucket_list.rs`, `public_access_cors.rs`, `public_access_range.rs`, and `public_access_object_lock.rs`: feature-specific anonymous/public access behavior split out of the ordinary feature files.

ABAC/tag-condition tests were not moved merely because they used a tag value
that looked public. Non-public arbitrary tag values were renamed to
`security=allow/deny` to avoid implying public-access semantics.

## Helper Cleanup

Added explicit test helpers in `crates/s3-tests/src/helpers.rs`:

- `create_boe_bucket(client) -> String`
  - Creates a bucket through the default path and documents that BOE is expected.
  - Use this when a test is intentionally BOE/default rather than incidentally default.
- `create_bucket_with_ownership(client, ObjectOwnership) -> String`
  - Creates a bucket with an explicit ownership header.
- `create_acl_enabled_bucket(client, ObjectOwnership) -> String`
  - Creates or switches a bucket into `ObjectWriter` or `BucketOwnerPreferred` and disables bucket-level PublicAccessBlock when ACL grants need to be public.
- Keep `create_public_bucket` and `create_public_write_bucket`, but reimplement them through the new ACL helper so setup is consistent.

Some local duplicate helpers remain in older files, but the shared helpers now
exist and new split files use them where practical.

## Coverage Additions

Added focused ACL-mode smoke coverage for surfaces previously dominated by
BOE/default tests. These are intentionally small and do not duplicate every BOE
case.

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

Completed:

1. Added shared helper functions and updated representative tests to use them.
2. Moved public access tests out of unrelated feature files into `public_access_*` files.
3. Moved ACL-mode cases out of mixed feature files into `*_acl.rs` files where mode is relevant.
4. Added `sse_c_acl.rs` and first-priority ACL-mode smoke tests.
5. Added second-priority ACL smoke tests.
6. Ran local `s3-tests` and targeted AWS reruns for failing groups after the final fixes.

## Progress

- Plan implementation is complete as of commit `0e6ef00` plus the earlier split/smoke-test commits.
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
- Moved bucket CreateBucket/recreate/header ACL cases from `bucket_crud.rs` into `bucket_acl.rs`.
- Split public/anonymous header behavior from `headers.rs` into `public_access_headers.rs`, and moved bad ACL header coverage into `headers_acl.rs`.
- Split anonymous public-write POST object behavior from `post_object.rs` into `public_access_post_object.rs`.
- Split anonymous public bucket listing success cases from `bucket_list.rs` into `public_access_bucket_list.rs`; private anonymous-denied listing cases remain in `bucket_list.rs`.
- Split public-bucket CORS actual request and presigned preflight coverage from `cors.rs` into `public_access_cors.rs`; private-bucket and ordinary CORS coverage remains in `cors.rs`.
- Split bucket/object ACL expected-owner cases from `expected_bucket_owner.rs` into `expected_bucket_owner_acl.rs`.
- Split versioned object ACL grant behavior from `versioning.rs` into `versioning_acl.rs`.
- Moved anonymous/public-read ownership behavior from `ownership.rs` into `public_access_acl.rs`, kept public-read bucket ACL BOE-transition coverage there, and rewrote ownership bucket ACL rejection setup to use explicit alternate-account grants instead of public bucket ACLs.
- Renamed arbitrary ABAC tag values in `tagging.rs` from `security=public/private` to `security=allow/deny` where no public access behavior is involved.
- Moved the `s3:x-amz-acl=public*` PutObject bucket-policy condition test from `tagging.rs` into `bucket_policy.rs`.
- Split anonymous public range response-shape and malformed `Range` header coverage from `range.rs` into `public_access_range.rs`.
- Added `bucket_encryption_acl.rs` ObjectWriter smoke coverage for SSE-C default blocking, explicit blocking, multipart initiation blocking, and explicit unblocking.
- Split public-write/anonymous object-lock access checks from `object_lock.rs` into `public_access_object_lock.rs`, renamed non-public ABAC tag values there to `security=allow/deny`, and added `object_lock_acl.rs` ObjectWriter smoke coverage for retention, legal hold, PutObject headers, and multipart headers.
- Added `lifecycle_acl.rs` ObjectWriter lifecycle CRUD smoke coverage and `checksums_acl.rs` ObjectWriter single-part and multipart checksum smoke coverage.
- Added second-priority ObjectWriter smoke coverage in `conditional_acl.rs` and `website_redirect_acl.rs`, plus a raw signed ACL header acceptance case in `headers_acl.rs`.
- Full AWS verification uncovered follow-up test-harness and conformance fixes:
  - raw HTTP helper now sends `Content-Length` for normal PUT/POST requests and has an explicit malformed-request opt-out;
  - `PutObject` without `Content-Length` is now covered and rejected with `MissingContentLength`;
  - transient hyper closed/incomplete-message errors are classified as retryable connector IO;
  - lifecycle ACL deletion waits through AWS delete convergence;
  - SSE-C cleanup aborts visible multipart uploads and retries bucket deletion on AWS cleanup races;
  - atomic read tests retry the whole scenario on external-S3 transport timeouts.

## Verification Checklist

- `cargo fmt`: run during the implementation slices.
- `cargo nextest run -p s3-tests`: passed after final fixes (`1452 passed`).
- `cargo clippy --all-targets --all-features -- -D warnings`: passed during the smoke-test slices before final AWS-failure fixes.
- Targeted AWS reruns for the failing groups: passed per operator report.
- Full `./scripts/aws-tests`: recommended once more if a final end-to-end AWS green run is required for close-out; not recorded as clean after commit `0e6ef00`.

For pure file moves, compare test inventory before/after so coverage is preserved:

```bash
cargo test -p s3-tests -- --list 2>&1 | rg ': test$' | sort > /tmp/s3-tests.before
cargo test -p s3-tests -- --list 2>&1 | rg ': test$' | sort > /tmp/s3-tests.after
diff -u /tmp/s3-tests.before /tmp/s3-tests.after
```
