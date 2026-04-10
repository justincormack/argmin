# Bucket Config Auth Follow-Up Plan

## Status

Completed:

1. bucket encryption put/get/delete now use per-operation auth tokens
2. bucket ACL get/put now use per-operation auth tokens
3. `GetBucketObjectLockConfiguration` now uses a per-operation auth token
4. `GetBucketPolicyStatus` now uses a per-operation auth token
5. bucket ACL validation now runs in the auth path
6. coordinator handlers now follow `authorize -> apply`
7. direct auth tests cover the nontrivial bucket encryption, bucket ACL,
   object-lock, and bucket-policy-status cases
8. object tagging / ACL / object-lock operations now use per-operation auth
   entrypoints with authorized tokens or results
9. direct auth tests cover representative object tagging, object ACL, object
   retention, and object legal hold cases

## Context

The bucket-subresource refactor established a cleaner pattern for S3
configuration operations:

1. one auth entrypoint per S3 operation in `authz.rs`
2. operation-specific validation in the auth path
3. typed authorized values flowing into storage/mutation helpers

That pattern is now in place for bucket subresources, but some neighboring
bucket configuration APIs still use older inline auth-and-mutate handlers.

## Completed Slice

Applied the same pattern to:

1. bucket encryption
2. bucket ACL
3. `GetBucketObjectLockConfiguration`
4. `GetBucketPolicyStatus`
5. object tagging / ACL / object-lock operations
