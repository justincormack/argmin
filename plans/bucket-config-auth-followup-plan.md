# Bucket Config Auth Follow-Up Plan

## Status

Completed:

1. bucket encryption put/get/delete now use per-operation auth tokens
2. bucket ACL get/put now use per-operation auth tokens
3. bucket ACL validation now runs in the auth path
4. coordinator handlers now follow `authorize -> apply`
5. direct auth tests cover the nontrivial bucket encryption and bucket ACL cases

Remaining candidates:

1. `GetBucketObjectLockConfiguration`
2. `GetBucketPolicyStatus`
3. object tagging / ACL / object-lock authorization helpers that still combine
   fetch, lock, and authorization into shared flows

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
