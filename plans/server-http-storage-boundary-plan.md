# Server HTTP / Storage Boundary Plan

## Status

Completed:

1. checksum-domain shared types moved into `checksum`
2. `VersionId` and `BucketVersioningState` moved into `s3-types`
3. `server-core` now returns core-owned DTOs instead of leaking
   `storage::BucketInfo` to HTTP
4. runtime wiring moved into `crates/argmin-s3`
5. `server-http` no longer has a normal dependency on `storage`
6. object-level authorization moved into `server-core` for:
   - `PutObject`
   - `CopyObject`
   - `UploadPartCopy`
   - `GetObject`
   - `HeadObject`
   - `GetObjectPart`
   - `HeadObjectPart`
   - `GetObjectRange`
   - `GetObjectAttributes`
   - `DeleteObject`
   - `DeleteObjects`

Current state:

1. `server-http` owns HTTP authentication, header/XML/query parsing, and
   response rendering
2. `server-core` owns object-level authorization for the common object paths
3. `storage` remains independent of both `server-core` and `server-http`
4. the main remaining boundary issue is bucket-scoped and multipart/streaming
   authorization and stored-state policy still living in `server-http`
5. the in-flight read vs object reclamation race discovered during this work is
   tracked separately in
   [`plans/in-flight-read-reclamation-plan.md`](./in-flight-read-reclamation-plan.md)

## Context

The `server` split is in place:

1. `crates/server-core` holds coordinator/core logic
2. `crates/server-http` holds HTTP parsing/rendering and the frontend surface
3. `crates/argmin-s3` is the concrete runtime entrypoint

The original `server-http -> storage` boundary cleanup has served its purpose:

1. shared checksum types moved to `checksum`
2. shared non-checksum S3 value types moved to `s3-types`
3. `server-http` no longer depends on storage-owned types in normal code
4. runtime/bootstrap wiring no longer lives in `server-http`

The remaining issue is now narrower:

1. `server-http` still performs authorization for a number of bucket-scoped,
   multipart, and streaming operations
2. `server-http` still enforces some policy checks that depend on stored bucket
   state
3. `server-core` then executes the operation later against current state

That is still the wrong layering, and for write paths it still leaves the check
outside the point of use.

## Goals

1. finish moving authorization into `server-core`
2. move stored-state policy checks into `server-core`
3. keep `server-http` responsible only for authentication and request parsing
4. keep `storage` policy-agnostic
5. only revisit the deeper `server-core -> storage` concreteness after this
   boundary cleanup is actually complete

## Non-goals

1. do not redesign `Coordinator` around generic storage traits yet
2. do not move S3 policy semantics into `storage`
3. do not solve the in-flight read/object-reclamation race in this plan
4. do not add read-side bucket locking as part of this plan

## Serialization Points

The serialization points today are:

1. per-PG mutexes in `SharedStorageNode`
2. the bucket stripe lock in `SharedStorageNode`

That means:

1. moving authz into `server-core` fixes the layering and non-HTTP bypass
   problems
2. it does not by itself create stronger serialization
3. for write paths, authz and stored-state policy should run in `server-core`
   after taking the bucket lock and immediately before the mutation proceeds
4. for read paths, authz in `server-core` without bucket locking means
   authorize-at-operation-start semantics

That read-side policy is acceptable here. The separate in-flight read/object
reclamation race is not an auth problem and is tracked separately.

## What Stays in `server-http`

1. SigV4 header, presign, and POST authentication
2. aws-chunked signature verification
3. request syntax validation
4. parsing raw headers, XML, and query strings into typed core requests
5. response formatting

## What Moves to `server-core`

1. bucket and object authorization decisions
2. public-read plus public-access-block interpretation for reads
3. BucketOwnerEnforced / ACL compatibility checks that depend on stored bucket
   state
4. BlockPublicAcls-style checks that depend on stored bucket state
5. source-read plus destination-write authorization for copy and multipart-copy
   operations

## Remaining Surface

### Bucket and list operations still using HTTP-side authorization

1. `DeleteBucket`
2. `HeadBucket`
3. `ListObjectsV1`
4. `ListObjectsV2`
5. `ListObjectVersions`
6. `PutBucketVersioning`
7. `GetBucketVersioning`
8. `PutBucketCors`
9. `GetBucketCors`
10. `DeleteBucketCors`
11. `PutBucketTagging`
12. `GetBucketTagging`
13. `DeleteBucketTagging`
14. `PutObjectTagging`
15. `GetObjectTagging`
16. `DeleteObjectTagging`
17. `PutBucketPublicAccessBlock`
18. `GetBucketPublicAccessBlock`
19. `DeleteBucketPublicAccessBlock`
20. `PutBucketOwnershipControls`
21. `GetBucketOwnershipControls`
22. `DeleteBucketOwnershipControls`
23. `PutBucketAcl`

### Multipart and streaming operations still using HTTP-side authorization

1. `CreateMultipartUpload`
2. `UploadPart`
3. `CompleteMultipartUpload`
4. `AbortMultipartUpload`
5. `ListMultipartUploads`
6. `ListParts`
7. `POST Object`
8. streaming `PutObject`
9. streaming `UploadPart`

### Stored-state policy still enforced in `server-http`

1. `authorize_bucket_read` still derives effective public visibility from
   `public_read` plus `IgnorePublicAcls`
2. `PutBucketOwnershipControls` still checks
   `BucketOwnerEnforced` against the current bucket ACL state
3. `PutBucketAcl` still enforces `BucketOwnerEnforced` and `BlockPublicAcls`
   using stored bucket state
4. `POST Object` and streaming `PutObject` still enforce
   BucketOwnerEnforced-vs-ACL in HTTP

## Recommended Interface Shape

Use a small core-facing requester type:

```rust
pub enum Requester<'a> {
    Anonymous,
    Principal(&'a str),
}
```

The rule is:

1. `server-http` authenticates and constructs `Requester`
2. `server-core` authorizes using `Requester` plus current bucket or object
   state
3. `storage` remains auth-agnostic

## Remaining Implementation Order

### Step 1: bucket read and list operations

Move requester-aware authz into `server-core` for:

1. `HeadBucket`
2. `ListObjectsV1`
3. `ListObjectsV2`
4. `ListObjectVersions`
5. `ListMultipartUploads`
6. `ListParts`

These are the remaining common read-shaped entry points still gated in HTTP.

### Step 2: bucket-scoped write and subresource operations

Move requester-aware authz into `server-core` for:

1. `DeleteBucket`
2. `PutBucketVersioning`
3. `GetBucketVersioning`
4. `PutBucketCors`
5. `GetBucketCors`
6. `DeleteBucketCors`
7. `PutBucketTagging`
8. `GetBucketTagging`
9. `DeleteBucketTagging`
10. `PutObjectTagging`
11. `GetObjectTagging`
12. `DeleteObjectTagging`
13. `PutBucketPublicAccessBlock`
14. `GetBucketPublicAccessBlock`
15. `DeleteBucketPublicAccessBlock`
16. `PutBucketOwnershipControls`
17. `GetBucketOwnershipControls`
18. `DeleteBucketOwnershipControls`
19. `PutBucketAcl`

This step should also move the stored-state policy checks for ownership
controls, ACLs, and public-access-block into `server-core`.

### Step 3: multipart mutation operations

Move requester-aware authz into `server-core` for:

1. `CreateMultipartUpload`
2. `UploadPart`
3. `CompleteMultipartUpload`
4. `AbortMultipartUpload`

The main requirement is that write authorization and bucket-state policy are
checked in the same layer that finalizes or mutates the upload.

### Step 4: POST and streaming write paths

Move requester-aware authz and BucketOwnerEnforced ACL checks into
`server-core` for:

1. `POST Object`
2. streaming `PutObject`
3. streaming `UploadPart`

`server-http` should keep POST authentication and chunk-signature validation,
but it should stop making the authorization decision.

### Step 5: delete HTTP-side authz helpers

Once all remaining call sites are moved:

1. remove `authorize_bucket_read`
2. remove `authorize_bucket_write`
3. remove the remaining direct use of HTTP-layer authz policy helpers

## Deeper `server-core -> storage` Split

Only after the boundary work above is complete should we decide whether the
remaining `server-core -> storage` concreteness is worth abstracting further.

Current concrete coupling includes:

1. `Coordinator` stores `Arc<SharedStorageNode>`
2. helper code works with `MutexGuard<PgStore>`
3. many internal methods take `&PgStore`

That is a separate question from the HTTP/core boundary. Do not start there
until the remaining boundary work above is done.

## Validation

After each implementation step:

1. `cargo fmt --all`
2. `cargo test -p storage -p server-core -p server-http`
3. `cargo clippy -p server-core -p server-http --all-targets --no-deps -- -D warnings -W clippy::pedantic`

Add focused regression tests for:

1. direct coordinator calls that should now reject unauthorized access
2. bucket read authorization with `public_read` plus `IgnorePublicAcls`
3. BucketOwnerEnforced and BlockPublicAcls enforcement after the checks move
4. multipart and streaming write authz at the core boundary

## Open Questions

1. whether object-tagging operations should reuse the existing requester-aware
   object helpers or grow dedicated authz helpers in `server-core`
2. whether `HeadBucket` and bucket subresource reads should share a bucket-level
   request DTO rather than each growing requester fields independently
3. whether the remaining `server-core -> storage` concreteness is actually a
   problem once the authz move is complete

## Recommendation Summary

Recommended next move:

1. finish the remaining authz move into `server-core`
2. start with bucket read and list operations
3. then move bucket subresource writes and their stored-state policy
4. then cover multipart, POST, and streaming paths
5. only after that reassess the deeper core/storage coupling
