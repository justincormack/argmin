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
7. bucket and list authorization moved into `server-core` for:
   - `DeleteBucket`
   - `HeadBucket`
   - `ListObjectsV1`
   - `ListObjectsV2`
   - `ListObjectVersions`
   - `ListMultipartUploads`
   - `ListParts`
   - `PutBucketVersioning`
   - `GetBucketVersioning`
   - `PutBucketCors`
   - `GetBucketCors`
   - `DeleteBucketCors`
   - `PutBucketTagging`
   - `GetBucketTagging`
   - `DeleteBucketTagging`
   - `PutObjectTagging`
   - `GetObjectTagging`
   - `DeleteObjectTagging`
   - `PutBucketPublicAccessBlock`
   - `GetBucketPublicAccessBlock`
   - `DeleteBucketPublicAccessBlock`
   - `PutBucketOwnershipControls`
   - `GetBucketOwnershipControls`
   - `DeleteBucketOwnershipControls`
   - `PutBucketAcl`
8. multipart and streaming write authorization moved into `server-core` for:
   - `CreateMultipartUpload`
   - `UploadPart`
   - `CompleteMultipartUpload`
   - `AbortMultipartUpload`
   - streaming `PutObject` session creation
   - streaming `UploadPart` session creation
9. stored-state ACL and ownership-controls policy moved into `server-core` for:
   - `PutBucketOwnershipControls`
   - `PutBucketAcl`
   - `PutObject`
   - `CopyObject`
   - streaming `PutObject`
10. `server-http` no longer has bucket/object auth helpers
11. `CreateBucket` and `ListBuckets` now use core-owned request types and no
    longer route owner-principal plumbing or ownership-controls semantics
    through `server-http`
12. object-creation tagging now commits in the initial core/storage write for:
    - `PutObject`
    - `CopyObject`
    - streaming `PutObject` finalize

Current state:

1. `server-http` owns HTTP authentication, header/XML/query parsing, and
   response rendering
2. `server-core` owns bucket/object authorization for the S3 data-path and
   bucket-subresource operations
3. `storage` remains independent of both `server-core` and `server-http`
4. the main authz boundary goal of this plan is effectively complete
5. the main HTTP/core boundary work is complete
6. the in-flight read vs object reclamation race discovered during this work is
   tracked separately in
   [`in-flight-read-reclamation-plan.md`](./in-flight-read-reclamation-plan.md)
7. `GetBucketAcl` is also core-owned now

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

The original issue has now been addressed for the main API surface:

1. `server-http` authenticates and parses requests
2. `server-core` authorizes against current bucket/object state and executes
   the operation
3. `storage` remains policy-agnostic

The original boundary bug is now addressed. The remaining question is whether
the deeper `server-core -> storage` concreteness is worth abstracting further.

## Goals

1. keep `server-http` responsible only for authentication and request parsing
2. keep `storage` policy-agnostic
3. only revisit the deeper `server-core -> storage` concreteness after the
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

The HTTP/core boundary work tracked by this plan is complete.

The remaining open item is:

1. the deeper `server-core -> storage` concreteness question, which remains
   intentionally deferred

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

## Deeper `server-core -> storage` Split

Only after the boundary work above is complete should we decide whether the
remaining `server-core -> storage` concreteness is worth abstracting further.

Current concrete coupling includes:

1. `Coordinator` stores `Arc<SharedStorageNode>`
2. helper code works with `MutexGuard<PgStore>`
3. many internal methods take `&PgStore`

That is a separate question from the HTTP/core boundary. Do not start there
until the remaining boundary work above is done.

## Completion Note

This plan is complete for its intended scope:

1. `server-http` is the transport/authentication boundary
2. `server-core` owns S3 authorization and stored-state policy
3. `storage` remains policy-agnostic

The only remaining question is whether a narrower storage abstraction is worth
introducing under `server-core`. That is a separate future design choice, not
unfinished work from this boundary cleanup.
