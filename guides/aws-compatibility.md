# AWS Compatibility Guide

Argmin aims to match AWS S3 behavior as closely as possible on the implemented
surface.

The default expectation in this repository is:

- behavioral differences from AWS are bugs unless they are explicitly documented
- AWS-backed tests are the source of truth when docs and assumptions diverge
- known incompatibilities should be written down clearly rather than left as
  tribal knowledge

This guide is a short list of currently known gaps. It is intentionally
non-exhaustive, but anything listed here should be treated as an explicit,
temporary compatibility exception rather than a surprise.

## Bucket scope

Argmin currently supports the normal S3 API surface for standard buckets.
It does not implement AWS S3 directory buckets / S3 Express One Zone as a
bucket class.

That said, directory-bucket-only request features still need to behave like
AWS when they are sent to a standard bucket. We keep explicit compatibility
coverage for that behavior in
[crates/s3-tests/tests/directory_bucket_features.rs](/home/justin/src/github.com/justincormack/argmin/crates/s3-tests/tests/directory_bucket_features.rs),
including cases where AWS ignores a directory-bucket-specific query and cases
where AWS rejects a directory-bucket-specific header or parameter with the
corresponding error.

## Behavior notes

### Delete-then-recreate bucket name reuse may require retry

`DeleteBucket` should not be treated as proof that the same bucket name is
immediately reusable.

AWS appears to have a name-reuse window here as well: a client that deletes a
bucket and immediately issues `CreateBucket` for the same name can observe a
transient `409` such as `BucketAlreadyExists`, `BucketAlreadyOwnedByYou`, or
`OperationAborted` before the namespace is fully reusable.

This is not tracked as a compatibility gap. It is a behavior note for tests and
clients:

- prefer retrying the exact `CreateBucket` operation when reusing a just-deleted
  bucket name
- do not assume that a successful `DeleteBucket` implies synchronous namespace
  release
- in `crates/s3-tests`, use the bounded recreate helper rather than a fixed
  sleep for delete-then-recreate flows

### `x-amz-copy-source` percent-encoded NUL is intentionally rejected as a client error

AWS currently responds with `500 InternalError` for at least one malformed
`x-amz-copy-source` case: a percent-encoded NUL byte in the source object key.

Argmin intentionally does not match that behavior. We reject this as
`400 InvalidArgument` because the input is client-invalid and treating it as a
server fault would be the wrong contract to preserve.

The AWS-backed `s3-tests` coverage for this case is allowed to diverge
explicitly, and should not be treated as a general license to ignore AWS
results elsewhere.

### Streaming write prepare failures can differ at the transport level

For streaming `PutObject`, `UploadPart`, and streaming `POST Object` flows,
Argmin distinguishes between:

- auth-layer failures or anonymous denies, where the server closes promptly to
  avoid letting an unauthenticated client hold request capacity by continuing
  to stream a body, and
- authenticated permission denials, where the server prefers to drain/respond
  so SDK clients see a normal S3 error instead of a transport-level broken
  pipe while still writing the request body.

The compatibility target remains the same final S3 error code and body.
However, exact transport timing and whether the connection is closed early can
still differ slightly from AWS in some edge cases with unread streaming
bodies.

## Current known gaps

### 1. Single-part `ETag` is not AWS MD5

For single-part objects, Argmin does not currently return the AWS-style MD5
`ETag`.

Our current `ETag` is an opaque integrity token derived from the internal object
representation. Clients should treat `ETag` as opaque.

If a client needs payload checksumming semantics, use S3 checksum fields
instead:

- `x-amz-checksum-*` response headers
- checksum fields in XML responses where AWS provides them
- `Content-MD5` if an MD5 payload checksum is specifically wanted

Related notes:

- [plans/completed/territory-map.md](/home/justin/src/github.com/justincormack/argmin/plans/completed/territory-map.md)
- [plans/completed/sse-c-encryption-plan.md](/home/justin/src/github.com/justincormack/argmin/plans/completed/sse-c-encryption-plan.md)

### 2. `SSE-KMS` is not implemented

Argmin supports `SSE-S3` and `SSE-C`, but not real AWS-compatible `SSE-KMS`
yet.

Related plan:

- [plans/encryption-compat-plan.md](/home/justin/src/github.com/justincormack/argmin/plans/encryption-compat-plan.md)

### 3. Bucket logging is not implemented

Bucket logging and the related `LogDelivery` compatibility surface are not
implemented yet.

This also means the logging-specific ACL compatibility work remains deferred.

Related notes:

- [plans/completed/ceph-closeout-plan.md](/home/justin/src/github.com/justincormack/argmin/plans/completed/ceph-closeout-plan.md)
- [plans/completed/acl-compatibility-follow-up-plan.md](/home/justin/src/github.com/justincormack/argmin/plans/completed/acl-compatibility-follow-up-plan.md)

### 4. Bucket website support is only partial

Argmin now supports the REST object-metadata surface for
`x-amz-website-redirect-location` / `WebsiteRedirectLocation`, including
request parsing, persistence, readback, copy, multipart, POST, and the
corresponding AWS error shapes.

The remaining website-related gaps are:

- bucket website configuration APIs such as `PutBucketWebsite`,
  `GetBucketWebsite`, and `DeleteBucketWebsite`
- broader website-endpoint hosting behavior

In practice, treat bucket website hosting as unsupported. The implemented
piece here is limited to object redirect metadata on the normal S3 REST
endpoint, not the website endpoint or bucket website configuration APIs.

Related note:

- [plans/website-behavior-note.md](/home/justin/src/github.com/justincormack/argmin/plans/website-behavior-note.md)

### 5. Alternate AWS access URL forms are not supported

Argmin currently targets path-style access on the normal API endpoint.

Argmin currently exposes a single S3 API endpoint. It does not implement the
separate AWS `s3-control` endpoint family, and it does not currently inspect or
differentiate endpoint-style host headers beyond the normal S3 API surface.

Current narrow exception:

- the minimal bucket-ABAC `TagResource` / `UntagResource` subset is currently
  accepted on the same endpoint as the rest of the S3 API
- AWS itself expects those operations on the account-prefixed control-plane
  host `https://{account_id}.s3-control.{region}.amazonaws.com`
- Argmin does not yet enforce that distinct AWS `s3-control` `Host` /
  endpoint shape for those operations and still accepts them on the ordinary
  S3 endpoint
- this is a temporary compatibility compromise and may be tightened later once
  the dedicated `s3-control` routing surface exists locally

Unsupported URL/addressing forms include:

- website endpoints
- DNS / virtual-hosted-style bucket addressing
- `s3-control` endpoints and their distinct host/endpoint routing model
- other AWS endpoint variants that depend on bucket-in-host routing

Related note:

- [guides/threat_model.md](/home/justin/src/github.com/justincormack/argmin/guides/threat_model.md)

### 6. Billing surfaces are not implemented

Argmin does not implement AWS billing behavior.

This is broader than one missing header or bucket flag. Matching AWS here would
require a real billing model with charge attribution, owner-versus-requester
cost semantics, and the corresponding request / response behavior across the
affected APIs. That infrastructure is not currently planned.

#### Requester Pays

Requester Pays is therefore also not implemented.

This remains a real AWS compatibility gap, but it does not make sense as a
standalone feature without the wider billing surface behind it.

The missing wire-visible surface includes:

- bucket requester-pays configuration
- `x-amz-request-payer: requester`
- `x-amz-request-charged: requester`

### 7. MFA Delete is not implemented

Argmin supports the basic bucket versioning state (`Enabled` / `Suspended`),
but it does not implement AWS's MFA Delete surface.

That means the current gap includes both the bucket-versioning control path and
the delete path that AWS protects with the same `x-amz-mfa` header family:

- `GetBucketVersioning` does not expose the `MfaDelete` response element
- `PutBucketVersioning` does not implement `MfaDelete` request handling or the
  required `x-amz-mfa` header flow for changing MFA Delete state
- `DeleteObjects` does not implement the optional `x-amz-mfa` header behavior
  used by AWS when MFA Delete is enabled on the target bucket

AWS documents this as part of the bucket versioning configuration rather than a
separate API surface:

- `GetBucketVersioning` returns `MfaDelete` when the bucket has been configured
  with MFA Delete
- `PutBucketVersioning` requires `x-amz-mfa` plus both `Status` and
  `MfaDelete` when enabling MFA Delete

Sources:

- https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html
- https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html
- https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html

### 8. Lifecycle transition-default header is not implemented

Argmin does not currently implement lifecycle transition behavior, and it also
does not implement the corresponding
`x-amz-transition-default-minimum-object-size` request surface on
`PutBucketLifecycleConfiguration`.

Today:

- `PutBucketLifecycleConfiguration` ignores that modeled request header because
  transition semantics are not implemented
- `GetBucketLifecycleConfiguration` currently returns a fixed
  `x-amz-transition-default-minimum-object-size: all_storage_classes_128K`
  header rather than a real persisted value

Treat this header as unsupported until lifecycle transition behavior is
implemented end-to-end.

Source:

- https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketLifecycleConfiguration.html

### 9. `HeadBucket` omits `Transfer-Encoding: chunked`

AWS includes `Transfer-Encoding: chunked` on successful `HeadBucket`
responses. Argmin does not currently emit that header.

This is not an intentional S3 behavior difference in our application logic.
The remaining mismatch appears to come from Hyper's `HEAD` response handling,
which suppresses chunked transfer encoding and sends a zero-length response
instead.

The `s3-diff-tests` `HeadBucket` response-shape check ignores only this header.

### 10. SigV2 is not implemented

Argmin does not implement AWS Signature Version 2 authentication.

That is intentionally not part of the current auth surface, but it is still a
real compatibility gap because AWS continues to accept SigV2 in at least some
older regions even though newer regions require SigV4.

In practice:

- Argmin requires SigV4 everywhere
- AWS still accepts SigV2 in some regions such as `us-east-1` and `us-west-2`
- AWS rejects SigV2 in newer regions such as `eu-central-1`

Related note:

- [plans/sigv2-compat-note.md](/home/justin/src/github.com/justincormack/argmin/plans/sigv2-compat-note.md)

### 11. Account-level Block Public Access is not implemented

Argmin supports the bucket-level public access block surface needed by the
current feature set, but it does not implement AWS account-level Block Public
Access controls yet.

That means account-scoped public-access restrictions and their interaction with
bucket ACL and public-write behavior can still differ from AWS.

Related plan:

- [plans/aws-auth-compat-plan.md](/home/justin/src/github.com/justincormack/argmin/plans/aws-auth-compat-plan.md)

### 12. The `s3-control` API family is not implemented

Argmin implements the normal S3 API/data-plane endpoint and only a very narrow
bucket-ABAC subset of `s3-control`.

That means the following AWS surface is currently unsupported:

- `s3-control` endpoint routing and host-style distinctions
- broader `s3-control` surfaces such as access-point, multi-region access
  point, Storage Lens, batch operations, and other account/control-plane APIs

Current narrow exception:

- `TagResource` / `UntagResource` for the bucket-ABAC general-purpose-bucket
  flow are implemented
- AWS-pinned control-plane behavior uses the account-prefixed host
  `https://{account_id}.s3-control.{region}.amazonaws.com`
- locally they are still accepted on the ordinary S3 endpoint without distinct
  `s3-control` host validation
- this is intentionally temporary and may be tightened later

In practice, any AWS behavior that depends on `s3-control` APIs, endpoint
routing, or control-plane state such as `TagResource` / `UntagResource` should
be treated as unsupported apart from this narrow bucket-ABAC tagging subset.

### 12. Bucket-policy condition acceptance and runtime context are still partial

Argmin's compatibility target is to accept the same bucket-policy condition
keys that AWS accepts on the implemented S3 surface, even when the current
runtime does not yet have enough request context to make every condition
equally useful.

Today, that means two different compatibility questions have to be kept
separate:

- policy upload acceptance
- runtime evaluation of a stored policy against a live request

Policy upload acceptance is being aligned to AWS-backed behavior. Runtime
evaluation is still narrower.

Current runtime-evaluation limits include:

- network/account condition keys such as `aws:PrincipalArn`, `aws:SourceVpc`,
  `aws:SourceVpce`,
  `aws:SourceArn`, `aws:SourceAccount`, `aws:SourceOwner`, `aws:userid`,
  `aws:PrincipalOrgID`, `s3:DataAccessPointAccount`, and
  `s3:DataAccessPointArn` still remain outside the current accepted/evaluable
  object-condition subset
- `s3:ResourceTag/*` object-policy evaluation is still deferred

The current implemented evaluator is strongest on:

- `s3:ExistingObjectTag/*`
- `s3:RequestObjectTag/*`
- the supported `s3:x-amz-*` request condition keys already threaded through
  `PolicyRequest`

So a bucket policy may now be AWS-accepted and storable even if some condition
clauses are still not runtime-evaluable locally. That is the correct direction
for upload-time conformance, but it remains a known compatibility gap until
those request attributes are modeled directly.

Related plan:

- [plans/aws-auth-compat-plan.md](/home/justin/src/github.com/justincormack/argmin/plans/aws-auth-compat-plan.md)

### 13. `AccessDenied` does not yet match AWS principal-specific error text

Argmin now matches the generic XML error shape for several `AccessDenied`
cases, but it does not yet reproduce AWS's more specific denial messages that
name the requester principal and the denied action/resource.

In practice this means anonymous/private-object `AccessDenied` responses are
covered, but authenticated denials that AWS renders with caller-specific IAM
or account details may still differ in `<Message>`.

This should be revisited as part of durable account and credential work,
because exact matching depends on carrying richer persistent account identity
through to error rendering.

Related plan:

- [plans/persistent-account-and-credential-management.md](/home/justin/src/github.com/justincormack/argmin/plans/persistent-account-and-credential-management.md)

## How to treat new differences

If AWS-backed tests expose a difference that is not listed here:

1. treat it as a bug first
2. verify against real AWS behavior
3. either fix it or document it here as an intentional temporary gap

The goal is to keep the undocumented compatibility surface as small as
possible.
