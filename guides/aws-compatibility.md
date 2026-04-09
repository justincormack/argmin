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

Argmin has only partial website-related support today.

Known gaps here include:

- object-level website redirect metadata such as
  `x-amz-website-redirect-location` / `WebsiteRedirectLocation`
- bucket website configuration APIs such as `PutBucketWebsite`,
  `GetBucketWebsite`, and `DeleteBucketWebsite`
- broader website-endpoint hosting behavior

In practice, treat bucket website hosting as unsupported except for the pieces
that are already covered by tests.

Related note:

- [plans/website-behavior-note.md](/home/justin/src/github.com/justincormack/argmin/plans/website-behavior-note.md)

### 5. Alternate AWS access URL forms are not supported

Argmin currently targets path-style access on the normal API endpoint.

Unsupported URL/addressing forms include:

- website endpoints
- DNS / virtual-hosted-style bucket addressing
- other AWS endpoint variants that depend on bucket-in-host routing

Related note:

- [guides/threat_model.md](/home/justin/src/github.com/justincormack/argmin/guides/threat_model.md)

### 6. Requester Pays is not implemented

Requester Pays is a real AWS compatibility gap and is not implemented yet.

That includes the main wire-visible surface:

- bucket requester-pays configuration
- `x-amz-request-payer: requester`
- `x-amz-request-charged: requester`

Related note:

- [plans/requester-pays-note.md](/home/justin/src/github.com/justincormack/argmin/plans/requester-pays-note.md)

### 7. MFA Delete on bucket versioning is not implemented

Argmin supports the basic bucket versioning state (`Enabled` / `Suspended`),
but it does not implement AWS's MFA Delete surface.

That means the current gap includes both APIs involved in the AWS behavior:

- `GetBucketVersioning` does not expose the `MfaDelete` response element
- `PutBucketVersioning` does not implement `MfaDelete` request handling or the
  required `x-amz-mfa` header flow for changing MFA Delete state

AWS documents this as part of the bucket versioning configuration rather than a
separate API surface:

- `GetBucketVersioning` returns `MfaDelete` when the bucket has been configured
  with MFA Delete
- `PutBucketVersioning` requires `x-amz-mfa` plus both `Status` and
  `MfaDelete` when enabling MFA Delete

Sources:

- https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketVersioning.html
- https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketVersioning.html

### 8. `HeadBucket` omits `Transfer-Encoding: chunked`

AWS includes `Transfer-Encoding: chunked` on successful `HeadBucket`
responses. Argmin does not currently emit that header.

This is not an intentional S3 behavior difference in our application logic.
The remaining mismatch appears to come from Hyper's `HEAD` response handling,
which suppresses chunked transfer encoding and sends a zero-length response
instead.

The `s3-diff-tests` `HeadBucket` response-shape check ignores only this header.

### 9. SigV2 is not implemented

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

### 10. Object ownership and object ACL behavior are incomplete

Argmin does not yet implement full AWS object ownership semantics or the full
object ACL API surface.

This is an AWS-visible compatibility gap for objects written by a different
principal than the bucket owner. In particular, AWS distinguishes the object
owner from the bucket owner for anonymous or public-write uploads, and bucket
owner read/delete behavior can differ from Argmin's current simplified model.

Related plan:

- [plans/aws-auth-compat-plan.md](/home/justin/src/github.com/justincormack/argmin/plans/aws-auth-compat-plan.md)

### 11. Account-level Block Public Access is not implemented

Argmin supports the bucket-level public access block surface needed by the
current feature set, but it does not implement AWS account-level Block Public
Access controls yet.

That means account-scoped public-access restrictions and their interaction with
bucket ACL and public-write behavior can still differ from AWS.

Related plan:

- [plans/aws-auth-compat-plan.md](/home/justin/src/github.com/justincormack/argmin/plans/aws-auth-compat-plan.md)

### 12. Bucket policy CRUD does not yet match AWS root-principal behavior

For `GetBucketPolicy`, `PutBucketPolicy`, and `DeleteBucketPolicy`, AWS allows
the bucket owner's account `root` principal to perform the operation even if
the bucket policy explicitly denies that root principal.

Argmin does not yet model that carveout precisely. Current behavior is broader
than AWS because it does not fully distinguish the owner account root principal
from other principals in the same account.

Related plan:

- [plans/bucket-policy-root-principal-compat-plan.md](/home/justin/src/github.com/justincormack/argmin/plans/bucket-policy-root-principal-compat-plan.md)

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
