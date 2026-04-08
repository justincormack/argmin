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

Argmin has only partial website-related support today. The CORS-related pieces
that were needed for implemented behavior are covered, but the broader S3
website-hosting feature set is not complete.

In practice, treat bucket website hosting as unsupported except for the pieces
that are already covered by tests.

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

## How to treat new differences

If AWS-backed tests expose a difference that is not listed here:

1. treat it as a bug first
2. verify against real AWS behavior
3. either fix it or document it here as an intentional temporary gap

The goal is to keep the undocumented compatibility surface as small as
possible.
