# Authorization Timing Guide

This guide defines the repository policy for when authentication-derived
authorization decisions are made during request handling.

The main goal is to keep client-visible behavior coherent and to avoid
accidentally turning internal implementation phases into externally visible
authorization boundaries.

## Policy

### 1. Authorization is per external request

Authorization decisions are made per external S3 request, not per internal
helper, session, or implementation phase.

Examples:

- `GetObject` is one authorization decision
- `PutObject` is one authorization decision
- `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, and
  `AbortMultipartUpload` are each separate authorization decisions because
  they are separate client requests

Internal streaming phases such as `begin_stream_put`,
`append_stream_segment`, and `finalize_stream_put` must not implicitly become
separate permission checks for a single `PutObject` request.

### 2. Reads authorize at operation start

Read operations must authorize before they:

- return object or bucket metadata
- acquire a readable body handle
- reveal object existence beyond the operation's intended masked-error
  semantics

For reads, authorize-at-operation-start semantics are the default policy.

### 3. Single-request writes currently authorize once at request entry

A single client write request must authorize once before meaningful body
ingestion or durable side effects begin.

This includes:

- normal buffered writes
- streaming `PutObject`
- copy destination writes
- any future streamed single-request write path

The purpose is to avoid late `AccessDenied` responses after substantial upload
work for a request that was already structurally valid and authenticated.

### 4. Finalize and commit phases currently do not re-authorize the same request

Commit-time logic may still reject the request, but those rejections should be
about mutable stored state or request validity rather than permission being
re-evaluated for the same request.

Allowed commit-time checks include:

- overwrite and conditional request conflicts
- object lock and retention state checks
- checksum and integrity validation
- session state validation
- durable storage and metadata commit invariants

Do not re-run the same request's bucket/object authorization decision at
finalize time just because the implementation is internally multi-phase.

This is the current implementation rule, not a complete claim about AWS.
Exploratory AWS oracles found two ObjectWriter PutObject cases in which state
that changed after the body started controlled the final response: an
identical-byte replacement that changed current object ownership, and a bucket
ACL write grant revoked before the body completed. Both returned `403
AccessDenied` without publishing the in-flight write. An already-denied paused
PutObject also did not expose its response until its body completed.

These observations are tracked for a comprehensive authorization-timing pass.
Do not add a shared commit-time reauthorization call based only on them:
PutObject, CopyObject, POST Object, BOE, ObjectWriter, current delete markers,
and bucket-policy versus object authorization state require separate oracle
coverage and operation-specific capabilities. Until that matrix is complete,
the entry authorization token remains authoritative locally and the known
PutObject differences remain documented in the work plan.

### 5. Multi-phase writes should carry an authorized token

If a single external write request spans multiple internal phases, request
entry should produce an opaque authorized token or capability that represents
the approved write intent for that request.

That token should carry the auth-sensitive request state that the
authorization decision depended on, such as:

- bucket and key
- requester identity
- expected bucket owner
- ACL intent
- requested object lock state
- tags
- resolved encryption policy

Later internal phases should consume that token rather than accepting a second
independent description of the same auth-sensitive write request.

In practice this means:

- begin/finalize helpers should take the authorized token as input
- later phases may accept commit-time data such as checksums, ETags, segment
  references, or conditional-write inputs
- later phases should not accept caller-controlled bucket/key/requester/ACL/
  tag/object-lock inputs that could diverge from what was authorized at
  request entry

This keeps the "authorize once per external request" rule enforceable in API
shape, not only by convention.

### 6. Stored-state authorization still belongs in `server-core`

This guide does not change the layering rule that authorization decisions that
depend on stored bucket/object state belong in `server-core`.

If stored state or write reservations are needed to make the authorization
decision, take the necessary core-side lock or reservation at request entry,
make the decision there, and then carry that authorized intent through the
rest of the request.

### 7. Streaming bodies must be gated before body ingestion

Streaming endpoints must perform their request authorization before reading a
meaningful amount of attacker-controlled object body data.

Normal transport buffering and already-read protocol bytes are unavoidable,
but repository code must not intentionally defer write authorization until
after large body buffers, segment promotion, or similar body-driven internal
transitions.

### 8. Multi-request workflows authorize per request

Multipart upload is intentionally different from a single streamed `PutObject`.

These are separate authorization boundaries:

- `CreateMultipartUpload`
- each `UploadPart`
- `UploadPartCopy`
- `CompleteMultipartUpload`
- `AbortMultipartUpload`
- `ListParts`

Each request must authorize independently because AWS exposes them as separate
operations with separate failure points and independent retries.

## Rationale

This policy keeps behavior predictable for clients:

- reads fail before data exposure
- writes fail before substantial body upload if permission is missing
- commit-time failures reflect conflicts or validation, not a second surprise
  permission decision for the same request

It also keeps internal implementation details from leaking into externally
visible semantics. A streamed write may need several internal phases for
storage reasons, but that should not change when the client is considered
authorized for the request. For multi-phase single-request writes, the
authorized-token pattern makes that invariant explicit in the type and helper
design instead of relying on every caller to manually thread the same
auth-sensitive inputs through each phase.

## PR Checklist

If a change touches an object or bucket read/write path, check:

1. What is the external request boundary for this operation?
2. Where is authorization decided for that external request?
3. Does any internal phase re-authorize the same request?
4. For streaming writes, does authorization happen before meaningful body
   ingestion?
5. Are commit-time rejections limited to conflicts, validation, and stored
   state that must be checked at commit?
6. For a multi-phase single-request write, does a bound authorized token carry
   the auth-sensitive request state across phases?
7. Do later internal phases avoid taking a second caller-controlled copy of
   bucket/key/requester/ACL/tag/object-lock state?
8. If the path is multipart or another multi-request workflow, are the
   authorization boundaries aligned with the external API rather than internal
   helpers?
