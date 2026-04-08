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

### 3. Single-request writes authorize once at request entry

A single client write request must authorize once before meaningful body
ingestion or durable side effects begin.

This includes:

- normal buffered writes
- streaming `PutObject`
- copy destination writes
- any future streamed single-request write path

The purpose is to avoid late `AccessDenied` responses after substantial upload
work for a request that was already structurally valid and authenticated.

### 4. Finalize and commit phases do not re-authorize the same request

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

### 5. Stored-state authorization still belongs in `server-core`

This guide does not change the layering rule that authorization decisions that
depend on stored bucket/object state belong in `server-core`.

If stored state or write reservations are needed to make the authorization
decision, take the necessary core-side lock or reservation at request entry,
make the decision there, and then carry that authorized intent through the
rest of the request.

### 6. Streaming bodies must be gated before body ingestion

Streaming endpoints must perform their request authorization before reading a
meaningful amount of attacker-controlled object body data.

Normal transport buffering and already-read protocol bytes are unavoidable,
but repository code must not intentionally defer write authorization until
after large body buffers, segment promotion, or similar body-driven internal
transitions.

### 7. Multi-request workflows authorize per request

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
authorized for the request.

## PR Checklist

If a change touches an object or bucket read/write path, check:

1. What is the external request boundary for this operation?
2. Where is authorization decided for that external request?
3. Does any internal phase re-authorize the same request?
4. For streaming writes, does authorization happen before meaningful body
   ingestion?
5. Are commit-time rejections limited to conflicts, validation, and stored
   state that must be checked at commit?
6. If the path is multipart or another multi-request workflow, are the
   authorization boundaries aligned with the external API rather than internal
   helpers?
