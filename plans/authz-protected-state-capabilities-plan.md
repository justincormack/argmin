## Authz Protected State Capabilities Plan

Status: in progress

Current slice:

- added AWS-pinned coverage for multipart management visibility after abort
- AWS behavior confirmed:
  - the upload initiator sees a just-aborted upload as `NoSuchUpload`
  - a same-account non-initiator with only `s3:PutObject` sees that same
    just-aborted real upload ID as `AccessDenied`
  - an arbitrary never-existing upload ID still returns `NoSuchUpload`
- decision: do not retain unbounded completed/aborted hidden-upload identity
  just to match AWS's terminal `403` versus `404` distinction; document this as
  an explicit AWS compatibility difference
- ListParts now receives an `AuthorizedListParts` capability from authz instead
  of performing the protected upload-management lookup in the operation path
- AbortMultipartUpload now receives an `AuthorizedAbortMultipartUpload`
  capability carrying the authorized in-progress upload snapshot; the storage
  abort helper consumes that snapshot and rejects if the upload changed before
  abort is prepared
- UploadPart stream-session creation now requires authz to return an
  `AuthorizedMultipartUploadRecord`; storage revalidates that token before
  installing the stream session
- UploadPartCopy stream-session creation now consumes the authorized
  multipart-upload snapshot instead of raw bucket/key/upload ID fields
- CompleteMultipartUpload preflight and part snapshots now consume the
  authorized multipart-upload snapshot instead of raw bucket/key/upload ID
  fields
- protected multipart storage helpers now require the nominal
  `AuthorizedMultipartUploadRecord` wrapper rather than accepting a plain
  `MultipartUploadRecord`
- added AWS-pinned multipart abort lifecycle tests:
  - aborting an upload with completed parts makes `ListParts` return
    `NoSuchUpload`
  - abort racing a slow already-started `UploadPart` returns AWS-compatible
    `NoSuchUpload` or success, but must not surface a server `InternalError`
  - the same race after one completed part has already established the upload
    also returned `NoSuchUpload` in the observed AWS run
  - in both slow-upload races, `ListParts` immediately after abort returns
    `NoSuchUpload`, before waiting for the raced `UploadPart` result
- AWS probing found aborted upload hidden identity is not just upload ID syntax
  validation: same-length mutated IDs and real IDs under the wrong key return
  `NoSuchUpload`, while the real aborted ID under the original key keeps
  returning `AccessDenied` to an unauthorized same-account caller for at least
  two minutes
- possible future tightening: encode key-bound validation material into upload
  IDs so terminal hidden-ID checks can be recognized without retaining
  unbounded tombstones

## Goal

Make auth ordering bugs hard to write by requiring typed authorization
capabilities before code can load or use protected state.

The motivating bug was fixed in `cf4cd36c38f93423e604af052b65b643f2249626`:
`TagResource`/`UntagResource` merge handling loaded existing bucket tags before
the caller was authorized for the S3 Control tagging action. That allowed hidden
tag state to influence the error returned to an unauthorized caller.

The general invariant is:

- if state is not visible to a requester, that requester must not be able to
  distinguish errors or validation outcomes that depend on that state
- any storage read that exposes object, upload, bucket subresource, tag,
  policy, lifecycle, lock, encryption, checksum, ACL, or version state must be
  sequenced behind an authorization decision that permits that visibility
- when AWS allows missing-resource discovery only for some requesters, that
  discovery permission must be represented explicitly rather than falling out of
  storage error ordering

## Design Shape

Keep raw request parsing and client-supplied validation separate from protected
state reads.

Introduce narrow capability tokens for protected state access. Existing types
such as `AuthorizedBucketSubresourceGet`, `AuthorizedObjectRead`, and
`AuthorizedPutObjectWrite` are the right pattern. Extend it to areas that still
need stronger structure:

- multipart upload write capability
- multipart upload management capability
- multipart upload lookup/discovery capability
- copy-source read capability
- object metadata/tag/ACL/version read capability where needed

Protected storage helpers should require these capabilities instead of raw
request structs. Operation code should receive either:

- an authorized state snapshot that is already safe to use, or
- an authorized operation token that is the only way to perform the follow-up
  mutation/read

Avoid adding general-purpose accessors that take only bucket/key/upload IDs and
return protected state.

## First Audit Targets

### Multipart Upload State

This is the highest-risk current surface.

Several paths need multipart upload records for initiator, owner, encryption,
checksum configuration, and bucket-policy context. That makes it easy to load
upload state before deciding whether the caller may see that upload at all.

Audit and refactor:

- `UploadPart` (first pass complete for authorized-upload session creation)
- `UploadPartCopy` (first pass complete for authorized-upload session creation)
- `CompleteMultipartUpload` (first pass complete for preflight and part
  snapshots)
- `AbortMultipartUpload` (first pass complete for active-upload visibility and
  authorized-upload abort)
- `ListParts` (first pass complete for active-upload visibility and
  authorized-upload listing)

Questions to lock down against AWS:

- when an unrelated caller supplies a real upload ID, should the response be
  `AccessDenied` or `NoSuchUpload`?
- when an unrelated caller supplies a missing upload ID, should the response be
  the same as for a real hidden upload?
- do SSE-C, checksum, completed-upload, or invalid-part validation errors ever
  take priority over the hidden-upload denial?

### Multipart Abort And In-Flight Parts

This was found while tightening multipart management authz, but it is a
separate conformance issue from protected-state visibility.

AWS documents a looser lifecycle: after abort, in-flight part uploads might
still succeed, and callers may need to abort repeatedly and use `ListParts` to
verify that all part storage has gone. The AWS behavior we pinned for slow
UploadPart races returned `NoSuchUpload` once abort won, and `ListParts`
immediately after abort also returned `NoSuchUpload`.

The compatibility target is therefore narrower than retaining every terminal
upload ID:

- active uploads preserve AWS-compatible `403` versus `404` auth behavior
- completed and aborted uploads match operation-visible behavior for authorized
  callers
- raced `UploadPart` must never surface `500 InternalError`; return
  `NoSuchUpload` if abort won
- terminal hidden upload IDs may return `NoSuchUpload` rather than AWS's
  `AccessDenied`, because `404` is less revealing and avoids unbounded
  terminal-ID retention

Refactor target:

- reject new UploadPart/UploadPartCopy stream sessions once an upload is
  aborting
- make already-started part commits that lose the abort race return
  `NoSuchUpload` rather than leaking storage/internal errors
- keep the active-upload auth path explicit enough to preserve `403` versus
  `404` for uploads that are still in progress
- avoid unbounded completed/aborted hidden-ID tombstones
- use durable reclaim-style metadata for any physical part shard deletion that
  cannot be completed synchronously

AWS-facing tests should pin:

- abort with no parts
- abort with completed parts and `ListParts` immediately after (covered)
- abort racing an already-started part upload (covered for a slow streaming
  body; AWS returned `NoSuchUpload` in the observed run; `ListParts` immediately
  after abort also returns `NoSuchUpload`)
- abort racing an already-started second part after one part has completed
  (covered; AWS returned `NoSuchUpload` in the observed run; `ListParts`
  immediately after abort also returns `NoSuchUpload`)
- repeated abort after a raced part finishes
- authorization/error precedence for hidden aborting uploads versus missing
  uploads

### Object Read Snapshots

Object read authorization already has better shape through
`MissingObjectDiscovery` and `load_object_read_snapshot_if`. Preserve that
model and look for bypasses where code loads object state directly before
authorizing:

- object tags
- object ACLs
- object lock retention/legal hold
- object attributes
- copy source reads
- conditional reads and writes that depend on ETag/version state

### Bucket Subresources

Bucket subresources mostly use `AuthorizedBucketSubresource*` wrappers. Audit
for direct loads or merge logic that can still use hidden state before action
authorization:

- bucket tags and S3 Control tag merge paths
- bucket policy status/public policy analysis
- lifecycle/encryption/object-lock/public-access-block reads used by later
  validation

## Implementation Steps

1. Inventory protected storage calls.
   Classify direct `storage_node.load_*`, `lookup_*`, `get_*`, and subresource
   reads by whether they are public, owner-only, policy-visible, or internal
   background maintenance.

2. Add capability-specific protected access helpers.
   Start with multipart upload state because it has the clearest remaining
   risk. The helper should encode whether the caller may discover missing
   uploads and whether the caller may manage or write to an existing upload.

3. Move operation code onto authorized helpers.
   Operation modules should stop loading protected state directly. Authz code
   can still load state when the state is needed to complete the authorization
   decision, but the resulting error mapping must be explicit.

4. Add guard tests.
   Add a lightweight source-level test or lint-style test that rejects new
   direct protected storage reads outside authz/protected-access modules and
   explicitly allowed internal maintenance code.

5. Add AWS-pinned oracle tests.
   For each refactored family, test both real-hidden and missing resources
   from an unauthorized caller. Include cases where hidden state would otherwise
   trigger a more specific error, such as invalid merged tags, SSE-C key
   mismatch, checksum mismatch, completed upload state, delete marker state, or
   object-lock state.

## Acceptance Criteria

- Protected state loads needed for auth-sensitive operations require typed
  capabilities or live inside the authz/protected-access layer.
- Unauthorized callers cannot distinguish hidden existing state from hidden
  missing state except where AWS explicitly permits discovery.
- More specific validation errors do not take priority over denial when they
  depend on state the caller is not authorized to observe.
- AWS-facing s3-tests pin the expected precedence for multipart upload state
  and at least one object-state and bucket-subresource example.
- The source-level guard test prevents new direct protected storage reads from
  being added casually.
