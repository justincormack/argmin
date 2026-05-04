## Authz Protected State Capabilities Plan

Status: in progress

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

- `UploadPart`
- `UploadPartCopy`
- `CompleteMultipartUpload`
- `AbortMultipartUpload`
- `ListParts`

Questions to lock down against AWS:

- when an unrelated caller supplies a real upload ID, should the response be
  `AccessDenied` or `NoSuchUpload`?
- when an unrelated caller supplies a missing upload ID, should the response be
  the same as for a real hidden upload?
- do SSE-C, checksum, completed-upload, or invalid-part validation errors ever
  take priority over the hidden-upload denial?

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
