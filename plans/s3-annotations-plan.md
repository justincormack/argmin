# S3 Annotations Implementation Plan

AWS announced S3 Annotations in June 2026. Annotations are mutable, named,
queryable text payloads attached to a specific object version. They are larger
and more numerous than object tags, and should be treated as a separate
object-version payload subsystem rather than as another column on `objects`.

References:

- AWS News Blog: <https://aws.amazon.com/blogs/aws/amazon-s3-annotations-attach-rich-queryable-context-directly-to-your-objects/>
- API reference:
  - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObjectAnnotation.html>
  - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAnnotation.html>
  - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectAnnotations.html>
  - <https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectAnnotation.html>
- User guide:
  <https://docs.aws.amazon.com/AmazonS3/latest/userguide/annotations-overview.html>

## Documented AWS Behavior

The documented object APIs are:

- `PUT /{Key+}?annotation&annotationName=...&versionId=...`
- `GET /{Key+}?annotation&annotationName=...&versionId=...`
- `GET /{Key+}?annotation&annotation-prefix=...&continuation-token=...&max-annotation-results=...&versionId=...`
- `DELETE /{Key+}?annotation&annotationName=...&versionId=...`

Documented limits and validation:

- Up to 1,000 annotations per object version.
- Annotation name is 1 to 512 UTF-8 bytes.
- Names may contain letters, digits, `_`, `.`, and `-`.
- Names cannot start with `aws` or `s3`, case-insensitively.
- Names must not be empty or only whitespace.
- Payload is 1 byte to 1 MiB.
- Payload must be valid UTF-8 text. Binary data must be encoded by the caller.
- `ListObjectAnnotations` has `max-annotation-results` in the range 1 to 1000.
- `annotation-prefix` filters by annotation name prefix.

Documented object/version semantics:

- Annotations attach to one object version.
- Different versions of the same key have independent annotations.
- Creating a new object version does not copy annotations from the previous
  version.
- Adding, updating, or deleting an annotation does not change the parent object
  ETag.
- In non-versioned buckets, object overwrite/delete removes annotations with
  the overwritten/deleted object.
- In versioned buckets, deleting a current object without a version ID creates a
  delete marker but preserves annotations on older object versions.
- Deleting a specific object version deletes all annotations for that version.
- Annotation deletion is permanent. There are no annotation delete markers or
  annotation version history.

Documented copy behavior:

- `CopyObject` copies annotations from the source object by default.
- `x-amz-annotation-directive: COPY` preserves them.
- `x-amz-annotation-directive: EXCLUDE` omits them.
- Multipart copy does not copy annotations as part of a single S3 service
  operation; SDKs can opt in by reading and writing annotations separately after
  completion.

Documented checksum/encryption behavior:

- Annotation checksum algorithm is independent from the parent object's
  checksum algorithm.
- `PutObjectAnnotation` supports the current S3 checksum header set.
- If no checksum is specified, AWS says S3 uses CRC64NVME for the annotation.
- `GetObjectAnnotation` returns stored checksum headers when checksum mode is
  enabled.
- Annotations inherit parent object encryption for SSE-S3/SSE-KMS/DSSE-KMS.
- Objects encrypted with SSE-C cannot have annotations.
- If the parent object has no server-side encryption, annotations are encrypted
  with SSE-S3 by default.

Conditional behavior:

- `x-amz-object-if-match` on `PutObjectAnnotation` and
  `DeleteObjectAnnotation` checks the parent object ETag, not an annotation
  ETag.
- The docs explicitly say there is no conditional write based on the presence,
  absence, or current value of another annotation.

## AWS Oracle Questions

The docs are new and likely incomplete. Add AWS-facing tests before relying on
the documented details:

- Exact error codes and messages for invalid names:
  empty, whitespace-only, too long, reserved prefix, non-allowed punctuation,
  UTF-8 edge cases, and percent-encoded query spellings.
- Whether `annotationName` is case-sensitive.
- Whether `annotation-prefix` uses byte prefix, Unicode scalar prefix, or some
  normalized string behavior.
- Exact behavior for missing `annotationName` versus list mode.
- Exact behavior for empty payload, oversized payload, and non-UTF8 payload.
- Whether `Content-MD5` and `x-amz-checksum-*` errors match existing object
  checksum error codes.
- What `ETag` means for an annotation: likely payload MD5, but must be measured.
- Whether `PutObjectAnnotation` returns XML body in all SDK/raw cases, despite
  HTTP 200.
- Whether `GetObjectAnnotation` supports range headers. The docs do not mention
  ranges, so assume no until tested.
- Whether `HEAD` with `?annotation` is implemented or rejected.
- Conditional behavior for `x-amz-object-if-match`: quoted versus unquoted
  accepted forms, wildcard behavior, comma lists, and mismatch code.
- Versioning behavior:
  current version, explicit version ID, delete marker current version,
  nonexistent version ID, suspended versioning/null version.
- Object Lock governance/compliance interactions for delete annotation.
- SSE-C rejection codes for put/get/list/delete annotation against an SSE-C
  parent object.
- Copy behavior:
  default copy, explicit `COPY`, explicit `EXCLUDE`, invalid directive, source
  version, destination versioning, metadata/tagging directive interaction,
  checksum algorithm interaction, and copying into an SSE-C destination.
- Authorization actions and any condition keys once AWS documents or exposes
  them.

## Storage Design

Do not store annotation payloads inline in the `objects` row, object tags field,
or metadata command payloads. A single object version can have 1 GiB of
annotation payloads. Inlining would bloat SQLite metadata, metadata command
logs, metadata digests, peering transfer, and recovery.

Use a metadata-plus-payload design:

- Add an `object_annotations` metadata table owned by the object metadata PG.
- Key it by `(bucket, key, version_id, annotation_name)`.
- Store metadata only:
  - annotation name
  - payload size
  - annotation ETag
  - checksum algorithm and checksum bytes
  - last modified timestamp
  - encryption type/state needed to read the payload
  - payload placement record, likely one placed payload root per annotation
  - created/updated command identity if needed for replay/debugging
- Store annotation payload bytes through the existing storage payload path,
  protected by storage CRCs and read verification.
- Add annotation payload reclaim rows for overwritten/deleted annotations and
  for object-version deletion.
- Include `object_annotations` and annotation reclaim tables in metadata digest
  coverage.

Payload layout options:

1. Single placed annotation payload record.
   - Simpler.
   - 1 MiB payload fits comfortably within existing object segment size limits.
   - Good first implementation.

2. Reuse object segment tables with an annotation payload kind.
   - Avoids parallel payload tables.
   - Risks overloading object segment semantics and reclaim paths.

The preferred first implementation is a dedicated annotation payload record with
one stored payload per annotation. It should reuse lower-level shard write/read
and integrity helpers, not object-manifest semantics.

## Metadata Commands

Add command-stream-owned mutations:

- `PutObjectAnnotation`
- `DeleteObjectAnnotation`
- Possibly `DeleteObjectAnnotationsForVersion` if object-version deletion needs
  a distinct internal command shape, although normal object deletion can also
  delete annotation rows in the same transaction.

Command construction must:

- Load and authorize the target object version.
- Validate the parent object still matches the authorized snapshot.
- Validate `x-amz-object-if-match` against the parent object ETag.
- Reject SSE-C parent objects for `PutObjectAnnotation`.
- Enforce max annotation count when adding a new name.
- Replace existing annotation metadata atomically when overwriting.
- Enqueue old payload reclaim before or with metadata replacement.
- On delete, remove the annotation row and enqueue its payload reclaim.

The command payload should contain annotation metadata and payload placement
references, not the annotation body.

## HTTP Layer

Add router variants:

- `PutObjectAnnotation { bucket, key }`
- `GetObjectAnnotation { bucket, key }`
- `ListObjectAnnotations { bucket, key }`
- `DeleteObjectAnnotation { bucket, key }`

Routing:

- `?annotation&annotationName=...` plus method selects put/get/delete by method.
- `GET ?annotation` without `annotationName` selects list mode.
- Parse raw query carefully; AWS uses `annotationName` camel case and
  `annotation-prefix` hyphenated lower case.

Request body handling:

- `PutObjectAnnotation` body limit: 1 MiB plus any small framing overhead needed
  by the existing body collector.
- Reject zero-length bodies.
- Reject bodies over 1 MiB before buffering beyond the limit.
- Validate UTF-8 after collecting or during streaming.
- Existing checksum validation helpers should be reused.

Responses:

- `PutObjectAnnotation`: HTTP 200, annotation XML output, `ETag`,
  `x-amz-object-version-id`, checksum headers, encryption header.
- `GetObjectAnnotation`: HTTP 200, raw annotation payload body, `Content-Length`,
  `Last-Modified`, `ETag`, optional checksum headers, encryption header,
  `x-amz-object-version-id`.
- `ListObjectAnnotations`: HTTP 200, XML listing entries with name, size, ETag,
  last modified, checksum algorithm, and replication status if applicable.
- `DeleteObjectAnnotation`: HTTP 204, `x-amz-object-version-id`.

## Coordinator

Add coordinator APIs:

- `put_object_annotation`
- `get_object_annotation`
- `list_object_annotations`
- `delete_object_annotation`

Put path:

1. Authorize bucket/object access and expected bucket owner.
2. Resolve explicit version ID or current version.
3. Reject missing object/delete marker as AWS does.
4. Reject SSE-C parent object.
5. Validate parent ETag condition if present.
6. Validate annotation name and payload.
7. Validate and/or compute annotation checksum. Default should be CRC64NVME if
   AWS behavior confirms docs.
8. Write payload through storage path.
9. Commit annotation metadata via metadata command.
10. Reclaim any replaced payload.

Get path:

1. Authorize read of annotation.
2. Resolve version and annotation metadata.
3. Read payload through storage path with storage CRC verification.
4. Verify any annotation stored checksum before response, or at least when
   checksum mode is enabled. Prefer always verifying on server exit if cheap.
5. Return payload and metadata headers.

List path:

1. Authorize list annotation action.
2. Resolve object version and ensure object exists.
3. Page rows ordered by annotation name.
4. Apply prefix and continuation token.
5. Clamp `max-annotation-results` to 1000 if AWS does.

Delete path:

1. Authorize delete annotation action.
2. Resolve version and parent object state.
3. Enforce object lock/governance bypass behavior.
4. Validate parent ETag condition if present.
5. Remove annotation row and enqueue payload reclaim.

## CopyObject Integration

Add `AnnotationDirective` for `CopyObject`:

- Missing header defaults to `COPY`.
- `COPY` copies annotations from source object version.
- `EXCLUDE` omits them.
- Invalid values should match AWS errors.

Implementation choices:

- For normal `CopyObject`, copy object payload and annotation payloads under one
  high-level operation.
- Because annotations can total 1 GiB, avoid holding all annotation payloads in
  memory. Iterate annotation records and copy each payload independently.
- If an annotation copy fails after the destination object is committed, decide
  whether the entire `CopyObject` can remain atomic. AWS documents single
  operation copy for objects under 5 GiB, so the desired behavior is all-or-none
  for normal `CopyObject`.
- For multipart copy, do not automatically copy annotations in storage. SDKs may
  implement opt-in copying by issuing annotation APIs after completion.

This may require a staged destination object plus staged annotation copies, then
one metadata command that publishes both object and copied annotation records.
If that is too large for the first slice, implement core annotation APIs first
and leave `CopyObject` explicitly unsupported behind AWS-facing failing tests.

## Authorization

Add policy actions:

- `s3:PutObjectAnnotation`
- `s3:GetObjectAnnotation`
- `s3:ListObjectAnnotations`
- `s3:DeleteObjectAnnotation`

Replication-related actions are documented but can be deferred until replication
exists:

- `s3:GetObjectVersionAnnotationForReplication`
- `s3:ReplicateObjectAnnotation`

Open questions:

- Whether AWS exposes annotation-specific condition keys.
- Whether existing object tag policy condition paths interact with annotation
  APIs. The user guide says tags are for IAM/lifecycle filtering, annotations
  are for rich context, so assume no tag-like annotation policy conditions until
  AWS documents otherwise.

## Testing Plan

AWS-facing `s3-tests`:

- Basic put/get/list/delete happy paths.
- Name validation matrix.
- Payload validation matrix: empty, 1 byte, 1 MiB, 1 MiB + 1, non-UTF8.
- Missing object, missing bucket, missing annotation.
- Explicit version ID, current version, delete marker, suspended/null version.
- Parent overwrite does not carry annotations to new version.
- Annotation update does not change parent ETag.
- `x-amz-object-if-match` success/mismatch/parser edge cases.
- Checksum algorithms and malformed checksum cases.
- SSE-C parent object rejection.
- `CopyObject` default copy and `EXCLUDE`.
- List prefix, continuation token, and max result behavior.

Local integration:

- Storage metadata round trips for annotation rows.
- Payload write/read integrity.
- Replacement enqueues reclaim for old payload.
- Delete enqueues reclaim.
- Object version deletion cascades annotation metadata and payload reclaim.
- Metadata digest includes annotation tables.
- Replay/peering transfer preserves annotation metadata and reclaim state.
- Unix storage-node RPC path covers command build/apply and read/list.

Property/stateful tests:

- Random object versioning plus annotation operations.
- Invariant: annotations are scoped to exact version ID.
- Invariant: live overwrite starts with no annotations unless copied by
  `CopyObject`.
- Invariant: annotation mutation never changes parent ETag.
- Invariant: all unreachable annotation payloads are eventually reclaimable.

## Suggested Implementation Order

1. Add AWS-facing discovery tests for API shape, errors, versioning, checksums,
   and copy directive.
2. Add types and validation helpers for annotation names, payload sizes, and
   annotation list limits.
3. Add storage schema and metadata store methods for annotation metadata only.
4. Add payload write/read/reclaim support for single-payload annotations.
5. Add metadata command variants and local command build/apply paths.
6. Add Unix RPC support for annotation metadata command build/list/read.
7. Add coordinator APIs and HTTP routes/responses.
8. Add checksum and encryption behavior.
9. Add version deletion and bucket deletion cleanup coverage.
10. Add `CopyObject` annotation directive and copying semantics.
11. Add S3 Metadata annotation-table configuration only if/when S3 Metadata
    support is otherwise implemented. It should not block object annotation API
    compatibility.

## Initial Scope Recommendation

First implementation should target:

- `PutObjectAnnotation`
- `GetObjectAnnotation`
- `ListObjectAnnotations`
- `DeleteObjectAnnotation`
- version-specific semantics
- checksum validation and response headers
- SSE-C rejection
- object deletion cleanup

Defer:

- annotation table query/index export
- replication actions
- event notifications
- multipart copy SDK-style annotation copy

`CopyObject` default annotation copying is documented core behavior, so it
should not be deferred for a compatibility release unless the feature is clearly
marked incomplete and tests capture the gap.
