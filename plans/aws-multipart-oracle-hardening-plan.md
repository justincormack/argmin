# AWS Multipart Oracle Hardening Plan

## Goal

Harden multipart S3 behavior by deriving ambiguous request, lifecycle, and error
precedence rules from AWS and pinning them in tests that run unchanged against
AWS and the local server.

AWS responses are the authority. Documentation may suggest probes, but does not
decide expected behavior. Tests must not inspect the endpoint or select different
assertions for AWS and local execution.

## Workflow

For each matrix:

1. Construct fixtures with positive canaries so an error cannot pass because the
   credentials, bucket, upload, or part setup is broken.
2. Probe AWS with raw signed requests when the SDK cannot preserve the required
   malformed or duplicate wire shape.
3. Replace diagnostics with exact shared assertions, then run the same test
   locally.
4. If local behavior differs, understand the AWS ordering rule before changing
   implementation code. Assert both response behavior and externally visible
   state after failed mutations.

## Coverage

### 1. Upload identity, authorization, and parsing precedence

- [x] Pin the exact overlong nonexistent upload-ID response for the primary
  principal across UploadPart, CompleteMultipartUpload, ListParts, and
  AbortMultipartUpload.
- [x] Cross active and overlong nonexistent upload IDs with the alternate
  principal for all four operations. Require `AccessDenied` for the active ID
  before accepting `NoSuchUpload` for the malformed ID, and prove denied writes
  stored no parts.
- [x] Pin CompleteMultipartUpload ordering across active/nonexistent upload IDs,
  well-formed/malformed XML, and primary/alternate principals.
- [x] Cover missing/empty, duplicate, percent-encoded, and wrong-key upload IDs
  across UploadPart, CompleteMultipartUpload, ListParts, and
  AbortMultipartUpload, including exact error fields, first-value precedence,
  key binding, and final upload/part/object state. An absent `uploadId` only
  identifies UploadPart when `partNumber` is present; absent IDs on POST, GET,
  and DELETE select the corresponding non-multipart object operation.

### 2. Completion validation and atomic publication

- [x] Prove `InvalidPart` leaves the upload and parts usable, publishes no object,
  and permits a corrected completion.
- [x] Prove rejected completion over an existing object leaves the old ETag and
  bytes visible until a valid completion atomically replaces it.
- [x] Apply the same state assertions to `InvalidPartOrder`, `EntityTooSmall`,
  checksum failures, expected-size failures, conditional failures, and malformed
  completion bodies. Raw response-shape tests explicitly accept AWS's documented
  CompleteMultipartUpload behavior where a processing failure may use either
  its ordinary error status or an error body embedded in HTTP 200. SDK-based
  tests already handle both forms; the local server uses ordinary error statuses.
- [x] Determine AWS precedence when several completion errors coexist; include
  upload existence, XML shape, part order, missing part, ETag, part size,
  checksum, condition, and expected object size. The oracle matrix also pins
  header-shape validation and proves that aggregate-checksum precedence is not
  a total linear order: it beats a missing part, while an ETag mismatch beats
  the aggregate checksum. AWS's repeatable `InternalError` for a valid,
  checksum-configured but non-consecutive part list is covered under the
  documented compatibility policy that permits Argmin's `InvalidRequest`.
- [x] Probe retries after successful completion and after abort, including object
  overwrite/delete and bucket delete/recreate histories. AWS replays the exact
  successful completion while its published object version remains retained,
  but returns `NoSuchUpload` for a changed manifest or after an unversioned
  overwrite/delete.
  Abort is idempotent for completed and aborted upload IDs and does not consume
  a completed upload's exact replay. These terminal retries remain valid until
  bucket deletion/recreation, which makes every operation on the old IDs return
  `NoSuchUpload`.
- [x] Extend the terminal-retry oracle matrix to versioned buckets. A completed
  version remains the exact-replay target across later versions and delete
  markers, including removal of the delete marker, and ceases to replay only
  when that completed version is explicitly deleted. The replay returns the
  original ETag and `x-amz-version-id`.
- [x] Probe otherwise exact terminal completion retries with changed request
  conditions, aggregate-checksum headers, and `x-amz-mp-object-size`. AWS uses
  only the completion manifest as replay identity. It ignores syntactically
  valid matching or mismatching `If-Match`, aggregate-checksum, and expected-size
  values, and ignores `If-None-Match: *`; malformed values and unsupported
  specific `If-None-Match` values fail before replay. Successful terminal
  replays return the original ETag and version ID but omit checksum result
  fields, even when the original completion supplied aggregate-checksum and
  expected-size claims. The shared AWS/local matrix pins these response shapes
  and proves that neither object data, object versions, nor upload state mutate.
- [x] Probe the corresponding terminal-retry histories in a suspended bucket.
  A retained numbered completion continues to replay through suspension, null
  writes, a null delete marker and its removal, and a later null multipart
  completion. Deleting that numbered version ends only its replay. A completed
  null version replays only while that exact null object row remains: a later
  PUT, null delete marker, explicit null-version deletion, or later multipart
  completion ends the old replay, and removing the delete marker does not
  resurrect it. The shared matrix also pins the suspended delete marker's
  `x-amz-version-id: null` response, the same header on explicit null-version
  deletion, and the replacement/reclamation of the old null payload.
  Repeating an unversioned delete while that null delete marker is current
  remains immediately successful and returns the null marker response; local
  and Unix-client storage regressions pin the no-payload command path.
- [x] Probe terminal completion retries after the initiator's current
  `s3:PutObject` permission is removed and under an explicit deny, with positive
  policy canaries before each transition. AWS applies the current policy: an
  explicit deny rejects the replay, and removing it restores the replay. AWS
  also permits an exact replay by a same-account non-initiator that is currently
  granted `s3:PutObject`, in both ACL and bucket-owner-enforced buckets.
  Authenticated upload identity therefore establishes the target but does not
  replace current completion authorization.
- [x] Cross malformed completion XML and malformed expected-size headers with a
  completed upload whose published object has subsequently been
  overwritten. AWS still reports `MalformedXML` or `InvalidRequest` before
  `NoSuchUpload`: request-layer preflight recognizes an authenticated issued ID
  independently of whether a retained completion replay still exists. The full
  valid completion path performs the replay lookup and returns `NoSuchUpload`
  when no retained replay is applicable.
- [x] Replace terminal multipart tombstones with a storage model proportional to
  active uploads and live object versions, not historical multipart activity.
  Per-bucket retention limits are not acceptable: multipart is the ordinary
  object-write path, so even a fixed limit such as 10,000 records per bucket
  creates a large permanent metadata tax across many buckets.
  - Generate opaque authenticated upload IDs bound to the bucket, key, and
    bucket-incarnation generation. Include any initiator/owner claims required
    by the AWS authorization oracle, and define durable signing-key lifecycle
    and restart behavior. A mutated ID, an ID used with another key, or an ID
    from a deleted bucket incarnation must fail validation.
  - Keep exact-completion replay data with the object version produced by the
    completion: the authenticated upload identity, completion-manifest digest,
    and response fields not otherwise derivable from the object metadata.
    Replay succeeds only while the completed object version is retained and the
    manifest matches exactly. Request preflight authenticates the issued ID
    without requiring replay state, preserving AWS's XML/header validation
    precedence; the full path separately loads the retained replay and repeats
    the lookup to close deletion races.
  - For an authenticated issued ID with no applicable completion replay,
    CompleteMultipartUpload returns `NoSuchUpload`; AbortMultipartUpload remains
    idempotently successful after either completion or abort without retaining
    a terminal row. Perform authorization after authenticating the key-bound ID
    so the existing hidden-resource `AccessDenied` behavior remains possible.
  - Remove the existing bounded completed-upload tombstone table/path as well as
    the uncommitted completed/aborted extension. Add storage tests proving that
    completion, abort, overwrite, and deletion do not accumulate historical
    terminal rows, plus restart and bucket delete/recreate tests for upload-ID
    authentication and replay state. No pruning or cleanup mechanism may be
    required to maintain the storage bound.
  - Preserve the replicated cross-PG completion dependency with
    `AdvanceMultipartCompletionBarrier`. It advances one fixed-size scalar on
    the bucket row before object-PG publication and retains no upload identity
    or terminal history. Because the command contains no reservation identity,
    a retry first drains any barrier left pending by an earlier reservation and
    then advances a fresh barrier validated against its current reservation.
- [x] Once the tombstone-free implementation matches the shared AWS/local
  matrix, replace the compatibility-guide exception for terminal hidden-resource
  denial with the authenticated-ID and object-scoped replay invariants.

### 3. Part upload and copy boundaries

- [x] Pin UploadPart part-number parsing and limits at missing, empty,
  nonnumeric, signed, zero, 1, 10,000, 10,001, duplicate, and overflow values.
  Pin UploadPartCopy's shared numeric/duplicate rules and its distinct
  no-XML-declaration error shape. Assert the accepted first duplicate is the
  part stored and that missing `partNumber` publishes no object.
- [x] Pin zero-byte parts, 5 MiB minus one/exactly 5 MiB non-final parts, final
  part exceptions, overwritten parts, and maximum-part completion behavior.
  AWS applies the minimum only to the selected non-final parts and uses the
  latest upload for an overwritten part number. A lone part numbered 10,000
  completes successfully. A 10,000-entry manifest proceeds to part lookup,
  while 10,001 entries fail first with AWS's exact `InvalidArgument` response;
  the count boundary is tested without transferring 10,000 full-size parts.
- [x] Cross invalid part numbers/ranges/checksums with invalid upload IDs and
  unauthorized principals to establish error precedence and non-publication.
  AWS first resolves whether the upload ID names an active upload, ahead of
  malformed Content-MD5/checksum and incomplete SSE-C headers. For an active
  upload, syntactically or semantically invalid part numbers and malformed copy
  ranges precede current-policy authorization; Content-MD5 mismatch and copy
  ranges beyond the source size follow authorization. The shared matrix
  includes a successful copy canary and proves every rejected request leaves
  the target upload empty and publishes no object.
- [x] Verify failed UploadPart and UploadPartCopy replacements, plus an
  interrupted UploadPart request body, neither replace a prior valid part nor
  become visible in ListParts/completion. AWS and local retain the original
  part's ETag and size after a checksum failure, a mid-body transport failure,
  and an UploadPartCopy source-condition failure; completion with that original
  ETag publishes its original bytes. UploadPartCopy has no request body to
  interrupt. Disconnecting while awaiting its response does not establish that
  the server-side copy failed, so permitted raced outcomes belong to the
  concurrency matrix below rather than this failed-request invariant.

### 4. Listing and marker behavior

- [x] Expand ListParts limits, markers, ordering, overwritten-part metadata,
  checksum fields, and active/terminal upload states.
- [x] Expand ListMultipartUploads ordering and pagination across same-key uploads,
  prefixes, delimiters, encoding, duplicate timestamps, completion, abort, and
  concurrent state changes.
  Same-key creation ordering, two-marker continuation, non-truncated next
  markers, prefix/encoding behavior, duplicate-timestamp tie-breaking, and
  completion/abort removal and delimiter/common-prefix pagination are covered.
  The lifecycle matrix pins continuation after the exact marker is aborted,
  creation before and after that stale marker, creation with the same key, and
  completion/abort removal between pages. Authenticated upload IDs carry their
  durable creation position, consisting of the PG metadata command's cluster
  epoch and within-epoch log index, so the stale marker resumes at its former
  position across epoch transitions without retaining a per-key counter or
  terminal upload metadata. The minimal
  abort/recreate case proves a replacement remains visible even when no other
  same-key upload preserves the old object generation.
- [x] Pin malformed and duplicate listing parameters and authorization/error
  precedence with positive list canaries. ListParts and
  ListMultipartUploads treat empty numeric parameters as omitted, accept an
  explicit plus sign, reject values outside the signed 32-bit range, clamp
  valid limits above 1,000, and select the first duplicate value. Malformed
  `max-parts` precedes `part-number-marker`, which precedes upload lookup;
  malformed `max-uploads` precedes `encoding-type`, which precedes an
  effective `upload-id-marker`. These validation failures precede current
  authorization. ListMultipartUploads ignores `upload-id-marker` unless a
  nonempty `key-marker` is also present, and first-value selection is pinned
  for key markers, prefixes, and delimiters.

### 5. Lifecycle and concurrency

- [x] Probe complete/abort races, UploadPart/abort races, UploadPart/complete
  races, simultaneous completions, and concurrent uploads to the same key.
- [x] Assert only AWS-permitted outcomes, plus final object bytes, upload
  visibility, part visibility, and retry behavior for every basic outcome.
  Complete versus abort is serializable. When completion wins, the raced abort
  may either succeed or return `NoSuchUpload`; when abort wins, it succeeds and
  completion returns `NoSuchUpload`. Sequential abort after completion remains
  idempotently successful. Simultaneous identical completions are idempotent
  and all return the same result. Different manifests for one
  upload may both return success, although only the published manifest remains
  replayable; a competing call may instead return `NoSuchUpload`. Distinct
  uploads to one key both complete successfully, with last-writer object state
  and replay retained only for the current unversioned object. UploadPart
  replacement versus completion is serializable: either completion publishes
  the original part and replacement returns `NoSuchUpload`, or replacement is
  retained and completion of the old ETag returns `InvalidPart`.
  Deterministic coordinator regressions pin completion races after
  authorization, after snapshot lookup, and before commit. Completion restarts
  terminal-transition races once and uses a separate bounded stale-snapshot
  retry budget so one streaming replacement can settle without leaking an
  `InternalError`. Exhausting that budget under sustained valid same-ETag part
  replacement returns retryable `OperationAborted`; a deterministic regression
  proves that it publishes no object and leaves the upload and selected part
  intact. Multipart management lookup drains a concurrent durable terminal
  command before classification, preserving AWS's idempotent abort result when
  completion wins the race.
- [x] Cover versioned buckets, delete markers, conditional completion, and object
  replacement without relying on timing-sensitive exact boundaries. Concurrent
  completions of distinct uploads in a versioned bucket both publish retained,
  independently replayable versions. A completion racing a versioned delete
  retains both the completed version and delete marker; only their latest state
  varies. Conditional completion is also bound to the current-object identity
  observed at multipart initiation: normal conditions are evaluated against the
  current object first, but a condition that would otherwise pass returns
  `409 ConditionalRequestConflict` when an intervening replacement or deletion
  changed that identity. The old upload remains listable with its parts, while a
  newly initiated upload can complete under the new current-object condition.
  In a versioned bucket, deleting an intervening replacement so the exact
  initiation-time version becomes current again permits conditional completion;
  the rule is current identity, not merely whether an intervening write occurred.
  In a suspended bucket, replacing a current null delete marker with another
  null marker produces a different identity and returns the same conflict. Local
  identity therefore includes the marker's durable write sequence rather than
  relying on its nullable version ID and millisecond timestamp.
- [x] Cover multipart copy source changes without relying on timing-sensitive
  exact boundaries. UploadPartCopy racing an unversioned source replacement
  copies exactly one complete source version: its result ETag, listed part, and
  completed destination bytes all identify either the old or new object, never
  a mixture. Racing source deletion either copies the old version successfully
  or returns `NoSuchKey` without storing a part. In the rejected branch the
  upload remains usable by a successful copy retry after the source is restored.

### 6. Cross-feature multipart contracts

- [ ] Inventory and fill oracle gaps for checksums, metadata, tagging, ACL/BOE,
  SSE-C, managed encryption rejection, expected owner, requester pays, object
  lock, and lifecycle abort headers.
- [ ] Keep feature-specific setup in shared fixtures and run identical assertions
  on AWS and local endpoints.

### 7. Conditional operations beyond multipart completion

Multipart completion exposed a broader class of conditional-operation risks:
the condition, authorization, and mutation must refer to a coherent object
state, and contention responses must be derived from AWS rather than inferred
from documentation. Extend the oracle pass to PutObject, CopyObject,
UploadPartCopy, DeleteObject, DeleteObjects, GetObject, and HeadObject. Keep all
public tests endpoint-independent and do not retry the first raced conditional
request, because retrying `OperationAborted` would hide AWS's original 409/412
choice.

- [x] Fill the static CopyObject destination matrix. Pin `If-None-Match: *`
  against missing and existing destinations, then cover `If-Match` and
  `If-None-Match: *` across current live objects, current delete markers,
  enabled versioning, and suspended null versions. Every rejection must prove
  that the destination bytes, ETag, versions, and source remain unchanged.
  The shared AWS/local cases now pin success for a missing destination and
  `412 PreconditionFailed` with unchanged ETag and bytes for an existing
  destination. In a versioned bucket, rejection over a current live version
  creates no new version; a current delete marker makes destination `If-Match`
  return `404 NoSuchKey`, while `If-None-Match: *` succeeds and publishes a new
  current version without removing the retained object version or marker.
  Matching `If-Match` over a versioned live destination publishes a distinct
  latest version and retains the exact prior version and bytes. In a suspended
  bucket, rejected conditions preserve the current null live object or null
  delete marker and the older numbered version. `If-None-Match: *` over the
  null marker replaces it with a null live copy; matching `If-Match` then
  replaces that null live row without accumulating null history, while the
  numbered version remains readable. AWS omits `x-amz-version-id` from the
  suspended PutObject setup response but returns `x-amz-version-id: null` from
  suspended CopyObject and every GetObject/HeadObject variant, including range
  and `partNumber` requests. The local response paths now carry the bucket
  versioning state so they can match this operation-specific distinction. The
  same oracle also pins that `partNumber=1` over a non-multipart object omits
  `x-amz-mp-parts-count`; multipart objects continue to return their count.
- [x] Fill CopyObject and UploadPartCopy source-condition coverage. Cross the
  four source conditional families individually and in AWS's combined-header
  precedence pairs, including malformed dates, missing/current-delete-marker
  sources, explicit source versions, and source replacement or deletion while
  a copy is in progress. A success must copy one coherent source snapshot; a
  failure must publish no destination or part. The combined ETag/date pairs are
  now pinned for both operations: a present `If-Match` controls independently
  of `If-Unmodified-Since`, and a present `If-None-Match` controls independently
  of `If-Modified-Since`. The full truth table proves successful copies contain
  the source bytes and ETag, while each 412 publishes neither a destination nor
  a multipart part. A second cross-operation matrix pins source resolution
  ahead of all four condition families: missing and current-delete-marker
  sources return `404 NoSuchKey`, an explicitly selected delete-marker version
  returns `400 InvalidRequest`, and conditions on an explicitly selected live
  version use that version's ETag and timestamp rather than the current source
  state. Successful explicit-version copies return
  `x-amz-copy-source-version-id`; local CopyObject and UploadPartCopy responses
  now preserve that selected source identity. A separate source-version header
  oracle pins the complete rule for both operations: an implicit current
  numbered version returns its ID; an implicit null version in a never-versioned
  or suspended bucket omits the header; and explicitly selecting
  `versionId=null` returns `null` in both bucket states. Exact source version and
  marker history is unchanged, and every rejection publishes neither a
  destination nor a part. Both malformed source date headers are ignored by
  both operations, with the successful destination or part state verified.
  AWS-facing CopyObject probes establish the permitted replacement and
  deletion orderings: replacement publishes exactly one complete old or new
  source snapshot, while deletion either publishes the complete old snapshot
  or returns `404 NoSuchKey` without a destination. These match the equivalent
  UploadPartCopy source-race outcomes in the multipart concurrency matrix. The
  staggered AWS probes do not themselves prove that the operations overlap.
  Deterministic coordinator regressions now pause CopyObject after its source
  snapshot, complete replacement or deletion, and then require the old body,
  ETag, content type, and user metadata on resume. The overwrite case also
  verifies that the current source contains the distinct replacement state.
- [ ] Probe conditional PutObject contention for both the direct and streamed
  paths, including aws-chunked requests. Cover simultaneous
  `If-None-Match: *` creates, competing `If-Match` overwrites, intervening
  different-ETag replacement, and same-ETag replacement with different
  ownership, ACLs, or existing tags. Record the first AWS response without the
  OperationAborted retry helper, then assert permitted 409/412 outcomes, final
  object bytes and metadata, retry behavior, and absence of partial writes.
- [ ] Probe CopyObject destination contention with the same destination-state
  and authorization matrix. Copy's source read creates a naturally longer
  interval between destination authorization and publication, so explicitly
  establish whether AWS binds object-dependent authorization to the initial
  destination state, the commit state, or another linearization point before
  changing the local capability model.
- [ ] Probe DeleteObject and DeleteObjects races against same-ETag and
  different-ETag replacement, current delete-marker insertion, enabled
  versioning, and suspended null replacement. Pin per-entry DeleteObjects
  results and final state. The local implementation already re-runs current
  object authorization and the ETag condition inside one storage callback;
  deterministic regressions must prove that invariant under replacement.
- [ ] Add the remaining conditional input and precedence matrix for operations
  that accept ETag lists or dates: weak and unquoted tags, wildcard/list forms,
  duplicate headers, malformed dates, missing objects, authorization failures,
  and GET/HEAD combined-condition ordering. Only retain parser behavior after
  it has been observed on AWS.
- [ ] Bound streamed PutObject/CopyObject stale-finalization retries. Direct PUT
  already has a retry/deadline budget, while the streamed finalizer currently
  loops on `StaleStreamFinalizeSnapshot`. After the public contention oracle is
  known, return the appropriate retryable S3 error on exhaustion and add a
  deterministic regression proving no publication, preserved staged cleanup,
  and no `InternalError` or indefinitely held request/reservation.

## Completion

- Every added behavior was first observed on AWS and is asserted by endpoint-
  independent `s3-tests` code.
- Negative matrices contain a positive fixture/authorization canary and mutation
  failures assert final visible state.
- Targeted AWS suites and matching local suites pass.
- `cargo nextest run` and
  `cargo clippy --all-targets --all-features -- -D warnings` pass.
