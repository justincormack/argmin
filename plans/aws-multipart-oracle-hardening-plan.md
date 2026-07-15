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
- [ ] Cross invalid part numbers/ranges/checksums with invalid upload IDs and
  unauthorized principals to establish error precedence and non-publication.
- [ ] Verify failed or interrupted UploadPart and UploadPartCopy requests neither
  replace a prior valid part nor become visible in ListParts/completion.

### 4. Listing and marker behavior

- [ ] Expand ListParts limits, markers, ordering, overwritten-part metadata,
  checksum fields, and active/terminal upload states.
- [ ] Expand ListMultipartUploads ordering and pagination across same-key uploads,
  prefixes, delimiters, encoding, duplicate timestamps, completion, abort, and
  concurrent state changes.
- [ ] Pin malformed and duplicate listing parameters and authorization/error
  precedence with positive list canaries.

### 5. Lifecycle and concurrency

- [ ] Probe complete/abort races, UploadPart/abort races, UploadPart/complete
  races, simultaneous completions, and concurrent uploads to the same key.
- [ ] Assert only AWS-permitted outcomes, plus final object bytes, upload
  visibility, part visibility, and retry behavior for every outcome.
- [ ] Cover versioned buckets, delete markers, conditional completion, multipart
  copy source changes, and object replacement without relying on timing-sensitive
  exact boundaries.

### 6. Cross-feature multipart contracts

- [ ] Inventory and fill oracle gaps for checksums, metadata, tagging, ACL/BOE,
  SSE-C, managed encryption rejection, expected owner, requester pays, object
  lock, and lifecycle abort headers.
- [ ] Keep feature-specific setup in shared fixtures and run identical assertions
  on AWS and local endpoints.

## Completion

- Every added behavior was first observed on AWS and is asserted by endpoint-
  independent `s3-tests` code.
- Negative matrices contain a positive fixture/authorization canary and mutation
  failures assert final visible state.
- Targeted AWS suites and matching local suites pass.
- `cargo nextest run` and
  `cargo clippy --all-targets --all-features -- -D warnings` pass.
