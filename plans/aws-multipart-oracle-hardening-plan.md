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
  successful completion while its published object generation remains current,
  but returns `NoSuchUpload` for a changed manifest or after overwrite/delete.
  Abort is idempotent for completed and aborted upload IDs and does not consume
  a completed upload's exact replay. These terminal retries remain valid until
  bucket deletion/recreation, which makes every operation on the old IDs return
  `NoSuchUpload`.
- [ ] Extend the terminal-retry oracle matrix to versioned and suspended
  buckets. Probe a later object version, delete markers, removal of the delete
  marker, deletion of the completed version, and a later multipart completion
  of the same key. Pin whether an exact completion retry remains valid and which
  `versionId` and response metadata AWS replays in every history.
- [ ] Probe terminal completion retries after the initiator's current
  `s3:PutObject` permission is removed and under an explicit deny, with positive
  policy canaries before each transition. Unless AWS proves a terminal-replay
  exception, an authenticated replay must traverse the same current bucket/IAM
  policy authorization as an in-progress completion; possession of a valid
  upload ID and retained initiator/owner claims establishes identity, not
  authorization.
- [ ] Cross malformed completion XML and malformed checksum/expected-size
  headers with a completed upload whose published object has subsequently been
  overwritten or deleted. Pin that the request-layer target preflight treats
  only an AWS-applicable completion replay as existing, so `NoSuchUpload` wins
  before body and completion-header validation once replay eligibility ends.
- [ ] Replace terminal multipart tombstones with a storage model proportional to
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
    Replay succeeds only for the AWS-selected applicable object version and an
    exact manifest match. Both request preflight and full completion must use
    the same replay-eligibility rule, while the full path repeats the lookup to
    close overwrite/delete races.
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
- [ ] Once the tombstone-free implementation matches the shared AWS/local
  matrix, replace the compatibility-guide exception for terminal hidden-resource
  denial with the authenticated-ID and object-scoped replay invariants.

### 3. Part upload and copy boundaries

- [x] Pin UploadPart part-number parsing and limits at missing, empty,
  nonnumeric, signed, zero, 1, 10,000, 10,001, duplicate, and overflow values.
  Pin UploadPartCopy's shared numeric/duplicate rules and its distinct
  no-XML-declaration error shape. Assert the accepted first duplicate is the
  part stored and that missing `partNumber` publishes no object.
- [ ] Pin zero-byte parts, 5 MiB minus one/exactly 5 MiB non-final parts, final
  part exceptions, overwritten parts, and maximum-part completion behavior.
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
