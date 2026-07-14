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
- [ ] Cover missing, empty, duplicate, percent-encoded, and wrong-key upload IDs
  across the four operations, including exact error fields and precedence.

### 2. Completion validation and atomic publication

- [x] Prove `InvalidPart` leaves the upload and parts usable, publishes no object,
  and permits a corrected completion.
- [x] Prove rejected completion over an existing object leaves the old ETag and
  bytes visible until a valid completion atomically replaces it.
- [ ] Apply the same state assertions to `InvalidPartOrder`, `EntityTooSmall`,
  checksum failures, expected-size failures, conditional failures, and malformed
  completion bodies.
- [ ] Determine AWS precedence when several completion errors coexist; include
  upload existence, XML shape, part order, missing part, ETag, part size,
  checksum, condition, and expected object size.
- [ ] Probe retries after successful completion and after abort, including object
  overwrite/delete and bucket delete/recreate histories.

### 3. Part upload and copy boundaries

- [ ] Pin part-number parsing and limits at missing, empty, nonnumeric, signed,
  zero, 1, 10,000, 10,001, duplicate, and overflow values for UploadPart and
  UploadPartCopy.
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
