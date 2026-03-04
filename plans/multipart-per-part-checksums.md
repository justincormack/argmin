# Multipart Per-Part Checksums Plan

## Context

We have multipart upload core support, but these tests are still ignored:

- `crates/s3-tests/tests/checksums.rs`
  - `test_multipart_checksum_sha256`
  - `test_multipart_use_cksum_helper_sha256`
  - `test_multipart_use_cksum_helper_crc64nvme`
  - `test_multipart_use_cksum_helper_crc32`
  - `test_multipart_use_cksum_helper_crc32c`
  - `test_multipart_use_cksum_helper_sha1`
- `crates/s3-tests/tests/object_attributes.rs`
  - `test_get_multipart_checksum_object_attributes`

Goal: implement AWS-compatible multipart checksum behavior sufficient to enable
all 7 tests above.

## AWS Interface Surface (what to implement)

Endpoints and fields relevant to multipart checksums:

1. `CreateMultipartUpload`
- request headers: `x-amz-checksum-algorithm`, optional `x-amz-checksum-type`
- response fields: `ChecksumAlgorithm`, optional `ChecksumType`

2. `UploadPart`
- request: `ChecksumAlgorithm` and one checksum header/value for that algorithm
- response headers: checksum header for the uploaded part

3. `CompleteMultipartUpload`
- request headers: object checksum header/value, optional `x-amz-checksum-type`
- request XML: each `<Part>` may include checksum field
  (`ChecksumCRC32|ChecksumCRC32C|ChecksumCRC64NVME|ChecksumSHA1|ChecksumSHA256`)
- response fields: object checksum and `ChecksumType`

4. `ListParts`
- response fields: `ChecksumAlgorithm`, `ChecksumType`
- each `<Part>` can include per-part checksum field

5. `HeadObject` and `GetObject` (with `ChecksumMode=ENABLED`)
- return object checksum and `x-amz-checksum-type`

6. `GetObjectAttributes`
- `Checksum` includes object checksum and `ChecksumType`
- `ObjectParts.Parts[]` includes per-part checksums when available

## Scope

In scope for this slice:

1. Algorithms: `SHA256`, `SHA1`, `CRC32`, `CRC32C`, `CRC64NVME`.
2. Checksum types:
- `COMPOSITE` for SHA algorithms.
- `FULL_OBJECT` for CRC algorithms.
3. Enabling the 7 currently-ignored tests above.

Out of scope:

1. `GET ?partNumber=` behavior (already tracked under separate ignored tests).
2. SSE-C/SSE-KMS checksum interactions.
3. Backfill migration for already-existing on-disk PG DBs.

## Data Model Changes

No new dependency required.

Add multipart checksum metadata in storage:

1. `multipart_uploads`
- `checksum_algorithm` (`TEXT` or compact enum `INTEGER`, nullable)
- `checksum_type` (`TEXT`/enum, nullable)

2. `multipart_parts`
- `checksum` (`BLOB`, nullable) for raw digest bytes

3. `object_parts`
- `checksum` (`BLOB`, nullable) for committed manifest parts

Notes:

1. Keep existing `etag`/`etag_kind` unchanged (ETag remains separate from checksum).
2. Store raw bytes in DB; encode/decode Base64 only at HTTP boundary.

## Execution Plan

### Step 1: Add checksum primitives and storage fields

Files:

- `crates/storage/src/schema.rs`
- `crates/storage/src/types.rs`
- `crates/storage/src/pg_store.rs`
- `crates/server/src/coordinator.rs` (types only)

Tasks:

1. Add `ChecksumAlgorithm` and `ChecksumType` internal enums.
2. Extend multipart upload/part/object-part records with checksum fields.
3. Thread new fields through `PgStore` row mapping and insert/upsert APIs.

Acceptance:

1. Storage unit tests cover round-trip persistence for new fields.

### Step 1b: Add CRC32/CRC32C combine primitives (no full-object read at complete)

Files:

- `crates/server/src/coordinator.rs`
- `crates/server/src/http/mod.rs` (if shared helpers are placed here)
- `crates/crc64/src/lib.rs` (reference for API shape only)
- `crates/server/src` tests (unit tests for combine correctness)

Tasks:

1. Implement CRC32 and CRC32C checksum-combine math so object-level FULL_OBJECT
   checksums can be derived from per-part checksums + part lengths.
2. Expose a small, tested helper API used by multipart complete logic.
3. Validate combine helpers against direct whole-buffer checksum results for
   2-part and 3-part inputs.

Acceptance:

1. Dedicated unit tests prove `combine(part1, part2, len2)` equals checksum of
   concatenated bytes for CRC32 and CRC32C.
2. `CompleteMultipartUpload` can use combine helpers without reconstructing
   full object bytes for CRC32/CRC32C FULL_OBJECT checksums.

### Step 2: CreateMultipartUpload checksum contract

Files:

- `crates/server/src/http/mod.rs`
- `crates/server/src/coordinator.rs`
- `crates/server/src/http/xml.rs`
- `crates/server/src/http/response.rs`

Tasks:

1. Parse and validate `x-amz-checksum-algorithm` and `x-amz-checksum-type`.
2. Persist algorithm/type on multipart upload record.
3. Return `ChecksumAlgorithm`/`ChecksumType` in initiate XML response.

Acceptance:

1. HTTP tests for invalid algorithm/type and mismatch cases.
2. Initiate response exposes checksum fields when set.

### Step 3: UploadPart checksum verify/store/respond

Files:

- `crates/server/src/http/mod.rs`
- `crates/server/src/coordinator.rs`
- `crates/server/src/http/response.rs`

Tasks:

1. Reuse/extend checksum validation helper for UploadPart.
2. Enforce one checksum header max and algorithm consistency with upload config.
3. Compute checksum from part bytes, verify claimed value, return `BadDigest` on mismatch.
4. Persist per-part checksum bytes in `multipart_parts`.
5. Include part checksum header in UploadPart response.

Acceptance:

1. Unit tests for: bad digest, missing/multiple checksum headers, algorithm mismatch.
2. Re-upload same part number preserves latest checksum metadata.

### Step 4: CompleteMultipartUpload checksum validation/computation

Files:

- `crates/server/src/http/xml.rs`
- `crates/server/src/coordinator.rs`
- `crates/server/src/http/response.rs`
- `crates/server/src/metadata_blob.rs`

Tasks:

1. Extend complete XML parser to read per-part checksum fields.
2. Validate per-part checksums in request against stored part checksums.
3. Compute object checksum by checksum type:
- `COMPOSITE` (SHA1/SHA256): hash concatenated raw part checksums and append `-N`.
- `FULL_OBJECT` (CRC*): compute from per-part checksums using combine math.
4. Compare provided object checksum (if present); on mismatch return `BadDigest`.
5. Persist final object checksum + `x-amz-checksum-type` in metadata blob.
6. Return checksum fields in complete response XML.

Acceptance:

1. `test_multipart_checksum_sha256` passes, including `BadDigest` negative cases.

### Step 5: Surface checksums in list/head/get/object-attributes

Files:

- `crates/server/src/coordinator.rs`
- `crates/server/src/http/response.rs`
- `crates/server/src/http/xml.rs`

Tasks:

1. `ListParts` XML:
- top-level `ChecksumAlgorithm`, `ChecksumType`
- per-part checksum field in each `<Part>`
2. `HeadObject`/`GetObject` with `ChecksumMode=ENABLED`:
- include object checksum
- include `x-amz-checksum-type`
3. `GetObjectAttributes`:
- include `ChecksumType` in `<Checksum>`
- include per-part checksums in `<ObjectParts><Part>...`

Acceptance:

1. `test_get_multipart_checksum_object_attributes` passes.
2. XML unit tests cover with/without checksum fields.

### Step 6: Enable and harden tests

Files:

- `crates/s3-tests/tests/checksums.rs`
- `crates/s3-tests/tests/object_attributes.rs`
- server unit tests under `crates/server/src/http/` and `crates/server/src/coordinator.rs`

Tasks:

1. Unignore the 6 checksum tests + 1 object-attributes test.
2. Port helper behavior from Ceph semantics already referenced in `tmp/s3-tests`.
3. Add explicit negative tests for malformed checksum headers/XML fields.

Acceptance:

1. `cargo test -p s3-tests --test checksums`
2. `cargo test -p s3-tests --test object_attributes`
3. `cargo test -p s3-tests`

## Risks and Guardrails

1. Risk: algorithm/type mismatch behavior differs from AWS.
- Guardrail: add contract tests for explicit combinations we support now.

2. Risk: large complete operations become expensive for FULL_OBJECT CRC.
- Guardrail: Step 1b requires combine math up front; forbid full-object reads in
  complete checksum path.

3. Risk: checksum field drift between multipart tables and committed object manifest.
- Guardrail: keep checksum fields in `multipart_parts` -> `object_parts` commit path
  as part of the same transaction.

## Follow-up (after this slice)

1. Add `GET ?partNumber=` support so per-part checksum retrieval via `GetObject`
   can match Ceph helper behavior exactly.
2. Add fault-injection tests for checksum metadata failure points.
