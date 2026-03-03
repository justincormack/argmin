# GetObjectAttributes Implementation Plan

## Overview

Add the `GetObjectAttributes` API (`GET /<bucket>/<key>?attributes`). This is
similar to HeadObject but returns a selective XML body instead of headers-only.
The caller specifies which attributes to include via the
`x-amz-object-attributes` request header.

No data structure changes are needed — the coordinator's existing `head_object`
already fetches everything we need (etag, size, last_modified, version_id,
metadata blob with checksums). We just build a different response format.

## Scope

### Attributes supported

| Attribute | Source | Notes |
|-----------|--------|-------|
| `ETag` | `HeadObjectResult.etag` | Strip quotes for XML body |
| `ObjectSize` | `HeadObjectResult.size` | u64 |
| `StorageClass` | Always `STANDARD` | We only have one storage class |
| `Checksum` | `HeadObjectResult.metadata` checksum entries | SHA256, CRC32, CRC32C, CRC64NVME, SHA1 |
| `ObjectParts` | Not applicable yet | We don't have multipart — omit from response |

### Out of scope (for now)

- `ObjectParts` with part-level detail (needs multipart upload support)
- SSE-C encrypted object support (needs encryption)
- `x-amz-max-parts` / `x-amz-part-number-marker` pagination headers
- `x-amz-expected-bucket-owner` header

## Implementation steps

### 1. Router: add `GetObjectAttributes` operation

In `crates/server/src/http/router.rs`:

- Add `GetObjectAttributes { bucket, key }` variant to `S3Operation` enum
- Route `GET /<bucket>/<key>?attributes` to it — must appear before the
  catch-all `GET /<bucket>/<key>` → `GetObject` match arm
- Add router unit tests

### 2. XML: add response formatter

In `crates/server/src/http/xml.rs`:

- Add `get_object_attributes_xml()` function that takes the requested
  attributes set, etag, size, and checksum entries, and builds the
  `<GetObjectAttributesResponse>` XML body containing only the requested
  attributes
- Add unit tests for the XML builder

### 3. Handler: add operation handler

In `crates/server/src/http/mod.rs`:

- Parse the `x-amz-object-attributes` header (comma-separated, trim
  whitespace). Reject unknown attribute names with `InvalidArgument`
- Reuse `coordinator.head_object()` — no new coordinator method needed
- Parse `?versionId` from query as with HeadObject
- Evaluate conditional request headers (If-Match etc) — same as HeadObject
- Build response: XML body via `get_object_attributes_xml()`, plus
  `Last-Modified` header and `x-amz-version-id` header if versioned
- `x-amz-delete-marker` header if the object is a delete marker (return 404
  with the header set, matching S3 behavior)

### 4. Response builder

In `crates/server/src/http/response.rs`:

- Add `S3Response::get_object_attributes()` method that takes the XML body
  string and result metadata (last_modified, version_id) and builds a 200
  response with the XML body and appropriate headers

### 5. Integration tests

In `crates/s3-tests/tests/object_attributes.rs`:

Port the 8 Ceph tests, skipping multipart and SSE-C ones as `#[ignore]`:

| Test | Status |
|------|--------|
| `test_get_object_attributes` | Implement — basic: put object, get attributes, verify ETag/Size/StorageClass |
| `test_get_checksum_object_attributes` | Implement — put object with SHA-256 checksum, verify Checksum in response |
| `test_get_versioned_object_attributes` | Implement — versioned bucket, verify VersionId, fetch specific version |
| `test_get_sse_c_encrypted_object_attributes` | `#[ignore]` — needs SSE-C |
| `test_get_multipart_object_attributes` | `#[ignore]` — needs multipart |
| `test_get_single_multipart_object_attributes` | `#[ignore]` — needs multipart |
| `test_get_paginated_multipart_object_attributes` | `#[ignore]` — needs multipart |
| `test_get_multipart_checksum_object_attributes` | `#[ignore]` — needs multipart |

Additional edge-case tests beyond Ceph:
- Request with no `x-amz-object-attributes` header → error
- Request with invalid attribute name → error
- Request for nonexistent key → 404
- Request for single attribute only (e.g. just `ObjectSize`) → verify
  response only contains that element

## Response XML format

```xml
<?xml version="1.0" encoding="UTF-8"?>
<GetObjectAttributesResponse>
    <ETag>hex-string</ETag>
    <Checksum>
        <ChecksumSHA256>base64</ChecksumSHA256>
    </Checksum>
    <StorageClass>STANDARD</StorageClass>
    <ObjectSize>12345</ObjectSize>
</GetObjectAttributesResponse>
```

Only requested attributes are included. `ObjectParts` is omitted for non-
multipart objects even if requested.

## Notes

- The ETag in the XML body is unquoted (unlike the ETag HTTP header which is
  quoted). HeadObjectResult.etag includes quotes, so strip them.
- The `x-amz-object-attributes` header values are case-sensitive (`ETag` not
  `etag`).
- Response must include `Last-Modified` header (not in XML, in HTTP headers).
- Response must include `x-amz-version-id` header if versioned.
- No `Content-Length` header for the object data (unlike HEAD) — the
  Content-Length is for the XML body.
- The AWS SDK `get_object_attributes()` sends the attributes header as
  comma-separated. We need to handle whitespace around commas.
