# Threat model

Note this will continue to evolve as more features around users, direct TLS
support, and encryption are added.

## 1. Overview
Argmin2 is a single-node, S3-compatible object storage server written in Rust (the `argmin-s3` binary). It exposes an HTTP/1 endpoint implementing a subset of S3 APIs (bucket/object CRUD, multipart uploads, tagging, CORS, ACLs, etc.) using path-style addressing. Requests are parsed in `server-http`, authenticated using AWS Signature Version 4 in the `auth` crate, and dispatched to `server-core`, which enforces bucket/object authorization and orchestrates erasure-coded storage. Object data and metadata are persisted locally: shard files on disk plus per-placement-group SQLite metadata databases (`storage` crate). Data integrity uses CRC64-NVME checksums and verified reads; erasure coding via ISA‑L (`ec`/`ec-sys`) provides redundancy.
Typical deployments are local or internal S3-compatible storage for testing or
lightweight environments, configured by environment variables. Security is
centered on SigV4 authentication plus optional public bucket ACLs. The server
now includes object encryption features such as `SSE-C`, but direct TLS support
is still being implemented. Until that lands, confidentiality depends on
external transport protection and filesystem permissions. That external-TLS
assumption is transitional, not the intended end state for `SSE-C`.

## 2. Threat model, Trust boundaries and assumptions
### Assets
- Object data (payloads), metadata (tags, ACLs, versioning state), and bucket listings.
- Access credentials (access key/secret, optional session tokens).
- Integrity of shard files and SQLite metadata.
- Availability of the service and storage capacity.

### Trust boundaries
- **Network boundary:** All HTTP request components (method, path, query string, headers, body, XML, multipart form fields, aws-chunked frames) are attacker-controlled.
- **Authentication boundary:** `auth::authenticate_request` and `auth::authenticate_post_sigv4` are the primary gates; any bug here impacts all operations.
- **Core/storage boundary:** `server-core` assumes validated bucket/key strings and authenticated `Requester` identity; `storage` assumes internal shard keys and well-formed metadata.
- **FFI boundary:** `ec-sys` and ISA‑L routines operate on untrusted data; memory safety depends on correct FFI usage and bounds checks.
- **Configuration boundary:** Environment variables control credentials, listen address, limits, data directory, and tracing output.
- **Developer boundary:** Test utilities (`crates/s3-tests`, `test-util`) and build scripts are not part of production runtime.

### Assumptions
- Host OS and filesystem permissions are trusted; unprivileged local users cannot modify `ARGMIN_DATA_DIR` contents.
- System clock is reasonably accurate for SigV4 expiry checks.
- Until direct server TLS lands, TLS is provided externally (trusted network or
  external terminator). This is a temporary assumption and is not sufficient
  for AWS-compatible `SSE-C`, which must ultimately be enforced over direct
  HTTPS. Secrets are not logged or exposed via tracing.
- The server is effectively single-tenant; ACLs primarily govern public vs owner access rather than multi-user isolation.

## 3. Attack surface, mitigations and attacker stories
### HTTP entrypoints and request parsing
- **Surface:** Hyper-based HTTP/1 server (`server-http/src/http/serve.rs`) accepts TCP connections; risks include request smuggling, slowloris, oversized bodies, and malformed headers.
- **Mitigations:** configurable `max_connections`/`max_inflight_requests`, header and body idle timeouts, bounded buffered control-plane bodies (`MAX_BUFFERED_CONTROL_BODY_SIZE` in `request.rs`), content-length validation, and UTF‑8 validation of header values.
- **Input validation:** bucket and object key constraints (`router.rs`), strict percent-decoding, rejection of null/control bytes, and explicit maximum key lengths.

### Authentication and authorization
- **SigV4 verification:** `auth/request.rs`, `auth/sigv4.rs`, and `auth/canonical.rs` implement canonicalization, signature derivation, and constant-time comparison. Limits exist for header lengths, query sizes, signed header counts, and duplicate Authorization headers.
- **Presigned URLs:** expiry and scope validation with explicit max TTLs.
- **POST Object:** multipart form parsing and policy validation (`multipart.rs`, `auth/post.rs`) including JSON policy parsing and condition enforcement.
- **Authorization:** `server-core/src/coordinator.rs` enforces owner-only admin operations, public read/write ACLs, public access blocks, and ownership controls. Anonymous access is only permitted for explicitly public buckets.

### Object data handling, integrity, and streaming
- **Streaming uploads:** aws-chunked decoding and per-chunk signatures (`chunked.rs`, `serve.rs`) with chunk size validation, trailer checksum verification, and size caps (`MAX_OBJECT_SIZE`).
- **Integrity:** CRC64-NVME checksums on writes and reads, with shard quarantine on mismatch (`storage/pg_store.rs`, `checksum` crate). ETags use CRC64.
- **Erasure coding:** ISA‑L via `ec`/`ec-sys` provides redundancy; unsafe FFI boundaries are a potential memory-safety risk if misused.
- **Multipart uploads:** XML parsing for `CompleteMultipartUpload` and delete/multipart controls (`xml.rs`); risks include orphaned uploads consuming space.

### Metadata and storage layer
- **SQLite and shard files:** parameterized queries reduce SQL injection risk; shard paths are derived from hashed shard keys, preventing user-controlled path traversal.
- **Durability measures:** temp-file writes + fsync/rename for shards; per-PG mutexing and bucket locks limit race conditions.

### CORS and browser-facing behavior
- Bucket-specific CORS matching (`cors.rs`) can allow cross-origin reads/writes when misconfigured. This is an operational risk rather than a code bug, but affects confidentiality when combined with public buckets or presigned URLs.

### Observability and secrets
- Optional tracing logs request metadata and timings; secrets should not be logged, but operators must protect trace files and avoid enabling verbose logs in untrusted environments.

### Attacker stories
1. **Unauthenticated network attacker** exploits a canonicalization or signature verification bug to bypass SigV4 and read/write private objects (critical impact).
2. **Authenticated malicious client** floods multipart uploads or slow streaming requests to exhaust disk or CPU; mitigated by timeouts and size caps but no quotas.
3. **Anonymous access misuse:** public-read/write buckets intentionally allow unauthenticated access; vulnerabilities would stem from ACL or public access block logic failures.
4. **Local filesystem attacker** tampers with SQLite metadata or shard files. CRC checks can detect corruption, but confidentiality/integrity relies on OS permissions.
5. **CORS misuse:** overly broad CORS rules combined with presigned URLs enable browser-based exfiltration.

### Out-of-scope or lower relevance
- CSRF/XSS risks are limited because the API is not a session-based web app; XML responses are escaped.
- SSRF is not applicable; the server does not perform outbound fetches.
- SQL injection risk is low due to parameterized queries and no dynamic SQL generation.

## 4. Criticality calibration (critical, high, medium, low)
- **Critical:** remote code execution (e.g., unsafe FFI memory corruption), SigV4 auth bypass allowing unauthenticated read/write/delete of private buckets, or leakage of access keys/secrets.
- **High:** authorization bugs that allow public write/read when ACLs forbid it, path traversal allowing writes outside `ARGMIN_DATA_DIR`, or metadata corruption causing permanent data loss.
- **Medium:** transient denial of service (CPU/memory spikes, excessive multipart uploads) or information disclosure of bucket metadata that does not expose object data.
- **Low:** minor logging of non-sensitive metadata, incorrect error codes, or edge-case canonicalization mismatches that only affect interoperability.
