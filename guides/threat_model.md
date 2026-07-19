# Threat model

Note this will continue to evolve as more features around users and encryption
are added.

## 1. Overview
Argmin is a single-node, S3-compatible object storage server written in Rust
(the `argmin-s3` binary). It exposes an HTTP/1 endpoint implementing a subset
of S3 APIs (bucket/object CRUD, multipart uploads, tagging, CORS, ACLs, bucket
policy, object lock, etc.) using path-style addressing. Requests are parsed in
`server-http`, authenticated using AWS Signature Version 4 in the `auth`
crate, and dispatched to `server-core`, which enforces bucket/object
authorization and orchestrates erasure-coded storage. Object data and metadata
are persisted locally: shard files on disk plus per-placement-group SQLite
metadata databases (`storage` crate). Data integrity uses CRC64-NVME checksums
and verified reads; erasure coding via native Rust backends provides
redundancy.
Typical deployments are local or internal S3-compatible storage for testing or
lightweight environments, configured by environment variables. Security is
centered on SigV4 authentication plus optional public ACL and bucket-policy
paths. The server now has optional built-in TLS support when
`ARGMIN_TLS_CERT_PATH` and `ARGMIN_TLS_KEY_PATH` are configured, and still
supports plain HTTP deployments. It also supports object encryption features
such as `SSE-C`; the HTTP layer rejects `SSE-C` requests unless the transport
is marked secure. Confidentiality therefore depends on actually deploying with
TLS, either directly in `argmin-s3` or via a trusted external terminator, plus
filesystem permissions. Plain HTTP remains possible and should be treated as a
non-confidential deployment mode.

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
- **Configuration boundary:** Environment variables control credentials, listen address, limits, data directory, and tracing output.
- **Cluster-internal boundary:** Phase 11 control-plane and storage-node
  roles communicate over local Unix sockets and trust the local host, process
  identity, data directory ownership, and socket directory permissions. A
  storage node heartbeat is treated as a statement from a trusted node process
  about its locally verified durable PG state, not as an arbitrary public
  network input.
- **Developer boundary:** Test utilities (`crates/s3-tests`, `crates/sts-tests`,
  `test-util`) and build scripts are not part of production runtime.

### Assumptions
- Host OS and filesystem permissions are trusted; unprivileged local users cannot modify `ARGMIN_DATA_DIR` contents.
- Phase 11 multihost/control-plane work assumes the storage-node and
  single-authority control-plane processes are trusted runtime components on
  trusted hosts. It does not attempt to defend against a malicious or
  compromised storage node forging heartbeats, corrupting its local metadata
  database, or lying about PG metadata proofs. Local storage nodes validate
  their command-log hash chain and metadata state digest before reporting PG
  heartbeat proofs, and the authority uses those proofs as fencing evidence
  from trusted nodes. Remote authenticated control-plane transport and any
  stronger node attestation story are future work and outside the current
  object-store threat boundary; they belong to the deployment/runtime trust
  model rather than S3 request authorization.
- System clock is reasonably accurate for SigV4 expiry checks.
- Deployments that need confidentiality or `SSE-C` use secure transport. This
  can be provided directly by `argmin-s3` via its TLS config or by a trusted
  external terminator. Plain HTTP is still a supported deployment mode, but it
  is not appropriate for confidential traffic. The HTTP layer rejects `SSE-C`
  requests unless the transport is marked secure.
- Tracing and diagnostics now have an observability-safe escaping and redaction
  baseline for attacker-controlled text and secret-bearing values, but trace
  output is still operationally sensitive and should be protected accordingly.
- The server is effectively single-tenant; ACLs primarily govern public vs owner access rather than multi-user isolation.

## 3. Attack surface, mitigations and attacker stories
### HTTP entrypoints and request parsing
- **Surface:** Hyper-based HTTP/1 server (`server-http/src/http/serve.rs`) accepts TCP connections; risks include request smuggling, slowloris, oversized bodies, and malformed headers.
- **Mitigations:** configurable `max_connections`/`max_inflight_requests`, header and body idle timeouts, bounded buffered control-plane bodies (`MAX_BUFFERED_CONTROL_BODY_SIZE` in `request.rs`), content-length validation, and UTF‑8 validation of header values.
- **Input validation:** bucket names follow S3-style constraints and object
  keys are limited to 1-1024 bytes with NUL-byte rejection (`router.rs`),
  strict percent-decoding, and explicit per-surface header/body validation.
  Other control bytes in object keys are currently accepted on the API
  surface, so downstream XML/listing/observability paths must continue to
  escape or encode them safely.

### Authentication and authorization
- **SigV4 verification:** `auth/request.rs`, `auth/sigv4.rs`, and `auth/canonical.rs` implement canonicalization, signature derivation, and constant-time comparison. Limits exist for header lengths, query sizes, signed header counts, and duplicate Authorization headers.
- **Presigned URLs:** expiry and scope validation with explicit max TTLs.
- **POST Object:** multipart form parsing and policy validation (`multipart.rs`, `auth/post.rs`) including JSON policy parsing and condition enforcement.
- **Authorization:** `server-core/src/coordinator.rs` enforces owner/admin-only
  operations, ACLs, bucket policy evaluation, public access blocks, ownership
  controls, object-lock retention and legal-hold rules, and missing-object
  discovery masking. Anonymous access is only permitted when ACLs or bucket
  policy make a bucket/object public.

### Object data handling, integrity, and streaming
- **Streaming uploads:** aws-chunked decoding and per-chunk signatures (`chunked.rs`, `serve.rs`) with chunk size validation, trailer checksum verification, and size caps (`MAX_OBJECT_SIZE`).
- **Integrity:** CRC64-NVME checksums on writes and reads, with shard quarantine on mismatch (`storage/pg_store.rs`, `checksum` crate). ETags use CRC64.
- **Erasure coding:** native Rust erasure coding provides redundancy with architecture-specific SIMD paths on supported CPUs.
- **Multipart uploads and streamed writes:** XML parsing for
  `CompleteMultipartUpload` and delete/multipart controls (`xml.rs`), plus
  staged stream-session state in `server-core`; risks include orphaned uploads,
  leaked staged state, or incorrect cleanup when lifetime invariants break.

### Metadata and storage layer
- **SQLite and shard files:** parameterized queries reduce SQL injection risk; shard paths are derived from hashed shard keys, preventing user-controlled path traversal.
- **Durability measures:** temp-file writes + fsync/rename for shards, per-PG
  metadata command serialization, durable bucket write reservations/drains, and
  storage-side snapshot validation limit race conditions. Bucket locks are
  retained only for test probes and are not production correctness boundaries.
- **Background maintenance paths:** reclaim queues, payload leases,
  multipart/session cleanup, and lifecycle sweeps are security-relevant even
  though they are internal. Races or stale-state bugs here can cause resource
  exhaustion, orphaned data, or violations of retention/deletion guarantees.

### CORS and browser-facing behavior
- Bucket-specific CORS matching (`cors.rs`) can allow cross-origin reads/writes when misconfigured. This is an operational risk rather than a code bug, but affects confidentiality when combined with public buckets or presigned URLs.

### Observability and secrets
- Tracing now uses escaping and redaction helpers for attacker-controlled text
  and secret-bearing values such as SigV4 material, session tokens, and
  `SSE-C` key-derived state. Operators should still treat trace output and
  diagnostics as sensitive operational data and avoid adding raw protocol or
  encryption material to new logs.

### Attacker stories
1. **Unauthenticated network attacker** exploits a canonicalization or signature verification bug to bypass SigV4 and read/write private objects (critical impact).
2. **Authenticated malicious client** floods multipart uploads, streamed writes,
   or cleanup-sensitive paths to exhaust disk, memory, or CPU. Timeouts and
   size caps help, but there are still no tenant quotas.
3. **Anonymous or unintended public access misuse:** public-read/write buckets
   and public bucket policies intentionally allow unauthenticated access;
   vulnerabilities here would stem from ACL, bucket-policy, ownership-control,
   or public-access-block logic failures.
4. **Local filesystem attacker** tampers with SQLite metadata or shard files. CRC checks can detect corruption, but confidentiality/integrity relies on OS permissions.
5. **CORS misuse:** overly broad CORS rules combined with presigned URLs enable browser-based exfiltration.
6. **Transport misconfiguration:** operators run plain HTTP or misconfigure TLS
   termination, exposing credentials or object data on the wire. `SSE-C`
   requests are rejected without secure transport, but non-`SSE-C` traffic is
   still only as confidential as the deployed transport.

### Out-of-scope or lower relevance
- CSRF/XSS risks are limited because the API is not a session-based web app; XML responses are escaped.
- SSRF is not applicable; the server does not perform outbound fetches.
- SQL injection risk is low due to parameterized queries and no dynamic SQL generation.
- Database migrations are not currently supported or implemented, and older schemas are not supported until a future date when stability will be declared.

## 4. Criticality calibration (critical, high, medium, low)
- **Critical:** remote code execution, SigV4 auth bypass allowing unauthenticated read/write/delete of private buckets, or leakage of access keys/secrets.
- **High:** authorization bugs that allow public write/read when ACLs forbid it, path traversal allowing writes outside `ARGMIN_DATA_DIR`, or metadata corruption causing permanent data loss.
- **Medium:** transient denial of service (CPU/memory spikes, excessive multipart uploads) or information disclosure of bucket metadata that does not expose object data.
- **Low:** minor logging of non-sensitive metadata, incorrect error codes, or edge-case canonicalization mismatches that only affect interoperability.
