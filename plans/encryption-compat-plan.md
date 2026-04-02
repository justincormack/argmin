# Encryption Compatibility Follow-Up Plan

## Scope

This is a stub plan for the remaining S3 encryption compatibility surface.

The current server does not implement per-object S3 encryption semantics. The
remaining `s3-tests` coverage in this area should be tracked here rather than in
the old integration-framework bootstrap plan.

This plan covers the three main S3 encryption families:
- SSE-S3
- SSE-KMS
- SSE-C

It also covers the directly related configuration and compatibility surfaces:
- default bucket encryption
- encryption-related `GetObjectAttributes` coverage
- policy/enforcement tests that require encryption support

This does not yet choose the final key-management architecture.

Detailed follow-up plans should live in separate documents as decisions become
concrete. The current detailed plan for the first non-`SSE-C` encryption phase
is:

- `plans/sse-s3-plan.md`

## Current Status

Implemented:
- SSE-C object encryption/decryption
- SSE-C multipart, copy, POST, presigned, and object-attributes coverage
- SSE-C checksum-metadata protection
- SSE-C bucket-level gating subset
- direct TLS support and HTTPS enforcement for SSE-C
- transport/auth support needed to carry encryption headers through the HTTP layer

Not implemented:
- SSE-S3 object encryption/decryption
- SSE-KMS object encryption/decryption
- default bucket encryption
- encryption policy/enforcement behavior

Partially implemented:
- encryption-aware object attribute responses for SSE-C; broader SSE-S3/SSE-KMS
  behavior remains open

Known test surface still blocked on this work:
- `encryption_kms.rs`
- `encryption_s3.rs`
- `encryption_kms_default.rs`
- `policy_encryption.rs`
- remaining non-SSE-C encryption cases in `object_attributes.rs`

## Compatibility Areas

### 1. SSE-S3

Server-managed keys for per-object encryption.

Chosen direction:

- implement `SSE-S3` first
- use a temporary but explicit service-managed wrapping-key provider
- keep that key role separate from the `SSE-C` validator key
- preserve the per-object random `DEK` envelope model already used by `SSE-C`

See `plans/sse-s3-plan.md` for the detailed implementation plan.

### 2. SSE-KMS

KMS-managed keys and the related API semantics.

Open design work:
- internal KMS abstraction
- testing strategy (mock vs real integration)
- object metadata and request validation
- default bucket encryption interaction

### 3. SSE-C

Customer-provided encryption keys supplied per request.

Open design work:
- header validation and key-MD5 handling
- key usage rules on GET/HEAD/COPY/multipart
- metadata needed to validate access without persisting raw keys

### 4. Default Bucket Encryption

Bucket-level default encryption configuration and its interaction with object
writes, copy, multipart, and reporting APIs.

## Recommended Implementation Order

1. SSE-S3 core object encryption model
2. default bucket encryption on top of SSE-S3
3. SSE-KMS abstraction and compatibility
4. encryption policy/enforcement tests

## Initial Design Defaults

- start with correctness and compatibility, not key-management sophistication
- keep the cryptographic/key-management boundary explicit in `server-core`
- do not mix this work into the remaining auth/ACL ownership plan

## Test Plan

Targeted integration tests once work starts:
- `cargo test -p s3-tests --test encryption_s3`
- `cargo test -p s3-tests --test encryption_sse_c`
- `cargo test -p s3-tests --test encryption_kms`
- `cargo test -p s3-tests --test encryption_kms_default`
- `cargo test -p s3-tests --test policy_encryption`
- `cargo test -p s3-tests --test object_attributes`
