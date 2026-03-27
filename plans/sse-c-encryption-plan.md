# SSE-C Encryption Plan

## Goal

Implement AWS-compatible server-side encryption with customer-provided keys
(`SSE-C`) in a way that:

1. matches the S3 API surface and error behavior as closely as possible
2. does not store customer keys
3. fits the current object/segment/part storage model
4. becomes the shared foundation for later `SSE-S3` / `SSE-KMS`

This plan is intentionally broader than “accept the headers and encrypt some
bytes”. `SSE-C` touches write paths, read paths, multipart state, copy, object
attributes, checksums, presigning, and bucket encryption policy.

## AWS Constraints To Match

Primary references:

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/specifying-s3-c-encryption.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/default-s3-c-encryption-setting-faq.html
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/blocking-unblocking-s3-c-encryption-gpb.html

Important behavior from the AWS docs:

1. `SSE-C` encrypts object data only, not all object metadata at rest.
   However, AWS still requires the SSE-C header triad for some metadata-bearing
   APIs such as `HeadObject` and `GetObjectAttributes`, so metadata access is
   not freely available without the customer key.
   Also, AWS documents that stored object checksum metadata is protected under
   server-side encryption, so the checksum-related metadata surface must be
   treated differently from ordinary unencrypted object metadata.
2. Requests using `SSE-C` headers must use HTTPS. AWS rejects HTTP.
   Temporary implementation note:
   local integration testing may allow HTTP initially so the crypto/read-write
   path can be exercised before HTTPS-only enforcement lands. Restoring the AWS
   HTTPS requirement remains a deferred compatibility item before completion.
3. The request header triad is:
   - `x-amz-server-side-encryption-customer-algorithm: AES256`
   - `x-amz-server-side-encryption-customer-key`
   - `x-amz-server-side-encryption-customer-key-MD5`
4. AWS does not store the customer key. It stores a randomly salted HMAC value
   of the key to validate later requests.
5. For multipart, the encryption information on part uploads must match the
   initiate request.
6. `SSE-C` applies to:
   - `PutObject`
   - `GetObject`
   - `HeadObject`
   - `CreateMultipartUpload`
   - `UploadPart`
   - `UploadPartCopy`
   - `CopyObject`
   - `POST Object`
   - `GetObjectAttributes`
7. For source objects encrypted with `SSE-C`, copy-style requests require the
   `x-amz-copy-source-server-side-encryption-customer-*` header triad.
8. The ETag for `SSE-C` objects is not the MD5 of object data.

### Date-sensitive AWS note

As of March 27, 2026, the AWS docs say:

- starting in April 2026, new buckets will have `SSE-C` disabled by default
- buckets that do not already contain `SSE-C` data may also have `SSE-C`
  disabled by default
- enabling `SSE-C` becomes a bucket encryption configuration concern

That means we should not design `SSE-C` as an unconditional object feature with
no bucket-level gate. The object encryption work should be separable from the
bucket-level “blocked encryption types” policy, but must be ready for it.

## Current State In Argmin

What we already have that helps:

1. strong typed write/read coordinator paths
2. stable internal segmenting for standard objects
3. multipart state and manifest model
4. shard CRC64 integrity over stored bytes
5. direct small `PutObject` path and streamed paths

What we do not have:

1. any object encryption state in schema or storage types
2. any request parsing for `SSE-C` headers
3. any bucket encryption policy model
4. any crypto layer for wrapping object keys / decrypting reads

Relevant code areas:

- config: [`crates/argmin-s3/src/config.rs`](/home/justin/src/github.com/justincormack/argmin/crates/argmin-s3/src/config.rs)
- HTTP request handling: [`crates/server-http/src/http/mod.rs`](/home/justin/src/github.com/justincormack/argmin/crates/server-http/src/http/mod.rs)
- coordinator object and multipart flows: [`crates/server-core/src/coordinator.rs`](/home/justin/src/github.com/justincormack/argmin/crates/server-core/src/coordinator.rs)
- object metadata types: [`crates/storage/src/types.rs`](/home/justin/src/github.com/justincormack/argmin/crates/storage/src/types.rs)
- metadata schema: [`crates/storage/src/schema.rs`](/home/justin/src/github.com/justincormack/argmin/crates/storage/src/schema.rs)
- object/multipart metadata SQL: [`crates/storage/src/pg_store.rs`](/home/justin/src/github.com/justincormack/argmin/crates/storage/src/pg_store.rs)
- ignored `GetObjectAttributes` SSE-C test: [`crates/s3-tests/tests/object_attributes.rs`](/home/justin/src/github.com/justincormack/argmin/crates/s3-tests/tests/object_attributes.rs)

## Proposed Internal Model

### 1. Add versioned SSE-C validator keys to config

For `SSE-C`, the server-side secret should be only for validating customer-key
identity, not for recovering object data.

So the first config surface should be an `SSE-C` validator key or keyring, not
a general encryption root used for data-key wrapping.

Suggested initial model:

- `ARGMIN_SSE_C_VALIDATOR_KEY`

with an internal key id stored in metadata from day one.

If we want explicit rotation at config level immediately, the cleaner target is
an `SSE-C` validator keyring rather than a single bare key, but the important
part is:

1. `SSE-C` validator keys are a distinct key role
2. stored object metadata carries `validator_key_id`
3. future `SSE-S3` service-managed wrapping keys are a separate key role

That avoids conflating:

1. `SSE-C` key validation
2. `SSE-S3` service-managed encryption hierarchy

### 2. Separate key validation from data encryption

The clean model is:

1. validate the customer key with a stored salted HMAC using a server-side
   validator key
2. use the customer key to unwrap an object data-encryption key (`DEK`)
3. use the `DEK` to encrypt/decrypt object payload bytes

That means we do **not** derive stored ciphertext directly from the customer
key. Instead:

1. on write, generate a random per-object `DEK`
2. wrap that `DEK` with a key-encryption key derived only from the customer key
3. store:
   - `validator_key_id`
   - salted HMAC validator of the customer key
   - wrapped `DEK`
   - wrap salt / crypto version / algorithm metadata

This preserves the expected `SSE-C` property:

1. object recovery depends only on the customer key
2. rotating the server-side validator key does not make old objects
   undecryptable
3. the server-side secret is not a second dependency for data recovery

This also still gives us the same object-crypto abstraction we will want later
for `SSE-S3` and `SSE-KMS`, but with distinct key roles rather than one shared
root.

### 3. Encrypt before EC, store ciphertext shards

Write path should be:

1. validate plaintext checksums / `Content-MD5`
2. encrypt plaintext segment or part payload into ciphertext
3. EC-encode the ciphertext
4. write shard files
5. CRC64 stays over the stored ciphertext shard bytes

Read path should be:

1. read / reconstruct ciphertext shards
2. validate shard CRC64 over ciphertext
3. decrypt the segment/part ciphertext
4. serve plaintext bytes

This keeps the current disk integrity model valid.

### 4. Use AEAD per segment / part chunk

We already have segment-sized boundaries in the standard object path, and the
multipart path already has natural part and streamed-part-segment boundaries.

The design should use per-chunk AEAD:

1. standard object:
   - one encrypted chunk per committed segment
2. direct small `PutObject`:
   - one encrypted chunk
3. multipart:
   - one encrypted chunk per stored part, or per streamed part-segment where
     that path already exists

This keeps range reads workable: decrypt the containing chunk, then slice the
requested plaintext range.

### 5. No extra offline checksum field is needed initially

We do not need a new “ciphertext checksum” column just for encryption.

Reason:

1. we already store shard CRC64 for the bytes actually written to disk
2. after encryption, those bytes are ciphertext, so the existing CRC64 becomes
   the offline integrity check for ciphertext-at-rest
3. AEAD authentication covers tamper detection during decrypt/read

So the current CRC64 layer remains useful. The plaintext object checksum model
is separate and should continue to work at the S3 API level.

## Schema And Type Changes

We need first-class encryption state, not metadata-blob hacks.

### Add object encryption columns

Add an explicit encryption discriminator plus serialized encryption state to:

1. `objects`
2. `multipart_uploads`
3. `stream_uploads`

Suggested shape:

- `encryption_type INTEGER NOT NULL DEFAULT 0`
- `encryption_state BLOB`

Where:

- `0 = none`
- `1 = sse-c`

And the serialized `encryption_state` contains a versioned encoding of:

1. `validator_key_id`
2. validator salt
3. validator HMAC
4. wrapped object `DEK`
5. wrap salt / wrap nonce
6. internal cipher / version metadata

For `SSE-C`, there should be no server-side wrapping-key id here because the
wrapped `DEK` must be recoverable from the customer key alone.

For later non-`SSE-C` modes, the same top-level envelope can carry different
mode-specific state.

### Storage types

Add typed encryption fields to the storage request/record types rather than raw
header strings.

Likely new shared types:

1. `EncryptionType`
2. `EncryptionState`
3. `SseCustomerState`
4. `SseCustomerRequest`

These should sit close to the storage/core boundary, not in `MetadataBlob`.

## HTTP / API Surface To Support

## Phase 0: AWS conformance discovery

Before implementation, pin the exact behavior against AWS for:

1. missing one of the three `SSE-C` headers
2. malformed base64 key
3. wrong key length
4. malformed key-MD5
5. key-MD5 mismatch vs supplied key
6. wrong customer key on `GET` and `HEAD`
7. sending `SSE-C` headers for non-`SSE-C` objects
8. `HTTP` rather than `HTTPS`
9. multipart header behavior on:
   - `CreateMultipartUpload`
   - `UploadPart`
   - `CompleteMultipartUpload`
10. `GetObjectAttributes` for SSE-C objects
11. copy permutations:
   - source SSE-C only
   - destination SSE-C only
   - both source and destination SSE-C

This needs direct `s3-tests` coverage against AWS, because the docs are not
perfectly consistent around multipart-complete headers and the new April 2026
bucket setting.

Current discovery from AWS-backed `s3-tests`:

1. `HeadObject` without SSE-C headers fails with `400`, but the AWS SDK does
   not surface a parsed S3 error code on that `HEAD` failure path.
2. plain SSE-C `CompleteMultipartUpload` succeeds without repeating the SSE-C
   headers in the non-checksum case, so we must not over-constrain that path.
3. `CopyObject` and `UploadPartCopy` from an SSE-C source fail with
   `400 InvalidRequest` when the copy-source SSE-C header triad is missing.

## Phase 1: Core happy path

Implement:

1. `PutObject`
2. `GetObject`
3. `HeadObject`
4. `CreateMultipartUpload`
5. `UploadPart`
6. `CompleteMultipartUpload`
7. `AbortMultipartUpload`

Notes:

1. `DeleteObject`, tagging, listing, and many metadata-only operations should
   not need customer keys because most object metadata is not encrypted at
   rest.
   But AWS does require SSE-C headers on specific metadata reads like
   `HeadObject` and `GetObjectAttributes`, and stored S3 checksum metadata is
   protected under server-side encryption, so those paths must stay explicitly
   compatibility-tested rather than inferred from a blanket “metadata is clear”
   assumption.
2. `HeadObject` still must validate the customer key on SSE-C objects.
3. multipart upload state must persist the encryption state from initiate time,
   so later part uploads can validate “same key as initiate”.

This is still the recommended first implementation order versus `SSE-S3`,
because it avoids full service-managed key infrastructure while forcing the
request/storage/read-path plumbing we will need anyway.

## Phase 2: Secondary request paths

Implement once Phase 1 is stable:

1. `CopyObject`
2. `UploadPartCopy`
3. `POST Object`
4. presigned SSE-C requests

`GetObjectAttributes` is already part of the implemented happy-path slice and
should stay covered in [`object_attributes.rs`](/home/justin/src/github.com/justincormack/argmin/crates/s3-tests/tests/object_attributes.rs).

Current Phase 2 status:

1. `CopyObject` implemented, including SSE-C source reads and SSE-C destination
   writes.
2. `UploadPartCopy` implemented, including SSE-C source reads and SSE-C
   multipart-destination writes.
3. `POST Object` SSE-C still deferred.
4. presigned SSE-C requests still need explicit coverage and any fixes that
   fall out of that coverage.

## Phase 3: Bucket-level gating

Add bucket encryption policy support for SSE-C blocking/unblocking, matching the
AWS model that is rolling out during April 2026.

This likely means:

1. add bucket encryption configuration storage
2. model blocked encryption types (`NONE | SSE-C`)
3. reject SSE-C write requests with `403 AccessDenied` when blocked

This is related but should be kept separate from object crypto internals.

## Request Handling Design

### Parse and validate at the HTTP edge

Do not pass raw `SSE-C` headers around the system.

Instead:

1. parse the request triad into a typed `SseCustomerRequest`
2. validate:
   - algorithm is exactly `AES256`
   - key is valid base64
   - decoded key length is 32 bytes
   - MD5 is valid base64
   - MD5 matches the decoded key bytes
3. pass typed values into coordinator methods

This should mirror the typed checksum work already done.

### Keep response headers derived from the request

On successful `SSE-C` operations, response headers should include:

1. `x-amz-server-side-encryption-customer-algorithm`
2. `x-amz-server-side-encryption-customer-key-MD5`

These can be derived from the validated request, not from stored metadata.

## Read / Write Coordinator Changes

### PutObject

For direct and streamed `PutObject`:

1. request parse validates SSE-C headers
2. coordinator creates object encryption state
3. payload bytes are encrypted before EC encoding
4. committed object row stores encryption state

### Multipart

`CreateMultipartUpload` must persist the encryption state in the MPU row.

Then every later `UploadPart` / `UploadPartCopy` must:

1. require SSE-C headers
2. validate the supplied customer key against the persisted MPU encryption state
3. reuse the same wrapped-object `DEK` / encryption state

`CompleteMultipartUpload` must carry that encryption state into the final object
row.

### Reads

`GetObject` and `HeadObject` on SSE-C objects must:

1. require valid SSE-C headers
2. validate the supplied customer key against stored encryption state
3. reject wrong keys with AWS-compatible errors

For ranged reads, decrypt at the segment/part chunk boundary, then slice the
requested plaintext.

## Checksums, CRC64, And ETags

### Plaintext request checksums

These remain request semantics and must be validated on the plaintext request
body before encryption:

1. `Content-MD5`
2. `x-amz-checksum-*`

### Stored CRC64

Keep current shard CRC64 over the actual bytes written to disk, which for
SSE-C objects will be ciphertext shards.

### API checksum responses

Checksums exposed via S3 APIs should remain checksums of the logical object
data, not of ciphertext-at-rest. That means the checksum metadata model remains
an object-level concern separate from shard CRC64.

For encrypted objects, the persisted S3-visible checksum metadata itself should
be treated as protected metadata:

1. internal shard CRC64 remains cleartext so offline storage integrity checks do
   not need decryption keys
2. S3-visible object checksum metadata (`Content-MD5`, `x-amz-checksum-*`,
   checksum type) remains a logical-object property, not a ciphertext checksum
3. when server-side encryption is in use, the stored checksum metadata should
   be encrypted or otherwise cryptographically protected as part of the
   encryption envelope rather than left as ordinary plaintext object metadata
4. that checksum metadata is only exposed after the normal SSE-C read gate
   succeeds

This matches the AWS documentation distinction: storage-layer integrity data can
remain clear for offline validation, while persisted API checksum metadata is
part of the protected object state.

### ETag

AWS docs explicitly say SSE-C ETags are not MD5. Our current ETag model is
already non-MD5, so SSE-C does not force a new ETag direction immediately.

However:

1. this plan does not solve the broader “ETag exact AWS compatibility” question
2. we should keep encryption work independent from any later ETag redesign

## Recommended Internal Crypto Shape

Use a small internal crypto layer with:

1. customer-key validator:
   - `validator = HMAC(validator_key, validator_salt || customer_key)`
2. key wrapping:
   - derive a KEK from the customer key and wrap salt
   - wrap the random object `DEK`
3. payload encryption:
   - `AES-256-GCM`
   - one AEAD operation per segment/part chunk

Nonce handling should be deterministic from stored object encryption state plus
chunk identity, so we do not need per-segment nonce columns.

## Testing Plan

### Unit and storage tests

1. request parser tests for all malformed/missing header combinations
2. encryption-state round-trip tests in storage
3. wrong-key validator tests
4. chunk encrypt/decrypt round-trip tests
5. range-read decrypt tests

### Integration tests in `s3-tests`

1. `PutObject` / `GetObject` / `HeadObject` happy path
2. wrong key on `GET` / `HEAD`
3. multipart initiate/upload/complete happy path
4. multipart with wrong key on `UploadPart`
5. `GetObjectAttributes` happy path for SSE-C
6. copy-source SSE-C tests
7. presigned SSE-C tests

### AWS compatibility tests

This work should include direct AWS verification for:

1. error codes
2. error status codes
3. required headers
4. response headers
5. multipart header rules

## Suggested Implementation Order

1. Add typed SSE-C request parsing and versioned validator-key config.
2. Add storage schema/type support for encryption state.
3. Add crypto helper for validator + wrap/unwrap + chunk AEAD.
4. Implement direct `PutObject` / `GetObject` / `HeadObject`.
5. Implement streamed `PutObject`.
6. Implement multipart initiate/upload/complete.
7. Protect persisted S3 checksum metadata under the SSE object envelope while
   keeping internal shard CRC64 cleartext.
8. Unignore `GetObjectAttributes` SSE-C test and implement attributes path.
9. Implement copy paths.
10. Add bucket-level SSE-C blocking support.

## Open Questions To Resolve Early

1. exact AWS error mapping for wrong vs missing vs malformed SSE-C headers
2. exact behavior of `CompleteMultipartUpload` with and without SSE-C headers
3. whether `POST Object` should ship in Phase 1 or Phase 2
4. whether we need a dedicated internal crate/module for encryption helpers, or
   whether this should live under `server-core` initially

## Summary

The main design choice is:

- treat `SSE-C` as the first concrete mode of a general object-encryption
  framework, not a special case bolted onto headers

That implies:

1. explicit `SSE-C` validator keys with stored key ids, not one shared
   encryption root
2. explicit encryption state in schema
3. wrapped per-object `DEK`s recoverable from the customer key alone
4. customer-key validation via salted HMAC
5. ciphertext shard CRC64 reuse for offline integrity

That is the cleanest way to implement `SSE-C` now without having to undo the
storage model when `SSE-S3` and `SSE-KMS` arrive later.
