# SSE-S3 Plan

## Goal

Implement AWS-compatible `SSE-S3` in a way that:

1. matches the S3 API and object behavior closely enough to unblock the real
   `SSE-S3` test surface
2. reuses and generalizes the existing `SSE-C` encryption foundation rather
   than creating a second unrelated crypto path
3. introduces only the minimum service-managed key handling needed for
   `SSE-S3`
4. keeps the internal key-management boundary explicit so later `SSE-KMS` can
   replace the temporary provider without redesigning object state

This plan intentionally does **not** try to implement real `SSE-KMS`.

## Recommendation

Do `SSE-S3` first, using a small service-managed key provider in `server-core`.

The temporary key-management model should be:

1. a distinct service-managed wrapping key role, separate from the existing
   `SSE-C` validator key
2. a random per-object data-encryption key (`DEK`)
3. wrapped `DEK` material stored in object metadata
4. a stored wrapping-key id from day one
5. optionally a tiny keyring shape with one active key plus older read-only
   keys

This is the minimum acceptable stopgap because it gives us:

1. correct `SSE-S3` object behavior
2. a rotation story that is at least structurally sane
3. the same envelope model we will need for `SSE-KMS`

## What Not To Do

Do **not** make the temporary system derive object keys deterministically from
account ids or bucket ids alone.

That is too broken because:

1. it collapses too many objects onto the same effective key material
2. it gives us no credible wrapping-key rotation story
3. it pushes us away from the existing per-object `DEK` envelope model
4. it makes later `SSE-KMS` migration harder rather than easier

If account or bucket identity is used at all, it should only be derivation
context or AEAD associated data, not the replacement for random `DEK`s.

## Current State

The existing `SSE-C` implementation already established the right basic shape:

1. per-object encryption state in storage metadata
2. random object `DEK`s
3. wrapped `DEK`s stored in metadata
4. payload encryption before erasure coding
5. encrypted checksum metadata
6. coordinator-controlled read/write encryption paths

Relevant current code:

1. `crates/server-core/src/sse.rs`
2. `crates/server-core/src/coordinator.rs`
3. `crates/storage/src/types.rs`
4. `crates/server-http/src/http/xml.rs`

Current gaps:

1. object encryption types only cover `None | SseCustomer`
2. bucket encryption config is still a stub focused on `SSE-C` blocking
3. there is no service-managed key provider abstraction
4. request parsing and response emission for `SSE-S3` are not implemented
5. policy condition support only covers `SSE-C`

## AWS Compatibility Surface To Match First

The first `SSE-S3` target should cover the behavior exercised by the upstream
`s3-tests` surface:

1. explicit `PutObject` with `x-amz-server-side-encryption: AES256`
2. bucket default encryption with `SSEAlgorithm=AES256`
3. default encryption applied to normal writes, multipart uploads, and `POST`
   object
4. `GetObject` and `HeadObject` responses carrying
   `x-amz-server-side-encryption: AES256`
5. read-side rejection when the caller sends managed-encryption request headers
   on operations where AWS rejects them
6. request conflict rejection for:
   - `SSE-C` plus `SSE-S3`
   - `SSE-C` plus `SSE-KMS`
   - `SSE-S3` plus KMS key id
7. bucket-policy condition support for `s3:x-amz-server-side-encryption`

This phase should **not** claim `SSE-KMS` compatibility.

## Proposed Internal Model

### 1. Introduce a managed key provider abstraction

Add an internal service boundary in `server-core` for managed encryption:

1. generate a random object `DEK`
2. wrap the `DEK` under a service-managed wrapping key
3. unwrap the `DEK` later for reads
4. expose a stable wrapping-key id

Suggested shape:

1. `ManagedKeyProvider`
2. `ManagedWrappingKeyConfig`
3. `ManagedDekEnvelope`

The first implementation is local and config-backed. Later `SSE-KMS` swaps in a
different provider behind the same boundary.

### 2. Keep key roles separate

Do not reuse the `SSE-C` validator secret as an `SSE-S3` wrapping key.

Key roles should stay distinct:

1. `SSE-C` validator key: validates customer-key identity only
2. managed wrapping key: protects service-managed `DEK`s for `SSE-S3`
3. future KMS provider credentials/config: selects and unwraps KMS-managed
   keys

### 3. Extend object encryption state

Generalize the storage enum from:

1. `None`
2. `SseCustomer`

to include at least:

1. `SseS3`

and reserve the internal model for a later `SseKms` variant.

For `SSE-S3`, stored state should contain:

1. encryption-state version
2. wrapping-key id
3. wrapped object `DEK`
4. wrap nonce and any other wrap parameters required by the local provider
5. segment nonce prefix
6. encrypted checksum-metadata blob and its nonce

Unlike `SSE-C`, `SSE-S3` state does not need any customer-key validation
material.

### 4. Keep the payload crypto model the same

Reuse the current approach:

1. encrypt plaintext before erasure coding
2. store ciphertext shards
3. validate shard CRC64 over ciphertext-at-rest
4. decrypt only after reconstructing and validating ciphertext

This is already the right model for the storage engine and should not diverge
between `SSE-C` and `SSE-S3`.

### 5. Generalize checksum metadata sealing

The current checksum-metadata path is shaped around `SSE-C`, but the same rule
should apply to `SSE-S3`:

1. checksum metadata remains protected under object encryption
2. sealed checksum metadata is part of the per-object encryption state
3. visible system metadata is reconstructed only after decrypting that state

The long-term target should be one “managed encryption” branch for:

1. segment encryption
2. checksum metadata sealing
3. decrypt-on-read

with `SSE-C` remaining a distinct mode because its access rules are different.

## Temporary Key Management

### Config

Add a separate service-managed key config, for example:

1. one configured active wrapping key
2. optional older wrapping keys retained for decrypt-only use

The exact env-var naming can be decided during implementation, but it must be:

1. separate from `ARGMIN_SSE_C_VALIDATOR_KEY`
2. versioned by stored key id
3. able to support later key rotation without a metadata redesign

### Acceptable temporary shape

The minimum acceptable implementation is:

1. one configured 256-bit wrapping key
2. stored internal key id, starting at `1`
3. support for reading objects only if their stored wrapping-key id is
   available

Better, but still small:

1. a tiny keyring config
2. one active wrapping key for new writes
3. zero or more old wrapping keys for reads

### Out of scope for this phase

1. external key stores
2. envelope rewrap workflows
3. customer-selectable KMS keys
4. bucket keys
5. automatic re-encryption of old objects

## Bucket Encryption

Bucket encryption must stop pretending to be implemented and become real for the
`SSE-S3` subset.

That means:

1. `GetBucketEncryption` returns
   `ServerSideEncryptionConfigurationNotFoundError` when unset
2. `PutBucketEncryption` persists a real default configuration for
   `AES256`
3. `DeleteBucketEncryption` clears that configuration
4. object writes without explicit encryption inherit the bucket default
5. existing `BlockedEncryptionTypes` support for `SSE-C` keeps working on top
   of the real default-encryption config

For this phase:

1. support `AES256`
2. continue rejecting `aws:kms`
3. continue rejecting `BucketKeyEnabled=true`

## HTTP And API Rules

### Write-side request handling

Support:

1. `x-amz-server-side-encryption: AES256` on write APIs
2. inherited `AES256` from bucket default encryption

Reject:

1. invalid managed-encryption algorithm values
2. `AES256` plus `x-amz-server-side-encryption-aws-kms-key-id`
3. `AES256` plus any `SSE-C` headers
4. `aws:kms` requests until real KMS support exists

### Read-side behavior

For `SSE-S3` objects:

1. `GetObject` and `HeadObject` succeed without extra request headers
2. responses include `x-amz-server-side-encryption: AES256`
3. sending managed-encryption request headers where AWS rejects them should
   fail with the matching request error shape

### Multipart

The encryption decision must be fixed at initiate time:

1. explicit `AES256` request encryption wins
2. otherwise the bucket default applies if present
3. uploaded parts inherit the stored multipart encryption state
4. `CompleteMultipartUpload` carries that state into the final object

### POST Object

`POST` object handling should inherit bucket-default `SSE-S3` even when the
form does not explicitly specify encryption, matching the upstream test
expectation.

## Bucket Policy

Add support for evaluating:

1. `s3:x-amz-server-side-encryption`

for write requests, alongside the existing `SSE-C` condition support.

This is needed for policies such as:

1. deny unencrypted uploads
2. deny uploads unless the algorithm is `AES256`

This phase does not need to implement:

1. `s3:x-amz-server-side-encryption-aws-kms-key-id`

because that belongs with real `SSE-KMS`.

## Implementation Phases

### Phase 1: Generalize encryption internals

1. add managed key-provider abstractions
2. add `SSE-S3` object encryption state
3. generalize segment encryption/decryption and checksum-metadata sealing
4. keep `SSE-C` behavior unchanged

Exit criteria:

1. new object metadata can represent `SSE-S3`
2. encryption/decryption paths work for a service-managed `DEK`
3. existing `SSE-C` tests still pass

### Phase 2: Explicit `SSE-S3` request support

1. parse and validate `x-amz-server-side-encryption: AES256`
2. reject conflicting request combinations
3. return `x-amz-server-side-encryption: AES256` on encrypted responses

Exit criteria:

1. explicit `PutObject(..., ServerSideEncryption='AES256')` works
2. invalid/conflicting request combinations fail correctly
3. read-side header misuse is rejected correctly

### Phase 3: Real bucket default encryption for `AES256`

1. persist bucket default encryption state
2. implement `GetBucketEncryption` / `PutBucketEncryption` /
   `DeleteBucketEncryption` for the `AES256` subset
3. apply bucket-default `SSE-S3` to `PutObject`, multipart initiate, and `POST`
   object

Exit criteria:

1. unset buckets return the not-found encryption error
2. set buckets return real persisted config
3. inherited `SSE-S3` works for standard writes, multipart, and `POST`

### Phase 4: Bucket-policy integration

1. add policy request fields for managed encryption algorithm
2. evaluate `s3:x-amz-server-side-encryption`
3. cover explicit and inherited `SSE-S3` behavior

Exit criteria:

1. deny-unencrypted bucket policies work
2. deny-wrong-algorithm bucket policies work
3. inherited bucket-default encryption interacts correctly with policy checks

### Phase 5: Follow-up cleanup for KMS readiness

1. verify the temporary managed-key provider boundary is sufficient for later
   `SSE-KMS`
2. separate any remaining `SSE-S3`-specific logic from provider-independent
   managed-encryption logic
3. leave `aws:kms` request acceptance for the later KMS plan

Exit criteria:

1. no fake `aws:kms` mode exists
2. the next step toward `SSE-KMS` is adding a real provider, not redesigning
   object crypto state

## Validation

When implementation starts, validate at least:

1. existing `SSE-C` tests still pass
2. the `SSE-S3` test scope should match the current `SSE-C` test surface
   wherever the API semantics overlap, plus any `SSE-S3`-specific cases
3. focused new tests for:
   - explicit `AES256` uploads
   - bucket-default `AES256` uploads
   - `HeadObject`/`GetObject` response headers
   - multipart inheritance
   - `POST` inheritance
   - conflict rejection
   - bucket policy conditions
4. upstream `s3-tests` cases covering:
   - bucket encryption `AES256`
   - default `SSE-S3` uploads
   - explicit `SSE-S3` uploads
   - `SSE-S3` bucket-policy enforcement

Relevant target commands once tests exist:

1. `cargo test -p s3-tests --test bucket_encryption`
2. `cargo test -p s3-tests --test bucket_policy`
3. `cargo test -p s3-tests --test multipart`
4. `cargo test -p s3-tests --test post_object`
5. AWS/local upstream harness cases for `SSE-S3`

## Explicit Deferrals

The following are intentionally deferred to the later `SSE-KMS` plan:

1. accepting `aws:kms` object writes
2. persisting and returning KMS key ids
3. bucket default encryption with `aws:kms`
4. KMS policy condition keys
5. bucket keys
6. any external key-management service

## Success Condition

This plan is complete when Argmin has a real `SSE-S3` implementation backed by
a small but structurally correct service-managed key provider, and the next
increment to `SSE-KMS` is “replace or extend the provider” rather than “redo
the whole encryption model”.
