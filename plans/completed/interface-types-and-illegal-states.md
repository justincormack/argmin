# Interface Types and Illegal States Plan

## Status

Complete.

What landed:

1. public storage/core interfaces now use typed enums and newtypes for the
   important primitive domains:
   - `VersionId`
   - `BucketVersioningState`
   - `ObjectState`
   - `EtagKind`
   - `StorageClass`
   - `BucketName`
   - `ObjectKey`
   - `UploadId`
   - `SessionId`
2. storage-facing object records and write requests are variant-based:
   - `StoredObject`
   - `LiveObjectRecord`
   - `DeleteMarkerRecord`
   - `PutObjectReq`
   - `PutLiveObjectReq`
   - `PutDeleteMarkerReq`
   - `ObjectLayout`
3. checksum interfaces are typed end-to-end at the boundary:
   - `ChecksumClaim`
   - `EncodedChecksumClaim`
   - typed multipart-complete part checksums
4. metadata/tag boundaries are explicit with:
   - `SerializedMetadataBlob`
   - `SerializedTagSet`
5. read/write/delete/copy condition types are all parsed into typed condition
   values before they reach coordinator logic
6. streaming contexts now use typed bindings and checksum contracts instead of
   loose related string fields

Explicitly deferred:

1. introducing a separate shared leaf crate for these boundary types
   - the current wrapper/newtype approach is sufficient
   - this can be revisited later only if cross-crate duplication becomes
     meaningfully painful

## Constraint

`storage` must remain independent of `server`.

That means:

1. `storage` must not depend on `server` modules such as `metadata_blob`.
2. Cross-layer types must either live in `storage`, or in a small leaf crate that
   both `storage` and `server` can depend on.
3. Serialization details should not leak upward unless we deliberately choose an
   opaque serialized wrapper type.

## Why this work is needed

Recent refactoring tightened several important boundaries, especially around
streaming uploads, checksum handling, and internal object layout. The remaining
interface issues are mostly structural:

1. Core storage records are still "bags of fields" with correlated invariants.
2. Several cross-layer interfaces still use raw primitives (`u8`, `Vec<u8>`,
   `String`, tuples of options) where the valid combinations are much smaller.
3. HTTP parsing guarantees are not consistently preserved in coordinator-facing
   request types.
4. Metadata, tags, checksums, and ETags still cross boundaries in partially
   serialized or partially parsed forms.

The result is that many important invariants are currently enforced only by
comments, checks, or repeated pattern matching.

## Goals

1. Make illegal object states unrepresentable in the `coordinator <-> storage`
   interface.
2. Preserve parse-time guarantees in the `http <-> coordinator` interface.
3. Remove raw discriminants and naked correlated `Option` fields from public
   interfaces.
4. Keep storage independent from server-specific parsing and XML/header logic.
5. Improve readability by making data model intent visible in type signatures.

## Non-goals

1. Reworking on-disk layout or adding backward-compatibility migrations.
2. Redesigning the external S3 API surface.
3. Adding external dependencies.
4. Fully rewriting `http` request/response handling in one step.

## Design principles

### Sum types over correlated fields

If two or more fields are only valid in certain combinations, prefer an enum.

### Newtypes over raw primitives

If a `u8`, `u64`, `Vec<u8>`, or `String` has domain meaning, wrap it.

### Parse once, validate once

If HTTP parsing rejects invalid forms, later layers should not accept those
forms again through a weaker type.

### Opaque serialized wrappers are acceptable

Where fully typed sharing would force the wrong dependency direction, prefer an
explicit wrapper like `SerializedMetadataBlob(Vec<u8>)` over a naked `Vec<u8>`.

## Recommended sequence

Completed.

## Phase 1: Replace raw primitive state with enums and newtypes

Status: largely complete.

This phase is low-risk and should happen before bigger model changes.

### Replace raw discriminants

Introduce strongly typed values for fields that are currently raw primitives in
public storage-facing types:

1. `ObjectState` instead of `status: u8`
   - `Live`
   - `DeleteMarker`
2. `BucketVersioningState` instead of `versioning: u8`
   - `Disabled`
   - `Enabled`
   - `Suspended`
3. `EtagKind` instead of `etag_kind: u8`
   - `Crc64`
   - `MultipartComposite`
4. `StorageClass` instead of `storage_class: u8`
5. `VersionId` instead of raw `u64`
   - preferred shape: `enum VersionId { Null, Versioned(NonZeroU64) }`

### Replace naked byte/string identifiers where helpful

These are lower priority, but worth introducing when touching the call sites:

1. `UploadId`
2. `SessionId`
3. `BucketName`
4. `ObjectKey`

These can begin as simple tuple newtypes without validation logic.

## Phase 2: Replace object bag-of-fields types with variant-based models

Status: largely complete.

This is the biggest improvement for illegal states.

### Current issue

`ObjectRecord` and `PutObjectMetaReq` currently expose correlated fields such as:

1. `status`
2. `data_layout`
3. `parts_count`
4. `metadata_blob`
5. `etag_kind`

Even with validation, the public type still allows invalid combinations.

### Target model

Replace the current public object metadata model with something closer to:

```rust
pub enum StoredObject {
    Live(LiveObjectRecord),
    DeleteMarker(DeleteMarkerRecord),
}

pub struct LiveObjectRecord {
    pub id: ObjectId,
    pub size: u64,
    pub etag: ObjectEtag,
    pub storage_class: StorageClass,
    pub ec: EcShape,
    pub layout: ObjectLayout,
    pub metadata: SerializedMetadataBlob,
    pub tags: Option<SerializedTagSet>,
    pub last_modified: u64,
}

pub struct DeleteMarkerRecord {
    pub id: ObjectId,
    pub last_modified: u64,
}

pub enum ObjectLayout {
    ChunkManifest,
    MultipartManifest { parts_count: NonZeroU32 },
}
```

### Effects

This removes the following illegal states from the interface:

1. delete marker with multipart layout
2. delete marker with multipart part count
3. delete marker with tags
4. multipart object with missing part count
5. chunk-manifest object with part count
6. object with raw `status` that every caller must inspect manually

### Write-side pair

Do the same for write requests. Instead of `PutObjectMetaReq` as a bag of
optional fields, make the write intent explicit:

1. `PutLiveObjectReq`
2. `PutDeleteMarkerReq`
3. or one enum `PutObjectMetaReq::Live(...) | DeleteMarker(...)`

`PutDeleteMarkerReq` should likewise have no tag field.

## Phase 3: Make checksum and ETag interfaces typed

Status: complete.

What is done:

1. `ObjectEtag` is in place
2. `ChecksumClaim` exists and is used in several HTTP/core paths
3. edge parsing already converts checksum headers into typed claims in several
   operations

No remaining required work in this phase.

Several interfaces still use correlated option tuples:

1. `Option<(ChecksumAlgorithm, String)>`
2. `Option<(ChecksumAlgorithm, Vec<u8>)>`
3. `checksum_algorithm: Option<_>` plus `checksum_bytes: Option<_>`
4. `etag: Vec<u8>` plus `etag_kind: ...`

### Target checksum model

The important distinction is not "encoded vs raw". It is:

1. a checksum claim/expectation parsed from HTTP or XML
2. a verifier built from that claim
3. a verified checksum result used internally and for storage

Use raw bytes internally once parsing succeeds.

Suggested shape:

```rust
pub struct RawChecksum {
    pub algorithm: ChecksumAlgorithm,
    pub bytes: Vec<u8>,
}

pub struct ChecksumClaim {
    pub algorithm: ChecksumAlgorithm,
    pub expected: Option<Vec<u8>>,
}

pub struct VerifiedChecksum {
    pub algorithm: ChecksumAlgorithm,
    pub bytes: Vec<u8>,
}
```

With construction and verification APIs along the lines of:

```rust
impl ChecksumClaim {
    pub fn from_base64(
        algorithm: ChecksumAlgorithm,
        expected_b64: Option<&str>,
    ) -> Result<Self, ChecksumError> { ... }

    pub fn verifier(self) -> ChecksumVerifier { ... }
}

impl RawChecksum {
    pub fn to_base64(&self) -> String { ... }
}

impl ChecksumVerifier {
    pub fn update(&mut self, data: &[u8]);
    pub fn finish(self) -> Result<VerifiedChecksum, ChecksumMismatch>;
}
```

Then use:

1. `ChecksumClaim` at the `http` boundary instead of `(algo, String)`
2. `VerifiedChecksum` or `RawChecksum` below `http` instead of `(algo, bytes)`
3. `UploadPartResult { etag, checksum: Option<VerifiedChecksum> }`
4. `CompletePart { part_number, etag, checksum: Option<ChecksumClaim> }`

This keeps base64 parsing at the edge and avoids passing untrusted checksum
strings into `coordinator` or `storage`.

### Target ETag model

Wrap object ETags in a type that knows how to format itself:

```rust
pub enum ObjectEtag {
    SinglePart([u8; 8]),
    MultipartComposite { crc64: [u8; 8], parts: NonZeroU32 },
}
```

This removes the need to pass `etag` bytes and `etag_kind` separately through
most of the system.

## Phase 4: Tighten the `http -> coordinator` request boundary

Status: complete.

Coordinator entry points are now explicit request structs, and the remaining raw
checksum/condition/streaming-context gaps called out in this phase have been
closed.

### Current issue

`S3Request` is intentionally generic, but coordinator-facing calls still take
many loosely related values that were already parsed from headers/query/body.

Examples:

1. conditions are still raw optional strings
2. checksum claims are still tuples
3. streaming contexts carry several related fields separately
4. duplicate-header handling is scattered in helper functions

### Target shape

Keep `S3Request` as the low-level HTTP representation, but do not pass its raw
data shape past `http`.

Introduce per-operation parsed request types for coordinator entry points, for
example:

1. `ParsedPutObject`
2. `ParsedCopyObject`
3. `ParsedUploadPart`
4. `ParsedCompleteMultipartUpload`
5. `ParsedDeleteObject`

Each parsed request should preserve parse-time guarantees.

### Conditions

Replace raw condition structs with supported-form enums, for example:

```rust
pub enum WriteCondition {
    None,
    IfMatch(ObjectEtag),
    IfNoneMatchStar,
}
```

For reads and copy-source conditions, use typed fields rather than raw strings
once parsing succeeds.

### Checksum parsing contract

`http` should parse checksum strings into `ChecksumClaim` immediately.

After that:

1. `coordinator` should receive checksum claims or verified checksums, not raw
   base64 strings
2. streaming and buffered paths should share the same verifier model
3. response rendering should base64-encode from `RawChecksum` or
   `VerifiedChecksum` at the edge

### Header storage

Do not make header normalization itself the project. Instead:

1. keep `S3Request` generic for the serve/auth/router layer
2. parse into operation-specific structs before calling coordinator
3. confine duplicate-header handling to the `http` layer

This yields most of the type-safety benefit without a large HTTP rewrite.

## Phase 5: Metadata and tag typing while preserving storage independence

Status: complete.

This needs an explicit decision because `storage` must stay independent.

### Option A: Opaque serialized wrappers first

Recommended first step:

1. replace naked `Vec<u8>` metadata blobs with `SerializedMetadataBlob`
2. replace naked tag XML strings with `SerializedTagSet`
3. keep serialization/deserialization local to the boundary layers

This immediately makes the wire/storage format explicit without creating a new
crate.

### Option B: Small shared leaf crate later

If repeated parse/serialize churn remains painful, move these types into a leaf
crate with no dependency on `server` or `storage` internals:

1. `Metadata`
2. `TagSet`
3. `ObjectEtag`
4. checksum value types
5. condition value types

This keeps `storage` independent from `server` while still allowing a typed
shared model.

### Recommendation

Do not start with a new crate. Start with opaque serialized wrappers, and only
introduce a leaf crate if the remaining duplication is still substantial after
Phases 1-4.

## Phase 6: Clean up streaming-specific interfaces

Status: complete.

Recent work already improved `StreamUploadTarget`. The next step is to carry the
same approach through the remaining streaming interfaces.

### Improve streaming contexts

Examples:

1. `StreamingPartContext` should carry a typed session binding object instead of
   separate `bucket`, `key`, `upload_id`, and `part_number`.
2. checksum negotiation should be a dedicated type rather than:
   - `upload_checksum_algorithm`
   - `claimed_checksum`
   - `checksum_response`

### Suggested direction

```rust
pub struct StreamPartBinding {
    pub session_id: SessionId,
    pub target: StreamUploadTarget,
    pub bucket: BucketName,
    pub key: ObjectKey,
}

pub struct ChecksumContract {
    pub claim: Option<ChecksumClaim>,
    pub response_headers: ChecksumResponseHeaders,
}
```

This keeps coordinator finalize APIs from accepting mismatched pieces that then
need to be revalidated together.

## Rollout plan

Completed.

## Verification

Each phase should include:

1. compile-time reduction in raw primitive fields at public boundaries
2. targeted unit tests for constructors and conversions
3. storage-layer tests for "invalid state rejected at construction"
4. coordinator/http tests showing fewer runtime mismatch branches are needed
5. `cargo test --workspace`
6. `cargo clippy --workspace --all-targets -- -D warnings`

## Success criteria

This work is successful when:

1. public `storage` interfaces no longer expose raw object lifecycle/layout
   combinations
2. coordinator APIs accept parsed request objects instead of ad hoc parameter
   groups
3. the remaining checksum-bearing interfaces stop using raw tuple/string forms
4. metadata/tag boundaries are made explicit, whether by wrappers or a later
   shared crate
5. storage remains independent from server-specific modules
6. the number of runtime "this combination should never happen" checks is
   materially reduced
