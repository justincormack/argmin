# Remove The Unreachable Plaintext Payload Profile Plan

Status: in progress. Phases 0 and 1 are complete. Production write selection and active write
authority are encrypted-only, including terminal publication and provider-failure behavior. Phase
2's positive test-corpus migration is in progress. The design decision to remove the plaintext
payload state is accepted.

## Context

Argmin models the absence of an explicit bucket-encryption configuration using
`BucketEncryptionConfig::default_encryption = None`. That is an AWS control-plane state, not a
plaintext object setting. Its effective data-plane behavior is automatic SSE-S3 with AES-256.
Deleting the explicit bucket-encryption configuration restores the same automatic SSE-S3 default.
SSE-C is a separate request-selected mode and is blocked unless the effective bucket policy allows
it.

Consequently, every production S3 write currently resolves to one of these states:

- SSE-S3, selected explicitly or through the automatic bucket default; or
- SSE-C, selected by valid customer-key headers on a bucket that permits it.

If the managed key provider is unavailable, an implicit SSE-S3 write fails. It must never fall
back to plaintext. There is no AWS-facing request or bucket configuration that selects plaintext.

Despite this, the durable internal model still contains `ObjectEncryption::None`, an
object-encryption discriminator tag for `None`, plaintext segment-size branches, plaintext read
branches, and extensive test fixtures built around them. Phase 1 removed both the former
caller-selected `ActiveWriteEncryptionRef` finalization sentinel and
`ActiveWriteEncryption::None`; no active write path can now mint plaintext authority. The
chunked-encryption plan consequently still spends design effort keeping an unreachable durable
plaintext profile aligned with the encrypted profiles.

This plan removes that unsupported state before implementing the new chunked encrypted codec. The
repository is pre-release and supports neither existing installations nor format migration, so the
correct behavior for old plaintext durable state is current-format rejection before serving, not a
fallback reader.

## Relationship To Other Plans And Guides

- [`segment-chunked-encryption-plan.md`](segment-chunked-encryption-plan.md) should assume only
  encrypted payload codecs after this work. This removal should land before its profile-interface
  implementation.
- [`storage-upgrade-versioning-plan.md`](storage-upgrade-versioning-plan.md) remains future work and
  does not authorize a plaintext compatibility reader or migration.
- [`guides/versioning.md`](../guides/versioning.md) requires current readers to reject old formats
  and requires incompatible nested-format changes to advance every unversioned containing format.
- [`guides/storage-format-ledger.md`](../guides/storage-format-ledger.md) owns the exact version
  vector and immutable representation evidence that this change must update.
- [`encryption-compat-plan.md`](encryption-compat-plan.md) continues to own future SSE-KMS behavior.
  Adding another managed encryption mode must extend the encrypted state, not recreate plaintext.

## Goals

- Make an unencrypted persisted object, multipart upload, stream session, part, or segment
  unrepresentable in current Rust production types.
- Make every production write resolve explicitly to SSE-S3 or SSE-C, with no plaintext fallback.
- Reject former plaintext tags and old containing formats before shard IO, mutation, replay,
  recovery, response exposure, or serving.
- Remove plaintext-specific payload geometry, copying, read, range, copy, recovery, repair,
  reclaim, and scavenger branches.
- Preserve AWS's distinction between absent and explicit bucket-encryption configuration while
  retaining automatic SSE-S3 data-plane behavior for both.
- Migrate tests to states that can arise through production paths and prevent fake encrypted
  metadata from being paired with plaintext payload in end-to-end tests.
- Simplify the chunked-encryption design to one encrypted codec interface shared by SSE-S3 and
  SSE-C.

## Non-Goals

- Removing SSE-C or changing when it is permitted.
- Collapsing absent and explicit bucket-encryption configuration in S3 responses or policy
  evaluation.
- Implementing SSE-KMS.
- Migrating an old deployment or reading old plaintext payloads. No released deployment exists,
  and current policy supports only fresh current-format initialization.
- Changing AWS-visible ETag, checksum, range, multipart, or encryption-header behavior.
- Replacing the current whole-segment AES-GCM format with Cobblestone in this slice.
- Removing CRC64 acknowledgement, scrub, repair, or EC validation.

## Required Invariants

1. `ObjectEncryption` describes live payloads only and has only supported encrypted states. It has
   no `None` variant and no default that silently chooses a security mode. Payloadless records such
   as delete markers do not carry an `ObjectEncryption` value at all.
2. A production write cannot proceed until it owns either a valid managed write context or a valid
   SSE-C write context.
3. Missing managed-key configuration fails the request; it never changes payload protection.
4. The absence of explicit bucket encryption continues to resolve to AES-256 SSE-S3 and continues
   to report the AWS-observed control-plane state.
5. Durable object-encryption discriminator values 1 and 2 remain assigned to SSE-C and SSE-S3.
   Former plaintext tag 0 is reserved and rejected, not reassigned. Payloadless rows use an
   explicit payloadless schema shape, not tag 0 under another name.
6. Current-format decoders accept no plaintext object state, including tag 0 with absent state,
   while continuing to accept canonical payloadless delete markers.
7. Logical payload size and stored encrypted size are never conflated. Until the chunked-encryption
   work adds an explicit durable codec discriminator, all live payloads use the one fixed legacy
   encrypted geometry. Geometry APIs express that encrypted-only invariant directly and do not
   branch on whether an object is encrypted.
8. Metadata-only storage tests may use a storage-owned canonical encrypted-state fixture. Any test
   that decrypts, copies, ranges, reconstructs, or exposes payload bytes must use ciphertext and
   key state produced by the real encryption path.
9. Historical exact bytes remain immutable evidence. They may be used to prove rejection but are
   never accepted by production readers.

## Design

### 1. Keep bucket configuration distinct from payload state

Retain `BucketEncryptionConfig::default_encryption: Option<ManagedEncryptionAlgorithm>`. `None`
means that no explicit bucket configuration exists; `effective()` must continue to return AES-256
SSE-S3. Do not replace this option with an object-payload `None` state or make `GetBucketEncryption`
claim an explicit configuration where AWS reports none.

The request resolver becomes exhaustive over only two successful outcomes:

- valid SSE-C request -> `ActiveWriteEncryption::SseCustomer`; and
- otherwise -> `ActiveWriteEncryption::Managed(Aes256)`.

Remove its unreachable final `ActiveWriteEncryption::None` branch. Provider/configuration failures
return their existing AWS-facing failure rather than selecting another mode.

### 2. Make current object state encrypted-only

Change the durable logical type to contain only:

```text
ObjectEncryption = SseCustomer(SseCustomerObjectState)
                 | SseS3(SseS3ObjectState)
```

Remove `Default`, `is_encrypted()`, and every API whose meaning depends on a possible plaintext
variant. Replace `segment_ciphertext_extra_len()` and similar encryption booleans with an
encrypted-only legacy geometry API. This removal deliberately does **not** introduce the durable
payload-codec discriminator: SSE-C and SSE-S3 both use the single whole-segment AES-GCM geometry
until the chunked-encryption plan adds the codec field and profile-owned geometry. This avoids
inventing an interim discriminator whose complete persisted semantics would immediately change.

Similarly, remove `ActiveWriteEncryption::None` and `ActiveWriteEncryptionRef::None`. Resume and
copy paths must carry an exact stored SSE-S3 or SSE-C state. Any “no new write context” distinction
cannot be represented by another caller-selected sentinel.

Remove `write_encryption` from `AuthorizedFinalizeStreamPutRequest`. Storage must load the exact
stream session named by `session_id` under the admitted object route and derive an opaque,
owner-minted finalization capability bound to that session's bucket, key, session ID, reserved
generation, reservation identity, and exact SSE-S3 or SSE-C state. Only that capability may build
the terminal command. The HTTP/coordinator caller cannot select “use session encryption,” supply a
replacement encryption value, or reuse authority from another session. Crossed-session,
same-key/different-session, crossed-generation, and SSE-S3/SSE-C substitution tests must fail before
command-ID allocation, pending-slot mutation, shard access, or reservation release.

Keep discriminator values 1 and 2 stable. Remove `ObjectEncryptionType::None`; decoding byte 0 for
a live payload must produce a typed unsupported-plaintext-state failure. Do not map it to unknown
state and do not reuse it for a future codec or managed-key mode.

### 2a. Represent payloadless delete markers without an encryption sentinel

`StoredObject::DeleteMarker` is not a payload profile. Schema 7 must make the object encryption
columns nullable and constrain their presence by object status:

- a live row has `encryption_type IN (1, 2)` and a profile-valid non-null encryption state;
- a delete-marker row has both `encryption_type IS NULL` and `encryption_state IS NULL`; and
- no current row uses discriminator 0.

The row decoder must branch on the validated object status before decoding encryption. It decodes
and requires encryption only for `StoredObject::Live`; it requires the two columns to be null for
`StoredObject::DeleteMarker` and constructs the payloadless variant without calling
`parse_object_encryption`. Command, RPC, canonical-state, checkpoint, and diagnostic codecs must
likewise encode delete markers as their payloadless variant rather than embedding a placeholder
`ObjectEncryption` value.

Add current-format coverage for versioned and suspended delete-marker insertion, lookup, listing,
restart, replication, recovery, and removal. Malformed live rows with null encryption, live rows
with tag 0, delete markers with either encryption column present, and old carriers whose marker
uses tag 0 must all fail before mutation or shard IO. Unversioned deletion remains physical object
removal and must not manufacture a payloadless object row.

### 3. Remove plaintext payload execution branches

Inventory and remove all branches that:

- copy a logical segment directly into stored bytes;
- expose stored bytes directly as logical response bytes;
- calculate stored length without encrypted framing;
- skip DEK/key-state validation;
- special-case `ObjectEncryption::None` in PutObject, streaming PUT/POST, CopyObject,
  UploadPart, UploadPartCopy, multipart completion, GET, HEAD metadata projection, range reads,
  checksum handling, repair, reclaim, backfill, or scavenging; or
- interpret absent encryption state as a valid durable payload.

The storage layer may continue treating ciphertext as opaque bytes. It must nevertheless validate
that every durable object/session/upload command carries one of the two valid typed encryption
states and profile-consistent stored geometry before publication or recovery.

### 4. Advance every affected format coherently

Removing a formerly valid discriminator arm changes the accepted grammar even if tags 1 and 2
retain identical bytes. Before editing codecs, produce an exact carrier inventory from the storage
format ledger and production call graph. At minimum review and, where the dependency ledger
requires it, advance:

- the per-PG SQLite schema/catalogue and all encryption columns/constraints;
- metadata-command encoding and its all-variant aggregate;
- storage-node RPC encoding and every object/upload/session carrier;
- canonical metadata state and metadata checkpoint versions;
- restart, snapshot, command-log, repair, reclaim, backfill, scavenger, and diagnostics carriers;
- object, multipart-upload, stream-session, part, and read-snapshot codecs; and
- exact format-vector and dependency-ledger evidence.

The known directly affected baseline starts with this candidate vector. Phase 0 must confirm it
against the complete writer/reader registry before implementation:

| Format | Current | Expected decision |
| --- | ---: | --- |
| Per-PG SQLite schema/catalogue | 6 | Advance to 7 and reject schema 6 at open |
| Metadata-command encoding | 8 | Advance to 9; preserve the v8 aggregate as rejection evidence |
| Storage-node RPC frame/payload encoding | 22 | Advance to 23 because request/response payloads carry object encryption state |
| Canonical metadata state | 5 | Advance to 6 because its complete accepted logical state loses a variant |
| Metadata-command checkpoint | 3 | Advance to 4 with canonical-state 6 |
| Object SSE-C state | 3 | No byte-layout change expected; record the containing-format dependency decision |
| Object SSE-S3 state | 1 | No byte-layout change expected; record the containing-format dependency decision |
| Storage-RPC authentication binding/transport | 2 / 1 | No framing change expected; append the new inner-RPC composite evidence and retain the old vector |

This table is not permission to omit another carrier. Discovery of a direct container adds it to
the coordinated version vector before code changes land.

Do not infer this list from source filenames. Compare the complete production writer/reader
registry against the ledger and fail the slice if a carrier has no explicit bump-or-no-bump
decision.

For each advanced format:

- append new current evidence rather than rewriting historical evidence;
- retain the prior exact aggregate as rejection evidence;
- add correctly checksummed/authenticated old-version rejection through the real outer boundary;
- prove rejection occurs before mutation, replay, repair, shard IO, listener publication, or
  response commitment as appropriate; and
- do not add a migration or old plaintext reader.

The current PG catalogue must make plaintext rows structurally invalid through status-sensitive
encryption-column constraints. Schema 7 changes `encryption_type` from its current non-null,
default-zero shape to the live-or-payloadless shape in section 2a; it does not merely change the
allowed integer set. Opening schema 6 must fail at the schema-version boundary rather than
partially loading its tag-0 delete markers or plaintext rows.

### 5. Migrate tests without inventing impossible states

The initial audit finds roughly 500 direct `ObjectEncryption::None` uses, mostly in storage-owned
tests and model fixtures. Classify every use before replacement:

1. **Production-selection tests:** create buckets and requests through the real coordinator and
   assert the resulting object/session/upload uses SSE-S3 unless valid SSE-C was requested.
2. **End-to-end payload tests:** generate encryption state and ciphertext using production SSE-S3
   or SSE-C writers. Verify normal read/copy/range/decrypt behavior. Never change only metadata to
   look encrypted while retaining plaintext shard bytes.
3. **Storage metadata/command tests:** where storage deliberately treats payload bytes as opaque,
   use one storage-owned canonical encrypted-state fixture and encrypted stored-length shape. The
   fixture must be conspicuously limited to storage-owner tests and must not claim cryptographic
   validity.
4. **Property/model tests:** generate only SSE-S3 and SSE-C variants and make the variant choice
   compiler-exhaustive. Stored length, segment CRC, shard acknowledgements, and reclaim witnesses
   must follow the selected encrypted profile.
5. **Malformed-format tests:** retain old tag-0 bytes solely as immutable rejection inputs and
   prove they cannot reach application/recovery logic.
6. **Authorization-only tests:** use an opaque authorized encrypted write fixture or a real
   production authorization result rather than a public `ActiveWriteEncryptionRef::None` escape.

Positive canaries must cover both SSE-S3 and SSE-C for standard objects and multipart parts. Every
S3 write family needs at least an SSE-S3 production-path canary: buffered PutObject, promoted and
streamed PutObject, POST Object, CopyObject, CreateMultipartUpload plus UploadPart and completion,
and UploadPartCopy.

### 6. Enforce the reduced state space

Prefer compiler enforcement over source-name checks:

- removing enum variants makes old matches and constructors fail compilation;
- removing `Default` prevents tests and builders from silently selecting a mode;
- constructors require concrete encrypted state;
- interim geometry APIs encode the one fixed encrypted legacy representation rather than accept an
  encryption boolean; the later chunked-codec slice replaces them with codec capabilities; and
- raw test helpers cannot accept arbitrary encryption tags or optional encryption state.

Add a focused boundary check only if a semantically equivalent raw plaintext construction remains
possible through primitive fields after the type changes. Do not retain checks merely for deleted
symbol spellings.

## Implementation Slices

### Phase 0: Freeze the removal inventory

- Record every production and test use of the plaintext variants.
- Separately inventory every place where tag 0 is currently used as the payloadless delete-marker
  sentinel; those sites migrate to the schema-7 null shape rather than the encrypted fixture.
- Produce the authoritative containing-format bump/no-bump table.
- Pin a production S3 matrix proving absent configuration, deleted configuration, and explicit
  AES-256 configuration all write SSE-S3, while permitted SSE-C writes SSE-C.
- Pin provider-unavailable behavior as failure without payload mutation or plaintext fallback.
- Update this plan if any genuine production plaintext producer is discovered.

Exit: every current producer, consumer, durable carrier, and test class has an owner and intended
replacement.

#### Phase 0 inventory result (2026-10-18)

The source inventory found no genuine production plaintext producer. `BucketSummary` contains an
`EffectiveBucketEncryptionConfig`, whose default encryption is always AES-256, so the final
`ActiveWriteEncryption::None` branch in `resolve_write_encryption` was unreachable. Phase 1 removes
that fallback first and pins missing-provider failure before durable stream-session creation.

The initial exact source baseline is an audit aid rather than a permanent name-based boundary:

| Construction | Occurrences | Files | Owner and replacement |
| --- | ---: | ---: | --- |
| `ObjectEncryption::None` | 492 | 47 | `storage` owns the durable variant. Positive storage fixtures move to the canonical encrypted fixture or production ciphertext; server-core payload tests use real SSE-S3/SSE-C writes; delete-marker uses move to the payloadless row shape; only immutable malformed evidence remains. |
| `ActiveWriteEncryption::None` | 2 | 2 | `server-core` owns both the unreachable resolver/resume construction and the checksum-metadata branch. Remove them in Phase 1. |
| `ActiveWriteEncryptionRef::None` | 44 | 8 | `server-core` owns the caller-selected finalization sentinel. Remove the request fields and derive exact session-bound authority inside finalization. |
| `ObjectEncryptionType::None` | 6 | 2 | `storage` owns the durable tag. Reserve byte 0 as an explicitly rejected former plaintext tag in Phase 3. |

The production producer/consumer ownership is:

| Surface | Current role | Replacement owner |
| --- | --- | --- |
| Write selection and resume | `server-core::coordinator::{authz_types,streaming}` can produce or resume plaintext active state | Phase 1 makes new selection managed-or-SSE-C only and removes caller-selected resume/finalization sentinels. |
| Segment encryption, checksum sealing, read decryption, and copy compatibility | `server-core::coordinator::{object_state,streaming,runtime,copy}` branches on plaintext | Phase 1 closes new write authority; Phase 3 removes the remaining durable-reader branches after positive fixtures migrate. |
| Durable logical state and geometry | `storage::types` owns `ObjectEncryption`, tag 0, default construction, and plaintext geometry | Phase 3 removes the variant/default/tag and exposes fixed encrypted legacy geometry. |
| SQLite rows | PG schema 6 stores tags 0/1/2 in `objects`, `multipart_uploads`, and `stream_uploads`; object tag 0 is also the delete-marker sentinel | PG schema 7 requires tags 1/2 for every payload-bearing row and nullable encryption columns only for canonical delete markers. Multipart uploads and stream sessions remain payload-bearing and require tags 1/2. |
| Row loading, canonical state, checkpoint, command apply/recovery, repair, reclaim, backfill, and scavenging | `storage::{pg_store,metadata_command,storage_rpc,cluster,node}` decode or propagate the durable enum | Phase 3 advances the authoritative containers together and rejects old state before effects. |
| Tests and models | Storage metadata/command/model fixtures dominate the 492 positive constructions; server-core uses the active sentinel primarily in streaming-finalization tests | Phase 2 supplies storage-owned encrypted metadata fixtures and production ciphertext fixtures before Phase 3 removes the durable variant. Phase 1 migrates finalization tests with the active API. |

The complete direct containing-format decision is now frozen as follows. Self-versioned nested
carriers advance when their accepted grammar changes; outer wrappers whose grammar is unchanged do
not advance merely because they carry a newly versioned inner value, but receive new composite
evidence.

| Format | Baseline | Decision for removal | Reason/evidence obligation |
| --- | ---: | ---: | --- |
| Per-PG SQLite schema/catalogue | 6 | **7** | Encryption columns become live-or-payloadless and tag 0 becomes structurally invalid. Preserve schema-6 bytes as startup rejection evidence. |
| Applied metadata-command envelope | 8 | **9** | Commands and snapshots lose a formerly accepted nested encryption variant. Preserve the exact v8 aggregate and add recovery/replica rejection evidence. |
| Storage-node RPC frame/payload | 22 | **23** | Object, upload, session, command-build, and snapshot payload decoders lose tag 0. Add authenticated old/new server-boundary rejection evidence. |
| Canonical metadata-state digest | 5 | **6** | Object-row canonical values change from integer tag 0 to null for markers and reject plaintext live rows. |
| Metadata checkpoint payload | 3 | **4** | The checkpoint carries complete physical object/upload/session rows and its accepted logical row grammar changes. Preserve the exact v3 aggregate. |
| SSE-C object state | 3 | **3 (no bump)** | The tag-1 state bytes and cryptographic profile do not change; append containing-format dependency evidence. |
| SSE-S3 object state | 1 | **1 (no bump)** | The tag-2 state bytes and cryptographic profile do not change; append containing-format dependency evidence. |
| Storage-RPC authentication binding | 2 | **2 (no bump)** | Binding grammar is unchanged and carries a self-versioned v23 frame. Append exact inner-v23 composite and authenticated v22 rejection evidence. |
| Storage-RPC authenticated transport | 1 | **1 (no bump)** | Transport framing is unchanged and carries the independently versioned binding/frame. |
| Storage-node TLS protocol | `argmin-storage-rpc/1` | **no change** | Negotiation semantics and bytes are unchanged. |
| Runtime-map/config proof carriers | runtime config 5 and current proof-carrier versions | **no outer bump** | They carry the independently versioned canonical-state digest in an unchanged fixed-width carrier; append noncurrent-v5/current-v6 proof rejection and acceptance evidence. |
| Control-plane, Raft, standalone, and peering containers | current ledger vector | **no outer bump unless their encoded field grammar changes during implementation** | They carry self-versioned metadata commands, checkpoints, RPC frames, or proof carriers. Phase 3 must append composite evidence and re-audit the production writer/reader registry before landing; discovery of raw unversioned encryption fields changes this decision. |

Tag 0 currently has two distinct roles that must not be conflated during migration:

- `objects.status = 1` requires `encryption_type = 0` and null state for delete markers; this moves
  to nullable encryption columns and status-sensitive constraints in schema 7; and
- live objects, multipart uploads, and stream sessions may currently carry tag 0 as plaintext;
  those states are rejected rather than translated.

The Phase 0 exit condition is satisfied: all known production and test producers, consumers,
direct carriers, and payloadless-sentinel sites have an owner and replacement. Phase 3 retains a
mandatory final registry/ledger comparison so a newly introduced carrier cannot be omitted.

Preimplementation AWS evidence recorded on 2026-10-18:

| Selection/operation surface | Shared AWS-facing regression | Result |
| --- | --- | --- |
| Fresh implicit SSE-S3, explicit bucket AES256, implicit SSE-S3 after `DeleteBucketEncryption`, and request-explicit AES256 across PutObject, POST Object, CopyObject, multipart, and UploadPartCopy | `bucket_encryption::test_bucket_encryption_write_selection_matrix` | Passed locally and on AWS; not ignored |
| Fresh-default PutObject plus GET/HEAD response headers | `object_crud::test_put_get_head_object_response_shape` | Passed on AWS |
| Fresh-default POST Object | `post_object::test_post_object_inherits_bucket_default_sse_s3` | Passed on AWS |
| Fresh-default CopyObject | `copy_object::test_copy_object_response_shape` | Passed on AWS |
| Fresh-default CreateMultipartUpload, UploadPart, and completion | `multipart::test_multipart_flow_response_shape` | Passed on AWS |
| Fresh-default UploadPartCopy | `multipart::test_upload_part_copy_response_shape` | Passed on AWS |
| Permitted SSE-C PutObject across payload boundaries | `sse_c::test_sse_c_put_get_head_round_trip*` | Ten cases passed on AWS |
| Permitted SSE-C POST Object | `post_object::test_post_object_sse_c_round_trip` | Passed on AWS |
| Permitted SSE-C multipart across payload boundaries | `sse_c::test_sse_c_multipart_round_trip*` | Ten cases passed on AWS |
| Permitted SSE-C CopyObject and UploadPartCopy | `sse_c::test_sse_c_copy_object_round_trip`, `sse_c::test_sse_c_upload_part_copy_round_trip` | Passed on AWS |

These shared tests establish the AWS-visible selection contract before internal removal begins.
They do not prove the internal durable variant; Phase 2/3 production-path observations and
malformed-state tests own that evidence. Do not mark a future local mismatch ignored: preserve the
AWS expectation and treat local divergence as an implementation failure.

### Phase 1: Close write authority

- Remove `ActiveWriteEncryption::None` and `ActiveWriteEncryptionRef::None`.
- Make request resolution return only managed or customer encryption contexts.
- Thread the encrypted-only capability through direct, streaming, copy, and multipart writes.
- Remove the stream-finalization encryption request field and derive an opaque terminal capability
  from the exact loaded session.
- Add crossed-session, crossed-generation, and crossed SSE-S3/SSE-C finalization regressions.
- Add operation-family canaries and provider/configuration failure non-mutation tests.

The removal of the active-write variants and migration of all call sites that mention them is one
compiling slice; Phase 1 may not leave a removed variant referenced by tests.

Exit: no production command or stored record can be constructed from a plaintext write context.

#### Phase 1 implementation ledger

- The production resolver now returns only managed or SSE-C write authority. Missing managed-key
  configuration fails before stream-session creation rather than falling back to plaintext.
- `ActiveWriteEncryptionRef` and both finalization request fields that carried it have been
  removed. An admitted `ActivePutObjectRoute` now loads the exact durable session and mints a
  non-cloneable `AuthorizedStreamPutFinalization`. The capability borrows the exact admitted route,
  thereby binding its publication domain and immutable deadline as well as the bucket, key,
  session ID, target, encryption state, allocator position, cleanup cutoff, and stable reservation
  identity. The storage retry loop revalidates that complete subject against every authoritative
  snapshot before command preparation, including the post-apply reinspection path; neither a
  stale-command-build retry nor safe-abandonment reinspection can silently carry the old authority
  onto a changed session.
- Production finalization accepts only the `ActiveWriteEncryption` retained by the original
  opaque `AuthorizedPutObjectWrite` and requires its stored encryption state to equal the
  capability's session state. HTTP, buffered PutObject, promoted streaming PutObject, POST Object,
  and CopyObject can no longer provide a replacement encryption value or a no-context sentinel.
- `ActiveWriteEncryption::None` has been removed. The active-write type is now coordinator-private
  and opaque even to sibling modules: its private representation contains a concrete SSE-C or
  SSE-S3 state, and construction is possible only from a validated write context or the exact
  resumed SSE-C state. Segment encryption, checksum sealing, and stored-encryption projection are
  methods on that wrapper, so removing the named variant cannot be bypassed by pairing an encrypted
  enum variant with `ObjectEncryption::None`. A durable tag-0 session is rejected rather than
  resumed into mutation authority while the durable variant remains temporarily readable until
  Phase 3.
- Crossed managed-session authority, equivalent routes from another publication domain, and wrong
  SSE-C request-key regressions reject before terminal publication. A deterministic mutation in
  the snapshot-to-command gap also proves that reinspection rejects the changed allocator state
  without allocating or publishing a terminal command. A separate safe-abandonment regression
  changes the session before the post-apply snapshot load and proves its preparation callback is
  not invoked again. The capability's private representation and non-`Clone` property are also
  pinned by compile-fail coverage.
- Missing managed-key configuration now has non-mutation canaries for buffered PutObject,
  promoted and explicitly streamed PutObject, POST Object, CopyObject, CreateMultipartUpload,
  UploadPart, UploadPartCopy, and CompleteMultipartUpload. They verify the relevant destination
  object, stream session, multipart upload, and part state rather than asserting only the returned
  error. Together with the existing SSE-C missing-validator and wrong-key tests, these pin failure
  rather than plaintext fallback across every active write family.

Phase 1 is complete.

### Phase 2: Migrate the positive test corpus

Complete the roughly 500-call-site fixture migration while the old enum variant still exists, so
this phase compiles and tests independently:

- Replace metadata-only fixtures with the storage-owned encrypted-state fixture.
- Replace payload-bearing fixtures with production-generated ciphertext and state.
- Update property/model generators and expected fixed legacy encrypted geometry.
- Convert delete-marker fixtures to the logical payloadless variant rather than assigning an
  encryption sentinel.
- Remove raw plaintext test adapters and public/test-support escape hatches.
- Run coverage to identify encrypted branches lost during migration and add semantic canaries.

At this phase's exit no positive production, integration, property, or model test constructs
`ObjectEncryption::None`. The only remaining uses are the production definitions/branches being
removed in Phase 3 and immutable owner-local malformed/rejection evidence.

#### Phase 2 implementation ledger

- Storage now owns `metadata_only_test_encryption`, a canonical SSE-S3 state for tests that exercise
  metadata semantics without reading payload. Its name and documentation prohibit using its
  synthetic wrapped-key bytes in payload-bearing fixtures; those fixtures must continue through
  the production crypto path.
- The generic metadata-store suite, its state-machine/property generator, and the PgStore metadata
  suite now use encrypted state for their positive live-object, multipart-upload, part-session,
  listing, versioning, digest, and command-application fixtures. Direct SQL setup in that corpus
  writes the matching encoded encryption state rather than crossing an SSE-S3 discriminator with
  a null state.
- The first metadata-store slice removes 116 positive `ObjectEncryption::None` constructions.
- The node-client transport slice migrates another 56 request, response, route-capability,
  command-building, and installed Unix-RPC fixtures. These tests move typed metadata and commands
  but do not read or write payload under the synthetic key material; raw shard-transport tests are
  independent of the migrated encryption records.
- The storage-node capability/RPC slice migrates another 15 positive command-transport,
  capability-binding, and metadata-application fixtures. The frozen v22 plaintext wire specimen
  remains unchanged so Phase 3 can preserve it as immutable historical rejection evidence when
  the containing format advances.
- The owner-local command/type slice migrates another 11 proof-validation, command-matching,
  opaque-capability, replay, and placement-diagnostic fixtures. Stable command and encryption
  golden encodings remain unchanged for preservation as historical evidence during Phase 3.
- The first payload-bearing slice replaces the same-PG stream append atomicity fixture with a
  storage-owned production-compatible SSE-S3 segment fixture. Its wrapped DEK, ciphertext,
  logical/stored checksums, authentication-tag overhead, and 1+0 shard geometry are coherent;
  the test no longer pairs plaintext shard bytes with encrypted metadata or a partial 4+2 layout.
- The storage integration suite removes another 17 positive plaintext constructions. Tests that
  exercise independent metadata state use the explicitly metadata-only fixture; shard lifecycle,
  persistence, versioning, reclaim, multipart completion, UploadPart, and abort coverage now use
  coherent ciphertext, logical and stored checksums, and matching EC geometry. The production-
  compatible fixture now also models the multipart part-number nonce scope rather than reusing the
  ordinary object scope.
- The metadata-command support, storage-node RPC error-path, pending-command scavenger, and
  abandoned-recovery fixtures remove another eight positive plaintext constructions. These tests
  move or inspect metadata without reading payload, so they use the explicitly metadata-only
  encrypted state. Frozen command, storage-RPC v22, canonical-state v5/checkpoint v3, and tag-0
  golden evidence remains byte-for-byte unchanged until Phase 3 advances the owning formats.
- The local command and route slice removes another 32 positive plaintext constructions from
  stream-create fanout/recovery, multipart route and logical-observation, object-metadata
  validation, and durable allocator replay tests. These cases create, move, reject, or inspect
  metadata without exposing payload bytes; the three payload-bearing fanout cases remain for a
  production-compatible ciphertext migration.
- The recovery and multipart-management slice removes another 18 positive plaintext
  constructions from abandoned stream-create recovery, multipart create/convergence, empty-upload
  abort, and raced UploadPart-session creation tests. Payload-bearing append, completion cleanup,
  and uploaded-part abort cases remain coupled to their later ciphertext migration.
- The stream-session creation slice removes another ten positive plaintext constructions from
  exact applied-command matching, partial-apply retry, mismatch rejection, pending-command
  contention, and empty-session abort tests. These cases do not write or read payload; the adjacent
  append tests remain coupled to their production-compatible ciphertext migration.
- The empty-session recovery and finalization slice removes another 17 positive plaintext
  constructions from reopen/recovery, abort races, zero-byte finalization and reservation-proof
  binding, and segment-free malformed-proof rejection. Tests that stage or inspect non-empty
  payload remain coupled to their production-compatible ciphertext migration.
- The Unix metadata-route slice removes another eight positive plaintext constructions from
  multipart and stream metadata reads, historical recovery, scavenger validation, heartbeat
  renewal, append preparation, and stale/Peering session-create rejection. Unix tests that stage
  payload bytes remain coupled to their production-compatible ciphertext migration.
- The local multipart and stream-control slice removes another 37 positive plaintext
  constructions from empty-upload abort contention, UploadPart and PutObject session-create
  retry/recovery, reservation ownership, bucket-delete/scavenger coordination, heartbeat renewal,
  and segment-ID allocation fixtures. These tests exercise metadata allocation and convergence
  without writing shard payload; adjacent append, uploaded-part, and cleanup tests remain coupled
  to their production-compatible ciphertext migration.
- The direct-PUT route slice removes another 12 positive plaintext constructions. Payload-bearing
  route-ownership, pending-command, cleanup, deadline, response-loss, and versioned-convergence
  tests now shard production-compatible SSE-S3 ciphertext while keeping logical size and ETag CRC
  separate from stored size and CRC. Metadata-only crossed-proof and stale-snapshot fixtures use
  the explicitly synthetic encrypted state.
- The command-fanout slice removes another three positive plaintext constructions and migrates
  the adjacent shared-helper caller. Direct-PUT acknowledgement routing, metadata-command fanout,
  terminal-session cleanup, and streamed append fanout now persist coherent ciphertext and
  encrypted metadata. A shared encrypted direct-PUT request fixture preserves the logical/stored
  payload distinction for the remaining test migration.
- The shared committed-direct-segment slice removes another two positive plaintext constructions.
  The common fixture now persists coherent SSE-S3 ciphertext, and object-read lease, retained-route,
  Unix, placement, repair, backfill, scavenger, and replay tests explicitly distinguish logical
  plaintext metadata from reconstructed stored ciphertext. The encrypted empty-object health
  canary now covers its reachable tag-bearing representation rather than an impossible zero-byte
  stored segment. Corruption fixtures derive physical shard geometry from ciphertext and pin
  checksum corruption as same-length while testing short and long length corruption separately.
- The bounded state-model slice removes another four positive plaintext constructions. Direct-PUT
  ownership, cancellation, cleanup-progress, and overlapping-shard schedules now bind each staged
  payload and pending command to one coherent SSE-S3 ciphertext fixture while preserving plaintext
  logical size and ETag assertions. Multipart lifecycle traces now bind upload metadata to coherent
  SSE-S3 state and encrypt both streamed and copied parts with part-scoped nonces, logical sizes,
  and distinct stored and payload CRCs; each staged segment is reconstructed and compared with its
  exact ciphertext before terminal processing.
- The streamed-PutObject contention slice removes another 12 positive plaintext constructions.
  Pending-install, command-ID, abandonment, reissue, and overwrite tests now create an encrypted
  session and stage production-compatible ciphertext through one helper that keeps logical size,
  payload CRC, stored CRC, and durable encryption state coherent. The helper reconstructs every
  staged segment before publication. Direct-PUT contenders in the same schedules now also persist
  ciphertext and use the encrypted terminal request fixture, so the races contain no indirect
  plaintext winner.
- The stream-append recovery and cleanup slice removes another 18 positive plaintext
  constructions. Append preparation, partial apply, command-ID collision, authorized recovery,
  abandonment, duplicate-segment cleanup, missing-shard validation, and abort races now shard
  coherent SSE-S3 ciphertext while retaining distinct logical size, payload CRC, stored size, and
  stored CRC evidence. Successful/preserved payload cases reconstruct and compare the exact
  ciphertext; cleanup cases continue to inspect every acknowledgement and physical shard.
- The streamed-PutObject finalization slice removes another 23 positive plaintext constructions.
  Pending-command draining, terminal cleanup handoff, Peering rejection, ETag validation,
  matching-install races, versioned reservation, and exact-witness convergence now bind every
  payload-bearing session and committed object to coherent SSE-S3 ciphertext. Zero-byte sessions
  carry real managed encryption state without inventing a stored segment, while the unrelated
  abort-only fixture uses the explicitly metadata-only state.
- The streamed-UploadPart finalization slice removes another 11 positive plaintext constructions.
  Payload-CRC rejection, allocator cleanup, pending-command contention, partial-apply reopen,
  terminal-slot recovery, committed-response loss, and abort-winner cleanup now bind the upload,
  session, staged segment, and terminal part to one part-scoped SSE-S3 fixture. Preserved payload
  is reconstructed as exact ciphertext before the race, cleanup is checked with the encrypted
  stored length and CRC, and the payload-free foreign-terminal fixture uses the explicitly
  metadata-only state.
- The remaining 56 source occurrences are deliberately left for the shared multipart
  payload-bearing helpers, command-codec fixture migrations, Phase 3's production removal, and
  immutable rejection evidence. The adjacent same-part replacement and UploadPartCopy cleanup
  races still inherit the shared plaintext multipart seed and move with that atomic helper slice.
  Phase 2 is not yet complete.

### Phase 3: Atomically close durable and recovery state

- Remove `ObjectEncryption::None`, its default, and discriminator variant.
- Change schema 7 to require encryption tags 1/2 for live rows and null encryption columns for
  payloadless delete markers; reserve and reject old tag 0.
- Change row decoding to select live versus payloadless state before parsing encryption.
- Remove plaintext codec/geometry branches from storage, recovery, and maintenance paths.
- Advance the complete containing-format vector atomically and append immutable evidence.
- Add old-schema and old-carrier rejection tests at startup, RPC, command, recovery, and snapshot
  boundaries.

This is one atomic compile slice: enum removal, schema/codec changes, remaining production-branch
removal, and rejection fixtures land together. It may not leave the source in an intermediate
state where the enum is gone but positive tests still depend on it.

Exit: current durable live state accepts only SSE-S3 or SSE-C, canonical delete markers are
explicitly payloadless, and every prior plaintext or tag-0-marker carrier is rejected before
effects. No test passes by constructing a state production cannot create, except explicit
owner-local malformed rejection fixtures.

### Phase 4: Simplify the chunked-encryption design

- Remove `PlaintextWholeSegmentCrcV1` from the supported-profile registry and conformance matrix.
- Define the common codec interface only for the current whole-segment AES-GCM profile and the new
  Cobblestone profile, each paired with SSE-S3 and SSE-C key state.
- Remove plaintext benchmark and modern-only-profile deferrals.
- Update performance, storage, encryption, and format guides.
- Rerun the clean payload-read baseline after the state-space reduction if the hot path changes.

Exit: the chunked-encryption plan contains no obligation to preserve or emulate an unreachable
plaintext payload mode.

## Test Matrix

### AWS-visible write selection

| Bucket/request state | Expected stored state |
| --- | --- |
| No explicit bucket encryption; ordinary write | SSE-S3 AES-256 |
| Explicit AES-256 bucket encryption; ordinary write | SSE-S3 AES-256 |
| Encryption configuration deleted; ordinary write | SSE-S3 AES-256 |
| Explicit SSE-S3 request | SSE-S3 AES-256 |
| Valid permitted SSE-C request | SSE-C |
| SSE-C request when blocked | AWS-compatible rejection; no payload mutation |
| Managed key provider unavailable | Existing mapped failure; no payload mutation |

Run that matrix across PutObject, POST Object, CopyObject, and CreateMultipartUpload wherever the
operation accepts the corresponding selection. Managed-encryption selection is fixed by multipart
initiation: UploadPart, UploadPartCopy, and CompleteMultipartUpload do not accept a new managed
selection and must report/inherit the exact initiation result. The shared
`test_bucket_encryption_write_selection_matrix` exercises all four rows across those public paths,
including the inherited multipart responses and completed-object HEAD. Streaming PutObject is the
same PutObject protocol operation; local coverage must exercise both buffered and promoted storage
implementations without inventing a separate AWS selection contract.

### Payloadless delete-marker behavior

- versioning-enabled DeleteObject inserts a schema-7 payloadless numbered marker and makes it
  current;
- versioning-suspended DeleteObject inserts/replaces the canonical payloadless null marker;
- marker GET/HEAD, listing, explicit-version deletion, restart, replica apply, and recovery retain
  existing S3 behavior without decoding an encryption value;
- unversioned DeleteObject removes the live row and creates no marker; and
- marker paths perform no shard IO and cannot be used to construct a live object without concrete
  encrypted state.

### Durable rejection

- live row with former encryption discriminator tag 0 and absent state;
- live row with tag 0 and present state;
- live row with either encryption column null;
- delete-marker row with either encryption column non-null;
- canonical schema-7 payloadless delete marker as a positive canary;
- absent state for SSE-C and SSE-S3;
- old PG schema containing plaintext rows;
- previous metadata-command, RPC, canonical-state, and checkpoint aggregates;
- correctly authenticated/checksummed outer carriers containing former plaintext nested state;
- recovery/reissue/replica apply of a former plaintext command; and
- restart with prior current-format evidence.

Every case must assert no log advancement, pending-slot mutation, shard read/write/delete, repair
enqueue, reservation release, response exposure, or runtime-map publication, as applicable.

### Payload behavior

- zero-byte, sub-segment, exact-segment, multi-segment, and multipart objects under SSE-S3;
- corresponding SSE-C boundaries with correct, absent, and wrong customer keys;
- full GET, range GET, partNumber GET, CopyObject, and UploadPartCopy;
- checksum metadata present/absent and every supported checksum algorithm class;
- retained-route, missing-shard reconstruction, corruption, repair, reclaim, and scavenging;
- response-loss and recovery convergence for direct and streamed terminal commands; and
- encryption-state/codec crossings rejected before shard IO or plaintext exposure.

## Validation

- `cargo nextest run`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo fmt --all -- --check`
- `./scripts/check-storage-cluster-boundaries`
- `./scripts/coverage` for the migrated integration surface
- Ring and OpenSSL provider jobs
- format-ledger bump regressions and old-version startup/RPC/recovery rejection suites
- clean `./scripts/bench-payload-read` run if the reader hot path changes

## Completion Criteria

- No production or test-support type can represent a current plaintext payload.
- Delete markers and other payloadless records are represented explicitly without an encryption
  sentinel, and versioned/suspended delete behavior remains covered end to end.
- No AWS-facing operation can select plaintext, including on key-provider failure.
- The former tag 0 and all prior containing versions fail closed before effects.
- Both supported encrypted modes cover every write/read/copy/multipart operation family.
- Tests that observe payload bytes use real production encryption and decryption.
- The storage format ledger contains the complete new version vector and immutable prior evidence.
- The chunked-encryption plan and implementation contain no plaintext codec, benchmark, or
  conformance obligation.
- The full validation matrix passes under both crypto providers.
