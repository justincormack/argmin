# Segment-Level Chunked Encryption And Authenticated Range Reads Plan

Status: In progress (Phase 0)

## Context

Argmin currently encrypts each internal payload segment as one AES-256-GCM record. Newly written
standard-object and multipart-part payloads use an 8 MiB internal segment size, so a small range
read can require reading, assembling, and authenticating an entire segment. The storage layer also
validates complete shard bytes against durable CRC64 acknowledgements before returning the segment.
This remains true even though a shard-range RPC exists: that RPC currently reads and hashes the
whole shard before slicing the requested range.

The C2SP [chunked-encryption specification](https://c2sp.org/chunked-encryption) defines
authenticated streaming and random-access encryption with a predictable plaintext/ciphertext
mapping. Its recommended `Cobblestone-128` profile and compliance-oriented `Cobblestone-256`
profile use fixed 16 KiB plaintext chunks and one AEAD tag per chunk. The format also includes a
random salt, key commitment, explicit final-chunk semantics, and caller-provided context bound
through key derivation.

This is potentially a large improvement for small `Range` requests: the cryptographic
authentication unit would fall from as much as 8 MiB to 16 KiB. It does not improve the complete
read path by itself. Unless storage can read only the corresponding healthy systematic shard
ranges, the same disk IO and most of the same integrity work still occur.

This plan treats encryption format, logical-subject authentication, and healthy systematic partial
shard IO as one design. It does not introduce a chunked cipher while leaving its useful
normal-path random-access property unreachable. Partial EC reconstruction is a later, independently
measured optimization; the initial degraded path deliberately falls back to the current full
segment validation and reconstruction machinery.

## Provisional Decision

The preferred design, subject to the measurement and specification-stability gates below, is:

1. One Cobblestone message per existing Argmin payload segment, never one message per S3 object.
2. `Cobblestone-256`, retaining Argmin's AES-256 choice for SSE-S3 and SSE-C payloads.
3. The full Cobblestone format, including salt and key commitment; do not expose or use its raw
   mode for persisted object payloads.
4. A canonical, versioned Argmin encryption context binding every segment to its immutable logical
   object/part identity.
5. Successful AEAD authentication is the normal-path data-integrity proof for encrypted reads.
   Durable shard CRCs remain for write acknowledgement, scrub, corruption localization, repair,
   scavenging, and diagnostic fallback.
6. S3-visible object and part checksums remain independent end-to-end client integrity metadata.
7. A missing, unavailable, malformed, or unauthenticated required shard leaves the partial path
   and uses full-segment physical validation/reconstruction. General partial EC reconstruction is
   not an initial-format completion requirement.
8. The current format remains authoritative until partial IO and benchmark gates demonstrate a
   material range-read benefit without an unacceptable sequential-read regression.

An 8 MiB segment is comfortably below Cobblestone's message limit. Treating each segment as a
message preserves the existing publication, recovery, reclaim, multipart, and EC units. Treating a
complete S3 object as one message would not: S3 objects can be much larger, and multipart parts are
written, replaced, and recovered independently before their final order is committed.

The C2SP document is currently labelled a development specification. Before making it the only
writer format, record the exact reviewed revision or stable profile and decide how future upstream
changes affect Argmin's persisted profile. An Argmin format identifier must never silently mean
"whatever the current C2SP main branch says."

### Reviewed specification and implementation choice

The Phase 0 design baseline is the C2SP `chunked-encryption.md` file at commit
[`d757bb90e5966d92d17353a3964ca1af776b63e0`](https://github.com/C2SP/C2SP/blob/d757bb90e5966d92d17353a3964ca1af776b63e0/chunked-encryption.md),
dated 2026-08-24. As checked on 2026-10-17, C2SP still labels the document a development
specification and has no chunked-encryption release tag. The pinned commit is therefore review
evidence, not approval to make the profile Argmin's sole writer. Before enabling that writer,
compare the pinned text and vectors with the then-current stable specification or, if it remains
untagged, repeat and record the upstream review. The durable Argmin profile identifier always
means the pinned construction; an upstream change requires a new explicit compatibility decision
and may require a new Argmin profile identifier.

The independently versioned Wycheproof baseline is commit
[`b61843a9a5115bb758134b6a1f5d5e502d445342`](https://github.com/C2SP/wycheproof/blob/b61843a9a5115bb758134b6a1f5d5e502d445342/testvectors_v1/c2sp_chunked_encryption_aes_256_gcm_test.json).
The SHA-256 digest of
`testvectors_v1/c2sp_chunked_encryption_aes_256_gcm_test.json` at that commit is
`0f8e35568d59649fb55fb0e5602dc678dc8910d6822beef4945aee0b5994ef05`. Import the fixture
verbatim with this provenance and assert its digest rather than following C2SP's mutable
Wycheproof `main` link. A later vector revision is additive evidence and does not rewrite the
imported corpus.

Do not add a production Cobblestone dependency in the initial implementation. The reviewed C2SP
page lists Go and Python implementations but no Rust implementation. Implement the small
provider-neutral construction inside `argmin-crypto`, using its existing AES-256-GCM and SHA-512
provider abstractions and adding only the narrowly scoped HKDF-Expand-SHA-512 primitive the
profile requires. Both Ring and OpenSSL builds must consume the same pinned Wycheproof corpus and
pass bidirectional persistent-format interoperability tests. Reconsider a dependency only if a
mature, audited Rust implementation appears and passes the repository's dependency and
provider-interoperability review.

## Goals

- Reduce healthy encrypted range reads to the header and authenticated chunks intersecting the
  requested plaintext range.
- Preserve availability through the existing full-segment reconstruction path when a required
  systematic shard range is unavailable or suspect.
- Cryptographically bind payload bytes to the exact durable bucket, key, generation, part, and
  segment rather than only to a random DEK and position.
- Preserve independent S3 checksum behavior and all AWS-visible SSE-S3/SSE-C semantics.
- Retain durable shard acknowledgements and deterministic repair evidence.
- Preserve bounded memory and streaming writes.
- Use one encryption-context and payload-codec implementation across PutObject, POST Object,
  CopyObject, CreateMultipartUpload/UploadPart/CompleteMultipartUpload, and UploadPartCopy.
- Route encrypted and plaintext payloads through one profile-neutral write/read orchestration
  contract, with profile-specific integrity and range implementations behind that boundary.
- Make the payload profile explicit and versioned in durable metadata, commands, RPCs, fixtures,
  diagnostics, and the storage format ledger.

## Non-Goals

- Encrypting an entire S3 object or completed multipart manifest as one Cobblestone message.
- Changing S3's checksum, ETag, range, or encryption-header behavior.
- Removing shard CRC64 acknowledgements or the scrub/repair machinery.
- Providing efficient unauthenticated partial reads for unencrypted payloads. That requires a
  separate chunk-checksum or authenticated-tree design.
- General partial EC reconstruction in the initial implementation. A later optimization may add it
  for already-known erasures without changing the persisted encryption profile.
- Binding payload encryption to physical placement, route epochs, nodes, PGs, EC shape, or shard
  indices.
- Re-encrypting payload merely because it is repaired, backfilled, or moved.
- Adding a production crypto dependency without a separate dependency review.

## Common Payload-Profile Boundary

Do not preserve the current shape in which the plaintext branch copies bytes while encrypted
branches are special-cased before and after the otherwise shared storage path. Separate two
concerns explicitly:

1. the AWS-visible encryption mode and key state (`None`, SSE-S3, or SSE-C); and
2. the internal, versioned payload protection/codec profile used to frame, verify, range-map, and
   stream each segment.

Use one sealed profile interface or compiler-exhaustive enum dispatch for both encrypted and
plaintext payloads. The common orchestration owns exact logical subjects, range selection,
retained/deletion-exclusion leases, response commitment, bounded output chunks, cancellation, and
the rule that a body is successful only after profile finalization. Each profile implementation
owns:

- stored-size calculation and framing;
- incremental write encoding and finalization;
- mapping a plaintext range to required stored spans;
- incremental read verification and the point at which a plaintext chunk may be emitted;
- final logical-integrity and physical-health results; and
- profile-specific malformed-input and late-failure classification.

The interface must not require every profile to have identical cryptographic strength or IO
granularity. It must give the HTTP, copy, and storage orchestration one bounded state machine with
the same cancellation and error contract. A profile may internally buffer a complete segment; it
may not reintroduce a second raw-`Vec` publication or read path around that contract. Conformance
tests run the same write, full-read, range, copy, cancellation, retained-route, and late-error
scenarios against every enabled profile and assert the intended profile-specific release boundary.

### Plaintext profile policy

Do not implement plaintext as AEAD under a fixed, empty, or otherwise public "null key." Such a tag
would consume encryption CPU and framing overhead while providing no meaningful authentication,
and would obscure rather than describe the persisted security property. A secret MAC-only design
would be a distinct keyed-integrity format and needs its own key lifecycle and threat-model review.

For the initial Cobblestone work, keep the current plaintext format behind an explicit
`PlaintextWholeSegmentCrc`-style profile adapter. Full and range reads retain their current physical
validation guarantees; the adapter may consequently buffer or read more than the encrypted
profile. That performance difference is intentional, measurable, and documented rather than
hidden in shared orchestration.

If unencrypted range workloads later justify equivalent granularity, design a separate,
explicitly versioned plaintext chunk-integrity profile, for example 16 KiB plaintext records with
per-record checksums bound to segment position and exact logical subject. CRC or an unkeyed digest
would detect accidental corruption but would not become authentication merely by sharing the
Cobblestone geometry. Measure metadata/framing overhead, checksum cost, range IO, and corruption
semantics before selecting that profile. It is an independent format project, not a prerequisite
for the encrypted range improvement.

An incremental plaintext full-reader could lower TTFB without changing the persisted format by
rolling CRC while streaming and retaining enough trailing output that a final mismatch terminates
below the declared `Content-Length`. That permits callers to observe earlier bytes before a late
body error and is therefore weaker than per-record authenticated release. Adopt it only through an
explicit profile contract and client matrix; do not obtain it accidentally by moving the existing
final CRC check after response completion.

### Future modern-only builds

A future compile-time modern-only configuration may exclude plaintext or other legacy writers and
AWS-facing options, but it should select an explicit supported-profile registry rather than fork
the storage and HTTP pipelines. Disabled profiles must never be silently reinterpreted: either keep
their readers for deliberate read-old/write-new operation or reject existing unsupported durable
state before serving. Build and test the full and modern-only registries independently, including
AWS-compatible rejection behavior for disabled request options. Do not make completion of this
plan depend on that future product configuration.

### Phase 0 implementation inventory and target seam

The current code has no independent payload-codec choice. `ActiveWriteEncryption` combines the
AWS-visible key mode with whole-segment framing: `None` copies into a new `Vec`, while SSE-C and
SSE-S3 call their whole-segment AES-GCM helpers. On reads, `SegmentPayloadRecord` carries only
`ObjectEncryption`; the reader loads a complete stored segment, chooses plaintext or whole-segment
AEAD from that value, and only then slices the requested logical range. `max_stored_segment_size`
likewise assumes the only encrypted representation is `plaintext length + one 16-byte tag`.

The write-path inventory is:

| Effective path | Current codec selection and identity timing | Required Phase 1/2 seam |
| --- | --- | --- |
| Buffered direct `PutObject` | Encrypts segment zero before reserving the object generation. | Reserve the immutable payload generation first, mint the opaque standard-object subject, then construct the segment encoder. |
| Promoted buffered and streamed `PutObject`/POST Object | Session creation already durably reserves the eventual object generation under the session ID; append placement and finalization reload that reservation, but encryption does not consume it. Every append independently calls `encrypt_segment`. | Reuse the existing storage-owned reservation as the owner-minted encryption subject and expose it only through a subject-bound session encoder; do not duplicate it in `StreamUploadRecord`. |
| `CopyObject` destination | Reads source chunks and feeds them to the same destination stream append/encryption path. | Use the same destination session subject and encoder as streamed PUT; source encryption state never supplies destination identity. |
| `UploadPart` | The session target binds upload ID and part number, but no immutable generation for the new part is fixed before its first encrypted segment. | Allocate and persist the part payload generation at session creation and bind every segment to upload, part number, part generation, and segment index. |
| `UploadPartCopy` destination | Uses the same multipart stream append path after reading source chunks. | Use exactly the same part-session subject and encoder as ordinary `UploadPart`. |
| Full and ranged object/copy-source reads | Infer the representation from `ObjectEncryption`, read and verify a complete segment, decrypt one AEAD record, then slice. | Dispatch on an explicit durable profile and let its subject-bound reader define stored spans, authentication, release, finalization, and physical-health evidence. |

`next_segment_vid` is a physical stream-segment allocator and is not a substitute for the logical
object or part generation required by the encryption context. Streamed PUT already has a separate
durable object-generation reservation keyed by its session ID; the encryption subject must reuse
that exact reservation through a storage-owned capability. Do not bind ciphertext to session IDs,
temporary shard identities, or placement, and do not copy the reservation into a second mutable
record.

The common seam is an exhaustive internal profile registry, not a public trait implementable by
callers. It must provide these opaque, subject-bound states:

- a prepared segment encoder created only from a supported durable profile, matching key state,
  and an owner-minted standard-object or multipart-part subject;
- an incremental encoder that owns framing/digest state and yields bounded stored chunks, followed
  by one consuming finalization result containing exact stored size and integrity evidence;
- a prepared segment reader created from the durable profile, matching key state, persisted
  subject, and requested logical range;
- a reader-owned stored-span plan and incremental verifier that is the only route by which
  plaintext chunks can be released; and
- one consuming read finalization result that separates logical authentication success, physical
  shard health/repair evidence, and a post-response late failure.

Profile dispatch must exhaustively admit only the durable cross-product specified below. HTTP,
copy, multipart, EC, repair, and storage orchestration may carry these opaque states and results,
but may not call raw encryption/decryption helpers or publish a caller-built plaintext/stored
`Vec` around them. Stored-size and range geometry are profile operations rather than global
constants.

The initial release guarantees are deliberately different behind the common contract:

| Profile | Earliest plaintext release | Successful finalization requirement | Late failure behavior |
| --- | --- | --- | --- |
| `PlaintextWholeSegmentCrcV1` | Per segment, after that complete segment's required physical CRC validation. | Exact logical/stored length and whole-segment CRC evidence agree for every requested segment. | Emit no bytes from the failing segment. If the response is uncommitted, return the mapped error; otherwise abort the transport, even when no body bytes were emitted. |
| `SegmentAes256GcmV1` | Per segment, after that complete segment AEAD record authenticates. | AEAD, exact length, and required physical-health processing complete for every requested segment. | Emit no bytes from the failing segment. If the response is uncommitted, return the mapped error; otherwise abort the transport, even when no body bytes were emitted. |
| `Cobblestone256SegmentV1` | Per 16 KiB chunk, only after that chunk authenticates. | Canonical terminal-chunk/length rules, every selected/full-read chunk, and final physical-health processing complete. | If headers or prior authenticated chunks were emitted, send no bytes from the failing chunk and abort the transport; otherwise return the mapped pre-response error. |

The concrete Rust types may differ, but Phase 2 is not complete if callers can bypass these
ownership and release boundaries or reconstruct a subject from primitive fields.

## Current Cryptographic Binding

The current payload and metadata AEAD associated data consists only of fixed domain/version labels:

- `argmin:sse-c:wrap:v1`
- `argmin:sse-c:segment:v1`
- `argmin:sse-c:checksum:v1`
- the corresponding `argmin:sse-s3:*:v1` labels

Segment nonces additionally contain a random per-encryption-state prefix, an object-or-part scope,
the multipart part number where applicable, and the segment index. This provides nonce separation
and binds a ciphertext to its position under one DEK. Each write also receives a random DEK, so an
ordinary segment copied between independently encrypted objects fails authentication.

The missing property is external logical-subject binding. If an encryption state, its wrapped DEK,
and its ciphertext were moved together, the AEAD does not itself know that they now belong to a
different bucket, key, incarnation, or object generation. Storage capabilities, metadata-command
validation, checksums, and exact-subject records are expected to reject such a mix-up, but the
payload encryption should independently authenticate the same invariant.

This is defense in depth rather than a claim that the present random-DEK design has an immediate
ciphertext-forgery weakness.

## Integrity Layers

Keep three integrity domains distinct:

1. **Physical storage integrity**
   - durable per-shard size and CRC64 acknowledgement;
   - detects disk/file corruption and wrong physical shard bytes;
   - identifies repair candidates and supports scrub, scavenging, and reconstruction diagnostics.
2. **Encrypted logical payload integrity**
   - Cobblestone key commitment and per-chunk AEAD tags;
   - authenticates logical ciphertext/plaintext, ordering, context, position, and finality;
   - is sufficient to release authenticated plaintext on the encrypted normal read path.
3. **S3 client-visible integrity**
   - stored S3 checksum metadata and request checksum validation;
   - preserves AWS-compatible checksum headers and whole-object/part validation;
   - remains independent of internal shard CRC and payload AEAD choices.

Do not make a whole-shard CRC scan a prerequisite for returning an encrypted subrange that has
successfully authenticated under the exact logical subject. Conversely, do not use an AEAD failure
alone to identify which physical shard is corrupt. The exceptional repair path retains or obtains
the physical evidence necessary to localize the fault.

A non-ranged read is different: it already consumes every byte of each selected systematic shard.
It must calculate that shard's CRC64 during the read and compare it with the durable witness before
treating the shard as healthy. This adds no physical IO and preserves exact corruption localization.
Implementations should fuse CRC calculation with the existing IO/copy path and use CRC composition
over the validated systematic-shard CRCs to check the logical segment CRC. They must not add a
second pass over the assembled segment merely to recompute the same checksum.

### Durable per-shard integrity witness

The new profile must retain the exact physical acknowledgement evidence needed to make the repair
guarantee durable. The current committed segment representation keeps the logical whole-segment
CRC but does not retain every original per-shard acknowledgement alongside object metadata. That
is insufficient to identify multiple unknown corrupt shard indices after the original data-node
acknowledgement state is unavailable or the payload has crossed historical-route transitions.

Persist an opaque `SegmentShardIntegrityWitness` with every segment written in the new profile. Its
logical shape is:

```text
SegmentShardIntegrityWitness {
    shard_size,
    ordered_crc64s,
}
```

`shard_size` is the one canonical stored size shared by every data and parity shard. The position
of each entry in `ordered_crc64s` is its shard index, and the array has exactly `k + m` entries.
Do not repeat either the size or index in every entry: unequal expected shard sizes and duplicate
indices are not valid EC layouts and should not be representable in the durable witness.

Storage constructs the witness only from exact durable write acknowledgements. Before terminal
command installation it derives the canonical shard size from the logical stored length and `k`,
requires every acknowledgement's stored size to equal it, orders the acknowledgements by their
unique contiguous shard indices, validates the EC shape and complete placement membership, and
validates the existing systematic-CRC composition relationship to the logical stored segment.
Callers cannot construct, edit, or enumerate its physical identities.

At read time, a shard file or independently stored acknowledgement whose size differs from the
single witnessed `shard_size` is corrupt, not an alternative valid layout. The normal direct path
enters full recovery; recovery treats the shard as an erasure, repairs it after authenticated
reconstruction when at least `k` valid shards remain, and otherwise fails closed.

The object-PG terminal command carries the self-contained witness and replica apply persists it
atomically with the segment record. Apply does not contact data PGs. Object snapshots, retained
read capabilities, recovery, repair, backfill, and format migration carry the same immutable
witness. Its lifetime and the exact route-history references needed to locate those shards remain
pinned until payload reclaim completes. It therefore remains available after restart, original
data-node acknowledgement-row loss, and historical-route transitions.

This requires schema, metadata-command, RPC, snapshot, checksum, and format-ledger changes. Exact
codec fixtures must prove the one canonical shard size and all `k + m` ordered CRC entries survive
every boundary. Missing, duplicated, reordered, truncated, crossed-segment, crossed-EC, and
non-canonical-size acknowledgement sets fail before publication or read.

On the normal encrypted subrange path the witness does not force a whole-shard scan. If AEAD
authentication fails, the diagnostic path reads the relevant complete shards and compares them to
the witness, turning every identified corrupt shard into a known erasure. This supports recovery
from any combination of up to `m` missing or witness-identified corrupt shards without
combinatorial trial decryption.

## Canonical Encryption Context

### Context hierarchy

Define private, non-forgeable owner types rather than passing arbitrary byte slices:

- `ObjectEncryptionSubject`
- `SegmentEncryptionSubject`
- `ChecksumEncryptionSubject`

The storage/coordinator boundary should expose opaque construction from admitted, exact-subject
state. Callers must not independently combine a bucket from one snapshot, a generation from
another, and an upload or part identity from a third.

The canonical encoding is binary, versioned, tagged, and length-prefixed. Do not use display
strings, slash concatenation, locale-sensitive representations, or implicit field omission. Freeze
exact encoding fixtures and reject noncanonical encodings.

### Object encryption subject

The base subject should include:

- Argmin encryption-context domain and encoding version;
- selected payload encryption profile;
- a random, immutable encryption-instance identifier;
- bucket incarnation generation;
- exact bucket name;
- exact object key; and
- immutable internal object payload generation.

The encryption-instance identifier ties key wrapping, checksum metadata, and all segments of one
write together. It is not a substitute for the external bucket/key/generation fields: an identifier
stored beside ciphertext could otherwise be moved with it.

### Segment encryption subject

Extend the object subject with:

- payload provenance tag: standard-object segment or multipart-part segment;
- multipart upload ID for a multipart segment;
- multipart part number and immutable part generation for a multipart segment;
- segment index; and
- logical plaintext segment length.

Cobblestone itself authenticates chunk position, final-chunk semantics, and message length. These
must not be replaced by an Argmin-specific framing rule. The expected logical length remains in the
external subject so changing segment metadata cannot reinterpret otherwise valid bytes.

### Checksum and wrap subjects

Use the same base object subject for DEK wrapping and encrypted checksum metadata, with distinct
purpose tags:

- `dek-wrap`
- `payload-segment`
- `checksum-metadata`

For the current AES-GCM wrapping and checksum codecs, serialize the purpose-specific subject as
AEAD associated data. For Cobblestone segment messages, supply the segment subject through
Cobblestone's context input, which is incorporated into key derivation. Do not modify the
Cobblestone chunk construction and still describe the result as the standard profile.

The SSE-C salted key validator remains a customer-key identity check. It need not become
object-specific, but successful validation must only open the DEK under the exact object wrap
subject.

### Fields that must not be bound

Do not include values that are mutable, physical, transient, or unavailable during streaming:

- request ID or authenticated request principal;
- admission deadline, lease deadline, or frontend session lifetime;
- cluster epoch, PG, node, shard, EC shape, or storage class;
- ACL, tags, user metadata, retention, or other independently mutable metadata;
- ETag or whole-object checksum that is finalized only at EOF;
- final S3 version ID when it is selected only during terminal publication.

Copy operations decrypt under the complete source subject and create a new destination encryption
subject and ciphertext. Repair, backfill, and placement transitions preserve the logical subject
and therefore do not require re-encryption.

## Allocate Identity Before Encryption

Every immutable context field must exist before the first encrypted byte. Do not omit generation
binding merely because one current write path allocates it late.

Required ordering changes:

- Direct PutObject reserves its internal object generation before encrypting its segment.
- Streamed PutObject already reserves the eventual object payload generation durably during
  session creation. Reuse that exact reservation to mint the encryption subject before the first
  append; do not allocate or persist a duplicate generation in the stream-session record.
- Multipart upload creation already reserves the eventual object generation; UploadPart also
  needs an immutable part generation before its first segment is encrypted.
- Request-owned UploadPart staging introduced by the unified staged-write plan receives the same
  exact identity before writing payload.
- CopyObject and UploadPartCopy allocate the destination subject before beginning destination
  encryption.

Reservation failure or cancellation must retain the existing bounded cleanup and release
semantics. Merely allocating an ID is not authorization to publish it. The immutable subject is
identity, while admitted route/effect capabilities remain the authority for every durable write.

If holding the existing generation reservation throughout a streamed body would alter contention
semantics, introduce a dedicated immutable payload-encryption generation allocated from the same
trusted storage owner and carry it into the final object generation relationship. Do not fall back
to a transient HTTP request ID that completed readers cannot reproduce.

## Payload Profile

### Message boundary

Each current Argmin logical segment is one independent Cobblestone message:

```text
object/part payload
  -> 8 MiB logical Argmin segments
    -> one Cobblestone header per segment
    -> 16 KiB authenticated Cobblestone chunks within the segment
    -> EC encode the complete segment ciphertext representation
    -> placed shard files
```

The segment header is part of the stored bytes. Random access reads the fixed header plus the
selected encrypted chunks. Exact-multiple messages retain the standard's required final short
chunk behavior.

Use the per-object DEK as Cobblestone's input key and a fresh standard-compliant salt per segment.
The derived message key and base nonce are segment-specific. Enforce the standard's message-count
and total-data limits even though an 8 MiB message size and S3's object limits should remain well
inside them.

### Profile choice

Prefer `Cobblestone-256` over `Cobblestone-128` so Argmin does not silently move its implemented
SSE-S3/SSE-C payload cipher from AES-256 to AES-128. This is an internal choice and does not change
the AWS wire algorithm name, but preserving the stronger existing key-size contract avoids an
unnecessary downgrade.

If benchmarks show that fixed 16 KiB chunks impose unacceptable sequential CPU overhead, do not
change the Cobblestone chunk size while retaining the Cobblestone profile name. Either keep the
current format or define and review a distinct Argmin encryption profile with its own analysis,
version, vectors, and security rationale. Candidate 64 KiB or 256 KiB records may be useful
engineering comparisons, but they are not Cobblestone.

### Space and CPU expectations

The current format adds one 16-byte tag per 8 MiB segment. Cobblestone adds one tag per 16 KiB
chunk, its fixed header, and the required terminal chunk. Space amplification remains roughly
0.1%, but a sequential 8 MiB read performs hundreds of AEAD record operations rather than one.

The implementation must measure:

- full-read AES/HKDF/tag-finalization CPU;
- key-commitment and header cost;
- allocation and copying behavior;
- ability to decrypt directly into pooled response buffers; and
- whether smaller authenticated records improve first-byte latency or memory enough to offset
  their fixed per-record work.

## Authenticated Partial-Shard Reads

### Healthy systematic path

Add a production range-read operation that:

1. holds deletion-exclusion leases for exactly every shard location it may read;
2. uses positioned file IO rather than loading a complete shard;
3. maps the requested Cobblestone header/chunk ciphertext ranges onto systematic data-shard ranges;
4. reads only those ranges from healthy systematic shards;
5. validates authenticated transport framing and exact response lengths;
6. reconstructs the selected ciphertext bytes in canonical order; and
7. releases each plaintext chunk only after the key commitment and that chunk's tag verify under
   the exact segment subject.

The current range RPC's whole-shard CRC-before-slice behavior must not be reused and relabelled as
partial IO. Its protocol should carry an exact leased shard subject, offset, length, and bounded
response size. The storage node validates offset arithmetic and file bounds before IO.

If any required systematic range cannot be read exactly, or if the assembled chunk does not
authenticate, abandon the partial result and enter the full-segment path below. The initial reader
does not mix healthy range reads with parity ranges.

### Deferred partial EC reconstruction

Argmin's systematic data shards are contiguous portions of the padded segment representation.
For each selected ciphertext span, compute its data-shard index and local offset. A span crossing a
data-shard boundary becomes multiple bounded range operations.

It is technically possible to reconstruct only a requested within-shard range from the same local
range on any valid `k` shards. That can turn an 8 MiB degraded fallback into roughly `k * 16 KiB`
of shard IO for a one-chunk range. The EC arithmetic is not the main cost; the feature also needs
range-to-shard mapping, leases across the chosen survivors, bounded parallel RPCs, retained-route
handling, partial-response validation, retry rules, and a much larger failure test matrix.

Do not require this optimization for the initial format or reader. It helps primarily when a
required systematic shard is already a known erasure, such as an unavailable node or absent file.
It does not remove the full path: an AEAD failure over plausible returned bytes does not identify
the corrupt input shard and must still trigger complete physical validation. Broader corruption is
also likely to extend beyond the one requested chunk, making the full diagnostic read appropriate.

Instrument the initial fallback by reason, requested bytes, physical bytes, EC profile, and node
state. Decide on partial EC reconstruction later from the observed combination of range-request
frequency and machine/data failure rates. A workload with rare ranges or rare unavailable shards
may not justify the additional state space; a range-heavy workload under a prolonged node outage
may justify it to avoid repeated full-segment amplification.

The full fallback is intentionally no worse in IO granularity than today's encrypted 8 MiB
segment path; it omits the new 16 KiB optimization only for that degraded request. A transiently
missing machine may return before many requests take this path, while an authentication failure is
reason to suspect corruption beyond the requested chunk and perform the complete diagnostic read.
This makes the simpler fallback a safe initial trade rather than a regression from current
behavior.

If implemented later, limit the first version to already-known erasures. Reed-Solomon operations
are byte-position independent, but expose and test an explicit bounded range API rather than
relying on an undocumented property of the full-shard codec. AEAD failure in this optional path
still falls back to complete witness validation and must not schedule repair from partial evidence.

The partial reader must preserve the retained-route and deletion-exclusion guarantees used by full
reads. It may read only from locations for which it holds a lease, including after a route
transition or node failure. This applies both to the required healthy systematic path and any later
partial-reconstruction extension.

### Corruption and repair fallback

A valid AEAD tag is enough to return the encrypted logical range. A tag failure establishes that
the assembled logical ciphertext is invalid but may not identify the faulty physical shard.

On failure:

1. return no plaintext from the failing chunk or any later chunk;
2. enter the existing physical diagnostic/reconstruction path;
3. validate complete relevant shards against the segment's durable per-shard integrity witness;
4. reconstruct and re-authenticate the requested chunks;
5. schedule repair only for locations identified by exact durable evidence; and
6. surface an internal data-integrity failure through the response-commitment rules below only if
   no authenticated reconstruction is possible.

For `1+0`, AEAD failure identifies the sole copy as unusable but cannot recover it. For redundant
profiles, tests must prove that full reconstruction succeeds with up to `m` missing or
durable-witness-identified corrupt shards and fails closed beyond tolerance.

Do not use combinatorial trial decryption as the primary corruption-localization mechanism. It is
not a bounded replacement for durable physical acknowledgement evidence.

### HTTP response commitment and late authentication failure

Authentication is chunk-granular, while an HTTP response may span many chunks. Once response
headers and earlier authenticated chunks have been delivered, a later authentication failure
cannot retract those bytes or replace the response with an S3 XML error.

Use one explicit response state machine for full and range GETs:

1. Before committing headers, validate the stored profile/context and the fixed segment header and
   key commitment needed to construct the body. If body construction has already loaded the first
   chunk, authenticate it as well; do not require buffering the complete response.
2. If authentication or reconstruction fails while the response is uncommitted, return the normal
   contract-mapped S3 error response and no payload bytes.
3. After headers are committed, authenticate every chunk before emitting bytes from that chunk.
4. If a later chunk cannot be authenticated or reconstructed, emit none of that chunk or any later
   chunk, record the exact integrity/repair diagnostic, and abort the response transport.
5. For HTTP/1.x, close the connection with an incomplete declared/chunked body and do not reuse it.
   For a future multiplexed transport, reset only the affected stream. Never append an XML error to
   a partially emitted object body.

Earlier successfully authenticated chunks may therefore have reached the client on a late
failure. They are authenticated but the response is incomplete and must not be reported as a
successful object read. A bounded small range may be fully authenticated before committing
headers as an optimization, but correctness must not depend on buffering the complete response.

Tests must distinguish pre-header failure from post-header failure with a raw client. They verify
the former receives the selected S3 error, while the latter receives an unmistakably incomplete or
reset response containing only complete previously authenticated chunks. SDK-level tests verify a
retry never concatenates bytes across attempts and never reports the partial response as success.

### Official-client behavior for committed response failures

Do not assume that closing a response is sufficient. Successful full and ranged GET responses must
retain their exact `Content-Length`; after a committed body failure, the HTTP implementation must
surface a body-stream error and terminate the connection or stream before that declared length is
satisfied. It must not send a clean zero chunk, successful end-of-stream marker, shortened length,
or XML error body. This is what lets an SDK distinguish a failed body read from a shorter successful
object. Streaming-operation dispatch success is not object-read success: the consumer must observe
and handle the body error.

The repository's locked Rust AWS S3 SDK already carries upstream tests in which `GetObject.send()`
succeeds and `ByteStream::collect()` then fails for premature EOF and both short and long
`Content-Length` mismatches. Botocore's `StreamingBody` likewise compares bytes consumed with
`Content-Length` at EOF and raises `IncompleteReadError`. Preserve these as permanent integration
canaries against Argmin, using exact pinned SDK versions rather than relying only on source review.

Before enabling chunk streaming, run and record the equivalent behavior for the official AWS SDKs
for Rust, Python/Boto3, Go v2, Java v2, and JavaScript v3, plus the AWS CLI:

- headers and at least one authenticated body chunk are received successfully;
- a deterministic later body error terminates below the declared full or `206` range length;
- consuming or collecting the body reports failure rather than clean EOF;
- no SDK silently concatenates bytes from an automatic retry with the first attempt;
- file-oriented helpers do not publish a successful complete destination; and
- callers that explicitly stop reading early are not counted as evidence, because no protocol can
  force an application to observe an error after it abandons the body.

Keep raw HTTP/1.x framing tests authoritative for the server behavior and representative official
client tests authoritative for consumption behavior. A future HTTP/2 implementation needs the same
matrix with an error/reset stream rather than a clean short DATA sequence.

### Full reads

For encrypted payloads, authentication of every Cobblestone chunk proves the complete logical
payload. Full reads must nevertheless validate the size and CRC64 of every complete shard they
read against `SegmentShardIntegrityWitness`: those bytes are already traversed, and the physical
evidence identifies an exact erasure and repair target if corruption is found. This requirement
does not make parity-shard reads mandatory on an otherwise healthy systematic read.

Calculate each shard CRC incrementally as its bytes are read, then use CRC composition over the
validated systematic shards to check the logical segment CRC. Do not perform a second whole-payload
pass merely to authorize response bytes. AEAD remains the authority for releasing plaintext;
physical CRC remains the authority for classifying the underlying shard as healthy or corrupt.

A CRC mismatch discovered only after all corresponding logical ciphertext has passed AEAD is a
physical diagnostic and repair event, not by itself a reason to retroactively fail the authenticated
client response. If logical bytes from that shard remain unauthenticated, continue to gate those
bytes on AEAD and enter the diagnostic path; an unrecoverable AEAD failure follows the committed
response rules above. Unencrypted reads do not have this separation and must not release data early
without an independently authenticated chunk-integrity design.

The full reader should stream authenticated chunks rather than requiring an 8 MiB ciphertext and
plaintext allocation. It must not release a chunk before its tag verifies. Whole-object S3
checksum verification remains a separate incremental operation when the API requires it. A late
failure follows the transport-abort rules above; the plan does not promise that previously emitted
authenticated chunks can be withdrawn.

## Durable Representation And Versioning

Persist an explicit segment payload-codec profile alongside the independently modeled
`ObjectEncryption` key/mode state, for example:

- `PlaintextWholeSegmentCrcV1` for the current plaintext representation;
- `SegmentAes256GcmV1` for the current encrypted representation;
- `Cobblestone256SegmentV1` for the proposed representation.

The exact names are less important than making the distinction durable and exhaustive. Validate
the allowed cross-product: plaintext codec with `ObjectEncryption::None`, and encrypted codecs with
the applicable SSE-S3 or SSE-C state. Crossed codec/key-state combinations fail closed. Stored size
calculations must become profile-aware; they can no longer assume either identity framing or
`plaintext length + 16`.

Update every containing representation named by the storage-format dependency ledger, including:

- object, multipart-upload, stream-session, part, and segment metadata;
- metadata commands and command checksums;
- storage RPC request/response codecs;
- snapshots, restart/checkpoint evidence, diagnostics, and test fixtures;
- reclaim, repair, backfill, and scavenger records that carry stored sizes or payload subjects; and
- the durable per-shard integrity witness and its route-history retention references; and
- `guides/storage-format-ledger.md` and `plans/storage-upgrade-versioning-plan.md`.

Follow the storage-upgrade plan's nested-codec and immutable-payload rules. Freeze current-format
evidence rather than regenerating it from new structs. Unknown profiles and crossed
profile/state combinations fail closed.

Because Argmin is pre-release, implementation may deliberately choose a clean format break rather
than a production migration. That does not permit silent reinterpretation: bump the appropriate
schema/codec/profile versions, retain immutable rejection evidence, and update the complete version
vector. If old payloads are retained, use read-old/write-new plus a verified background rewrite;
never rewrite unbounded payload synchronously at startup.

## Write-Path Integration

All encrypted write paths must consume the same opaque subject and codec:

- buffered and streamed PutObject;
- POST Object;
- CopyObject destination;
- CreateMultipartUpload and UploadPart;
- UploadPartCopy destination;
- request-owned and durable-session staging modes from the unified staged-write plan.

Encryption happens before EC. The complete Cobblestone segment representation is EC encoded and
its placed shard bytes receive the existing durable acknowledgement records.

Publication commands carry or reference enough exact profile/context evidence for recovery to
reject:

- a segment from another bucket/key/generation;
- standard/multipart provenance crossing;
- another upload, part number, or part generation;
- another segment index or logical length;
- a different encryption profile or encryption instance; and
- duplicated, missing, reordered, truncated, or extended segment ciphertext.

Replica apply validates immutable command and object-PG-local evidence. It must not synchronously
contact data PGs to revalidate shard bytes. As with existing staged publication, shard
acknowledgement evidence is validated before installation and encoded as a self-contained witness.

## Implementation Phases

### Phase 0: Measure and freeze the current model

- Add counters for logical range bytes, stored bytes read, shard bytes read, bytes reconstructed,
  bytes decrypted, AEAD records opened, CRC bytes processed, buffer checkouts and actual buffer
  allocations, and repair fallback.
- Benchmark the current encrypted and unencrypted range/full-read paths.
- Inventory every existing plaintext/encrypted branch and specify the sealed common profile
  interface, including per-profile release guarantees and finalization errors.
- Freeze current encryption-state, segment, command, and RPC fixtures.
- Add negative tests demonstrating the current context boundary without treating them as an
  exploitable bypass: ordinary segment crossing fails due to independent DEKs, while the design
  currently relies on storage subject validation against correlated state/payload crossing.
- Record the exact Cobblestone revision and review implementation/dependency choices.

Exit: the expected benefit and current format are measurable and immutable evidence exists.

Implementation update (2026-10-17):

- The first measurement slice exports cumulative `payload_read_*` counters through the existing
  metrics snapshot and local metrics endpoint. Logical bytes are counted when a read chunk is
  exposed; stored bytes after a complete segment read; physical shard bytes at successful file
  reads; reconstructed bytes at the EC output boundary; AEAD open attempts before authentication;
  decrypted bytes only after a successful open; and CRC bytes at each actual checksum pass.
- Pool checkouts, new backing-buffer allocations, and allocated capacity are separate counters.
  This avoids calling every checkout an allocation and makes reuse measurable.
- Repair fallback counts each healthy systematic read that enters the full-shard recovery path.
  Physical shard and CRC counters deliberately include fallback rereads and repeated validation,
  so they expose work amplification rather than merely restating the logical response size.
- The current-path inventory confirms that local systematic reads validate each shard CRC and
  compose the segment CRC without rescanning the assembled segment. Unix/TLS full-shard reads
  through the production read-handle path use the range RPC with the complete shard range: the
  storage node validates the whole-shard CRC once, then the authenticated transport carries the
  selected bytes. Historical inspection computes the acknowledgement at the storage node and
  validates it again at the receiver. The test-only full-shard RPC also validates during
  handler processing, response encoding, and response decoding. The counters intentionally expose
  each pass actually taken.
- The current reader still obtains and decrypts one complete stored segment before returning any
  selected bytes. `ObjectEncryption::None` bypasses the decrypt step; SSE-C and SSE-S3 each open
  one whole-segment AEAD record. The common sealed profile interface and benchmark matrix remain
  Phase 0 work.
- The format-evidence audit found existing exact fixtures for `ObjectEncryption` state, the
  metadata-command corpus (including encrypted stream-session state), and the SSE-S3 segment
  cipher. This slice adds the missing deterministic SSE-C whole-segment cipher vector and a
  crossed-DEK authentication failure, plus an exact encrypted storage-RPC v22 payload fixture.
  Changing the current profile now requires an explicit fixture and outer-version decision rather
  than silently rewriting the baseline.
- The write/read inventory now identifies the current branch points and the identity-ordering gaps
  for direct PUT, streamed PUT/POST, CopyObject, UploadPart, UploadPartCopy, full reads, and range
  reads. In particular, direct PUT encrypts before generation reservation; streamed PUT durably
  reserves its object generation at session creation but does not thread that reservation into
  encryption; and UploadPart does not yet fix its replacement-part generation before encryption.
- The target seam is an exhaustive internal durable-profile registry producing opaque,
  subject-bound incremental encoders and readers. It owns stored-size/range geometry, plaintext
  release, and consuming finalization; AWS-visible key mode remains a separate dimension. The
  initial plaintext, legacy segment-AEAD, and Cobblestone release/error guarantees are recorded
  above.
- The reviewed upstream baseline is C2SP commit
  `d757bb90e5966d92d17353a3964ca1af776b63e0`. Because the specification remains untagged and in
  development, writer enablement requires a fresh upstream review. The implementation choice is a
  provider-neutral `argmin-crypto` implementation over existing Ring/OpenSSL primitives rather
  than a new production dependency. The separate Wycheproof corpus is pinned to commit
  `b61843a9a5115bb758134b6a1f5d5e502d445342` and the exact AES-256-GCM vector-file digest recorded
  above; both providers must pass that corpus and interoperability tests.
- The reproducible encrypted/plaintext full/range benchmark matrix remains the Phase 0 exit item.

### Phase 1: Exact encryption subjects

- Introduce private canonical context codecs and opaque subject constructors.
- Ensure every write path exposes its existing or newly allocated immutable generation to the
  owner-minted encryption subject before encryption.
- Persist bucket incarnation, encryption-instance identity, and required payload provenance.
- Bind DEK wrapping and encrypted checksum metadata to the base object subject.
- Add exhaustive crossed-field and noncanonical-encoding tests.
- Update the storage version dependency ledger before changing writers.

Exit: no encryption operation accepts caller-assembled identity fields, and every field needed by
the eventual reader exists before the first payload effect.

### Phase 2: Cobblestone segment codec

- Implement or integrate the reviewed Cobblestone-256 profile behind `argmin-crypto`.
- Import the official/Wycheproof vectors and add exact Argmin context vectors.
- Run every official and Argmin vector under both supported `argmin-crypto` provider builds.
- Add deterministic bidirectional interoperability tests: seal with Ring and open with OpenSSL,
  then seal with OpenSSL and open with Ring, across empty, boundary, multi-chunk, and context-bound
  messages. Fixed key/salt/context inputs must produce the same persisted bytes in both builds.
- Implement streaming seal/open, header/commitment validation, selected-chunk opening, and exact
  final-chunk validation.
- Persist the complete storage-owned per-shard integrity witness through segment schema, terminal
  commands, apply/recovery, snapshots, RPC, retained reads, repair, and route-history retention.
- Add the durable profile discriminator and profile-aware stored-size calculations.
- Implement one compiler-exhaustive codec/encryption-state compatibility registry and exercise its
  complete valid and invalid cross-product at construction, decoding, apply/recovery, and restart.
- Integrate the codec with every encrypted write path while retaining the old reader if required by
  the selected format transition.
- Put the current plaintext representation and both encrypted representations behind the common
  profile contract; do not retain a raw plaintext bypass in write, read, range, or copy paths.

Exit: full writes and full reads use one segment codec across all operations and pass adversarial
format tests, but the new profile is not yet selected solely for an unmeasured range benefit.

### Phase 3: Real partial shard IO

- Replace the test-shaped whole-read range RPC with a production capability-bound range reader.
- Use descriptor-relative positioned IO and exact deletion-exclusion leases.
- Add local, Unix, and TLS range framing with bounded lengths and malicious-response validation.
- Add partial systematic mapping for healthy reads.
- Route every missing, unavailable, malformed, or unauthenticated required range into the existing
  full-segment witness-validation and reconstruction path.
- Preserve retained-route operation across publication and node-loss scenarios.

Exit: requesting a bounded ciphertext range causes bounded disk and network IO in healthy and
systematic profiles, while every degraded or suspect case remains correct through the full-segment
fallback.

### Phase 4: Authenticated Range GET path

- Map S3 plaintext ranges through segment/chunk/ciphertext/shard coordinates.
- Read each required segment header and minimal chunk set.
- Authenticate before exposing plaintext.
- Fall back to full physical validation and repair only after authentication/read failure.
- Stream full GETs chunk by chunk, fuse required per-shard CRC calculation with shard IO, compose
  the logical segment CRC from validated systematic-shard CRCs, and remove only the redundant
  second checksum pass over the assembled encrypted payload.

Exit: metrics and deterministic tests prove small encrypted ranges no longer read or decrypt whole
segments on the healthy path.

### Phase 5: Performance and writer decision

- Run the benchmark matrix below on local `1+0`, representative redundant EC, and multihost
  configurations.
- Compare the current segment AEAD, Cobblestone-256, and any explicitly named experimental chunk
  profile without conflating their security claims.
- Record sequential throughput, random-range amplification, CPU, memory, and degraded-read cost.
- Enable the new writer only if the measured range benefit is material and the accepted sequential
  cost is documented.
- Update operator/configuration documentation, storage format ledger, and the encryption
  compatibility plan.

Exit: the format choice is evidence-based and every enabled writer has a supported reader,
recovery path, and durability/versioning story.

### Optional follow-up: Partial EC reconstruction

This is not part of the initial completion path. Start it only if Phase 3/4 production-like metrics
show that known-erasure full-segment fallbacks materially harm a range-heavy workload.

- Add a bounded EC range API operating on equal within-shard offsets.
- Use it only when every omitted shard is already a known erasure.
- Acquire exact deletion-exclusion leases for all selected survivors before issuing range reads.
- Authenticate reconstructed chunks before emission.
- On malformed input, short read, or tag failure, discard the partial reconstruction and enter the
  full durable-witness diagnostic path.
- Do not schedule repair or classify a corrupt shard from partial evidence alone.
- Benchmark the optional path separately; it does not change encryption bytes or format versions.

## Correctness And Security Tests

### Cryptographic vectors

- Official Cobblestone-256 and Wycheproof vectors.
- Empty, one-byte, 16 KiB minus/at/plus one, 8 MiB minus/at/plus one, and maximum segment cases.
- Exact-multiple messages with the required terminal short chunk.
- Wrong input key, salt, commitment, context, nonce/counter, and tag.
- Malformed final ciphertext length, missing empty terminal chunk for an exact multiple, a full-size
  terminal chunk, truncation, extension, duplicated/reordered chunks, and trailing chunks or bytes.
- No plaintext returned from any unauthenticated chunk.

### Crypto-provider interoperability

- Run the official/Wycheproof and Argmin context vectors in Ring and OpenSSL CI jobs.
- Ring-sealed persisted messages open with OpenSSL and OpenSSL-sealed messages open with Ring.
- Fixed key, salt, context, plaintext, and profile produce byte-identical headers, commitments,
  chunks, tags, and terminal encodings across providers.
- Provider changes across restart preserve reads, range mapping, checksum opening, and DEK unwrap.
- Unsupported provider/profile combinations fail before a writer is enabled, not after payload is
  committed.

### Payload-codec and encryption-state cross-product

Pin the complete compatibility relation rather than testing only operational behavior for valid
profiles. At minimum, the current registry contains these valid pairs:

| Payload codec | `ObjectEncryption` state |
|---|---|
| `PlaintextWholeSegmentCrcV1` | `None` |
| `SegmentAes256GcmV1` | SSE-S3 |
| `SegmentAes256GcmV1` | SSE-C |
| `Cobblestone256SegmentV1` | SSE-S3 |
| `Cobblestone256SegmentV1` | SSE-C |

Construct this table through compiler-exhaustive mappings without wildcard arms so adding either a
codec or encryption-state variant forces an explicit compatibility decision and fixture update.
For every valid pair, use production constructors to write, encode/decode, apply/recover, restart,
and read positive canaries. Independently construct immutable malformed fixtures for:

- each encrypted codec paired with `ObjectEncryption::None`;
- the plaintext codec paired independently with SSE-S3 and SSE-C;
- unknown codec and encryption-state tags;
- a current encryption state carrying the wrong current/previous codec profile;
- current codec tags containing bytes framed under another codec, in both old-to-new and
  new-to-old directions; and
- supported outer state with noncurrent nested codec/state versions, plus noncurrent outer state
  containing an otherwise current nested profile.

Exercise every malformed class at all boundaries capable of creating or restoring the pair:

1. typed production constructors and profile selection;
2. direct segment/profile and containing metadata codecs;
3. metadata-command and storage-RPC decoding;
4. pending-command installation, replica apply, replay, and authorized recovery;
5. SQLite/checkpoint/runtime restart and restored stream or multipart-session state; and
6. active and retained snapshot/read construction.

Each invalid case must fail before data-PG/shard IO, plaintext exposure, HTTP response commitment,
repair scheduling, or durable state mutation. Test-only sentinels at those boundaries must prove
the absence of effects; merely asserting an error is insufficient because a later shard failure
could mask acceptance of the malformed pair. Exact old/current fixtures remain immutable under the
storage-format ledger rather than being regenerated from the newest structs.

### Context-crossing matrix

Change each field independently while holding all others constant:

- bucket incarnation;
- bucket;
- key;
- object generation;
- encryption instance;
- standard versus multipart provenance;
- upload ID;
- part number;
- part generation;
- segment index;
- logical segment length;
- purpose tag; and
- encryption profile.

Every crossing must fail before plaintext or checksum metadata is exposed. Positive canaries prove
that the same exact subject opens successfully through buffered, streamed, restart, retained-read,
and copy paths.

### Range and EC matrix

- Ranges wholly within one chunk, crossing chunk boundaries, crossing segment boundaries,
  crossing systematic shard boundaries, and crossing multipart part boundaries.
- First byte, last byte, suffix range, open-ended range, and unsatisfiable ranges.
- Healthy `1+0`, `2+1`, and representative wider profiles.
- Each systematic and parity shard missing or corrupted in turn, all combinations of up to `m`
  missing or durable-witness-identified corrupt shards, and failures beyond tolerance, through the
  required full-segment fallback.
- The per-shard witness remains usable after restart, original acknowledgement-row loss, and
  multiple retained-route transitions.
- Storage-node death after response-body creation, retained-route publication, and first body read.
- Exact leased-subset enforcement: no partial read from an unleased current or historical location.
- Tag failure triggers diagnostic fallback without emitting the failing chunk or repairing an
  unproven target; pre-header and post-header failures follow their distinct response semantics.
- Raw HTTP and pinned official-client canaries prove that an unrecoverable post-header failure
  produces an incomplete/error body, never a successful short full or ranged GET.
- Initial-path canaries prove a missing required systematic range selects the full fallback rather
  than an unreviewed parity-range path.
- If the optional partial EC path is implemented, separate tests cover every known-erasure
  combination through `m`, shard/chunk boundary crossings, and forced fallback on any suspect
  returned bytes.

### Write, publication, and recovery matrix

- PutObject, POST Object, CopyObject, UploadPart, UploadPartCopy, and CompleteMultipartUpload.
- Buffered/request-owned and durable-stream staging produce equivalent encrypted committed
  representations apart from intentional random values.
- Multipart same-part replacement, completion, abort, and restart retain exact subject binding.
- Response loss, pending-command replay, partial replica apply, and recovery never re-encrypt with
  a different context or publish a crossed segment.
- Cancellation and authority expiry clean unowned ciphertext without releasing another subject's
  reservation.
- Repair/backfill/relocation preserve ciphertext and context; copy creates new ciphertext under the
  destination subject.

### Checksum independence

- S3 checksums and checksum headers are unchanged between encryption profiles.
- Full-body requested checksum validation remains incremental and exact.
- Healthy non-ranged reads validate every completely read shard against the durable witness and
  compose the logical segment CRC without a second payload scan; healthy range reads do not expand
  IO merely to validate complete-shard CRCs.
- A payload may pass physical CRC but fail AEAD; no bytes from the failing chunk are returned, and
  a response already committed by earlier authenticated chunks is aborted rather than rewritten.
- A payload may pass AEAD while separately corrupted checksum metadata fails its own context-bound
  authentication.
- Physical CRC mismatch with valid reconstructable ciphertext repairs the exact shard and returns
  the same client-visible bytes/checksum.

## Benchmark Matrix

Measure both sequential GET and random `Range` GET for at least:

- 4 KiB, 16 KiB, 64 KiB, 256 KiB, and 1 MiB ranges;
- ranges crossing a 16 KiB chunk, EC shard, 8 MiB segment, and multipart-part boundary;
- 4 KiB, 1 MiB, 8 MiB, 10 MiB, 64 MiB, and representative large objects;
- encryption disabled, current AES-256-GCM segment format, and Cobblestone-256;
- `1+0`, redundant local EC, and multihost EC;
- healthy reads and one missing/corrupt shard; and
- cold-cache and warm-cache conditions where the platform permits reliable control.

Record:

- requests and payload bytes per second;
- p50/p95/p99 latency and time to first byte;
- disk and network bytes per returned byte;
- bytes passed through CRC, EC, and AEAD;
- AEAD records and key derivations per request;
- allocations, copies, and peak pooled-buffer occupancy;
- CPU cycles/instructions where stable measurement is available; and
- repair/fallback frequency and cost.

Classify fallbacks at least as known missing/unavailable shard, short/malformed response, AEAD
failure, and durable-witness mismatch. Report results for representative range-request fractions
and failure durations; a single always-healthy benchmark cannot justify or reject the optional
partial EC path.

Do not infer the decision from full-object Warp throughput alone. Add a reproducible range workload
with a recorded distribution. Conversely, do not optimize exclusively for 4 KiB ranges without
measuring common full/sequential object traffic.

## Documentation And Review Updates

Implementation must update:

- `guides/durable-storage.md` for acknowledgement and authenticated-read durability boundaries;
- `guides/storage-cluster-invariants.md` for partial reads, deletion-exclusion leases, and repair;
- `guides/storage-format-ledger.md` for the new encryption/context/profile versions;
- `plans/storage-upgrade-versioning-plan.md` for the nested encryption and immutable payload edge;
- `plans/unified-staged-write-publication-plan.md` for identity allocation before encryption and
  common staging subjects; and
- `plans/encryption-compat-plan.md` for the selected SSE-S3/SSE-C internal payload profile.

The final security review should cover the exact context encoding, nonce/salt lifecycle, maximum
messages per input key, key commitment, partial-plaintext release boundary, error handling, and
every path that can construct or resume an encryption subject.

## Completion Criteria

- Every encrypted segment is one explicitly versioned message below the profile's size limit.
- Every payload, wrapped DEK, and encrypted checksum is authenticated under a canonical exact
  logical subject.
- No mutable or physical placement field is required to decrypt stable logical payload.
- Every immutable context field exists before the first encrypted payload effect.
- Healthy encrypted subrange reads perform bounded partial shard IO and decrypt only intersecting
  authenticated chunks plus fixed framing.
- Degraded or suspect initial reads fall back to complete witness validation/reconstruction and
  never read unleased locations; partial EC is not required for format completion.
- Successful AEAD authentication is sufficient for encrypted response integrity; physical CRCs
  remain authoritative for acknowledgement, localization, scrub, and repair.
- Non-ranged reads validate all completely consumed shards against the witness, with CRC calculation
  fused into IO and no redundant assembled-segment checksum pass.
- Every new-profile segment retains a complete durable per-shard integrity witness sufficient to
  classify up to `m` missing or witness-identified corrupt shards as erasures after restart and
  route transitions.
- Pre-header integrity failures return a normal S3 error; post-header failures emit no failing
  chunk and terminate the transport without pretending the partial body succeeded.
- Ring and OpenSSL produce and consume one byte-identical persistent profile in both directions.
- S3-visible checksum and encryption behavior is unchanged.
- Current and new persisted profiles are never confused, and the format/version ledger contains
  immutable evidence for the selected transition.
- Full and range benchmarks justify the enabled writer profile with recorded, reproducible data.
