# Metadata Model And Integrity

This guide defines the target model for replicated metadata. It is intentionally
separate from the S3 coordinator model: S3 request handling decides which
storage mutation is legal, while storage replication, replay, scrub, and repair
operate on canonical storage metadata.

## Source Of Truth

Per-PG metadata has three related forms:

- The metadata command log is the ordered mutation history.
- SQLite tables are the materialized serving view used by request paths.
- Checkpoints or snapshots are compact equivalence points at a specific log
  index.

The command log and SQLite tables are allowed to contain overlapping
information. They are not two independent authorities. A replica is clean only
when its accepted log prefix, materialized canonical state, and checkpoint state
agree.

Pending commands are unclosed log intents. They exist so an interrupted command
can converge or be durably abandoned; they are not a separate source of serving
state.

## Payload Boundary

Object payload bytes are outside the metadata command log, canonical metadata
state, checkpoint, and metadata scrub digest. Metadata records payload
descriptors only:

- logical object size and layout
- segment and part manifests
- placement references such as object key hash, version, data PG, and EC shape
- payload CRC64 values needed to verify object data

Payload bitrot is detected through payload shard checksums and read-time EC
validation. Metadata bitrot is detected through canonical metadata encodings,
command-log checksums, hash-chain state, and checkpoint/range checksums.

## Canonical State

Canonical metadata state is the storage-level state that must compare equal
between clean replicas. It should not depend on accidental SQLite details such
as table creation order, hidden row ids, index layout, or local-only helper
counters.

The target canonical state includes:

- bucket records and bucket subresources
- object versions and delete markers
- object segment manifests and multipart object part manifests
- in-progress multipart uploads and uploaded parts
- stream session metadata and streamed part segment manifests while they remain
  command-owned durable state
- reclaim records and reclaim manifests
- durable allocators and counters that affect future visible metadata, such as
  object version counters, bucket execution generations, write sequences, and
  completed multipart pruning order

The current
[multihost transition Phase 7.1](../plans/multihost-transition-plan.md#phase-7-metadata-model-integrity-and-divergence-policy)
implementation has an explicit interim digest inventory for the command-owned
committed serving view:

- `bucket_subresources`
- `buckets`
- committed rows in `multipart_part_segments`
- `object_parts`
- `object_segments`
- `objects`

This list is deliberately explicit in code. Tables that are not yet included are
Phase 7 gaps, not hidden exclusions.

## Commands

The storage layer should move toward storage-shaped commands. S3-shaped
authorization and request semantics belong in the coordinator. The command log
should record deterministic storage mutations that can be replayed without
reconstructing an AWS request from current database state.

Every accepted command must define:

- deterministic preconditions over canonical state
- deterministic canonical state changes
- the exact payload descriptors written into metadata, including CRC64 values
- whether a failed zero-apply command is abandoned or must remain pending

Reverse mapping from materialized state back to the original AWS request is not
required after overwrites or deletes. Replay and checkpoint validation must be
unambiguous.

### Command Mapping Inventory

Phase 7.2 classifies each current metadata command by the state it mutates. New
commands should be storage-shaped by default. Existing mixed commands should be
converted before their payload shape is frozen into canonical binary row/range
encodings.

| Command | Shape | Canonical Reads | Canonical Writes | Retry Rule |
| --- | --- | --- | --- | --- |
| `CreateBucket` | Mixed | bucket absence / matching existing bucket | bucket row | identical bucket row converges |
| `PutBucketVersioning` | Mixed | bucket row generation | bucket versioning, execution generation | matching target state converges |
| `PutBucketAcl` | Mixed | bucket row generation | bucket ACL/public flags, execution generation | matching ACL effect converges |
| `PutBucketProperty` | Mixed | bucket row generation | bucket property columns, execution generation | matching property effect converges |
| `PutBucketSubresource` | Storage-shaped | bucket row generation | bucket subresource row, generation mirrors | matching subresource effect converges |
| `ReserveObjectGeneration` | Storage-shaped | object generation allocators and live/reclaim/reservation rows | generation reservation row | matching reservation id converges |
| `ReleaseObjectGeneration` | Storage-shaped | generation reservation row | removes reservation row | missing matching reservation is idempotent |
| `CommitDirectPutObject` | Storage-shaped | object row, version/write-sequence state, reservation row | object row, segment manifest, stale reclaim rows | matching object generation/reservation converges |
| `CommitMultipartObject` | Storage-shaped | MPU rows, part rows, completed-MPU order, object state | object row, part manifest, selected segment rows, completed-MPU row, stale reclaim rows | matching upload completion converges |
| `DeleteObjectVersion` | Storage-shaped | exact object version row | removes version, writes reclaim metadata for live payload | matching version/generation converges |
| `InsertDeleteMarker` | Storage-shaped | object version/write-sequence state | delete marker row, optional stale reclaim metadata | matching bucket/key marker insertion converges |
| `PutObjectMetadata` | Request-shaped | exact object version row | tags, retention, legal hold, or ACL fields | matching mutation converges |
| `CreateStreamUpload` | Storage-shaped | target object/upload row for validation | stream session row | matching session row converges |
| `AppendStreamSegment` | Storage-shaped | stream session row and segment allocator | stream segment row, session segment allocator | matching segment row converges |
| `AbortStreamUpload` | Storage-shaped | stream session and staged segments | removes stream session/segments | matching session abort converges |
| `CommitStreamPart` | Storage-shaped | stream session, upload row, staged segments, existing part | multipart part row and staged segment rows | matching session/upload/part converges |
| `CreateMultipartUpload` | Storage-shaped | object generation allocators and reservation rows | multipart upload row and generation reservation row | matching upload row converges |
| `AbortMultipartUpload` | Storage-shaped | upload row, part rows, staged part segments | upload/part metadata cleanup rows | matching upload abort converges |
| `DeleteObjectPayloadReclaim` | Storage-shaped | reclaim root and manifest rows | removes reclaim metadata | matching reclaim root converges |

For row-shaped create commands, retry matching is exact over the stored row
published by the command. Matching only the original request fields is not
enough, because row fields such as timestamps, generation ids, and allocator
state are part of the command checksum and replay effect. For stream upload
session creation this includes the initial `next_segment_vid` allocator value.

`PutObjectMetadata` is the main remaining request-shaped object command. It
still records "put tags", "delete tags", "put ACL", "put retention", and "put
legal hold" mutations rather than an explicit post-image object metadata row.
That should be converted before Phase 7.3 if the binary canonical command
format is going to commit object metadata command payloads long term.

## Integrity And Divergence

Every persisted binary command-log entry must carry a checksum over its
canonical command encoding. The command-log hash chain links log index, PG,
epoch, previous hash, and command checksum. Checkpoints, snapshots, row blocks,
and range blocks must also carry checksums over their canonical binary
encodings.

Replica disagreement is never resolved by choosing the first or fastest answer.
The allowed outcomes are:

- replay from a valid log prefix
- rebuild a bad replica from a clean peer or checkpoint
- enter `Inconsistent`, `Peering`, or another fail-closed state when no safe
  authority exists

SQLite page checks, indexes, constraints, and SQL row scans are useful
diagnostics and implementation tools. They are not the long-term replicated
metadata integrity boundary.
