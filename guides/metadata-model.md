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
state. The target runtime ownership model for allocating, pending, and applying
these commands is described in
[metadata-command-stream.md](metadata-command-stream.md).

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
[multihost transition Phase 7](../plans/multihost-transition-plan.md#phase-7-metadata-model-integrity-and-divergence-policy)
implementation has an explicit canonical binary digest inventory for the
command-owned durable serving, in-progress, and cleanup metadata:

- `bucket_subresources`
- `buckets`
- `completed_multipart_uploads`
- `multipart_part_segments`, including staging rows
- `multipart_parts`
- `multipart_reclaim_part_segments`
- `multipart_reclaim_parts`
- `multipart_reclaims`
- `multipart_uploads`
- `object_generation_reservations`
- `object_parts`
- `object_segment_reclaim_segments`
- `object_segments`
- `object_segments_reclaims`
- `object_version_counters`
- `objects`
- `pg_counters`
- `stream_upload_segments`
- `stream_uploads`

The legacy bucket-row write-drain counters, `write_reservations_blocked` and
`active_write_reservations`, remain transitional Phase 9 compatibility state.
The Phase 9.4 replacement rows, `bucket_write_reservations` and
`bucket_write_drains`, are bucket-PG-primary coordination authority, not
replica-wide command-owned metadata. They are therefore intentionally outside
the canonical full-PG command-state digest until Phase 9 either replicates them
through the command stream or defines a separate primary-owned coordination
integrity record. The Phase 9.4 replacement model and current publisher audit
are tracked in [bucket-write-drain.md](bucket-write-drain.md).

The full-PG encoding starts with a stable domain/version header. Each included
table digest encodes explicit table and column names, filter identity, row
count, and aggregate row-digest statistics. Each row digest uses typed SQL
values: `NULL`, integers, text, and blobs are separate binary value kinds; blob
values are encoded as byte length plus CRC64 of the blob contents so large
opaque payloads do not dominate the outer state digest. The digest does not use
SQLite `quote(...)` output or SQL row text as its integrity boundary. This list
remains deliberately explicit in code. Tables that are not yet included are
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
| `CreateBucket` | Storage-shaped | bucket absence / matching existing command-owned bucket row | command-owned bucket row | identical command-owned bucket row converges |
| `PutBucketVersioning` | Storage-shaped | command-owned bucket row, versioning transition rule | post-mutation command-owned bucket row | matching command-owned bucket row post-image converges |
| `PutBucketAcl` | Storage-shaped | command-owned bucket row | post-mutation command-owned bucket row | matching command-owned bucket row post-image converges |
| `PutBucketProperty` | Storage-shaped | command-owned bucket row | post-mutation command-owned bucket row plus property effect group | matching command-owned bucket row post-image converges |
| `PutBucketSubresource` | Storage-shaped | bucket row generation | bucket subresource row, generation mirrors | matching subresource effect converges |
| `MarkBucketDeleting` | Storage-shaped | drained command-owned bucket row | post-mutation deleting bucket row | matching deleting bucket row post-image converges |
| `AdvanceCompletedMultipartUploadSequence` | Storage-shaped | bucket completed-MPU sequence | advances bucket completed-MPU sequence to the command order | matching bucket/order converges |
| `ReserveObjectGeneration` | Storage-shaped | object generation allocators and live/reclaim/reservation rows | generation reservation row | matching reservation id converges |
| `ReleaseObjectGeneration` | Storage-shaped | generation reservation row | removes reservation row | missing matching reservation is idempotent |
| `ReserveObjectVersion` | Storage-shaped | object version counter and existing object versions | advances the per-key object version counter to the reserved version | matching bucket/key/version converges |
| `CommitDirectPutObject` | Storage-shaped | object row, reserved version/write-sequence state, reservation row | object row, segment manifest, stale reclaim rows | matching object generation/reservation converges |
| `CommitMultipartObject` | Storage-shaped | MPU rows, part rows, reserved object version, reserved completed-MPU order, object state | object row, part manifest, selected segment rows, completed-MPU row, stale reclaim rows | matching upload completion converges |
| `DeleteObjectVersion` | Storage-shaped | exact object version row | removes version, writes reclaim metadata for live payload | matching version/generation converges |
| `InsertDeleteMarker` | Storage-shaped | reserved object version/write-sequence state | delete marker row, optional stale reclaim metadata | matching bucket/key marker insertion converges |
| `PutObjectMetadata` | Storage-shaped | exact object version row | post-mutation live object metadata row | matching live object post-image converges |
| `CreateStreamUpload` | Storage-shaped | target object/upload row for validation | stream session row | matching session row converges |
| `AppendStreamSegment` | Storage-shaped | stream session row and existing segment rows | stream segment row | matching segment row converges |
| `AbortStreamUpload` | Storage-shaped | stream session and staged segments | removes stream session/segments | matching session abort converges |
| `CommitStreamPart` | Storage-shaped | stream session, upload row, staged segments, existing part | multipart part row and staged segment rows | matching session/upload/part converges |
| `CreateMultipartUpload` | Storage-shaped | object generation allocators and reservation rows | multipart upload row and generation reservation row | matching upload row converges |
| `AbortMultipartUpload` | Storage-shaped | upload row, part rows, staged part segments | upload/part metadata cleanup rows | matching upload abort converges |
| `DeleteObjectPayloadReclaim` | Storage-shaped | reclaim root and manifest rows | removes reclaim metadata | matching reclaim root converges |
| `DeleteCompletedMultipartUpload` | Storage-shaped | exact completed-MPU tombstone row | removes completed-MPU tombstone row | missing matching tombstone is idempotent |

For row-shaped create commands, retry matching is exact over the stored row
published by the command. Matching only the original request fields is not
enough, because row fields such as timestamps and generation ids are part of
the command checksum and replay effect. For bucket creation this includes the
bucket creation timestamp, raw encryption columns, execution generation, and
other command-owned bucket-table storage columns.

Stream segment payload generation allocation is local runtime state, not a
durable SQLite row. It only gives concurrent uncommitted stream appends distinct
placed payload shard keys. The durable command records the concrete
`stream_upload_segments.segment_vid` value that was used, and that segment row
is included in the canonical metadata digest.

Multipart upload state is canonical metadata. `CreateMultipartUpload` publishes
an exact `InProgress` upload row, and terminal object-PG commands serialize
through the pending command stream before deleting that row. Abort and complete
preparation are protected by the object-PG bucket lock while they snapshot
cleanup and install the pending command; abort does not write an intermediate
`Aborting` state before the abort command exists. Terminal commands also carry
active streamed UploadPart sessions and staged segment rows for that upload so
command apply removes their metadata and cluster cleanup can delete their staged
payload shards. `UploadState` values outside the command path are test-hook
state injection only.

Bucket metadata commands keep the AWS-facing operation split at the storage API
boundary, but the durable command carries the command-owned bucket-table
post-image. Local runtime columns such as write-drain counters are not part of
bucket command checksums, preimage equality, or retry matching. The
completed-MPU order sequence is command-owned: ordinary bucket-row post-image
commands preserve its current value, and multipart completion advances it
through `AdvanceCompletedMultipartUploadSequence` before publishing the object
completion command. Bucket property commands also carry the storage property
group being changed so replay can validate that only the intended bucket-row
columns changed. Raw bucket encryption columns are part of the row image;
matching only the effective encryption behavior is not exact
enough.

The bucket execution-generation counter in `pg_counters` is part of the
canonical digest. Bucket command construction reads a candidate generation from
that counter, but does not mutate it; command apply advances the counter in the
same transaction as the bucket row mutation. DeleteBucket begin uses the same
command stream: after the write-drain fence and emptiness check, it publishes a
`MarkBucketDeleting` bucket-row post-image rather than mutating the bucket state
or `pg_counters` directly.

Direct command-owned metadata mutation is not a production API. `PgMetadataStore`
is crate-private, its legacy direct mutators are test-only or explicit test
hooks, and `SharedStorageNode` does not expose production bucket/object
mutation helpers that bypass `StorageCluster`. Production metadata mutation
should enter through a routed command API, then use private `PgStore` helpers
inside metadata command apply. The named exception is finalized bucket row
deletion: `PgMetadataStore::delete_finalized_bucket` is only for the cluster
fanout step after `MarkBucketDeleting` has committed, new writes have drained,
and all visible data and reclaim roots have been removed. Each local PgStore
delete verifies the bucket row is already `Deleting`; an `Active` replica is
treated as divergence and the finalized-delete fanout fails rather than hiding
the mismatch.

`PutObjectMetadata` keeps the AWS-facing "put tags", "delete tags", "put ACL",
"put retention", and "put legal hold" distinctions at the coordinator/storage
API boundary, but the durable command carries the post-mutation live object row.
Retry matching compares that row image rather than reconstructing AWS request
semantics from materialized state.

## Persisted Integrity Record Inventory

Phase 7.4 starts from this inventory of persisted or soon-to-be-persisted
binary/integrity records. `metadata_command_log` now persists the canonical
bytes for applied command entries and a distinct canonical tombstone encoding
for abandoned entries. Checkpoint/range block persistence remains the main
replay gap.

| Record | Current Persistence | Encoded Bytes | Checksum Or Hash Coverage | Replay Role |
| --- | --- | --- | --- | --- |
| Metadata command canonical bytes | Generated by `MetadataCommandEnvelope` and persisted in applied `metadata_command_log.command_bytes` entries | `canonical_command_bytes`: domain magic, version, epoch, PG, log index, payload kind, and storage-shaped payload fields | `checksum_crc64` is CRC64 over those canonical bytes | Source bytes for command-log identity and future standalone replay |
| `metadata_command_log` row | Persisted in each PG SQLite store | Applied rows persist canonical command bytes; abandoned rows persist a distinct tombstone encoding with tombstone magic/version, epoch, PG, log index, and original command checksum; row keys and hash-link fields persist `cluster_epoch`, `pg_id`, `log_index`, `command_checksum`, `abandoned`, `previous_log_hash`, and `log_hash` | `command_checksum` is CRC64 over the persisted `command_bytes`; loaded rows verify the bytes' embedded epoch, PG, log index, and applied-vs-tombstone kind against the SQL row before use; applied rows must decode as a known payload kind and consume the whole command byte slice; `log_hash` links epoch, PG, log index, previous hash, and command checksum | Validates accepted command identity, duplicate/conflict handling, sparse-prefix advancement, abandoned-command identity, and hash-chain continuity; future replay can consume applied command bytes directly |
| `metadata_command_replica_state` row | Persisted singleton in each PG SQLite store | `cluster_epoch`, `applied_log_index`, `applied_log_hash`, `state_digest` | `applied_log_hash` is the current command-log prefix hash; `state_digest` is the current canonical full-PG digest at that prefix | Fast fail-closed check that materialized state still matches the accepted log prefix; not a checkpoint by itself |
| Canonical full-PG state digest input | Generated from the materialized SQLite serving view; only the digest value is persisted in replica state | Stable PG-state header plus explicit per-table digest entries. Each table digest covers table/range domain, explicit table and column names, range/filter identity, row count, and aggregate typed row-digest statistics for the explicit canonical inventory | `state_digest` is computed from per-table canonical digest stats maintained by SQLite row-change triggers during command apply. Restart validation recomputes the materialized digest from SQLite rows rather than trusting the cache | Detects materialized-row divergence and corruption; cannot replay state because the encoded row stream is not persisted as a block |
| Checkpoint/snapshot/range block | Not persisted yet | To be defined in Phase 7.5/Phase 10: kind/version, PG, epoch, covered log index, range identity, row count or range metadata, and canonical row bytes or range digest | Must carry a checksum over the whole canonical block | Future compact replay equivalence point. Phase 7.4 deliberately keeps checkpoint semantics deferred |
| Object payload bytes | Persisted as placed payload shards, outside metadata command-log/checkpoint records | Not included in metadata canonical bytes; metadata stores payload descriptors such as size, layout, placement references, segment/part CRC64, and EC shape | Payload shards have their own shard checksums; metadata descriptors and payload CRC64 values are covered by command/state encodings | Replay restores metadata references, not object bytes; payload recovery/repair validates shard data separately |

## Integrity And Divergence

Every persisted binary command-log entry must carry a checksum over its
canonical command encoding. The command-log hash chain links log index, PG,
epoch, previous hash, and command checksum. Checkpoints, snapshots, row blocks,
and range blocks must also carry checksums over their canonical binary
encodings.

Every command payload kind added to `MetadataCommandPayload` must be added to
the stable command-encoding test matrix. Those tests also run the applied
command-log verifier against the exact encoded bytes, so encoder/verifier drift
fails before a row can be accepted into the durable prefix.

Phase 7.4 does not introduce checkpoint/range blocks. Until those blocks are
introduced, local restart validation treats
the materialized SQLite state plus the retained command-log prefix as the
replay boundary. `LocalClusterMap::open` validates every opened PG's
`metadata_command_replica_state` by walking log entries `1..=applied_log_index`,
checking each entry's binary checksum, row/header identity, abandoned/applied
kind, and hash-chain links, then comparing the stored state digest with the
current canonical materialized metadata digest. Command apply keeps a
per-table digest cache maintained by SQLite row-change triggers, so advancing
the composed PG state digest does not require scanning growing metadata tables
on every command. Trigger installation and the first full cache refresh are
guarded by a durable bootstrap marker written in the same transaction, so an
interrupted repair is retried on the next open. Restart validation recomputes
the digest from materialized rows and never derives new trust from the current
materialized rows alone.
Missing or corrupted prefix entries, hash mismatches, materialized-row drift, or
a missing replica-state row on a nonempty PG fail cluster open for that replica
instead of silently accepting the state. After each opened replica validates
locally, `LocalClusterMap::open` also requires all
opened replicas for the PG to agree on the validated epoch, applied log index,
log hash, and canonical state digest. A stale but internally coherent replica is
therefore rejected as divergent until a later peering/repair phase can rebuild
or exclude it deliberately. This agreement check also rejects replicas that have
the same materialized metadata digest but a different accepted command-log
history; until standalone replay/checkpoint repair exists, matching rows are not
enough to prove a replica is clean.
Replica-state initialization is allowed only for a freshly initialized empty PG
with the schema baseline and no command log. Log entries beyond the applied
prefix remain retained tail entries, not a checkpoint.
Replay-state tests build representative bucket and object metadata through
`StorageCluster`, then reopen the local stores and require the accepted log
prefix, hash, digest, durable max log index, and materialized rows to remain
stable.

Digest mismatch repair, checkpoint-only replay, checkpoint-plus-tail replay,
checkpoint corruption handling, and command-log compaction are not implemented
by Phase 7.4. Until Phase 7.5/Phase 10 defines compact equivalence points and
repair authority, validated startup either accepts an agreeing acting set or
fails closed.

## Command Log Retention

Before durable checkpoint/range blocks exist, command-log retention is
conservative: every accepted command-log row at or below the applied prefix is
required for restart validation. Applied rows and abandoned tombstone rows are
retained under the same rule because both contribute to the command-log hash
chain. Sparse tail rows beyond the applied prefix are also retained; a missing
earlier command can still arrive later and let the prefix advance through that
tail. Pending commands remain retained until they converge or are durably
abandoned.

The materialized `state_digest` is a validation digest, not a compaction
authority. It proves that the current SQLite serving view matches the accepted
prefix; it does not contain enough bytes to replay state or safely discard the
prefix. Until a checkpoint records PG, epoch, applied log index, applied log
hash, canonical state or row/range bytes, and checksums over those canonical
bytes, command-log compaction must be an explicit no-op.

Current storage exposes per-PG command-log stats for retained entries,
abandoned rows, sparse tail rows, and missing applied-prefix rows. The
compaction entry point reports `UnsupportedUntilCheckpoint` and does not delete
rows. Future checkpoint-backed compaction may prune only log entries covered by
a durable checkpoint or equivalent compact proof that all required acting-set
replicas agree on; pending commands and sparse tail rows beyond that point must
remain retained.

Replica disagreement is never resolved by choosing the first or fastest answer.
The allowed outcomes are:

- replay from a valid log prefix
- rebuild a bad replica from a clean peer or checkpoint
- enter `Inconsistent`, `Peering`, or another fail-closed state when no safe
  authority exists

SQLite page checks, indexes, constraints, and SQL row scans are useful
diagnostics and implementation tools. They are not the long-term replicated
metadata integrity boundary.
