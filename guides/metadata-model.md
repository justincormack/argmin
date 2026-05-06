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
