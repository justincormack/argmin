# Metadata Write Amplification Plan

## Context

This plan tracks a performance-only follow-up discovered while investigating
slow disk-backed test runs. It is not part of the current shard repair or Phase
11 correctness work.

The strongest reproducer is:

```bash
TMPDIR=$PWD/tmp cargo nextest run -p s3-tests --test versioning \
  test_versioning_list_object_versions_oversized_max_keys_returns_at_most_1000_entries
```

The test is a useful stress case because it creates many versions for one key
and then deletes/cleans them up. That concentrates metadata work into one
bucket PG and makes SQLite WAL write amplification easy to observe.

## Status

Deferred.

The current behavior is not an S3 semantic bug. It is a metadata durability and
digest-maintenance cost issue that becomes very visible when test temporary
directories are on disk instead of tmpfs.

## Observed Data

From a straced UAT run of the oversized max-keys versioning test:

- final UAT data directory: about `169 MiB`
- metadata `pwrite64` traffic: about `1.66 GiB`
- `metadata.db-wal` traffic: about `1.58 GiB`
- hot PG command log rows: `4010` on each of 6 storage nodes
- hot PG digest revision: `14025` on each of 6 storage nodes
- hot PG retained reclaim rows after the test:
  - `object_segments_reclaims`: `995`
  - `object_segment_reclaim_segments`: `995`
  - live `objects`: `0`
  - live `object_segments`: `0`

The hot command stream was regular:

- `1001` `ReserveObjectGeneration`
- `1001` `ReserveObjectVersion`
- `1001` `CommitDirectPutObject`
- `1001` `DeleteObjectVersion`
- `6` `DeleteObjectPayloadReclaim`

The `metadata_digest_revision=14025` count is therefore plausible: each version
causes roughly 14 digest-tracked table mutations across reservation, version
counter, write counter, object row, object segment, reclaim, and delete/restore
state updates.

## Current Amplification Shape

Metadata commands are applied in one immediate SQLite transaction. Nested
metadata helpers correctly avoid opening a second transaction when one is
already active.

The amplification comes from what happens inside that transaction:

1. The logical command mutates digest-tracked metadata tables.
2. Each row-level insert/update/delete fires metadata digest triggers.
3. Each digest trigger updates:
   - `metadata_table_digests`
   - `metadata_digest_revision`
4. Each accepted command also records metadata command log state:
   - insert `metadata_command_log`
   - update the same row with `previous_log_hash` and `log_hash`
   - update `metadata_command_replica_state`

For this workload, the row-level digest trigger model turns about `14k` logical
metadata row mutations into about `42k` table-row updates before command-log
and replica-state writes are counted. SQLite WAL then writes full pages, so
small row changes become large disk traffic.

## Goals

- reduce metadata WAL write amplification without weakening metadata command
  replay, replica digest validation, or crash safety
- keep AWS-visible behavior unchanged
- keep the oversized max-keys versioning test as a regression benchmark
- preserve clear invariants for metadata digests and command-log hashes

## Non-Goals

- do not special-case this one S3 test
- do not weaken metadata digest validation to make tests faster
- do not hide the issue by requiring tmpfs for correctness tests
- do not combine this work with shard repair or multihost correctness changes

## Candidate Improvements

### Replace row triggers with direct digest updates

The largest likely win is replacing per-row SQLite triggers with explicit
digest maintenance inside metadata command apply paths.

The direct model could update digest state once per command or once per touched
table, instead of once per row mutation. This should reduce writes to
`metadata_table_digests` and `metadata_digest_revision` significantly.

Constraints:

- illegal missed digest updates must be hard to express
- tests should compare direct digest state against a full recomputation
- crash and replay behavior must remain deterministic
- command replay must still detect materialized metadata divergence

### Coalesce command-log hash writes

Today command-log rows are inserted with `previous_log_hash` and `log_hash` as
`NULL`, then updated after insertion when the prefix advances.

For the common append-at-tail case, the previous hash is already known from
`metadata_command_replica_state`. Inserting the row with both hashes populated
could remove one update per command on the hot path.

This is lower-risk than digest rewrites, but likely a smaller win.

### Revisit WAL checkpoint policy

The current SQLite default `wal_autocheckpoint` behavior contributes repeated
WAL checkpoint writes once the WAL reaches about 1000 pages. Changing checkpoint
policy may improve disk-backed tests, but this should be treated as an
operational/storage-engine tuning question, not the first correctness-preserving
optimization.

Any checkpoint change needs benchmarks on:

- tmpfs
- local SSD
- slower disk-backed temp directories
- large metadata command streams
- mixed read/write workloads

### Reclaim command batching

The test leaves many object reclaim rows and then drains them in a few
`DeleteObjectPayloadReclaim` commands. There may be room to reduce intermediate
metadata churn by batching reclaim publication or cleanup more effectively.

This should stay secondary until digest and command-log write amplification are
measured, because the current write volume is mostly explained by the generic
metadata accounting path.

## Benchmark / Regression Target

Use the oversized max-keys versioning test as the first benchmark because it is
small, repeatable, and concentrates metadata churn:

```bash
TMPDIR=$PWD/tmp cargo nextest run -p s3-tests --test versioning \
  test_versioning_list_object_versions_oversized_max_keys_returns_at_most_1000_entries
```

Useful measurements:

- wall-clock time
- total `metadata.db-wal` bytes written
- WAL fsync count
- command-log rows by PG
- metadata digest revision by PG
- row counts for reclaim tables after cleanup
- final `metadata.db` and `metadata.db-wal` sizes

## Open Questions

- Can we make digest updates command-scoped without making missed updates
  easier than the current trigger model?
- Should digest maintenance be generated from a single table mutation API so
  row writes and digest writes remain mechanically coupled?
- Can command-log hash insert-at-tail be made single-write without weakening
  conflict detection for out-of-order or recovered entries?
- Is the reclaim row churn acceptable once digest maintenance is cheaper?
- What production checkpoint policy gives good latency without excessive WAL
  growth?
