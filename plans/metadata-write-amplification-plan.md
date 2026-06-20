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

Analysis needed before optimization.

The current behavior is not an S3 semantic bug. It is a metadata durability and
digest-maintenance cost issue that becomes very visible when test temporary
directories are on disk instead of tmpfs.

The original analysis over-focused on metadata digest triggers. The trigger
work happens inside the same SQLite transaction as the command apply, so it
does not add a separate durability boundary or per-trigger fsync. Triggers can
still dirty extra pages, add CPU, and increase WAL frames, but the current
measurements do not show that they are the leading cost. For this workload, the
stronger signal is shard durability: EC 4+2 writes each tiny object shard to six
local stores, and each local shard write currently performs both a temp-file
`fdatasync` and a parent-directory `fsync`.

For this reproducer, `plans/shard-write-group-commit.md` is likely the
higher-value optimization plan. Even batching two shard updates per durability
flush could plausibly remove about half of the shard sync boundaries in the hot
path, which would attack the largest measured elapsed-time component. Metadata
WAL work should still be understood, but it is probably not the first
optimization to implement for this slowdown.

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

### 2026-06-20 measurement update

The test was rerun locally with `TMPDIR` on disk and `ARGMIN_KEEP_TEST_DIRS=1`.
The cluster shape was EC 4+2, so both metadata and shard durability work happen
on six storage nodes. This must be accounted for before drawing conclusions
from cluster-wide byte or fsync totals.

Baseline run:

```bash
env ARGMIN_KEEP_TEST_DIRS=1 \
  TMPDIR=$PWD/tmp/metadata-write-analysis \
  cargo nextest run -p s3-tests --test versioning \
  test_versioning_list_object_versions_oversized_max_keys_returns_at_most_1000_entries
```

- wall-clock: about `40.1s`
- final data directory: about `72 MiB`
- one hot `pg-0000` database on each of six nodes
- final `metadata.db-wal`: about `4.1 MiB` per node
- final `metadata.db`: about `2.9 MiB` per secondary node, `3.4 MiB` on
  `node-0000`
- runtime SQLite settings from code:
  - `journal_mode=WAL`
  - `synchronous=NORMAL`
  - `temp_store=MEMORY`
  - `foreign_keys=ON`
  - `recursive_triggers=ON`
- post-run `sqlite3` inspection showed:
  - `page_size=4096`
  - `wal_autocheckpoint=1000`
  - `cache_size=-2000`
  - `cache_spill=483`

Note that `PRAGMA synchronous` is connection-local. Querying it with a fresh
`sqlite3` connection after the run reports that client connection's setting,
not necessarily Argmin's runtime connection setting. The Argmin runtime setting
comes from the schema initialization code.

The command stream is not stable unless reclaim timing is controlled. Different
valid runs finished with different amounts of background reclaim work:

- baseline run: `4010` command log rows, including `3`
  `DeleteObjectPayloadReclaim` commands
- sync-traced run: `4301` command log rows, including `294`
  `DeleteObjectPayloadReclaim` commands
- pwrite-traced run: `4243` command log rows, including `236`
  `DeleteObjectPayloadReclaim` commands
- timed I/O trace run: `4148` command log rows, including `141`
  `DeleteObjectPayloadReclaim` commands

Any future benchmark needs an explicit measurement window: before reclaim
drain, after full reclaim drain, or both reported separately.

`strace -ff -e trace=fsync,fdatasync -yy` on the same single test showed:

- total sync calls: `13161`
- shard temp-file `fdatasync`: `6006`
- shard parent-directory `fsync`: `6006`
- `metadata.db-wal` `fsync`: `748`
- `metadata.db` `fsync`: `377`
- `metadata.db-journal` `fsync`: `12`

This is the key correction to the original diagnosis: sync call count is
dominated by shard durability, not SQLite.

`strace -ff -e trace=pwrite64,pwritev -yy` reproduced the previous SQLite
write-volume scale:

- total SQLite pwrite traffic: `1,636,298,856` bytes
- `metadata.db-wal`: `1,543,635,952` bytes
- `metadata.db`: `92,659,712` bytes
- `metadata.db-journal`: `3,144` bytes

Per-node WAL write traffic was:

- `node-0000`: about `352.6 MiB`
- `node-0001` through `node-0005`: about `238.2 MiB` each

`node-0000` has extra bucket/control-plane work, but most of the total is the
same replicated PG metadata work repeated across six nodes.

`strace -ff -T -e trace=write,writev,pwrite64,pwritev,fsync,fdatasync -yy`
gave this rough syscall elapsed-time split:

- shard data `write`: `6006` calls, `24024` bytes, about `0.14s`
- shard temp-file `fdatasync`: `6006` calls, about `13.62s`
- shard parent-directory `fsync`: `6006` calls, about `13.26s`
- SQLite WAL writes: `736984` calls, about `1.52 GiB`, about `5.49s`
- SQLite DB writes: `22229` calls, about `91.0 MiB`, about `0.21s`
- SQLite WAL `fsync`: `730` calls, about `4.00s`
- SQLite DB `fsync`: `368` calls, about `1.81s`

For categorized shard and SQLite syscall elapsed time in that run:

- shard write plus sync: about `27.0s`
- SQLite write plus sync: about `11.5s`
- shard sync only: about `26.9s`
- SQLite sync only: about `5.8s`

These numbers are syscall elapsed time under tracing, so they should not be
read as exact application wall-clock attribution. They are good enough to show
that shard durability is the leading measured I/O cost for this test. The
shard-write batching direction in `plans/shard-write-group-commit.md` is
therefore likely more valuable for this workload than a metadata-trigger
rewrite.

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

The original analysis treated row-level digest triggers as the likely dominant
amplification source. That is not established. The triggers do dirty additional
SQLite pages, but they are not separate transactions and do not add fsyncs by
themselves. The current measured cost model is:

1. EC 4+2 multiplies per-node metadata and shard durability work by six.
2. Shard durability performs two sync calls per local shard write.
3. SQLite WAL write traffic is large, but SQLite sync elapsed time is smaller
   than shard sync elapsed time for this workload.
4. Background reclaim timing changes the command stream and must be controlled
   before comparing optimization results.

Metadata write amplification remains real, but the first-stage task is
attribution, not a digest-trigger rewrite.

## Goals

- attribute disk-backed test cost between shard durability, SQLite WAL writes,
  SQLite syncs, reclaim timing, and metadata digest maintenance
- reduce metadata WAL write amplification where it is shown to matter, without
  weakening metadata command replay, replica digest validation, or crash safety
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

This was previously listed as the largest likely win. That is no longer
justified by the measurements above.

Replacing per-row SQLite triggers with explicit digest maintenance inside
metadata command apply paths may reduce WAL frames and CPU, but it should be
treated as a hypothesis. The triggers run inside the command transaction and do
not create extra durability boundaries.

The direct model could update digest state once per command or once per touched
table, instead of once per row mutation. This could reduce writes to
`metadata_table_digests` and `metadata_digest_revision`, but the value needs to
be measured against the larger shard durability cost and the command-log and
checkpoint costs.

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

The 2026-06-20 reruns showed that reclaim timing changes the measured command
stream materially. Reclaim batching may matter, but first the benchmark needs a
stable measurement boundary so that one run is not measuring three reclaim
commands while another is measuring hundreds.

### Shard write group commit

Although this document started as a metadata write amplification plan, the
current measurements point at shard durability as the larger cost in the
oversized versioning test. The relevant follow-up is
`plans/shard-write-group-commit.md`.

In the timed trace, `6006` local shard writes generated `6006` temp-file
`fdatasync` calls and `6006` parent-directory `fsync` calls. The shard data
itself was tiny (`24024` bytes total), but those durability calls accounted for
about `27s` of traced syscall elapsed time. Batching shard writes and directory
syncs should therefore be evaluated before metadata digest rewrites for this
specific slowdown. A batch size of only two shard updates would already be
expected to cut the number of shard durability boundaries by roughly half when
the workload has enough concurrency to keep the batcher fed.

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
- shard temp-file `fdatasync` count and elapsed time
- shard parent-directory `fsync` count and elapsed time
- SQLite `pwrite64` bytes and elapsed time by `metadata.db`,
  `metadata.db-wal`, and `metadata.db-journal`
- SQLite `fsync` count and elapsed time by `metadata.db`,
  `metadata.db-wal`, and `metadata.db-journal`
- command-log rows by PG
- command-log rows by command kind
- metadata digest revision by PG
- row counts for reclaim tables after cleanup
- final `metadata.db` and `metadata.db-wal` sizes
- whether the measurement was taken before reclaim drain, after full reclaim
  drain, or across both

## Open Questions

- Can we make digest updates command-scoped without making missed updates
  easier than the current trigger model?
- How much WAL traffic is specifically from digest maintenance, command-log
  rows, command-log hash updates, indexes, and checkpoint writes?
- Should digest maintenance be generated from a single table mutation API so
  row writes and digest writes remain mechanically coupled?
- Can command-log hash insert-at-tail be made single-write without weakening
  conflict detection for out-of-order or recovered entries?
- Is the reclaim row churn acceptable once digest maintenance is cheaper?
- What production checkpoint policy gives good latency without excessive WAL
  growth?
- How much wall-clock improvement does shard write group commit provide for
  this test compared with SQLite-level tuning?
