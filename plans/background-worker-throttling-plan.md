# Background Worker Throttling Plan

## Context

The shard scavenger is now audit-only and no longer scans shard files while
holding a `PgStore` mutex. That removes the immediate availability bug where a
long filesystem walk could block normal metadata operations on the same PG.

There is still a separate operational concern: background workers can consume
IO, filesystem metadata cache, CPU, and database read bandwidth. On large
instances, a full shard scavenger audit may run for a significant fraction of
wall-clock time even though it is not on the correctness path. Other background
workers, such as lifecycle and reclaim workers, have similar scheduling and
visibility questions.

This is not part of the multihost transition. It needs control-plane
configuration and resource observability infrastructure that we do not have yet,
so the right near-term action is to track the work as a standalone plan.

## Status

Partially implemented.

The first availability slice now separates the audit-only shard scan from
backfill discovery and routine metadata-command checkpoint maintenance:

- audit-only scans retain their conservative production interval and do not run
  immediately at worker startup
- backfill discovery and metadata-command checkpoint scans retain canonical PG
  cursors across runtime-map publication and inspect at most eight PGs per tick
- backfill discovery and audit reference discovery use native-table keyset
  pages; pending metadata commands atomically publish indexed, fixed-width
  placed-reference child pages, and pending cursors bind the command epoch, log
  index, and checksum so slot replacement restarts traversal; every reference
  RPC page contains at most 64 entries
- backfill discovery has a 256-row per-tick budget, a separate 256-candidate
  verification budget, and a 50 ms cooperative time budget
- the accelerated backfill UAT cadence applies only to bounded candidate
  discovery, rather than also accelerating full shard audits and checkpoint
  scans
- audit passes page one metadata PG reference source per scheduler tick and
  partition references by data PG during collection, so each physical PG
  consumes one prebuilt index without rebuilding the cluster-wide set
- physical shard audits retain their pinned in-memory pass across ordinary
  runtime-map publication, inspect one PG per tick after reference discovery,
  and discard cross-generation negative-reference history; production progress
  ticks are separated by 250 ms and complete passes start no more frequently
  than every 30 minutes, while transient step errors retry after one second
- unreferenced row/file observations require confirmation in two completed
  passes, and missing-shard repair eligibility is checked against current
  authoritative metadata immediately before durable repair scheduling
- physical file discovery is authorized on the exact current storage node even
  when that node is the PG spare, without granting the node ordinary acting-set
  access

Node/prefix/file cursors within one PG, static manifest controls, and the
complete observability model below remain pending. Reference-page and PG pacing
remove the former all-PG bursts, but a single PG file scan is not yet item- or
time-bounded.

## Goals

- make background worker resource use configurable and observable
- prevent audit-only work from competing heavily with client reads and writes
- support large deployments where a full audit pass may be expensive
- keep production defaults conservative
- keep test defaults fast and deterministic
- make throttling behavior explicit and testable

## Non-Goals

- adding immediate deletion of unreferenced shard files
- changing S3-visible behavior
- adding ad hoc environment variables as the final control-plane interface
- relying only on OS-level IO priority as the primary control mechanism
- making every background worker share one policy before we understand their
  different correctness requirements

## Dependencies

This should wait until we have enough infrastructure to make the controls useful:

1. **Control-plane configuration**
   Operators need a supported way to inspect and change background worker
   settings. The first implementation can still use static config, but the plan
   should not assume hard-coded constants are the final interface.

2. **Resource observability**
   We need worker-level metrics before tuning is meaningful:
   - pass duration
   - time spent scanning
   - directories visited
   - files statted
   - database rows scanned
   - observations recorded/resolved
   - budget exits
   - scan errors
   - last completed full pass timestamp

3. **Operational defaults**
   Production defaults should favor low resource use over prompt audit
   completion. Test defaults can stay much faster.

## Target Model

Background workers should have explicit per-worker policies:

- enabled/disabled state
- sweep interval
- optional jitter
- per-run time budget
- per-run item budget, where applicable
- maximum concurrent worker count
- summary metrics for resource use and backlog/progress

For audit-only work, a budget-limited pass should be a normal outcome, not an
error. The worker should resume later from a stable cursor or restart safely if
the cursor is only in memory.

## Shard Scavenger Throttling

The shard scavenger should move from "full scan every interval" to a bounded
incremental scan.

### Desired Behavior

- each tick scans until either:
  - the shard tree is exhausted
  - the time budget is reached
  - the item budget is reached
- if budget is exhausted, the worker records a budget-exit metric and resumes
  from the next cursor position on a future tick
- a full pass completion records a timestamp and resets the cursor
- scan errors remain visible as audit observations or scan-incomplete events
- the worker never holds metadata locks while walking shard directories

### Candidate Cursor

The cursor can start as in-memory state:

- data PG
- node ID
- shard prefix directory
- position within the prefix, if needed

An in-memory cursor is acceptable because the work is audit-only. A crash can
restart the pass from the beginning without risking data loss.

If later operational evidence shows very large trees make restart-from-beginning
too wasteful, we can consider a durable cursor, but that should not be the first
step.

### Candidate Controls

Initial controls should include:

- `shard_scavenger.enabled`
- `shard_scavenger.interval`
- `shard_scavenger.jitter`
- `shard_scavenger.max_run_time`
- `shard_scavenger.max_files_per_run`
- `shard_scavenger.max_prefixes_per_run`

Production defaults should be conservative. Since the shard scavenger is
audit-only, a cadence such as tens of minutes to hours is more appropriate than
sub-minute scanning unless operators explicitly opt in.

## Other Background Workers

This plan should eventually cover all recurring background workers, but not all
workers have the same correctness requirements.

### Lifecycle Worker

Lifecycle is visible as delayed expiration/abort work. It can be throttled, but
policy decisions should consider AWS-compatible timing expectations and avoid
starving later candidates behind one problematic object or MPU.

Useful controls:

- enabled/disabled state
- sweep interval and jitter
- per-bucket and per-candidate budgets
- error isolation and progress metrics

### Reclaim Worker

Payload reclaim is correctness-adjacent for disk-space recovery but not part of
the immediate S3 response path. It already has durable roots/claims for crash
recovery, so throttling should focus on queue pressure, worker concurrency, and
physical delete pacing.

Useful controls:

- concurrent reclaim workers
- per-run delete budget
- queue depth/backlog metrics
- retry/error counters
- physical delete latency summaries

## Implementation Phases

### Phase 1: Observability

Add low-cardinality metrics for worker resource use and progress:

- shard scavenger pass start/finish counts
- duration summaries
- files/directories scanned
- DB rows read
- budget exits
- scan incomplete/error counts
- last successful full pass timestamp

Do not add high-cardinality per-shard metrics.

### Phase 2: Static Config

Add static configuration for shard scavenger cadence and budgets.

Keep the scope narrow:

- shard scavenger only
- config read at process start
- test defaults remain fast
- production defaults are conservative

### Phase 3: Incremental Shard Scans

Teach the shard scavenger to scan incrementally using an in-memory cursor and
time/item budgets.

Status: in progress. Backfill candidate discovery now has PG and native
metadata-row cursors plus row, candidate, RPC-page, and cooperative time
budgets. Routine metadata-command checkpoint maintenance has a PG cursor. The
physical audit now pages reference sources and advances one physical PG per
tick while retaining a pinned pass across ordinary runtime-map publication. It
still needs the node/prefix/file cursor and budgets described above to
hard-bound work within one PG.

Pending-command placed and reclaim-only references are published
transactionally as fixed-width pages in PG schema v6, so every discovery page
reads at most 64 references without decoding the potentially 2 MiB command
envelope under PG serialization. Repair discovery retains every exact metadata
cursor that authorizes one physical shard; revalidation accepts any still-live
authority and defers on read errors when none can be confirmed, preventing a
cleaned terminal slot from masking an applied-object reference.

Regression coverage:

- budget-limited scan resumes on a later tick
- budget-limited scan does not clear existing unresolved observations
- scan still reports malformed files/directories
- scan still does not hold PG mutexes during filesystem enumeration
- a full pass eventually completes under repeated budgeted ticks

### Phase 4: Control-Plane Integration

Expose background worker settings through the future control plane.

This phase should include:

- read current settings
- update settings safely
- validation of minimum/maximum values
- visibility into last pass/progress metrics

### Phase 5: Extend to Lifecycle and Reclaim

Apply the same policy shape to lifecycle and reclaim workers once shard
scavenger throttling has proven useful.

Do this worker-by-worker because the correctness and liveness constraints differ.

## Open Questions

- What production default cadence should audit-only shard scavenging use?
- Should shard scavenger ever persist its cursor, or is restart-from-beginning
  acceptable indefinitely?
- Do we need IO priority controls in addition to in-process budgets?
- What control-plane API should own background worker settings?
- Should per-worker budgets be global per process, per storage node, or per PG?
