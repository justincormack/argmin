<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Bounded Metadata Checkpoint Transfer

Status: **Next — separate workstream, implementation not started.** Recorded
2026-11-06 after withdrawing the uncommitted paging experiment from
[durable cleanup ownership](durable-generation-cleanup-ownership.md).
See the [roadmap](README.md). This document records the problem, constraints and
preferred direction; it does not approve the withdrawn interface or claim a fix.

## Why this surfaced during cleanup ownership

Cleanup owners must retain independently authenticated abandoned-finalization
reservation proofs until their release responsibility is settled. A review
identified that a valid accumulated resource table could exceed the checkpoint
decoder's 100,000-row limit. The follow-on experiment also exercised a checkpoint
larger than the 64 MiB RPC payload limit.

Neither limit is unique to cleanup owners: ordinary live metadata can already
exceed them. Cleanup obligations add another source of growth and expose the
existing mismatch between durable state accepted locally and state recoverable
through checkpoint loading/transfer. Introducing a general transfer redesign
inside the already complex cleanup protocol was the wrong workstream boundary.

Keep cleanup-specific records, semantic validation, canonical/checkpoint version
advances, immutable format evidence and custody integration in the cleanup plan.
Move **all checkpoint-size, streaming and aggregate-memory fixes here**. Owner
integration may proceed independently, but its production activation must not
claim transferability until this dependency is resolved. Do not discard an
accepted obligation to satisfy a transport limit.

## Problem inventory

### Pre-existing limitations, still present after withdrawal

1. **Writer/reader domain mismatch.** Checkpoint encoding can produce a table
   with more than 100,000 rows; the whole-checkpoint decoder rejects it.
   Durable candidate loading uses that decoder and skips undecodable entries.
   Locally recording a checkpoint therefore does not establish that it can be
   loaded after restart or used for recovery. Retain corruption/version
   rejection, but do not confuse a valid oversized checkpoint with absent
   recovery evidence.
2. **Whole-frame transfer ceiling.** Export and base installation carry complete
   checkpoints in RPC payloads bounded by 64 MiB, including envelope overhead.
   Candidate response construction can omit candidates which do not fit. A
   useful checkpoint must not become undiscoverable solely because of framing.
3. **Whole-state memory amplification.** Capture, candidate inspection, encoding,
   transport and decoding use complete byte buffers and/or complete row DTOs.
   Candidate loading fetches up to eight retained blobs for an epoch before
   decoding/selecting the requested count. Connection-count and frame-size
   limits are not process-wide byte admission; reducing the result count alone
   does not bound capture allocations. Audit intermediate copies, concurrent
   maintenance and ordinary inspection as well as explicit PG transfer.
4. **Compaction is not a bound on live PG size.** There is no corresponding
   production admission invariant guaranteeing that each valid PG fits either
   the row limit or the complete-frame limit. Transfer can fail precisely when
   replacement/recovery is required.

Source map (under `crates/storage/src/`):

| Responsibility | Sources |
| --- | --- |
| Durable capture, candidate loading, compaction and installation | `pg_store/command_log.rs` |
| Whole-checkpoint grammar and row/value bounds | `storage_rpc/metadata_command_codec.rs`, `storage_rpc/limits.rs` |
| Frame-limited candidate filtering and installed endpoints | `storage_node_server/server.rs`, `storage_node_server.rs` |
| Embedded/Unix metadata inspection and transfer | `node_client/local.rs`, `node_client/unix_rpc.rs`, `cluster/metadata_operations.rs` |
| Materialized table inventory and cleanup state | `pg_store/command_log.rs`, `pg_store/payload_staging*.rs` |

### Problems introduced by the withdrawn experiment

These findings explain why bounded pages alone were insufficient. They are
**not claims that the removed RPC endpoints remain reachable**.

- `CheckpointPageSource` owned a complete encoded `Vec`. The source session's
  `OpenCandidates` could retain eight such checkpoints; capture also involved
  raw SQL blobs and decoded DTOs. At 64 MiB each that is over 512 MiB for eight
  larger-than-frame candidates, before transient copies. With up to 1,024
  connections, even one 64 MiB export per connection admits roughly 64 GiB of
  retained encoded data. These are admitted worst-case budgets, not a measured
  production incident. Fixed expiry limits duration, not simultaneous memory.
- The receiver accepted bounded pages into a caller-supplied file spool but
  still decoded the entire checkpoint into memory at completion. It did not
  establish end-to-end bounded memory and introduced unwanted file ownership,
  disk-space and cleanup concerns.
- `OpenCandidates { limit: 0 }` reached a lower decoder which pushed a result
  before testing `len == limit`, returning candidates rather than none. The
  existing public helper already handles zero; the new server bypassed it.
  Future interfaces must specify zero behavior before any scan or allocation.
- Open/read/close handles, per-connection export state and a new page format
  expanded the storage interface before the full source/receiver protocol was
  agreed. The experiment had no migrated production client or streamed
  destination installation.

The page/spool reader, decoder-limit relaxation, source-session RPCs and their
tests have been removed from the uncommitted cleanup slice. Their functional
test passes did not prove aggregate resource safety. No new page format is part
of the current writer vector. Existing cleanup-related checkpoint/transfer
composite fixtures remain immutable and retained.

## What compaction can and cannot do

Current command-log compaction removes a checkpoint-covered log prefix and
retains bounded terminal receipt history (4,096 per epoch). Checkpoint retention
keeps up to eight snapshots per epoch across four epochs. These bound history
copies, **not the size of one snapshot**.

Live objects, versions/delete markers, user metadata, segments, multipart parts
and integrity witnesses, unfinished uploads, reclaim/cleanup obligations and
generation counters remain necessary materialized state. Compaction cannot
discard them merely to fit a frame. Physical database allocation, WAL size,
retained checkpoint copies and serialized live checkpoint size are different
quantities and need separate measurements.

More PGs can distribute keys, but all versions/manifests for one key remain on
its object PG. Moving a whole PG does not shrink it. We cannot guarantee a
64 MiB maximum without a separate admission/repartitioning contract that also
reserves recovery headroom. We also cannot claim large transfers are rare until
we measure representative PG populations.

Measure encoded bytes/rows by table, maximum and percentile PG sizes, retained
snapshot/WAL bytes, cleanup backlog/age, transfer frequency/duration and peak
process memory. Distinguish necessary live state from delayed cleanup. Prompt
safe retirement, removing proven redundancy and compact representation may
reduce typical size; never prune unresolved proofs or recycling-prevention
counters. Compression is a possible later measured optimization, not a safety
bound or a reason to add a production dependency now.

## Constraints and preferred direction

1. **No new application-managed spool files.** Use existing SQLite ownership,
   journaling and recovery. This does not mean SQLite's existing WAL stops
   growing; capacity/headroom and cancellation still need explicit budgets.
2. **Minimize the storage interface.** Prefer one scoped logical export/import
   stream with bounded framing internal to that operation. Do not expose a
   general-purpose checkpoint handle service with independently callable
   open/read/close operations. Audit and consolidate existing callers rather
   than adding a second permanent transfer path.
3. **Bound aggregate resources before capture.** Admit concurrent jobs and
   buffer bytes process-wide before loading/encoding checkpoint state. Select
   candidates lazily, one at a time, from bounded metadata descriptions. All
   layers, including a forwarding frontend, must apply backpressure and avoid
   collecting the complete stream or a complete destination DTO.
4. **Preserve exact authority and atomic visibility.** A transfer binds node,
   PG, source checkpoint/epoch/prefix, format vector, integrity evidence and
   destination authorization. Stream identity is not installation authority.
   Validate canonical grammar, topology and semantic state before publishing
   the destination; partial input must never become authoritative state.
5. **No new ordinary-write fsyncs.** This is a recovery/transfer redesign, not
   an additional PUT/append durability protocol. Avoid per-page commits. Retain
   normal durability settings and existing publication ordering.

### Initial destination: one longer transaction

Prefer incremental decoding/insertion under one destination SQLite transaction
initially, with final completeness, integrity, semantic and authority validation
before commit. Roll back on malformed input, EOF, cancellation, deadline expiry
or failed validation. A longer transaction is an accepted initial tradeoff to
avoid files and a separate staged-transfer lifecycle, **not** permission to hold
unrelated PG/physical locks or unbounded network waits.

Before implementation, define PG-local serialization and lock ordering for this
path, maximum admitted jobs, immutable operation deadlines, bounded buffers/WAL
headroom and route-publication interaction. Preserve the existing dirty-target,
allocation-lifetime, pending-command and exact retry checks. Response loss after
commit must converge by exact installed-state evidence, not blind replacement.
Replica validation must remain deterministic and not consult remote state or
independent clocks during apply. Semantic checks which currently take a complete
DTO need incremental SQL-backed equivalents without weakening their invariants.

### Source consistency and evidence: design decision still required

For retained immutable checkpoints, investigate incremental reading of the
existing SQLite checkpoint blob rather than copying it into a new `Vec` or file.
Pin its identity/lifetime against pruning. For a current checkpoint, choose a
consistent database view and streaming canonical encoder, or an existing
immutable captured checkpoint with the required prefix. Peering/quiescence at
the orchestrator is relevant but not a substitute for auditing all background
mutators and database snapshot semantics. Per-page reads must not mix states.

Resolve when total length, digests and the final seal are computed and verified
without a whole-buffer pass; a trailer or bounded extra pass may be appropriate.
Keep authenticated transport and exact source/proof validation. Audit integer,
row/value and SQLite blob limits: streaming removes frame-size coupling, not
every representation limit. Any remaining hard limit must have a coherent
durable admission/recovery contract, not silently drop accepted state.

### Deferred optimization: receive staging inside the existing database

If long transaction measurements justify it, receive chunks into a disposable
staging table in the existing metadata database, then combine/decode them under
one atomic installation transaction. Do not add this in the initial slice.

This is not automatically a SQLite `TEMP` table: the current `temp_store=MEMORY`
configuration would keep that data in RAM, while disk-backed TEMP would use a
separate SQLite-managed temporary database. An ordinary staging table would need
exact transfer identity, quotas, completion/freeze rules, restart/abandonment
cleanup and explicit exclusion from authoritative state digests/checkpoints.
The schema and those exclusions require their own reviewed format evidence.
Batch commits under existing WAL/FULL settings add durability work; measure and
batch them, never weaken main-database durability. Final installation must stream
from staged rows rather than concatenate them into another full allocation.

### Alternatives not selected

- Raising the single-frame/row limits postpones the mismatch and increases
  exposure without bounding memory.
- Whole-checkpoint in-memory export sessions with a semaphore could contain
  concurrency but still need a maximum checkpoint size and pre-allocation byte
  accounting; they do not meet the intended streaming/minimal-interface design.
- Application-managed file spools are excluded by the storage-space constraint.
- PG-wide replicated capacity reservation before new authority is minted would
  need row/byte accounting for concurrent, pending and uncertain work plus
  recovery headroom. A per-owner count or rejection at proof-retention time is
  insufficient. Do not substitute that larger protocol into cleanup ownership.

## Implementation sequence and acceptance

1. **Design/size audit first.** Inventory all checkpoint consumers, capture and
   copy sites, existing serialization, snapshot persistence, decoder limits and
   live-state growth. Measure representative PGs. Review the smallest complete
   source/receiver contract and global resource budgets before changing RPCs.
2. **Bounded codec/database pipeline.** Implement incremental canonical export
   and destination validation/installation under that contract. Separate durable
   validity from frame bounds without accepting impossible counts/fields.
   Include routine capture and candidate loading, not just network paging.
3. **Installed integration.** Use the scoped operation through embedded, Unix
   and TLS paths and real recovery/PG transfer callers. Remove superseded
   whole-state transfers where they would retain the same correctness gap.
   Small and large checkpoints must have identical validation/publication
   semantics; optimize framing only if useful.
4. **Failure/resource evidence and measurements.** Complete the matrix below,
   record throughput, peak memory, lock/transaction duration and WAL growth,
   and then close the external cleanup activation dependency. Receive staging
   or compression needs separate measured justification afterward.

Required regressions:

- More than 100,000 rows and more than 64 MiB, separately and together; include
  ordinary live metadata and 100,001 valid cleanup resource rows. Record/reopen
  source, transfer/install, record/reopen destination and compare exact state.
- Limits 0/1/maximum and malformed counts; zero must not fetch candidate bodies.
  Multiple large retained candidates must not be eagerly materialized.
- Concurrent authenticated callers, refused admission **before** capture, bounded
  encoder/decoder/forwarder memory independent of checkpoint size, no orphaned
  permits and no full-buffer allocation at completion.
- Wrong node/PG/epoch/prefix/topology, candidate substitution, unsupported formats,
  truncated/duplicate/reordered/trailing input, bad lengths/seals and malformed
  owner/reference/resource graphs. No partial destination visibility.
- Source mutation/pruning and coherent snapshot selection; publication/renewal,
  immutable deadline expiry, stalled peers, disconnect/cancel at each stage,
  final commit reply loss and exact retry. Preserve transfer authorization.
- Destination restart, transaction rollback and ENOSPC/WAL pressure; incomplete
  transfers preserve the prior authoritative state. Recovery must not require
  uncontrolled extra storage or per-page fsyncs to escape failure.
- Compaction/receipt retention and historical route dependencies still support
  recovery; no fix may forget live cleanup obligations or generation fences.

Follow [versioning](../guides/versioning.md) and the
[format ledger](../guides/storage-format-ledger.md): keep historical bytes,
append complete version-vector fixtures, and change each affected format once
per committed slice. Do not resurrect withdrawn page specimens as a supported
format. Use the repository's formatting, strict Clippy and full Nextest checks,
plus installed multi-node transfer/restart evidence; passing codec round trips
alone is not completion.
