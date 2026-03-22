# Shard Write Group Commit

## Problem

The remaining write-path gap is now in durable shard-file commit cost, not in
HTTP, auth, or bucket/object PG coordination.

Recent `warp` traces and isolated benchmarks show:

- tiny-object `PUT` is much better after the direct single-segment fast path,
  but still slower than rustfs
- `256 KiB` `PUT` is still far behind rustfs even though it is already on the
  direct single-segment path
- the current `PUT` bottleneck is the durable shard-file path in
  `SharedStorageNode::write_shard_files` / `PgStore::write_shard_files_durable`

For the traced `256 KiB` `PUT` run:

- `response_body_complete`: avg about `81.5 ms`, p50 about `23.4 ms`
- `Coordinator::put_object`: avg about `78.2 ms`, p50 about `20.0 ms`
- `SharedStorageNode::write_shard_files`: avg about `73.4 ms`, p50 about
  `17.8 ms`, p95 about `427 ms`, p99 about `529 ms`

So almost all of the remaining traced `PUT` time is in shard durability.

## Current write shape

Today a direct single-segment `PutObject`:

1. encodes one EC shard batch
2. writes all shard temp files
3. `sync_data()`s each shard file
4. renames each shard into place
5. `fsync`s each affected shard parent directory
6. publishes shard rows and object metadata

This preserves the current invariant:

- shard files may exist before metadata rows
- reads only treat metadata publication as visibility

That invariant is good and should not change.

## Goal

Reduce fixed durable-write cost, especially for small and medium writes, by
grouping shard durability work across multiple requests while preserving exact
visibility and crash-safety semantics.

## Why group commit

This is the main remaining lever that attacks the actual measured bottleneck:

- not another lock or cache cleanup
- not another HTTP optimization
- not another tiny-object special case

The expensive part now is that each request pays its own file durability
boundary. Group commit would amortize that cost across multiple requests.

This is especially attractive for:

- tiny direct `PutObject`
- medium single-segment `PutObject` like `256 KiB`
- later, direct single-segment `UploadPart`

## Correctness constraints

We must keep these properties:

1. No object or part becomes visible before its shard data is durable.
2. A crash may leave orphan shard files, but must not leave visible metadata
   pointing at non-durable shard data.
3. Failure of one request in a batch must not make another request visible
   incorrectly.
4. The design must still work when shard writes are remote in the multi-host
   future.

These imply:

- data durability and metadata visibility remain separate phases
- metadata publish still happens after successful durability
- partial batch failure needs explicit per-request success/failure accounting

## Intended design

### Durability batcher

Introduce a storage-node-local durability batcher for shard-file writes.

Requests submit prepared shard-file work items containing:

- target shard PG
- shard key
- shard bytes
- request-local completion handle

The batcher collects work for a short window or until size thresholds are met,
then runs one durability cycle for the batch.

### Batch commit phases

For one batch:

1. create and write all temp shard files
2. `sync_data()` all temp shard files
3. rename all temp shard files into place
4. `fsync` each touched parent directory once
5. mark the successful work items as durable
6. let the originating request publish metadata for those successful items

This keeps the same visibility rule as today.

### Metadata publish

The durability batcher should not publish object metadata itself.

Instead, each request:

1. waits for its shard durability work to complete
2. then acquires the required PG lock(s)
3. publishes shard rows and object metadata using the existing commit path

That keeps the metadata step request-scoped and avoids coupling unrelated
object commits into one cross-request metadata transaction.

### Failure model

If a batch durability cycle fails:

- requests whose shard files definitely reached durable completion continue
- requests whose shard files did not complete fail
- orphan shard files are acceptable and can be reclaimed later
- no request publishes metadata until its own durable success is known

## Phasing

### Phase 1: Measurement

Add finer tracing inside `write_shard_files_durable` to split:

- temp file create/write
- per-file `sync_data`
- rename loop
- parent `fsync`

That confirms exactly which durability step dominates on real runs.

### Phase 2: Prototype local batcher

Add a single-node durability batcher behind the existing direct single-segment
`PutObject` path only:

- no behavior change for reads
- no behavior change for streaming multi-segment paths yet
- no behavior change for multipart yet

This limits risk while testing the basic crash-safety model.

### Phase 3: Extend to other write paths

If the prototype is successful:

- direct single-segment `UploadPart`
- streamed finalize/object publish paths where they use the same shard-file
  durability primitive

## Multi-host notes

This is compatible with the expected future remote-shard design.

In the multi-host case, the same logical boundary still exists:

1. shard writes acknowledged durable by shard owners
2. metadata published after durability confirmation

The owner-side batching mechanism may later become:

- local durable-file batching on each shard host
- plus owner-side aggregation of per-shard durability acknowledgements

So this plan is not wasted by multi-host work.

## Risks

- batching may improve throughput but worsen tail latency if flush windows are
  too large
- failure accounting must be exact, especially around partial success
- shutdown behavior needs a clean drain or explicit request failure
- tests need to cover crash-adjacent states carefully

## Validation

Before implementation is considered complete:

- isolated `warp put --obj.size=4KiB`
- isolated `warp put --obj.size=256KiB`
- mixed `warp` rerun
- regression tests for:
  - durable-before-visible invariant
  - failed batch requests not publishing metadata
  - successful batch neighbors still publishing correctly
  - orphan shard cleanup remaining safe

## Recommendation

This is likely the most important remaining write-path optimization, but it is
large enough to treat as a dedicated subsystem change rather than a quick
follow-on patch.

The next step should be Phase 1 instrumentation, then a narrow Phase 2
prototype on direct single-segment `PutObject` only.
