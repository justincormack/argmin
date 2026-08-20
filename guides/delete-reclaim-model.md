<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Delete And Reclaim Model

Status: active implementation note.

This guide explains why object delete in Argmin is only synchronous for
metadata visibility, while physical shard cleanup is deferred.

## Summary

`DeleteObject` is a synchronous metadata operation. It is not a promise that
all backing shard files have already been removed from disk when the request
returns.

The implementation intentionally separates:

- request-visible delete semantics
- background payload reclaim

This is required so reads that already hold storage-node read handles for a
payload generation can continue safely even after the object has become deleted
in metadata.

## What Is Synchronous

For a normal object delete request, the request path does all of the
following before returning success:

- authorize the delete
- evaluate conditional delete state
- delete or mutate object metadata
- record durable reclaim metadata for any live payload generation that was
  removed
- enqueue background reclaim work

From the client point of view, the object is deleted as soon as the metadata
change commits.

## What Is Asynchronous

Physical shard deletion happens later in reclaim work:

- shard files for the deleted payload generation are removed in the
  background
- reclaim metadata is then cleared
- bucket delete finalization may continue once reclaim roots are gone

So it is expected that shard files can remain on disk briefly after a
successful delete request.

## Why This Is Required

Argmin allows in-flight readers to keep using an already-authorized payload
generation through volatile read handles owned by the shard-owning storage
nodes.

If delete tried to synchronously remove all shard files before returning, it
would have to do one of:

- break those in-flight reads
- block the delete request on lease release and potentially slow readers

We do not want either behavior.

So the design rule is:

- metadata visibility changes synchronously
- payload bytes are reclaimed asynchronously once the shard-owning storage nodes
  can fence new reads and observe that no local read handle still needs them

## Read Handle Ownership

Read lifetime protection is intentionally not a metadata/database write. Reads
are ephemeral request state: if the process or node serving a read fails, that
read fails and the client retries from a fresh metadata snapshot.

The storage node that owns a shard file is the authority for local file lifetime:

- read paths must obtain storage-node read handles for the selected shard files
  before streaming payload bytes
- physical shard deletion must go through the storage-node reclaim/delete API
- the storage-node delete path must defer while local read handles are active
- once deletion is fenced or started, new read handles for that reclaimed
  generation must be rejected so the caller can resnapshot or fail cleanly

Durable metadata records reclaim work. It does not record every active reader.
This keeps reads cheap while preserving the rule that no shard-owning node
deletes a file it is currently serving.

## Worker Ownership

Payload reclaim workers must not depend on the process-local wakeup queue for
correctness. The queue is a latency hint; durable reclaim metadata is the source
of truth.

The Phase 9.6 model uses token-fenced durable reclaim ownership:

- object payload reclaim has one durable claim per object metadata PG; the
  claim identifies the exact payload root and includes the bucket incarnation
  so stale workers cannot release or steal a newer claim after bucket
  delete/recreate
- bucket delete finalization has one durable claim per deleting bucket
  incarnation, allowing independent buckets on the same PG to finalize in
  parallel
- a claim is token-fenced, expires if its worker stops, and can be stolen only
  after re-reading the durable root on the PG primary

Each frontend also serializes live object-payload reclaim executions by exact
payload root across queued workers, durable scans, and nested bucket-finalizer
calls. A same-owner retry may resume a retained durable claim only after the
previous local execution has left this single-flight boundary. This prevents
two executions from reentrantly acquiring the same node fence while still
allowing independent bucket finalizers and independent object roots to run in
parallel. Physical deletion remains idempotent because a worker can crash after
deleting some shard files but before clearing reclaim metadata.

Each frontend runs a bounded reclaim worker pool sized by both available CPU
parallelism and the installed metadata-PG count. Process-local root ownership
prevents duplicate queue hints from occupying multiple workers; durable claims
remain the cross-process authority. The shared maintenance admission limit is
at least the pool width, so adding workers increases actual cleanup throughput
rather than only increasing threads waiting for one permit.

Fresh process-local queue hints and eligible deferred retries receive fair
worker admission. Sustained foreground cleanup hints must not starve a root
that already made partial durable progress and entered its retry cooldown; the
worker alternates between the two sources whenever both have runnable work.
Bucket finalization distinguishes productive bounded scan continuation from a
blocked retry. A clean frontier advance receives a small immediate burst so a
bucket spanning a few scan windows completes without returning to the back of
the global retry queue. Claim contention, visible data, outstanding reclaim,
and transient failures retain cooldown and fair rotation.

New `DeleteBucket` operations reserve finalizer capacity before installing the
durable deleting state. The process rejects distinct new deletions with
`SlowDown` when admitted plus outstanding finalizers reach a worker-scaled
high-water mark. Exact retries for an already admitted bucket incarnation do
not consume another slot. One per-root state tracks every in-flight retry and
whether durable work remains outstanding, so finalization racing an exact retry
cannot temporarily free and then overcommit capacity. Failed begin attempts
release their reservation only after rollback is proven; retained durable
attempts keep capacity while background begin recovery runs. Successful
attempts atomically promote the root to queued finalization, and terminal
cleanup releases capacity after the final in-flight retry exits. Durable state
remains the recovery authority, so this backpressure is an admission bound
rather than a correctness queue. Recovery-discovered begin roots consume the
same capacity. The per-incarnation capacity record retains each exact begin
execution generation: stale roots and cross-runtime duplicate hints retire only
that exact begin, while a successful begin atomically promotes it to finalizer
work. Only terminal finalization clears all remaining begin and finalizer state
for the bucket incarnation.

Terminal cleanup is retryable protocol state, not best-effort cleanup. If a
reclaim or bucket-finalizer metadata command has already become terminal but
the matching durable claim or pending cleanup marker survives a crash, startup
and reopen recovery must preserve enough token-fenced identity to clear the
matching claim before unrelated work on that PG is blocked indefinitely.

## Operational Consequences

- `DeleteObject` success means future reads observe the object as deleted
  according to normal metadata semantics
- it does not mean the backing shard files are already gone from disk
- `DeleteBucket` may remain pending until payload reclaim roots drain; active
  reads can delay this indirectly by making reclaim defer physical shard deletion

This is expected behavior, not evidence of leaked object data by itself.
