# In-flight Read / Reclamation Plan

## Context

This issue was discovered while moving authorization into `server-core`, but it
is not primarily an auth problem and should be tracked separately.

The key observation is:

1. authorizing a read at operation start is fine
2. the real risk is that an in-flight read may lose access to backing data if a
   concurrent delete or overwrite reclaims shards part way through the read

This plan is about object-lifecycle and data-availability semantics, not about
the HTTP/core/storage boundary.

## Problem Statement

Some read paths snapshot object metadata, release metadata guards, and then
perform a sequence of later shard or chunk reads.

At the same time, delete and overwrite paths remove metadata visibility and
then immediately reclaim shard data for the old generation.

That creates a race:

1. read starts against object generation N
2. read successfully consumes early parts or chunks
3. delete or overwrite removes metadata and reclaims remaining shard sets for N
4. later steps of the read fail even though the operation had already started

## Affected Surface

The risk is clearest for read paths that do not consume all needed shard data
under a single metadata snapshot:

1. `GetObject` for multipart-manifest objects
2. `GetObject` for chunk-manifest objects
3. `GetObjectRange` for those same layouts
4. `GetObjectPart` / `HeadObjectPart` where the source part is backed by
   streaming chunk manifests
5. copy-source reads that reuse the same internal data-reconstruction helpers

Simple `HEAD` is not affected because it does not perform later shard reads.

Simple single-generation non-multipart reads are less exposed because they do
not walk a sequence of later part or chunk manifests after releasing the
initial metadata snapshot.

## Broader Scope Than Delete

This is not only `delete_object`.

The same immediate-reclamation pattern exists in:

1. unversioned overwrite paths that delete prior-generation shards after
   committing a new generation
2. stream-put finalize paths that clean stale chunk manifests and shards
3. multipart completion paths that replace prior generations and then reclaim
   the old shard data
4. delete paths for both current-object delete and explicit-version delete

So the right framing is:

1. in-flight reads
2. versus immediate reclamation of old generations

## Current Serialization Points

The relevant serialization points are:

1. per-PG mutexes in `SharedStorageNode`
2. the bucket stripe lock for some write paths

These are not sufficient to protect long reads because:

1. read paths do not hold all relevant metadata and shard PG guards for the
   full duration of a multipart or chunk-manifest read
2. bucket locking is orthogonal to the lifetime of underlying shard data
3. delete and overwrite paths intentionally drop PG guards before reclaiming
   old shards to avoid deadlock and broad lock retention

So this is not a missing-authz-lock issue. It is a data-lifetime issue.

## Why an AWS ACL Revocation Test Is Not Useful

The previously discussed AWS experiment about revoking public-read mid-transfer
does not answer the important question here.

Once a `GET` has started, the client-facing S3 API does not have a meaningful
surface for a late authorization failure part way through the response. So
authorize-at-operation-start semantics are acceptable.

The issue we need to solve is not "should auth be rechecked mid-stream?".
It is "should already-started reads keep their backing data available long
enough to finish?".

## Current Failure Modes

There are two related races.

### 1. Metadata follow-on race

Some read helpers look up later chunk manifests during the read instead of
fully snapshotting them up front.

If a delete or overwrite removes those metadata rows first, the read can fail
before shard reclamation is even reached.

### 2. Physical shard lifetime race

Even when enough metadata has been snapshotted, delete and overwrite currently
reclaim old shard sets immediately after dropping metadata guards.

So a read that has already decided which old generation it is reading can still
fail on a later shard read.

## Recommendation

Do not solve this by holding bucket locks across large reads.

That is too coarse and ties correctness to a lock that is not really about
underlying shard lifetime.

Instead, solve it in two layers:

1. snapshot all metadata needed for the read before releasing metadata guards
2. stop reclaiming old shard data synchronously on delete and overwrite

That gives a cleaner model:

1. metadata visibility changes immediately
2. physical reclamation becomes deferred cleanup
3. already-started reads that snapped a manifest can finish

## Proposed Implementation Phases

### Phase 1: Reproducer tests

Add deterministic coordinator-level tests that force the race.

Target cases:

1. multipart `GetObject` while `DeleteObject` runs after the first part read
2. chunk-manifest `GetObject` while `DeleteObject` runs after the first chunk
3. multipart or chunk-manifest `GetObject` while an unversioned overwrite
   replaces the object and reclaims the old generation
4. copy-source read against an old generation while delete or overwrite reclaims
   it

The point is to prove the current behavior and then lock in the intended fix.

### Phase 2: Snapshot completeness

Change read paths so they collect all metadata needed for the current read
before releasing metadata guards.

This likely means:

1. multipart reads prefetch any streaming-part chunk manifests they may need
2. chunk-manifest reads prefetch the full chunk list they will traverse
3. copy-source helpers reuse the same fully snapshotted internal representation

This fixes the metadata-half of the race.

### Phase 3: Deferred reclamation

Change delete and overwrite paths so they stop deleting old shard data
synchronously in the foreground operation.

The current write or delete should:

1. remove namespace visibility or install the new current generation
2. record the old shard sets or chunk manifests as reclaimable
3. return success without physically removing those shards inline

Then a later cleanup path can reclaim the data.

Possible implementations:

1. a small durable cleanup queue in metadata storage
2. tombstoned reclaim records scanned by a background sweeper
3. a simpler best-effort local journal if durability requirements are looser

The important property is:

1. foreground reads do not depend on immediate physical reclamation

### Phase 4: Expand reclamation coverage

After the first delete/read fix, apply the same deferred-reclamation model to
all old-generation cleanup sites:

1. unversioned overwrite in normal `PutObject`
2. stream-put overwrite cleanup
3. multipart-complete old-generation cleanup
4. any other path that eagerly deletes stale chunk or part shard sets

This should end with one consistent old-generation reclamation mechanism, not a
mix of deferred and immediate cleanup.

## Design Constraints

1. do not hold bucket locks for the full duration of large reads
2. do not require long-lived locks across all shard PGs of a multipart object
3. do not reintroduce user-visible namespace visibility after delete
4. keep cleanup idempotent and restart-safe
5. preserve the current correctness of versioned delete semantics

## Validation

After each phase:

1. `cargo fmt --all`
2. `cargo test -p server-core -p server-http`
3. `cargo clippy -p server-core -p server-http --all-targets --no-deps -- -D warnings -W clippy::pedantic`

Add focused regression tests for:

1. read continues successfully after concurrent delete once it has started
2. read continues successfully after concurrent unversioned overwrite once it
   has started
3. namespace visibility still changes immediately for new readers after delete
   or overwrite
4. deferred cleanup eventually removes stale shard data

## Open Questions

1. whether reclamation records should live in the existing SQLite metadata DB or
   in a separate local queue
2. whether cleanup should run opportunistically on foreground requests, in a
   background thread, or both
3. whether any current tests already assume immediate physical deletion of old
   shards and will need to be adjusted
4. whether some read helpers should be refactored to a shared snapshotted
   manifest type before the cleanup change lands

## Recommendation Summary

Recommended next move:

1. write the deterministic reproducer tests first
2. fix metadata snapshot completeness
3. then introduce deferred old-generation reclamation
4. then apply that mechanism consistently to delete and overwrite paths
