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

This is required so reads that already hold a payload generation lease can
continue safely even after the object has become deleted in metadata.

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
- bucket delete finalization may continue once reclaim roots and leases are
  gone

So it is expected that shard files can remain on disk briefly after a
successful delete request.

## Why This Is Required

Argmin allows in-flight readers to keep using an already-authorized payload
generation through leases.

If delete tried to synchronously remove all shard files before returning, it
would have to do one of:

- break those in-flight reads
- block the delete request on lease release and potentially slow readers

We do not want either behavior.

So the design rule is:

- metadata visibility changes synchronously
- payload bytes are reclaimed asynchronously once no lease still needs them

## Operational Consequences

- `DeleteObject` success means future reads observe the object as deleted
  according to normal metadata semantics
- it does not mean the backing shard files are already gone from disk
- `DeleteBucket` may remain pending until payload reclaim roots and
  outstanding payload leases drain

This is expected behavior, not evidence of leaked object data by itself.
