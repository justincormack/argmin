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

## Operational Consequences

- `DeleteObject` success means future reads observe the object as deleted
  according to normal metadata semantics
- it does not mean the backing shard files are already gone from disk
- `DeleteBucket` may remain pending until payload reclaim roots drain; active
  reads can delay this indirectly by making reclaim defer physical shard deletion

This is expected behavior, not evidence of leaked object data by itself.
