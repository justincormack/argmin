# Storage Topology Resize Plan

Status: draft

## Context

The current storage implementation assumes the configured metadata/object PG ID set is
fixed for a cluster lifetime. That assumption is visible in code and plans:

- `PgTopology` maps buckets, objects, segments, multipart parts, and local object PG
  sets from the current configured PG set.
- The distributed correctness review notes that changing the PG-ID set remaps nearly
  everything and is safe only while the PG count/set is fixed.
- Several durable cleanup cursors and worker queues encode progress in terms of the
  current PG set rather than an explicit topology generation.

That is acceptable for current pre-alpha operation, but it is not the final production
model. Storage replacement will often be resize-shaped: adding disks/nodes should add
capacity and eventually add/move PGs, and retiring storage may require draining or
removing PGs. We need to enumerate these dependencies before implementing any PG-set
change.

This plan is separate from `storage-upgrade-versioning-plan.md`. Upgrade/versioning is
about old/new durable formats and mixed binary versions. Topology resize is about
changing the live key-to-PG and PG-to-node placement model while preserving object,
bucket, cleanup, and worker correctness.

## Current Policy

- Do not change the configured metadata/object PG set in a running cluster.
- PG acting-set changes for an existing PG are supported through the control-plane
  route/epoch machinery.
- PG-set changes, keyspace remapping, PG addition, and PG removal are not supported yet.
- Any code or test that depends on changing `pg_ids()` should be treated as future
  topology-resize work, not as a supported current recovery path.

## Goals

1. Identify every path that assumes a stable PG set or uses positional PG progress.
2. Define topology-generation/fencing semantics for PG-set changes.
3. Formalize the resize model for bucket/object metadata while preserving recorded
   payload placement generations.
4. Make cursor, worker, and reclaim state explicit enough to survive PG-set changes.
5. Add deterministic tests and UAT smokes before enabling any resize operation.

## Non-Goals For Now

- Implementing live resize immediately.
- Supporting old on-disk schema migration.
- Supporting arbitrary mixed-version binaries during resize.
- Allowing PG-set changes without an explicit topology-generation transition.

## Candidate Resize Model

The resize model should not be "recompute placement from the latest PG set and hope
old work follows". Committed placement is generation-bound:

- payload/data placement already records the placement view needed to recompute shard
  locations for a committed object generation
- object metadata ownership must follow the same rule: a metadata row is owned by a
  topology generation until an explicit migration command moves it
- new writes use the current topology generation, while old rows remain visible through
  their recorded/source generation until migration completes

Resize therefore has three related but distinct version axes:

- **topology generation**: the key-to-metadata-PG set for bucket/object ownership
- **route generation / cluster epoch**: the PG-to-node acting-set mapping
- **payload placement generation**: the recorded data-PG/shard placement for committed
  payload bytes

This matches the existing placement documentation in
`plans/completed/segment-pg-placement-tradeoffs.md`,
`guides/temporary-write-availability.md`, and the older metadata-cluster design's
PG-backfill model. The open work is to make the object/bucket metadata topology
generation equally explicit in current code.

The old metadata-cluster design favored rendezvous hashing over an explicit PG set:
adding a PG moves a small fraction of keys from every existing PG, and removing a PG
redistributes its keys. That remains plausible for topology-generation changes, but the
open questions are now narrower:

- whether bucket metadata and object metadata use the same topology-generation mechanism
- whether bucket rows are migrated or pinned for bucket lifetime
- the exact migration command/fence shape for moving object metadata ownership
- how reads/listings merge source and destination topology generations during backfill
- whether PG removal is strictly drain-only followed by removing an empty PG
- how topology generations are represented in bucket, object, multipart, stream, and
  worker state

## Initial Hazard Inventory

### H1. Key-to-PG remapping for bucket rows

`bucket_pg_for(bucket)` is recomputed from the current `PgTopology`. If the PG set
changes, a bucket can map to a different bucket PG. Any bucket row, bucket subresource,
bucket write drain, delete drain, finalizer claim, or bucket-control pending command may
then be looked up on the wrong PG unless the bucket row is migrated or bucket placement is
versioned/pinned.

Needed work:

- Define bucket placement generation.
- Decide whether bucket rows are migrated eagerly, lazily, or pinned.
- Ensure bucket authorization/state reads use a single proven bucket placement.
- Ensure bucket-control pending command slots and bucket write reservations cannot split
  across source/destination PGs.

### H2. Key-to-PG remapping for object metadata

`object_pg_for(bucket, key)` is recomputed from the current topology. A PG-set change can
move current object rows, noncurrent versions, delete markers, object tags/ACL metadata,
multipart metadata, and stream target metadata.

The resize-safe invariant is that existing rows remain owned by their source topology
generation until an explicit migration command moves them. They must not silently become
owned by whichever PG the latest topology would choose.

Needed work:

- Define object metadata placement generation.
- Define the migration scheduling model: scanner-driven, write-driven, or another
  explicit command path.
- During migration, reads, writes, lists, lifecycle, and deletes must know whether to
  consult source PG, destination PG, or both.
- Avoid split-brain updates where two PGs both accept writes for the same object key.

### H3. List and version listing correctness during migration

ListObjects/ListObjectVersions currently fan out over the configured PG set. If source
and destination topology generations coexist, listing must merge both generations
without duplicates or gaps.

Needed work:

- Add generation-aware list fanout.
- Define duplicate suppression if an object is visible in both old and new placement
  during migration.
- Ensure continuation tokens encode enough placement-generation state.

### H4. Payload placement and object-local PG sets

Payload shard placement is tied to metadata records and object-local PG set selection.
Changing the global PG set can change candidate data PG sets for new writes, but old
payload records must remain readable from their recorded placement generation. This is
already the intended model: committed object metadata records the placement view used for
that payload generation, and reads/repair recompute locations from that recorded view
rather than the latest topology.

Needed work:

- Audit that all committed payload reads are driven by recorded placement generation, not
  recomputed current topology.
- Decide whether payload movement is part of metadata PG resize or a separate shard
  rebalance.
- Keep repair/backfill/reclaim using recorded placement generations.

### H5. Durable cleanup cursors that encode PG position

The original public API review S5 case was `DeleteBucket` completed-MPU
finalization's positional PG cursor. The tombstone-free multipart replay design removed
the completed-upload table, cross-PG cleanup scan, and cursor, so that specific hazard is
resolved. The same positional-cursor audit still applies to the remaining cleanup and
worker paths below.

Known related cursors to audit:

- `post_reservation_next_object_pg_id`
- object-payload reclaim scan markers
- lifecycle sweep markers
- shard repair/backfill scan markers
- any worker queue item that implies "all PGs below this point were already checked"

### H6. Durable queues and claims that point at PGs

Reclaim, repair, backfill, lifecycle, stream cleanup, bucket-delete begin/finalize, and
metadata-transfer workers store PG identities or derive PG lists at scan time. A resize can
make a queued root refer to a PG that is no longer current, or require scanning both old
and new PGs.

Needed work:

- Add topology generation to durable queue/claim rows where a PG reference is interpreted
  through a PG set.
- Classify rows as recorded-placement rows, old-topology migration rows, or current
  topology rows.
- Define recovery behavior for rows from retired PGs.

### H7. Pending metadata command slots and command log streams

Each metadata PG has an independent command log. Moving a key between PGs means command
ordering for that key crosses log streams unless the migration itself provides a fence.

Needed work:

- Define a migration command that fences old and new PGs before moving ownership.
- Ensure pending command slots cannot exist for the same logical bucket/object on both
  old and new PGs.
- Define committed retry behavior when the request is retried after the ownership move.

### H8. Multipart and stream sessions

Multipart uploads and stream sessions can span long enough to overlap a topology change.
Their target bucket/object PG, staged segment records, completed part rows, and final
commit paths must not be recomputed under a different topology. In-progress sessions
should carry the topology/placement generation selected at creation and finish or abort
under that same generation unless an explicit migration command moves the session.

Needed work:

- Audit and fill any missing topology/placement generation fields in multipart upload and
  stream session records.
- Decide whether in-progress sessions block PG-set resize, are migrated, or continue on
  their original placement.
- Ensure abort/complete/finalize cleanup uses the session's original placement.

### H9. DeleteBucket and terminal cleanup

DeleteBucket already has multiple resumable phases. Resize can make the "bucket is empty"
proof and terminal cleanup depend on old and new object PG sets.

Needed work:

- Define whether DeleteBucket blocks topology resize for that bucket.
- Make emptiness proofs generation-aware.
- Ensure terminal cleanup checks every relevant PG generation before deleting the bucket
  row.

### H10. Control-plane route history versus topology history

The route history records PG-to-node placement for known PG IDs. PG-set resize needs a
separate topology history: which PG IDs existed, when, and which topology generation
applies to each object/bucket/session/worker row. Route history is not enough: an old
row may need the current route for its source PG, but the source PG itself is selected by
the row's topology generation.

Needed work:

- Add topology generation to the control-plane state.
- Persist topology-generation history separately from route history.
- Define compaction rules for topology history once all old-generation rows are migrated
  or deleted.

## Required Test Matrix

Before enabling resize, add focused tests for:

- adding a PG while bucket/object writes are in flight
- adding a PG while DeleteBucket is between resumable phases
- completed-MPU cleanup cursor across PG-set addition/removal
- object read/write/list during old-to-new object metadata migration
- bucket-control mutation during bucket PG migration
- multipart complete/abort when the upload was created before resize
- stream PUT/upload-part finalize after resize
- lifecycle/reclaim/repair/backfill queues created before resize and drained after resize
- restart during every resize phase
- UAT smoke that adds capacity, migrates at least one metadata PG and one object metadata
  key range, restarts nodes, and verifies read/list/write/delete correctness

## Immediate Next Steps

1. Keep current code on the fixed-PG-set invariant.
2. Update findings that depend on PG-set changes to point at this plan instead of treating
   them as current production bugs.
3. Audit code for positional PG cursors and append them to H5.
4. Audit durable rows that store or imply PG identity and append them to H6-H8.
5. Decide whether bucket placement is pinned or migratable; that decision drives most of
   the remaining design.
