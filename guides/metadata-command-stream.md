# Metadata Command Stream

This guide defines the target command-stream coordination model for Phase 9.2
of the multihost transition. It is about runtime ownership and recovery of
metadata mutations. The canonical metadata encoding, state digest, and replay
format are described in [metadata-model.md](metadata-model.md).

## Model

A placement group has one metadata command stream. All metadata mutations for
that PG are ordered by this stream.

The command stream is deliberately single-writer and single-pending:

- a PG has at most one unresolved command slot
- the PG-primary durable state allocates command log indexes
- retries must finish or durably abandon the existing command slot before a
  later command can be issued on that PG
- command application is serialized by durable PG-primary state and SQLite
  transactions, not by process-local locks
- concurrency comes from different PGs, not from concurrent mutations inside
  one PG

This is a correctness choice, not just an implementation shortcut. Allowing
multiple unresolved commands inside one PG would require durable queueing,
gap-handling, and retry ordering. That can be optimized later if needed. The
Phase 9.2 target keeps the unit of ordering small and explicit.

## Command Slots

A command slot is a durable intent for one metadata command on one PG. It is
created on the PG primary before acting-set fanout and remains visible until
the command has reached a terminal durable state. Non-primary replicas must not
store unresolved command slots; they store only accepted command-log entries
and materialized metadata state.

The Phase 9.2 implementation starts with a minimal unresolved-slot row. It
records:

- PG id
- log index
- command id, checksum, and canonical command bytes
- command kind and diagnostic scope, such as bucket, object key, upload id, or
  stream session id where available

The broader target model may add explicit slot state and timestamps if they are
needed for observability, retry ownership, or timeout handling. A terminal
applied or abandoned state is currently represented by the command log itself;
once the terminal log entry is durable, recovery should clean the unresolved
slot row.

The transitional in-process retry cache must follow the same shape while the
request paths are being moved over: it is keyed by PG, not by bucket or object.
Older helper names may still mention buckets because callers use a bucket to
derive the routed metadata PG, but a pending command for any bucket on that PG
occupies the single stream slot.

While that bridge exists, any bucket-named cleanup helper must still prove slot
ownership before removing it. A cleanup path for bucket `A` may observe that
bucket `B` owns the PG slot so it can drain or wait for that command, but it
must not erase bucket `B`'s recovery state.

Production command-id allocation is no longer process-local. New request-path
commands derive the next log index from the routed PG primary's durable command
log. If the PG primary has an unresolved durable pending slot, request paths
must load and converge that slot before allocating later work. The current
implementation can rehydrate bucket-PG and object-PG command slots from
durable command bytes. It must never skip over an unresolved durable slot. This
means an already-open coordinator handle must allocate after commands appended
by another handle, but must not allocate after unresolved durable intent. Test
fixtures may still use test-only helpers when they manually construct
artificial command envelopes; those helpers are not production ordering
authorities.

Finishing another request's pending slot must not steal ownership of resources
created by that command. In particular, draining a non-matching
`ReserveObjectGeneration` command applies the reservation and leaves it owned by
its original reservation id; the drainer must not release it just because it was
not the current request. Cleanup of genuinely orphaned reservations needs
durable ownership/scavenger semantics, not a best-effort guess by a later
request.

The slot is coordination state, not serving metadata. Request paths must not
derive visible S3 state from a pending slot. Serving state is the materialized
metadata tables whose accepted log prefix and digest agree with the command
stream.

## Normal Flow

For a new metadata mutation:

1. Route the request to the owning PG primary.
2. If the PG has an unresolved command slot, finish or abandon that command
   before starting new work.
3. In a PG-primary transaction, allocate the next log index and insert the new
   pending command slot.
4. Apply the command to the required acting-set replicas in log order.
5. Record the applied command log entry or abandoned tombstone durably.
6. Mark the slot terminal and remove it only after the terminal durable record
   is visible enough for retry/recovery.

Any process routed to the same PG primary must observe the same unresolved
slot. Retrying through a different coordinator must therefore converge the same
command instead of allocating a different command.

On each replica, applying the metadata mutation and recording or advancing the
command-log state is one SQLite transaction. A replica must not commit
materialized metadata without the matching command-log record, and must not
commit a command-log record that claims a mutation occurred without the
matching materialized metadata mutation. Failure injection must cover rollback
of both directions.

## Publisher Classification

Command safety is classified at the publisher path, not just by command kind.
The risk depends on which metadata snapshot, authorization result, or request
precondition was used before the pending slot was installed.

Publisher classes:

- `SnapshotSensitive`: the publisher must rebuild the command from a fresh
  snapshot after pending-slot contention. Draining a competing slot and
  installing the old command is not safe.
- `ApplyValidated`: command apply fully validates the state this publisher
  depends on. The same command may be retried after draining a competing slot,
  subject to the normal reissue/hash-chain rules.
- `AllocatorCleanup`: the command is cleanup or allocator state with a known
  external owner. It must not steal another request's resources when a pending
  slot is drained.

Current production pending-command publishers:

| Publisher path | Command kind | Class | Required contention shape |
| --- | --- | --- | --- |
| `create_bucket_with_config_and_load_info` | `CreateBucket` | `ApplyValidated` | Drain competing PG slot; exact-row apply handles idempotence/conflict. |
| `begin_bucket_delete` | `MarkBucketDeleting` | `SnapshotSensitive` | Rebuild from current bucket/delete preconditions after contention. |
| `delete_completed_multipart_upload_record_with_command` | `DeleteCompletedMultipartUpload` | `ApplyValidated` | Drain competing PG slot; delete is exact tombstone cleanup. |
| `put_bucket_versioning_and_load_info` | `PutBucketVersioning` | `SnapshotSensitive` | Rebuild bucket post-image after contention. |
| `put_bucket_acl_and_load_info` | `PutBucketAcl` | `SnapshotSensitive` | Rebuild bucket post-image after contention. |
| `put_bucket_property_command_and_load_info` | `PutBucketProperty` | `SnapshotSensitive` | Rebuild bucket post-image after contention. |
| `put_bucket_subresource_command_and_load_info` | `PutBucketSubresource` | `SnapshotSensitive` | Rebuild bucket post-image after contention. |
| `reserve_put_object_generation` | `ReserveObjectGeneration` | `AllocatorCleanup` | Drain competing PG slot, then allocate/reuse through the reservation owner; do not release a non-matching reservation. |
| `reserve_next_object_version` | `ReserveObjectVersion` | `AllocatorCleanup` | Drain competing PG slot, then allocate/reuse through the version reservation owner. |
| `release_object_generation_reservation_command_required` | `ReleaseObjectGeneration` | `AllocatorCleanup` | Required cleanup must retry through slot contention until terminal or fail without losing the cleanup intent. |
| `release_object_generation_reservation` | `ReleaseObjectGeneration` | `AllocatorCleanup` | Release an already-owned reservation; drain competing PG slot and retry. |
| `commit_direct_put_object_from_payload_shards` | `CommitDirectPutObject` | `SnapshotSensitive` | Rebuild commit from fresh object preconditions and stale-payload snapshot after contention. |
| `create_put_object_stream_session_record` | `CreateStreamUpload` | `SnapshotSensitive` | Rebuild session command and reservation cleanup from fresh object state after contention. |
| `commit_stream_segment_append` | `AppendStreamSegment` | `ApplyValidated` | Apply validates session binding/state and existing staged segment before inserting. |
| `abort_stream_upload_session` | `AbortStreamUpload` | `SnapshotSensitive` | Rebuild staged-segment snapshot after contention. |
| `put_object_metadata_if` | `PutObjectMetadata` | `SnapshotSensitive` | Rerun request action/preconditions after contention. |
| `delete_specific_object_version_if` | `DeleteObjectVersion` | `SnapshotSensitive` | Rerun delete preconditions after contention. |
| `delete_current_object_if` | `DeleteObjectVersion` | `SnapshotSensitive` | Rerun current-object selection after contention. |
| `insert_current_delete_marker_if` | `InsertDeleteMarker` | `SnapshotSensitive` | Rerun current-object/versioning selection after contention. |
| `expire_current_object_if_due` | `DeleteObjectVersion`/`InsertDeleteMarker` | `SnapshotSensitive` | Rerun lifecycle selector and object-lock checks after contention. |
| `delete_noncurrent_live_versions_if_due` | `DeleteObjectVersion` | `SnapshotSensitive` | Rerun lifecycle selector and object-lock checks after contention. |
| `delete_expired_delete_marker_if_due` | `DeleteObjectVersion` | `SnapshotSensitive` | Rerun expired-marker selector after contention. |
| `reclaim_object_payload_if_unleased` | `DeleteObjectPayloadReclaim` | `SnapshotSensitive` | Recheck lease/fence and reclaim state after contention; cleanup must preserve retryability. |
| `create_put_object_stream_session` | `CreateStreamUpload` | `SnapshotSensitive` | Rerun authorization/object snapshot after contention. |
| `finalize_put_object_stream` | `CommitDirectPutObject` | `SnapshotSensitive` | Rebuild commit from current object preconditions and stream-session snapshot after contention. |
| `create_multipart_upload` | `CreateMultipartUpload` | `SnapshotSensitive` | Rerun authorization/object snapshot after contention. |
| `begin_upload_part_stream_session` | `CreateStreamUpload` | `SnapshotSensitive` | Revalidate MPU/session target after contention. |
| `create_upload_part_stream_session` | `CreateStreamUpload` | `SnapshotSensitive` | Revalidate MPU/session target after contention. |
| `reserve_completed_multipart_upload_order` | `AdvanceCompletedMultipartUploadSequence` | `AllocatorCleanup` | Serialize through the bucket-PG slot; a later object-PG command must be derived from a terminal reservation. |
| `complete_multipart_upload_commit_serialized` | `CommitMultipartObject` | `SnapshotSensitive` | Rebuild completion parts, cleanup snapshot, stale payload, and bucket-PG order after contention. |
| `finalize_upload_part_stream` | `CommitStreamPart` | `SnapshotSensitive` | Rebuild stream-session and staged-segment snapshot after contention. |
| `abort_multipart_upload_locked` | `AbortMultipartUpload` | `SnapshotSensitive` | Rebuild upload/part/active stream cleanup snapshot after contention. |
| `abort_authorized_multipart_upload_locked` | `AbortMultipartUpload` | `SnapshotSensitive` | Rebuild authorized upload cleanup snapshot after contention. |

Adding a production call site that creates or installs a pending metadata
command requires updating this table and the boundary check allowlist. Direct
uses of `try_set_pending_metadata_command_for_bucket`,
`try_install_pending_metadata_command_for_bucket`, and
`set_pending_metadata_command_for_bucket` are intentionally tracked.

## Recovery

Before accepting new work on a PG, recovery must inspect durable command-stream
state:

- if there is a pending command slot that has not been accepted, mutated, or
  logged by any acting-set replica, retry command application or durably
  abandon it according to the command's normal rules
- if any acting-set replica accepted, mutated, or logged the command, recovery
  must converge that same command to a terminal durable record; it must not
  abandon the command and issue a later replacement
- if a terminal log record exists for the slot, finish slot cleanup
- if the materialized metadata digest disagrees with the accepted log prefix,
  fail closed and require repair
- if command-log prefix state is incomplete, do not allocate a later command

A process-local crash must not lose the fact that a command partially applied.
A different process must be able to finish the same slot.

The abandon boundary is therefore exactly the zero-replica-apply boundary. A
command may be abandoned only while no acting-set replica has accepted the
mutation, changed materialized state, or recorded the command/tombstone. Once
that boundary is crossed, retries and recovery must finish the original
command, including any matching terminal tombstone, before new work can start on
the PG.

## Multi-PG Requests

Phase 9.2 defines PG-local command streams. It does not provide cross-PG
transactions.

Some request flows already sequence commands across more than one PG. For
example, multipart completion can reserve completed-upload order on the bucket
PG and then commit object metadata on the object PG. These flows must use a
deterministic PG order and explicit retry cleanup. They must not leave
ambiguous pending slots on multiple PGs where either PG cannot decide whether
to converge or abandon its own command independently.

The rule for these flows is:

- each PG command slot is independently recoverable using the PG-local recovery
  rules above
- later PG commands must either be derived from already-terminal earlier PG
  commands, or have explicit cleanup when a later step cannot proceed
- no process-local lock may be used to make the multi-PG sequence appear
  atomic

Cross-PG atomicity, if needed later, is a separate design from Phase 9.2.
Targeted multipart race coverage for these multi-PG flows belongs to Phase
9.3, after Phase 9.2 has provided the PG-local command-slot primitive.

## Stream Segment IDs

Stream segment VID allocation is part of the metadata command stream model.
It must not be owned by `LocalClusterRuntimeState` or any other process-local
map.

The implemented rule is:

- the stream session row has a durable `next_segment_vid` allocator on the PG
  primary, updated by SQLite before payload shards are written
- the append command carries the allocated segment VID, and command apply
  advances each replica's allocator floor to at least the committed VID plus
  one
- allocated but uncommitted VIDs are allowed gaps; they are allocator state, not
  object metadata or object data in the command log
- terminal stream commands delete or make irrelevant the stream session row, so
  no process-local allocator cleanup is required

The allocator column is not part of the canonical metadata command digest. The
command-owned state is the append segment row and the payload CRC/hash carried
by that row; the allocator is durable PG-primary coordination state used to
avoid cross-process shard-key reuse before the append command exists.

This split is intentionally a little different from ordinary command-owned
metadata. `stream_upload_segments.segment_vid` is semantic metadata: it is the
payload shard identity for a staged segment, so the exact value is encoded in
`AppendStreamSegment` and replicated through command apply. By contrast,
`stream_uploads.next_segment_vid` is only an allocator floor. It may be ahead
on the primary while append commands are still in flight, and replicas may
advance to the same or a lower floor until those commands apply.

Terminal MPU commands do not encode full `StreamUploadRecord` rows. They use a
terminal cleanup record containing the stream session identity, target, state,
creation time, and encryption state, but not `next_segment_vid`. That keeps
the command-owned cleanup state separate from the durable runtime allocator
floor, and avoids equality checks that have to remember to ignore allocator
progress.

We considered replacing the allocator floor with non-state allocation. A
random `u64` segment VID is not strong enough to treat collisions as
impossible, and checking for collisions by scanning existing segment rows would
be more expensive and less direct than using SQLite's counter update. Deriving
the VID from the command id/log index would be exact, but it requires creating
or reserving the append command before payload shards are written. That would
need a larger protocol change so a pending append command cannot be applied by
another drainer before payload acks exist.

For now, keep the SQLite allocator floor. If this is revisited, prefer either a
larger collision-resistant opaque segment identity or a command-protocol change
that makes command-derived VIDs safe before payload write. Do not switch to
best-effort random `u64` allocation without an explicit collision model.

## Local Locks And Caches

Process-local locks may still protect Rust object safety, SQLite connection
use, or performance caches. They must not be the authority for logical command
ordering.

Allowed local mechanisms:

- a mutex around a `PgStore` handle for per-connection safety
- immutable or recomputable caches, such as EC write-state caches
- digest fast paths guarded by durable revision checks and fail-closed restart
  validation

Not allowed as logical authorities after Phase 9.2:

- in-memory command log index allocators
- in-memory pending command maps
- process-local metadata command apply locks
- process-local stream segment VID allocators

## Phase 9.2 Exit Criteria

Phase 9.2 is complete when:

1. command log index allocation is durable and PG-primary owned
2. every PG has at most one unresolved durable command slot
3. pending command convergence is visible after process restart and from a
   second coordinator handle
4. command application ordering does not depend on a process-local apply lock
5. stream segment VID allocation is durable or command-owned
6. tests cover two handles racing on the same PG for command allocation,
   pending command convergence, and stream append segment allocation
7. tests cover two different buckets on the same PG trying to create
   overlapping pending commands, proving the slot is PG-scoped rather than
   `(PG, bucket)` scoped
8. tests cover the zero-replica-apply abandon boundary and the nonzero-apply
   convergence boundary
9. failure-injection tests prove a replica cannot persist materialized metadata
   without the matching command-log record, or a command-log record without the
   matching materialized metadata mutation

## Non-Goals

Phase 9.2 does not need to maximize intra-PG concurrency. It also does not need
to implement remote RPC, peering, repair, or placement changes. Those later
phases can rely on the command stream being a durable, strictly ordered PG
coordination primitive.
