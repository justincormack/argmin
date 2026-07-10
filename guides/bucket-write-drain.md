# Bucket Write Drain

Phase 9.4 replaces the current single-process bucket write-drain mechanism with
cluster-visible bucket-PG coordination. The goal is not to change S3 semantics:
it is to make the existing DeleteBucket/write race behavior survive multiple
processes, restart, and PG-primary ownership.

## Current Authority

The current write-drain path uses durable coordination rows for production write
admission and DeleteBucket begin:

- `StorageCluster::with_bucket_write_snapshot` acquires a durable
  `bucket_write_reservations` row on the bucket-PG primary, loads the bucket
  snapshot, runs the caller action, then releases that exact row.
- `StorageCluster::begin_bucket_delete` installs a durable
  `bucket_write_drains` row on the bucket-PG primary, drains bucket-relevant
  pending object commands while waiting for durable reservations to empty, then
  either rolls back the durable drain by exact identity or publishes terminal
  `MarkBucketDeleting`.
- The legacy bucket-row counters have been removed. The retired drain counters
  were anonymous: they did not identify the writer, the bucket incarnation, the
  request class, or whether another process crashed while holding the
  reservation. They are not a multi-process correctness boundary.

## Target Authority

Bucket write-drain state must move to explicit bucket-PG-primary coordination
records:

- `bucket_write_reservations`: a live write admission token for one bucket
  incarnation.
- `bucket_write_drains`: a temporary DeleteBucket fence while the delete waits
  for writers, drains pending object commands, and decides whether to roll back
  or publish `MarkBucketDeleting`.

These records are not Phase 9.4 command-owned replica state. They are written
on the bucket PG primary as coordination authority, so they are intentionally
outside the replica-wide metadata command digest. A later phase may either
replicate them through the bucket command stream or add a separate
primary-owned coordination integrity record; until then, they must not be
treated as part of acting-set metadata agreement.

Each reservation must include at least:

- bucket name
- bucket execution generation or bucket row digest
- reservation id
- owner/process token
- bucket PG and cluster epoch
- operation kind
- creation time and heartbeat or lease deadline
- optional request target context for traceability

Release, reap, and apply-time validation must match bucket name, reservation id,
owner token, and bucket incarnation. A stale release after delete/recreate must
not affect a reservation for the new bucket incarnation.

Reservation IDs must be unique across independent `StorageCluster` handles and
process restarts. The Phase 9.4 implementation uses 128 bits of random entropy
for the reservation-id suffix; it must not use a per-handle counter as durable
identity.

## Publisher Classification

Phase 9.4.1 classifies current request publishers by what they need from the
write-drain mechanism.

| Publisher family | Current path | Classification |
| --- | --- | --- |
| Small direct PutObject | `Coordinator::put_object_from_authorized_write` via `with_bucket_write_handle_for` | Carries a durable reservation proof in the direct PUT commit command; apply/retry/open-time convergence validate and release it |
| Stream PutObject session create | `create_stream_put_session_for_authorized_write` / `create_put_object_stream_session` / `create_put_object_stream_session_record` | Carries a durable reservation proof in the session command; apply/retry/open-time convergence validate and release it |
| Stream PutObject finalize | `Coordinator::finalize_stream_put` via `with_bucket_write_handle_for` | Carries a durable reservation proof in the stream finalization commit command; apply/retry/open-time convergence validate and release it |
| CopyObject destination write | `authorize_copy_object` and stream destination commit | Needs destination bucket write reservation; source reads do not acquire destination write protection |
| CreateMultipartUpload | `authorize_create_multipart_upload` / `StorageCluster::create_multipart_upload` | Carries a durable reservation proof in the MPU-create command; apply/retry/open-time convergence validate and release it |
| CompleteMultipartUpload | `authorize_complete_multipart_upload` / `complete_multipart_upload_commit_serialized` | Carries a durable reservation proof in the completed-object command; apply/retry/open-time convergence validate and release it |
| UploadPart stream session create | `begin_stream_part` / `begin_upload_part_stream_session` / `create_upload_part_stream_session` | Carries a durable reservation proof in the UploadPart session command; apply/retry/open-time convergence validate and release it |
| UploadPart stream finalize | `finalize_upload_part_stream` | Carries a durable reservation proof in the committed-part command; apply/retry/open-time convergence validate and release it |
| UploadPartCopy | `upload_part_copy` creates a destination stream session, appends copied source segments, and finalizes | Needs destination reservation for session create/finalize; source read authorization is separate |
| Bucket control-plane writes | policy, CORS, tagging/ABAC, public access block, ownership controls, lifecycle, encryption, versioning, object lock, ACL | Must either acquire the durable reservation, be explicitly blocked by an active drain, or prove the command is itself the drain/delete transition |
| Object tags/ACL/retention/legal-hold | `put_object_metadata_if` / `PutObjectMetadata` | Carries a durable reservation proof in the object-metadata command; apply/retry/open-time convergence validate and release it |
| User-visible object metadata deletes | current DeleteObject, specific-version delete, delete-marker insertion, lifecycle current/noncurrent expiry, expired delete-marker cleanup | Carry a durable reservation proof in `DeleteObjectVersion`/`InsertDeleteMarker`; apply/retry/open-time convergence validate and release it |
| DeleteBucket begin | `begin_bucket_delete` / `MarkBucketDeleting` | Owns the temporary drain fence and publishes the terminal delete command |
| DeleteBucket finalize | `try_finalize_bucket_delete` | Must not acquire a new write reservation; it only finalizes a bucket already marked Deleting |
| Payload reclaim and physical cleanup | payload reclaim metadata delete, shard cleanup, scavenger cleanup | Must not acquire a new bucket write reservation just to remove payload bytes or cleanup metadata for already-decided object state |
| Bucket reads and list operations | bucket read handle and list authorization paths | Do not acquire write reservations |

CreateBucket is outside the bucket write-drain mechanism because no existing
bucket incarnation can be reserved. It still uses the bucket-PG command stream.

## Apply-Time Reservation Fence

A write reservation is not only permission to load a bucket snapshot. It must
also be a fence at metadata command apply.

Every bucket-write metadata command built under a reservation must carry a
bucket-PG reservation reference, either in the command payload or in an
equivalent command envelope field:

- bucket name
- bucket execution generation or bucket row digest
- reservation id
- reservation owner token
- bucket PG and cluster epoch

The object metadata command families that can publish user-visible bucket
writes are proof-required commands: `CommitDirectPutObject`,
`CreateStreamUpload`, `CreateMultipartUpload`, `CommitStreamPart`,
`CommitMultipartObject`, `PutObjectMetadata`, `DeleteObjectVersion`, and
`InsertDeleteMarker`. Command bytes without a durable bucket-write proof are
malformed and must fail decode/replay validation.

Object-PG command apply must re-read and validate that bucket-PG reference. The
validation must run for:

- initial object-PG command apply
- matching pending-command retry and finish paths before the command has already
  been durably accepted by that replica
- open-time in-flight command convergence

Validation requires both the durable reservation row and the current bucket row:
the proof must match the reservation identity, and the bucket must still be the
same active bucket incarnation generation recorded in the proof. A stale
reservation row is not authority for a recreated or terminal bucket.

When a replica reports `AlreadyApplied` for the exact command bytes/hash chain,
the live request path may run only idempotent terminal cleanup and proof release
without revalidating the still-live reservation row. At that point the metadata
mutation is already materialized on that replica; the safety proof is the
accepted command log entry, not current reservation liveness.

If no object-PG replica has accepted the command yet, a missing, reaped,
expired, wrong-owner, wrong-incarnation, or terminal-drain reservation must make
apply fail closed.

## Reap vs Convergence Rule

Reservation reaping must not strand a partially accepted object-PG command.
Phase 9.4 must implement one of these rules:

1. A reservation is not reapable while any pending object-PG command or
   accepted-but-not-converged object-PG log entry references it across the full
   acting set and open-time recovery shapes.
2. The first object-PG acceptance durably records a reservation proof in the
   command/log state. Later exact-command convergence validates that proof
   rather than requiring the live reservation row to still exist.

The selected rule must fail closed for divergent command bytes or hash chains,
but it must allow an exact command already accepted by one replica to converge.
DeleteBucket must not reap a reservation in a way that leaves one object-PG
replica mutated while remaining replicas can no longer accept the same command.

## DeleteBucket Drain Loop

`begin_bucket_delete` is a bucket-PG-primary state machine:

1. Install or resume the durable drain fence on the bucket-PG primary.
2. Finish pending bucket-PG commands for the bucket under that fence.
3. Drain object-PG pending commands that can publish visible data or MPU state
   under that fence.
4. Wait or poll until active durable write reservations are empty, reaping only
   reservations allowed by the owner-token and reap-vs-convergence rules.
5. Drain object-PG pending commands for the bucket again. A writer that already
   held a reservation can publish or partially apply after the first drain.
6. Re-check visible data and in-progress MPU state from fresh metadata.
7. If new relevant state or a new live reservation appears, repeat the
   wait/drain/check loop.
8. If the bucket is not empty, roll back the temporary drain fence durably and
   let waiting writers proceed.
9. If the bucket is empty, publish `MarkBucketDeleting` through the bucket-PG
   command stream and make the drain terminal.

Once `MarkBucketDeleting` is durable, new writes fail through the normal
missing/deleting bucket semantics. Before that terminal point, writers observing
the temporary drain wait/back off and retry from fresh bucket state.

The implemented recovery rule is conservative: a durable drain without terminal
`MarkBucketDeleting` is rolled back only when it has an explicit expired lease.
Different owner tokens alone are not proof of a dead owner. Terminal
`MarkBucketDeleting` with a surviving durable drain is idempotent; reopen
converges primary-last partial apply and keeps the terminal drain until
finalization removes the bucket row.

## DeleteBucket Finalization

`try_finalize_bucket_delete` is process-independent. Once the bucket row is in
`Deleting` state, any cluster handle may retry finalization; it does not require
the process or local waiter that installed the delete drain.

Finalization still checks the state that must remain asynchronous after the
DeleteBucket response boundary:

- visible object versions and in-progress multipart uploads
- payload reclaim roots
- completed multipart-upload idempotence rows that must be pruned before the
  bucket row is removed

Missing local queue wakeups are therefore performance issues, not correctness
issues. A worker can make progress by polling/listing deleting buckets and
calling finalization again after reclaim blockers clear. Active in-memory read
handles are not a direct finalization blocker; they only defer physical payload
reclaim, which leaves the durable reclaim roots visible. Local reclaim queues
are FIFO wakeup hints only; they must not be used as the authority for whether
object reclaim or bucket finalization is safe to run next.

## Required Tests

Phase 9.4 must include storage and request-level coverage for:

- active reservation blocks DeleteBucket until release, then DeleteBucket
  observes the published data and returns BucketNotEmpty
- reservation reaped before first object-PG apply rejects the command
- partial object-PG apply while reservation is live, then DeleteBucket attempts
  to reap; either reaping is blocked until convergence or durable proof allows
  retry/reopen convergence
- DeleteBucket waits for reservations empty, drains object-PG commands again,
  then bases BucketNotEmpty/finalize on the converged state
- DeleteBucket races versioned current DeleteObject that inserts a delete marker
  during a drain; the marker command is either fenced/drained before emptiness is
  trusted or blocked by the active drain
- DeleteBucket races specific-version delete that removes the last visible
  version; finalization must be based on the converged delete command state, not
  the pre-drain snapshot
- DeleteBucket races lifecycle current expiry, noncurrent expiry, and expired
  delete-marker cleanup; lifecycle object metadata commands must follow the same
  drain/fence rules as request-path object deletes
- temporary drain against a non-empty bucket rolls back and waiting writers
  proceed
- empty-bucket DeleteBucket survives restart and finalizes without a
  same-process waiter
- stale cluster handles cannot acquire new reservations but can release an
  already acquired reservation by exact identity; remote release routes through
  the current cleanup epoch while preserving the reservation's original
  acquire epoch as part of that durable identity
- DeleteBucket can reap an expired reservation created before a route-epoch
  change through the current bucket-PG primary
- delete/recreate does not let stale release/reap affect the new bucket
  incarnation
