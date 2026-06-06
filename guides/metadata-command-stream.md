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

On restart, acting-set replicas normally must agree on the accepted command
prefix and materialized-state digest before the PG is considered clean. The
only accepted in-flight exception is a primary-owned pending slot for the next
log index where every advanced replica has accepted exactly that command,
chained from the primary's current log hash, all unadvanced replicas match the
primary's current replica state, and all advanced replicas agree on the full
post-command replica state including the materialized-state digest. Any other
prefix or digest disagreement remains a fail-closed divergence. When the
in-flight exception is accepted, open-time recovery must converge that command
and remove the primary pending slot before returning the cluster map; read-only
paths must never serve stale primary materialized rows while relying on the
advanced replicas' command log state.

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
- `MatchingOutcomeRetry`: the publisher is snapshot-sensitive for unrelated
  contention, but an equivalent pending command is also the result the caller
  is waiting for. These publishers must not use a generic drain-and-retry path
  for a matching contender, because draining the command can delete the
  request state needed to reconstruct the response. They need an explicit
  matching predicate, an outcome extractor from the command-owned row image,
  and a same-command install-race regression.
- `TerminalSessionRetry`: the publisher is snapshot-sensitive and has a
  terminal command that deletes the stream/upload session it is finalizing or
  aborting. An equivalent contender must remain visible to the publisher's
  top-of-loop matching branch; generic drain can delete the session before the
  retry can validate or finish the matching command.

Current production pending-command publishers:

| Publisher path | Command kind | Class | Required contention shape |
| --- | --- | --- | --- |
| `create_bucket_with_config_and_load_info` | `CreateBucket` | `ApplyValidated` | Drain competing PG slot during pending-slot checks and command-id allocation; exact-row apply handles idempotence/conflict. |
| `begin_bucket_delete` | `MarkBucketDeleting` | `SnapshotSensitive` | Rebuild from current bucket/delete preconditions after contention. |
| `delete_completed_multipart_upload_record_with_command` | `DeleteCompletedMultipartUpload` | `ApplyValidated` | Drain competing PG slot during pending-slot checks and command-id allocation; delete is exact tombstone cleanup. |
| `put_bucket_versioning_and_load_info` | `PutBucketVersioning` | `SnapshotSensitive` | Drain competing PG slot during pending-slot checks and command-id allocation; rebuild bucket post-image after contention. |
| `put_bucket_acl_and_load_info` | `PutBucketAcl` | `SnapshotSensitive` | Drain competing PG slot during pending-slot checks and command-id allocation; rebuild bucket post-image after contention. |
| `put_bucket_property_command_and_load_info` | `PutBucketProperty` | `SnapshotSensitive` | Drain competing PG slot during pending-slot checks and command-id allocation; rebuild bucket post-image after contention. |
| `put_bucket_subresource_command_and_load_info` | `PutBucketSubresource` | `SnapshotSensitive` | Drain competing PG slot during pending-slot checks and command-id allocation; rebuild bucket post-image after contention. |
| `reserve_put_object_generation` | `ReserveObjectGeneration` | `AllocatorCleanup` | Drain competing PG slot before and during command-id allocation, reread the allocator before final command-id allocation, then allocate/reuse through the reservation owner. If the generation becomes stale before publish, exact `ObjectGenerationReservationConflict` is retried without releasing a non-matching reservation. |
| `reserve_next_object_version` | `ReserveObjectVersion` | `AllocatorCleanup` | Drain competing PG slot before and during command-id allocation, then allocate/reuse through the version reservation owner. |
| `release_object_generation_reservation_command_required` | `ReleaseObjectGeneration` | `AllocatorCleanup` | Required cleanup must retry through slot contention, including command-id contention, until terminal or fail without losing the cleanup intent. |
| `release_object_generation_reservation` | `ReleaseObjectGeneration` | `AllocatorCleanup` | Release an already-owned reservation; drain competing PG slot before and during command-id allocation and retry. |
| `commit_direct_put_object_from_payload_shards` | `CommitDirectPutObject` | `SnapshotSensitive` | Rebuild commit from fresh object preconditions and stale-payload snapshot after pending-install contention or pre-publish command-id contention. The commit command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `create_put_object_stream_session_record_under_reservation` | `CreateStreamUpload` | `SnapshotSensitive` | Low-level PutObject stream-create publisher. The public wrapper first holds a durable bucket write reservation; this internal publisher rebuilds session command and reservation cleanup from fresh object state after contention. |
| `commit_stream_segment_append` | `AppendStreamSegment` | `ApplyValidated` | Command-id and pending-install contention are drained and retried from a fresh stream-session snapshot without deleting the staged payload; apply validates session binding/state and existing staged segment before inserting. |
| `abort_stream_upload_session` | `AbortStreamUpload` | `TerminalSessionRetry` | Rebuild staged-segment snapshot after unrelated pending-slot or command-id contention. If an equivalent terminal command wins the pending slot, restart without draining so the matching/session-completion branch can finish it. |
| `put_object_metadata_if` | `PutObjectMetadata` | `SnapshotSensitive` | Rerun request action/preconditions after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `delete_specific_object_version_if` | `DeleteObjectVersion` | `SnapshotSensitive` | Rerun delete preconditions after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `delete_current_object_if` | `DeleteObjectVersion` | `SnapshotSensitive` | Rerun current-object selection after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `insert_current_delete_marker_if` | `InsertDeleteMarker` | `SnapshotSensitive` | Rerun current-object/versioning selection after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `expire_current_object_if_due` | `DeleteObjectVersion`/`InsertDeleteMarker` | `SnapshotSensitive` | Rerun lifecycle selector and object-lock checks after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `delete_noncurrent_live_versions_if_due` | `DeleteObjectVersion` | `SnapshotSensitive` | Rerun lifecycle selector and object-lock checks after contention. Each delete command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `delete_expired_delete_marker_if_due` | `DeleteObjectVersion` | `SnapshotSensitive` | Rerun expired-marker selector after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `reclaim_object_payload_if_unleased` | `DeleteObjectPayloadReclaim` | `SnapshotSensitive` | Recheck lease/fence and reclaim state after contention, including command-id contention after payload cleanup has started; cleanup must preserve retryability. |
| `create_put_object_stream_session` | `CreateStreamUpload` | `SnapshotSensitive` | Rerun authorization/object snapshot after contention. |
| `finalize_put_object_stream` | `CommitDirectPutObject` | `TerminalSessionRetry` | Rebuild commit from current object preconditions and stream-session snapshot after unrelated contention. If an equivalent terminal command wins the pending slot, restart without draining so the matching branch can finish it while the session state is still coherent. Production stream finalization carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `create_multipart_upload` | `CreateMultipartUpload` | `SnapshotSensitive` | Rerun authorization/object snapshot after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `begin_upload_part_stream_session` | `CreateStreamUpload` | `SnapshotSensitive` | Revalidate MPU/session target and rerun the caller action against fresh MPU state using the request-entry authorization snapshot/capability after contention. The command carries the request-entry bucket-write reservation proof and releases it only after convergence. |
| `create_upload_part_stream_session` | `CreateStreamUpload` | `SnapshotSensitive` | Revalidate MPU/session target after contention. The low-level UploadPartCopy path acquires a durable bucket-write reservation before publishing the session command and releases it only after convergence. |
| `reserve_completed_multipart_upload_order` | `AdvanceCompletedMultipartUploadSequence` | `AllocatorCleanup` | Serialize through the bucket-PG slot; a later object-PG command must be derived from a terminal reservation. |
| `complete_multipart_upload_commit_serialized` | `CommitMultipartObject` | `MatchingOutcomeRetry` | Rebuild completion parts, cleanup snapshot, stale payload, and bucket-PG order after unrelated contention. If the contender is the same completion request, finish that exact command through the matching-pending branch and return its computed outcome. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `finalize_upload_part_stream` | `CommitStreamPart` | `TerminalSessionRetry` | Rebuild stream session, MPU row, staged segments, and displaced part refs after unrelated contention. If an equivalent terminal command wins the pending slot, restart without draining so the matching branch can finish it. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `abort_multipart_upload_locked` | `AbortMultipartUpload` | `TerminalSessionRetry` | Rebuild upload, part, active stream session, staged segment, and cleanup snapshots after unrelated contention. If an equivalent abort wins the pending slot, restart without draining so the matching branch returns the successful abort outcome. |
| `abort_authorized_multipart_upload_locked` | `AbortMultipartUpload` | `TerminalSessionRetry` | Rebuild authorized upload cleanup snapshot after unrelated contention and compare the current upload row to the authorized row before install. If an equivalent abort wins the pending slot, restart without draining so the matching branch returns the successful abort outcome. |

Adding a production call site that creates or installs a pending metadata
command requires updating this table and the boundary check allowlist. Direct
uses of `try_set_pending_metadata_command_for_bucket`,
`try_install_pending_metadata_command_for_bucket`, and
`set_pending_metadata_command_for_bucket` are intentionally tracked.
Snapshot-sensitive publishers should prefer
`install_snapshot_sensitive_metadata_command_or_drain` so slot contention
drains the winner and returns to the caller's fresh-snapshot loop.
Matching-outcome retry publishers are the exception: they may use the lower
level `try_install_pending_metadata_command_for_bucket` only when the
publisher's retry loop first checks for an equivalent pending command and can
return the response from that command. Every such exception must be documented
in the table above, listed in the boundary script allowlist as a
matching-outcome path, and covered by an install-race regression where the
equivalent command wins the pending slot after snapshot/command construction.
Terminal-session retry publishers follow the same lower-level install rule, but
the reason is different: they preserve the equivalent pending command instead
of draining a command that deletes the session row needed by the matching
branch. If they need to inspect an occupied slot before generic drain, they may
call `try_set_pending_metadata_command_for_bucket` directly; any caller-owned
proof or reservation that did not enter the winning command must be released
before the publisher finishes the equivalent contender. Every such publisher
needs a same-command install-race regression. If the raw install reports
`MetadataCommandLogConflict` because the prebuilt command id is already
terminal, that is still ordinary unrelated contention: drain the current PG
slot and restart from a fresh snapshot. Only an occupied slot with an equivalent
terminal command is preserved for the matching branch.
If a PG-wide pending slot appears after the publisher has taken its snapshot
but before it allocates the command id, `MetadataCommandLogConflict` is the
same pre-publish contention class: the publisher must drain the winner and
restart from a fresh snapshot, not surface the conflict to the request.

The multipart publisher rows above are inventoried here and in
`scripts/check-storage-cluster-boundaries` so future publisher changes cannot
silently bypass the snapshot-sensitive restart rules.

## Multipart Command-Stream Invariants

Phase 9.3 owns multipart upload lifecycle serialization. Multipart commands are
still PG-local commands: the bucket PG can own completed-upload order
allocation, while the object PG owns upload, part, stream session, object
publication, and terminal cleanup rows. Multi-PG multipart flows must derive
later commands only from terminal earlier commands; they must not leave two PGs
with ambiguous pending slots where either PG cannot determine whether the
request should converge or restart.

Command-owned multipart metadata includes:

- `multipart_uploads`
- `multipart_parts`
- `multipart_part_segments`
- `stream_uploads`
- `stream_upload_segments`
- `completed_multipart_uploads`
- `buckets.completed_multipart_upload_sequence`

For one upload id, the command stream must make exactly one terminal lifecycle
outcome visible: the upload is still in progress, completed, or aborted. Once a
complete or abort command is terminal, no active UploadPart stream session or
staged stream segment for that upload may remain valid. Cleanup of active
UploadPart stream sessions, staged stream segments, omitted parts, displaced
part segments, and UploadPartCopy staged copied segments must be carried by the
terminal command or by explicit retry/scavenger records. Later cleanup must not
infer those refs from current rows after the terminal command has already
published.

UploadPartCopy is part of the same surface as streamed UploadPart. It creates a
destination UploadPart stream session, appends copied source segments, finalizes
the destination part, and aborts the session if source read/copy work fails.
Phase 9.3 tests must therefore cover UploadPartCopy races with destination MPU
abort and complete, plus cleanup of copied staged segments when the destination
upload becomes terminal before finalize.

The old process-local multipart-completion lock is not part of the completion
path. Completed-upload ordering and object publication correctness must come
from durable PG-primary pending slots, command apply validation, and
fresh-snapshot restart after contention. Remaining bucket locks are not
command-stream authorities and must not be used as production correctness
boundaries.

Multipart publisher rules:

- `begin_upload_part_stream_session` and
  `create_upload_part_stream_session` must reload the in-progress MPU row and
  rerun the caller action against fresh MPU state after object-PG slot
  contention. This uses the request-entry authorization snapshot/capability; it
  does not mean storage reauthorizes against current bucket policy.
- `finalize_upload_part_stream` must reload the stream session, MPU row,
  existing part row, staged segments, and displaced part refs after contention.
  Its `CommitStreamPart` command carries the bucket-write reservation proof;
  live retry and open-time convergence must validate and release that proof
  before clearing the terminal pending slot.
- `abort_multipart_upload_locked` and
  `abort_authorized_multipart_upload_locked` must rebuild upload, part, active
  stream session, staged segment, and cleanup snapshots after contention.
  Matching pending abort contenders are inspected before generic drain because
  draining them can delete the upload row needed to report a successful abort;
  these paths release their unused bucket-write proof before finishing the
  matching abort command.
  Authorized abort must compare the current upload row with the already
  authorized row before publishing; if the row changed, storage returns the
  normal missing/non-abortable outcome instead of applying a stale
  authorization snapshot.
- `complete_multipart_upload_commit_serialized` must reserve completed-MPU
  order through a terminal bucket-PG command before constructing the object-PG
  commit, then reload the object-PG completion snapshot after that order is
  terminal. If another object-PG command wins the pending slot before the
  completion command id is allocated, completion drains the winner and restarts
  from a fresh object-PG snapshot so stale-payload and cleanup refs are rebuilt.
  If an equivalent completion command wins the pending slot after the snapshot
  and command construction, completion must not drain it generically; the retry
  loop must observe the matching pending command, finish it, and return the
  command-derived success outcome.
- A retry of a duplicate UploadPart or finalize request is accepted only when
  the command-owned row image and cleanup refs match exactly, except for
  explicitly documented idempotence fields such as normalized timestamps.

## Finish And Convergence Paths

Publishing a pending command is only the first boundary. A request can also
fail while finishing a pending command after one or more acting-set replicas
have already accepted, mutated, or logged it. Those finish errors must be
classified separately from pending-slot install contention.

The general rule is fail closed unless the caller has a positive proof that
the conflict belongs to the exact same command and hash-chain state. A broad
`MetadataCommandLogConflict` match is not enough: the same PG, epoch, and log
index can also describe a divergent command-log row. A partial exact-command
retry proof must be established by replicas that applied before the failing
replica; later replicas may only confirm that already-established command
chain. If the first failing replica already has the exact command row, retry is
allowed only after validating that row's bytes, checksum, `previous_log_hash`,
and `log_hash` against the primary prefix; a same-index conflict without that
hash-chain proof is still divergent state and must fail closed.

Object-PG pending slots have two separate finish APIs:

- Exact request outcome: the caller has already matched the PG-slot command to
  the request whose result it will return. It must pass an
  `ExactPendingObjectMetadataCommand` proof token and handle `Applied`,
  `Abandoned`, and retryable partial-exact outcomes explicitly.
- Generic drain: the caller only needs to make PG-slot progress before
  restarting from a fresh snapshot. It must not report the drained command as
  the current request's success, and it must not delete extra request state
  outside the command finisher.

The old bucket-named object finisher shape is banned because it hid the fact
that the slot is PG-scoped. Draining a command for another bucket, key, upload,
or generation is normal contention progress; treating it as this request's
outcome is only valid after an exact matching predicate has succeeded.

| Finish caller/path | Command scope | Finish classification | Notes |
| --- | --- | --- | --- |
| `create_bucket_with_config_and_load_info` | bucket PG | Fail closed after partial apply; zero-apply command may be abandoned. | Create is not reported successful until the command converges and the primary row is reloaded. |
| `put_bucket_versioning_and_load_info` | bucket PG | Fail closed after partial apply; zero-apply command may be abandoned. | Request retries re-enter the bucket snapshot path. |
| `put_bucket_acl_and_load_info` | bucket PG | Fail closed after partial apply; zero-apply command may be abandoned. | Request retries re-enter the bucket snapshot path. |
| `put_bucket_property_command_and_load_info` | bucket PG | Fail closed after partial apply; zero-apply command may be abandoned. | Request retries re-enter the bucket snapshot path. |
| `put_bucket_subresource_command_and_load_info` | bucket PG | Fail closed after partial apply; zero-apply command may be abandoned. | Request retries re-enter the bucket snapshot path. |
| `begin_bucket_delete` | bucket PG | Partial exact-command conflicts are retryable only after validating exact command bytes plus matching `previous_log_hash` and `log_hash` across applied rows. Divergent same-index rows fail closed. | Bucket deletion is special because a failed finish can make the bucket visible as `Deleting` and allow a queued finalizer to remove it before an SDK retry. |
| `delete_completed_multipart_upload_record_with_command` | bucket PG | Fail closed after partial apply; zero-apply command may be abandoned. | Used as cleanup after completed MPU retention decisions. |
| `reserve_completed_multipart_upload_order` | bucket PG | Fail closed after partial apply; zero-apply command may be abandoned. | The returned order must come from a terminal bucket-PG command. |
| `drain_pending_metadata_command_pg_slot` and `drain_pending_completed_multipart_sequence_command` | bucket PG drain | Fail closed on unsafe finish conflicts. | These are generic drain helpers; they must not hide divergent command-log state from the caller. |
| `finish_pending_command_for_completed_multipart_order` | bucket/object PG drain | Follows the command family finisher. | Multi-PG MPU completion must not hold ambiguous pending state across PGs; Phase 9.3 pins the multipart serialization rules. |

The boundary script inventories production uses of
`MetadataCommandLogConflict` and `metadata_command_log_conflict_matches`.
Adding a new broad match must update that inventory and document why it is
pre-publish retry, exact-command convergence, or fail-closed validation.
Pre-publish command-id allocation conflicts are retryable only for
snapshot-sensitive loops that immediately drain the durable PG slot and rerun
their request action from a fresh snapshot. Finish/convergence conflicts after
any replica may have accepted the command still require exact command bytes and
hash-chain proof, or must fail closed.

## Object Version Reservations

Object version IDs are allocated by the `ReserveObjectVersion` metadata
command before some terminal object commands are installed. Once that
reservation command has applied, losing the later terminal command's pending
slot race may leave an unused version ID if the fresh retry no longer passes
request preconditions. That gap is allowed: object version IDs are opaque
allocator outputs, and the reserved value is not published as an object version
unless the terminal object command applies.

Tests should distinguish this from precondition failures that happen before
reservation. A request rejected before `ReserveObjectVersion` must not advance
`object_version_counters`; a request that has already durably reserved a
version may advance the counter even if later pending-slot contention causes a
fresh retry to fail.

## Recovery

Before accepting new work on a PG, recovery must inspect durable command-stream
state:

- if there is a pending command slot that has not been accepted, mutated, or
  logged by any acting-set replica, retry command application or durably
  abandon it according to the command's normal rules
- if any acting-set replica accepted, mutated, or logged the command, recovery
  must converge that same command to a terminal durable record; it must not
  abandon the command and issue a later replacement
- recovery convergence must use the same primary-last apply ordering as normal
  command fanout. A terminal primary log row is not sufficient reason to clean
  the primary pending slot until all required replicas have either converged or
  the PG has failed closed for repair.
- open-time command convergence only proves metadata convergence. Any
  post-commit best-effort physical payload cleanup that would normally run
  after the command commits can still require the later scavenger path, just as
  it would after a crash between metadata commit and cleanup.
- if a terminal log record exists for the slot, finish slot cleanup
- if a replica has a contiguous abandoned log tail but no pending slot,
  advance the replica index/hash across that abandoned tail while preserving
  the previous materialized-state digest; an abandoned tail must not bless any
  bucket/object rows that the abandoned command should not have changed
- an unadvanced applied log tail without matching materialized state is not
  recoverable and must fail closed
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

`CreateStreamUpload` also avoids carrying a broad `StreamUploadRecord`. Its
payload contains the command-owned session projection plus an explicit initial
`next_segment_vid` floor. Retry matching must compare both pieces: session
identity/state/encryption proves the command-owned row, while the initial floor
proves the allocator started from the expected value. Later allocator progress
belongs to append allocation and append commands, not to generic stream-session
equality.

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
