<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

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
4. After final admission succeeds, durably mark the exact primary pending slot
   as publication-started while holding the primary command section. This mark
   must commit before any witness apply is dispatched.
5. For a redundant acting set, durably apply the
   exact command to the lowest-node-ID non-primary replica. This deterministic
   member is the off-primary publication witness.
6. Apply the exact command to the PG primary. The primary's durable row is the
   serving-publication boundary; the off-primary witness ensures that state
   visible through the primary is already durable in another configured failure
   domain.
7. Apply the exact command to the remaining non-primary acting-set replicas in
   deterministic node-id order.
8. Record the applied command log entry or abandoned tombstone durably on each
   acting-set member as part of that member's atomic metadata transaction.
9. Mark the slot terminal and remove it only after the terminal durable record
   is visible enough for retry/recovery and acting-set convergence has been
   proven, or an explicit command path owns an equivalent cleanup proof.

A failure before publication-start uses the caller's finite work budget. Once
publication-start may have been recorded, publication confirmation uses its own
short absolute deadline and retries only the exact installed command. The
client transport carries a typed `NotSent`, `Definitive`, or `MayHaveApplied`
result from connection admission through authenticated response verification;
coordinators must not infer dispatch from a broad error such as `Io` or
`PayloadDecode`. The one deadline covers admission, connect, write, response,
and exact-state observation, and no new operation may start after it expires.
Once the primary has confirmed publication, a recoverable trailing-replica
failure is handed to the long-lived recovery worker instead of retaining the
request worker. No stage may allocate a replacement command or rerun request
preparation after publication-start. After acting-set application has converged,
transient reservation-release failure is terminal cleanup rather than a failed
mutation outcome. The primary pending slot remains durable so command recovery
can retry replica convergence or release, and is removed only after both
succeed.

### Publication And Retry Boundary

Witness-then-primary fanout is a deliberate protocol contract, not merely an
iteration order. A request remains **abortable** only until final admission has
succeeded and before the primary pending slot is durably marked
publication-started. Bounded contention or an authoritative rejection in that
state may return `SlowDown`, and a command that provably did not cross that
marker may be abandoned according to its command-family rules.

The request enters **irrevocable convergence** as soon as either:

- the exact primary pending slot is durably marked publication-started;
- any acting-set member is known to have accepted, mutated, or logged the exact
  command; or
- a dispatched apply has an ambiguous transport outcome, so the caller cannot
  prove that no durable apply occurred.

The publication-start marker is durable intent, not a serving-publication or
success witness. It closes the interval in which a witness RPC has been
accepted by the server but has not committed yet: a replacement owner that
acquires the primary section must reconstruct the marker and retain the exact
command even when no actor row is currently observable. Abandonment and reissue
must reject the marked slot. The S3 success boundary remains confirmed exact
rows on both the deterministic witness and the primary.

Standalone restart must also treat a marked primary pending slot as
irrevocable even when every actor row is still at the preceding log index. It
must converge that exact command without revalidating an expired command-owned
reservation, then perform terminal reservation release and pending-slot
cleanup. An unmarked command with no applied actor row remains abortable and
must pass ordinary admission validation before its first apply.

Post-budget publication classification must observe the exact primary pending
slot while holding the command-bound primary section. If the section, marker,
or actor state cannot be observed before the shared absolute deadline, the
classifier retains irrevocable uncertainty; it must not infer `NotPublished`
from the absence of an actor row and return caller-visible `SlowDown`.

After that boundary the implementation must converge the same canonical
command and hash-chain position. It must not abandon or replace the command,
rerun request preparation, or turn request-work-budget exhaustion into a
caller-visible `SlowDown`. The exact durable row already present on the witness
proves the original admission for the primary and later replicas even if the
original bucket-write reservation expires during convergence. Divergent
same-index state remains a fatal fail-closed condition; it is not retryable
contention.

Normal reads may observe the primary's published mutation while the originating
request is still converging trailing replicas. The S3 success response may be
published once both the off-primary witness and primary apply are confirmed.
The request does not wait indefinitely for every trailing replica: a
recoverable trailing failure retains the exact primary pending slot and hands
convergence to the replicated-mode recovery worker. Heartbeat discovery fences
the PG in Peering, and the refresh worker uses the exact command identity plus
the process-wide per-command recovery flight to deduplicate convergence. The
pending slot and bucket-write reservation remain durable until that worker has
converged every required replica and completed terminal cleanup.

Response construction must not introduce a new fallible storage or parsing
boundary after publication. Bucket metadata mutations return a receipt derived
from the exact applied command, including its bucket execution generation;
they do not issue a second bucket read to construct success or invalidate the
frontend cache. This includes the `Created` outcome from `CreateBucket`.
`CreateBucket::Exists` carries the pre-publication bucket snapshot needed to
apply AWS ownership and legacy-region semantics; it is not reconstructed after
a create command publishes. Object operations must parse stored lifecycle
configuration and resolve every other fallible response input before publishing
object metadata. After publication, only infallible projection of the
already-validated inputs and command outcome is permitted.

Cross-PG dependency commands are stricter. Publishing a dependency command on
its witness and primary is irrevocable, but does not authorize publication of
the dependent PG command. The dependency finisher must require convergence on
the complete acting set. If a trailing replica cannot be confirmed within the
bounded request work, it returns the typed internal
`MetadataCommandDependencyConvergencePending` outcome, leaves the exact command
and reservation durable for recovery, and does not publish the dependent
command. This outcome is not metadata contention and must not map to
caller-visible `SlowDown`.

An ambiguous witness or primary response is different from a confirmed
publication. The request retries the exact command under a short absolute
confirmation deadline. If it still cannot distinguish applied from not
applied, it returns an internal outcome-unconfirmed error, never `SlowDown`, and
retains the pending slot for recovery. A later request must drain that exact
slot before preparing another command. Ambiguous loss after confirmed primary
publication is resolved by the same exact-command checks or handed off as
published recovery; it never issues a replacement. Transient terminal cleanup
after convergence is also deferred and cannot replace the committed response
with an error.

A definitive routing or transport-admission failure after the witness returns
the internal `MetadataCommandIrrevocableConvergencePending` outcome rather than
caller-retryable contention. A definitive semantic, command-byte, checksum, or
hash-chain failure is preserved verbatim for diagnosis and fail-closed
recovery; it must not be overwritten as generic outcome uncertainty.

Standalone one-replica mode has no second failure domain and therefore has no
off-primary witness. Its configured lack of redundancy is explicit; the same
exact-command retry and pending-slot rules still prevent replacement after an
ambiguous primary dispatch.

This contract prevents Argmin from returning an explicitly retryable S3 error
after publishing a version that a retry could duplicate. A process or network
failure can still prevent delivery of the final HTTP response; as with AWS S3,
that transport-level outcome is inherently ambiguous to the client. Argmin
must not manufacture the same ambiguity as an application-level `SlowDown`.

Authorization is decided before command admission. A policy change racing an
already admitted command does not revoke that command during irrevocable
convergence; either admission loses the race and the primary does not apply,
or the exact admitted command converges and returns its committed result.

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

Normal and recovery convergence inspection treats a missing log entry or a
`MetadataCommandLogConflict` from any acting-set member as “not applied on all
acting nodes.” It must not accept that command as converged or classify a
same-index conflict as proof of application; the owning finish/recovery path
must retain the pending command and perform the exact-command checks described
below.

## Publisher Classification

Command safety is classified at the publisher path, not just by command kind.
The risk depends on which metadata snapshot, authorization result, or request
precondition was used before the pending slot was installed.

For frontend-admitted publishers, checking admission before calling the
installer is insufficient: lock waits and RPC transport can cross the captured
deadline while the raw same-epoch route is renewed. Their immutable
`AdmittedRouteEffectFence`, including the admission's conservatively bound
monotonic deadline, is therefore carried to the effect boundary. Monotonic
timestamps are never serialized: RPC requests carry a portable wall-clock
upper bound, and the receiving host subtracts the inter-host skew budget before
binding it to its own monotonic clock. Embedded and RPC nodes revalidate clock
health and their local effective deadline immediately before the durable
pending-slot insert.

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

Direct streamed `PutObject` session liveness is represented by the
stream-create bucket-write reservation proof, not by the stream row age. A live
frontend refreshes that proof while the request can still append or finalize.
`DeleteBucket` must treat a direct `PutObject` stream row with a validating
proof as live bucket contents and return the normal non-empty result; if the
proof is missing, mismatched, or expired, the stream is abandoned staging and
may be aborted during DeleteBucket's synchronous drain. The background stream
session sweeper uses the same proof-validation rule so abandoned sessions are
eventually cleaned even without a DeleteBucket request.

Current production pending-command publishers:

| Publisher path | Command kind | Class | Required contention shape |
| --- | --- | --- | --- |
| `create_bucket_with_config_and_load_info_with_route_validation` | `CreateBucket` | `ApplyValidated` | Revalidate the admitted bucket route before command construction and carry its immutable effect fence to pending-slot insertion; drain competing PG slot during pending-slot checks and command-id allocation, while exact-row apply handles idempotence/conflict. |
| `begin_bucket_delete_if_current_with_route_validation` | `MarkBucketDeleting` | `SnapshotSensitive` | Revalidate the admitted bucket route before the durable write drain and pending-slot insertion, then rebuild from current bucket/delete preconditions after contention. |
| `delete_bucket_from_acting_set` | `DeleteFinalizedBucket` | `SnapshotSensitive` | Rebuild from the current deleting-bucket generations after unrelated contention; an equivalent pending finalization may be finished only after its exact bucket identity is matched. |
| `put_bucket_versioning_with_route_validation` | `PutBucketVersioning` | `SnapshotSensitive` | Drain competing PG slot during pending-slot checks and command-id allocation; rebuild bucket post-image after contention. |
| `put_bucket_acl_with_route_validation` | `PutBucketAcl` | `SnapshotSensitive` | Drain competing PG slot during pending-slot checks and command-id allocation; rebuild bucket post-image after contention. |
| `put_bucket_property_command_with_route_validation` | `PutBucketProperty` | `SnapshotSensitive` | Drain competing PG slot during pending-slot checks and command-id allocation; rebuild bucket post-image after contention. |
| `put_bucket_subresource_command_with_route_validation` | `PutBucketSubresource` | `SnapshotSensitive` | Drain competing PG slot during pending-slot checks and command-id allocation; rebuild bucket post-image after contention. |
| `reserve_put_object_generation_with_route_validation` | `ReserveObjectGeneration` | `AllocatorCleanup` | Revalidate admitted PutObject authority before each allocation attempt and carry its immutable effect fence to pending-slot insertion. Drain competing PG slot before and during command-id allocation, reread the allocator before final command-id allocation, then allocate/reuse through the reservation owner. If the generation becomes stale before publish, exact `ObjectGenerationReservationConflict` is retried without releasing a non-matching reservation. |
| `reserve_next_object_version` | `ReserveObjectVersion` | `AllocatorCleanup` | Drain competing PG slot before and during command-id allocation, then allocate/reuse through the version reservation owner. |
| `release_object_generation_reservation_command_required` | `ReleaseObjectGeneration` | `AllocatorCleanup` | Required cleanup must retry through slot contention, including command-id contention, until terminal or fail without losing the cleanup intent. |
| `release_object_generation_reservation` | `ReleaseObjectGeneration` | `AllocatorCleanup` | Release an already-owned reservation; drain competing PG slot before and during command-id allocation and retry. |
| `commit_direct_put_object_from_payload_shards_with_route_validation` | `CommitDirectPutObject` | `SnapshotSensitive` | Revalidate the admitted PutObject route while rebuilding the commit from fresh object preconditions and stale-payload snapshots after pending-install contention or pre-publish command-id contention. Carry the immutable effect fence to version reservation and final pending-slot insertion. The commit command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `create_put_object_stream_session_record_under_reservation` | `CreateStreamUpload` | `SnapshotSensitive` | Low-level PutObject stream-create publisher. The public wrapper first holds a durable bucket write reservation; this internal publisher rebuilds session command and reservation cleanup from fresh object state after contention. |
| `commit_stream_segment_append` | `AppendStreamSegment` | `ApplyValidated` | Command-id and pending-install contention are drained and retried from a fresh stream-session snapshot without deleting the staged payload; apply validates session binding/state and existing staged segment before inserting. |
| `abort_stream_upload_session` | `AbortStreamUpload` | `TerminalSessionRetry` | Rebuild staged-segment snapshot after unrelated pending-slot or command-id contention. If an equivalent terminal command wins the pending slot, restart without draining so the matching/session-completion branch can finish it. |
| `put_object_metadata_if_with_route_validation` | `PutObjectMetadata` | `SnapshotSensitive` | Revalidate request-scoped route authority and rerun request action/preconditions after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `delete_specific_object_version_if_with_route_validation` | `DeleteObjectVersion` | `SnapshotSensitive` | Revalidate request-scoped route authority and rerun delete preconditions after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `delete_current_object_if_with_route_validation` | `DeleteObjectVersion` | `SnapshotSensitive` | Revalidate request-scoped route authority and rerun current-object selection after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `insert_current_delete_marker_if_with_route_validation` | `InsertDeleteMarker` | `SnapshotSensitive` | Revalidate request-scoped route authority and rerun current-object/versioning selection after contention. A matching pending marker is finished as the request's outcome only if it applies; if its snapshot becomes stale and it is abandoned, the request discards the computed result and re-evaluates its condition against a fresh object snapshot. The version-ID reservation and marker command are both fenced at their durable insertion boundaries. Each command carries a durable bucket-write reservation proof where applicable, and terminal convergence releases it before removing the pending slot. |
| `expire_current_object_if_due_raw` | `DeleteObjectVersion`/`InsertDeleteMarker` | `SnapshotSensitive` | Rerun lifecycle selector and object-lock checks after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `delete_noncurrent_live_versions_if_due_raw` | `DeleteObjectVersion` | `SnapshotSensitive` | Rerun lifecycle selector and object-lock checks after contention. Each delete command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `delete_expired_delete_marker_if_due_raw` | `DeleteObjectVersion` | `SnapshotSensitive` | Rerun expired-marker selector after contention. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `reclaim_object_payload_if_unleased` | `DeleteObjectPayloadReclaim` | `SnapshotSensitive` | Recheck lease/fence and reclaim state after contention, including command-id contention after payload cleanup has started; cleanup must preserve retryability. |
| `create_put_object_stream_session_with_route_validation` | `CreateStreamUpload` | `SnapshotSensitive` | Rerun authorization/object snapshot after contention; enforce the request admission's immutable effect fence at reservation and pending-command insertion; frontend streams also persist the admitted route deadline for durable cleanup handoff. |
| `finalize_put_object_stream_with_route_validation` | `CommitDirectPutObject` | `TerminalSessionRetry` | Rebuild commit from current object preconditions and stream-session snapshot after unrelated contention. If an equivalent terminal command wins the pending slot, restart without draining so the matching branch can finish it while the session state is still coherent. Production stream finalization carries the request admission's immutable effect fence and a durable bucket-write reservation proof; terminal convergence releases the proof before removing the pending slot. |
| `create_multipart_upload_inner_with_route_validation` | `CreateMultipartUpload` | `SnapshotSensitive` | Revalidate the request-scoped multipart object route and rerun authorization/object snapshot after contention. The reservation acquisition and pending-command installation both enforce the admitted effect fence. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `create_upload_part_stream_session_with_route_validation` | `CreateStreamUpload` | `SnapshotSensitive` | Revalidate the admitted multipart object route and MPU/session target after contention. UploadPart and UploadPartCopy carry the immutable request effect fence through reservation acquisition and pending-command insertion, persist the admitted cleanup deadline, and release the durable bucket-write proof only after convergence. |
| `establish_multipart_completion_barrier` | `AdvanceMultipartCompletionBarrier` | `AllocatorCleanup` | Drain competing bucket-PG slots, validate the current completion bucket-write proof, allocate and replicate a fresh monotonic scalar, and only then publish the object-PG completion. A pre-existing or contender barrier is not proof for the current reservation because the command carries no reservation identity; return to the owner loop and build a fresh barrier after contention. No terminal upload row is created. |
| `complete_multipart_upload_commit_serialized_with_route_validation` | `CommitMultipartObject` | `MatchingOutcomeRetry` | Revalidate the admitted multipart object route while rebuilding completion parts, cleanup snapshot, stale payload, and bucket-PG barrier after unrelated contention. The completion reservation, version reservation, barrier insertion, and final object-PG insertion all enforce the request admission's immutable effect fence. If the contender is the same completion request, finish that exact command through the matching-pending branch and return its computed outcome. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `finalize_upload_part_stream_with_route_validation` | `CommitStreamPart` | `TerminalSessionRetry` | Revalidate the admitted multipart object route while rebuilding the stream session, MPU row, staged segments, and displaced part refs after unrelated contention. Carry the immutable request effect fence through the finalization reservation and pending-command insertion. If an equivalent terminal command wins the pending slot, restart without draining so the matching branch can finish it. The command carries a durable bucket-write reservation proof and terminal convergence releases it before removing the pending slot. |
| `abort_multipart_upload_locked` | `AbortMultipartUpload` | `TerminalSessionRetry` | Rebuild upload, part, active stream session, staged segment, and cleanup snapshots after unrelated contention. If an equivalent abort wins the pending slot, restart without draining so the matching branch returns the successful abort outcome. |
| `abort_authorized_multipart_upload_locked` | `AbortMultipartUpload` | `TerminalSessionRetry` | Rebuild authorized upload cleanup snapshot after unrelated contention and compare the current upload row to the authorized row before install. The foreground S3 path acquires its reservation and installs its pending command through the request admission's immutable effect fence. If an equivalent abort wins the pending slot, restart without draining so the matching branch returns the successful abort outcome. |

Adding a production call site that creates or installs a pending metadata
command requires adding it to the authoritative Rust registry and updating
this table. Production publisher entry points cannot directly use
`try_set_pending_metadata_command_for_bucket`,
`try_install_pending_metadata_command_for_bucket`,
`try_install_object_pg_pending_command_with_fresh_id`, and
`set_pending_metadata_command_for_bucket`; the boundary checker rejects any
such call outside the compiler-classified typed installer implementations.
Snapshot-sensitive publishers use
`install_snapshot_sensitive_metadata_command_or_drain` so slot contention
drains exactly the observed winner and returns to the caller's fresh-snapshot
loop. The typed installer must not drain until the slot is empty: each owner
iteration rechecks its request work budget and admitted route authority before
handling another contender.
Terminal-session publishers use
`install_terminal_session_retry_metadata_command`, whose exhaustive result
distinguishes installation, a matching visible contender, an unrelated visible
contender, and contention without a visible slot. The helper never drains a
contender: the owner loop must preserve a matching terminal command and may
drain unrelated work only after it has rerun its matching check.
Matching-outcome publishers use
`install_matching_outcome_retry_metadata_command`, whose exhaustive result
also preserves a matching visible command instead of draining it. The command
envelope carries the response row and reservation ownership evidence needed by
the retrying caller. Unrelated or no-longer-visible contention returns to the
owner loop, which reruns its matching check before any generic drain. Every
matching-outcome publisher must be documented above and covered by an
install-race regression where the equivalent command wins after command
construction.
If a PG-wide pending slot appears after the publisher has taken its snapshot
but before it allocates the command id, `MetadataCommandLogConflict` is the
same pre-publish contention class: the publisher must drain the winner and
restart from a fresh snapshot, not surface the conflict to the request.

The registry generates one sealed publisher token per row. Typed installers
accept only tokens whose registry class matches their retry/convergence path;
changing a migrated publisher's class therefore breaks its production call
site at compile time. Typed calls no longer participate in the temporary shell
helper-count inventory, although the boundary check still requires their
authoritative registry marker and rejects every direct call or function-item
reference to the private-field token constructor. Class-specific seals are
implemented only by the corresponding registry macro arms, so one publisher
class cannot acquire another class's authority inside the crate.

The object-PG snapshot-sensitive installer returns exhaustive, must-use
`SnapshotSensitiveInstallOutcome`. Allocator/cleanup publication uses separate
exhaustive, must-use fresh-command and prebuilt-command outcomes. Those paths
distinguish installed work from a drained pending-slot contender and handled
log-index contention, so retry cannot be mistaken for successful allocation or
cleanup. Terminal-session publication returns the exhaustive, must-use
`TerminalSessionRetryInstallOutcome`; matching command evidence is carried in
that type and is never generically drained inside the typed installer.
Matching-outcome publication provides the corresponding exhaustive, must-use
`MatchingOutcomeRetryInstallOutcome`, retaining the exact command-owned result
until the caller can converge it.
Bucket-control snapshot-sensitive publication uses the same exhaustive
`SnapshotSensitiveInstallOutcome` at its distinct fenced control-slot
boundary. Versioning, ACL, bucket-property, and subresource publishers must
therefore handle a drained contender by restarting from their current bucket
snapshot rather than treating the attempted install as successful.
Bucket-delete begin and finalization use the same exhaustive outcome at the
bucket-PG slot boundary. Fresh mark/finalize commands restart after a drained
contender, while finalization retains its separate exact-command branch so a
matching already-pending delete can converge without being rebuilt or drained.
Apply-validated publication likewise has separate exhaustive, must-use
fresh-command and prebuilt bucket-PG outcomes. Stream append preserves the
distinction between a drained visible contender and a handled log-index
conflict because each path has its own retry-budget diagnostic and payload
reference-check boundary; CreateBucket must explicitly restart when its
prebuilt command loses the pending-slot race.
Publisher paths which still call lower-level installers remain inventoried here
and in
`scripts/check-storage-cluster-boundaries` until their Phase 4 typed API is
introduced.

## Multipart Command-Stream Invariants

Phase 9.3 owns multipart upload lifecycle serialization. Multipart commands are
still PG-local commands: the bucket PG owns the fixed-size completion barrier,
while the object PG owns upload, part, stream session, object publication, and
cleanup rows. Multi-PG multipart flows must derive later commands only from
terminal earlier commands; they must not leave two PGs with ambiguous pending
slots where either PG cannot determine whether the request should converge or
restart.

Command-owned multipart metadata includes:

- `multipart_uploads`
- `multipart_parts`
- `multipart_part_segments`
- `stream_uploads`
- `stream_upload_segments`
- object-row multipart completion upload identity and request fingerprint
- `buckets.multipart_completion_barrier_sequence`

For one upload ID, the command stream must make at most one active lifecycle
outcome visible: an in-progress upload row or completion replay attached to its
published object version. Abort leaves neither. Once a complete or abort command
is terminal, no active UploadPart stream session or staged stream segment for
that upload may remain valid. Cleanup of active
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

- `create_upload_part_stream_session_with_route_validation` must reload the
  in-progress MPU row after object-PG slot contention and compare it with the
  request-entry authorized upload record. This uses the request-entry
  authorization snapshot/capability; it does not mean storage reauthorizes
  against current bucket policy.
- `finalize_upload_part_stream_with_route_validation` must reload the stream
  session, MPU row, existing part row, staged segments, and displaced part
  refs after contention. Its `CommitStreamPart` command carries the
  bucket-write reservation proof; live retry and open-time convergence must
  validate and release that proof before clearing the terminal pending slot.
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
  authorization snapshot. Foreground authorized abort also binds reservation
  acquisition and pending-slot installation to its request admission; raw
  unbounded authority remains test-only.
- `complete_multipart_upload_commit_serialized_with_route_validation` must
  reserve completed-MPU order through a fully converged bucket-PG command before
  constructing the object-PG commit, then reload the object-PG completion
  snapshot after that dependency converges. A bucket-PG recovery handoff is not
  sufficient authorization to publish the object-PG command. If another
  object-PG command wins the pending slot before the
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
| `create_bucket_with_config_and_load_info_with_route_validation` | bucket PG | Abort only before publication-start; exact confirmation or recovery handoff afterward. | A confirmed primary publication is returned as success while trailing convergence retains the exact slot. |
| `put_bucket_versioning_with_route_validation` | bucket PG | Abort only before publication-start; exact confirmation or recovery handoff afterward. | A fresh request snapshot is allowed only before irrevocable convergence; success carries a command-derived mutation receipt. |
| `put_bucket_acl_with_route_validation` | bucket PG | Abort only before publication-start; exact confirmation or recovery handoff afterward. | A fresh request snapshot is allowed only before irrevocable convergence; success carries a command-derived mutation receipt. |
| `put_bucket_property_command_with_route_validation` | bucket PG | Abort only before publication-start; exact confirmation or recovery handoff afterward. | A fresh request snapshot is allowed only before irrevocable convergence; success carries a command-derived mutation receipt. |
| `put_bucket_subresource_command_with_route_validation` | bucket PG | Abort only before publication-start; exact confirmation or recovery handoff afterward. | A fresh request snapshot is allowed only before irrevocable convergence; success carries a command-derived mutation receipt. |
| `begin_bucket_delete_if_current_with_route_validation` | bucket PG | After publication-start, partial exact-command conflicts are retryable only after validating exact command bytes plus matching `previous_log_hash` and `log_hash`; divergent same-index rows fail closed. | Once the primary publishes `Deleting`, a recoverable trailing error returns the committed outcome and retains the slot for recovery. |
| `establish_multipart_completion_barrier` | bucket PG | Abort only before publication-start; after publication-start, retain the exact command and require complete acting-set convergence. | A published but unconverged barrier returns an internal dependency-pending outcome and must not publish the object-PG command. The returned idempotence sequence comes only from the exact fully converged barrier. |
| `drain_pending_metadata_command_pg_slot` and `drain_pending_multipart_completion_barrier_command` | bucket PG drain | Fail closed on unsafe finish conflicts. | These are generic drain helpers; they must not hide divergent command-log state from the caller. |
| `finish_pending_command_for_multipart_completion_barrier` | bucket/object PG drain | Follows the command family finisher. | Multi-PG MPU completion must not hold ambiguous pending state across PGs; Phase 9.3 pins the multipart serialization rules. |

Pre-publish conflict handling belongs to the compiler-classified publisher.
Every registered publisher token implements exactly one sealed class trait,
and even its generic contender-drain paths require that token. A pre-publish
conflict branch drains at most one contender before returning to its
route-authority, work-budget, and fresh-snapshot checks. Preflight paths that
must inspect applied commands for idempotence retain a separate token-gated
collecting drain with one shared finite work budget. Terminal and
matching-outcome classes preserve equivalent pending commands through their
exhaustive typed outcomes instead of draining them.

Storage-owned inspection and recovery do not borrow a publisher token. They
must instead hold an opaque, non-copy recovery-drain authority which borrows
one request work budget across every contender. The primitive drain itself
accepts only a typed per-invocation authority derived from either that recovery
authority or a registered publisher token; it has no optional raw-budget or
default-budget path. Both authority states have fields private to a child
module, preventing same-module struct-literal construction. The boundary
scanner inventories every live recovery-authority constructor, conversion,
primitive call, recovery wrapper, recovery execution-route construction, and
recovery-only bucket finisher (including function-item references), so an
unmarked publisher cannot mint or invoke recovery authority without changing
the audited inventory. The primitive converts its joined leader guard into an
opaque leader capability. Every lower historical apply, abandonment, reissue,
and pending-slot removal boundary requires a proof borrowed from that live
capability, so neither a direct lower call nor a recovery execution-route
struct literal can escape the joined guard's lifetime. The proof is also bound
to the joined guard's exact `(PG, log index, checksum)` recovery subject, and
every lower boundary checks that subject before storage access. Reissue returns
a newly derived proof bound to the replacement command only after validating a
same-epoch, later-index chain with either identical payload or the one explicit
abandoned-command cleanup derivative. Thus a leader for one command cannot be
redirected to another command, while legitimate recovery follow-ups remain
explicitly certified. A cleanup proof also retains the exact abandoned-command
predecessor: lower mutation and subsequent same-payload reissue both require
that context, and omission is rejected rather than silently broadening the
proof. The scanner inventories those lower calls and
struct-literal syntax as defense in depth. Production exposes no raw
all-command drain: the remaining unclassified convenience drain is test-only,
and the scanner rejects any production reference to it, including from an
unmarked wrapper.

Finish/convergence handling is a separate storage-engine boundary. A caller
may report a pending command as its request's outcome only through an
`ExactPendingObjectMetadataCommand` created after matching that exact request.
After any replica may have accepted a command, convergence still requires exact
command bytes and hash-chain proof; divergent same-index state fails closed.
These compiler-visible publisher and exact-command boundaries replace the old
shell inventory of function-name occurrence counts.

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
- recovery convergence must use the same witness-then-primary apply ordering as
  normal command fanout. The off-primary row proves admission and the primary
  row proves serving publication, but neither is sufficient reason to clean the
  primary pending slot until all required replicas have either converged or the
  PG has failed closed for repair.
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
example, multipart completion establishes its bucket-PG barrier and then commits
object metadata on the object PG. These flows must use a deterministic PG order
and explicit retry cleanup. They must not leave
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
