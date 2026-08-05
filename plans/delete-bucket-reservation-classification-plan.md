# DeleteBucket Reservation Classification Optimization

## Goal

Reduce unnecessary synchronous `DeleteBucket` waiting without weakening the
Phase 9.4 correctness rule that already-admitted bucket writers must not publish
visible data after `DeleteBucket` has decided the bucket is empty.

This is an optimization plan, not part of the Phase 9.4.4 correctness closeout.
The conservative implementation may continue to wait for durable bucket write
reservations to empty. This plan describes a future refinement that can return
`BucketNotEmpty` or proceed with deletion from durable reservation context when
that decision is mechanically provable.

Background: [Phase 9.4.4 in the completed multihost transition
plan](completed/multihost-transition-plan.md#phase-9-replace-process-local-coordination)
keeps the conservative return boundary while DeleteBucket begin is made durable
and recoverable.

## Current Conservative Rule

`begin_bucket_delete` installs the durable delete drain, drains pending
bucket/object metadata commands, then waits until the bucket-PG primary has no
durable `bucket_write_reservations` rows for the bucket. While waiting, it keeps
draining bucket-relevant object-PG pending commands, because some reservations
have already been transferred into pending or partially applied metadata
commands.

This rule is simple and correct, but it can wait for admitted writers whose
future outcome could be classified without waiting.

The conservative wait must still be bounded at the HTTP operation level. A
durable reservation that cannot be resolved by draining pending commands must
not make `DeleteBucket` hang until the client SDK attempt timeout. The current
fallback is to return `BucketNotEmpty` after a short reservation-drain grace
period, log the stuck reservation context, and let the client/test cleanup retry.
This preserves the Phase 9.4 rule because the reservation is not discarded and
the owning writer still has to converge or fail against the durable delete
drain.

## Optimization Model

The key distinction is the writer stage:

- `AdmissionOnly`: the request has authorization and a bucket snapshot, but has
  not passed all fallible request-input stages. Examples include streaming body
  receipt, checksum validation, source-copy reads, or other input work that can
  still fail before publish. DeleteBucket must not infer non-emptiness from this
  reservation alone.
- `PublishReady`: the request has passed the initial/fallible input stages and
  is ready to publish metadata if its object-state precondition allows it.
  DeleteBucket may be able to decide from this reservation without waiting.
- `CommandOwned`: the proof has been installed into a pending or accepted
  metadata command. DeleteBucket must converge the command or fail closed before
  trusting emptiness.

The optimization should only use reservation classification after the durable
delete drain is installed, so no new writer can enter behind the decision.

## Decision Rules

For `PublishReady` reservations, DeleteBucket can avoid waiting when the outcome
is determined by empty-bucket state:

- Unconditional object creation or replacement: if it is publish-ready, it can
  make the bucket non-empty, so DeleteBucket may return `BucketNotEmpty`.
- Versioned `DeleteObject` delete-marker insertion: if it is publish-ready, it
  can create a delete marker even when the key has no current object. An
  otherwise empty versioned bucket can therefore become non-empty, so
  DeleteBucket may return `BucketNotEmpty`.
- Destination `If-None-Match: *`: if the bucket is empty, the destination key has
  no current object and the condition succeeds; successful publish makes the
  bucket non-empty, so DeleteBucket may return `BucketNotEmpty`.
- Destination `If-Match: <etag>`: if the bucket is empty, there is no current
  object that can match, so the write is guaranteed to fail; DeleteBucket may
  proceed without waiting for that reservation, provided the writer will observe
  the terminal delete/drain fence before publish.
- Object metadata/tag/ACL/object-lock mutations: a valid publish-ready mutation
  requires an existing target object/version, so the normal fresh visible-data
  scan should already report the bucket non-empty. If the target disappears
  before publish, the operation fails and should not block deletion.

Any operation whose outcome depends on request body completion, checksum
validation, source read success, MPU part availability, or another post-admission
fallible stage must stay `AdmissionOnly` until that stage has passed.

## Required Durable Context

Reservation rows need enough context to make the above decisions auditable:

- bucket incarnation/generation proof
- owner token and cluster epoch
- operation class
- writer stage: `AdmissionOnly`, `PublishReady`, or `CommandOwned`
- target key/version/upload/session identity where relevant
- conditional write class, at least unconditional, `If-None-Match: *`, and
  `If-Match`

The command payload still remains the authority once a reservation is
`CommandOwned`; DeleteBucket should drain the command rather than reasoning from
the reservation row alone.

## Implementation Sketch

1. Extend bucket write reservation records with operation class, stage, and
   condition summary.
2. Move reservation stage transitions to explicit storage APIs so stage changes
   are durable and testable.
3. Teach `begin_bucket_delete` to classify reservations after installing the
   durable drain and draining existing pending commands.
4. Return `BucketNotEmpty` immediately for publish-ready reservations that
   prove a visible-data producer.
5. Ignore or terminally fence publish-ready reservations that are guaranteed to
   fail against an empty bucket, such as destination `If-Match`.
   Proceeding past such a reservation requires either a durable terminal-fenced
   reservation state, or proof that command apply will re-read the terminal
   delete drain / `Deleting` bucket state and reject before any metadata
   mutation.
6. Continue waiting or draining for `AdmissionOnly` and `CommandOwned`
   reservations unless a later rule proves a safe outcome.

## Required Tests

- DeleteBucket vs publish-ready unconditional PUT returns `BucketNotEmpty`
  without waiting for the caller to release its reservation.
- DeleteBucket vs publish-ready `If-None-Match: *` PUT on an otherwise empty
  bucket returns `BucketNotEmpty`.
- DeleteBucket vs publish-ready versioned `DeleteObject` delete-marker insertion
  on an otherwise empty bucket returns `BucketNotEmpty`.
- DeleteBucket vs publish-ready `CreateMultipartUpload` or
  `CompleteMultipartUpload` reservation is classified explicitly and does not
  fall through an object-PUT-only decision path.
- DeleteBucket vs publish-ready `If-Match` PUT on an otherwise empty bucket
  proceeds to terminal `MarkBucketDeleting`, and the writer later fails without
  publishing.
- DeleteBucket does not return `BucketNotEmpty` for an `AdmissionOnly` stream
  PUT whose body/checksum later fails.
- DeleteBucket does not return `BucketNotEmpty` for an `AdmissionOnly`
  UploadPartCopy whose source read later fails.
- Command-owned reservations are still converged through pending metadata
  command replay before emptiness is trusted.
- An orphaned or otherwise unresolved durable reservation does not create an
  unbounded `DeleteBucket` request; it returns a retryable not-empty outcome and
  preserves the reservation for the owning operation.
- Reopen/retry cases preserve the classification decision and never allow a
  writer to publish after terminal `MarkBucketDeleting`.

## Non-Goals

- Do not change Phase 9.4.4's conservative correctness boundary as part of this
  optimization.
- Do not infer S3-visible state from authorization alone. Permissions are
  decided up front, but request input and object preconditions may still decide
  whether a metadata command is publishable.
- Do not make `DeleteBucket` wait for finalization, payload reclaim, read leases,
  completed async cleanup, or final row deletion; those remain finalizer work.

## Benchmarking Note

Recent UAT soak runs are intentionally heavy on short-lived buckets and cleanup.
Those runs produced very large bucket-delete finalizer queue/deduplication
counts even after object-payload reclaim was bounded per PG. This appears to be
test-workload amplification rather than a representative production request mix.

Do not tune bucket-delete finalizer scan/dequeue/deduplication policy solely from
that workload. Revisit per-PG finalizer limits, cooldowns, or scan pacing after
we have a more realistic workload benchmark that includes longer-lived buckets
and normal object access patterns.

## Phase 11 Soak Follow-up

Although the short-lived-bucket soak is not a production workload model, repeated
Phase 11 soak failures have clustered around `DeleteBucket`. Treat that as a
correctness and retry-semantics signal, not just benchmark noise.

The concrete Phase 11 close-out checklist now lives in
[`multihost-followup-plan.md`](multihost-followup-plan.md#21-close-deletebucket-attempt-convergence).
Revisit this reservation
classification optimization only after those correctness, retry-semantics, and
cleanup-stress items are stable.
