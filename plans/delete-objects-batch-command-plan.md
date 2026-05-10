# DeleteObjects Storage Batch Command Plan

Status: deferred standalone plan.

This plan tracks possible storage-layer batching for S3 `DeleteObjects`. It was
split out from Phase 7.6 of the multihost transition because Phase 7.6 recovered
the main metadata-command performance regression with current-path
optimisations. Batch delete is a semantic storage API change, not a small
hot-path optimisation, and should only be implemented if measurement shows it is
worth the added command-log and failure-mode complexity.

## Why This Is Separate

The current coordinator handles `DeleteObjects` as a loop over independent S3
object deletes. That is simple and matches the per-object AWS result model.

A storage-level batch command can only reduce cost when multiple requested
objects route to the same object metadata PG and can share command-log,
replica-fanout, digest, and transaction work. A request whose keys spread across
many PGs still needs one command stream per PG, so batching is not automatically
better.

Before implementation, the design needs to prove:

- common `DeleteObjects` workloads actually contain enough same-PG keys to
  amortise the extra complexity
- per-object AWS success/error reporting remains exact
- command-log replay is deterministic
- partial replica apply, abandoned command tombstones, and retry idempotence are
  clear
- reclaim/fence behavior remains correct for every object in the batch

## Measurement First

Add instrumentation before designing the command payload:

- record the number of keys per `DeleteObjects` request
- record the number of distinct object metadata PGs per request
- record the per-PG key group sizes
- record how often versioned and unversioned deletes appear in the same request
- record how often explicit version IDs, delete-marker insertion, object-lock
  failures, missing keys, and conditional failures appear in the same batch

Use those measurements to answer whether batching is likely to matter in
production. The useful threshold is not "large request"; it is "large enough
same-PG groups after routing."

## Candidate Shape

Keep the coordinator-facing S3 behavior per-object. The storage layer may group
only after auth, condition, and request parsing have produced typed delete
intents.

Potential shape:

- coordinator classifies each requested object into a `DeleteObjectIntent`
- storage groups intents by object metadata PG
- each PG group becomes one command-log entry only if the group has more than
  one object and all included intents can share one deterministic command
- singletons continue using the existing single-object command path

The batch command must be storage-shaped, not XML/request-shaped. It should carry
the exact per-object preconditions and target effects needed for deterministic
apply, including version IDs, delete-marker rows, stale payload/reclaim roots,
object-lock decisions, and expected current-object state where relevant.

## Failure Semantics

The command needs an explicit answer for each failure class:

- validation/auth failure before command construction: return the per-object S3
  error and do not log that object
- deterministic per-object condition failure during command construction: record
  the per-object result without including that object in the storage mutation
- transient replica failure after partial apply: keep the pending batch command
  and retry until acting-set convergence
- zero-apply transient failure: either abandon the batch command with a durable
  tombstone or keep the same pending command for retry; do not create log holes
- mixed object outcomes: apply the successful storage effects atomically for the
  command and preserve exact per-object response mapping for retry
- payload reclaim failure after metadata publish: leave retryable reclaim
  metadata/fences in the same shape as the single-object path

The batch command must not make one object's failure roll back another object's
AWS-visible success unless AWS does that for the corresponding request class.

## Command-Log Invariants

Any batch command must satisfy the same invariants as single-object commands:

- one PG command stream owns the mutation
- command bytes are canonical and CRC64 protected
- replay from log plus materialized state is deterministic
- digest-covered metadata tables are updated only by command apply or explicit
  documented exceptions
- abandoned commands are durable and retry-safe
- matching-pending retry cannot report success for an abandoned command
- stale, reordered, or modified command rows fail restart validation

Add encoding stability tests and replay/restart tests before enabling the API
from coordinator paths.

## Test Plan

Minimum tests before implementation is considered complete:

- same-PG multi-key unversioned delete produces the same per-object response as
  the current loop
- mixed-PG request groups by PG and preserves original response ordering
- missing keys, explicit version IDs, delete markers, object-lock failures, and
  conditional failures produce AWS-compatible per-object results
- partial apply on one replica retries and converges all objects in the batch
- zero-apply failure does not leave a command-log hole
- abandoned batch retry does not report object success
- reclaim payload fences remain closed until metadata delete/reclaim convergence
- restart validation accepts converged batch commands and rejects corrupted
  command bytes or divergent materialized state
- property/model trace comparing looped single-object deletes with grouped
  batch deletes for visible object state and reclaim roots

## Open Questions

- Should batching be limited to homogeneous delete classes first, for example
  only unversioned current-object deletes, before supporting mixed versioned
  cases?
- Should the coordinator expose batching only behind internal storage grouping,
  with no public API difference, or should storage expose an explicit
  `delete_objects` API?
- Is the main remaining production cost command fanout/transaction overhead, or
  is per-object auth/model work dominant enough that storage batching would not
  materially help?
- Should lifecycle expiry and bucket cleanup eventually share the same batch
  command shape, or is this specific to S3 `DeleteObjects`?

## Exit Criteria

- representative measurements show same-PG grouping is common enough to justify
  the work, or the plan is closed as not worth doing now
- command payload and retry semantics are documented before implementation
- focused correctness tests cover partial apply, abandoned commands, restart
  validation, reclaim fencing, and mixed per-object AWS results
- implementation, if done, preserves the existing single-object command path for
  singleton groups
