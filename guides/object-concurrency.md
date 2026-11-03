<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Object Concurrency Invariants

Status: active guidance. This guide describes the lock-order and concurrency
rules that current object paths are expected to follow.

This guide defines the lock and read/write rules for object operations in
`crates/server-core/src/coordinator.rs`.

## Scope

Applies to object data/metadata operations:

- `put_object`
- `complete_multipart_upload`
- `begin_stream_put` / `append_stream_segment` / `finalize_stream_put` / `abort_stream_put`
- `copy_object` (source read and destination write)
- `upload_part` / streamed `UploadPart` finalize
- `get_object`
- `head_object`
- `get_object_range`
- `delete_object`

Bucket metadata operations are in scope when they coordinate with object
metadata paths (for example, `DeleteBucket` emptiness checks).

## Required Invariants

1. Single-object multi-PG operations must lock PGs in global ascending PG ID order.
2. Streaming segment appends must never hold a lock while waiting for network input.
3. Streaming segment appends must validate session state under the metadata/session PG, then release PG locks before durable shard file IO.
4. Streaming finalize lock scope includes metadata/session PG (and any required secondary PGs), in global order.
5. Session state transitions are serialized via metadata-PG transactions over session rows (no ad-hoc in-memory correctness lock).
6. Latest-version reads must snapshot metadata and acquire any payload-generation lease before releasing the metadata PG lock.
7. Version ID allocation for versioned writes must be derived while holding the metadata PG lock and revalidated when relocking is required.
8. Full-object reads (`GET` and copy-source full reads) must verify reconstructed data CRC against stored ETag.
9. Streaming must not change external ETag/checksum semantics (headers/XML/validation).
10. Shard payload must become visible only after durable shard files exist; raw shard files may exist before metadata publication, but reads must treat metadata rows as the visibility boundary.
11. Mixed bucket+object metadata operations must lock PGs in global ascending PG
    ID order when more than one PG is held.
12. `CompleteMultipartUpload` must replicate its bucket-write dependency through
    the bucket-PG command stream before publishing the object-PG completion, via
    `AdvanceMultipartCompletionBarrier`.
13. The multipart completion barrier must be durable and storage-node owned;
    request paths must not use process-local multipart-completion mutexes for
    correctness.
14. The barrier may advance one fixed-size bucket scalar, but must not retain
    per-upload completion or abort history.
15. Exact completion replay metadata must publish atomically with the completed
    object version, and disappear only when that version is replaced or removed.
16. Copy-source authorization must return its exact metadata snapshot together
    with a broad payload-generation lease acquired on the actual storage nodes.
    The lease must remain visible across frontend/runtime-map replacement.
    Copy processing may replace it only after acquiring the shard-specific read
    lease, so source overwrite/delete reclaim never observes an unleased
    interval. Long-lived Unix generation-lease sessions consume the same
    aggregate configured RPC admission limit and the shared non-control budget.
    All lease acquisition stops one slot short of that shared budget, preserving
    narrow-lease to read-handle progress. Broad acquisition stops one additional
    slot earlier, so a new broad lease cannot consume the broad-to-narrow
    transition slot while existing broad leases acquire shard-scoped
    successors. Each broad lease is released after its successor is acquired.
    The remaining reserved capacity must allow short completion/control
    operations to progress.

## Required Coordinator APIs

### Local shard-file mutation exclusion

Each opened PG owns a process-local, per-`ShardKey` file-mutation registry shared
by `PgStore` and `SharedStorageNode`'s unlocked file facade. Publishers take
shared access; deletion takes exclusive access. Hold access through final-name
publication/unlink, its directory sync and file `WriteAck` production. Multiple
matching publishers retain the hard-link path's existing concurrency semantics;
unrelated keys and separate PGs use independent locks. Entries are removed when
the last active/waiting operation exits, including errors and unwinding.

The registry mutex only protects lock lookup/removal; never hold it across IO.
Low-level locked-store callers take the PG mutex **before** the file guard.
Unlocked file paths must not acquire a PG mutex while holding the file guard,
and no network request may run under either lock. Captured effect fences are
checked inside the guarded file operation, so waiting cannot turn an expired
request into a new publication.

This guard serializes local file effects, not allocation or cleanup authority.
It does not revoke a delayed request, establish a durable owner, or protect
payload after the file operation returns. Separate acknowledgement-row mutation
remains under its owning PG's SQL/lock boundary; file acknowledgement production
is not a cross-node acknowledgement-persistence lease. The durable-cleanup plan
must still establish finite write authority, reference protection and closure
at both physical participants before retiring an owner. Waiting for an admitted
filesystem operation is not a promise of progress when that IO is stalled.

Acknowledgement insertion and deletion already share the acknowledgement PG's
mutex. Exact batch registration holds it across `BEGIN IMMEDIATE`, per-row
captured-fence checks and commit/rollback; retained acknowledgement deletion
acquires the same mutex. Do not add a second acknowledgement lock merely to
mirror the unlocked file path. A delayed fresh batch must still revalidate its
captured fence after acquiring the PG, before any insertion. Conversely, a
deletion must wait for an admitted batch's transaction to finish. These are local
serialization guarantees, not permission to clean while future delegated
effects remain possible.

Retained cleanup of an explicit segment subset must validate the complete subset
before either file or acknowledgement deletion. Use the shared storage-owned
cleanup scope: each member binds its shard key, placement epoch/data PG, file
location and independently selected historical acknowledgement primary. A file
key mismatch must not fall through to acknowledgement deletion, and a valid
prefix must not be deleted before discovering a malformed later member. This
subject check does not establish reference absence, expiry, cross-node atomicity
or durable retry ownership; those remain separate cleanup obligations.

A Direct PUT payload handle can be consumed into `DirectPutPayloadCleanup` to
give up publication permanently while retaining request-local retry custody.
`finish()` attempts every captured file and acknowledgement, preserves the first
error, and releases the generation reservation only after all physical deletions
acknowledge success. A failed generation release also leaves the handle armed.
Successful completion disarms it; repeated `finish()` and Drop then do no work.
The consuming `discard_direct_object_payload` convenience performs one attempt
and returns failures, but retains no retry handle. Owner-side cleanup failures
take precedence over a crossed-route mismatch; a mismatch is returned only when
cleanup succeeds. Drop is still best effort.
Neither API provides crash-safe discovery or fences late delegated writes. The
durable owner/window work remains necessary, including when a failed cleanup
leaves a reservation but the frontend loses its handle.

The Direct PUT commit loop's common **caller-owned, pre-publication** cleanup
also deletes the captured payload subset before releasing its generation. It
reports either failure instead of treating best-effort cleanup as complete.
After successful cleanup, it disarms the admitted payload handle so Drop does
not repeat deletion or publish another generation-release command. Whenever the
authoritative snapshot rejects the request, delete its staging before the
budgeted generation-release path, and reuse that successful deletion when
finishing common cleanup. Do not select this convergence behavior from whether
the request previously observed a same-object pending command: the winner can
publish between an empty pending-slot observation and the snapshot load. Retry
cleanup contention/recovery handoffs within the request budget before returning
the evaluated condition; genuine cleanup failures still take precedence, and
budget exhaustion is not broadly remapped to a conditional conflict.
Failed cleanup leaves the payload armed for its
existing best-effort Drop attempt; this is not a durable handoff. Accepted-command,
abandonment and post-apply cleanup have separate ownership rules and are not
covered by this pre-publication completion claim.

Direct PUT preparation distinguishes a fresh command carrying its evaluated
snapshot from an exact matching pending command. Only fresh preparation installs
a command; failure of its post-registration payload validation uses the same
caller-owned cleanup described above. Matching-pending preparation instead restores
acknowledgements under the pending command's authority. Restoration or validation
failure must preserve command-owned payload and generation reservations for recovery,
not invoke caller cleanup or re-arm the consumed payload handle.

If a matching logical reservation has a different pending physical payload, first
disarm generic caller cleanup, then delete only the exact caller-only shard subset.
Overlapping command-owned files/acknowledgements and the logical reservation must
survive. Attempt every caller-only participant and report cleanup failure before
the payload-mismatch diagnostic; successful cleanup still returns the mismatch.
Do not re-arm Drop on failure: it cannot distinguish this mixed ownership. This
consuming error path does not provide retry custody or durable cleanup discovery.

After authoritative abandonment, Direct PUT's foreground terminal/reinspection
exits also complete retained file/acknowledgement cleanup before their standalone
generation release. Physical or release failures take precedence over the original
terminal diagnostic. This is not permission to clean accepted or ambiguous commands:
their publication/recovery ownership remains unchanged. Nor does it supply durable
cleanup discovery after the abandoned slot has gone, or change generation cleanup
that may already have been performed by a recovery derivative.

Before a full payload handle exists, shard writing distinguishes a write error
from failure to roll back its acknowledged prefix. Preserve that distinction
through Direct PUT's internal write result: incomplete rollback disarms the
generation guard's release, retaining the reservation instead of releasing it
on Drop. After successful rollback, Direct PUT explicitly attempts generation
release: a release failure takes precedence over the original write diagnostic;
successful release preserves that diagnostic. The guard disarms before the
attempt, so Drop cannot silently retry a reported release failure. Unsolicited
Drop remains best effort. Streaming still owns its generation through the durable
session and reports rollback failures through that workflow. This only covers
acknowledged-prefix rollback, not cleanup discovery after frontend loss or the
revocation of delayed/ambiguous unacknowledged writes.

For stream append, failures after shard writing also perform fallible cleanup:
lease-maintenance failure and append-publication rejection report physical
cleanup failure before the original diagnostic. Attempt all file and
acknowledgement participants even when one fails. Once concurrent publication
is possible, retain the existing serialized reference inspection before deletion;
a reference preserves the payload, and failed inspection preserves it and returns
the inspection error. Each failure is reported once: reference-inspection errors
at the wrapper, and deletion errors at the physical participant boundary. This
does not release the stream's generation reservation, make cleanup durable after
session removal, or revoke delayed writes. Terminal recovery's separate
best-effort cleanup semantics are unchanged.

Retained stream abort also attempts every file/acknowledgement participant and
every staged segment before returning its first cleanup error. Once abort apply
has succeeded on all replicas, failure to release the retained PUT creation proof
must not skip independent physical cleanup. Attempt reservation release first
and payload cleanup second, preserving the first error if both fail. It must not
finish and remove its pending abort until both obligations succeed. An explicit
retained retry reuses that exact abort and idempotently releases its exact proof
and deletes any remaining effects; unrelated reservations remain untouched.
This is not a guarantee for generic recovery, whose separate best-effort cleanup
and pending-slot retirement still require durable owner/window integration.

Durable object-payload reclaim follows the same independent-participant rule:
attempt every file and acknowledgement in every recorded segment/part, preserving
the first deletion error. Do not install the reclaim-root removal command after
any deletion error. The exact durable root remains retryable and the existing
payload-lease fence stays closed after deletion has started. Retry repeats
idempotent deletion, including already-absent participants. This does not add
historical placement authority or solve delayed staging writes; it preserves the
existing reclaim route and claim protocol.

Reopening a runtime discards the queue hint and all process-local completion
observations. Rediscover retained reclaim roots from durable metadata and repeat
every recorded file/acknowledgement deletion, even when inspection finds every
effect already absent after an unconfirmed completion. Namespace absence does
not replace participant confirmation. Retire the root only after the complete
retry succeeds; an unrelated live payload on the same PG must remain unchanged.

Durable discovery must also progress past a root that remains leased, deferred,
or repeatedly fails cleanup. PG-wide discovery rotates in `(bucket, key,
generation)` order using a process-local cursor, wrapping after the last root.
Each call returns at most one root and performs at most two metadata queries.
The cursor is only a scheduling hint: it grants no deletion authority, is not
persisted or replicated, and resets on reopen. A lost discovery response skips
that root only until wraparound; it cannot retire the root. Bucket-specific
draining retains its existing first-root behavior. Workers still revalidate the
exact root, leases and claim before effects and request another scan pass after
any nonempty result.

### Coordinator helpers

For object read/write/copy paths, use these helpers instead of open-coded lock
orchestration:

- `lock_object_pgs_for_read(...)`
- `lock_object_pgs_for_write(...)`

Use `TwoPgGuards::meta()` and `TwoPgGuards::shard()` for write-side access.
Read-side helpers snapshot metadata under the metadata PG lock and must not
open-code read-side shard relocking.

For streaming write paths, use dedicated coordinator streaming APIs and shared
lock helpers for:

- metadata/session PG validation, unlocked shard file durability, then ordered shard/segment publication
- metadata/session PG finalize lock orchestration
- transactional finalize that commits metadata and removes staging rows together

For multipart completion publication, use:

- the storage-cluster completion barrier, which publishes an
  `AdvanceMultipartCompletionBarrier` bucket-PG metadata command under the
  current completion reservation before constructing the object-PG completion
  command; a barrier left pending by an earlier reservation is drained before
  a fresh barrier is allocated
- the storage-node-owned metadata command serialization boundary for both the
  bucket-PG barrier command and object-PG completion command
- object-version-scoped replay metadata; never add terminal upload history or
  a pruning path

Do not open-code this pattern in object paths:

- `get_pg(meta)` then `get_pg(shard)`
- manual lock-order branching
- ad-hoc relock/retry loops
- open-coded durable bucket write reservation acquire/release pairs in migrated
  production paths; use a shared storage wrapper that owns acquire, snapshot
  load, protected action, and release
- lock-holding network reads in streaming paths
- shard-file visibility that bypasses metadata publication
- shard-file writes while holding a PG mutex unless the code is intentionally changing the write visibility model
- allocating multipart completion order outside the bucket-PG metadata command
  stream
- using process-local multipart-completion mutexes as a correctness boundary
- using `completed_at` timestamps or upload IDs as the authoritative prune order

## Why These Rules Exist

- Without lock ordering, multi-PG operations can deadlock.
- Without short append lock windows, slow clients or durable shard IO can block unrelated work.
- Without metadata-locked snapshotting plus payload leases, reads can race with
  overwrite/delete reclaim and observe mixed snapshots or missing payload.
- Without command-owned version allocation, concurrent versioned writes can
  choose the same `version_id`.
- Without a durable-before-visible rule, reads can observe metadata that points
  at shard files that were never fully committed, or bypass cleanup rules by
  treating orphan files as live payload.
- Without transactional finalize cleanup, stale staging rows can leak or race
  with retries/recovery.
- Without the durable bucket-PG completion barrier, replicated object-PG commit
  recovery can observe a half-published bucket-write dependency.
- Without storage-node-owned command serialization, multiple frontend processes
  can observe each other's half-complete barrier/object-publication windows.
- Without shared reservation wrappers and explicit error-path review, fallible
  operations can leak bucket write reservations or release the wrong error
  shape.

## PR Checklist (Object Paths)

1. Does this change touch object read/write/delete/copy flow?
2. If yes, does it use:
   - `lock_object_pgs_for_read` / `lock_object_pgs_for_write` for non-streaming
     object paths?
   - shared streaming lock helpers/APIs for streamed append/finalize paths?
3. Are lock-order assumptions unchanged and explicit (including metadata/session PG)?
4. For streaming append: metadata/session validation before durable file IO, no network wait under lock, and shard/segment metadata publication only after shard durability succeeds?
5. For streaming finalize: metadata commit + staging-row removal in one
   transaction?
6. Are CRC checks preserved for full-object reads?
7. Are external ETag/checksum semantics unchanged?
8. For streamed writes, is the visibility boundary still metadata publication rather than raw shard-file presence?
9. For bucket+object mixed operations, is lock order explicit and ascending by PG ID?
10. For `CompleteMultipartUpload`, is the bucket-write dependency replicated
    through a fresh `AdvanceMultipartCompletionBarrier` under the current
    reservation before the object-PG completion command is built?
11. Is exact replay metadata still atomically scoped to the completed object
    version, with no per-upload terminal history or cleanup path?
12. If this change touches reservations, drains, guards, or storage critical sections:
   - is acquisition and release owned by a shared helper rather than open-coded?
   - does every fallible path (`?`, early return, closure error) still release?
   - does the protected action run inside the intended reservation/critical-section lifetime?
   - if both the action and release fail, does the action error still win?
   - did you grep for sibling helpers with the same acquire/release pattern?
   - did you add a regression for release-on-error and, where relevant, “blocked action holds off conflicting work” behavior?
13. Did you run:
   - `cargo clippy --workspace -- -D warnings`
   - `cargo test -p server-http --lib`
   - `cargo test -p s3-tests`
   - atomic stress loop (`test_atomic_dual_write`)
