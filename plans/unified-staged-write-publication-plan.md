# Unified Staged Write Publication Plan

Status: Proposed

## Context

Small `PutObject` requests use a request-owned, single-segment fast path, while larger requests
promote to a durable stream session. Both ultimately publish the same standard segmented object
layout and already use `CommitDirectPutObject`, but snapshot loading, command construction,
retry, cleanup, and coordinator preparation remain split between direct and streamed paths.

`UploadPart` currently creates a durable stream session before reading the body, even when the
part fits in one 8 MiB internal segment. Its terminal command, `CommitStreamPart`, is coupled to
that session. Common multipart part sizes of 5--8 MiB therefore pay create, append, and finalize
metadata cycles for one segment.

The workload rationale differs between the two operations:

- PutObject was correctly prioritized because object workloads can contain very large numbers
  of tiny objects, including 4 KiB objects where fixed session overhead can dominate payload IO.
- UploadPart payload IO is larger, so the relative saving per byte is lower, but exact 8 MiB
  parts are common and the avoidable session lifecycle repeats for every part in an upload.

This plan preserves the existing small-PutObject optimization. Multipart work is justified by
the frequency and repetition of single-segment parts, not by assuming their absolute latency
profile matches tiny PutObject.

The optimization and durable streaming lifecycle have genuinely different staging ownership.
They should not have different publication semantics.

## Goal

Keep request-owned single-segment staging and durable streamed staging, but converge them before
terminal publication:

```text
request-owned segment ----+
                          +--> one terminal publisher --> committed segment manifest
durable stream session ---+
```

There should be one terminal publication implementation for a standard object and one for a
multipart part. PutObject and UploadPart remain different S3 mutations; this plan does not merge
their metadata commands with each other.

## Design

### Opaque staged payloads

Introduce storage-owned, non-cloneable staged payload capabilities:

- `StagedStandardObjectPayload`
- `StagedMultipartPartPayload`

Each privately represents either request-owned shards or an exact durable stream session. It
binds the bucket, key, placement epoch, generation/session identity, segment manifest, bucket
write reservation, cleanup authority, and, for parts, upload ID and part number.

Request-owned capabilities delete unowned shards and release reservations on failure. Durable
session capabilities preserve the existing abort, heartbeat, sweeper, recovery, and retained
cleanup behavior. Callers cannot extract shard identities or convert between ownership modes.

The capability is only the live frontend representation. Terminal publication must not depend
on that in-memory type surviving. For `StagedMultipartPartPayload` only, before the first
request-owned shard write, storage binds an immutable admitted-route effect fence, derives exact
retained cleanup authority, and durably installs the multipart cleanup root described below. The
capability then accumulates the exact shard-write acknowledgements while that root remains the
crash-safe cleanup owner.

`StagedStandardObjectPayload` preserves the existing request-owned standard-object protocol:
exact shard-write acknowledgements, reference-checked cleanup after ambiguous installation, and
ownership transfer to `CommitStandardObject`. Unifying its terminal publisher must not add a
cleanup-root command or another metadata cycle to small PutObject. Replacing that ownership
protocol would require a separate measured design change; it is not implied by the multipart
root introduced here.

### One standard-object publisher

Rename/generalize `CommitDirectPutObject` as `CommitStandardObject`. Both staging modes must use
one implementation for:

- fresh snapshot loading and stale-snapshot retry
- conditional evaluation
- version and write-sequence reservation
- owner, ACL, tags, metadata, encryption, checksum, and object-lock publication
- overwrite and stale-payload reclaim construction
- pending-command installation, apply, idempotent convergence, and reservation release

PutObject, streamed POST Object, and copy destinations that publish the standard segmented
layout must reach this publisher. Their authorization and body acquisition may remain distinct.

#### Typed terminal-publisher class

This convergence also replaces the current split publisher classification: direct publication
is `SnapshotSensitive`, while stream finalization is `TerminalSessionRetry`. Introduce one sealed
`StagedPayloadTerminal` publisher class for terminal standard-object publication and use the same
class for terminal multipart-part publication. Its exhaustive installer result is:

- `Installed` or `AppliedExact`: the exact command owns the payload and convergence may finish;
- `ExactPendingVisible`: preserve the matching command and return to the owner so it can finish
  that command; never drain it as unrelated contention;
- `ContenderDrained`: drain exactly the one observed unrelated contender and return to the owner;
- `SnapshotStale`: return without consuming staging provenance so the owner can reload and
  rebuild from a fresh snapshot; and
- `AuthorityExpired` or `BudgetExhausted`: create or resume provenance-specific cleanup without
  installing fresh publication work.

Every owner-loop iteration rechecks its immutable route deadline and shared work budget before
loading another snapshot or handling another contender. Matching-command detection compares the
complete command, staging provenance, reservation proof, and response identity. The class owns
only contention/installation semantics; request-owned versus stream-session cleanup remains a
closed match on the provenance enum, so neither mode can accidentally use the other's cleanup.

The implementation must update the authoritative publisher registry, sealed token definitions,
boundary checker/fixtures, publisher inventory, and `guides/metadata-command-stream.md`. The
guide must replace both old rows with the new class and record the exhaustive outcomes,
one-contender rule, matching-command visibility, budget/deadline rechecks, and cleanup split.

#### Cleanup-root publisher class

Root creation is a separate registered publisher, not an incidental write inside HTTP code. Add
registry ID `CreateStagedMultipartPayloadCleanupRoot`, command kind of the same name, and a sealed
`SnapshotSensitive` publisher token. It revalidates the admitted multipart route, in-progress MPU,
exact part subject, bucket incarnation, reservation proof, and immutable effect fence before
construction and again at pending-slot installation.

Its exhaustive outcomes are:

- an exact installed/applied command converges creation on every required replica and returns the
  capability only after the root is durably visible across the acting set;
- an exact visible pending command is preserved and finished rather than drained;
- one unrelated contender is drained before returning to the owner loop for budget, deadline,
  and MPU-snapshot revalidation;
- a stale MPU or reservation snapshot may abandon the command only when the primary
  authoritatively proves publication-start was never recorded, no replica accepted or logged the
  command, and every dispatch outcome is typed `NotSent` or otherwise definitive; in that case no
  replica root exists and only the unstarted pending intent is cleared; and
- authority expiry or budget exhaustion before installation creates no root and writes no shard;
  once publication-start is recorded, any replica may have applied, or dispatch is ambiguous,
  exact root creation is irrevocable and recovery must converge it on every replica.

After irrevocable root creation converges, an expired or failed frontend publishes the separate
cleanup-ready transition described below, even when no shard was written. It never removes a root
as part of root-command abandonment. This uses the repository-wide publication boundary:
authoritatively unstarted work may be abandoned, while publication-start, any accepted apply, or
`MayHaveApplied` requires exact-command convergence.

Mutable MPU eligibility is therefore a construction/install precondition, not a reason for
replicas to reject an irrevocable exact root command. Once publication starts, apply validates the
self-contained command subject, reservation, and local command ordering, creates the root, and
lets the maintenance transition make it cleanup-ready if the MPU is no longer writable.

The command is idempotent only for the complete root subject, intended manifest, cleanup cutoff,
bucket-write reservation proof, and command identity. The registry, checker fixtures, and
metadata-command guide must inventory this publisher and reject raw or differently classified
root creation.

### One multipart-part publisher

Rename/generalize `CommitStreamPart` as `CommitMultipartPart`. Both staging modes must use one
implementation for:

- exact MPU, bucket, key, upload ID, and part-number binding
- in-progress upload revalidation
- upload checksum and encryption configuration
- part ETag/checksum/size construction
- replacement of an existing part and displaced-segment cleanup
- pending-command retry, apply, convergence, and reservation release
- compatibility with concurrent multipart completion and abort

The terminal command is built at the object-PG primary from the staged capability and a fresh
durable snapshot. Recovery validates the same complete invariants; it must not rely on the
frontend having used the intended path.

#### Durable staging provenance

`CommitMultipartPart` must carry a durable, mutually exclusive staging-provenance enum rather
than assuming that every part has a stream session:

- `RequestOwned` contains a unique staging identity, the exact committed part-segment manifest,
  and a self-contained immutable payload witness derived from the exact durable shard-write
  acknowledgements. It contains no stream-session identity or staged stream rows.
- `StreamSession` contains the exact session identity and snapshot plus the staged stream rows
  from which the committed part manifest is derived. It cannot contain request-owned write
  acknowledgements.

The enum is encoded in the metadata command so pending-command replay and restart recovery have
the same provenance as the foreground publisher. Central fanout, apply, recovery, idempotence,
and abandonment validation must reject mixed modes, a missing required record, extra records
from the other mode, crossed upload/part/session/staging identities, and a manifest that is not
an exact derivation of the selected provenance.

For `RequestOwned`, placed IO validates every required data-PG shard acknowledgement before
object-PG pending-slot installation and constructs the immutable witness. The command checksum
binds that witness, the complete manifest, staging identity, placement epochs, and part subject.
Object-PG replica apply performs no live data-PG reads: it validates only the command's internal
provenance/witness consistency and its local object-PG multipart state, then publishes the encoded
manifest. This keeps replica apply deterministic and independent of data-PG availability.
`StreamSession` retains the current exact local session-and-staged-row validation.

This is an incompatible metadata-command representation change. The implementation slice must
advance the exact-current metadata-command encoding version, update all containing format
fixtures and `plans/storage-upgrade-versioning-plan.md`, and add owner-local rejection fixtures
for the immediately previous command version and every invalid provenance combination. There is
no compatibility decoder because the project does not support mixed or historical formats yet.

#### Durable cleanup owner and ownership transfer

The object metadata PG owns request-owned multipart cleanup. Add a command-owned
`staged_multipart_payload_cleanup_roots` record keyed by bucket, key, upload ID, part number, and
staging identity. Before any shard write, a fenced metadata command creates the root with the
intended manifest, shard identities, placement epochs, originating object-PG command epoch,
cleanup cutoff, exact bucket-write reservation proof and stable proof identity, bucket-incarnation
generations, reservation-release responsibility, and worker claim state. The shard identities and
placement are known after encoding and placement selection but before placed IO; the later
terminal command adds the validated acknowledgement witness. The root cannot be created for
stream-session provenance.

The replicated root binds a durable, session-owned staging-lease identity: the staging identity,
an unforgeable owner token, lease generation, and absolute wall-clock expiry. The object-PG
primary owns the corresponding active lease record under the same command-serialization lock.
The live capability owns that lease but cannot extend it beyond the request's immutable
publication cutoff. Closing the capability to further writes either installs the exact terminal
pending command or atomically relinquishes the primary lease and wakes the expiry adopter. Lease
relinquishment never makes cleanup eligible early: the root must remain in non-cleanable
`Staging` or `AbandonedAwaitingCutoff` until its immutable cutoff has passed. This deliberately
lets any already-dispatched request arrive before the cutoff, acquire its shard-local lease, and
leave payload still owned by the root.

Every placed shard write carries the exact lease identity and cutoff and must have been admitted
while the primary lease was active. The data-PG/storage-node effect holds a shard-local
deletion-exclusion lease from before file publication until the durable shard acknowledgement is
committed or the write is rejected, and revalidates the absolute conservative wall-clock cutoff
bound to its local monotonic clock beside that acknowledgement. A write delayed past expiry
cannot publish a late acknowledgement.

The root has a closed state machine:

- `Staging { cleanup_cutoff, staging_lease }`: shard writes may proceed while the exact request
  authority and staging lease remain live;
- `AbandonedAwaitingCutoff { cleanup_cutoff, closed_lease_generation }`: terminal publication and
  new frontend dispatch are permanently closed, but already-authorized shard requests may still
  arrive before the immutable cutoff, so cleanup cannot yet be claimed;
- `CleanupReady`: publication is permanently forbidden and cleanup may be claimed;
- `CleanupClaimed { claim_id, owner, lease_deadline }`: one worker owns reference checking and
  shard deletion; and
- `PayloadDeletedAwaitingReservationRelease`: shard deletion is complete, but the root remains
  the durable owner of reservation release until the bucket PG confirms the exact stable proof
  has been released.

Every root-state transition is metadata-command-owned within the object-PG serialization
boundary; there is no independent row mutator. A storage-owned expiry adopter scans `Staging` and
`AbandonedAwaitingCutoff` roots whose durable wall-clock cleanup cutoff has passed. Its command is
admitted only after the primary atomically evaluates that the cutoff has passed, proves the exact
staging lease absent, closed, or expired, and checks the bucket/object/part subject plus absence of
an exact terminal pending command or committed manifest. The installed command binds the exact
source state, lease generation, and cutoff. Replica apply compares those immutable values and the
exact local root without reading its local wall or monotonic clock, then changes the root to
`CleanupReady`. A frontend failure may relinquish its lease and wake adoption, but retained
cleanup authority cannot install this transition before the immutable cutoff. The scan itself
conveys no mutation authority; stale candidates are revalidated before command installation and
structurally at local apply.

Terminal pending-slot installation and cleanup-claim command installation are mutually exclusive
primary operations serialized by the same object-PG command stream. Terminal installation
atomically requires an unexpired `Staging` root, complete acknowledgement witness, exact
reservation proof, and matching active staging lease; it closes that primary lease while
installing the exact pending command. It does not change replicated root state: witness and other
replicas correctly retain the exact `Staging` root and bound lease identity until that command
applies. Cleanup-claim
installation atomically requires `CleanupReady` and rechecks that neither an exact pending command
nor a committed durable manifest references the payload, and that no staging lease is live. Its
apply changes each exact local root to `CleanupClaimed`. Terminal installation rejects
`AbandonedAwaitingCutoff`, `CleanupReady`, `CleanupClaimed`, and
`PayloadDeletedAwaitingReservationRelease`; the worker never relies on a check-then-delete window.

Successful `CommitMultipartPart` apply accepts and atomically consumes the exact `Staging` root on
every replica while publishing the part in that replica's object-PG transaction. The primary has
the additional exact pending-slot ownership evidence, but root validation and mutation are
identical on primary and witnesses. Terminal convergence assumes the command's encoded
responsibility for releasing the exact reservation proof. Abandonment atomically records its
tombstone and changes the exact local `Staging` root to `AbandonedAwaitingCutoff`, preserving the
immutable cutoff and closed lease generation, before that replica reports the command abandoned.
It never makes the root cleanup-ready. The cluster finisher may clear the primary pending slot
only after every required replica has durably applied either successful publication or that
abandonment transaction. Thus a crash before terminal apply leaves the pending command and
replicated roots, while a crash afterward leaves either the committed part or non-cleanable
abandoned roots until the separate primary-only cutoff transition applies.

Ambiguous installation or apply outcomes reconcile the exact command ID/checksum, committed part
generation/manifest, and cleanup-root identity on the authoritative acting set. Exact pending or
committed evidence resumes command convergence. Otherwise the already-durable root remains the
owner; no new handoff has to be created after authority or connectivity has been lost. Cleanup may
never delete a shard while an exact pending command, committed part, or other durable manifest
references it.

The object-payload reclaim worker operates only on `CleanupClaimed` roots and uses exact retained
data-PG routes. Before deleting each shard it acquires the same shard-local deletion-exclusion
domain used by placed writes and rechecks that no live staging lease or durable acknowledgement
can still appear. After all required deletion acknowledgements,
an object-PG command moves the root to `PayloadDeletedAwaitingReservationRelease`. The worker then
releases the exact bucket-write reservation through the bucket-PG command stream. Only durable
evidence that the stable proof identity has been released permits a final object-PG command to
remove the root. A crash at any cross-PG boundary leaves the root and release responsibility
retryable. Claim expiry returns `CleanupClaimed` to `CleanupReady`; it never returns any cleanup
state to `Staging`.

The durable route-history reference summary must include the root's exact object-PG command
route, every `(data_pg_id, placement_epoch)` in its intended manifest, and the exact
`(bucket_pg_id, reservation_proof.cluster_epoch)` needed to release the reservation. Each tuple is
retained independently until its corresponding cleanup or release step is durably confirmed;
another PG from the same epoch is not a substitute. Two or more route transitions therefore
cannot prune cleanup or reservation-release authority. On successful terminal apply, removal of
the root transfers the bucket-route reference to the still-durable terminal pending command,
whose encoded reservation proof retains it until release is confirmed and the slot is cleared.
Adding the root requires coordinated
exact-current bumps for the PG SQLite schema, metadata-command encoding, canonical PG-state
digest, metadata checkpoint, route-history reference summary, and any storage RPC frames that
carry it. Those versions and their old-version/malformed-record rejection fixtures must be
recorded in `plans/storage-upgrade-versioning-plan.md` in the implementation commit. The baseline
and final benchmarks must include this root-creation command; skipping a stream session is
worthwhile only if the complete protocol still reduces cost.

#### Cleanup-maintenance publisher family

All post-creation transitions belong to one sealed `StagedPayloadCleanupMaintenance` publisher
family. The authoritative registry contains these distinct IDs and command kinds:

| Registry ID | Exact transition | Required apply precondition |
| --- | --- | --- |
| `MarkStagedMultipartPayloadCleanupReady` | `Staging or AbandonedAwaitingCutoff -> CleanupReady` | Primary proves the immutable cutoff passed and the exact lease generation is relinquished, closed, or expired; no exact terminal pending/committed evidence. |
| `ClaimStagedMultipartPayloadCleanup` | `CleanupReady -> CleanupClaimed` | Exact unclaimed root; no live staging lease or terminal/committed reference. |
| `ReleaseStagedMultipartPayloadCleanupClaim` | `CleanupClaimed -> CleanupReady` | Exact claim identity and explicit release or durable claim expiry. |
| `RecordStagedMultipartPayloadDeleted` | `CleanupClaimed -> PayloadDeletedAwaitingReservationRelease` | Exact claim plus every required shard-deletion acknowledgement. |
| `FinalizeStagedMultipartPayloadCleanup` | remove release-pending root | Durable bucket-PG evidence that the exact stable reservation proof was released. |

This family is required cleanup, not request publication. It derives authority only from the
exact durable root and retained routes and cannot create a root, write a shard, install a terminal
part, or widen the subject. Each publisher has the same exhaustive contention/recovery outcomes:

- an exact installed, applied, or already-target-state command converges idempotently;
- an exact visible pending command is preserved and finished;
- one unrelated contender is drained before returning to the worker, which rechecks root state,
  retained authority, claim lease, and its finite work budget;
- an incompatible or crossed state/claim/proof fails closed without mutation, while terminal or
  committed evidence routes ownership back to exact terminal convergence; and
- budget exhaustion leaves the durable root queued, while publication-start, any accepted apply,
  or an ambiguous dispatch forces exact maintenance-command recovery rather than abandonment.

Registry tokens, typed installers, central apply/recovery validation, boundary-checker fixtures,
and `guides/metadata-command-stream.md` must enumerate every member. No generic root-state mutator
or unclassified maintenance wrapper remains callable from production code.

All time-based maintenance decisions are primary-only installation decisions. In particular, the
primary evaluates the staging cutoff for `MarkStagedMultipartPayloadCleanupReady` and the claim
lease deadline for `ReleaseStagedMultipartPayloadCleanupClaim`. Each command binds the exact prior
state—including whether it is ordinary staging or terminal abandonment—the lease/claim generation,
and absolute cutoff or deadline used for that decision. Replica
apply validates equality with its local durable state and command ordering only; it never consults
its local wall or monotonic clock. Irrevocable apply therefore cannot diverge because replica
clocks straddle an expiry boundary.

### Immutable admission fencing

Reading or buffering a body does not extend mutation authority. Both staged capabilities carry
the immutable effect deadline captured from request admission. The admitted fence is revalidated
at the storage effect boundary immediately before each of the following:

- reservation or generation allocation;
- multipart request-owned cleanup-root creation;
- stream-session creation;
- every shard write and every stream-segment append;
- terminal pending-slot installation; and
- any retry that would create fresh durable work.

Retained cleanup authority is derived before the first shard write, but it authorizes only
reference-checked state transition and cleanup of the already-created exact root, not publication
after expiry. It cannot be converted back into publication authority or widened to another
payload.

Local publication calls enforce the conservatively bound monotonic deadline. RPC requests contain
no monotonic timestamp and do not carry a remaining duration. Unix and TLS/TCP requests carry the
absolute conservative wall-clock upper bound captured by the sender. The receiver shortens that
upper bound for inter-host skew, intersects it with the storage node's route deadline, and binds
the resulting cutoff to its own monotonic clock before revalidating beside the durable mutation.
Network transit can therefore consume authority but can never restart or extend it. Deterministic
tests expire the captured admission inside each pre-effect hook while renewing the raw same-epoch
route and prove that no reservation, session, shard, append, command ID, or pending slot is created
after expiry; only exact retained cleanup may continue.

### Lazy UploadPart promotion

Split current UploadPart preparation into non-mutating authorization/configuration loading and
durable session creation. Buffer the first internal segment before choosing a staging mode:

1. authenticate and capture the admitted route at ingress
2. load an opaque authorized UploadPart context, including checksum and encryption requirements
3. incrementally decode the body and update signature/checksum state while filling at most one
   internal segment plus bounded decoder/lookahead state
4. poll for EOF after the threshold; if EOF is proven at or below 8 MiB, perform terminal
   checksum/trailer validation, revalidate admission, create the cleanup root, write
   request-owned staging, and publish directly
5. if decoded payload beyond 8 MiB is observed, revalidate admission, create the durable session,
   transfer the buffered bytes plus incremental decoder/signature/checksum state, append the
   first segment, and continue without restarting validation or hashing

Exact 8 MiB and zero-byte parts remain eligible for request-owned staging. S3's 5 MiB minimum is
enforced at multipart completion for non-final selected parts, not while UploadPart is received.

Final publication always revalidates that the MPU is still in progress. The ingress snapshot
does not authorize mutation after a concurrent completion or abort.

Whole-body checksums, final aws-chunked signatures, and checksum trailers are validated only at
EOF. Promotion transfers the live incremental state; it does not treat the first segment as a
complete body. The decoder must split an oversized incoming frame rather than retaining an
unbounded overflow frame.

### Segment-size and staging-boundary benchmarks

The internal segment size and the request-owned-to-session promotion boundary are storage design
parameters, not S3 multipart thresholds. Benchmark them independently of the client's choice to
use `PutObject` or multipart APIs. In particular, Warp's default 10 MiB object is one S3
`PutObject` with its current MinIO Go client, but crosses Argmin's current 8 MiB internal boundary.
AWS transfer helpers also vary materially: common automatic multipart thresholds include 5, 8,
16, and 100 MiB. No one client default is sufficient evidence for Argmin's boundary.

Establish one reproducible benchmark harness before changing the boundary. It must support the
current 8 MiB build and candidate 16 MiB build, and where practical a 4 MiB sensitivity build.
The build variant changes the complete internal segment-size contract consistently; it must not
fake a larger request-owned object while retaining 8 MiB manifest segments. Record the exact
binary revision, segment size, EC profile, PG count, node count, frontend count, storage medium,
CPU allocation, client version, and client flags with every result.

Run at least this payload-size matrix:

- 4 KiB and 64 KiB, to retain tiny-object fixed-cost baselines;
- 1 MiB, 4 MiB, and 5 MiB, covering representative sub-segment objects and the S3 minimum
  non-final multipart-part size;
- one byte below, exactly at, and one byte above each tested internal segment boundary;
- 10 MiB, covering Warp's default object size;
- 16 MiB minus one, exactly 16 MiB, and 16 MiB plus one; and
- 32 MiB, 64 MiB, and a representative large streamed object, so a larger segment does not hide
  regressions in sustained streaming.

For every relevant size, benchmark:

- ordinary known-length `PutObject`, both automatic staging and test-only forced
  request-owned/durable-session staging where the representation permits both;
- aws-chunked `PutObject` with trailing checksum, so decoding and digest-state transfer are
  included rather than measured separately;
- isolated `UploadPart`, followed by completion, using one part and repeated equal-sized parts;
- complete multipart uploads large enough to show cumulative create/append/finalize amplification;
- overwrite and conditional-PUT variants, because fresh-object throughput alone omits terminal
  snapshot and displaced-payload work; and
- cleanup after the workload, including aborts and overwrites, rather than stopping once writes
  have returned success.

Run a single-client latency case, Warp's default concurrency, and a higher concurrency that
saturates but does not intentionally overload the cluster. Measure both an embedded/local setup
and the standard four-host 2+1 multihost setup. Use a fixed warm-up followed by a long steady-state
window, repeat each cell enough to report dispersion, and randomize or rotate cell ordering so
thermal state, cache warmth, and accumulated cleanup work do not consistently favor one size.
Do not compare runs performed during unrelated soak or model-test load.

Capture more than aggregate throughput:

- successful operations per second, bytes per second, and p50/p95/p99/max foreground latency;
- HTTP 4xx/5xx counts, SDK retries, Argmin `SlowDown`, and incomplete/aborted uploads;
- frontend and storage CPU, peak and steady RSS, allocator pressure, and per-request buffered
  bytes at each tested concurrency;
- metadata commands, pending-slot attempts, storage RPCs, command-log/WAL bytes and fsyncs per
  completed object or part;
- payload shard writes and bytes, stream-session creates/appends/finalizes, direct publications,
  and promotion counts;
- route-heartbeat/lease stability, queue depths, worker saturation, and recovery or cleanup work
  generated during and after the run; and
- time and request amplification required to return all background queues and durable cleanup
  roots to their pre-run baseline.

Use the results to choose the segment size and promotion boundary separately where the design
allows that separation. Retain 8 MiB unless another value shows a material, repeatable improvement
in the intended workload mix without unacceptable memory growth, tail-latency regression,
metadata amplification, lease instability, or cleanup debt. A 16 MiB boundary is not justified
solely because MinIO Go uses it, just as 8 MiB is not justified solely by AWS CLI or Boto3. Record
the final decision, raw benchmark commands, summarized results, and rejected alternatives in this
plan before implementing a boundary change.

## Implementation Slices

1. **Baseline and inventory**
   - record command/RPC counts and latency for 5 MiB, exact 8 MiB, and representative larger
     UploadPart sizes
   - measure complete multipart uploads with repeated 8 MiB parts, not only isolated part latency
   - retain 4 KiB and representative sub-segment PutObject benchmarks as regression baselines
   - inventory semantic logic duplicated between direct and streamed standard-object publication
   - inventory every publisher of standard-object and multipart-part segment manifests
2. **Standard-object convergence**
   - introduce the opaque staged standard-object capability
   - introduce the sealed `StagedPayloadTerminal` publisher class and exhaustive installer result
   - move both current PutObject finalizers behind `CommitStandardObject`
   - migrate POST/copy standard-object publishers and remove staging-specific semantic branches
3. **Multipart-part convergence**
   - introduce the opaque staged multipart-part capability
   - add the durable staging-provenance enum and central apply/recovery validation
   - validate data-PG acknowledgements before installation and encode the immutable witness
   - register the snapshot-sensitive cleanup-root publisher and exhaustive owner loop
   - add the durable staging lease and shard-local deletion-exclusion handoff
   - register every sealed cleanup-maintenance transition and typed installer
   - add the object-PG cleanup-root state machine, expiry adopter, worker, route-history references,
     reservation-release convergence, and atomic terminal/claim transitions
   - advance the metadata-command encoding and exact-current rejection fixtures
   - move the existing streamed path unchanged behind `CommitMultipartPart`
   - add request-owned single-segment part staging only after the terminal path is shared
4. **HTTP lazy promotion and cleanup**
   - delay UploadPart session creation until the body crosses one segment
   - thread the immutable effect fence through every new durable boundary
   - implement bounded lookahead and incremental decoder/digest-state transfer
   - implement evidence-based ownership reconciliation against the pre-existing cleanup root
   - retain bounded pooled buffering and current admission/authentication timing
   - remove obsolete direct/stream terminal RPCs, error variants, helpers, and tests
   - update the publisher registry, compiler boundary fixtures, metadata-command guide, and
     storage-format version ledger
5. **Performance decision**
   - execute the segment-size and staging-boundary matrix locally and on the four-host cluster
   - evaluate per-request latency, memory, cumulative metadata amplification, lease stability,
     and post-workload cleanup debt across each operation and concurrency level
   - retain the UploadPart fast path only if it materially improves the common exact-8-MiB or
     repeated-single-segment workload without regressing larger streamed parts
   - retain or change the 8 MiB internal boundary only from the recorded comparative evidence;
     do not infer it from any one SDK's multipart threshold

## Correctness Tests

- Forced request-owned and forced durable-session staging of the same one-segment payload produce
  equivalent committed object/part metadata, excluding explicitly nondeterministic identities.
- Direct and streamed PutObject have identical conditional, versioning, overwrite, object-lock,
  checksum, encryption, ACL, tag, lifecycle, and stale-reclaim outcomes.
- Direct and streamed UploadPart have identical checksum, encryption, replacement, ListParts,
  completion, abort, and displaced-payload outcomes.
- Deterministic races cover UploadPart versus completion, abort, and another write of the same
  part number, plus route expiry and pending-command contention at terminal installation.
- Response-loss-before-install-acknowledgement, partial fanout/apply, abandonment, and restart
  tests prove exact command/part evidence transfers ownership and ambiguous outcomes neither
  delete referenced payload nor leak permanently.
- Request-owned command apply succeeds deterministically with data PGs unavailable after the
  acknowledgement witness is built; malformed, incomplete, duplicated, and crossed witnesses
  fail before pending-slot installation or through object-PG-local recovery validation.
- Crash interleavings before and after cleanup-root creation, terminal-command installation,
  part publication/root removal, abandonment/cutoff transition, and pending-slot clearing always
  leave at least one exact durable owner. Cleanup still succeeds after two or more route changes.
- Root-creation dispatch tests distinguish authoritative pre-publication `NotSent` from
  publication-start, accepted replica apply, and `MayHaveApplied`: only the first may clear the
  unstarted pending intent; every latter case converges exact creation before a maintenance
  transition.
- An expired `Staging` root with no live frontend is adopted into `CleanupReady`, cleaned, and has
  its exact reservation released; exact pending or committed evidence prevents that transition.
- A delayed shard write that passed its initial check before expiry remains covered by its staging
  and shard-local deletion-exclusion leases through acknowledgement persistence. A deterministic
  hook pauses it after write admission but before file/ack commit, expires the primary staging
  lease, and runs adoption plus claim. Deletion remains blocked until the placed-write lease is
  released, and the delayed write cannot publish a late ack after its cutoff; cleanup then removes
  every partial file and row.
- A transport-gap regression pauses a shard request after frontend/primary authorization but
  before storage-node admission. Early frontend lease relinquishment cannot make the root
  cleanup-ready before its immutable cutoff. Delivery before the cutoff acquires the shard-local
  lease and remains root-owned; delivery after the cutoff is rejected before file or ack creation.
- An abandonment transport-gap regression installs a terminal command, pauses an already-authorized
  shard request before storage-node admission, and abandons the command before the cutoff. Every
  replica records `AbandonedAwaitingCutoff`; neither adoption nor cleanup claim can advance it
  early. Delivery before cutoff remains root-owned, while delivery after cutoff is rejected. Only
  the later primary-decided cutoff command makes the root cleanup-ready and permits deletion.
- Deterministic terminal-install-versus-cleanup-claim races prove exactly one serialized transition
  wins. Once cleanup is ready or claimed, publication cannot install; while the exact terminal
  command occupies the primary pending slot, cleanup cannot claim or delete its payload.
- Multi-replica terminal tests prove pending-slot installation leaves every root in exact
  `Staging`, then primary-first and witness-first apply each consume that same local state without
  role-dependent command validation.
- Crashes after shard deletion but before bucket-PG reservation release, and after release but
  before root removal, resume from `PayloadDeletedAwaitingReservationRelease` without leaking or
  releasing a crossed reservation proof.
- Reservation release still converges after at least two route transitions and proves the root
  retained the exact historical bucket-PG route, not merely another PG from the same epoch.
- Cancellation and checksum/signature failure before publication leave no visible part/object or
  leaked reservation/session, and cleanup convergence removes the root, shard rows, and files.
- Recovery and Unix/TLS paths reject crossed staging ownership, upload IDs, part numbers,
  reservation proofs, epochs, and segment manifests before mutation.
- Threshold tests split payload at 8 MiB minus one, exactly 8 MiB, and 8 MiB plus one across every
  relevant HTTP-frame boundary, including signed aws-chunked bodies with checksum trailers, and
  prove promoted digest/decoder state is identical to uninterrupted streaming.
- Admission-expiry interleavings cover reservation allocation, session/cleanup-root creation, every
  shard/append write, and pending-slot installation over local, Unix, and TLS paths.
- Unix/TLS tests delay delivery across the absolute wall-clock cutoff and use deliberately
  different process monotonic origins, proving transit never restarts a remaining duration.
- Publisher-class tests exercise every exhaustive outcome for both staging modes, including
  matching-command preservation, exactly one contender drain per owner iteration, and budget or
  deadline exhaustion before the next attempt.
- Registry and recovery tests exercise every cleanup-maintenance family member, including exact
  replay, one-contender return, claim expiry, crossed state/proof rejection, ambiguous dispatch,
  and restart after partial replica apply.
- Multi-replica expiry tests place replica clocks on opposite sides of both staging and claim
  deadlines and prove only the primary decides expiry; replicas accept or reject solely from the
  command-bound generation/cutoff and exact local state.
- AWS oracle tests pin any observable race outcomes changed by delaying UploadPart session
  creation.

## Non-Goals

- A second committed object or multipart-part layout.
- Inline tiny-object or tiny-part storage.
- Resumable S3 PutObject/UploadPart requests.
- Removing durable stream sessions for bodies larger than one internal segment.
- Combining standard-object and multipart-part terminal commands.

## Completion Criteria

- Staging ownership is the only direct-versus-streamed distinction below HTTP ingestion.
- All standard segmented objects use one terminal publisher and recovery invariant.
- All multipart parts use one terminal publisher and recovery invariant.
- Every committed or pending multipart part has exactly one durable staging provenance that can
  be validated and cleaned after restart.
- No conditional, versioning, overwrite, checksum, encryption, or reclaim semantics are selected
  by staging mode.
- Single-segment UploadPart skips durable stream-session creation and segment-append publication.
- Larger parts retain bounded memory, durable cleanup, and current failure recovery.
- Benchmarks quantify the retained optimization rather than relying on assumed benefit.
