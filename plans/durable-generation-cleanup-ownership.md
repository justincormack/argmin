<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Durable Cleanup Ownership — Unresolved Findings

> Roadmap status: **Active, highest priority** as of 2026-10-28. Implement the
> shared write/cleanup exclusion contract and enable all six ignored
> regressions; see [`plans/README.md`](README.md#active-plan-inventory).

Status: production regression evidence plus initial abstract design modelling.
The unfinished production fix has been removed; no new
ownership tables, worker, staging tickets, RPC operations, or format versions are
part of this slice. A shared fix will be designed and reviewed separately under
[unified-staged-write-publication-plan.md](unified-staged-write-publication-plan.md#shared-cleanup-investigation-before-further-implementation).

## Lost Direct-PUT Reservation Cleanup

The [discovery experiment](bounded-model-checking-plan.md#discovery-experiment--cleanup-without-a-model-supplied-retry)
found that cancellation can lose the obligation to release a generation reservation:

1. Two admitted direct PUTs reserve generations and stage payload.
2. One leaves an exact pending command with a retained authorized-recovery flight.
3. The other drops or explicitly discards its payload handle. Shard cleanup runs,
   but reservation release is blocked by the unrelated recovery flight.
4. Production maintenance recovers the pending command, then performs healthy
   stream, shard-audit, and reclaim passes.
5. The cancelled caller's reservation remains. No durable cleanup obligation was
   installed for maintenance to discover.

The permanent test
`production_maintenance_releases_cancelled_direct_put_reservation_after_recovery`
covers both command owners and both cancellation forms. It requires cleanup
without a model-supplied release retry. The non-ignored paired control,
`production_maintenance_preserves_live_direct_put_and_cleans_cancellation_after_recovery`,
checks live-request preservation and cancellation after the pending slot is free.

This establishes retained unused metadata, not object-data loss. An in-memory
retry queue alone would not establish recovery after restart; an age-only scan
cannot distinguish abandoned reservations from slow live callers.

## Late Writes After Cleanup

A separate schedule shows that completing metadata cleanup does not fence physical
writes made under a still-valid route:

1. Prepare a direct PUT or stream segment and retain its original route admission.
2. Release the direct-PUT generation reservation, or finish stream-session abort.
3. Deliver the prepared physical write without a surviving frontend running
   compensating cleanup.

The direct regression `direct_put_release_fences_delayed_staging` uses the
existing explicit reservation-release operation and verifies release on every
replica. It also checks every expected shard file and acknowledgement before and
after the delayed write, independently of its result and before any returned
payload handle is dropped. It does not depend on the removed prototype's
generation scanner.

Four stream regressions cover PutObject and UploadPart sessions. For each target,
one pauses after preparation; the other first writes and publishes the segment,
checks its exact files and durable acknowledgement rows, aborts and verifies
deletion, then delivers a delayed duplicate. The latter is not the unrecorded-
manifest crash window. Both targets previously reproduced successful late writes
leaving all three 2+1 shard files present without acknowledgement rows or a session.

These tests split local production phases deterministically, without sleeps. They
do not establish transport-gap, node-restart, or whole-MPU-abort coverage.
The existing caller-compensation tests remain useful but do not prove safety
after that caller disappears.

## Retained Regressions

Owner-local tests live in:

- `crates/storage/src/cluster/local/tests/direct_put_cleanup_progress.rs`
- `crates/storage/src/cluster/local/tests/late_stream_cleanup.rs`

The six known-failing regressions have explicit `#[ignore = "..."]` reasons
pointing here. The healthy control remains enabled. Ignoring the reproducers
records unresolved bugs; it does not weaken their assertions or claim a fix.

Run the normal selection, then explicitly reproduce the unresolved failures:

```bash
cargo nextest run -p storage -E 'test(cleanup_progress) | test(late_stream_cleanup)'
cargo nextest run -p storage -E 'test(cleanup_progress) | test(late_stream_cleanup)' --run-ignored ignored-only --no-fail-fast
```

## Shared Fix Requirements

Shard and metadata publication are not transactional. The known crash window
between writing shards and recording their manifest still needs orphan scanning;
this investigation does not require a metadata command per shard or claim to
eliminate that window.

A future fix must use a common physical write/cleanup exclusion contract across
direct PUT, streamed PUT, and UploadPart. Durable provenance can differ, but
cleanup must retain its obligation through failed release/deletion, unavailable
nodes, restart, and retained-route changes. Pending and committed payload must
remain protected, while cleanup cannot declare completion while delayed writes
can recreate it. Scanner candidates are hints, not authority to revoke live state.

The removed metadata-owner and node-ticket prototypes were incomplete design
experiments, not selected architecture. Reconcile any future lease, cutoff,
ticket, or scanner design in the unification plan before production integration.
Measure changes to small-PUT metadata cost and follow the versioning guide for
any eventual durable or wire-format changes. Fix-only tests for the removed
prototype are not retained as evidence for existing production behavior.

## Design Evaluation — 2026-10-29

No production protocol is selected yet. Evaluate two independent obligations:
durable cleanup ownership after caller loss, and physical exclusion of delayed
writes after cleanup. A cleanup record alone does not establish exclusion.

### Durable owner alternatives

1. **Extend existing reservations/sessions (preferred starting point).** Persist
   exact attempt/generation and bucket-incarnation identity, owner authority,
   cancellation/expiry evidence, command ownership, cleanup/release responsibility,
   and required historical route references with the existing creation operation.
   Reusing bucket-write authority is a candidate, not permission to make an
   unfenced cross-PG check followed by deletion. Cancellation is an acceleration
   signal; loss before that signal persists must still leave discoverable work.
2. **Dedicated staging owner.** Create a common durable staging record before
   writes, separate from stream progress. This makes ownership explicit but may
   add replicated metadata work to every small PUT. Compare piggybacking creation
   on the existing reservation transaction against a separate command.
3. **Retry queues/scanning alone: insufficient.** Process-local retries vanish on
   restart; an intent written only after failure has a crash gap; age does not
   distinguish a slow live owner from an abandoned one. Scanners are discovery
   aids and must obtain authoritative exclusion before deletion.

Neither owner representation by itself closes the physical file/manifest crash
window. Safe orphan discovery remains necessary, including files without rows.

### Physical exclusion alternatives

| Candidate | Required contract | Cost / open question |
| --- | --- | --- |
| Explicit revocation | Each target durably closes the exact staging identity, serializes closure against write admission, drains admitted writes, and rejects delayed duplicates after restart. | Extra durable state/RPCs; revocation evidence cannot be retired until old messages are independently inadmissible. |
| Bounded write authority | An immutable cutoff bounds every dispatched write; cleanup waits until no node can admit it and drains admitted operations through file and acknowledgement persistence. | Delayed reclamation, clock-health assumptions, and retention of the maximum outstanding authorization horizon. |
| Hybrid | Explicit revocation allows early cleanup; cutoff supplies a recovery backstop. | Both protocols and their proof obligations; defer unless measured cleanup latency justifies it. |

Compare existing-record ownership with each of the first two exclusion options.
The initial preference is bounded authority if bounded delayed reclamation is
acceptable, not a final architecture decision. Ordinary route/owner renewal must
not extend a previously dispatched write's cutoff. Stream appends may need
separately bounded authorities whose outstanding horizon survives abort.

Transport uses the established absolute conservative wall-clock upper bound,
shortened for inter-host skew and rebound to receiver-local monotonic time, never
a remaining duration or a serialized monotonic timestamp. Clock uncertainty
defers reclamation. A write admitted before expiry can still finish afterward:
local exclusion must last through file publication and acknowledgement persistence.
Replica apply compares immutable command evidence and does not evaluate clocks
or perform availability-dependent cross-PG reads.

### Ownership and completion rules

- Persist responsibility before effects requiring cleanup, not inside Drop.
- Serialize fresh terminal installation against cleanup adoption on the object
  PG; exact pending/committed commands protect their payload through ambiguity.
- Retain durable retry responsibility through deletion and exact reservation
  release, including unavailable nodes and multiple route transitions.
- Missing or unavailable ownership observations do not authorize reclamation.
- An unavailable target must not retain frontend admission indefinitely or starve
  unrelated PGs. Physical cleanup can remain durably pending.
- Distinguish cancellation acceptance from completed physical cleanup. A cutoff
  design must not quietly weaken the existing release/abort regression contract.
- Data-node revocation retirement, if used, needs an independent proof that old
  messages cannot regain authority; process-local locks or finite tombstone age
  are not that proof.

### Evaluation gates

1. **Safety model:** explicitly separate dispatch, remote admission, file
   publication, acknowledgement persistence/delivery, pending installation,
   publication-start, cancellation, expiry, cleanup, and release. Explore
   delayed delivery before admission as well as already-admitted writes.
2. **Production replay:** enable the six ignored regressions without manual
   maintenance retries. Add live-owner, pre-session cancellation, restart,
   response-loss/partial-fanout, whole-MPU-abort, delayed-duplicate, and multi-route
   transition cases. Assert files and durable rows independently.
3. **Recovery progress:** state healthy-delivery, scheduling, and clock-progress
   assumptions. Bound work per PG, preserve obligations on errors, and include
   ENOSPC/recovery-headroom requirements from DUR-5.
4. **Cost:** measure metadata commands, replica writes, fsyncs and RPCs per attempt,
   retained state, cleanup delay/abandoned bytes, 4 KiB PUT, and 8 MiB UploadPart.

Keep terminal publisher unification and small-UploadPart optimization out of the
first correctness slice. Update the detailed unification proposal after selecting
the shared contract; its multipart-root design is not an implemented prerequisite.

## Initial Safety Model

The storage-owned, test-only
[`cleanup_ownership_model.rs`](../crates/storage/src/cleanup_ownership_model.rs)
uses the existing bounded explorer to compare cutoff and explicit-revocation
protocols. This is an **independent abstract model**, not an implementation test
or evidence that either protocol is production-ready. The six production bug
regressions remain ignored and unresolved.

### State and atomic boundaries

The initial state assumes one already-durable owner/reservation, one exact
payload/shard, and two possible duplicate write deliveries. Durable file and
acknowledgement state are separate. Each delivery advances independently through
dispatch, data-node admission, file persistence, acknowledgement persistence,
local exclusion release, and optional frontend receipt delivery. An admitted
operation can finish after the cutoff: this deliberately exercises the stronger
requirement to drain in-flight IO, rather than assuming expiry cancels IO.

Cancellation persistence is separate from frontend loss. Frontend loss does not
persist cancellation or withdraw transport messages. One data-node restart
terminates admitted volatile operations but preserves files, acknowledgements,
and durable revocation; messages not yet admitted can still arrive. One release
failure preserves the owner, followed by optional restart and successful retry.

Object-PG terminal installation and cleanup claiming are assumed serialized.
Pending installation immediately protects the exact payload; application is a
separate event and can succeed after frontend loss. The initial model deliberately
does not allow abandoning a pending command, conservatively protecting it. The
later publication extension separates authoritatively unstarted abandonment from
irrevocable publication and composes those events with its physical transitions.

For cutoff cleanup, the one expiry event represents the point after which all
healthy nodes must reject this immutable write authority. For revocation, closure
is a durable data-node event serialized against admission, followed by draining
existing exclusion holders. No revocation retirement is modelled.

### Properties and negative controls

- Pending/committed payload retains both its file and acknowledgement and cannot
  be claimed for cleanup.
- Deleted payload cannot reappear, including delayed duplicate requests.
- Reservation-release responsibility cannot disappear while the reservation exists.
- Required reachable states cover live staging, command application after caller
  loss, admitted IO across cutoff, rejected deliveries with completed cleanup,
  retry/restart after release failure, and cancellation before any physical write.
- Explicit ordered traces additionally pin delivery rejection **after** cleanup
  and restart, and prevent cleanup while the second duplicate still holds exclusion.

Independent mutations remove cutoff waiting, in-flight draining, durable
revocation across restart, durable responsibility on release failure, pending
ownership protection, or the terminal-install/cleanup exclusion. Each must produce
its own expected safety violation; a shared failing fixture is not sufficient.

The graph is explored to exhaustion with a 250,000-state resource cap per run;
hitting the cap is a failure, not a successful bounded proof. Time has two
values (before/after cutoff); no allocation identity wraps. There is no fairness
or eventual-progress proof. Cleanup reachability only demonstrates possible
progress with successful scheduled delivery and release.

### Remaining modelling gates

The fanout extension below covers two data nodes, partial durable closure,
and fixed healthy clock offsets with abstract absolute-cutoff rebinding. The
renewal extension separately covers one durable horizon extension, delayed
renewal replies, and abstract detected clock faults/trusted resynchronization.
The publication extension composes three metadata replicas and recovery with
the initial physical-cleanup model, including publication-start, lost replies,
and safe abandonment. Before selecting a production protocol, combine that
composition with renewal/all-node closure; exercise production clock binding/health
and authority
re-establishment; extend renewal to multiple append horizons and compose it with
all-node closure; add owner creation and crash-before-session windows;
multiple subjects and generations;
route/history changes; whole-MPU abort; revocation retirement; and bounded healthy
recovery tails. File-sync failure, orphan discovery, ENOSPC and physical filesystem
recovery remain production replay/UAT obligations. The model currently assumes
successful persistence actions are durable and that restart ends local IO.

```bash
cargo nextest run -p storage -E 'test(cleanup_ownership_model)' --success-output immediate
```

Initial focused validation (2026-10-29): all three tests passed. The correct
cutoff candidate exhausted 2,043 states / 5,810 transitions; the revocation
candidate exhausted 3,488 states / 11,003 transitions. All six unsafe mutations
produced their independently expected counterexamples. These counts apply only
to the explicit assumptions and bounds above, not production safety or liveness.

## Data-Node Fanout and Clock Extension — 2026-10-29

[`cleanup_ownership_model/fanout.rs`](../crates/storage/src/cleanup_ownership_model/fanout.rs)
is a separate abstract submodel starting with a cancelled durable owner and one
already-dispatched write for each of two shard locations. It explores two
different shard locations, not a metadata replication quorum. The parent model
retains command installation/application and duplicate-write coverage; these
models are not yet one composed proof.

### Added boundaries

- Revocation, draining admitted IO, receiving the closure reply, deleting a
  shard, and receiving the deletion reply are independent events per target.
  Cleanup requires **all** target closure replies, not quorum evidence. Owner
  retirement requires all deletion replies. Reservation-release retry remains
  in the parent model; the fanout model abstracts that step after confirmed
  physical deletion.
- Node 1 can become unavailable and restart once. Other-node closure/deletion
  continues where enabled, but unavailable-target work remains owned. Restart
  terminates admitted local IO, retains durable revocation/file/ack state, and
  discards process-local deadline bindings. An unadmitted request survives and
  can arrive after restart. Reply delivery in this submodel requires an online
  node; arbitrary in-flight response delivery across a crash remains future work.
- Both nodes use distinct monotonic origins; node 1 changes origin on restart.
  Deadline binding and write admission are separate actions. A delayed frame
  binds its immutable **absolute** portable cutoff against receiver wall time,
  not a transported duration or a sender-monotonic timestamp.
- One raw-route renewal is allowed, but cannot change the captured write cutoff.
  This tests separation from raw-route renewal, not durable owner/append lease
  renewal, which still needs a model.

### Clock assumptions and cleanup proof

Time has nine values (ticks 0 through 8). Healthy clocks advance at equal rates
with fixed offsets; the pairwise skew bound is two abstract units. Three explicit
offset profiles place either target behind the cleaner by the full skew budget,
or put the cleaner behind a target. These are boundary cases, not an exhaustive
clock-fault model. Monotonic origins are 100 and 10,000, changing to 1,000 for
node 1 after restart. Arithmetic is model-owned, not the production binder or
wire codec, and does not redefine their shortening policy.

Let `D` be the already-issued portable write cutoff. A receiver admits only before
its bound local deadline. To prove every receiver has passed `D` using the
cleaner's healthy clock alone, cutoff cleanup waits for `cleaner_wall >= D + skew`.
It then obtains per-node quiescence acknowledgements. The extra cleanup margin
accounts for a cleaner ahead of a data node; it is not another shortening of the
write's admission deadline. Revocation permits earlier cleanup after every node
has durably closed admission and drained IO, while ordinary deadline enforcement
still applies to requests. Neither candidate treats expiry/revocation as cancelling
an already-admitted filesystem operation.

### Independent evidence

Required reachable states include partial closure while node 1 is unavailable,
successful persistence on both nodes, deleted shards with no deletion replies,
and completed cleanup after restart plus raw-route renewal. Ordered canaries pin
the slow-node admission window at the cleaner's numerical cutoff, rejection after
the conservative boundary, binding before renewal, rebinding after restart, and
early revocation cleanup without advancing time. They also assert that partial
closure cannot claim cleanup and missing deletion acknowledgement cannot retire
the owner.

Six separate negative controls must produce their named violation: omitted
cleanup skew, partial closure acceptance, borrowing the renewed route deadline,
restarting the original duration on receipt, losing revocation on restart, and
retiring the owner before deletion replies. The resource cap is 750,000 states
per exploration, and exhaustion of that cap fails the test.

This does not model quorum metadata apply, clock jumps/unhealthy clocks, raw TLS
transport, dynamic route changes, tombstone retirement, deletion durability after
power loss (DUR-3), or eventual recovery under fair scheduling. Successfully
persisted effects remain durable by assumption. No production fix is claimed.

Focused validation (2026-10-29): all seven parent/fanout tests passed, including
all twelve independent unsafe variants and the ordered boundary canaries.

| Protocol | Cleaner / data-node offsets | States | Transitions |
| --- | --- | ---: | ---: |
| Cutoff | +1 / (-1, 0) | 34,676 | 129,212 |
| Cutoff | +1 / (0, -1) | 36,838 | 136,377 |
| Cutoff | -1 / (+1, 0) | 35,704 | 127,308 |
| Revocation | +1 / (-1, 0) | 71,916 | 287,442 |
| Revocation | +1 / (0, -1) | 75,086 | 298,173 |
| Revocation | -1 / (+1, 0) | 97,612 | 389,176 |

Each graph exhausted below the resource cap. These counts are coverage of the
declared abstraction, not comparative production performance measurements.

## Owner Renewal and Clock-Health Extension — 2026-10-29

[`cleanup_ownership_model/renewal.rs`](../crates/storage/src/cleanup_ownership_model/renewal.rs)
is a separate cutoff-candidate submodel with one durable owner, one data node,
one old write and one potentially renewed write. It starts before cancellation
with no pending or committed metadata command. It does not compose the parent
publication model or the two-node fanout model yet.

### Proposed renewal contract under test

- The owner durably records the maximum cutoff it may issue **before** a renewal
  reply can escape. Persistence, reply issuance, and delivery/dispatch are
  distinct events. A lost reply cannot shorten that durable horizon.
- Renewal persistence and cancellation serialize on the same owner. Cancellation
  prohibits further horizon extension, but cannot retract a reply already
  authorized by a preceding durable grant. Such a reply may arrive and dispatch
  a write after cancellation; cleanup must still honor its cutoff.
- Renewing the owner gives the new write a later immutable cutoff. It does not
  extend the old write. This is different from the fanout model's raw-route
  renewal, which cannot extend either captured write's authority.
- Cleanup uses the maximum durable horizon plus the cleaner/data-node skew
  margin, then waits for admitted IO through acknowledgement persistence. It
  does not use the original cutoff, receipt delivery, or cancellation as proof
  that all write authority has ended.
- Owner restart retains the horizon. Data-node IO is independent and can finish
  while the owner is down. This slice ends at physical deletion; owner retirement
  and reservation-release retries remain separately covered by the earlier model.

The finite schedule allows one renewal and one owner restart. Reply issuance and
delivery require an online owner in this submodel; already dispatched data IO
does not. Delivery of buffered renewal replies during owner downtime remains a
transport-composition case, rather than a tested guarantee here.

### Clock scope and assumptions

The model explores ticks 0–10, initial/renewed portable cutoffs 14/17, a wall base
of 10, cleaner offset +1 and data-node offset -1 (pairwise skew 2). One cleaner
wall jump of either +8 or -8 can occur. Write admission at the healthy data node
uses the unchanged absolute cutoff; the fanout model separately covers binding
to receiver-local monotonic clocks. Neither model transports a remaining duration
or a process-local monotonic timestamp.

Each clock fault is assumed detected at effect admission and latches the owner
clock unhealthy. Wall correction alone does not restore trust. Owner restart
enters an unbound clock state, rather than resetting the clock to healthy.
One explicit **trusted resynchronization** event may restore the original skew
bound without changing durable horizons or issued cutoffs. This is an assumption
on the future recovery protocol, not a proposal to clear the existing process
clock-health latch in place. The model calls neither the production health monitor
nor the production lease binder. Establishing how fresh authority is obtained,
handling unavailable clock-health sources, receiver clock faults, and proving
this abstraction against production are still open gates.

### Evidence and limits

The correct graph must reach delayed renewal delivery after cancellation,
completed cleanup despite an undelivered durable renewal, and cleanup after both
restart and clock recovery. Ordered canaries independently pin old-write expiry,
continued validity of the renewed write, waiting for its in-flight IO, the clock
latch blocking renewal on an open owner before its initial cutoff, and wall
correction/restart being insufficient to authorize cleanup.

Six negative controls require their own named violation: issue before persistence,
lose the horizon on restart, clean using the original cutoff, renew after
cancellation, trust an unhealthy clock, and trust a clock merely because its
process restarted. The cleanup-time invariant compares against the independent
data-node clock, not the cleaner's health flag or chosen cleanup cutoff.

Focused validation (2026-10-29): each healthy-protocol graph exhausted 25,036
states / 95,567 transitions for the forward and backward jump cases. All six
negative controls produced their expected counterexamples. The cap remains
750,000 states per graph; reaching it fails the test. These are bounded safety
checks and reachable progress witnesses, **not** fairness/eventual-cleanup proofs.
No production mutation path, format, or ignored regression changes in this slice.

Slice validation: all 11 cleanup-model tests passed; full Nextest passed all
9,019 tests (one slow, six skipped) in 239.080 seconds. Workspace all-targets /
all-features Clippy with `-D warnings`, formatting, and `git diff --check` passed.

## Publication/Recovery Composition — 2026-10-29

[`cleanup_ownership_model/publication.rs`](../crates/storage/src/cleanup_ownership_model/publication.rs)
reuses the initial model's actual physical-transition generator, replacing only
its atomic install/apply abstraction. It adds the publication ordering described
in [metadata-command-stream.md](../guides/metadata-command-stream.md#normal-flow).
This remains an **abstract design model**, not execution of the production
publisher or the separate production-connected publication pilots.

### State and boundaries

The initial state contains one durable owner/reservation, one already-written
payload with file and acknowledgement present, and one duplicate write in
transport. Metadata has three fixed replicas: witness, primary, and trailing.
There is one exact command identity and no competing command or route change.

- Frontend pending-install dispatch, primary acceptance, and reply delivery/loss
  are separate events. Acceptance rechecks the captured expiry and cleanup state;
  a response lost before acceptance does not withdraw the request. Acceptance can
  still occur after frontend loss, leaving a discoverable exact pending owner.
- The primary publication-start marker persists before any witness dispatch.
  Marking alone forbids abandonment, even with no replica rows or dispatched apply.
  Recovery of a marked command continues after request expiry and cancellation;
  these are not fresh publication authority.
- Replica dispatch, apply, reply receipt/loss, and exact-row observation are
  separate events. Lost replies can precede remote apply or follow durable apply.
  Witness confirmation precedes primary dispatch, which precedes trailing dispatch.
  Observation certifies exact durable application without requiring the original
  response. Absence of a replica row cannot undo a publication marker.
- One primary restart preserves pending intent, marker, and replica rows while
  discarding cached confirmations. It does not retract in-flight remote requests.
  Frontend loss and activating an authorized recovery worker are separate events;
  the model assumes that recovery authority is valid.
- Authoritatively unstarted abandonment atomically records the terminal decision,
  retains the existing durable owner as cancelled cleanup work, and removes the
  pending slot. It does **not** revoke shard authority or waive cutoff waiting.
  Physical cleanup and reservation release then use the earlier model's events,
  including release failure and data-node restart.
- Applied metadata continues protecting the payload after slot removal. Slot
  removal requires all replica confirmations and successful reservation release;
  failed release retains the slot and its retry responsibility.

The model treats abandonment's durable decision/cleanup handoff as one atomic
ownership boundary. It does not yet model replication of abandonment tombstones
or the implementation of that transaction. Primary restart is an atomic reopen
event, not a model of metadata-node downtime, torn writes, or election. Existing
physical restart terminates local IO and preserves files/acknowledgements by
assumption. Unknown/crossed command evidence is not represented: all observations
refer to the one exact command. These assumptions must be checked during production
realization, not inferred from graph exhaustion.

### Independent checks and evidence

Invariants protect payloads whenever the pending slot or **any** applied replica
references them, prohibit abandonment after publication-start, retain the slot
through partial replica convergence, and require a durable witness before primary
publication. The parent invariants additionally prohibit payload resurrection and
loss of reservation-release responsibility.

Required witnesses cover lost installation replies after frontend loss, marked
intent without replica rows after restart, lost witness replies, recovery of
trailing replicas, terminal release failure, and abandoned cleanup after release
failure/data restart. Ordered canaries independently check marker-only protection
before any dispatch, an in-flight apply with no visible row, exact recovery after
lost replies at all three replicas, release failure retaining the slot, and no
cleanup of committed payload after slot removal. The abandonment canary verifies
that neither safe abandonment nor a delayed install bypasses physical closure;
the delayed duplicate is rejected after cleanup and data restart.

Seven separate unsafe variants must produce their expected violations: treating a
lost install reply as release of ownership, abandoning marked intent with no rows,
forgetting the marker on restart, dropping the owner during abandonment, clearing
the slot before convergence, forgetting committed ownership, and installing during
cleanup. These are independent runs, not one shared failing schedule.

Focused graph evidence (2026-10-29): cutoff exhausted 714,270 states / 3,914,237
transitions; revocation exhausted 717,708 states / 3,929,534 transitions. Both are
below the explicit 750,000-state cap; exceeding it fails the test. The explored
bound includes one frontend loss, one primary reopen, one data-node restart,
one release failure, and one dispatch per replica. Exact-row observation stands
in for idempotent recovery; repeated RPC delivery is not independently modelled.

This composes publication with the original physical cleanup model, **not yet**
with two-target fanout, clock faults, or owner renewal. It does not prove liveness,
select a production owner schema, or resolve any of the six ignored regressions.

Slice validation: all 15 cleanup-model tests passed. Full Nextest passed all
9,023 tests (one slow, six skipped) in 234.486 seconds. Workspace all-targets /
all-features Clippy with `-D warnings`, formatting, and `git diff --check` passed.
