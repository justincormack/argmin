<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Durable Cleanup Ownership — Unresolved Findings

> Roadmap status: **Active, highest priority** as of 2026-10-28. Implement the
> shared write/cleanup exclusion contract and enable all six ignored
> regressions; see [`plans/README.md`](README.md#active-plan-inventory).

Status: production regression evidence, bounded design modelling, and a concrete
[contract proposal for review](#proposed-production-contract--2026-10-29).
The subsequent [minimum-contract and transaction review](#minimum-contract-and-transaction-review--2026-10-29)
sets the current direction: reuse existing durability barriers; the earlier
proposal's additional publishers and per-segment intents are not selected.
The [concrete window/transaction candidate](#concrete-window-and-transaction-candidate--2026-10-29)
now specifies the retained owner, separate allocation bounds, and integration
gates. It is a design for review, not an implemented or fully proven protocol.
The [atomic stream-creation implementation](#atomic-stream-generation-and-session-creation--2026-10-29)
combines stream generation reservation with session creation, before adding the
retained owner/window records. It advances command/RPC encodings but reuses the
existing tables and apply transaction. The unfinished cleanup prototype was
removed; no retained ownership tables, worker, staging tickets or new RPC
operations have landed. The six cleanup regressions remain unresolved.
The shared cleanup fix is coordinated with
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
and safe abandonment. The interface-composition extension below adds one renewal
and two-target closure, with explicitly collapsed install/apply steps and clock
barriers. This is not a proof of the unrestricted cross-product. Before production
integration, specify the concrete owner/closure contract using the source audit;
exercise production clock binding/health and authority re-establishment; extend
renewal to multiple append horizons; add owner creation and crash-before-session windows;
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

## Renewal / All-Target / Publication Interface Composition — 2026-10-29

[`publication/composition.rs`](../crates/storage/src/cleanup_ownership_model/publication/composition.rs)
executes the publication model's existing transitions alongside two physical data
targets and one durable horizon extension. Both targets initially hold the same
already-staged payload's respective shards. Target 0 has an old duplicate write in
transport; target 1 may receive a renewed authorization for an exact duplicate.
This is not a second object, generation, or newly appended segment.

### Bounds and deliberate abstractions

- Three metadata replicas retain the parent model's marker, exact application,
  confirmation, abandonment, release, and slot-clear semantics. One command is
  installed and every modeled successful apply loses its original reply; exact
  observation confirms it later.
- Installation is a macro step executing parent dispatch, durable acceptance,
  and lost reply. Each replica apply is a macro step executing dispatch, apply,
  and lost reply. These call the parent's actual transition generator and guards.
  Interleavings **inside** these sequences are omitted from this graph, not
  claimed safe by composition; the preceding model explores those boundaries
  separately. No automatic refinement or partial-order-reduction proof is claimed.
- Time has three states: before the original cutoff, original cutoff passed,
  renewed cutoff passed. These are conservative global expiry barriers, abstracting
  away clock conversion/skew. Passing the original barrier also expires the
  original terminal admission. Clock faults and receiver-local rebinding remain
  separate evidence, not production logic exercised by this composition.
- Renewal persists before its reply can be delivered. Cancellation, terminal
  installation, or original expiry closes fresh renewal. Already-granted authority
  can still arrive after abandonment, frontend loss, publication, or cleanup.
  Primary restart retains the maximum horizon; it never extends the old write.
- Data targets independently admit writes, persist files and acknowledgements,
  finish IO, close admission, report closure, delete, and report deletion. Cutoff
  closure waits for the maximum granted horizon, including undelivered grants;
  revocation can close earlier. Both drain in-flight IO and require **every**
  target's closure reply before claiming cleanup.
- Target 1 can be unavailable and restart once. In-flight local IO ends on crash;
  durable file/ack/revocation state survives. The owner persists through incomplete
  closure, missing deletion replies, and release failure. A failed target does not
  prevent the other target's closure progress. No fairness or eventual-delivery
  guarantee is asserted.

Closure/deletion replies recorded in the model are treated as durable owner
evidence and survive primary reopen. The implementation must either persist that
exact evidence or reacquire it before advancing cleanup; retaining an ordinary
process-local reply cache across a real restart would not realize this assumption.

Publication protection is checked against **both actual target states**, not just
the parent's aggregate payload projection. Aggregate deletion advances only after
both deletion replies, then exact reservation release and owner retirement use
the parent transitions. Restart/IO durability and exact-duplicate identity remain
assumptions. Revocation evidence survives owner retirement in this model: safe
tombstone retirement is still an open design obligation.

### Evidence

Both correct graphs exhaust below the 750,000-state cap: cutoff has 162,432 states /
693,684 transitions; revocation has 252,180 states / 1,143,918 transitions. Required
witnesses include partial closure with an unavailable target after renewal,
renewed publication converging through recovery, and abandoned renewed ownership
finishing cleanup after target restart.

Five independent unsafe variants detect: using the original cutoff for a renewed
owner, ignoring admitted IO, accepting partial closure, forgetting the delivered
renewal on primary restart, and retiring before all deletion replies. Ordered
canaries additionally pin:

1. A renewed write admits after the original cutoff, blocks closure past its own
   cutoff until IO ends, and leaves cleanup pending while its node is unavailable.
   A primary restart retains the horizon; missing deletion replies and release
   failure retain ownership independently.
2. Publication-start with no replica rows protects both targets after renewal,
   frontend loss, primary restart, and both cutoffs. Recovery applies the original
   command and committed metadata still protects both targets after slot removal.
3. Revocation completes cleanup before **either** cutoff, then a renewal reply
   arrives after owner retirement and target restart. The target rejects that
   still-time-valid write because durable revocation remains, not because time
   happened to expire.

All 20 cleanup-model tests passed in focused validation. Full Nextest passed
all 9,028 tests (one slow, six skipped) in 236.897 seconds. Workspace all-targets /
all-features Clippy with `-D warnings`, formatting, and `git diff --check` passed.
No production format, mutation path, or ignored test changed.

## Production Boundary Audit — 2026-10-29

This is a source mapping, not an assertion that the modeled protocol exists.
The six ignored production regressions remain the acceptance gate.

| Required boundary | Current owner / source | What exists and what is still needed |
| --- | --- | --- |
| Durable owner before dispatch; maximum issued horizon | [`pg_store/schema.rs`](../crates/storage/src/pg_store/schema.rs), `object_generation_reservations` and `stream_uploads` | Reservations retain ID, bucket/key, generation and creation time; sessions additionally retain `cleanup_after` and bucket-write proof fields. Neither is the modeled common maximum-issued-shard-cutoff/cancellation/closure record. Specify the owner incarnation, grant ordering, renewal serialization, and cleanup state before changing schema. |
| Effect deadline at append preparation | [`node_client/local.rs`](../crates/storage/src/node_client/local.rs), `prepare_stream_segment_append_inner`; [`cluster/payload_operations.rs`](../crates/storage/src/cluster/payload_operations.rs), `prepare_stream_segment_append_with_route_validation` | Session/generation authority is compared under the object-PG guard and the effect fence precedes segment-ID allocation. This does not persist an outstanding shard-write horizon or provide data-node exclusion for the later IO. |
| Portable request deadline | [`types.rs`](../crates/storage/src/types.rs), `AdmittedRouteEffectFence`; [`storage_node_server/session.rs`](../crates/storage/src/storage_node_server/session.rs), `admitted_route_effect_fence` | Absolute conservative wall cutoffs are rebound to receiver-local monotonic deadlines. Reuse that representation; do not send durations or monotonic timestamps. Production clock health/re-establishment must be tested against the proposed cleanup eligibility rule, not assumed from the abstract barriers. |
| Data-node write admission through acknowledgement persistence | [`node_client/local.rs`](../crates/storage/src/node_client/local.rs), `LocalPlacedShardRoute::write_placed_shard_with_effect_fence`; [`storage_node_server/routes.rs`](../crates/storage/src/storage_node_server/routes.rs), `StorageNodeActiveShardRoute::write_if_absent`; [`node.rs`](../crates/storage/src/node.rs), shard-file writers | Current paths check route/effect validity before physical IO. They do not acquire the modeled attempt-bound staging exclusion guard lasting through file and acknowledgement persistence. A route permit is not proof that cleanup cannot delete this attempt. |
| Exact remote staging authority and all-target closure | [`storage_rpc/types.rs`](../crates/storage/src/storage_rpc/types.rs), `StorageRpcShardWriteRequest`; [`node.rs`](../crates/storage/src/node.rs), `try_begin_object_payload_reclaim` | Shard writes carry location, key, expected bytes/checksum and optional route deadline, not the proposed durable staging-owner authority. Existing generation read leases/reclaim fences protect reads and cannot be assumed to fence prepared writes; their registry is process-local. Define the write guard/closure RPC and restart contract, not an unfenced cross-PG lookup. |
| Terminal installation versus cleanup adoption | [`node_client/local.rs`](../crates/storage/src/node_client/local.rs), fenced pending-slot insertion; [`pg_store/command_log.rs`](../crates/storage/src/pg_store/command_log.rs) | Exact pending slots and effect checks under the PG guard exist. The proposed owner-state check and cleanup-claim transition must share serialization with insertion; adding a coordinator precheck alone cannot implement this modeled boundary. |
| Irrevocable publication and abandonment handoff | [`pg_store/command_log.rs`](../crates/storage/src/pg_store/command_log.rs), `mark_pending_metadata_command_publication_started` and `record_metadata_command_abandoned`; [`guides/metadata-command-stream.md`](../guides/metadata-command-stream.md) | Exact durable publication marking and transactional abandonment records exist. The common cancelled-owner handoff does not. Specify its transaction/replication relationship to abandonment and pending-slot removal; only authoritatively unstarted commands may take that path. |
| Retryable deletion and reservation release | [`cluster/operation_routes.rs`](../crates/storage/src/cluster/operation_routes.rs), `DirectPutPayloadWrite::cleanup_on_owner` and `DirectPutGenerationReservation::release_on_owner` | Cleanup currently disarms its process-local handle before deletion/release and ignores release failure. Retain a discoverable durable obligation through all target replies and exact release. This is the existing lost-cleanup finding, not a newly fixed path. |
| Historical route ownership | [`pg_store.rs`](../crates/storage/src/pg_store.rs), `cluster_map_history_reference_summary` and `cluster_map_history_route_references` | Current retention covers live segments, backfill, pending command resources, bucket reservations and reclaim claims. A future cleanup owner must retain its exact object/data/bucket-PG route dependencies after session/pending-state removal until cleanup and reservation release converge. The abstract fixed topology cannot prove this. |

### Next design decision

The audit still favors evaluating an extension of existing reservation/session
ownership with bounded write authority first: existing effect deadlines are useful
building blocks, but do **not** supply the durable horizon or in-flight exclusion.
Explicit revocation additionally needs durable data-node admission tombstones and
a safe retirement proof; keep it as a measured alternative rather than silently
implementing a hybrid.

Next specify the concrete owner schema and state machine, how authority is minted
before dispatch, where all-target closure drains writes, and how abandonment and
cleanup transfer/release responsibility. Include exact historical route references,
replica-deterministic apply, format/version evidence, worker recovery/ENOSPC, and
small-PUT command/fsync cost. Then review that contract before production changes
and replay the six ignored regressions plus the new cross-boundary schedules.
The model counts are not a reason to skip the remaining gates above.

## Proposed Production Contract — 2026-10-29

**Review proposal, not approved architecture or implemented behavior.** Use a
common object-PG durable owner, created with existing allocation/session commands,
and finite write-authority cutoffs. Do not add early data-node revocation in the
first implementation. Cancellation stops new grants, but physical deletion waits
for the maximum granted cutoff and quiescence of every effect participant.

**Review disposition:** the [transaction review below](#minimum-contract-and-transaction-review--2026-10-29)
supersedes this proposal as an implementation prescription. In particular,
replicated per-segment preparation, idle grant commands, cold confirmation RPCs,
and separately persisted progress states require independent justification.
The safety obligations remain; this complete but more expensive candidate is
retained for comparison, not approved by its level of detail.

This replaces the assumption that a live frontend cleanup handle is sufficient.
It is a separately measured correctness change, not permission to unify terminal
publishers or optimize UploadPart in the same slice. The abstract models support
parts of this contract; they do not yet cover the full schema/protocol below.

### 1. Durable records and ownership identity

Evolve the existing generation-reservation storage into a common
`payload_staging_owners` relation on the object's metadata PG. Stream sessions
reference that owner rather than being its only lifetime anchor. A row must
survive removal of its session or pending slot when cleanup is unfinished.

The exact immutable subject is `(bucket, bucket_incarnation, key, generation,
attempt_id, target)`. `target` is a closed enum: direct PutObject, streamed
PutObject, or UploadPart with exact upload ID and part number. Attempt IDs are
storage-minted; generation allocation remains durable and non-recycling. Neither
same-key generation substitution nor a renewed route creates another owner.

The owner stores:

- its creation command identity, origin object-PG/epoch, target, and immutable
  subject; creation/retry compares the complete identity;
- the exact stable bucket-write reservation identity and bucket-PG route needed
  for eventual release; mutable heartbeat deadlines are not reservation identity;
- owner state, grant sequence and monotone maximum **issued-or-issuable** portable
  wall cutoff; a durable grant counts even if its reply is lost;
- the authoritative terminal-command binding when applicable, and cleanup claim
  identity/generation; primary-local pending-slot ownership is distinguished from
  replicated owner state;
- exact cleanup participant identities and durable progress, or references to
  the child records below. Failure diagnostics do not replace progress evidence.

Add owner-scoped grant and segment-intent child relations in the same SQLite PG.
A grant records sequence, immutable cutoff, allowed target/placement domain and
allocation bounds. A segment intent records exact segment identity, codec/size
geometry, EC shape, placement epoch/data PG, file locations, and acknowledgement
metadata locations. Intent creation precedes physical dispatch. Distinguish a
file-owning node from the data-PG primary that registers its acknowledgements.

For direct single-segment PUT, piggyback owner, initial grant, and segment-zero
intent on `ReserveObjectGeneration`; the request supplies codec/size/EC inputs
and storage derives locations after allocating the generation. Encryption may
follow allocation; the initial intent need not know ciphertext CRC. It must bind
the exact allowed byte length and identity, while write validation and the later
integrity witness bind the actual bytes. This needs **no separate owner-creation
command** on the successful small-PUT path.

For streamed PUT and UploadPart, piggyback owner/initial grant on the existing
generation/session creation workflow. Replace primary-local append-ID allocation
with a replicated `AuthorizePayloadSegment` command that allocates the exact
segment identity and persists its intent before returning write authority. It may
also extend the grant horizon. This is one command per prepared segment, not per
shard; it is an explicit additional streaming cost, not a free fixture change.
Do not claim cross-replica recovery from a primary-only allocator update.

Original owners, grants, and intents participate in metadata digests/checkpoints.
Roots/children are never reconstructed from filesystem age or caller-supplied
shard lists. This records intended final paths even when a caller dies before
publishing a completed segment manifest. Orphan/tmp-file scanning remains needed
for artifacts outside that inventory; this is not an assertion of cross-store
transactions or elimination of all orphan windows.

### 2. Owner states and primary serialization

Use closed typed states with schema constraints, not independent permissive flags:

| State | Permitted progress / retained responsibility |
| --- | --- |
| `Open` | Authorize segments/renew grants, install an exact terminal command, or seal against future grants. |
| `SealedUnpublished` | No new grants or terminal installation. Preserve all previously granted cutoffs and segment intents until physical closure. |
| `PublishedDraining` | Payload has been published; retain writer closure and all live/reclaim references, including after a later overwrite removes the live reference. No physical deletion or fresh grants. |
| `ClosedPublished` | All staging effects are closed; retain exact closure provenance while live/history/MPU metadata still references payload. |
| `CleanupReady` | Writes are closed and authoritative local metadata proves the selected intent set unreferenced; durable cleanup may be claimed. |
| `Cleaning` | Claim owns exact file/ack locations and retains retries across failures. |
| `PayloadDeleted` | Every required file/row deletion is confirmed durable; exact reservation release and final retirement remain. |

Only the primary decides clock-dependent expiry before installing a transition.
Replicas validate exact owner/grant sequence, immutable cutoff, state, and command
evidence without consulting their clocks or doing remote shard reads. Old grants
cannot be forgotten when advancing state. Transitions and proofs cannot switch
bucket incarnation, generation, attempt, upload, part, or authorized target set.

Under the same object-PG command serialization and SQLite transaction boundary:

- terminal installation checks `Open`, exact owner/intents, and its immutable
  admitted effect fence, then installs the owner-bound pending command;
- grant extension and cancellation/expiry adoption inspect that same pending
  state; they cannot grant fresh authority or claim cleanup around an unresolved
  terminal command;
- an exact pending command protects its owner regardless of response delivery;
  unavailable or contradictory observations are not an absent command;
- publication-start, any accepted apply, or ambiguous apply requires convergence
  of that command, not expiry-driven abandonment;
- only authoritatively unstarted abandonment can atomically record abandonment,
  preserve/seal the cleanup owner and intents, and remove the pending slot. If
  cross-replica abandonment is partial, the slot remains recoverable until the
  handoff is established; never clear it first and enqueue work afterward.

Pending installation is primary-local. Terminal replica apply must therefore
accept the exact replicated `Open` owner/intents and atomically produce published
metadata plus `PublishedDraining`; it must not require a primary-only pending
state on replicas. Exact replay validates the same command/owner relationship.
After publication, metadata owns the payload but the owner row still carries
outstanding-write and reservation-release responsibilities.

### 3. Finite authority, renewal and transport

Every staging grant has a finite absolute conservative wall-clock cutoff, even
on static clusters whose route itself is unbounded. Bound a new grant by both
the captured route authority and a named maximum staging window `W`. `W` is a
configuration/schema decision with a finite maximum; choose its default from
measured admission latency and acceptable cleanup delay before implementation.
It is not a remaining duration transported to a receiver.

Grant extension is replicated and converged before its authority can escape.
`AuthorizePayloadSegment` may extend the horizon for the next segment; an idle
session renewal uses an explicit registered `RenewPayloadStagingAuthority`
command if needed. Existing bucket-reservation heartbeats alone cannot extend
physical-write authority. The stored maximum covers every possible outstanding
grant; losing a reply or restarting never shrinks it. A newly renewed grant does
not change old prepared writes' deadlines.

For this first candidate, the current grant horizon also bounds the live owner's
lease. Idle-body keepalives therefore durably renew that lease/grant before it
expires, even if no new shard is ready; their extra replication cost is explicit.
Renewal requires a still-live `Open` owner and fresh admitted authority at its
installation boundary. An expired owner cannot be resurrected merely because
the sweeper has not yet sealed it. The worker must not infer abandonment from
`created_at` or from an older individual write's cutoff while a newer owner grant
is live. Separating idle-owner leases from write grants is a later optimization,
not an unstated second source of liveness authority.

Frontend preparation returns a storage-owned capability binding owner, grant,
segment, exact location/ack scope, operation, byte geometry, and cutoff. Node
clients must not accept free-form combinations of those fields. The wire carries
the canonical subject and portable cutoff, never process-local monotonic time.
Receivers validate and bind it once using the established deadline machinery,
then revalidate under the local effect-admission lock immediately before IO.

For the first protocol, authenticate grants by exact owner-primary confirmation
over existing authenticated RPC, not by trusting a caller's serialized Rust
token or introducing an unspecified new signature format. The confirmation binds
the authenticated requesting participant, owner/grant sequence, segment, operation,
placement and cutoff to the converged durable record. Positive confirmations may
be cached for that immutable tuple until its cutoff; absence/timeout is never
permission. No cross-PG lock is held across the confirmation RPC. Recheck local
deadline/clock health after it returns and before admitting an effect.

This adds cold confirmation RPCs and availability dependence on the owner
authority; measure them. A later self-contained signed grant would be a separate
security/performance design, not an assumed property of ordinary RPC auth.
Sealing does not invalidate outstanding confirmed grants before their cutoff,
so caching cannot race early deletion. Cold confirmation after restart still
requires exact durable evidence and valid current/retained authority; it does
not mint or renew a grant. An owner missing after retirement cannot confirm one.

### 4. File and acknowledgement effects are separate participants

Use an owner/intent-bound node-local effect registry. Entry validation and guard
acquisition serialize with closure; every admitted effect holds its guard until
its local mutation and durability work have finished. No await/network operation
while holding the registry mutex. A process crash ends local IO; restart cannot
admit an expired grant merely because its in-memory registry is empty.

Two distinct effect classes must use this protocol:

1. Physical shard creation: guard spans temporary-file IO, final publication and
   required syncs. Embedded and Unix/TLS staging both use write-if-absent with
   exact existing-byte validation; delayed writes must not overwrite another
   value at a published identity.
2. Acknowledgement registration: the actual acknowledgement-metadata participant
   acquires its own guard under the **same grant cutoff** through SQLite commit.
   A delayed registration is a durable effect, not a harmless retry exempt from
   expiry. Unknown/ambiguous registration retains the owner for inspection.

These are not one imaginary guard held across two different machines. Between
them, the durable grant and maximum horizon preserve responsibility; cleanup
cannot start before their common cutoff and must drain **both** effect classes.
An expired registration may fail after file creation; the durable intent then
owns that unregistered file's cleanup. Successful acknowledgements remain
self-contained integrity evidence for terminal command construction; replica
apply does not synchronously inspect data-PG acknowledgement rows.

Before implementation, inventory maintenance/repair/transfer writers too. Their
existing retained authorities must interlock with physical deletion through their
own exact live-reference/reclaim guards; they cannot use an unfenced staging
fallback or be treated as a fresh grant on a closed owner.

### 5. Closure, publication, and later reclaim

Sealing is a durable command, not completed cleanup. Once the primary can safely
establish that the maximum portable cutoff has elapsed (including the supported
cleaner/receiver skew bound), it requests closure from **every** file and
acknowledgement participant named by the intents. Each participant checks exact
sealed-owner evidence, its own healthy-clock expiry, and drains admitted effects
before replying. No quorum shortcut. Uncertain clocks or unavailable targets
leave durable work pending; they do not keep frontend admission indefinitely.

Closure receipts bind owner/grant horizon, participant and scope. Persist them
under the owner, or reacquire them after restart before advancement. The
clock-dependent decision occurs before command installation; replication records
immutable evidence without independently testing wall time. With cutoff-only
admission, no durable data-node revocation tombstone is needed: after cutoff and
quiescence, neither an old request nor restart can admit another staging effect.

Crucially, publication cannot erase this obligation. A published object may be
immediately overwritten, its version deleted, or its multipart upload aborted
while an old write is still in transport. Terminal publication records the
staging-owner provenance in the committed segment/part descriptors. Subsequent
reclaim inherits it and requires closure of the originating writer **as well as**
the existing reader/reclaim exclusions. `ClosedPublished` is retained as durable
closure evidence until the last live/historical/MPU reference and reclaim
obligation is gone; absence of an owner is not an invented closure receipt.

Multipart completion can assemble parts from several staging owners. It must
preserve each part's provenance, not replace it with the completed object's
generation. Part overwrite and whole-MPU abort must hand off all relevant owners
before removing the only discoverable session/part records. Orphan cleanup must
not delete an intent referenced by any pending or committed command/version.

### 6. Deletion, release, and API completion

After closure and exact unreferenced-state validation, claim cleanup in the
object-PG stream. Delete only recorded targets; verify durable row **and** file
absence independently. Deletion acknowledgement must include the durable unlink
contract in DUR-3, not just a live `exists() == false`. Data-PG acknowledgement
deletion and each file location are separate retriable obligations. Record all
replies before `PayloadDeleted`; on reply loss, repeat exact idempotent deletion
or observation. Do not retire the owner after a best-effort loop.

Release the exact stable bucket-write reservation through its owning bucket PG.
Persist confirmation before dropping its route reference. Generation allocation
authority is sealed with the owner; retain the cleanup record even if a logical
reservation lookup no longer returns an active reservation. Final owner removal
requires no references, all deletion confirmations, and no outstanding release.
Confirmation means exact successful release or authoritative exact-identity
absence at the required route; a generic bucket-not-found or unavailable-history
response is not release evidence.
Published owners may release their terminal reservation after convergence while
retaining closure provenance; release is not permission to delete their payload.

Keep these return contracts distinct:

- `CancelAccepted`: durable handoff exists; physical work may remain. Drop is
  bounded and best effort only in delivering this hint, because creation/expiry
  already made the owner discoverable without it.
- `CleanupComplete`: all physical/release obligations are durably complete. The
  existing explicit release/abort regression entry points must retain this
  meaning on success. They may drive/wait for bounded-cutoff closure without
  holding frontend publication admission; on exhaustion report pending/failure,
  never a successful no-op. Do not weaken the six tests to accept cancellation.

The shard cutoff is independent of route validity, so the six delayed-write
regressions can still assert that the original route remains valid after cleanup.
Add deterministic owner-local clock/closure controls, not arbitrary sleeps or
manual release retries. SSE behavior and S3 abort/error precedence remain unchanged.

### 7. Recovery, routes, and capacity

A storage-owned worker discovers owners by durable state/horizon, not by a
frontend retry queue. Process bounded work per owner/PG, retain the first error
while attempting unrelated eligible work, and persist progress. Reuse the
authorized exact-command recovery path for pending/partially published commands;
the cleanup worker must not forge static or historical recovery authority.

Owner/intents retain exact object-PG epochs, every data/ack route epoch, and the
bucket-PG reservation epoch until each responsibility is discharged. Include them
in both route-reference enumeration and compact retention summaries. Test at
least two map transitions, restart, and a changed primary with outstanding writes
and release work. Closure observations cannot be transported to a different
owner incarnation or participant set.

ENOSPC can block the command needed to authorize cleanup or persist release.
Fail closed; do not unlink unowned candidates to make space. Add admission
backpressure and explicitly budget recovery headroom for owner transitions,
command/WAL growth, checkpoints and deletion bookkeeping. Emergency reserve and
operator recovery policy remain coordinated with DUR-5. The implementation cannot
claim recovery progress under arbitrary full-disk conditions until that test
matrix passes; enlarging the cluster is not assumed to be possible without a
control-plane write.

### 8. Publisher classes, formats, costs, and implementation slices

Owner creation and segment authorization are fresh `SnapshotSensitive` publishers:
validate exact live subject and admission, drain at most one contender per owner
iteration, and respect the shared budget. Same-request exact retries converge
without allocating another owner/segment or extending its cutoff accidentally.
Publication-start or ambiguous dispatch makes an installed grant command
irrevocable; recovery first converges it, then seals/cleans if its caller vanished.

Register a sealed owner-transition maintenance family for seal/expiry, closure
evidence, cleanup claim, deletion progress, and retirement. Its authority is an
exact durable owner and validated predecessor state, not a raw unbounded request
fence. Exhaustive outcomes distinguish exact convergence, a single contender,
stale owner/proof, not-yet-eligible closure, and unavailable retained authority.
It uses joined exact-command recovery and a bounded worker budget. Add canonical
IDs/classification to the registry, checker evidence and metadata-command guide
when implementing; ordinary terminal publisher unification remains deferred.

No version constants change in this documentation slice. Implementation changes
the PG schema, owner/segment/session/command encodings, checkpoint/digest inputs,
and storage-RPC payloads/registry. Audit each owning grammar and containing
version vector under `guides/versioning.md` and `guides/storage-format-ledger.md`;
advance affected outer versions when their grammar changes. Append manifests,
complete request/success/error fixtures and resealed rejection evidence; never
rewrite historical bytes or add compatibility readers/migrations.

| Path | Proposed incremental cost to measure |
| --- | --- |
| Successful small direct PUT | More fields/rows in existing reservation/publication transactions; no standalone owner-creation command. Cold participant-to-owner confirmations and later closure work are new. Retaining published closure provenance costs durable space. |
| Streamed PUT / UploadPart | Replicated segment authorization replaces primary-local preparation; up to one extra command per segment. Piggyback horizon extension where possible; explicit idle renewal has its own cost. |
| Cancellation/reclaim | Cutoff wait plus all-participant drain, durable progress, deletion and exact release retries. No per-shard consensus command is proposed, but batching progress cannot skip ownership evidence. |

Do not infer that preserving command count preserves fsyncs or latency. Measure
4 KiB direct PUT and 8 MiB parts, command count, SQLite transactions/fsyncs,
confirmation/closure RPCs, retained rows/bytes, cleanup latency, unavailable-node
behavior and recovery throughput. Approve `W` and its limits from these results.

Implement in reviewable slices after contract review:

1. Versioned owner/grant/intent state and exhaustive transition/apply/restart
   tests, including pre-session crashes and independent cross-subject negatives.
2. Fenced file **and acknowledgement** effects plus portable deadline confirmation,
   all-participant closure, and Unix/TLS delayed-admission/IO/restart regressions.
3. Owner-bound terminal installation, atomic abandonment handoff, and committed
   provenance through overwrite, version deletion, MPU completion/abort and reclaim.
4. Discoverable bounded worker, route retention, durability/capacity evidence,
   and unchanged completion assertions in all six previously ignored regressions.
5. Cost measurements and acceptance review before enabling the contract across
   all staging paths. Keep the new path unavailable until its full safety chain
   exists; format acceptance must not expose a partially implemented owner protocol.

Review gates still include the finite window value, cold-confirmation cost,
replicated append-preparation cost, exact retained-owner authorization after
primary changes, successful-publication/overwrite schedules, and DUR-3/DUR-5.
The model has not proved these production obligations, and this proposal does
not resolve the six ignored regressions.

Documentation-slice validation: full Nextest passed all 9,028 tests (one slow,
six skipped) in 237.441 seconds. Strict workspace Clippy, formatting and
`git diff --check` passed. This verifies the unchanged code baseline, not the
unimplemented contract described above.

## Minimum Contract and Transaction Review — 2026-10-29

**Design review, not an implementation or a claim that the bugs are fixed.**
The cost objective is **no additional durability barrier on ordinary successful
small PUT, or per streamed segment**, where the necessary evidence can be added
to an existing transaction. Count SQLite commits, replicated command phases and
file/directory syncs separately. Adding rows to an existing transaction increases
WAL bytes and may increase checkpoint work even without adding a commit. Do not
trade away a required durability barrier or promise zero additional total fsyncs.

### Smallest complete safety contract

The following obligations are necessary; the eight-state owner machine, child
tables and publisher names in the earlier proposal are possible implementations,
not separate requirements.

1. **An owner exists before writes can escape.** Durable, non-recycling attempt
   identity binds bucket incarnation, key, generation and PUT/session/part target.
   It retains exact reservation-release responsibility and enough placement
   provenance for recovery. Caller loss before session creation must be covered,
   not only loss after a session exists. Ambiguous owner creation is recovered as
   the exact existing command, not replaced by another allocation.
2. **Every effect has a finite, owner-covered authority.** Durable evidence must
   bound the maximum cutoff of all issued or issuable grants and their scope;
   this may be a conservative upper bound, not an individually recorded grant.
   File writes and separate acknowledgement writes both enforce that bound at
   their own admission boundary and hold local exclusion through durable effect
   completion. Neither a route renewal nor an old caller can extend it. Remote
   authorization must be authenticated and subject-bound, not just an opaque
   Rust value serialized by the caller. Clock uncertainty fails closed.
3. **Publication and cleanup cannot both own the same payload.** Grant extension,
   terminal install and cleanup adoption serialize on the object PG. An exact
   pending command protects payload through lost replies. Publication-start,
   possible apply and partial fanout require exact convergence. Authoritatively
   unstarted abandonment preserves cleanup responsibility before removing the
   pending slot. Replica apply uses immutable evidence and local state, not clocks
   or remote data-PG reads.
4. **Deletion cannot be followed by an old write.** Cleanup waits past the entire
   authorized horizon with the supported clock-skew margin, then drains every
   relevant file/ack participant, not a quorum. This includes transport-delayed
   writes and IO admitted before expiry. Restart must not reopen that authority.
   Repair/transfer writers and reader leases retain their own exact exclusions;
   they cannot bypass this with a generic staging grant.
5. **Ownership survives publication and removal of references.** Terminal commit,
   overwrite, version delete, part replacement, MPU completion and abort transfer
   the original writer's horizon/provenance with payload ownership. Immediate
   reclaim of a successfully published payload must also obey obligation 4.
   Removing a session or reservation is not evidence that its writes have stopped.
6. **Cleanup remains discoverable until all obligations are discharged.** Files,
   ack rows, and exact reservation release are independently retriable. Preserve
   required object/data/ack/bucket route history across restart and primary change.
   Unknown outcomes or unavailable targets retain responsibility; workers use
   bounded work and do not hold frontend admission indefinitely. Successful
   `CleanupComplete` still means durable completion, not cancellation accepted.
   Unrecorded files need a defined discovery owner; a missing manifest is neither
   deletion authority nor proof that cleanup is complete.

These obligations cover all six retained regressions **and** late acknowledgement
registration, caller loss, restart, partial publication and immediate overwrite/
reclaim. Passing only the six tests is not sufficient. DUR-3 durable unlink and
DUR-5 recovery headroom remain requirements, not costs waived by this review.

### Existing transaction map

This is a source audit, not a measured syscall count. The common
`apply_metadata_command_and_record_with_commit_guard` in
[`command_apply.rs`](../crates/storage/src/pg_store/metadata/command_apply.rs)
opens one `BEGIN IMMEDIATE`, applies the payload and records its log/digest state,
then commits. `with_immediate_txn` in
[`support.rs`](../crates/storage/src/pg_store/metadata/support.rs)
reuses an already-open transaction; nested mutation helpers do not imply a
second commit. A replicated command still has other durable phases such as
pending-slot install, publication-start and terminal slot removal.

| Existing boundary | Evidence/work that can share it | Limit on the reuse claim |
| --- | --- | --- |
| Bucket-write reservation acquisition | Existing exact bucket proof and release identity. | Different PG from object ownership; this is not one atomic cross-PG transaction. Pre-owner crash recovery of this reservation remains with its bucket authority. |
| Direct PUT `ReserveObjectGeneration` apply | Owner identity, initial finite horizon, placement scope, stable bucket proof and its one exact segment intent. | Extend the command inputs; the current row has only reservation ID, bucket/key, generation and creation time. Streams no longer use this preceding command. |
| `CreateStreamUpload` apply | Insert the retained owner and initial scope/grant alongside generation reservation, session, target and encryption. | Generation reservation and session creation now share one command; retained owner/window fields are still future work. |
| Primary-local `allocate_stream_segment_vid` | Local preparation/intent fields can share this existing SQLite update if changed to one explicit transaction. | This is not replicated preparation. Surviving the local restart is not evidence that a new primary can enumerate all prepared segments. |
| Shard temporary-file write, file sync and final-name directory sync | An owner-identifying file envelope, if selected, could share existing file persistence. Effect admission/draining can be process-local under immutable cutoff semantics. | No new sidecar may be assumed free. Current files contain raw shard bytes, not a recoverable owner envelope. Changing this requires versioned format, read-path and checksum design. |
| `register_written_shards_batch_exact` / batch acknowledgement transaction | Owner/grant provenance can accompany ack rows in their existing transaction. | This happens after file publication and may be on a different node; it cannot cover a file-only crash window or replace file-node exclusion. |
| `AppendStreamSegment` apply | Completed segment ownership, cumulative horizon, and a grant/scope extension for **future** segments. | This runs after the current segment's writes. It cannot retroactively authorize those writes or make their pre-append crash window disappear. |
| `CommitDirectPutObject` / `CommitStreamPart` apply | Transfer writer cutoff/provenance to published state in the same transaction as object/part metadata and session removal. | Leave any unresolved cleanup responsibility discoverable. Do not add an unconditional `MarkPublishedDrained` command to every healthy PUT. |
| Existing abort, delete/reclaim and generation-release commands | Hand off/seal ownership before removing session/reservation/reference rows; reuse the pending command or a retained owner for retries. | Logical removal cannot also mean physical completion before cutoff/drain/deletion. Preserve publication rules and existing external completion semantics. |
| Existing unstarted-abandonment/tombstone and slot cleanup | Preserve the durable cleanup handoff within the owner transaction before slot removal. | If these are separate commits today, retain the earlier responsibility through the gap; do not invent a cross-replica atomic transaction. |
| Final cleanup/release | Retire owner with an existing eligible release/reclaim command where possible. Reacquire closure observations and repeat exact idempotent deletes after restart. | Release on the bucket PG remains separate. If no existing transaction remains, later owner retirement is additional maintenance work and must be counted. |

Relevant implementation anchors:

- [`metadata_helpers.rs`](../crates/storage/src/pg_store/metadata/metadata_helpers.rs):
  reservation insertion and durable generation high-water advancement.
- [`command_mutations.rs`](../crates/storage/src/pg_store/metadata/command_mutations.rs):
  create, append, abort and part-commit transactions; abort/commit currently remove
  the stream session.
- [`local.rs`](../crates/storage/src/node_client/local.rs):
  `prepare_stream_segment_append_inner` validates the captured session/generation
  under the PG lock, then performs the primary-local allocator update.
- [`payload_operations.rs`](../crates/storage/src/cluster/payload_operations.rs):
  reserve/install/fanout and append flows; ack registration precedes append apply.
- [`shards.rs`](../crates/storage/src/pg_store/shards.rs):
  file durability and separate acknowledgement SQL transactions.

### Preferred low-barrier candidate, and its unresolved part

Evaluate **owner-level finite grants**, initially persisted with existing owner/
session creation, rather than mandating one replicated intent command before
every segment. An existing append may persist the next grant while publishing
the preceding segment. Authority escapes only after the required publication/
recovery guarantee for that exact command is established. An installed or
possibly applied extension counts toward the cleanup horizon even if its reply
is lost. Old writes retain their old cutoff.

The grant must cover a storage-derived, finite identity and placement scope
before dispatch. A broad owner grant cannot silently permit arbitrary shard keys,
new placement epochs or unrecorded participant sets. Grant changes need durable
coverage before use; current retained-route machinery does not automatically
retain routes for segments which have not been recorded. Do not weaken the
existing non-recycling generation/nonce or append-authority guarantees to avoid
replication.

Also evaluate a **seal-time upper bound** before deciding that every grant renewal
needs durable horizon advancement: if issuance has a strictly enforced maximum
lifetime `W`, and a durable seal definitively stops all issuers, cleanup can wait
until the seal time plus `W` and the necessary clock margin. That can share the
existing abort/release transaction without recording each grant. This is only a
candidate: prove that an old primary, cached issuer authority, or delayed grant
request cannot mint a fresh cutoff after the seal. An old primary's still-live
authority may require an additional bounded wait or stronger fencing. Persist
the resulting absolute cleanup bound with the seal, never restart the wait from
receipt time. Live-owner detection and crash adoption still need a sound lease;
this does not make idle liveness free. The existing renewal model assumes a
persisted horizon and does **not** establish this alternative's safety.

**The unresolved issue is discovery, not just the cutoff.** Complete exact intents
before writes give cheap, direct enumeration but add replicated streaming work.
Without them, one of the following must be justified:

- A bounded, storage-derived superset of all possible locations/identities is
  recoverably enumerable from the initial owner and subsequent committed grants.
  Cleanup scans that scope after closure, verifies exact ownership/references,
  and retains the owner until enumeration and deletion finish. Overbroad scope
  increases scan cost, route retention and unavailable-node dependencies.
- Each physical artifact carries recoverable owner provenance in its **existing**
  durable publication, with a complete storage-owned discovery protocol, including
  file-only artifacts and ack-only remnants. This is a format/discovery change,
  not a free use of current raw shard files or a claim that the full scavenger
  already exists. Its discovery scope and history-retention proof are still needed.

A local preparation row plus later replicated append is useful evidence but is
not a complete substitute: loss of that primary before append must not lose the
only cleanup index. An age scan and current metadata absence are also insufficient.
If neither lower-barrier discovery option is supportable, retain replicated
pre-write intents as the explicit fallback and measure their cost. Do not declare
the common fix complete by silently leaving unenumerable payload behind, or make
the scanner authorize deletion independently of the durable owner.

This is a separate decision from remote grant verification. Existing authenticated
RPC transport does not prove an arbitrary caller-supplied grant matches durable
owner state. Cold owner confirmation is one read-only option (latency/availability
cost, not intrinsically a new fsync); a self-contained authenticated grant needs
its own security design. Neither is approved merely to finish this document.

### Costs that cannot honestly be called piggybacked

- **Idle renewal:** there is no append to carry a horizon extension during a long
  body pause. The existing PUT heartbeat updates the bucket reservation and then
  the object-PG primary's proof separately
  ([`bucket_delete.rs`](../crates/storage/src/cluster/request_ops/bucket_delete.rs),
  `heartbeat_put_object_stream_session_with_route_validation`). It is not an
  already-replicated owner-horizon update. Either choose a finite lifetime with
  justified timeout behavior, or explicitly budget occasional durable renewal.
  Extending it opportunistically from wall time after restart is unsafe. Test long
  healthy uploads before selecting a window; do not introduce arbitrary S3 errors.
- **Crash/cancellation maintenance:** foreground transactions may never happen.
  Workers must sometimes persist ownership transitions themselves. Prefer retaining
  the existing owner and batching/retrying cleanup, not a commit per receipt or
  per state name. After restart re-establish closure before more effects; persist
  any evidence that cannot safely be reacquired. Batch only within a valid owner/
  PG transaction, not across independent stores by convention.
- **Physical unlink durability:** directory syncs required by DUR-3 remain real
  costs even if logical cleanup shares an existing metadata command. Measure and
  consider safe directory batching separately; do not substitute row absence.
- **Retained state:** carrying compact horizon/provenance in published metadata
  can defer closure checks until reclaim. An outstanding orphan/release obligation
  still needs its own durable anchor. Eventual retirement/checkpoint work belongs
  in the cost report, not hidden outside request latency measurements.

### Decision and next bounded step

Approve the six safety obligations and the reuse-first cost objective for further
design. Do **not** yet approve replicated `AuthorizePayloadSegment`, one durable
record per grant/receipt, or a separate closure command on every successful PUT.

**Priority clarification:** replacement-primary discovery is the next design gate.
Slow/idle uploads need a bounded resource policy, not the same performance budget
as healthy uploads. Occasional durable renewal on that exceptional path is
acceptable; sufficiently stalled requests can be terminated under an explicit,
tested timeout/progress policy. Preserve the correct S3/transport error behavior
and durable cleanup after termination. Distinguish a large, steadily progressing
upload from a stalled connection; this is not an arbitrary short total-upload
limit. No timeout duration is selected here, and cutoff safety remains mandatory.
Do not delay the discovery decision to optimize idle renewals.

The next bounded step is a **replacement-primary discovery feasibility spike**, not broad
production integration: work through exact direct PUT and two-segment UploadPart
identities against current placement, filenames and allocator state; kill the
frontend/primary between allocation, file sync, ack registration and append.
Specify what a replacement primary can enumerate, which historical routes remain
pinned, and where the next authority horizon becomes durable (or how a seal-time
bound safely covers all issuers without a per-renewal commit). Include idle expiry
and immediate published-part replacement. Reject any design that needs forgotten
frontend state or an unavailable old primary's private allocator to finish.

Then select the discovery representation, with a conservative bounded renewal/
timeout policy rather than an optimized idle path, and amend the model's
atomic boundaries to match those actual transactions, and implement a narrow
end-to-end slice. Before extending it, measure a real-filesystem 4 KiB PUT and
8 MiB part: foreground and eventual background SQLite commits, command phases,
file/directory syncs, bytes written, RPCs, and retained-space/cleanup latency.
Record incremental counts against the unchanged baseline; model state counts
and tmpfs throughput do not establish fsync cost. The preferred acceptance target
is no added ordinary-path commit; any exception needs an explicit review rather
than being hidden inside a new helper.

#### Initial replacement-primary evidence audit

The important crash boundary is **file publication before replicated append**,
not merely frontend failure after a completed append:

- Session creation and generation reservation supply replicated starting state.
  Completed append commands carry exact segment records and advance the replica's
  allocator floor in `apply_append_stream_segment_command`.
- Preparation's `allocate_stream_segment_vid` advances only the primary's local
  session row. A replacement cannot treat its own lower floor as proof that no
  later segment identity was prepared or written.
- The shard filename encodes a 16-byte segment hash, segment VID and shard index
  ([`ShardKey`](../crates/storage/src/types.rs)); the stream hash is derived from
  session ID and segment index
  ([`shard_key_hash.rs`](../crates/storage/src/shard_key_hash.rs)). It does not
  directly expose the session or segment index. Known candidate indices can be
  hashed, but recovery still needs a justified enumeration bound and placement
  scope; it cannot recover these by inverting the hash.
- Ack registration is later and separate. Files may exist with no ack row, so
  listing acknowledgements alone cannot establish cleanup completeness.

Evaluate the lower-barrier alternatives against this exact gap: recoverably
bounded identities/placement granted by existing commands, or owner-identifiable
artifacts published using the existing file sync. Keep replicated pre-write
intents as the comparison/fallback. For each, require discovery without the old
primary or frontend, reference-safe deletion, delayed-write exclusion, retained
route coverage and bounded scan cost. This audit identifies missing evidence;
it is not yet a production failover experiment or proof that either alternative
is sufficient.

## Replacement-Primary Discovery Evidence — 2026-10-29

The next experiment uses production storage phases in
[`cleanup_discovery.rs`](../crates/storage/src/cluster/local/tests/cleanup_discovery.rs).
For each of streamed PUT and UploadPart, it publishes a real encrypted 8 MiB
first segment, then stops the second segment after preparation, file publication,
ack registration or append. Two preparations of the second index model a lost
preparation reply: the old primary consumes two VIDs, not one. Object metadata
has primary node 0; both data/ack PGs have primary node 1. After dropping all
runtimes, the test reopens **only node 1**, never node 0.

| Last completed second-segment phase | Replica object-PG evidence after reopen | Surviving physical evidence |
| --- | --- | --- |
| Preparation | Original session/floor, first manifest and command state unchanged. | No second-segment file or ack. |
| Files | Same unchanged replica metadata. | A second-segment file on the surviving node, no second-segment ack rows. |
| Acknowledgements | Same unchanged replica metadata. | File plus all three ack rows on the independent data-PG primary. |
| Append | Exact second manifest and advanced floor/log position. | File and ack rows, with successful full ciphertext reconstruction before reopen. |

The tests independently compare complete pre-append session/manifest/command
state, positive file and ack counts, discovered VID, data PG, ciphertext geometry
and append placement epoch. They scan actual filenames across the surviving
node's PGs; they do not reopen paths supplied by the lost preparation. Filtering
by the hash of known session/index 1 is an **experimental candidate**, not proof
that production currently authorizes or bounds that index. Expected prepared
records are retained only as assertion canaries.

This establishes which durable evidence is available without the old primary.
It is **not** SIGKILL/power-loss testing, a control-plane election, a route-history
transition, or a cleanup implementation. Stores are closed before reopen; no
second runtime is opened over live SQLite files. The two evidence tests do not
resolve or unignore the six cleanup regressions. Direct PUT was source-audited
here, not added to this restart matrix: its single segment hash is derivable
from the reservation ID and index zero, and its VID is the reserved generation;
the owner still needs durable placement/EC scope, cutoff and release responsibility.

### First candidate to test: replicated bounded lookahead

Prefer evaluating a **bounded reservation window carried by existing commands**
before adding a file envelope or replicated preparation command. Current HTTP
PUT, POST and UploadPart loops wait for each append before processing the next
segment; the buffered PUT and copy loops also append serially. This makes a
one-segment lookahead plausible. It is not presently a storage-enforced invariant:
the node preparation boundary accepts a supplied index and checks duplicates,
not a durable authorized frontier. Future parallelism must obtain explicit wider
windows, not silently bypass the bound.

The proposed window consists of exact owner/session identity, segment index/hash,
a finite pre-reserved retry-VID interval, codec/EC and maximum byte geometry,
placement epoch, data PG, file/ack participants and an immutable authority bound.
All these are storage-derived or validated; the cleanup set is not supplied by
the failed frontend. For a window of `B` VIDs and `k+m` shards, at most
`B * (k+m)` final shard keys need checking at the recorded locations, regardless
of how many files share the PG. `B` is not chosen by this audit.

A narrower alternative records the authorized index/hash but discovers all VIDs
by scanning filenames, without reserving a fixed retry block. The current
`list_scavenger_shard_files` walks the PG's directories; it is not a bounded
per-owner lookup. Compare that option too, including scan completion while
unrelated owners mutate the directory, unavailable participants, and safe
continuation without the old allocator. The block's justification is predictable
exact-key discovery and allocation bounds, not a claim that hashes cannot match
files without it.

1. **Initial window:** extend existing generation/session creation so segment
   zero's window is durable before its first physical dispatch. Direct PUT has
   its one exact segment in the existing generation-reservation command. Cover
   the crash between reservation and session creation without inventing a session.
2. **Advance:** append `i` records its selected complete manifest **and** reserves
   the window for `i+1` in the same existing replicated command/apply transaction.
   Local preparation may consume only within the replicated window. A pending or
   ambiguously applied append already protects the future window until exact
   recovery; its reply is not the source of cleanup ownership.
3. **Retries and primary change:** a local allocator floor cannot prove which
   slots in the window are unused on a replacement primary. Do not recycle its
   VIDs, mint overlapping allocation authority, or resume from the stale floor.
   Retire the uncertain block and obtain a fresh non-overlapping durable block
   through an exceptional operation, or stop that attempt and clean it safely.
   Exhaustion, route/EC changes and slow-upload renewal likewise need explicit
   exceptional progress; they are not reasons for a command on every normal append.
4. **Keep prior windows:** committing one chosen VID does not prove other VIDs
   were never issued. Persist or compact their complete reservation coverage and
   deadlines with the owner, including through session finalization, part overwrite
   and object reclaim. Until closure, an old write may still target a non-selected
   VID. Cleanup excludes exact pending/committed references and checks every other
   authorized key. No dropping an old window merely because the next one exists.
5. **Closure and completeness:** pin the exact recorded historical participants;
   wait out authority and drain both effect classes before deletion. Unavailable
   participants retain work. Do not infer an empty window from one replica's empty
   manifest, ack absence, or the local counter. Temporary-file residue keeps its
   separate bounded cleanup rules; this window concerns final shard keys.

This preserves raw shard-file layout and avoids a mandatory extra replicated
preparation command. It still adds command/schema fields and durable bytes, may
reserve unused identifiers, and requires checking unused slots during cleanup.
Background retirement/progress commits are not free. Speculatively reserving the
next window may retain one otherwise unused route and extend the conservative
cleanup scope even when EOF makes that window unnecessary. Compare these costs
with owner-tagged files (format/read-path change and discovery scans) and exact
pre-write intents (extra normal-path replicated command). Do not claim a measured
fsync saving from this structural comparison.

### Next acceptance gate

Model the bounded-window candidate with **independent primary and replica
allocator state** and exact pending/apply boundaries. Required counterexamples
include lost preparation replies, exhaustion, failover reusing a slot, issuing an
out-of-window identity, forgotten prior windows, delayed ack/file effects and a
part overwritten immediately after publication. Include absent/partial append
publication and retained-route changes. The model must reject each unsafe variant
independently, while a replacement discovers every authorized final key without
consulting the old primary.

Only then review the concrete window schema and its piggyback transactions.
Actual primary replacement and delayed Unix/TLS requests remain production
integration gates, as do the six original regressions and real-filesystem cost
measurements. This slice adds evidence and a preferred candidate for modelling;
it does not select a deployed protocol or add persistence/RPC formats.

Slice validation: both focused evidence tests passed (eight phase/target cases).
Full Nextest passed all 9,030 tests, with six existing ignored regressions, in
238.792 seconds. Strict workspace Clippy, storage boundary checks, formatting
and `git diff --check` passed. These checks do not validate the proposed window
protocol, which is not implemented.

## Bounded Lookahead Model — 2026-10-29

The independent, test-only
[`lookahead.rs`](../crates/storage/src/cleanup_ownership_model/lookahead.rs)
uses the existing bounded explorer to evaluate the reservation-window candidate.
It exhausts **26,312 states and 94,554 transitions** under a hard 100,000-state
cap. Exhaustion and every required witness must succeed; reaching the cap is a
failure, not a partial proof. No production commands, schemas, RPCs or dependencies
change in this slice.

### Explicit bounds and atomic boundaries

- One owner, two retry identities in its first segment window and one
  representative identity in the next segment window. These are complete symbolic
  physical keys, not production VID constants or a chosen production block size.
- Two object-metadata replicas with independent allocation floors. Initial owner/
  window creation is assumed replicated. Preparation changes only the issuing
  primary's floor; ghost issued-identity history detects reuse but is never used
  to enumerate cleanup candidates or choose a new identity.
- Separate append dispatch, pending install, irrevocable marker, witness apply,
  primary apply and slot clearance. Each apply atomically records its selected
  manifest and next window while preserving prior scope. One lost append response
  does not erase any durable grant. Fresh next-window issuance waits for settled
  publication in this candidate; this is conservative, not a claim that every
  weaker handoff is unsafe.
- One primary replacement fences fresh append publication. The replacement does
  **not** allocate from its stale floor. The old issuer may still allocate unused
  slots within its initial delegated window, even after the successor seals the
  owner, but cannot extend scope or cutoff. Already prepared/delayed file and ack
  requests also survive failover. This deliberately tests a stronger discovery
  obligation than merely enumerating the preparations visible at failure time.
- File dispatch/admission/persistence and ack dispatch/admission/persistence are
  separate actions and exclusion guards, with one delivery of each effect per
  symbolic key. A file can finish after cutoff; an ack
  can arrive after file cleanup and must then be rejected. No continuous lease
  across two remote participants is assumed.
- One global conservative cutoff, no renewal, and one symbolic placement-epoch
  advance. Discovery reads the surviving replica's retained scopes and references,
  not the old primary's counter or current placement. Clock skew, lease rebinding
  and actual route-history admission remain separate obligations.
- Cancellation, part finalization and replacement represent already-converged
  metadata transitions. They retain window provenance. Cleanup is an atomic batch
  over exact discovered keys after the cutoff and both effect guards drain; the
  model does not claim atomic physical deletion across real nodes.

An unresolved command remains protected after failover. In particular, the model
does not manufacture an absent/unstarted certificate from the surviving replica's
empty manifest. Witness-only application, or even both replicas having applied
without authoritative slot clearance, can remain blocked indefinitely when the
old primary is unavailable. This is a safety model, not a proof that the existing
recovery protocol can always produce the required authoritative resolution.

### Non-vacuity, mutations and ordered canaries

Eight required reachability witnesses cover: cleaning both unknown initial
preparations with the replica's floor still zero; retry exhaustion; next-window
use after a lost append response; partial publication protection after failover
and expiry; cleanup after part replacement with all three identities issued;
rejected late effects; retained-placement discovery after failover; and cleanup
of an allocation made by the stale old issuer after failover.

Each of nine unsafe variants runs independently and must produce its expected
counterexample:

| Deliberately removed boundary | Required failure |
| --- | --- |
| Stop replacement allocation from a stale floor | An uncertain identity is allocated twice. |
| Enforce the first window's allocation bound | An issued identity has no surviving durable scope. |
| Replicate the next window before allowing use | Issuance from an installed-but-unapplied append escapes replica discovery. |
| Preserve prior retry windows on append | A non-selected issued retry disappears from discoverable scope. |
| Protect unresolved pending commands | Cleanup deletes payload still owned by an installed command. |
| Drain admitted file IO | File persistence recreates payload after completed cleanup. |
| Drain admitted acknowledgement IO | Ack persistence recreates durable state after completed cleanup. |
| Use recorded rather than current placement | Issued historical identities become undiscoverable. |
| Wait out the immutable cutoff | A transport-delayed file request is admitted after cleanup and recreates payload. |

Ordered canaries use the same successor generator and pin both positive and
disabled actions: initial exhaustion and stale-floor rejection; a stale issuer
allocating after cancellation; witness-only publication with cleanup/abandonment
disabled; both-applied publication without permission to clear the old slot after
failover; each file/ack guard independently blocking cleanup; and a live part
remaining protected before replacement. The replacement canary retains the
selected payload, a file-only earlier retry and a future-window request in
transport, then checks exact absence and rejection after closure/cleanup.

### Decision supported, and what remains unproved

Within these bounds, **replicated window coverage is sufficient to discover
unknown preparations without recovering the old primary's allocation counter**.
The window must bound all issuable identities, not just acknowledgements or the
selected manifest. Preservation of prior windows and immutable cutoffs remains
necessary after successful publication as well as cancellation.

This supports continuing with bounded lookahead as the first low-barrier candidate
for a concrete schema/transaction review. It does not measure fsync savings or
prove arbitrary block sizes, hash collisions/subject substitution, all-participant
EC closure, durable release, renewal, clock health, real route transitions,
authenticated grant transport or multi-primary publication fencing. The older
clock/fanout/publication models are complementary independent evidence, not a
composed end-to-end proof of this new candidate. Fresh non-overlapping block
acquisition after exhaustion/failover is not modelled; this slice chooses the
safe stop-and-retain behavior instead of assuming continuation is free.

Next specify the exact owner/window fields, persistent allocation high-water
rules, compact retention of earlier windows and route references, and how they
fit the existing reserve/create/append/terminal transactions. Include the RPC
authority checks that keep stale issuers within their delegated scope. Review
that integration before implementing it; do not add a command on every normal
append merely because exceptional renewal/replacement needs durable progress.
Actual primary replacement, Unix/TLS delayed requests, the six original cleanup
regressions, DUR-3/DUR-5 and real-filesystem cost measurements remain open gates.

Slice validation: all five focused model tests passed, including the nine
independent unsafe variants. Full Nextest passed all 9,035 tests, with six existing
ignored regressions, in 249.861 seconds. Strict workspace Clippy, storage boundary
checks, formatting and `git diff --check` passed. This validates the bounded model
and repository regression suite, not a deployed window protocol.

## Concrete Window And Transaction Candidate — 2026-10-29

This source-backed review advances the bounded model into a proposed physical
representation. The acceptance target remains **no additional ordinary-path
metadata command or SQLite commit**. Extra rows, WAL bytes, retained routes and
eventual cleanup are costs, not free work. No production formats change here.

### What the existing transactions actually provide

- [`schema.rs`](../crates/storage/src/pg_store/schema.rs):
  `object_generation_reservations` contains identity, generation and creation
  time, but no placement, finite write horizon or bucket-proof obligation.
  `stream_uploads` has the local `next_segment_vid`, optional `cleanup_after`
  and optional bucket proof; its segment rows cascade on session deletion.
- [`local.rs`](../crates/storage/src/node_client/local.rs),
  `prepare_stream_segment_append_inner`, validates the loaded session and reserved
  generation under the PG lock, then calls `allocate_stream_segment_vid`.
  [`metadata_store.rs`](../crates/storage/src/pg_store/metadata/metadata_store.rs)
  implements that allocation as a local `UPDATE ... RETURNING`. It is neither a
  replicated window reservation nor a bound on the supplied segment index.
- [`command_mutations.rs`](../crates/storage/src/pg_store/metadata/command_mutations.rs),
  `apply_append_stream_segment_command`, records the selected segment and raises
  the allocator floor from that selected VID. The floor does not record unused
  or uncertain retries. `apply_commit_stream_part_command` and stream abort delete
  the session; [`direct_helpers.rs`](../crates/storage/src/pg_store/metadata/direct_helpers.rs),
  `delete_stream_upload_in_open_txn`, also deletes its generation reservation.
- [`command_apply.rs`](../crates/storage/src/pg_store/metadata/command_apply.rs),
  `apply_metadata_command_and_record_with_commit_guard`, surrounds effects and
  the applied command record with one immediate transaction. Nested
  `with_immediate_txn` calls reuse it
  ([`support.rs`](../crates/storage/src/pg_store/metadata/support.rs)). Adding owner
  rows **inside that transaction** need not introduce another commit. This does
  not collapse the existing install, publication-start, replica and slot-retirement
  phases into one distributed transaction.
- `next_generation_id` already includes `object_write_counters`' durable generation
  high-water, advanced by reservation apply. Preserve it. A segment-window bound
  is a different, session-scoped allocator obligation; do not replace either with
  `MAX(live rows)` or the selected segment's VID.

### Proposed representation: retained owner plus bounded windows

Use an object-PG-owned anchor which can outlive both generation reservation and
stream session. Working table names are `payload_staging_owners` and
`payload_staging_windows`; they are proposals, not new public APIs. A child window
may cascade from its owner, but **neither table cascades from the session,
reservation, multipart upload or live object**. Deleting any of those must leave
the cleanup obligation or atomically transfer it. Table separation is not a
requirement for a separate transaction.

| Record | Required durable information and binding |
| --- | --- |
| Owner | Storage-minted reservation/session identity; bucket and incarnation; key; object PG and originating authority epoch; target (`DirectPut`, streamed PUT, or exact upload ID/part number); reserved payload generation. UploadPart also binds the enclosing upload generation: placement uses that generation, while the distinct part generation binds encryption authority. |
| Owner lifecycle | Explicit pre-session/staging, published, or retiring disposition; immutable owner revision and finite maximum issued-write cutoff; monotone reserved-VID high-water; exact outstanding resource identities and release responsibility. No nullable cutoff meaning infinite write authority. |
| Resource obligation | Exact stable bucket-write proof identity and its bucket PG/original epoch when this owner is responsible for it, plus the generation reservation identity. Mutable heartbeat deadlines are not resource identity. Exact release is idempotent; absence must be authoritatively confirmed, not inferred from a missing session. |
| Window | Owner/revision, segment index and derived hash, nonempty checked VID interval, designated allocation issuer, immutable conservative cutoff, payload codec and maximum stored/shard geometry, EC shape, exact data PG/placement epoch and canonical file/ack participants. Multiple exceptional windows for one index have disjoint intervals and distinct identities. |
| Published binding | Exact selected physical keys and their object/part-generation ownership, or a durable relation to manifests retaining that identity. An object key or upload/part number alone is insufficient after overwrite. Unselected retry windows remain owned separately from selected payload. |

The window is authority for a bounded **physical** effect, not permission to
replace encryption metadata or bypass the existing storage-minted encryption
subject. Direct PUT reserves before its process-local encryption binding; extend
the reservation request with the information necessary to bound placement and
geometry, but do not claim a complete encrypted object already exists there.
Keep the current immutable encryption/subject validation at encryption, append
and terminal publication.

Direct PUT needs one exact `(hash(reservation, 0), reserved generation)` key per
shard, not a speculative stream retry block. The subsequent atomic-creation
refinement eliminates the proposed separate stream pre-session owner: stream
generation reservation and session now share one creation command. Add the
retained owner and first window in that same apply transaction, rather than
reintroducing a preceding command. The separately acquired bucket proof still
needs exact failure/ambiguity handling; do not turn UploadPart's short-lived
create proof into an indefinitely held session proof.

Start with explicit window rows. Compact only adjacent intervals whose complete
subject, index/hash, placement, issuer and cutoff agree; merging different
indices requires an equally exact, versioned derivation, not a wider unchecked
range. Do not pick a production block size `B` from the model's two retry slots.
Bounded lookup costs `B * (k+m)` final keys per window; count empty probes, not
just discovered files. Existing object/part limits and explicit exceptional-window
limits must bound retained rows and scan work as well as each individual block.

### Allocation and replacement-primary rules

Maintain two different values: **replicated reserved end** and **local consumed
cursor**. Creation reserves `[start, end)` and advances the former in its existing
command. Preparation validates the complete expected subject/window and issuer,
checks the immutable effect fence immediately before mutation, and consumes one
slot below `end` in the existing local allocation transaction. Never allocate an
out-of-window index or use an overflowed interval.

Append `i` atomically verifies the selected record belongs to an authorized
window, stores its manifest, and reserves index `i+1` from the reserved end—not
`selected_vid + 1`. It retains every earlier window, including unused slots and
any exceptional retry blocks. Exact replay must match the same next window and
must neither reserve another interval nor rewind an already advanced local
cursor. Next-window issuance requires the publication/convergence evidence
specified by the existing command protocol; mere pending-slot installation or
an isolated witness row is not sufficient.

Replacement authority must not be inferred from the replica's consumed cursor.
The first implementation should **stop allocation for the affected attempt on
primary change or uncertain issuer continuity**, retain its windows, and recover
or clean it. Old prepared requests remain covered until their recorded cutoff.
An old issuer may consume only its already delegated finite window; it cannot
extend its deadline or reserve another block after losing publication authority.
An issuer binding must distinguish authority lifetimes even when a node returns
as primary in the same topology; node ID alone is not exclusive allocation
authority. Preserve local durable allocation on restart, but do not assume that
a reopened replica's floor proves it was the sole previous allocator.

Resuming an affected attempt requires an explicit exceptional command which
reserves a disjoint block from the replicated high-water and binds a new issuer.
That continuation is deferred, not silently supplied by a normal preparation RPC.
The same applies to exhaustion or an incompatible placement/EC change. Keep
failures inside existing typed request/recovery handling and test AWS-visible
outcomes; do not invent a new S3 error or abandon an irrevocable publication.

### Integration by existing transaction

| Existing boundary | Candidate added effects | Added ordinary-path commit target |
| --- | --- | --- |
| Direct PUT `ReserveObjectGeneration` apply | Insert owner and resource responsibility with reservation/high-water and its exact physical window. | None. Inputs/build response/command must carry the exact checked binding. |
| `CreateStreamUpload` apply | Insert owner bound to the exact generation/session/target and reserve the initial window with its finite cutoff. | None. Atomic generation/session creation is implemented; extend its single transaction with owner/window state. |
| Local prepare | Consume the issuer's slot and validate the durable window under the mutation lock. | None beyond the current local allocator update. This is not replica discovery evidence. |
| File publication and acknowledgement registration | Validate exact owner/window authority separately at both effect owners; hold local exclusion through file durability and through ack-transaction completion respectively. | No sidecar or per-effect durable ticket proposed. Existing file/directory sync and ack transaction remain. |
| `AppendStreamSegment` apply | Store selected manifest plus the next immutable window, reserved high-water and cumulative horizon in the same apply/log transaction. | None. Additional command bytes and row writes must be measured. |
| `CommitDirectPutObject` / `CommitStreamPart` apply | Bind selected manifests to the retained owner and mark it published before removing session/reservation rows. Preserve earlier retries and unused lookahead. | None; no unconditional successful-PUT closure command. |
| Existing abort/release and payload-displacement/reclaim commands | Seal issuance or transfer the exact selected-generation reference and leave owner/window cleanup responsibility before removing other rows. | Reuse those transactions; deletion cannot start until cutoff/exclusion and reference protection permit it. |
| Authoritatively unstarted abandonment | Retain the owner across tombstone/slot removal; seal only through a legitimate owner transition. | Do not add a second command solely to copy an obligation already anchored before the write. Ambiguous/started publication still converges. |

Terminal and recovery apply must check these relationships centrally, including
for malformed decoded envelopes; request-side capability checks alone are not
enough. Replica apply compares immutable command/row evidence, never local expiry
clocks. Primary-side expiry selection and local physical admission evaluate time.
Witness-first and primary-later apply need identical valid effects: do not require
a primary-only pending-slot mutation to have changed an owner row on every replica.

Sealing/retirement must compare the exact owner revision under the **same object-PG
serialization as pending-slot installation**. A fresh append or terminal install
must reject a retiring owner; a cleanup worker cannot merely observe no references
and race a new command adopting a candidate key. Resolve already pending/ambiguous
commands through the existing publication protocol before sealing their scope.
Once an owner is published, do not allow a second terminal command to adopt one
of its leftover retries. Expiry adoption is a conditional maintenance transition,
not an inference from current time plus row absence on one replica.

### Successful publication, retention and cleanup cost

Do not erase a window when one slot commits. The initial low-barrier candidate
retains the owner/windows alongside the published payload, so finalization does
not wait for cutoff and does not add a closure command. Unselected attempts can
be cleaned after closure, but **a selected live or pending reference is never
classified as an orphan**. CompleteMultipartUpload must preserve the owner link
when parts become object segments; overwrite, version removal, MPU abort and
payload reclaim must transfer it before removing the old manifest. Record these
links explicitly rather than reconstructing an owner from an irreversible hash.

This conservative choice retains metadata and historical route references for
potentially long-lived objects. It is not the final space optimization. Early
retirement requires transferring a durable closed-write proof to surviving
payload metadata and preserving any other cleanup/release obligations in one
object-PG transaction. Prefer piggybacking that on a later eligible mutation;
if pressure requires a standalone batched maintenance command, count its commits.
Do not claim zero eventual overhead, or start a new retirement command per
healthy PUT by default. Actual unlink directory syncs remain DUR-3 work.

Cancellation/crash cleanup retains the anchor until exact files **and** ack rows
have been removed at all historical participants and its resource releases are
confirmed. Retries may repeat safe deletes/releases instead of persisting one
receipt per key; after restart they must re-establish closure, not trust a lost
process-local observation. Workers must be budgeted and allow unrelated owners/
PGs to progress when one participant is unavailable. Explicit cleanup-complete
still means physical completion; a request which can only hand off cleanup must
not report that stronger result.

Retaining published window evidence does not extend a bucket reservation's live
lease until object deletion. Preserve existing terminal release timing and exact
identity checks; only unresolved release responsibility remains outstanding. A
retained proof can be rechecked/released idempotently and compacted with later
owner work. Distinguish that conservative retained evidence from live mutation
authority, and retain its historical release route until confirmation.

Route retention must include all window data-PG epochs (even without manifest or
ack rows), owner/recovery object-PG epochs, and each exact bucket-PG reservation
epoch until release is confirmed. Update both summary and exact-reference paths
in [`pg_store.rs`](../crates/storage/src/pg_store.rs), including pending commands
which introduce future windows. Existing live-segment and reservation queries
do not automatically cover this anchor. Migration/backfill/export must preserve
these rows and their references before a replacement can use them.

### Remaining protocol gates and next implementation slice

The row layout does **not** yet authenticate a remote write grant. Before changing
production shard RPCs, specify one exact validation protocol. The baseline to
evaluate is read-only, scoped confirmation of an applied/usable owner window by
its authoritative metadata route, with bounded caching only until that immutable
grant's cutoff. This costs RPCs/availability, not inherently another fsync.
Transport credentials or a caller-provided interval alone are not evidence.
No new signing key/certificate scheme is assumed. Confirmation must not hold a
data-PG lock across the object-PG RPC; physical admission must recheck the bound
under its local exclusion lock after confirmation and before mutation.

Both file and ack owners use the existing portable **absolute conservative
wall-clock upper bound**, shortened for supported inter-host skew and bound once
to the receiver's monotonic clock. Never transmit a remaining duration or absolute
monotonic value. Intersect grant, request and receiving-route authority. Cached
confirmation cannot extend any bound; early cancellation cannot make already
dispatched grants vanish. Cleanup waits out the full durable cutoff and drains
both independently admitted effect classes, including after reconnect/restart.

Next implement a small **storage-owned owner/window state-machine slice** at the
existing reserve/create/append/apply boundaries, before granting any new remote
write authority. Its review must include schema, command and export/digest
membership together, not just new structs. First canaries:

1. Reserve then crash before session creation: empty physical scope, exact retained
   release obligation on another replica.
2. Two preparations and a lost reply: replica keeps the reserved end while its
   consumed floor is stale; no replacement or same-node/new-authority reuse.
3. Append a non-final slot: next interval starts beyond the **entire** old block;
   replay preserves both blocks without duplicate allocation.
4. Abort/finalize/overwrite and complete-MPU: session deletion never cascades away
   old retry scopes; selected live and pending references remain exact.
5. Invalid/crossed window, participant, cutoff or resource identity: reject before
   allocation, command installation or apply mutation, including decoded recovery.
6. Two route transitions plus object-PG export/reopen: every pre-append placement
   and historical bucket-release route remains recoverable.

Choose `B`, finite grant/progress limits and the remote confirmation protocol
before enabling physical writes through these fields. Do not expose dormant
fields as an already-enforced capability or enable the six ignored regressions
until the end-to-end exclusion/recovery contract is implemented.

All eventual format work follows [`versioning.md`](../guides/versioning.md) and
the [format ledger](../guides/storage-format-ledger.md): SQLite catalogue,
affected command and RPC grammars, digest membership, snapshot/export containment,
and append-only old/new version-vector evidence. Do not preassign version numbers
in this design. Existing publisher classes remain authoritative; exceptional
window/retirement publishers require explicit registry classification, exact-match
and one-contender retry semantics, finite work budgets and recovery tests.

Review-slice validation: source/transaction audit and plan changes only; no
production behavior or format changes. Full Nextest passed all 9,035 tests with
six existing ignored regressions in 246.024 seconds. Strict workspace Clippy,
storage boundary checks, formatting and `git diff --check` passed. These results
are regression checks, not implementation evidence for the proposed records or
remote grant protocol.

## Pre-Implementation Command Evidence — 2026-10-29

Before adding owner/window fields, preserve the current command corpus at a
separate unchanged-writer commit boundary, following
[`versioning.md`](../guides/versioning.md#commit-boundaries-are-evidence-boundaries).
The existing checksum guards are valuable evidence, but several historical
tests reconstruct bytes from current command structs. An incompatible field
addition cannot use that technique to preserve the predecessor grammar without
retaining a second encoder. Independent bytes avoid that dependency.

This slice freezes the 30 existing object/session specimens for command v11 /
SSE-S3 state v5, checks their exact bytes against the unchanged writer and their
unchanged checksum constants, and independently exercises both decoders on the
frozen inputs. The [format ledger](../guides/storage-format-ledger.md#cleanup-owner-transition-baseline-2026-10-29)
records the file digest, scope and future rejection treatment. The current PG
schema v9 already has its independent complete catalogue; neither its version
nor the command/RPC/digest versions advance here.

This is a prerequisite evidence slice, **not the owner/window state-machine
implementation**. No owner rows, grant issuance, cleanup worker, physical fencing
or new production dependencies are added. It does not resolve the six ignored
regressions or settle remote confirmation and finite policy bounds. After this
baseline is committed, proceed with the reserve/create/append transaction slice
and its containing-format evidence together; retain these bytes unchanged as
rejection evidence when advancing the command format.

Validation: both focused writer/independent-reader tests passed. Full Nextest
passed all 9,036 tests with six existing ignored regressions in 254.679 seconds.
Strict workspace Clippy, storage boundary checks, formatting and `git diff --check`
passed. No production schema or encoding changed.

## Checked Local Allocation Foundation — 2026-10-29

The first production component is deliberately smaller than the complete
reserve/create/append owner integration. The private
[`segment_vid_window.rs`](../crates/storage/src/pg_store/metadata/segment_vid_window.rs)
implements checked finite local VID intervals and is now used by the existing
single-VID preparation path. It is **not a replicated window reservation or a
write capability**. No production block size, issuer grant, cleanup cutoff or
new schema field is selected by this step.

Both bounds of `[start, end)` must fit the SQLite signed-integer domain, because
`end` is itself the persisted next cursor. The SQL update checks that condition
before mutation; Rust projects the resulting checked interval. Normal preparation
still consumes exactly one VID with one `UPDATE ... RETURNING`, without an added
pre-read, transaction, commit or RPC. An enclosing apply transaction remains in
charge of its commit/rollback. Local allocation still cannot establish replica
discovery or authorize replacement-primary continuation.

The implementation also fixes a statement-completion hazard: reading only the
first `RETURNING` row can report success before SQLite completes the statement.
The new path steps to completion and propagates a late failure instead of dropping
the statement and losing its error. An owner-local trace/interrupt regression
first demonstrates that SQLite returns a candidate row and then rolls back on
the next step; the production allocator must return an error and preserve the
complete session. This is not a simulated power loss or proof against every
filesystem/commit failure.

Eight focused tests cover interval arithmetic, signed/unsigned overflow,
all-or-nothing exhaustion with complete same-PG session canaries, local-store
reopen, joining and rolling back an outer transaction, unchanged missing/terminal
session errors, the actual post-row interruption, and SQL tracing of the
single-statement successful path. Metadata-only session fixtures write no shard
payload. The reopen test closes the original store before reopening it; it does
not introduce a second runtime over live files. The tracing assertion pins SQL
statement count, not a measured syscall/fsync count.

The full owner/window schema, command and export/digest integration remains next.
Its replicated reserved end must remain distinct from this consumed local cursor;
it cannot substitute this helper for the owner/issuer/placement/deadline checks
specified above. No ignored cleanup regression is enabled by this slice, and the
current format versions remain unchanged.

Validation: all eight focused allocator tests passed. Full Nextest passed all
9,044 tests with six existing ignored regressions in 252.299 seconds. Strict
workspace Clippy, storage boundary checks, formatting and `git diff --check`
passed.

## Checked Replicated Append Floor — 2026-10-29

Reviewing the next owner/window integration exposed a prerequisite defect in the
existing replicated append path. A selected VID of `i64::MAX` fits the segment
row, but its next cursor does not fit SQLite. The old floor update checked only
unsigned overflow and cast the result to `i64`: the resulting negative value
left the old floor unchanged. A focused regression against the old implementation
confirmed that application nevertheless succeeded and advanced the applied log
to index 1. Local allocation's check alone did not protect decoded/recovered
commands.

The checked interval is now a private-field value used by both local allocation
and append application. Append constructs it before any segment mutation; floor
advancement accepts that checked value rather than independently performing
unchecked signed conversion. Both fresh insertion and matching-segment replay
use the same validated end. This establishes arithmetic validity, **not** owner,
issuer, placement, deadline or replicated-window authority.

Four added owner-local regressions cover:

- canonical decoded commands at the signed boundary and beyond it: exact
  invariant error, no segment/floor SQL writes, complete session canaries,
  unchanged digest and log; a valid command proves the SQL observer sees writes;
- a correctly checksummed malformed pending command reopened twice: the shared
  apply-and-record path rejects it without changing the slot, sessions or log;
- the last representable allocation, its persisted exhausted cursor, matching
  segment replay and recorded-command replay across reopen; and
- a primary with a locally consumed cursor ahead of the selected VID and a
  replica with the initial cursor: append/replay raises but never rewinds either
  floor, preserving their existing distinction rather than claiming discovery of
  unreplicated allocations.

These are metadata-only fixtures; no synthetic ciphertext is exposed as a
readable payload. The change adds no SQL statement, transaction, commit, RPC or
format revision. The retained owner/window reserve/create/append integration
remains next, with the format/export/digest and remote-authority requirements
above unchanged. The six ignored cleanup regressions remain unresolved.

Validation: all 12 focused allocator/append tests passed. Full Nextest passed
all 9,048 tests with six existing ignored regressions in 250.601 seconds. Strict
workspace Clippy, storage boundary checks, formatting and `git diff --check`
passed.

## Stream Creation Transaction Refinement And RPC Evidence — 2026-10-29

The next transaction audit found a smaller candidate for the stream pre-session
gap. Today streamed PUT and UploadPart publish a standalone
`ReserveObjectGeneration` and then `CreateStreamUpload`:

- [`multipart_operations.rs`](../crates/storage/src/cluster/request_ops/multipart_operations.rs)
  calls `reserve_put_object_generation_with_route_validation` before building
  session creation in its PUT and UploadPart workflows;
- [`payload_operations.rs`](../crates/storage/src/cluster/payload_operations.rs),
  `create_put_object_stream_session_record_under_reservation`, does the same;
- [`local.rs`](../crates/storage/src/node_client/local.rs),
  `build_create_stream_upload_command_for_route`, checks that the reservation
  exists but does not put its exact generation into the creation command; and
- [`command_mutations.rs`](../crates/storage/src/pg_store/metadata/command_mutations.rs)
  already reserves an MPU's generation and inserts its upload record inside one
  transaction in `create_multipart_upload_explicit`. Stream creation does not
  yet use that pattern.

**Recommended next implementation:** make the stream creation command carry the
exact selected payload generation and apply its reservation/high-water update
alongside the session in the existing apply/log transaction. Remove the preceding
standalone stream reservation command once the builder, apply, matching, recovery
and all production callers use that binding. This can remove one command from
healthy stream creation without adding a new durability barrier. It does not
remove Direct PUT's reservation command or claim a measured fsync reduction yet.

This refines the earlier two-command stream candidate: the intended new command
has no durable generation-only gap requiring a separate pre-session stream owner.
The bucket-PG proof is still acquired separately, so failed/ambiguous creation
must retain its existing exact release/convergence semantics. Retained owners and
windows must still outlive successful session removal; atomic creation does not
solve delayed physical writes, unused retry discovery, or Direct PUT cancellation.

The implementation gates are:

1. Select the generation under the authoritative object-PG lock before command
   construction; do not perform an unrecorded generation allocation or add a
   pre-install durable effect. Keep captured deadline validation at the existing
   builder/installation boundaries.
2. Apply validates the target, exact bucket-proof subject, session and selected
   generation before committing the reservation, high-water, session and applied
   log record together. Rejected commands leave all of them unchanged, including
   when invoked through decoded recovery. Replicas use immutable command/row
   evidence rather than local clocks.
3. Matching and replay bind the generation as well as the session/encryption/
   cleanup deadline. Contention rebuilds from fresh authoritative state; it must
   not adopt an unrelated pre-existing reservation merely because its session ID
   matches. Preserve one-contender draining, work budgets and immutable admission.
4. Request failure before installation leaves neither generation reservation nor
   session. An ambiguous or irrevocable installation converges the exact command;
   cleanup must not release a reservation that this command still owns.
5. Cover PUT and UploadPart over embedded and installed Unix paths, two concurrent
   same-key sessions, failed apply/rollback, exact replay, replica application and
   reopen. Prove both the generation and session are present/absent together and
   count the actual healthy-path commands before claiming the cost reduction.

The command needs a containing metadata-command version advance. The raw
`CreateStreamUploadCommand` embedded in `StorageRpcStreamUploadMatchRequest` is
separately governed by the RPC frame version; it is **not** protected by the
metadata-command envelope's marker. Changing that raw field or the creation
precondition grammar also requires an RPC version advance. Reusing existing
reservation/session tables alone would not change their schema or digest grammar;
later retained-owner/window fields still require the full format review above.
Recalculate affected framed request/response limits for the added fields and pin
exact-at-cap admission; a command that fits its own limit must still fit its RPC
carrier, including request-only overhead.

Before either advance, this evidence-only slice freezes 35 complete current frames
at `(RPC v27, metadata-command v11, SSE-S3 state v5)` in
[`stream_creation_rpc_v27_command_v11_sse_s3_v5.hex`](../crates/storage/src/storage_rpc/testdata/stream_creation_rpc_v27_command_v11_sse_s3_v5.hex).
The previous exact RPC example pinned only a missing expected command. The new
corpus pins:

- PUT and UploadPart match requests with no expected command, or a complete
  expected command with either cleanup-deadline arm;
- every current stream-creation precondition variant, both reservation booleans,
  absent/live/delete-marker current-object alternatives, and both deadline arms;
- versioned creation-command build responses for both targets/deadline arms,
  stale/missing/conflict outcomes, true/false match responses and error envelopes.

Separate tests compare writer bytes and decode the literal frames directly, with
exact named-inventory comparison and duplicate rejection. The fixtures are
metadata-only: synthetic encryption material is not used to write or read shard
payload. This is targeted stream-creation grammar evidence, not a claim that every
unrelated RPC field combination is covered. The temporary capture helper is not
retained; future changes must append version-indexed evidence, not regenerate
this file. No production behavior, owner record, format version or ignored
regression changes in this slice.

Validation: both focused format tests passed; the full `cargo nextest run
--offline` passed 9,050 tests (one slow, six skipped) in 253.000 seconds.
`cargo clippy --offline --all-targets --all-features -- -D warnings`,
`./scripts/check-storage-cluster-boundaries`, formatting and `git diff --check`
also passed. The six skipped cleanup regressions remain unresolved.

## Atomic Stream Generation And Session Creation — 2026-10-29

Implemented the stream-creation refinement above. The scoped builder selects the
next payload generation read-only while holding the authoritative object-PG lock,
rejects a pre-existing reservation under the requested session ID, and includes
the selected generation in `CreateStreamUpload`. Application validates the target
and commits the generation reservation/high-water, session and applied command log
in the existing transaction. Both PUT and UploadPart replicas consume the same
immutable generation; neither allocates independently or consults a lease clock
during apply. Exact session matching/replay also checks the durable reservation
against that command generation.

Apply also requires reservation absence under the same transaction when the
session is absent. A decoded/recovered creation cannot adopt an independently
owned reservation even when every subject field and the generation match;
idempotence is restricted to the existing exact-session branch. The regression
seeds a separately applied reservation, decodes the creation envelope, and proves
PUT/UploadPart rejection preserves the complete checkpoint, reservation, high-water,
replica state and log, including after reopen. A positive canary exercises exact
session idempotence directly rather than relying only on the applied-log shortcut.

The four stream-creation workflows no longer publish a preceding
`ReserveObjectGeneration` or release that nonexistent reservation on a failed
build/install. Bucket-write proofs retain their existing release versus
pending-command-convergence handling, captured effect fences, bounded retries and
one-contender draining. Direct PUT still uses its separate reservation protocol.
The sealed publisher IDs/classes are unchanged. Healthy stream creation now uses
one object-PG command instead of two; this is a command-count assertion, not a
measured fsync claim.

Evidence includes:

- owner-local PUT/UploadPart rollback at session insertion and transaction commit,
  exact replay, substituted-generation rejection, independently decoded replica
  apply, and reopen; generation, session, high-water and log move together;
- a scoped builder canary proving selection is read-only and refuses an existing
  reservation before command-ID allocation, plus exact/crossed matching;
- a three-replica production cluster with four distinct same-key PUT/part sessions,
  exact one-command creation counts and identical reservation/session ownership;
- an installed-Unix production path for PUT and UploadPart, one-command counts,
  exact replay matching and substituted-generation rejection;
- coordinator CopyObject/UploadPartCopy epoch-change canaries asserting the exact
  two-command prefix (combined creation, append) and one terminal publication;
  stream PUT begin succeeds despite a standalone-reservation failure injector,
  advances one command, and exposes both the session and its reservation;
- updated contention, snapshot retry, authorization/deadline and heartbeat
  fixtures without synthetic pre-session reservations; and
- command v12 / RPC v28 append-only format evidence, predecessor rejection,
  and exact-cap admission. The existing SQLite/digest/checkpoint/encryption
  representations do not change. See the
  [format ledger](../guides/storage-format-ledger.md#atomic-stream-creation-format-vector-2026-10-29).

This closes only the generation-only pre-session gap. It does not retain an owner
after session removal, fence delayed physical writes, discover a lost Direct PUT
release, or solve replacement-primary/window-grant acquisition. The six ignored
regressions remain unresolved. Next integrate the selected retained owner/window
contract into existing transactions, with replay/export/replica evidence before
enabling the cleanup worker.

Validation: the full `cargo nextest run --offline --no-fail-fast` passed all
9,058 tests (one slow, six skipped) in 263.014 seconds after the apply-ownership
review correction. Strict workspace Clippy,
storage boundary checks, formatting and `git diff --check` passed. The initial
full run identified four fixtures still assuming standalone stream reservation;
the corrected run retains their exact command-prefix, error-mapping and wrong-PG
canaries. The stricter apply check also caught an old-epoch heartbeat fixture that
pre-reserved its stream generation; it now lets creation establish the reservation
and verifies that ownership before continuing its heartbeat assertions. The three
focused atomic-creation tests and the corrected full suite pass. No failure was
ignored or retried without a corresponding correction.
The six skipped cleanup regressions are unchanged and remain unresolved.
