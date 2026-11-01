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
removed; no retained ownership tables, worker, staging tickets or physical-grant
RPC operations have landed. The six cleanup regressions remain unresolved.
The [checkpoint evidence prerequisite](#checkpoint-evidence-before-ownerwindow-state--2026-10-30)
preserves literal current-format bytes before changing state-table membership.
The [confirmation and issuer-lifetime refinement](#window-confirmation-and-issuer-lifetime--2026-10-30)
defines the remote authority handoff and adds bounded counterexample checks;
these are design/model evidence, not implemented grants or schema integration.
The [settled-prefix review](#settled-prefix-confirmation-decision--2026-10-30)
selects materialized-window confirmation rather than indefinitely retaining an
introducing-command receipt, and pins the existing compaction/import behavior.
The [local lifetime implementation](#pg-allocation-lifetime-hooks--2026-10-30)
now tracks fresh PG-open and transfer-replacement lifetimes without persistent
writes. Fresh Direct PUT/stream builders and append preparation reject retired
lifetimes. The subsequent [durable stream issuer binding](#durable-stream-allocation-issuer--2026-11-01)
also prevents a replacement lifetime from allocating for an old stream. Physical
write/cleanup exclusion and retained owner/window state remain open.
The subsequent [storage-node route-publication hook](#storage-node-route-publication-lifetimes--2026-10-30)
handles issuer replacement on installed storage-node route changes. Embedded
cluster publication is covered by the subsequent embedded-lifetime slice;
durable window/allocator binding remains outstanding. The
[storage-owned extent limits](#storage-owned-stream-extent-limits--2026-10-30)
enforce the selected per-attempt segment bound before preparation allocation
and central append application, without adding owner/window rows yet.
The [single outstanding preparation slice](#single-outstanding-local-preparation--2026-10-30)
now bounds local speculative allocation as well. This is not replicated window
authority or permission for a replacement primary to reuse a slot.
The [creation-origin and placement-input audit](#creation-origin-and-placement-input-audit--2026-10-30)
closes decoded creation's unchecked initial cursor and specifies the captured
placement input needed before adding durable first-window records.
The [shared placement resolver](#shared-placement-resolver--2026-10-30) extracts
the existing computation for reuse by installed builders; captured placement
authority and durable descriptors are not implemented by that extraction.
The [captured creation inputs](#captured-placement-inputs-for-stream-creation--2026-10-30)
extend placement validation to generation-selecting stream creation builders.
They remain computational inputs, not durable first-window authority.
The [integration boundary audit](#ownerwindow-integration-boundary-audit--2026-10-31)
pins the remaining primary-builder inputs and resource-release responsibilities
for the next coordinated durable state slice; route-local lifetime capture is
not a substitute for persisted window/issuer binding.
The [primary-owned append builder](#primary-owned-stream-append-construction--2026-10-31)
now supplies the actual-primary construction boundary needed by future window
issuance, without adding owner records or changing durable command semantics.
The [explicit reservation disposition](#explicit-command-reservation-disposition--2026-10-31)
unifies current foreground/restart release policy before adding durable obligations.
The [Direct PUT primary proof input](#primary-direct-put-reservation-proof-input--2026-10-31)
carries and checks the reservation subject at the builder; durable proof ownership
and the owner/window state remain part of the coordinated integration.
The [append placement input](#captured-placement-input-for-append-construction--2026-10-31)
checks fresh append placement against durable PUT/MPU generation under the primary
lock. This remains preflight, not persisted window authority.
The [primary preparation check](#primary-append-preparation-binding--2026-10-31)
also rejects unallocated or crossed local slots before fresh command construction;
it does not bind a persisted window to its issuing lifetime.
The [Direct PUT handoff audit](#direct-put-proof-handoff-integration-gate--2026-10-31)
identifies the remaining frontend ownership change that must accompany command
proof persistence. Do not land a proof-only reservation-command change.
The [storage-owned frontend scope](#storage-owned-direct-put-frontend-scope--2026-10-31)
now owns the existing terminal handoff decision instead of a coordinator flag.
It does not move custody to generation reservation or implement durable owners.
The [scope unwind follow-up](#direct-put-scope-unwind-release--2026-10-31) adds
best-effort release for a still-frontend-owned proof when its callback unwinds;
process loss and failed cleanup still require durable ownership.
The [owner-introduction model](#owner-introduction-and-proof-custody-model--2026-10-31)
now covers the pending-command-to-materialized-owner handoff and preservation
of release evidence across compaction/restart. It is abstract model evidence,
not implementation of durable owner/window records or physical exclusion.
The [pre-install retirement contract](#pre-install-orphan-retirement-contract--2026-10-31)
separates reservation expiry from authoritative absence of an accepted
introduction. Its bounded model covers delayed duplicate installation and
inspection under shared serialization; replacement-primary reconciliation
remains an integration prerequisite, not a modelled guarantee.
The [Direct PUT successor-evidence tests](#direct-put-successor-evidence--2026-10-31)
now exercise real pending slots, witness-first application, peering decisions and
compacted generation-reservation transfer. They do not supply a replacement
primary's route certificate or the still-missing retained owner/proof obligation.
The [successor orphan-inspection model](#successor-orphan-inspection-model--2026-10-31)
now explores replacement overlapping an insertion that finishes after the cutoff,
unavailable inspection, and retained custody after compaction/restart. Its
settled-prefix reconciliation is an abstract obligation, not an implemented
replacement protocol.
The [single-release correction](#single-release-for-a-rejected-direct-put-contender--2026-10-31)
removes a redundant generation-release publication after successful budgeted
loser cleanup. It does not change the planned durable custody handoff.
The [participant-capture extraction](#captured-file-and-acknowledgement-participants--2026-10-31)
now returns the shared resolver's actual shard locations and independent
acknowledgement primary. Existing builders still use it only for preflight;
durable owner/window integration remains outstanding.
The [reservation-kind apply guard](#direct-put-reservation-kind-at-apply--2026-10-31)
now rejects a standalone Direct PUT reservation command naming a live stream's
generation. Exact Direct PUT replay/reissue remains supported; this does not yet
persist the retained owner or change the frontend proof handoff.
The [atomic stream cleanup correction](#atomic-stream-creation-and-abort-cleanup--2026-10-31)
removes obsolete standalone generation-release commands from creation failure,
abandonment and post-abort maintenance. Failed creation now preserves independent
reservations; real Direct PUT cleanup derivatives retain their recovery coverage.
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
The [initial policy decision](#initial-window-policy-and-retry-evidence--2026-10-30)
below selects one slot using the production retry-path evidence and a separate
one-slot exploration; enforcement remains part of owner/window integration.
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

1. Direct PUT reserve then crash before staging: empty physical scope, exact
   retained release obligation on another replica. Stream creation is now atomic:
   failed creation must leave neither a new session nor its generation reservation.
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

## Checkpoint Evidence Before Owner/Window State — 2026-10-30

Before introducing retained owner/window tables, preserve the existing checkpoint
specimen as literal bytes at a separate commit boundary, as required by the
[versioning guide](../guides/versioning.md). The current schema-9 catalogue already
has literal evidence; the checkpoint specimen previously retained only its
state-digest/CRC/length/SHA-256 tuple.

The new checkpoint-v6 / canonical-state-v8 / PG-schema-v9 / SSE-S3-v5 fixture
captures the unchanged writer at `144ba8ac4e683642bdf3f0d64754f5b1219454ca`.
Its 7,760 decoded bytes match the original SHA-256
`ecd3abb5de9f324640ced63f47dbebfca63641e5b72e74a375c79e29512f7eda`.
The existing writer test now compares exact bytes, while independent literal-reader
tests verify the complete table catalogue, install the checkpoint through the
storage import path, and reopen it with identical materialized state and encoding.
This is a metadata-only representative with a populated multipart-upload row,
not an exhaustive populated-row or payload-read corpus. All previous evidence is
unchanged. The [format ledger](../guides/storage-format-ledger.md#checkpoint-evidence-before-cleanup-ownership-changes)
records the complete version vector and retention requirements.

Production behavior and format versions are unchanged. The next implementation
slice remains retained owner/window integration into existing transactions, with
coordinated schema, command and export/digest membership and replica/restart tests.
No new ordinary-path commit, remote grant, cleanup worker or skipped-test change
is introduced here. All six cleanup regressions remain unresolved.

Validation: all three focused checkpoint tests passed. The full permitted
`cargo nextest run --offline --no-fail-fast` passed 9,060 tests (one slow, six
skipped) in 278.501 seconds. Strict workspace Clippy, storage boundary checks,
formatting and `git diff --check` passed. An initial sandboxed full run was
stopped after read-only test-directory and socket-permission failures; the full
replacement run used the required permissions without changing those tests.

## Window Confirmation And Issuer Lifetime — 2026-10-30

The next transaction slice needs a concrete answer to the remote-confirmation
gate above. Persisting a window must not accidentally make its allocation
authority reconstructible by every replica holding that row. This refinement
separates three operations before freezing those fields into a schema:

| Operation | Required authority | What it cannot do |
| --- | --- | --- |
| Consume a retry slot | Exact storage-owned allocation lifetime, matching owner/window and local consumed cursor, current request fence and immutable grant cutoff | A replica/restarted runtime cannot derive this authority from node ID, epoch, window row or consumed floor. |
| Confirm an existing physical scope | Exact durable window plus authoritative evidence that its introducing creation/append is usable, through the authorized metadata route | Does not allocate a VID, extend a cutoff, renew a grant, reopen a sealed owner or perform recovery mutations. |
| Admit file or acknowledgement mutation | Exact confirmed descriptor and request subject/geometry, checked at the effect owner under its exclusion guard | Confirmation is not a substitute for local deadline/route checks or for retaining the guard through persistence. |

### Allocation lifetime is not route or node identity

The current [`prepare_stream_segment_append_inner`](../crates/storage/src/node_client/local.rs)
loads the complete session/generation under the object-PG lock and consumes the
local allocator. Preserve that lock boundary when adding window checks. Neither
its persisted cursor nor the replicated reserved end proves exclusive authority
after importing a replica or reopening a runtime.

The proposed issuer binding is the storage node identity plus an opaque,
storage-minted allocation-lifetime nonce, attached to the specific opened PG
incarnation. It is not supplied by the frontend, synthesized from a topology
epoch, or restored from the window row. Minting the local identity is in-memory;
the initial owner/window command records it in the existing apply transaction.
No independently synced nonce file is proposed. Production must use a fresh
128-bit random identity, not the model's small symbolic labels.

Fresh runtime open, checkpoint replacement/import, and reactivation after losing
allocation continuity cannot adopt the old lifetime. The row retains the old
identity for cleanup and confirmation, but the new runtime lacks its local
allocation capability. Ordinary validity-only renewal must not mint a new
lifetime or extend an existing window's cutoff. A replacement which later
returns to the same node ID still cannot resume using an imported floor.

An old issuer may finish consuming an already delegated finite block while its
old lifetime genuinely remains live, within the original cutoff. That permission
does not authorize new blocks or fresh publication after losing its route.
Stopping new frontend preparation on primary change and retaining all previously
issued work is the first implementation policy. The lifecycle hook must retire
new-allocation authority without pretending it has revoked transport requests
already sent. Resumption needs the separately classified exceptional disjoint-
window command; this slice does not implement that continuation.

### Read-only confirmation protocol candidate

Confirmation is a scoped storage-node operation on the owner object PG. Its
request identifies the exact owner revision/window and concrete effect: segment
index/hash/VID, participant/shard index, historical data placement, codec and
bounded geometry, issuer binding and absolute cutoff. Derive the expected
descriptor from the durable window and compare every field, including bucket
incarnation and target. Neither authenticated node credentials nor a caller's
descriptor alone grants physical authority. Response validation must bind the
same complete descriptor to the request before constructing an opaque local
confirmed-scope capability.

The metadata owner must also establish that the window's introducing command is
usable under the existing exact-command publication/recovery protocol. A pending
slot, `publication_started`, or an applied witness row alone is insufficient.
[`metadata_operations.rs`](../crates/storage/src/cluster/metadata_operations.rs)
already distinguishes `Published`, `Witnessed`, `PublicationUnconfirmed` and
`IrrevocableUnconfirmed`; do not collapse those outcomes for window confirmation.
The conservative issuance boundary is completion of the existing successful
creation/append workflow. Lost replies use exact-command matching/convergence,
not another allocation.

For remote reinspection, exact publication evidence (or certified transferred
state covering that introduction) must establish the same usable fact. A
replacement must resolve any ambiguous introducing command through the existing
authorized recovery workflow before confirming it. The window revision/descriptor
must remain stable across a validated same-payload reissue; a changed envelope
log index/checksum is not permission to mint a different window. The concrete
row/certificate relationship, including checkpoint/log-pruning and reissue
evidence, remains part of the implementation review: the model's `Ready` state
assumes this proof, it does not manufacture or validate it.

This RPC is read-only. It cannot implicitly run an all-command drain, allocate,
or add a durability barrier. If proof is unavailable or conflicting, fail closed
within the caller's existing bounded work budget; explicit recovery handles the
obstruction. No fallback to an unrelated current route or unchecked local row.
After an authoritative transition, the successor may confirm a still-covered
old window for delayed physical work; this is distinct from letting that
successor allocate from the old window.

### Confirmation-to-effect handoff

1. Receive and bound the physical request; identify the exact historical effect
   owner. Do not hold the data-PG mutation lock while making confirmation RPCs.
2. Confirm/cache the immutable scope under the rules above. Cache keys include
   the complete descriptor and participant; positive replies are not transferable
   between owners, windows, placements, shards or effect kinds. Negative lookup
   does not authorize mutation. A failed lookup never invents a new deadline.
3. At the file owner, acquire local write/cleanup exclusion and recheck the
   intersection of grant, request and receiving-route authority immediately before
   file mutation. Keep exclusion through file publication and acknowledgement
   production. At the separately admitted acknowledgement-storage owner, repeat
   validation and hold its guard through the acknowledgement transaction.
4. A positive reply delayed beyond its cutoff is rejected even if the sender or
   current raw route has renewed. Transport only the existing absolute
   conservative wall-clock upper bound, shorten for supported inter-host skew,
   and bind once to receiver monotonic time. Neither reconnect nor cache hit may
   restart a duration or rebind a later effective deadline.
5. Sealing/cancellation does not revoke a cached or in-transit grant. Cleanup
   still waits for the immutable cutoff and both independently admitted effect
   classes to drain. An admitted effect may finish after the cutoff while its
   guard blocks cleanup. A request which has not acquired that guard must fail
   after the cutoff, even if cleanup has already removed the owner and payload.

The local guard registry must survive frontend/session disconnection for as long
as its IO task can persist. Its exact runtime/PG identity and shutdown/import
draining are integration obligations. This proposal uses no per-write ticket,
new signing key, sidecar or ordinary-path metadata commit. It does add confirmation
RPCs/cache state and availability dependencies; measure those costs rather than
claiming the protocol is free.

### Bounded evidence and limits

Two independent owner-local models supplement the prior allocation/fanout models:

- [`confirmation.rs`](../crates/storage/src/cleanup_ownership_model/confirmation.rs)
  separates publication readiness, request delivery, positive confirmation,
  guarded local admission, physical persistence, sealing, cutoff and cleanup for
  file and acknowledgement effects. It includes authenticated-but-premature
  requests and a positive reply delayed until after owner removal. Six independent
  unsafe variants omit publication readiness, the file cutoff, the ack cutoff,
  the cleanup cutoff, the file guard or the ack guard; each must produce its own
  counterexample. Explicit traces pin both delayed replies and both guards.
- [`issuer.rs`](../crates/storage/src/cleanup_ownership_model/issuer.rs) separates
  original issuer A, replacement B, A returning with replica state, and A reopening
  with its own persisted floor. Prepared deliveries survive those transitions,
  but allocation authority does not transfer. Independent node-ID-only and
  replica-floor substitutions must fail; a trace also pins actual duplicate
  allocation after A returns. The original live issuer can exhaust only its
  two-slot model window before cutoff.

These are exhaustive finite interleavings with required positive reachability,
not production-connected failover/RPC tests, cryptographic proofs, liveness under
unbounded failures, or a composition of all earlier models. The confirmation
model assumes one exact descriptor, an authoritative usable-publication fact,
a healthy shared cutoff and no selected/pending payload at sealing. The issuer
model assumes a fresh local lifetime on reopen/import. Field substitution,
clock-health/skew, exact route-history authorization, distributed publication,
real filesystem failure and authenticated Unix/TLS response validation still
require implementation tests.

The next production slice remains retained owner/window integration. Before its
schema is finalized, settle the usable-introduction/reissue/checkpoint evidence
and runtime invalidation hooks described here; choose `B` and finite retry/horizon
limits from existing request budgets rather than the two-slot model. No format
version, production grant, cleanup worker or ignored regression changes here.

Validation: all six focused tests passed. The safe confirmation model exhausted
78 states / 144 transitions and the issuer model 80 states / 132 transitions,
including every declared reachability witness; all eight independent unsafe
variants produced their required counterexample. Full
`cargo nextest run --offline --no-fail-fast` passed 9,066 tests (one slow, six
skipped) in 235.323 seconds. Strict workspace Clippy, storage boundary checks,
formatting and `git diff --check` passed. The six skipped cleanup regressions
remain unresolved; these results do not certify the unimplemented protocol.

## Settled-Prefix Confirmation Decision — 2026-10-30

**Selected direction:** confirm the exact materialized owner/window through the
current authoritative object-PG primary at a settled prefix. Do not require its
original introducing envelope or exact terminal receipt to remain available
forever, and do not add a separate durable publication receipt for each window.
The window's immutable descriptor/revision is its identity; log positions are
publication/recovery evidence, not that identity.
Settlement requires matching primary/witness prefix evidence (or the corresponding
certified transfer proof), not merely local row presence and an empty primary slot.

This resolves the previous section's introduction/reissue/checkpoint design
question. It does not implement the confirmation RPC or a new route capability.

### Source-backed reasons

- `compact_metadata_command_log_with_receipt_retention` in
  [`command_log.rs`](../crates/storage/src/pg_store/command_log.rs) removes
  checkpoint-covered log rows and bounds terminal receipts. After both are
  pruned, `applied_metadata_command_log_entry_hashes` returns `None`, even though
  the materialized session/reservation still exists. An old exact-command retry
  correctly fails closed rather than inferring its envelope identity from rows.
- `install_metadata_transfer_checkpoint_base` preserves the checkpoint's complete
  materialized state/digest but installs it at the destination epoch with applied
  index/hash zero. An old command's epoch/index is therefore not a permanent
  lookup key for a transferred owner/window.
- `validate_metadata_command_checkpoint_state_read_only` already requires the
  selected epoch, absence of a pending slot, a valid checkpoint/log suffix, no
  unresolved tail, and the matching state digest. This supplies useful local
  settled-prefix semantics, **not** primary authority or a global publication
  certificate. An applied witness may pass it while the primary still has an
  unresolved slot. It must never be used alone to mint a grant.
- The existing exact-command publication protocol and peering certification
  establish which primary/state may serve. The new owner/window must participate
  in that canonical state, checkpoint and transfer protocol rather than adding
  an independently synced confirmation journal.

### Concrete confirmation rule for implementation

1. Enter through a current active-primary, exact object-PG route capability with
   its immutable admission fence. A retained historical inspection route, a
   standalone witness checkpoint, or possession of a node credential is not
   sufficient. A successor confirms old windows only after its ordinary
   certified transfer/recovery and active-primary admission have completed.
2. Under the existing object-PG/metadata-command serialization boundary, reject
   an unresolved pending slot or unapplied tail before returning a positive
   result. Do not wait for an unbounded all-command drain. A typed retry/unavailable
   result lets the existing bounded publisher/recovery workflow settle it.
   The same rule applies if an unrelated command currently occupies the slot;
   the initial implementation prefers a conservative, simple prefix boundary.
3. Read the exact owner/window and compare all requested subject, scope, resource,
   cutoff, geometry, participant and lifecycle fields in that same protected
   observation. Validate the local replica's epoch/prefix against the admitted
   primary route and obtain matching epoch/index/log-hash/state-digest evidence
   from its designated publication witness, or validate the equivalent certified
   imported-prefix evidence. A singleton route has no independent witness; preserve
   its existing single-node publication semantics. Bind the witness identity to
   the admitted acting set. Identical owner/window rows at different prefixes do
   not suffice. Do not release primary serialization between selecting the prefix,
   checking settlement and capturing the descriptor. The bounded metadata-witness
   inspection runs while primary serialization remains held, following the existing
   held-primary inspection order; no data-PG/physical lock is held across that
   network call. Replica apply of future owner/window commands
   and checkpoint semantic validation must enforce the immutable relationships.
4. Return an opaque confirmed physical scope bounded by the descriptor's original
   cutoff and the request/receiving-route fences. The later file/ack owner still
   performs its own guarded admission check. This result does not let the caller
   allocate another VID or reconstitute an issuer lifetime.

Use the existing settled-prefix invariants, not full checkpoint generation or a
complete table scan on every confirmation. The implementation must demonstrate a
bounded lookup using owner-local validated/cached prefix state, plus a no-write
canary. Holding metadata serialization for the lookup adds contention, not a new
commit. The metadata-witness inspection is the permitted network call under
primary serialization and must respect the captured deadline and work budget.
Release primary serialization after capturing the confirmed descriptor, before
physical IO or data-node calls. No data-PG or physical-effect lock may be held
across a network call.

Same-payload reissue must preserve the owner/window descriptor. An old pending
command is still resolved using its exact authorized recovery chain before the
current prefix can serve; the new confirmation path must not invent a second
reissue validator or treat `None` from an old-envelope lookup as proof of absence.
After authorized transfer, canonical row membership plus the new route's
settled-prefix authority replaces the obsolete log identity for confirmation.
No support for old-format readers or schema migration is introduced.

### Runtime lifetime integration points

The source audit also rules out reusing the existing managed storage-node
incarnation counter as the sole allocation-lifetime proof: it does not describe
every in-process PG replacement/import or every embedded runtime. The nonce
proposal in the previous section remains selected.

- Mint the local PG lifetime on opening its storage-owned runtime/store. Cloning
  `LocalNodeRuntime` shares its `Arc<SharedStorageNode>`; it must not create an
  independent allocator or mint another lifetime for the same live PG.
- Checkpoint-base replacement and
  `adopt_metadata_transfer_state_from_rebased_commands` both invalidate prior
  local allocation authority, including adoption that does not rewrite session
  rows. Perform invalidation under the same PG serialization; on failure, keeping
  old authority invalid is conservative. No post-error path may silently remint
  the old identity from persisted owner/window fields.
- Restart opens a fresh lifetime. Ordinary command apply, heartbeat renewal and
  same-runtime route-view clones do not recreate or extend a grant. Primary-loss
  and subsequent activation must distinguish new allocation admission from old
  bounded in-flight work, as specified above.

At this evidence slice these hooks and their actual runtime/Unix tests remained
implementation work. The subsequent local-lifetime slice implements the store
open/transfer hooks only; neither slice asserts that today's allocator is fenced.

### Production-store regression evidence

[`stream_creation_evidence_tests.rs`](../crates/storage/src/pg_store/metadata/stream_creation_evidence_tests.rs)
adds three tests, each covering PUT and UploadPart:

- Apply real creation and a later command, checkpoint, then run the production
  compactor with a one-receipt retention bound. Assert that the creation's log and
  receipt disappear while the complete session, reserved generation/high-water
  and checkpoint remain exact, including after reopen. An attempted old-envelope
  replay must fail without SQL mutation, slot/log advance or row changes.
- Import that compacted checkpoint into epoch two, assert index/hash zero and
  exact state digest/table blocks, generation and session, then reopen. No
  original envelope receipt is manufactured. This is the production **store
  import** path, not an end-to-end control-plane transfer authorization test.
- Apply creation witness-first with the primary's real pending/publication-start
  state. Pin that the witness can export a valid local checkpoint while primary
  export remains rejected, both before and after primary apply. Rejection does
  not mutate SQL state or clear the publication marker. Only after slot removal
  does primary checkpoint export succeed. Both stores start from the same base;
  prefix proofs differ before primary apply and match afterward. A later witness
  command then proves that exact stream-row equality can coexist with divergent
  prefix proofs. The test does not claim that calling the store's slot-removal
  primitive independently proves distributed settlement.

The fixtures are metadata-only and write no shard payload. The next production
slice is still owner/window schema/command integration with issuer invalidation,
followed by the scoped confirmation and physical-exclusion paths. Finite window
size/horizon policy remains to be selected before write grants are enabled.
No extra ordinary-path commit, production format or ignored-test change lands
in this evidence/decision slice; the six cleanup regressions remain unresolved.

Validation: all three focused store tests passed for both targets. The full
`cargo nextest run --offline --no-fail-fast` passed 9,069 tests (one slow, six
skipped) in 237.810 seconds after the primary/witness-prefix canary was
strengthened. Strict workspace Clippy, storage boundary checks, formatting and
`git diff --check` passed. This validates the existing store behavior used in the
design decision, not an implemented window-confirmation or cleanup protocol.

## PG Allocation Lifetime Hooks — 2026-10-30

The next schema slice needs a concrete source for its issuer binding. Implement
that process-local source first, separately from durable window issuance, rather
than encode node ID or a restored cursor as provisional authority.

[`allocation_lifetime.rs`](../crates/storage/src/pg_store/allocation_lifetime.rs)
owns a private 128-bit random identity per writable `PgStore` open. It uses the
existing crypto provider, not a new dependency, file, row or separately synced
counter. A cloned local node runtime shares the same PG store and identity;
independent PGs, runtime opens and reopen after restart do not. Read-only shard
inventory stores start retired and do not mint an identity.

Four existing store transfer boundaries now retire the previous lifetime before
attempting their transaction: checkpoint-base installation, rebased-command
state adoption, and empty/matching-state initialization. The last two matter even
when the materialized session rows are unchanged. Static input rejection (such
as a wrong-PG checkpoint) can return before a replacement attempt begins.
A private replacement guard
holds a newly generated candidate; successful commit and digest validation must
both finish before it becomes active. Entropy is acquired before SQL mutation,
not after commit. Once replacement begins, error/rollback or dropping the guard
leaves the lifetime retired; retry may establish a fresh identity but cannot
revive the previous one.
An older superseded guard cannot activate or retire its successor's identity.
The existing PG serialization boundary remains required around these methods.

No schema, command, RPC, canonical-state or checkpoint bytes change. There is no
additional SQL statement or commit for lifetime tracking. Existing command apply,
heartbeat inspection and log/receipt compaction leave it unchanged. This slice
does not attach an unbounded registry to sessions or retain abandoned requests in
memory.

### Coverage and explicit limits

Owner-local tests cover both PUT and UploadPart sessions across reopen,
checkpoint replacement, rollback after destination-table clearing, and rebased
adoption without session-row replacement. They compare complete checkpoints or
session records, not just existence. Separate canaries cover shared runtime
clones, foreign PG/runtime identities, empty/matching initialization, a deferred
constraint failure at COMMIT, and superseded/dropped replacement guards.
Heartbeat reads assert no SQL mutations. Reopen and same-state retry assertions
pin that the local nonce does not enter canonical state or checkpoint evidence.

**This remains a prerequisite, not an allocation fence.** No allocator, command
builder or physical write consumes the identity yet, and no downstream API can
mint grants from it. Still required:

- persist and compare the issuer binding in the exact owner/window descriptor;
- connect primary loss/reactivation to retirement under route/PG serialization;
- enforce checked finite window bounds at allocation and preserve them on replicas;
- implement settled-prefix confirmation and independently guarded file/ack effects;
- integrate retained cleanup and enable the six unresolved regression schedules.

Choose finite window/horizon policy before the durable window format. Integration
must preserve existing fsync boundaries and immutable format evidence; this slice
does not change the no-extra-ordinary-path-commit acceptance target.

Validation: all nine focused lifetime tests passed. Full
`cargo nextest run --offline --no-fail-fast` passed 9,078 tests (one slow, six
skipped) in 250.827 seconds. Strict workspace Clippy, storage boundary checks,
formatting and `git diff --check` passed. The six skipped cleanup regressions
remain unresolved; no write grant or window-enforcement completion is claimed.

## Storage-Node Route-Publication Lifetimes — 2026-10-30

The schema review exposed another lifetime boundary which must be implemented
before a durable descriptor can rely on the local issuer nonce: a still-open
storage-node process can lose and regain primary authority without a checkpoint
import or restart. This slice implements that installed-server boundary; it does
not yet add owner/window rows or make the nonce sufficient write authority.

`StorageNodeServer::install_control_plane_runtime_config` now compares the complete
old/new current-route descriptors by PG, after admission has drained and while
holding route-state publication serialization. Changed descriptors replace the
affected PG lifetimes. Global epoch or default-EC changes also invalidate them,
including configurations using implicit routes. Same-node Active/Peering/Active
and primary-away/primary-back cycles cannot restore an earlier identity.

The storage-owned node helper acquires affected PG mutexes in sorted, deduplicated
order. It retires their old lifetimes and holds both the PG locks and opaque
replacement guards through **local** runtime-config publication. Successful
publication completes the fresh candidates before the new in-memory route is
exposed. Failure drops the guards and leaves the old identities retired. The
callback performs no network IO and must not acquire PG locks itself. This adds
no SQL operation, format field or ordinary-path durability barrier; it uses the
existing config staging, rename and directory sync.

Healthy validity-only renewal remains memory-only and does not acquire PG locks
or drain admitted frames. Unchanged routes, history-only edits and exact repeat
installs preserve their identities. There is one deliberate exceptional path:
failed publication records the affected PG IDs in a bounded process-local pending
set. The next successful install replaces those identities even if the control
plane abandoned the candidate and merely renewed the old topology. Such a retry
takes the full publication barrier instead of leaving those PGs retired forever
through the renewal fast path. Success clears the pending set; restart opens
fresh lifetimes, so this set needs no durable sidecar or recovery record.

### Regression scope

- Installed-server tests independently change state, acting set and primary
  within one epoch, then move the primary away/back across epochs. An unaffected
  PG is a positive preservation canary, and checkpoint equality pins that the
  nonce changes do not mutate durable PG state.
- Validity renewal, history-only changes and repeated installation preserve the
  lifetime. Existing memory-only renewal coverage remains applicable.
- A staged-file removal forces publication rename failure after staging. Both
  retrying the candidate and renewing the old topology must establish fresh
  identities without reviving the retired ones; a later healthy renewal must
  preserve the recovered identity.
- A held admitted frame and an observed `Draining` state prove that replacement
  waits for admission drain, rather than invalidating a still-running frame.
- An owner-local helper test checks that all affected PG mutexes remain held
  through the publication callback, an unrelated PG stays unlocked, and callback
  failure retires only the affected lifetimes before retry.

**Still open:** the analogous process-local `StorageClusterRouteHandle` publication
hook; durable owner/window fields and finite window/horizon policy; comparison of
the captured issuer at allocation; settled-prefix confirmation; and independent
file/ack exclusion with retained cleanup. An active local nonce is not an Active
PG role, a settled-publication proof, or a physical write grant. None of the six
ignored cleanup regressions is enabled or claimed fixed by these hooks.

Validation: all 15 focused lifetime/publication tests passed. Full
`cargo nextest run --offline --no-fail-fast` passed 9,082 tests (two slow, six
skipped) in 273.371 seconds. Strict workspace Clippy, storage boundary checks,
formatting and `git diff --check` passed. The six skipped cleanup regressions
remain unresolved.

## Embedded Route-Publication Lifetimes — 2026-10-30

The corresponding embedded runtime-map publication hook is now implemented in
`StorageClusterRouteHandle`. Candidate construction alone does not alter live
issuers. After frontend admission drains and the expected-current/epoch/validity
checks succeed, publication compares the old and new maps' complete PG routes,
global epoch and EC configuration, and embedded node runtime/endpoint/incarnation
bindings. It replaces affected issuers on **both** sides, including removed nodes
and genuinely different runtime instances with the same node IDs. Unchanged
routes and healthy validity-only renewal preserve their identities.

`LocalNodeRuntime` owns the batch operation. It sorts by PG ID and process-local
node identity, deduplicates shared runtime clones, acquires all affected PG
mutexes, and holds opaque replacement guards across the local publication
callback. The callback rechecks candidate validity after lock acquisition, then
updates pinned-generation leases and the current map. It performs no node access
or network IO. The map write lock and publication barrier remain held until fresh
lifetimes are completed. This adds no SQL transaction, persistent representation,
or fsync, and never reopens stores.

Failure after retirement leaves the affected issuers retired and records a
shared process-local retry flag. Status-only renewal then requests a full map
instead of extending the old lease. The next successful full publication resets
the embedded issuers, even if the control plane abandoned the candidate and
renewed the original topology. This exceptional retry conservatively resets all
embedded PGs in the two maps; subsequent healthy renewals preserve the recovered
identities. Restart needs no flag persistence because it opens fresh lifetimes.

Unix/TCP endpoints and topology-only frontends are excluded from embedded
replacement. Their actual storage-node server owns lifetime transitions; a
frontend's shadow node stores must not be mistaken for those servers.

### Regression scope

- State-only, primary-only, and epoch changes replace the relevant lifetimes;
  candidate construction, unchanged renewal, and an unrelated PG are preservation
  canaries. Active/Peering/Active cannot resurrect the earlier identity.
- Publication covers both replaced runtime instances and nodes removed from the
  candidate. Unix and TCP shadow runtimes retain their identities.
- Observed admission draining precedes replacement. A stale expected-current
  candidate and a candidate already expired before replacement leave identities
  unchanged.
- A deterministic hook expires the candidate after replacement begins. The old
  map remains installed with retired affected issuers; status-only renewal cannot
  conceal that state, and full renewal of the original topology recovers fresh
  identities.
- The owner-local batch test checks actual mutex exclusion, duplicate shared
  runtime targets, callback failure, retry, an untouched PG, and exact durable
  checkpoint preservation.

**Still open:** durable owner/window descriptors and finite window/horizon policy;
captured-issuer comparison during allocation; settled-prefix confirmation; and
independent file/ack exclusion with retained cleanup. These lifetime hooks still
do not authorize allocation or physical writes. The six ignored cleanup
regressions remain unresolved.

Validation for this embedded publication slice: all five focused publication
tests passed; the full suite, including the additional owner-local batch test,
passed 9,088 tests (one slow, six skipped) in 269.495 seconds using
`cargo nextest run --offline --no-fail-fast`. Strict workspace Clippy, storage
boundary checks, formatting and `git diff --check` passed.

## Initial Window Policy And Retry Evidence — 2026-10-30

With local lifetime invalidation implemented, select the initial policy before
encoding owner/window records. The goal remains no additional ordinary-path
metadata command or fsync. This slice supplies policy and executable evidence;
it does **not** change the production allocator, timeouts, or persisted formats.

### One physical allocation per segment window

Use **`B = 1` initially**, for both the first stream window and each append's
lookahead window. Direct PUT already needs one exact key per shard. Do not reserve
four/eight speculative VIDs merely because publication has a retry loop:

- `prepare_stream_segment_append_with_route_validation` retries unrelated pending
  commands **before** invoking the preparation RPC. It returns or propagates that
  RPC's result; it does not repeat successful or uncertain allocation.
- `append_stream_segment_with_route_validation` prepares once, writes the selected
  shard identities, and gives the same record to the append publisher. Command-ID
  contention/reissue retries publication, not physical allocation.
- PUT, POST, UploadPart and copy ingestion feed segments sequentially. A failed
  append is handled as failed/uncertain work, not permission to encrypt or allocate
  another physical candidate under the same segment index.

The new production-workflow tests in
[`cleanup_discovery.rs`](../crates/storage/src/cluster/local/tests/cleanup_discovery.rs)
force a command-ID collision after shard preparation for both streamed PUT and
UploadPart. Each requires at least two command builds but exactly one preparation,
one consumed VID on the primary and replicas, one identical committed segment,
and exact ciphertext reconstruction. These are evidence for publication retry
behavior, not proof that today's allocator enforces `B = 1`.

An ambiguous/lost **preparation** response is different from a lost append reply.
Without exact response evidence the caller must fail the attempt and hand off
cleanup, not obtain a second slot or reuse the first identity. A committed append
with a lost reply still converges through exact-command inspection; its already
reserved next window is not allocated again. Exceptional disjoint windows and
replacement-issuer continuation remain deferred. The existing adversarial tests
which prepare multiple candidates describe today's behavior; window enforcement
must migrate them to explicit rejection or an owner-local malformed-state fixture,
not silently widen the production policy to keep those fixtures working.

Cleanup enumeration is then one candidate per shard per window, rather than
`B * (k+m)` candidates. With 2+1, increasing B from one to eight would increase
potential file probes from three to twenty-four for each window, even if only
one candidate was ever written; acknowledgement checks are additional. This is
a bound on enumeration work, not a measured RPC count or latency improvement.

The finite lookahead model now explores **both** one-slot and two-slot initial
windows. Each checks the same safety properties and positive reachability, and
each independent unsafe mutation must still produce its own counterexample.
An ordered one-slot trace pins exhaustion before append, withholding the next
window until settlement, and preserving the next window across a lost reply.
Its keys are symbolic (the next window's representative is still key 2), not a
production VID layout. It retains the prior model's abstract publication, clock,
fanout and issuer assumptions; this is not an end-to-end failover proof.

### Finite physical-authority horizon

Use the following initial implementation policy, subject to the acceptance tests
below rather than treating current idle/heartbeat limits as equivalent authority:

- **Direct PUT:** at most ten seconds of new physical admission from descriptor
  creation, additionally bounded by the captured request and receiving-route
  fences. This matches the existing order of magnitude of write/publication work
  budgets, but is a new explicit physical-scope cap, not a claim that the current
  budgets already cover all effects.
- **Streams:** at most five minutes per immutable window, starting when its
  introducing create/append command is built. Each successful append may reserve
  the next window in its existing transaction with a new cutoff; it cannot extend
  an earlier window. The owner tracks the maximum cutoff of every retained window.
  No standalone idle-renewal command is introduced.
- The five-minute bound is a **per-segment progress limit**, not a total upload
  duration. At the current 8 MiB segment size it permits roughly 27 KiB/s between
  appends before other limits; it intentionally rejects an indefinitely trickling
  body even if frames arrive within the current 30-second HTTP idle timeout.
  Faster overall uploads can continue through successive windows. Existing
  stricter admission and body deadlines still win.
- The ten-second reservation heartbeat and fifteen-second bucket-proof lease
  remain separate resource-liveness mechanisms. Neither renews physical write
  authority. Conversely, retaining cleanup evidence after cancellation must not
  keep a live bucket reservation until the physical cutoff.
- Construct each cutoff once with checked arithmetic; reject overflow. Preserve
  it through exact retries and same-payload reissue. If command construction or
  contention consumes the usable interval, fail rather than resetting its clock.
  Static/unbounded route authority must still receive a finite physical cutoff.
- Transport the existing absolute conservative wall-clock upper bound, with the
  supported skew shortening and receiver-local monotonic binding. Never transmit
  a duration or monotonic timestamp. Replica apply compares immutable values;
  only primary/physical admission evaluates time. Already-admitted IO retains its
  exclusion guard through persistence even if it finishes after cutoff.

This trades delayed cleanup (up to the remaining window horizon, plus guarded
in-flight work and unavailable participants) for avoiding an extra revocation
commit. It does not promise physical completion at exactly ten seconds/five
minutes. Bound per-owner rows as well: the current 5 GiB request limit and 8 MiB
segments imply at most 640 selected segments plus one unused lookahead window per
stream attempt. Enforce this at the storage owner before introducing windows,
not merely through HTTP checks. It is a per-attempt limit, not a whole-MPU object
limit or a bound on aggregate retained state. Limits must have a shared owner so
future request-size/segment-size changes cannot silently diverge.

### Next implementation and acceptance gate

Implement owner/window state together with its existing creation, append and
terminal transactions, route-reference/digest/export membership and versioned
evidence. Carry the primary's opaque local issuer into the descriptor; replicas
must persist that issuer rather than substituting their own. Enforce one-slot
consumption under the allocator lock and preserve exact descriptors on retries.
Neither imported rows nor a new runtime nonce may resume an old window.

Before enabling the policy, add deterministic tests for first-slot success and
second-slot rejection without mutation; lost preparation reply; repeated append
publication without another allocation; next-window expiry while buffering body;
static-route finite cutoff; same-epoch renewal; arithmetic limits; and independent
file/ack delayed delivery. Cover existing AWS-facing failure/transport handling
for PUT, POST and UploadPart instead of introducing an ad-hoc S3 error. Actual
capacity/progress checks and remote confirmation remain implementation work.

Validation: eight focused model/workflow tests passed. Full
`cargo nextest run --offline --no-fail-fast` passed 9,091 tests (one slow, six
skipped) in 235.990 seconds. Strict workspace Clippy, storage boundary checks,
formatting and `git diff --check` passed. The first focused run exposed an invalid
hyphenated upload-ID label in the new UploadPart fixture; correcting that label
allowed the test to reach its intended publication collision. The six ignored
cleanup regressions remain unresolved.

## Storage-Owned Stream Extent Limits — 2026-10-30

The owner/window integration needs an enforced row bound, not merely the HTTP
body limit. Move the existing 5 GiB single-write/part limit and 8 MiB logical
segment size into storage; server-core reexports those same values for ingestion.
This does not limit the total size of an object assembled from multipart parts.

The shared extent predicate permits selected indices 0 through 639 and logical
sizes up to 8 MiB, within the single-write limit. Index 640 is unavailable for
payload preparation even with size zero: the proposed unused lookahead window
is not an additional payload segment. Zero-size segments at a valid index remain
permitted. The derived segment count has a compile-time representability check.

Enforcement is deliberately at two production boundaries:

- Embedded preparation validates before opening the PG or allocating a VID.
  Unix clients reject before any transport; installed storage nodes independently
  use the same local preparation guard.
- Central `AppendStreamSegment` application validates before segment mutation,
  including commands decoded from a pending slot after restart.
  A malformed internal/recovered envelope cannot bypass the preparation guard
  and advance the segment rows, cursor, command log or digest through apply.

The regression matrix includes oversized segments, extreme integer values,
the first forbidden index (including a zero-length request), positive boundary
controls, PUT and UploadPart sessions, no-server sender rejection, and raw RPCs
that bypass sender validation to exercise the installed server. Rejections compare
complete session/reservation rows and command-log state. Reopened pending-slot
tests use coherent integrity witnesses so they fail for the extent violation,
not an unrelated geometry/checksum mismatch, and preserve the exact pending slot.

No encoded fields, schema versions, transactions or fsyncs are added. This is a
semantic bound on production preparation/application, not a claim that every raw
test helper or imported row has been validated. It does not establish contiguous
segments, exclusive one-slot consumption, finite cutoffs, durable owner/window
records, or physical cleanup exclusion. Those remain the next integration work;
all six ignored cleanup regressions remain unresolved.

Validation: all five focused regressions passed. Full
`cargo nextest run --offline --no-fail-fast` passed 9,096 tests (one slow, six
skipped) in 268.521 seconds. The first focused run caught missing command-digest
initialization after raw metadata-only session setup in the new recovery
fixture; refreshing that digest made its positive command control reach apply.
The slow `raft_production_shaped_heartbeat_write_amplification_gate` passed in
87.325 seconds. Strict workspace Clippy, storage boundary checks, formatting and
`git diff --check` passed.

## Single Outstanding Local Preparation — 2026-10-30

Enforce the chosen one-slot policy at the existing local allocator update before
adding durable owner/window authority. Production PUT and UploadPart preparation
now require the requested index to follow the locally applied segment prefix,
remain below the storage-owned segment bound, and have an unconsumed local VID.
The latter compares `next_segment_vid - 1` with the maximum locally appended VID
(zero for an empty session). The subtraction avoids overflowing a corrupt maximum
VID. A successful preparation advances the cursor once. Until its append applies,
neither repeating that index nor skipping ahead can allocate another candidate.

All predicates are part of the same `UPDATE ... RETURNING`; successful allocation
still steps through statement completion before returning. There is no additional
write statement, transaction, command, format change or fsync. The subqueries add
bounded local inspection of at most 640 segment rows. The unrestricted interval
allocator remains only a storage-unit-test arithmetic fixture; production node
clients use the bounded entry point. Rejected preparation uses the existing
invalid-request outcome (Unix `PayloadDecode`), not a new AWS-facing error.

This is intentionally **local consumption**, not the planned replicated reserved
end. In particular, a replacement replica with a stale cursor can still appear to
have a free slot; these checks do not establish issuer continuity. Nor does an
isolated appended row prove publication has settled. The future owner/window
confirmation path must enforce those obligations before granting physical write
authority. Do not infer cleanup coverage or a finite write cutoff from this local
predicate, and do not replace the proposed explicit retained windows with
`MAX(live rows)`. Retained history, replica discovery, terminal ownership transfer,
finite cutoffs and file/ack exclusion all remain outstanding.

Coverage includes:

- lost preparation reply modelled by ignoring its successful result, then
  rejecting a second allocation both before and after reopening the same store;
- an independent already-open store observing the consumed cursor, with the
  retry using the same index so a skipped-index check cannot mask the assertion;
- first-index and last-index success, next-index rejection before append, and
  rejection of the unused terminal lookahead after a complete 640-segment prefix;
- injected allocation-update failure preserving the free slot;
- applied append unlocking one next preparation, followed by exact append replay
  that neither allocates nor rewinds an already consumed cursor;
- embedded and installed-Unix PUT/UploadPart duplicate-preparation rejection,
  preserving complete session/reservation and command-log observations; and
- the production append-publication collision tests still consuming exactly one
  VID while retrying command publication, with exact encrypted readback; and
- same-PG/cross-PG coordinator races holding the successful preparation before
  payload writes, requiring the competing request to reject while it is held,
  then verifying one staged winner and exact finalized plaintext. Bounded channel
  waits and a release-on-drop guard replace the obsolete two-preparation barrier.

The replica-discovery experiments now assert rejection of the second speculative
preparation and retain their independent file/ack/manifest observations. The
unrelated-pending duplicate cleanup canary uses an explicitly malformed
storage-owned conflicting record: its cleanup and preservation assertions still
run after physical staging, rather than being replaced by early rejection alone.

Validation: 15 focused storage/model/workflow checks and both corrected
coordinator race tests passed. The final full
`cargo nextest run --offline --no-fail-fast` passed 9,099 tests (one slow, six
skipped) in 235.301 seconds. The Raft production-shaped write-amplification gate
passed in 114.468 seconds. Strict workspace Clippy, boundary checks, formatting
and `git diff --check` passed.

The initial focused run exposed the two discovery tests'
old multiple-preparation assumption; the storage run exposed four more fixtures
with that assumption (reopen, already-open handle, durable allocator and duplicate
cleanup). Those fixtures were migrated to the selected one-slot policy without
weakening the production guard. The first full run passed 9,097 tests but was
interrupted after diagnosing two coordinator race tests blocked on that obsolete
barrier; they expected both preparations to succeed before either could proceed.
No unrelated process was stopped. All six ignored cleanup regressions remain open.

## Creation Origin And Placement Input Audit — 2026-10-30

### Central creation validation (implemented)

Creation builders always set `initial_next_segment_vid` to `GenerationId::MIN`,
and their request matcher requires it. Central apply previously accepted a
decoded creation with a different initial cursor. Such a session has no appended
prefix to justify that cursor and cannot prepare its first segment under the
one-slot rule. Require the initial cursor to equal the first VID before any
reservation, high-water or session mutation, for PUT and UploadPart alike.
This preserves the existing writer's contract, not a new allocation format.

The regressions use correctly checksummed, decoded commands varying only the
initial cursor: 2, 641, the SQLite integer maximum and the `u64` maximum. They
require the specific creation-origin diagnostic, compare the complete checkpoint,
and observe SQL writes to reject a mutate-then-rollback implementation. A valid
creation proves the observer detects real reservation/counter/session writes.
Separate persisted-pending tests reopen twice and require the malformed command
to leave the exact slot, session absence, reservation absence, high-water and
applied prefix unchanged. Rejecting this envelope does not authorize its
abandonment; ordinary recovery authority still governs that decision.

### Inputs for a durable first window (implementation decision)

The existing creation transaction is the right durability boundary, but its
current builder does not yet receive all the inputs needed by the proposed
window. Do not fill them later from whichever runtime map is then current.

| Fact | Current owner and point of availability | Required integration |
| --- | --- | --- |
| Exact session, target, bucket incarnation and creation proof | Scoped creation request and loaded object/MPU precondition | Preserve the existing subject/proof validation; derive the window's subject from those checked values. |
| PUT/part payload generation | `LocalStorageNodeClient::build_create_stream_upload_command_for_route` selects it read-only under the object-PG lock | Resolve first-window placement after this selection, within the same builder; do not reintroduce a preceding generation-reservation command. |
| UploadPart placement generation | Loaded enclosing upload's `object_generation_id` | Keep it distinct from the selected part payload generation; bind both. |
| PG topology and default EC shape | `SharedStorageNode` | These identify a data PG and geometry, but not its acting set, epoch or acknowledgement owner. They are not a complete placement authority. |
| Captured data routes | Embedded admitted cluster map; installed server's captured config includes all current PG routes | Supply an opaque, immutable placement context to creation, append/lookahead and Direct PUT reservation builders. Do not use a caller-implemented callback or perform a route refresh while holding the PG lock. |
| Allocation issuer | Actual metadata primary's `PgStore` allocation lifetime, read under its lock | Persist that node/lifetime identity. A frontend's Unix/TCP shadow runtime must never mint it. |
| File and acknowledgement participants | Existing placed-shard derivation and data-PG acknowledgement route | Derive both explicitly from the selected captured data route, not from acting-set positions or the object-PG primary. |
| Finite cutoff | Captured effective admission/receiving-route bounds plus the selected physical-window horizon | Compute an immutable minimum with checked arithmetic; `cleanup_after` is not a substitute for physical write authority. |

Source boundaries:

- [`local.rs`](../crates/storage/src/node_client/local.rs),
  `build_create_stream_upload_command_for_route`, selects the generation after
  checking session/reservation absence. Its client currently holds only node ID
  and `SharedStorageNode`; it has no admitted data-route catalogue.
- [`routes.rs`](../crates/storage/src/storage_node_server/routes.rs),
  `build_create_stream_upload_command`, opens that local route from an already
  validated storage-node primary route. Its captured handler config is the
  installed server's source for placement context.
- [`config.rs`](../crates/storage/src/storage_node_server/config.rs),
  `from_runtime_map`, copies all current PG routes, not just the object route.
  The context must retain that captured version rather than reload config later.
- [`local.rs`](../crates/storage/src/cluster/local.rs),
  `place_payload_shards_for_pg_route`, runs the canonical placement algorithm;
  copying acting-set order into shard indices is not equivalent.
- [`payload_operations.rs`](../crates/storage/src/cluster/payload_operations.rs),
  `register_validated_payload_shard_acks`, registers the batch at the data-PG
  metadata primary. That acknowledgement owner is distinct from each file owner.

The context must be minted from the already authorized operation, bound to its
publication domain, epoch, metadata subject and effective deadline. Project or
share the immutable route catalogue before taking the object-PG lock. Under that
lock, generation selection, issuer capture and placement derivation must be
read-only local computation; no network call or control-plane lookup is needed.
Reuse the ordinary placement algorithm and reject an unavailable/unplaceable
selected route rather than invent participants. Other unrelated PGs need not be
Active merely because the catalogue contains them.

For Unix/TLS, the server derives its context from its admitted frame and actual
store. Client-supplied participants or an issuer nonce are not authority. The
frontend validates the returned descriptor against its own original admitted
placement context after learning the selected generation, before installation or
physical writes. A changed map, crossed subject, wrong EC shape, wrong issuer
node or noncanonical participant assignment must fail closed. Transport only the
existing absolute conservative wall-clock cutoff representation; bind it at the
receiver and intersect it with the server's captured authority. Do not transport
a monotonic value or renew the cutoff after a retry.

Replica apply must compare the command's immutable subject, interval, issuer and
placement evidence against its local object-PG state. It must not choose a new
generation, substitute its own lifetime, consult current wall clocks, or query
data-PG shard rows. Witness-first apply and reissued-command recovery must produce
the same owner/window rows as primary apply.

### Next complete state slice

First thread this context through the shared builder boundary and introduce the
retained owner/first window together with append's next window and terminal
ownership retention. Preserve both current creation entry points and the lower
record-under-reservation workflow; cover PUT/POST, UploadPart and copy users.
Direct PUT's reservation builder needs the same captured-placement contract.
Keep immutable intervals and terminal exhaustion explicit; index 640 cannot
become another payload allocation merely because a lookahead is represented.

Schema/table catalogue, canonical digest membership, checkpoint/export/import,
command/RPC descriptors and route-history summary/exact references must be
reviewed together. In particular, keep the original bucket-PG proof route until
its release is confirmed, and retain pre-append data routes even when no segment
or ack row exists. Preserve the already-frozen parent-version evidence and append
the new complete version-vector specimens; do not assign version numbers in this
design entry.

Acceptance must include a route change between creation and first preparation,
same-epoch renewal past the captured cutoff, unsorted acting sets with a distinct
ack primary, crossed frontend/server placement, and replacement-primary issuer
rejection. These supplement the previously listed crash, exact-replay, terminal
retention and export/reopen tests. No durable owner/window tables, placement
context API, or physical grants are implemented by this audit. All six cleanup
regressions remain unresolved. No additional normal-path commit is proposed.

Validation: all 24 focused creation/allocator tests passed. The full Nextest
suite passed 9,101 tests (3 slow, 6 skipped) in 333.178s. Strict all-target,
all-feature Clippy, the storage boundary checker, formatting and diff checks
passed. The first focused recovery test correctly hit checkpoint export's
pending-slot prohibition; its observation was corrected to compare the
materialized-state digest, exact replica state and pending slot without weakening
that export guard.

## Shared Placement Resolver — 2026-10-30

As a preparatory refactor for the next complete state slice, move the existing
canonical file-placement computation out of `LocalClusterMap` into the private
node-runtime implementation at
[`payload_placement.rs`](../crates/storage/src/payload_placement.rs). Current
active and historical placement, repair, deletion and topology validation now
use that shared implementation. Storage-node builders can reuse it without
constructing a frontend cluster or copying the placement algorithm.

The resolver is pure computation, **not a capability**. It takes already
captured inputs and performs no runtime-map lookup, clock read, database access
or network call. Existing callers keep their active/retained route validation.
In particular, accepting an epoch and acting set does not authorize either writes
or cleanup. Do not use its result as a substitute for the future admitted
placement context, durable window, settled-prefix confirmation or exclusion.

Keep the placement key domain, big-endian data-PG prefix, node topology/weights,
placement constraint and error mapping unchanged. The placement engine scans
canonical NodeId order, but its candidate replacement order determines shard
indices when there are more nodes than shards; do not sort the final result or
index the acting set directly. The acknowledgement participant remains the
captured data-PG metadata primary, independently of file placement.

Coverage pins the literal key framing, all six permutations of a three-node
acting set, keyed selection from an eight-node set for 1+0/2+1/4+2, and invalid,
duplicate or insufficient nodes. An embedded production-path canary uses acting
set `[4, 2, 3]` with primary `3`, verifies the independently expected shard-index
mapping `[2, 3, 4]`, writes files and registers acknowledgements, then checks each
file exists only at its assigned node and every acknowledgement exists only at
the metadata primary. Existing explicit historical-route coverage remains.

This slice adds no durable rows, schema/encoding changes, extra transactions or
fsyncs. Capturing authoritative builder inputs and integrating owner/window state
with creation, append and terminal commands remains the next state-changing
slice; all six ignored cleanup regressions remain unresolved.

Validation: all 11 focused placement tests passed. Full Nextest passed 9,106
tests (one slow, six skipped) in 274.562s. Strict all-target/all-feature Clippy,
the storage boundary checker, formatting and diff checks passed. The initial
focused run exposed the fixture's use of a convenience constructor requiring
node 0; it now uses the explicit-primary constructor for its `[4, 2, 3]` nodes.

## Captured Placement Inputs for Stream Preparation — 2026-10-30

Stream append preparation now takes a borrowed immutable placement catalogue.
Embedded callers supply their captured cluster map; Unix/TLS handlers derive
the catalogue from the admitted handler's config, not a later runtime-map lookup
or client-supplied participant list. This is computational input, **not a write
capability** or a durable window. The session route, exact expected append state
and immutable effect fence remain the authority checks.

While holding the object-PG lock, preparation validates the loaded session and
generation, computes the selected data PG and candidate VID, and resolves the
canonical file placement before consuming the VID. Missing, inactive, stale,
crossed-epoch or unplaceable selected routes cannot consume the single local
preparation slot. The effect fence is rechecked after placement, immediately
before allocation; installed handlers intersect the delegated deadline with
their own captured route fence. Returned placement epochs are assigned by the
builder rather than patched by its callers. No monotonic timestamps or new
deadline representation cross the wire.

The Unix sender checks its captured catalogue before transport and validates
the returned data PG, epoch and EC shape against the expected durable generation,
then resolves the exact returned hash/VID. PUT placement uses the reserved object
generation; UploadPart placement continues to use the enclosing upload generation
and part number, not its independently reserved part-payload generation.

Coverage exercises PUT and UploadPart rejection with complete session,
reservation, segment and command-state comparisons, independent invalid-catalogue
cases, and positive allocator controls. An installed Unix request bypasses sender
preflight to exercise the server's unplaceable catalogue. A malicious-response
fixture supplies a valid response followed by crossed existing PG, epoch and EC
responses and requires the placement-specific `PayloadDecode` diagnostic.

This slice adds placement computation under the existing lock, but no durable
rows, schema/encoding changes, new transactions or fsyncs. Creation builders still
need captured placement inputs. Retained owner/first-window, next-window and
terminal integration, issuer binding, settlement and physical exclusion remain
unfinished; all six ignored cleanup regressions remain unresolved.

Validation: all seven final focused preparation/deadline tests passed. Full
Nextest passed 9,109 tests (one slow, six skipped) in 255.717s. Strict
all-target/all-feature Clippy, the storage boundary checker, formatting and diff
checks passed. The initial full run exposed an existing deadline fixture with
4+2 descriptors but only one placement participant; it now supplies six
participants while retaining its deadline assertions. New-fixture failures were
also corrected: a session label exceeded the encoded ID limit, and the installed
RPC assertion expected an internal reason that the error layer intentionally
redacts. Embedded tests pin the precise reasons; the installed test checks the
wire error and unchanged durable state. Clippy prompted moving preparation onto
the scoped route rather than passing its PG/bucket/key separately.

## Captured Placement Inputs for Stream Creation — 2026-10-30

The shared stream-creation builder now requires the captured placement catalogue
used by preparation. All four cluster creation call sites pass their immutable
map, including current-object-checked PUT creation, the lower
record-under-reservation entry point, and both UploadPart workflows. Installed
Unix/TLS handlers derive the catalogue from their admitted config; it is not
serialized as client-selected participants.

Under the existing object-PG lock, the builder validates the current object or
upload, requires an unused session/reservation identity, and selects the payload
generation read-only. It derives the first segment's data PG using the actual
store topology and that validated state, then checks the captured route and
canonical placement for index zero/VID one before allocating a command ID. PUT
uses its newly selected generation; UploadPart uses the enclosing upload's
generation and part number, retaining the existing payload placement contract.
The independent part generation remains the encryption/ownership subject.

Unix response validation resolves the first placement against the frontend's
original catalogue after learning the returned payload generation. This catches
a command whose selected route is unusable in that catalogue before installation.
It does not certify equality of arbitrary frontend/server catalogues or persist
participants: an immutable descriptor and its exact comparison still belong to
the owner/window integration. Existing route and effect-fence checks remain
required; the borrowed catalogue is not an admission capability.

New embedded and installed-Unix canaries cover PUT and UploadPart with the
selected data PG distinct from the object PG. A healthy frontend catalogue
cannot bypass the server's unavailable selected route. Rejections preserve the
session/segment/upload rows, generation-reservation absence, generation candidate,
replica state and command index. Positive controls build and apply successfully
with an unrelated PG in Peering, and reject the same returned command against a
frontend catalogue whose selected PG is unavailable. The UploadPart case selects
a key for which substituting the part generation for the upload generation would
choose a different data PG.

No wire/schema version, durable table, transaction or fsync is added. This
completes captured placement inputs for stream creation and preparation, not
durable owner/window integration. Direct PUT reservation placement, issuer
binding, replicated intervals, terminal retention, confirmation and physical
exclusion remain open, as do all six ignored cleanup regressions.

Validation: all 84 focused creation, stream and Unix tests passed. Full Nextest
passed 9,111 tests (two slow, six skipped) in 270.172s. Strict all-target/all-feature
Clippy, the storage boundary checker, formatting and diff checks passed. Existing
metadata-only builder fixtures now provide placeable catalogues consistent with
their EC shapes; no payload fixtures or historical encoding specimens changed.

## Direct PUT Reservation Placement Preflight — 2026-10-30

Fresh Direct PUT generation reservations now resolve index-zero placement using
the already captured cluster map before entering fresh command-ID/pending-slot
installation. The key is the ordinary Direct PUT reservation-ID hash and the
selected generation is both the placement generation and shard VID; do not use
the stream session's initial VID-one convention here. This prevents knowingly
reserving a generation whose selected data PG is inactive or unplaceable.

Keep the existing stale-generation reinspection before this check. If another
request reserves the candidate first, the outer retry selects a new generation
and recomputes its data PG and exact placement. A previous candidate's successful
preflight must not authorize the next candidate. The normal fresh pending-slot
effect fence and central reservation apply conflict checks remain unchanged.

Preflight applies only to fresh issuance. An exact pending reservation command
must still converge, and an already materialized exact reservation must still
be observable, when its data route is no longer Active. Neither result grants
physical write authority; the subsequent payload write retains its own current
route/effect checks. Do not turn a placement preflight into an abandonment rule
for a command that might already have published.

New multi-replica regressions pin rejection before the pending-install hook and
compare replica state, log position, next generation, reservation absence and
empty pending slots. A healthy positive control must reach that hook and reserve
the original generation. A deterministic competing reservation advances the
candidate from a usable PG to a different Peering PG; the losing attempt must
reject without further mutation. An exact pending-command canary converges on
every replica despite the unusable data route. An installed-Unix case checks the
actual remote store remains unchanged on frontend preflight rejection, then
reserves successfully through the healthy map and verifies no reservation was
written to frontend shadow storage.

This is **frontend captured-map preflight**, not an atomic storage-node reservation
builder or a persisted placement witness. Direct PUT currently selects/rechecks
the generation via scoped reads and builds its reservation command in the
cluster. The durable owner/window slice must still move complete generation,
issuer, cutoff and descriptor minting to the actual metadata-primary boundary,
validate the returned descriptor, and retain it through apply/recovery. This
slice adds no RPC, schema/encoding change, transaction or fsync. All six ignored
cleanup regressions remain unresolved.

Validation: all 39 focused reservation, race, recovery and Unix tests passed.
Full Nextest passed 9,115 tests (one slow, six skipped) in 246.259s. Strict
all-target/all-feature Clippy, storage boundary checks, formatting and diff
checks passed. The first focused run caught a positive-control observer scoped
only to the restricted map; the healthy control now installs its own observer
and demonstrably reaches pending installation. Production behavior was not
changed to accommodate that fixture failure.

## Fallible Allocator Command Installation Boundary — 2026-10-30

The next actual-primary Direct PUT builder cannot use the simpler prebuilt
allocator installer unchanged: that path creates a separate drain budget and
does not expose the fresh installer's exact recovery-handoff outcomes. First
separate command construction from ID allocation without weakening those
outcomes.

`install_allocator_cleanup_built_metadata_command` now accepts a fallible
complete-envelope builder. Existing fresh-ID allocator publishers delegate to
it, retaining their current builders and command formats. The raw primitive
checks the pending slot under the same frontend metadata-command PG
serialization before invoking construction. The typed allocator layer checks
the original request work budget after that serialization wait. A visible
contender skips construction; a build-time log conflict reobserves the slot;
either case drains at most the observed command and returns to the owner loop.
Transferred/awaiting-recovery results carry that exact contender. Other build
errors propagate without installing anything. Successful envelopes must match
the selected PG and epoch; pending installation still receives the original
effect fence.

The boundary checker recognizes both the raw primitive and sealed typed entry
point, with fixtures for an unclassified raw call and a missing publisher
marker. Owner-local regressions cover build rejection, successful exact
installation, crossed PG/epoch envelopes, exhausted shared budget, build-time
log conflict with/without a visible slot, one-contender draining when a second
command arrives, and exact failed-contender recovery handoff.

This is an installation integration point, **not** the storage-primary builder
or durable owner/window integration. Frontend serialization does not exclude
another frontend's requests. The next builder must select/revalidate generation,
placement and issuer at the actual primary, enforce the captured deadline beside
allocation, and validate its returned descriptor before installation. It must
not publish a pending command itself: ambiguous builder responses must not hide
publication ownership. No RPC, schema, encoding, transaction or fsync is added
here. All six ignored cleanup regressions remain unresolved.

Validation: all 39 focused allocator/generation/Unix race tests passed. Full
Nextest passed 9,121 tests (one slow, six skipped) in 256.099s. Strict
all-target/all-feature Clippy, storage boundary checks, formatting and diff
checks passed.

## Primary-Side Direct PUT Reservation Builder — 2026-10-31

Fresh Direct PUT now obtains its complete `ReserveObjectGeneration` command
through the scoped object-generation node route. The frontend retains candidate
and captured-placement preflight; the actual metadata-primary builder locks its
PG, requires reservation absence, selects the current generation read-only and
compares it to that candidate, resolves index-zero placement from actual store
topology and captured routes, and validates the immutable effect fence just
before command-ID selection. The creation timestamp is primary-owned. Building
does not install a pending slot or insert a reservation. The existing allocator
installer still owns publication, one-contender draining, request budgets and
exact recovery handoffs. Stale candidates restart the outer loop before fresh
installation; post-build races remain protected by ordinary install/apply
conflict validation.

RPC kind 176 carries the exact subject/reservation ID, expected generation and
optional conservative absolute wall-clock deadline. The handler admits the
active primary route, rebinds that portable cutoff and intersects it with its
own captured route deadline before local construction. The client validates
every returned subject field, PG, epoch and generation; missing or crossed
commands are `PayloadDecode`, not successful reservation evidence. No process-
local monotonic timestamp is serialized. The shared client supports Unix/TLS;
this slice's new installed-server regressions use Unix.

Embedded/Unix matrices distinguish the selected data PG from the object PG,
reject unavailable selected placement even with a healthy frontend catalogue,
permit unrelated Peering PGs, reject stale generations and existing identities,
and compare reservation absence, pending slots, log/replica state and generation
candidates. A bounded successful request exercises sender conversion; an expired
raw portable request exercises receiver rejection. A malicious peer crosses PG,
epoch, bucket, key, reservation ID and generation independently. Deterministic
frontend pre-install races verify the primary check rejects a stale candidate
without publishing/abandoning an extra command. Unix race observations now use
the live server-owned node rather than reopening its files.

Storage RPC advances v28 → v29; metadata commands remain v12 and durable formats
are unchanged. Frozen request evidence covers both deadline branches and exact
maximum-frame admission. Original v28 frames, stream-creation corpus and v2/v28
authentication bindings remain immutable rejection evidence; current v29
composites are appended. No transaction or fsync is added. This supplies the
actual-primary construction boundary, not the durable issuer/window descriptor,
owner retention, confirmation or physical deletion exclusion. Those integrations
and all six ignored cleanup regressions remain open.

Validation: all 237 focused generation, race and RPC-format tests passed. Full
Nextest passed 9,128 tests (one slow, six skipped) in 265.085s. Strict
all-target/all-feature Clippy, storage boundary checks, formatting and diff
checks passed. An initial focused run caught the authentication-binding golden
still expecting the nested v28 frame; its literal request/response specimens
are now preserved as rejection evidence beside the appended v29 composites.

## Live Allocation Lifetime At Fresh Builder Boundaries — 2026-10-31

The actual-primary builder audit exposed an unconnected prerequisite: PG
replacement already retired the volatile allocation lifetime, including after
failed route publication/import, but fresh builders and append preparation did
not consult it. An unchanged installed route could therefore continue issuing
work from a retired store. Connect this check before attaching durable window
issuers to those builders.

The shared local routes now require a live allocation lifetime under the actual
storage node's PG lock immediately before Direct PUT reservation-command ID
selection, stream-creation command ID selection, and stream append VID allocation.
Both `Replacing` and `Retired` reject fresh work. Installed Unix/TLS dispatch uses
these same local routes; Unix regression coverage checks the exact
`InactivePgRoute` response and diagnostic. Embedded and remote failures retain
retryable-convergence classification, not a conditional-conflict classification.
No new wire error tag, format version, SQL transaction or fsync is introduced.

The check deliberately does not guard replica apply, exact command replay,
matching-session inspection or existing-reservation lookup. A retired primary
must not strand work already issued/published, and replica application must not
depend on a replica's local issuer lifetime. Tests cover Direct PUT construction,
PUT/UploadPart creation and preparation, comparing checkpoints, pending/log
state, generation candidates, complete sessions and staged rows after rejection.
Positive controls exercise active construction, apply/replay while retired,
inspection while retired and preparation after a successful lifetime replacement.
The lifetime unit test independently rejects the in-progress replacement state.

This is **liveness gating only**, not the persisted issuer/session binding. A
successfully replaced PG still cannot distinguish an old window from a new one
until the command/schema work lands. It also does not revalidate a command's
issuer between construction and installation. The retained owner/window records,
atomic creation/append/terminal retention, confirmation and physical exclusion
remain the next complete state integration; all six cleanup regressions remain
unresolved. Do not treat this check as a physical grant or cleanup safety fix.

Validation: all 22 focused lifetime tests passed. Full Nextest passed 9,131 tests
(one slow, six skipped) in 265.133s. Strict all-target/all-feature Clippy, storage
boundary checks, formatting and diff checks passed. The reported slow Raft
heartbeat write-amplification test completed successfully in 94.690s; no rerun or
test change was needed.

## Captured Stream-Creation Fence At The Primary — 2026-10-31

The input audit for first-window issuance found that stream creation still
carried only placement and `cleanup_after` to its primary builder, unlike the
Direct PUT builder. A cleanup horizon is not the request's immutable authority.
Thread that missing input before deriving a physical window cutoff from it.

All four creation workflows now supply an explicit `AdmittedRouteEffectFence`
to the shared node builder. The test-only raw UploadPart adapter supplies an
explicit unbounded fence; production paths carry their original admitted fence.
The builder validates it before waiting for its PG lock and again after loading
the session/reservation candidate and selected placement, immediately before
command-ID selection. It retains the live-allocation-lifetime check. Existing
pending installation still receives its own original fence; building neither
inserts a session/reservation nor installs a pending slot.

The Unix/TLS client validates the fence and projects its conservative absolute
wall-clock cutoff into the RPC request. The server rebinds that cutoff to its
own monotonic clock and intersects it with the storage node's captured route
fence, including when the sender supplies no deadline. No monotonic timestamp or
remaining duration is transported. A renewed/longer raw route cannot extend
the request's shorter captured authority.

RPC advances v29 → v30; creation-build admission preserves its existing payload
budget plus the maximum 17-byte optional-deadline field. Command v12, schema,
digest/checkpoint and encryption encodings do not change. The original 27-case
v29 corpus, generic/encrypted frames and authentication bindings remain literal
rejection evidence. The appended 37-case v30 corpus covers both deadline arms
for all existing creation-precondition/cleanup alternatives. Malformed deadline,
truncation/trailing-byte and exact-cap frame tests supplement it.

Owner-local embedded/installed-Unix tests advance time after initial validation
but before acquiring the PG lock, across the effective deadline and before the
raw authority timestamp. They cover PUT and UploadPart, either deadline expiring
first, and an absent delegated deadline, with successful controls and exact
checkpoint/pending/log/candidate/session non-mutation assertions. Authenticated
TLS tests invoke the production fenced client with different frontend/storage
monotonic origins, demonstrating success before the portable cutoff and rejection
after network transit crosses it while the server route remains valid.

This adds no normal-path transaction or fsync. It supplies the missing cutoff
input, not a durable window or write grant. Retained owner/window integration,
issuer matching, confirmation, physical exclusion and all six cleanup regressions
remain open.

Validation: all 58 focused deadline/creation/format tests passed. The full-suite
run exposed a stale-epoch fixture incorrectly given an initial-epoch fence;
binding that fence to its request epoch restores the intended installed-server
rejection (also verified independently). The full rerun passed all 9,135 tests
(one slow, six skipped) in 235.419s. Strict all-target/all-feature Clippy, storage
boundary checks, formatting and diff checks passed. TLS test setup was corrected
to use a separate connection per receiver-clock scope and keep authenticated
response timestamps within the supported skew; it still exercises production
sender conversion and receiver rebinding with distinct monotonic origins.

## Owner/Window Integration Boundary Audit — 2026-10-31

The next change is the coordinated durable state integration, not another claim
that the live-lifetime check establishes exclusive issuance. The input and
transaction audit below identifies the remaining changes that must land with
that state. No owner/window table or physical grant is implemented by this audit.

Subsequent prerequisite slices implement the primary-owned append builder,
explicit command reservation disposition and Direct PUT primary proof input
below. The audit's source snapshot
and remaining coordinated-state requirements are otherwise retained.

### Bind persisted windows, not the lifetime of a frontend route object

`require_live_allocation_lifetime()` proves only that the store currently has an
active issuer. It deliberately does not compare that issuer with a durable
window. Capturing a nonce in an embedded node-client route and comparing it on
each call would not establish the intended guarantee: Unix creates the local
route again for each frame, whereas an embedded caller can retain its route
across calls. Reopening either route must not authorize an old attempt to resume.
Also, today's route constructors use `require_open_pg()` without acquiring the
store mutex; adding an incidental lock there would change their lock contract.

Keep the binding at the existing primary mutation lock instead. Mint the
descriptor's `(primary NodeId, PG-open lifetime)` there, encode it in the
introducing command, and persist the exact value on every replica. Preparation
loads the descriptor and compares its issuer to the current store lifetime under
the same lock immediately before consuming the slot. Route reconstruction,
successful replacement, replica apply and session-row equality cannot renew that
binding. Already-issued commands may still converge without consulting a
replica's local lifetime. The original process-local nonce stays private; this
work adds a durable issuer identity, not a serializable live guard.

### Missing inputs and construction boundaries

| Boundary | Current evidence | Required integration |
| --- | --- | --- |
| Direct PUT reserve | `ActivePutObjectRoute::reserve_generation_with_id` validates the exact bucket proof and incarnation, but passes only the reservation ID and effect route into `reserve_put_object_generation_with_route_validation`. The primary builder receives placement/fence, not that proof. | Carry the already-validated proof to the primary builder and introducing command. Bind the owner to its bucket incarnation, exact reservation subject and historical bucket-PG release route. Do not reload an unrelated latest bucket snapshot to recover these fields. |
| Stream create | `BuildCreateStreamUploadCommandReq` now carries placement, proof and captured fence; the primary selects the payload generation under its PG lock. UploadPart placement additionally uses the loaded MPU generation. | Derive owner and first window there. Preserve both part payload generation and MPU placement generation. Persist owner/window with reservation, high-water and session in the existing apply transaction. |
| Stream append | `commit_stream_segment_append_with_work_budget` still constructs `AppendStreamSegmentCommand` in a frontend closure supplied to `install_apply_validated_metadata_command_with_fresh_id`. Its current payload contains only bucket/key, target and selected segment. | Move window-bearing construction behind an actual-primary builder before attaching a new issuer or cutoff. It must validate the durable owner, current window and selected append, and return the exact next descriptor. A frontend-generated nonce or frontend shadow-store lifetime is not acceptable. |
| Preparation | The local route already checks the expected session/generation, placement, captured fence and current lifetime under the allocator lock. | Add the durable window/issuer comparison at that same boundary; keep the replicated reserved end separate from the local consumed cursor. Do not add an inspection RPC followed by an unchecked allocator RPC. |
| Terminal/abort/release | Existing apply paths remove session/reservation rows; command recovery and terminal cleanup have separate resource-release handling. | Retain or transfer owner/window state in those same apply transactions before removing the old rows. Preserve exact selected references and all unused lookahead windows. |

Relevant implementations are in
[`operation_routes.rs`](../crates/storage/src/cluster/operation_routes.rs),
[`payload_operations.rs`](../crates/storage/src/cluster/payload_operations.rs),
[`local.rs`](../crates/storage/src/node_client/local.rs), and
[`command_mutations.rs`](../crates/storage/src/pg_store/metadata/command_mutations.rs).

Moving append construction must preserve its current publication behavior:
one observed contender per owner-loop retry, the original work budget and effect
fence, matching-command success, exact recovery handoff, and reference-checked
payload cleanup after an ambiguous install. Reissue preserves the entire window
descriptor; a log-index conflict does not authorize a new cutoff or issuer. Do
not mint another window merely because the caller did not receive a reply.

### Resource ownership is not just proof presence

The owner records an exact release obligation, not permission to release every
proof that occurs in an introducing command. In
[`metadata_operations.rs`](../crates/storage/src/cluster/metadata_operations.rs),
`release_applied_metadata_command_bucket_write_reservations` currently preserves
streamed PUT's creation proof but releases UploadPart's creation proof. Direct
PUT's generation command currently has no proof for that helper to extract.
Adding one without updating this classification would release it before payload
publication.

The integration must distinguish these responsibilities explicitly:

- Direct PUT: successful generation reservation transfers the live proof to the
  attempt; it does not release it. Terminal publication/cancellation retains the
  existing release timing, with the owner retaining unresolved release evidence.
- Streamed PUT: creation keeps its live proof through the session. Heartbeat
  deadlines may change, but stable proof identity and the original release route
  remain bound. Terminal/abort transfers any unresolved release obligation before
  removing the session.
- UploadPart: creation's proof is short-lived and is released after creation
  settles. The retained owner may remember an unresolved release, but must not
  treat this proof as continuing mutation authority. Finalization uses its own
  separately validated proof; it cannot overwrite the creation obligation or be
  confused with it. Represent both obligations when both remain unresolved.

Use a closed responsibility state, not `Option<Proof>` to mean both "no proof
was required" and "release succeeded." Confirmation of exact release is
idempotent. No ordinary successful write should gain a standalone command merely
to erase retained evidence: keep that evidence until an eligible existing owner
transition or eventual cleanup transaction can retire it. Its exact historical
bucket-PG route must remain pinned until the durable obligation is retired.
Fanout, abandonment and recovery must validate internal subject consistency
before attempting release; authorization validation and release timing are
separate decisions.

### Reviewable implementation sequence

1. Define the private owner/window and resource-obligation records together with
   SQL constraints and canonical table membership. Integrate reserve/create,
   primary append construction, preparation, terminal retention and route-history
   references as one coherent state slice. Include the primary-owned inputs above;
   do not introduce nullable "not yet enforced" owner fields into live commands.
   Include the Direct PUT frontend proof-handoff contract below: the existing
   terminal-only transfer flag cannot govern a proof now owned by generation
   reservation publication or a retained owner.
2. Advance affected schema, command, RPC and canonical-state formats together,
   following their actual containment rules. Preserve the committed schema-9,
   command-12, RPC-30 and checkpoint/state composite specimens unchanged; append
   new current evidence. Checkpoint table membership and semantic validation live
   in [`command_log.rs`](../crates/storage/src/pg_store/command_log.rs), not just
   the schema catalogue. Review maximum RPC carrier overhead as well as command
   size. Do not preassign the next versions until the complete diff is known.
3. Prove complete rows/obligations survive rollback, exact replay, reissue,
   witness-first apply, checkpoint/import, reopen and two route transitions.
   Include same-node/new-lifetime preparation rejection over both embedded and
   installed Unix paths; a newly opened route must not make the negative pass.
   Independently pin each proof's release timing and an unresolved pair of
   UploadPart creation/finalization obligations.
4. Then integrate settled-prefix confirmation, bounded physical write/ack
   admission and local exclusion, followed by cleanup discovery. Owner rows by
   themselves do not stop delayed physical writes. Keep all six original cleanup
   regressions open until their actual end-to-end invariants pass.

This is a source-backed refinement of the existing transaction candidate. It
does not select another publisher class, new ordinary-path fsync, per-write
journal, or optional legacy representation. No production code or format changes
are included in this audit slice.

Validation: full Nextest passed 9,135 tests (one slow, six skipped) in 236.295s;
strict all-target/all-feature Clippy, storage boundary checks, formatting and
diff checks passed. These are unchanged-code regression results, not evidence
that the proposed owner/window integration is implemented. The six cleanup
regressions remain unresolved.

## Primary-owned stream append construction — 2026-10-31

Implemented the append-builder prerequisite identified by the integration audit.
The production frontend no longer constructs `AppendStreamSegment` from an ID
selected separately. The scoped stream-session route builds it on the actual
object-PG primary under its PG lock. Shared read-only preflight checks the exact
bucket/key/session/target, InProgress session and MPU, segment extent, checked
VID successor, encrypted geometry/integrity witness and existing segment. The
builder then requires a live allocation lifetime and revalidates the captured
effect fence before selecting the command ID. Replica apply independently uses
the same durable validation in its existing transaction; it does not require a
live issuer lifetime to converge already-built commands.

Unix/TLS kind 177 carries the exact target/segment and absolute portable deadline.
The server intersects delegated authority with its own captured route fence; the
client rejects substituted PG, epoch, bucket, key, target or segment responses.
The typed ApplyValidated installer accepts a fallible primary-built envelope and
retains the existing pending-slot fence, one-observed-contender retry, work-budget,
recovery handoff and reference-aware payload cleanup behavior. Construction is
read-only: no additional durable command, transaction or fsync. Remote operation
does introduce a builder RPC; this slice makes no performance improvement claim.

Storage RPC advances from v30 to v31; command v12 and durable formats do not
change. All v30 frames, 37 creation specimens and authentication composites remain
unchanged rejection evidence. New v31 composites and eight append-request
specimens are appended. The request cap includes the complete 256-entry encoded
witness index space (the codec's bound, not a new supported EC runtime shape).

Coverage includes embedded and installed Unix PUT/UploadPart construction with
exact checkpoint, pending-slot, log, session and segment non-mutation canaries;
crossed request and malicious response subjects; retired-lifetime rejection while
already-built commands still apply/replay; and deterministic expiry after the
initial check but before the PG lock. Unix cases cover both sender-first and
server-first deadlines, including an absent delegated deadline. TLS cases use the
real client conversion with different frontend/storage monotonic origins. Codec
tests pin optional branches, truncation/trailing rejection and exact maximum
framed admission. Existing append contention/recovery tests remain in scope.

This is **not** persisted issuer/window binding, settled-prefix confirmation or
physical write/cleanup exclusion. Owner/resource-obligation schema integration
and all six original cleanup regressions remain open. Future append-window
issuance must derive its persisted issuer from this primary boundary; merely
checking the currently live lifetime is not replacement-primary authorization.

Validation: 270 focused tests passed. Full Nextest passed 9,142 tests (one slow,
six skipped) in 265.901s; strict all-target/all-feature Clippy, storage boundary
checks, formatting, shell syntax and diff checks passed. All pre-existing long
hex specimens in the changed RPC/authentication tests remain present. The six
skipped cleanup regressions are unchanged and remain unresolved.

## Explicit Command Reservation Disposition — 2026-10-31

The owner-obligation audit requires release timing to remain separate from proof
presence. Before adding the planned Direct PUT proof or persisted obligations,
replace the two independent foreground/open-time stream-creation exceptions with
one exhaustive command classification. `MetadataCommandReservationDisposition`
now distinguishes no command-carried proof, release on application, retention for
a live PUT stream, and stream-creation cleanup after an applied abort.

Both foreground terminal cleanup and local-open convergence consume that
classification. Authorization-proof selection deliberately excludes abort's
auxiliary cleanup proof: abandoning an unstarted abort must not release a still-
live session's reservation. Pending route dependencies include all carried
proofs, including retained PUT creation and abort cleanup, regardless of whether
successful application releases them. Ordinary release validation and existing
idempotent/error handling remain in place; classification alone grants no new
authority to release a proof.

The match is exhaustive over command variants. The currently proof-free
`ReserveObjectGenerationCommand` is also destructured without `..`, so adding the
planned proof field forces an explicit policy decision even without a new enum
variant. That future introducing command must retain the proof through staging,
not inherit ordinary release-on-apply. This safeguard does not claim that adding
arbitrary fields to every other payload is compiler-inventoried.

Independent disposition expectations cover the immutable decoded object-command
corpus plus abort with a cleanup proof. A three-replica storage regression builds
PUT and UploadPart creation through the primary, persists an exact pending command
and applies it on every replica. It verifies that application alone releases
neither proof, then checks foreground cleanup/retry and actual close/reopen
convergence: PUT retains its exact creation reservation; UploadPart releases only
its creation reservation. A second same-object proof remains an exact-row canary
(a separate finalization proof for UploadPart). Subsequent abort releases only the
PUT session's proof and preserves that canary. Existing unstarted creation/abort
restart regressions separately pin abandonment behavior.

No durable or wire format changes, SQL statements, commits or fsyncs are added.
This is a prerequisite refactoring of current responsibility, **not** a persisted
release receipt or durable owner/window implementation. The coordinated schema,
command and runtime integration remains next; the six cleanup regressions remain
open. The previous append-builder ledger has been moved beside this entry so the
embedded-publication validation remains attached to its original section.

Validation: six focused tests passed. Full Nextest passed 9,144 tests (one slow,
six skipped) in 255.215s. Strict all-target/all-feature Clippy, storage boundary
checks, formatting and diff checks passed. The six skipped cleanup regressions
are unchanged and remain unresolved.

## Primary Direct PUT Reservation Proof Input — 2026-10-31

The next missing primary-builder input from the integration audit now reaches
the actual object-PG owner. `ActivePutObjectRoute` passes its already validated
bucket-write reservation unchanged through the generation-reservation workflow,
the scoped node client and the installed Unix/TLS builder. Embedded and Unix
clients reject crossed bucket, key/absent target, operation or epoch before PG
inspection or transport; the storage-node capability independently checks the
same subject before delegating to the local builder. The builder's existing
placement, candidate, lifetime and captured-deadline checks remain in place.

This is **structural proof-subject preflight**, not another bucket-PG durable
record validation or a persisted ownership link. The existing admitted frontend
continues to validate the exact live reservation. The introducing metadata
command still does not carry the proof; attaching it to retained owner/resource
state and preserving it through recovery remains part of the coordinated state
slice. No extra bucket-PG lookup under the object-PG lock, SQL transaction or
fsync is added. Owner-local allocator fixtures supply explicitly structural
metadata-only proof inputs, rather than acquiring unrelated bucket state and
altering their original command/placement scenarios.

RPC v32 adds the full proof to kind 176. The frame cap adds its maximum encoded
size, not a nested request's unrelated header. Command v12 and schema v9 remain
unchanged. The previous raw request specimens, v31 frames, 37 creation specimens
and authentication composites remain immutable rejection evidence; current v32
fixtures are appended. The unchanged append payload still matches its original
v31 payload specimen under the new containing frame.

Coverage includes same-PG crossed proofs through the embedded route, a Unix
client with no listener (proving pre-transport rejection), and raw requests to
the installed server (bypassing sender checks). Complete checkpoint, pending
slot, log index and generation observations remain unchanged after each rejected
request and a successful read-only build. A peer regression compares the entire
transported proof. Authenticated TLS exercises the actual fenced client with
independent sender/receiver monotonic origins, including receiver-side expiry.

Durable owner/windows, release obligations, settled-prefix confirmation and
physical exclusion remain unimplemented. All six original cleanup regressions
remain open; this input slice does not claim to fix them.

Validation: 231 focused tests and the installed-server version-rejection test
passed. Full Nextest passed 9,147 tests (two slow, six skipped) in 241.111s;
strict all-target/all-feature Clippy, storage boundary checks, formatting and
diff checks passed. The initial focused run exposed the test helper's unintended
bucket-acquisition prerequisite and an unplaceable positive fixture; those were
corrected without changing production validation. Both codec and installed-server
future-version rejection matrices were updated from v32 to v33, adding v31
rejection. All prior long RPC/authentication hex specimens remain unchanged.

## Captured Placement Input for Append Construction — 2026-10-31

Fresh append construction now receives the immutable placement catalogue already
captured by the request. The embedded builder loads the durable placement
generation under the same object-PG lock as session/target/witness validation:
the PUT generation reservation, or the enclosing MPU generation for UploadPart
(not the separately reserved part payload generation). It verifies the selected
data PG, placement epoch, configured EC shape and selected route availability
before command-ID allocation. The installed Unix/TLS handler supplies its own
captured configuration; the Unix client additionally rejects crossed epoch/EC or
unavailable selected routes before transport.

The multi-PG regression exercises embedded and installed Unix PUT/UploadPart
builders. Its MPU and part generations deliberately select different data PGs;
crossed PG, epoch and coherent-but-crossed EC witnesses must reject without
changing checkpoint, pending slot, log index, session or segment rows. Raw Unix
requests bypass sender checks to pin independent receiver validation. A successful
build is read-only, and the already-built command still applies/replays after
retiring the issuer lifetime.

No new wire field, format version, transaction or fsync is introduced. Replica
apply and exact pending-command recovery are unchanged: this is fresh-builder
preflight, not a new active-route requirement for converging accepted work.
Persisted owner/window state, release obligations, settled-prefix confirmation
and physical exclusion are still unimplemented; all six cleanup regressions
remain open.

Validation: all 60 focused tests passed. Full Nextest passed 9,147 tests (two
slow, six skipped) in 280.642s. Strict all-target/all-feature Clippy, storage
boundary checks, formatting and diff checks passed. Initial focused failures
were assertion mistakes in the new regression: public error formatting redacts
diagnostics. The corrected tests match the typed error and use storage's
test-only remote-detail accessor, retaining exact `PayloadDecode` and placement
diagnostic assertions. The six skipped cleanup regressions remain unresolved.

## Primary Append Preparation Binding — 2026-10-31

The integration review found a fresh-command bypass of the implemented local
single-outstanding-slot rule: the new primary builder accepted an append without
any preparation. The regression reproduced an index-zero/VID-one command at log
index two immediately after creation, while the local cursor was still one.
Application would advance that cursor without the primary ever consuming a slot.

Fresh construction now requires either an exact already-appended record or the
single locally prepared next segment: its index follows the appended prefix, its
VID follows the prefix's maximum VID, and the local cursor equals that VID's
checked exclusive end. The preflight result comes from the session/segment reads
already made under the same primary PG lock, before command-ID selection. It adds
no SQL query, transaction, fsync or format change. Unix/TLS dispatch uses the same
local builder and rejects preparation mismatch as `PayloadDecode`.

Replica apply deliberately does **not** impose primary-local consumption. It
continues validating the immutable command and advancing its floor, including on
replicas which never prepared the segment. The preflight type keeps that result
separate from fresh primary issuance. Exact earlier append matching remains valid
after later preparations/appends advance the cursor; already-built replay and
recovery remain unchanged.

Embedded and installed-Unix PUT/UploadPart tests reject unprepared, skipped-VID
and crossed-index requests before log/slot/session/segment changes, then build
successfully after real scoped preparation. Existing deadline/TLS fixtures now
prepare through the scoped API rather than manufacturing an unallocated positive
append. An owner-local canary independently crosses the consumed cursor, the
prefix's next VID and next index, preserves state after rejection, and proves
replica application/replay converges without local preparation. Its malformed
extra-consumption case is explicitly owner-local, not a production writer path.

This closes a fresh-construction bypass, **not** replacement-primary issuance or
durable cleanup ownership. A local cursor can survive reopen and replicas receive
only its applied floor; neither proves the future persisted issuer/window binding.
Decoded recovery commands still rely on their existing command authorization and
apply validation, not this local preflight. The coordinated owner/window records,
retained obligations, confirmation, physical exclusion and all six cleanup
regressions remain outstanding.

Validation: the new no-preparation regression first failed against the previous
builder, returning an append envelope as expected. All 73 focused tests then
passed with the check installed. Full Nextest passed 9,148 tests (one slow, six
skipped) in 261.494s. Strict all-target/all-feature Clippy, storage boundary
checks, formatting and diff checks passed. An initial focused fixture failure
was corrected by supplying the exact loaded session/generation to preparation,
rather than bypassing its required authority with `None`. No production check
was relaxed. The six skipped cleanup regressions remain unresolved.

## Direct PUT Proof Handoff Integration Gate — 2026-10-31

The next command change cannot safely be limited to adding the already-validated
bucket proof to `ReserveObjectGeneration`. That is necessary evidence, but the
frontend still owns release until terminal publication. This audit refines the
coordinated state slice; it does not change production ownership or formats.

### Current ownership path

- [`put.rs`](../crates/server-core/src/coordinator/put.rs),
  `put_object_from_authorized_write_on_admitted_route`, sets
  `proof_transferred_to_command` only immediately before `commit_direct_object`.
  Generation reservation, encryption and shard
  writing occur earlier. Any returned error from those steps selects
  `BucketWriteSnapshotAction::release`.
- [`bucket_delete.rs`](../crates/storage/src/cluster/request_ops/bucket_delete.rs),
  `with_put_object_bucket_write_snapshot_for_command_with_route_validation`, acts
  on that decision by releasing the durable bucket reservation; the snapshot
  callback does not return exact generation-publication ownership evidence.
- [`payload_operations.rs`](../crates/storage/src/cluster/payload_operations.rs),
  `reserve_put_object_generation_with_route_validation`, returns only a generation
  or `ObjectPgActionError`. It can leave an installed/partially applied command
  for recovery. The error type does not tell the coordinator whether its own
  generation command took custody of the bucket proof, as opposed to an unrelated
  contender blocking progress.
- [`metadata_operations.rs`](../crates/storage/src/cluster/metadata_operations.rs),
  `validate_metadata_command_bucket_write_reservation_inner`, validates a
  command-carried primary proof against the durable bucket-PG record. Once the
  reservation command carries that proof, releasing it on the earlier frontend
  error path can prevent later validation of otherwise recoverable work.

Today the generation command carries no bucket proof, so this audit does not
claim the additional validation failure already occurs. It is a dependency of
the proposed format change. Conversely, simply setting the frontend transfer
flag before calling reservation is also wrong: a rejected build/install would
then leave no durable recipient responsible for releasing the proof.

### Required closed handoff contract

The storage-owned attempt must distinguish the following states/outcomes; a
generic error, a boolean set before a call, or the mere presence of some pending
command is not sufficient evidence.

| Boundary/outcome | Owner of exact bucket-proof release responsibility |
| --- | --- |
| Before installation; read-only build rejection; authoritative proof that this attempt was never installed | Frontend reservation scope; release normally. An unrelated contender does not acquire this attempt's proof. |
| This attempt's exact installation may have applied, publication started, or any replica applied | Exact command/recovery handoff; frontend must not independently release. Preserve the full introducing envelope and pending route dependencies until convergence/authorized abandonment. |
| Generation reservation and retained owner applied, including lost success response | Retained owner; generation, owner, proof obligation and first window are committed together. Do not require the frontend to receive a capability before durable custody exists. |
| Encryption, shard-write or pre-terminal failure after reservation | Retained owner/cancellation transition; preserve its proof and all issued-window obligations before relinquishing the frontend. Do not restore frontend-only release because no terminal command was installed. |
| Terminal publication or authoritative unstarted abandonment | The exact command/owner transition decides existing release timing. Persist unresolved responsibility before removing its previous owner; partial/ambiguous publication still converges. |

Keep logical request failure separate from this ownership result. The scoped
storage workflow should own the decision, including cancellation/drop; the
coordinator should map the S3 error without reconstructing custody from an error
code. A successful return must bind the same generation, reservation identity,
bucket incarnation and exact stable bucket proof as the durable owner. Recovery
of a matching reservation must not substitute a newly supplied proof. If exact
installation outcome is unknown, retain/handoff conservatively rather than
release on a failed inspection.

Persisting the proof only in the introducing log/receipt is insufficient after
compaction or transfer. Add it to the retained resource obligation in the same
apply transaction. Preserve the explicit disposition: reservation apply retains
the live proof for staging; it is not ordinary release-on-apply. The pending
command pins the bucket-PG route until materialization, and the owner pins that
exact PG/epoch until release responsibility is durably retired. No new normal-path
transaction or standalone proof-handoff command is selected.

### Implementation and regression consequences

Land the introducing-command proof, retained owner/resource state, central
subject/identity validation, pending route references and frontend handoff
together. Update the exact command matcher and Unix response validation to bind
the transported proof, not merely bucket/key/generation. Keep already-published
and authoritatively unstarted outcomes distinct through reissue and recovery.

The current owner-local raw generation helper supplies only a structural proof,
and numerous publication fixtures use proof-free generation commands. They must
not gain a `cfg(test)` bypass around new production proof validation. Fixtures
which exercise fanout/recovery must acquire real bucket reservations and preserve
their unrelated-resource canaries. Metadata-only codec/store fixtures can supply
explicitly synthetic proof records without claiming bucket-PG authorization.

Acceptance must independently cover rejection before install, unrelated-contender
failure, accepted-install response loss, witness-first partial apply, slot-removal
failure after apply, lost reservation success, encryption/write failure before
terminal publication, cancellation, reissue, checkpoint/import and reopen. For
each case assert exactly which component owns the proof, that an unrelated proof
is unchanged, and that later authorized recovery converges. Include the actual
coordinator path so its snapshot callback cannot silently release a handed-off
proof. Successful reservation must demonstrably retain the proof, and successful
terminal publication must retain the existing release timing.

No production code or encoding is changed by this audit. All six cleanup
regressions remain open. Validation: source call-chain review and
`git diff --check`; no Rust test run for this documentation-only change.

## Storage-Owned Direct PUT Frontend Scope — 2026-10-31

Implemented the frontend-boundary prerequisite identified by the preceding
handoff audit. `ActivePutObjectRoute::with_direct_put_scope` issues an opaque,
non-cloneable `DirectPutWriteScope` carrying the acquired bucket proof. The
coordinator no longer maintains `proof_transferred_to_command` or chooses a
`BucketWriteSnapshotAction`. Storage records the handoff when the scope enters
terminal publication; an ordinary earlier error still releases the proof through
the existing snapshot wrapper.

Each scope permits one generation attempt (including an ambiguous failure), so
one proof cannot back multiple independently live generation tokens. Generation
and payload tokens borrow the scope and retain its exact proof.
Staging and commit reject crossed scopes, including two valid reservations for
the same object. Terminal preparation must supply that same proof. After terminal
handoff, the scope rejects further generation allocation, staging or commit.
The raw route reserve/write/commit entry points are no longer downstream-public.
Compile-fail examples pin the scope lifetime and private terminal entry point.

This preserves existing release timing; it is not the early durable handoff in
the preceding contract. In particular, panic/cancellation, lost generation
reservation responses and pre-terminal crash cleanup are not resolved by this
scope. The introducing-command proof, retained owner/window state, central
validation and route retention must still land together before changing custody
at reservation. There are no schema/command/RPC changes and no new fsyncs. All six
ignored cleanup regressions remain open.

Owner-local regressions cover an early error with an unrelated exact-row canary,
same-key proof substitution before terminal entry, successful publication, and a
failed trailing replica. The last case verifies that returning from the callback
preserves the pending command's proof, then exact recovery releases only that
proof. Nested same-object scopes also exercise generation and payload crossing.

Validation: the final `cargo nextest run --offline` passed all 9,152 tests
(one slow, six skipped) in 257.207s, including all four new scope regressions.
All 35 storage doctests passed, including both new compile-fail examples.
`cargo clippy --offline --all-targets --all-features -- -D warnings`, the storage
boundary checker, `cargo fmt --all --check` and `git diff --check` passed.

## Direct PUT Scope Unwind Release — 2026-10-31

The preceding scope still relied on its callback returning a
`BucketWriteSnapshotAction`. A panic before terminal handoff skipped the snapshot
wrapper's normal release even when the bucket PG remained available. Added
storage-owned unwind handling without changing the reservation-command format
or moving proof custody earlier.

The scope now has an explicit returned-to-wrapper state. Consuming normal finish
selects the existing release/transfer action and disarms Drop; normal release
errors continue through the snapshot wrapper. If the callback unwinds while the
scope owns the proof, borrowed generation/payload guards unwind first, then the
scope makes one exact retained bucket-proof release attempt. It uses the existing
bucket-work deadline for connection/lock admission, with no retry loop. This is
not a bound on total unwinding time: existing payload/generation cleanup and
already-admitted persistence have their own completion behavior.

Terminal entry disarms frontend unwind release before invoking the publisher.
Neither a failed terminal call nor unwinding afterward authorizes the frontend
to release a proof potentially owned by an accepted command. Release failure on
unwind emits only the bounded diagnostic category and preserves the original
panic; it does not report cleanup completion or claim a durable handoff.

Regressions independently cover cancellation before generation allocation, with
a live generation token, with staged ciphertext, and after a successful terminal
publication leaves a failed trailing replica. They compare the complete unrelated
same-key reservation row, verify generation/shard cleanup where applicable, and
verify that pending terminal authority survives until exact recovery. An
owner-local replacement-token adversary forces retained release to fail and
checks that neither its replacement row nor the original panic is lost.

This handles synchronous unwinding while the process and stores remain alive.
It does not handle aborting the process, forgotten guards, failed remote cleanup,
or the accepted-generation/response-loss custody problem. The coordinated owner/
window, introducing proof and release-obligation integration remains next; all
six original ignored cleanup regressions remain open. No new normal-path
transaction or fsync is added.

Validation: all seven focused scope tests passed. Full `cargo nextest run
--offline` passed all 9,155 tests (two slow, six skipped) in 278.896s. All 35
storage doctests, strict all-target/all-feature Clippy, the storage boundary
checker, formatting and `git diff --check` passed.

## Owner Introduction And Proof Custody Model — 2026-10-31

Added the independent storage-owned
[`introduction.rs`](../crates/storage/src/cleanup_ownership_model/introduction.rs)
model to cover the introducing-command gap left by the earlier terminal
publication model, which starts with an owner already durable. This is test-only
protocol modelling, not a new production command, schema or write grant.

### Boundaries and evidence

The finite state graph has one introducing command, three object-PG replicas,
one own bucket proof and one unrelated proof, one restart and at most one failed
release. It separates dispatch, primary acceptance, response receipt/loss,
publication-start, witness/primary/trailing apply and inspection, slot removal,
per-replica log compaction, exact release, observation of absence and owner
retirement. Each replica's creation apply atomically stores generation, retained
owner, exact proof obligation and its introducing log record.

Publication/cancellation after creation is deliberately collapsed to an
authorized, converged owner transition. Both remove the generation reservation
while retaining the owner and unresolved release obligation. Successful live
proof release need not wait for owner retirement. Restart preserves durable rows
and pending intent but forgets inspections, release confirmation and frontend/
recovery activity; authorized recovery must reacquire evidence.

The positive exploration requires reachable canaries for:

- Authoritative rejection releasing only the frontend's proof.
- Frontend loss before primary acceptance retaining an unresolved bucket record.
- Lost installation reply leaving a pending custodian before owner application.
- Witness-first owner materialization surviving restart before primary apply.
- Staging proof identity surviving complete log compaction and restart.
- Successful publication releasing its live proof while the retained owner stays.
- Failed release, restart and eventual exact release/owner retirement.
- Authoritatively unstarted abandonment releasing before its pending slot is removed.

Ten independently enabled mutations must each produce the expected invariant
violation: release on unknown installation, release on creation apply, generation
without owner, proof kept only in the command log, early slot removal, abandoned
slot removal before release, owner loss on restart, retirement before release,
retirement after release but before observing it, and release of the unrelated proof. The
ordered regression additionally walks lost reply → replica convergence → complete
log compaction → cancellation → failed release → restart → exact release →
retirement. It asserts that restart finds owner-carried proof without log rows.

### Explicit limits and implementation consequence

This does **not** model cross-PG proof validation/installation atomicity, lease
expiry/heartbeat races, storage RPC authorization or encoding, multiple owners,
route history, physical files/acknowledgements or the full terminal fanout. The
accepted-install transition assumes exact proof validation; it is not evidence
that today's separate bucket-PG check and object-PG install close that interval.
The previous physical, publication and issuer models are not automatically
composed with this state graph.

In particular, frontend loss while dispatch is still unaccepted retains the
bucket reservation but has no fictitious object-PG custodian. Discovery and safe
retirement of that pre-install reservation remain an explicit implementation/
modelling gate. The graph proves neither eventual progress nor a lease-expiry
algorithm. An unknown transport outcome cannot be treated as authoritative
rejection merely because an immediate inspection sees no owner: the request may
still arrive. The introducing command must preserve exact responsibility once
accepted, and its materialized owner must retain that evidence independently of
the compactable command log before the pending custodian is removed.

The coordinated production slice remains unchanged: owner/window records,
introducing proof, central validation, retained route references and frontend
handoff must land together. No optional, unenforced owner fields or extra
ordinary-path fsyncs were added, and all six production cleanup regressions
remain open.

Validation: all three focused tests passed. The positive graph exhausted
21,559 states / 69,833 transitions; all ten independent mutations produced their
expected counterexamples. Exploration has a 250,000-state hard cap; reaching it
is a failure, not a bounded success. Full `cargo nextest run --offline` passed
all 9,158 tests (two slow, six skipped) in 239.617s. Strict all-target/all-feature
Clippy, the storage boundary checker, formatting and `git diff --check` passed.

## Pre-Install Orphan Retirement Contract — 2026-10-31

### Existing boundaries and the missing distinction

The source audit distinguishes three operations which must not be conflated:

- `acquire_durable_bucket_write_reservation` in
  `pg_store/metadata/metadata_store.rs` persists the bucket proof before the
  object-PG generation command exists. The row already carries bucket
  incarnation, operation, target context and originating epoch. It does not
  carry a separate immutable introduction cutoff or object-PG custody state.
- `try_install_object_pg_built_pending_command` in
  `cluster/metadata_operations.rs` serializes build/install with a
  **cluster-runtime-local** PG lock. Remote construction and pending installation
  are separate operations. That frontend lock is not a storage-primary
  installation/inspection barrier shared by all frontends.
- `wait_for_durable_bucket_write_reservations_empty` in
  `cluster/request_ops/bucket_delete.rs` currently releases expired reservation
  records before continuing its drain. It does not first establish absence of
  an introducing pending command or retained owner. This existing expiry policy
  is **not** the future owner-aware orphan-discovery protocol.

The previous model intentionally left the pre-install row unresolved. A scan of
that row can discover the attempt, but neither expiry alone nor an early empty
object-PG inspection proves it was never accepted. In particular, checking an
effect fence immediately before a write does not bound the write's completion
time: a transaction can finish after the cutoff. Inspection must also serialize
against that in-flight installation.

### Selected integration contract

1. Persist an immutable **introduction acceptance cutoff** with the exact bucket
   reservation in its existing acquisition transaction. It is distinct from the
   renewable bucket lease and from subsequent physical-write window horizons.
   Captured route authority can shorten it, never extend it; heartbeat and
   restart must not restart it. Use the established absolute conservative
   wall-clock upper bound and receiver-local monotonic binding, not a transmitted
   duration or monotonic timestamp. Clock uncertainty fails closed. The row must
   also retain enough exact subject/routing evidence to locate the authoritative
   object PG until the handoff or orphan release is confirmed.
2. Every fresh installation using this proof must enforce that cutoff at the
   **storage-primary** pending-slot boundary. Hold the shared primary
   installation/inspection serialization from the final check through commit
   (or authoritative rollback). A request checked earlier, delayed in transport
   or queued for the lock must recheck there. A commit that completes after the
   cutoff remains an accepted command, not an orphan. Replica convergence of an
   already accepted command is not fresh installation and must not independently
   fail because its acceptance cutoff passed.
3. After the immutable cutoff, inspect the exact proof's pending/materialized
   custody under that same serialization. Return an opaque classified result:
   **pending custodian**, **retained owner**, or **closed and absent**. The last
   result requires a settled authoritative publication prefix, no relevant
   pending/partial creation, and no materialized owner; absence of command-log
   rows alone is insufficient after compaction. An unavailable witness,
   unsettled route, failed inspection or unknown transport result is not absence.
   Do not retire the bucket row in those cases.
4. Release a pre-install orphan only from closed-and-absent evidence bound to
   the exact reservation identity, bucket incarnation, object subject, cutoff
   and authoritative route/prefix. Release is a separate bucket-PG transaction;
   **do not hold the object-PG serialization or physical locks across it**.
   Absence stays valid because fresh installation is closed, including delayed
   duplicates. Retry failed/ambiguous release idempotently; conflicting replacement
   rows must remain untouched. A restart can discard the inspection and inspect
   again from the durable bucket record. No additional orphan tombstone is needed
   *provided* closure survives restart and route replacement as required below.
5. For either positive custody result, hand the attempt to exact command/owner
   recovery. Do not let the orphan reaper, bucket-delete expiry path or frontend
   release a still-owned proof merely because its lease expired. Existing normal
   successful release timing remains provenance-specific; this contract does not
   force a completed upload to keep a live bucket reservation until physical
   owner retirement.

Replacement-primary safety is a hard integration gate: the successor must
settle the old accepted prefix/materialized owner state, and the old issuer must
be fenced from fresh installation, before it can certify closed-and-absent.
A local lock plus a local empty database is not sufficient. The existing
settled-prefix and issuer-lifetime designs supply requirements for that work;
this slice does not implement or prove their composition. Retain the required
bucket/object historical routes throughout the unresolved handoff. Do not infer
historical absence from an unavailable or pruned route.

This uses the existing acquisition, pending insertion and owner-materialization
transactions, plus exceptional read-only inspection and the existing exact
release transaction. It proposes no additional ordinary-path fsync. Persisted
cutoff/discovery fields, owner proof obligations, command/wire/state version
advances, owner-aware bucket draining and frontend handoff must be implemented
together. The current frontend scope's release policy is not changed by this
model, and no proof-only command change is safe to land independently.

### Bounded evidence and remaining implementation tests

[`preinstall.rs`](../crates/storage/src/cleanup_ownership_model/preinstall.rs)
models two delayed duplicate deliveries, separate bucket proof validation and
object installation, one immutable cutoff, one restart and one failed exact
release, with a foreign-proof canary. Installation keeps serialization across
its check/commit gap, so time can advance while it is still in flight. A separate
ordered test explicitly proves inspection cannot run in that gap, then observes
the pending custodian and rejects the delayed duplicate after the cutoff.
Another ordered trace requires failed release → restart → reinspection →
release → rejection of both previously validated deliveries. It asserts that
restart forgets the inspection, that release is disabled until reinspection,
and that stale bucket-PG validation cannot authorize installation afterward.

The positive exploration must exhaust and reach orphan release with both
deliveries rejected, cutoff during installation, pending-custodian discovery,
compacted-owner discovery after restart, and release retry across restart. Six
independent mutations require counterexamples: early inspection, inspection
outside serialization, missing installation fence, pending loss on restart,
ignoring an owner after log compaction, and releasing the foreign proof.

This is an **abstract candidate model**, not production-connected verification.
It assumes one settled primary and healthy clocks; it does not model route
replacement, replica reconciliation, lease renewal, wire rebinding, physical
writes or terminal release. Materialization collapses converged creation/slot
retirement; the preceding introduction model covers those replica steps
separately. Exhaustion proves the stated finite safety properties, not fairness
or eventual cleanup. Neither model substitutes for installed Unix/TLS delayed
delivery, locked-insertion expiry, failed release/restart, or successor-primary
regressions in the coordinated implementation. All six original cleanup
regressions remain open.

Validation: all four new model tests passed. The positive graph exhausted
295 states / 721 transitions, and all six independent mutations produced their
expected counterexamples. The 100,000-state cap fails rather than reporting
success if exhausted. Full `cargo nextest run --offline` passed all 9,162 tests
(two slow, six skipped) in 238.097s. Strict all-target/all-feature Clippy, the
storage boundary checker, formatting and `git diff --check` passed.

## Direct PUT Successor Evidence — 2026-10-31

### What the existing recovery primitives establish

The preceding pre-install model assumes a settled authoritative primary. This
slice tests the existing store and peering primitives that the implementation
must use to establish that precondition; it does not simply extend the model
with an unconditional "successor settled" transition.

`reconstruct_pg_peering_from_primary_retained_log` in `peering.rs` rejects a
pending slot on any supplied replica, a replica ahead of the selected primary,
and equal-index proof disagreement. Its input is **not itself authority**: the
helper does not establish that the supplied replicas form the certified route
or that an unavailable old primary has been fenced. The future closed-and-absent
result still needs the admitted current-primary route, the correct witness/
certified-import proof, and old-issuer closure from the earlier contract.

The control-plane `pending_metadata_command_recovery_for_pg` listing is based
on reported observations and rejects conflicting reports. Absence from that
listing is not synchronous proof that an unreported, delayed or unavailable
primary never accepted a command. Do not use listing absence as the orphan
retirement certificate.

For Direct PUT specifically, today's `ReserveObjectGeneration` application
persists a generation reservation/high-water but no retained owner, physical
window or bucket-proof obligation. Keeping that row across transfer is necessary
evidence, not sufficient cleanup authority. The coordinated production slice
must add those missing records to the same canonical-state machinery.

### Production-store regression matrix

[`direct_put_introduction_evidence_tests.rs`](../crates/storage/src/pg_store/metadata/direct_put_introduction_evidence_tests.rs)
adds two tests using production pending-slot, apply, checkpoint, compaction,
import and reopen methods. They are metadata-only: no fabricated encrypted
payload or physical-write grant is involved.

The first starts three stores from an identical checkpoint and selects a still
empty successor candidate while the original primary retains the introduction:

| Durable stage | Candidate's local observation | Required recovery result |
| --- | --- | --- |
| Accepted but publication unstarted; original primary reopened | No generation row, empty slot, valid local checkpoint | Reject reconstruction because the original primary has the exact pending command |
| Publication-start recorded but no replica applied | Same locally empty candidate | Reject on the pending command; preserve publication-start and every inspected state |
| Witness applied, original primary still pending | Candidate still empty; witness has the reserved generation | Reject pending evidence; even when the original primary is omitted from the input, reject the witness-ahead prefix |
| All three applied and the exact pending slot removed | Matching materialized reservation and prefixes | Positive `AlreadyConverged` result |

The tests compare complete pending-slot bytes/identity, publication-start,
replica state and SQLite change counters around rejected inspection. The
original primary is reopened both before publication starts and after witness
application. The final primitive slot removal is deliberately labelled: this
test does **not** execute distributed authorized recovery or activate a new route.

The second applies Direct PUT reservation and a real later command, then uses
the production compactor with a one-receipt window. The introducing log and
receipt disappear while the exact reservation and next-generation high-water
remain. Reopen preserves both. Two successive checkpoint-base imports (epochs
two and three), each followed by reopen, preserve the complete table blocks,
table digests, state digest and reservation subject/generation while rebasing
the applied index/hash to zero. No exact old-command receipt is manufactured.

### Remaining gate

These tests pin concrete existing behavior used by the design. They do not
prove safe orphan release, route-history retention, old-primary fencing,
cross-PG proof handoff or physical cleanup, and do not add a new command, schema,
RPC, transaction or fsync. In particular, an unavailable old primary plus an
empty candidate is still **unknown**, unless the actual certified recovery/
transfer protocol establishes the required settled prefix and issuer closure.
The next coordinated state integration must preserve this evidence for the
retained owner/resource obligation, not just today's generation reservation.
All six original cleanup regressions remain open.

Validation: both focused tests passed. Full `cargo nextest run --offline` passed
all 9,164 tests (two slow, six skipped) in 237.579s. Strict all-target/all-feature
Clippy, the storage boundary checker, formatting and `git diff --check` passed.
The initial focused run failed before exercising storage because the fixture
label encoded to a 38-character session ID; shortening the label fixed that
fixture error. No production test failure was suppressed or reclassified.

## Captured File And Acknowledgement Participants — 2026-10-31

Starting the owner-state integration exposed one remaining data-shape issue:
`PayloadPlacementInput::require_segment_placement` computed canonical file
locations and then discarded them. The future owner must retain those exact
locations **and** the data-PG acknowledgement primary, which is not inferred
from a shard index and need not hold any file when there are spare candidates.

The shared resolver in
[`payload_placement.rs`](../crates/storage/src/payload_placement.rs) now exposes
`capture_segment_participants` inside the node runtime. It returns owned
placement data: the acknowledgement-primary NodeId and the complete canonical
shard-index-ordered locations, each with its data PG and epoch. The old preflight
entry point delegates to that same implementation and discards the result.
Existing Direct PUT, creation, preparation and append callers therefore retain
their current validation and error behavior; there is no second placement
algorithm or additional placement pass.

This result is **not an authority token or a complete window descriptor**. The
caller still has to bind the exact segment hash/VID, owner, generation, issuer,
EC/geometry, immutable cutoff and publication evidence in the future durable
record. The extraction does not serialize live route authority or supply
permission to write/delete at any returned participant. It adds no lookup,
RPC, mutation, durable format change or fsync.

[`payload_participant_tests.rs`](../crates/storage/src/storage_node_server/payload_participant_tests.rs)
exercises both the embedded-map and captured storage-node-config inputs:

- A 2+1 layout on four candidates chooses the unused node as acknowledgement
  primary and asserts it is absent from the complete three-file placement.
  Both resolver inputs must return identical participants, and captured file
  locations must match the existing placed-write resolver for the exact
  segment hash/generation.
- Acting-set reordering preserves shard-index assignment. Changing only the
  primary changes only the acknowledgement owner; the earlier owned result
  remains exact, and an independent PG retains its own route subject/primary.
- Independent negatives pin operation-epoch mismatch, route-epoch mismatch,
  Peering state, a primary outside the acting set, crossed EC shape, duplicate
  candidates and a missing PG route. Each asserts its specific error class for
  both inputs, followed by a valid positive canary.

These are resolver tests, not installed Unix/TLS transport or retained-window
confirmation tests. The next durable record must consume this captured result
under the primary build lock; persisting only a placement seed and recomputing
against a later map would lose the guarantee. No optional owner fields were
added to live commands, and all six original cleanup regressions remain open.

Validation: both participant-capture tests passed. Full `cargo nextest run
--offline` passed all 9,166 tests (one slow, six skipped) in 259.970s. Strict
all-target/all-feature Clippy, the storage boundary checker, formatting and
`git diff --check` passed. Initial test setup errors (a private import path and
omitting the convenience map opener's required node 0) were corrected without
changing production validation.

## Direct PUT Reservation Kind At Apply — 2026-10-31

Reviewing the creation/replay boundary for retained owners exposed the reverse
of the stream-creation adoption gap fixed earlier. The primary Direct PUT builder
requires reservation absence, but decoded `ReserveObjectGeneration` application
used the generic idempotent reservation allocator directly. An existing PUT or
UploadPart session with the same reservation ID, bucket, key and generation was
therefore accepted as if it were a standalone Direct PUT reservation reissue.

Application now checks for the stream session before invoking the allocator, in
the existing apply/log transaction. A live session is a different durable owner
kind, even when every reservation field matches. Session lookup failures other
than authoritative absence propagate. No local clock, network call, new table,
format change, transaction or fsync is introduced; this adds one local session
lookup to standalone reservation apply. Stream creation still reserves its
generation directly inside its own atomic creation transaction.

An unconditional existing-reservation rejection would be incorrect: a genuine
same-payload Direct PUT reissue can encounter materialized state after the
original log and receipt have been compacted. The allocator's existing exact
subject/generation idempotence remains available when no stream owns the ID.
This check does not manufacture provenance for a standalone row, bind a future
owner/window descriptor, or make a generation reservation into bucket-proof
custody. Those remain part of the coordinated integration, as do cleanup/release
authority and physical write exclusion.

The owner-local regressions cover:

- PUT and UploadPart creation followed by a separately encoded/decoded standalone
  reservation envelope with identical ID, bucket, key, generation **and creation
  time**. Both must reject with the ownership diagnostic before SQLite changes,
  preserving the complete session, reservation, high-water, checkpoint and log,
  including after reopen.
- Exact Direct PUT log replay, followed by original-log/receipt compaction and
  reopen, then a decoded same-payload command at a new log index. Application
  must succeed without changing any canonical table block. This is store-level
  reissue application evidence, not a distributed recovery authorization test.
- Release followed by reopen and another decoded reservation reissue. The durable
  generation high-water must reject resurrection without changing the released
  checkpoint or advancing the log.

All six original cleanup regressions remain open. This is a central creation-kind
validation correction, not implementation of durable owner/window state or the
pre-install orphan-retirement protocol.

Validation: both focused regressions passed. Full `cargo nextest run --offline`
passed all 9,168 tests (one slow, six skipped) in 263.745s. Strict
all-target/all-feature Clippy, the storage boundary checker, formatting and
`git diff --check` passed. No format fixtures or ignored tests changed.

## Atomic Stream Creation And Abort Cleanup — 2026-10-31

The follow-up audit found cleanup paths that still assumed the old two-command
stream creation protocol, despite atomic creation and the new apply ownership
checks:

- The low-level PUT stream-creation wrapper issued `ReleaseObjectGeneration` on
  failure when no pending command retained its bucket proof. If creation rejected
  a pre-existing generation or session, this cleanup could delete the generation
  it had correctly refused to adopt.
- Ordinary abandonment and the shared historical-recovery derivative classifier
  still authorized a generation-release follow-up for `CreateStreamUpload`.
  Authoritatively abandoned atomic creation applied neither the session nor its
  generation. There is no independent resource from that creation to release.
- Stream scavenging and bucket deletion issued another generation-release command
  after successful abort, although abort already deletes session and generation
  together in its apply transaction.

Removed those calls and the obsolete stream-create recovery derivative. Failed
creation retains its existing exact bucket-proof disposition and error mapping;
it no longer guesses ownership of a generation from the requested session ID.
Ambiguous or started publication still belongs to convergence, not abandonment.
No new transaction, fsync, schema, command/RPC encoding or optional state is added.
Existing frozen format specimens are unchanged. This is a correction of resource
ownership/authorization, not a new representation or compatibility path.

Direct PUT terminal abandonment still requires its generation-release derivative.
Its generation exists before the terminal command, unlike atomic stream creation.
The failed-release, omitted-predecessor, ambiguous replacement, recovery-flight
handoff, same-payload reissue and foreground-outcome tests now use staged Direct
PUT commands with real encrypted shards. Protocol-only certificate tests use
self-contained Direct PUT command evidence with coherent encrypted geometry and
witnesses; they do not apply that source or claim physical cleanup coverage.
Installed Unix reissue/cleanup coverage remains in place under the real derivative.

Regression changes also pin:

- rejected frontend creation with an existing Direct PUT reservation and with an
  existing live stream: complete object-PG checkpoints/logs remain unchanged on
  all replicas, and the existing bucket proofs survive the failed attempt's
  exact proof release;
- ordinary and control-plane-authorized historical stream-creation abandonment:
  a same-ID independent generation survives, and no release command advances
  the object-PG log;
- both PUT and UploadPart creation explicitly refuse a generation-release
  derivative while equivalent source reissue remains valid; and
- scavenging and bucket-delete cleanup each use exactly one object-PG abort
  command, remove both session and generation on all replicas, and preserve a
  live-session canary in the scavenger case. The bucket-delete fixture now creates
  its expired stream through atomic production creation, not separate raw rows.

These command-count assertions establish removal of redundant publication work,
not measured filesystem sync savings. The retained owner/window, pre-install
orphan retirement and physical-exclusion integration is still outstanding; all
six original cleanup regressions remain open.

Validation: the expanded focused selection passed all 57 tests. Full `cargo
nextest run --offline` passed all 9,170 tests (one slow, six skipped) in 260.992s.
Strict all-target/all-feature Clippy, the storage boundary checker, formatting
and `git diff --check` passed.

## Successor Orphan Inspection Model — 2026-10-31

The pre-install model assumed an already settled authoritative primary. Added a
separate bounded model for the replacement boundary in
`cleanup_ownership_model/replacement.rs`: one introduction, an old primary,
witness and successor, one immutable cutoff, one successor restart and one
unavailable inspection. This composes the insertion/inspection timing obligations
without claiming that the production route-transfer protocol is implemented.

The old primary holds insertion serialization from its final deadline check
through commit. Replacement may begin and the cutoff may expire during that
interval. Fencing must drain the in-flight insertion before the successor can
certify absence; expiry alone cannot roll back an already-admitted transaction.
Inspection then considers the settled prefix's pending and materialized custody,
not merely the successor's initially empty local state. Witness-first application
does not authorize orphan release. Compaction removes log evidence, not the
materialized owner's release obligation. Restart discards the volatile inspection
result and requires reinspection. An unavailable inspection remains unsettled;
it is never evidence of absence. Exact orphan release is a separate bucket-PG
action after object-PG inspection, not a cross-PG transaction.

Required reachability canaries cover overlap of expiry/replacement/insertion,
remote pending custody with an empty successor, witness-ahead custody, compacted
owner preservation across successor restart, and successful orphan release after
an unavailable inspection and restart. Independent faulty variants must produce
counterexamples for fencing without draining, trusting successor-local absence,
dropping compacted owner state, and treating unavailable inspection as absence.
A scripted trace also pins the old insertion completing after expiry, witness
application, fencing, reinspection, convergence, compaction and restart.
An independent empty-prefix trace pins failure-before-restart, mandatory
reinspection, orphan release and rejection of the still-queued old request.

`Reconcile` is explicitly an abstract authenticated settled-prefix operation.
This model does not prove how route certificates fence an unreachable old node,
how a distributed implementation obtains that prefix, clock health, or physical
write exclusion. Those remain production integration obligations. No format,
runtime behavior, transaction or fsync changes are introduced. The coordinated
owner/window state integration remains next, and all six cleanup regressions
remain unresolved.

Validation: exhaustive exploration completed 149 states and 369 transitions;
all four faulty variants produced the expected independent counterexamples.
Full Nextest passed 9,173 tests (one slow, six skipped) in 237.053s. The explicit
empty-prefix trace was added after that build began; the subsequent focused run
passed all four replacement-model tests. Strict all-target/all-feature Clippy,
the storage boundary checker, formatting and `git diff --check` passed.

## Single Release For A Rejected Direct PUT Contender — 2026-10-31

The integration audit found another redundant existing cleanup publication. When
a Direct PUT observes a same-object winner and its re-evaluated precondition
rejects, it first waits for its own generation release through the budgeted
cleanup helper. The common caller-owned cleanup block then published that same
release again, even when the first command had fully converged and cleared its
pending slot. The release helper does not treat an absent reservation as a
read-only success; the second call introduces another durable command.

The foreground workflow now records successful generation release and skips
only that step in common cleanup. Its bucket-proof release and staged-shard
deletion still run. An error from the budgeted release does not mark it complete,
so the existing best-effort failure path remains intact. Ordinary precondition
rejection without a preceding release still performs generation cleanup. The
existing successful release outcomes and pending-recovery semantics are unchanged.

The strengthened three-replica terminal-cleanup handoff regression failed against
the prior implementation with two applied release commands instead of one. It now
requires exactly one applied command for the losing attempt on every replica,
preserves the winner's object and shards and both unrelated reservation canaries,
and retains assertions that the loser staging and bucket proofs are cleaned. A
separate ordinary precondition-rejection control uses real encrypted payload and
requires the same one-release count, complete cleanup and no object publication.

This removes redundant command publication, not a measured number of filesystem
syncs. No schema, command/RPC encoding, recovery authority or production interface
changes. The coordinated retained owner/window integration remains outstanding;
all six original cleanup regressions remain open.

Validation: the strengthened regression reproduced two releases before the fix;
both focused tests passed afterward. Full Nextest passed all 9,175 tests (one
slow, six skipped) in 273.313s. Strict all-target/all-feature Clippy, the storage
boundary checker, formatting and `git diff --check` passed. The new control
fixture initially used inaccessible helper names; those were corrected before
the successful focused and full runs.

## Durable Stream Allocation Issuer — 2026-11-01

Stream creation now captures the actual primary node and its live 128-bit
process-local PG lifetime under primary serialization. The creation command
persists that issuer alongside the generation reservation and session in the
existing apply transaction. It is included in canonical state and checkpoint
transfer; replicas preserve the original primary's identity, not their own.
This adds no ordinary-path write transaction or fsync.

Fresh segment preparation and fresh append-command construction compare the
stored issuer with the actual node's current lifetime under the same PG lock
used for allocator/command-ID mutation. The deadline check follows the issuer
lookup before allocation. A retired, replaced, reopened, or foreign
lifetime cannot adopt an old session's local cursor. Failures use the existing
allocation-lifetime-unavailable route error. A new session created under the
replacement lifetime can allocate normally. There is deliberately no automatic
continuation of the old session: disjoint replicated-window reissuance remains
deferred.

Already-issued creation and append commands still apply/replay after replacement,
and exact creation matching remains available without live issuer authority.
An existing session must match both the durable generation and issuer before a
creation command can be treated as idempotent. Replica apply does not compare
against a local process nonce. Finalization and retained cleanup are unchanged.
The issuer record is evidence of allocation provenance, not a live capability,
lease, physical-write grant, or permission to reclaim payload.

Owner-local PUT/UploadPart matrices cover embedded and installed Unix paths:
retirement, successful lifetime replacement, new route construction, rejection
without allocator/log/checkpoint changes, and positive allocation for a fresh
session. Append tests preserve convergence of an already-built command while
rejecting fresh command building under the replacement lifetime. Decoded,
checksummed creation reissues independently cross node and lifetime; they leave
the exact session, generation, log and checkpoint unchanged, both before and
after reopen. Additional canaries pin durable issuer preservation across reopen,
malformed checkpoint issuer rejection before import, and a faulty Unix builder
response whose only changed field is the issuing node.

The format vector advances together: PG schema 10, metadata command 13,
canonical state 9, checkpoint 7, and storage RPC 33. The untagged expected-command
RPC carrier gains 20 bytes, including its cap. Independently tagged control-plane
and authentication containers retain their outer versions and append new nested
fixtures. Prior exact command, RPC, checkpoint, and proof evidence remains;
aggregate-only predecessor specimens were captured from `b445ca14` as literal
bytes rather than rebuilt using today's registry. See the
[format ledger](../guides/storage-format-ledger.md#stream-allocation-issuer-version-vector--2026-11-01).

Retained owner/window records, cutoff confirmation, physical write/cleanup
exclusion, proof-release obligations, and successor-primary reconciliation are
still outstanding. All six original cleanup regressions remain open. This slice
closes fresh stream allocator adoption only; it does not claim those cleanup
safety properties.

Validation: all 3,689 storage tests passed (six skipped). The final full Nextest
run passed all 9,180 tests (one slow, six skipped) in 259.702s. Strict
all-target/all-feature Clippy, the storage boundary checker, formatting and
`git diff --check` passed. Intermediate runs exposed stale versioned fixtures,
including CLI/admin proof inputs; those were corrected with predecessor evidence
preserved and explicit retired-proof rejection checks. The final run includes the
deadline check moved after the issuer lookup, before allocation.

## Installed Primary Move And Return — 2026-11-01

The issuer acceptance matrix now composes actual storage-node route publication
with Unix stream construction, replica application and append preparation. The
prior direct lifetime-replacement tests remain; these additional tests do not
manually rotate or overwrite a store's issuer identity.

Separate PUT and UploadPart regressions use three independent storage runtimes
and an installed Unix endpoint on each. Creation is built by the current primary
and applied witness-first through RPC to every replica. The resulting replica
states must agree even though the three process lifetimes are independent.
Actual runtime-config installation moves the primary away and then returns it to
the original still-open process. Keeping the epoch fixed isolates lifetime
binding from stale-route-epoch rejection; this deliberately is not a test of a
control-plane election or certified epoch transfer.

At each replacement, the old session is still InProgress, has its first unused
VID, has no appended segments, and supplies a freshly loaded expected session
and the exact durable payload generation. Its preparation request must reach the
installed peer and fail specifically with the allocation-lifetime route error.
Full checkpoints, pending slots, sessions and segment rows remain exact on all
three replicas. Returning to the original node additionally isolates the nonce
binding: node identity alone is unchanged and cannot authorize allocation.

Positive controls create and prepare a fresh session under every published
primary, asserting its exact stored node/lifetime, VID allocation, and that only
the primary advances its local cursor. Exact old-command replay and creation
matching continue to succeed without mutation after replacement. The original
session and its issuer remain unchanged throughout. Fixtures are metadata-only;
they exercise no encrypted payload or physical deletion path.

This closes an installed-route coverage gap for the existing stream issuer
binding. There are no production behavior, schema, wire or fsync changes. Retained
owner/window state, authoritative-prefix confirmation, physical exclusion and
the six original cleanup regressions remain outstanding.

Both focused regressions passed. A deliberate mutation that compared only the
issuer's node ID, ignoring its process lifetime, made both tests fail at the
return-to-original-primary stage: the old session incorrectly allocated VID 1.
The full issuer comparison was restored before the full suite. This pins the
lifetime check independently of node identity and route epoch. Strict
all-target/all-feature Clippy, the storage boundary checker, formatting and
`git diff --check` passed. The full Nextest suite passed all 9,182 tests (one
slow, six skipped) in 272.361s.

## Installed Checkpoint Transfer And Reopen — 2026-11-01

The stream issuer acceptance matrix now connects checkpoint export/import,
peering suffix replay, route activation and runtime reopen through installed
Unix endpoints. Separate PUT and UploadPart tests use independent stores with
the **same node ID**, isolating the fresh PG lifetime from node identity. The
source runtime is stopped before the destination starts; the destination is
fully dropped before reopening its files.

The source exports a checkpoint before preparing its first segment, then builds
and applies one append through its active primary route. The destination imports
that checkpoint through the Peering RPC, preserving the exact table blocks,
state digest, generation reservation and original issuer. Its local lifetime is
different while the imported cursor is still unused. An already-issued append
is rebased onto the destination epoch's initial log position and replayed through
the installed peering route. Its resulting digest must match the source's;
exact replay remains idempotent. This exercises the storage transfer/apply
boundary, not control-plane certification or distributed prefix selection.

After activation, a freshly loaded InProgress session with one selected segment
and an unused next slot must reject preparation through the current epoch's
Unix route with the exact allocation-lifetime error. The request must reach the
peer. Full checkpoint bytes, pending slot, session, segments, generation and
issuer remain unchanged. The same checks run again after reopening, with an
additional exact full-checkpoint comparison across restart. Fresh-session
creation and preparation succeed under each new lifetime, so rejection cannot
be explained by an inactive route or generally disabled allocator.

These are metadata-only fixtures: the selected segment has coherent stored
geometry and integrity evidence but no physical payload is written or read.
There are no production, schema, wire or fsync changes. The retained owner/window
integration and all six cleanup regressions remain open; preserving a historical
issuer is not physical-write or cleanup authority.

The two focused regressions passed. A deliberate node-ID-only preparation
mutation made both fail by allocating VID 2 for the transferred session under
epoch 2; the full issuer comparison was restored before full-suite validation.
The final full Nextest suite passed all 9,184 tests (one slow, six skipped) in
263.805s. Strict all-target/all-feature Clippy, the storage boundary checker,
formatting and `git diff --check` passed.

## Transactional Stream Allocation Fence — 2026-11-01

Reviewing the next owner/window integration found a remaining allocation effect
gap: preparation checked its fence under the PG mutex, then entered an implicit
SQLite UPDATE transaction. A wait for SQLite write admission or statement
preparation could cross the captured deadline after that check. A renewed raw
route must not authorize the resulting cursor mutation.

Production preparation now passes its node identity and captured effect fence
into the storage-owned allocator. The allocator enters `BEGIN IMMEDIATE`, prepares
the UPDATE, compares the durable session issuer with the current local issuer,
then checks the fence immediately before issuing the UPDATE. The PG mutex remains
held throughout the existing session/generation/placement validation and this
transaction. The unfenced arithmetic/preparation entry points are test-only;
production cannot call them. Expiry and issuer rejection retain their existing
typed route-error mapping through embedded and Unix paths.

The explicit transaction replaces the UPDATE's former implicit transaction; it
does not add another successful-path commit or a new metadata command. A SQLite
trace canary pins one BEGIN, one issuer SELECT, one UPDATE and one COMMIT. An
outer-transaction canary verifies allocation neither commits its caller's work
nor survives its rollback. UPDATE and COMMIT failure canaries preserve the cursor
and prove a subsequent successful call still obtains the first VID. No durable
schema, canonical encoding or wire representation changes.

Separate embedded and installed Unix regressions cover PUT and UploadPart. A
one-shot hook runs after SQLite admission and issuer lookup, renews only the raw
route's validity, and advances time from 1,000 to 4,500 ms: after the captured
effective cutoff at 4,000 but before the raw authority timestamp at 5,000. Unix
cases independently cover the client deadline, a shorter receiving-node deadline,
and an absent client deadline. Full checkpoint, pending slot, session and segment
rows remain unchanged; genuinely fresh admission can consume the untouched first
slot. The owner-local canary also requires SQLite's total change count to remain
unchanged, ruling out an UPDATE followed only by rollback.

Moving validation before the hook deliberately caused all three independent
expiry tests (owner-local, embedded and Unix) to allocate VID 1 and fail. Correct
ordering was restored afterward. An initial Unix fixture failure was traced to
placing the hook around the client's preliminary session-load RPC; the fixture
now explicitly serves that frame before wrapping the allocation frame.

This hardens an existing mutation boundary without implementing retained
owner/window state or physical-write/cleanup exclusion. All six original cleanup
regressions remain open.

Validation: all 49 focused tests passed. The full Nextest suite passed all 9,190
tests (one slow, six skipped) in 262.307s. Strict all-target/all-feature Clippy,
the storage boundary checker, formatting and `git diff --check` passed.

## Transactional Bucket Reservation Fence — 2026-11-01

The companion reservation audit found two gaps before owner/window integration:
the embedded client checked the captured deadline before SQLite's implicit INSERT
transaction, and the installed bucket route did not intersect the delegated fence
with the receiving node's captured route deadline. A wait followed by same-epoch
renewal could therefore authorize a new reservation after either original cutoff.

Acquisition now enters or joins `BEGIN IMMEDIATE`, prepares the statement, checks
the effect fence immediately before INSERT, and reads back the exact reservation
within that transaction. The receiving-node route intersects its own immutable
fence with the client fence before passing it to the local operation. Expiry keeps
the existing embedded and Unix route-error mapping. The trusted raw store API
shares the transaction implementation with its existing unbounded authority; this
does not claim to eliminate every raw store mutation interface.

The explicit transaction replaces the INSERT's implicit transaction rather than
adding a successful-path commit. A trace canary requires exactly BEGIN IMMEDIATE,
INSERT, SELECT and COMMIT. Separate canaries cover INSERT failure, COMMIT failure,
retry, exact idempotence and rollback of an enclosing transaction. There are no
schema, wire-format or canonical encoding changes.

Owner-local, embedded and installed Unix tests expire authority after SQLite write
admission, at 4,500 ms between the effective 4,000 ms cutoff and raw 5,000 ms
authority timestamp. Installed cases renew the raw same-epoch route and cover
client-first expiry, server-first expiry and absent delegated deadlines. Complete
checkpoint, pending-slot and reservation observations remain exact, including an
independent canary reservation; fresh admission and its exact retry succeed. The
owner-local test additionally checks SQLite's total change count, so INSERT then
rollback cannot masquerade as rejection before mutation.

Moving validation before the hook deliberately made all three expiry regressions
create the target reservation and fail. Independently removing receiving-node
intersection made the Unix test fail on the server-first case after its
client-first case passed. Both mutations were restored. An initial fixture error
was the Unix scoped client lacking installed PG topology; correcting that setup
made all 12 focused tests pass.

This closes an existing reservation effect gap, not the retained owner/window or
physical exclusion protocol. All six original cleanup regressions remain open.

Validation: all 12 focused tests passed. The full Nextest suite passed all 9,196
tests (one slow, six skipped) in 263.804s. Strict all-target/all-feature Clippy,
the storage boundary checker, formatting and `git diff --check` passed.

## Transactional Pending-Slot Acceptance Fence — 2026-11-01

The next acceptance-boundary audit found the same pre-transaction gap in pending
installation: fenced embedded clients checked authority under the PG mutex, then
called unfenced store methods which acquired SQLite write serialization and
prepared/validated the insertion later. Installed generic and bucket-control RPCs
also checked the receiving route early without carrying that bound into the
transaction. These gaps must close before the immutable introduction-cutoff and
closed-and-absent inspection protocol can rely on pending installation.

All five fenced client entry points now pass the captured fence through to the
store: generic, generic-until, classified-generic-until, bucket-control and
bucket-control-until. Both installed RPC handlers intersect the delegated fence
with their captured receiving-node route fence. Within the existing pending-slot
transaction, prerequisite reads and statement preparation precede the final
fence check immediately before INSERT. The slot and its reference/dependency
sidecars remain under the same write serialization through commit or rollback.
Trusted raw/recovery callers retain their existing unbounded entry points; this
does not turn historical convergence into fresh request admission.

No successful-path transaction, commit/fsync, command, schema or wire version is
added. Trace canaries pin one BEGIN IMMEDIATE and one COMMIT for each slot kind;
outer-transaction tests prove insertion neither commits its caller's transaction
nor survives its rollback. A rejected pre-effect fence remains a definitive
insertion failure. An injected INSERT failure is also definitive; an injected
COMMIT failure preserves the conservative MayHaveApplied classification, with
rollback and subsequent successful retry tested independently. The bucket-control
API retains its existing unclassified error contract.

Owner-local tests cover vacant and occupied slots, complete canonical table
blocks/replica state, exact pending state and SQLite total_changes. Rejection
cannot pass by inserting then rolling back, and an unrelated unapplied canary
remains untouched. Fresh insertion and exact retry are positive controls in the
vacant-slot cases. A separate trace-controlled schedule advances time only once
INSERT executes: acceptance before the cutoff may finish committing afterward,
and its exact slot must remain visible. This preserves the pre-install orphan
contract's distinction between late completion and rejection before acceptance.

Embedded and installed Unix matrices exercise all five APIs. The one-shot hook
runs after SQLite admission and statement preparation, renews the raw same-epoch
route and advances time from 1,000 to 4,500 ms, past the captured effective cutoff
but before the 5,000 ms authority timestamp. Unix cases cover client-first expiry,
server-first expiry and absent client deadlines. Rejected calls leave the full
checkpoint, pending slot and independent bucket reservation unchanged. The
classified client must return Definitive; genuinely fresh insertion and exact
retry succeed without advancing replica state. Checkpoint export is intentionally
unavailable with a pending slot, so post-install observations use replica state
and exact pending/reservation records rather than treating export failure as
evidence of equality.

This implements the existing captured-route acceptance boundary only. Persisting
the introduction cutoff, retained owner/window state, replacement-primary
settlement and physical exclusion remain outstanding. All six original cleanup
regressions remain open.

Moving the final check before the interleaving hook deliberately made all three
expiry tests install a slot and fail. Independently removing only the
bucket-control receiving-node intersection made the Unix matrix fail at its
server-first BucketControl case, after all generic cases and the client-first
bucket case passed. Both mutations were restored before full-suite validation.
Initial fixture failures were corrected by respecting checkpoint-export exclusion
and leaving an unrelated unapplied canary installed rather than removing it.

Validation: all 17 focused tests passed. The full Nextest suite passed all 9,203
tests (one slow, six skipped) in 254.441s. Strict all-target/all-feature Clippy,
the storage boundary checker, formatting and `git diff --check` passed.

### Transactional Stream Reservation Heartbeat Fences

The acceptance-boundary audit next found that streamed PUT heartbeat checked
captured authority under the PG mutex, but both its bucket reservation renewal
and stream-row proof update subsequently entered SQLite without that fence.
The bucket update also compared lease expiry with time sampled before write
admission. A delayed heartbeat could therefore renew an expired reservation or
mutate either record after its immutable route cutoff under a renewed raw map.

Both updates now acquire or join SQLite write serialization, prepare the exact
UPDATE, and validate the effect fence immediately before mutation. The bucket
update samples lease time there as well. Installed Unix/TLS handlers intersect
the caller's delegated fence with the receiving frame's captured route fence;
neither same-epoch renewal nor an absent/longer caller deadline extends it.
The existing portable deadline representation is unchanged. The old durable
proof epoch remains independent of the current route authorizing renewal.

These remain two separately committed operations, not a new cross-PG transaction.
If the bucket renewal succeeds and the stream update expires, the renewed bucket
record and old stream proof remain intact for the existing refresh/retry path.
Each successful hop still has one commit/fsync; explicit BEGIN IMMEDIATE replaces
its former implicit write transaction. Nested callers retain ownership of their
transaction. No command, schema, format version, or successful-path durable write
is added. Trusted raw test/store callers retain their explicit-time interface;
request-admitted heartbeat resamples time at the effect.

Owner-local regressions cover expiry after statement preparation, exact records,
checkpoint and SQLite total_changes non-mutation, lease expiry during admission,
UPDATE/COMMIT failure and retry, one-commit traces and outer rollback. The stream
proof cases first commit the real bucket renewal. Embedded and installed Unix
matrices renew the raw map inside the transaction hook and advance time past the
effective cutoff but before the authority timestamp. Client-first, server-first,
and absent-client deadlines each reject; fresh retries succeed, preserving an
independent reservation. A two-hop Unix canary renews an epoch-1 proof through an
epoch-2 route and verifies both durable records.

This closes heartbeat's existing captured-route effect boundary only. It does
not implement retained owner/window renewal, introduction-cutoff persistence,
replacement-primary settlement, or physical write exclusion. All six original
cleanup regressions remain open. The adjacent bucket-delete drain acquisition
still needs its own transactional effect-boundary audit; this slice does not
claim to cover that separate operation.

Moving the final fence and time sampling ahead of the interleaving hook made
both owner-local expiry canaries accept the write and fail. Independently
removing only the stream update's receiving-node intersection made the Unix
matrix fail at its server-first StreamProof case, after all bucket cases and the
client-first stream case passed. Both mutations were restored. Fixture setup
errors were corrected by selecting 1+0 for the one-node placement and supplying
finite validity for runtime-map refresh; they did not require production changes.

The first full run passed 9,209 tests and failed
`active_object_routes_keep_their_captured_deadline_after_validity_extension`:
the new stream intersection classified an already-expired receiving route as an
object error before the existing route check could return its route error.
The handler now preserves that early route check and still carries the
intersected fence to the SQL effect. The existing regression is unchanged.

Validation: all ten final focused tests passed, including the unchanged
route-expiry regression. The final full Nextest suite passed all 9,210 tests
(one slow, six skipped) in 260.576s. Strict all-target/all-feature Clippy,
the storage boundary checker, formatting and `git diff --check` passed.

### Transactional Bucket-Delete Drain Acquisition Fence

The adjacent drain-acquisition audit confirmed that the fenced client checked
authority before taking the bucket PG lock, then delegated to an unfenced
implicit SQLite INSERT transaction. The installed handler checked its captured
route before dispatch but did not carry that bound into insertion. A lock wait
or delayed RPC could therefore install a fresh drain after captured authority
expired while the raw same-epoch map remained valid.

The fenced path now carries the immutable effect fence into the store. It
acquires or joins BEGIN IMMEDIATE, prepares the INSERT, then checks the fence
immediately before mutation. Exact retry/conflict readback remains under the
same write serialization. The receiving handler preserves early route/epoch
validation and intersects its captured route bound with delegated authority.
Trusted raw callers retain their unbounded entry point; this change does not
make recovery/adoption depend on expired request authority.

No schema, wire format, command or successful-path commit/fsync is added. The
explicit transaction replaces the previous implicit insertion transaction.
Existing bucket-state and exact drain-identity semantics are unchanged. Store
tests pin one BEGIN/INSERT/SELECT/COMMIT sequence, joining without committing
an outer transaction, rollback on insertion/commit failure, and successful retry.

Expiry canaries run after SQLite admission and statement preparation, and
compare the full checkpoint plus SQLite total_changes to exclude an
insert-then-rollback false positive. Vacant and occupied target drains preserve
an independent drain and reservation. Fresh acquisition, exact retry and a
conflicting contender provide positive/negative controls. Embedded and installed
Unix matrices renew the raw route inside the hook and advance time past the
effective cutoff but before its authority timestamp. Client-first, server-first
and absent-client cases reject without changing drain/reservation/checkpoint
state; freshly admitted acquisition and exact retry succeed.

This is another acceptance-boundary prerequisite, not an implementation of
durable generation cleanup ownership. Introduction-cutoff persistence, retained
owner/window records, replacement-primary settlement and physical exclusion
remain outstanding; all six original cleanup regressions remain open.

Moving the final effect check before the interleaving hook made all three
expiry tests accept a drain and fail. Independently omitting the receiving-node
intersection made the Unix server-first case accept a drain after the client-first
case passed. Both mutations were restored before full validation.

Validation: all 22 focused tests passed after refreshing the raw fixture's
metadata digest before checkpoint export. The full Nextest suite passed all
9,215 tests (one slow, six skipped) in 266.929s. Strict all-target/all-feature
Clippy, the storage boundary checker, formatting and `git diff --check` passed.

## Physical Shard Publication Effect Fence — 2026-11-01

The physical-write audit found that fresh fenced shard writes checked captured
authority before CRC calculation, prefix preparation, temporary-file I/O and
sync, then published through unfenced rename/hard-link helpers. The installed
shard route also checked its own captured deadline early without carrying that
bound to file publication. Those delays could therefore publish a final shard
after request authority expired under a same-epoch renewed raw map.

Fenced embedded writes now carry their immutable effect fence to the filesystem
helper. Installed writes intersect the delegated bound with the receiving
frame's captured route fence and pass it through the write-if-absent path. Both
helpers check after CRC work before prefix preparation, after prefix sync before
creating a temporary file, and after temporary data sync immediately before the
atomic rename/hard link. Rejection at the last boundary unlinks only that
attempt's temporary path; an existing final shard and independent temporary
files remain untouched. No acknowledgement is returned and no shard row is
registered. As with the existing I/O error cleanup, temporary unlink is best
effort, not a new crash-recovery or durable scavenging guarantee. A prepared empty
prefix may remain. The unbounded write-if-absent convenience wrappers now have
only owner-local test callers and are test-gated; trusted repair/raw rename
paths retain their existing behavior.

The acceptance point is the final pre-publication check, not completion of the
following filesystem call or directory sync. Once accepted, publication may
finish after the cutoff. This explicitly does **not** establish physical
write/cleanup exclusion: the planned local grant/exclusion lifetime must span
that completion tail, and a cleanup worker cannot treat the cutoff alone as
proof of quiescence. No new file/directory sync, metadata transaction, durable
record, format version or ordinary-path network request is introduced.

Store regressions cover both publication protocols with vacant and occupied
final keys, exact original bytes/acknowledgements, independent shard/temp canaries,
unchanged SQL total_changes, and fresh retry. A prefix-sync schedule verifies
expiry before temporary creation; a hard-link schedule verifies accepted writes
and matching-existing retries still complete directory sync after the cutoff.
Embedded and installed Unix matrices pause after temporary-data sync, renew the
raw same-epoch route and cross the effective deadline while remaining before
the authority timestamp. Client-first, server-first and absent-client cases
reject with the existing expiry classification, preserve exact files/rows and
remove the attempt's temp; fresh requests succeed.

This remains a prerequisite to the coordinated owner/window integration. It
does not persist write horizons, confirm settled prefixes, bind physical effects
to retained owners, or fence acknowledgement registration against cleanup. All
six original cleanup regressions remain open.

Moving the final check before the interleaving hook made all three expiry
regressions publish a shard and return an acknowledgement. Independently omitting
the receiving-node intersection made the Unix server-first case return an
acknowledgement after both client-first cases passed. Both mutations were
restored before full validation.

Validation: all 12 focused tests passed. The full Nextest suite passed all
9,220 tests (two slow, six skipped) in 262.494s. Strict all-target/all-feature
Clippy, the storage boundary checker, formatting and `git diff --check` passed.

## Receiving-Node Shard Acknowledgement Acceptance Fence — 2026-11-01

The next audit found a distinct durable effect after file publication:
`ShardAckRecord` checked the receiving frame's route before acquiring the PG
mutex, then inserted acknowledgement rows without that captured bound. SQLite
admission, statement preparation or row lookup could cross the deadline while
a same-epoch renewal kept the raw runtime map live.

The installed primary data route now carries its own immutable effect fence
into exact batch registration. The store checks it inside the existing
BEGIN IMMEDIATE transaction after preparation and after each row lookup,
immediately before insertion or accepting an exact existing row. If a batch
crosses the cutoff after an earlier accepted insertion, the next check rejects
and rolls back the whole batch. Once all rows have been accepted, COMMIT may
finish after the cutoff; it is not replaced with post-commit expiry/rollback.
This adds no transaction, fsync, schema or wire-format change. Raw trusted
registration keeps its existing behavior and does not acquire request expiry
semantics implicitly.

Store canaries preserve already-durable files, independent acknowledgement rows
and occupied exact-retry rows. Expiry after lookup but before the first effect
leaves SQLite total_changes unchanged, excluding insert-then-rollback as a
false positive. Further canaries cover a later conflicting row, expiry between
two insertions, exact retry, one successful batch transaction, and accepted
commit completion after cutoff. The installed Unix regression renews the raw
map inside the transaction, then crosses the captured effective cutoff while
remaining before the authority wall timestamp; a freshly admitted batch and
its exact retry succeed.

Scope remains deliberately narrower than complete acknowledgement ownership.
The shared acknowledgement client API and wire batch do **not** carry the
frontend's captured deadline. Embedded registration and a newly admitted Unix
frame can therefore still outlive the initiating request's deadline. The next
delegation slice must distinguish fresh Direct PUT/append registration from
exact-command convergence and maintenance, carry a portable deadline for fresh
effects, and retain non-request-expiring convergence authority. The existing
registration helper serves those different paths; blindly adding expiry to
all its callers would be incorrect. This receiver-side check also establishes
neither a retained owner/window nor physical write/cleanup exclusion. The six
original cleanup regressions remain open.

All 24 focused tests passed. Removing the per-row final check made the store
post-lookup test, the between-inserts test and the installed Unix renewal test
all accept expired registration and fail. The check was restored before full
validation.

Validation: the full Nextest suite passed all 9,224 tests (one slow, six skipped)
in 268.040s. Strict all-target/all-feature Clippy, the storage boundary checker,
formatting and `git diff --check` passed.
