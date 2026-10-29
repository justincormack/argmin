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
separate event and can succeed after frontend loss. The model deliberately does
not allow abandoning a pending command, conservatively protecting it until later
modelling separates authoritatively unstarted and irrevocable publication.

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

The fanout extension below now covers two data nodes, partial durable closure,
and fixed healthy clock offsets with abstract absolute-cutoff rebinding. Before
selecting a production protocol, add metadata-replica fanout; production clock
binding/health, clock faults, and authority re-establishment; owner renewal and
bounded append horizons; owner creation and crash-before-session windows;
publication-start/response-loss/abandonment; multiple subjects and generations;
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
