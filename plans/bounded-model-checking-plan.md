<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Bounded Distributed Model-Checking Plan

Status: local explorer selected; Stateright and the temporary comparison adapter
removed. Lifecycle baseline and historical incarnation-renewal mutation/replay
are retained. Phase 1's explicit-time and delayed-heartbeat extensions are next;
the broader publication/payload-ownership model has not started.

## Goal

Evaluate whether bounded model checking can find distributed correctness bugs
more systematically than selected fault schedules and randomized testing alone.
Start with a small production-connected model, demonstrate detection of a
previously fixed defect, and measure the cost before expanding the approach.
The pilot evaluated Stateright and selected the smaller storage-owned explorer
after equivalent-state and historical-mutation comparisons. Preserve that
evidence below while building the next bounded model slices.

This is an experiment supporting
[multihost Phase 5: independent correctness evidence](multihost-followup-plan.md#phase-5-independent-correctness-evidence),
particularly its small formal models. Completing this experiment does not
complete that phase, replace deterministic simulation, or establish correctness
of the whole server. Existing fault-injection, process-crash, AWS compatibility,
and soak tests remain necessary.

## Approach And Limits

Use the test-only [local explorer](../crates/storage/src/bounded_explorer.rs).
Models supply initial states, explicitly bounded successors, safety checks and
required reachability properties. Full-state equality defines deduplication;
resource exhaustion is not successful completion. No external model-checker
dependency, actor runtime, Tokio replacement or OpenRaft replacement is needed.

Distinguish three kinds of evidence:

| Integration | Evidence | Limitation |
| --- | --- | --- |
| Independent abstract model | The described protocol satisfies checked properties within the declared bounds. | Implementation may not implement that protocol. |
| Model executing production transition logic | Actual decisions satisfy independently specified properties within the model. | Effect adapters and unmodelled interleavings remain assumptions. |
| Deterministic implementation replay | A discovered schedule exercises the corresponding real implementation boundary. | One replay is a regression, not exhaustive implementation coverage. |

Prefer the second approach, coupled with the third. Keep an independent oracle:
calling the same production decision to obtain both actual and expected results
does not establish correctness. An abstract-only result is useful but must not
be reported as checking the implementation.

Check safety and non-vacuous reachability. The local explorer does not check
liveness or fairness; do not base general eventual recovery claims on it.
Bounded recovery checks must explicitly state eventual
delivery, worker scheduling, clock progress, and failure cessation assumptions.
No protocol can guarantee progress under permanent partition or starvation.

Treat Raft as an explicit contract for ordered committed commands, leadership,
and read barriers. Do not reimplement consensus. Model local apply, durable
publication, and response delivery separately where the adapter requires it;
an abstract Raft commit must not silently certify a local checkpoint. Real
OpenRaft integration and filesystem behavior retain their own tests.

## Existing Starting Points

- [Pending-command lifecycle tests](../crates/storage/src/control_plane/tests.rs):
  `pending_command_heartbeat_lifecycle_model_exhausts_short_interleavings`
  enumerates five actions through depth six, visiting 19,531 trace prefixes.
  It executes production control-plane transitions and compares a separate
  lifecycle model. This is the first harness target.
- [Lease and horizon models](../crates/storage/src/control_plane_lease.rs):
  generated clock, renewal, authority-change, response-loss, and successor
  activation events already exercise production lease primitives.
- [Runtime-map regressions](../crates/storage/src/cluster/runtime_map_tests.rs):
  `same_epoch_new_authority_incarnation_does_not_extend_pinned_generation`
  supplies a concrete historical-bug candidate.
- [Direct-PUT regressions](../crates/storage/src/cluster/local/tests/direct_put.rs):
  durable pending-command ownership, ambiguous outcomes, and overlapping
  physical cleanup provide candidates for a later publication model.

Normative protocol references are the
[clock and lease model](../guides/control-plane-clock-and-lease-model.md),
[metadata-command stream](../guides/metadata-command-stream.md), and
[object concurrency invariants](../guides/object-concurrency.md).
Verify current source behavior when implementing each slice; these links are
entry points, not claims that all listed tests already form one reusable model.

## Model And Ownership Rules

Keep models in test modules of the protocol-owning crate, initially `storage`.
Do not expose PG, physical payload, raw database, or capability-construction
APIs merely to accommodate an external model harness. Move tests with their
owner if the storage-node extraction changes crate ownership.

Where necessary, extract a private production transition function of the form
`state + event -> next state + requested effects`. The runtime performs effects;
the model controls their results and ordering. This is a bounded refactor of
the selected protocol, not a prerequisite to rewrite all distributed code.
Do not introduce model-only production behavior or a second production path.

For every model, record:

- The state, actions, invariants, owner functions exercised, and independent
  oracle. Identify each transition's real lock/transaction/atomicity boundary.
- Separate volatile state, durable state, in-flight messages, and client
  observations. A request to persist, completion of persistence, and delivery
  of its acknowledgement are distinct events where failure can separate them.
- Crash/restart semantics: lose volatile state, preserve only the permitted
  durable prefix, and retain or discard messages according to explicit transport
  and incarnation rules. Do not model storage as losing acknowledged durable
  state unless the selected fault model explicitly includes device loss.
- Typed pre-dispatch, definite-result, and ambiguous-publication outcomes.
  Response loss or rejection after dispatch is not assumed to undo mutation.
- Explicit time and deadline inputs. Separate time advancement from unrelated
  actions; model expiry boundaries and the applicable clock-skew assumptions.
- Finite limits for nodes, commands, generations, restarts, messages, and time.
  Exhausting an ID bound disables new allocation; never wrap identities and
  introduce artificial aliasing.
- State equality/fingerprinting that includes everything affecting future
  behavior. Do not omit timestamps, pending effects, authority identities, or
  hidden durable state merely to make exploration smaller. Any normalization
  or symmetry reduction needs a reason it preserves the checked properties.

Start with simple explicit bounds and no symmetry reduction. Retain identity
distinctions such as primary versus witness and old versus new authority.
More aggressive reductions are separate reviewed changes.

## Phase 0 — Fix The Experiment Contract

- [x] Inventory the pending-command test's production transitions and abstract
  actions. Document which operations are atomic and which durability/network
  effects it does not represent.
- [x] Define its initial finite configuration, independent safety properties,
  and positive reachability cases, including successful recovery/reactivation.
- [x] Select one historical defect for the pilot. Prefer one in the lifecycle
  transitions; if none is suitable, use the incarnation-renewal regression as
  a second small model. Identify the faulty decision and its current owner;
  do not expand into the entire payload-publication protocol for this gate.
- [x] Record model bounds and resource limits, the planned ordinary-test entry
  point, and how search exhaustion is distinguished from early termination.
- [x] Establish the existing enumerator as the tool-evaluation baseline. Compare
  coverage and cost under equivalent bounds, separating improvements from the
  checker from improvements due to changing the model.

Exit: a bounded experiment with a named historical defect and explicit
implementation coverage. This inventory does not require building a simulator.

### Initial Experiment Contract

The first slice evaluated `stateright = "=0.31.0"` solely as a `storage`
development dependency. It added 16 lockfile packages, including unused HTTP
explorer dependencies. That dependency and the packages it uniquely required
have now been removed; the models directly use the local explorer.

The fixture begins with one Active storage node, one Active PG (41), one fixed
metadata proof, and one possible pending command. The five actions remain
`InstallPending`, `ConvergePending`, `Heartbeat`, `CompleteReadyPeerings`, and
`Restart`. Every action advances fixture time by one millisecond, exactly as in
the original enumerator. Search covers all action sequences of length zero
through six: at most six restarts and times 2,020 through 2,026 ms. A separate
depth-seven exploration checks full serving recovery, with at most seven
restarts and time 2,027 ms. There are no
queued messages, lease-expiry exploration, concurrency within a command, or
symmetry reductions in this baseline.

Production coverage and abstractions:

- Heartbeat applies `RecordNodeHeartbeat` to `ClusterControlSnapshot` and
  checks recovery discovery plus retained historical routes.
- Completion performs the production readiness scan followed by
  `CompleteReadyPgPeerings`. That scan/apply pair is one model action; races
  between them are not covered.
- Restart round-trips the state codec, then performs the production authority
  incarnation/epoch bump and history recording as one action. This tests
  logical restart semantics, not WAL/sidecar ordering or partial file durability.
- Install/converge update an abstract node slot. The independent lifecycle
  oracle also supplies simulated node observations; it does not represent
  actual node database operations, payload cleanup, or replication.

Safety requires agreement with the original lifecycle oracle for PG state,
exact recovery tasks, route references, historical Active primary, and blocked
peering completion while recovery requires that route. Production publication
invariant validation is an additional check, not the independent oracle.
Positive properties require discoverable pending recovery and convergence after
rediscovery across restart. A history monitor records that rediscovery and is
part of state identity. The depth-seven property additionally requires an
admissible Active route: completion changes the epoch, so a subsequent heartbeat
is needed before serving. The initially attempted depth-six serving witness
failed because the bound excluded that last step; the property was not removed
or treated as a production failure.

The local explorer test compares its exact reachable-state set against exhaustive
enumeration of all 19,531 trace prefixes. State includes the complete fixture,
remaining action budget, recovery-history monitor, and any transition failure. Interning compares full
fixture equality; serialized snapshot text only groups candidates for lookup,
because that encoder prunes history. Exact-equality deduplication is
checked against exact enumeration, and every generated state must have its
safety property evaluated. No checker timeout or target-state/depth cutoff can
silently pass; the action bound supplies terminal states, and exceeding the
97,656-state harness capacity (all prefixes through depth seven) panics.
Unexpected transition errors become
failing model states so the checker can report their action path.

The existing enumerator is retained. A deliberately false no-pending property
checks counterexample discovery and semantic-action replay; it is only a harness
negative control, not historical-bug evidence. Default-suite entry points are
`pending_command_model_*` in the storage-owned
[model-checking tests](../crates/storage/src/control_plane/tests/model_checking.rs).

Selected historical defect for Phase 2: a same-epoch/content-digest
renewal extended pinned generations from an older authority incarnation. The
current owner is `renew_from_runtime_map_status` in
[runtime_map.rs](../crates/storage/src/cluster/runtime_map.rs); the permanent
`same_epoch_new_authority_incarnation_does_not_extend_pinned_generation`
regression exercises installation followed by renewal. The second bounded model
and isolated mutation below reproduce the former epoch/digest-only selection.
The production incarnation check remains in place; the lifecycle baseline's
negative control is not used as Phase 2 evidence.

## Phase 1 — Adapt The Existing Lifecycle Exploration

- [x] Adapt the existing production state-machine transitions and independent
  oracle to bounded exploration. The initial Stateright pilot is complete;
  the models now directly use the selected local explorer.
- [x] First preserve the current actions and depth-six semantics. Compare
  reachable observations and invariant results with the existing enumerator;
  trace count and deduplicated state count are different measurements.
- [x] Record runtime, memory, states and transitions explored, depth, bounds,
  checker configuration, and termination reason. Do not remove the existing
  enumerator until equivalent coverage has been demonstrated.
- [ ] Separate clock advancement into explicit actions before claiming useful
  state deduplication: the current test advances time on every operation.
  Preserve the existing timing cases and document the revised model's scope.
- [ ] Add separately deliverable heartbeat observations, including delayed
  observations across restart, with a bounded message set. Do not combine send
  and receive into one step when their separation is the behavior under test.
- [x] Assert positive witnesses as well as safety, so rejecting all work or
  never enabling recovery cannot make the test pass vacuously.

Exit: a normal `cargo nextest run` test with a completed declared bounded search
and reproducible failure traces. A timeout or state/resource cap is an
incomplete exploration, not a successful exhaustive result. Depth-limited
results must be reported only as such.

## Phase 2 — Demonstrate Historical-Bug Detection

For the selected defect:

- [x] Reproduce the old faulty decision with a reviewable isolated mutation,
  ideally in production logic shared by the model. Keep mutations out of the
  shipped implementation; no runtime switch for unsafe behavior.
- [x] Require the checker to find a counterexample without supplying the known
  failing schedule. Prefer single-threaded breadth-first search for a short,
  stable explanation, subject to the model's memory limit.
- [x] Require the corrected implementation to complete the same bounded search.
- [x] Replay the counterexample through the actual owner implementation using
  explicit barriers/events, not sleeps. Preserve it as a permanent regression.
- [x] Retain the faulty decision description or reproducible mutation patch,
  model version, bounds, and trace. Distinguish a faithful historical mutation
  from a synthetic analogue, and abstract-only detection from shared-code
  detection.

Candidate expansion cases, not all required for the pilot:

| Defect | Required property |
| --- | --- |
| Old-incarnation pinned generation renewed by new authority | Renewal never grants authority to a different incarnation. |
| Caller cleanup after pending-command ownership | Cleanup cannot remove payload required by a durable recoverable command or live metadata. |
| Malformed/lost response interpreted as definite non-application | Potentially applied work is not certified as unapplied or retried as a replacement mutation. |
| Serving observed before required publication/fencing | Every admitted serving action has the required local durability and authority evidence. |

Exit: at least one real historical defect detected, a clean fixed bounded run,
and a deterministic implementation regression. An invariant deliberately made
false or a model-only mutation with no implementation correspondence does not
satisfy this gate.

### Incarnation Renewal Model v1

The storage-owned [renewal model](../crates/storage/src/cluster/runtime_map_model_checking.rs)
executes `StorageClusterRouteHandle::install`,
`renew_from_runtime_map_status`, production conservative lease binding, and
`require_route_map_valid_now`. It does not implement a second renewal predicate.
Each schedule replays against a fresh owner fixture with an independent
expected-deadline oracle. The two new tests run in the default suite under
`incarnation_renewal_*`; the original incarnation regression is retained.

Bounds and assumptions:

- One frontend publication domain, one node, one Active PG, one fixed epoch
  and content digest, two authority incarnations. The old cluster remains
  strongly pinned throughout; the successor can be installed once. There are
  no admitted requests blocking publication or concurrent operations inside
  either owner method. Each install/renewal action completes its real locking
  and publication operation before the next action.
- At most six events: install, send a renewal from the currently installed
  incarnation, deliver a selected queued renewal, lose a selected renewal, or
  advance time. Each incarnation can send once; delivery/loss consumes its
  message. At most two messages are queued. An old message can cross installation.
  The status represents trusted authority evidence at the route-handle boundary;
  authentication, transport bytes, actual RPCs, and authority-side issuance are
  not modelled.
- Wall and monotonic clocks are equal and advance only through explicit events.
  Initial time is 1,000 ms; later samples are 3,999, 4,000, 10,999, and 11,000 ms,
  chosen to straddle the old and renewed monotonic deadlines. Initial authority
  deadlines are 5,000/9,000 ms; the renewal deadline is 12,000 ms. With the
  1,000 ms skew budget the corresponding initial monotonic bounds are
  4,000/8,000 ms and renewal binds to 11,000 ms (immediately expired at that
  final sample). Installation is enabled only before the successor lease expires.
  No clock rollback, unhealthy-clock latch, filesystem restart, renewed content,
  changed epoch, second replacement, or node serving protocol is represented.
- The independent oracle assigns each generation exclusively to its issuer.
  A renewal for the current issuer changes only that generation's authority and
  monotonic deadlines; a stale issuer changes neither. After every event, actual
  deadlines and admission outcomes must agree, including strict expiry, and
  renewal must preserve the current cluster's identity.
- State identity includes the full semantic trace and observations, including
  queued messages, time, and error. Every transition replays from scratch rather
  than cloning live locks or merging states by redacted/partial observations.
  Random fixture ownership IDs are not compared across replays; this model
  never crosses domains and tests pointer identity only within one replay.
  There is no semantic deduplication or symmetry reduction. The explorer must
  check every generated trace; the exact sets and count are compared, including
  terminal states. The enabled-action bound terminates exploration, not a
  checker timeout or depth cutoff. Exceeding 100,000 states fails the harness.

Single-threaded breadth-first exploration of the corrected code completed all
2,146 trace states through depth six. The initial focused run took approximately
0.122 s inside the model test (unoptimized test profile, not a performance
threshold). Positive witnesses demonstrate renewal by each issuer, stale-message
rejection, and new-authority serving while the old pinned generation is expired.
These are bounded reachability statements, not liveness claims.

The [isolated mutation patch](../crates/storage/src/cluster/model_mutations/renewal_without_incarnation.patch)
removes only the incarnation comparison from the pinned-generation renewal
filter and its now-unused local variable. The current-generation eligibility
check and install filter remain unchanged. It reconstructs the reported
historical faulty decision in today's production implementation, not an entire
historical checkout or a model-only analogue. There is no production runtime
switch and the patch is not compiled into the normal implementation.

The original Stateright pilot's mutated model run failed and deterministically
replayed this counterexample (the selected local engine finds the shorter trace
recorded below):

```text
Send(old) -> AdvanceTo(3999) -> Install(new) -> Deliver(old)
          -> Send(new) -> Deliver(new)
```

The old renewal is correctly ignored, but the new renewal incorrectly changes
the old generation from `(authority=5000, monotonic=4000)` to
`(authority=12000, monotonic=11000)`. This is a production-state mismatch, not an
intentionally false property. The reported BFS discovery is not claimed to be
globally shortest. `incarnation_renewal_discovered_schedule_replays_through_route_handle`
retains that exact schedule and then advances to 4,000 ms, requiring the old
generation to reject admission while the new generation remains valid. Replay
uses clock overrides and sequential events, with no sleeps.

To reproduce the mutation from the repository root, in an isolated worktree
without concurrent builds or edits:

```bash
patch --dry-run -p1 -i crates/storage/src/cluster/model_mutations/renewal_without_incarnation.patch
patch -p1 -i crates/storage/src/cluster/model_mutations/renewal_without_incarnation.patch
cargo nextest run --locked -p storage \
  -E 'test(incarnation_renewal_model_exhausts_bounded_schedules)' \
  --test-threads 1 --failure-output immediate
# Expected failure: historical-renewal counterexample, not a compilation error.
patch -R -p1 -i crates/storage/src/cluster/model_mutations/renewal_without_incarnation.patch
cargo nextest run --locked -p storage -E 'test(incarnation_renewal_)'
```

Always restore the mutation even if the command fails unexpectedly. The
mutation was tested and reversed during this slice. This completes the
historical-defect gate, not the whole experiment: the separate lifecycle
model's delayed-heartbeat/time work remains outstanding. For this small
trace-replay model, an ordinary exhaustive enumerator could also find the bug;
the evidence establishes production correspondence and actionable checker
traces, not superior coverage or efficiency over that alternative.

Verification after restoring the production predicate: 54 focused runtime-map
and lifecycle/model tests passed, as did storage all-targets/all-features Clippy
with warnings denied, formatting (including the included model source), and
the storage boundary checker. After review, the full `cargo nextest run` passed:
9,099 tests, none skipped, in 221.866 s. No production behavior or public API is
changed.

## Experiment Decision Gate

Time-box the initial experiment to roughly one engineering week, then review
results even if a gate is incomplete. This is an estimate, not a deadline that
permits weakening evidence requirements.

### Historical Initial Slice Results (2026-09-22)

The following measurements describe the Stateright pilot, not the current
dependency set. Its original test entry points are preserved in commit
`f9dddf6a`; use that revision for the historical commands below.

Stateright 0.31.0, single-threaded breadth-first search, no symmetry, ordinary
unoptimized test profile. Measurements from one warm-build focused run are
diagnostic observations, not performance thresholds:

| Exploration | Trace prefixes | Exact reachable states | Generated successors | Exploration time |
| --- | --- | --- | --- | --- |
| Existing depth-six test, unchanged traversal | 19,531 | Not recorded by that test | 19,530 | 0.825 s for the whole test |
| Depth-six enumeration with exact-state collection and property monitor | 19,531 | 2,082 | 19,530 | 1.498 s |
| Stateright depth six, same instrumented state and actions | Not enumerated individually | 2,082 | 3,990 | 0.323 s |
| Stateright depth seven | Not enumerated individually | 5,096 | 10,410 | 0.867 s |

The depth-six comparison asserts exact state-set equality, not just those
counts. Both Stateright runs finish the enabled action graph and check every
generated state. The observer distinguishes histories needed by the positive
recovery property without changing production transitions. No time abstraction
or separately delayed messages have been introduced yet.

The serving witness, replayed through the production fixture, is:

```text
InstallPending -> Restart -> Heartbeat -> ConvergePending
               -> Heartbeat -> CompleteReadyPeerings -> Heartbeat
```

The harness negative control discovers and replays
`InstallPending -> Heartbeat` for its deliberately false no-pending property.
Neither result is detection of a historical or previously unknown production
bug. The subsequent Phase 2 mutation/replay above supplies that historical-bug
evidence separately.

The six focused tests passed in 3.634 s. `/usr/bin/time -v` around the warm
nextest invocation reported 89,112 KiB maximum RSS. That is a run-level
parent/child process high-water measurement, including harness/framework
overhead, not isolated Stateright heap usage or a memory comparison with the
original enumerator. The parity test deliberately retains both exact-state
collections. The measured command was:

```bash
/usr/bin/time -v cargo nextest run --locked -p storage \
  -E 'test(pending_command_stateright) | test(pending_command_heartbeat_lifecycle)' \
  --test-threads 1 --success-output immediate --no-fail-fast
```

Focused warnings-denied Clippy (`-p storage --all-targets --all-features`),
formatting, and the storage boundary checker also pass. After review, the full
`cargo nextest run` passed: 9,097 tests, none skipped, in 221.756 s.

Preliminary assessment: useful reduction in repeated transition execution and
actionable traces, with no production API or behavior changes. Full-state
interning and exact comparison add nontrivial harness code because the live
snapshot has no complete hash representation and its persistence encoder prunes
history. Retain the original enumerator and evaluate this maintenance cost
before expanding. At this stage tool selection remained open; the subsequent
comparison and selection below complete that evaluation.

Record:

- Historical defects detected and whether production decisions were exercised.
- Bounds exhausted, search costs, and scaling when one bound is increased.
- Amount of duplicated protocol logic and production refactoring required.
- Whether traces are actionable and replayable against real code.
- Coverage missing from the existing enumerator/proptest/fault-injection tests.

- [x] Evaluate Stateright against the baseline: useful exploration depth,
  deduplication, runtime/memory cost, counterexample quality, production-code
  reuse, modelling overhead, and default-test-suite integration.
- [x] Decide explicitly whether to retain Stateright, narrow its use, evaluate
  an alternative, or stop. If independent protocol specification or
  liveness/fairness reasoning dominates, consider TLA+/TLC or P. If the main gap
  is implementation scheduling or integrated fault execution, consider Shuttle
  or deterministic simulation instead. These answer different questions; do not
  treat a simulation run as equivalent to exhaustive model exploration.

A small local explorer was compared on identical models and selected. Future
tool evaluation should identify a specific missing capability before adding
another implementation; this decision does not preclude a separate history
checker or a specification-oriented tool for a different question.

Continue if it provides useful systematic coverage with credible implementation
correspondence and manageable maintenance cost. A new unknown bug is valuable
but not required. If state explosion or model drift defeats the pilot, report
that result and consider narrower models or deterministic simulation; a green
abstract model alone is not sufficient justification for a broad rewrite.

### Historical Local Explorer Comparison (2026-09-22)

Comparison baseline: commit `98723f7a`. Dual-engine tests, the adapter, and the
Stateright commands in this subsection refer to that revision. Current tests
use only the local explorer; retained coverage and selection are recorded below.

The [storage-owned explorer](../crates/storage/src/bounded_explorer.rs) is a
test-only, single-threaded breadth-first search with exact `Eq`/`Hash` visited
states, an append-only FIFO frontier, and parent/action links. Hash collisions
are resolved by equality, not treated as identical states. It checks safety at
every visited state, retains the first witness for each reachability property,
and returns a shortest counterexample in its declared action graph. Finding
witnesses does not stop exploration. Required reachability properties are
declared up front in the local contract; `assert_complete()` rejects any missing
witness even after safe graph exhaustion. Duplicate declarations and undeclared
witness reports fail as model errors. State-cap exhaustion is a distinct
incomplete result, never success. An empty initial-state set fails rather than
passing vacuously. Models still own finite action/budget bounds and complete
state equality; there is no implicit depth cutoff or clock normalization.

The deliberately narrow scope excludes linearizability/history checking,
temporal logic, fairness, symmetry, partial-order reduction, networking,
parallel exploration, and a UI. None of those Stateright facilities is used by
the current models. This is not an attempt to build a general-purpose checker.

A temporary adapter at `98723f7a:crates/storage/src/bounded_explorer/comparison.rs`
ran identical model actions, transitions and properties through the local engine.
It registered every `Sometimes` property as required reachability and rejected
unsupported liveness properties and duplicate property names. This separated
engine comparison from model adaptation. The adapter is now removed, and each
model declares its required properties directly to the explorer.

The default-suite comparison tests at that baseline required:

- Exact lifecycle-state set equality at depths six and seven, plus equality
  of successor counts. The original depth-six exhaustive enumerator remains.
- Exact renewal trace and observation equality, not merely equal counts.
- Evaluation of every generated state, replayable positive witnesses for
  every existing reachability property, and matching false-property detection.
- Explorer regressions covering deliberate hash collisions, cycles,
  diamond joins, deterministic shortest traces, multiple initial states,
  initial/terminal checks, missing witnesses, budget-sensitive state identity,
  boundary-state checks, resource exhaustion, and empty initial input. The
  declared-but-unreachable regression requires completion assertion to fail
  even when another declared witness was found. Additional regressions reject
  duplicate/undeclared witnesses and lock registration through the adapter.

Warm unoptimized measurements, with one checker thread and identical bounds:

| Model | Exact states, both engines | Successors, both engines | Local | Stateright |
| --- | --- | --- | --- | --- |
| Lifecycle depth six | 2,082 | 3,990 | 0.309 s | 0.324 s |
| Lifecycle depth seven | 5,096 | 10,410 | 0.792 s | 0.834 s |
| Incarnation renewal depth six | 2,146 | 2,145 | 0.118 s | 0.119 s |

These are initial comparison-run observations, not benchmark thresholds or a
claim of a statistically significant speedup. The later focused run also
passed the exact-set comparisons. The renewal model deliberately retains full
traces and does not merge semantically equivalent schedules under either engine.

Memory was measured separately using the default-built storage test executable,
not by timing Cargo or keeping both engines' state sets in one process. Three
fresh process runs per engine alternated order (`local, Stateright, Stateright,
local, local, Stateright`) for their depth-seven serving tests:

| Engine | Process peak RSS range | Median peak RSS | Wall time range |
| --- | --- | --- | --- |
| Local | 86,308–86,500 KiB | 86,476 KiB | 0.84–0.86 s |
| Stateright | 89,616–90,072 KiB | 89,804 KiB | 0.88–0.89 s |

Both tests explore the same state/transition counts and replay serving evidence.
RSS includes the common model/interner, library test harness, allocator, and
loaded executable pages; it is not isolated engine heap usage. This small
sample establishes comparable process cost, not a general memory advantage.
The local engine holds each state in an `Arc` shared between its frontier and
visited table; the comparison still includes existing model-owned bookkeeping.

Reproduce those resource observations by resolving the storage `binary-path`
with `cargo nextest list --locked -p storage --list-type binaries-only
--message-format json`, then invoking each permanent test separately:

```bash
/usr/bin/time -f 'RESOURCE elapsed_seconds=%e max_rss_kib=%M' <storage-test-binary> \
  --exact control_plane::tests::model_checking::pending_command_local_explorer_observed_recovery_serves_at_depth_seven --nocapture
/usr/bin/time -f 'RESOURCE elapsed_seconds=%e max_rss_kib=%M' <storage-test-binary> \
  --exact control_plane::tests::model_checking::pending_command_stateright_observed_recovery_serves_at_depth_seven --nocapture
```

The unchanged historical mutation patch was applied to the production renewal
filter. Both engines failed and replayed the same invalid old-generation lease
extension. The local engine reported the shorter schedule:

```text
Install(new) -> Send(new) -> Deliver(new)
```

Stateright reported the previously recorded six-action schedule. No known
schedule is supplied to either search. The two failure runs are expected
mutation evidence, not failures ignored by rerunning. The production filter was
restored immediately, and all 65 focused explorer, model, and runtime-map tests
then passed. Storage all-targets/all-features Clippy with warnings denied,
formatting, and the storage boundary checker passed. Following required-witness
review hardening, all 15 focused explorer/engine-comparison tests and the same
Clippy, formatting, and boundary checks passed. The first full workspace run
failed in `direct_put_command_id_race_retries_abandoned_terminal_cleanup` with
`metadata_command_recovery_transferred`; that failure was handed off for separate
investigation without changing the test. The user-requested full-suite retry
passed all 9,114 tests, none skipped, in 250.014 s. This retry does not resolve
the original failure. No production behavior changed.
To repeat the paired mutation run, use the existing patch procedure
with the filter `test(incarnation_renewal_local_explorer_matches_stateright) |
test(incarnation_renewal_stateright_exhausts_bounded_schedules)` and
`--no-fail-fast`; reverse the patch before normal verification.

Maintenance assessment at the comparison baseline: the local engine was
191 lines, with 213 lines of focused engine tests and 94 lines of temporary
comparison adapter (including its registration regression), including comments
and formatting. Differential model tests add temporary integration coverage.
That slice temporarily increased total harness code because both engines remained.
The complex part—production correspondence, independent oracles and exact state
identity—remains model-owned regardless of engine. The local implementation
replaces only the limited search facilities actually used; it does not eliminate
that modelling cost or establish whole-system correctness.

### Selected Engine And Retained Coverage

Decision: use the local explorer for these bounded safety/reachability models.
Comparable coverage/cost and the small owned implementation support that choice;
a claimed speed advantage is not needed. Stateright, its development dependency,
and the temporary adapter and dual-engine tests are removed. No production
behavior or public API is changed. Linearizability testing, if needed later,
should be evaluated against a concrete concurrent-history workload separately.

The lifecycle model directly declares required pending/converged witnesses and,
at depth seven, serving recovery. The owner-local zero-action regression locks
failure when that registration cannot be satisfied. The renewal model directly
declares all three positive witnesses. `assert_complete()` owns required-witness
validation; replay assertions are additional evidence, not the sole safeguard.

Retained default-suite entry points:

- `bounded_explorer::tests::*`: exact equality under hash collision, cycles,
  shortest traces, terminal/initial states, bounds, missing/duplicate/undeclared
  witnesses, and non-vacuous completion.
- `pending_command_model_matches_depth_six_enumerator`: exact state-set equality
  with all 19,531 original trace prefixes, 2,082 states and 3,990 successors.
- `pending_command_model_observed_recovery_serves_at_depth_seven`: 5,096 states,
  10,410 successors, complete inspection and replay of every required witness.
- `pending_command_model_identity_includes_nonserialized_fixture_state`,
  `pending_command_model_counterexample_replays_semantic_actions`, and
  `pending_command_model_requires_recovery_witnesses`.
- `incarnation_renewal_model_exhausts_bounded_schedules`: all 2,146 trace states
  and 2,145 successors, exact generated/checked trace sets and positive replay.
- `incarnation_renewal_discovered_schedule_replays_through_route_handle` and
  the original same-incarnation and historical-renewal regressions, unchanged.

The isolated mutation patch remains reproducible against the production filter;
the current command is in Phase 2 above. Historical pairwise engine comparisons
remain evidence at `98723f7a`, not ongoing dependency requirements.

Removal verification: the historical mutation still fails with the local
counterexample `[Install, Send, Deliver(1)]`. After restoring the production
filter, all 65 focused explorer, lifecycle and runtime-map tests passed.
Storage all-targets/all-features Clippy with warnings denied, formatting and the
storage boundary checker passed. After review, the full `cargo nextest run
--locked` passed all 9,110 tests (3 slow, none skipped) in 376.659 s.

Next: finish the existing lifecycle model's explicit clock advancement and
separately deliverable heartbeat observations before expanding into payload
ownership. Revisit bounds and independent oracles for those new interleavings.

## Follow-On Slice — Publication, Recovery, And Payload Ownership

Start after the lifecycle time/message extensions above; the engine decision
gate is complete. Initial proposed configuration: one PG,
one object, two competing requests, and three replicas (primary, off-primary
witness, trailing replica). Fix exact command/crash/message bounds before
implementation. Route-transition and cross-PG composition are later extensions.

- [ ] Model pending-slot installation, publication-start marking, witness then
  primary application, trailing convergence, exact replay, terminal cleanup,
  reservation ownership, and physical payload ownership as separate steps.
- [ ] Include lost responses, delayed application, request-budget expiry,
  recovery takeover, cancellation and process restart. Model exact command
  identity and partially overlapping staged payloads, not just a batch boolean.
- [ ] Assert that visible metadata references durable payload, command-owned
  payload survives ambiguous outcomes, competing work cannot steal reservations,
  and retries cannot replace an irrevocably published command.
- [ ] Keep physical reclamation permitted only by an independent ownership
  argument; add bounded cleanup progress checks under explicitly healthy
  delivery/scheduling assumptions.
- [ ] Demonstrate relevant historical ownership/publication bugs and replay
  counterexamples through production paths, including transport where the
  outcome-classification boundary matters.

Estimated additional effort: two to four engineering weeks, depending on the
amount of transition extraction needed. Broader lease/peering, reclaim/read
handle, and cross-PG bucket-deletion models are separate reviewed slices. This
plan does not promise exhaustive whole-cluster verification.

## Verification And Handoff

Keep small bounded model tests in the default test suite, not solely behind an
optional feature or ignored target. Provide larger exploratory runs separately;
record incomplete searches honestly. Counterexamples should retain semantic
actions and configuration, not depend only on unstable action-list indices.

For code slices, run focused model and implementation regressions, the relevant
storage boundary checks, formatting, and warnings-denied Clippy. Run the full
`cargo nextest run` before committing. Keep real durability/crash tests and
soaks; model results do not replace their evidence. No AWS behavior should
change for model integration; any intentional S3-facing behavior change still
requires shared AWS/local tests.

Complete the pilot with a concise results section in this plan and an explicit
continue/narrow/stop decision. Completion means the experiment was evaluated,
not that the listed protocols or distributed system are proved correct.
