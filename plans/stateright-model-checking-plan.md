<!-- Copyright The Argmin Authors. -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# Stateright Model-Checking Experiment

Status: proposed; implementation not started.

## Goal

Evaluate whether bounded model checking can find distributed correctness bugs
more systematically than selected fault schedules and randomized testing alone.
Start with a small production-connected model, demonstrate detection of a
previously fixed defect, and measure the cost before expanding the approach.
Evaluate Stateright itself as part of this pilot: it is the initial candidate,
not a preselected long-term tool.

This is an experiment supporting
[multihost Phase 5: independent correctness evidence](multihost-followup-plan.md#phase-5-independent-correctness-evidence),
particularly its small formal models. Completing this experiment does not
complete that phase, replace deterministic simulation, or establish correctness
of the whole server. Existing fault-injection, process-crash, AWS compatibility,
and soak tests remain necessary.

## Approach And Limits

Trial [Stateright](https://github.com/stateright/stateright) as a development-only
model checker. Its [Model interface](https://docs.rs/stateright/latest/stateright/trait.Model.html)
accepts initial states, enabled actions, transitions, and properties; using its
actor runtime is optional. Do not replace Tokio or OpenRaft to run the experiment.
Select and record the dependency version when implementing the harness.

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

Initially check safety and non-vacuous reachability. Stateright documents
liveness checking as experimental/incomplete; do not base general eventual
recovery claims on it. Bounded recovery checks must explicitly state eventual
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

- [ ] Inventory the pending-command test's production transitions and abstract
  actions. Document which operations are atomic and which durability/network
  effects it does not represent.
- [ ] Define its initial finite configuration, independent safety properties,
  and positive reachability cases, including successful recovery/reactivation.
- [ ] Select one historical defect for the pilot. Prefer one in the lifecycle
  transitions; if none is suitable, use the incarnation-renewal regression as
  a second small model. Identify the faulty decision and its current owner;
  do not expand into the entire payload-publication protocol for this gate.
- [ ] Record model bounds and resource limits, the planned ordinary-test entry
  point, and how search exhaustion is distinguished from early termination.
- [ ] Establish the existing enumerator as the tool-evaluation baseline. Compare
  coverage and cost under equivalent bounds, separating improvements from the
  checker from improvements due to changing the model.

Exit: a bounded experiment with a named historical defect and explicit
implementation coverage. This inventory does not require building a simulator.

## Phase 1 — Adapt The Existing Lifecycle Exploration

- [ ] Add the development dependency and a storage-owned Stateright model using
  the existing production state-machine transitions and independent oracle.
- [ ] First preserve the current actions and depth-six semantics. Compare
  reachable observations and invariant results with the existing enumerator;
  trace count and deduplicated state count are different measurements.
- [ ] Record runtime, memory, states and transitions explored, depth, bounds,
  checker configuration, and termination reason. Do not remove the existing
  enumerator until equivalent coverage has been demonstrated.
- [ ] Separate clock advancement into explicit actions before claiming useful
  state deduplication: the current test advances time on every operation.
  Preserve the existing timing cases and document the revised model's scope.
- [ ] Add separately deliverable heartbeat observations, including delayed
  observations across restart, with a bounded message set. Do not combine send
  and receive into one step when their separation is the behavior under test.
- [ ] Assert positive witnesses as well as safety, so rejecting all work or
  never enabling recovery cannot make the test pass vacuously.

Exit: a normal `cargo nextest run` test with a completed declared bounded search
and reproducible failure traces. A timeout or state/resource cap is an
incomplete exploration, not a successful exhaustive result. Depth-limited
results must be reported only as such.

## Phase 2 — Demonstrate Historical-Bug Detection

For the selected defect:

- [ ] Reproduce the old faulty decision with a reviewable isolated mutation,
  ideally in production logic shared by the model. Keep mutations out of the
  shipped implementation; no runtime switch for unsafe behavior.
- [ ] Require the checker to find a counterexample without supplying the known
  failing schedule. Prefer single-threaded breadth-first search for a short,
  stable explanation, subject to the model's memory limit.
- [ ] Require the corrected implementation to complete the same bounded search.
- [ ] Replay the counterexample through the actual owner implementation using
  explicit barriers/events, not sleeps. Preserve it as a permanent regression.
- [ ] Retain the faulty decision description or reproducible mutation patch,
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

## Experiment Decision Gate

Time-box the initial experiment to roughly one engineering week, then review
results even if a gate is incomplete. This is an estimate, not a deadline that
permits weakening evidence requirements.

Record:

- Historical defects detected and whether production decisions were exercised.
- Bounds exhausted, search costs, and scaling when one bound is increased.
- Amount of duplicated protocol logic and production refactoring required.
- Whether traces are actionable and replayable against real code.
- Coverage missing from the existing enumerator/proptest/fault-injection tests.

- [ ] Evaluate Stateright against the baseline: useful exploration depth,
  deduplication, runtime/memory cost, counterexample quality, production-code
  reuse, modelling overhead, and default-test-suite integration.
- [ ] Decide explicitly whether to retain Stateright, narrow its use, evaluate
  an alternative, or stop. If independent protocol specification or
  liveness/fairness reasoning dominates, consider TLA+/TLC or P. If the main gap
  is implementation scheduling or integrated fault execution, consider Shuttle
  or deterministic simulation instead. These answer different questions; do not
  treat a simulation run as equivalent to exhaustive model exploration.

A second-tool implementation is not mandatory for the pilot. Identify the
specific limitation and comparison question before spending time on one. Tool
selection remains open until this evaluation, even if the model itself is useful.

Continue if it provides useful systematic coverage with credible implementation
correspondence and manageable maintenance cost. A new unknown bug is valuable
but not required. If state explosion or model drift defeats the pilot, report
that result and consider narrower models or deterministic simulation; a green
abstract model alone is not sufficient justification for a broad rewrite.

## Follow-On Slice — Publication, Recovery, And Payload Ownership

Start only after the decision gate. Initial proposed configuration: one PG,
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
