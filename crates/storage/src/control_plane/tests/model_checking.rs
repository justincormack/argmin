// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! First pilot: preserve the existing depth-six lifecycle semantics exactly.
//! Installation/convergence are abstract node events; heartbeat delivery and
//! command application are atomic here. This is not a crash/transport model.

use super::*;
use crate::bounded_explorer::{explore, Checks, Exploration};
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

const DEPTH: u8 = 6;
const TRACE_PREFIXES: usize = 19_531;
const RECOVERY_DEPTH: u8 = 7;
const MAX_STATES: usize = 97_656; // All five-action prefixes through depth seven.
const ACTIONS: [PendingCommandLifecycleOp; 5] = [
    PendingCommandLifecycleOp::InstallPending,
    PendingCommandLifecycleOp::ConvergePending,
    PendingCommandLifecycleOp::Heartbeat,
    PendingCommandLifecycleOp::CompleteReadyPeerings,
    PendingCommandLifecycleOp::Restart,
];
const SAFETY: &str = "production state agrees with lifecycle oracle";
const PENDING: &str = "pending recovery is discoverable";
const CONVERGED: &str = "observed pending recovery converges after restart";
const RECOVERED: &str = "converged command can reactivate after restart";

#[derive(Debug, Clone, PartialEq, Eq)]
struct StateData {
    case: PendingCommandLifecycleCase,
    remaining: u8,
    transition_error: Option<String>,
    // Require rediscovery after restart, not merely completion before restart.
    saw_pending_recovery: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct State {
    ordinal: usize,
    data: Arc<StateData>,
}

impl Hash for State {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.ordinal.hash(state);
    }
}

/// Intern by *exact* fixture equality before giving a state a numeric identity.
/// The persisted snapshot text is only a lookup bucket: its encoder prunes
/// history, so it must not itself define equality of the live snapshot.
/// Neither redacted Debug nor a route digest is a complete state identity.
#[derive(Default)]
struct States {
    buckets: BTreeMap<String, Vec<State>>,
    count: usize,
}

impl States {
    fn intern(&mut self, data: StateData) -> State {
        let bucket = self
            .buckets
            .entry(format_snapshot(&data.case.snapshot))
            .or_default();
        if let Some(existing) = bucket.iter().find(|state| *state.data == data) {
            return existing.clone();
        }
        // A hard harness failure, never a successful truncated exploration.
        assert!(self.count < MAX_STATES, "pilot state bound exceeded");
        let state = State {
            ordinal: self.count,
            data: Arc::new(data),
        };
        self.count += 1;
        bucket.push(state.clone());
        state
    }

    fn assert_same_states(&self, other: &Self) {
        assert_eq!(self.count, other.count, "reachable state count differs");
        assert_eq!(self.buckets.len(), other.buckets.len());
        for (key, states) in &self.buckets {
            let actual = other.buckets.get(key).expect("reachable snapshot missing");
            assert_eq!(states.len(), actual.len());
            for state in states {
                assert!(
                    actual.iter().any(|candidate| candidate.data == state.data),
                    "missing exact reachable state: {:?}",
                    state.data
                );
            }
        }
    }
}

struct Lifecycle {
    initial: State,
    states: Mutex<States>,
    checked: Mutex<BTreeSet<usize>>,
}

#[test]
fn pending_command_model_identity_includes_nonserialized_fixture_state() {
    let data = StateData {
        case: PendingCommandLifecycleCase::new(),
        remaining: DEPTH,
        transition_error: None,
        saw_pending_recovery: false,
    };
    let mut states = States::default();
    let original = states.intern(data.clone());
    assert_eq!(states.intern(data.clone()), original);

    let mut changed_time = data.clone();
    changed_time.case.now_ms += 1;
    assert_ne!(states.intern(changed_time).ordinal, original.ordinal);
    let mut changed_slot = data.clone();
    changed_slot.case.model.slot = PendingCommandSlotState::Pending;
    assert_ne!(states.intern(changed_slot).ordinal, original.ordinal);
    let mut changed_bound = data.clone();
    changed_bound.remaining -= 1;
    assert_ne!(states.intern(changed_bound).ordinal, original.ordinal);
    let mut changed_error = data.clone();
    changed_error.transition_error = Some("failed transition".to_owned());
    assert_ne!(states.intern(changed_error).ordinal, original.ordinal);
    let mut changed_monitor = data;
    changed_monitor.saw_pending_recovery = true;
    assert_ne!(states.intern(changed_monitor).ordinal, original.ordinal);
    assert_eq!(states.count, 6);
    assert_eq!(
        states.buckets.len(),
        1,
        "all variants share serialized snapshot bytes"
    );
}

impl Lifecycle {
    fn new(initial: PendingCommandLifecycleCase, depth: u8) -> Self {
        assert!(depth <= RECOVERY_DEPTH);
        let mut states = States::default();
        let initial = states.intern(StateData {
            case: initial,
            remaining: depth,
            transition_error: None,
            saw_pending_recovery: false,
        });
        Self {
            initial,
            states: Mutex::new(states),
            checked: Mutex::new(BTreeSet::new()),
        }
    }

    fn safe(&self, state: &State) -> bool {
        self.checked.lock().unwrap().insert(state.ordinal);
        state.data.transition_error.is_none() && state.data.case.check_matches_model().is_ok()
    }

    fn assert_all_generated_states_checked(&self, expected: usize) {
        let states = self.states.lock().unwrap();
        let checked = self.checked.lock().unwrap();
        assert_eq!(
            checked.len(),
            states.count,
            "generated but unchecked states"
        );
        assert!((0..states.count).all(|ordinal| checked.contains(&ordinal)));
        assert_eq!(expected, states.count);
    }
}

fn observed_recovery_converged(state: &State) -> bool {
    let case = &state.data.case;
    state.data.saw_pending_recovery
        && case.model.slot == PendingCommandSlotState::Converged
        && !case.model.pending_epoch_current
        && case
            .snapshot
            .pending_metadata_command_recoveries()
            .tasks()
            .is_empty()
}

impl Lifecycle {
    fn next_state(&self, state: &State, action: PendingCommandLifecycleOp) -> Option<State> {
        if state.data.remaining == 0 || state.data.transition_error.is_some() {
            return None;
        }
        let mut data = (*state.data).clone();
        data.remaining -= 1;
        data.transition_error = data.case.transition(action).err();
        data.saw_pending_recovery |= !data.case.model.pending_epoch_current
            && !data
                .case
                .snapshot
                .pending_metadata_command_recoveries()
                .tasks()
                .is_empty();
        Some(self.states.lock().unwrap().intern(data))
    }

    fn successors(&self, state: &State) -> Vec<(PendingCommandLifecycleOp, State)> {
        ACTIONS
            .into_iter()
            .filter_map(|action| self.next_state(state, action).map(|next| (action, next)))
            .collect()
    }

    fn required_witnesses(&self) -> &'static [&'static str] {
        if self.initial.data.remaining >= RECOVERY_DEPTH {
            &[PENDING, CONVERGED, RECOVERED]
        } else {
            &[PENDING, CONVERGED]
        }
    }

    fn checks(&self, state: &State) -> Checks {
        let mut checks = Checks {
            failure: (!self.safe(state)).then_some(SAFETY),
            witnesses: Vec::new(),
        };
        let case = &state.data.case;
        if !case
            .snapshot
            .pending_metadata_command_recoveries()
            .tasks()
            .is_empty()
        {
            checks.witnesses.push(PENDING);
        }
        if observed_recovery_converged(state) {
            checks.witnesses.push(CONVERGED);
            // Activation needs a new-epoch heartbeat: only depth seven can
            // reach this property, retaining the original bounded contract.
            if self.initial.data.remaining >= RECOVERY_DEPTH
                && case
                    .snapshot
                    .active_pg_route(heartbeat_model_pg_id(), case.now_ms)
                    .is_ok()
            {
                checks.witnesses.push(RECOVERED);
            }
        }
        checks
    }

    fn run(&self) -> Exploration<State, PendingCommandLifecycleOp> {
        explore(
            [self.initial.clone()],
            self.required_witnesses(),
            |state| self.successors(state),
            |state| self.checks(state),
            MAX_STATES,
        )
    }

    fn assert_witness_replays(
        &self,
        explored: &Exploration<State, PendingCommandLifecycleOp>,
        name: &'static str,
    ) {
        let actions = explored.witness(name).expect("required witness missing");
        let mut state = self.initial.clone();
        for action in actions {
            state = self.next_state(&state, action).unwrap();
            assert!(
                self.safe(&state),
                "witness replay violated the lifecycle oracle"
            );
        }
        let checks = self.checks(&state);
        assert_eq!(checks.failure, None);
        assert!(
            checks.witnesses.contains(&name),
            "witness did not replay: {name}"
        );
    }
}

// Independent traversal, retaining the original fixture's assertions and action
// order. Do not deduplicate traversal: every one of the 19,531 prefixes runs.
fn enumerate(
    case: PendingCommandLifecycleCase,
    remaining: u8,
    saw_pending_recovery: bool,
    trace: &mut Vec<PendingCommandLifecycleOp>,
    states: &mut States,
) -> usize {
    let saw_pending_recovery = saw_pending_recovery
        || (!case.model.pending_epoch_current
            && !case
                .snapshot
                .pending_metadata_command_recoveries()
                .tasks()
                .is_empty());
    states.intern(StateData {
        case: case.clone(),
        remaining,
        transition_error: None,
        saw_pending_recovery,
    });
    let mut prefixes = 1;
    if remaining > 0 {
        for action in ACTIONS {
            let mut child = case.clone();
            trace.push(action);
            child.apply(action, trace);
            prefixes += enumerate(child, remaining - 1, saw_pending_recovery, trace, states);
            trace.pop();
        }
    }
    prefixes
}

#[test]
fn pending_command_model_matches_depth_six_enumerator() {
    let initial = PendingCommandLifecycleCase::new();
    initial.assert_matches_model(&[]);
    let start = Instant::now();
    let mut baseline = States::default();
    let prefixes = enumerate(
        initial.clone(),
        DEPTH,
        false,
        &mut Vec::new(),
        &mut baseline,
    );
    let baseline_elapsed = start.elapsed();
    assert_eq!(prefixes, TRACE_PREFIXES);

    let model = Lifecycle::new(initial, DEPTH);
    let start = Instant::now();
    let explored = model.run();
    let elapsed = start.elapsed();
    explored.assert_complete();
    model.assert_all_generated_states_checked(explored.state_count());
    baseline.assert_same_states(&model.states.lock().unwrap());
    assert_eq!(explored.state_count(), 2_082);
    assert_eq!(explored.transitions, 3_990);
    for name in model.required_witnesses() {
        model.assert_witness_replays(&explored, name);
    }
    eprintln!("lifecycle model: bound={DEPTH}, prefixes={prefixes}, exact_states={}, transitions={}, baseline={baseline_elapsed:?}, local={elapsed:?}",
        explored.state_count(), explored.transitions);
}

#[test]
fn pending_command_model_observed_recovery_serves_at_depth_seven() {
    let model = Lifecycle::new(PendingCommandLifecycleCase::new(), RECOVERY_DEPTH);
    let start = Instant::now();
    let explored = model.run();
    explored.assert_complete();
    model.assert_all_generated_states_checked(explored.state_count());
    assert_eq!(explored.state_count(), 5_096);
    assert_eq!(explored.transitions, 10_410);
    for name in model.required_witnesses() {
        model.assert_witness_replays(&explored, name);
    }
    let actions = explored.witness(RECOVERED).unwrap();
    assert_eq!(actions.len(), usize::from(RECOVERY_DEPTH));
    eprintln!("recovery model: exact_states={}, transitions={}, elapsed={:?}; serving witness={actions:?}",
        explored.state_count(), explored.transitions, start.elapsed());
}

#[test]
#[should_panic(expected = "required reachability witnesses not found")]
fn pending_command_model_requires_recovery_witnesses() {
    // Exercise the owner registration after removing the comparison bridge.
    // The zero-action bound is safe but cannot witness pending recovery.
    Lifecycle::new(PendingCommandLifecycleCase::new(), 0)
        .run()
        .assert_complete();
}

#[test]
fn pending_command_model_counterexample_replays_semantic_actions() {
    // An intentionally false property tests the harness, not a historical bug.
    let model = Lifecycle::new(PendingCommandLifecycleCase::new(), DEPTH);
    let explored = explore(
        [model.initial.clone()],
        &[],
        |state| model.successors(state),
        |state| Checks {
            failure: (!state
                .data
                .case
                .snapshot
                .pending_metadata_command_recoveries()
                .tasks()
                .is_empty())
            .then_some("no pending command is ever observed"),
            witnesses: Vec::new(),
        },
        MAX_STATES,
    );
    let actions = explored.counterexample().expect("false property must fail");
    assert_eq!(
        actions,
        [
            PendingCommandLifecycleOp::InstallPending,
            PendingCommandLifecycleOp::Heartbeat
        ]
    );
    let mut replay = PendingCommandLifecycleCase::new();
    let mut trace = Vec::new();
    for action in actions {
        trace.push(action);
        replay.apply(action, &trace);
    }
    assert_eq!(
        replay
            .snapshot
            .pending_metadata_command_recoveries()
            .tasks()
            .len(),
        1
    );
}
