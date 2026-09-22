// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! First pilot: preserve the existing depth-six lifecycle semantics exactly.
//! Installation/convergence are abstract node events; heartbeat delivery and
//! command application are atomic here. This is not a crash/transport model.

use super::*;
use stateright::{Checker, HasDiscoveries, Model, Property};
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

/// Stateright deduplicates fingerprints, without comparing the full state.
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
fn pending_command_stateright_identity_includes_nonserialized_fixture_state() {
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

impl Model for Lifecycle {
    type State = State;
    type Action = PendingCommandLifecycleOp;

    fn init_states(&self) -> Vec<State> {
        vec![self.initial.clone()]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Self::Action>) {
        if state.data.remaining > 0 && state.data.transition_error.is_none() {
            actions.extend(ACTIONS);
        }
    }

    fn next_state(&self, state: &State, action: Self::Action) -> Option<State> {
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

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always(SAFETY, Self::safe),
            Property::sometimes(PENDING, |_, state: &State| {
                !state
                    .data
                    .case
                    .snapshot
                    .pending_metadata_command_recoveries()
                    .tasks()
                    .is_empty()
            }),
            Property::sometimes(CONVERGED, |_, state: &State| {
                observed_recovery_converged(state)
            }),
        ];
        // Observation, convergence, activation and the new-epoch heartbeat
        // require seven actions. Do not mistake insufficient bounds for a bug.
        if self.initial.data.remaining >= RECOVERY_DEPTH {
            properties.push(Property::sometimes(RECOVERED, |_, state: &State| {
                let case = &state.data.case;
                observed_recovery_converged(state)
                    && case
                        .snapshot
                        .active_pg_route(heartbeat_model_pg_id(), case.now_ms)
                        .is_ok()
            }));
        }
        properties
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
fn pending_command_stateright_matches_depth_six_enumerator() {
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

    let model = Lifecycle::new(initial.clone(), DEPTH);
    let start = Instant::now();
    // No timeout, target-state cap, or checker depth cap: enabled actions encode
    // the depth bound and all terminal states still undergo property checks.
    let checker = model
        .checker()
        .threads(1)
        .finish_when(HasDiscoveries::AnyFailures)
        .spawn_bfs()
        .join();
    let checker_elapsed = start.elapsed();
    checker.assert_properties();
    assert!(checker.is_done());
    let states = checker.model().states.lock().unwrap();
    baseline.assert_same_states(&states);
    assert_eq!(checker.max_depth(), usize::from(DEPTH) + 1);
    let exact_states = states.count;
    drop(states);
    checker
        .model()
        .assert_all_generated_states_checked(checker.unique_state_count());

    // Replay semantic actions, not unstable Stateright action-index paths.
    for name in [PENDING, CONVERGED] {
        let discovery = checker.discovery(name).unwrap();
        let expected = discovery.last_state().data.case.clone();
        let actions = discovery.into_actions();
        let mut replay = initial.clone();
        let mut trace = Vec::new();
        for action in &actions {
            trace.push(*action);
            replay.apply(*action, &trace);
        }
        assert_eq!(
            replay, expected,
            "discovery must replay the exact production fixture"
        );
        eprintln!("{name}: {actions:?}");
    }
    eprintln!(
        "lifecycle pilot: bound={DEPTH} actions, prefixes={prefixes}, exact_states={}, \
         checker_generated={}, checker_transitions={}, baseline={baseline_elapsed:?}, \
         stateright={checker_elapsed:?}; completed bounded search, no symmetry",
        exact_states,
        checker.state_count(),
        checker.state_count() - 1,
    );
}

#[test]
fn pending_command_stateright_observed_recovery_serves_at_depth_seven() {
    let initial = PendingCommandLifecycleCase::new();
    let start = Instant::now();
    let checker = Lifecycle::new(initial.clone(), RECOVERY_DEPTH)
        .checker()
        .threads(1)
        .finish_when(HasDiscoveries::AnyFailures)
        .spawn_bfs()
        .join();
    let elapsed = start.elapsed();
    checker.assert_properties();
    assert!(checker.is_done());
    checker
        .model()
        .assert_all_generated_states_checked(checker.unique_state_count());
    assert_eq!(checker.max_depth(), usize::from(RECOVERY_DEPTH) + 1);

    let actions = checker.discovery(RECOVERED).unwrap().into_actions();
    assert_eq!(actions.len(), usize::from(RECOVERY_DEPTH));
    let mut replay = initial;
    let mut trace = Vec::new();
    let mut observed = false;
    for action in &actions {
        trace.push(*action);
        replay.apply(*action, &trace);
        observed |= !replay.model.pending_epoch_current
            && !replay
                .snapshot
                .pending_metadata_command_recoveries()
                .tasks()
                .is_empty();
    }
    assert!(observed);
    assert_eq!(replay.model.slot, PendingCommandSlotState::Converged);
    assert!(!replay.model.pending_epoch_current);
    assert!(replay
        .snapshot
        .pending_metadata_command_recoveries()
        .tasks()
        .is_empty());
    assert!(replay
        .snapshot
        .active_pg_route(heartbeat_model_pg_id(), replay.now_ms)
        .is_ok());
    eprintln!(
        "recovery pilot: bound={RECOVERY_DEPTH} actions, exact_states={}, generated={}, \
         elapsed={elapsed:?}; completed bounded search; serving witness={actions:?}",
        checker.unique_state_count(),
        checker.state_count(),
    );
}

/// An intentionally false property tests the harness, not a historical bug.
struct NegativeControl(Lifecycle);

impl Model for NegativeControl {
    type State = State;
    type Action = PendingCommandLifecycleOp;

    fn init_states(&self) -> Vec<State> {
        self.0.init_states()
    }

    fn actions(&self, state: &State, actions: &mut Vec<Self::Action>) {
        self.0.actions(state, actions);
    }

    fn next_state(&self, state: &State, action: Self::Action) -> Option<State> {
        self.0.next_state(state, action)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![Property::always(
            "no pending command is ever observed",
            |_, state: &State| {
                state
                    .data
                    .case
                    .snapshot
                    .pending_metadata_command_recoveries()
                    .tasks()
                    .is_empty()
            },
        )]
    }
}

#[test]
fn pending_command_stateright_counterexample_replays_semantic_actions() {
    let initial = PendingCommandLifecycleCase::new();
    let checker = NegativeControl(Lifecycle::new(initial.clone(), DEPTH))
        .checker()
        .threads(1)
        .finish_when(HasDiscoveries::AnyFailures)
        .spawn_bfs()
        .join();
    let actions = checker
        .discovery("no pending command is ever observed")
        .expect("the deliberately false property must produce a counterexample")
        .into_actions();
    assert_eq!(
        actions,
        [
            PendingCommandLifecycleOp::InstallPending,
            PendingCommandLifecycleOp::Heartbeat
        ]
    );
    let mut replay = initial;
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
