// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bounded composition of real introduction-model transitions with first-window
//! physical admission. NOT production-connected verification. One introduction,
//! three metadata replicas, one file and one separately admitted acknowledgement,
//! one coordination loss, one metadata restart, and one immutable expiry barrier.
//!
//! In this composition the parent's `own_proof` is unresolved cleanup custody,
//! not evidence that a primary-local bucket row survives. `coordination` is the
//! independently losable attempt liveness. Releasing custody after loss means
//! authoritative confirmation of absence, not reconstructing the lost record.
//! Replica inspections and compaction are macro-steps using the parent's actual
//! transitions; apply remains separate for witness, primary and trailing node.
//! Terminal publication is the parent's atomic abstraction, not terminal fanout.
//! Route certificates, real clocks, RPC codecs, multiple windows, data-node restart
//! and new-client multipart retries remain outside this graph.

use super::{
    Action as IntroAction, Disposition, Fault as IntroFault, Install, State as Introduction,
};
use super::{PRIMARY, TRAILING, WITNESS};
use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Effect {
    Unsent,
    Sent,
    Confirmed,
    Admitted,
    Persisted,
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum LossPoint {
    BeforeAcceptance,
    Accepted,
    WitnessOnly,
    Ready,
    InFlight,
    Published,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    ConfirmPending,
    ConfirmWitnessOnly,
    ForgetOwnerOnCoordinationLoss,
    AbandonAcceptedOnCoordinationLoss,
    RetireBeforePhysicalCleanup,
    CleanupAtCoordinationLoss,
    IgnoreFileGuard,
    IgnoreAckGuard,
    RequireCoordinationForRecovery,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    intro: Introduction,
    coordination: bool,
    loss: Option<LossPoint>,
    effects: [Effect; 2],
    durable: [bool; 2],
    expired: bool,
    deleted: bool,
    unsafe_confirmation: bool,
    accepted_before_loss: bool,
    terminal_accepted: bool,
    restarted_after_loss: bool,
    persisted_after_cutoff: [bool; 2],
}

impl Default for State {
    fn default() -> Self {
        Self {
            intro: Introduction::default(),
            coordination: true,
            loss: None,
            effects: [Effect::Unsent; 2],
            durable: [false; 2],
            expired: false,
            deleted: false,
            unsafe_confirmation: false,
            accepted_before_loss: false,
            terminal_accepted: false,
            restarted_after_loss: false,
            persisted_after_cutoff: [false; 2],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Introduction(IntroAction),
    InspectPrefix,
    CompactPrefix,
    LoseCoordination,
    Send(usize),
    Confirm(usize),
    Admit(usize),
    Persist(usize),
    Reject(usize),
    Expire,
    DeleteEffects,
    AcceptTerminal,
}

fn advance_intro(state: &Introduction, action: IntroAction) -> Option<Introduction> {
    state
        .successors(IntroFault::None)
        .into_iter()
        .find_map(|(candidate, next)| (candidate == action).then_some(next))
}

fn prefix_macro(state: &Introduction, compact: bool) -> Option<Introduction> {
    let mut next = state.clone();
    let mut changed = false;
    for index in [WITNESS, PRIMARY, TRAILING] {
        let needed = if compact {
            next.replicas[index].log
        } else {
            !next.confirmed[index]
        };
        if needed {
            next = advance_intro(
                &next,
                if compact {
                    IntroAction::Compact(index)
                } else {
                    IntroAction::InspectReplica(index)
                },
            )?;
            changed = true;
        }
    }
    changed.then_some(next)
}

impl State {
    fn ready(&self) -> bool {
        self.intro.settled() && !self.intro.pending && self.intro.replicas.iter().all(|r| r.owner)
    }

    fn successors(&self, fault: Fault) -> Vec<(Action, Self)> {
        let mut out = Vec::new();
        for (action, intro) in self.intro.successors(IntroFault::None) {
            let allowed = match action {
                IntroAction::InspectReplica(_) | IntroAction::Compact(_) => false,
                IntroAction::AcceptInstall => self.coordination,
                IntroAction::AcquireRecovery => {
                    !self.intro.frontend
                        && (self.coordination || fault != Fault::RequireCoordinationForRecovery)
                }
                IntroAction::Publish => self.terminal_accepted,
                IntroAction::Cancel => !self.terminal_accepted,
                IntroAction::Retire(_) => {
                    self.deleted || fault == Fault::RetireBeforePhysicalCleanup
                }
                IntroAction::Release | IntroAction::FailRelease => {
                    self.intro.disposition != Disposition::Cancelled || self.deleted
                }
                // Receiving a positive reply is not needed for this graph's
                // confirmation path; explicitly explore lost/unknown replies.
                IntroAction::ReceiveReply => false,
                _ => true,
            };
            if allowed {
                let mut next = self.clone();
                next.intro = intro;
                if action == IntroAction::Restart {
                    next.restarted_after_loss = self.loss.is_some();
                }
                out.push((Action::Introduction(action), next));
            }
        }
        for compact in [false, true] {
            if let Some(intro) = prefix_macro(&self.intro, compact) {
                let mut next = self.clone();
                next.intro = intro;
                out.push((
                    if compact {
                        Action::CompactPrefix
                    } else {
                        Action::InspectPrefix
                    },
                    next,
                ));
            }
        }
        // Loss after completed cleanup cannot demonstrate cleanup independent
        // of attempt liveness. Keep this event before physical deletion.
        if self.coordination && !self.deleted {
            let mut next = self.clone();
            let point = if self.intro.disposition == Disposition::Published {
                LossPoint::Published
            } else if self.effects.contains(&Effect::Admitted) {
                LossPoint::InFlight
            } else if self.ready() {
                LossPoint::Ready
            } else if self.intro.replicas[WITNESS].applied && !self.intro.replicas[PRIMARY].applied
            {
                LossPoint::WitnessOnly
            } else if self.intro.install == Install::Accepted {
                LossPoint::Accepted
            } else {
                LossPoint::BeforeAcceptance
            };
            next.coordination = false;
            next.loss = Some(point);
            next.intro.frontend = false;
            next.accepted_before_loss = self.intro.install == Install::Accepted
                && self.intro.disposition != Disposition::Abandoned;
            if fault == Fault::ForgetOwnerOnCoordinationLoss {
                for replica in &mut next.intro.replicas {
                    replica.owner = false;
                }
            }
            if fault == Fault::AbandonAcceptedOnCoordinationLoss && self.intro.started {
                next.intro.disposition = Disposition::Abandoned;
                next.intro.pending = false;
            }
            out.push((Action::LoseCoordination, next));
        }
        for i in 0..2 {
            let mut next = self.clone();
            let action = match self.effects[i] {
                Effect::Unsent
                    if self.coordination
                        && self.intro.frontend
                        && self.intro.disposition == Disposition::Staging
                        && !self.terminal_accepted
                        && (i == 0 || self.durable[0])
                        && !self.expired =>
                {
                    // Early authenticated requests are deliberately included:
                    // sending a descriptor is not usable publication evidence.
                    next.effects[i] = Effect::Sent;
                    Some(Action::Send(i))
                }
                Effect::Sent => {
                    let usable = self.ready() && self.intro.disposition == Disposition::Staging;
                    let broken = (fault == Fault::ConfirmPending && self.intro.pending)
                        || (fault == Fault::ConfirmWitnessOnly
                            && self.intro.replicas[WITNESS].owner);
                    if !self.expired && (usable || broken) {
                        next.effects[i] = Effect::Confirmed;
                        next.unsafe_confirmation |= !usable;
                        Some(Action::Confirm(i))
                    } else {
                        next.effects[i] = Effect::Rejected;
                        Some(Action::Reject(i))
                    }
                }
                Effect::Confirmed if !self.expired => {
                    next.effects[i] = Effect::Admitted;
                    Some(Action::Admit(i))
                }
                Effect::Confirmed => {
                    next.effects[i] = Effect::Rejected;
                    Some(Action::Reject(i))
                }
                Effect::Admitted => {
                    next.durable[i] = true;
                    next.persisted_after_cutoff[i] = self.expired;
                    next.effects[i] = Effect::Persisted;
                    Some(Action::Persist(i))
                }
                _ => None,
            };
            if let Some(action) = action {
                out.push((action, next));
            }
        }
        if !self.expired {
            let mut next = self.clone();
            next.expired = true;
            out.push((Action::Expire, next));
        }
        if self.ready()
            && self.coordination
            && self.intro.frontend
            && !self.expired
            && self.durable == [true; 2]
            && self.intro.disposition == Disposition::Staging
            && !self.terminal_accepted
        {
            let mut next = self.clone();
            next.terminal_accepted = true;
            out.push((Action::AcceptTerminal, next));
        }
        if !self.deleted
            && self.ready()
            && self.intro.disposition == Disposition::Cancelled
            && (self.expired || (fault == Fault::CleanupAtCoordinationLoss && self.loss.is_some()))
            && (self.effects[0] != Effect::Admitted || fault == Fault::IgnoreFileGuard)
            && (self.effects[1] != Effect::Admitted || fault == Fault::IgnoreAckGuard)
        {
            let mut next = self.clone();
            next.durable = [false; 2];
            next.deleted = true;
            out.push((Action::DeleteEffects, next));
        }
        out
    }

    fn check(&self) -> Checks {
        let mut checks = self.intro.check();
        checks.witnesses.clear();
        if self.unsafe_confirmation {
            checks.failure = Some("physical confirmation requires a settled durable owner");
        } else if self.accepted_before_loss
            && self.intro.started
            && self.intro.disposition == Disposition::Abandoned
        {
            checks.failure = Some("coordination loss cannot abandon started introduction");
        } else if self.deleted && self.durable.contains(&true) {
            checks.failure = Some("physical state cannot reappear after cleanup");
        } else if self.intro.replicas.iter().any(|r| r.retired) && !self.deleted {
            checks.failure = Some("owner retirement follows physical completion");
        } else if self.intro.disposition == Disposition::Published && self.durable != [true; 2] {
            checks.failure = Some("published payload survives attempt loss");
        }
        if self.loss == Some(LossPoint::BeforeAcceptance) && self.intro.install == Install::Rejected
        {
            checks
                .witnesses
                .push("lost pre-acceptance attempt fails without owner creation");
        }
        if self.loss == Some(LossPoint::WitnessOnly) && self.ready() && self.restarted_after_loss {
            checks
                .witnesses
                .push("witness-first introduction converges after loss and restart");
        }
        if self.effects.contains(&Effect::Confirmed) && self.loss.is_some() && !self.expired {
            checks
                .witnesses
                .push("delayed confirmed authority survives attempt failure");
        }
        if self.loss.is_some() && self.deleted && self.intro.replicas.iter().all(|r| r.retired) {
            checks
                .witnesses
                .push("cleanup retires owner without restoring coordination");
            if self.intro.restarted_after_release_failure {
                checks
                    .witnesses
                    .push("cleanup survives restart and release failure");
            }
        }
        if self.intro.disposition == Disposition::Published && self.loss.is_some() {
            checks
                .witnesses
                .push("completed publication survives lost coordination");
        }
        if self.intro.replicas.iter().all(|r| r.owner && !r.log)
            && self.effects[0] == Effect::Confirmed
        {
            checks
                .witnesses
                .push("compacted materialized owner still confirms exact physical work");
        }
        if self.persisted_after_cutoff[1] && !self.deleted {
            checks
                .witnesses
                .push("admitted acknowledgement completes across cutoff");
        }
        checks
    }
}

const WITNESSES: &[&str] = &[
    "lost pre-acceptance attempt fails without owner creation",
    "witness-first introduction converges after loss and restart",
    "delayed confirmed authority survives attempt failure",
    "cleanup retires owner without restoring coordination",
    "cleanup survives restart and release failure",
    "completed publication survives lost coordination",
    "compacted materialized owner still confirms exact physical work",
    "admitted acknowledgement completes across cutoff",
];

fn run(fault: Fault) -> Exploration<State, Action> {
    explore(
        [State::default()],
        WITNESSES,
        |s| s.successors(fault),
        State::check,
        500_000,
    )
}

#[test]
fn cleanup_introduction_physical_handoff_exhausts_without_preserving_client_coordination() {
    let result = run(Fault::None);
    result.assert_complete();
    eprintln!(
        "{} states / {} transitions",
        result.checked, result.transitions
    );
}

#[test]
fn cleanup_introduction_physical_handoff_detects_independent_safety_mutations() {
    for (fault, expected) in [
        (
            Fault::ConfirmPending,
            "physical confirmation requires a settled durable owner",
        ),
        (
            Fault::ConfirmWitnessOnly,
            "physical confirmation requires a settled durable owner",
        ),
        (
            Fault::ForgetOwnerOnCoordinationLoss,
            "materialized ownership survives until explicit retirement",
        ),
        (
            Fault::AbandonAcceptedOnCoordinationLoss,
            "coordination loss cannot abandon started introduction",
        ),
        (
            Fault::RetireBeforePhysicalCleanup,
            "owner retirement follows physical completion",
        ),
        (
            Fault::CleanupAtCoordinationLoss,
            "physical state cannot reappear after cleanup",
        ),
        (
            Fault::IgnoreFileGuard,
            "physical state cannot reappear after cleanup",
        ),
        (
            Fault::IgnoreAckGuard,
            "physical state cannot reappear after cleanup",
        ),
    ] {
        let result = run(fault);
        assert!(
            matches!(result.termination, Termination::Violation { property, .. } if property == expected),
            "{fault:?}: {:?}; trace={:?}",
            result.termination,
            result.counterexample()
        );
    }
}

#[test]
fn cleanup_introduction_physical_handoff_does_not_require_live_attempt_for_recovery() {
    let result = run(Fault::RequireCoordinationForRecovery);
    assert_eq!(result.termination, Termination::Complete);
    assert!(result
        .witness("witness-first introduction converges after loss and restart")
        .is_none());
    assert!(result
        .witness("lost pre-acceptance attempt fails without owner creation")
        .is_some());
}

fn step(state: &mut State, action: Action) {
    *state = state
        .successors(Fault::None)
        .into_iter()
        .find_map(|(candidate, next)| (candidate == action).then_some(next))
        .unwrap_or_else(|| panic!("disabled {action:?} in {state:?}"));
    assert_eq!(state.check().failure, None, "after {action:?}: {state:?}");
}

fn disabled(state: &State, action: Action) {
    assert!(
        state
            .successors(Fault::None)
            .iter()
            .all(|(a, _)| *a != action),
        "unexpectedly enabled {action:?} in {state:?}"
    );
}

fn introduce() -> State {
    let mut state = State::default();
    for action in [
        IntroAction::Dispatch,
        IntroAction::AcceptInstall,
        IntroAction::StartPublication,
        IntroAction::Apply(WITNESS),
        IntroAction::Apply(PRIMARY),
        IntroAction::Apply(TRAILING),
    ] {
        step(&mut state, Action::Introduction(action));
    }
    step(&mut state, Action::InspectPrefix);
    step(&mut state, Action::Introduction(IntroAction::ClearSlot));
    state
}

#[test]
fn cleanup_introduction_physical_handoff_recovers_witness_before_confirming_delayed_request() {
    let mut state = State::default();
    for action in [
        IntroAction::Dispatch,
        IntroAction::AcceptInstall,
        IntroAction::StartPublication,
        IntroAction::Apply(WITNESS),
    ] {
        step(&mut state, Action::Introduction(action));
    }
    step(&mut state, Action::Send(0));
    disabled(&state, Action::Confirm(0));
    step(&mut state, Action::LoseCoordination);
    step(&mut state, Action::Introduction(IntroAction::Restart));
    disabled(&state, Action::Introduction(IntroAction::Apply(PRIMARY)));
    for action in [
        IntroAction::AcquireRecovery,
        IntroAction::Apply(PRIMARY),
        IntroAction::Apply(TRAILING),
    ] {
        step(&mut state, Action::Introduction(action));
    }
    step(&mut state, Action::InspectPrefix);
    disabled(&state, Action::Confirm(0));
    step(&mut state, Action::Introduction(IntroAction::ClearSlot));
    step(&mut state, Action::CompactPrefix);
    step(&mut state, Action::Confirm(0));
    step(&mut state, Action::Admit(0));
    step(&mut state, Action::Introduction(IntroAction::Cancel));
    step(&mut state, Action::Expire);
    disabled(&state, Action::DeleteEffects);
    step(&mut state, Action::Persist(0));
    step(&mut state, Action::DeleteEffects);
    assert!(!state.coordination);
    assert_eq!(state.durable, [false; 2]);
    assert!(state.intro.replicas.iter().all(|r| r.owner && !r.log));
}

#[test]
fn cleanup_introduction_physical_handoff_drains_ack_guard_and_rejects_delayed_admission() {
    // Both an already-admitted acknowledgement and one still in transport must
    // be safe, but for different reasons: exclusion versus the immutable cutoff.
    for admitted in [false, true] {
        let mut state = introduce();
        for action in [
            Action::Send(0),
            Action::Confirm(0),
            Action::Admit(0),
            Action::Persist(0),
            Action::Send(1),
            Action::Confirm(1),
        ] {
            step(&mut state, action);
        }
        if admitted {
            step(&mut state, Action::Admit(1));
        }
        step(&mut state, Action::LoseCoordination);
        step(
            &mut state,
            Action::Introduction(IntroAction::AcquireRecovery),
        );
        step(&mut state, Action::Introduction(IntroAction::Cancel));
        disabled(&state, Action::DeleteEffects);
        step(&mut state, Action::Expire);
        if admitted {
            disabled(&state, Action::DeleteEffects);
            step(&mut state, Action::Persist(1));
            assert!(state.persisted_after_cutoff[1]);
        }
        step(&mut state, Action::DeleteEffects);
        step(&mut state, Action::Introduction(IntroAction::FailRelease));
        step(&mut state, Action::Introduction(IntroAction::Restart));
        step(
            &mut state,
            Action::Introduction(IntroAction::AcquireRecovery),
        );
        step(&mut state, Action::Introduction(IntroAction::Release));
        step(
            &mut state,
            Action::Introduction(IntroAction::ConfirmAbsence),
        );
        for i in [WITNESS, PRIMARY, TRAILING] {
            step(&mut state, Action::Introduction(IntroAction::Retire(i)));
        }
        if !admitted {
            disabled(&state, Action::Admit(1));
            step(&mut state, Action::Reject(1));
        }
        assert_eq!(state.durable, [false; 2]);
        assert!(!state.coordination);
        assert!(state.intro.replicas.iter().all(|r| r.retired));
    }
}

#[test]
fn cleanup_introduction_physical_handoff_converges_only_previously_accepted_terminal_work() {
    for accepted in [false, true] {
        let mut state = introduce();
        for i in 0..2 {
            for action in [
                Action::Send(i),
                Action::Confirm(i),
                Action::Admit(i),
                Action::Persist(i),
            ] {
                step(&mut state, action);
            }
        }
        if accepted {
            step(&mut state, Action::AcceptTerminal);
        }
        step(&mut state, Action::LoseCoordination);
        step(&mut state, Action::Introduction(IntroAction::Restart));
        step(
            &mut state,
            Action::Introduction(IntroAction::AcquireRecovery),
        );
        step(&mut state, Action::InspectPrefix);
        disabled(&state, Action::AcceptTerminal);
        step(&mut state, Action::Expire);
        if accepted {
            disabled(&state, Action::Introduction(IntroAction::Cancel));
            step(&mut state, Action::Introduction(IntroAction::Publish));
            disabled(&state, Action::DeleteEffects);
            assert_eq!(state.durable, [true; 2]);
        } else {
            disabled(&state, Action::Introduction(IntroAction::Publish));
            step(&mut state, Action::Introduction(IntroAction::Cancel));
            step(&mut state, Action::DeleteEffects);
        }
    }
    let mut state = State::default();
    step(&mut state, Action::Introduction(IntroAction::Dispatch));
    step(&mut state, Action::LoseCoordination);
    disabled(&state, Action::Introduction(IntroAction::AcceptInstall));
    step(&mut state, Action::Introduction(IntroAction::RejectInstall));
    assert!(state.intro.replicas.iter().all(|r| !r.owner));
    assert_eq!(state.durable, [false; 2]);
}
