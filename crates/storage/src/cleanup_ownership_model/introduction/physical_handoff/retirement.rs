// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Refine the parent's atomic physical deletion into separately fallible file
//! and acknowledgement operations. Reuse its introduction, confirmation, write,
//! cutoff, publication, release and replica-retirement transitions unchanged.
//! This is abstract candidate evidence, NOT production-connected verification.
//! One failed cleanup, one lost cleanup reply and the parent's one restart bound
//! the graph. Exact identities, clocks and durable syscall completion retain the
//! parent's abstractions; this does not model route retention or data-node crash.

use super::{Action as HandoffAction, Fault as HandoffFault, State as Handoff};
use super::{Disposition, Effect, IntroAction};
use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    EarlyCleanup,
    IgnoreFileGuard,
    IgnoreAckGuard,
    TrustLostReply,
    KeepCompletionOnRestart,
}

impl Fault {
    fn handoff(self) -> HandoffFault {
        match self {
            Self::EarlyCleanup => HandoffFault::CleanupAtCoordinationLoss,
            Self::IgnoreFileGuard => HandoffFault::IgnoreFileGuard,
            Self::IgnoreAckGuard => HandoffFault::IgnoreAckGuard,
            Self::None | Self::TrustLostReply | Self::KeepCompletionOnRestart => HandoffFault::None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct State {
    handoff: Handoff,
    confirmed: [bool; 2],
    // Ghost history detects recreation even before the other participant finishes.
    removed: [bool; 2],
    failed: Option<usize>,
    lost_reply: Option<usize>,
    restarted_after_cleanup: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Handoff(HandoffAction),
    Complete(usize),
    Fail(usize),
    LoseReply(usize),
    ConfirmPhysicalCompletion,
}

impl State {
    fn retirement_started(&self) -> bool {
        self.handoff.intro.replicas.iter().any(|r| r.retired)
    }

    fn successors(&self, fault: Fault) -> Vec<(Action, Self)> {
        let mut out = Vec::new();
        for (action, handoff) in self.handoff.successors(fault.handoff()) {
            if action == HandoffAction::DeleteEffects {
                // The parent's guard proves settled cancellation, cutoff and
                // drained physical admission. Keep evaluating it during retry;
                // do not replace it with an assumed "cleanup is now safe" bit.
                for i in 0..2 {
                    if self.confirmed[i] {
                        continue;
                    }
                    let mut next = self.clone();
                    next.handoff.durable[i] = false;
                    next.removed[i] = true;
                    next.confirmed[i] = true;
                    out.push((Action::Complete(i), next));
                    if self.failed.is_none() {
                        let mut next = self.clone();
                        next.failed = Some(i);
                        out.push((Action::Fail(i), next));
                    }
                    if self.lost_reply.is_none() {
                        let mut next = self.clone();
                        next.handoff.durable[i] = false;
                        next.removed[i] = true;
                        next.lost_reply = Some(i);
                        out.push((Action::LoseReply(i), next));
                    }
                }
                if (0..2).all(|i| {
                    self.confirmed[i]
                        || (fault == Fault::TrustLostReply && self.lost_reply == Some(i))
                }) {
                    let mut next = self.clone();
                    next.handoff = handoff;
                    out.push((Action::ConfirmPhysicalCompletion, next));
                }
                continue;
            }
            let mut next = self.clone();
            next.handoff = handoff;
            if action == HandoffAction::Introduction(IntroAction::Restart) {
                next.restarted_after_cleanup = self.removed.contains(&true);
                next.confirmed = [false; 2];
                // Before accepted retirement there is no durable completion
                // receipt. Reconfirm both participants, even if both are absent.
                // A retired replica instead witnesses irrevocable retirement;
                // its remaining replicas converge the already accepted command.
                if !self.retirement_started() && fault != Fault::KeepCompletionOnRestart {
                    next.handoff.deleted = false;
                }
            }
            out.push((Action::Handoff(action), next));
        }
        out
    }

    fn check(&self) -> Checks {
        let mut checks = self.handoff.check();
        checks.witnesses.clear();
        if (0..2).any(|i| self.removed[i] && self.handoff.durable[i]) {
            checks.failure = Some("closed participant cannot reappear during partial cleanup");
        } else if self.handoff.deleted && !self.retirement_started() && self.confirmed != [true; 2]
        {
            checks.failure = Some("physical completion requires fresh participant confirmations");
        }
        if self.handoff.intro.replicas.iter().all(|r| r.retired)
            && self.restarted_after_cleanup
            && self.lost_reply.is_some()
            && self.failed.is_some()
            && self.handoff.persisted_after_cutoff[1]
        {
            checks
                .witnesses
                .push("late admitted ack, partial cleanup, lost reply and restart converge");
        }
        if self.restarted_after_cleanup
            && self.lost_reply.is_some()
            && !self.retirement_started()
            && !self.handoff.deleted
            && self.confirmed == [false; 2]
            && self.handoff.durable == [false; 2]
        {
            checks
                .witnesses
                .push("restart retains owner despite absent effects and lost reply");
        }
        if self.handoff.intro.disposition == Disposition::Published && self.handoff.loss.is_some() {
            checks
                .witnesses
                .push("published payload never enters participant cleanup");
        }
        checks
    }
}

fn run(fault: Fault) -> Exploration<State, Action> {
    explore(
        [State::default()],
        &[
            "late admitted ack, partial cleanup, lost reply and restart converge",
            "restart retains owner despite absent effects and lost reply",
            "published payload never enters participant cleanup",
        ],
        |state| state.successors(fault),
        State::check,
        750_000,
    )
}

#[test]
fn cleanup_handoff_retirement_composition_exhausts() {
    let result = run(Fault::None);
    result.assert_complete();
    eprintln!(
        "{} states / {} transitions",
        result.checked, result.transitions
    );
}

#[test]
fn cleanup_handoff_retirement_composition_detects_independent_mutations() {
    for (fault, property) in [
        (
            Fault::EarlyCleanup,
            "closed participant cannot reappear during partial cleanup",
        ),
        (
            Fault::IgnoreFileGuard,
            "closed participant cannot reappear during partial cleanup",
        ),
        (
            Fault::IgnoreAckGuard,
            "closed participant cannot reappear during partial cleanup",
        ),
        (
            Fault::TrustLostReply,
            "physical completion requires fresh participant confirmations",
        ),
        (
            Fault::KeepCompletionOnRestart,
            "physical completion requires fresh participant confirmations",
        ),
    ] {
        let result = run(fault);
        assert!(
            matches!(result.termination, Termination::Violation { property: found, .. } if found == property),
            "{fault:?}: {:?}; trace={:?}",
            result.termination,
            result.counterexample()
        );
    }
}

fn step(state: &mut State, action: Action) {
    *state = state
        .successors(Fault::None)
        .into_iter()
        .find_map(|(candidate, next)| (candidate == action).then_some(next))
        .unwrap_or_else(|| panic!("disabled {action:?} in {state:?}"));
    assert_eq!(state.check().failure, None, "after {action:?}: {state:?}");
}

#[test]
fn cleanup_handoff_retirement_reconfirms_each_lost_reply_after_restart() {
    for participant in 0..2 {
        let mut state = State {
            handoff: super::introduce(),
            ..State::default()
        };
        for i in 0..2 {
            for action in [
                HandoffAction::Send(i),
                HandoffAction::Confirm(i),
                HandoffAction::Admit(i),
            ] {
                step(&mut state, Action::Handoff(action));
            }
            if i == 0 {
                step(&mut state, Action::Handoff(HandoffAction::Persist(i)));
            }
        }
        step(&mut state, Action::Handoff(HandoffAction::LoseCoordination));
        step(
            &mut state,
            Action::Handoff(HandoffAction::Introduction(IntroAction::AcquireRecovery)),
        );
        step(
            &mut state,
            Action::Handoff(HandoffAction::Introduction(IntroAction::Cancel)),
        );
        step(&mut state, Action::Handoff(HandoffAction::Expire));
        assert_eq!(state.handoff.effects[1], Effect::Admitted);
        assert!(!state
            .successors(Fault::None)
            .iter()
            .any(|(a, _)| matches!(a, Action::Complete(_))));
        step(&mut state, Action::Handoff(HandoffAction::Persist(1)));
        step(&mut state, Action::Complete(1 - participant));
        step(&mut state, Action::LoseReply(participant));
        assert_eq!(state.handoff.durable, [false; 2]);
        assert!(!state.handoff.deleted);
        step(
            &mut state,
            Action::Handoff(HandoffAction::Introduction(IntroAction::Restart)),
        );
        assert_eq!(state.confirmed, [false; 2]);
        assert!(state.handoff.intro.replicas.iter().all(|r| r.owner));
        step(
            &mut state,
            Action::Handoff(HandoffAction::Introduction(IntroAction::AcquireRecovery)),
        );
        step(&mut state, Action::Handoff(HandoffAction::InspectPrefix));
        for i in 0..2 {
            step(&mut state, Action::Complete(i));
        }
        step(&mut state, Action::ConfirmPhysicalCompletion);
        for action in [
            IntroAction::Release,
            IntroAction::ConfirmAbsence,
            IntroAction::Retire(0),
            IntroAction::Retire(1),
            IntroAction::Retire(2),
        ] {
            step(
                &mut state,
                Action::Handoff(HandoffAction::Introduction(action)),
            );
        }
        assert!(state.handoff.intro.replicas.iter().all(|r| r.retired));
    }
}

#[test]
fn cleanup_handoff_retirement_rejects_delayed_packets_after_partial_retirement_restart() {
    for participant in 0..2 {
        for confirmed in [false, true] {
            let mut state = State {
                handoff: super::introduce(),
                ..State::default()
            };
            if participant == 1 {
                for action in [
                    HandoffAction::Send(0),
                    HandoffAction::Confirm(0),
                    HandoffAction::Admit(0),
                    HandoffAction::Persist(0),
                ] {
                    step(&mut state, Action::Handoff(action));
                }
            }
            step(
                &mut state,
                Action::Handoff(HandoffAction::Send(participant)),
            );
            if confirmed {
                step(
                    &mut state,
                    Action::Handoff(HandoffAction::Confirm(participant)),
                );
            }
            step(&mut state, Action::Handoff(HandoffAction::LoseCoordination));
            for action in [IntroAction::AcquireRecovery, IntroAction::Cancel] {
                step(
                    &mut state,
                    Action::Handoff(HandoffAction::Introduction(action)),
                );
            }
            step(&mut state, Action::Handoff(HandoffAction::Expire));
            for i in 0..2 {
                step(&mut state, Action::Complete(i));
            }
            step(&mut state, Action::ConfirmPhysicalCompletion);
            for action in [
                IntroAction::Release,
                IntroAction::ConfirmAbsence,
                IntroAction::Retire(0),
                IntroAction::Restart,
                IntroAction::AcquireRecovery,
                IntroAction::ConfirmAbsence,
                IntroAction::Retire(1),
                IntroAction::Retire(2),
            ] {
                step(
                    &mut state,
                    Action::Handoff(HandoffAction::Introduction(action)),
                );
            }
            assert_eq!(
                state.confirmed, [false; 2],
                "accepted retirement converges without volatile receipts"
            );
            assert!(state.handoff.intro.replicas.iter().all(|r| r.retired));
            assert!(!state.successors(Fault::None).iter().any(|(a, _)| matches!(
                a,
                Action::Handoff(HandoffAction::Admit(_) | HandoffAction::Confirm(_))
            )));
            step(
                &mut state,
                Action::Handoff(HandoffAction::Reject(participant)),
            );
            assert_eq!(state.handoff.durable, [false; 2]);
        }
    }
}
