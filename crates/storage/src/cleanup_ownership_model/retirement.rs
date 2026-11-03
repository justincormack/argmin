// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Abstract refinement of cleanup completion, NOT production-connected proof.
//! Start with a settled, cancelled durable owner: all write cutoffs have passed,
//! admitted writes have drained, and exact pending/live references are absent.
//! Those are assumptions supplied by the other models, not established here.
//!
//! Two historical file participants, an independent acknowledgement participant,
//! a generation reservation and a bucket-proof obligation complete separately.
//! Success includes the participant's durability barrier; failure may happen
//! before the effect or after it with a lost reply. Worker restart forgets all
//! observations, but not the owner, its exact obligations or completed effects.
//! Repeating exact idempotent operations needs no per-participant durable receipt.
//! Exact identities cannot be reused and closed effects cannot reappear. Route
//! retention, discovery and proof authorization remain external obligations.
//! Each transition abstracts one completed RPC, not physical write admission or
//! syscall/crash ordering. Owner removal abstracts a settled metadata command.
//! One error, one lost reply and one worker restart bound the graph. Availability
//! is fixed per exploration; progress witnesses are reachability, not fairness.

use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Participant {
    FirstFile,
    SecondFile,
    Acknowledgements,
    Generation,
    BucketReservation,
}

const PARTICIPANTS: [Participant; 5] = [
    Participant::FirstFile,
    Participant::SecondFile,
    Participant::Acknowledgements,
    Participant::Generation,
    Participant::BucketReservation,
];

impl Participant {
    fn index(self) -> usize {
        match self {
            Self::FirstFile => 0,
            Self::SecondFile => 1,
            Self::Acknowledgements => 2,
            Self::Generation => 3,
            Self::BucketReservation => 4,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    OmitObligation(Participant),
    ConfirmFailedEffect,
    TrustLostReply,
    ForgetOwnerOnRestart,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    // Durable state. The participant identities/obligations are constant in this
    // model; owner=true retains every obligation even when its effect is absent.
    owner: bool,
    present: [bool; 5],
    // Worker-local successful RPC observations; never written into the owner.
    confirmed: [bool; 5],
    // Finite budgets and ghost history, included in state equality.
    failed: Option<Participant>,
    lost_reply: Option<Participant>,
    restarted: bool,
    restarted_partially_cleaned: bool,
    retired_without_confirmation: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            owner: true,
            present: [true; 5],
            confirmed: [false; 5],
            failed: None,
            lost_reply: None,
            restarted: false,
            restarted_partially_cleaned: false,
            retired_without_confirmation: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Complete(Participant),
    FailBeforeEffect(Participant),
    ApplyButLoseReply(Participant),
    RestartWorker,
    RetireOwner,
}

impl State {
    fn successors(&self, fault: Fault, unavailable: Option<Participant>) -> Vec<(Action, Self)> {
        let mut out = Vec::new();
        if !self.owner {
            return out;
        }
        for participant in PARTICIPANTS {
            let i = participant.index();
            if self.confirmed[i] || unavailable == Some(participant) {
                continue;
            }
            let mut next = self.clone();
            next.present[i] = false;
            next.confirmed[i] = true;
            out.push((Action::Complete(participant), next));
            if self.failed.is_none() {
                let mut next = self.clone();
                next.failed = Some(participant);
                if fault == Fault::ConfirmFailedEffect {
                    next.confirmed[i] = true;
                }
                out.push((Action::FailBeforeEffect(participant), next));
            }
            if self.lost_reply.is_none() {
                let mut next = self.clone();
                next.present[i] = false;
                next.lost_reply = Some(participant);
                out.push((Action::ApplyButLoseReply(participant), next));
            }
        }
        if !self.restarted {
            let mut next = self.clone();
            next.restarted = true;
            next.restarted_partially_cleaned =
                self.present.contains(&false) && self.present.contains(&true);
            if fault == Fault::ForgetOwnerOnRestart {
                // Owner loss is retirement even when every effect happened.
                // Preserve missing evidence before discarding volatile replies.
                next.retired_without_confirmation = self.confirmed.contains(&false);
                next.owner = false;
            }
            next.confirmed = [false; 5];
            out.push((Action::RestartWorker, next));
        }
        if PARTICIPANTS.iter().all(|&participant| {
            self.confirmed[participant.index()]
                || fault == Fault::OmitObligation(participant)
                || (fault == Fault::TrustLostReply && self.lost_reply == Some(participant))
        }) {
            let mut next = self.clone();
            next.retired_without_confirmation = self.confirmed.contains(&false);
            next.owner = false;
            out.push((Action::RetireOwner, next));
        }
        out
    }

    fn check(&self) -> Checks {
        let mut checks = Checks::default();
        if !self.owner && self.present.contains(&true) {
            checks.failure = Some("retirement must retain every unfinished obligation");
        } else if !self.owner && self.retired_without_confirmation {
            checks.failure = Some("retirement requires every exact participant confirmation");
        }
        if !self.owner
            && self.failed.is_some()
            && self.lost_reply.is_some()
            && self.restarted_partially_cleaned
        {
            checks
                .witnesses
                .push("retirement after error, lost reply and partial-cleanup restart");
        }
        if self.owner
            && self.restarted_partially_cleaned
            && self.confirmed == [false; 5]
            && self.present.contains(&true)
            && self.present.contains(&false)
        {
            checks
                .witnesses
                .push("restart preserves partial effects and forgets confirmations");
        }
        checks
    }
}

fn run(fault: Fault, unavailable: Option<Participant>) -> Exploration<State, Action> {
    let witnesses = if unavailable.is_none() {
        &[
            "retirement after error, lost reply and partial-cleanup restart",
            "restart preserves partial effects and forgets confirmations",
        ][..]
    } else {
        &["restart preserves partial effects and forgets confirmations"][..]
    };
    explore(
        [State::default()],
        witnesses,
        |state| state.successors(fault, unavailable),
        State::check,
        250_000,
    )
}

fn advance(state: &mut State, action: Action) {
    *state = state
        .successors(Fault::None, None)
        .into_iter()
        .find_map(|(candidate, next)| (candidate == action).then_some(next))
        .unwrap_or_else(|| panic!("disabled {action:?} in {state:?}"));
    assert_eq!(state.check().failure, None, "{action:?}: {state:?}");
}

#[test]
fn cleanup_retirement_model_exhausts_independent_participant_outcomes() {
    let result = run(Fault::None, None);
    result.assert_complete();
    println!(
        "states={}, transitions={}",
        result.state_count(),
        result.transitions
    );
}

#[test]
fn cleanup_retirement_model_detects_each_missing_obligation_and_false_confirmation() {
    for fault in PARTICIPANTS.map(Fault::OmitObligation).into_iter().chain([
        Fault::ConfirmFailedEffect,
        Fault::TrustLostReply,
        Fault::ForgetOwnerOnRestart,
    ]) {
        let result = run(fault, None);
        let expected = if fault == Fault::TrustLostReply {
            "retirement requires every exact participant confirmation"
        } else {
            "retirement must retain every unfinished obligation"
        };
        assert!(
            matches!(result.termination, Termination::Violation { property, .. } if property == expected),
            "{fault:?}: {:?}, trace={:?}",
            result.termination,
            result.counterexample()
        );
        println!("{fault:?}: {:?}", result.counterexample().unwrap());
    }
}

#[test]
fn cleanup_retirement_model_unavailable_participant_preserves_owner_not_other_work() {
    for unavailable in PARTICIPANTS {
        let result = run(Fault::None, Some(unavailable));
        result.assert_complete();
        assert!(result
            .states()
            .all(|state| state.owner && state.present[unavailable.index()]));
        // Reachability only: every other participant can finish while this one
        // remains unavailable, including after worker restart. No fairness claim.
        assert!(
            result.states().any(|state| {
                state.restarted_partially_cleaned
                    && PARTICIPANTS.iter().all(|&participant| {
                        participant == unavailable || state.confirmed[participant.index()]
                    })
            }),
            "healthy participants must progress around {unavailable:?}"
        );
    }
}

#[test]
fn cleanup_retirement_model_retries_each_ambiguous_effect_after_restart() {
    for ambiguous in PARTICIPANTS {
        let mut state = State::default();
        let confirmed_before_restart = PARTICIPANTS
            .into_iter()
            .find(|&participant| participant != ambiguous)
            .unwrap();
        advance(&mut state, Action::Complete(confirmed_before_restart));
        advance(&mut state, Action::ApplyButLoseReply(ambiguous));
        assert!(!state.present[ambiguous.index()]);
        assert!(!state.confirmed[ambiguous.index()]);
        let durable_before_restart = state.present;
        advance(&mut state, Action::RestartWorker);
        assert!(state.owner);
        assert_eq!(state.present, durable_before_restart);
        assert_eq!(state.confirmed, [false; 5]);
        for participant in PARTICIPANTS {
            if participant != ambiguous {
                advance(&mut state, Action::Complete(participant));
            }
        }
        assert_eq!(state.present, [false; 5]);
        assert!(!state
            .successors(Fault::None, None)
            .iter()
            .any(|(a, _)| *a == Action::RetireOwner));
        advance(&mut state, Action::Complete(ambiguous));
        advance(&mut state, Action::RetireOwner);
        assert!(!state.owner);
    }
}

#[test]
fn cleanup_retirement_model_restart_detects_owner_loss_after_unconfirmed_effects() {
    let forget_owner_on_restart = |state: &State| {
        state
            .successors(Fault::ForgetOwnerOnRestart, None)
            .into_iter()
            .find_map(|(action, next)| (action == Action::RestartWorker).then_some(next))
            .expect("restart must be enabled")
    };
    for ambiguous in PARTICIPANTS {
        let mut state = State::default();
        advance(&mut state, Action::ApplyButLoseReply(ambiguous));
        for participant in PARTICIPANTS {
            if participant != ambiguous {
                advance(&mut state, Action::Complete(participant));
            }
        }
        assert_eq!(state.present, [false; 5]);
        assert_eq!(state.confirmed, PARTICIPANTS.map(|p| p != ambiguous));

        // No remaining physical/resource effect can mask the lost confirmation.
        let forgotten = forget_owner_on_restart(&state);
        assert!(!forgotten.owner);
        assert_eq!(forgotten.present, [false; 5]);
        assert_eq!(forgotten.confirmed, [false; 5]);
        assert_eq!(
            forgotten.check().failure,
            Some("retirement requires every exact participant confirmation"),
            "lost reply from {ambiguous:?} must survive as retirement evidence"
        );

        let mut retained = state.clone();
        advance(&mut retained, Action::RestartWorker);
        assert!(retained.owner);
        assert_eq!(retained.confirmed, [false; 5]);

        // Capture evidence before restart clears volatile confirmations. Losing
        // an already fully confirmed owner is not an unconfirmed retirement.
        advance(&mut state, Action::Complete(ambiguous));
        let confirmed = forget_owner_on_restart(&state);
        assert_eq!(confirmed.confirmed, [false; 5]);
        assert!(!confirmed.retired_without_confirmation);
        assert_eq!(confirmed.check().failure, None);
    }
}

#[test]
fn cleanup_retirement_model_error_never_confirms_or_blocks_other_participants() {
    for failed in PARTICIPANTS {
        let mut state = State::default();
        advance(&mut state, Action::FailBeforeEffect(failed));
        assert_eq!(state.present, [true; 5]);
        assert_eq!(state.confirmed, [false; 5]);
        for participant in PARTICIPANTS {
            if participant != failed {
                advance(&mut state, Action::Complete(participant));
            }
        }
        assert!(state.owner);
        assert!(state.present[failed.index()]);
        assert!(!state
            .successors(Fault::None, None)
            .iter()
            .any(|(action, _)| *action == Action::RetireOwner));
        advance(&mut state, Action::Complete(failed));
        advance(&mut state, Action::RetireOwner);
        assert!(!state.owner);
    }
}
