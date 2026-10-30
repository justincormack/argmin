// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Proposed read-only window confirmation, NOT a production RPC model.
//! One exact immutable window, two independent physical effects (file and ack),
//! one healthy-clock cutoff, and one retained owner. Confirmation and physical
//! admission are separate steps; a positive response may be delayed indefinitely.
//! `Ready` assumes authoritative publication/convergence evidence, not merely a
//! witness row. Replica fanout, payload references, authenticated field matching,
//! clock rebinding, route certificates and recovery are separate obligations.
//! Cleanup assumes an already serialized seal and no selected/pending payload.

use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    ConfirmWitnessOnly,
    SkipFileAdmissionCutoff,
    SkipAckAdmissionCutoff,
    CleanBeforeCutoff,
    IgnoreFileGuard,
    IgnoreAckGuard,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Publication {
    Pending,
    WitnessOnly,
    Ready,
    Abandoned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Effect {
    Unsent,
    Sent,
    // A positive confirmation response exists, possibly still in transport.
    Confirmed,
    Admitted,
    Persisted,
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    publication: Publication,
    owner: bool,
    sealed: bool,
    expired: bool,
    effects: [Effect; 2],
    durable: [bool; 2],
    confirmed_without_publication: bool,
    admitted_after_cutoff: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            publication: Publication::Pending,
            owner: true,
            sealed: false,
            expired: false,
            effects: [Effect::Unsent; 2],
            durable: [false; 2],
            confirmed_without_publication: false,
            admitted_after_cutoff: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    ApplyWitness,
    Converge,
    AbandonUnstarted,
    Send(usize),
    Confirm(usize),
    Admit(usize),
    Reject(usize),
    Persist(usize),
    Seal,
    Expire,
    Cleanup,
}

impl State {
    fn successors(&self, fault: Fault) -> Vec<(Action, Self)> {
        let mut out = Vec::new();
        let mut add = |action, change: &dyn Fn(&mut Self)| {
            let mut next = self.clone();
            change(&mut next);
            out.push((action, next));
        };
        match self.publication {
            Publication::Pending => {
                add(Action::ApplyWitness, &|s| {
                    s.publication = Publication::WitnessOnly
                });
                add(Action::AbandonUnstarted, &|s| {
                    s.publication = Publication::Abandoned
                });
            }
            Publication::WitnessOnly => {
                add(Action::Converge, &|s| s.publication = Publication::Ready);
            }
            Publication::Ready | Publication::Abandoned => {}
        }
        for effect in 0..2 {
            match self.effects[effect] {
                // Include adversarial early requests: authenticated transport is
                // not proof that the referenced window is ready for use.
                Effect::Unsent if effect == 0 || self.durable[0] => {
                    add(Action::Send(effect), &|s| s.effects[effect] = Effect::Sent);
                }
                Effect::Sent => {
                    if self.owner
                        && !self.expired
                        && (self.publication == Publication::Ready
                            || (fault == Fault::ConfirmWitnessOnly
                                && self.publication == Publication::WitnessOnly))
                    {
                        add(Action::Confirm(effect), &|s| {
                            s.effects[effect] = Effect::Confirmed;
                            s.confirmed_without_publication |= s.publication != Publication::Ready;
                        });
                    } else {
                        add(Action::Reject(effect), &|s| {
                            s.effects[effect] = Effect::Rejected
                        });
                    }
                }
                Effect::Confirmed => {
                    let skips_cutoff = (effect == 0 && fault == Fault::SkipFileAdmissionCutoff)
                        || (effect == 1 && fault == Fault::SkipAckAdmissionCutoff);
                    if !self.expired || skips_cutoff {
                        // Acquiring the exclusion guard and rechecking the bound
                        // are ONE local transition. Remote lookup holds no guard.
                        add(Action::Admit(effect), &|s| {
                            s.effects[effect] = Effect::Admitted;
                            s.admitted_after_cutoff |= s.expired;
                        });
                    } else {
                        add(Action::Reject(effect), &|s| {
                            s.effects[effect] = Effect::Rejected
                        });
                    }
                }
                Effect::Admitted => {
                    // Completion may legally cross the cutoff: the guard lasts
                    // through persistence and prevents cleanup until this step.
                    add(Action::Persist(effect), &|s| {
                        s.durable[effect] = true;
                        s.effects[effect] = Effect::Persisted;
                    });
                }
                Effect::Unsent | Effect::Persisted | Effect::Rejected => {}
            }
        }
        if !self.sealed
            && matches!(
                self.publication,
                Publication::Ready | Publication::Abandoned
            )
        {
            add(Action::Seal, &|s| s.sealed = true);
        }
        if !self.expired {
            add(Action::Expire, &|s| s.expired = true);
        }
        if self.owner
            && self.sealed
            && (self.expired || fault == Fault::CleanBeforeCutoff)
            && (self.effects[0] != Effect::Admitted || fault == Fault::IgnoreFileGuard)
            && (self.effects[1] != Effect::Admitted || fault == Fault::IgnoreAckGuard)
        {
            add(Action::Cleanup, &|s| {
                s.owner = false;
                s.durable = [false; 2];
            });
        }
        out
    }

    fn check(&self) -> Checks {
        let failure = if self.confirmed_without_publication {
            Some("confirmation requires usable publication evidence")
        } else if self.admitted_after_cutoff {
            Some("each physical admission rechecks the immutable cutoff")
        } else if !self.owner && self.durable.contains(&true) {
            Some("removed ownership cannot leave or recreate physical state")
        } else {
            None
        };
        let mut witnesses = Vec::new();
        if self.durable == [true; 2] && !self.expired {
            witnesses.push("file and acknowledgement complete before cutoff");
        }
        if self.sealed && !self.expired && self.effects[0] == Effect::Admitted {
            witnesses.push("sealing does not revoke an in-transit immutable grant");
        }
        if !self.owner && self.effects.contains(&Effect::Confirmed) {
            witnesses.push("cleanup completes while a confirmation response is delayed");
        }
        if !self.owner && self.effects == [Effect::Persisted, Effect::Rejected] {
            witnesses.push("late acknowledgement rejects after file cleanup");
        }
        if !self.owner && self.effects == [Effect::Persisted; 2] {
            witnesses.push("both physical effect guards drain before cleanup");
        }
        Checks { failure, witnesses }
    }
}

fn run(fault: Fault) -> Exploration<State, Action> {
    explore(
        [State::default()],
        &[
            "file and acknowledgement complete before cutoff",
            "sealing does not revoke an in-transit immutable grant",
            "cleanup completes while a confirmation response is delayed",
            "late acknowledgement rejects after file cleanup",
            "both physical effect guards drain before cleanup",
        ],
        |s| s.successors(fault),
        State::check,
        10_000,
    )
}

fn step(state: State, action: Action) -> State {
    let next = state
        .successors(Fault::None)
        .into_iter()
        .find_map(|(candidate, next)| (candidate == action).then_some(next))
        .unwrap_or_else(|| panic!("disabled {action:?}: {state:?}"));
    assert_eq!(next.check().failure, None, "after {action:?}: {next:?}");
    next
}

#[test]
fn cleanup_confirmation_model_exhausts_lookup_admission_and_cleanup_interleavings() {
    let result = run(Fault::None);
    result.assert_complete();
    println!(
        "{} states, {} transitions",
        result.state_count(),
        result.transitions
    );
}

#[test]
fn cleanup_confirmation_model_detects_each_independent_omission() {
    for (fault, property) in [
        (
            Fault::ConfirmWitnessOnly,
            "confirmation requires usable publication evidence",
        ),
        (
            Fault::SkipFileAdmissionCutoff,
            "each physical admission rechecks the immutable cutoff",
        ),
        (
            Fault::SkipAckAdmissionCutoff,
            "each physical admission rechecks the immutable cutoff",
        ),
        (
            Fault::CleanBeforeCutoff,
            "removed ownership cannot leave or recreate physical state",
        ),
        (
            Fault::IgnoreFileGuard,
            "removed ownership cannot leave or recreate physical state",
        ),
        (
            Fault::IgnoreAckGuard,
            "removed ownership cannot leave or recreate physical state",
        ),
    ] {
        let result = run(fault);
        assert!(
            matches!(result.termination, Termination::Violation { property: actual, .. } if actual == property),
            "{fault:?}: {:?}; trace={:?}",
            result.termination,
            result.counterexample()
        );
        println!("{fault:?}: {:?}", result.counterexample().unwrap());
    }
}

#[test]
fn cleanup_confirmation_model_pins_delayed_positive_reply_at_both_effect_owners() {
    for effect in 0..2 {
        let mut s = State::default();
        for action in [
            Action::ApplyWitness,
            Action::Converge,
            Action::Send(0),
            Action::Confirm(0),
        ] {
            s = step(s, action);
        }
        if effect == 1 {
            for action in [
                Action::Admit(0),
                Action::Persist(0),
                Action::Send(1),
                Action::Confirm(1),
            ] {
                s = step(s, action);
            }
        }
        for action in [Action::Seal, Action::Expire, Action::Cleanup] {
            s = step(s, action);
        }
        assert_eq!(s.effects[effect], Effect::Confirmed);
        assert!(!s
            .successors(Fault::None)
            .iter()
            .any(|(action, _)| *action == Action::Admit(effect)));
        s = step(s, Action::Reject(effect));
        assert!(!s.owner && s.durable == [false; 2]);
    }
}

#[test]
fn cleanup_confirmation_model_pins_each_inflight_guard_through_persistence() {
    for effect in 0..2 {
        let mut s = State::default();
        for action in [
            Action::ApplyWitness,
            Action::Converge,
            Action::Send(0),
            Action::Confirm(0),
            Action::Admit(0),
        ] {
            s = step(s, action);
        }
        if effect == 1 {
            for action in [
                Action::Persist(0),
                Action::Send(1),
                Action::Confirm(1),
                Action::Admit(1),
            ] {
                s = step(s, action);
            }
        }
        s = step(s, Action::Seal);
        s = step(s, Action::Expire);
        assert!(!s
            .successors(Fault::None)
            .iter()
            .any(|(action, _)| *action == Action::Cleanup));
        s = step(s, Action::Persist(effect));
        s = step(s, Action::Cleanup);
        assert!(!s.owner && s.durable == [false; 2]);
    }
}
