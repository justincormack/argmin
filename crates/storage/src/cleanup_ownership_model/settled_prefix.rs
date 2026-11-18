// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Settled-prefix prerequisite for confirmation.rs, not a production verifier.
//! One immutable requested window, a primary and optional designated witness,
//! one lookup, and at most one concurrent disturbance. Prefixes stand for
//! independently validated local state; this does not model hashes, SQL cache
//! validity, authenticated transport, transfer certification or physical IO.
//! An imported prefix is assumed certified before active-route admission.
//! Primary serialization spans witness inspection and descriptor capture. A
//! witness reply describes its protected observation, not its state forever.

use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    IgnoreEpoch,
    IgnoreIndex,
    IgnoreLogHash,
    IgnoreStateDigest,
    IgnoreWitnessIdentity,
    IgnorePending,
    IgnoreTail,
    IgnoreWindow,
    IgnoreRoute,
    IgnoreCutoff,
    ReleasePrimaryEarly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Prefix {
    epoch: u8,
    index: u8,
    log_hash: u8,
    state_digest: u8,
}

impl Prefix {
    const SETTLED: Self = Self {
        epoch: 1,
        index: 1,
        log_hash: 1,
        state_digest: 1,
    };

    fn matches(self, other: Self, fault: Fault) -> bool {
        (self.epoch == other.epoch || fault == Fault::IgnoreEpoch)
            && (self.index == other.index || fault == Fault::IgnoreIndex)
            && (self.log_hash == other.log_hash || fault == Fault::IgnoreLogHash)
            && (self.state_digest == other.state_digest || fault == Fault::IgnoreStateDigest)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Phase {
    Idle,
    Inspecting,
    Confirmed,
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Observation {
    prefix: Prefix,
    window: u8,
    pending: bool,
    tail: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    primary: Observation,
    witness: Option<Prefix>,
    designated_witness: bool,
    active_route: bool,
    expired: bool,
    imported: bool,
    phase: Phase,
    locked: bool,
    captured: Option<Observation>,
    reply: Option<Prefix>,
    disturbed: bool,
    violation: Option<&'static str>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            primary: Observation {
                prefix: Prefix::SETTLED,
                window: 0,
                pending: false,
                tail: false,
            },
            witness: Some(Prefix::SETTLED),
            designated_witness: true,
            active_route: true,
            expired: false,
            imported: false,
            phase: Phase::Idle,
            locked: false,
            captured: None,
            reply: None,
            disturbed: false,
            violation: None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Begin,
    InspectWitness,
    AdvancePrimary,
    ReplaceWindow,
    InstallPending,
    AppendTail,
    WithdrawRoute,
    Expire,
    Confirm,
    Reject,
}

const SERIALIZATION: &str = "primary observation stays serialized through confirmation";
const ROUTE: &str = "confirmation requires current active-primary authority";
const CUTOFF: &str = "confirmation cannot outlive its captured cutoff";
const WINDOW: &str = "confirmation binds the exact requested window";
const PENDING: &str = "confirmation excludes every unresolved pending command";
const TAIL: &str = "confirmation excludes unapplied log tails";
const WITNESS: &str = "confirmation uses the route-designated witness";
const EPOCH: &str = "primary and witness epochs match";
const INDEX: &str = "primary and witness applied indexes match";
const LOG_HASH: &str = "primary and witness log hashes match";
const STATE_DIGEST: &str = "primary and witness state digests match";

impl State {
    fn successors(&self, fault: Fault) -> Vec<(Action, Self)> {
        if matches!(self.phase, Phase::Confirmed | Phase::Rejected) {
            return Vec::new();
        }
        let mut out = Vec::new();
        let mut add = |action, change: &dyn Fn(&mut Self)| {
            let mut next = self.clone();
            change(&mut next);
            out.push((action, next));
        };
        match self.phase {
            Phase::Idle => add(Action::Begin, &|s| {
                s.phase = Phase::Inspecting;
                s.locked = true;
                s.captured = Some(s.primary);
            }),
            Phase::Inspecting => {
                if self.witness.is_some() && self.reply.is_none() {
                    add(Action::InspectWitness, &|s| {
                        s.reply = s.witness;
                        if fault == Fault::ReleasePrimaryEarly {
                            s.locked = false;
                        }
                    });
                } else if self.can_confirm(fault) {
                    add(Action::Confirm, &|s| {
                        // Record the obligation AT issuance. Later state changes
                        // cannot retroactively revoke an immutable finite grant.
                        s.violation = s.issuance_violation();
                        s.phase = Phase::Confirmed;
                        s.locked = false;
                    });
                } else {
                    add(Action::Reject, &|s| {
                        s.phase = Phase::Rejected;
                        s.locked = false;
                    });
                }
            }
            Phase::Confirmed | Phase::Rejected => unreachable!(),
        }
        if !self.disturbed {
            if !self.locked {
                // A different command changes the prefix while leaving the
                // requested owner/window completely unchanged.
                add(Action::AdvancePrimary, &|s| {
                    s.primary.prefix.index += 1;
                    s.primary.prefix.log_hash += 1;
                    s.primary.prefix.state_digest += 1;
                    s.disturbed = true;
                });
                add(Action::ReplaceWindow, &|s| {
                    s.primary.window = 1;
                    s.primary.prefix.index += 1;
                    s.primary.prefix.log_hash += 1;
                    s.primary.prefix.state_digest += 1;
                    s.disturbed = true;
                });
                add(Action::InstallPending, &|s| {
                    s.primary.pending = true;
                    s.disturbed = true;
                });
                add(Action::AppendTail, &|s| {
                    s.primary.tail = true;
                    s.disturbed = true;
                });
            }
            // Conservative admission invalidation/expiry may occur even while
            // metadata serialization is held; the final check remains required.
            add(Action::WithdrawRoute, &|s| {
                s.active_route = false;
                s.disturbed = true;
            });
            add(Action::Expire, &|s| {
                s.expired = true;
                s.disturbed = true;
            });
        }
        out
    }

    fn can_confirm(&self, fault: Fault) -> bool {
        let captured = self.captured.unwrap();
        (self.active_route || fault == Fault::IgnoreRoute)
            && (!self.expired || fault == Fault::IgnoreCutoff)
            && (captured.window == 0 || fault == Fault::IgnoreWindow)
            && (!captured.pending || fault == Fault::IgnorePending)
            && (!captured.tail || fault == Fault::IgnoreTail)
            && match self.witness {
                None => true, // Existing singleton publication semantics.
                Some(_) => {
                    (self.designated_witness || fault == Fault::IgnoreWitnessIdentity)
                        && self
                            .reply
                            .is_some_and(|p| captured.prefix.matches(p, fault))
                }
            }
    }

    fn issuance_violation(&self) -> Option<&'static str> {
        if self.captured != Some(self.primary) {
            Some(SERIALIZATION)
        } else if !self.active_route {
            Some(ROUTE)
        } else if self.expired {
            Some(CUTOFF)
        } else if self.primary.window != 0 {
            Some(WINDOW)
        } else if self.primary.pending {
            Some(PENDING)
        } else if self.primary.tail {
            Some(TAIL)
        } else if self.witness.is_some() {
            let reply = self
                .reply
                .expect("replicated confirmation requires a reply");
            if !self.designated_witness {
                Some(WITNESS)
            } else if self.primary.prefix.epoch != reply.epoch {
                Some(EPOCH)
            } else if self.primary.prefix.index != reply.index {
                Some(INDEX)
            } else if self.primary.prefix.log_hash != reply.log_hash {
                Some(LOG_HASH)
            } else if self.primary.prefix.state_digest != reply.state_digest {
                Some(STATE_DIGEST)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn check(&self) -> Checks {
        let mut witnesses = Vec::new();
        if self.phase == Phase::Confirmed {
            witnesses.push(if self.imported {
                "certified imported prefix confirms without original command receipts"
            } else if self.witness.is_none() {
                "singleton confirms without inventing a witness"
            } else {
                "matching replicated prefix confirms"
            });
        }
        if self.phase == Phase::Rejected {
            if self.primary.window == 0
                && self.reply.is_some_and(|reply| reply != self.primary.prefix)
            {
                witnesses.push("identical window rows do not hide divergent prefixes");
            }
            if self.primary.pending {
                witnesses.push("even an unrelated pending command blocks confirmation");
            }
            if self.primary.tail {
                witnesses.push("an empty pending slot does not hide an unapplied tail");
            }
            if self.reply.is_some() && self.expired && self.disturbed {
                witnesses.push("expiry after witness inspection rejects confirmation");
            }
            if self.reply.is_some() && !self.active_route && self.disturbed {
                witnesses.push("route withdrawal after witness inspection rejects confirmation");
            }
        }
        Checks {
            failure: self.violation,
            witnesses,
        }
    }
}

fn initial_states() -> Vec<State> {
    let base = State::default();
    let mut states = vec![base.clone()];
    let mut singleton = base.clone();
    singleton.witness = None;
    states.push(singleton);
    let mut imported = base.clone();
    imported.imported = true;
    imported.primary.prefix = Prefix {
        epoch: 2,
        index: 0,
        log_hash: 0,
        state_digest: 1,
    };
    imported.witness = Some(imported.primary.prefix);
    states.push(imported);
    for fault in [
        Fault::IgnoreEpoch,
        Fault::IgnoreIndex,
        Fault::IgnoreLogHash,
        Fault::IgnoreStateDigest,
        Fault::IgnoreWitnessIdentity,
        Fault::IgnorePending,
        Fault::IgnoreTail,
        Fault::IgnoreWindow,
        Fault::IgnoreRoute,
        Fault::IgnoreCutoff,
    ] {
        let mut s = base.clone();
        match fault {
            Fault::IgnoreEpoch => s.witness.as_mut().unwrap().epoch += 1,
            Fault::IgnoreIndex => s.witness.as_mut().unwrap().index += 1,
            Fault::IgnoreLogHash => s.witness.as_mut().unwrap().log_hash += 1,
            Fault::IgnoreStateDigest => s.witness.as_mut().unwrap().state_digest += 1,
            Fault::IgnoreWitnessIdentity => s.designated_witness = false,
            Fault::IgnorePending => s.primary.pending = true,
            Fault::IgnoreTail => s.primary.tail = true,
            Fault::IgnoreWindow => s.primary.window = 1,
            Fault::IgnoreRoute => s.active_route = false,
            Fault::IgnoreCutoff => s.expired = true,
            Fault::None | Fault::ReleasePrimaryEarly => unreachable!(),
        }
        states.push(s);
    }
    states
}

fn run(fault: Fault) -> Exploration<State, Action> {
    explore(
        initial_states(),
        &[
            "matching replicated prefix confirms",
            "singleton confirms without inventing a witness",
            "certified imported prefix confirms without original command receipts",
            "identical window rows do not hide divergent prefixes",
            "even an unrelated pending command blocks confirmation",
            "an empty pending slot does not hide an unapplied tail",
            "expiry after witness inspection rejects confirmation",
            "route withdrawal after witness inspection rejects confirmation",
        ],
        |s| s.successors(fault),
        State::check,
        10_000,
    )
}

fn step(s: State, action: Action, fault: Fault) -> State {
    s.successors(fault)
        .into_iter()
        .find_map(|(candidate, next)| (candidate == action).then_some(next))
        .unwrap_or_else(|| panic!("disabled {action:?}: {s:?}"))
}

#[test]
fn cleanup_settled_prefix_model_exhausts_confirmation_interleavings() {
    let result = run(Fault::None);
    result.assert_complete();
    println!(
        "{} states, {} transitions",
        result.state_count(),
        result.transitions
    );
}

#[test]
fn cleanup_settled_prefix_model_detects_each_independent_omission() {
    for (fault, expected) in [
        (Fault::IgnoreEpoch, EPOCH),
        (Fault::IgnoreIndex, INDEX),
        (Fault::IgnoreLogHash, LOG_HASH),
        (Fault::IgnoreStateDigest, STATE_DIGEST),
        (Fault::IgnoreWitnessIdentity, WITNESS),
        (Fault::IgnorePending, PENDING),
        (Fault::IgnoreTail, TAIL),
        (Fault::IgnoreWindow, WINDOW),
        (Fault::IgnoreRoute, ROUTE),
        (Fault::IgnoreCutoff, CUTOFF),
        (Fault::ReleasePrimaryEarly, SERIALIZATION),
    ] {
        let result = run(fault);
        assert!(
            matches!(result.termination, Termination::Violation { property, .. } if property == expected),
            "{fault:?}: {:?}; trace={:?}",
            result.termination,
            result.counterexample()
        );
        println!("{fault:?}: {:?}", result.counterexample().unwrap());
    }
}

#[test]
fn cleanup_settled_prefix_model_holds_primary_through_witness_and_capture() {
    for fault in [Fault::None, Fault::ReleasePrimaryEarly] {
        let s = step(State::default(), Action::Begin, fault);
        let s = step(s, Action::InspectWitness, fault);
        let metadata_actions = [
            Action::AdvancePrimary,
            Action::ReplaceWindow,
            Action::InstallPending,
            Action::AppendTail,
        ];
        if fault == Fault::None {
            for action in metadata_actions {
                assert!(!s.successors(fault).iter().any(|(a, _)| *a == action));
            }
            assert_eq!(step(s, Action::Confirm, fault).check().failure, None);
        } else {
            // In particular, unchanged owner/window rows cannot rescue a reply
            // captured before an unrelated command advanced the primary prefix.
            let s = step(s, Action::AdvancePrimary, fault);
            assert_eq!(s.primary.window, 0);
            assert_eq!(
                step(s, Action::Confirm, fault).check().failure,
                Some(SERIALIZATION)
            );
        }
    }
}

#[test]
fn cleanup_settled_prefix_model_rechecks_authority_after_witness_reply() {
    for action in [Action::Expire, Action::WithdrawRoute] {
        let s = step(State::default(), Action::Begin, Fault::None);
        let s = step(s, Action::InspectWitness, Fault::None);
        let s = step(s, action, Fault::None);
        assert!(!s
            .successors(Fault::None)
            .iter()
            .any(|(a, _)| *a == Action::Confirm));
        assert_eq!(step(s, Action::Reject, Fault::None).check().failure, None);
    }
}
