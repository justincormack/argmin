// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Independent allocation-lifetime candidate. Two retry slots, one immutable
//! window, original node A, replacement B and one return/import or local restart
//! of A. Counters are deliberately not authority. The original issuer may finish
//! its bounded delegation after replacement; a new lifetime cannot recreate it.
//! No production node/route certificate, clock implementation or RPC is modelled.

use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    NodeIdentityOnly,
    ResumeReplicaCursor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Runtime {
    OriginalA,
    ReplacementB,
    ReturnedA,
    RestartedA,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    runtime: Runtime,
    original_alive: bool,
    original_cursor: u8,
    replacement_cursor: u8,
    issued: u8,
    delivered: u8,
    expired: bool,
    foreign_allocation: bool,
    duplicate: bool,
    late_original_allocation: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            runtime: Runtime::OriginalA,
            original_alive: true,
            original_cursor: 0,
            replacement_cursor: 0,
            issued: 0,
            delivered: 0,
            expired: false,
            foreign_allocation: false,
            duplicate: false,
            late_original_allocation: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    AllocateOriginal,
    AllocateReplacement,
    Deliver(u8),
    Replace,
    ReturnWithReplicaState,
    RestartWithLocalState,
    Expire,
}

impl State {
    fn issue(&mut self, slot: u8) {
        self.duplicate |= self.issued & (1 << slot) != 0;
        self.issued |= 1 << slot;
    }

    fn successors(&self, fault: Fault) -> Vec<(Action, Self)> {
        let mut out = Vec::new();
        let mut add = |action, change: &dyn Fn(&mut Self)| {
            let mut next = self.clone();
            change(&mut next);
            out.push((action, next));
        };
        if !self.expired {
            if self.original_alive && self.original_cursor < 2 {
                add(Action::AllocateOriginal, &|s| {
                    s.issue(s.original_cursor);
                    s.original_cursor += 1;
                    s.late_original_allocation |= s.runtime != Runtime::OriginalA;
                });
            }
            let permitted_by_bug = fault == Fault::ResumeReplicaCursor
                || (fault == Fault::NodeIdentityOnly
                    && matches!(self.runtime, Runtime::ReturnedA | Runtime::RestartedA));
            if self.runtime != Runtime::OriginalA && self.replacement_cursor < 2 && permitted_by_bug
            {
                add(Action::AllocateReplacement, &|s| {
                    s.issue(s.replacement_cursor);
                    s.replacement_cursor += 1;
                    s.foreign_allocation = true;
                });
            }
            // Existing exact prepared grants remain deliverable even when their
            // allocator has stopped. Delivery neither allocates nor extends time.
            for slot in 0..2 {
                if self.issued & (1 << slot) != 0 && self.delivered & (1 << slot) == 0 {
                    add(Action::Deliver(slot), &|s| s.delivered |= 1 << slot);
                }
            }
            add(Action::Expire, &|s| s.expired = true);
        }
        match self.runtime {
            Runtime::OriginalA => {
                add(Action::Replace, &|s| s.runtime = Runtime::ReplacementB);
                add(Action::RestartWithLocalState, &|s| {
                    s.runtime = Runtime::RestartedA;
                    s.original_alive = false;
                    s.replacement_cursor = s.original_cursor;
                });
            }
            Runtime::ReplacementB => {
                add(Action::ReturnWithReplicaState, &|s| {
                    s.runtime = Runtime::ReturnedA;
                    s.original_alive = false;
                    // A certified import may have an older consumed floor. It
                    // retains the full reserved window, not allocation history.
                    s.replacement_cursor = 0;
                });
            }
            Runtime::ReturnedA | Runtime::RestartedA => {}
        }
        out
    }

    fn check(&self) -> Checks {
        let failure = if self.foreign_allocation {
            Some("a new allocation lifetime cannot adopt an old window")
        } else if self.duplicate {
            Some("each physical identity is allocated at most once")
        } else {
            None
        };
        let mut witnesses = Vec::new();
        if self.runtime == Runtime::RestartedA
            && self.replacement_cursor == 1
            && self.delivered == 1
        {
            witnesses.push("local restart preserves its floor and delivers a prior grant");
        }
        if self.runtime == Runtime::ReturnedA && self.issued == 3 && self.delivered == 3 {
            witnesses.push("returned node retains old deliveries without using replica floor");
        }
        if self.late_original_allocation && self.issued == 3 && self.expired {
            witnesses.push("old issuer exhausts only its original finite window");
        }
        Checks { failure, witnesses }
    }
}

fn run(fault: Fault) -> Exploration<State, Action> {
    explore(
        [State::default()],
        &[
            "local restart preserves its floor and delivers a prior grant",
            "returned node retains old deliveries without using replica floor",
            "old issuer exhausts only its original finite window",
        ],
        |s| s.successors(fault),
        State::check,
        10_000,
    )
}

#[test]
fn cleanup_issuer_model_exhausts_restart_return_and_delayed_delivery() {
    let result = run(Fault::None);
    result.assert_complete();
    for state in result.states().filter(|s| s.runtime != Runtime::OriginalA) {
        assert!(!state
            .successors(Fault::None)
            .iter()
            .any(|(a, _)| *a == Action::AllocateReplacement));
    }
    println!(
        "{} states, {} transitions",
        result.state_count(),
        result.transitions
    );
}

#[test]
fn cleanup_issuer_model_detects_node_identity_and_replica_floor_substitution() {
    for fault in [Fault::NodeIdentityOnly, Fault::ResumeReplicaCursor] {
        let result = run(fault);
        assert!(
            matches!(
                result.termination,
                Termination::Violation {
                    property: "a new allocation lifetime cannot adopt an old window",
                    ..
                }
            ),
            "{fault:?}: {:?}; trace={:?}",
            result.termination,
            result.counterexample()
        );
        println!("{fault:?}: {:?}", result.counterexample().unwrap());
    }
    // Also pin actual identity reuse, not just the stronger lifetime violation.
    let mut state = State::default();
    for action in [
        Action::AllocateOriginal,
        Action::Replace,
        Action::ReturnWithReplicaState,
        Action::AllocateReplacement,
    ] {
        state = state
            .successors(Fault::NodeIdentityOnly)
            .into_iter()
            .find_map(|(candidate, next)| (candidate == action).then_some(next))
            .unwrap();
    }
    assert!(state.duplicate && state.foreign_allocation);
}
