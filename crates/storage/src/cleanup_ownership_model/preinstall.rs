// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Abstract pre-install orphan-release protocol, NOT production verification.
//! Two delayed duplicate deliveries, one bucket proof plus a foreign canary,
//! one immutable acceptance cutoff, one object-primary restart and one failed
//! release. Bucket proof validation, object installation and bucket release are
//! separate actions. The primary's serialization spans its final fence check
//! through durable insertion; insertion may finish after the cutoff.
//!
//! A settled authoritative primary and healthy clocks are assumptions. Replica
//! reconciliation, route replacement, proof renewal, physical writes and terminal
//! release are outside this model. MaterializeOwner collapses already-converged
//! creation and slot retirement, covered separately by the introduction model.
//! All replies can be lost: recovery uses durable evidence, never delivery state.

use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    InspectBeforeCutoff,
    InspectOutsideSerialization,
    SkipInstallationFence,
    ForgetPendingOnRestart,
    IgnoreCompactedOwner,
    ReleaseForeignProof,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Delivery {
    InFlight,
    ProofChecked,
    Installing,
    Accepted,
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    deliveries: [Delivery; 2],
    expired: bool,
    proof: bool,
    foreign_proof: bool,
    pending: bool,
    owner: bool,
    log: bool,
    accepted: bool,
    absence: bool,
    discovered_custodian: bool,
    restarted: bool,
    release_failed: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            deliveries: [Delivery::InFlight; 2],
            expired: false,
            proof: true,
            foreign_proof: true,
            pending: false,
            owner: false,
            log: false,
            accepted: false,
            absence: false,
            discovered_custodian: false,
            restarted: false,
            release_failed: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    CheckProof(usize),
    EnterInstallation(usize),
    Reject(usize),
    Commit(usize),
    Expire,
    Inspect,
    MaterializeOwner,
    Compact,
    Restart,
    FailRelease,
    Release,
}

impl State {
    fn locked(&self) -> bool {
        self.deliveries.contains(&Delivery::Installing)
    }

    fn successors(&self, fault: Fault) -> Vec<(Action, Self)> {
        let mut out = Vec::new();
        let mut add = |action, change: &dyn Fn(&mut Self)| {
            let mut next = self.clone();
            change(&mut next);
            out.push((action, next));
        };
        for (i, delivery) in self.deliveries.iter().enumerate() {
            match delivery {
                Delivery::InFlight if self.proof && !self.expired => {
                    add(Action::CheckProof(i), &|s| {
                        s.deliveries[i] = Delivery::ProofChecked;
                    });
                }
                Delivery::InFlight => add(Action::Reject(i), &|s| {
                    s.deliveries[i] = Delivery::Rejected;
                }),
                Delivery::ProofChecked if !self.locked() => {
                    if !self.expired || fault == Fault::SkipInstallationFence {
                        add(Action::EnterInstallation(i), &|s| {
                            s.deliveries[i] = Delivery::Installing;
                        });
                    } else {
                        add(Action::Reject(i), &|s| {
                            s.deliveries[i] = Delivery::Rejected;
                        });
                    }
                }
                Delivery::Installing => add(Action::Commit(i), &|s| {
                    // Exact duplicates do not allocate a second owner. A fence
                    // checked before insertion does not bound commit latency;
                    // the shared lock is essential even after time has passed.
                    if !s.owner {
                        s.pending = true;
                        s.log = true;
                    }
                    s.accepted = true;
                    s.deliveries[i] = Delivery::Accepted;
                }),
                _ => {}
            }
        }
        if !self.expired {
            add(Action::Expire, &|s| s.expired = true);
        }
        if (self.expired || fault == Fault::InspectBeforeCutoff)
            && (!self.locked() || fault == Fault::InspectOutsideSerialization)
            && !self.absence
            && !self.discovered_custodian
        {
            add(Action::Inspect, &|s| {
                if s.pending || (s.owner && !(fault == Fault::IgnoreCompactedOwner && !s.log)) {
                    s.discovered_custodian = true;
                } else {
                    s.absence = true;
                }
            });
        }
        if self.pending && !self.locked() {
            add(Action::MaterializeOwner, &|s| {
                s.owner = true;
                s.pending = false;
            });
        }
        if self.owner && self.log && !self.locked() {
            add(Action::Compact, &|s| s.log = false);
        }
        if !self.restarted {
            add(Action::Restart, &|s| {
                s.restarted = true;
                s.absence = false;
                s.discovered_custodian = false;
                // A local uncommitted installation is lost. Delayed requests
                // and their earlier remote proof check can outlive this process.
                for delivery in &mut s.deliveries {
                    if *delivery == Delivery::Installing {
                        *delivery = Delivery::Rejected;
                    }
                }
                if fault == Fault::ForgetPendingOnRestart {
                    s.pending = false;
                }
            });
        }
        if self.absence && self.proof {
            if !self.release_failed {
                add(Action::FailRelease, &|s| s.release_failed = true);
            }
            add(Action::Release, &|s| {
                if fault == Fault::ReleaseForeignProof {
                    s.foreign_proof = false;
                } else {
                    s.proof = false;
                }
            });
        }
        out
    }

    fn check(&self) -> Checks {
        let failure = if !self.foreign_proof {
            Some("orphan release preserves unrelated reservation")
        } else if self.accepted && !self.pending && !self.owner {
            Some("accepted introduction always has a durable custodian")
        } else if !self.proof && (self.pending || self.owner) {
            Some("orphan release never consumes an accepted introduction's proof")
        } else {
            None
        };
        let mut witnesses = Vec::new();
        if !self.proof && self.deliveries == [Delivery::Rejected; 2] {
            witnesses.push("both delayed deliveries reject after orphan release");
        }
        if self.expired && self.locked() && !self.absence && self.proof {
            witnesses.push("cutoff can pass while installation still owns serialization");
        }
        if self.expired && self.pending && self.discovered_custodian && self.proof {
            witnesses.push("inspection hands accepted introduction to pending recovery");
        }
        if self.restarted && self.owner && !self.log && self.discovered_custodian && self.proof {
            witnesses.push("compacted owner remains discoverable after restart");
        }
        if self.restarted && self.release_failed && !self.proof && !self.accepted {
            witnesses.push("orphan release can retry across restart");
        }
        Checks { failure, witnesses }
    }
}

fn run(fault: Fault) -> Exploration<State, Action> {
    explore(
        [State::default()],
        &[
            "both delayed deliveries reject after orphan release",
            "cutoff can pass while installation still owns serialization",
            "inspection hands accepted introduction to pending recovery",
            "compacted owner remains discoverable after restart",
            "orphan release can retry across restart",
        ],
        |s| s.successors(fault),
        State::check,
        100_000,
    )
}

#[test]
fn cleanup_preinstall_model_exhausts_cutoff_inspection_and_release() {
    let result = run(Fault::None);
    result.assert_complete();
    println!(
        "{} states, {} transitions",
        result.state_count(),
        result.transitions
    );
}

#[test]
fn cleanup_preinstall_model_detects_independent_handoff_faults() {
    for (fault, expected) in [
        (
            Fault::InspectBeforeCutoff,
            "orphan release never consumes an accepted introduction's proof",
        ),
        (
            Fault::InspectOutsideSerialization,
            "orphan release never consumes an accepted introduction's proof",
        ),
        (
            Fault::SkipInstallationFence,
            "orphan release never consumes an accepted introduction's proof",
        ),
        (
            Fault::ForgetPendingOnRestart,
            "accepted introduction always has a durable custodian",
        ),
        (
            Fault::IgnoreCompactedOwner,
            "orphan release never consumes an accepted introduction's proof",
        ),
        (
            Fault::ReleaseForeignProof,
            "orphan release preserves unrelated reservation",
        ),
    ] {
        let result = run(fault);
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
fn cleanup_preinstall_model_expiry_waits_for_inflight_insertion() {
    let mut state = State::default();
    for action in [
        Action::CheckProof(0),
        Action::CheckProof(1),
        Action::EnterInstallation(0),
        Action::Expire,
        Action::Commit(0),
        Action::Inspect,
        Action::Reject(1),
    ] {
        state = state
            .successors(Fault::None)
            .into_iter()
            .find_map(|(candidate, next)| (candidate == action).then_some(next))
            .unwrap_or_else(|| panic!("disabled {action:?} in {state:?}"));
        assert_eq!(state.check().failure, None);
        if action == Action::Expire {
            assert!(!state
                .successors(Fault::None)
                .iter()
                .any(|(a, _)| *a == Action::Inspect));
        }
    }
    assert!(state.pending && state.proof && state.discovered_custodian);
    assert!(!state.absence);
    assert!(!state
        .successors(Fault::None)
        .iter()
        .any(|(a, _)| *a == Action::Release));
}

#[test]
fn cleanup_preinstall_model_failed_release_reinspects_after_restart() {
    let mut state = State::default();
    for action in [
        Action::CheckProof(0),
        Action::CheckProof(1),
        Action::Expire,
        Action::Inspect,
        Action::FailRelease,
        Action::Restart,
        Action::Inspect,
        Action::Release,
        Action::Reject(0),
        Action::Reject(1),
    ] {
        state = state
            .successors(Fault::None)
            .into_iter()
            .find_map(|(candidate, next)| (candidate == action).then_some(next))
            .unwrap_or_else(|| panic!("disabled {action:?} in {state:?}"));
        assert_eq!(state.check().failure, None);
        if action == Action::Restart {
            assert!(state.release_failed && state.proof && state.expired);
            assert!(!state.absence);
            assert!(!state
                .successors(Fault::None)
                .iter()
                .any(|(a, _)| *a == Action::Release));
        }
        if action == Action::Release {
            // Both deliveries retain their old bucket-PG validation. Neither
            // may turn that stale observation into a fresh object-PG install.
            assert_eq!(state.deliveries, [Delivery::ProofChecked; 2]);
            assert!(!state
                .successors(Fault::None)
                .iter()
                .any(|(a, _)| matches!(a, Action::EnterInstallation(_))));
        }
    }
    assert!(state.foreign_proof);
    assert!(!state.proof && !state.accepted && !state.pending && !state.owner);
    assert_eq!(state.deliveries, [Delivery::Rejected; 2]);
}
