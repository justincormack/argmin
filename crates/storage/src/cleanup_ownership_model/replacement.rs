// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Abstract successor-primary orphan inspection, NOT production verification.
//! One introduction, old primary/witness/successor, one cutoff, one successor
//! restart and one unavailable inspection. Old insertion holds serialization
//! from its final deadline check through commit, which may finish after expiry.
//! Replacement fencing must drain that insertion before certifying the prefix.
//! Reconciliation below stands for authenticated, settled-prefix recovery; it
//! does not implement route certificates, distributed fencing or a quorum rule.
//! Physical windows, terminal publication and clock faults are separate models.

use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    FenceWithoutDraining,
    TrustSuccessorLocalAbsence,
    DropCompactedOwner,
    UnavailableMeansAbsent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Request {
    Queued,
    Installing,
    Accepted,
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Inspection {
    Unsettled,
    Custodian,
    ClosedAbsent,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    request: Request,
    expired: bool,
    replacing: bool,
    old_fenced: bool,
    pending: bool,
    owners: [bool; 3],
    compacted: bool,
    inspection: Inspection,
    proof: bool,
    restarted: bool,
    inspection_failed: bool,
    lost_owner: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            request: Request::Queued,
            expired: false,
            replacing: false,
            old_fenced: false,
            pending: false,
            owners: [false; 3],
            compacted: false,
            inspection: Inspection::Unsettled,
            proof: true,
            restarted: false,
            inspection_failed: false,
            lost_owner: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    EnterInsertion,
    CommitInsertion,
    Reject,
    ApplyWitness,
    Converge,
    Compact,
    Expire,
    BeginReplacement,
    FenceOld,
    InspectionUnavailable,
    Reconcile,
    RestartSuccessor,
    ReleaseOrphan,
}

impl State {
    fn successors(&self, fault: Fault) -> Vec<(Action, Self)> {
        let mut out = Vec::new();
        let mut add = |action, change: &dyn Fn(&mut Self)| {
            let mut next = self.clone();
            change(&mut next);
            out.push((action, next));
        };
        match self.request {
            Request::Queued if !self.expired && !self.old_fenced => {
                add(Action::EnterInsertion, &|s| s.request = Request::Installing);
            }
            Request::Queued => add(Action::Reject, &|s| s.request = Request::Rejected),
            Request::Installing => add(Action::CommitInsertion, &|s| {
                s.pending = true;
                s.request = Request::Accepted;
            }),
            Request::Accepted | Request::Rejected => {}
        }
        if self.pending && !self.owners[1] {
            add(Action::ApplyWitness, &|s| s.owners[1] = true);
        }
        if self.pending && self.owners[1] {
            // Exact introduction recovery commits owner/proof custody everywhere
            // before retiring its pending slot. Replies need not be delivered.
            add(Action::Converge, &|s| {
                s.owners = [true; 3];
                s.pending = false;
            });
        }
        if self.owners == [true; 3] && !self.pending && !self.compacted {
            add(Action::Compact, &|s| s.compacted = true);
        }
        if !self.expired {
            add(Action::Expire, &|s| s.expired = true);
        }
        if !self.replacing {
            add(Action::BeginReplacement, &|s| s.replacing = true);
        }
        if self.replacing
            && !self.old_fenced
            && (self.request != Request::Installing || fault == Fault::FenceWithoutDraining)
        {
            add(Action::FenceOld, &|s| s.old_fenced = true);
        }
        if self.replacing && self.inspection == Inspection::Unsettled {
            if !self.inspection_failed {
                add(Action::InspectionUnavailable, &|s| {
                    s.inspection_failed = true;
                    if fault == Fault::UnavailableMeansAbsent && s.expired {
                        s.inspection = Inspection::ClosedAbsent;
                    }
                });
            }
            if self.old_fenced && self.expired {
                add(Action::Reconcile, &|s| {
                    if fault == Fault::DropCompactedOwner && s.compacted {
                        // Counterexample for interpreting an empty compacted log
                        // as the complete state transferred to the successor.
                        s.lost_owner = s.owners[2];
                        s.owners[2] = false;
                    }
                    let present = match fault {
                        Fault::TrustSuccessorLocalAbsence => s.owners[2],
                        _ => s.pending || s.owners.iter().any(|owner| *owner),
                    };
                    s.inspection = if present {
                        Inspection::Custodian
                    } else {
                        Inspection::ClosedAbsent
                    };
                });
            }
        }
        if self.replacing && !self.restarted {
            add(Action::RestartSuccessor, &|s| {
                s.restarted = true;
                s.inspection = Inspection::Unsettled;
            });
        }
        if self.inspection == Inspection::ClosedAbsent && self.proof {
            // Separate bucket-PG transaction, after releasing object-PG locks.
            add(Action::ReleaseOrphan, &|s| s.proof = false);
        }
        out
    }

    fn check(&self) -> Checks {
        let mut checks = Checks::default();
        if self.lost_owner {
            checks.failure = Some("compaction and replacement preserve materialized custody");
        } else if !self.proof && self.request == Request::Accepted {
            checks.failure = Some("accepted introduction retains its bucket proof");
        } else if self.inspection == Inspection::ClosedAbsent
            && (!self.old_fenced || self.request == Request::Installing)
        {
            checks.failure = Some("absence requires drained old-primary installation authority");
        }
        if self.replacing && self.expired && self.request == Request::Installing {
            checks
                .witnesses
                .push("expired insertion overlaps replacement");
        }
        if self.inspection == Inspection::Custodian && self.pending && !self.owners[2] {
            checks
                .witnesses
                .push("empty successor preserves remote pending custody");
            if self.owners[1] {
                checks
                    .witnesses
                    .push("witness-ahead custody survives replacement");
            }
        }
        if self.compacted && self.restarted && self.inspection == Inspection::Custodian {
            checks
                .witnesses
                .push("compacted owner survives successor restart");
        }
        if !self.proof
            && self.request == Request::Rejected
            && self.inspection_failed
            && self.restarted
        {
            checks
                .witnesses
                .push("unavailable inspection retries to orphan release");
        }
        checks
    }
}

fn run(fault: Fault) -> Exploration<State, Action> {
    explore(
        [State::default()],
        &[
            "expired insertion overlaps replacement",
            "empty successor preserves remote pending custody",
            "witness-ahead custody survives replacement",
            "compacted owner survives successor restart",
            "unavailable inspection retries to orphan release",
        ],
        |state| state.successors(fault),
        State::check,
        50_000,
    )
}

#[test]
fn cleanup_replacement_model_requires_settled_prefix_before_orphan_release() {
    let result = run(Fault::None);
    result.assert_complete();
    println!(
        "states={}, transitions={}",
        result.state_count(),
        result.transitions
    );
}

#[test]
fn cleanup_replacement_model_detects_missing_handoff_obligations() {
    for (fault, expected) in [
        (
            Fault::FenceWithoutDraining,
            "absence requires drained old-primary installation authority",
        ),
        (
            Fault::TrustSuccessorLocalAbsence,
            "accepted introduction retains its bucket proof",
        ),
        (
            Fault::DropCompactedOwner,
            "compaction and replacement preserve materialized custody",
        ),
        (
            Fault::UnavailableMeansAbsent,
            "absence requires drained old-primary installation authority",
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
fn cleanup_replacement_model_waits_for_old_commit_then_inspects_witness() {
    let mut state = State::default();
    for action in [
        Action::EnterInsertion,
        Action::BeginReplacement,
        Action::Expire,
        Action::InspectionUnavailable,
        Action::CommitInsertion,
        Action::ApplyWitness,
        Action::FenceOld,
        Action::Reconcile,
        Action::Converge,
        Action::Compact,
        Action::RestartSuccessor,
        Action::Reconcile,
    ] {
        state = state
            .successors(Fault::None)
            .into_iter()
            .find_map(|(candidate, next)| (candidate == action).then_some(next))
            .unwrap_or_else(|| panic!("disabled {action:?} in {state:?}"));
        assert_eq!(state.check().failure, None);
        if action == Action::Expire {
            assert!(!state.successors(Fault::None).iter().any(|(a, _)| {
                matches!(
                    a,
                    Action::FenceOld | Action::Reconcile | Action::ReleaseOrphan
                )
            }));
        }
        if action == Action::ApplyWitness {
            assert!(state.pending);
            assert_eq!(state.owners, [false, true, false]);
        }
        if action == Action::RestartSuccessor {
            assert_eq!(state.inspection, Inspection::Unsettled);
            assert_eq!(state.owners, [true; 3]);
        }
    }
    assert!(state.proof && state.compacted && state.restarted);
    assert_eq!(state.inspection, Inspection::Custodian);
    assert!(!state
        .successors(Fault::None)
        .iter()
        .any(|(a, _)| *a == Action::ReleaseOrphan));
}

#[test]
fn cleanup_replacement_model_unavailable_inspection_does_not_release_orphan() {
    let mut state = State::default();
    for action in [
        Action::BeginReplacement,
        Action::Expire,
        Action::InspectionUnavailable,
        Action::FenceOld,
        Action::RestartSuccessor,
        Action::Reconcile,
        Action::ReleaseOrphan,
        Action::Reject,
    ] {
        state = state
            .successors(Fault::None)
            .into_iter()
            .find_map(|(candidate, next)| (candidate == action).then_some(next))
            .unwrap_or_else(|| panic!("disabled {action:?} in {state:?}"));
        assert_eq!(state.check().failure, None);
        if matches!(
            action,
            Action::InspectionUnavailable | Action::RestartSuccessor
        ) {
            assert!(state.proof);
            assert_eq!(state.inspection, Inspection::Unsettled);
            assert!(!state
                .successors(Fault::None)
                .iter()
                .any(|(a, _)| *a == Action::ReleaseOrphan));
        }
        if action == Action::ReleaseOrphan {
            assert_eq!(state.request, Request::Queued);
            assert!(!state
                .successors(Fault::None)
                .iter()
                .any(|(a, _)| *a == Action::EnterInsertion));
        }
    }
    assert!(!state.proof && !state.pending && state.owners == [false; 3]);
    assert_eq!(state.request, Request::Rejected);
}
