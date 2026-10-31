// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Independent abstract model for creation of the retained owner and its exact
//! bucket-proof obligation. NOT production-connected verification. One introducing
//! command, three object-PG replicas, two distinct bucket proofs, one restart and
//! one failed release. Physical writes/cleanup, clock expiry, route authorization
//! and terminal fanout are deliberately outside this model.
//!
//! Pending install and each replica apply are separate durable actions. Apply
//! atomically materializes generation, owner, proof obligation and command log.
//! Terminal/abort is collapsed to an already-authorized, converged transition.
//! Bucket release and recording its observed absence are separate actions.
//! An uncertain unsent/uninstalled request has no invented object-PG custodian:
//! frontend loss there retains the bucket record, but this model does NOT prove
//! discovery/retirement of that pre-install reservation. No fairness is assumed.

use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

const PRIMARY: usize = 0;
const WITNESS: usize = 1;
const TRAILING: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    ReleaseOnUnknownInstall,
    ReleaseOnCreationApply,
    GenerationWithoutOwner,
    ProofOnlyInCommandLog,
    ClearSlotBeforeConvergence,
    ClearAbandonedBeforeRelease,
    ForgetOwnerOnRestart,
    RetireBeforeRelease,
    RetireBeforeConfirmingRelease,
    ReleaseForeignProof,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Install {
    Unsent,
    InFlight,
    Rejected,
    Accepted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Disposition {
    Staging,
    Published,
    Cancelled,
    Abandoned,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
struct Replica {
    applied: bool,
    generation: bool,
    owner: bool,
    proof_obligation: bool,
    log: bool,
    retired: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    frontend: bool,
    recovery: bool,
    own_proof: bool,
    foreign_proof: bool,
    install: Install,
    reply_received: bool,
    reply_lost: bool,
    pending: bool,
    started: bool,
    replicas: [Replica; 3],
    confirmed: [bool; 3],
    disposition: Disposition,
    release_failed: bool,
    absence_confirmed: bool,
    restarted: bool,
    restarted_after_release_failure: bool,
    // Ghost monitor: a later restart may legitimately forget volatile absence
    // confirmation, so check what was known at retirement rather than afterward.
    retired_without_confirmation: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            frontend: true,
            recovery: false,
            own_proof: true,
            foreign_proof: true,
            install: Install::Unsent,
            reply_received: false,
            reply_lost: false,
            pending: false,
            started: false,
            replicas: [Replica::default(); 3],
            confirmed: [false; 3],
            disposition: Disposition::Staging,
            release_failed: false,
            absence_confirmed: false,
            restarted: false,
            restarted_after_release_failure: false,
            retired_without_confirmation: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    RejectBeforeDispatch,
    Dispatch,
    RejectInstall,
    AcceptInstall,
    ReceiveReply,
    LoseReply,
    LoseFrontend,
    AcquireRecovery,
    StartPublication,
    Apply(usize),
    InspectReplica(usize),
    ClearSlot,
    AbandonUnstarted,
    Compact(usize),
    Restart,
    Publish,
    Cancel,
    FailRelease,
    Release,
    ConfirmAbsence,
    Retire(usize),
}

impl State {
    fn settled(&self) -> bool {
        self.replicas.iter().all(|r| r.applied) && self.confirmed == [true; 3]
    }

    fn successors(&self, fault: Fault) -> Vec<(Action, Self)> {
        let mut out = Vec::new();
        let mut add = |action, change: &dyn Fn(&mut Self)| {
            let mut next = self.clone();
            change(&mut next);
            out.push((action, next));
        };
        let actor = self.frontend || self.recovery;
        if self.frontend && self.install == Install::Unsent {
            add(Action::RejectBeforeDispatch, &|s| {
                s.install = Install::Rejected;
                s.reply_received = true;
                s.own_proof = false;
            });
            add(Action::Dispatch, &|s| s.install = Install::InFlight);
        }
        if self.install == Install::InFlight {
            add(Action::RejectInstall, &|s| s.install = Install::Rejected);
            // Exact proof/subject validation before installation is an explicit
            // assumption here, not a model of cross-PG check/commit atomicity.
            if self.own_proof {
                add(Action::AcceptInstall, &|s| {
                    s.install = Install::Accepted;
                    s.pending = true;
                });
            }
        }
        if self.frontend
            && !self.reply_received
            && !self.reply_lost
            && matches!(self.install, Install::Accepted | Install::Rejected)
        {
            add(Action::ReceiveReply, &|s| {
                s.reply_received = true;
                if s.install == Install::Rejected {
                    s.own_proof = false;
                }
            });
            add(Action::LoseReply, &|s| s.reply_lost = true);
        }
        if self.frontend {
            add(Action::LoseFrontend, &|s| {
                s.frontend = false;
                if fault == Fault::ReleaseOnUnknownInstall && !s.reply_received {
                    s.own_proof = false;
                }
            });
        }
        // An authorized scan can discover a pending command or retained owner.
        // It cannot conjure an object-PG owner for an uninstalled request.
        if !self.recovery && (self.pending || self.replicas.iter().any(|r| r.owner)) {
            add(Action::AcquireRecovery, &|s| s.recovery = true);
        }
        if actor && self.pending && !self.started && self.disposition == Disposition::Staging {
            add(Action::StartPublication, &|s| s.started = true);
            add(Action::AbandonUnstarted, &|s| {
                s.disposition = Disposition::Abandoned
            });
        }
        for i in [WITNESS, PRIMARY, TRAILING] {
            let prior_applied = match i {
                WITNESS => true,
                PRIMARY => self.replicas[WITNESS].applied,
                TRAILING => self.replicas[PRIMARY].applied,
                _ => unreachable!(),
            };
            if actor && self.pending && self.started && prior_applied && !self.replicas[i].applied {
                add(Action::Apply(i), &|s| {
                    s.replicas[i] = Replica {
                        applied: true,
                        generation: true,
                        owner: fault != Fault::GenerationWithoutOwner,
                        proof_obligation: fault != Fault::ProofOnlyInCommandLog,
                        log: true,
                        retired: false,
                    };
                    if fault == Fault::ReleaseOnCreationApply {
                        s.own_proof = false;
                    }
                });
            }
            if actor && self.replicas[i].applied && !self.confirmed[i] {
                add(Action::InspectReplica(i), &|s| s.confirmed[i] = true);
            }
            if self.settled() && !self.pending && self.replicas[i].log {
                add(Action::Compact(i), &|s| s.replicas[i].log = false);
            }
            if actor
                && self.disposition != Disposition::Staging
                && self.replicas[i].owner
                && (self.absence_confirmed
                    || fault == Fault::RetireBeforeRelease
                    || (fault == Fault::RetireBeforeConfirmingRelease && !self.own_proof))
            {
                add(Action::Retire(i), &|s| {
                    s.replicas[i].owner = false;
                    s.replicas[i].proof_obligation = false;
                    s.replicas[i].retired = true;
                    s.retired_without_confirmation |= !s.absence_confirmed;
                });
            }
        }
        if actor
            && self.pending
            && (self.settled()
                || (self.disposition == Disposition::Abandoned && self.absence_confirmed)
                || fault == Fault::ClearSlotBeforeConvergence
                || (fault == Fault::ClearAbandonedBeforeRelease
                    && self.disposition == Disposition::Abandoned))
        {
            add(Action::ClearSlot, &|s| s.pending = false);
        }
        if actor && self.settled() && !self.pending && self.disposition == Disposition::Staging {
            for (action, disposition) in [
                (Action::Publish, Disposition::Published),
                (Action::Cancel, Disposition::Cancelled),
            ] {
                add(action, &|s| {
                    s.disposition = disposition;
                    for replica in &mut s.replicas {
                        replica.generation = false;
                    }
                });
            }
        }
        if actor && self.disposition != Disposition::Staging && self.own_proof {
            if !self.release_failed {
                add(Action::FailRelease, &|s| s.release_failed = true);
            }
            add(Action::Release, &|s| {
                if fault == Fault::ReleaseForeignProof {
                    s.foreign_proof = false;
                } else {
                    s.own_proof = false;
                }
            });
        }
        if actor
            && self.disposition != Disposition::Staging
            && !self.own_proof
            && !self.absence_confirmed
        {
            add(Action::ConfirmAbsence, &|s| s.absence_confirmed = true);
        }
        if !self.restarted && self.install == Install::Accepted {
            add(Action::Restart, &|s| {
                s.restarted = true;
                s.restarted_after_release_failure = s.release_failed;
                s.frontend = false;
                s.recovery = false;
                s.confirmed = [false; 3];
                s.absence_confirmed = false;
                if fault == Fault::ForgetOwnerOnRestart {
                    for replica in &mut s.replicas {
                        replica.owner = false;
                        replica.proof_obligation = false;
                    }
                }
            });
        }
        out
    }

    fn check(&self) -> Checks {
        let failure = if !self.foreign_proof {
            Some("release never consumes another proof")
        } else if self
            .replicas
            .iter()
            .any(|r| r.applied && r.generation != (self.disposition == Disposition::Staging))
        {
            Some("generation lifetime follows the owner transition")
        } else if self.disposition == Disposition::Staging
            && self.install == Install::Accepted
            && !self.own_proof
        {
            Some("introducing publication and staging retain the live proof")
        } else if self
            .replicas
            .iter()
            .any(|r| r.applied && !r.owner && !r.retired)
        {
            Some("materialized ownership survives until explicit retirement")
        } else if self.replicas.iter().any(|r| r.retired) && self.own_proof {
            Some("owner retirement requires confirmed exact release")
        } else if self.retired_without_confirmation {
            Some("release success must be observed before retiring its obligation")
        } else if self
            .replicas
            .iter()
            .any(|r| r.owner && !r.proof_obligation && !r.log)
        {
            Some("owner retains proof independently of compactable log")
        } else if self.disposition == Disposition::Abandoned && !self.pending && self.own_proof {
            Some("abandonment retains release responsibility until exact release")
        } else if self.install == Install::Accepted
            && self.disposition != Disposition::Abandoned
            && !self.pending
            && !self.replicas.iter().all(|r| r.applied)
        {
            Some("pending introduction survives partial materialization")
        } else {
            None
        };
        let mut witnesses = Vec::new();
        if self.disposition == Disposition::Published
            && !self.own_proof
            && self.replicas.iter().all(|r| r.owner)
        {
            witnesses.push("successful publication releases live proof before owner retirement");
        }
        if !self.frontend && self.install == Install::InFlight && self.own_proof && !self.pending {
            witnesses.push("frontend loss before acceptance retains unresolved bucket reservation");
        }
        if self.install == Install::Rejected && self.reply_received && !self.own_proof {
            witnesses.push("authoritative rejection releases frontend proof");
        }
        if self.pending
            && self.reply_lost
            && !self.frontend
            && self.own_proof
            && !self.replicas.iter().any(|r| r.applied)
        {
            witnesses
                .push("lost install reply leaves exact pending custodian before owner creation");
        }
        if self.replicas[WITNESS].owner
            && !self.replicas[PRIMARY].owner
            && self.restarted
            && self.pending
        {
            witnesses.push("witness-first owner survives restart before primary apply");
        }
        if self.disposition == Disposition::Staging
            && self.restarted
            && !self.pending
            && self
                .replicas
                .iter()
                .all(|r| r.owner && r.proof_obligation && !r.log)
        {
            witnesses.push("staging proof survives complete log compaction and restart");
        }
        if self.restarted_after_release_failure
            && self.replicas.iter().all(|r| r.retired)
            && !self.own_proof
        {
            witnesses.push("release failure and restart converge without a live proof leak");
        }
        if self.disposition == Disposition::Abandoned && !self.pending && !self.own_proof {
            witnesses.push("authoritatively unstarted abandonment releases before slot removal");
        }
        Checks { failure, witnesses }
    }
}

fn run(fault: Fault) -> Exploration<State, Action> {
    explore(
        [State::default()],
        &[
            "successful publication releases live proof before owner retirement",
            "frontend loss before acceptance retains unresolved bucket reservation",
            "authoritative rejection releases frontend proof",
            "lost install reply leaves exact pending custodian before owner creation",
            "witness-first owner survives restart before primary apply",
            "staging proof survives complete log compaction and restart",
            "release failure and restart converge without a live proof leak",
            "authoritatively unstarted abandonment releases before slot removal",
        ],
        |s| s.successors(fault),
        State::check,
        250_000,
    )
}

#[test]
fn cleanup_introduction_model_compaction_restart_and_release_failure_trace() {
    let mut state = State::default();
    for action in [
        Action::Dispatch,
        Action::AcceptInstall,
        Action::LoseReply,
        Action::LoseFrontend,
        Action::AcquireRecovery,
        Action::StartPublication,
        Action::Apply(WITNESS),
        Action::InspectReplica(WITNESS),
        Action::Apply(PRIMARY),
        Action::InspectReplica(PRIMARY),
        Action::Apply(TRAILING),
        Action::InspectReplica(TRAILING),
        Action::ClearSlot,
        Action::Compact(WITNESS),
        Action::Compact(PRIMARY),
        Action::Compact(TRAILING),
        Action::Cancel,
        Action::FailRelease,
        Action::Restart,
        Action::AcquireRecovery,
        Action::Release,
        Action::ConfirmAbsence,
        Action::Retire(WITNESS),
        Action::Retire(PRIMARY),
        Action::Retire(TRAILING),
    ] {
        state = state
            .successors(Fault::None)
            .into_iter()
            .find_map(|(candidate, next)| (candidate == action).then_some(next))
            .unwrap_or_else(|| panic!("disabled {action:?} in {state:?}"));
        assert_eq!(state.check().failure, None, "after {action:?}: {state:?}");
        if action == Action::Restart {
            assert!(state
                .replicas
                .iter()
                .all(|r| r.owner && r.proof_obligation && !r.log));
            assert!(state.own_proof);
            assert!(!state.absence_confirmed);
        }
    }
    assert!(state.foreign_proof);
    assert!(!state.own_proof);
    assert!(!state.pending);
    assert!(state.replicas.iter().all(|r| r.retired));
    assert!(state.restarted_after_release_failure);
}

#[test]
fn cleanup_introduction_model_exhausts_owner_and_proof_handoffs() {
    let result = run(Fault::None);
    result.assert_complete();
    println!(
        "{} states, {} transitions",
        result.state_count(),
        result.transitions
    );
}

#[test]
fn cleanup_introduction_model_detects_independent_custody_faults() {
    for (fault, expected) in [
        (
            Fault::ReleaseOnUnknownInstall,
            "introducing publication and staging retain the live proof",
        ),
        (
            Fault::ReleaseOnCreationApply,
            "introducing publication and staging retain the live proof",
        ),
        (
            Fault::GenerationWithoutOwner,
            "materialized ownership survives until explicit retirement",
        ),
        (
            Fault::ProofOnlyInCommandLog,
            "owner retains proof independently of compactable log",
        ),
        (
            Fault::ClearSlotBeforeConvergence,
            "pending introduction survives partial materialization",
        ),
        (
            Fault::ClearAbandonedBeforeRelease,
            "abandonment retains release responsibility until exact release",
        ),
        (
            Fault::ForgetOwnerOnRestart,
            "materialized ownership survives until explicit retirement",
        ),
        (
            Fault::RetireBeforeRelease,
            "owner retirement requires confirmed exact release",
        ),
        (
            Fault::RetireBeforeConfirmingRelease,
            "release success must be observed before retiring its obligation",
        ),
        (
            Fault::ReleaseForeignProof,
            "release never consumes another proof",
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
