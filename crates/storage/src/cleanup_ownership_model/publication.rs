// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Abstract composition of the parent physical-cleanup transitions and the
//! command-stream publication contract. No production publisher/SQLite calls.
//! Three fixed metadata replicas, one exact command, one payload/data location.
//! The initial payload is durable, with a duplicate write already in transport.

use super::{Cleanup, Command, Mutation, Protocol, State as Payload, Step, Write};
use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

mod composition;

const PRIMARY: usize = 0;
const WITNESS: usize = 1;
const TRAILING: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    LostInstallReplyReleasesOwnership,
    AbandonMarkedWithoutReplicaRows,
    ForgetMarkerOnRestart,
    DropOwnerOnAbandonment,
    ClearBeforeConvergence,
    ForgetCommittedOwnership,
    InstallDuringCleanup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Rpc {
    Unsent,
    InFlight,
    Lost,
    Received,
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    payload: Payload,
    install: Rpc,
    installed: bool,
    slot: bool,
    started: bool,
    // Ghost history, not a second durable publication-start marker.
    ever_started: bool,
    abandoned: bool,
    replica_rpc: [Rpc; 3],
    applied: [bool; 3],
    confirmed: [bool; 3],
    recovering: bool,
    primary_restarted: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            payload: Payload {
                file: true,
                ack: true,
                receipt: true,
                writes: [Write::Finished, Write::Sent],
                ..Payload::default()
            },
            install: Rpc::Unsent,
            installed: false,
            slot: false,
            started: false,
            ever_started: false,
            abandoned: false,
            replica_rpc: [Rpc::Unsent; 3],
            applied: [false; 3],
            confirmed: [false; 3],
            recovering: false,
            primary_restarted: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Physical(Step),
    DispatchInstall,
    Install,
    RejectInstall,
    LoseInstallReply,
    ReceiveInstallReply,
    MarkStarted,
    Send(usize),
    Apply(usize),
    LoseReply(usize),
    ReceiveReply(usize),
    ObserveReplica(usize),
    Recover,
    RestartPrimary,
    Abandon,
    FailCommittedRelease,
    ReleaseCommittedReservation,
    ClearCommittedSlot,
}

fn push_change(
    out: &mut Vec<(Action, State)>,
    state: &State,
    action: Action,
    change: impl FnOnce(&mut State),
) {
    let mut next = state.clone();
    change(&mut next);
    out.push((action, next));
}

impl State {
    fn actor_available(&self) -> bool {
        self.payload.frontend_alive || self.recovering
    }

    fn successors(&self, protocol: Protocol, fault: Fault) -> Vec<(Action, Self)> {
        let mut out = Vec::new();
        // Compose the SAME physical transitions as the parent, not a copy of
        // their guards. Replace only its atomic install/apply abstraction.
        for (step, payload) in self.payload.successors(protocol, Mutation::None) {
            if matches!(step, Step::InstallCommand | Step::ApplyCommand) {
                continue;
            }
            let mut next = self.clone();
            next.payload = payload;
            out.push((Action::Physical(step), next));
        }
        if self.install == Rpc::Unsent
            && self.payload.frontend_alive
            && self.payload.receipt
            && !self.payload.expired
            && self.payload.cleanup == Cleanup::Staging
        {
            push_change(&mut out, self, Action::DispatchInstall, |s| {
                s.install = Rpc::InFlight
            });
        }
        if !self.installed && matches!(self.install, Rpc::InFlight | Rpc::Lost) {
            // Check at durable acceptance, not just at frontend dispatch. A
            // queued request can arrive after cancellation or frontend loss.
            if !self.payload.expired
                && (self.payload.cleanup == Cleanup::Staging
                    || fault == Fault::InstallDuringCleanup)
            {
                push_change(&mut out, self, Action::Install, |s| {
                    s.installed = true;
                    s.slot = true;
                    s.payload.command = Command::Pending;
                });
            } else {
                push_change(&mut out, self, Action::RejectInstall, |s| {
                    s.install = Rpc::Rejected
                });
            }
        }
        if self.install == Rpc::InFlight {
            let mut next = self.clone();
            next.install = Rpc::Lost;
            if fault == Fault::LostInstallReplyReleasesOwnership && self.slot {
                next.payload.command = Command::None;
                next.payload.cleanup = Cleanup::Cancelled;
            }
            out.push((Action::LoseInstallReply, next));
        }
        if self.installed && self.install == Rpc::InFlight && self.payload.frontend_alive {
            push_change(&mut out, self, Action::ReceiveInstallReply, |s| {
                s.install = Rpc::Received
            });
        }
        if !self.payload.frontend_alive && !self.recovering {
            push_change(&mut out, self, Action::Recover, |s| s.recovering = true);
        }
        if !self.primary_restarted {
            let mut next = self.clone();
            next.primary_restarted = true;
            // Volatile observations are lost; exact durable rows and the
            // pending slot/marker survive. In-flight applies are NOT withdrawn.
            next.confirmed = [false; 3];
            if fault == Fault::ForgetMarkerOnRestart {
                next.started = false;
            }
            out.push((Action::RestartPrimary, next));
        }
        if self.slot
            && !self.started
            && !self.abandoned
            && self.actor_available()
            && !self.payload.expired
            && self.payload.cleanup == Cleanup::Staging
        {
            push_change(&mut out, self, Action::MarkStarted, |s| {
                s.started = true;
                s.ever_started = true;
            });
        }
        if self.slot
            && self.actor_available()
            && (!self.started || fault == Fault::AbandonMarkedWithoutReplicaRows)
            && !self.applied.iter().any(|applied| *applied)
            && self.replica_rpc.iter().all(|rpc| *rpc == Rpc::Unsent)
        {
            let mut next = self.clone();
            // Proposed atomic handoff: preserve durable cleanup responsibility
            // while recording exact abandonment and clearing the pending slot.
            // This does NOT close data-node admission or permit early deletion.
            next.abandoned = true;
            next.slot = false;
            next.payload.command = Command::None;
            next.payload.cleanup = Cleanup::Cancelled;
            if fault == Fault::DropOwnerOnAbandonment {
                next.payload.owner_record = false;
            }
            out.push((Action::Abandon, next));
        }
        for replica in [WITNESS, PRIMARY, TRAILING] {
            let predecessor_confirmed = match replica {
                WITNESS => true,
                PRIMARY => self.confirmed[WITNESS],
                TRAILING => self.confirmed[PRIMARY],
                _ => unreachable!(),
            };
            if self.slot
                && self.started
                && self.actor_available()
                && predecessor_confirmed
                && self.replica_rpc[replica] == Rpc::Unsent
            {
                let mut next = self.clone();
                next.replica_rpc[replica] = Rpc::InFlight;
                out.push((Action::Send(replica), next));
            }
            if matches!(self.replica_rpc[replica], Rpc::InFlight | Rpc::Lost)
                && !self.applied[replica]
            {
                let mut next = self.clone();
                next.applied[replica] = true;
                out.push((Action::Apply(replica), next));
            }
            if self.replica_rpc[replica] == Rpc::InFlight {
                let mut next = self.clone();
                next.replica_rpc[replica] = Rpc::Lost;
                out.push((Action::LoseReply(replica), next));
                if self.applied[replica] && self.actor_available() {
                    let mut next = self.clone();
                    next.replica_rpc[replica] = Rpc::Received;
                    next.confirmed[replica] = true;
                    out.push((Action::ReceiveReply(replica), next));
                }
            }
            // Recovery can certify exact application despite a lost response;
            // absence of a row never undoes the durable publication marker.
            if self.slot
                && self.applied[replica]
                && !self.confirmed[replica]
                && self.actor_available()
            {
                let mut next = self.clone();
                next.confirmed[replica] = true;
                out.push((Action::ObserveReplica(replica), next));
            }
        }
        if self.slot
            && self.confirmed == [true; 3]
            && self.actor_available()
            && self.payload.reservation
        {
            if !self.payload.release_failed {
                push_change(&mut out, self, Action::FailCommittedRelease, |s| {
                    s.payload.release_failed = true
                });
            }
            push_change(&mut out, self, Action::ReleaseCommittedReservation, |s| {
                s.payload.reservation = false
            });
        }
        if self.slot
            && self.actor_available()
            && ((self.confirmed == [true; 3] && !self.payload.reservation)
                || (fault == Fault::ClearBeforeConvergence && self.confirmed[PRIMARY]))
        {
            let mut next = self.clone();
            next.slot = false;
            next.payload.command = if fault == Fault::ForgetCommittedOwnership {
                Command::None
            } else {
                Command::Committed
            };
            next.payload.owner_record = false;
            out.push((Action::ClearCommittedSlot, next));
        }
        out
    }

    fn check(&self) -> Checks {
        let cleaning = matches!(
            self.payload.cleanup,
            Cleanup::Claimed | Cleanup::Deleted | Cleanup::Finished
        );
        let failure = if self.ever_started && self.abandoned {
            Some("irrevocable publication cannot be abandoned")
        } else if (self.slot || self.applied.iter().any(|a| *a))
            && (cleaning || !self.payload.file || !self.payload.ack)
        {
            Some("pending or applied replicas protect the payload")
        } else if self.applied.iter().any(|a| *a) && self.applied != [true; 3] && !self.slot {
            Some("partial replica convergence retains the recovery slot")
        } else if self.applied[PRIMARY] && !self.applied[WITNESS] {
            Some("serving publication requires the durable witness")
        } else {
            self.payload.check().failure
        };
        let mut witnesses = Vec::new();
        if self.slot && self.install == Rpc::Lost && !self.payload.frontend_alive {
            witnesses.push("lost installation response retains pending ownership");
        }
        if self.started && self.applied == [false; 3] && self.recovering && self.primary_restarted {
            witnesses.push("recovery sees irrevocable intent before any replica applies");
        }
        if self.applied[WITNESS] && !self.applied[PRIMARY] && self.replica_rpc[WITNESS] == Rpc::Lost
        {
            witnesses.push("witness application survives its lost reply");
        }
        if self.applied[PRIMARY] && !self.applied[TRAILING] && self.recovering {
            witnesses.push("recovery owns trailing replica convergence");
        }
        if !self.slot && self.applied == [true; 3] && self.payload.release_failed && self.recovering
        {
            witnesses.push("publication converges after release failure and frontend loss");
        }
        if self.abandoned
            && self.payload.cleanup == Cleanup::Finished
            && self.payload.restarted_after_release_failure
        {
            witnesses.push("abandonment cleanup survives failed release and data restart");
        }
        Checks { failure, witnesses }
    }
}

fn run(protocol: Protocol, fault: Fault) -> Exploration<State, Action> {
    explore(
        [State::default()],
        &[
            "lost installation response retains pending ownership",
            "recovery sees irrevocable intent before any replica applies",
            "witness application survives its lost reply",
            "recovery owns trailing replica convergence",
            "publication converges after release failure and frontend loss",
            "abandonment cleanup survives failed release and data restart",
        ],
        |s| s.successors(protocol, fault),
        State::check,
        750_000,
    )
}

fn step(state: State, protocol: Protocol, action: Action) -> State {
    let next = state
        .successors(protocol, Fault::None)
        .into_iter()
        .find_map(|(candidate, next)| (candidate == action).then_some(next))
        .unwrap_or_else(|| panic!("disabled {action:?}: {state:?}"));
    assert_eq!(next.check().failure, None, "after {action:?}: {next:?}");
    next
}

fn enabled(state: &State, protocol: Protocol, action: Action) -> bool {
    state
        .successors(protocol, Fault::None)
        .iter()
        .any(|(candidate, _)| *candidate == action)
}

#[test]
fn cleanup_publication_model_exhausts_physical_and_replica_interleavings() {
    for protocol in [Protocol::Cutoff, Protocol::Revocation] {
        let result = run(protocol, Fault::None);
        result.assert_complete();
        println!(
            "{protocol:?}: {} states, {} transitions",
            result.state_count(),
            result.transitions
        );
    }
}

#[test]
fn cleanup_publication_model_detects_independent_handoff_errors() {
    for (fault, property) in [
        (
            Fault::LostInstallReplyReleasesOwnership,
            "pending or applied replicas protect the payload",
        ),
        (
            Fault::AbandonMarkedWithoutReplicaRows,
            "irrevocable publication cannot be abandoned",
        ),
        (
            Fault::ForgetMarkerOnRestart,
            "irrevocable publication cannot be abandoned",
        ),
        (
            Fault::DropOwnerOnAbandonment,
            "reservation release must retain a durable owner",
        ),
        (
            Fault::ClearBeforeConvergence,
            "partial replica convergence retains the recovery slot",
        ),
        (
            Fault::ForgetCommittedOwnership,
            "pending or applied replicas protect the payload",
        ),
        (
            Fault::InstallDuringCleanup,
            "pending or applied replicas protect the payload",
        ),
    ] {
        let result = run(Protocol::Revocation, fault);
        assert!(
            matches!(result.termination, Termination::Violation { property: actual, .. } if actual == property),
            "{fault:?}: {:?}, trace={:?}",
            result.termination,
            result.counterexample()
        );
        println!("{fault:?}: {:?}", result.counterexample().unwrap());
    }
}

#[test]
fn cleanup_publication_model_pins_marker_and_lost_reply_recovery() {
    for protocol in [Protocol::Cutoff, Protocol::Revocation] {
        let mut s = State::default();
        for action in [
            Action::DispatchInstall,
            Action::Install,
            Action::LoseInstallReply,
            Action::MarkStarted,
        ] {
            s = step(s, protocol, action);
        }
        assert_eq!(s.replica_rpc, [Rpc::Unsent; 3]);
        assert_eq!(s.applied, [false; 3]);
        assert!(
            !enabled(&s, protocol, Action::Abandon),
            "the marker alone must forbid abandonment before any apply is dispatched"
        );
        for action in [
            Action::Send(WITNESS),
            Action::LoseReply(WITNESS),
            Action::Physical(Step::CrashFrontend),
            Action::RestartPrimary,
            Action::Recover,
            Action::Physical(Step::Expire),
        ] {
            s = step(s, protocol, action);
        }
        assert_eq!(s.applied, [false; 3]);
        assert!(!enabled(&s, protocol, Action::Abandon));
        assert!(!enabled(&s, protocol, Action::Physical(Step::ClaimCleanup)));
        for action in [
            Action::Apply(WITNESS),
            Action::ObserveReplica(WITNESS),
            Action::Send(PRIMARY),
            Action::Apply(PRIMARY),
            Action::LoseReply(PRIMARY),
            Action::ObserveReplica(PRIMARY),
        ] {
            s = step(s, protocol, action);
        }
        assert!(!enabled(&s, protocol, Action::ClearCommittedSlot));
        for action in [
            Action::Send(TRAILING),
            Action::Apply(TRAILING),
            Action::LoseReply(TRAILING),
            Action::ObserveReplica(TRAILING),
            Action::FailCommittedRelease,
        ] {
            s = step(s, protocol, action);
        }
        assert!(s.slot && s.payload.reservation);
        assert!(!enabled(&s, protocol, Action::ClearCommittedSlot));
        s = step(s, protocol, Action::ReleaseCommittedReservation);
        s = step(s, protocol, Action::ClearCommittedSlot);
        assert!(s.payload.file && s.payload.ack);
        assert!(!enabled(&s, protocol, Action::Physical(Step::ClaimCleanup)));
    }
}

#[test]
fn cleanup_publication_model_pins_abandonment_and_delayed_install_exclusion() {
    for protocol in [Protocol::Cutoff, Protocol::Revocation] {
        let closure = match protocol {
            Protocol::Cutoff => Step::Expire,
            Protocol::Revocation => Step::Revoke,
        };
        let mut s = State::default();
        for action in [Action::DispatchInstall, Action::Install, Action::Abandon] {
            s = step(s, protocol, action);
        }
        assert!(!s.slot && s.payload.owner_record && s.payload.reservation);
        assert!(!enabled(&s, protocol, Action::Physical(Step::ClaimCleanup)));
        for action in [
            Action::Physical(closure),
            Action::Physical(Step::ClaimCleanup),
            Action::Physical(Step::DeletePayload),
            Action::Physical(Step::FailRelease),
            Action::Physical(Step::RestartDataNode),
            Action::Physical(Step::ReleaseReservation),
            Action::Physical(Step::RemoveOwner),
            Action::Physical(Step::Reject(1)),
        ] {
            s = step(s, protocol, action);
        }
        assert_eq!(s.payload.cleanup, Cleanup::Finished);
        assert!(!s.payload.file && !s.payload.ack);

        let mut delayed = State::default();
        for action in [
            Action::DispatchInstall,
            Action::Physical(Step::Cancel),
            Action::Physical(closure),
            Action::Physical(Step::ClaimCleanup),
        ] {
            delayed = step(delayed, protocol, action);
        }
        assert!(!enabled(&delayed, protocol, Action::Install));
        delayed = step(delayed, protocol, Action::RejectInstall);
        assert!(!delayed.installed);

        let mut accepted_after_loss = State::default();
        for action in [
            Action::DispatchInstall,
            Action::LoseInstallReply,
            Action::Physical(Step::CrashFrontend),
            Action::Install,
            Action::Recover,
        ] {
            accepted_after_loss = step(accepted_after_loss, protocol, action);
        }
        assert!(accepted_after_loss.slot && accepted_after_loss.payload.owner_record);
        assert_eq!(accepted_after_loss.payload.command, Command::Pending);
        assert!(!enabled(
            &accepted_after_loss,
            protocol,
            Action::Physical(Step::ClaimCleanup)
        ));
    }
}
