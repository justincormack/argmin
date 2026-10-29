// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bounded interface composition using the actual publication-model transitions.
//! Local clocks are abstracted to two conservative expiry barriers. Individual
//! RPC scheduling remains in the parent; install and apply use explicit macro
//! steps here to keep the cross-product bounded. This is NOT a refinement proof.

use super::{Action as PublicationAction, Fault as PublicationFault, State as Publication};
use super::{Cleanup, Command, Protocol, Step, Write, PRIMARY, TRAILING, WITNESS};
use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Time {
    BeforeOriginal,
    OriginalExpired,
    RenewedExpired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    CloseAtOriginalCutoff,
    PartialClosure,
    IgnoreInflightWrite,
    ForgetRenewalOnRestart,
    RetireBeforeDeleteReply,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Target {
    write: Write,
    file: bool,
    ack: bool,
    revoked: bool,
    closed: bool,
    closure_received: bool,
    deleted: bool,
    delete_received: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    publication: Publication,
    targets: [Target; 2],
    time: Time,
    renewed: bool,
    renewal_delivered: bool,
    offline: bool,
    crashed: bool,
    restarted: bool,
}

impl Default for State {
    fn default() -> Self {
        let mut publication = Publication::default();
        // Physical effects are now represented by the two targets. Do not also
        // execute the parent's one-location duplicate write.
        publication.payload.writes = [Write::Finished; 2];
        Self {
            publication,
            targets: std::array::from_fn(|i| Target {
                write: if i == 0 { Write::Sent } else { Write::Unsent },
                file: true,
                ack: true,
                revoked: false,
                closed: false,
                closure_received: false,
                deleted: false,
                delete_received: false,
            }),
            time: Time::BeforeOriginal,
            renewed: false,
            renewal_delivered: false,
            offline: false,
            crashed: false,
            restarted: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Publication(PublicationAction),
    InstallWithLostReply,
    ApplyWithLostReply(usize),
    Renew,
    DeliverRenewal,
    ExpireOriginal,
    ExpireRenewed,
    Write(usize),
    Revoke(usize),
    Close(usize),
    ReceiveClosure(usize),
    Delete(usize),
    ReceiveDelete(usize),
    CrashTarget,
    RestartTarget,
}

// Each macro step executes the parent model's real guards/effects, not a second
// implementation of its publication state machine. Interleavings inside these
// sequences are deliberately omitted from THIS graph and covered by the parent.
fn publication_steps(
    state: &Publication,
    protocol: Protocol,
    actions: &[PublicationAction],
) -> Option<Publication> {
    let mut state = state.clone();
    for action in actions {
        state = state
            .successors(protocol, PublicationFault::None)
            .into_iter()
            .find_map(|(candidate, next)| (candidate == *action).then_some(next))?;
    }
    Some(state)
}

impl State {
    fn cutoff_expired(&self, target: usize) -> bool {
        match target {
            0 => self.time != Time::BeforeOriginal,
            1 => self.time == Time::RenewedExpired,
            _ => unreachable!(),
        }
    }

    fn horizon_expired(&self) -> bool {
        if self.renewed {
            self.time == Time::RenewedExpired
        } else {
            self.time != Time::BeforeOriginal
        }
    }

    fn successors(&self, protocol: Protocol, fault: Fault) -> Vec<(Action, Self)> {
        let mut out = Vec::new();
        let p = &self.publication.payload;
        // Retain publication and ownership transitions, but replace its physical
        // IO, one-node closure, and one-step expiry with the composed boundaries.
        for (action, publication) in self
            .publication
            .successors(protocol, PublicationFault::None)
        {
            let allowed = match action {
                PublicationAction::MarkStarted
                | PublicationAction::ObserveReplica(_)
                | PublicationAction::Recover
                | PublicationAction::RestartPrimary
                | PublicationAction::Abandon
                | PublicationAction::FailCommittedRelease
                | PublicationAction::ReleaseCommittedReservation
                | PublicationAction::ClearCommittedSlot => true,
                PublicationAction::Physical(step) => match step {
                    Step::Cancel
                    | Step::CrashFrontend
                    | Step::FailRelease
                    | Step::ReleaseReservation
                    | Step::RemoveOwner => true,
                    Step::Revoke => self.targets.iter().all(|n| n.revoked),
                    Step::ClaimCleanup => {
                        let closures = self.targets.iter().filter(|n| n.closure_received).count();
                        closures == 2 || (fault == Fault::PartialClosure && closures == 1)
                    }
                    Step::DeletePayload => self.targets.iter().all(|n| {
                        n.delete_received || (fault == Fault::RetireBeforeDeleteReply && n.deleted)
                    }),
                    _ => false,
                },
                _ => false,
            };
            if allowed {
                let mut next = self.clone();
                next.publication = publication;
                if action == PublicationAction::RestartPrimary
                    && fault == Fault::ForgetRenewalOnRestart
                {
                    next.renewed = false;
                }
                out.push((Action::Publication(action), next));
            }
        }
        if let Some(publication) = publication_steps(
            &self.publication,
            protocol,
            &[
                PublicationAction::DispatchInstall,
                PublicationAction::Install,
                PublicationAction::LoseInstallReply,
            ],
        ) {
            let mut next = self.clone();
            next.publication = publication;
            out.push((Action::InstallWithLostReply, next));
        }
        for replica in [WITNESS, PRIMARY, TRAILING] {
            if let Some(publication) = publication_steps(
                &self.publication,
                protocol,
                &[
                    PublicationAction::Send(replica),
                    PublicationAction::Apply(replica),
                    PublicationAction::LoseReply(replica),
                ],
            ) {
                let mut next = self.clone();
                next.publication = publication;
                out.push((Action::ApplyWithLostReply(replica), next));
            }
        }
        if !self.renewed
            && self.time == Time::BeforeOriginal
            && p.cleanup == Cleanup::Staging
            && p.command == Command::None
            && p.frontend_alive
        {
            let mut next = self.clone();
            next.renewed = true;
            out.push((Action::Renew, next));
        }
        // A granted reply can arrive after cancellation, publication, loss of
        // its frontend, or cleanup. No liveness dependence on that frontend.
        if self.renewed && !self.renewal_delivered {
            let mut next = self.clone();
            next.renewal_delivered = true;
            next.targets[1].write = Write::Sent;
            out.push((Action::DeliverRenewal, next));
        }
        if self.time == Time::BeforeOriginal {
            let mut next = self.clone();
            next.time = Time::OriginalExpired;
            next.publication.payload.expired = true;
            out.push((Action::ExpireOriginal, next));
        } else if self.time == Time::OriginalExpired {
            let mut next = self.clone();
            next.time = Time::RenewedExpired;
            out.push((Action::ExpireRenewed, next));
        }
        if !self.crashed {
            let mut next = self.clone();
            next.crashed = true;
            next.offline = true;
            if next.targets[1].write.holds_exclusion() {
                next.targets[1].write = Write::Rejected;
            }
            out.push((Action::CrashTarget, next));
        }
        if self.offline {
            let mut next = self.clone();
            next.offline = false;
            next.restarted = true;
            next.publication.payload.restarted = true;
            next.publication.payload.restarted_after_release_failure = p.release_failed;
            out.push((Action::RestartTarget, next));
        }
        let cleanup_allowed =
            p.command == Command::None && (p.cleanup == Cleanup::Cancelled || p.expired);
        for i in 0..2 {
            if i == 1 && self.offline {
                continue;
            }
            let n = &self.targets[i];
            let mut next = self.clone();
            let advanced = match n.write {
                Write::Sent => {
                    next.targets[i].write = if n.revoked || self.cutoff_expired(i) {
                        Write::Rejected
                    } else {
                        Write::Admitted
                    };
                    true
                }
                Write::Admitted => {
                    next.targets[i].file = true;
                    next.targets[i].write = Write::FilePublished;
                    true
                }
                Write::FilePublished => {
                    next.targets[i].ack = true;
                    next.targets[i].write = Write::AckPersisted;
                    true
                }
                Write::AckPersisted => {
                    next.targets[i].write = Write::Finished;
                    true
                }
                _ => false,
            };
            if advanced {
                out.push((Action::Write(i), next));
            }
            if cleanup_allowed && !n.closed {
                if matches!(protocol, Protocol::Revocation) && !n.revoked {
                    let mut next = self.clone();
                    next.targets[i].revoked = true;
                    out.push((Action::Revoke(i), next));
                }
                let authority_closed = match protocol {
                    Protocol::Revocation => n.revoked,
                    Protocol::Cutoff => {
                        self.horizon_expired()
                            || (fault == Fault::CloseAtOriginalCutoff && p.expired)
                    }
                };
                if authority_closed
                    && (!n.write.holds_exclusion() || fault == Fault::IgnoreInflightWrite)
                {
                    let mut next = self.clone();
                    next.targets[i].closed = true;
                    out.push((Action::Close(i), next));
                }
            }
            if n.closed && !n.closure_received {
                let mut next = self.clone();
                next.targets[i].closure_received = true;
                out.push((Action::ReceiveClosure(i), next));
            }
            if p.cleanup == Cleanup::Claimed && !n.deleted {
                let mut next = self.clone();
                next.targets[i].deleted = true;
                next.targets[i].file = false;
                next.targets[i].ack = false;
                out.push((Action::Delete(i), next));
            }
            if n.deleted && !n.delete_received {
                let mut next = self.clone();
                next.targets[i].delete_received = true;
                out.push((Action::ReceiveDelete(i), next));
            }
        }
        out
    }

    fn check(&self) -> Checks {
        let p = &self.publication.payload;
        let cleaning = matches!(
            p.cleanup,
            Cleanup::Claimed | Cleanup::Deleted | Cleanup::Finished
        );
        let failure = if self.renewal_delivered && !self.renewed {
            Some("restart must retain the delivered renewal horizon")
        } else if self.targets.iter().enumerate().any(|(i, n)| {
            n.closed
                && (n.write.holds_exclusion()
                    || (!n.revoked && !self.cutoff_expired(i) && (i == 0 || self.renewed)))
        }) {
            Some("target closure must exclude renewed and in-flight writes")
        } else if cleaning && self.targets.iter().any(|n| !n.closure_received) {
            Some("cleanup requires every target closure reply")
        } else if self.targets.iter().any(|n| n.deleted && (n.file || n.ack)) {
            Some("deleted target must not reappear")
        } else if (self.publication.slot || self.publication.applied.iter().any(|a| *a))
            && self.targets.iter().any(|n| !n.file || !n.ack || n.deleted)
        {
            Some("publication protects every payload target")
        } else if p.cleanup == Cleanup::Finished && self.targets.iter().any(|n| !n.delete_received)
        {
            Some("retirement requires every deletion reply")
        } else {
            self.publication.check().failure
        };
        let mut witnesses = Vec::new();
        if self.renewed
            && self.offline
            && self.targets[0].closure_received
            && !self.targets[1].closure_received
        {
            witnesses.push("renewed owner survives partial closure and unavailable target");
        }
        if self.renewed
            && self.publication.ever_started
            && self.publication.recovering
            && self.publication.applied == [true; 3]
            && !self.publication.slot
        {
            witnesses.push("renewed publication converges through recovery");
        }
        if self.renewal_delivered
            && self.publication.abandoned
            && p.cleanup == Cleanup::Finished
            && self.restarted
        {
            witnesses.push("renewed abandonment cleans after target restart");
        }
        Checks { failure, witnesses }
    }
}

fn run(protocol: Protocol, fault: Fault) -> Exploration<State, Action> {
    explore(
        [State::default()],
        &[
            "renewed owner survives partial closure and unavailable target",
            "renewed publication converges through recovery",
            "renewed abandonment cleans after target restart",
        ],
        |s| s.successors(protocol, fault),
        State::check,
        750_000,
    )
}

#[test]
fn cleanup_composition_model_exhausts_renewal_closure_and_publication() {
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
fn cleanup_composition_model_detects_cross_boundary_omissions() {
    for (protocol, fault, property) in [
        (
            Protocol::Cutoff,
            Fault::CloseAtOriginalCutoff,
            "target closure must exclude renewed and in-flight writes",
        ),
        (
            Protocol::Revocation,
            Fault::IgnoreInflightWrite,
            "target closure must exclude renewed and in-flight writes",
        ),
        (
            Protocol::Revocation,
            Fault::PartialClosure,
            "cleanup requires every target closure reply",
        ),
        (
            Protocol::Cutoff,
            Fault::ForgetRenewalOnRestart,
            "restart must retain the delivered renewal horizon",
        ),
        (
            Protocol::Revocation,
            Fault::RetireBeforeDeleteReply,
            "retirement requires every deletion reply",
        ),
    ] {
        let result = run(protocol, fault);
        assert!(
            matches!(result.termination, Termination::Violation { property: actual, .. } if actual == property),
            "{fault:?}: {:?}; trace={:?}",
            result.termination,
            result.counterexample()
        );
        println!("{fault:?}: {:?}", result.counterexample().unwrap());
    }
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

fn physical(step: Step) -> Action {
    Action::Publication(PublicationAction::Physical(step))
}

#[test]
fn cleanup_composition_model_pins_renewed_inflight_write_and_unavailable_target() {
    let protocol = Protocol::Cutoff;
    let mut s = State::default();
    for action in [
        Action::Renew,
        Action::InstallWithLostReply,
        Action::Publication(PublicationAction::Abandon),
        Action::DeliverRenewal,
        Action::ExpireOriginal,
        Action::Write(1),
    ] {
        s = step(s, protocol, action);
    }
    assert_eq!(s.targets[1].write, Write::Admitted);
    assert!(
        !enabled(&s, protocol, Action::Close(0)),
        "the original cutoff cannot close a renewed owner"
    );
    assert!(!enabled(&s, protocol, Action::Close(1)));
    s = step(s, protocol, Action::ExpireRenewed);
    s = step(s, protocol, Action::Close(0));
    s = step(s, protocol, Action::ReceiveClosure(0));
    assert!(
        !enabled(&s, protocol, Action::Close(1)),
        "expiry cannot cancel admitted IO"
    );
    assert!(!enabled(&s, protocol, Action::InstallWithLostReply));
    s = step(s, protocol, Action::CrashTarget);
    assert!(!enabled(&s, protocol, physical(Step::ClaimCleanup)));
    s = step(
        s,
        protocol,
        Action::Publication(PublicationAction::RestartPrimary),
    );
    assert!(s.renewed);
    for action in [
        Action::RestartTarget,
        Action::Close(1),
        Action::ReceiveClosure(1),
        physical(Step::ClaimCleanup),
        Action::Delete(0),
        Action::ReceiveDelete(0),
        Action::Delete(1),
    ] {
        s = step(s, protocol, action);
    }
    assert!(
        !enabled(&s, protocol, physical(Step::DeletePayload)),
        "physical deletion without its reply is not terminal cleanup evidence"
    );
    for action in [
        Action::ReceiveDelete(1),
        physical(Step::DeletePayload),
        physical(Step::FailRelease),
    ] {
        s = step(s, protocol, action);
    }
    assert!(s.publication.payload.owner_record && s.publication.payload.reservation);
    for action in [
        physical(Step::ReleaseReservation),
        physical(Step::RemoveOwner),
        Action::Write(0),
    ] {
        s = step(s, protocol, action);
    }
    assert!(s.targets.iter().all(|n| !n.file && !n.ack));
    assert_eq!(s.targets[0].write, Write::Rejected);
}

#[test]
fn cleanup_composition_model_pins_publication_protection_after_renewal_and_expiry() {
    for protocol in [Protocol::Cutoff, Protocol::Revocation] {
        let mut s = State::default();
        for action in [
            Action::Renew,
            Action::InstallWithLostReply,
            Action::Publication(PublicationAction::MarkStarted),
            physical(Step::CrashFrontend),
            Action::Publication(PublicationAction::Recover),
            Action::Publication(PublicationAction::RestartPrimary),
            Action::ExpireOriginal,
            Action::ExpireRenewed,
            Action::DeliverRenewal,
            Action::Write(1),
        ] {
            s = step(s, protocol, action);
        }
        assert!(s.renewed && s.publication.slot);
        assert_eq!(s.targets[1].write, Write::Rejected);
        assert_eq!(s.publication.applied, [false; 3]);
        assert!(!enabled(
            &s,
            protocol,
            Action::Publication(PublicationAction::Abandon)
        ));
        for i in 0..2 {
            assert!(!enabled(&s, protocol, Action::Close(i)));
            assert!(!enabled(&s, protocol, Action::Revoke(i)));
        }
        for replica in [WITNESS, PRIMARY, TRAILING] {
            s = step(s, protocol, Action::ApplyWithLostReply(replica));
            assert!(!enabled(&s, protocol, physical(Step::ClaimCleanup)));
            s = step(
                s,
                protocol,
                Action::Publication(PublicationAction::ObserveReplica(replica)),
            );
        }
        for action in [
            PublicationAction::ReleaseCommittedReservation,
            PublicationAction::ClearCommittedSlot,
        ] {
            s = step(s, protocol, Action::Publication(action));
        }
        assert!(!s.publication.slot);
        assert!(s.targets.iter().all(|n| n.file && n.ack && !n.deleted));
        assert!(!enabled(&s, protocol, physical(Step::ClaimCleanup)));
    }
}

#[test]
fn cleanup_composition_model_pins_delayed_renewal_after_early_revocation_cleanup() {
    let protocol = Protocol::Revocation;
    let mut s = State::default();
    for action in [
        Action::Renew,
        Action::InstallWithLostReply,
        Action::Publication(PublicationAction::Abandon),
        Action::Revoke(0),
        Action::Close(0),
        Action::ReceiveClosure(0),
        Action::Revoke(1),
        Action::Close(1),
        Action::ReceiveClosure(1),
        physical(Step::Revoke),
        physical(Step::ClaimCleanup),
        Action::Delete(0),
        Action::ReceiveDelete(0),
        Action::Delete(1),
        Action::ReceiveDelete(1),
        physical(Step::DeletePayload),
        physical(Step::ReleaseReservation),
        physical(Step::RemoveOwner),
        Action::CrashTarget,
        Action::RestartTarget,
        Action::DeliverRenewal,
        Action::Write(1),
    ] {
        s = step(s, protocol, action);
    }
    assert_eq!(s.time, Time::BeforeOriginal);
    assert_eq!(s.targets[1].write, Write::Rejected);
    assert!(s.targets.iter().all(|n| !n.file && !n.ack));
}
