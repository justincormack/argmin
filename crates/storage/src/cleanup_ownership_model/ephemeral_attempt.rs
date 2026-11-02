// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Abstract cleanup-only scope, NOT production-connected verification.
//! Reuse the parent's physical effects/exclusion and atomic terminal-publication
//! abstraction. The starting owner/window is assumed durably introduced; this
//! does not prove its construction, replica settlement, transfer, or wire fences.
//! One attempt, one physical location, two exact deliveries, one loss of bucket
//! coordination visibility, one data-node restart and one release failure.
//! Losing visibility invalidates the client attempt, NOT its cleanup resource.
//! There is deliberately no action to restore/resume that attempt. Multipart
//! records/committed parts and a client's fresh retry are outside this model.

use super::{Cleanup, Command, Mutation, Protocol, State as Payload, Step, Write};
use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum LossPoint {
    BeforeDispatch,
    Transport,
    InFlight,
    Staged,
    Pending,
    Published,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    ForgetOwner,
    CoordinationAbsenceClosesPhysicalAuthority,
    IgnoreInflight,
    DiscardAcceptedPublication,
    RequireLiveAttemptForCleanup,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
struct State {
    payload: Payload,
    // History is part of state equality. A cleanup completed before loss must
    // never satisfy an after-loss progress canary.
    loss: Option<LossPoint>,
    accepted_publication: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Physical(Step),
    LoseCoordination,
}

impl State {
    fn successors(&self, fault: Fault) -> Vec<(Action, Self)> {
        let mutation = match (self.loss, fault) {
            (Some(_), Fault::CoordinationAbsenceClosesPhysicalAuthority) => {
                Mutation::EarlyCutoffCleanup
            }
            (Some(_), Fault::IgnoreInflight) => Mutation::IgnoreInflightWrites,
            _ => Mutation::None,
        };
        let mut out = Vec::new();
        for (step, payload) in self.payload.successors(Protocol::Cutoff, mutation) {
            if self.loss.is_some()
                && matches!(
                    step,
                    Step::Dispatch(_) | Step::DeliverAck | Step::InstallCommand
                )
            {
                // The old client may still be alive, but its failed attempt
                // cannot issue new work. Already-sent frames are not retracted.
                continue;
            }
            if self.loss.is_some()
                && fault == Fault::RequireLiveAttemptForCleanup
                && step == Step::ClaimCleanup
            {
                continue;
            }
            let mut next = self.clone();
            next.payload = payload;
            next.accepted_publication |= step == Step::InstallCommand;
            out.push((Action::Physical(step), next));
        }
        if self.loss.is_none() && self.payload.cleanup == Cleanup::Staging {
            let point = match self.payload.command {
                Command::Pending => LossPoint::Pending,
                Command::Committed => LossPoint::Published,
                Command::None
                    if self
                        .payload
                        .writes
                        .iter()
                        .any(|write| write.holds_exclusion()) =>
                {
                    LossPoint::InFlight
                }
                Command::None if self.payload.writes.contains(&Write::Finished) => {
                    LossPoint::Staged
                }
                Command::None if self.payload.writes.contains(&Write::Sent) => LossPoint::Transport,
                Command::None => LossPoint::BeforeDispatch,
            };
            let mut next = self.clone();
            next.loss = Some(point);
            next.payload.cleanup = Cleanup::Cancelled;
            next.payload.receipt = false;
            // This records request failure, not a physical revocation or a
            // replicated loss of cleanup ownership. Existing effects may finish.
            if fault == Fault::ForgetOwner {
                next.payload.owner_record = false;
            }
            if fault == Fault::DiscardAcceptedPublication {
                next.payload.command = Command::None;
            }
            out.push((Action::LoseCoordination, next));
        }
        out
    }

    fn check(&self) -> Checks {
        let mut checks = self.payload.check();
        // Inherit all physical safety properties, but declare this model's own
        // after-loss reachability obligations rather than the parent's corpus.
        checks.witnesses.clear();
        if self.accepted_publication && self.payload.command == Command::None {
            checks.failure = Some("request failure preserves accepted publication ownership");
        }
        if let Some(loss) = self.loss {
            if self.payload.cleanup == Cleanup::Finished {
                checks
                    .witnesses
                    .push("cleanup completes without restoring attempt liveness");
                match loss {
                    LossPoint::BeforeDispatch => {
                        checks.witnesses.push("cleanup after loss before dispatch")
                    }
                    LossPoint::Transport => checks
                        .witnesses
                        .push("cleanup after loss with delayed delivery"),
                    LossPoint::InFlight => {
                        checks.witnesses.push("cleanup after loss with admitted IO")
                    }
                    LossPoint::Staged => checks
                        .witnesses
                        .push("cleanup after loss of staged attempt"),
                    LossPoint::Pending | LossPoint::Published => {}
                }
                if self.payload.restarted_after_release_failure {
                    checks
                        .witnesses
                        .push("failed attempt cleanup survives release failure and restart");
                }
            }
            if self.payload.command == Command::Committed && self.payload.file && self.payload.ack {
                match loss {
                    LossPoint::Pending => checks
                        .witnesses
                        .push("accepted publication converges after attempt failure"),
                    LossPoint::Published => checks
                        .witnesses
                        .push("published payload survives attempt failure"),
                    _ => {}
                }
            }
            if loss == LossPoint::Transport && self.payload.writes.contains(&Write::Admitted) {
                checks
                    .witnesses
                    .push("previously sent write may be admitted after attempt failure");
            }
        }
        checks
    }
}

const WITNESSES: &[&str] = &[
    "cleanup completes without restoring attempt liveness",
    "cleanup after loss before dispatch",
    "cleanup after loss with delayed delivery",
    "cleanup after loss with admitted IO",
    "cleanup after loss of staged attempt",
    "failed attempt cleanup survives release failure and restart",
    "accepted publication converges after attempt failure",
    "published payload survives attempt failure",
    "previously sent write may be admitted after attempt failure",
];

fn run(fault: Fault) -> Exploration<State, Action> {
    explore(
        [State::default()],
        WITNESSES,
        |state| state.successors(fault),
        State::check,
        250_000,
    )
}

fn advance(state: &mut State, action: Action) {
    *state = state
        .successors(Fault::None)
        .into_iter()
        .find_map(|(candidate, next)| (candidate == action).then_some(next))
        .unwrap_or_else(|| panic!("disabled {action:?} in {state:?}"));
    assert_eq!(state.check().failure, None, "{action:?}: {state:?}");
}

fn enabled(state: &State, step: Step) -> bool {
    state
        .successors(Fault::None)
        .iter()
        .any(|(action, _)| *action == Action::Physical(step))
}

#[test]
fn cleanup_ephemeral_attempt_model_exhausts_without_session_recovery() {
    let result = run(Fault::None);
    result.assert_complete();
    println!(
        "states={}, transitions={}",
        result.state_count(),
        result.transitions
    );
}

#[test]
fn cleanup_ephemeral_attempt_model_detects_independent_ownership_errors() {
    for (fault, expected) in [
        (
            Fault::ForgetOwner,
            "reservation release must retain a durable owner",
        ),
        (
            Fault::CoordinationAbsenceClosesPhysicalAuthority,
            "deleted payload must not reappear",
        ),
        (Fault::IgnoreInflight, "deleted payload must not reappear"),
        (
            Fault::DiscardAcceptedPublication,
            "request failure preserves accepted publication ownership",
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
    // This mutation is a progress failure, not an unsafe deletion. Exhaustion
    // without the required witness must not be mistaken for a successful model.
    let stalled = run(Fault::RequireLiveAttemptForCleanup);
    assert_eq!(stalled.termination, Termination::Complete);
    assert!(stalled
        .witness("cleanup completes without restoring attempt liveness")
        .is_none());
    assert!(stalled
        .witness("accepted publication converges after attempt failure")
        .is_some());
}

#[test]
fn cleanup_ephemeral_attempt_model_pins_delayed_and_admitted_effects() {
    let mut state = State::default();
    advance(&mut state, Action::Physical(Step::Dispatch(0)));
    advance(&mut state, Action::Physical(Step::Dispatch(1)));
    advance(&mut state, Action::LoseCoordination);
    assert!(
        state.payload.frontend_alive,
        "request failure does not require frontend death"
    );
    assert!(state.payload.owner_record && state.payload.reservation);
    assert!(!enabled(&state, Step::ClaimCleanup));
    advance(&mut state, Action::Physical(Step::Admit(0)));
    advance(&mut state, Action::Physical(Step::Expire));
    assert!(
        !enabled(&state, Step::ClaimCleanup),
        "expiry does not cancel admitted IO"
    );
    for step in [
        Step::PersistFile(0),
        Step::PersistAck(0),
        Step::FinishWrite(0),
        Step::ClaimCleanup,
        Step::DeletePayload,
        Step::FailRelease,
        Step::RestartDataNode,
        Step::ReleaseReservation,
        Step::RemoveOwner,
        Step::Reject(1),
    ] {
        advance(&mut state, Action::Physical(step));
    }
    assert_eq!(state.payload.cleanup, Cleanup::Finished);
    assert!(
        !state.payload.file
            && !state.payload.ack
            && !state.payload.reservation
            && !state.payload.owner_record
    );
    assert_eq!(state.loss, Some(LossPoint::Transport));
}

#[test]
fn cleanup_ephemeral_attempt_model_distinguishes_staging_from_accepted_publication() {
    let mut staged = State::default();
    for step in [
        Step::Dispatch(0),
        Step::Admit(0),
        Step::PersistFile(0),
        Step::PersistAck(0),
        Step::FinishWrite(0),
        Step::DeliverAck,
    ] {
        advance(&mut staged, Action::Physical(step));
    }
    let mut pending = staged.clone();
    advance(&mut pending, Action::Physical(Step::InstallCommand));
    advance(&mut staged, Action::LoseCoordination);
    advance(&mut pending, Action::LoseCoordination);
    assert!(!enabled(&staged, Step::InstallCommand));
    for state in [&mut staged, &mut pending] {
        advance(state, Action::Physical(Step::Expire));
    }
    assert!(enabled(&staged, Step::ClaimCleanup));
    assert!(!enabled(&pending, Step::ClaimCleanup));
    advance(&mut pending, Action::Physical(Step::ApplyCommand));
    assert_eq!(pending.payload.command, Command::Committed);
    assert!(pending.payload.file && pending.payload.ack);
    assert!(!enabled(&pending, Step::ClaimCleanup));
}
