// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Abstract candidate comparison, NOT a production-connected safety proof.
//! One owner/shard, two exact duplicate deliveries, one cutoff, one frontend
//! crash and one data-node restart. Each action is a proposed atomic boundary.
//! Ordered object-PG decisions and durable data-node closure are assumptions;
//! The fanout child adds independent data-node closure and healthy clock offsets;
//! The renewal child models durable horizons and abstract clock-health fencing.
//! The publication child composes these physical transitions with abstract
//! witness/primary/trailing publication. The independent lookahead child adds
//! replica discovery of bounded allocation windows and symbolic placement epochs;
//! production route migration remains unmodelled.
//! Confirmation separately explores read-only grant lookup versus local physical
//! admission, and issuer lifetime fencing. These are protocol candidates, not
//! implemented RPCs or a composed production proof.

use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

mod confirmation;
mod ephemeral_attempt;
mod fanout;
mod introduction;
mod issuer;
mod lookahead;
mod preinstall;
mod publication;
mod renewal;
mod replacement;
mod retirement;

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Cutoff,
    Revocation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mutation {
    None,
    EarlyCutoffCleanup,
    IgnoreInflightWrites,
    ForgetRevocationOnRestart,
    ForgetOwnerOnReleaseFailure,
    IgnoreCommandOwnership,
    InstallDuringCleanup,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Write {
    Unsent,
    Sent,
    Admitted,
    FilePublished,
    AckPersisted,
    Finished,
    Rejected,
}

impl Write {
    fn holds_exclusion(self) -> bool {
        matches!(
            self,
            Self::Admitted | Self::FilePublished | Self::AckPersisted
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Cleanup {
    Staging,
    Cancelled,
    Claimed,
    Deleted,
    Finished,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Command {
    None,
    Pending,
    Committed,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    // Durable state (file and acknowledgement are separate persistence steps).
    cleanup: Cleanup,
    command: Command,
    owner_record: bool,
    // Abstract resource owned by cleanup (e.g. the generation reservation),
    // not evidence that a primary-local bucket liveness row remains visible.
    reservation: bool,
    file: bool,
    ack: bool,
    revoked: bool,
    // Transport / process-local state, with explicit crash transitions.
    writes: [Write; 2],
    receipt: bool,
    frontend_alive: bool,
    // One abstract healthy-clock expiry boundary; never reset on restart.
    expired: bool,
    // Finite fault budgets and property monitors are part of state equality.
    restarted: bool,
    release_failed: bool,
    restarted_after_release_failure: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            cleanup: Cleanup::Staging,
            command: Command::None,
            owner_record: true,
            reservation: true,
            file: false,
            ack: false,
            revoked: false,
            writes: [Write::Unsent; 2],
            receipt: false,
            frontend_alive: true,
            expired: false,
            restarted: false,
            release_failed: false,
            restarted_after_release_failure: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Dispatch(usize),
    Admit(usize),
    Reject(usize),
    PersistFile(usize),
    PersistAck(usize),
    FinishWrite(usize),
    DeliverAck,
    Cancel,
    Expire,
    CrashFrontend,
    RestartDataNode,
    Revoke,
    ClaimCleanup,
    DeletePayload,
    FailRelease,
    ReleaseReservation,
    RemoveOwner,
    InstallCommand,
    ApplyCommand,
}

impl State {
    fn successors(&self, protocol: Protocol, mutation: Mutation) -> Vec<(Step, Self)> {
        let mut out = Vec::new();
        let mut push = |step, next| out.push((step, next));
        for (index, write) in self.writes.iter().enumerate() {
            let mut next = self.clone();
            let step = match write {
                Write::Unsent
                    if self.frontend_alive
                        && !self.expired
                        && self.cleanup == Cleanup::Staging
                        && self.command == Command::None =>
                {
                    next.writes[index] = Write::Sent;
                    Some(Step::Dispatch(index))
                }
                Write::Sent => {
                    // Revocation must reject independently of central owner state:
                    // a delayed frame has not observed cancellation at the primary.
                    if !self.expired && !self.revoked {
                        next.writes[index] = Write::Admitted;
                        Some(Step::Admit(index))
                    } else {
                        next.writes[index] = Write::Rejected;
                        Some(Step::Reject(index))
                    }
                }
                Write::Admitted => {
                    next.file = true;
                    next.writes[index] = Write::FilePublished;
                    Some(Step::PersistFile(index))
                }
                Write::FilePublished => {
                    next.ack = true;
                    next.writes[index] = Write::AckPersisted;
                    Some(Step::PersistAck(index))
                }
                Write::AckPersisted => {
                    next.writes[index] = Write::Finished;
                    Some(Step::FinishWrite(index))
                }
                _ => None,
            };
            if let Some(step) = step {
                push(step, next);
            }
        }
        if !self.receipt && self.frontend_alive && self.writes.contains(&Write::Finished) {
            let mut next = self.clone();
            next.receipt = true;
            push(Step::DeliverAck, next);
        }
        if self.frontend_alive && self.cleanup == Cleanup::Staging {
            let mut next = self.clone();
            next.cleanup = Cleanup::Cancelled;
            push(Step::Cancel, next);
        }
        if !self.expired {
            let mut next = self.clone();
            next.expired = true;
            push(Step::Expire, next);
        }
        if self.frontend_alive {
            let mut next = self.clone();
            next.frontend_alive = false;
            next.receipt = false;
            // Does NOT magically persist cancellation or retract sent messages.
            push(Step::CrashFrontend, next);
        }
        if !self.restarted {
            let mut next = self.clone();
            next.restarted = true;
            next.restarted_after_release_failure = self.release_failed;
            for write in &mut next.writes {
                if write.holds_exclusion() {
                    *write = Write::Rejected;
                }
            }
            if mutation == Mutation::ForgetRevocationOnRestart {
                next.revoked = false;
            }
            // Files/acks survive; pre-admission messages can still arrive.
            push(Step::RestartDataNode, next);
        }
        let abandoned = self.cleanup == Cleanup::Cancelled || self.expired;
        if matches!(protocol, Protocol::Revocation)
            && abandoned
            && self.command == Command::None
            && !self.revoked
        {
            let mut next = self.clone();
            next.revoked = true;
            // Closure prevents new admissions; existing guards still need draining.
            push(Step::Revoke, next);
        }
        let closed = match protocol {
            Protocol::Cutoff => self.expired || mutation == Mutation::EarlyCutoffCleanup,
            Protocol::Revocation => self.revoked,
        };
        let quiescent = !self.writes.iter().any(|write| write.holds_exclusion());
        if matches!(self.cleanup, Cleanup::Staging | Cleanup::Cancelled)
            && abandoned
            && closed
            && (quiescent || mutation == Mutation::IgnoreInflightWrites)
            && (self.command == Command::None || mutation == Mutation::IgnoreCommandOwnership)
        {
            let mut next = self.clone();
            next.cleanup = Cleanup::Claimed;
            push(Step::ClaimCleanup, next);
        }
        if self.cleanup == Cleanup::Claimed {
            let mut next = self.clone();
            next.file = false;
            next.ack = false;
            next.cleanup = Cleanup::Deleted;
            push(Step::DeletePayload, next);
        }
        if self.cleanup == Cleanup::Deleted && self.reservation {
            if !self.release_failed {
                let mut next = self.clone();
                next.release_failed = true;
                if mutation == Mutation::ForgetOwnerOnReleaseFailure {
                    next.owner_record = false;
                }
                push(Step::FailRelease, next);
            }
            let mut next = self.clone();
            next.reservation = false;
            push(Step::ReleaseReservation, next);
        }
        if self.cleanup == Cleanup::Deleted && !self.reservation {
            let mut next = self.clone();
            next.owner_record = false;
            next.cleanup = Cleanup::Finished;
            push(Step::RemoveOwner, next);
        }
        if self.frontend_alive
            && !self.expired
            && self.receipt
            && self.command == Command::None
            && (self.cleanup == Cleanup::Staging || mutation == Mutation::InstallDuringCleanup)
        {
            let mut next = self.clone();
            next.command = Command::Pending;
            push(Step::InstallCommand, next);
        }
        if self.command == Command::Pending {
            let mut next = self.clone();
            next.command = Command::Committed;
            push(Step::ApplyCommand, next);
        }
        out
    }

    fn check(&self) -> Checks {
        let mut checks = Checks::default();
        let cleaning = matches!(
            self.cleanup,
            Cleanup::Claimed | Cleanup::Deleted | Cleanup::Finished
        );
        checks.failure = if self.command != Command::None && (cleaning || !self.file || !self.ack) {
            Some("command-owned payload must remain protected")
        } else if matches!(self.cleanup, Cleanup::Deleted | Cleanup::Finished)
            && (self.file || self.ack)
        {
            Some("deleted payload must not reappear")
        } else if !self.owner_record && self.reservation {
            Some("reservation release must retain a durable owner")
        } else {
            None
        };
        if self.frontend_alive
            && !self.expired
            && self.command == Command::None
            && self.cleanup == Cleanup::Staging
            && self.receipt
        {
            checks.witnesses.push("live staged payload");
        }
        if self.command == Command::Committed && !self.frontend_alive {
            checks
                .witnesses
                .push("command converges after frontend loss");
        }
        if self.expired && self.writes.contains(&Write::AckPersisted) {
            checks
                .witnesses
                .push("admitted IO survives cutoff under exclusion");
        }
        if self.cleanup == Cleanup::Finished && self.writes.contains(&Write::Rejected) {
            checks.witnesses.push("cleanup with a rejected delivery");
        }
        if self.cleanup == Cleanup::Finished && self.restarted_after_release_failure {
            checks
                .witnesses
                .push("cleanup survives release failure and restart");
        }
        if self.cleanup == Cleanup::Finished && self.writes == [Write::Unsent; 2] {
            checks.witnesses.push("pre-write cancellation cleanup");
        }
        checks
    }
}

const WITNESSES: &[&str] = &[
    "live staged payload",
    "command converges after frontend loss",
    "admitted IO survives cutoff under exclusion",
    "cleanup with a rejected delivery",
    "cleanup survives release failure and restart",
    "pre-write cancellation cleanup",
];

fn follow(protocol: Protocol, steps: &[Step]) -> State {
    let mut state = State::default();
    for step in steps {
        state = state
            .successors(protocol, Mutation::None)
            .into_iter()
            .find_map(|(action, next)| (action == *step).then_some(next))
            .unwrap_or_else(|| panic!("disabled {step:?} in {state:?}"));
        assert_eq!(state.check().failure, None, "after {step:?}: {state:?}");
    }
    state
}

#[test]
fn cleanup_ownership_model_pins_transport_gap_and_inflight_boundaries() {
    // The rejection happens AFTER completion and a node restart, not merely
    // somewhere on a trace that also happens to complete cleanup.
    for protocol in [Protocol::Cutoff, Protocol::Revocation] {
        let closure = match protocol {
            Protocol::Cutoff => Step::Expire,
            Protocol::Revocation => Step::Revoke,
        };
        let state = follow(
            protocol,
            &[
                Step::Dispatch(0),
                Step::Cancel,
                closure,
                Step::ClaimCleanup,
                Step::DeletePayload,
                Step::FailRelease,
                Step::RestartDataNode,
                Step::ReleaseReservation,
                Step::RemoveOwner,
                Step::Reject(0),
            ],
        );
        assert_eq!(state.cleanup, Cleanup::Finished);
        assert!(!state.file && !state.ack && !state.reservation);
        assert_eq!(state.expired, matches!(protocol, Protocol::Cutoff));

        // Two exact duplicates may independently hold exclusion. Finishing
        // just the first operation is not enough to permit cleanup.
        let state = follow(
            protocol,
            &[
                Step::Dispatch(0),
                Step::Dispatch(1),
                Step::Admit(0),
                Step::Admit(1),
                Step::Cancel,
                closure,
                Step::PersistFile(0),
                Step::PersistAck(0),
                Step::FinishWrite(0),
            ],
        );
        assert!(state.file && state.ack);
        assert!(!state
            .successors(protocol, Mutation::None)
            .iter()
            .any(|(step, _)| *step == Step::ClaimCleanup));
    }
}

fn run(protocol: Protocol, mutation: Mutation) -> Exploration<State, Step> {
    explore(
        [State::default()],
        WITNESSES,
        |state| state.successors(protocol, mutation),
        State::check,
        250_000,
    )
}

#[test]
fn cleanup_ownership_model_exhausts_both_exclusion_candidates() {
    for protocol in [Protocol::Cutoff, Protocol::Revocation] {
        let result = run(protocol, Mutation::None);
        result.assert_complete();
        println!(
            "{protocol:?}: {} states, {} transitions",
            result.state_count(),
            result.transitions
        );
    }
}

#[test]
fn cleanup_ownership_model_detects_independent_protocol_omissions() {
    for (protocol, mutation, property) in [
        (
            Protocol::Cutoff,
            Mutation::EarlyCutoffCleanup,
            "deleted payload must not reappear",
        ),
        (
            Protocol::Cutoff,
            Mutation::IgnoreInflightWrites,
            "deleted payload must not reappear",
        ),
        (
            Protocol::Revocation,
            Mutation::ForgetRevocationOnRestart,
            "deleted payload must not reappear",
        ),
        (
            Protocol::Cutoff,
            Mutation::ForgetOwnerOnReleaseFailure,
            "reservation release must retain a durable owner",
        ),
        (
            Protocol::Cutoff,
            Mutation::IgnoreCommandOwnership,
            "command-owned payload must remain protected",
        ),
        (
            Protocol::Revocation,
            Mutation::InstallDuringCleanup,
            "command-owned payload must remain protected",
        ),
    ] {
        let result = run(protocol, mutation);
        assert!(
            matches!(result.termination, Termination::Violation { property: found, .. } if found == property),
            "{mutation:?}: {:?}, trace={:?}",
            result.termination,
            result.counterexample()
        );
        println!("{mutation:?}: {:?}", result.counterexample().unwrap());
    }
}
