// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Independent bounded-window candidate, NOT production failover validation.
//! Two retry identities for segment zero, one representative identity in the next
//! window, two object-metadata replicas and one abstract file/ack participant per
//! identity. Creation has already replicated window zero. Append has separate
//! install, irrevocable marker, witness apply, primary apply and slot-clear steps.
//! No elected-primary certificate is fabricated: failover stops the replacement's
//! allocation and unresolved pending publication blocks cleanup. The old issuer
//! may still consume its initial finite window before the cutoff, but cannot
//! extend it or install fresh commands. Recovery of an unavailable
//! primary's partial command is deliberately not supplied by this model.
//!
//! Expiry is one conservative global cutoff (no renewal). Cleanup batches all
//! exact keys atomically after local IO drains; EC fanout, receipts, clocks,
//! reservation release and real route-history authorization are separate models/
//! integration obligations. The finalization/replacement actions are abstract
//! converged metadata transitions, not a second fanout protocol proof.

use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

const FIRST_WINDOW: u8 = 0b011;
const NEXT_WINDOW: u8 = 0b100;
const KEYS: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    ReuseReplicaCounter,
    AllocatePastWindow,
    IssueBeforeReplication,
    ForgetPriorWindow,
    ForgetPendingProtection,
    IgnoreFileIo,
    IgnoreAckIo,
    DiscoverAtCurrentRoute,
    DeleteBeforeCutoff,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Write {
    Unissued,
    Prepared,
    FileSent,
    FileAdmitted,
    FileDone,
    AckSent,
    AckAdmitted,
    Done,
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Command {
    None,
    Sent,
    Pending,
    Started,
    WitnessApplied,
    BothApplied,
    Settled,
    Abandoned,
}

impl Command {
    fn unresolved(self) -> bool {
        matches!(
            self,
            Self::Sent | Self::Pending | Self::Started | Self::WitnessApplied | Self::BothApplied
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    // Replicated scopes are durable; counters represent each node's local floor.
    windows: [u8; 2],
    counters: [u8; 2],
    references: [Option<usize>; 2],
    next_window_epoch: u8,
    command: Command,
    selected: Option<usize>,
    reply_lost: bool,
    primary: usize,
    sealed: bool,
    part_published: bool,
    replaced: bool,
    expired: bool,
    current_epoch: u8,
    writes: [Write; KEYS],
    files: [bool; KEYS],
    acks: [bool; KEYS],
    cleaned: bool,
    // Ghost allocation history; never used to discover keys or choose a VID.
    issued: u8,
    duplicate_allocation: bool,
    late_old_primary_allocation: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            windows: [FIRST_WINDOW; 2],
            counters: [0; 2],
            references: [None; 2],
            next_window_epoch: 0,
            command: Command::None,
            selected: None,
            reply_lost: false,
            primary: 0,
            sealed: false,
            part_published: false,
            replaced: false,
            expired: false,
            current_epoch: 0,
            writes: [Write::Unissued; KEYS],
            files: [false; KEYS],
            acks: [false; KEYS],
            cleaned: false,
            issued: 0,
            duplicate_allocation: false,
            late_old_primary_allocation: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Prepare(usize),
    LatePrepareAtOldPrimary(usize),
    FileSend(usize),
    FileAdmit(usize),
    FileReject(usize),
    FilePersist(usize),
    AckSend(usize),
    AckAdmit(usize),
    AckReject(usize),
    AckPersist(usize),
    DispatchAppend(usize),
    Install,
    MarkStarted,
    ApplyWitness,
    ApplyPrimary,
    Settle,
    LoseReply,
    AbandonUnstarted,
    Failover,
    AdvancePlacementEpoch,
    Expire,
    Cancel,
    FinalizePart,
    ReplacePart,
    Cleanup,
}

impl State {
    fn key_epoch(&self, key: usize) -> u8 {
        if key < 2 {
            0
        } else {
            self.next_window_epoch
        }
    }

    fn discoverable(&self, fault: Fault) -> u8 {
        // Only the surviving replica's durable windows/reference are inspected,
        // never the old primary's counter or ghost allocation history.
        let mut keys = self.windows[1];
        if let Some(key) = self.references[1] {
            keys |= 1 << key;
        }
        if fault == Fault::DiscoverAtCurrentRoute {
            for key in 0..KEYS {
                if self.key_epoch(key) != self.current_epoch {
                    keys &= !(1 << key);
                }
            }
        }
        keys
    }

    fn allocate(&mut self, key: usize) {
        if self.issued & (1 << key) != 0 {
            self.duplicate_allocation = true;
        } else {
            self.writes[key] = Write::Prepared;
        }
        self.issued |= 1 << key;
    }

    fn successors(&self, fault: Fault) -> Vec<(Action, Self)> {
        let mut out = Vec::new();
        let mut add = |action, change: &dyn Fn(&mut Self)| {
            let mut next = self.clone();
            change(&mut next);
            out.push((action, next));
        };
        if !self.expired && !self.sealed {
            if self.primary == 0 && self.command == Command::None {
                let key = usize::from(self.counters[0]);
                if key < 2 || (key == 2 && fault == Fault::AllocatePastWindow) {
                    add(Action::Prepare(key), &|s| {
                        s.allocate(key);
                        s.counters[0] += 1;
                    });
                }
            }
            if self.primary == 1 && fault == Fault::ReuseReplicaCounter {
                let key = usize::from(self.counters[1]);
                if key < 2 {
                    add(Action::Prepare(key), &|s| {
                        s.allocate(key);
                        s.counters[1] += 1;
                    });
                }
            }
            let has_next =
                self.command == Command::Settled && self.windows[self.primary] & NEXT_WINDOW != 0;
            let premature = fault == Fault::IssueBeforeReplication
                && matches!(self.command, Command::Pending | Command::Started);
            if self.primary == 0 && (has_next || premature) && self.writes[2] == Write::Unissued {
                add(Action::Prepare(2), &|s| s.allocate(2));
            }
        }
        // A stopped/replaced primary need not retract already delegated scope.
        // Model a stale issuer still consuming that bounded scope, even after
        // the successor has sealed the owner. It cannot renew the cutoff.
        if self.primary == 1 && !self.expired && self.counters[0] < 2 {
            let key = usize::from(self.counters[0]);
            add(Action::LatePrepareAtOldPrimary(key), &|s| {
                s.allocate(key);
                s.counters[0] += 1;
                s.late_old_primary_allocation = true;
            });
        }
        // Prepared frames and delayed acknowledgement requests remain in transit
        // independently of the primary, its counters, and cancellation.
        for key in 0..KEYS {
            match self.writes[key] {
                Write::Prepared => add(Action::FileSend(key), &|s| s.writes[key] = Write::FileSent),
                Write::FileSent if !self.expired => add(Action::FileAdmit(key), &|s| {
                    s.writes[key] = Write::FileAdmitted
                }),
                Write::FileSent => add(Action::FileReject(key), &|s| {
                    s.writes[key] = Write::Rejected
                }),
                Write::FileAdmitted => add(Action::FilePersist(key), &|s| {
                    s.files[key] = true;
                    s.writes[key] = Write::FileDone;
                }),
                Write::FileDone => add(Action::AckSend(key), &|s| s.writes[key] = Write::AckSent),
                Write::AckSent if !self.expired => add(Action::AckAdmit(key), &|s| {
                    s.writes[key] = Write::AckAdmitted
                }),
                Write::AckSent => add(Action::AckReject(key), &|s| s.writes[key] = Write::Rejected),
                Write::AckAdmitted => add(Action::AckPersist(key), &|s| {
                    s.acks[key] = true;
                    s.writes[key] = Write::Done;
                }),
                Write::Unissued | Write::Done | Write::Rejected => {}
            }
        }
        if self.primary == 0 && !self.sealed && !self.expired && self.command == Command::None {
            for key in 0..2 {
                if self.writes[key] == Write::Done {
                    add(Action::DispatchAppend(key), &|s| {
                        s.command = Command::Sent;
                        s.selected = Some(key);
                        s.next_window_epoch = s.current_epoch;
                    });
                }
            }
        }
        match self.command {
            Command::Sent if self.primary == 0 && !self.expired && !self.sealed => {
                add(Action::Install, &|s| s.command = Command::Pending);
            }
            Command::Pending if self.primary == 0 && !self.expired && !self.sealed => {
                add(Action::MarkStarted, &|s| s.command = Command::Started);
            }
            Command::Started => add(Action::ApplyWitness, &|s| {
                s.command = Command::WitnessApplied;
                s.references[1] = s.selected;
                s.counters[1] = s.selected.unwrap() as u8 + 1;
                if fault == Fault::ForgetPriorWindow {
                    s.windows[1] = NEXT_WINDOW;
                } else {
                    s.windows[1] |= NEXT_WINDOW;
                }
            }),
            Command::WitnessApplied if self.primary == 0 => add(Action::ApplyPrimary, &|s| {
                s.command = Command::BothApplied;
                s.references[0] = s.selected;
                s.windows[0] |= NEXT_WINDOW;
            }),
            Command::BothApplied if self.primary == 0 => {
                add(Action::Settle, &|s| s.command = Command::Settled);
            }
            _ => {}
        }
        if self.primary == 0 && matches!(self.command, Command::Sent | Command::Pending) {
            // Assumes the authoritative unstarted certificate used by the
            // existing recovery protocol; never inferred by a failed-over node.
            add(Action::AbandonUnstarted, &|s| {
                s.command = Command::Abandoned;
                s.sealed = true;
            });
        }
        if !self.reply_lost && !matches!(self.command, Command::None | Command::Abandoned) {
            add(Action::LoseReply, &|s| s.reply_lost = true);
        }
        if self.primary == 0 {
            // Abstract fresh-publication fencing, not revocation of the initial
            // delegated window. Existing replica metadata/floor are untouched.
            add(Action::Failover, &|s| s.primary = 1);
        }
        if self.current_epoch == 0 {
            add(Action::AdvancePlacementEpoch, &|s| s.current_epoch = 1);
        }
        if !self.expired {
            add(Action::Expire, &|s| s.expired = true);
        }
        if !self.sealed
            && !self.part_published
            && (!self.command.unresolved() || fault == Fault::ForgetPendingProtection)
        {
            // Abstract converged abort: retain windows while removing staged refs.
            add(Action::Cancel, &|s| {
                s.sealed = true;
                s.references = [None; 2];
            });
        }
        if self.command == Command::Settled && !self.sealed {
            add(Action::FinalizePart, &|s| {
                s.part_published = true;
                s.sealed = true;
            });
        }
        if self.part_published && !self.replaced {
            add(Action::ReplacePart, &|s| {
                s.replaced = true;
                s.references = [None; 2];
            });
        }
        let file_drained =
            fault == Fault::IgnoreFileIo || !self.writes.contains(&Write::FileAdmitted);
        let ack_drained = fault == Fault::IgnoreAckIo || !self.writes.contains(&Write::AckAdmitted);
        if self.sealed
            && !self.cleaned
            && self.references == [None; 2]
            && (self.expired || fault == Fault::DeleteBeforeCutoff)
            && file_drained
            && ack_drained
            && (!self.command.unresolved() || fault == Fault::ForgetPendingProtection)
        {
            let discovered = self.discoverable(fault);
            add(Action::Cleanup, &|s| {
                for key in 0..KEYS {
                    if discovered & (1 << key) != 0 {
                        s.files[key] = false;
                        s.acks[key] = false;
                    }
                }
                s.cleaned = true;
            });
        }
        out
    }

    fn check(&self, fault: Fault) -> Checks {
        let failure = if self.duplicate_allocation {
            Some("failover must not recycle an uncertain identity")
        } else if self.issued & !self.discoverable(fault) != 0 {
            Some("surviving durable evidence covers every issued identity and placement")
        } else if self.selected.is_some_and(|key| {
            let protected = matches!(
                self.command,
                Command::Pending
                    | Command::Started
                    | Command::WitnessApplied
                    | Command::BothApplied
            ) || self.references.contains(&Some(key));
            protected && (!self.files[key] || !self.acks[key])
        }) {
            Some("pending or published payload remains intact")
        } else if self.cleaned && (self.files.contains(&true) || self.acks.contains(&true)) {
            Some("completed cleanup cannot leave or recreate payload")
        } else {
            None
        };
        let mut witnesses = Vec::new();
        if self.primary == 1
            && self.issued & FIRST_WINDOW == FIRST_WINDOW
            && self.counters[1] == 0
            && self.cleaned
        {
            witnesses.push(
                "replacement cleans both locally prepared identities from the durable window",
            );
        }
        if self.counters[0] == 2 && self.command == Command::None && !self.expired && !self.sealed {
            witnesses.push("initial retry window is exhausted without expanding authority");
        }
        if self.reply_lost && self.writes[2] == Write::Done && self.command == Command::Settled {
            witnesses.push("lost append reply retains the next window");
        }
        if self.primary == 1 && self.command == Command::WitnessApplied && self.expired {
            witnesses.push("partial publication remains protected after failover and expiry");
        }
        if self.replaced && self.cleaned && self.issued == FIRST_WINDOW | NEXT_WINDOW {
            witnesses.push("replacement cleans old retries and the speculative next window");
        }
        if self.cleaned && self.writes.contains(&Write::Rejected) {
            witnesses.push("late physical delivery rejects after cleanup");
        }
        if self.primary == 1 && self.current_epoch == 1 && self.cleaned && self.issued != 0 {
            witnesses.push("replacement discovers retained placement after route advance");
        }
        if self.cleaned && self.late_old_primary_allocation {
            witnesses.push("stale issuer allocations remain inside discoverable durable scope");
        }
        Checks { failure, witnesses }
    }
}

fn run(fault: Fault) -> Exploration<State, Action> {
    explore(
        [State::default()],
        &[
            "replacement cleans both locally prepared identities from the durable window",
            "initial retry window is exhausted without expanding authority",
            "lost append reply retains the next window",
            "partial publication remains protected after failover and expiry",
            "replacement cleans old retries and the speculative next window",
            "late physical delivery rejects after cleanup",
            "replacement discovers retained placement after route advance",
            "stale issuer allocations remain inside discoverable durable scope",
        ],
        |s| s.successors(fault),
        |s| s.check(fault),
        100_000,
    )
}

fn step(state: State, action: Action) -> State {
    let next = state
        .successors(Fault::None)
        .into_iter()
        .find_map(|(candidate, next)| (candidate == action).then_some(next))
        .unwrap_or_else(|| panic!("disabled {action:?}: {state:?}"));
    assert_eq!(
        next.check(Fault::None).failure,
        None,
        "after {action:?}: {next:?}"
    );
    next
}

fn enabled(state: &State, action: Action) -> bool {
    state
        .successors(Fault::None)
        .iter()
        .any(|(candidate, _)| *candidate == action)
}

#[test]
fn cleanup_lookahead_model_exhausts_replica_discovery_and_write_interleavings() {
    let result = run(Fault::None);
    result.assert_complete();
    println!(
        "{} states, {} transitions",
        result.state_count(),
        result.transitions
    );
}

#[test]
fn cleanup_lookahead_model_detects_independent_boundary_omissions() {
    for (fault, property) in [
        (
            Fault::ReuseReplicaCounter,
            "failover must not recycle an uncertain identity",
        ),
        (
            Fault::AllocatePastWindow,
            "surviving durable evidence covers every issued identity and placement",
        ),
        (
            Fault::IssueBeforeReplication,
            "surviving durable evidence covers every issued identity and placement",
        ),
        (
            Fault::ForgetPriorWindow,
            "surviving durable evidence covers every issued identity and placement",
        ),
        (
            Fault::ForgetPendingProtection,
            "pending or published payload remains intact",
        ),
        (
            Fault::IgnoreFileIo,
            "completed cleanup cannot leave or recreate payload",
        ),
        (
            Fault::IgnoreAckIo,
            "completed cleanup cannot leave or recreate payload",
        ),
        (
            Fault::DiscoverAtCurrentRoute,
            "surviving durable evidence covers every issued identity and placement",
        ),
        (
            Fault::DeleteBeforeCutoff,
            "completed cleanup cannot leave or recreate payload",
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
fn cleanup_lookahead_model_pins_exhaustion_and_uncertain_failover() {
    let mut s = State::default();
    s = step(s, Action::Prepare(0));
    s = step(s, Action::Prepare(1));
    assert!(!enabled(&s, Action::Prepare(2)));
    s = step(s, Action::Failover);
    assert_eq!(s.counters, [2, 0]);
    assert_eq!(s.discoverable(Fault::None), FIRST_WINDOW);
    assert!(!enabled(&s, Action::Prepare(0)));
    assert!(!enabled(&s, Action::Prepare(1)));
    for action in [
        Action::Cancel,
        Action::AdvancePlacementEpoch,
        Action::Expire,
        Action::Cleanup,
        Action::FileSend(0),
        Action::FileReject(0),
        Action::FileSend(1),
        Action::FileReject(1),
    ] {
        s = step(s, action);
    }
    assert!(s.cleaned && !s.files.contains(&true) && !s.acks.contains(&true));

    let mut stale = State::default();
    for action in [
        Action::Failover,
        Action::Cancel,
        Action::LatePrepareAtOldPrimary(0),
        Action::FileSend(0),
        Action::FileAdmit(0),
        Action::FilePersist(0),
        Action::Expire,
        Action::Cleanup,
        Action::AckSend(0),
        Action::AckReject(0),
    ] {
        stale = step(stale, action);
    }
    assert_eq!(stale.counters, [1, 0]);
    assert!(stale.late_old_primary_allocation && stale.cleaned);
    assert!(!stale.files.contains(&true) && !stale.acks.contains(&true));
}

#[test]
fn cleanup_lookahead_model_pins_replacement_with_prior_retry_and_future_window() {
    let mut s = completed_first_write();
    for action in [
        Action::Prepare(1),
        Action::FileSend(1),
        Action::FileAdmit(1),
        Action::FilePersist(1),
        Action::DispatchAppend(0),
        Action::Install,
        Action::MarkStarted,
        Action::ApplyWitness,
        Action::ApplyPrimary,
        Action::LoseReply,
        Action::Settle,
        Action::Prepare(2),
        Action::FileSend(2),
        Action::FinalizePart,
        Action::Expire,
    ] {
        s = step(s, action);
    }
    assert_eq!(s.discoverable(Fault::None), FIRST_WINDOW | NEXT_WINDOW);
    assert_eq!(s.files, [true, true, false]);
    assert_eq!(s.acks, [true, false, false]);
    assert!(
        !enabled(&s, Action::Cleanup),
        "live part still owns the selected payload"
    );
    for action in [
        Action::ReplacePart,
        Action::Failover,
        Action::AdvancePlacementEpoch,
        Action::Cleanup,
        Action::AckSend(1),
        Action::AckReject(1),
        Action::FileReject(2),
    ] {
        s = step(s, action);
    }
    assert!(s.cleaned && s.replaced);
    assert_eq!(s.files, [false; KEYS]);
    assert_eq!(s.acks, [false; KEYS]);
}

fn completed_first_write() -> State {
    let mut s = State::default();
    for action in [
        Action::Prepare(0),
        Action::FileSend(0),
        Action::FileAdmit(0),
        Action::FilePersist(0),
        Action::AckSend(0),
        Action::AckAdmit(0),
        Action::AckPersist(0),
    ] {
        s = step(s, action);
    }
    s
}

#[test]
fn cleanup_lookahead_model_pins_partial_append_and_independent_effect_guards() {
    let mut partial = completed_first_write();
    for action in [
        Action::DispatchAppend(0),
        Action::Install,
        Action::MarkStarted,
        Action::ApplyWitness,
        Action::LoseReply,
        Action::Failover,
        Action::Expire,
    ] {
        partial = step(partial, action);
    }
    assert_eq!(partial.windows, [FIRST_WINDOW, FIRST_WINDOW | NEXT_WINDOW]);
    assert!(!enabled(&partial, Action::AbandonUnstarted));
    assert!(!enabled(&partial, Action::Cancel));
    assert!(!enabled(&partial, Action::Cleanup));

    let mut uncleared = completed_first_write();
    for action in [
        Action::DispatchAppend(0),
        Action::Install,
        Action::MarkStarted,
        Action::ApplyWitness,
        Action::ApplyPrimary,
        Action::Failover,
        Action::Expire,
    ] {
        uncleared = step(uncleared, action);
    }
    // The ghost fact that both replicas applied is not a replacement leader's
    // certificate to clear an inaccessible old-primary slot.
    assert!(!enabled(&uncleared, Action::Settle));
    assert!(!enabled(&uncleared, Action::Cancel));
    assert!(!enabled(&uncleared, Action::Cleanup));

    for ack in [false, true] {
        let mut s = State::default();
        for action in [
            Action::Prepare(0),
            Action::FileSend(0),
            Action::FileAdmit(0),
        ] {
            s = step(s, action);
        }
        if ack {
            for action in [
                Action::FilePersist(0),
                Action::AckSend(0),
                Action::AckAdmit(0),
            ] {
                s = step(s, action);
            }
        }
        s = step(s, Action::Cancel);
        s = step(s, Action::Expire);
        assert!(!enabled(&s, Action::Cleanup));
        s = step(
            s,
            if ack {
                Action::AckPersist(0)
            } else {
                Action::FilePersist(0)
            },
        );
        s = step(s, Action::Cleanup);
        if !ack {
            s = step(s, Action::AckSend(0));
            s = step(s, Action::AckReject(0));
        }
        assert!(!s.files.contains(&true) && !s.acks.contains(&true));
    }
}
