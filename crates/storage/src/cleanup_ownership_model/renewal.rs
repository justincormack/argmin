// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Independent cutoff/owner-renewal model, not a production clock adapter.
//! One object-PG owner and one data node; no pending/committed command exists.
//! Durable grant/cancellation decisions are serialized atomic actions. Clock
//! faults are detected at effect admission; trusted resynchronization is an
//! explicit environmental assumption, not a protocol implemented by this model.

use super::Write;
use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

const BASE: u64 = 10;
const INITIAL_CUTOFF: u64 = 14;
const RENEWED_CUTOFF: u64 = 17;
const SKEW: u64 = 2;
const LAST_TICK: u64 = 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    IssueBeforePersistence,
    LoseHorizonOnRestart,
    UseOriginalCleanupCutoff,
    RenewAfterCancellation,
    IgnoreClockHealth,
    TrustClockAfterRestart,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Stage {
    Open,
    Cancelled,
    Claimed,
    Deleted,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum ClockHealth {
    Healthy,
    Unhealthy,
    Unbound,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    stage: Stage,
    // Durable maximum of all cutoffs that may be issued, including lost replies.
    horizon: u64,
    cancelled_horizon: Option<u64>,
    // Ghost transport history is retained for invariant checking across restart.
    renewal_issued: bool,
    writes: [Write; 2],
    file: bool,
    ack: bool,
    tick: u64,
    cleaner_offset: i8,
    health: ClockHealth,
    jumped: bool,
    corrected: bool,
    resynchronized: bool,
    crashed: bool,
    offline: bool,
    restarted: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            stage: Stage::Open,
            horizon: INITIAL_CUTOFF,
            cancelled_horizon: None,
            renewal_issued: false,
            writes: [Write::Sent, Write::Unsent],
            file: false,
            ack: false,
            tick: 0,
            cleaner_offset: 1,
            health: ClockHealth::Healthy,
            jumped: false,
            corrected: false,
            resynchronized: false,
            crashed: false,
            offline: false,
            restarted: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Tick,
    ClockJump,
    CorrectWall,
    Resynchronize,
    Crash,
    Restart,
    PersistRenewal,
    IssueRenewal,
    DeliverRenewal,
    Cancel,
    Admit(usize),
    Reject(usize),
    File(usize),
    Ack(usize),
    Finish(usize),
    Claim,
    Delete,
}

impl State {
    fn node_wall(&self) -> u64 {
        BASE + self.tick - 1
    }

    fn successors(&self, jump: i8, fault: Fault) -> Vec<(Action, Self)> {
        let mut out = Vec::new();
        let mut add = |action, change: fn(&mut Self)| {
            let mut next = self.clone();
            change(&mut next);
            out.push((action, next));
        };
        if self.tick < LAST_TICK {
            add(Action::Tick, |s| s.tick += 1);
        }
        if !self.crashed {
            add(Action::Crash, |s| {
                s.crashed = true;
                s.offline = true;
                s.health = ClockHealth::Unbound;
            });
        }
        // The data node is independent of the owner process. Already dispatched
        // writes remain executable during owner downtime and after cancellation.
        for (index, cutoff) in [INITIAL_CUTOFF, RENEWED_CUTOFF].into_iter().enumerate() {
            let mut next = self.clone();
            let action = match self.writes[index] {
                Write::Sent if self.node_wall() < cutoff => {
                    next.writes[index] = Write::Admitted;
                    Action::Admit(index)
                }
                Write::Sent => {
                    next.writes[index] = Write::Rejected;
                    Action::Reject(index)
                }
                Write::Admitted => {
                    next.file = true;
                    next.writes[index] = Write::FilePublished;
                    Action::File(index)
                }
                Write::FilePublished => {
                    next.ack = true;
                    next.writes[index] = Write::AckPersisted;
                    Action::Ack(index)
                }
                Write::AckPersisted => {
                    next.writes[index] = Write::Finished;
                    Action::Finish(index)
                }
                _ => continue,
            };
            out.push((action, next));
        }
        if self.offline {
            let mut next = self.clone();
            next.offline = false;
            next.restarted = true;
            if fault == Fault::LoseHorizonOnRestart {
                next.horizon = INITIAL_CUTOFF;
            }
            if fault == Fault::TrustClockAfterRestart {
                next.health = ClockHealth::Healthy;
            }
            out.push((Action::Restart, next));
            return out;
        }
        if !self.jumped {
            let mut next = self.clone();
            next.jumped = true;
            next.cleaner_offset += jump;
            next.health = ClockHealth::Unhealthy;
            out.push((Action::ClockJump, next));
        }
        if self.jumped && !self.corrected {
            let mut next = self.clone();
            next.corrected = true;
            next.cleaner_offset = 1;
            // Correcting wall time does not clear the health latch.
            out.push((Action::CorrectWall, next));
        }
        if self.health != ClockHealth::Healthy && !self.resynchronized {
            let mut next = self.clone();
            next.resynchronized = true;
            next.health = ClockHealth::Healthy;
            next.cleaner_offset = 1;
            // Assume fresh trusted synchronization restores the declared skew
            // bound, without replacing the durable horizon or issued cutoffs.
            out.push((Action::Resynchronize, next));
        }
        let clock_usable = self.health == ClockHealth::Healthy || fault == Fault::IgnoreClockHealth;
        let cleaner_wall = (BASE + self.tick)
            .checked_add_signed(i64::from(self.cleaner_offset))
            .unwrap();
        if self.horizon == INITIAL_CUTOFF
            && clock_usable
            && cleaner_wall < INITIAL_CUTOFF
            && (self.stage == Stage::Open || fault == Fault::RenewAfterCancellation)
        {
            let mut next = self.clone();
            next.horizon = RENEWED_CUTOFF;
            out.push((Action::PersistRenewal, next));
        }
        if !self.renewal_issued
            && (self.horizon == RENEWED_CUTOFF || fault == Fault::IssueBeforePersistence)
        {
            let mut next = self.clone();
            next.renewal_issued = true;
            // A persisted grant may have a delayed reply after cancellation.
            out.push((Action::IssueRenewal, next));
        }
        if self.renewal_issued && self.writes[1] == Write::Unsent {
            let mut next = self.clone();
            next.writes[1] = Write::Sent;
            out.push((Action::DeliverRenewal, next));
        }
        if self.stage == Stage::Open {
            let mut next = self.clone();
            next.stage = Stage::Cancelled;
            next.cancelled_horizon = Some(self.horizon);
            out.push((Action::Cancel, next));
        }
        let cleanup_cutoff = if fault == Fault::UseOriginalCleanupCutoff {
            INITIAL_CUTOFF
        } else {
            self.horizon
        };
        if self.stage == Stage::Cancelled
            && clock_usable
            && cleaner_wall >= cleanup_cutoff + SKEW
            && !self.writes.iter().any(|write| write.holds_exclusion())
        {
            let mut next = self.clone();
            next.stage = Stage::Claimed;
            out.push((Action::Claim, next));
        }
        if self.stage == Stage::Claimed {
            let mut next = self.clone();
            next.stage = Stage::Deleted;
            next.file = false;
            next.ack = false;
            out.push((Action::Delete, next));
        }
        out
    }

    fn check(&self) -> Checks {
        let failure = if self.renewal_issued && self.horizon < RENEWED_CUTOFF {
            Some("every issued cutoff must remain durably covered")
        } else if self.cancelled_horizon.is_some_and(|h| self.horizon > h) {
            Some("cancellation must close horizon extension")
        } else if matches!(self.stage, Stage::Claimed | Stage::Deleted)
            && self.node_wall() < self.horizon
        {
            // Independent receiver-clock oracle; does not reuse the cleaner
            // health flag, its wall time, or its choice of cleanup cutoff.
            Some("cleanup cannot precede the receiver horizon")
        } else if self.stage == Stage::Deleted && (self.file || self.ack) {
            Some("deleted payload must not reappear")
        } else {
            None
        };
        let mut witnesses = Vec::new();
        if self.stage == Stage::Cancelled && self.writes[1] == Write::Sent {
            witnesses.push("renewal reply can arrive after cancellation");
        }
        if self.stage == Stage::Deleted && self.horizon == RENEWED_CUTOFF && !self.renewal_issued {
            witnesses.push("lost renewal reply still delays cleanup");
        }
        if self.stage == Stage::Deleted && self.restarted && self.resynchronized && self.jumped {
            witnesses.push("cleanup can finish after restart and clock recovery");
        }
        Checks { failure, witnesses }
    }
}

fn run(jump: i8, fault: Fault) -> Exploration<State, Action> {
    explore(
        [State::default()],
        &[
            "renewal reply can arrive after cancellation",
            "lost renewal reply still delays cleanup",
            "cleanup can finish after restart and clock recovery",
        ],
        |s| s.successors(jump, fault),
        State::check,
        750_000,
    )
}

fn step(state: State, action: Action) -> State {
    let next = state
        .successors(8, Fault::None)
        .into_iter()
        .find_map(|(candidate, next)| (candidate == action).then_some(next))
        .unwrap_or_else(|| panic!("disabled {action:?}: {state:?}"));
    assert_eq!(next.check().failure, None, "after {action:?}: {next:?}");
    next
}

fn enabled(state: &State, action: Action) -> bool {
    state
        .successors(8, Fault::None)
        .iter()
        .any(|(a, _)| *a == action)
}

#[test]
fn cleanup_renewal_model_exhausts_clock_fault_and_owner_renewal_orderings() {
    for jump in [-8, 8] {
        let result = run(jump, Fault::None);
        result.assert_complete();
        println!(
            "jump={jump}: {} states, {} transitions",
            result.state_count(),
            result.transitions
        );
    }
}

#[test]
fn cleanup_renewal_model_detects_independent_horizon_and_clock_errors() {
    for (fault, expected) in [
        (
            Fault::IssueBeforePersistence,
            "every issued cutoff must remain durably covered",
        ),
        (
            Fault::LoseHorizonOnRestart,
            "every issued cutoff must remain durably covered",
        ),
        (
            Fault::UseOriginalCleanupCutoff,
            "cleanup cannot precede the receiver horizon",
        ),
        (
            Fault::RenewAfterCancellation,
            "cancellation must close horizon extension",
        ),
        (
            Fault::IgnoreClockHealth,
            "cleanup cannot precede the receiver horizon",
        ),
        (
            Fault::TrustClockAfterRestart,
            "cleanup cannot precede the receiver horizon",
        ),
    ] {
        let result = run(8, fault);
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
fn cleanup_renewal_model_pins_lost_reply_and_immutable_old_write() {
    let mut s = step(State::default(), Action::PersistRenewal);
    s = step(s, Action::Cancel);
    assert!(!enabled(&s, Action::PersistRenewal));
    for _ in 0..5 {
        s = step(s, Action::Tick);
    }
    assert!(
        enabled(&s, Action::Reject(0)),
        "renewal cannot extend old writes"
    );
    assert!(
        !enabled(&s, Action::Claim),
        "lost reply does not undo the grant"
    );
    s = step(s, Action::IssueRenewal);
    s = step(s, Action::DeliverRenewal);
    s = step(s, Action::Admit(1));
    for _ in 0..3 {
        s = step(s, Action::Tick);
    }
    assert!(
        !enabled(&s, Action::Claim),
        "expiry must still drain admitted IO"
    );
    for action in [
        Action::File(1),
        Action::Ack(1),
        Action::Finish(1),
        Action::Claim,
        Action::Delete,
    ] {
        s = step(s, action);
    }
    assert_eq!(s.stage, Stage::Deleted);
}

#[test]
fn cleanup_renewal_model_pins_clock_latch_and_restart_reauthorization() {
    let open = step(State::default(), Action::ClockJump);
    let open = step(open, Action::CorrectWall);
    assert_eq!(open.stage, Stage::Open);
    assert_eq!(open.tick, 0);
    assert!(
        !enabled(&open, Action::PersistRenewal),
        "the clock latch must fence renewal even before the original cutoff"
    );
    let open = step(open, Action::Resynchronize);
    let open = step(open, Action::PersistRenewal);
    assert_eq!(open.horizon, RENEWED_CUTOFF);

    let mut s = step(State::default(), Action::Cancel);
    s = step(s, Action::ClockJump);
    assert!(!enabled(&s, Action::Claim));
    s = step(s, Action::CorrectWall);
    for _ in 0..8 {
        s = step(s, Action::Tick);
    }
    assert!(
        !enabled(&s, Action::Claim),
        "wall correction cannot clear the latch"
    );
    s = step(s, Action::Crash);
    s = step(s, Action::Restart);
    assert!(
        !enabled(&s, Action::Claim),
        "restart is not trusted clock evidence"
    );
    s = step(s, Action::Resynchronize);
    s = step(s, Action::Claim);
    s = step(s, Action::Delete);
    assert_eq!(s.stage, Stage::Deleted);
    assert_eq!(s.horizon, INITIAL_CUTOFF);
}
