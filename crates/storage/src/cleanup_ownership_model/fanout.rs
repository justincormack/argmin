// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Abstract data-node fanout/clock model, not a production RPC adapter.
//! Starts after cancellation with an owner and one dispatched write per node.
//! Object-PG command serialization is covered separately by the parent model.

use super::{Protocol, Write};
use crate::bounded_explorer::{explore, Checks, Exploration, Termination};

const WALL_BASE: u64 = 10;
const WRITE_CUTOFF: u64 = 14;
const SKEW: u64 = 2;
const LAST_TICK: u64 = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    OmitCleanupSkew,
    AcceptPartialClosure,
    BorrowRenewedDeadline,
    RestartDurationOnReceipt,
    ForgetDurableRevocation,
    RetireBeforeDeleteReply,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Stage {
    Waiting,
    Closing,
    Deleting,
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Node {
    write: Write,
    bound_deadline: Option<u64>,
    file: bool,
    ack: bool,
    revoked: bool,
    barrier: bool,
    barrier_received: bool,
    deleted: bool,
    delete_received: bool,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            write: Write::Sent,
            bound_deadline: None,
            file: false,
            ack: false,
            revoked: false,
            barrier: false,
            barrier_received: false,
            deleted: false,
            delete_received: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct State {
    nodes: [Node; 2],
    stage: Stage,
    tick: u64,
    raw_route_renewed: bool,
    // Node 1 can fail and restart once, without losing durable state.
    offline: bool,
    crashed: bool,
    restarted: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            nodes: std::array::from_fn(|_| Node::default()),
            stage: Stage::Waiting,
            tick: 0,
            raw_route_renewed: false,
            offline: false,
            crashed: false,
            restarted: false,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Clocks {
    // Fixed healthy offsets; every pair differs by at most SKEW.
    cleaner: i8,
    nodes: [i8; 2],
}

impl Clocks {
    fn wall(self, tick: u64, offset: i8) -> u64 {
        (WALL_BASE + tick)
            .checked_add_signed(i64::from(offset))
            .unwrap()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Tick,
    RenewRawRoute,
    Bind(usize),
    Admit(usize),
    Reject(usize),
    File(usize),
    Ack(usize),
    FinishWrite(usize),
    StartClosure,
    Revoke(usize),
    Drain(usize),
    ReceiveBarrier(usize),
    ClaimCleanup,
    Delete(usize),
    ReceiveDelete(usize),
    RetireOwner,
    Crash,
    Restart,
}

impl State {
    fn mono(&self, node: usize) -> u64 {
        // Deliberately unrelated origins, including a changed origin on restart.
        let origin = match (node, self.restarted) {
            (0, _) => 100,
            (1, false) => 10_000,
            (1, true) => 1_000,
            _ => unreachable!(),
        };
        origin + self.tick
    }

    fn successors(&self, protocol: Protocol, clocks: Clocks, fault: Fault) -> Vec<(Action, Self)> {
        let mut out = Vec::new();
        if self.tick < LAST_TICK {
            let mut next = self.clone();
            next.tick += 1;
            out.push((Action::Tick, next));
        }
        if !self.raw_route_renewed {
            let mut next = self.clone();
            next.raw_route_renewed = true;
            out.push((Action::RenewRawRoute, next));
        }
        if !self.crashed {
            let mut next = self.clone();
            next.crashed = true;
            next.offline = true;
            next.nodes[1].bound_deadline = None;
            if next.nodes[1].write.holds_exclusion() {
                next.nodes[1].write = Write::Rejected;
            }
            out.push((Action::Crash, next));
        }
        if self.offline {
            let mut next = self.clone();
            next.offline = false;
            next.restarted = true;
            if fault == Fault::ForgetDurableRevocation {
                next.nodes[1].revoked = false;
            }
            out.push((Action::Restart, next));
        }
        let margin = if fault == Fault::OmitCleanupSkew {
            0
        } else {
            SKEW
        };
        let expiry_proven = clocks.wall(self.tick, clocks.cleaner) >= WRITE_CUTOFF + margin;
        if self.stage == Stage::Waiting
            && (matches!(protocol, Protocol::Revocation) || expiry_proven)
        {
            let mut next = self.clone();
            next.stage = Stage::Closing;
            out.push((Action::StartClosure, next));
        }
        for index in 0..2 {
            if index == 1 && self.offline {
                continue;
            }
            let node = &self.nodes[index];
            if node.write == Write::Sent {
                let mut next = self.clone();
                if let Some(deadline) = node.bound_deadline {
                    if self.mono(index) < deadline && !node.revoked {
                        next.nodes[index].write = Write::Admitted;
                        out.push((Action::Admit(index), next));
                    } else {
                        next.nodes[index].write = Write::Rejected;
                        out.push((Action::Reject(index), next));
                    }
                } else {
                    // Absolute portable cutoff: transit consumes authority. This
                    // is abstract arithmetic, NOT the production codec or binder.
                    let cutoff = if fault == Fault::BorrowRenewedDeadline && self.raw_route_renewed
                    {
                        WRITE_CUTOFF + 4
                    } else {
                        WRITE_CUTOFF
                    };
                    let remaining = if fault == Fault::RestartDurationOnReceipt {
                        WRITE_CUTOFF - WALL_BASE
                    } else {
                        cutoff.saturating_sub(clocks.wall(self.tick, clocks.nodes[index]))
                    };
                    next.nodes[index].bound_deadline = Some(self.mono(index) + remaining);
                    out.push((Action::Bind(index), next));
                }
            }
            let mut next = self.clone();
            match node.write {
                Write::Admitted => {
                    next.nodes[index].file = true;
                    next.nodes[index].write = Write::FilePublished;
                    out.push((Action::File(index), next));
                }
                Write::FilePublished => {
                    next.nodes[index].ack = true;
                    next.nodes[index].write = Write::AckPersisted;
                    out.push((Action::Ack(index), next));
                }
                Write::AckPersisted => {
                    next.nodes[index].write = Write::Finished;
                    out.push((Action::FinishWrite(index), next));
                }
                _ => {}
            }
            if self.stage == Stage::Closing {
                if matches!(protocol, Protocol::Revocation) && !node.revoked {
                    let mut next = self.clone();
                    next.nodes[index].revoked = true;
                    out.push((Action::Revoke(index), next));
                }
                if !node.barrier
                    && !node.write.holds_exclusion()
                    && (matches!(protocol, Protocol::Cutoff) || node.revoked)
                {
                    let mut next = self.clone();
                    next.nodes[index].barrier = true;
                    out.push((Action::Drain(index), next));
                }
                if node.barrier && !node.barrier_received {
                    let mut next = self.clone();
                    next.nodes[index].barrier_received = true;
                    out.push((Action::ReceiveBarrier(index), next));
                }
            }
            if self.stage == Stage::Deleting {
                if !node.deleted {
                    let mut next = self.clone();
                    next.nodes[index].file = false;
                    next.nodes[index].ack = false;
                    next.nodes[index].deleted = true;
                    out.push((Action::Delete(index), next));
                }
                if node.deleted && !node.delete_received {
                    let mut next = self.clone();
                    next.nodes[index].delete_received = true;
                    out.push((Action::ReceiveDelete(index), next));
                }
            }
        }
        let all_closed = self.nodes.iter().all(|node| node.barrier_received);
        let some_closed = self.nodes.iter().any(|node| node.barrier_received);
        if self.stage == Stage::Closing
            && (all_closed || (fault == Fault::AcceptPartialClosure && some_closed))
        {
            let mut next = self.clone();
            next.stage = Stage::Deleting;
            out.push((Action::ClaimCleanup, next));
        }
        if self.stage == Stage::Deleting
            && self.nodes.iter().all(|node| {
                node.delete_received || (fault == Fault::RetireBeforeDeleteReply && node.deleted)
            })
        {
            let mut next = self.clone();
            next.stage = Stage::Done;
            out.push((Action::RetireOwner, next));
        }
        out
    }

    fn check(&self) -> Checks {
        let failure = if matches!(self.stage, Stage::Deleting | Stage::Done)
            && self.nodes.iter().any(|node| !node.barrier_received)
        {
            Some("all targets must acknowledge closure before cleanup")
        } else if self
            .nodes
            .iter()
            .any(|node| node.deleted && (node.file || node.ack))
        {
            Some("deleted shard must not reappear")
        } else if self.stage == Stage::Done && self.nodes.iter().any(|node| !node.delete_received) {
            Some("owner must survive ambiguous deletion")
        } else {
            None
        };
        let mut witnesses = Vec::new();
        if self.offline && self.nodes[0].barrier_received && !self.nodes[1].barrier_received {
            witnesses.push("partial closure while another target is unavailable");
        }
        if self.nodes.iter().all(|node| node.file && node.ack) {
            witnesses.push("both writes can persist");
        }
        if self.stage == Stage::Deleting
            && self
                .nodes
                .iter()
                .all(|node| node.deleted && !node.delete_received)
        {
            witnesses.push("deleted files do not imply acknowledged cleanup");
        }
        if self.stage == Stage::Done && self.restarted && self.raw_route_renewed {
            witnesses.push("cleanup completes despite restart and raw renewal");
        }
        Checks { failure, witnesses }
    }
}

const WITNESSES: &[&str] = &[
    "partial closure while another target is unavailable",
    "both writes can persist",
    "deleted files do not imply acknowledged cleanup",
    "cleanup completes despite restart and raw renewal",
];

const CLOCK_CASES: [Clocks; 3] = [
    Clocks {
        cleaner: 1,
        nodes: [-1, 0],
    },
    Clocks {
        cleaner: 1,
        nodes: [0, -1],
    },
    Clocks {
        cleaner: -1,
        nodes: [1, 0],
    },
];

fn run(protocol: Protocol, clocks: Clocks, fault: Fault) -> Exploration<State, Action> {
    explore(
        [State::default()],
        WITNESSES,
        |state| state.successors(protocol, clocks, fault),
        State::check,
        750_000,
    )
}

fn follow(protocol: Protocol, actions: &[Action]) -> State {
    let mut state = State::default();
    for action in actions {
        state = state
            .successors(protocol, CLOCK_CASES[1], Fault::None)
            .into_iter()
            .find_map(|(step, next)| (step == *action).then_some(next))
            .unwrap_or_else(|| panic!("disabled {action:?} in {state:?}"));
        assert_eq!(state.check().failure, None, "after {action:?}: {state:?}");
    }
    state
}

#[test]
fn cleanup_fanout_model_requires_all_closures_and_all_delete_receipts() {
    let partial = [
        Action::StartClosure,
        Action::Revoke(0),
        Action::Drain(0),
        Action::ReceiveBarrier(0),
        Action::Crash,
    ];
    let state = follow(Protocol::Revocation, &partial);
    assert!(state.offline);
    assert!(state.nodes[0].barrier_received);
    assert!(!state
        .successors(Protocol::Revocation, CLOCK_CASES[1], Fault::None)
        .iter()
        .any(|(action, _)| *action == Action::ClaimCleanup));

    let mut actions = partial.to_vec();
    actions.extend([
        Action::Restart,
        Action::Revoke(1),
        Action::Drain(1),
        Action::ReceiveBarrier(1),
        Action::ClaimCleanup,
        Action::Delete(0),
        Action::ReceiveDelete(0),
        Action::Delete(1),
    ]);
    let state = follow(Protocol::Revocation, &actions);
    assert_eq!(state.stage, Stage::Deleting);
    assert!(!state.nodes[1].delete_received);
    assert!(!state
        .successors(Protocol::Revocation, CLOCK_CASES[1], Fault::None)
        .iter()
        .any(|(action, _)| *action == Action::RetireOwner));

    actions.extend([
        Action::ReceiveDelete(1),
        Action::RetireOwner,
        Action::Bind(0),
        Action::Reject(0),
        Action::Bind(1),
        Action::Reject(1),
    ]);
    let state = follow(Protocol::Revocation, &actions);
    assert_eq!(state.stage, Stage::Done);
    assert_eq!(
        state.tick, 0,
        "early revocation must not rely on cutoff expiry"
    );
}

#[test]
fn cleanup_fanout_model_pins_clock_margin_binding_and_restart() {
    // The cleaner has reached the numerical cutoff, but slow node 1 has not.
    let state = follow(
        Protocol::Cutoff,
        &[
            Action::Bind(1),
            Action::Tick,
            Action::Tick,
            Action::Tick,
            Action::RenewRawRoute,
        ],
    );
    assert_eq!(
        CLOCK_CASES[1].wall(state.tick, CLOCK_CASES[1].cleaner),
        WRITE_CUTOFF
    );
    let successors = state.successors(Protocol::Cutoff, CLOCK_CASES[1], Fault::None);
    assert!(successors
        .iter()
        .any(|(action, _)| *action == Action::Admit(1)));
    assert!(!successors
        .iter()
        .any(|(action, _)| *action == Action::StartClosure));

    // Bind before expiry, then renew the raw route; it must not extend the
    // binding. After restart, a delayed frame is rebound using the same absolute
    // cutoff, never a fresh copy of its original duration or old monotonic origin.
    let state = follow(
        Protocol::Cutoff,
        &[
            Action::Bind(1),
            Action::RenewRawRoute,
            Action::Tick,
            Action::Tick,
            Action::Tick,
            Action::Tick,
            Action::Tick,
        ],
    );
    assert!(state
        .successors(Protocol::Cutoff, CLOCK_CASES[1], Fault::None)
        .iter()
        .any(|(action, _)| *action == Action::Reject(1)));
    let state = follow(
        Protocol::Cutoff,
        &[
            Action::Bind(1),
            Action::RenewRawRoute,
            Action::Tick,
            Action::Tick,
            Action::Tick,
            Action::Tick,
            Action::Tick,
            Action::StartClosure,
            Action::Drain(0),
            Action::Drain(1),
            Action::ReceiveBarrier(0),
            Action::ReceiveBarrier(1),
            Action::ClaimCleanup,
            Action::Delete(0),
            Action::Delete(1),
            Action::ReceiveDelete(0),
            Action::ReceiveDelete(1),
            Action::RetireOwner,
            Action::Crash,
            Action::Restart,
            Action::Bind(1),
            Action::Reject(1),
        ],
    );
    assert_eq!(state.stage, Stage::Done);
    assert!(!state.nodes[1].file && !state.nodes[1].ack);
    assert_eq!(state.nodes[1].bound_deadline, Some(state.mono(1)));
}

#[test]
fn cleanup_fanout_model_exhausts_partial_closure_and_clock_offsets() {
    for protocol in [Protocol::Cutoff, Protocol::Revocation] {
        for clocks in CLOCK_CASES {
            let result = run(protocol, clocks, Fault::None);
            result.assert_complete();
            println!(
                "{protocol:?}, {clocks:?}: {} states, {} transitions",
                result.state_count(),
                result.transitions
            );
        }
    }
}

#[test]
fn cleanup_fanout_model_detects_independent_authority_and_handoff_errors() {
    for (protocol, fault, property) in [
        (
            Protocol::Cutoff,
            Fault::OmitCleanupSkew,
            "deleted shard must not reappear",
        ),
        (
            Protocol::Revocation,
            Fault::AcceptPartialClosure,
            "all targets must acknowledge closure before cleanup",
        ),
        (
            Protocol::Cutoff,
            Fault::BorrowRenewedDeadline,
            "deleted shard must not reappear",
        ),
        (
            Protocol::Cutoff,
            Fault::RestartDurationOnReceipt,
            "deleted shard must not reappear",
        ),
        (
            Protocol::Revocation,
            Fault::ForgetDurableRevocation,
            "deleted shard must not reappear",
        ),
        (
            Protocol::Cutoff,
            Fault::RetireBeforeDeleteReply,
            "owner must survive ambiguous deletion",
        ),
    ] {
        let result = run(protocol, CLOCK_CASES[1], fault);
        assert!(
            matches!(result.termination, Termination::Violation { property: actual, .. } if actual == property),
            "{fault:?}: {:?}; trace={:?}",
            result.termination,
            result.counterexample()
        );
        println!("{fault:?}: {:?}", result.counterexample().unwrap());
    }
}
