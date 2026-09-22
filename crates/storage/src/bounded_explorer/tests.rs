// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;

// All states deliberately collide. Eq, not a fingerprint, must determine identity.
#[derive(Debug, PartialEq, Eq)]
struct Collision(u8);
impl Hash for Collision {
    fn hash<H: std::hash::Hasher>(&self, hasher: &mut H) {
        0_u8.hash(hasher);
    }
}

#[test]
fn collisions_cycles_and_diamond_joins_preserve_exact_states() {
    let result = explore(
        [Collision(0), Collision(0)],
        &["joined"],
        |state| match state.0 {
            0 => vec![(1, Collision(1)), (2, Collision(2))],
            1 | 2 => vec![(3, Collision(3))],
            3 => vec![(0, Collision(0))],
            _ => unreachable!(),
        },
        |state| Checks {
            witnesses: if state.0 == 3 { vec!["joined"] } else { vec![] },
            ..Checks::default()
        },
        4,
    );
    result.assert_complete();
    assert_eq!(result.state_count(), 4);
    assert_eq!(result.transitions, 5);
    assert_eq!(result.witness("joined"), Some(vec![1, 3]));
    assert_eq!(result.witness("absent"), None);
}

#[test]
fn witnesses_do_not_stop_search_and_terminal_failures_have_shortest_traces() {
    let result = explore(
        [0],
        &["found"],
        |state| match state {
            0 => vec![(1, 1), (3, 3)],
            1 => vec![(2, 2)],
            2 => vec![(4, 4)],
            3 => vec![(4, 4)],
            4 => vec![],
            _ => unreachable!(),
        },
        |state| Checks {
            failure: (*state == 4).then_some("terminal"),
            witnesses: vec!["found"],
        },
        5,
    );
    assert!(matches!(
        result.termination,
        Termination::Violation {
            property: "terminal",
            ..
        }
    ));
    assert_eq!(result.counterexample(), Some(vec![3, 4]));
    assert_eq!(result.witness("found"), Some(vec![]));
    assert_eq!(result.checked, 5);
}

#[test]
fn every_initial_and_terminal_state_is_checked() {
    let result = explore(
        [0, 1],
        &[],
        |_| Vec::<((), i32)>::new(),
        |state| Checks {
            failure: (*state == 1).then_some("bad root"),
            ..Checks::default()
        },
        2,
    );
    assert_eq!(result.counterexample(), Some(vec![]));
    assert_eq!(result.checked, 2);
    let complete = explore(
        [0],
        &[],
        |_| Vec::<((), i32)>::new(),
        |_| Checks::default(),
        1,
    );
    complete.assert_complete();
}

#[test]
fn limits_are_incomplete_even_after_witness_discovery() {
    for limit in [0, 1, 2] {
        let result = explore(
            [0],
            &["found"],
            |state| vec![((), state + 1)],
            |_| Checks {
                witnesses: vec!["found"],
                ..Checks::default()
            },
            limit,
        );
        assert_eq!(result.termination, Termination::StateLimit { limit });
        assert_eq!(result.state_count(), limit);
    }
    let result = explore(
        [0, 1],
        &[],
        |_| Vec::<((), i32)>::new(),
        |_| Checks::default(),
        1,
    );
    assert_eq!(result.termination, Termination::StateLimit { limit: 1 });
}

#[test]
fn remaining_budget_is_part_of_state_and_boundary_states_are_checked() {
    let result = explore(
        [(0, 0), (0, 1)],
        &["boundary"],
        |&(value, budget)| {
            if budget == 0 {
                vec![]
            } else {
                vec![((), (value + 1, budget - 1))]
            }
        },
        |&(value, _)| Checks {
            witnesses: if value == 1 { vec!["boundary"] } else { vec![] },
            ..Checks::default()
        },
        3,
    );
    result.assert_complete();
    assert_eq!(result.state_count(), 3);
    assert_eq!(result.witness("boundary"), Some(vec![()]));
}

#[test]
#[should_panic(expected = "model must have an initial state")]
fn empty_initial_set_is_not_vacuous_success() {
    explore(
        Vec::<i32>::new(),
        &[],
        |_| Vec::<((), i32)>::new(),
        |_| Checks::default(),
        1,
    );
}

#[test]
#[should_panic(expected = "exploration did not complete safely")]
fn capacity_exhaustion_cannot_pass_completion_gate() {
    explore(
        [0],
        &[],
        |state| vec![((), state + 1)],
        |_| Checks::default(),
        1,
    )
    .assert_complete();
}

#[test]
#[should_panic(expected = "required reachability witnesses not found: [\"unreachable\"]")]
fn declared_but_unreachable_witness_fails_completion() {
    let result = explore(
        [0],
        &["reached", "unreachable"],
        |_| Vec::<((), i32)>::new(),
        |_| Checks {
            witnesses: vec!["reached"],
            ..Checks::default()
        },
        1,
    );
    assert_eq!(result.termination, Termination::Complete);
    assert_eq!(result.checked, result.state_count());
    assert_eq!(result.witness("reached"), Some(vec![]));
    assert_eq!(result.witness("unreachable"), None);
    result.assert_complete();
}

#[test]
#[should_panic(expected = "undeclared reachability witness: typo")]
fn undeclared_witness_is_a_model_error() {
    explore(
        [0],
        &["expected"],
        |_| Vec::<((), i32)>::new(),
        |_| Checks {
            witnesses: vec!["typo"],
            ..Checks::default()
        },
        1,
    );
}

#[test]
#[should_panic(expected = "duplicate required witness")]
fn duplicate_required_witness_is_a_model_error() {
    explore(
        [0],
        &["duplicate", "duplicate"],
        |_| Vec::<((), i32)>::new(),
        |_| Checks::default(),
        1,
    );
}
