// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Temporary bridge for comparing engines with identical model semantics.
//! The explorer itself has no Stateright dependency. Remove this bridge when
//! selecting an engine; ownership/oracles remain in the two model modules.

use super::{explore, Checks, Exploration};
use stateright::{Expectation, Model};
use std::collections::BTreeSet;

pub(crate) fn explore_stateright_model<M>(
    model: &M,
    limit: usize,
) -> Exploration<M::State, M::Action>
where
    M: Model,
    M::State: Eq + std::hash::Hash,
    M::Action: Clone,
{
    let properties = model.properties();
    let mut names = BTreeSet::new();
    for property in &properties {
        assert!(names.insert(property.name), "duplicate property name");
        assert!(
            matches!(
                property.expectation,
                Expectation::Always | Expectation::Sometimes
            ),
            "local explorer supports safety/reachability only"
        );
    }
    explore(
        model.init_states(),
        &properties
            .iter()
            .filter(|property| property.expectation == Expectation::Sometimes)
            .map(|property| property.name)
            .collect::<Vec<_>>(),
        |state| {
            let mut actions = Vec::new();
            model.actions(state, &mut actions);
            actions
                .into_iter()
                .filter_map(|action| {
                    model.next_state(state, action.clone()).map(|next| {
                        assert!(
                            model.within_boundary(&next),
                            "express model bounds in enabled actions"
                        );
                        (action, next)
                    })
                })
                .collect()
        },
        |state| {
            let mut checks = Checks::default();
            for property in &properties {
                let holds = (property.condition)(model, state);
                match property.expectation {
                    Expectation::Always if !holds => {
                        checks.failure.get_or_insert(property.name);
                    }
                    Expectation::Sometimes if holds => checks.witnesses.push(property.name),
                    Expectation::Always | Expectation::Sometimes => {}
                    Expectation::Eventually => unreachable!("validated above"),
                }
            }
            checks
        },
        limit,
    )
}

#[test]
#[should_panic(expected = "required reachability witnesses not found: [\"unreachable\"]")]
fn comparison_registers_required_reachability_properties() {
    struct Unreachable;
    impl Model for Unreachable {
        type State = ();
        type Action = ();
        fn init_states(&self) -> Vec<()> {
            vec![()]
        }
        fn actions(&self, _: &(), _: &mut Vec<()>) {}
        fn next_state(&self, _: &(), _: ()) -> Option<()> {
            None
        }
        fn properties(&self) -> Vec<stateright::Property<Self>> {
            vec![stateright::Property::sometimes("unreachable", |_, _| false)]
        }
    }
    explore_stateright_model(&Unreachable, 1).assert_complete();
}
