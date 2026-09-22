// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Single-threaded test-only exploration of an explicitly bounded state graph.
//! State equality must include all future-relevant state, property monitors and
//! remaining budgets. No temporal logic, fairness, symmetry or hash-only merging.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Default)]
pub(crate) struct Checks {
    pub(crate) failure: Option<&'static str>,
    pub(crate) witnesses: Vec<&'static str>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Termination {
    // Graph exhausted; required reachability is checked by assert_complete().
    Complete,
    Violation { property: &'static str, node: usize },
    StateLimit { limit: usize },
}

struct Node<S, A> {
    state: Arc<S>,
    parent: Option<(usize, A)>,
}

pub(crate) struct Exploration<S, A> {
    nodes: Vec<Node<S, A>>,
    witnesses: BTreeMap<&'static str, usize>,
    required_witnesses: BTreeSet<&'static str>,
    pub(crate) termination: Termination,
    pub(crate) checked: usize,
    pub(crate) transitions: usize,
}

impl<S, A: Clone> Exploration<S, A> {
    pub(crate) fn states(&self) -> impl Iterator<Item = &S> {
        self.nodes.iter().map(|node| node.state.as_ref())
    }

    pub(crate) fn state_count(&self) -> usize {
        self.nodes.len()
    }

    fn trace(&self, mut index: usize) -> Vec<A> {
        let mut actions = Vec::new();
        while let Some((parent, action)) = &self.nodes[index].parent {
            actions.push(action.clone());
            index = *parent;
        }
        actions.reverse();
        actions
    }

    pub(crate) fn witness(&self, name: &str) -> Option<Vec<A>> {
        self.witnesses.get(name).map(|index| self.trace(*index))
    }

    pub(crate) fn counterexample(&self) -> Option<Vec<A>> {
        match self.termination {
            Termination::Violation { node, .. } => Some(self.trace(node)),
            Termination::Complete | Termination::StateLimit { .. } => None,
        }
    }
}

impl<S, A: Clone + Debug> Exploration<S, A> {
    pub(crate) fn assert_complete(&self) {
        assert_eq!(
            self.termination,
            Termination::Complete,
            "exploration did not complete safely; counterexample={:?}",
            self.counterexample()
        );
        assert_eq!(self.checked, self.nodes.len(), "unchecked states");
        let missing: Vec<_> = self
            .required_witnesses
            .iter()
            .filter(|name| !self.witnesses.contains_key(**name))
            .copied()
            .collect();
        assert!(
            missing.is_empty(),
            "required reachability witnesses not found: {missing:?}"
        );
    }
}

/// Explore all reachable states, stopping only on a violation or resource limit.
/// The model supplies finite state/action bounds; there is no hidden depth cutoff.
/// A limit result is never a successful proof, even if all witnesses were found.
/// Successor lists must themselves be finite and bounded by the model.
/// Declare every required reachability property up front. Completion assertion
/// requires all of them, independently of any caller inspecting witness traces.
pub(crate) fn explore<S: Eq + Hash, A: Clone>(
    initial: impl IntoIterator<Item = S>,
    required_witnesses: &[&'static str],
    mut successors: impl FnMut(&S) -> Vec<(A, S)>,
    mut inspect: impl FnMut(&S) -> Checks,
    max_states: usize,
) -> Exploration<S, A> {
    let required: BTreeSet<_> = required_witnesses.iter().copied().collect();
    assert_eq!(
        required.len(),
        required_witnesses.len(),
        "duplicate required witness"
    );
    let mut result = Exploration {
        nodes: Vec::new(),
        witnesses: BTreeMap::new(),
        required_witnesses: required,
        termination: Termination::Complete,
        checked: 0,
        transitions: 0,
    };
    let mut visited = HashMap::new();
    for state in initial {
        if !insert(&mut result.nodes, &mut visited, state, None, max_states) {
            result.termination = Termination::StateLimit { limit: max_states };
            return result;
        }
    }
    assert!(!result.nodes.is_empty(), "model must have an initial state");
    // Append-only nodes are a FIFO frontier and retain shortest-path parents.
    // HashMap iteration order is never used to choose actions or traces.
    let mut cursor = 0;
    while cursor < result.nodes.len() {
        let state = Arc::clone(&result.nodes[cursor].state);
        let checks = inspect(&state);
        result.checked += 1;
        if let Some(property) = checks.failure {
            result.termination = Termination::Violation {
                property,
                node: cursor,
            };
            return result;
        }
        for witness in checks.witnesses {
            assert!(
                result.required_witnesses.contains(witness),
                "undeclared reachability witness: {witness}"
            );
            result.witnesses.entry(witness).or_insert(cursor);
        }
        for (action, next) in successors(&state) {
            result.transitions += 1;
            if !insert(
                &mut result.nodes,
                &mut visited,
                next,
                Some((cursor, action)),
                max_states,
            ) {
                result.termination = Termination::StateLimit { limit: max_states };
                return result;
            }
        }
        cursor += 1;
    }
    result
}

fn insert<S: Eq + Hash, A>(
    nodes: &mut Vec<Node<S, A>>,
    visited: &mut HashMap<Arc<S>, usize>,
    state: S,
    parent: Option<(usize, A)>,
    limit: usize,
) -> bool {
    if visited.contains_key(&state) {
        return true;
    }
    if nodes.len() == limit {
        return false;
    }
    let state = Arc::new(state);
    visited.insert(Arc::clone(&state), nodes.len());
    nodes.push(Node { state, parent });
    true
}

#[cfg(test)]
mod tests;
