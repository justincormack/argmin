//! Condition operator table.
//!
//! This module hosts one definition per supported AWS IAM condition operator
//! (`StringEquals`, `StringLike`, `Null`, …). Each operator owns its own
//! semantics, including `IfExists` handling and negation, so call sites do
//! not re-enumerate the operator list. The public evaluator in
//! `super::bucket_policy` will migrate its operator dispatch onto this table
//! in later commits of the phase 1 refactor tracked by
//! `plans/bucket-policy-evaluator-structure-plan.md`.
//!
//! The existing evaluator continues to use its own operator match statements
//! until the migration commits route through [`CONDITION_OPS`].

#![allow(dead_code)]

use super::{wildcard_matches, ConditionMatchResult};

/// Value presented to a condition operator by the evaluator.
///
/// The three branches exactly mirror the states the evaluator already
/// distinguishes:
///
/// - `Present(value)`: the condition key resolved to a concrete string
/// - `Absent`: the key is known but not supplied by the request
/// - `Unavailable`: the key is known but its value cannot be determined in
///   this context (for example, object tags on a `PutObject` request)
///
/// `Unavailable` is produced and handled by the condition-key resolver, not
/// the operator, but it is carried through this type so that a future
/// resolver-driven call site can pass it opaquely.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActualValue<'a> {
    Present(&'a str),
    Absent,
    Unavailable,
}

/// The kind of values a condition operator expects on its right-hand side.
///
/// Reserved for later phases where non-string operators (`IpAddress`,
/// `NumericLessThan`, `Bool`, `DateEquals`, …) get their own parsing and
/// normalization step. String operators all share the `String` kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConditionValueKind {
    String,
}

/// Tagged discriminant for condition operators.
///
/// Kept as an explicit enum so the table can be walked by category (for
/// example, filtering supportedness predicates) without string comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConditionOpKind {
    StringEquals,
    StringNotEquals,
    StringLike,
    StringNotLike,
    Null,
}

/// One row of the condition operator table.
///
/// `name` is the on-the-wire operator string (for example `"StringEquals"`
/// or `"StringEqualsIfExists"`). Operators that accept an `IfExists` variant
/// appear as two separate rows so lookup stays a single exact-match step;
/// `if_exists` records which form the row represents.
#[derive(Debug, Clone, Copy)]
pub(super) struct ConditionOpDef {
    pub(super) name: &'static str,
    pub(super) kind: ConditionOpKind,
    pub(super) if_exists: bool,
    pub(super) value_kind: ConditionValueKind,
    pub(super) evaluate: fn(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult,
    /// Whether this operator is evaluated by the currently enforced
    /// object-action policy subset. Mirrors the existing
    /// `evaluable_string_condition_operator_supported` filter; derived from
    /// the table rather than enumerated at call sites.
    pub(super) evaluable_on_evaluable_object_actions: bool,
}

/// The compile-time operator table.
///
/// `StringNotLikeIfExists` is intentionally absent so the supportedness
/// predicate preserves the asymmetry currently encoded in
/// `evaluable_string_condition_operator_supported`.
pub(super) const CONDITION_OPS: &[ConditionOpDef] = &[
    ConditionOpDef {
        name: "StringEquals",
        kind: ConditionOpKind::StringEquals,
        if_exists: false,
        value_kind: ConditionValueKind::String,
        evaluate: eval_string_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringEqualsIfExists",
        kind: ConditionOpKind::StringEquals,
        if_exists: true,
        value_kind: ConditionValueKind::String,
        evaluate: eval_string_equals_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotEquals",
        kind: ConditionOpKind::StringNotEquals,
        if_exists: false,
        value_kind: ConditionValueKind::String,
        evaluate: eval_string_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotEqualsIfExists",
        kind: ConditionOpKind::StringNotEquals,
        if_exists: true,
        value_kind: ConditionValueKind::String,
        // StringNotEquals already treats Absent as Matches, so the IfExists
        // variant shares the same evaluator.
        evaluate: eval_string_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringLike",
        kind: ConditionOpKind::StringLike,
        if_exists: false,
        value_kind: ConditionValueKind::String,
        evaluate: eval_string_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringLikeIfExists",
        kind: ConditionOpKind::StringLike,
        if_exists: true,
        value_kind: ConditionValueKind::String,
        evaluate: eval_string_like_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotLike",
        kind: ConditionOpKind::StringNotLike,
        if_exists: false,
        value_kind: ConditionValueKind::String,
        evaluate: eval_string_not_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotLikeIfExists",
        kind: ConditionOpKind::StringNotLike,
        if_exists: true,
        value_kind: ConditionValueKind::String,
        // StringNotLike already treats Absent as Matches, so the IfExists
        // variant shares the same evaluator.
        evaluate: eval_string_not_like,
        // Intentionally not in the currently enforced object-action subset,
        // preserving the existing supportedness asymmetry.
        evaluable_on_evaluable_object_actions: false,
    },
    ConditionOpDef {
        name: "Null",
        kind: ConditionOpKind::Null,
        if_exists: false,
        value_kind: ConditionValueKind::String,
        evaluate: eval_null,
        evaluable_on_evaluable_object_actions: true,
    },
];

/// Look up an operator definition by its wire name (for example
/// `"StringEquals"` or `"StringEqualsIfExists"`).
pub(super) fn lookup(name: &str) -> Option<&'static ConditionOpDef> {
    CONDITION_OPS.iter().find(|op| op.name == name)
}

/// Whether the named operator is evaluated by the currently enforced
/// object-action policy subset.
///
/// Callers can derive this by walking [`CONDITION_OPS`]; this helper exists
/// so the migration can replace the existing
/// `evaluable_string_condition_operator_supported` predicate with a single
/// table-driven lookup.
pub(super) fn is_evaluable_on_evaluable_object_actions(name: &str) -> bool {
    lookup(name).is_some_and(|op| op.evaluable_on_evaluable_object_actions)
}

fn eval_string_equals(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands.iter().any(|expected| expected == actual) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::Absent => ConditionMatchResult::NoMatch,
        ActualValue::Unavailable => ConditionMatchResult::InputUnavailable,
    }
}

fn eval_string_equals_if_exists(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(_) => eval_string_equals(operands, actual),
        ActualValue::Absent => ConditionMatchResult::Matches,
        ActualValue::Unavailable => ConditionMatchResult::InputUnavailable,
    }
}

fn eval_string_not_equals(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands.iter().all(|expected| expected != actual) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
        ActualValue::Unavailable => ConditionMatchResult::InputUnavailable,
    }
}

fn eval_string_like(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .any(|expected| wildcard_matches(expected, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::Absent => ConditionMatchResult::NoMatch,
        ActualValue::Unavailable => ConditionMatchResult::InputUnavailable,
    }
}

fn eval_string_like_if_exists(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(_) => eval_string_like(operands, actual),
        ActualValue::Absent => ConditionMatchResult::Matches,
        ActualValue::Unavailable => ConditionMatchResult::InputUnavailable,
    }
}

fn eval_string_not_like(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .all(|expected| !wildcard_matches(expected, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
        ActualValue::Unavailable => ConditionMatchResult::InputUnavailable,
    }
}

fn eval_null(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult {
    let is_null = matches!(actual, ActualValue::Absent);
    if operands.iter().any(|expected| match expected.as_str() {
        "true" => is_null,
        "false" => !is_null,
        _ => false,
    }) {
        ConditionMatchResult::Matches
    } else {
        ConditionMatchResult::NoMatch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn present<'a>(value: &'a str) -> ActualValue<'a> {
        ActualValue::Present(value)
    }

    fn operands(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    #[test]
    fn lookup_unknown_operator_returns_none() {
        assert!(lookup("DefinitelyNotAnOperator").is_none());
    }

    #[test]
    fn lookup_distinguishes_base_and_if_exists_rows() {
        let base = lookup("StringEquals").expect("StringEquals is in the table");
        let if_exists = lookup("StringEqualsIfExists").expect("IfExists variant is in the table");
        assert_eq!(base.kind, ConditionOpKind::StringEquals);
        assert_eq!(if_exists.kind, ConditionOpKind::StringEquals);
        assert!(!base.if_exists);
        assert!(if_exists.if_exists);
    }

    #[test]
    fn string_not_like_if_exists_is_not_evaluable_on_object_actions() {
        // The existing supportedness predicate intentionally omits this
        // operator. The table row exists so evaluation stays total for the
        // current dispatch, but the supportedness check rejects it.
        assert!(!is_evaluable_on_evaluable_object_actions(
            "StringNotLikeIfExists"
        ));
        assert!(is_evaluable_on_evaluable_object_actions("StringNotLike"));
    }

    #[test]
    fn string_equals_present_value() {
        let op = lookup("StringEquals").unwrap();
        let expected = operands(&["allow", "value"]);
        assert_eq!(
            (op.evaluate)(&expected, present("value")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present("deny")),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn string_equals_absent_is_no_match_but_if_exists_is_match() {
        let base = lookup("StringEquals").unwrap();
        let if_exists = lookup("StringEqualsIfExists").unwrap();
        let expected = operands(&["value"]);
        assert_eq!(
            (base.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (if_exists.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn string_equals_unavailable_propagates() {
        let op = lookup("StringEquals").unwrap();
        let if_exists = lookup("StringEqualsIfExists").unwrap();
        let expected = operands(&["value"]);
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Unavailable),
            ConditionMatchResult::InputUnavailable
        );
        assert_eq!(
            (if_exists.evaluate)(&expected, ActualValue::Unavailable),
            ConditionMatchResult::InputUnavailable
        );
    }

    #[test]
    fn string_not_equals_absent_matches_regardless_of_if_exists() {
        let base = lookup("StringNotEquals").unwrap();
        let if_exists = lookup("StringNotEqualsIfExists").unwrap();
        let expected = operands(&["value"]);
        assert_eq!(
            (base.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (if_exists.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn string_not_equals_present_matches_only_when_all_differ() {
        let op = lookup("StringNotEquals").unwrap();
        let expected = operands(&["a", "b"]);
        assert_eq!(
            (op.evaluate)(&expected, present("c")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present("a")),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn string_like_uses_wildcards() {
        let op = lookup("StringLike").unwrap();
        let expected = operands(&["foo-*"]);
        assert_eq!(
            (op.evaluate)(&expected, present("foo-bar")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present("bar")),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn string_like_if_exists_absent_matches() {
        let op = lookup("StringLikeIfExists").unwrap();
        let expected = operands(&["foo-*"]);
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn string_not_like_absent_matches() {
        let op = lookup("StringNotLike").unwrap();
        let expected = operands(&["foo-*"]);
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present("foo-bar")),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&expected, present("bar")),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn null_true_requires_absent() {
        let op = lookup("Null").unwrap();
        let expected = operands(&["true"]);
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present("anything")),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn null_false_requires_present() {
        let op = lookup("Null").unwrap();
        let expected = operands(&["false"]);
        assert_eq!(
            (op.evaluate)(&expected, present("anything")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn null_unknown_operand_never_matches() {
        let op = lookup("Null").unwrap();
        let expected = operands(&["maybe"]);
        assert_eq!(
            (op.evaluate)(&expected, present("anything")),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn every_evaluable_operator_has_an_absent_handler() {
        // Guard: every row must produce a determinate `Matches`/`NoMatch`
        // answer on an absent value, with `Unavailable` reserved for keys
        // the resolver cannot answer.
        for op in CONDITION_OPS {
            let result = (op.evaluate)(&[], ActualValue::Absent);
            assert!(
                matches!(
                    result,
                    ConditionMatchResult::Matches | ConditionMatchResult::NoMatch
                ),
                "operator {} produced {:?} on Absent",
                op.name,
                result
            );
        }
    }
}
