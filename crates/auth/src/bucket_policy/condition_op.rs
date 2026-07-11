//! Condition operator table.
//!
//! This module hosts one definition per supported AWS IAM condition operator
//! (`StringEquals`, `StringLike`, `Null`, …). Each operator owns its own
//! semantics, including `IfExists` handling and negation, so call sites do
//! not re-enumerate the operator list. The public evaluator in
//! `super::bucket_policy` routes its operator dispatch onto this table so
//! that adding a new operator is a one-row table change instead of a new
//! arm in several different match statements.

use super::{wildcard_matches, ConditionMatchResult};
use std::net::IpAddr;

/// Value presented to a condition operator by the evaluator.
///
/// - `Present(value)`: the condition key resolved to a concrete string
/// - `Absent`: the key is known but not supplied by the request
///
/// The third input state the evaluator distinguishes — "cannot be
/// determined in this context" — is handled by the condition-key resolver
/// (see `super::condition_key::evaluate_clause`) before the operator is
/// called, so operators do not need to model it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ActualValue<'a> {
    Present(&'a str),
    PresentValues(Vec<&'a str>),
    SourceIp(IpAddr),
    Absent,
}

/// Tagged discriminant for condition operators.
///
/// Kept as an explicit enum so the table can be walked by category (for
/// example, filtering supportedness predicates) without string comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConditionOpKind {
    Bool,
    StringEquals,
    StringEqualsIgnoreCase,
    StringNotEquals,
    StringNotEqualsIgnoreCase,
    StringLike,
    StringNotLike,
    NumericEquals,
    NumericNotEquals,
    NumericLessThan,
    NumericLessThanEquals,
    NumericGreaterThan,
    NumericGreaterThanEquals,
    IpAddress,
    NotIpAddress,
    Null,
}

/// One row of the condition operator table.
///
/// `name` is the on-the-wire operator string (for example `"StringEquals"`
/// or `"StringEqualsIfExists"`). Operators that accept an `IfExists` variant
/// appear as two separate rows so lookup stays a single exact-match step;
/// the row's `evaluate` function captures the full `IfExists` semantics.
#[derive(Debug, Clone, Copy)]
pub(super) struct ConditionOpDef {
    pub(super) name: &'static str,
    pub(super) kind: ConditionOpKind,
    pub(super) evaluate: fn(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult,
    /// Whether this operator is evaluated by the currently enforced
    /// object-action policy subset. Mirrors the existing
    /// `evaluable_string_condition_operator_supported` filter; derived from
    /// the table rather than enumerated at call sites.
    pub(super) evaluable_on_evaluable_object_actions: bool,
}

/// The compile-time operator table.
///
pub(super) const CONDITION_OPS: &[ConditionOpDef] = &[
    ConditionOpDef {
        name: "Bool",
        kind: ConditionOpKind::Bool,
        evaluate: eval_bool,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringEquals",
        kind: ConditionOpKind::StringEquals,
        evaluate: eval_string_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:StringEquals",
        kind: ConditionOpKind::StringEquals,
        evaluate: eval_for_all_values_string_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:StringEquals",
        kind: ConditionOpKind::StringEquals,
        evaluate: eval_for_any_value_string_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringEqualsIfExists",
        kind: ConditionOpKind::StringEquals,
        evaluate: eval_string_equals_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringEqualsIgnoreCase",
        kind: ConditionOpKind::StringEqualsIgnoreCase,
        evaluate: eval_string_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:StringEqualsIgnoreCase",
        kind: ConditionOpKind::StringEqualsIgnoreCase,
        evaluate: eval_for_all_values_string_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:StringEqualsIgnoreCase",
        kind: ConditionOpKind::StringEqualsIgnoreCase,
        evaluate: eval_for_any_value_string_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringEqualsIgnoreCaseIfExists",
        kind: ConditionOpKind::StringEqualsIgnoreCase,
        evaluate: eval_string_equals_ignore_case_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotEquals",
        kind: ConditionOpKind::StringNotEquals,
        evaluate: eval_string_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotEqualsIfExists",
        kind: ConditionOpKind::StringNotEquals,
        // StringNotEquals already treats Absent as Matches, so the IfExists
        // variant shares the same evaluator.
        evaluate: eval_string_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotEqualsIgnoreCase",
        kind: ConditionOpKind::StringNotEqualsIgnoreCase,
        evaluate: eval_string_not_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:StringNotEqualsIgnoreCase",
        kind: ConditionOpKind::StringNotEqualsIgnoreCase,
        evaluate: eval_for_all_values_string_not_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:StringNotEqualsIgnoreCase",
        kind: ConditionOpKind::StringNotEqualsIgnoreCase,
        evaluate: eval_for_any_value_string_not_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotEqualsIgnoreCaseIfExists",
        kind: ConditionOpKind::StringNotEqualsIgnoreCase,
        // StringNotEqualsIgnoreCase already treats Absent as Matches, so
        // the IfExists variant shares the same evaluator.
        evaluate: eval_string_not_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringLike",
        kind: ConditionOpKind::StringLike,
        evaluate: eval_string_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:StringLike",
        kind: ConditionOpKind::StringLike,
        evaluate: eval_for_all_values_string_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:StringLike",
        kind: ConditionOpKind::StringLike,
        evaluate: eval_for_any_value_string_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringLikeIfExists",
        kind: ConditionOpKind::StringLike,
        evaluate: eval_string_like_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotLike",
        kind: ConditionOpKind::StringNotLike,
        evaluate: eval_string_not_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:StringNotLike",
        kind: ConditionOpKind::StringNotLike,
        evaluate: eval_for_all_values_string_not_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:StringNotLike",
        kind: ConditionOpKind::StringNotLike,
        evaluate: eval_for_any_value_string_not_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotLikeIfExists",
        kind: ConditionOpKind::StringNotLike,
        // StringNotLike already treats Absent as Matches, so the IfExists
        // variant shares the same evaluator.
        evaluate: eval_string_not_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericEquals",
        kind: ConditionOpKind::NumericEquals,
        evaluate: eval_numeric_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericEqualsIfExists",
        kind: ConditionOpKind::NumericEquals,
        evaluate: eval_numeric_equals_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericNotEquals",
        kind: ConditionOpKind::NumericNotEquals,
        evaluate: eval_numeric_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericNotEqualsIfExists",
        kind: ConditionOpKind::NumericNotEquals,
        evaluate: eval_numeric_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericLessThan",
        kind: ConditionOpKind::NumericLessThan,
        evaluate: eval_numeric_less_than,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericLessThanIfExists",
        kind: ConditionOpKind::NumericLessThan,
        evaluate: eval_numeric_less_than_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericLessThanEquals",
        kind: ConditionOpKind::NumericLessThanEquals,
        evaluate: eval_numeric_less_than_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericLessThanEqualsIfExists",
        kind: ConditionOpKind::NumericLessThanEquals,
        evaluate: eval_numeric_less_than_equals_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericGreaterThan",
        kind: ConditionOpKind::NumericGreaterThan,
        evaluate: eval_numeric_greater_than,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericGreaterThanIfExists",
        kind: ConditionOpKind::NumericGreaterThan,
        evaluate: eval_numeric_greater_than_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericGreaterThanEquals",
        kind: ConditionOpKind::NumericGreaterThanEquals,
        evaluate: eval_numeric_greater_than_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericGreaterThanEqualsIfExists",
        kind: ConditionOpKind::NumericGreaterThanEquals,
        evaluate: eval_numeric_greater_than_equals_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "IpAddress",
        kind: ConditionOpKind::IpAddress,
        evaluate: eval_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:IpAddress",
        kind: ConditionOpKind::IpAddress,
        evaluate: eval_for_all_values_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:IpAddress",
        kind: ConditionOpKind::IpAddress,
        evaluate: eval_for_any_value_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "IpAddressIfExists",
        kind: ConditionOpKind::IpAddress,
        evaluate: eval_ip_address_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NotIpAddress",
        kind: ConditionOpKind::NotIpAddress,
        evaluate: eval_not_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:NotIpAddress",
        kind: ConditionOpKind::NotIpAddress,
        evaluate: eval_for_all_values_not_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:NotIpAddress",
        kind: ConditionOpKind::NotIpAddress,
        evaluate: eval_for_any_value_not_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NotIpAddressIfExists",
        kind: ConditionOpKind::NotIpAddress,
        evaluate: eval_not_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "Null",
        kind: ConditionOpKind::Null,
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
#[cfg(test)]
fn is_evaluable_on_evaluable_object_actions(name: &str) -> bool {
    lookup(name).is_some_and(|op| op.evaluable_on_evaluable_object_actions)
}

pub(super) const fn is_string_condition_kind(kind: ConditionOpKind) -> bool {
    matches!(
        kind,
        ConditionOpKind::StringEquals
            | ConditionOpKind::StringEqualsIgnoreCase
            | ConditionOpKind::StringNotEquals
            | ConditionOpKind::StringNotEqualsIgnoreCase
            | ConditionOpKind::StringLike
            | ConditionOpKind::StringNotLike
            | ConditionOpKind::Null
    )
}

pub(super) const fn is_ip_condition_kind(kind: ConditionOpKind) -> bool {
    matches!(
        kind,
        ConditionOpKind::IpAddress | ConditionOpKind::NotIpAddress
    )
}

pub(super) const fn is_numeric_condition_kind(kind: ConditionOpKind) -> bool {
    matches!(
        kind,
        ConditionOpKind::NumericEquals
            | ConditionOpKind::NumericNotEquals
            | ConditionOpKind::NumericLessThan
            | ConditionOpKind::NumericLessThanEquals
            | ConditionOpKind::NumericGreaterThan
            | ConditionOpKind::NumericGreaterThanEquals
            | ConditionOpKind::Null
    )
}

fn string_eq_ignore_case(expected: &str, actual: &str) -> bool {
    expected == actual || expected.to_lowercase() == actual.to_lowercase()
}

fn eval_bool(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .any(|expected| expected.eq_ignore_ascii_case(actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(_) | ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
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
        ActualValue::PresentValues(_) | ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_for_all_values_string_equals(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands.iter().any(|expected| expected == actual) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(actuals) => {
            if actuals
                .iter()
                .all(|actual| operands.iter().any(|expected| expected == actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_any_value_string_equals(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands.iter().any(|expected| expected == actual) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(actuals) => {
            if actuals
                .iter()
                .any(|actual| operands.iter().any(|expected| expected == actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_string_equals_if_exists(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(_) | ActualValue::PresentValues(_) => {
            eval_string_equals(operands, actual)
        }
        ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_string_equals_ignore_case(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .any(|expected| string_eq_ignore_case(expected, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(_) | ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_for_all_values_string_equals_ignore_case(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .any(|expected| string_eq_ignore_case(expected, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(actuals) => {
            if actuals.iter().all(|actual| {
                operands
                    .iter()
                    .any(|expected| string_eq_ignore_case(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_any_value_string_equals_ignore_case(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .any(|expected| string_eq_ignore_case(expected, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(actuals) => {
            if actuals.iter().any(|actual| {
                operands
                    .iter()
                    .any(|expected| string_eq_ignore_case(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_string_equals_ignore_case_if_exists(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(_) | ActualValue::PresentValues(_) => {
            eval_string_equals_ignore_case(operands, actual)
        }
        ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
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
        ActualValue::PresentValues(_) | ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_string_not_equals_ignore_case(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .all(|expected| !string_eq_ignore_case(expected, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(_) | ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_all_values_string_not_equals_ignore_case(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .all(|expected| !string_eq_ignore_case(expected, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(actuals) => {
            if actuals.iter().all(|actual| {
                operands
                    .iter()
                    .all(|expected| !string_eq_ignore_case(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_any_value_string_not_equals_ignore_case(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .all(|expected| !string_eq_ignore_case(expected, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(actuals) => {
            if actuals.iter().any(|actual| {
                operands
                    .iter()
                    .all(|expected| !string_eq_ignore_case(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
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
        ActualValue::PresentValues(_) | ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_for_all_values_string_like(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
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
        ActualValue::PresentValues(actuals) => {
            if actuals.iter().all(|actual| {
                operands
                    .iter()
                    .any(|expected| wildcard_matches(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_any_value_string_like(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
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
        ActualValue::PresentValues(actuals) => {
            if actuals.iter().any(|actual| {
                operands
                    .iter()
                    .any(|expected| wildcard_matches(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_string_like_if_exists(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(_) | ActualValue::PresentValues(_) => {
            eval_string_like(operands, actual)
        }
        ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
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
        ActualValue::PresentValues(_) | ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_all_values_string_not_like(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
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
        ActualValue::PresentValues(actuals) => {
            if actuals.iter().all(|actual| {
                operands
                    .iter()
                    .all(|expected| !wildcard_matches(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_any_value_string_not_like(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
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
        ActualValue::PresentValues(actuals) => {
            if actuals.iter().any(|actual| {
                operands
                    .iter()
                    .all(|expected| !wildcard_matches(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

#[derive(Clone, Copy)]
enum NumericComparison {
    Equals,
    NotEquals,
    LessThan,
    LessThanEquals,
    GreaterThan,
    GreaterThanEquals,
}

fn eval_numeric_comparison(
    operands: &[String],
    actual: ActualValue<'_>,
    comparison: NumericComparison,
    if_exists: bool,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            let Some(actual) = parse_numeric(actual) else {
                return ConditionMatchResult::NoMatch;
            };
            eval_numeric_operands(operands, actual, comparison)
        }
        ActualValue::PresentValues(_) | ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent if if_exists || matches!(comparison, NumericComparison::NotEquals) => {
            ConditionMatchResult::Matches
        }
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_numeric_operands(
    operands: &[String],
    actual: f64,
    comparison: NumericComparison,
) -> ConditionMatchResult {
    let matches = match comparison {
        NumericComparison::Equals => operands
            .iter()
            .filter_map(|expected| parse_numeric(expected))
            .any(|expected| actual.total_cmp(&expected).is_eq()),
        NumericComparison::NotEquals => {
            operands
                .iter()
                .all(|expected| match parse_numeric(expected) {
                    Some(expected) => actual.total_cmp(&expected).is_ne(),
                    None => true,
                })
        }
        NumericComparison::LessThan => operands
            .iter()
            .filter_map(|expected| parse_numeric(expected))
            .any(|expected| actual < expected),
        NumericComparison::LessThanEquals => operands
            .iter()
            .filter_map(|expected| parse_numeric(expected))
            .any(|expected| actual <= expected),
        NumericComparison::GreaterThan => operands
            .iter()
            .filter_map(|expected| parse_numeric(expected))
            .any(|expected| actual > expected),
        NumericComparison::GreaterThanEquals => operands
            .iter()
            .filter_map(|expected| parse_numeric(expected))
            .any(|expected| actual >= expected),
    };
    if matches {
        ConditionMatchResult::Matches
    } else {
        ConditionMatchResult::NoMatch
    }
}

fn eval_numeric_equals(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::Equals, false)
}

fn eval_numeric_equals_if_exists(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::Equals, true)
}

fn eval_numeric_not_equals(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::NotEquals, false)
}

fn eval_numeric_less_than(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::LessThan, false)
}

fn eval_numeric_less_than_if_exists(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::LessThan, true)
}

fn eval_numeric_less_than_equals(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::LessThanEquals, false)
}

fn eval_numeric_less_than_equals_if_exists(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::LessThanEquals, true)
}

fn eval_numeric_greater_than(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::GreaterThan, false)
}

fn eval_numeric_greater_than_if_exists(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::GreaterThan, true)
}

fn eval_numeric_greater_than_equals(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(
        operands,
        actual,
        NumericComparison::GreaterThanEquals,
        false,
    )
}

fn eval_numeric_greater_than_equals_if_exists(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::GreaterThanEquals, true)
}

fn parse_numeric(value: &str) -> Option<f64> {
    let parsed: f64 = value.parse().ok()?;
    parsed.is_finite().then_some(parsed)
}

fn eval_ip_address(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult {
    eval_ip_address_comparison(operands, actual, IpComparison::Equals, false)
}

fn eval_for_all_values_ip_address(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_ip_address(operands, actual)
}

fn eval_for_any_value_ip_address(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_ip_address(operands, actual)
}

fn eval_ip_address_if_exists(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult {
    eval_ip_address_comparison(operands, actual, IpComparison::Equals, true)
}

fn eval_not_ip_address(operands: &[String], actual: ActualValue<'_>) -> ConditionMatchResult {
    eval_ip_address_comparison(operands, actual, IpComparison::NotEquals, false)
}

fn eval_for_all_values_not_ip_address(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_not_ip_address(operands, actual)
}

fn eval_for_any_value_not_ip_address(
    operands: &[String],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_not_ip_address(operands, actual)
}

#[derive(Clone, Copy)]
enum IpComparison {
    Equals,
    NotEquals,
}

fn eval_ip_address_comparison(
    operands: &[String],
    actual: ActualValue<'_>,
    comparison: IpComparison,
    if_exists: bool,
) -> ConditionMatchResult {
    let matches = match actual {
        ActualValue::SourceIp(actual) => operands
            .iter()
            .filter_map(|expected| super::parse_ip_addr_or_cidr(expected))
            .any(|(network, prefix)| ip_addr_matches_cidr(actual, network, prefix)),
        ActualValue::Absent if if_exists || matches!(comparison, IpComparison::NotEquals) => {
            return ConditionMatchResult::Matches;
        }
        ActualValue::Present(_) | ActualValue::PresentValues(_) | ActualValue::Absent => false,
    };
    let matches = match comparison {
        IpComparison::Equals => matches,
        IpComparison::NotEquals => !matches,
    };
    if matches {
        ConditionMatchResult::Matches
    } else {
        ConditionMatchResult::NoMatch
    }
}

fn ip_addr_matches_cidr(actual: IpAddr, network: IpAddr, prefix: u8) -> bool {
    match (actual, network) {
        (IpAddr::V4(actual), IpAddr::V4(network)) => {
            let actual = u32::from(actual);
            let network = u32::from(network);
            prefix_bits_v4(actual, prefix) == prefix_bits_v4(network, prefix)
        }
        (IpAddr::V6(actual), IpAddr::V6(network)) => {
            let actual = u128::from(actual);
            let network = u128::from(network);
            prefix_bits_v6(actual, prefix) == prefix_bits_v6(network, prefix)
        }
        (IpAddr::V4(_), IpAddr::V6(_)) | (IpAddr::V6(_), IpAddr::V4(_)) => false,
    }
}

fn prefix_bits_v4(value: u32, prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        value & (u32::MAX << (32 - u32::from(prefix)))
    }
}

fn prefix_bits_v6(value: u128, prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        value & (u128::MAX << (128 - u32::from(prefix)))
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

    fn present_values<'a>(values: &[&'a str]) -> ActualValue<'a> {
        ActualValue::PresentValues(values.to_vec())
    }

    fn source_ip(value: &str) -> ActualValue<'_> {
        ActualValue::SourceIp(value.parse().expect("test source IP is valid"))
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
        // The two rows share a kind but must use different evaluator fns;
        // compare their observable behaviour on an absent value.
        let operands = operands(&["value"]);
        assert_eq!(
            (base.evaluate)(&operands, ActualValue::Absent),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (if_exists.evaluate)(&operands, ActualValue::Absent),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn bool_operator_matches_case_insensitive_boolean_strings() {
        let op = lookup("Bool").expect("Bool is in the table");
        assert_eq!(op.kind, ConditionOpKind::Bool);
        assert_eq!(
            (op.evaluate)(&operands(&["true"]), present("true")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&operands(&["TRUE"]), present("true")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&operands(&["false"]), present("true")),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&operands(&["true"]), ActualValue::Absent),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn numeric_equals_matches_numeric_strings() {
        let op = lookup("NumericEquals").expect("NumericEquals is in the table");
        assert_eq!(op.kind, ConditionOpKind::NumericEquals);
        assert_eq!(
            (op.evaluate)(&operands(&["2"]), present("2")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&operands(&["2.0"]), present("2")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&operands(&["3"]), present("2")),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&operands(&["not-a-number"]), present("2")),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&operands(&["2"]), ActualValue::Absent),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn numeric_comparison_operators_match_expected_ordering() {
        let cases = [
            ("NumericNotEquals", "3", ConditionMatchResult::Matches),
            ("NumericNotEquals", "2", ConditionMatchResult::NoMatch),
            ("NumericLessThan", "3", ConditionMatchResult::Matches),
            ("NumericLessThan", "2", ConditionMatchResult::NoMatch),
            ("NumericLessThanEquals", "2", ConditionMatchResult::Matches),
            ("NumericLessThanEquals", "1", ConditionMatchResult::NoMatch),
            ("NumericGreaterThan", "1", ConditionMatchResult::Matches),
            ("NumericGreaterThan", "2", ConditionMatchResult::NoMatch),
            (
                "NumericGreaterThanEquals",
                "2",
                ConditionMatchResult::Matches,
            ),
            (
                "NumericGreaterThanEquals",
                "3",
                ConditionMatchResult::NoMatch,
            ),
        ];

        for (operator, expected, outcome) in cases {
            let op = lookup(operator).expect("numeric operator is in the table");
            assert_eq!((op.evaluate)(&operands(&[expected]), present("2")), outcome);
        }
    }

    #[test]
    fn numeric_if_exists_variants_match_absent_values() {
        for operator in [
            "NumericEqualsIfExists",
            "NumericNotEqualsIfExists",
            "NumericLessThanIfExists",
            "NumericLessThanEqualsIfExists",
            "NumericGreaterThanIfExists",
            "NumericGreaterThanEqualsIfExists",
        ] {
            let op = lookup(operator).expect("numeric IfExists operator is in the table");
            assert_eq!(
                (op.evaluate)(&operands(&["2"]), ActualValue::Absent),
                ConditionMatchResult::Matches,
                "{operator} should match absent context values"
            );
        }
    }

    #[test]
    fn numeric_not_equals_matches_absent_and_invalid_policy_operands() {
        let op = lookup("NumericNotEquals").expect("NumericNotEquals is in the table");
        assert_eq!(
            (op.evaluate)(&operands(&["2"]), ActualValue::Absent),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&operands(&["not-a-number"]), present("2")),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn string_not_like_if_exists_is_evaluable_on_object_actions() {
        assert!(is_evaluable_on_evaluable_object_actions("StringNotLike"));
        assert!(is_evaluable_on_evaluable_object_actions(
            "StringNotLikeIfExists"
        ));
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
    fn string_equals_does_not_match_multivalue_context() {
        let op = lookup("StringEquals").unwrap();
        assert_eq!(
            (op.evaluate)(
                &operands(&["security", "team"]),
                present_values(&["security"])
            ),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(
                &operands(&["security", "team"]),
                present_values(&["security", "team"])
            ),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn string_equals_ignore_case_present_value() {
        let op = lookup("StringEqualsIgnoreCase").unwrap();
        assert_eq!(op.kind, ConditionOpKind::StringEqualsIgnoreCase);
        let expected = operands(&["allow", "VALUE"]);
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
    fn string_equals_ignore_case_matches_non_ascii_case_variants() {
        let op = lookup("StringEqualsIgnoreCase").unwrap();
        let expected = operands(&["sëcret"]);
        assert_eq!(
            (op.evaluate)(&expected, present("SËCRET")),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn string_equals_ignore_case_does_not_match_multivalue_context() {
        let op = lookup("StringEqualsIgnoreCase").unwrap();
        assert_eq!(
            (op.evaluate)(
                &operands(&["security", "team"]),
                present_values(&["SECURITY"])
            ),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn for_all_values_string_equals_requires_every_actual_value_to_match() {
        let op = lookup("ForAllValues:StringEquals").unwrap();
        let expected = operands(&["security", "team"]);
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["security"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["security", "team"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["security", "project"])),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&[])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn for_any_value_string_equals_requires_at_least_one_actual_value_to_match() {
        let op = lookup("ForAnyValue:StringEquals").unwrap();
        let expected = operands(&["security"]);
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["security", "project"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["project"])),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn for_all_values_string_equals_ignore_case_requires_every_actual_value_to_match() {
        let op = lookup("ForAllValues:StringEqualsIgnoreCase").unwrap();
        assert_eq!(op.kind, ConditionOpKind::StringEqualsIgnoreCase);
        let expected = operands(&["security", "TEAM"]);
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["SECURITY"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["SECURITY", "team"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["SECURITY", "project"])),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&[])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn for_any_value_string_equals_ignore_case_requires_at_least_one_actual_value_to_match() {
        let op = lookup("ForAnyValue:StringEqualsIgnoreCase").unwrap();
        assert_eq!(op.kind, ConditionOpKind::StringEqualsIgnoreCase);
        let expected = operands(&["SECURITY"]);
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["security", "project"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["project"])),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
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
    fn string_equals_ignore_case_absent_is_no_match_but_if_exists_is_match() {
        let base = lookup("StringEqualsIgnoreCase").unwrap();
        let if_exists = lookup("StringEqualsIgnoreCaseIfExists").unwrap();
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
    fn string_not_equals_ignore_case_absent_matches_regardless_of_if_exists() {
        let base = lookup("StringNotEqualsIgnoreCase").unwrap();
        let if_exists = lookup("StringNotEqualsIgnoreCaseIfExists").unwrap();
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
    fn string_not_equals_ignore_case_present_matches_only_when_all_differ() {
        let op = lookup("StringNotEqualsIgnoreCase").unwrap();
        assert_eq!(op.kind, ConditionOpKind::StringNotEqualsIgnoreCase);
        let expected = operands(&["a", "B"]);
        assert_eq!(
            (op.evaluate)(&expected, present("c")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present("b")),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn string_not_equals_ignore_case_rejects_non_ascii_case_variants() {
        let op = lookup("StringNotEqualsIgnoreCase").unwrap();
        let expected = operands(&["sëcret"]);
        assert_eq!(
            (op.evaluate)(&expected, present("private")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present("SËCRET")),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn for_all_values_string_not_equals_ignore_case_requires_every_actual_value_to_differ() {
        let op = lookup("ForAllValues:StringNotEqualsIgnoreCase").unwrap();
        assert_eq!(op.kind, ConditionOpKind::StringNotEqualsIgnoreCase);
        let expected = operands(&["security", "TEAM"]);
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["project"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["project", "owner"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["project", "team"])),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&[])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn for_any_value_string_not_equals_ignore_case_requires_one_actual_value_to_differ() {
        let op = lookup("ForAnyValue:StringNotEqualsIgnoreCase").unwrap();
        assert_eq!(op.kind, ConditionOpKind::StringNotEqualsIgnoreCase);
        let expected = operands(&["SECURITY"]);
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["security", "project"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["security"])),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
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
    fn for_all_values_string_like_requires_every_actual_value_to_match() {
        let op = lookup("ForAllValues:StringLike").unwrap();
        assert_eq!(op.kind, ConditionOpKind::StringLike);
        let expected = operands(&["sec*", "team"]);
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["security"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["security", "team"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["security", "project"])),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&[])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn for_any_value_string_like_requires_at_least_one_actual_value_to_match() {
        let op = lookup("ForAnyValue:StringLike").unwrap();
        assert_eq!(op.kind, ConditionOpKind::StringLike);
        let expected = operands(&["sec*"]);
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["security", "project"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["project"])),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::NoMatch
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
    fn for_all_values_string_not_like_requires_every_actual_value_to_differ() {
        let op = lookup("ForAllValues:StringNotLike").unwrap();
        assert_eq!(op.kind, ConditionOpKind::StringNotLike);
        let expected = operands(&["sec*", "team"]);
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["project"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["project", "owner"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["project", "team"])),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&[])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn for_any_value_string_not_like_requires_one_actual_value_to_differ() {
        let op = lookup("ForAnyValue:StringNotLike").unwrap();
        assert_eq!(op.kind, ConditionOpKind::StringNotLike);
        let expected = operands(&["sec*"]);
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["security", "project"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&expected, present_values(&["security"])),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::NoMatch
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
    fn ip_address_matches_cidr_operands() {
        let op = lookup("IpAddress").unwrap();
        assert_eq!(
            (op.evaluate)(&operands(&["127.0.0.0/8"]), source_ip("127.0.0.1")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&operands(&["10.0.0.0/8"]), source_ip("127.0.0.1")),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn ip_address_does_not_match_other_address_family() {
        let op = lookup("IpAddress").unwrap();
        assert_eq!(
            (op.evaluate)(&operands(&["::/0"]), source_ip("127.0.0.1")),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn not_ip_address_matches_when_no_operand_contains_source() {
        let op = lookup("NotIpAddress").unwrap();
        assert_eq!(
            (op.evaluate)(&operands(&["10.0.0.0/8"]), source_ip("127.0.0.1")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&operands(&["127.0.0.0/8"]), source_ip("127.0.0.1")),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn every_evaluable_operator_has_an_absent_handler() {
        // Guard: every row must produce a determinate `Matches`/`NoMatch`
        // answer on an absent value.
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
