// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Condition operator table.
//!
//! This module hosts one definition per supported AWS IAM condition operator
//! (`StringEquals`, `StringLike`, `Null`, …). Each operator owns its own
//! semantics, including `IfExists` handling and negation, so call sites do
//! not re-enumerate the operator list. The public evaluator in
//! `super::bucket_policy` routes its operator dispatch onto this table so
//! that adding a new operator is a one-row table change instead of a new
//! arm in several different match statements.

use super::{
    policy_string_equals, policy_string_equals_ignore_case, policy_value_wildcard_matches,
    ConditionMatchResult, PolicyValue,
};
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
    Numeric(u64),
    EpochSeconds(u64),
    Absent,
}

/// Tagged discriminant for condition operators.
///
/// Kept as an explicit enum so the table can be walked by category (for
/// example, filtering supportedness predicates) without string comparisons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConditionOpKind {
    ArnEquals,
    BinaryEquals,
    Bool,
    DateEquals,
    DateNotEquals,
    DateLessThan,
    DateLessThanEquals,
    DateGreaterThan,
    DateGreaterThanEquals,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ConditionSetQualifier {
    None,
    ForAllValues,
    ForAnyValue,
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
    pub(super) set_qualifier: ConditionSetQualifier,
    pub(super) kind: ConditionOpKind,
    pub(super) evaluate:
        fn(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult,
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
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::Bool,
        evaluate: eval_bool,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "BoolIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::Bool,
        evaluate: eval_bool_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:Bool",
        set_qualifier: ConditionSetQualifier::ForAllValues,
        kind: ConditionOpKind::Bool,
        evaluate: eval_for_all_values_bool,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:Bool",
        set_qualifier: ConditionSetQualifier::ForAnyValue,
        kind: ConditionOpKind::Bool,
        evaluate: eval_for_any_value_bool,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "BinaryEquals",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::BinaryEquals,
        evaluate: eval_binary_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:BinaryEquals",
        set_qualifier: ConditionSetQualifier::ForAllValues,
        kind: ConditionOpKind::BinaryEquals,
        evaluate: eval_for_all_values_binary_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:BinaryEquals",
        set_qualifier: ConditionSetQualifier::ForAnyValue,
        kind: ConditionOpKind::BinaryEquals,
        evaluate: eval_for_any_value_binary_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "BinaryEqualsIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::BinaryEquals,
        evaluate: eval_binary_equals_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "DateEquals",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::DateEquals,
        evaluate: eval_date_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "DateEqualsIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::DateEquals,
        evaluate: eval_date_equals_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "DateNotEquals",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::DateNotEquals,
        evaluate: eval_date_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "DateNotEqualsIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::DateNotEquals,
        evaluate: eval_date_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "DateLessThan",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::DateLessThan,
        evaluate: eval_date_less_than,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "DateLessThanIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::DateLessThan,
        evaluate: eval_date_less_than_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "DateLessThanEquals",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::DateLessThanEquals,
        evaluate: eval_date_less_than_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "DateLessThanEqualsIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::DateLessThanEquals,
        evaluate: eval_date_less_than_equals_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "DateGreaterThan",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::DateGreaterThan,
        evaluate: eval_date_greater_than,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "DateGreaterThanIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::DateGreaterThan,
        evaluate: eval_date_greater_than_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "DateGreaterThanEquals",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::DateGreaterThanEquals,
        evaluate: eval_date_greater_than_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "DateGreaterThanEqualsIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::DateGreaterThanEquals,
        evaluate: eval_date_greater_than_equals_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ArnEquals",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::ArnEquals,
        evaluate: eval_string_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringEquals",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::StringEquals,
        evaluate: eval_string_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:StringEquals",
        set_qualifier: ConditionSetQualifier::ForAllValues,
        kind: ConditionOpKind::StringEquals,
        evaluate: eval_for_all_values_string_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:StringEquals",
        set_qualifier: ConditionSetQualifier::ForAnyValue,
        kind: ConditionOpKind::StringEquals,
        evaluate: eval_for_any_value_string_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringEqualsIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::StringEquals,
        evaluate: eval_string_equals_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringEqualsIgnoreCase",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::StringEqualsIgnoreCase,
        evaluate: eval_string_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:StringEqualsIgnoreCase",
        set_qualifier: ConditionSetQualifier::ForAllValues,
        kind: ConditionOpKind::StringEqualsIgnoreCase,
        evaluate: eval_for_all_values_string_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:StringEqualsIgnoreCase",
        set_qualifier: ConditionSetQualifier::ForAnyValue,
        kind: ConditionOpKind::StringEqualsIgnoreCase,
        evaluate: eval_for_any_value_string_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringEqualsIgnoreCaseIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::StringEqualsIgnoreCase,
        evaluate: eval_string_equals_ignore_case_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotEquals",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::StringNotEquals,
        evaluate: eval_string_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:StringNotEquals",
        set_qualifier: ConditionSetQualifier::ForAllValues,
        kind: ConditionOpKind::StringNotEquals,
        evaluate: eval_for_all_values_string_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:StringNotEquals",
        set_qualifier: ConditionSetQualifier::ForAnyValue,
        kind: ConditionOpKind::StringNotEquals,
        evaluate: eval_for_any_value_string_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotEqualsIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::StringNotEquals,
        // StringNotEquals already treats Absent as Matches, so the IfExists
        // variant shares the same evaluator.
        evaluate: eval_string_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotEqualsIgnoreCase",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::StringNotEqualsIgnoreCase,
        evaluate: eval_string_not_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:StringNotEqualsIgnoreCase",
        set_qualifier: ConditionSetQualifier::ForAllValues,
        kind: ConditionOpKind::StringNotEqualsIgnoreCase,
        evaluate: eval_for_all_values_string_not_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:StringNotEqualsIgnoreCase",
        set_qualifier: ConditionSetQualifier::ForAnyValue,
        kind: ConditionOpKind::StringNotEqualsIgnoreCase,
        evaluate: eval_for_any_value_string_not_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotEqualsIgnoreCaseIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::StringNotEqualsIgnoreCase,
        // StringNotEqualsIgnoreCase already treats Absent as Matches, so
        // the IfExists variant shares the same evaluator.
        evaluate: eval_string_not_equals_ignore_case,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringLike",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::StringLike,
        evaluate: eval_string_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:StringLike",
        set_qualifier: ConditionSetQualifier::ForAllValues,
        kind: ConditionOpKind::StringLike,
        evaluate: eval_for_all_values_string_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:StringLike",
        set_qualifier: ConditionSetQualifier::ForAnyValue,
        kind: ConditionOpKind::StringLike,
        evaluate: eval_for_any_value_string_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringLikeIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::StringLike,
        evaluate: eval_string_like_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotLike",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::StringNotLike,
        evaluate: eval_string_not_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:StringNotLike",
        set_qualifier: ConditionSetQualifier::ForAllValues,
        kind: ConditionOpKind::StringNotLike,
        evaluate: eval_for_all_values_string_not_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:StringNotLike",
        set_qualifier: ConditionSetQualifier::ForAnyValue,
        kind: ConditionOpKind::StringNotLike,
        evaluate: eval_for_any_value_string_not_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "StringNotLikeIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::StringNotLike,
        // StringNotLike already treats Absent as Matches, so the IfExists
        // variant shares the same evaluator.
        evaluate: eval_string_not_like,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericEquals",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NumericEquals,
        evaluate: eval_numeric_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericEqualsIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NumericEquals,
        evaluate: eval_numeric_equals_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericNotEquals",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NumericNotEquals,
        evaluate: eval_numeric_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericNotEqualsIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NumericNotEquals,
        evaluate: eval_numeric_not_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericLessThan",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NumericLessThan,
        evaluate: eval_numeric_less_than,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericLessThanIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NumericLessThan,
        evaluate: eval_numeric_less_than_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericLessThanEquals",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NumericLessThanEquals,
        evaluate: eval_numeric_less_than_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericLessThanEqualsIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NumericLessThanEquals,
        evaluate: eval_numeric_less_than_equals_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericGreaterThan",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NumericGreaterThan,
        evaluate: eval_numeric_greater_than,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericGreaterThanIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NumericGreaterThan,
        evaluate: eval_numeric_greater_than_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericGreaterThanEquals",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NumericGreaterThanEquals,
        evaluate: eval_numeric_greater_than_equals,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NumericGreaterThanEqualsIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NumericGreaterThanEquals,
        evaluate: eval_numeric_greater_than_equals_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "IpAddress",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::IpAddress,
        evaluate: eval_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:IpAddress",
        set_qualifier: ConditionSetQualifier::ForAllValues,
        kind: ConditionOpKind::IpAddress,
        evaluate: eval_for_all_values_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:IpAddress",
        set_qualifier: ConditionSetQualifier::ForAnyValue,
        kind: ConditionOpKind::IpAddress,
        evaluate: eval_for_any_value_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "IpAddressIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::IpAddress,
        evaluate: eval_ip_address_if_exists,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NotIpAddress",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NotIpAddress,
        evaluate: eval_not_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAllValues:NotIpAddress",
        set_qualifier: ConditionSetQualifier::ForAllValues,
        kind: ConditionOpKind::NotIpAddress,
        evaluate: eval_for_all_values_not_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "ForAnyValue:NotIpAddress",
        set_qualifier: ConditionSetQualifier::ForAnyValue,
        kind: ConditionOpKind::NotIpAddress,
        evaluate: eval_for_any_value_not_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "NotIpAddressIfExists",
        set_qualifier: ConditionSetQualifier::None,
        kind: ConditionOpKind::NotIpAddress,
        evaluate: eval_not_ip_address,
        evaluable_on_evaluable_object_actions: true,
    },
    ConditionOpDef {
        name: "Null",
        set_qualifier: ConditionSetQualifier::None,
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

pub(super) const fn supports_policy_variables(kind: ConditionOpKind) -> bool {
    matches!(
        kind,
        ConditionOpKind::StringEquals
            | ConditionOpKind::StringEqualsIgnoreCase
            | ConditionOpKind::StringNotEquals
            | ConditionOpKind::StringNotEqualsIgnoreCase
            | ConditionOpKind::StringLike
            | ConditionOpKind::StringNotLike
    )
}

pub(super) const fn is_binary_condition_kind(kind: ConditionOpKind) -> bool {
    matches!(kind, ConditionOpKind::BinaryEquals)
}

pub(super) const fn is_date_condition_kind(kind: ConditionOpKind) -> bool {
    matches!(
        kind,
        ConditionOpKind::DateEquals
            | ConditionOpKind::DateNotEquals
            | ConditionOpKind::DateLessThan
            | ConditionOpKind::DateLessThanEquals
            | ConditionOpKind::DateGreaterThan
            | ConditionOpKind::DateGreaterThanEquals
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

pub(super) fn decode_binary_value(value: &str) -> Option<Vec<u8>> {
    use base64::Engine;

    base64::engine::general_purpose::STANDARD.decode(value).ok()
}

fn string_eq_ignore_case(expected: &PolicyValue, actual: &str) -> bool {
    policy_string_equals_ignore_case(expected, actual)
}

fn binary_value_matches(operands: &[PolicyValue], actual: &str) -> bool {
    let Some(actual) = decode_binary_value(actual) else {
        return false;
    };
    operands
        .iter()
        .filter_map(|expected| decode_binary_value(expected))
        .any(|expected| expected == actual)
}

fn eval_binary_equals(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if binary_value_matches(operands, actual) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(_)
        | ActualValue::SourceIp(_)
        | ActualValue::Numeric(_)
        | ActualValue::EpochSeconds(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_for_all_values_binary_equals(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if binary_value_matches(operands, actual) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(actuals) => {
            if actuals
                .iter()
                .all(|actual| binary_value_matches(operands, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_any_value_binary_equals(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if binary_value_matches(operands, actual) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(actuals) => {
            if actuals
                .iter()
                .any(|actual| binary_value_matches(operands, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_binary_equals_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(_) | ActualValue::PresentValues(_) => {
            eval_binary_equals(operands, actual)
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

#[derive(Clone, Copy)]
enum DateComparison {
    Equals,
    NotEquals,
    LessThan,
    LessThanEquals,
    GreaterThan,
    GreaterThanEquals,
}

fn eval_date_comparison(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
    comparison: DateComparison,
    if_exists: bool,
) -> ConditionMatchResult {
    let actual_nanos = match actual {
        ActualValue::EpochSeconds(actual) => i128::from(actual) * 1_000_000_000,
        ActualValue::Present(actual) => {
            let Some(actual) = parse_date_epoch_nanos(actual) else {
                return ConditionMatchResult::NoMatch;
            };
            actual
        }
        ActualValue::Absent if if_exists || matches!(comparison, DateComparison::NotEquals) => {
            return ConditionMatchResult::Matches;
        }
        ActualValue::PresentValues(_)
        | ActualValue::SourceIp(_)
        | ActualValue::Numeric(_)
        | ActualValue::Absent => return ConditionMatchResult::NoMatch,
    };
    eval_date_operands(operands, actual_nanos, comparison)
}

fn eval_date_operands(
    operands: &[PolicyValue],
    actual_nanos: i128,
    comparison: DateComparison,
) -> ConditionMatchResult {
    let matches = match comparison {
        DateComparison::Equals => operands
            .iter()
            .filter_map(|expected| parse_date_epoch_nanos(expected))
            .any(|expected| actual_nanos == expected),
        DateComparison::NotEquals => {
            operands
                .iter()
                .all(|expected| match parse_date_epoch_nanos(expected) {
                    Some(expected) => actual_nanos != expected,
                    None => true,
                })
        }
        DateComparison::LessThan => operands
            .iter()
            .filter_map(|expected| parse_date_epoch_nanos(expected))
            .any(|expected| actual_nanos < expected),
        DateComparison::LessThanEquals => operands
            .iter()
            .filter_map(|expected| parse_date_epoch_nanos(expected))
            .any(|expected| actual_nanos <= expected),
        DateComparison::GreaterThan => operands
            .iter()
            .filter_map(|expected| parse_date_epoch_nanos(expected))
            .any(|expected| actual_nanos > expected),
        DateComparison::GreaterThanEquals => operands
            .iter()
            .filter_map(|expected| parse_date_epoch_nanos(expected))
            .any(|expected| actual_nanos >= expected),
    };
    if matches {
        ConditionMatchResult::Matches
    } else {
        ConditionMatchResult::NoMatch
    }
}

fn eval_date_equals(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult {
    eval_date_comparison(operands, actual, DateComparison::Equals, false)
}

fn eval_date_equals_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_date_comparison(operands, actual, DateComparison::Equals, true)
}

fn eval_date_not_equals(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult {
    eval_date_comparison(operands, actual, DateComparison::NotEquals, false)
}

fn eval_date_less_than(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult {
    eval_date_comparison(operands, actual, DateComparison::LessThan, false)
}

fn eval_date_less_than_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_date_comparison(operands, actual, DateComparison::LessThan, true)
}

fn eval_date_less_than_equals(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_date_comparison(operands, actual, DateComparison::LessThanEquals, false)
}

fn eval_date_less_than_equals_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_date_comparison(operands, actual, DateComparison::LessThanEquals, true)
}

fn eval_date_greater_than(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_date_comparison(operands, actual, DateComparison::GreaterThan, false)
}

fn eval_date_greater_than_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_date_comparison(operands, actual, DateComparison::GreaterThan, true)
}

fn eval_date_greater_than_equals(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_date_comparison(operands, actual, DateComparison::GreaterThanEquals, false)
}

fn eval_date_greater_than_equals_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_date_comparison(operands, actual, DateComparison::GreaterThanEquals, true)
}

fn parse_date_epoch_nanos(value: &str) -> Option<i128> {
    let (datetime, offset_seconds) = split_datetime_offset(value)?;
    let (date, time) = datetime.split_once('T')?;
    let (year, month, day) = parse_date(date)?;
    let (hour, minute, second, nanos) = parse_time(time)?;
    let days = days_from_civil(year, month, day)?;
    let seconds = i128::from(days) * 86_400
        + i128::from(hour) * 3_600
        + i128::from(minute) * 60
        + i128::from(second)
        - i128::from(offset_seconds);
    Some(seconds * 1_000_000_000 + i128::from(nanos))
}

fn split_datetime_offset(value: &str) -> Option<(&str, i32)> {
    if let Some(datetime) = value.strip_suffix('Z') {
        return Some((datetime, 0));
    }
    let time_start = value.find('T')? + 1;
    let offset_pos = value[time_start..]
        .rfind(['+', '-'])
        .map(|pos| time_start + pos)?;
    let offset = &value[offset_pos..];
    let sign = if offset.starts_with('+') { 1 } else { -1 };
    let offset = &offset[1..];
    let (hours, minutes) = offset.split_once(':')?;
    if hours.len() != 2 || minutes.len() != 2 {
        return None;
    }
    let hours = parse_fixed_u32(hours)?;
    let minutes = parse_fixed_u32(minutes)?;
    if hours > 23 || minutes > 59 {
        return None;
    }
    let offset_seconds = i32::try_from(hours * 3_600 + minutes * 60).ok()? * sign;
    Some((&value[..offset_pos], offset_seconds))
}

fn parse_date(value: &str) -> Option<(i32, u32, u32)> {
    let mut parts = value.split('-');
    let year = parts.next()?;
    let month = parts.next()?;
    let day = parts.next()?;
    if parts.next().is_some() || year.len() != 4 || month.len() != 2 || day.len() != 2 {
        return None;
    }
    let year = i32::try_from(parse_fixed_u32(year)?).ok()?;
    let month = parse_fixed_u32(month)?;
    let day = parse_fixed_u32(day)?;
    if year == 0 || !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return None;
    }
    Some((year, month, day))
}

fn parse_time(value: &str) -> Option<(u32, u32, u32, u32)> {
    let mut parts = value.split(':');
    let hour = parts.next()?;
    let minute = parts.next()?;
    let second = parts.next()?;
    if parts.next().is_some() || hour.len() != 2 || minute.len() != 2 {
        return None;
    }
    let (second, nanos) = parse_second_and_nanos(second)?;
    let hour = parse_fixed_u32(hour)?;
    let minute = parse_fixed_u32(minute)?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some((hour, minute, second, nanos))
}

fn parse_second_and_nanos(value: &str) -> Option<(u32, u32)> {
    let (second, fraction) = value.split_once('.').unwrap_or((value, ""));
    if second.len() != 2 {
        return None;
    }
    let second = parse_fixed_u32(second)?;
    let nanos = if fraction.is_empty() {
        0
    } else {
        if fraction.len() > 9 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        let mut nanos = 0;
        for index in 0..9 {
            nanos *= 10;
            nanos += fraction
                .as_bytes()
                .get(index)
                .map_or(0, |byte| u32::from(byte - b'0'));
        }
        nanos
    };
    Some((second, nanos))
}

fn parse_fixed_u32(value: &str) -> Option<u32> {
    if value.bytes().all(|byte| byte.is_ascii_digit()) {
        value.parse().ok()
    } else {
        None
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    let month = i32::try_from(month).ok()?;
    let day = i32::try_from(day).ok()?;
    let year = year - i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_for_formula = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_for_formula + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some(i64::from(era * 146_097 + day_of_era - 719_468))
}

fn eval_bool(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult {
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
        ActualValue::PresentValues(_)
        | ActualValue::SourceIp(_)
        | ActualValue::Numeric(_)
        | ActualValue::EpochSeconds(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn bool_operand_matches(operands: &[PolicyValue], actual: &str) -> bool {
    operands
        .iter()
        .any(|expected| expected.eq_ignore_ascii_case(actual))
}

fn eval_bool_if_exists(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(_) | ActualValue::PresentValues(_) => eval_bool(operands, actual),
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_all_values_bool(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if bool_operand_matches(operands, actual) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(actuals) => {
            if actuals
                .iter()
                .all(|actual| bool_operand_matches(operands, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_any_value_bool(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if bool_operand_matches(operands, actual) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(actuals) => {
            if actuals
                .iter()
                .any(|actual| bool_operand_matches(operands, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_string_equals(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .any(|expected| policy_string_equals(expected, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(_)
        | ActualValue::SourceIp(_)
        | ActualValue::Numeric(_)
        | ActualValue::EpochSeconds(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_for_all_values_string_equals(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .any(|expected| policy_string_equals(expected, actual))
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
                    .any(|expected| policy_string_equals(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_any_value_string_equals(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .any(|expected| policy_string_equals(expected, actual))
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
                    .any(|expected| policy_string_equals(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_string_equals_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(_) | ActualValue::PresentValues(_) => {
            eval_string_equals(operands, actual)
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_string_equals_ignore_case(
    operands: &[PolicyValue],
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
        ActualValue::PresentValues(_)
        | ActualValue::SourceIp(_)
        | ActualValue::Numeric(_)
        | ActualValue::EpochSeconds(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_for_all_values_string_equals_ignore_case(
    operands: &[PolicyValue],
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
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_any_value_string_equals_ignore_case(
    operands: &[PolicyValue],
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
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_string_equals_ignore_case_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(_) | ActualValue::PresentValues(_) => {
            eval_string_equals_ignore_case(operands, actual)
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_string_not_equals(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .all(|expected| !policy_string_equals(expected, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(_)
        | ActualValue::SourceIp(_)
        | ActualValue::Numeric(_)
        | ActualValue::EpochSeconds(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_all_values_string_not_equals(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .all(|expected| !policy_string_equals(expected, actual))
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
                    .all(|expected| !policy_string_equals(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_any_value_string_not_equals(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .all(|expected| !policy_string_equals(expected, actual))
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
                    .all(|expected| !policy_string_equals(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_string_not_equals_ignore_case(
    operands: &[PolicyValue],
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
        ActualValue::PresentValues(_)
        | ActualValue::SourceIp(_)
        | ActualValue::Numeric(_)
        | ActualValue::EpochSeconds(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_all_values_string_not_equals_ignore_case(
    operands: &[PolicyValue],
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
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_any_value_string_not_equals_ignore_case(
    operands: &[PolicyValue],
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
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_string_like(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .any(|expected| policy_value_wildcard_matches(expected, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(_)
        | ActualValue::SourceIp(_)
        | ActualValue::Numeric(_)
        | ActualValue::EpochSeconds(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_for_all_values_string_like(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .any(|expected| policy_value_wildcard_matches(expected, actual))
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
                    .any(|expected| policy_value_wildcard_matches(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_any_value_string_like(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .any(|expected| policy_value_wildcard_matches(expected, actual))
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
                    .any(|expected| policy_value_wildcard_matches(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_string_like_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(_) | ActualValue::PresentValues(_) => {
            eval_string_like(operands, actual)
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_string_not_like(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .all(|expected| !policy_value_wildcard_matches(expected, actual))
            {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::PresentValues(_)
        | ActualValue::SourceIp(_)
        | ActualValue::Numeric(_)
        | ActualValue::EpochSeconds(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_all_values_string_not_like(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .all(|expected| !policy_value_wildcard_matches(expected, actual))
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
                    .all(|expected| !policy_value_wildcard_matches(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
        ActualValue::Absent => ConditionMatchResult::Matches,
    }
}

fn eval_for_any_value_string_not_like(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Present(actual) => {
            if operands
                .iter()
                .all(|expected| !policy_value_wildcard_matches(expected, actual))
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
                    .all(|expected| !policy_value_wildcard_matches(expected, actual))
            }) {
                ConditionMatchResult::Matches
            } else {
                ConditionMatchResult::NoMatch
            }
        }
        ActualValue::SourceIp(_) | ActualValue::Numeric(_) | ActualValue::EpochSeconds(_) => {
            ConditionMatchResult::NoMatch
        }
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
    operands: &[PolicyValue],
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
        ActualValue::Numeric(actual) | ActualValue::EpochSeconds(actual) => {
            eval_numeric_operands(operands, actual as f64, comparison)
        }
        ActualValue::PresentValues(_) | ActualValue::SourceIp(_) => ConditionMatchResult::NoMatch,
        ActualValue::Absent if if_exists || matches!(comparison, NumericComparison::NotEquals) => {
            ConditionMatchResult::Matches
        }
        ActualValue::Absent => ConditionMatchResult::NoMatch,
    }
}

fn eval_numeric_operands(
    operands: &[PolicyValue],
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

fn eval_numeric_equals(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::Equals, false)
}

fn eval_numeric_equals_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::Equals, true)
}

fn eval_numeric_not_equals(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::NotEquals, false)
}

fn eval_numeric_less_than(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::LessThan, false)
}

fn eval_numeric_less_than_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::LessThan, true)
}

fn eval_numeric_less_than_equals(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::LessThanEquals, false)
}

fn eval_numeric_less_than_equals_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::LessThanEquals, true)
}

fn eval_numeric_greater_than(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::GreaterThan, false)
}

fn eval_numeric_greater_than_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::GreaterThan, true)
}

fn eval_numeric_greater_than_equals(
    operands: &[PolicyValue],
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
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_numeric_comparison(operands, actual, NumericComparison::GreaterThanEquals, true)
}

fn parse_numeric(value: &str) -> Option<f64> {
    let parsed: f64 = value.parse().ok()?;
    parsed.is_finite().then_some(parsed)
}

fn eval_ip_address(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult {
    eval_ip_address_comparison(operands, actual, IpComparison::Equals, false)
}

fn eval_for_all_values_ip_address(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Absent => ConditionMatchResult::Matches,
        _ => eval_ip_address(operands, actual),
    }
}

fn eval_for_any_value_ip_address(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Absent => ConditionMatchResult::NoMatch,
        _ => eval_ip_address(operands, actual),
    }
}

fn eval_ip_address_if_exists(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    eval_ip_address_comparison(operands, actual, IpComparison::Equals, true)
}

fn eval_not_ip_address(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult {
    eval_ip_address_comparison(operands, actual, IpComparison::NotEquals, false)
}

fn eval_for_all_values_not_ip_address(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Absent => ConditionMatchResult::Matches,
        _ => eval_not_ip_address(operands, actual),
    }
}

fn eval_for_any_value_not_ip_address(
    operands: &[PolicyValue],
    actual: ActualValue<'_>,
) -> ConditionMatchResult {
    match actual {
        ActualValue::Absent => ConditionMatchResult::NoMatch,
        _ => eval_not_ip_address(operands, actual),
    }
}

#[derive(Clone, Copy)]
enum IpComparison {
    Equals,
    NotEquals,
}

fn eval_ip_address_comparison(
    operands: &[PolicyValue],
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
        ActualValue::Present(_)
        | ActualValue::PresentValues(_)
        | ActualValue::Numeric(_)
        | ActualValue::EpochSeconds(_)
        | ActualValue::Absent => false,
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

fn eval_null(operands: &[PolicyValue], actual: ActualValue<'_>) -> ConditionMatchResult {
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

    fn epoch_seconds(value: u64) -> ActualValue<'static> {
        ActualValue::EpochSeconds(value)
    }

    fn operands(values: &[&str]) -> Vec<PolicyValue> {
        values
            .iter()
            .map(|value| PolicyValue::literal(value))
            .collect()
    }

    #[test]
    fn lookup_unknown_operator_returns_none() {
        assert!(lookup("DefinitelyNotAnOperator").is_none());
    }

    #[test]
    fn arn_equals_has_a_distinct_operator_kind() {
        let op = lookup("ArnEquals").expect("ArnEquals is in the table");
        assert_eq!(op.kind, ConditionOpKind::ArnEquals);
        assert!(!is_string_condition_kind(op.kind));
        assert!(!is_numeric_condition_kind(op.kind));
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
    fn bool_if_exists_matches_absent_context() {
        let op = lookup("BoolIfExists").expect("BoolIfExists is in the table");
        assert_eq!(op.kind, ConditionOpKind::Bool);
        assert_eq!(
            (op.evaluate)(&operands(&["true"]), ActualValue::Absent),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&operands(&["true"]), present("true")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&operands(&["false"]), present("true")),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn bool_set_operators_apply_generic_missing_context_semantics() {
        let all = lookup("ForAllValues:Bool").expect("ForAllValues:Bool is in the table");
        let any = lookup("ForAnyValue:Bool").expect("ForAnyValue:Bool is in the table");
        let expected = operands(&["true"]);

        assert_eq!(all.kind, ConditionOpKind::Bool);
        assert_eq!(any.kind, ConditionOpKind::Bool);
        assert_eq!(
            (all.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (any.evaluate)(&expected, ActualValue::Absent),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (all.evaluate)(&expected, present_values(&[])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (any.evaluate)(&expected, present_values(&[])),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn bool_set_operators_match_present_values() {
        let all = lookup("ForAllValues:Bool").unwrap();
        let any = lookup("ForAnyValue:Bool").unwrap();
        let expected = operands(&["true"]);

        assert_eq!(
            (all.evaluate)(&expected, present("true")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (all.evaluate)(&expected, present_values(&["true", "TRUE"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (all.evaluate)(&expected, present_values(&["true", "false"])),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (any.evaluate)(&expected, present_values(&["false", "true"])),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (any.evaluate)(&expected, present_values(&["false"])),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn binary_equals_matches_decoded_base64_bytes() {
        let op = lookup("BinaryEquals").expect("BinaryEquals is in the table");
        assert_eq!(op.kind, ConditionOpKind::BinaryEquals);
        assert_eq!(
            (op.evaluate)(&operands(&["YQ=="]), present("YQ==")),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&operands(&["Yg=="]), present("YQ==")),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&operands(&["Kg=="]), present("YQ==")),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&operands(&["YQ=="]), present("not-base64")),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&operands(&["not-base64"]), present("YQ==")),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (op.evaluate)(&operands(&["YQ=="]), ActualValue::Absent),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn binary_equals_set_operators_match_decoded_values() {
        let all = lookup("ForAllValues:BinaryEquals").unwrap();
        let any = lookup("ForAnyValue:BinaryEquals").unwrap();
        assert_eq!(
            (all.evaluate)(
                &operands(&["YQ==", "Yg=="]),
                present_values(&["YQ==", "Yg=="])
            ),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (all.evaluate)(
                &operands(&["YQ==", "Yg=="]),
                present_values(&["YQ==", "Yw=="])
            ),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (all.evaluate)(&operands(&["YQ=="]), ActualValue::Absent),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (any.evaluate)(
                &operands(&["YQ==", "Yg=="]),
                present_values(&["Yw==", "Yg=="])
            ),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (any.evaluate)(
                &operands(&["YQ==", "Yg=="]),
                present_values(&["Yw==", "ZA=="])
            ),
            ConditionMatchResult::NoMatch
        );
        assert_eq!(
            (any.evaluate)(&operands(&["YQ=="]), ActualValue::Absent),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn binary_equals_if_exists_matches_absent_values() {
        let op = lookup("BinaryEqualsIfExists").unwrap();
        assert_eq!(op.kind, ConditionOpKind::BinaryEquals);
        assert_eq!(
            (op.evaluate)(&operands(&["YQ=="]), ActualValue::Absent),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            (op.evaluate)(&operands(&["YQ=="]), present("YQ==")),
            ConditionMatchResult::Matches
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
    fn numeric_operators_match_epoch_seconds_actuals() {
        let op = lookup("NumericGreaterThan").unwrap();
        assert_eq!(
            (op.evaluate)(&operands(&["946684800"]), epoch_seconds(1_704_067_200)),
            ConditionMatchResult::Matches
        );
        let op = lookup("NumericLessThan").unwrap();
        assert_eq!(
            (op.evaluate)(&operands(&["946684800"]), epoch_seconds(1_704_067_200)),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn date_operators_match_epoch_seconds_actuals() {
        let actual = epoch_seconds(1_704_067_200); // 2024-01-01T00:00:00Z
        let cases = [
            (
                "DateEquals",
                "2024-01-01T00:00:00Z",
                ConditionMatchResult::Matches,
            ),
            (
                "DateEquals",
                "2024-01-01T00:00:01Z",
                ConditionMatchResult::NoMatch,
            ),
            (
                "DateNotEquals",
                "2024-01-01T00:00:01Z",
                ConditionMatchResult::Matches,
            ),
            (
                "DateLessThan",
                "2024-01-01T00:00:01Z",
                ConditionMatchResult::Matches,
            ),
            (
                "DateLessThanEquals",
                "2024-01-01T00:00:00Z",
                ConditionMatchResult::Matches,
            ),
            (
                "DateGreaterThan",
                "2023-12-31T23:59:59Z",
                ConditionMatchResult::Matches,
            ),
            (
                "DateGreaterThanEquals",
                "2024-01-01T00:00:00Z",
                ConditionMatchResult::Matches,
            ),
        ];

        for (operator, expected, outcome) in cases {
            let op = lookup(operator).unwrap();
            assert_eq!(
                (op.evaluate)(&operands(&[expected]), actual.clone()),
                outcome,
                "{operator} {expected}"
            );
        }
    }

    #[test]
    fn date_if_exists_variants_match_absent_values() {
        for operator in [
            "DateEqualsIfExists",
            "DateNotEqualsIfExists",
            "DateLessThanIfExists",
            "DateLessThanEqualsIfExists",
            "DateGreaterThanIfExists",
            "DateGreaterThanEqualsIfExists",
        ] {
            let op = lookup(operator).unwrap();
            assert_eq!(
                (op.evaluate)(&operands(&["2024-01-01T00:00:00Z"]), ActualValue::Absent),
                ConditionMatchResult::Matches,
                "{operator}"
            );
        }
    }

    #[test]
    fn date_parser_accepts_utc_offsets_and_rejects_invalid_dates() {
        assert_eq!(
            parse_date_epoch_nanos("2024-01-01T01:00:00+01:00"),
            parse_date_epoch_nanos("2024-01-01T00:00:00Z")
        );
        assert_eq!(
            parse_date_epoch_nanos("2024-01-01T00:00:00.123Z"),
            Some(1_704_067_200_123_000_000)
        );
        assert_eq!(
            parse_date_epoch_nanos("2024-01-01T00:00:00.000000001Z"),
            Some(1_704_067_200_000_000_001)
        );
        assert!(parse_date_epoch_nanos("2024-02-30T00:00:00Z").is_none());
        assert!(parse_date_epoch_nanos("2024-01-01T00:00:00").is_none());
        assert!(parse_date_epoch_nanos("2024-01-01T00:00:00.0000000001Z").is_none());
    }

    #[test]
    fn date_fractional_seconds_are_not_truncated_to_millis() {
        let actual = epoch_seconds(1_704_067_200); // 2024-01-01T00:00:00Z
        let equals = lookup("DateEquals").unwrap();
        assert_eq!(
            (equals.evaluate)(&operands(&["2024-01-01T00:00:00.0001Z"]), actual.clone()),
            ConditionMatchResult::NoMatch
        );

        let less_than = lookup("DateLessThan").unwrap();
        assert_eq!(
            (less_than.evaluate)(&operands(&["2024-01-01T00:00:00.0001Z"]), actual.clone()),
            ConditionMatchResult::Matches
        );

        let greater_than_equals = lookup("DateGreaterThanEquals").unwrap();
        assert_eq!(
            (greater_than_equals.evaluate)(&operands(&["2024-01-01T00:00:00.0001Z"]), actual),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn date_invalid_operands_follow_operator_match_semantics() {
        let less_than = lookup("DateLessThan").unwrap();
        assert_eq!(
            (less_than.evaluate)(&operands(&["not-a-date"]), epoch_seconds(1_704_067_200)),
            ConditionMatchResult::NoMatch
        );

        let not_equals = lookup("DateNotEquals").unwrap();
        assert_eq!(
            (not_equals.evaluate)(&operands(&["not-a-date"]), epoch_seconds(1_704_067_200)),
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
    fn for_all_values_string_not_equals_requires_every_actual_value_to_differ() {
        let op = lookup("ForAllValues:StringNotEquals").unwrap();
        assert_eq!(op.kind, ConditionOpKind::StringNotEquals);
        let expected = operands(&["security", "team"]);
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
    fn for_any_value_string_not_equals_requires_one_actual_value_to_differ() {
        let op = lookup("ForAnyValue:StringNotEquals").unwrap();
        assert_eq!(op.kind, ConditionOpKind::StringNotEquals);
        let expected = operands(&["security"]);
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
    fn source_ip_set_operators_handle_absent_context() {
        let operands = operands(&["127.0.0.0/8"]);

        for name in ["IpAddress", "ForAnyValue:IpAddress"] {
            let op = lookup(name).unwrap();
            assert_eq!(
                (op.evaluate)(&operands, ActualValue::Absent),
                ConditionMatchResult::NoMatch,
                "{name}"
            );
        }

        assert_eq!(
            (lookup("ForAllValues:IpAddress").unwrap().evaluate)(&operands, ActualValue::Absent),
            ConditionMatchResult::Matches
        );

        assert_eq!(
            (lookup("IpAddressIfExists").unwrap().evaluate)(&operands, ActualValue::Absent),
            ConditionMatchResult::Matches
        );

        for name in [
            "NotIpAddress",
            "ForAllValues:NotIpAddress",
            "NotIpAddressIfExists",
        ] {
            let op = lookup(name).unwrap();
            assert_eq!(
                (op.evaluate)(&operands, ActualValue::Absent),
                ConditionMatchResult::Matches,
                "{name}"
            );
        }

        assert_eq!(
            (lookup("ForAnyValue:NotIpAddress").unwrap().evaluate)(&operands, ActualValue::Absent),
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
