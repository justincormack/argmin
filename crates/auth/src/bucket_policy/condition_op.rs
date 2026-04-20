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
//! This file is introduced as dead code ahead of migration; the existing
//! evaluator continues to use its own operator match statements until the
//! migration commits route through [`CONDITION_OPS`].

#![allow(dead_code)]

use super::ConditionMatchResult;

/// Value presented to a condition operator by the evaluator.
///
/// The three branches exactly mirror the states the evaluator already
/// distinguishes:
///
/// - `Present(value)`: the condition key resolved to a concrete string
/// - `Absent`: the key is known but not supplied by the request
/// - `Unavailable`: the key is known but its value cannot be determined in
///   this context (for example, object tags on a `PutObject` request)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ActualValue<'a> {
    Present(&'a str),
    Absent,
    Unavailable,
}

/// The kind of values a condition operator expects on its right-hand side.
///
/// This is reserved for later phases where non-string operators (`IpAddress`,
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
/// Empty for now. Later commits populate it with `StringEquals`,
/// `StringEqualsIfExists`, `StringNotEquals`, `StringLike`,
/// `StringLikeIfExists`, `StringNotEquals`, `StringNotEqualsIfExists`,
/// `StringNotLike`, and `Null`.
pub(super) const CONDITION_OPS: &[ConditionOpDef] = &[];

/// Look up an operator definition by its wire name (for example
/// `"StringEquals"` or `"StringEqualsIfExists"`).
pub(super) fn lookup(name: &str) -> Option<&'static ConditionOpDef> {
    CONDITION_OPS.iter().find(|op| op.name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_unknown_operator_returns_none() {
        assert!(lookup("DefinitelyNotAnOperator").is_none());
    }

    #[test]
    fn table_is_empty_until_migration_begins() {
        // The table is intentionally empty until the phase 1 migration
        // commits begin populating it. Once an operator lands, this test
        // should be deleted rather than updated.
        assert!(CONDITION_OPS.is_empty());
    }
}
