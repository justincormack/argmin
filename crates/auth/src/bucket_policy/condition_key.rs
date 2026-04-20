//! Condition-key resolver table.
//!
//! One row per supported AWS IAM condition key. Each row owns the knowledge
//! of how to pull the actual value out of a [`PolicyRequest`] and which
//! operator families and actions make sense for that key. The evaluator in
//! `super::bucket_policy` routes its dispatch through this table so that
//! adding a new condition key is a one-row change instead of a new arm in
//! several different match statements.
//!
//! This module is introduced as dead code ahead of the migration commits
//! tracked by `plans/bucket-policy-evaluator-structure-plan.md` phase 2.
//! The existing evaluator continues to use its hand-rolled dispatch until
//! those commits route through [`CONDITION_KEYS`].

#![allow(dead_code)]

use super::{PolicyAction, PolicyRequest};

/// Resolved value for a condition key in a given request.
///
/// Mirrors the existing evaluator's distinction between "known absent",
/// "present with this value", and "cannot be determined in this context".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResolvedValue<'a> {
    Present(&'a str),
    Absent,
    Unavailable,
}

/// Which operator families a condition key supports.
///
/// `AnyEvaluable` accepts every operator flagged with
/// `evaluable_on_evaluable_object_actions` in the operator table.
/// `StringEqualsOnly` narrows to the `StringEquals` / `StringEqualsIfExists`
/// fast path used by `s3:ExistingObjectTag/*`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OperatorSupport {
    AnyEvaluable,
    StringEqualsOnly,
}

/// How the resolver matches a request condition key.
///
/// Exact keys compare equal; prefix keys accept anything that starts with
/// the prefix string and pass the remainder to `resolve` as the parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KeyMatch {
    Exact(&'static str),
    Prefix(&'static str),
}

impl KeyMatch {
    fn match_key<'a>(&self, key: &'a str) -> Option<&'a str> {
        match self {
            Self::Exact(name) => (key == *name).then_some(""),
            Self::Prefix(prefix) => key.strip_prefix(prefix),
        }
    }
}

/// Signature for a per-key value resolver.
///
/// The second argument is the parameter portion for prefix keys (for
/// example the tag name after `s3:ExistingObjectTag/`) or `""` for exact
/// keys.
pub(super) type ResolveFn = for<'a> fn(&PolicyRequest<'a>, &str) -> ResolvedValue<'a>;

/// One row of the condition-key resolver table.
///
/// `evaluable_for_action`: if set, the evaluator returns
/// `ConditionMatchResult::AcceptedButNotEvaluable` at eval time when the
/// predicate returns false. This is how `s3:ExistingObjectTag/*` is
/// signalled as unevaluable on `GetObjectAttributes`-style actions where
/// tags are not available at the evaluator call site.
///
/// `supported_for_action`: if set, statement-level validation requires the
/// predicate to return true before the policy is accepted. A key that is
/// known for any action leaves this `None`; keys that apply only to a
/// subset of actions (for example `s3:RequestObjectTag/*` which is not
/// supported for retention updates) supply a predicate.
#[derive(Debug, Clone, Copy)]
pub(super) struct ConditionKeyResolver {
    pub(super) key: KeyMatch,
    pub(super) operator_support: OperatorSupport,
    pub(super) resolve: ResolveFn,
    pub(super) evaluable_for_action: Option<fn(PolicyAction) -> bool>,
    pub(super) supported_for_action: Option<fn(PolicyAction) -> bool>,
}

/// The compile-time condition-key table.
///
/// Empty for now. Later commits populate it with `s3:ExistingObjectTag/*`,
/// `s3:RequestObjectTag/*`, the `s3:x-amz-*` header keys, and their
/// per-action evaluability / support predicates.
pub(super) const CONDITION_KEYS: &[ConditionKeyResolver] = &[];

/// Look up a resolver for a given condition-key name.
///
/// Returns the resolver and the parameter portion of the key (or `""` for
/// exact-match keys).
pub(super) fn lookup(key: &str) -> Option<(&'static ConditionKeyResolver, &str)> {
    for resolver in CONDITION_KEYS {
        if let Some(param) = resolver.key.match_key(key) {
            return Some((resolver, param));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_match_exact_requires_equality() {
        let key = KeyMatch::Exact("s3:x-amz-acl");
        assert_eq!(key.match_key("s3:x-amz-acl"), Some(""));
        assert_eq!(key.match_key("s3:x-amz-acls"), None);
        assert_eq!(key.match_key("x-amz-acl"), None);
    }

    #[test]
    fn key_match_prefix_returns_remainder() {
        let key = KeyMatch::Prefix("s3:ExistingObjectTag/");
        assert_eq!(
            key.match_key("s3:ExistingObjectTag/classification"),
            Some("classification")
        );
        assert_eq!(key.match_key("s3:ExistingObjectTag/"), Some(""));
        assert_eq!(key.match_key("s3:RequestObjectTag/x"), None);
    }

    #[test]
    fn lookup_empty_table_returns_none() {
        // The table is intentionally empty until the phase 2 migration
        // commits begin populating it. Once a resolver row lands, this
        // test should be deleted rather than updated.
        assert!(CONDITION_KEYS.is_empty());
        assert!(lookup("s3:x-amz-acl").is_none());
    }
}
