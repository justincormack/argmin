//! Policy evaluator seam.
//!
//! This module introduces a small trait and decision type so that bucket
//! policy becomes one implementor among several when IAM work lands. It is
//! intentionally thin: the decision combinator currently has exactly one
//! input (a bucket policy decision), but the explicit-deny / default-deny
//! logic is already expressed and regression-tested here so that adding
//! identity policies, permission boundaries, or service control policies
//! later is a change of inputs rather than a change of rules.
//!
//! Tracked by `plans/bucket-policy-evaluator-structure-plan.md` phase 3.
//!
//! ## Non-goals
//!
//! - this phase does not build the IAM evaluator
//! - this phase does not wire the trait into `server-core`
//! - the existing public `BucketPolicy::evaluate` entry point is kept
//!   unchanged

#![allow(dead_code)]

use super::{PolicyEffect, PolicyRequest};

/// Which policy source produced a given decision.
///
/// AWS combines bucket policy, IAM identity policy, permission boundaries,
/// session policies, and service control policies using source-dependent
/// rules. Phase 3 only needs to represent bucket policy; additional
/// variants will arrive with the IAM plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PolicySourceKind {
    BucketPolicy,
}

/// One evaluator's decision about a given request.
///
/// `effect = Some(Allow)` — the source matched an explicit allow statement.
/// `effect = Some(Deny)` — the source matched an explicit deny statement.
/// `effect = None`       — the source had no applicable statement.
///
/// The three-valued shape mirrors the existing `PolicyEvaluation` enum;
/// wrapping it in a struct with an attached `source` lets the combinator
/// remain source-aware without re-encoding the effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PolicyDecision {
    pub(crate) effect: Option<PolicyEffect>,
    pub(crate) source: PolicySourceKind,
}

impl PolicyDecision {
    pub(crate) const fn allow(source: PolicySourceKind) -> Self {
        Self {
            effect: Some(PolicyEffect::Allow),
            source,
        }
    }

    pub(crate) const fn deny(source: PolicySourceKind) -> Self {
        Self {
            effect: Some(PolicyEffect::Deny),
            source,
        }
    }

    pub(crate) const fn no_match(source: PolicySourceKind) -> Self {
        Self {
            effect: None,
            source,
        }
    }
}

/// The externally visible outcome after combining all applicable sources.
///
/// Distinct from [`PolicyDecision`] because the final answer is binary:
/// either the request is allowed or denied. There is no "no match"
/// because default-deny is the resolution for that case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FinalDecision {
    Allow,
    Deny,
}

/// A policy source that can produce a [`PolicyDecision`] for a given
/// request.
///
/// Named `evaluate_decision` rather than `evaluate` to avoid shadowing
/// the existing public `BucketPolicy::evaluate` method, which returns the
/// legacy `PolicyEvaluation` type and is the stable entry point for
/// existing callers.
pub(crate) trait PolicyEvaluator {
    fn evaluate_decision(&self, request: &PolicyRequest<'_>) -> PolicyDecision;
}

/// Combine a list of policy decisions into a final allow/deny answer.
///
/// AWS rules currently applied:
///
/// 1. Any explicit `Deny` from any source produces `Deny`.
/// 2. Otherwise, at least one explicit `Allow` produces `Allow`.
/// 3. Otherwise, `Deny` (default deny).
///
/// These are the source-independent rules. Source-specific rules such as
/// cross-account requiring both identity and resource policy, or permission
/// boundaries being restrictive rather than granting, will layer on top
/// when IAM arrives.
pub(crate) fn combine_decisions(decisions: &[PolicyDecision]) -> FinalDecision {
    if decisions
        .iter()
        .any(|d| d.effect == Some(PolicyEffect::Deny))
    {
        return FinalDecision::Deny;
    }
    if decisions
        .iter()
        .any(|d| d.effect == Some(PolicyEffect::Allow))
    {
        return FinalDecision::Allow;
    }
    FinalDecision::Deny
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUCKET: PolicySourceKind = PolicySourceKind::BucketPolicy;

    #[test]
    fn empty_list_defaults_to_deny() {
        assert_eq!(combine_decisions(&[]), FinalDecision::Deny);
    }

    #[test]
    fn sole_no_match_defaults_to_deny() {
        assert_eq!(
            combine_decisions(&[PolicyDecision::no_match(BUCKET)]),
            FinalDecision::Deny
        );
    }

    #[test]
    fn sole_allow_is_allow() {
        assert_eq!(
            combine_decisions(&[PolicyDecision::allow(BUCKET)]),
            FinalDecision::Allow
        );
    }

    #[test]
    fn sole_deny_is_deny() {
        assert_eq!(
            combine_decisions(&[PolicyDecision::deny(BUCKET)]),
            FinalDecision::Deny
        );
    }

    #[test]
    fn allow_plus_no_match_is_allow() {
        assert_eq!(
            combine_decisions(&[
                PolicyDecision::allow(BUCKET),
                PolicyDecision::no_match(BUCKET),
            ]),
            FinalDecision::Allow
        );
    }

    #[test]
    fn allow_plus_deny_yields_deny() {
        // Explicit deny wins over explicit allow, regardless of order.
        assert_eq!(
            combine_decisions(&[PolicyDecision::allow(BUCKET), PolicyDecision::deny(BUCKET),]),
            FinalDecision::Deny
        );
        assert_eq!(
            combine_decisions(&[PolicyDecision::deny(BUCKET), PolicyDecision::allow(BUCKET),]),
            FinalDecision::Deny
        );
    }

    #[test]
    fn deny_plus_multiple_allows_yields_deny() {
        assert_eq!(
            combine_decisions(&[
                PolicyDecision::allow(BUCKET),
                PolicyDecision::allow(BUCKET),
                PolicyDecision::deny(BUCKET),
                PolicyDecision::no_match(BUCKET),
            ]),
            FinalDecision::Deny
        );
    }

    #[test]
    fn all_no_match_yields_deny() {
        assert_eq!(
            combine_decisions(&[
                PolicyDecision::no_match(BUCKET),
                PolicyDecision::no_match(BUCKET),
            ]),
            FinalDecision::Deny
        );
    }

    #[test]
    fn multiple_allows_yield_allow() {
        assert_eq!(
            combine_decisions(&[PolicyDecision::allow(BUCKET), PolicyDecision::allow(BUCKET),]),
            FinalDecision::Allow
        );
    }

    #[test]
    fn decision_constructors_set_source_and_effect() {
        let allow = PolicyDecision::allow(BUCKET);
        assert_eq!(allow.effect, Some(PolicyEffect::Allow));
        assert_eq!(allow.source, BUCKET);

        let deny = PolicyDecision::deny(BUCKET);
        assert_eq!(deny.effect, Some(PolicyEffect::Deny));

        let none = PolicyDecision::no_match(BUCKET);
        assert_eq!(none.effect, None);
    }
}
