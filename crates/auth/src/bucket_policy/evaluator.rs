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
//! Tracked by `plans/completed/bucket-policy-evaluator-structure-plan.md`
//! phase 3.
//!
//! ## Non-goals
//!
//! - this phase does not build the IAM evaluator
//! - this phase does not wire the trait into `server-core`
//! - the existing public `BucketPolicy::evaluate` entry point is kept
//!   unchanged
//!
//! Consequence: every public item in this module currently has no
//! non-test caller. The `allow(dead_code)` below is a deliberate
//! seam-ahead-of-use. It should be removed when the IAM plan wires a
//! second `PolicyEvaluator` into `server-core` via `combine_decisions`.

#![allow(dead_code)]

use super::{BucketPolicy, PolicyEffect, PolicyEvaluation, PolicyRequest};

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
impl PolicyEvaluator for BucketPolicy {
    fn evaluate_decision(&self, request: &PolicyRequest<'_>) -> PolicyDecision {
        policy_evaluation_to_decision(self.evaluate(request), PolicySourceKind::BucketPolicy)
    }
}

/// Translate a [`PolicyEvaluation`] from a single source into a
/// [`PolicyDecision`].
///
/// This is the one conversion between the legacy per-source outcome enum
/// and the source-tagged decision shape used by the combinator. Kept as a
/// free function so it can be exercised directly without constructing a
/// real `BucketPolicy`.
const fn policy_evaluation_to_decision(
    evaluation: PolicyEvaluation,
    source: PolicySourceKind,
) -> PolicyDecision {
    match evaluation {
        PolicyEvaluation::ExplicitAllow => PolicyDecision::allow(source),
        PolicyEvaluation::ExplicitDeny => PolicyDecision::deny(source),
        PolicyEvaluation::NoMatch => PolicyDecision::no_match(source),
    }
}

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

    #[test]
    fn policy_evaluation_to_decision_maps_all_three_cases() {
        assert_eq!(
            policy_evaluation_to_decision(PolicyEvaluation::ExplicitAllow, BUCKET),
            PolicyDecision::allow(BUCKET)
        );
        assert_eq!(
            policy_evaluation_to_decision(PolicyEvaluation::ExplicitDeny, BUCKET),
            PolicyDecision::deny(BUCKET)
        );
        assert_eq!(
            policy_evaluation_to_decision(PolicyEvaluation::NoMatch, BUCKET),
            PolicyDecision::no_match(BUCKET)
        );
    }

    mod bucket_policy_impl {
        //! End-to-end plumbing tests: parse a real bucket policy, invoke
        //! it through the [`PolicyEvaluator`] trait, and feed the decision
        //! into [`combine_decisions`]. Policy-evaluation semantics are
        //! covered elsewhere (`bucket_policy.rs` unit tests and the
        //! `bucket_policy_differential` integration tests); these tests
        //! only exercise the trait wiring and the combinator integration.

        use super::*;
        use crate::bucket_policy::{parse_bucket_policy, ExistingObjectTags, PolicyAction};

        const ALT_USER: &str = "arn:aws:iam::444455556666:user/alt";

        fn request_for(action: PolicyAction, principal: Option<&str>) -> PolicyRequest<'_> {
            PolicyRequest::for_object(
                action,
                "bucket",
                "key",
                principal,
                None,
                ExistingObjectTags::Available(&[]),
            )
        }

        #[test]
        fn allow_policy_produces_allow_decision_via_trait() {
            let policy = parse_bucket_policy(
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
            )
            .unwrap();
            let request = request_for(PolicyAction::GetObject, Some(ALT_USER));
            let decision = policy.evaluate_decision(&request);
            assert_eq!(decision, PolicyDecision::allow(BUCKET));
            assert_eq!(combine_decisions(&[decision]), FinalDecision::Allow);
        }

        #[test]
        fn deny_policy_produces_deny_decision_via_trait() {
            let policy = parse_bucket_policy(
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
            )
            .unwrap();
            let request = request_for(PolicyAction::GetObject, Some(ALT_USER));
            let decision = policy.evaluate_decision(&request);
            assert_eq!(decision, PolicyDecision::deny(BUCKET));
            assert_eq!(combine_decisions(&[decision]), FinalDecision::Deny);
        }

        #[test]
        fn no_match_policy_produces_no_match_decision_via_trait() {
            // Policy targets PutObject; request is GetObject, so nothing
            // matches.
            let policy = parse_bucket_policy(
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
            )
            .unwrap();
            let request = request_for(PolicyAction::GetObject, Some(ALT_USER));
            let decision = policy.evaluate_decision(&request);
            assert_eq!(decision, PolicyDecision::no_match(BUCKET));
            // Default-deny resolves the single no-match decision.
            assert_eq!(combine_decisions(&[decision]), FinalDecision::Deny);
        }

        #[test]
        fn allow_and_deny_statements_combine_via_combinator_to_deny() {
            // Explicit deny in a second statement wins over an allow in
            // the first, which the bucket-policy evaluator already
            // encodes as ExplicitDeny. This test validates that the
            // trait preserves that collapse and the combinator agrees.
            let policy = parse_bucket_policy(
                r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"},{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
            )
            .unwrap();
            let request = request_for(PolicyAction::GetObject, Some(ALT_USER));
            let decision = policy.evaluate_decision(&request);
            assert_eq!(decision, PolicyDecision::deny(BUCKET));
            assert_eq!(combine_decisions(&[decision]), FinalDecision::Deny);
        }

        #[test]
        fn trait_call_matches_inherent_evaluate() {
            // The inherent BucketPolicy::evaluate is the stable entry
            // point; the trait must produce a decision that round-trips
            // with it for every policy evaluation outcome.
            let policies = [
                (
                    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                    PolicyEvaluation::ExplicitAllow,
                ),
                (
                    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                    PolicyEvaluation::ExplicitDeny,
                ),
                (
                    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
                    PolicyEvaluation::NoMatch,
                ),
            ];
            for (json, expected_evaluation) in policies {
                let policy = parse_bucket_policy(json).unwrap();
                let request = request_for(PolicyAction::GetObject, Some(ALT_USER));
                assert_eq!(policy.evaluate(&request), expected_evaluation);
                let decision = policy.evaluate_decision(&request);
                assert_eq!(
                    decision,
                    policy_evaluation_to_decision(expected_evaluation, BUCKET)
                );
            }
        }
    }
}
