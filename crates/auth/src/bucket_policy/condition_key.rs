//! Condition-key resolver table.
//!
//! One row per supported AWS IAM condition key. Each row owns the knowledge
//! of how to pull the actual value out of a [`PolicyRequest`] and which
//! operator families and actions make sense for that key. The evaluator in
//! `super::bucket_policy` routes its dispatch through this table so that
//! adding a new condition key is a one-row change instead of a new arm in
//! several different match statements.
//!
//! The existing evaluator continues to use its hand-rolled dispatch until
//! the migration commits route through [`CONDITION_KEYS`].

use super::condition_op::{self, ActualValue, ConditionOpKind};
use super::{
    BucketTagValue, ConditionMatchResult, ExistingObjectTagValue, PolicyAction,
    PolicyConditionClause, PolicyRequest,
};

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
/// Order is not load-bearing; lookup is a linear scan and prefix keys match
/// only on their literal prefix, so the table cannot have ambiguous rows.
pub(super) const CONDITION_KEYS: &[ConditionKeyResolver] = &[
    ConditionKeyResolver {
        key: KeyMatch::Prefix("s3:ExistingObjectTag/"),
        operator_support: OperatorSupport::StringEqualsOnly,
        resolve: resolve_existing_object_tag,
        evaluable_for_action: Some(existing_object_tag_evaluable_for_action),
        supported_for_action: Some(existing_object_tag_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Prefix("s3:BucketTag/"),
        operator_support: OperatorSupport::AnyEvaluable,
        resolve: resolve_bucket_tag,
        evaluable_for_action: None,
        supported_for_action: Some(bucket_tag_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Prefix("s3:RequestObjectTag/"),
        operator_support: OperatorSupport::AnyEvaluable,
        resolve: resolve_request_object_tag,
        evaluable_for_action: None,
        supported_for_action: Some(request_object_tag_supported_for_action),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-copy-source"),
        operator_support: OperatorSupport::AnyEvaluable,
        resolve: resolve_copy_source,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-metadata-directive"),
        operator_support: OperatorSupport::AnyEvaluable,
        resolve: resolve_metadata_directive,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-acl"),
        operator_support: OperatorSupport::AnyEvaluable,
        resolve: resolve_canned_acl,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-server-side-encryption"),
        operator_support: OperatorSupport::AnyEvaluable,
        resolve: resolve_server_side_encryption,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-server-side-encryption-customer-algorithm"),
        operator_support: OperatorSupport::AnyEvaluable,
        resolve: resolve_sse_customer_algorithm,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-grant-read"),
        operator_support: OperatorSupport::AnyEvaluable,
        resolve: resolve_grant_read,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-grant-write"),
        operator_support: OperatorSupport::AnyEvaluable,
        resolve: resolve_grant_write,
        evaluable_for_action: None,
        // grant-write and grant-write-acp are intentionally supported for
        // all actions, including GetObject/GetObjectVersion. Preserves the
        // asymmetry documented in the legacy request_header_condition_
        // supported_for_action predicate.
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-grant-read-acp"),
        operator_support: OperatorSupport::AnyEvaluable,
        resolve: resolve_grant_read_acp,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-grant-write-acp"),
        operator_support: OperatorSupport::AnyEvaluable,
        resolve: resolve_grant_write_acp,
        evaluable_for_action: None,
        supported_for_action: None,
    },
    ConditionKeyResolver {
        key: KeyMatch::Exact("s3:x-amz-grant-full-control"),
        operator_support: OperatorSupport::AnyEvaluable,
        resolve: resolve_grant_full_control,
        evaluable_for_action: None,
        supported_for_action: Some(request_header_supported_for_action_non_get),
    },
];

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

fn resolve_existing_object_tag<'a>(request: &PolicyRequest<'a>, param: &str) -> ResolvedValue<'a> {
    match request.existing_object_tag_value(param) {
        ExistingObjectTagValue::Unavailable => ResolvedValue::Unavailable,
        ExistingObjectTagValue::Available(Some(value)) => ResolvedValue::Present(value),
        ExistingObjectTagValue::Available(None) => ResolvedValue::Absent,
    }
}

fn resolve_request_object_tag<'a>(request: &PolicyRequest<'a>, param: &str) -> ResolvedValue<'a> {
    option_to_resolved(request.request_object_tag_value(param))
}

fn resolve_bucket_tag<'a>(request: &PolicyRequest<'a>, param: &str) -> ResolvedValue<'a> {
    match request.bucket_tag_value(param) {
        BucketTagValue::Unavailable => ResolvedValue::Unavailable,
        BucketTagValue::Available(Some(value)) => ResolvedValue::Present(value),
        BucketTagValue::Available(None) => ResolvedValue::Absent,
    }
}

fn resolve_copy_source<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    option_to_resolved(request.copy_source())
}

fn resolve_metadata_directive<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    option_to_resolved(request.metadata_directive())
}

fn resolve_canned_acl<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    option_to_resolved(request.canned_acl())
}

fn resolve_server_side_encryption<'a>(
    request: &PolicyRequest<'a>,
    _param: &str,
) -> ResolvedValue<'a> {
    option_to_resolved(request.server_side_encryption())
}

fn resolve_sse_customer_algorithm<'a>(
    request: &PolicyRequest<'a>,
    _param: &str,
) -> ResolvedValue<'a> {
    option_to_resolved(request.sse_customer_algorithm())
}

fn resolve_grant_read<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    option_to_resolved(request.grant_read())
}

fn resolve_grant_write<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    option_to_resolved(request.grant_write())
}

fn resolve_grant_read_acp<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    option_to_resolved(request.grant_read_acp())
}

fn resolve_grant_write_acp<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    option_to_resolved(request.grant_write_acp())
}

fn resolve_grant_full_control<'a>(request: &PolicyRequest<'a>, _param: &str) -> ResolvedValue<'a> {
    option_to_resolved(request.grant_full_control())
}

fn option_to_resolved(value: Option<&str>) -> ResolvedValue<'_> {
    match value {
        Some(value) => ResolvedValue::Present(value),
        None => ResolvedValue::Absent,
    }
}

fn existing_object_tag_evaluable_for_action(action: PolicyAction) -> bool {
    !matches!(
        action,
        PolicyAction::GetObjectAttributes | PolicyAction::GetObjectVersionAttributes
    )
}

fn existing_object_tag_supported_for_action(action: PolicyAction) -> bool {
    !matches!(
        action,
        PolicyAction::PutObject
            | PolicyAction::GetObjectRetention
            | PolicyAction::GetObjectLegalHold
            | PolicyAction::PutObjectRetention
            | PolicyAction::PutObjectLegalHold
            | PolicyAction::BypassGovernanceRetention
            | PolicyAction::DeleteObject
            | PolicyAction::DeleteObjectVersion
    )
}

fn request_object_tag_supported_for_action(action: PolicyAction) -> bool {
    !matches!(
        action,
        PolicyAction::PutObjectAcl
            | PolicyAction::PutObjectRetention
            | PolicyAction::PutObjectLegalHold
    )
}

fn bucket_tag_supported_for_action(action: PolicyAction) -> bool {
    matches!(
        action,
        PolicyAction::GetObject
            | PolicyAction::PutObject
            | PolicyAction::GetBucketPolicy
            | PolicyAction::PutBucketPolicy
            | PolicyAction::DeleteBucketPolicy
            | PolicyAction::GetBucketLocation
            | PolicyAction::GetBucketCors
            | PolicyAction::GetBucketAcl
            | PolicyAction::GetBucketVersioning
            | PolicyAction::GetBucketOwnershipControls
            | PolicyAction::GetBucketTagging
            | PolicyAction::GetEncryptionConfiguration
            | PolicyAction::GetLifecycleConfiguration
            | PolicyAction::GetBucketPolicyStatus
            | PolicyAction::GetBucketPublicAccessBlock
            | PolicyAction::GetBucketObjectLockConfiguration
            | PolicyAction::ListBucket
            | PolicyAction::ListBucketVersions
            | PolicyAction::ListBucketMultipartUploads
            | PolicyAction::PutBucketAcl
            | PolicyAction::PutBucketCors
            | PolicyAction::PutBucketVersioning
            | PolicyAction::PutBucketOwnershipControls
            | PolicyAction::PutBucketTagging
            | PolicyAction::PutEncryptionConfiguration
            | PolicyAction::PutLifecycleConfiguration
            | PolicyAction::PutBucketPublicAccessBlock
            | PolicyAction::PutBucketObjectLockConfiguration
    )
}

/// Shared support predicate for request-header keys that are not evaluable
/// on read paths.
fn request_header_supported_for_action_non_get(action: PolicyAction) -> bool {
    !matches!(
        action,
        PolicyAction::GetObject | PolicyAction::GetObjectVersion
    )
}

/// Whether a condition clause is supported on a given action at
/// statement-validation time.
///
/// A clause is supported when its key has a resolver row, the operator is
/// compatible with the row's `operator_support`, and the row's
/// `supported_for_action` predicate (if any) admits the action.
pub(super) fn supports_clause_for_action(
    clause: &PolicyConditionClause,
    action: PolicyAction,
) -> bool {
    let Some((resolver, _param)) = lookup(clause.key.as_str()) else {
        return false;
    };
    let operator = clause.operator.as_str();
    let operator_ok = match resolver.operator_support {
        OperatorSupport::AnyEvaluable => {
            condition_op::is_evaluable_on_evaluable_object_actions(operator)
        }
        OperatorSupport::StringEqualsOnly => {
            matches!(operator, "StringEquals" | "StringEqualsIfExists")
        }
    };
    if !operator_ok {
        return false;
    }
    resolver
        .supported_for_action
        .is_none_or(|predicate| predicate(action))
}

/// Whether a resolver key is evaluable for an action at evaluator time.
///
/// Returns `true` by default when the resolver does not constrain
/// evaluability, or when the key is not in the table (the caller typically
/// treats unknown keys as not requiring this gate).
pub(super) fn key_is_evaluable_for_action(key: &str, action: PolicyAction) -> bool {
    let Some((resolver, _)) = lookup(key) else {
        return true;
    };
    resolver
        .evaluable_for_action
        .is_none_or(|predicate| predicate(action))
}

/// Evaluate a condition clause against a request.
///
/// Single entry point for the evaluator. Returns `Unsupported` if the
/// condition key is not in the table or if the operator is not in the
/// resolver's allowed set.
pub(super) fn evaluate_clause(
    clause: &PolicyConditionClause,
    request: &PolicyRequest<'_>,
) -> ConditionMatchResult {
    let Some((resolver, param)) = lookup(clause.key.as_str()) else {
        return ConditionMatchResult::Unsupported;
    };
    if let Some(evaluable) = resolver.evaluable_for_action {
        if !evaluable(request.action()) {
            return ConditionMatchResult::AcceptedButNotEvaluable;
        }
    }
    let Some(op) = condition_op::lookup(clause.operator.as_str()) else {
        return ConditionMatchResult::Unsupported;
    };
    match resolver.operator_support {
        OperatorSupport::AnyEvaluable => {}
        OperatorSupport::StringEqualsOnly => {
            if op.kind != ConditionOpKind::StringEquals {
                return ConditionMatchResult::Unsupported;
            }
        }
    }
    let actual = match (resolver.resolve)(request, param) {
        ResolvedValue::Present(value) => ActualValue::Present(value),
        ResolvedValue::Absent => ActualValue::Absent,
        ResolvedValue::Unavailable => return ConditionMatchResult::InputUnavailable,
    };
    (op.evaluate)(&clause.values, actual)
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
    fn lookup_existing_object_tag_returns_prefix_resolver() {
        let (resolver, param) = lookup("s3:ExistingObjectTag/classification").unwrap();
        assert_eq!(param, "classification");
        assert_eq!(resolver.operator_support, OperatorSupport::StringEqualsOnly);
        assert!(resolver.evaluable_for_action.is_some());
        assert!(resolver.supported_for_action.is_some());
    }

    #[test]
    fn lookup_request_object_tag_returns_prefix_resolver() {
        let (resolver, param) = lookup("s3:RequestObjectTag/classification").unwrap();
        assert_eq!(param, "classification");
        assert_eq!(resolver.operator_support, OperatorSupport::AnyEvaluable);
        assert!(resolver.evaluable_for_action.is_none());
    }

    #[test]
    fn lookup_exact_header_keys() {
        let (acl, param) = lookup("s3:x-amz-acl").unwrap();
        assert_eq!(param, "");
        assert_eq!(acl.operator_support, OperatorSupport::AnyEvaluable);

        let (sse, _) = lookup("s3:x-amz-server-side-encryption").unwrap();
        assert!(sse.supported_for_action.is_some());
    }

    #[test]
    fn lookup_unknown_key_returns_none() {
        assert!(lookup("aws:SourceVpc").is_none());
        assert!(lookup("").is_none());
    }

    #[test]
    fn grant_write_keys_are_supported_for_get_object() {
        // The legacy predicate intentionally allowed grant-write and
        // grant-write-acp on every action. The resolver preserves that by
        // leaving supported_for_action=None, so we test here that the
        // table row encodes this asymmetry.
        for key in ["s3:x-amz-grant-write", "s3:x-amz-grant-write-acp"] {
            let (resolver, _) = lookup(key).unwrap();
            assert!(
                resolver.supported_for_action.is_none(),
                "{key} should be supported for every action"
            );
        }

        // Counterexample: the read-flavor grants are supported only on
        // non-Get actions, via the shared predicate.
        for key in [
            "s3:x-amz-grant-read",
            "s3:x-amz-grant-read-acp",
            "s3:x-amz-grant-full-control",
        ] {
            let (resolver, _) = lookup(key).unwrap();
            let predicate = resolver
                .supported_for_action
                .expect("read-flavor grants have a support predicate");
            assert!(!predicate(PolicyAction::GetObject));
            assert!(!predicate(PolicyAction::GetObjectVersion));
            assert!(predicate(PolicyAction::PutObject));
        }
    }

    #[test]
    fn existing_object_tag_unevaluable_for_get_object_attributes() {
        let (resolver, _) = lookup("s3:ExistingObjectTag/x").unwrap();
        let predicate = resolver
            .evaluable_for_action
            .expect("ExistingObjectTag has an evaluability predicate");
        assert!(!predicate(PolicyAction::GetObjectAttributes));
        assert!(!predicate(PolicyAction::GetObjectVersionAttributes));
        assert!(predicate(PolicyAction::GetObject));
    }

    #[test]
    fn existing_object_tag_unsupported_for_retention_and_delete() {
        let (resolver, _) = lookup("s3:ExistingObjectTag/x").unwrap();
        let predicate = resolver
            .supported_for_action
            .expect("ExistingObjectTag has a support predicate");
        assert!(!predicate(PolicyAction::DeleteObject));
        assert!(!predicate(PolicyAction::DeleteObjectVersion));
        assert!(!predicate(PolicyAction::PutObjectRetention));
        assert!(!predicate(PolicyAction::PutObjectLegalHold));
        assert!(!predicate(PolicyAction::BypassGovernanceRetention));
        assert!(predicate(PolicyAction::GetObject));
        assert!(!predicate(PolicyAction::PutObject));
    }

    #[test]
    fn request_object_tag_unsupported_for_acl_and_lock_updates() {
        let (resolver, _) = lookup("s3:RequestObjectTag/x").unwrap();
        let predicate = resolver
            .supported_for_action
            .expect("RequestObjectTag has a support predicate");
        assert!(!predicate(PolicyAction::PutObjectAcl));
        assert!(!predicate(PolicyAction::PutObjectRetention));
        assert!(!predicate(PolicyAction::PutObjectLegalHold));
        assert!(predicate(PolicyAction::PutObject));
    }

    #[test]
    fn every_expected_key_has_a_row() {
        for key in [
            "s3:ExistingObjectTag/x",
            "s3:RequestObjectTag/x",
            "s3:x-amz-copy-source",
            "s3:x-amz-metadata-directive",
            "s3:x-amz-acl",
            "s3:x-amz-server-side-encryption",
            "s3:x-amz-server-side-encryption-customer-algorithm",
            "s3:x-amz-grant-read",
            "s3:x-amz-grant-write",
            "s3:x-amz-grant-read-acp",
            "s3:x-amz-grant-write-acp",
            "s3:x-amz-grant-full-control",
        ] {
            assert!(lookup(key).is_some(), "missing resolver for {key}");
        }
    }
}
