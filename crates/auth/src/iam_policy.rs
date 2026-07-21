//! Typed IAM policy documents used by in-memory identity records.
//!
//! These constructors are the authorization-facing types. JSON parsing is a
//! separate boundary that will feed them; it must not expose a permissive raw
//! statement to evaluators.

use std::sync::Arc;

use crate::identity::{parse_iam_principal_arn, IamPrincipalArnKind};
use crate::policy::{action_pattern_matches, wildcard_matches, PolicyStatementCore};
use crate::{
    AwsAccountId, ConfiguredPrincipalIdentity, IamRoleIdentity, PolicyAction, PolicyEffect,
    PolicyEvaluation, PolicyVersion,
};

const IAM_POLICY_NAME_MAX_LEN: usize = 128;
const IAM_ACTION_PATTERN_MAX_LEN: usize = 128;
const IAM_RESOURCE_PATTERN_MAX_LEN: usize = 2048;

/// Invalid typed IAM policy input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum IamPolicyError {
    #[error("invalid inline policy name")]
    InvalidPolicyName,
    #[error("invalid IAM action pattern")]
    InvalidActionPattern,
    #[error("invalid IAM resource pattern")]
    InvalidResourcePattern,
    #[error("an IAM policy must contain at least one statement")]
    EmptyPolicy,
    #[error("an IAM policy statement must contain at least one action")]
    EmptyActions,
    #[error("an IAM identity policy statement must contain at least one resource")]
    EmptyResources,
    #[error("a role trust statement must contain at least one principal")]
    EmptyPrincipals,
    #[error("invalid role trust principal")]
    InvalidTrustPrincipal,
}

/// IAM inline-policy name.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct InlinePolicyName(String);

impl InlinePolicyName {
    pub fn new(value: impl Into<String>) -> Result<Self, IamPolicyError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= IAM_POLICY_NAME_MAX_LEN
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(byte, b'_' | b'+' | b'=' | b',' | b'.' | b'@' | b'-')
            });
        valid
            .then_some(Self(value))
            .ok_or(IamPolicyError::InvalidPolicyName)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for InlinePolicyName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&observability::escaped(&self.0), f)
    }
}

/// Validated action pattern for the currently supported IAM policy surface.
#[derive(Clone, PartialEq, Eq)]
pub struct IamActionPattern(String);

impl IamActionPattern {
    pub fn new(value: impl Into<String>) -> Result<Self, IamPolicyError> {
        let value = value.into();
        let valid_shape = !value.is_empty()
            && value.len() <= IAM_ACTION_PATTERN_MAX_LEN
            && value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b':' | b'*' | b'?' | b'-')
            });
        let supported = crate::bucket_policy::policy_action_pattern_is_supported(&value)
            || action_pattern_matches(&value, "sts:AssumeRole");
        (valid_shape && supported)
            .then_some(Self(value))
            .ok_or(IamPolicyError::InvalidActionPattern)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for IamActionPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&observability::escaped(&self.0), f)
    }
}

/// Validated resource pattern in an IAM identity or session policy.
#[derive(Clone, PartialEq, Eq)]
pub struct IamResourcePattern(String);

impl IamResourcePattern {
    pub fn new(value: impl Into<String>) -> Result<Self, IamPolicyError> {
        let value = value.into();
        (!value.is_empty()
            && value.len() <= IAM_RESOURCE_PATTERN_MAX_LEN
            && value.chars().all(|character| !character.is_control()))
        .then_some(Self(value))
        .ok_or(IamPolicyError::InvalidResourcePattern)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for IamResourcePattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&observability::escaped(&self.0), f)
    }
}

#[derive(Clone, PartialEq, Eq)]
enum RoleTrustPrincipalKind {
    Any,
    Account(AwsAccountId),
    AccountRoot(AwsAccountId),
    ExactIamPrincipal(AwsAccountId),
}

/// Principal in an initial role trust-policy statement.
#[derive(Clone, PartialEq, Eq)]
pub struct RoleTrustPrincipal {
    value: String,
    kind: RoleTrustPrincipalKind,
}

impl RoleTrustPrincipal {
    pub fn new(value: impl Into<String>) -> Result<Self, IamPolicyError> {
        let value = value.into();
        let kind = if value == "*" {
            Some(RoleTrustPrincipalKind::Any)
        } else if s3_types::is_valid_aws_account_id(&value) {
            Some(RoleTrustPrincipalKind::Account(
                AwsAccountId::new(value.clone()).expect("validated AWS account ID"),
            ))
        } else {
            parse_iam_principal_arn(&value).map(|principal| match principal.kind() {
                IamPrincipalArnKind::Root => {
                    RoleTrustPrincipalKind::AccountRoot(principal.account_id().clone())
                }
                IamPrincipalArnKind::User | IamPrincipalArnKind::Role => {
                    RoleTrustPrincipalKind::ExactIamPrincipal(principal.account_id().clone())
                }
            })
        };
        (value.len() <= IAM_RESOURCE_PATTERN_MAX_LEN)
            .then_some(kind)
            .flatten()
            .map(|kind| Self { value, kind })
            .ok_or(IamPolicyError::InvalidTrustPrincipal)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl std::fmt::Debug for RoleTrustPrincipal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&observability::escaped(&self.value), f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RoleTrustPolicyEvaluation {
    ExplicitDeny,
    SameAccountDirectAllow,
    DelegatedAllow,
    NoMatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InvalidRoleTrustCaller;

#[derive(Clone, Copy, PartialEq, Eq)]
enum RoleTrustPrincipalMatch {
    Delegated,
    SameAccountDirect,
}

fn strongest_principal_match(
    left: Option<RoleTrustPrincipalMatch>,
    right: Option<RoleTrustPrincipalMatch>,
) -> Option<RoleTrustPrincipalMatch> {
    if left == Some(RoleTrustPrincipalMatch::SameAccountDirect)
        || right == Some(RoleTrustPrincipalMatch::SameAccountDirect)
    {
        Some(RoleTrustPrincipalMatch::SameAccountDirect)
    } else {
        left.or(right)
    }
}

impl RoleTrustPrincipal {
    fn matches_configured_caller(
        &self,
        caller_account_id: &AwsAccountId,
        caller_principal: &ConfiguredPrincipalIdentity,
        target_role: &IamRoleIdentity,
    ) -> Option<RoleTrustPrincipalMatch> {
        match &self.kind {
            RoleTrustPrincipalKind::Any => Some(if caller_account_id == target_role.account_id() {
                RoleTrustPrincipalMatch::SameAccountDirect
            } else {
                RoleTrustPrincipalMatch::Delegated
            }),
            RoleTrustPrincipalKind::Account(account_id)
            | RoleTrustPrincipalKind::AccountRoot(account_id) => {
                (account_id == caller_account_id).then_some(RoleTrustPrincipalMatch::Delegated)
            }
            RoleTrustPrincipalKind::ExactIamPrincipal(account_id) => {
                (account_id == caller_account_id && self.value == caller_principal.principal())
                    .then_some(if caller_account_id == target_role.account_id() {
                        RoleTrustPrincipalMatch::SameAccountDirect
                    } else {
                        RoleTrustPrincipalMatch::Delegated
                    })
            }
        }
    }
}

/// Principal-free statement valid for an identity or session policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityPolicyStatement {
    core: PolicyStatementCore,
    resources: Vec<IamResourcePattern>,
}

impl IdentityPolicyStatement {
    pub fn new(
        effect: PolicyEffect,
        actions: Vec<IamActionPattern>,
        resources: Vec<IamResourcePattern>,
    ) -> Result<Self, IamPolicyError> {
        if actions.is_empty() {
            return Err(IamPolicyError::EmptyActions);
        }
        if resources.is_empty() {
            return Err(IamPolicyError::EmptyResources);
        }
        Ok(Self {
            core: PolicyStatementCore::new(
                None,
                effect,
                actions
                    .iter()
                    .map(|action| action.as_str().to_string())
                    .collect(),
                Vec::new(),
            ),
            resources,
        })
    }

    fn matches(&self, action: &str, resource: &str) -> bool {
        self.core
            .actions
            .iter()
            .any(|pattern| action_pattern_matches(pattern, action))
            && self
                .resources
                .iter()
                .any(|pattern| wildcard_matches(pattern.as_str(), resource))
    }
}

/// Typed principal-free IAM identity policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityPolicy {
    version: Option<PolicyVersion>,
    statements: Vec<IdentityPolicyStatement>,
}

/// S3 resource kind supplied by the S3 authorization adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3IdentityPolicyResource<'a> {
    Bucket(&'a str),
    Object { bucket: &'a str, key: &'a str },
}

/// Service-specific request data for an IAM identity or session policy.
///
/// Trust-policy requests use a separate adapter because their principal and
/// context composition differs from identity permissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityPolicyRequest<'a> {
    S3 {
        action: PolicyAction,
        resource: S3IdentityPolicyResource<'a>,
    },
    StsAssumeRole {
        role: &'a IamRoleIdentity,
    },
}

impl IdentityPolicyRequest<'_> {
    fn action(&self) -> &'static str {
        match self {
            Self::S3 { action, .. } => action.as_str(),
            Self::StsAssumeRole { .. } => "sts:AssumeRole",
        }
    }

    fn resource_arn(&self) -> String {
        match self {
            Self::S3 {
                resource: S3IdentityPolicyResource::Bucket(bucket),
                ..
            } => format!("arn:aws:s3:::{bucket}"),
            Self::S3 {
                resource: S3IdentityPolicyResource::Object { bucket, key },
                ..
            } => format!("arn:aws:s3:::{bucket}/{key}"),
            Self::StsAssumeRole { role } => role.arn().as_str().to_string(),
        }
    }
}

impl IdentityPolicy {
    pub fn new(
        version: Option<PolicyVersion>,
        statements: Vec<IdentityPolicyStatement>,
    ) -> Result<Self, IamPolicyError> {
        (!statements.is_empty())
            .then_some(Self {
                version,
                statements,
            })
            .ok_or(IamPolicyError::EmptyPolicy)
    }

    #[must_use]
    pub const fn version(&self) -> Option<PolicyVersion> {
        self.version
    }

    #[must_use]
    pub fn evaluate(&self, request: &IdentityPolicyRequest<'_>) -> PolicyEvaluation {
        let action = request.action();
        let resource = request.resource_arn();
        let mut saw_allow = false;
        for statement in &self.statements {
            if !statement.matches(action, &resource) {
                continue;
            }
            match statement.core.effect {
                PolicyEffect::Deny => return PolicyEvaluation::ExplicitDeny,
                PolicyEffect::Allow => saw_allow = true,
            }
        }
        if saw_allow {
            PolicyEvaluation::ExplicitAllow
        } else {
            PolicyEvaluation::NoMatch
        }
    }
}

/// Typed inline session policy. Composition treats this document only as an
/// intersection with the role's identity permissions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionPolicy(IdentityPolicy);

impl SessionPolicy {
    #[must_use]
    pub fn new(policy: IdentityPolicy) -> Self {
        Self(policy)
    }

    #[must_use]
    pub fn evaluate(&self, request: &IdentityPolicyRequest<'_>) -> PolicyEvaluation {
        self.0.evaluate(request)
    }
}

/// One named inline identity-policy attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineIdentityPolicy {
    name: InlinePolicyName,
    document: Arc<IdentityPolicy>,
}

impl InlineIdentityPolicy {
    #[must_use]
    pub fn new(name: InlinePolicyName, document: Arc<IdentityPolicy>) -> Self {
        Self { name, document }
    }

    #[must_use]
    pub fn name(&self) -> &InlinePolicyName {
        &self.name
    }

    #[must_use]
    pub fn document(&self) -> &Arc<IdentityPolicy> {
        &self.document
    }
}

/// One initial `sts:AssumeRole` trust-policy statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleTrustPolicyStatement {
    core: PolicyStatementCore,
    principals: Vec<RoleTrustPrincipal>,
}

impl RoleTrustPolicyStatement {
    pub fn new(
        effect: PolicyEffect,
        principals: Vec<RoleTrustPrincipal>,
    ) -> Result<Self, IamPolicyError> {
        (!principals.is_empty())
            .then_some(Self {
                core: PolicyStatementCore::new(
                    None,
                    effect,
                    vec!["sts:AssumeRole".to_string()],
                    Vec::new(),
                ),
                principals,
            })
            .ok_or(IamPolicyError::EmptyPrincipals)
    }

    #[must_use]
    pub fn effect(&self) -> PolicyEffect {
        self.core.effect
    }

    #[must_use]
    pub fn principals(&self) -> &[RoleTrustPrincipal] {
        &self.principals
    }
}

/// Typed role trust policy. Its statement shape has principals but no
/// document `Resource`; the target role supplies the resource at evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleTrustPolicy {
    version: Option<PolicyVersion>,
    statements: Vec<RoleTrustPolicyStatement>,
}

impl RoleTrustPolicy {
    pub fn new(
        version: Option<PolicyVersion>,
        statements: Vec<RoleTrustPolicyStatement>,
    ) -> Result<Self, IamPolicyError> {
        (!statements.is_empty())
            .then_some(Self {
                version,
                statements,
            })
            .ok_or(IamPolicyError::EmptyPolicy)
    }

    #[must_use]
    pub const fn version(&self) -> Option<PolicyVersion> {
        self.version
    }

    #[must_use]
    pub fn statements(&self) -> &[RoleTrustPolicyStatement] {
        &self.statements
    }

    pub(crate) fn evaluate_configured_caller(
        &self,
        caller_account_id: &AwsAccountId,
        caller_principal: &ConfiguredPrincipalIdentity,
        target_role: &IamRoleIdentity,
    ) -> Result<RoleTrustPolicyEvaluation, InvalidRoleTrustCaller> {
        let principal =
            parse_iam_principal_arn(caller_principal.principal()).ok_or(InvalidRoleTrustCaller)?;
        if principal.kind() != IamPrincipalArnKind::User
            || principal.account_id() != caller_account_id
        {
            return Err(InvalidRoleTrustCaller);
        }
        let mut strongest_allow = None;
        for statement in &self.statements {
            let statement_match = statement
                .principals
                .iter()
                .filter_map(|principal| {
                    principal.matches_configured_caller(
                        caller_account_id,
                        caller_principal,
                        target_role,
                    )
                })
                .fold(None, |current, candidate| {
                    strongest_principal_match(current, Some(candidate))
                });
            let Some(statement_match) = statement_match else {
                continue;
            };
            match statement.core.effect {
                PolicyEffect::Deny => return Ok(RoleTrustPolicyEvaluation::ExplicitDeny),
                PolicyEffect::Allow => {
                    strongest_allow =
                        strongest_principal_match(strongest_allow, Some(statement_match));
                }
            }
        }
        Ok(match strongest_allow {
            Some(RoleTrustPrincipalMatch::SameAccountDirect) => {
                RoleTrustPolicyEvaluation::SameAccountDirectAllow
            }
            Some(RoleTrustPrincipalMatch::Delegated) => RoleTrustPolicyEvaluation::DelegatedAllow,
            None => RoleTrustPolicyEvaluation::NoMatch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(value: &str) -> IamActionPattern {
        IamActionPattern::new(value).unwrap()
    }

    fn resource(value: &str) -> IamResourcePattern {
        IamResourcePattern::new(value).unwrap()
    }

    fn s3_object_request<'a>(bucket: &'a str, key: &'a str) -> IdentityPolicyRequest<'a> {
        IdentityPolicyRequest::S3 {
            action: PolicyAction::GetObject,
            resource: S3IdentityPolicyResource::Object { bucket, key },
        }
    }

    #[test]
    fn identity_policy_explicit_deny_wins() {
        let policy = IdentityPolicy::new(
            Some(PolicyVersion::V2012_10_17),
            vec![
                IdentityPolicyStatement::new(
                    PolicyEffect::Allow,
                    vec![pattern("s3:Get*")],
                    vec![resource("arn:aws:s3:::bucket/*")],
                )
                .unwrap(),
                IdentityPolicyStatement::new(
                    PolicyEffect::Deny,
                    vec![pattern("s3:GetObject")],
                    vec![resource("arn:aws:s3:::bucket/private/*")],
                )
                .unwrap(),
            ],
        )
        .unwrap();

        assert_eq!(
            policy.evaluate(&s3_object_request("bucket", "public/key")),
            PolicyEvaluation::ExplicitAllow
        );
        assert_eq!(
            policy.evaluate(&s3_object_request("bucket", "private/key")),
            PolicyEvaluation::ExplicitDeny
        );
        assert_eq!(
            policy.evaluate(&IdentityPolicyRequest::S3 {
                action: PolicyAction::PutObject,
                resource: S3IdentityPolicyResource::Object {
                    bucket: "bucket",
                    key: "public/key",
                },
            }),
            PolicyEvaluation::NoMatch
        );
    }

    #[test]
    fn unsupported_actions_and_empty_statement_shapes_are_unrepresentable() {
        assert_eq!(
            IamActionPattern::new("iam:DeleteRole"),
            Err(IamPolicyError::InvalidActionPattern)
        );
        assert_eq!(
            IdentityPolicyStatement::new(PolicyEffect::Allow, Vec::new(), vec![resource("*")],),
            Err(IamPolicyError::EmptyActions)
        );
        assert_eq!(
            RoleTrustPolicyStatement::new(PolicyEffect::Allow, Vec::new()),
            Err(IamPolicyError::EmptyPrincipals)
        );
        assert_eq!(
            RoleTrustPrincipal::new("arn:aws:iam::123456789012:group/not-valid-in-trust"),
            Err(IamPolicyError::InvalidTrustPrincipal)
        );
        assert!(RoleTrustPrincipal::new("arn:aws:iam::123456789012:role/team/valid-role").is_ok());
        assert!(RoleTrustPrincipal::new("arn:aws:iam::123456789012:role/team//valid-role").is_ok());
        for invalid in [
            "arn:aws:iam::123456789012:role/team/*",
            "arn:aws:iam::123456789012:user/name?",
            "arn:aws:iam::123456789012:role/team/",
            "arn:aws:iam::123456789012:role/team/*/valid-role",
        ] {
            assert_eq!(
                RoleTrustPrincipal::new(invalid),
                Err(IamPolicyError::InvalidTrustPrincipal)
            );
        }
        assert_eq!(
            IamResourcePattern::new("arn:aws:s3:::bucket/line\nbreak"),
            Err(IamPolicyError::InvalidResourcePattern)
        );
    }
}
