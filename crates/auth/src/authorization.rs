// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Authoritative mutable IAM authorization records.
//!
//! These records are deliberately separate from the immutable role-incarnation
//! index used during temporary-credential authentication.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::iam_policy::RoleTrustPolicyEvaluation;
use crate::{
    AwsAccountId, ConfiguredPrincipalIdentity, IdentityPolicyRequest, InlineIdentityPolicy,
    LiveRoleIdentity, PolicyEvaluation, RoleTrustPolicy, SessionPolicy, StableRoleId,
};

const MIN_ROLE_SESSION_DURATION_SECS: u32 = 3_600;
const MAX_ROLE_SESSION_DURATION_SECS: u32 = 43_200;

/// Invalid IAM authorization record or duplicate bootstrap state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuthorizationRecordError {
    #[error("role maximum session duration is outside the supported IAM range")]
    InvalidMaximumSessionDuration,
    #[error("role timestamps are invalid")]
    InvalidRoleTimestamps,
    #[error("duplicate inline policy name")]
    DuplicateInlinePolicyName,
    #[error("the configured principal account identity is inconsistent")]
    AccountMismatch,
    #[error("duplicate stable role ID")]
    DuplicateStableRoleId,
    #[error("duplicate live IAM role ARN")]
    DuplicateRoleArn,
    #[error("duplicate configured principal authorization record")]
    DuplicateConfiguredPrincipal,
}

/// Invalid typed input to an IAM authorization decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuthorizationEvaluationError {
    #[error("the configured caller is not a valid account-bound IAM user principal")]
    InvalidConfiguredCaller,
}

/// Validated maximum duration of sessions issued for a role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoleMaximumSessionDuration(u32);

impl RoleMaximumSessionDuration {
    pub fn new(seconds: u32) -> Result<Self, AuthorizationRecordError> {
        (MIN_ROLE_SESSION_DURATION_SECS..=MAX_ROLE_SESSION_DURATION_SECS)
            .contains(&seconds)
            .then_some(Self(seconds))
            .ok_or(AuthorizationRecordError::InvalidMaximumSessionDuration)
    }

    #[must_use]
    pub const fn seconds(self) -> u32 {
        self.0
    }
}

/// Role creation and last-update instants retained for future IAM responses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoleRecordTimestamps {
    created_at_epoch_secs: i64,
    updated_at_epoch_secs: i64,
}

impl RoleRecordTimestamps {
    pub fn new(
        created_at_epoch_secs: i64,
        updated_at_epoch_secs: i64,
    ) -> Result<Self, AuthorizationRecordError> {
        (created_at_epoch_secs >= 0 && updated_at_epoch_secs >= created_at_epoch_secs)
            .then_some(Self {
                created_at_epoch_secs,
                updated_at_epoch_secs,
            })
            .ok_or(AuthorizationRecordError::InvalidRoleTimestamps)
    }

    #[must_use]
    pub const fn created_at_epoch_secs(self) -> i64 {
        self.created_at_epoch_secs
    }

    #[must_use]
    pub const fn updated_at_epoch_secs(self) -> i64 {
        self.updated_at_epoch_secs
    }
}

fn validate_unique_policy_names(
    policies: &[InlineIdentityPolicy],
) -> Result<(), AuthorizationRecordError> {
    let mut names = HashSet::with_capacity(policies.len());
    if policies
        .iter()
        .all(|policy| names.insert(policy.name().clone()))
    {
        Ok(())
    } else {
        Err(AuthorizationRecordError::DuplicateInlinePolicyName)
    }
}

fn evaluate_inline_identity_policies(
    policies: &[InlineIdentityPolicy],
    request: &IdentityPolicyRequest<'_>,
) -> PolicyEvaluation {
    let mut saw_allow = false;
    for policy in policies {
        match policy.document().evaluate(request) {
            PolicyEvaluation::ExplicitDeny => return PolicyEvaluation::ExplicitDeny,
            PolicyEvaluation::ExplicitAllow => saw_allow = true,
            PolicyEvaluation::NoMatch => {}
        }
    }
    if saw_allow {
        PolicyEvaluation::ExplicitAllow
    } else {
        PolicyEvaluation::NoMatch
    }
}

/// Session-policy input to role-session permission composition.
///
/// Absence means no session policy was supplied and therefore imposes no
/// additional restriction. A present policy intersects with current role
/// permissions and can never add an allow the role does not already grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPolicyRestriction<'a> {
    Absent,
    Policy(&'a SessionPolicy),
}

fn intersect_role_and_session_decisions(
    role: PolicyEvaluation,
    session: PolicyEvaluation,
) -> PolicyEvaluation {
    if role == PolicyEvaluation::ExplicitDeny || session == PolicyEvaluation::ExplicitDeny {
        PolicyEvaluation::ExplicitDeny
    } else if role == PolicyEvaluation::ExplicitAllow && session == PolicyEvaluation::ExplicitAllow
    {
        PolicyEvaluation::ExplicitAllow
    } else {
        PolicyEvaluation::NoMatch
    }
}

fn compose_assume_role_decisions(
    trust: RoleTrustPolicyEvaluation,
    caller_identity: PolicyEvaluation,
) -> PolicyEvaluation {
    if trust == RoleTrustPolicyEvaluation::ExplicitDeny
        || caller_identity == PolicyEvaluation::ExplicitDeny
    {
        return PolicyEvaluation::ExplicitDeny;
    }
    match trust {
        RoleTrustPolicyEvaluation::SameAccountDirectAllow => PolicyEvaluation::ExplicitAllow,
        RoleTrustPolicyEvaluation::DelegatedAllow
            if caller_identity == PolicyEvaluation::ExplicitAllow =>
        {
            PolicyEvaluation::ExplicitAllow
        }
        RoleTrustPolicyEvaluation::ExplicitDeny
        | RoleTrustPolicyEvaluation::DelegatedAllow
        | RoleTrustPolicyEvaluation::NoMatch => PolicyEvaluation::NoMatch,
    }
}

/// Current trust-independent and trust-policy state for one live role.
///
/// The embedded immutable identity binds the mutable record to a role
/// incarnation. Authentication does not read this record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleAuthorizationRecord {
    identity: Arc<LiveRoleIdentity>,
    timestamps: RoleRecordTimestamps,
    maximum_session_duration: RoleMaximumSessionDuration,
    trust_policy: Arc<RoleTrustPolicy>,
    permission_policies: Vec<InlineIdentityPolicy>,
}

impl RoleAuthorizationRecord {
    pub fn new(
        identity: Arc<LiveRoleIdentity>,
        timestamps: RoleRecordTimestamps,
        maximum_session_duration: RoleMaximumSessionDuration,
        trust_policy: Arc<RoleTrustPolicy>,
        permission_policies: Vec<InlineIdentityPolicy>,
    ) -> Result<Self, AuthorizationRecordError> {
        validate_unique_policy_names(&permission_policies)?;
        Ok(Self {
            identity,
            timestamps,
            maximum_session_duration,
            trust_policy,
            permission_policies,
        })
    }

    #[must_use]
    pub fn identity(&self) -> &Arc<LiveRoleIdentity> {
        &self.identity
    }

    #[must_use]
    pub const fn timestamps(&self) -> RoleRecordTimestamps {
        self.timestamps
    }

    #[must_use]
    pub const fn maximum_session_duration(&self) -> RoleMaximumSessionDuration {
        self.maximum_session_duration
    }

    #[must_use]
    pub fn trust_policy(&self) -> &Arc<RoleTrustPolicy> {
        &self.trust_policy
    }

    #[must_use]
    pub fn permission_policies(&self) -> &[InlineIdentityPolicy] {
        &self.permission_policies
    }

    /// Evaluate the union of the role's current identity-policy attachments.
    /// An explicit deny in any attachment wins over allows in every other
    /// attachment.
    #[must_use]
    pub fn evaluate_identity_permissions(
        &self,
        request: &IdentityPolicyRequest<'_>,
    ) -> PolicyEvaluation {
        evaluate_inline_identity_policies(&self.permission_policies, request)
    }

    /// Evaluate a role session's permissions after applying its session-policy
    /// restriction. A session policy is an intersection, never another allow
    /// source.
    #[must_use]
    pub fn evaluate_session_permissions(
        &self,
        request: &IdentityPolicyRequest<'_>,
        session_policy: SessionPolicyRestriction<'_>,
    ) -> PolicyEvaluation {
        let role_decision = self.evaluate_identity_permissions(request);
        match session_policy {
            SessionPolicyRestriction::Absent => role_decision,
            SessionPolicyRestriction::Policy(policy) => {
                intersect_role_and_session_decisions(role_decision, policy.evaluate(request))
            }
        }
    }

    /// Evaluate whether one configured long-lived IAM user may assume this
    /// role. Exact-principal and wildcard same-account trust are direct
    /// resource-policy grants and do not require an identity-policy allow.
    /// Account delegation and every cross-account grant require both sides.
    pub fn evaluate_configured_caller_assume_role(
        &self,
        caller: &ConfiguredPrincipalAuthorizationRecord,
    ) -> Result<PolicyEvaluation, AuthorizationEvaluationError> {
        let trust = self
            .trust_policy
            .evaluate_configured_caller(
                caller.key().account_id(),
                caller.key().principal(),
                self.identity.role(),
            )
            .map_err(|_| AuthorizationEvaluationError::InvalidConfiguredCaller)?;
        let identity =
            caller.evaluate_identity_permissions(&IdentityPolicyRequest::StsAssumeRole {
                role: self.identity.role(),
            });
        Ok(compose_assume_role_decisions(trust, identity))
    }
}

/// Stable key for a configured principal's mutable policy attachments.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfiguredPrincipalAuthorizationKey {
    account_id: AwsAccountId,
    principal: ConfiguredPrincipalIdentity,
}

impl ConfiguredPrincipalAuthorizationKey {
    #[must_use]
    pub fn new(account_id: AwsAccountId, principal: ConfiguredPrincipalIdentity) -> Self {
        Self {
            account_id,
            principal,
        }
    }

    #[must_use]
    pub fn account_id(&self) -> &AwsAccountId {
        &self.account_id
    }

    #[must_use]
    pub fn principal(&self) -> &ConfiguredPrincipalIdentity {
        &self.principal
    }
}

/// Current identity-policy attachments for one configured long-lived
/// principal. Access keys remain separate authentication records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredPrincipalAuthorizationRecord {
    key: ConfiguredPrincipalAuthorizationKey,
    account: s3_types::AccountIdentity,
    identity_policies: Vec<InlineIdentityPolicy>,
}

impl ConfiguredPrincipalAuthorizationRecord {
    pub fn new(
        key: ConfiguredPrincipalAuthorizationKey,
        account: s3_types::AccountIdentity,
        identity_policies: Vec<InlineIdentityPolicy>,
    ) -> Result<Self, AuthorizationRecordError> {
        if account.account_id() != Some(key.account_id().as_str()) {
            return Err(AuthorizationRecordError::AccountMismatch);
        }
        validate_unique_policy_names(&identity_policies)?;
        Ok(Self {
            key,
            account,
            identity_policies,
        })
    }

    #[must_use]
    pub fn key(&self) -> &ConfiguredPrincipalAuthorizationKey {
        &self.key
    }

    #[must_use]
    pub fn account(&self) -> &s3_types::AccountIdentity {
        &self.account
    }

    #[must_use]
    pub fn identity_policies(&self) -> &[InlineIdentityPolicy] {
        &self.identity_policies
    }

    /// Evaluate the union of this configured principal's current identity
    /// policies with explicit-deny precedence.
    #[must_use]
    pub fn evaluate_identity_permissions(
        &self,
        request: &IdentityPolicyRequest<'_>,
    ) -> PolicyEvaluation {
        evaluate_inline_identity_policies(&self.identity_policies, request)
    }
}

/// Bootstrap collection of current mutable IAM authorization records.
pub struct AuthorizationRecordStore {
    roles: HashMap<StableRoleId, Arc<RoleAuthorizationRecord>>,
    role_arns: HashMap<crate::IamRoleArn, StableRoleId>,
    configured_principals:
        HashMap<ConfiguredPrincipalAuthorizationKey, Arc<ConfiguredPrincipalAuthorizationRecord>>,
}

impl AuthorizationRecordStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            roles: HashMap::new(),
            role_arns: HashMap::new(),
            configured_principals: HashMap::new(),
        }
    }

    pub fn add_role(
        &mut self,
        record: RoleAuthorizationRecord,
    ) -> Result<(), AuthorizationRecordError> {
        let stable_id = record.identity().role().stable_id().clone();
        let role_arn = record.identity().role().arn().clone();
        if self.roles.contains_key(&stable_id) {
            return Err(AuthorizationRecordError::DuplicateStableRoleId);
        }
        if self.role_arns.contains_key(&role_arn) {
            return Err(AuthorizationRecordError::DuplicateRoleArn);
        }
        self.role_arns.insert(role_arn, stable_id.clone());
        self.roles.insert(stable_id, Arc::new(record));
        Ok(())
    }

    #[must_use]
    pub fn role_by_arn(
        &self,
        role_arn: &crate::IamRoleArn,
    ) -> Option<Arc<RoleAuthorizationRecord>> {
        let stable_id = self.role_arns.get(role_arn)?;
        self.roles.get(stable_id).cloned()
    }

    pub fn add_configured_principal(
        &mut self,
        record: ConfiguredPrincipalAuthorizationRecord,
    ) -> Result<(), AuthorizationRecordError> {
        if self.configured_principals.contains_key(record.key()) {
            return Err(AuthorizationRecordError::DuplicateConfiguredPrincipal);
        }
        self.configured_principals
            .insert(record.key().clone(), Arc::new(record));
        Ok(())
    }

    pub(crate) fn role(
        &self,
        stable_role_id: &StableRoleId,
    ) -> Option<Arc<RoleAuthorizationRecord>> {
        self.roles.get(stable_role_id).cloned()
    }

    pub(crate) fn configured_principal(
        &self,
        key: &ConfiguredPrincipalAuthorizationKey,
    ) -> Option<Arc<ConfiguredPrincipalAuthorizationRecord>> {
        self.configured_principals.get(key).cloned()
    }
}

impl Default for AuthorizationRecordStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        IamActionPattern, IamPath, IamResourcePattern, IamRoleIdentity, IdentityPolicy,
        IdentityPolicyStatement, InlinePolicyName, PolicyAction, PolicyEffect, PolicyVersion,
        RoleName, RoleTrustPolicyStatement, RoleTrustPrincipal, S3IdentityPolicyResource,
        SessionPolicy,
    };

    fn account() -> s3_types::AccountIdentity {
        account_for("123456789012")
    }

    fn account_for(account_id: &str) -> s3_types::AccountIdentity {
        s3_types::AccountIdentity::new(
            account_id,
            s3_types::CanonicalUserId::from_principal(account_id),
            "test account",
        )
    }

    fn live_role() -> Arc<LiveRoleIdentity> {
        Arc::new(
            LiveRoleIdentity::new(
                account(),
                IamRoleIdentity::new(
                    AwsAccountId::new("123456789012").unwrap(),
                    StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap(),
                    RoleName::new("test-role").unwrap(),
                    IamPath::new("/").unwrap(),
                ),
            )
            .unwrap(),
        )
    }

    fn trust_policy() -> Arc<RoleTrustPolicy> {
        Arc::new(
            RoleTrustPolicy::new(
                Some(crate::PolicyVersion::V2012_10_17),
                vec![RoleTrustPolicyStatement::new(
                    crate::PolicyEffect::Allow,
                    vec![RoleTrustPrincipal::new("arn:aws:iam::123456789012:root").unwrap()],
                )
                .unwrap()],
            )
            .unwrap(),
        )
    }

    fn identity_policy(effect: PolicyEffect, action: &str, resource: &str) -> IdentityPolicy {
        IdentityPolicy::new(
            Some(PolicyVersion::V2012_10_17),
            vec![IdentityPolicyStatement::new(
                effect,
                vec![IamActionPattern::new(action).unwrap()],
                vec![IamResourcePattern::new(resource).unwrap()],
            )
            .unwrap()],
        )
        .unwrap()
    }

    fn permission_policy(
        name: &str,
        effect: PolicyEffect,
        action: &str,
        resource: &str,
    ) -> InlineIdentityPolicy {
        InlineIdentityPolicy::new(
            InlinePolicyName::new(name).unwrap(),
            Arc::new(identity_policy(effect, action, resource)),
        )
    }

    fn get_object_request<'a>(bucket: &'a str, key: &'a str) -> IdentityPolicyRequest<'a> {
        IdentityPolicyRequest::S3 {
            action: PolicyAction::GetObject,
            resource: S3IdentityPolicyResource::Object { bucket, key },
        }
    }

    fn role_record(permission_policies: Vec<InlineIdentityPolicy>) -> RoleAuthorizationRecord {
        RoleAuthorizationRecord::new(
            live_role(),
            RoleRecordTimestamps::new(1, 1).unwrap(),
            RoleMaximumSessionDuration::new(3_600).unwrap(),
            trust_policy(),
            permission_policies,
        )
        .unwrap()
    }

    fn role_record_with_trust(
        principals: &[&str],
        effect: PolicyEffect,
    ) -> RoleAuthorizationRecord {
        role_record_with_trust_statements(&[(effect, principals)])
    }

    fn role_record_with_trust_statements(
        statements: &[(PolicyEffect, &[&str])],
    ) -> RoleAuthorizationRecord {
        RoleAuthorizationRecord::new(
            live_role(),
            RoleRecordTimestamps::new(1, 1).unwrap(),
            RoleMaximumSessionDuration::new(3_600).unwrap(),
            Arc::new(
                RoleTrustPolicy::new(
                    Some(PolicyVersion::V2012_10_17),
                    statements
                        .iter()
                        .map(|(effect, principals)| {
                            RoleTrustPolicyStatement::new(
                                *effect,
                                principals
                                    .iter()
                                    .map(|principal| RoleTrustPrincipal::new(*principal).unwrap())
                                    .collect(),
                            )
                            .unwrap()
                        })
                        .collect(),
                )
                .unwrap(),
            ),
            Vec::new(),
        )
        .unwrap()
    }

    fn configured_caller(
        account_id: &str,
        principal: &str,
        decision: PolicyEvaluation,
        target_role: &IamRoleIdentity,
    ) -> ConfiguredPrincipalAuthorizationRecord {
        let policies = match decision {
            PolicyEvaluation::ExplicitAllow => vec![permission_policy(
                "assume-allow",
                PolicyEffect::Allow,
                "sts:AssumeRole",
                target_role.arn().as_str(),
            )],
            PolicyEvaluation::ExplicitDeny => vec![permission_policy(
                "assume-deny",
                PolicyEffect::Deny,
                "sts:AssumeRole",
                target_role.arn().as_str(),
            )],
            PolicyEvaluation::NoMatch => Vec::new(),
        };
        ConfiguredPrincipalAuthorizationRecord::new(
            ConfiguredPrincipalAuthorizationKey::new(
                AwsAccountId::new(account_id).unwrap(),
                ConfiguredPrincipalIdentity::new(principal),
            ),
            account_for(account_id),
            policies,
        )
        .unwrap()
    }

    #[test]
    fn role_configuration_bounds_and_timestamps_are_validated() {
        assert_eq!(
            RoleMaximumSessionDuration::new(3_599),
            Err(AuthorizationRecordError::InvalidMaximumSessionDuration)
        );
        assert_eq!(
            RoleMaximumSessionDuration::new(3_600).unwrap().seconds(),
            3_600
        );
        assert_eq!(
            RoleMaximumSessionDuration::new(43_200).unwrap().seconds(),
            43_200
        );
        assert_eq!(
            RoleMaximumSessionDuration::new(43_201),
            Err(AuthorizationRecordError::InvalidMaximumSessionDuration)
        );
        assert_eq!(
            RoleRecordTimestamps::new(2, 1),
            Err(AuthorizationRecordError::InvalidRoleTimestamps)
        );
    }

    #[test]
    fn stores_reject_duplicate_authorization_keys() {
        let role = live_role();
        let record = |role: Arc<LiveRoleIdentity>| {
            RoleAuthorizationRecord::new(
                role,
                RoleRecordTimestamps::new(1, 1).unwrap(),
                RoleMaximumSessionDuration::new(3_600).unwrap(),
                trust_policy(),
                Vec::new(),
            )
            .unwrap()
        };
        let mut store = AuthorizationRecordStore::new();
        store.add_role(record(Arc::clone(&role))).unwrap();
        assert_eq!(
            store.add_role(record(Arc::clone(&role))),
            Err(AuthorizationRecordError::DuplicateStableRoleId)
        );
        let duplicate_arn = Arc::new(
            LiveRoleIdentity::new(
                account(),
                IamRoleIdentity::new(
                    AwsAccountId::new("123456789012").unwrap(),
                    StableRoleId::new("ARGRKLMNOPQRST0123456789").unwrap(),
                    RoleName::new("test-role").unwrap(),
                    IamPath::new("/").unwrap(),
                ),
            )
            .unwrap(),
        );
        assert_eq!(
            store.add_role(record(duplicate_arn)),
            Err(AuthorizationRecordError::DuplicateRoleArn)
        );

        let key = ConfiguredPrincipalAuthorizationKey::new(
            AwsAccountId::new("123456789012").unwrap(),
            ConfiguredPrincipalIdentity::new("arn:aws:iam::123456789012:user/test"),
        );
        let principal_record = || {
            ConfiguredPrincipalAuthorizationRecord::new(key.clone(), account(), Vec::new()).unwrap()
        };
        store.add_configured_principal(principal_record()).unwrap();
        assert_eq!(
            store.add_configured_principal(principal_record()),
            Err(AuthorizationRecordError::DuplicateConfiguredPrincipal)
        );
    }

    #[test]
    fn records_reject_duplicate_inline_policy_names() {
        assert_eq!(
            RoleAuthorizationRecord::new(
                live_role(),
                RoleRecordTimestamps::new(1, 1).unwrap(),
                RoleMaximumSessionDuration::new(3_600).unwrap(),
                trust_policy(),
                vec![
                    permission_policy(
                        "duplicate",
                        PolicyEffect::Allow,
                        "s3:GetObject",
                        "arn:aws:s3:::bucket/*",
                    ),
                    permission_policy(
                        "duplicate",
                        PolicyEffect::Allow,
                        "s3:GetObject",
                        "arn:aws:s3:::bucket/*",
                    )
                ],
            ),
            Err(AuthorizationRecordError::DuplicateInlinePolicyName)
        );
    }

    #[test]
    fn attached_identity_policies_form_one_explicit_deny_first_union() {
        let policies = vec![
            permission_policy(
                "allow-reads",
                PolicyEffect::Allow,
                "s3:GetObject",
                "arn:aws:s3:::bucket/*",
            ),
            permission_policy(
                "deny-private",
                PolicyEffect::Deny,
                "s3:GetObject",
                "arn:aws:s3:::bucket/private/*",
            ),
        ];
        let role = role_record(policies.clone());
        let configured = ConfiguredPrincipalAuthorizationRecord::new(
            ConfiguredPrincipalAuthorizationKey::new(
                AwsAccountId::new("123456789012").unwrap(),
                ConfiguredPrincipalIdentity::new("arn:aws:iam::123456789012:user/test"),
            ),
            account(),
            policies,
        )
        .unwrap();

        fn assert_decisions(evaluate: impl Fn(&IdentityPolicyRequest<'_>) -> PolicyEvaluation) {
            assert_eq!(
                evaluate(&get_object_request("bucket", "public/key")),
                PolicyEvaluation::ExplicitAllow
            );
            assert_eq!(
                evaluate(&get_object_request("bucket", "private/key")),
                PolicyEvaluation::ExplicitDeny
            );
            assert_eq!(
                evaluate(&get_object_request("other-bucket", "public/key")),
                PolicyEvaluation::NoMatch
            );
        }
        assert_decisions(|request| role.evaluate_identity_permissions(request));
        assert_decisions(|request| configured.evaluate_identity_permissions(request));
    }

    #[test]
    fn session_policy_is_an_intersection_with_role_permissions() {
        let role_allow = role_record(vec![permission_policy(
            "allow-reads",
            PolicyEffect::Allow,
            "s3:GetObject",
            "arn:aws:s3:::bucket/*",
        )]);
        let role_deny = role_record(vec![permission_policy(
            "deny-reads",
            PolicyEffect::Deny,
            "s3:GetObject",
            "arn:aws:s3:::bucket/*",
        )]);
        let session_allow = SessionPolicy::new(identity_policy(
            PolicyEffect::Allow,
            "s3:GetObject",
            "arn:aws:s3:::bucket/public/*",
        ));
        let session_deny = SessionPolicy::new(identity_policy(
            PolicyEffect::Deny,
            "s3:GetObject",
            "arn:aws:s3:::bucket/public/*",
        ));
        let session_put_allow = SessionPolicy::new(identity_policy(
            PolicyEffect::Allow,
            "s3:PutObject",
            "arn:aws:s3:::bucket/*",
        ));
        let get = get_object_request("bucket", "public/key");
        let put = IdentityPolicyRequest::S3 {
            action: PolicyAction::PutObject,
            resource: S3IdentityPolicyResource::Object {
                bucket: "bucket",
                key: "public/key",
            },
        };

        assert_eq!(
            role_allow.evaluate_session_permissions(&get, SessionPolicyRestriction::Absent),
            PolicyEvaluation::ExplicitAllow
        );
        assert_eq!(
            role_allow.evaluate_session_permissions(&put, SessionPolicyRestriction::Absent),
            PolicyEvaluation::NoMatch
        );
        assert_eq!(
            role_deny.evaluate_session_permissions(&get, SessionPolicyRestriction::Absent),
            PolicyEvaluation::ExplicitDeny
        );
        assert_eq!(
            role_allow.evaluate_session_permissions(
                &get,
                SessionPolicyRestriction::Policy(&session_allow),
            ),
            PolicyEvaluation::ExplicitAllow
        );
        assert_eq!(
            role_allow.evaluate_session_permissions(
                &get,
                SessionPolicyRestriction::Policy(&session_put_allow),
            ),
            PolicyEvaluation::NoMatch
        );
        assert_eq!(
            role_allow.evaluate_session_permissions(
                &get,
                SessionPolicyRestriction::Policy(&session_deny),
            ),
            PolicyEvaluation::ExplicitDeny
        );
        assert_eq!(
            role_allow.evaluate_session_permissions(
                &put,
                SessionPolicyRestriction::Policy(&session_put_allow),
            ),
            PolicyEvaluation::NoMatch,
            "a session policy cannot add an allow missing from the role"
        );
        assert_eq!(
            role_deny.evaluate_session_permissions(
                &get,
                SessionPolicyRestriction::Policy(&session_allow),
            ),
            PolicyEvaluation::ExplicitDeny
        );
    }

    #[test]
    fn role_and_session_decision_intersection_has_complete_deny_first_matrix() {
        use PolicyEvaluation::{ExplicitAllow, ExplicitDeny, NoMatch};

        for (role, session, expected) in [
            (ExplicitDeny, ExplicitDeny, ExplicitDeny),
            (ExplicitDeny, ExplicitAllow, ExplicitDeny),
            (ExplicitDeny, NoMatch, ExplicitDeny),
            (ExplicitAllow, ExplicitDeny, ExplicitDeny),
            (ExplicitAllow, ExplicitAllow, ExplicitAllow),
            (ExplicitAllow, NoMatch, NoMatch),
            (NoMatch, ExplicitDeny, ExplicitDeny),
            (NoMatch, ExplicitAllow, NoMatch),
            (NoMatch, NoMatch, NoMatch),
        ] {
            assert_eq!(
                intersect_role_and_session_decisions(role, session),
                expected,
                "unexpected role={role:?}, session={session:?} composition"
            );
        }
    }

    #[test]
    fn assume_role_decision_composition_has_complete_deny_first_matrix() {
        use PolicyEvaluation::{ExplicitAllow, ExplicitDeny, NoMatch};
        use RoleTrustPolicyEvaluation::{
            DelegatedAllow, ExplicitDeny as TrustDeny, NoMatch as TrustNoMatch,
            SameAccountDirectAllow,
        };

        for (trust, identity, expected) in [
            (TrustDeny, ExplicitDeny, ExplicitDeny),
            (TrustDeny, ExplicitAllow, ExplicitDeny),
            (TrustDeny, NoMatch, ExplicitDeny),
            (SameAccountDirectAllow, ExplicitDeny, ExplicitDeny),
            (SameAccountDirectAllow, ExplicitAllow, ExplicitAllow),
            (SameAccountDirectAllow, NoMatch, ExplicitAllow),
            (DelegatedAllow, ExplicitDeny, ExplicitDeny),
            (DelegatedAllow, ExplicitAllow, ExplicitAllow),
            (DelegatedAllow, NoMatch, NoMatch),
            (TrustNoMatch, ExplicitDeny, ExplicitDeny),
            (TrustNoMatch, ExplicitAllow, NoMatch),
            (TrustNoMatch, NoMatch, NoMatch),
        ] {
            assert_eq!(
                compose_assume_role_decisions(trust, identity),
                expected,
                "unexpected trust={trust:?}, identity={identity:?} composition"
            );
        }
    }

    #[test]
    fn assume_role_distinguishes_direct_same_account_and_delegated_trust() {
        let same_account_principal = "arn:aws:iam::123456789012:user/team/test";
        let cross_account_principal = "arn:aws:iam::210987654321:user/team/test";

        let direct = role_record_with_trust(&[same_account_principal], PolicyEffect::Allow);
        let same_no_policy = configured_caller(
            "123456789012",
            same_account_principal,
            PolicyEvaluation::NoMatch,
            direct.identity().role(),
        );
        assert_eq!(
            direct
                .evaluate_configured_caller_assume_role(&same_no_policy)
                .unwrap(),
            PolicyEvaluation::ExplicitAllow,
            "AWS permits an exactly trusted same-account principal directly"
        );

        for delegated_principal in ["123456789012", "arn:aws:iam::123456789012:root"] {
            let delegated = role_record_with_trust(&[delegated_principal], PolicyEffect::Allow);
            let no_policy = configured_caller(
                "123456789012",
                same_account_principal,
                PolicyEvaluation::NoMatch,
                delegated.identity().role(),
            );
            let allowed = configured_caller(
                "123456789012",
                same_account_principal,
                PolicyEvaluation::ExplicitAllow,
                delegated.identity().role(),
            );
            assert_eq!(
                delegated
                    .evaluate_configured_caller_assume_role(&no_policy)
                    .unwrap(),
                PolicyEvaluation::NoMatch,
                "account trust delegates instead of granting directly"
            );
            assert_eq!(
                delegated
                    .evaluate_configured_caller_assume_role(&allowed)
                    .unwrap(),
                PolicyEvaluation::ExplicitAllow
            );
        }

        let same_account_wildcard = role_record_with_trust(&["*"], PolicyEffect::Allow);
        assert_eq!(
            same_account_wildcard
                .evaluate_configured_caller_assume_role(&same_no_policy)
                .unwrap(),
            PolicyEvaluation::ExplicitAllow,
            "AWS permits wildcard trust directly for a same-account IAM user"
        );

        let cross = role_record_with_trust(&[cross_account_principal], PolicyEffect::Allow);
        let cross_no_policy = configured_caller(
            "210987654321",
            cross_account_principal,
            PolicyEvaluation::NoMatch,
            cross.identity().role(),
        );
        let cross_allowed = configured_caller(
            "210987654321",
            cross_account_principal,
            PolicyEvaluation::ExplicitAllow,
            cross.identity().role(),
        );
        assert_eq!(
            cross
                .evaluate_configured_caller_assume_role(&cross_no_policy)
                .unwrap(),
            PolicyEvaluation::NoMatch
        );
        assert_eq!(
            cross
                .evaluate_configured_caller_assume_role(&cross_allowed)
                .unwrap(),
            PolicyEvaluation::ExplicitAllow
        );

        let cross_wildcard = role_record_with_trust(&["*"], PolicyEffect::Allow);
        assert_eq!(
            cross_wildcard
                .evaluate_configured_caller_assume_role(&cross_no_policy)
                .unwrap(),
            PolicyEvaluation::NoMatch,
            "cross-account wildcard trust still requires caller permission"
        );
        assert_eq!(
            cross_wildcard
                .evaluate_configured_caller_assume_role(&cross_allowed)
                .unwrap(),
            PolicyEvaluation::ExplicitAllow
        );
    }

    #[test]
    fn assume_role_denies_when_either_policy_side_denies_or_trust_omits_caller() {
        let principal = "arn:aws:iam::123456789012:user/test";
        let direct = role_record_with_trust(&[principal], PolicyEffect::Allow);
        let identity_deny = configured_caller(
            "123456789012",
            principal,
            PolicyEvaluation::ExplicitDeny,
            direct.identity().role(),
        );
        assert_eq!(
            direct
                .evaluate_configured_caller_assume_role(&identity_deny)
                .unwrap(),
            PolicyEvaluation::ExplicitDeny
        );

        let trust_deny = role_record_with_trust(&[principal], PolicyEffect::Deny);
        let identity_allow = configured_caller(
            "123456789012",
            principal,
            PolicyEvaluation::ExplicitAllow,
            trust_deny.identity().role(),
        );
        assert_eq!(
            trust_deny
                .evaluate_configured_caller_assume_role(&identity_allow)
                .unwrap(),
            PolicyEvaluation::ExplicitDeny
        );

        let omitted = role_record_with_trust(
            &["arn:aws:iam::123456789012:user/someone-else"],
            PolicyEffect::Allow,
        );
        let identity_allow = configured_caller(
            "123456789012",
            principal,
            PolicyEvaluation::ExplicitAllow,
            omitted.identity().role(),
        );
        assert_eq!(
            omitted
                .evaluate_configured_caller_assume_role(&identity_allow)
                .unwrap(),
            PolicyEvaluation::NoMatch
        );
    }

    #[test]
    fn assume_role_trust_document_combines_statements_deny_first() {
        let principal = "arn:aws:iam::123456789012:user/test";
        let no_identity_policy = |role: &RoleAuthorizationRecord| {
            configured_caller(
                "123456789012",
                principal,
                PolicyEvaluation::NoMatch,
                role.identity().role(),
            )
        };

        let direct_and_delegated = role_record_with_trust_statements(&[
            (PolicyEffect::Allow, &["arn:aws:iam::123456789012:root"]),
            (PolicyEffect::Allow, &[principal]),
        ]);
        assert_eq!(
            direct_and_delegated
                .evaluate_configured_caller_assume_role(&no_identity_policy(&direct_and_delegated,))
                .unwrap(),
            PolicyEvaluation::ExplicitAllow,
            "an exact direct grant is not weakened by a separate delegated grant"
        );

        let allow_then_deny = role_record_with_trust_statements(&[
            (PolicyEffect::Allow, &[principal]),
            (PolicyEffect::Deny, &["arn:aws:iam::123456789012:root"]),
        ]);
        assert_eq!(
            allow_then_deny
                .evaluate_configured_caller_assume_role(&no_identity_policy(&allow_then_deny))
                .unwrap(),
            PolicyEvaluation::ExplicitDeny
        );
    }

    #[test]
    fn assume_role_rejects_untyped_or_account_mismatched_configured_callers() {
        let role = role_record_with_trust(&["*"], PolicyEffect::Allow);
        for principal in [
            "legacy-principal",
            "arn:aws:iam::210987654321:user/wrong-account",
            "arn:aws:iam::123456789012:group/not-a-caller",
            "arn:aws:iam::123456789012:role/not-a-long-lived-caller",
            "arn:aws:iam::123456789012:root",
        ] {
            let caller = configured_caller(
                "123456789012",
                principal,
                PolicyEvaluation::ExplicitAllow,
                role.identity().role(),
            );
            assert_eq!(
                role.evaluate_configured_caller_assume_role(&caller),
                Err(AuthorizationEvaluationError::InvalidConfiguredCaller)
            );
        }
    }
}
