// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use s3_types::{aws_account_id_from_principal, CanonicalUserId};
use serde_json::Value;
use std::net::{IpAddr, Ipv4Addr};

#[cfg(test)]
use crate::policy::wildcard_matches;
use crate::policy::{
    action_pattern_matches, policy_value_wildcard_matches, PolicyStatementCore, PolicyValue,
};
pub use crate::policy::{PolicyConditionClause, PolicyEffect, PolicyEvaluation, PolicyVersion};
use crate::{AuthenticatedIdentity, ConfiguredPrincipalIdentity, PrincipalIdentity};

mod condition_key;
mod condition_op;

pub const MAX_BUCKET_POLICY_BYTES: usize = 20 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketPolicy {
    version: Option<PolicyVersion>,
    statements: Vec<PolicyStatement>,
}

impl BucketPolicy {
    #[must_use]
    pub fn version(&self) -> Option<PolicyVersion> {
        self.version
    }

    #[must_use]
    pub fn statements(&self) -> &[PolicyStatement] {
        &self.statements
    }

    #[must_use]
    pub fn is_public(&self) -> bool {
        self.statements
            .iter()
            .any(PolicyStatement::allows_public_access)
    }

    #[must_use]
    pub fn requires_existing_object_tags_for_action(&self, action: PolicyAction) -> bool {
        self.requires_condition_input_for_action(
            action,
            condition_key::ConditionInput::ExistingObject,
        )
    }

    #[must_use]
    pub fn requires_request_object_tags_for_action(&self, action: PolicyAction) -> bool {
        self.requires_condition_input_for_action(action, condition_key::ConditionInput::Request)
    }

    #[must_use]
    pub fn requires_bucket_tags_for_action(&self, action: PolicyAction) -> bool {
        self.requires_condition_input_for_action(action, condition_key::ConditionInput::Bucket)
    }

    #[must_use]
    pub fn requires_source_ip_for_action(&self, action: PolicyAction) -> bool {
        self.requires_condition_input_for_action(action, condition_key::ConditionInput::SourceIp)
    }

    #[must_use]
    pub fn requires_current_time_for_action(&self, action: PolicyAction) -> bool {
        self.requires_condition_input_for_action(action, condition_key::ConditionInput::CurrentTime)
    }

    #[must_use]
    pub fn requires_secure_transport_for_action(&self, action: PolicyAction) -> bool {
        self.requires_condition_input_for_action(
            action,
            condition_key::ConditionInput::SecureTransport,
        )
    }

    #[must_use]
    pub fn requires_requested_region_for_action(&self, action: PolicyAction) -> bool {
        self.requires_condition_input_for_action(
            action,
            condition_key::ConditionInput::RequestedRegion,
        )
    }

    #[must_use]
    pub fn requires_referer_for_action(&self, action: PolicyAction) -> bool {
        self.requires_condition_input_for_action(action, condition_key::ConditionInput::Referer)
    }

    #[must_use]
    pub fn requires_auth_type_for_action(&self, action: PolicyAction) -> bool {
        self.requires_condition_input_for_action(action, condition_key::ConditionInput::AuthType)
    }

    #[must_use]
    pub fn requires_signature_version_for_action(&self, action: PolicyAction) -> bool {
        self.requires_condition_input_for_action(
            action,
            condition_key::ConditionInput::SignatureVersion,
        )
    }

    #[must_use]
    pub fn requires_signature_age_for_action(&self, action: PolicyAction) -> bool {
        self.requires_condition_input_for_action(
            action,
            condition_key::ConditionInput::SignatureAge,
        )
    }

    #[must_use]
    pub fn requires_tls_version_for_action(&self, action: PolicyAction) -> bool {
        self.requires_condition_input_for_action(action, condition_key::ConditionInput::TlsVersion)
    }

    #[must_use]
    pub fn requires_content_sha256_for_action(&self, action: PolicyAction) -> bool {
        self.requires_condition_input_for_action(
            action,
            condition_key::ConditionInput::ContentSha256,
        )
    }

    fn requires_condition_input_for_action(
        &self,
        action: PolicyAction,
        input: condition_key::ConditionInput,
    ) -> bool {
        let action_str = action.as_str();
        self.statements.iter().any(|statement| {
            statement.matches_action(action_str)
                && statement.references_condition_input_for_action(action, input)
        })
    }

    #[must_use]
    pub fn references_bucket_tag_conditions(&self) -> bool {
        self.statements
            .iter()
            .any(PolicyStatement::references_bucket_tag_condition)
    }

    pub fn validate_evaluable_object_conditions(&self) -> Result<(), BucketPolicyError> {
        for statement in &self.statements {
            if let Some(err) = statement.condition_validation_error_for_policy_actions() {
                return Err(err);
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn evaluate(&self, request: &PolicyRequest<'_>) -> PolicyEvaluation {
        let action = request.action.as_str();
        let resource = request.resource_arn();
        let mut saw_allow = false;
        let variables_enabled = self.version == Some(PolicyVersion::V2012_10_17);

        for statement in &self.statements {
            let Some(effect) =
                statement.request_effect(request, action, &resource, variables_enabled)
            else {
                continue;
            };

            match effect {
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

    #[must_use]
    pub fn normalized_json(&self) -> String {
        let mut out = String::from("{");
        if let Some(version) = self.version {
            out.push_str("\"Version\":");
            push_json_string(&mut out, version.as_str());
            out.push(',');
        }
        out.push_str("\"Statement\":[");
        for (index, statement) in self.statements.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            statement.push_normalized_json(&mut out);
        }
        out.push_str("]}");
        out
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyAction {
    DeleteBucket,
    GetBucketPolicy,
    PutBucketPolicy,
    DeleteBucketPolicy,
    GetBucketLocation,
    GetBucketCors,
    GetBucketAcl,
    GetBucketVersioning,
    GetBucketOwnershipControls,
    GetBucketTagging,
    GetEncryptionConfiguration,
    GetLifecycleConfiguration,
    GetBucketPolicyStatus,
    GetBucketPublicAccessBlock,
    GetBucketObjectLockConfiguration,
    ListBucket,
    ListBucketVersions,
    ListBucketMultipartUploads,
    PutBucketAcl,
    PutBucketCors,
    PutBucketVersioning,
    PutBucketOwnershipControls,
    PutBucketTagging,
    PutEncryptionConfiguration,
    PutLifecycleConfiguration,
    PutBucketPublicAccessBlock,
    PutBucketObjectLockConfiguration,
    ListTagsForResource,
    TagResource,
    UntagResource,
    GetObject,
    GetObjectVersion,
    GetObjectAttributes,
    GetObjectVersionAttributes,
    GetObjectAcl,
    GetObjectVersionAcl,
    GetObjectTagging,
    GetObjectVersionTagging,
    GetObjectRetention,
    GetObjectLegalHold,
    PutObject,
    PutObjectAcl,
    PutObjectVersionAcl,
    PutObjectTagging,
    PutObjectVersionTagging,
    PutObjectRetention,
    PutObjectLegalHold,
    BypassGovernanceRetention,
    AbortMultipartUpload,
    DeleteObject,
    DeleteObjectVersion,
    DeleteObjectTagging,
    DeleteObjectVersionTagging,
}

impl PolicyAction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DeleteBucket => "s3:DeleteBucket",
            Self::GetBucketPolicy => "s3:GetBucketPolicy",
            Self::PutBucketPolicy => "s3:PutBucketPolicy",
            Self::DeleteBucketPolicy => "s3:DeleteBucketPolicy",
            Self::GetBucketLocation => "s3:GetBucketLocation",
            Self::GetBucketCors => "s3:GetBucketCORS",
            Self::GetBucketAcl => "s3:GetBucketAcl",
            Self::GetBucketVersioning => "s3:GetBucketVersioning",
            Self::GetBucketOwnershipControls => "s3:GetBucketOwnershipControls",
            Self::GetBucketTagging => "s3:GetBucketTagging",
            Self::GetEncryptionConfiguration => "s3:GetEncryptionConfiguration",
            Self::GetLifecycleConfiguration => "s3:GetLifecycleConfiguration",
            Self::GetBucketPolicyStatus => "s3:GetBucketPolicyStatus",
            Self::GetBucketPublicAccessBlock => "s3:GetBucketPublicAccessBlock",
            Self::GetBucketObjectLockConfiguration => "s3:GetBucketObjectLockConfiguration",
            Self::ListBucket => "s3:ListBucket",
            Self::ListBucketVersions => "s3:ListBucketVersions",
            Self::ListBucketMultipartUploads => "s3:ListBucketMultipartUploads",
            Self::PutBucketAcl => "s3:PutBucketAcl",
            Self::PutBucketCors => "s3:PutBucketCORS",
            Self::PutBucketVersioning => "s3:PutBucketVersioning",
            Self::PutBucketOwnershipControls => "s3:PutBucketOwnershipControls",
            Self::PutBucketTagging => "s3:PutBucketTagging",
            Self::PutEncryptionConfiguration => "s3:PutEncryptionConfiguration",
            Self::PutLifecycleConfiguration => "s3:PutLifecycleConfiguration",
            Self::PutBucketPublicAccessBlock => "s3:PutBucketPublicAccessBlock",
            Self::PutBucketObjectLockConfiguration => "s3:PutBucketObjectLockConfiguration",
            Self::ListTagsForResource => "s3:ListTagsForResource",
            Self::TagResource => "s3:TagResource",
            Self::UntagResource => "s3:UntagResource",
            Self::GetObject => "s3:GetObject",
            Self::GetObjectVersion => "s3:GetObjectVersion",
            Self::GetObjectAttributes => "s3:GetObjectAttributes",
            Self::GetObjectVersionAttributes => "s3:GetObjectVersionAttributes",
            Self::GetObjectAcl => "s3:GetObjectAcl",
            Self::GetObjectVersionAcl => "s3:GetObjectVersionAcl",
            Self::GetObjectTagging => "s3:GetObjectTagging",
            Self::GetObjectVersionTagging => "s3:GetObjectVersionTagging",
            Self::GetObjectRetention => "s3:GetObjectRetention",
            Self::GetObjectLegalHold => "s3:GetObjectLegalHold",
            Self::PutObject => "s3:PutObject",
            Self::PutObjectAcl => "s3:PutObjectAcl",
            Self::PutObjectVersionAcl => "s3:PutObjectVersionAcl",
            Self::PutObjectTagging => "s3:PutObjectTagging",
            Self::PutObjectVersionTagging => "s3:PutObjectVersionTagging",
            Self::PutObjectRetention => "s3:PutObjectRetention",
            Self::PutObjectLegalHold => "s3:PutObjectLegalHold",
            Self::BypassGovernanceRetention => "s3:BypassGovernanceRetention",
            Self::AbortMultipartUpload => "s3:AbortMultipartUpload",
            Self::DeleteObject => "s3:DeleteObject",
            Self::DeleteObjectVersion => "s3:DeleteObjectVersion",
            Self::DeleteObjectTagging => "s3:DeleteObjectTagging",
            Self::DeleteObjectVersionTagging => "s3:DeleteObjectVersionTagging",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyTag<'a> {
    key: &'a str,
    value: &'a str,
}

impl<'a> PolicyTag<'a> {
    #[must_use]
    pub const fn new(key: &'a str, value: &'a str) -> Self {
        Self { key, value }
    }

    #[must_use]
    pub const fn key(self) -> &'a str {
        self.key
    }

    #[must_use]
    pub const fn value(self) -> &'a str {
        self.value
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistingObjectTags<'a> {
    Unavailable,
    Available(&'a [PolicyTag<'a>]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketTags<'a> {
    Unavailable,
    Available(&'a [PolicyTag<'a>]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestObjectTags<'a> {
    Unavailable,
    Available(&'a [PolicyTag<'a>]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestField<'a> {
    Unavailable,
    Available(Option<&'a str>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestBool {
    Unavailable,
    Available(Option<bool>),
}

/// Authenticated principal facts supplied to an S3 resource-policy request.
///
/// Constructing this from [`AuthenticatedIdentity`] keeps every assumed-role
/// value bound to one authenticated session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyRequester<'a>(PolicyRequesterKind<'a>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PolicyRequesterKind<'a> {
    Anonymous,
    Configured {
        principal: &'a ConfiguredPrincipalIdentity,
        canonical_user_id: &'a CanonicalUserId,
        principal_arn: Option<&'a crate::IamUserArn>,
    },
    AssumedRoleSession(&'a crate::AssumedRoleSessionIdentity),
}

impl<'a> PolicyRequester<'a> {
    #[must_use]
    pub const fn anonymous() -> Self {
        Self(PolicyRequesterKind::Anonymous)
    }

    #[must_use]
    pub fn authenticated(identity: &'a AuthenticatedIdentity) -> Self {
        match identity.kind() {
            PrincipalIdentity::Configured { principal, .. } => {
                Self(PolicyRequesterKind::Configured {
                    principal,
                    canonical_user_id: identity.account().canonical_user_id(),
                    principal_arn: identity.configured_iam_user_arn(),
                })
            }
            PrincipalIdentity::AssumedRoleSession(session) => {
                Self(PolicyRequesterKind::AssumedRoleSession(session))
            }
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LegacyPolicyRequester<'a> {
    principal: Option<&'a str>,
    canonical_user_id: Option<&'a CanonicalUserId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PolicyRequesterContext<'a> {
    #[cfg(test)]
    Legacy(LegacyPolicyRequester<'a>),
    Typed(PolicyRequester<'a>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyRequest<'a> {
    action: PolicyAction,
    bucket: &'a str,
    key: &'a str,
    bucket_resource: bool,
    requester: PolicyRequesterContext<'a>,
    bucket_tags: BucketTags<'a>,
    existing_object_tags: ExistingObjectTags<'a>,
    request_object_tags: RequestObjectTags<'a>,
    copy_source: RequestField<'a>,
    metadata_directive: RequestField<'a>,
    canned_acl: RequestField<'a>,
    server_side_encryption: RequestField<'a>,
    sse_customer_algorithm: RequestField<'a>,
    grant_read: RequestField<'a>,
    grant_write: RequestField<'a>,
    grant_read_acp: RequestField<'a>,
    grant_write_acp: RequestField<'a>,
    grant_full_control: RequestField<'a>,
    if_match: RequestField<'a>,
    if_none_match: RequestField<'a>,
    object_creation_operation: RequestBool,
    prefix: RequestField<'a>,
    delimiter: RequestField<'a>,
    max_keys: RequestField<'a>,
    object_ownership: RequestField<'a>,
    version_id: RequestField<'a>,
    source_ip: Option<IpAddr>,
    current_time_epoch_seconds: Option<u64>,
    secure_transport: RequestBool,
    requested_region: RequestField<'a>,
    referer: RequestField<'a>,
    auth_type: RequestField<'a>,
    signature_version: RequestField<'a>,
    signature_age_millis: Option<Option<u64>>,
    tls_version: RequestField<'a>,
    content_sha256: RequestField<'a>,
    website_redirect_location: RequestField<'a>,
}

impl<'a> PolicyRequest<'a> {
    #[must_use]
    pub const fn for_object_with_requester(
        action: PolicyAction,
        bucket: &'a str,
        key: &'a str,
        requester: PolicyRequester<'a>,
        existing_object_tags: ExistingObjectTags<'a>,
    ) -> Self {
        Self::for_object_context(
            action,
            bucket,
            key,
            PolicyRequesterContext::Typed(requester),
            existing_object_tags,
        )
    }

    #[cfg(test)]
    #[must_use]
    const fn for_object(
        action: PolicyAction,
        bucket: &'a str,
        key: &'a str,
        requester_principal: Option<&'a str>,
        requester_canonical_user_id: Option<&'a CanonicalUserId>,
        existing_object_tags: ExistingObjectTags<'a>,
    ) -> Self {
        Self::for_object_context(
            action,
            bucket,
            key,
            PolicyRequesterContext::Legacy(LegacyPolicyRequester {
                principal: requester_principal,
                canonical_user_id: requester_canonical_user_id,
            }),
            existing_object_tags,
        )
    }

    const fn for_object_context(
        action: PolicyAction,
        bucket: &'a str,
        key: &'a str,
        requester: PolicyRequesterContext<'a>,
        existing_object_tags: ExistingObjectTags<'a>,
    ) -> Self {
        Self {
            action,
            bucket,
            key,
            bucket_resource: false,
            requester,
            bucket_tags: BucketTags::Unavailable,
            existing_object_tags,
            request_object_tags: RequestObjectTags::Unavailable,
            copy_source: RequestField::Unavailable,
            metadata_directive: RequestField::Unavailable,
            canned_acl: RequestField::Unavailable,
            server_side_encryption: RequestField::Unavailable,
            sse_customer_algorithm: RequestField::Unavailable,
            grant_read: RequestField::Unavailable,
            grant_write: RequestField::Unavailable,
            grant_read_acp: RequestField::Unavailable,
            grant_write_acp: RequestField::Unavailable,
            grant_full_control: RequestField::Unavailable,
            if_match: RequestField::Unavailable,
            if_none_match: RequestField::Unavailable,
            object_creation_operation: RequestBool::Unavailable,
            prefix: RequestField::Unavailable,
            delimiter: RequestField::Unavailable,
            max_keys: RequestField::Unavailable,
            object_ownership: RequestField::Unavailable,
            version_id: RequestField::Unavailable,
            source_ip: None,
            current_time_epoch_seconds: None,
            secure_transport: RequestBool::Unavailable,
            requested_region: RequestField::Unavailable,
            referer: RequestField::Unavailable,
            auth_type: RequestField::Unavailable,
            signature_version: RequestField::Unavailable,
            signature_age_millis: None,
            tls_version: RequestField::Unavailable,
            content_sha256: RequestField::Unavailable,
            website_redirect_location: RequestField::Unavailable,
        }
    }

    #[must_use]
    pub const fn for_bucket_with_requester(
        action: PolicyAction,
        bucket: &'a str,
        requester: PolicyRequester<'a>,
        bucket_tags: BucketTags<'a>,
    ) -> Self {
        Self::for_bucket_context(
            action,
            bucket,
            PolicyRequesterContext::Typed(requester),
            bucket_tags,
        )
    }

    #[cfg(test)]
    #[must_use]
    const fn for_bucket(
        action: PolicyAction,
        bucket: &'a str,
        requester_principal: Option<&'a str>,
        requester_canonical_user_id: Option<&'a CanonicalUserId>,
        bucket_tags: BucketTags<'a>,
    ) -> Self {
        Self::for_bucket_context(
            action,
            bucket,
            PolicyRequesterContext::Legacy(LegacyPolicyRequester {
                principal: requester_principal,
                canonical_user_id: requester_canonical_user_id,
            }),
            bucket_tags,
        )
    }

    const fn for_bucket_context(
        action: PolicyAction,
        bucket: &'a str,
        requester: PolicyRequesterContext<'a>,
        bucket_tags: BucketTags<'a>,
    ) -> Self {
        Self {
            action,
            bucket,
            key: "",
            bucket_resource: true,
            requester,
            bucket_tags,
            existing_object_tags: ExistingObjectTags::Unavailable,
            request_object_tags: RequestObjectTags::Unavailable,
            copy_source: RequestField::Unavailable,
            metadata_directive: RequestField::Unavailable,
            canned_acl: RequestField::Unavailable,
            server_side_encryption: RequestField::Unavailable,
            sse_customer_algorithm: RequestField::Unavailable,
            grant_read: RequestField::Unavailable,
            grant_write: RequestField::Unavailable,
            grant_read_acp: RequestField::Unavailable,
            grant_write_acp: RequestField::Unavailable,
            grant_full_control: RequestField::Unavailable,
            if_match: RequestField::Unavailable,
            if_none_match: RequestField::Unavailable,
            object_creation_operation: RequestBool::Unavailable,
            prefix: RequestField::Unavailable,
            delimiter: RequestField::Unavailable,
            max_keys: RequestField::Unavailable,
            object_ownership: RequestField::Unavailable,
            version_id: RequestField::Unavailable,
            source_ip: None,
            current_time_epoch_seconds: None,
            secure_transport: RequestBool::Unavailable,
            requested_region: RequestField::Unavailable,
            referer: RequestField::Unavailable,
            auth_type: RequestField::Unavailable,
            signature_version: RequestField::Unavailable,
            signature_age_millis: None,
            tls_version: RequestField::Unavailable,
            content_sha256: RequestField::Unavailable,
            website_redirect_location: RequestField::Unavailable,
        }
    }

    #[must_use]
    pub const fn action(&self) -> PolicyAction {
        self.action
    }

    #[must_use]
    pub fn requester_principal(&self) -> Option<&'a str> {
        match self.requester {
            #[cfg(test)]
            PolicyRequesterContext::Legacy(requester) => requester.principal,
            PolicyRequesterContext::Typed(PolicyRequester(PolicyRequesterKind::Anonymous)) => None,
            PolicyRequesterContext::Typed(PolicyRequester(PolicyRequesterKind::Configured {
                principal,
                ..
            })) => Some(principal.principal()),
            PolicyRequesterContext::Typed(PolicyRequester(
                PolicyRequesterKind::AssumedRoleSession(session),
            )) => Some(session.session_arn().as_str()),
        }
    }

    #[must_use]
    pub const fn requester_canonical_user_id(&self) -> Option<&'a CanonicalUserId> {
        match self.requester {
            #[cfg(test)]
            PolicyRequesterContext::Legacy(requester) => requester.canonical_user_id,
            PolicyRequesterContext::Typed(PolicyRequester(PolicyRequesterKind::Configured {
                canonical_user_id,
                ..
            })) => Some(canonical_user_id),
            PolicyRequesterContext::Typed(PolicyRequester(
                PolicyRequesterKind::Anonymous | PolicyRequesterKind::AssumedRoleSession(_),
            )) => None,
        }
    }

    fn principal_matches(&self, policy_value: &str) -> bool {
        match self.requester {
            #[cfg(test)]
            PolicyRequesterContext::Legacy(requester) => requester
                .principal
                .is_some_and(|principal| aws_principal_matches_request(principal, policy_value)),
            PolicyRequesterContext::Typed(PolicyRequester(PolicyRequesterKind::Anonymous)) => false,
            PolicyRequesterContext::Typed(PolicyRequester(PolicyRequesterKind::Configured {
                principal,
                ..
            })) => aws_principal_matches_request(principal.principal(), policy_value),
            PolicyRequesterContext::Typed(PolicyRequester(
                PolicyRequesterKind::AssumedRoleSession(session),
            )) => {
                aws_principal_matches_request(session.session_arn().as_str(), policy_value)
                    || aws_principal_matches_request(session.role().arn().as_str(), policy_value)
            }
        }
    }

    fn aws_principal_arn(&self) -> Option<&'a str> {
        match self.requester {
            PolicyRequesterContext::Typed(PolicyRequester(
                PolicyRequesterKind::AssumedRoleSession(session),
            )) => Some(session.role().arn().as_str()),
            PolicyRequesterContext::Typed(PolicyRequester(PolicyRequesterKind::Configured {
                principal_arn: Some(principal_arn),
                ..
            })) => Some(principal_arn.as_str()),
            #[cfg(test)]
            PolicyRequesterContext::Legacy(_) => None,
            PolicyRequesterContext::Typed(PolicyRequester(
                PolicyRequesterKind::Anonymous
                | PolicyRequesterKind::Configured {
                    principal_arn: None,
                    ..
                },
            )) => None,
        }
    }

    fn aws_userid(&self) -> Option<&'a str> {
        match self.requester {
            PolicyRequesterContext::Typed(PolicyRequester(
                PolicyRequesterKind::AssumedRoleSession(session),
            )) => Some(session.assumed_role_id().as_str()),
            #[cfg(test)]
            PolicyRequesterContext::Legacy(_) => None,
            PolicyRequesterContext::Typed(PolicyRequester(
                PolicyRequesterKind::Anonymous | PolicyRequesterKind::Configured { .. },
            )) => None,
        }
    }

    fn token_issue_time_epoch_seconds(&self) -> Option<u64> {
        match self.requester {
            PolicyRequesterContext::Typed(PolicyRequester(
                PolicyRequesterKind::AssumedRoleSession(session),
            )) => u64::try_from(session.lifetime().issued_at_epoch_secs()).ok(),
            #[cfg(test)]
            PolicyRequesterContext::Legacy(_) => None,
            PolicyRequesterContext::Typed(PolicyRequester(
                PolicyRequesterKind::Anonymous | PolicyRequesterKind::Configured { .. },
            )) => None,
        }
    }

    const fn requester_is_anonymous(&self) -> bool {
        match self.requester {
            #[cfg(test)]
            PolicyRequesterContext::Legacy(requester) => requester.principal.is_none(),
            PolicyRequesterContext::Typed(PolicyRequester(PolicyRequesterKind::Anonymous)) => true,
            PolicyRequesterContext::Typed(PolicyRequester(
                PolicyRequesterKind::Configured { .. } | PolicyRequesterKind::AssumedRoleSession(_),
            )) => false,
        }
    }

    #[must_use]
    pub fn resource_arn(&self) -> String {
        if self.bucket_resource {
            format!("arn:aws:s3:::{}", self.bucket)
        } else {
            format!("arn:aws:s3:::{}", self.object_path())
        }
    }

    #[must_use]
    fn object_path(&self) -> String {
        format!("{}/{}", self.bucket, self.key)
    }

    #[must_use]
    fn existing_object_tag_value(&self, key: &str) -> ExistingObjectTagValue<'a> {
        match self.existing_object_tags {
            ExistingObjectTags::Unavailable => ExistingObjectTagValue::Unavailable,
            ExistingObjectTags::Available(tags) => {
                let value = tags
                    .iter()
                    .find(|tag| tag.key == key)
                    .or_else(|| tags.iter().find(|tag| tag.key.eq_ignore_ascii_case(key)))
                    .map(|tag| tag.value);
                ExistingObjectTagValue::Available(value)
            }
        }
    }

    #[must_use]
    fn bucket_tag_value(&self, key: &str) -> BucketTagValue<'a> {
        match self.bucket_tags {
            BucketTags::Unavailable => BucketTagValue::Unavailable,
            BucketTags::Available(tags) => {
                let value = tags
                    .iter()
                    .find(|tag| tag.key == key)
                    .or_else(|| tags.iter().find(|tag| tag.key.eq_ignore_ascii_case(key)))
                    .map(|tag| tag.value);
                BucketTagValue::Available(value)
            }
        }
    }

    #[must_use]
    fn resource_tag_value(&self, key: &str) -> BucketTagValue<'a> {
        match self.bucket_tags {
            BucketTags::Unavailable => BucketTagValue::Unavailable,
            BucketTags::Available(tags) => {
                let lowercase_key = key.to_ascii_lowercase();
                let value = tags
                    .iter()
                    .find(|tag| tag.key == lowercase_key)
                    .or_else(|| tags.iter().find(|tag| tag.key.eq_ignore_ascii_case(key)))
                    .map(|tag| tag.value);
                BucketTagValue::Available(value)
            }
        }
    }

    #[must_use]
    fn request_tag_keys(&self) -> RequestObjectTagKeysValue<'a> {
        match self.request_object_tags {
            RequestObjectTags::Unavailable => RequestObjectTagKeysValue::Unavailable,
            RequestObjectTags::Available(tags) => {
                RequestObjectTagKeysValue::Available(tags.iter().map(|tag| tag.key).collect())
            }
        }
    }

    #[must_use]
    pub fn with_request_object_tags(mut self, request_object_tags: &'a [PolicyTag<'a>]) -> Self {
        self.request_object_tags = RequestObjectTags::Available(request_object_tags);
        self
    }

    #[must_use]
    pub fn with_bucket_tags(mut self, bucket_tags: BucketTags<'a>) -> Self {
        self.bucket_tags = bucket_tags;
        self
    }

    #[must_use]
    pub fn with_copy_source(mut self, copy_source: Option<&'a str>) -> Self {
        self.copy_source = RequestField::Available(copy_source);
        self
    }

    #[must_use]
    pub fn with_metadata_directive(mut self, metadata_directive: Option<&'a str>) -> Self {
        self.metadata_directive = RequestField::Available(metadata_directive);
        self
    }

    #[must_use]
    pub fn with_canned_acl(mut self, canned_acl: Option<&'a str>) -> Self {
        self.canned_acl = RequestField::Available(canned_acl);
        self
    }

    #[must_use]
    pub fn with_server_side_encryption(mut self, server_side_encryption: Option<&'a str>) -> Self {
        self.server_side_encryption = RequestField::Available(server_side_encryption);
        self
    }

    #[must_use]
    pub fn with_sse_customer_algorithm(mut self, sse_customer_algorithm: Option<&'a str>) -> Self {
        self.sse_customer_algorithm = RequestField::Available(sse_customer_algorithm);
        self
    }

    #[must_use]
    pub fn with_grant_read(mut self, grant_read: Option<&'a str>) -> Self {
        self.grant_read = RequestField::Available(grant_read);
        self
    }

    #[must_use]
    pub fn with_grant_write(mut self, grant_write: Option<&'a str>) -> Self {
        self.grant_write = RequestField::Available(grant_write);
        self
    }

    #[must_use]
    pub fn with_grant_read_acp(mut self, grant_read_acp: Option<&'a str>) -> Self {
        self.grant_read_acp = RequestField::Available(grant_read_acp);
        self
    }

    #[must_use]
    pub fn with_grant_write_acp(mut self, grant_write_acp: Option<&'a str>) -> Self {
        self.grant_write_acp = RequestField::Available(grant_write_acp);
        self
    }

    #[must_use]
    pub fn with_grant_full_control(mut self, grant_full_control: Option<&'a str>) -> Self {
        self.grant_full_control = RequestField::Available(grant_full_control);
        self
    }

    #[must_use]
    pub fn with_if_match(mut self, if_match: Option<&'a str>) -> Self {
        self.if_match = RequestField::Available(if_match);
        self
    }

    #[must_use]
    pub fn with_if_none_match(mut self, if_none_match: Option<&'a str>) -> Self {
        self.if_none_match = RequestField::Available(if_none_match);
        self
    }

    #[must_use]
    pub fn with_absent_request_headers(mut self) -> Self {
        self.copy_source = RequestField::Available(None);
        self.metadata_directive = RequestField::Available(None);
        self.canned_acl = RequestField::Available(None);
        self.server_side_encryption = RequestField::Available(None);
        self.sse_customer_algorithm = RequestField::Available(None);
        self.grant_read = RequestField::Available(None);
        self.grant_write = RequestField::Available(None);
        self.grant_read_acp = RequestField::Available(None);
        self.grant_write_acp = RequestField::Available(None);
        self.grant_full_control = RequestField::Available(None);
        self.if_match = RequestField::Available(None);
        self.if_none_match = RequestField::Available(None);
        self
    }

    #[must_use]
    pub fn with_absent_list_parameters(mut self) -> Self {
        self.prefix = RequestField::Available(None);
        self.delimiter = RequestField::Available(None);
        self.max_keys = RequestField::Available(None);
        self
    }

    #[must_use]
    pub fn with_object_creation_operation(
        mut self,
        object_creation_operation: Option<bool>,
    ) -> Self {
        self.object_creation_operation = RequestBool::Available(object_creation_operation);
        self
    }

    #[must_use]
    pub fn with_prefix(mut self, prefix: Option<&'a str>) -> Self {
        self.prefix = RequestField::Available(prefix);
        self
    }

    #[must_use]
    pub fn with_delimiter(mut self, delimiter: Option<&'a str>) -> Self {
        self.delimiter = RequestField::Available(delimiter);
        self
    }

    #[must_use]
    pub fn with_max_keys(mut self, max_keys: Option<&'a str>) -> Self {
        self.max_keys = RequestField::Available(max_keys);
        self
    }

    #[must_use]
    pub fn with_object_ownership(mut self, object_ownership: Option<&'a str>) -> Self {
        self.object_ownership = RequestField::Available(object_ownership);
        self
    }

    #[must_use]
    pub fn with_version_id(mut self, version_id: Option<&'a str>) -> Self {
        self.version_id = RequestField::Available(version_id);
        self
    }

    #[must_use]
    pub const fn with_source_ip(mut self, source_ip: Option<IpAddr>) -> Self {
        self.source_ip = source_ip;
        self
    }

    #[must_use]
    pub const fn with_current_time_epoch_seconds(
        mut self,
        current_time_epoch_seconds: Option<u64>,
    ) -> Self {
        self.current_time_epoch_seconds = current_time_epoch_seconds;
        self
    }

    #[must_use]
    pub const fn with_secure_transport(mut self, secure_transport: Option<bool>) -> Self {
        self.secure_transport = RequestBool::Available(secure_transport);
        self
    }

    #[must_use]
    pub fn with_requested_region(mut self, requested_region: Option<&'a str>) -> Self {
        self.requested_region = RequestField::Available(requested_region);
        self
    }

    #[must_use]
    pub fn with_referer(mut self, referer: Option<&'a str>) -> Self {
        self.referer = RequestField::Available(referer);
        self
    }

    #[must_use]
    pub fn with_auth_type(mut self, auth_type: Option<&'a str>) -> Self {
        self.auth_type = RequestField::Available(auth_type);
        self
    }

    #[must_use]
    pub fn with_signature_version(mut self, signature_version: Option<&'a str>) -> Self {
        self.signature_version = RequestField::Available(signature_version);
        self
    }

    #[must_use]
    pub const fn with_signature_age_millis(mut self, signature_age_millis: Option<u64>) -> Self {
        self.signature_age_millis = Some(signature_age_millis);
        self
    }

    #[must_use]
    pub fn with_tls_version(mut self, tls_version: Option<&'a str>) -> Self {
        self.tls_version = RequestField::Available(tls_version);
        self
    }

    #[must_use]
    pub fn with_content_sha256(mut self, content_sha256: Option<&'a str>) -> Self {
        self.content_sha256 = RequestField::Available(content_sha256);
        self
    }

    #[must_use]
    pub fn with_website_redirect_location(
        mut self,
        website_redirect_location: Option<&'a str>,
    ) -> Self {
        self.website_redirect_location = RequestField::Available(website_redirect_location);
        self
    }

    #[must_use]
    fn copy_source(&self) -> RequestField<'a> {
        self.copy_source
    }

    #[must_use]
    fn metadata_directive(&self) -> RequestField<'a> {
        self.metadata_directive
    }

    #[must_use]
    fn canned_acl(&self) -> RequestField<'a> {
        self.canned_acl
    }

    #[must_use]
    fn server_side_encryption(&self) -> RequestField<'a> {
        self.server_side_encryption
    }

    #[must_use]
    fn sse_customer_algorithm(&self) -> RequestField<'a> {
        self.sse_customer_algorithm
    }

    #[must_use]
    fn request_object_tag_value(&self, key: &str) -> RequestObjectTagValue<'a> {
        match self.request_object_tags {
            RequestObjectTags::Unavailable => RequestObjectTagValue::Unavailable,
            RequestObjectTags::Available(tags) => RequestObjectTagValue::Available(
                tags.iter()
                    .filter(|tag| tag.key.eq_ignore_ascii_case(key))
                    .map(|tag| tag.value)
                    .collect(),
            ),
        }
    }

    #[must_use]
    fn grant_read(&self) -> RequestField<'a> {
        self.grant_read
    }

    #[must_use]
    fn grant_write(&self) -> RequestField<'a> {
        self.grant_write
    }

    #[must_use]
    fn grant_read_acp(&self) -> RequestField<'a> {
        self.grant_read_acp
    }

    #[must_use]
    fn grant_write_acp(&self) -> RequestField<'a> {
        self.grant_write_acp
    }

    #[must_use]
    fn grant_full_control(&self) -> RequestField<'a> {
        self.grant_full_control
    }

    #[must_use]
    fn if_match(&self) -> RequestField<'a> {
        self.if_match
    }

    #[must_use]
    fn if_none_match(&self) -> RequestField<'a> {
        self.if_none_match
    }

    #[must_use]
    fn object_creation_operation(&self) -> RequestBool {
        self.object_creation_operation
    }

    #[must_use]
    fn prefix(&self) -> RequestField<'a> {
        self.prefix
    }

    #[must_use]
    fn delimiter(&self) -> RequestField<'a> {
        self.delimiter
    }

    #[must_use]
    fn max_keys(&self) -> RequestField<'a> {
        self.max_keys
    }

    #[must_use]
    fn object_ownership(&self) -> RequestField<'a> {
        self.object_ownership
    }

    #[must_use]
    fn version_id(&self) -> RequestField<'a> {
        self.version_id
    }

    #[must_use]
    fn source_ip(&self) -> Option<IpAddr> {
        self.source_ip
    }

    #[must_use]
    fn current_time_epoch_seconds(&self) -> Option<u64> {
        self.current_time_epoch_seconds
    }

    #[must_use]
    fn secure_transport(&self) -> RequestBool {
        self.secure_transport
    }

    #[must_use]
    fn requested_region(&self) -> RequestField<'a> {
        self.requested_region
    }

    #[must_use]
    fn referer(&self) -> RequestField<'a> {
        self.referer
    }

    #[must_use]
    fn auth_type(&self) -> RequestField<'a> {
        self.auth_type
    }

    #[must_use]
    fn signature_version(&self) -> RequestField<'a> {
        self.signature_version
    }

    #[must_use]
    fn signature_age_millis(&self) -> Option<Option<u64>> {
        self.signature_age_millis
    }

    #[must_use]
    fn tls_version(&self) -> RequestField<'a> {
        self.tls_version
    }

    #[must_use]
    fn content_sha256(&self) -> RequestField<'a> {
        self.content_sha256
    }

    #[must_use]
    fn website_redirect_location(&self) -> RequestField<'a> {
        self.website_redirect_location
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyStatement {
    principal: PolicyPrincipal,
    core: PolicyStatementCore,
    resources: Vec<String>,
}

impl PolicyStatement {
    #[must_use]
    pub fn sid(&self) -> Option<&str> {
        self.core.sid.as_deref()
    }

    #[must_use]
    pub fn effect(&self) -> PolicyEffect {
        self.core.effect
    }

    #[must_use]
    pub fn principal(&self) -> &PolicyPrincipal {
        &self.principal
    }

    #[must_use]
    pub fn actions(&self) -> &[String] {
        &self.core.actions
    }

    #[must_use]
    pub fn resources(&self) -> &[String] {
        &self.resources
    }

    #[must_use]
    pub fn conditions(&self) -> &[PolicyConditionClause] {
        &self.core.conditions
    }

    fn allows_public_access(&self) -> bool {
        if self.core.effect != PolicyEffect::Allow {
            return false;
        }

        if self.principal.is_fixed_non_public() {
            return false;
        }

        !conditions_constrain_public_principal(&self.core.conditions)
    }

    fn request_effect(
        &self,
        request: &PolicyRequest<'_>,
        action: &str,
        resource: &str,
        variables_enabled: bool,
    ) -> Option<PolicyEffect> {
        if !self.matches_principal(request)
            || !self.matches_action(action)
            || !self.matches_resource(request, resource, variables_enabled)
        {
            return None;
        }

        match self.condition_match_result(request, variables_enabled) {
            ConditionMatchResult::Matches => Some(self.core.effect),
            ConditionMatchResult::NoMatch => None,
            ConditionMatchResult::AcceptedButNotEvaluable => None,
            ConditionMatchResult::InputUnavailable => None,
            ConditionMatchResult::Unsupported => {
                (self.core.effect == PolicyEffect::Deny).then_some(PolicyEffect::Deny)
            }
        }
    }

    fn matches_principal(&self, request: &PolicyRequest<'_>) -> bool {
        self.principal.matches_request(request)
    }

    fn matches_action(&self, action: &str) -> bool {
        self.core
            .actions
            .iter()
            .any(|pattern| action_pattern_matches(pattern, action))
    }

    fn matches_resource(
        &self,
        request: &PolicyRequest<'_>,
        resource: &str,
        variables_enabled: bool,
    ) -> bool {
        self.resources.iter().any(|pattern| {
            let pattern = if variables_enabled {
                let Some(pattern) = expand_policy_template(pattern, request) else {
                    return false;
                };
                pattern
            } else {
                PolicyValue::literal(pattern)
            };
            policy_value_wildcard_matches(&pattern, resource)
        })
    }

    fn references_bucket_tag_condition(&self) -> bool {
        self.core.conditions.iter().any(|clause| {
            condition_key::clause_input(clause) == Some(condition_key::ConditionInput::Bucket)
        })
    }

    fn references_condition_input_for_action(
        &self,
        action: PolicyAction,
        input: condition_key::ConditionInput,
    ) -> bool {
        self.core
            .conditions
            .iter()
            .any(|clause| condition_key::clause_requires_input_for_action(clause, action, input))
    }

    fn condition_validation_error_for_policy_actions(&self) -> Option<BucketPolicyError> {
        SUPPORTED_BUCKET_POLICY_BUCKET_ACTIONS
            .iter()
            .chain(SUPPORTED_BUCKET_POLICY_OBJECT_ACTIONS.iter())
            .copied()
            .filter(|action| {
                self.core
                    .actions
                    .iter()
                    .any(|pattern| action_pattern_matches(pattern, action.as_str()))
            })
            .find_map(|action| {
                self.core.conditions.iter().find_map(|clause| {
                    if !condition_key::is_known_condition_key(clause.key.as_str()) {
                        return Some(BucketPolicyError::malformed_with_detail(
                            "Policy has an invalid condition key",
                            clause.key.clone(),
                        ));
                    }
                    if condition_key::supports_clause_for_action(clause, action) {
                        None
                    } else {
                        Some(BucketPolicyError::malformed(
                            "unsupported Condition for currently enforced bucket policy action",
                        ))
                    }
                })
            })
    }

    fn condition_match_result(
        &self,
        request: &PolicyRequest<'_>,
        variables_enabled: bool,
    ) -> ConditionMatchResult {
        let mut saw_unsupported = false;
        let mut saw_accepted_but_not_evaluable = false;
        let mut saw_input_unavailable = false;
        for clause in &self.core.conditions {
            match condition_clause_matches_request(clause, request, variables_enabled) {
                ConditionMatchResult::Matches => {}
                ConditionMatchResult::NoMatch => return ConditionMatchResult::NoMatch,
                ConditionMatchResult::AcceptedButNotEvaluable => {
                    saw_accepted_but_not_evaluable = true;
                }
                ConditionMatchResult::InputUnavailable => {
                    saw_input_unavailable = true;
                }
                ConditionMatchResult::Unsupported => saw_unsupported = true,
            }
        }
        if saw_unsupported {
            ConditionMatchResult::Unsupported
        } else if saw_input_unavailable {
            ConditionMatchResult::InputUnavailable
        } else if saw_accepted_but_not_evaluable {
            ConditionMatchResult::AcceptedButNotEvaluable
        } else {
            ConditionMatchResult::Matches
        }
    }

    fn push_normalized_json(&self, out: &mut String) {
        out.push('{');
        let mut first_field = true;

        if let Some(sid) = self.sid() {
            push_json_field_name(out, "Sid", &mut first_field);
            push_json_string(out, sid);
        }

        push_json_field_name(out, "Effect", &mut first_field);
        push_json_string(out, self.core.effect.as_str());

        push_json_field_name(out, "Principal", &mut first_field);
        self.principal.push_normalized_json(out);

        push_json_field_name(out, "Action", &mut first_field);
        push_json_string_or_array(out, self.actions());

        push_json_field_name(out, "Resource", &mut first_field);
        push_json_string_or_array(out, self.resources());

        if !self.core.conditions.is_empty() {
            push_json_field_name(out, "Condition", &mut first_field);
            push_condition_map(out, self.conditions());
        }

        out.push('}');
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistingObjectTagValue<'a> {
    Unavailable,
    Available(Option<&'a str>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketTagValue<'a> {
    Unavailable,
    Available(Option<&'a str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RequestObjectTagKeysValue<'a> {
    Unavailable,
    Available(Vec<&'a str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RequestObjectTagValue<'a> {
    Unavailable,
    Available(Vec<&'a str>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConditionMatchResult {
    Matches,
    NoMatch,
    AcceptedButNotEvaluable,
    InputUnavailable,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PolicyPrincipal {
    aws: Vec<String>,
    service: Vec<String>,
    canonical_user: Vec<String>,
    wildcard: bool,
}

impl PolicyPrincipal {
    #[must_use]
    pub fn aws(&self) -> &[String] {
        &self.aws
    }

    #[must_use]
    pub fn service(&self) -> &[String] {
        &self.service
    }

    #[must_use]
    pub fn canonical_user(&self) -> &[String] {
        &self.canonical_user
    }

    #[must_use]
    pub fn wildcard(&self) -> bool {
        self.wildcard
    }

    fn is_fixed_non_public(&self) -> bool {
        !self.wildcard && self.has_any() && self.all_values().into_iter().all(is_fixed_value)
    }

    fn has_any(&self) -> bool {
        self.wildcard
            || !self.aws.is_empty()
            || !self.service.is_empty()
            || !self.canonical_user.is_empty()
    }

    fn all_values(&self) -> Vec<&str> {
        self.aws
            .iter()
            .chain(&self.service)
            .chain(&self.canonical_user)
            .map(String::as_str)
            .collect()
    }

    fn matches_request(&self, request: &PolicyRequest<'_>) -> bool {
        if self.wildcard {
            return true;
        }

        let requester_canonical_user_id = request.requester_canonical_user_id();

        self.aws
            .iter()
            .any(|value| request.principal_matches(value))
            || self
                .service
                .iter()
                .any(|value| request.requester_principal() == Some(value.as_str()))
            || self.canonical_user.iter().any(|value| {
                requester_canonical_user_id.is_some_and(|requester| requester.as_str() == value)
            })
    }

    fn push_normalized_json(&self, out: &mut String) {
        if self.wildcard {
            push_json_string(out, "*");
            return;
        }

        out.push('{');
        let mut first_field = true;
        if !self.aws().is_empty() {
            push_json_field_name(out, "AWS", &mut first_field);
            push_json_string_or_array(out, self.aws());
        }
        if !self.canonical_user().is_empty() {
            push_json_field_name(out, "CanonicalUser", &mut first_field);
            push_json_string_or_array(out, self.canonical_user());
        }
        if !self.service().is_empty() {
            push_json_field_name(out, "Service", &mut first_field);
            push_json_string_or_array(out, self.service());
        }
        out.push('}');
    }
}

fn push_json_field_name(out: &mut String, field_name: &str, first_field: &mut bool) {
    if !*first_field {
        out.push(',');
    }
    *first_field = false;
    push_json_string(out, field_name);
    out.push(':');
}

fn push_json_string(out: &mut String, value: &str) {
    out.push_str(&serde_json::to_string(value).expect("serializing JSON string"));
}

fn push_json_string_or_array(out: &mut String, values: &[String]) {
    match values {
        [value] => push_json_string(out, value),
        _ => {
            out.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                push_json_string(out, value);
            }
            out.push(']');
        }
    }
}

fn push_condition_map(out: &mut String, clauses: &[PolicyConditionClause]) {
    type ConditionOperandGroup<'a> = Vec<(&'a str, &'a [String])>;
    type ConditionOperatorGroup<'a> = Vec<(&'a str, ConditionOperandGroup<'a>)>;

    let mut grouped: ConditionOperatorGroup<'_> = Vec::new();
    for clause in clauses {
        if let Some((_, operands)) = grouped
            .iter_mut()
            .find(|(operator, _)| *operator == clause.operator())
        {
            operands.push((clause.key(), clause.values()));
        } else {
            grouped.push((clause.operator(), vec![(clause.key(), clause.values())]));
        }
    }

    out.push('{');
    for (operator_index, (operator, operands)) in grouped.iter().enumerate() {
        if operator_index > 0 {
            out.push(',');
        }
        push_json_string(out, operator);
        out.push(':');
        out.push('{');
        for (operand_index, (key, values)) in operands.iter().enumerate() {
            if operand_index > 0 {
                out.push(',');
            }
            push_json_string(out, key);
            out.push(':');
            push_json_string_or_array(out, values);
        }
        out.push('}');
    }
    out.push('}');
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum BucketPolicyError {
    #[error("malformed policy: {reason}")]
    Malformed {
        reason: &'static str,
        /// Offending value echoed by AWS in the error body's `<Detail>`
        /// element, e.g. the invalid action, resource, or principal.
        detail: Option<String>,
    },
}

impl BucketPolicyError {
    #[must_use]
    pub fn malformed(reason: &'static str) -> Self {
        Self::Malformed {
            reason,
            detail: None,
        }
    }

    #[must_use]
    pub fn malformed_with_detail(reason: &'static str, detail: impl Into<String>) -> Self {
        Self::Malformed {
            reason,
            detail: Some(detail.into()),
        }
    }

    #[must_use]
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Malformed { reason, .. } => reason,
        }
    }

    #[must_use]
    pub fn detail(&self) -> Option<&str> {
        match self {
            Self::Malformed { detail, .. } => detail.as_deref(),
        }
    }
}

impl BucketPolicy {
    /// The first statement resource not scoped to `bucket`, if any. AWS
    /// rejects PutBucketPolicy unless every resource is the bucket's ARN or
    /// an object ARN under it ("Policy has invalid resource"); wildcards do
    /// not span buckets (`arn:aws:s3:::*` is rejected).
    #[must_use]
    pub fn first_resource_not_scoped_to_bucket(&self, bucket: &str) -> Option<&str> {
        let bucket_arn = format!("arn:aws:s3:::{bucket}");
        self.statements
            .iter()
            .flat_map(|statement| statement.resources.iter())
            .find(|resource| {
                resource.as_str() != bucket_arn
                    && !resource
                        .strip_prefix(bucket_arn.as_str())
                        .is_some_and(|rest| rest.starts_with('/'))
            })
            .map(String::as_str)
    }
}

pub fn parse_bucket_policy(policy: &str) -> Result<BucketPolicy, BucketPolicyError> {
    let value: Value = serde_json::from_str(policy).map_err(|_| {
        BucketPolicyError::malformed("Policies must be valid JSON and the first byte must be '{'")
    })?;
    let object = value.as_object().ok_or_else(|| {
        BucketPolicyError::malformed("Policies must be valid JSON and the first byte must be '{'")
    })?;

    let version = match object.get("Version") {
        Some(Value::String(version)) => Some(parse_version(version)?),
        Some(_) => {
            return Err(BucketPolicyError::malformed("Version must be a string"));
        }
        None => None,
    };

    let statements = parse_statements(object.get("Statement").ok_or(
        BucketPolicyError::malformed("Missing required field Statement"),
    )?)?;

    Ok(BucketPolicy {
        version,
        statements,
    })
}

fn parse_version(version: &str) -> Result<PolicyVersion, BucketPolicyError> {
    match version {
        "2008-10-17" => Ok(PolicyVersion::V2008_10_17),
        "2012-10-17" => Ok(PolicyVersion::V2012_10_17),
        _ => Err(BucketPolicyError::malformed(
            "The policy must contain a valid version string",
        )),
    }
}

fn parse_statements(value: &Value) -> Result<Vec<PolicyStatement>, BucketPolicyError> {
    match value {
        Value::Array(statements) => {
            if statements.is_empty() {
                return Err(BucketPolicyError::malformed(
                    "Could not parse the policy: Statement is empty!",
                ));
            }
            statements
                .iter()
                .enumerate()
                .map(|(index, statement)| parse_statement(statement, index))
                .collect()
        }
        Value::Object(_) => Ok(vec![parse_statement(value, 0)?]),
        _ => Err(BucketPolicyError::malformed(
            "Statement must be an object or array",
        )),
    }
}

fn parse_statement(value: &Value, index: usize) -> Result<PolicyStatement, BucketPolicyError> {
    let object = value
        .as_object()
        .ok_or(BucketPolicyError::malformed("statement must be an object"))?;

    if object.contains_key("NotPrincipal")
        || object.contains_key("NotAction")
        || object.contains_key("NotResource")
    {
        return Err(BucketPolicyError::malformed(
            "NotPrincipal, NotAction, and NotResource are not supported",
        ));
    }

    let sid = match object.get("Sid") {
        Some(Value::String(sid)) => Some(sid.clone()),
        Some(_) => {
            return Err(BucketPolicyError::malformed("Sid must be a string"));
        }
        None => None,
    };

    let effect = match object.get("Effect") {
        Some(Value::String(effect)) => parse_effect(effect)?,
        Some(_) => {
            return Err(BucketPolicyError::malformed("Effect must be a string"));
        }
        None => {
            return Err(BucketPolicyError::malformed("missing Effect"));
        }
    };

    let principal = parse_principal(
        object
            .get("Principal")
            .ok_or(BucketPolicyError::malformed("missing Principal"))?,
    )?;

    let actions = parse_string_or_array(
        object
            .get("Action")
            .ok_or(BucketPolicyError::malformed("missing Action"))?,
        "Action must be a string or array of strings",
    )?;
    let resources = parse_string_or_array(
        object
            .get("Resource")
            .ok_or(BucketPolicyError::malformed("missing Resource"))?,
        "Resource must be a string or array of strings",
    )?;
    let statement_label = sid.clone().unwrap_or_else(|| format!("NO_ID-{index}"));
    validate_resource_applicability(&actions, &resources, &statement_label)?;
    let conditions = match object.get("Condition") {
        Some(value) => parse_conditions(value)?,
        None => Vec::new(),
    };

    Ok(PolicyStatement {
        principal,
        core: PolicyStatementCore::new(sid, effect, actions, conditions),
        resources,
    })
}

fn validate_resource_applicability(
    actions: &[String],
    resources: &[String],
    statement_label: &str,
) -> Result<(), BucketPolicyError> {
    let matches_bucket_action = |pattern: &String| {
        SUPPORTED_BUCKET_POLICY_BUCKET_ACTIONS
            .iter()
            .any(|action| action_pattern_matches(pattern, action.as_str()))
    };
    let matches_object_action = |pattern: &String| {
        SUPPORTED_BUCKET_POLICY_OBJECT_ACTIONS
            .iter()
            .any(|action| action_pattern_matches(pattern, action.as_str()))
    };
    if let Some(unsupported) = actions
        .iter()
        .find(|pattern| !matches_bucket_action(pattern) && !matches_object_action(pattern))
    {
        return Err(BucketPolicyError::Malformed {
            reason: "Policy has invalid action",
            detail: Some(unsupported.clone()),
        });
    }

    let has_bucket_resource = resources.iter().any(|resource| {
        matches!(
            resource_scope(resource),
            Some(ResourceScope::Bucket) | Some(ResourceScope::Both)
        )
    });
    let has_object_resource = resources.iter().any(|resource| {
        matches!(
            resource_scope(resource),
            Some(ResourceScope::Object) | Some(ResourceScope::Both)
        )
    });

    // AWS names the first action whose resource kind is absent, e.g.
    // `Action "s3:GetObject" in Statement "NO_ID-0"`.
    let inapplicable = actions.iter().find(|pattern| {
        (!has_object_resource && matches_object_action(pattern))
            || (!has_bucket_resource && matches_bucket_action(pattern))
    });
    if let Some(action) = inapplicable {
        return Err(BucketPolicyError::Malformed {
            reason: "Action does not apply to any resource(s) in statement",
            detail: Some(format!(
                "Action \"{action}\" in Statement \"{statement_label}\""
            )),
        });
    }

    Ok(())
}

const SUPPORTED_BUCKET_POLICY_BUCKET_ACTIONS: [PolicyAction; 30] = [
    PolicyAction::DeleteBucket,
    PolicyAction::GetBucketPolicy,
    PolicyAction::PutBucketPolicy,
    PolicyAction::DeleteBucketPolicy,
    PolicyAction::GetBucketLocation,
    PolicyAction::GetBucketCors,
    PolicyAction::GetBucketAcl,
    PolicyAction::GetBucketVersioning,
    PolicyAction::GetBucketOwnershipControls,
    PolicyAction::GetBucketTagging,
    PolicyAction::GetEncryptionConfiguration,
    PolicyAction::GetLifecycleConfiguration,
    PolicyAction::GetBucketPolicyStatus,
    PolicyAction::GetBucketPublicAccessBlock,
    PolicyAction::GetBucketObjectLockConfiguration,
    PolicyAction::ListBucket,
    PolicyAction::ListBucketVersions,
    PolicyAction::ListBucketMultipartUploads,
    PolicyAction::PutBucketAcl,
    PolicyAction::PutBucketCors,
    PolicyAction::PutBucketVersioning,
    PolicyAction::PutBucketOwnershipControls,
    PolicyAction::PutBucketTagging,
    PolicyAction::PutEncryptionConfiguration,
    PolicyAction::PutLifecycleConfiguration,
    PolicyAction::PutBucketPublicAccessBlock,
    PolicyAction::PutBucketObjectLockConfiguration,
    PolicyAction::ListTagsForResource,
    PolicyAction::TagResource,
    PolicyAction::UntagResource,
];

const SUPPORTED_BUCKET_POLICY_OBJECT_ACTIONS: [PolicyAction; 23] = [
    PolicyAction::GetObject,
    PolicyAction::GetObjectVersion,
    PolicyAction::GetObjectAttributes,
    PolicyAction::GetObjectVersionAttributes,
    PolicyAction::GetObjectAcl,
    PolicyAction::GetObjectVersionAcl,
    PolicyAction::GetObjectTagging,
    PolicyAction::GetObjectVersionTagging,
    PolicyAction::GetObjectRetention,
    PolicyAction::GetObjectLegalHold,
    PolicyAction::PutObject,
    PolicyAction::PutObjectAcl,
    PolicyAction::PutObjectVersionAcl,
    PolicyAction::PutObjectTagging,
    PolicyAction::PutObjectVersionTagging,
    PolicyAction::PutObjectRetention,
    PolicyAction::PutObjectLegalHold,
    PolicyAction::BypassGovernanceRetention,
    PolicyAction::AbortMultipartUpload,
    PolicyAction::DeleteObject,
    PolicyAction::DeleteObjectVersion,
    PolicyAction::DeleteObjectTagging,
    PolicyAction::DeleteObjectVersionTagging,
];

pub(crate) fn policy_action_pattern_is_supported(pattern: &str) -> bool {
    SUPPORTED_BUCKET_POLICY_BUCKET_ACTIONS
        .iter()
        .chain(SUPPORTED_BUCKET_POLICY_OBJECT_ACTIONS.iter())
        .any(|action| action_pattern_matches(pattern, action.as_str()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResourceScope {
    Bucket,
    Object,
    Both,
}

fn resource_scope(resource: &str) -> Option<ResourceScope> {
    if resource == "*" {
        return Some(ResourceScope::Both);
    }

    let suffix = resource.strip_prefix("arn:aws:s3:::")?;
    if suffix == "*" {
        return Some(ResourceScope::Both);
    }
    if suffix.contains('/') {
        return Some(ResourceScope::Object);
    }
    Some(ResourceScope::Bucket)
}

fn parse_effect(effect: &str) -> Result<PolicyEffect, BucketPolicyError> {
    match effect {
        "Allow" => Ok(PolicyEffect::Allow),
        "Deny" => Ok(PolicyEffect::Deny),
        _ => Err(BucketPolicyError::malformed("Effect must be Allow or Deny")),
    }
}

fn parse_principal(value: &Value) -> Result<PolicyPrincipal, BucketPolicyError> {
    match value {
        Value::String(principal) => {
            if principal == "*" {
                return Ok(PolicyPrincipal {
                    wildcard: true,
                    ..PolicyPrincipal::default()
                });
            }

            // AWS accepts only "*" in string position; every other string,
            // including a well-formed ARN, is a syntax error (probed).
            Err(BucketPolicyError::malformed("Invalid policy syntax."))
        }
        Value::Object(object) => {
            let mut principal = PolicyPrincipal::default();
            for (kind, value) in object {
                let values = parse_string_or_array(
                    value,
                    "Principal value must be a string or array of strings",
                )?;
                match kind.as_str() {
                    "AWS" => {
                        for value in values {
                            if value == "*" {
                                principal.wildcard = true;
                            } else {
                                validate_aws_principal_format(&value)?;
                                principal.aws.push(value);
                            }
                        }
                    }
                    "Service" => principal.service.extend(values),
                    "CanonicalUser" => principal.canonical_user.extend(values),
                    _ => {
                        return Err(BucketPolicyError::malformed("unsupported Principal type"));
                    }
                }
            }
            if principal.has_any() {
                Ok(principal)
            } else {
                Err(BucketPolicyError::malformed("Principal must not be empty"))
            }
        }
        _ => Err(BucketPolicyError::malformed(
            "Principal must be a string or object",
        )),
    }
}

/// Reject `AWS` principal entries whose IAM ARN qualifier is not one AWS
/// accepts. This is format validation only: AWS additionally rejects
/// principals that do not exist, which depends on the account universe and
/// is not checked here.
fn validate_aws_principal_format(value: &str) -> Result<(), BucketPolicyError> {
    let Some(rest) = value.strip_prefix("arn:aws:iam::") else {
        return Ok(());
    };
    let invalid = || BucketPolicyError::Malformed {
        reason: "Invalid principal in policy",
        detail: Some(format!("\"AWS\" : \"{value}\"")),
    };
    let Some((_account, qualifier)) = rest.split_once(':') else {
        return Err(invalid());
    };
    let valid = qualifier == "root"
        || qualifier
            .strip_prefix("user/")
            .is_some_and(|name| !name.is_empty())
        || qualifier
            .strip_prefix("role/")
            .is_some_and(|name| !name.is_empty());
    if valid {
        Ok(())
    } else {
        Err(invalid())
    }
}

fn parse_conditions(value: &Value) -> Result<Vec<PolicyConditionClause>, BucketPolicyError> {
    let operators = value
        .as_object()
        .ok_or(BucketPolicyError::malformed("Condition must be an object"))?;
    let mut clauses = Vec::new();
    for (operator, operands) in operators {
        let operand_object = operands.as_object().ok_or(BucketPolicyError::malformed(
            "Condition operator value must be an object",
        ))?;
        for (key, value) in operand_object {
            let clause = PolicyConditionClause {
                operator: operator.clone(),
                key: key.clone(),
                values: parse_condition_value(
                    value,
                    "Condition value must be a string, number, or array of strings or numbers",
                )?,
            };
            validate_condition_operands(&clause)?;
            clauses.push(clause);
        }
    }
    Ok(clauses)
}

fn parse_string_or_array(
    value: &Value,
    field_name: &'static str,
) -> Result<Vec<String>, BucketPolicyError> {
    match value {
        Value::String(value) => Ok(vec![value.clone()]),
        Value::Array(values) => {
            if values.is_empty() {
                return Err(BucketPolicyError::malformed(
                    "array field must not be empty",
                ));
            }
            let mut parsed = Vec::with_capacity(values.len());
            for value in values {
                let value = value
                    .as_str()
                    .ok_or(BucketPolicyError::malformed(field_name))?;
                parsed.push(value.to_string());
            }
            Ok(parsed)
        }
        _ => Err(BucketPolicyError::malformed(field_name)),
    }
}

fn parse_condition_value(
    value: &Value,
    field_name: &'static str,
) -> Result<Vec<String>, BucketPolicyError> {
    match value {
        Value::String(value) => Ok(vec![value.clone()]),
        Value::Number(value) => Ok(vec![value.to_string()]),
        Value::Array(values) => {
            if values.is_empty() {
                return Err(BucketPolicyError::malformed(
                    "array field must not be empty",
                ));
            }
            let mut parsed = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    Value::String(value) => parsed.push(value.clone()),
                    Value::Number(value) => parsed.push(value.to_string()),
                    _ => return Err(BucketPolicyError::malformed(field_name)),
                }
            }
            Ok(parsed)
        }
        _ => Err(BucketPolicyError::malformed(field_name)),
    }
}

fn conditions_constrain_public_principal(conditions: &[PolicyConditionClause]) -> bool {
    conditions.iter().any(is_non_public_condition_clause)
}

fn condition_clause_matches_request(
    clause: &PolicyConditionClause,
    request: &PolicyRequest<'_>,
    variables_enabled: bool,
) -> ConditionMatchResult {
    condition_key::evaluate_clause(clause, request, variables_enabled)
}

fn expand_policy_template(template: &str, request: &PolicyRequest<'_>) -> Option<PolicyValue> {
    let mut cursor = 0;
    let mut expanded: Option<PolicyValue> = None;

    while let Some(relative_start) = template[cursor..].find("${") {
        let start = cursor + relative_start;
        let content_start = start + 2;
        let Some(relative_end) = template[content_start..].find('}') else {
            break;
        };
        let end = content_start + relative_end;
        let output = expanded.get_or_insert_with(|| PolicyValue::literal(""));
        push_policy_literal(output, &template[cursor..start]);
        push_policy_variable(
            output,
            &resolve_policy_template_variable(&template[content_start..end], request)?,
        );
        cursor = end + 1;
    }

    match expanded {
        Some(mut output) => {
            push_policy_literal(&mut output, &template[cursor..]);
            Some(output)
        }
        None => Some(PolicyValue::literal(template)),
    }
}

enum PolicyVariableValue {
    Text(String),
    LiteralWildcard(char),
}

fn push_policy_literal(output: &mut PolicyValue, value: &str) {
    output.push_pattern_fragment(value);
}

fn push_policy_variable(output: &mut PolicyValue, value: &PolicyVariableValue) {
    match value {
        PolicyVariableValue::Text(value) => output.push_pattern_fragment(value),
        PolicyVariableValue::LiteralWildcard('*') => output.push_literal_asterisk(),
        PolicyVariableValue::LiteralWildcard('?') => output.push_literal_question_mark(),
        PolicyVariableValue::LiteralWildcard(value) => {
            output.push_pattern_fragment(&value.to_string());
        }
    }
}

fn resolve_policy_template_variable(
    content: &str,
    request: &PolicyRequest<'_>,
) -> Option<PolicyVariableValue> {
    match content {
        "*" => return Some(PolicyVariableValue::LiteralWildcard('*')),
        "?" => return Some(PolicyVariableValue::LiteralWildcard('?')),
        "$" => return Some(PolicyVariableValue::Text("$".to_string())),
        _ => {}
    }

    let (key, default) = parse_policy_variable_default(content);
    match condition_key::resolve_policy_variable(request, key) {
        condition_key::PolicyVariableResolution::Value(value) => {
            Some(PolicyVariableValue::Text(value))
        }
        condition_key::PolicyVariableResolution::Absent => {
            default.map(|value| PolicyVariableValue::Text(value.to_string()))
        }
        condition_key::PolicyVariableResolution::Unavailable => None,
    }
}

fn parse_policy_variable_default(content: &str) -> (&str, Option<&str>) {
    let Some((key, default)) = content.split_once(',') else {
        return (content.trim(), None);
    };
    let default = default.trim();
    let Some(default) = default
        .strip_prefix('\'')
        .and_then(|value| value.strip_suffix('\''))
    else {
        return (content.trim(), None);
    };
    (key.trim(), Some(default))
}

fn policy_string_equals(expected: &PolicyValue, actual: &str) -> bool {
    expected.as_str() == actual
}

fn policy_string_equals_ignore_case(expected: &PolicyValue, actual: &str) -> bool {
    expected.as_str() == actual || expected.as_str().to_lowercase() == actual.to_lowercase()
}

#[doc(hidden)]
#[must_use]
pub fn clause_supported_for_action_for_tests(
    operator: &str,
    key: &str,
    action: PolicyAction,
) -> bool {
    let clause = PolicyConditionClause {
        operator: operator.to_string(),
        key: key.to_string(),
        values: vec!["test".to_string()],
    };
    condition_key::supports_clause_for_action(&clause, action)
}

fn aws_principal_matches_request(requester_principal: &str, policy_value: &str) -> bool {
    if requester_principal == policy_value {
        return true;
    }

    let Some(policy_root_account_id) = root_account_principal_account_id(policy_value) else {
        return false;
    };

    requester_principal == policy_root_account_id
        || aws_account_id_from_principal(requester_principal) == Some(policy_root_account_id)
}

fn root_account_principal_account_id(value: &str) -> Option<&str> {
    let account_id = aws_account_id_from_principal(value)?;
    value.ends_with(":root").then_some(account_id)
}

fn is_non_public_condition_clause(clause: &PolicyConditionClause) -> bool {
    if condition_key_matches_any(
        &clause.key,
        &[
            "aws:PrincipalOrgID",
            "aws:SourceVpce",
            "aws:SourceOwner",
            "aws:SourceAccount",
            "aws:userid",
            "s3:DataAccessPointAccount",
        ],
    ) {
        matches!(
            clause.operator.as_str(),
            "StringEquals" | "StringEqualsIgnoreCase" | "StringLike"
        ) && clause.values.iter().all(|value| is_fixed_value(value))
    } else if clause.key.eq_ignore_ascii_case("aws:PrincipalArn") {
        clause.operator == "ArnEquals" && clause.values.iter().all(|value| is_fixed_value(value))
    } else if condition_key_matches_any(&clause.key, &["aws:SourceArn", "s3:DataAccessPointArn"]) {
        matches!(
            clause.operator.as_str(),
            "ArnEquals" | "ArnLike" | "StringEquals" | "StringEqualsIgnoreCase" | "StringLike"
        ) && clause.values.iter().all(|value| is_fixed_value(value))
    } else if clause.key.eq_ignore_ascii_case("aws:SourceIp") {
        matches!(
            clause.operator.as_str(),
            "IpAddress" | "ForAnyValue:IpAddress"
        ) && clause.values.iter().all(|value| is_fixed_source_ip(value))
    } else {
        false
    }
}

fn condition_key_matches_any(key: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|candidate| key.eq_ignore_ascii_case(candidate))
}

fn is_fixed_value(value: &str) -> bool {
    !value.contains('*') && !value.contains("${")
}

fn is_fixed_source_ip(value: &str) -> bool {
    if !is_fixed_value(value) {
        return false;
    }

    let Some((addr, prefix)) = parse_ip_addr_or_cidr(value) else {
        return false;
    };

    match addr {
        IpAddr::V4(addr) => ipv4_source_ip_is_non_public(addr, prefix),
        IpAddr::V6(_) => prefix >= 32,
    }
}

fn parse_ip_addr_or_cidr(value: &str) -> Option<(IpAddr, u8)> {
    if let Some((addr, prefix)) = value.split_once('/') {
        let addr = addr.parse::<IpAddr>().ok()?;
        let prefix = prefix.parse::<u8>().ok()?;
        let max_prefix = match addr {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        (prefix <= max_prefix).then_some((addr, prefix))
    } else {
        let addr = value.parse::<IpAddr>().ok()?;
        let prefix = match addr {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        Some((addr, prefix))
    }
}

fn validate_condition_operands(clause: &PolicyConditionClause) -> Result<(), BucketPolicyError> {
    let Some(op) = condition_op::lookup(clause.operator()) else {
        return Ok(());
    };
    if clause.operator() == "Null" {
        return Ok(());
    }
    if condition_op::is_binary_condition_kind(op.kind) {
        return validate_binary_condition_operands(clause);
    }
    if !condition_op::is_ip_condition_kind(op.kind) {
        return Ok(());
    }
    if clause
        .values()
        .iter()
        .all(|value| parse_ip_addr_or_cidr(value).is_some())
    {
        Ok(())
    } else {
        Err(BucketPolicyError::malformed(
            "Invalid IP address in Conditions",
        ))
    }
}

fn validate_binary_condition_operands(
    clause: &PolicyConditionClause,
) -> Result<(), BucketPolicyError> {
    if clause
        .values()
        .iter()
        .all(|value| condition_op::decode_binary_value(value).is_some())
    {
        Ok(())
    } else {
        Err(BucketPolicyError::malformed(
            "Invalid Base64 value for binary condition",
        ))
    }
}

fn ipv4_source_ip_is_non_public(addr: Ipv4Addr, prefix: u8) -> bool {
    prefix >= 8 || ipv4_cidr_is_subset_of_rfc1918(addr, prefix)
}

fn ipv4_cidr_is_subset_of_rfc1918(addr: Ipv4Addr, prefix: u8) -> bool {
    ipv4_cidr_is_subset_of(addr, prefix, Ipv4Addr::new(10, 0, 0, 0), 8)
        || ipv4_cidr_is_subset_of(addr, prefix, Ipv4Addr::new(172, 16, 0, 0), 12)
        || ipv4_cidr_is_subset_of(addr, prefix, Ipv4Addr::new(192, 168, 0, 0), 16)
}

fn ipv4_cidr_is_subset_of(
    addr: Ipv4Addr,
    prefix: u8,
    range_addr: Ipv4Addr,
    range_prefix: u8,
) -> bool {
    prefix >= range_prefix
        && ipv4_prefix_bits(addr, range_prefix) == ipv4_prefix_bits(range_addr, range_prefix)
}

fn ipv4_prefix_bits(addr: Ipv4Addr, prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::from(addr) & (u32::MAX << (32 - u32::from(prefix)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AwsAccountId, IamPath, IamRoleIdentity, RoleName, RoleSessionName, SessionLifetime,
        StableRoleId,
    };

    fn request<'a>(
        action: PolicyAction,
        bucket: &'a str,
        key: &'a str,
        requester_principal: Option<&'a str>,
        existing_object_tags: &'a [PolicyTag<'a>],
    ) -> PolicyRequest<'a> {
        PolicyRequest::for_object(
            action,
            bucket,
            key,
            requester_principal,
            None,
            ExistingObjectTags::Available(existing_object_tags),
        )
    }

    fn bucket_request<'a>(
        action: PolicyAction,
        bucket: &'a str,
        requester_principal: Option<&'a str>,
    ) -> PolicyRequest<'a> {
        PolicyRequest::for_bucket(
            action,
            bucket,
            requester_principal,
            None,
            BucketTags::Unavailable,
        )
    }

    fn bucket_request_with_tags<'a>(
        action: PolicyAction,
        bucket: &'a str,
        requester_principal: Option<&'a str>,
        bucket_tags: &'a [PolicyTag<'a>],
    ) -> PolicyRequest<'a> {
        PolicyRequest::for_bucket(
            action,
            bucket,
            requester_principal,
            None,
            BucketTags::Available(bucket_tags),
        )
    }

    fn request_with_bucket_tags<'a>(
        action: PolicyAction,
        bucket: &'a str,
        key: &'a str,
        requester_principal: Option<&'a str>,
        existing_object_tags: &'a [PolicyTag<'a>],
        bucket_tags: &'a [PolicyTag<'a>],
    ) -> PolicyRequest<'a> {
        request(
            action,
            bucket,
            key,
            requester_principal,
            existing_object_tags,
        )
        .with_bucket_tags(BucketTags::Available(bucket_tags))
    }

    fn assumed_role_identity() -> AuthenticatedIdentity {
        let session = crate::AssumedRoleSessionIdentity::new(
            IamRoleIdentity::new(
                AwsAccountId::new("123456789012").unwrap(),
                StableRoleId::new("ARGR0123456789ABCDEFGHIJ").unwrap(),
                RoleName::new("test-role").unwrap(),
                IamPath::new("/team/").unwrap(),
            ),
            RoleSessionName::new("test-session").unwrap(),
            SessionLifetime::new(1_704_067_200, 1_704_070_800).unwrap(),
            None,
        );
        AuthenticatedIdentity::assumed_role_session(
            s3_types::AccountIdentity::new(
                "123456789012",
                CanonicalUserId::from_principal("123456789012"),
                "Test account",
            ),
            session,
        )
        .unwrap()
    }

    fn assumed_role_request<'a>(
        identity: &'a AuthenticatedIdentity,
        key: &'a str,
    ) -> PolicyRequest<'a> {
        PolicyRequest::for_object_with_requester(
            PolicyAction::PutObject,
            "bucket",
            key,
            PolicyRequester::authenticated(identity),
            ExistingObjectTags::Unavailable,
        )
    }

    #[test]
    fn assumed_role_resource_policy_matches_role_and_session_principal_arns() {
        let identity = assumed_role_identity();
        let session = identity.role_session().unwrap();
        let policy = parse_bucket_policy(&format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/role"}},{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/session"}}]}}"#,
            session.role().arn().as_str(),
            session.session_arn().as_str(),
        ))
        .unwrap();

        assert_eq!(
            policy.evaluate(&assumed_role_request(&identity, "role")),
            PolicyEvaluation::ExplicitAllow
        );
        assert_eq!(
            policy.evaluate(&assumed_role_request(&identity, "session")),
            PolicyEvaluation::ExplicitAllow
        );
    }

    #[test]
    fn assumed_role_global_condition_values_are_distinct_and_immutable() {
        let identity = assumed_role_identity();
        let session = identity.role_session().unwrap();
        let policy = parse_bucket_policy(&format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/principal-role","Condition":{{"ArnEquals":{{"aws:PrincipalArn":"{}"}}}}}},{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/principal-session","Condition":{{"ArnEquals":{{"aws:PrincipalArn":"{}"}}}}}},{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/userid","Condition":{{"StringEquals":{{"aws:userid":"{}"}}}}}},{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/issued-after","Condition":{{"DateGreaterThan":{{"aws:TokenIssueTime":"2023-12-31T23:59:59Z"}}}}}},{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/issued-before","Condition":{{"DateLessThan":{{"aws:TokenIssueTime":"2024-01-01T00:00:01Z"}}}}}}]}}"#,
            session.role().arn().as_str(),
            session.role().arn().as_str(),
            session.role().arn().as_str(),
            session.session_arn().as_str(),
            session.role().arn().as_str(),
            session.assumed_role_id().as_str(),
            session.role().arn().as_str(),
            session.role().arn().as_str(),
        ))
        .unwrap();

        for key in ["principal-role", "userid", "issued-after", "issued-before"] {
            assert_eq!(
                policy.evaluate(&assumed_role_request(&identity, key)),
                PolicyEvaluation::ExplicitAllow,
                "{key}"
            );
        }
        assert_eq!(
            policy.evaluate(&assumed_role_request(&identity, "principal-session")),
            PolicyEvaluation::NoMatch
        );

        let deny_policy = parse_bucket_policy(&format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/issue-deny"}},{{"Effect":"Deny","Principal":{{"AWS":"{}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/issue-deny","Condition":{{"DateLessThan":{{"aws:TokenIssueTime":"2024-01-01T00:00:01Z"}}}}}}]}}"#,
            session.role().arn().as_str(),
            session.role().arn().as_str(),
        ))
        .unwrap();
        assert_eq!(
            deny_policy.evaluate(&assumed_role_request(&identity, "issue-deny")),
            PolicyEvaluation::ExplicitDeny
        );
    }

    #[test]
    fn account_bound_configured_iam_user_principal_arn_is_evaluated() {
        let principal = "arn:aws:iam::123456789012:user/test-user";
        let identity = AuthenticatedIdentity::configured(
            s3_types::AccountIdentity::new(
                "123456789012",
                CanonicalUserId::from_principal("123456789012"),
                "Test account",
            ),
            ConfiguredPrincipalIdentity::new(principal),
        );
        let policy = parse_bucket_policy(&format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/key"}},{{"Effect":"Deny","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/key","Condition":{{"ArnEquals":{{"aws:PrincipalArn":"{principal}"}}}}}}]}}"#,
        ))
        .unwrap();
        let request = PolicyRequest::for_object_with_requester(
            PolicyAction::PutObject,
            "bucket",
            "key",
            PolicyRequester::authenticated(&identity),
            ExistingObjectTags::Unavailable,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn untyped_configured_principal_arn_deny_fails_closed() {
        let principal = "arn:aws:iam::123456789012:user/test-user";
        let identity = AuthenticatedIdentity::configured(
            s3_types::AccountIdentity::new(
                "210987654321",
                CanonicalUserId::from_principal("210987654321"),
                "Test account",
            ),
            ConfiguredPrincipalIdentity::new(principal),
        );
        let policy = parse_bucket_policy(&format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/key"}},{{"Effect":"Deny","Principal":{{"AWS":"{principal}"}},"Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/key","Condition":{{"ArnEquals":{{"aws:PrincipalArn":"{principal}"}}}}}}]}}"#,
        ))
        .unwrap();
        let request = PolicyRequest::for_object_with_requester(
            PolicyAction::PutObject,
            "bucket",
            "key",
            PolicyRequester::authenticated(&identity),
            ExistingObjectTags::Unavailable,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn version_id_condition_matches_version_scoped_object_request() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObjectVersion","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:versionid":"42"}}}]}"#,
        )
        .unwrap();
        let allowed = request(
            PolicyAction::GetObjectVersion,
            "bucket",
            "key",
            Some("caller"),
            &[],
        )
        .with_version_id(Some("42"));
        let denied = request(
            PolicyAction::GetObjectVersion,
            "bucket",
            "key",
            Some("caller"),
            &[],
        )
        .with_version_id(Some("43"));

        assert_eq!(policy.evaluate(&allowed), PolicyEvaluation::ExplicitAllow);
        assert_eq!(policy.evaluate(&denied), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn parse_empty_statement_array_is_rejected() {
        let err = parse_bucket_policy(r#"{"Version":"2012-10-17","Statement":[]}"#).unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::malformed("Could not parse the policy: Statement is empty!")
        );
    }

    #[test]
    fn parse_unsupported_policy_version_uses_aws_message() {
        let err = parse_bucket_policy(
            r#"{"Version":"2026-07-11","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::malformed("The policy must contain a valid version string")
        );
    }

    #[test]
    fn wildcard_question_mark_matches_exactly_one_character() {
        assert!(wildcard_matches("question-?/", "question-a/"));
        assert!(wildcard_matches("question-?/", "question-*/"));
        assert!(!wildcard_matches("question-?/", "question-ab/"));
        assert!(!wildcard_matches("question-?/", "question-/"));
    }

    #[test]
    fn policy_variable_special_forms_escape_wildcard_characters() {
        let request = bucket_request(PolicyAction::ListBucket, "bucket", None);
        let literal_star =
            expand_policy_template("star-${*}", &request).expect("special form expands");
        let literal_question =
            expand_policy_template("question-${?}", &request).expect("special form expands");
        let literal_dollar =
            expand_policy_template("dollar-${$}", &request).expect("special form expands");

        assert!(policy_value_wildcard_matches(&literal_star, "star-*"));
        assert!(!policy_value_wildcard_matches(&literal_star, "star-public"));
        assert!(policy_value_wildcard_matches(
            &literal_question,
            "question-?"
        ));
        assert!(!policy_value_wildcard_matches(
            &literal_question,
            "question-a"
        ));
        assert!(policy_string_equals(&literal_dollar, "dollar-$"));
        assert!(!policy_string_equals(&literal_dollar, "dollar-x"));
    }

    #[test]
    fn literal_private_use_unicode_is_not_an_escape_marker() {
        let literal_star_marker = PolicyValue::literal("star-\u{E000}");
        let literal_question_marker = PolicyValue::literal("question-\u{E001}");

        assert!(policy_value_wildcard_matches(
            &literal_star_marker,
            "star-\u{E000}"
        ));
        assert!(!policy_value_wildcard_matches(
            &literal_star_marker,
            "star-*"
        ));
        assert!(policy_value_wildcard_matches(
            &literal_question_marker,
            "question-\u{E001}"
        ));
        assert!(!policy_value_wildcard_matches(
            &literal_question_marker,
            "question-?"
        ));
    }

    #[test]
    fn policy_template_unclosed_variable_is_literal_text() {
        let request = bucket_request(PolicyAction::ListBucket, "bucket", None);
        let expanded = expand_policy_template("prefix-${aws:userid}-${unterminated", &request)
            .expect("anonymous userid resolves");

        assert_eq!(expanded.as_str(), "prefix-anonymous-${unterminated");
    }

    #[test]
    fn policy_variables_expand_only_for_2012_version() {
        let expanding = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:prefix":"${AWS:UserId}"}}}]}"#,
        )
        .unwrap();
        let literal_2008 = parse_bucket_policy(
            r#"{"Version":"2008-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:prefix":"${aws:userid}"}}}]}"#,
        )
        .unwrap();
        let literal_default = parse_bucket_policy(
            r#"{"Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:prefix":"${aws:userid}"}}}]}"#,
        )
        .unwrap();
        let expanded_request =
            bucket_request(PolicyAction::ListBucket, "bucket", None).with_prefix(Some("anonymous"));
        let literal_request = bucket_request(PolicyAction::ListBucket, "bucket", None)
            .with_prefix(Some("${aws:userid}"));

        assert_eq!(
            expanding.evaluate(&expanded_request),
            PolicyEvaluation::ExplicitAllow
        );
        assert_eq!(
            expanding.evaluate(&literal_request),
            PolicyEvaluation::NoMatch
        );
        assert_eq!(
            literal_2008.evaluate(&expanded_request),
            PolicyEvaluation::NoMatch
        );
        assert_eq!(
            literal_2008.evaluate(&literal_request),
            PolicyEvaluation::ExplicitAllow
        );
        assert_eq!(
            literal_default.evaluate(&expanded_request),
            PolicyEvaluation::NoMatch
        );
        assert_eq!(
            literal_default.evaluate(&literal_request),
            PolicyEvaluation::ExplicitAllow
        );
    }

    #[test]
    fn policy_variables_expand_in_resource_patterns() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/${aws:userid}/*"}]}"#,
        )
        .unwrap();
        let allowed = request(
            PolicyAction::GetObject,
            "bucket",
            "anonymous/object",
            None,
            &[],
        );
        let denied = request(
            PolicyAction::GetObject,
            "bucket",
            "private/object",
            None,
            &[],
        );

        assert_eq!(policy.evaluate(&allowed), PolicyEvaluation::ExplicitAllow);
        assert_eq!(policy.evaluate(&denied), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn authenticated_identity_policy_variables_are_unavailable_until_iam_context_exists() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:prefix":"${aws:userid}"}}}]}"#,
        )
        .unwrap();
        let guessed_request = bucket_request(
            PolicyAction::ListBucket,
            "bucket",
            Some("arn:aws:iam::123456789012:user/alice"),
        )
        .with_prefix(Some("arn:aws:iam::123456789012:user/alice"));

        assert_eq!(policy.evaluate(&guessed_request), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn authenticated_identity_policy_variable_defaults_do_not_apply_to_unavailable_context() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:prefix":"${aws:userid, 'fallback'}"}}}]}"#,
        )
        .unwrap();
        let fallback_request = bucket_request(
            PolicyAction::ListBucket,
            "bucket",
            Some("arn:aws:iam::123456789012:user/alice"),
        )
        .with_prefix(Some("fallback"));

        assert_eq!(
            policy.evaluate(&fallback_request),
            PolicyEvaluation::NoMatch
        );
    }

    #[test]
    fn multivalued_policy_variable_defaults_do_not_apply_to_present_context() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/${s3:RequestObjectTagKeys, 'fallback'}"}]}"#,
        )
        .unwrap();
        let request_tags = [
            PolicyTag {
                key: "public",
                value: "1",
            },
            PolicyTag {
                key: "shared",
                value: "2",
            },
        ];
        let fallback_request = request(
            PolicyAction::PutObject,
            "bucket",
            "fallback",
            Some("arn:aws:iam::123456789012:user/alice"),
            &[],
        )
        .with_request_object_tags(&request_tags);

        assert_eq!(
            policy.evaluate(&fallback_request),
            PolicyEvaluation::NoMatch
        );
    }

    #[test]
    fn normalized_json_canonicalizes_bucket_policy_shape() {
        let policy = parse_bucket_policy(
            "{\n  \"Statement\": {\n    \"Resource\": [\"arn:aws:s3:::bucket\"],\n    \"Action\": [\"s3:ListBucket\"],\n    \"Principal\": {\"AWS\": \"arn:aws:iam::123456789012:root\"},\n    \"Effect\": \"Allow\",\n    \"Sid\": \"One\"\n  },\n  \"Version\": \"2012-10-17\"\n}",
        )
        .unwrap();
        assert_eq!(
            policy.normalized_json(),
            r#"{"Version":"2012-10-17","Statement":[{"Sid":"One","Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}"#
        );
    }

    #[test]
    fn normalized_json_groups_conditions_by_operator() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/classification":["public","shared"],"s3:ExistingObjectTag/region":"us-east-1"},"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}]}"#,
        )
        .unwrap();
        assert_eq!(
            policy.normalized_json(),
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"},"StringEquals":{"s3:ExistingObjectTag/classification":["public","shared"],"s3:ExistingObjectTag/region":"us-east-1"}}}]}"#
        );
    }

    #[test]
    fn wildcard_allow_is_public() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();
        assert!(policy.is_public());
    }

    #[test]
    fn fixed_aws_principal_is_not_public() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();
        assert!(!policy.is_public());
    }

    #[test]
    fn fixed_principal_arn_condition_constrains_wildcard_principal() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"ArnEquals":{"aws:PrincipalArn":"arn:aws:iam::123456789012:role/test"}}}]}"#,
        )
        .unwrap();
        assert!(!policy.is_public());
    }

    #[test]
    fn unpinned_principal_arn_operator_does_not_constrain_wildcard_principal() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"aws:PrincipalArn":"arn:aws:iam::123456789012:role/test"}}}]}"#,
        )
        .unwrap();
        assert!(policy.is_public());
        assert!(policy.validate_evaluable_object_conditions().is_err());
    }

    #[test]
    fn fixed_source_vpce_constrains_wildcard_principal() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"aws:SourceVpce":"vpce-12345678"}}}]}"#,
        )
        .unwrap();
        assert!(!policy.is_public());
    }

    #[test]
    fn broad_ipv4_source_ip_cidr_is_public() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"IpAddress":{"aws:SourceIp":"0.0.0.0/0"}}}]}"#,
        )
        .unwrap();
        assert!(policy.is_public());
    }

    #[test]
    fn broad_ipv6_source_ip_cidr_is_public() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"IpAddress":{"aws:SourceIp":"::/0"}}}]}"#,
        )
        .unwrap();
        assert!(policy.is_public());
    }

    #[test]
    fn broad_ipv6_ula_source_ip_cidr_is_public() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"IpAddress":{"aws:SourceIp":"fd00::/8"}}}]}"#,
        )
        .unwrap();
        assert!(policy.is_public());
    }

    #[test]
    fn narrow_private_ipv4_source_ip_cidr_is_not_public() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}]}"#,
        )
        .unwrap();
        assert!(!policy.is_public());
    }

    #[test]
    fn mixed_case_source_ip_cidr_constrains_wildcard_principal() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"IpAddress":{"AWS:SourceIP":"10.0.0.0/8"}}}]}"#,
        )
        .unwrap();
        assert!(!policy.is_public());
    }

    #[test]
    fn for_any_value_source_ip_cidr_constrains_wildcard_principal() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"ForAnyValue:IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}]}"#,
        )
        .unwrap();
        assert!(!policy.is_public());
    }

    #[test]
    fn if_exists_source_ip_cidr_does_not_constrain_wildcard_principal() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"IpAddressIfExists":{"aws:SourceIp":"10.0.0.0/8"}}}]}"#,
        )
        .unwrap();
        assert!(policy.is_public());
    }

    #[test]
    fn for_all_values_source_ip_cidr_does_not_constrain_wildcard_principal() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"ForAllValues:IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}]}"#,
        )
        .unwrap();
        assert!(policy.is_public());
    }

    #[test]
    fn ipv4_source_ip_cidr_broader_than_slash_8_is_public() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"IpAddress":{"aws:SourceIp":"11.0.0.0/7"}}}]}"#,
        )
        .unwrap();
        assert!(policy.is_public());
    }

    #[test]
    fn string_not_equals_does_not_constrain_wildcard_principal() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"aws:SourceVpc":"vpc-12345678"}}}]}"#,
        )
        .unwrap();
        assert!(policy.is_public());
    }

    #[test]
    fn deny_statement_is_not_public() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();
        assert!(!policy.is_public());
    }

    #[test]
    fn invalid_json_is_rejected() {
        let err = parse_bucket_policy("{").unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::malformed(
                "Policies must be valid JSON and the first byte must be '{'"
            )
        );
        let err = parse_bucket_policy("[]").unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::malformed(
                "Policies must be valid JSON and the first byte must be '{'"
            )
        );
    }

    #[test]
    fn missing_statement_is_rejected() {
        let err = parse_bucket_policy(r#"{"Version":"2012-10-17"}"#).unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::malformed("Missing required field Statement")
        );
    }

    #[test]
    fn not_principal_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","NotPrincipal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::malformed(
                "NotPrincipal, NotAction, and NotResource are not supported"
            )
        );
    }

    #[test]
    fn invalid_create_bucket_action_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:CreateBucket","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::Malformed {
                reason: "Policy has invalid action",
                detail: Some("s3:CreateBucket".to_string()),
            }
        );
    }

    #[test]
    fn abort_multipart_upload_action_is_accepted() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:AbortMultipartUpload","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();

        let request = PolicyRequest::for_object(
            PolicyAction::AbortMultipartUpload,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn existing_object_tag_condition_matches_matching_tag() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let tags = [PolicyTag::new("security", "public")];
        let request = request(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("444455556666"),
            &tags,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn existing_object_tag_condition_rejects_missing_or_mismatched_tag() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let missing_tags: [PolicyTag<'_>; 0] = [];
        let missing_request = request(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("caller"),
            &missing_tags,
        );
        let mismatched_tags = [PolicyTag::new("security", "private")];
        let mismatched_request = request(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("caller"),
            &mismatched_tags,
        );

        assert_eq!(policy.evaluate(&missing_request), PolicyEvaluation::NoMatch);
        assert_eq!(
            policy.evaluate(&mismatched_request),
            PolicyEvaluation::NoMatch
        );
    }

    #[test]
    fn existing_object_tag_string_equals_if_exists_matches_missing_tag() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEqualsIfExists":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let missing_tags: [PolicyTag<'_>; 0] = [];
        let request = request(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("caller"),
            &missing_tags,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
        assert_eq!(policy.validate_evaluable_object_conditions(), Ok(()));
    }

    #[test]
    fn explicit_deny_beats_allow() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"},{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/private/*"}]}"#,
        )
        .unwrap();
        let tags: [PolicyTag<'_>; 0] = [];
        let allowed_request = request(
            PolicyAction::GetObject,
            "bucket",
            "public/key",
            Some("caller"),
            &tags,
        );
        let denied_request = request(
            PolicyAction::GetObject,
            "bucket",
            "private/key",
            Some("caller"),
            &tags,
        );

        assert_eq!(
            policy.evaluate(&allowed_request),
            PolicyEvaluation::ExplicitAllow
        );
        assert_eq!(
            policy.evaluate(&denied_request),
            PolicyEvaluation::ExplicitDeny
        );
    }

    #[test]
    fn version_requests_require_version_action() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();
        let tags: [PolicyTag<'_>; 0] = [];
        let request = request(
            PolicyAction::GetObjectVersion,
            "bucket",
            "key",
            Some("caller"),
            &tags,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn root_account_principal_matches_account_id_requester() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::444455556666:root"},"Action":"s3:GetObjectTagging","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();
        let tags: [PolicyTag<'_>; 0] = [];
        let request = request(
            PolicyAction::GetObjectTagging,
            "bucket",
            "key",
            Some("444455556666"),
            &tags,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn requires_existing_object_tags_for_matching_action() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}},{"Effect":"Allow","Principal":"*","Action":"s3:GetObjectTagging","Resource":"arn:aws:s3:::bucket/*"},{"Effect":"Allow","Principal":"*","Action":"s3:GetObjectAttributes","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert!(policy.requires_existing_object_tags_for_action(PolicyAction::GetObject));
        assert!(!policy.requires_existing_object_tags_for_action(PolicyAction::GetObjectTagging));
        assert!(!policy.requires_existing_object_tags_for_action(PolicyAction::GetObjectAttributes));
    }

    #[test]
    fn existing_object_tag_condition_unavailable_input_is_distinct_from_missing_tag() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let unavailable_request = PolicyRequest::for_object(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        );
        let available_empty_tags: [PolicyTag<'_>; 0] = [];
        let available_request = request(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("caller"),
            &available_empty_tags,
        );

        assert_eq!(
            policy.statements[0].condition_match_result(&unavailable_request, false),
            ConditionMatchResult::InputUnavailable
        );
        assert_eq!(
            policy.statements[0].condition_match_result(&available_request, false),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn existing_object_tag_condition_is_not_evaluable_for_get_object_attributes() {
        let allow_policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObjectAttributes","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let deny_policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObjectAttributes","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let tags = [PolicyTag::new("security", "public")];
        let attrs_request = request(
            PolicyAction::GetObjectAttributes,
            "bucket",
            "key",
            Some("caller"),
            &tags,
        );
        let object_request = request(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("caller"),
            &tags,
        );
        let object_policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            allow_policy.evaluate(&attrs_request),
            PolicyEvaluation::NoMatch
        );
        assert_eq!(
            deny_policy.evaluate(&attrs_request),
            PolicyEvaluation::NoMatch
        );
        assert_eq!(
            object_policy.evaluate(&object_request),
            PolicyEvaluation::ExplicitAllow
        );
    }

    #[test]
    fn mixed_delete_and_delete_tagging_existing_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:DeleteObject","s3:DeleteObjectTagging"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn put_object_existing_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_and_put_object_existing_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn requires_request_object_tags_for_matching_action() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}},{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();

        assert!(policy.requires_request_object_tags_for_action(PolicyAction::PutObject));
        assert!(!policy.requires_request_object_tags_for_action(PolicyAction::GetObject));
        assert!(!policy.requires_request_object_tags_for_action(PolicyAction::PutObjectTagging));

        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObjectTagging","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert!(policy.requires_request_object_tags_for_action(PolicyAction::PutObjectTagging));
    }

    #[test]
    fn request_object_tag_condition_matches() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let request_tags = [PolicyTag::new("security", "public")];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&request_tags);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn request_object_tag_condition_key_is_case_insensitive_and_matches_any_same_key_case_value() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"S3:RequestObjectTag/Classification":"public"}}}]}"#,
        )
        .unwrap();
        let request_tags = [PolicyTag::new("classification", "public")];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&request_tags);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

        let request_tags = [
            PolicyTag::new("Classification", "private"),
            PolicyTag::new("classification", "public"),
        ];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&request_tags);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

        let request_tags = [
            PolicyTag::new("classification", "public"),
            PolicyTag::new("Classification", "private"),
        ];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&request_tags);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn request_object_tag_case_equivalent_values_preserve_forall_forany_semantics() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/all-*","Condition":{"ForAllValues:StringEquals":{"S3:RequestObjectTag/Classification":"public"}}},{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/any-*","Condition":{"ForAnyValue:StringEquals":{"S3:RequestObjectTag/Classification":"public"}}}]}"#,
        )
        .unwrap();

        let all_match_tags = [
            PolicyTag::new("Classification", "public"),
            PolicyTag::new("classification", "public"),
        ];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "all-match",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&all_match_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

        let one_mismatch_tags = [
            PolicyTag::new("Classification", "public"),
            PolicyTag::new("classification", "private"),
        ];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "all-mismatch",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&one_mismatch_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);

        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "any-match",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&one_mismatch_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

        let no_match_tags = [
            PolicyTag::new("Classification", "private"),
            PolicyTag::new("classification", "internal"),
        ];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "any-mismatch",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&no_match_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn request_object_tag_case_equivalent_values_preserve_binary_equals_semantics() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/binary-*","Condition":{"BinaryEquals":{"S3:RequestObjectTag/Classification":"cHVibGlj"}}},{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/all-*","Condition":{"ForAllValues:BinaryEquals":{"S3:RequestObjectTag/Classification":"cHVibGlj"}}},{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/any-*","Condition":{"ForAnyValue:BinaryEquals":{"S3:RequestObjectTag/Classification":"cHVibGlj"}}}]}"#,
        )
        .unwrap();

        let one_match_tags = [
            PolicyTag::new("Classification", "cHJpdmF0ZQ=="),
            PolicyTag::new("classification", "cHVibGlj"),
        ];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "binary-match",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&one_match_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

        let no_match_tags = [
            PolicyTag::new("Classification", "cHJpdmF0ZQ=="),
            PolicyTag::new("classification", "aW50ZXJuYWw="),
        ];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "binary-mismatch",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&no_match_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);

        let all_match_tags = [
            PolicyTag::new("Classification", "cHVibGlj"),
            PolicyTag::new("classification", "cHVibGlj"),
        ];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "all-match",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&all_match_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "all-mismatch",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&one_match_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);

        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "any-match",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&one_match_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "any-mismatch",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&no_match_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn existing_object_tag_condition_key_is_case_insensitive_with_exact_case_precedence() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"S3:ExistingObjectTag/Classification":"public"}}}]}"#,
        )
        .unwrap();
        let existing_tags = [
            PolicyTag::new("Classification", "private"),
            PolicyTag::new("classification", "public"),
        ];
        let request = PolicyRequest::for_object(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Available(&existing_tags),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);

        let existing_tags = [
            PolicyTag::new("Classification", "public"),
            PolicyTag::new("classification", "private"),
        ];
        let request = PolicyRequest::for_object(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Available(&existing_tags),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn request_object_tag_binary_equals_condition_matches_decoded_base64() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"BinaryEquals":{"s3:RequestObjectTag/security":"cHVibGlj"}}}]}"#,
        )
        .unwrap();
        let matching_tags = [PolicyTag::new("security", "cHVibGlj")];
        let raw_matching_text_tags = [PolicyTag::new("security", "public")];
        let nonmatching_tags = [PolicyTag::new("security", "cHJpdmF0ZQ==")];
        let wildcard_text_tags = [PolicyTag::new("security", "Kg==")];

        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&matching_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&raw_matching_text_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);

        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&nonmatching_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);

        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&wildcard_text_tags);
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn invalid_binary_equals_policy_operand_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"BinaryEquals":{"s3:RequestObjectTag/security":"not-base64"}}}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err,
            BucketPolicyError::malformed("Invalid Base64 value for binary condition")
        );
    }

    #[test]
    fn request_object_tag_ignore_case_condition_matches() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEqualsIgnoreCase":{"s3:RequestObjectTag/security":"public"}}},{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEqualsIgnoreCase":{"s3:RequestObjectTag/classification":"internal"}}}]}"#,
        )
        .unwrap();
        let request_tags = [
            PolicyTag::new("security", "PUBLIC"),
            PolicyTag::new("classification", "Internal"),
        ];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&request_tags);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

        let request_tags = [
            PolicyTag::new("security", "PUBLIC"),
            PolicyTag::new("classification", "external"),
        ];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&request_tags);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn request_object_tag_null_condition_matches_missing_tag() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"Null":{"s3:RequestObjectTag/security":"true"}}}]}"#,
        )
        .unwrap();
        let no_request_tags: [PolicyTag<'_>; 0] = [];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&no_request_tags);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn request_object_tag_unavailable_input_is_distinct_from_empty_request() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"Null":{"s3:RequestObjectTag/security":"true"}}},{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"Null":{"s3:RequestObjectTagKeys":"true"}}}]}"#,
        )
        .unwrap();
        let unavailable_request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        );
        let no_request_tags: [PolicyTag<'_>; 0] = [];
        let empty_request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&no_request_tags);

        assert_eq!(
            policy.statements[0].condition_match_result(&unavailable_request, false),
            ConditionMatchResult::InputUnavailable
        );
        assert_eq!(
            policy.statements[1].condition_match_result(&unavailable_request, false),
            ConditionMatchResult::InputUnavailable
        );
        assert_eq!(
            policy.statements[0].condition_match_result(&empty_request, false),
            ConditionMatchResult::Matches
        );
        assert_eq!(
            policy.statements[1].condition_match_result(&empty_request, false),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn request_object_tag_string_not_equals_matches_mismatched_tag() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let request_tags = [PolicyTag::new("security", "private")];
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_request_object_tags(&request_tags);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn put_object_acl_request_object_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObjectAcl","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn put_object_retention_request_object_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObjectRetention","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn put_object_legal_hold_request_object_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObjectLegalHold","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_put_object_acl_and_tagging_request_object_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:PutObjectAcl","s3:PutObjectTagging"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:RequestObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_and_put_object_copy_source_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringLike":{"s3:x-amz-copy-source":"src/public/*"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_and_put_object_metadata_directive_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-metadata-directive":"COPY"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_and_put_object_sse_s3_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"Null":{"s3:x-amz-server-side-encryption":"true"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_and_put_object_sse_c_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"Null":{"s3:x-amz-server-side-encryption-customer-algorithm":"true"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_and_put_object_canned_acl_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-acl":"private"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_and_put_object_grant_read_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-grant-read":"uri=http://acs.amazonaws.com/groups/global/AllUsers"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_version_and_put_object_copy_source_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObjectVersion","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringLike":{"s3:x-amz-copy-source":"src/public/*"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_version_and_put_object_canned_acl_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObjectVersion","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-acl":"private"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_version_and_put_object_sse_s3_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObjectVersion","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"Null":{"s3:x-amz-server-side-encryption":"true"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_version_and_put_object_metadata_directive_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObjectVersion","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-metadata-directive":"COPY"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_version_and_put_object_sse_c_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObjectVersion","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"Null":{"s3:x-amz-server-side-encryption-customer-algorithm":"true"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_and_put_object_grant_read_acp_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-grant-read-acp":"uri=http://acs.amazonaws.com/groups/global/AllUsers"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_and_put_object_grant_full_control_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-grant-full-control":"uri=http://acs.amazonaws.com/groups/global/AllUsers"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_version_and_put_object_grant_read_acp_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObjectVersion","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-grant-read-acp":"uri=http://acs.amazonaws.com/groups/global/AllUsers"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn mixed_get_object_version_and_put_object_grant_full_control_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":["s3:GetObjectVersion","s3:PutObject"],"Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-grant-full-control":"uri=http://acs.amazonaws.com/groups/global/AllUsers"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn grant_full_control_condition_matches() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-grant-full-control":"id=owner-canonical-id"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_grant_full_control(Some("id=owner-canonical-id"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn grant_full_control_null_condition_matches_missing_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"Null":{"s3:x-amz-grant-full-control":"true"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_grant_full_control(None);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn grant_full_control_string_not_equals_matches_mismatched_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"s3:x-amz-grant-full-control":"id=owner-canonical-id"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_grant_full_control(Some("id=someone-else"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn sse_customer_algorithm_condition_matches() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-server-side-encryption-customer-algorithm":"AES256"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_sse_customer_algorithm(Some("AES256"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn sse_customer_algorithm_null_condition_matches_missing_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"Null":{"s3:x-amz-server-side-encryption-customer-algorithm":"true"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_sse_customer_algorithm(None);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn sse_customer_algorithm_string_not_equals_matches_mismatched_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"s3:x-amz-server-side-encryption-customer-algorithm":"AES256"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_sse_customer_algorithm(Some("AES192"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn server_side_encryption_condition_matches() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-server-side-encryption":"AES256"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_server_side_encryption(Some("AES256"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn server_side_encryption_null_condition_matches_missing_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"Null":{"s3:x-amz-server-side-encryption":"true"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_server_side_encryption(None);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn request_field_unavailable_input_is_distinct_from_missing_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"Null":{"s3:x-amz-server-side-encryption":"true"}}}]}"#,
        )
        .unwrap();
        let unavailable_request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        );
        let missing_header_request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_server_side_encryption(None);

        assert_eq!(
            policy.statements[0].condition_match_result(&unavailable_request, false),
            ConditionMatchResult::InputUnavailable
        );
        assert_eq!(
            policy.statements[0].condition_match_result(&missing_header_request, false),
            ConditionMatchResult::Matches
        );
    }

    #[test]
    fn object_creation_operation_unavailable_input_is_distinct_from_absent_value() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"Bool":{"s3:ObjectCreationOperation":"true"}}}]}"#,
        )
        .unwrap();
        let unavailable_request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        );
        let absent_request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_object_creation_operation(None);

        assert_eq!(
            policy.statements[0].condition_match_result(&unavailable_request, false),
            ConditionMatchResult::InputUnavailable
        );
        assert_eq!(
            policy.statements[0].condition_match_result(&absent_request, false),
            ConditionMatchResult::NoMatch
        );
    }

    #[test]
    fn server_side_encryption_string_not_equals_matches_mismatched_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"s3:x-amz-server-side-encryption":"AES256"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_server_side_encryption(Some("aws:kms"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn evaluable_object_conditions_reject_unsupported_condition_clause() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn principal_arn_condition_is_accepted_for_evaluable_object_actions() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"ArnEquals":{"aws:PrincipalArn":"arn:aws:iam::444455556666:user/other"}}}]}"#,
        )
        .unwrap();

        assert_eq!(policy.validate_evaluable_object_conditions(), Ok(()));
    }

    #[test]
    fn source_ip_condition_is_accepted_for_policy_status_classification() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"IpAddress":{"aws:SourceIp":"fd00::/8"}}}]}"#,
        )
        .unwrap();

        assert_eq!(policy.validate_evaluable_object_conditions(), Ok(()));
    }

    #[test]
    fn malformed_source_ip_condition_operand_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"IpAddress":{"aws:SourceIp":"not-an-ip"}}}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err,
            BucketPolicyError::malformed("Invalid IP address in Conditions")
        );
    }

    #[test]
    fn source_ip_condition_evaluates_request_source_ip() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.0/8"}}},{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"NotIpAddress":{"aws:SourceIp":"127.0.0.0/8"}}}]}"#,
        )
        .unwrap();
        let local_request = request(PolicyAction::GetObject, "bucket", "key", None, &[])
            .with_source_ip(Some("127.0.0.1".parse().unwrap()));
        let remote_request = request(PolicyAction::GetObject, "bucket", "key", None, &[])
            .with_source_ip(Some("10.0.0.1".parse().unwrap()));
        let missing_source_request = request(PolicyAction::GetObject, "bucket", "key", None, &[]);

        assert_eq!(
            policy.evaluate(&local_request),
            PolicyEvaluation::ExplicitAllow
        );
        assert_eq!(
            policy.evaluate(&remote_request),
            PolicyEvaluation::ExplicitDeny
        );
        assert_eq!(
            policy.evaluate(&missing_source_request),
            PolicyEvaluation::NoMatch
        );
        assert!(policy.requires_source_ip_for_action(PolicyAction::GetObject));
    }

    #[test]
    fn current_time_condition_evaluates_request_epoch_time() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/date","Condition":{"DateEquals":{"aws:CurrentTime":"2024-01-01T00:00:00Z"}}},{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/epoch","Condition":{"NumericEquals":{"aws:EpochTime":1704067200}}}]}"#,
        )
        .unwrap();
        let date_request = request(PolicyAction::GetObject, "bucket", "date", None, &[])
            .with_current_time_epoch_seconds(Some(1_704_067_200));
        let epoch_request = request(PolicyAction::GetObject, "bucket", "epoch", None, &[])
            .with_current_time_epoch_seconds(Some(1_704_067_200));
        let missing_time_request = request(PolicyAction::GetObject, "bucket", "date", None, &[]);

        assert_eq!(
            policy.evaluate(&date_request),
            PolicyEvaluation::ExplicitAllow
        );
        assert_eq!(
            policy.evaluate(&epoch_request),
            PolicyEvaluation::ExplicitAllow
        );
        assert_eq!(
            policy.evaluate(&missing_time_request),
            PolicyEvaluation::NoMatch
        );
        assert!(policy.requires_current_time_for_action(PolicyAction::GetObject));
    }

    #[test]
    fn source_vpc_condition_is_rejected_for_evaluable_object_actions() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringNotEquals":{"aws:SourceVpc":"vpc-12345678"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn resource_account_condition_is_rejected_as_known_deferred_key() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ResourceAccount":"123456789012"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn global_condition_examples_are_rejected_as_known_deferred_keys() {
        for (operator, key, value) in [
            ("StringEquals", "aws:PrincipalAccount", "123456789012"),
            ("Bool", "aws:PrincipalIsAWSService", "false"),
            ("IpAddress", "aws:VpcSourceIp", "10.0.0.0/8"),
            ("StringEquals", "aws:ResourceAccount", "123456789012"),
        ] {
            let policy = parse_bucket_policy(&format!(
                "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\
                 \"Principal\":\"*\",\"Action\":\"s3:GetObject\",\
                 \"Resource\":\"arn:aws:s3:::bucket/*\",\
                 \"Condition\":{{\"{operator}\":{{\"{key}\":\"{value}\"}}}}}}]}}"
            ))
            .unwrap();

            assert_eq!(
                policy.validate_evaluable_object_conditions(),
                Err(BucketPolicyError::malformed(
                    "unsupported Condition for currently enforced bucket policy action"
                )),
                "{key} should be recognized but deferred"
            );
        }
    }

    #[test]
    fn access_grants_conditions_are_rejected_as_known_deferred_keys() {
        for key in [
            "s3:AccessGrantScope",
            "s3:AccessGrantsInstanceArn",
            "s3:AccessGrantsLocationScope",
        ] {
            let policy = parse_bucket_policy(&format!(
                "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\
                 \"Principal\":\"*\",\"Action\":\"s3:GetObject\",\
                 \"Resource\":\"arn:aws:s3:::bucket/*\",\
                 \"Condition\":{{\"StringEquals\":{{\"{key}\":\"value\"}}}}}}]}}"
            ))
            .unwrap();

            assert_eq!(
                policy.validate_evaluable_object_conditions(),
                Err(BucketPolicyError::malformed(
                    "unsupported Condition for currently enforced bucket policy action"
                )),
                "{key} should be recognized but deferred"
            );
        }
    }

    #[test]
    fn access_point_conditions_are_rejected_as_known_deferred_keys() {
        for key in [
            "s3:AccessPointNetworkOrigin",
            "s3:AccessPointTag/environment",
        ] {
            let policy = parse_bucket_policy(&format!(
                "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\
                 \"Principal\":\"*\",\"Action\":\"s3:GetObject\",\
                 \"Resource\":\"arn:aws:s3:::bucket/*\",\
                 \"Condition\":{{\"StringEquals\":{{\"{key}\":\"value\"}}}}}}]}}"
            ))
            .unwrap();

            assert_eq!(
                policy.validate_evaluable_object_conditions(),
                Err(BucketPolicyError::malformed(
                    "unsupported Condition for currently enforced bucket policy action"
                )),
                "{key} should be recognized but deferred"
            );
        }
    }

    #[test]
    fn object_lock_conditions_are_rejected_as_known_deferred_keys() {
        for key in [
            "s3:object-lock-mode",
            "s3:object-lock-legal-hold",
            "s3:object-lock-retain-until-date",
            "s3:object-lock-remaining-retention-days",
        ] {
            let policy = parse_bucket_policy(&format!(
                "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\
                 \"Principal\":\"*\",\"Action\":\"s3:GetObject\",\
                 \"Resource\":\"arn:aws:s3:::bucket/*\",\
                 \"Condition\":{{\"StringEquals\":{{\"{key}\":\"value\"}}}}}}]}}"
            ))
            .unwrap();

            assert_eq!(
                policy.validate_evaluable_object_conditions(),
                Err(BucketPolicyError::malformed(
                    "unsupported Condition for currently enforced bucket policy action"
                )),
                "{key} should be recognized but deferred"
            );
        }
    }

    #[test]
    fn annotation_conditions_are_rejected_as_known_deferred_keys() {
        for key in [
            "s3:annotation-prefix",
            "s3:max-annotation-results",
            "s3:x-amz-object-annotation-directive",
        ] {
            let policy = parse_bucket_policy(&format!(
                "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\
                 \"Principal\":\"*\",\"Action\":\"s3:PutObject\",\
                 \"Resource\":\"arn:aws:s3:::bucket/*\",\
                 \"Condition\":{{\"StringEquals\":{{\"{key}\":\"value\"}}}}}}]}}"
            ))
            .unwrap();

            assert_eq!(
                policy.validate_evaluable_object_conditions(),
                Err(BucketPolicyError::malformed(
                    "unsupported Condition for currently enforced bucket policy action"
                )),
                "{key} should be recognized but deferred"
            );
        }
    }

    #[test]
    fn namespace_and_object_if_match_conditions_are_rejected_as_known_deferred_keys() {
        for key in ["s3:x-amz-bucket-namespace", "s3:x-amz-object-if-match"] {
            let policy = parse_bucket_policy(&format!(
                "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\
                 \"Principal\":\"*\",\"Action\":\"s3:PutObject\",\
                 \"Resource\":\"arn:aws:s3:::bucket/*\",\
                 \"Condition\":{{\"StringEquals\":{{\"{key}\":\"value\"}}}}}}]}}"
            ))
            .unwrap();

            assert_eq!(
                policy.validate_evaluable_object_conditions(),
                Err(BucketPolicyError::malformed(
                    "unsupported Condition for currently enforced bucket policy action"
                )),
                "{key} should be recognized but deferred"
            );
        }
    }

    #[test]
    fn unknown_condition_key_is_rejected_with_detail() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"aaaaaaaaaaaaaaaaaaaaé":"value"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed_with_detail(
                "Policy has an invalid condition key",
                "aaaaaaaaaaaaaaaaaaaaé"
            ))
        );
    }

    #[test]
    fn unsupported_bucket_condition_still_denies_conservatively() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetBucketAcl","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"aws:SourceVpce":"vpce-12345678"}}}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::GetBucketAcl, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn mixed_bucket_conditions_preserve_conservative_deny_when_any_clause_is_unsupported() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetBucketAcl","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"aws:SourceVpc":"vpc-12345678","aws:SourceVpce":"vpce-12345678"}}}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::GetBucketAcl, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn copy_source_condition_matches_string_like() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::dst/*","Condition":{"StringLike":{"s3:x-amz-copy-source":"src/public/*"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "dst",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_copy_source(Some("src/public/foo"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn copy_source_condition_string_like_if_exists_matches_missing_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::dst/*","Condition":{"StringLikeIfExists":{"s3:x-amz-copy-source":"src/public/*"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "dst",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_copy_source(None);

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
        assert_eq!(policy.validate_evaluable_object_conditions(), Ok(()));
    }

    #[test]
    fn copy_source_condition_string_like_if_exists_rejects_mismatched_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::dst/*","Condition":{"StringLikeIfExists":{"s3:x-amz-copy-source":"src/public/*"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "dst",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_copy_source(Some("src/private/foo"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn copy_source_condition_string_not_like_matches_mismatched_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::dst/*","Condition":{"StringNotLike":{"s3:x-amz-copy-source":"src/public/*"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "dst",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_copy_source(Some("src/private/foo"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
        assert_eq!(policy.validate_evaluable_object_conditions(), Ok(()));
    }

    #[test]
    fn metadata_directive_condition_requires_explicit_copy_header() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:x-amz-metadata-directive":"COPY"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        );
        let explicit_copy = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_metadata_directive(Some("COPY"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);
        assert_eq!(
            policy.evaluate(&explicit_copy),
            PolicyEvaluation::ExplicitAllow
        );
    }

    #[test]
    fn canned_acl_condition_matches_public_read() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:PutObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringLike":{"s3:x-amz-acl":"public*"}}}]}"#,
        )
        .unwrap();
        let request = PolicyRequest::for_object(
            PolicyAction::PutObject,
            "bucket",
            "key",
            Some("caller"),
            None,
            ExistingObjectTags::Unavailable,
        )
        .with_canned_acl(Some("public-read"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn get_object_acl_existing_tag_condition_matches() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObjectAcl","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let tags = [PolicyTag::new("security", "public")];
        let request = request(
            PolicyAction::GetObjectAcl,
            "bucket",
            "key",
            Some("caller"),
            &tags,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn put_object_acl_existing_tag_condition_matches() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObjectAcl","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let tags = [PolicyTag::new("security", "public")];
        let request = request(
            PolicyAction::PutObjectAcl,
            "bucket",
            "key",
            Some("caller"),
            &tags,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn put_object_version_acl_existing_tag_condition_matches() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObjectVersionAcl","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let tags = [PolicyTag::new("security", "public")];
        let request = request(
            PolicyAction::PutObjectVersionAcl,
            "bucket",
            "key",
            Some("caller"),
            &tags,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn get_object_retention_existing_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObjectRetention","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn get_object_legal_hold_existing_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObjectLegalHold","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn delete_object_existing_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:DeleteObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn delete_object_version_existing_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:DeleteObjectVersion","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn put_object_retention_existing_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObjectRetention","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn put_object_legal_hold_existing_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutObjectLegalHold","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn bypass_governance_retention_existing_tag_condition_is_rejected() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:BypassGovernanceRetention","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]}"#,
        )
        .unwrap();

        assert_eq!(
            policy.validate_evaluable_object_conditions(),
            Err(BucketPolicyError::malformed(
                "unsupported Condition for currently enforced bucket policy action"
            ))
        );
    }

    #[test]
    fn bypass_governance_retention_matches_object_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:BypassGovernanceRetention","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();
        let tags: [PolicyTag<'_>; 0] = [];
        let request = request(
            PolicyAction::BypassGovernanceRetention,
            "bucket",
            "key",
            Some("caller"),
            &tags,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn delete_object_version_matches_object_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:DeleteObjectVersion","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap();
        let tags: [PolicyTag<'_>; 0] = [];
        let request = request(
            PolicyAction::DeleteObjectVersion,
            "bucket",
            "key",
            Some("caller"),
            &tags,
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn bucket_resource_request_matches_bucket_action() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetBucketPublicAccessBlock","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(
            PolicyAction::GetBucketPublicAccessBlock,
            "bucket",
            Some("caller"),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitDeny);
    }

    #[test]
    fn get_bucket_policy_status_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketPolicyStatus","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(
            PolicyAction::GetBucketPolicyStatus,
            "bucket",
            Some("caller"),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn get_bucket_policy_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketPolicy","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::GetBucketPolicy, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn put_bucket_policy_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketPolicy","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::PutBucketPolicy, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn delete_bucket_policy_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:DeleteBucketPolicy","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::DeleteBucketPolicy, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn get_bucket_cors_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketCORS","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::GetBucketCors, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn get_bucket_acl_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketAcl","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::GetBucketAcl, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn get_bucket_location_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketLocation","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::GetBucketLocation, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn get_bucket_versioning_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketVersioning","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::GetBucketVersioning, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn get_bucket_ownership_controls_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketOwnershipControls","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(
            PolicyAction::GetBucketOwnershipControls,
            "bucket",
            Some("caller"),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn get_bucket_tagging_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketTagging","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::GetBucketTagging, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn get_bucket_tagging_matches_bucket_tag_condition() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketTagging","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let public = [PolicyTag::new("security", "public")];
        let private = [PolicyTag::new("security", "private")];

        let request = bucket_request_with_tags(
            PolicyAction::GetBucketTagging,
            "bucket",
            Some("caller"),
            &public,
        );
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

        let request = bucket_request_with_tags(
            PolicyAction::GetBucketTagging,
            "bucket",
            Some("caller"),
            &private,
        );
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn get_bucket_tagging_matches_resource_tag_condition() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketTagging","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"aws:ResourceTag/security":"public"}}}]}"#,
        )
        .unwrap();
        assert!(policy.requires_bucket_tags_for_action(PolicyAction::GetBucketTagging));
        assert!(policy.references_bucket_tag_conditions());
        let public = [PolicyTag::new("security", "public")];
        let private = [PolicyTag::new("security", "private")];

        let request = bucket_request_with_tags(
            PolicyAction::GetBucketTagging,
            "bucket",
            Some("caller"),
            &public,
        );
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

        let request = bucket_request_with_tags(
            PolicyAction::GetBucketTagging,
            "bucket",
            Some("caller"),
            &private,
        );
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn bucket_tag_condition_keys_are_case_insensitive_with_exact_case_precedence() {
        for (condition_key, resource_tag_lowercase_precedence) in [
            ("S3:BucketTag/Classification", false),
            ("AWS:ResourceTag/Classification", true),
        ] {
            let policy = parse_bucket_policy(&format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketTagging","Resource":"arn:aws:s3:::bucket","Condition":{{"StringEquals":{{"{condition_key}":"public"}}}}}}]}}"#
            ))
            .unwrap();

            let fallback_tags = [PolicyTag::new("classification", "public")];
            let request = bucket_request_with_tags(
                PolicyAction::GetBucketTagging,
                "bucket",
                Some("caller"),
                &fallback_tags,
            );
            assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

            let denied_tags = [
                PolicyTag::new("Classification", "private"),
                PolicyTag::new("classification", "public"),
            ];
            let request = bucket_request_with_tags(
                PolicyAction::GetBucketTagging,
                "bucket",
                Some("caller"),
                &denied_tags,
            );
            assert_eq!(
                policy.evaluate(&request),
                if resource_tag_lowercase_precedence {
                    PolicyEvaluation::ExplicitAllow
                } else {
                    PolicyEvaluation::NoMatch
                }
            );

            let allowed_tags = [
                PolicyTag::new("Classification", "public"),
                PolicyTag::new("classification", "private"),
            ];
            let request = bucket_request_with_tags(
                PolicyAction::GetBucketTagging,
                "bucket",
                Some("caller"),
                &allowed_tags,
            );
            assert_eq!(
                policy.evaluate(&request),
                if resource_tag_lowercase_precedence {
                    PolicyEvaluation::NoMatch
                } else {
                    PolicyEvaluation::ExplicitAllow
                }
            );

            let reversed_allowed_tags = [
                PolicyTag::new("classification", "private"),
                PolicyTag::new("Classification", "public"),
            ];
            let request = bucket_request_with_tags(
                PolicyAction::GetBucketTagging,
                "bucket",
                Some("caller"),
                &reversed_allowed_tags,
            );
            assert_eq!(
                policy.evaluate(&request),
                if resource_tag_lowercase_precedence {
                    PolicyEvaluation::NoMatch
                } else {
                    PolicyEvaluation::ExplicitAllow
                }
            );

            let reversed_denied_tags = [
                PolicyTag::new("classification", "public"),
                PolicyTag::new("Classification", "private"),
            ];
            let request = bucket_request_with_tags(
                PolicyAction::GetBucketTagging,
                "bucket",
                Some("caller"),
                &reversed_denied_tags,
            );
            assert_eq!(
                policy.evaluate(&request),
                if resource_tag_lowercase_precedence {
                    PolicyEvaluation::ExplicitAllow
                } else {
                    PolicyEvaluation::NoMatch
                }
            );

            let no_match_tags = [
                PolicyTag::new("Classification", "private"),
                PolicyTag::new("classification", "internal"),
            ];
            let request = bucket_request_with_tags(
                PolicyAction::GetBucketTagging,
                "bucket",
                Some("caller"),
                &no_match_tags,
            );
            assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);
        }
    }

    #[test]
    fn get_bucket_tagging_bucket_tag_condition_no_match_when_bucket_tags_unavailable() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketTagging","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::GetBucketTagging, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn delete_bucket_matches_bucket_tag_condition() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:DeleteBucket","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let public = [PolicyTag::new("security", "public")];
        let private = [PolicyTag::new("security", "private")];

        let request = bucket_request_with_tags(
            PolicyAction::DeleteBucket,
            "bucket",
            Some("caller"),
            &public,
        );
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

        let request = bucket_request_with_tags(
            PolicyAction::DeleteBucket,
            "bucket",
            Some("caller"),
            &private,
        );
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn get_object_matches_bucket_tag_condition() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let public = [PolicyTag::new("security", "public")];
        let private = [PolicyTag::new("security", "private")];

        let request = request_with_bucket_tags(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("caller"),
            &[],
            &public,
        );
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);

        let request = request_with_bucket_tags(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("caller"),
            &[],
            &private,
        );
        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn get_object_bucket_tag_condition_no_match_when_bucket_tags_unavailable() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:BucketTag/security":"public"}}}]}"#,
        )
        .unwrap();
        let request = request(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("caller"),
            &[],
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn get_object_bucket_tag_deny_condition_is_nonoperative_when_bucket_tags_unavailable() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"},{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"s3:BucketTag/security":"private"}}}]}"#,
        )
        .unwrap();
        let request = request(
            PolicyAction::GetObject,
            "bucket",
            "key",
            Some("caller"),
            &[],
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn bucket_tag_condition_is_supported_for_pinned_object_actions() {
        for action in [
            "s3:GetObjectAcl",
            "s3:GetObjectVersionAcl",
            "s3:GetObjectTagging",
            "s3:GetObjectVersionTagging",
            "s3:GetObjectRetention",
            "s3:GetObjectLegalHold",
            "s3:PutObjectAcl",
            "s3:PutObjectVersionAcl",
            "s3:PutObjectTagging",
            "s3:PutObjectVersionTagging",
            "s3:PutObjectRetention",
            "s3:PutObjectLegalHold",
            "s3:BypassGovernanceRetention",
            "s3:DeleteObject",
            "s3:DeleteObjectVersion",
            "s3:DeleteObjectTagging",
            "s3:DeleteObjectVersionTagging",
        ] {
            let policy = parse_bucket_policy(&format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":"*","Action":"{action}","Resource":"arn:aws:s3:::bucket/*","Condition":{{"StringEquals":{{"s3:BucketTag/security":"public"}}}}}}]}}"#
            ))
            .unwrap();

            assert_eq!(
                policy.validate_evaluable_object_conditions(),
                Ok(()),
                "{action}"
            );
        }
    }

    #[test]
    fn get_encryption_configuration_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetEncryptionConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(
            PolicyAction::GetEncryptionConfiguration,
            "bucket",
            Some("caller"),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn get_lifecycle_configuration_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetLifecycleConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(
            PolicyAction::GetLifecycleConfiguration,
            "bucket",
            Some("caller"),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn get_bucket_object_lock_configuration_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketObjectLockConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(
            PolicyAction::GetBucketObjectLockConfiguration,
            "bucket",
            Some("caller"),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn put_bucket_cors_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketCORS","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::PutBucketCors, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn put_bucket_acl_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketAcl","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::PutBucketAcl, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn put_bucket_versioning_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketVersioning","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::PutBucketVersioning, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn put_bucket_ownership_controls_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketOwnershipControls","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(
            PolicyAction::PutBucketOwnershipControls,
            "bucket",
            Some("caller"),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn put_bucket_tagging_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketTagging","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::PutBucketTagging, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn put_encryption_configuration_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutEncryptionConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(
            PolicyAction::PutEncryptionConfiguration,
            "bucket",
            Some("caller"),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn put_lifecycle_configuration_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutLifecycleConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(
            PolicyAction::PutLifecycleConfiguration,
            "bucket",
            Some("caller"),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn put_bucket_public_access_block_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketPublicAccessBlock","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(
            PolicyAction::PutBucketPublicAccessBlock,
            "bucket",
            Some("caller"),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn put_bucket_object_lock_configuration_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketObjectLockConfiguration","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(
            PolicyAction::PutBucketObjectLockConfiguration,
            "bucket",
            Some("caller"),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn list_bucket_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::ListBucket, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn list_bucket_versions_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucketVersions","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(PolicyAction::ListBucketVersions, "bucket", Some("caller"));

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn list_bucket_delimiter_condition_matches_requested_delimiter() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:delimiter":"/"}}}]}"#,
        )
        .unwrap();
        let matching = bucket_request(PolicyAction::ListBucket, "bucket", Some("caller"))
            .with_delimiter(Some("/"));
        let missing = bucket_request(PolicyAction::ListBucket, "bucket", Some("caller"));
        let nonmatching = bucket_request(PolicyAction::ListBucket, "bucket", Some("caller"))
            .with_delimiter(Some("."));

        assert_eq!(policy.evaluate(&matching), PolicyEvaluation::ExplicitAllow);
        assert_eq!(policy.evaluate(&missing), PolicyEvaluation::NoMatch);
        assert_eq!(policy.evaluate(&nonmatching), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn list_bucket_numeric_max_keys_condition_matches_only_requested_value() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"NumericEquals":{"s3:max-keys":2}}}]}"#,
        )
        .unwrap();
        let matching = bucket_request(PolicyAction::ListBucket, "bucket", Some("caller"))
            .with_max_keys(Some("2"));
        let missing = bucket_request(PolicyAction::ListBucket, "bucket", Some("caller"));
        let nonmatching = bucket_request(PolicyAction::ListBucket, "bucket", Some("caller"))
            .with_max_keys(Some("3"));

        assert_eq!(policy.evaluate(&matching), PolicyEvaluation::ExplicitAllow);
        assert_eq!(policy.evaluate(&missing), PolicyEvaluation::NoMatch);
        assert_eq!(policy.evaluate(&nonmatching), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn list_bucket_string_max_keys_condition_matches_requested_value() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket","Condition":{"StringEquals":{"s3:max-keys":"2"}}}]}"#,
        )
        .unwrap();
        let matching = bucket_request(PolicyAction::ListBucket, "bucket", Some("caller"))
            .with_max_keys(Some("2"));
        let nonmatching = bucket_request(PolicyAction::ListBucket, "bucket", Some("caller"))
            .with_max_keys(Some("3"));

        assert_eq!(policy.evaluate(&matching), PolicyEvaluation::ExplicitAllow);
        assert_eq!(policy.evaluate(&nonmatching), PolicyEvaluation::NoMatch);
    }

    #[test]
    fn list_bucket_multipart_uploads_matches_bucket_resource() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucketMultipartUploads","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap();
        let request = bucket_request(
            PolicyAction::ListBucketMultipartUploads,
            "bucket",
            Some("caller"),
        );

        assert_eq!(policy.evaluate(&request), PolicyEvaluation::ExplicitAllow);
    }

    #[test]
    fn resource_applicability_detail_names_action_and_statement() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::Malformed {
                reason: "Action does not apply to any resource(s) in statement",
                detail: Some("Action \"s3:GetObject\" in Statement \"NO_ID-0\"".to_string()),
            }
        );

        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Sid":"First","Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"},{"Sid":"Second","Effect":"Deny","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::Malformed {
                reason: "Action does not apply to any resource(s) in statement",
                detail: Some("Action \"s3:ListBucket\" in Statement \"Second\"".to_string()),
            }
        );
    }

    #[test]
    fn invalid_iam_principal_format_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":{"AWS":"arn:aws:iam::123456789012:nonexistent-thing"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();
        assert_eq!(
            err,
            BucketPolicyError::Malformed {
                reason: "Invalid principal in policy",
                detail: Some(
                    "\"AWS\" : \"arn:aws:iam::123456789012:nonexistent-thing\"".to_string()
                ),
            }
        );

        for valid in [
            "arn:aws:iam::123456789012:root",
            "arn:aws:iam::123456789012:user/alice",
            "arn:aws:iam::123456789012:role/deploy",
            "123456789012",
        ] {
            let policy = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":{{"AWS":"{valid}"}},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}}]}}"#
            );
            parse_bucket_policy(&policy)
                .unwrap_or_else(|e| panic!("principal {valid} should parse: {e:?}"));
        }
    }

    #[test]
    fn non_wildcard_string_principal_is_rejected() {
        for principal in [
            "arn:aws:iam::123456789012:root",
            "arn:aws:iam::123456789012:nonexistent-thing",
            "123456789012",
        ] {
            let policy = format!(
                r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Deny","Principal":"{principal}","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}}]}}"#
            );
            let err = parse_bucket_policy(&policy).unwrap_err();
            assert_eq!(
                err,
                BucketPolicyError::malformed("Invalid policy syntax."),
                "principal {principal}"
            );
        }
    }

    #[test]
    fn first_resource_not_scoped_to_bucket_flags_foreign_arns() {
        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":["arn:aws:s3:::bucket/*","arn:aws:s3:::other/*"]}]}"#,
        )
        .unwrap();
        assert_eq!(
            policy.first_resource_not_scoped_to_bucket("bucket"),
            Some("arn:aws:s3:::other/*")
        );
        assert_eq!(
            policy.first_resource_not_scoped_to_bucket("other"),
            Some("arn:aws:s3:::bucket/*")
        );

        let policy = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":["s3:GetObject","s3:ListBucket"],"Resource":["arn:aws:s3:::bucket","arn:aws:s3:::bucket/*"]}]}"#,
        )
        .unwrap();
        assert_eq!(policy.first_resource_not_scoped_to_bucket("bucket"), None);
    }

    #[test]
    fn get_bucket_policy_status_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketPolicyStatus","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn get_bucket_policy_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketPolicy","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn put_bucket_policy_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketPolicy","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn delete_bucket_policy_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:DeleteBucketPolicy","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn get_bucket_cors_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketCORS","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn get_bucket_acl_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketAcl","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn get_bucket_location_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketLocation","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn get_bucket_versioning_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketVersioning","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn get_bucket_ownership_controls_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketOwnershipControls","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn get_bucket_tagging_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketTagging","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn get_encryption_configuration_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetEncryptionConfiguration","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn get_lifecycle_configuration_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetLifecycleConfiguration","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn get_bucket_object_lock_configuration_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetBucketObjectLockConfiguration","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn put_bucket_cors_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketCORS","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn put_bucket_acl_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketAcl","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn put_bucket_versioning_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketVersioning","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn put_bucket_ownership_controls_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketOwnershipControls","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn put_bucket_tagging_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketTagging","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn put_encryption_configuration_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutEncryptionConfiguration","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn put_lifecycle_configuration_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutLifecycleConfiguration","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn put_bucket_public_access_block_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketPublicAccessBlock","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn put_bucket_object_lock_configuration_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:PutBucketObjectLockConfiguration","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn list_bucket_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn list_bucket_versions_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucketVersions","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn list_bucket_multipart_uploads_object_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucketMultipartUploads","Resource":"arn:aws:s3:::bucket/*"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }

    #[test]
    fn get_object_bucket_only_resource_is_rejected() {
        let err = parse_bucket_policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket"}]}"#,
        )
        .unwrap_err();

        assert_eq!(
            err.reason(),
            "Action does not apply to any resource(s) in statement"
        );
    }
}
